//! A cloneable handle around the deterministic backend.
//!
//! The validator harness runs the writer on the main thread and the uploader's
//! I/O worker on a dedicated thread, both against the same simulated storage. This
//! wrapper serializes access through a mutex (the simulated backend's own `File`
//! handles already share per-image state, so just cloning the backend would fork
//! its namespace) and records the events the harness needs to check ordering
//! invariants: file acquisition and durable unlink.
//!
//! The `bkxpro-io` backend deliberately exposes only the minimal deterministic
//! storage seam (one-shot write/sync/read/directory faults, visible and durable
//! bytes, crash, namespace restore). Everything the battle-testing harness needs
//! beyond that - EAGAIN/ENOSPC, torn writes with explicit garbage, post-write byte
//! corruption, slow syncs, failed truncates/acquires, stale reads, persistent
//! read-only/ENOSPC modes, counters, and the live path set - is emulated here, in
//! the harness, so the checked I/O crate stays untouched.
//!
//! When a mirror root is configured, acquiring a segment also creates an empty
//! placeholder file so `Index::rebuild_from_segments` (which discovers segment
//! files with `std::fs::read_dir`) sees the segment; the simulated backend remains
//! the only source of bytes.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    thread,
    time::Duration,
};

use bkxpro_codec::BLOCK_SIZE;
use bkxpro_io::simulated::{SimulatedFile, SimulatedSegmentIo};
use bkxpro_segmenter::{AcquiredSegment, AlignedBuffer, SegmentIo, WriteOp, WriteOpError};

use crate::trace::{Trace, TraceEvent};

/// Stable, session-relative label for a segment file recorded in trace events.
/// Absolute paths live in a fresh temp directory per session, so putting them in
/// events would break the content digest across otherwise identical replays.
pub(crate) fn trace_file_label(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

/// The harness's device-fault vocabulary. The underlying `bkxpro-io` simulator
/// carries a smaller set; [`SharedSimIo`] emulates the richer one on top.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulatedFault {
    FailNextWrite,
    EagainNextWrite,
    EnospcNextWrite,
    /// `prefix_bytes` land, `garbage_bytes` after them are destroyed, then the
    /// write reports a torn/short completion.
    TornNextWrite {
        prefix_bytes: usize,
        garbage_bytes: usize,
    },
    /// The write succeeds and then one byte at `byte_offset` is flipped.
    CorruptNextWrite {
        byte_offset: usize,
        xor: u8,
    },
    FailNextSync,
    SlowNextSync {
        micros: u64,
    },
    FailNextTruncate,
    FailNextDirectorySync,
    FailNextAcquire,
    /// The next read observes only the last durable bytes.
    StaleNextRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultClass {
    Write,
    Sync,
    Read,
    Truncate,
    DirectorySync,
    Acquire,
}

impl SimulatedFault {
    fn class(self) -> FaultClass {
        match self {
            Self::FailNextWrite
            | Self::EagainNextWrite
            | Self::EnospcNextWrite
            | Self::TornNextWrite { .. }
            | Self::CorruptNextWrite { .. } => FaultClass::Write,
            Self::FailNextSync | Self::SlowNextSync { .. } => FaultClass::Sync,
            Self::StaleNextRead => FaultClass::Read,
            Self::FailNextTruncate => FaultClass::Truncate,
            Self::FailNextDirectorySync => FaultClass::DirectorySync,
            Self::FailNextAcquire => FaultClass::Acquire,
        }
    }
}

/// Counters observed across one session, including the slow-sync delay the
/// harness emulates until the duration is consumed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SimulatedCounters {
    pub writes: u64,
    pub syncs: u64,
    pub reads: u64,
    pub truncates: u64,
    pub unlinks: u64,
    pub directory_syncs: u64,
    pub sync_delay_micros: u64,
}

#[derive(Default)]
struct Aux {
    faults: VecDeque<SimulatedFault>,
    read_only: bool,
    enospc: bool,
    counters: SimulatedCounters,
    known_paths: BTreeSet<PathBuf>,
}

fn take_fault(queue: &mut VecDeque<SimulatedFault>, class: FaultClass) -> Option<SimulatedFault> {
    let index = queue.iter().position(|fault| fault.class() == class)?;
    queue.remove(index)
}

fn eio() -> io::Error {
    io::Error::from_raw_os_error(libc::EIO)
}

fn eagain() -> io::Error {
    io::Error::from_raw_os_error(libc::EAGAIN)
}

fn enospc() -> io::Error {
    io::Error::from_raw_os_error(libc::ENOSPC)
}

fn erofs() -> io::Error {
    io::Error::from_raw_os_error(libc::EROFS)
}

fn write_op_error(error: WriteOpError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

/// Byte value the emulated torn write leaves behind: deterministically wrong for
/// the codec's CRC32C, so recovery must stop at the first bad block.
const TORN_GARBAGE: u8 = 0x5A;

/// Emulate the prefix of a torn write: the first `prefix_bytes` of the batch are
/// written for real, `garbage_bytes` after them are replaced with garbage, and
/// everything after that is never written (the preallocated file keeps its
/// zeros). The caller reports the short completion as an error.
fn torn_write(
    io: &mut SimulatedSegmentIo,
    file: &SimulatedFile,
    writes: &[WriteOp<'_>],
    prefix_bytes: usize,
    garbage_bytes: usize,
) -> io::Result<()> {
    let window_start = prefix_bytes;
    let window_end = prefix_bytes.saturating_add(garbage_bytes);
    let mut position = 0usize;
    for write in writes {
        let len = write.bytes().len();
        let start = position;
        let end = position + len;
        position = end;
        if end <= window_start {
            // Entirely before the tear: this block really landed.
            io.write_batch(file, std::slice::from_ref(write))?;
            continue;
        }
        if start >= window_end {
            // After the tear: never written.
            break;
        }
        let mut buffer = AlignedBuffer::new_zeroed(len)?;
        let kept = window_start.saturating_sub(start).min(len);
        buffer[..kept].copy_from_slice(&write.bytes()[..kept]);
        let garbage_end = (window_end - start).min(len);
        for byte in &mut buffer[kept..garbage_end] {
            *byte = TORN_GARBAGE;
        }
        let op = WriteOp::new(write.offset(), &buffer[..]).map_err(write_op_error)?;
        io.write_batch(file, &[op])?;
    }
    Ok(())
}

/// Flip one byte through a 4 KiB read-modify-write, because the aligned
/// `SegmentIo` seam has no smaller unit than a block.
fn flip_byte(
    io: &mut SimulatedSegmentIo,
    file: &SimulatedFile,
    byte_offset: usize,
    xor: u8,
) -> io::Result<()> {
    let block_start = (byte_offset / BLOCK_SIZE) * BLOCK_SIZE;
    let mut block = AlignedBuffer::new_zeroed(BLOCK_SIZE)?;
    io.read_aligned(file, block_start as u64, &mut block)?;
    block[byte_offset - block_start] ^= xor;
    let op = WriteOp::new(block_start as u64, &block[..]).map_err(write_op_error)?;
    io.write_batch(file, &[op])
}

fn published_paths_locked(io: &SimulatedSegmentIo, aux: &Aux) -> Vec<PathBuf> {
    aux.known_paths
        .iter()
        .filter(|path| io.visible_bytes(path).is_some())
        .cloned()
        .collect()
}

/// Shared, thread-safe deterministic `SegmentIo`.
#[derive(Clone)]
pub struct SharedSimIo {
    inner: Arc<Mutex<SimulatedSegmentIo>>,
    aux: Arc<Mutex<Aux>>,
    trace: Arc<Trace>,
    mirror_root: Option<PathBuf>,
    fail_unlink_after_removal: Arc<AtomicBool>,
    current_epoch: Arc<AtomicU64>,
}

impl SharedSimIo {
    pub fn new(trace: Arc<Trace>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimulatedSegmentIo::new())),
            aux: Arc::new(Mutex::new(Aux::default())),
            trace,
            mirror_root: None,
            fail_unlink_after_removal: Arc::new(AtomicBool::new(false)),
            current_epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Like [`SharedSimIo::new`], but also creates zero-length placeholder files
    /// under `root` for every acquired segment so `rebuild_from_segments` can
    /// discover them. Paths outside `root` are not mirrored.
    pub fn with_mirror(trace: Arc<Trace>, root: impl Into<PathBuf>) -> Self {
        Self {
            mirror_root: Some(root.into()),
            ..Self::new(trace)
        }
    }

    fn lock(&self) -> MutexGuard<'_, SimulatedSegmentIo> {
        self.inner.lock().expect("simulated io mutex poisoned")
    }

    fn aux(&self) -> MutexGuard<'_, Aux> {
        self.aux.lock().expect("simulated io aux mutex poisoned")
    }

    /// Queue a deterministic fault. The fault is consumed by the next operation
    /// of its class (write, sync, read, truncate, acquire, directory sync); faults
    /// never consume each other's slot out of order.
    pub fn inject_fault(&self, fault: SimulatedFault) {
        self.aux().faults.push_back(fault);
    }

    pub fn inject_faults(&self, faults: impl IntoIterator<Item = SimulatedFault>) {
        self.aux().faults.extend(faults);
    }

    /// Drop every queued one-shot fault. Persistent toggles (read-only, ENOSPC)
    /// and the unlink-crash-window arm are cleared separately.
    pub fn clear_faults(&self) {
        self.aux().faults.clear();
    }

    pub fn set_read_only(&self, read_only: bool) {
        self.aux().read_only = read_only;
    }

    pub fn set_enospc(&self, enospc: bool) {
        self.aux().enospc = enospc;
    }

    /// Simulate a power cut: discard every unsynced data change and restore the
    /// last directory-synced namespace. One-shot faults are consumed by the power
    /// cycle; persistent toggles survive it.
    pub fn crash(&self) -> io::Result<()> {
        let mut aux = self.aux();
        let mut io = self.lock();
        io.simulate_crash()?;
        aux.faults.clear();
        aux.known_paths
            .retain(|path| io.visible_bytes(path).is_some());
        Ok(())
    }

    pub fn counters(&self) -> SimulatedCounters {
        self.aux().counters
    }

    pub fn reset_counters(&self) {
        self.aux().counters = SimulatedCounters::default();
    }

    pub fn visible_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        self.lock().visible_bytes(path)
    }

    pub fn durable_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        self.lock().durable_bytes(path)
    }

    pub fn visible_len(&self, path: &Path) -> Option<usize> {
        self.visible_bytes(path).map(|bytes| bytes.len())
    }

    pub fn published_paths(&self) -> Vec<PathBuf> {
        let aux = self.aux();
        let io = self.lock();
        published_paths_locked(&io, &aux)
    }

    /// Flip one byte of a live file, outside any injected fault: used by tests
    /// that need to corrupt a sealed segment after the fact.
    pub fn corrupt(&self, path: &Path, offset: u64, xor: u8) -> io::Result<()> {
        let offset = usize::try_from(offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "corruption offset exceeds usize",
            )
        })?;
        let mut io = self.lock();
        let file = io.open_segment_for_read(path)?;
        flip_byte(&mut io, &file, offset, xor)
    }

    /// Writer epoch tagged onto `SegmentAcquired` trace events. The writer itself
    /// does not report its epoch through the `SegmentIo` seam, so the harness sets
    /// this before creating a writer (or repairing a segment).
    pub fn set_current_epoch(&self, epoch: u64) {
        self.current_epoch.store(epoch, Ordering::SeqCst);
    }

    /// Remove placeholder files that have no simulated image (their create never
    /// survived a simulated crash) so directory scans see exactly what the
    /// simulated device exposes.
    pub fn sync_mirror(&self) {
        let Some(root) = &self.mirror_root else {
            return;
        };
        let Ok(entries) = std::fs::read_dir(root) else {
            return;
        };
        let published = self.published_paths();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("seg") {
                continue;
            }
            if !published.contains(&path) {
                let _ = std::fs::remove_file(&path);
            }
        }
    }

    /// Arm a failure that removes the file and *then* reports `EIO`. This
    /// reproduces the crash window "unlink durable, `Deleted` state not written",
    /// which recovery must reconcile through the manifest record.
    pub fn arm_unlink_failure_after_removal(&self) {
        self.fail_unlink_after_removal.store(true, Ordering::SeqCst);
    }

    /// Remove an orphaned preallocation: a file the writer created but never
    /// brought to a valid first commit. Records `OrphanRemoved` instead of
    /// `LocalUnlinked`, because delete-gating only governs segments that could
    /// have acknowledged records.
    pub fn unlink_orphan(&self, path: &Path, directory: &Path) -> io::Result<()> {
        let mut aux = self.aux();
        let result = {
            let mut io = self.lock();
            io.unlink(path, directory)
        };
        if result.is_ok() {
            aux.known_paths.remove(path);
        }
        drop(aux);
        result?;
        self.mirror(path, false);
        self.trace.record(TraceEvent::OrphanRemoved {
            segment_id: Self::segment_id_of(path),
            path: trace_file_label(path),
        });
        Ok(())
    }

    pub fn unlink_failure_armed(&self) -> bool {
        self.fail_unlink_after_removal.load(Ordering::SeqCst)
    }

    fn segment_id_of(path: &Path) -> u64 {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u64>().ok())
            .unwrap_or(0)
    }

    fn mirror(&self, path: &Path, create: bool) {
        let Some(root) = &self.mirror_root else {
            return;
        };
        if !path.starts_with(root) {
            return;
        }
        if create {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path);
        } else {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl SegmentIo for SharedSimIo {
    type File = SimulatedFile;
    type Error = io::Error;

    fn acquire_preallocated_segment(
        &mut self,
        path: &Path,
        capacity: usize,
    ) -> io::Result<AcquiredSegment<Self::File>> {
        let mut aux = self.aux();
        if aux.read_only {
            return Err(erofs());
        }
        if take_fault(&mut aux.faults, FaultClass::Acquire).is_some() {
            return Err(eio());
        }
        let acquired = {
            let mut io = self.lock();
            io.acquire_preallocated_segment(path, capacity)?
        };
        aux.known_paths.insert(path.to_path_buf());
        drop(aux);
        if acquired.directory_entry_changed {
            self.mirror(path, true);
        }
        let segment_id = Self::segment_id_of(path);
        self.trace.record(TraceEvent::SegmentAcquired {
            segment_id,
            epoch: self.current_epoch.load(Ordering::SeqCst),
            capacity,
        });
        Ok(acquired)
    }

    fn open_segment_for_read(&mut self, path: &Path) -> io::Result<Self::File> {
        self.lock().open_segment_for_read(path)
    }

    fn write_batch(&mut self, file: &Self::File, writes: &[WriteOp<'_>]) -> io::Result<()> {
        let mut aux = self.aux();
        if aux.read_only {
            return Err(erofs());
        }
        if aux.enospc {
            return Err(enospc());
        }
        let fault = take_fault(&mut aux.faults, FaultClass::Write);
        aux.counters.writes = aux.counters.writes.saturating_add(writes.len() as u64);
        let mut io = self.lock();
        match fault {
            Some(SimulatedFault::FailNextWrite) => Err(eio()),
            Some(SimulatedFault::EagainNextWrite) => Err(eagain()),
            Some(SimulatedFault::EnospcNextWrite) => Err(enospc()),
            Some(SimulatedFault::TornNextWrite {
                prefix_bytes,
                garbage_bytes,
            }) => {
                torn_write(&mut io, file, writes, prefix_bytes, garbage_bytes)?;
                Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "injected torn write; no retry is attempted",
                ))
            }
            Some(SimulatedFault::CorruptNextWrite { byte_offset, xor }) => {
                io.write_batch(file, writes)?;
                flip_byte(&mut io, file, byte_offset, xor)?;
                Ok(())
            }
            _ => io.write_batch(file, writes),
        }
    }

    fn sync_data(&mut self, file: &Self::File) -> io::Result<()> {
        let mut aux = self.aux();
        aux.counters.syncs = aux.counters.syncs.saturating_add(1);
        match take_fault(&mut aux.faults, FaultClass::Sync) {
            Some(SimulatedFault::FailNextSync) => return Err(eio()),
            Some(SimulatedFault::SlowNextSync { micros }) => {
                aux.counters.sync_delay_micros =
                    aux.counters.sync_delay_micros.saturating_add(micros);
                drop(aux);
                thread::sleep(Duration::from_micros(micros));
                return self.lock().sync_data(file);
            }
            _ => {}
        }
        self.lock().sync_data(file)
    }

    fn commit_batch(&mut self, file: &Self::File, writes: &[WriteOp<'_>]) -> io::Result<()> {
        let mut aux = self.aux();
        if aux.read_only {
            return Err(erofs());
        }
        if aux.enospc {
            return Err(enospc());
        }
        let write_fault = take_fault(&mut aux.faults, FaultClass::Write);
        aux.counters.writes = aux.counters.writes.saturating_add(writes.len() as u64);
        aux.counters.syncs = aux.counters.syncs.saturating_add(1);
        let mut io = self.lock();
        match write_fault {
            Some(SimulatedFault::FailNextWrite) => return Err(eio()),
            Some(SimulatedFault::EagainNextWrite) => return Err(eagain()),
            Some(SimulatedFault::EnospcNextWrite) => return Err(enospc()),
            Some(SimulatedFault::TornNextWrite {
                prefix_bytes,
                garbage_bytes,
            }) => {
                torn_write(&mut io, file, writes, prefix_bytes, garbage_bytes)?;
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "injected torn write; no retry is attempted",
                ));
            }
            Some(SimulatedFault::CorruptNextWrite { byte_offset, xor }) => {
                io.write_batch(file, writes)?;
                flip_byte(&mut io, file, byte_offset, xor)?;
            }
            _ => io.write_batch(file, writes)?,
        }
        // The write phase succeeded (or corrupted itself); now the sync phase.
        match take_fault(&mut aux.faults, FaultClass::Sync) {
            Some(SimulatedFault::FailNextSync) => return Err(eio()),
            Some(SimulatedFault::SlowNextSync { micros }) => {
                aux.counters.sync_delay_micros =
                    aux.counters.sync_delay_micros.saturating_add(micros);
                drop(aux);
                thread::sleep(Duration::from_micros(micros));
                return io.sync_data(file);
            }
            _ => {}
        }
        io.sync_data(file)
    }

    fn read_aligned(
        &mut self,
        file: &Self::File,
        offset: u64,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        let mut aux = self.aux();
        let stale = take_fault(&mut aux.faults, FaultClass::Read).is_some();
        aux.counters.reads = aux.counters.reads.saturating_add(1);
        let io = self.lock();
        if stale {
            // A stale read observes the last durable bytes; regions that were
            // never synced read back as the preallocated zeros.
            let offset = usize::try_from(offset).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "read offset exceeds usize")
            })?;
            buffer.fill(0);
            if let Some(durable) = io.durable_bytes(file.path()) {
                if offset < durable.len() {
                    let end = (offset + buffer.len()).min(durable.len());
                    buffer[..end - offset].copy_from_slice(&durable[offset..end]);
                }
            }
            return Ok(());
        }
        let mut io = io;
        io.read_aligned(file, offset, buffer)
    }

    fn truncate(&mut self, file: &Self::File, len: u64) -> io::Result<()> {
        let mut aux = self.aux();
        if aux.read_only {
            return Err(erofs());
        }
        if take_fault(&mut aux.faults, FaultClass::Truncate).is_some() {
            return Err(eio());
        }
        aux.counters.truncates = aux.counters.truncates.saturating_add(1);
        self.lock().truncate(file, len)
    }

    fn unlink(&mut self, path: &Path, directory: &Path) -> io::Result<()> {
        let mut aux = self.aux();
        if aux.read_only {
            return Err(erofs());
        }
        aux.counters.unlinks = aux.counters.unlinks.saturating_add(1);
        let result = {
            let mut io = self.lock();
            io.unlink(path, directory)
        };
        if result.is_ok() {
            aux.known_paths.remove(path);
        }
        drop(aux);
        if result.is_ok() {
            self.mirror(path, false);
            self.trace.record(TraceEvent::LocalUnlinked {
                segment_id: Self::segment_id_of(path),
                path: trace_file_label(path),
            });
        }
        if self.fail_unlink_after_removal.swap(false, Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected crash window after unlink-before-state-change",
            ));
        }
        result
    }

    fn sync_directory(&mut self, directory: &Path) -> io::Result<()> {
        let mut aux = self.aux();
        if take_fault(&mut aux.faults, FaultClass::DirectorySync).is_some() {
            return Err(eio());
        }
        aux.counters.directory_syncs = aux.counters.directory_syncs.saturating_add(1);
        self.lock().sync_directory(directory)
    }

    fn file_size(&self, file: &Self::File) -> Result<u64, Self::Error> {
        self.lock().file_size(file)
    }
}

/// The live files in a simulated namespace, by path, with their visible bytes.
/// Used by the invariant checker to inspect segment images after a crash.
pub fn snapshot(sim: &SharedSimIo) -> BTreeMap<PathBuf, Vec<u8>> {
    sim.published_paths()
        .into_iter()
        .filter_map(|path| sim.visible_bytes(&path).map(|bytes| (path, bytes)))
        .collect()
}
