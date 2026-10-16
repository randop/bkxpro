//! A cloneable handle around the RAN-128 deterministic backend.
//!
//! The validator harness runs the writer on the main thread and the uploader's
//! I/O worker on a dedicated thread, both against the same simulated storage. This
//! wrapper serializes access through a mutex (the simulated backend's own `File`
//! handles already share per-image state, so just cloning the backend would fork
//! its namespace) and records the events the harness needs to check ordering
//! invariants: file acquisition and durable unlink.
//!
//! When a mirror root is configured, acquiring a segment also creates an empty
//! placeholder file so `Index::rebuild_from_segments` (which discovers segment
//! files with `std::fs::read_dir`) sees the segment; the simulated backend remains
//! the only source of bytes.

use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use bkxpro_io::simulated::{SimulatedCounters, SimulatedFault, SimulatedSegmentIo};
use bkxpro_segment_writer::{AcquiredSegment, SegmentIo, WriteOp};

use crate::trace::{Trace, TraceEvent};

/// Stable, session-relative label for a segment file recorded in trace events.
/// Absolute paths live in a fresh temp directory per session, so putting them in
/// events would break the content digest across otherwise identical replays.
pub(crate) fn trace_file_label(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

/// Shared, thread-safe deterministic `SegmentIo`.
#[derive(Clone)]
pub struct SharedSimIo {
    inner: Arc<Mutex<SimulatedSegmentIo>>,
    trace: Arc<Trace>,
    mirror_root: Option<PathBuf>,
    fail_unlink_after_removal: Arc<AtomicBool>,
    current_epoch: Arc<AtomicU64>,
}

impl SharedSimIo {
    pub fn new(trace: Arc<Trace>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimulatedSegmentIo::new())),
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

    fn lock(&self) -> std::sync::MutexGuard<'_, SimulatedSegmentIo> {
        self.inner.lock().expect("simulated io mutex poisoned")
    }

    /// Apply a deterministic fault to the next matching operation.
    pub fn inject_fault(&self, fault: SimulatedFault) {
        self.lock().inject_fault(fault);
    }

    pub fn inject_faults(&self, faults: impl IntoIterator<Item = SimulatedFault>) {
        self.lock().inject_faults(faults);
    }

    /// Drop every queued one-shot fault. Persistent toggles (read-only, ENOSPC)
    /// and the unlink-crash-window arm are cleared separately.
    pub fn clear_faults(&self) {
        self.lock().clear_faults();
    }

    pub fn set_read_only(&self, read_only: bool) {
        self.lock().set_read_only(read_only);
    }

    pub fn set_enospc(&self, enospc: bool) {
        self.lock().set_enospc(enospc);
    }

    /// Simulate a power cut: discard every unsynced data change and restore the
    /// last directory-synced namespace.
    pub fn crash(&self) -> io::Result<()> {
        self.lock().simulate_crash()
    }

    pub fn counters(&self) -> SimulatedCounters {
        self.lock().counters()
    }

    pub fn reset_counters(&self) {
        self.lock().reset_counters();
    }

    pub fn visible_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        self.lock().visible_bytes(path)
    }

    pub fn durable_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        self.lock().durable_bytes(path)
    }

    pub fn visible_len(&self, path: &Path) -> Option<usize> {
        self.lock().visible_len(path)
    }

    pub fn published_paths(&self) -> Vec<PathBuf> {
        self.lock().published_paths()
    }

    pub fn corrupt(&self, path: &Path, offset: u64, xor: u8) -> io::Result<()> {
        self.lock().corrupt(path, offset, xor)
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
        let published = self.lock().published_paths();
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
        self.lock().unlink(path, directory)?;
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
    type File = bkxpro_io::simulated::SimulatedFile;
    type Error = io::Error;

    fn acquire_preallocated_segment(
        &mut self,
        path: &Path,
        capacity: usize,
    ) -> io::Result<AcquiredSegment<Self::File>> {
        let acquired = self.lock().acquire_preallocated_segment(path, capacity)?;
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
        self.lock().write_batch(file, writes)
    }

    fn sync_data(&mut self, file: &Self::File) -> io::Result<()> {
        self.lock().sync_data(file)
    }

    fn commit_batch(&mut self, file: &Self::File, writes: &[WriteOp<'_>]) -> io::Result<()> {
        self.lock().commit_batch(file, writes)
    }

    fn read_aligned(
        &mut self,
        file: &Self::File,
        offset: u64,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        self.lock().read_aligned(file, offset, buffer)
    }

    fn truncate(&mut self, file: &Self::File, len: u64) -> io::Result<()> {
        self.lock().truncate(file, len)
    }

    fn unlink(&mut self, path: &Path, directory: &Path) -> io::Result<()> {
        let result = self.lock().unlink(path, directory);
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
