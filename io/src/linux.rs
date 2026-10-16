use crate::cgroup::MemoryCgroup;
use bkxpro_segmenter::{AcquiredSegment, SegmentIo, WriteOp};
use io_uring::{
    opcode,
    squeue::{Entry, Flags as SqFlags},
    types::{Fd, Fixed, FsyncFlags},
    IoUring, Probe,
};
use std::{
    ffi::{CStr, CString},
    fmt,
    fs::{self, File, OpenOptions},
    io,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::{ffi::OsStrExt, fs::OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    thread::{self, ThreadId},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const BLOCK_SIZE: usize = 4096;
const EXT4_SUPER_MAGIC: u64 = 0xEF53;
const XFS_SUPER_MAGIC: u64 = 0x5846_5342;
const STATX_DIOALIGN_MASK: u32 = libc::STATX_DIOALIGN;
const F_OFD_SETLK_CMD: libc::c_int = 37;
const DEFAULT_RING_ENTRIES: u32 = 512;
const DEFAULT_FILE_SLOTS: usize = 64;
const DEFAULT_BUFFER_SLOTS: usize = 64;

/// Commit primitive selected for a device class only after the fio baseline is measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    /// Submit independent writes, then an `IORING_OP_FSYNC` with `IOSQE_IO_DRAIN`
    /// and `DATASYNC` set. This keeps writes concurrent while ordering sync after all.
    DrainFdatasync,
    /// Submit each fixed-buffer write with `RWF_DSYNC`; no separate sync SQE is used.
    /// This is an experiment switch, not the default, because support/performance varies.
    RwfDsync,
}

/// One io_uring instance belongs to one shard/issuer thread. SQPOLL is deliberately off
/// by default: it reserves CPU and must be justified by device-specific measurements.
#[derive(Debug, Clone)]
pub struct RingConfig {
    pub entries: u32,
    pub registered_file_slots: usize,
    pub registered_buffer_slots: usize,
    pub commit_mode: CommitMode,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self {
            entries: DEFAULT_RING_ENTRIES,
            registered_file_slots: DEFAULT_FILE_SLOTS,
            registered_buffer_slots: DEFAULT_BUFFER_SLOTS,
            commit_mode: CommitMode::DrainFdatasync,
        }
    }
}

/// Bounded counters useful for throughput/latency comparisons against fio. A benchmark
/// harness should record each commit's latency externally to calculate p50/p99; this
/// struct records cumulative counters without allocating an unbounded sample vector.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeStats {
    pub submit_and_wait_calls: u64,
    pub submitted_sqes: u64,
    pub completed_sqes: u64,
    pub committed_batches: u64,
    pub committed_bytes: u64,
    pub commit_time_ns_total: u64,
    pub commit_time_ns_max: u64,
    pub read_ops: u64,
    pub read_bytes: u64,
}

#[derive(Debug)]
struct BufferRegion {
    base: usize,
    len: usize,
}

#[derive(Debug, Default)]
struct FileSlot {
    lease: Weak<()>,
    fd: Option<RawFd>,
}

/// A segment file whose descriptor is also bound into this shard's registered-file
/// table. The slot remains reserved while any copy of this handle exists.
pub struct RegisteredFile {
    file: File,
    slot: u32,
    lease: Arc<()>,
    path: PathBuf,
}

impl RegisteredFile {
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
    pub fn fixed_file_slot(&self) -> u32 {
        self.slot
    }
}

impl fmt::Debug for RegisteredFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisteredFile")
            .field("path", &self.path)
            .field("slot", &self.slot)
            .field("fd", &self.file.as_raw_fd())
            .finish()
    }
}

#[derive(Clone, Copy)]
enum ExpectedResult {
    Exact(u32),
    Zero,
    /// `openat` returns a new fd or a negative errno. Negative results are surfaced to
    /// the caller but do not poison the ring, because ENOENT is part of create/reuse.
    OpenFd,
}

struct Request {
    entry: Entry,
    expected: ExpectedResult,
    label: &'static str,
    // OPENAT's pathname pointer is borrowed by the SQE. Keep its allocation alive
    // until completion, and retain it until ring teardown if submission cannot be
    // proven quiescent.
    keepalive: Option<CString>,
}

/// Real Linux implementation of the `SegmentIo` trait.
///
/// The ring is not internally synchronized and is restricted to its construction
/// thread. Do not share it across threads. Every operation waits for completion before
/// returning, which keeps the borrowed registered-buffer ranges valid and makes error
/// handling deterministic. The shard loop provides batching; this adapter provides
/// submit/completion batching within each group commit.
pub struct UringSegmentIo {
    // Field drop order matters: the kernel ring must be destroyed before any
    // pathname allocations retained for outstanding OPENAT requests are released.
    ring: IoUring,
    poisoned_keepalive: Vec<CString>,
    config: RingConfig,
    issuer: ThreadId,
    files: Vec<FileSlot>,
    buffers: Vec<Option<BufferRegion>>,
    stats: RuntimeStats,
    poisoned: bool,
    /// Internally owned registered region that serves aligned reads whose caller
    /// buffer is not registered (for example the generic segment read path).
    /// Declared after `ring` so its allocation outlives ring teardown.
    read_scratch: Option<ReadScratchBuffer>,
}

/// An internally owned, 4 KiB-aligned allocation registered in a fixed-buffer slot.
///
/// The address is stored as `usize` rather than a pointer so the adapter keeps its
/// `Send + Sync` auto-traits; access is serialized through `&mut UringSegmentIo`, and
/// the ring itself enforces its issuing thread.
struct ReadScratchBuffer {
    ptr: usize,
    layout: std::alloc::Layout,
    len: usize,
    slot: usize,
}

impl ReadScratchBuffer {
    fn new(len: usize) -> io::Result<Self> {
        let layout = std::alloc::Layout::from_size_align(len, BLOCK_SIZE)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        // SAFETY: valid non-zero 4 KiB-aligned layout; released in Drop.
        let raw = unsafe { std::alloc::alloc_zeroed(layout) };
        if raw.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "read scratch allocation failed",
            ));
        }
        Ok(Self {
            ptr: raw as usize,
            layout,
            len,
            // Replaced before the buffer is used; a scratch is never registered at
            // `usize::MAX` without an explicit slot update.
            slot: usize::MAX,
        })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: the allocation covers `len` bytes for the lifetime of self.
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.len) }
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: the allocation covers `len` bytes for the lifetime of self.
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }
}

impl Drop for ReadScratchBuffer {
    fn drop(&mut self) {
        // SAFETY: the address was allocated with this exact layout in `new`.
        unsafe { std::alloc::dealloc(self.ptr as *mut u8, self.layout) };
    }
}

impl Drop for UringSegmentIo {
    fn drop(&mut self) {
        // Destroying an io_uring instance does not always release the locked-memory
        // accounting of its registered buffers: on Linux 6.8 the pages stay charged
        // against RLIMIT_MEMLOCK (which is accounted per user) until an explicit
        // unregister. Shard teardown and process restarts would otherwise leak the
        // whole fixed-buffer footprint and eventually fail with ENOMEM. The ring, the
        // registered scratch allocation, and the caller-owned write buffer are still
        // live here; field order destroys the ring first.
        if self.buffers.iter().any(|buffer| buffer.is_some()) {
            let _ = self.ring.submitter().unregister_buffers();
        }
        if self.files.iter().any(|slot| slot.fd.is_some()) {
            let _ = self.ring.submitter().unregister_files();
        }
    }
}

impl UringSegmentIo {
    pub fn new(config: RingConfig) -> io::Result<Self> {
        if config.entries < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ring entries must be at least 2",
            ));
        }
        if config.registered_file_slots == 0 || config.registered_buffer_slots == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered file and buffer slot counts must be non-zero",
            ));
        }
        if config.registered_file_slots > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered-file slot count exceeds u32",
            ));
        }
        // WRITE_FIXED/READ_FIXED encode the buffer index in u16.
        if config.registered_buffer_slots > (u16::MAX as usize) + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered-buffer slot count exceeds the u16 fixed-buffer index space",
            ));
        }

        let mut builder = IoUring::builder();
        builder.setup_single_issuer();
        builder.setup_defer_taskrun();
        // Do not enable SQPOLL by default: it costs a dedicated core and is a benchmark option.
        let ring = builder.build(config.entries).map_err(|e| io::Error::new(
            e.kind(), format!("io_uring_setup failed (check Linux >= 6.1, kernel.io_uring_disabled, container seccomp, and RLIMIT_MEMLOCK): {e}"),
        ))?;

        ring.submitter()
            .register_buffers_sparse(config.registered_buffer_slots as u32)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("registering sparse fixed buffers failed: {e}"),
                )
            })?;
        ring.submitter()
            .register_files_sparse(config.registered_file_slots as u32)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("registering sparse fixed files failed: {e}"),
                )
            })?;
        // If CQ events can be dropped, a missing CQE can strand the completion barrier.
        // Require NODROP on startup rather than risk waiting forever after CQ overflow.
        if !ring.params().is_feature_nodrop() {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "io_uring lacks IORING_FEAT_NODROP; refusing a runtime where CQ overflow can lose completions"));
        }
        let mut probe = Probe::new();
        ring.submitter()
            .register_probe(&mut probe)
            .map_err(|e| io::Error::new(e.kind(), format!("IORING_REGISTER_PROBE failed: {e}")))?;
        for (name, opcode) in [
            ("WRITE_FIXED", opcode::WriteFixed::CODE),
            ("READ_FIXED", opcode::ReadFixed::CODE),
            ("FSYNC", opcode::Fsync::CODE),
            ("UNLINKAT", opcode::UnlinkAt::CODE),
            ("OPENAT", opcode::OpenAt::CODE),
        ] {
            if !probe.is_supported(opcode) {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("kernel io_uring does not support required opcode {name}"),
                ));
            }
        }

        Ok(Self {
            ring,
            poisoned_keepalive: Vec::new(),
            files: (0..config.registered_file_slots)
                .map(|_| FileSlot::default())
                .collect(),
            buffers: (0..config.registered_buffer_slots).map(|_| None).collect(),
            issuer: thread::current().id(),
            stats: RuntimeStats::default(),
            poisoned: false,
            read_scratch: None,
            config,
        })
    }

    pub fn config(&self) -> &RingConfig {
        &self.config
    }
    pub fn stats(&self) -> RuntimeStats {
        self.stats
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Register an aligned, stable memory region in a pre-created sparse fixed-buffer
    /// slot. The allocation must remain live at the same address until this ring is
    /// dropped or that slot is successfully re-registered. The caller must not deallocate
    /// or reallocate the region while registered, and must not mutate/read it while the
    /// kernel is performing I/O. This adapter waits for every operation before returning.
    ///
    /// # Safety
    /// `ptr..ptr+len` must refer to a valid allocation for the lifetime described above.
    pub unsafe fn register_buffer(
        &mut self,
        index: usize,
        ptr: *mut u8,
        len: usize,
    ) -> io::Result<()> {
        self.ensure_healthy()?;
        if index >= self.buffers.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fixed buffer slot index is out of range",
            ));
        }
        if self
            .read_scratch
            .as_ref()
            .is_some_and(|scratch| scratch.slot == index)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fixed buffer slot is reserved for the internal read scratch buffer",
            ));
        }
        if ptr.is_null() || (ptr as usize) % BLOCK_SIZE != 0 || len == 0 || len % BLOCK_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered buffer must have a non-null 4 KiB-aligned base and length",
            ));
        }
        let base = ptr as usize;
        let end = base.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "buffer address range overflow")
        })?;
        for (other_index, other) in self.buffers.iter().enumerate() {
            if other_index == index {
                continue;
            }
            if let Some(other) = other {
                let other_end = other.base.checked_add(other.len).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "existing buffer address range overflow",
                    )
                })?;
                if base < other_end && other.base < end {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "fixed registered buffer regions must not overlap",
                    ));
                }
            }
        }
        let iov = libc::iovec {
            iov_base: ptr.cast(),
            iov_len: len,
        };
        // SAFETY: The caller upholds this method's allocation lifetime contract.
        unsafe {
            self.ring
                .submitter()
                .register_buffers_update(index as u32, &[iov], None)?;
        }
        self.buffers[index] = Some(BufferRegion { base, len });
        Ok(())
    }

    fn ensure_owner(&self) -> io::Result<()> {
        if thread::current().id() != self.issuer {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "io_uring shard used from a thread other than its SINGLE_ISSUER owner",
            ));
        }
        Ok(())
    }

    fn ensure_healthy(&self) -> io::Result<()> {
        self.ensure_owner()?;
        if self.poisoned {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "io_uring adapter is fail-stop poisoned; restart and recover instead of retrying",
            ));
        }
        Ok(())
    }

    fn poison_and_retain_request_paths(&mut self, requests: &mut [Request]) {
        self.poisoned = true;
        for request in requests {
            if let Some(path) = request.keepalive.take() {
                self.poisoned_keepalive.push(path);
            }
        }
    }

    fn file_slot(&self, file: &RegisteredFile) -> io::Result<u32> {
        self.ensure_healthy()?;
        let index = file.slot as usize;
        let slot = self.files.get(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered file slot is out of range",
            )
        })?;
        let current = slot.lease.upgrade();
        if !current
            .as_ref()
            .is_some_and(|token| Arc::ptr_eq(token, &file.lease))
            || slot.fd != Some(file.raw_fd())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered file handle no longer owns its fixed-file slot",
            ));
        }
        Ok(file.slot)
    }

    /// Find the fixed-buffer slot whose registered region contains `ptr..ptr+len`,
    /// if any. Callers must validate alignment before using the result.
    fn find_buffer_slot(&self, ptr: *const u8, len: usize) -> Option<u16> {
        let start = ptr as usize;
        let end = start.checked_add(len)?;
        self.buffers.iter().enumerate().find_map(|(index, slot)| {
            let region = slot.as_ref()?;
            let region_end = region.base.checked_add(region.len)?;
            (start >= region.base && end <= region_end).then_some(index as u16)
        })
    }

    /// Find a sparse fixed-buffer slot that has not been registered yet.
    fn free_buffer_slot(&self) -> io::Result<usize> {
        self.buffers
            .iter()
            .position(|slot| slot.is_none())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "fixed-buffer table exhausted; increase registered_buffer_slots or release registered buffers",
                )
            })
    }

    fn buffer_slot(&self, ptr: *const u8, len: usize) -> io::Result<u16> {
        self.ensure_healthy()?;
        if ptr.is_null() || len == 0 || (ptr as usize) % BLOCK_SIZE != 0 || len % BLOCK_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "O_DIRECT request buffer address and length must be 4 KiB aligned",
            ));
        }
        self.find_buffer_slot(ptr, len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O buffer is not contained in a registered fixed-buffer slot",
            )
        })
    }

    /// Read `buffer.len()` bytes at `offset` into an already-registered buffer.
    fn read_aligned_registered(
        &mut self,
        file: &RegisteredFile,
        offset: u64,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        let slot = self.file_slot(file)?;
        let index = self.buffer_slot(buffer.as_ptr(), buffer.len())?;
        let request = Request {
            entry: opcode::ReadFixed::new(
                Fixed(slot),
                buffer.as_mut_ptr(),
                u32::try_from(buffer.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "read length exceeds io_uring u32 request size",
                    )
                })?,
                index,
            )
            .offset(offset)
            .build(),
            expected: ExpectedResult::Exact(buffer.len() as u32),
            label: "read",
            keepalive: None,
        };
        self.submit_specs(vec![request])?;
        self.stats.read_ops = self.stats.read_ops.saturating_add(1);
        self.stats.read_bytes = self.stats.read_bytes.saturating_add(buffer.len() as u64);
        Ok(())
    }

    /// Serve a read into an unregistered caller buffer through the internal scratch
    /// region. The scratch is registered once and grown on demand; the old region is
    /// only released after its slot has been replaced, and never while I/O is in flight.
    fn read_aligned_staged(
        &mut self,
        file: &RegisteredFile,
        offset: u64,
        out: &mut [u8],
    ) -> io::Result<()> {
        let len = out.len();
        let grow = self
            .read_scratch
            .as_ref()
            .is_none_or(|scratch| scratch.len < len);
        if grow {
            // Allocate before touching the current registration so an allocation
            // failure leaves the adapter exactly as it was.
            let mut fresh = ReadScratchBuffer::new(len)?;
            let slot = match self.read_scratch.as_ref() {
                Some(scratch) => scratch.slot,
                None => self.free_buffer_slot()?,
            };
            let iov = libc::iovec {
                iov_base: fresh.ptr as *mut libc::c_void,
                iov_len: len,
            };
            // SAFETY: the region is owned by `fresh`, which is moved into
            // `self.read_scratch` before this method returns, and it stays live until
            // this ring is dropped.
            unsafe {
                self.ring
                    .submitter()
                    .register_buffers_update(slot as u32, &[iov], None)?;
            }
            self.buffers[slot] = Some(BufferRegion {
                base: fresh.ptr,
                len,
            });
            fresh.slot = slot;
            // Dropping the previous scratch here is safe: its slot already points at
            // the new region and no operation is in flight (every operation waits).
            self.read_scratch = Some(fresh);
        }

        let mut scratch = self
            .read_scratch
            .take()
            .expect("scratch exists after the grow step");
        let result = self.read_aligned_registered(file, offset, &mut scratch.as_mut_slice()[..len]);
        if result.is_ok() {
            out.copy_from_slice(&scratch.as_slice()[..len]);
        }
        self.read_scratch = Some(scratch);
        result
    }

    fn submit_specs(&mut self, mut requests: Vec<Request>) -> io::Result<Vec<i32>> {
        self.ensure_healthy()?;
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        if requests.len() > self.config.entries as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "request batch exceeds the configured submission queue depth",
            ));
        }
        for (index, request) in requests.iter_mut().enumerate() {
            let entry = std::mem::replace(
                &mut request.entry,
                // Placeholder; immediately overwritten. Entry is not Default,
                // so build a throwaway SQE that is never submitted.
                opcode::Nop::new().build(),
            );
            request.entry = entry.user_data(index as u64 + 1);
        }
        let push_failed = {
            let mut sq = self.ring.submission();
            let mut failed = false;
            for request in &requests {
                // SAFETY: request entries, fixed files, and registered buffers remain valid
                // through the synchronous submission/completion barrier below.
                if unsafe { sq.push(&request.entry) }.is_err() {
                    failed = true;
                    break;
                }
            }
            failed
        };
        if push_failed {
            self.poison_and_retain_request_paths(&mut requests);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "io_uring submission queue unexpectedly full; adapter poisoned",
            ));
        }

        self.stats.submitted_sqes = self
            .stats
            .submitted_sqes
            .saturating_add(requests.len() as u64);
        let mut results: Vec<Option<i32>> = vec![None; requests.len()];
        let mut received = 0usize;
        while received < requests.len() {
            self.stats.submit_and_wait_calls = self.stats.submit_and_wait_calls.saturating_add(1);
            let wait_for = (requests.len() - received).max(1) as u32;
            if let Err(error) = self.ring.submit_and_wait(wait_for as usize) {
                self.poison_and_retain_request_paths(&mut requests);
                return Err(io::Error::new(
                    error.kind(),
                    format!("io_uring_enter/submit_and_wait failed; adapter poisoned: {error}"),
                ));
            }
            let (mut unexpected_completion, dropped_completions) = {
                let mut unexpected = false;
                let mut cq = self.ring.completion();
                for cqe in &mut cq {
                    let id = cqe.user_data();
                    if id == 0 || id as usize > results.len() || results[id as usize - 1].is_some()
                    {
                        unexpected = true;
                        continue;
                    }
                    results[id as usize - 1] = Some(cqe.result());
                    received += 1;
                }
                (unexpected, cq.overflow())
            };
            if dropped_completions != 0 {
                unexpected_completion = true;
            }
            if unexpected_completion {
                self.poison_and_retain_request_paths(&mut requests);
                let detail = if dropped_completions != 0 {
                    format!("completion queue reports {dropped_completions} dropped CQEs")
                } else {
                    "unexpected or duplicate io_uring completion".to_owned()
                };
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("{detail}; adapter poisoned"),
                ));
            }
        }

        self.stats.completed_sqes = self.stats.completed_sqes.saturating_add(received as u64);
        let mut first_error = None;
        let mut completed_values = Vec::with_capacity(results.len());
        for (index, result) in results.into_iter().enumerate() {
            let request = &requests[index];
            let result = match result {
                Some(result) => result,
                None => {
                    first_error = Some(io::Error::new(
                        io::ErrorKind::Other,
                        format!("missing completion for {}", request.label),
                    ));
                    break;
                }
            };
            completed_values.push(result);
            match request.expected {
                ExpectedResult::OpenFd => continue,
                ExpectedResult::Exact(expected) if result >= 0 && result as u32 == expected => {
                    continue
                }
                ExpectedResult::Exact(expected) if result >= 0 => {
                    let kind = if request.label.contains("read") {
                        io::ErrorKind::UnexpectedEof
                    } else {
                        io::ErrorKind::WriteZero
                    };
                    first_error = Some(io::Error::new(
                        kind,
                        format!(
                            "short {}: completed {} bytes, expected {}; no retry is attempted",
                            request.label, result, expected
                        ),
                    ));
                    break;
                }
                ExpectedResult::Zero if result == 0 => continue,
                ExpectedResult::Zero if result > 0 => {
                    first_error = Some(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "{} returned unexpected positive result {result}",
                            request.label
                        ),
                    ));
                    break;
                }
                ExpectedResult::Exact(_) | ExpectedResult::Zero => {
                    first_error = Some(io::Error::from_raw_os_error(-result));
                    break;
                }
            }
        }
        if let Some(error) = first_error {
            self.poisoned = true;
            return Err(io::Error::new(
                error.kind(),
                format!("{error}; io_uring adapter poisoned, restart and recover"),
            ));
        }
        Ok(completed_values)
    }

    fn open_at(&mut self, path: &Path, flags: i32, mode: u32) -> io::Result<File> {
        self.ensure_healthy()?;
        let c_path = path_to_cstring(path)?;
        let request = Request {
            entry: opcode::OpenAt::new(Fd(libc::AT_FDCWD), c_path.as_ptr())
                .flags(flags)
                .mode(mode)
                .build(),
            expected: ExpectedResult::OpenFd,
            label: "openat",
            keepalive: Some(c_path),
        };
        let result = self
            .submit_specs(vec![request])?
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "openat completion missing"))?;
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        // SAFETY: successful IORING_OP_OPENAT returns a newly owned raw fd.
        Ok(unsafe { File::from_raw_fd(result) })
    }

    fn write_specs(
        &self,
        file: &RegisteredFile,
        writes: &[WriteOp<'_>],
        link: bool,
        dsync: bool,
    ) -> io::Result<Vec<Request>> {
        let fixed_file = self.file_slot(file)?;
        let mut requests = Vec::with_capacity(writes.len());
        for write in writes {
            let bytes = write.bytes();
            if write.offset() % BLOCK_SIZE as u64 != 0 || bytes.len() % BLOCK_SIZE != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "write offset and length must be 4 KiB aligned",
                ));
            }
            let buf_index = self.buffer_slot(bytes.as_ptr(), bytes.len())?;
            let mut builder = opcode::WriteFixed::new(
                Fixed(fixed_file),
                bytes.as_ptr(),
                u32::try_from(bytes.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "write length exceeds io_uring u32 request size",
                    )
                })?,
                buf_index,
            )
            .offset(write.offset());
            if dsync {
                builder = builder.rw_flags(libc::RWF_DSYNC);
            }
            let mut entry = builder.build();
            if link {
                entry = entry.flags(SqFlags::IO_LINK);
            }
            requests.push(Request {
                entry,
                expected: ExpectedResult::Exact(bytes.len() as u32),
                label: "write",
                keepalive: None,
            });
        }
        Ok(requests)
    }

    fn queue_plain_writes(
        &mut self,
        file: &RegisteredFile,
        writes: &[WriteOp<'_>],
    ) -> io::Result<()> {
        let max = self.config.entries as usize;
        for batch in writes.chunks(max) {
            let specs = self.write_specs(file, batch, false, false)?;
            self.submit_specs(specs)?;
        }
        Ok(())
    }

    fn sync_file(&mut self, file: &RegisteredFile, datasync: bool) -> io::Result<()> {
        let slot = self.file_slot(file)?;
        let flags = if datasync {
            FsyncFlags::DATASYNC
        } else {
            FsyncFlags::empty()
        };
        self.submit_specs(vec![Request {
            entry: opcode::Fsync::new(Fixed(slot)).flags(flags).build(),
            expected: ExpectedResult::Zero,
            label: if datasync { "fdatasync" } else { "fsync" },
            keepalive: None,
        }])
        .map(|_| ())
    }

    fn validate_direct_buffer(buffer: &[u8], offset: u64) -> io::Result<()> {
        if offset % BLOCK_SIZE as u64 != 0
            || buffer.is_empty()
            || buffer.len() % BLOCK_SIZE != 0
            || (buffer.as_ptr() as usize) % BLOCK_SIZE != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "O_DIRECT buffer, offset, and length must be 4 KiB aligned",
            ));
        }
        Ok(())
    }
}

impl SegmentIo for UringSegmentIo {
    type File = RegisteredFile;
    type Error = io::Error;

    fn acquire_preallocated_segment(
        &mut self,
        path: &Path,
        capacity: usize,
    ) -> io::Result<AcquiredSegment<RegisteredFile>> {
        self.ensure_healthy()?;
        if capacity == 0 || capacity % BLOCK_SIZE != 0 || capacity > i64::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment capacity must be a 4 KiB multiple that fits off_t",
            ));
        }
        let direct_rw = libc::O_RDWR | libc::O_DIRECT | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        let (file, changed) = match self.open_at(path, direct_rw, 0) {
            Ok(file) => (file, false),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match self.open_at(path, direct_rw | libc::O_CREAT | libc::O_EXCL, 0o640) {
                    Ok(file) => (file, true),
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (self.open_at(path, direct_rw, 0)?, false),
                    Err(e) => return Err(io::Error::new(e.kind(), format!("cannot create segment {} (ensure data directory exists and supports O_DIRECT): {e}", path.display()))),
                }
            }
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "cannot open segment {} with O_DIRECT: {error}",
                        path.display()
                    ),
                ))
            }
        };
        if !file.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment path is not a regular file",
            ));
        }
        let raw_fd = file.as_raw_fd();
        if unsafe { libc::ftruncate(raw_fd, capacity as libc::off_t) } < 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { libc::fallocate(raw_fd, 0, 0, capacity as libc::off_t) } < 0 {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                error.kind(),
                format!("fallocate failed for {}: {error}", path.display()),
            ));
        }

        let slot_index = self.files.iter().position(|slot| slot.lease.upgrade().is_none()).ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "registered-file table exhausted; increase registered_file_slots or release sealed-file handles"))?;
        let updated = self
            .ring
            .submitter()
            .register_files_update(slot_index as u32, &[raw_fd])?;
        if updated != 1 {
            self.poisoned = true;
            return Err(io::Error::new(io::ErrorKind::Other, format!("registered-file update changed {updated} entries, expected 1; adapter poisoned")));
        }
        let lease = Arc::new(());
        self.files[slot_index] = FileSlot {
            lease: Arc::downgrade(&lease),
            fd: Some(raw_fd),
        };
        Ok(AcquiredSegment {
            file: RegisteredFile {
                file,
                slot: slot_index as u32,
                lease,
                path: path.to_path_buf(),
            },
            directory_entry_changed: changed,
        })
    }

    fn open_segment_for_read(&mut self, path: &Path) -> io::Result<RegisteredFile> {
        self.ensure_healthy()?;
        let direct_ro = libc::O_RDONLY | libc::O_DIRECT | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        let file = self.open_at(path, direct_ro, 0).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "cannot open segment {} read-only with O_DIRECT: {error}",
                    path.display()
                ),
            )
        })?;
        if !file.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment path is not a regular file",
            ));
        }
        let raw_fd = file.as_raw_fd();
        let slot_index = self.files.iter().position(|slot| slot.lease.upgrade().is_none()).ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "registered-file table exhausted; increase registered_file_slots or release sealed-file handles"))?;
        let updated = self
            .ring
            .submitter()
            .register_files_update(slot_index as u32, &[raw_fd])?;
        if updated != 1 {
            self.poisoned = true;
            return Err(io::Error::new(io::ErrorKind::Other, format!("registered-file update changed {updated} entries, expected 1; adapter poisoned")));
        }
        let lease = Arc::new(());
        self.files[slot_index] = FileSlot {
            lease: Arc::downgrade(&lease),
            fd: Some(raw_fd),
        };
        Ok(RegisteredFile {
            file,
            slot: slot_index as u32,
            lease,
            path: path.to_path_buf(),
        })
    }

    fn write_batch(&mut self, file: &RegisteredFile, writes: &[WriteOp<'_>]) -> io::Result<()> {
        self.ensure_healthy()?;
        if writes.is_empty() {
            return Ok(());
        }
        self.queue_plain_writes(file, writes)
    }

    fn sync_data(&mut self, file: &RegisteredFile) -> io::Result<()> {
        self.ensure_healthy()?;
        self.sync_file(file, true)
    }

    fn commit_batch(&mut self, file: &RegisteredFile, writes: &[WriteOp<'_>]) -> io::Result<()> {
        self.ensure_healthy()?;
        let started = Instant::now();
        let bytes: u64 = writes.iter().map(|write| write.bytes().len() as u64).sum();
        let result = (|| -> io::Result<()> {
            match self.config.commit_mode {
                CommitMode::RwfDsync => {
                    if writes.is_empty() {
                        return self.sync_file(file, true);
                    }
                    let mut specs = self.write_specs(file, writes, false, true)?;
                    let max = self.config.entries as usize;
                    while !specs.is_empty() {
                        let take = specs.len().min(max);
                        let batch: Vec<Request> = specs.drain(..take).collect();
                        self.submit_specs(batch)?;
                    }
                    Ok(())
                }
                CommitMode::DrainFdatasync => {
                    if writes.is_empty() {
                        return self.sync_file(file, true);
                    }
                    if writes.len() + 1 <= self.config.entries as usize {
                        let mut specs = self.write_specs(file, writes, false, false)?;
                        specs.push(Request {
                            entry: opcode::Fsync::new(Fixed(self.file_slot(file)?))
                                .flags(FsyncFlags::DATASYNC)
                                .build()
                                .flags(SqFlags::IO_DRAIN),
                            expected: ExpectedResult::Zero,
                            label: "fdatasync",
                            keepalive: None,
                        });
                        self.submit_specs(specs).map(|_| ())
                    } else {
                        // Very large footer/seal batches can exceed the SQ depth. Drain and
                        // validate all write completions in chunks, then submit fdatasync.
                        self.queue_plain_writes(file, writes)?;
                        self.sync_file(file, true)
                    }
                }
            }
        })();
        let elapsed = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.stats.committed_batches = self.stats.committed_batches.saturating_add(1);
        self.stats.committed_bytes = self.stats.committed_bytes.saturating_add(bytes);
        self.stats.commit_time_ns_total = self.stats.commit_time_ns_total.saturating_add(elapsed);
        self.stats.commit_time_ns_max = self.stats.commit_time_ns_max.max(elapsed);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn read_aligned(
        &mut self,
        file: &RegisteredFile,
        offset: u64,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        self.ensure_healthy()?;
        Self::validate_direct_buffer(buffer, offset)?;
        if self
            .find_buffer_slot(buffer.as_ptr(), buffer.len())
            .is_some()
        {
            return self.read_aligned_registered(file, offset, buffer);
        }
        // The caller did not lease a registered buffer. Stage the read through the
        // internally owned registered scratch region and copy the bytes out so the
        // generic read path stays usable without a caller-managed buffer pool.
        self.read_aligned_staged(file, offset, buffer)
    }

    fn truncate(&mut self, file: &RegisteredFile, len: u64) -> io::Result<()> {
        self.ensure_healthy()?;
        self.file_slot(file)?;
        if len > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "truncate size exceeds off_t",
            ));
        }
        // IORING_OP_FTRUNCATE is newer than the Linux 6.1 baseline. Use ftruncate(2)
        // for this control-plane operation; durability is explicit in the subsequent
        // io_uring fdatasync. No data-path write falls back to pwrite or buffered I/O.
        if unsafe { libc::ftruncate(file.raw_fd(), len as libc::off_t) } < 0 {
            self.poisoned = true;
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                error.kind(),
                format!("ftruncate failed; adapter poisoned: {error}"),
            ));
        }
        Ok(())
    }

    fn unlink(&mut self, path: &Path, directory: &Path) -> io::Result<()> {
        self.ensure_healthy()?;
        let full_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            let file_name = path.file_name().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unlink path has no final component",
                )
            })?;
            directory.join(file_name)
        };
        let c_path = path_to_cstring(&full_path)?;
        let request = Request {
            entry: opcode::UnlinkAt::new(Fd(libc::AT_FDCWD), c_path.as_ptr())
                .flags(0)
                .build(),
            expected: ExpectedResult::Zero,
            label: "unlinkat",
            keepalive: Some(c_path),
        };
        self.submit_specs(vec![request])?;
        self.sync_directory(directory)
    }

    fn sync_directory(&mut self, directory: &Path) -> io::Result<()> {
        self.ensure_healthy()?;
        let directory_file = match File::open(directory) {
            Ok(file) => file,
            Err(error) => {
                self.poisoned = true;
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "cannot open directory {} for fsync; adapter poisoned: {error}",
                        directory.display()
                    ),
                ));
            }
        };
        let result = self
            .submit_specs(vec![Request {
                entry: opcode::Fsync::new(Fd(directory_file.as_raw_fd())).build(),
                expected: ExpectedResult::Zero,
                label: "directory fsync",
                keepalive: None,
            }])
            .map(|_| ());
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn file_size(&self, file: &RegisteredFile) -> Result<u64, Self::Error> {
        self.ensure_healthy()?;
        self.file_slot(file)?;
        let metadata = file.file.metadata()?;
        Ok(metadata.len())
    }
}

fn path_to_cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains an embedded NUL"))
}

/// Keep the lock object alive for the entire service lifetime. The lock is an OFD lock
/// on a stable file inside the data directory, not a process-associated POSIX record lock.
pub struct DirectoryLock {
    file: File,
    path: PathBuf,
}

impl DirectoryLock {
    pub fn acquire(data_dir: &Path) -> io::Result<Self> {
        let path = data_dir.join(".bkxpro.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .mode(0o640)
            .open(&path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("cannot open data-directory lock {}: {e}", path.display()),
                )
            })?;
        // Linux struct flock layout is provided by libc. F_OFD_SETLK is 37 on Linux;
        // using the constant keeps the implementation portable across libc crate versions.
        // SAFETY: zeroed flock is initialized field by field before fcntl reads it.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_WRLCK as _;
        lock.l_whence = libc::SEEK_SET as _;
        lock.l_start = 0;
        lock.l_len = 0;
        if unsafe { libc::fcntl(file.as_raw_fd(), F_OFD_SETLK_CMD, &lock) } < 0 {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(error.kind(), format!("data directory is already locked or OFD locks are unavailable: {}. Stop the other bkxpro process or verify Linux OFD-lock support.", error)));
        }
        Ok(Self { file, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

#[derive(Debug, Clone)]
pub struct HostPreflightConfig {
    pub data_dir: PathBuf,
    pub memory_budget_bytes: u64,
    pub registered_buffer_bytes: u64,
    pub ring_entries: u32,
    pub commit_mode: CommitMode,
}

impl Default for HostPreflightConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            memory_budget_bytes: 2 * 1024 * 1024 * 1024,
            registered_buffer_bytes: 256 * 1024 * 1024,
            ring_entries: DEFAULT_RING_ENTRIES,
            commit_mode: CommitMode::DrainFdatasync,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreflightReport {
    pub data_dir: PathBuf,
    pub kernel_release: String,
    pub filesystem: String,
    pub dio_mem_align: u32,
    pub dio_offset_align: u32,
    pub write_cache: String,
    pub cgroup_memory_current: u64,
    pub cgroup_memory_max: u64,
    pub memlock_limit: u64,
    pub locked_memory_bytes: u64,
    pub memory_budget_bytes: u64,
    pub registered_buffer_bytes: u64,
    pub ring_entries: u32,
    pub commit_mode: CommitMode,
}

pub struct PreflightResult {
    pub report: PreflightReport,
    /// Keep this value alive for the life of the service to enforce single-process ownership.
    pub directory_lock: DirectoryLock,
}

#[derive(Debug)]
pub struct PreflightError {
    stage: &'static str,
    source: io::Error,
}

impl PreflightError {
    fn new(stage: &'static str, source: io::Error) -> Self {
        Self { stage, source }
    }
}

impl fmt::Display for PreflightError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{0}: {1}", self.stage, self.source)
    }
}
impl std::error::Error for PreflightError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Validate host and data directory requirements, perform a real one-block O_DIRECT
/// write+fdatasync through fixed buffers/files, and return the held directory lock.
pub fn run_host_preflight(config: &HostPreflightConfig) -> Result<PreflightResult, PreflightError> {
    let data_dir = config
        .data_dir
        .canonicalize()
        .map_err(|e| PreflightError::new("data directory must exist and be accessible", e))?;
    if !data_dir.is_dir() {
        return Err(PreflightError::new(
            "invalid data directory",
            io::Error::new(io::ErrorKind::NotADirectory, data_dir.display().to_string()),
        ));
    }
    let directory_lock = DirectoryLock::acquire(&data_dir)
        .map_err(|e| PreflightError::new("single-process data-directory lock failed", e))?;
    File::open(&data_dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| {
            PreflightError::new("cannot fsync data directory after lock-file creation", e)
        })?;

    let kernel_release = kernel_release()
        .map_err(|e| PreflightError::new("cannot determine Linux kernel version", e))?;
    let version = parse_kernel_version(&kernel_release).ok_or_else(|| {
        PreflightError::new(
            "cannot parse Linux kernel release",
            io::Error::new(io::ErrorKind::InvalidData, kernel_release.clone()),
        )
    })?;
    if version < (6, 1) {
        return Err(PreflightError::new("unsupported Linux kernel", io::Error::new(io::ErrorKind::Unsupported, format!("Linux 6.1 or newer is required; found {kernel_release}. Upgrade the host kernel."))));
    }
    check_io_uring_sysctl()
        .map_err(|e| PreflightError::new("io_uring host policy check failed", e))?;

    let (memory_current, memory_max) =
        cgroup_memory().map_err(|e| PreflightError::new("cgroup v2 memory check failed", e))?;
    if config.memory_budget_bytes == 0 || config.memory_budget_bytes > memory_max {
        return Err(PreflightError::new("configured memory budget exceeds cgroup limit", io::Error::new(io::ErrorKind::InvalidInput, format!("configured budget {} bytes, cgroup memory.max {} bytes; lower the service budget or raise memory.max", config.memory_budget_bytes, memory_max))));
    }
    if config.registered_buffer_bytes == 0
        || config.registered_buffer_bytes > config.memory_budget_bytes
    {
        return Err(PreflightError::new("registered-buffer budget is inconsistent", io::Error::new(io::ErrorKind::InvalidInput, format!("registered buffers require {} bytes but the entire service memory budget is {} bytes; lower the buffer pool or raise the service budget", config.registered_buffer_bytes, config.memory_budget_bytes))));
    }
    if memory_current.saturating_add(config.registered_buffer_bytes) > config.memory_budget_bytes {
        return Err(PreflightError::new("insufficient cgroup memory headroom", io::Error::new(io::ErrorKind::OutOfMemory, format!("memory.current is {memory_current} bytes and registered buffers need {} bytes, exceeding service memory budget {} bytes; free cgroup memory or lower configured budgets", config.registered_buffer_bytes, config.memory_budget_bytes))));
    }
    let memlock_limit =
        memlock_limit().map_err(|e| PreflightError::new("RLIMIT_MEMLOCK query failed", e))?;
    let currently_locked = vm_locked_bytes()
        .map_err(|e| PreflightError::new("cannot read current locked-memory usage", e))?;
    let memlock_headroom = memlock_limit.saturating_sub(currently_locked);
    if config.registered_buffer_bytes == 0 || config.registered_buffer_bytes > memlock_headroom {
        return Err(PreflightError::new("registered-buffer memlock budget is insufficient", io::Error::new(io::ErrorKind::PermissionDenied, format!("registered buffers require {} bytes; VmLck is {currently_locked} bytes, RLIMIT_MEMLOCK is {memlock_limit} bytes, and remaining headroom is {memlock_headroom} bytes; raise the service/container memlock limit or lower the registered buffer pool", config.registered_buffer_bytes))));
    }

    let filesystem = filesystem_type(&data_dir)
        .map_err(|e| PreflightError::new("filesystem preflight failed", e))?;
    if filesystem != "ext4" && filesystem != "xfs" {
        return Err(PreflightError::new(
            "unsupported staging filesystem",
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "{} is on {filesystem}; only tested XFS and ext4 filesystems are supported",
                    data_dir.display()
                ),
            ),
        ));
    }

    if config.ring_entries < 8 {
        return Err(PreflightError::new(
            "invalid io_uring entry count",
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "ring_entries must be at least 8; choose a higher entry count",
            ),
        ));
    }
    let entries = config.ring_entries;
    // Declare the registered allocation before the ring so Rust drops the ring first
    // on every return path; registered memory must remain live until ring teardown.
    let mut probe_buffer = AlignedProbeBuffer::new(BLOCK_SIZE)
        .map_err(|e| PreflightError::new("cannot allocate aligned O_DIRECT probe buffer", e))?;
    let ring_config = RingConfig {
        entries,
        registered_file_slots: 4,
        registered_buffer_slots: 4,
        commit_mode: config.commit_mode,
    };
    let mut io = UringSegmentIo::new(ring_config).map_err(|e| {
        PreflightError::new(
            "io_uring capability setup failed (seccomp or kernel policy may block it)",
            e,
        )
    })?;
    // SAFETY: probe_buffer owns a stable, aligned allocation and outlives `io` below.
    unsafe { io.register_buffer(0, probe_buffer.as_mut_ptr(), probe_buffer.len()) }
        .map_err(|e| PreflightError::new("cannot register O_DIRECT probe buffer", e))?;

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos();
    let probe_path = data_dir.join(format!(
        ".bkxpro-direct-probe-{}-{nonce}.tmp",
        std::process::id()
    ));
    let mut cleanup = ProbeFileCleanup {
        path: probe_path.clone(),
        directory: data_dir.clone(),
        active: true,
    };
    let acquired = io
        .acquire_preallocated_segment(&probe_path, BLOCK_SIZE)
        .map_err(|e| PreflightError::new("O_DIRECT probe open/preallocation failed", e))?;
    if acquired.directory_entry_changed {
        io.sync_directory(&data_dir).map_err(|e| {
            PreflightError::new(
                "cannot fsync data directory after creating O_DIRECT probe",
                e,
            )
        })?;
    }
    let (dio_mem_align, dio_offset_align, dev_major, dev_minor) =
        dio_alignment(acquired.file.raw_fd())
            .map_err(|e| PreflightError::new("STATX_DIOALIGN query failed", e))?;
    if dio_mem_align == 0
        || dio_offset_align == 0
        || dio_mem_align > BLOCK_SIZE as u32
        || dio_offset_align > BLOCK_SIZE as u32
        || BLOCK_SIZE as u32 % dio_mem_align != 0
        || BLOCK_SIZE as u32 % dio_offset_align != 0
    {
        return Err(PreflightError::new("device direct-I/O alignment is incompatible with segment format", io::Error::new(io::ErrorKind::Unsupported, format!("STATX_DIOALIGN reported memory={dio_mem_align}, offset={dio_offset_align}; both must be non-zero divisors of 4096. Use a supported XFS/ext4 device with alignment <= 4 KiB."))));
    }
    let write_cache = device_write_cache(dev_major, dev_minor)
        .map_err(|e| PreflightError::new("cannot read block-device write-cache state", e))?;

    probe_buffer.fill(0xA5);
    let probe_write = [WriteOp::new(0, probe_buffer.as_slice()).map_err(|e| {
        PreflightError::new(
            "O_DIRECT probe block is invalid",
            io::Error::new(io::ErrorKind::InvalidInput, e.to_string()),
        )
    })?];
    io.commit_batch(&acquired.file, &probe_write)
        .map_err(|e| PreflightError::new("real io_uring O_DIRECT write probe failed", e))?;
    // Even when production is configured for per-write RWF_DSYNC, preflight must
    // independently verify the fdatasync operation needed for bulk pre-zeroing.
    io.sync_data(&acquired.file)
        .map_err(|e| PreflightError::new("real io_uring fdatasync probe failed", e))?;
    io.unlink(&probe_path, &data_dir).map_err(|e| {
        PreflightError::new("cannot remove O_DIRECT probe file and fsync directory", e)
    })?;
    cleanup.active = false;
    drop(acquired);

    Ok(PreflightResult {
        report: PreflightReport {
            data_dir,
            kernel_release,
            filesystem,
            dio_mem_align,
            dio_offset_align,
            write_cache,
            cgroup_memory_current: memory_current,
            cgroup_memory_max: memory_max,
            memlock_limit,
            locked_memory_bytes: currently_locked,
            memory_budget_bytes: config.memory_budget_bytes,
            registered_buffer_bytes: config.registered_buffer_bytes,
            ring_entries: entries,
            commit_mode: config.commit_mode,
        },
        directory_lock,
    })
}

struct ProbeFileCleanup {
    path: PathBuf,
    directory: PathBuf,
    active: bool,
}
impl Drop for ProbeFileCleanup {
    fn drop(&mut self) {
        if self.active {
            let _ = fs::remove_file(&self.path);
            if let Ok(dir) = File::open(&self.directory) {
                let _ = dir.sync_all();
            }
        }
    }
}

struct AlignedProbeBuffer {
    ptr: std::ptr::NonNull<u8>,
    layout: std::alloc::Layout,
    len: usize,
}
impl AlignedProbeBuffer {
    fn new(len: usize) -> io::Result<Self> {
        let layout = std::alloc::Layout::from_size_align(len, BLOCK_SIZE)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // SAFETY: valid nonzero 4 KiB aligned layout; released in Drop.
        let raw = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = std::ptr::NonNull::new(raw).ok_or_else(|| {
            io::Error::new(io::ErrorKind::OutOfMemory, "aligned allocation failed")
        })?;
        Ok(Self { ptr, layout, len })
    }
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }
    fn len(&self) -> usize {
        self.len
    }
    fn as_slice(&self) -> &[u8] {
        // SAFETY: allocation covers len bytes for the lifetime of self.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}
impl std::ops::Deref for AlignedProbeBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}
impl std::ops::DerefMut for AlignedProbeBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `&mut self` grants unique access to this allocation.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}
impl Drop for AlignedProbeBuffer {
    fn drop(&mut self) {
        // SAFETY: pointer was allocated with this exact layout.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

fn kernel_release() -> io::Result<String> {
    // SAFETY: uname initializes the output structure on success.
    let mut value: libc::utsname = unsafe { std::mem::zeroed() };
    if unsafe { libc::uname(&mut value) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: utsname.release is NUL-terminated by uname.
    Ok(unsafe { CStr::from_ptr(value.release.as_ptr()) }
        .to_string_lossy()
        .into_owned())
}

fn parse_kernel_version(release: &str) -> Option<(u32, u32)> {
    let mut parts = release.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor_text = parts.next()?;
    let minor = minor_text
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()?;
    Some((major, minor))
}

fn check_io_uring_sysctl() -> io::Result<()> {
    let value = match fs::read_to_string("/proc/sys/kernel/io_uring_disabled") {
        Ok(value) => value.trim().parse::<u8>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid /proc/sys/kernel/io_uring_disabled value",
            )
        })?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error),
    };
    match value {
        0 => Ok(()),
        1 if has_cap_sys_admin()? || has_io_uring_group()? => Ok(()),
        1 => Err(io::Error::new(io::ErrorKind::PermissionDenied, "kernel.io_uring_disabled=1 blocks this process; grant CAP_SYS_ADMIN, add it to the GID configured by kernel.io_uring_group, or set the sysctl to 0 according to host policy")),
        2 => Err(io::Error::new(io::ErrorKind::PermissionDenied, "kernel.io_uring_disabled=2 disables io_uring for all processes; set it to 0 according to host policy")),
        n => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unexpected kernel.io_uring_disabled value {n}"))),
    }
}

fn has_io_uring_group() -> io::Result<bool> {
    let raw = match fs::read_to_string("/proc/sys/kernel/io_uring_group") {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let group = raw.trim().parse::<i64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid /proc/sys/kernel/io_uring_group value",
        )
    })?;
    if group < 0 {
        return Ok(false);
    }
    let group = group as libc::gid_t;
    // SAFETY: getegid takes no pointer arguments.
    if unsafe { libc::getegid() } == group {
        return Ok(true);
    }
    // SAFETY: getgroups(0, null) asks for the number of supplementary groups.
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    if count == 0 {
        return Ok(false);
    }
    let mut groups = vec![0 as libc::gid_t; count as usize];
    // SAFETY: groups has space for `count` gid_t values.
    let filled = unsafe { libc::getgroups(count, groups.as_mut_ptr()) };
    if filled < 0 {
        return Err(io::Error::last_os_error());
    }
    groups.truncate(filled as usize);
    Ok(groups.contains(&group))
}

fn has_cap_sys_admin() -> io::Result<bool> {
    let status = fs::read_to_string("/proc/self/status")?;
    let caps = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "CapEff missing from /proc/self/status",
            )
        })?;
    let caps = u64::from_str_radix(caps.trim(), 16)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid CapEff value"))?;
    Ok((caps & (1u64 << 21)) != 0)
}

fn cgroup_memory() -> io::Result<(u64, u64)> {
    let cgroup = MemoryCgroup::detect()?;
    let current = cgroup.current_bytes()?;
    let max = cgroup.effective_limit_bytes()?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "cgroup v2 memory.max is unlimited ('max') at every visible ancestor; configure a finite memory.max for bounded memory",
        )
    })?;
    Ok((current, max))
}

fn vm_locked_bytes() -> io::Result<u64> {
    let status = fs::read_to_string("/proc/self/status")?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix("VmLck:"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "VmLck is missing from /proc/self/status",
            )
        })?;
    let kib = value
        .split_whitespace()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmLck has no value"))?
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "VmLck is not an integer"))?;
    kib.checked_mul(1024)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmLck byte count overflow"))
}

fn memlock_limit() -> io::Result<u64> {
    // SAFETY: getrlimit initializes the supplied rlimit on success.
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) } < 0 {
        return Err(io::Error::last_os_error());
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        Ok(u64::MAX)
    } else {
        Ok(limit.rlim_cur as u64)
    }
}

fn filesystem_type(directory: &Path) -> io::Result<String> {
    let c_path = path_to_cstring(directory)?;
    // SAFETY: statfs writes to the initialized output object.
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c_path.as_ptr(), &mut stat) } < 0 {
        return Err(io::Error::last_os_error());
    }
    match stat.f_type as u64 {
        EXT4_SUPER_MAGIC => Ok("ext4".into()),
        XFS_SUPER_MAGIC => Ok("xfs".into()),
        other => Ok(format!("unknown (statfs magic {other:#x})")),
    }
}

fn dio_alignment(fd: RawFd) -> io::Result<(u32, u32, u32, u32)> {
    let empty = CString::new("").expect("empty CString");
    // SAFETY: statx writes a complete structure to `result` when successful.
    let mut result: libc::statx = unsafe { std::mem::zeroed() };
    let mask = libc::STATX_BASIC_STATS | STATX_DIOALIGN_MASK;
    if unsafe {
        libc::statx(
            fd,
            empty.as_ptr(),
            libc::AT_EMPTY_PATH | libc::AT_STATX_SYNC_AS_STAT,
            mask,
            &mut result,
        )
    } < 0
    {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(error.kind(), format!("statx(STATX_DIOALIGN) failed: {error}; require Linux 6.1+ and a filesystem/kernel that reports DIO alignment")));
    }
    if result.stx_mask & STATX_DIOALIGN_MASK == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "filesystem did not return STATX_DIOALIGN; refusing to assume O_DIRECT alignment",
        ));
    }
    Ok((
        result.stx_dio_mem_align,
        result.stx_dio_offset_align,
        result.stx_dev_major,
        result.stx_dev_minor,
    ))
}

fn device_write_cache(major: u32, minor: u32) -> io::Result<String> {
    let dev_link = PathBuf::from(format!("/sys/dev/block/{major}:{minor}"));
    let device = fs::canonicalize(&dev_link).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "cannot resolve block device {major}:{minor} at {}: {e}",
                dev_link.display()
            ),
        )
    })?;
    for ancestor in device.ancestors() {
        let path = ancestor.join("queue/write_cache");
        if path.is_file() {
            return fs::read_to_string(&path)
                .map(|s| s.trim().to_string())
                .map_err(|e| {
                    io::Error::new(e.kind(), format!("cannot read {}: {e}", path.display()))
                });
        }
    }
    Err(io::Error::new(io::ErrorKind::NotFound, format!("no queue/write_cache found for block device {major}:{minor}; expose sysfs or run preflight on the storage host")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_version_parser_ignores_distro_suffix() {
        assert_eq!(parse_kernel_version("6.1.89-ubuntu-foo"), Some((6, 1)));
        assert_eq!(parse_kernel_version("5.15.0-abc"), Some((5, 15)));
        assert_eq!(parse_kernel_version("not-a-kernel"), None);
    }

    #[test]
    fn commit_mode_defaults_to_drained_fdatasync() {
        assert_eq!(
            RingConfig::default().commit_mode,
            CommitMode::DrainFdatasync
        );
        assert_eq!(
            HostPreflightConfig::default().commit_mode,
            CommitMode::DrainFdatasync
        );
    }

    #[test]
    fn direct_buffer_alignment_is_strict() {
        let layout = std::alloc::Layout::from_size_align(BLOCK_SIZE * 2, BLOCK_SIZE).unwrap();
        // SAFETY: allocation is freed below using matching layout.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        // SAFETY: 4 KiB buffer refers to the allocation above.
        let aligned = unsafe { std::slice::from_raw_parts(ptr, BLOCK_SIZE) };
        assert!(UringSegmentIo::validate_direct_buffer(aligned, 0).is_ok());
        assert!(UringSegmentIo::validate_direct_buffer(aligned, 1).is_err());
        assert!(UringSegmentIo::validate_direct_buffer(&aligned[1..], 0).is_err());
        // SAFETY: matching allocation/layout; slices above are no longer used.
        unsafe { std::alloc::dealloc(ptr, layout) };
    }

    #[test]
    fn caller_cannot_replace_the_internal_read_scratch_slot() {
        let mut ring = match UringSegmentIo::new(RingConfig::default()) {
            Ok(ring) => ring,
            Err(error) => {
                eprintln!("skipping io_uring scratch-slot test: {error}");
                return;
            }
        };
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("read.seg");
        fs::write(&path, vec![0x3c; BLOCK_SIZE]).unwrap();
        let file = ring.open_segment_for_read(&path).unwrap();
        let mut out = vec![0; BLOCK_SIZE];
        ring.read_aligned_staged(&file, 0, &mut out).unwrap();
        let slot = ring.read_scratch.as_ref().unwrap().slot;
        let mut caller = bkxpro_segmenter::AlignedBuffer::new_zeroed(BLOCK_SIZE).unwrap();
        // SAFETY: caller stays allocated until after the ring is dropped.
        let error =
            unsafe { ring.register_buffer(slot, caller.as_mut_ptr(), caller.len()) }.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        // Other slots remain available to callers.
        let other_slot = ring.free_buffer_slot().unwrap();
        unsafe { ring.register_buffer(other_slot, caller.as_mut_ptr(), caller.len()) }.unwrap();
        out.fill(0);
        ring.read_aligned_staged(&file, 0, &mut out).unwrap();
        assert_eq!(out, vec![0x3c; BLOCK_SIZE]);
        drop(ring);
    }

    #[test]
    fn statx_dioalign_mask_is_linux_uapi_constant() {
        assert_eq!(STATX_DIOALIGN_MASK, 0x2000);
    }

    #[test]
    fn dropping_the_ring_releases_registered_buffer_accounting() {
        // The kernel charges registered buffers against RLIMIT_MEMLOCK (per user) and
        // keeps them charged until an explicit unregister, so a ring that is dropped
        // without unregistering permanently leaks its fixed-buffer footprint.
        let mut limits = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limits` is a valid, initialized rlimit struct.
        unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limits) };
        // Three quarters of the limit: the second registration only fits when the
        // first ring released its accounting instead of leaking it.
        let bytes = (limits.rlim_cur / 4 * 3).min(32 * 1024 * 1024) as usize;
        if bytes < 256 * 1024 {
            eprintln!("skipping: RLIMIT_MEMLOCK is too small for a registration probe");
            return;
        }
        let layout = std::alloc::Layout::from_size_align(bytes, BLOCK_SIZE).unwrap();
        // Registering the same size twice only fits when the first ring released its
        // accounting, which makes the second attempt the actual assertion.
        for attempt in 0..2 {
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!ptr.is_null(), "allocation failed");
            let mut ring = UringSegmentIo::new(RingConfig::default())
                .expect("io_uring ring; kernel >= 6.1 with io_uring enabled");
            // SAFETY: the buffer stays allocated and at a fixed address until the ring
            // is dropped below.
            let result = unsafe { ring.register_buffer(0, ptr, bytes) };
            drop(ring);
            // SAFETY: the ring is gone, so nothing references the buffer any more.
            unsafe { std::alloc::dealloc(ptr, layout) };
            result.unwrap_or_else(|error| {
                panic!("attempt {attempt}: registering {bytes} bytes failed: {error}")
            });
        }
    }

    #[test]
    fn ofd_lock_command_is_linux_fcntl_constant() {
        assert_eq!(F_OFD_SETLK_CMD, 37);
    }
}
