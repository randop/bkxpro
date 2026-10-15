use crate::io_trait::{SegmentFile, StorageIo};
use io_uring::{
    opcode,
    squeue::Entry,
    types::{Fd, FsyncFlags},
    IoUring,
};
use std::{
    any::Any,
    collections::HashMap,
    ffi::CString,
    fs::File,
    io::{Error, ErrorKind, Result},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub(crate) const BLOCK_SIZE: usize = 4096;

#[repr(align(4096))]
#[derive(Clone)]
pub(crate) struct AlignedBlock(pub [u8; BLOCK_SIZE]);

fn validate_alignment(offset: u64, len: usize) -> Result<()> {
    if offset % BLOCK_SIZE as u64 != 0 || len % BLOCK_SIZE != 0 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "direct I/O requires 4 KiB offset and length alignment",
        ));
    }
    Ok(())
}

// Serialize one submission/completion at a time. Owned operation resources stay
// alive even if submission fails after the kernel has accepted the SQE.
struct Ring {
    ring: IoUring,
    pending: Option<Box<dyn Any + Send + Sync>>,
    _preflight: Option<crate::preflight::PreflightReport>,
}

impl Drop for Ring {
    fn drop(&mut self) {
        // A fatal submit/wait failure leaves completion state unknown. Closing a
        // ring can cancel asynchronously, so retain owned resources until process
        // exit rather than free memory the kernel may still reference.
        if let Some(resources) = self.pending.take() {
            std::mem::forget(resources);
        }
    }
}

impl Ring {
    /// Every pointer in `entry` must point into a stable allocation in `resources`.
    unsafe fn execute<T: Any + Send + Sync>(
        &mut self,
        entry: Entry,
        resources: T,
    ) -> Result<(i32, T)> {
        if self.pending.is_some() {
            return Err(Error::other(
                "io_uring is poisoned after a submission failure",
            ));
        }
        // SAFETY: caller supplies owned resources retained through completion.
        unsafe { self.ring.submission().push(&entry) }
            .map_err(|_| Error::other("io_uring submission queue full"))?;
        loop {
            match self.ring.submit_and_wait(1) {
                Ok(_) => break,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error) => {
                    self.pending = Some(Box::new(resources));
                    return Err(error);
                }
            }
        }
        let result = self
            .ring
            .completion()
            .next()
            .expect("one outstanding SQE")
            .result();
        if result < 0 {
            return Err(Error::from_raw_os_error(-result));
        }
        Ok((result, resources))
    }
}

/// Blocking trait facade over io_uring submissions and completions, with O_DIRECT
/// segment data. Ring creation is fallible; there is no synchronous data fallback.
pub struct IoUringBackend {
    ring: Arc<Mutex<Ring>>,
}

impl IoUringBackend {
    pub fn new(ring_entries: u32) -> Result<Self> {
        Ok(Self {
            ring: Arc::new(Mutex::new(Ring {
                ring: IoUring::new(ring_entries)?,
                pending: None,
                _preflight: None,
            })),
        })
    }

    /// Service constructor: holds the directory lock until this backend and all
    /// segment handles have been dropped. `new` is for already-checked hosts/tools.
    pub fn with_preflight(
        ring_entries: u32,
        data_dir: &Path,
        memory_budget_bytes: u64,
    ) -> Result<Self> {
        let report = crate::preflight::run_preflight(data_dir, memory_budget_bytes)?;
        let backend = Self::new(ring_entries)?;
        backend.ring.lock().unwrap()._preflight = Some(report);
        Ok(backend)
    }

    fn open(&self, path: &Path, flags: i32) -> Result<Arc<File>> {
        let path = CString::new(path.as_os_str().as_bytes())?;
        let entry = opcode::OpenAt::new(Fd(libc::AT_FDCWD), path.as_ptr())
            .flags(flags | libc::O_CLOEXEC)
            .mode(0o600)
            .build();
        // SAFETY: CString allocation is stable until completion.
        let (fd, _) = unsafe { self.ring.lock().unwrap().execute(entry, path)? };
        // SAFETY: successful OPENAT completion transfers a new fd to us.
        Ok(Arc::new(unsafe { File::from_raw_fd(fd) }))
    }
}

impl StorageIo for IoUringBackend {
    fn open_segment(&self, path: &Path, create_new: bool) -> Result<Box<dyn SegmentFile>> {
        let flags = libc::O_RDWR
            | libc::O_DIRECT
            | if create_new {
                libc::O_CREAT | libc::O_EXCL
            } else {
                0
            };
        Ok(Box::new(IoUringSegmentFile {
            file: self.open(path, flags)?,
            ring: Arc::clone(&self.ring),
        }))
    }

    fn preallocate_and_prezero(&self, path: &Path, size: u64) -> Result<()> {
        let file = self.open(path, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC)?;
        let entry = opcode::Fallocate::new(Fd(file.as_raw_fd()), size).build();
        // SAFETY: the owned file remains open until completion. CQE errors contain
        // the fallocate error itself, rather than an unrelated thread errno.
        unsafe {
            self.ring
                .lock()
                .unwrap()
                .execute(entry, Arc::clone(&file))?;
        }
        let mut segment = IoUringSegmentFile {
            file,
            ring: Arc::clone(&self.ring),
        };
        let zeros = [0; BLOCK_SIZE];
        let mut offset = 0;
        while offset < size {
            let len = (size - offset).min(BLOCK_SIZE as u64) as usize;
            segment.write_all_at(offset, &zeros[..len])?;
            offset += len as u64;
        }
        segment.datasync()?;
        if let Some(parent) = path.parent() {
            self.sync_directory(parent)?;
        }
        Ok(())
    }

    fn unlink_and_sync_dir(&self, path: &Path) -> Result<()> {
        let name = CString::new(path.as_os_str().as_bytes())?;
        let entry = opcode::UnlinkAt::new(Fd(libc::AT_FDCWD), name.as_ptr()).build();
        // SAFETY: CString allocation stays alive until completion.
        unsafe {
            self.ring.lock().unwrap().execute(entry, name)?;
        }
        if let Some(parent) = path.parent() {
            self.sync_directory(parent)?;
        }
        Ok(())
    }

    fn sync_directory(&self, path: &Path) -> Result<()> {
        let dir = self.open(path, libc::O_RDONLY | libc::O_DIRECTORY)?;
        let entry = opcode::Fsync::new(Fd(dir.as_raw_fd())).build();
        // SAFETY: directory fd stays alive until completion.
        unsafe {
            self.ring.lock().unwrap().execute(entry, dir)?;
        }
        Ok(())
    }
}

pub struct IoUringSegmentFile {
    file: Arc<File>,
    ring: Arc<Mutex<Ring>>,
}

impl IoUringSegmentFile {
    fn write_all_at(&mut self, offset: u64, buffer: &[u8]) -> Result<usize> {
        complete_write(
            |written| {
                let len = (buffer.len() - written).min(BLOCK_SIZE);
                let mut block = Box::new(AlignedBlock([0; BLOCK_SIZE]));
                block.0[..len].copy_from_slice(&buffer[written..written + len]);
                let entry =
                    opcode::Write::new(Fd(self.file.as_raw_fd()), block.0.as_ptr(), len as u32)
                        .offset(offset + written as u64)
                        .build();
                // SAFETY: block and file are owned until completion; no borrowed caller
                // memory is exposed to the kernel. Short writes use a fresh aligned block.
                let (n, _) = unsafe {
                    self.ring
                        .lock()
                        .unwrap()
                        .execute(entry, (Arc::clone(&self.file), block))?
                };
                Ok(n as usize)
            },
            buffer.len(),
        )
    }
}

fn complete_write(mut write: impl FnMut(usize) -> Result<usize>, len: usize) -> Result<usize> {
    let mut written = 0;
    while written < len {
        let n = write(written)?;
        if n == 0 {
            return Err(Error::new(ErrorKind::WriteZero, "write made no progress"));
        }
        if n > len - written {
            return Err(Error::new(ErrorKind::InvalidData, "invalid write count"));
        }
        written += n;
    }
    Ok(written)
}

impl SegmentFile for IoUringSegmentFile {
    fn aligned_write(&mut self, offset: u64, buffer: &[u8]) -> Result<usize> {
        validate_alignment(offset, buffer.len())?;
        self.write_all_at(offset, buffer)
    }

    fn datasync(&mut self) -> Result<()> {
        let entry = opcode::Fsync::new(Fd(self.file.as_raw_fd()))
            .flags(FsyncFlags::DATASYNC)
            .build();
        // SAFETY: file remains open until completion.
        unsafe {
            self.ring
                .lock()
                .unwrap()
                .execute(entry, Arc::clone(&self.file))?;
        }
        Ok(())
    }

    fn aligned_read(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        validate_alignment(offset, buffer.len())?;
        let mut read = 0;
        for chunk in buffer.chunks_mut(BLOCK_SIZE) {
            let mut block = Box::new(AlignedBlock([0; BLOCK_SIZE]));
            let entry = opcode::Read::new(
                Fd(self.file.as_raw_fd()),
                block.0.as_mut_ptr(),
                BLOCK_SIZE as u32,
            )
            .offset(offset + read as u64)
            .build();
            // SAFETY: block and file are retained; only copy back after completion.
            let (n, (_, block)) = unsafe {
                self.ring
                    .lock()
                    .unwrap()
                    .execute(entry, (Arc::clone(&self.file), block))?
            };
            let n = n as usize;
            chunk[..n].copy_from_slice(&block.0[..n]);
            read += n;
            if n < BLOCK_SIZE {
                break;
            }
        }
        Ok(read)
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        // Control syscall: IORING_OP_FTRUNCATE requires a kernel newer than 6.1.
        // Data and durability operations still go through the ring.
        self.file.set_len(size)
    }

    fn len(&self) -> Result<u64> {
        let mut stat: Box<libc::statx> = Box::new(unsafe { std::mem::zeroed() });
        let entry = opcode::Statx::new(
            Fd(self.file.as_raw_fd()),
            c"".as_ptr(),
            (&mut *stat as *mut libc::statx).cast(),
        )
        .flags(libc::AT_EMPTY_PATH)
        .mask(libc::STATX_SIZE)
        .build();
        // SAFETY: output allocation and fd remain alive until completion.
        let (_, (_, stat)) = unsafe {
            self.ring
                .lock()
                .unwrap()
                .execute(entry, (Arc::clone(&self.file), stat))?
        };
        Ok(stat.stx_size)
    }
}

/// Deterministic Simulated I/O Backend for testing (RAN-128 / RAN-132)
#[derive(Clone)]
pub struct SimulatedBackend {
    files: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
}

impl SimulatedBackend {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl StorageIo for SimulatedBackend {
    fn open_segment(&self, path: &Path, create_new: bool) -> Result<Box<dyn SegmentFile>> {
        let mut files = self.files.lock().unwrap();
        if create_new {
            files.entry(path.to_path_buf()).or_insert_with(Vec::new);
        } else if !files.contains_key(path) {
            // For robust test compatibility, if preallocate wasn't explicitly called or path needs auto-creation,
            // or return NotFound. But preallocate inserts it. Let's ensure if it's missing, we either error or insert.
            // Stricter: return NotFound, but ensure tests preallocate first.
            return Err(Error::new(ErrorKind::NotFound, "segment missing"));
        }
        Ok(Box::new(SimulatedSegmentFile {
            path: path.to_path_buf(),
            files: Arc::clone(&self.files),
        }))
    }

    fn preallocate_and_prezero(&self, path: &Path, size: u64) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        let data = vec![0u8; size as usize];
        files.insert(path.to_path_buf(), data);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        Ok(())
    }

    fn unlink_and_sync_dir(&self, path: &Path) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        files.remove(path);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        if let Some(parent) = path.parent() {
            self.sync_directory(parent)?;
        }
        Ok(())
    }

    fn sync_directory(&self, _path: &Path) -> Result<()> {
        Ok(())
    }
}

pub struct SimulatedSegmentFile {
    path: PathBuf,
    files: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
}

impl SegmentFile for SimulatedSegmentFile {
    fn aligned_write(&mut self, offset: u64, buffer: &[u8]) -> Result<usize> {
        validate_alignment(offset, buffer.len())?;
        let mut files = self.files.lock().unwrap();
        let data = files
            .get_mut(&self.path)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Simulated segment file not found"))?;
        let end = (offset as usize) + buffer.len();
        if end > data.len() {
            data.resize(end, 0);
        }
        data[(offset as usize)..end].copy_from_slice(buffer);
        Ok(buffer.len())
    }

    fn datasync(&mut self) -> Result<()> {
        Ok(())
    }

    fn aligned_read(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        validate_alignment(offset, buffer.len())?;
        let files = self.files.lock().unwrap();
        let data = files
            .get(&self.path)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Simulated segment file not found"))?;
        let start = offset as usize;
        if start >= data.len() {
            return Ok(0);
        }
        let end = std::cmp::min(start + buffer.len(), data.len());
        let n = end - start;
        buffer[..n].copy_from_slice(&data[start..end]);
        Ok(n)
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        let data = files
            .get_mut(&self.path)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Simulated segment file not found"))?;
        data.resize(size as usize, 0);
        Ok(())
    }

    fn len(&self) -> Result<u64> {
        let files = self.files.lock().unwrap();
        let data = files
            .get(&self.path)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Simulated segment file not found"))?;
        Ok(data.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_write_handles_short_writes_and_zero_progress() {
        let mut offsets = Vec::new();
        assert_eq!(
            complete_write(
                |offset| {
                    offsets.push(offset);
                    Ok((10 - offset).min(3))
                },
                10
            )
            .unwrap(),
            10
        );
        assert_eq!(offsets, [0, 3, 6, 9]);
        assert_eq!(
            complete_write(|_| Ok(0), 10).unwrap_err().kind(),
            ErrorKind::WriteZero
        );
        assert_eq!(
            complete_write(|_| Err(Error::from_raw_os_error(libc::ENOSPC)), 10)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOSPC)
        );
    }

    #[test]
    fn probe_and_staging_blocks_are_address_aligned() {
        let stack = AlignedBlock([0; BLOCK_SIZE]);
        let heap = Box::new(AlignedBlock([0; BLOCK_SIZE]));
        assert_eq!(stack.0.as_ptr() as usize % BLOCK_SIZE, 0);
        assert_eq!(heap.0.as_ptr() as usize % BLOCK_SIZE, 0);
    }
}
