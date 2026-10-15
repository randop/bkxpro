//! Deterministic in-memory `SegmentIo` backend for codec/writer conformance and the
//! future recovery/fault harnesses. This is a test seam, not a production fallback.
use bkxpro_segment_writer::{AcquiredSegment, SegmentIo, WriteOp};
use std::{
    collections::{BTreeMap, HashSet},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Debug, Default)]
struct Image {
    /// Bytes visible to the running process, including unsynced writes.
    volatile: Vec<u8>,
    /// Bytes survived by the last simulated data sync.
    durable: Vec<u8>,
}

#[derive(Default)]
struct Store {
    /// Namespace visible before the next simulated crash.
    files: BTreeMap<PathBuf, Arc<Mutex<Image>>>,
    /// Namespace persisted by the last directory sync.
    durable_files: BTreeMap<PathBuf, Arc<Mutex<Image>>>,
}

#[derive(Debug, Clone)]
pub struct SimulatedFile {
    path: PathBuf,
    image: Arc<Mutex<Image>>,
}

impl SimulatedFile {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Deterministic one-shot faults. A short write copies only the requested prefix into
/// volatile bytes and then errors, emulating a torn/short completion without retrying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulatedFault {
    FailNextWrite,
    ShortNextWrite { prefix_bytes: usize },
    FailNextSync,
    FailNextRead,
    FailNextDirectorySync,
}

#[derive(Default)]
pub struct SimulatedSegmentIo {
    store: Store,
    next_write_fault: Option<SimulatedFault>,
    fail_next_sync: bool,
    fail_next_read: bool,
    fail_next_directory_sync: bool,
    directory_syncs: u64,
}

impl SimulatedSegmentIo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inject_fault(&mut self, fault: SimulatedFault) {
        match fault {
            SimulatedFault::FailNextWrite | SimulatedFault::ShortNextWrite { .. } => {
                self.next_write_fault = Some(fault);
            }
            SimulatedFault::FailNextSync => self.fail_next_sync = true,
            SimulatedFault::FailNextRead => self.fail_next_read = true,
            SimulatedFault::FailNextDirectorySync => self.fail_next_directory_sync = true,
        }
    }

    pub fn directory_syncs(&self) -> u64 {
        self.directory_syncs
    }

    /// Current visible file contents, primarily for deterministic tests.
    pub fn visible_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        self.store
            .files
            .get(path)
            .and_then(|image| image.lock().ok().map(|image| image.volatile.clone()))
    }

    /// Last data-synchronized bytes for a file which is still in the durable namespace.
    pub fn durable_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        self.store
            .durable_files
            .get(path)
            .and_then(|image| image.lock().ok().map(|image| image.durable.clone()))
    }

    /// Discard every unsynced data change and restore the last synced directory namespace.
    /// Directory fsync and data sync are independent durability boundaries.
    pub fn simulate_crash(&mut self) -> io::Result<()> {
        self.store.files = self.store.durable_files.clone();
        let mut visited = HashSet::new();
        for shared in self.store.files.values() {
            let address = Arc::as_ptr(shared) as usize;
            if !visited.insert(address) {
                continue;
            }
            let mut image = shared
                .lock()
                .map_err(|_| poisoned_lock("simulated file image"))?;
            image.volatile = image.durable.clone();
        }
        self.next_write_fault = None;
        self.fail_next_sync = false;
        self.fail_next_read = false;
        self.fail_next_directory_sync = false;
        Ok(())
    }

    #[allow(dead_code)]
    fn io_error(kind: io::ErrorKind, message: &'static str) -> io::Error {
        io::Error::new(kind, message)
    }
}

fn poisoned_lock(name: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("{name} mutex poisoned"))
}

impl SegmentIo for SimulatedSegmentIo {
    type File = SimulatedFile;
    type Error = io::Error;

    fn acquire_preallocated_segment(
        &mut self,
        path: &Path,
        capacity: usize,
    ) -> io::Result<AcquiredSegment<Self::File>> {
        if capacity == 0 || capacity % 4096 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "simulated segment capacity must be a non-zero 4 KiB multiple",
            ));
        }
        let created = !self.store.files.contains_key(path);
        let image = self
            .store
            .files
            .entry(path.to_path_buf())
            .or_insert_with(|| {
                Arc::new(Mutex::new(Image {
                    volatile: vec![0; capacity],
                    durable: Vec::new(),
                }))
            })
            .clone();
        {
            let mut locked = image
                .lock()
                .map_err(|_| poisoned_lock("simulated file image"))?;
            locked.volatile.resize(capacity, 0);
        }
        Ok(AcquiredSegment {
            file: SimulatedFile {
                path: path.to_path_buf(),
                image,
            },
            directory_entry_changed: created,
        })
    }

    fn write_batch(&mut self, file: &Self::File, writes: &[WriteOp<'_>]) -> io::Result<()> {
        if let Some(fault) = self.next_write_fault.take() {
            match fault {
                SimulatedFault::FailNextWrite => {
                    return Err(io::Error::from_raw_os_error(libc::EIO))
                }
                SimulatedFault::ShortNextWrite { prefix_bytes } => {
                    if let Some(write) = writes.first() {
                        let prefix = prefix_bytes.min(write.bytes().len());
                        let start = usize::try_from(write.offset()).map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "simulated write offset exceeds usize",
                            )
                        })?;
                        let end = start.checked_add(prefix).ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "simulated write range overflow",
                            )
                        })?;
                        let mut image = file
                            .image
                            .lock()
                            .map_err(|_| poisoned_lock("simulated file image"))?;
                        if end > image.volatile.len() {
                            return Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "simulated short write exceeded file capacity",
                            ));
                        }
                        image.volatile[start..end].copy_from_slice(&write.bytes()[..prefix]);
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "injected short write; no retry is attempted",
                    ));
                }
                _ => unreachable!("only write faults are stored in next_write_fault"),
            }
        }

        let mut image = file
            .image
            .lock()
            .map_err(|_| poisoned_lock("simulated file image"))?;
        for write in writes {
            let start = usize::try_from(write.offset()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "simulated write offset exceeds usize",
                )
            })?;
            let end = start.checked_add(write.bytes().len()).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "simulated write range overflow",
                )
            })?;
            if end > image.volatile.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "simulated write exceeded preallocated file capacity",
                ));
            }
            image.volatile[start..end].copy_from_slice(write.bytes());
        }
        Ok(())
    }

    fn sync_data(&mut self, file: &Self::File) -> io::Result<()> {
        if self.fail_next_sync {
            self.fail_next_sync = false;
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        let mut image = file
            .image
            .lock()
            .map_err(|_| poisoned_lock("simulated file image"))?;
        image.durable = image.volatile.clone();
        Ok(())
    }

    fn commit_batch(&mut self, file: &Self::File, writes: &[WriteOp<'_>]) -> io::Result<()> {
        self.write_batch(file, writes)?;
        self.sync_data(file)
    }

    fn read_aligned(
        &mut self,
        file: &Self::File,
        offset: u64,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        if self.fail_next_read {
            self.fail_next_read = false;
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        if offset % 4096 != 0
            || buffer.is_empty()
            || buffer.len() % 4096 != 0
            || (buffer.as_ptr() as usize) % 4096 != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "simulated direct read must be 4 KiB aligned",
            ));
        }
        let start = usize::try_from(offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "simulated read offset exceeds usize",
            )
        })?;
        let end = start.checked_add(buffer.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "simulated read range overflow")
        })?;
        let image = file
            .image
            .lock()
            .map_err(|_| poisoned_lock("simulated file image"))?;
        if end > image.volatile.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "simulated short read",
            ));
        }
        buffer.copy_from_slice(&image.volatile[start..end]);
        Ok(())
    }

    fn truncate(&mut self, file: &Self::File, len: u64) -> io::Result<()> {
        let len = usize::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "simulated truncate length exceeds usize",
            )
        })?;
        let mut image = file
            .image
            .lock()
            .map_err(|_| poisoned_lock("simulated file image"))?;
        image.volatile.resize(len, 0);
        Ok(())
    }

    fn unlink(&mut self, path: &Path, _directory: &Path) -> io::Result<()> {
        if self.store.files.remove(path).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "simulated unlink target does not exist",
            ));
        }
        // Match the production trait contract: unlink returns only after directory sync.
        self.sync_directory(_directory)
    }

    fn sync_directory(&mut self, _directory: &Path) -> io::Result<()> {
        if self.fail_next_directory_sync {
            self.fail_next_directory_sync = false;
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        self.store.durable_files = self.store.files.clone();
        self.directory_syncs = self.directory_syncs.saturating_add(1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bkxpro_segment_writer::{AlignedBuffer, WriteOp};

    #[test]
    fn committed_bytes_survive_crash_but_unsynced_bytes_do_not() {
        let mut io = SimulatedSegmentIo::new();
        let path = PathBuf::from("/sim/1.seg");
        let acquired = io.acquire_preallocated_segment(&path, 4096).unwrap();
        io.sync_directory(Path::new("/sim")).unwrap();
        let mut buffer = AlignedBuffer::new_zeroed(4096).unwrap();
        buffer.fill(0x5A);
        let writes = [WriteOp::new(0, &buffer[..]).unwrap()];
        io.commit_batch(&acquired.file, &writes).unwrap();
        buffer.fill(0xA5);
        let writes = [WriteOp::new(0, &buffer[..]).unwrap()];
        io.write_batch(&acquired.file, &writes).unwrap();
        io.simulate_crash().unwrap();
        assert_eq!(io.visible_bytes(&path).unwrap()[0], 0x5A);
    }

    #[test]
    fn short_write_fault_is_one_shot_and_never_silently_retries() {
        let mut io = SimulatedSegmentIo::new();
        let path = PathBuf::from("/sim/2.seg");
        let acquired = io.acquire_preallocated_segment(&path, 4096).unwrap();
        let mut buffer = AlignedBuffer::new_zeroed(4096).unwrap();
        buffer.fill(0x37);
        let writes = [WriteOp::new(0, &buffer[..]).unwrap()];
        io.inject_fault(SimulatedFault::ShortNextWrite { prefix_bytes: 17 });
        assert_eq!(
            io.write_batch(&acquired.file, &writes).unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        assert_eq!(&io.visible_bytes(&path).unwrap()[..17], &[0x37; 17]);
        io.write_batch(&acquired.file, &writes).unwrap();
        assert_eq!(io.visible_bytes(&path).unwrap()[0], 0x37);
    }
}
