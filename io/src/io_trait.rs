use std::io::Result;
use std::path::Path;

/// Storage operations complete before returning. A production implementation must
/// use io_uring for data transfers and durability; truncate may use a control syscall.
pub trait StorageIo: Send + Sync {
    fn open_segment(&self, path: &Path, create_new: bool) -> Result<Box<dyn SegmentFile>>;
    fn preallocate_and_prezero(&self, path: &Path, size: u64) -> Result<()>;
    fn unlink_and_sync_dir(&self, path: &Path) -> Result<()>;
    fn sync_directory(&self, path: &Path) -> Result<()>;
}

/// Offsets and buffer lengths must be multiples of 4096. Callers may pass ordinary
/// slices: the production backend stages them in address-aligned owned buffers.
pub trait SegmentFile: Send + Sync {
    fn aligned_write(&mut self, offset: u64, buffer: &[u8]) -> Result<usize>;
    fn datasync(&mut self) -> Result<()>;
    fn aligned_read(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize>;
    fn truncate(&mut self, size: u64) -> Result<()>;
    fn len(&self) -> Result<u64>;
}
