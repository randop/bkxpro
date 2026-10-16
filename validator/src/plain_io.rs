//! A real-filesystem [`SegmentIo`] backend for the kill-soak runs (Linux only).
//!
//! The deterministic simulator (`SimulatedSegmentIo`) is the right backend for
//! every replayable scenario, but a `SIGKILL` soak must exercise the real process
//! boundary: a child process writes through `std::fs`, the parent kills it, and
//! recovery reads whatever a real filesystem kept. This mirrors the uploader
//! integration-test fixture (`uploader/tests/support/mod.rs`), deliberately so:
//! both are plain positioned I/O with `fsync`/`fsync(dir)` where the writer asks
//! for durability.
//!
//! Note what this backend can and cannot model: a `SIGKILL` leaves the page cache
//! intact, so this run proves that *every acknowledged entry is recoverable from
//! what was written*, not that the writer synced before acknowledging. The
//! power-loss model with its unsynced-byte discard lives in the simulator, and
//! the `dm-log-writes` replay script in `validator/scripts/` covers real block
//! devices.

use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::FileExt,
    path::Path,
};

use bkxpro_segment_writer::{AcquiredSegment, SegmentIo, WriteOp};

/// Minimal [`SegmentIo`] over plain `std::fs` files, with positioned I/O so the
/// writer's single-issuer semantics are preserved.
#[derive(Default)]
pub struct PlainFileIo;

/// Handle of one file opened through [`PlainFileIo`].
pub struct PlainFile(File);

impl SegmentIo for PlainFileIo {
    type File = PlainFile;
    type Error = io::Error;

    fn acquire_preallocated_segment(
        &mut self,
        path: &Path,
        capacity: usize,
    ) -> io::Result<AcquiredSegment<PlainFile>> {
        let created = !path.exists();
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        if file.metadata()?.len() != capacity as u64 {
            file.set_len(capacity as u64)?;
        }
        Ok(AcquiredSegment {
            file: PlainFile(file),
            directory_entry_changed: created,
        })
    }

    fn open_segment_for_read(&mut self, path: &Path) -> io::Result<PlainFile> {
        Ok(PlainFile(File::open(path)?))
    }

    fn write_batch(&mut self, file: &PlainFile, writes: &[WriteOp<'_>]) -> io::Result<()> {
        for write in writes {
            file.0.write_all_at(write.bytes(), write.offset())?;
        }
        Ok(())
    }

    fn sync_data(&mut self, file: &PlainFile) -> io::Result<()> {
        file.0.sync_data()
    }

    fn commit_batch(&mut self, file: &PlainFile, writes: &[WriteOp<'_>]) -> io::Result<()> {
        self.write_batch(file, writes)?;
        self.sync_data(file)
    }

    fn read_aligned(&mut self, file: &PlainFile, offset: u64, buffer: &mut [u8]) -> io::Result<()> {
        file.0.read_exact_at(buffer, offset)
    }

    fn truncate(&mut self, file: &PlainFile, len: u64) -> io::Result<()> {
        file.0.set_len(len)
    }

    fn unlink(&mut self, path: &Path, directory: &Path) -> io::Result<()> {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        self.sync_directory(directory)
    }

    fn sync_directory(&mut self, directory: &Path) -> io::Result<()> {
        File::open(directory)?.sync_all()
    }

    fn file_size(&self, file: &PlainFile) -> io::Result<u64> {
        Ok(file.0.metadata()?.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bkxpro_segment_writer::AlignedBuffer;

    #[test]
    fn writes_reads_and_sizes_round_trip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("1.seg");
        let mut io = PlainFileIo;
        let acquired = io.acquire_preallocated_segment(&path, 8192).unwrap();
        assert!(acquired.directory_entry_changed);
        assert_eq!(io.file_size(&acquired.file).unwrap(), 8192);
        let mut scratch = AlignedBuffer::new_zeroed(4096).unwrap();
        scratch.iter_mut().for_each(|byte| *byte = 7);
        let op = WriteOp::new(0, &scratch[..]).unwrap();
        io.commit_batch(&acquired.file, &[op]).unwrap();
        let mut read = vec![0u8; 4096];
        io.read_aligned(&acquired.file, 0, &mut read).unwrap();
        assert!(read.iter().all(|byte| *byte == 7));

        io.truncate(&acquired.file, 4096).unwrap();
        assert_eq!(io.file_size(&acquired.file).unwrap(), 4096);

        // Re-acquiring an existing file reports no directory change and restores
        // the capacity.
        let again = io.acquire_preallocated_segment(&path, 8192).unwrap();
        assert!(!again.directory_entry_changed);
        assert_eq!(io.file_size(&again.file).unwrap(), 8192);
        drop(again);

        io.unlink(&path, dir.path()).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn reads_are_visible_without_sync() {
        // The kill-soak soundness argument: a SIGKILL preserves the page cache, so
        // everything `write_all_at` returned must be visible to a fresh handle.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("1.seg");
        let mut io = PlainFileIo;
        let acquired = io.acquire_preallocated_segment(&path, 4096).unwrap();
        let mut scratch = AlignedBuffer::new_zeroed(4096).unwrap();
        scratch[..15].copy_from_slice(b"hello kill-soak");
        let op = WriteOp::new(0, &scratch[..]).unwrap();
        io.write_batch(&acquired.file, &[op]).unwrap();
        drop(acquired);

        let mut fresh = io.open_segment_for_read(&path).unwrap();
        let mut read = vec![0u8; 15];
        io.read_aligned(&mut fresh, 0, &mut read).unwrap();
        assert_eq!(&read, b"hello kill-soak");
    }

    #[test]
    fn unlink_of_a_missing_file_is_ok() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut io = PlainFileIo;
        io.unlink(&dir.path().join("missing.seg"), dir.path())
            .unwrap();
    }
}
