//! Shared fixtures for the uploader integration tests: a `SegmentIo` backend on
//! plain files, sealed-segment builders, and index helpers.
//!
//! The integration tests exercise real files on the test filesystem, which is often
//! tmpfs and therefore has no O_DIRECT support, so the production io_uring backend
//! cannot be used here. [`PlainFileIo`] implements the same seam with `std::fs`,
//! using positioned reads so the worker's single-issuer semantics are preserved.

#![allow(dead_code)]

use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::FileExt,
    path::Path,
    sync::Arc,
};

use bkxpro_codec::{SegmentEncoder, SegmentHeader, SegmentView, RECORD_HEADER_SIZE};
use bkxpro_indexer::{EntryLocation, Index, IndexConfig, SegmentMeta, SegmentState};
use bkxpro_segmenter::{AcquiredSegment, SegmentIo, WriteOp};
use bytes::Bytes;

/// Minimal [`SegmentIo`] on plain `std::fs` files.
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

/// Payload [`sealed_segment`] writes for one entry.
pub fn payload_for(entry: u64, len: usize) -> Bytes {
    Bytes::from(vec![0xA0u8.wrapping_add(entry as u8); len])
}

/// Build one sealed segment image: `entries` records on `ledger`, each with a
/// `payload_len`-byte payload, with one writer epoch and a plausible target size.
pub fn sealed_segment(
    segment_id: u64,
    writer_epoch: u64,
    ledger: u64,
    entries: u64,
    payload_len: usize,
) -> Bytes {
    let header = SegmentHeader {
        segment_id,
        writer_epoch,
        target_size: 64 * 1024 * 1024,
        created_at_ns: 1_700_000_000,
    };
    let mut encoder = SegmentEncoder::new(header);
    for entry in 1..=entries {
        encoder
            .push_record(ledger, entry, 0, &payload_for(entry, payload_len))
            .unwrap();
    }
    encoder.seal().unwrap()
}

/// Write a sealed image as `{segment_id}.seg` in `data_dir`.
pub fn write_segment(data_dir: &Path, segment_id: u64, image: &[u8]) {
    fs::write(data_dir.join(format!("{segment_id}.seg")), image).unwrap();
}

/// Open a fresh fjall index under `dir`.
pub fn open_index(dir: &Path) -> Arc<Index> {
    Arc::new(
        Index::open(IndexConfig {
            db_path: dir.to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        })
        .unwrap(),
    )
}

/// Index every record of a sealed image the way recovery does: payload offsets
/// (`EntryLocation.offset` excludes the record header), the ledger range, and the
/// sealed byte size. Returns the indexed entry ids in record order.
pub fn index_sealed(
    index: &Index,
    segment_id: u64,
    state: SegmentState,
    s3_key: Option<String>,
    image: &[u8],
) -> Vec<u64> {
    let view = SegmentView::open_sealed(Bytes::copy_from_slice(image)).unwrap();
    let writer_epoch = view.header().writer_epoch;
    let mut batch = index.batch();
    let mut entries = Vec::new();
    let (mut min_ledger, mut max_ledger) = (u64::MAX, 0u64);
    for record in view.records() {
        let record = record.unwrap();
        entries.push(record.entry);
        min_ledger = min_ledger.min(record.ledger);
        max_ledger = max_ledger.max(record.ledger);
        index
            .put_entry(
                &mut batch,
                record.ledger,
                record.entry,
                EntryLocation {
                    segment_id: segment_id as u32,
                    offset: (record.offset + RECORD_HEADER_SIZE as u64) as u32,
                    len: record.len,
                },
            )
            .unwrap();
    }
    index
        .put_segment(
            &mut batch,
            segment_id,
            SegmentMeta {
                state,
                s3_key,
                min_ledger,
                max_ledger,
                byte_size: image.len() as u64,
                writer_epoch,
            },
        )
        .unwrap();
    batch.commit().unwrap();
    entries
}

/// Overwrite one segment's state while keeping its other metadata (used to
/// reconstruct the index state of an interrupted uploader between passes).
pub fn set_segment_state(index: &Index, segment_id: u64, state: SegmentState) {
    let mut meta = index
        .get_segment(segment_id)
        .unwrap()
        .expect("segment row exists");
    meta.state = state;
    let mut batch = index.batch();
    index.put_segment(&mut batch, segment_id, meta).unwrap();
    batch.commit().unwrap();
}
