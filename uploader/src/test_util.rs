//! Shared in-crate test fixtures: a valid sealed segment image, a way to place it
//! into a simulated [`bkxpro_segmenter::SegmentIo`] backend, and a blob store
//! that models a real object store's part lifetime.

use std::{collections::HashMap, ops::Range, path::Path, sync::Mutex};

use async_trait::async_trait;
use bkxpro_codec::{SegmentEncoder, SegmentHeader};
use bkxpro_io::simulated::SimulatedSegmentIo;
use bkxpro_segmenter::{AlignedBuffer, SegmentIo, WriteOp};
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::blob::{
    BlobError, BlobObject, BlobStore, ExpectedVersion, ObjectInfo, ObjectVersion, PutOutcome,
    StreamItem,
};

/// Build one sealed segment image: `entries` records on ledger 1 with 32-byte
/// payloads, one byte pattern per entry.
pub(crate) fn sealed_image(segment_id: u64, epoch: u64, entries: u64) -> Bytes {
    let header = SegmentHeader {
        segment_id,
        writer_epoch: epoch,
        target_size: 64 * 1024,
        created_at_ns: 0,
    };
    let mut encoder = SegmentEncoder::new(header);
    for entry in 1..=entries {
        encoder
            .push_record(1, entry, 0, &Bytes::from(vec![0xA0 + entry as u8; 32]))
            .unwrap();
    }
    encoder.seal().unwrap()
}

/// Place a sealed image into the simulated backend at `path`.
pub(crate) fn install(io: &mut SimulatedSegmentIo, path: &Path, image: &Bytes) {
    let acquired = io.acquire_preallocated_segment(path, image.len()).unwrap();
    let mut buffer = AlignedBuffer::new_zeroed(image.len()).unwrap();
    buffer.copy_from_slice(image);
    let write = WriteOp::new(0, &buffer).unwrap();
    io.commit_batch(&acquired.file, &[write]).unwrap();
    if acquired.directory_entry_changed {
        io.sync_directory(path.parent().unwrap()).unwrap();
    }
}

/// Blob store that drains upload streams, dropping every chunk as it arrives.
///
/// `InMemory`'s multipart upload retains all parts until `complete()`, so a byte
/// budget smaller than the whole file would block the reader forever. Real object
/// stores release a part when its request completes; this wrapper models that,
/// and records the streamed length and checksum so tests can assert on them.
/// Metadata reads and conditional writes delegate to the inner store.
pub(crate) struct DrainBlob {
    inner: std::sync::Arc<dyn BlobStore>,
    uploads: Mutex<HashMap<String, (u64, u32)>>,
}

impl DrainBlob {
    pub(crate) fn new(inner: std::sync::Arc<dyn BlobStore>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            uploads: Mutex::new(HashMap::new()),
        })
    }

    /// Streamed length and CRC32C of one uploaded key.
    pub(crate) fn uploaded(&self, key: &str) -> Option<(u64, u32)> {
        self.uploads.lock().unwrap().get(key).copied()
    }
}

#[async_trait]
impl BlobStore for DrainBlob {
    async fn put_stream(
        &self,
        key: &str,
        _part_size: usize,
        mut chunks: mpsc::Receiver<StreamItem>,
    ) -> Result<PutOutcome, BlobError> {
        let mut written = 0u64;
        let mut crc32c = 0u32;
        loop {
            match chunks.recv().await {
                Some(StreamItem::Chunk(chunk)) => {
                    written += chunk.len() as u64;
                    crc32c = crc32c::crc32c_append(crc32c, &chunk);
                    // `chunk` drops here, releasing its byte-budget permit exactly
                    // like a completed part upload against a real object store.
                }
                Some(StreamItem::End {
                    len,
                    crc32c: expected,
                }) => {
                    if len != written {
                        return Err(BlobError::Source(format!(
                            "drained {written} bytes but the stream reported {len}"
                        )));
                    }
                    if expected != crc32c {
                        return Err(BlobError::Source(format!(
                            "streamed CRC32C {crc32c:#010x} does not match the reader's {expected:#010x}"
                        )));
                    }
                    self.uploads
                        .lock()
                        .unwrap()
                        .insert(key.to_string(), (written, crc32c));
                    return Ok(PutOutcome {
                        len: written,
                        crc32c,
                        version: ObjectVersion::default(),
                    });
                }
                Some(StreamItem::Failed(message)) => return Err(BlobError::Source(message)),
                None => {
                    return Err(BlobError::Source(
                        "upload stream closed before the end marker".to_string(),
                    ))
                }
            }
        }
    }

    async fn get(&self, key: &str) -> Result<BlobObject, BlobError> {
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, BlobError> {
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectInfo, BlobError> {
        if let Some((len, _)) = self.uploads.lock().unwrap().get(key).copied() {
            return Ok(ObjectInfo {
                key: key.to_string(),
                len,
                version: ObjectVersion::default(),
            });
        }
        self.inner.head(key).await
    }

    async fn put_if_version(
        &self,
        key: &str,
        data: Bytes,
        expected: ExpectedVersion,
    ) -> Result<ObjectInfo, BlobError> {
        self.inner.put_if_version(key, data, expected).await
    }
}
