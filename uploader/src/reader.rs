//! Read-side fallback: once the uploader deletes a local segment file, reads are
//! served by ranged GETs against the uploaded object.
//!
//! Three readers compose here:
//!
//! * [`WorkerSegmentReader`] - local reads through the uploader's I/O thread
//!   (aligned O_DIRECT reads, no page-cache pollution).
//! * [`S3SegmentReader`] - ranged S3 GETs, bounded by the segment digest so a read
//!   never fetches more than the requested blocks plus one footer fetch per
//!   segment.
//! * [`FallbackSegmentReader`] - picks local or remote per [crate::index] state:
//!   a segment is local only while it is not `Deleted` and its file exists.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bkxpro_codec::{parse_block, CodecError, BLOCK_SIZE, HEADER_SIZE};
use bkxpro_indexer::{Index, SegmentMeta, SegmentState};
use bkxpro_reader::{ReadCacheError, SegmentReader};
use bytes::Bytes;

use crate::{
    blob::{BlobError, BlobStore},
    digest::{footer_bounds, DigestError, SegmentDigest},
    worker::{SegmentIoWorker, WorkerError},
};

const DEFAULT_DIGEST_CACHE_ENTRIES: usize = 1024;

fn blob_error(segment_id: u64, key: &str, error: BlobError) -> ReadCacheError {
    match error {
        BlobError::NotFound(_) => ReadCacheError::S3Read(format!(
            "segment {segment_id} is missing from the object store at {key}"
        )),
        other => ReadCacheError::S3Read(format!("segment {segment_id} at {key}: {other}")),
    }
}

fn digest_error(segment_id: u64, key: &str, error: DigestError) -> ReadCacheError {
    ReadCacheError::S3Read(format!(
        "segment {segment_id} at {key} failed digest validation: {error}"
    ))
}

fn worker_error(segment_id: u64, error: WorkerError) -> ReadCacheError {
    if error.is_not_found() {
        ReadCacheError::SegmentNotFound(segment_id)
    } else {
        ReadCacheError::SegmentIo(error.to_string())
    }
}

/// Reads segments that live only in the object store, using ranged GETs.
pub struct S3SegmentReader<B: BlobStore> {
    blob: Arc<B>,
    index: Arc<Index>,
    digests: Mutex<HashMap<u64, Arc<SegmentDigest>>>,
    max_digests: usize,
}

impl<B: BlobStore> S3SegmentReader<B> {
    pub fn new(blob: Arc<B>, index: Arc<Index>) -> Self {
        Self {
            blob,
            index,
            digests: Mutex::new(HashMap::new()),
            max_digests: DEFAULT_DIGEST_CACHE_ENTRIES,
        }
    }

    /// Bound the number of cached digests; a full cache is cleared rather than
    /// evicted per entry, which is cheap and keeps the hot path allocation-free.
    pub fn with_digest_cache_entries(mut self, entries: usize) -> Self {
        self.max_digests = entries.max(1);
        self
    }

    /// Object key of a segment, resolved from the index.
    fn key_for(&self, segment_id: u64) -> Result<String, ReadCacheError> {
        match self.index.get_segment(segment_id)? {
            Some(meta) => meta.s3_key.ok_or_else(|| {
                ReadCacheError::S3Read(format!("segment {segment_id} has no recorded S3 key"))
            }),
            None => Err(ReadCacheError::SegmentNotFound(segment_id)),
        }
    }

    /// Cached (or freshly fetched) digest of a remote segment.
    pub async fn digest(&self, segment_id: u64) -> Result<Arc<SegmentDigest>, ReadCacheError> {
        if let Some(digest) = self.digests.lock().unwrap().get(&segment_id) {
            return Ok(Arc::clone(digest));
        }
        let key = self.key_for(segment_id)?;
        let digest = Arc::new(self.fetch_digest(segment_id, &key).await?);
        let mut cache = self.digests.lock().unwrap();
        if cache.len() >= self.max_digests {
            cache.clear();
        }
        cache.insert(segment_id, Arc::clone(&digest));
        Ok(digest)
    }

    /// Fetch the header block, trailer block, and footer and validate them.
    async fn fetch_digest(
        &self,
        segment_id: u64,
        key: &str,
    ) -> Result<SegmentDigest, ReadCacheError> {
        let info = self
            .blob
            .head(key)
            .await
            .map_err(|error| blob_error(segment_id, key, error))?;
        let file_len = info.len;
        let minimum = (HEADER_SIZE + BLOCK_SIZE) as u64;
        if file_len < minimum || file_len % BLOCK_SIZE as u64 != 0 {
            return Err(ReadCacheError::S3Read(format!(
                "segment {segment_id} at {key} is {file_len} bytes, which is not a sealed segment"
            )));
        }
        let header = self
            .blob
            .get_range(key, 0..BLOCK_SIZE as u64)
            .await
            .map_err(|error| blob_error(segment_id, key, error))?;
        let tail = self
            .blob
            .get_range(key, file_len - BLOCK_SIZE as u64..file_len)
            .await
            .map_err(|error| blob_error(segment_id, key, error))?;
        let (footer_start, footer_len) =
            footer_bounds(&tail, file_len).map_err(|error| digest_error(segment_id, key, error))?;
        let footer = self
            .blob
            .get_range(key, footer_start..footer_start + footer_len as u64)
            .await
            .map_err(|error| blob_error(segment_id, key, error))?;
        SegmentDigest::parse(file_len, &header, &tail, footer)
            .map_err(|error| digest_error(segment_id, key, error))
    }

    /// Fetch `[offset, offset + length)` out of the record data region with one
    /// ranged GET, validating every complete 4 KiB block it returns.
    async fn read_data(
        &self,
        segment_id: u64,
        key: &str,
        digest: &SegmentDigest,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ReadCacheError> {
        let data_end = digest.data_end();
        if offset >= data_end {
            return Err(ReadCacheError::S3Read(format!(
                "segment {segment_id} read starts at {offset} past the data region end {data_end}"
            )));
        }
        let aligned_offset = offset & !(BLOCK_SIZE as u64 - 1);
        let requested_end = offset.saturating_add(length as u64);
        let aligned_end = requested_end
            .div_ceil(BLOCK_SIZE as u64)
            .saturating_mul(BLOCK_SIZE as u64)
            .min(data_end);
        if aligned_end <= aligned_offset {
            return Err(ReadCacheError::S3Read(format!(
                "segment {segment_id} has no complete block at offset {offset} (data region ends at {data_end})"
            )));
        }
        let bytes = self
            .blob
            .get_range(key, aligned_offset..aligned_end)
            .await
            .map_err(|error| blob_error(segment_id, key, error))?;
        let expected = (aligned_end - aligned_offset) as usize;
        if bytes.len() != expected {
            return Err(ReadCacheError::S3Read(format!(
                "segment {segment_id} ranged GET returned {} bytes for a {expected}-byte range",
                bytes.len()
            )));
        }

        // Validate the complete data blocks in the fetched range. Blocks that are
        // not fully inside the data region (the header block and any partially
        // requested tail) are not block-checksummed by the codec, and all-zero
        // padding is tolerated because a sealed file may legitimately contain it.
        for (index, block_start) in (aligned_offset..aligned_end)
            .step_by(BLOCK_SIZE)
            .enumerate()
        {
            if block_start < HEADER_SIZE as u64 || block_start + BLOCK_SIZE as u64 > data_end {
                continue;
            }
            let start = index * BLOCK_SIZE;
            let block = &bytes[start..start + BLOCK_SIZE];
            match parse_block(block) {
                Ok(_) => {}
                Err(CodecError::InvalidField("zero block")) => {}
                Err(error) => {
                    let message = format!(
                        "segment {segment_id} block at offset {block_start} failed validation: {error}"
                    );
                    return Err(ReadCacheError::S3Read(message));
                }
            }
        }

        let start = (offset - aligned_offset) as usize;
        let end = start + (requested_end.min(data_end) - offset) as usize;
        Ok(bytes.slice(start..end))
    }

    /// Drop every cached digest, for example after an object was rewritten.
    pub fn invalidate_digests(&self) {
        self.digests.lock().unwrap().clear();
    }

    /// Number of cached digests.
    pub fn cached_digests(&self) -> usize {
        self.digests.lock().unwrap().len()
    }
}

#[async_trait]
impl<B: BlobStore> SegmentReader for S3SegmentReader<B> {
    async fn read_segment_range(
        &self,
        segment_id: u64,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ReadCacheError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let digest = self.digest(segment_id).await?;
        let key = self.key_for(segment_id)?;
        self.read_data(segment_id, &key, &digest, offset, length)
            .await
    }

    /// True when the object exists in the store (the local file is irrelevant).
    async fn segment_exists(&self, segment_id: u64) -> Result<bool, ReadCacheError> {
        let key = match self.index.get_segment(segment_id)? {
            Some(meta) => match meta.s3_key {
                Some(key) => key,
                None => return Ok(false),
            },
            None => return Ok(false),
        };
        match self.blob.head(&key).await {
            Ok(_) => Ok(true),
            Err(BlobError::NotFound(_)) => Ok(false),
            Err(error) => Err(blob_error(segment_id, &key, error)),
        }
    }

    async fn get_segment_meta(
        &self,
        segment_id: u64,
    ) -> Result<Option<SegmentMeta>, ReadCacheError> {
        Ok(self.index.get_segment(segment_id)?)
    }
}

/// Local segment reads through the uploader's I/O thread.
///
/// The read path never preallocates or truncates: it opens the file read-only and
/// is bounded by the sealed file length, exactly like the writer left it.
pub struct WorkerSegmentReader {
    worker: Arc<SegmentIoWorker>,
    data_dir: PathBuf,
    index: Arc<Index>,
}

impl WorkerSegmentReader {
    pub fn new(worker: Arc<SegmentIoWorker>, data_dir: PathBuf, index: Arc<Index>) -> Self {
        Self {
            worker,
            data_dir,
            index,
        }
    }

    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.data_dir.join(format!("{segment_id}.seg"))
    }
}

#[async_trait]
impl SegmentReader for WorkerSegmentReader {
    async fn read_segment_range(
        &self,
        segment_id: u64,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ReadCacheError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let path = self.segment_path(segment_id);
        let file_len = self
            .worker
            .file_len(&path)
            .await
            .map_err(|error| worker_error(segment_id, error))?;
        if offset >= file_len {
            return Err(ReadCacheError::SegmentIo(format!(
                "read of segment {segment_id} starts at {offset} past end of file ({file_len} bytes)"
            )));
        }

        let aligned_offset = offset & !(BLOCK_SIZE as u64 - 1);
        let aligned_end =
            (offset + length as u64 + BLOCK_SIZE as u64 - 1) & !(BLOCK_SIZE as u64 - 1);
        let read_end = aligned_end.min(file_len & !(BLOCK_SIZE as u64 - 1));
        if read_end <= aligned_offset {
            return Err(ReadCacheError::SegmentIo(format!(
                "segment {segment_id} has no complete block at offset {offset} (file size {file_len})"
            )));
        }
        let extent = self
            .worker
            .read(&path, aligned_offset, (read_end - aligned_offset) as usize)
            .await
            .map_err(|error| worker_error(segment_id, error))?;

        let start = (offset - aligned_offset) as usize;
        let end = (start + length).min(extent.len());
        Ok(extent.slice(start..end))
    }

    async fn segment_exists(&self, segment_id: u64) -> Result<bool, ReadCacheError> {
        Ok(self.segment_path(segment_id).exists())
    }

    async fn get_segment_meta(
        &self,
        segment_id: u64,
    ) -> Result<Option<SegmentMeta>, ReadCacheError> {
        Ok(self.index.get_segment(segment_id)?)
    }
}

/// Local-first reader that falls back to the object store.
///
/// A segment is served locally only while it is not `Deleted` and its file still
/// exists; otherwise the remote reader answers. This is the exact condition the
/// read cache uses before it consults the reader, so both agree on which copy is
/// authoritative.
pub struct FallbackSegmentReader {
    local: Arc<dyn SegmentReader>,
    remote: Arc<dyn SegmentReader>,
}

impl FallbackSegmentReader {
    pub fn new(local: Arc<dyn SegmentReader>, remote: Arc<dyn SegmentReader>) -> Self {
        Self { local, remote }
    }

    /// True when the local copy is authoritative for this segment.
    pub async fn is_local(&self, segment_id: u64) -> Result<bool, ReadCacheError> {
        if let Some(meta) = self.local.get_segment_meta(segment_id).await? {
            if meta.state == SegmentState::Deleted {
                return Ok(false);
            }
        }
        self.local.segment_exists(segment_id).await
    }
}

#[async_trait]
impl SegmentReader for FallbackSegmentReader {
    async fn read_segment_range(
        &self,
        segment_id: u64,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ReadCacheError> {
        if self.is_local(segment_id).await? {
            match self
                .local
                .read_segment_range(segment_id, offset, length)
                .await
            {
                Err(ReadCacheError::SegmentNotFound(_)) => {
                    self.remote
                        .read_segment_range(segment_id, offset, length)
                        .await
                }
                other => other,
            }
        } else {
            self.remote
                .read_segment_range(segment_id, offset, length)
                .await
        }
    }

    async fn segment_exists(&self, segment_id: u64) -> Result<bool, ReadCacheError> {
        self.local.segment_exists(segment_id).await
    }

    async fn get_segment_meta(
        &self,
        segment_id: u64,
    ) -> Result<Option<SegmentMeta>, ReadCacheError> {
        self.local.get_segment_meta(segment_id).await
    }
}

#[cfg(test)]
mod tests {
    use bkxpro_codec::{
        FooterView, SegmentEncoder, SegmentHeader, TrailerWire, RECORD_HEADER_SIZE, TRAILER_SIZE,
    };
    use bkxpro_indexer::{Index, IndexConfig};
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::*;
    use crate::blob::ObjectStoreBlob;

    /// Sealed image with one record per entry; payloads are 32 bytes of `0xA0 + entry`.
    fn sealed_image(segment_id: u64, entries: u64) -> Bytes {
        let header = SegmentHeader {
            segment_id,
            writer_epoch: 9,
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

    /// Absolute payload offset and payload length of one record, read from the
    /// footer. `EntryLocation.offset` (and therefore `read_segment_range`) points
    /// at the payload, so the record header size is added here.
    fn entry_location(image: &Bytes, ledger: u64, entry: u64) -> (u64, u32) {
        let trailer = TrailerWire::parse(&image[image.len() - TRAILER_SIZE..]).unwrap();
        let footer_start = u64::from_le(trailer.footer_start) as usize;
        let footer_len = u32::from_le(trailer.footer_len) as usize;
        let footer =
            FooterView::parse(image.slice(footer_start..footer_start + footer_len)).unwrap();
        let item = footer
            .index_entries()
            .find(|item| item.ledger == ledger && item.entry == entry)
            .unwrap();
        (item.offset + RECORD_HEADER_SIZE as u64, item.len)
    }

    async fn upload(blob: &dyn BlobStore, key: &str, data: &Bytes) {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(crate::blob::StreamItem::Chunk(data.clone()))
            .await
            .unwrap();
        tx.send(crate::blob::StreamItem::End {
            len: data.len() as u64,
            crc32c: crc32c::crc32c(data),
        })
        .await
        .unwrap();
        drop(tx);
        blob.put_stream(key, data.len().max(1), rx).await.unwrap();
    }

    fn env() -> (TempDir, Arc<Index>) {
        let dir = TempDir::new().unwrap();
        let index = Arc::new(
            Index::open(IndexConfig {
                db_path: dir.path().join("index"),
                max_memory_bytes: 64 * 1024 * 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        (dir, index)
    }

    fn put_segment_meta(index: &Index, segment_id: u64, key: Option<&str>, state: SegmentState) {
        let mut batch = index.batch();
        index
            .put_segment(
                &mut batch,
                segment_id,
                SegmentMeta {
                    state,
                    s3_key: key.map(str::to_string),
                    min_ledger: 1,
                    max_ledger: 1,
                    byte_size: 0,
                    writer_epoch: 9,
                },
            )
            .unwrap();
        batch.commit().unwrap();
    }

    #[tokio::test]
    async fn s3_reader_serves_ranges_from_the_object() {
        let (_dir, index) = env();
        let image = sealed_image(21, 3);
        let key = "segments/00000000000000000021.seg";
        let blob = Arc::new(ObjectStoreBlob::new(InMemory::new()));
        upload(blob.as_ref(), key, &image).await;
        put_segment_meta(&index, 21, Some(key), SegmentState::Deleted);

        let reader = S3SegmentReader::new(Arc::clone(&blob), Arc::clone(&index));
        let (offset, len) = entry_location(&image, 1, 2);
        let payload = reader
            .read_segment_range(21, offset, len as usize)
            .await
            .unwrap();
        assert_eq!(
            payload,
            image.slice(offset as usize..offset as usize + len as usize)
        );

        // The digest is cached: a second read must not refetch it.
        let digest_before = reader.cached_digests();
        let again = reader
            .read_segment_range(21, offset, len as usize)
            .await
            .unwrap();
        assert_eq!(again, payload);
        assert_eq!(reader.cached_digests(), digest_before);

        // A short read (fewer bytes than requested) must clamp, not fail.
        let short = reader.read_segment_range(21, offset, 1024).await.unwrap();
        assert!(short.len() <= 1024);
    }

    #[tokio::test]
    async fn s3_reader_detects_corruption_and_missing_objects() {
        let (_dir, index) = env();
        let mut image = sealed_image(22, 2).to_vec();
        let key = "segments/00000000000000000022.seg";
        // Corrupt a data block after sealing.
        image[HEADER_SIZE + 64] ^= 0xff;
        let blob = Arc::new(ObjectStoreBlob::new(InMemory::new()));
        upload(blob.as_ref(), key, &Bytes::from(image.clone())).await;
        put_segment_meta(&index, 22, Some(key), SegmentState::Uploaded);

        let reader = S3SegmentReader::new(Arc::clone(&blob), Arc::clone(&index));
        let error = reader
            .read_segment_range(22, HEADER_SIZE as u64, 32)
            .await
            .unwrap_err();
        assert!(matches!(error, ReadCacheError::S3Read(_)), "{error:?}");

        // An index row without an S3 key cannot be resolved.
        put_segment_meta(&index, 23, None, SegmentState::Sealed);
        let error = reader.read_segment_range(23, 4096, 32).await.unwrap_err();
        assert!(matches!(error, ReadCacheError::S3Read(_)));

        // A segment the index never heard of is not found.
        let error = reader.read_segment_range(24, 4096, 32).await.unwrap_err();
        assert!(matches!(error, ReadCacheError::SegmentNotFound(24)));

        // segment_exists reflects the object store, not the index.
        put_segment_meta(
            &index,
            25,
            Some("segments/does-not-exist.seg"),
            SegmentState::Deleted,
        );
        assert!(!reader.segment_exists(25).await.unwrap());
        assert!(reader.segment_exists(22).await.unwrap());
    }

    #[tokio::test]
    async fn fallback_reader_prefers_local_until_deleted() {
        let (_dir, index) = env();
        let image = sealed_image(31, 1);
        let key = "segments/00000000000000000031.seg";
        let blob = Arc::new(ObjectStoreBlob::new(InMemory::new()));
        upload(blob.as_ref(), key, &image).await;

        // Local file exists; the index says Sealed.
        let local: Arc<dyn SegmentReader> = Arc::new(LocalStub::new(index.clone(), vec![31]));
        let remote: Arc<dyn SegmentReader> = Arc::new(S3SegmentReader::new(blob, index.clone()));
        let reader = FallbackSegmentReader::new(local, remote);

        assert!(reader.is_local(31).await.unwrap());
        let local_bytes = reader.read_segment_range(31, 0, 16).await.unwrap();
        assert_eq!(local_bytes, LocalStub::MAGIC);

        // Once the index marks the segment Deleted, the remote copy answers even
        // if a stale local file is still present.
        put_segment_meta(&index, 31, Some(key), SegmentState::Deleted);
        assert!(!reader.is_local(31).await.unwrap());
        let remote_bytes = reader.read_segment_range(31, 0, 8).await.unwrap();
        assert_eq!(remote_bytes, image.slice(0..8));
    }

    #[tokio::test]
    async fn fallback_reader_uses_remote_when_the_local_file_is_gone() {
        let (_dir, index) = env();
        let image = sealed_image(41, 2);
        let key = "segments/00000000000000000041.seg";
        let blob = Arc::new(ObjectStoreBlob::new(InMemory::new()));
        upload(blob.as_ref(), key, &image).await;
        // Index says Uploaded, but no local file exists (crash after unlink).
        put_segment_meta(&index, 41, Some(key), SegmentState::Uploaded);

        let local: Arc<dyn SegmentReader> = Arc::new(LocalStub::new(index.clone(), vec![]));
        let remote: Arc<dyn SegmentReader> = Arc::new(S3SegmentReader::new(blob, index));
        let reader = FallbackSegmentReader::new(local, remote);
        assert!(!reader.is_local(41).await.unwrap());

        let (offset, len) = entry_location(&image, 1, 1);
        let payload = reader
            .read_segment_range(41, offset, len as usize)
            .await
            .unwrap();
        assert_eq!(payload.len(), len as usize);
        assert_eq!(payload[0], 0xA1);
    }

    /// Minimal local reader used to exercise the fallback selection logic.
    struct LocalStub {
        index: Arc<Index>,
        present: Vec<u64>,
    }

    impl LocalStub {
        const MAGIC: Bytes = Bytes::from_static(b"local-bytes!!!!!");

        fn new(index: Arc<Index>, present: Vec<u64>) -> Self {
            Self { index, present }
        }
    }

    #[async_trait]
    impl SegmentReader for LocalStub {
        async fn read_segment_range(
            &self,
            _segment_id: u64,
            _offset: u64,
            _length: usize,
        ) -> Result<Bytes, ReadCacheError> {
            Ok(Self::MAGIC.slice(0..16))
        }

        async fn segment_exists(&self, segment_id: u64) -> Result<bool, ReadCacheError> {
            Ok(self.present.contains(&segment_id))
        }

        async fn get_segment_meta(
            &self,
            segment_id: u64,
        ) -> Result<Option<SegmentMeta>, ReadCacheError> {
            Ok(self.index.get_segment(segment_id)?)
        }
    }
}
