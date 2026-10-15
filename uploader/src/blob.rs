//! Blob-store seam: the uploader talks to S3 through this trait so every fault
//! path (failed PUT, lost response, CAS conflict) is injectable in tests.
//!
//! The production implementation wraps any [`object_store::ObjectStore`] value,
//! which keeps authentication, TLS, and retry policy out of this crate.

use std::{
    ops::Range,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    path::Path as ObjectPath, Error as ObjectError, ObjectStore, ObjectStoreExt, PutMode,
    PutOptions, UpdateVersion, WriteMultipart,
};
use thiserror::Error;
use tokio::sync::mpsc;

/// One item of a streamed segment upload.
#[derive(Debug)]
pub enum StreamItem {
    /// A contiguous run of segment bytes. Ownership moves to the blob store, so a
    /// whole-file upload never copies the payload.
    Chunk(Bytes),
    /// Terminates a successful stream: total bytes and CRC32C of those bytes.
    End { len: u64, crc32c: u32 },
    /// The local reader failed; the PUT must be aborted, not completed.
    Failed(String),
}

/// Errors surfaced by a [`BlobStore`]. The variant decides whether the caller may
/// retry: transient failures are retried with backoff, fatal ones are not, and
/// conflicts are resolved by re-reading the current version and re-applying.
#[derive(Debug, Error)]
pub enum BlobError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("compare-and-swap conflict on {0}")]
    Conflict(String),
    #[error("transient blob-store error: {0}")]
    Transient(String),
    #[error("fatal blob-store error: {0}")]
    Fatal(String),
    #[error("local read failed during upload: {0}")]
    Source(String),
    #[error("invalid blob-store argument: {0}")]
    Invalid(String),
}

impl BlobError {
    /// True when a retry with backoff (or a fresh stream) can plausibly succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(self, BlobError::Transient(_) | BlobError::Source(_))
    }
}

/// Opaque object version (S3 ETag, and a store-specific version when available).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectVersion {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

impl ObjectVersion {
    /// True when the version carries no information and therefore cannot be used
    /// as a compare-and-swap witness.
    pub fn is_unusable(&self) -> bool {
        self.e_tag.is_none() && self.version.is_none()
    }
}

/// Metadata of a stored object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub len: u64,
    pub version: ObjectVersion,
}

/// A fetched object: payload plus the version it was read at.
#[derive(Debug, Clone)]
pub struct BlobObject {
    pub bytes: Bytes,
    pub info: ObjectInfo,
}

/// Precondition for a conditional write.
#[derive(Debug, Clone)]
pub enum ExpectedVersion {
    /// The object must not exist yet (`If-None-Match: *`).
    MustNotExist,
    /// The object must still be at this version.
    Match(ObjectVersion),
}

/// Result of a streamed PUT.
#[derive(Debug, Clone)]
pub struct PutOutcome {
    pub len: u64,
    pub crc32c: u32,
    pub version: ObjectVersion,
}

/// Minimal object store used by the uploader. Every method is cancellation-safe:
/// dropping the future mid-write must never leave a partially visible object
/// (object stores only publish completed multipart uploads).
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// Stream `chunks` into one object at `key`, splitting into `part_size`
    /// multipart parts. Consumes the receiver; a retry must start a fresh stream.
    ///
    /// Each [`StreamItem::Chunk`] holds a byte-budget permit until it is dropped, so an
    /// implementation must release the chunk as soon as it owns the bytes (copy them
    /// into a part buffer, or drain them) instead of retaining it for the lifetime of
    /// the upload.
    async fn put_stream(
        &self,
        key: &str,
        part_size: usize,
        chunks: mpsc::Receiver<StreamItem>,
    ) -> Result<PutOutcome, BlobError>;

    /// Fetch one whole object.
    async fn get(&self, key: &str) -> Result<BlobObject, BlobError>;

    /// Fetch a byte range of an object.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, BlobError>;

    /// Fetch object metadata without the payload.
    async fn head(&self, key: &str) -> Result<ObjectInfo, BlobError>;

    /// Conditional write used by the manifest append.
    async fn put_if_version(
        &self,
        key: &str,
        data: Bytes,
        expected: ExpectedVersion,
    ) -> Result<ObjectInfo, BlobError>;
}

/// `Arc<B>` is itself a store. This is what lets a manifest log and an uploader
/// share one handle (including `Arc<dyn BlobStore>`) without an extra indirection.
#[async_trait]
impl<B: BlobStore + ?Sized> BlobStore for Arc<B> {
    async fn put_stream(
        &self,
        key: &str,
        part_size: usize,
        chunks: mpsc::Receiver<StreamItem>,
    ) -> Result<PutOutcome, BlobError> {
        (**self).put_stream(key, part_size, chunks).await
    }

    async fn get(&self, key: &str) -> Result<BlobObject, BlobError> {
        (**self).get(key).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, BlobError> {
        (**self).get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectInfo, BlobError> {
        (**self).head(key).await
    }

    async fn put_if_version(
        &self,
        key: &str,
        data: Bytes,
        expected: ExpectedVersion,
    ) -> Result<ObjectInfo, BlobError> {
        (**self).put_if_version(key, data, expected).await
    }
}

/// [`BlobStore`] over any [`object_store::ObjectStore`].
pub struct ObjectStoreBlob<S: ObjectStore> {
    store: S,
    write_concurrency: usize,
}

impl<S: ObjectStore> ObjectStoreBlob<S> {
    /// Wrap a store (for example `AmazonS3` built with `AmazonS3Builder`).
    pub fn new(store: S) -> Self {
        Self {
            store,
            write_concurrency: 4,
        }
    }

    /// Number of multipart parts uploaded concurrently per object.
    pub fn with_write_concurrency(mut self, concurrency: usize) -> Self {
        self.write_concurrency = concurrency.max(1);
        self
    }

    /// Map an object-store error onto the uploader's retry/fatal classification.
    fn map_error(key: &str, error: ObjectError) -> BlobError {
        let detail = error.to_string();
        match error {
            ObjectError::NotFound { .. } => BlobError::NotFound(key.to_string()),
            ObjectError::Precondition { .. }
            | ObjectError::AlreadyExists { .. }
            | ObjectError::NotModified { .. } => BlobError::Conflict(format!("{key}: {detail}")),
            ObjectError::NotSupported { .. }
            | ObjectError::NotImplemented { .. }
            | ObjectError::Unauthenticated { .. }
            | ObjectError::PermissionDenied { .. }
            | ObjectError::InvalidPath { .. }
            | ObjectError::UnknownConfigurationKey { .. } => {
                BlobError::Fatal(format!("{key}: {detail}"))
            }
            ObjectError::Generic { .. } | ObjectError::JoinError { .. } => {
                BlobError::Transient(format!("{key}: {detail}"))
            }
            // `object_store::Error` is `#[non_exhaustive]`: an unrecognized variant
            // is retried, which is always safe (the uploader never deletes local
            // bytes without a verified upload) but may be slow to give up.
            _ => BlobError::Transient(format!("{key}: {detail}")),
        }
    }

    fn info(key: &str, len: u64, version: ObjectVersion) -> ObjectInfo {
        ObjectInfo {
            key: key.to_string(),
            len,
            version,
        }
    }
}

fn version_of(result: &object_store::PutResult) -> ObjectVersion {
    ObjectVersion {
        e_tag: result.e_tag.clone(),
        version: result.version.clone(),
    }
}

#[async_trait]
impl<S: ObjectStore> BlobStore for ObjectStoreBlob<S> {
    async fn put_stream(
        &self,
        key: &str,
        part_size: usize,
        mut chunks: mpsc::Receiver<StreamItem>,
    ) -> Result<PutOutcome, BlobError> {
        if part_size == 0 {
            return Err(BlobError::Invalid("part_size must be non-zero".to_string()));
        }
        let path = ObjectPath::from(key);
        let upload = self
            .store
            .put_multipart(&path)
            .await
            .map_err(|error| Self::map_error(key, error))?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, part_size);
        let mut written = 0u64;

        loop {
            // Keep at most `write_concurrency` parts in flight. This is the second
            // half of the backpressure story: the reader thread blocks on the byte
            // budget, the writer awaits in-flight capacity.
            if let Err(error) = writer.wait_for_capacity(self.write_concurrency).await {
                let _ = writer.abort().await;
                return Err(Self::map_error(key, error));
            }
            match chunks.recv().await {
                Some(StreamItem::Chunk(chunk)) => {
                    written += chunk.len() as u64;
                    // Copy into the writer's part buffer instead of moving the chunk in.
                    // A chunk carries its byte-budget permit until it is dropped, and a
                    // store may hold every uploaded part until `complete()` (the
                    // `InMemory` store does). Moving the chunk into such a store would
                    // let it keep the read permits while the reader waits for budget, so
                    // a segment larger than `max_inflight_bytes` would stall: copying
                    // releases the reader as soon as the part buffer owns the bytes, and
                    // store-side parts stay bounded by `write_concurrency` instead.
                    writer.write(&chunk);
                }
                Some(StreamItem::End { len, crc32c }) => {
                    if len != written {
                        let _ = writer.abort().await;
                        return Err(BlobError::Source(format!(
                            "upload stream ended after {written} bytes but reported {len}"
                        )));
                    }
                    return match writer.finish().await {
                        Ok(result) => Ok(PutOutcome {
                            len: written,
                            crc32c,
                            version: version_of(&result),
                        }),
                        Err(error) => Err(Self::map_error(key, error)),
                    };
                }
                Some(StreamItem::Failed(message)) => {
                    let _ = writer.abort().await;
                    return Err(BlobError::Source(message));
                }
                None => {
                    // Teardown of the reader (or a panic) without an End marker:
                    // never publish a truncated object.
                    let _ = writer.abort().await;
                    return Err(BlobError::Source(
                        "upload stream closed before the end marker".to_string(),
                    ));
                }
            }
        }
    }

    async fn get(&self, key: &str) -> Result<BlobObject, BlobError> {
        let path = ObjectPath::from(key);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|error| Self::map_error(key, error))?;
        let len = result.meta.size;
        let version = ObjectVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| Self::map_error(key, error))?;
        Ok(BlobObject {
            bytes,
            info: Self::info(key, len, version),
        })
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, BlobError> {
        let path = ObjectPath::from(key);
        self.store
            .get_range(&path, range)
            .await
            .map_err(|error| Self::map_error(key, error))
    }

    async fn head(&self, key: &str) -> Result<ObjectInfo, BlobError> {
        let path = ObjectPath::from(key);
        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|error| Self::map_error(key, error))?;
        Ok(Self::info(
            key,
            meta.size,
            ObjectVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            },
        ))
    }

    async fn put_if_version(
        &self,
        key: &str,
        data: Bytes,
        expected: ExpectedVersion,
    ) -> Result<ObjectInfo, BlobError> {
        let mode = match expected {
            ExpectedVersion::MustNotExist => PutMode::Create,
            ExpectedVersion::Match(version) => {
                if version.is_unusable() {
                    return Err(BlobError::Invalid(format!(
                        "{key}: conditional write needs an ETag or version to match"
                    )));
                }
                PutMode::Update(UpdateVersion {
                    e_tag: version.e_tag,
                    version: version.version,
                })
            }
        };
        let len = data.len() as u64;
        let options = PutOptions {
            mode,
            ..Default::default()
        };
        let path = ObjectPath::from(key);
        match self
            .store
            .put_opts(&path, data.into(), options)
            .await
            .map_err(|error| Self::map_error(key, error))
        {
            Ok(result) => Ok(Self::info(key, len, version_of(&result))),
            Err(error) => Err(error),
        }
    }
}

/// Fault plan for [`FaultyBlob`]. Each counter is consumed by one call; a
/// persistent PUT failure keeps failing until cleared.
#[derive(Debug, Clone, Default)]
pub struct FaultPlan {
    /// PUTs that fail transiently before writing anything.
    pub put_transient: usize,
    /// PUTs that write the object but report a transient error (lost response).
    pub put_lost_response: usize,
    /// PUTs that keep failing until the plan is cleared.
    pub put_persistent_failure: bool,
    /// CAS writes that fail transiently without writing.
    pub cas_transient: usize,
    /// CAS writes that lose the race and return a conflict without writing.
    pub cas_conflict: usize,
    /// CAS writes that land but report a transient error (lost response).
    pub cas_lost_response: usize,
    /// Whole-object GETs that fail transiently.
    pub get_transient: usize,
}

/// Test double that injects deterministic faults into any [`BlobStore`] and
/// counts call attempts so tests can assert retry behaviour.
pub struct FaultyBlob {
    inner: Arc<dyn BlobStore>,
    plan: Mutex<FaultPlan>,
    put_attempts: AtomicU64,
    cas_attempts: AtomicU64,
    get_attempts: AtomicU64,
    bytes_written: AtomicU64,
}

impl FaultyBlob {
    pub fn new(inner: Arc<dyn BlobStore>) -> Self {
        Self {
            inner,
            plan: Mutex::new(FaultPlan::default()),
            put_attempts: AtomicU64::new(0),
            cas_attempts: AtomicU64::new(0),
            get_attempts: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }

    /// Replace the whole plan.
    pub fn set_plan(&self, plan: FaultPlan) {
        *self.plan.lock().unwrap() = plan;
    }

    /// Mutate the plan in place.
    pub fn with_plan(&self, edit: impl FnOnce(&mut FaultPlan)) {
        edit(&mut self.plan.lock().unwrap());
    }

    /// Underlying store, for reading back uploaded objects.
    pub fn inner(&self) -> &Arc<dyn BlobStore> {
        &self.inner
    }

    pub fn put_attempts(&self) -> u64 {
        self.put_attempts.load(Ordering::SeqCst)
    }

    pub fn cas_attempts(&self) -> u64 {
        self.cas_attempts.load(Ordering::SeqCst)
    }

    pub fn get_attempts(&self) -> u64 {
        self.get_attempts.load(Ordering::SeqCst)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::SeqCst)
    }

    fn take(plan: &Mutex<FaultPlan>, field: fn(&mut FaultPlan) -> &mut usize) -> bool {
        let mut plan = plan.lock().unwrap();
        let counter = field(&mut plan);
        if *counter > 0 {
            *counter -= 1;
            true
        } else {
            false
        }
    }

    /// Drain a stream into a fresh channel so the inner store can consume it while
    /// this wrapper remembers that the response was lost.
    fn tee(mut chunks: mpsc::Receiver<StreamItem>, capacity: usize) -> mpsc::Receiver<StreamItem> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        tokio::spawn(async move {
            while let Some(item) = chunks.recv().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}

#[async_trait]
impl BlobStore for FaultyBlob {
    async fn put_stream(
        &self,
        key: &str,
        part_size: usize,
        chunks: mpsc::Receiver<StreamItem>,
    ) -> Result<PutOutcome, BlobError> {
        self.put_attempts.fetch_add(1, Ordering::SeqCst);
        let persistent = self.plan.lock().unwrap().put_persistent_failure;
        if persistent || Self::take(&self.plan, |plan| &mut plan.put_transient) {
            // Drop the receiver: the reader stops at the next send, exactly like a
            // connection reset while the upload was still probing.
            drop(chunks);
            return Err(BlobError::Transient(
                "injected transient PUT failure".to_string(),
            ));
        }
        if Self::take(&self.plan, |plan| &mut plan.put_lost_response) {
            let outcome = self
                .inner
                .put_stream(key, part_size, Self::tee(chunks, 4))
                .await?;
            self.bytes_written.fetch_add(outcome.len, Ordering::SeqCst);
            return Err(BlobError::Transient(
                "injected lost PUT response".to_string(),
            ));
        }
        let outcome = self.inner.put_stream(key, part_size, chunks).await?;
        self.bytes_written.fetch_add(outcome.len, Ordering::SeqCst);
        Ok(outcome)
    }

    async fn get(&self, key: &str) -> Result<BlobObject, BlobError> {
        self.get_attempts.fetch_add(1, Ordering::SeqCst);
        if Self::take(&self.plan, |plan| &mut plan.get_transient) {
            return Err(BlobError::Transient(
                "injected transient GET failure".to_string(),
            ));
        }
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, BlobError> {
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectInfo, BlobError> {
        self.inner.head(key).await
    }

    async fn put_if_version(
        &self,
        key: &str,
        data: Bytes,
        expected: ExpectedVersion,
    ) -> Result<ObjectInfo, BlobError> {
        self.cas_attempts.fetch_add(1, Ordering::SeqCst);
        if Self::take(&self.plan, |plan| &mut plan.cas_conflict) {
            return Err(BlobError::Conflict(format!("{key}: injected CAS conflict")));
        }
        if Self::take(&self.plan, |plan| &mut plan.cas_transient) {
            return Err(BlobError::Transient(
                "injected transient CAS failure".to_string(),
            ));
        }
        if Self::take(&self.plan, |plan| &mut plan.cas_lost_response) {
            let info = self.inner.put_if_version(key, data, expected).await?;
            return Err(BlobError::Transient(format!(
                "{}: injected lost CAS response",
                info.key
            )));
        }
        self.inner.put_if_version(key, data, expected).await
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use tokio::sync::mpsc;

    use super::*;

    fn memory_blob() -> Arc<dyn BlobStore> {
        Arc::new(ObjectStoreBlob::new(InMemory::new()))
    }

    async fn put_bytes(
        blob: &dyn BlobStore,
        key: &str,
        data: &[u8],
    ) -> Result<PutOutcome, BlobError> {
        let (tx, rx) = mpsc::channel(2);
        let len = data.len() as u64;
        let crc = crc32c::crc32c(data);
        let payload = data.to_vec();
        // Feed from a separate task: the channel is bounded, so sending inline
        // would deadlock as soon as the payload outgrows it.
        let feeder = tokio::spawn(async move {
            for chunk in payload.chunks(4096) {
                if tx
                    .send(StreamItem::Chunk(Bytes::copy_from_slice(chunk)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = tx.send(StreamItem::End { len, crc32c: crc }).await;
        });
        let outcome = blob.put_stream(key, 4096, rx).await;
        let _ = feeder.await;
        outcome
    }

    #[tokio::test]
    async fn put_stream_round_trips_and_reports_checksum() {
        let blob = memory_blob();
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let outcome = put_bytes(blob.as_ref(), "segments/1.seg", &data)
            .await
            .unwrap();
        assert_eq!(outcome.len, data.len() as u64);
        assert_eq!(outcome.crc32c, crc32c::crc32c(&data));

        let fetched = blob.get("segments/1.seg").await.unwrap();
        assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        assert_eq!(fetched.info.len, data.len() as u64);

        let range = blob.get_range("segments/1.seg", 10..20).await.unwrap();
        assert_eq!(range.as_ref(), &data[10..20]);
    }

    #[tokio::test]
    async fn put_stream_multi_part_splits_at_part_size() {
        let blob = memory_blob();
        let data = vec![7u8; 4096 * 5];
        let outcome = put_bytes(blob.as_ref(), "segments/2.seg", &data)
            .await
            .unwrap();
        assert_eq!(outcome.len, data.len() as u64);
        assert_eq!(
            blob.get("segments/2.seg").await.unwrap().bytes.len(),
            data.len()
        );
    }

    #[tokio::test]
    async fn put_stream_rejects_short_stream() {
        let blob = memory_blob();
        let (tx, rx) = mpsc::channel(4);
        tx.send(StreamItem::Chunk(Bytes::from_static(b"abc")))
            .await
            .unwrap();
        tx.send(StreamItem::End { len: 5, crc32c: 0 })
            .await
            .unwrap();
        drop(tx);
        let error = blob
            .put_stream("segments/3.seg", 4096, rx)
            .await
            .unwrap_err();
        assert!(matches!(error, BlobError::Source(_)));
        assert!(blob.head("segments/3.seg").await.is_err());
    }

    #[tokio::test]
    async fn put_stream_propagates_reader_failure() {
        let blob = memory_blob();
        let (tx, rx) = mpsc::channel(4);
        tx.send(StreamItem::Failed("disk went away".to_string()))
            .await
            .unwrap();
        drop(tx);
        let error = blob
            .put_stream("segments/4.seg", 4096, rx)
            .await
            .unwrap_err();
        assert!(matches!(error, BlobError::Source(_)));
    }

    #[tokio::test]
    async fn conditional_write_enforces_version() {
        let blob = memory_blob();
        let initial = blob
            .put_if_version(
                "manifest/log.jsonl",
                Bytes::from_static(b"first\n"),
                ExpectedVersion::MustNotExist,
            )
            .await
            .unwrap();

        let conflict = blob
            .put_if_version(
                "manifest/log.jsonl",
                Bytes::from_static(b"second\n"),
                ExpectedVersion::MustNotExist,
            )
            .await
            .unwrap_err();
        assert!(matches!(conflict, BlobError::Conflict(_)));

        blob.put_if_version(
            "manifest/log.jsonl",
            Bytes::from_static(b"first\nsecond\n"),
            ExpectedVersion::Match(initial.version.clone()),
        )
        .await
        .unwrap();

        let stale = blob
            .put_if_version(
                "manifest/log.jsonl",
                Bytes::from_static(b"stale\n"),
                ExpectedVersion::Match(initial.version),
            )
            .await
            .unwrap_err();
        assert!(matches!(stale, BlobError::Conflict(_)));

        let versionless = blob
            .put_if_version(
                "manifest/log.jsonl",
                Bytes::from_static(b"x"),
                ExpectedVersion::Match(ObjectVersion::default()),
            )
            .await
            .unwrap_err();
        assert!(matches!(versionless, BlobError::Invalid(_)));
    }

    #[tokio::test]
    async fn error_classification() {
        let not_found = ObjectStoreBlob::<InMemory>::map_error(
            "k",
            ObjectError::NotFound {
                path: "k".to_string(),
                source: "gone".into(),
            },
        );
        assert!(matches!(not_found, BlobError::NotFound(_)));

        let precondition = ObjectStoreBlob::<InMemory>::map_error(
            "k",
            ObjectError::Precondition {
                path: "k".to_string(),
                source: "etag mismatch".into(),
            },
        );
        assert!(matches!(precondition, BlobError::Conflict(_)));

        let permission = ObjectStoreBlob::<InMemory>::map_error(
            "k",
            ObjectError::PermissionDenied {
                path: "k".to_string(),
                source: "denied".into(),
            },
        );
        assert!(matches!(permission, BlobError::Fatal(_)));

        let generic = ObjectStoreBlob::<InMemory>::map_error(
            "k",
            ObjectError::Generic {
                store: "test",
                source: "flaky".into(),
            },
        );
        assert!(generic.is_retryable());
    }

    #[tokio::test]
    async fn faulty_blob_supports_lost_response_and_conflicts() {
        let inner = memory_blob();
        let faulty = FaultyBlob::new(inner);
        faulty.with_plan(|plan| {
            plan.put_lost_response = 1;
            plan.cas_conflict = 1;
        });

        let outcome = put_bytes(&faulty, "segments/9.seg", b"payload").await;
        assert!(matches!(outcome.unwrap_err(), BlobError::Transient(_)));
        // The lost response still wrote the object.
        assert_eq!(
            faulty.inner().get("segments/9.seg").await.unwrap().bytes,
            Bytes::from_static(b"payload")
        );
        assert_eq!(faulty.put_attempts(), 1);

        let outcome = put_bytes(&faulty, "segments/10.seg", b"payload2").await;
        assert_eq!(outcome.unwrap().len, 8);

        let conflict = faulty
            .put_if_version(
                "manifest/log.jsonl",
                Bytes::from_static(b"row\n"),
                ExpectedVersion::MustNotExist,
            )
            .await
            .unwrap_err();
        assert!(matches!(conflict, BlobError::Conflict(_)));
        assert_eq!(faulty.cas_attempts(), 1);
    }
}
