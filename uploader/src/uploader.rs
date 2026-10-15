//! Streaming uploader with manifest-gated local deletion.
//!
//! One upload of one segment is a fixed sequence; every step is idempotent, so a
//! crash between any two steps is repaired by the next pass:
//!
//! 1. `stat` - validate the sealed file through the I/O worker and read its digest
//!    (writer epoch, ledger range, length). Corrupt bytes fail-stop here.
//! 2. mark `Uploading` with the S3 key in the index (local intent record).
//! 3. PUT - stream the file as one object, retrying with backoff. The object store
//!    only publishes a completed multipart upload, so a torn attempt is invisible.
//! 4. verify - HEAD the object and require the expected length.
//! 5. manifest CAS append - the durable proof that the bytes are in the object
//!    store. A conflict means a competing writer (or a lost response); both are
//!    resolved by re-reading the log. A newer writer epoch fences us out.
//! 6. mark `Uploaded`, `unlink` the local file (plus directory fsync), mark
//!    `Deleted`, and keep the entry rows plus the S3 key so reads fall back to
//!    ranged GETs. Entry rows are only removed by an explicit retention purge.
//!
//! The uploader is fail-stop: a missing local file with no manifest record, a
//! fenced epoch, a corrupt segment, or an upload that does not match the
//! committed manifest record poisons it until an operator intervenes. It never
//! deletes bytes it has not just proven to be in the object store.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bkxpro_indexer::{Index, SegmentMeta, SegmentState};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{
    blob::{BlobStore, PutOutcome},
    budget::ByteBudget,
    config::UploaderConfig,
    digest::SegmentDigest,
    manifest::{CommitOutcome, ManifestLog, ManifestRecord},
    worker::{SegmentIoWorker, WorkerError},
    UploaderError,
};

/// Outcome of one segment's advance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Advance {
    /// PUT + manifest + local delete all completed.
    Uploaded,
    /// A manifest record already existed; only the local delete was needed.
    AlreadyRemote,
    /// The local file was already gone and the manifest records the segment.
    AlreadyDeleted,
    /// The segment still needs an upload; it stays local until then.
    PendingUpload,
}

/// Per-pass report.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UploadReport {
    pub examined: usize,
    pub uploaded: usize,
    pub already_remote: usize,
    pub already_deleted: usize,
    pub pending: usize,
}

impl UploadReport {
    /// Segments that reached `Deleted` during this pass.
    pub fn deleted(&self) -> usize {
        self.uploaded + self.already_remote + self.already_deleted
    }
}

/// Recovery scan report.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub scanned: usize,
    pub pending_uploads: usize,
    pub reconciled_deleted: usize,
    pub reconciled_uploaded: usize,
}

/// Counters exported by [`SegmentUploader::stats`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UploaderStats {
    pub segments_uploaded: u64,
    pub segments_deleted: u64,
    pub bytes_uploaded: u64,
    pub put_attempts: u64,
    pub put_retries: u64,
    pub put_failures: u64,
    pub manifest_commits: u64,
    pub cas_conflicts: u64,
    pub already_committed: u64,
    pub recovered_deletes: u64,
    pub fenced: u64,
    pub missing_local: u64,
}

#[derive(Debug, Default)]
struct Counters {
    segments_uploaded: AtomicU64,
    segments_deleted: AtomicU64,
    bytes_uploaded: AtomicU64,
    put_attempts: AtomicU64,
    put_retries: AtomicU64,
    put_failures: AtomicU64,
    manifest_commits: AtomicU64,
    cas_conflicts: AtomicU64,
    already_committed: AtomicU64,
    recovered_deletes: AtomicU64,
    fenced: AtomicU64,
    missing_local: AtomicU64,
}

/// The uploader. Share it across tasks with `Arc`.
pub struct SegmentUploader<B: BlobStore> {
    config: UploaderConfig,
    index: Arc<Index>,
    blob: Arc<B>,
    worker: Arc<SegmentIoWorker>,
    manifest: ManifestLog<Arc<B>>,
    budget: Arc<ByteBudget>,
    counters: Counters,
    poison: Mutex<Option<String>>,
}

impl<B: BlobStore> SegmentUploader<B> {
    /// Build an uploader. The worker must already own a live I/O backend.
    pub fn new(
        config: UploaderConfig,
        index: Arc<Index>,
        blob: Arc<B>,
        worker: Arc<SegmentIoWorker>,
    ) -> Result<Self, UploaderError> {
        config.validate().map_err(UploaderError::Config)?;
        let budget = ByteBudget::new(config.max_inflight_bytes);
        let manifest = ManifestLog::new(
            Arc::clone(&blob),
            config.manifest_key.clone(),
            config.manifest_max_cas_retries,
        );
        Ok(Self {
            config,
            index,
            blob,
            worker,
            manifest,
            budget,
            counters: Counters::default(),
            poison: Mutex::new(None),
        })
    }

    pub fn config(&self) -> &UploaderConfig {
        &self.config
    }

    /// Byte budget bounding in-flight read chunks; the reader blocks on it, so it
    /// is the uploader's backpressure control.
    pub fn budget(&self) -> &Arc<ByteBudget> {
        &self.budget
    }

    pub fn stats(&self) -> UploaderStats {
        let counters = &self.counters;
        UploaderStats {
            segments_uploaded: counters.segments_uploaded.load(Ordering::SeqCst),
            segments_deleted: counters.segments_deleted.load(Ordering::SeqCst),
            bytes_uploaded: counters.bytes_uploaded.load(Ordering::SeqCst),
            put_attempts: counters.put_attempts.load(Ordering::SeqCst),
            put_retries: counters.put_retries.load(Ordering::SeqCst),
            put_failures: counters.put_failures.load(Ordering::SeqCst),
            manifest_commits: counters.manifest_commits.load(Ordering::SeqCst),
            cas_conflicts: counters.cas_conflicts.load(Ordering::SeqCst),
            already_committed: counters.already_committed.load(Ordering::SeqCst),
            recovered_deletes: counters.recovered_deletes.load(Ordering::SeqCst),
            fenced: counters.fenced.load(Ordering::SeqCst),
            missing_local: counters.missing_local.load(Ordering::SeqCst),
        }
    }

    /// True once a fatal inconsistency was observed; no further work is done.
    pub fn is_poisoned(&self) -> bool {
        self.poison.lock().unwrap().is_some()
    }

    /// Reason the uploader stopped, if any.
    pub fn poison_reason(&self) -> Option<String> {
        self.poison.lock().unwrap().clone()
    }

    fn poison(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let mut slot = self.poison.lock().unwrap();
        if slot.is_none() {
            warn!(reason = reason.as_str(), "uploader poisoned; fail-stop");
            *slot = Some(reason);
        }
    }

    fn check_healthy(&self) -> Result<(), UploaderError> {
        match self.poison_reason() {
            Some(reason) => Err(UploaderError::Poisoned { reason }),
            None => Ok(()),
        }
    }

    /// Scan the index and repair the interrupted windows of a previous process:
    /// finish uploads that were committed, mark locally-deleted segments, and
    /// fail-stop on a missing local file that no manifest record covers.
    pub async fn recover(&self) -> Result<RecoveryReport, UploaderError> {
        self.check_healthy()?;
        let manifest = self.manifest.refresh().await?;
        let mut report = RecoveryReport::default();

        for (segment_id, meta) in self.index.iter_segments()? {
            if meta.state == SegmentState::Open || meta.state == SegmentState::Deleted {
                continue;
            }
            report.scanned += 1;
            let path = self.config.segment_path(segment_id);
            match self.worker.stat(&path, segment_id).await {
                Ok(digest) => {
                    let record = manifest.find(segment_id).cloned();
                    match record {
                        None => report.pending_uploads += 1,
                        Some(record) if record.writer_epoch < digest.writer_epoch => {
                            report.pending_uploads += 1;
                        }
                        Some(record) => {
                            let key = self.record_key(segment_id, digest.writer_epoch);
                            self.check_record(segment_id, &record, &digest, &key)?;
                            self.mark_uploaded(segment_id, &record)?;
                            if meta.state != SegmentState::Uploaded {
                                report.reconciled_uploaded += 1;
                            }
                            self.delete_local(segment_id, &path).await?;
                            report.reconciled_deleted += 1;
                        }
                    }
                }
                Err(error) if error.is_not_found() => match manifest.find(segment_id) {
                    Some(record) => {
                        self.mark_uploaded(segment_id, record)?;
                        self.mark_deleted(segment_id)?;
                        report.reconciled_deleted += 1;
                    }
                    None => {
                        self.counters.missing_local.fetch_add(1, Ordering::SeqCst);
                        let error = UploaderError::LocalSegmentMissing { segment_id };
                        self.poison(error.to_string());
                        return Err(error);
                    }
                },
                Err(error) => return Err(UploaderError::Worker(error)),
            }
        }
        Ok(report)
    }

    /// One upload pass over every segment that still needs remote durability.
    pub async fn process_pending(&self) -> Result<UploadReport, UploaderError> {
        self.check_healthy()?;
        // A fresh manifest view: the cached copy may predate another writer or a
        // previous process, and every deletion decision reads from it.
        let manifest = self.manifest.refresh().await?;
        let mut pending = Vec::new();
        for (segment_id, meta) in self.index.iter_segments()? {
            if matches!(
                meta.state,
                SegmentState::Sealed | SegmentState::Uploading | SegmentState::Uploaded
            ) {
                pending.push((segment_id, meta));
            }
        }

        let mut report = UploadReport {
            examined: pending.len(),
            ..Default::default()
        };
        for (segment_id, meta) in pending {
            let record = manifest.find(segment_id).cloned();
            match self.advance(segment_id, &meta, record).await {
                Ok(Advance::Uploaded) => report.uploaded += 1,
                Ok(Advance::AlreadyRemote) => report.already_remote += 1,
                Ok(Advance::AlreadyDeleted) => report.already_deleted += 1,
                Ok(Advance::PendingUpload) => report.pending += 1,
                Err(error) => {
                    if self.is_poisoned() {
                        return Err(error);
                    }
                    // Keep the local bytes and retry on the next pass, but do not
                    // starve the remaining segments.
                    warn!(segment_id, error = %error, "upload failed for one segment");
                    report.pending += 1;
                }
            }
        }
        Ok(report)
    }

    /// Run recovery once, then keep uploading until `shutdown` is set. Fatal
    /// inconsistencies return an error; a poisoned uploader returns immediately.
    pub async fn run(&self, shutdown: Arc<AtomicBool>) -> Result<(), UploaderError> {
        self.recover().await?;
        while !shutdown.load(Ordering::Relaxed) {
            match self.process_pending().await {
                Ok(report) => {
                    if report.deleted() > 0 {
                        info!(
                            uploaded = report.uploaded,
                            already_remote = report.already_remote,
                            already_deleted = report.already_deleted,
                            "upload pass complete"
                        );
                    }
                }
                Err(error) => {
                    if self.is_poisoned() {
                        return Err(error);
                    }
                    warn!(error = %error, "upload pass failed; retrying after the poll interval");
                }
            }
            let deadline = Instant::now() + self.config.poll_interval;
            while !shutdown.load(Ordering::Relaxed) {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
            }
        }
        Ok(())
    }

    /// Advance one segment as far as its durable state allows.
    async fn advance(
        &self,
        segment_id: u64,
        _meta: &SegmentMeta,
        record: Option<ManifestRecord>,
    ) -> Result<Advance, UploaderError> {
        let path = self.config.segment_path(segment_id);

        let digest = match self.worker.stat(&path, segment_id).await {
            Ok(digest) => digest,
            Err(error) if error.is_not_found() => {
                return match record {
                    Some(record) => {
                        self.mark_uploaded(segment_id, &record)?;
                        self.mark_deleted(segment_id)?;
                        self.counters
                            .recovered_deletes
                            .fetch_add(1, Ordering::SeqCst);
                        Ok(Advance::AlreadyDeleted)
                    }
                    None => {
                        self.counters.missing_local.fetch_add(1, Ordering::SeqCst);
                        let error = UploaderError::LocalSegmentMissing { segment_id };
                        self.poison(error.to_string());
                        Err(error)
                    }
                };
            }
            Err(error) => {
                if let WorkerError::Codec { .. } | WorkerError::IdMismatch { .. } = &error {
                    let error = UploaderError::CorruptSegment {
                        segment_id,
                        detail: error.to_string(),
                    };
                    self.poison(error.to_string());
                    return Err(error);
                }
                return Err(UploaderError::Worker(error));
            }
        };

        let key = self.record_key(segment_id, digest.writer_epoch);
        if let Some(record) = record
            .as_ref()
            .filter(|row| row.writer_epoch >= digest.writer_epoch)
        {
            // The manifest already proves these bytes are remote: never PUT again,
            // just finish the local half of the transaction.
            match self.check_record(segment_id, record, &digest, &key) {
                Ok(()) => {}
                Err(error) => {
                    self.poison(error.to_string());
                    return Err(error);
                }
            }
            self.mark_uploaded(segment_id, record)?;
            self.delete_local(segment_id, &path).await?;
            self.counters
                .recovered_deletes
                .fetch_add(1, Ordering::SeqCst);
            return Ok(Advance::AlreadyRemote);
        }

        self.mark_uploading(segment_id, &key, &digest)?;
        let outcome = self.put_segment(segment_id, &path, &key).await?;
        if outcome.len != digest.file_len {
            let error = UploaderError::VerificationMismatch {
                segment_id,
                remote: outcome.len,
                local: digest.file_len,
            };
            self.poison(error.to_string());
            return Err(error);
        }
        let info = self.blob.head(&key).await?;
        if info.len != digest.file_len {
            let error = UploaderError::VerificationMismatch {
                segment_id,
                remote: info.len,
                local: digest.file_len,
            };
            self.poison(error.to_string());
            return Err(error);
        }

        let record = ManifestRecord {
            segment_id,
            key: key.clone(),
            bytes: outcome.len,
            crc32c: outcome.crc32c,
            writer_epoch: digest.writer_epoch,
            min_ledger: digest.min_ledger,
            max_ledger: digest.max_ledger,
            uploaded_at_ms: now_ms(),
        };
        match self.manifest.commit(&record).await {
            Ok(CommitOutcome::Committed { .. }) => {
                self.counters
                    .manifest_commits
                    .fetch_add(1, Ordering::SeqCst);
            }
            Ok(CommitOutcome::AlreadyCommitted { .. }) => {
                self.counters
                    .already_committed
                    .fetch_add(1, Ordering::SeqCst);
            }
            Ok(CommitOutcome::Fenced { existing_epoch }) => {
                self.counters.fenced.fetch_add(1, Ordering::SeqCst);
                let error = UploaderError::Fenced {
                    segment_id,
                    manifest_epoch: existing_epoch,
                    local_epoch: digest.writer_epoch,
                };
                self.poison(error.to_string());
                return Err(error);
            }
            Err(error) => return Err(error.into()),
        }

        self.counters
            .bytes_uploaded
            .fetch_add(outcome.len, Ordering::SeqCst);
        self.counters
            .segments_uploaded
            .fetch_add(1, Ordering::SeqCst);
        self.mark_uploaded(segment_id, &record)?;
        self.delete_local(segment_id, &path).await?;
        Ok(Advance::Uploaded)
    }

    /// Validate that a committing manifest record describes this exact local file.
    fn check_record(
        &self,
        segment_id: u64,
        record: &ManifestRecord,
        digest: &SegmentDigest,
        key: &str,
    ) -> Result<(), UploaderError> {
        if record.writer_epoch > digest.writer_epoch {
            self.counters.fenced.fetch_add(1, Ordering::SeqCst);
            return Err(UploaderError::Fenced {
                segment_id,
                manifest_epoch: record.writer_epoch,
                local_epoch: digest.writer_epoch,
            });
        }
        if record.key != key
            || record.writer_epoch != digest.writer_epoch
            || record.bytes != digest.file_len
        {
            return Err(UploaderError::ManifestConflict { segment_id });
        }
        Ok(())
    }

    fn record_key(&self, segment_id: u64, writer_epoch: u64) -> String {
        self.config.segment_key(segment_id, writer_epoch)
    }

    /// Stream one object with bounded retries. Each attempt re-reads the file from
    /// the beginning: a failed multipart upload is never resumed, only restarted,
    /// which keeps the retry path free of partial-upload bookkeeping.
    async fn put_segment(
        &self,
        segment_id: u64,
        path: &Path,
        key: &str,
    ) -> Result<PutOutcome, UploaderError> {
        let deadline = Instant::now() + self.config.retry_timeout;
        let mut delay = self.config.retry_base_backoff;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            self.counters.put_attempts.fetch_add(1, Ordering::SeqCst);

            let (sink, chunks) = mpsc::channel(self.config.stream_channel_capacity);
            let stream = self.worker.stream(
                path,
                segment_id,
                self.config.read_chunk_bytes,
                Arc::clone(&self.budget),
                sink,
            );
            let put = self.blob.put_stream(key, self.config.part_size, chunks);
            let (streamed, uploaded) = tokio::join!(stream, put);

            if let Err(error) = &streamed {
                if matches!(
                    error,
                    WorkerError::Codec { .. } | WorkerError::IdMismatch { .. }
                ) {
                    let error = UploaderError::CorruptSegment {
                        segment_id,
                        detail: error.to_string(),
                    };
                    self.poison(error.to_string());
                    return Err(error);
                }
                debug!(segment_id, error = %error, "local read ended early");
            }

            match uploaded {
                Ok(outcome) => {
                    if let Err(error) = &streamed {
                        // The store saw the end marker, so the object is complete;
                        // a late local error must not invalidate it.
                        warn!(segment_id, error = %error, "local read reported an error after the upload completed");
                    }
                    return Ok(outcome);
                }
                Err(error) => {
                    self.counters.put_failures.fetch_add(1, Ordering::SeqCst);
                    let retryable = error.is_retryable();
                    let budget_left = Instant::now() < deadline;
                    if retryable && attempt < self.config.max_retries && budget_left {
                        self.counters.put_retries.fetch_add(1, Ordering::SeqCst);
                        debug!(
                            segment_id,
                            attempt,
                            delay_ms = delay.as_millis() as u64,
                            error = %error,
                            "retrying segment upload"
                        );
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(self.config.retry_max_backoff);
                        continue;
                    }
                    return Err(UploaderError::RetriesExhausted {
                        segment_id,
                        detail: error.to_string(),
                    });
                }
            }
        }
    }

    /// Record the local intent before the PUT so a crash mid-upload leaves a
    /// resolvable S3 key behind.
    fn mark_uploading(
        &self,
        segment_id: u64,
        key: &str,
        digest: &SegmentDigest,
    ) -> Result<(), UploaderError> {
        self.mutate_segment(segment_id, |meta| {
            meta.state = SegmentState::Uploading;
            meta.s3_key = Some(key.to_string());
            meta.byte_size = digest.file_len;
            meta.writer_epoch = digest.writer_epoch;
            meta.min_ledger = digest.min_ledger;
            meta.max_ledger = digest.max_ledger;
        })?;
        Ok(())
    }

    fn mark_uploaded(&self, segment_id: u64, record: &ManifestRecord) -> Result<(), UploaderError> {
        self.mutate_segment(segment_id, |meta| {
            meta.state = SegmentState::Uploaded;
            meta.s3_key = Some(record.key.clone());
            meta.writer_epoch = record.writer_epoch;
            meta.byte_size = record.bytes;
            meta.min_ledger = record.min_ledger;
            meta.max_ledger = record.max_ledger;
        })
    }

    /// The segment leaves the local set here; entry rows and the S3 key stay so
    /// reads keep resolving through the object store.
    fn mark_deleted(&self, segment_id: u64) -> Result<(), UploaderError> {
        self.mutate_segment(segment_id, |meta| meta.state = SegmentState::Deleted)?;
        self.counters
            .segments_deleted
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Unlink the local file (durably) before publishing the `Deleted` state.
    async fn delete_local(&self, segment_id: u64, path: &Path) -> Result<(), UploaderError> {
        match self.worker.unlink(path, self.config.data_dir.clone()).await {
            Ok(()) => {}
            Err(error) if error.is_not_found() => {}
            Err(error) => return Err(UploaderError::Worker(error)),
        }
        self.mark_deleted(segment_id)?;
        Ok(())
    }

    fn mutate_segment(
        &self,
        segment_id: u64,
        mutate: impl FnOnce(&mut SegmentMeta),
    ) -> Result<(), UploaderError> {
        let mut meta = self
            .index
            .get_segment(segment_id)?
            .ok_or(bkxpro_indexer::IndexError::SegmentNotFound(segment_id))?;
        mutate(&mut meta);
        let mut batch = self.index.batch();
        self.index.put_segment(&mut batch, segment_id, meta)?;
        batch.commit().map_err(bkxpro_indexer::IndexError::Fjall)?;
        self.index.persist()?;
        Ok(())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bkxpro_indexer::{Index, IndexConfig};
    use bkxpro_io::simulated::SimulatedSegmentIo;
    use bkxpro_segmenter::SegmentIo;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        blob::{BlobStore, FaultyBlob, ObjectStoreBlob, StreamItem},
        test_util::{install, sealed_image, DrainBlob},
    };

    struct Fixture {
        uploader: SegmentUploader<FaultyBlob>,
        blob: Arc<FaultyBlob>,
        index: Arc<Index>,
        worker: Arc<SegmentIoWorker>,
        config: UploaderConfig,
        _index_dir: TempDir,
    }

    /// A worker plus fixture. `setup` runs against the simulated backend before
    /// the worker takes ownership of it; `blob` is the underlying store the fault
    /// injector wraps.
    fn fixture_with_blob(
        edit: impl FnOnce(&mut UploaderConfig),
        setup: impl FnOnce(&mut SimulatedSegmentIo),
        blob: Arc<dyn BlobStore>,
    ) -> Fixture {
        let index_dir = TempDir::new().unwrap();
        let index = Arc::new(
            Index::open(IndexConfig {
                db_path: index_dir.path().join("index"),
                max_memory_bytes: 32 * 1024 * 1024,
                ..Default::default()
            })
            .unwrap(),
        );

        let mut config = UploaderConfig {
            // Simulated segments live at `/sim/{segment_id}.seg`, so the uploader's
            // data dir must name that namespace (the backend ignores real paths).
            data_dir: PathBuf::from("/sim"),
            part_size: 4096,
            read_chunk_bytes: 4096,
            // Small buffers: the blob copies chunks out of the reader's budget, so the
            // budget only bounds the reader's own in-flight bytes, never the store's.
            max_inflight_bytes: 64 * 1024,
            stream_channel_capacity: 2,
            max_retries: 3,
            retry_base_backoff: Duration::from_millis(1),
            retry_max_backoff: Duration::from_millis(5),
            retry_timeout: Duration::from_secs(5),
            ..UploaderConfig::default()
        };
        edit(&mut config);

        let mut io = SimulatedSegmentIo::new();
        setup(&mut io);
        let worker = SegmentIoWorker::spawn(move || Ok(io)).unwrap();
        let blob = Arc::new(FaultyBlob::new(blob));
        let uploader = SegmentUploader::new(
            config.clone(),
            Arc::clone(&index),
            Arc::clone(&blob),
            Arc::clone(&worker),
        )
        .unwrap();

        Fixture {
            uploader,
            blob,
            index,
            worker,
            config,
            _index_dir: index_dir,
        }
    }

    fn fixture_with(
        edit: impl FnOnce(&mut UploaderConfig),
        setup: impl FnOnce(&mut SimulatedSegmentIo),
    ) -> Fixture {
        fixture_with_blob(edit, setup, Arc::new(ObjectStoreBlob::new(InMemory::new())))
    }

    fn fixture(setup: impl FnOnce(&mut SimulatedSegmentIo)) -> Fixture {
        fixture_with(|_| {}, setup)
    }

    fn put_segment_meta(index: &Index, segment_id: u64, state: SegmentState, key: Option<&str>) {
        let mut batch = index.batch();
        index
            .put_segment(&mut batch, segment_id, crate::test_meta(state, key, 0))
            .unwrap();
        batch.commit().unwrap();
    }

    fn state_of(index: &Index, segment_id: u64) -> SegmentState {
        index.get_segment(segment_id).unwrap().unwrap().state
    }

    async fn manifest_records(fixture: &Fixture) -> Vec<ManifestRecord> {
        let log = ManifestLog::new(
            Arc::clone(&fixture.blob),
            fixture.config.manifest_key.clone(),
            4,
        );
        log.load().await.unwrap().records().to_vec()
    }

    #[tokio::test]
    async fn stale_writer_put_cannot_overwrite_authoritative_generation() {
        let blob: Arc<dyn BlobStore> = Arc::new(ObjectStoreBlob::new(InMemory::new()));
        let stale_image = sealed_image(70, 1, 1);
        let new_image = sealed_image(70, 2, 2);
        let stale = fixture_with_blob(
            |_| {},
            |io| install(io, Path::new("/sim/70.seg"), &stale_image),
            Arc::clone(&blob),
        );
        let newer = fixture_with_blob(
            |_| {},
            |io| install(io, Path::new("/sim/70.seg"), &new_image),
            Arc::clone(&blob),
        );
        put_segment_meta(&stale.index, 70, SegmentState::Sealed, None);
        put_segment_meta(&newer.index, 70, SegmentState::Sealed, None);
        // Stale snapshot lets the old writer PUT even after the newer commit.
        stale.uploader.manifest.load().await.unwrap();
        newer.uploader.process_pending().await.unwrap();
        let new_key = newer.config.segment_key(70, 2);
        let old_key = stale.config.segment_key(70, 1);
        assert_ne!(old_key, new_key);
        let stale_meta = stale.index.get_segment(70).unwrap().unwrap();
        // Resume after the stale pass has loaded its manifest, as if the newer
        // writer committed between that snapshot and this segment's PUT.
        let error = stale
            .uploader
            .advance(70, &stale_meta, None)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            UploaderError::Fenced {
                manifest_epoch: 2,
                local_epoch: 1,
                ..
            }
        ));
        assert_eq!(blob.get(&new_key).await.unwrap().bytes, new_image);
        assert_eq!(blob.get(&old_key).await.unwrap().bytes, stale_image);
        let manifest = ManifestLog::new(Arc::clone(&blob), newer.config.manifest_key.clone(), 4)
            .load()
            .await
            .unwrap();
        assert_eq!(manifest.find(70).unwrap().key, new_key);
        assert_eq!(
            newer
                .index
                .get_segment(70)
                .unwrap()
                .unwrap()
                .s3_key
                .as_deref(),
            Some(new_key.as_str())
        );
        assert!(stale.worker.stat("/sim/70.seg", 70).await.is_ok());
        stale.worker.shutdown().await.unwrap();
        newer.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn newer_local_generation_supersedes_an_older_manifest_row() {
        let fixture = fixture(|io| install(io, Path::new("/sim/71.seg"), &sealed_image(71, 2, 2)));
        put_segment_meta(
            &fixture.index,
            71,
            SegmentState::Sealed,
            Some("legacy/shared.seg"),
        );
        let record = ManifestRecord {
            segment_id: 71,
            key: fixture.config.segment_key(71, 1),
            bytes: 4096,
            crc32c: 0,
            writer_epoch: 1,
            min_ledger: 1,
            max_ledger: 1,
            uploaded_at_ms: 0,
        };
        fixture.uploader.manifest.commit(&record).await.unwrap();
        let recovery = fixture.uploader.recover().await.unwrap();
        assert_eq!(recovery.pending_uploads, 1);
        fixture.uploader.process_pending().await.unwrap();
        let manifest = fixture.uploader.manifest.refresh().await.unwrap();
        let row = manifest.find(71).unwrap();
        assert_eq!(row.writer_epoch, 2);
        assert_eq!(row.key, fixture.config.segment_key(71, 2));
        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn uploads_commits_and_deletes() {
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/1.seg"), &sealed_image(1, 11, 3));
        });
        put_segment_meta(&fixture.index, 1, SegmentState::Sealed, None);

        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(report.pending, 0);

        let key = fixture.config.segment_key(1, 11);
        let remote = fixture.blob.inner().get(&key).await.unwrap();
        assert_eq!(remote.bytes, sealed_image(1, 11, 3));
        assert_eq!(state_of(&fixture.index, 1), SegmentState::Deleted);

        let meta = fixture.index.get_segment(1).unwrap().unwrap();
        assert_eq!(meta.s3_key.as_deref(), Some(key.as_str()));
        assert_eq!(meta.writer_epoch, 11);
        assert_eq!(meta.byte_size, remote.bytes.len() as u64);

        let records = manifest_records(&fixture).await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].segment_id, 1);
        assert_eq!(records[0].bytes, remote.bytes.len() as u64);
        assert_eq!(records[0].crc32c, crc32c::crc32c(&remote.bytes));
        assert_eq!(records[0].writer_epoch, 11);

        // The local file is gone: a second stat reports "not found".
        assert!(fixture
            .worker
            .stat("/sim/1.seg", 1)
            .await
            .unwrap_err()
            .is_not_found());

        // Idempotent: a second pass uploads nothing.
        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.examined, 0);
        assert_eq!(fixture.blob.put_attempts(), 1);
        assert_eq!(fixture.uploader.stats().segments_deleted, 1);

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn crash_after_put_before_commit_reuploads_once() {
        // The PUT landed but the manifest commit did not: state Uploading, local
        // file present, no manifest row. Recovery must re-PUT (idempotent
        // overwrite) and then commit + delete.
        let image = sealed_image(2, 5, 2);
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/2.seg"), &image);
        });
        put_segment_meta(
            &fixture.index,
            2,
            SegmentState::Uploading,
            Some(&fixture.config.segment_key(2, 5)),
        );
        let key = fixture.config.segment_key(2, 5);
        {
            let (tx, rx) = mpsc::channel(4);
            tx.send(StreamItem::Chunk(image.clone())).await.unwrap();
            tx.send(StreamItem::End {
                len: image.len() as u64,
                crc32c: crc32c::crc32c(&image),
            })
            .await
            .unwrap();
            drop(tx);
            fixture
                .blob
                .inner()
                .put_stream(&key, image.len(), rx)
                .await
                .unwrap();
        }

        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(fixture.blob.put_attempts(), 1, "exactly one re-PUT");
        assert_eq!(state_of(&fixture.index, 2), SegmentState::Deleted);
        assert_eq!(manifest_records(&fixture).await.len(), 1);
        assert_eq!(
            fixture.blob.inner().get(&key).await.unwrap().bytes,
            image,
            "the object is byte-identical"
        );

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn crash_after_commit_before_delete_skips_the_put() {
        let image = sealed_image(3, 6, 2);
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/3.seg"), &image);
        });
        let key = fixture.config.segment_key(3, 6);
        put_segment_meta(&fixture.index, 3, SegmentState::Uploading, Some(&key));

        {
            let (tx, rx) = mpsc::channel(4);
            tx.send(StreamItem::Chunk(image.clone())).await.unwrap();
            tx.send(StreamItem::End {
                len: image.len() as u64,
                crc32c: crc32c::crc32c(&image),
            })
            .await
            .unwrap();
            drop(tx);
            fixture
                .blob
                .inner()
                .put_stream(&key, image.len(), rx)
                .await
                .unwrap();
        }
        let record = ManifestRecord {
            segment_id: 3,
            key: key.clone(),
            bytes: image.len() as u64,
            crc32c: crc32c::crc32c(&image),
            writer_epoch: 6,
            min_ledger: 1,
            max_ledger: 1,
            uploaded_at_ms: 0,
        };
        let blob: Arc<dyn BlobStore> = fixture.blob.clone();
        ManifestLog::new(blob, fixture.config.manifest_key.clone(), 4)
            .commit(&record)
            .await
            .unwrap();

        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.already_remote, 1);
        assert_eq!(fixture.blob.put_attempts(), 0, "no PUT after the commit");
        assert_eq!(state_of(&fixture.index, 3), SegmentState::Deleted);
        assert!(fixture
            .worker
            .stat("/sim/3.seg", 3)
            .await
            .unwrap_err()
            .is_not_found());

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn crash_after_delete_before_state_change_recovers() {
        // The file is gone but the index still says Uploaded: the manifest record
        // is the authority and the segment is marked Deleted without any PUT.
        let image = sealed_image(4, 7, 1);
        let fixture = fixture(|_| {});
        put_segment_meta(
            &fixture.index,
            4,
            SegmentState::Uploaded,
            Some(&fixture.config.segment_key(4, 7)),
        );
        let record = ManifestRecord {
            segment_id: 4,
            key: fixture.config.segment_key(4, 7),
            bytes: image.len() as u64,
            crc32c: crc32c::crc32c(&image),
            writer_epoch: 7,
            min_ledger: 1,
            max_ledger: 1,
            uploaded_at_ms: 0,
        };
        let blob: Arc<dyn BlobStore> = fixture.blob.clone();
        ManifestLog::new(blob, fixture.config.manifest_key.clone(), 4)
            .commit(&record)
            .await
            .unwrap();

        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.already_deleted, 1);
        assert_eq!(fixture.blob.put_attempts(), 0);
        assert_eq!(state_of(&fixture.index, 4), SegmentState::Deleted);

        // recover() also reconciles it without any work left over.
        let recovery = fixture.uploader.recover().await.unwrap();
        assert_eq!(recovery.scanned, 0);

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn missing_file_without_record_is_fail_stop() {
        let fixture = fixture(|_| {});
        put_segment_meta(&fixture.index, 5, SegmentState::Sealed, None);

        let error = fixture.uploader.process_pending().await.unwrap_err();
        assert!(
            matches!(error, UploaderError::LocalSegmentMissing { segment_id: 5 }),
            "{error:?}"
        );
        assert!(fixture.uploader.is_poisoned());
        assert_eq!(fixture.blob.put_attempts(), 0);
        assert_eq!(state_of(&fixture.index, 5), SegmentState::Sealed);
        assert!(manifest_records(&fixture).await.is_empty());

        let error = fixture.uploader.process_pending().await.unwrap_err();
        assert!(matches!(error, UploaderError::Poisoned { .. }));

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn fenced_epoch_is_fail_stop_and_keeps_the_file() {
        let image = sealed_image(6, 4, 1);
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/6.seg"), &image);
        });
        put_segment_meta(
            &fixture.index,
            6,
            SegmentState::Uploaded,
            Some(&fixture.config.segment_key(6, 4)),
        );
        // A newer writer already owns this segment id in the manifest.
        let record = ManifestRecord {
            segment_id: 6,
            key: fixture.config.segment_key(6, 99),
            bytes: 8192,
            crc32c: 0,
            writer_epoch: 99,
            min_ledger: 1,
            max_ledger: 1,
            uploaded_at_ms: 0,
        };
        let blob: Arc<dyn BlobStore> = fixture.blob.clone();
        ManifestLog::new(blob, fixture.config.manifest_key.clone(), 4)
            .commit(&record)
            .await
            .unwrap();

        let error = fixture.uploader.process_pending().await.unwrap_err();
        assert!(
            matches!(
                error,
                UploaderError::Fenced {
                    segment_id: 6,
                    manifest_epoch: 99,
                    local_epoch: 4
                }
            ),
            "{error:?}"
        );
        assert!(fixture.uploader.is_poisoned());
        assert_eq!(
            fixture.blob.put_attempts(),
            0,
            "fenced segments are not re-uploaded"
        );
        // The local file is untouched.
        let digest = fixture.worker.stat("/sim/6.seg", 6).await.unwrap();
        assert_eq!(digest.writer_epoch, 4);

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn manifest_mismatch_is_fail_stop() {
        let image = sealed_image(7, 8, 1);
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/7.seg"), &image);
        });
        put_segment_meta(
            &fixture.index,
            7,
            SegmentState::Uploaded,
            Some(&fixture.config.segment_key(7, 8)),
        );
        let record = ManifestRecord {
            segment_id: 7,
            key: fixture.config.segment_key(7, 8),
            bytes: image.len() as u64 + 4096,
            crc32c: 0,
            writer_epoch: 8,
            min_ledger: 1,
            max_ledger: 1,
            uploaded_at_ms: 0,
        };
        let blob: Arc<dyn BlobStore> = fixture.blob.clone();
        ManifestLog::new(blob, fixture.config.manifest_key.clone(), 4)
            .commit(&record)
            .await
            .unwrap();

        let error = fixture.uploader.process_pending().await.unwrap_err();
        assert!(
            matches!(error, UploaderError::ManifestConflict { segment_id: 7 }),
            "{error:?}"
        );
        assert!(fixture.uploader.is_poisoned());
        assert_eq!(fixture.blob.put_attempts(), 0);

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn lost_put_response_is_retried_and_marked_once() {
        let image = sealed_image(8, 2, 2);
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/8.seg"), &image);
        });
        put_segment_meta(&fixture.index, 8, SegmentState::Sealed, None);
        fixture.blob.with_plan(|plan| plan.put_lost_response = 1);

        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(
            fixture.blob.put_attempts(),
            2,
            "one lost response, one retry"
        );
        let stats = fixture.uploader.stats();
        assert_eq!(stats.put_retries, 1);
        assert_eq!(stats.put_failures, 1);
        assert_eq!(stats.segments_uploaded, 1);
        assert_eq!(stats.segments_deleted, 1);

        let records = manifest_records(&fixture).await;
        assert_eq!(records.len(), 1, "the manifest row is written once");
        assert_eq!(records[0].bytes, image.len() as u64);

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_puts_are_retried_with_backoff() {
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/9.seg"), &sealed_image(9, 3, 2));
        });
        put_segment_meta(&fixture.index, 9, SegmentState::Sealed, None);
        fixture.blob.with_plan(|plan| plan.put_transient = 2);

        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(fixture.blob.put_attempts(), 3);
        assert_eq!(fixture.uploader.stats().put_retries, 2);
        assert!(!fixture.uploader.is_poisoned());

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn exhausted_retries_keep_everything_local() {
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/10.seg"), &sealed_image(10, 3, 2));
        });
        put_segment_meta(&fixture.index, 10, SegmentState::Sealed, None);
        fixture
            .blob
            .with_plan(|plan| plan.put_persistent_failure = true);

        let error = fixture.uploader.process_pending().await.unwrap_err();
        assert!(
            matches!(
                error,
                UploaderError::RetriesExhausted { segment_id: 10, .. }
            ),
            "{error:?}"
        );
        assert!(
            !fixture.uploader.is_poisoned(),
            "transient failures do not poison"
        );
        assert_eq!(fixture.blob.put_attempts(), 3, "max_retries is respected");
        assert_eq!(state_of(&fixture.index, 10), SegmentState::Uploading);
        assert!(manifest_records(&fixture).await.is_empty());
        // Nothing was deleted.
        assert!(fixture.worker.stat("/sim/10.seg", 10).await.is_ok());

        // Clearing the fault lets the next pass finish normally.
        fixture
            .blob
            .with_plan(|plan| plan.put_persistent_failure = false);
        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(state_of(&fixture.index, 10), SegmentState::Deleted);

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cas_conflict_and_lost_cas_response_are_absorbed() {
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/11.seg"), &sealed_image(11, 1, 1));
        });
        put_segment_meta(&fixture.index, 11, SegmentState::Sealed, None);
        fixture.blob.with_plan(|plan| plan.cas_conflict = 1);

        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(fixture.blob.cas_attempts(), 2, "conflict then success");
        assert_eq!(manifest_records(&fixture).await.len(), 1);
        assert_eq!(state_of(&fixture.index, 11), SegmentState::Deleted);

        // A lost CAS response on the next segment is idempotent: the record is
        // present when the retry re-reads the log.
        let fixture2 = fixture_with(
            |_| {},
            |io| install(io, Path::new("/sim/12.seg"), &sealed_image(12, 1, 1)),
        );
        put_segment_meta(&fixture2.index, 12, SegmentState::Sealed, None);
        fixture2.blob.with_plan(|plan| plan.cas_lost_response = 1);
        let report = fixture2.uploader.process_pending().await.unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(manifest_records(&fixture2).await.len(), 1);
        assert_eq!(state_of(&fixture2.index, 12), SegmentState::Deleted);

        fixture.worker.shutdown().await.unwrap();
        fixture2.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recover_finishes_every_crash_window() {
        // One segment per crash window, all in one pass.
        let image_a = sealed_image(20, 2, 2);
        let image_b = sealed_image(21, 2, 2);
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/20.seg"), &image_a);
            install(io, Path::new("/sim/21.seg"), &image_b);
        });
        // 20: uploaded (object + record) but never deleted locally.
        let key20 = fixture.config.segment_key(20, 2);
        put_segment_meta(&fixture.index, 20, SegmentState::Uploading, Some(&key20));
        {
            let (tx, rx) = mpsc::channel(2);
            tx.send(StreamItem::Chunk(image_a.clone())).await.unwrap();
            tx.send(StreamItem::End {
                len: image_a.len() as u64,
                crc32c: crc32c::crc32c(&image_a),
            })
            .await
            .unwrap();
            drop(tx);
            fixture
                .blob
                .inner()
                .put_stream(&key20, image_a.len(), rx)
                .await
                .unwrap();
        }
        let record = ManifestRecord {
            segment_id: 20,
            key: key20,
            bytes: image_a.len() as u64,
            crc32c: crc32c::crc32c(&image_a),
            writer_epoch: 2,
            min_ledger: 1,
            max_ledger: 1,
            uploaded_at_ms: 0,
        };
        let blob: Arc<dyn BlobStore> = fixture.blob.clone();
        ManifestLog::new(blob, fixture.config.manifest_key.clone(), 4)
            .commit(&record)
            .await
            .unwrap();
        // 21: never uploaded, still local.
        put_segment_meta(&fixture.index, 21, SegmentState::Sealed, None);

        let recovery = fixture.uploader.recover().await.unwrap();
        assert_eq!(recovery.scanned, 2);
        assert_eq!(recovery.reconciled_deleted, 1);
        assert_eq!(recovery.reconciled_uploaded, 1);
        assert_eq!(recovery.pending_uploads, 1);
        assert_eq!(state_of(&fixture.index, 20), SegmentState::Deleted);
        assert_eq!(state_of(&fixture.index, 21), SegmentState::Sealed);

        // process_pending then handles the remaining segment.
        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(state_of(&fixture.index, 21), SegmentState::Deleted);
        assert_eq!(fixture.blob.put_attempts(), 1);

        // A second recovery is a no-op.
        let recovery = fixture.uploader.recover().await.unwrap();
        assert_eq!(recovery.scanned, 0);

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_real_crash_before_the_directory_sync_loses_only_the_new_file() {
        // The simulated backend can actually lose an unsynced file. This proves the
        // uploader's fail-stop: a segment whose file vanished before any manifest
        // record existed must stop the pipeline instead of marking it deleted.
        let mut io = SimulatedSegmentIo::new();
        let path = Path::new("/sim/30.seg");
        io.acquire_preallocated_segment(path, 4096).unwrap();
        // No directory sync: the file is not durable.
        io.simulate_crash().unwrap();
        assert!(io.visible_bytes(path).is_none());

        let fixture = fixture_with(|_| {}, move |target| *target = io);
        put_segment_meta(&fixture.index, 30, SegmentState::Sealed, None);
        let error = fixture.uploader.recover().await.unwrap_err();
        assert!(matches!(
            error,
            UploaderError::LocalSegmentMissing { segment_id: 30 }
        ));
        assert_eq!(state_of(&fixture.index, 30), SegmentState::Sealed);

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn backpressure_keeps_inflight_bytes_within_the_budget() {
        // `DrainBlob` never stores the object, so this test isolates the reader's own
        // budget: each chunk leases the whole budget while it is in flight.
        // `a_memory_store_uploads_a_segment_larger_than_the_byte_budget` covers the
        // other half, a store that buffers every part until completion.
        let drain = DrainBlob::new(Arc::new(ObjectStoreBlob::new(InMemory::new())));
        let image = sealed_image(40, 1, 1);
        let fixture = fixture_with_blob(
            |config| {
                config.read_chunk_bytes = 4096;
                config.max_inflight_bytes = 4096;
            },
            |io| install(io, Path::new("/sim/40.seg"), &image),
            drain.clone(),
        );
        put_segment_meta(&fixture.index, 40, SegmentState::Sealed, None);

        let budget = Arc::clone(fixture.uploader.budget());
        let report = fixture.uploader.process_pending().await.unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(
            drain
                .uploaded(&fixture.config.segment_key(40, 1))
                .unwrap()
                .0,
            image.len() as u64
        );
        assert!(budget.high_water() <= budget.capacity());
        assert_eq!(
            budget.high_water(),
            budget.capacity(),
            "the reader leased the full budget"
        );
        assert_eq!(budget.in_use(), 0, "all permits released after the upload");

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_memory_store_uploads_a_segment_larger_than_the_byte_budget() {
        // `InMemory` retains every part (and its payload) until `complete()`, so the
        // store holds whatever bytes it is handed. The blob copies each chunk into its
        // part buffer and drops the chunk immediately, which releases the reader's
        // permit: a segment several times the byte budget must still upload instead of
        // stalling on a store that will never free the reader's buffers.
        let blob = Arc::new(ObjectStoreBlob::new(InMemory::new()));
        let image = sealed_image(41, 4, 4);
        let fixture = fixture_with_blob(
            |config| {
                config.read_chunk_bytes = 4096;
                config.max_inflight_bytes = 4096;
            },
            |io| install(io, Path::new("/sim/41.seg"), &image),
            blob.clone(),
        );
        put_segment_meta(&fixture.index, 41, SegmentState::Sealed, None);

        let report =
            tokio::time::timeout(Duration::from_secs(20), fixture.uploader.process_pending())
                .await
                .expect("uploading past the byte budget must not stall")
                .unwrap();
        assert_eq!(report.uploaded, 1);
        assert_eq!(report.deleted(), 1);

        // The copied parts must reproduce the segment byte for byte.
        let object = blob
            .get(&fixture.config.segment_key(41, 4))
            .await
            .expect("the uploaded object");
        assert_eq!(object.bytes.as_ref(), image.as_ref());

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn run_loop_uploads_then_stops_on_shutdown() {
        let fixture = fixture_with(
            |config| config.poll_interval = Duration::from_millis(5),
            |io| install(io, Path::new("/sim/50.seg"), &sealed_image(50, 1, 2)),
        );
        put_segment_meta(&fixture.index, 50, SegmentState::Sealed, None);

        let shutdown = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            stop.store(true, Ordering::Relaxed);
        });
        fixture.uploader.run(shutdown).await.unwrap();
        assert_eq!(state_of(&fixture.index, 50), SegmentState::Deleted);

        fixture.worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn object_key_and_manifest_are_stable_across_recovery() {
        let fixture = fixture(|io| {
            install(io, Path::new("/sim/60.seg"), &sealed_image(60, 1, 3));
        });
        put_segment_meta(&fixture.index, 60, SegmentState::Sealed, None);
        fixture.uploader.recover().await.unwrap();
        fixture.uploader.process_pending().await.unwrap();

        let key = fixture.config.segment_key(60, 1);
        let info = fixture.blob.inner().head(&key).await.unwrap();
        assert_eq!(info.len, sealed_image(60, 1, 3).len() as u64);

        fixture.worker.shutdown().await.unwrap();
    }
}
