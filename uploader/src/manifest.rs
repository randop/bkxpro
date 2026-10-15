//! Append-only upload manifest with compare-and-swap appends and writer-epoch
//! fencing.
//!
//! The manifest is the authority for "this segment is durably in S3". A local
//! segment file may be deleted only after its exact record is present in the
//! manifest, so every crash window between PUT and delete is recoverable:
//!
//! * PUT landed, manifest commit did not -> the record is absent; recovery re-PUTs
//!   (idempotent overwrite) and commits again.
//! * manifest commit landed, response lost -> recovery reads the manifest, finds
//!   the record and skips the PUT.
//! * a competing writer appended first -> CAS conflict, re-read, decide again.
//! * a newer writer epoch owns the segment id -> [`CommitOutcome::Fenced`]; the
//!   uploader must not delete local bytes it did not just verify against S3.
//!
//! The object is JSONL so appends never rewrite existing rows and a truncated
//! final line (from an interrupted external tool) can be tolerated.

use std::sync::Mutex;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::blob::{BlobError, BlobStore, ExpectedVersion, ObjectVersion};

/// One committed segment upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRecord {
    pub segment_id: u64,
    /// Object key of the uploaded segment.
    pub key: String,
    /// Uploaded byte length.
    pub bytes: u64,
    /// CRC32C of the uploaded bytes, computed by the reader that streamed them.
    pub crc32c: u32,
    /// Writer epoch from the segment header that produced these bytes.
    pub writer_epoch: u64,
    pub min_ledger: u64,
    pub max_ledger: u64,
    /// Wall-clock time of the commit; informational only.
    pub uploaded_at_ms: u64,
}

impl ManifestRecord {
    /// True when both records describe the same uploaded bytes for the same epoch.
    /// Used to make a repeated commit idempotent when the response was lost.
    pub fn same_upload(&self, other: &Self) -> bool {
        self.segment_id == other.segment_id
            && self.key == other.key
            && self.bytes == other.bytes
            && self.crc32c == other.crc32c
            && self.writer_epoch == other.writer_epoch
    }
}

/// Parsed manifest object.
#[derive(Debug, Clone, Default)]
pub struct Manifest {
    raw: Bytes,
    valid_len: usize,
    version: Option<ObjectVersion>,
    records: Vec<ManifestRecord>,
}

impl Manifest {
    /// Parse a manifest object. `raw` is kept verbatim so appends can extend the
    /// exact bytes the store returned.
    pub fn parse(raw: Bytes, version: Option<ObjectVersion>, key: &str) -> Result<Self, BlobError> {
        let mut records = Vec::new();
        let mut valid_len = raw.len();
        let mut offset = 0;
        let mut lines = raw
            .split(|byte| *byte == b'\n')
            .enumerate()
            .map(|(index, line)| {
                let start = offset;
                offset += line.len() + 1;
                (index, start, line)
            })
            .filter(|(_, _, line)| !line.is_empty())
            .peekable();
        while let Some((index, start, line)) = lines.next() {
            let is_last = lines.peek().is_none();
            match serde_json::from_slice::<ManifestRecord>(line) {
                Ok(record) => records.push(record),
                Err(error) if is_last => {
                    valid_len = start;
                    // A crash during an external append can leave a partial last
                    // line; it carries no committed information, so drop it.
                    warn!(
                        manifest = key,
                        line = index,
                        "ignoring truncated trailing manifest line: {error}"
                    );
                }
                Err(error) => {
                    return Err(BlobError::Fatal(format!(
                        "manifest {key} line {index} is corrupt: {error}"
                    )));
                }
            }
        }
        Ok(Self {
            raw,
            valid_len,
            version,
            records,
        })
    }

    /// All parsed records in append order.
    pub fn records(&self) -> &[ManifestRecord] {
        &self.records
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Last record for one segment id, which is the newest generation.
    pub fn find(&self, segment_id: u64) -> Option<&ManifestRecord> {
        self.records
            .iter()
            .rev()
            .find(|record| record.segment_id == segment_id)
    }

    /// Version this manifest was read at, if the object existed.
    pub fn version(&self) -> Option<&ObjectVersion> {
        self.version.as_ref()
    }
}

/// Result of appending one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The record was appended by this call.
    Committed { version: ObjectVersion },
    /// The record was already present (a previous commit landed).
    AlreadyCommitted { version: Option<ObjectVersion> },
    /// A newer writer epoch owns this segment id; do not delete local bytes.
    Fenced { existing_epoch: u64 },
}

/// CAS-append log over a manifest object.
pub struct ManifestLog<B: BlobStore> {
    blob: B,
    key: String,
    max_cas_retries: u32,
    cache: Mutex<Option<Manifest>>,
}

impl<B: BlobStore> ManifestLog<B> {
    /// Wrap a store handle. Pass an `Arc` when the log must share the store with
    /// an uploader (`Arc<B>` is itself a [`BlobStore`]).
    pub fn new(blob: B, key: impl Into<String>, max_cas_retries: u32) -> Self {
        Self {
            blob,
            key: key.into(),
            max_cas_retries: max_cas_retries.max(1),
            cache: Mutex::new(None),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Load the manifest, using the cached copy when one is present.
    pub async fn load(&self) -> Result<Manifest, BlobError> {
        if let Some(cached) = self.cached() {
            return Ok(cached);
        }
        self.refresh().await
    }

    /// Force a GET even when a cached copy exists.
    pub async fn refresh(&self) -> Result<Manifest, BlobError> {
        let manifest = match self.blob.get(&self.key).await {
            Ok(object) => Manifest::parse(object.bytes, Some(object.info.version), &self.key)?,
            Err(BlobError::NotFound(_)) => Manifest::default(),
            Err(error) => return Err(error),
        };
        self.store_cache(manifest.clone());
        Ok(manifest)
    }

    /// Cached manifest, if this instance has loaded one.
    pub fn cached(&self) -> Option<Manifest> {
        self.cache.lock().unwrap().clone()
    }

    fn store_cache(&self, manifest: Manifest) {
        *self.cache.lock().unwrap() = Some(manifest);
    }

    /// Append `record` with compare-and-swap semantics.
    ///
    /// A conflict means another writer won the race; a transient failure may mean
    /// the write landed but the response was lost. Both are absorbed by dropping
    /// the cached copy, re-reading the log and deciding again, bounded by the
    /// configured CAS retries.
    pub async fn commit(&self, record: &ManifestRecord) -> Result<CommitOutcome, BlobError> {
        let attempts = self.max_cas_retries.saturating_add(1);
        let mut last_retryable = None;
        for _ in 0..attempts {
            let manifest = self.load().await?;
            if let Some(existing) = manifest.find(record.segment_id) {
                if existing.writer_epoch > record.writer_epoch {
                    return Ok(CommitOutcome::Fenced {
                        existing_epoch: existing.writer_epoch,
                    });
                }
                if existing.writer_epoch == record.writer_epoch {
                    if existing.same_upload(record) {
                        return Ok(CommitOutcome::AlreadyCommitted {
                            version: manifest.version.clone(),
                        });
                    }
                    return Err(BlobError::Fatal(format!(
                        "manifest {} already records segment {} epoch {} with different bytes; \
                         refusing to overwrite a divergent upload",
                        self.key, record.segment_id, record.writer_epoch
                    )));
                }
                // A lower epoch is superseded by this newer generation: append.
            }

            let mut next = manifest.raw[..manifest.valid_len].to_vec();
            if !next.is_empty() && next.last() != Some(&b'\n') {
                next.push(b'\n');
            }
            let row = serde_json::to_vec(record).map_err(|error| {
                BlobError::Fatal(format!("manifest row encoding failed: {error}"))
            })?;
            next.extend_from_slice(&row);
            next.push(b'\n');

            let expected = if manifest.raw.is_empty() {
                ExpectedVersion::MustNotExist
            } else {
                match manifest.version.clone() {
                    Some(version) => ExpectedVersion::Match(version),
                    None => {
                        return Err(BlobError::Fatal(format!(
                            "manifest {} was read without a version; cannot guard the append",
                            self.key
                        )))
                    }
                }
            };

            // The conditional write is atomic: on success the object contains
            // exactly these bytes, so the cache can be updated without a re-read.
            let payload = Bytes::from(next);
            match self
                .blob
                .put_if_version(&self.key, payload.clone(), expected)
                .await
            {
                Ok(info) => {
                    let mut committed = manifest.clone();
                    committed.valid_len = payload.len();
                    committed.raw = payload;
                    committed.records.push(record.clone());
                    committed.version = Some(info.version.clone());
                    self.store_cache(committed);
                    return Ok(CommitOutcome::Committed {
                        version: info.version,
                    });
                }
                Err(error @ (BlobError::Conflict(_) | BlobError::Transient(_))) => {
                    // Another writer won the CAS, or the conditional write landed
                    // but its response was lost; either way the cached copy is
                    // stale, so drop it and re-read before deciding again.
                    last_retryable = Some(error);
                    *self.cache.lock().unwrap() = None;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_retryable
            .unwrap_or_else(|| BlobError::Conflict(format!("{}: CAS retries exhausted", self.key))))
    }

    /// Drop the cached manifest, forcing the next load to hit the store.
    pub fn invalidate(&self) {
        *self.cache.lock().unwrap() = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::blob::{FaultyBlob, ObjectStoreBlob};

    fn record(segment_id: u64, epoch: u64, crc: u32) -> ManifestRecord {
        ManifestRecord {
            segment_id,
            key: format!("segments/{segment_id:020}/{epoch:020}.seg"),
            bytes: 4096,
            crc32c: crc,
            writer_epoch: epoch,
            min_ledger: 1,
            max_ledger: 1,
            uploaded_at_ms: 0,
        }
    }

    fn new_log(blob: Arc<dyn BlobStore>) -> ManifestLog<Arc<dyn BlobStore>> {
        ManifestLog::new(blob, "manifest/log.jsonl", 4)
    }

    fn memory() -> Arc<dyn BlobStore> {
        Arc::new(ObjectStoreBlob::new(InMemory::new()))
    }

    #[tokio::test]
    async fn commit_appends_and_is_idempotent() {
        let blob = memory();
        let log = new_log(Arc::clone(&blob));
        let first = record(1, 7, 11);

        let outcome = log.commit(&first).await.unwrap();
        assert!(matches!(outcome, CommitOutcome::Committed { .. }));

        let outcome = log.commit(&first).await.unwrap();
        assert!(matches!(outcome, CommitOutcome::AlreadyCommitted { .. }));

        // A fresh instance must agree without any cached state.
        let fresh = new_log(Arc::clone(&blob));
        let loaded = fresh.load().await.unwrap();
        assert_eq!(loaded.records(), std::slice::from_ref(&first));
        let outcome = fresh.commit(&first).await.unwrap();
        assert!(matches!(outcome, CommitOutcome::AlreadyCommitted { .. }));
        let outcome = fresh.commit(&record(2, 7, 22)).await.unwrap();
        assert!(matches!(outcome, CommitOutcome::Committed { .. }));
    }

    #[tokio::test]
    async fn newer_epoch_fences_and_stale_epoch_is_superseded() {
        let blob = memory();
        let log = new_log(Arc::clone(&blob));
        log.commit(&record(1, 5, 0)).await.unwrap();

        let fenced = log.commit(&record(1, 4, 0)).await.unwrap();
        assert_eq!(fenced, CommitOutcome::Fenced { existing_epoch: 5 });

        let superseded = log.commit(&record(1, 6, 99)).await.unwrap();
        assert!(matches!(superseded, CommitOutcome::Committed { .. }));

        let manifest = log.refresh().await.unwrap();
        assert_eq!(manifest.records().len(), 2);
        assert_eq!(manifest.find(1).unwrap().writer_epoch, 6);
        assert_eq!(manifest.find(1).unwrap().crc32c, 99);
    }

    #[tokio::test]
    async fn same_epoch_with_different_bytes_is_fatal() {
        let blob = memory();
        let log = new_log(Arc::clone(&blob));
        log.commit(&record(1, 3, 1)).await.unwrap();
        let error = log.commit(&record(1, 3, 2)).await.unwrap_err();
        assert!(matches!(error, BlobError::Fatal(_)));
    }

    #[tokio::test]
    async fn lost_cas_response_is_absorbed_by_re_reading_the_log() {
        // The conditional write landed but the response was lost: the next
        // attempt re-reads the log, finds the record and reports it as already
        // committed instead of appending a duplicate or failing the pass.
        let inner = memory();
        let faulty = Arc::new(FaultyBlob::new(Arc::clone(&inner)));
        faulty.with_plan(|plan| plan.cas_lost_response = 1);
        let log = ManifestLog::new(faulty, "manifest/log.jsonl", 4);
        let first = record(1, 7, 11);

        let outcome = log.commit(&first).await.unwrap();
        assert!(
            matches!(
                outcome,
                CommitOutcome::AlreadyCommitted { .. } | CommitOutcome::Committed { .. }
            ),
            "{outcome:?}"
        );
        let loaded = new_log(Arc::clone(&inner)).load().await.unwrap();
        assert_eq!(loaded.records(), &[first]);
    }

    #[tokio::test]
    async fn cas_conflict_reloads_and_retries() {
        let blob = memory();
        let log = new_log(Arc::clone(&blob));
        log.commit(&record(1, 1, 0)).await.unwrap();

        // Another writer appends behind our back after we cached the manifest.
        let other = new_log(Arc::clone(&blob));
        other.commit(&record(2, 1, 0)).await.unwrap();

        // Our cached copy is stale; the CAS must conflict, refresh, and succeed.
        let outcome = log.commit(&record(3, 1, 0)).await.unwrap();
        assert!(matches!(outcome, CommitOutcome::Committed { .. }));
        let manifest = log.load().await.unwrap();
        assert_eq!(manifest.records().len(), 3);
    }

    #[tokio::test]
    async fn conflicting_writer_that_removed_our_row_is_detected() {
        let blob = memory();
        let log = new_log(Arc::clone(&blob));
        let first = record(1, 2, 3);
        log.commit(&first).await.unwrap();

        let outcome = log.commit(&first).await.unwrap();
        assert!(matches!(outcome, CommitOutcome::AlreadyCommitted { .. }));

        let outcome = log.commit(&record(1, 2, 4)).await.unwrap_err();
        assert!(matches!(outcome, BlobError::Fatal(_)));
    }

    #[tokio::test]
    async fn truncated_trailing_line_is_ignored_but_middle_corruption_is_fatal() {
        let blob = memory();
        let mut good = serde_json::to_vec(&record(1, 1, 1)).unwrap();
        good.push(b'\n');
        let mut trailing = good.clone();
        trailing.extend_from_slice(b"{\"segment_id\":2,\"key\":\"segm");
        blob.put_if_version(
            "manifest/log.jsonl",
            Bytes::from(trailing),
            ExpectedVersion::MustNotExist,
        )
        .await
        .unwrap();

        let log = new_log(Arc::clone(&blob));
        let manifest = log.load().await.unwrap();
        assert_eq!(manifest.records().len(), 1);
        log.commit(&record(2, 1, 2)).await.unwrap();
        log.commit(&record(3, 1, 3)).await.unwrap();
        let repaired = new_log(Arc::clone(&blob)).load().await.unwrap();
        assert_eq!(
            repaired.records(),
            &[record(1, 1, 1), record(2, 1, 2), record(3, 1, 3)]
        );

        let mut middle = good.clone();
        middle.extend_from_slice(b"not json\n");
        middle.extend_from_slice(&good);
        let blob = memory();
        blob.put_if_version(
            "manifest/log.jsonl",
            Bytes::from(middle),
            ExpectedVersion::MustNotExist,
        )
        .await
        .unwrap();
        let error = new_log(blob).load().await.unwrap_err();
        assert!(matches!(error, BlobError::Fatal(_)));
    }

    #[tokio::test]
    async fn append_repairs_missing_newline_and_an_entirely_invalid_object() {
        for raw in [
            serde_json::to_vec(&record(1, 1, 1)).unwrap(),
            b"{broken".to_vec(),
        ] {
            let blob = memory();
            blob.put_if_version(
                "manifest/log.jsonl",
                Bytes::from(raw.clone()),
                ExpectedVersion::MustNotExist,
            )
            .await
            .unwrap();
            let log = new_log(Arc::clone(&blob));
            log.commit(&record(2, 1, 2)).await.unwrap();
            log.commit(&record(3, 1, 3)).await.unwrap();
            let manifest = new_log(Arc::clone(&blob)).load().await.unwrap();
            let expected = if raw == b"{broken" {
                vec![record(2, 1, 2), record(3, 1, 3)]
            } else {
                vec![record(1, 1, 1), record(2, 1, 2), record(3, 1, 3)]
            };
            assert_eq!(manifest.records(), expected);
        }
    }

    #[tokio::test]
    async fn missing_manifest_is_an_empty_log() {
        let log = new_log(memory());
        let manifest = log.load().await.unwrap();
        assert!(manifest.is_empty());
        assert!(manifest.version().is_none());
    }
}
