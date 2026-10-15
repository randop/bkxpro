//! Streaming S3 uploader with manifest-gated segment deletion (RAN-130).
//!
//! Sealed segments move to the object store as one PUT each (multipart for large
//! files) and are removed from local disk only after two durable facts are
//! established: the PUT completed and a compare-and-swap append to the upload
//! manifest landed. Every intermediate crash window is repaired by
//! [`SegmentUploader::recover`], and a segment that cannot be proven remote is
//! never deleted.
//!
//! Module map:
//!
//! * [`config`] - tuning knobs, object-key layout, and local path layout.
//! * [`blob`] - the [`BlobStore`] seam, its `object_store` implementation, and the
//!   fault-injecting test double.
//! * [`manifest`] - the CAS append-only JSONL manifest and writer-epoch fencing.
//! * [`worker`] - the dedicated I/O thread that owns the thread-bound
//!   [`bkxpro_segmenter::SegmentIo`] backend (aligned reads, unlink).
//! * [`budget`] - the in-flight byte budget that backpressures the reader.
//! * [`digest`] - sealed-segment validation shared by the uploader and the S3 reader.
//! * [`reader`] - local/remote readers, including the ranged-GET fallback.
//! * [`uploader`] - the pipeline itself.

pub mod blob;
pub mod budget;
pub mod config;
pub mod digest;
pub mod ioprio;
pub mod manifest;
pub mod reader;
pub mod uploader;
pub mod worker;

#[cfg(test)]
pub(crate) mod test_util;

pub use blob::{
    BlobError, BlobObject, BlobStore, ExpectedVersion, FaultPlan, FaultyBlob, ObjectInfo,
    ObjectStoreBlob, ObjectVersion, PutOutcome, StreamItem,
};
pub use budget::{BudgetGuard, ByteBudget};
pub use config::{UploaderConfig, DEFAULT_MANIFEST_KEY, DEFAULT_SEGMENT_PREFIX};
pub use digest::{footer_bounds, DigestError, SegmentDigest};
pub use ioprio::{current_thread_priority, set_current_thread_priority, IoPriority};
pub use manifest::{CommitOutcome, Manifest, ManifestLog, ManifestRecord};
pub use reader::{FallbackSegmentReader, S3SegmentReader, WorkerSegmentReader};
pub use uploader::{Advance, RecoveryReport, SegmentUploader, UploadReport, UploaderStats};
pub use worker::{AlignedChunk, SegmentIoWorker, WorkerError};

use thiserror::Error;

/// Every failure the uploader can surface.
#[derive(Debug, Error)]
pub enum UploaderError {
    /// Object-store failure.
    #[error("blob store: {0}")]
    Blob(#[from] BlobError),
    /// Index failure.
    #[error("index: {0}")]
    Index(#[from] bkxpro_indexer::IndexError),
    /// Local I/O worker failure.
    #[error("segment I/O worker: {0}")]
    Worker(#[from] WorkerError),
    /// Invalid configuration rejected at construction time.
    #[error("invalid uploader configuration: {0}")]
    Config(String),
    /// The local file vanished and no manifest record covers the segment.
    #[error("local segment {segment_id} is missing and the manifest does not record it")]
    LocalSegmentMissing { segment_id: u64 },
    /// A newer writer epoch owns this segment id.
    #[error(
        "segment {segment_id} is fenced by manifest epoch {manifest_epoch} \
         (local epoch {local_epoch})"
    )]
    Fenced {
        segment_id: u64,
        manifest_epoch: u64,
        local_epoch: u64,
    },
    /// The local file is not a valid sealed segment.
    #[error("segment {segment_id} failed codec validation: {detail}")]
    CorruptSegment { segment_id: u64, detail: String },
    /// The uploaded object's length does not match the local file.
    #[error("uploaded segment {segment_id} is {remote} bytes; the local file is {local} bytes")]
    VerificationMismatch {
        segment_id: u64,
        remote: u64,
        local: u64,
    },
    /// A manifest record contradicts the local file.
    #[error("manifest record for segment {segment_id} contradicts the local file")]
    ManifestConflict { segment_id: u64 },
    /// All PUT attempts for one segment failed; the local bytes are intact.
    #[error("upload retries exhausted for segment {segment_id}: {detail}")]
    RetriesExhausted { segment_id: u64, detail: String },
    /// A fatal inconsistency stopped the uploader; an operator must intervene.
    #[error("uploader is poisoned: {reason}")]
    Poisoned { reason: String },
    /// The I/O thread could not be started.
    #[error("segment I/O worker failed to start: {0}")]
    WorkerStart(String),
}

/// Index metadata used by the crate's tests. `writer_epoch` starts at zero; the
/// uploader overwrites it with the digest's authoritative epoch.
#[cfg(test)]
pub(crate) fn test_meta(
    state: bkxpro_indexer::SegmentState,
    s3_key: Option<&str>,
    byte_size: u64,
) -> bkxpro_indexer::SegmentMeta {
    bkxpro_indexer::SegmentMeta {
        state,
        s3_key: s3_key.map(str::to_string),
        min_ledger: 1,
        max_ledger: 1,
        byte_size,
        writer_epoch: 0,
    }
}
