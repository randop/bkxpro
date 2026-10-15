//! Uploader configuration.
//!
//! The defaults target the reference deployment: 8 MiB multipart parts (S3 requires
//! at least 5 MiB per non-final part), 4 MiB aligned read chunks, and a 32 MiB
//! in-flight byte budget shared with the read path's buffer pool accounting.

use std::{path::PathBuf, sync::Arc, time::Duration};

use bkxpro_codec::BLOCK_SIZE;

use crate::{
    blob::{BlobStore, ObjectStoreBlob},
    reader::S3SegmentReader,
};

/// One segment id -> object key: `segments/{segment_id:020}/{writer_epoch:020}.seg`.
pub const DEFAULT_SEGMENT_PREFIX: &str = "segments";
/// Default key of the append-only upload manifest.
pub const DEFAULT_MANIFEST_KEY: &str = "manifest/log.jsonl";

/// Uploader tuning. All byte counts are validated for alignment and budget
/// consistency by [`UploaderConfig::validate`].
#[derive(Debug, Clone)]
pub struct UploaderConfig {
    /// Directory holding sealed segment files (`{segment_id}.seg`).
    pub data_dir: PathBuf,
    /// Object key prefix for segment objects.
    pub s3_prefix: String,
    /// Object key of the JSONL manifest that gates local deletion.
    pub manifest_key: String,
    /// Multipart part size. S3 requires >= 5 MiB for every part except the last.
    pub part_size: usize,
    /// Maximum number of parts uploaded concurrently per segment; applied by
    /// [`UploaderConfig::blob`].
    pub write_concurrency: usize,
    /// Aligned read chunk size handed to the blob store. Must be a 4 KiB multiple.
    pub read_chunk_bytes: usize,
    /// Bounded byte budget for in-flight read chunks (uploader backpressure).
    pub max_inflight_bytes: usize,
    /// Bounded channel capacity between the reader thread and the blob store.
    pub stream_channel_capacity: usize,
    /// Attempts per PUT (first attempt plus retries).
    pub max_retries: u32,
    /// First retry delay; subsequent delays double up to `retry_max_backoff`.
    pub retry_base_backoff: Duration,
    /// Retry delay ceiling.
    pub retry_max_backoff: Duration,
    /// Overall deadline for one segment's upload attempts.
    pub retry_timeout: Duration,
    /// CAS attempts when appending to the manifest.
    pub manifest_max_cas_retries: u32,
    /// Sleep between `process_pending` passes in [`crate::SegmentUploader::run`].
    pub poll_interval: Duration,
    /// Cached segment digests (header/footer/trailer) in the ranged-GET reader;
    /// applied by [`UploaderConfig::s3_reader`].
    pub s3_digest_cache_entries: usize,
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/bkxpro/segments"),
            s3_prefix: DEFAULT_SEGMENT_PREFIX.to_string(),
            manifest_key: DEFAULT_MANIFEST_KEY.to_string(),
            part_size: 8 * 1024 * 1024,
            write_concurrency: 4,
            read_chunk_bytes: 4 * 1024 * 1024,
            max_inflight_bytes: 32 * 1024 * 1024,
            stream_channel_capacity: 8,
            max_retries: 8,
            retry_base_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_secs(10),
            retry_timeout: Duration::from_secs(15 * 60),
            manifest_max_cas_retries: 8,
            poll_interval: Duration::from_secs(1),
            s3_digest_cache_entries: 1024,
        }
    }
}

impl UploaderConfig {
    /// Wrap an object store with this configuration's part concurrency.
    pub fn blob<S: object_store::ObjectStore>(&self, store: S) -> ObjectStoreBlob<S> {
        ObjectStoreBlob::new(store).with_write_concurrency(self.write_concurrency)
    }

    /// Build the ranged-GET reader used for segments that no longer exist locally,
    /// with this configuration's digest cache bound.
    pub fn s3_reader<B: BlobStore>(
        &self,
        blob: Arc<B>,
        index: Arc<bkxpro_indexer::Index>,
    ) -> S3SegmentReader<B> {
        S3SegmentReader::new(blob, index).with_digest_cache_entries(self.s3_digest_cache_entries)
    }

    /// Object key for one segment generation; competing epochs never share bytes.
    pub fn segment_key(&self, segment_id: u64, writer_epoch: u64) -> String {
        let prefix = self.s3_prefix.trim_matches('/');
        if prefix.is_empty() {
            format!("{segment_id:020}/{writer_epoch:020}.seg")
        } else {
            format!("{prefix}/{segment_id:020}/{writer_epoch:020}.seg")
        }
    }

    /// Local path of one segment file. The name matches the read cache and the
    /// segment scan convention (`{segment_id}.seg`).
    pub fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.data_dir.join(format!("{segment_id}.seg"))
    }

    /// Reject configurations that would silently break alignment or budgeting.
    pub fn validate(&self) -> Result<(), String> {
        if self.part_size == 0 {
            return Err("part_size must be non-zero".to_string());
        }
        if self.part_size % BLOCK_SIZE != 0 {
            return Err(format!(
                "part_size ({}) must be a multiple of the {BLOCK_SIZE}-byte block size",
                self.part_size
            ));
        }
        if self.write_concurrency == 0 {
            return Err("write_concurrency must be non-zero".to_string());
        }
        if self.read_chunk_bytes == 0 || self.read_chunk_bytes % BLOCK_SIZE != 0 {
            return Err(format!(
                "read_chunk_bytes ({}) must be a non-zero multiple of {BLOCK_SIZE}",
                self.read_chunk_bytes
            ));
        }
        if self.max_inflight_bytes < self.read_chunk_bytes {
            return Err(format!(
                "max_inflight_bytes ({}) must be at least read_chunk_bytes ({})",
                self.max_inflight_bytes, self.read_chunk_bytes
            ));
        }
        if self.stream_channel_capacity == 0 {
            return Err("stream_channel_capacity must be non-zero".to_string());
        }
        if self.manifest_key.is_empty() {
            return Err("manifest_key must not be empty".to_string());
        }
        if self.max_retries == 0 {
            return Err("max_retries must be at least 1".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        UploaderConfig::default().validate().unwrap();
    }

    #[test]
    fn segment_key_is_zero_padded_and_prefixed() {
        let config = UploaderConfig::default();
        assert_eq!(
            config.segment_key(7, 9),
            "segments/00000000000000000007/00000000000000000009.seg"
        );

        let config = UploaderConfig {
            s3_prefix: "prod/segments/".to_string(),
            ..UploaderConfig::default()
        };
        assert_eq!(
            config.segment_key(7, 9),
            "prod/segments/00000000000000000007/00000000000000000009.seg"
        );

        let config = UploaderConfig {
            s3_prefix: String::new(),
            ..UploaderConfig::default()
        };
        assert_eq!(
            config.segment_key(7, 9),
            "00000000000000000007/00000000000000000009.seg"
        );
    }

    #[test]
    fn segment_path_matches_read_cache_convention() {
        let config = UploaderConfig {
            data_dir: PathBuf::from("/data/segments"),
            ..UploaderConfig::default()
        };
        assert_eq!(
            config.segment_path(42),
            PathBuf::from("/data/segments/42.seg")
        );
    }

    #[test]
    fn invalid_configs_are_rejected() {
        let base = UploaderConfig::default();

        let mut config = base.clone();
        config.part_size = 4096 + 1;
        assert!(config.validate().is_err());

        let mut config = base.clone();
        config.read_chunk_bytes = 4096 + 1;
        assert!(config.validate().is_err());

        let mut config = base.clone();
        config.max_inflight_bytes = config.read_chunk_bytes - 1;
        assert!(config.validate().is_err());

        let mut config = base.clone();
        config.write_concurrency = 0;
        assert!(config.validate().is_err());

        let mut config = base;
        config.max_retries = 0;
        assert!(config.validate().is_err());
    }
}
