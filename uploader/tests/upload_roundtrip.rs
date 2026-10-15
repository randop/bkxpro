//! End-to-end tests over real files: a sealed segment written to disk is streamed
//! to an object store, deleted locally once the upload is committed, and then read
//! back through the read cache's S3 fallback.

mod support;

use std::{path::Path, path::PathBuf, sync::Arc, time::Duration};

use bkxpro_codec::BLOCK_SIZE;
use bkxpro_indexer::{Index, SegmentState};
use bkxpro_reader::{ReadCache, ReadCacheConfig, ReadSource};
use bkxpro_uploader::{
    BlobStore, FallbackSegmentReader, FaultyBlob, SegmentIoWorker, SegmentUploader, UploaderConfig,
    WorkerSegmentReader,
};
use bytes::Bytes;
use object_store::memory::InMemory;
use tempfile::TempDir;

use support::{
    index_sealed, open_index, payload_for, sealed_segment, set_segment_state, write_segment,
    PlainFileIo,
};

const SEGMENT_ID: u64 = 7;
const LEDGER: u64 = 1;
const ENTRIES: u64 = 4;
/// Every record starts in its own data block at this size, so each entry needs its
/// own remote read instead of sharing one cached block.
const PAYLOAD_LEN: usize = 2048;

/// Uploader tuning for tiny files: 4 KiB parts and chunks still exercise the
/// multipart path, and the byte budget comfortably covers the whole segment.
fn uploader_config(data_dir: PathBuf) -> UploaderConfig {
    UploaderConfig {
        data_dir,
        part_size: 4096,
        read_chunk_bytes: BLOCK_SIZE,
        max_inflight_bytes: 64 * 1024,
        stream_channel_capacity: 2,
        max_retries: 3,
        retry_base_backoff: Duration::from_millis(1),
        retry_max_backoff: Duration::from_millis(5),
        retry_timeout: Duration::from_secs(5),
        ..UploaderConfig::default()
    }
}

fn read_cache_config(temp: &TempDir, data_dir: &Path) -> ReadCacheConfig {
    ReadCacheConfig {
        data_dir: data_dir.to_path_buf(),
        cache_dir: temp.path().join("cache"),
        memory_tier_bytes: 8 * 1024 * 1024,
        nvme_tier_bytes: 64 * 1024 * 1024,
        block_size: 16 * 1024 * 1024,
        cache_shards: 1,
        max_concurrent_reads: 8,
        enable_compression: false,
        ..Default::default()
    }
}

/// One sealed segment on real disk plus the index rows that describe it: the
/// staging directory, the index, and the sealed image.
fn stage_segment(temp: &TempDir) -> (PathBuf, Arc<Index>, Bytes) {
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let index = open_index(&temp.path().join("index"));
    let image = sealed_segment(SEGMENT_ID, 1, LEDGER, ENTRIES, PAYLOAD_LEN);
    write_segment(&data_dir, SEGMENT_ID, &image);
    let entries = index_sealed(&index, SEGMENT_ID, SegmentState::Sealed, None, &image);
    assert_eq!(entries, (1..=ENTRIES).collect::<Vec<_>>());
    (data_dir, index, image)
}

#[tokio::test]
async fn uploads_a_real_segment_and_serves_reads_from_the_object_store() {
    let temp = TempDir::new().unwrap();
    let (data_dir, index, image) = stage_segment(&temp);
    let worker = SegmentIoWorker::spawn(|| Ok(PlainFileIo)).unwrap();
    let config = uploader_config(data_dir.clone());
    let blob = Arc::new(config.blob(InMemory::new()));
    let uploader = SegmentUploader::new(
        config.clone(),
        Arc::clone(&index),
        Arc::clone(&blob),
        Arc::clone(&worker),
    )
    .unwrap();

    let report = uploader.process_pending().await.unwrap();
    assert_eq!(report.uploaded, 1);
    assert_eq!(report.pending, 0);

    // The object is byte-identical (no re-encoding) and the local file is gone.
    let key = config.segment_key(SEGMENT_ID, 1);
    let object = blob.get(&key).await.unwrap();
    assert_eq!(object.bytes, image);
    assert!(!config.segment_path(SEGMENT_ID).exists());
    let meta = index.get_segment(SEGMENT_ID).unwrap().unwrap();
    assert_eq!(meta.state, SegmentState::Deleted);
    assert_eq!(meta.s3_key.as_deref(), Some(key.as_str()));

    // Reads resolve through the object store once the local copy is gone.
    let local = Arc::new(WorkerSegmentReader::new(
        Arc::clone(&worker),
        data_dir.clone(),
        Arc::clone(&index),
    ));
    let remote = Arc::new(config.s3_reader(Arc::clone(&blob), Arc::clone(&index)));
    let reader = Arc::new(FallbackSegmentReader::new(local, remote));
    assert!(!reader.is_local(SEGMENT_ID).await.unwrap());

    let cache = ReadCache::new(
        read_cache_config(&temp, &data_dir),
        Arc::clone(&index),
        reader,
    )
    .await
    .unwrap();
    for entry in 1..=ENTRIES {
        let result = cache.read_entry(LEDGER, entry).await.unwrap();
        assert_eq!(
            result.payload,
            payload_for(entry, PAYLOAD_LEN),
            "entry {entry}"
        );
        assert_eq!(result.source, ReadSource::S3, "entry {entry}");
    }
    let stats = cache.stats();
    assert_eq!(stats.s3_reads, ENTRIES);
    assert_eq!(stats.segment_hits, 0);

    // The remote read filled the block cache; a repeated read is a memory hit.
    let warm = cache.read_entry(LEDGER, 1).await.unwrap();
    assert_eq!(warm.payload, payload_for(1, PAYLOAD_LEN));
    assert_eq!(warm.source, ReadSource::MemoryCache);
    assert_eq!(cache.stats().memory_hits, 1);

    cache.close().await.unwrap();
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_finishes_an_interrupted_upload_without_putting_again() {
    let temp = TempDir::new().unwrap();
    let (data_dir, index, image) = stage_segment(&temp);
    let worker = SegmentIoWorker::spawn(|| Ok(PlainFileIo)).unwrap();
    let config = uploader_config(data_dir.clone());
    let blob = Arc::new(FaultyBlob::new(Arc::new(config.blob(InMemory::new()))));

    let first = SegmentUploader::new(
        config.clone(),
        Arc::clone(&index),
        Arc::clone(&blob),
        Arc::clone(&worker),
    )
    .unwrap();
    assert_eq!(first.process_pending().await.unwrap().uploaded, 1);
    let puts_after_first_pass = blob.put_attempts();

    // Reconstruct the crash window "manifest committed, local cleanup not done":
    // the segment file is back on disk and the index still says Uploading.
    write_segment(&data_dir, SEGMENT_ID, &image);
    set_segment_state(&index, SEGMENT_ID, SegmentState::Uploading);

    // Every PUT now fails: finishing the window must be driven by the manifest
    // record, never by uploading the segment again.
    blob.with_plan(|plan| plan.put_persistent_failure = true);
    let second = SegmentUploader::new(
        config.clone(),
        Arc::clone(&index),
        Arc::clone(&blob),
        Arc::clone(&worker),
    )
    .unwrap();

    let report = second.recover().await.unwrap();
    assert_eq!(report.reconciled_uploaded, 1);
    assert_eq!(report.reconciled_deleted, 1);
    assert_eq!(
        blob.put_attempts(),
        puts_after_first_pass,
        "the committed manifest record must gate the PUT"
    );
    assert!(!config.segment_path(SEGMENT_ID).exists());
    assert_eq!(
        index.get_segment(SEGMENT_ID).unwrap().unwrap().state,
        SegmentState::Deleted
    );

    worker.shutdown().await.unwrap();
}
