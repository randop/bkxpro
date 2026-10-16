//! Real S3 object-store test.
//!
//! The test is skipped unless the environment describes a reachable bucket:
//!
//! * `BKXPRO_S3_TEST_BUCKET` - bucket name, for example `bkxpro-staging`
//! * `BKXPRO_S3_TEST_ACCESS_KEY`
//! * `BKXPRO_S3_TEST_SECRET_KEY`
//! * `BKXPRO_S3_TEST_ENDPOINT` - optional custom endpoint for an S3-compatible
//!   server, for example `http://127.0.0.1:9000`; omit it to talk to AWS S3
//! * `BKXPRO_S3_TEST_REGION` - optional; required for AWS S3, defaults to
//!   `us-east-1` for a custom endpoint
//!
//! It uploads one multi-part segment through the real uploader, verifies the stored
//! bytes, then reads the segment back with ranged GETs after the local file is
//! deleted:
//!
//! ```text
//! BKXPRO_S3_TEST_ENDPOINT=http://127.0.0.1:9000 \
//! BKXPRO_S3_TEST_BUCKET=bkxpro-staging \
//! BKXPRO_S3_TEST_ACCESS_KEY=localadmin \
//! BKXPRO_S3_TEST_SECRET_KEY=localdevsecret123 \
//!   cargo test -p bkxpro-uploader --test s3 -- --nocapture
//! ```

mod support;

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bkxpro_indexer::SegmentState;
use bkxpro_reader::SegmentReader;
use bkxpro_uploader::{BlobStore, SegmentIoWorker, SegmentUploader, UploaderConfig};
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    path::Path as ObjectPath,
    ObjectStoreExt,
};
use tempfile::TempDir;

use support::{index_sealed, open_index, payload_for, sealed_segment, write_segment, PlainFileIo};

const SEGMENT_ID: u64 = 1;
const LEDGER: u64 = 1;
/// One record per 4 KiB block, each smaller than the block data area, so every
/// sampled payload is physically contiguous in the segment - the shape the ranged
/// read path serves.
const PAYLOAD_LEN: usize = 4000;
/// Enough records to push the sealed segment past one S3 part.
const ENTRIES: u64 = 1408;
/// S3 requires at least 5 MiB for every part except the last, so the ~5.5 MiB
/// segment uploads as a real two-part multipart PUT.
const PART_SIZE: usize = 5 * 1024 * 1024;

struct S3Env {
    bucket: String,
    access_key: String,
    secret_key: String,
    /// Custom endpoint; `None` selects AWS S3 itself.
    endpoint: Option<String>,
    /// Explicit region; `None` lets the client resolve it (AWS only).
    region: Option<String>,
}

/// Read the test environment, or explain what to set and return `None`.
fn s3_env() -> Option<S3Env> {
    let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let (Some(bucket), Some(access_key), Some(secret_key)) = (
        var("BKXPRO_S3_TEST_BUCKET"),
        var("BKXPRO_S3_TEST_ACCESS_KEY"),
        var("BKXPRO_S3_TEST_SECRET_KEY"),
    ) else {
        eprintln!(
            "skipping S3 test: set BKXPRO_S3_TEST_BUCKET, BKXPRO_S3_TEST_ACCESS_KEY and \
             BKXPRO_S3_TEST_SECRET_KEY to run it (plus BKXPRO_S3_TEST_ENDPOINT and \
             BKXPRO_S3_TEST_REGION for an S3-compatible endpoint)"
        );
        return None;
    };
    Some(S3Env {
        bucket,
        access_key,
        secret_key,
        endpoint: var("BKXPRO_S3_TEST_ENDPOINT"),
        region: var("BKXPRO_S3_TEST_REGION"),
    })
}

fn store(env: &S3Env) -> AmazonS3 {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&env.bucket)
        .with_access_key_id(&env.access_key)
        .with_secret_access_key(&env.secret_key);
    match &env.endpoint {
        // AWS S3 itself: virtual-hosted HTTPS, and let the client resolve the region
        // from the environment or the bucket unless one is named here.
        None => {
            if let Some(region) = &env.region {
                builder = builder.with_region(region);
            }
        }
        // An S3-compatible server: `<bucket>.<endpoint>` does not resolve for a local
        // address, and such endpoints usually speak plain HTTP. Path-style addressing
        // and a region still have to be configured explicitly.
        Some(endpoint) => {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false)
                .with_allow_http(endpoint.starts_with("http://"))
                .with_region(
                    env.region
                        .clone()
                        .unwrap_or_else(|| "us-east-1".to_string()),
                );
        }
    }
    builder.build().unwrap()
}

#[tokio::test]
async fn uploads_a_multipart_segment_and_reads_it_back_with_ranged_gets() {
    let Some(env) = s3_env() else {
        return;
    };
    eprintln!(
        "S3 test: bucket {} via {}",
        env.bucket,
        env.endpoint.as_deref().unwrap_or("AWS S3")
    );
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let index = open_index(&temp.path().join("index"));

    let image = sealed_segment(SEGMENT_ID, 1, LEDGER, ENTRIES, PAYLOAD_LEN);
    assert!(image.len() > PART_SIZE, "the segment must need two parts");
    write_segment(&data_dir, SEGMENT_ID, &image);
    index_sealed(&index, SEGMENT_ID, SegmentState::Sealed, None, &image);

    // Every run gets its own keys so a shared bucket keeps no state between runs.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let run = format!("uploader-test-{}-{nanos}", std::process::id());
    let config = UploaderConfig {
        data_dir: data_dir.clone(),
        s3_prefix: format!("{run}/segments"),
        manifest_key: format!("{run}/manifest/log.jsonl"),
        part_size: PART_SIZE,
        read_chunk_bytes: 1024 * 1024,
        max_inflight_bytes: 32 * 1024 * 1024,
        stream_channel_capacity: 4,
        ..UploaderConfig::default()
    };

    let worker = SegmentIoWorker::spawn(|| Ok(PlainFileIo)).unwrap();
    let blob = Arc::new(config.blob(store(&env)));
    let uploader = SegmentUploader::new(
        config.clone(),
        Arc::clone(&index),
        Arc::clone(&blob),
        Arc::clone(&worker),
    )
    .unwrap();

    let report = uploader.process_pending().await.unwrap();
    assert_eq!(report.uploaded, 1);
    let key = config.segment_key(SEGMENT_ID, 1);

    // The stored object is the local file byte-for-byte and the local file is gone.
    let object = blob.get(&key).await.unwrap();
    assert_eq!(
        object.bytes, image,
        "the PUT must not re-encode the segment"
    );
    assert!(!config.segment_path(SEGMENT_ID).exists());
    assert_eq!(
        index.get_segment(SEGMENT_ID).unwrap().unwrap().state,
        SegmentState::Deleted
    );

    // A second pass finds only a tombstone: nothing left to examine or upload.
    let second = uploader.process_pending().await.unwrap();
    assert_eq!(second.examined, 0);
    assert_eq!(second.uploaded, 0);

    // Reads now go through the ranged-GET reader: sample the first, middle and last
    // record instead of all ~1400 entries.
    let remote = Arc::new(config.s3_reader(Arc::clone(&blob), Arc::clone(&index)));
    for entry in [1, ENTRIES / 2, ENTRIES] {
        let location = index.get_entry(LEDGER, entry).unwrap().unwrap();
        let read = remote
            .read_segment_range(SEGMENT_ID, location.offset as u64, location.len as usize)
            .await
            .unwrap();
        assert_eq!(read, payload_for(entry, PAYLOAD_LEN), "entry {entry}");
    }

    worker.shutdown().await.unwrap();

    // Best-effort cleanup of this run's objects.
    let cleanup = store(&env);
    for key in [key, config.manifest_key.clone()] {
        let _ = cleanup.delete(&ObjectPath::from(key.as_str())).await;
    }
}
