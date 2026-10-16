//! Real-endpoint S3 runs.
//!
//! The simulated scenarios prove the durability invariants against the
//! fault-injecting stand-in; this module drives the same lifecycle against a
//! live S3-compatible endpoint (minio locally, AWS or minio in CI) so the
//! object-store client, multipart PUTs, conditional manifest writes, and ranged
//! GETs are exercised end to end.
//!
//! Chaos is injected from outside the process (the script kills and restarts
//! the endpoint while the run is in flight), so the driver is deliberately
//! boring: write every segment sealed, restart (index rebuild + open-segment
//! repair), then run upload passes in a deadline-bounded settle loop that keeps
//! retrying while the endpoint is down. Only a poisoned uploader (fencing,
//! corruption, a fail-stop condition) ends the loop early; anything else is a
//! transient outage the run must ride out without losing a local byte.
//!
//! Every run uses a unique `<prefix>/segments` and `<prefix>/manifest/log.jsonl`
//! so a shared bucket keeps no state between runs; unless `--keep` is set (or a
//! violation is found), the run deletes its own objects afterwards.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bkxpro_uploader::{BlobStore, ObjectStoreBlob};
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    path::Path as ObjectPath,
    ObjectStoreExt,
};
use serde::{Deserialize, Serialize};

use crate::{
    lifecycle::{HarnessError, RemoteVerifyReport, Session, StoreKeys},
    plan::{preset, Plan},
    scenarios::{write_all_sealed, ReadBackSummary},
};

/// Connection, workload, and retry policy of one real-endpoint run.
#[derive(Debug, Clone)]
pub struct RealS3Config {
    /// Custom endpoint (`http://127.0.0.1:9000` for minio); `None` selects AWS.
    pub endpoint: Option<String>,
    /// Explicit region; `None` lets the client resolve it on AWS, and defaults
    /// to `us-east-1` for a custom endpoint.
    pub region: Option<String>,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// Object-key prefix. Empty selects a unique `validator/<seed>-<pid>-<ns>`.
    pub prefix: String,
    pub seed: u64,
    pub segments: u64,
    pub entries_per_segment: u64,
    pub payload_bytes: usize,
    /// Multipart part size for segment PUTs. Real S3 requires at least 5 MiB per
    /// non-final part; 8 MiB keeps whole small segments as single-part PUTs.
    pub part_size: usize,
    /// How long the settle loop may keep retrying while the endpoint is down.
    /// Must exceed the total outage the chaos script injects.
    pub settle_timeout: Duration,
    /// Keep the uploaded objects for inspection instead of cleaning them up.
    pub keep: bool,
}

impl Default for RealS3Config {
    fn default() -> Self {
        Self {
            endpoint: None,
            region: None,
            bucket: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            prefix: String::new(),
            seed: 1,
            segments: 3,
            entries_per_segment: 40,
            payload_bytes: 96,
            part_size: 8 * 1024 * 1024,
            settle_timeout: Duration::from_secs(120),
            keep: false,
        }
    }
}

impl RealS3Config {
    /// Fill connection fields from the `BKXPRO_S3_TEST_*` variables (the same
    /// contract as the uploader's real-S3 integration test); workload fields keep
    /// their defaults. Explicit CLI flags override these afterwards.
    pub fn from_env() -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let (Some(bucket), Some(access_key), Some(secret_key)) = (
            var("BKXPRO_S3_TEST_BUCKET"),
            var("BKXPRO_S3_TEST_ACCESS_KEY"),
            var("BKXPRO_S3_TEST_SECRET_KEY"),
        ) else {
            return Err("set BKXPRO_S3_TEST_BUCKET, BKXPRO_S3_TEST_ACCESS_KEY and \
                 BKXPRO_S3_TEST_SECRET_KEY (plus BKXPRO_S3_TEST_ENDPOINT and \
                 BKXPRO_S3_TEST_REGION for an S3-compatible endpoint)"
                .to_string());
        };
        Ok(Self {
            endpoint: var("BKXPRO_S3_TEST_ENDPOINT"),
            region: var("BKXPRO_S3_TEST_REGION"),
            bucket,
            access_key,
            secret_key,
            ..Self::default()
        })
    }

    /// The object-key prefix this run uses.
    pub fn resolved_prefix(&self) -> String {
        let prefix = self.prefix.trim_matches('/');
        if !prefix.is_empty() {
            return prefix.to_string();
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("validator/{}-{}-{nanos}", self.seed, std::process::id())
    }

    /// The plan this run drives: the smoke workload with the configured volume
    /// and a real-S3-friendly uploader shape. The writer's entry/target bounds
    /// are raised so one work segment really holds `entries_per_segment` records
    /// (large enough workloads then exercise multi-part PUTs).
    pub fn plan(&self) -> Plan {
        let mut plan = preset("smoke", self.seed).expect("the smoke preset exists");
        plan.name = "real-s3".to_string();
        plan.work.segments = self.segments;
        plan.work.entries_per_segment = self.entries_per_segment;
        plan.work.payload_bytes = self.payload_bytes;
        plan.uploader.part_size = self.part_size;
        // The mock presets tune for tiny segments; a real endpoint wants bigger
        // read chunks, a bigger in-flight budget, and production-style retries
        // so an outage is ridden inside one pass as well as by the settle loop.
        plan.uploader.read_chunk_bytes = 1024 * 1024;
        plan.uploader.max_inflight_bytes = 16 * 1024 * 1024;
        plan.uploader.max_retries = 8;
        plan.uploader.retry_base_ms = 100;
        plan.uploader.retry_max_ms = 5_000;
        plan.uploader.retry_timeout_ms = 120_000;
        let entries = self.entries_per_segment as usize;
        // One record can occupy a full 4 KiB block; keep headroom for the
        // encoder's packing and the seal trailer.
        plan.writer.max_entries = entries + 16;
        plan.writer.target_data_bytes = (entries + 16) * crate::plan::BLOCK;
        plan
    }

    /// Command that reproduces this run's workload (not its prefix, which is
    /// unique per run).
    pub fn replay_command(&self) -> String {
        format!(
            "cargo run -p bkxpro-validator --bin bkxpro-validator -- s3-run --seed {} \
             --segments {} --entries {} --payload-bytes {}",
            self.seed, self.segments, self.entries_per_segment, self.payload_bytes,
        )
    }
}

/// The complete, serializable result of one real-endpoint run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealS3Report {
    pub scenario: String,
    pub seed: u64,
    /// Endpoint description: the URL, or `AWS S3` for the default endpoint.
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    /// True when no invariant or scenario assertion was violated.
    pub ok: bool,
    pub violations: Vec<String>,
    pub notes: Vec<String>,
    pub passes: u32,
    pub settle_ms: u64,
    pub uploaded: usize,
    pub remote_reads: usize,
    pub read_backs: Vec<ReadBackSummary>,
    pub remote_verify: RemoteVerifyReport,
    pub manifest_rows: usize,
    pub acked_entries: usize,
    /// False when the run kept its objects (--keep or a violation).
    pub cleaned: bool,
    pub deleted_objects: usize,
    pub duration_ms: u64,
    pub replay: String,
}

/// Run one real-endpoint session and verify it end to end.
pub fn run_real_s3(config: &RealS3Config) -> Result<RealS3Report, HarnessError> {
    let started = Instant::now();
    let plan = config.plan();
    let prefix = config.resolved_prefix();
    let keys = StoreKeys {
        s3_prefix: format!("{prefix}/segments"),
        manifest_key: format!("{prefix}/manifest/log.jsonl"),
    };
    let store = build_store(config).map_err(HarnessError::Message)?;
    let blob: Arc<dyn BlobStore> =
        Arc::new(ObjectStoreBlob::new(store.clone()).with_write_concurrency(2));
    let mut session = Session::with_store(&plan, blob, keys.clone())?;

    let mut notes = Vec::new();
    let mut violations = Vec::new();

    // A prefix that already carries a manifest belongs to an earlier run: the
    // uploader would reconcile those segments without a PUT, which is correct
    // behavior but not what this run's delete-gating proof is about.
    let existing = session.manifest_records()?;
    if !existing.is_empty() {
        return Err(HarnessError::Message(format!(
            "prefix {prefix} already holds a manifest with {} row(s); use a unique prefix \
             (or delete {} first)",
            existing.len(),
            keys.manifest_key,
        )));
    }

    // Phase 1: write every segment sealed, exactly like the happy path.
    let entries = write_all_sealed(&mut session, &plan)?;
    let restart = session.restart()?;
    notes.push(format!(
        "restart rebuilt {} segment(s), indexed {} entries, repaired {}",
        restart.segments_rebuilt,
        restart.entries_indexed,
        restart.repairs.len(),
    ));
    session.step("write-complete");

    // Phase 2: settle the uploads, riding out endpoint outages until the deadline.
    let settle_started = Instant::now();
    let deadline = settle_started + config.settle_timeout;
    let mut passes = 0u32;
    let mut uploaded = 0usize;
    loop {
        passes += 1;
        match session.upload_pass() {
            Ok(report) => {
                uploaded += report.uploaded;
                if report.examined == 0 {
                    notes.push(format!("uploads settled after {passes} pass(es)"));
                    break;
                }
                if report.pending > 0 {
                    notes.push(format!(
                        "{} segment(s) still pending; retrying",
                        report.pending
                    ));
                }
            }
            Err((report, error)) => {
                uploaded += report.uploaded;
                session.note_upload_error(&error);
                if session.uploader_poisoned() {
                    violations.push(format!("uploader poisoned: {error}"));
                    break;
                }
                notes.push(format!("transient upload failure (retrying): {error}"));
            }
        }
        if Instant::now() >= deadline {
            violations.push(format!(
                "uploads did not settle within {}s",
                config.settle_timeout.as_secs(),
            ));
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let settle_ms = settle_started.elapsed().as_millis() as u64;
    session.step("settled");

    // Phase 3: prove every acknowledged entry is still readable and durable.
    let mut read_backs = Vec::new();
    let mut remote_reads = 0usize;
    let mut remote_verify = RemoteVerifyReport::default();
    let mut manifest_rows = 0usize;
    if violations.is_empty() {
        let remote = session.read_back(&entries)?;
        remote_reads = remote.served_remote;
        read_backs.push(ReadBackSummary {
            source: "remote".to_string(),
            report: remote,
        });
        let epochs: Vec<(u64, u64)> = session
            .sealed_epochs
            .iter()
            .map(|(segment_id, epoch)| (*segment_id, *epoch))
            .collect();
        remote_verify = session.verify_remote(&epochs)?;
        manifest_rows = session.manifest_records()?.len();
        notes.push(format!(
            "manifest committed {manifest_rows} row(s); remote verify checked {} segment(s), {} record(s)",
            remote_verify.segments_checked, remote_verify.records_checked,
        ));
    }
    violations.extend(
        remote_verify
            .failures
            .iter()
            .map(|failure| format!("remote verify: {failure}")),
    );
    violations.extend(session.check_invariants());

    // Phase 4: remove this run's objects from the shared bucket. A failed run
    // keeps them for inspection instead, and cleanup itself is deadline-bounded
    // and best-effort: a brief outage at the end must not turn a proven run into
    // a failure, only leave a loud note.
    let mut deleted_objects = 0usize;
    let mut cleaned = false;
    if config.keep || !violations.is_empty() {
        if !config.keep {
            notes.push(format!(
                "objects kept for inspection under {prefix} (violations present)"
            ));
        } else {
            notes.push(format!("objects kept under {prefix} (--keep)"));
        }
    } else {
        match cleanup(config, &plan, &keys) {
            Ok(deleted) => {
                deleted_objects = deleted;
                cleaned = true;
                notes.push(format!("deleted {deleted_objects} object(s)"));
            }
            Err(error) => notes.push(format!(
                "cleanup failed ({error}); objects under {prefix} were left behind"
            )),
        }
    }

    Ok(RealS3Report {
        scenario: plan.name.clone(),
        seed: config.seed,
        endpoint: config
            .endpoint
            .clone()
            .unwrap_or_else(|| "AWS S3".to_string()),
        bucket: config.bucket.clone(),
        prefix,
        ok: violations.is_empty(),
        violations,
        notes,
        passes,
        settle_ms,
        uploaded,
        remote_reads,
        read_backs,
        remote_verify,
        manifest_rows,
        acked_entries: session.acked.len(),
        cleaned,
        deleted_objects,
        duration_ms: started.elapsed().as_millis() as u64,
        replay: config.replay_command(),
    })
}

/// Build the `AmazonS3` client the way the uploader's real-S3 test does: AWS
/// uses virtual-hosted addressing and resolves the region; an S3-compatible
/// endpoint gets path-style addressing, plain HTTP when named, and an explicit
/// region.
fn build_store(config: &RealS3Config) -> Result<AmazonS3, String> {
    if config.bucket.is_empty() {
        return Err("an S3 bucket is required".to_string());
    }
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_access_key_id(&config.access_key)
        .with_secret_access_key(&config.secret_key);
    match &config.endpoint {
        None => {
            if let Some(region) = &config.region {
                builder = builder.with_region(region);
            }
        }
        Some(endpoint) => {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false)
                .with_allow_http(endpoint.starts_with("http://"))
                .with_region(
                    config
                        .region
                        .clone()
                        .unwrap_or_else(|| "us-east-1".to_string()),
                );
        }
    }
    builder
        .build()
        .map_err(|error| format!("building the S3 client: {error}"))
}

/// Delete this run's segment objects and manifest, retrying until the settle
/// deadline so an outage at the end of the run does not strand objects. Only the
/// keys the harness itself writes are touched; a missing object counts as deleted.
///
/// A fresh client is built here on purpose: the run's client keeps its pooled
/// connections on the session's runtime, and driving them from this process's
/// short-lived cleanup runtime would hang until the request timeout.
fn cleanup(config: &RealS3Config, plan: &Plan, keys: &StoreKeys) -> Result<usize, HarnessError> {
    let store = build_store(config).map_err(HarnessError::Message)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| HarnessError::Message(format!("cleanup runtime: {error}")))?;
    let deadline = Instant::now() + config.settle_timeout;
    runtime.block_on(async {
        loop {
            match cleanup_once(&store, plan, keys).await {
                Ok(deleted) => return Ok(deleted),
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(error);
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    })
}

async fn cleanup_once(
    store: &AmazonS3,
    plan: &Plan,
    keys: &StoreKeys,
) -> Result<usize, HarnessError> {
    let mut deleted = 0usize;
    let prefix = keys.s3_prefix.trim_matches('/').to_string();
    for segment_id in 0..plan.work.segments {
        let key = format!("{prefix}/{segment_id:020}.seg");
        match store.delete(&ObjectPath::from(key.as_str())).await {
            Ok(()) => deleted += 1,
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                return Err(HarnessError::Message(format!(
                    "cleanup: deleting {key}: {error}"
                )))
            }
        }
    }
    match store
        .delete(&ObjectPath::from(keys.manifest_key.as_str()))
        .await
    {
        Ok(()) => deleted += 1,
        Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => {
            return Err(HarnessError::Message(format!(
                "cleanup: deleting {}: {error}",
                keys.manifest_key
            )))
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_unique_prefix_is_per_run_and_well_formed() {
        let config = RealS3Config {
            seed: 4,
            ..RealS3Config::default()
        };
        let prefix = config.resolved_prefix();
        assert!(
            prefix.starts_with("validator/4-"),
            "unexpected prefix {prefix}"
        );
        assert!(!prefix.contains("//"), "no empty path segment");
    }

    #[test]
    fn an_explicit_prefix_is_normalized() {
        let config = RealS3Config {
            prefix: "chaos/run/".to_string(),
            ..RealS3Config::default()
        };
        assert_eq!(config.resolved_prefix(), "chaos/run");
    }

    #[test]
    fn the_plan_carries_the_configured_volume() {
        let config = RealS3Config {
            seed: 9,
            segments: 2,
            entries_per_segment: 512,
            payload_bytes: 4000,
            ..RealS3Config::default()
        };
        let plan = config.plan();
        assert_eq!(plan.name, "real-s3");
        assert_eq!(plan.seed, 9);
        assert_eq!(plan.work.segments, 2);
        assert_eq!(plan.work.entries_per_segment, 512);
        assert_eq!(plan.work.payload_bytes, 4000);
        // The writer must not cap a run short of the configured entries, and
        // the target must be able to hold them (one record per 4 KiB block).
        assert!(plan.writer.max_entries > 512, "{plan:?}");
        assert!(plan.writer.target_data_bytes >= 512 * crate::plan::BLOCK);
        assert!(plan.uploader.part_size % crate::plan::BLOCK == 0);
        assert!(config.replay_command().contains("s3-run --seed 9"));
    }
}
