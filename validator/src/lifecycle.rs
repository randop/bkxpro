//! The simulated lifecycle: writer -> seal -> index -> uploader -> crash/restart
//! -> recovery -> read-back, all against the deterministic storage backend.
//!
//! A [`Session`] owns one fresh simulated environment (a temp directory, the
//! simulated device, the fjall index, the fault-injecting S3 stand-in, and the
//! uploader's I/O worker thread). Scenarios drive it through the phase primitives
//! on this type; [`crate::scenarios::run_plan`] is the preset driver.
//!
//! Three durability models are kept distinct, because conflating them is how
//! storage tests lie:
//!
//! * **process death** ([`Session::restart`] without [`Session::crash_device`]):
//!   in-memory state is gone, every byte ever written is still there. The index is
//!   wiped and rebuilt from segments, which is the only state a new process has.
//! * **power loss** ([`Session::crash_device`]): unsynced bytes and
//!   directory entries that were never synced are discarded, exactly like a real
//!   power cut. Anything that was acknowledged through `commit_batch`/`seal` must
//!   survive.
//! * **unclean unlink** ([`SharedSimIo::arm_unlink_failure_after_removal`]): the
//!   file is gone but the caller saw an error, modelling a crash between a durable
//!   unlink and the index state change.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

use bytes::Bytes;
use bkxpro_codec::{
    AlignedSegmentEncoder, CodecError, SegmentEncoder, SegmentHeader, SegmentView, TrailerWire,
    BLOCK_SIZE, HEADER_SIZE, RECORD_HEADER_SIZE, TRAILER_SIZE,
};
use bkxpro_indexer::{
    EntryLocation, Index, IndexConfig, IndexError, SegmentMeta, SegmentState,
};
use bkxpro_reader::SegmentReader;
use bkxpro_segmenter::{
    AlignedBuffer, SegmentIo, SegmentWriter, WriterConfig, WriteOp,
};
use bkxpro_uploader::{
    BlobError, BlobObject, BlobStore, ExpectedVersion, FallbackSegmentReader, ManifestRecord,
    ObjectInfo, PutOutcome, RecoveryReport, S3SegmentReader, SegmentIoWorker, SegmentUploader,
    StreamItem, UploadReport, UploaderConfig, UploaderError, UploaderStats, WorkerError,
    WorkerSegmentReader,
};
use tempfile::TempDir;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{
    mock_s3::{MockS3, MANIFEST_KEY},
    plan::{DiskFault, FaultArm, Plan},
    rng::DetRng,
    sim_io::{trace_file_label, SharedSimIo, SimulatedCounters, SimulatedFault},
    trace::{Trace, TraceEvent},
};

/// Everything that can go wrong while driving the environment. Faults *injected
/// into the system under test* are values (see [`WriteOutcome`]), not errors.
#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("harness io: {0}")]
    Io(#[from] io::Error),
    #[error("index: {0}")]
    Index(#[from] IndexError),
    #[error("codec: {0}")]
    Codec(#[from] CodecError),
    #[error("uploader: {0}")]
    Uploader(#[from] UploaderError),
    #[error("worker: {0}")]
    Worker(#[from] WorkerError),
    #[error("writer: {0}")]
    Writer(String),
    #[error("{0}")]
    Message(String),
}

impl HarnessError {
    fn message(context: impl AsRef<str>, error: impl std::fmt::Display) -> Self {
        Self::Message(format!("{}: {error}", context.as_ref()))
    }
}

/// One acknowledged entry: the client was told this (ledger, entry) is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Acked {
    pub ledger: u64,
    pub entry: u64,
    pub segment_id: u64,
    /// Sequence number within the segment.
    pub sequence: u64,
}

/// One entry to append.
#[derive(Debug, Clone)]
pub struct AppendSpec {
    pub ledger: u64,
    pub entry: u64,
    pub flags: u8,
    pub payload: Bytes,
}

/// How a write phase ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteEnd {
    /// The segment was sealed durably.
    Sealed { file_len: usize },
    /// The loop stopped (per `stop_after`) with the segment still open.
    Stopped,
    /// The write loop finished with the segment still open.
    Open,
    /// A disk fault failed the writer (which poisons itself).
    Failed,
}

/// Result of one write phase.
#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub segment_id: u64,
    pub epoch: u64,
    pub end: WriteEnd,
    /// Every acknowledgement the writer returned during this phase.
    pub acked: Vec<Acked>,
    /// Every record the encoder accepted (acked or not).
    pub records: Vec<(u64, u64)>,
    /// The writer error, when the phase failed.
    pub error: Option<String>,
}

/// What an index rebuild plus open-segment repair recovered.
#[derive(Debug, Clone, Default)]
pub struct RestartReport {
    pub segments_rebuilt: usize,
    pub segments_failed: usize,
    pub entries_indexed: usize,
    pub repairs: Vec<RepairReport>,
}

#[derive(Debug, Clone)]
pub struct RepairReport {
    pub segment_id: u64,
    pub epoch: u64,
    pub records: u64,
    pub bytes_before: usize,
    pub bytes_after: usize,
    /// Records in the recovered prefix that were never acknowledged.
    pub unacked_recovered: Vec<(u64, u64)>,
}

/// One read-back verification result.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReadBackReport {
    pub checked: usize,
    pub served_local: usize,
    pub served_remote: usize,
    /// Entries skipped because the (rebuilt) index no longer had a location for
    /// them; only non-zero for lenient read-backs.
    pub skipped: usize,
    pub failures: Vec<String>,
}

/// Remote durability proof for uploaded segments, independent of the index: the
/// manifest row, the stored object's length/checksum, a full sealed-segment parse,
/// and the payload of every acknowledged entry in that segment.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteVerifyReport {
    pub segments_checked: usize,
    pub records_checked: usize,
    pub failures: Vec<String>,
}

/// The simulated environment.
pub struct Session {
    root: TempDir,
    pub data_dir: PathBuf,
    index_dir: PathBuf,
    pub trace: Arc<Trace>,
    pub sim: SharedSimIo,
    /// The fault-injecting S3 stand-in, when this session runs on the mock. A
    /// session built with [`Session::with_store`] (a real endpoint) has no mock;
    /// mock-only scenario controls must go through [`Session::mock`].
    pub mock: Option<Arc<MockS3>>,
    traced: Arc<TracingBlob>,
    index: Option<Arc<Index>>,
    worker: Arc<SegmentIoWorker>,
    uploader: Option<Arc<SegmentUploader<TracingBlob>>>,
    runtime: tokio::runtime::Runtime,
    uploader_thread: Option<(Arc<AtomicBool>, JoinHandle<()>)>,
    pub config: SessionConfig,
    /// The client-facing acknowledgement journal; survives every restart.
    pub acked: Vec<Acked>,
    /// Expected payload of every entry the run wrote.
    pub payloads: BTreeMap<(u64, u64), Vec<u8>>,
    /// Epoch of every sealed or repaired segment.
    pub sealed_epochs: BTreeMap<u64, u64>,
    /// Records observed to be durable (sealed or recovered from disk).
    pub durable_records: BTreeSet<(u64, u64)>,
    /// Records the writer pushed per segment, in append order.
    pub pushed_records: BTreeMap<u64, Vec<(u64, u64)>>,
    /// Records the run attempted to append per segment, in append order. Superset
    /// of `pushed_records`: recovery may legitimately observe a record whose
    /// append failed (its bytes reached the page cache before the sync failed).
    pub attempted_records: BTreeMap<u64, Vec<(u64, u64)>>,
    /// Records recovered per segment by `repair_open_segments`.
    pub recovered_records: BTreeMap<u64, Vec<(u64, u64)>>,
    pub steps: Vec<String>,
}

/// Writer/uploader tuning resolved from a [`Plan`].
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub writer: WriterConfig,
    pub uploader: UploaderConfig,
}

/// Object keys a session uploads under. Seeded scenarios use the defaults; a
/// real-S3 chaos run picks a unique per-run prefix so a shared bucket keeps no
/// state between runs.
#[derive(Debug, Clone)]
pub struct StoreKeys {
    pub s3_prefix: String,
    pub manifest_key: String,
}

impl Default for StoreKeys {
    fn default() -> Self {
        Self {
            s3_prefix: bkxpro_uploader::DEFAULT_SEGMENT_PREFIX.to_string(),
            manifest_key: MANIFEST_KEY.to_string(),
        }
    }
}

impl Session {
    /// A session on the deterministic, fault-injecting S3 stand-in: the shape every
    /// seeded scenario uses.
    pub fn new(plan: &Plan) -> Result<Self, HarnessError> {
        let mock = Arc::new(MockS3::with_config(plan.mock.clone()));
        let mut session = Self::with_store(
            plan,
            Arc::clone(&mock) as Arc<dyn BlobStore>,
            StoreKeys::default(),
        )?;
        session.mock = Some(mock);
        Ok(session)
    }

    /// A session whose blob store is supplied by the caller: a real S3-compatible
    /// endpoint for chaos runs. Store-key-independent checks (`check_invariants`,
    /// read-back, remote verification) work unchanged; mock-only controls
    /// (crash points, fences, outage windows) do not exist on this path.
    pub fn with_store(
        plan: &Plan,
        store: Arc<dyn BlobStore>,
        keys: StoreKeys,
    ) -> Result<Self, HarnessError> {
        let root = TempDir::new()?;
        let data_dir = root.path().join("segments");
        std::fs::create_dir_all(&data_dir)?;
        let index_dir = root.path().join("index");

        let trace = Arc::new(Trace::new());
        let sim = SharedSimIo::with_mirror(Arc::clone(&trace), &data_dir);
        let traced = Arc::new(TracingBlob::new(
            store,
            Arc::clone(&trace),
            keys.manifest_key.clone(),
        ));
        let index = Arc::new(Index::open(index_config(&index_dir))?);
        let worker = {
            let io = sim.clone();
            SegmentIoWorker::spawn(move || Ok(io))?
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let writer = WriterConfig {
            target_data_bytes: plan.writer.target_data_bytes,
            max_entries: plan.writer.max_entries,
            group_commit_bytes: plan.writer.group_commit_bytes,
            max_inflight_bytes: plan.writer.max_inflight_bytes,
            flush_interval: Duration::from_millis(1),
            seal_interval: Duration::from_secs(3600),
            prezero_batch_blocks: plan.writer.prezero_batch_blocks,
        };
        let uploader = UploaderConfig {
            data_dir: data_dir.clone(),
            s3_prefix: keys.s3_prefix,
            manifest_key: keys.manifest_key,
            part_size: plan.uploader.part_size,
            write_concurrency: 2,
            read_chunk_bytes: plan.uploader.read_chunk_bytes,
            max_inflight_bytes: plan.uploader.max_inflight_bytes,
            stream_channel_capacity: plan.uploader.stream_channel_capacity,
            max_retries: plan.uploader.max_retries,
            retry_base_backoff: Duration::from_millis(plan.uploader.retry_base_ms),
            retry_max_backoff: Duration::from_millis(plan.uploader.retry_max_ms),
            retry_timeout: Duration::from_millis(plan.uploader.retry_timeout_ms),
            manifest_max_cas_retries: 4,
            poll_interval: Duration::from_millis(2),
            s3_digest_cache_entries: 32,
        };

        Ok(Self {
            root,
            data_dir,
            index_dir,
            trace,
            sim,
            mock: None,
            traced,
            index: Some(index),
            worker,
            uploader: None,
            runtime,
            uploader_thread: None,
            config: SessionConfig { writer, uploader },
            acked: Vec::new(),
            payloads: BTreeMap::new(),
            sealed_epochs: BTreeMap::new(),
            durable_records: BTreeSet::new(),
            pushed_records: BTreeMap::new(),
            attempted_records: BTreeMap::new(),
            recovered_records: BTreeMap::new(),
            steps: Vec::new(),
        })
    }

    /// The mock store; panics when this session runs on a real endpoint. Scenarios
    /// that arm mock-only chaos points are only ever driven over [`Session::new`].
    pub fn mock(&self) -> &Arc<MockS3> {
        self.mock.as_ref().expect(
            "this scenario requires the fault-injecting S3 stand-in; the session runs on a real store",
        )
    }

    pub fn mock_opt(&self) -> Option<&Arc<MockS3>> {
        self.mock.as_ref()
    }

    pub fn index(&self) -> &Arc<Index> {
        self.index.as_ref().expect("index is open")
    }

    pub fn root(&self) -> &Path {
        self.root.path()
    }

    pub fn index_config(&self) -> IndexConfig {
        index_config(&self.index_dir)
    }

    pub fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.data_dir.join(format!("{segment_id}.seg"))
    }

    pub fn step(&mut self, name: impl Into<String>) {
        let name = name.into();
        self.trace.record(TraceEvent::Checkpoint {
            name: name.clone(),
        });
        self.steps.push(name);
    }

    pub fn io_counters(&self) -> SimulatedCounters {
        self.sim.counters()
    }

    /// Queue one device fault to be consumed by the next operation of its class.
    pub fn inject_fault(&self, fault: DiskFault) {
        self.sim.inject_fault(to_simulated_fault(fault));
    }

    pub fn clear_faults(&self) {
        self.sim.clear_faults();
    }

    /// Persistent read-only remount (writes, truncates and unlinks fail).
    pub fn set_read_only(&self, read_only: bool) {
        self.sim.set_read_only(read_only);
    }

    /// Persistent ENOSPC (writes fail until cleared).
    pub fn set_enospc(&self, enospc: bool) {
        self.sim.set_enospc(enospc);
    }

    pub fn uploader_stats(&self) -> Option<UploaderStats> {
        self.uploader.as_ref().map(|uploader| uploader.stats())
    }

    pub fn uploader_error(&self) -> Option<String> {
        self.uploader
            .as_ref()
            .and_then(|uploader| uploader.poison_reason())
    }

    pub fn uploader_poisoned(&self) -> bool {
        self.uploader
            .as_ref()
            .is_some_and(|uploader| uploader.is_poisoned())
    }

    /// Surface the typed uploader error into the trace. `process_pending`/`recover`
    /// only report it as an error value, but the evidence belongs in the single
    /// event log the invariant checks read.
    pub fn note_upload_error(&self, error: &HarnessError) {
        if let HarnessError::Uploader(UploaderError::Fenced {
            segment_id,
            manifest_epoch,
            local_epoch,
        }) = error
        {
            self.trace.record(TraceEvent::Fenced {
                segment_id: *segment_id,
                manifest_epoch: *manifest_epoch,
                local_epoch: *local_epoch,
            });
        }
    }

    // ===== payload generation =====

    /// Deterministic payload of one entry: a pure function of `(seed, ledger, entry)`.
    pub fn entry_payload(seed: u64, ledger: u64, entry: u64, len: usize) -> Bytes {
        let mut rng = DetRng::new(seed).derive_from(&[
            b"payload",
            &ledger.to_le_bytes(),
            &entry.to_le_bytes(),
            &(len as u64).to_le_bytes(),
        ]);
        Bytes::from(rng.bytes(len))
    }

    /// Register the expected payload for an entry (so read-back can verify it).
    pub fn remember_payload(&mut self, ledger: u64, entry: u64, payload: &Bytes) {
        self.payloads.insert((ledger, entry), payload.to_vec());
    }

    // ===== writer phases =====

    /// Write one segment: acquire, zero, append, optionally seal.
    ///
    /// Faults in `spec.arms` are armed right before the entry at their
    /// `at_append` index is appended. When `stop_after` is reached the segment is
    /// left open (the caller decides whether to simulate a power cut).
    pub fn write_segment(&mut self, spec: &WriteSpec) -> Result<WriteOutcome, HarnessError> {
        let capacity = self
            .config
            .writer
            .buffer_capacity()
            .map_err(|error| HarnessError::message("writer config", error))?;
        let mut buffer = AlignedBuffer::new_zeroed(capacity)?;
        let path = self.segment_path(spec.segment_id);
        let header = SegmentHeader {
            segment_id: spec.segment_id,
            writer_epoch: spec.epoch,
            target_size: (HEADER_SIZE + self.config.writer.target_data_bytes) as u64,
            created_at_ns: 0,
        };
        self.sim.set_current_epoch(spec.epoch);
        let mut writer = SegmentWriter::create(
            self.sim.clone(),
            &path,
            &self.data_dir,
            header,
            self.config.writer.clone(),
            &mut buffer,
        )
        .map_err(|error| HarnessError::Writer(error.to_string()))?;
        self.put_segment_meta(
            spec.segment_id,
            SegmentMeta {
                state: SegmentState::Open,
                s3_key: None,
                min_ledger: 0,
                max_ledger: 0,
                byte_size: 0,
                writer_epoch: spec.epoch,
            },
        )?;

        let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer)?;
        let mut pending: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
        let mut outcome = WriteOutcome {
            segment_id: spec.segment_id,
            epoch: spec.epoch,
            end: WriteEnd::Open,
            acked: Vec::new(),
            records: Vec::new(),
            error: None,
        };
        let mut arm_cursor = 0usize;
        let mut stopped = false;
        let mut attempted: Vec<(u64, u64)> = Vec::new();

        for (index, entry) in spec.entries.iter().enumerate() {
            while arm_cursor < spec.arms.len() && spec.arms[arm_cursor].at_append <= index {
                self.sim
                    .inject_fault(to_simulated_fault(spec.arms[arm_cursor].fault));
                arm_cursor += 1;
            }
            attempted.push((entry.ledger, entry.entry));
            match writer.append(
                &mut encoder,
                entry.ledger,
                entry.entry,
                entry.flags,
                &entry.payload,
            ) {
                Ok(result) => {
                    pending.insert(result.sequence, (entry.ledger, entry.entry));
                    outcome.records.push((entry.ledger, entry.entry));
                    self.note_acks(
                        spec.segment_id,
                        result.acknowledged,
                        &mut pending,
                        &mut outcome,
                    );
                }
                Err(error) => {
                    outcome.error = Some(error.to_string());
                    outcome.end = WriteEnd::Failed;
                    break;
                }
            }
            if spec.stop_after > 0 && index + 1 == spec.stop_after {
                stopped = true;
                break;
            }
        }

        if outcome.error.is_none() && !stopped && spec.seal {
            match writer.seal(encoder) {
                Ok(sealed) => {
                    outcome.end = WriteEnd::Sealed {
                        file_len: sealed.file_len,
                    };
                    self.note_acks(
                        spec.segment_id,
                        sealed.acknowledged,
                        &mut pending,
                        &mut outcome,
                    );
                    // Sealing durably committed every record; nothing may be
                    // acknowledged that the seal did not cover.
                    self.sealed_epochs.insert(spec.segment_id, spec.epoch);
                    self.put_segment_meta(
                        spec.segment_id,
                        sealed_meta(spec.epoch, sealed.file_len, &outcome.records),
                    )?;
                    self.trace.record(TraceEvent::SegmentSealed {
                        segment_id: spec.segment_id,
                        epoch: spec.epoch,
                        file_len: sealed.file_len as u64,
                    });
                    for record in &outcome.records {
                        self.durable_records.insert(*record);
                    }
                }
                Err(error) => {
                    outcome.error = Some(error.to_string());
                    outcome.end = WriteEnd::Failed;
                }
            }
        } else if stopped {
            outcome.end = WriteEnd::Stopped;
        }

        self.pushed_records
            .insert(spec.segment_id, outcome.records.clone());
        self.attempted_records
            .insert(spec.segment_id, attempted);
        for ack in &outcome.acked {
            self.acked.push(*ack);
        }
        Ok(outcome)
    }

    fn note_acks(
        &self,
        segment_id: u64,
        acknowledged: Vec<u64>,
        pending: &mut BTreeMap<u64, (u64, u64)>,
        outcome: &mut WriteOutcome,
    ) {
        for sequence in acknowledged {
            let (ledger, entry) = pending
                .remove(&sequence)
                .expect("writer acknowledged a sequence it never accepted");
            outcome.acked.push(Acked {
                ledger,
                entry,
                segment_id,
                sequence,
            });
            self.trace.record(TraceEvent::AppendAcked {
                segment_id,
                ledger,
                entry,
                sequence,
            });
        }
    }

    /// Simulate a power cut: unsynced data and unsynced directory entries vanish.
    pub fn crash_device(&mut self) -> Result<(), HarnessError> {
        self.trace.record(TraceEvent::Crash {
            point: "power-cut".to_string(),
        });
        self.sim.crash()?;
        self.sim.sync_mirror();
        Ok(())
    }

    /// Simulate process death: drop every in-memory handle, wipe the derived
    /// index, rebuild it from the segment files, and repair any segment that was
    /// open when the process died.
    pub fn restart(&mut self) -> Result<RestartReport, HarnessError> {
        self.stop_background_uploader();
        self.uploader = None;
        if let Some(mock) = &self.mock {
            // A fresh process: the dead-process flag and the kill schedule of
            // the previous life must not carry over.
            mock.clear_crash();
        }
        let index = self.index.take().expect("index is open");
        debug_assert_eq!(
            Arc::strong_count(&index),
            1,
            "every index handle must be dropped before a restart"
        );
        drop(index);
        std::fs::remove_dir_all(&self.index_dir)?;

        // A segment created but never brought to a valid first commit (all-zero
        // preallocation) cannot be recovered by the index rebuild: it never
        // acknowledged a record, so it must be reconciled before the scan.
        self.remove_zero_preallocations()?;

        let mut index = Index::open(self.index_config())?;
        let mut io = self.sim.clone();
        let rebuild = index.rebuild_from_segments(&mut io, &self.data_dir, &self.config.writer)?;
        self.index = Some(Arc::new(index));
        self.trace.record(TraceEvent::IndexRebuilt {
            segments_processed: rebuild.segments_processed,
            entries_indexed: rebuild.entries_indexed,
        });

        let repairs = self.repair_open_segments()?;
        Ok(RestartReport {
            segments_rebuilt: rebuild.segments_processed,
            segments_failed: rebuild.segments_failed,
            entries_indexed: rebuild.entries_indexed,
            repairs,
        })
    }

    /// Unlink preallocated segment shells that never received a durable header
    /// block (a crash or a failed create before the first commit). The index
    /// rebuild refuses headerless open segments, and no record can have been
    /// acknowledged through one, so they are removed instead of "recovered".
    /// This mirrors the all-zero branch of [`Session::repair_open_segments`],
    /// which cannot run first because the rebuild would fail before it.
    fn remove_zero_preallocations(&self) -> Result<(), HarnessError> {
        let io = self.sim.clone();
        for path in self.sim.published_paths() {
            if path.extension().and_then(|extension| extension.to_str()) != Some("seg") {
                continue;
            }
            let Some(image) = self.sim.visible_bytes(&path) else {
                continue;
            };
            if image.is_empty()
                || image.len() % BLOCK_SIZE != 0
                || !image.iter().all(|byte| *byte == 0)
            {
                continue;
            }
            // A headerless all-zero shell: unlink it. No meta can exist for it
            // because the derived index was wiped just above.
            io.unlink_orphan(&path, &self.data_dir)?;
        }
        Ok(())
    }

    /// Re-seal every segment that has no valid trailer (an open segment left by a
    /// crash). Recovery straight from the stored prefix keeps every acknowledged
    /// record; a corrupt sealed segment is *not* repaired (it must fail-stop the
    /// uploader instead of silently losing bytes).
    pub fn repair_open_segments(&mut self) -> Result<Vec<RepairReport>, HarnessError> {
        let capacity = self
            .config
            .writer
            .buffer_capacity()
            .map_err(|error| HarnessError::message("writer config", error))?;
        let mut reports = Vec::new();
        let mut io = self.sim.clone();

        for path in self.sim.published_paths() {
            if path.extension().and_then(|extension| extension.to_str()) != Some("seg") {
                continue;
            }
            let Ok(segment_id) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_string)
                .unwrap_or_default()
                .parse::<u64>()
            else {
                continue;
            };
            let Some(image) = self.sim.visible_bytes(&path) else {
                continue;
            };
            if image.is_empty() || image.len() % BLOCK_SIZE != 0 {
                return Err(HarnessError::Message(format!(
                    "segment {} has a non-block-aligned length {}",
                    path.display(),
                    image.len()
                )));
            }
            let bytes = Bytes::from(image.clone());
            if SegmentView::open_sealed(bytes.clone()).is_ok() {
                continue;
            }
            if is_sealed_trailer(&image) {
                // A sealed segment that fails full validation is corruption, not
                // an unfinished write: recovery must not "repair" it by dropping
                // the tail. The uploader's stat will fail-stop on it.
                self.trace.record(TraceEvent::Checkpoint {
                    name: format!("corrupt-sealed:{}", trace_file_label(&path)),
                });
                continue;
            }

            let recovered = SegmentView::recover_open(bytes);
            let Some(view) = recovered.view else {
                if image.iter().all(|byte| *byte == 0) {
                    // Never wrote a durable header block: no record can have been
                    // acknowledged. Unlink the preallocated shell (and drop any
                    // meta) instead of leaking the file forever.
                    let mut batch = self.index().batch();
                    self.index().remove_segment(&mut batch, segment_id)?;
                    batch.commit().map_err(IndexError::from)?;
                    self.index().persist()?;
                    io.unlink_orphan(&path, &self.data_dir)?;
                    continue;
                }
                // Bytes but no valid header: a header that was corrupted after it
                // committed. Recovery must not discard it (acks may exist), and
                // must not "repair" it either; the uploader has to fail-stop.
                self.trace.record(TraceEvent::Checkpoint {
                    name: format!("corrupt-header:{}", trace_file_label(&path)),
                });
                continue;
            };
            let header = view.header();
            let mut encoder = SegmentEncoder::new(header);
            let mut records = Vec::new();
            for record in view.records() {
                let record = record?;
                let mut payload = Vec::new();
                record.copy_payload_to(&mut payload)?;
                encoder.push_record(
                    record.ledger,
                    record.entry,
                    record.flags,
                    &Bytes::from(payload),
                )?;
                records.push((record.ledger, record.entry));
            }
            let sealed_bytes = encoder.seal()?;
            if sealed_bytes.len() > capacity {
                return Err(HarnessError::Message(format!(
                    "repaired segment {segment_id} needs {} bytes but the writer capacity is {capacity}",
                    sealed_bytes.len()
                )));
            }

            let mut buffer = AlignedBuffer::new_zeroed(capacity)?;
            buffer[..sealed_bytes.len()].copy_from_slice(&sealed_bytes);
            self.sim.set_current_epoch(header.writer_epoch);
            let acquired = io.acquire_preallocated_segment(&path, capacity)?;
            let mut writes = Vec::new();
            for offset in (0..sealed_bytes.len()).step_by(BLOCK_SIZE) {
                writes.push(
                    WriteOp::new(offset as u64, &buffer[offset..offset + BLOCK_SIZE])
                        .map_err(|error| HarnessError::message("repair write op", error))?,
                );
            }
            io.commit_batch(&acquired.file, &writes)?;
            io.truncate(&acquired.file, sealed_bytes.len() as u64)?;
            io.sync_data(&acquired.file)?;

            self.sealed_epochs.insert(segment_id, header.writer_epoch);
            // Re-index the entry locations from the re-sealed image: the encoder may
            // pack records at different offsets than the recovered open segment did,
            // and a stale row would send readers to the wrong bytes.
            let sealed_view = SegmentView::open_sealed(Bytes::from(sealed_bytes.clone()))
                .map_err(|error| HarnessError::message("repaired segment validation", error))?;
            let mut entry_rows = Vec::new();
            for record in sealed_view.records() {
                let record = record?;
                entry_rows.push((
                    record.ledger,
                    record.entry,
                    EntryLocation {
                        segment_id: segment_id as u32,
                        offset: (record.offset + RECORD_HEADER_SIZE as u64) as u32,
                        len: record.len as u32,
                    },
                ));
            }
            let mut batch = self.index().batch();
            self.index().put_segment(
                &mut batch,
                segment_id,
                sealed_meta(header.writer_epoch, sealed_bytes.len(), &records),
            )?;
            for (ledger, entry, location) in entry_rows {
                self.index().put_entry(&mut batch, ledger, entry, location)?;
            }
            batch.commit().map_err(IndexError::from)?;
            self.index().persist()?;
            let unacked_recovered = records
                .iter()
                .filter(|record| !self.acked.iter().any(|ack| (ack.ledger, ack.entry) == **record))
                .copied()
                .collect();
            for record in &records {
                self.durable_records.insert(*record);
            }
            self.recovered_records.insert(segment_id, records.clone());
            self.trace.record(TraceEvent::OpenSegmentRecovered {
                segment_id,
                epoch: header.writer_epoch,
                bytes: image.len() as u64,
                records: records.len() as u64,
            });
            reports.push(RepairReport {
                segment_id,
                epoch: header.writer_epoch,
                records: records.len() as u64,
                bytes_before: image.len(),
                bytes_after: sealed_bytes.len(),
                unacked_recovered,
            });
        }
        Ok(reports)
    }

    // ===== uploader phases =====

    fn ensure_uploader(&mut self) -> Result<(), HarnessError> {
        if self.uploader.is_none() {
            let uploader = SegmentUploader::new(
                self.config.uploader.clone(),
                Arc::clone(self.index()),
                Arc::clone(&self.traced),
                Arc::clone(&self.worker),
            )?;
            self.uploader = Some(Arc::new(uploader));
        }
        Ok(())
    }

    /// One `process_pending` pass over the index.
    ///
    /// When the mock's kill point fires mid-pass, the pass fails with the
    /// per-segment error, whose detail names the simulated process death, so the
    /// first life never looks like a clean pass. The crash flag is re-checked
    /// afterwards so a death can never be mistaken for success either, exactly
    /// like the killed production process would show up.
    pub fn upload_pass(&mut self) -> Result<UploadReport, (UploadReport, HarnessError)> {
        self.ensure_uploader()
            .map_err(|error| (UploadReport::default(), error))?;
        let uploader = self.uploader.as_ref().expect("uploader just built");
        let report = self
            .runtime
            .block_on(uploader.process_pending())
            .map_err(|(report, error)| (report, error.into()))?;
        if let Some(reason) = self.mock_opt().and_then(|mock| mock.crash_reason()) {
            return Err((
                report,
                HarnessError::Message(format!("simulated process death: {reason}")),
            ));
        }
        Ok(report)
    }

    /// The uploader's recovery scan (repairs crash windows from a previous life).
    pub fn uploader_recover(&mut self) -> Result<RecoveryReport, HarnessError> {
        self.ensure_uploader()?;
        let uploader = self.uploader.as_ref().expect("uploader just built");
        let report = self.runtime.block_on(uploader.recover())?;
        Ok(report)
    }

    /// Drive upload passes until nothing is left or the pass budget is exhausted.
    /// Transient failures (chaos) do not end the loop; a poisoned uploader, a
    /// simulated process death, and the pass budget do.
    pub fn drive_uploads(&mut self, max_passes: u32) -> Result<UploadSummary, HarnessError> {
        let mut summary = UploadSummary::default();
        for _ in 0..max_passes {
            match self.upload_pass() {
                Ok(report) => {
                    summary.uploaded += report.uploaded;
                    summary.already_remote += report.already_remote;
                    summary.already_deleted += report.already_deleted;
                    summary.pending = report.pending;
                    summary.examined = report.examined;
                    summary.passes += 1;
                    summary.last_error = None;
                    if report.examined == 0 {
                        break;
                    }
                    if report.pending > 0 {
                        // Transient chaos (an outage window, a lost response)
                        // leaves segments pending; the settle loop retries until
                        // the budget runs out instead of giving up after one
                        // unlucky pass.
                        summary.last_error = Some(format!(
                            "{} segments still pending after a pass",
                            report.pending
                        ));
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                }
                Err((report, error)) => {
                    summary.uploaded += report.uploaded;
                    summary.already_remote += report.already_remote;
                    summary.already_deleted += report.already_deleted;
                    summary.pending = report.pending;
                    summary.examined = report.examined;
                    self.note_upload_error(&error);
                    let poisoned = self.uploader_poisoned();
                    let crashed = self
                        .mock_opt()
                        .is_some_and(|mock| mock.crash_reason().is_some());
                    summary.last_error = Some(error.to_string());
                    summary.passes += 1;
                    if poisoned || crashed {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        Ok(summary)
    }

    /// Run the real uploader loop on its own thread while the caller keeps
    /// writing, which is the concurrency shape production uses.
    pub fn start_background_uploader(&mut self) -> Result<(), HarnessError> {
        self.ensure_uploader()?;
        if self.uploader_thread.is_some() {
            return Ok(());
        }
        let uploader = Arc::clone(self.uploader.as_ref().expect("uploader just built"));
        let shutdown = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&shutdown);
        let handle = std::thread::Builder::new()
            .name("validator-uploader".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        eprintln!("validator uploader runtime failed to start: {error}");
                        return;
                    }
                };
                let _ = runtime.block_on(uploader.run(stop));
            })?;
        self.uploader_thread = Some((shutdown, handle));
        Ok(())
    }

    pub fn stop_background_uploader(&mut self) {
        if let Some((shutdown, handle)) = self.uploader_thread.take() {
            shutdown.store(true, Ordering::SeqCst);
            let _ = handle.join();
        }
    }

    // ===== read-back =====

    /// Verify every entry through the real fallback reader: local reads while the
    /// file is present, ranged S3 GETs once it has been deleted. A missing index
    /// row is a failure.
    pub fn read_back(&mut self, entries: &[(u64, u64)]) -> Result<ReadBackReport, HarnessError> {
        self.read_back_inner(entries, false)
    }

    /// Like [`Session::read_back`], but entries whose location is missing from the
    /// index are counted as `skipped` instead of failing. A full index wipe cannot
    /// reconstruct rows of segments that were already uploaded and deleted, so
    /// scenarios that restart in the middle of the lifecycle use this variant and
    /// cover the skipped entries with [`Session::verify_remote`].
    pub fn read_back_available(
        &mut self,
        entries: &[(u64, u64)],
    ) -> Result<ReadBackReport, HarnessError> {
        self.read_back_inner(entries, true)
    }

    fn read_back_inner(
        &mut self,
        entries: &[(u64, u64)],
        allow_missing: bool,
    ) -> Result<ReadBackReport, HarnessError> {
        let index = Arc::clone(self.index());
        let local = Arc::new(WorkerSegmentReader::new(
            Arc::clone(&self.worker),
            self.data_dir.clone(),
            Arc::clone(&index),
        ));
        let remote = Arc::new(S3SegmentReader::new(
            Arc::clone(&self.traced),
            Arc::clone(&index),
        ));
        let fallback = FallbackSegmentReader::new(
            Arc::clone(&local) as Arc<dyn bkxpro_reader::SegmentReader>,
            Arc::clone(&remote) as Arc<dyn bkxpro_reader::SegmentReader>,
        );

        let mut report = ReadBackReport::default();
        for (ledger, entry) in entries {
            let Some(expected) = self.payloads.get(&(*ledger, *entry)) else {
                report.failures.push(format!(
                    "no expected payload for ({ledger}, {entry}); the run never produced it"
                ));
                continue;
            };
            let Some(location) = index.get_entry(*ledger, *entry)? else {
                if allow_missing {
                    report.skipped += 1;
                } else {
                    report
                        .failures
                        .push(format!("({ledger}, {entry}) is not in the index"));
                }
                continue;
            };
            let segment_id = u64::from(location.segment_id);
            let source = match self.runtime.block_on(fallback.is_local(segment_id)) {
                Ok(true) => "local",
                Ok(false) => "remote",
                Err(error) => {
                    report
                        .failures
                        .push(format!("({ledger}, {entry}) fallback selection: {error}"));
                    continue;
                }
            };
            let read = if source == "local" {
                report.served_local += 1;
                self.runtime.block_on(local.read_segment_range(
                    segment_id,
                    location.offset as u64,
                    location.len as usize,
                ))
            } else {
                report.served_remote += 1;
                self.runtime.block_on(remote.read_segment_range(
                    segment_id,
                    location.offset as u64,
                    location.len as usize,
                ))
            };
            report.checked += 1;
            match read {
                Ok(bytes) => {
                    if bytes.as_ref() != expected.as_slice() {
                        report.failures.push(format!(
                            "({ledger}, {entry}) read back {} bytes that do not match the acknowledged payload",
                            bytes.len()
                        ));
                    }
                }
                Err(error) => report
                    .failures
                    .push(format!("({ledger}, {entry}) read failed: {error}")),
            }
            self.trace.record(TraceEvent::ReadBack {
                ledger: *ledger,
                entry: *entry,
                source,
                len: expected.len(),
            });
        }
        Ok(report)
    }

    /// Parse the current manifest object through the blob store. A missing
    /// manifest is an empty one: nothing has committed yet.
    pub fn manifest_records(&self) -> Result<Vec<ManifestRecord>, HarnessError> {
        let key = &self.config.uploader.manifest_key;
        match self.fetch_with_retry(key)? {
            Some(object) => Ok(crate::mock_s3::parse_manifest(&object.bytes)),
            None => Ok(Vec::new()),
        }
    }

    /// Fetch a whole object, retrying transient blob-store failures. Verification
    /// is not the product path: an injected or real transient failure must not
    /// hide a genuine violation (or pass) behind an error return. Retries are
    /// deterministic because every fault decision on the mock store is a pure
    /// function of (seed, key, op index).
    fn fetch_with_retry(
        &self,
        key: &str,
    ) -> Result<Option<bkxpro_uploader::BlobObject>, HarnessError> {
        const MAX_ATTEMPTS: u32 = 8;
        for attempt in 0..MAX_ATTEMPTS {
            let error = match self.runtime.block_on(self.traced.get(key)) {
                Ok(object) => return Ok(Some(object)),
                Err(BlobError::NotFound(_)) => return Ok(None),
                Err(error) => error,
            };
            if !matches!(error, BlobError::Transient(_)) || attempt + 1 == MAX_ATTEMPTS {
                return Err(HarnessError::message(
                    format!("fetching object {key}"),
                    error,
                ));
            }
            std::thread::sleep(Duration::from_millis(1 << attempt));
        }
        unreachable!("the loop returns on the final attempt")
    }

    /// Prove remote durability straight from the object store, without trusting the
    /// index: for every `(segment_id, epoch)` given, the manifest must carry a
    /// matching row, the stored object must match the row's key/length/checksum,
    /// parse as a sealed segment with that writer epoch, and contain the exact
    /// payload of every acknowledged entry in the segment.
    pub fn verify_remote(
        &self,
        segment_epochs: &[(u64, u64)],
    ) -> Result<RemoteVerifyReport, HarnessError> {
        let manifest = self.manifest_records()?;
        let mut report = RemoteVerifyReport::default();

        for (segment_id, epoch) in segment_epochs {
            let row = manifest
                .iter()
                .find(|record| record.segment_id == *segment_id && record.writer_epoch == *epoch);
            let Some(row) = row else {
                report.failures.push(format!(
                    "segment {segment_id} epoch {epoch}: no manifest row"
                ));
                continue;
            };
            let expected_key = self.config.uploader.segment_key(*segment_id, *epoch);
            if row.key != expected_key {
                report.failures.push(format!(
                    "segment {segment_id}: manifest row key {} does not match {expected_key}",
                    row.key
                ));
            }
            let bytes = match self.fetch_with_retry(&row.key)? {
                Some(object) => object.bytes,
                None => {
                    report.failures.push(format!(
                        "segment {segment_id}: manifest names {} but the object is absent",
                        row.key
                    ));
                    continue;
                }
            };
            if bytes.len() as u64 != row.bytes {
                report.failures.push(format!(
                    "segment {segment_id}: manifest says {} bytes, object has {}",
                    row.bytes,
                    bytes.len()
                ));
            }
            let crc = crc32c::crc32c(&bytes);
            if crc != row.crc32c {
                report.failures.push(format!(
                    "segment {segment_id}: manifest crc {:#010x}, object crc {crc:#010x}",
                    row.crc32c
                ));
            }
            let view = match SegmentView::open_sealed(bytes) {
                Ok(view) => view,
                Err(error) => {
                    report.failures.push(format!(
                        "segment {segment_id}: stored object is not a sealed segment: {error}"
                    ));
                    continue;
                }
            };
            if view.header().writer_epoch != *epoch {
                report.failures.push(format!(
                    "segment {segment_id}: stored object header epoch {} but the manifest row \
                     says {epoch}",
                    view.header().writer_epoch
                ));
            }
            report.segments_checked += 1;

            let acks: Vec<&Acked> = self
                .acked
                .iter()
                .filter(|ack| ack.segment_id == *segment_id)
                .collect();
            let mut found = BTreeSet::new();
            for record in view.records() {
                let record = record?;
                report.records_checked += 1;
                let key = (record.ledger, record.entry);
                if !acks
                    .iter()
                    .any(|ack| ack.ledger == record.ledger && ack.entry == record.entry)
                {
                    continue;
                }
                let mut payload = Vec::new();
                record.copy_payload_to(&mut payload)?;
                match self.payloads.get(&key) {
                    Some(expected) if expected.as_slice() == payload.as_slice() => {}
                    Some(_) => report.failures.push(format!(
                        "segment {segment_id}: entry ({}, {}) payload differs from what was \
                         acknowledged",
                        record.ledger, record.entry
                    )),
                    None => report.failures.push(format!(
                        "segment {segment_id}: entry ({}, {}) has no expected payload",
                        record.ledger, record.entry
                    )),
                }
                found.insert(key);
            }
            for ack in acks {
                if !found.contains(&(ack.ledger, ack.entry)) {
                    report.failures.push(format!(
                        "segment {segment_id}: acknowledged entry ({}, {}) is absent from the \
                         stored object",
                        ack.ledger, ack.entry
                    ));
                }
            }
        }
        Ok(report)
    }

    // ===== invariants =====

    /// Invariants that do not need the run to be over.
    ///
    /// * every acknowledged entry was observed durable (sealed or recovered);
    /// * a segment is unlinked only after a successful PUT and a manifest commit
    ///   of the same epoch;
    /// * a fenced epoch never committed and never deleted the local file;
    /// * per-segment recovered records are a prefix of what the writer attempted
    ///   (no holes, no invented entries; a record whose sync failed may legitimately
    ///   be present on disk even though it was never acknowledged).
    pub fn check_invariants(&self) -> Vec<String> {
        let mut violations = Vec::new();
        let events = self.trace.events();

        for ack in &self.acked {
            if !self.durable_records.contains(&(ack.ledger, ack.entry)) {
                violations.push(format!(
                    "acknowledged entry ({}, {}) in segment {} was never observed durable",
                    ack.ledger, ack.entry, ack.segment_id
                ));
            }
        }

        for (index, event) in events.iter().enumerate() {
            let TraceEvent::LocalUnlinked { segment_id, path } = event else {
                continue;
            };
            let epoch = self.sealed_epochs.get(segment_id).copied();
            let put_before = events[..index].iter().any(|prior| {
                matches!(prior, TraceEvent::PutDone { segment_id: id, .. } if id == segment_id)
            });
            let committed = events[..index]
                .iter()
                .filter_map(|prior| match prior {
                    TraceEvent::ManifestCommitted {
                        segment_id: id,
                        epoch,
                        ..
                    } if id == segment_id => Some(*epoch),
                    _ => None,
                })
                .max();
            let committed_ok = match (epoch, committed) {
                // Repaired segments have no seal event; any commit proves remote
                // durability for the bytes that were uploaded.
                (Some(epoch), Some(commit)) => commit == epoch,
                (_, Some(_)) => true,
                _ => false,
            };
            if !put_before || !committed_ok {
                violations.push(format!(
                    "segment {segment_id} at {path} was unlinked with PUT={put_before} and \
                     commit(epoch={epoch:?})={committed:?}"
                ));
            }
        }

        for event in &events {
            let TraceEvent::Fenced { segment_id, .. } = event else {
                continue;
            };
            let our_epoch = self.sealed_epochs.get(segment_id).copied();
            if let Some(epoch) = our_epoch {
                if events.iter().any(|other| {
                    matches!(
                        other,
                        TraceEvent::ManifestCommitted { segment_id: id, epoch: committed, .. }
                            if id == segment_id && *committed == epoch
                    )
                }) {
                    violations.push(format!(
                        "segment {segment_id} was fenced by a newer epoch but our epoch {epoch} still committed"
                    ));
                }
            }
            if events.iter().any(|other| {
                matches!(other, TraceEvent::LocalUnlinked { segment_id: id, .. } if id == segment_id)
            }) {
                violations.push(format!(
                    "segment {segment_id} was fenced by a newer epoch but was deleted locally"
                ));
            }
        }

        for (segment_id, recovered) in &self.recovered_records {
            if let Some(attempted) = self.attempted_records.get(segment_id) {
                if recovered.len() > attempted.len()
                    || recovered.as_slice() != &attempted[..recovered.len()]
                {
                    violations.push(format!(
                        "segment {segment_id}: recovered records {recovered:?} are not a prefix of \
                         the attempted records {attempted:?}"
                    ));
                }
            }
        }

        violations
    }

    // ===== internals =====

    fn put_segment_meta(&self, segment_id: u64, meta: SegmentMeta) -> Result<(), HarnessError> {
        let mut batch = self.index().batch();
        self.index().put_segment(&mut batch, segment_id, meta)?;
        batch.commit().map_err(IndexError::from)?;
        self.index().persist()?;
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.stop_background_uploader();
    }
}

/// One write phase.
#[derive(Debug, Clone)]
pub struct WriteSpec {
    pub segment_id: u64,
    pub epoch: u64,
    pub entries: Vec<AppendSpec>,
    pub arms: Vec<FaultArm>,
    /// Stop after this many entries (0 = never).
    pub stop_after: usize,
    /// Seal the segment once every entry is appended.
    pub seal: bool,
}

#[derive(Debug, Clone, Default)]
pub struct UploadSummary {
    pub passes: u32,
    pub examined: usize,
    pub uploaded: usize,
    pub already_remote: usize,
    pub already_deleted: usize,
    pub pending: usize,
    pub last_error: Option<String>,
}

impl UploadSummary {
    pub fn deleted(&self) -> usize {
        self.uploaded + self.already_remote + self.already_deleted
    }
}

/// Append-only decoration over any [`BlobStore`] that records the two facts the
/// delete-gating invariant needs: a PUT that completed, and a manifest commit
/// that landed (even when its response was lost, which the next manifest read
/// discovers).
pub struct TracingBlob {
    inner: Arc<dyn BlobStore>,
    trace: Arc<Trace>,
    /// Key of the manifest object; the real-S3 path prefixes it per run.
    manifest_key: String,
    manifest_seen: Mutex<BTreeSet<(u64, u64)>>,
}

impl TracingBlob {
    pub fn new(inner: Arc<dyn BlobStore>, trace: Arc<Trace>, manifest_key: String) -> Self {
        Self {
            inner,
            trace,
            manifest_key,
            manifest_seen: Mutex::new(BTreeSet::new()),
        }
    }

    pub fn inner(&self) -> &Arc<dyn BlobStore> {
        &self.inner
    }

    /// Record one `ManifestCommitted` per `(segment_id, epoch)` ever observed.
    fn observe_manifest(&self, raw: &[u8]) {
        for record in crate::mock_s3::parse_manifest(raw) {
            let key = (record.segment_id, record.writer_epoch);
            if self
                .manifest_seen
                .lock()
                .expect("manifest seen mutex poisoned")
                .insert(key)
            {
                self.trace.record(TraceEvent::ManifestCommitted {
                    segment_id: record.segment_id,
                    epoch: record.writer_epoch,
                    key: record.key,
                });
            }
        }
    }
}

#[async_trait::async_trait]
impl BlobStore for TracingBlob {
    async fn put_stream(
        &self,
        key: &str,
        part_size: usize,
        chunks: mpsc::Receiver<StreamItem>,
    ) -> Result<PutOutcome, BlobError> {
        let outcome = self.inner.put_stream(key, part_size, chunks).await?;
        if let Some(segment_id) = crate::mock_s3::segment_id_of_key(key) {
            self.trace.record(TraceEvent::PutDone {
                segment_id,
                key: key.to_string(),
                bytes: outcome.len,
            });
        }
        Ok(outcome)
    }

    async fn get(&self, key: &str) -> Result<BlobObject, BlobError> {
        let object = self.inner.get(key).await?;
        if key == self.manifest_key {
            self.observe_manifest(&object.bytes);
        }
        Ok(object)
    }

    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes, BlobError> {
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
        let outcome = self.inner.put_if_version(key, data.clone(), expected).await;
        if key == self.manifest_key && outcome.is_ok() {
            // The conditional write landed with exactly these bytes.
            self.observe_manifest(&data);
        }
        outcome
    }
}

fn sealed_meta(epoch: u64, file_len: usize, records: &[(u64, u64)]) -> SegmentMeta {
    let min_ledger = records.iter().map(|record| record.0).min().unwrap_or(0);
    let max_ledger = records.iter().map(|record| record.0).max().unwrap_or(0);
    SegmentMeta {
        state: SegmentState::Sealed,
        s3_key: None,
        min_ledger,
        max_ledger,
        byte_size: file_len as u64,
        writer_epoch: epoch,
    }
}

fn index_config(db_path: &Path) -> IndexConfig {
    IndexConfig {
        db_path: db_path.to_path_buf(),
        max_memory_bytes: 32 * 1024 * 1024,
        max_open_files: 64,
        journal_sync_interval_ms: 1000,
        enable_compaction: true,
        compaction_threads: 1,
    }
}

/// A sealed trailer signature in the final block; used to distinguish "open
/// segment left by a crash" (repairable) from "sealed segment with corruption"
/// (must fail-stop).
pub(crate) fn is_sealed_trailer(image: &[u8]) -> bool {
    image.len() >= TRAILER_SIZE
        && TrailerWire::parse(&image[image.len() - TRAILER_SIZE..]).is_ok()
}

pub fn to_simulated_fault(fault: DiskFault) -> SimulatedFault {
    match fault {
        DiskFault::FailWrite => SimulatedFault::FailNextWrite,
        DiskFault::Eagain => SimulatedFault::EagainNextWrite,
        DiskFault::Enospc => SimulatedFault::EnospcNextWrite,
        DiskFault::Torn {
            prefix_bytes,
            garbage_bytes,
        } => SimulatedFault::TornNextWrite {
            prefix_bytes,
            garbage_bytes,
        },
        DiskFault::Corrupt { byte_offset, xor } => SimulatedFault::CorruptNextWrite {
            byte_offset,
            xor,
        },
        DiskFault::FailSync => SimulatedFault::FailNextSync,
        DiskFault::SlowSync { micros } => SimulatedFault::SlowNextSync { micros },
        DiskFault::FailTruncate => SimulatedFault::FailNextTruncate,
        DiskFault::FailDirectorySync => SimulatedFault::FailNextDirectorySync,
        DiskFault::FailAcquire => SimulatedFault::FailNextAcquire,
        DiskFault::StaleRead => SimulatedFault::StaleNextRead,
    }
}
