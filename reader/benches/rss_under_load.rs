//! RSS-under-load benchmark suite for the RAN-131 reader cache.
//!
//! Every scenario samples the process cgroup's `memory.current` (RSS + page cache +
//! kernel memory) and `/proc/self/status` `VmRSS`, reports cache hit rates and per-tier
//! read latency percentiles, and fails when the peak exceeds the configured budget or
//! when `memory.events` shows OOM kills or allocation stalls at `memory.max`.
//!
//! Scenarios:
//! * `reads`  — cold pass (one read per 4 KiB block, served from the local segment and
//!   filling the cache) followed by a warm pass over every entry; reports hit rate and
//!   p50/p95/p99 per source tier.
//! * `writes` — sustained segment writes with preallocation, group commits, sealing,
//!   rotation, and index population.
//! * `mixed`  — sustained writes on a writer thread concurrent with cache reads.
//! * `outage` — a backlog of reads for segments that only exist in S3 while the ranged
//!   GET fallback is unavailable; the read semaphore bounds in-flight work.
//!
//! Run under a real memory cap so the budget checks are meaningful:
//!
//! ```text
//! systemd-run --user --scope -p MemoryMax=2G -q \
//!   cargo bench -p bkxpro-reader --bench rss_under_load
//! ```
//!
//! Select scenarios by passing their names: `... --bench rss_under_load -- reads outage`.
//!
//! Environment overrides: `BENCH_MEMORY_BUDGET_MIB` (default 2048),
//! `BENCH_ENTRIES` (20000), `BENCH_PAYLOAD_BYTES` (1024), `BENCH_DURATION_SECS` (5),
//! `BENCH_BACKLOG` (5000), `BENCH_RETRIES` (10), `BENCH_RETRY_DELAY_MS` (20),
//! `BENCH_MAX_CONCURRENT_READS` (128), `BENCH_FOYER_IO_ENGINE` (`psync` or `uring`),
//! `BENCH_MEMORY_TIER_MIB` (a quarter of the budget; set it below the working set to
//! exercise foyer's NVMe tier), `BENCH_BLOCK_SIZE_MIB` (16; foyer's device allocation
//! block, which also bounds how much data must accumulate before the NVMe tier is
//! written), `BENCH_ROOT` (workspace `target/bench-scratch`; must support O_DIRECT).

#[cfg(not(target_os = "linux"))]
compile_error!("rss_under_load benchmarks support Linux only");

use std::{
    fmt::Display,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use bkxpro_codec::{
    AlignedSegmentEncoder, SegmentHeader, SegmentView, BLOCK_DATA_SIZE, BLOCK_SIZE, HEADER_SIZE,
    RECORD_HEADER_SIZE,
};
use bkxpro_indexer::{EntryLocation, Index, IndexConfig, SegmentMeta, SegmentState};
use bkxpro_io::cgroup::{MemoryCgroup, MemoryEvents};
use bkxpro_io::{RingConfig, UringSegmentIo};
use bkxpro_reader::{
    IoEngineType, LocalSegmentReader, ReadCache, ReadCacheConfig, ReadCacheStats, ReadSource,
};
use bkxpro_segmenter::{AlignedBuffer, SegmentWriter, WriterConfig};
use bytes::Bytes;
use foyer_storage::UringIoEngineConfig;
use tempfile::TempDir;

const MIB: u64 = 1024 * 1024;
/// A 4 KiB block holds three 1 KiB-payload records, so entries stay inside one block
/// and an entry never interleaves with the next block's header.
const DEFAULT_PAYLOAD_BYTES: usize = 1024;

/// Entries appended between explicit group commits in the write load.
const FLUSH_EVERY_ENTRIES: u64 = 256;

/// Bytes a segment target must budget per entry: the record header, the packing waste
/// that ends each block when a record does not fit, plus the partially filled block a
/// group commit pads to a block boundary and seals off early.
fn bytes_per_entry(payload_bytes: usize) -> usize {
    let record = RECORD_HEADER_SIZE + payload_bytes;
    let per_block = (BLOCK_DATA_SIZE / record).max(1);
    BLOCK_SIZE.div_ceil(per_block) + BLOCK_SIZE / FLUSH_EVERY_ENTRIES as usize + 1
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Reads,
    Writes,
    Mixed,
    Outage,
}

impl Scenario {
    fn parse(text: &str) -> Option<Self> {
        match text {
            "reads" => Some(Self::Reads),
            "writes" => Some(Self::Writes),
            "mixed" => Some(Self::Mixed),
            "outage" => Some(Self::Outage),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Reads => "reads",
            Self::Writes => "writes",
            Self::Mixed => "mixed",
            Self::Outage => "outage",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FoyerEngine {
    Psync,
    Uring,
}

impl FoyerEngine {
    fn from_env() -> Self {
        match std::env::var("BENCH_FOYER_IO_ENGINE").as_deref() {
            Ok("uring") => Self::Uring,
            _ => Self::Psync,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Psync => "psync",
            Self::Uring => "uring (4 threads)",
        }
    }

    fn to_config(self) -> IoEngineType {
        match self {
            Self::Psync => IoEngineType::Psync,
            Self::Uring => IoEngineType::Uring(UringIoEngineConfig::new().with_threads(4)),
        }
    }
}

#[derive(Debug, Clone)]
struct Params {
    budget_bytes: u64,
    entries: usize,
    payload_bytes: usize,
    duration: Duration,
    backlog: usize,
    retries: u32,
    retry_delay: Duration,
    max_concurrent_reads: usize,
    io_engine: FoyerEngine,
    /// Overrides the memory tier size so a run can force NVMe-tier eviction.
    memory_tier_bytes: Option<u64>,
    /// foyer device allocation block size for the NVMe tier.
    block_size_bytes: u64,
    root: PathBuf,
}

impl Params {
    fn from_env() -> Self {
        let budget_bytes = env_u64("BENCH_MEMORY_BUDGET_MIB").unwrap_or(2048) * MIB;
        let entries = env_u64("BENCH_ENTRIES").unwrap_or(20_000).max(64) as usize;
        let payload_bytes =
            env_u64("BENCH_PAYLOAD_BYTES").unwrap_or(DEFAULT_PAYLOAD_BYTES as u64) as usize;
        let payload_bytes = payload_bytes.clamp(64, 3200);
        let duration = Duration::from_secs(env_u64("BENCH_DURATION_SECS").unwrap_or(5).max(1));
        let backlog = env_u64("BENCH_BACKLOG").unwrap_or(5_000).max(1) as usize;
        let retries = env_u64("BENCH_RETRIES").unwrap_or(10).max(1) as u32;
        let retry_delay = Duration::from_millis(env_u64("BENCH_RETRY_DELAY_MS").unwrap_or(20));
        let max_concurrent_reads =
            env_u64("BENCH_MAX_CONCURRENT_READS").unwrap_or(128).max(1) as usize;
        let io_engine = FoyerEngine::from_env();
        let memory_tier_bytes = env_u64("BENCH_MEMORY_TIER_MIB").map(|mib| mib * MIB);
        let block_size_bytes = env_u64("BENCH_BLOCK_SIZE_MIB").unwrap_or(16).max(1) * MIB;
        let root = match std::env::var("BENCH_ROOT") {
            Ok(value) => PathBuf::from(value),
            Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("workspace root")
                .join("target/bench-scratch"),
        };
        Self {
            budget_bytes,
            entries,
            payload_bytes,
            duration,
            backlog,
            retries,
            retry_delay,
            max_concurrent_reads,
            io_engine,
            memory_tier_bytes,
            block_size_bytes,
            root,
        }
    }

    /// Bytes one segment needs to hold `entries` entries with the configured payload,
    /// including header, footer, and block-alignment slack.
    fn segment_target_bytes(&self, entries: usize) -> usize {
        entries * bytes_per_entry(self.payload_bytes) + 8 * BLOCK_SIZE
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse().ok()
}

/// Soft/hard `RLIMIT_MEMLOCK`, in bytes. io_uring fixed buffers are pinned against this
/// limit on kernels without memcg buffer accounting.
fn memlock_limit_bytes() -> (u64, u64) {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid, initialized rlimit struct.
    unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) };
    (limit.rlim_cur, limit.rlim_max)
}

// ---------------------------------------------------------------------------
// Budget
// ---------------------------------------------------------------------------

/// Split the configured cap the same way the memory budget example does:
/// memory tier and NVMe-tier file each get a quarter, fjall gets a sixteenth, and the
/// remaining half stays headroom for page cache, pinning, and fragmentation.
#[derive(Debug, Clone, Copy)]
struct Budget {
    total_bytes: u64,
}

impl Budget {
    fn memory_tier_bytes(&self) -> usize {
        (self.total_bytes / 4) as usize
    }

    fn nvme_tier_bytes(&self) -> usize {
        (self.total_bytes / 4).max(64 * MIB) as usize
    }

    fn index_memory_bytes(&self) -> u64 {
        (self.total_bytes / 16).max(8 * MIB)
    }

    fn write_target_bytes(&self) -> usize {
        (self.total_bytes / 64).max(4 * MIB) as usize
    }

    fn check_line(&self) -> String {
        format!("{} MiB total", self.total_bytes / MIB)
    }
}

// ---------------------------------------------------------------------------
// Memory accounting
// ---------------------------------------------------------------------------

struct MemoryReport {
    peak_current_bytes: Option<u64>,
    peak_rss_bytes: Option<u64>,
    samples: u64,
    effective_limit_bytes: Option<u64>,
    events_before: Option<MemoryEvents>,
    events_after: Option<MemoryEvents>,
}

impl MemoryReport {
    fn events_delta(&self) -> Option<MemoryEvents> {
        let (before, after) = (self.events_before?, self.events_after?);
        Some(MemoryEvents {
            high: after.high.saturating_sub(before.high),
            max: after.max.saturating_sub(before.max),
            oom: after.oom.saturating_sub(before.oom),
            oom_kill: after.oom_kill.saturating_sub(before.oom_kill),
            oom_group_kill: after.oom_group_kill.saturating_sub(before.oom_group_kill),
        })
    }
}

/// Samples cgroup `memory.current` and process RSS on a dedicated thread until stopped.
struct MemorySampler {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<MemoryReport>>,
}

impl MemorySampler {
    fn start(cgroup: Option<MemoryCgroup>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let effective_limit_bytes = cgroup
            .as_ref()
            .and_then(|cgroup| cgroup.effective_limit_bytes().ok())
            .flatten();
        let events_before = cgroup.as_ref().and_then(|cgroup| cgroup.events().ok());
        let handle = {
            let stop = Arc::clone(&stop);
            let cgroup = cgroup.clone();
            thread::spawn(move || {
                let mut peak_current_bytes: Option<u64> = None;
                let mut peak_rss_bytes: Option<u64> = None;
                let mut samples = 0u64;
                loop {
                    let stopped = stop.load(Ordering::Relaxed);
                    if let Some(cgroup) = &cgroup {
                        if let Ok(current) = cgroup.current_bytes() {
                            peak_current_bytes =
                                Some(peak_current_bytes.map_or(current, |peak| peak.max(current)));
                        }
                    }
                    if let Some(rss) = read_vm_rss() {
                        peak_rss_bytes = Some(peak_rss_bytes.map_or(rss, |peak| peak.max(rss)));
                    }
                    samples += 1;
                    if stopped {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                MemoryReport {
                    peak_current_bytes,
                    peak_rss_bytes,
                    samples,
                    effective_limit_bytes,
                    events_before,
                    events_after: cgroup.and_then(|cgroup| cgroup.events().ok()),
                }
            })
        };
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn stop(mut self) -> MemoryReport {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .take()
            .expect("sampler thread")
            .join()
            .expect("sampler thread panicked")
    }
}

fn read_vm_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Latency collection and reporting
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Latency {
    samples: Vec<u64>,
}

impl Latency {
    fn record(&mut self, ns: u64) {
        self.samples.push(ns);
    }

    fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    fn percentile_ns(&self, percentile: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let rank = ((percentile / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        sorted[rank.min(sorted.len() - 1)]
    }

    fn line(&self) -> String {
        if self.samples.is_empty() {
            return "no samples".to_string();
        }
        let mean = self.samples.iter().sum::<u64>() / self.samples.len() as u64;
        format!(
            "n={} mean={} p50={} p95={} p99={} max={}",
            self.samples.len(),
            us(mean),
            us(self.percentile_ns(50.0)),
            us(self.percentile_ns(95.0)),
            us(self.percentile_ns(99.0)),
            us(self.samples.iter().copied().max().unwrap_or(0)),
        )
    }
}

fn us(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}

fn bytes(value: u64) -> String {
    if value >= MIB {
        format!("{:.1} MiB", value as f64 / MIB as f64)
    } else if value >= 1024 {
        format!("{:.1} KiB", value as f64 / 1024.0)
    } else {
        format!("{value} B")
    }
}

fn row(label: &str, value: impl Display) {
    println!("  {label:<44} {value}");
}

// ---------------------------------------------------------------------------
// Scenario bookkeeping
// ---------------------------------------------------------------------------

struct ScenarioReport {
    failures: Vec<String>,
    warnings: Vec<String>,
}

impl ScenarioReport {
    fn new(name: &'static str) -> Self {
        println!("\n== scenario {} ==", name);
        Self {
            failures: Vec::new(),
            warnings: Vec::new(),
        }
    }

    fn fail(&mut self, message: impl Into<String>) {
        self.failures.push(message.into());
    }

    /// Report the memory picture and enforce the budget.
    fn finish(mut self, memory: &MemoryReport, params: &Params) -> bool {
        if let Some(peak) = memory.peak_current_bytes {
            row("peak cgroup memory.current", bytes(peak));
        } else {
            row("peak cgroup memory.current", "unavailable");
        }
        if let Some(peak) = memory.peak_rss_bytes {
            row("peak RSS (VmRSS)", bytes(peak));
        }
        if let Some(delta) = memory.events_delta() {
            row(
                "memory.events delta high/max/oom/oom_kill",
                format!(
                    "{} / {} / {} / {}",
                    delta.high, delta.max, delta.oom, delta.oom_kill
                ),
            );
            if delta.max > 0 {
                self.fail("allocations stalled at memory.max (see memory.events max)");
            }
            if delta.oom_kill > 0 || delta.oom_group_kill > 0 {
                self.fail("the cgroup OOM-killed processes during the scenario");
            }
            if delta.high > 0 {
                self.warnings.push(format!(
                    "{} reclaim events under memory.high; raise the cap or shrink the tiers",
                    delta.high
                ));
            }
        } else {
            row("memory.events", "unavailable");
        }
        row("memory samples", memory.samples);

        // Without a finite `memory.max` every visible ancestor is unlimited and
        // `memory.current` covers unrelated processes in the same scope, so the cgroup
        // check is only enforceable under a real cap. Fall back to process RSS.
        match memory.effective_limit_bytes {
            Some(cgroup_limit) => {
                let limit = cgroup_limit.min(params.budget_bytes);
                let within = memory.peak_current_bytes.is_none_or(|peak| peak <= limit);
                row(
                    "budget check (cgroup)",
                    format!(
                        "{} (enforced {}, cgroup {}, budget {}, peak {})",
                        if within { "PASS" } else { "FAIL" },
                        bytes(limit),
                        bytes(cgroup_limit),
                        bytes(params.budget_bytes),
                        memory
                            .peak_current_bytes
                            .map_or("unavailable".to_string(), bytes),
                    ),
                );
                if !within {
                    self.fail(format!(
                        "peak cgroup memory.current exceeded the limit ({} > {})",
                        bytes(memory.peak_current_bytes.unwrap_or(0)),
                        bytes(limit),
                    ));
                }
            }
            None => {
                row(
                    "cgroup cap",
                    "unlimited at every visible ancestor; memory.current covers unrelated \
                     processes and is not enforced",
                );
                self.warnings.push(
                    "no finite memory.max: run under `systemd-run --user --scope -p \
                     MemoryMax=...` (or a container limit) to enforce the cgroup check"
                        .to_string(),
                );
                let within = memory
                    .peak_rss_bytes
                    .is_none_or(|peak| peak <= params.budget_bytes);
                row(
                    "budget check (rss fallback)",
                    format!(
                        "{} (budget {}, peak {})",
                        if within { "PASS" } else { "FAIL" },
                        bytes(params.budget_bytes),
                        memory
                            .peak_rss_bytes
                            .map_or("unavailable".to_string(), bytes),
                    ),
                );
                if !within {
                    self.fail(format!(
                        "peak RSS exceeded the budget ({} > {})",
                        bytes(memory.peak_rss_bytes.unwrap_or(0)),
                        bytes(params.budget_bytes),
                    ));
                }
            }
        }

        for warning in &self.warnings {
            println!("  warning: {warning}");
        }
        for failure in &self.failures {
            println!("  FAILURE: {failure}");
        }
        println!(
            "  result: {}",
            if self.failures.is_empty() {
                "PASS"
            } else {
                "FAIL"
            }
        );
        self.failures.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Segment fixture
// ---------------------------------------------------------------------------

fn ring() -> UringSegmentIo {
    UringSegmentIo::new(RingConfig {
        registered_file_slots: 64,
        ..RingConfig::default()
    })
    .expect("io_uring ring; this benchmark needs Linux >= 6.1 with io_uring enabled")
}

fn scratch_root(params: &Params) -> PathBuf {
    std::fs::create_dir_all(&params.root).expect("create benchmark scratch root");
    params.root.clone()
}

struct Fixture {
    temp: TempDir,
    data_dir: PathBuf,
}

impl Fixture {
    fn new(params: &Params) -> Self {
        let root = scratch_root(params);
        let temp = TempDir::new_in(&root).expect("create scenario scratch dir");
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        Self { temp, data_dir }
    }

    fn open_index(&self, budget: &Budget, name: &str) -> Index {
        Index::open(IndexConfig {
            db_path: self.temp.path().join(name),
            max_memory_bytes: budget.index_memory_bytes() as usize,
            ..Default::default()
        })
        .expect("open index")
    }

    fn cache_dir(&self) -> PathBuf {
        self.temp.path().join("cache")
    }
}

fn make_payload(ledger: u64, entry: u64, len: usize) -> Bytes {
    let prefix = format!("L{ledger:08}E{entry:08}");
    let mut data = vec![b'.'; len];
    let prefix = prefix.as_bytes();
    let take = prefix.len().min(len);
    data[..take].copy_from_slice(&prefix[..take]);
    Bytes::from(data)
}

fn writer_config(params: &Params, entries_per_segment: usize) -> WriterConfig {
    WriterConfig::default_for(
        params.segment_target_bytes(entries_per_segment),
        entries_per_segment + 8,
    )
}

struct WrittenSegment {
    file_bytes: u64,
    entries: u64,
}

/// Register the encoder buffer a write load reuses for every segment. Callers must
/// declare the buffer before the ring so it outlives ring teardown.
fn register_writer_buffer(io: &mut UringSegmentIo, buffer: &mut AlignedBuffer) {
    // SAFETY: the caller keeps `buffer` alive and at a fixed address for every writer
    // operation and until the ring is dropped.
    unsafe { io.register_buffer(0, buffer.as_mut_ptr(), buffer.len()) }.unwrap_or_else(|error| {
        panic!(
            "failed to register the {} encoder buffer with the io_uring ring: {error}\n\
             io_uring fixed buffers are pinned against RLIMIT_MEMLOCK; raise it \
             (`ulimit -l`, systemd `LimitMEMLOCK=`) or reduce BENCH_MEMORY_BUDGET_MIB",
            bytes(buffer.len() as u64),
        )
    });
}

/// Write, seal, and index one segment through a real io_uring ring. The ring is
/// consumed and returned because it must stay on its issuing thread.
// The helper mirrors one shard's write loop: ring, config, buffer, identity, and
// entry count are all part of its contract, and splitting them into a struct would only
// hide which values the caller must keep alive across the call.
#[allow(clippy::too_many_arguments)]
fn write_segment(
    io: UringSegmentIo,
    params: &Params,
    data_dir: &Path,
    index: &Index,
    config: &WriterConfig,
    buffer: &mut AlignedBuffer,
    segment_id: u64,
    ledger: u64,
    entries: usize,
) -> (UringSegmentIo, WrittenSegment) {
    let path = data_dir.join(format!("{segment_id}.seg"));
    let mut writer = SegmentWriter::create(
        io,
        &path,
        data_dir,
        SegmentHeader {
            segment_id,
            writer_epoch: 1,
            target_size: (HEADER_SIZE + config.target_data_bytes) as u64,
            created_at_ns: 1_700_000_000,
        },
        config.clone(),
        buffer,
    )
    .expect("create segment writer");

    let mut encoder = AlignedSegmentEncoder::new(writer.header(), buffer).expect("segment encoder");
    for entry in 1..=entries as u64 {
        let payload = make_payload(ledger, entry, params.payload_bytes);
        if let Err(error) = writer.append(&mut encoder, ledger, entry, 0, &payload) {
            panic!(
                "append {ledger}/{entry} of {entries} failed: {error:?} \
                 (target_data_bytes={}, max_entries={})",
                config.target_data_bytes, config.max_entries,
            );
        }
        if entry % FLUSH_EVERY_ENTRIES == 0 {
            writer.flush_group(&mut encoder).expect("flush group");
        }
    }
    let sealed = writer.seal(encoder).expect("seal segment");

    // Index the sealed segment exactly like recovery would: parse footer and records.
    let segment_bytes = std::fs::read(&sealed.path).expect("read sealed segment");
    let view = SegmentView::open_sealed(Bytes::from(segment_bytes)).expect("parse sealed segment");
    let mut batch = index.batch();
    let mut indexed = 0u64;
    for record in view.records() {
        let record = record.expect("parse record");
        index
            .put_entry(
                &mut batch,
                record.ledger,
                record.entry,
                EntryLocation {
                    segment_id: segment_id as u32,
                    // `EntryLocation.offset` points at the payload; the read path
                    // reconstructs the entry from the payload bytes.
                    offset: (record.offset + RECORD_HEADER_SIZE as u64) as u32,
                    len: record.len,
                },
            )
            .expect("index entry");
        indexed += 1;
    }
    index
        .put_segment(
            &mut batch,
            segment_id,
            SegmentMeta {
                state: SegmentState::Sealed,
                s3_key: None,
                min_ledger: ledger,
                max_ledger: ledger,
                byte_size: sealed.file_len as u64,
                writer_epoch: 1,
            },
        )
        .expect("index segment meta");
    batch.commit().expect("commit index batch");
    assert_eq!(indexed, entries as u64, "every entry must be indexed");

    let (io, _file, _path) = match writer.try_into_parts() {
        Ok(parts) => parts,
        Err(_) => panic!("writer poisoned after seal"),
    };
    (
        io,
        WrittenSegment {
            file_bytes: sealed.file_len as u64,
            entries: indexed,
        },
    )
}

/// Write, seal, and index one segment with a throwaway ring and encoder buffer, then
/// drop both. Dropping the ring here releases its fixed-buffer registration before the
/// cache builds its own ring, so peak `RLIMIT_MEMLOCK` usage stays at one encoder
/// buffer per phase.
fn prepopulate_segment(
    params: &Params,
    data_dir: &Path,
    index: &Index,
    config: &WriterConfig,
    segment_id: u64,
    entries: usize,
) -> WrittenSegment {
    let mut buffer = AlignedBuffer::new_zeroed(config.buffer_capacity().expect("capacity"))
        .expect("writer buffer");
    let mut io = ring();
    register_writer_buffer(&mut io, &mut buffer);
    let (_io, written) = write_segment(
        io,
        params,
        data_dir,
        index,
        config,
        &mut buffer,
        segment_id,
        segment_id,
        entries,
    );
    written
}

fn read_cache_config(
    params: &Params,
    budget: &Budget,
    fixture: &Fixture,
    data_dir: PathBuf,
) -> ReadCacheConfig {
    ReadCacheConfig {
        data_dir,
        cache_dir: fixture.cache_dir(),
        memory_tier_bytes: params
            .memory_tier_bytes
            .unwrap_or_else(|| budget.memory_tier_bytes() as u64)
            as usize,
        nvme_tier_bytes: budget.nvme_tier_bytes(),
        cache_shards: 1,
        max_concurrent_reads: params.max_concurrent_reads,
        block_size: params.block_size_bytes as usize,
        enable_compression: false,
        io_engine_type: params.io_engine.to_config(),
        read_buffer_blocks: 256,
        s3_read_timeout: Duration::from_secs(5),
    }
}

fn describe_cache_stats(stats: &ReadCacheStats) -> String {
    format!(
        "memory={} nvme={} segment={} s3={} misses={} fills={} evictions={}",
        stats.memory_hits,
        stats.nvme_hits,
        stats.segment_hits,
        stats.s3_reads,
        stats.misses,
        stats.cache_fills,
        stats.evictions,
    )
}

// ---------------------------------------------------------------------------
// Scenario: reads
// ---------------------------------------------------------------------------

fn scenario_reads(params: &Params, budget: &Budget, cgroup: &Option<MemoryCgroup>) -> bool {
    let mut report = ScenarioReport::new("reads");
    let fixture = Fixture::new(params);
    let index = Arc::new(fixture.open_index(budget, "index"));

    let config = writer_config(params, params.entries);
    let written = prepopulate_segment(
        params,
        &fixture.data_dir,
        &index,
        &config,
        1,
        params.entries,
    );
    let locations = index
        .scan_entries(1, 1, params.entries as u64 + 1)
        .expect("scan entries");
    assert_eq!(locations.len(), params.entries);

    row("entries", params.entries);
    row("payload bytes per entry", params.payload_bytes);
    row("segment bytes", bytes(written.file_bytes));
    row(
        "memory tier",
        bytes(
            params
                .memory_tier_bytes
                .unwrap_or_else(|| budget.memory_tier_bytes() as u64),
        ),
    );
    row("nvme tier", bytes(budget.nvme_tier_bytes() as u64));

    let sampler = MemorySampler::start(cgroup.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut local_latency = Latency::default();
    let mut memory_latency = Latency::default();
    let mut other_latency = Latency::default();
    let (cache_stats, tiered_hits) = runtime.block_on(async {
        let reader = Arc::new(
            LocalSegmentReader::new(
                || Ok(ring()),
                fixture.data_dir.clone(),
                Arc::clone(&index),
                writer_config(params, params.entries),
            )
            .expect("start local I/O reader"),
        );
        let cache_config = read_cache_config(params, budget, &fixture, fixture.data_dir.clone());
        let cache = ReadCache::new(cache_config, Arc::clone(&index), reader)
            .await
            .expect("build read cache");

        // Cold pass: read exactly one entry per 4 KiB block so every read misses the
        // cache and loads its block from the local segment.
        let mut seen_blocks = std::collections::HashSet::new();
        let mut cold_reads = 0u64;
        for (_, entry, location) in &locations {
            let block = location.offset / BLOCK_SIZE as u32;
            if !seen_blocks.insert(block) {
                continue;
            }
            let started = Instant::now();
            let result = cache.read_entry(1, *entry).await.expect("cold read");
            let elapsed = started.elapsed().as_nanos() as u64;
            assert_eq!(result.source, ReadSource::LocalSegment);
            assert_eq!(
                result.payload,
                make_payload(1, *entry, params.payload_bytes)
            );
            local_latency.record(elapsed);
            cold_reads += 1;
        }
        println!("  cold pass: {cold_reads} reads, one per block (all local segment)");

        // Warm pass: every entry; blocks were just filled, so this measures the memory
        // tier unless the working set exceeded it and foyer spilled to the NVMe tier.
        let mut tiered = [0u64; 4]; // memory, nvme, segment, s3
        for (_, entry, _) in &locations {
            let started = Instant::now();
            let result = cache.read_entry(1, *entry).await.expect("warm read");
            let elapsed = started.elapsed().as_nanos() as u64;
            assert_eq!(
                result.payload,
                make_payload(1, *entry, params.payload_bytes)
            );
            match result.source {
                ReadSource::MemoryCache => {
                    memory_latency.record(elapsed);
                    tiered[0] += 1;
                }
                ReadSource::NVMeCache => {
                    other_latency.record(elapsed);
                    tiered[1] += 1;
                }
                ReadSource::LocalSegment => {
                    local_latency.record(elapsed);
                    tiered[2] += 1;
                }
                ReadSource::S3 => {
                    other_latency.record(elapsed);
                    tiered[3] += 1;
                }
            }
        }
        let stats = cache.stats();
        cache.close().await.expect("close cache");
        (stats, tiered)
    });

    let warm_total: u64 = tiered_hits.iter().sum();
    row("warm pass reads", warm_total);
    row(
        "warm hit rate by tier",
        format!(
            "memory {:.1}% / nvme {:.1}% / segment {:.1}% / s3 {:.1}%",
            100.0 * tiered_hits[0] as f64 / warm_total as f64,
            100.0 * tiered_hits[1] as f64 / warm_total as f64,
            100.0 * tiered_hits[2] as f64 / warm_total as f64,
            100.0 * tiered_hits[3] as f64 / warm_total as f64,
        ),
    );
    if !local_latency.is_empty() {
        row("latency local segment", local_latency.line());
    }
    if !memory_latency.is_empty() {
        row("latency memory tier", memory_latency.line());
    }
    if !other_latency.is_empty() {
        row("latency nvme/s3 tier", other_latency.line());
    }
    row("cache stats", describe_cache_stats(&cache_stats));

    if tiered_hits[1] + tiered_hits[2] + tiered_hits[3] > 0 && memory_latency.is_empty() {
        report
            .warnings
            .push("no memory-tier hits: the working set exceeded the memory tier".to_string());
    }

    let memory = sampler.stop();
    let passed = report.finish(&memory, params);
    drop(fixture);
    passed
}

// ---------------------------------------------------------------------------
// Scenario: writes
// ---------------------------------------------------------------------------

/// Sustained writes with rotation. Returns entries and bytes written.
fn run_write_load(
    params: &Params,
    budget: &Budget,
    data_dir: &Path,
    index: &Index,
    deadline: Instant,
    stop: Option<&AtomicBool>,
    first_segment_id: u64,
) -> (u64, u64, u64) {
    let entries_per_segment =
        (budget.write_target_bytes() / bytes_per_entry(params.payload_bytes)).max(64);
    let config = writer_config(params, entries_per_segment);
    // Declared before the ring so it is the last thing dropped.
    let mut buffer = AlignedBuffer::new_zeroed(config.buffer_capacity().expect("capacity"))
        .expect("writer buffer");
    let mut io = ring();
    register_writer_buffer(&mut io, &mut buffer);
    let mut segment_id = first_segment_id;
    let mut total_entries = 0u64;
    let mut total_bytes = 0u64;
    let mut seals = 0u64;
    while Instant::now() < deadline && !stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
        let (returned_io, written) = write_segment(
            io,
            params,
            data_dir,
            index,
            &config,
            &mut buffer,
            segment_id,
            segment_id,
            entries_per_segment,
        );
        io = returned_io;
        segment_id += 1;
        total_entries += written.entries;
        total_bytes += written.file_bytes;
        seals += 1;
    }
    (total_entries, total_bytes, seals)
}

fn scenario_writes(params: &Params, budget: &Budget, cgroup: &Option<MemoryCgroup>) -> bool {
    let mut report = ScenarioReport::new("writes");
    let fixture = Fixture::new(params);
    let index = fixture.open_index(budget, "index");
    row(
        "target bytes per segment",
        bytes(budget.write_target_bytes() as u64),
    );
    row("duration", format!("{:?}", params.duration));

    let sampler = MemorySampler::start(cgroup.clone());
    let started = Instant::now();
    let deadline = started + params.duration;
    let (entries, written_bytes, seals) =
        run_write_load(params, budget, &fixture.data_dir, &index, deadline, None, 1);
    let elapsed = started.elapsed();

    row("sealed segments", seals);
    row("entries written", entries);
    row("bytes written", bytes(written_bytes));
    row(
        "throughput",
        format!(
            "{:.0} entries/s, {:.1} MiB/s",
            entries as f64 / elapsed.as_secs_f64(),
            written_bytes as f64 / elapsed.as_secs_f64() / MIB as f64
        ),
    );
    if seals == 0 {
        report.fail("no segment was sealed; the write load did not run");
    }

    let memory = sampler.stop();
    let passed = report.finish(&memory, params);
    drop(index);
    drop(fixture);
    passed
}

// ---------------------------------------------------------------------------
// Scenario: mixed
// ---------------------------------------------------------------------------

fn scenario_mixed(params: &Params, budget: &Budget, cgroup: &Option<MemoryCgroup>) -> bool {
    let mut report = ScenarioReport::new("mixed");
    let fixture = Fixture::new(params);
    let index = Arc::new(fixture.open_index(budget, "index"));

    // Pre-populate the read set.
    let config = writer_config(params, params.entries);
    let written = prepopulate_segment(
        params,
        &fixture.data_dir,
        &index,
        &config,
        1,
        params.entries,
    );
    let locations = index
        .scan_entries(1, 1, params.entries as u64 + 1)
        .expect("scan entries");
    row("read set entries", params.entries);
    row("read set segment bytes", bytes(written.file_bytes));
    row("duration", format!("{:?}", params.duration));

    let sampler = MemorySampler::start(cgroup.clone());
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let deadline = started + params.duration;

    // Writer thread: own ring, own segment rotation, shares the index and data dir.
    let writer_stop = Arc::clone(&stop);
    let writer_params = params.clone();
    let writer_budget = *budget;
    let writer_data_dir = fixture.data_dir.clone();
    let writer_index = Arc::clone(&index);
    let writer = thread::spawn(move || {
        let (entries, bytes_written, seals) = run_write_load(
            &writer_params,
            &writer_budget,
            &writer_data_dir,
            &writer_index,
            deadline,
            Some(&writer_stop),
            2,
        );
        (entries, bytes_written, seals)
    });

    // Reader: single-threaded runtime on this thread so the ring stays on its issuer.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut memory_latency = Latency::default();
    let mut segment_latency = Latency::default();
    let mut other_latency = Latency::default();
    let hits = runtime.block_on(async {
        let reader = Arc::new(
            LocalSegmentReader::new(
                || Ok(ring()),
                fixture.data_dir.clone(),
                Arc::clone(&index),
                writer_config(params, params.entries),
            )
            .expect("start local I/O reader"),
        );
        let cache_config = read_cache_config(params, budget, &fixture, fixture.data_dir.clone());
        let cache = ReadCache::new(cache_config, Arc::clone(&index), reader)
            .await
            .expect("build read cache");
        let mut reads = 0u64;
        let mut hits = [0u64; 2]; // memory, local segment
        let mut rng = XorShift::new(0x5eed);
        while Instant::now() < deadline {
            let (_, entry, _) = locations[rng.next_usize(locations.len())];
            let started = Instant::now();
            let result = cache.read_entry(1, entry).await.expect("mixed read");
            let elapsed = started.elapsed().as_nanos() as u64;
            reads += 1;
            match result.source {
                ReadSource::MemoryCache => {
                    memory_latency.record(elapsed);
                    hits[0] += 1;
                }
                ReadSource::LocalSegment => {
                    segment_latency.record(elapsed);
                    hits[1] += 1;
                }
                _ => other_latency.record(elapsed),
            }
        }
        let stats = cache.stats();
        cache.close().await.expect("close cache");
        (reads, hits, stats)
    });
    let (reads, hits, stats) = hits;
    let (writer_entries, writer_bytes, writer_seals) = writer.join().expect("writer thread");
    stop.store(true, Ordering::Relaxed);
    let elapsed = started.elapsed();

    row("reader reads", reads);
    row(
        "reader hit rate",
        format!(
            "memory {:.1}% / local segment {:.1}%",
            100.0 * hits[0] as f64 / reads.max(1) as f64,
            100.0 * hits[1] as f64 / reads.max(1) as f64,
        ),
    );
    if !memory_latency.is_empty() {
        row("reader latency memory tier", memory_latency.line());
    }
    if !segment_latency.is_empty() {
        row("reader latency local segment", segment_latency.line());
    }
    if !other_latency.is_empty() {
        row("reader latency nvme/s3 tier", other_latency.line());
    }
    row("cache stats", describe_cache_stats(&stats));
    row("writer sealed segments", writer_seals);
    row("writer entries", writer_entries);
    row("writer bytes", bytes(writer_bytes));
    row(
        "writer throughput",
        format!(
            "{:.0} entries/s, {:.1} MiB/s",
            writer_entries as f64 / elapsed.as_secs_f64(),
            writer_bytes as f64 / elapsed.as_secs_f64() / MIB as f64
        ),
    );
    if reads == 0 {
        report.fail("reader issued no reads");
    }
    if writer_seals == 0 {
        report.fail("writer sealed no segments");
    }

    let memory = sampler.stop();
    let passed = report.finish(&memory, params);
    drop(index);
    drop(fixture);
    passed
}

/// Small deterministic PRNG so mixed reads do not depend on external crates.
struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

// ---------------------------------------------------------------------------
// Scenario: S3 outage backlog
// ---------------------------------------------------------------------------

/// Segment reader for entries whose only copy is in S3: no local file exists, so the
/// authoritative state comes from the index and every read takes the S3 fallback.
struct S3OnlySegments {
    index: Arc<Index>,
}

#[async_trait::async_trait]
impl bkxpro_reader::SegmentReader for S3OnlySegments {
    async fn read_segment_range(
        &self,
        segment_id: u64,
        _offset: u64,
        _length: usize,
    ) -> Result<Bytes, bkxpro_reader::ReadCacheError> {
        Err(bkxpro_reader::ReadCacheError::SegmentIo(format!(
            "segment {segment_id} is not available locally"
        )))
    }

    async fn segment_exists(&self, segment_id: u64) -> Result<bool, bkxpro_reader::ReadCacheError> {
        Ok(self.index.get_segment(segment_id)?.is_some())
    }

    async fn get_segment_meta(
        &self,
        segment_id: u64,
    ) -> Result<Option<SegmentMeta>, bkxpro_reader::ReadCacheError> {
        Ok(self.index.get_segment(segment_id)?)
    }
}

fn scenario_outage(params: &Params, budget: &Budget, cgroup: &Option<MemoryCgroup>) -> bool {
    let mut report = ScenarioReport::new("outage");
    let fixture = Fixture::new(params);
    let index = Arc::new(fixture.open_index(budget, "index"));

    // One segment that has been uploaded and deleted locally: index metadata says
    // Deleted, and no file exists in the data directory.
    let segment_id = 1u64;
    let segment_meta = SegmentMeta {
        state: SegmentState::Deleted,
        s3_key: Some(format!("segments/{segment_id}.seg")),
        min_ledger: 1,
        max_ledger: 1,
        byte_size: 1,
        writer_epoch: 1,
    };
    {
        let mut batch = index.batch();
        index
            .put_segment(&mut batch, segment_id, segment_meta)
            .expect("index segment");
        for entry in 1..=params.entries as u64 {
            index
                .put_entry(
                    &mut batch,
                    1,
                    entry,
                    EntryLocation {
                        segment_id: segment_id as u32,
                        offset: 4096,
                        len: params.payload_bytes as u32,
                    },
                )
                .expect("index entry");
        }
        batch.commit().expect("commit");
    }

    row("backlog reads", params.backlog);
    row("retries per read", params.retries);
    row("retry delay", format!("{:?}", params.retry_delay));
    row("max concurrent reads", params.max_concurrent_reads);
    row(
        "s3 fallback",
        "ranged GET against a segment with no local copy; every read returns an S3 error",
    );

    let sampler = MemorySampler::start(cgroup.clone());
    let started = Instant::now();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let inflight = Arc::new(AtomicU64::new(0));
    let max_inflight = Arc::new(AtomicU64::new(0));
    let (attempts, errors, permits_peak) = runtime.block_on(async {
        let reader = Arc::new(S3OnlySegments {
            index: Arc::clone(&index),
        });
        let cache_config = read_cache_config(params, budget, &fixture, fixture.data_dir.clone());
        let cache = Arc::new(
            ReadCache::new(cache_config, Arc::clone(&index), reader)
                .await
                .expect("build read cache"),
        );
        let mut tasks = Vec::with_capacity(params.backlog);
        for task in 0..params.backlog {
            let cache = Arc::clone(&cache);
            let inflight = Arc::clone(&inflight);
            let max_inflight = Arc::clone(&max_inflight);
            let retry_delay = params.retry_delay;
            let retries = params.retries;
            let entry = (task as u64 % params.entries as u64) + 1;
            tasks.push(tokio::spawn(async move {
                let mut attempts = 0u64;
                let mut errors = 0u64;
                for _ in 0..retries {
                    let current = inflight.fetch_add(1, Ordering::Relaxed) + 1;
                    max_inflight.fetch_max(current, Ordering::Relaxed);
                    attempts += 1;
                    match cache.read_entry(1, entry).await {
                        Ok(_) => {}
                        Err(bkxpro_reader::ReadCacheError::S3Read(_)) => errors += 1,
                        Err(error) => panic!("unexpected read error: {error}"),
                    }
                    inflight.fetch_sub(1, Ordering::Relaxed);
                    // Emulate an S3 timeout before the next attempt.
                    tokio::time::sleep(retry_delay).await;
                }
                (attempts, errors)
            }));
        }
        let mut attempts = 0u64;
        let mut errors = 0u64;
        for task in tasks {
            let (task_attempts, task_errors) = task.await.expect("backlog task");
            attempts += task_attempts;
            errors += task_errors;
        }
        let stats = cache.stats();
        let permits_peak = stats.s3_reads;
        cache.close().await.expect("close cache");
        (attempts, errors, permits_peak)
    });
    let elapsed = started.elapsed();

    row("read attempts", attempts);
    row("s3 errors", errors);
    row("s3 reads counted by the cache", permits_peak);
    row(
        "backlog drain time",
        format!("{:.2}s", elapsed.as_secs_f64()),
    );
    // The counter tracks read calls that have entered `read_entry`, including calls
    // waiting for a read permit. It measures backlog pressure, not device concurrency:
    // this scenario's reader fails the ranged GET immediately, so reads return without
    // waiting on a real object store and the semaphore is only held for the lookup.
    row(
        "peak queued read calls",
        format!(
            "{} (read permits: {})",
            max_inflight.load(Ordering::Relaxed),
            params.max_concurrent_reads
        ),
    );
    if attempts != (params.backlog as u64) * (params.retries as u64) {
        report.fail("the backlog did not issue every configured attempt");
    }
    if errors == 0 {
        report.fail("no S3 errors were surfaced; the outage path was not exercised");
    }

    let memory = sampler.stop();
    let passed = report.finish(&memory, params);
    drop(index);
    drop(fixture);
    passed
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let params = Params::from_env();
    let budget = Budget {
        total_bytes: params.budget_bytes,
    };

    let requested: Vec<Scenario> = std::env::args()
        .skip(1)
        // `cargo bench` passes its own flags (for example `--bench`) to the target.
        .filter(|arg| !arg.starts_with('-'))
        .filter_map(|arg| match Scenario::parse(&arg) {
            Some(scenario) => Some(scenario),
            None => {
                eprintln!("ignoring unknown scenario `{arg}` (reads, writes, mixed, outage)");
                None
            }
        })
        .collect();
    let scenarios = if requested.is_empty() {
        vec![
            Scenario::Reads,
            Scenario::Writes,
            Scenario::Mixed,
            Scenario::Outage,
        ]
    } else {
        requested
    };

    let cgroup = match MemoryCgroup::detect() {
        Ok(cgroup) => {
            let limit = cgroup
                .effective_limit_bytes()
                .ok()
                .flatten()
                .map_or("unlimited".to_string(), bytes);
            println!(
                "cgroup memory: {} (limit {limit})",
                cgroup.directory().display(),
            );
            Some(cgroup)
        }
        Err(error) => {
            println!("cgroup memory accounting unavailable: {error}");
            None
        }
    };
    let (memlock_soft, memlock_hard) = memlock_limit_bytes();
    println!(
        "memlock: soft {} / hard {} (registered io_uring buffers are pinned against it)",
        bytes(memlock_soft),
        if memlock_hard == u64::MAX {
            "unlimited".to_string()
        } else {
            bytes(memlock_hard)
        },
    );
    println!(
        "bkxpro reader RSS-under-load benchmark (RAN-131)\n\
         budget: {} (foyer io engine: {})\n\
         params: entries={} payload={} B duration={:?} backlog={} root={}\n\
         caution: on tmpfs the segment bytes are charged to memory.current; prefer a \
         disk-backed BENCH_ROOT (default: workspace target/bench-scratch)",
        budget.check_line(),
        params.io_engine.label(),
        params.entries,
        params.payload_bytes,
        params.duration,
        params.backlog,
        params.root.display(),
    );

    let mut failed = Vec::new();
    for scenario in scenarios {
        let passed = match scenario {
            Scenario::Reads => scenario_reads(&params, &budget, &cgroup),
            Scenario::Writes => scenario_writes(&params, &budget, &cgroup),
            Scenario::Mixed => scenario_mixed(&params, &budget, &cgroup),
            Scenario::Outage => scenario_outage(&params, &budget, &cgroup),
        };
        if !passed {
            failed.push(scenario.name());
        }
    }

    println!();
    if failed.is_empty() {
        println!("all scenarios passed their memory budget checks");
    } else {
        println!("scenarios failed: {}", failed.join(", "));
        std::process::exit(1);
    }
}
