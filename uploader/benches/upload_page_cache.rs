//! Upload I/O must not pollute the page cache (acceptance criterion).
//!
//! Sealed segments are read through the production io_uring adapter with `O_DIRECT` into
//! budgeted aligned chunks, so the cgroup's page-cache bytes (`memory.stat: file`)
//! should barely move while a large segment uploads. The same file is read once with
//! buffered `read(2)` as a control: that delta must be close to the segment size, which
//! proves the measurement can see page-cache growth at all.
//!
//! ```sh
//! cargo bench -p bkxpro-uploader --bench upload_page_cache
//! ```
//!
//! The bench needs a scratch directory on a filesystem that supports `O_DIRECT` (the
//! default is the workspace `target/bench-scratch`, which is not tmpfs) and a resolvable
//! cgroup with `memory.stat`. It prints a report and exits non-zero when a budget is
//! exceeded; when the host cannot provide the prerequisites it exits 0 with a skip note.
//! Filesystems that serve buffered reads from their own cache (ZFS: the ARC) never move
//! `memory.stat:file`, so the control read detects that and skips; run the check on the
//! XFS/ext4 staging volume the host preflight requires, or set
//! `BENCH_SKIP_CACHE_CONTROL=1` to exercise the upload path without the budget check.
//!
//! Knobs: `BKXPRO_BENCH_DIR` (scratch directory), `BENCH_SEGMENT_MIB` (segment size),
//! `BENCH_READ_CHUNK_MIB` (reader chunk, bounded by `RLIMIT_MEMLOCK`),
//! `BENCH_SKIP_CACHE_CONTROL=1` (run without the control phase).

use std::{
    fs::File,
    io::Read,
    os::unix::io::AsRawFd,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use bkxpro_codec::{SegmentEncoder, SegmentHeader};
use bkxpro_indexer::{Index, IndexConfig, SegmentMeta, SegmentState};
use bkxpro_io::cgroup::MemoryCgroup;
use bkxpro_io::{RingConfig, UringSegmentIo};
use bkxpro_uploader::{SegmentIoWorker, SegmentUploader, UploaderConfig};
use bytes::Bytes;
use object_store::memory::InMemory;

const MIB: u64 = 1024 * 1024;
const SEGMENT_ID: u64 = 1;
const LEDGER: u64 = 1;

/// Payload of one record. A handful of large records seals a large segment without
/// making record indexing part of the measurement.
const RECORD_PAYLOAD_BYTES: usize = 4 * MIB as usize;
/// Default segment size (16 x 4 MiB records plus the codec's header/tail blocks).
const DEFAULT_SEGMENT_MIB: u64 = 64;
/// Default reader chunk. Staged reads pin a scratch buffer of this size against
/// `RLIMIT_MEMLOCK`, so it stays small enough for the common 8 MiB soft limit.
const DEFAULT_READ_CHUNK_MIB: u64 = 1;
/// Page-cache growth allowed during the upload: 8 MiB, ~12.5% of a 64 MiB segment.
const UPLOAD_PAGE_CACHE_BUDGET_BYTES: u64 = 8 * MIB;
/// The buffered control read must move at least this much into the page cache. Half of a
/// 64 MiB segment leaves room for reclaim while still failing when the measurement is
/// broken.
const CONTROL_MIN_BYTES: u64 = 32 * MIB;
/// I/O size of the buffered control read.
const CONTROL_CHUNK_BYTES: usize = 4 * MIB as usize;
/// How long to wait for `posix_fadvise(DONTNEED)` to show up in `memory.stat`.
const DROP_TIMEOUT: Duration = Duration::from_secs(5);

fn main() {
    let Some(dir) = bench_dir() else { return };
    let Some(cgroup) = detect_cgroup() else {
        return;
    };
    // Every run gets its own directory: a killed run leaves the fjall index locked, and
    // a unique path keeps reruns deterministic (the S3 integration test does the same
    // with its object prefix).
    let run_dir = dir.join(format!(
        "run-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&run_dir).expect("create the run directory");
    let segment_mib = std::env::var("BENCH_SEGMENT_MIB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEGMENT_MIB)
        .max(8);
    let records = (segment_mib * MIB / RECORD_PAYLOAD_BYTES as u64).max(1);
    // The reader's staged-read scratch is pinned for the ring and charged against
    // RLIMIT_MEMLOCK, so keep the chunk well under the common 8 MiB soft limit and let
    // the operator raise it on a host with a larger budget.
    let read_chunk_mib = std::env::var("BENCH_READ_CHUNK_MIB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_READ_CHUNK_MIB)
        .max(1);

    println!("bkxpro uploader page-cache bench");
    println!("  run dir:   {}", run_dir.display());
    println!(
        "  cgroup:    {} (memory.max: {})",
        cgroup.directory().display(),
        limit_text(&cgroup)
    );
    let (memlock_soft, memlock_hard) = memlock_bytes();
    println!(
        "  reader:    {} chunks (memlock {} / {} KiB)",
        bytes(read_chunk_mib * MIB),
        memlock_soft / 1024,
        memlock_hard / 1024,
    );

    // The setup write leaves the segment in the page cache, so drop it first: both the
    // control read and the upload must start from a cache that does not already hold
    // the file. The cache counts as dropped once it is back within the upload budget of
    // the snapshot taken before the segment existed.
    let cold = MemStat::read(&cgroup).unwrap_or_default();
    let image = sealed_image(records);
    let image_len = image.len() as u64;
    let data_dir = run_dir.join("data");
    std::fs::create_dir_all(&data_dir).expect("create the bench data directory");
    let segment_path = data_dir.join(format!("{SEGMENT_ID}.seg"));
    std::fs::write(&segment_path, &image).expect("write the sealed segment");
    drop(image);
    drop_page_cache(&segment_path);
    let drop_target = cold.file_bytes + UPLOAD_PAGE_CACHE_BUDGET_BYTES;
    let after_write = wait_for_drop(&cgroup, drop_target);

    // Control phase: buffered reads must move the segment into `file`.
    let before_control = MemStat::read(&cgroup).unwrap_or(after_write);
    let control_read = buffered_read(&segment_path, image_len);
    let after_control = MemStat::read(&cgroup).unwrap_or(before_control);
    let control_growth = after_control
        .file_bytes
        .saturating_sub(before_control.file_bytes);

    // A control read that does not move the page cache means this host cannot observe
    // buffered reads through `memory.stat` (a filesystem serving them from its own cache,
    // for example); the upload check would then be vacuous, so stop before uploading
    // unless the operator explicitly asks for the upload phase anyway.
    let mut control_validated = true;
    if control_growth < CONTROL_MIN_BYTES {
        control_validated = false;
        println!(
            "  page cache, buffered control read: +{} (expected at least {})",
            bytes(control_growth),
            bytes(CONTROL_MIN_BYTES)
        );
        println!(
            "  page cache before the control read: +{} vs the pre-write baseline (a dropped \
             cache should be near 0)",
            bytes(before_control.file_bytes.saturating_sub(cold.file_bytes))
        );
        if std::env::var_os("BENCH_SKIP_CACHE_CONTROL").is_none() {
            println!(
                "skipping: buffered reads do not move memory.stat:file on this filesystem, so \
                 the upload measurement would be vacuous; run on XFS/ext4, or set \
                 BENCH_SKIP_CACHE_CONTROL=1 to exercise the upload path without the budget check"
            );
            let _ = std::fs::remove_dir_all(&run_dir);
            return;
        }
        println!(
            "note: BENCH_SKIP_CACHE_CONTROL is set; the upload runs, but its page-cache delta \
             is not validated on this filesystem"
        );
    }

    // Evict the control read again so the upload is measured from a cold cache.
    drop_page_cache(&segment_path);
    let before_upload = wait_for_drop(&cgroup, drop_target);
    if before_upload.file_bytes > cold.file_bytes + 16 * MIB {
        println!(
            "  note: page cache stayed at {} after fadvise(DONTNEED); the upload delta is \
             still measured against it",
            bytes(before_upload.file_bytes)
        );
    }

    // Upload phase: the production ring reads with O_DIRECT through the budgeted worker.
    if let Err(error) = UringSegmentIo::new(ring_config()) {
        println!("skipping: cannot create an io_uring instance on this host: {error}");
        return;
    }
    let worker = match SegmentIoWorker::spawn(|| UringSegmentIo::new(ring_config())) {
        Ok(worker) => worker,
        Err(error) => {
            println!("skipping: cannot spawn the segment I/O thread: {error}");
            return;
        }
    };
    let index = open_index(&run_dir.join("index"));
    stage_segment(&index, image_len);
    let config = UploaderConfig {
        data_dir: data_dir.clone(),
        part_size: 8 * MIB as usize,
        read_chunk_bytes: (read_chunk_mib * MIB) as usize,
        max_inflight_bytes: 32 * MIB as usize,
        ..Default::default()
    };
    let blob = Arc::new(config.blob(InMemory::new()));
    let uploader = Arc::new(
        SegmentUploader::new(
            config.clone(),
            Arc::clone(&index),
            Arc::clone(&blob),
            Arc::clone(&worker),
        )
        .expect("build the uploader"),
    );

    let sampler = PeakSampler::start(&cgroup);
    let progress_stop = Arc::new(AtomicBool::new(false));
    let progress_handle = std::env::var_os("BENCH_PROGRESS").map(|_| {
        let stop = Arc::clone(&progress_stop);
        let uploader = Arc::clone(&uploader);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(2));
                let stats = uploader.stats();
                println!(
                    "  progress: budget {} / {} (high water {}), put_attempts {}, retries {}, \
                     uploaded {}, manifest_commits {}",
                    bytes(uploader.budget().in_use() as u64),
                    bytes(uploader.budget().capacity() as u64),
                    bytes(uploader.budget().high_water() as u64),
                    stats.put_attempts,
                    stats.put_retries,
                    stats.bytes_uploaded,
                    stats.manifest_commits
                );
            }
        })
    });
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let report = match runtime.block_on(uploader.process_pending()) {
        Ok(report) => report,
        Err(error) => {
            // Keep the failure loud, but make the one environmental failure a bench
            // author hits immediately actionable.
            println!("FAIL: the upload pass failed: {error:?}");
            if format!("{error:?}").contains("os error 12") {
                println!(
                    "  hint: the reader pins a {} staged-read scratch against RLIMIT_MEMLOCK \
                     ({} KiB here); raise the limit or lower BENCH_READ_CHUNK_MIB",
                    bytes(read_chunk_mib * MIB),
                    memlock_bytes().0 / 1024
                );
            }
            println!("  run dir kept for inspection: {}", run_dir.display());
            std::process::exit(1);
        }
    };
    progress_stop.store(true, Ordering::Relaxed);
    if let Some(handle) = progress_handle {
        let _ = handle.join();
    }
    let peak_current = sampler.stop();
    let after_upload = MemStat::read(&cgroup).unwrap_or(before_upload);
    let upload_growth = after_upload
        .file_bytes
        .saturating_sub(before_upload.file_bytes);
    let stats = uploader.stats();
    let _ = runtime.block_on(worker.shutdown());
    drop(index);

    let mut failures = Vec::new();
    if control_read != image_len {
        failures.push(format!(
            "the buffered control read consumed {control_read} of {image_len} bytes"
        ));
    }
    if upload_growth > UPLOAD_PAGE_CACHE_BUDGET_BYTES {
        failures.push(format!(
            "the upload added {} of page cache, over the {} budget",
            bytes(upload_growth),
            bytes(UPLOAD_PAGE_CACHE_BUDGET_BYTES)
        ));
    }
    if report.uploaded != 1 || report.deleted() != 1 {
        failures.push(format!(
            "expected one upload and one deletion, got {report:?}"
        ));
    }
    if stats.bytes_uploaded != image_len {
        failures.push(format!(
            "the uploader reports {} uploaded bytes, expected {image_len}",
            stats.bytes_uploaded
        ));
    }
    if segment_path.exists() {
        failures.push("the local segment file still exists after the upload".to_string());
    }

    println!(
        "  segment:   {} in {records} records, uploaded as one object",
        bytes(image_len)
    );
    println!(
        "  page cache, buffered control read: +{}",
        bytes(control_growth)
    );
    println!(
        "  page cache, upload (O_DIRECT):     +{} (budget {})",
        bytes(upload_growth),
        bytes(UPLOAD_PAGE_CACHE_BUDGET_BYTES)
    );
    println!(
        "  reader budget high water:          {} / {} (read buffers leased by the reader)",
        bytes(uploader.budget().high_water() as u64),
        bytes(config.max_inflight_bytes as u64)
    );
    println!(
        "  peak memory.current growth:        {} (includes the in-memory store's own copy \
         of the object)",
        peak_growth(&before_upload, peak_current)
    );
    println!(
        "  memory.stat file: cold {}, after drop {}, after upload {}",
        bytes(cold.file_bytes),
        bytes(before_upload.file_bytes),
        bytes(after_upload.file_bytes)
    );

    if failures.is_empty() {
        // The run directory holds the segment image and the fjall index; drop it on
        // success and keep it on failure so a check can be investigated.
        if let Err(error) = std::fs::remove_dir_all(&run_dir) {
            println!("  note: could not remove {}: {error}", run_dir.display());
        }
        if control_validated {
            println!("page-cache budget check passed");
        } else {
            println!(
                "upload completed; the page-cache budget check was skipped because this \
                 filesystem gives no control signal"
            );
        }
    } else {
        for failure in &failures {
            println!("FAIL: {failure}");
        }
        println!("  run dir kept for inspection: {}", run_dir.display());
        std::process::exit(1);
    }
}

/// One `memory.stat` sample: page cache charged to the cgroup plus `memory.current`.
#[derive(Debug, Clone, Copy, Default)]
struct MemStat {
    file_bytes: u64,
    current_bytes: u64,
}

impl MemStat {
    fn read(cgroup: &MemoryCgroup) -> Option<Self> {
        let contents = std::fs::read_to_string(cgroup.directory().join("memory.stat")).ok()?;
        Some(Self {
            file_bytes: parse_file_bytes(&contents)?,
            current_bytes: cgroup.current_bytes().unwrap_or(0),
        })
    }
}

/// The `file` line of `memory.stat`: page cache + tmpfs + shared memory.
fn parse_file_bytes(contents: &str) -> Option<u64> {
    for line in contents.lines() {
        if let Some(value) = line.strip_prefix("file ") {
            return value.trim().parse::<u64>().ok();
        }
    }
    None
}

fn peak_growth(before: &MemStat, peak: u64) -> String {
    if before.current_bytes == 0 {
        return "unknown".to_string();
    }
    bytes(peak.saturating_sub(before.current_bytes))
}

/// Samples `memory.current` on a dedicated thread and reports the peak.
struct PeakSampler {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<u64>>,
}

impl PeakSampler {
    fn start(cgroup: &MemoryCgroup) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let cgroup = cgroup.clone();
        let handle = {
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut peak = 0u64;
                loop {
                    if let Ok(current) = cgroup.current_bytes() {
                        peak = peak.max(current);
                    }
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                peak
            })
        };
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .take()
            .expect("sampler thread")
            .join()
            .expect("sampler thread panicked")
    }
}

fn bench_dir() -> Option<PathBuf> {
    let dir = match std::env::var("BKXPRO_BENCH_DIR") {
        Ok(value) => PathBuf::from(value),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join("target/bench-scratch/uploader-page-cache"),
    };
    match std::fs::create_dir_all(&dir) {
        Ok(()) => Some(dir),
        Err(error) => {
            println!(
                "skipping: cannot create the bench directory {}: {error}",
                dir.display()
            );
            None
        }
    }
}

fn detect_cgroup() -> Option<MemoryCgroup> {
    let cgroup = match MemoryCgroup::detect() {
        Ok(cgroup) => cgroup,
        Err(error) => {
            println!("skipping: cannot resolve the process cgroup: {error}");
            return None;
        }
    };
    let stat = std::fs::read_to_string(cgroup.directory().join("memory.stat")).ok()?;
    match parse_file_bytes(&stat) {
        Some(_) => Some(cgroup),
        None => {
            println!("skipping: the cgroup has no readable memory.stat");
            None
        }
    }
}

fn ring_config() -> RingConfig {
    RingConfig {
        registered_file_slots: 64,
        ..RingConfig::default()
    }
}

fn sealed_image(records: u64) -> Bytes {
    let mut encoder = SegmentEncoder::new(SegmentHeader {
        segment_id: SEGMENT_ID,
        writer_epoch: 1,
        target_size: 2 * records * RECORD_PAYLOAD_BYTES as u64,
        created_at_ns: 1_700_000_000,
    });
    let payload = Bytes::from(vec![0x5Au8; RECORD_PAYLOAD_BYTES]);
    for entry in 1..=records {
        encoder.push_record(LEDGER, entry, 0, &payload).unwrap();
    }
    encoder.seal().unwrap()
}

fn open_index(dir: &Path) -> Arc<Index> {
    Arc::new(
        Index::open(IndexConfig {
            db_path: dir.to_path_buf(),
            max_memory_bytes: 64 * MIB as usize,
            ..Default::default()
        })
        .expect("open the bench index"),
    )
}

fn stage_segment(index: &Index, byte_size: u64) {
    let mut batch = index.batch();
    index
        .put_segment(
            &mut batch,
            SEGMENT_ID,
            SegmentMeta {
                state: SegmentState::Sealed,
                s3_key: None,
                min_ledger: LEDGER,
                max_ledger: LEDGER,
                byte_size,
                writer_epoch: 1,
            },
        )
        .unwrap();
    batch.commit().unwrap();
}

/// Read the file with buffered `read(2)`, discarding the bytes.
fn buffered_read(path: &Path, bytes: u64) -> u64 {
    let mut file = File::open(path).expect("open the segment for the control read");
    let mut buffer = vec![0u8; CONTROL_CHUNK_BYTES];
    let mut total = 0u64;
    while total < bytes {
        let want = CONTROL_CHUNK_BYTES.min((bytes - total) as usize);
        let got = file.read(&mut buffer[..want]).expect("buffered read");
        if got == 0 {
            break;
        }
        total += got as u64;
    }
    total
}

/// Drop this file's clean page cache.
fn drop_page_cache(path: &Path) {
    let Ok(file) = File::open(path) else { return };
    // SAFETY: `posix_fadvise` only inspects the descriptor and its integer arguments.
    let _ = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
}

/// Wait for the drop to be reflected in `memory.stat`, then return that sample.
fn wait_for_drop(cgroup: &MemoryCgroup, target: u64) -> MemStat {
    let deadline = Instant::now() + DROP_TIMEOUT;
    loop {
        let stat = MemStat::read(cgroup).unwrap_or_default();
        if stat.file_bytes <= target || Instant::now() >= deadline {
            return stat;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn limit_text(cgroup: &MemoryCgroup) -> String {
    match cgroup.effective_limit_bytes() {
        Ok(Some(limit)) => bytes(limit),
        Ok(None) => "unlimited".to_string(),
        Err(error) => format!("unknown ({error})"),
    }
}

/// Soft and hard `RLIMIT_MEMLOCK`, which bounds registered io_uring memory.
fn memlock_bytes() -> (u64, u64) {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid, initialized rlimit struct.
    unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) };
    (limit.rlim_cur, limit.rlim_max)
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
