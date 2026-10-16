//! Real-process kill testing: a child writer is `SIGKILL`ed mid-append and the
//! parent proves every acknowledged entry is recoverable from the real files.
//!
//! The deterministic scenarios model crashes by construction (`Session::restart`
//! drops state, `crash_device` discards unsynced bytes). This module covers the
//! boundary the simulator cannot: an actual process killed by a signal while it
//! is inside `append`/`flush_group`/`seal`, writing through [`PlainFileIo`] to
//! real files. The child journals every acknowledgement to `acked.jsonl`
//! immediately after the writer returns it; the parent kills the child at a
//! seeded moment, then recovers every segment and checks:
//!
//! * every journaled acknowledgement is present in the recovered records,
//! * its payload equals the seeded payload recomputation,
//! * no `(ledger, entry)` is surfaced twice,
//! * no sealed segment fails validation and no nonzero header is unreadable.
//!
//! What `SIGKILL` cannot show is sync ordering: the page cache survives the
//! signal, so a child that acked before `fsync` would still be "recoverable".
//! That window is the simulator's job (power-cut scenarios) and the
//! `dm-log-writes` replay script's job (`validator/scripts/`).
//!
//! The child half is [`child_main`], reached through the `kill-soak-child`
//! subcommand of the `bkxpro-validator` binary; `tests/kill_soak.rs` locates the
//! binary via `CARGO_BIN_EXE_bkxpro-validator` and drives rounds through
//! [`run_kill_soak`].

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, Write},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use bkxpro_codec::{AlignedSegmentEncoder, SegmentView};
use bkxpro_segment_writer::{AlignedBuffer, SegmentWriter, WriterConfig, WriterError};
use serde::{Deserialize, Serialize};

use crate::{
    lifecycle::{is_sealed_trailer, Session},
    plain_io::PlainFileIo,
    rng::DetRng,
};

/// Journal file the child appends every acknowledgement to.
pub const JOURNAL_FILE: &str = "acked.jsonl";

/// Metadata the child drops next to the journal so a verifier can recover the
/// seed (and workload shape) without the parent process.
pub const CHILD_META_FILE: &str = "child-meta.json";

/// The child's workload identity, written at startup. `seed` is the only field
/// verification strictly needs, because payload lengths come from the records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildMeta {
    pub seed: u64,
    pub payload_bytes: usize,
    pub seal_at: usize,
    pub ledgers: u64,
}

/// Read the child metadata file, if the child got far enough to write one.
pub fn read_child_meta(dir: &Path) -> io::Result<Option<ChildMeta>> {
    let path = dir.join(CHILD_META_FILE);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| io::Error::other(format!("{}: {error}", path.display()))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// One child-side acknowledgement, written after the writer returned it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalEntry {
    pub segment_id: u64,
    pub ledger: u64,
    pub entry: u64,
    pub len: usize,
}

/// Everything the child process needs to write until it is killed.
#[derive(Debug, Clone)]
pub struct ChildConfig {
    /// Session directory; the segment data lives in `<dir>/segments`.
    pub dir: PathBuf,
    /// Payload derivation seed (the parent recomputes payloads with it).
    pub seed: u64,
    pub payload_bytes: usize,
    /// Seal and rotate after this many entries in one segment.
    pub seal_at: usize,
    pub ledgers: u64,
    /// Safety bound so a missed kill cannot write forever.
    pub max_entries: u64,
    /// Sleep between appends; stretches the window the parent can kill in.
    pub step_micros: u64,
}

impl ChildConfig {
    pub fn new(dir: impl Into<PathBuf>, seed: u64) -> Self {
        Self {
            dir: dir.into(),
            seed,
            payload_bytes: 1024,
            seal_at: 32,
            ledgers: 2,
            max_entries: 500_000,
            step_micros: 500,
        }
    }
}

fn writer_config() -> WriterConfig {
    // Deliberately small: the preallocation/zeroing pass must be a tiny slice of
    // each segment's lifetime, so the parent's kill lands inside the append loop
    // (where an open segment has to be recovered) rather than during `create`.
    WriterConfig {
        target_data_bytes: 64 * 1024,
        max_entries: 4096,
        group_commit_bytes: 4096,
        max_inflight_bytes: 16 * 1024 * 1024,
        flush_interval: Duration::from_millis(1),
        seal_interval: Duration::from_secs(3600),
        prezero_batch_blocks: 16,
    }
}

fn writer_err(context: &str, error: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("{context}: {error}"))
}

/// The child: append entries to rotating segments through [`PlainFileIo`],
/// journaling every acknowledgement, until the parent kills it.
pub fn child_main(config: &ChildConfig) -> io::Result<()> {
    let data_dir = config.dir.join("segments");
    fs::create_dir_all(&data_dir)?;
    let meta = ChildMeta {
        seed: config.seed,
        payload_bytes: config.payload_bytes,
        seal_at: config.seal_at,
        ledgers: config.ledgers,
    };
    fs::write(
        config.dir.join(CHILD_META_FILE),
        serde_json::to_vec(&meta).map_err(io::Error::other)?,
    )?;
    let journal_path = config.dir.join(JOURNAL_FILE);
    let mut journal = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)?;

    let writer_config = writer_config();
    let capacity = writer_config
        .buffer_capacity()
        .map_err(|error| writer_err("writer capacity", error))?;
    let mut buffer = AlignedBuffer::new_zeroed(capacity)?;

    let header = |segment_id: u64| bkxpro_codec::SegmentHeader {
        segment_id,
        writer_epoch: 1,
        target_size: (bkxpro_codec::HEADER_SIZE + writer_config.target_data_bytes) as u64,
        created_at_ns: 0,
    };

    let mut segment_id = 0u64;
    let mut writer = SegmentWriter::create(
        PlainFileIo,
        data_dir.join(format!("{segment_id}.seg")),
        &data_dir,
        header(segment_id),
        writer_config.clone(),
        &mut buffer,
    )
    .map_err(|error| writer_err("create", error))?;
    let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer)
        .map_err(|error| writer_err("encoder", error))?;

    let mut pending: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
    let mut records_in_segment = 0usize;
    let mut next_entry = 1u64;
    let len = config.payload_bytes;

    for _ in 0..config.max_entries {
        let entry = next_entry;
        let ledger = entry % config.ledgers + 1;
        next_entry += 1;
        let payload = Session::entry_payload(config.seed, ledger, entry, len);

        let appended = match writer.append(&mut encoder, ledger, entry, 0, &payload) {
            Ok(appended) => appended,
            Err(WriterError::SegmentFull) => {
                // `seal_at` entries do not always fit: the data-region target can
                // fill first (64 KiB holds 16 of the 1 KiB records). Sealing and
                // rotating is the writer's own contract for `SegmentFull`; without
                // this the child would die of an expected condition and the next
                // append would never see a fresh segment.
                let sealed = writer
                    .seal(encoder)
                    .map_err(|error| writer_err("seal", error))?;
                journal_acks(
                    &mut journal,
                    segment_id,
                    &sealed.acknowledged,
                    &mut pending,
                    len,
                )?;
                segment_id += 1;
                records_in_segment = 0;
                writer = SegmentWriter::create(
                    PlainFileIo,
                    data_dir.join(format!("{segment_id}.seg")),
                    &data_dir,
                    header(segment_id),
                    writer_config.clone(),
                    &mut buffer,
                )
                .map_err(|error| writer_err("create", error))?;
                encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer)
                    .map_err(|error| writer_err("encoder", error))?;
                writer
                    .append(&mut encoder, ledger, entry, 0, &payload)
                    .map_err(|error| writer_err("append", error))?
            }
            Err(error) => return Err(writer_err("append", error)),
        };
        pending.insert(appended.sequence, (ledger, entry));
        journal_acks(
            &mut journal,
            segment_id,
            &appended.acknowledged,
            &mut pending,
            len,
        )?;
        records_in_segment += 1;

        if !pending.is_empty() && writer.should_flush_at(Instant::now()) {
            let acknowledged = writer
                .flush_group(&mut encoder)
                .map_err(|error| writer_err("flush", error))?;
            journal_acks(&mut journal, segment_id, &acknowledged, &mut pending, len)?;
        }

        if records_in_segment >= config.seal_at {
            let sealed = writer
                .seal(encoder)
                .map_err(|error| writer_err("seal", error))?;
            journal_acks(
                &mut journal,
                segment_id,
                &sealed.acknowledged,
                &mut pending,
                len,
            )?;
            segment_id += 1;
            records_in_segment = 0;
            writer = SegmentWriter::create(
                PlainFileIo,
                data_dir.join(format!("{segment_id}.seg")),
                &data_dir,
                header(segment_id),
                writer_config.clone(),
                &mut buffer,
            )
            .map_err(|error| writer_err("create", error))?;
            encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer)
                .map_err(|error| writer_err("encoder", error))?;
        }

        if config.step_micros > 0 {
            thread::sleep(Duration::from_micros(config.step_micros));
        }
    }
    Ok(())
}

fn journal_acks(
    journal: &mut fs::File,
    segment_id: u64,
    acknowledged: &[u64],
    pending: &mut BTreeMap<u64, (u64, u64)>,
    len: usize,
) -> io::Result<()> {
    for sequence in acknowledged {
        let (ledger, entry) = pending
            .remove(sequence)
            .expect("writer acknowledged a sequence it never accepted");
        let record = JournalEntry {
            segment_id,
            ledger,
            entry,
            len,
        };
        // One write syscall per line: a kill can lose a trailing partial line,
        // never merge or tear an earlier one.
        let mut line = serde_json::to_vec(&record).map_err(io::Error::other)?;
        line.push(b'\n');
        journal.write_all(&line)?;
    }
    Ok(())
}

// ===== parent side =====

/// How one recovered segment file was interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveredKind {
    /// A fully valid sealed segment.
    Sealed,
    /// An open segment recovered from its durable prefix.
    Open,
    /// A never-written preallocation (all zero bytes); no record can be in it.
    Orphan,
}

/// One record surfaced by recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredRecord {
    pub ledger: u64,
    pub entry: u64,
    pub len: usize,
    pub payload: Vec<u8>,
}

/// One segment file after recovery.
#[derive(Debug, Clone)]
pub struct RecoveredSegment {
    pub segment_id: u64,
    pub kind: RecoveredKind,
    pub records: Vec<RecoveredRecord>,
}

/// Recover every `*.seg` file in a populated data directory exactly the way the
/// product does: a valid sealed view wins, an open segment is prefix-recovered,
/// an all-zero preallocation is an orphan, and anything else (invalid sealed
/// trailer, unreadable nonzero header) is a hard error, never a silent truncation.
pub fn recover_segments(data_dir: &Path) -> Result<Vec<RecoveredSegment>, String> {
    let entries = fs::read_dir(data_dir).map_err(|error| format!("{data_dir:?}: {error}"))?;
    let mut files: Vec<(u64, PathBuf)> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("{data_dir:?}: {error}"))?;
        let path = entry.path();
        if let Some(segment_id) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u64>().ok())
        {
            if path.extension().and_then(|extension| extension.to_str()) == Some("seg") {
                files.push((segment_id, path));
            }
        }
    }
    files.sort_by_key(|(segment_id, _)| *segment_id);

    let mut recovered = Vec::with_capacity(files.len());
    for (segment_id, path) in files {
        let image = fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?;
        let bytes = Bytes::from(image.clone());
        if let Ok(view) = SegmentView::open_sealed(bytes.clone()) {
            recovered.push(RecoveredSegment {
                segment_id,
                kind: RecoveredKind::Sealed,
                records: collect_records(&view)
                    .map_err(|error| format!("{}: {error}", path.display()))?,
            });
            continue;
        }
        if is_sealed_trailer(&image) {
            return Err(format!(
                "{}: sealed trailer present but the segment does not validate (corrupt sealed segment)",
                path.display()
            ));
        }
        let result = SegmentView::recover_open(bytes);
        match result.view {
            Some(view) => recovered.push(RecoveredSegment {
                segment_id,
                kind: RecoveredKind::Open,
                records: collect_records(&view)
                    .map_err(|error| format!("{}: {error}", path.display()))?,
            }),
            None => {
                if image.iter().all(|byte| *byte == 0) {
                    recovered.push(RecoveredSegment {
                        segment_id,
                        kind: RecoveredKind::Orphan,
                        records: Vec::new(),
                    });
                } else {
                    return Err(format!(
                        "{}: nonzero bytes but no readable header (corrupt header)",
                        path.display()
                    ));
                }
            }
        }
    }
    Ok(recovered)
}

fn collect_records(view: &SegmentView) -> Result<Vec<RecoveredRecord>, String> {
    let mut records = Vec::new();
    for record in view.records() {
        let record = record.map_err(|error| error.to_string())?;
        let mut payload = Vec::new();
        record
            .copy_payload_to(&mut payload)
            .map_err(|error| error.to_string())?;
        records.push(RecoveredRecord {
            ledger: record.ledger,
            entry: record.entry,
            len: record.len as usize,
            payload,
        });
    }
    Ok(records)
}

/// The durability check for one kill: every journaled acknowledgement must be
/// recovered, with the seeded payload intact, and nothing may be surfaced twice.
pub fn verify_recovery(
    seed: u64,
    journal: &[JournalEntry],
    segments: &[RecoveredSegment],
) -> Vec<String> {
    let mut violations = Vec::new();

    let mut occurrences: BTreeMap<(u64, u64), usize> = BTreeMap::new();
    for segment in segments {
        let mut seen = BTreeSet::new();
        for record in &segment.records {
            *occurrences.entry((record.ledger, record.entry)).or_default() += 1;
            if !seen.insert((record.ledger, record.entry)) {
                violations.push(format!(
                    "segment {} surfaced ledger {} entry {} twice",
                    segment.segment_id, record.ledger, record.entry
                ));
            }
            let expected = Session::entry_payload(seed, record.ledger, record.entry, record.len);
            if expected.as_ref() != record.payload.as_slice() {
                violations.push(format!(
                    "segment {} ledger {} entry {} surfaced a {} byte payload that is not the seeded payload (torn or corrupt record)",
                    segment.segment_id,
                    record.ledger,
                    record.entry,
                    record.payload.len(),
                ));
            }
        }
    }
    for ((ledger, entry), count) in &occurrences {
        if *count > 1 {
            violations.push(format!(
                "ledger {ledger} entry {entry} was surfaced {count} times across segments"
            ));
        }
    }

    for journaled in journal {
        let found = segments
            .iter()
            .flat_map(|segment| {
                segment
                    .records
                    .iter()
                    .map(move |record| (segment.segment_id, record))
            })
            .find(|(_, record)| record.ledger == journaled.ledger && record.entry == journaled.entry);
        match found {
            None => violations.push(format!(
                "acknowledged entry lost: segment {} ledger {} entry {}",
                journaled.segment_id, journaled.ledger, journaled.entry
            )),
            Some((segment_id, _record)) => {
                if segment_id != journaled.segment_id {
                    violations.push(format!(
                        "acknowledged ledger {} entry {} was journaled in segment {} but recovered from segment {segment_id}",
                        journaled.ledger, journaled.entry, journaled.segment_id
                    ));
                }
            }
        }
    }
    violations
}

/// One kill round's observations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillRound {
    pub round: u32,
    pub seed: u64,
    pub kill_delay_ms: u64,
    /// False when the child died (or failed) before the first acknowledgement.
    pub killed_mid_write: bool,
    pub journaled: usize,
    pub recovered: usize,
    pub sealed_segments: usize,
    pub open_segments: usize,
    pub orphan_segments: usize,
    pub duration_ms: u64,
    /// Kept only when the round failed; elsewhere empty.
    pub round_dir: Option<PathBuf>,
    pub violations: Vec<String>,
    pub notes: Vec<String>,
}

/// Parent configuration for a kill-soak run.
#[derive(Debug, Clone)]
pub struct KillSoakConfig {
    /// Path of the `bkxpro-validator` binary that hosts the child.
    pub child_exe: PathBuf,
    /// Root under which per-round directories are created.
    pub work_root: PathBuf,
    pub base_seed: u64,
    pub rounds: u32,
    pub payload_bytes: usize,
    pub seal_at: usize,
    pub ledgers: u64,
    pub kill_delay_ms_min: u64,
    pub kill_delay_ms_max: u64,
    /// Keep round directories even when a round passes (for inspection/tests).
    pub keep_dirs: bool,
}

impl KillSoakConfig {
    pub fn new(child_exe: impl Into<PathBuf>, work_root: impl Into<PathBuf>, base_seed: u64) -> Self {
        Self {
            child_exe: child_exe.into(),
            work_root: work_root.into(),
            base_seed,
            rounds: 8,
            payload_bytes: 1024,
            seal_at: 32,
            ledgers: 2,
            kill_delay_ms_min: 50,
            kill_delay_ms_max: 500,
            keep_dirs: false,
        }
    }
}

/// The aggregated result of a kill-soak run.
#[derive(Debug, Clone)]
pub struct KillSoakOutcome {
    pub rounds: Vec<KillRound>,
    pub violations: Vec<String>,
}

impl KillSoakOutcome {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn total_journaled(&self) -> usize {
        self.rounds.iter().map(|round| round.journaled).sum()
    }

    pub fn killed_rounds(&self) -> usize {
        self.rounds
            .iter()
            .filter(|round| round.killed_mid_write)
            .count()
    }
}

fn wait_for_journal(path: &Path, timeout: Duration) -> io::Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        if !read_journal(path)?.0.is_empty() {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn read_journal(path: &Path) -> io::Result<(Vec<JournalEntry>, usize)> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((vec![], 0)),
        Err(error) => return Err(error),
    };
    let ends_with_newline = text.ends_with('\n');
    let lines: Vec<&str> = text.lines().collect();
    let mut entries = Vec::new();
    let mut malformed = 0usize;
    for (index, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<JournalEntry>(line) {
            Ok(entry) => entries.push(entry),
            // A kill can cut the file mid-line; only the final unterminated line
            // is allowed to be garbage.
            Err(_) if !ends_with_newline && index + 1 == lines.len() => {}
            Err(_) => malformed += 1,
        }
    }
    Ok((entries, malformed))
}

/// Outcome of [`verify_session_dir`]: one recovery pass over a populated
/// session directory plus the journal check.
#[derive(Debug, Clone, Serialize)]
pub struct SessionVerifyReport {
    pub dir: PathBuf,
    pub seed: u64,
    pub journal_entries: usize,
    pub malformed_journal_lines: usize,
    pub recovered_records: usize,
    pub sealed_segments: usize,
    pub open_segments: usize,
    pub orphan_segments: usize,
    pub violations: Vec<String>,
}

impl SessionVerifyReport {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Recover a session directory the way the kill-soak parent does and check
/// every journaled acknowledgement against the recovered records. The directory
/// is either a session root (with `segments/` and `acked.jsonl`) or the data
/// directory itself; `journal` overrides the default `<dir>/acked.jsonl`.
///
/// This is the verification step the kernel fault scripts run after a
/// `dm-log-writes` replay, a remount, or a fault window.
pub fn verify_session_dir(
    dir: &Path,
    seed: u64,
    journal: Option<&Path>,
) -> Result<SessionVerifyReport, String> {
    let data_dir = if dir.join("segments").is_dir() {
        dir.join("segments")
    } else {
        dir.to_path_buf()
    };
    let segments = recover_segments(&data_dir)?;
    let journal_path = journal
        .map(Path::to_path_buf)
        .unwrap_or_else(|| dir.join(JOURNAL_FILE));
    let (journal_entries, malformed_journal_lines) = read_journal(&journal_path)
        .map_err(|error| format!("{}: {error}", journal_path.display()))?;
    let violations = verify_recovery(seed, &journal_entries, &segments);
    Ok(SessionVerifyReport {
        dir: dir.to_path_buf(),
        seed,
        journal_entries: journal_entries.len(),
        malformed_journal_lines,
        recovered_records: segments.iter().map(|segment| segment.records.len()).sum(),
        sealed_segments: segments
            .iter()
            .filter(|segment| segment.kind == RecoveredKind::Sealed)
            .count(),
        open_segments: segments
            .iter()
            .filter(|segment| segment.kind == RecoveredKind::Open)
            .count(),
        orphan_segments: segments
            .iter()
            .filter(|segment| segment.kind == RecoveredKind::Orphan)
            .count(),
        violations,
    })
}

/// Run `config.rounds` kill rounds. `on_round` is called once per completed
/// round so long soaks can print progress.
pub fn run_kill_soak(
    config: &KillSoakConfig,
    mut on_round: impl FnMut(&KillRound),
) -> io::Result<KillSoakOutcome> {
    fs::create_dir_all(&config.work_root)?;
    let mut outcome = KillSoakOutcome {
        rounds: Vec::new(),
        violations: Vec::new(),
    };

    for round in 0..config.rounds {
        let mut rng = DetRng::new(config.base_seed)
            .derive_from(&[b"kill-soak-round", &(round as u64).to_le_bytes()]);
        let seed = rng.next_u64();
        let kill_delay_ms = rng.range_inclusive(config.kill_delay_ms_min, config.kill_delay_ms_max);

        let dir = config.work_root.join(format!("kill-round-{round:04}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("segments"))?;
        let started = Instant::now();

        let mut command = Command::new(&config.child_exe);
        command
            .args(child_arguments(&dir, seed, config))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("spawn {}: {error}", config.child_exe.display()),
            )
        })?;

        let journal_path = dir.join(JOURNAL_FILE);
        let first_ack = wait_for_journal(&journal_path, Duration::from_secs(30))?;
        let mut killed_mid_write = false;
        if first_ack {
            thread::sleep(Duration::from_millis(kill_delay_ms));
        }
        if child.try_wait()?.is_none() {
            let killed = child.kill().is_ok();
            killed_mid_write = first_ack && killed;
        }
        let output = child.wait_with_output()?;

        let (journal, malformed) = read_journal(&journal_path)?;
        let mut violations = Vec::new();
        let mut notes = Vec::new();
        if malformed > 0 {
            violations.push(format!("journal has {malformed} malformed lines"));
        }
        if !first_ack {
            violations.push(format!(
                "child produced no acknowledgement within 30 s (exit: {:?})",
                output.status
            ));
        } else if !killed_mid_write || output.status.signal() != Some(libc::SIGKILL) {
            violations.push(format!(
                "child exited before a mid-write SIGKILL (exit: {:?})",
                output.status
            ));
        }
        let segments_result = recover_segments(&dir.join("segments"));
        let (segments, sealed, open, orphan) = match segments_result {
            Ok(segments) => {
                let sealed = segments
                    .iter()
                    .filter(|segment| segment.kind == RecoveredKind::Sealed)
                    .count();
                let open = segments
                    .iter()
                    .filter(|segment| segment.kind == RecoveredKind::Open)
                    .count();
                let orphan = segments
                    .iter()
                    .filter(|segment| segment.kind == RecoveredKind::Orphan)
                    .count();
                (segments, sealed, open, orphan)
            }
            Err(error) => {
                violations.push(format!("recovery failed: {error}"));
                (Vec::new(), 0, 0, 0)
            }
        };
        violations.extend(verify_recovery(seed, &journal, &segments));
        if journal.is_empty() && killed_mid_write {
            notes.push("killed before any acknowledgement became durable".to_string());
        }

        let stderr_text = String::from_utf8_lossy(&output.stderr).into_owned();
        if !stderr_text.trim().is_empty() {
            fs::write(dir.join("child-stderr.txt"), &stderr_text)?;
            notes.push(format!(
                "child wrote {} bytes to stderr (kept in child-stderr.txt)",
                stderr_text.len()
            ));
        }

        let ok = violations.is_empty();
        let keep = config.keep_dirs || !ok;
        let round_dir = keep.then(|| dir.clone());
        let finished = KillRound {
            round,
            seed,
            kill_delay_ms,
            killed_mid_write,
            journaled: journal.len(),
            recovered: segments.iter().map(|segment| segment.records.len()).sum(),
            sealed_segments: sealed,
            open_segments: open,
            orphan_segments: orphan,
            duration_ms: started.elapsed().as_millis() as u64,
            round_dir,
            violations: violations
                .iter()
                .map(|violation| format!("round {round}: {violation}"))
                .collect(),
            notes,
        };
        if !ok {
            outcome
                .violations
                .extend(finished.violations.iter().cloned());
        } else if !config.keep_dirs {
            let _ = fs::remove_dir_all(&dir);
        }
        outcome.rounds.push(finished.clone());
        on_round(&finished);
    }
    Ok(outcome)
}

fn child_arguments(dir: &Path, seed: u64, config: &KillSoakConfig) -> Vec<String> {
    vec![
        "kill-soak-child".to_string(),
        "--dir".to_string(),
        dir.display().to_string(),
        "--seed".to_string(),
        seed.to_string(),
        "--payload-bytes".to_string(),
        config.payload_bytes.to_string(),
        "--seal-at".to_string(),
        config.seal_at.to_string(),
        "--ledgers".to_string(),
        config.ledgers.to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn journal_entry(segment_id: u64, ledger: u64, entry: u64, len: usize) -> JournalEntry {
        JournalEntry {
            segment_id,
            ledger,
            entry,
            len,
        }
    }

    fn recovered_record(seed: u64, ledger: u64, entry: u64, len: usize) -> RecoveredRecord {
        let payload = Session::entry_payload(seed, ledger, entry, len);
        RecoveredRecord {
            ledger,
            entry,
            len,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn a_clean_recovery_passes() {
        let seed = 77;
        let segments = vec![RecoveredSegment {
            segment_id: 0,
            kind: RecoveredKind::Open,
            records: vec![recovered_record(seed, 1, 1, 16), recovered_record(seed, 2, 2, 16)],
        }];
        let journal = vec![journal_entry(0, 1, 1, 16)];
        assert!(verify_recovery(seed, &journal, &segments).is_empty());
    }

    #[test]
    fn a_lost_acknowledgement_is_a_violation() {
        let seed = 5;
        let segments = vec![RecoveredSegment {
            segment_id: 0,
            kind: RecoveredKind::Open,
            records: vec![recovered_record(seed, 1, 1, 8)],
        }];
        let journal = vec![journal_entry(0, 1, 1, 8), journal_entry(0, 2, 2, 8)];
        let violations = verify_recovery(seed, &journal, &segments);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("acknowledged entry lost"), "{violations:?}");
    }

    #[test]
    fn a_torn_payload_is_a_violation() {
        let seed = 5;
        let mut record = recovered_record(seed, 1, 1, 8);
        record.payload[0] ^= 0xFF;
        let segments = vec![RecoveredSegment {
            segment_id: 0,
            kind: RecoveredKind::Open,
            records: vec![record],
        }];
        let violations = verify_recovery(seed, &[], &segments);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("torn or corrupt"), "{violations:?}");
    }

    #[test]
    fn a_duplicate_record_is_a_violation() {
        let seed = 5;
        let record = recovered_record(seed, 1, 1, 8);
        let segments = vec![
            RecoveredSegment {
                segment_id: 0,
                kind: RecoveredKind::Open,
                records: vec![record.clone()],
            },
            RecoveredSegment {
                segment_id: 1,
                kind: RecoveredKind::Open,
                records: vec![record.clone()],
            },
        ];
        let violations = verify_recovery(seed, &[], &segments);
        assert!(
            violations.iter().any(|v| v.contains("surfaced 2 times")),
            "{violations:?}"
        );
    }

    #[test]
    fn journal_lines_are_read_leniently_after_a_kill() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join(JOURNAL_FILE);
        let mut file = fs::File::create(&path).unwrap();
        writeln!(file, r#"{{"segment_id":0,"ledger":1,"entry":1,"len":8}}"#).unwrap();
        // A killed process can leave a half-written trailing line.
        write!(file, r#"{{"segment_id":0,"ledge"#).unwrap();
        drop(file);
        let (entries, malformed) = read_journal(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(malformed, 0);

        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "not json").unwrap();
        writeln!(file, r#"{{"segment_id":0,"ledger":2,"entry":2,"len":8}}"#).unwrap();
        let (entries, malformed) = read_journal(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(malformed, 1);
    }
}
