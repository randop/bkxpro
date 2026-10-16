//! Integration test for the real-process kill soak (RAN-132).
//!
//! Drives the `bkxpro-validator` binary (located through `CARGO_BIN_EXE_...`) so the
//! process that gets `SIGKILL`ed is a real, cargo-built executable writing
//! through `std::fs`, exactly like the nightly job.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use bkxpro_validator::{
    kill_soak::{recover_segments, run_kill_soak, verify_recovery, JournalEntry, RecoveredKind},
    KillSoakConfig,
};

fn kill_config(dir: &Path, rounds: u32) -> KillSoakConfig {
    let mut config = KillSoakConfig::new(
        PathBuf::from(env!("CARGO_BIN_EXE_bkxpro-validator")),
        dir,
        1,
    );
    config.rounds = rounds;
    config.keep_dirs = true;
    config
}

#[test]
fn sigkill_mid_append_recovers_every_acknowledged_entry() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = kill_config(dir.path(), 3);
    let outcome = run_kill_soak(&config, |_| {}).unwrap();

    assert!(
        outcome.ok(),
        "kill soak produced violations: {:?}",
        outcome.violations
    );
    assert_eq!(outcome.rounds.len(), 3);
    assert_eq!(outcome.killed_rounds(), 3, "every round must kill the child");
    assert!(
        outcome.total_journaled() > 0,
        "no round acknowledged anything; the harness is not exercising the kill window"
    );
    for round in &outcome.rounds {
        assert!(round.journaled > 0, "round {} journaled nothing", round.round);
        // The parent only records a round as killed after the first ack and a
        // seeded delay, so there must be at least one recovered record.
        assert!(round.recovered >= round.journaled);
    }
}

#[test]
fn verification_catches_a_lost_acknowledgement_in_a_real_round() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = kill_config(dir.path(), 1);
    let outcome = run_kill_soak(&config, |_| {}).unwrap();
    assert!(outcome.ok(), "{:?}", outcome.violations);
    let round = &outcome.rounds[0];
    let round_dir = round.round_dir.as_ref().expect("kept dir");

    // Reconstruct the round's own verification inputs and prove the checker has
    // teeth: fabricating an acknowledgement that was never written must fail.
    let seed = round.seed;
    let segments = recover_segments(&round_dir.join("segments")).unwrap();
    let journal_text = fs::read_to_string(round_dir.join("acked.jsonl")).unwrap();
    let mut journal: Vec<JournalEntry> = journal_text
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    assert_eq!(journal.len(), round.journaled);
    assert!(verify_recovery(seed, &journal, &segments).is_empty());

    journal.push(JournalEntry {
        segment_id: 0,
        ledger: 1,
        entry: u64::MAX,
        len: 1024,
    });
    let violations = verify_recovery(seed, &journal, &segments);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("acknowledged entry lost")),
        "{violations:?}"
    );
}

#[test]
fn orphan_preallocations_are_recognized_not_recovered() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = kill_config(dir.path(), 1);
    let outcome = run_kill_soak(&config, |_| {}).unwrap();
    assert!(outcome.ok(), "{:?}", outcome.violations);
    let round_dir = outcome.rounds[0].round_dir.as_ref().expect("kept dir");

    // A fresh preallocation that never got its header is exactly what a kill
    // during `create` leaves behind; it must be an orphan, not a recovery error.
    let orphan = round_dir.join("segments/9999.seg");
    fs::write(&orphan, vec![0u8; 64 * 1024]).unwrap();
    let segments = recover_segments(&round_dir.join("segments")).unwrap();
    let orphan_segment = segments
        .iter()
        .find(|segment| segment.segment_id == 9999)
        .expect("orphan listed");
    assert!(orphan_segment.records.is_empty());
    assert_eq!(
        orphan_segment.kind,
        bkxpro_validator::kill_soak::RecoveredKind::Orphan
    );

    // A nonzero byte in an otherwise zero preallocation is a corrupt header and
    // must be an error, never a silent truncation.
    let mut image = vec![0u8; 64 * 1024];
    image[100] = 0xAB;
    fs::write(&orphan, &image).unwrap();
    let error = recover_segments(&round_dir.join("segments")).unwrap_err();
    assert!(error.contains("corrupt header"), "{error}");
}

#[test]
fn a_sigkilled_child_keeps_its_journal_as_jsonl() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = kill_config(dir.path(), 1);
    let outcome = run_kill_soak(&config, |_| {}).unwrap();
    assert!(outcome.ok(), "{:?}", outcome.violations);
    let round_dir = outcome.rounds[0].round_dir.as_ref().expect("kept dir");
    let text = fs::read_to_string(round_dir.join("acked.jsonl")).unwrap();
    let mut parsed = 0usize;
    for line in text.lines() {
        // Only the final line may be a partial write from the kill.
        if let Ok(entry) = serde_json::from_str::<JournalEntry>(line) {
            parsed += 1;
            assert!(entry.len > 0, "journal must record the payload length");
            assert!(entry.entry > 0);
        }
    }
    assert_eq!(parsed, outcome.rounds[0].journaled);

    // The child must not have written anything to stderr in a clean run.
    let stderr = round_dir.join("child-stderr.txt");
    assert!(
        !stderr.exists(),
        "unexpected child stderr: {}",
        fs::read_to_string(&stderr).unwrap_or_default()
    );
}

/// Regression: the data-region target can fill before `seal_at` entries are
/// appended (one 4000-byte record per 4 KiB block, so a 64 KiB segment holds 16).
/// The child must seal and rotate on `SegmentFull`, not die of an expected
/// condition: run it past the capacity and prove every record survives, full
/// segments sealed and only the final segment left open (the child stops writing
/// without a final seal, as after a kill).
#[test]
fn the_child_rotates_when_the_target_fills_before_seal_at() {
    let dir = tempfile::TempDir::new().unwrap();
    let data_dir = dir.path().join("segments");
    fs::create_dir_all(&data_dir).unwrap();
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_bkxpro-validator"))
        .args([
            "kill-soak-child",
            "--dir",
            dir.path().to_str().unwrap(),
            "--seed",
            "7",
            "--payload-bytes",
            "4000",
            "--seal-at",
            "1000",
            "--max-entries",
            "40",
            "--step-micros",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "child exited with {status}");

    let journal_text = fs::read_to_string(dir.path().join("acked.jsonl")).unwrap();
    let journal: Vec<JournalEntry> = journal_text
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();

    let segments = recover_segments(&data_dir).unwrap();
    assert!(
        segments.len() > 1,
        "40 records must not fit one 64 KiB segment: {segments:?}"
    );
    let sealed = segments
        .iter()
        .filter(|segment| segment.kind == RecoveredKind::Sealed)
        .count();
    assert!(
        sealed >= 2,
        "rotation must seal every full segment, not just rotate: {segments:?}"
    );
    let open: Vec<&_> = segments
        .iter()
        .filter(|segment| segment.kind == RecoveredKind::Open)
        .collect();
    assert!(
        open.len() <= 1,
        "at most the final segment may stay open: {segments:?}"
    );
    if let Some(open) = open.first() {
        assert_eq!(
            open.segment_id,
            segments.last().unwrap().segment_id,
            "only the final segment may remain open: {segments:?}"
        );
    }
    // The child exits between group commits, so its final records may still be
    // buffered and never acknowledged; recovery must surface everything that was
    // acknowledged, and nothing beyond what was attempted (40).
    let total: usize = segments
        .iter()
        .map(|segment| segment.records.len())
        .sum();
    assert!(total <= 40, "recovered more records than were appended: {total}");
    let mut recovered_entries: Vec<u64> = segments
        .iter()
        .flat_map(|segment| segment.records.iter().map(|record| record.entry))
        .collect();
    recovered_entries.sort_unstable();
    let expected: Vec<u64> = (1..=total as u64).collect();
    assert_eq!(
        recovered_entries, expected,
        "recovered records must be a hole-free prefix of the appended entries"
    );
    assert!(!journal.is_empty(), "no acknowledgement was journaled");
    let violations = verify_recovery(7, &journal, &segments);
    assert!(violations.is_empty(), "{violations:?}");
}

/// Not required by the checks above, but cheap insurance: the journal and the
/// segments are the only artifacts the parent trusts, so ensure a truncated
/// journal (as after a kill) still parses to a prefix.
#[test]
fn a_truncated_journal_parses_as_a_prefix() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = kill_config(dir.path(), 1);
    let outcome = run_kill_soak(&config, |_| {}).unwrap();
    assert!(outcome.ok(), "{:?}", outcome.violations);
    let round_dir = outcome.rounds[0].round_dir.as_ref().expect("kept dir");
    let journal_path = round_dir.join("acked.jsonl");
    let text = fs::read_to_string(&journal_path).unwrap();
    if text.len() < 4 {
        return;
    }
    let cut = text.len() / 2;
    let mut truncated = text.clone();
    truncated.truncate(cut);
    let mut file = fs::File::create(&journal_path).unwrap();
    file.write_all(truncated.as_bytes()).unwrap();
    drop(file);

    let segments = recover_segments(&round_dir.join("segments")).unwrap();
    let prefix: Vec<JournalEntry> = truncated
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    assert!(verify_recovery(outcome.rounds[0].seed, &prefix, &segments).is_empty());
}

#[test]
fn an_acknowledged_child_that_exits_before_the_kill_is_a_violation() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::TempDir::new().unwrap();
    let child = dir.path().join("early-exit.sh");
    fs::write(
        &child,
        r##"#!/usr/bin/env bash
while [ "$1" != --dir ]; do shift; done
printf '%s\n' '{"segment_id":0,"ledger":1,"entry":1,"len":1}' > "$2/acked.jsonl"
exit 0
"##,
    )
    .unwrap();
    fs::set_permissions(&child, fs::Permissions::from_mode(0o755)).unwrap();
    let mut config = kill_config(dir.path(), 1);
    config.child_exe = child;
    // Ensure the child has exited before the parent checks whether to kill.
    config.kill_delay_ms_min = 100;
    config.kill_delay_ms_max = 100;
    let outcome = run_kill_soak(&config, |_| {}).unwrap();
    assert!(!outcome.ok());
    assert!(!outcome.rounds[0].killed_mid_write);
    assert!(outcome
        .violations
        .iter()
        .any(|v| v.contains("exited before a mid-write SIGKILL")));
    assert!(!outcome
        .violations
        .iter()
        .any(|v| v.contains("no acknowledgement")));
}
