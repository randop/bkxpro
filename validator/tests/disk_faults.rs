//! Per-fault coverage: every disk-fault variant the simulator can inject is run
//! through a scenario, and corruption of a sealed segment must never be silently
//! uploaded or "repaired".
//!
//! Two corruption classes are distinguished because the uploader's digest covers
//! only the header block, the trailer block, and the footer:
//!
//! * corruption inside the digest's reach (here: the header block, which the
//!   index rebuild never reads) must fail-stop the uploader before any PUT or
//!   manifest commit, and recovery must refuse to "repair" the segment;
//! * corruption below it (a data-block payload byte) is a known validation gap:
//!   the segment is uploaded and committed, and only read-back / remote
//!   verification catch the corrupt bytes. The second test pins that behavior, so
//!   closing the gap later shows up as a test change instead of a silent surprise.

use bkxpro_codec::HEADER_SIZE;
use bkxpro_validator::lifecycle::{AppendSpec, WriteOutcome, WriteSpec};
use bkxpro_validator::plan::{DiskFault, FaultArm, Plan};
use bkxpro_validator::{preset, run_plan, Session, TraceEvent, WriteEnd};

fn report_with_fault(fault: DiskFault, at: usize) -> bkxpro_validator::RunReport {
    let mut plan = preset("disk-faults", 9).unwrap();
    plan.disk_faults = vec![FaultArm {
        at_append: at,
        fault,
    }];
    run_plan(&plan).expect("scenario runs")
}

#[test]
fn write_faults_survive_recovery() {
    for fault in [
        DiskFault::FailWrite,
        DiskFault::Eagain,
        DiskFault::Enospc,
        DiskFault::FailSync,
        DiskFault::Torn {
            prefix_bytes: 2048,
            garbage_bytes: 256,
        },
    ] {
        let report = report_with_fault(fault, 5);
        assert!(
            report.ok,
            "{fault:?}: violations {:?} notes {:?}",
            report.violations, report.notes
        );
    }
}

#[test]
fn acquire_faults_fail_the_write_phase_cleanly() {
    for fault in [DiskFault::FailAcquire, DiskFault::FailDirectorySync] {
        let report = report_with_fault(fault, 0);
        assert!(
            report.ok,
            "{fault:?}: violations {:?} notes {:?}",
            report.violations, report.notes
        );
        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains("did not start")),
            "{fault:?}: the write phase should have failed before starting; notes {:?}",
            report.notes
        );
    }
}

/// Write one clean sealed segment 0 and return its outcome; every payload is
/// registered with the session so read-back can verify it later.
fn write_sealed_zero(session: &mut Session, plan: &Plan) -> WriteOutcome {
    let ledgers = plan.work.ledgers.max(1);
    let entries = (0..plan.work.entries_per_segment)
        .map(|index| {
            let ledger = index % ledgers + 1;
            let entry = index;
            let payload = Session::entry_payload(plan.seed, ledger, entry, plan.work.payload_bytes);
            session.remember_payload(ledger, entry, &payload);
            AppendSpec {
                ledger,
                entry,
                flags: 0,
                payload,
            }
        })
        .collect::<Vec<_>>();
    let outcome = session
        .write_segment(&WriteSpec {
            segment_id: 0,
            epoch: 1,
            entries,
            arms: Vec::new(),
            stop_after: 0,
            seal: true,
        })
        .expect("clean sealed write");
    assert!(
        matches!(outcome.end, WriteEnd::Sealed { .. }),
        "the clean write must seal, got {:?} ({:?})",
        outcome.end,
        outcome.error
    );
    outcome
}

fn recorded_corrupt_sealed(session: &Session) -> bool {
    session.trace.events().iter().any(|event| {
        matches!(
            event,
            TraceEvent::Checkpoint { name } if name.starts_with("corrupt-sealed:")
        )
    })
}

fn recorded_unlink(session: &Session, segment_id: u64) -> bool {
    session.trace.events().iter().any(
        |event| matches!(event, TraceEvent::LocalUnlinked { segment_id: id, .. } if *id == segment_id),
    )
}

/// Flipping a byte inside the sealed header block must poison the uploader
/// (fail-stop) instead of uploading, and recovery must keep the file for that
/// fail-stop rather than re-sealing it.
#[test]
fn corrupt_sealed_header_fail_stops_the_uploader() {
    let plan = preset("disk-faults", 9).unwrap();
    let mut session = Session::new(&plan).unwrap();
    write_sealed_zero(&mut session, &plan);

    let path = session.segment_path(0);
    session
        .sim
        .corrupt(&path, 0, 0xFF)
        .expect("the header magic byte flips");

    // The rebuild indexes entry locations from the footer and never reads the
    // header, so the segment still has a meta row; recovery then refuses to
    // "repair" the bytes it cannot validate.
    let restart = session.restart().unwrap();
    assert_eq!(restart.segments_rebuilt, 1);
    assert_eq!(restart.segments_failed, 0);
    assert!(restart.repairs.is_empty());
    assert!(
        recorded_corrupt_sealed(&session),
        "recovery must record that it refused to repair the corrupt sealed segment"
    );

    let result = session.upload_pass();
    match &result {
        Err((
            _,
            bkxpro_validator::HarnessError::Uploader(
                bkxpro_uploader::UploaderError::CorruptSegment { segment_id, .. },
            ),
        )) => assert_eq!(*segment_id, 0),
        other => panic!("expected CorruptSegment, got {other:?}"),
    }
    assert!(session.uploader_poisoned());
    assert!(
        session.sim.visible_bytes(&path).is_some(),
        "a corrupt segment must never be deleted"
    );
    assert!(
        !recorded_unlink(&session, 0),
        "a corrupt segment must never be unlinked"
    );
    assert!(
        !session
            .mock()
            .manifest_records()
            .iter()
            .any(|record| record.segment_id == 0),
        "a segment that fails validation must never be committed to the manifest"
    );
}

/// A data-block flip is below the digest's validation coverage: the uploader
/// uploads and commits the corrupt bytes and deletes the local file. Read-back on
/// the local copy and remote verification both detect the corruption, which is
/// the harness's safety net until the uploader validates full block content.
#[test]
fn data_block_corruption_is_caught_by_verification_not_the_uploader() {
    let plan = preset("disk-faults", 9).unwrap();
    let mut session = Session::new(&plan).unwrap();
    let outcome = write_sealed_zero(&mut session, &plan);
    let pairs: Vec<(u64, u64)> = outcome.records.iter().copied().collect();

    // 128 bytes into the first record's payload: records start right after the
    // header block, and the record header is 21 bytes, so this lands in payload.
    let path = session.segment_path(0);
    session
        .sim
        .corrupt(&path, (HEADER_SIZE + 128) as u64, 0x5A)
        .expect("the first record's payload flips");

    let restart = session.restart().unwrap();
    assert_eq!(restart.segments_rebuilt, 1);
    assert!(recorded_corrupt_sealed(&session));

    // The local reader returns the corrupted bytes; read-back compares payloads
    // and must not accept them.
    let read_back = session.read_back(&pairs).unwrap();
    assert!(
        !read_back.failures.is_empty(),
        "read-back must detect the corrupted payload, got {read_back:?}"
    );

    // Known gap: the digest never reads the data blocks, so the uploader accepts
    // the segment. This assertion pins the gap; when the uploader grows full
    // content validation, this test must change to expect CorruptSegment.
    let report = session
        .upload_pass()
        .expect("digest checks cannot see data-block corruption");
    assert_eq!(report.uploaded, 1);

    let verified = session.verify_remote(&[(0, 1)]).unwrap();
    assert!(
        !verified.failures.is_empty(),
        "remote verification must not trust the committed object, got {verified:?}"
    );
}
