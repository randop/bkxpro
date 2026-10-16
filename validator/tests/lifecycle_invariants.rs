//! Focused lifecycle invariants, driven directly through [`Session`] so each test
//! controls exactly which durability model it exercises.

use bkxpro_validator::lifecycle::{AppendSpec, WriteEnd, WriteSpec};
use bkxpro_validator::plan::{DiskFault, FaultArm};
use bkxpro_validator::{preset, Plan, Session, TraceEvent};

fn entries_for(session: &mut Session, plan: &Plan, segment_id: u64) -> Vec<AppendSpec> {
    let ledgers = plan.work.ledgers.max(1);
    (0..plan.work.entries_per_segment)
        .map(|index| {
            let ledger = index % ledgers + 1;
            let entry = segment_id * 1_000_000 + index;
            let payload =
                Session::entry_payload(plan.seed, ledger, entry, plan.work.payload_bytes);
            session.remember_payload(ledger, entry, &payload);
            AppendSpec {
                ledger,
                entry,
                flags: 0,
                payload,
            }
        })
        .collect()
}

fn sealed_write(
    session: &mut Session,
    plan: &Plan,
    segment_id: u64,
) -> Vec<(u64, u64)> {
    let entries = entries_for(session, plan, segment_id);
    let outcome = session
        .write_segment(&WriteSpec {
            segment_id,
            epoch: 1,
            entries,
            arms: Vec::new(),
            stop_after: 0,
            seal: true,
        })
        .unwrap();
    assert!(matches!(outcome.end, WriteEnd::Sealed { .. }));
    outcome.records
}

/// An acknowledged entry must survive a power cut even when the append *after*
/// it is torn, and the torn append itself must never be acknowledged.
#[test]
fn acknowledged_entries_survive_a_torn_power_cut() {
    let mut plan = preset("crash-lifecycle", 3).unwrap();
    plan.work.entries_per_segment = 32;
    plan.work.payload_bytes = 1024;
    plan.writer.group_commit_bytes = 4096;
    // The writer commits every third append (records are block-packed), so the
    // torn fault armed at 23 fires inside the commit that append 24 triggers. The
    // loop must run past the stop point for the fault to land; the stop point is
    // only a safety net against the fault never firing.
    plan.stop_first_segment_after = 32;
    plan.disk_faults = vec![FaultArm {
        at_append: 23,
        fault: DiskFault::Torn {
            prefix_bytes: 2048,
            garbage_bytes: 256,
        },
    }];
    plan.power_cut_after_write = true;

    let mut session = Session::new(&plan).unwrap();
    let entries = entries_for(&mut session, &plan, 0);
    let outcome = session
        .write_segment(&WriteSpec {
            segment_id: 0,
            epoch: 1,
            entries,
            arms: plan.disk_faults.clone(),
            stop_after: plan.stop_first_segment_after,
            seal: true,
        })
        .unwrap();

    assert!(
        matches!(outcome.end, WriteEnd::Failed),
        "the torn write must poison the append loop, got {:?}",
        outcome.end
    );
    assert!(
        !outcome.acked.is_empty(),
        "the plan must acknowledge entries before the torn write"
    );
    let torn = (2, 23);
    assert!(
        !outcome
            .acked
            .iter()
            .any(|ack| (ack.ledger, ack.entry) == torn),
        "the torn record must never be acknowledged"
    );

    session.crash_device().unwrap();
    let restart = session.restart().unwrap();
    let recovered = session
        .recovered_records
        .get(&0)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        restart.repairs.len(),
        1,
        "the open segment must be repaired exactly once"
    );
    for ack in &outcome.acked {
        assert!(
            recovered.contains(&(ack.ledger, ack.entry)),
            "acknowledged ({}, {}) was lost across the power cut; recovered {:?}",
            ack.ledger,
            ack.entry,
            recovered
        );
    }

    // The repaired segment must upload and read back.
    session.drive_uploads(50).unwrap();
    let acked_pairs: Vec<(u64, u64)> = outcome
        .acked
        .iter()
        .map(|ack| (ack.ledger, ack.entry))
        .collect();
    let read_back = session.read_back(&acked_pairs).unwrap();
    assert!(
        read_back.failures.is_empty(),
        "read-back failures: {:?}",
        read_back.failures
    );
    assert_eq!(read_back.checked, acked_pairs.len());
    assert!(session.check_invariants().is_empty());
}

/// The delete-gating invariant holds with events produced by the real uploader,
/// not by hand-built traces.
#[test]
fn unlink_gating_holds_on_the_real_upload_path() {
    let plan = preset("smoke", 5).unwrap();
    let mut session = Session::new(&plan).unwrap();
    let mut all = Vec::new();
    for segment_id in 0..plan.work.segments {
        all.extend(sealed_write(&mut session, &plan, segment_id));
    }
    session.restart().unwrap();
    let summary = session.drive_uploads(50).unwrap();
    assert_eq!(summary.pending, 0);
    assert!(session.uploader_error().is_none());

    let events = session.trace.events();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, TraceEvent::LocalUnlinked { .. })),
        "the run must actually delete local segments"
    );
    assert!(session.trace.delete_gating_violations().is_empty());
    assert!(session.check_invariants().is_empty());
    assert!(session.read_back(&all).unwrap().failures.is_empty());
}

/// A fenced writer must fail-stop: no commit of the old epoch, no local unlink.
#[test]
fn a_fenced_writer_never_commits_and_never_deletes() {
    let plan = preset("fenced", 2).unwrap();
    let fence = plan.fence.expect("fenced plan carries a fence");
    let mut session = Session::new(&plan).unwrap();
    for segment_id in 0..plan.work.segments {
        sealed_write(&mut session, &plan, segment_id);
    }
    session.restart().unwrap();
    session.mock().fence_after_put(fence.segment_index, fence.epoch);

    let result = session.upload_pass();
    match &result {
        Err(bkxpro_validator::HarnessError::Uploader(
            bkxpro_uploader::UploaderError::Fenced { segment_id, .. },
        )) => assert_eq!(*segment_id, fence.segment_index),
        other => panic!("expected an UploaderError::Fenced, got {other:?}"),
    }
    assert!(session.uploader_poisoned());

    let target = fence.segment_index;
    let unlinked = session.trace.events().iter().any(
        |event| matches!(event, TraceEvent::LocalUnlinked { segment_id, .. } if *segment_id == target),
    );
    assert!(!unlinked, "a fenced segment must never be unlinked");
    assert!(
        session
            .sim
            .visible_bytes(&session.segment_path(target))
            .is_some(),
        "the fenced local file must be retained"
    );
    let our_epoch = session.sealed_epochs.get(&target).copied().unwrap();
    let committed = session
        .mock()
        .manifest_records()
        .iter()
        .any(|record| record.segment_id == target && record.writer_epoch == our_epoch);
    assert!(
        !committed,
        "the old epoch must never appear in the manifest after fencing"
    );
    assert_eq!(session.check_invariants(), Vec::<String>::new());
}
