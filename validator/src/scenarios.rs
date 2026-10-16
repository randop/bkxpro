//! Scenario drivers: run one seeded [`Plan`] through the simulated lifecycle and
//! collect a [`RunReport`].
//!
//! Every driver follows the same shape: produce deterministic work, drive the
//! real writer/index/uploader code paths, inject the planned faults, simulate the
//! planned durability event (process death, power cut, unlink crash window), and
//! then verify through read-back and the object store. Assertions land in
//! [`RunState::violations`]; observations land in `notes`.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::{
    lifecycle::{
        to_simulated_fault, AppendSpec, HarnessError, ReadBackReport, RemoteVerifyReport,
        RepairReport, RestartReport, Session, WriteEnd, WriteSpec,
    },
    plan::{CrashPlan, DiskFault, FaultArm, Plan, ScenarioKind},
    trace::TraceEvent,
};

/// Run one plan end to end. The returned report is fully serializable: it is the
/// unit of the JSONL run log and of the incident replay record.
pub fn run_plan(plan: &Plan) -> Result<RunReport, HarnessError> {
    let started = Instant::now();
    let mut session = Session::new(plan)?;
    let mut run = RunState::default();

    match plan.kind {
        ScenarioKind::Smoke => smoke(&mut session, plan, &mut run)?,
        ScenarioKind::CrashUpload => crash_upload(&mut session, plan, &mut run)?,
        ScenarioKind::CrashLifecycle => crash_lifecycle(&mut session, plan, &mut run)?,
        ScenarioKind::PowerCut => power_cut(&mut session, plan, &mut run)?,
        ScenarioKind::DiskFaults => disk_faults(&mut session, plan, &mut run)?,
        ScenarioKind::Fenced => fenced(&mut session, plan, &mut run)?,
        ScenarioKind::S3Chaos => s3_chaos(&mut session, plan, &mut run)?,
        ScenarioKind::Outage => outage(&mut session, plan, &mut run)?,
        ScenarioKind::SlowSync => slow_sync(&mut session, plan, &mut run)?,
    }

    run.violations.extend(session.check_invariants());
    Ok(build_report(plan, &session, run, started))
}

// ===== scenario drivers =====

/// Happy path: write, seal, rebuild, read locally, upload, delete, read remotely.
fn smoke(
    session: &mut Session,
    plan: &Plan,
    run: &mut RunState,
) -> Result<(), HarnessError> {
    let entries = write_all_sealed(session, plan)?;
    let restart = session.restart()?;
    run.record_restart(&restart);
    session.step("index-rebuilt");

    let local = session.read_back(&entries)?;
    run.note_read_back("local", local);

    upload_until_settled(session, run, 100)?;
    session.step("uploads-settled");
    expect_all_deleted(session, run);

    let remote = session.read_back(&entries)?;
    run.note_read_back("remote", remote);
    run.remote_verify = Some(session.verify_remote(&fully_uploaded_epochs(session))?);
    Ok(())
}

/// Process death in the middle of the upload pipeline (after a successful PUT or
/// CAS, in the windows the manifest is designed to repair).
fn crash_upload(
    session: &mut Session,
    plan: &Plan,
    run: &mut RunState,
) -> Result<(), HarnessError> {
    let entries = write_all_sealed(session, plan)?;
    session.step("write-complete");

    let crash = plan.crash.expect("crash-upload plan carries a crash");
    if let Some(index) = crash.after_successful_puts {
        session.mock().crash_after_successful_put(index);
    }
    if let Some(index) = crash.after_successful_cas {
        session.mock().crash_after_successful_cas(index);
    }

    let crashed = drive_upload_life(session, run, Some(crash), 100)?;
    run.expect(crashed, "the armed crash point never fired");
    run.expect(
        run.notes.iter().any(|note| note.contains("upload pass error")),
        "the crash point must surface as an upload-pass error in the first life",
    );
    session.trace.record(TraceEvent::Crash {
        point: format!(
            "uploads: after_puts={:?} after_cas={:?}",
            crash.after_successful_puts, crash.after_successful_cas
        ),
    });
    session.step("crashed");

    let restart = session.restart()?;
    run.record_restart(&restart);
    recover_uploader(session, run)?;
    session.step("recovered");

    upload_until_settled(session, run, 100)?;
    expect_all_deleted(session, run);

    let read_back = session.read_back_available(&entries)?;
    run.note_read_back("remote", read_back);
    run.remote_verify = Some(session.verify_remote(&fully_uploaded_epochs(session))?);
    Ok(())
}

/// Process death mid-append: the segment stays open and must be recovered by the
/// restart path (and by a power cut when the plan asks for one).
fn crash_lifecycle(
    session: &mut Session,
    plan: &Plan,
    run: &mut RunState,
) -> Result<(), HarnessError> {
    let stop_after = plan.stop_first_segment_after;
    run.expect(stop_after > 0, "crash-lifecycle plan stops mid-append");
    let entries = entries_for(session, plan, 0);
    let outcome = session.write_segment(&WriteSpec {
        segment_id: 0,
        epoch: 1,
        entries,
        arms: plan.disk_faults.clone(),
        stop_after,
        seal: true,
    })?;
    session.step("mid-append");
    run.expect(
        matches!(outcome.end, WriteEnd::Stopped),
        format!(
            "stopped append should end cleanly open, got {:?} ({:?})",
            outcome.end, outcome.error
        ),
    );

    if plan.power_cut_after_write {
        session.crash_device()?;
        session.step("power-cut");
    }
    let restart = session.restart()?;
    run.record_restart(&restart);
    session.step("repaired");

    let mut entries = outcome.records.clone();
    for segment_id in 1..plan.work.segments {
        entries.extend(sealed_segment_write(session, plan, segment_id, 1)?);
    }

    upload_until_settled(session, run, 100)?;
    expect_all_deleted(session, run);
    let read_back = session.read_back_available(&entries)?;
    run.note_read_back("remote", read_back);
    run.remote_verify = Some(session.verify_remote(&fully_uploaded_epochs(session))?);
    Ok(())
}

/// Power loss with a torn completion on the final append: the unsynced tail
/// vanishes and recovery may only surface the valid prefix.
fn power_cut(
    session: &mut Session,
    plan: &Plan,
    run: &mut RunState,
) -> Result<(), HarnessError> {
    let stop_after = plan.stop_first_segment_after;
    run.expect(stop_after > 0, "power-cut plan stops mid-append");
    let entries = entries_for(session, plan, 0);
    let outcome = session.write_segment(&WriteSpec {
        segment_id: 0,
        epoch: 1,
        entries,
        arms: plan.disk_faults.clone(),
        stop_after,
        seal: true,
    })?;
    session.step("torn-append");
    run.expect(
        matches!(
            outcome.end,
            WriteEnd::Failed | WriteEnd::Stopped | WriteEnd::Open
        ),
        format!("unexpected write end after a torn append: {:?}", outcome.end),
    );

    session.crash_device()?;
    session.step("power-cut");
    let restart = session.restart()?;
    run.record_restart(&restart);
    session.step("repaired");

    // Every recovered record must be re-sealed and readable; the check that the
    // recovered set is a prefix of what was attempted lives in `check_invariants`.
    let mut entries = outcome.records.clone();
    for segment_id in 1..plan.work.segments {
        entries.extend(sealed_segment_write(session, plan, segment_id, 1)?);
    }

    upload_until_settled(session, run, 100)?;
    expect_all_deleted(session, run);
    let read_back = session.read_back_available(&entries)?;
    run.note_read_back("remote", read_back);
    run.remote_verify = Some(session.verify_remote(&fully_uploaded_epochs(session))?);
    Ok(())
}

/// Device faults (EIO/EAGAIN/ENOSPC/sync failure) during a sealed write. A failed
/// writer means the process must fail-stop; a restart recovers from the bytes on
/// disk and the run continues.
fn disk_faults(
    session: &mut Session,
    plan: &Plan,
    run: &mut RunState,
) -> Result<(), HarnessError> {
    run.expect(
        !plan.disk_faults.is_empty(),
        "disk-faults plan arms at least one fault",
    );
    let entries_spec = entries_for(session, plan, 0);
    // `FailAcquire`/`FailDirectorySync` are consumed by `SegmentWriter::create`,
    // before the first append exists to attach them to, so arms at append 0 of
    // those classes are injected directly on the device. The rest travel through
    // `WriteSpec::arms`, which arms each right before its append index.
    let (early, late): (Vec<FaultArm>, Vec<FaultArm>) = plan
        .disk_faults
        .iter()
        .cloned()
        .partition(|arm| {
            arm.at_append == 0
                && matches!(
                    arm.fault,
                    DiskFault::FailAcquire | DiskFault::FailDirectorySync
                )
        });
    run.expect(
        early.len() <= 1,
        format!("{} acquire-phase faults armed for a single create", early.len()),
    );
    for arm in &early {
        session.sim.inject_fault(to_simulated_fault(arm.fault));
    }
    let outcome = match session.write_segment(&WriteSpec {
        segment_id: 0,
        epoch: 1,
        entries: entries_spec,
        arms: late,
        stop_after: 0,
        seal: true,
    }) {
        Ok(outcome) => Some(outcome),
        Err(HarnessError::Writer(message)) => {
            // A fault on acquire/directory-sync stops the writer before the
            // segment exists. Only expected when the plan arms such a fault.
            run.expect(
                !early.is_empty(),
                format!("the write phase failed to start unexpectedly: {message}"),
            );
            run.notes.push(format!("write phase did not start: {message}"));
            None
        }
        Err(error) => return Err(error),
    };

    let mut entries = Vec::new();
    let mut failed = false;
    match &outcome {
        Some(outcome) => {
            entries.extend(outcome.records.iter().copied());
            match outcome.end {
                WriteEnd::Sealed { .. } => {
                    run.notes
                        .push("the armed fault was absorbed before acknowledgement".to_string());
                }
                WriteEnd::Failed => failed = true,
                other => run.expect(false, format!("unexpected write end {other:?}")),
            }
        }
        None => failed = true,
    }

    if failed {
        // A poisoned writer never retries; production fail-stops the process.
        // Model process death (no power cut): unsynced bytes stay readable.
        session.trace.record(TraceEvent::Crash {
            point: "writer-failed".to_string(),
        });
        session.step("writer-failed");
        let restart = session.restart()?;
        run.record_restart(&restart);
        if let Some(repair) = restart.repairs.first() {
            run.notes.push(format!(
                "recovered {} record(s) from the failed segment ({} unacknowledged)",
                repair.records,
                repair.unacked_recovered.len()
            ));
        }
    }

    for segment_id in 1..plan.work.segments {
        entries.extend(sealed_segment_write(session, plan, segment_id, 1)?);
    }

    upload_until_settled(session, run, 100)?;
    expect_all_deleted(session, run);
    let read_back = session.read_back_available(&entries)?;
    run.note_read_back("remote", read_back);
    run.remote_verify = Some(session.verify_remote(&fully_uploaded_epochs(session))?);
    Ok(())
}

/// A competing writer commits a newer manifest epoch for one segment. The
/// uploader must fail-stop without committing our epoch and without deleting the
/// local file.
fn fenced(session: &mut Session, plan: &Plan, run: &mut RunState) -> Result<(), HarnessError> {
    let entries = write_all_sealed(session, plan)?;
    let restart = session.restart()?;
    run.record_restart(&restart);

    let fence = plan.fence.expect("fenced plan carries a fence");
    let target = fence.segment_index;
    session.mock().fence_after_put(target, fence.epoch);

    let _ = drive_upload_life(session, run, None, 100)?;
    session.step("fenced");

    run.expect(
        session.uploader_poisoned(),
        "a fenced segment must poison the uploader",
    );
    let reason = session.uploader_error().unwrap_or_default();
    run.expect(
        reason.to_lowercase().contains("fenced"),
        format!("poison reason does not mention fencing: {reason}"),
    );
    run.expect(
        session
            .sim
            .visible_bytes(&session.segment_path(target))
            .is_some(),
        "the fenced local segment must stay on disk",
    );
    let unlinked = session.trace.events().iter().any(
        |event| matches!(event, TraceEvent::LocalUnlinked { segment_id, .. } if *segment_id == target),
    );
    run.expect(!unlinked, "the fenced segment must never be unlinked");
    let our_epoch = session.sealed_epochs.get(&target).copied().unwrap_or(0);
    let committed_ours = session
        .mock()
        .manifest_records()
        .iter()
        .any(|record| record.segment_id == target && record.writer_epoch == our_epoch);
    run.expect(
        !committed_ours,
        "the fenced segment committed our (older) epoch after the race",
    );
    if let Some(stats) = session.uploader_stats() {
        run.expect(stats.fenced == 1, format!("expected 1 fencing event, saw {}", stats.fenced));
    }

    // The fenced segment is still local, so it must be readable through the
    // fallback reader; every segment that made it remote is verified from S3.
    let target_entries: Vec<(u64, u64)> = entries
        .iter()
        .filter(|(_, entry)| entry / 1_000_000 == target)
        .copied()
        .collect();
    let read_back = session.read_back(&target_entries)?;
    run.note_read_back("fenced-local", read_back);
    run.remote_verify = Some(session.verify_remote(&fully_uploaded_epochs(session))?);
    Ok(())
}

/// S3 chaos: lost responses, CAS races, GET/HEAD failures, latency. The uploader
/// must still converge without lying about durability.
fn s3_chaos(session: &mut Session, plan: &Plan, run: &mut RunState) -> Result<(), HarnessError> {
    let entries = write_all_sealed(session, plan)?;
    let restart = session.restart()?;
    run.record_restart(&restart);

    upload_until_settled(session, run, 500)?;
    session.step("settled");
    expect_all_deleted(session, run);

    let read_back = session.read_back(&entries)?;
    run.note_read_back("remote", read_back);
    run.remote_verify = Some(session.verify_remote(&fully_uploaded_epochs(session))?);
    Ok(())
}

/// A transient S3 outage window. Every operation in the window fails without
/// accepting bytes; the retry loop must ride it out and the local bytes must stay
/// intact meanwhile.
fn outage(session: &mut Session, plan: &Plan, run: &mut RunState) -> Result<(), HarnessError> {
    let entries = write_all_sealed(session, plan)?;
    let restart = session.restart()?;
    run.record_restart(&restart);

    let outage = plan.outage.expect("outage plan carries a window");
    session
        .mock()
        .begin_outage_after(outage.after_ops, outage.ops);

    upload_until_settled(session, run, 500)?;
    session.step("settled");

    let transient = session
        .mock()
        .events()
        .iter()
        .any(|event| event.outcome == crate::mock_s3::S3Outcome::Transient);
    run.expect(transient, "the outage window never fired");
    expect_all_deleted(session, run);

    let read_back = session.read_back(&entries)?;
    run.note_read_back("remote", read_back);
    run.remote_verify = Some(session.verify_remote(&fully_uploaded_epochs(session))?);
    Ok(())
}

/// Slow device syncs with otherwise clean uploads.
fn slow_sync(session: &mut Session, plan: &Plan, run: &mut RunState) -> Result<(), HarnessError> {
    // The slow syncs are armed on the first segment's appends.
    let mut entries = sealed_segment_write_with_arms(session, plan, 0, 1, &plan.disk_faults)?;
    run.expect(
        session.io_counters().sync_delay_micros > 0,
        "the slow-sync plan never actually delayed a sync",
    );
    for segment_id in 1..plan.work.segments {
        entries.extend(sealed_segment_write(session, plan, segment_id, 1)?);
    }
    session.step("write-complete");
    let restart = session.restart()?;
    run.record_restart(&restart);

    upload_until_settled(session, run, 100)?;
    expect_all_deleted(session, run);

    let read_back = session.read_back(&entries)?;
    run.note_read_back("remote", read_back);
    run.remote_verify = Some(session.verify_remote(&fully_uploaded_epochs(session))?);
    Ok(())
}

// ===== shared driver pieces =====

/// Write every segment sealed; returns the accepted `(ledger, entry)` pairs.
/// Public because the real-S3 driver (`real_s3::run_real_s3`) runs the same
/// write phase before its settle loop.
pub fn write_all_sealed(
    session: &mut Session,
    plan: &Plan,
) -> Result<Vec<(u64, u64)>, HarnessError> {
    let mut entries = Vec::new();
    for segment_id in 0..plan.work.segments {
        entries.extend(sealed_segment_write(session, plan, segment_id, 1)?);
    }
    session.step("write-complete");
    Ok(entries)
}

/// Deterministic entries of one segment; payloads are registered for read-back.
fn entries_for(session: &mut Session, plan: &Plan, segment_id: u64) -> Vec<AppendSpec> {
    let mut specs = Vec::with_capacity(plan.work.entries_per_segment as usize);
    let ledgers = plan.work.ledgers.max(1);
    for index in 0..plan.work.entries_per_segment {
        let ledger = index % ledgers + 1;
        let entry = segment_id * 1_000_000 + index;
        let payload = Session::entry_payload(plan.seed, ledger, entry, plan.work.payload_bytes);
        session.remember_payload(ledger, entry, &payload);
        specs.push(AppendSpec {
            ledger,
            entry,
            flags: 0,
            payload,
        });
    }
    specs
}

/// Write one segment with a clean seal; a non-sealed end is a harness bug.
fn sealed_segment_write(
    session: &mut Session,
    plan: &Plan,
    segment_id: u64,
    epoch: u64,
) -> Result<Vec<(u64, u64)>, HarnessError> {
    sealed_segment_write_with_arms(session, plan, segment_id, epoch, &[])
}

/// Like [`sealed_segment_write`], but arms disk faults at the given appends.
fn sealed_segment_write_with_arms(
    session: &mut Session,
    plan: &Plan,
    segment_id: u64,
    epoch: u64,
    arms: &[crate::plan::FaultArm],
) -> Result<Vec<(u64, u64)>, HarnessError> {
    let entries = entries_for(session, plan, segment_id);
    let outcome = session.write_segment(&WriteSpec {
        segment_id,
        epoch,
        entries,
        arms: arms.to_vec(),
        stop_after: 0,
        seal: true,
    })?;
    match outcome.end {
        WriteEnd::Sealed { .. } => Ok(outcome.records),
        other => Err(HarnessError::Message(format!(
            "segment {segment_id}: expected a sealed write, got {other:?} ({:?})",
            outcome.error
        ))),
    }
}

/// Drive upload passes until nothing is left, an armed crash point fires, the
/// uploader poisons itself, or the pass budget runs out. Returns whether a crash
/// point fired.
fn drive_upload_life(
    session: &mut Session,
    run: &mut RunState,
    crash: Option<CrashPlan>,
    max_passes: u32,
) -> Result<bool, HarnessError> {
    let mut crashed = false;
    let mut stall_noted = false;
    for pass in 0..max_passes {
        match session.upload_pass() {
            Ok(report) => {
                run.record_upload_pass(&report);
                if crash_fired(session, crash) {
                    crashed = true;
                    break;
                }
                if report.examined == 0 {
                    return Ok(crashed);
                }
                if report.pending > 0 {
                    // Transient chaos (an outage window, a lost response, a CAS
                    // race) leaves segments pending for a pass or two; only the
                    // budget ends the settle loop.
                    if !stall_noted {
                        run.notes.push(format!(
                            "{} segment(s) still pending after a pass; retrying",
                            report.pending
                        ));
                        stall_noted = true;
                    }
                    if pass + 1 == max_passes {
                        run.expect(
                            false,
                            format!(
                                "upload pass budget exhausted with {} segment(s) pending",
                                report.pending
                            ),
                        );
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Err((report, error)) => {
                run.record_upload_pass(&report);
                session.note_upload_error(&error);
                run.notes.push(format!("upload pass error: {error}"));
                if crash_fired(session, crash) {
                    crashed = true;
                    break;
                }
                if session.uploader_poisoned() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
                if pass + 1 == max_passes {
                    run.expect(false, "upload pass budget exhausted");
                }
            }
        }
    }
    Ok(crashed)
}

fn crash_fired(session: &Session, crash: Option<CrashPlan>) -> bool {
    let Some(crash) = crash else {
        return false;
    };
    let puts = crash
        .after_successful_puts
        .is_some_and(|index| session.mock().successful_puts() >= index);
    let cas = crash
        .after_successful_cas
        .is_some_and(|index| session.mock().successful_cas() >= index);
    puts || cas
}

fn upload_until_settled(
    session: &mut Session,
    run: &mut RunState,
    max_passes: u32,
) -> Result<(), HarnessError> {
    let crashed = drive_upload_life(session, run, None, max_passes)?;
    run.expect(!crashed, "upload life crashed although no crash point was armed");
    run.expect(
        !session.uploader_poisoned(),
        format!(
            "uploader poisoned: {}",
            session.uploader_error().unwrap_or_default()
        ),
    );
    Ok(())
}

fn recover_uploader(session: &mut Session, run: &mut RunState) -> Result<(), HarnessError> {
    match session.uploader_recover() {
        Ok(recovery) => {
            run.notes.push(format!(
                "recovery: scanned={} pending={} reconciled_uploaded={} reconciled_deleted={}",
                recovery.scanned,
                recovery.pending_uploads,
                recovery.reconciled_uploaded,
                recovery.reconciled_deleted
            ));
            Ok(())
        }
        Err(error) => {
            session.note_upload_error(&error);
            Err(error)
        }
    }
}

fn expect_all_deleted(session: &Session, run: &mut RunState) {
    let remaining = session.sim.published_paths();
    run.expect(
        remaining.is_empty(),
        format!(
            "{} segment file(s) still local after settling: {remaining:?}",
            remaining.len()
        ),
    );
}

/// `(segment_id, epoch)` pairs for every segment whose local file is gone - the
/// ones [`Session::verify_remote`] can hold the object store to.
fn fully_uploaded_epochs(session: &Session) -> Vec<(u64, u64)> {
    session
        .sealed_epochs
        .iter()
        .filter(|(segment_id, _)| {
            session
                .sim
                .visible_bytes(&session.segment_path(**segment_id))
                .is_none()
        })
        .map(|(segment_id, epoch)| (*segment_id, *epoch))
        .collect()
}

// ===== report =====

/// Accumulated assertions and observations of one run.
#[derive(Debug, Default)]
struct RunState {
    notes: Vec<String>,
    violations: Vec<String>,
    restarts: usize,
    repairs: Vec<RepairSummary>,
    uploads: Vec<UploadPassSummary>,
    read_backs: Vec<ReadBackSummary>,
    remote_verify: Option<RemoteVerifyReport>,
}

impl RunState {
    fn expect(&mut self, condition: bool, message: impl Into<String>) {
        if !condition {
            self.violations.push(message.into());
        }
    }

    fn record_restart(&mut self, restart: &RestartReport) {
        self.restarts += 1;
        self.repairs
            .extend(restart.repairs.iter().map(RepairSummary::from));
    }

    fn record_upload_pass(&mut self, report: &bkxpro_uploader::UploadReport) {
        self.uploads.push(UploadPassSummary {
            examined: report.examined,
            uploaded: report.uploaded,
            already_remote: report.already_remote,
            already_deleted: report.already_deleted,
            pending: report.pending,
        });
    }

    fn note_read_back(&mut self, source: &str, report: ReadBackReport) {
        self.read_backs.push(ReadBackSummary {
            source: source.to_string(),
            report,
        });
    }
}

/// Serializable summary of one open-segment repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairSummary {
    pub segment_id: u64,
    pub epoch: u64,
    pub records: u64,
    pub bytes_before: usize,
    pub bytes_after: usize,
    pub unacked_recovered: usize,
}

impl From<&RepairReport> for RepairSummary {
    fn from(report: &RepairReport) -> Self {
        Self {
            segment_id: report.segment_id,
            epoch: report.epoch,
            records: report.records,
            bytes_before: report.bytes_before,
            bytes_after: report.bytes_after,
            unacked_recovered: report.unacked_recovered.len(),
        }
    }
}

/// Serializable summary of one upload pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadPassSummary {
    pub examined: usize,
    pub uploaded: usize,
    pub already_remote: usize,
    pub already_deleted: usize,
    pub pending: usize,
}

/// One read-back with the source phase it ran in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadBackSummary {
    pub source: String,
    pub report: ReadBackReport,
}

/// Serializable snapshot of the uploader counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploaderStatsSummary {
    pub segments_uploaded: u64,
    pub segments_deleted: u64,
    pub bytes_uploaded: u64,
    pub put_attempts: u64,
    pub put_retries: u64,
    pub put_failures: u64,
    pub manifest_commits: u64,
    pub cas_conflicts: u64,
    pub already_committed: u64,
    pub recovered_deletes: u64,
    pub fenced: u64,
    pub missing_local: u64,
}

impl From<bkxpro_uploader::UploaderStats> for UploaderStatsSummary {
    fn from(stats: bkxpro_uploader::UploaderStats) -> Self {
        Self {
            segments_uploaded: stats.segments_uploaded,
            segments_deleted: stats.segments_deleted,
            bytes_uploaded: stats.bytes_uploaded,
            put_attempts: stats.put_attempts,
            put_retries: stats.put_retries,
            put_failures: stats.put_failures,
            manifest_commits: stats.manifest_commits,
            cas_conflicts: stats.cas_conflicts,
            already_committed: stats.already_committed,
            recovered_deletes: stats.recovered_deletes,
            fenced: stats.fenced,
            missing_local: stats.missing_local,
        }
    }
}

/// The complete, serializable result of one scenario run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub scenario: String,
    pub seed: u64,
    pub kind: ScenarioKind,
    pub plan_fingerprint: u64,
    /// True when every invariant and scenario assertion held.
    pub ok: bool,
    pub violations: Vec<String>,
    pub notes: Vec<String>,
    pub steps: Vec<String>,
    pub restarts: usize,
    pub repairs: Vec<RepairSummary>,
    pub uploads: Vec<UploadPassSummary>,
    pub read_backs: Vec<ReadBackSummary>,
    pub remote_verify: RemoteVerifyReport,
    pub uploader_poisoned: bool,
    pub uploader_stats: Option<UploaderStatsSummary>,
    pub acked_entries: usize,
    pub trace_events: usize,
    pub mock_events: usize,
    pub trace_digest: u64,
    pub trace_content_digest: u64,
    pub mock_event_digest: u64,
    pub mock_content_digest: u64,
    pub replay: String,
    pub duration_ms: u64,
}

fn build_report(
    plan: &Plan,
    session: &Session,
    run: RunState,
    started: Instant,
) -> RunReport {
    let violations = run.violations;
    RunReport {
        scenario: plan.name.clone(),
        seed: plan.seed,
        kind: plan.kind,
        plan_fingerprint: plan.fingerprint(),
        ok: violations.is_empty(),
        violations,
        notes: run.notes,
        steps: session.steps.clone(),
        restarts: run.restarts,
        repairs: run.repairs,
        uploads: run.uploads,
        read_backs: run.read_backs,
        remote_verify: run.remote_verify.unwrap_or_default(),
        uploader_poisoned: session.uploader_poisoned(),
        uploader_stats: session.uploader_stats().map(Into::into),
        acked_entries: session.acked.len(),
        trace_events: session.trace.len(),
        mock_events: session.mock_opt().map_or(0, |mock| mock.events().len()),
        trace_digest: session.trace.digest(),
        trace_content_digest: session.trace.content_digest(),
        mock_event_digest: session.mock_opt().map_or(0, |mock| mock.event_digest()),
        mock_content_digest: session.mock_opt().map_or(0, |mock| mock.content_digest()),
        replay: plan.replay_command(),
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_life_records_partial_progress_from_a_failed_pass() {
        let mut plan = crate::plan::preset("smoke", 5).unwrap();
        plan.work.segments = 2;
        let mut session = Session::new(&plan).unwrap();
        write_all_sealed(&mut session, &plan).unwrap();
        session.restart().unwrap();
        session.mock().fence_after_put(1, 99);
        let mut run = RunState::default();
        assert!(!drive_upload_life(&mut session, &mut run, None, 1).unwrap());
        assert_eq!(run.uploads.len(), 1);
        assert_eq!(run.uploads[0].examined, 2);
        assert_eq!(run.uploads[0].uploaded, 1);
        assert_eq!(run.uploads[0].pending, 1);
        assert!(run.notes.iter().any(|note| note.contains("fenced")));
        assert!(session.uploader_poisoned());
    }
}
