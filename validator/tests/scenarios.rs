//! End-to-end scenario runs: every preset must keep its invariants.

use bkxpro_validator::{preset, run_plan, RunReport};

fn run(name: &str, seed: u64) -> RunReport {
    let plan = preset(name, seed).expect("preset exists");
    let report = run_plan(&plan).expect("run completes");
    assert!(
        report.ok,
        "scenario {} seed {} failed\nviolations: {:#?}\nnotes: {:#?}",
        name, seed, report.violations, report.notes
    );
    report
}

#[test]
fn smoke_seed_1() {
    let report = run("smoke", 1);
    assert_eq!(report.restarts, 1);
    assert!(!report.uploader_poisoned);
    assert!(report.remote_verify.failures.is_empty());
    // Local read-back happened before the uploads, remote after.
    assert!(report.read_backs.iter().any(|b| b.source == "local"));
    assert!(report.read_backs.iter().any(|b| b.source == "remote"));
    let remote = report
        .read_backs
        .iter()
        .find(|b| b.source == "remote")
        .unwrap();
    assert!(remote.report.served_remote > 0);
    assert!(remote.report.checked > 0);
    // 3 segments uploaded and deleted.
    if let Some(stats) = &report.uploader_stats {
        assert_eq!(stats.segments_deleted, 3);
    }
}

#[test]
fn crash_upload_seed_1() {
    let report = run("crash-upload", 1);
    assert!(report.restarts >= 1);
}

#[test]
fn crash_lifecycle_seed_1() {
    let report = run("crash-lifecycle", 1);
    assert!(report.restarts >= 1);
    assert!(
        !report.repairs.is_empty(),
        "an open segment must be repaired"
    );
}

#[test]
fn power_cut_seed_1() {
    let report = run("power-cut", 1);
    assert!(report.restarts >= 1);
    assert!(
        !report.repairs.is_empty(),
        "the open segment must be repaired"
    );
}

#[test]
fn disk_faults_seed_1() {
    run("disk-faults", 1);
}

#[test]
fn fenced_seed_1() {
    let report = run("fenced", 1);
    assert!(report.uploader_poisoned);
}

#[test]
fn s3_chaos_seed_1() {
    let report = run("s3-chaos", 1);
    assert!(!report.uploader_poisoned);
}

#[test]
fn outage_seed_1() {
    run("outage", 1);
}

#[test]
fn slow_sync_seed_1() {
    run("slow-sync", 1);
}
