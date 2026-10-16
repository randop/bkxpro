//! Seeded reproducibility: the same `(scenario, seed)` must produce the same
//! trace content and the same mock-S3 content, and replay commands must encode
//! exactly what produced them.

use bkxpro_validator::{preset, run_plan, PRESETS};

const DETERMINISTIC_PRESETS: &[&str] = &[
    "smoke",
    "crash-upload",
    "crash-lifecycle",
    "power-cut",
    "disk-faults",
    "fenced",
    "s3-chaos",
    "outage",
    "slow-sync",
];

#[test]
fn same_seed_reproduces_the_content_digests() {
    for name in DETERMINISTIC_PRESETS {
        let first = run_plan(&preset(name, 11).unwrap()).unwrap();
        let second = run_plan(&preset(name, 11).unwrap()).unwrap();
        assert_eq!(
            first.trace_content_digest, second.trace_content_digest,
            "{name}: trace content digests differ for the same seed"
        );
        assert_eq!(
            first.mock_content_digest, second.mock_content_digest,
            "{name}: mock-S3 content digests differ for the same seed"
        );
        assert_eq!(
            first.plan_fingerprint, second.plan_fingerprint,
            "{name}: plan fingerprints differ for the same seed"
        );
        assert_eq!(first.acked_entries, second.acked_entries, "{name}");
        assert_eq!(first.repairs, second.repairs, "{name}");
    }
}

#[test]
fn different_seeds_explore_different_schedules() {
    let mut divergent = Vec::new();
    for name in PRESETS {
        let first = run_plan(&preset(name, 1).unwrap()).unwrap();
        let second = run_plan(&preset(name, 2).unwrap()).unwrap();
        if first.trace_content_digest != second.trace_content_digest {
            divergent.push(*name);
        }
    }
    assert!(
        divergent.len() >= 4,
        "only {} preset(s) diverge between seeds ({divergent:?}); nightly random \
         seeds would not explore anything",
        divergent.len()
    );
}

#[test]
fn replay_command_reproduces_the_plan_fingerprint() {
    let plan = preset("s3-chaos", 77).unwrap();
    let report = run_plan(&plan).unwrap();
    assert!(report.replay.contains("--scenario s3-chaos --seed 77"));
    assert_eq!(report.plan_fingerprint, plan.fingerprint());
}
