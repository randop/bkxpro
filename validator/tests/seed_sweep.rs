//! Seed sweeps. CI runs the fixed matrix; the nightly job runs this file with
//! `--ignored`, optionally supplying `BKXPRO_VALIDATOR_SEED_COUNT`.

use bkxpro_validator::{preset, run_plan, PRESETS};

fn sweep(presets: &[&str], seeds: &[u64]) -> Vec<String> {
    let mut failures = Vec::new();
    for name in presets {
        for seed in seeds {
            let plan = preset(name, *seed).expect("preset exists");
            match run_plan(&plan) {
                Ok(report) if report.ok => {}
                Ok(report) => failures.push(format!(
                    "{name} seed {seed}: {:?} (notes {:?})",
                    report.violations, report.notes
                )),
                Err(error) => failures.push(format!("{name} seed {seed}: run error: {error}")),
            }
        }
    }
    failures
}

#[test]
#[ignore = "nightly: full seed sweep"]
fn every_preset_survives_64_seeds() {
    let count: u64 = std::env::var("BKXPRO_VALIDATOR_SEED_COUNT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64);
    let seeds: Vec<u64> = (1..=count).collect();
    let failures = sweep(PRESETS, &seeds);
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
#[ignore = "nightly: deep chaos sweep on the two chaos presets"]
fn chaos_presets_survive_128_seeds() {
    let seeds: Vec<u64> = (1..=128).collect();
    let failures = sweep(&["s3-chaos", "outage"], &seeds);
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
