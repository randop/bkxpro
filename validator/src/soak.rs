//! Soak driver: the long-running CI/nightly loop (RAN-132).
//!
//! A soak is the scenario matrix plus wall-clock: every iteration picks a preset
//! and a seed derived from the run index, runs it against the deterministic
//! simulator, appends the report to a JSONL log, samples process/cgroup
//! resources, and periodically interleaves a real-process kill round. The
//! multi-day procedure in `validator/README.md` is this same loop with a
//! large duration and a persistent log; `soak --resume` continues from the last
//! logged run so a restart does not repeat seeds.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::PathBuf,
    time::{Duration, Instant},
};

use serde::Serialize;

use crate::{
    kill_soak::{run_kill_soak, KillRound, KillSoakConfig},
    lifecycle::HarnessError,
    mem::{dir_usage_bytes, ResourceMonitor},
    plan::{preset, Plan, PRESETS},
    report::{append_run_log, logged_run_count, write_harness_incident, write_incident},
    rng::DetRng,
    scenarios::{run_plan, RunReport},
};

/// Everything the soak loop needs.
#[derive(Debug, Clone)]
pub struct SoakConfig {
    pub base_seed: u64,
    /// Wall-clock budget for the whole soak.
    pub duration: Duration,
    /// Preset names to rotate through (defaults to the full matrix).
    pub scenarios: Vec<String>,
    /// Stop after this many runs in *this* invocation (`None` = duration only).
    pub max_runs: Option<u64>,
    /// JSONL run log; also the resume point when `resume` is set.
    pub log: Option<PathBuf>,
    /// Directory for incident folders of failing runs.
    pub incidents: Option<PathBuf>,
    /// Continue the seed sequence from the logged run count.
    pub resume: bool,
    pub rss_limit_bytes: Option<u64>,
    pub fd_limit: Option<usize>,
    /// Directory whose disk usage is tracked for leaks (typically the validator
    /// work root); `None` disables the check.
    pub disk_root: Option<PathBuf>,
    pub dir_limit_bytes: Option<u64>,
    /// How often `disk_root` is measured.
    pub sample_interval: Duration,
    /// Interleave one real-process kill round every `kill_every` scenario runs
    /// (`0` disables; requires `kill_soak` to be set).
    pub kill_every: u64,
    pub kill_soak: Option<KillSoakConfig>,
}

impl SoakConfig {
    pub fn new(base_seed: u64, duration: Duration) -> Self {
        Self {
            base_seed,
            duration,
            scenarios: PRESETS.iter().map(|name| name.to_string()).collect(),
            max_runs: None,
            log: None,
            incidents: None,
            resume: false,
            rss_limit_bytes: None,
            fd_limit: None,
            disk_root: None,
            dir_limit_bytes: None,
            sample_interval: Duration::from_secs(10),
            kill_every: 0,
            kill_soak: None,
        }
    }
}

/// The aggregated soak result, printed by the CLI and appended by CI.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SoakReport {
    pub base_seed: u64,
    pub runs: u64,
    pub failures: u64,
    pub scenario_runs: BTreeMap<String, u64>,
    pub kill_rounds: u64,
    pub kill_violations: Vec<String>,
    pub violations: Vec<String>,
    pub peak_rss_bytes: u64,
    pub peak_cgroup_bytes: u64,
    pub peak_open_fds: usize,
    pub peak_dir_bytes: u64,
    pub dir_bytes_last: u64,
    pub incidents: Vec<PathBuf>,
    pub duration_ms: u64,
}

impl SoakReport {
    pub fn ok(&self) -> bool {
        self.violations.is_empty() && self.kill_violations.is_empty()
    }

    /// Stable one-line summary for a CI log.
    pub fn summary(&self) -> String {
        format!(
            "soak: {} runs, {} failures, {} kill rounds, peak RSS {} MiB, peak fds {}, {} incidents, {} ms",
            self.runs,
            self.failures,
            self.kill_rounds,
            self.peak_rss_bytes / (1024 * 1024),
            self.peak_open_fds,
            self.incidents.len(),
            self.duration_ms,
        )
    }
}

/// Run the soak. `on_scenario` and `on_kill` are called after every run/round for
/// progress printing; return value aggregation happens regardless.
pub fn run_soak(
    config: &SoakConfig,
    on_scenario: impl FnMut(&RunReport),
    on_kill: impl FnMut(&KillRound),
) -> io::Result<SoakReport> {
    run_soak_with(config, on_scenario, on_kill, run_plan)
}

fn run_soak_with(
    config: &SoakConfig,
    mut on_scenario: impl FnMut(&RunReport),
    mut on_kill: impl FnMut(&KillRound),
    mut run_plan: impl FnMut(&Plan) -> Result<RunReport, HarnessError>,
) -> io::Result<SoakReport> {
    let started = Instant::now();
    let mut monitor = ResourceMonitor::new(config.rss_limit_bytes, config.fd_limit);
    let mut report = SoakReport {
        base_seed: config.base_seed,
        ..SoakReport::default()
    };
    if config.scenarios.is_empty() {
        return Err(io::Error::other("soak has no scenarios to run"));
    }

    let mut run_index = if config.resume {
        config
            .log
            .as_deref()
            .map(logged_run_count)
            .transpose()?
            .unwrap_or(0)
    } else {
        0
    };
    let mut next_disk_sample = Instant::now();

    loop {
        if started.elapsed() >= config.duration {
            break;
        }
        // `max_runs` is the target *total* run count: with `--resume`, a log
        // holding 3 runs and `--runs 5` performs 2 more.
        if config.max_runs.is_some_and(|max| run_index >= max) {
            break;
        }

        let seed = DetRng::new(config.base_seed)
            .derive_from(&[b"soak-run", &run_index.to_le_bytes()])
            .next_u64();
        let scenario_name =
            config.scenarios[(run_index as usize) % config.scenarios.len()].clone();
        let plan = match preset(&scenario_name, seed) {
            Some(plan) => plan,
            None => {
                return Err(io::Error::other(format!(
                    "unknown scenario {scenario_name:?}; known: {}",
                    PRESETS.join(", ")
                )))
            }
        };

        let run_started = Instant::now();
        match run_plan(&plan) {
            Ok(run_report) => {
                if !run_report.ok {
                    report.failures += 1;
                    for violation in &run_report.violations {
                        report.violations.push(format!(
                            "run {run_index} ({} seed {seed}): {violation}",
                            run_report.scenario
                        ));
                    }
                    if let Some(incidents) = &config.incidents {
                        report.incidents.push(write_incident(incidents, &run_report)?);
                    }
                }
                if let Some(log) = &config.log {
                    append_run_log(log, &run_report)?;
                }
                on_scenario(&run_report);
            }
            Err(error) => {
                report.failures += 1;
                report.violations.push(format!(
                    "run {run_index} ({scenario_name} seed {seed}): harness error: {error}"
                ));
                if let Some(incidents) = &config.incidents {
                    report.incidents.push(write_harness_incident(
                        incidents,
                        &scenario_name,
                        seed,
                        &error.to_string(),
                    )?);
                }
                let run_report = RunReport {
                    scenario: plan.name.clone(),
                    seed,
                    kind: plan.kind,
                    plan_fingerprint: plan.fingerprint(),
                    ok: false,
                    violations: vec![format!("harness error: {error}")],
                    notes: vec![],
                    steps: vec![],
                    restarts: 0,
                    repairs: vec![],
                    uploads: vec![],
                    read_backs: vec![],
                    remote_verify: Default::default(),
                    uploader_poisoned: false,
                    uploader_stats: None,
                    acked_entries: 0,
                    trace_events: 0,
                    mock_events: 0,
                    trace_digest: 0,
                    trace_content_digest: 0,
                    mock_event_digest: 0,
                    mock_content_digest: 0,
                    replay: plan.replay_command(),
                    duration_ms: run_started.elapsed().as_millis() as u64,
                };
                if let Some(log) = &config.log {
                    append_run_log(log, &run_report)?;
                }
                on_scenario(&run_report);
            }
        }
        *report
            .scenario_runs
            .entry(scenario_name.clone())
            .or_default() += 1;
        report.runs += 1;
        run_index += 1;

        if let Some(kill_config) = &config.kill_soak {
            if config.kill_every > 0 && report.runs % config.kill_every == 0 {
                let mut one_round = kill_config.clone();
                one_round.rounds = 1;
                one_round.base_seed = DetRng::new(config.base_seed)
                    .derive_from(&[b"soak-kill", &report.kill_rounds.to_le_bytes()])
                    .next_u64();
                let base_round = report.kill_rounds;
                // One soak slot drives exactly one child round, so the child's
                // per-invocation `round` index is always 0: relabel it with the
                // soak-wide round number before the progress callback sees it.
                let outcome = run_kill_soak(&one_round, |_| {})?;
                report.kill_rounds += outcome.rounds.len() as u64;
                for (offset, round) in outcome.rounds.iter().enumerate() {
                    let mut labeled = round.clone();
                    labeled.round = (base_round + offset as u64) as u32;
                    on_kill(&labeled);
                    report.kill_violations.extend(round.violations.iter().cloned());
                }
            }
        }

        monitor.observe();
        if Instant::now() >= next_disk_sample {
            next_disk_sample = Instant::now() + config.sample_interval;
            if let Some(root) = &config.disk_root {
                if let Ok(bytes) = dir_usage_bytes(root) {
                    report.peak_dir_bytes = report.peak_dir_bytes.max(bytes);
                    report.dir_bytes_last = bytes;
                    if let Some(limit) = config.dir_limit_bytes {
                        if bytes > limit {
                            report.violations.push(format!(
                                "disk usage of {} grew to {bytes} bytes, beyond the {limit} byte budget",
                                root.display()
                            ));
                        }
                    }
                }
            }
        }
    }

    report.peak_rss_bytes = monitor.peak_rss_bytes;
    report.peak_cgroup_bytes = monitor.peak_cgroup_bytes;
    report.peak_open_fds = monitor.peak_open_fds;
    let mut resource_violations = BTreeSet::new();
    for violation in &monitor.violations {
        resource_violations.insert(violation.clone());
    }
    report.violations.extend(resource_violations);
    report.duration_ms = started.elapsed().as_millis() as u64;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::ScenarioKind;

    /// A tiny smoke-only soak; the full matrix lives in the nightly CI job.
    #[test]
    fn a_short_soak_runs_the_seeded_sequence_and_logs_it() {
        let dir = tempfile::TempDir::new().unwrap();
        let log = dir.path().join("run/log.jsonl");
        let mut config = SoakConfig::new(11, Duration::from_secs(30));
        config.scenarios = vec!["smoke".to_string()];
        config.max_runs = Some(3);
        config.log = Some(log.clone());
        config.resume = false;

        let report = run_soak(&config, |_| {}, |_| {}).unwrap();
        assert_eq!(report.runs, 3);
        assert_eq!(report.failures, 0);
        assert!(report.ok(), "{:?}", report.violations);
        assert_eq!(report.scenario_runs.get("smoke"), Some(&3));
        assert_eq!(logged_run_count(&log).unwrap(), 3);

        // A resumed soak continues the derived seed sequence instead of repeating.
        let first_runs: Vec<u64> = std::fs::read_to_string(&log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<RunReport>(line).unwrap().seed)
            .collect();
        let mut resumed = config.clone();
        resumed.max_runs = Some(5);
        resumed.resume = true;
        let report = run_soak(&resumed, |_| {}, |_| {}).unwrap();
        assert_eq!(report.runs, 2);
        let all_runs: Vec<u64> = std::fs::read_to_string(&log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<RunReport>(line).unwrap().seed)
            .collect();
        assert_eq!(all_runs.len(), 5);
        assert_eq!(&all_runs[..3], &first_runs[..]);
        assert_ne!(all_runs[2], all_runs[3]);
        assert_eq!(
            resumed.scenarios,
            vec!["smoke".to_string()],
            "sanity: scenario list unchanged"
        );
    }

    #[test]
    fn harness_failures_are_logged_reported_and_advance_resume() {
        for with_incidents in [false, true] {
            let dir = tempfile::TempDir::new().unwrap();
            let log = dir.path().join("runs.jsonl");
            let mut config = SoakConfig::new(11, Duration::from_secs(30));
            config.scenarios = vec!["smoke".to_string()];
            config.max_runs = Some(1);
            config.log = Some(log.clone());
            config.incidents = with_incidents.then(|| dir.path().join("incidents"));
            let mut observed = Vec::new();
            let report = run_soak_with(
                &config,
                |run| observed.push(run.clone()),
                |_| {},
                |_| Err(HarnessError::Message("injected failure".into())),
            )
            .unwrap();
            assert_eq!(report.runs, 1);
            assert_eq!(report.failures, 1);
            assert!(!report.ok());
            assert_eq!(report.scenario_runs.get("smoke"), Some(&1));
            assert_eq!(observed.len(), 1);
            assert!(!observed[0].ok);
            assert_eq!(
                observed[0].violations,
                vec!["harness error: injected failure"]
            );
            assert_eq!(logged_run_count(&log).unwrap(), 1);
            let logged: RunReport =
                serde_json::from_str(std::fs::read_to_string(&log).unwrap().trim()).unwrap();
            let plan = preset("smoke", logged.seed).unwrap();
            assert_eq!(logged.seed, observed[0].seed);
            assert_eq!(logged.plan_fingerprint, plan.fingerprint());
            assert_eq!(logged.replay, plan.replay_command());
            assert_eq!(logged.violations, observed[0].violations);
            assert_eq!(report.incidents.len(), usize::from(with_incidents));
            if with_incidents {
                let repro = std::fs::read_to_string(report.incidents[0].join("repro.md")).unwrap();
                assert!(repro.contains("injected failure"));
                assert!(repro.contains(&format!("seed: {}", logged.seed)));
            }

            config.resume = true;
            config.max_runs = Some(2);
            let mut resumed_seeds = Vec::new();
            let resumed = run_soak(&config, |run| resumed_seeds.push(run.seed), |_| {}).unwrap();
            assert_eq!(resumed.runs, 1);
            assert_eq!(resumed.failures, 0);
            assert_eq!(logged_run_count(&log).unwrap(), 2);
            let expected = DetRng::new(config.base_seed)
                .derive_from(&[b"soak-run", &1u64.to_le_bytes()])
                .next_u64();
            assert_eq!(resumed_seeds, vec![expected]);
            assert_ne!(expected, logged.seed);
        }
    }

    #[test]
    fn an_empty_matrix_is_rejected() {
        let mut config = SoakConfig::new(1, Duration::from_secs(1));
        config.scenarios.clear();
        let error = run_soak(&config, |_| {}, |_| {}).unwrap_err();
        assert!(error.to_string().contains("no scenarios"));
    }

    #[test]
    fn unknown_scenarios_are_rejected() {
        let mut config = SoakConfig::new(1, Duration::from_secs(1));
        config.scenarios = vec!["not-a-scenario".to_string()];
        let error = run_soak(&config, |_| {}, |_| {}).unwrap_err();
        assert!(error.to_string().contains("unknown scenario"));
    }

    #[test]
    fn scenario_kind_round_trips_for_the_matrix() {
        for name in &SoakConfig::new(1, Duration::from_secs(1)).scenarios {
            assert!(ScenarioKind::parse(name).is_some(), "{name}");
        }
    }
}
