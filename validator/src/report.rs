//! Reporting: the JSONL run log, incident directories with a replay command, and
//! the replay comparison and dashboard aggregations.
//!
//! Every scenario run produces a serializable [`RunReport`]. CI appends one line
//! per run to a JSONL log; failing runs additionally get an incident directory
//! that an operator can replay with `bkxpro-validator replay --report report.json`.

use std::{
    fs,
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::scenarios::RunReport;

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error.to_string())
}

/// One JSON line for `report` (no trailing newline).
pub fn json_line(report: &RunReport) -> io::Result<String> {
    serde_json::to_string(report).map_err(other)
}

/// Append one JSON line for `report` to the run log, creating parent directories.
pub fn append_run_log(path: &Path, report: &RunReport) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut file, report).map_err(other)?;
    file.write_all(b"\n")?;
    Ok(())
}

/// The last seed recorded in a JSONL run log, for `soak --resume`.
pub fn last_logged_seed(path: &Path) -> io::Result<Option<u64>> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut last = None;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(report) = serde_json::from_str::<RunReport>(&line) {
            last = Some(report.seed);
        }
    }
    Ok(last)
}

/// The number of parseable run rows in a JSONL log, for `soak --resume` (a soak
/// derives each run's seed from its run index, so the count is the resume point).
pub fn logged_run_count(path: &Path) -> io::Result<u64> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut count = 0u64;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if serde_json::from_str::<RunReport>(&line).is_ok() {
            count += 1;
        }
    }
    Ok(count)
}

/// Write an incident directory for a failing run and return its path.
pub fn write_incident(root: &Path, report: &RunReport) -> io::Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let dir = root.join(format!(
        "{}-seed{}-{stamp:010}",
        report.scenario, report.seed
    ));
    fs::create_dir_all(&dir)?;
    let mut json = serde_json::to_vec_pretty(report).map_err(other)?;
    json.push(b'\n');
    fs::write(dir.join("report.json"), json)?;
    fs::write(dir.join("repro.md"), repro_markdown(report))?;
    Ok(dir)
}

/// Write an incident for a run that failed before producing a report (a harness
/// error rather than an invariant violation).
pub fn write_harness_incident(
    root: &Path,
    scenario: &str,
    seed: u64,
    message: &str,
) -> io::Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let dir = root.join(format!("{scenario}-seed{seed}-{stamp:010}"));
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join("repro.md"),
        format!(
            "# bkxpro validator harness error\n\nscenario: {scenario}\nseed: {seed}\n\n\
             ```\n{message}\n```\n\nReproduce:\n\n```\n{}\n```\n",
            replay_command(scenario, seed),
        ),
    )?;
    Ok(dir)
}

/// The copy-pasteable command that reproduces a run.
pub fn replay_command(scenario: &str, seed: u64) -> String {
    format!(
        "cargo run -p bkxpro-validator --bin bkxpro-validator -- sim --scenario {scenario} --seed {seed}"
    )
}

/// The incident runbook: what ran, how to reproduce it, and the digests a replay
/// must match.
pub fn repro_markdown(report: &RunReport) -> String {
    let mut out = String::new();
    out.push_str("# bkxpro validator incident\n\n");
    out.push_str(&format!(
        "scenario: {}\nseed: {}\nplan fingerprint: {:#018x}\nresult: {}\n\n",
        report.scenario,
        report.seed,
        report.plan_fingerprint,
        if report.ok { "PASS" } else { "FAIL" },
    ));
    out.push_str("## Reproduce\n\n```\n");
    out.push_str(&replay_command(&report.scenario, report.seed));
    out.push_str("\n```\n\nOr replay this exact report (from the incident directory):\n\n```\n");
    out.push_str(
        "cargo run -p bkxpro-validator --bin bkxpro-validator -- replay --report report.json",
    );
    out.push_str("\n```\n\n");
    out.push_str("## Digests (a replay must match)\n\n");
    out.push_str(&format!(
        "| digest | value |\n|---|---|\n\
         | trace (sequence) | `{:#018x}` |\n\
         | trace (content) | `{:#018x}` |\n\
         | mock S3 (sequence) | `{:#018x}` |\n\
         | mock S3 (content) | `{:#018x}` |\n\n",
        report.trace_digest,
        report.trace_content_digest,
        report.mock_event_digest,
        report.mock_content_digest,
    ));
    if !report.violations.is_empty() {
        out.push_str("## Violations\n\n");
        for violation in &report.violations {
            out.push_str(&format!("- {violation}\n"));
        }
        out.push('\n');
    }
    if !report.notes.is_empty() {
        out.push_str("## Notes\n\n");
        for note in &report.notes {
            out.push_str(&format!("- {note}\n"));
        }
        out.push('\n');
    }
    out.push_str("## Observed state\n\n");
    out.push_str(&format!(
        "- acknowledged entries: {}\n- restarts: {}\n- trace events: {}\n- mock S3 ops: {}\n\
         - uploader poisoned: {}\n- duration: {} ms\n",
        report.acked_entries,
        report.restarts,
        report.trace_events,
        report.mock_events,
        report.uploader_poisoned,
        report.duration_ms,
    ));
    out
}

/// Read a report written by [`write_incident`] (or any `RunReport` JSON).
pub fn read_report(path: &Path) -> Result<RunReport, String> {
    let text = fs::read_to_string(path).map_err(|error| format!("{path:?}: {error}"))?;
    serde_json::from_str(&text).map_err(|error| format!("{path:?}: {error}"))
}

/// Compare a replayed report against the recorded one. Returns every mismatch;
/// an empty list means the replay reproduced the incident exactly.
pub fn compare_reports(recorded: &RunReport, replayed: &RunReport) -> Vec<String> {
    let mut mismatches = Vec::new();
    let checks: [(&str, u64, u64); 3] = [
        (
            "plan fingerprint",
            recorded.plan_fingerprint,
            replayed.plan_fingerprint,
        ),
        (
            "trace content digest",
            recorded.trace_content_digest,
            replayed.trace_content_digest,
        ),
        (
            "mock S3 content digest",
            recorded.mock_content_digest,
            replayed.mock_content_digest,
        ),
    ];
    for (name, expected, actual) in checks {
        if expected != actual {
            mismatches.push(format!(
                "{name}: expected {expected:#018x}, replay produced {actual:#018x}"
            ));
        }
    }
    if recorded.acked_entries != replayed.acked_entries {
        mismatches.push(format!(
            "acknowledged entries: expected {}, replay produced {}",
            recorded.acked_entries, replayed.acked_entries
        ));
    }
    if recorded.ok != replayed.ok {
        mismatches.push(format!(
            "outcome: recorded {}, replay {}",
            recorded.ok, replayed.ok
        ));
    }
    mismatches
}

/// One aggregated scenario row of the dashboard.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct DashboardRow {
    pub scenario: String,
    pub runs: u64,
    pub failures: u64,
    pub first_failure_seed: Option<u64>,
    pub last_failure_seed: Option<u64>,
    pub last_ok: bool,
    pub total_duration_ms: u64,
    pub max_trace_events: usize,
    pub max_acked_entries: usize,
}

impl DashboardRow {
    pub fn pass_rate(&self) -> f64 {
        if self.runs == 0 {
            0.0
        } else {
            (self.runs - self.failures) as f64 / self.runs as f64
        }
    }
}

/// Pass-rate and coverage dashboard over a JSONL run log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dashboard {
    pub rows: Vec<DashboardRow>,
    pub skipped_lines: usize,
}

impl Dashboard {
    pub fn total_runs(&self) -> u64 {
        self.rows.iter().map(|row| row.runs).sum()
    }

    pub fn total_failures(&self) -> u64 {
        self.rows.iter().map(|row| row.failures).sum()
    }
}

/// Aggregate a JSONL run log. Unparsable lines are counted, not fatal, so a log
/// being appended to concurrently still renders.
pub fn dashboard(log: &Path) -> io::Result<Dashboard> {
    let file = fs::File::open(log)?;
    let mut dashboard = Dashboard::default();
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Ok(report) = serde_json::from_str::<RunReport>(&line) else {
            dashboard.skipped_lines += 1;
            continue;
        };
        let row = match dashboard
            .rows
            .iter_mut()
            .find(|row| row.scenario == report.scenario)
        {
            Some(row) => row,
            None => {
                dashboard.rows.push(DashboardRow {
                    scenario: report.scenario.clone(),
                    ..DashboardRow::default()
                });
                dashboard.rows.last_mut().expect("row just pushed")
            }
        };
        row.runs += 1;
        if !report.ok {
            row.failures += 1;
            row.first_failure_seed.get_or_insert(report.seed);
            row.last_failure_seed = Some(report.seed);
        }
        row.last_ok = report.ok;
        row.total_duration_ms = row.total_duration_ms.saturating_add(report.duration_ms);
        row.max_trace_events = row.max_trace_events.max(report.trace_events);
        row.max_acked_entries = row.max_acked_entries.max(report.acked_entries);
    }
    dashboard.rows.sort_by(|a, b| a.scenario.cmp(&b.scenario));
    Ok(dashboard)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::ScenarioKind;

    fn report(scenario: &str, seed: u64, ok: bool, content: u64) -> RunReport {
        RunReport {
            scenario: scenario.to_string(),
            seed,
            kind: ScenarioKind::Smoke,
            plan_fingerprint: 7,
            ok,
            violations: if ok { vec![] } else { vec!["boom".into()] },
            notes: vec![],
            steps: vec![],
            restarts: 0,
            repairs: vec![],
            uploads: vec![],
            read_backs: vec![],
            remote_verify: Default::default(),
            uploader_poisoned: false,
            uploader_stats: None,
            acked_entries: 3,
            trace_events: 9,
            mock_events: 4,
            trace_digest: 1,
            trace_content_digest: content,
            mock_event_digest: 2,
            mock_content_digest: 3,
            replay: String::new(),
            duration_ms: 5,
        }
    }

    #[test]
    fn run_log_round_trips_and_resumes() {
        let dir = tempfile::TempDir::new().unwrap();
        let log = dir.path().join("runs/log.jsonl");
        append_run_log(&log, &report("smoke", 1, true, 10)).unwrap();
        append_run_log(&log, &report("smoke", 2, false, 11)).unwrap();
        assert_eq!(last_logged_seed(&log).unwrap(), Some(2));
        assert_eq!(logged_run_count(&log).unwrap(), 2);
        assert_eq!(logged_run_count(&dir.path().join("missing.jsonl")).unwrap(), 0);
        let dashboard = dashboard(&log).unwrap();
        assert_eq!(dashboard.total_runs(), 2);
        assert_eq!(dashboard.total_failures(), 1);
        assert_eq!(dashboard.rows[0].first_failure_seed, Some(2));
        assert!((dashboard.rows[0].pass_rate() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn incidents_carry_repro_and_digests() {
        let dir = tempfile::TempDir::new().unwrap();
        let failing = report("s3-chaos", 42, false, 0xABCD);
        let incident = write_incident(dir.path(), &failing).unwrap();
        let reread = read_report(&incident.join("report.json")).unwrap();
        assert_eq!(reread.seed, 42);
        let repro = fs::read_to_string(incident.join("repro.md")).unwrap();
        assert!(repro.contains("sim --scenario s3-chaos --seed 42"));
        assert!(repro.contains("replay --report"));
        assert!(repro.contains(&format!("{:#018x}", 0xABCDu64)));
    }

    #[test]
    fn replay_comparison_flags_digest_drift() {
        let recorded = report("smoke", 3, false, 100);
        let same = report("smoke", 3, false, 100);
        assert!(compare_reports(&recorded, &same).is_empty());
        let drifted = report("smoke", 3, true, 101);
        let mismatches = compare_reports(&recorded, &drifted);
        assert_eq!(mismatches.len(), 2);
    }
}
