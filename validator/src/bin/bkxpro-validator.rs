//! `bkxpro-validator` command-line entry point (RAN-132).
//!
//! Every harness capability is reachable here:
//!
//! * `sim` - run one preset or the whole matrix at a seed, append JSONL runs,
//!   write incident directories, and double-run with `--replay` to prove the
//!   digests reproduce.
//! * `soak` - the long-running loop with resource budgets, resume, and
//!   interleaved real-process kill rounds.
//! * `kill-soak` - real `SIGKILL` rounds against a child writer.
//! * `replay --report` - reproduce an incident from a saved report and compare.
//! * `dashboard --log` - pass-rate aggregation over a run log.
//! * `kill-soak-child` - internal child mode spawned by `kill-soak`.
//!
//! See `validator/README.md` for the runbook.

#[cfg(not(target_os = "linux"))]
compile_error!("bkxpro-validator supports Linux only");

#[cfg(target_os = "linux")]
use std::{
    env,
    path::PathBuf,
    process::ExitCode,
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use bkxpro_validator::{
    fuzz::{run_fuzz, run_tail_fuzz, write_corpus, FuzzConfig},
    kill_soak::{child_main, read_child_meta, run_kill_soak, verify_session_dir},
    plan::{all_presets, preset, PRESETS},
    real_s3::{run_real_s3, RealS3Config},
    report::{
        append_run_log, compare_reports, dashboard, read_report, replay_command,
        write_harness_incident, write_incident,
    },
    scenarios::{run_plan, RunReport},
    soak::{run_soak, SoakConfig},
    ChildConfig, KillSoakConfig,
};

#[cfg(target_os = "linux")]
fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(error) => {
            eprintln!("bkxpro-validator: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(target_os = "linux")]
fn run() -> Result<bool, String> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage();
        return Err("missing subcommand".into());
    };
    let rest: Vec<String> = args.collect();

    match command.as_str() {
        "sim" => {
            let options = SimOptions::parse(rest)?;
            run_sim(&options)
        }
        "soak" => {
            let options = SoakOptions::parse(rest)?;
            run_soak_command(&options)
        }
        "kill-soak" => {
            let options = KillSoakOptions::parse(rest)?;
            run_kill_soak_command(&options)
        }
        "s3-run" => {
            let options = S3RunOptions::parse(rest)?;
            run_s3_run_command(&options)
        }
        "kill-soak-child" => {
            let config = parse_child_config(rest)?;
            child_main(&config).map_err(|error| format!("kill-soak child: {error}"))?;
            Ok(true)
        }
        "verify" => {
            let options = VerifyOptions::parse(rest)?;
            run_verify_command(&options)
        }
        "fuzz" => {
            let options = FuzzOptions::parse(rest)?;
            run_fuzz_command(&options)
        }
        "replay" => {
            let mut report = None;
            let mut json = false;
            let mut cursor = ArgCursor::new(rest);
            while let Some(arg) = cursor.next() {
                match arg.as_str() {
                    "--report" => report = Some(PathBuf::from(cursor.value("--report")?)),
                    "--json" => json = true,
                    other => return Err(format!("unknown replay argument {other:?}; see --help")),
                }
            }
            let report = report.ok_or("replay requires --report PATH")?;
            let recorded =
                read_report(&report).map_err(|error| format!("{}: {error}", report.display()))?;
            let plan = preset(&recorded.scenario, recorded.seed).ok_or_else(|| {
                format!(
                    "report names unknown scenario {:?}; known: {}",
                    recorded.scenario,
                    PRESETS.join(", ")
                )
            })?;
            let replayed =
                run_plan(&plan).map_err(|error| format!("replay run failed: {error}"))?;
            let mismatches = compare_reports(&recorded, &replayed);
            if json {
                let payload = serde_json::json!({
                    "recorded": recorded,
                    "replayed": replayed,
                    "mismatches": mismatches,
                });
                println!(
                    "{}",
                    serde_json::to_string_pretty(&payload).map_err(|error| error.to_string())?
                );
            } else if mismatches.is_empty() {
                println!(
                    "replay reproduced {} seed {} exactly ({} acknowledgements, digests match)",
                    replayed.scenario, replayed.seed, replayed.acked_entries
                );
            } else {
                println!(
                    "replay of {} seed {} diverged:",
                    recorded.scenario, recorded.seed
                );
                for mismatch in &mismatches {
                    println!("  mismatch: {mismatch}");
                }
                println!(
                    "  reproduce: {}",
                    replay_command(&recorded.scenario, recorded.seed)
                );
            }
            Ok(mismatches.is_empty())
        }
        "dashboard" => {
            let mut log = None;
            let mut json = false;
            let mut cursor = ArgCursor::new(rest);
            while let Some(arg) = cursor.next() {
                match arg.as_str() {
                    "--log" => log = Some(PathBuf::from(cursor.value("--log")?)),
                    "--json" => json = true,
                    other => return Err(format!("unknown dashboard argument {other:?}; see --help")),
                }
            }
            let log = log.ok_or("dashboard requires --log PATH")?;
            let dashboard = dashboard(&log).map_err(|error| format!("{}: {error}", log.display()))?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&dashboard.rows)
                        .map_err(|error| error.to_string())?
                );
            } else {
                println!(
                    "{:<16} {:>6} {:>8} {:>8} {:>12} {:>12}",
                    "scenario", "runs", "pass%", "last", "first-fail", "last-fail"
                );
                for row in &dashboard.rows {
                    println!(
                        "{:<16} {:>6} {:>7.1}% {:>8} {:>12} {:>12}",
                        row.scenario,
                        row.runs,
                        row.pass_rate() * 100.0,
                        if row.last_ok { "PASS" } else { "FAIL" },
                        row.first_failure_seed
                            .map(|seed| seed.to_string())
                            .unwrap_or_else(|| "-".into()),
                        row.last_failure_seed
                            .map(|seed| seed.to_string())
                            .unwrap_or_else(|| "-".into()),
                    );
                }
                println!(
                    "total: {} runs, {} failures, {} unparsable log lines",
                    dashboard.total_runs(),
                    dashboard.total_failures(),
                    dashboard.skipped_lines,
                );
            }
            Ok(true)
        }
        "--help" | "-h" | "help" => {
            print_usage();
            Ok(true)
        }
        other => Err(format!("unknown subcommand {other:?}; see --help")),
    }
}

#[cfg(target_os = "linux")]
/// Minimal cursor over the remaining arguments (space-separated values only).
struct ArgCursor {
    items: Vec<String>,
    index: usize,
}

impl ArgCursor {
    fn new(items: Vec<String>) -> Self {
        Self { items, index: 0 }
    }

    fn next(&mut self) -> Option<String> {
        let value = self.items.get(self.index).cloned();
        self.index += 1;
        value
    }

    fn value(&mut self, flag: &str) -> Result<String, String> {
        self.next()
            .ok_or_else(|| format!("{flag} requires a value"))
    }

    fn parse<T: std::str::FromStr>(&mut self, flag: &str) -> Result<T, String>
    where
        T::Err: std::fmt::Display,
    {
        let value = self.value(flag)?;
        value
            .parse()
            .map_err(|error| format!("invalid {flag} value {value:?}: {error}"))
    }
}

fn parse_mib(flag: &str, value: &str) -> Result<u64, String> {
    let mib: u64 = value
        .parse()
        .map_err(|error| format!("invalid {flag} value {value:?}: {error}"))?;
    mib.checked_mul(1024 * 1024)
        .ok_or_else(|| format!("{flag} overflows bytes"))
}

/// Default work root for logs, incidents, and kill-soak scratch.
fn default_root() -> PathBuf {
    if let Ok(path) = std::env::var("BKXPRO_VALIDATOR_DIR") {
        return PathBuf::from(path);
    }
    PathBuf::from("target/bkxpro-validator")
}

struct SimOptions {
    scenario: Option<String>,
    seed: u64,
    runs: u64,
    replay: bool,
    log: Option<PathBuf>,
    incidents: Option<PathBuf>,
    json: bool,
    quiet: bool,
}

impl SimOptions {
    fn parse(items: Vec<String>) -> Result<Self, String> {
        let mut options = Self {
            scenario: None,
            seed: 1,
            runs: 1,
            replay: false,
            log: None,
            incidents: None,
            json: false,
            quiet: false,
        };
        let mut all = false;
        let mut cursor = ArgCursor::new(items);
        while let Some(arg) = cursor.next() {
            match arg.as_str() {
                "--scenario" => options.scenario = Some(cursor.value("--scenario")?),
                "--all" => all = true,
                "--seed" => options.seed = cursor.parse("--seed")?,
                "--runs" => options.runs = cursor.parse("--runs")?,
                "--replay" => options.replay = true,
                "--log" => options.log = Some(PathBuf::from(cursor.value("--log")?)),
                "--incidents" => options.incidents = Some(PathBuf::from(cursor.value("--incidents")?)),
                "--json" => options.json = true,
                "--quiet" => options.quiet = true,
                other => return Err(format!("unknown sim argument {other:?}; see --help")),
            }
        }
        if all && options.scenario.is_some() {
            return Err("--all and --scenario are mutually exclusive".into());
        }
        if options.runs == 0 {
            return Err("--runs must be at least 1".into());
        }
        if let Some(name) = &options.scenario {
            if !PRESETS.contains(&name.as_str()) {
                return Err(format!(
                    "unknown scenario {name:?}; known: {}",
                    PRESETS.join(", ")
                ));
            }
        }
        Ok(options)
    }
}

fn run_sim(options: &SimOptions) -> Result<bool, String> {
    let plans: Vec<_> = match &options.scenario {
        Some(name) => (0..options.runs)
            .map(|offset| {
                let seed = options.seed.wrapping_add(offset);
                preset(name, seed).expect("scenario was validated")
            })
            .collect(),
        None => (0..options.runs)
            .flat_map(|offset| all_presets(options.seed.wrapping_add(offset)))
            .collect(),
    };

    let mut failures = 0usize;
    for plan in &plans {
        let mut report = match run_plan(plan) {
            Ok(report) => report,
            Err(error) => {
                failures += 1;
                if let Some(incidents) = &options.incidents {
                    write_harness_incident(incidents, plan.kind.name(), plan.seed, &error.to_string())
                        .map_err(|io_error| io_error.to_string())?;
                }
                eprintln!(
                    "FAIL {} seed={} harness error: {error}\n  reproduce: {}",
                    plan.name,
                    plan.seed,
                    plan.replay_command(),
                );
                continue;
            }
        };

        if options.replay {
            let second = run_plan(plan).map_err(|error| format!("replay run failed: {error}"))?;
            let mismatches = compare_reports(&report, &second);
            if !mismatches.is_empty() {
                report.ok = false;
                for mismatch in mismatches {
                    report.violations.push(format!("replay mismatch: {mismatch}"));
                }
            }
        }

        if let Some(log) = &options.log {
            append_run_log(log, &report).map_err(|error| format!("{}: {error}", log.display()))?;
        }
        if !report.ok {
            failures += 1;
            if let Some(incidents) = &options.incidents {
                let dir = write_incident(incidents, &report)
                    .map_err(|error| format!("{}: {error}", incidents.display()))?;
                if !options.json {
                    println!("  incident: {}/repro.md", dir.display());
                }
            }
        }
        if !options.quiet || !report.ok {
            print_run_report(&report, options.json);
        }
    }

    let total = plans.len();
    if !options.json {
        println!(
            "sim: {}/{total} runs passed{}",
            total - failures,
            if options.replay { " (double-run replay)" } else { "" }
        );
    }
    Ok(failures == 0)
}

fn print_run_report(report: &RunReport, json: bool) {
    if json {
        match serde_json::to_string(report) {
            Ok(line) => println!("{line}"),
            Err(error) => eprintln!("sim: report serialization failed: {error}"),
        }
        return;
    }
    println!(
        "{} {} seed={} acked={} trace={} mock_ops={} restarts={} digest={:#018x} content={:#018x} {}ms",
        if report.ok { "PASS" } else { "FAIL" },
        report.scenario,
        report.seed,
        report.acked_entries,
        report.trace_events,
        report.mock_events,
        report.restarts,
        report.trace_digest,
        report.trace_content_digest,
        report.duration_ms,
    );
    for violation in &report.violations {
        println!("  violation: {violation}");
    }
    for note in &report.notes {
        println!("  note: {note}");
    }
    if !report.ok {
        println!(
            "  reproduce: {}",
            replay_command(&report.scenario, report.seed)
        );
    }
}

struct SoakOptions {
    seed: u64,
    duration: Duration,
    scenarios: Vec<String>,
    runs: Option<u64>,
    log: Option<PathBuf>,
    incidents: Option<PathBuf>,
    resume: bool,
    rss_limit_bytes: Option<u64>,
    fd_limit: Option<usize>,
    root: PathBuf,
    dir_limit_bytes: Option<u64>,
    kill_every: u64,
    json: bool,
}

impl SoakOptions {
    fn parse(items: Vec<String>) -> Result<Self, String> {
        let mut options = Self {
            seed: 1,
            duration: Duration::ZERO,
            scenarios: Vec::new(),
            runs: None,
            log: None,
            incidents: None,
            resume: false,
            rss_limit_bytes: None,
            fd_limit: None,
            root: default_root(),
            dir_limit_bytes: None,
            kill_every: 0,
            json: false,
        };
        let mut minutes = 0u64;
        let mut seconds = 0u64;
        let mut cursor = ArgCursor::new(items);
        while let Some(arg) = cursor.next() {
            match arg.as_str() {
                "--seed" => options.seed = cursor.parse("--seed")?,
                "--minutes" => minutes = cursor.parse("--minutes")?,
                "--seconds" => seconds = cursor.parse("--seconds")?,
                "--runs" => options.runs = Some(cursor.parse("--runs")?),
                "--scenario" => options.scenarios.push(cursor.value("--scenario")?),
                "--log" => options.log = Some(PathBuf::from(cursor.value("--log")?)),
                "--incidents" => {
                    options.incidents = Some(PathBuf::from(cursor.value("--incidents")?))
                }
                "--resume" => options.resume = true,
                "--rss-limit-mib" => {
                    let value = cursor.value("--rss-limit-mib")?;
                    options.rss_limit_bytes = Some(parse_mib("--rss-limit-mib", &value)?);
                }
                "--fd-limit" => options.fd_limit = Some(cursor.parse("--fd-limit")?),
                "--dir" => options.root = PathBuf::from(cursor.value("--dir")?),
                "--dir-limit-mib" => {
                    let value = cursor.value("--dir-limit-mib")?;
                    options.dir_limit_bytes = Some(parse_mib("--dir-limit-mib", &value)?);
                }
                "--kill-every" => options.kill_every = cursor.parse("--kill-every")?,
                "--json" => options.json = true,
                other => return Err(format!("unknown soak argument {other:?}; see --help")),
            }
        }
        let total = minutes
            .checked_mul(60)
            .and_then(|value| value.checked_add(seconds))
            .ok_or_else(|| "soak duration is too large".to_string())?;
        options.duration = if total == 0 {
            Duration::from_secs(300)
        } else {
            Duration::from_secs(total)
        };
        for name in &options.scenarios {
            if !PRESETS.contains(&name.as_str()) {
                return Err(format!(
                    "unknown scenario {name:?}; known: {}",
                    PRESETS.join(", ")
                ));
            }
        }
        Ok(options)
    }
}

fn run_soak_command(options: &SoakOptions) -> Result<bool, String> {
    let mut config = SoakConfig::new(options.seed, options.duration);
    if !options.scenarios.is_empty() {
        config.scenarios = options.scenarios.clone();
    }
    config.max_runs = options.runs;
    config.log = Some(
        options
            .log
            .clone()
            .unwrap_or_else(|| options.root.join("runs.jsonl")),
    );
    config.incidents = Some(
        options
            .incidents
            .clone()
            .unwrap_or_else(|| options.root.join("incidents")),
    );
    config.resume = options.resume;
    config.rss_limit_bytes = options.rss_limit_bytes;
    config.fd_limit = options.fd_limit;
    config.disk_root = Some(options.root.clone());
    config.dir_limit_bytes = options.dir_limit_bytes;
    config.kill_every = options.kill_every;
    if options.kill_every > 0 {
        let exe = std::env::current_exe().map_err(|error| format!("current_exe: {error}"))?;
        let mut kill_config =
            KillSoakConfig::new(exe, options.root.join("kill-soak"), options.seed);
        kill_config.rounds = 1;
        config.kill_soak = Some(kill_config);
    }

    let mut last_progress = Instant::now();
    let report = run_soak(
        &config,
        |run| {
            let now = Instant::now();
            if !run.ok || now.duration_since(last_progress) >= Duration::from_secs(30) {
                println!(
                    "soak {} {} seed={} acked={} {}ms",
                    if run.ok { "PASS" } else { "FAIL" },
                    run.scenario,
                    run.seed,
                    run.acked_entries,
                    run.duration_ms,
                );
                last_progress = now;
            }
        },
        |round| {
            println!(
                "soak kill round {} seed={} delay={}ms journaled={} recovered={}",
                round.round, round.seed, round.kill_delay_ms, round.journaled, round.recovered,
            );
        },
    )
    .map_err(|error| error.to_string())?;

    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
        );
    } else {
        println!("{}", report.summary());
        for violation in &report.violations {
            println!("  violation: {violation}");
        }
        for violation in &report.kill_violations {
            println!("  kill violation: {violation}");
        }
        if !report.ok() {
            println!(
                "  reproduce an incident: cargo run -p bkxpro-validator --bin bkxpro-validator -- replay --report <incident>/report.json"
            );
        }
    }
    Ok(report.ok())
}

struct KillSoakOptions {
    seed: u64,
    rounds: u32,
    root: PathBuf,
    payload_bytes: usize,
    seal_at: usize,
    ledgers: u64,
    kill_delay_ms_min: u64,
    kill_delay_ms_max: u64,
    keep: bool,
    json: bool,
}

impl KillSoakOptions {
    fn parse(items: Vec<String>) -> Result<Self, String> {
        let mut options = Self {
            seed: 1,
            rounds: 8,
            root: default_root().join("kill-soak"),
            payload_bytes: 1024,
            seal_at: 32,
            ledgers: 2,
            kill_delay_ms_min: 50,
            kill_delay_ms_max: 500,
            keep: false,
            json: false,
        };
        let mut cursor = ArgCursor::new(items);
        while let Some(arg) = cursor.next() {
            match arg.as_str() {
                "--seed" => options.seed = cursor.parse("--seed")?,
                "--rounds" => options.rounds = cursor.parse("--rounds")?,
                "--dir" => options.root = PathBuf::from(cursor.value("--dir")?),
                "--payload-bytes" => options.payload_bytes = cursor.parse("--payload-bytes")?,
                "--seal-at" => options.seal_at = cursor.parse("--seal-at")?,
                "--ledgers" => options.ledgers = cursor.parse("--ledgers")?,
                "--kill-delay-min-ms" => {
                    options.kill_delay_ms_min = cursor.parse("--kill-delay-min-ms")?
                }
                "--kill-delay-max-ms" => {
                    options.kill_delay_ms_max = cursor.parse("--kill-delay-max-ms")?
                }
                "--keep" => options.keep = true,
                "--json" => options.json = true,
                other => {
                    return Err(format!("unknown kill-soak argument {other:?}; see --help"))
                }
            }
        }
        if options.rounds == 0 {
            return Err("--rounds must be at least 1".into());
        }
        if options.kill_delay_ms_min > options.kill_delay_ms_max {
            return Err("--kill-delay-min-ms must not exceed --kill-delay-max-ms".into());
        }
        Ok(options)
    }
}

fn run_kill_soak_command(options: &KillSoakOptions) -> Result<bool, String> {
    let exe = std::env::current_exe().map_err(|error| format!("current_exe: {error}"))?;
    let mut config = KillSoakConfig::new(exe, &options.root, options.seed);
    config.rounds = options.rounds;
    config.payload_bytes = options.payload_bytes;
    config.seal_at = options.seal_at;
    config.ledgers = options.ledgers;
    config.kill_delay_ms_min = options.kill_delay_ms_min;
    config.kill_delay_ms_max = options.kill_delay_ms_max;
    config.keep_dirs = options.keep;

    let outcome = run_kill_soak(&config, |round| {
        println!(
            "round {:>3} seed={} delay={}ms journaled={} recovered={} sealed={} open={} orphan={} {}ms",
            round.round,
            round.seed,
            round.kill_delay_ms,
            round.journaled,
            round.recovered,
            round.sealed_segments,
            round.open_segments,
            round.orphan_segments,
            round.duration_ms,
        );
        for violation in &round.violations {
            println!("  violation: {violation}");
        }
        if let Some(dir) = &round.round_dir {
            println!("  round dir kept: {}", dir.display());
        }
    })
    .map_err(|error| error.to_string())?;

    if options.json {
        let payload = serde_json::json!({
            "rounds": outcome.rounds,
            "violations": outcome.violations,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&payload).map_err(|error| error.to_string())?
        );
    } else {
        println!(
            "kill-soak: {} rounds, {} killed mid-write, {} acknowledged entries checked, {} violations",
            outcome.rounds.len(),
            outcome.killed_rounds(),
            outcome.total_journaled(),
            outcome.violations.len(),
        );
    }
    Ok(outcome.ok())
}

/// Options for `s3-run`: a real-endpoint run with values from flags, falling
/// back to the `BKXPRO_S3_TEST_*` environment contract.
struct S3RunOptions {
    seed: u64,
    segments: u64,
    entries: u64,
    payload_bytes: usize,
    endpoint: Option<String>,
    region: Option<String>,
    bucket: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    prefix: Option<String>,
    settle_timeout_seconds: u64,
    keep: bool,
    json: bool,
}

impl S3RunOptions {
    fn parse(items: Vec<String>) -> Result<Self, String> {
        let mut options = Self {
            seed: 1,
            segments: 3,
            entries: 40,
            payload_bytes: 96,
            endpoint: None,
            region: None,
            bucket: None,
            access_key: None,
            secret_key: None,
            prefix: None,
            settle_timeout_seconds: 120,
            keep: false,
            json: false,
        };
        let mut cursor = ArgCursor::new(items);
        while let Some(arg) = cursor.next() {
            match arg.as_str() {
                "--seed" => options.seed = cursor.parse("--seed")?,
                "--segments" => options.segments = cursor.parse("--segments")?,
                "--entries" => options.entries = cursor.parse("--entries")?,
                "--payload-bytes" => options.payload_bytes = cursor.parse("--payload-bytes")?,
                "--endpoint" => options.endpoint = Some(cursor.value("--endpoint")?),
                "--region" => options.region = Some(cursor.value("--region")?),
                "--bucket" => options.bucket = Some(cursor.value("--bucket")?),
                "--access-key" => options.access_key = Some(cursor.value("--access-key")?),
                "--secret-key" => options.secret_key = Some(cursor.value("--secret-key")?),
                "--prefix" => options.prefix = Some(cursor.value("--prefix")?),
                "--settle-timeout-seconds" => {
                    options.settle_timeout_seconds = cursor.parse("--settle-timeout-seconds")?
                }
                "--keep" => options.keep = true,
                "--json" => options.json = true,
                other => return Err(format!("unknown s3-run argument {other:?}; see --help")),
            }
        }
        if options.segments == 0 || options.entries == 0 {
            return Err("--segments and --entries must be at least 1".into());
        }
        if options.settle_timeout_seconds == 0 {
            return Err("--settle-timeout-seconds must be at least 1".into());
        }
        Ok(options)
    }

    /// Flags override the environment; the connection triple must come from one
    /// of the two.
    fn to_config(&self) -> Result<RealS3Config, String> {
        let mut config = RealS3Config::from_env().unwrap_or_default();
        if let Some(endpoint) = &self.endpoint {
            config.endpoint = Some(endpoint.clone());
        }
        if let Some(region) = &self.region {
            config.region = Some(region.clone());
        }
        if let Some(bucket) = &self.bucket {
            config.bucket = bucket.clone();
        }
        if let Some(access_key) = &self.access_key {
            config.access_key = access_key.clone();
        }
        if let Some(secret_key) = &self.secret_key {
            config.secret_key = secret_key.clone();
        }
        if let Some(prefix) = &self.prefix {
            config.prefix = prefix.clone();
        }
        config.seed = self.seed;
        config.segments = self.segments;
        config.entries_per_segment = self.entries;
        config.payload_bytes = self.payload_bytes;
        config.settle_timeout = Duration::from_secs(self.settle_timeout_seconds);
        config.keep = self.keep;
        if config.bucket.is_empty() || config.access_key.is_empty() || config.secret_key.is_empty()
        {
            return Err(
                "s3-run needs --bucket, --access-key and --secret-key (or \
                 BKXPRO_S3_TEST_BUCKET, BKXPRO_S3_TEST_ACCESS_KEY and \
                 BKXPRO_S3_TEST_SECRET_KEY)"
                    .into(),
            );
        }
        Ok(config)
    }
}

fn run_s3_run_command(options: &S3RunOptions) -> Result<bool, String> {
    let config = options.to_config()?;
    if !options.json {
        println!(
            "s3-run: bucket {} via {} prefix {} (seed {}, {} segments x {} entries x {} B, settle {}s)",
            config.bucket,
            config.endpoint.as_deref().unwrap_or("AWS S3"),
            config.resolved_prefix(),
            config.seed,
            config.segments,
            config.entries_per_segment,
            config.payload_bytes,
            config.settle_timeout.as_secs(),
        );
    }
    let report = run_real_s3(&config).map_err(|error| format!("s3-run: {error}"))?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
        );
    } else {
        println!(
            "{} real-s3 seed={} uploaded={} remote_reads={} verify={}seg/{}rec manifest={} passes={} settle={}ms violations={} {}ms",
            if report.ok { "PASS" } else { "FAIL" },
            report.seed,
            report.uploaded,
            report.remote_reads,
            report.remote_verify.segments_checked,
            report.remote_verify.records_checked,
            report.manifest_rows,
            report.passes,
            report.settle_ms,
            report.violations.len(),
            report.duration_ms,
        );
        for violation in &report.violations {
            println!("  violation: {violation}");
        }
        for note in &report.notes {
            println!("  note: {note}");
        }
        if !report.ok {
            println!("  reproduce: {}", report.replay);
            println!("  objects kept under {} for inspection", report.prefix);
        }
    }
    Ok(report.ok)
}

struct VerifyOptions {
    dir: PathBuf,
    seed: Option<u64>,
    journal: Option<PathBuf>,
    json: bool,
}

impl VerifyOptions {
    fn parse(items: Vec<String>) -> Result<Self, String> {
        let mut dir = None;
        let mut seed = None;
        let mut journal = None;
        let mut json = false;
        let mut cursor = ArgCursor::new(items);
        while let Some(arg) = cursor.next() {
            match arg.as_str() {
                "--dir" => dir = Some(PathBuf::from(cursor.value("--dir")?)),
                "--seed" => seed = Some(cursor.parse("--seed")?),
                "--journal" => journal = Some(PathBuf::from(cursor.value("--journal")?)),
                "--json" => json = true,
                other => return Err(format!("unknown verify argument {other:?}; see --help")),
            }
        }
        Ok(Self {
            dir: dir.ok_or("verify requires --dir PATH")?,
            seed,
            journal,
            json,
        })
    }
}

/// Verify a populated session directory: recover every segment and check that
/// every journaled acknowledgement is present with its seeded payload intact.
/// This is what the kernel fault scripts run after a `dm-log-writes` replay,
/// fault window, or remount. The seed comes from `--seed` or, when the child
/// wrote it, `child-meta.json`.
fn run_verify_command(options: &VerifyOptions) -> Result<bool, String> {
    let seed = match options.seed {
        Some(seed) => seed,
        None => read_child_meta(&options.dir)
            .map_err(|error| format!("{}: {error}", options.dir.display()))?
            .map(|meta| meta.seed)
            .ok_or_else(|| {
                format!(
                    "verify {}: no --seed and no {} found",
                    options.dir.display(),
                    bkxpro_validator::kill_soak::CHILD_META_FILE,
                )
            })?,
    };
    let report = verify_session_dir(&options.dir, seed, options.journal.as_deref())
        .map_err(|error| format!("verify {}: {error}", options.dir.display()))?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
        );
    } else {
        println!(
            "verify {}: seed={} journaled={} (malformed {}) recovered={} sealed={} open={} orphan={} violations={}",
            report.dir.display(),
            report.seed,
            report.journal_entries,
            report.malformed_journal_lines,
            report.recovered_records,
            report.sealed_segments,
            report.open_segments,
            report.orphan_segments,
            report.violations.len(),
        );
        for violation in &report.violations {
            println!("  violation: {violation}");
        }
    }
    Ok(report.ok())
}

struct FuzzOptions {
    seed: u64,
    iterations: Option<u64>,
    entries: u64,
    payload_bytes: usize,
    tail: bool,
    write_corpus: Option<PathBuf>,
    json: bool,
}

impl FuzzOptions {
    fn parse(items: Vec<String>) -> Result<Self, String> {
        let mut options = Self {
            seed: 1,
            iterations: None,
            entries: 16,
            payload_bytes: 96,
            tail: false,
            write_corpus: None,
            json: false,
        };
        let mut cursor = ArgCursor::new(items);
        while let Some(arg) = cursor.next() {
            match arg.as_str() {
                "--seed" => options.seed = cursor.parse("--seed")?,
                "--iterations" => options.iterations = Some(cursor.parse("--iterations")?),
                "--entries" => options.entries = cursor.parse("--entries")?,
                "--payload-bytes" => options.payload_bytes = cursor.parse("--payload-bytes")?,
                "--tail" => options.tail = true,
                "--write-corpus" => {
                    options.write_corpus = Some(PathBuf::from(cursor.value("--write-corpus")?))
                }
                "--json" => options.json = true,
                other => return Err(format!("unknown fuzz argument {other:?}; see --help")),
            }
        }
        if options.entries == 0 {
            return Err("--entries must be at least 1".into());
        }
        if options.write_corpus.is_none() && options.iterations.is_none() {
            options.iterations = Some(3_000);
        }
        Ok(options)
    }
}

fn run_fuzz_command(options: &FuzzOptions) -> Result<bool, String> {
    if let Some(dir) = &options.write_corpus {
        let count = write_corpus(dir, options.seed)
            .map_err(|error| format!("{}: {error}", dir.display()))?;
        println!("wrote {count} corpus files under {}", dir.display());
    }
    let Some(iterations) = options.iterations else {
        return Ok(true);
    };
    let config = FuzzConfig {
        seed: options.seed,
        iterations,
        entries: options.entries,
        payload_bytes: options.payload_bytes,
        max_mutations: 3,
    };
    let report = if options.tail {
        run_tail_fuzz(&config)
    } else {
        run_fuzz(&config)
    };
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
        );
    } else {
        println!(
            "fuzz seed={} iterations={} ({}) sealed_parses={} recovered_views={} invalid_headers={} recovered_records={}",
            report.seed,
            report.iterations,
            if options.tail { "tail faults" } else { "structural" },
            report.sealed_parses,
            report.recovered_views,
            report.invalid_headers,
            report.recovered_records,
        );
        println!("stops: {:?}", report.stops);
        for violation in &report.violations {
            println!("  violation: {violation}");
        }
        if !report.ok() {
            println!(
                "  reproduce: cargo run -p bkxpro-validator --bin bkxpro-validator -- fuzz --seed {} --iterations {} --entries {} --payload-bytes {}{}",
                report.seed,
                iterations,
                options.entries,
                options.payload_bytes,
                if options.tail { " --tail" } else { "" },
            );
        }
    }
    Ok(report.ok())
}

fn parse_child_config(items: Vec<String>) -> Result<ChildConfig, String> {    let mut dir = None;
    let mut seed = 0u64;
    let mut payload_bytes = 1024usize;
    let mut seal_at = 32usize;
    let mut ledgers = 2u64;
    let mut max_entries = 500_000u64;
    let mut step_micros = 500u64;
    let mut cursor = ArgCursor::new(items);
    while let Some(arg) = cursor.next() {
        match arg.as_str() {
            "--dir" => dir = Some(PathBuf::from(cursor.value("--dir")?)),
            "--seed" => seed = cursor.parse("--seed")?,
            "--payload-bytes" => payload_bytes = cursor.parse("--payload-bytes")?,
            "--seal-at" => seal_at = cursor.parse("--seal-at")?,
            "--ledgers" => ledgers = cursor.parse("--ledgers")?,
            "--max-entries" => max_entries = cursor.parse("--max-entries")?,
            "--step-micros" => step_micros = cursor.parse("--step-micros")?,
            other => {
                return Err(format!("unknown kill-soak-child argument {other:?}; see --help"))
            }
        }
    }
    let dir = dir.ok_or("kill-soak-child requires --dir PATH")?;
    if ledgers == 0 {
        return Err("--ledgers must be at least 1".into());
    }
    if seal_at == 0 {
        return Err("--seal-at must be at least 1".into());
    }
    Ok(ChildConfig {
        dir,
        seed,
        payload_bytes,
        seal_at,
        ledgers,
        max_entries,
        step_micros,
    })
}

fn print_usage() {
    println!(
        "\
bkxpro-validator - deterministic simulation, kill/soak testing, replay, dashboard (RAN-132)

USAGE:
  bkxpro-validator sim [--scenario NAME | --all] [--seed N] [--runs N] [--replay]
                    [--log PATH] [--incidents DIR] [--json] [--quiet]
      Run presets at consecutive seeds (default: the whole matrix at seed 1).
      --replay runs every plan twice and fails when the digests differ.

  bkxpro-validator soak [--seed N] [--minutes M] [--seconds S] [--runs N]
                     [--scenario NAME]... [--log PATH] [--incidents DIR] [--resume]
                     [--dir PATH] [--rss-limit-mib N] [--fd-limit N]
                     [--dir-limit-mib N] [--kill-every N] [--json]
      Long-running loop over the matrix with resource budgets and an optional
      real-process kill round every N runs. --runs is a target total, so
      --resume continues the seed sequence instead of repeating it.

  bkxpro-validator kill-soak [--seed N] [--rounds N] [--dir PATH]
                          [--payload-bytes N] [--seal-at N] [--ledgers N]
                          [--kill-delay-min-ms N] [--kill-delay-max-ms N] [--keep] [--json]
      SIGKILL a child writer mid-append and verify every acknowledged entry is
      recovered with its exact payload.

  bkxpro-validator s3-run [--seed N] [--segments N] [--entries N] [--payload-bytes N]
                       [--endpoint URL] [--region REGION] [--bucket NAME]
                       [--access-key KEY] [--secret-key KEY] [--prefix P]
                       [--settle-timeout-seconds N] [--keep] [--json]
      Drive the smoke lifecycle against a real S3-compatible endpoint
      (minio/AWS). Connection values fall back to BKXPRO_S3_TEST_ENDPOINT,
      _REGION, _BUCKET, _ACCESS_KEY, _SECRET_KEY. Uploads retry until they
      drain or the settle timeout, riding out endpoint outages; each run uses a
      unique object prefix and cleans it up unless --keep (or a violation).

  bkxpro-validator replay --report PATH [--json]
      Re-run the recorded scenario/seed and compare digests with the report.

  bkxpro-validator verify --dir PATH [--seed N] [--journal PATH] [--json]
      Recover a journaled session directory (segments plus acked.jsonl) and
      check every acknowledgement against the recovered records. The seed is
      read from child-meta.json when --seed is omitted. Used after a
      dm-log-writes replay, fault window, or real remount.

  bkxpro-validator fuzz [--seed N] [--iterations N] [--entries N] [--payload-bytes N]
                     [--tail] [--write-corpus DIR] [--json]
      Structural corruption fuzzing of the segment parsers (prefix and exact
      tail properties). --tail limits mutations to truncation/zero tails.
      --write-corpus regenerates the cargo-fuzz corpus under DIR.

  bkxpro-validator dashboard --log PATH [--json]
      Pass-rate aggregation over a JSONL run log.

Exit codes: 0 all passed, 1 failures/violations, 2 usage or harness error."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soak_duration_rejects_multiplication_and_addition_overflow() {
        for args in [
            vec!["--minutes".to_string(), u64::MAX.to_string()],
            vec![
                "--minutes".to_string(),
                "1".to_string(),
                "--seconds".to_string(),
                u64::MAX.to_string(),
            ],
        ] {
            assert!(SoakOptions::parse(args)
                .err()
                .unwrap()
                .contains("duration is too large"));
        }
        let options = SoakOptions::parse(vec!["--seconds".into(), u64::MAX.to_string()]).unwrap();
        assert_eq!(options.duration, Duration::from_secs(u64::MAX));
        assert_eq!(
            SoakOptions::parse(vec![]).unwrap().duration,
            Duration::from_secs(300)
        );
    }
}
