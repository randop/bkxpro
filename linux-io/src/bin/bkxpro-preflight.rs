#[cfg(not(target_os = "linux"))]
compile_error!("bkxpro-preflight supports Linux only");

#[cfg(target_os = "linux")]
use std::{env, path::PathBuf, process::ExitCode};
fn main() -> ExitCode {
    use bkxpro_linux_io::{run_host_preflight, HostPreflightConfig};

    fn run() -> Result<(), String> {
        let mut args = env::args().skip(1);
        let mut config = HostPreflightConfig::default();
        let mut has_dir = false;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--data-dir" => {
                    let value = args.next().ok_or("--data-dir requires a path")?;
                    config.data_dir = PathBuf::from(value);
                    has_dir = true;
                }
                "--memory-budget-mib" => {
                    let value = args
                        .next()
                        .ok_or("--memory-budget-mib requires an integer")?;
                    config.memory_budget_bytes = parse_mib(&value)?;
                }
                "--registered-buffer-mib" => {
                    let value = args
                        .next()
                        .ok_or("--registered-buffer-mib requires an integer")?;
                    config.registered_buffer_bytes = parse_mib(&value)?;
                }
                "--ring-entries" => {
                    let value = args.next().ok_or("--ring-entries requires an integer")?;
                    config.ring_entries = value.parse().map_err(|_| "invalid --ring-entries")?;
                }
                "--commit-mode" => {
                    let value = args.next().ok_or("--commit-mode requires a value")?;
                    config.commit_mode = match value.as_str() {
                        "drain-fdatasync" => bkxpro_linux_io::CommitMode::DrainFdatasync,
                        "rwf-dsync" => bkxpro_linux_io::CommitMode::RwfDsync,
                        _ => {
                            return Err(
                                "--commit-mode must be drain-fdatasync (default) or rwf-dsync"
                                    .into(),
                            )
                        }
                    };
                }
                "--help" | "-h" => {
                    println!("Usage: bkxpro-preflight --data-dir PATH [--memory-budget-mib N] [--registered-buffer-mib N] [--ring-entries N] [--commit-mode drain-fdatasync|rwf-dsync]");
                    return Ok(());
                }
                other => return Err(format!("unknown argument {other:?}; use --help")),
            }
        }
        if !has_dir {
            return Err("--data-dir is required".into());
        }
        let result = run_host_preflight(&config).map_err(|e| e.to_string())?;
        println!("bkxpro Linux I/O preflight: PASS");
        println!("kernel: {}", result.report.kernel_release);
        println!("filesystem: {}", result.report.filesystem);
        println!("data directory: {}", result.report.data_dir.display());
        println!(
            "O_DIRECT alignment: memory={} offset={} bytes",
            result.report.dio_mem_align, result.report.dio_offset_align
        );
        println!("device write cache: {}", result.report.write_cache);
        println!(
            "cgroup memory: current={} bytes max={} bytes",
            result.report.cgroup_memory_current, result.report.cgroup_memory_max
        );
        println!(
            "RLIMIT_MEMLOCK: {} bytes; VmLck: {} bytes; required pinned buffers: {} bytes",
            result.report.memlock_limit,
            result.report.locked_memory_bytes,
            config.registered_buffer_bytes
        );
        println!(
            "io_uring: SINGLE_ISSUER + DEFER_TASKRUN; registered buffers/files; mode={:?}",
            config.commit_mode
        );
        println!("the data-directory lock is held while this command runs; the service must retain its PreflightResult for its lifetime");
        // Retaining result through this scope demonstrates the lock is alive for all checks.
        drop(result);
        Ok(())
    }

    fn parse_mib(value: &str) -> Result<u64, String> {
        let n: u64 = value
            .parse()
            .map_err(|_| format!("invalid MiB value {value:?}"))?;
        n.checked_mul(1024 * 1024)
            .ok_or_else(|| "MiB value overflows byte count".into())
    }

    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("bkxpro-preflight: {error}");
            ExitCode::FAILURE
        }
    }
}
