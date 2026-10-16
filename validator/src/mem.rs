//! Process and cgroup resource sampling for soak runs (Linux only).
//!
//! The soak budget is enforced against the same signals the staging hosts are
//! monitored on: RSS from `/proc/self/statm`, cgroup v2 `memory.current` (falling
//! back to the cgroup v1 file), and the open-fd count from `/proc/self/fd`.

use std::{fs, path::Path};

/// One sample of the resource pressure observed during a soak.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct ResourceSample {
    /// Resident set size in bytes.
    pub rss_bytes: u64,
    /// cgroup memory usage in bytes (`memory.current`, or cgroup v1
    /// `memory.usage_in_bytes`).
    pub cgroup_current_bytes: u64,
    /// cgroup memory cap in bytes when one is configured; 0 means unbounded/unknown.
    pub cgroup_limit_bytes: u64,
    /// Open file descriptors of this process.
    pub open_fds: usize,
}

impl ResourceSample {
    /// Take one sample; unknown readings are zero.
    pub fn take() -> Self {
        Self {
            rss_bytes: rss_bytes().unwrap_or(0),
            cgroup_current_bytes: read_counter(&[
                "/sys/fs/cgroup/memory.current",
                "/sys/fs/cgroup/memory/memory.usage_in_bytes",
            ])
            .unwrap_or(0),
            cgroup_limit_bytes: read_limit(&[
                "/sys/fs/cgroup/memory.max",
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            ])
            .unwrap_or(0),
            open_fds: open_fds().unwrap_or(0),
        }
    }
}

/// Resident set size from `/proc/self/statm`, in bytes.
pub fn rss_bytes() -> Option<u64> {
    let text = fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = text.split_whitespace().nth(1)?.parse().ok()?;
    Some(resident_pages * page_size())
}

fn page_size() -> u64 {
    // SAFETY: sysconf with a valid name is safe and cannot fail for the page size.
    let value = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if value > 0 {
        value as u64
    } else {
        4096
    }
}

/// Open file descriptors of this process.
pub fn open_fds() -> Option<usize> {
    Some(fs::read_dir("/proc/self/fd").ok()?.count())
}

fn read_counter(paths: &[&str]) -> Option<u64> {
    for path in paths {
        if let Ok(text) = fs::read_to_string(path) {
            if let Ok(value) = text.trim().parse::<u64>() {
                return Some(value);
            }
        }
    }
    None
}

fn read_limit(paths: &[&str]) -> Option<u64> {
    for path in paths {
        if let Ok(text) = fs::read_to_string(path) {
            let text = text.trim();
            if text == "max" {
                return None;
            }
            if let Ok(value) = text.parse::<u64>() {
                // cgroup v1 reports an absurd sentinel for "unlimited".
                if value >= (1 << 60) {
                    return None;
                }
                return Some(value);
            }
        }
    }
    None
}

/// Tracks peaks over a soak and flags budget violations.
#[derive(Debug, Default)]
pub struct ResourceMonitor {
    pub peak_rss_bytes: u64,
    pub peak_cgroup_bytes: u64,
    pub peak_open_fds: usize,
    pub rss_limit_bytes: Option<u64>,
    pub fd_limit: Option<usize>,
    pub violations: Vec<String>,
}

impl ResourceMonitor {
    pub fn new(rss_limit_bytes: Option<u64>, fd_limit: Option<usize>) -> Self {
        Self {
            rss_limit_bytes,
            fd_limit,
            ..Self::default()
        }
    }

    /// Sample once, update peaks, and record limit violations.
    pub fn observe(&mut self) -> ResourceSample {
        let sample = ResourceSample::take();
        self.peak_rss_bytes = self.peak_rss_bytes.max(sample.rss_bytes);
        self.peak_cgroup_bytes = self.peak_cgroup_bytes.max(sample.cgroup_current_bytes);
        self.peak_open_fds = self.peak_open_fds.max(sample.open_fds);
        if let Some(limit) = self.rss_limit_bytes {
            if sample.rss_bytes > limit {
                self.violations.push(format!(
                    "RSS {} bytes exceeded the {} byte budget",
                    sample.rss_bytes, limit
                ));
            }
        }
        if let Some(limit) = self.fd_limit {
            if sample.open_fds > limit {
                self.violations.push(format!(
                    "{} open fds exceeded the {} limit",
                    sample.open_fds, limit
                ));
            }
        }
        sample
    }
}

/// The soak's own scratch/cgroup usage, for the multi-day procedure: the point is
/// that neither this process nor its temp data directory grows without bound.
pub fn dir_usage_bytes(dir: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if metadata.is_dir() {
                stack.push(entry.path());
            } else {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_look_plausible() {
        let sample = ResourceSample::take();
        assert!(sample.rss_bytes > 0, "RSS must be readable on Linux");
        assert!(sample.open_fds > 0, "the process must have open fds");
    }

    #[test]
    fn monitor_records_a_peak_and_flags_impossible_limits() {
        let mut monitor = ResourceMonitor::new(Some(1), Some(0));
        monitor.observe();
        assert!(monitor.peak_rss_bytes > 1);
        assert_eq!(monitor.violations.len(), 2);
    }
}
