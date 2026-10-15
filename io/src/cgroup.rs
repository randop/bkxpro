//! cgroup v2 memory accounting shared by the host preflight and the memory benches.
//!
//! `memory.current` counts RSS, page cache, and kernel memory charged to the process
//! cgroup, so it measures the RAN-130 page-cache budget and the RAN-131 RSS budget
//! against one number. Only the unified (v2) hierarchy is supported: the RAN-134 host
//! preflight requires a finite `memory.max` and rejects hosts that still run cgroup v1.

use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

/// `memory.events` counters (cgroup v2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemoryEvents {
    /// Times reclaim was invoked while the cgroup exceeded `memory.high`.
    pub high: u64,
    /// Times the cgroup reached `memory.max` and allocations stalled.
    pub max: u64,
    /// Times an allocation in the cgroup triggered the OOM killer.
    pub oom: u64,
    /// Processes killed because of this cgroup's memory limit.
    pub oom_kill: u64,
    /// Times the whole cgroup (or subtree) was OOM-killed.
    pub oom_group_kill: u64,
}

impl MemoryEvents {
    /// True when the cgroup was OOM-killed at least once since it was created.
    pub fn killed(&self) -> bool {
        self.oom_kill > 0 || self.oom_group_kill > 0
    }
}

/// The cgroup v2 memory cgroup the current process is accounted in.
#[derive(Debug, Clone)]
pub struct MemoryCgroup {
    directory: PathBuf,
}

impl MemoryCgroup {
    /// Resolve the process cgroup from `/proc/self/cgroup` and
    /// `/proc/self/mountinfo`.
    pub fn detect() -> io::Result<Self> {
        let cgroups = fs::read_to_string("/proc/self/cgroup")?;
        let mountinfo = fs::read_to_string("/proc/self/mountinfo")?;
        Ok(Self {
            directory: resolve_directory(&cgroups, &mountinfo, Path::new(CGROUP2_MOUNT))?,
        })
    }

    /// Directory holding this cgroup's `memory.*` files.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// `memory.current` in bytes (RSS, page cache, and kernel memory charged here).
    pub fn current_bytes(&self) -> io::Result<u64> {
        let path = self.directory.join("memory.current");
        let text = fs::read_to_string(&path)?;
        text.trim()
            .parse::<u64>()
            .map_err(|_| invalid_data("cgroup memory.current is not an integer"))
    }

    /// Nearest finite `memory.max` at this cgroup or an ancestor.
    ///
    /// `None` when every visible ancestor is unlimited (`max`).
    pub fn effective_limit_bytes(&self) -> io::Result<Option<u64>> {
        let mut effective: Option<u64> = None;
        for ancestor in self.directory.ancestors() {
            let path = ancestor.join("memory.max");
            if !path.is_file() {
                continue;
            }
            let text = fs::read_to_string(&path)?;
            let raw = text.trim();
            if raw == "max" {
                continue;
            }
            let value = raw.parse::<u64>().map_err(|_| {
                invalid_data(format!("{} is not an integer or 'max'", path.display()))
            })?;
            if value == 0 {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("{} must be non-zero", path.display()),
                ));
            }
            effective = Some(effective.map_or(value, |old| old.min(value)));
        }
        Ok(effective)
    }

    /// `memory.events` counters.
    pub fn events(&self) -> io::Result<MemoryEvents> {
        let text = fs::read_to_string(self.directory.join("memory.events"))?;
        parse_events(&text)
    }

    #[cfg(test)]
    fn at(directory: PathBuf) -> Self {
        Self { directory }
    }
}

/// Conventional cgroup2 mount point, used when `/proc/self/mountinfo` exposes none.
const CGROUP2_MOUNT: &str = "/sys/fs/cgroup";

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message.into())
}

/// Resolve the directory of the process cgroup from `/proc/self/cgroup` and
/// `/proc/self/mountinfo` contents.
///
/// `cgroup_text` provides the unified path (`0::/...`); `mountinfo_text` locates the
/// cgroup2 mount that path is relative to. `conventional` is the fallback mount point
/// used when no cgroup2 entry can be matched.
fn resolve_directory(
    cgroup_text: &str,
    mountinfo_text: &str,
    conventional: &Path,
) -> io::Result<PathBuf> {
    let cgroup_path = cgroup_text
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                "process is not in a visible unified cgroup v2 hierarchy",
            )
        })?;
    for line in mountinfo_text.lines() {
        let mut halves = line.split(" - ");
        let left = halves.next().unwrap_or("");
        let right = halves.next().unwrap_or("");
        if !right.starts_with("cgroup2 ") {
            continue;
        }
        let fields: Vec<&str> = left.split_whitespace().collect();
        if fields.len() < 5 {
            continue;
        }
        let root = fields[3];
        let mount = fields[4].replace("\\040", " ");
        let relative = if root == "/" {
            cgroup_path.trim_start_matches('/')
        } else if cgroup_path == root {
            ""
        } else if let Some(rest) = cgroup_path
            .strip_prefix(root)
            .and_then(|rest| rest.strip_prefix('/'))
        {
            rest
        } else {
            continue;
        };
        return Ok(PathBuf::from(mount).join(relative));
    }
    if conventional.join("memory.max").exists() {
        return Ok(conventional.to_path_buf());
    }
    Err(io::Error::new(
        ErrorKind::NotFound,
        "could not resolve cgroup v2 mount; mount cgroup2 and expose memory.max/memory.current",
    ))
}

/// Parse `memory.events` contents; unknown keys are ignored.
fn parse_events(text: &str) -> io::Result<MemoryEvents> {
    let mut events = MemoryEvents::default();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(' ') else {
            continue;
        };
        let value = value
            .trim()
            .parse::<u64>()
            .map_err(|_| invalid_data(format!("memory.events {key} is not an integer")))?;
        match key {
            "high" => events.high = value,
            "max" => events.max = value,
            "oom" => events.oom = value,
            "oom_kill" => events.oom_kill = value,
            "oom_group_kill" => events.oom_group_kill = value,
            _ => {}
        }
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOUNTINFO: &str = "23 28 0:22 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw\n\
                             30 23 0:26 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:9 - cgroup2 cgroup2 rw\n";

    #[test]
    fn resolves_a_root_mounted_cgroup2() {
        let cgroups = "0::/user.slice/user-1000.slice/session-1.scope\n";
        let directory = resolve_directory(cgroups, MOUNTINFO, Path::new("/nope")).unwrap();
        assert_eq!(
            directory,
            Path::new("/sys/fs/cgroup/user.slice/user-1000.slice/session-1.scope")
        );
    }

    #[test]
    fn detects_the_process_cgroup_when_the_host_exposes_one() {
        match MemoryCgroup::detect() {
            Ok(cgroup) => assert!(cgroup.directory().join("memory.current").is_file()),
            // A container without a cgroup2 mount is not a test failure: the
            // preflight rejects such a host at startup instead.
            Err(error) => assert_eq!(error.kind(), ErrorKind::NotFound),
        }
    }

    #[test]
    fn resolves_a_nested_mount_root() {
        let cgroups = "0::/kubepods/pod123/container\n";
        let mountinfo = "30 23 0:26 /kubepods/pod123 /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n";
        let directory = resolve_directory(cgroups, mountinfo, Path::new("/nope")).unwrap();
        assert_eq!(directory, Path::new("/sys/fs/cgroup/container"));
    }

    #[test]
    fn resolves_a_cgroup_equal_to_the_mount_root() {
        let cgroups = "0::/kubepods/pod123\n";
        let mountinfo = "30 23 0:26 /kubepods/pod123 /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n";
        let directory = resolve_directory(cgroups, mountinfo, Path::new("/nope")).unwrap();
        assert_eq!(directory, Path::new("/sys/fs/cgroup"));
    }

    #[test]
    fn unescapes_spaces_in_the_mount_point() {
        let cgroups = "0::/scope\n";
        let mountinfo = "30 23 0:26 / /mnt/cgroup2\\040mount rw - cgroup2 cgroup2 rw\n";
        let directory = resolve_directory(cgroups, mountinfo, Path::new("/nope")).unwrap();
        assert_eq!(directory, Path::new("/mnt/cgroup2 mount/scope"));
    }

    #[test]
    fn rejects_a_legacy_hierarchy() {
        let error = resolve_directory("2::/legacy\n", MOUNTINFO, Path::new("/nope")).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn rejects_an_unmatched_path_without_a_conventional_mount() {
        let cgroups = "0::/unrelated\n";
        let mountinfo = "30 23 0:26 /kubepods/pod123 /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n";
        let error =
            resolve_directory(cgroups, mountinfo, Path::new("/nope/nothing/here")).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn reads_the_effective_limit_from_the_nearest_finite_ancestor() {
        let root = tempfile::tempdir().unwrap();
        let child = root.path().join("child");
        fs::create_dir(&child).unwrap();
        fs::write(root.path().join("memory.max"), "max\n").unwrap();
        let cgroup = MemoryCgroup::at(child.clone());
        assert_eq!(cgroup.effective_limit_bytes().unwrap(), None);
        fs::write(child.join("memory.max"), "2048\n").unwrap();
        assert_eq!(cgroup.effective_limit_bytes().unwrap(), Some(2048));
        fs::write(root.path().join("memory.max"), "1024\n").unwrap();
        assert_eq!(cgroup.effective_limit_bytes().unwrap(), Some(1024));
    }

    #[test]
    fn rejects_a_zero_limit() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("memory.max"), "0\n").unwrap();
        let cgroup = MemoryCgroup::at(root.path().to_path_buf());
        assert_eq!(
            cgroup.effective_limit_bytes().unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }

    #[test]
    fn reads_current_and_events() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("memory.current"), "4096\n").unwrap();
        fs::write(
            root.path().join("memory.events"),
            "low 0\nhigh 3\nmax 1\noom 1\noom_kill 0\noom_group_kill 0\n",
        )
        .unwrap();
        let cgroup = MemoryCgroup::at(root.path().to_path_buf());
        assert_eq!(cgroup.current_bytes().unwrap(), 4096);
        let events = cgroup.events().unwrap();
        assert_eq!(events.high, 3);
        assert_eq!(events.max, 1);
        assert_eq!(events.oom, 1);
        assert!(!events.killed());
    }

    #[test]
    fn treats_oom_group_kills_as_killed() {
        let events = parse_events("oom_kill 0\noom_group_kill 2\n").unwrap();
        assert!(events.killed());
    }

    #[test]
    fn rejects_a_corrupt_events_file() {
        let error = parse_events("oom maybe\n").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }
}
