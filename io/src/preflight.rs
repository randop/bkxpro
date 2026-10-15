use crate::backend::AlignedBlock;
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Result, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Component, Path, PathBuf};

#[derive(Debug)]
pub struct PreflightReport {
    pub kernel_version_ok: bool,
    pub io_uring_enabled: bool,
    pub filesystem_supported: bool,
    pub dio_alignment_ok: bool,
    pub write_cache_state: String,
    pub memlock_ok: bool,
    pub lock_acquired: bool,
    /// Retain this report (or move this guard to the storage owner) until shutdown.
    pub directory_lock: DirectoryLock,
}

/// Owns the open file description; closing it releases the OFD lock.
#[derive(Debug)]
pub struct DirectoryLock {
    _file: File,
}

impl DirectoryLock {
    pub fn acquire(data_dir: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(data_dir.join("bkxpro.lock"))?;
        let mut fl: libc::flock = unsafe { std::mem::zeroed() };
        fl.l_type = libc::F_WRLCK as i16;
        fl.l_whence = libc::SEEK_SET as i16;
        // SAFETY: fl is initialized and the fd stays alive in the returned guard.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &fl) } == -1 {
            let err = Error::last_os_error();
            return Err(Error::new(
                err.kind(),
                format!(
                    "Failed to acquire Open File Description (OFD) lock on data directory: {err}"
                ),
            ));
        }
        Ok(Self { _file: file })
    }
}

pub fn run_preflight(data_dir: &Path, memory_budget_bytes: u64) -> Result<PreflightReport> {
    let mut uts = unsafe { std::mem::zeroed::<libc::utsname>() };
    if unsafe { libc::uname(&mut uts) } != 0 {
        return Err(Error::last_os_error());
    }
    let release_str = unsafe { std::ffi::CStr::from_ptr(uts.release.as_ptr()) }.to_string_lossy();
    let kernel_version_ok = parse_kernel_version(&release_str);
    if !kernel_version_ok {
        return Err(Error::new(
            ErrorKind::Other,
            format!("Preflight Failed: Linux kernel >= 6.1 required (detected {}). Fix: Upgrade kernel to 6.1 LTS or newer.", release_str)
        ));
    }

    // Creation is the definitive eligibility probe, including sysctl values 0/1/2,
    // allowed groups/capabilities, seccomp and unknown/unreadable sysctl state.
    let _ring = io_uring::IoUring::new(2).map_err(|error| Error::new(
        error.kind(), format!("Preflight failed: cannot create io_uring; check host sysctl and seccomp policy: {error}"),
    ))?;
    let io_uring_enabled = true;

    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)?;
    }
    let directory_lock = DirectoryLock::acquire(data_dir)?;
    let test_file_path = data_dir.join(".preflight_dio_test");
    let dio_test_result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(&test_file_path)?;
        let align_buf = AlignedBlock([0; 4096]);
        file.write_all(&align_buf.0)?;
        file.sync_all()?;
        Ok(())
    })();
    let _ = std::fs::remove_file(&test_file_path);

    if dio_test_result.is_err() {
        return Err(Error::new(
            ErrorKind::Other,
            "Preflight Failed: Filesystem does not support O_DIRECT or data directory is unsupported. Fix: Ensure data directory resides on an XFS or ext4 filesystem mounted with O_DIRECT capability."
        ));
    }

    let lock_acquired = true;
    let cgroup_limit = effective_memory_limit(
        &std::fs::read_to_string("/proc/self/cgroup")?,
        &std::fs::read_to_string("/proc/self/mountinfo")?,
    )?;
    if cgroup_limit.is_some_and(|limit| limit < memory_budget_bytes) {
        return Err(Error::other(format!(
            "Preflight failed: effective cgroup memory limit ({}) is below configured budget ({memory_budget_bytes})",
            cgroup_limit.unwrap(),
        )));
    }

    let write_cache_state = std::fs::read_to_string("/sys/block/sda/queue/write_cache")
        .unwrap_or_else(|_| "unknown".to_string())
        .trim()
        .to_string();

    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    unsafe {
        libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut rlim);
    };
    let memlock_ok = rlim.rlim_cur >= 64 * 1024 * 1024;

    Ok(PreflightReport {
        kernel_version_ok,
        io_uring_enabled,
        filesystem_supported: true,
        dio_alignment_ok: true,
        write_cache_state,
        memlock_ok,
        lock_acquired,
        directory_lock,
    })
}

fn parse_kernel_version(release: &str) -> bool {
    let parts: Vec<&str> = release.split('.').collect();
    if parts.is_empty() {
        return false;
    }
    if let Ok(major) = parts[0].parse::<u32>() {
        if major > 6 {
            return true;
        }
        if major == 6 {
            if parts.len() > 1 {
                if let Ok(minor) = parts[1].parse::<u32>() {
                    return minor >= 1;
                }
            }
        }
    }
    false
}

fn invalid_cgroup(message: &str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn mount_path(field: &str) -> Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    let mut bytes = Vec::new();
    let raw = field.as_bytes();
    let mut index = 0;
    while index < raw.len() {
        if raw[index] == b'\\' {
            let escape = raw
                .get(index + 1..index + 4)
                .ok_or_else(|| invalid_cgroup("invalid mountinfo escape"))?;
            let text = std::str::from_utf8(escape)
                .map_err(|_| invalid_cgroup("invalid mountinfo escape"))?;
            let value = u8::from_str_radix(text, 8)
                .map_err(|_| invalid_cgroup("invalid mountinfo escape"))?;
            bytes.push(value);
            index += 4;
        } else {
            bytes.push(raw[index]);
            index += 1;
        }
    }
    let path = PathBuf::from(std::ffi::OsString::from_vec(bytes));
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(invalid_cgroup(
            "cgroup paths must be absolute without traversal",
        ));
    }
    Ok(path)
}

fn resolve_memory_cgroup(cgroups: &str, mountinfo: &str) -> Result<(PathBuf, PathBuf, bool)> {
    // Prefer a v1 memory controller when running in a hybrid hierarchy.
    let mut unified = None;
    let mut memory = None;
    for line in cgroups.lines() {
        let fields: Vec<_> = line.splitn(3, ':').collect();
        if fields.len() != 3 {
            return Err(invalid_cgroup("invalid /proc/self/cgroup"));
        }
        if fields[1].split(',').any(|c| c == "memory") {
            memory = Some(fields[2]);
        } else if fields[0] == "0" && fields[1].is_empty() {
            unified = Some(fields[2]);
        }
    }
    let v2 = memory.is_none();
    let group = mount_path(
        memory
            .or(unified)
            .ok_or_else(|| invalid_cgroup("no process memory cgroup"))?,
    )?;
    let mut matches = Vec::new();
    for line in mountinfo.lines() {
        let (left, right) = line
            .split_once(" - ")
            .ok_or_else(|| invalid_cgroup("invalid mountinfo"))?;
        let right: Vec<_> = right.split_whitespace().collect();
        if right.len() < 3 {
            return Err(invalid_cgroup("invalid mountinfo filesystem"));
        }
        if (v2 && right[0] != "cgroup2")
            || (!v2 && (right[0] != "cgroup" || !right[2].split(',').any(|c| c == "memory")))
        {
            continue;
        }
        let left: Vec<_> = left.split_whitespace().collect();
        if left.len() < 6 {
            return Err(invalid_cgroup("invalid mountinfo fields"));
        }
        let root = mount_path(left[3])?;
        let mount = mount_path(left[4])?;
        if let Ok(relative) = group.strip_prefix(&root) {
            matches.push((root.components().count(), mount.join(relative), mount));
        }
    }
    // A less restricted mount exposes more ancestor limits.
    matches.sort_by_key(|m| m.0);
    let (_, directory, mount) = matches
        .into_iter()
        .next()
        .ok_or_else(|| invalid_cgroup("cannot resolve process memory cgroup mount"))?;
    Ok((directory, mount, v2))
}

fn effective_memory_limit(cgroups: &str, mountinfo: &str) -> Result<Option<u64>> {
    let (directory, mount, v2) = resolve_memory_cgroup(cgroups, mountinfo)?;
    let mut effective = None;
    for ancestor in directory.ancestors().take_while(|p| p.starts_with(&mount)) {
        let value = std::fs::read_to_string(ancestor.join(if v2 {
            "memory.max"
        } else {
            "memory.limit_in_bytes"
        }))?;
        let value = value.trim();
        if v2 && value == "max" {
            continue;
        }
        let limit = value
            .parse::<u64>()
            .map_err(|_| invalid_cgroup("invalid cgroup memory limit"))?;
        effective = Some(effective.map_or(limit, |previous: u64| previous.min(limit)));
    }
    Ok(effective)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fixture(v2: bool) -> (tempfile::TempDir, String, String) {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("team/service")).unwrap();
        let file = if v2 {
            "memory.max"
        } else {
            "memory.limit_in_bytes"
        };
        for (ancestor, limit) in [("", "8192"), ("team", "4096"), ("team/service", "16384")] {
            std::fs::write(dir.path().join(ancestor).join(file), limit).unwrap();
        }
        let groups = if v2 {
            "0::/team/service"
        } else {
            "5:cpu,memory:/team/service"
        }
        .to_owned();
        let filesystem = if v2 {
            "cgroup2 cgroup rw"
        } else {
            "cgroup cgroup rw,memory"
        };
        let mounts = format!("29 23 0:26 / {} rw - {filesystem}", dir.path().display());
        (dir, groups, mounts)
    }

    #[test]
    fn lock_is_held_until_guard_is_dropped() {
        let dir = tempdir().unwrap();
        let guard = DirectoryLock::acquire(dir.path()).unwrap();
        let error = DirectoryLock::acquire(dir.path()).err().unwrap();
        assert!(
            matches!(
                error.raw_os_error(),
                Some(libc::EAGAIN)
                    | Some(libc::EACCES)
                    | Some(libc::ENOLCK)
                    | Some(libc::EBUSY)
                    | Some(libc::EPERM)
            ) || error
                .to_string()
                .contains("Resource temporarily unavailable")
                || error.to_string().contains("11"),
            "Unexpected error for lock conflict: {:?}, system error: {}",
            error.raw_os_error(),
            error
        );
        drop(guard);
        DirectoryLock::acquire(dir.path()).unwrap();
    }

    #[test]
    fn effective_memory_limit_uses_process_path_and_all_visible_ancestors() {
        for v2 in [true, false] {
            let (_dir, groups, mounts) = fixture(v2);
            assert_eq!(
                effective_memory_limit(&groups, &mounts).unwrap(),
                Some(4096)
            );
        }
        let (dir, groups, mounts) = fixture(true);
        std::fs::write(dir.path().join("team/service/memory.max"), "max").unwrap();
        assert_eq!(
            effective_memory_limit(&groups, &mounts).unwrap(),
            Some(4096)
        );
        std::fs::write(dir.path().join("team/memory.max"), "max").unwrap();
        std::fs::write(dir.path().join("memory.max"), "max").unwrap();
        assert_eq!(effective_memory_limit(&groups, &mounts).unwrap(), None);
    }

    #[test]
    fn invalid_and_unreadable_memory_limits_are_errors() {
        let (dir, groups, mounts) = fixture(true);
        std::fs::write(dir.path().join("team/memory.max"), "invalid").unwrap();
        assert_eq!(
            effective_memory_limit(&groups, &mounts).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        std::fs::remove_file(dir.path().join("team/memory.max")).unwrap();
        assert_eq!(
            effective_memory_limit(&groups, &mounts).unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }

    #[test]
    fn resolves_mount_roots_escapes_and_hybrid_memory_controller() {
        let mounts = "29 23 0:26 /tenant /cgroup\\040mount rw - cgroup2 cgroup rw";
        assert_eq!(
            resolve_memory_cgroup("0::/tenant/service", mounts).unwrap(),
            (
                PathBuf::from("/cgroup mount/service"),
                PathBuf::from("/cgroup mount"),
                true
            )
        );
        let mounts = format!("{mounts}\n30 23 0:27 / /v1 rw - cgroup cgroup rw,memory");
        assert_eq!(
            resolve_memory_cgroup("0::/tenant/service\n5:memory:/legacy", &mounts).unwrap(),
            (PathBuf::from("/v1/legacy"), PathBuf::from("/v1"), false)
        );
        assert!(resolve_memory_cgroup("0::/../service", &mounts).is_err());
    }
}
