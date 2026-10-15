//! Best-effort lowering of the uploader I/O thread's I/O scheduling priority.
//!
//! Uploads are background work: they must not compete with the commit path for
//! device bandwidth (RAN-130 scope). [`set_current_thread_priority`] wraps
//! `ioprio_set(2)` for the calling thread, which is why the worker applies it from
//! inside its own thread - `IOPRIO_WHO_PROCESS` with `who = 0` targets the calling
//! thread in the kernel. Deployments that cannot grant the capability, or that
//! prefer to express the isolation with cgroup v2 `io.weight`, use
//! [`IoPriority::Unchanged`].

use std::io;

/// `ioprio_set(2)` selectors and classes from `linux/ioprio.h`; the libc crate
/// exposes the syscall numbers but not these symbols.
const IOPRIO_WHO_PROCESS: libc::c_long = 1;
const IOPRIO_CLASS_SHIFT: u32 = 13;
const IOPRIO_CLASS_BE: u32 = 2;
const IOPRIO_CLASS_IDLE: u32 = 3;
const IOPRIO_PRIO_MASK: u32 = (1 << IOPRIO_CLASS_SHIFT) - 1;

/// I/O scheduling priority for the uploader's worker thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoPriority {
    /// Keep the inherited priority; use cgroup v2 `io.weight` for isolation.
    Unchanged,
    /// Best-effort class with a level from 0 (highest) to 7 (lowest).
    BestEffort(u8),
    /// Only served while the device is otherwise idle.
    Idle,
}

impl Default for IoPriority {
    /// The uploader is background work: best-effort at the lowest level.
    fn default() -> Self {
        Self::BestEffort(7)
    }
}

impl IoPriority {
    /// Kernel encoding of this priority, or `None` when nothing must change.
    fn value(self) -> Option<u32> {
        match self {
            IoPriority::Unchanged => None,
            IoPriority::BestEffort(level) => {
                Some((IOPRIO_CLASS_BE << IOPRIO_CLASS_SHIFT) | u32::from(level.min(7)))
            }
            IoPriority::Idle => Some(IOPRIO_CLASS_IDLE << IOPRIO_CLASS_SHIFT),
        }
    }

    /// Decode a kernel priority value. Real-time priorities are reported as
    /// [`IoPriority::Unchanged`] because the uploader never requests them.
    fn from_value(value: u32) -> Self {
        match (value >> IOPRIO_CLASS_SHIFT) & 0x7 {
            IOPRIO_CLASS_BE => Self::BestEffort(((value & IOPRIO_PRIO_MASK) as u8).min(7)),
            IOPRIO_CLASS_IDLE => Self::Idle,
            _ => Self::Unchanged,
        }
    }
}

/// Apply `priority` to the calling thread.
///
/// An error means the kernel refused the change; `EPERM`/`EACCES` are common in
/// containers without I/O priority privileges, and callers are expected to log and
/// continue rather than fail.
pub fn set_current_thread_priority(priority: IoPriority) -> io::Result<()> {
    let Some(value) = priority.value() else {
        return Ok(());
    };
    // SAFETY: `ioprio_set` takes integer arguments only and does not access memory
    // owned by this process.
    let result = unsafe {
        libc::syscall(
            libc::SYS_ioprio_set,
            IOPRIO_WHO_PROCESS,
            0,
            libc::c_long::from(value),
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The calling thread's current I/O priority.
pub fn current_thread_priority() -> io::Result<IoPriority> {
    // SAFETY: as above; `ioprio_get` returns the encoded priority or -1.
    let result = unsafe { libc::syscall(libc::SYS_ioprio_get, IOPRIO_WHO_PROCESS, 0) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(IoPriority::from_value(result as u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_values_use_the_linux_encoding() {
        assert_eq!(
            IoPriority::BestEffort(7).value(),
            Some((IOPRIO_CLASS_BE << IOPRIO_CLASS_SHIFT) | 7)
        );
        assert_eq!(
            IoPriority::BestEffort(0).value(),
            Some(IOPRIO_CLASS_BE << IOPRIO_CLASS_SHIFT)
        );
        assert_eq!(
            IoPriority::Idle.value(),
            Some(IOPRIO_CLASS_IDLE << IOPRIO_CLASS_SHIFT)
        );
        assert_eq!(IoPriority::Unchanged.value(), None);
        assert_eq!(IoPriority::default(), IoPriority::BestEffort(7));

        assert_eq!(
            IoPriority::from_value((IOPRIO_CLASS_BE << IOPRIO_CLASS_SHIFT) | 5),
            IoPriority::BestEffort(5)
        );
        assert_eq!(
            IoPriority::from_value(IOPRIO_CLASS_IDLE << IOPRIO_CLASS_SHIFT),
            IoPriority::Idle
        );
        assert_eq!(
            IoPriority::from_value(0),
            IoPriority::Unchanged,
            "class 0 (none) means the inherited priority"
        );
        // Out-of-range levels are clamped to the best-effort range.
        assert_eq!(
            IoPriority::BestEffort(9).value(),
            Some((IOPRIO_CLASS_BE << IOPRIO_CLASS_SHIFT) | 7)
        );
    }

    #[test]
    fn unchanged_is_a_no_op_and_best_effort_round_trips_when_permitted() {
        let before = match current_thread_priority() {
            Ok(priority) => priority,
            Err(error) => {
                eprintln!("skipping: ioprio_get is not permitted here: {error}");
                return;
            }
        };

        set_current_thread_priority(IoPriority::Unchanged).unwrap();
        assert_eq!(current_thread_priority().unwrap(), before);

        let target = if before == IoPriority::BestEffort(6) {
            IoPriority::BestEffort(5)
        } else {
            IoPriority::BestEffort(6)
        };
        match set_current_thread_priority(target) {
            Ok(()) => assert_eq!(current_thread_priority().unwrap(), target),
            Err(error) => {
                eprintln!("skipping the round trip: ioprio_set is not permitted here: {error}");
            }
        }

        let _ = set_current_thread_priority(before);
    }
}
