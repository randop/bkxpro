//! Linux-only io_uring storage I/O and host preflight for RAN-134.
//!
//! Production writes use `O_DIRECT`, registered (fixed) buffers, registered files,
//! and explicit write -> `fdatasync` ordering. There is intentionally no buffered or
//! `pwrite` fallback. A failed I/O or sync poisons the ring adapter; callers must stop
//! acknowledging writes and restart through recovery.

#[cfg(not(target_os = "linux"))]
compile_error!("bkxpro-linux-io supports Linux only; develop and test in a Linux VM/devcontainer");

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(target_os = "linux")]
pub mod simulated;
