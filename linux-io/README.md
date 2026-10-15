# bkxpro-linux-io (RAN-134)

Linux-only host preflight and production `SegmentIo` backend for RAN-127. The crate does not provide a buffered or `pwrite` fallback. Non-Linux builds fail with a clear compile-time error.

## Runtime decision

The first implementation uses a thin in-house reactor around `io-uring` 0.7 rather than adopting `glommio`, `monoio`, `compio`, or `tokio-uring` as the storage runtime. That keeps shard ownership explicit and limits the hot-path interface to a small set of fixed-buffer/file operations. The S3 client can continue running on Tokio outside the acknowledgement path. See [the decision record](../docs/rfc/0001-linux-io-runtime.md).

Each shard owns one `UringSegmentIo`, one ring, a fixed-file table, a fixed-buffer table, and its memory/in-flight budget. A ring is restricted to the thread that constructed it. SQPOLL is off by default because it reserves CPU; measure it separately before enabling it.

## Preflight

Run against the actual data directory/filesystem during deployment and retain the resulting `PreflightResult` for the lifetime of the service:

```rust,no_run
use bkxpro_linux_io::{run_host_preflight, HostPreflightConfig};

let config = HostPreflightConfig {
    data_dir: "/var/lib/bkxpro".into(),
    ..HostPreflightConfig::default()
};
let preflight = run_host_preflight(&config)?;
// Keep `preflight` (especially `directory_lock`) alive for the process lifetime.
// Construct shard rings only after these checks succeed.
# Ok::<(), Box<dyn std::error::Error>>(())
```

The standalone `bkxpro-preflight` binary performs the same checks. It creates `.bkxpro.lock`, acquires an OFD write lock, then checks. Set the container/service memlock limit high enough for the registered buffer pool; the default 256 MiB pool intentionally fails preflight on hosts with the common 64 KiB soft limit.

- Linux 6.1+, `/proc/sys/kernel/io_uring_disabled`, and actual ring creation (which also catches container seccomp/permission denials).
- Required io_uring opcodes, fixed-buffer/file table registration, `SINGLE_ISSUER` and `DEFER_TASKRUN`.
- A finite cgroup v2 memory cap (including visible ancestors), `memory.current` plus registered-buffer headroom against the service budget, and `RLIMIT_MEMLOCK` headroom after current `VmLck` usage.
- XFS or ext4, `STATX_DIOALIGN` returned by a real O_DIRECT probe file, both memory/offset alignment <= 4 KiB and divisors of 4 KiB, and the device's `/sys/.../queue/write_cache` state.
- A real 4 KiB aligned `O_DIRECT` `WriteFixed` and durable commit through the ring, followed by `unlinkat` and directory `fsync`.

Missing alignment metadata or inaccessible device cache information is a hard error. Alignment is never guessed. Fix the named host prerequisite instead of enabling a fallback.

## Integrating with the writer

The writer's single segment working allocation must be registered before creating `SegmentWriter`; it must remain allocated at the same address while registered. Example shape:

```rust,ignore
let mut buffer = AlignedBuffer::new_zeroed(config.buffer_capacity()?)?;
let mut io = UringSegmentIo::new(RingConfig::default())?;
// SAFETY: `buffer` outlives `io` and remains at a stable address until `io` is dropped.
unsafe { io.register_buffer(0, buffer.as_mut_ptr(), buffer.len())?; }
let writer = SegmentWriter::create(io, path, dir, header, config, &mut buffer)?;
```

The production buffer pool should eventually own and register every allocation as a set; this API deliberately requires an explicit unsafe registration boundary to make that lifetime obligation visible. File slots are updated only when their previous `RegisteredFile` lease has been dropped, so do not retain segment handles forever.

The `SegmentIo` implementation:

- Opens segment paths with `IORING_OP_OPENAT` and `O_DIRECT`, then preallocates them with Linux `ftruncate(2)`/`fallocate(2)` control operations.
- Uses `WRITE_FIXED`/`READ_FIXED` with registered files and buffers for data operations.
- Defaults to independent writes followed by a `DATASYNC` fsync SQE marked `IOSQE_IO_DRAIN`, avoiding the unnecessary serialization of every write in a linked chain. `RWF_DSYNC` is a selectable measurement mode.
- Treats any data-path short write/read, operation error, submission failure, or sync error as fail-stop. It does not retry. After a write/fsync failure the runtime remains poisoned.
- Uses a direct `ftruncate(2)` control syscall followed by io_uring `fdatasync`; `IORING_OP_FTRUNCATE` is newer than the chosen Linux 6.1 minimum.
- Uses `UNLINKAT` and an io_uring `FSYNC` on the parent directory after deletion.

## Benchmarking

`scripts/fio-baseline.sh /path/to/test-dir` runs direct io_uring baselines for no-sync, `fsync=N`, `fdatasync=N`, and `dsync` variants, writing JSON outputs outside the source tree. Use a dedicated temporary directory on the same device and filesystem as staging, never the live staging directory. Compare p50/p99 commit latency and throughput with the application benchmark, and count `io_uring_enter` calls with `perf trace` or `strace` because `RuntimeStats::submit_and_wait_calls` counts Rust submit/wait invocations, not necessarily one syscall each.

No device benchmark results are checked in yet: the provided environment has no Rust toolchain and is not a validated target XFS/ext4 device. Run the script on each target environment before selecting `RwfDsync` over the default drain + `fdatasync` mode.

## Deterministic I/O simulation

`bkxpro_linux_io::simulated::SimulatedSegmentIo` implements the same `SegmentIo` boundary for deterministic unit tests. It tracks volatile bytes separately from synchronized bytes and separates file data sync from directory-entry sync. `inject_fault` supports one-shot write errors, short writes, read errors, data-sync errors, and directory-sync errors; `simulate_crash` restores the last durable data and namespace. This backend is for tests/fault harnesses only and is not selected automatically in production.
