# ADR 0001: Linux I/O runtime and host preflight

- **Status:** Initial implementation; target-device fio evidence pending.
- **Scope:** local staging data path only. S3/TLS stays on Tokio/object_store and is not part of the acknowledgement path.

## Decision

Use a thin in-house synchronous shard reactor based on `io-uring` 0.7, wrapped by the narrow `SegmentIo` trait consumed by the segment writer. Do not introduce a portable backend. Non-Linux targets fail to compile.

Each shard owns exactly one ring and submits from one fixed issuer thread. Start with `SINGLE_ISSUER` and `DEFER_TASKRUN` (both require the selected Linux 6.1 baseline). SQPOLL and IOPOLL are not enabled by default: SQPOLL reserves a CPU, and IOPOLL has device/filesystem restrictions. Evaluate them only as measured variants. Register fixed buffers and fixed files; probe required opcodes at startup. Fail if a required capability is absent.

The selected durability primitive is independent fixed-buffer writes followed by an `FSYNC`/`fdatasync` SQE carrying `IOSQE_IO_DRAIN`. This starts writes without chaining every write to the next and guarantees the sync SQE does not start until earlier SQEs have completed. Keep `RWF_DSYNC` as a selectable benchmark mode; do not select it per device until measurements are captured. A write or sync error, short data operation, or unexpected completion poisons the adapter; no automatic retries and no acknowledgements.

The minimum kernel is Linux 6.1 LTS. Only XFS and ext4 are supported for the data directory in this version. Require `STATX_DIOALIGN` and fail unless both DIO memory and offset alignments are non-zero, <= 4096 bytes, and divide 4096. Require a real O_DIRECT ring write probe, a readable block device `queue/write_cache` value, `io_uring` permitted by sysctl/seccomp, sufficient `RLIMIT_MEMLOCK`, and a finite cgroup v2 `memory.max` large enough for the configured service budget.

Lock the data directory using `F_OFD_SETLK` on `.bkxpro.lock`. Keep the lock descriptor open for the process lifetime. The preflight executable runs the same checks as the service API; a passing one-shot command is only a deployment diagnostic, not a substitute for retaining the lock in the live process.

## Sharding

The implementation is one-ring-per-shard and enforces the ring owner's thread at runtime. Shard-to-ledger mapping is owned by the service layer, outside this adapter. Segment IDs remain caller-supplied `u64` values; reserving shard bits in the segment ID is intentionally left as a cross-component decision with recovery, fjall, and uploader rather than encoded in the I/O layer.

## Runtime alternatives

- **glommio / monoio / compio:** valid alternatives for thread-per-core asynchronous runtimes, but adopting one would couple storage ownership and integration to a larger runtime before the narrow I/O conformance boundary is established.
- **tokio-uring:** keeps a Tokio-compatible programming model, but the file path must not inherit Tokio scheduling or cross-thread handoffs on the acknowledgement path. The planned S3 client already runs on Tokio, so it remains outside the storage runtime.
- **in-house `io-uring` crate wrapper (chosen):** explicit fixed buffer/file slots, direct control over SQE order, no hidden fallback, and a test seam usable by a deterministic simulated `SegmentIo` backend.

## Filesystem setup and recovery contracts

Segment files are opened `O_DIRECT`, created/renamed/unlinked with parent directory `fsync`, preallocated with `fallocate`, and fully pre-zeroed by the segment writer before first use. The io_uring adapter does not acknowledge a block until every write completion and a following successful `fdatasync` have been observed. Truncation uses `ftruncate(2)` because `IORING_OP_FTRUNCATE` is newer than Linux 6.1, then syncs the size change through the ring. No commit-path `pwrite`/buffered fallback exists.

## Measurement plan

On each target machine, use `scripts/fio-baseline.sh` against a dedicated test directory on the exact staging device. Capture fio JSON for direct io_uring writes with no sync, `fsync=N`, `fdatasync=N`, and `dsync` variants; additionally measure `RwfDsync` through the application because fio's sync modes are comparison baselines, not an exact emulation of the app's `RWF_DSYNC` SQEs. Capture kernel version, filesystem, device model, queue/write-cache state, fio version, and test parameters with the output. Compare throughput and p50/p99 commit latency and count `io_uring_enter` using `perf trace`/`strace`. Use results to choose the per-device-class mode and define the segment writer's target gap.

| Environment | Kernel / FS / device | Result | Status |
|---|---|---|---|
| Current build environment | kernel info available, but not a qualified XFS/ext4 block-device setup; Rust toolchain absent | No benchmark run | Pending |
| Each production/staging target | Record during execution | Attach fio JSON and app benchmark report | Pending |

## Failure behavior

If io_uring is disabled or blocked, a required opcode is missing, DIO alignment is missing or incompatible, the filesystem is not XFS/ext4, cgroup/memlock budgets do not fit, the data directory is already locked, or the real O_DIRECT probe fails, startup exits with an actionable error. Runtime data-path errors poison the ring; operators restart and recover from durable segment bytes. Do not retry or issue an acknowledgement after a write/sync error.
