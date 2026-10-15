# bkxpro - BookKeeper process, a data plane microservice

Project: P-RAN-8

## Summary

Clean-sheet Rust log store (BookKeeper https://github.com/apache/bookkeeper.git  as architecture reference), S3 as source of truth. Staging tier: segment files + fjall index + foyer cache.

## Overview

**Platform: Linux only.** This is a Linux-only microservice that deliberately uses OS-level performance and low-overhead facilities (io_uring, O_DIRECT, registered buffers, cgroup v2, and similar) instead of portable abstractions. There is no portability layer; non-Linux targets fail to compile. See "Linux platform and I/O strategy" below.

Clean-sheet Rust rewrite of a BookKeeper-style log store. No Java compatibility, no ZooKeeper/Raft. S3 is the source of truth for data and metadata (manifest log with conditional-write CAS, writer epochs for fencing).

This project covers the **local staging tier** for in-flight data before S3 upload.

## Key decisions

- No protobuf on the hot path: fixed `#[repr(C)]` little-endian frames parsed with `zerocopy`, CRC32C integrity.
- Segment file format is identical to the S3 batch object, so upload is a streaming copy with no re-encoding.
- Payloads live in append-only segment files (O_DIRECT via io_uring, 4 KiB-aligned blocks), not in an LSM.
- Index in **fjall** (derived data, rebuildable from segment footers). Read cache in **foyer** (memory + NVMe tiers).
- Segments are deleted only after the S3 PUT succeeds and the manifest CAS commit lands.
- Linux only: io_uring is the primary I/O interface for the commit path, with no portable fallback. The storage path sits behind a narrow I/O trait with a production io_uring backend and a deterministic simulated backend for testing (RAN-134).
- Write or `fdatasync` failures are fail-stop: no retry, no ack, restart and recover from disk.

## Linux platform and I/O strategy

- **io_uring first:** segment writes, aligned reads, and uploader file reads go through io_uring with O_DIRECT, registered (fixed) buffers and files, and explicit write-then-`fdatasync` ordering (linked or drained SQEs). Per-write `RWF_DSYNC` is evaluated against separate `fdatasync` per device class during group-commit tuning (RAN-127).
- **Proposed baseline:** Linux 6.1 LTS or newer (adds `STATX_DIOALIGN`, `SINGLE_ISSUER`, `DEFER_TASKRUN`), ratified in RAN-134. Features are capability-probed at startup; the service fails fast with an actionable error rather than silently degrading.
- **Shared-nothing, thread-per-core (proposed):** each shard owns its ring, open segment, aligned buffer pool, and in-flight budget, with ledgers mapped to shards and threads pinned to cores. This affects segment IDs, the `segments` keyspace, and recovery (one open segment per shard); see Open decisions.
- **Filesystem and device:** XFS or ext4 on local NVMe or an equivalent block device. Segment files are preallocated and pre-zeroed (written, not merely `fallocate`d) so overwrite-in-place needs no filesystem metadata updates at `fdatasync`. DIO alignment comes from `statx`, never assumed. Directory `fsync` after create, rename, and unlink.
- **Memory is a cgroup concern:** budgets are measured against cgroup v2 `memory.current` (RSS, page cache, and kernel memory), not RSS alone. Registered buffers are pinned and count toward memlock/memcg limits. The buffer pool uses hugepage/THP-friendly memory where it helps. Staging and upload reads bypass the page cache (O_DIRECT or `posix_fadvise(DONTNEED)`).
- **Components outside io_uring:** `fjall` does blocking file I/O on its own threads, and `object_store` (HTTP/TLS) runs on tokio. Both stay off the ack path. `foyer` has an io_uring I/O engine for its NVMe tier (verify availability in the pinned version).
- **I/O isolation:** compaction, cache fills, and uploads must not inflate commit latency. Use separate directories or devices where possible, otherwise `ioprio_set` or cgroup v2 `io.weight`.
- **Deployment prerequisites:** io_uring permitted (`kernel.io_uring_disabled`, container seccomp profile), sufficient `RLIMIT_MEMLOCK`, and an O_DIRECT-capable filesystem. All validated by a startup preflight check (RAN-134).
- **Testing on Linux:** a deterministic simulated I/O backend, plus kernel-level crash and fault testing on real XFS and ext4 (`dm-log-writes`, `dm-flakey`, `null_blk` fault options), and a CI matrix over the minimum supported and latest LTS kernels. Device ceilings are baselined with `fio --ioengine=io_uring`.

## Segment layout

`[Header 4 KiB] [Block]* [Footer]`

- Block: header (record count, CRC32C) + packed records, zero padded to 4 KiB.
- Record: `len u32 | ledger u64 | entry u64 | flags u8 | payload`.
- Footer (at seal): sorted `(ledger, entry, offset, len)` table, per-ledger min/max summary, footer CRC, fixed-size trailer.
- Lifecycle: `open -> sealed -> uploading -> uploaded -> deleted`.

## fjall keyspaces

- `entries`: `ledger BE || entry BE` -> `segment u32 || offset u32 || len u32`
- `ledgers`: `ledger BE` -> epoch, LAC, state
- `segments`: `segment BE` -> state, S3 key, ledger range, size

## Memory budget (example, 2 GB cap)

Write buffers 256 MB, fjall 128 MB, foyer memory tier 512 MB, foyer NVMe tier disk-sized, remainder headroom for allocator and stacks. In-flight bytes are gated by a bounded semaphore.

On Linux the cap is enforced and measured against cgroup v2 `memory.current` (RSS, page cache, and kernel memory). Pinned io_uring registered buffers are carved out of the write-buffer allocation, not added on top; ring and other kernel-side memory sit in the headroom.

## Build order

0. Linux I/O foundation (RAN-134; runs in parallel with the codec and gates the segment writer and recovery), 1. Codec, 2. Segment writer, 3. Recovery and power-loss sim, 4. fjall index, 5. Uploader, 6. foyer and memory benchmarks, 7. Battle-testing harness (RAN-132; starts alongside step 3 and runs continuously).

## Open decisions

- **Runtime:** thin in-house io_uring reactor vs `glommio` / `monoio` / `compio` (RAN-134 spike).
- **Sharding:** how shard identity is encoded in segment IDs and keyspaces, and how ledgers map to shards. Affects footers, recovery (one open segment per shard), and the fjall layout.
- **Minimum kernel:** 6.1 LTS proposed. Is any target environment stuck on an older kernel?
- **Production fallback:** fail fast (proposed) vs a `pwrite` + `fdatasync` fallback for environments where io_uring is blocked.
- **Group-commit primitive:** write + linked `fdatasync` vs `RWF_DSYNC`, decided per device class from the fio baseline.
- **Upload path:** whether kernel TLS or zero-copy send is worth evaluating later. Not planned initially.

**Assisted By:** Anthropic Claude Sonnet 5.5

---

# Issues

## RAN-134 — Linux I/O foundation: io_uring runtime decision, I/O layer, and host preflight

Status: Backlog

Establish the Linux-only I/O foundation that the segment writer, recovery, uploader, and cache build on. The service targets Linux exclusively and uses OS-level, low-overhead primitives (io_uring first). There is no portability layer and no non-Linux support.

### Scope

- **Platform gate:** `compile_error!` on non-Linux targets, with no portability shims. Development happens in a Linux VM or devcontainer.
- **Runtime spike (time-boxed):** compare a thin in-house reactor on the `io-uring` crate against `glommio`, `monoio`, and `compio` (with `tokio-uring` for reference). Produce a short decision record covering:
  - O_DIRECT file I/O with registered (fixed) buffers and registered files
  - Explicit write-then-`fdatasync` ordering (linked or drained SQEs) and `RWF_DSYNC` support
  - Thread-per-core, shared-nothing sharding and CPU pinning
  - Coexistence with the tokio-based `object_store` for S3, and with fjall and foyer background threads
  - Not precluding an io_uring network frontend later (the frontend itself is out of scope for this project)
- **I/O trait:** a narrow abstraction (open, preallocate and pre-zero, aligned write, datasync, aligned read, truncate, unlink, directory sync) with two backends: production io_uring, and a deterministic simulated backend used by RAN-128 and RAN-132. `turmoil` and `madsim` do not model io_uring semantics, so the storage path needs its own seam.
- **Ring configuration:** evaluate `SINGLE_ISSUER`, `COOP_TASKRUN` / `DEFER_TASKRUN`, and SQPOLL (costs a dedicated core); ring sizing; handling of short writes, `EAGAIN`, `-ECANCELED` on broken linked chains, and CQ overflow.
- **Capability probe:** at startup, probe the needed opcodes (`IORING_REGISTER_PROBE`) and check kernel version, the `kernel.io_uring_disabled` sysctl, container seccomp policy, and `RLIMIT_MEMLOCK` / memcg headroom for registered buffers.
- **Host preflight (fail fast with actionable errors):**
  - Data directory filesystem is supported (XFS or ext4; others unsupported until tested) and accepts O_DIRECT, verified by a real probe write
  - Device DIO alignment from `statx` (`STATX_DIOALIGN`) is at most the 4 KiB format block
  - Volatile write cache state read from `/sys/block/<dev>/queue/write_cache` and reported
  - Single-process lock on the data directory (OFD lock, `F_OFD_SETLK`)
  - cgroup v2 `memory.max` reconciled with the configured memory budget
- **Failure policy:** write or `fdatasync` errors are fail-stop. No retry, no ack; restart and recover from disk, because a failed fsync can leave dirty pages marked clean.
- **Baseline:** record device ceilings with `fio --ioengine=io_uring --direct=1` (including fdatasync and dsync variants) on each target environment, to anchor the RAN-127 targets.

### Proposed baseline (to ratify here)

- Linux 6.1 LTS or newer (provides `STATX_DIOALIGN`, `SINGLE_ISSUER`, `DEFER_TASKRUN`).
- No production fallback path: if io_uring or O_DIRECT is unavailable, the service refuses to start.

### Acceptance criteria

- Decision record merged: runtime, minimum kernel, supported filesystems, ring flags, sharding model.
- Non-Linux builds fail at compile time with a clear message.
- Preflight check runs at startup and as a standalone subcommand; each failure names the fix.
- Simulated and io_uring backends pass the same conformance suite for the I/O trait.
- fio baseline results documented per target environment.

**Assisted By:** Anthropic Claude Sonnet 5.5

---

## RAN-132 — Battle-testing harness: simulation, fault injection, fuzzing, soak

Status: Backlog

Develop the battle-testing program for the staging tier and its S3 interactions. Goal: find durability, ordering, and memory bugs before production, using deterministic and adversarial testing.

### Scope

- **Deterministic simulation:** run the writer, uploader, and recovery under `turmoil` or `madsim` against a fault-injecting mock S3 (latency spikes, partial failures, lost responses, etag races, CAS conflicts, fenced epochs). Seeds must be reproducible. The storage path runs on the deterministic backend of the RAN-134 I/O trait, because `turmoil` and `madsim` do not model io_uring submission and completion semantics (completion reordering, short writes, delayed CQEs).
- **Crash and power-loss testing:** extend the RAN-128 simulator to random kill points across the full lifecycle (`open -> sealed -> uploading -> uploaded -> deleted`), including crashes mid-upload and between manifest commit and local delete.
- **Disk faults:** torn writes, bit flips, ENOSPC, EIO, slow fsync, and read-only remount.
- **Kernel-level faults on real filesystems:** `dm-log-writes` replay to arbitrary points, `dm-flakey` and `dm-dust` fault windows, `null_blk` fault options, and block-layer fault injection (`fail_make_request`), on both XFS and ext4. This validates the simulator against the real kernel.
- **io_uring edge cases:** SQ/CQ overflow, short reads and writes, `EAGAIN`, `-ECANCELED` on a failed linked chain (write then `fdatasync`), ring teardown with I/O in flight, `RLIMIT_MEMLOCK` exhaustion, and io_uring disabled or blocked at startup (the preflight must fail clearly).
- **Kernel and filesystem matrix:** CI on the minimum supported kernel and the latest LTS, on XFS and ext4, with local NVMe and network-attached block storage.
- **Fuzzing:** `cargo-fuzz` on the codec and recovery parsers with corpus checked in; run continuously in CI.
- **Concurrency:** `loom` or `shuttle` models for the buffer pool, group-commit loop, and in-flight semaphore.
- **Model checking:** property tests (`proptest`) against a simple reference model of ledger semantics (append, read, fence, LAC).
- **Soak and chaos:** multi-day soak runs with periodic process kills, S3 outage and backlog growth, and memory-pressure limits (cgroup caps), tracking RSS, cgroup `memory.current`, locked/pinned memory, fd count, and disk usage for leaks.
- **Real S3:** scheduled chaos runs against real S3 and S3 Express to validate the mock's behavior.
- **Reporting:** failing seeds auto-filed with a repro command; dashboard of pass rate and coverage over time.

### Invariants to assert

- No acknowledged entry is ever lost.
- No torn or duplicate record is ever surfaced.
- A fenced writer can never commit.
- Local segments are deleted only after the PUT and manifest commit succeed.
- RSS and cgroup `memory.current` stay within the configured budget.
- A write or `fdatasync` failure never produces an ack.

### Acceptance criteria

- Simulation suite runs in CI on every PR with a fixed seed set, and a nightly run with random seeds.
- At least one multi-day soak completes with zero invariant violations.
- Every bug found gets a regression seed or test checked in.
- Runbook documents how to reproduce a failure from a seed.
- The suite passes on the minimum supported kernel and the latest LTS, on both XFS and ext4.

**Assisted By:** Anthropic Claude Sonnet 5.5

---

## RAN-131 — foyer read cache integration and RSS-under-load benchmarks

Status: Backlog

Integrate the foyer hybrid cache for the read path and benchmark memory under load.

### Read path

1. foyer memory tier.
2. Open segment in-memory index, then fjall index to locate segment and offset.
3. Aligned O_DIRECT read from the local segment through the io_uring path (fixed buffers), or ranged S3 GET if the segment is deleted.
4. Fill foyer. The NVMe tier is a separate directory from staging so eviction never touches staging data.

### Memory budget (example, 2 GB cap)

Write buffers 256 MB, fjall 128 MB, foyer memory tier 512 MB, foyer NVMe tier disk-sized, remainder headroom.

The cap is enforced and measured against cgroup v2 `memory.current` (RSS, page cache, and kernel memory), not RSS alone. Pinned io_uring registered buffers are part of the write-buffer allocation, not additional.

### Scope

- Byte-budgeted (not entry-counted) cache tiers.
- Allocator evaluation (jemalloc/mimalloc settings) and hugepage/THP settings for fragmentation.
- foyer NVMe tier I/O engine: evaluate foyer's io_uring engine against psync (verify availability in the pinned version), using direct I/O on a directory separate from staging. Set `ioprio` or cgroup v2 `io.weight` so cache fills cannot hurt staging commit latency.
- RSS-under-load benchmark suite: sustained writes, mixed read/write, S3 outage with backlog growth. Run all scenarios inside a cgroup v2 with a real `memory.max`.

### Acceptance criteria

- cgroup v2 `memory.current` (RSS, page cache, and kernel memory) stays under the configured cap in all benchmark scenarios, with zero OOM kills and no sustained `memory.high` throttling (checked via `memory.events`). RSS is reported alongside.
- Cache hit-rate and p99 read latency reported per tier.
- Documented tuning guide for the memory budget.

**Assisted By:** Anthropic Claude Sonnet 5.5

---

## RAN-130 — Uploader with manifest-gated segment deletion

Status: Backlog

Build the uploader that streams sealed segments to S3 and gates local deletion on the manifest commit.

### Scope

- Stream sealed segment files as one PUT (multipart for large segments) via `object_store`, with no re-encoding.
- Retry with backoff; resume or restart uploads after crash.
- Delete the local segment only after the PUT succeeds **and** the manifest CAS commit lands.
- Update segment state (`uploading -> uploaded -> deleted`) and index cleanup atomically with deletion.
- Fall back to ranged S3 GET for reads once the local segment is deleted.
- Read sealed segments with large sequential aligned reads through io_uring (O_DIRECT, or buffered reads with `posix_fadvise(DONTNEED)`), drawing from a bounded buffer budget, so uploads do not pollute the page cache or break the memory cap. Lower the uploader's I/O priority (`ioprio_set` or cgroup v2 `io.weight`) relative to the commit path.
- Bridge the io_uring file reads and the tokio-based `object_store` client with a bounded channel. S3 and TLS traffic stays on tokio threads, off the ack path.
- Local deletion is `unlink` followed by a directory `fsync`, or recycle into the writer's pool (a decision shared with RAN-127).
- Out of scope for now: kernel TLS and zero-copy send. Revisit if profiling shows the upload path is CPU-bound.

### Acceptance criteria

- Crash at any point in upload/commit/delete never loses data or leaks an undeletable segment.
- Fault-injected tests (failed PUT, lost response, CAS conflict, fenced epoch) pass.
- Uploader backpressure feeds the in-flight bytes semaphore.
- Page cache growth attributable to uploads stays within budget (checked via cgroup v2 `memory.stat`).

**Assisted By:** Anthropic Claude Sonnet 5.5

---

## RAN-129 — fjall index and rebuild-from-segments

Status: Backlog

Build the fjall-backed index and the rebuild-from-segments path. The index is derived data; segments are the source of truth.

### Keyspaces

- `entries`: `ledger u64 BE || entry u64 BE` -> `segment u32 || offset u32 || len u32`
- `ledgers`: `ledger u64 BE` -> epoch, LAC, state (open/fenced/closed)
- `segments`: `segment u64 BE` -> state, S3 key, ledger range, byte size

### Scope

- Small fjall journal and relaxed durability, with hard-capped cache and write buffers (\~128 MB in the example budget).
- Rebuild index by scanning sealed segment footers plus the open segment.
- Cleanup on segment deletion via one write batch bounded by the `segments` row.
- Evaluate: skip per-entry index writes for sealed segments and resolve from cached footers, keeping fjall for the open segment and ledger state.
- **I/O model:** fjall does blocking file I/O on its own background threads and, as far as we know, does not use io_uring (verify for the pinned version). Keep it off the commit/ack path: acks never wait on fjall, and the open-segment in-memory index serves fresh reads. Set its thread and fd limits explicitly.
- **I/O isolation:** place the fjall directory on a separate device when possible. Otherwise lower its I/O priority (`ioprio_set` or cgroup v2 `io.weight`) so compaction and flush traffic cannot inflate segment `fdatasync` latency.
- Account for fjall's page-cache footprint (buffered I/O) in the cgroup-based memory budget, and evaluate `posix_fadvise` hints.

### Acceptance criteria

- Deleting the index and restarting reproduces identical lookups.
- Range scans per ledger are ordered and correct.
- fjall memory (RSS plus page cache attributable to the index directory) stays within budget, measured at the cgroup level, with millions of in-flight entries.

**Assisted By:** Anthropic Claude Sonnet 5.5

---

## RAN-128 — Recovery scanner and power-loss simulation

Status: Backlog

Implement crash recovery for the open segment and a power-loss simulation harness. Build the simulation first.

### Scope

- Recovery scanner: scan blocks from the head, stop at the first bad CRC or zero block, truncate there. Only the open segment needs a scan.
- Power-loss simulator: fault-injecting file layer that drops, reorders, or tears unsynced writes at arbitrary points.
- Verify sealed segments via footer and trailer CRC.
- The simulator implements the RAN-134 I/O trait, so the production code path (including io_uring-style completion reordering and short writes) is what gets tested, reproducible by seed.
- Validate the simulator against the real kernel: replay recorded writes with `dm-log-writes` to arbitrary points on XFS and ext4, plus `dm-flakey` fault windows, and compare recovery results.
- Recovery reads use large sequential aligned reads (O_DIRECT) on the ring. After a crash, reconcile the data directory: partially created or pre-zeroed segments, orphaned recycled files, and unsynced directory entries.

### Acceptance criteria

- No acknowledged entry is ever lost across randomized crash points.
- No torn or partial record is ever surfaced after recovery.
- Recovery time is bounded by one open segment (per shard, with shards recovering in parallel, if sharding is adopted in RAN-134).
- Recovery outcomes agree between the simulated backend and real XFS/ext4 crash replays; any disagreement is treated as a simulator bug.

**Assisted By:** Anthropic Claude Sonnet 5.5

---

## RAN-127 — Segment writer with group commit and aligned buffer pool

Status: Backlog

Build the segment writer with a group-commit loop and an aligned buffer pool.

### Scope

- Preallocated **and pre-zeroed** segments. `fallocate` alone leaves unwritten extents whose first write needs a metadata update at `fdatasync`, so write zeros once and recycle files (rather than unlink and recreate) so steady-state overwrites are metadata-free. Directory `fsync` after create, rename, and unlink.
- Submit O_DIRECT writes of 4 KiB-aligned blocks through io_uring using registered (fixed) buffers and registered files (RAN-134 I/O layer); group commit tuned to device fsync latency.
- Make durability ordering explicit: link (or drain) the write SQEs to the `fdatasync`, since io_uring does not order an fsync after earlier writes by default. Evaluate `RWF_DSYNC` (may map to FUA on devices that support it) against write + `fdatasync` per device class, and pick by measurement.
- Write or `fdatasync` errors are fail-stop: no retry, no ack; restart and recover from disk.
- The group-commit loop runs on its shard's ring (thread-per-core, pinned if sharding is adopted in RAN-134), with no cross-thread handoff on the ack path.
- Pooled aligned buffers backed by hugepage/THP-friendly memory and registered with the ring; no per-entry `Vec`.
- Bounded in-flight bytes semaphore so writers block instead of buffering.
- Seal at size target (32-64 MB) or flush timer, whichever comes first; write footer at seal.
- Lifecycle states: `open -> sealed -> uploading -> uploaded -> deleted`.

### Acceptance criteria

- Ack only after the block containing the entry is durable.
- Write buffer memory (including pinned registered buffers) stays within the configured budget under sustained load, measured at the cgroup level.
- Benchmarks for throughput and p50/p99 commit latency.
- Benchmarks are compared against a `fio --ioengine=io_uring --direct=1` baseline on the same device; the target gap is set once the RAN-134 baseline exists.
- Syscalls per committed batch are measured and reported (goal: roughly one `io_uring_enter` per batch).
- In steady state `fdatasync` issues no filesystem journal commits (verified, for example with `perf` or `blktrace`).
- Behavior verified on both XFS and ext4.

**Assisted By:** Anthropic Claude Sonnet 5.5

---

## RAN-126 — Frame, block, and segment codec

Status: Done

Implement the on-disk and on-wire codec for the staging tier. The segment format doubles as the S3 batch object.

### Scope

- Segment header (4 KiB): magic, format version, segment ID, writer epoch, target size, creation time, header CRC.
- Block (4 KiB aligned): block header (record count, CRC32C) + packed records, zero padded.
- Record: `len u32 | ledger u64 | entry u64 | flags u8 | payload`, little-endian, `#[repr(C)]` + `zerocopy`. Entries larger than a block span consecutive blocks.
- Footer written at seal: sorted `(ledger, entry, offset, len)` table, per-ledger min/max summary, footer CRC, fixed-size trailer pointing to footer start.
- **Alignment and Linux I/O:** the format fixes 4 KiB blocks (a superset of 512 B and 4 KiB sector devices). The runtime verifies device O_DIRECT alignment via `statx` (`STATX_DIOALIGN`) in the RAN-134 preflight and refuses to start if it exceeds 4 KiB.
- **Encode into I/O buffers:** the encoder writes directly into caller-provided 4 KiB-aligned buffers from the pool (usable as io_uring registered buffers), with no intermediate copy. CRC32C uses the hardware-accelerated path.

### Acceptance criteria

- Round-trip encode/decode with no per-entry allocation (`bytes::Bytes` slices).
- Fuzz tests (`cargo-fuzz`) on block, record, and footer parsers.
- Crash-truncation tests: truncating at any byte offset yields a clean recovery point.
- Format spec documented in the repo.
- Encoder output lands in aligned pool buffers with no extra copy (verified in a benchmark).
- CRC32C throughput benchmarked on the hardware-accelerated path.

**Assisted By:** Anthropic Claude Sonnet 5.5
