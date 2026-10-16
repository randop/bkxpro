# Read cache memory tuning guide

How to budget the read path's memory, run the RSS-under-load benchmark under a real
cgroup cap, and interpret the report.

Relevant code:

| Piece | Location |
| --- | --- |
| Cache configuration (`ReadCacheConfig`) | `reader/src/lib.rs` |
| Benchmark harness (4 scenarios, per-tier latency) | `reader/benches/rss_under_load.rs` |
| cgroup v2 accounting (`MemoryCgroup`, `memory.current`/`memory.max`/`memory.events`) | `io/src/cgroup.rs` |
| Registered io_uring buffers and teardown | `io/src/linux.rs` (drop unregisters buffers/files) |
| cgroup preflight for production | `io/src/linux.rs` (`run_host_preflight`, `bkxpro-preflight`) |

## Budget model

The read path's memory has four consumers:

1. **Writer encoder buffers** (`write_target_bytes`) — one aligned segment buffer per
   shard, registered with io_uring so the kernel pins it. The buffer must hold the
   whole segment: `entries_per_segment * bytes_per_entry`, where
   `bytes_per_entry ≈ ceil(BLOCK_SIZE / floor(BLOCK_DATA_SIZE / (RECORD_HEADER_SIZE + payload)))`
   plus one 4 KiB block per group commit (every `flush_group` pads the open block). For
   the 1 KiB benchmark payloads that is ~1.4 KiB per entry (three records per 4 KiB
   block). This memory is *not* RSS: it is charged against `RLIMIT_MEMLOCK` (see below).
2. **fjall index** (`index_memory_bytes`) — memtable + block cache.
3. **foyer memory tier** (`memory_tier_bytes`) — hybrid cache memory capacity; foyer's
   internal buffer pool adds `memory_tier_bytes / 2` while the tier fills.
4. **foyer NVMe tier** (`nvme_tier_bytes`) — a separate directory from staging
   segments, so eviction never touches staging data. Its data is charged to the page
   cache, not to the process RSS, and it must hold at least
   `4 * block_size` (`FsDevice` requires four allocation blocks).

The benchmark derives all four from `BENCH_MEMORY_BUDGET_MIB` so a run can be pinned to
a cgroup cap:

| Tier | Share of budget | Floor |
| --- | --- | --- |
| foyer memory tier | 1/4 | — |
| foyer NVMe tier | 1/4 | 64 MiB (`4 * 16 MiB` block size) |
| fjall index | 1/16 | 8 MiB |
| writer target (per segment) | 1/64 | 4 MiB |

For the 2 GB cap used as the example in `plans/project.md`, the benchmark therefore
provisions 512 MiB memory tier, 512 MiB NVMe tier, 128 MiB index, and 32 MiB writer
targets, leaving the rest for in-flight reads, the S3 backlog, and the runtime.

Cache sizing rules of thumb:

- The memory tier holds `memory_tier_bytes / 4096` blocks; with 1 KiB payloads a block
  serves three entries, so the effective entry capacity is `3 * memory_tier_bytes / 4096`.
- The NVMe tier only receives data once (a) the memory tier evicts and (b) a full
  device allocation block (`block_size`, 16 MiB by default) has accumulated. A working
  set smaller than `block_size` never reaches disk — lower `BENCH_BLOCK_SIZE_MIB`
  (and `ReadCacheConfig::block_size`) to exercise the NVMe tier with small sets.
- `max_concurrent_reads` bounds how many read calls can be in flight, which is also the
  memory bound for an S3-outage backlog (`backlog entries * payload` can queue, but only
  `max_concurrent_reads` of them hold buffers).
- Keep `cache_shards` small (1–2) for predictable measurements: shards partition the
  keyspace and add per-shard table metadata and bookkeeping.

## Running the benchmark

Build first, so no compilation happens inside the measured cgroup:

```sh
cargo bench -p bkxpro-reader --bench rss_under_load --no-run

systemd-run --user --scope -p MemoryMax=2G -p MemorySwapMax=0 -q \
  cargo bench -p bkxpro-reader --bench rss_under_load
```

`MemorySwapMax=0` keeps the run from swapping its way out of the budget. Run only some
scenarios by appending them (cargo's own `--bench` flag is filtered out):

```sh
systemd-run --user --scope -p MemoryMax=2G -p MemorySwapMax=0 -q \
  cargo bench -p bkxpro-reader --bench rss_under_load -- reads mixed
```

Without `systemd-run` the harness still runs, but if no ancestor cgroup has a finite
`memory.max` the cgroup check cannot be enforced (a session scope's `memory.current`
includes unrelated processes) and the report falls back to peak `VmRSS` with a warning.

| Variable | Default | Purpose |
| --- | --- | --- |
| `BENCH_MEMORY_BUDGET_MIB` | 2048 | Total budget split across the four tiers; also the enforced cap. |
| `BENCH_ENTRIES` | 20000 | Entries in the read set (reads/mixed; sizes their segment buffers). Write-scenario segments are sized by the budget instead. |
| `BENCH_PAYLOAD_BYTES` | 1024 | Entry payload size. |
| `BENCH_DURATION_SECS` | 5 | Duration of the writes and mixed scenarios. |
| `BENCH_BACKLOG` | 5000 | Queued reads in the S3-outage scenario. |
| `BENCH_RETRIES` / `BENCH_RETRY_DELAY_MS` | 10 / 20 | Retry budget per backlog read. |
| `BENCH_MAX_CONCURRENT_READS` | 128 | Read permits (S3 backlog bound). |
| `BENCH_FOYER_IO_ENGINE` | `psync` | `psync` or `uring` (foyer NVMe device engine). |
| `BENCH_MEMORY_TIER_MIB` | budget/4 | Override to force NVMe-tier traffic. |
| `BENCH_BLOCK_SIZE_MIB` | 16 | foyer device allocation block (`ReadCacheConfig::block_size`). |
| `BENCH_ROOT` | `target/bench-scratch` | Scratch root; must support `O_DIRECT`. |

### Registered buffers and `RLIMIT_MEMLOCK`

io_uring fixed buffers (the writer's encoder buffer, the read scratch region) are pinned
and charged against `RLIMIT_MEMLOCK`, which Linux accounts **per user**, not per
process. Two consequences:

- Budget `LimitMEMLOCK` for the sum of all live shard buffers plus headroom; 64 MiB is a
  reasonable production floor. When a registration fails, the harness prints the
  memlock limit and how to raise it (`ulimit -l`, systemd `LimitMEMLOCK=`).
- Destroying an io_uring instance does **not** always release that accounting (observed
  on Linux 6.8): without an explicit unregister, the whole fixed-buffer footprint stays
  charged for the lifetime of the user session, so shard restarts eventually fail with
  `ENOMEM`. `UringSegmentIo` therefore unregisters its buffers and files in `Drop`
  before the ring is torn down; the io crate's
  `dropping_the_ring_releases_registered_buffer_accounting` test guards this.

The sandbox this was developed in has a hard 8 MiB memlock, so the validation runs use a
384 MiB budget with 5000 entries (a ~7 MiB writer buffer). On hosts with 64 MiB+ the
defaults work unchanged.

## Reading the report

Every scenario prints, at minimum:

- **peaks** — `memory.current` peak from the cgroup and `VmRSS` peak from the process.
  The cgroup number is the one the budget is enforced against; RSS excludes page cache
  and pinned buffers.
- **memory.events delta** — `high / max / oom / oom_kill` over the scenario. `max > 0`
  and any `oom_kill` fail the run; `high > 0` is reported as a warning because it means
  reclaim started throttling the cache (raise the cap or shrink the tiers).
- **budget check** — `PASS` when peak `memory.current` ≤ `min(cgroup memory.max, budget)`.
  The three numbers are printed so an over-budget run shows whether the cgroup cap or
  the configured budget was exceeded.
- **per-tier hit rate and latency** — a read is attributed to the tier that served it:
  memory tier (~8–10 µs p50), foyer NVMe tier (~100 µs p50), local segment via aligned
  `O_DIRECT` (~150–180 µs p50), S3 fallback. Latency rows appear only for tiers that
  served at least one read, with p50/p95/p99/max.

A run fails if any scenario fails its budget check, if any allocation stalls at
`memory.max`, or if the cgroup OOM-kills the process.

## Measured baseline

All numbers below are from this repository's harness on a 4-core sandbox with an 8 MiB
memlock limit (`BENCH_MEMORY_BUDGET_MIB=384`, 5000 entries, 1 KiB payloads, 4 s write
phases, `MemoryMax=1G`, psync engine, 16 MiB device blocks):

| Scenario | Key results | Peak `memory.current` / RSS | Result |
| --- | --- | --- | --- |
| reads | cold 1680 block reads p50 146.7 µs / p99 214.3 µs; warm 5000 reads, 100 % memory tier p50 8.5 µs / p99 18.9 µs | 53.4 / 25.9 MiB | PASS |
| writes | 25 sealed segments, 113 725 entries, 152.8 MiB, 37.7 MiB/s | 57.2 / 29.5 MiB | PASS |
| mixed | 220 106 reader reads at 99.2 % memory (p50 11.4 µs / p99 28.4 µs) while a writer sustained 37.0 MiB/s | 101.2 / 73.0 MiB | PASS |
| outage | 20 000 queued reads × 8 retries = 160 000 S3 errors, drained in 1.85 s, 128 read permits | 87.7 / 60.0 MiB | PASS |

All four scenarios reported `memory.events` deltas of `0 / 0 / 0 / 0`: no reclaim
throttling, no stalls at `memory.max`, no OOM kills.

To see the NVMe tier do work, shrink the memory tier below the working set and lower the
device block size (`BENCH_MEMORY_TIER_MIB=2 BENCH_BLOCK_SIZE_MIB=1` with the 6.7 MiB read
set above):

| Tier | Hits | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| memory | 66.4 % | 11.8 µs | 17.5 µs | 20.8 µs |
| NVMe | 21.9 % | 103.7 µs | 142.4 µs | 166.1 µs |
| local segment (`O_DIRECT`) | 11.7 % | 195.8 µs | 280.1 µs | 337.3 µs |

## foyer device engine: io_uring vs psync

`BENCH_FOYER_IO_ENGINE=uring` selects foyer's io_uring engine (4 threads, matching the
sandbox's core count); the default `psync` engine uses synchronous pread/pwrite. Only
the NVMe tier's device path differs, so the comparison must be run in a configuration
where the NVMe tier actually serves reads (see the small-tier recipe above), and both
runs must use the same budget, entries, and memory tier.

Measured with the small-tier recipe (1680–5000 NVMe reads, one 6.7 MiB segment):

| Metric | psync | uring (4 threads) |
| --- | --- | --- |
| memory tier hits / p50 / p99 | 66.4 % / 11.8 µs / 20.8 µs | 66.4 % / 12.8 µs / 21.4 µs |
| NVMe tier hits / p50 / p99 | 21.9 % / 103.7 µs / 166.1 µs | 33.6 % / 96.1 µs / 3.37 ms |
| local segment hits / p50 | 11.7 % / 195.8 µs | 0 % |
| peak `memory.current` | 38.3 MiB | 38.2 MiB |
| budget check | PASS | PASS |

Both engines leave the memory tier untouched (~12 µs) and land the NVMe tier around
96–104 µs p50. The io_uring engine's tail is visibly worse in this sample (p95 1.13 ms,
p99 3.37 ms) and it admitted more entries into the disk tier (33.6 % vs 21.9 % of
reads); with a few thousand reads on a loaded 4-core box those differences are within
flush-batching noise, so the harness keeps `psync` as the default and the A/B above as
the procedure. Decide on the
target host, where the device queue depth, core count, and memlock limit differ: run
both engines with the same `BENCH_MEMORY_TIER_MIB`/`BENCH_BLOCK_SIZE_MIB`, compare NVMe
p99 and throughput, and prefer `uring` only if it wins on the real device.

## Allocator and hugepage evaluation

The benchmark separates the allocator's contribution from the configured tiers, which is
what makes an allocator A/B meaningful:

- `peak RSS (VmRSS)` excludes page cache and pinned buffers, so it isolates the heap.
  In the baseline runs RSS peaks at 25.6–72.7 MiB against tiers that sum to 480 MiB of
  budget, i.e. the process heap is not the dominant consumer; the tiers are.
- `memory.current` includes page cache, so NVMe-tier bytes show up there. On a
  disk-backed `BENCH_ROOT` they are reclaimable; on tmpfs they are charged to the
  cgroup and must be counted against the budget.
- Hugepages: the hot paths already allocate 4 KiB-aligned buffers (encoder buffer,
  scratch, `O_DIRECT` read buffers), and foyer's memory tier allocates entries directly
  from its own pool, so THP mostly benefits the fjall index. To evaluate, compare
  `peak RSS` with THP `madvise` vs `always`
  (`echo always > /sys/kernel/mm/transparent_hugepage/enabled`) at the same budget, and
  check `memory.events` for new reclaim activity on the capped run.

The A/B procedure (no build changes needed):

1. Run the baseline under the cap and record `peak RSS`, `peak memory.current`, the
   `memory.events` delta, and the per-tier p99s.
2. Optionally swap the global allocator (e.g. add `tikv-jemallocator` as a
   `#[global_allocator]` in the bench) or change THP settings.
3. Re-run with identical parameters; a difference is only meaningful if it exceeds the
   run-to-run spread you measure by repeating step 1 twice.

Current baseline: the peak RSS of every scenario is a small fraction of the configured
tier budget, so there is no evidence that the default allocator is the limiting factor;
allocator tuning is a second-order optimization behind tier sizing.

## Known limitations

- The S3 fallback is a stub (`ReadCacheError::S3Read`) until the uploader exists;
  the outage scenario therefore measures backlog memory and retry behavior, not ranged
  GET latency.
- The benchmark uses temp directories under `BENCH_ROOT`; point it at a disk-backed
  filesystem. On tmpfs, cached segment bytes are charged to `memory.current` and
  inflate the cgroup peak.
- `memory.high` throttling is reported as a warning, not a failure: only the operator
  knows the intended reclaim tolerance for a given cap.
