# bkxpro read cache — RAN-131

This crate contains the foyer hybrid read cache for sealed segment blocks and the
RSS-under-load benchmark that validates its memory budget under a real cgroup cap.

## Read path

`ReadCache::read_entry` serves an entry by ledger and entry id in this order:

1. **foyer hybrid cache** — one lookup serves the memory tier and the NVMe tier, and
   foyer's `Source` on the returned entry says which tier answered, so a hit is
   attributed to the tier that actually served it. The NVMe tier lives in its own
   directory (`ReadCacheConfig::cache_dir`), separate from staging, so eviction never
   touches staged segments.
2. **Local segment** — an aligned `O_DIRECT` range read through the `SegmentIo`
   boundary (`UringSegmentIo` in production, fixed buffers). The read path opens the
   sealed segment read-only via `open_segment_for_read` and clamps to the file size: it
   never preallocates, truncates, or extends a sealed segment. `LocalSegmentReader::new`
   takes a backend factory and returns a `Result`; the factory runs on a dedicated
   I/O thread to preserve io_uring ownership. Local reads are serialized on that
   thread and callers wait asynchronously through a bounded request channel.
3. **S3** — ranged GET fallback (RAN-130). Once the uploader has removed the local
   copy, the injected `SegmentReader` (the uploader's `FallbackSegmentReader`) routes
   the range read to the object store; a successful read is served with
   `ReadSource::S3` and warms the block cache, and a failed one surfaces as
   `ReadCacheError::S3Read`. A local file that is missing while the index still says
   not-deleted (a crash between unlink and the state update) also takes this path. The
   outage benchmark measures backlog and retry behavior when the object store fails.

A read of a block that is only cached partially (truncated tail block, stale range)
falls through to the authoritative local segment read instead of failing.

`ReadResult` reports the served `ReadSource`, the payload, and the latency, and
`ReadCache::stats` aggregates per-tier hit counts, byte counts, and latency totals so a
caller can report hit rate and p99 per tier. Concurrency is bounded by
`ReadCacheConfig::max_concurrent_reads`; the permit is acquired inside `read_entry`, so
queued callers do not hold buffers.

## Configuration

`ReadCacheConfig` carries both staging (`data_dir`) and cache (`cache_dir`) paths, the
memory and NVMe tier capacities, shard count, block size, `IoEngineType` (`Psync` or
`Uring`), and the read-timeout. `block_size` is foyer's device allocation block and must
be at most `nvme_tier_bytes / 4`.

See [MEMORY_TUNING.md](MEMORY_TUNING.md) for the budget model, the benchmark procedure,
per-tier results, the foyer io_uring vs psync evaluation, and the
`RLIMIT_MEMLOCK` requirements of registered buffers.

## Build, test, benchmark

```sh
cargo test -p bkxpro-reader
cargo bench -p bkxpro-reader --bench rss_under_load --no-run
systemd-run --user --scope -p MemoryMax=2G -p MemorySwapMax=0 -q \
  cargo bench -p bkxpro-reader --bench rss_under_load
```

The integration test `reader/tests/uring_segment_read.rs` exercises the real
io_uring adapter against sealed segments: it verifies that the read path does not resize
the file it reads and that a cold `LocalSegment` read populates the cache so the next
read is served from the memory tier.
