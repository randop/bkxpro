# bkxpro segment writer — RAN-127

This crate contains the segment-writer state machine and group-commit policy. It consumes the RAN-126 `AlignedSegmentEncoder` and the narrow `SegmentIo` boundary that the RAN-134 io_uring foundation must implement.

## Scope implemented

- Acquires a preallocated segment file (the I/O adapter may recycle an existing file), writes zero blocks across the entire configured capacity, then calls `fdatasync` once after the bulk zeroing batches. The directory is synced after create/rename changes.
- Reuses the caller's 4 KiB-aligned output buffer for codec bytes and direct-I/O writes; it does not allocate a payload `Vec` per record.
- Enforces segment target, footer entry bound, duplicate-key rejection, and a bounded in-flight-byte budget.
- Batches finalized data blocks until the configured group-commit threshold, with a forced flush for backpressure. Per-record acknowledgements are released only after the commit containing each record's final block succeeds.
- Fails stop after write, `fdatasync`, footer commit, or truncation errors; there are no retries and no acknowledgements returned from a failed operation.
- Writes the footer/trailer, truncates the preallocated file to the codec's canonical sealed length, and syncs that size change. This is required because RAN-126 expects the trailer to end exactly at EOF.
- Exposes `should_seal_at()` for the shard's timer. The owning event loop drives the timer and calls `seal`; this crate creates no worker thread and adds no thread hop to the ack path.

## RAN-134 integration contract

`SegmentIo` is the RAN-134 integration boundary. `bkxpro-linux-io::UringSegmentIo` implements the production adapter using registered files and buffers with io_uring; no buffered or `pwrite` fallback is provided. `commit_batch` orders writes before `fdatasync` and makes short writes/errors fatal. Existing files are reused and fully re-zeroed by the writer before use.

`AlignedBuffer` is a test/tooling helper, not a registered buffer pool. Production integration must pass a leased, registered buffer from the RAN-134 pool to both `SegmentWriter::create` and `AlignedSegmentEncoder::new`.

## Build and tests

From the workspace root:

```sh
cargo test -p bkxpro-segment-writer
cargo test -p bkxpro-codec
```

The current execution environment has no Rust toolchain, so these commands have not been run here. Linux/io_uring conformance, XFS/ext4 behavior, crash testing, and performance baselines must still be run on qualified Linux hosts.


## Production I/O integration (RAN-134)

Use `bkxpro-linux-io::UringSegmentIo` as the `SegmentIo` backend. Allocate a stable `AlignedBuffer`, register it with a fixed-buffer slot using the unsafe `register_buffer` method, and keep that allocation alive until the ring is dropped. Keep the runtime on the shard's creator thread; it uses `SINGLE_ISSUER` + `DEFER_TASKRUN`. Do not use the writer's aligned helper as a substitute for registration in production.
