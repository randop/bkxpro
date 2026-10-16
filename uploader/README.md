# bkxpro uploader

This crate streams sealed segment files to an S3-compatible object store and gates
local deletion on a compare-and-swap commit to an append-only manifest, so a crash at
any point can be repaired without losing data or leaking an undeletable segment. It
also provides the ranged-GET reader that serves reads once the local file is gone.

## Upload protocol

`SegmentUploader::advance` moves one segment through `Sealed -> Uploading -> Uploaded
-> Deleted`. Every index transition is a single fjall write batch:

1. **Validate.** The dedicated I/O thread `stat`s the local file and validates the
   sealed layout (header, trailer, footer, canonical tail) into a `SegmentDigest`.
2. **Reserve.** The index row is marked `Uploading` and the object key is recorded,
   in one batch, before any bytes leave the host.
3. **PUT.** The file streams as a single object (`segments/{segment_id:020}.seg`) with
   `WriteMultipart` — one `part_size` chunk at a time, up to `write_concurrency` parts
   in flight. Nothing is re-encoded: the reader hands owned 4 KiB-aligned chunks to the
   store. `End { len, crc32c }` ends the stream: the store rejects a byte count that
   does not match `len` (aborting the multipart upload, so a short object is never
   published), the object length is verified again with a `HEAD`, and the streamed
   CRC32C is recorded in the manifest. Transient failures are retried with exponential
   backoff (`max_retries`, `retry_base_backoff`, `retry_max_backoff`) inside an overall
   `retry_timeout`.
4. **Commit.** A JSONL record is appended to `manifest/log.jsonl` with a conditional
   `PUT` (`If-None-Match: *` for the first append, `If-Match: <etag>` afterwards). This
   is the durable fact that gates deletion. A lost CAS response is absorbed by
   re-reading the log and retrying, because the append may have landed.
5. **Mark.** The index row is marked `Uploaded`.
6. **Delete.** The file is unlinked and the directory fsynced through the `SegmentIo`
   backend; only then is the row marked `Deleted` (retaining `s3_key`).

`recover()` runs this reconciliation over every non-deleted row at startup, and
`process_pending()` is the same logic for the service loop; `run(shutdown)` polls
`process_pending()` until the flag is set. Every crash window is repairable:

| Crash point | Durable state | Recovery |
| --- | --- | --- |
| before the PUT | file present, no record | upload again |
| after the PUT, before the CAS commit | file present, no record | re-upload once (the PUT is idempotent per key) |
| after the CAS commit, before the state update | file present, record present | verify, mark, delete locally — no second PUT |
| after the unlink, before the `Deleted` state | file missing, record present | mark `Deleted` |
| file missing without a record | nothing | fail stop (`LocalSegmentMissing`): the only copy may be gone |

Contradictions poison the uploader and keep the local file: `LocalSegmentMissing`,
`Fenced` (a manifest record with a newer writer epoch), `CorruptSegment`,
`VerificationMismatch`, `ManifestConflict`. `RetriesExhausted` is *not* fatal: the
segment stays `Uploading` with its file intact and the next pass retries.

## Read fallback

`FallbackSegmentReader` composes `WorkerSegmentReader` (aligned local reads via the I/O
thread) with `S3SegmentReader` (ranged GETs) and routes per segment: local while the
index state is not `Deleted` and the file exists, remote otherwise. `S3SegmentReader`
caches one digest per segment (`s3_digest_cache_entries`), fetches the header block,
trailer, and footer once, then serves aligned ranges and validates every complete 4 KiB
block against the codec.

`ReadCache::read_entry` in `bkxpro-reader` calls this reader both for the `Deleted`
tombstone and for a missing local file (the crash window before the state update),
fills the foyer block cache from the remote copy, and reports `ReadSource::S3`; a
failure surfaces as `ReadCacheError::S3Read`.

## Bounded memory and I/O isolation

- The worker thread owns the `SegmentIo` backend (io_uring rings are `SINGLE_ISSUER`).
  Reads stream as `read_chunk_bytes` aligned chunks that are charged to `ByteBudget`
  (`max_inflight_bytes`); the permit travels inside the `Bytes` and is released as soon
  as the blob has copied the chunk into its part buffer, so in-flight reader bytes stay
  bounded no matter how slow the store is. A store that retained those chunks (holding
  the permits) until `complete()` would starve the reader, so `ObjectStoreBlob` copies
  instead of moving them, and store-side parts stay bounded by `write_concurrency`
  parts of `part_size`. A bounded channel (`stream_channel_capacity`) bridges the reader
  thread and the tokio `object_store` client.
- Production wires `bkxpro-io::UringSegmentIo` with registered buffers and
  O_DIRECT, so uploads do not pollute the page cache; this crate never reads a segment
  through buffered I/O itself.
- Part size and part concurrency are applied where the store is constructed
  (`UploaderConfig::blob`), the digest cache bound where the reader is constructed
  (`UploaderConfig::s3_reader`).
- The worker thread lowers its own I/O priority before creating the backend:
  `IoPriority::default()` is best-effort level 7 via `ioprio_set(2)`.
  `SegmentIoWorker::spawn_with_io_priority` selects `Unchanged`/`Idle`/`BestEffort(n)`;
  a refused syscall is logged and ignored, and cgroup v2 `io.weight` is the documented
  alternative.

## Composition

```rust
let config = UploaderConfig { data_dir: staging.clone(), ..Default::default() };
let index = Arc::new(Index::open(index_config)?);
let worker = SegmentIoWorker::spawn(move || UringSegmentIo::new(ring_config))?;
let blob = Arc::new(config.blob(store)); // part size + part concurrency
let uploader = SegmentUploader::new(config.clone(), index.clone(), blob.clone(), worker.clone())?;
uploader.recover().await?;
// ... uploader.run(shutdown) or uploader.process_pending() in the service loop ...

// Reads after deletion:
let reader = FallbackSegmentReader::new(
    Arc::new(WorkerSegmentReader::new(worker, staging, index.clone())),
    Arc::new(config.s3_reader(blob, index.clone())),
);
let cache = ReadCache::new(read_cache_config, index, reader).await?;
```

Key layout: `segments/{segment_id:020}.seg` for segment objects (prefix
`UploaderConfig::s3_prefix`), `manifest/log.jsonl` for the manifest
(`UploaderConfig::manifest_key`), `{segment_id}.seg` for local files. Every key in
`UploaderConfig` is a `pub const` (`DEFAULT_SEGMENT_PREFIX`, `DEFAULT_MANIFEST_KEY`).

## Tests

```sh
cargo test -p bkxpro-uploader
```

56 unit tests plus two integration tests and one environment-gated object-store test:

- Fault injection through `FaultyBlob`/`FaultPlan`: transient PUT, PUT with a lost
  response, persistent PUT failure, CAS conflict/transient/lost response, transient
  GET — including `exhausted_retries_keep_everything_local`,
  `cas_conflict_and_lost_cas_response_are_absorbed`, and
  `lost_put_response_is_retried_and_marked_once`.
- Crash windows built from durable state (`recover_finishes_every_crash_window`,
  `crash_after_commit_before_delete_skips_the_put`,
  `crash_after_delete_before_state_change_recovers`) plus fail-stop cases (`fenced_epoch_is_fail_stop_and_keeps_the_file`,
  `manifest_mismatch_is_fail_stop`, `missing_file_without_record_is_fail_stop`).
- I/O worker behavior: digest validation, streaming with checksum, backpressure
  (`stream_backpressure_bounds_inflight_bytes`, `backpressure_keeps_inflight_bytes_within_the_budget`,
  `a_memory_store_uploads_a_segment_larger_than_the_byte_budget`),
  idempotent read/unlink, shutdown, and the worker thread's I/O priority.
- Integration (`uploader/tests/upload_roundtrip.rs`), over real files: a sealed
  segment is uploaded byte-for-byte, deleted locally, and then read through
  `ReadCache` with `FallbackSegmentReader`, which proves the read path switches to the
  object store and warms the block cache. A second test restarts the uploader in the
  "manifest committed, cleanup unfinished" window with **every PUT failing** and
  requires recovery to finish from the manifest alone.
- `uploader/tests/s3.rs` runs the same pipeline against real S3 when the environment
  provides a bucket (skipped otherwise):

  ```sh
  BKXPRO_S3_TEST_ENDPOINT=http://127.0.0.1:9000 \
  BKXPRO_S3_TEST_BUCKET=bkxpro-staging \
  BKXPRO_S3_TEST_ACCESS_KEY=... \
  BKXPRO_S3_TEST_SECRET_KEY=... \
    cargo test -p bkxpro-uploader --test s3 -- --nocapture
  ```

  `BKXPRO_S3_TEST_ENDPOINT` names an S3-compatible endpoint (path-style, HTTP allowed);
  leaving it unset addresses AWS S3 itself with virtual-hosted HTTPS requests, where
  `BKXPRO_S3_TEST_REGION` (or the usual AWS environment) supplies the region.
  It uploads a ~5.5 MiB sealed segment as a real two-part multipart PUT, verifies the
  stored object byte-for-byte, checks the local file is gone, and reads sampled
  entries back through ranged GETs.

The integration tests use `PlainFileIo`, a `SegmentIo` implementation on plain
`std::fs`, because the test filesystem (tmpfs) has no O_DIRECT support; the production
io_uring backend is covered by the `bkxpro-io` and `bkxpro-reader` suites.

## Benchmarks

`uploader/benches/upload_page_cache.rs` (`harness = false`) runs a buffered control read
and a real `UringSegmentIo` upload over the same sealed segment: the control read must
move at least 32 MiB into the page cache (`memory.stat: file`) and the upload must add
at most 8 MiB, which is evidence that O_DIRECT uploads do not pollute the
page cache. It also reports the reader's byte-budget high water and the `memory.current`
peak.

```sh
cargo bench -p bkxpro-uploader --bench upload_page_cache
```

Knobs: `BKXPRO_BENCH_DIR` picks the scratch directory (default
`target/bench-scratch`, which must support O_DIRECT), `BENCH_SEGMENT_MIB` changes the
segment size, `BENCH_READ_CHUNK_MIB` changes the reader chunk (bounded by
`RLIMIT_MEMLOCK`, which pins the staged-read scratch), `BENCH_PROGRESS=1` prints upload
progress every two seconds, and `BENCH_SKIP_CACHE_CONTROL=1` runs the upload phase on a
filesystem that gives no control signal. A filesystem that serves buffered reads from
its own cache (ZFS's ARC) never moves `memory.stat: file`; the control phase detects
that and skips the run instead of reporting a vacuous pass, so run the check on the
XFS/ext4 staging volume.

## Notes and limitations

- **Entry rows are retained on deletion.** Deleting a segment takes one batch that
  tombstones the `segments` row (`Deleted` + `s3_key`); the `entries` rows stay because
  the read fallback resolves payload locations through them.
  `Index::delete_segment_entries` remains the explicit retention purge for when a
  segment leaves the retention window.
- **Payload offsets.** `EntryLocation.offset` is the payload offset (the 21-byte record
  header is excluded), which is the convention the read path slices with. Both index
  rebuild paths translate the codec's record-header offsets accordingly.
- **Block-spanning payloads.** Ranged reads (local and remote alike) serve payloads
  that fit inside one 4 KiB block, which is the granularity of the block cache. The
  physical file interleaves 16-byte block headers, so a payload spanning blocks needs
  reassembly in the read path (`bkxpro-reader`); that work is not part of the uploader.
- **Uploads are idempotent per key.** A crash between the PUT and the manifest commit
  re-uploads to the same key, so retries never leave orphaned objects or duplicate
  copies.

Segment object keys include both the segment id and the writer epoch:
`segments/{segment_id:020}/{writer_epoch:020}.seg`. Competing epochs upload to
separate objects; the manifest CAS selects the authoritative generation and its
exact object key is retained in the index for remote reads.
