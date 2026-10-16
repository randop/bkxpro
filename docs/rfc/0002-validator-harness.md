# ADR 0002: Validator harness: simulation, fault injection, fuzzing, soak

- **Status:** Implemented; kernel-fault evidence collected per run (see the runbook).
- **Scope:** staging tier verification of the writer/index/uploader lifecycle. Not a performance
  harness and not a production service component: it exists to break the durability contract on
  purpose and prove the product notices.

## Decision

Build an in-house deterministic harness (`crates` convention: the `validator/` package) rather
than adopting a distributed-systems simulator such as turmoil or madsim.

- The system under test is a single process with one I/O trait (`SegmentIo`), one index, and one
  object-store trait (`BlobStore`). A network simulator would add an adaptation layer between the
  harness and the code paths that actually have to hold the invariants.
- Determinism must come from the seed, not from scheduling luck. The harness derives every fault
  decision from `(seed, key)` content streams (`rng::DetRng::derive_from`) and orders every
  observation into one append-only event trace with two digests per run. A failing run is replayed
  by name and seed; the harness proves replay by running every plan twice and comparing digests.
- The simulator can only model crashes by construction. Anything involving a real signal, a real
  unlink, a real kernel, or a real object store is exercised through separate mechanism-specific
  layers (kill rounds, kernel fault targets, real endpoint chaos) instead of being faked.

## Invariants under test

Every layer asserts the same contract:

1. **No acknowledged entry is lost.** An entry the writer acknowledged (group commit commit-batch
   or seal) is durable: recoverable from disk after process death, after power loss, and from the
   object store after upload.
2. **No torn or duplicate record is surfaced.** Recovery yields the valid prefix of each segment;
   every record's payload equals the seeded payload recomputation; no `(ledger, entry)` appears
   twice across segments.
3. **A fenced writer can never commit.** A newer manifest epoch on a segment fences our epoch: no
   commit of the older epoch, no local delete.
4. **A local segment is deleted only after PUT + manifest commit.** Delete-gating checks the trace
   event order: `PutDone` and `ManifestCommitted` for the same `(segment_id, epoch)` must precede
   every `LocalUnlinked`.
5. **Resources stay in budget.** Soak runs track RSS, open file descriptors, and on-disk bytes
   against explicit limits, and fail on monotonic growth (a leak), not on a single high sample.
6. **No acknowledgement after a failed write or `fdatasync`.** The writer poisons itself and the
   process must fail-stop; a restart recovers only durable bytes.

Recovered records must be a **prefix of what was attempted**, never a prefix of what was
acknowledged: a record whose append landed in the page cache before a failing sync may be visible
after a crash, and that is legitimate. It is not legitimate to lose something acknowledged, to
invent an entry, or to surface a record twice.

## Three durability models, kept distinct

Conflating them is how storage tests lie:

| Model | Harness mechanism | What it can and cannot show |
|---|---|---|
| Process death | `Session::restart` drops in-memory state, wipes the derived index, rebuilds from segment files and re-seals open segments; kill rounds `SIGKILL` the real child | Every byte ever written is still there. Cannot show sync ordering. |
| Power loss | `SharedSimIo::crash` discards unsynced bytes and unsynced directory entries; `scripts/kernel-faults.sh` replays the `dm-log-writes` log to every mark | Anything acknowledged must have been synced. |
| Unclean unlink | `SharedSimIo::arm_unlink_failure_after_removal` fails the unlink after the bytes are gone | The caller must not treat the local file as uploaded. |

## Simulation layer

`scenarios::run_plan` drives one `plan::preset(name, seed)` through the real writer, index, and
uploader code paths against two deterministic stand-ins:

- **`io::simulated`** behind `sim_io::SharedSimIo` (the deterministic backend): disk faults
  (`FailWrite`, `Eagain`, `Enospc`, torn writes, byte-flip corruption, `FailSync`, slow syncs,
  failed truncates, directory-sync failures, failed acquires, stale reads) armed at chosen appends
  or on the device, plus power-cut and crash semantics.
- **`mock_s3`**: lost PUT responses, CAS conflicts and lost CAS responses, GET/HEAD failures,
  latency spikes, outage windows, fence-after-PUT, and crash-after-N-successful-PUT/CAS points.

The presets (`smoke`, `crash-upload`, `crash-lifecycle`, `power-cut`, `disk-faults`, `fenced`,
`s3-chaos`, `outage`, `slow-sync`) group by which hazard they try to break. A plan is a pure
function of `(name, seed)`, so CI can pin fixed seeds per PR and nightly sweeps random ones.

## Layers that are not the simulator

- **Kill rounds** (`kill_soak`): the `bkxpro-validator kill-soak-child` subcommand writes to real
  files through `PlainFileIo` and journals each acknowledgement to `acked.jsonl` immediately. The
  parent `SIGKILL`s the child mid-append at a seeded delay and runs the same recovery proof the
  product uses (`verify_session_dir`, also reachable as the `verify` subcommand). `SIGKILL`
  preserves the page cache, so sync ordering stays with the power-cut model and the kernel replay
  script.
- **Kernel fault targets** (`validator/scripts/kernel-faults.sh`, root only): `dm-log-writes`
  replay to every mark, `dm-flakey` error windows (`error_writes`/`drop_writes`), `dm-dust` read
  errors on injected bad blocks, `null_blk` memory-backed devices, `CONFIG_FAIL_MAKE_REQUEST`
  windows, live read-only remounts, and ENOSPC on small XFS/ext4 filesystems, each followed by a
  recovery verify. The script exits 77 (skipped) when a facility is missing so CI can treat
  "unavailable" differently from "broken".
- **Real endpoint chaos** (`real_s3::run_real_s3` and
  `validator/scripts/s3-chaos.sh`): the full lifecycle against a live S3-compatible endpoint
  (multipart PUT, conditional manifest CAS, ranged-GET read-back, manifest-independent remote
  verification) while the script kills and restarts minio (process mode or
  `docker kill`/`docker start` in CI). Uploads run in a deadline-bounded settle loop that retries
  transient failures; only a poisoned uploader stops the loop. A local minio is treated as AWS S3
  using the `BKXPRO_S3_TEST_*` contract. Each run uses a unique object prefix, and cleanup is
  retried until the deadline and best-effort (evidence stays when violations exist).
- **Fuzzing**: `fuzz.rs` mutates sealed images and tails (truncation/zeroing) and asserts parser
  stops never produce silent truncation; `codec/fuzz` runs `cargo-fuzz` over the codec and
  recovery parsers with a corpus generated by the harness (seed 9, byte-reproduced in CI).
- **Concurrency model**: `validator/tests/model.rs` is a proptest reference ledger (append /
  read / fence / LAC interleavings) that the segment state machine must match.
- **Soak** (`soak`): long-running loop over the matrix with resource budgets, `--resume` for
  multi-day continuation, and interleaved real kill rounds.

## Runbook

See [`validator/README.md`](../../validator/README.md) for reproducing a failure from a seed,
driving the local minio, running the kernel scripts, and operating the dashboards.

## Limitations

- One memory-corruption shape (slow syncs) is modeled, not observed: the harness cannot flip
  bytes at runtime like real SRAM errors; `dm-dust` and log-writes replay are the closest real
  substrates.
- Kernel fault runs require root on a machine with the dm targets and filesystem tools available;
  CI covers hosted-runner kernels only. The 6.1 leg is opt-in for the same reason.
- Multipart uploads smaller than 5 MiB are exercised as single-part PUTs locally; larger
  workloads (the harness default exceeds one part) exercise real multipart.
