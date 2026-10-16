# Validator harness

The harness breaks the durability contract on purpose and proves the product notices. Every
layer asserts the same invariants:

1. no acknowledged entry is lost;
2. no torn or duplicated record is surfaced;
3. a fenced writer can never commit;
4. a local segment is deleted only after a successful PUT **and** a manifest commit of the same
   epoch;
5. soak runs hold RSS / open-file / on-disk budgets and flag monotonic growth;
6. nothing is acknowledged after a failed write or `fdatasync` (the writer fail-stops; a restart
   recovers only durable bytes).

The design is documented in
[`docs/rfc/0002-validator-harness.md`](../docs/rfc/0002-validator-harness.md).

Everything below runs on the `bkxpro-validator` binary:

```sh
cargo build -p bkxpro-validator --bin bkxpro-validator
```

## Simulation suite

```sh
# one preset at one seed
./target/debug/bkxpro-validator sim --scenario s3-chaos --seed 42

# the whole matrix at one seed, each plan run twice so digests must reproduce
./target/debug/bkxpro-validator sim --all --seed 1 --replay \
  --log target/bkxpro-validator/runs.jsonl \
  --incidents target/bkxpro-validator/incidents

# consecutive seeds (nightly style)
./target/debug/bkxpro-validator sim --all --seed "$RUN_ID" --runs 8 --replay
```

Presets: `smoke`, `crash-upload`, `crash-lifecycle`, `power-cut`, `disk-faults`, `fenced`,
`s3-chaos`, `outage`, `slow-sync`. A failing run writes an incident directory containing the
report (`report.json`) and copy-paste repro (`repro.md`).

## Reproducing a failure from a seed

Every report names the exact plan: `(scenario, seed)` fully determines a run.

```sh
# from a saved incident
./target/debug/bkxpro-validator replay --report target/bkxpro-validator/incidents/<incident>/report.json

# by hand
./target/debug/bkxpro-validator sim --scenario power-cut --seed 1234

# a recorded failing seed becomes a regression test: pin it in CI with
./target/debug/bkxpro-validator sim --seed <seed> --runs 1
```

Exit codes everywhere: `0` pass, `1` failures/violations, `2` usage or harness error.

## Kill soak (real SIGKILL of a writer)

A real child process appends through `std::fs` and journals every acknowledgement; the parent
`SIGKILL`s it at a seeded delay and runs the same recovery proof as the product.

```sh
./target/debug/bkxpro-validator kill-soak --seed 7 --rounds 8 \
  --dir target/bkxpro-validator/kill-soak --payload-bytes 1024 --seal-at 32

# verify a session directory by hand (segments + acked.jsonl + child-meta.json)
./target/debug/bkxpro-validator verify --dir target/bkxpro-validator/kill-soak/round-0001
```

`SIGKILL` preserves the page cache, so this layer proves recovery of *written* bytes; sync ordering
is the power-cut scenarios' and the kernel replay script's job.

## Soak (short run and the multi-day procedure)

```sh
# bounded soak: 30 minutes, resource budgets, a kill round every 8 runs
./target/debug/bkxpro-validator soak --seed 1 --minutes 30 --kill-every 8 \
  --dir target/bkxpro-validator/soak \
  --rss-limit-mib 2048 --fd-limit 8192 --dir-limit-mib 6144 \
  --log target/bkxpro-validator/soak-runs.jsonl
```

Multi-day procedure (one operator session per day):

1. Reserve a dedicated Linux host with XFS **and** ext4 on the data paths used.
2. Start with `--resume` and a fixed base seed so the seed sequence continues:
   `soak --seed <base> --minutes 1440 --runs 2000 --resume --kill-every 16 ...`.
3. Between sessions, collect: the run log (`runs.jsonl`), the dashboard, incident directories,
   dmesg for any OOM or io_uring failure, and `smaps`/cgroup `memory.peak` samples for RSS.
4. File an issue per failing seed (the nightly workflow does this automatically) and add a
   regression test or fixed seed that pins it.
5. Watch the dashboard for two days without failures before declaring the suite clean:

```sh
./target/debug/bkxpro-validator dashboard --log target/bkxpro-validator/runs.jsonl
```

## Kernel-level faults (root required)

`validator/scripts/kernel-faults.sh` runs the child writer on XFS/ext4 layered over real kernel
fault targets, then recovers with `bkxpro-validator verify`:

```sh
sudo validator/scripts/kernel-faults.sh --fs both --seconds 8 \
  --work /var/tmp/bkxpro-kernel-faults
```

| Scenario | Substrate | Notable requirement |
|---|---|---|
| `log-writes` | `dm-log-writes` replay to every mark | external `replay-log` (xfstests `make -C src log-writes`) |
| `flakey` | `dm-flakey` error windows | `error_writes` (or `BKXPRO_FLAKEY_FEATURE=drop_writes`) |
| `dust` | `dm-dust` bad-block read errors | — |
| `null-blk` | memory-backed `null_blk` device | configfs |
| `fail-make-request` | `CONFIG_FAIL_MAKE_REQUEST` | debugfs |
| `remount-ro` | filesystem shutdown mid-append | `xfs_io` with filesystem shutdown support |
| `enospc` | small XFS/ext4 filesystem fills mid-append | — |

The script exits `77` when a facility is missing (neither pass nor failure) and `0/1/2` otherwise.

## Real endpoint S3 chaos (local minio stands in for AWS S3)

Start a local minio with the credentials the harness expects:

```sh
docker run -d --name validator-minio -p 9000:9000 \
  -e MINIO_ROOT_USER=localadmin -e MINIO_ROOT_PASSWORD=localdevsecret123 \
  minio/minio server /data --console-address ":9001"
docker run --rm --network host \
  -e MC_HOST_local=http://localadmin:localdevsecret123@127.0.0.1:9000 \
  minio/mc mb --ignore-existing local/bkxpro-staging
```

One clean end-to-end run (write, seal, restart, multipart PUT, manifest CAS, delete, ranged-GET
read-back, remote verification):

```sh
BKXPRO_S3_TEST_ENDPOINT=http://127.0.0.1:9000 \
BKXPRO_S3_TEST_BUCKET=bkxpro-staging \
BKXPRO_S3_TEST_ACCESS_KEY=localadmin \
BKXPRO_S3_TEST_SECRET_KEY=localdevsecret123 \
  ./target/debug/bkxpro-validator s3-run --seed 3 --segments 2 --entries 512 --payload-bytes 4000
```

Chaos: the script kills and restarts minio while a run is in flight, and the run must ride the
outage out:

```sh
BKXPRO_S3_TEST_ENDPOINT=http://127.0.0.1:9000 \
BKXPRO_S3_TEST_BUCKET=bkxpro-staging \
BKXPRO_S3_TEST_ACCESS_KEY=localadmin \
BKXPRO_S3_TEST_SECRET_KEY=localdevsecret123 \
  validator/scripts/s3-chaos.sh --rounds 2 --segments 2 --entries 2100 \
  --payload-bytes 4000 --outage-seconds 10 --work /tmp/bkxpro-s3-chaos
```

The script discovers the running `minio server` process and restarts it with its own command line,
working directory and `MINIO_*` environment (`--docker-minio NAME` for containers, `--no-minio`
for a no-fault sanity run). Each run writes to its own object prefix and cleans it up afterwards.
The flight-check output must contain `transient upload failure (retrying)` for the chaos to have
landed; the script warns when the outage missed the upload window, in which case raise `--entries`
or lower `--kill-after-seconds`.

## Fuzzing

```sh
# structural and tail mutations of sealed images (deterministic per seed)
./target/debug/bkxpro-validator fuzz --seed 9 --iterations 200000
./target/debug/bkxpro-validator fuzz --tail --seed 9 --iterations 200000

# regenerate the cargo-fuzz corpus (byte-identical for seed 9; CI verifies this)
./target/debug/bkxpro-validator fuzz --seed 9 --write-corpus codec/fuzz/corpus
cargo +nightly fuzz run recovery_parser -- -max_total_time=300   # in codec/fuzz
```

## CI

- **PR** (`validator.yml`): workspace tests, fixed-seed `sim --all --replay`, cargo-fuzz 60 s
  per target, XFS/ext4 legs on the hosted 6.8 runner. The 6.1 LTS leg runs only when the repo sets
  `BKXPRO_KERNEL61_RUNNER` (JSON runs-on labels).
- **Nightly** (`validator-nightly.yml`): random seed, sweep, bounded soak with kill interleave,
  kill rounds, long fuzzing, dashboard, auto-filed issues. Real-S3 chaos is opt-in through the
  `BKXPRO_S3_CHAOS` repository variable (always on manual dispatch).

Every bug found by any layer gets: a regression seed/test in the suite, and a note in the incident
repro so the same seed fails loudly forever.
