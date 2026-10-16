#!/usr/bin/env bash
# Battle-testing campaign for bkxpro: the validator harness end to end.
#
# Executed for the battle-testing pass on this host (Linux 6.8, non-root):
#   1. build bkxpro-validator
#   2. regenerate the deterministic fuzz corpus (seed 9) and prove generation
#      byte-identical across runs
#   3. validator suite, full workspace suite, and the nightly (ignored) seed
#      sweeps: 64 seeds x 9 presets + 128 seeds x the two chaos presets
#   4. simulator matrix at seed 1, a 64-seed matrix sweep, and 128-seed deep
#      chaos sweeps, all with double-run digest replay
#   5. eight real SIGKILL kill-soak rounds
#   6. a bounded soak (3 min) with RSS/fd/disk budgets and a kill round every
#      8 scenario runs
#   7. 200k structural and 200k tail fuzz iterations
#   8. real-endpoint S3: one clean lifecycle run, then endpoint-kill chaos
#      (two rounds against local minio)
# The root-only kernel fault targets (validator/scripts/kernel-faults.sh) are
# skipped with instructions when this script is not run as root.
#
# Scratch, logs, and incidents go to a fresh `mktemp -d` directory: it is
# removed when the whole campaign passes, and kept (path printed) when any
# step fails, so failing seeds and repro evidence survive.
#
# Known repo gaps observed by this campaign (2026-10-16):
#   * validator/tests/fuzz.rs::checked_in_corpus_audits_clean expects a corpus
#     at codec/fuzz/corpus/recovery_parser, but codec/.gitignore ignores
#     `fuzz/corpus/` and no corpus is committed: a fresh clone fails
#     `cargo test -p bkxpro-validator`. This script regenerates it first.
#   * validator/README.md and validator/src/fuzz.rs reference
#     `cargo +nightly fuzz run recovery_parser`, but codec/fuzz defines only
#     the record_parser, block_parser and footer_parser targets.
#
# Usage: scripts/battle-test.sh
#
# Env overrides:
#   BATTLE_TEST_SEED             base seed for the CLI sweeps (default: random
#                                64-bit; the recorded pass used 20261016)
#   BATTLE_TEST_SWEEP_RUNS       matrix sweep seeds (default 64)
#   BATTLE_TEST_CHAOS_RUNS       chaos sweep seeds per preset (default 128)
#   BATTLE_TEST_SOAK_MINUTES     soak wall clock (default 3)
#   BATTLE_TEST_FUZZ_ITERATIONS  iterations per fuzz mode (default 200000)
#   BATTLE_TEST_SKIP_S3=1        skip the real-endpoint legs
#   BKXPRO_S3_TEST_*             endpoint contract; defaults target local minio
#
# Exit codes: 0 all passed, 1 a run failed/violated, 2 setup error.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/bkxpro-battle-test.XXXXXXXX")"

SEED="${BATTLE_TEST_SEED:-$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')}"
SWEEP_RUNS="${BATTLE_TEST_SWEEP_RUNS:-64}"
CHAOS_RUNS="${BATTLE_TEST_CHAOS_RUNS:-128}"
SOAK_MINUTES="${BATTLE_TEST_SOAK_MINUTES:-3}"
FUZZ_ITERATIONS="${BATTLE_TEST_FUZZ_ITERATIONS:-200000}"
SKIP_S3="${BATTLE_TEST_SKIP_S3:-0}"

log() { printf 'battle-test: %s\n' "$*"; }

# Run the harness through cargo, so no binary path is hard-coded.
validator() { cargo run --quiet -p bkxpro-validator --bin bkxpro-validator -- "$@"; }

# Scratch is disposable on success and evidence on failure.
cleanup() {
  if [ "$?" -eq 0 ]; then
    rm -rf "$WORK" || true
  else
    log "failure: artifacts kept at $WORK"
  fi
}
trap cleanup EXIT

cd "$REPO_ROOT"
log "work dir: $WORK (kept on failure)"
log "sweep base seed: $SEED (pin with BATTLE_TEST_SEED)"

# ===== build =====
log "building bkxpro-validator"
cargo build -p bkxpro-validator --bin bkxpro-validator

# ===== fuzz corpus =====
# The corpus is required by validator/tests/fuzz.rs but not committed (see the
# header): regenerate it before any test run, and prove generation is
# deterministic by writing it twice and diffing.
log "regenerating the fuzz corpus (seed 9)"
validator fuzz --seed 9 --write-corpus "$REPO_ROOT/codec/fuzz/corpus"
validator fuzz --seed 9 --write-corpus "$WORK/corpus-a"
validator fuzz --seed 9 --write-corpus "$WORK/corpus-b"
diff -r "$WORK/corpus-a" "$WORK/corpus-b"
log "corpus generation is byte-identical across runs"

# ===== test suites =====
log "validator test suite"
cargo test -p bkxpro-validator

log "workspace test suite"
cargo test --workspace

log "nightly seed sweeps (ignored tests)"
cargo test -p bkxpro-validator --test seed_sweep -- --ignored

# ===== deterministic simulator: matrix, sweeps, replay =====
log "sim matrix at seed 1 with double-run replay"
validator sim --all --seed 1 --replay \
  --log "$WORK/sim-runs.jsonl" --incidents "$WORK/incidents"

log "sim matrix sweep: $SWEEP_RUNS consecutive seeds from $SEED, replayed"
validator sim --all --seed "$SEED" --runs "$SWEEP_RUNS" --replay \
  --log "$WORK/sim-runs.jsonl" --incidents "$WORK/incidents" --quiet

log "deep chaos sweeps: $CHAOS_RUNS seeds each, replayed"
validator sim --scenario s3-chaos --seed "$SEED" --runs "$CHAOS_RUNS" --replay \
  --log "$WORK/sim-runs.jsonl" --incidents "$WORK/incidents" --quiet
validator sim --scenario outage --seed "$SEED" --runs "$CHAOS_RUNS" --replay \
  --log "$WORK/sim-runs.jsonl" --incidents "$WORK/incidents" --quiet

log "run-log dashboard"
validator dashboard --log "$WORK/sim-runs.jsonl"

# ===== real SIGKILL rounds =====
log "kill-soak: 8 real SIGKILL rounds against a child writer"
validator kill-soak --seed 7 --rounds 8 \
  --dir "$WORK/kill-soak" --payload-bytes 1024 --seal-at 32

# ===== soak =====
log "soak: $SOAK_MINUTES minute(s), budgets, one kill round every 8 runs"
validator soak --seed 1 --minutes "$SOAK_MINUTES" --kill-every 8 \
  --dir "$WORK/soak" \
  --rss-limit-mib 2048 --fd-limit 8192 --dir-limit-mib 4096 \
  --log "$WORK/soak-runs.jsonl" --incidents "$WORK/soak-incidents"

# ===== parser fuzzing =====
log "structural fuzz: $FUZZ_ITERATIONS iterations"
validator fuzz --seed 9 --iterations "$FUZZ_ITERATIONS"
log "tail fuzz: $FUZZ_ITERATIONS iterations"
validator fuzz --tail --seed 9 --iterations "$FUZZ_ITERATIONS"

# ===== real S3 endpoint =====
if [ "$SKIP_S3" = "1" ]; then
  log "skipping the real-endpoint legs (BATTLE_TEST_SKIP_S3=1)"
else
  export BKXPRO_S3_TEST_ENDPOINT="${BKXPRO_S3_TEST_ENDPOINT:-http://127.0.0.1:9000}"
  export BKXPRO_S3_TEST_BUCKET="${BKXPRO_S3_TEST_BUCKET:-bkxpro-staging}"
  export BKXPRO_S3_TEST_ACCESS_KEY="${BKXPRO_S3_TEST_ACCESS_KEY:-localadmin}"
  export BKXPRO_S3_TEST_SECRET_KEY="${BKXPRO_S3_TEST_SECRET_KEY:-localdevsecret123}"

  if curl -sf -o /dev/null --max-time 5 \
      "${BKXPRO_S3_TEST_ENDPOINT%/}/minio/health/live"; then
    if command -v aws >/dev/null 2>&1; then
      log "ensuring bucket $BKXPRO_S3_TEST_BUCKET exists"
      export AWS_ACCESS_KEY_ID="$BKXPRO_S3_TEST_ACCESS_KEY"
      export AWS_SECRET_ACCESS_KEY="$BKXPRO_S3_TEST_SECRET_KEY"
      export AWS_DEFAULT_REGION="${BKXPRO_S3_TEST_REGION:-us-east-1}"
      aws --endpoint-url "$BKXPRO_S3_TEST_ENDPOINT" s3api create-bucket \
          --bucket "$BKXPRO_S3_TEST_BUCKET" >/dev/null 2>&1 \
        || aws --endpoint-url "$BKXPRO_S3_TEST_ENDPOINT" s3api head-bucket \
          --bucket "$BKXPRO_S3_TEST_BUCKET" >/dev/null
    else
      log "aws CLI not found; assuming bucket $BKXPRO_S3_TEST_BUCKET already exists"
    fi

    log "clean real-endpoint lifecycle run"
    validator s3-run --seed 3 --segments 2 --entries 512 --payload-bytes 4000

    log "real-endpoint chaos: endpoint killed mid-run, 2 rounds"
    "$REPO_ROOT/validator/scripts/s3-chaos.sh" \
      --rounds 2 --segments 2 --entries 2100 --payload-bytes 4000 \
      --outage-seconds 10 --work "$WORK/s3-chaos"
  else
    log "skipping the real-endpoint legs: ${BKXPRO_S3_TEST_ENDPOINT} is not reachable"
  fi
fi

# ===== kernel faults (root only) =====
if [ "$(id -u)" -eq 0 ]; then
  log "kernel fault targets on XFS and ext4 (dm-log-writes, dm-flakey, ...)"
  "$REPO_ROOT/validator/scripts/kernel-faults.sh" --fs both --seconds 8 \
    --work "$WORK/kernel-faults"
else
  log "skipping kernel fault targets: requires root; run"
  log "  sudo $REPO_ROOT/validator/scripts/kernel-faults.sh --fs both --seconds 8 --work $WORK/kernel-faults"
fi

log "campaign complete"
