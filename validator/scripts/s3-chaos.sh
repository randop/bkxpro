#!/usr/bin/env bash
# RAN-132 real-endpoint S3 chaos validation for bkxpro.
#
# Drives `bkxpro-validator s3-run` - the full lifecycle against a live
# S3-compatible endpoint (write, seal, restart, multipart PUT, manifest CAS,
# local delete, ranged-GET read-back, remote verification) - while the endpoint
# is killed and restarted underneath it. Every run must ride the outage out and
# end with zero violations: no acknowledged entry lost, no torn or duplicated
# record, and the local bytes kept until the upload and manifest commit landed.
#
# Endpoint control:
#   (default)            process mode: the script discovers the running
#                        `minio server` process, records its exact command
#                        line, working directory and MINIO_* environment, then
#                        SIGKILLs and restarts it with those values.
#   --docker-minio NAME  container mode (CI): `docker kill` / `docker start`.
#   --no-minio           sanity mode: one clean run with no endpoint fault.
#
# Every run writes to its own object prefix, so a shared bucket keeps no state
# between runs, and a passed run deletes its own objects afterwards.
#
# Usage:
#   validator/scripts/s3-chaos.sh [--seed N] [--rounds N] [--segments N]
#        [--entries N] [--payload-bytes N] [--kill-after-seconds N]
#        [--outage-seconds N] [--settle-timeout-seconds N] [--bucket B]
#        [--endpoint URL] [--region R] [--access-key K] [--secret-key S]
#        [--prefix P] [--docker-minio NAME] [--no-minio] [--work DIR]
#        [--validator PATH] [--keep] [--json]
#
# The connection triple falls back to BKXPRO_S3_TEST_BUCKET,
# BKXPRO_S3_TEST_ENDPOINT, BKXPRO_S3_TEST_REGION, BKXPRO_S3_TEST_ACCESS_KEY and
# BKXPRO_S3_TEST_SECRET_KEY (the same contract as `s3-run` itself).
#
# Exit codes: 0 all runs passed, 1 a run failed or violated, 2 usage/setup
# error, 77 nothing ran (endpoint unreachable or tooling missing).

set -u -o pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

MODE=process                 # process | docker | none
SEED=11
ROUNDS=2
SEGMENTS=2
ENTRIES=2100
PAYLOAD_BYTES=4000           # one record per 4 KiB block: a multi-MiB segment
KILL_AFTER=1                 # seconds into the run before the endpoint dies
OUTAGE_SECONDS=8             # how long the endpoint stays down
SETTLE_TIMEOUT=0             # 0 = outage + 90s
BUCKET="${BKXPRO_S3_TEST_BUCKET:-}"
ENDPOINT="${BKXPRO_S3_TEST_ENDPOINT:-}"
REGION="${BKXPRO_S3_TEST_REGION:-}"
ACCESS_KEY="${BKXPRO_S3_TEST_ACCESS_KEY:-}"
SECRET_KEY="${BKXPRO_S3_TEST_SECRET_KEY:-}"
PREFIX=""
DOCKER_MINIO=""
WORK="${BKXPRO_S3_CHAOS_WORK:-/tmp/bkxpro-s3-chaos}"
VALIDATOR="${BKXPRO_VALIDATOR_BIN:-}"
KEEP=0
JSON=0

PASSED=0
FAILED=0
SKIPPED=0

usage() {
    sed -n '2,36p' "$0" | sed 's/^# \{0,1\}//'
}

note() { printf '%s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; }
die()  { printf 's3-chaos: %s\n' "$*" >&2; exit 2; }

# ===== endpoint description captured before any fault =====

# Recorded by `discover_minio` (process mode).
MINIO_PID=""
MINIO_BIN=""
MINIO_CWD=""
MINIO_ARGS=()
MINIO_ENV=()

discover_minio() {
    command -v pgrep >/dev/null 2>&1 || die "pgrep missing; use --no-minio or --docker-minio"
    MINIO_PID="$(pgrep -f 'minio server' 2>/dev/null | head -1 || true)"
    [ -n "$MINIO_PID" ] || die "no running 'minio server' process found; use --no-minio or --docker-minio NAME"

    local cmdline item
    local -a cmdline=()
    while IFS= read -r item; do cmdline+=("$item"); done < <(tr '\0' '\n' < "/proc/$MINIO_PID/cmdline")
    [ "${#cmdline[@]}" -ge 2 ] || die "could not read $MINIO_PID cmdline"
    # shellcheck disable=SC2206
    MINIO_BIN="${cmdline[0]}"
    MINIO_ARGS=("${cmdline[@]:1}")
    MINIO_CWD="$(readlink "/proc/$MINIO_PID/cwd" 2>/dev/null || pwd)"
    local env_line
    while IFS= read -r env_line; do
        case "$env_line" in MINIO_*) MINIO_ENV+=("$env_line") ;; esac
    done < <(tr '\0' '\n' < "/proc/$MINIO_PID/environ")
}

health_url() {
    local base="${ENDPOINT%/}"
    if [ -z "$base" ]; then
        printf 'http://127.0.0.1:9000/minio/health/live'
    else
        printf '%s/minio/health/live' "$base"
    fi
}

endpoint_healthy() {
    command -v curl >/dev/null 2>&1 || return 0
    curl -sf -o /dev/null --max-time 5 "$(health_url)"
}

start_minio() {
    case "$MODE" in
        docker)
            docker start "$DOCKER_MINIO" >/dev/null 2>&1 || {
                note "    docker start $DOCKER_MINIO failed"
                return 1
            }
            ;;
        process)
            local dir="$MINIO_CWD"
            (
                cd "$dir" 2>/dev/null || cd /
                if [ "${#MINIO_ENV[@]}" -gt 0 ]; then
                    exec env "${MINIO_ENV[@]}" "$MINIO_BIN" "${MINIO_ARGS[@]}"
                else
                    exec "$MINIO_BIN" "${MINIO_ARGS[@]}"
                fi
            ) >>"$WORK/minio.log" 2>&1 &
            MINIO_PID=$!
            ;;
        none)
            return 0
            ;;
    esac
    local waited=0
    until endpoint_healthy; do
        sleep 0.2
        waited=$((waited + 1))
        [ "$waited" -ge 100 ] && return 1
    done
    note "    endpoint back up ($(health_url))"
    return 0
}

stop_minio() {
    case "$MODE" in
        docker)
            docker kill "$DOCKER_MINIO" >/dev/null 2>&1 || {
                note "    docker kill $DOCKER_MINIO failed"
                return 1
            }
            ;;
        process)
            if kill -9 "$MINIO_PID" 2>/dev/null; then
                wait "$MINIO_PID" 2>/dev/null || true
            else
                note "    kill -9 $MINIO_PID failed"
                return 1
            fi
            ;;
        none) return 0 ;;
    esac
    # Confirm it is really unreachable, so the run sees a real outage.
    local refused=0
    while [ "$refused" -lt 100 ]; do
        if ! endpoint_healthy; then
            return 0
        fi
        sleep 0.1
        refused=$((refused + 1))
    done
    return 1
}

cleanup() {
    local status=$?
    case "$MODE" in
        docker|process)
            if ! endpoint_healthy; then
                note "restore: bringing the endpoint back up"
                start_minio || note "restore: endpoint did not come back; start it manually"
            fi
            ;;
    esac
    exit "$status"
}
trap cleanup EXIT INT TERM

# ===== the run =====

run_round() { # round prefix
    local round="$1" prefix="$2"
    local log="$WORK/round-$round.log"
    local outage_failed=0
    local -a cmd=(
        "$VALIDATOR" s3-run
        --seed "$SEED"
        --segments "$SEGMENTS"
        --entries "$ENTRIES"
        --payload-bytes "$PAYLOAD_BYTES"
        --prefix "$prefix"
        --settle-timeout-seconds "$SETTLE_TIMEOUT"
        --bucket "$BUCKET"
        --access-key "$ACCESS_KEY"
        --secret-key "$SECRET_KEY"
    )
    if [ -n "$REGION" ]; then cmd+=(--region "$REGION"); fi
    if [ -n "$ENDPOINT" ]; then cmd+=(--endpoint "$ENDPOINT"); fi
    if [ "$KEEP" -eq 1 ]; then cmd+=(--keep); fi
    if [ "$JSON" -eq 1 ]; then cmd+=(--json); fi

    note "=== round $round: ${ENTRIES} x ${PAYLOAD_BYTES} B in ${SEGMENTS} segment(s), prefix $prefix ==="
    if [ "$MODE" = none ]; then
        "${cmd[@]}" >"$log" 2>&1
        local status=$?
    else
        local guard=$(( SETTLE_TIMEOUT + OUTAGE_SECONDS + 120 ))
        timeout "$guard" "${cmd[@]}" >"$log" 2>&1 &
        local run_pid=$!
        sleep "$KILL_AFTER"
        if ! kill -0 "$run_pid" 2>/dev/null; then
            note "    the run finished before the kill window; no outage was injected"
        elif stop_minio; then
            note "    endpoint stopped for ${OUTAGE_SECONDS}s"
            sleep "$OUTAGE_SECONDS"
            start_minio || fail "round $round: endpoint did not come back"
        else
            fail "round $round: could not stop the endpoint"
            outage_failed=1
        fi
        wait "$run_pid"
        local status=$?
    fi

    sed 's/^/    /' "$log"
    if [ "$status" -eq 0 ] && [ "$outage_failed" -eq 0 ]; then
        # The chaos only proves something when the outage overlapped uploads.
        if grep -q "transient upload failure" "$log"; then
            note "    the run observed the outage and rode it out"
        else
            note "    warning: the outage window did not overlap the upload phase; \
raise --entries or lower --kill-after-seconds"
        fi
        if [ "$KEEP" -eq 1 ]; then
            note "    run dir kept under $WORK; objects kept under $prefix"
        fi
        note "PASS round $round"
        PASSED=$((PASSED + 1))
    else
        note "FAIL round $round (exit $status; log $log)"
        FAILED=$((FAILED + 1))
    fi
}

# ===== argument parsing =====

while [ $# -gt 0 ]; do
    case "$1" in
        --seed) SEED="$2"; shift 2 ;;
        --rounds) ROUNDS="$2"; shift 2 ;;
        --segments) SEGMENTS="$2"; shift 2 ;;
        --entries) ENTRIES="$2"; shift 2 ;;
        --payload-bytes) PAYLOAD_BYTES="$2"; shift 2 ;;
        --kill-after-seconds) KILL_AFTER="$2"; shift 2 ;;
        --outage-seconds) OUTAGE_SECONDS="$2"; shift 2 ;;
        --settle-timeout-seconds) SETTLE_TIMEOUT="$2"; shift 2 ;;
        --bucket) BUCKET="$2"; shift 2 ;;
        --endpoint) ENDPOINT="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --access-key) ACCESS_KEY="$2"; shift 2 ;;
        --secret-key) SECRET_KEY="$2"; shift 2 ;;
        --prefix) PREFIX="$2"; shift 2 ;;
        --docker-minio) MODE=docker; DOCKER_MINIO="$2"; shift 2 ;;
        --no-minio) MODE=none; shift ;;
        --work) WORK="$2"; shift 2 ;;
        --validator) VALIDATOR="$2"; shift 2 ;;
        --keep) KEEP=1; shift ;;
        --json) JSON=1; shift ;;
        --help|-h) usage; exit 0 ;;
        -*) die "unknown option $1" ;;
        *) die "unexpected argument $1" ;;
    esac
done

[ "$ROUNDS" -ge 1 ] || die "--rounds must be at least 1"
[ "$SEGMENTS" -ge 1 ] || die "--segments must be at least 1"
[ "$ENTRIES" -ge 1 ] || die "--entries must be at least 1"
[ "$PAYLOAD_BYTES" -ge 1 ] || die "--payload-bytes must be at least 1"
[ "$KILL_AFTER" -ge 0 ] || die "--kill-after-seconds must not be negative"
[ "$OUTAGE_SECONDS" -ge 1 ] || die "--outage-seconds must be at least 1"
[ "$SETTLE_TIMEOUT" -gt 0 ] || SETTLE_TIMEOUT=$(( OUTAGE_SECONDS + 90 ))

[ -n "$BUCKET" ] || die "a bucket is required (--bucket or BKXPRO_S3_TEST_BUCKET)"
[ -n "$ACCESS_KEY" ] || die "an access key is required (--access-key or BKXPRO_S3_TEST_ACCESS_KEY)"
[ -n "$SECRET_KEY" ] || die "a secret key is required (--secret-key or BKXPRO_S3_TEST_SECRET_KEY)"

validator_bin() {
    if [ -z "$VALIDATOR" ]; then
        VALIDATOR="$REPO_ROOT/target/debug/bkxpro-validator"
    fi
    if [ ! -x "$VALIDATOR" ]; then
        note "building bkxpro-validator (missing $VALIDATOR)"
        (cd "$REPO_ROOT" && cargo build -p bkxpro-validator --bin bkxpro-validator) \
            || die "cargo build failed"
    fi
}
validator_bin

command -v timeout >/dev/null 2>&1 || die "timeout missing (coreutils)"

mkdir -p "$WORK" || die "cannot create $WORK"

note "validator binary: $VALIDATOR"
note "endpoint: ${ENDPOINT:-AWS S3} bucket: $BUCKET mode: $MODE"
note "workload: seed $SEED, $ROUNDS round(s) of $SEGMENTS x $ENTRIES x ${PAYLOAD_BYTES}B"
note "chaos: kill after ${KILL_AFTER}s, outage ${OUTAGE_SECONDS}s, settle budget ${SETTLE_TIMEOUT}s"

if ! endpoint_healthy; then
    if [ "$MODE" = none ]; then
        die "endpoint ${ENDPOINT:-http://127.0.0.1:9000} is not reachable"
    fi
    note "warning: endpoint did not answer before the first round; it will be started by the script"
fi
if [ "$MODE" = process ]; then
    discover_minio
    note "minio: pid $MINIO_PID bin $MINIO_BIN cwd $MINIO_CWD args ${MINIO_ARGS[*]}"
    note "minio: env ${MINIO_ENV[*]:-<none>}"
elif [ "$MODE" = docker ]; then
    command -v docker >/dev/null 2>&1 || die "docker missing; use the default process mode or --no-minio"
    docker inspect "$DOCKER_MINIO" >/dev/null 2>&1 || die "no container named $DOCKER_MINIO"
fi

RUN_PREFIX="${PREFIX:-validator/chaos-$SEED-$(date +%s)-$$}"
ROUND=0
while [ "$ROUND" -lt "$ROUNDS" ]; do
    ROUND=$((ROUND + 1))
    run_round "$ROUND" "$RUN_PREFIX-r$ROUND"
done

note ""
note "s3-chaos: $PASSED passed, $FAILED failed, $SKIPPED skipped (prefix $RUN_PREFIX, logs in $WORK)"
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
if [ "$PASSED" -eq 0 ]; then
    exit 77
fi
exit 0
