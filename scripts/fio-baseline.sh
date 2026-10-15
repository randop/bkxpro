#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 /path/to/dedicated-test-dir" >&2
  echo "Use a disposable directory on the same device/filesystem as staging; never use live data." >&2
  exit 2
fi

DIR=$(realpath "$1")
[[ -d "$DIR" && -w "$DIR" ]] || {
  echo "test directory must exist and be writable: $DIR" >&2
  exit 2
}
command -v fio >/dev/null || {
  echo "fio is required (install fio with io_uring support)" >&2
  exit 127
}

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="$DIR/fio-bkxpro-$STAMP"
mkdir -p "$OUT"
SIZE=${SIZE:-512m}
RUNTIME=${RUNTIME:-30}
BS=${BS:-4k}
DEPTH=${DEPTH:-64}

{
  echo "utc=$STAMP"
  echo "kernel=$(uname -r)"
  echo "filesystem=$(stat -f -c %T "$DIR")"
  echo "fio=$(fio --version)"
  echo "directory=$DIR"
  echo "size=$SIZE runtime=$RUNTIME bs=$BS iodepth=$DEPTH"
  findmnt -T "$DIR" || true
  df -T "$DIR"
} >"$OUT/environment.txt"

run_case() (
  local name=$1
  shift
  local file="$DIR/.fio-bkxpro-$STAMP-$name.tmp"
  trap 'rm -f -- "$file"' EXIT
  echo "Running $name -> $OUT/$name.json"
  fio --name="$name" --filename="$file" --size="$SIZE" --time_based=1 \
    --runtime="$RUNTIME" --ioengine=io_uring --direct=1 --rw=write --bs="$BS" \
    --iodepth="$DEPTH" --numjobs=1 --group_reporting=1 --output-format=json \
    --output="$OUT/$name.json" "$@"
  sync -d "$DIR"
)

run_case direct-nosync
run_case direct-fsync-64 --fsync=64
run_case direct-fdatasync-64 --fdatasync=64
run_case direct-dsync --sync=dsync --iodepth=1

cat >"$OUT/README.txt" <<TXT
Fio output directory: $OUT

These are per-environment device ceilings, not acceptance results by themselves.
Record the exact staging device and repeat on ext4 and XFS where both are supported.
Run the application benchmark with the same block sizes/load and record commit p50/p99,
throughput, and perf trace counts of io_uring_enter next to these JSON files.
The fio dsync case is a comparison baseline; it does not prove that RWF_DSYNC in the
application is the fastest or semantically identical on every device.
TXT

echo "Wrote baseline output to $OUT"
