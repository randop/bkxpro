#!/usr/bin/env bash
# RAN-132 kernel-level fault validation for bkxpro.
#
# Runs the kill-soak child writer against real XFS/ext4 filesystems layered on
# kernel fault targets, then recovers the session with `bkxpro-validator verify`
# and checks the durability contract: every acknowledged entry must be
# recoverable with its seeded payload, and a fenced/erroring device must never
# produce a torn or duplicated record.
#
# Scenarios:
#   log-writes         dm-log-writes records every flush-ordered write; the log
#                      is then replayed to every mark and each replayed image
#                      is mounted and verified (the power-cut model).
#   flakey             dm-flakey error windows fail live writes (error_writes),
#                      then the device is restored and the fs verified.
#   dust               dm-dust read failures on injected bad blocks during live
#                      writes, then a clean verify.
#   null-blk           null_blk memory-backed device: fastest O_DIRECT path,
#                      workload + kill + remount + verify.
#   fail-make-request  per-device CONFIG_FAIL_MAKE_REQUEST injection: every
#                      block request fails for a window, then verify.
#   remount-ro         filesystem shutdown mid-append; every ack before the
#                      shutdown must recover (requires xfs_io).
#   enospc             fill a small filesystem until ENOSPC mid-append; every
#                      ack before the error must recover.
#
# Requirements: Linux, root, losetup, dmsetup with the log-writes/flakey/dust
# targets, mkfs.xfs/mkfs.ext4, mount, debugfs for fail-make-request, configfs
# for null-blk, and the `replay-log` userspace tool for log-writes (build from
# xfstests: `make -C xfstests-dev/src log-writes`, or point
# BKXPRO_REPLAY_LOG at it).
#
# Usage:
#   sudo validator/scripts/kernel-faults.sh [SCENARIO ...] [--fs xfs|ext4|both]
#        [--seconds N] [--work DIR] [--keep] [--validator PATH]
#
# Exit codes: 0 pass, 1 failure, 2 usage/setup error, 77 nothing ran (skipped).

set -u -o pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

SCENARIOS=()
FILESYSTEMS=(xfs ext4)
SECONDS_PER_RUN=8
WORK="${BKXPRO_KERNEL_WORK:-/tmp/bkxpro-kernel-faults}"
KEEP=0
VALIDATOR="${BKXPRO_VALIDATOR_BIN:-}"
REPLAY_LOG="${BKXPRO_REPLAY_LOG:-replay-log}"
FLAKEY_FEATURE="${BKXPRO_FLAKEY_FEATURE:-error_writes}"
SEED="${BKXPRO_KERNEL_SEED:-2026}"

PASSED=0
FAILED=0
SKIPPED=0

ALL_SCENARIOS=(log-writes flakey dust null-blk fail-make-request remount-ro enospc)

# Active resources, cleaned up by `cleanup` on any exit path.
ACTIVE_CHILD_PID=""
ACTIVE_MOUNTS=()
ACTIVE_DM=()
ACTIVE_LOOPS=()
LOOP_DEVICE=""
ACTIVE_NULLB=""

usage() {
    sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
}

note()  { printf '%s\n' "$*"; }
fail()  { printf 'FAIL: %s\n' "$*" >&2; }
die()   { printf 'kernel-faults: %s\n' "$*" >&2; exit 2; }

require_root() {
    [ "$(id -u)" -eq 0 ] || die "must run as root"
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || return 1
    return 0
}

have_dm_target() {
    # dmsetup targets lists built-in and module targets.
    dmsetup targets 2>/dev/null | awk '{print $1}' | grep -qx "$1"
}

# ===== cleanup =====

stop_child() {
    if [ -n "$ACTIVE_CHILD_PID" ] && kill -0 "$ACTIVE_CHILD_PID" 2>/dev/null; then
        kill -9 "$ACTIVE_CHILD_PID" 2>/dev/null || true
    fi
    if [ -n "$ACTIVE_CHILD_PID" ]; then
        wait "$ACTIVE_CHILD_PID" 2>/dev/null || true
    fi
    ACTIVE_CHILD_PID=""
}

unmount_dir() {
    local mnt="$1"
    if mountpoint -q "$mnt" 2>/dev/null; then
        sync || true
        umount "$mnt" 2>/dev/null || umount -l "$mnt" 2>/dev/null || true
    fi
}

cleanup_resources() {
    stop_child
    local mnt
    for mnt in "${ACTIVE_MOUNTS[@]:-}"; do
        [ -n "$mnt" ] && unmount_dir "$mnt"
    done
    local dm
    for dm in "${ACTIVE_DM[@]:-}"; do
        [ -n "$dm" ] && dmsetup remove -f "$dm" >/dev/null 2>&1 || true
    done
    local loop
    for loop in "${ACTIVE_LOOPS[@]:-}"; do
        [ -n "$loop" ] && losetup -d "$loop" >/dev/null 2>&1 || true
    done
    if [ -n "$ACTIVE_NULLB" ]; then
        echo 0 > "/sys/kernel/config/nullb/$ACTIVE_NULLB/power" 2>/dev/null || true
        rmdir "/sys/kernel/config/nullb/$ACTIVE_NULLB" 2>/dev/null || true
    fi
    ACTIVE_MOUNTS=()
    ACTIVE_DM=()
    ACTIVE_LOOPS=()
    ACTIVE_NULLB=""
}

cleanup() {
    local status=$?
    cleanup_resources
    exit "$status"
}
trap cleanup EXIT INT TERM

# ===== shared helpers =====

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

new_image() { # path mib
    rm -f "$1"
    truncate -s "$2M" "$1"
}

loop_up() { # path -> LOOP_DEVICE (must run in the parent shell)
    LOOP_DEVICE="$(losetup --show -f "$1")" || return 1
    ACTIVE_LOOPS+=("$LOOP_DEVICE")
}

dm_create() { # name table
    local name="$1" table="$2"
    dmsetup create "$name" --table "$table" || return 1
    ACTIVE_DM+=("$name")
}

dm_remove() { # name
    dmsetup remove "$1" 2>/dev/null || dmsetup remove -f "$1" 2>/dev/null || true
}

mkfs_fs() { # dev fs
    local dev="$1" fs="$2"
    case "$fs" in
        xfs)  mkfs.xfs -f -q "$dev" ;;
        ext4) mkfs.ext4 -q -F "$dev" ;;
        *) die "unknown filesystem $fs" ;;
    esac
}

mount_fs() { # dev fs mnt
    local dev="$1" fs="$2" mnt="$3"
    mkdir -p "$mnt"
    mount -t "$fs" -o noatime "$dev" "$mnt" || return 1
    ACTIVE_MOUNTS+=("$mnt")
}

unmount_fs() { # mnt
    local mnt="$1"
    unmount_dir "$mnt"
}

start_workload() { # dir seed [step_micros]
    local dir="$1" seed="$2" step="${3:-500}"
    mkdir -p "$dir"
    "$VALIDATOR" kill-soak-child --dir "$dir" --seed "$seed" \
        --payload-bytes 1024 --seal-at 32 --ledgers 2 \
        --step-micros "$step" \
        >"$dir/child.out" 2>"$dir/child.err" &
    ACTIVE_CHILD_PID=$!
}

wait_for_acks() { # dir timeout_seconds
    local dir="$1" timeout="$2" waited=0
    while [ "$waited" -lt $((timeout * 100)) ]; do
        if [ -s "$dir/acked.jsonl" ]; then
            return 0
        fi
        if ! kill -0 "$ACTIVE_CHILD_PID" 2>/dev/null; then
            return 1
        fi
        sleep 0.01
        waited=$((waited + 1))
    done
    return 1
}

workload_alive() {
    [ -n "$ACTIVE_CHILD_PID" ] && kill -0 "$ACTIVE_CHILD_PID" 2>/dev/null
}

describe_child_status() { # dir
    local dir="$1"
    if [ -s "$dir/child.err" ]; then
        note "    child stderr: $(tail -n 1 "$dir/child.err")"
    fi
}

verify_session() { # dir
    local dir="$1"
    if [ ! -d "$dir/segments" ]; then
        note "    no session at $dir (nothing to verify at this point)"
        return 0
    fi
    if "$VALIDATOR" verify --dir "$dir"; then
        return 0
    fi
    return 1
}

# A device that silently drops writes violates its own contract; the harness is
# expected to detect the loss. Those observations are reported, not failed.
expect_violations() {
    [ "$FLAKEY_FEATURE" = "drop_writes" ]
}

report() { # name ok
    local name="$1" ok="$2"
    if [ "$ok" -eq 0 ]; then
        note "PASS $name"
        PASSED=$((PASSED + 1))
    else
        note "FAIL $name"
        FAILED=$((FAILED + 1))
    fi
}

skip() { # name reason
    note "SKIP $1: $2"
    SKIPPED=$((SKIPPED + 1))
}

# ===== scenarios =====

scenario_log_writes() { # fs
    local fs="$1" name="log-writes/$fs"

    require_cmd losetup || { skip "$name" "losetup missing"; return; }
    require_cmd dmsetup || { skip "$name" "dmsetup missing"; return; }
    have_dm_target log-writes || { skip "$name" "dm target log-writes unavailable"; return; }
    require_cmd "$REPLAY_LOG" || {
        skip "$name" "replay-log missing (build it from xfstests: make -C xfstests-dev/src log-writes)"
        return
    }

    local dir="$SCENARIO_DIR"
    mkdir -p "$dir"
    local data_img="$dir/data.img" log_img="$dir/log.img"
    new_image "$data_img" 256
    new_image "$log_img" 512
    local data_loop log_loop
    loop_up "$data_img" || { skip "$name" "losetup failed"; return; }
    data_loop="$LOOP_DEVICE"
    loop_up "$log_img" || { skip "$name" "losetup failed"; return; }
    log_loop="$LOOP_DEVICE"

    local sectors dm="kf-logwrites"
    sectors="$(blockdev --getsz "$data_loop")"
    dm_create "$dm" "0 $sectors log-writes $data_loop $log_loop" \
        || { skip "$name" "dmsetup create failed"; return; }
    local dev="/dev/mapper/$dm" mnt="$dir/mnt"
    mkfs_fs "$dev" "$fs" || { skip "$name" "mkfs failed"; return; }
    dmsetup message "$dm" 0 mark mkfs >/dev/null
    mount_fs "$dev" "$fs" "$mnt" || { skip "$name" "mount failed"; return; }

    local work="$mnt/work"
    start_workload "$work" "$SEED"
    if ! wait_for_acks "$work" 30; then
        describe_child_status "$work"
        stop_child
        report "$name" 1
        return
    fi

    # Marks sample the write stream; each is a candidate power-cut point.
    local marks=() index interval
    interval="$(awk "BEGIN {print $SECONDS_PER_RUN / 4}")"
    for index in $(seq 1 4); do
        sleep "$interval"
        dmsetup message "$dm" 0 mark "step-$index" >/dev/null
        marks+=("step-$index")
    done
    # The kill lands wherever it lands; the final mark is written by dmsetup
    # remove below (dm-log-writes-end).
    stop_child
    unmount_fs "$mnt"
    dm_remove "$dm"

    local ok=0 mark
    for mark in mkfs "${marks[@]}"; do
        if ! "$REPLAY_LOG" --log "$log_loop" --replay "$data_loop" --end-mark "$mark" >/dev/null 2>&1; then
            note "    mark $mark: replay failed"
            ok=1
            continue
        fi
        if ! mount_fs "$data_loop" "$fs" "$mnt"; then
            note "    mark $mark: filesystem did not mount after replay (fs-level failure)"
            ok=1
            continue
        fi
        note "    mark $mark: replayed and mounted"
        verify_session "$work" || ok=1
        unmount_fs "$mnt"
    done
    report "$name" "$ok"
}

scenario_flakey() { # fs
    local fs="$1" name="flakey/$fs"
    require_cmd dmsetup || { skip "$name" "dmsetup missing"; return; }
    have_dm_target flakey || { skip "$name" "dm target flakey unavailable"; return; }

    local dir="$SCENARIO_DIR"
    mkdir -p "$dir"
    local img="$dir/data.img"
    new_image "$img" 256
    local loop
    loop_up "$img" || { skip "$name" "losetup failed"; return; }
    loop="$LOOP_DEVICE"

    local sectors dm="kf-flakey" dev="/dev/mapper/kf-flakey"
    sectors="$(blockdev --getsz "$loop")"
    # A long initial up window lets mkfs finish before faults start.
    dm_create "$dm" "0 $sectors flakey $loop 0 3600 0" \
        || { skip "$name" "dmsetup create failed"; return; }
    mkfs_fs "$dev" "$fs" || { skip "$name" "mkfs failed"; return; }

    local mnt="$dir/mnt"
    mount_fs "$dev" "$fs" "$mnt" || { skip "$name" "mount failed"; return; }

    # Now arm the fault windows on the live mapping.
    dmsetup reload "$dm" --table "0 $sectors flakey $loop 0 2 2 1 $FLAKEY_FEATURE" || {
        skip "$name" "reload with $FLAKEY_FEATURE failed"
        return
    }
    dmsetup resume "$dm" >/dev/null

    local work="$mnt/work"
    start_workload "$work" "$SEED"
    wait_for_acks "$work" 30 || true
    sleep "$SECONDS_PER_RUN"
    local saw_error=0
    if [ -s "$work/child.err" ]; then
        saw_error=1
    fi
    if workload_alive; then
        stop_child
    fi

    # Restore a transparent device before the clean unmount.
    dmsetup reload "$dm" --table "0 $sectors linear $loop 0" || true
    dmsetup resume "$dm" >/dev/null 2>&1 || true
    unmount_fs "$mnt"
    dm_remove "$dm"
    ACTIVE_DM=()

    if ! mount_fs "$loop" "$fs" "$mnt"; then
        report "$name" 1
        return
    fi
    local ok=0
    if ! verify_session "$work"; then
        if [ "$saw_error" -eq 1 ] && expect_violations; then
            note "    violations observed with $FLAKEY_FEATURE (device contract violation, expected for this feature)"
            ok=0
        else
            ok=1
        fi
    fi
    if [ "$saw_error" -eq 0 ]; then
        note "    warning: the fault window never surfaced an error to the child"
    fi
    unmount_fs "$mnt"
    report "$name" "$ok"
}

scenario_dust() { # fs
    local fs="$1" name="dust/$fs"
    require_cmd dmsetup || { skip "$name" "dmsetup missing"; return; }
    have_dm_target dust || { skip "$name" "dm target dust unavailable"; return; }

    local dir="$SCENARIO_DIR"
    mkdir -p "$dir"
    local img="$dir/data.img"
    new_image "$img" 128
    local loop
    loop_up "$img" || { skip "$name" "losetup failed"; return; }
    loop="$LOOP_DEVICE"

    local sectors dm="kf-dust" dev="/dev/mapper/kf-dust"
    sectors="$(blockdev --getsz "$loop")"
    dm_create "$dm" "0 $sectors dust $loop 0 4096" \
        || { skip "$name" "dmsetup create failed"; return; }
    mkfs_fs "$dev" "$fs" || { skip "$name" "mkfs failed"; return; }

    local mnt="$dir/mnt"
    mount_fs "$dev" "$fs" "$mnt" || { skip "$name" "mount failed"; return; }

    local work="$mnt/work"
    start_workload "$work" "$SEED"
    wait_for_acks "$work" 30 || true
    sleep 1

    # Inject bad blocks across the device and enable read failures.
    local total_blocks frac block
    total_blocks=$(( $(blockdev --getsize64 "$loop") / 4096 ))
    for frac in 40 50 60 70; do
        block=$((total_blocks * frac / 100))
        dmsetup message "$dm" 0 addbadblock "$block" >/dev/null 2>&1 || true
    done
    dmsetup message "$dm" 0 enable >/dev/null 2>&1 || true
    note "    bad blocks armed at 40%/50%/60%/70% of $total_blocks 4k blocks"

    # Prove EIO surfaces on direct reads of a bad block.
    block=$((total_blocks * 50 / 100))
    if dd if="$dev" of=/dev/null bs=4096 skip="$block" count=1 iflag=direct 2>/dev/null; then
        note "    warning: direct read of injected bad block did not fail"
    fi

    sleep "$SECONDS_PER_RUN"
    if workload_alive; then
        stop_child
    fi
    dmsetup message "$dm" 0 disable >/dev/null 2>&1 || true
    unmount_fs "$mnt"
    dm_remove "$dm"
    ACTIVE_DM=()

    if ! mount_fs "$loop" "$fs" "$mnt"; then
        report "$name" 1
        return
    fi
    local ok=0
    verify_session "$work" || ok=1
    unmount_fs "$mnt"
    report "$name" "$ok"
}

scenario_null_blk() { # fs
    local fs="$1" name="null-blk/$fs"
    [ -d /sys/kernel/config/nullb ] || {
        modprobe null_blk nr_devices=0 >/dev/null 2>&1 || { skip "$name" "null_blk/configfs unavailable"; return; }
    }
    [ -d /sys/kernel/config/nullb ] || { skip "$name" "configfs nullb unavailable"; return; }

    local dir="$SCENARIO_DIR"
    mkdir -p "$dir"
    local nullb="bkxpro-$$-$fs-$RANDOM"
    mkdir "/sys/kernel/config/nullb/$nullb" 2>/dev/null || {
        skip "$name" "could not create $nullb"
        return
    }
    ACTIVE_NULLB="$nullb"
    echo 1 > "/sys/kernel/config/nullb/$nullb/memory_backed"
    echo 256 > "/sys/kernel/config/nullb/$nullb/size"
    echo 4096 > "/sys/kernel/config/nullb/$nullb/blocksize"
    if ! echo 1 > "/sys/kernel/config/nullb/$nullb/power"; then
        skip "$name" "null_blk power-on failed"
        return
    fi
    local index
    index="$(cat "/sys/kernel/config/nullb/$nullb/index")" || { skip "$name" "null_blk index unavailable"; return; }
    local dev="/dev/nullb$index"
    [ -b "$dev" ] || { skip "$name" "$dev did not appear"; return; }

    mkfs_fs "$dev" "$fs" || { skip "$name" "mkfs failed"; return; }
    local mnt="$dir/mnt"
    mount_fs "$dev" "$fs" "$mnt" || { skip "$name" "mount failed"; return; }

    local work="$mnt/work"
    start_workload "$work" "$SEED"
    if ! wait_for_acks "$work" 30; then
        describe_child_status "$work"
        stop_child
        report "$name" 1
        return
    fi
    sleep "$SECONDS_PER_RUN"
    stop_child
    unmount_fs "$mnt"

    mount_fs "$dev" "$fs" "$mnt" || { report "$name" 1; return; }
    local ok=0
    verify_session "$work" || ok=1
    unmount_fs "$mnt"
    report "$name" "$ok"
}

scenario_fail_make_request() { # fs
    local fs="$1" name="fail-make-request/$fs"
    local debugfs_dir=/sys/kernel/debug/fail_make_request
    if [ ! -d /sys/kernel/debug ]; then
        mount -t debugfs debugfs /sys/kernel/debug 2>/dev/null || true
    fi
    if [ ! -d "$debugfs_dir" ]; then
        modprobe fail_make_request >/dev/null 2>&1 || true
    fi
    [ -d "$debugfs_dir" ] || { skip "$name" "CONFIG_FAIL_MAKE_REQUEST unavailable"; return; }

    local dir="$SCENARIO_DIR"
    mkdir -p "$dir"
    local img="$dir/data.img"
    new_image "$img" 128
    local loop
    loop_up "$img" || { skip "$name" "losetup failed"; return; }
    loop="$LOOP_DEVICE"
    local loop_name
    loop_name="$(basename "$loop")"

    mkfs_fs "$loop" "$fs" || { skip "$name" "mkfs failed"; return; }
    local mnt="$dir/mnt"
    mount_fs "$loop" "$fs" "$mnt" || { skip "$name" "mount failed"; return; }

    local work="$mnt/work"
    start_workload "$work" "$SEED"
    wait_for_acks "$work" 30 || true
    sleep 1

    note "    100% request failures on $loop_name for one window"
    echo 100 > "$debugfs_dir/probability"
    echo 1 > "$debugfs_dir/interval"
    echo -1 > "$debugfs_dir/times"
    echo 0 > "$debugfs_dir/space"
    echo 1 > "/sys/block/$loop_name/make-it-fail"

    # Give the workload time to hit EIO, then disable injection.
    local waited=0
    while [ "$waited" -lt $((SECONDS_PER_RUN * 10)) ]; do
        [ -s "$work/child.err" ] && break
        workload_alive || break
        sleep 0.1
        waited=$((waited + 1))
    done
    echo 0 > "/sys/block/$loop_name/make-it-fail"
    echo 0 > "$debugfs_dir/probability"

    if workload_alive; then
        stop_child
    fi
    sync || true
    unmount_fs "$mnt"
    # The loop device still owns the filesystem; remount the same tree.
    mount_fs "$loop" "$fs" "$mnt" || { report "$name" 1; return; }
    local ok=0
    verify_session "$work" || ok=1
    unmount_fs "$mnt"
    report "$name" "$ok"
}

scenario_remount_ro() { # fs
    local fs="$1" name="remount-ro/$fs"
    require_cmd xfs_io || { skip "$name" "xfs_io missing"; return; }
    local dir="$SCENARIO_DIR"
    mkdir -p "$dir"
    local img="$dir/data.img"
    new_image "$img" 128
    local loop
    loop_up "$img" || { skip "$name" "losetup failed"; return; }
    loop="$LOOP_DEVICE"

    mkfs_fs "$loop" "$fs" || { skip "$name" "mkfs failed"; return; }
    local mnt="$dir/mnt"
    mount_fs "$loop" "$fs" "$mnt" || { skip "$name" "mount failed"; return; }

    local work="$mnt/work"
    start_workload "$work" "$SEED"
    if ! wait_for_acks "$work" 30; then
        describe_child_status "$work"
        stop_child
        report "$name" 1
        return
    fi
    sleep 1
    if ! xfs_io -x -c "shutdown -f" "$mnt"; then
        stop_child
        report "$name" 1
        return
    fi
    note "    filesystem shut down mid-append"

    local waited=0
    while [ "$waited" -lt 100 ]; do
        workload_alive || break
        sleep 0.1
        waited=$((waited + 1))
    done
    if workload_alive; then
        stop_child
    fi
    unmount_fs "$mnt"

    mount_fs "$loop" "$fs" "$mnt" || { report "$name" 1; return; }
    local ok=0
    verify_session "$work" || ok=1
    unmount_fs "$mnt"
    report "$name" "$ok"
}

scenario_enospc() { # fs
    local fs="$1" name="enospc/$fs"
    local dir="$SCENARIO_DIR"
    mkdir -p "$dir"
    local img="$dir/data.img"
    local size_mib
    # XFS refuses tiny filesystems; ext4 happily fills a small one.
    if [ "$fs" = xfs ]; then size_mib=320; else size_mib=48; fi
    new_image "$img" "$size_mib"
    local loop
    loop_up "$img" || { skip "$name" "losetup failed"; return; }
    loop="$LOOP_DEVICE"

    if [ "$fs" = xfs ]; then
        mkfs.xfs -f -q -d agcount=1 "$loop" || { skip "$name" "mkfs.xfs failed"; return; }
    else
        mkfs.ext4 -q -F "$loop" || { skip "$name" "mkfs failed"; return; }
    fi
    local mnt="$dir/mnt"
    mount_fs "$loop" "$fs" "$mnt" || { skip "$name" "mount failed"; return; }

    local work="$mnt/work"
    # No sleep between appends: fill the filesystem as fast as the loop can take.
    start_workload "$work" "$SEED" 0
    local waited=0
    while [ "$waited" -lt $((SECONDS_PER_RUN * 30)) ]; do
        workload_alive || break
        sleep 0.1
        waited=$((waited + 1))
    done
    if workload_alive; then
        note "    warning: workload did not hit ENOSPC within $((SECONDS_PER_RUN * 3))s"
        stop_child
    fi
    if ! grep -qi "space" "$work/child.err" 2>/dev/null; then
        note "    note: child stderr does not mention ENOSPC"
    fi
    sync || true
    unmount_fs "$mnt"

    mount_fs "$loop" "$fs" "$mnt" || { report "$name" 1; return; }
    local ok=0
    verify_session "$work" || ok=1
    unmount_fs "$mnt"
    report "$name" "$ok"
}

run_scenario() { # scenario fs
    local scenario="$1" fs="$2"
    SCENARIO_DIR="$WORK/$scenario-$fs"
    rm -rf "$SCENARIO_DIR"
    mkdir -p "$SCENARIO_DIR"
    note "=== $scenario on $fs ($SCENARIO_DIR) ==="
    local failed_before=$FAILED
    case "$scenario" in
        log-writes)        scenario_log_writes "$fs" ;;
        flakey)            scenario_flakey "$fs" ;;
        dust)              scenario_dust "$fs" ;;
        null-blk)          scenario_null_blk "$fs" ;;
        fail-make-request) scenario_fail_make_request "$fs" ;;
        remount-ro)        scenario_remount_ro "$fs" ;;
        enospc)            scenario_enospc "$fs" ;;
        *) die "unknown scenario $scenario" ;;
    esac
    cleanup_resources
    # Keep the directory on failure for post-mortem, or when --keep was given.
    if [ "$KEEP" -eq 0 ] && [ "$FAILED" -eq "$failed_before" ]; then
        rm -rf "$SCENARIO_DIR" 2>/dev/null || true
    fi
}

# ===== argument parsing =====

while [ $# -gt 0 ]; do
    case "$1" in
        --fs) FILESYSTEMS=("$2"); shift 2 ;;
        --seconds) SECONDS_PER_RUN="$2"; shift 2 ;;
        --work) WORK="$2"; shift 2 ;;
        --keep) KEEP=1; shift ;;
        --validator) VALIDATOR="$2"; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        -*) die "unknown option $1" ;;
        *)
            found=0
            for known in "${ALL_SCENARIOS[@]}"; do
                [ "$1" = "$known" ] && found=1
            done
            [ "$found" -eq 1 ] || die "unknown scenario $1 (known: ${ALL_SCENARIOS[*]})"
            SCENARIOS+=("$1")
            shift
            ;;
    esac
done

[ "${#SCENARIOS[@]}" -eq 0 ] && SCENARIOS=("${ALL_SCENARIOS[@]}")
case "${FILESYSTEMS[0]}" in
    both) FILESYSTEMS=(xfs ext4) ;;
    xfs|ext4) ;;
    *) die "--fs must be xfs, ext4, or both" ;;
esac

require_root
[ -d /sys/kernel/config ] || modprobe configfs >/dev/null 2>&1 || true
command -v modprobe >/dev/null 2>&1 && {
    modprobe loop >/dev/null 2>&1 || true
    modprobe dm_mod >/dev/null 2>&1 || true
}
validator_bin

note "validator binary: $VALIDATOR"
note "work root: $WORK"
note "scenarios: ${SCENARIOS[*]} on ${FILESYSTEMS[*]}"

for scenario in "${SCENARIOS[@]}"; do
    for fs in "${FILESYSTEMS[@]}"; do
        run_scenario "$scenario" "$fs"
    done
done

note ""
note "kernel-faults: $PASSED passed, $FAILED failed, $SKIPPED skipped"
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
if [ "$PASSED" -eq 0 ]; then
    exit 77
fi
exit 0
