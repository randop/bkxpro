# bkxpro

bkxpro - BookKeeper process, a data plane microservice.

## Crates

- [`codec/`](codec/README.md): fixed-format segment codec, RAN-126.
- [`segment-writer/`](segment-writer/README.md): aligned segment writer and group-commit state machine, RAN-127.
- [`linux-io/`](linux-io/README.md): Linux-only io_uring adapter, registered resource management, and host preflight, RAN-134.

## Linux development and host preflight

The complete storage workspace is Linux-only. Non-Linux builds fail at compile time.

Run the standalone host preflight on the actual XFS/ext4 staging volume before starting a service:

```sh
cargo run -p bkxpro-linux-io --bin bkxpro-preflight -- \
  --data-dir /var/lib/bkxpro \
  --memory-budget-mib 2048 \
  --registered-buffer-mib 256 \
  --commit-mode drain-fdatasync
```

The preflight requires Linux 6.1+, permitted io_uring, a finite cgroup v2 `memory.max`, enough `RLIMIT_MEMLOCK`, working O_DIRECT, `STATX_DIOALIGN` <= 4 KiB, and XFS or ext4. It leaves `.bkxpro.lock` in the data directory and holds an OFD lock only while the command is running. The service must call `run_host_preflight` at startup and retain the returned `PreflightResult` (especially `directory_lock`) for its entire lifetime.

See [the RAN-134 runtime decision](docs/rfc/0001-linux-io-runtime.md) for the backend decision, failure policy, and fio baseline procedure.

The segment-writer crate is structured around the `SegmentIo` boundary required from RAN-134. It does not substitute a portable or buffered I/O backend when that adapter is unavailable.
