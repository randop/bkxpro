# bkxpro codec — RAN-126

A standalone codec crate for the on-disk segment and S3 batch-object format. It deliberately stops at serialization, parsing, sealed-footer validation, and open-segment recovery; aligned file I/O, group commit, fjall, S3 upload, and cache integration belong to downstream tasks. The format remains portable for offline inspection and fuzzing; Linux host preflight, device-specific DIO alignment, buffer registration, and io_uring I/O belong to RAN-134/RAN-127.

## Wire-format specification (version 1)

All integer fields are little-endian. CRCs are CRC32C/Castagnoli. Unknown versions, non-zero reserved fields, malformed bounds, checksum failures, and non-zero block padding are rejected. The footer index is sorted lexicographically by `(ledger, entry)` and duplicate keys are rejected at seal time.

### Segment header: 4,096 bytes

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | `BKXPROSG` magic |
| 8 | 2 | format version (`1`) |
| 10 | 2 | header length (`4096`) |
| 12 | 4 | flags (must be zero) |
| 16 | 8 | segment ID |
| 24 | 8 | writer epoch |
| 32 | 8 | target size |
| 40 | 8 | creation time in Unix nanoseconds |
| 48 | 4 | CRC32C of all 4,096 header bytes with this field zeroed |
| 52 | 4 | reserved (zero) |
| 56 | 4,040 | reserved zero padding |

### Data block: 4,096 bytes, aligned

The 16-byte header is followed by at most 4,080 bytes of packed record-stream data; the remaining bytes are zero padded. Block-header layout:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | `BLK1` magic |
| 4 | 2 | count of record headers that start in this block |
| 6 | 2 | flags (zero) |
| 8 | 4 | number of data bytes used in this block |
| 12 | 4 | CRC32C of all 4,096 block bytes with this field zeroed |

`used_len` excludes zero padding. Concatenating only the used data regions in block order yields the logical packed record stream. The record header is never split across blocks. A record whose `21 + payload_len` exceeds a block's 4,080-byte data capacity starts in a fresh block and its payload continues in subsequent block data regions until complete. That reservation makes an incomplete large record recoverable by rolling back to its first block without discarding earlier records. Continuation blocks count zero new record headers unless a later record starts in a block that contains the tail of a previous record.

### Record: 21-byte header plus payload

| Header offset | Size | Field |
|---:|---:|---|
| 0 | 4 | payload length (`u32`) |
| 4 | 8 | ledger ID |
| 12 | 8 | entry ID |
| 20 | 1 | flags |
| 21 | `len` | payload bytes |

The wire header uses `#[repr(C)]` with byte-array integer fields (`[u8; 4]`, `[u8; 8]`, `[u8; 8]`) so its layout is exactly 21 bytes without ABI padding. Integer fields are converted explicitly as little-endian. The header derives zerocopy traits and is parsed by `zerocopy`.

Decoded payloads are exposed as `Bytes::slice` chunks. A payload fully within one block yields one slice; a multi-block payload yields one slice per physical block data span. The codec does not allocate or coalesce an entry-sized buffer during parsing.

### Footer

The footer begins at the first byte following the last data block, so its start is 4 KiB aligned. Its encoded length ends at the footer CRC; any required zero padding is between that footer and the 32-byte trailer.

Footer header (`28` bytes): `magic[8] = BKXPROFT`, version `u16`, header length `u16 = 28`, index count `u32`, ledger-summary count `u32`, data-block count `u32`, flags `u32 = 0`.

This is followed by `index_count` 32-byte entries: `ledger u64`, `entry u64`, absolute record-header byte offset `u64`, payload length `u32`, reserved `u32 = 0`. Entries are sorted by `(ledger, entry)` and keys are unique.

Next are `ledger_count` 24-byte summaries sorted by ledger: `ledger u64`, minimum entry ID `u64`, maximum entry ID `u64`. A 4-byte CRC32C over the footer header and both tables (excluding the CRC field) terminates the footer. Summary ranges must exactly match the sorted index.

The 32-byte trailer layout is `magic[8] = BKXPROTR`, footer start `u64`, footer length including footer CRC `u32`, footer CRC32C `u32`, version `u16`, trailer length `u16 = 32`, trailer CRC32C `u32`. The trailer CRC covers all 32 trailer bytes with its own field zeroed. The file ends immediately after the trailer, and the total file length is a multiple of 4 KiB. The footer CRC is repeated in the trailer so a reader can validate the pointed-to footer before exposing the sealed segment.

## Aligned-buffer encoding and CRC32C

`AlignedSegmentEncoder::new(header, output)` writes into the caller's storage. The slice base address and length must both be 4 KiB-aligned, and its capacity must cover the full sealed segment. The output storage is the encoder's working area: block headers, records, padding, footer, and trailer are written in place. There is no intermediate encoded-segment `Vec` and no copy of a completed block into the output; source payload bytes are copied once into their final block. The returned length from `seal()` is 4 KiB-aligned. The caller owns storage lifetime and io_uring registered-buffer lifecycle. `SegmentEncoder` remains a convenient growable-memory/reference encoder for tests and tooling.

CRC32C calls use the `crc32c` crate, which automatically selects CPU acceleration when available. CRC validation zeros the checksum field logically using incremental CRC calls, avoiding a 4 KiB scratch copy for each block. Benchmark the selected runtime path on the target machine with `cargo bench --bench crc32c`; the benchmark reports throughput for 4 KiB, 64 KiB, and 1 MiB buffers. Compare the in-place aligned-output path to the growable reference encoder with `cargo bench --bench encoder`.

## Recovery behavior

`SegmentView::recover_open` / `scan_recovery` validates the segment header and scans full blocks until EOF, a partial block, a zero block, or the first invalid block. It then verifies logical record framing across the valid data spans. If a multi-block record is incomplete, recovery rolls back to the block where that record began. A caller can truncate to `RecoveryPoint.valid_len`; bytes after that point are never exposed as records. A missing/invalid header yields a safe prefix of length zero.

`SegmentView::open_sealed` is stricter: the trailer must point to a footer whose end and canonical zero-padding region lead to the final trailer, every data block must validate, footer tables and CRC must validate, every record must be complete, and every footer index row must match a decoded record.

## Tests and fuzzing

Unit tests cover ordinary and multi-block round trips, sorted index / per-ledger summaries, duplicate-key rejection, CRC corruption, and truncation at every byte offset in a sample sealed segment. Property tests exercise block and record-header parsers on arbitrary input. `fuzz/` provides cargo-fuzz targets for the record, block, and footer parsers.

Run tests with `cargo test`. Run the CRC32C throughput benchmark on the target host with `cargo bench --bench crc32c`, and compare encoder output paths with `cargo bench --bench encoder`. Run fuzzers from this directory with `cargo fuzz run record_parser`, `cargo fuzz run block_parser`, and `cargo fuzz run footer_parser` (install `cargo-fuzz` first). The tests, benchmark, and fuzz targets are supplied but were not run in this environment because the Rust toolchain is unavailable.
