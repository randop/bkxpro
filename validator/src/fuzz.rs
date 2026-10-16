//! Seeded structural fuzzing of the segment parsers.
//!
//! This is the in-tree fuzzer that runs on every PR: it builds valid segment
//! images (sealed and open variants) from the seed, applies deterministic
//! structural corruption, and checks what the parsers do with the result:
//!
//! * `SegmentView::open_sealed` never accepts an image that differs from the
//!   source (every byte of a sealed image is CRC-covered).
//! * `SegmentView::recover_open` either rejects the image (invalid header) or
//!   yields records that are an exact prefix of the attempted records, payload
//!   for payload; it never surfaces a hole, a torn record, or a record whose
//!   bytes were substituted.
//! * For pure *tail* faults (truncation, zeroing the tail) recovery is exact:
//!   the recovered count equals the number of records fully contained in the
//!   surviving blocks, not one fewer.
//! * The recovery point's `complete_records` agrees with what iterating the
//!   view yields, and `valid_len` never exceeds the image.
//!
//! What is deliberately out of scope: relocating CRC-valid blocks from another
//! segment (a bit-level adversary). Blocks carry no segment identity, so such an
//! image is "a different valid segment prefix", not corruption - no storage
//! fault model produces it. The `dm-dust`/`dm-flakey` scripts and the simulator
//! cover the fault models that do.
//!
//! [`write_corpus`] materializes a fixed corpus for the cargo-fuzz project in
//! `codec/fuzz` (`cargo +nightly fuzz run recovery_parser`); the checked-in
//! corpus lives in `codec/fuzz/corpus/recovery_parser/`.

use std::{collections::BTreeMap, fs, io, path::Path};

use bkxpro_codec::{
    parse_segment_header, AlignedSegmentEncoder, SegmentHeader, SegmentView, TrailerWire,
    BLOCK_DATA_SIZE, BLOCK_SIZE, HEADER_SIZE, RECORD_HEADER_SIZE, TRAILER_SIZE,
};
use bkxpro_segmenter::AlignedBuffer;
use bytes::Bytes;
use serde::Serialize;

use crate::{lifecycle::Session, rng::DetRng};

/// Fuzzer knobs. A run is a pure function of `(seed, iterations, entries,
/// payload_bytes)`, so a violating seed replays exactly.
#[derive(Debug, Clone)]
pub struct FuzzConfig {
    pub seed: u64,
    pub iterations: u64,
    /// Records in the source segment.
    pub entries: u64,
    /// Payload bytes per record; large values exercise multi-block records.
    pub payload_bytes: usize,
    /// Maximum corruption layers applied per iteration.
    pub max_mutations: usize,
}

impl Default for FuzzConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            iterations: 2_000,
            entries: 16,
            payload_bytes: 96,
            max_mutations: 3,
        }
    }
}

/// What one fuzz run saw and found.
#[derive(Debug, Default, Clone, Serialize)]
pub struct FuzzReport {
    pub seed: u64,
    pub iterations: u64,
    /// Mutated images `open_sealed` accepted (must match the source exactly).
    pub sealed_parses: u64,
    /// Mutated images recovery produced a view for.
    pub recovered_views: u64,
    /// Mutated images with an unreadable header.
    pub invalid_headers: u64,
    pub recovered_records: u64,
    /// Histogram of `RecoveryStop` reasons.
    pub stops: BTreeMap<String, u64>,
    pub violations: Vec<String>,
}

impl FuzzReport {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// One record of the source segment, with its exact byte range.
struct SourceRecord {
    ledger: u64,
    entry: u64,
    len: usize,
    payload: Bytes,
    /// Offset one past the record's payload in the source image.
    end: usize,
}

struct Source {
    sealed: Vec<u8>,
    open: Vec<u8>,
    records: Vec<SourceRecord>,
}

fn build_source(seed: u64, entries: u64, payload_bytes: usize) -> Source {
    let blocks =
        entries as usize * (1 + (RECORD_HEADER_SIZE + payload_bytes) / BLOCK_DATA_SIZE) + 2;
    let mut buffer = AlignedBuffer::new_zeroed(HEADER_SIZE + blocks * BLOCK_SIZE).unwrap();
    let header = SegmentHeader {
        segment_id: 1,
        writer_epoch: 1,
        target_size: (HEADER_SIZE + blocks * BLOCK_SIZE) as u64,
        created_at_ns: 0,
    };
    let mut encoder = AlignedSegmentEncoder::new(header, &mut buffer).unwrap();
    let mut records = Vec::new();
    for entry in 1..=entries {
        let ledger = 1;
        let payload = Session::entry_payload(seed, ledger, entry, payload_bytes);
        encoder.push_record(ledger, entry, 0, &payload).unwrap();
        records.push(SourceRecord {
            ledger,
            entry,
            len: payload_bytes,
            payload,
            end: 0,
        });
    }
    encoder.flush_pending_data_block().unwrap();
    let open_len = encoder.committed_prefix_len();
    let (buffer, sealed_len) = encoder.seal_and_return_buffer().unwrap();
    let open = buffer[..open_len].to_vec();
    let sealed = buffer[..sealed_len].to_vec();

    // Fill in the physical extents from a trusted parse of the open image.
    let view = SegmentView::recover_open(Bytes::copy_from_slice(&open))
        .view
        .expect("source open image must recover");
    for (record, parsed) in records.iter_mut().zip(view.records()) {
        let parsed = parsed.expect("source records must parse");
        assert_eq!(
            (parsed.ledger, parsed.entry, parsed.len as usize),
            (record.ledger, record.entry, record.len)
        );
        record.end = parsed.offset as usize + RECORD_HEADER_SIZE + parsed.len as usize;
    }
    assert_eq!(records.len(), entries as usize);
    Source {
        sealed,
        open,
        records,
    }
}

/// A pure tail fault: everything at and after `at` is gone or zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TailFault {
    Truncate { at: usize },
    Zero { at: usize },
}

/// The exact recovered-record count a tail fault can leave behind.
fn tail_expected(source: &[u8], records: &[SourceRecord], fault: TailFault) -> usize {
    let surviving = match fault {
        TailFault::Truncate { at } => (at.min(source.len()) / BLOCK_SIZE) * BLOCK_SIZE,
        TailFault::Zero { at } => {
            // Zeroing is a no-op inside already-zero padding; the first block
            // whose bytes actually change is the first block recovery drops.
            let mut block_start = (at / BLOCK_SIZE) * BLOCK_SIZE;
            loop {
                if block_start >= source.len() {
                    break source.len();
                }
                let from = at.max(block_start);
                if source[from..block_start + BLOCK_SIZE]
                    .iter()
                    .any(|byte| *byte != 0)
                {
                    break block_start;
                }
                block_start += BLOCK_SIZE;
            }
        }
    };
    records
        .iter()
        .filter(|record| record.end <= surviving)
        .count()
}

/// Apply up to `layers` non-tail corruptions.
fn mutate(rng: &mut DetRng, base: &[u8], layers: usize) -> Vec<u8> {
    let mut bytes = base.to_vec();
    for _ in 0..layers {
        match rng.below(6) {
            0 => {
                if !bytes.is_empty() {
                    let offset = rng.below(bytes.len() as u64) as usize;
                    bytes[offset] ^= 1u8 << rng.below(8);
                }
            }
            1 => {
                if !bytes.is_empty() {
                    let start = rng.below(bytes.len() as u64) as usize;
                    let len = rng.range_inclusive(1, (bytes.len() - start) as u64) as usize;
                    bytes[start..start + len].fill(0);
                }
            }
            2 => {
                if !bytes.is_empty() {
                    let start = rng.below(bytes.len() as u64) as usize;
                    let len = rng.range_inclusive(1, (bytes.len() - start) as u64) as usize;
                    bytes[start..start + len].fill(0xFF);
                }
            }
            3 => {
                // Extend with garbage: a torn/extra tail shape.
                let extra = (rng.range_inclusive(1, 3) as usize) * BLOCK_SIZE
                    + rng.below(BLOCK_SIZE as u64 + 1) as usize;
                bytes.extend_from_slice(&rng.bytes(extra));
            }
            4 => {
                // Overwrite a structural region (header start/end, footer,
                // trailer) with random bytes.
                if bytes.len() >= HEADER_SIZE + TRAILER_SIZE {
                    let regions = [
                        0,
                        HEADER_SIZE.saturating_sub(64),
                        HEADER_SIZE,
                        bytes.len() - TRAILER_SIZE - 16,
                        bytes.len() - TRAILER_SIZE,
                    ];
                    let start = regions[rng.below(regions.len() as u64) as usize];
                    let len = 16.min(bytes.len() - start);
                    bytes[start..start + len].copy_from_slice(&rng.bytes(len));
                }
            }
            _ => {
                // Overwrite an arbitrary aligned block header with garbage.
                let blocks = bytes.len() / BLOCK_SIZE;
                if blocks > 0 {
                    let block = rng.below(blocks as u64) as usize;
                    let offset = block * BLOCK_SIZE + rng.below(16) as usize;
                    bytes[offset] ^= 0x80;
                }
            }
        }
    }
    bytes
}

/// One deterministic iteration: either a pure tail fault (with its exact
/// expectation) or mixed corruption (prefix property only). The bool reports
/// whether the sealed image was the base, so the caller can compute the exact
/// tail expectation against the right source bytes.
fn mutate_iteration(
    seed: u64,
    iteration: u64,
    source: &Source,
) -> (Vec<u8>, Option<TailFault>, bool) {
    let mut rng = DetRng::new(seed).derive_from(&[b"fuzz-iteration", &iteration.to_le_bytes()]);
    let sealed_base = rng.chance(50);
    let base = if sealed_base {
        &source.sealed
    } else {
        &source.open
    };
    if rng.chance(40) {
        // Pure tail fault: exactly one fault, checked against the exact count.
        let at = rng.below(base.len() as u64 + 1) as usize;
        let fault = if rng.chance(50) {
            TailFault::Truncate { at }
        } else {
            TailFault::Zero { at }
        };
        let mut bytes = base.clone();
        apply_tail(&mut bytes, fault);
        return (bytes, Some(fault), sealed_base);
    }
    let layers = rng.range_inclusive(1, 3) as usize;
    (mutate(&mut rng, base, layers), None, sealed_base)
}

fn apply_tail(bytes: &mut Vec<u8>, fault: TailFault) {
    match fault {
        TailFault::Truncate { at } => bytes.truncate(at),
        TailFault::Zero { at } => bytes[at..].fill(0),
    }
}

fn check(iteration: u64, source: &Source, image: &[u8], report: &mut FuzzReport) {
    let bytes = Bytes::copy_from_slice(image);
    if let Ok(view) = SegmentView::open_sealed(bytes.clone()) {
        report.sealed_parses += 1;
        let mut count = 0usize;
        for record in view.records() {
            match record {
                Ok(record) => {
                    match source.records.get(count) {
                        Some(expected) => {
                            let mut payload = Vec::new();
                            let copied = record.copy_payload_to(&mut payload).is_ok();
                            if !copied
                                || (record.ledger, record.entry, record.len as usize)
                                    != (expected.ledger, expected.entry, expected.len)
                                || payload.as_slice() != expected.payload.as_ref()
                            {
                                report.violations.push(format!(
                                    "iteration {iteration}: open_sealed accepted a mutated image (record {count} differs)"
                                ));
                            }
                        }
                        None => report.violations.push(format!(
                            "iteration {iteration}: open_sealed accepted a mutated image with extra records"
                        )),
                    }
                    count += 1;
                }
                Err(error) => {
                    report.violations.push(format!(
                        "iteration {iteration}: sealed record iteration failed: {error}"
                    ));
                    break;
                }
            }
        }
        if count != source.records.len() {
            report.violations.push(format!(
                "iteration {iteration}: open_sealed accepted an image with {count} records instead of {}",
                source.records.len()
            ));
        }
    }

    let result = SegmentView::recover_open(bytes.clone());
    *report
        .stops
        .entry(format!("{:?}", result.point.stop))
        .or_default() += 1;
    match result.view {
        Some(view) => {
            report.recovered_views += 1;
            let mut count = 0usize;
            let mut consistent = true;
            for record in view.records() {
                match record {
                    Ok(record) => {
                        let expected = match source.records.get(count) {
                            Some(expected) => expected,
                            None => {
                                report.violations.push(format!(
                                    "iteration {iteration}: recovery surfaced record {count} beyond the source prefix"
                                ));
                                consistent = false;
                                break;
                            }
                        };
                        let mut payload = Vec::new();
                        if let Err(error) = record.copy_payload_to(&mut payload) {
                            report.violations.push(format!(
                                "iteration {iteration}: recovered record {count} payload unreadable: {error}"
                            ));
                            consistent = false;
                            break;
                        }
                        if (record.ledger, record.entry, record.len as usize)
                            != (expected.ledger, expected.entry, expected.len)
                            || payload.as_slice() != expected.payload.as_ref()
                        {
                            report.violations.push(format!(
                                "iteration {iteration}: recovered record {count} is not the source record (torn or substituted bytes)"
                            ));
                            consistent = false;
                            break;
                        }
                        count += 1;
                    }
                    Err(error) => {
                        report.violations.push(format!(
                            "iteration {iteration}: recovered record {count} failed to parse: {error}"
                        ));
                        consistent = false;
                        break;
                    }
                }
            }
            report.recovered_records += count as u64;
            if consistent {
                if result.point.complete_records != count {
                    report.violations.push(format!(
                        "iteration {iteration}: recovery point says {} records but the view yields {count}",
                        result.point.complete_records
                    ));
                }
                if result.point.valid_len != view.data_end() {
                    report.violations.push(format!(
                        "iteration {iteration}: recovery point valid_len {} disagrees with data_end {}",
                        result.point.valid_len,
                        view.data_end()
                    ));
                }
                if result.point.valid_len > image.len() {
                    report.violations.push(format!(
                        "iteration {iteration}: recovery valid_len {} exceeds the {}-byte image",
                        result.point.valid_len,
                        image.len()
                    ));
                }
            }
        }
        None => {
            report.invalid_headers += 1;
            if parse_segment_header(&bytes).is_ok() {
                report.violations.push(format!(
                    "iteration {iteration}: recovery rejected an image whose header parses"
                ));
            }
        }
    }
}

// The tail check needs the exact expectation against the *source* image, so it
// lives in the callers that still hold the base image.
fn check_tail_expectation(
    iteration: u64,
    source: &Source,
    source_image: &[u8],
    fault: TailFault,
    recovered: u64,
    report: &mut FuzzReport,
) {
    let expected = tail_expected(source_image, &source.records, fault) as u64;
    if recovered != expected {
        report.violations.push(format!(
            "iteration {iteration}: tail fault {fault:?} recovered {recovered} records, expected exactly {expected}"
        ));
    }
}

/// Run the structural fuzzer: corruption anywhere in the image, strong prefix
/// property for recovery, exact equality for sealed parses, and exact counts
/// for the pure-tail subset.
pub fn run_fuzz(config: &FuzzConfig) -> FuzzReport {
    let source = build_source(config.seed, config.entries, config.payload_bytes);
    let mut report = FuzzReport {
        seed: config.seed,
        ..FuzzReport::default()
    };
    for iteration in 0..config.iterations {
        let before = report.recovered_records;
        let (image, tail, sealed_base) = mutate_iteration(config.seed, iteration, &source);
        check(iteration, &source, &image, &mut report);
        if let Some(fault) = tail {
            let base = if sealed_base {
                &source.sealed
            } else {
                &source.open
            };
            check_tail_expectation(
                iteration,
                &source,
                base,
                fault,
                report.recovered_records - before,
                &mut report,
            );
        }
        report.iterations += 1;
    }
    report
}

/// Run a tail-fault fuzzer: only truncation and zero-fill of the tail, checked
/// for the exact recovered count (not just a prefix).
pub fn run_tail_fuzz(config: &FuzzConfig) -> FuzzReport {
    let source = build_source(config.seed, config.entries, config.payload_bytes);
    let mut report = FuzzReport {
        seed: config.seed,
        ..FuzzReport::default()
    };
    for iteration in 0..config.iterations {
        let before = report.recovered_records;
        let mut rng =
            DetRng::new(config.seed).derive_from(&[b"fuzz-tail", &iteration.to_le_bytes()]);
        let base = if rng.chance(50) {
            &source.sealed
        } else {
            &source.open
        };
        let at = rng.below(base.len() as u64 + 1) as usize;
        let fault = if rng.chance(50) {
            TailFault::Truncate { at }
        } else {
            TailFault::Zero { at }
        };
        let mut image = base.clone();
        apply_tail(&mut image, fault);
        check(iteration, &source, &image, &mut report);
        check_tail_expectation(
            iteration,
            &source,
            base,
            fault,
            report.recovered_records - before,
            &mut report,
        );
        report.iterations += 1;
    }
    report
}

/// Audit one image against the fuzzer's default source segment: the same checks
/// the fuzzer applies, as a pure function of the image. Used by tests to keep
/// the checked-in cargo-fuzz corpus honest.
pub fn audit_image(seed: u64, image: &[u8]) -> Vec<String> {
    let defaults = FuzzConfig::default();
    let source = build_source(seed, defaults.entries, defaults.payload_bytes);
    let mut report = FuzzReport::default();
    check(0, &source, image, &mut report);
    report.violations
}

/// Write the cargo-fuzz corpus for the codec targets. Deterministic for a given
/// seed; the checked-in copy was generated with seed 9. Returns the file count.
pub fn write_corpus(root: &Path, seed: u64) -> io::Result<usize> {
    let defaults = FuzzConfig::default();
    let source = build_source(seed, defaults.entries, defaults.payload_bytes);
    let mut written = 0usize;

    let mut recovery: Vec<(String, Vec<u8>)> = Vec::new();
    let mut add = |name: &str, bytes: Vec<u8>| {
        recovery.push((name.to_string(), bytes));
    };
    add("valid-sealed.seg", source.sealed.clone());
    add("valid-open.seg", source.open.clone());
    add("open-header-only.seg", source.open[..HEADER_SIZE].to_vec());
    add(
        "open-truncated-mid-block.seg",
        source.open[..HEADER_SIZE + BLOCK_SIZE / 2].to_vec(),
    );
    let mut zero_tail = source.open.clone();
    zero_tail[HEADER_SIZE + 128..].fill(0);
    add("open-zero-tail.seg", zero_tail);
    let mut garbage_tail = source.open.clone();
    garbage_tail.extend_from_slice(&[0xA5u8; BLOCK_SIZE]);
    add("open-garbage-tail.seg", garbage_tail);
    let mut flipped_data = source.open.clone();
    flipped_data[HEADER_SIZE + 128] ^= 0x10;
    add("flip-in-data-block.seg", flipped_data);
    let mut flipped_header = source.sealed.clone();
    flipped_header[8] ^= 0x01;
    add("flip-in-header.seg", flipped_header);
    let mut flipped_trailer = source.sealed.clone();
    let last = flipped_trailer.len() - 5;
    flipped_trailer[last] ^= 0x80;
    add("flip-in-trailer.seg", flipped_trailer);
    add(
        "garbage-header.seg",
        DetRng::new(seed).bytes(HEADER_SIZE + BLOCK_SIZE),
    );
    add("garbage-small.seg", DetRng::new(seed ^ 1).bytes(600));
    add("empty.seg", Vec::new());
    for iteration in 0..12u64 {
        let (image, _, _) = mutate_iteration(seed, iteration, &source);
        add(&format!("mutated-{iteration:02}.seg"), image);
    }

    for (name, bytes) in &recovery {
        let path = root.join("recovery_parser").join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, bytes)?;
        written += 1;
    }

    // Small seeds for the other tests in the same fuzz project.
    let record_offset = {
        let view = SegmentView::recover_open(Bytes::copy_from_slice(&source.open))
            .view
            .expect("source recovers");
        let first = view
            .records()
            .next()
            .expect("records")
            .expect("first record");
        first.offset as usize
    };
    let record_end = record_offset + RECORD_HEADER_SIZE + source.records[0].len;
    let small: [(&str, Vec<u8>); 6] = [
        (
            "record_parser/valid-record.seg",
            source.open[record_offset..record_end].to_vec(),
        ),
        ("record_parser/garbage.bin", DetRng::new(seed ^ 2).bytes(64)),
        (
            "block_parser/valid-block.seg",
            source.open[HEADER_SIZE..HEADER_SIZE + BLOCK_SIZE].to_vec(),
        ),
        (
            "block_parser/garbage.bin",
            DetRng::new(seed ^ 3).bytes(BLOCK_SIZE),
        ),
        (
            "footer_parser/garbage.bin",
            DetRng::new(seed ^ 4).bytes(256),
        ),
        (
            "footer_parser/valid-footer.seg",
            footer_bytes(&source.sealed),
        ),
    ];
    for (name, bytes) in &small {
        let path = root.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, bytes)?;
        written += 1;
    }
    Ok(written)
}

fn footer_bytes(sealed: &[u8]) -> Vec<u8> {
    let trailer = TrailerWire::parse(&sealed[sealed.len() - TRAILER_SIZE..])
        .expect("sealed image has a valid trailer");
    let start = usize::try_from(u64::from_le(trailer.footer_start)).expect("footer start fits");
    let len = trailer.footer_len as usize;
    sealed[start..start + len].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_source_segment_is_valid() {
        let source = build_source(3, 8, 128);
        let sealed = SegmentView::open_sealed(Bytes::copy_from_slice(&source.sealed)).unwrap();
        assert_eq!(sealed.records().count(), 8);
        let recovered = SegmentView::recover_open(Bytes::copy_from_slice(&source.open))
            .view
            .unwrap();
        assert_eq!(
            recovered.records().count(),
            8,
            "open image must recover all records"
        );
        assert!(source.records.iter().all(|record| record.end > HEADER_SIZE));
    }

    #[test]
    fn corpus_writer_round_trips_footers() {
        let dir = tempfile::TempDir::new().unwrap();
        let count = write_corpus(dir.path(), 9).unwrap();
        assert_eq!(count, 12 + 12 + 6);
        let footer = fs::read(dir.path().join("footer_parser/valid-footer.seg")).unwrap();
        assert!(bkxpro_codec::FooterView::parse(Bytes::from(footer)).is_ok());
    }

    #[test]
    fn audits_catch_a_swapped_record_block() {
        // Relocating a CRC-valid block from another segment is the documented
        // out-of-scope adversary; the audit must flag it rather than bless it.
        let source = build_source(9, 16, 96);
        let other = build_source(10, 16, 96);
        let mut image = source.open.clone();
        image[HEADER_SIZE..HEADER_SIZE + BLOCK_SIZE]
            .copy_from_slice(&other.open[HEADER_SIZE..HEADER_SIZE + BLOCK_SIZE]);
        let violations = audit_image(9, &image);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("not the source record")),
            "{violations:?}"
        );
    }

    #[test]
    fn tail_expectations_are_exact() {
        let source = build_source(5, 16, 96);
        assert_eq!(
            tail_expected(
                &source.open,
                &source.records,
                TailFault::Truncate {
                    at: source.open.len()
                }
            ),
            16
        );
        assert_eq!(
            tail_expected(&source.open, &source.records, TailFault::Zero { at: 0 }),
            0
        );
    }
}
