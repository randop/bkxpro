//! Property-based reference model for the ledger semantics implemented by the
//! segment codec (RAN-131): append, read, LAC, and the recovery prefix guarantee.
//!
//! The model is deliberately trivial: a ledger is an ordered map of
//! `(ledger, entry) -> payload`, its LAC is the highest entry recovered for a
//! ledger, and "durable" means "present in the recovered byte image". The system
//! under test is the real codec: `SegmentEncoder` for appends, `SegmentView`
//! for reads and for both recovery modes (`open_sealed` and `recover_open`).
//!
//! Properties:
//!
//! 1. A sealed segment round-trips exactly: every attempted record reads back
//!    with its flags and payload, footer rows point at the right bytes, and the
//!    per-ledger LAC equals the highest attempted entry.
//! 2. Cutting a sealed segment at *any* byte offset can only ever expose a
//!    prefix of the attempted records: no torn record, no reordering, no
//!    invented entries. This is the durable-prefix invariant that fences all
//!    crash points the harness simulates.
//! 3. A writer that loses the epoch race (fence) cannot surface records from its
//!    superseded attempt: recovery only ever reads the winning image, the epoch
//!    is visible in the header, and the merged LAC never decreases.

use std::collections::BTreeMap;

use bkxpro_validator::DetRng;
use bkxpro_codec::{
    SegmentEncoder, SegmentHeader, SegmentView, BLOCK_DATA_SIZE, HEADER_SIZE, RECORD_HEADER_SIZE,
};
use bytes::Bytes;
use proptest::prelude::*;

/// Payload oracle: a deterministic function of the entry identity, so
/// "read(ledger, entry)" can be checked without carrying payloads around.
fn reference_payload(ledger: u64, entry: u64, len: usize) -> Vec<u8> {
    let mut rng = DetRng::new(0).derive_from(&[
        b"model-payload",
        &ledger.to_le_bytes(),
        &entry.to_le_bytes(),
    ]);
    rng.bytes(len)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordSpec {
    ledger: u64,
    entry: u64,
    flags: u8,
    len: usize,
}

impl RecordSpec {
    fn payload(&self) -> Vec<u8> {
        reference_payload(self.ledger, self.entry, self.len)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct RecoveredRecord {
    ledger: u64,
    entry: u64,
    flags: u8,
    offset: u64,
    payload: Vec<u8>,
}

#[derive(Debug, Default)]
struct LedgerModel {
    records: Vec<RecoveredRecord>,
    /// Highest recovered entry per ledger (the LAC the index would derive).
    lac: BTreeMap<u64, u64>,
}

impl LedgerModel {
    fn ingest(&mut self, view: &SegmentView) {
        for record in view.records() {
            let record = record.expect("recovered records must parse");
            let mut payload = Vec::with_capacity(record.len as usize);
            record
                .copy_payload_to(&mut payload)
                .expect("in-memory payload write cannot fail");
            let chunk_total: usize = record.payload_chunks().map(|chunk| chunk.len()).sum();
            assert_eq!(chunk_total, record.len as usize, "chunk total equals len");
            self.lac
                .entry(record.ledger)
                .and_modify(|lac| *lac = (*lac).max(record.entry))
                .or_insert(record.entry);
            self.records.push(RecoveredRecord {
                ledger: record.ledger,
                entry: record.entry,
                flags: record.flags,
                offset: record.offset,
                payload,
            });
        }
    }
}

/// Everything that survived must be exactly a prefix of what the writer
/// attempted, in append order, with byte-identical payloads.
fn assert_prefix(attempted: &[RecordSpec], recovered: &LedgerModel, context: &str) {
    assert!(
        recovered.records.len() <= attempted.len(),
        "{context}: recovered {} records from only {} attempts",
        recovered.records.len(),
        attempted.len()
    );
    for (index, got) in recovered.records.iter().enumerate() {
        let want = attempted[index];
        assert_eq!(
            (got.ledger, got.entry, got.flags),
            (want.ledger, want.entry, want.flags),
            "{context}: record {index} identity"
        );
        assert_eq!(
            got.payload,
            want.payload(),
            "{context}: record {index} payload (ledger {}, entry {})",
            want.ledger,
            want.entry
        );
    }
}

fn encode_sealed(segment_id: u64, epoch: u64, records: &[RecordSpec]) -> Bytes {
    let header = SegmentHeader {
        segment_id,
        writer_epoch: epoch,
        target_size: 0,
        created_at_ns: 0,
    };
    let mut encoder = SegmentEncoder::new(header);
    for record in records {
        encoder
            .push_record(
                record.ledger,
                record.entry,
                record.flags,
                &Bytes::from(record.payload()),
            )
            .expect("record fits the encoder");
    }
    encoder.seal().expect("seal succeeds")
}

/// Payload sizes that exercise both sides of the block boundary: 21 bytes of
/// record header + payload must fit in 4,080 bytes, so 4,059 is the largest
/// single-block record and 4,060 the smallest multi-block one.
fn payload_len() -> impl Strategy<Value = usize> {
    prop_oneof![
        Just(0usize),
        Just(1),
        Just(RECORD_HEADER_SIZE),
        Just(BLOCK_DATA_SIZE - RECORD_HEADER_SIZE),
        Just(BLOCK_DATA_SIZE - RECORD_HEADER_SIZE + 1),
        2usize..=128,
        129usize..=BLOCK_DATA_SIZE - RECORD_HEADER_SIZE,
        BLOCK_DATA_SIZE - RECORD_HEADER_SIZE + 1..=3 * BLOCK_DATA_SIZE,
    ]
}

/// Raw (ledger index, payload length, flags) tuples; entries are assigned by
/// `assign_entries` so every ledger's entries strictly increase with append order.
fn raw_records(max_records: usize) -> impl Strategy<Value = Vec<(usize, usize, u8)>> {
    prop::collection::vec((0usize..3, payload_len(), any::<u8>()), 0..=max_records)
}

fn assign_entries(raw: &[(usize, usize, u8)], next: &mut BTreeMap<u64, u64>) -> Vec<RecordSpec> {
    raw.iter()
        .map(|&(ledger_index, len, flags)| {
            let ledger = ledger_index as u64 + 1;
            let entry = next.entry(ledger).or_insert(1);
            let assigned = *entry;
            *entry += 1;
            RecordSpec {
                ledger,
                entry: assigned,
                flags,
                len,
            }
        })
        .collect()
}

/// Cut a sealed image at a fraction of its length; optionally snap the cut to
/// a 4 KiB block boundary (the crash point where a whole block was flushed).
fn cut_at(bytes: &Bytes, numerator: u64, block_aligned: bool) -> Bytes {
    let len = bytes.len() as u64;
    let mut cut = (len * numerator.min(4096)) / 4096;
    if block_aligned {
        cut -= cut % bkxpro_codec::BLOCK_SIZE as u64;
    }
    bytes.slice(..cut as usize)
}

proptest! {
    /// Sealed segments are exact: ordered reads, matching flags and payloads,
    /// footer rows agreeing with the record stream, and LAC = max entry.
    #[test]
    fn sealed_round_trip_matches_reference(raw in raw_records(16)) {
        let mut next = BTreeMap::new();
        let attempted = assign_entries(&raw, &mut next);
        let image = encode_sealed(1, 7, &attempted);

        let view = SegmentView::open_sealed(image.clone()).expect("canonical seal opens");
        prop_assert_eq!(view.header().segment_id, 1);
        prop_assert_eq!(view.header().writer_epoch, 7);

        let mut model = LedgerModel::default();
        model.ingest(&view);
        assert_prefix(&attempted, &model, "sealed");
        prop_assert_eq!(model.records.len(), attempted.len());

        // The read path is key-addressed: every attempted entry resolves to the
        // exact bytes the oracle says, once, at the footer-recorded offset.
        let by_key: BTreeMap<(u64, u64), &RecoveredRecord> =
            model.records.iter().map(|r| ((r.ledger, r.entry), r)).collect();
        prop_assert_eq!(by_key.len(), attempted.len());
        let footer = view.footer().expect("sealed view has a footer");
        prop_assert_eq!(footer.index_count(), attempted.len());
        prop_assert_eq!(footer.ledger_count(), model.lac.len());
        for index_entry in footer.index_entries() {
            let record = by_key
                .get(&(index_entry.ledger, index_entry.entry))
                .expect("footer row resolves");
            prop_assert_eq!(record.offset, index_entry.offset);
            prop_assert_eq!(record.payload.len() as u32, index_entry.len);
        }

        // LAC is exactly the highest attempted entry per ledger.
        let mut expected_lac: BTreeMap<u64, u64> = BTreeMap::new();
        for record in &attempted {
            expected_lac
                .entry(record.ledger)
                .and_modify(|lac| *lac = (*lac).max(record.entry))
                .or_insert(record.entry);
        }
        prop_assert_eq!(model.lac, expected_lac);
    }

    /// Cutting a sealed image at any offset exposes a prefix of the attempted
    /// records and nothing else. No acknowledgable prefix is ever lost, and no
    /// torn tail is ever surfaced as a record.
    #[test]
    fn truncation_always_recovers_a_prefix(
        raw in raw_records(16),
        numerator in 0u64..=4096,
        block_aligned in any::<bool>(),
    ) {
        let mut next = BTreeMap::new();
        let attempted = assign_entries(&raw, &mut next);
        let image = encode_sealed(2, 1, &attempted);
        let cut = cut_at(&image, numerator, block_aligned);

        if cut.len() == image.len() {
            // No damage: this is property 1 again, through the same cut path.
            let view = SegmentView::open_sealed(cut.clone()).expect("intact seal opens");
            let mut model = LedgerModel::default();
            model.ingest(&view);
            assert_prefix(&attempted, &model, "uncut");
            prop_assert_eq!(model.records.len(), attempted.len());
        } else {
            // A truncated image must not validate as sealed; recovery is the
            // only parser and it yields the durable prefix.
            prop_assert!(SegmentView::open_sealed(cut.clone()).is_err());

            let recovery = SegmentView::recover_open(cut.clone());
            prop_assert_eq!(
                recovery.view.is_none(),
                cut.len() < HEADER_SIZE,
                "a full header is always recoverable"
            );
            if let Some(view) = recovery.view {
                let mut model = LedgerModel::default();
                model.ingest(&view);
                assert_prefix(&attempted, &model, "truncated");

                prop_assert_eq!(view.data_end(), recovery.point.valid_len);
                prop_assert_eq!(recovery.point.valid_len % bkxpro_codec::BLOCK_SIZE, 0);
                prop_assert!(recovery.point.valid_len <= cut.len());
                prop_assert_eq!(recovery.point.complete_records, model.records.len());
                for record in &model.records {
                    let end = record.offset
                        + RECORD_HEADER_SIZE as u64
                        + record.payload.len() as u64;
                    prop_assert!(
                        end <= recovery.point.valid_len as u64,
                        "record at {} ends past the recovery point",
                        record.offset
                    );
                }
            }
        }
    }

    /// Fence: when a newer epoch rewrote a segment slot, recovery sees only the
    /// winning attempt. Superseded records never surface, the epoch is readable
    /// in the header, and the merged LAC never goes backwards.
    #[test]
    fn fenced_attempt_never_surfaces_and_lac_is_monotone(
        slots in prop::collection::vec(
            prop::collection::vec(raw_records(6), 1..=2),
            1..=3,
        ),
        cuts in prop::collection::vec((0u64..=4096, any::<bool>()), 1..=3),
    ) {
        let mut counters: BTreeMap<u64, u64> = BTreeMap::new();
        let mut global_lac: BTreeMap<u64, u64> = BTreeMap::new();

        for (slot_index, attempts) in slots.iter().enumerate() {
            let segment_id = slot_index as u64 + 1;
            let mut attempted: Vec<Vec<RecordSpec>> = Vec::new();
            for raw in attempts {
                attempted.push(assign_entries(raw, &mut counters));
            }
            let winner_epoch = attempted.len() as u64;
            let winner = attempted.last().expect("at least one attempt");

            let image = encode_sealed(segment_id, winner_epoch, winner);
            let (numerator, block_aligned) = cuts[slot_index % cuts.len()];
            let cut = cut_at(&image, numerator, block_aligned);

            let recovery = SegmentView::recover_open(cut.clone());
            if let Some(view) = recovery.view {
                prop_assert_eq!(view.header().segment_id, segment_id);
                prop_assert_eq!(view.header().writer_epoch, winner_epoch);

                let mut model = LedgerModel::default();
                model.ingest(&view);
                assert_prefix(winner, &model, "fenced winner");

                // No superseded attempt may contribute a record. Entries are
                // assigned globally increasing per ledger, so loser-only keys
                // are disjoint from the winner.
                let superseded: BTreeMap<(u64, u64), ()> = attempted[..attempted.len() - 1]
                    .iter()
                    .flatten()
                    .map(|record| ((record.ledger, record.entry), ()))
                    .collect();
                for record in &model.records {
                    prop_assert!(
                        !superseded.contains_key(&(record.ledger, record.entry)),
                        "superseded record ({}, {}) surfaced",
                        record.ledger,
                        record.entry
                    );
                }

                // LAC over slots processed in segment order never decreases: a
                // later slot can only append entries above the earlier LAC.
                for (ledger, lac) in &model.lac {
                    let previous = global_lac.insert(*ledger, *lac);
                    prop_assert!(
                        previous.is_none_or(|value| *lac >= value),
                        "LAC for ledger {} regressed from {:?} to {}",
                        ledger,
                        previous,
                        lac
                    );
                }
            }
        }
    }
}
