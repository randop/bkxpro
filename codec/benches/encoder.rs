use bkxpro_codec::{
    is_4k_aligned_buffer, AlignedSegmentEncoder, SegmentEncoder, SegmentHeader, BLOCK_SIZE,
};
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

const RECORDS: usize = 256;
const PAYLOAD_SIZE: usize = 1024;
const OUTPUT_CAPACITY: usize = 2 * 1024 * 1024;

fn make_header() -> SegmentHeader {
    SegmentHeader {
        segment_id: 1,
        writer_epoch: 1,
        target_size: 32 * 1024 * 1024,
        created_at_ns: 1_700_000_000,
    }
}

fn aligned_storage(capacity: usize) -> (Vec<u8>, usize) {
    assert_eq!(capacity % BLOCK_SIZE, 0);
    let storage = vec![0u8; capacity + BLOCK_SIZE];
    let base = storage.as_ptr() as usize;
    let offset = (BLOCK_SIZE - base % BLOCK_SIZE) % BLOCK_SIZE;
    (storage, offset)
}

fn encoder_throughput(c: &mut Criterion) {
    let payload = Bytes::from(vec![0x5au8; PAYLOAD_SIZE]);
    let logical_bytes = (RECORDS * (21 + PAYLOAD_SIZE)) as u64;
    let mut group = c.benchmark_group("segment_encoder_no_intermediate_segment_copy");
    group.throughput(Throughput::Bytes(logical_bytes));

    group.bench_function("aligned_caller_buffer_in_place", |b| {
        let (mut storage, offset) = aligned_storage(OUTPUT_CAPACITY);
        assert!(is_4k_aligned_buffer(
            &storage[offset..offset + OUTPUT_CAPACITY]
        ));
        b.iter(|| {
            let out = &mut storage[offset..offset + OUTPUT_CAPACITY];
            let mut encoder = AlignedSegmentEncoder::new(make_header(), out).unwrap();
            for entry in 0..RECORDS as u64 {
                encoder.push_record(1, entry, 0, &payload).unwrap();
            }
            black_box(encoder.seal().unwrap());
        });
    });

    group.bench_function("growable_reference_encoder", |b| {
        b.iter(|| {
            let mut encoder = SegmentEncoder::new(make_header());
            for entry in 0..RECORDS as u64 {
                encoder.push_record(1, entry, 0, &payload).unwrap();
            }
            black_box(encoder.seal().unwrap());
        });
    });
    group.finish();
}

criterion_group!(benches, encoder_throughput);
criterion_main!(benches);
