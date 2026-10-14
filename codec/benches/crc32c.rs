use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn crc32c_throughput(c: &mut Criterion) {
    // The crc32c crate dispatches to the CPU's accelerated implementation when
    // available (for example SSE4.2 on x86_64 or the CRC extension on AArch64).
    // Run on the target production CPU to measure the actual selected path.
    let mut group = c.benchmark_group("crc32c_auto_dispatch");
    for size in [4096usize, 64 * 1024, 1024 * 1024] {
        let data = vec![0x5au8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, bytes| {
            b.iter(|| crc32c::crc32c(black_box(bytes.as_slice())))
        });
    }
    group.finish();
}

criterion_group!(benches, crc32c_throughput);
criterion_main!(benches);
