use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use lzy::{compress, decompress};
use std::fs;
use std::time::Duration;

fn compression_benchmark(c: &mut Criterion) {
    let data = fs::read("data/compression_66k_JSON.txt").expect("Failed to read benchmark data");

    let mut group = c.benchmark_group("LZY Compression");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);
    group.throughput(Throughput::Bytes(data.len() as u64));

    let compressed = compress(&data);
    let compression_ratio = compressed.len() as f64 / data.len() as f64;
    println!(
        "Compression ratio (compressed/original): {:.4} ({} / {} bytes)",
        compression_ratio,
        compressed.len(),
        data.len()
    );

    group.bench_function("compress", |b| b.iter(|| compress(black_box(&data))));

    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(data, decompressed);

    group.bench_function("decompress", |b| {
        b.iter(|| decompress(black_box(&compressed)))
    });
}

criterion_group!(benches, compression_benchmark);
criterion_main!(benches);
