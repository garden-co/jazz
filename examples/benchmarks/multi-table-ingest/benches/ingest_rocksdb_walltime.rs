use jazz_example_multi_table_ingest_benchmark::IngestFixture;
use jazz_storage_rocksdb::RocksDbStorage;

fn main() {
    divan::main();
}

/// Persistent-backend companion to the fixed-batch scaling receipt.
#[divan::bench(args = [0, 1_000, 3_000, 5_000], sample_count = 1)]
fn next_1k_jobs_rocksdb(bencher: divan::Bencher<'_, '_>, existing_jobs: usize) {
    bencher
        .with_inputs(|| IngestFixture::<RocksDbStorage>::rocksdb(existing_jobs))
        .bench_local_values(|(_dir, fixture)| fixture.insert_next_1k());
}
