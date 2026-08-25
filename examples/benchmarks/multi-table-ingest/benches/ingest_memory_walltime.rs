use jazz::groove::storage::MemoryStorage;
use jazz_example_multi_table_ingest_benchmark::IngestFixture;

fn main() {
    divan::main();
}

/// Thesis #2030: the next fixed 1k insert batch should not depend on table depth.
#[divan::bench(args = [0, 1_000, 3_000, 5_000], sample_count = 1)]
fn next_1k_jobs_memory(bencher: divan::Bencher<'_, '_>, existing_jobs: usize) {
    bencher
        .with_inputs(|| IngestFixture::<MemoryStorage>::memory(existing_jobs))
        .bench_local_values(IngestFixture::insert_next_1k);
}

/// Differential receipt for the trusted-backend attribution path.
#[divan::bench(args = [0, 1_000, 3_000, 5_000], sample_count = 1)]
fn next_1k_jobs_memory_attributed(bencher: divan::Bencher<'_, '_>, existing_jobs: usize) {
    bencher
        .with_inputs(|| IngestFixture::<MemoryStorage>::memory_attributed(existing_jobs))
        .bench_local_values(IngestFixture::insert_next_1k);
}
