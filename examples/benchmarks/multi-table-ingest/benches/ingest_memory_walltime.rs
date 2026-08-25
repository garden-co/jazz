use jazz::groove::storage::MemoryStorage;
use jazz_example_multi_table_ingest_benchmark::{ClientIngestFixture, IngestFixture};

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

/// Differential for an indexed correlated EXISTS insert policy.
#[divan::bench(args = [0, 1_000, 3_000, 5_000], sample_count = 1)]
fn next_1k_jobs_memory_attributed_exists(bencher: divan::Bencher<'_, '_>, existing_jobs: usize) {
    bencher
        .with_inputs(|| {
            IngestFixture::<MemoryStorage>::memory_attributed_with_exists_policy(existing_jobs)
        })
        .bench_local_values(IngestFixture::insert_next_1k);
}

/// Differential for the synchronous fate check performed by `JazzClient`.
#[divan::bench(args = [0, 1_000, 3_000, 5_000], sample_count = 1)]
fn next_1k_jobs_memory_with_write_state_check(
    bencher: divan::Bencher<'_, '_>,
    existing_jobs: usize,
) {
    bencher
        .with_inputs(|| {
            IngestFixture::<MemoryStorage>::memory_with_write_state_check(existing_jobs)
        })
        .bench_local_values(IngestFixture::insert_next_1k);
}

/// Exact public Rust client execution-model differential.
#[divan::bench(args = [0, 1_000, 3_000, 5_000], sample_count = 1)]
fn next_1k_jobs_public_client_memory(bencher: divan::Bencher<'_, '_>, existing_jobs: usize) {
    bencher
        .with_inputs(|| ClientIngestFixture::memory(existing_jobs))
        .bench_local_values(ClientIngestFixture::insert_next_1k);
}
