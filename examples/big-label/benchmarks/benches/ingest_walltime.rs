use jazz_example_big_label_benchmark::IngestFixture;

fn main() {
    divan::main();
}

/// Thesis #1964 wall-clock receipt: realistic app ingestion at scales that are
/// too expensive for CodSpeed's instruction-level simulation.
#[divan::bench(args = [10_000, 100_000])]
fn ingest_walltime(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    const BATCH_SIZE: usize = 1_000;
    bencher
        .with_inputs(IngestFixture::new)
        .bench_local_values(|fixture| fixture.ingest_releases(release_count, BATCH_SIZE));
}
