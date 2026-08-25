use jazz_example_big_label_benchmark::IngestFixture;

fn main() {
    divan::main();
}

/// Thesis #1964 wall-clock receipt: realistic app ingestion at scales that are
/// too expensive for CodSpeed's instruction-level simulation.
#[divan::bench(sample_count = 20)]
fn ingest_walltime_10k(bencher: divan::Bencher<'_, '_>) {
    const BATCH_SIZE: usize = 1_000;
    bencher
        .with_inputs(IngestFixture::new)
        .bench_local_values(|fixture| fixture.ingest_releases(10_000, BATCH_SIZE));
}

/// Five rounds keep the 100k receipt within the hosted job budget while still
/// exposing its distribution. Increase this only when the ingest path is fast
/// enough to preserve that bound.
#[divan::bench(sample_count = 5)]
fn ingest_walltime_100k(bencher: divan::Bencher<'_, '_>) {
    const BATCH_SIZE: usize = 1_000;
    bencher
        .with_inputs(IngestFixture::new)
        .bench_local_values(|fixture| fixture.ingest_releases(100_000, BATCH_SIZE));
}
