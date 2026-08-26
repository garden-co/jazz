use jazz_example_big_label_benchmark::{Fixture, IngestFixture};

fn main() {
    divan::main();
}

#[divan::bench(args = [512, 4096])]
fn label_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.label_load()));
}

#[divan::bench(args = [512, 4096])]
fn artist_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.artist_load()));
}

#[divan::bench(args = [512, 4096])]
fn catalog_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.catalog_load()));
}

/// Thesis #1964: larger transactions should amortize fixed ingest work.
#[divan::bench(args = [1, 10, 100, 1_000])]
fn ingest_batch_amortization(bencher: divan::Bencher<'_, '_>, batch_size: usize) {
    const RELEASE_COUNT: usize = 1_000;
    bencher
        .with_inputs(IngestFixture::new)
        .bench_local_values(|fixture| fixture.ingest_releases(RELEASE_COUNT, batch_size));
}

/// Thesis #1964: the 1,000-row batch path must sustain a 10k-row app import.
/// Larger scales remain local/wall-time experiments until simulation is cheap
/// enough to fit the hosted workflow budget.
#[divan::bench(args = [10_000])]
fn ingest_high_scale(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    const BATCH_SIZE: usize = 1_000;
    bencher
        .with_inputs(IngestFixture::new)
        .bench_local_values(|fixture| fixture.ingest_releases(release_count, BATCH_SIZE));
}
