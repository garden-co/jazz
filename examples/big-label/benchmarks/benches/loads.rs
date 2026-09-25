use jazz_example_big_label_benchmark::{Fixture, IngestFixture};

// Wall-clock suite on CodSpeed's macro runner. Names are app-prefixed because
// the examples page matches results by exact name. The 10k-row import lives in
// ingest_walltime.rs as ingest_walltime_10k.

fn main() {
    divan::main();
}

#[divan::bench(args = [512, 4096])]
fn big_label_label_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.label_load()));
}

#[divan::bench(args = [512, 4096])]
fn big_label_artist_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.artist_load()));
}

#[divan::bench(args = [512, 4096])]
fn big_label_catalog_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.catalog_load()));
}

/// Thesis #1964: larger transactions should amortize fixed ingest work.
#[divan::bench(args = [1, 10, 100, 1_000])]
fn big_label_ingest_batch_amortization(bencher: divan::Bencher<'_, '_>, batch_size: usize) {
    const RELEASE_COUNT: usize = 1_000;
    bencher
        .with_inputs(IngestFixture::new)
        .bench_local_values(|fixture| fixture.ingest_releases(RELEASE_COUNT, batch_size));
}
