use jazz_example_big_label_benchmark::live_view::LiveViewFixture;
use jazz_example_big_label_benchmark::{Fixture, IngestFixture};

// Wall-clock suite on CodSpeed's macro runner. Names are app-prefixed because
// the examples page matches results by exact name. The 100k-row import lives
// in ingest_walltime.rs as ingest_walltime_100k.

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// A label page: every release of one label, newest first (indexed read at
/// tenant scale).
#[divan::bench(args = [4096])]
fn big_label_label_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.label_load()));
}

/// Thesis #1964: larger transactions should amortize fixed ingest work.
#[divan::bench(args = [1, 100, 1_000])]
fn big_label_ingest_batch_amortization(bencher: divan::Bencher<'_, '_>, batch_size: usize) {
    const RELEASE_COUNT: usize = 1_000;
    bencher
        .with_inputs(IngestFixture::new)
        .bench_local_values(|fixture| fixture.ingest_releases(RELEASE_COUNT, batch_size));
}

/// Open one label's live release view over a persisted 100,000-release,
/// multi-tenant table: the initial hydration must follow the label index.
/// Seeding, reopening, preparation and one fully asserted validation pass are
/// outside the timed closure; so is retiring each dropped view.
#[divan::bench(sample_count = 3)]
fn big_label_releases_live_view_100k(bencher: divan::Bencher<'_, '_>) {
    let fixture = LiveViewFixture::new(100_000);
    let baseline = fixture.active_groove_subscriptions();
    fixture.assert_selective_hydration();
    fixture.assert_subscription_baseline(baseline, "validation hydration");
    bencher.bench_local(|| divan::black_box(fixture.hydrate()));
    fixture.assert_subscription_baseline(baseline, "Divan samples");
}
