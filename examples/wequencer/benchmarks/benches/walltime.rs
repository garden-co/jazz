//! Wall-clock receipts for what a Wequencer bandmate notices. Names are
//! app-prefixed because the examples page matches CodSpeed results by exact
//! name; `metadata.ts` documents each timed iteration.

use jazz_example_wequencer_benchmark::Fixture;
use jazz_example_wequencer_benchmark::pad_history::PadHistoryFixture;
use jazz_example_wequencer_benchmark::pattern_views::PatternViewsFixture;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Opening a session: the track list plus 16 per-track step subscriptions.
#[divan::bench(sample_count = 50)]
fn wequencer_open_pattern(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.open_pattern().1));
}

/// Toggling one pad while the whole 16×64 grid stays subscribed.
#[divan::bench(sample_count = 100)]
fn wequencer_toggle_pad(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    let (mut live, _) = fixture.open_pattern();
    bencher.bench_local(|| divan::black_box(fixture.toggle_pad(&mut live)));
}

const PATTERN_VIEWS: usize = 100;

/// 100 bandmates each open their own pattern view: one prepared query shape,
/// 100 parameter bindings, each hydrated and consumed. Fixture setup is outside
/// the timing (`skip_ext_time`).
#[divan::bench(args = [PATTERN_VIEWS], sample_count = 3, skip_ext_time)]
fn wequencer_open_pattern_views(bencher: divan::Bencher<'_, '_>, patterns: usize) {
    bencher
        .with_inputs(|| PatternViewsFixture::seeded(patterns))
        .bench_local_values(|fixture| fixture.open_all().runtime.active_subscriptions);
}

/// Read a pad's current value after it was toggled `depth` times while
/// offline, each edit settled locally on top of the last (RocksDB).
#[divan::bench(args = [1_000, 10_000])]
fn wequencer_pad_edit_history(bencher: divan::Bencher<'_, '_>, depth: usize) {
    let mut fixture = PadHistoryFixture::new(depth);
    fixture.assert_receipt();
    bencher.bench_local(|| divan::black_box(fixture.current_rows()));
}
