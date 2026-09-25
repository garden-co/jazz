//! Wall-clock receipts for what a Wequencer bandmate notices. Names are
//! app-prefixed because the examples page matches CodSpeed results by exact
//! name; `metadata.ts` documents each timed iteration.

use jazz_example_wequencer_benchmark::Fixture;

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
