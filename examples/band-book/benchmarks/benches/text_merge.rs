//! BandBook block text merges beyond one paragraph, measured by the nightly
//! CodSpeed run on main: an offline chain of writes, long tour notes, and a
//! merge past its work budget. The per-merge suite (`walltime`) measures the
//! fast path and one paragraph two devices edited offline. Run locally with
//! `cargo bench -p jazz-example-band-book-benchmark --bench text_merge`.

use jazz_example_band_book_benchmark::text_merge;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// A device reconnects after typing into a paragraph offline, one write per
/// pause, while the other device kept editing it: every write of the chain
/// is merged over what its writer had seen.
#[divan::bench(args = [20], sample_count = 5, sample_size = 1)]
fn band_book_text_offline_chain(bencher: divan::Bencher, writes: usize) {
    bencher
        .with_inputs(|| text_merge::offline_chain(writes))
        .bench_local_values(|reconnect| divan::black_box(reconnect.run()));
}

/// Two devices made scattered edits to a long tour note offline; the second
/// to reconnect has its write merged with the first.
#[divan::bench(args = [100_000, 1_000_000], sample_count = 3, sample_size = 1)]
fn band_book_text_long_note(bencher: divan::Bencher, bytes: usize) {
    bencher
        .with_inputs(|| text_merge::long_note(bytes))
        .bench_local_values(|reconnect| divan::black_box(reconnect.run()));
}

/// One device replaced a long note while the other edited it offline: the
/// merge runs out of its work budget and the later write's value wins.
#[divan::bench(args = [1_000_000], sample_count = 3, sample_size = 1)]
fn band_book_text_over_budget(bencher: divan::Bencher, bytes: usize) {
    bencher
        .with_inputs(|| text_merge::over_budget(bytes).0)
        .bench_local_values(|reconnect| divan::black_box(reconnect.run()));
}
