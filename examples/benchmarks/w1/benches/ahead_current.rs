use jazz::tx::DurabilityTier;
use jazz_example_benchmark_w1::AheadCurrentFixture;

fn main() {
    divan::main();
}

#[divan::bench(args = [100, 1_000, 10_000])]
fn local_ahead_current_history(bencher: divan::Bencher<'_, '_>, depth: usize) {
    let mut fixture = AheadCurrentFixture::new(depth, DurabilityTier::Local);
    fixture.assert_receipt();
    bencher.bench_local(|| divan::black_box(fixture.current_rows()));
}

#[divan::bench(args = [100, 1_000, 10_000])]
fn edge_ahead_current_history(bencher: divan::Bencher<'_, '_>, depth: usize) {
    let mut fixture = AheadCurrentFixture::new(depth, DurabilityTier::Edge);
    fixture.assert_receipt();
    bencher.bench_local(|| divan::black_box(fixture.current_rows()));
}
