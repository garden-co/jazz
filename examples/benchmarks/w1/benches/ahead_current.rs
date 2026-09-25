use jazz_example_benchmark_w1::AheadCurrentFixture;

// Wall-clock suite on CodSpeed's macro runner; the app prefix keeps the name
// unique on the examples page.

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

#[divan::bench(args = [100, 1_000, 10_000])]
fn w1_local_ahead_current_history(bencher: divan::Bencher<'_, '_>, depth: usize) {
    let mut fixture = AheadCurrentFixture::new(depth);
    fixture.assert_receipt();
    bencher.bench_local(|| divan::black_box(fixture.current_rows()));
}
