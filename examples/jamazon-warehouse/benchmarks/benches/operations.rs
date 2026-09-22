use jazz_example_jamazon_warehouse_benchmark::Fixture;
fn main() {
    divan::main();
}
#[divan::bench(args = [100, 10_000])]
fn pending_orders(bencher: divan::Bencher<'_, '_>, orders: usize) {
    let fixture = Fixture::new(orders);
    bencher.bench_local(|| divan::black_box(fixture.pending_order_count()));
}
#[divan::bench(args = [100, 10_000])]
fn low_stock(bencher: divan::Bencher<'_, '_>, orders: usize) {
    let fixture = Fixture::new(orders);
    bencher.bench_local(|| divan::black_box(fixture.low_stock_count()));
}
