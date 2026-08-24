use jazz_example_band_binder_benchmark::Fixture;

fn main() {
    divan::main();
}

#[divan::bench(args = [128, 4096])]
fn ordered_sibling_window(bencher: divan::Bencher<'_, '_>, block_count: usize) {
    let fixture = Fixture::new(block_count);
    bencher.bench_local(|| divan::black_box(fixture.sibling_window_count()));
}

#[divan::bench(args = [128, 4096])]
fn recursive_child_page_step(bencher: divan::Bencher<'_, '_>, block_count: usize) {
    let fixture = Fixture::new(block_count);
    bencher.bench_local(|| divan::black_box(fixture.child_page_count()));
}
