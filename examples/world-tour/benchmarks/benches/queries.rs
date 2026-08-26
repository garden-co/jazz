use jazz_example_world_tour_benchmark::Fixture;

fn main() {
    divan::main();
}

#[divan::bench(args = [128, 4096])]
fn calendar_window(bencher: divan::Bencher<'_, '_>, stop_count: usize) {
    let fixture = Fixture::new(stop_count);
    bencher.bench_local(|| divan::black_box(fixture.calendar_window_count()));
}

#[divan::bench(args = [128, 4096])]
fn map_viewport(bencher: divan::Bencher<'_, '_>, stop_count: usize) {
    let fixture = Fixture::new(stop_count);
    bencher.bench_local(|| divan::black_box(fixture.map_viewport_count()));
}
