//! WorldTour wall-clock suite, measured on CodSpeed's macro runner. Names are
//! app-prefixed because the examples page matches results by exact name.

use jazz_example_world_tour_benchmark::Fixture;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

#[divan::bench(args = [128, 4096])]
fn world_tour_member_calendar_window(bencher: divan::Bencher<'_, '_>, stop_count: usize) {
    let fixture = Fixture::new(stop_count);
    bencher.bench_local(|| divan::black_box(fixture.member_calendar_window_count()));
}

#[divan::bench(args = [128, 4096])]
fn world_tour_public_calendar_window(bencher: divan::Bencher<'_, '_>, stop_count: usize) {
    let fixture = Fixture::new(stop_count);
    bencher.bench_local(|| divan::black_box(fixture.public_calendar_window_count()));
}
