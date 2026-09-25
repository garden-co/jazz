//! Wall-clock receipts for what a PosterShop editor notices. Names are
//! app-prefixed because the examples page matches CodSpeed results by exact
//! name; `metadata.ts` documents each timed iteration.

use jazz_example_poster_shop_benchmark::Fixture;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Opening a poster: subscribe to all five canvas surfaces and receive their
/// first results.
#[divan::bench(args = [512, 4096], sample_count = 20)]
fn poster_shop_open_canvas(bencher: divan::Bencher<'_, '_>, shapes: usize) {
    let fixture = Fixture::new(shapes);
    bencher.bench_local(|| divan::black_box(fixture.open_canvas()));
}

/// Drawing a shape: one local insert until the live ordered canvas shows it.
#[divan::bench(args = [4096], sample_count = 50)]
fn poster_shop_add_shape(bencher: divan::Bencher<'_, '_>, shapes: usize) {
    let fixture = Fixture::new(shapes);
    let mut live = fixture.live_canvas();
    bencher.bench_local(|| divan::black_box(fixture.add_shape(&mut live)));
}

/// A collaborator's cursor moving while the 4,096-shape canvas stays live.
#[divan::bench(args = [4096], sample_count = 50)]
fn poster_shop_move_cursor(bencher: divan::Bencher<'_, '_>, shapes: usize) {
    let fixture = Fixture::new(shapes);
    let mut live = fixture.live_canvas();
    bencher.bench_local(|| divan::black_box(fixture.move_cursor(&mut live)));
}
