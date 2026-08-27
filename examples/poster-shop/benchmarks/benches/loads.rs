use jazz_example_poster_shop_benchmark::Fixture;
fn main() {
    divan::main();
}
#[divan::bench(args = [512, 4096])]
fn ordered_canvas_shapes(bencher: divan::Bencher<'_, '_>, shapes: usize) {
    let fixture = Fixture::new(shapes);
    bencher.bench_local(|| divan::black_box(fixture.ordered_shape_count()));
}
#[divan::bench(args = [512, 4096])]
fn ordered_canvas_layers(bencher: divan::Bencher<'_, '_>, shapes: usize) {
    let fixture = Fixture::new(shapes);
    bencher.bench_local(|| divan::black_box(fixture.ordered_layer_count()));
}
#[divan::bench(args = [512, 4096])]
fn active_layer_shapes(bencher: divan::Bencher<'_, '_>, shapes: usize) {
    let fixture = Fixture::new(shapes);
    bencher.bench_local(|| divan::black_box(fixture.layer_shape_count()));
}
#[divan::bench(args = [512, 4096])]
fn cursor_presence_fanout(bencher: divan::Bencher<'_, '_>, shapes: usize) {
    let fixture = Fixture::new(shapes);
    bencher.bench_local(|| {
        divan::black_box(shapes);
        divan::black_box(fixture.cursor_fanout_count())
    });
}

#[divan::bench(args = [512, 4096])]
fn metadata_and_checkpoint_shelves(bencher: divan::Bencher<'_, '_>, shapes: usize) {
    let fixture = Fixture::new(shapes);
    bencher.bench_local(|| {
        divan::black_box(fixture.asset_metadata_count());
        divan::black_box(fixture.checkpoint_count())
    });
}
