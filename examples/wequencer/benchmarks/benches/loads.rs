use jazz_example_wequencer_benchmark::Fixture;

fn main() {
    divan::main();
}

#[divan::bench]
fn full_ordered_track(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.track_steps()));
}

#[divan::bench(args = [(0, 16), (24, 16), (48, 16)])]
fn playhead_window(bencher: divan::Bencher<'_, '_>, range: (usize, usize)) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.playhead_window(range.0, range.1)));
}

#[divan::bench(args = [8, 32, 64])]
fn concurrent_edit_burst(bencher: divan::Bencher<'_, '_>, editors: usize) {
    bencher.bench_local(|| {
        let fixture = Fixture::new();
        divan::black_box(fixture.concurrent_edit_burst(editors))
    });
}

#[divan::bench]
fn subscribed_step_edit(bencher: divan::Bencher<'_, '_>) {
    bencher.bench_local(|| divan::black_box(Fixture::new().subscribed_step_edit()));
}

#[divan::bench(args = [1, 8, 32])]
fn subscribed_step_fanout(bencher: divan::Bencher<'_, '_>, listeners: usize) {
    bencher.bench_local(|| divan::black_box(Fixture::new().subscribed_step_fanout(listeners)));
}
