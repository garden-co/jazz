// Historical backfill harness: this target keeps main's file path and function
// identity so CodSpeed files the result under the same benchmark ID, but runs
// only the subscription fan-out cases. The other W1 memory cases were measured
// on main before every backfilled release and are not re-measured here.
fn main() {
    divan::main();
}

/// Independent dashboard bindings through core -> relay -> foreground.
#[divan::bench(args = [(600, 0), (600, 10), (600, 60), (6000, 60)], sample_count = 3)]
fn subscription_fanout_memory(bencher: divan::Bencher<'_, '_>, (rows, lists): (usize, usize)) {
    use jazz_example_benchmark_w1::subscription_fanout::FanoutFixture;
    bencher
        .with_inputs(|| FanoutFixture::new(rows, lists))
        .bench_local_values(|mut fixture| {
            let receipt = fixture.hydrate();
            (fixture, receipt)
        });
}
