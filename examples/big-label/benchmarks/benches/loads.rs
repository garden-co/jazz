use jazz_example_big_label_benchmark::Fixture;

fn main() {
    divan::main();
}

#[divan::bench(args = [512, 4096])]
fn label_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.label_load()));
}

#[divan::bench(args = [512, 4096])]
fn artist_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.artist_load()));
}

#[divan::bench(args = [512, 4096])]
fn catalog_load(bencher: divan::Bencher<'_, '_>, release_count: usize) {
    let fixture = Fixture::new(release_count);
    bencher.bench_local(|| divan::black_box(fixture.catalog_load()));
}
