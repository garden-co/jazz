use jazz_example_epic_drop_benchmark::Fixture;

fn main() {
    divan::main();
}

#[divan::bench(args = [256 * 1024, 4 * 1024 * 1024])]
fn folder_listing(bencher: divan::Bencher<'_, '_>, file_bytes: usize) {
    let fixture = Fixture::new(file_bytes);
    bencher.bench_local(|| divan::black_box(fixture.list_folder()));
}

#[divan::bench(args = [256 * 1024, 4 * 1024 * 1024])]
fn middle_range_download(bencher: divan::Bencher<'_, '_>, file_bytes: usize) {
    let fixture = Fixture::new(file_bytes);
    bencher.bench_local(|| divan::black_box(fixture.download_middle_range()));
}
