use jazz_example_record_player_benchmark::Fixture;
fn main() {
    divan::main();
}
#[divan::bench(args = [128, 4096])]
fn coverflow(bencher: divan::Bencher<'_, '_>, tracks: usize) {
    let fixture = Fixture::new(tracks);
    bencher.bench_local(|| divan::black_box(fixture.coverflow_count()));
}
#[divan::bench(args = [128, 4096])]
fn ordered_playlist_window(bencher: divan::Bencher<'_, '_>, tracks: usize) {
    let fixture = Fixture::new(tracks);
    bencher.bench_local(|| divan::black_box(fixture.playlist_window_count()));
}

#[divan::bench(args = [128, 4096])]
fn metadata_only_tracks(bencher: divan::Bencher<'_, '_>, tracks: usize) {
    let fixture = Fixture::new(tracks);
    bencher.bench_local(|| divan::black_box(fixture.track_metadata_count()));
}
