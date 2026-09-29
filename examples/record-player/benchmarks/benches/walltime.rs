//! Wall-clock receipts for what a RecordPlayer listener notices. Names are
//! app-prefixed because the examples page matches CodSpeed results by exact
//! name; `metadata.ts` documents each timed iteration.

use jazz_example_record_player_benchmark::Fixture;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Opening the library: CoverFlow shelf plus the focused album's tracks.
#[divan::bench(args = [4096], sample_count = 50)]
fn record_player_open_coverflow(bencher: divan::Bencher<'_, '_>, tracks: usize) {
    let fixture = Fixture::new(tracks);
    bencher.bench_local(|| divan::black_box(fixture.open_coverflow()));
}

/// Opening a playlist of every track and showing its visible window.
#[divan::bench(args = [4096], sample_count = 50)]
fn record_player_open_playlist(bencher: divan::Bencher<'_, '_>, tracks: usize) {
    let fixture = Fixture::new(tracks);
    bencher.bench_local(|| divan::black_box(fixture.open_playlist()));
}

/// Adding a track inside the visible window of a live 4,096-entry playlist.
#[divan::bench(args = [4096], sample_count = 50)]
fn record_player_add_to_playlist(bencher: divan::Bencher<'_, '_>, tracks: usize) {
    let fixture = Fixture::new(tracks);
    let mut live = fixture.live_playlist();
    bencher.bench_local(|| divan::black_box(fixture.add_to_playlist(&mut live)));
}
