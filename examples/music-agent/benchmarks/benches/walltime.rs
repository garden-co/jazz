//! MusicAgent wall-clock receipts for what a user of an agent chat notices:
//! how fast a streamed reply lands, how quickly a long conversation opens
//! (live and after an app restart). Seeking into audio is measured by
//! RecordPlayer (`record_player_scrub_track_64mb`). Names are app-prefixed
//! because the examples page matches CodSpeed results by exact name.

use jazz_example_music_agent_benchmark::Fixture;

const TURNS: usize = 200;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Stream a 1,000-chunk assistant reply (24 bytes per chunk, roughly a few
/// tokens each) onto a turn that is already a large value.
#[divan::bench(sample_count = 5)]
fn music_agent_stream_reply_1000_chunks(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(Fixture::new)
        .bench_local_values(|fixture| {
            fixture.stream_reply(1_000, 24);
            fixture
        });
}

/// Open a 200-turn conversation: read and materialize every turn body,
/// including the long streamed reply.
#[divan::bench]
fn music_agent_open_transcript_200_turns(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::with_shape(TURNS, 256 * 1024);
    bencher.bench_local(|| divan::black_box(fixture.materialized_transcript()));
}

/// Reopen the database after an app restart and read the same 200-turn
/// conversation from storage.
#[divan::bench(sample_count = 20)]
fn music_agent_reopen_transcript_200_turns(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::with_shape(TURNS, 256 * 1024);
    bencher.bench_local(|| divan::black_box(fixture.restarted_transcript()));
}
