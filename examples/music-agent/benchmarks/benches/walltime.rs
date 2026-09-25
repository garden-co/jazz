//! MusicAgent wall-clock receipts for what a user of an agent chat notices:
//! how fast a streamed reply lands, how quickly a long conversation opens
//! (live and after an app restart), and how fast a seek into an audio
//! attachment returns. Names are app-prefixed because the examples page
//! matches CodSpeed results by exact name.

use jazz_example_music_agent_benchmark::Fixture;

const MIB: usize = 1024 * 1024;
const TURNS: usize = 200;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Stream a 1,000-chunk assistant reply (24 bytes per chunk, roughly a few
/// tokens each) onto a turn that is already a large value.
#[divan::bench(sample_count = 10)]
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

/// Seek into the middle of an 8 MiB audio attachment and read 64 KiB.
#[divan::bench]
fn music_agent_attachment_seek_8mb(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::with_shape(2, 8 * MIB);
    bencher.bench_local(|| divan::black_box(fixture.attachment_seek()));
}
