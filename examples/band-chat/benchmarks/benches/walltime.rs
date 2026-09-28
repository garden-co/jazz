//! BandChat wall-clock suite, measured on CodSpeed's macro runner. Names are
//! app-prefixed because the examples page matches results by exact name.

use jazz_example_band_chat_benchmark::{FastResumeFixture, Fixture};

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

#[divan::bench(args = [1024, 4096])]
fn band_chat_timeline_second_page(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let fixture = Fixture::new(message_count);
    bencher.bench_local(|| divan::black_box(fixture.timeline_page_count()));
}

#[divan::bench(args = [1024, 4096])]
fn band_chat_unread_recent_rooms(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let fixture = Fixture::new(message_count);
    bencher.bench_local(|| divan::black_box(fixture.unread_room_count()));
}

#[divan::bench(args = [1024, 4096])]
fn band_chat_author_history(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let fixture = Fixture::new(message_count);
    bencher.bench_local(|| divan::black_box(fixture.author_history_count()));
}

/// Measure the remaining manifest cost for thesis #2136. A fresh usage cannot
/// infer its input closure from a cursor; known row bodies remain deduplicated.
#[divan::bench(args = [100, 1_000, 10_000])]
fn band_chat_caught_up_fast_resume(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let mut fixture = FastResumeFixture::new(message_count);
    bencher.bench_local(|| {
        let receipt = fixture.caught_up_fast_resume();
        assert!(
            receipt.is_body_deduplicated_reset(),
            "caught-up resume leaked a payload: {receipt:?}"
        );
        divan::black_box(receipt)
    });
}
