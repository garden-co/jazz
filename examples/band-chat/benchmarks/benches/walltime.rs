//! BandChat wall-clock suite, measured on CodSpeed's macro runner. Names are
//! app-prefixed because the examples page matches results by exact name.

use jazz_example_band_chat_benchmark::live_rooms::LiveRoomsFixture;
use jazz_example_band_chat_benchmark::membership_room::{OpenFixture, SendFixture};
use jazz_example_band_chat_benchmark::{FastResumeFixture, Fixture};

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Scroll back in a busy room: the second 25-message page (offset read).
#[divan::bench(args = [4096])]
fn band_chat_timeline_second_page(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let fixture = Fixture::new(message_count);
    bencher.bench_local(|| divan::black_box(fixture.timeline_page_count()));
}

/// A member's rooms with unread messages, most recently active first.
#[divan::bench(args = [4096])]
fn band_chat_unread_recent_rooms(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let fixture = Fixture::new(message_count);
    bencher.bench_local(|| divan::black_box(fixture.unread_room_count()));
}

/// Measure the remaining manifest cost for thesis #2136. A fresh usage cannot
/// infer its input closure from a cursor; known row bodies remain deduplicated.
#[divan::bench(args = [100, 10_000])]
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

/// A member opens a private room: the newest 21 messages with their senders,
/// through the membership read policy, until the first published result.
#[divan::bench(args = [10_000], sample_count = 20, sample_size = 1)]
fn band_chat_open_room(bencher: divan::Bencher<'_, '_>, messages: usize) {
    bencher
        .with_inputs(|| OpenFixture::new(messages))
        .bench_local_refs(|fixture| fixture.open_chat());
}

/// A member sends 100 messages into an open room; each passes the membership
/// and own-profile insert policy and reaches the open room's page.
#[divan::bench(args = [10_000], sample_count = 20, sample_size = 1)]
fn band_chat_send_100(bencher: divan::Bencher<'_, '_>, messages: usize) {
    bencher
        .with_inputs(|| SendFixture::new(messages))
        .bench_local_refs(|fixture| fixture.send_messages(100));
}

const ROOMS_OPEN: usize = 100;

/// One new message while 100 members each keep a different room open (one
/// query shape, 100 bindings); only the busy room's view changes. Fixture
/// setup and teardown are outside the timing (`skip_ext_time`).
#[divan::bench(args = [ROOMS_OPEN], sample_count = 3, skip_ext_time)]
fn band_chat_new_message_rooms_open(bencher: divan::Bencher<'_, '_>, rooms: usize) {
    bencher
        .with_inputs(|| LiveRoomsFixture::seeded(rooms).open_all())
        .bench_local_values(|open| open.new_message());
}
