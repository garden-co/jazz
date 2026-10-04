//! BandChat after years of use, measured by the nightly CodSpeed run on main,
//! not on every merge: every case runs against a band of 303,695 messages (one
//! room of 100,000), which takes 17 s to seed and seconds per read today, so the
//! suite does not fit the per-merge budget. Run locally with
//! `cargo bench -p jazz-example-band-chat-benchmark --bench nightly`.
//!
//! Each case seeds its own band, so no case sees another's writes or caches.
//! Reads finalize the previous iteration's dropped subscription before the
//! timing starts (`settle` as the input), so teardown is never measured.

use jazz_example_band_chat_benchmark::deep_room::{
    self, DeepRoom, LiveInbox, LiveUnreadCount, LiveWindow, ReceiptsFanout, Shape,
};

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Messages in the deep room of the band fixture: one room with years of
/// history among 1,000 rooms (ten of 10,000 messages, the rest 10–200).
const DEEP: usize = 100_000;
/// Rooms the reader belongs to: the room list's size.
const INBOX_ROOMS: usize = 100;
/// Members of the deep room.
const DEEP_MEMBERS: usize = 50;
const _: () = assert!(INBOX_ROOMS == deep_room::INBOX_ROOMS);
const _: () = assert!(DEEP_MEMBERS == deep_room::DEEP_MEMBERS);

fn band() -> DeepRoom {
    DeepRoom::seeded(Shape::band(DEEP))
}

/// A member opens the deep room: the newest 50 messages, each with its sender
/// and reactions, through the membership read policy, until published.
#[divan::bench(args = [DEEP], sample_count = 3, sample_size = 1)]
fn band_chat_open_deep_room(bencher: divan::Bencher<'_, '_>, _deep: usize) {
    let room = band();
    bencher
        .with_inputs(|| room.settle())
        .bench_local_values(|()| room.open_newest_page());
}

/// Scrolling back: the 50 messages before a cursor halfway down the deep
/// room, with senders and reactions.
#[divan::bench(args = [DEEP], sample_count = 3, sample_size = 1)]
fn band_chat_scroll_back_deep(bencher: divan::Bencher<'_, '_>, _deep: usize) {
    let room = band();
    bencher
        .with_inputs(|| room.settle())
        .bench_local_values(|()| room.open_older_page());
}

/// Jumping to a message a quarter of the way into the deep room: 25 messages
/// up to it and 25 after it, as two bounded windows.
#[divan::bench(args = [DEEP], sample_count = 3, sample_size = 1)]
fn band_chat_jump_to_message(bencher: divan::Bencher<'_, '_>, _deep: usize) {
    let room = band();
    bencher
        .with_inputs(|| room.settle())
        .bench_local_values(|()| room.jump_to_message());
}

/// Searching the deep room for a word: the newest 50 hits with senders, one
/// read.
#[divan::bench(args = [DEEP], sample_count = 3, sample_size = 1)]
fn band_chat_search_room(bencher: divan::Bencher<'_, '_>, _deep: usize) {
    let room = band();
    bencher
        .with_inputs(|| room.settle())
        .bench_local_values(|()| room.search_room());
}

/// The reader has the deep room open; a bandmate's message arrives and
/// reaches the page.
#[divan::bench(args = [DEEP], sample_count = 10, sample_size = 1)]
fn band_chat_live_window_new_message(bencher: divan::Bencher<'_, '_>, _deep: usize) {
    let mut window = LiveWindow::new(band());
    bencher.bench_local(|| window.member_sends(1));
}

/// The reader's room list: their 100 rooms, each with its newest message and
/// their own marker, until published.
#[divan::bench(args = [INBOX_ROOMS], sample_count = 3, sample_size = 1)]
fn band_chat_inbox_open(bencher: divan::Bencher<'_, '_>, _rooms: usize) {
    let room = band();
    bencher
        .with_inputs(|| room.settle())
        .bench_local_values(|()| room.open_inbox());
}

/// The room list is open; a message lands in one of the reader's rooms and
/// becomes that room's newest.
#[divan::bench(args = [INBOX_ROOMS], sample_count = 10, sample_size = 1)]
fn band_chat_inbox_new_message(bencher: divan::Bencher<'_, '_>, _rooms: usize) {
    let mut inbox = LiveInbox::new(band());
    bencher.bench_local(|| inbox.message_lands());
}

/// The reader's unread count for the deep room: messages from others after
/// their marker, capped at 100. Five are unread.
#[divan::bench(args = [DEEP], sample_count = 3, sample_size = 1)]
fn band_chat_unread_count_deep(bencher: divan::Bencher<'_, '_>, _deep: usize) {
    let room = band();
    bencher
        .with_inputs(|| room.settle())
        .bench_local_values(|()| room.open_unread_count());
}

/// The unread count is live; a bandmate's message arrives and the count goes
/// up.
#[divan::bench(args = [DEEP], sample_count = 10, sample_size = 1)]
fn band_chat_unread_count_new_message(bencher: divan::Bencher<'_, '_>, _deep: usize) {
    let mut count = LiveUnreadCount::new(band());
    bencher.bench_local(|| count.message_arrives());
}

/// All 50 members have the deep room open with everyone's markers (check
/// marks). One reads to the newest message: the marker moves and is
/// journaled in one transaction, and every open view shows it.
#[divan::bench(args = [DEEP_MEMBERS], sample_count = 10, sample_size = 1)]
fn band_chat_marker_move_fanout(bencher: divan::Bencher<'_, '_>, open_by: usize) {
    let mut fanout = ReceiptsFanout::new(band(), open_by);
    bencher.bench_local(|| fanout.member_reads());
}

/// "Read by" on a recent message in the 50-member deep room: members whose
/// read journal reaches it, with the first entry that did, one read.
#[divan::bench(args = [DEEP_MEMBERS], sample_count = 3, sample_size = 1)]
fn band_chat_read_by_sheet(bencher: divan::Bencher<'_, '_>, _members: usize) {
    let room = band();
    bencher
        .with_inputs(|| room.settle())
        .bench_local_values(|()| room.read_by_sheet());
}
