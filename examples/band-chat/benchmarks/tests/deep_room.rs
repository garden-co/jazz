use jazz_example_band_chat_benchmark::deep_room::{
    DEEP_MEMBERS, DEEP_UNREAD, DeepRoom, JUMP_HALF, LiveInbox, LiveUnreadCount, LiveWindow, PAGE,
    ReceiptsFanout, Shape, outsider,
};

const DEEP: usize = 8_000;

/// alice (the reader) opens a room with a long history: they get the newest
/// page, a page halfway back, the window around a jump target and the newest
/// search hits. mallory, who is not a member, gets nothing.
///
/// ```
/// alice ──open / scroll / jump / search──► deep room ──policy: member──► rows
/// mallory ──open──────────────────────────► deep room ──policy──✗
/// ```
#[test]
fn member_reads_the_deep_room_at_every_depth() {
    let room = DeepRoom::seeded(Shape::miniature(DEEP));
    assert_eq!(room.open_newest_page().1, PAGE);
    assert_eq!(room.open_newest_page_as(outsider()), 0);
    assert_eq!(room.open_older_page().1, PAGE);
    let [older, newer] = room.jump_to_message();
    assert_eq!((older.1, newer.1), (JUMP_HALF, JUMP_HALF));
    assert_eq!(room.search_room(), room.shape.expected_search_hits());
}

/// alice's room list holds every room they are a member of, each with its
/// newest message and their own marker only; their unread count for the deep
/// room is the messages others sent after their marker.
#[test]
fn room_list_and_unread_count_derive_from_the_newest_message_and_the_marker() {
    let shape = Shape::miniature(DEEP);
    let room = DeepRoom::seeded(shape);
    let rows = room.inbox_rows();
    assert_eq!(rows.len(), shape.reader_rooms().len());
    assert!(rows.iter().all(|&shown| shown == (1, 1)));
    assert_eq!(room.open_inbox().1, shape.reader_rooms().len());
    assert_eq!(room.open_unread_count().1, DEEP_UNREAD);
}

/// "Read by" lists the members whose read journal reaches the message.
#[test]
fn read_by_lists_members_whose_journal_reaches_the_message() {
    let shape = Shape::miniature(DEEP);
    let room = DeepRoom::seeded(shape);
    let expected = shape.expected_read_by();
    assert!(expected > 1 && expected < DEEP_MEMBERS);
    assert_eq!(room.read_by_sheet(), expected);
}

/// bob sends into the deep room while alice has it open: each message is
/// accepted and reaches their page.
///
/// ```
/// bob ──insert──► authority ──accept──► alice's open page
/// ```
#[test]
fn a_new_message_reaches_the_open_page() {
    let mut window = LiveWindow::new(DeepRoom::seeded(Shape::miniature(DEEP)));
    assert_eq!(window.shown, PAGE);
    assert_eq!(window.member_sends(3), 3);
    assert_eq!(window.shown, PAGE);
}

/// bob's message after alice's marker raises their live unread count by one.
#[test]
fn a_new_message_raises_the_live_unread_count() {
    let mut count = LiveUnreadCount::new(DeepRoom::seeded(Shape::miniature(DEEP)));
    assert_eq!(count.count, DEEP_UNREAD);
    assert_eq!(count.message_arrives(), DEEP_UNREAD + 1);
    assert_eq!(count.message_arrives(), DEEP_UNREAD + 2);
}

/// A message in one of alice's rooms becomes that room's newest message in
/// their open room list, without anyone writing to the room.
#[test]
fn a_new_message_reaches_the_open_room_list() {
    let mut inbox = LiveInbox::new(DeepRoom::seeded(Shape::miniature(DEEP)));
    assert!(inbox.message_lands() >= 1);
    assert!(inbox.message_lands() >= 1);
}

/// Every member has the room open; when bob reads to the newest message, their
/// marker moves (with a journal row) and every open view shows it.
#[test]
fn a_marker_move_reaches_every_open_view() {
    let mut fanout = ReceiptsFanout::new(DeepRoom::seeded(Shape::miniature(DEEP)), DEEP_MEMBERS);
    assert_eq!(fanout.markers_seen, DEEP_MEMBERS);
    assert_eq!(fanout.member_reads(), DEEP_MEMBERS);
    assert_eq!(fanout.member_reads(), DEEP_MEMBERS);
}
