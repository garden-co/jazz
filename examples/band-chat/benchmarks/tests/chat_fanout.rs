//! The fan-out cases wake what their fix promises, and what they would wake
//! without it: each case is a regression receipt for one fan-out behaviour.
//! The timed operations assert their own deltas (which results changed).

use jazz::db::{set_correlation_filter_for_test, set_policy_pins_for_test};
use jazz_example_band_chat_benchmark::chat_fanout::{Access, CHATS_PER_MEMBER, ChatFixture};

/// Fewer members than the benchmark, so a debug build stays quick; every
/// chat still has five members.
const MEMBERS: usize = 50;
const WRITES: u64 = 4;

/// Groups woken per timed write, with correlation watches and policy pins
/// as given.
fn woken(correlate: bool, pins: bool, case: impl FnOnce() -> (u64, u64)) -> u64 {
    set_correlation_filter_for_test(correlate);
    set_policy_pins_for_test(pins);
    let (before, after) = case();
    set_correlation_filter_for_test(true);
    set_policy_pins_for_test(true);
    (after - before) / WRITES
}

fn read_marks() -> (u64, u64) {
    let mut lists = ChatFixture::seeded(MEMBERS, Access::Exists).open_chat_lists();
    let before = lists.fanout_stats().woken;
    for _ in 0..WRITES {
        lists.read_mark();
    }
    (before, lists.fanout_stats().woken)
}

fn joins(access: Access) -> (u64, u64) {
    let mut lists = ChatFixture::seeded(MEMBERS, access).open_chat_lists();
    let mut woken = 0;
    for _ in 0..WRITES {
        let before = lists.fanout_stats().woken;
        lists.join();
        woken += lists.fanout_stats().woken - before;
        lists.leave();
    }
    (0, woken)
}

fn replies() -> (u64, u64) {
    let mut rooms = ChatFixture::seeded(MEMBERS, Access::Exists).open_rooms(false);
    let before = rooms.fanout_stats().woken;
    for _ in 0..WRITES {
        rooms.new_message();
    }
    (before, rooms.fanout_stats().woken)
}

#[test]
fn a_read_mark_wakes_only_the_lists_holding_its_chat() {
    // The chat's five members (#5333); every list without correlation.
    assert_eq!(woken(true, true, read_marks), CHATS_PER_MEMBER as u64);
    assert_eq!(woken(false, true, read_marks), MEMBERS as u64);
}

#[test]
fn a_join_wakes_the_joiner_and_the_chats_members_under_an_exists_policy() {
    // The joiner's list and the chat's five members (#5341); every list
    // without policy pins.
    assert!(woken(true, true, || joins(Access::Exists)) <= CHATS_PER_MEMBER as u64 + 1);
    assert_eq!(woken(true, false, || joins(Access::Exists)), MEMBERS as u64);
}

#[test]
fn a_join_wakes_every_list_under_a_relation_style_policy() {
    // The gap this case tracks: when it closes, this matches the case
    // above.
    assert_eq!(
        woken(true, true, || joins(Access::Relation)),
        MEMBERS as u64
    );
}

#[test]
fn a_reply_wakes_only_its_room_despite_the_self_relation() {
    // The room keeps its `chatId` key (#5333); every room without it.
    assert_eq!(woken(true, true, replies), 1);
    assert_eq!(woken(false, true, replies), MEMBERS as u64);
}

#[test]
fn typing_shows_every_draft_and_the_message() {
    let mut rooms = ChatFixture::seeded(MEMBERS, Access::Exists).open_rooms(true);
    for _ in 0..2 {
        rooms.type_and_send();
    }
}
