use jazz_example_band_chat_benchmark::announcements::{AnnouncementsFixture, admin, guest, member};

#[test]
fn announcements_are_claim_gated_for_reads() {
    for room_messages in [10, 1_000] {
        let fixture = AnnouncementsFixture::new(room_messages);
        // The admin's open view holds exactly the announcements room.
        assert_eq!(fixture.visible, room_messages);
        assert_eq!(fixture.announcements_as(admin()), room_messages);
        assert_eq!(fixture.announcements_as(member()), room_messages);
        // No role claim, no messages.
        assert_eq!(fixture.announcements_as(guest()), 0);
    }
}

#[test]
fn only_the_admin_posts_and_every_post_reaches_the_open_full_history_view() {
    let mut fixture = AnnouncementsFixture::new(100);
    assert_eq!(fixture.post_announcements(25), 25);
    assert_eq!(fixture.visible, 125);
    assert_eq!(fixture.announcements_as(member()), 125);
    assert!(fixture.member_announcement_is_denied());
}
