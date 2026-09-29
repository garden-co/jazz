use jazz_example_auth_chat_benchmark::{OpenFixture, SendFixture, guest};

#[test]
fn member_opens_exactly_the_general_room() {
    for room_messages in [10, 1_000] {
        let fixture = OpenFixture::new(room_messages);
        assert_eq!(fixture.open_room().1, room_messages);
        assert_eq!(fixture.open_room_as(guest()).1, 0);
    }
}

#[test]
fn member_sends_are_policy_checked_and_delivered_to_the_open_room() {
    let mut fixture = SendFixture::new(100);
    assert_eq!(fixture.visible, 100);
    assert_eq!(fixture.send_messages(25), 25);
    assert_eq!(fixture.visible, 125);
    assert!(fixture.member_announcement_is_denied());
}
