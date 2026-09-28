use jazz_example_chat_benchmark::{OpenFixture, PAGE, SendFixture, outsider};

#[test]
fn member_opens_the_newest_page_of_a_private_chat() {
    for messages in [40, 1_000] {
        let fixture = OpenFixture::new(messages);
        assert_eq!(fixture.open_chat().1, PAGE.min(messages.div_ceil(4)));
        assert_eq!(fixture.open_chat_as(outsider()).1, 0);
    }
}

#[test]
fn member_sends_are_policy_checked_and_shown_in_the_open_chat() {
    let mut fixture = SendFixture::new(1_000);
    assert_eq!(fixture.shown, PAGE);
    assert_eq!(fixture.send_messages(10), 10);
    assert_eq!(fixture.shown, PAGE + 10);
    assert!(fixture.non_member_send_is_denied());
}
