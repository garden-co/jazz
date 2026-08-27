use jazz_example_band_chat_benchmark::{FastResumeFixture, Fixture, expected_counts};

#[test]
fn caught_up_fast_resume_emits_no_membership_or_version_payload() {
    for message_count in [100, 1_000] {
        let mut fixture = FastResumeFixture::new(message_count);
        let receipt = fixture.caught_up_fast_resume();
        assert!(
            receipt.is_caught_up_noop(),
            "{message_count} messages leaked a caught-up resume payload: {receipt:?}"
        );
    }
}

#[test]
fn representative_loads_have_exact_cardinality_and_order() {
    for message_count in [1024, 4096] {
        let fixture = Fixture::new(message_count);
        assert_eq!(
            (
                fixture.timeline_page_count(),
                fixture.unread_room_count(),
                fixture.author_history_count(),
            ),
            expected_counts(message_count),
        );

        assert_eq!(
            fixture.timeline_sent_at(),
            (50..=74).rev().collect::<Vec<_>>()
        );

        let room_count = message_count / 16;
        let unread_rooms = (0..room_count as u64)
            .filter(|room| room % 64 == 0)
            .rev()
            .collect::<Vec<_>>();
        assert_eq!(fixture.unread_room_activity(), unread_rooms);

        let author_messages = (0..message_count as u64)
            .filter(|message| message % 32 == 0)
            .rev()
            .collect::<Vec<_>>();
        assert_eq!(fixture.author_history_sent_at(), author_messages);
    }
}
