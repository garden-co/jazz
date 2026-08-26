use jazz_example_wequencer_benchmark::{Fixture, STEPS};

#[test]
fn ordered_track_window_is_complete_and_stable() {
    let fixture = Fixture::new();
    let steps = fixture.track_steps();
    assert_eq!(steps.len(), STEPS);
    assert_eq!(
        steps
            .iter()
            .map(|(position, _)| *position)
            .collect::<Vec<_>>(),
        (0..STEPS as u64).collect::<Vec<_>>()
    );
    assert_eq!(
        fixture
            .playhead_window(12, 16)
            .into_iter()
            .map(|(position, _)| position)
            .collect::<Vec<_>>(),
        (12..28).collect::<Vec<_>>()
    );
}

#[test]
fn app_session_queries_keep_their_order_and_cardinality_contracts() {
    let fixture = Fixture::new();
    let (session_titles, track_positions, membership_count, presence_count) =
        fixture.session_browser_shape();

    assert_eq!(session_titles, ["Soundcheck", "Weekend set"]);
    assert_eq!(track_positions, (0..16).collect::<Vec<_>>());
    assert_eq!(membership_count, 1);
    assert_eq!(presence_count, 1);
}

#[test]
fn editor_edit_burst_preserves_a_readable_pattern() {
    let fixture = Fixture::new();
    let _ = fixture.editor_edit_burst(32);

    let steps = fixture.track_steps();
    let expected = (0..STEPS as u64)
        .map(|position| {
            let enabled = if position < 32 {
                position % 2 == 0
            } else {
                position % 3 == 0
            };
            (position, enabled)
        })
        .collect::<Vec<_>>();
    assert_eq!(steps, expected);
    assert_eq!(steps.iter().filter(|(_, enabled)| *enabled).count(), 27);
    assert_eq!(fixture.playhead_window(0, 1).len(), 1);
}

#[test]
fn subscribed_step_edit_delivers_a_public_subscription_event() {
    assert!(Fixture::new().subscribed_step_edit());
}

#[test]
fn one_write_reaches_every_subscribed_pattern_listener() {
    assert_eq!(Fixture::new().subscribed_step_fanout(8), 8);
}

#[test]
fn playback_receipt_is_available_through_the_ordered_session_query() {
    assert_eq!(Fixture::new().playback_receipt(), (true, 7));
}
