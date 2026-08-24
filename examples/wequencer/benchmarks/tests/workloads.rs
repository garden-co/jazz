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
    assert_eq!(fixture.playhead_window(12, 16).len(), 16);
}

#[test]
fn edit_burst_preserves_a_readable_converged_pattern() {
    let fixture = Fixture::new();
    assert!(fixture.concurrent_edit_burst(32) <= STEPS);
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
