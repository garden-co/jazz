use jazz_example_world_tour_benchmark::Fixture;

#[test]
fn calendar_and_map_queries_keep_their_bounds() {
    let fixture = Fixture::new(128);
    let mut expected_start_times = (0..11)
        .map(|day| 1_700_000_000 + day * 86_400)
        .collect::<Vec<_>>();
    expected_start_times.push(1_700_000_000 + 21 * 86_400);
    assert_eq!(fixture.calendar_window_count(), 12);
    assert_eq!(fixture.calendar_window_start_times(), expected_start_times);
    assert_eq!(
        fixture.calendar_window_venue_names(),
        (1..=12)
            .map(|venue| format!("Venue {venue:05}"))
            .collect::<Vec<_>>()
    );
    assert!(fixture.map_viewport_count() > 0);
    assert!(fixture.map_viewport_count() <= 100);
}
