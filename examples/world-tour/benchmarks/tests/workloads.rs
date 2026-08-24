use jazz_example_world_tour_benchmark::Fixture;

#[test]
fn calendar_and_map_queries_keep_their_bounds() {
    let fixture = Fixture::new(128);
    let expected_start_times = (0..12)
        .map(|day| 1_700_000_000 + day * 86_400)
        .collect::<Vec<_>>();
    assert_eq!(fixture.calendar_window_count(), 12);
    assert_eq!(fixture.calendar_window_start_times(), expected_start_times);
    assert!(fixture.map_viewport_count() > 0);
    assert!(fixture.map_viewport_count() <= 100);
}
