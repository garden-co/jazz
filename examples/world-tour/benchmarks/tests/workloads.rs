use jazz_example_world_tour_benchmark::Fixture;

#[test]
fn calendar_and_map_queries_keep_their_bounds() {
    let fixture = Fixture::new(128);
    assert_eq!(fixture.calendar_window_count(), 10);
    assert!(fixture.map_viewport_count() > 0);
    assert!(fixture.map_viewport_count() <= 100);
}
