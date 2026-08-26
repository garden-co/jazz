use jazz_example_world_tour_benchmark::Fixture;

#[test]
fn calendar_and_map_queries_keep_their_bounds() {
    // 512 has more than 100 matching venues, so this catches a removed map
    // limit without turning the correctness receipt into a benchmark-sized
    // fixture. The benchmark itself carries the 4096-stop workload.
    for stop_count in [128, 512] {
        let fixture = Fixture::new(stop_count);
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

        let expected_viewport = (0..stop_count)
            .filter(|stop| (5..=10).contains(&(stop % 20)))
            .map(|stop| format!("Venue {stop:05}"))
            .take(100)
            .collect::<Vec<_>>();
        assert_eq!(fixture.map_viewport_count(), expected_viewport.len());
        assert_eq!(fixture.map_viewport_venue_names(), expected_viewport);
    }
}
