use jazz_example_world_tour_benchmark::Fixture;

#[test]
fn member_and_public_calendar_queries_keep_their_actual_semantics() {
    // Each fixture has 22 stops inside the three-week window. Both app query
    // variants must retain their 12-row cap; the public variant also excludes
    // tentative/cancelled stops before applying its cap.
    for stop_count in [128, 512] {
        let fixture = Fixture::new(stop_count);
        let expected_member_start_times = (0..12)
            .map(|day| 1_700_000_000 + day * 86_400)
            .collect::<Vec<_>>();
        assert_eq!(fixture.member_calendar_window_count(), 12);
        assert_eq!(
            fixture.member_calendar_window_start_times(),
            expected_member_start_times
        );
        assert_eq!(
            fixture.member_calendar_window_venue_names(),
            (1..=12)
                .map(|venue| format!("Venue {venue:05}"))
                .collect::<Vec<_>>()
        );

        let expected_public_stops = [1_u64, 2, 3, 4, 6, 8, 9, 11, 12, 13, 16, 17];
        let expected_public_start_times = expected_public_stops
            .iter()
            .map(|stop| 1_700_000_000 + (stop - 1) * 86_400)
            .collect::<Vec<_>>();
        assert_eq!(fixture.public_calendar_window_count(), 12);
        assert_eq!(
            fixture.public_calendar_window_start_times(),
            expected_public_start_times
        );
        assert_eq!(
            fixture.public_calendar_window_venue_names(),
            expected_public_stops
                .iter()
                .map(|stop| format!("Venue {stop:05}"))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn member_calendar_keeps_both_inclusive_date_bounds() {
    let fixture = Fixture::boundary_receipt();
    assert_eq!(
        fixture.member_calendar_window_start_times(),
        [
            1_700_000_000,
            1_700_000_000 + 86_400,
            1_700_000_000 + 21 * 86_400,
        ]
    );
}

#[test]
fn public_calendar_keeps_both_inclusive_date_bounds() {
    let fixture = Fixture::boundary_receipt();
    assert_eq!(
        fixture.public_calendar_window_start_times(),
        [
            1_700_000_000,
            1_700_000_000 + 86_400,
            1_700_000_000 + 21 * 86_400,
        ]
    );
}
