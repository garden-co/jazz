use jazz_example_record_player_benchmark::Fixture;
#[test]
fn catalogue_and_ordered_playlist_are_bounded() {
    let fixture = Fixture::new(128);
    assert_eq!(fixture.coverflow_count(), 16);
    assert_eq!(fixture.playlist_window_count(), 16);
}
