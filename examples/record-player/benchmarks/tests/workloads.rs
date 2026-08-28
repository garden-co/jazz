use jazz_example_record_player_benchmark::Fixture;
#[test]
fn catalogue_and_ordered_playlist_are_bounded() {
    let fixture = Fixture::new(128);
    assert_eq!(fixture.coverflow_count(), 16);
    assert_eq!(fixture.track_metadata_count(), 8);
    assert_eq!(
        fixture.track_metadata_projection(),
        ["album_id", "duration_ms", "ordinal", "title"]
    );
    assert_eq!(fixture.playlist_window_count(), 16);
    let documented_access_paths = serde_json::from_str(include_str!("../../access-paths.json"))
        .expect("RecordPlayer access-path manifest is valid JSON");
    assert_eq!(Fixture::indexed_access_paths(), documented_access_paths);
}
