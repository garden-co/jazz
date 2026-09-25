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

#[test]
fn opening_the_library_and_a_playlist_delivers_bounded_windows() {
    let fixture = Fixture::new(4096);
    // 20 CoverFlow albums + 8 metadata-only tracks of the focused album.
    assert_eq!(fixture.open_coverflow(), 28);
    assert_eq!(fixture.open_playlist(), 16);
}

#[test]
fn additions_inside_the_live_playlist_window_are_shown() {
    let fixture = Fixture::new(128);
    let mut live = fixture.live_playlist();
    for _ in 0..3 {
        assert_eq!(fixture.add_to_playlist(&mut live), 1);
    }
    assert_eq!(fixture.open_playlist(), 16);
}
