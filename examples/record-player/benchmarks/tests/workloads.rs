use jazz_example_record_player_benchmark::Fixture;
#[test]
fn catalogue_and_ordered_playlist_are_bounded() {
    let fixture = Fixture::new(256);
    assert_eq!(fixture.coverflow_count(), 20);
    assert_eq!(fixture.playlist_window_count(), 16);
    assert_eq!(fixture.track_metadata_count(), 8);
    let projection = fixture.track_metadata_projection();
    assert_eq!(projection.len(), 4);
    for column in ["album_id", "title", "ordinal", "duration_ms"] {
        assert!(projection.iter().any(|selected| selected == column));
    }
    assert!(
        !fixture
            .track_metadata_projection()
            .iter()
            .any(|column| column == "audio_bytes")
    );
}

#[test]
fn native_access_paths_match_the_shared_record_player_contract() {
    let expected: std::collections::BTreeMap<String, Vec<String>> =
        serde_json::from_str(include_str!("../../access-paths.json")).unwrap();
    assert_eq!(Fixture::indexed_access_paths(), expected);
}
