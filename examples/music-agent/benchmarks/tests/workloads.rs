use jazz_example_music_agent_benchmark::Fixture;

#[test]
fn append_range_materialization_and_restart_keep_one_logical_transcript() {
    let fixture = Fixture::new();
    assert_eq!(fixture.attachment_range(), vec![7; 64]);
    let before = fixture.materialized_transcript();
    assert_eq!(before.len(), 2);
    fixture.append_assistant_tail();
    let after = fixture.materialized_transcript();
    assert!(after[1].ends_with('!'));
    assert_eq!(fixture.restarted_transcript(), after);
}
