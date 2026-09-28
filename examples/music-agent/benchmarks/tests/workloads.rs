use jazz_example_music_agent_benchmark::Fixture;

#[test]
fn append_range_materialization_and_restart_keep_one_logical_transcript() {
    let fixture = Fixture::new();
    assert_eq!(
        fixture.attachment_range(),
        (64..128_usize)
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>()
    );
    let before = fixture.materialized_transcript();
    assert_eq!(before.len(), 2);
    fixture.append_assistant_tail();
    let after = fixture.materialized_transcript();
    assert!(after[1].ends_with('!'));
    assert_eq!(fixture.restarted_transcript(), after);
}

#[test]
fn long_conversation_streams_seeks_and_survives_restart() {
    let fixture = Fixture::with_shape(40, 512 * 1024);
    let before = fixture.materialized_transcript();
    assert_eq!(before.len(), 40);
    assert_eq!(before[0], "warm saxophone");
    assert!(before[39].ends_with("final chorus"));

    assert_eq!(fixture.stream_reply(50, 24), 50);
    let after = fixture.materialized_transcript();
    assert_eq!(after[39].len(), before[39].len() + 50 * 24);
    assert_eq!(fixture.restarted_transcript(), after);

    assert_eq!(
        fixture.attachment_seek(),
        fixture.expected_attachment_seek()
    );
}
