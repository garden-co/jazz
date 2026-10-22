use jazz_example_doc_shelf_benchmark::*;

#[test]
fn reads_return_the_logical_answers() {
    let fixture = Fixture::new();
    assert_eq!(fixture.open_shelf(), 50);
    let mut found = fixture.search();
    found.sort();
    let mut want: Vec<String> = NEEDLE_DOCS.iter().map(|n| format!("Doc {n:03}")).collect();
    want.sort();
    assert_eq!(found, want);
    let mut dups = fixture.find_duplicates();
    dups.sort();
    assert_eq!(
        dups,
        [
            format!("Doc {PASTED:03}"),
            format!("Doc {PASTED:03} (copy)")
        ]
    );
    let mut bodies: Vec<(String, String)> = (0..DOCS)
        .map(|n| (body(n), format!("Doc {n:03}")))
        .chain([(body(PASTED), format!("Doc {PASTED:03} (copy)"))])
        .collect();
    bodies.sort();
    let first = fixture.first_by_body();
    assert_eq!(first.len(), 20);
    let want: Vec<String> = bodies.iter().take(20).map(|(_, t)| t.clone()).collect();
    assert_eq!(first, want);
}

#[test]
fn typing_and_importing_reach_the_editor_and_shelf() {
    let fixture = Fixture::new();
    let mut editor = fixture.editor();
    let mut expected = body(OPEN);
    for _ in 0..5 {
        assert_eq!(fixture.type_word(&mut editor), 1);
        expected.push_str(" encore");
    }
    assert_eq!(fixture.open_body(), expected);
    assert_eq!(fixture.import(), IMPORT_BYTES);
}

#[test]
fn a_new_device_syncs_every_document() {
    let fixture = SyncFixture::new();
    let (docs, rounds, sent) = fixture.sync(fixture.device());
    assert_eq!(docs, DOCS + 1);
    // Every row's missing chunks are asked for together, one tree level per
    // round trip, not one document at a time.
    assert!(rounds <= 6, "{rounds} round trips");
    let logical: usize = (0..DOCS).map(|n| body(n).len()).sum();
    assert!(
        sent > logical / 2,
        "sent {sent} for {logical} logical bytes"
    );
}
