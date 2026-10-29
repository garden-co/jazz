use jazz_example_doc_shelf_benchmark::*;

#[test]
fn reads_return_the_logical_answers() {
    let fixture = Fixture::new();
    assert_eq!(fixture.open_shelf(), 50);
    let mut found = fixture.search();
    found.sort();
    // Only inline bodies are searched (SPEC 19.5).
    let want: Vec<String> = needle_found()
        .iter()
        .map(|n| format!("Doc {n:03}"))
        .collect();
    assert!(!want.is_empty() && want.len() < NEEDLE_DOCS.len());
    assert_eq!(found, want);
    assert_eq!(fixture.find_duplicates(), (2, body(PASTED)));
    assert!(
        fixture
            .find_by_long_literal()
            .contains("over 65536 bytes is not supported yet")
    );
    // Chunked bodies sort before every inline one, among themselves in no
    // meaningful order; there are more than 20 of them.
    let long: Vec<String> = (0..DOCS)
        .filter(|&n| chunked(n))
        .map(|n| format!("Doc {n:03}"))
        .chain([format!("Doc {PASTED:03} (copy)")])
        .collect();
    assert!(long.len() > 20);
    let mut first = fixture.first_by_body();
    assert_eq!(first.len(), 20);
    assert!(first.iter().all(|title| long.contains(title)), "{first:?}");
    first.sort();
    first.dedup();
    assert_eq!(first.len(), 20);
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
