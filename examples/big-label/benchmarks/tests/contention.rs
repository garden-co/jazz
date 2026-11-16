use jazz_example_big_label_benchmark::contention::{ContentionFixture, PAGE};

#[test]
fn contention_requests_hold_their_oracles_with_and_without_the_search() {
    let mut fixture = ContentionFixture::new(1_000);
    assert!(
        fixture.matched_search_count() > 0,
        "the searched catalogue is readable"
    );

    assert_eq!(fixture.open_own_releases(), PAGE);
    fixture.rename_release();

    let search = fixture.start_searching();
    for _ in 0..3 {
        assert_eq!(fixture.open_own_releases(), PAGE);
        fixture.rename_release();
    }
    assert!(
        !search.stop().is_empty(),
        "searches ran alongside the requests"
    );
}
