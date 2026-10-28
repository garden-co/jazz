use jazz_example_band_book_benchmark::contention::{ContentionFixture, LIST};

#[test]
fn contention_requests_hold_their_oracles_with_and_without_browsing() {
    let mut fixture = ContentionFixture::new(1_000);
    assert_eq!(fixture.open_own_pages(), LIST);
    fixture.check_off_page();

    let browsing = fixture.start_browsing();
    for _ in 0..3 {
        assert_eq!(fixture.open_own_pages(), LIST);
        fixture.check_off_page();
    }
    assert!(
        !browsing.stop().is_empty(),
        "workspace opens ran alongside the requests"
    );
}
