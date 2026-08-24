use jazz_example_band_binder_benchmark::Fixture;

#[test]
fn ordered_siblings_and_one_recursive_step_are_bounded() {
    let fixture = Fixture::new(128);
    assert_eq!(fixture.sibling_window_count(), 16);
    assert_eq!(fixture.child_page_count(), 16);
}
