use jazz_example_epic_drop_benchmark::{Fixture, expected_range};

#[test]
fn streamed_file_lists_as_one_metadata_row_and_returns_only_requested_range() {
    let file_bytes = 256 * 1024;
    let fixture = Fixture::new(file_bytes);
    assert_eq!(fixture.list_folder(), 1);
    assert_eq!(fixture.download_middle_range(), expected_range(file_bytes));
}
