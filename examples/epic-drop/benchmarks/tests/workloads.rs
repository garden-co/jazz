use jazz_example_epic_drop_benchmark::{Fixture, UploadFixture, expected_bytes, expected_range};

#[test]
fn streamed_file_lists_as_one_metadata_row_and_returns_only_requested_range() {
    let file_bytes = 256 * 1024;
    let fixture = Fixture::new(file_bytes);
    assert_eq!(fixture.list_folder(), 1);
    assert_eq!(fixture.download_middle_range(), expected_range(file_bytes));
    assert_eq!(fixture.download_file(), expected_bytes(0..file_bytes));
}

#[test]
fn folder_listing_returns_one_row_per_file() {
    let fixture = Fixture::with_files(12, 128 * 1024);
    assert_eq!(fixture.list_folder(), 12);
}

#[test]
fn upload_is_readable_once_it_returns() {
    let file_bytes = 300 * 1024;
    let fixture = UploadFixture::new();
    assert_eq!(fixture.upload(7, file_bytes), file_bytes);
    assert_eq!(
        fixture.uploaded_range(7, 100_000..100_100),
        expected_bytes(100_000..100_100)
    );
}
