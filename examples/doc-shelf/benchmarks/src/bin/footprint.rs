//! Prints the on-disk size of a seeded DocShelf file store.

fn main() {
    let dir = tempfile::tempdir().expect("scratch directory");
    let bytes = jazz_example_doc_shelf_benchmark::file_footprint(&dir.path().join("shelf.sqlite"));
    let logical: usize = (0..jazz_example_doc_shelf_benchmark::DOCS)
        .map(|n| jazz_example_doc_shelf_benchmark::body(n).len())
        .sum::<usize>()
        + jazz_example_doc_shelf_benchmark::body(jazz_example_doc_shelf_benchmark::PASTED).len();
    println!("file store: {bytes} bytes for {logical} logical body bytes");
}
