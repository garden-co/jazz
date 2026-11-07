//! Wall-clock receipts for what a DocShelf writer notices.

use jazz_example_doc_shelf_benchmark::{Fixture, SyncFixture};

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Opening the shelf: the first 50 titles, bodies not projected.
#[divan::bench(sample_count = 50)]
fn doc_shelf_open_shelf(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.open_shelf()));
}

/// Searching every body for a phrase; only inline bodies are searched.
#[divan::bench(sample_count = 20)]
fn doc_shelf_search_bodies(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.search()));
}

/// Finding the most repeated body (a pasted 230 KiB document) by grouping on
/// the body cell, then reading that body back.
#[divan::bench(sample_count = 50)]
fn doc_shelf_find_duplicates(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.find_duplicates()));
}

/// The first 20 documents ordered by body: chunked bodies sort first.
#[divan::bench(sample_count = 50)]
fn doc_shelf_first_by_body(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.first_by_body()));
}

/// Typing a word at the end of an open long document until the editor shows it.
#[divan::bench(sample_count = 50)]
fn doc_shelf_type_word(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    let mut editor = fixture.editor();
    bencher.bench_local(|| divan::black_box(fixture.type_word(&mut editor)));
}

/// Importing a 1 MiB document.
#[divan::bench(sample_count = 20)]
fn doc_shelf_import_1mb(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.import()));
}

/// A new device syncing the whole shelf (about 12 MB of bodies).
#[divan::bench(sample_count = 10)]
fn doc_shelf_sync_new_device(bencher: divan::Bencher<'_, '_>) {
    let fixture = SyncFixture::new();
    bencher
        .with_inputs(|| fixture.device())
        .bench_local_values(|device| divan::black_box(fixture.sync(device)));
}
