//! BandBook wall-clock suite, measured on CodSpeed's macro runner: what
//! row-level security costs on an ordered page list, and what merging two
//! people's edits to the same block of text costs. Names are app-prefixed
//! because the examples page matches results by exact name; `metadata.ts`
//! documents each timed iteration.
//!
//! BandBook is a Notion-style workspace: members own pages, belong to
//! workspaces, and may read their own pages plus those of workspaces they were
//! admitted to. The fixture is the former policy-scoped documents workload
//! (documents = pages, organizations = workspaces, owners = members).

use jazz_example_band_book_benchmark::text_merge;
use jazz_example_band_book_benchmark::{Fixture, Page, Policy, QUERY_OWNER, user};

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

fn page(bencher: divan::Bencher, rows: usize, policy: Policy, page: Page) {
    let fixture = Fixture::new(rows, policy);
    bencher
        .with_inputs(|| fixture.session(page, 50, user(QUERY_OWNER)))
        .bench_local_refs(|session| divan::black_box(session.read()));
}

/// A workspace's newest 50 pages with an explicit allow-all policy: the
/// baseline the permissioned list is compared against.
#[divan::bench(args = [100_000], sample_count = 3, sample_size = 1)]
fn band_book_workspace_pages_unrestricted(bencher: divan::Bencher, rows: usize) {
    page(bencher, rows, Policy::Unrestricted, Page::Org(0));
}

/// The same workspace page list under "own pages OR admitted workspace".
#[divan::bench(args = [100_000], sample_count = 3, sample_size = 1)]
fn band_book_workspace_pages(bencher: divan::Bencher, rows: usize) {
    page(bencher, rows, Policy::OwnerOrOrg, Page::Org(0));
}

/// "My pages" under the owner-only policy.
#[divan::bench(args = [100_000], sample_count = 3, sample_size = 1)]
fn band_book_my_pages(bencher: divan::Bencher, rows: usize) {
    page(bencher, rows, Policy::Owner, Page::Owner(QUERY_OWNER));
}

/// Open the permissioned workspace page list live, through its first result.
#[divan::bench(args = [100_000], sample_count = 3, sample_size = 1)]
fn band_book_workspace_pages_live(bencher: divan::Bencher, rows: usize) {
    let fixture = Fixture::new(rows, Policy::OwnerOrOrg);
    bencher
        .with_inputs(|| fixture.session(Page::Org(0), 50, user(QUERY_OWNER)))
        .bench_local_refs(|session| divan::black_box(session.subscribe()));
}

/// A device reconnects with an edit to a block nobody else touched: the
/// authority applies the write as is (the text merge fast path).
#[divan::bench(sample_count = 10, sample_size = 1)]
fn band_book_text_fast_path(bencher: divan::Bencher) {
    bencher
        .with_inputs(text_merge::fast_path)
        .bench_local_values(|reconnect| divan::black_box(reconnect.run()));
}

/// Two devices edited the same paragraph offline; the second to reconnect
/// has its write merged with the first.
#[divan::bench(sample_count = 10, sample_size = 1)]
fn band_book_text_concurrent_paragraph(bencher: divan::Bencher) {
    bencher
        .with_inputs(text_merge::concurrent_paragraph)
        .bench_local_values(|reconnect| divan::black_box(reconnect.run()));
}
