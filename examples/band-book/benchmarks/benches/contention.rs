use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::Duration;

use jazz_example_band_book_benchmark::contention::{ContentionFixture, WORKSPACE_PAGES};

// One band's workspace opens against another band's cheap requests, on one
// server (see src/contention.rs). Each request is timed idle and while the
// workspace opens back to back; the difference is the contention. Measured
// by the nightly CodSpeed run only.

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Seeding and starting the server take longer than every measurement, so
/// all four cases share one server.
static FIXTURE: LazyLock<Mutex<ContentionFixture>> =
    LazyLock::new(|| Mutex::new(ContentionFixture::new(WORKSPACE_PAGES)));

fn fixture() -> MutexGuard<'static, ContentionFixture> {
    FIXTURE.lock().expect("fixture is not poisoned")
}

fn report(case: &str, opens: Vec<Duration>) {
    assert!(
        !opens.is_empty(),
        "{case}: no workspace open overlapped the samples"
    );
    let mut opens = opens;
    opens.sort();
    eprintln!(
        "{case}: {} workspace opens overlapped, median {:?}",
        opens.len(),
        opens[opens.len() / 2]
    );
}

/// A member opens their newest pages, server otherwise idle.
#[divan::bench(sample_count = 40)]
fn band_book_open_my_pages_idle(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher.bench_local(|| divan::black_box(fixture.open_own_pages()));
}

/// The same, while another band's member opens their big workspace.
#[divan::bench(sample_count = 40)]
fn band_book_open_my_pages_during_workspace_open(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    let browsing = fixture.start_browsing();
    bencher.bench_local(|| divan::black_box(fixture.open_own_pages()));
    report(
        "band_book_open_my_pages_during_workspace_open",
        browsing.stop(),
    );
}

/// A member checks off a page until the server accepts it, server otherwise
/// idle.
#[divan::bench(sample_count = 40)]
fn band_book_check_off_page_idle(bencher: divan::Bencher<'_, '_>) {
    let mut fixture = fixture();
    bencher.bench_local(|| fixture.check_off_page());
}

/// The same, while another band's member opens their big workspace.
#[divan::bench(sample_count = 40)]
fn band_book_check_off_page_during_workspace_open(bencher: divan::Bencher<'_, '_>) {
    let mut fixture = fixture();
    let browsing = fixture.start_browsing();
    bencher.bench_local(|| fixture.check_off_page());
    report(
        "band_book_check_off_page_during_workspace_open",
        browsing.stop(),
    );
}
