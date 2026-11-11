use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::Duration;

use jazz_example_big_label_benchmark::contention::{ContentionFixture, SEARCHED_RELEASES};

// One label's back-catalogue search against another label's cheap requests,
// on one server (see src/contention.rs). Each request is timed idle and while
// the search runs back to back; the difference is the contention. Measured by
// the nightly CodSpeed run only.

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Seeding and starting the server take longer than every measurement, so
/// all four cases share one server.
static FIXTURE: LazyLock<Mutex<ContentionFixture>> =
    LazyLock::new(|| Mutex::new(ContentionFixture::new(SEARCHED_RELEASES)));

fn fixture() -> MutexGuard<'static, ContentionFixture> {
    FIXTURE.lock().expect("fixture is not poisoned")
}

fn report(case: &str, searches: Vec<Duration>) {
    assert!(
        !searches.is_empty(),
        "{case}: no search overlapped the samples"
    );
    let mut searches = searches;
    searches.sort();
    eprintln!(
        "{case}: {} searches overlapped, median {:?}",
        searches.len(),
        searches[searches.len() / 2]
    );
}

/// A staffer opens their label's newest releases, server otherwise idle.
#[divan::bench(sample_count = 40)]
fn big_label_open_releases_idle(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher.bench_local(|| divan::black_box(fixture.open_own_releases()));
}

/// The same, while another label's staffer searches its back catalogue.
#[divan::bench(sample_count = 40)]
fn big_label_open_releases_during_search(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    let search = fixture.start_searching();
    bencher.bench_local(|| divan::black_box(fixture.open_own_releases()));
    report("big_label_open_releases_during_search", search.stop());
}

/// A staffer renames a release until the server accepts it, server
/// otherwise idle.
#[divan::bench(sample_count = 40)]
fn big_label_rename_release_idle(bencher: divan::Bencher<'_, '_>) {
    let mut fixture = fixture();
    bencher.bench_local(|| fixture.rename_release());
}

/// The same, while another label's staffer searches its back catalogue.
#[divan::bench(sample_count = 40)]
fn big_label_rename_release_during_search(bencher: divan::Bencher<'_, '_>) {
    let mut fixture = fixture();
    let search = fixture.start_searching();
    bencher.bench_local(|| fixture.rename_release());
    report("big_label_rename_release_during_search", search.stop());
}
