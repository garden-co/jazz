use jazz_example_policy_scoped_documents_benchmark::{Fixture, Page, Policy, QUERY_OWNER, user};

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

// Historical policy_free IDs retain the unrestricted-read comparison series.
// The fixture now expresses that permission with an explicit allow-all policy.
#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn policy_free_owner_page50(bencher: divan::Bencher, rows: usize) {
    page(
        bencher,
        rows,
        Policy::Unrestricted,
        Page::Owner(QUERY_OWNER),
    );
}

#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn owner_policy_page50(bencher: divan::Bencher, rows: usize) {
    page(bencher, rows, Policy::Owner, Page::Owner(QUERY_OWNER));
}

#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn owner_or_org_policy_owner_page50(bencher: divan::Bencher, rows: usize) {
    page(bencher, rows, Policy::OwnerOrOrg, Page::Owner(QUERY_OWNER));
}

#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn policy_free_org_page50(bencher: divan::Bencher, rows: usize) {
    page(bencher, rows, Policy::Unrestricted, Page::Org(0));
}

#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn owner_or_org_policy_org_page50(bencher: divan::Bencher, rows: usize) {
    page(bencher, rows, Policy::OwnerOrOrg, Page::Org(0));
}

fn subscribe_page(bencher: divan::Bencher, rows: usize, policy: Policy, page: Page) {
    let fixture = Fixture::new(rows, policy);
    bencher
        .with_inputs(|| fixture.session(page, 50, user(QUERY_OWNER)))
        .bench_local_refs(|session| divan::black_box(session.subscribe()));
}

#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn subscribe_policy_free_owner_page50(bencher: divan::Bencher, rows: usize) {
    subscribe_page(
        bencher,
        rows,
        Policy::Unrestricted,
        Page::Owner(QUERY_OWNER),
    );
}

#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn subscribe_owner_policy_page50(bencher: divan::Bencher, rows: usize) {
    subscribe_page(bencher, rows, Policy::Owner, Page::Owner(QUERY_OWNER));
}

#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn subscribe_owner_or_org_policy_owner_page50(bencher: divan::Bencher, rows: usize) {
    subscribe_page(bencher, rows, Policy::OwnerOrOrg, Page::Owner(QUERY_OWNER));
}

#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn subscribe_policy_free_org_page50(bencher: divan::Bencher, rows: usize) {
    subscribe_page(bencher, rows, Policy::Unrestricted, Page::Org(0));
}

#[divan::bench(args = [10_000, 100_000], sample_count = 3, sample_size = 1)]
fn subscribe_owner_or_org_policy_org_page50(bencher: divan::Bencher, rows: usize) {
    subscribe_page(bencher, rows, Policy::OwnerOrOrg, Page::Org(0));
}
