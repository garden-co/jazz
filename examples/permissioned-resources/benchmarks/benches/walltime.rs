use jazz_example_permissioned_resources_benchmark::{Fixture, SelectedAllocator};
#[global_allocator]
static ALLOCATOR: SelectedAllocator = SelectedAllocator;
fn main() {
    divan::main();
}
#[divan::bench(sample_count = 3, sample_size = 1)]
fn first_sync_direct_27518_file(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| Fixture::new(1.0))
        .bench_local_refs(|fixture| fixture.first_sync());
}
#[divan::bench(sample_count = 3, sample_size = 1)]
fn first_sync_local_relay_27518_file(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| Fixture::new(1.0))
        .bench_local_refs(|fixture| fixture.first_sync_via_relay());
}
#[divan::bench(sample_count = 3, sample_size = 1)]
fn first_sync_bundle_27518_file(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| Fixture::new(1.0))
        .bench_local_refs(|fixture| fixture.first_sync_via_bundle());
}
