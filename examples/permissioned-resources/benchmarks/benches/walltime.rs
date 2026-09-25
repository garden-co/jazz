use jazz_example_permissioned_resources_benchmark::{Fixture, SelectedAllocator};
#[global_allocator]
static ALLOCATOR: SelectedAllocator = SelectedAllocator;
fn main() {
    divan::main();
}
#[divan::bench(sample_count = 3, sample_size = 1)]
fn first_sync_local_relay_27518_rocksdb(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| Fixture::new(1.0))
        .bench_local_refs(|fixture| fixture.first_sync());
}

// Initial SELECTs have a different timing boundary from first sync: store
// copy/open and query preparation happen in with_inputs, before each sample.
#[divan::bench(sample_count = 3, sample_size = 1)]
fn initial_selects_39_tables_27518_rows_rocksdb(bencher: divan::Bencher) {
    let fixture = Fixture::new(1.0);
    bencher
        .with_inputs(|| fixture.initial_selects(None))
        .bench_local_refs(|sample| divan::black_box(sample.read()));
}

#[divan::bench(sample_count = 3, sample_size = 1)]
fn initial_selects_39_tables_limit100_879_rows_rocksdb(bencher: divan::Bencher) {
    let fixture = Fixture::new(1.0);
    bencher
        .with_inputs(|| fixture.initial_selects(Some(100)))
        .bench_local_refs(|sample| divan::black_box(sample.read()));
}
