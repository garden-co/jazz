use jazz_example_todo_benchmark::Fixture;
#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;
fn main() {
    divan::main();
}
#[divan::bench(sample_count = 5, sample_size = 1)]
fn reopen_1500_rocksdb(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| Fixture::rocksdb(1500))
        .bench_local_refs(|f| f.reopen());
}
#[divan::bench(sample_count = 5, sample_size = 1)]
fn batch_update_1350_rocksdb(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| Fixture::loaded(1500))
        .bench_local_refs(|f| f.batch_update(1350));
}
#[divan::bench(sample_count = 3, sample_size = 1)]
fn sequential_update_1350_rocksdb(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| Fixture::loaded(1500))
        .bench_local_refs(|f| f.sequential_update(1350));
}
