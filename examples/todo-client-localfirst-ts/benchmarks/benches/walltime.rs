use jazz_example_todo_benchmark::Fixture;
#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;
struct CheckedFixture(Fixture<jazz_storage_rocksdb::RocksDbStorage>);
impl Drop for CheckedFixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.0.verify();
        }
    }
}
fn main() {
    divan::main();
}
#[divan::bench(sample_count = 5, sample_size = 1)]
fn reopen_1500_rocksdb(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedFixture(Fixture::rocksdb(1500)))
        .bench_local_refs(|f| f.0.reopen());
}
#[divan::bench(sample_count = 5, sample_size = 1)]
fn batch_update_1350_rocksdb(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedFixture(Fixture::loaded(1500)))
        .bench_local_refs(|f| f.0.batch_update(1350));
}
#[divan::bench(sample_count = 3, sample_size = 1)]
fn sequential_update_1350_rocksdb(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedFixture(Fixture::loaded(1500)))
        .bench_local_refs(|f| f.0.sequential_update(1350));
}
