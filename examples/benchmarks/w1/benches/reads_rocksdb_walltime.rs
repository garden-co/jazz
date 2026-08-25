use jazz_example_w1_benchmark::Fixture;
use jazz_storage_rocksdb::RocksDbStorage;

fn main() {
    divan::main();
}

/// Persistent-backend W1 receipt comparable in shape to the native adopter run.
#[divan::bench(sample_count = 10)]
fn query_board_profile_s_rocksdb(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = Fixture::<RocksDbStorage>::rocksdb_profile_s();
    bencher.bench_local(|| fixture.board_count());
}

/// The two RocksDB-backed one-shot reads in W1 task detail.
#[divan::bench(sample_count = 5)]
fn query_task_detail_profile_s_rocksdb(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = Fixture::<RocksDbStorage>::rocksdb_profile_s();
    bencher.bench_local(|| fixture.task_detail_count());
}

/// Fixed-result RocksDB scaling receipt with the same fixture as the memory lane.
#[divan::bench(args = [(300, 1_200, 900), (3_000, 12_000, 9_000)], sample_count = 5)]
fn query_comments_scaling_rocksdb(
    bencher: divan::Bencher<'_, '_>,
    (tasks, comments, activity): (usize, usize, usize),
) {
    let (_dir, fixture) = Fixture::<RocksDbStorage>::rocksdb(tasks, comments, activity);
    bencher.bench_local(|| fixture.comments_count());
}
