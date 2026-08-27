use jazz_example_benchmark_w1::Fixture;
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

/// Persistent-backend two-equality LIMIT 50 receipt for deletion-safe bounds.
#[divan::bench(sample_count = 5)]
fn query_bounded_activity_page_profile_s_rocksdb(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = Fixture::<RocksDbStorage>::rocksdb_profile_s();
    bencher.bench_local(|| fixture.bounded_activity_page_count());
}

#[divan::bench(args = [9_000, 30_000], sample_count = 1)]
fn query_bounded_activity_page_scaling_rocksdb(
    bencher: divan::Bencher<'_, '_>,
    activity_events: usize,
) {
    let (_dir, fixture) = Fixture::<RocksDbStorage>::rocksdb(3_000, 12_000, activity_events);
    bencher.bench_local(|| fixture.bounded_activity_page_count());
}

#[divan::bench(sample_count = 10)]
fn subscribe_activity_intersection_delta_rocksdb(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = Fixture::<RocksDbStorage>::rocksdb_profile_s();
    let mut fixture = fixture.into_maintained_activity();
    bencher.bench_local(|| fixture.toggle_indexed_predicate());
}

#[divan::bench(sample_count = 10)]
fn update_activity_indexed_predicate_no_subscription_rocksdb(bencher: divan::Bencher<'_, '_>) {
    let (_dir, mut fixture) = Fixture::<RocksDbStorage>::rocksdb_profile_s();
    bencher.bench_local(|| fixture.toggle_activity_indexed_predicate());
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
