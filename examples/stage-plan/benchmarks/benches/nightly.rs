//! StagePlan board scaling sweeps measured by the nightly CodSpeed run on main,
//! not on every merge. Each keeps its former W1 function name, so its CodSpeed
//! history continues. The per-merge suite (`walltime`) measures one point of
//! each area; these extra points explain how a cost scales.
//!
//! The `_sqlite` twins repeat the per-merge storage-bound cases on SQLite, the
//! durable store of the native relay behind React Native, Swift and Kotlin
//! (the realistic suite's former SQLite engine runs). Run locally with:
//!
//! ```sh
//! cargo bench -p jazz-example-stage-plan-benchmark --bench nightly
//! ```

use jazz::groove::storage::MemoryStorage;
use jazz_example_stage_plan_benchmark::board::Fixture;
use jazz_example_stage_plan_benchmark::tasks::Fixture as TaskListFixture;
use jazz_storage_rocksdb::RocksDbStorage;
use jazz_storage_sqlite::SqliteStorage;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Two indexed equalities and LIMIT 50 over the profile-S fixture.
#[divan::bench(sample_count = 5)]
fn query_bounded_activity_page_profile_s_rocksdb(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = Fixture::<RocksDbStorage>::rocksdb_profile_s();
    bencher.bench_local(|| fixture.bounded_activity_page_count());
}

/// The smaller point of the page-cost sweep (CodSpeed measures 30,000).
#[divan::bench(args = [9_000], sample_count = 1)]
fn query_bounded_activity_page_scaling_rocksdb(
    bencher: divan::Bencher<'_, '_>,
    activity_events: usize,
) {
    let (_dir, fixture) = Fixture::<RocksDbStorage>::rocksdb(3_000, 12_000, activity_events);
    bencher.bench_local(|| fixture.bounded_activity_page_count());
}

#[divan::bench(args = [9_000, 30_000], sample_count = 1)]
fn query_bounded_activity_page_scaling_memory(
    bencher: divan::Bencher<'_, '_>,
    activity_events: usize,
) {
    let fixture = Fixture::<MemoryStorage>::memory(3_000, 12_000, activity_events);
    bencher.bench_local(|| fixture.bounded_activity_page_count());
}

/// Fixed-result comments read while the surrounding tables scale.
#[divan::bench(args = [(300, 1_200, 900), (3_000, 12_000, 9_000)], sample_count = 5)]
fn query_comments_scaling_rocksdb(
    bencher: divan::Bencher<'_, '_>,
    (tasks, comments, activity): (usize, usize, usize),
) {
    let (_dir, fixture) = Fixture::<RocksDbStorage>::rocksdb(tasks, comments, activity);
    bencher.bench_local(|| fixture.comments_count());
}

#[divan::bench(args = [(300, 1_200, 900), (3_000, 12_000, 9_000)], sample_count = 5)]
fn query_comments_scaling_memory(
    bencher: divan::Bencher<'_, '_>,
    (tasks, comments, activity): (usize, usize, usize),
) {
    let fixture = Fixture::<MemoryStorage>::memory(tasks, comments, activity);
    bencher.bench_local(|| fixture.comments_count());
}

/// Indexed-field update with no live subscription attached.
#[divan::bench(sample_count = 10)]
fn update_activity_indexed_predicate_no_subscription_rocksdb(bencher: divan::Bencher<'_, '_>) {
    let (_dir, mut fixture) = Fixture::<RocksDbStorage>::rocksdb_profile_s();
    bencher.bench_local(|| fixture.toggle_activity_indexed_predicate());
}

#[divan::bench(sample_count = 10)]
fn update_activity_indexed_predicate_no_subscription_memory(bencher: divan::Bencher<'_, '_>) {
    let mut fixture = Fixture::<MemoryStorage>::memory_profile_s();
    bencher.bench_local(|| fixture.toggle_activity_indexed_predicate());
}

/// Point subscription attach without a policy, both table sizes.
#[divan::bench(args = [900, 9_000], sample_count = 1)]
fn subscribe_activity_point_scaling_memory(
    bencher: divan::Bencher<'_, '_>,
    activity_events: usize,
) {
    bencher
        .with_inputs(|| Fixture::<MemoryStorage>::memory(300, 1_200, activity_events))
        .bench_local_values(|fixture| fixture.subscribe_point_activity_once());
}

/// The smaller point of the policy attach sweep (CodSpeed measures 9,000).
#[divan::bench(args = [900], sample_count = 1)]
fn subscribe_activity_policy_point_scaling_memory(
    bencher: divan::Bencher<'_, '_>,
    activity_events: usize,
) {
    bencher
        .with_inputs(|| Fixture::<MemoryStorage>::memory_policy_update(300, 1_200, activity_events))
        .bench_local_values(|fixture| fixture.subscribe_point_activity_once());
}

#[divan::bench(sample_count = 5)]
fn resubscribe_activity_point_profile_s_memory(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::<MemoryStorage>::memory(300, 1_200, 9_000);
    assert_eq!(fixture.subscribe_point_activity_once(), 1);
    bencher.bench_local(|| fixture.subscribe_point_activity_once());
}

// --- SQLite twins of the per-merge storage-bound cases ---

/// Verifies exact task IDs and completion when Divan drops it, outside timing.
struct CheckedTaskList(TaskListFixture<SqliteStorage>);
impl Drop for CheckedTaskList {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.0.verify();
        }
    }
}

/// `stage_plan_add_task_1350` with a SQLite worker.
#[divan::bench(sample_count = 3, sample_size = 1)]
fn stage_plan_add_task_1350_sqlite(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedTaskList(TaskListFixture::loaded_sqlite(150)))
        .bench_local_refs(|f| f.0.sequential_insert(1350));
}

/// `stage_plan_check_off_task_1350` with a SQLite worker.
#[divan::bench(sample_count = 3, sample_size = 1)]
fn stage_plan_check_off_task_1350_sqlite(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedTaskList(TaskListFixture::loaded_sqlite(1500)))
        .bench_local_refs(|f| f.0.sequential_update(1350));
}

/// `stage_plan_bulk_complete_1350` with a SQLite worker.
#[divan::bench(sample_count = 5, sample_size = 1)]
fn stage_plan_bulk_complete_1350_sqlite(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedTaskList(TaskListFixture::loaded_sqlite(1500)))
        .bench_local_refs(|f| f.0.batch_update(1350));
}

/// `stage_plan_reopen_1500` over a SQLite worker.
#[divan::bench(sample_count = 5, sample_size = 1)]
fn stage_plan_reopen_1500_sqlite(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedTaskList(TaskListFixture::sqlite(1500)))
        .bench_local_refs(|f| f.0.reopen());
}

/// `stage_plan_open_board` over SQLite.
#[divan::bench(sample_count = 10)]
fn stage_plan_open_board_sqlite(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = Fixture::<SqliteStorage>::sqlite_profile_s();
    bencher.bench_local(|| fixture.board_count());
}

/// `stage_plan_open_task_detail` over SQLite.
#[divan::bench(sample_count = 5)]
fn stage_plan_open_task_detail_sqlite(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = Fixture::<SqliteStorage>::sqlite_profile_s();
    bencher.bench_local(|| fixture.task_detail_count());
}

/// `stage_plan_activity_page` over SQLite.
#[divan::bench(args = [30_000], sample_count = 1)]
fn stage_plan_activity_page_sqlite(bencher: divan::Bencher<'_, '_>, activity_events: usize) {
    let (_dir, fixture) = Fixture::<SqliteStorage>::sqlite(3_000, 12_000, activity_events);
    bencher.bench_local(|| fixture.bounded_activity_page_count());
}

/// `stage_plan_move_card_to_done` over SQLite.
#[divan::bench(sample_count = 10)]
fn stage_plan_move_card_to_done_sqlite(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = Fixture::<SqliteStorage>::sqlite_profile_s();
    let mut fixture = fixture.into_maintained_activity();
    bencher.bench_local(|| fixture.toggle_indexed_predicate());
}
