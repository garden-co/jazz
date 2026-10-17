//! StagePlan wall-clock suite, measured on CodSpeed's macro runner. Names are
//! app-prefixed because the examples page matches results by exact name;
//! `metadata.ts` documents each timed iteration. Former names: see README.

use jazz::groove::storage::MemoryStorage;
use jazz_example_stage_plan_benchmark::board::crew_dashboard::FanoutFixture;
use jazz_example_stage_plan_benchmark::board::{Fixture as BoardFixture, ResumeFixture};
use jazz_example_stage_plan_benchmark::tasks::Fixture as TaskListFixture;
use jazz_storage_rocksdb::RocksDbStorage;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Verifies exact task IDs and completion when Divan drops it, outside timing.
struct CheckedTaskList(TaskListFixture<RocksDbStorage>);
impl Drop for CheckedTaskList {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.0.verify();
        }
    }
}

// --- One show's task list: RocksDB worker + in-memory foreground ---

/// Add 1,350 tasks one transaction at a time, each delivered back to the list.
#[divan::bench(sample_count = 3, sample_size = 1)]
fn stage_plan_add_task_1350(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedTaskList(TaskListFixture::loaded(150)))
        .bench_local_refs(|f| f.0.sequential_insert(1350));
}

/// Check off 1,350 tasks one transaction at a time.
#[divan::bench(sample_count = 3, sample_size = 1)]
fn stage_plan_check_off_task_1350(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedTaskList(TaskListFixture::loaded(1500)))
        .bench_local_refs(|f| f.0.sequential_update(1350));
}

/// Mark 1,350 tasks done in one transaction ("complete all").
#[divan::bench(sample_count = 5, sample_size = 1)]
fn stage_plan_bulk_complete_1350(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedTaskList(TaskListFixture::loaded(1500)))
        .bench_local_refs(|f| f.0.batch_update(1350));
}

/// Reopen the app over 1,500 stored tasks and show them all.
#[divan::bench(sample_count = 5, sample_size = 1)]
fn stage_plan_reopen_1500(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| CheckedTaskList(TaskListFixture::rocksdb(1500)))
        .bench_local_refs(|f| f.0.reopen());
}

// --- The show board (RocksDB): reads a board UI makes ---

/// Open one show's board: filter and order its tasks, bounded.
#[divan::bench(sample_count = 10)]
fn stage_plan_open_board(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = BoardFixture::<RocksDbStorage>::rocksdb_profile_s();
    bencher.bench_local(|| fixture.board_count());
}

/// Open a task's detail: its discussion and activity, two bounded reads.
#[divan::bench(sample_count = 5)]
fn stage_plan_open_task_detail(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = BoardFixture::<RocksDbStorage>::rocksdb_profile_s();
    bencher.bench_local(|| fixture.task_detail_count());
}

/// A 50-row activity page (two indexed equalities) from a 30,000-row log:
/// page cost should not depend on the table size (#2026).
#[divan::bench(args = [30_000], sample_count = 1)]
fn stage_plan_activity_page(bencher: divan::Bencher<'_, '_>, activity_events: usize) {
    let (_dir, fixture) = BoardFixture::<RocksDbStorage>::rocksdb(3_000, 12_000, activity_events);
    bencher.bench_local(|| fixture.bounded_activity_page_count());
}

/// Move a card in or out of a live filtered view: one indexed-field flip
/// delivered to an already-hydrated subscription.
#[divan::bench(sample_count = 10)]
fn stage_plan_move_card_to_done(bencher: divan::Bencher<'_, '_>) {
    let (_dir, fixture) = BoardFixture::<RocksDbStorage>::rocksdb_profile_s();
    let mut fixture = fixture.into_maintained_activity();
    bencher.bench_local(|| fixture.toggle_indexed_predicate());
}

// --- Crew dashboard and permissions (in-memory runtimes) ---

/// A crew member's dashboard: one overview plus 60 department lists opened at
/// once through Core, a scope-isolated relay and a foreground.
#[divan::bench(args = [(600, 60), (6000, 60)], sample_count = 3)]
fn stage_plan_crew_dashboard(bencher: divan::Bencher<'_, '_>, (rows, lists): (usize, usize)) {
    bencher
        .with_inputs(|| FanoutFixture::new(rows, lists))
        .bench_local_values(|mut fixture| {
            let receipt = fixture.hydrate();
            (fixture, receipt)
        });
}

/// Update one activity row under a row-dependent SELECT/UPDATE policy.
#[divan::bench(args = [9_000], sample_count = 3)]
fn stage_plan_update_under_policy(bencher: divan::Bencher<'_, '_>, activity_events: usize) {
    let mut fixture =
        BoardFixture::<MemoryStorage>::memory_policy_update(3_000, 12_000, activity_events);
    bencher.bench_local(|| fixture.toggle_activity_indexed_predicate());
}

/// Attach one live activity subscription under the row-dependent policy.
#[divan::bench(args = [9_000], sample_count = 1)]
fn stage_plan_subscribe_under_policy(bencher: divan::Bencher<'_, '_>, activity_events: usize) {
    bencher
        .with_inputs(|| {
            BoardFixture::<MemoryStorage>::memory_policy_update(300, 1_200, activity_events)
        })
        .bench_local_values(|fixture| fixture.subscribe_point_activity_once());
}

/// Archive (delete) one task and wait for local settlement.
#[divan::bench(args = [9_000], sample_count = 1)]
fn stage_plan_archive_task(bencher: divan::Bencher<'_, '_>, activity_events: usize) {
    bencher
        .with_inputs(|| BoardFixture::<MemoryStorage>::memory(300, 1_200, activity_events))
        .bench_local_values(|fixture| {
            fixture.delete_target_task();
            fixture
        });
}

/// Restore one archived task and wait for local settlement.
#[divan::bench(args = [9_000], sample_count = 1)]
fn stage_plan_restore_task(bencher: divan::Bencher<'_, '_>, activity_events: usize) {
    bencher
        .with_inputs(|| {
            let fixture = BoardFixture::<MemoryStorage>::memory(300, 1_200, activity_events);
            fixture.delete_target_task();
            fixture
        })
        .bench_local_values(|fixture| {
            fixture.restore_target_task();
            fixture
        });
}

/// Reconnect over the byte wire after one task changed while offline.
#[divan::bench(args = [(500, 2_000, 1_500)], sample_count = 1)]
fn stage_plan_resume_after_offline(
    bencher: divan::Bencher<'_, '_>,
    (tasks, comments, activity): (usize, usize, usize),
) {
    bencher
        .with_inputs(|| ResumeFixture::memory(tasks, comments, activity))
        .bench_local_values(|mut fixture| {
            let resume_bytes = fixture.resume_once();
            (fixture, resume_bytes)
        });
}
