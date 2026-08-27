use jazz::groove::storage::MemoryStorage;
use jazz_example_benchmark_w1::Fixture;

fn main() {
    divan::main();
}

/// Query-engine microbenchmark. This deliberately excludes persistent-backend cost.
#[divan::bench(sample_count = 10)]
fn query_board_profile_s_memory(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::<MemoryStorage>::memory_profile_s();
    bencher.bench_local(|| fixture.board_count());
}

/// The two one-shot reads performed by W1's task-detail operation.
#[divan::bench(sample_count = 5)]
fn query_task_detail_profile_s_memory(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::<MemoryStorage>::memory_profile_s();
    bencher.bench_local(|| fixture.task_detail_count());
}

/// Two indexed equalities and LIMIT 50; thesis #2026 requires deletion-safe bounds.
#[divan::bench(sample_count = 5)]
fn query_bounded_activity_page_profile_s_memory(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::<MemoryStorage>::memory_profile_s();
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

#[divan::bench(sample_count = 10)]
fn subscribe_activity_intersection_delta_memory(bencher: divan::Bencher<'_, '_>) {
    let mut fixture = Fixture::<MemoryStorage>::memory_profile_s().into_maintained_activity();
    bencher.bench_local(|| fixture.toggle_indexed_predicate());
}

#[divan::bench(sample_count = 10)]
fn update_activity_indexed_predicate_no_subscription_memory(bencher: divan::Bencher<'_, '_>) {
    let mut fixture = Fixture::<MemoryStorage>::memory_profile_s();
    bencher.bench_local(|| fixture.toggle_activity_indexed_predicate());
}

/// Trusted-session update with a row-dependent SELECT/UPDATE policy.
#[divan::bench(sample_count = 10)]
fn update_activity_policy_no_subscription_memory(bencher: divan::Bencher<'_, '_>) {
    let mut fixture = Fixture::<MemoryStorage>::memory_profile_s_policy_update();
    bencher.bench_local(|| fixture.toggle_activity_indexed_predicate());
}

#[divan::bench(args = [900, 9_000], sample_count = 3)]
fn update_activity_policy_scaling_memory(bencher: divan::Bencher<'_, '_>, activity_events: usize) {
    let mut fixture =
        Fixture::<MemoryStorage>::memory_policy_update(3_000, 12_000, activity_events);
    bencher.bench_local(|| fixture.toggle_activity_indexed_predicate());
}

#[divan::bench(args = [900, 9_000], sample_count = 1)]
fn subscribe_activity_point_scaling_memory(
    bencher: divan::Bencher<'_, '_>,
    activity_events: usize,
) {
    bencher
        .with_inputs(|| Fixture::<MemoryStorage>::memory(300, 1_200, activity_events))
        .bench_local_values(|fixture| fixture.subscribe_point_activity_once());
}

#[divan::bench(args = [900, 9_000], sample_count = 1)]
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

/// Fixed-result scaling receipt exposing query-engine candidate hydration.
#[divan::bench(args = [(300, 1_200, 900), (3_000, 12_000, 9_000)], sample_count = 5)]
fn query_comments_scaling_memory(
    bencher: divan::Bencher<'_, '_>,
    (tasks, comments, activity): (usize, usize, usize),
) {
    let fixture = Fixture::<MemoryStorage>::memory(tasks, comments, activity);
    bencher.bench_local(|| fixture.comments_count());
}
