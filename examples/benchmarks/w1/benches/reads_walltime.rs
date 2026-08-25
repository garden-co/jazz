use jazz_example_w1_benchmark::Fixture;

fn main() {
    divan::main();
}

/// Thesis #2026: W1 Local reads should scale with candidates/results, not tables.
#[divan::bench(sample_count = 10)]
fn query_board_profile_s(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::profile_s();
    bencher.bench_local(|| fixture.board_count());
}

/// The two one-shot reads performed by W1's task-detail operation.
#[divan::bench(sample_count = 5)]
fn query_task_detail_profile_s(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::profile_s();
    bencher.bench_local(|| fixture.task_detail_count());
}

/// Fixed-result scaling receipt exposing table-proportional hydration.
#[divan::bench(args = [(300, 1_200, 900), (3_000, 12_000, 9_000)], sample_count = 5)]
fn query_comments_scaling(
    bencher: divan::Bencher<'_, '_>,
    (tasks, comments, activity): (usize, usize, usize),
) {
    let fixture = Fixture::new(tasks, comments, activity);
    bencher.bench_local(|| fixture.comments_count());
}
