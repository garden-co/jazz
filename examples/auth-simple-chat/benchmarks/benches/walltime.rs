use jazz_example_auth_chat_benchmark::{OpenFixture, SendFixture};

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// A signed-in member opens the general room: claim-gated read policy plus
/// the room's full ascending history, through the first published result.
#[divan::bench(args = [1_000, 10_000], sample_count = 20, sample_size = 1)]
fn auth_chat_open_room(bencher: divan::Bencher<'_, '_>, room_messages: usize) {
    bencher
        .with_inputs(|| OpenFixture::new(room_messages))
        .bench_local_refs(|fixture| fixture.open_room());
}

/// A signed-in member sends 100 messages into an open room; each passes the
/// claim-gated insert policy and reaches the open subscription. Three samples:
/// the 10k case takes over a minute per sample on the macro runner, and the
/// whole workload must fit its 20-minute job.
#[divan::bench(args = [1_000, 10_000], sample_count = 3, sample_size = 1)]
fn auth_chat_send_100(bencher: divan::Bencher<'_, '_>, room_messages: usize) {
    bencher
        .with_inputs(|| SendFixture::new(room_messages))
        .bench_local_refs(|fixture| fixture.send_messages(100));
}
