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

/// A signed-in member sends messages into an open room; each passes the
/// claim-gated insert policy and reaches the open subscription. The 10k case
/// sends 10 rather than 100: every send's room update scales with the retained
/// history (#2086), and the workload must fit its 20-minute macro job.
#[divan::bench(args = [1_000, 10_000], sample_count = 10, sample_size = 1)]
fn auth_chat_send(bencher: divan::Bencher<'_, '_>, room_messages: usize) {
    let sends = sends_for(room_messages);
    bencher
        .with_inputs(|| SendFixture::new(room_messages))
        .bench_local_refs(|fixture| fixture.send_messages(sends));
}

fn sends_for(room_messages: usize) -> usize {
    if room_messages >= 10_000 { 10 } else { 100 }
}
