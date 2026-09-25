use jazz_example_chat_benchmark::{OpenFixture, SendFixture};

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// A member opens a private chat: the newest 21 messages with their senders,
/// through the membership read policy, until the first published result.
#[divan::bench(args = [1_000, 10_000], sample_count = 20, sample_size = 1)]
fn chat_open_chat(bencher: divan::Bencher<'_, '_>, messages: usize) {
    bencher
        .with_inputs(|| OpenFixture::new(messages))
        .bench_local_refs(|fixture| fixture.open_chat());
}

/// A member sends 100 messages into an open chat; each passes the membership
/// and own-profile insert policy and reaches the open chat's page.
#[divan::bench(args = [1_000, 10_000], sample_count = 20, sample_size = 1)]
fn chat_send_100(bencher: divan::Bencher<'_, '_>, messages: usize) {
    bencher
        .with_inputs(|| SendFixture::new(messages))
        .bench_local_refs(|fixture| fixture.send_messages(100));
}
