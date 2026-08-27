use jazz_example_band_chat_benchmark::FastResumeFixture;

fn main() {
    divan::main();
}

/// Thesis #2136: attaching a caught-up history subscriber must be bounded by
/// the missed delta (zero here), rather than by all retained messages.
#[divan::bench(args = [100, 1_000, 10_000])]
fn caught_up_fast_resume(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let mut fixture = FastResumeFixture::new(message_count);
    bencher.bench_local(|| {
        let receipt = fixture.caught_up_fast_resume();
        assert!(
            receipt.is_caught_up_noop(),
            "caught-up resume leaked a payload: {receipt:?}"
        );
        divan::black_box(receipt)
    });
}
