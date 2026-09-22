use jazz_example_band_chat_benchmark::FastResumeFixture;

fn main() {
    divan::main();
}

/// Measure the remaining manifest cost for thesis #2136. A fresh usage cannot
/// infer its input closure from a cursor; known row bodies remain deduplicated.
#[divan::bench(args = [100, 1_000, 10_000])]
fn caught_up_fast_resume(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let mut fixture = FastResumeFixture::new(message_count);
    bencher.bench_local(|| {
        let receipt = fixture.caught_up_fast_resume();
        assert!(
            receipt.is_body_deduplicated_reset(),
            "caught-up resume leaked a payload: {receipt:?}"
        );
        divan::black_box(receipt)
    });
}
