use jazz_example_music_agent_benchmark::Fixture;

fn main() {
    divan::main();
}

#[divan::bench]
fn append_assistant_tail(bencher: divan::Bencher<'_, '_>) {
    bencher.bench_local(|| Fixture::new().append_assistant_tail());
}

#[divan::bench]
fn attachment_range(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.attachment_range()));
}

#[divan::bench]
fn materialized_transcript(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new();
    bencher.bench_local(|| divan::black_box(fixture.materialized_transcript()));
}
