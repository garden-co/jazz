use jazz_example_band_chat_benchmark::Fixture;

fn main() {
    divan::main();
}

#[divan::bench(args = [1024, 4096])]
fn timeline_second_page(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let fixture = Fixture::new(message_count);
    bencher.bench_local(|| divan::black_box(fixture.timeline_page_count()));
}

#[divan::bench(args = [1024, 4096])]
fn unread_recent_rooms(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let fixture = Fixture::new(message_count);
    bencher.bench_local(|| divan::black_box(fixture.unread_room_count()));
}

#[divan::bench(args = [1024, 4096])]
fn author_history(bencher: divan::Bencher<'_, '_>, message_count: usize) {
    let fixture = Fixture::new(message_count);
    bencher.bench_local(|| divan::black_box(fixture.author_history_count()));
}
