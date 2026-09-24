use jazz_example_benchmark_w1::subscription_fanout::FanoutFixture;

fn main() {
    let mut args = std::env::args().skip(1);
    let rows = args.next().map(|s| s.parse().unwrap()).unwrap_or(600);
    let lists = args.next().map(|s| s.parse().unwrap()).unwrap_or(60);
    let repeats = args.next().map(|s| s.parse().unwrap()).unwrap_or(3);
    for sample in 0..repeats {
        let mut fixture = FanoutFixture::new(rows, lists);
        let receipt = fixture.hydrate();
        fixture.assert_initial_results();
        println!("sample={sample} {receipt:?}");
    }
}
