use jazz_example_benchmark_w1::direct_upload::UploadFixture;

fn main() {
    let mut args = std::env::args().skip(1);
    let rows = args.next().map(|s| s.parse().unwrap()).unwrap_or(2_000);
    let batch = args.next().map(|s| s.parse().unwrap()).unwrap_or(50);
    let repeats = args.next().map(|s| s.parse().unwrap()).unwrap_or(3);
    for sample in 0..repeats {
        let mut fixture = UploadFixture::new(rows, batch);
        let receipt = fixture.upload();
        fixture.assert_uploaded();
        println!("sample={sample} {receipt:?}");
    }
}
