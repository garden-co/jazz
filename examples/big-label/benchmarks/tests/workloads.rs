use jazz_example_big_label_benchmark::{Fixture, expected_counts};

#[test]
fn representative_loads_return_exact_rows_at_each_benchmark_scale() {
    for release_count in [512, 4096] {
        let fixture = Fixture::new(release_count);
        let actual = (
            fixture.label_load(),
            fixture.artist_load(),
            fixture.catalog_load(),
        );
        assert_eq!(actual, expected_counts(release_count));
    }
}

#[test]
fn representative_loads_are_newest_release_first() {
    let release_count = 512;
    let fixture = Fixture::new(release_count);

    let expected = (0..release_count as u64)
        .filter(|release| release % 8 == 0)
        .rev()
        .collect::<Vec<_>>();
    assert_eq!(fixture.label_release_order(), expected);
}
