use jazz_example_big_label_benchmark::{Fixture, IngestFixture, expected_counts};

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

#[test]
fn batched_ingest_commits_every_release() {
    for batch_size in [1, 10, 100, 1_000] {
        let fixture = IngestFixture::new();
        fixture.ingest_releases(1_000, batch_size);
        assert_eq!(fixture.release_count(), 1_000);
    }
}

#[test]
fn batched_generated_id_ingest_preserves_values_and_indexed_label_order() {
    let fixture = IngestFixture::new();
    fixture.ingest_releases(1_000, 100);

    let expected = (0..1_000)
        .filter(|release| release % 8 == 3)
        .rev()
        .map(|release| (format!("Release {release:06}"), release as u64))
        .collect::<Vec<_>>();
    assert_eq!(fixture.label_release_titles_and_order(3), expected);
}
