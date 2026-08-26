use jazz_example_poster_shop_benchmark::Fixture;
#[test]
fn canvas_queries_preserve_shape_z_order_and_cursor_fanout() {
    let fixture = Fixture::new(64);
    assert_eq!(fixture.ordered_shape_count(), 64);
    assert_eq!(fixture.ordered_layer_count(), 4);
    assert_eq!(fixture.cursor_fanout_count(), 8);
    assert_eq!(fixture.layer_shape_count(), 16);
    assert_eq!(fixture.asset_metadata_count(), 4);
    assert_eq!(fixture.checkpoint_count(), 3);
    assert_eq!(fixture.ordered_z_indices(), (0..64).collect::<Vec<_>>());
    assert_eq!(
        fixture.shape_indexed_columns(),
        ["canvas", "layer", "z_index"]
    );
}
