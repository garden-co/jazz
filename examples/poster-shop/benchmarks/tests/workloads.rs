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

#[test]
fn opening_the_canvas_delivers_every_surface() {
    // 64 shapes + 4 layers + 8 cursors + 4 assets + 3 checkpoints.
    assert_eq!(Fixture::new(64).open_canvas(), 83);
}

#[test]
fn live_canvas_shows_added_shapes_and_cursor_moves_leave_it_alone() {
    let fixture = Fixture::new(64);
    let mut live = fixture.live_canvas();
    assert_eq!(fixture.add_shape(&mut live), 1);
    assert_eq!(fixture.add_shape(&mut live), 1);
    assert_eq!(fixture.move_cursor(&mut live), 1);
    assert_eq!(fixture.move_cursor(&mut live), 1);
    assert!(!Fixture::canvas_has_pending_event(&mut live));
    assert_eq!(fixture.ordered_shape_count(), 66);
    // Reopening after edits still sees the whole canvas.
    assert_eq!(fixture.open_canvas(), 85);
}
