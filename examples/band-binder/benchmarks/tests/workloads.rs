use jazz_example_band_binder_benchmark::Fixture;

#[test]
fn ordered_siblings_and_one_recursive_step_are_bounded() {
    let fixture = Fixture::new(128);
    assert_eq!(fixture.sibling_window_count(), 16);
    assert_eq!(fixture.child_page_count(), 16);
    assert_eq!(fixture.surface_window_counts(), [12, 12, 12, 12]);
}

#[test]
fn live_suggestion_window_orders_by_created_at() {
    let rows = Fixture::new(128).suggestion_window_receipt();
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        (20..32)
            .rev()
            .map(|index| {
                let mut bytes = [0_u8; 16];
                bytes[0] = 7;
                bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
                jazz::ids::RowUuid::from_bytes(bytes)
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows.iter().map(|row| row.created_at).collect::<Vec<_>>(),
        (2_000_000..2_000_012).collect::<Vec<_>>()
    );
    assert!(rows.iter().all(|row| {
        row.selected_fields
            == [
                "row_uuid",
                "user_payload",
                "user_status",
                "$createdBy",
                "$createdAt",
                "$updatedBy",
                "$updatedAt",
                "tx_time",
                "tx_node_id",
            ]
    }));
}
