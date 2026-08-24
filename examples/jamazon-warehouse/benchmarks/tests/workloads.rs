use jazz_example_jamazon_warehouse_benchmark::Fixture;
#[test]
fn pending_order_access_path_is_bounded() {
    let fixture = Fixture::new(120);
    assert_eq!(fixture.pending_order_count(), 20);
    assert_eq!(fixture.low_stock_count(), 0);
}
