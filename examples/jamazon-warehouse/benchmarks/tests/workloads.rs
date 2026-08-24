use jazz_example_jamazon_warehouse_benchmark::Fixture;
#[test]
fn pending_order_access_path_is_bounded() {
    let fixture = Fixture::new(120);
    assert_eq!(fixture.warehouse_district_count(), 1);
    assert_eq!(fixture.district_customer_count(), 1);
    assert_eq!(fixture.pending_order_count(), 20);
    assert_eq!(fixture.low_stock_count(), 1);
}

#[test]
fn exclusive_purchase_commits_every_operational_row_together() {
    let fixture = Fixture::new(2);

    fixture.purchase(3).expect("purchase commits");

    assert_eq!(fixture.stock_on_hand(), 7);
    assert_eq!(fixture.district_next_order_number(), 3);
    assert_eq!(fixture.order_count(), 3);
    assert_eq!(fixture.order_line_count(), 1);
}

#[test]
fn insufficient_stock_abandons_the_entire_purchase() {
    let fixture = Fixture::new(2);

    assert_eq!(fixture.purchase(11), Err("insufficient stock"));

    assert_eq!(fixture.stock_on_hand(), 10);
    assert_eq!(fixture.district_next_order_number(), 2);
    assert_eq!(fixture.order_count(), 2);
    assert_eq!(fixture.order_line_count(), 0);
}
