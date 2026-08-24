use jazz_example_jamazon_warehouse_benchmark::Fixture;
#[test]
fn pending_order_access_path_is_bounded() {
    let fixture = Fixture::new(120);
    assert_eq!(fixture.warehouse_district_count(), 2);
    assert_eq!(fixture.district_customer_count(), 1);
    assert_eq!(fixture.pending_order_count(), 20);
    assert_eq!(fixture.low_stock_count(), 2);

    let scope_sensitivity = Fixture::new(2);
    assert_eq!(
        scope_sensitivity.pending_order_count(),
        1,
        "a pending order from another district must not enter this district's window"
    );
}

#[test]
fn exclusive_purchase_commits_every_operational_row_together() {
    let fixture = Fixture::new(2);

    assert_eq!(
        fixture.purchase("checkout-1", 3),
        Ok(jazz_example_jamazon_warehouse_benchmark::PurchaseReceipt {
            order_number: 2,
            total_cents: 7_500,
        })
    );

    assert_eq!(fixture.stock_on_hand(), 7);
    assert_eq!(fixture.district_next_order_number(), 3);
    assert_eq!(fixture.customer_balance(), -7_500);
    assert_eq!(fixture.order_count(), 3);
    assert_eq!(fixture.order_line_count(), 1);
    assert_eq!(fixture.payment_count(), 1);
    assert_eq!(
        fixture.purchase_artifacts(),
        jazz_example_jamazon_warehouse_benchmark::PurchaseArtifacts {
            line_quantity: 3,
            line_amount_cents: 7_500,
            payment_amount_cents: 7_500,
            line_references_item: true,
            payment_references_customer: true,
            payment_references_order: true,
        }
    );
}

#[test]
fn insufficient_stock_abandons_the_entire_purchase() {
    let fixture = Fixture::new(2);

    assert_eq!(
        fixture.purchase("checkout-1", 11),
        Err("insufficient stock")
    );

    assert_eq!(fixture.stock_on_hand(), 10);
    assert_eq!(fixture.district_next_order_number(), 2);
    assert_eq!(fixture.order_count(), 2);
    assert_eq!(fixture.order_line_count(), 0);
    assert_eq!(fixture.payment_count(), 0);
}

#[test]
fn purchases_accumulate_balance_and_retries_return_the_original_receipt() {
    let fixture = Fixture::new(0);

    let first = fixture.purchase("checkout-1", 2).expect("first purchase");
    let retry = fixture.purchase("checkout-1", 2).expect("idempotent retry");
    let second = fixture.purchase("checkout-2", 3).expect("second purchase");

    assert_eq!(
        first, retry,
        "same request key returns its original receipt"
    );
    assert_eq!(first.total_cents, 5_000);
    assert_eq!(second.total_cents, 7_500);
    assert_eq!(fixture.stock_on_hand(), 5);
    assert_eq!(fixture.customer_balance(), -12_500);
    assert_eq!(fixture.district_next_order_number(), 2);
    assert_eq!(fixture.order_count(), 2);
    assert_eq!(fixture.order_line_count(), 2);
    assert_eq!(fixture.payment_count(), 2);
}

#[test]
fn total_overflow_abandons_purchase_before_any_row_is_staged() {
    let fixture = Fixture::new(0);
    fixture.set_stock_on_hand_for_test(i32::MAX);

    assert_eq!(
        fixture.purchase("overflow", i32::MAX),
        Err("total overflow")
    );

    assert_eq!(fixture.stock_on_hand(), i32::MAX);
    assert_eq!(fixture.customer_balance(), 0);
    assert_eq!(fixture.district_next_order_number(), 0);
    assert_eq!(fixture.order_count(), 0);
    assert_eq!(fixture.order_line_count(), 0);
    assert_eq!(fixture.payment_count(), 0);
}
