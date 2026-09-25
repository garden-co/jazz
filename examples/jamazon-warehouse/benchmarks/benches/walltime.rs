//! Jamazon Warehouse wall-clock receipts for what an operator notices: how
//! long a checkout takes and how fast the pending-order console opens once
//! the store has a real order history. Names are app-prefixed because the
//! examples page matches CodSpeed results by exact name.

use jazz_example_jamazon_warehouse_benchmark::Fixture;

const CHECKOUTS: usize = 100;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

fn checkout_fixture(order_history: usize) -> Fixture {
    let fixture = Fixture::new(order_history);
    // Enough stock that no checkout in the measured run is refused.
    fixture.set_stock_on_hand_for_test(1_000_000);
    fixture
}

/// 100 consecutive checkouts against a store that already holds 1,000 orders.
/// Each is one exclusive transaction: idempotency lookup, stock, district
/// counter and customer balance reads, three updates and three inserts.
#[divan::bench(sample_count = 10)]
fn jamazon_checkout_100(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(|| checkout_fixture(1_000))
        .bench_local_values(|fixture| {
            for n in 0..CHECKOUTS {
                fixture
                    .purchase(&format!("checkout-{n}"), 1)
                    .expect("benchmark checkout succeeds");
            }
            fixture
        });
}

/// A retried checkout (same request key) returns the original receipt
/// without writing anything, against the same 1,000-order store.
#[divan::bench]
fn jamazon_checkout_retry(bencher: divan::Bencher<'_, '_>) {
    let fixture = checkout_fixture(1_000);
    fixture
        .purchase("checkout-retry", 1)
        .expect("first checkout succeeds");
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .purchase("checkout-retry", 1)
                .expect("retry returns the receipt"),
        )
    });
}

/// Open the console's first page of 20 pending orders in a district with
/// 10,000 orders of history.
#[divan::bench]
fn jamazon_pending_orders_10k(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new(10_000);
    bencher.bench_local(|| divan::black_box(fixture.pending_order_count()));
}
