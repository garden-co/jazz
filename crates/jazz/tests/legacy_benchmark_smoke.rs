// The legacy executable benchmark checks contain real protocol and delivery
// assertions. Keep those assertions in ordinary deterministic tests; timing is
// measured separately by the benchmark systems.
#[allow(dead_code)]
#[path = "../benches/cold_subscription.rs"]
mod cold_subscription;
#[allow(dead_code)]
#[path = "../benches/relation_include_delivery.rs"]
mod relation_include_delivery;
#[allow(dead_code)]
#[path = "../benches/route_subscription_curve.rs"]
mod route_subscription_curve;
#[allow(dead_code)]
#[path = "../benches/sync.rs"]
mod sync;
#[allow(dead_code)]
#[path = "../benches/validation.rs"]
mod validation;

#[test]
fn cold_subscription_correctness_smoke() {
    cold_subscription::correctness_smoke();
}

#[test]
fn sync_correctness_smoke() {
    sync::correctness_smoke();
}

#[test]
fn validation_correctness_smoke() {
    validation::correctness_smoke();
}

#[test]
fn relation_include_delivery_correctness_smoke() {
    relation_include_delivery::correctness_smoke();
}

#[test]
fn route_subscription_curve_correctness_smoke() {
    route_subscription_curve::correctness_smoke();
}
