//! Guards the CodSpeed wall-time boundary for the route subscription receipt.

#[test]
fn route_curve_codspeed_receipts_exclude_fixture_lifecycle() {
    // `with_inputs` runs fixture construction before the measured closure.
    // The vendored CodSpeed Divan collector includes that external time
    // unless the macro option is present.
    let source = include_str!("../benches/route_subscription_curve.rs");
    assert_eq!(source.matches(".with_inputs(").count(), 2);
    assert_eq!(
        source.matches("sample_count = 3, skip_ext_time").count(),
        2,
        "every RouteFixture receipt must exclude setup and Drop from wall time"
    );
}
