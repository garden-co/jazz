#![cfg(feature = "otel")]

#[path = "support/otel_test.rs"]
mod otel_test;
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use otel_test::TestMeterProvider;

/// A u64 counter aggregates increments by unique attribute set.
/// Two adds to app-abc and one to app-xyz should produce two data points.
#[test]
fn counter_with_attributes_is_exported() {
    let test_provider = TestMeterProvider::new();
    let meter = test_provider.provider().meter("test");

    let counter = meter.u64_counter("jazz.sync.connections.total").build();
    counter.add(1, &[KeyValue::new("app_id", "app-abc")]);
    counter.add(2, &[KeyValue::new("app_id", "app-abc")]);
    counter.add(5, &[KeyValue::new("app_id", "app-xyz")]);

    let metrics = test_provider.collect();
    assert!(!metrics.is_empty(), "should have exported metrics");

    let mut found = false;
    for rm in &metrics {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                if metric.name() == "jazz.sync.connections.total" {
                    found = true;
                    match metric.data() {
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            let data_points: Vec<_> = sum.data_points().collect();
                            assert_eq!(
                                data_points.len(),
                                2,
                                "expected 2 data points (one per unique attribute set), got {}",
                                data_points.len()
                            );
                        }
                        other => panic!(
                            "expected U64 Sum for counter, got {:?}",
                            std::mem::discriminant(other)
                        ),
                    }
                }
            }
        }
    }
    assert!(
        found,
        "should find jazz.sync.connections.total in exported metrics"
    );

    test_provider.shutdown();
}

/// An f64 histogram partitions observations by unique attribute set.
/// Three records across two app_ids should produce two data points.
#[test]
fn histogram_records_observations() {
    let test_provider = TestMeterProvider::new();
    let meter = test_provider.provider().meter("test");

    let histogram = meter.f64_histogram("jazz.sync.message.size_bytes").build();
    histogram.record(128.0, &[KeyValue::new("app_id", "app-abc")]);
    histogram.record(256.0, &[KeyValue::new("app_id", "app-abc")]);
    histogram.record(512.0, &[KeyValue::new("app_id", "app-xyz")]);

    let metrics = test_provider.collect();
    assert!(!metrics.is_empty(), "should have exported metrics");

    let mut found = false;
    for rm in &metrics {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                if metric.name() == "jazz.sync.message.size_bytes" {
                    found = true;
                    match metric.data() {
                        AggregatedMetrics::F64(MetricData::Histogram(hist)) => {
                            let data_points: Vec<_> = hist.data_points().collect();
                            assert_eq!(
                                data_points.len(),
                                2,
                                "expected 2 data points (one per unique attribute set), got {}",
                                data_points.len()
                            );
                        }
                        other => panic!(
                            "expected F64 Histogram, got {:?}",
                            std::mem::discriminant(other)
                        ),
                    }
                }
            }
        }
    }
    assert!(
        found,
        "should find jazz.sync.message.size_bytes in exported metrics"
    );

    test_provider.shutdown();
}

/// An i64 up_down_counter nets positive and negative adds.
/// +1, +1, -1 for the same attribute set should yield a final value of 1.
#[test]
fn up_down_counter_tracks_active_value() {
    let test_provider = TestMeterProvider::new();
    let meter = test_provider.provider().meter("test");

    let counter = meter
        .i64_up_down_counter("jazz.sync.connections.active")
        .build();
    counter.add(1, &[KeyValue::new("app_id", "app-abc")]);
    counter.add(1, &[KeyValue::new("app_id", "app-abc")]);
    counter.add(-1, &[KeyValue::new("app_id", "app-abc")]);

    let metrics = test_provider.collect();
    assert!(!metrics.is_empty(), "should have exported metrics");

    let mut found = false;
    for rm in &metrics {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                if metric.name() == "jazz.sync.connections.active" {
                    found = true;
                    match metric.data() {
                        AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                            let data_points: Vec<_> = sum.data_points().collect();
                            assert_eq!(
                                data_points.len(),
                                1,
                                "expected 1 data point for app-abc, got {}",
                                data_points.len()
                            );
                            assert_eq!(
                                data_points[0].value(),
                                1,
                                "expected value 1 (1 + 1 - 1), got {}",
                                data_points[0].value()
                            );
                        }
                        other => panic!(
                            "expected I64 Sum for up_down_counter, got {:?}",
                            std::mem::discriminant(other)
                        ),
                    }
                }
            }
        }
    }
    assert!(
        found,
        "should find jazz.sync.connections.active in exported metrics"
    );

    test_provider.shutdown();
}
