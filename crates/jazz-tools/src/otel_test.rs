//! In-memory OTel metric exporter for integration tests.
//!
//! Provides a `TestMeterProvider` that collects metrics in memory
//! and allows assertions on the exported data.

use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

/// A meter provider backed by an in-memory exporter for test assertions.
pub struct TestMeterProvider {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
}

impl TestMeterProvider {
    /// Create a new test meter provider with in-memory collection.
    pub fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter.clone())
            .with_interval(std::time::Duration::from_millis(100))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();

        Self { provider, exporter }
    }

    /// Get a reference to the underlying `SdkMeterProvider`.
    pub fn provider(&self) -> &SdkMeterProvider {
        &self.provider
    }

    /// Force a metric collection and return all collected `ResourceMetrics`.
    pub fn collect(&self) -> Vec<ResourceMetrics> {
        // Force flush triggers the reader to collect and export
        self.provider.force_flush().expect("force flush metrics");
        self.exporter.get_finished_metrics().expect("get metrics")
    }

    /// Shutdown the provider.
    pub fn shutdown(self) {
        let _ = self.provider.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider as _;

    #[test]
    fn test_meter_provider_collects_counter() {
        let test_provider = TestMeterProvider::new();
        let meter = test_provider.provider().meter("test");

        let counter = meter.u64_counter("smoke.test.counter").build();
        counter.add(42, &[opentelemetry::KeyValue::new("app_id", "app-1")]);

        let metrics = test_provider.collect();

        // Should have at least one ResourceMetrics with our counter
        assert!(!metrics.is_empty(), "should have exported metrics");

        let mut found = false;
        for rm in &metrics {
            for scope_metrics in rm.scope_metrics() {
                for metric in scope_metrics.metrics() {
                    if metric.name() == "smoke.test.counter" {
                        found = true;
                    }
                }
            }
        }
        assert!(found, "should find smoke.test.counter in exported metrics");

        test_provider.shutdown();
    }
}
