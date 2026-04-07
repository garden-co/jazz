//! OpenTelemetry tracer and meter initialization.
//!
//! Activated by the `otel` feature + `JAZZ_OTEL=1` env var at runtime.
//! Standard OTel env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`,
//! `OTEL_TRACES_SAMPLER`, etc.) are respected automatically by the SDK.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;

/// Build the shared OTel resource with service metadata.
fn build_resource() -> opentelemetry_sdk::Resource {
    opentelemetry_sdk::Resource::builder()
        .with_service_name(
            std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "jazz-server".into()),
        )
        .with_attribute(opentelemetry::KeyValue::new(
            "service.version",
            env!("CARGO_PKG_VERSION"),
        ))
        .with_attribute(opentelemetry::KeyValue::new(
            "service.instance.id",
            std::env::var("OTEL_SERVICE_INSTANCE_ID").unwrap_or_else(|_| uuid_instance_id()),
        ))
        .build()
}

/// Generate a random instance ID when none is provided.
fn uuid_instance_id() -> String {
    use std::sync::OnceLock;
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| uuid::Uuid::new_v4().to_string()).clone()
}

/// Parse the metric export interval from env, defaulting to 60 seconds.
/// `OTEL_METRIC_EXPORT_INTERVAL` is in milliseconds per the OTel spec.
fn metric_export_interval() -> std::time::Duration {
    std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_millis)
        .unwrap_or(std::time::Duration::from_secs(60))
}

/// Build an OTel TracerProvider.
///
/// - If `OTEL_EXPORTER_OTLP_ENDPOINT` is set -> OTLP gRPC exporter (tonic).
/// - Otherwise -> stdout exporter for local dev.
pub fn init_tracer_provider() -> SdkTracerProvider {
    let mut builder = SdkTracerProvider::builder().with_resource(build_resource());

    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()
            .expect("failed to build OTLP span exporter");
        builder = builder.with_batch_exporter(exporter);
    } else {
        let exporter = opentelemetry_stdout::SpanExporter::default();
        builder = builder.with_simple_exporter(exporter);
    }

    builder.build()
}

/// Build an OTel MeterProvider.
///
/// - If `OTEL_EXPORTER_OTLP_ENDPOINT` is set -> OTLP gRPC exporter.
/// - Otherwise -> stdout exporter for local dev.
///
/// Export interval is controlled by `OTEL_METRIC_EXPORT_INTERVAL` (seconds),
/// defaulting to 60s.
pub fn init_meter_provider() -> SdkMeterProvider {
    let interval = metric_export_interval();

    let builder = if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .build()
            .expect("failed to build OTLP metric exporter");

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
            .with_interval(interval)
            .build();

        SdkMeterProvider::builder()
            .with_resource(build_resource())
            .with_reader(reader)
    } else {
        let exporter = opentelemetry_stdout::MetricExporter::default();

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
            .with_interval(interval)
            .build();

        SdkMeterProvider::builder()
            .with_resource(build_resource())
            .with_reader(reader)
    };

    builder.build()
}

/// Build the `tracing_opentelemetry` layer from a provider.
pub fn layer<S>(
    provider: &SdkTracerProvider,
) -> tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    let tracer = provider.tracer("jazz-server");
    tracing_opentelemetry::layer().with_tracer(tracer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider as _;

    #[test]
    fn init_meter_provider_returns_provider() {
        let provider = init_meter_provider();
        let meter = provider.meter("test");
        let counter = meter.u64_counter("test.counter").build();
        counter.add(1, &[]);
        let _ = provider.shutdown();
    }
}
