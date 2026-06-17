//! OpenTelemetry tracer initialization.
//!
//! Activated by the `otel` feature + `OTEL_EXPORTER_OTLP_ENDPOINT` being set
//! at runtime in the CLI, or by explicit dev-server telemetry configuration
//! in NAPI. Standard OTel env vars (`OTEL_SERVICE_NAME`, `OTEL_TRACES_SAMPLER`,
//! etc.) are respected automatically by the SDK.

use opentelemetry::metrics::{Meter, ObservableGauge};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;

const DEFAULT_SERVICE_NAME: &str = "jazz-server";

/// Resolve the service name from `OTEL_SERVICE_NAME`, falling back to
/// `DEFAULT_SERVICE_NAME`. Used by the env-driven provider builders. The
/// explicit-name path (`*_with_endpoint`, e.g. the NAPI dev-server) passes its
/// own name straight to `otel_resource` and intentionally bypasses this.
fn resolved_service_name() -> String {
    std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| DEFAULT_SERVICE_NAME.into())
}

/// Build an OTel TracerProvider exporting to the OTLP/HTTP endpoint
/// configured via `OTEL_EXPORTER_OTLP_ENDPOINT`.
pub fn init_tracer_provider() -> SdkTracerProvider {
    let builder = tracer_provider_builder(&resolved_service_name());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .build()
        .expect("failed to build OTLP exporter");

    builder.with_batch_exporter(exporter).build()
}

/// Build an OTel TracerProvider for a specific service and optional OTLP/HTTP traces endpoint.
pub fn init_tracer_provider_with_endpoint(
    service_name: &str,
    traces_endpoint: Option<&str>,
) -> SdkTracerProvider {
    let mut builder = tracer_provider_builder(service_name);

    if let Some(endpoint) = traces_endpoint {
        let mut exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson);
        exporter = exporter.with_endpoint(endpoint.to_string());
        builder =
            builder.with_batch_exporter(exporter.build().expect("failed to build OTLP exporter"));
    } else {
        let exporter = opentelemetry_stdout::SpanExporter::default();
        builder = builder.with_simple_exporter(exporter);
    }

    builder.build()
}

fn tracer_provider_builder(service_name: &str) -> opentelemetry_sdk::trace::TracerProviderBuilder {
    SdkTracerProvider::builder().with_resource(otel_resource(service_name))
}

/// Build the OTel `Resource` (service name + version) shared by the tracer,
/// logger, and meter providers, so the three never silently diverge.
fn otel_resource(service_name: &str) -> opentelemetry_sdk::Resource {
    opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name.to_string())
        .with_attribute(opentelemetry::KeyValue::new(
            "service.version",
            env!("CARGO_PKG_VERSION"),
        ))
        .build()
}

pub fn normalize_otlp_traces_endpoint(collector_url: &str) -> String {
    let trimmed = collector_url.trim().trim_end_matches('/');
    if trimmed.ends_with("/v1/traces") {
        return trimmed.to_string();
    }
    if trimmed.ends_with("/v1/logs") {
        return format!("{}/v1/traces", trimmed.trim_end_matches("/v1/logs"));
    }
    format!("{trimmed}/v1/traces")
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

/// Build an OTel LoggerProvider exporting to the OTLP/HTTP endpoint
/// configured via `OTEL_EXPORTER_OTLP_ENDPOINT`.
pub fn init_logger_provider() -> SdkLoggerProvider {
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .build()
        .expect("failed to build OTLP log exporter");

    SdkLoggerProvider::builder()
        .with_resource(otel_resource(&resolved_service_name()))
        .with_batch_exporter(exporter)
        .build()
}

/// Build an OTel MeterProvider exporting to the OTLP/HTTP endpoint configured
/// via `OTEL_EXPORTER_OTLP_ENDPOINT`.
pub fn init_meter_provider() -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .build()
        .expect("failed to build OTLP metric exporter");

    SdkMeterProvider::builder()
        .with_resource(otel_resource(&resolved_service_name()))
        .with_periodic_exporter(exporter)
        .build()
}

/// Build the `opentelemetry-appender-tracing` bridge layer from a logger provider.
/// Converts `tracing` events into OTel `LogRecord`s with trace context attached.
pub fn log_bridge<S>(
    provider: &SdkLoggerProvider,
) -> opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge<
    SdkLoggerProvider,
    opentelemetry_sdk::logs::SdkLogger,
>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(provider)
}

/// Metric name for the active inbound WebSocket gauge.
const ACTIVE_WEBSOCKETS_METRIC: &str = "jazz.server.active_websockets";

/// Register an observable gauge that samples `count` on each metric collection
/// and reports it as `jazz.server.active_websockets`. The returned instrument
/// must be kept alive for as long as the metric should be reported.
pub fn register_active_websockets_gauge<F>(meter: &Meter, count: F) -> ObservableGauge<u64>
where
    F: Fn() -> u64 + Send + Sync + 'static,
{
    meter
        .u64_observable_gauge(ACTIVE_WEBSOCKETS_METRIC)
        .with_description("Currently active inbound WebSocket connections")
        .with_unit("{connection}")
        .with_callback(move |observer| {
            observer.observe(count(), &[]);
        })
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn active_websockets_gauge_reports_the_count() {
        use opentelemetry::metrics::MeterProvider as _;
        use opentelemetry_sdk::metrics::InMemoryMetricExporter;
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let meter = provider.meter("test");

        // Wire the gauge to a fixed count and force a collect, then assert the
        // exported datapoint actually carries that value — not merely that the
        // flush succeeded.
        let _gauge = register_active_websockets_gauge(&meter, || 1);
        provider.force_flush().expect("flush collects the gauge");

        let observed = exporter
            .get_finished_metrics()
            .expect("metrics collected")
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .find(|m| m.name() == "jazz.server.active_websockets")
            .and_then(|m| match m.data() {
                AggregatedMetrics::U64(MetricData::Gauge(g)) => {
                    g.data_points().next().map(|dp| dp.value())
                }
                _ => None,
            })
            .expect("active_websockets gauge datapoint present");
        assert_eq!(observed, 1);
    }
}
