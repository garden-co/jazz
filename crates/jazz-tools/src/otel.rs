//! OpenTelemetry tracer initialization.
//!
//! Activated by the `otel` feature + `OTEL_EXPORTER_OTLP_ENDPOINT` being set
//! at runtime in the CLI, or by explicit dev-server telemetry configuration
//! in NAPI. Standard OTel env vars (`OTEL_SERVICE_NAME`, `OTEL_TRACES_SAMPLER`,
//! etc.) are respected automatically by the SDK.

use crate::server::ShutdownController;
use opentelemetry::metrics::{Meter, ObservableGauge};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;

const DEFAULT_SERVICE_NAME: &str = "jazz-server";

/// Build an OTel TracerProvider exporting to the OTLP/HTTP endpoint
/// configured via `OTEL_EXPORTER_OTLP_ENDPOINT`.
pub fn init_tracer_provider() -> SdkTracerProvider {
    let builder = tracer_provider_builder(
        std::env::var("OTEL_SERVICE_NAME")
            .unwrap_or_else(|_| DEFAULT_SERVICE_NAME.into())
            .as_str(),
    );

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
    SdkTracerProvider::builder().with_resource(
        opentelemetry_sdk::Resource::builder()
            .with_service_name(service_name.to_string())
            .with_attribute(opentelemetry::KeyValue::new(
                "service.version",
                env!("CARGO_PKG_VERSION"),
            ))
            .build(),
    )
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
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(
                    std::env::var("OTEL_SERVICE_NAME")
                        .unwrap_or_else(|_| DEFAULT_SERVICE_NAME.into()),
                )
                .with_attribute(opentelemetry::KeyValue::new(
                    "service.version",
                    env!("CARGO_PKG_VERSION"),
                ))
                .build(),
        )
        .with_batch_exporter(exporter)
        .build()
}

/// Build a `Resource` carrying the service name + version, shared by the
/// metric providers (mirrors the tracer/logger resource).
fn meter_resource(service_name: String) -> opentelemetry_sdk::Resource {
    opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name)
        .with_attribute(opentelemetry::KeyValue::new(
            "service.version",
            env!("CARGO_PKG_VERSION"),
        ))
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
        .with_resource(meter_resource(
            std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| DEFAULT_SERVICE_NAME.into()),
        ))
        .with_periodic_exporter(exporter)
        .build()
}

/// Build an OTel MeterProvider for a specific service and optional OTLP/HTTP
/// metrics endpoint. Falls back to the stdout exporter when no endpoint is
/// given (dev parity with `init_tracer_provider_with_endpoint`).
pub fn init_meter_provider_with_endpoint(
    service_name: &str,
    metrics_endpoint: Option<&str>,
) -> SdkMeterProvider {
    let builder =
        SdkMeterProvider::builder().with_resource(meter_resource(service_name.to_string()));

    match metrics_endpoint {
        Some(endpoint) => {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpJson)
                .with_endpoint(endpoint.to_string())
                .build()
                .expect("failed to build OTLP metric exporter");
            builder.with_periodic_exporter(exporter).build()
        }
        None => builder
            .with_periodic_exporter(opentelemetry_stdout::MetricExporter::default())
            .build(),
    }
}

/// Map an arbitrary collector base/`/v1/logs`/`/v1/traces` URL to its
/// `/v1/metrics` path (mirrors `normalize_otlp_traces_endpoint`).
pub fn normalize_otlp_metrics_endpoint(collector_url: &str) -> String {
    let trimmed = collector_url.trim().trim_end_matches('/');
    if trimmed.ends_with("/v1/metrics") {
        return trimmed.to_string();
    }
    if let Some(base) = trimmed.strip_suffix("/v1/logs") {
        return format!("{base}/v1/metrics");
    }
    if let Some(base) = trimmed.strip_suffix("/v1/traces") {
        return format!("{base}/v1/metrics");
    }
    format!("{trimmed}/v1/metrics")
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

/// Current inbound WebSocket connection count, as observed by the gauge.
pub fn active_websocket_count(controller: &ShutdownController) -> u64 {
    controller.active_websockets() as u64
}

/// Register an observable gauge that samples the server's active inbound
/// WebSocket count on each metric collection. The returned instrument must be
/// kept alive for as long as the metric should be reported.
pub fn register_active_websockets_gauge(
    meter: &Meter,
    controller: ShutdownController,
) -> ObservableGauge<u64> {
    meter
        .u64_observable_gauge(ACTIVE_WEBSOCKETS_METRIC)
        .with_description("Currently active inbound WebSocket connections")
        .with_unit("{connection}")
        .with_callback(move |observer| {
            observer.observe(active_websocket_count(&controller), &[]);
        })
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_metrics_endpoint_appends_v1_metrics() {
        assert_eq!(
            normalize_otlp_metrics_endpoint("http://collector:4318"),
            "http://collector:4318/v1/metrics"
        );
    }

    #[test]
    fn normalize_metrics_endpoint_is_idempotent() {
        assert_eq!(
            normalize_otlp_metrics_endpoint("http://collector:4318/v1/metrics/"),
            "http://collector:4318/v1/metrics"
        );
    }

    #[test]
    fn normalize_metrics_endpoint_swaps_logs_and_traces_paths() {
        assert_eq!(
            normalize_otlp_metrics_endpoint("http://collector:4318/v1/logs"),
            "http://collector:4318/v1/metrics"
        );
        assert_eq!(
            normalize_otlp_metrics_endpoint("http://collector:4318/v1/traces"),
            "http://collector:4318/v1/metrics"
        );
    }

    #[tokio::test]
    async fn meter_provider_with_endpoint_builds_without_panic() {
        // No endpoint -> stdout exporter path; just prove construction + clean
        // shutdown work under a runtime (PeriodicReader needs rt-tokio).
        let provider = init_meter_provider_with_endpoint("test-service", None);
        provider
            .shutdown()
            .expect("meter provider shuts down cleanly");
    }

    #[test]
    fn active_websocket_count_tracks_controller() {
        use crate::server::ShutdownController;
        use std::time::Duration;

        let controller = ShutdownController::new(Duration::from_secs(30));
        assert_eq!(active_websocket_count(&controller), 0);

        let g1 = controller.try_enter_websocket().expect("enter ws 1");
        let g2 = controller.try_enter_websocket().expect("enter ws 2");
        assert_eq!(active_websocket_count(&controller), 2);

        drop(g1);
        assert_eq!(active_websocket_count(&controller), 1);
        drop(g2);
        assert_eq!(active_websocket_count(&controller), 0);
    }

    #[tokio::test]
    async fn active_websockets_gauge_registers_and_flushes() {
        use crate::server::ShutdownController;
        use opentelemetry::metrics::MeterProvider as _;
        use std::time::Duration;

        let controller = ShutdownController::new(Duration::from_secs(30));
        let _guard = controller.try_enter_websocket().expect("enter ws");

        // Use the real stdout PeriodicReader provider (no experimental feature).
        // force_flush drives a collect, which invokes the observable callback —
        // proving the wiring runs end to end.
        let provider = init_meter_provider_with_endpoint("test-service", None);
        let meter = provider.meter("test");
        let _gauge = register_active_websockets_gauge(&meter, controller.clone());

        provider
            .force_flush()
            .expect("flush collects the gauge without error");
        provider.shutdown().expect("provider shuts down cleanly");
    }
}
