//! OpenTelemetry tracer initialization.
//!
//! Activated by the `otel` feature + `OTEL_EXPORTER_OTLP_ENDPOINT` being set
//! at runtime in the CLI, or by explicit dev-server telemetry configuration
//! in NAPI. Standard OTel env vars (`OTEL_SERVICE_NAME`, `OTEL_TRACES_SAMPLER`,
//! etc.) are respected automatically by the SDK.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
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
