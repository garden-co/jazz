//! OpenTelemetry tracer initialization.
//!
//! Activated by the `otel` feature + `JAZZ_OTEL=1` env var at runtime.
//! Standard OTel env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`,
//! `OTEL_TRACES_SAMPLER`, etc.) are respected automatically by the SDK.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;

/// Build an OTel TracerProvider.
///
/// - If `OTEL_EXPORTER_OTLP_ENDPOINT` is set → OTLP gRPC exporter (tonic).
/// - Otherwise → stdout exporter for local dev.
pub fn init_tracer_provider() -> SdkTracerProvider {
    let mut builder = SdkTracerProvider::builder().with_resource(
        opentelemetry_sdk::Resource::builder()
            .with_service_name(
                std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "jazz-server".into()),
            )
            .with_attribute(opentelemetry::KeyValue::new(
                "service.version",
                env!("CARGO_PKG_VERSION"),
            ))
            .build(),
    );

    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()
            .expect("failed to build OTLP exporter");
        builder = builder.with_batch_exporter(exporter);
    } else {
        let exporter = opentelemetry_stdout::SpanExporter::default();
        builder = builder.with_simple_exporter(exporter);
    }

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
