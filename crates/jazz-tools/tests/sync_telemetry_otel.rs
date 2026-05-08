//! Verification test: drive a small two-client sync flow with OpenTelemetry
//! exporting to a local OTLP/HTTP collector (e.g. Everr).
//!
//! Marked `#[ignore]` so it does not run in CI by default — it requires a
//! collector listening at `OTEL_EXPORTER_OTLP_ENDPOINT`.
//!
//! Run with Everr running locally:
//!
//! ```bash
//! everr telemetry start
//! OTEL_EXPORTER_OTLP_ENDPOINT=$(everr telemetry endpoint) \
//! OTEL_SERVICE_NAME=jazz-sync-telemetry-demo \
//! RUST_LOG=jazz_tools=debug \
//! cargo test -p jazz-tools --features test,otel \
//!   --test sync_telemetry_otel -- --ignored --nocapture
//! ```
//!
//! Then verify spans landed:
//!
//! ```bash
//! everr telemetry query "SELECT name, count(*) FROM otel_traces \
//!   WHERE service_name = 'jazz-sync-telemetry-demo' \
//!   AND name IN ('sync.send', 'sync.recv', 'process_from_server', 'process_from_client') \
//!   GROUP BY name"
//! ```

#![cfg(all(feature = "test", feature = "otel-core"))]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::otel;
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};
use opentelemetry::trace::TracerProvider;
use support::wait_for_query;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

#[tokio::test]
#[ignore = "requires a local OTLP/HTTP collector at OTEL_EXPORTER_OTLP_ENDPOINT"]
async fn sync_layers_emit_otel_spans() {
    let provider = otel::init_tracer_provider();
    let tracer = provider.tracer("jazz-sync-telemetry-demo");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("jazz_tools=debug"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .init();

    let schema = schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;

    let alice = JazzClient::connect(server.make_client_context(schema.clone()))
        .await
        .expect("connect alice");
    let bob = JazzClient::connect(server.make_client_context(schema.clone()))
        .await
        .expect("connect bob");

    wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "alice EdgeServer ready",
        |_| Some(()),
    )
    .await;
    wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob EdgeServer ready",
        |_| Some(()),
    )
    .await;

    alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("first".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("alice create");

    let observed = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob observes alice's todo",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;
    assert_eq!(observed.len(), 1);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;

    provider.shutdown().expect("OTel shutdown");
}
