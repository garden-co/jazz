#![cfg(all(feature = "test", feature = "otel-core"))]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::otel;
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};
use support::{TestingClient, has_row, wait_for_query};
use tracing_subscriber::{EnvFilter, prelude::*};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn test_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

fn todo_values(title: &str, completed: bool) -> HashMap<String, Value> {
    HashMap::from([
        ("title".to_string(), Value::Text(title.to_string())),
        ("completed".to_string(), Value::Boolean(completed)),
    ])
}

#[tokio::test]
#[ignore = "requires a local OTLP/HTTP collector at OTEL_EXPORTER_OTLP_ENDPOINT"]
async fn sync_layers_emit_otel_spans() {
    let traces_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .map(|url| otel::normalize_otlp_traces_endpoint(&url));
    let provider = otel::init_tracer_provider_with_endpoint(
        "jazz-sync-telemetry-test",
        traces_endpoint.as_deref(),
    );
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new("jazz_tools=debug"))
        .with(otel::layer(&provider));
    let subscriber_guard = tracing::subscriber::set_default(subscriber);

    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-otel")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-otel")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let (todo_id, expected_values, batch_id) = alice
        .insert("todos", todo_values("trace sync telemetry", false))
        .expect("alice creates persisted todo");
    alice
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("alice persisted todo reaches edge");

    wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees alice's todo through sync",
        |rows| has_row(&rows, todo_id, &expected_values).then_some(()),
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;

    drop(subscriber_guard);
    provider.shutdown().expect("shutdown telemetry provider");
}
