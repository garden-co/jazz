use jazz_testkit as support;

use std::collections::HashMap;
use std::time::Duration;

use jazz::tools::{ColumnType, DurabilityTier, Schema, SchemaBuilder, TableSchema, Value};
use jazz_otel as otel;
use jazz_server::JazzServer;
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
#[ignore = "#1787: manual telemetry receipt; stdout fallback executes, collector is only needed to inspect OTLP export"]
async fn sync_layers_emit_otel_spans() {
    tokio::task::LocalSet::new()
        .run_until(async {
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
            let server = JazzServer::start_with_schema(schema.clone()).await;
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

            let (todo_id, expected_values, transaction_id) = alice
                .insert("todos", todo_values("trace sync telemetry", false))
                .expect("alice creates persisted todo");
            alice
                .wait_for_transaction(
                    transaction_id.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("alice persisted todo reaches edge");

            wait_for_query(
                &bob,
                jazz::query::Query::from("todos"),
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
        })
        .await;
}
