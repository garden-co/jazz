#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::runtime_core::ReadDurabilityOptions;
use jazz_tools::runtime_tokio::TokioRuntime;
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::server::TestingServer;
use jazz_tools::storage::MemoryStorage;
use jazz_tools::sync_manager::SyncManager;
use jazz_tools::transport_manager::{self, AuthConfig, TickNotifier, TransportInbound};
use jazz_tools::ws_stream::NativeWsStream;
use jazz_tools::{ColumnType, DurabilityTier, QueryBuilder, SchemaBuilder, TableSchema, Value};

fn todos_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

/// No-op tick notifier for the low-level transport test where we manually poll
/// the inbound channel rather than driving a full runtime tick loop.
struct NoopTickNotifier;

impl TickNotifier for NoopTickNotifier {
    fn notify(&self) {}
}

fn ws_url(server: &TestingServer) -> String {
    format!("ws://127.0.0.1:{}/ws", server.port())
}

fn admin_auth() -> AuthConfig {
    AuthConfig {
        backend_secret: Some(TestingServer::BACKEND_SECRET.to_string()),
        admin_secret: Some(TestingServer::ADMIN_SECRET.to_string()),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Test 1: Low-level WebSocket connection delivers a Connected event
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_connects_via_websocket_and_receives_connected_event() {
    let server = TestingServer::start().await;

    let (mut handle, manager) = transport_manager::create::<NativeWsStream, NoopTickNotifier>(
        ws_url(&server),
        admin_auth(),
        NoopTickNotifier,
    );

    tokio::spawn(manager.run());

    use futures::StreamExt as _;
    let event = tokio::time::timeout(Duration::from_secs(5), handle.inbound_rx.next())
        .await
        .expect("timed out waiting for Connected event from WebSocket transport")
        .expect("transport channel closed before Connected event");

    assert!(
        matches!(event, TransportInbound::Connected { .. }),
        "first inbound event should be Connected, got: {event:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 2: Two TokioRuntime clients sync a todo through the server via WS
// ---------------------------------------------------------------------------

/// Builds a `TokioRuntime` backed by in-memory storage and connects it to the
/// server over WebSocket transport.
fn connect_ws_runtime(
    schema: jazz_tools::Schema,
    server: &TestingServer,
) -> TokioRuntime<MemoryStorage> {
    let app_id = AppId::from_name("ws-sync-test");
    let sm = SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main")
        .expect("build schema manager");
    let runtime = TokioRuntime::new(sm, MemoryStorage::new());

    // Persist schema so the catalogue objects reach the server.
    runtime.persist_schema().expect("persist schema");

    runtime
        .connect(ws_url(server), admin_auth())
        .expect("ws connect");

    runtime
}

#[tokio::test]
async fn two_clients_sync_via_websocket() {
    let schema = todos_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;

    // -- alice connects and writes a todo --
    let alice = connect_ws_runtime(schema.clone(), &server);

    // Give the transport a moment to complete the handshake and let the first
    // batched_tick drain the Connected event (adds the server).
    alice.flush().await.expect("alice flush after connect");
    tokio::time::sleep(Duration::from_millis(200)).await;
    alice.flush().await.expect("alice flush catalogue");

    let (todo_id, _) = alice
        .insert(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("buy milk".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
            None,
        )
        .expect("alice insert todo");

    // Verify alice sees her own row locally.
    {
        let future = alice
            .query(
                QueryBuilder::new("todos").build(),
                None,
                ReadDurabilityOptions::default(),
            )
            .expect("alice local query");
        let rows = future.await.expect("alice local query future");
        assert_eq!(rows.len(), 1, "alice should see her own todo locally");
    }

    // Wait until the EdgeServer acknowledges alice's write before connecting bob.
    // Drives alice's transport in a loop until the durability future resolves.
    let edge_ack = alice
        .query(
            QueryBuilder::new("todos").build(),
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                ..ReadDurabilityOptions::default()
            },
        )
        .expect("alice EdgeServer ack query");

    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = async {
                loop {
                    alice.flush().await.expect("alice flush");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            } => {}
            result = edge_ack => {
                result.expect("alice EdgeServer ack future");
            }
        }
    })
    .await
    .expect("timed out waiting for alice's write to reach EdgeServer");

    // -- bob connects and should see alice's todo --
    let bob = connect_ws_runtime(schema, &server);

    // Poll bob's local query until the todo synced from alice appears.
    let rows = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            // Kick the runtime so inbound sync messages get processed.
            let _ = bob.flush().await;

            let query = QueryBuilder::new("todos").build();
            let future = bob
                .query(query, None, ReadDurabilityOptions::default())
                .expect("bob query");
            let rows = future.await.expect("bob query future");
            if !rows.is_empty() {
                return rows;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("timed out waiting for bob to see alice's todo");

    assert_eq!(rows.len(), 1, "bob should see exactly one todo");

    let (row_id, values) = &rows[0];
    assert_eq!(
        *row_id, todo_id,
        "row id should match alice's inserted todo"
    );
    // Columns are returned in alphabetical order: completed (Boolean) then title (Text).
    assert_eq!(
        *values,
        vec![Value::Boolean(false), Value::Text("buy milk".to_string()),],
        "bob's row values mismatch, got: {values:?}"
    );
}
