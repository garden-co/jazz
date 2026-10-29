//! The Rust client's tick driver tells recoverable tick failures from fatal
//! ones (INV-API-23): it rides out a recoverable failure with bounded
//! backoff and keeps syncing, and on a fatal failure, or once recovery is
//! exhausted, it stops and every caller gets the error instead of a sync
//! operation that never finishes.

use std::collections::HashMap;
use std::time::Duration;

use jazz::db::ErrorCode;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, JazzError, SchemaBuilder, TableSchema, TransactionId,
    Value,
};
use jazz_server::JazzServer;

fn test_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("todos").column("title", ColumnType::Text))
        .build()
}

/// Run `body` against a client of a fresh server.
async fn with_client<F, Fut>(user: &str, body: F)
where
    F: FnOnce(JazzClient) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let server = JazzServer::start_with_schema(test_schema())
        .await
        .expect("start test server");
    let client = jazz_testkit::connect(server.make_client_context_for_user(test_schema(), user))
        .await
        .expect("connect client");
    body(client).await;
    server.shutdown().await;
}

fn write(client: &JazzClient, title: &str) -> TransactionId {
    let (_, _, tx) = client
        .insert(
            "todos",
            HashMap::from([("title".to_owned(), Value::Text(title.to_owned()))]),
        )
        .expect("insert a todo");
    tx.expect("an ordinary write commits at once")
}

async fn reach_server(client: &JazzClient, tx: TransactionId) -> jazz::tools::Result<()> {
    client
        .wait_for_transaction_with_timeout_for_test(
            tx,
            DurabilityTier::GlobalServer,
            Duration::from_secs(20),
        )
        .await
}

/// Recoverable failures (a full queue, a protocol hiccup) do not stop the
/// driver: it retries with backoff and the write still reaches the server.
///
/// ```text
/// alice ──insert──► local ──tick ✗ backpressure, ✗ protocol, …──tick ✓──► server
/// ```
#[tokio::test]
async fn recoverable_tick_failures_are_retried_until_sync_resumes() {
    tokio::task::LocalSet::new()
        .run_until(with_client("alice-recoverable", |alice| async move {
            alice.fail_next_ticks_for_test([
                ErrorCode::Backpressure,
                ErrorCode::Protocol,
                ErrorCode::Backpressure,
                ErrorCode::NotObserved,
            ]);
            let tx = write(&alice, "rides it out");
            reach_server(&alice, tx)
                .await
                .expect("the driver kept going and the write synced");
            // The client stays usable.
            write(&alice, "still syncing");
        }))
        .await
}

/// A fatal failure stops the driver, and the caller waiting on sync gets
/// it as an error at once instead of waiting forever; later calls report
/// it too.
///
/// ```text
/// alice ──insert──► local ──tick ✗ storage──► driver stops
///   └── wait for server ◄── Err(storage: sync driver stopped)
/// ```
#[tokio::test]
async fn a_fatal_tick_failure_is_reported_to_callers() {
    tokio::task::LocalSet::new()
        .run_until(with_client("alice-fatal", |alice| async move {
            alice.fail_next_ticks_for_test([ErrorCode::Storage]);
            let tx = write(&alice, "never leaves");
            let error = reach_server(&alice, tx)
                .await
                .expect_err("a stopped driver is reported, not waited on");
            assert!(
                matches!(&error, JazzError::Storage(message) if message.contains("sync driver stopped")),
                "{error:?}"
            );
            let later = alice
                .insert(
                    "todos",
                    HashMap::from([("title".to_owned(), Value::Text("after".to_owned()))]),
                )
                .expect_err("later calls report the stopped driver");
            assert!(later.to_string().contains("sync driver stopped"), "{later}");
        }))
        .await
}

/// Recovery is bounded: a recoverable failure that keeps coming stops the
/// driver like a fatal one.
#[tokio::test]
async fn exhausted_recovery_is_reported_to_callers() {
    tokio::task::LocalSet::new()
        .run_until(with_client("alice-exhausted", |alice| async move {
            alice.fail_next_ticks_for_test(std::iter::repeat_n(ErrorCode::Protocol, 64));
            let tx = write(&alice, "never leaves");
            let error = reach_server(&alice, tx)
                .await
                .expect_err("recovery that never succeeds is reported");
            assert!(
                matches!(&error, JazzError::Sync(message) if message.contains("sync driver stopped")),
                "{error:?}"
            );
        }))
        .await
}
