//! Durable local writes made while the upstream is unavailable keep their
//! upstream settlement target: they stay pending for replay instead of
//! silently settling at the local tier, and reach the server on reconnect.

use std::time::Duration;

use jazz::row_input;
use jazz::tools::test_support::{disconnect_client, reconnect_client};
use jazz::tools::{ColumnType, DurabilityTier, Schema, SchemaBuilder, TableSchema};
use jazz_server::JazzServer;
use jazz_testkit as support;
use support::{TestingClient, has_row, wait_for_rows};

// Bounded wait proving a durability tier has not been reached. // ms.
const NOT_REACHED_WINDOW: Duration = Duration::from_millis(1200);

fn document_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("documents").column("title", ColumnType::Text))
        .build()
}

/// A locally durable write made while the upstream transport is down keeps
/// its upstream settlement target: it reaches local durability, does not
/// report global durability while disconnected, and after the upstream
/// transport is restored the write replays and settles at the global tier —
/// observable both through the writer's durability wait and through a second
/// client reading from the server.
///
/// Actors: alice writes while offline; bob reads through the server.
///
/// ```text
/// alice ─offline─ insert ──► local store (durable, pending upstream)
///   │      wait(Local) ✓        wait(Global) ✗ (still pending)
///   └─reconnect──► server ──accept──► wait(Global) ✓ ──► bob sees the row
/// ```
#[tokio::test]
async fn offline_durable_write_keeps_global_target_and_replays_on_reconnect() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = document_schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("alice-offline-writer")
                .with_persistent_storage()
                .ready_on("documents", Duration::from_secs(30))
                .connect()
                .await;

            assert!(disconnect_client(&alice), "detach the live transport");

            let (document_id, expected_values, transaction_id) = alice
                .insert("documents", row_input!("title" => "written while offline"))
                .expect("insert document while offline");
            let transaction_id = transaction_id.expect("ordinary mutation commits immediately");
            alice
                .wait_for_transaction(transaction_id, DurabilityTier::Local)
                .await
                .expect("offline write reaches local durability");

            // The write must not report global durability while the upstream
            // transport is down; the bounded wait expires instead.
            let premature = alice
                .wait_for_transaction_with_timeout_for_test(
                    transaction_id,
                    DurabilityTier::GlobalServer,
                    NOT_REACHED_WINDOW,
                )
                .await;
            assert!(
                premature.is_err(),
                "a disconnected client must not report global durability for \
                 a pending local write: {premature:?}"
            );

            assert!(
                reconnect_client(&alice)
                    .await
                    .expect("reconnect once the upstream is reachable"),
                "the preserved client state must reattach"
            );
            alice
                .wait_for_transaction(transaction_id, DurabilityTier::GlobalServer)
                .await
                .expect("replayed write settles at the global tier");

            let bob = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("bob-server-reader")
                .ready_on("documents", Duration::from_secs(30))
                .connect()
                .await;
            wait_for_rows(
                &bob,
                jazz::query::Query::from("documents"),
                "bob sees alice's replayed write through the server",
                |rows| has_row(&rows, document_id, &expected_values).then_some(()),
            )
            .await;

            bob.shutdown().await.expect("shutdown bob");
            alice.shutdown().await.expect("shutdown alice");
            server.shutdown().await;
        })
        .await;
}

/// Explicitly detaching an upstream preserves a pending Global durability
/// wait. Local durability remains available, and the original Global wait
/// completes only after a replacement authority accepts the replayed write.
/// The public `Db` API provides the exact detach operation and lets this test
/// withhold authority processing; the client facade only exposes transport
/// disconnects.
///
/// Actors: alice writes locally, then reconnects to the authority.
///
/// ```text
/// alice ──insert──► local store   wait(Global) pending
///   ├──detach upstream──────────► wait(Local) ✓, wait(Global) pending
///   ├──attach new upstream──────► wait(Global) pending
///   └──replay──► authority ──accept──► original wait(Global) ✓
/// ```
#[test]
fn detaching_the_upstream_keeps_global_wait_pending_until_reconnected_authority_confirms() {
    use std::cell::Cell;
    use std::rc::Rc;

    use jazz::db::{Db, DbConfig, DbIdentity, block_on};
    use jazz::groove::records::Value;
    use jazz::groove::storage::MemoryStorage;
    use jazz::ids::{AuthorSubject, NodeUuid};
    use jazz::schema::JazzSchema;
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchema};
    use jazz::tx::{DurabilityTier, Fate};
    use jazz_testkit::duplex_transport::duplex;

    let source = SchemaBuilder::new()
        .table(TableSchema::builder("documents").column("title", ColumnType::Text))
        .build();
    let schema = JazzSchema::new(&source).expect("reconnect durability public schema compiles");
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let node = block_on(Db::open(DbConfig::new(
        schema.clone(),
        MemoryStorage::new(&refs).expect("valid memory storage families"),
        DbIdentity {
            node: NodeUuid::from_bytes([0x51; 16]),
            author: AuthorSubject::for_test_bytes([0xa9; 16]),
        },
    )))
    .expect("open database");

    let (upstream_transport, _held_far_end) = duplex();
    let upstream = block_on(node.connect_upstream(upstream_transport));

    let write = block_on(node.insert(
        "documents",
        std::collections::BTreeMap::from([(
            "title".to_owned(),
            Value::String("pending for a silent upstream".to_owned()),
        )]),
        Default::default(),
    ))
    .expect("insert local document");
    let tx_id = write.mergeable_tx_id();
    assert_eq!(
        node.write_state(tx_id)
            .expect("local write state")
            .durability,
        DurabilityTier::Local,
        "the write must be locally durable immediately"
    );

    let global_wait = Rc::new(Cell::new(None));
    let observed_wait = Rc::clone(&global_wait);
    node.wait_for_transaction_with(tx_id, DurabilityTier::Global, move |result| {
        observed_wait.set(Some(result.is_ok()));
    });
    for _ in 0..3 {
        block_on(node.tick()).expect("queue the write for the silent upstream");
    }
    assert_eq!(
        global_wait.get(),
        None,
        "the global wait must stay pending while the upstream is installed"
    );

    assert!(node.detach_connection(&upstream));
    for _ in 0..3 {
        block_on(node.tick()).expect("process the detached connection");
    }
    assert_eq!(
        global_wait.get(),
        None,
        "detaching must not complete or downgrade the pending Global wait"
    );
    assert_eq!(
        block_on(node.wait_for_transaction(tx_id, DurabilityTier::Local))
            .expect("Local durability remains available after detach"),
        tx_id,
    );
    let state = node.write_state(tx_id).expect("write state after detach");
    assert_eq!(state.fate, Fate::Pending);
    assert_eq!(state.durability, DurabilityTier::Local);
    assert_eq!(state.global_time, None);

    let authority = block_on(Db::open_history_complete(DbConfig::new(
        schema,
        MemoryStorage::new(&refs).expect("valid authority storage families"),
        DbIdentity {
            node: NodeUuid::from_bytes([0x52; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open authority database");
    let (replacement_transport, authority_transport) = duplex();
    let _replacement = block_on(node.connect_upstream(replacement_transport));
    let _subscriber = authority.accept_subscriber(
        authority_transport,
        AuthorSubject::for_test_bytes([0xa9; 16]),
    );
    for _ in 0..3 {
        block_on(node.tick()).expect("queue replay without an authority response");
    }
    assert_eq!(
        global_wait.get(),
        None,
        "attaching a replacement must not satisfy Global without confirmation"
    );

    for _ in 0..100 {
        block_on(authority.tick()).expect("authority processes the replayed write");
        block_on(node.tick()).expect("alice receives authority confirmation");
        if global_wait.get().is_some() {
            break;
        }
    }
    assert_eq!(
        global_wait.get(),
        Some(true),
        "the original Global wait must succeed after authority confirmation"
    );
    let state = node.write_state(tx_id).expect("confirmed write state");
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.durability, DurabilityTier::Global);
    assert!(
        state.global_time.is_some(),
        "Global requires an authority timestamp"
    );
}
