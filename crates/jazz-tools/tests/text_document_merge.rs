#![cfg(feature = "test-utils")]

use std::time::Duration;

mod support;

use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::test_support::{disconnect_client, reconnect_client};
use jazz_tools::{
    ColumnDescriptor, ColumnType, DurabilityTier, JazzClient, LargeValueKind, ObjectId,
    QueryBuilder, RowDescriptor, Schema, Session, TableName, TableSchema, TextEdit, Value,
};
use support::TestingClient;

fn text_doc_schema() -> Schema {
    [(
        TableName::new("docs"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("body", ColumnType::Bytea).large_value(LargeValueKind::Text),
        ])),
    )]
    .into_iter()
    .collect()
}

fn docs_query() -> jazz_tools::Query {
    QueryBuilder::new("docs").select(&["body"]).build()
}

async fn connect(server: &JazzServer, user: &str) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(text_doc_schema())
        .with_user_id(user)
        .ready_on("docs", Duration::from_secs(30))
        .connect()
        .await
}

async fn connect_admin(server: &JazzServer, user: &str) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(text_doc_schema())
        .with_user_id(user)
        .as_admin()
        .ready_on("docs", Duration::from_secs(30))
        .connect()
        .await
}

async fn wait_for_body(client: &JazzClient, row: ObjectId, body: impl Into<Vec<u8>>, label: &str) {
    wait_for_body_at(client, row, body, None, label).await;
}

async fn wait_for_body_at(
    client: &JazzClient,
    row: ObjectId,
    body: impl Into<Vec<u8>>,
    durability: Option<DurabilityTier>,
    label: &str,
) {
    let body = body.into();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut last_seen = None;
    loop {
        let rows = match client.query(docs_query(), durability).await {
            Ok(rows) => rows,
            Err(error) => {
                #[cfg(feature = "sync-autopsy")]
                let autopsy = jazz::db::sync_autopsy::dump();
                #[cfg(not(feature = "sync-autopsy"))]
                let autopsy = String::new();
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting for {label}; last seen: {:?}; last error: {}\n{}",
                    last_seen,
                    error,
                    autopsy
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        for (id, values) in rows.iter() {
            if *id != row {
                continue;
            }
            let Some(Value::LargeValue(handle)) = values.first() else {
                continue;
            };
            if client.hydrate_large_value(handle).await.is_ok_and(|bytes| {
                last_seen = Some(bytes.clone());
                bytes == body
            }) {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {label}; last seen: {:?}\n{}",
            last_seen,
            {
                #[cfg(feature = "sync-autopsy")]
                {
                    jazz::db::sync_autopsy::dump()
                }
                #[cfg(not(feature = "sync-autopsy"))]
                {
                    String::new()
                }
            }
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
/// Exercises byte-exact sequential text edit replay for alice typing into one
/// document.
///
/// alice --insert empty doc--> server
/// alice --one-byte text edits in order--> server --materialized text--> alice
async fn sequential_text_typing_round_trips_byte_exact() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start_with_schema(text_doc_schema()).await;
            let alice_id = "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaac001";
            let alice = connect(&server, alice_id).await;

            let (doc, _, batch) = alice
                .for_session(Session::new(alice_id))
                .insert("docs", row_input!("body" => b"".to_vec()))
                .expect("insert doc");
            alice
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("base settles");

            let mut expected = Vec::new();
            for byte in b"typed as one-byte runs" {
                let batch = alice
                    .edit_text(doc, "body", TextEdit::new().insert(expected.len(), [*byte]))
                    .expect("type byte");
                expected.push(*byte);
                alice
                    .wait_for_batch(batch, DurabilityTier::EdgeServer)
                    .await
                    .expect("edit settles");
            }

            wait_for_body(&alice, doc, expected, "typed bytes round-trip").await;
            alice.shutdown().await.expect("shutdown alice");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
/// Upgrades the core topology concurrent text-merge shape to the public API:
/// bob keeps his local sync state while offline, edits the same base text as
/// alice, then reconnects and both clients observe the deterministic merge.
async fn offline_concurrent_text_edits_reconnect_and_converge() {
    tokio::task::LocalSet::new()
        .run_until(async {
            #[cfg(feature = "sync-autopsy")]
            jazz::db::sync_autopsy::enable();
            let server = JazzServer::start_with_schema(text_doc_schema()).await;
            let alice_id = "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaac101";
            let bob_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbc102";
            let alice = connect_admin(&server, alice_id).await;
            let bob = connect_admin(&server, bob_id).await;
            let mut _alice_docs = alice
                .subscribe(docs_query())
                .await
                .expect("alice subscribes");
            let mut _bob_docs = bob.subscribe(docs_query()).await.expect("bob subscribes");

            let doc = ObjectId::new();
            let (_, _, bob_base_batch) = bob
                .for_session(Session::new(bob_id))
                .insert_with_id("docs", *doc.uuid(), row_input!("body" => b"".to_vec()))
                .expect("bob inserts base doc");
            bob.wait_for_batch(bob_base_batch, DurabilityTier::EdgeServer)
                .await
                .expect("bob base settles");
            wait_for_body_at(
                &alice,
                doc,
                b"".to_vec(),
                Some(DurabilityTier::EdgeServer),
                "alice observes shared base",
            )
            .await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            assert!(disconnect_client(&bob), "bob disconnects from upstream");
            assert!(!bob.is_connected(), "bob reports offline");

            let alice_batch = alice
                .edit_text(doc, "body", TextEdit::new().insert(0, b"LEFT"))
                .expect("alice edits text");
            let bob_batch = bob
                .edit_text(doc, "body", TextEdit::new().insert(0, b"RIGHT"))
                .expect("bob edits text offline");
            bob.wait_for_batch(bob_batch, DurabilityTier::Local)
                .await
                .expect("bob local edit recorded");
            alice
                .wait_for_batch(alice_batch, DurabilityTier::EdgeServer)
                .await
                .expect("alice edit settles before bob reconnects");

            assert!(reconnect_client(&bob).await.expect("bob reconnects"));
            assert!(bob.is_connected(), "bob reports online");
            bob.wait_for_batch(bob_batch, DurabilityTier::EdgeServer)
                .await
                .expect("bob offline edit settles after reconnect");

            let expected = b"LEFTRIGHT".to_vec();
            wait_for_body(&alice, doc, expected.clone(), "alice observes text merge").await;
            wait_for_body(&bob, doc, expected, "bob observes text merge").await;

            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            server.shutdown().await;
        })
        .await;
}
