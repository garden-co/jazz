#![cfg(feature = "test-utils")]

use std::time::Duration;

mod support;

use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnDescriptor, ColumnType, DurabilityTier, JazzClient, LargeValueKind, ObjectId,
    QueryBuilder, RowDescriptor, Schema, Session, TableName, TableSchema, TextEdit, Value,
};
use support::{TestingClient, wait_for_query};

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

async fn wait_for_body(client: &JazzClient, row: ObjectId, body: impl Into<Vec<u8>>, label: &str) {
    let body = body.into();
    wait_for_query(
        client,
        docs_query(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        label,
        move |rows| {
            rows.iter()
                .any(|(id, values)| *id == row && values == &vec![Value::Bytea(body.clone())])
                .then_some(rows)
        },
    )
    .await;
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
