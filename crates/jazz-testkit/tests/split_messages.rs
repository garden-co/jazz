//! Messages larger than one 2 MiB wire frame travel in parts through the
//! server runtime, in both directions.

use std::time::Duration;

use jazz::query::Query;
use jazz::row_input;
use jazz::tools::sync::ReadTier;
use jazz::tools::{ColumnType, Schema, SchemaBuilder, TableSchema, WriteContext, permissions};
use jazz_server::JazzServer;
use jazz_testkit::{TestingClient, has_added_id, wait_for_subscription_update};

const TIMEOUT: Duration = Duration::from_secs(30);
const ROWS: usize = 100;
const BODY: usize = 30_000;

fn schema() -> Schema {
    let table = TableSchema::builder("notes")
        .column("body", ColumnType::Text)
        .policies(permissions(|p| {
            p.allow_insert().always();
            p.allow_read().always();
        }));
    SchemaBuilder::new().table(table).build()
}

/// An untrusted client's commit of about 3 MB leaves in parts, which the
/// server joins (within the session's budget) and serves to another client
/// the same way.
#[tokio::test]
async fn a_commit_larger_than_a_frame_reaches_the_server_and_another_client() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = schema();
            let authority = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let alice = TestingClient::builder()
                .with_server(&authority)
                .with_schema(schema.clone())
                .with_user_id("alice")
                .as_user()
                .connect()
                .await;
            let bob = TestingClient::builder()
                .with_server(&authority)
                .with_schema(schema)
                .with_user_id("bob")
                .as_user()
                .connect()
                .await;

            let tx = alice.begin_transaction().unwrap().transaction_id();
            let staged = alice.with_write_context(WriteContext::default().with_transaction_id(tx));
            let body = "x".repeat(BODY);
            let mut notes = Vec::new();
            for _ in 0..ROWS {
                let (note, _, _) = staged
                    .insert("notes", row_input!("body" => body.as_str()))
                    .unwrap();
                notes.push(note);
            }
            let commit = alice.commit_transaction(tx).unwrap();
            jazz_testkit::wait_for_global_txs(&alice, &[commit]).await;

            let mut read = bob
                .subscribe_with_read_tier(Query::from("notes"), ReadTier::Remote)
                .await
                .unwrap();
            let mut log = Vec::new();
            wait_for_subscription_update(
                &mut read,
                &mut log,
                TIMEOUT,
                "bob reads every note",
                |log| notes.iter().all(|note| has_added_id(log, *note)),
            )
            .await;

            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
            authority.shutdown().await;
        })
        .await;
}
