use jazz::tools::DurabilityTier;
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_edge_txs};

use super::*;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Verifies that write contexts can explicitly override `$updatedAt` while
/// preserving the original creator and creation timestamp.
#[tokio::test]
async fn provenance_magic_columns_allow_explicit_updated_at_override() {
    tokio::task::LocalSet::new()
        .run_until(provenance_magic_columns_allow_explicit_updated_at_override_inner())
        .await;
}

async fn provenance_magic_columns_allow_explicit_updated_at_override_inner() {
    let schema = provenance_notes_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let client =
        connect_ready_client(&server, &schema, "provenance-admin", "notes", READY_TIMEOUT).await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;

    let (note, _, note_tx) = alice
        .insert("notes", crate::row_input!("title" => "draft"))
        .expect("alice-authored note should insert");
    wait_for_edge_txs(
        &alice,
        &[note_tx.expect("alice note should commit immediately")],
    )
    .await;

    let initial = client
        .query(
            Query::from("notes")
                .filter(eq(col("title"), lit("draft")))
                .select(["$createdAt", "$updatedAt"]),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query initial note timestamps");
    assert_eq!(initial.len(), 1, "draft note should be queryable");
    let Value::Timestamp(initial_created_at) = initial[0].1[0] else {
        panic!("$createdAt should decode as a timestamp")
    };

    let custom_updated_at = initial_created_at + 10_000;
    let backfill = WriteContext::default().with_updated_at(custom_updated_at);

    let update_tx = alice
        .with_write_context(backfill)
        .update(
            note,
            vec![("title".into(), Value::Text("backfilled".into()))],
        )
        .expect("explicit updated_at override should succeed")
        .expect("backfill update should commit immediately");
    wait_for_edge_txs(&alice, &[update_tx]).await;

    let updated = client
        .query(
            Query::from("notes")
                .filter(eq(col("title"), lit("backfilled")))
                .select([
                    "title",
                    "$createdBy",
                    "$updatedBy",
                    "$createdAt",
                    "$updatedAt",
                ]),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query backfilled note");
    assert_eq!(updated.len(), 1, "backfilled note should remain queryable");
    assert_eq!(updated[0].1[0], Value::Text("backfilled".into()));
    assert_eq!(updated[0].1[1], Value::Text(super::ALICE_ID.into()));
    assert_eq!(updated[0].1[2], Value::Text(super::ALICE_ID.into()));
    let Value::Timestamp(updated_created_at) = updated[0].1[3] else {
        panic!("updated $createdAt should decode as a timestamp")
    };
    let Value::Timestamp(updated_updated_at) = updated[0].1[4] else {
        panic!("updated $updatedAt should decode as a timestamp")
    };
    assert_eq!(updated_created_at, initial_created_at);
    assert_eq!(updated_updated_at, custom_updated_at);

    client.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}
