use jazz::tools::DurabilityTier;
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_edge_txs};

use super::*;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn attributed_to(principal: &str) -> WriteContext {
    WriteContext {
        attribution: Some(principal.into()),
        ..Default::default()
    }
}

/// Verifies provenance magic columns for normal session writes, backend
/// attribution, timestamps, query filters, and system-authored writes.
#[tokio::test]
#[ignore = "WriteContext attribution is not applied by the synced Rust client, so $updatedBy is not the requested principal"]
async fn provenance_magic_columns_capture_insert_update_and_system_authors() {
    tokio::task::LocalSet::new()
        .run_until(provenance_magic_columns_capture_insert_update_and_system_authors_inner())
        .await;
}

async fn provenance_magic_columns_capture_insert_update_and_system_authors_inner() {
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
        .expect("query initial note");
    assert_eq!(initial.len(), 1, "draft note should be queryable");
    assert_eq!(
        initial[0].1[0],
        Value::Text("draft".into()),
        "projected title should decode"
    );
    assert_eq!(initial[0].1[1], Value::Text(super::ALICE_ID.into()));
    assert_eq!(initial[0].1[2], Value::Text(super::ALICE_ID.into()));
    let Value::Timestamp(initial_created_at) = initial[0].1[3] else {
        panic!("$createdAt should decode as a timestamp")
    };
    let Value::Timestamp(initial_updated_at) = initial[0].1[4] else {
        panic!("$updatedAt should decode as a timestamp")
    };
    assert_eq!(
        initial_created_at, initial_updated_at,
        "fresh inserts should initialize created/updated timestamps together"
    );

    let update_tx = client
        .with_write_context(attributed_to(super::BOB_ID))
        .update(note, vec![("title".into(), Value::Text("revised".into()))])
        .expect("attributed update should succeed without a session")
        .expect("attributed update should commit immediately");
    wait_for_edge_txs(&client, &[update_tx]).await;

    let updated = client
        .query(
            Query::from("notes")
                .filter(eq(col("title"), lit("revised")))
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
        .expect("query updated note");
    assert_eq!(updated.len(), 1, "updated note should remain queryable");
    assert_eq!(updated[0].1[0], Value::Text("revised".into()));
    assert_eq!(updated[0].1[1], Value::Text(super::ALICE_ID.into()));
    assert_eq!(updated[0].1[2], Value::Text(super::BOB_ID.into()));
    let Value::Timestamp(updated_created_at) = updated[0].1[3] else {
        panic!("updated $createdAt should decode as a timestamp")
    };
    let Value::Timestamp(updated_updated_at) = updated[0].1[4] else {
        panic!("updated $updatedAt should decode as a timestamp")
    };
    assert_eq!(
        updated_created_at, initial_created_at,
        "created_at should be preserved across updates"
    );
    assert!(
        updated_updated_at >= initial_updated_at,
        "updated_at should move forward on update"
    );

    let updated_by_bob = client
        .query(
            Query::from("notes")
                .filter(eq(col("$updatedBy"), lit(super::BOB_ID)))
                .select(["title", "$updatedBy"]),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query notes updated by bob");
    assert_eq!(updated_by_bob.len(), 1);
    assert_eq!(
        updated_by_bob[0].1,
        vec![
            Value::Text("revised".into()),
            Value::Text(super::BOB_ID.into())
        ]
    );

    let system_tx = client
        .insert("notes", crate::row_input!("title" => "system note"))
        .expect("system-authored note should insert without a session")
        .2
        .expect("system note should commit immediately");
    wait_for_edge_txs(&client, &[system_tx]).await;
    let system = client
        .query(
            Query::from("notes")
                .filter(eq(col("title"), lit("system note")))
                .select(["title", "$createdBy", "$updatedBy"]),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query system-authored note");
    assert_eq!(system.len(), 1);
    assert_eq!(
        system[0].1,
        vec![
            Value::Text("system note".into()),
            Value::Text(SYSTEM_PRINCIPAL_ID.into()),
            Value::Text(SYSTEM_PRINCIPAL_ID.into()),
        ]
    );

    client.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that write contexts can explicitly override `$updatedAt` while
/// preserving the original creator and creation timestamp.
#[tokio::test]
#[ignore = "WriteContext attribution and updated_at overrides are not applied by the synced Rust client"]
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
    let bob_backfill = WriteContext {
        updated_at: Some(custom_updated_at),
        ..attributed_to(super::BOB_ID)
    };

    let update_tx = client
        .with_write_context(bob_backfill)
        .update(
            note,
            vec![("title".into(), Value::Text("backfilled".into()))],
        )
        .expect("explicit updated_at override should succeed")
        .expect("backfill update should commit immediately");
    wait_for_edge_txs(&client, &[update_tx]).await;

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
    assert_eq!(updated[0].1[2], Value::Text(super::BOB_ID.into()));
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

/// Verifies `$createdBy`-based row policies: creators can read/update/delete
/// their rows, backend-attributed rows behave as creator-owned, and system rows stay hidden.
#[tokio::test]
#[ignore = "trusted backend attribution is ignored by the synced Rust client, so backend-attributed rows are not creator-owned"]
async fn created_by_permissions_allow_creators_and_hide_system_rows() {
    tokio::task::LocalSet::new()
        .run_until(created_by_permissions_allow_creators_and_hide_system_rows_inner())
        .await;
}

async fn created_by_permissions_allow_creators_and_hide_system_rows_inner() {
    let schema = authorship_permissions_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let client =
        connect_ready_client(&server, &schema, "provenance-admin", "notes", READY_TIMEOUT).await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "notes", READY_TIMEOUT).await;

    let (alice_owned, _, alice_owned_tx) = alice
        .insert("notes", crate::row_input!("title" => "alice-owned"))
        .expect("creator-based insert policy should allow alice");
    let (alice_attributed, _, attributed_tx) = client
        .with_write_context(attributed_to(super::ALICE_ID))
        .insert("notes", crate::row_input!("title" => "alice-attributed"))
        .expect("backend-attributed note should stamp alice as creator");
    let system_tx = client
        .insert("notes", crate::row_input!("title" => "system-owned"))
        .expect("system note should insert")
        .2
        .expect("system note should commit immediately");
    wait_for_edge_txs(
        &alice,
        &[alice_owned_tx.expect("alice note should commit immediately")],
    )
    .await;
    wait_for_edge_txs(
        &client,
        &[
            attributed_tx.expect("attributed note should commit immediately"),
            system_tx,
        ],
    )
    .await;

    let alice_visible = alice
        .query(
            Query::from("notes")
                .select(["title", "$createdBy"])
                .order_by("title", jazz::query::OrderDirection::Asc),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query notes as alice");
    assert_eq!(
        alice_visible
            .iter()
            .map(|(_, values)| values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("alice-attributed".into()),
                Value::Text(super::ALICE_ID.into()),
            ],
            vec![
                Value::Text("alice-owned".into()),
                Value::Text(super::ALICE_ID.into())
            ],
        ],
        "alice should only see notes authored as alice"
    );

    let bob_visible = bob
        .query(
            Query::from("notes").select(["title"]),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query notes as bob");
    assert!(
        bob_visible.is_empty(),
        "bob should not see alice/system notes"
    );

    let alice_update_tx = alice
        .update(
            alice_attributed,
            vec![(
                "title".into(),
                Value::Text("alice-attributed-updated".into()),
            )],
        )
        .expect("creator should be able to update attributed rows")
        .expect("creator update should commit immediately");
    let alice_delete_tx = alice
        .delete(alice_owned)
        .expect("creator should be able to delete her own row")
        .expect("creator delete should commit immediately");
    wait_for_edge_txs(&alice, &[alice_update_tx, alice_delete_tx]).await;

    let alice_after_mutations = alice
        .query(
            Query::from("notes")
                .select(["title"])
                .order_by("title", jazz::query::OrderDirection::Asc),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query notes as alice after mutations");
    assert_eq!(
        alice_after_mutations
            .iter()
            .map(|(_, values)| values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Text("alice-attributed-updated".into())],
        "alice should retain access to the surviving creator-owned row"
    );

    client.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
