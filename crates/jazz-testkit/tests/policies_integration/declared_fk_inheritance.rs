use crate::JazzClient;
use jazz::tools::{DurabilityTier, TransactionId};
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_edge_txs};

use super::*;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn insert_file(client: &JazzClient, owner_id: &str, name: &str) -> (ObjectId, TransactionId) {
    let (id, _, transaction_id) = client
        .insert(
            "files",
            crate::row_input!("owner_id" => owner_id, "name" => name),
        )
        .expect("insert file");
    (
        id,
        transaction_id.expect("file insert should commit immediately"),
    )
}

fn insert_todo_with_image(
    client: &JazzClient,
    owner_id: &str,
    title: &str,
    image: impl Into<Value>,
) -> (ObjectId, TransactionId) {
    let image = image.into();
    let (id, _, transaction_id) = client
        .insert(
            "todos",
            crate::row_input!("owner_id" => owner_id, "title" => title, "image" => image),
        )
        .expect("insert todo");
    (
        id,
        transaction_id.expect("todo insert should commit immediately"),
    )
}

fn insert_todo_with_images(
    client: &JazzClient,
    owner_id: &str,
    title: &str,
    images: Vec<Value>,
) -> (ObjectId, TransactionId) {
    let (id, _, transaction_id) = client
        .insert(
            "todos",
            crate::row_input!(
                "owner_id" => owner_id,
                "title" => title,
                "images" => Value::Array(images),
            ),
        )
        .expect("insert todo");
    (
        id,
        transaction_id.expect("todo insert should commit immediately"),
    )
}

async fn query_ids(client: &JazzClient, table: &str) -> HashSet<ObjectId> {
    client
        .query(Query::from(table), Some(DurabilityTier::EdgeServer))
        .await
        .expect("query rows")
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

/// Verifies that declared reverse-FK inheritance can grant SELECT on a target
/// row when the current session owns a row that references it.
#[tokio::test]
async fn rebac_declared_fk_inheritance_grants_select_access() {
    tokio::task::LocalSet::new()
        .run_until(rebac_declared_fk_inheritance_grants_select_access_inner())
        .await;
}

async fn rebac_declared_fk_inheritance_grants_select_access_inner() {
    let schema = declared_file_inheritance_schema(false);
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "declared-fk-admin",
        "files",
        READY_TIMEOUT,
    )
    .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "files", READY_TIMEOUT).await;

    let (file_id, file_tx) = insert_file(&admin, super::BOB_ID, "bob-file");
    let (_, todo_tx) = insert_todo_with_image(&admin, super::ALICE_ID, "todo", file_id);
    wait_for_edge_txs(&admin, &[file_tx, todo_tx]).await;

    let visible_ids = query_ids(&alice, "files").await;

    assert!(
        visible_ids.contains(&file_id),
        "alice should see file via allowedTo.readReferencing(policy.todos, \"image\")"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that declared reverse-FK inheritance can grant UPDATE on a target
/// row through a visible referencing row.
#[tokio::test]
#[ignore = "#1762: an inherited-visible row is rejected locally on update with `read policy denied UPSERT`"]
async fn rebac_declared_fk_inheritance_grants_update_access() {
    tokio::task::LocalSet::new()
        .run_until(rebac_declared_fk_inheritance_grants_update_access_inner())
        .await;
}

async fn rebac_declared_fk_inheritance_grants_update_access_inner() {
    let schema = declared_file_inheritance_schema(false);
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "declared-fk-admin",
        "files",
        READY_TIMEOUT,
    )
    .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "files", READY_TIMEOUT).await;

    let (file_id, file_tx) = insert_file(&admin, super::BOB_ID, "bob-file");
    let (_, todo_tx) = insert_todo_with_image(&admin, super::ALICE_ID, "todo", file_id);
    wait_for_edge_txs(&admin, &[file_tx, todo_tx]).await;
    assert!(query_ids(&alice, "files").await.contains(&file_id));

    let update = alice.update(
        file_id,
        vec![
            ("owner_id".into(), Value::Text(super::BOB_ID.into())),
            ("name".into(), Value::Text("updated by alice".into())),
        ],
    );
    assert!(
        update.is_ok(),
        "alice should update file via declared inherited access from todos row: {update:?}"
    );
    wait_for_edge_txs(
        &alice,
        &[update
            .expect("checked above")
            .expect("file update should commit immediately")],
    )
    .await;

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that declared reverse-FK inheritance also works for UUID-array
/// reference columns, including duplicate target ids in the array.
#[tokio::test]
async fn rebac_declared_fk_inheritance_array_membership_grants_access() {
    tokio::task::LocalSet::new()
        .run_until(rebac_declared_fk_inheritance_array_membership_grants_access_inner())
        .await;
}

async fn rebac_declared_fk_inheritance_array_membership_grants_access_inner() {
    let schema = declared_file_inheritance_schema(true);
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "declared-fk-admin",
        "files",
        READY_TIMEOUT,
    )
    .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "files", READY_TIMEOUT).await;

    let (file_id, file_tx) = insert_file(&admin, super::BOB_ID, "array-file");
    let (_, todo_tx) = insert_todo_with_images(
        &admin,
        super::ALICE_ID,
        "todo",
        vec![Value::Uuid(file_id), Value::Uuid(file_id)],
    );
    wait_for_edge_txs(&admin, &[file_tx, todo_tx]).await;

    let visible_ids = query_ids(&alice, "files").await;

    assert!(
        visible_ids.contains(&file_id),
        "array FK membership should grant inherited access when target id is present"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that cyclic declared reverse-FK inheritance fails closed instead
/// of recursively granting access through the cycle.
#[tokio::test]
#[ignore = "#1763: recursive reverse-FK policy cycles currently overflow the Rust evaluator stack"]
async fn rebac_declared_fk_inheritance_cycle_fails_closed() {
    tokio::task::LocalSet::new()
        .run_until(rebac_declared_fk_inheritance_cycle_fails_closed_inner())
        .await;
}

async fn rebac_declared_fk_inheritance_cycle_fails_closed_inner() {
    let a_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session(vec!["claims", "sub"])),
            pe::allowed_to_read_referencing("table_b", "a_id"),
        ]));
    });
    let b_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session(vec!["claims", "sub"])),
            pe::allowed_to_read_referencing("table_a", "b_id"),
        ]));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("table_a")
                .column("owner_id", ColumnType::Text)
                .nullable_fk_column("b_id", "table_b")
                .policies(a_policies),
        )
        .table(
            TableSchema::builder("table_b")
                .column("owner_id", ColumnType::Text)
                .nullable_fk_column("a_id", "table_a")
                .policies(b_policies),
        )
        .build();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "declared-cycle-admin",
        "table_a",
        READY_TIMEOUT,
    )
    .await;
    let alice =
        connect_ready_user(&server, &schema, super::ALICE_ID, "table_a", READY_TIMEOUT).await;

    let (a_id, _, a_tx) = admin
        .insert(
            "table_a",
            crate::row_input!("owner_id" => super::BOB_ID, "b_id" => Value::Null),
        )
        .expect("insert table_a");
    let (b_id, _, b_tx) = admin
        .insert(
            "table_b",
            crate::row_input!("owner_id" => super::CAROL_ID, "a_id" => a_id),
        )
        .expect("insert table_b");
    wait_for_edge_txs(
        &admin,
        &[
            a_tx.expect("table_a insert should commit immediately"),
            b_tx.expect("table_b insert should commit immediately"),
        ],
    )
    .await;

    let link_tx = admin
        .update(a_id, vec![("b_id".into(), Value::Uuid(b_id))])
        .expect("link table_a")
        .expect("table_a update should commit immediately");
    wait_for_edge_txs(&admin, &[link_tx]).await;

    let visible_ids = query_ids(&alice, "table_a").await;

    assert!(
        visible_ids.is_empty(),
        "cycle path should fail closed and not grant access"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that access through a declared reverse-FK path is re-evaluated
/// when the referencing FK column changes from NULL to a target id.
#[tokio::test]
#[ignore = "#1762: an accepted referencing-FK update does not re-evaluate the inherited target view"]
async fn rebac_declared_fk_inheritance_reacts_to_fk_updates() {
    tokio::task::LocalSet::new()
        .run_until(rebac_declared_fk_inheritance_reacts_to_fk_updates_inner())
        .await;
}

async fn rebac_declared_fk_inheritance_reacts_to_fk_updates_inner() {
    let schema = declared_file_inheritance_schema(false);
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "declared-fk-admin",
        "files",
        READY_TIMEOUT,
    )
    .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "files", READY_TIMEOUT).await;

    let (file_id, file_tx) = insert_file(&admin, super::BOB_ID, "delayed-link");
    let (todo_id, todo_tx) = insert_todo_with_image(&admin, super::ALICE_ID, "todo", Value::Null);
    wait_for_edge_txs(&admin, &[file_tx, todo_tx]).await;

    let initially_visible = query_ids(&alice, "files").await;
    assert!(
        !initially_visible.contains(&file_id),
        "file should be hidden before an inheriting reference exists"
    );

    assert!(
        query_ids(&alice, "todos").await.contains(&todo_id),
        "alice should see her todo before updating its image"
    );
    let update_tx = alice
        .update(todo_id, vec![("image".into(), Value::Uuid(file_id))])
        .expect("link todo image")
        .expect("todo update should commit immediately");
    wait_for_edge_txs(&alice, &[update_tx]).await;

    let visible_after_link = query_ids(&alice, "files").await;
    assert!(
        visible_after_link.contains(&file_id),
        "updating referencing FK should re-evaluate and grant access to linked target row"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}
