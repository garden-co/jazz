use crate::JazzClient;
use jazz::tools::TransactionId;
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_global_txs};

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
        .query(Query::from(table), jazz::tools::ReadTier::Remote)
        .await
        .map(jazz::tools::test_support::ordinary_rows)
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
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
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
    wait_for_global_txs(&admin, &[file_tx, todo_tx]).await;

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
async fn rebac_declared_fk_inheritance_grants_update_access() {
    tokio::task::LocalSet::new()
        .run_until(rebac_declared_fk_inheritance_grants_update_access_inner())
        .await;
}

async fn rebac_declared_fk_inheritance_grants_update_access_inner() {
    let schema = declared_file_inheritance_schema(false);
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
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
    wait_for_global_txs(&admin, &[file_tx, todo_tx]).await;
    assert!(query_ids(&alice, "files").await.contains(&file_id));

    let update = alice.update(
        "files",
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
    wait_for_global_txs(
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
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
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
    wait_for_global_txs(&admin, &[file_tx, todo_tx]).await;

    let visible_ids = query_ids(&alice, "files").await;

    assert!(
        visible_ids.contains(&file_id),
        "array FK membership should grant inherited access when target id is present"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Rejects cyclic reverse-FK policy expansion before starting the server.
#[tokio::test]
async fn rebac_declared_fk_inheritance_cycle_is_rejected() {
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
    match JazzServer::start_with_schema(schema).await {
        Err(error) => {
            assert!(
                error.contains("cyclic policy expansion under INHERITS_REFERENCING"),
                "{error}"
            );
            assert!(error.contains("table_a.SELECT"), "{error}");
            assert!(error.contains("table_b.SELECT"), "{error}");
        }
        Ok(server) => {
            server.shutdown().await;
            panic!("cyclic reverse-FK policy expansion must be rejected");
        }
    }
}

/// A forward dependency expanded inside a reverse source must share its guard.
#[tokio::test]
async fn mixed_forward_reverse_inheritance_cycle_is_rejected() {
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("roots").policies(permissions(|p| {
            p.allow_read()
                .where_(pe::allowed_to_read_referencing("sources", "root_id"));
        })))
        .table(
            TableSchema::builder("sources")
                .fk_column("root_id", "roots")
                .policies(permissions(|p| {
                    p.allow_read().where_(pe::allowed_to_read("root_id"));
                })),
        )
        .build();
    match JazzServer::start_with_schema(schema).await {
        Err(error) => assert!(
            error.contains("cyclic policy expansion under INHERITS_REFERENCING"),
            "{error}",
        ),
        Ok(server) => {
            server.shutdown().await;
            panic!("mixed forward/reverse policy expansion must be rejected");
        }
    }
}

/// Reusing the same source in separate alternatives is not a dependency cycle.
#[tokio::test]
async fn reverse_inheritance_allows_repeated_source_branches() {
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("roots").policies(permissions(|p| {
            p.allow_read().where_(pe::any_of([
                pe::allowed_to_read_referencing("sources", "root_id"),
                pe::allowed_to_read_referencing("sources", "other_root_id"),
            ]));
        })))
        .table(
            TableSchema::builder("sources")
                .fk_column("root_id", "roots")
                .fk_column("other_root_id", "roots")
                .policies(permissions(|p| {
                    p.allow_read()
                        .where_(pe::allowed_to_read_referencing("grants", "source_id"));
                })),
        )
        .table(
            TableSchema::builder("grants")
                .fk_column("source_id", "sources")
                .policies(permissions(|p| {
                    p.allow_read().always();
                })),
        )
        .build();
    let server = JazzServer::start_with_schema(schema)
        .await
        .expect("independent branches may expand the same source policy");
    server.shutdown().await;
}

/// Revisiting a table for another operation is valid when expansion terminates.
#[tokio::test]
async fn reverse_inheritance_distinguishes_source_operations() {
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("roots").policies(permissions(|p| {
            p.allow_read()
                .where_(pe::allowed_to_read_referencing("sources", "root_id"));
        })))
        .table(
            TableSchema::builder("sources")
                .fk_column("root_id", "roots")
                .nullable_fk_column("parent_id", "sources")
                .policies(permissions(|p| {
                    p.allow_read().where_(pe::allowed_to_referencing(
                        jazz::tools::Operation::Update,
                        "sources",
                        "parent_id",
                    ));
                    p.allow_update()
                        .where_old(pe::allowed_to_read_referencing("grants", "source_id"))
                        .where_new(pe::always());
                })),
        )
        .table(
            TableSchema::builder("grants")
                .fk_column("source_id", "sources")
                .policies(permissions(|p| {
                    p.allow_read().always();
                })),
        )
        .build();
    let server = JazzServer::start_with_schema(schema)
        .await
        .expect("source SELECT may depend on source UPDATE without a cycle");
    server.shutdown().await;
}

/// Verifies that access through a declared reverse-FK path is re-evaluated
/// when the referencing FK column changes from NULL to a target id.
#[tokio::test]
async fn rebac_declared_fk_inheritance_reacts_to_fk_updates() {
    tokio::task::LocalSet::new()
        .run_until(rebac_declared_fk_inheritance_reacts_to_fk_updates_inner())
        .await;
}

async fn rebac_declared_fk_inheritance_reacts_to_fk_updates_inner() {
    let schema = declared_file_inheritance_schema(false);
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
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
    wait_for_global_txs(&admin, &[file_tx, todo_tx]).await;

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
        .update(
            "todos",
            todo_id,
            vec![("image".into(), Value::Uuid(file_id))],
        )
        .expect("link todo image")
        .expect("todo update should commit immediately");
    wait_for_global_txs(&alice, &[update_tx]).await;

    let visible_after_link = query_ids(&alice, "files").await;
    assert!(
        visible_after_link.contains(&file_id),
        "updating referencing FK should re-evaluate and grant access to linked target row"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}
