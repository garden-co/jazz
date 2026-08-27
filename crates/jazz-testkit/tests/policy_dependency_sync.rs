//! Delivery of policy-dependency rows to cold downstream clients.
//!
//! When a query result contains rows whose read policy grants access through
//! another table (an inherits or exists dependency), the sync layer must also
//! deliver the dependency rows themselves to a client that never queried those
//! tables, so the client can re-prove access locally. The dependency closure
//! must be filtered by each dependency row's own read policy and must
//! participate in incremental maintenance, not just initial delivery.

use jazz_testkit as support;

use std::time::Duration;

use jazz::query::Query;
use jazz::row_input;
use jazz::tools::policy_expr::rel;
use jazz::tools::public_schema::{RelPredicateCmpOp, RelValueRef, RowIdRef};
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, Schema, SchemaBuilder, TableSchema, Value, permissions,
    policy_expr as pe,
};
use jazz_server::JazzServer;
use support::{
    TestingClient, has_added_id, has_removed, wait_for_query, wait_for_rows,
    wait_for_subscription_update,
};

// The server requires UUID principals.
const ALICE_ID: &str = "9750dcc2-516e-5ea0-8a26-54fa6ff6986b";
const BOB_ID: &str = "756886b3-2033-583f-bd5a-a22f02fb5a6b";

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(25);
const LOCAL_TIMEOUT: Duration = Duration::from_secs(25);

/// Documents readable by their author or through the containing folder, which
/// is readable only by its owner.
fn folder_grant_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read()
                        .where_(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
                })),
        )
        .table(
            TableSchema::builder("documents")
                .column("author_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read().where_(pe::any_of([
                        pe::eq("author_id", pe::session(vec!["claims", "sub"])),
                        pe::all_of([
                            pe::is_not_null("folder_id"),
                            pe::allowed_to_read("folder_id"),
                        ]),
                    ]));
                })),
        )
        .build()
}

/// Documents readable through their folder, which is readable only when a
/// membership row links the session to that exact folder.
fn membership_chain_schema() -> Schema {
    let member_of_this_folder = pe::exists(pe::table("memberships").where_(rel::all_of([
        rel::eq_session("user_id", vec!["claims", "sub"]),
        rel::cmp(
            "folder_id",
            RelPredicateCmpOp::Eq,
            RelValueRef::RowId(RowIdRef::Outer),
        ),
    ])));

    SchemaBuilder::new()
        .table(
            TableSchema::builder("memberships")
                .column("user_id", ColumnType::Text)
                .fk_column("folder_id", "folders")
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_delete().always();
                    p.allow_read()
                        .where_(pe::eq("user_id", pe::session(vec!["claims", "sub"])));
                })),
        )
        .table(
            TableSchema::builder("folders")
                .column("name", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read().where_(member_of_this_folder);
                })),
        )
        .table(
            TableSchema::builder("documents")
                .column("title", ColumnType::Text)
                .fk_column("folder_id", "folders")
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read().where_(pe::allowed_to_read("folder_id"));
                })),
        )
        .build()
}

async fn connect_admin(server: &JazzServer, schema: &Schema) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .as_admin()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await
}

async fn connect_cold_user(server: &JazzServer, schema: &Schema, user_id: &str) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id(user_id)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await
}

/// A cold client receives rows whose read grant depends on another table.
///
/// Bob's document is readable by alice only because it sits in a folder alice
/// owns. Alice connects after all rows are settled and queries only
/// `documents`; the folder row must travel with the result so alice can
/// re-prove access locally.
///
/// ```text
/// admin ──folder(owner=alice)──► server
/// admin ──doc(author=bob, folder)──► server
///                                      │
/// alice connects cold, queries documents only
///                                      │
///                                      └──► doc + folder dependency ──► visible
/// ```
#[tokio::test]
async fn cold_client_receives_rows_granted_through_a_dependency_table() {
    tokio::task::LocalSet::new()
        .run_until(cold_client_receives_rows_granted_through_a_dependency_table_inner())
        .await;
}

async fn cold_client_receives_rows_granted_through_a_dependency_table_inner() {
    let schema = folder_grant_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_admin(&server, &schema).await;

    let (folder_id, _, folder_tx) = admin
        .insert(
            "folders",
            row_input!("owner_id" => ALICE_ID, "name" => "Alice folder"),
        )
        .expect("seed folder");
    let (doc_id, _, doc_tx) = admin
        .insert(
            "documents",
            row_input!(
                "author_id" => BOB_ID,
                "title" => "Doc in Alice folder",
                "folder_id" => folder_id
            ),
        )
        .expect("seed document");
    support::wait_for_edge_txs(
        &admin,
        &[
            folder_tx.expect("ordinary mutation commits immediately"),
            doc_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    let alice = connect_cold_user(&server, &schema, ALICE_ID).await;
    let expected_values = vec![
        Value::Text(BOB_ID.into()),
        Value::Text("Doc in Alice folder".into()),
        Some(folder_id).into(),
    ];

    let rows = wait_for_rows(
        &alice,
        Query::from("documents"),
        "alice sees bob's document through her folder",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1, expected_values);

    // The row must also be provable against alice's local state, which needs
    // the folder dependency row to have been delivered alongside it.
    let local_rows = wait_for_query(
        &alice,
        Query::from("documents"),
        Some(DurabilityTier::Local),
        LOCAL_TIMEOUT,
        "alice proves bob's document locally",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(rows),
    )
    .await;
    assert_eq!(local_rows[0].1, expected_values);

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// A cold client receives every transitively required dependency row.
///
/// The document's grant chains through two dependency tables: proving the
/// document needs the folder, and proving the folder needs alice's membership
/// row. Alice queries only `documents`, so both dependency rows must travel.
///
/// ```text
/// membership(alice) ──grants──► folder ──grants──► document
///
/// alice connects cold, queries documents only
///     └──► doc + folder + membership ──► visible
/// ```
#[tokio::test]
async fn cold_client_receives_transitively_required_dependency_rows() {
    tokio::task::LocalSet::new()
        .run_until(cold_client_receives_transitively_required_dependency_rows_inner())
        .await;
}

async fn cold_client_receives_transitively_required_dependency_rows_inner() {
    let schema = membership_chain_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_admin(&server, &schema).await;

    let (folder_id, _, folder_tx) = admin
        .insert("folders", row_input!("name" => "Shared"))
        .expect("seed folder");
    let (_, _, membership_tx) = admin
        .insert(
            "memberships",
            row_input!("user_id" => ALICE_ID, "folder_id" => folder_id),
        )
        .expect("seed alice membership");
    let (doc_id, _, doc_tx) = admin
        .insert(
            "documents",
            row_input!("title" => "Doc in shared folder", "folder_id" => folder_id),
        )
        .expect("seed document");
    support::wait_for_edge_txs(
        &admin,
        &[
            folder_tx.expect("ordinary mutation commits immediately"),
            membership_tx.expect("ordinary mutation commits immediately"),
            doc_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    let alice = connect_cold_user(&server, &schema, ALICE_ID).await;
    let expected_values = vec![
        Value::Text("Doc in shared folder".into()),
        Some(folder_id).into(),
    ];

    let rows = wait_for_rows(
        &alice,
        Query::from("documents"),
        "alice sees the document through membership and folder",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1, expected_values);

    let local_rows = wait_for_query(
        &alice,
        Query::from("documents"),
        Some(DurabilityTier::Local),
        LOCAL_TIMEOUT,
        "alice proves the document locally through both dependency rows",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(rows),
    )
    .await;
    assert_eq!(local_rows[0].1, expected_values);

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Dependency delivery is filtered by each dependency row's own read policy
/// and never widens visibility.
///
/// A second folder is readable only through bob's membership. Alice must not
/// see bob's document, and querying the dependency tables directly must return
/// only her own membership and folder rows.
///
/// ```text
/// membership(alice) ──► folder A ──► doc A   (alice's chain)
/// membership(bob) ────► folder B ──► doc B   (bob's chain)
///
/// alice queries documents ──► doc A only
/// alice queries memberships ──► her row only
/// alice queries folders ──► folder A only
/// ```
#[tokio::test]
async fn dependency_delivery_does_not_widen_visibility() {
    tokio::task::LocalSet::new()
        .run_until(dependency_delivery_does_not_widen_visibility_inner())
        .await;
}

async fn dependency_delivery_does_not_widen_visibility_inner() {
    let schema = membership_chain_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_admin(&server, &schema).await;

    let (alice_folder_id, _, alice_folder_tx) = admin
        .insert("folders", row_input!("name" => "Alice folder"))
        .expect("seed alice folder");
    let (alice_membership_id, _, alice_membership_tx) = admin
        .insert(
            "memberships",
            row_input!("user_id" => ALICE_ID, "folder_id" => alice_folder_id),
        )
        .expect("seed alice membership");
    let (alice_doc_id, _, alice_doc_tx) = admin
        .insert(
            "documents",
            row_input!("title" => "Alice doc", "folder_id" => alice_folder_id),
        )
        .expect("seed alice document");

    let (bob_folder_id, _, bob_folder_tx) = admin
        .insert("folders", row_input!("name" => "Bob folder"))
        .expect("seed bob folder");
    let (_, _, bob_membership_tx) = admin
        .insert(
            "memberships",
            row_input!("user_id" => BOB_ID, "folder_id" => bob_folder_id),
        )
        .expect("seed bob membership");
    let (bob_doc_id, _, bob_doc_tx) = admin
        .insert(
            "documents",
            row_input!("title" => "Bob doc", "folder_id" => bob_folder_id),
        )
        .expect("seed bob document");

    support::wait_for_edge_txs(
        &admin,
        &[
            alice_folder_tx.expect("ordinary mutation commits immediately"),
            alice_membership_tx.expect("ordinary mutation commits immediately"),
            alice_doc_tx.expect("ordinary mutation commits immediately"),
            bob_folder_tx.expect("ordinary mutation commits immediately"),
            bob_membership_tx.expect("ordinary mutation commits immediately"),
            bob_doc_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    let alice = connect_cold_user(&server, &schema, ALICE_ID).await;

    // All seeds were Edge-settled before alice connected, so the settled
    // result set below is exact, not a snapshot of in-flight deliveries.
    let doc_rows = wait_for_rows(
        &alice,
        Query::from("documents"),
        "alice sees only her own chain's document",
        |rows| (rows.len() == 1 && rows[0].0 == alice_doc_id).then_some(rows),
    )
    .await;
    assert!(doc_rows.iter().all(|(id, _)| *id != bob_doc_id));

    let membership_rows = wait_for_rows(
        &alice,
        Query::from("memberships"),
        "alice sees only her own membership row",
        |rows| (rows.len() == 1 && rows[0].0 == alice_membership_id).then_some(rows),
    )
    .await;
    assert_eq!(
        membership_rows[0].1,
        vec![Value::Text(ALICE_ID.into()), Some(alice_folder_id).into()]
    );

    let folder_rows = wait_for_rows(
        &alice,
        Query::from("folders"),
        "alice sees only the folder her membership grants",
        |rows| (rows.len() == 1 && rows[0].0 == alice_folder_id).then_some(rows),
    )
    .await;
    assert!(folder_rows.iter().all(|(id, _)| *id != bob_folder_id));

    // Bob's dependency rows must not have been over-delivered into alice's
    // local state either: local reads stay limited to her own chain.
    let local_membership_rows = wait_for_query(
        &alice,
        Query::from("memberships"),
        Some(DurabilityTier::Local),
        LOCAL_TIMEOUT,
        "alice's local membership view stays limited to her own row",
        |rows| (rows.len() == 1 && rows[0].0 == alice_membership_id).then_some(rows),
    )
    .await;
    assert_eq!(local_membership_rows.len(), 1);
    let local_folder_rows = wait_for_query(
        &alice,
        Query::from("folders"),
        Some(DurabilityTier::Local),
        LOCAL_TIMEOUT,
        "alice's local folder view stays limited to her own folder",
        |rows| (rows.len() == 1 && rows[0].0 == alice_folder_id).then_some(rows),
    )
    .await;
    assert_eq!(local_folder_rows.len(), 1);

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Dependency rows participate in incremental maintenance of dependent
/// visibility, not only in initial delivery.
///
/// Revoking alice's membership row must remove the document from her live
/// subscription; re-granting a membership must bring it back.
///
/// ```text
/// alice ──subscribe documents──► sees doc
/// admin ──delete membership(alice)──► doc removed from subscription
/// admin ──insert membership(alice)──► doc added back
/// ```
#[tokio::test]
async fn dependency_row_update_propagates_to_dependent_visibility() {
    tokio::task::LocalSet::new()
        .run_until(dependency_row_update_propagates_to_dependent_visibility_inner())
        .await;
}

async fn dependency_row_update_propagates_to_dependent_visibility_inner() {
    let schema = membership_chain_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_admin(&server, &schema).await;

    let (folder_id, _, folder_tx) = admin
        .insert("folders", row_input!("name" => "Shared"))
        .expect("seed folder");
    let (membership_id, _, membership_tx) = admin
        .insert(
            "memberships",
            row_input!("user_id" => ALICE_ID, "folder_id" => folder_id),
        )
        .expect("seed alice membership");
    let (doc_id, _, doc_tx) = admin
        .insert(
            "documents",
            row_input!("title" => "Doc in shared folder", "folder_id" => folder_id),
        )
        .expect("seed document");
    support::wait_for_edge_txs(
        &admin,
        &[
            folder_tx.expect("ordinary mutation commits immediately"),
            membership_tx.expect("ordinary mutation commits immediately"),
            doc_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    let alice = connect_cold_user(&server, &schema, ALICE_ID).await;
    let mut stream = alice
        .subscribe(Query::from("documents"))
        .await
        .expect("alice subscribes to documents");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        SUBSCRIPTION_TIMEOUT,
        "alice's subscription delivers the granted document",
        |log| has_added_id(log, doc_id),
    )
    .await;

    // Revoke: deleting the membership row breaks the grant chain, so the
    // dependent document must leave alice's subscription.
    let revoke_tx = admin
        .delete(membership_id)
        .expect("admin revokes alice's membership");
    support::wait_for_edge_txs(
        &admin,
        &[revoke_tx.expect("ordinary mutation commits immediately")],
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        SUBSCRIPTION_TIMEOUT,
        "alice's subscription removes the document after revocation",
        |log| has_removed(log, doc_id),
    )
    .await;
    let log_after_revoke = log.len();

    // Re-grant: a fresh membership row restores the chain and the document.
    let (_, _, regrant_tx) = admin
        .insert(
            "memberships",
            row_input!("user_id" => ALICE_ID, "folder_id" => folder_id),
        )
        .expect("admin re-grants alice's membership");
    support::wait_for_edge_txs(
        &admin,
        &[regrant_tx.expect("ordinary mutation commits immediately")],
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        SUBSCRIPTION_TIMEOUT,
        "alice's subscription re-adds the document after the re-grant",
        |log| has_added_id(&log[log_after_revoke..], doc_id),
    )
    .await;

    let rows = wait_for_rows(
        &alice,
        Query::from("documents"),
        "alice's settled view contains the document again",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].0, doc_id);

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}
