//! Policy evaluation must keep working across schema migrations.
//!
//! Rows authored under an earlier schema generation must still be granted and
//! denied correctly once the catalogue advances to a newer generation, and the
//! same holds for policy DEPENDENCY rows: a membership row referenced by an
//! EXISTS select policy that was written under the old generation must keep
//! granting (and its absence must keep denying) reads served to clients on the
//! new generation.

use jazz_testkit as support;

use std::time::Duration;

use jazz::query::Query;
use jazz::row_input;
use jazz::tools::public_schema::SchemaHash;
use jazz::tools::schema_lens::{Lens, LensOp, LensTransform};
use jazz::tools::{
    ClientId, ColumnType, DurabilityTier, JazzClient, ObjectId, Schema, SchemaBuilder, TableSchema,
    Value, permissions, policy_expr as pe,
};
use jazz_server::JazzServer;
use support::{
    TestingClient, collect_stream_deltas, has_added_id, has_any_change, publish_permissions,
    push_catalogue_in_memory, wait_for_edge_query_ready, wait_for_edge_txs, wait_for_query,
    wait_for_subscription_update, wait_for_visible_row,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const STEADY_STATE_TIMEOUT: Duration = Duration::from_secs(3);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(500);

// The server requires UUID principals.
const ALICE_ID: &str = "9750dcc2-516e-5ea0-8a26-54fa6ff6986b";
const MALLORY_ID: &str = "5363f5ca-d268-52d3-af19-c4c0c5e93f63";
const ADMIN_ID: &str = "211663a4-14bd-52c4-92b4-f369967c20b3";

/// Publishes the schema's own table policies as the app's permissions head.
async fn publish_schema_permissions(server: &JazzServer, schema: &Schema) {
    let table_permissions = schema
        .iter()
        .map(|(table_name, table_schema)| (*table_name, table_schema.policies.clone()))
        .collect::<Vec<_>>();
    publish_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        schema,
        table_permissions,
        None,
    )
    .await;
}

/// Pushes a generation's schemas and lenses through the catalogue pipeline and
/// publishes the newest schema's policies as the permissions head.
async fn publish_generation(server: &JazzServer, schemas: &[Schema], lenses: &[Lens]) {
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        schemas,
        lenses,
    )
    .await
    .expect("push generation catalogue");
    publish_schema_permissions(server, schemas.last().expect("at least one schema")).await;
}

/// Pushes the full two-generation lineage before any client connects; the
/// permissions head then selects the active write generation. Post-migration
/// writes require this order today: a lineage bundle published only at
/// runtime leaves later writes unable to settle at the edge.
async fn push_full_catalogue(server: &JazzServer, schemas: &[Schema], lenses: &[Lens]) {
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        schemas,
        lenses,
    )
    .await
    .expect("push full catalogue");
}

/// Connects a client with a fresh device (client) id, so several sequential
/// connections by the same principal never collide on stored transaction ids.
async fn connect_with_fresh_client_id(builder: TestingClient<'_>) -> JazzClient {
    let mut context = builder.build_context();
    context.client_id = Some(ClientId::new());
    let client = jazz_testkit::connect(context)
        .await
        .expect("connect test client");
    wait_for_edge_query_ready(&client, "documents", READY_TIMEOUT).await;
    client
}

async fn connect_user(server: &JazzServer, schema: &Schema, user_id: &str) -> JazzClient {
    connect_with_fresh_client_id(
        TestingClient::builder()
            .with_server(server)
            .with_schema(schema.clone())
            .with_user_id(user_id)
            .as_user(),
    )
    .await
}

async fn connect_admin(server: &JazzServer, schema: &Schema) -> JazzClient {
    connect_with_fresh_client_id(
        TestingClient::builder()
            .with_server(server)
            .with_schema(schema.clone())
            .with_user_id(ADMIN_ID)
            .as_admin(),
    )
    .await
}

// ---------------------------------------------------------------------------
// Fixture 1: a self-column owner policy on `documents`.
// ---------------------------------------------------------------------------

fn owner_documents_policies() -> jazz::tools::TablePolicies {
    permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
        p.allow_insert()
            .where_(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
        p.allow_update()
            .where_old(pe::eq("owner_id", pe::session(vec!["claims", "sub"])))
            .where_new(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
    })
}

/// Generation v1: documents gated by an owner-based policy.
fn owner_schema_v1() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("folder_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(owner_documents_policies()),
        )
        .build()
}

/// Generation v2: the same table with an added nullable variable-length
/// `labels` column, so v1 and v2 rows encode different column sets.
fn owner_schema_v2() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("folder_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .nullable_column("labels", ColumnType::Text)
                .policies(owner_documents_policies()),
        )
        .build()
}

fn owner_lens_v1_to_v2() -> Lens {
    Lens::new(
        SchemaHash::compute(&owner_schema_v1()),
        SchemaHash::compute(&owner_schema_v2()),
        LensTransform::with_ops(vec![LensOp::AddColumn {
            table: "documents".to_string(),
            column: "labels".to_string(),
            column_type: ColumnType::Text,
            default: Value::Null,
        }]),
    )
}

/// Seeds the owner fixture under generation v1: `count` alice-owned documents
/// plus one mallory-owned document, all authored by their owners as ordinary
/// users. Returns (alice document ids, mallory document id).
async fn seed_owner_documents_under_v1(
    server: &JazzServer,
    count: usize,
) -> (Vec<ObjectId>, ObjectId) {
    let v1 = owner_schema_v1();
    let alice = connect_user(server, &v1, ALICE_ID).await;
    let mallory = connect_user(server, &v1, MALLORY_ID).await;

    let mut alice_ids = Vec::new();
    let mut alice_txs = Vec::new();
    for index in 0..count {
        let (id, _, tx) = alice
            .insert(
                "documents",
                row_input!(
                    "owner_id" => ALICE_ID,
                    "folder_id" => "folder-1",
                    "name" => format!("doc-{index}"),
                ),
            )
            .expect("alice inserts her v1 document");
        alice_ids.push(id);
        alice_txs.push(tx.expect("ordinary mutation commits immediately"));
    }
    wait_for_edge_txs(&alice, &alice_txs).await;

    let (mallory_id, _, mallory_tx) = mallory
        .insert(
            "documents",
            row_input!(
                "owner_id" => MALLORY_ID,
                "folder_id" => "folder-9",
                "name" => "mallory-doc",
            ),
        )
        .expect("mallory inserts her v1 document");
    wait_for_edge_txs(
        &mallory,
        &[mallory_tx.expect("ordinary mutation commits immediately")],
    )
    .await;

    alice.shutdown().await.expect("shutdown v1 alice");
    mallory.shutdown().await.expect("shutdown v1 mallory");
    (alice_ids, mallory_id)
}

/// Rows authored under schema v1 must remain served to a v2 reader whose
/// owner policy passes, and rows owned by another session must stay excluded
/// from the same read.
///
/// ```text
/// alice ──insert v1 docs──► server ◄──insert v1 doc── mallory
/// admin ──publish v2 + lens──► server
/// alice (v2) ──query──► server ──policy──► alice docs only
/// ```
#[tokio::test]
async fn owner_policy_keeps_serving_v1_documents_to_v2_reader() {
    tokio::task::LocalSet::new()
        .run_until(owner_policy_keeps_serving_v1_documents_to_v2_reader_impl())
        .await
}

async fn owner_policy_keeps_serving_v1_documents_to_v2_reader_impl() {
    let server = JazzServer::start().await;
    publish_generation(&server, &[owner_schema_v1()], &[]).await;
    let (alice_ids, mallory_id) = seed_owner_documents_under_v1(&server, 3).await;
    publish_generation(
        &server,
        &[owner_schema_v1(), owner_schema_v2()],
        &[owner_lens_v1_to_v2()],
    )
    .await;

    let alice = connect_user(&server, &owner_schema_v2(), ALICE_ID).await;
    let rows = wait_for_query(
        &alice,
        Query::from("documents"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice sees all of her v1-authored documents through the v2 generation",
        |rows| {
            alice_ids
                .iter()
                .all(|id| rows.iter().any(|(row_id, _)| row_id == id))
                .then_some(rows)
        },
    )
    .await;
    assert!(
        rows.iter().all(|(row_id, _)| *row_id != mallory_id),
        "mallory's document must stay excluded from alice's read; rows: {rows:?}"
    );
    let (_, first_values) = rows
        .iter()
        .find(|(row_id, _)| *row_id == alice_ids[0])
        .expect("first alice document present");
    assert_eq!(
        *first_values,
        vec![
            Value::Text(ALICE_ID.into()),
            Value::Text("folder-1".into()),
            Value::Text("doc-0".into()),
            Value::Null,
        ],
        "the v1-authored row must surface all its columns plus the v2 default"
    );

    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// The deny direction must not regress with the migration: v1-authored rows
/// whose owner policy fails for the session stay excluded from v2 reads while
/// the session's own row is still served.
#[tokio::test]
async fn owner_policy_still_denies_v1_documents_to_other_sessions_after_migration() {
    tokio::task::LocalSet::new()
        .run_until(owner_policy_still_denies_v1_documents_to_other_sessions_after_migration_impl())
        .await
}

async fn owner_policy_still_denies_v1_documents_to_other_sessions_after_migration_impl() {
    let server = JazzServer::start().await;
    publish_generation(&server, &[owner_schema_v1()], &[]).await;
    let (alice_ids, mallory_id) = seed_owner_documents_under_v1(&server, 2).await;
    publish_generation(
        &server,
        &[owner_schema_v1(), owner_schema_v2()],
        &[owner_lens_v1_to_v2()],
    )
    .await;

    let mallory = connect_user(&server, &owner_schema_v2(), MALLORY_ID).await;
    let rows = wait_for_query(
        &mallory,
        Query::from("documents"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "mallory sees her own v1-authored document through the v2 generation",
        |rows| {
            rows.iter()
                .any(|(row_id, _)| *row_id == mallory_id)
                .then_some(rows)
        },
    )
    .await;
    for alice_id in &alice_ids {
        assert!(
            rows.iter().all(|(row_id, _)| row_id != alice_id),
            "alice's documents must stay excluded from mallory's read; rows: {rows:?}"
        );
    }

    mallory.shutdown().await.expect("shutdown mallory");
    server.shutdown().await;
}

/// A v2 update touching one column of a v1-authored row must preserve the
/// v1-era values of the columns the update did not touch, as observed by a
/// fresh reader.
///
/// ```text
/// alice ──insert v1 doc──► server
/// admin ──publish v2 + lens──► server
/// alice (v2) ──update name──► server
/// fresh alice (v2) ──query──► owner + folder columns intact
/// ```
#[tokio::test]
#[ignore = "#1779: alice's v2 update of her v1-authored row never settles at the edge (neither accepted nor rejected) when the published read policy is session-scoped; the same update settles under an allow-all head and a v2-authored row updates fine"]
async fn v2_update_of_v1_document_preserves_untouched_columns() {
    tokio::task::LocalSet::new()
        .run_until(v2_update_of_v1_document_preserves_untouched_columns_impl())
        .await
}

async fn v2_update_of_v1_document_preserves_untouched_columns_impl() {
    let server = JazzServer::start().await;
    push_full_catalogue(
        &server,
        &[owner_schema_v1(), owner_schema_v2()],
        &[owner_lens_v1_to_v2()],
    )
    .await;
    publish_schema_permissions(&server, &owner_schema_v1()).await;
    let (alice_ids, _mallory_id) = seed_owner_documents_under_v1(&server, 1).await;
    let doc_id = alice_ids[0];
    publish_schema_permissions(&server, &owner_schema_v2()).await;

    let alice = connect_user(&server, &owner_schema_v2(), ALICE_ID).await;
    wait_for_visible_row(
        &alice,
        Query::from("documents"),
        "alice observes her v1 document before updating it",
        doc_id,
        vec![
            Value::Text(ALICE_ID.into()),
            Value::Text("folder-1".into()),
            Value::Text("doc-0".into()),
            Value::Null,
        ],
    )
    .await;
    let transaction_id = alice
        .update(doc_id, vec![("name".into(), Value::Text("renamed".into()))])
        .expect("alice updates her v1 document through the v2 schema");
    wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    // A fresh client proves the server serves the full merged row, not only
    // the columns the update carried.
    let reader = connect_user(&server, &owner_schema_v2(), ALICE_ID).await;
    wait_for_visible_row(
        &reader,
        Query::from("documents"),
        "a fresh reader sees the updated row with its v1-era columns intact",
        doc_id,
        vec![
            Value::Text(ALICE_ID.into()),
            Value::Text("folder-1".into()),
            Value::Text("renamed".into()),
            Value::Null,
        ],
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    reader.shutdown().await.expect("shutdown reader");
    server.shutdown().await;
}

/// An update that the owner policy forbids stays rejected after the
/// migration: mallory cannot rewrite alice's v1-authored row through the v2
/// generation, and alice keeps seeing the original values.
#[tokio::test]
async fn v2_update_denied_by_owner_policy_stays_rejected() {
    tokio::task::LocalSet::new()
        .run_until(v2_update_denied_by_owner_policy_stays_rejected_impl())
        .await
}

async fn v2_update_denied_by_owner_policy_stays_rejected_impl() {
    let server = JazzServer::start().await;
    push_full_catalogue(
        &server,
        &[owner_schema_v1(), owner_schema_v2()],
        &[owner_lens_v1_to_v2()],
    )
    .await;
    publish_schema_permissions(&server, &owner_schema_v1()).await;
    let (alice_ids, _mallory_id) = seed_owner_documents_under_v1(&server, 1).await;
    let doc_id = alice_ids[0];
    publish_schema_permissions(&server, &owner_schema_v2()).await;

    // The public client enforces the deny at its earliest surface: either the
    // local write is refused outright or the synced write settles rejected.
    let mallory = connect_user(&server, &owner_schema_v2(), MALLORY_ID).await;
    match mallory.update(
        doc_id,
        vec![("name".into(), Value::Text("hijacked".into()))],
    ) {
        Err(_) => {}
        Ok(transaction_id) => {
            let settled = mallory
                .wait_for_transaction(
                    transaction_id.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await;
            assert!(
                settled.is_err(),
                "mallory's update of alice's v1 document must settle rejected"
            );
        }
    }

    let alice = connect_user(&server, &owner_schema_v2(), ALICE_ID).await;
    wait_for_visible_row(
        &alice,
        Query::from("documents"),
        "alice still sees the original values after mallory's rejected update",
        doc_id,
        vec![
            Value::Text(ALICE_ID.into()),
            Value::Text("folder-1".into()),
            Value::Text("doc-0".into()),
            Value::Null,
        ],
    )
    .await;

    mallory.shutdown().await.expect("shutdown mallory");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Fixture 2: a dependency-bearing select policy. Documents are readable only
// through a matching membership row (session user is a member of the
// document's folder); membership rows are readable by their own user.
// ---------------------------------------------------------------------------

fn membership_documents_policies() -> jazz::tools::TablePolicies {
    permissions(|p| {
        p.allow_read()
            .where_(pe::exists(pe::table("memberships").where_(
                pe::rel::all_of([
                    pe::rel::eq_outer("folder_id", "folder_id"),
                    pe::rel::eq_session("user_id", vec!["claims", "sub"]),
                ]),
            )));
        p.allow_insert().always();
    })
}

fn memberships_policies() -> jazz::tools::TablePolicies {
    permissions(|p| {
        p.allow_read()
            .where_(pe::eq("user_id", pe::session(vec!["claims", "sub"])));
        p.allow_insert().always();
    })
}

fn folders_policies() -> jazz::tools::TablePolicies {
    permissions(|p| {
        p.allow_read().always();
        p.allow_insert().always();
    })
}

/// Generation v1 of the dependency fixture.
fn membership_schema_v1() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("name", ColumnType::Text)
                .policies(folders_policies()),
        )
        .table(
            TableSchema::builder("documents")
                .fk_column("folder_id", "folders")
                .column("name", ColumnType::Text)
                .policies(membership_documents_policies()),
        )
        .table(
            TableSchema::builder("memberships")
                .column("user_id", ColumnType::Text)
                .fk_column("folder_id", "folders")
                .policies(memberships_policies()),
        )
        .build()
}

/// Generation v2 of the dependency fixture: documents gain a nullable
/// variable-length `labels` column.
fn membership_schema_v2() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("name", ColumnType::Text)
                .policies(folders_policies()),
        )
        .table(
            TableSchema::builder("documents")
                .fk_column("folder_id", "folders")
                .column("name", ColumnType::Text)
                .nullable_column("labels", ColumnType::Text)
                .policies(membership_documents_policies()),
        )
        .table(
            TableSchema::builder("memberships")
                .column("user_id", ColumnType::Text)
                .fk_column("folder_id", "folders")
                .policies(memberships_policies()),
        )
        .build()
}

fn membership_lens_v1_to_v2() -> Lens {
    Lens::new(
        SchemaHash::compute(&membership_schema_v1()),
        SchemaHash::compute(&membership_schema_v2()),
        LensTransform::with_ops(vec![LensOp::AddColumn {
            table: "documents".to_string(),
            column: "labels".to_string(),
            column_type: ColumnType::Text,
            default: Value::Null,
        }]),
    )
}

/// Rows the dependency fixture seeds under generation v1.
struct MembershipSeed {
    /// Folder alice's membership row grants.
    member_folder_id: ObjectId,
    /// Document in the granted folder.
    granted_id: ObjectId,
    /// Document in a folder no membership grants.
    ungranted_id: ObjectId,
}

/// Seeds the dependency fixture under generation v1: two folders, alice's
/// membership in the first, one document in the granted folder, and one
/// document in the folder no membership grants.
async fn seed_membership_rows_under_v1(server: &JazzServer) -> MembershipSeed {
    let admin = connect_admin(server, &membership_schema_v1()).await;

    let (member_folder_id, _, member_folder_tx) = admin
        .insert("folders", row_input!("name" => "folder-a"))
        .expect("admin seeds the granted v1 folder");
    let (other_folder_id, _, other_folder_tx) = admin
        .insert("folders", row_input!("name" => "folder-b"))
        .expect("admin seeds the ungranted v1 folder");
    let (_, _, membership_tx) = admin
        .insert(
            "memberships",
            row_input!("user_id" => ALICE_ID, "folder_id" => member_folder_id),
        )
        .expect("admin seeds alice's v1 membership");
    let (granted_id, _, granted_tx) = admin
        .insert(
            "documents",
            row_input!("folder_id" => member_folder_id, "name" => "doc-a"),
        )
        .expect("admin seeds the granted v1 document");
    let (ungranted_id, _, ungranted_tx) = admin
        .insert(
            "documents",
            row_input!("folder_id" => other_folder_id, "name" => "doc-b"),
        )
        .expect("admin seeds the ungranted v1 document");
    wait_for_edge_txs(
        &admin,
        &[
            member_folder_tx.expect("ordinary mutation commits immediately"),
            other_folder_tx.expect("ordinary mutation commits immediately"),
            membership_tx.expect("ordinary mutation commits immediately"),
            granted_tx.expect("ordinary mutation commits immediately"),
            ungranted_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    admin.shutdown().await.expect("shutdown v1 admin");
    MembershipSeed {
        member_folder_id,
        granted_id,
        ungranted_id,
    }
}

/// When the v2 catalogue bundle reaches the server relative to the v1 seed
/// data.
enum V2CataloguePush {
    /// The full lineage is known before any client connects; only the
    /// permissions head moves after seeding. Post-migration writes require
    /// this order today.
    BeforeSeeding,
    /// The v2 bundle arrives only after the v1 rows already exist.
    AfterSeeding,
}

/// Starts the dependency-fixture server, seeds all rows under v1, then moves
/// the active generation to v2.
async fn migrated_membership_server(push: V2CataloguePush) -> (JazzServer, MembershipSeed) {
    let server = JazzServer::start().await;
    match push {
        V2CataloguePush::BeforeSeeding => {
            push_full_catalogue(
                &server,
                &[membership_schema_v1(), membership_schema_v2()],
                &[membership_lens_v1_to_v2()],
            )
            .await;
            publish_schema_permissions(&server, &membership_schema_v1()).await;
            let seed = seed_membership_rows_under_v1(&server).await;
            publish_schema_permissions(&server, &membership_schema_v2()).await;
            (server, seed)
        }
        V2CataloguePush::AfterSeeding => {
            publish_generation(&server, &[membership_schema_v1()], &[]).await;
            let seed = seed_membership_rows_under_v1(&server).await;
            publish_generation(
                &server,
                &[membership_schema_v1(), membership_schema_v2()],
                &[membership_lens_v1_to_v2()],
            )
            .await;
            (server, seed)
        }
    }
}

/// A select policy that depends on OTHER rows must keep serving v1-authored
/// documents to a v2 subscription when the membership row proving access also
/// lives under v1, and a session without any membership must stay denied.
///
/// ```text
/// admin ──seed v1 membership + docs──► server
/// admin ──publish v2 + lens──► server
/// alice (v2) ──subscribe──► server ──membership row──► add doc-a, never doc-b
/// mallory (v2) ──subscribe──► server ──no membership──✗ empty view
/// ```
#[tokio::test]
async fn v1_membership_rows_still_grant_v1_documents_to_v2_subscription() {
    tokio::task::LocalSet::new()
        .run_until(v1_membership_rows_still_grant_v1_documents_to_v2_subscription_impl())
        .await
}

async fn v1_membership_rows_still_grant_v1_documents_to_v2_subscription_impl() {
    let (server, seed) = migrated_membership_server(V2CataloguePush::AfterSeeding).await;
    let (granted_id, ungranted_id) = (seed.granted_id, seed.ungranted_id);

    let alice = connect_user(&server, &membership_schema_v2(), ALICE_ID).await;
    let mut alice_stream = alice
        .subscribe(Query::from("documents"))
        .await
        .expect("subscribe alice");
    let mut alice_log = Vec::new();
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        QUERY_TIMEOUT,
        "alice's v2 subscription hydrates the membership-granted v1 document",
        |log| has_added_id(log, granted_id),
    )
    .await;
    collect_stream_deltas(&mut alice_stream, &mut alice_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&alice_log, ungranted_id),
        "the document in a folder alice is not a member of must never reach her \
         subscription; log: {alice_log:?}"
    );

    let mallory = connect_user(&server, &membership_schema_v2(), MALLORY_ID).await;
    let mut mallory_stream = mallory
        .subscribe(Query::from("documents"))
        .await
        .expect("subscribe mallory");
    let mut mallory_log = Vec::new();
    collect_stream_deltas(&mut mallory_stream, &mut mallory_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&mallory_log, granted_id) && !has_any_change(&mallory_log, ungranted_id),
        "a session with no membership row must not receive any document; \
         log: {mallory_log:?}"
    );

    alice.shutdown().await.expect("shutdown alice");
    mallory.shutdown().await.expect("shutdown mallory");
    server.shutdown().await;
}

/// The same grants and denies must hold for one-shot session queries, not
/// only subscriptions: alice's read returns the membership-granted v1
/// document, mallory's read returns nothing.
#[tokio::test]
async fn one_shot_query_honors_v1_membership_dependency() {
    tokio::task::LocalSet::new()
        .run_until(one_shot_query_honors_v1_membership_dependency_impl())
        .await
}

async fn one_shot_query_honors_v1_membership_dependency_impl() {
    let (server, seed) = migrated_membership_server(V2CataloguePush::AfterSeeding).await;
    let (granted_id, ungranted_id) = (seed.granted_id, seed.ungranted_id);

    let alice = connect_user(&server, &membership_schema_v2(), ALICE_ID).await;
    let rows = wait_for_query(
        &alice,
        Query::from("documents"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice's one-shot query returns the membership-granted v1 document",
        |rows| {
            rows.iter()
                .any(|(row_id, _)| *row_id == granted_id)
                .then_some(rows)
        },
    )
    .await;
    assert!(
        rows.iter().all(|(row_id, _)| *row_id != ungranted_id),
        "the ungranted document must stay excluded from alice's one-shot query; \
         rows: {rows:?}"
    );
    let (_, granted_values) = rows
        .iter()
        .find(|(row_id, _)| *row_id == granted_id)
        .expect("granted document present");
    assert_eq!(
        *granted_values,
        vec![
            Value::Uuid(seed.member_folder_id),
            Value::Text("doc-a".into()),
            Value::Null,
        ],
        "the granted v1 row must surface its columns plus the v2 default"
    );

    // Alice's settled grant proves the migrated state is fully served, so
    // mallory's read observes the same settled state.
    let mallory = connect_user(&server, &membership_schema_v2(), MALLORY_ID).await;
    let mallory_rows = wait_for_query(
        &mallory,
        Query::from("documents"),
        Some(DurabilityTier::EdgeServer),
        STEADY_STATE_TIMEOUT,
        "mallory's one-shot query settles",
        Some,
    )
    .await;
    assert!(
        mallory_rows.is_empty(),
        "a session with no membership row must read no documents; \
         rows: {mallory_rows:?}"
    );

    alice.shutdown().await.expect("shutdown alice");
    mallory.shutdown().await.expect("shutdown mallory");
    server.shutdown().await;
}

/// A document inserted AFTER the migration must be served when its policy
/// dependency row is still v1-era: new data, old grant. The membership lookup
/// must span generations, not just the document's own generation.
///
/// ```text
/// admin ──seed v1 membership──► server
/// admin ──publish v2 + lens──► server
/// admin (v2) ──insert doc in folder-1──► server
/// alice (v2) ──query──► v1 membership row grants the v2 document
/// ```
#[tokio::test]
async fn v2_document_with_v1_membership_dependency_is_served() {
    tokio::task::LocalSet::new()
        .run_until(v2_document_with_v1_membership_dependency_is_served_impl())
        .await
}

async fn v2_document_with_v1_membership_dependency_is_served_impl() {
    let (server, seed) = migrated_membership_server(V2CataloguePush::BeforeSeeding).await;
    let (granted_id, ungranted_id) = (seed.granted_id, seed.ungranted_id);

    let admin = connect_admin(&server, &membership_schema_v2()).await;
    let (new_doc_id, _, transaction_id) = admin
        .insert(
            "documents",
            row_input!(
                "folder_id" => seed.member_folder_id,
                "name" => "doc-new",
                "labels" => "fresh",
            ),
        )
        .expect("admin inserts a v2 document in alice's folder");
    wait_for_edge_txs(
        &admin,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let alice = connect_user(&server, &membership_schema_v2(), ALICE_ID).await;
    let rows = wait_for_query(
        &alice,
        Query::from("documents"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice sees both the v1 and the v2 document through her v1 membership",
        |rows| {
            (rows.iter().any(|(row_id, _)| *row_id == new_doc_id)
                && rows.iter().any(|(row_id, _)| *row_id == granted_id))
            .then_some(rows)
        },
    )
    .await;
    assert!(
        rows.iter().all(|(row_id, _)| *row_id != ungranted_id),
        "the ungranted document must stay excluded; rows: {rows:?}"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// After sync, the client's LOCAL policy evaluation must also honor the
/// v1-era membership row: a local-tier read of a hydrated replica shows the
/// granted document, and a session without membership still reads nothing
/// locally.
///
/// ```text
/// alice (v2) ──subscribe──► server ──► local replica hydrates
/// alice (v2) ──local-tier query──► membership row grants doc-a
/// mallory (v2) ──local-tier query──✗ empty
/// ```
#[tokio::test]
async fn local_query_honors_v1_membership_dependency() {
    tokio::task::LocalSet::new()
        .run_until(local_query_honors_v1_membership_dependency_impl())
        .await
}

async fn local_query_honors_v1_membership_dependency_impl() {
    let (server, seed) = migrated_membership_server(V2CataloguePush::BeforeSeeding).await;
    let (granted_id, ungranted_id) = (seed.granted_id, seed.ungranted_id);

    let alice = connect_user(&server, &membership_schema_v2(), ALICE_ID).await;
    // Subscriptions hydrate the local replica with the documents and the
    // membership row the local policy evaluation depends on.
    let mut documents_stream = alice
        .subscribe(Query::from("documents"))
        .await
        .expect("subscribe alice to documents");
    let mut memberships_stream = alice
        .subscribe(Query::from("memberships"))
        .await
        .expect("subscribe alice to memberships");
    let mut documents_log = Vec::new();
    wait_for_subscription_update(
        &mut documents_stream,
        &mut documents_log,
        QUERY_TIMEOUT,
        "alice's replica hydrates the granted document",
        |log| has_added_id(log, granted_id),
    )
    .await;
    let mut memberships_log = Vec::new();
    wait_for_subscription_update(
        &mut memberships_stream,
        &mut memberships_log,
        QUERY_TIMEOUT,
        "alice's replica hydrates her membership row",
        |log| log.iter().any(|delta| !delta.added.is_empty()),
    )
    .await;

    let local_rows = wait_for_query(
        &alice,
        Query::from("documents"),
        Some(DurabilityTier::Local),
        QUERY_TIMEOUT,
        "alice's local-tier read shows the membership-granted document",
        |rows| {
            rows.iter()
                .any(|(row_id, _)| *row_id == granted_id)
                .then_some(rows)
        },
    )
    .await;
    assert!(
        local_rows.iter().all(|(row_id, _)| *row_id != ungranted_id),
        "the ungranted document must stay excluded from the local read; \
         rows: {local_rows:?}"
    );

    let mallory = connect_user(&server, &membership_schema_v2(), MALLORY_ID).await;
    let mallory_rows = mallory
        .query(Query::from("documents"), Some(DurabilityTier::Local))
        .await
        .expect("mallory's local-tier read succeeds");
    assert!(
        mallory_rows.is_empty(),
        "a session with no membership row must read no documents locally; \
         rows: {mallory_rows:?}"
    );

    alice.shutdown().await.expect("shutdown alice");
    mallory.shutdown().await.expect("shutdown mallory");
    server.shutdown().await;
}

/// The client's LOCAL policy evaluation must honor a v1-era membership row
/// when gating a v2-authored document: the dependency row parked under the
/// old generation still grants the local read, and a session without
/// membership still reads nothing locally.
///
/// ```text
/// admin ──seed v1 membership──► server
/// admin (v2) ──insert doc in alice's folder──► server
/// alice (v2) ──subscribe──► local replica hydrates doc + membership
/// alice (v2) ──local-tier query──► v1 membership row grants the v2 doc
/// mallory (v2) ──local-tier query──✗ empty
/// ```
#[tokio::test]
async fn local_query_honors_v1_membership_for_v2_documents() {
    tokio::task::LocalSet::new()
        .run_until(local_query_honors_v1_membership_for_v2_documents_impl())
        .await
}

async fn local_query_honors_v1_membership_for_v2_documents_impl() {
    let (server, seed) = migrated_membership_server(V2CataloguePush::BeforeSeeding).await;

    let admin = connect_admin(&server, &membership_schema_v2()).await;
    let (new_doc_id, _, transaction_id) = admin
        .insert(
            "documents",
            row_input!(
                "folder_id" => seed.member_folder_id,
                "name" => "doc-local",
                "labels" => Value::Null,
            ),
        )
        .expect("admin inserts a v2 document in alice's folder");
    wait_for_edge_txs(
        &admin,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let alice = connect_user(&server, &membership_schema_v2(), ALICE_ID).await;
    // Subscriptions hydrate the local replica with the document and the
    // membership row the local policy evaluation depends on.
    let mut documents_stream = alice
        .subscribe(Query::from("documents"))
        .await
        .expect("subscribe alice to documents");
    let mut memberships_stream = alice
        .subscribe(Query::from("memberships"))
        .await
        .expect("subscribe alice to memberships");
    let mut documents_log = Vec::new();
    wait_for_subscription_update(
        &mut documents_stream,
        &mut documents_log,
        QUERY_TIMEOUT,
        "alice's replica hydrates the v2 document",
        |log| has_added_id(log, new_doc_id),
    )
    .await;
    let mut memberships_log = Vec::new();
    wait_for_subscription_update(
        &mut memberships_stream,
        &mut memberships_log,
        QUERY_TIMEOUT,
        "alice's replica hydrates her membership row",
        |log| log.iter().any(|delta| !delta.added.is_empty()),
    )
    .await;

    let local_rows = wait_for_query(
        &alice,
        Query::from("documents"),
        Some(DurabilityTier::Local),
        QUERY_TIMEOUT,
        "alice's local-tier read shows the v2 document granted by her v1 membership",
        |rows| {
            rows.iter()
                .any(|(row_id, _)| *row_id == new_doc_id)
                .then_some(rows)
        },
    )
    .await;
    assert!(
        local_rows
            .iter()
            .all(|(row_id, _)| *row_id != seed.ungranted_id),
        "the ungranted document must stay excluded from the local read; \
         rows: {local_rows:?}"
    );

    let mallory = connect_user(&server, &membership_schema_v2(), MALLORY_ID).await;
    let mallory_rows = mallory
        .query(Query::from("documents"), Some(DurabilityTier::Local))
        .await
        .expect("mallory's local-tier read succeeds");
    assert!(
        mallory_rows.is_empty(),
        "a session with no membership row must read no documents locally; \
         rows: {mallory_rows:?}"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    mallory.shutdown().await.expect("shutdown mallory");
    server.shutdown().await;
}
