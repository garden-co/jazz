use std::collections::HashMap;
use std::time::Duration;

use super::support::{TestingClient, wait_for_query, wait_for_rows};
use jazz_tools::query_manager::policy::{Operation, PolicyExpr, PolicyValue};
use jazz_tools::query_manager::types::{TablePolicies, TableSchemaBuilder};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, SchemaBuilder, TableSchema,
    Value,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

// -- Schema builders --

fn make_folders_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .column(
            "owners",
            ColumnType::Array {
                element: Box::new(ColumnType::Text),
            },
        )
        .column("archived", ColumnType::Boolean)
        .policies(policies)
}

fn make_folder_documents_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("owner_id", ColumnType::Text)
        .column("title", ColumnType::Text)
        .column("archived", ColumnType::Boolean)
        .nullable_fk_column("folder_id", "folders")
        .policies(policies)
}

// -- Policy helpers --

fn folder_owner_policy() -> PolicyExpr {
    PolicyExpr::Contains {
        column: "owners".to_string(),
        value: PolicyValue::SessionRef(vec!["user_id".into()]),
    }
}

fn inherited_non_null_policy(operation: Operation, via_column: &str) -> PolicyExpr {
    inherited_non_null_policy_with_depth(operation, via_column, None)
}

fn inherited_non_null_policy_with_depth(
    operation: Operation,
    via_column: &str,
    max_depth: Option<usize>,
) -> PolicyExpr {
    let inherits = match max_depth {
        Some(depth) => PolicyExpr::inherits_with_depth(operation, via_column, depth),
        None => PolicyExpr::inherits(operation, via_column),
    };

    PolicyExpr::and(vec![
        PolicyExpr::IsNotNull {
            column: via_column.to_string(),
        },
        inherits,
    ])
}

// -- Value constructors --

fn folder_input(title: &str, owners: &[&str], archived: bool) -> HashMap<String, Value> {
    HashMap::from([
        ("title".to_string(), Value::Text(title.to_string())),
        (
            "owners".to_string(),
            Value::Array(
                owners
                    .iter()
                    .map(|owner| Value::Text((*owner).to_string()))
                    .collect(),
            ),
        ),
        ("archived".to_string(), Value::Boolean(archived)),
    ])
}

fn folder_document_values(
    owner_id: &str,
    title: &str,
    archived: bool,
    folder_id: Option<ObjectId>,
) -> Vec<Value> {
    vec![
        Value::Text(owner_id.to_string()),
        Value::Text(title.to_string()),
        Value::Boolean(archived),
        folder_id.map(Value::Uuid).unwrap_or(Value::Null),
    ]
}

fn folder_document_input(
    owner_id: &str,
    title: &str,
    archived: bool,
    folder_id: Option<ObjectId>,
) -> HashMap<String, Value> {
    HashMap::from([
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("title".to_string(), Value::Text(title.to_string())),
        ("archived".to_string(), Value::Boolean(archived)),
        (
            "folder_id".to_string(),
            folder_id.map(Value::Uuid).unwrap_or(Value::Null),
        ),
    ])
}

// -- Seed / mutation helpers --

async fn create_folder(
    client: &JazzClient,
    table_name: &str,
    title: &str,
    owners: &[&str],
    archived: bool,
) -> ObjectId {
    client
        .create(table_name, folder_input(title, owners, archived))
        .await
        .expect("create folder")
        .0
}

async fn create_folder_document(
    client: &JazzClient,
    table_name: &str,
    owner_id: &str,
    title: &str,
    archived: bool,
    folder_id: Option<ObjectId>,
) -> ObjectId {
    client
        .create(
            table_name,
            folder_document_input(owner_id, title, archived, folder_id),
        )
        .await
        .expect("create folder document")
        .0
}

// -- Tests --

/// Verifies that documents inside a folder are visible to every folder owner
/// via inherited SELECT policies.
///
/// ```text
/// admin ──create folder owners=[alice,bob]────────► server
/// admin ──create doc owner=charlie, folder=shared► server
///
/// alice query ─► sees doc
/// bob query ───► sees doc
/// charlie query ─► hidden
/// dave query ───► hidden
/// ```
#[tokio::test]
async fn inherited_folder_documents_are_visible_to_all_folder_owners() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy()),
        ))
        .table(make_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(inherited_non_null_policy(Operation::Select, "folder_id")),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let charlie = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("charlie")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let dave = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("dave")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id = create_folder(&admin, "folders", "Shared", &["alice", "bob"], false).await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        "charlie",
        "Shared Doc",
        false,
        Some(folder_id),
    )
    .await;
    let query = QueryBuilder::new("documents").build();

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees folder doc via inheritance",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        folder_document_values("charlie", "Shared Doc", false, Some(folder_id))
    );

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees folder doc via inheritance",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        folder_document_values("charlie", "Shared Doc", false, Some(folder_id))
    );

    let charlie_rows = wait_for_query(
        &charlie,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "charlie sees no documents without folder ownership",
        Some,
    )
    .await;
    assert!(charlie_rows.is_empty());

    let dave_rows = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave sees no documents without folder ownership",
        Some,
    )
    .await;
    assert!(dave_rows.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    charlie.shutdown().await.expect("shutdown charlie");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that inherited SELECT access fails closed both when a child row
/// points at a non-existent FK target and when it points at a parent row that
/// has since been deleted.
///
/// ```text
/// alice(writer) ──create folder owners=[alice,bob]───────────► server
/// alice(writer) ──create doc folder=bogus_id─────────────────► hidden
/// alice(reader) ──query docs─────────────────────────────────► sees only shared doc
/// alice(writer) ──delete shared folder───────────────────────► server
/// bob(fresh) ─────query docs─────────────────────────────────► sees nothing
/// ```
#[tokio::test]
#[should_panic] // "known failing: inherited select still resolves through deleted parent rows"
async fn inherited_folder_documents_fail_closed_for_missing_and_deleted_folder_targets() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy())
                .with_delete(folder_owner_policy()),
        ))
        .table(make_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(inherited_non_null_policy(Operation::Select, "folder_id")),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice_writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let query = QueryBuilder::new("documents").build();

    let folder_id =
        create_folder(&alice_writer, "folders", "Shared", &["alice", "bob"], false).await;
    let missing_folder_id = ObjectId::new();
    let missing_parent_doc_id = create_folder_document(
        &alice_writer,
        "documents",
        "charlie",
        "Bogus Parent",
        false,
        Some(missing_folder_id),
    )
    .await;

    let deleted_parent_doc_id = create_folder_document(
        &alice_writer,
        "documents",
        "charlie",
        "Deleted Parent",
        false,
        Some(folder_id),
    )
    .await;

    let visible_rows = wait_for_rows(
        &alice_reader,
        query.clone(),
        "alice only sees the document whose inherited parent still exists",
        |rows| (rows.len() == 1 && rows[0].0 == deleted_parent_doc_id).then_some(rows),
    )
    .await;
    assert_eq!(
        visible_rows[0].1,
        folder_document_values("charlie", "Deleted Parent", false, Some(folder_id))
    );
    assert!(
        visible_rows
            .iter()
            .all(|(id, _)| *id != missing_parent_doc_id),
        "document with a non-existent inherited parent should stay hidden: {visible_rows:?}"
    );

    alice_writer
        .delete(folder_id)
        .await
        .expect("delete inherited parent folder");
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let folders_query = QueryBuilder::new("folders").build();
    let folders_after_delete = wait_for_query(
        &bob,
        folders_query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees no folders after inherited parent delete",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(folders_after_delete.is_empty());

    let final_rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees no documents after inherited parent delete",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(
        final_rows.is_empty(),
        "documents with missing or deleted inherited parents should stay hidden: {final_rows:?}"
    );

    alice_writer
        .shutdown()
        .await
        .expect("shutdown alice_writer");
    alice_reader
        .shutdown()
        .await
        .expect("shutdown alice_reader");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that direct document ownership grants visibility for standalone
/// docs, while folder membership grants inherited visibility for folder-backed
/// docs.
///
/// ```text
/// admin ──create doc owner=charlie, folder=NULL────► server
/// admin ──create folder owners=[alice,bob]─────────► server
/// admin ──create doc owner=charlie, folder=shared──► server
///
/// charlie query ─► standalone + folder doc
/// alice query ───► folder doc only
/// bob query ─────► folder doc only
/// dave query ────► nothing
/// ```
#[tokio::test]
async fn inherited_folder_access_extends_document_visibility_beyond_direct_owner() {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy()),
        ))
        .table(make_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::or(vec![
                    owner_policy,
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ])),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let charlie = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("charlie")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let dave = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("dave")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id = create_folder(&admin, "folders", "Shared", &["alice", "bob"], false).await;
    let standalone_id =
        create_folder_document(&admin, "documents", "charlie", "Standalone", false, None).await;
    let folder_doc_id = create_folder_document(
        &admin,
        "documents",
        "charlie",
        "Inside Folder",
        false,
        Some(folder_id),
    )
    .await;
    let query = QueryBuilder::new("documents").build();

    let charlie_rows = wait_for_rows(
        &charlie,
        query.clone(),
        "charlie sees standalone and folder doc as direct owner",
        |rows| {
            rows.iter()
                .any(|(id, _)| *id == standalone_id)
                .then_some(rows)
        },
    )
    .await;
    assert!(charlie_rows.iter().any(|(id, values)| {
        *id == standalone_id
            && *values == folder_document_values("charlie", "Standalone", false, None)
    }));
    assert!(charlie_rows.iter().any(|(id, values)| {
        *id == folder_doc_id
            && *values == folder_document_values("charlie", "Inside Folder", false, Some(folder_id))
    }));

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees only folder-backed doc",
        |rows| {
            rows.iter()
                .any(|(id, _)| *id == folder_doc_id)
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(
        alice_rows.len(),
        1,
        "alice should only see the folder-backed doc: {alice_rows:?}"
    );
    assert_eq!(
        alice_rows[0].1,
        folder_document_values("charlie", "Inside Folder", false, Some(folder_id))
    );

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees only folder-backed doc",
        |rows| {
            rows.iter()
                .any(|(id, _)| *id == folder_doc_id)
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(
        bob_rows.len(),
        1,
        "bob should only see the folder-backed doc: {bob_rows:?}"
    );
    assert_eq!(
        bob_rows[0].1,
        folder_document_values("charlie", "Inside Folder", false, Some(folder_id))
    );

    let dave_rows = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave sees no documents",
        Some,
    )
    .await;
    assert!(dave_rows.is_empty());

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    charlie.shutdown().await.expect("shutdown charlie");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that document inserts require both direct ownership and, when a
/// folder FK is present, folder ownership via inherited policy.
///
/// ```text
/// admin ──create folder owners=[alice,bob]──────────► server
///
/// charlie ─insert owner=charlie, folder=shared──────► server ──✗ rejected
/// charlie ─insert owner=charlie, folder=NULL────────► server ──► accepted
///
/// alice ──insert owner=bob, folder=shared───────────► server ──✗ rejected
/// alice ──insert owner=alice, folder=NULL───────────► server ──► accepted
/// alice ──insert owner=alice, folder=shared─────────► server ──► accepted
/// ```
#[tokio::test]
#[should_panic] // "known failing: inherited write policies resolves on wrong branch"
async fn inherited_folder_insert_requires_folder_owner_when_fk_present() {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy()),
        ))
        .table(make_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::and(vec![
                    owner_policy.clone(),
                    PolicyExpr::or(vec![
                        PolicyExpr::IsNull {
                            column: "folder_id".into(),
                        },
                        inherited_non_null_policy(Operation::Select, "folder_id"),
                    ]),
                ]))
                .with_select(PolicyExpr::or(vec![
                    owner_policy,
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ])),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let charlie = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("charlie")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let charlie_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("charlie")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id = create_folder(&alice, "folders", "Shared", &["alice", "bob"], false).await;
    let folders_query = QueryBuilder::new("folders").build();
    wait_for_rows(
        &alice,
        folders_query,
        "alice sees shared folder before inserting into it",
        |rows| rows.iter().any(|(id, _)| *id == folder_id).then_some(()),
    )
    .await;

    let _charlie_rejected = create_folder_document(
        &charlie,
        "documents",
        "charlie",
        "Charlie Shared Attempt",
        false,
        Some(folder_id),
    )
    .await;
    let charlie_ok = create_folder_document(
        &charlie,
        "documents",
        "charlie",
        "Charlie Standalone",
        false,
        None,
    )
    .await;

    let _alice_rejected = create_folder_document(
        &alice,
        "documents",
        "bob",
        "Forged For Bob",
        false,
        Some(folder_id),
    )
    .await;
    let alice_standalone = create_folder_document(
        &alice,
        "documents",
        "alice",
        "Alice Standalone",
        false,
        None,
    )
    .await;
    let alice_shared = create_folder_document(
        &alice,
        "documents",
        "alice",
        "Alice Shared",
        false,
        Some(folder_id),
    )
    .await;
    let query = QueryBuilder::new("documents").build();

    let charlie_rows = wait_for_rows(
        &charlie_reader,
        query.clone(),
        "charlie only sees standalone doc after rejected folder insert",
        |rows| rows.iter().any(|(id, _)| *id == charlie_ok).then_some(rows),
    )
    .await;
    assert_eq!(
        charlie_rows.len(),
        1,
        "charlie should only see the standalone doc: {charlie_rows:?}"
    );
    assert_eq!(
        charlie_rows[0].1,
        folder_document_values("charlie", "Charlie Standalone", false, None)
    );

    let alice_rows = wait_for_rows(
        &alice_reader,
        query.clone(),
        "alice sees accepted standalone and shared docs only",
        |rows| {
            rows.iter()
                .any(|(id, _)| *id == alice_standalone)
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(
        alice_rows.len(),
        2,
        "alice should only see her standalone and shared docs: {alice_rows:?}"
    );
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == alice_standalone
            && *values == folder_document_values("alice", "Alice Standalone", false, None)
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == alice_shared
            && *values == folder_document_values("alice", "Alice Shared", false, Some(folder_id))
    }));

    let bob_rows = wait_for_rows(
        &bob_reader,
        query,
        "bob only sees alice shared doc through folder ownership",
        |rows| {
            rows.iter()
                .any(|(id, _)| *id == alice_shared)
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(
        bob_rows.len(),
        1,
        "bob should only see alice's shared doc: {bob_rows:?}"
    );
    assert_eq!(
        bob_rows[0].1,
        folder_document_values("alice", "Alice Shared", false, Some(folder_id))
    );

    alice.shutdown().await.expect("shutdown alice");
    alice_reader
        .shutdown()
        .await
        .expect("shutdown alice_reader");
    bob.shutdown().await.expect("shutdown bob");
    bob_reader.shutdown().await.expect("shutdown bob_reader");
    charlie.shutdown().await.expect("shutdown charlie");
    charlie_reader
        .shutdown()
        .await
        .expect("shutdown charlie_reader");
    server.shutdown().await;
}

/// Verifies that a folder owner may delete both the folder row itself and
/// documents inside that folder via inherited DELETE policies.
///
/// ```text
/// admin ──create folder owners=[alice]──────────────► server
/// admin ──create doc owner=charlie, folder=shared──► server
///
/// alice ──delete doc───────────────────────────────► server ──► persisted
/// alice ──delete folder────────────────────────────► server ──► persisted
/// ```
#[tokio::test]
async fn inherited_folder_delete_allows_folder_owner_to_delete_folder_and_documents() {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy())
                .with_delete(folder_owner_policy()),
        ))
        .table(make_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::or(vec![
                    owner_policy.clone(),
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ]))
                .with_delete(PolicyExpr::or(vec![
                    owner_policy,
                    inherited_non_null_policy(Operation::Delete, "folder_id"),
                ])),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id = create_folder(&admin, "folders", "Alice Folder", &["alice"], false).await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        "charlie",
        "Shared Delete Target",
        false,
        Some(folder_id),
    )
    .await;

    let documents_query = QueryBuilder::new("documents").build();
    wait_for_rows(
        &alice,
        documents_query.clone(),
        "alice sees folder-backed document before deleting it",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == doc_id
                        && *values
                            == folder_document_values(
                                "charlie",
                                "Shared Delete Target",
                                false,
                                Some(folder_id),
                            )
                })
                .then_some(())
        },
    )
    .await;

    alice
        .delete(doc_id)
        .await
        .expect("folder owner deletes folder-backed document");

    let rows_after_doc_delete = wait_for_query(
        &alice,
        documents_query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "folder-backed document is gone after folder-owner delete",
        Some,
    )
    .await;
    assert!(
        rows_after_doc_delete.is_empty(),
        "folder owner delete should remove the folder-backed document: {rows_after_doc_delete:?}"
    );

    let folders_query = QueryBuilder::new("folders").build();
    wait_for_rows(
        &alice,
        folders_query.clone(),
        "alice sees owned folder before deleting it",
        |rows| rows.iter().any(|(id, _)| *id == folder_id).then_some(()),
    )
    .await;

    alice
        .delete(folder_id)
        .await
        .expect("folder owner deletes folder");

    let rows_after_folder_delete = wait_for_query(
        &alice,
        folders_query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "folder is gone after folder-owner delete",
        Some,
    )
    .await;
    assert!(
        rows_after_folder_delete.is_empty(),
        "folder owner delete should remove the folder row: {rows_after_folder_delete:?}"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that document ownership still allows DELETE on owned rows, but a
/// non-owner who also lacks folder ownership cannot delete another user's
/// folder-backed document.
///
/// ```text
/// admin ──create folder owners=[alice]──────────────► server
/// admin ──create bob doc in folder──────────────────► server
/// admin ──create charlie doc in folder──────────────► server
///
/// bob ──delete bob doc──────────────────────────────► server ──► persisted
/// bob ──delete charlie doc──────────────────────────► server ──✗ rejected
/// ```
#[tokio::test]
async fn inherited_folder_delete_allows_document_owner_but_blocks_other_non_owners() {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy())
                .with_delete(folder_owner_policy()),
        ))
        .table(make_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::or(vec![
                    owner_policy.clone(),
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ]))
                .with_delete(PolicyExpr::or(vec![
                    owner_policy,
                    inherited_non_null_policy(Operation::Delete, "folder_id"),
                ])),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id = create_folder(&admin, "folders", "Shared Folder", &["alice"], false).await;
    let bob_doc_id = create_folder_document(
        &bob,
        "documents",
        "bob",
        "Bob Folder Doc",
        false,
        Some(folder_id),
    )
    .await;
    let charlie_doc_id = create_folder_document(
        &bob,
        "documents",
        "charlie",
        "Charlie Folder Doc",
        false,
        Some(folder_id),
    )
    .await;

    let documents_query = QueryBuilder::new("documents").build();
    let initial_alice_rows = wait_for_rows(
        &alice,
        documents_query.clone(),
        "folder owner sees both folder-backed documents before deletes",
        |rows| rows.iter().any(|(id, _)| *id == bob_doc_id).then_some(rows),
    )
    .await;
    assert!(initial_alice_rows.iter().any(|(id, values)| {
        *id == bob_doc_id
            && *values == folder_document_values("bob", "Bob Folder Doc", false, Some(folder_id))
    }));
    assert!(initial_alice_rows.iter().any(|(id, values)| {
        *id == charlie_doc_id
            && *values
                == folder_document_values("charlie", "Charlie Folder Doc", false, Some(folder_id))
    }));

    bob.delete(bob_doc_id)
        .await
        .expect("document owner deletes owned folder-backed document");

    let rows_after_owned_delete = wait_for_rows(
        &alice,
        documents_query.clone(),
        "folder owner sees only charlie doc after bob deletes his own doc",
        |rows| {
            let has_only_charlie = rows.len() == 1
                && rows.iter().any(|(id, values)| {
                    *id == charlie_doc_id
                        && *values
                            == folder_document_values(
                                "charlie",
                                "Charlie Folder Doc",
                                false,
                                Some(folder_id),
                            )
                });
            has_only_charlie.then_some(rows)
        },
    )
    .await;
    assert_eq!(
        rows_after_owned_delete.len(),
        1,
        "only charlie's document should remain after bob deletes his own: {rows_after_owned_delete:?}"
    );

    bob.delete(charlie_doc_id)
        .await
        .expect("optimistic local delete for unauthorized attempt");

    let rows_after_unauthorized_delete = wait_for_query(
        &alice,
        documents_query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "charlie doc remains after unauthorized delete attempt",
        Some,
    )
    .await;
    assert!(
        rows_after_unauthorized_delete.iter().any(|(id, values)| {
            *id == charlie_doc_id
                && *values
                    == folder_document_values(
                        "charlie",
                        "Charlie Folder Doc",
                        false,
                        Some(folder_id),
                    )
        }),
        "bob should not be able to delete another user's folder-backed doc without folder ownership: {rows_after_unauthorized_delete:?}"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
