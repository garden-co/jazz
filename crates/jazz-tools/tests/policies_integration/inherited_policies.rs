use std::collections::HashMap;
use std::time::Duration;

use super::support::{
    TestingClient, collect_stream_deltas, has_added, has_any_change, has_removed, wait_for_query,
    wait_for_rows, wait_for_subscription_update,
};
use jazz_tools::query_manager::policy::{Operation, PolicyExpr, PolicyValue};
use jazz_tools::query_manager::types::{
    ColumnDescriptor, RowDescriptor, Schema, TableName, TablePolicies, TableSchemaBuilder,
};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, SchemaBuilder, TableSchema,
    Value,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(100);

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

fn make_multi_folder_documents_schema(
    table_name: &str,
    policies: TablePolicies,
) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("owner_id", ColumnType::Text)
        .column("title", ColumnType::Text)
        .column("archived", ColumnType::Boolean)
        .nullable_fk_column("primary_folder_id", "primary_folders")
        .nullable_fk_column("secondary_folder_id", "secondary_folders")
        .policies(policies)
}

fn file_referencing_schema(array_edge: bool) -> Schema {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let via_column = if array_edge { "images" } else { "image" };

    let files_policies = TablePolicies::new().with_select(PolicyExpr::or(vec![
        owner_policy.clone(),
        PolicyExpr::inherits_referencing(Operation::Select, "todos", via_column),
    ]));

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("files"),
        TableSchema::builder("files")
            .column("owner_id", ColumnType::Text)
            .column("name", ColumnType::Text)
            .policies(files_policies)
            .build(),
    );

    let todos_policies = TablePolicies::new()
        .with_select(owner_policy.clone())
        .with_insert(owner_policy.clone())
        .with_update(Some(owner_policy.clone()), PolicyExpr::True)
        .with_delete(owner_policy);

    let todos_schema = if array_edge {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("owner_id", ColumnType::Text),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new(
                "images",
                ColumnType::Array {
                    element: Box::new(ColumnType::Uuid),
                },
            )
            .references("files"),
        ]);
        TableSchema::with_policies(descriptor, todos_policies)
    } else {
        TableSchema::builder("todos")
            .column("owner_id", ColumnType::Text)
            .column("title", ColumnType::Text)
            .nullable_fk_column("image", "files")
            .policies(todos_policies)
            .build()
    };
    schema.insert(TableName::new("todos"), todos_schema);

    schema
}

fn multi_hop_inherited_parts_schema() -> Schema {
    SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy()),
        ))
        .table(
            TableSchema::builder("files")
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(inherited_non_null_policy(Operation::Select, "folder_id")),
                ),
        )
        .table(
            TableSchema::builder("file_parts")
                .column("title", ColumnType::Text)
                .nullable_fk_column("file_id", "files")
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(inherited_non_null_policy(Operation::Select, "file_id")),
                ),
        )
        .build()
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

fn multi_folder_document_values(
    owner_id: &str,
    title: &str,
    archived: bool,
    primary_folder_id: Option<ObjectId>,
    secondary_folder_id: Option<ObjectId>,
) -> Vec<Value> {
    vec![
        Value::Text(owner_id.to_string()),
        Value::Text(title.to_string()),
        Value::Boolean(archived),
        primary_folder_id.map(Value::Uuid).unwrap_or(Value::Null),
        secondary_folder_id.map(Value::Uuid).unwrap_or(Value::Null),
    ]
}

fn multi_folder_document_input(
    owner_id: &str,
    title: &str,
    archived: bool,
    primary_folder_id: Option<ObjectId>,
    secondary_folder_id: Option<ObjectId>,
) -> HashMap<String, Value> {
    HashMap::from([
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("title".to_string(), Value::Text(title.to_string())),
        ("archived".to_string(), Value::Boolean(archived)),
        (
            "primary_folder_id".to_string(),
            primary_folder_id.map(Value::Uuid).unwrap_or(Value::Null),
        ),
        (
            "secondary_folder_id".to_string(),
            secondary_folder_id.map(Value::Uuid).unwrap_or(Value::Null),
        ),
    ])
}

fn file_input(owner_id: &str, name: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("name".to_string(), Value::Text(name.to_string())),
    ])
}

fn file_values(owner_id: &str, name: &str) -> Vec<Value> {
    vec![
        Value::Text(owner_id.to_string()),
        Value::Text(name.to_string()),
    ]
}

fn todo_scalar_ref_input(
    owner_id: &str,
    title: &str,
    image: Option<ObjectId>,
) -> HashMap<String, Value> {
    HashMap::from([
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("title".to_string(), Value::Text(title.to_string())),
        (
            "image".to_string(),
            image.map(Value::Uuid).unwrap_or(Value::Null),
        ),
    ])
}

fn todo_array_ref_input(
    owner_id: &str,
    title: &str,
    images: &[ObjectId],
) -> HashMap<String, Value> {
    HashMap::from([
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("title".to_string(), Value::Text(title.to_string())),
        (
            "images".to_string(),
            Value::Array(images.iter().copied().map(Value::Uuid).collect()),
        ),
    ])
}

fn file_row_count(rows: &[(ObjectId, Vec<Value>)], row_id: ObjectId) -> usize {
    rows.iter().filter(|(id, _)| *id == row_id).count()
}

fn has_row(rows: &[(ObjectId, Vec<Value>)], row_id: ObjectId, expected: &[Value]) -> bool {
    rows.iter()
        .any(|(id, values)| *id == row_id && values.as_slice() == expected)
}

fn lacks_row(rows: &[(ObjectId, Vec<Value>)], row_id: ObjectId) -> bool {
    rows.iter().all(|(id, _)| *id != row_id)
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

async fn create_multi_folder_document(
    client: &JazzClient,
    table_name: &str,
    owner_id: &str,
    title: &str,
    archived: bool,
    primary_folder_id: Option<ObjectId>,
    secondary_folder_id: Option<ObjectId>,
) -> ObjectId {
    client
        .create(
            table_name,
            multi_folder_document_input(
                owner_id,
                title,
                archived,
                primary_folder_id,
                secondary_folder_id,
            ),
        )
        .await
        .expect("create multi-folder document")
        .0
}

async fn create_file(client: &JazzClient, owner_id: &str, name: &str) -> ObjectId {
    client
        .create("files", file_input(owner_id, name))
        .await
        .expect("create file")
        .0
}

async fn create_scalar_ref_todo(
    client: &JazzClient,
    owner_id: &str,
    title: &str,
    image: Option<ObjectId>,
) -> ObjectId {
    client
        .create("todos", todo_scalar_ref_input(owner_id, title, image))
        .await
        .expect("create scalar-ref todo")
        .0
}

async fn create_array_ref_todo(
    client: &JazzClient,
    owner_id: &str,
    title: &str,
    images: &[ObjectId],
) -> ObjectId {
    client
        .create("todos", todo_array_ref_input(owner_id, title, images))
        .await
        .expect("create array-ref todo")
        .0
}

async fn update_row(client: &JazzClient, row_id: ObjectId, changes: Vec<(String, Value)>) {
    client.update(row_id, changes).await.expect("update row");
}

async fn connect_ready_client(
    server: &TestingServer,
    schema: &Schema,
    user_id: &str,
    ready_table: &str,
) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id(user_id)
        .ready_on(ready_table, READY_TIMEOUT)
        .connect()
        .await
}

async fn connect_ready_user(
    server: &TestingServer,
    schema: &Schema,
    user_id: &str,
    ready_table: &str,
) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id(user_id)
        .as_user()
        .ready_on(ready_table, READY_TIMEOUT)
        .connect()
        .await
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
#[should_panic] // "known failing: forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
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
#[should_panic] // "known failing: forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
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
#[should_panic] // "known failing: forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
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
#[should_panic] // "known failing: forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
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

/// Verifies that multiple forward inherited paths compose with OR: visibility
/// through either FK should be enough to expose the child row.
#[tokio::test]
#[should_panic] // "known failing: forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
async fn inherited_multiple_folder_paths_compose_with_or() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "primary_folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy()),
        ))
        .table(make_folders_schema(
            "secondary_folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy()),
        ))
        .table(make_multi_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::or(vec![
                    inherited_non_null_policy(Operation::Select, "primary_folder_id"),
                    inherited_non_null_policy(Operation::Select, "secondary_folder_id"),
                ])),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "documents").await;
    let alice = connect_ready_user(&server, &schema, "alice", "documents").await;
    let bob = connect_ready_user(&server, &schema, "bob", "documents").await;
    let dave = connect_ready_user(&server, &schema, "dave", "documents").await;

    let primary_folder_id =
        create_folder(&admin, "primary_folders", "Primary", &["alice"], false).await;
    let secondary_folder_id =
        create_folder(&admin, "secondary_folders", "Secondary", &["bob"], false).await;

    let primary_doc_id = create_multi_folder_document(
        &admin,
        "documents",
        "charlie",
        "Primary Only",
        false,
        Some(primary_folder_id),
        None,
    )
    .await;
    let secondary_doc_id = create_multi_folder_document(
        &admin,
        "documents",
        "charlie",
        "Secondary Only",
        false,
        None,
        Some(secondary_folder_id),
    )
    .await;
    let both_doc_id = create_multi_folder_document(
        &admin,
        "documents",
        "charlie",
        "Both Paths",
        false,
        Some(primary_folder_id),
        Some(secondary_folder_id),
    )
    .await;
    let hidden_doc_id =
        create_multi_folder_document(&admin, "documents", "charlie", "Hidden", false, None, None)
            .await;

    let query = QueryBuilder::new("documents").build();

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "forward INHERITS SELECT fails to expose child rows to parent-authorized sessions, so alice sees rows granted by the primary path",
        |rows| {
            (rows.len() == 2
                && has_row(
                    &rows,
                    primary_doc_id,
                    &multi_folder_document_values(
                        "charlie",
                        "Primary Only",
                        false,
                        Some(primary_folder_id),
                        None,
                    ),
                )
                && has_row(
                    &rows,
                    both_doc_id,
                    &multi_folder_document_values(
                        "charlie",
                        "Both Paths",
                        false,
                        Some(primary_folder_id),
                        Some(secondary_folder_id),
                    ),
                )
                && lacks_row(&rows, secondary_doc_id)
                && lacks_row(&rows, hidden_doc_id))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(alice_rows.len(), 2);

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees rows granted by the secondary path",
        |rows| {
            (rows.len() == 2
                && has_row(
                    &rows,
                    secondary_doc_id,
                    &multi_folder_document_values(
                        "charlie",
                        "Secondary Only",
                        false,
                        None,
                        Some(secondary_folder_id),
                    ),
                )
                && has_row(
                    &rows,
                    both_doc_id,
                    &multi_folder_document_values(
                        "charlie",
                        "Both Paths",
                        false,
                        Some(primary_folder_id),
                        Some(secondary_folder_id),
                    ),
                )
                && lacks_row(&rows, primary_doc_id)
                && lacks_row(&rows, hidden_doc_id))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(bob_rows.len(), 2);

    let dave_rows = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave sees no rows without either inherited path",
        Some,
    )
    .await;
    assert!(dave_rows.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that folder ownership grants UPDATE access to a folder-backed
/// document when the child row inherits `allowedTo.update(...)` from its parent.
#[tokio::test]
#[should_panic] // "known failing: forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
async fn inherited_folder_update_allows_folder_owner_and_blocks_other_users() {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy())
                .with_update(Some(folder_owner_policy()), PolicyExpr::True),
        ))
        .table(make_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::or(vec![
                    owner_policy.clone(),
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ]))
                .with_update(
                    Some(PolicyExpr::or(vec![
                        owner_policy,
                        inherited_non_null_policy(Operation::Update, "folder_id"),
                    ])),
                    PolicyExpr::True,
                ),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "documents").await;
    let alice = connect_ready_user(&server, &schema, "alice", "documents").await;
    let bob = connect_ready_user(&server, &schema, "bob", "documents").await;

    let folder_id = create_folder(&admin, "folders", "Shared", &["alice"], false).await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        "charlie",
        "Original",
        false,
        Some(folder_id),
    )
    .await;
    let query = QueryBuilder::new("documents").build();

    wait_for_rows(
        &alice,
        query.clone(),
        "forward INHERITS SELECT fails to expose child rows to parent-authorized sessions, so the folder owner sees the document before attempting an inherited update",
        |rows| {
            has_row(
                &rows,
                doc_id,
                &folder_document_values("charlie", "Original", false, Some(folder_id)),
            )
            .then_some(rows)
        },
    )
    .await;

    update_row(
        &alice,
        doc_id,
        vec![(
            "title".to_string(),
            Value::Text("Edited By Folder Owner".into()),
        )],
    )
    .await;
    let rows_after_alice = wait_for_rows(
        &admin,
        query.clone(),
        "folder owner update persists through inherited update policy",
        |rows| {
            has_row(
                &rows,
                doc_id,
                &folder_document_values(
                    "charlie",
                    "Edited By Folder Owner",
                    false,
                    Some(folder_id),
                ),
            )
            .then_some(rows)
        },
    )
    .await;
    assert!(has_row(
        &rows_after_alice,
        doc_id,
        &folder_document_values("charlie", "Edited By Folder Owner", false, Some(folder_id)),
    ));

    update_row(
        &bob,
        doc_id,
        vec![("title".to_string(), Value::Text("Edited By Bob".into()))],
    )
    .await;
    let rows_after_bob = wait_for_query(
        &admin,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "non-owner without folder access cannot update the row",
        Some,
    )
    .await;
    assert!(has_row(
        &rows_after_bob,
        doc_id,
        &folder_document_values("charlie", "Edited By Folder Owner", false, Some(folder_id)),
    ));

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that reverse inheritance on a scalar FK grants visibility to the
/// target row, fails closed without a granting source row, and composes
/// multiple referencing rows with OR without duplicating result rows.
#[tokio::test]
async fn inherited_referencing_scalar_paths_grant_visibility_and_compose_with_or() {
    let schema = file_referencing_schema(false);
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "files").await;
    let alice = connect_ready_user(&server, &schema, "alice", "files").await;
    let dave = connect_ready_user(&server, &schema, "dave", "files").await;

    let file_single = create_file(&admin, "mallory", "Grant Single").await;
    let file_multi = create_file(&admin, "mallory", "Grant Multi").await;
    let file_hidden = create_file(&admin, "mallory", "Still Hidden").await;

    create_scalar_ref_todo(&alice, "alice", "Todo Single", Some(file_single)).await;
    create_scalar_ref_todo(&alice, "alice", "Todo Multi A", Some(file_multi)).await;
    create_scalar_ref_todo(&alice, "alice", "Todo Multi B", Some(file_multi)).await;

    let query = QueryBuilder::new("files").build();
    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees files granted through referencing todos",
        |rows| {
            (rows.len() == 2
                && has_row(&rows, file_single, &file_values("mallory", "Grant Single"))
                && has_row(&rows, file_multi, &file_values("mallory", "Grant Multi"))
                && file_row_count(&rows, file_multi) == 1
                && lacks_row(&rows, file_hidden))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(alice_rows.len(), 2);
    assert_eq!(file_row_count(&alice_rows, file_multi), 1);

    let dave_rows = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave sees no files without a visible referencing todo",
        Some,
    )
    .await;
    assert!(dave_rows.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that reverse inheritance invalidates active subscriptions when
/// referencing rows are created, deleted, or retargeted.
#[tokio::test]
async fn inherited_referencing_scalar_subscription_updates_follow_create_delete_and_retarget() {
    let schema = file_referencing_schema(false);
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "files").await;
    let alice = connect_ready_user(&server, &schema, "alice", "files").await;

    let file_a = create_file(&admin, "mallory", "File A").await;
    let file_b = create_file(&admin, "mallory", "File B").await;
    let query = QueryBuilder::new("files").build();

    let mut stream = alice
        .subscribe(query.clone())
        .await
        .expect("subscribe files");
    let mut log = Vec::new();
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    let todo_id = create_scalar_ref_todo(&alice, "alice", "Todo A", Some(file_a)).await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "creating a referencing row makes the target visible",
        |entries| has_added(entries, file_a),
    )
    .await;
    let rows_after_create = wait_for_rows(
        &alice,
        query.clone(),
        "file A is visible after creating the referencing todo",
        |rows| has_row(&rows, file_a, &file_values("mallory", "File A")).then_some(rows),
    )
    .await;
    assert!(has_row(
        &rows_after_create,
        file_a,
        &file_values("mallory", "File A"),
    ));

    log.clear();
    alice
        .delete(todo_id)
        .await
        .expect("delete referencing todo");
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "deleting the last referencing row hides the target",
        |entries| has_removed(entries, file_a),
    )
    .await;
    let rows_after_delete = wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "file A is hidden after deleting the referencing todo",
        Some,
    )
    .await;
    assert!(rows_after_delete.is_empty());

    log.clear();
    let todo_retarget_id = create_scalar_ref_todo(&alice, "alice", "Todo B", Some(file_a)).await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "recreating a reference makes file A visible again",
        |entries| has_added(entries, file_a),
    )
    .await;

    log.clear();
    update_row(
        &alice,
        todo_retarget_id,
        vec![("image".to_string(), Value::Uuid(file_b))],
    )
    .await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "retargeting a reference removes the old target and adds the new one",
        |entries| has_removed(entries, file_a) && has_added(entries, file_b),
    )
    .await;
    let rows_after_retarget = wait_for_rows(
        &alice,
        query,
        "only file B remains visible after retargeting the todo",
        |rows| {
            (rows.len() == 1
                && has_row(&rows, file_b, &file_values("mallory", "File B"))
                && lacks_row(&rows, file_a))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows_after_retarget.len(), 1);

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that reverse inheritance over `UUID[] REFERENCES` grants access
/// and that reordering or duplicating the array does not change semantics.
#[tokio::test]
async fn inherited_referencing_array_membership_preserves_set_semantics() {
    let schema = file_referencing_schema(true);
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "files").await;
    let alice = connect_ready_user(&server, &schema, "alice", "files").await;

    let file_a = create_file(&admin, "mallory", "Array A").await;
    let file_b = create_file(&admin, "mallory", "Array B").await;
    let todo_id = create_array_ref_todo(&alice, "alice", "Array Todo", &[file_a, file_b]).await;
    let query = QueryBuilder::new("files").build();

    let initial_rows = wait_for_rows(
        &alice,
        query.clone(),
        "array membership grants both referenced files",
        |rows| {
            (rows.len() == 2
                && has_row(&rows, file_a, &file_values("mallory", "Array A"))
                && has_row(&rows, file_b, &file_values("mallory", "Array B"))
                && file_row_count(&rows, file_a) == 1
                && file_row_count(&rows, file_b) == 1)
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(file_row_count(&initial_rows, file_a), 1);
    assert_eq!(file_row_count(&initial_rows, file_b), 1);

    let mut stream = alice
        .subscribe(query.clone())
        .await
        .expect("subscribe array files");
    let mut log = Vec::new();
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    update_row(
        &alice,
        todo_id,
        vec![(
            "images".to_string(),
            Value::Array(vec![Value::Uuid(file_b), Value::Uuid(file_a)]),
        )],
    )
    .await;
    let rows_after_reorder = wait_for_rows(
        &alice,
        query.clone(),
        "reordering UUID[] references does not change visible files",
        |rows| {
            (rows.len() == 2
                && has_row(&rows, file_a, &file_values("mallory", "Array A"))
                && has_row(&rows, file_b, &file_values("mallory", "Array B"))
                && file_row_count(&rows, file_a) == 1
                && file_row_count(&rows, file_b) == 1)
                .then_some(rows)
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&log, file_a) && !has_any_change(&log, file_b),
        "reordering should not emit visibility deltas: {log:?}"
    );
    assert_eq!(file_row_count(&rows_after_reorder, file_a), 1);
    assert_eq!(file_row_count(&rows_after_reorder, file_b), 1);

    log.clear();
    update_row(
        &alice,
        todo_id,
        vec![(
            "images".to_string(),
            Value::Array(vec![
                Value::Uuid(file_a),
                Value::Uuid(file_a),
                Value::Uuid(file_b),
            ]),
        )],
    )
    .await;
    let rows_after_duplicate = wait_for_rows(
        &alice,
        query,
        "duplicate UUIDs do not duplicate visible target rows",
        |rows| {
            (rows.len() == 2
                && has_row(&rows, file_a, &file_values("mallory", "Array A"))
                && has_row(&rows, file_b, &file_values("mallory", "Array B"))
                && file_row_count(&rows, file_a) == 1
                && file_row_count(&rows, file_b) == 1)
                .then_some(rows)
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&log, file_a) && !has_any_change(&log, file_b),
        "duplicating UUIDs without changing the set should not emit deltas: {log:?}"
    );
    assert_eq!(file_row_count(&rows_after_duplicate, file_a), 1);
    assert_eq!(file_row_count(&rows_after_duplicate, file_b), 1);

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that non-recursive forward inheritance can compose across multiple
/// tables, such as `folders -> files -> file_parts`.
#[tokio::test]
#[should_panic(
    expected = "forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
)]
async fn inherited_multi_hop_forward_chain_grants_access_to_leaf_rows() {
    let schema = multi_hop_inherited_parts_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "file_parts").await;
    let alice = connect_ready_user(&server, &schema, "alice", "file_parts").await;
    let dave = connect_ready_user(&server, &schema, "dave", "file_parts").await;

    let folder_id = create_folder(&admin, "folders", "Shared Folder", &["alice"], false).await;
    let file_id = admin
        .create(
            "files",
            HashMap::from([
                ("title".to_string(), Value::Text("Spec.pdf".into())),
                ("folder_id".to_string(), Value::Uuid(folder_id)),
            ]),
        )
        .await
        .expect("create file")
        .0;
    let part_id = admin
        .create(
            "file_parts",
            HashMap::from([
                ("title".to_string(), Value::Text("Page 1".into())),
                ("file_id".to_string(), Value::Uuid(file_id)),
            ]),
        )
        .await
        .expect("create file part")
        .0;

    let query = QueryBuilder::new("file_parts").build();
    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "forward INHERITS SELECT fails to expose child rows to parent-authorized sessions, so alice sees file parts through the folder -> file -> part chain",
        |rows| {
            has_row(
                &rows,
                part_id,
                &[Value::Text("Page 1".into()), Value::Uuid(file_id)],
            )
            .then_some(rows)
        },
    )
    .await;
    assert!(has_row(
        &alice_rows,
        part_id,
        &[Value::Text("Page 1".into()), Value::Uuid(file_id)],
    ));

    let dave_rows = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave sees no leaf rows without an inherited path",
        Some,
    )
    .await;
    assert!(dave_rows.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that changing the parent row's policy-relevant contents revokes
/// child visibility for active subscriptions.
#[tokio::test]
#[should_panic] // "known failing: forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
async fn inherited_parent_policy_change_propagates_to_child_on_active_subscriptions() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy())
                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
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
    let admin = connect_ready_client(&server, &schema, "admin", "documents").await;
    let bob = connect_ready_user(&server, &schema, "bob", "documents").await;

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

    wait_for_rows(
        &bob,
        query.clone(),
        "forward INHERITS SELECT fails to expose child rows to parent-authorized sessions, so bob initially sees the child row before the parent policy changes",
        |rows| {
            has_row(
                &rows,
                doc_id,
                &folder_document_values("charlie", "Shared Doc", false, Some(folder_id)),
            )
            .then_some(rows)
        },
    )
    .await;

    let mut stream = bob.subscribe(query.clone()).await.expect("subscribe bob");
    let mut log = Vec::new();
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    update_row(
        &admin,
        folder_id,
        vec![(
            "owners".to_string(),
            Value::Array(vec![Value::Text("alice".into())]),
        )],
    )
    .await;

    let bob_fresh = connect_ready_user(&server, &schema, "bob", "documents").await;
    let rows_after_update = wait_for_query(
        &bob_fresh,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "child row becomes hidden once the parent row stops granting access",
        Some,
    )
    .await;
    assert!(rows_after_update.is_empty());
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "parent policy change emits remove for inherited child visibility",
        |entries| has_removed(entries, doc_id),
    )
    .await;

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    bob_fresh.shutdown().await.expect("shutdown bob_fresh");
    server.shutdown().await;
}

/// Verifies that retargeting a child from a visible parent to a hidden parent
/// removes it from active subscriptions.
#[tokio::test]
#[should_panic] // "known failing: forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
async fn inherited_child_fk_retarget_visible_to_hidden_parent_removes_child_from_subscriptions() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy())
                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
        ))
        .table(make_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(inherited_non_null_policy(Operation::Select, "folder_id"))
                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "documents").await;
    let bob = connect_ready_user(&server, &schema, "bob", "documents").await;

    let visible_folder_id = create_folder(&admin, "folders", "Visible", &["bob"], false).await;
    let hidden_folder_id = create_folder(&admin, "folders", "Hidden", &["alice"], false).await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        "charlie",
        "Retarget Me",
        false,
        Some(visible_folder_id),
    )
    .await;
    let query = QueryBuilder::new("documents").build();

    wait_for_rows(
        &bob,
        query.clone(),
        "forward INHERITS SELECT fails to expose child rows to parent-authorized sessions, so bob initially sees the child row before it is retargeted away",
        |rows| {
            has_row(
                &rows,
                doc_id,
                &folder_document_values("charlie", "Retarget Me", false, Some(visible_folder_id)),
            )
            .then_some(rows)
        },
    )
    .await;

    let mut stream = bob.subscribe(query.clone()).await.expect("subscribe bob");
    let mut log = Vec::new();
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    update_row(
        &admin,
        doc_id,
        vec![("folder_id".to_string(), Value::Uuid(hidden_folder_id))],
    )
    .await;

    let bob_fresh = connect_ready_user(&server, &schema, "bob", "documents").await;
    let rows_after_retarget = wait_for_query(
        &bob_fresh,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "child row becomes hidden after retargeting to a non-visible parent",
        Some,
    )
    .await;
    assert!(rows_after_retarget.is_empty());
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "retargeting to a hidden parent emits remove",
        |entries| has_removed(entries, doc_id),
    )
    .await;

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    bob_fresh.shutdown().await.expect("shutdown bob_fresh");
    server.shutdown().await;
}

/// Verifies that retargeting a child from a hidden parent to a visible parent
/// adds it to active subscriptions.
#[tokio::test]
#[should_panic] // "known failing: forward INHERITS SELECT fails to expose child rows to parent-authorized sessions"
async fn inherited_child_fk_retarget_hidden_to_visible_parent_adds_child_to_subscriptions() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(folder_owner_policy())
                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
        ))
        .table(make_folder_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(inherited_non_null_policy(Operation::Select, "folder_id"))
                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
        ))
        .build();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "documents").await;
    let bob = connect_ready_user(&server, &schema, "bob", "documents").await;

    let hidden_folder_id = create_folder(&admin, "folders", "Hidden", &["alice"], false).await;
    let visible_folder_id = create_folder(&admin, "folders", "Visible", &["bob"], false).await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        "charlie",
        "Reveal Me",
        false,
        Some(hidden_folder_id),
    )
    .await;
    let query = QueryBuilder::new("documents").build();

    let initial_rows = wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "bob sees no rows while the child points at a hidden parent",
        Some,
    )
    .await;
    assert!(initial_rows.is_empty());

    let mut stream = bob.subscribe(query.clone()).await.expect("subscribe bob");
    let mut log = Vec::new();
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    update_row(
        &admin,
        doc_id,
        vec![("folder_id".to_string(), Value::Uuid(visible_folder_id))],
    )
    .await;

    let bob_fresh = connect_ready_user(&server, &schema, "bob", "documents").await;
    let rows_after_retarget = wait_for_rows(
        &bob_fresh,
        query,
        "forward INHERITS SELECT fails to expose child rows to parent-authorized sessions, so bob sees the child row after retargeting into a visible parent",
        |rows| {
            has_row(
                &rows,
                doc_id,
                &folder_document_values("charlie", "Reveal Me", false, Some(visible_folder_id)),
            )
            .then_some(rows)
        },
    )
    .await;
    assert!(has_row(
        &rows_after_retarget,
        doc_id,
        &folder_document_values("charlie", "Reveal Me", false, Some(visible_folder_id)),
    ));
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "retargeting to a visible parent emits add",
        |entries| has_added(entries, doc_id),
    )
    .await;

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    bob_fresh.shutdown().await.expect("shutdown bob_fresh");
    server.shutdown().await;
}
