use std::collections::HashMap;
use std::time::Duration;

use jazz::query::Query;

use super::support::{
    TestingClient, collect_stream_deltas, connect_ready_client, connect_ready_user, has_added_id,
    has_any_change, has_removed, has_row, lacks_row, wait_for_edge_txs, wait_for_query,
    wait_for_rows, wait_for_subscription_update,
};
use super::{assert_client_policy_denied, pe, permissions};
use jazz::tools::{ColumnDescriptor, RowDescriptor, Schema, TableName};
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, SchemaBuilder, TablePolicies, TableSchema,
    TableSchemaBuilder, Value,
};
use jazz::tools::{Operation, PolicyExpr};
use jazz_server::JazzServer;

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
        .policies(super::explicit_allow_all_policies(policies))
}

fn make_folder_documents_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("owner_id", ColumnType::Text)
        .column("title", ColumnType::Text)
        .column("archived", ColumnType::Boolean)
        .nullable_fk_column("folder_id", "folders")
        .policies(super::explicit_allow_all_policies(policies))
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
        .policies(super::explicit_allow_all_policies(policies))
}

fn file_referencing_schema(array_edge: bool) -> Schema {
    let owner_policy = pe::eq("owner_id", pe::session(vec!["claims", "sub"]));
    let via_column = if array_edge { "images" } else { "image" };

    let files_policies = super::explicit_allow_all_policies(permissions(|p| {
        p.allow_read().where_(pe::any_of([
            owner_policy.clone(),
            pe::allowed_to_read_referencing("todos", via_column),
        ]));
    }));

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("files"),
        TableSchema::builder("files")
            .column("owner_id", ColumnType::Text)
            .column("name", ColumnType::Text)
            .policies(files_policies)
            .build(),
    );

    let todos_policies = super::explicit_allow_all_policies(permissions(|p| {
        p.allow_read().where_(owner_policy.clone());
        p.allow_insert().where_(owner_policy.clone());
        p.allow_update()
            .where_old(owner_policy.clone())
            .where_new(pe::always());
        p.allow_delete().where_(owner_policy);
    }));

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
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
            }),
        ))
        .table(
            TableSchema::builder("files")
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read()
                        .where_(inherited_non_null_policy(Operation::Select, "folder_id"));
                })),
        )
        .table(
            TableSchema::builder("file_parts")
                .column("title", ColumnType::Text)
                .nullable_fk_column("file_id", "files")
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read()
                        .where_(inherited_non_null_policy(Operation::Select, "file_id"));
                })),
        )
        .build()
}

// -- Policy helpers --

fn folder_owner_policy() -> PolicyExpr {
    pe::contains("owners", pe::session(vec!["claims", "sub"]))
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
        Some(depth) => pe::allowed_to_with_depth(operation, via_column, depth),
        None => pe::allowed_to(operation, via_column),
    };

    pe::all_of([pe::is_not_null(via_column), inherits])
}

// -- Value constructors --

fn folder_input(title: &str, owners: &[&str], archived: bool) -> HashMap<String, Value> {
    jazz::row_input!(
        "title" => title,
        "owners" => Value::Array(owners.iter().map(|owner| (*owner).into()).collect()),
        "archived" => archived
    )
}

fn folder_document_values(
    owner_id: &str,
    title: &str,
    archived: bool,
    folder_id: Option<ObjectId>,
) -> Vec<Value> {
    vec![
        owner_id.into(),
        title.into(),
        archived.into(),
        folder_id.into(),
    ]
}

fn folder_document_input(
    owner_id: &str,
    title: &str,
    archived: bool,
    folder_id: Option<ObjectId>,
) -> HashMap<String, Value> {
    jazz::row_input!(
        "owner_id" => owner_id,
        "title" => title,
        "archived" => archived,
        "folder_id" => folder_id
    )
}

fn multi_folder_document_values(
    owner_id: &str,
    title: &str,
    archived: bool,
    primary_folder_id: Option<ObjectId>,
    secondary_folder_id: Option<ObjectId>,
) -> Vec<Value> {
    vec![
        owner_id.into(),
        title.into(),
        archived.into(),
        primary_folder_id.into(),
        secondary_folder_id.into(),
    ]
}

fn multi_folder_document_input(
    owner_id: &str,
    title: &str,
    archived: bool,
    primary_folder_id: Option<ObjectId>,
    secondary_folder_id: Option<ObjectId>,
) -> HashMap<String, Value> {
    jazz::row_input!(
        "owner_id" => owner_id,
        "title" => title,
        "archived" => archived,
        "primary_folder_id" => primary_folder_id,
        "secondary_folder_id" => secondary_folder_id
    )
}

fn file_input(owner_id: &str, name: &str) -> HashMap<String, Value> {
    jazz::row_input!(
        "owner_id" => owner_id,
        "name" => name
    )
}

fn file_values(owner_id: &str, name: &str) -> Vec<Value> {
    vec![owner_id.into(), name.into()]
}

fn todo_scalar_ref_input(
    owner_id: &str,
    title: &str,
    image: Option<ObjectId>,
) -> HashMap<String, Value> {
    jazz::row_input!(
        "owner_id" => owner_id,
        "title" => title,
        "image" => image
    )
}

fn todo_array_ref_input(
    owner_id: &str,
    title: &str,
    images: &[ObjectId],
) -> HashMap<String, Value> {
    jazz::row_input!(
        "owner_id" => owner_id,
        "title" => title,
        "images" => Value::Array(images.iter().copied().map(Value::Uuid).collect())
    )
}

fn file_row_count(rows: &[(ObjectId, Vec<Value>)], row_id: ObjectId) -> usize {
    rows.iter().filter(|(id, _)| *id == row_id).count()
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
        .insert(table_name, folder_input(title, owners, archived))
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
        .insert(
            table_name,
            folder_document_input(owner_id, title, archived, folder_id),
        )
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
        .insert(
            table_name,
            multi_folder_document_input(
                owner_id,
                title,
                archived,
                primary_folder_id,
                secondary_folder_id,
            ),
        )
        .expect("create multi-folder document")
        .0
}

async fn create_file(client: &JazzClient, owner_id: &str, name: &str) -> ObjectId {
    client
        .insert("files", file_input(owner_id, name))
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
        .insert("todos", todo_scalar_ref_input(owner_id, title, image))
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
        .insert("todos", todo_array_ref_input(owner_id, title, images))
        .expect("create array-ref todo")
        .0
}

async fn update_row(
    client: &JazzClient,
    table_name: &str,
    row_id: ObjectId,
    changes: Vec<(String, Value)>,
) {
    client
        .update(table_name, row_id, changes)
        .expect("update row");
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
    tokio::task::LocalSet::new()
        .run_until(inherited_folder_documents_are_visible_to_all_folder_owners_inner())
        .await;
}

async fn inherited_folder_documents_are_visible_to_all_folder_owners_inner() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read()
                    .where_(inherited_non_null_policy(Operation::Select, "folder_id"));
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .as_admin()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::ALICE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::BOB_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let charlie = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::CHARLIE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let dave = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id(super::DAVE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id = create_folder(
        &admin,
        "folders",
        "Shared",
        &[super::ALICE_ID, super::BOB_ID],
        false,
    )
    .await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Shared Doc",
        false,
        Some(folder_id),
    )
    .await;
    let query = Query::from("documents");

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees folder doc via inheritance",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        folder_document_values(super::CHARLIE_ID, "Shared Doc", false, Some(folder_id))
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
        folder_document_values(super::CHARLIE_ID, "Shared Doc", false, Some(folder_id))
    );

    let charlie_rows = wait_for_query(
        &charlie,
        query.clone(),
        jazz::tools::ReadTier::Remote,
        Duration::from_secs(3),
        "charlie sees no documents without folder ownership",
        Some,
    )
    .await;
    assert!(charlie_rows.is_empty());

    let dave_rows = wait_for_query(
        &dave,
        query,
        jazz::tools::ReadTier::Remote,
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
/// alice(reader) ──existing docs query─────────────────────────► sees removal
/// bob(fresh) ─────query docs─────────────────────────────────► sees nothing
/// ```
#[tokio::test]
async fn inherited_folder_documents_fail_closed_for_missing_and_deleted_folder_targets() {
    tokio::task::LocalSet::new()
        .run_until(
            inherited_folder_documents_fail_closed_for_missing_and_deleted_folder_targets_inner(),
        )
        .await;
}

async fn inherited_folder_documents_fail_closed_for_missing_and_deleted_folder_targets_inner() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
                p.allow_delete().where_(folder_owner_policy());
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read()
                    .where_(inherited_non_null_policy(Operation::Select, "folder_id"));
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let alice_writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::ALICE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::ALICE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let query = Query::from("documents");

    let folder_id = create_folder(
        &alice_writer,
        "folders",
        "Shared",
        &[super::ALICE_ID, super::BOB_ID],
        false,
    )
    .await;
    let missing_folder_id = ObjectId::new();
    let missing_parent_doc_id = create_folder_document(
        &alice_writer,
        "documents",
        super::CHARLIE_ID,
        "Bogus Parent",
        false,
        Some(missing_folder_id),
    )
    .await;

    let deleted_parent_doc_id = create_folder_document(
        &alice_writer,
        "documents",
        super::CHARLIE_ID,
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
        folder_document_values(super::CHARLIE_ID, "Deleted Parent", false, Some(folder_id))
    );
    assert!(
        visible_rows
            .iter()
            .all(|(id, _)| *id != missing_parent_doc_id),
        "document with a non-existent inherited parent should stay hidden: {visible_rows:?}"
    );

    alice_writer
        .delete("folders", folder_id)
        .expect("delete inherited parent folder");
    let alice_rows_after_delete = wait_for_query(
        &alice_reader,
        query.clone(),
        jazz::tools::ReadTier::Remote,
        QUERY_TIMEOUT,
        "alice's retained document view removes the deleted-parent document",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(
        alice_rows_after_delete.is_empty(),
        "a retained document view must not preserve a row after its inherited parent is deleted: {alice_rows_after_delete:?}"
    );
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id(super::BOB_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let folders_query = Query::from("folders");
    let folders_after_delete = wait_for_query(
        &bob,
        folders_query,
        jazz::tools::ReadTier::Remote,
        QUERY_TIMEOUT,
        "bob sees no folders after inherited parent delete",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(folders_after_delete.is_empty());

    let final_rows = wait_for_query(
        &bob,
        query,
        jazz::tools::ReadTier::Remote,
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
    tokio::task::LocalSet::new()
        .run_until(inherited_folder_access_extends_document_visibility_beyond_direct_owner_inner())
        .await;
}

async fn inherited_folder_access_extends_document_visibility_beyond_direct_owner_inner() {
    let owner_policy = pe::eq("owner_id", pe::session(vec!["claims", "sub"]));
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(pe::any_of([
                    owner_policy,
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ]));
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .as_admin()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::ALICE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::BOB_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let charlie = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::CHARLIE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let dave = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id(super::DAVE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id = create_folder(
        &admin,
        "folders",
        "Shared",
        &[super::ALICE_ID, super::BOB_ID],
        false,
    )
    .await;
    let standalone_id = create_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Standalone",
        false,
        None,
    )
    .await;
    let folder_doc_id = create_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Inside Folder",
        false,
        Some(folder_id),
    )
    .await;
    let query = Query::from("documents");

    let charlie_rows = wait_for_rows(
        &charlie,
        query.clone(),
        "charlie sees standalone and folder doc as direct owner",
        |rows| {
            // The inserts are queued independently; observing the first does
            // not establish that the second has reached this reader yet.
            (rows.iter().any(|(id, _)| *id == standalone_id)
                && rows.iter().any(|(id, _)| *id == folder_doc_id))
            .then_some(rows)
        },
    )
    .await;
    assert!(charlie_rows.iter().any(|(id, values)| {
        *id == standalone_id
            && *values == folder_document_values(super::CHARLIE_ID, "Standalone", false, None)
    }));
    assert!(charlie_rows.iter().any(|(id, values)| {
        *id == folder_doc_id
            && *values
                == folder_document_values(
                    super::CHARLIE_ID,
                    "Inside Folder",
                    false,
                    Some(folder_id),
                )
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
        folder_document_values(super::CHARLIE_ID, "Inside Folder", false, Some(folder_id))
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
        folder_document_values(super::CHARLIE_ID, "Inside Folder", false, Some(folder_id))
    );

    let dave_rows = wait_for_query(
        &dave,
        query,
        jazz::tools::ReadTier::Remote,
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
async fn inherited_folder_insert_requires_folder_owner_when_fk_present() {
    tokio::task::LocalSet::new()
        .run_until(inherited_folder_insert_requires_folder_owner_when_fk_present_inner())
        .await;
}

async fn inherited_folder_insert_requires_folder_owner_when_fk_present_inner() {
    let owner_policy = pe::eq("owner_id", pe::session(vec!["claims", "sub"]));
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
                // Child inserts inherit the parent's UPDATE USING permission.
                p.allow_update().where_old(folder_owner_policy());
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().where_(pe::all_of([
                    owner_policy.clone(),
                    pe::any_of([
                        pe::is_null("folder_id"),
                        inherited_non_null_policy(Operation::Select, "folder_id"),
                    ]),
                ]));
                p.allow_read().where_(pe::any_of([
                    owner_policy,
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ]));
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::ALICE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::ALICE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::BOB_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::BOB_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let charlie = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::CHARLIE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let charlie_reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::CHARLIE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id = create_folder(
        &alice,
        "folders",
        "Shared",
        &[super::ALICE_ID, super::BOB_ID],
        false,
    )
    .await;
    let folders_query = Query::from("folders");
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
        super::CHARLIE_ID,
        "Charlie Shared Attempt",
        false,
        Some(folder_id),
    )
    .await;
    let charlie_ok = create_folder_document(
        &charlie,
        "documents",
        super::CHARLIE_ID,
        "Charlie Standalone",
        false,
        None,
    )
    .await;

    let _alice_rejected = create_folder_document(
        &alice,
        "documents",
        super::BOB_ID,
        "Forged For Bob",
        false,
        Some(folder_id),
    )
    .await;
    let alice_standalone = create_folder_document(
        &alice,
        "documents",
        super::ALICE_ID,
        "Alice Standalone",
        false,
        None,
    )
    .await;
    let alice_shared = create_folder_document(
        &alice,
        "documents",
        super::ALICE_ID,
        "Alice Shared",
        false,
        Some(folder_id),
    )
    .await;
    let query = Query::from("documents");

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
        folder_document_values(super::CHARLIE_ID, "Charlie Standalone", false, None)
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
            && *values == folder_document_values(super::ALICE_ID, "Alice Standalone", false, None)
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == alice_shared
            && *values
                == folder_document_values(super::ALICE_ID, "Alice Shared", false, Some(folder_id))
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
        folder_document_values(super::ALICE_ID, "Alice Shared", false, Some(folder_id))
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
    tokio::task::LocalSet::new()
        .run_until(
            inherited_folder_delete_allows_folder_owner_to_delete_folder_and_documents_inner(),
        )
        .await;
}

async fn inherited_folder_delete_allows_folder_owner_to_delete_folder_and_documents_inner() {
    let owner_policy = pe::eq("owner_id", pe::session(vec!["claims", "sub"]));
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
                p.allow_delete().where_(folder_owner_policy());
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(pe::any_of([
                    owner_policy.clone(),
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ]));
                p.allow_delete().where_(pe::any_of([
                    owner_policy,
                    inherited_non_null_policy(Operation::Delete, "folder_id"),
                ]));
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .as_admin()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id(super::ALICE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id =
        create_folder(&admin, "folders", "Alice Folder", &[super::ALICE_ID], false).await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Shared Delete Target",
        false,
        Some(folder_id),
    )
    .await;

    let documents_query = Query::from("documents");
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
                                super::CHARLIE_ID,
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
        .delete("documents", doc_id)
        .expect("folder owner deletes folder-backed document");

    let rows_after_doc_delete = wait_for_query(
        &alice,
        documents_query,
        jazz::tools::ReadTier::Remote,
        Duration::from_secs(3),
        "folder-backed document is gone after folder-owner delete",
        Some,
    )
    .await;
    assert!(
        rows_after_doc_delete.is_empty(),
        "folder owner delete should remove the folder-backed document: {rows_after_doc_delete:?}"
    );

    let folders_query = Query::from("folders");
    wait_for_rows(
        &alice,
        folders_query.clone(),
        "alice sees owned folder before deleting it",
        |rows| rows.iter().any(|(id, _)| *id == folder_id).then_some(()),
    )
    .await;

    alice
        .delete("folders", folder_id)
        .expect("folder owner deletes folder");

    let rows_after_folder_delete = wait_for_query(
        &alice,
        folders_query,
        jazz::tools::ReadTier::Remote,
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
    tokio::task::LocalSet::new()
        .run_until(
            inherited_folder_delete_allows_document_owner_but_blocks_other_non_owners_inner(),
        )
        .await;
}

async fn inherited_folder_delete_allows_document_owner_but_blocks_other_non_owners_inner() {
    let owner_policy = pe::eq("owner_id", pe::session(vec!["claims", "sub"]));
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
                p.allow_delete().where_(folder_owner_policy());
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(pe::any_of([
                    owner_policy.clone(),
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ]));
                p.allow_delete().where_(pe::any_of([
                    owner_policy,
                    inherited_non_null_policy(Operation::Delete, "folder_id"),
                ]));
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .as_admin()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::ALICE_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id(super::BOB_ID)
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let folder_id = create_folder(
        &admin,
        "folders",
        "Shared Folder",
        &[super::ALICE_ID],
        false,
    )
    .await;
    let bob_doc_id = create_folder_document(
        &bob,
        "documents",
        super::BOB_ID,
        "Bob Folder Doc",
        false,
        Some(folder_id),
    )
    .await;
    let charlie_doc_id = create_folder_document(
        &bob,
        "documents",
        super::CHARLIE_ID,
        "Charlie Folder Doc",
        false,
        Some(folder_id),
    )
    .await;

    let documents_query = Query::from("documents");
    let initial_alice_rows = wait_for_rows(
        &alice,
        documents_query.clone(),
        "folder owner sees both folder-backed documents before deletes",
        |rows| rows.iter().any(|(id, _)| *id == bob_doc_id).then_some(rows),
    )
    .await;
    assert!(initial_alice_rows.iter().any(|(id, values)| {
        *id == bob_doc_id
            && *values
                == folder_document_values(super::BOB_ID, "Bob Folder Doc", false, Some(folder_id))
    }));
    assert!(initial_alice_rows.iter().any(|(id, values)| {
        *id == charlie_doc_id
            && *values
                == folder_document_values(
                    super::CHARLIE_ID,
                    "Charlie Folder Doc",
                    false,
                    Some(folder_id),
                )
    }));

    bob.delete("documents", bob_doc_id)
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
                                super::CHARLIE_ID,
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

    bob.delete("documents", charlie_doc_id)
        .expect("optimistic local delete for unauthorized attempt");

    let rows_after_unauthorized_delete = wait_for_query(
        &alice,
        documents_query,
        jazz::tools::ReadTier::Remote,
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
                        super::CHARLIE_ID,
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
async fn inherited_multiple_folder_paths_compose_with_or() {
    tokio::task::LocalSet::new()
        .run_until(inherited_multiple_folder_paths_compose_with_or_inner())
        .await;
}

async fn inherited_multiple_folder_paths_compose_with_or_inner() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "primary_folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
            }),
        ))
        .table(make_folders_schema(
            "secondary_folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
            }),
        ))
        .table(make_multi_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(pe::any_of([
                    inherited_non_null_policy(Operation::Select, "primary_folder_id"),
                    inherited_non_null_policy(Operation::Select, "secondary_folder_id"),
                ]));
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "documents", READY_TIMEOUT).await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "documents",
        READY_TIMEOUT,
    )
    .await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;
    let dave =
        connect_ready_user(&server, &schema, super::DAVE_ID, "documents", READY_TIMEOUT).await;

    let primary_folder_id = create_folder(
        &admin,
        "primary_folders",
        "Primary",
        &[super::ALICE_ID],
        false,
    )
    .await;
    let secondary_folder_id = create_folder(
        &admin,
        "secondary_folders",
        "Secondary",
        &[super::BOB_ID],
        false,
    )
    .await;

    let primary_doc_id = create_multi_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Primary Only",
        false,
        Some(primary_folder_id),
        None,
    )
    .await;
    let secondary_doc_id = create_multi_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Secondary Only",
        false,
        None,
        Some(secondary_folder_id),
    )
    .await;
    let both_doc_id = create_multi_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Both Paths",
        false,
        Some(primary_folder_id),
        Some(secondary_folder_id),
    )
    .await;
    let hidden_doc_id = create_multi_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Hidden",
        false,
        None,
        None,
    )
    .await;

    let query = Query::from("documents");

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees rows granted by the primary inherited path",
        |rows| {
            (rows.len() == 2
                && has_row(
                    &rows,
                    primary_doc_id,
                    &multi_folder_document_values(
                        super::CHARLIE_ID,
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
                        super::CHARLIE_ID,
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
                        super::CHARLIE_ID,
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
                        super::CHARLIE_ID,
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
        jazz::tools::ReadTier::Remote,
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
async fn inherited_folder_update_allows_folder_owner_and_blocks_other_users() {
    tokio::task::LocalSet::new()
        .run_until(inherited_folder_update_allows_folder_owner_and_blocks_other_users_inner())
        .await;
}

async fn inherited_folder_update_allows_folder_owner_and_blocks_other_users_inner() {
    let owner_policy = pe::eq("owner_id", pe::session(vec!["claims", "sub"]));
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
                p.allow_update()
                    .where_old(folder_owner_policy())
                    .where_new(pe::always());
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(pe::any_of([
                    owner_policy.clone(),
                    inherited_non_null_policy(Operation::Select, "folder_id"),
                ]));
                p.allow_update()
                    .where_old(pe::any_of([
                        owner_policy,
                        inherited_non_null_policy(Operation::Update, "folder_id"),
                    ]))
                    .where_new(pe::always());
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "documents", READY_TIMEOUT).await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "documents",
        READY_TIMEOUT,
    )
    .await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;

    let folder_id = create_folder(&admin, "folders", "Shared", &[super::ALICE_ID], false).await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Original",
        false,
        Some(folder_id),
    )
    .await;
    let query = Query::from("documents");

    wait_for_rows(
        &alice,
        query.clone(),
        "the folder owner sees the document before attempting an inherited update",
        |rows| {
            has_row(
                &rows,
                doc_id,
                &folder_document_values(super::CHARLIE_ID, "Original", false, Some(folder_id)),
            )
            .then_some(rows)
        },
    )
    .await;

    update_row(
        &alice,
        "documents",
        doc_id,
        vec![("title".to_string(), "Edited By Folder Owner".into())],
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
                    super::CHARLIE_ID,
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
        &folder_document_values(
            super::CHARLIE_ID,
            "Edited By Folder Owner",
            false,
            Some(folder_id)
        ),
    ));

    let error = bob
        .update(
            "documents",
            doc_id,
            vec![("title".to_string(), "Edited By Bob".into())],
        )
        .expect_err("Bob cannot update a row absent from his readable local data");
    assert!(error.to_string().contains("read policy denied"), "{error}");
    let rows_after_bob = wait_for_query(
        &admin,
        query,
        jazz::tools::ReadTier::Remote,
        Duration::from_secs(3),
        "non-owner without folder access cannot update the row",
        Some,
    )
    .await;
    assert!(has_row(
        &rows_after_bob,
        doc_id,
        &folder_document_values(
            super::CHARLIE_ID,
            "Edited By Folder Owner",
            false,
            Some(folder_id)
        ),
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
    tokio::task::LocalSet::new()
        .run_until(inherited_referencing_scalar_paths_grant_visibility_and_compose_with_or_inner())
        .await;
}

async fn inherited_referencing_scalar_paths_grant_visibility_and_compose_with_or_inner() {
    let schema = file_referencing_schema(false);
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "files", READY_TIMEOUT).await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "files", READY_TIMEOUT).await;
    let dave = connect_ready_user(&server, &schema, super::DAVE_ID, "files", READY_TIMEOUT).await;

    let file_single = create_file(&admin, super::MALLORY_ID, "Grant Single").await;
    let file_multi = create_file(&admin, super::MALLORY_ID, "Grant Multi").await;
    let file_hidden = create_file(&admin, super::MALLORY_ID, "Still Hidden").await;

    create_scalar_ref_todo(&alice, super::ALICE_ID, "Todo Single", Some(file_single)).await;
    create_scalar_ref_todo(&alice, super::ALICE_ID, "Todo Multi A", Some(file_multi)).await;
    create_scalar_ref_todo(&alice, super::ALICE_ID, "Todo Multi B", Some(file_multi)).await;

    let query = Query::from("files");
    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees files granted through referencing todos",
        |rows| {
            (rows.len() == 2
                && has_row(
                    &rows,
                    file_single,
                    &file_values(super::MALLORY_ID, "Grant Single"),
                )
                && has_row(
                    &rows,
                    file_multi,
                    &file_values(super::MALLORY_ID, "Grant Multi"),
                )
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
        jazz::tools::ReadTier::Remote,
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
    tokio::task::LocalSet::new().run_until(inherited_referencing_scalar_subscription_updates_follow_create_delete_and_retarget_inner()).await;
}

async fn inherited_referencing_scalar_subscription_updates_follow_create_delete_and_retarget_inner()
{
    let schema = file_referencing_schema(false);
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "files", READY_TIMEOUT).await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "files", READY_TIMEOUT).await;

    let file_a = create_file(&admin, super::MALLORY_ID, "File A").await;
    let file_b = create_file(&admin, super::MALLORY_ID, "File B").await;
    let query = Query::from("files");

    let mut stream = alice
        .subscribe(query.clone())
        .await
        .expect("subscribe files");
    let mut log = Vec::new();
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    let todo_id = create_scalar_ref_todo(&alice, super::ALICE_ID, "Todo A", Some(file_a)).await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "creating a referencing row makes the target visible",
        |entries| has_added_id(entries, file_a),
    )
    .await;
    let rows_after_create = wait_for_rows(
        &alice,
        query.clone(),
        "file A is visible after creating the referencing todo",
        |rows| has_row(&rows, file_a, &file_values(super::MALLORY_ID, "File A")).then_some(rows),
    )
    .await;
    assert!(has_row(
        &rows_after_create,
        file_a,
        &file_values(super::MALLORY_ID, "File A"),
    ));

    log.clear();
    alice
        .delete("todos", todo_id)
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
        jazz::tools::ReadTier::Remote,
        Duration::from_secs(3),
        "file A is hidden after deleting the referencing todo",
        Some,
    )
    .await;
    assert!(rows_after_delete.is_empty());

    log.clear();
    let todo_retarget_id =
        create_scalar_ref_todo(&alice, super::ALICE_ID, "Todo B", Some(file_a)).await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "recreating a reference makes file A visible again",
        |entries| has_added_id(entries, file_a),
    )
    .await;

    log.clear();
    update_row(
        &alice,
        "todos",
        todo_retarget_id,
        vec![("image".to_string(), Value::Uuid(file_b))],
    )
    .await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "retargeting a reference removes the old target and adds the new one",
        |entries| has_removed(entries, file_a) && has_added_id(entries, file_b),
    )
    .await;
    let rows_after_retarget = wait_for_rows(
        &alice,
        query,
        "only file B remains visible after retargeting the todo",
        |rows| {
            (rows.len() == 1
                && has_row(&rows, file_b, &file_values(super::MALLORY_ID, "File B"))
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
    tokio::task::LocalSet::new()
        .run_until(inherited_referencing_array_membership_preserves_set_semantics_inner())
        .await;
}

async fn inherited_referencing_array_membership_preserves_set_semantics_inner() {
    let schema = file_referencing_schema(true);
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "files", READY_TIMEOUT).await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "files", READY_TIMEOUT).await;

    let file_a = create_file(&admin, super::MALLORY_ID, "Array A").await;
    let file_b = create_file(&admin, super::MALLORY_ID, "Array B").await;
    let todo_id =
        create_array_ref_todo(&alice, super::ALICE_ID, "Array Todo", &[file_a, file_b]).await;
    let query = Query::from("files");

    let initial_rows = wait_for_rows(
        &alice,
        query.clone(),
        "array membership grants both referenced files",
        |rows| {
            (rows.len() == 2
                && has_row(&rows, file_a, &file_values(super::MALLORY_ID, "Array A"))
                && has_row(&rows, file_b, &file_values(super::MALLORY_ID, "Array B"))
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
    // A no-change assertion needs an installed initial view, not a timed
    // guess: otherwise a slow initial snapshot looks like a reorder delta.
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "array subscription initially contains both referenced files",
        |entries| {
            has_added_id(entries, file_a)
                && has_added_id(entries, file_b)
                && entries.last().is_some_and(|delta| !delta.pending)
        },
    )
    .await;
    log.clear();

    update_row(
        &alice,
        "todos",
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
                && has_row(&rows, file_a, &file_values(super::MALLORY_ID, "Array A"))
                && has_row(&rows, file_b, &file_values(super::MALLORY_ID, "Array B"))
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
        "todos",
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
                && has_row(&rows, file_a, &file_values(super::MALLORY_ID, "Array A"))
                && has_row(&rows, file_b, &file_values(super::MALLORY_ID, "Array B"))
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
async fn inherited_multi_hop_forward_chain_grants_access_to_leaf_rows() {
    tokio::task::LocalSet::new()
        .run_until(inherited_multi_hop_forward_chain_grants_access_to_leaf_rows_inner())
        .await;
}

async fn inherited_multi_hop_forward_chain_grants_access_to_leaf_rows_inner() {
    let schema = multi_hop_inherited_parts_schema();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "file_parts", READY_TIMEOUT).await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "file_parts",
        READY_TIMEOUT,
    )
    .await;
    let dave = connect_ready_user(
        &server,
        &schema,
        super::DAVE_ID,
        "file_parts",
        READY_TIMEOUT,
    )
    .await;

    let folder_id = create_folder(
        &admin,
        "folders",
        "Shared Folder",
        &[super::ALICE_ID],
        false,
    )
    .await;
    let file_id = admin
        .insert(
            "files",
            jazz::row_input!(
                "title" => "Spec.pdf",
                "folder_id" => Value::Uuid(folder_id)
            ),
        )
        .expect("create file")
        .0;
    let part_id = admin
        .insert(
            "file_parts",
            jazz::row_input!(
                "title" => "Page 1",
                "file_id" => Value::Uuid(file_id)
            ),
        )
        .expect("create file part")
        .0;

    let query = Query::from("file_parts");
    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees file parts through the folder -> file -> part chain",
        |rows| has_row(&rows, part_id, &["Page 1".into(), Value::Uuid(file_id)]).then_some(rows),
    )
    .await;
    assert!(has_row(
        &alice_rows,
        part_id,
        &["Page 1".into(), Value::Uuid(file_id)],
    ));

    let dave_rows = wait_for_query(
        &dave,
        query,
        jazz::tools::ReadTier::Remote,
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
async fn inherited_parent_policy_change_propagates_to_child_on_active_subscriptions() {
    tokio::task::LocalSet::new()
        .run_until(
            inherited_parent_policy_change_propagates_to_child_on_active_subscriptions_inner(),
        )
        .await;
}

async fn inherited_parent_policy_change_propagates_to_child_on_active_subscriptions_inner() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
                p.allow_update().always();
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read()
                    .where_(inherited_non_null_policy(Operation::Select, "folder_id"));
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "documents", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;

    let folder_id = create_folder(
        &admin,
        "folders",
        "Shared",
        &[super::ALICE_ID, super::BOB_ID],
        false,
    )
    .await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Shared Doc",
        false,
        Some(folder_id),
    )
    .await;
    let query = Query::from("documents");

    wait_for_rows(
        &bob,
        query.clone(),
        "bob initially sees the child row before the parent policy changes",
        |rows| {
            has_row(
                &rows,
                doc_id,
                &folder_document_values(super::CHARLIE_ID, "Shared Doc", false, Some(folder_id)),
            )
            .then_some(rows)
        },
    )
    .await;

    let mut stream = bob.subscribe(query.clone()).await.expect("subscribe bob");
    let mut log = Vec::new();
    // The stream itself must have observed the row before revocation can
    // legitimately be required to produce a removal for that stream.
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "bob's active subscription initially contains the shared document",
        |entries| {
            has_added_id(entries, doc_id) && entries.last().is_some_and(|delta| !delta.pending)
        },
    )
    .await;
    log.clear();

    update_row(
        &admin,
        "folders",
        folder_id,
        vec![(
            "owners".to_string(),
            Value::Array(vec![super::ALICE_ID.into()]),
        )],
    )
    .await;

    let bob_fresh =
        connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;
    let rows_after_update = wait_for_query(
        &bob_fresh,
        query,
        jazz::tools::ReadTier::Remote,
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
async fn inherited_child_fk_retarget_visible_to_hidden_parent_removes_child_from_subscriptions() {
    tokio::task::LocalSet::new().run_until(inherited_child_fk_retarget_visible_to_hidden_parent_removes_child_from_subscriptions_inner()).await;
}

async fn inherited_child_fk_retarget_visible_to_hidden_parent_removes_child_from_subscriptions_inner()
 {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
                p.allow_update().always();
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read()
                    .where_(inherited_non_null_policy(Operation::Select, "folder_id"));
                p.allow_update().always();
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "documents", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;

    let visible_folder_id =
        create_folder(&admin, "folders", "Visible", &[super::BOB_ID], false).await;
    let hidden_folder_id =
        create_folder(&admin, "folders", "Hidden", &[super::ALICE_ID], false).await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Retarget Me",
        false,
        Some(visible_folder_id),
    )
    .await;
    let query = Query::from("documents");

    wait_for_rows(
        &bob,
        query.clone(),
        "bob initially sees the child row before it is retargeted away",
        |rows| {
            has_row(
                &rows,
                doc_id,
                &folder_document_values(
                    super::CHARLIE_ID,
                    "Retarget Me",
                    false,
                    Some(visible_folder_id),
                ),
            )
            .then_some(rows)
        },
    )
    .await;

    let mut stream = bob.subscribe(query.clone()).await.expect("subscribe bob");
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "bob's active subscription initially contains the document before retargeting",
        |entries| {
            has_added_id(entries, doc_id) && entries.last().is_some_and(|delta| !delta.pending)
        },
    )
    .await;
    log.clear();

    update_row(
        &admin,
        "documents",
        doc_id,
        vec![("folder_id".to_string(), Value::Uuid(hidden_folder_id))],
    )
    .await;

    let bob_fresh =
        connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;
    let rows_after_retarget = wait_for_query(
        &bob_fresh,
        query,
        jazz::tools::ReadTier::Remote,
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
async fn inherited_child_fk_retarget_hidden_to_visible_parent_adds_child_to_subscriptions() {
    tokio::task::LocalSet::new()
        .run_until(
            inherited_child_fk_retarget_hidden_to_visible_parent_adds_child_to_subscriptions_inner(
            ),
        )
        .await;
}

async fn inherited_child_fk_retarget_hidden_to_visible_parent_adds_child_to_subscriptions_inner() {
    let schema = SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read().where_(folder_owner_policy());
                p.allow_update().always();
            }),
        ))
        .table(make_folder_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_read()
                    .where_(inherited_non_null_policy(Operation::Select, "folder_id"));
                p.allow_update().always();
            }),
        ))
        .build();

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "documents", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;

    let hidden_folder_id =
        create_folder(&admin, "folders", "Hidden", &[super::ALICE_ID], false).await;
    let visible_folder_id =
        create_folder(&admin, "folders", "Visible", &[super::BOB_ID], false).await;
    let doc_id = create_folder_document(
        &admin,
        "documents",
        super::CHARLIE_ID,
        "Reveal Me",
        false,
        Some(hidden_folder_id),
    )
    .await;
    let query = Query::from("documents");

    let initial_rows = wait_for_query(
        &bob,
        query.clone(),
        jazz::tools::ReadTier::Remote,
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
        "documents",
        doc_id,
        vec![("folder_id".to_string(), Value::Uuid(visible_folder_id))],
    )
    .await;

    let bob_fresh =
        connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;
    let rows_after_retarget = wait_for_rows(
        &bob_fresh,
        query,
        "bob sees the child row after retargeting into a visible parent",
        |rows| {
            has_row(
                &rows,
                doc_id,
                &folder_document_values(
                    super::CHARLIE_ID,
                    "Reveal Me",
                    false,
                    Some(visible_folder_id),
                ),
            )
            .then_some(rows)
        },
    )
    .await;
    assert!(has_row(
        &rows_after_retarget,
        doc_id,
        &folder_document_values(
            super::CHARLIE_ID,
            "Reveal Me",
            false,
            Some(visible_folder_id)
        ),
    ));
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "retargeting to a visible parent emits add",
        |entries| has_added_id(entries, doc_id),
    )
    .await;

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    bob_fresh.shutdown().await.expect("shutdown bob_fresh");
    server.shutdown().await;
}

/// Verifies that server startup rejects a child SELECT policy that inherits
/// from a parent table without an explicit read policy.
#[tokio::test]
async fn inherits_select_rejects_missing_parent_read_policy() {
    let documents_policies = permissions(|p| {
        p.allow_read().where_(pe::allowed_to_read("folder_id"));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text),
        )
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(documents_policies),
        )
        .build();
    let error = match JazzServer::start_with_schema(schema).await {
        Err(error) => error,
        Ok(server) => {
            server.shutdown().await;
            panic!("inheritance from a missing parent read policy must be rejected");
        }
    };
    assert!(
        error.contains("$.documents.policies.select.using")
            && error.contains(
                "INHERITS via_column 'folder_id' references table 'folders' without a Select policy"
            ),
        "expected a missing parent read policy error, got: {error}"
    );
}

/// Verifies that server startup rejects a child INSERT policy that inherits
/// from a parent table without an explicit INSERT policy.
#[tokio::test]
async fn inherits_insert_rejects_missing_parent_insert_policy() {
    let documents_policies = permissions(|p| {
        p.allow_insert().where_(pe::allowed_to_insert("folder_id"));
    });
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("folders").column("title", ColumnType::Text))
        .table(
            TableSchema::builder("documents")
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(documents_policies),
        )
        .build();

    let error = match JazzServer::start_with_schema(schema).await {
        Err(error) => error,
        Ok(server) => {
            server.shutdown().await;
            panic!("inheritance from a missing parent INSERT policy must be rejected");
        }
    };
    assert!(
        error.contains("$.documents.policies.insert.with_check")
            && error.contains(
                "INHERITS via_column 'folder_id' references table 'folders' without a Insert policy"
            ),
        "expected a missing parent INSERT policy error, got: {error}"
    );
}

/// Verifies that server startup rejects reverse inherited UPDATE access when
/// the referencing source table has no explicit UPDATE policy.
#[tokio::test]
async fn inherits_referencing_rejects_missing_source_update_policy() {
    let files_policies = permissions(|p| {
        p.allow_update()
            .where_old(pe::allowed_to_update_referencing("todos", "file_id"))
            .where_new(pe::always());
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("files")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(files_policies),
        )
        .table(
            TableSchema::builder("todos")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_fk_column("file_id", "files"),
        )
        .build();

    let error = match JazzServer::start_with_schema(schema).await {
        Err(error) => error,
        Ok(server) => {
            server.shutdown().await;
            panic!("reverse inheritance from a missing source UPDATE policy must be rejected");
        }
    };
    assert!(
        error.contains("$.files.policies.update.using")
            && error.contains("INHERITS_REFERENCING source_table 'todos' has no Update policy"),
        "expected a missing source UPDATE policy error, got: {error}"
    );
}

/// Verifies that inherited WITH CHECK constraints evaluate the proposed new
/// row state and deny updates when the referenced parent is not updateable.
///
/// Bob owns the child folder and keeps its `parent_id` pointed at Alice's root
/// folder, but Bob cannot update that root folder. The child's update must
/// therefore fail the inherited `allowedTo.update(parent_id)` check.
#[tokio::test]
async fn update_with_check_inherits_denies_when_parent_is_not_updateable() {
    tokio::task::LocalSet::new()
        .run_until(update_with_check_inherits_denies_when_parent_is_not_updateable_inner())
        .await;
}

async fn update_with_check_inherits_denies_when_parent_is_not_updateable_inner() {
    let folders_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
        p.allow_update()
            .where_old(pe::eq("owner_id", pe::session(vec!["claims", "sub"])))
            .where_new(pe::allowed_to_update_with_depth("parent_id", 10));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .nullable_fk_column("parent_id", "folders")
                .policies(folders_policies),
        )
        .build();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let admin =
        connect_ready_client(&server, &schema, "inherits-admin", "folders", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "folders", READY_TIMEOUT).await;

    let (root_id, _, root_tx) = admin
        .insert(
            "folders",
            crate::row_input!("owner_id" => super::ALICE_ID, "name" => "Root", "parent_id" => Value::Null),
        )
        .expect("create root");
    let (child_id, _, child_tx) = admin
        .insert(
            "folders",
            crate::row_input!("owner_id" => super::BOB_ID, "name" => "Child", "parent_id" => root_id),
        )
        .expect("create child");
    wait_for_edge_txs(
        &admin,
        &[
            root_tx.expect("root insert should commit immediately"),
            child_tx.expect("child insert should commit immediately"),
        ],
    )
    .await;

    // Give UPDATE a readable preimage so the rejection exercises WITH CHECK,
    // rather than failing because the child is absent from Bob's local data.
    wait_for_rows(
        &bob,
        Query::from("folders"),
        "Bob receives his own child before updating it",
        |rows| rows.iter().any(|(id, _)| *id == child_id).then_some(()),
    )
    .await;

    let update_tx = bob
        .update(
            "folders",
            child_id,
            vec![
                ("owner_id".into(), Value::Text(super::BOB_ID.into())),
                ("name".into(), Value::Text("Child renamed".into())),
                ("parent_id".into(), Value::Uuid(root_id)),
            ],
        )
        .expect("submit update to Bob's readable child")
        .expect("ordinary update has a transaction");
    let update_err = bob
        .wait_for_transaction(update_tx, DurabilityTier::GlobalServer)
        .await
        .expect_err("authority should reject inherited WITH CHECK");
    assert!(
        update_err.to_string().contains("authorization_denied"),
        "expected policy rejection, got {update_err}"
    );
    let rows = wait_for_query(
        &admin,
        Query::from("folders").select(["name", "parent_id"]),
        jazz::tools::ReadTier::Remote,
        QUERY_TIMEOUT,
        "child remains unchanged after rejected update",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == child_id
                        && *values == vec![Value::Text("Child".into()), Value::Uuid(root_id)]
                })
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 2);

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
