#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, Operation, PolicyExpr, QueryBuilder,
    SchemaBuilder, Session, TablePolicies, TableSchema, Value, row_input,
};
use support::{
    publish_permissions, push_catalogue_in_memory, wait_for_edge_query_ready, wait_for_query,
};
use uuid::Uuid;

fn test_author_id(subject: &str) -> ObjectId {
    let uuid = Uuid::parse_str(subject)
        .unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_URL, subject.as_bytes()));
    ObjectId::from_uuid(uuid)
}

fn test_user_id(subject: &str) -> String {
    test_author_id(subject).uuid().to_string()
}

fn inherited_update_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("organizations")
                .column("owner_id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_update(
                            Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
                            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                        )
                        .with_delete(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
                ),
        )
        .table(
            TableSchema::builder("parents")
                .fk_column("organization_id", "organizations")
                .column("owner_id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_update(
                            Some(PolicyExpr::or(vec![
                                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                                PolicyExpr::inherits(Operation::Update, "organization_id"),
                            ])),
                            PolicyExpr::or(vec![
                                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                                PolicyExpr::inherits(Operation::Update, "organization_id"),
                            ]),
                        )
                        .with_delete(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
                ),
        )
        .table(
            TableSchema::builder("children")
                .fk_column("parent_id", "parents")
                .column("title", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_update(
                            Some(PolicyExpr::inherits(Operation::Update, "parent_id")),
                            PolicyExpr::inherits(Operation::Update, "parent_id"),
                        )
                        .with_delete(PolicyExpr::inherits(Operation::Update, "parent_id")),
                ),
        )
        .build()
}

fn inherited_select_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("organizations")
                .column("owner_id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("folders")
                .fk_column("organization_id", "organizations")
                .column("owner_id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::or(vec![
                            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                            PolicyExpr::inherits(Operation::Select, "organization_id"),
                        ]))
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("documents")
                .fk_column("folder_id", "folders")
                .nullable_fk_column("alternate_folder_id", "folders")
                .column("title", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "folder_id"))
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("shared_documents")
                .fk_column("folder_id", "folders")
                .fk_column("alternate_folder_id", "folders")
                .column("title", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::or(vec![
                            PolicyExpr::inherits(Operation::Select, "folder_id"),
                            PolicyExpr::inherits(Operation::Select, "alternate_folder_id"),
                        ]))
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .build()
}

async fn publish_schema(server: &JazzServer, schema: &jazz_tools::Schema) {
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[schema.clone()],
        &[],
    )
    .await
    .expect("push inherited policy catalogue");

    publish_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        schema,
        schema
            .iter()
            .map(|(table_name, table_schema)| (*table_name, table_schema.policies.clone()))
            .collect::<Vec<_>>(),
        None,
    )
    .await;
}

fn user_context(
    server: &JazzServer,
    schema: jazz_tools::Schema,
    user_id: &str,
) -> jazz_tools::AppContext {
    let mut context = server.make_client_context_for_user(schema, user_id);
    context.backend_secret = None;
    context
}

async fn connect_ready_user(
    server: &JazzServer,
    schema: jazz_tools::Schema,
    user_id: &str,
    ready_table: &str,
) -> JazzClient {
    let client = JazzClient::connect(user_context(server, schema, user_id))
        .await
        .expect("connect user");
    wait_for_edge_query_ready(&client, ready_table, Duration::from_secs(30)).await;
    client
}

/// Exercises forward inherited SELECT from child rows to a parent row.
///
/// Alice owns a folder. A document points at that folder and grants SELECT with
/// `INHERITS SELECT VIA folder_id`. Alice should see the document through the
/// parent-granted read path; Bob should not.
///
/// ```text
/// alice ──insert folder(owner=alice)──► server
/// alice ──insert document(folder_id)──► server
/// alice ──query documents────────────► INHERITS SELECT via folder ──► sees row
/// bob   ──query documents────────────► INHERITS SELECT via folder ──✗ empty
/// ```
#[tokio::test(flavor = "current_thread")]
async fn inherited_select_policy_exposes_child_row_through_parent() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = inherited_select_schema();
            publish_schema(&server, &schema).await;

            let alice_owner_id = test_author_id("alice");
            let alice_user_id = test_user_id("alice");
            let bob_user_id = test_user_id("bob");
            let alice =
                connect_ready_user(&server, schema.clone(), &alice_user_id, "documents").await;
            let bob = connect_ready_user(&server, schema.clone(), &bob_user_id, "documents").await;

            let alice_session = alice.for_session(Session::new(alice_user_id));
            let (folder_id, _, folder_batch) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => ObjectId::new(),
                        "owner_id" => alice_owner_id,
                        "name" => "Alice folder"
                    ),
                )
                .expect("alice inserts folder");
            alice
                .wait_for_batch(folder_batch, DurabilityTier::EdgeServer)
                .await
                .expect("folder reaches edge");

            let (document_id, _, document_batch) = alice_session
                .insert(
                    "documents",
                    row_input!(
                        "folder_id" => folder_id,
                        "alternate_folder_id" => Value::Null,
                        "title" => "visible through folder"
                    ),
                )
                .expect("alice inserts document");
            alice
                .wait_for_batch(document_batch, DurabilityTier::EdgeServer)
                .await
                .expect("document reaches edge");

            let alice_rows = wait_for_query(
                &alice,
                QueryBuilder::new("documents").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "alice sees forward-inherited document",
                |rows| (rows.len() == 1 && rows[0].0 == document_id).then_some(rows),
            )
            .await;
            assert_eq!(alice_rows[0].0, document_id);

            let bob_rows = wait_for_query(
                &bob,
                QueryBuilder::new("documents").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(3),
                "bob does not see alice's forward-inherited document",
                Some,
            )
            .await;
            assert!(bob_rows.is_empty());

            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            server.shutdown().await;
        })
        .await;
}

/// Exercises multi-hop forward inherited SELECT.
///
/// Alice owns an organization. A folder inherits SELECT from that organization,
/// and a document inherits SELECT from the folder. Alice should see the document
/// even though the folder's direct owner is not Alice.
///
/// ```text
/// alice ──insert org(owner=alice)────────► server
/// alice ──insert folder(org_id)──────────► server
/// alice ──insert document(folder_id)─────► server
/// alice ──query documents────────────────► doc → folder → org ──► sees row
/// ```
#[tokio::test(flavor = "current_thread")]
async fn inherited_select_policy_exposes_child_row_through_multi_hop_parent_chain() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = inherited_select_schema();
            publish_schema(&server, &schema).await;

            let alice_owner_id = test_author_id("alice");
            let alice_user_id = test_user_id("alice");
            let alice =
                connect_ready_user(&server, schema.clone(), &alice_user_id, "documents").await;

            let alice_session = alice.for_session(Session::new(alice_user_id));
            let (organization_id, _, organization_batch) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id, "name" => "Alice org"),
                )
                .expect("alice inserts organization");
            alice
                .wait_for_batch(organization_batch, DurabilityTier::EdgeServer)
                .await
                .expect("organization reaches edge");

            let (folder_id, _, folder_batch) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => ObjectId::new(),
                        "name" => "Inherited folder"
                    ),
                )
                .expect("alice inserts folder");
            alice
                .wait_for_batch(folder_batch, DurabilityTier::EdgeServer)
                .await
                .expect("folder reaches edge");

            let (document_id, _, document_batch) = alice_session
                .insert(
                    "documents",
                    row_input!(
                        "folder_id" => folder_id,
                        "alternate_folder_id" => Value::Null,
                        "title" => "visible through org"
                    ),
                )
                .expect("alice inserts document");
            alice
                .wait_for_batch(document_batch, DurabilityTier::EdgeServer)
                .await
                .expect("document reaches edge");

            wait_for_query(
                &alice,
                QueryBuilder::new("documents").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "alice sees multi-hop forward-inherited document",
                |rows| (rows.len() == 1 && rows[0].0 == document_id).then_some(rows),
            )
            .await;

            alice.shutdown().await.expect("shutdown alice");
            server.shutdown().await;
        })
        .await;
}

/// Exercises OR composition of multiple forward inherited SELECT parents.
///
/// Alice owns only the alternate folder. A shared document grants SELECT through
/// either `folder_id` or `alternate_folder_id`; Alice should see the document
/// through the second inherited path.
///
/// ```text
/// alice ──insert folder B(owner=alice)────────► server
/// bob   ──insert folder A(owner=bob)──────────► server
/// alice ──insert shared_document(A, B)────────► server
/// alice ──query shared_documents──────────────► OR(INHERITS A, INHERITS B) ──► sees row
/// ```
#[tokio::test(flavor = "current_thread")]
async fn inherited_select_policy_exposes_child_row_through_any_forward_parent() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = inherited_select_schema();
            publish_schema(&server, &schema).await;

            let alice_owner_id = test_author_id("alice");
            let alice_user_id = test_user_id("alice");
            let bob_owner_id = test_author_id("bob");
            let alice =
                connect_ready_user(&server, schema.clone(), &alice_user_id, "shared_documents")
                    .await;

            let alice_session = alice.for_session(Session::new(alice_user_id));
            let (bob_folder_id, _, bob_folder_batch) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => ObjectId::new(),
                        "owner_id" => bob_owner_id,
                        "name" => "Bob folder"
                    ),
                )
                .expect("insert bob-owned folder");
            alice
                .wait_for_batch(bob_folder_batch, DurabilityTier::EdgeServer)
                .await
                .expect("bob folder reaches edge");

            let (alice_folder_id, _, alice_folder_batch) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => ObjectId::new(),
                        "owner_id" => alice_owner_id,
                        "name" => "Alice folder"
                    ),
                )
                .expect("insert alice-owned folder");
            alice
                .wait_for_batch(alice_folder_batch, DurabilityTier::EdgeServer)
                .await
                .expect("alice folder reaches edge");

            let (document_id, _, document_batch) = alice_session
                .insert(
                    "shared_documents",
                    row_input!(
                        "folder_id" => bob_folder_id,
                        "alternate_folder_id" => alice_folder_id,
                        "title" => "visible through alternate folder"
                    ),
                )
                .expect("insert shared document");
            alice
                .wait_for_batch(document_batch, DurabilityTier::EdgeServer)
                .await
                .expect("shared document reaches edge");

            wait_for_query(
                &alice,
                QueryBuilder::new("shared_documents").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "alice sees document through one of two inherited parents",
                |rows| (rows.len() == 1 && rows[0].0 == document_id).then_some(rows),
            )
            .await;

            alice.shutdown().await.expect("shutdown alice");
            server.shutdown().await;
        })
        .await;
}

/// Exercises OR composition when both forward inherited SELECT parents expand
/// into branchy parent policies.
///
/// Both folders are visible only through their organization parent, not through
/// direct folder ownership. The shared-document policy must flatten both sides'
/// inherited alternatives in one pass.
#[tokio::test(flavor = "current_thread")]
async fn inherited_select_policy_expands_both_forward_parent_branches() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = inherited_select_schema();
            publish_schema(&server, &schema).await;

            let alice_owner_id = test_author_id("alice");
            let alice_user_id = test_user_id("alice");
            let alice =
                connect_ready_user(&server, schema.clone(), &alice_user_id, "shared_documents")
                    .await;

            let alice_session = alice.for_session(Session::new(alice_user_id));
            let (organization_id, _, organization_batch) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id, "name" => "Alice org"),
                )
                .expect("insert alice-owned organization");
            alice
                .wait_for_batch(organization_batch, DurabilityTier::EdgeServer)
                .await
                .expect("organization reaches edge");

            let (primary_folder_id, _, primary_folder_batch) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => ObjectId::new(),
                        "name" => "Primary inherited folder"
                    ),
                )
                .expect("insert primary inherited folder");
            alice
                .wait_for_batch(primary_folder_batch, DurabilityTier::EdgeServer)
                .await
                .expect("primary folder reaches edge");

            let (alternate_folder_id, _, alternate_folder_batch) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => ObjectId::new(),
                        "name" => "Alternate inherited folder"
                    ),
                )
                .expect("insert alternate inherited folder");
            alice
                .wait_for_batch(alternate_folder_batch, DurabilityTier::EdgeServer)
                .await
                .expect("alternate folder reaches edge");

            let (document_id, _, document_batch) = alice_session
                .insert(
                    "shared_documents",
                    row_input!(
                        "folder_id" => primary_folder_id,
                        "alternate_folder_id" => alternate_folder_id,
                        "title" => "visible through two branchy parents"
                    ),
                )
                .expect("insert shared document");
            alice
                .wait_for_batch(document_batch, DurabilityTier::EdgeServer)
                .await
                .expect("shared document reaches edge");

            wait_for_query(
                &alice,
                QueryBuilder::new("shared_documents").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "alice sees document when both inherited parents expand to branches",
                |rows| (rows.len() == 1 && rows[0].0 == document_id).then_some(rows),
            )
            .await;

            alice.shutdown().await.expect("shutdown alice");
            server.shutdown().await;
        })
        .await;
}

/// Exercises UPDATE authorization inherited through a parent row.
///
/// Alice owns a parent row. A child row points at that parent and grants UPDATE
/// with `INHERITS UPDATE VIA parent_id`. Alice should be able to update the
/// child because the parent row's UPDATE policy authorizes her.
///
/// ```text
/// alice ──insert parent(owner=alice)──► server
/// alice ──insert child(parent_id)─────► server
/// alice ──update child title─────────► server ──INHERITS UPDATE via parent──► allow
/// ```
#[tokio::test(flavor = "current_thread")]
async fn inherited_update_policy_allows_update_through_parent() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = inherited_update_schema();

            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                "main",
                &[schema.clone()],
                &[],
            )
            .await
            .expect("push inherited update catalogue");

            publish_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &schema,
                schema
                    .iter()
                    .map(|(table_name, table_schema)| (*table_name, table_schema.policies.clone()))
                    .collect::<Vec<_>>(),
                None,
            )
            .await;

            let alice_owner_id = test_author_id("alice");
            let alice_user_id = test_user_id("alice");
            let mut context = server.make_client_context_for_user(schema.clone(), &alice_user_id);
            context.backend_secret = None;

            let alice = JazzClient::connect(context).await.expect("connect alice");
            wait_for_edge_query_ready(&alice, "children", Duration::from_secs(30)).await;

            let alice_session = alice.for_session(Session::new(alice_user_id));
            let (organization_id, _, organization_batch) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id, "name" => "Alice org"),
                )
                .expect("alice inserts organization");
            alice
                .wait_for_batch(organization_batch, DurabilityTier::EdgeServer)
                .await
                .expect("organization reaches edge");

            let (parent_id, _, parent_batch) = alice_session
                .insert(
                    "parents",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => alice_owner_id,
                        "name" => "Alice parent"
                    ),
                )
                .expect("alice inserts parent");
            alice
                .wait_for_batch(parent_batch, DurabilityTier::EdgeServer)
                .await
                .expect("parent reaches edge");

            let (child_id, _, child_batch) = alice_session
                .insert(
                    "children",
                    row_input!("parent_id" => parent_id, "title" => "draft"),
                )
                .expect("alice inserts child");
            alice
                .wait_for_batch(child_batch, DurabilityTier::EdgeServer)
                .await
                .expect("child reaches edge");

            let update_batch = alice_session
                .update(
                    child_id,
                    vec![("title".to_string(), Value::Text("published".to_string()))],
                )
                .expect("alice update should be admitted by inherited UPDATE policy");
            alice
                .wait_for_batch(update_batch, DurabilityTier::EdgeServer)
                .await
                .expect("inherited child update reaches edge");

            let rows = alice
                .query(
                    QueryBuilder::new("children").build(),
                    Some(DurabilityTier::EdgeServer),
                )
                .await
                .expect("query children");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, child_id);
            assert_eq!(
                rows[0].1,
                vec![Value::Uuid(parent_id), Value::Text("published".to_string())]
            );

            alice.shutdown().await.expect("shutdown alice");
            server.shutdown().await;
        })
        .await;
}

/// Exercises multi-hop UPDATE authorization inherited through two parent rows.
///
/// Alice owns an organization. A parent row inherits UPDATE from that
/// organization, and a child row inherits UPDATE from the parent. Alice should be
/// able to update the child because the full inherited chain reaches the
/// organization row she owns.
///
/// ```text
/// alice ──insert org(owner=alice)──────► server
/// alice ──insert parent(org_id)────────► server
/// alice ──insert child(parent_id)──────► server
/// alice ──update child title───────────► child INHERITS parent INHERITS org ──► allow
/// ```
#[tokio::test(flavor = "current_thread")]
async fn inherited_update_policy_allows_multi_hop_update_chain() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = inherited_update_schema();

            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                "main",
                &[schema.clone()],
                &[],
            )
            .await
            .expect("push inherited update catalogue");

            publish_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &schema,
                schema
                    .iter()
                    .map(|(table_name, table_schema)| (*table_name, table_schema.policies.clone()))
                    .collect::<Vec<_>>(),
                None,
            )
            .await;

            let alice_owner_id = test_author_id("alice");
            let alice_user_id = test_user_id("alice");
            let mut context = server.make_client_context_for_user(schema.clone(), &alice_user_id);
            context.backend_secret = None;

            let alice = JazzClient::connect(context).await.expect("connect alice");
            wait_for_edge_query_ready(&alice, "children", Duration::from_secs(30)).await;

            let alice_session = alice.for_session(Session::new(alice_user_id));
            let (organization_id, _, organization_batch) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id, "name" => "Alice org"),
                )
                .expect("alice inserts organization");
            alice
                .wait_for_batch(organization_batch, DurabilityTier::EdgeServer)
                .await
                .expect("organization reaches edge");

            let (parent_id, _, parent_batch) = alice_session
                .insert(
                    "parents",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => ObjectId::new(),
                        "name" => "Project"
                    ),
                )
                .expect("alice inserts parent");
            alice
                .wait_for_batch(parent_batch, DurabilityTier::EdgeServer)
                .await
                .expect("parent reaches edge");

            let (child_id, _, child_batch) = alice_session
                .insert(
                    "children",
                    row_input!("parent_id" => parent_id, "title" => "draft"),
                )
                .expect("alice inserts child");
            alice
                .wait_for_batch(child_batch, DurabilityTier::EdgeServer)
                .await
                .expect("child reaches edge");

            let update_batch = alice_session
                .update(
                    child_id,
                    vec![("title".to_string(), Value::Text("published".to_string()))],
                )
                .expect("alice update should be admitted by multi-hop inherited UPDATE policy");
            alice
                .wait_for_batch(update_batch, DurabilityTier::EdgeServer)
                .await
                .expect("multi-hop inherited child update reaches edge");

            alice.shutdown().await.expect("shutdown alice");
            server.shutdown().await;
        })
        .await;
}

/// Exercises inherited UPDATE when the update changes the inheriting FK.
///
/// Alice owns two parent rows. A child starts under the first parent and grants
/// UPDATE via `parent_id`. Moving it to the second parent should pass because
/// `UPDATE USING` authorizes the old row and `UPDATE CHECK` authorizes the new
/// row through the same inherited policy.
///
/// ```text
/// alice ──insert parent A/B(owner=alice)──► server
/// alice ──insert child(parent=A)──────────► server
/// alice ──update child(parent=B)──────────► old INHERITS A + new INHERITS B ──► allow
/// ```
#[tokio::test(flavor = "current_thread")]
async fn inherited_update_policy_allows_reparenting_when_old_and_new_parents_grant() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = inherited_update_schema();

            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                "main",
                &[schema.clone()],
                &[],
            )
            .await
            .expect("push inherited update catalogue");

            publish_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &schema,
                schema
                    .iter()
                    .map(|(table_name, table_schema)| (*table_name, table_schema.policies.clone()))
                    .collect::<Vec<_>>(),
                None,
            )
            .await;

            let alice_owner_id = test_author_id("alice");
            let alice_user_id = test_user_id("alice");
            let mut context = server.make_client_context_for_user(schema.clone(), &alice_user_id);
            context.backend_secret = None;

            let alice = JazzClient::connect(context).await.expect("connect alice");
            wait_for_edge_query_ready(&alice, "children", Duration::from_secs(30)).await;

            let alice_session = alice.for_session(Session::new(alice_user_id));
            let (organization_id, _, organization_batch) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id, "name" => "Alice org"),
                )
                .expect("alice inserts organization");
            alice
                .wait_for_batch(organization_batch, DurabilityTier::EdgeServer)
                .await
                .expect("organization reaches edge");

            let (parent_a, _, parent_a_batch) = alice_session
                .insert(
                    "parents",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => alice_owner_id,
                        "name" => "Parent A"
                    ),
                )
                .expect("alice inserts parent A");
            alice
                .wait_for_batch(parent_a_batch, DurabilityTier::EdgeServer)
                .await
                .expect("parent A reaches edge");

            let (parent_b, _, parent_b_batch) = alice_session
                .insert(
                    "parents",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => alice_owner_id,
                        "name" => "Parent B"
                    ),
                )
                .expect("alice inserts parent B");
            alice
                .wait_for_batch(parent_b_batch, DurabilityTier::EdgeServer)
                .await
                .expect("parent B reaches edge");

            let (child_id, _, child_batch) = alice_session
                .insert(
                    "children",
                    row_input!("parent_id" => parent_a, "title" => "draft"),
                )
                .expect("alice inserts child");
            alice
                .wait_for_batch(child_batch, DurabilityTier::EdgeServer)
                .await
                .expect("child reaches edge");

            let update_batch = alice_session
                .update(
                    child_id,
                    vec![
                        ("parent_id".to_string(), Value::Uuid(parent_b)),
                        ("title".to_string(), Value::Text("moved".to_string())),
                    ],
                )
                .expect("alice reparent update should be admitted by inherited UPDATE policy");
            alice
                .wait_for_batch(update_batch, DurabilityTier::EdgeServer)
                .await
                .expect("inherited reparent update reaches edge");

            alice.shutdown().await.expect("shutdown alice");
            server.shutdown().await;
        })
        .await;
}
