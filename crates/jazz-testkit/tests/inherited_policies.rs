use jazz_testkit as support;

use std::time::Duration;

use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, Operation, PolicyExpr, SchemaBuilder,
    Session, TablePolicies, TableSchema, Value,
};
use jazz_server::JazzServer;
use support::{
    publish_permissions, push_catalogue_in_memory, wait_for_edge_query_ready, wait_for_query,
};
use uuid::Uuid;

fn test_user_id(subject: &str) -> String {
    let uuid = Uuid::parse_str(subject)
        .unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_URL, subject.as_bytes()));
    uuid.to_string()
}

fn test_author_id(subject: &str) -> String {
    Session::new("urn:jazz:test", test_user_id(subject))
        .author_subject()
        .expect("test user identity")
        .canonical()
        .to_owned()
}

fn inherited_update_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("organizations")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_update(
                            Some(PolicyExpr::eq_session("owner_id", vec!["user".to_owned()])),
                            PolicyExpr::eq_session("owner_id", vec!["user".to_owned()]),
                        )
                        .with_delete(PolicyExpr::eq_session("owner_id", vec!["user".to_owned()])),
                ),
        )
        .table(
            TableSchema::builder("parents")
                .fk_column("organization_id", "organizations")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_update(
                            Some(PolicyExpr::or(vec![
                                PolicyExpr::eq_session("owner_id", vec!["user".to_owned()]),
                                PolicyExpr::inherits(Operation::Update, "organization_id"),
                            ])),
                            PolicyExpr::or(vec![
                                PolicyExpr::eq_session("owner_id", vec!["user".to_owned()]),
                                PolicyExpr::inherits(Operation::Update, "organization_id"),
                            ]),
                        )
                        .with_delete(PolicyExpr::eq_session("owner_id", vec!["user".to_owned()])),
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

fn inherited_select_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("organizations")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::eq_session("owner_id", vec!["user".to_owned()]))
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("folders")
                .fk_column("organization_id", "organizations")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::or(vec![
                            PolicyExpr::eq_session("owner_id", vec!["user".to_owned()]),
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

fn reverse_inherited_select_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("organizations")
                .column("owner_id", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::eq_session("owner_id", vec!["user".to_owned()]))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("teams")
                .fk_column("organization_id", "organizations")
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "organization_id"))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("attachments")
                .fk_column("file_id", "files")
                .fk_column("team_id", "teams")
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "team_id"))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("files")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::InheritsReferencing {
                            operation: Operation::Select,
                            source_table: "attachments".to_owned(),
                            via_column: "file_id".to_owned(),
                            max_depth: None,
                        })
                        .with_insert(PolicyExpr::True),
                ),
        )
        .build()
}

async fn publish_schema(server: &JazzServer, schema: &jazz::tools::Schema) {
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        std::slice::from_ref(schema),
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
    schema: jazz::tools::Schema,
    user_id: &str,
) -> jazz::tools::AppContext {
    let mut context = server.make_client_context_for_user(schema, user_id);
    context.backend_secret = None;
    context
}

async fn connect_ready_user(
    server: &JazzServer,
    schema: jazz::tools::Schema,
    user_id: &str,
    ready_table: &str,
) -> JazzClient {
    let client = jazz_testkit::connect(user_context(server, schema, user_id))
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

            let alice_session = alice.for_session(Session::new("urn:jazz:test", alice_user_id));
            let (folder_id, _, folder_tx) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => ObjectId::new(),
                        "owner_id" => alice_owner_id,
                        "name" => "Alice folder"
                    ),
                )
                .expect("alice inserts folder");
            let (document_id, _, document_tx) = alice_session
                .insert(
                    "documents",
                    row_input!(
                        "folder_id" => folder_id,
                        "alternate_folder_id" => Value::Null,
                        "title" => "visible through folder"
                    ),
                )
                .expect("alice inserts document");
            support::wait_for_edge_txs(
                &alice,
                &[
                    folder_tx.expect("ordinary mutation commits immediately"),
                    document_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            let alice_rows = wait_for_query(
                &alice,
                jazz::query::Query::from("documents"),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "alice sees forward-inherited document",
                |rows| (rows.len() == 1 && rows[0].0 == document_id).then_some(rows),
            )
            .await;
            assert_eq!(alice_rows[0].0, document_id);

            let bob_rows = wait_for_query(
                &bob,
                jazz::query::Query::from("documents"),
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

/// Exercises reverse inherited SELECT through a source policy that itself
/// inherits from a parent.
///
/// Alice owns an organization. A team inherits SELECT from that organization,
/// and an attachment inherits SELECT from the team before pointing at a file.
/// The file inherits SELECT in reverse through attachments, so Alice may read
/// it and Bob may not; the outer policy must retain the complete source
/// inheritance chain.
///
/// ```text
/// alice ──insert organization(owner=alice)──► team(org) ──► attachment(team, file) ──► file
/// alice ──query files──────────────────────────────────────────► sees file
/// bob   ──query files──────────────────────────────────────────► ✗ empty
/// ```
#[tokio::test(flavor = "current_thread")]
async fn reverse_inherited_select_retains_nested_source_inheritance() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = reverse_inherited_select_schema();
            publish_schema(&server, &schema).await;

            let alice_owner_id = test_author_id("alice");
            let alice_user_id = test_user_id("alice");
            let bob_user_id = test_user_id("bob");
            let alice = connect_ready_user(&server, schema.clone(), &alice_user_id, "files").await;
            let bob = connect_ready_user(&server, schema.clone(), &bob_user_id, "files").await;
            let alice_session = alice.for_session(Session::new("urn:jazz:test", alice_user_id));

            let (organization_id, _, organization_tx) = alice_session
                .insert("organizations", row_input!("owner_id" => alice_owner_id))
                .expect("alice inserts organization");
            let (team_id, _, team_tx) = alice_session
                .insert("teams", row_input!("organization_id" => organization_id))
                .expect("alice inserts team");
            let (file_id, _, file_tx) = alice_session
                .insert("files", row_input!("name" => "team file"))
                .expect("alice inserts file");
            let (_, _, attachment_tx) = alice_session
                .insert(
                    "attachments",
                    row_input!("file_id" => file_id, "team_id" => team_id),
                )
                .expect("alice attaches file to team");
            support::wait_for_edge_txs(
                &alice,
                &[
                    organization_tx.expect("ordinary mutation commits immediately"),
                    team_tx.expect("ordinary mutation commits immediately"),
                    file_tx.expect("ordinary mutation commits immediately"),
                    attachment_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            let query = jazz::query::Query::from("files");
            wait_for_query(
                &alice,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "alice sees file through the attachment's inherited organization policy",
                |rows| rows.iter().any(|(id, _)| *id == file_id).then_some(()),
            )
            .await;
            let bob_rows = bob
                .query(query, Some(DurabilityTier::EdgeServer))
                .await
                .expect("bob queries files");
            assert!(
                bob_rows.iter().all(|(id, _)| *id != file_id),
                "bob must not see a file whose attachment inherits from alice's organization: {bob_rows:?}"
            );

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

            let alice_session = alice.for_session(Session::new("urn:jazz:test", alice_user_id));
            let (organization_id, _, organization_tx) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id.clone(), "name" => "Alice org"),
                )
                .expect("alice inserts organization");
            let (folder_id, _, folder_tx) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => "unrelated",
                        "name" => "Inherited folder"
                    ),
                )
                .expect("alice inserts folder");
            let (document_id, _, document_tx) = alice_session
                .insert(
                    "documents",
                    row_input!(
                        "folder_id" => folder_id,
                        "alternate_folder_id" => Value::Null,
                        "title" => "visible through org"
                    ),
                )
                .expect("alice inserts document");
            support::wait_for_edge_txs(
                &alice,
                &[
                    organization_tx.expect("ordinary mutation commits immediately"),
                    folder_tx.expect("ordinary mutation commits immediately"),
                    document_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            wait_for_query(
                &alice,
                jazz::query::Query::from("documents"),
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

            let alice_session = alice.for_session(Session::new("urn:jazz:test", alice_user_id));
            let (bob_folder_id, _, bob_folder_tx) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => ObjectId::new(),
                        "owner_id" => bob_owner_id,
                        "name" => "Bob folder"
                    ),
                )
                .expect("insert bob-owned folder");
            let (alice_folder_id, _, alice_folder_tx) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => ObjectId::new(),
                        "owner_id" => alice_owner_id,
                        "name" => "Alice folder"
                    ),
                )
                .expect("insert alice-owned folder");
            let (document_id, _, document_tx) = alice_session
                .insert(
                    "shared_documents",
                    row_input!(
                        "folder_id" => bob_folder_id,
                        "alternate_folder_id" => alice_folder_id,
                        "title" => "visible through alternate folder"
                    ),
                )
                .expect("insert shared document");
            support::wait_for_edge_txs(
                &alice,
                &[
                    bob_folder_tx.expect("ordinary mutation commits immediately"),
                    alice_folder_tx.expect("ordinary mutation commits immediately"),
                    document_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            wait_for_query(
                &alice,
                jazz::query::Query::from("shared_documents"),
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

            let alice_session = alice.for_session(Session::new("urn:jazz:test", alice_user_id));
            let (organization_id, _, organization_tx) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id.clone(), "name" => "Alice org"),
                )
                .expect("insert alice-owned organization");
            let (primary_folder_id, _, primary_folder_tx) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => "unrelated-primary",
                        "name" => "Primary inherited folder"
                    ),
                )
                .expect("insert primary inherited folder");
            let (alternate_folder_id, _, alternate_folder_tx) = alice_session
                .insert(
                    "folders",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => "unrelated-alternate",
                        "name" => "Alternate inherited folder"
                    ),
                )
                .expect("insert alternate inherited folder");
            let (document_id, _, document_tx) = alice_session
                .insert(
                    "shared_documents",
                    row_input!(
                        "folder_id" => primary_folder_id,
                        "alternate_folder_id" => alternate_folder_id,
                        "title" => "visible through two branchy parents"
                    ),
                )
                .expect("insert shared document");
            support::wait_for_edge_txs(
                &alice,
                &[
                    organization_tx.expect("ordinary mutation commits immediately"),
                    primary_folder_tx.expect("ordinary mutation commits immediately"),
                    alternate_folder_tx.expect("ordinary mutation commits immediately"),
                    document_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            wait_for_query(
                &alice,
                jazz::query::Query::from("shared_documents"),
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
                std::slice::from_ref(&schema),
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

            let alice = jazz_testkit::connect(context).await.expect("connect alice");
            wait_for_edge_query_ready(&alice, "children", Duration::from_secs(30)).await;

            let alice_session = alice.for_session(Session::new("urn:jazz:test", alice_user_id));
            let (organization_id, _, organization_tx) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id.clone(), "name" => "Alice org"),
                )
                .expect("alice inserts organization");
            let (parent_id, _, parent_tx) = alice_session
                .insert(
                    "parents",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => alice_owner_id.clone(),
                        "name" => "Alice parent"
                    ),
                )
                .expect("alice inserts parent");
            let (child_id, _, child_tx) = alice_session
                .insert(
                    "children",
                    row_input!("parent_id" => parent_id, "title" => "draft"),
                )
                .expect("alice inserts child");
            let update_tx = alice_session
                .update(
                    child_id,
                    vec![("title".to_string(), Value::Text("published".to_string()))],
                )
                .expect("alice update should be admitted by inherited UPDATE policy");
            support::wait_for_edge_txs(
                &alice,
                &[
                    organization_tx.expect("ordinary mutation commits immediately"),
                    parent_tx.expect("ordinary mutation commits immediately"),
                    child_tx.expect("ordinary mutation commits immediately"),
                    update_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            let rows = alice
                .query(
                    jazz::query::Query::from("children"),
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
                std::slice::from_ref(&schema),
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

            let alice = jazz_testkit::connect(context).await.expect("connect alice");
            wait_for_edge_query_ready(&alice, "children", Duration::from_secs(30)).await;

            let alice_session = alice.for_session(Session::new("urn:jazz:test", alice_user_id));
            let (organization_id, _, organization_tx) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id.clone(), "name" => "Alice org"),
                )
                .expect("alice inserts organization");
            let (parent_id, _, parent_tx) = alice_session
                .insert(
                    "parents",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => "unrelated",
                        "name" => "Project"
                    ),
                )
                .expect("alice inserts parent");
            let (child_id, _, child_tx) = alice_session
                .insert(
                    "children",
                    row_input!("parent_id" => parent_id, "title" => "draft"),
                )
                .expect("alice inserts child");
            let update_tx = alice_session
                .update(
                    child_id,
                    vec![("title".to_string(), Value::Text("published".to_string()))],
                )
                .expect("alice update should be admitted by multi-hop inherited UPDATE policy");
            support::wait_for_edge_txs(
                &alice,
                &[
                    organization_tx.expect("ordinary mutation commits immediately"),
                    parent_tx.expect("ordinary mutation commits immediately"),
                    child_tx.expect("ordinary mutation commits immediately"),
                    update_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

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
                std::slice::from_ref(&schema),
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

            let alice = jazz_testkit::connect(context).await.expect("connect alice");
            wait_for_edge_query_ready(&alice, "children", Duration::from_secs(30)).await;

            let alice_session = alice.for_session(Session::new("urn:jazz:test", alice_user_id));
            let (organization_id, _, organization_tx) = alice_session
                .insert(
                    "organizations",
                    row_input!("owner_id" => alice_owner_id.clone(), "name" => "Alice org"),
                )
                .expect("alice inserts organization");
            let (parent_a, _, parent_a_tx) = alice_session
                .insert(
                    "parents",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => alice_owner_id.clone(),
                        "name" => "Parent A"
                    ),
                )
                .expect("alice inserts parent A");
            let (parent_b, _, parent_b_tx) = alice_session
                .insert(
                    "parents",
                    row_input!(
                        "organization_id" => organization_id,
                        "owner_id" => alice_owner_id,
                        "name" => "Parent B"
                    ),
                )
                .expect("alice inserts parent B");
            let (child_id, _, child_tx) = alice_session
                .insert(
                    "children",
                    row_input!("parent_id" => parent_a, "title" => "draft"),
                )
                .expect("alice inserts child");
            let update_tx = alice_session
                .update(
                    child_id,
                    vec![
                        ("parent_id".to_string(), Value::Uuid(parent_b)),
                        ("title".to_string(), Value::Text("moved".to_string())),
                    ],
                )
                .expect("alice reparent update should be admitted by inherited UPDATE policy");
            support::wait_for_edge_txs(
                &alice,
                &[
                    organization_tx.expect("ordinary mutation commits immediately"),
                    parent_a_tx.expect("ordinary mutation commits immediately"),
                    parent_b_tx.expect("ordinary mutation commits immediately"),
                    child_tx.expect("ordinary mutation commits immediately"),
                    update_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            alice.shutdown().await.expect("shutdown alice");
            server.shutdown().await;
        })
        .await;
}
