#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, Operation, PolicyExpr, QueryBuilder,
    SchemaBuilder, Session, TablePolicies, TableSchema, Value, row_input,
};
use support::{publish_permissions, push_catalogue_in_memory, wait_for_edge_query_ready};
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
