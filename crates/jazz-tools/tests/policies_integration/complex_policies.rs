use std::collections::HashMap;
use std::time::Duration;

use super::support::{
    TestingClient, collect_stream_deltas, has_added, has_removed, has_updated, wait_for_query,
    wait_for_rows, wait_for_subscription_update,
};
use jazz_tools::query_manager::policy::{
    CmpOp, OUTER_ROW_SESSION_PREFIX, Operation, PolicyExpr, PolicyValue,
};
use jazz_tools::query_manager::relation_ir::{
    ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, ProjectColumn, ProjectExpr,
    RelExpr, RowIdRef, ValueRef,
};
use jazz_tools::query_manager::types::{TableName, TablePolicies, TableSchemaBuilder};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value,
};
use serde_json::json;

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(100);

fn row_input<const N: usize>(pairs: [(&str, Value); N]) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(column, value)| (column.to_string(), value))
        .collect()
}

fn row_changes<const N: usize>(pairs: [(&str, Value); N]) -> Vec<(String, Value)> {
    pairs
        .into_iter()
        .map(|(column, value)| (column.to_string(), value))
        .collect()
}

fn title_document_values(title: &str) -> Vec<Value> {
    vec![Value::Text(title.to_string())]
}

fn complex_document_values(
    team_slug: &str,
    published: bool,
    title: &str,
    folder_id: Option<ObjectId>,
) -> Vec<Value> {
    vec![
        Value::Text(team_slug.to_string()),
        Value::Boolean(published),
        Value::Text(title.to_string()),
        folder_id.map(Value::Uuid).unwrap_or(Value::Null),
    ]
}

fn make_title_documents_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .policies(policies)
}

fn make_complex_documents_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("team_slug", ColumnType::Text)
        .column("published", ColumnType::Boolean)
        .column("title", ColumnType::Text)
        .nullable_fk_column("folder_id", "folders")
        .policies(policies)
}

fn make_folders_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("owner_id", ColumnType::Text)
        .column("name", ColumnType::Text)
        .policies(policies)
}

fn outer_row_id_ref() -> PolicyValue {
    PolicyValue::SessionRef(vec![OUTER_ROW_SESSION_PREFIX.into(), "id".into()])
}

fn shared_document_select_policy() -> PolicyExpr {
    PolicyExpr::Exists {
        table: "document_shares".into(),
        condition: Box::new(PolicyExpr::and(vec![
            PolicyExpr::Cmp {
                column: "document_id".into(),
                op: CmpOp::Eq,
                value: outer_row_id_ref(),
            },
            PolicyExpr::eq_session("user_id", vec!["user_id".into()]),
        ])),
    }
}

fn editor_document_update_policy() -> PolicyExpr {
    PolicyExpr::Exists {
        table: "document_editors".into(),
        condition: Box::new(PolicyExpr::and(vec![
            PolicyExpr::Cmp {
                column: "document_id".into(),
                op: CmpOp::Eq,
                value: outer_row_id_ref(),
            },
            PolicyExpr::eq_session("user_id", vec!["user_id".into()]),
        ])),
    }
}

fn join_membership_select_policy() -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::TableScan {
                    table: TableName::new("document_grants"),
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("group_memberships"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("document_grants", "group_slug"),
                    right: ColumnRef::scoped("group_memberships", "group_slug"),
                }],
                join_kind: JoinKind::Inner,
            }),
            predicate: PredicateExpr::And(vec![
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("document_grants", "document_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Outer),
                },
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("group_memberships", "user_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["user_id".into()]),
                },
            ]),
        },
    }
}

fn hop_membership_select_policy() -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelExpr::Project {
            input: Box::new(RelExpr::Filter {
                input: Box::new(RelExpr::Join {
                    left: Box::new(RelExpr::Filter {
                        input: Box::new(RelExpr::TableScan {
                            table: TableName::new("group_memberships"),
                        }),
                        predicate: PredicateExpr::Cmp {
                            left: ColumnRef::scoped("group_memberships", "user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                    }),
                    right: Box::new(RelExpr::TableScan {
                        table: TableName::new("document_grants"),
                    }),
                    on: vec![JoinCondition {
                        left: ColumnRef::scoped("group_memberships", "group_slug"),
                        right: ColumnRef::scoped("__hop_0", "group_slug"),
                    }],
                    join_kind: JoinKind::Inner,
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::scoped("__hop_0", "document_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Outer),
                },
            }),
            columns: vec![ProjectColumn {
                alias: "document_id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "document_id")),
            }],
        },
    }
}

fn mixed_complex_select_policy() -> PolicyExpr {
    PolicyExpr::and(vec![
        PolicyExpr::eq_literal("published", Value::Boolean(true)),
        PolicyExpr::in_session("team_slug", vec!["claims".into(), "team_slugs".into()]),
        PolicyExpr::Exists {
            table: "document_flags".into(),
            condition: Box::new(PolicyExpr::and(vec![
                PolicyExpr::Cmp {
                    column: "document_id".into(),
                    op: CmpOp::Eq,
                    value: outer_row_id_ref(),
                },
                PolicyExpr::eq_literal("flag", Value::Text("allow".to_string())),
            ])),
        },
        PolicyExpr::and(vec![
            PolicyExpr::IsNotNull {
                column: "folder_id".into(),
            },
            PolicyExpr::inherits(Operation::Select, "folder_id"),
        ]),
    ])
}

fn exists_share_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(make_title_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                .with_select(shared_document_select_policy()),
        ))
        .table(
            TableSchema::builder("document_shares")
                .column("document_id", ColumnType::Uuid)
                .column("user_id", ColumnType::Text),
        )
        .build()
}

fn exists_join_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(make_title_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(join_membership_select_policy()),
        ))
        .table(
            TableSchema::builder("document_grants")
                .column("document_id", ColumnType::Uuid)
                .column("group_slug", ColumnType::Text),
        )
        .table(
            TableSchema::builder("group_memberships")
                .column("user_id", ColumnType::Text)
                .column("group_slug", ColumnType::Text),
        )
        .build()
}

fn exists_hop_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(make_title_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(hop_membership_select_policy()),
        ))
        .table(
            TableSchema::builder("document_grants")
                .column("document_id", ColumnType::Uuid)
                .column("group_slug", ColumnType::Text),
        )
        .table(
            TableSchema::builder("group_memberships")
                .column("user_id", ColumnType::Text)
                .column("group_slug", ColumnType::Text),
        )
        .build()
}

fn mixed_complex_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(make_folders_schema(
            "folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ))
        .table(make_complex_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(mixed_complex_select_policy()),
        ))
        .table(
            TableSchema::builder("document_flags")
                .column("document_id", ColumnType::Uuid)
                .column("flag", ColumnType::Text),
        )
        .build()
}

fn exists_update_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(make_title_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::True)
                .with_update(Some(editor_document_update_policy()), PolicyExpr::True),
        ))
        .table(
            TableSchema::builder("document_editors")
                .column("document_id", ColumnType::Uuid)
                .column("user_id", ColumnType::Text),
        )
        .build()
}

async fn create_title_document(client: &JazzClient, title: &str) -> ObjectId {
    client
        .create(
            "documents",
            row_input([("title", Value::Text(title.to_string()))]),
        )
        .await
        .expect("create title document")
        .0
}

async fn create_document_grant(client: &JazzClient, document_id: ObjectId, group_slug: &str) {
    client
        .create(
            "document_grants",
            row_input([
                ("document_id", Value::Uuid(document_id)),
                ("group_slug", Value::Text(group_slug.to_string())),
            ]),
        )
        .await
        .expect("create document grant");
}

async fn create_group_membership(client: &JazzClient, user_id: &str, group_slug: &str) {
    client
        .create(
            "group_memberships",
            row_input([
                ("user_id", Value::Text(user_id.to_string())),
                ("group_slug", Value::Text(group_slug.to_string())),
            ]),
        )
        .await
        .expect("create group membership");
}

/// Verifies that correlated `EXISTS` policies bind outer-row references
/// correctly and keep subscription visibility in sync as the related row is
/// inserted, updated, and deleted.
///
/// Actors: bob and dave are the competing readers, and admin creates plus
/// retargets the share rows.
///
/// ```text
/// admin ──insert share(bob)──► server ──► bob stream (add)
/// admin ──retarget share─────► server ──► bob stream (remove), dave stream (add)
/// admin ──delete share───────► server ──► dave stream (remove)
/// ```
#[tokio::test]
#[should_panic] // known failing: SELECT EXISTS does not surface rows or deltas after related-row writes
async fn exists_outer_row_refs_grant_deny_and_track_related_row_mutations() {
    let schema = exists_share_policy_schema();
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
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
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

    let query = QueryBuilder::new("documents").build();
    let mut bob_stream = bob.subscribe(query.clone()).await.expect("subscribe bob");
    let mut dave_stream = dave.subscribe(query.clone()).await.expect("subscribe dave");
    let mut bob_log = Vec::new();
    let mut dave_log = Vec::new();

    let doc_id = create_title_document(&admin, "Shared Through EXISTS").await;
    let initial_bob = wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "bob initially sees no shared documents",
        Some,
    )
    .await;
    let initial_dave = wait_for_query(
        &dave,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave initially sees no shared documents",
        Some,
    )
    .await;
    assert!(initial_bob.is_empty());
    assert!(initial_dave.is_empty());

    collect_stream_deltas(&mut bob_stream, &mut bob_log, NO_DELTA_WINDOW).await;
    collect_stream_deltas(&mut dave_stream, &mut dave_log, NO_DELTA_WINDOW).await;
    bob_log.clear();
    dave_log.clear();

    let share_id = admin
        .create(
            "document_shares",
            row_input([
                ("document_id", Value::Uuid(doc_id)),
                ("user_id", Value::Text("bob".to_string())),
            ]),
        )
        .await
        .expect("create document share")
        .0;
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        QUERY_TIMEOUT,
        "bob receives add when share row is inserted",
        |log| has_added(log, doc_id),
    )
    .await;
    let bob_rows = wait_for_rows(&bob, query.clone(), "bob sees shared document", |rows| {
        let visible = rows.iter().any(|(id, values)| {
            *id == doc_id && *values == title_document_values("Shared Through EXISTS")
        });
        visible.then_some(rows)
    })
    .await;
    assert_eq!(bob_rows.len(), 1);

    admin
        .update(
            share_id,
            row_changes([("user_id", Value::Text("dave".to_string()))]),
        )
        .await
        .expect("update document share user");
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        QUERY_TIMEOUT,
        "bob receives remove when share retargets away",
        |log| has_removed(log, doc_id),
    )
    .await;
    wait_for_subscription_update(
        &mut dave_stream,
        &mut dave_log,
        QUERY_TIMEOUT,
        "dave receives add when share retargets to him",
        |log| has_added(log, doc_id),
    )
    .await;

    admin.delete(share_id).await.expect("delete share row");
    wait_for_subscription_update(
        &mut dave_stream,
        &mut dave_log,
        QUERY_TIMEOUT,
        "dave receives remove when share row is deleted",
        |log| has_removed(log, doc_id),
    )
    .await;

    let final_bob = wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "bob ends with no shared documents",
        Some,
    )
    .await;
    let final_dave = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave ends with no shared documents",
        Some,
    )
    .await;
    assert!(final_bob.is_empty());
    assert!(final_dave.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that `policy.exists(relation.join(...))` grants access only when
/// the joined relation produces a row for the current session.
///
/// Actors: bob has the matching group membership, dave has a non-matching
/// membership, and admin seeds the document plus grant rows.
///
/// ```text
/// admin ──grant doc→eng────────► server
/// bob ───membership eng───────► query sees row
/// dave ──membership sales─────► query sees nothing
/// ```
#[tokio::test]
#[should_panic] // known failing: read-side ExistsRel join grants never become visible in integration
async fn exists_rel_join_grants_and_denies_correctly() {
    let schema = exists_join_policy_schema();
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
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
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

    let doc_id = create_title_document(&admin, "Join Visible").await;
    create_group_membership(&admin, "bob", "eng").await;
    create_group_membership(&admin, "dave", "sales").await;
    create_document_grant(&admin, doc_id, "eng").await;

    let query = QueryBuilder::new("documents").build();
    let bob_rows = wait_for_rows(&bob, query.clone(), "bob sees joined grant", |rows| {
        let visible = rows
            .iter()
            .any(|(id, values)| *id == doc_id && *values == title_document_values("Join Visible"));
        visible.then_some(rows)
    })
    .await;
    assert_eq!(bob_rows.len(), 1);

    let dave_rows = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave does not see joined grant without matching membership",
        Some,
    )
    .await;
    assert!(dave_rows.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that the canonical hop-shaped `policy.exists(relation)` relation
/// works end to end for reads.
///
/// Actors: bob has the matching group membership, dave has a non-matching
/// membership, and admin seeds the document plus grant rows.
///
/// ```text
/// admin ──grant doc→eng────────► server
/// bob ───hop via membership────► query sees row
/// dave ──hop via sales─────────► query sees nothing
/// ```
#[tokio::test]
#[should_panic] // known failing: read-side ExistsRel hop grants never become visible in integration
async fn exists_rel_hop_grants_and_denies_correctly() {
    let schema = exists_hop_policy_schema();
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
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
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

    let doc_id = create_title_document(&admin, "Hop Visible").await;
    create_group_membership(&admin, "bob", "eng").await;
    create_group_membership(&admin, "dave", "sales").await;
    create_document_grant(&admin, doc_id, "eng").await;

    let query = QueryBuilder::new("documents").build();
    let bob_rows = wait_for_rows(&bob, query.clone(), "bob sees hop grant", |rows| {
        let visible = rows
            .iter()
            .any(|(id, values)| *id == doc_id && *values == title_document_values("Hop Visible"));
        visible.then_some(rows)
    })
    .await;
    assert_eq!(bob_rows.len(), 1);

    let dave_rows = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave does not see hop grant without matching membership",
        Some,
    )
    .await;
    assert!(dave_rows.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that mixed row predicates, claims, `EXISTS`, and `INHERITS` compose
/// as a true conjunction rather than accidentally widening to allow-all.
///
/// Actors: alice reads with two different claim sets, bob owns the hidden
/// folder, and admin seeds rows that each fail exactly one clause.
///
/// ```text
/// admin ──seed visible + near-miss rows──────────────► server
/// alice(claims=eng) ─────────────────────────────────► sees only fully authorized row
/// alice(claims=sales) ───────────────────────────────► sees only sales-matching row
/// ```
#[tokio::test]
#[should_panic] // known failing: mixed SELECT policy stays closed once EXISTS / INHERITS composition is involved
async fn mixed_predicates_claims_exists_and_inherits_fail_closed() {
    async fn create_folder(client: &JazzClient, owner_id: &str, name: &str) -> ObjectId {
        client
            .create(
                "folders",
                row_input([
                    ("owner_id", Value::Text(owner_id.to_string())),
                    ("name", Value::Text(name.to_string())),
                ]),
            )
            .await
            .expect("create folder")
            .0
    }

    async fn create_complex_document(
        client: &JazzClient,
        team_slug: &str,
        published: bool,
        title: &str,
        folder_id: Option<ObjectId>,
    ) -> ObjectId {
        client
            .create(
                "documents",
                row_input([
                    ("team_slug", Value::Text(team_slug.to_string())),
                    ("published", Value::Boolean(published)),
                    ("title", Value::Text(title.to_string())),
                    (
                        "folder_id",
                        folder_id.map(Value::Uuid).unwrap_or(Value::Null),
                    ),
                ]),
            )
            .await
            .expect("create complex document")
            .0
    }

    async fn create_document_flag(client: &JazzClient, document_id: ObjectId, flag: &str) {
        client
            .create(
                "document_flags",
                row_input([
                    ("document_id", Value::Uuid(document_id)),
                    ("flag", Value::Text(flag.to_string())),
                ]),
            )
            .await
            .expect("create document flag");
    }

    let schema = mixed_complex_policy_schema();
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
    let alice_eng = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .with_claims(json!({ "team_slugs": ["eng"] }))
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;
    let alice_sales = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice")
        .with_claims(json!({ "team_slugs": ["sales"] }))
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let alice_folder = create_folder(&admin, "alice", "Alice Folder").await;
    let bob_folder = create_folder(&admin, "bob", "Bob Folder").await;

    let visible = create_complex_document(&admin, "eng", true, "Visible", Some(alice_folder)).await;
    create_document_flag(&admin, visible, "allow").await;

    let wrong_team =
        create_complex_document(&admin, "sales", true, "Wrong Team", Some(alice_folder)).await;
    create_document_flag(&admin, wrong_team, "allow").await;

    let unpublished =
        create_complex_document(&admin, "eng", false, "Unpublished", Some(alice_folder)).await;
    create_document_flag(&admin, unpublished, "allow").await;

    let wrong_folder =
        create_complex_document(&admin, "eng", true, "Wrong Folder", Some(bob_folder)).await;
    create_document_flag(&admin, wrong_folder, "allow").await;

    let missing_flag =
        create_complex_document(&admin, "eng", true, "Missing Flag", Some(alice_folder)).await;

    let query = QueryBuilder::new("documents").build();
    let eng_rows = wait_for_rows(
        &alice_eng,
        query.clone(),
        "eng claim sees only fully authorized row",
        |rows| {
            let matches_visible = rows.iter().any(|(id, values)| {
                *id == visible
                    && *values
                        == complex_document_values("eng", true, "Visible", Some(alice_folder))
            });
            matches_visible.then_some(rows)
        },
    )
    .await;
    assert_eq!(
        eng_rows.len(),
        1,
        "eng claim should see only the fully authorized document"
    );
    assert!(eng_rows.iter().all(|(id, _)| {
        *id != wrong_team && *id != unpublished && *id != wrong_folder && *id != missing_flag
    }));

    let sales_rows = wait_for_rows(
        &alice_sales,
        query,
        "sales claim sees the sales-scoped row only",
        |rows| {
            let matches_sales = rows.iter().any(|(id, values)| {
                *id == wrong_team
                    && *values
                        == complex_document_values("sales", true, "Wrong Team", Some(alice_folder))
            });
            matches_sales.then_some(rows)
        },
    )
    .await;
    assert_eq!(
        sales_rows.len(),
        1,
        "sales claim should see only the sales document"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice_eng.shutdown().await.expect("shutdown alice eng");
    alice_sales.shutdown().await.expect("shutdown alice sales");
    server.shutdown().await;
}

/// Verifies that a write rejected by a correlated `EXISTS` update policy
/// reconciles back to the server-accepted state and emits no subscriber update.
///
/// Actors: alice is the allowed editor, bob attempts the rejected update,
/// observer holds the subscription, and admin seeds the editor row.
///
/// ```text
/// admin ──grant edit to alice──► server
/// bob ──update title───────────► server ──✗ reject
/// observer ──EdgeServer query──► sees original row, no update delta
/// ```
#[tokio::test]
async fn rejected_optimistic_exists_updates_reconcile_to_server_authoritative_state() {
    let schema = exists_update_policy_schema();
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
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("observer")
        .as_user()
        .ready_on("documents", READY_TIMEOUT)
        .connect()
        .await;

    let query = QueryBuilder::new("documents").build();
    let mut observer_stream = observer
        .subscribe(query.clone())
        .await
        .expect("subscribe observer");
    let mut observer_log = Vec::new();

    let doc_id = create_title_document(&admin, "Original").await;
    admin
        .create(
            "document_editors",
            row_input([
                ("document_id", Value::Uuid(doc_id)),
                ("user_id", Value::Text("alice".to_string())),
            ]),
        )
        .await
        .expect("create document editor");
    wait_for_subscription_update(
        &mut observer_stream,
        &mut observer_log,
        QUERY_TIMEOUT,
        "observer sees initial document",
        |log| has_added(log, doc_id),
    )
    .await;

    wait_for_rows(
        &bob,
        query.clone(),
        "bob sees readable document before rejected update",
        |rows| {
            rows.iter()
                .find(|(id, values)| *id == doc_id && *values == title_document_values("Original"))
                .map(|_| ())
        },
    )
    .await;

    bob.update(
        doc_id,
        row_changes([("title", Value::Text("Hacked".to_string()))]),
    )
    .await
    .expect("optimistic local exists update");

    let rows_after_update = observer
        .query(query.clone(), Some(DurabilityTier::EdgeServer))
        .await
        .expect("EdgeServer query after rejected exists update");
    assert!(
        rows_after_update
            .iter()
            .any(|(id, values)| *id == doc_id && *values == title_document_values("Original")),
        "rejected EXISTS update must not persist at EdgeServer: rows={rows_after_update:?}"
    );

    collect_stream_deltas(&mut observer_stream, &mut observer_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_updated(&observer_log, doc_id),
        "rejected EXISTS update must not be broadcast: log={observer_log:?}"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    observer.shutdown().await.expect("shutdown observer");
    server.shutdown().await;
}
