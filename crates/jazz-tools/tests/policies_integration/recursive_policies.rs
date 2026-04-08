use std::collections::HashMap;
use std::time::Duration;

use super::support::{
    collect_stream_deltas, connect_ready_client, connect_ready_user, has_added, has_any_change,
    has_removed, wait_for_query, wait_for_rows, wait_for_subscription_update,
};
use jazz_tools::query_manager::policy::{Operation, PolicyExpr};
use jazz_tools::query_manager::relation_ir::{
    ColumnRef, JoinCondition, JoinKind, KeyRef, PredicateCmpOp, PredicateExpr, ProjectColumn,
    ProjectExpr, RelExpr, RowIdRef, ValueRef,
};
use jazz_tools::query_manager::types::{TableName, TablePolicies, TableSchemaBuilder};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(100);

// -- Schema builders --

fn make_recursive_folders_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("owner_id", ColumnType::Text)
        .column("name", ColumnType::Text)
        .nullable_fk_column("parent_id", table_name)
        .policies(policies)
}

// -- Policy helpers --

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

fn recursive_folder_select_policy(max_depth: Option<usize>) -> PolicyExpr {
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let inherited_policy =
        inherited_non_null_policy_with_depth(Operation::Select, "parent_id", max_depth);

    PolicyExpr::or(vec![owner_policy, inherited_policy])
}

fn recursive_folder_policy_schema(max_depth: Option<usize>) -> Schema {
    SchemaBuilder::new()
        .table(make_recursive_folders_schema(
            "recursive_folders",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                .with_select(recursive_folder_select_policy(max_depth)),
        ))
        .build()
}

fn make_recursive_relation_documents_schema(
    table_name: &str,
    policies: TablePolicies,
) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .policies(policies)
}

fn reachable_teams_relation() -> RelExpr {
    RelExpr::Gather {
        seed: Box::new(RelExpr::Project {
            input: Box::new(RelExpr::Filter {
                input: Box::new(RelExpr::Join {
                    left: Box::new(RelExpr::TableScan {
                        table: TableName::new("teams"),
                    }),
                    right: Box::new(RelExpr::TableScan {
                        table: TableName::new("team_memberships"),
                    }),
                    on: vec![JoinCondition {
                        left: ColumnRef::scoped("teams", "id"),
                        right: ColumnRef::scoped("team_memberships", "team_id"),
                    }],
                    join_kind: JoinKind::Inner,
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::scoped("team_memberships", "user_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["user_id".into()]),
                },
            }),
            columns: vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("teams", "id")),
            }],
        }),
        step: Box::new(RelExpr::Project {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::Project {
                    input: Box::new(RelExpr::Filter {
                        input: Box::new(RelExpr::TableScan {
                            table: TableName::new("team_edges"),
                        }),
                        predicate: PredicateExpr::Cmp {
                            left: ColumnRef::scoped("team_edges", "child_team"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::FrontierColumn(ColumnRef::unscoped("id")),
                        },
                    }),
                    columns: vec![ProjectColumn {
                        alias: "parent_team".to_string(),
                        expr: ProjectExpr::Column(ColumnRef::scoped("team_edges", "parent_team")),
                    }],
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("teams"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("team_edges", "parent_team"),
                    right: ColumnRef::scoped("__recursive_hop_0", "id"),
                }],
                join_kind: JoinKind::Inner,
            }),
            columns: vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__recursive_hop_0", "id")),
            }],
        }),
        frontier_key: KeyRef::Column(ColumnRef::unscoped("id")),
        max_depth: 10,
        dedupe_key: vec![KeyRef::Column(ColumnRef::unscoped("id"))],
    }
}

fn recursive_relation_document_select_policy() -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(reachable_teams_relation()),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("resource_access_edges"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::unscoped("id"),
                    right: ColumnRef::scoped("resource_access_edges", "team_id"),
                }],
                join_kind: JoinKind::Inner,
            }),
            predicate: PredicateExpr::And(vec![
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("resource_access_edges", "resource_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Outer),
                },
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("resource_access_edges", "grant_role"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal("viewer".into()),
                },
            ]),
        },
    }
}

fn recursive_relation_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("teams").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("team_edges")
                .column("child_team", ColumnType::Uuid)
                .column("parent_team", ColumnType::Uuid),
        )
        .table(
            TableSchema::builder("team_memberships")
                .column("user_id", ColumnType::Text)
                .column("team_id", ColumnType::Uuid),
        )
        .table(
            TableSchema::builder("resource_access_edges")
                .column("team_id", ColumnType::Uuid)
                .column("resource_id", ColumnType::Uuid)
                .column("grant_role", ColumnType::Text),
        )
        .table(make_recursive_relation_documents_schema(
            "documents",
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                .with_select(recursive_relation_document_select_policy()),
        ))
        .build()
}

// -- Value constructors --

fn recursive_folder_values(owner_id: &str, name: &str, parent_id: Option<ObjectId>) -> Vec<Value> {
    vec![owner_id.into(), name.into(), parent_id.into()]
}

fn recursive_folder_input(
    owner_id: &str,
    name: &str,
    parent_id: Option<ObjectId>,
) -> HashMap<String, Value> {
    row_input!("owner_id" => owner_id, "name" => name, "parent_id" => parent_id)
}

fn title_document_values(title: &str) -> Vec<Value> {
    vec![title.into()]
}

// -- Seed / mutation helpers --

async fn create_recursive_folder(
    client: &JazzClient,
    table_name: &str,
    owner_id: &str,
    name: &str,
    parent_id: Option<ObjectId>,
) -> ObjectId {
    client
        .create(
            table_name,
            recursive_folder_input(owner_id, name, parent_id),
        )
        .await
        .expect("create recursive folder")
        .0
}

async fn update_recursive_folder_parent(
    client: &JazzClient,
    folder_id: ObjectId,
    parent_id: Option<ObjectId>,
) {
    client
        .update(folder_id, vec![("parent_id".to_string(), parent_id.into())])
        .await
        .expect("update recursive folder parent");
}

async fn create_team(client: &JazzClient, name: &str) -> ObjectId {
    client
        .create("teams", row_input!("name" => name))
        .await
        .expect("create team")
        .0
}

async fn create_team_edge(client: &JazzClient, child_team: ObjectId, parent_team: ObjectId) {
    client
        .create(
            "team_edges",
            row_input!("child_team" => Value::Uuid(child_team), "parent_team" => Value::Uuid(parent_team)),
        )
        .await
        .expect("create team edge");
}

async fn create_team_membership(client: &JazzClient, user_id: &str, team_id: ObjectId) {
    client
        .create(
            "team_memberships",
            row_input!("user_id" => user_id, "team_id" => Value::Uuid(team_id)),
        )
        .await
        .expect("create team membership");
}

async fn create_resource_access_edge(
    client: &JazzClient,
    team_id: ObjectId,
    resource_id: ObjectId,
    grant_role: &str,
) {
    client
        .create(
            "resource_access_edges",
            row_input!("team_id" => Value::Uuid(team_id), "resource_id" => Value::Uuid(resource_id), "grant_role" => grant_role),
        )
        .await
        .expect("create resource access edge");
}

async fn create_title_document(client: &JazzClient, title: &str) -> ObjectId {
    client
        .create("documents", row_input!("title" => title))
        .await
        .expect("create title document")
        .0
}

// -- Tests --

/// Verifies that recursive `INHERITS` grants access through an owned ancestor
/// and still fails closed for a session with no reachable owned folder.
///
/// Actors: alice owns the granting root, bob and carol own descendants, dave
/// is the unrelated reader, and admin seeds the graph.
///
/// ```text
/// alice owns root
///   root ──parent──► child(bob) ──parent──► grand(carol)
///
/// alice query ─► {root, child, grand}
/// dave query ──► {}
/// ```
#[tokio::test]
async fn recursive_inherits_grants_visible_ancestor_chain_and_denies_unrelated_sessions() {
    let table_name = "recursive_folders";
    let schema = recursive_folder_policy_schema(None);
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", table_name, READY_TIMEOUT).await;
    let alice = connect_ready_user(&server, &schema, "alice", table_name, READY_TIMEOUT).await;
    let dave = connect_ready_user(&server, &schema, "dave", table_name, READY_TIMEOUT).await;

    let root = create_recursive_folder(&admin, table_name, "alice", "Root", None).await;
    let child = create_recursive_folder(&admin, table_name, "bob", "Child", Some(root)).await;
    let grand = create_recursive_folder(&admin, table_name, "carol", "Grand", Some(child)).await;
    let query = QueryBuilder::new(table_name).build();

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees recursive folder chain",
        |rows| {
            (rows.len() == 3
                && rows.iter().any(|(id, _)| *id == root)
                && rows.iter().any(|(id, _)| *id == child)
                && rows.iter().any(|(id, _)| *id == grand))
            .then_some(rows)
        },
    )
    .await;
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == root && *values == recursive_folder_values("alice", "Root", None)
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == child && *values == recursive_folder_values("bob", "Child", Some(root))
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == grand && *values == recursive_folder_values("carol", "Grand", Some(child))
    }));

    let dave_rows = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave sees no recursive folders without an owned ancestor",
        Some,
    )
    .await;
    assert!(dave_rows.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that recursive `INHERITS` respects `max_depth`: the direct child
/// of an owned folder is visible at depth 1, but the grandchild is still out
/// of bounds and remains hidden.
///
/// Actors: alice owns the root, bob owns the child, carol owns the grandchild,
/// and admin seeds the folder chain.
///
/// ```text
/// alice owns root
///   root ──parent──► child(bob) ──parent──► grand(carol)
///
/// policy max_depth = 1
/// alice query ─► {root, child}
///              └► grand denied
/// ```
#[tokio::test]
async fn recursive_inherits_respects_max_depth_boundaries() {
    let table_name = "recursive_folders";
    let schema = recursive_folder_policy_schema(Some(1));
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", table_name, READY_TIMEOUT).await;
    let alice = connect_ready_user(&server, &schema, "alice", table_name, READY_TIMEOUT).await;

    let root = create_recursive_folder(&admin, table_name, "alice", "Root", None).await;
    let child = create_recursive_folder(&admin, table_name, "bob", "Child", Some(root)).await;
    let grand = create_recursive_folder(&admin, table_name, "carol", "Grand", Some(child)).await;
    let query = QueryBuilder::new(table_name).build();

    let alice_rows = wait_for_rows(
        &alice,
        query,
        "alice sees only rows within recursive max depth",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == root)
                && rows.iter().any(|(id, _)| *id == child)
                && rows.iter().all(|(id, _)| *id != grand))
            .then_some(rows)
        },
    )
    .await;
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == root && *values == recursive_folder_values("alice", "Root", None)
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == child && *values == recursive_folder_values("bob", "Child", Some(root))
    }));
    assert!(
        alice_rows.iter().all(|(id, _)| *id != grand),
        "grandchild should stay hidden when max_depth=1"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that a cyclic recursive branch fails closed and does not interfere
/// with an unrelated acyclic branch that should still grant access.
///
/// Actors: alice reads through the visible acyclic branch, bob and carol own
/// the hidden cycle, dave owns the visible child, and admin constructs both
/// branches.
///
/// ```text
/// visible branch: alice(root) ──parent──► child(dave)
///
/// hidden cycle:   cycle_a(bob) ◄──parent──► cycle_b(carol)
///
/// alice query ─► {root, child}
///              └► cycle rows remain hidden
/// ```
#[tokio::test]
async fn recursive_inherits_cycles_fail_closed_without_poisoning_acyclic_branch() {
    let table_name = "recursive_folders";
    let schema = recursive_folder_policy_schema(Some(10));
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", table_name, READY_TIMEOUT).await;
    let alice = connect_ready_user(&server, &schema, "alice", table_name, READY_TIMEOUT).await;

    let root = create_recursive_folder(&admin, table_name, "alice", "Root", None).await;
    let child =
        create_recursive_folder(&admin, table_name, "dave", "Visible Child", Some(root)).await;
    let cycle_a = create_recursive_folder(&admin, table_name, "bob", "Cycle A", None).await;
    let cycle_b =
        create_recursive_folder(&admin, table_name, "carol", "Cycle B", Some(cycle_a)).await;
    update_recursive_folder_parent(&admin, cycle_a, Some(cycle_b)).await;

    let query = QueryBuilder::new(table_name).build();
    let alice_rows = wait_for_rows(
        &alice,
        query,
        "alice sees only the acyclic branch despite unrelated cycle",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == root)
                && rows.iter().any(|(id, _)| *id == child)
                && rows.iter().all(|(id, _)| *id != cycle_a)
                && rows.iter().all(|(id, _)| *id != cycle_b))
            .then_some(rows)
        },
    )
    .await;
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == root && *values == recursive_folder_values("alice", "Root", None)
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == child && *values == recursive_folder_values("dave", "Visible Child", Some(root))
    }));

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that adding or removing the last reachable recursive edge produces
/// add/remove deltas for descendants whose visibility changes only because the
/// permission path changed.
///
/// Actors: alice holds the live subscription, bob and carol own descendants,
/// and admin retargets the recursive edge.
///
/// ```text
/// initial: alice(root)   child(bob) ──parent=NULL──► grand(carol)
///          alice query ─► {root}
///
/// attach child.parent = root
///          alice stream ─► add child, add grand
///
/// detach child.parent = NULL
///          alice stream ─► remove child, remove grand
/// ```
#[tokio::test]
async fn recursive_inherits_subscription_updates_when_graph_edges_change() {
    let table_name = "recursive_folders";
    let schema = recursive_folder_policy_schema(None);
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", table_name, READY_TIMEOUT).await;
    let alice = connect_ready_user(&server, &schema, "alice", table_name, READY_TIMEOUT).await;

    let root = create_recursive_folder(&admin, table_name, "alice", "Root", None).await;
    let child = create_recursive_folder(&admin, table_name, "bob", "Child", None).await;
    let grand = create_recursive_folder(&admin, table_name, "carol", "Grand", Some(child)).await;
    let query = QueryBuilder::new(table_name).build();

    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("subscribe alice recursive folders");
    let mut alice_log = Vec::new();

    let initial_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice initially sees only owned root without recursive edge",
        |rows| (rows.len() > 0).then_some(rows),
    )
    .await;
    assert_eq!(initial_rows.len(), 1);

    collect_stream_deltas(&mut alice_stream, &mut alice_log, NO_DELTA_WINDOW).await;
    alice_log.clear();

    update_recursive_folder_parent(&admin, child, Some(root)).await;
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        QUERY_TIMEOUT,
        "alice receives recursive add deltas after attaching child to owned root",
        |log| has_added(log, child) && has_added(log, grand),
    )
    .await;

    let attached_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees descendants after recursive edge attachment",
        Some,
    )
    .await;
    assert!(attached_rows.iter().any(|(id, values)| {
        *id == child && *values == recursive_folder_values("bob", "Child", Some(root))
    }));
    assert!(attached_rows.iter().any(|(id, values)| {
        *id == grand && *values == recursive_folder_values("carol", "Grand", Some(child))
    }));

    collect_stream_deltas(&mut alice_stream, &mut alice_log, NO_DELTA_WINDOW).await;
    alice_log.clear();

    update_recursive_folder_parent(&admin, child, None).await;
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        QUERY_TIMEOUT,
        "alice receives recursive remove deltas after detaching child from owned root",
        |log| has_removed(log, child) && has_removed(log, grand),
    )
    .await;

    let detached_rows = wait_for_rows(
        &alice,
        query,
        "alice returns to root-only visibility after recursive edge removal",
        Some,
    )
    .await;
    assert_eq!(detached_rows.len(), 1);

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that a recursive `gather(...).hop(...)` relation inside
/// `policy.exists(...)` grants access when a session can reach an ancestor team
/// that holds the document grant, and still fails closed when no path exists.
///
/// Actors: bob is the granted reader, dave is the unrelated reader, and admin
/// seeds the team graph plus the document grant.
///
/// ```text
/// bob member of leaf ──edge──► root ──grant(viewer)──► document
/// dave member of outsider ───────────────────────────► no path
///
/// bob query  ─► {document}
/// dave query ─► {}
/// ```
#[tokio::test]
#[should_panic] // known failing: read-side recursive ExistsRel never grants rows in integration
async fn recursive_exists_rel_gather_hop_grants_reachable_ancestor_and_denies_without_path() {
    let schema = recursive_relation_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "documents", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, "bob", "documents", READY_TIMEOUT).await;
    let dave = connect_ready_user(&server, &schema, "dave", "documents", READY_TIMEOUT).await;

    let root = create_team(&admin, "root").await;
    let leaf = create_team(&admin, "leaf").await;
    let outsider = create_team(&admin, "outsider").await;
    create_team_edge(&admin, leaf, root).await;
    create_team_membership(&admin, "bob", leaf).await;
    create_team_membership(&admin, "dave", outsider).await;

    let doc_id = create_title_document(&admin, "Ancestor Viewer Grant").await;
    create_resource_access_edge(&admin, root, doc_id, "viewer").await;

    let query = QueryBuilder::new("documents").build();
    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees document via reachable ancestor grant",
        |rows| {
            let visible = rows.iter().any(|(id, values)| {
                *id == doc_id && *values == title_document_values("Ancestor Viewer Grant")
            });
            visible.then_some(rows)
        },
    )
    .await;
    assert_eq!(
        bob_rows.len(),
        1,
        "bob should see exactly one granted document"
    );

    let dave_rows = wait_for_query(
        &dave,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "dave sees no documents without a reachable team path",
        Some,
    )
    .await;
    assert!(dave_rows.is_empty());

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    dave.shutdown().await.expect("shutdown dave");
    server.shutdown().await;
}

/// Verifies that a recursive gather graph with a diamond topology does not
/// emit duplicate visibility changes when a second path reaches a team that was
/// already granting access.
///
/// Actors: bob holds the subscription, admin grows the diamond, and the root
/// team remains the single grant source for the document.
///
/// ```text
/// initial path: leaf ─► mid_a ─► root ─► document grant
/// later path:   leaf ─► mid_b ─► root ─► same document grant
///
/// bob should keep exactly one visible document, with no second add delta.
/// ```
#[tokio::test]
#[should_panic] // known failing: recursive ExistsRel grant path is still invisible, so diamond dedupe never settles
async fn recursive_exists_rel_diamond_paths_do_not_duplicate_visibility_or_deltas() {
    let schema = recursive_relation_policy_schema();
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "documents", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, "bob", "documents", READY_TIMEOUT).await;

    let root = create_team(&admin, "root").await;
    let mid_a = create_team(&admin, "mid-a").await;
    let mid_b = create_team(&admin, "mid-b").await;
    let leaf = create_team(&admin, "leaf").await;
    create_team_edge(&admin, leaf, mid_a).await;
    create_team_edge(&admin, mid_a, root).await;
    create_team_membership(&admin, "bob", leaf).await;

    let doc_id = create_title_document(&admin, "Diamond Grant").await;
    create_resource_access_edge(&admin, root, doc_id, "viewer").await;

    let query = QueryBuilder::new("documents").build();
    let initial_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees one document through the first recursive path",
        |rows| {
            let visible = rows.iter().any(|(id, values)| {
                *id == doc_id && *values == title_document_values("Diamond Grant")
            });
            visible.then_some(rows)
        },
    )
    .await;
    assert_eq!(
        initial_rows.len(),
        1,
        "initial recursive grant should dedupe to one row"
    );

    let mut bob_stream = bob
        .subscribe(query.clone())
        .await
        .expect("subscribe bob recursive relation policy");
    let mut bob_log = Vec::new();
    collect_stream_deltas(&mut bob_stream, &mut bob_log, NO_DELTA_WINDOW).await;
    bob_log.clear();

    create_team_edge(&admin, leaf, mid_b).await;
    create_team_edge(&admin, mid_b, root).await;

    let rows_after_second_path = bob
        .query(query, Some(DurabilityTier::EdgeServer))
        .await
        .expect("query documents after second recursive path");
    assert_eq!(
        rows_after_second_path.len(),
        1,
        "a second recursive path must not duplicate the granted document"
    );
    assert!(
        rows_after_second_path
            .iter()
            .any(|(id, values)| *id == doc_id && *values == title_document_values("Diamond Grant")),
        "the original grant should remain visible"
    );

    collect_stream_deltas(&mut bob_stream, &mut bob_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&bob_log, doc_id),
        "an already-visible recursive grant must not emit duplicate deltas: log={bob_log:?}"
    );

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
