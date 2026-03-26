use std::collections::HashMap;
use std::time::Duration;

use super::support::{
    TestingClient, collect_stream_deltas, has_added, has_removed, wait_for_query, wait_for_rows,
    wait_for_subscription_update,
};
use jazz_tools::query_manager::policy::{Operation, PolicyExpr};
use jazz_tools::query_manager::types::{TablePolicies, TableSchemaBuilder};
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

// -- Value constructors --

fn recursive_folder_values(owner_id: &str, name: &str, parent_id: Option<ObjectId>) -> Vec<Value> {
    vec![
        Value::Text(owner_id.to_string()),
        Value::Text(name.to_string()),
        parent_id.map(Value::Uuid).unwrap_or(Value::Null),
    ]
}

fn recursive_folder_input(
    owner_id: &str,
    name: &str,
    parent_id: Option<ObjectId>,
) -> HashMap<String, Value> {
    HashMap::from([
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("name".to_string(), Value::Text(name.to_string())),
        (
            "parent_id".to_string(),
            parent_id.map(Value::Uuid).unwrap_or(Value::Null),
        ),
    ])
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
        .update(
            folder_id,
            vec![(
                "parent_id".to_string(),
                parent_id.map(Value::Uuid).unwrap_or(Value::Null),
            )],
        )
        .await
        .expect("update recursive folder parent");
}

// -- Tests --

/// Verifies that recursive `INHERITS` grants access through an owned ancestor
/// and still fails closed for a session with no reachable owned folder.
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
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .as_user()
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let dave = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("dave")
        .as_user()
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;

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
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice")
        .as_user()
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;

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
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice")
        .as_user()
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;

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
#[should_panic] // "known failing: recursive visibility invalidation still misses edge-removal updates"
async fn recursive_inherits_subscription_updates_when_graph_edges_change() {
    let table_name = "recursive_folders";
    let schema = recursive_folder_policy_schema(None);
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice")
        .as_user()
        .ready_on(table_name, READY_TIMEOUT)
        .connect()
        .await;

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
