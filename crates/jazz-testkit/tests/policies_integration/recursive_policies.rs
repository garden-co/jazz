use std::collections::HashMap;
use std::time::Duration;

use jazz::query::Query;

use super::support::{
    collect_stream_deltas, connect_ready_client, connect_ready_user, has_added_id, has_any_change,
    has_removed, wait_for_query, wait_for_rows, wait_for_subscription_update,
};
use super::{pe, permissions};
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, Schema, SchemaBuilder, TablePolicies,
    TableSchema, TableSchemaBuilder, Value,
};
use jazz::tools::{Operation, PolicyExpr};
use jazz_server::JazzServer;

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
        Some(depth) => pe::allowed_to_with_depth(operation, via_column, depth),
        None => pe::allowed_to(operation, via_column),
    };

    pe::all_of([pe::is_not_null(via_column), inherits])
}

fn recursive_folder_select_policy(max_depth: Option<usize>) -> PolicyExpr {
    let owner_policy = pe::eq("owner_id", pe::session(vec!["claims", "sub"]));
    let inherited_policy =
        inherited_non_null_policy_with_depth(Operation::Select, "parent_id", max_depth);

    pe::any_of([owner_policy, inherited_policy])
}

fn recursive_folder_policy_schema(max_depth: Option<usize>) -> Schema {
    SchemaBuilder::new()
        .table(make_recursive_folders_schema(
            "recursive_folders",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_update().always();
                p.allow_read()
                    .where_(recursive_folder_select_policy(max_depth));
            }),
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

fn recursive_relation_document_select_policy() -> PolicyExpr {
    let seed = pe::table("team_memberships")
        .where_(pe::rel::eq_session("user_id", "claims.sub"))
        .join(
            pe::table("teams").alias("seed_team"),
            pe::rel::column("team_memberships", "team_id"),
            pe::rel::column("seed_team", "id"),
        )
        .select([("id", pe::rel::column("seed_team", "id"))]);
    let step = pe::table("team_edges")
        .where_(pe::rel::eq_frontier("child_team"))
        .select([("id", "parent_team")]);

    pe::exists(
        seed.gather(step, 10)
            .join(
                pe::table("resource_access_edges").alias("access"),
                pe::rel::column("teams", "id"),
                pe::rel::column("access", "team_id"),
            )
            .where_(pe::rel::all_of([
                pe::rel::eq_outer(pe::rel::column("access", "resource_id"), "id"),
                pe::rel::eq_literal(pe::rel::column("access", "grant_role"), "viewer"),
            ])),
    )
}

fn recursive_relation_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("teams").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("team_edges")
                .fk_column("child_team", "teams")
                .fk_column("parent_team", "teams"),
        )
        .table(
            TableSchema::builder("team_memberships")
                .column("user_id", ColumnType::Text)
                .fk_column("team_id", "teams"),
        )
        .table(
            TableSchema::builder("resource_access_edges")
                .fk_column("team_id", "teams")
                .fk_column("resource_id", "documents")
                .column("grant_role", ColumnType::Text),
        )
        .table(make_recursive_relation_documents_schema(
            "documents",
            permissions(|p| {
                p.allow_insert().always();
                p.allow_update().always();
                p.allow_read()
                    .where_(recursive_relation_document_select_policy());
            }),
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
    jazz::row_input!("owner_id" => owner_id, "name" => name, "parent_id" => parent_id)
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
        .insert(
            table_name,
            recursive_folder_input(owner_id, name, parent_id),
        )
        .expect("create recursive folder")
        .0
}

async fn update_recursive_folder_parent(
    client: &JazzClient,
    table_name: &str,
    folder_id: ObjectId,
    parent_id: Option<ObjectId>,
) {
    client
        .update(
            table_name,
            folder_id,
            vec![("parent_id".to_string(), parent_id.into())],
        )
        .expect("update recursive folder parent");
}

async fn create_team(client: &JazzClient, name: &str) -> ObjectId {
    client
        .insert("teams", jazz::row_input!("name" => name))
        .expect("create team")
        .0
}

async fn create_team_edge(
    client: &JazzClient,
    child_team: ObjectId,
    parent_team: ObjectId,
) -> ObjectId {
    let (edge_id, _, transaction_id) = client
        .insert(
            "team_edges",
            jazz::row_input!("child_team" => Value::Uuid(child_team), "parent_team" => Value::Uuid(parent_team)))

        .expect("create team edge");
    tokio::time::timeout(
        QUERY_TIMEOUT,
        client.wait_for_transaction(
            transaction_id.expect("team edge insert must commit immediately"),
            DurabilityTier::GlobalServer,
        ),
    )
    .await
    .expect("team edge settlement timed out")
    .expect("team edge must reach the server");
    edge_id
}

async fn create_team_membership(client: &JazzClient, user_id: &str, team_id: ObjectId) {
    client
        .insert(
            "team_memberships",
            jazz::row_input!("user_id" => user_id, "team_id" => Value::Uuid(team_id)),
        )
        .expect("create team membership");
}

async fn create_resource_access_edge(
    client: &JazzClient,
    team_id: ObjectId,
    resource_id: ObjectId,
    grant_role: &str,
) {
    client
        .insert(
            "resource_access_edges",
            jazz::row_input!("team_id" => Value::Uuid(team_id), "resource_id" => Value::Uuid(resource_id), "grant_role" => grant_role))

        .expect("create resource access edge");
}

async fn create_title_document(client: &JazzClient, title: &str) -> ObjectId {
    client
        .insert("documents", jazz::row_input!("title" => title))
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
    tokio::task::LocalSet::new()
        .run_until(
            recursive_inherits_grants_visible_ancestor_chain_and_denies_unrelated_sessions_inner(),
        )
        .await;
}

async fn recursive_inherits_grants_visible_ancestor_chain_and_denies_unrelated_sessions_inner() {
    let table_name = "recursive_folders";
    let schema = recursive_folder_policy_schema(None);
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", table_name, READY_TIMEOUT).await;
    let alice =
        connect_ready_user(&server, &schema, super::ALICE_ID, table_name, READY_TIMEOUT).await;
    let dave =
        connect_ready_user(&server, &schema, super::DAVE_ID, table_name, READY_TIMEOUT).await;

    let root = create_recursive_folder(&admin, table_name, super::ALICE_ID, "Root", None).await;
    let child =
        create_recursive_folder(&admin, table_name, super::BOB_ID, "Child", Some(root)).await;
    let grand =
        create_recursive_folder(&admin, table_name, super::CAROL_ID, "Grand", Some(child)).await;
    let query = Query::from(table_name);

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
        *id == root && *values == recursive_folder_values(super::ALICE_ID, "Root", None)
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == child && *values == recursive_folder_values(super::BOB_ID, "Child", Some(root))
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == grand && *values == recursive_folder_values(super::CAROL_ID, "Grand", Some(child))
    }));

    let dave_rows = wait_for_query(
        &dave,
        query,
        jazz::tools::ReadTier::Remote,
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
    tokio::task::LocalSet::new()
        .run_until(recursive_inherits_respects_max_depth_boundaries_inner())
        .await;
}

async fn recursive_inherits_respects_max_depth_boundaries_inner() {
    let table_name = "recursive_folders";
    let schema = recursive_folder_policy_schema(Some(1));
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", table_name, READY_TIMEOUT).await;
    let alice =
        connect_ready_user(&server, &schema, super::ALICE_ID, table_name, READY_TIMEOUT).await;

    let root = create_recursive_folder(&admin, table_name, super::ALICE_ID, "Root", None).await;
    let child =
        create_recursive_folder(&admin, table_name, super::BOB_ID, "Child", Some(root)).await;
    let grand =
        create_recursive_folder(&admin, table_name, super::CAROL_ID, "Grand", Some(child)).await;
    let query = Query::from(table_name);

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
        *id == root && *values == recursive_folder_values(super::ALICE_ID, "Root", None)
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == child && *values == recursive_folder_values(super::BOB_ID, "Child", Some(root))
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
    tokio::task::LocalSet::new()
        .run_until(recursive_inherits_cycles_fail_closed_without_poisoning_acyclic_branch_inner())
        .await;
}

async fn recursive_inherits_cycles_fail_closed_without_poisoning_acyclic_branch_inner() {
    let table_name = "recursive_folders";
    let schema = recursive_folder_policy_schema(Some(10));
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", table_name, READY_TIMEOUT).await;
    let alice =
        connect_ready_user(&server, &schema, super::ALICE_ID, table_name, READY_TIMEOUT).await;

    let root = create_recursive_folder(&admin, table_name, super::ALICE_ID, "Root", None).await;
    let child = create_recursive_folder(
        &admin,
        table_name,
        super::DAVE_ID,
        "Visible Child",
        Some(root),
    )
    .await;
    let cycle_a = create_recursive_folder(&admin, table_name, super::BOB_ID, "Cycle A", None).await;
    let cycle_b = create_recursive_folder(
        &admin,
        table_name,
        super::CAROL_ID,
        "Cycle B",
        Some(cycle_a),
    )
    .await;
    update_recursive_folder_parent(&admin, table_name, cycle_a, Some(cycle_b)).await;

    let query = Query::from(table_name);
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
        *id == root && *values == recursive_folder_values(super::ALICE_ID, "Root", None)
    }));
    assert!(alice_rows.iter().any(|(id, values)| {
        *id == child
            && *values == recursive_folder_values(super::DAVE_ID, "Visible Child", Some(root))
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
    tokio::task::LocalSet::new()
        .run_until(recursive_inherits_subscription_updates_when_graph_edges_change_inner())
        .await;
}

async fn recursive_inherits_subscription_updates_when_graph_edges_change_inner() {
    let table_name = "recursive_folders";
    let schema = recursive_folder_policy_schema(None);
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", table_name, READY_TIMEOUT).await;
    let alice =
        connect_ready_user(&server, &schema, super::ALICE_ID, table_name, READY_TIMEOUT).await;

    let root = create_recursive_folder(&admin, table_name, super::ALICE_ID, "Root", None).await;
    let child = create_recursive_folder(&admin, table_name, super::BOB_ID, "Child", None).await;
    let grand =
        create_recursive_folder(&admin, table_name, super::CAROL_ID, "Grand", Some(child)).await;
    let query = Query::from(table_name);

    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("subscribe alice recursive folders");
    let mut alice_log = Vec::new();

    let initial_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice initially sees only owned root without recursive edge",
        |rows| (!rows.is_empty()).then_some(rows),
    )
    .await;
    assert_eq!(initial_rows.len(), 1);

    collect_stream_deltas(&mut alice_stream, &mut alice_log, NO_DELTA_WINDOW).await;
    alice_log.clear();

    update_recursive_folder_parent(&admin, table_name, child, Some(root)).await;
    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        QUERY_TIMEOUT,
        "alice receives recursive add deltas after attaching child to owned root",
        |log| has_added_id(log, child) && has_added_id(log, grand),
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
        *id == child && *values == recursive_folder_values(super::BOB_ID, "Child", Some(root))
    }));
    assert!(attached_rows.iter().any(|(id, values)| {
        *id == grand && *values == recursive_folder_values(super::CAROL_ID, "Grand", Some(child))
    }));

    collect_stream_deltas(&mut alice_stream, &mut alice_log, NO_DELTA_WINDOW).await;
    alice_log.clear();

    update_recursive_folder_parent(&admin, table_name, child, None).await;
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
async fn recursive_exists_rel_gather_hop_grants_reachable_ancestor_and_denies_without_path() {
    tokio::task::LocalSet::new()
        .run_until(
            recursive_exists_rel_gather_hop_grants_reachable_ancestor_and_denies_without_path_inner(
            ),
        )
        .await;
}

async fn recursive_exists_rel_gather_hop_grants_reachable_ancestor_and_denies_without_path_inner() {
    let schema = recursive_relation_policy_schema();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "documents", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;
    let dave =
        connect_ready_user(&server, &schema, super::DAVE_ID, "documents", READY_TIMEOUT).await;

    let root = create_team(&admin, "root").await;
    let leaf = create_team(&admin, "leaf").await;
    let outsider = create_team(&admin, "outsider").await;
    create_team_edge(&admin, leaf, root).await;
    create_team_membership(&admin, super::BOB_ID, leaf).await;
    create_team_membership(&admin, super::DAVE_ID, outsider).await;

    let doc_id = create_title_document(&admin, "Ancestor Viewer Grant").await;
    create_resource_access_edge(&admin, root, doc_id, "viewer").await;

    let query = Query::from("documents");
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
        jazz::tools::ReadTier::Remote,
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
/// already granting access. Removing one path preserves visibility; removing
/// the last path revokes access in queries and the existing subscription.
///
/// Actors: bob holds the subscription, admin grows then removes the diamond,
/// and the root team remains the single grant source for the document.
///
/// ```text
/// initial path: leaf ─► mid_a ─► root ─► document grant
/// later path:   leaf ─► mid_b ─► root ─► same document grant
///
/// bob should keep exactly one visible document, with no second add delta.
/// ```
#[tokio::test]
async fn recursive_exists_rel_diamond_paths_do_not_duplicate_visibility_or_deltas() {
    tokio::task::LocalSet::new()
        .run_until(recursive_exists_rel_diamond_paths_do_not_duplicate_visibility_or_deltas_inner())
        .await;
}

async fn recursive_exists_rel_diamond_paths_do_not_duplicate_visibility_or_deltas_inner() {
    let schema = recursive_relation_policy_schema();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let admin = connect_ready_client(&server, &schema, "admin", "documents", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;

    let root = create_team(&admin, "root").await;
    let mid_a = create_team(&admin, "mid-a").await;
    let mid_b = create_team(&admin, "mid-b").await;
    let leaf = create_team(&admin, "leaf").await;
    let first_path_edge = create_team_edge(&admin, leaf, mid_a).await;
    create_team_edge(&admin, mid_a, root).await;
    create_team_membership(&admin, super::BOB_ID, leaf).await;

    let doc_id = create_title_document(&admin, "Diamond Grant").await;
    create_resource_access_edge(&admin, root, doc_id, "viewer").await;

    let query = Query::from("documents");
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
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        QUERY_TIMEOUT,
        "initial recursive grant subscription add",
        |log| has_added_id(log, doc_id),
    )
    .await;
    bob_log.clear();

    let second_path_edge = create_team_edge(&admin, leaf, mid_b).await;
    create_team_edge(&admin, mid_b, root).await;

    let rows_after_second_path = bob
        .query(query.clone(), jazz::tools::ReadTier::Remote)
        .await
        .map(jazz::tools::test_support::ordinary_rows)
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

    bob_log.clear();
    let tx = admin
        .delete("team_edges", first_path_edge)
        .expect("remove first recursive path")
        .expect("edge deletion transaction");
    jazz_testkit::wait_for_global_txs(&admin, &[tx]).await;
    let remaining_rows = bob
        .query(query.clone(), jazz::tools::ReadTier::Remote)
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query documents with one remaining path");
    assert_eq!(
        remaining_rows,
        vec![(doc_id, title_document_values("Diamond Grant"))]
    );
    collect_stream_deltas(&mut bob_stream, &mut bob_log, NO_DELTA_WINDOW).await;
    assert!(
        !has_any_change(&bob_log, doc_id),
        "removing one of two paths must preserve subscription visibility: log={bob_log:?}"
    );

    bob_log.clear();
    let tx = admin
        .delete("team_edges", second_path_edge)
        .expect("remove last recursive path")
        .expect("edge deletion transaction");
    jazz_testkit::wait_for_global_txs(&admin, &[tx]).await;
    wait_for_query(
        &bob,
        query,
        jazz::tools::ReadTier::Remote,
        QUERY_TIMEOUT,
        "no recursive path must mean no visible document",
        |rows| rows.is_empty().then_some(()),
    )
    .await;
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        QUERY_TIMEOUT,
        "last recursive path removal revokes subscription visibility",
        |log| has_removed(log, doc_id),
    )
    .await;

    admin.shutdown().await.expect("shutdown admin");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
