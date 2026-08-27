//! Reverse-reference read grants resolved through `Array<Uuid>` reference
//! columns: a target row is readable when a visible source row lists the
//! target's id in a UUID-array column, and that grant follows array edits.

use jazz_testkit as support;

use std::time::Duration;

use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, Schema, SchemaBuilder, TableSchema, Value,
    permissions, policy_expr as pe,
};
use jazz_server::JazzServer;
use support::{
    collect_stream_deltas, connect_ready_client, connect_ready_user, has_added_id, has_removed,
    has_row, lacks_row, wait_for_edge_txs, wait_for_query, wait_for_rows,
    wait_for_subscription_update,
};
use uuid::Uuid;

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(100);

fn test_user_id(subject: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, subject.as_bytes()).to_string()
}

/// Teams carry two array columns: `member_ids` (session membership) and
/// `project_ids` (`Array<Uuid>` reference to projects). Projects are readable
/// only in reverse through a team row that the session can read.
fn team_project_schema(index_only_name: bool) -> Schema {
    let member_policy = pe::contains("member_ids", pe::session(vec!["claims", "sub"]));
    let teams_policies = permissions(|p| {
        p.allow_read().where_(member_policy);
        p.allow_insert().always();
        p.allow_update().always();
        p.allow_delete().always();
    });
    let projects_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::allowed_to_read_referencing("teams", "project_ids"));
        p.allow_insert().always();
        p.allow_update().always();
        p.allow_delete().always();
    });

    let mut teams = TableSchema::builder("teams")
        .column("name", ColumnType::Text)
        .column(
            "member_ids",
            ColumnType::Array {
                element: Box::new(ColumnType::Text),
            },
        )
        .array_fk_column("project_ids", "projects")
        .policies(teams_policies);
    if index_only_name {
        teams = teams.index_only(["name"]);
    }

    SchemaBuilder::new()
        .table(
            TableSchema::builder("projects")
                .column("title", ColumnType::Text)
                .policies(projects_policies),
        )
        .table(teams)
        .build()
}

async fn create_project(admin: &JazzClient, title: &str) -> ObjectId {
    let (id, _, transaction_id) = admin
        .insert("projects", row_input!("title" => title))
        .expect("insert project");
    wait_for_edge_txs(
        admin,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;
    id
}

async fn create_team(
    admin: &JazzClient,
    name: &str,
    member_ids: &[&str],
    project_ids: &[ObjectId],
) -> ObjectId {
    let (id, _, transaction_id) = admin
        .insert(
            "teams",
            row_input!(
                "name" => name,
                "member_ids" => Value::Array(member_ids.iter().map(|id| (*id).into()).collect()),
                "project_ids" => Value::Array(project_ids.iter().copied().map(Value::Uuid).collect()),
            ),
        )
        .expect("insert team");
    wait_for_edge_txs(
        admin,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;
    id
}

async fn set_team_projects(admin: &JazzClient, team_id: ObjectId, project_ids: &[ObjectId]) {
    let transaction_id = admin
        .update(
            team_id,
            vec![(
                "project_ids".to_string(),
                Value::Array(project_ids.iter().copied().map(Value::Uuid).collect()),
            )],
        )
        .expect("update team project_ids")
        .expect("ordinary mutation commits immediately");
    wait_for_edge_txs(admin, &[transaction_id]).await;
}

async fn assert_alice_granted_and_mallory_denied(
    server: &JazzServer,
    schema: &Schema,
    granted_project: ObjectId,
    hidden_project: ObjectId,
) {
    let alice = connect_ready_user(
        server,
        schema,
        &test_user_id("alice"),
        "projects",
        READY_TIMEOUT,
    )
    .await;
    let mallory = connect_ready_user(
        server,
        schema,
        &test_user_id("mallory"),
        "projects",
        READY_TIMEOUT,
    )
    .await;

    let query = jazz::query::Query::from("projects");
    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees the project referenced by her team",
        |rows| {
            (has_row(&rows, granted_project, &[Value::Text("Atlas".to_string())])
                && lacks_row(&rows, hidden_project))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(alice_rows.len(), 1);

    let mallory_rows = wait_for_query(
        &mallory,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(3),
        "mallory sees no projects without team membership",
        Some,
    )
    .await;
    assert!(
        mallory_rows.is_empty(),
        "mallory must not gain a project grant through a team she is not listed on: {mallory_rows:?}"
    );

    alice.shutdown().await.expect("shutdown alice");
    mallory.shutdown().await.expect("shutdown mallory");
}

/// A reverse-reference read grant resolves through an `Array<Uuid>` reference
/// column: alice may read a project only because a team row both lists her in
/// `member_ids` and lists the project's id in `project_ids`. Mallory, absent
/// from `member_ids`, is denied; an unreferenced project stays hidden for both.
///
/// ```text
/// admin ──insert project Atlas / Zephyr───────► server
/// admin ──insert team(members=[alice], projects=[Atlas])──► server
/// alice ──query projects──► team grants Atlas ──► sees Atlas only
/// mallory ──query projects──► no readable team ──✗ empty
/// ```
#[tokio::test]
async fn array_reference_membership_grants_read() {
    tokio::task::LocalSet::new()
        .run_until(array_reference_membership_grants_read_inner())
        .await;
}

async fn array_reference_membership_grants_read_inner() {
    let schema = team_project_schema(false);
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "projects", READY_TIMEOUT).await;

    let atlas = create_project(&admin, "Atlas").await;
    let zephyr = create_project(&admin, "Zephyr").await;
    create_team(&admin, "Core Team", &[&test_user_id("alice")], &[atlas]).await;

    assert_alice_granted_and_mallory_denied(&server, &schema, atlas, zephyr).await;

    admin.shutdown().await.expect("shutdown admin");
    server.shutdown().await;
}

/// Grants derived from `Array<Uuid>` reference elements are maintained
/// incrementally: removing the project id from the team's array revokes the
/// project from alice's live subscription, and re-adding it restores the row.
///
/// ```text
/// admin ──team(projects=[Atlas])──► server ──► alice subscription shows Atlas
/// admin ──team(projects=[])───────► server ──► alice subscription removes Atlas
/// admin ──team(projects=[Atlas])──► server ──► alice subscription re-adds Atlas
/// ```
#[tokio::test]
async fn array_reference_grant_updates_incrementally() {
    tokio::task::LocalSet::new()
        .run_until(array_reference_grant_updates_incrementally_inner())
        .await;
}

async fn array_reference_grant_updates_incrementally_inner() {
    let schema = team_project_schema(false);
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "projects", READY_TIMEOUT).await;
    let alice = connect_ready_user(
        &server,
        &schema,
        &test_user_id("alice"),
        "projects",
        READY_TIMEOUT,
    )
    .await;

    let atlas = create_project(&admin, "Atlas").await;
    let team_id = create_team(&admin, "Core Team", &[&test_user_id("alice")], &[atlas]).await;

    let query = jazz::query::Query::from("projects");
    wait_for_rows(
        &alice,
        query.clone(),
        "alice sees the project before the array is edited",
        |rows| has_row(&rows, atlas, &[Value::Text("Atlas".to_string())]).then_some(()),
    )
    .await;

    let mut stream = alice
        .subscribe(query.clone())
        .await
        .expect("subscribe projects");
    let mut log = Vec::new();
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    set_team_projects(&admin, team_id, &[]).await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "removing the project id from the team's array revokes the project",
        |entries| has_removed(entries, atlas),
    )
    .await;
    let rows_after_remove = wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "the project is hidden after its id leaves the team's array",
        |rows| lacks_row(&rows, atlas).then_some(rows),
    )
    .await;
    assert!(rows_after_remove.is_empty());

    log.clear();
    set_team_projects(&admin, team_id, &[atlas]).await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "re-adding the project id to the team's array restores the project",
        |entries| has_added_id(entries, atlas),
    )
    .await;
    wait_for_rows(
        &alice,
        query,
        "the project is visible again after its id returns to the team's array",
        |rows| has_row(&rows, atlas, &[Value::Text("Atlas".to_string())]).then_some(()),
    )
    .await;

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// The reverse-reference grant does not depend on a secondary index over the
/// array column: with `teams` indexing only `name`, membership and the
/// `Array<Uuid>` reference still grant alice the project and deny mallory.
///
/// ```text
/// admin ──insert team(members=[alice], projects=[Atlas]), arrays unindexed──► server
/// alice ──query projects──► unindexed resolution still grants ──► sees Atlas
/// mallory ──query projects──────────────────────────────────────✗ empty
/// ```
#[tokio::test]
async fn unindexed_array_reference_still_grants() {
    tokio::task::LocalSet::new()
        .run_until(unindexed_array_reference_still_grants_inner())
        .await;
}

async fn unindexed_array_reference_still_grants_inner() {
    let schema = team_project_schema(true);
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let admin = connect_ready_client(&server, &schema, "admin", "projects", READY_TIMEOUT).await;

    let atlas = create_project(&admin, "Atlas").await;
    let zephyr = create_project(&admin, "Zephyr").await;
    create_team(&admin, "Core Team", &[&test_user_id("alice")], &[atlas]).await;

    assert_alice_granted_and_mallory_denied(&server, &schema, atlas, zephyr).await;

    admin.shutdown().await.expect("shutdown admin");
    server.shutdown().await;
}
