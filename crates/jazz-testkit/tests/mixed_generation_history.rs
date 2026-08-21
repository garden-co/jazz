//! Rows whose history spans several schema generations must converge.
//!
//! The migration between the two generations drops two nullable text columns
//! and adds two others, so the same row accumulates writes whose
//! variable-length column sets differ. A reader on the current schema must
//! select the newest write in the shared physical lineage and translate it
//! into the requested schema; the row must never be dropped because its
//! history cannot be decoded under a single descriptor.

use jazz_testkit as support;

use std::time::Duration;

use jazz::row_input;
use jazz::tools::public_schema::SchemaHash;
use jazz::tools::schema_lens::{Lens, LensOp, LensTransform};
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, Schema, SchemaBuilder, TableSchema, Value,
};
use jazz_server::JazzServer;
use reqwest::StatusCode;
use serde_json::json;
use support::{
    TestingClient, has_added, publish_allow_all_permissions, push_catalogue_in_memory,
    wait_for_query, wait_for_subscription_update,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

/// Generation v1: two nullable variable-length columns that v2 drops.
fn tasks_schema_v1() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("tasks")
                .column("project_id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("legacy_tags", ColumnType::Text)
                .nullable_column("legacy_labels", ColumnType::Text),
        )
        .build()
}

/// Generation v2: `legacy_tags`/`legacy_labels` are gone and `tags`/`labels`
/// exist instead, so the variable-length column set differs from v1 in both
/// membership and position.
fn tasks_schema_v2() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("tasks")
                .column("project_id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("tags", ColumnType::Text)
                .nullable_column("labels", ColumnType::Text),
        )
        .build()
}

fn v1_to_v2_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&tasks_schema_v1()),
        SchemaHash::compute(&tasks_schema_v2()),
        LensTransform::with_ops(vec![
            LensOp::RemoveColumn {
                table: "tasks".to_string(),
                column: "legacy_tags".to_string(),
                column_type: ColumnType::Text,
                default: Value::Null,
            },
            LensOp::RemoveColumn {
                table: "tasks".to_string(),
                column: "legacy_labels".to_string(),
                column_type: ColumnType::Text,
                default: Value::Null,
            },
            LensOp::AddColumn {
                table: "tasks".to_string(),
                column: "tags".to_string(),
                column_type: ColumnType::Text,
                default: Value::Null,
            },
            LensOp::AddColumn {
                table: "tasks".to_string(),
                column: "labels".to_string(),
                column_type: ColumnType::Text,
                default: Value::Null,
            },
        ]),
    )
}

/// Push the given schemas and lenses into the server catalogue.
async fn push_catalogue(server: &JazzServer, schemas: &[Schema], lenses: &[Lens]) {
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        schemas,
        lenses,
    )
    .await
    .expect("push catalogue");
}

/// Publishing a permissions head for `schema` is the public operation that
/// moves the server's current write pointer to that generation.
async fn activate_generation(server: &JazzServer, schema: &Schema) {
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        schema,
    )
    .await;
}

async fn connect_ready(server: &JazzServer, schema: Schema, user: &str) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(schema)
        .with_user_id(user)
        .ready_on("tasks", READY_TIMEOUT)
        .connect()
        .await
}

/// Insert alice's v1-era task and settle it at the edge. Returns the row id
/// and its project reference.
async fn insert_v1_task(alice: &JazzClient) -> (ObjectId, ObjectId) {
    let project_id = ObjectId::new();
    let (row_id, _, transaction_id) = alice
        .insert(
            "tasks",
            row_input!(
                "project_id" => project_id,
                "name" => "task-a",
                "legacy_tags" => "tag-a",
                "legacy_labels" => "label-a"
            ),
        )
        .expect("alice creates v1 task");
    support::wait_for_edge_txs(
        alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;
    (row_id, project_id)
}

/// Update the task's v2-only columns and settle the write at the edge.
async fn update_task_v2(bob: &JazzClient, row_id: ObjectId) {
    let transaction_id = bob
        .update(
            row_id,
            vec![
                ("tags".to_string(), Value::Text("sess-new".to_string())),
                ("labels".to_string(), Value::Text("label-new".to_string())),
            ],
        )
        .expect("bob updates the task under v2");
    support::wait_for_edge_txs(
        bob,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;
}

/// The v2 row shape after bob's update: v1-era `project_id`/`name` survive and
/// the v2-era `tags`/`labels` carry bob's values.
fn converged_v2_values(project_id: ObjectId) -> Vec<Value> {
    vec![
        Value::Uuid(project_id),
        Value::Text("task-a".to_string()),
        Value::Text("sess-new".to_string()),
        Value::Text("label-new".to_string()),
    ]
}

/// A row that accumulated writes under v1 and, after the migration became the
/// active write schema, under v2 must converge to the newest (v2-era) write on
/// a current-schema read, with the surviving v1-era columns translated
/// correctly instead of the row being dropped.
///
/// Actors: alice authors under v1, bob updates the same row under v2.
///
/// ```text
/// admin ──activate v1──► server ◄──insert (legacy columns)── alice (v1)
/// admin ──activate v2──► server ◄──update tags/labels── bob (v2)
/// bob (v2) ──query──► newest state: tags = "sess-new", name/project kept
/// ```
#[tokio::test]
async fn newest_write_wins_when_row_history_spans_variable_column_generations() {
    tokio::task::LocalSet::new()
        .run_until(newest_write_wins_when_row_history_spans_variable_column_generations_impl())
        .await
}

async fn newest_write_wins_when_row_history_spans_variable_column_generations_impl() {
    let server = JazzServer::start().await;
    push_catalogue(
        &server,
        &[tasks_schema_v1(), tasks_schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await;
    activate_generation(&server, &tasks_schema_v1()).await;

    let alice = connect_ready(&server, tasks_schema_v1(), "alice-mixed-history").await;
    let (row_id, project_id) = insert_v1_task(&alice).await;

    activate_generation(&server, &tasks_schema_v2()).await;

    let bob = connect_ready(&server, tasks_schema_v2(), "bob-mixed-history").await;
    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the v1-era task before updating it",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;

    update_task_v2(&bob, row_id).await;

    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "the mixed-generation row converges to the newest write",
        |rows| {
            (rows.len() == 1 && rows[0].0 == row_id && rows[0].1 == converged_v2_values(project_id))
                .then_some(rows)
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// A fresh client whose local store never saw the v1 era must still converge
/// the row: the sync-time rebuild of a history that spans both generations
/// may not drop the batch that carries the newest write.
///
/// Actors: alice authors under v1, bob updates under v2, carol connects cold
/// on v2 only after both writes settled.
///
/// ```text
/// alice (v1) ──insert──► server ◄──update── bob (v2)
///                            │
///        carol (v2, empty store) ──cold sync──► converged newest row
/// ```
#[tokio::test]
async fn cold_client_converges_row_with_mixed_generation_history() {
    tokio::task::LocalSet::new()
        .run_until(cold_client_converges_row_with_mixed_generation_history_impl())
        .await
}

async fn cold_client_converges_row_with_mixed_generation_history_impl() {
    let server = JazzServer::start().await;
    push_catalogue(
        &server,
        &[tasks_schema_v1(), tasks_schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await;
    activate_generation(&server, &tasks_schema_v1()).await;

    let alice = connect_ready(&server, tasks_schema_v1(), "alice-cold-history").await;
    let (row_id, project_id) = insert_v1_task(&alice).await;

    activate_generation(&server, &tasks_schema_v2()).await;

    let bob = connect_ready(&server, tasks_schema_v2(), "bob-cold-history").await;
    update_task_v2(&bob, row_id).await;

    // Carol connects only after both generations' writes are durable, so her
    // first sight of the row is the sync-time rebuild of its full history.
    let carol = connect_ready(&server, tasks_schema_v2(), "carol-cold-history").await;
    wait_for_query(
        &carol,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "a cold client converges the mixed-generation row",
        |rows| {
            (rows.len() == 1 && rows[0].0 == row_id && rows[0].1 == converged_v2_values(project_id))
                .then_some(rows)
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    carol.shutdown().await.expect("shutdown carol");
    server.shutdown().await;
}

/// A client still authoring under v1 after v2 is already the active write
/// schema must not have its late write rejected or rewritten; every client
/// converges, and a subsequent v2 update plus its durability round-trip keeps
/// both the update and the v1-era column values.
///
/// Actors: alice keeps writing on v1 after the pointer moved, bob reads and
/// then updates on v2.
///
/// ```text
/// alice (v1) ──insert──► server
/// admin ──activate v2──► server (write pointer now v2)
/// alice (v1) ──late rename──► server ──► bob (v2) sees the rename
/// bob (v2) ──update tags──► server ──fate──► both values survive
/// ```
#[tokio::test]
async fn late_write_under_prior_generation_converges_with_current_schema_update() {
    tokio::task::LocalSet::new()
        .run_until(late_write_under_prior_generation_converges_with_current_schema_update_impl())
        .await
}

async fn late_write_under_prior_generation_converges_with_current_schema_update_impl() {
    let server = JazzServer::start().await;
    push_catalogue(
        &server,
        &[tasks_schema_v1(), tasks_schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await;
    activate_generation(&server, &tasks_schema_v1()).await;

    let alice = connect_ready(&server, tasks_schema_v1(), "alice-late-writer").await;
    let (row_id, project_id) = insert_v1_task(&alice).await;

    activate_generation(&server, &tasks_schema_v2()).await;

    // The late write: alice's client still authors under v1 although the
    // active write schema is already v2.
    let transaction_id = alice
        .update(
            row_id,
            vec![(
                "name".to_string(),
                Value::Text("renamed-under-v1".to_string()),
            )],
        )
        .expect("alice's late v1 write must be accepted");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let bob = connect_ready(&server, tasks_schema_v2(), "bob-late-writer").await;
    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees alice's late v1 rename translated into v2",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == row_id
                && rows[0].1[1] == Value::Text("renamed-under-v1".to_string()))
            .then_some(rows)
        },
    )
    .await;

    update_task_v2(&bob, row_id).await;

    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "the v2 update and the late v1-era values both survive the fate round-trip",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == row_id
                && rows[0].1[0] == Value::Uuid(project_id)
                && rows[0].1[1] == Value::Text("renamed-under-v1".to_string())
                && rows[0].1[2] == Value::Text("sess-new".to_string())
                && rows[0].1[3] == Value::Text("label-new".to_string()))
            .then_some(rows)
        },
    )
    .await;

    // Alice's v1 view of the same row must also converge on the rename.
    wait_for_query(
        &alice,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice's v1 view converges on the renamed row",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == row_id
                && rows[0].1[0] == Value::Uuid(project_id)
                && rows[0].1[1] == Value::Text("renamed-under-v1".to_string()))
            .then_some(rows)
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// One-shot reads, a fresh subscription's first delivery, and a warm restart
/// from persistent storage must all agree on the newest state of a row whose
/// history spans both generations; no read path may resurface the stale
/// prior-generation copy.
///
/// Actors: alice authors under v1, bob updates under v2 and checks the
/// one-shot and subscription paths, dave holds a persistent store that he
/// reopens.
///
/// ```text
/// alice (v1) ──insert──► server ◄──update tags/labels── bob (v2)
/// bob ──one-shot query──► newest state
/// bob ──subscribe──► first delivery carries newest state
/// dave (v2, persistent) ──query──► newest state
/// dave ──shutdown / reopen store──► query still resolves newest state
/// ```
#[tokio::test]
async fn read_paths_agree_on_newest_state_after_mixed_generation_writes() {
    tokio::task::LocalSet::new()
        .run_until(read_paths_agree_on_newest_state_after_mixed_generation_writes_impl())
        .await
}

async fn read_paths_agree_on_newest_state_after_mixed_generation_writes_impl() {
    let server = JazzServer::start().await;
    push_catalogue(
        &server,
        &[tasks_schema_v1(), tasks_schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await;
    activate_generation(&server, &tasks_schema_v1()).await;

    let alice = connect_ready(&server, tasks_schema_v1(), "alice-read-paths").await;
    let (row_id, project_id) = insert_v1_task(&alice).await;

    activate_generation(&server, &tasks_schema_v2()).await;

    let bob = connect_ready(&server, tasks_schema_v2(), "bob-read-paths").await;
    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the v1-era task before updating it",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;

    update_task_v2(&bob, row_id).await;

    // Read path 1: the one-shot query resolves the newest state.
    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "one-shot read resolves the newest mixed-generation state",
        |rows| {
            (rows.len() == 1 && rows[0].0 == row_id && rows[0].1 == converged_v2_values(project_id))
                .then_some(rows)
        },
    )
    .await;

    // Read path 2: a fresh subscription's first delivery carries the newest
    // state, not the stale v1-era copy.
    let mut stream = bob
        .subscribe(jazz::query::Query::from("tasks"))
        .await
        .expect("bob subscribes to tasks");
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "subscription hydrates with the newest mixed-generation state",
        |log| {
            has_added(
                log,
                &[
                    ("project_id", Value::Uuid(project_id)),
                    ("name", Value::Text("task-a".to_string())),
                    ("tags", Value::Text("sess-new".to_string())),
                    ("labels", Value::Text("label-new".to_string())),
                ],
            )
        },
    )
    .await;
    drop(stream);

    // Read path 3: a persistent store that synced the converged row must
    // replay to the same newest state after a warm restart.
    let (dave_context, dave) = TestingClient::builder()
        .with_server(&server)
        .with_schema(tasks_schema_v2())
        .with_user_id("dave-read-paths")
        .with_persistent_storage()
        .ready_on("tasks", READY_TIMEOUT)
        .connect_with_context()
        .await;
    wait_for_query(
        &dave,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "the persistent client syncs the newest mixed-generation state",
        |rows| {
            (rows.len() == 1 && rows[0].0 == row_id && rows[0].1 == converged_v2_values(project_id))
                .then_some(rows)
        },
    )
    .await;
    dave.shutdown().await.expect("shutdown dave");
    let reopened = jazz_testkit::connect(dave_context)
        .await
        .expect("reopen dave's persistent client");
    wait_for_query(
        &reopened,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "warm restart resolves the newest mixed-generation state",
        |rows| {
            (rows.len() == 1 && rows[0].0 == row_id && rows[0].1 == converged_v2_values(project_id))
                .then_some(rows)
        },
    )
    .await;

    reopened.shutdown().await.expect("shutdown reopened dave");
    bob.shutdown().await.expect("shutdown bob");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// The same convergence contract when the v2 bundle is published only AFTER
/// the v1-era write, against a server that is already serving: the newest
/// (v2-era) write to the mixed-generation row must still converge for a
/// current-schema reader.
///
/// Actors: alice authors under v1 before the migration exists, bob updates
/// under v2 after the runtime publication.
///
/// ```text
/// admin ──publish v1──► server ◄──insert (legacy columns)── alice (v1)
/// admin ──publish v2 bundle at runtime──► server
/// bob (v2) ──update tags/labels──► server ──► newest state converges
/// ```
#[tokio::test]
#[ignore = "after a runtime-published v1->v2 catalogue bundle the serving edge rejects the current-schema table shape with UnsupportedShapeCapability (gap: Source(SchemaProjection)), so post-migration writes never settle at the edge"]
async fn migration_published_at_runtime_still_converges_mixed_generation_row() {
    tokio::task::LocalSet::new()
        .run_until(migration_published_at_runtime_still_converges_mixed_generation_row_impl())
        .await
}

async fn migration_published_at_runtime_still_converges_mixed_generation_row_impl() {
    let server = JazzServer::start().await;
    push_catalogue(&server, &[tasks_schema_v1()], &[]).await;
    activate_generation(&server, &tasks_schema_v1()).await;

    let alice = connect_ready(&server, tasks_schema_v1(), "alice-runtime-migration").await;
    let (row_id, project_id) = insert_v1_task(&alice).await;

    // The migration bundle arrives only now, while the server keeps serving.
    push_catalogue(
        &server,
        &[tasks_schema_v1(), tasks_schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await;
    activate_generation(&server, &tasks_schema_v2()).await;

    let bob = connect_ready(&server, tasks_schema_v2(), "bob-runtime-migration").await;
    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the v1-era task before updating it",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;

    update_task_v2(&bob, row_id).await;

    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "the runtime-migrated row converges to the newest write",
        |rows| {
            (rows.len() == 1 && rows[0].0 == row_id && rows[0].1 == converged_v2_values(project_id))
                .then_some(rows)
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// A schema published without its lineage-defining migration is a draft: it
/// is staged but never activated. Such a draft must not disturb reads or
/// writes on the active generation. (A torn or partial catalogue entry is
/// not representable through public APIs: catalogue admission is an atomic
/// dense-sequence bundle, so the draft is the closest publicly reachable
/// staged-never-activated state.)
///
/// Actors: alice reads and writes on v1 while an admin publishes a v2 draft.
///
/// ```text
/// admin ──publish v1──► server ◄──insert row1── alice (v1)
/// admin ──publish v2 schema only (draft)──► server
/// alice (v1) ──insert row2──► server ──► subscription + query still serve v1
/// ```
#[tokio::test]
async fn draft_schema_without_lineage_does_not_affect_active_generation_reads() {
    tokio::task::LocalSet::new()
        .run_until(draft_schema_without_lineage_does_not_affect_active_generation_reads_impl())
        .await
}

async fn draft_schema_without_lineage_does_not_affect_active_generation_reads_impl() {
    let server = JazzServer::start().await;
    push_catalogue(&server, &[tasks_schema_v1()], &[]).await;
    activate_generation(&server, &tasks_schema_v1()).await;

    let alice = connect_ready(&server, tasks_schema_v1(), "alice-draft-schema").await;
    let project_id = ObjectId::new();
    let (first_row_id, first_values, transaction_id) = alice
        .insert(
            "tasks",
            row_input!(
                "project_id" => project_id,
                "name" => "task-before-draft",
                "legacy_tags" => "tag-a",
                "legacy_labels" => "label-a"
            ),
        )
        .expect("alice creates a task before the draft publish");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    let mut stream = alice
        .subscribe(jazz::query::Query::from("tasks"))
        .await
        .expect("alice subscribes to tasks");
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "alice's subscription hydrates before the draft publish",
        |log| {
            log.iter()
                .any(|delta| delta.added.iter().any(|change| change.id == first_row_id))
        },
    )
    .await;

    // Publish only the v2 schema, never its v1 -> v2 migration: it stays a
    // draft with no active lineage.
    let response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/schemas",
            server.base_url(),
            server.app_id()
        ))
        .header("X-Jazz-Admin-Secret", server.admin_secret())
        .json(&json!({ "schema": tasks_schema_v2() }))
        .send()
        .await
        .expect("publish v2 draft schema");
    assert_eq!(response.status(), StatusCode::CREATED);

    let (second_row_id, second_values, transaction_id) = alice
        .insert(
            "tasks",
            row_input!(
                "project_id" => project_id,
                "name" => "task-after-draft",
                "legacy_tags" => "tag-b",
                "legacy_labels" => "label-b"
            ),
        )
        .expect("alice keeps writing while the draft exists");
    support::wait_for_edge_txs(
        &alice,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "alice's live subscription still delivers new v1 writes",
        |log| {
            log.iter()
                .any(|delta| delta.added.iter().any(|change| change.id == second_row_id))
        },
    )
    .await;

    wait_for_query(
        &alice,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "v1 queries keep serving both rows despite the staged draft",
        |rows| {
            (rows.len() == 2
                && rows
                    .iter()
                    .any(|(id, values)| *id == first_row_id && *values == first_values)
                && rows
                    .iter()
                    .any(|(id, values)| *id == second_row_id && *values == second_values))
            .then_some(rows)
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// A current-schema update touching only ONE of the lens-added columns must
/// leave the row readable: the untouched added column keeps its lens default
/// (null) instead of the whole row erroring out of every v2 read.
///
/// Actors: alice authors under v1, bob updates only `tags` under v2.
///
/// ```text
/// alice (v1) ──insert──► server ◄──update tags only── bob (v2)
/// bob (v2) ──query──► tags = "sess-new", labels = null, v1 columns kept
/// ```
#[tokio::test]
#[ignore = "a v2 update touching only one lens-added column leaves the other added column with no value, so every v2 read of the row fails with 'row missing projected value for column labels'"]
async fn partial_current_schema_update_keeps_untouched_added_column_readable() {
    tokio::task::LocalSet::new()
        .run_until(partial_current_schema_update_keeps_untouched_added_column_readable_impl())
        .await
}

async fn partial_current_schema_update_keeps_untouched_added_column_readable_impl() {
    let server = JazzServer::start().await;
    push_catalogue(
        &server,
        &[tasks_schema_v1(), tasks_schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await;
    activate_generation(&server, &tasks_schema_v1()).await;

    let alice = connect_ready(&server, tasks_schema_v1(), "alice-partial-update").await;
    let (row_id, project_id) = insert_v1_task(&alice).await;

    activate_generation(&server, &tasks_schema_v2()).await;

    let bob = connect_ready(&server, tasks_schema_v2(), "bob-partial-update").await;
    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the v1-era task before updating it",
        |rows| (rows.len() == 1 && rows[0].0 == row_id).then_some(rows),
    )
    .await;

    let transaction_id = bob
        .update(
            row_id,
            vec![("tags".to_string(), Value::Text("sess-new".to_string()))],
        )
        .expect("bob updates only the tags column");
    support::wait_for_edge_txs(
        &bob,
        &[transaction_id.expect("ordinary mutation commits immediately")],
    )
    .await;

    wait_for_query(
        &bob,
        jazz::query::Query::from("tasks"),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "the partially updated row stays readable with the lens default",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == row_id
                && rows[0].1
                    == vec![
                        Value::Uuid(project_id),
                        Value::Text("task-a".to_string()),
                        Value::Text("sess-new".to_string()),
                        Value::Null,
                    ])
            .then_some(rows)
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
