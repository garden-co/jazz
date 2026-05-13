#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::query_manager::types::SchemaHash;
use jazz_tools::server::TestingServer;
use jazz_tools::sync_manager::SyncPayload;
use jazz_tools::sync_tracer::SyncTracer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use reqwest::StatusCode;
use serde_json::json;
use support::{
    TestingClient, deny_all_select_permissions, has_added, has_removed,
    publish_allow_all_permissions, publish_permissions, wait_for, wait_for_query,
    wait_for_subscription_update,
};
use tempfile::TempDir;

fn todo_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("todos").column("title", ColumnType::Text))
        .build()
}

fn todo_schema_with_notes() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .nullable_column("notes", ColumnType::Text),
        )
        .build()
}

const PEER_SECRET: &str = "cluster-peer-secret";
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(10);
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(30);

async fn publish_schema_to_core(core: &TestingServer, schema: &jazz_tools::Schema) {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/schemas",
            core.base_url(),
            core.app_id()
        ))
        .header("X-Jazz-Admin-Secret", core.admin_secret())
        .json(&json!({ "schema": schema, "permissions": null }))
        .send()
        .await
        .expect("publish schema to core");
    let status = response.status();
    if status != StatusCode::CREATED {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable response body>".to_string());
        panic!("schema publish to core failed: {status} {body}");
    }
}

async fn wait_for_schema_hash(server: &TestingServer, schema: &jazz_tools::Schema) {
    let expected_hash = SchemaHash::compute(schema);

    wait_for(
        REPLICATION_TIMEOUT,
        format!("schema hash {expected_hash} to reach {}", server.base_url()),
        || async {
            server
                .server_state()
                .runtime
                .known_schema_hashes()
                .ok()?
                .contains(&expected_hash)
                .then_some(())
        },
    )
    .await;
}

async fn wait_for_permissions_head(server: &TestingServer, expected_bundle_object_id: &str) {
    wait_for(
        REPLICATION_TIMEOUT,
        format!(
            "permissions head {expected_bundle_object_id} to reach {}",
            server.base_url()
        ),
        || async move {
            let head = server
                .server_state()
                .runtime
                .current_permissions_head()
                .ok()??;

            (head.bundle_object_id.to_string() == expected_bundle_object_id).then_some(())
        },
    )
    .await;
}

async fn wait_for_upstream(edge: &TestingServer) {
    tokio::time::timeout(
        UPSTREAM_TIMEOUT,
        edge.server_state().runtime.transport_wait_until_connected(),
    )
    .await
    .expect("edge should connect to upstream before timeout")
    .then_some(())
    .expect("edge transport should report connected");
}

fn app_scoped_ws_upstream_url(server: &TestingServer) -> String {
    format!(
        "ws://127.0.0.1:{}/apps/{}/ws",
        server.port(),
        server.app_id()
    )
}

struct MultiServerCluster {
    schema: jazz_tools::Schema,
    core: TestingServer,
    edge_us: TestingServer,
    edge_eu: TestingServer,
}

impl MultiServerCluster {
    async fn start() -> Self {
        Self::start_with_tracer(None).await
    }

    async fn start_dynamic(schema: jazz_tools::Schema) -> Self {
        let app_id = TestingServer::default_app_id();

        let core = TestingServer::builder()
            .with_app_id(app_id)
            .with_peer_secret(PEER_SECRET)
            .start()
            .await;
        let edge_us = TestingServer::builder()
            .with_app_id(app_id)
            .with_peer_secret(PEER_SECRET)
            .with_upstream_url(core.base_url())
            .start()
            .await;
        let edge_eu = TestingServer::builder()
            .with_app_id(app_id)
            .with_peer_secret(PEER_SECRET)
            .with_upstream_url(core.base_url())
            .start()
            .await;

        wait_for_upstream(&edge_us).await;
        wait_for_upstream(&edge_eu).await;

        Self {
            schema,
            core,
            edge_us,
            edge_eu,
        }
    }

    async fn start_with_tracer(tracer: Option<SyncTracer>) -> Self {
        let schema = todo_schema();
        let app_id = TestingServer::default_app_id();

        let mut core_builder = TestingServer::builder()
            .with_app_id(app_id)
            .with_schema(schema.clone())
            .with_peer_secret(PEER_SECRET);
        if let Some(tracer) = tracer.clone() {
            core_builder = core_builder.with_tracer(tracer);
        }
        let core = core_builder.start().await;

        let mut edge_us_builder = TestingServer::builder()
            .with_app_id(app_id)
            .with_schema(schema.clone())
            .with_peer_secret(PEER_SECRET)
            .with_upstream_url(core.base_url());
        if let Some(tracer) = tracer.clone() {
            edge_us_builder = edge_us_builder.with_tracer(tracer);
        }
        let edge_us = edge_us_builder.start().await;

        let mut edge_eu_builder = TestingServer::builder()
            .with_app_id(app_id)
            .with_schema(schema.clone())
            .with_peer_secret(PEER_SECRET)
            .with_upstream_url(core.base_url());
        if let Some(tracer) = tracer {
            edge_eu_builder = edge_eu_builder.with_tracer(tracer);
        }
        let edge_eu = edge_eu_builder.start().await;

        wait_for_upstream(&edge_us).await;
        wait_for_upstream(&edge_eu).await;

        Self {
            schema,
            core,
            edge_us,
            edge_eu,
        }
    }

    async fn connect_user(
        &self,
        server: &TestingServer,
        user_id: &str,
        tracer: Option<&SyncTracer>,
    ) -> JazzClient {
        let mut client = TestingClient::builder()
            .with_server(server)
            .with_schema(self.schema.clone())
            .with_user_id(user_id)
            .ready_on("todos", READY_TIMEOUT);

        if let Some(tracer) = tracer {
            client = client.with_tracer(tracer, user_id);
        }

        client.connect().await
    }

    async fn shutdown(self) {
        self.edge_eu.shutdown().await;
        self.edge_us.shutdown().await;
        self.core.shutdown().await;
    }
}

/// Alice writes through the US edge with GlobalServer durability, and Bob reads
/// the replicated row through the EU edge.
///
/// ```text
/// alice --GlobalServer write--> edge_us --sync--> core --sync--> edge_eu
/// bob   <--EdgeServer read--------------------------------------- edge_eu
/// ```
#[tokio::test]
async fn write_through_one_edge_replica_becomes_visible_through_another_edge() {
    let cluster = MultiServerCluster::start().await;

    let alice = cluster.connect_user(&cluster.edge_us, "alice", None).await;
    let bob = cluster.connect_user(&cluster.edge_eu, "bob", None).await;

    let (todo_id, _) = tokio::time::timeout(
        Duration::from_secs(20),
        alice.create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("global via edge".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        ),
    )
    .await
    .expect("global settlement through edge should complete before timeout")
    .expect("alice create should succeed");

    let rows = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees alice row through the other edge",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;

    assert_eq!(rows[0].1, vec![Value::Text("global via edge".to_string())]);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    cluster.shutdown().await;
}

/// Alice writes at EdgeServer durability through the US edge.
///
/// ```text
/// alice --EdgeServer write--> edge_us --sync--> core --sync--> edge_eu
/// bob   <--same-edge read---- edge_us
/// carol <--global read------- core
/// dave  <--peer-edge read---- edge_eu
/// ```
#[tokio::test]
async fn edge_tier_write_propagates_from_writer_edge_to_core_and_peer_edge() {
    let cluster = MultiServerCluster::start().await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-edge-us", None)
        .await;
    let bob = cluster
        .connect_user(&cluster.edge_us, "bob-edge-us", None)
        .await;
    let carol = cluster
        .connect_user(&cluster.core, "carol-core", None)
        .await;
    let dave = cluster
        .connect_user(&cluster.edge_eu, "dave-edge-eu", None)
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("edge-local then replicated".to_string()),
            )]),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("alice edge-tier create should settle on edge_us");

    let same_edge_rows = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "bob sees alice row on the same edge",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        same_edge_rows[0].1,
        vec![Value::Text("edge-local then replicated".to_string())]
    );

    let core_rows = wait_for_query(
        &carol,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "carol sees alice row after core settlement",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        core_rows[0].1,
        vec![Value::Text("edge-local then replicated".to_string())]
    );

    let peer_edge_rows = wait_for_query(
        &dave,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "dave sees alice row on the peer edge",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        peer_edge_rows[0].1,
        vec![Value::Text("edge-local then replicated".to_string())]
    );

    let writer_global_rows = wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "alice can ask edge_us for the globally settled row",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        writer_global_rows[0].1,
        vec![Value::Text("edge-local then replicated".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    carol.shutdown().await.expect("shutdown carol");
    dave.shutdown().await.expect("shutdown dave");
    cluster.shutdown().await;
}

/// Alice writes through an edge but requests GlobalServer durability.
///
/// ```text
/// alice --GlobalServer write--> edge_us --peer sync--> core
/// alice <--return only after global settlement----------
/// carol <--global read----------- core
/// dave  <--global read----------- edge_eu
/// ```
#[tokio::test]
async fn global_tier_write_through_edge_is_visible_at_global_tier_everywhere() {
    let cluster = MultiServerCluster::start().await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-global-writer", None)
        .await;
    let carol = cluster
        .connect_user(&cluster.core, "carol-core-reader", None)
        .await;
    let dave = cluster
        .connect_user(&cluster.edge_eu, "dave-global-reader", None)
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("edge write with global durability".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("global-tier create through edge_us should wait for core");

    let core_rows = wait_for_query(
        &carol,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "core sees global-tier write from edge_us",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        core_rows[0].1,
        vec![Value::Text("edge write with global durability".to_string())]
    );

    let writer_edge_global_rows = wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "writer edge can answer GlobalServer query after settlement",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        writer_edge_global_rows[0].1,
        vec![Value::Text("edge write with global durability".to_string())]
    );

    let peer_edge_global_rows = wait_for_query(
        &dave,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "peer edge can answer GlobalServer query after upstream settlement",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        peer_edge_global_rows[0].1,
        vec![Value::Text("edge write with global durability".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    carol.shutdown().await.expect("shutdown carol");
    dave.shutdown().await.expect("shutdown dave");
    cluster.shutdown().await;
}

/// Core-origin writes should flow down to active subscribers on both edges.
///
/// ```text
/// alice subscribes on edge_us
/// bob   subscribes on edge_eu
/// carol writes on core --sync--> edge_us --> alice
///                      `-sync--> edge_eu --> bob
/// ```
#[tokio::test]
async fn core_write_reaches_subscribed_clients_on_both_edges() {
    let cluster = MultiServerCluster::start().await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-subscriber-us", None)
        .await;
    let bob = cluster
        .connect_user(&cluster.edge_eu, "bob-subscriber-eu", None)
        .await;
    let carol = cluster
        .connect_user(&cluster.core, "carol-core-writer", None)
        .await;

    let query = QueryBuilder::new("todos").build();
    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("alice subscribes");
    let mut bob_stream = bob.subscribe(query.clone()).await.expect("bob subscribes");
    let mut alice_log = Vec::new();
    let mut bob_log = Vec::new();

    let (todo_id, _) = carol
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("core write to both edges".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("core global-tier create");

    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        REPLICATION_TIMEOUT,
        "alice subscription on edge_us receives core row",
        |log| has_added(log, todo_id),
    )
    .await;

    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        REPLICATION_TIMEOUT,
        "bob subscription on edge_eu receives core row",
        |log| has_added(log, todo_id),
    )
    .await;

    let alice_rows = wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "alice edge query includes core row",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![Value::Text("core write to both edges".to_string())]
    );

    let bob_rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "bob edge query includes core row",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        vec![Value::Text("core write to both edges".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    carol.shutdown().await.expect("shutdown carol");
    cluster.shutdown().await;
}

/// Schema and permissions are published only to the core. Both edges learn the
/// catalogue through peer sync before clients on either edge use it.
///
/// ```text
/// mallory --schema + permissions--> core
/// core    --catalogue sync--------> edge_us
/// core    --catalogue sync--------> edge_eu
/// alice   --write-----------------> edge_us --sync--> core --sync--> edge_eu --> bob
/// ```
#[tokio::test]
async fn core_schema_and_permissions_pushes_reach_every_edge_before_edge_clients_use_them() {
    let schema = todo_schema();
    let cluster = MultiServerCluster::start_dynamic(schema.clone()).await;
    let query = QueryBuilder::new("todos").build();

    publish_schema_to_core(&cluster.core, &schema).await;
    let permissions_head = publish_allow_all_permissions(
        &cluster.core.base_url(),
        cluster.core.app_id(),
        cluster.core.admin_secret(),
        &schema,
    )
    .await;

    wait_for_schema_hash(&cluster.edge_us, &schema).await;
    wait_for_schema_hash(&cluster.edge_eu, &schema).await;
    wait_for_permissions_head(&cluster.edge_us, &permissions_head.bundle_object_id).await;
    wait_for_permissions_head(&cluster.edge_eu, &permissions_head.bundle_object_id).await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-after-catalogue", None)
        .await;
    let bob = cluster
        .connect_user(&cluster.edge_eu, "bob-after-catalogue", None)
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("released after catalogue push".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("alice create should settle globally after catalogue reaches edge_us");

    let bob_rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "bob sees alice row after catalogue reaches edge_eu",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        vec![Value::Text("released after catalogue push".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    cluster.shutdown().await;
}

/// A fresh edge connects after the core already has schema and permissions.
/// The edge must pull catalogue from core before any edge client query exists.
///
/// ```text
/// mallory --schema + permissions--> core
/// edge_eu --peer reconnect--------> core
/// core    --full catalogue replay-> edge_eu
/// alice   --write-----------------> edge_eu --sync--> core
/// ```
#[tokio::test]
async fn fresh_edge_pulls_existing_core_catalogue_on_connect_without_client_query() {
    let schema = todo_schema();
    let app_id = TestingServer::default_app_id();
    let query = QueryBuilder::new("todos").build();

    let core = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .start()
        .await;

    publish_schema_to_core(&core, &schema).await;
    let permissions_head = publish_allow_all_permissions(
        &core.base_url(),
        core.app_id(),
        core.admin_secret(),
        &schema,
    )
    .await;

    let edge_eu = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .with_upstream_url(core.base_url())
        .start()
        .await;

    wait_for_upstream(&edge_eu).await;
    wait_for_schema_hash(&edge_eu, &schema).await;
    wait_for_permissions_head(&edge_eu, &permissions_head.bundle_object_id).await;

    let alice = TestingClient::builder()
        .with_server(&edge_eu)
        .with_schema(schema.clone())
        .with_user_id("alice-fresh-edge-catalogue")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("fresh edge catalogue pull".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("fresh edge should write after pulling core catalogue");

    let alice_rows = wait_for_query(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "alice sees row written after fresh edge catalogue pull",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![Value::Text("fresh edge catalogue pull".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    edge_eu.shutdown().await;
    core.shutdown().await;
}

/// Catalogue published through one edge is forwarded to core, then core
/// propagates it to another connected edge over peer sync.
///
/// ```text
/// mallory --schema + permissions--> edge_us --HTTP forward--> core
/// core    --catalogue sync----------------------------------> edge_eu
/// alice   --write-------------------------------------------> edge_eu
/// ```
#[tokio::test]
async fn edge_catalogue_publish_reaches_peer_edge_through_core_sync() {
    let schema = todo_schema();
    let app_id = TestingServer::default_app_id();
    let query = QueryBuilder::new("todos").build();

    let core = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .start()
        .await;
    let edge_us = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .with_upstream_url(core.base_url())
        .start()
        .await;
    let edge_eu = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .with_upstream_url(core.base_url())
        .start()
        .await;

    wait_for_upstream(&edge_us).await;
    wait_for_upstream(&edge_eu).await;

    let publish_response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/schemas",
            edge_us.base_url(),
            edge_us.app_id()
        ))
        .header("X-Jazz-Admin-Secret", edge_us.admin_secret())
        .json(&json!({ "schema": schema, "permissions": null }))
        .send()
        .await
        .expect("publish schema through edge_us");
    let status = publish_response.status();
    if status != StatusCode::CREATED {
        let body = publish_response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable response body>".to_string());
        panic!("schema publish through edge_us failed: {status} {body}");
    }

    let permissions_head = publish_allow_all_permissions(
        &edge_us.base_url(),
        edge_us.app_id(),
        edge_us.admin_secret(),
        &schema,
    )
    .await;

    wait_for_schema_hash(&edge_eu, &schema).await;
    wait_for_permissions_head(&edge_eu, &permissions_head.bundle_object_id).await;

    let alice = TestingClient::builder()
        .with_server(&edge_eu)
        .with_schema(schema.clone())
        .with_user_id("alice-peer-edge-catalogue")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("catalogue forwarded through core".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("peer edge should write after receiving forwarded catalogue");

    let alice_rows = wait_for_query(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "alice sees row written after peer edge receives forwarded catalogue",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![Value::Text("catalogue forwarded through core".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    edge_eu.shutdown().await;
    edge_us.shutdown().await;
    core.shutdown().await;
}

/// A WebSocket-style, app-scoped upstream URL should drive both peer sync and
/// HTTP catalogue forwarding.
///
/// ```text
/// edge --upstream-url ws://core/apps/<app>/ws-- peer sync + HTTP forwarding
/// mallory --schema + permissions--------------> edge --------> core
/// alice   --write/read------------------------> edge
/// ```
#[tokio::test]
async fn app_scoped_ws_upstream_url_forwards_and_reads_catalogue_through_edge() {
    let schema = todo_schema();
    let schema_hash = SchemaHash::compute(&schema).to_string();
    let app_id = TestingServer::default_app_id();
    let query = QueryBuilder::new("todos").build();
    let client = reqwest::Client::new();

    let core = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .start()
        .await;
    let edge = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .with_upstream_url(app_scoped_ws_upstream_url(&core))
        .start()
        .await;

    wait_for_upstream(&edge).await;

    let publish_schema_response = client
        .post(format!(
            "{}/apps/{}/admin/schemas",
            edge.base_url(),
            edge.app_id()
        ))
        .header("X-Jazz-Admin-Secret", edge.admin_secret())
        .json(&json!({ "schema": schema, "permissions": null }))
        .send()
        .await
        .expect("publish schema through edge configured with app-scoped ws upstream");
    assert_eq!(publish_schema_response.status(), StatusCode::CREATED);

    let permissions_head = publish_allow_all_permissions(
        &edge.base_url(),
        edge.app_id(),
        edge.admin_secret(),
        &schema,
    )
    .await;

    wait_for_schema_hash(&core, &schema).await;
    wait_for_schema_hash(&edge, &schema).await;
    wait_for_permissions_head(&core, &permissions_head.bundle_object_id).await;
    wait_for_permissions_head(&edge, &permissions_head.bundle_object_id).await;

    let edge_hashes_response = client
        .get(format!(
            "{}/apps/{}/schemas",
            edge.base_url(),
            edge.app_id()
        ))
        .header("X-Jazz-Admin-Secret", edge.admin_secret())
        .send()
        .await
        .expect("read schema catalogue through edge");
    assert_eq!(edge_hashes_response.status(), StatusCode::OK);
    let edge_hashes: serde_json::Value = edge_hashes_response
        .json()
        .await
        .expect("decode edge schema hashes");
    let hashes = edge_hashes
        .get("hashes")
        .and_then(serde_json::Value::as_array)
        .expect("schema hashes response should include hashes");
    assert!(
        hashes
            .iter()
            .any(|hash| hash.as_str() == Some(schema_hash.as_str())),
        "edge catalogue read should include schema published through ws-style upstream"
    );

    let alice = TestingClient::builder()
        .with_server(&edge)
        .with_schema(schema.clone())
        .with_user_id("alice-ws-upstream-catalogue")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("catalogue over app-scoped ws upstream".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("alice writes after catalogue is published through ws-style upstream");

    let alice_rows = wait_for_query(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "alice reads row through edge configured with app-scoped ws upstream",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![Value::Text(
            "catalogue over app-scoped ws upstream".to_string()
        )]
    );

    alice.shutdown().await.expect("shutdown alice");
    edge.shutdown().await;
    core.shutdown().await;
}

/// A persisted edge that was offline for a core catalogue update reconnects
/// with its older catalogue hash. The core should treat that hash as stale and
/// replay the missing catalogue before the first post-restart client write.
///
/// ```text
/// core: v1 catalogue ----sync----> edge (persistent data_dir)
/// edge stops
/// core: v2 catalogue + permissions
/// edge restarts with v1 hash --handshake--> core --catalogue replay--> edge
/// alice(v2) writes through restarted edge
/// ```
#[tokio::test]
async fn persisted_stale_edge_reconnect_replays_catalogue_before_client_work() {
    let app_id = TestingServer::default_app_id();
    let v1_schema = todo_schema();
    let v2_schema = todo_schema_with_notes();
    let edge_data_dir = TempDir::new().expect("temp edge data dir");
    let query = QueryBuilder::new("todos").build();

    let core = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .start()
        .await;

    publish_schema_to_core(&core, &v1_schema).await;
    let v1_permissions_head = publish_allow_all_permissions(
        &core.base_url(),
        core.app_id(),
        core.admin_secret(),
        &v1_schema,
    )
    .await;

    let edge_before_restart = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .with_upstream_url(core.base_url())
        .with_persistent_storage()
        .with_data_dir(edge_data_dir.path())
        .start()
        .await;

    wait_for_upstream(&edge_before_restart).await;
    wait_for_schema_hash(&edge_before_restart, &v1_schema).await;
    wait_for_permissions_head(&edge_before_restart, &v1_permissions_head.bundle_object_id).await;
    let stale_edge_catalogue_hash = edge_before_restart
        .server_state()
        .runtime
        .catalogue_state_hash()
        .expect("read edge v1 catalogue hash");

    edge_before_restart.shutdown().await;

    publish_schema_to_core(&core, &v2_schema).await;
    let v2_permissions_head = publish_allow_all_permissions(
        &core.base_url(),
        core.app_id(),
        core.admin_secret(),
        &v2_schema,
    )
    .await;
    let core_v2_catalogue_hash = core
        .server_state()
        .runtime
        .catalogue_state_hash()
        .expect("read core v2 catalogue hash");
    assert_ne!(
        stale_edge_catalogue_hash, core_v2_catalogue_hash,
        "edge should restart from an older persisted catalogue hash"
    );

    let edge_after_restart = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .with_upstream_url(core.base_url())
        .with_persistent_storage()
        .with_data_dir(edge_data_dir.path())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&edge_after_restart)
        .with_schema(v2_schema.clone())
        .with_user_id("alice-stale-edge-replay")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    wait_for_schema_hash(&edge_after_restart, &v2_schema).await;
    wait_for_permissions_head(&edge_after_restart, &v2_permissions_head.bundle_object_id).await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("write after stale catalogue replay".to_string()),
                ),
                (
                    "notes".to_string(),
                    Value::Text("v2 column available before work proceeds".to_string()),
                ),
            ]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("v2 write should settle after stale edge receives catalogue replay");

    let alice_rows = wait_for_query(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "alice sees v2 row after stale edge replay",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![
            Value::Text("write after stale catalogue replay".to_string()),
            Value::Text("v2 column available before work proceeds".to_string()),
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    edge_after_restart.shutdown().await;
    core.shutdown().await;
}

/// Permission retightening is published only to the core and must invalidate
/// active edge subscriptions on every edge.
///
/// ```text
/// mallory --schema + allow permissions--> core --sync--> edge_us + edge_eu
/// alice subscribes on edge_us
/// bob   subscribes on edge_eu
/// carol writes on core ------------------------sync----> both edges
/// mallory --deny select permissions----> core --sync--> both edges remove row
/// ```
#[tokio::test]
async fn core_permission_retightening_reaches_subscribed_clients_on_every_edge() {
    let schema = todo_schema();
    let cluster = MultiServerCluster::start_dynamic(schema.clone()).await;
    let query = QueryBuilder::new("todos").build();

    publish_schema_to_core(&cluster.core, &schema).await;
    let allow_head = publish_allow_all_permissions(
        &cluster.core.base_url(),
        cluster.core.app_id(),
        cluster.core.admin_secret(),
        &schema,
    )
    .await;
    wait_for_schema_hash(&cluster.edge_us, &schema).await;
    wait_for_schema_hash(&cluster.edge_eu, &schema).await;
    wait_for_permissions_head(&cluster.edge_us, &allow_head.bundle_object_id).await;
    wait_for_permissions_head(&cluster.edge_eu, &allow_head.bundle_object_id).await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-permissions-us", None)
        .await;
    let bob = cluster
        .connect_user(&cluster.edge_eu, "bob-permissions-eu", None)
        .await;
    let carol = cluster
        .connect_user(&cluster.core, "carol-permissions-core", None)
        .await;

    let mut alice_stream = alice
        .subscribe(query.clone())
        .await
        .expect("alice subscribes before retightening");
    let mut bob_stream = bob
        .subscribe(query.clone())
        .await
        .expect("bob subscribes before retightening");
    let mut alice_log = Vec::new();
    let mut bob_log = Vec::new();

    let (todo_id, _) = carol
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("visible before permissions tighten".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("core write should settle under allow permissions");

    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        REPLICATION_TIMEOUT,
        "alice receives row before permissions retighten",
        |log| has_added(log, todo_id),
    )
    .await;
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        REPLICATION_TIMEOUT,
        "bob receives row before permissions retighten",
        |log| has_added(log, todo_id),
    )
    .await;

    let deny_head = publish_permissions(
        &cluster.core.base_url(),
        cluster.core.app_id(),
        cluster.core.admin_secret(),
        &schema,
        deny_all_select_permissions(&schema),
        Some(allow_head.bundle_object_id),
    )
    .await;
    wait_for_permissions_head(&cluster.edge_us, &deny_head.bundle_object_id).await;
    wait_for_permissions_head(&cluster.edge_eu, &deny_head.bundle_object_id).await;

    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        REPLICATION_TIMEOUT,
        "alice loses row after permissions retighten",
        |log| has_removed(log, todo_id),
    )
    .await;
    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        REPLICATION_TIMEOUT,
        "bob loses row after permissions retighten",
        |log| has_removed(log, todo_id),
    )
    .await;

    let alice_rows = wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "alice query is empty after permissions retighten",
        Some,
    )
    .await;
    assert!(alice_rows.is_empty());

    let bob_rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "bob query is empty after permissions retighten",
        Some,
    )
    .await;
    assert!(bob_rows.is_empty());

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    carol.shutdown().await.expect("shutdown carol");
    cluster.shutdown().await;
}

/// A GlobalServer query issued to an edge should settle as GlobalServer, not
/// merely as EdgeServer.
///
/// ```text
/// alice --GlobalServer query--> edge_us --forwards--> core
/// alice <--QuerySettled(GlobalServer)-----------------
/// bob   --GlobalServer write--> edge_eu --sync-------> core
/// alice <--global-tier query result via edge_us--------
/// ```
#[tokio::test]
async fn edge_global_query_receives_global_query_settled() {
    let tracer = SyncTracer::new();
    let cluster = MultiServerCluster::start_with_tracer(Some(tracer.clone())).await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-global-query", Some(&tracer))
        .await;
    let bob = cluster
        .connect_user(&cluster.edge_eu, "bob-global-writer", Some(&tracer))
        .await;

    tracer.clear();

    let empty_rows = alice
        .query(
            QueryBuilder::new("todos").build(),
            Some(DurabilityTier::GlobalServer),
        )
        .await
        .expect("empty global query through edge_us should settle");
    assert!(empty_rows.is_empty());

    let (todo_id, _) = bob
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("global query settlement through edge".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("bob global-tier create through edge_eu");

    let rows = wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "alice global query through edge_us sees bob row",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![Value::Text(
            "global query settlement through edge".to_string()
        )]
    );

    tracer.wait_until_settled(Duration::from_secs(10)).await;
    let alice_messages = tracer.to("alice-global-query");
    assert!(
        alice_messages.iter().any(|message| matches!(
            &message.payload,
            SyncPayload::QuerySettled { tier, .. }
                if *tier == DurabilityTier::GlobalServer
        )),
        "alice should receive QuerySettled(GlobalServer); trace:\n{}",
        tracer.dump_for("alice-global-query")
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    cluster.shutdown().await;
}
