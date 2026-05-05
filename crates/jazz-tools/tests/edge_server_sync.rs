#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::{wait_for_edge_query_ready, wait_for_query};

fn todo_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("todos").column("title", ColumnType::Text))
        .build()
}

async fn wait_for_upstream(edge: &TestingServer) {
    tokio::time::timeout(
        Duration::from_secs(10),
        edge.server_state().runtime.transport_wait_until_connected(),
    )
    .await
    .expect("edge should connect to upstream before timeout")
    .then_some(())
    .expect("edge transport should report connected");
}

#[tokio::test]
async fn write_through_one_edge_replica_becomes_visible_through_another_edge() {
    let schema = todo_schema();
    let peer_secret = "cluster-peer-secret";
    let app_id = TestingServer::default_app_id();

    let core = TestingServer::builder()
        .with_app_id(app_id)
        .with_schema(schema.clone())
        .with_peer_secret(peer_secret)
        .start()
        .await;
    let edge_us = TestingServer::builder()
        .with_app_id(app_id)
        .with_schema(schema.clone())
        .with_peer_secret(peer_secret)
        .with_upstream_url(core.base_url())
        .start()
        .await;
    let edge_eu = TestingServer::builder()
        .with_app_id(app_id)
        .with_schema(schema.clone())
        .with_peer_secret(peer_secret)
        .with_upstream_url(core.base_url())
        .start()
        .await;

    wait_for_upstream(&edge_us).await;
    wait_for_upstream(&edge_eu).await;

    let alice = JazzClient::connect(edge_us.make_client_context_for_user(schema.clone(), "alice"))
        .await
        .expect("connect alice to us edge");
    let bob = JazzClient::connect(edge_eu.make_client_context_for_user(schema, "bob"))
        .await
        .expect("connect bob to eu edge");

    wait_for_edge_query_ready(&alice, "todos", Duration::from_secs(20)).await;
    wait_for_edge_query_ready(&bob, "todos", Duration::from_secs(20)).await;

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
    edge_eu.shutdown().await;
    edge_us.shutdown().await;
    core.shutdown().await;
}
