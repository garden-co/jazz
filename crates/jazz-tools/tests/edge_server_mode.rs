#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::{
    AppId, ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value,
};
use support::{wait_for_edge_query_ready, wait_for_query};

fn todo_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build()
}

fn todo_query() -> jazz_tools::Query {
    QueryBuilder::new("todos")
        .select(&["title", "done"])
        .build()
}

fn reserve_local_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("reserve local port");
    listener.local_addr().expect("reserved local addr").port()
}

async fn connect_user(server: &JazzServer, schema: Schema, user_id: &str) -> JazzClient {
    let client = JazzClient::connect(server.make_client_context_for_user(schema, user_id))
        .await
        .expect("connect user");
    wait_for_edge_query_ready(&client, "todos", Duration::from_secs(30)).await;
    client
}

async fn wait_for_row(
    client: &JazzClient,
    tier: DurabilityTier,
    row_id: ObjectId,
    expected: Vec<Value>,
    description: &str,
) {
    wait_for_query(
        client,
        todo_query(),
        Some(tier),
        Duration::from_secs(30),
        description,
        |rows| {
            rows.iter()
                .any(|(id, values)| *id == row_id && *values == expected)
                .then_some(())
        },
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn edge_server_accepts_mergeable_write_while_core_down_then_promotes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let app_id = AppId::random();
            let core_port = reserve_local_port();
            let core_url = format!("http://127.0.0.1:{core_port}");

            let edge = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .with_upstream_url(core_url.clone())
                .start()
                .await;
            let alice = connect_user(&edge, schema.clone(), "alice-edge-server-mode").await;
            let bob = connect_user(&edge, schema.clone(), "bob-edge-server-mode").await;

            let (todo_id, expected, batch_id) = alice
                .insert(
                    "todos",
                    row_input!("title" => "edge first", "done" => false),
                )
                .expect("alice inserts while core is down");
            alice
                .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
                .await
                .expect("edge accepts write while core link is down");

            wait_for_row(
                &bob,
                DurabilityTier::EdgeServer,
                todo_id,
                expected.clone(),
                "bob sees edge-accepted row before core starts",
            )
            .await;

            let core = JazzServer::builder()
                .with_app_id(app_id)
                .with_port(core_port)
                .with_schema(schema.clone())
                .start()
                .await;

            alice
                .wait_for_batch(batch_id, DurabilityTier::GlobalServer)
                .await
                .expect("edge-promoted write reaches global core");
            wait_for_row(
                &alice,
                DurabilityTier::GlobalServer,
                todo_id,
                expected.clone(),
                "alice sees globally promoted row through edge",
            )
            .await;
            wait_for_row(
                &bob,
                DurabilityTier::GlobalServer,
                todo_id,
                expected,
                "bob sees globally promoted row through edge",
            )
            .await;

            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            edge.shutdown().await;
            core.shutdown().await;
        })
        .await;
}

#[test]
fn topology_matrix_conformance_smoke_inventory() {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Topology {
        ClientCore,
        ClientEdgeCore,
        ClientRelayEdgeCore,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Scenario {
        MergeableWrite,
        RlsNarrowedRead,
        ReconnectKnownState,
        LargeValueRefetch,
    }

    struct Cell {
        topology: Topology,
        scenario: Scenario,
        coverage: &'static str,
    }

    let cells = [
        Cell {
            topology: Topology::ClientCore,
            scenario: Scenario::MergeableWrite,
            coverage: "clients_sync::wait_for_batch_reaches_edge_and_global_tiers",
        },
        Cell {
            topology: Topology::ClientCore,
            scenario: Scenario::RlsNarrowedRead,
            coverage: "branch_claims_integration::query_applies_claims_select_policy",
        },
        Cell {
            topology: Topology::ClientCore,
            scenario: Scenario::ReconnectKnownState,
            coverage: "text_document_merge::offline_concurrent_text_edits_reconnect_and_converge",
        },
        Cell {
            topology: Topology::ClientCore,
            scenario: Scenario::LargeValueRefetch,
            coverage: "large_blob_permissions::large_blob_values_follow_ordinary_row_permissions",
        },
        Cell {
            topology: Topology::ClientEdgeCore,
            scenario: Scenario::MergeableWrite,
            coverage: "edge_server_mode::edge_server_accepts_mergeable_write_while_core_down_then_promotes",
        },
        Cell {
            topology: Topology::ClientEdgeCore,
            scenario: Scenario::RlsNarrowedRead,
            coverage: "catalogue_sync_integration::edge_catalogue_http_reads_and_writes_forward_to_real_core + branch_claims_integration::query_applies_claims_select_policy",
        },
        Cell {
            topology: Topology::ClientEdgeCore,
            scenario: Scenario::ReconnectKnownState,
            coverage: "text_document_merge::offline_concurrent_text_edits_reconnect_and_converge",
        },
        Cell {
            topology: Topology::ClientEdgeCore,
            scenario: Scenario::LargeValueRefetch,
            coverage: "catalogue_sync_integration::large_blob_values_follow_ordinary_row_permissions",
        },
        Cell {
            topology: Topology::ClientRelayEdgeCore,
            scenario: Scenario::MergeableWrite,
            coverage: "jazz::peer::non_global_peer_query_subscriptions_use_maintained_path + seeded m3 sync close-out soak",
        },
        Cell {
            topology: Topology::ClientRelayEdgeCore,
            scenario: Scenario::RlsNarrowedRead,
            coverage: "jazz::peer::aggregate_policy_oracle_matches_visible_rows_per_identity + seeded owner-policy captures",
        },
        Cell {
            topology: Topology::ClientRelayEdgeCore,
            scenario: Scenario::ReconnectKnownState,
            coverage: "text_document_merge::offline_concurrent_text_edits_reconnect_and_converge + seeded m3 sync close-out soak",
        },
        Cell {
            topology: Topology::ClientRelayEdgeCore,
            scenario: Scenario::LargeValueRefetch,
            coverage: "catalogue_sync_integration::large_blob_values_follow_ordinary_row_permissions + 7a refetch-after-eviction coverage",
        },
    ];

    let topologies = [
        Topology::ClientCore,
        Topology::ClientEdgeCore,
        Topology::ClientRelayEdgeCore,
    ];
    let scenarios = [
        Scenario::MergeableWrite,
        Scenario::RlsNarrowedRead,
        Scenario::ReconnectKnownState,
        Scenario::LargeValueRefetch,
    ];

    for topology in topologies {
        for scenario in scenarios {
            let matching = cells
                .iter()
                .filter(|cell| cell.topology == topology && cell.scenario == scenario)
                .collect::<Vec<_>>();
            assert_eq!(
                matching.len(),
                1,
                "topology matrix cell must have exactly one coverage entry: {topology:?} {scenario:?}"
            );
            assert!(
                !matching[0].coverage.is_empty(),
                "coverage entry must name the exercised or cited test"
            );
        }
    }
}
