#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::Operation;
use jazz_tools::public_schema::{
    RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind, RelKeyRef, RelPredicateCmpOp,
    RelPredicateExpr, RelRecursionBound, RelValueRef, RowIdRef, TablePolicies,
};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::{
    AppId, ColumnType, DurabilityTier, JazzClient, ObjectId, PolicyExpr, QueryBuilder, Schema,
    SchemaBuilder, TableSchema, Value,
};
use serde_json::json;
use support::{TestingClient, wait_for_edge_query_ready, wait_for_query};

fn todo_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build()
}

fn pilot-app_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("resources")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(resource_access_policy())
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("data_entries")
                .fk_column("resource", "resources")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "resource"))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("mapping_rules")
                .fk_column("resource", "resources")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "resource"))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("data_entry_entries")
                .fk_column("data_entry", "data_entries")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "data_entry"))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("teams")
                .column("identity_key", ColumnType::Text)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .table(
            TableSchema::builder("team_team_edges")
                .fk_column("child_team", "teams")
                .fk_column("parent_team", "teams")
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .table(
            TableSchema::builder("resource_access_edges")
                .fk_column("resource", "resources")
                .fk_column("team", "teams")
                .column("grant_role", ColumnType::Text)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .build()
}

fn resource_access_policy() -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::Gather {
                    seed: Box::new(RelExpr::Filter {
                        input: Box::new(RelExpr::TableScan {
                            table: "teams".into(),
                            alias: None,
                        }),
                        predicate: RelPredicateExpr::Cmp {
                            left: RelColumnRef {
                                scope: Some("teams".to_owned()),
                                column: "identity_key".to_owned(),
                            },
                            op: RelPredicateCmpOp::Eq,
                            right: RelValueRef::SessionRef(vec!["sub".to_owned()]),
                        },
                    }),
                    step: Box::new(RelExpr::Project {
                        input: Box::new(RelExpr::Join {
                            left: Box::new(RelExpr::Filter {
                                input: Box::new(RelExpr::TableScan {
                                    table: "team_team_edges".into(),
                                    alias: None,
                                }),
                                predicate: RelPredicateExpr::Cmp {
                                    left: RelColumnRef {
                                        scope: Some("team_team_edges".to_owned()),
                                        column: "child_team".to_owned(),
                                    },
                                    op: RelPredicateCmpOp::Eq,
                                    right: RelValueRef::RowId(RowIdRef::Frontier),
                                },
                            }),
                            right: Box::new(RelExpr::TableScan {
                                table: "teams".into(),
                                alias: Some("__recursive_hop_0".to_owned()),
                            }),
                            on: vec![RelJoinCondition {
                                left: RelColumnRef {
                                    scope: Some("team_team_edges".to_owned()),
                                    column: "parent_team".to_owned(),
                                },
                                right: RelColumnRef {
                                    scope: Some("__recursive_hop_0".to_owned()),
                                    column: "id".to_owned(),
                                },
                            }],
                            join_kind: RelJoinKind::Inner,
                        }),
                        columns: Vec::new(),
                    }),
                    frontier_key: RelKeyRef::RowId(RowIdRef::Current),
                    bound: RelRecursionBound::MaxDepth(8),
                    dedupe_key: vec![RelKeyRef::RowId(RowIdRef::Current)],
                }),
                right: Box::new(RelExpr::TableScan {
                    table: "resource_access_edges".into(),
                    alias: Some("access".to_owned()),
                }),
                on: vec![RelJoinCondition {
                    left: RelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "team".to_owned(),
                    },
                }],
                join_kind: RelJoinKind::Inner,
            }),
            predicate: RelPredicateExpr::And(vec![
                RelPredicateExpr::Cmp {
                    left: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "resource".to_owned(),
                    },
                    op: RelPredicateCmpOp::Eq,
                    right: RelValueRef::RowId(RowIdRef::Outer),
                },
                RelPredicateExpr::Cmp {
                    left: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "grant_role".to_owned(),
                    },
                    op: RelPredicateCmpOp::Eq,
                    right: RelValueRef::Literal(Value::Text("viewer".to_owned())),
                },
            ]),
        },
    }
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

async fn wait_edge_batch(client: &JazzClient, batch_id: jazz_tools::BatchId, label: &str) {
    tokio::time::timeout(
        Duration::from_secs(15),
        client.wait_for_batch(batch_id, DurabilityTier::EdgeServer),
    )
    .await
    .unwrap_or_else(|_| panic!("{label} timed out waiting for edge batch"))
    .unwrap_or_else(|err| panic!("{label} failed waiting for edge batch: {err}"));
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_server_publishes_seeded_reachable_policy_and_serves_member_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = pilot-app_policy_schema();
            let app_id = server.app_id();
            let response = reqwest::Client::new()
                .post(format!("{}/apps/{app_id}/admin/schemas", server.base_url()))
                .header("X-Jazz-Admin-Secret", server.admin_secret())
                .json(&json!({ "schema": schema }))
                .send()
                .await
                .expect("publish the pilot customer-shaped schema");
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.expect("schema publish error body");
                panic!("the pilot customer-shaped schema publish failed: {status} {body}");
            }

            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000a0")
                .as_admin()
                .connect()
                .await;
            let (seed_team, _, seed_batch) = admin
                .insert(
                    "teams",
                    row_input!("identity_key" => "00000000-0000-4000-8000-0000000000b0"),
                )
                .expect("insert seed team");
            wait_edge_batch(&admin, seed_batch, "seed team").await;
            let (resource_team, _, resource_team_batch) = admin
                .insert("teams", row_input!("identity_key" => "other-sub"))
                .expect("insert resource team");
            wait_edge_batch(&admin, resource_team_batch, "resource team").await;
            let (_, _, edge_batch) = admin
                .insert(
                    "team_team_edges",
                    row_input!("child_team" => seed_team, "parent_team" => resource_team),
                )
                .expect("insert team edge");
            wait_edge_batch(&admin, edge_batch, "team edge").await;
            let (resource, _, resource_batch) = admin
                .insert("resources", row_input!("label" => "visible resource"))
                .expect("insert resource");
            wait_edge_batch(&admin, resource_batch, "resource").await;
            let (_, _, access_batch) = admin
                .insert(
                    "resource_access_edges",
                    row_input!("resource" => resource, "team" => resource_team, "grant_role" => "viewer"),
                )
                .expect("insert resource access edge");
            wait_edge_batch(&admin, access_batch, "resource access").await;
            let (data_entry, _, data_entry_batch) = admin
                .insert(
                    "data_entries",
                    row_input!("resource" => resource, "label" => "visible data entry"),
                )
                .expect("insert inherited data entry");
            wait_edge_batch(&admin, data_entry_batch, "data entry").await;
            let (mapping_rule, _, mapping_rule_batch) = admin
                .insert(
                    "mapping_rules",
                    row_input!("resource" => resource, "label" => "visible mapping rule"),
                )
                .expect("insert inherited mapping rule");
            wait_edge_batch(&admin, mapping_rule_batch, "mapping rule").await;
            let (data_entry_entry, _, data_entry_entry_batch) = admin
                .insert(
                    "data_entry_entries",
                    row_input!("data_entry" => data_entry, "label" => "visible data entry child"),
                )
                .expect("insert inherited data entry child");
            wait_edge_batch(&admin, data_entry_entry_batch, "data entry child").await;

            let member = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000b0")
                .with_claims(json!({}))
                .connect()
                .await;
            let member_rows = wait_for_query(
                &member,
                QueryBuilder::new("resources").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(30),
                "member sees resource through seeded recursive access policy",
                |rows| (rows.len() == 1 && rows[0].0 == resource).then_some(rows),
            )
            .await;
            assert_eq!(
                member_rows[0].1,
                vec![Value::Text("visible resource".to_owned())]
            );
            wait_for_query(
                &member,
                QueryBuilder::new("data_entries").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(30),
                "member sees data entry through inherits over seeded resource policy",
                |rows| (rows.len() == 1 && rows[0].0 == data_entry).then_some(rows),
            )
            .await;
            wait_for_query(
                &member,
                QueryBuilder::new("mapping_rules").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(30),
                "member sees sibling mapping rule through inherits over seeded resource policy",
                |rows| (rows.len() == 1 && rows[0].0 == mapping_rule).then_some(rows),
            )
            .await;
            wait_for_query(
                &member,
                QueryBuilder::new("data_entry_entries").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(30),
                "member sees grandchild through nested inherits over seeded resource policy",
                |rows| (rows.len() == 1 && rows[0].0 == data_entry_entry).then_some(rows),
            )
            .await;

            let spy = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000c0")
                .with_claims(json!({}))
                .connect()
                .await;
            wait_for_query(
                &spy,
                QueryBuilder::new("resources").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(30),
                "spy sees no resources through seeded recursive access policy",
                |rows| rows.is_empty().then_some(rows),
            )
            .await;
            wait_for_query(
                &spy,
                QueryBuilder::new("data_entries").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(30),
                "spy sees no inherited data entries through seeded recursive access policy",
                |rows| rows.is_empty().then_some(rows),
            )
            .await;

            spy.shutdown().await.expect("shutdown spy");
            member.shutdown().await.expect("shutdown member");
            admin.shutdown().await.expect("shutdown admin");
            server.shutdown().await;
        })
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
