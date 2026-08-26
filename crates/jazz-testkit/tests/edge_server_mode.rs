#![allow(clippy::enum_variant_names)]

use jazz_testkit as support;

use std::time::Duration;

use jazz::query::OrderDirection;
use jazz::row_input;
use jazz::tools::Operation;
use jazz::tools::public_schema::{
    RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind, RelKeyRef, RelPredicateCmpOp,
    RelPredicateExpr, RelRecursionBound, RelValueRef, RowIdRef, TablePolicies,
};
use jazz::tools::{
    AppId, ColumnType, DurabilityTier, JazzClient, ObjectId, PolicyExpr, Schema, SchemaBuilder,
    SubscriptionStreamItem, TableName, TableSchema, Value, permissions,
};
use jazz_server::JazzServer;
use serde_json::json;
use support::{
    TestingClient, has_added_id, has_removed, publish_permissions, push_catalogue_in_memory,
    wait_for_edge_query_ready, wait_for_query, wait_for_subscription_update,
};
use tempfile::TempDir;

fn todo_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build()
}

fn rejected_todo_permissions(schema: &Schema) -> Vec<(TableName, TablePolicies)> {
    schema
        .keys()
        .map(|table| {
            let policies = permissions(|permissions| {
                permissions.allow_read();
                permissions.allow_insert().never();
            });
            (*table, policies)
        })
        .collect()
}

fn ranked_todo_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("rank", ColumnType::Integer),
        )
        .build()
}

/// The public stream deliberately carries no separate reset bit or serving-tier
/// metadata: its first item is the reset reduction, and `pending` is its
/// public settlement signal. Assert that that reduction is exact rather than
/// merely checking its additions, so a stale replacement cannot hide beside a
/// correctly ordered snapshot.
fn assert_exact_settled_initial_reset(
    label: &str,
    delta: jazz::tools::OrderedRowDelta,
    expected_ids: &[ObjectId],
) {
    assert!(
        !delta.pending,
        "{label} initial reset must be settled at the edge tier"
    );
    assert!(
        delta.removed.is_empty(),
        "{label} initial reset must not remove rows from an empty reduction: {:?}",
        delta.removed
    );
    assert!(
        delta.updated.is_empty(),
        "{label} initial reset must not carry replacement updates: {:?}",
        delta.updated
    );
    assert_eq!(
        delta.added.len(),
        expected_ids.len(),
        "{label} initial reset has exactly the expected additions"
    );
    for (index, (added, expected_id)) in delta.added.iter().zip(expected_ids).enumerate() {
        assert_eq!(added.index, index, "{label} reset position is exact");
        assert_eq!(
            added.id, *expected_id,
            "{label} reset addition preserves default row-id order"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn subscription_orders_by_unprojected_field() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = ranked_todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000011")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;
            let mut ids = Vec::new();
            let mut txs = Vec::new();
            for (title, rank) in [("charlie", 3), ("alpha", 1), ("bravo", 2)] {
                let (id, _, transaction_id) = client
                    .insert("todos", row_input!("title" => title, "rank" => rank))
                    .expect("insert ranked todo");
                ids.push(id);
                txs.push(transaction_id.expect("ordinary mutation commits immediately"));
            }
            support::wait_for_edge_txs(&client, &txs).await;

            let mut stream = client
                .subscribe(
                    jazz::query::Query::from("todos")
                        .select(["title"])
                        .order_by("rank", OrderDirection::Asc),
                )
                .await
                .expect("subscribe to projected ranked todos");
            let item = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("initial subscription reset arrives")
                .expect("subscription stream remains open");
            let SubscriptionStreamItem::Delta(delta) = item else {
                panic!("projected ranked subscription was rejected");
            };

            assert_eq!(
                delta
                    .added
                    .iter()
                    .map(|added| added.id.clone())
                    .collect::<Vec<_>>(),
                vec![ids[1], ids[2], ids[0]],
                "subscription reset must follow rank even when rank is not projected"
            );

            let tx = client
                .update(ids[0], vec![("rank".to_owned(), Value::Integer(0))])
                .expect("change only the unprojected ordering field");
            support::wait_for_edge_txs(
                &client,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;

            let mut updates = Vec::new();
            wait_for_subscription_update(
                &mut stream,
                &mut updates,
                Duration::from_secs(10),
                "rank-only reorder emits a positional update without inventing a payload",
                |deltas| {
                    deltas.iter().any(|delta| {
                        delta.updated.iter().any(|update| {
                            update.id == ids[0]
                                && update.old_index == 2
                                && update.new_index == 0
                                && update.row.is_none()
                        })
                    })
                },
            )
            .await;

            client.shutdown().await.expect("shutdown test client");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn edge_tier_public_subscription_opens_and_receives_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000001")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;

            let query = jazz::query::Query::from("todos");
            let mut stream = client
                .subscribe(query)
                .await
                .expect("edge-tier public subscription should open");
            let mut log = Vec::new();

            let (todo_id, _, transaction_id) = client
                .insert("todos", row_input!("title" => "visible", "done" => false))
                .expect("insert todo");
            support::wait_for_edge_txs(
                &client,
                &[transaction_id.expect("ordinary mutation commits immediately")],
            )
            .await;

            wait_for_subscription_update(
                &mut stream,
                &mut log,
                Duration::from_secs(10),
                "edge-tier public subscription receives inserted row",
                |deltas| has_added_id(deltas, todo_id),
            )
            .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn maintained_unordered_limit_and_offset_windows_open_offline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(todo_schema()).await;
            for query in [
                jazz::query::Query::from("todos").limit(2),
                jazz::query::Query::from("todos").offset(1).limit(1),
            ] {
                let _stream = client
                    .subscribe(query)
                    .await
                    .expect("default-ordered maintained window should open");
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_root_default_order_and_windows_are_stable_across_reset() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000102")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;

            let mut ids = Vec::new();
            let mut txs = Vec::new();
            for title in ["third", "first", "second", "fourth"] {
                let (id, _, tx) = client
                    .insert("todos", row_input!("title" => title, "done" => false))
                    .expect("insert todo");
                ids.push(id);
                txs.push(tx.expect("ordinary mutation commits immediately"));
            }
            support::wait_for_edge_txs(&client, &txs).await;
            let mut row_id_order = ids.clone();
            row_id_order.sort();

            let default_query = jazz::query::Query::from("todos");
            let one_shot = client
                .query(default_query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .expect("default-ordered one-shot query");
            assert_eq!(
                one_shot.into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
                row_id_order,
                "root queries default to ascending row id"
            );

            let mut initial = client
                .subscribe(default_query.clone())
                .await
                .expect("subscribe to default-ordered root");
            let first = tokio::time::timeout(Duration::from_secs(10), initial.next())
                .await
                .expect("initial reset arrives")
                .expect("stream remains open");
            let SubscriptionStreamItem::Delta(first) = first else {
                panic!("default root subscription was rejected");
            };
            assert_exact_settled_initial_reset("initial maintained reset", first, &row_id_order);
            drop(initial);

            let mut rehydrated = client
                .subscribe(default_query)
                .await
                .expect("rehydrate default-ordered root");
            let reset = tokio::time::timeout(Duration::from_secs(10), rehydrated.next())
                .await
                .expect("rehydrated reset arrives")
                .expect("rehydrated stream remains open");
            let SubscriptionStreamItem::Delta(reset) = reset else {
                panic!("rehydrated default root subscription was rejected");
            };
            assert_exact_settled_initial_reset("rehydrated maintained reset", reset, &row_id_order);

            for (query, expected) in [
                (
                    jazz::query::Query::from("todos").limit(2),
                    row_id_order[..2].to_vec(),
                ),
                (
                    jazz::query::Query::from("todos").offset(1).limit(2),
                    row_id_order[1..3].to_vec(),
                ),
            ] {
                let mut stream = client
                    .subscribe(query)
                    .await
                    .expect("default-ordered maintained window opens");
                let item = tokio::time::timeout(Duration::from_secs(10), stream.next())
                    .await
                    .expect("window reset arrives")
                    .expect("window stream remains open");
                let SubscriptionStreamItem::Delta(delta) = item else {
                    panic!("default-ordered window was rejected");
                };
                assert_exact_settled_initial_reset("maintained window reset", delta, &expected);
            }

            client.shutdown().await.expect("shutdown test client");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
/// An edge server sends every ordered-window boundary crossing to the client.
/// Alice creates tied rows, then promotes and demotes a third row across the
/// two-row window boundary.
async fn maintained_window_uses_row_id_tie_breaker_and_tracks_rows_crossing_boundary() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000103")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;

            let mut tied = Vec::new();
            let mut txs = Vec::new();
            for _ in 0..3 {
                let (id, _, tx) = client
                    .insert("todos", row_input!("title" => "same", "done" => false))
                    .expect("insert tied todo");
                tied.push(id);
                txs.push(tx.expect("ordinary mutation commits immediately"));
            }
            support::wait_for_edge_txs(&client, &txs).await;
            tied.sort();
            let tie_query =
                jazz::query::Query::from("todos").order_by("title", OrderDirection::Asc);
            let tied_rows = client
                .query(tie_query, Some(DurabilityTier::EdgeServer))
                .await
                .expect("query tied rows");
            assert_eq!(
                tied_rows.into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
                tied,
                "equal explicit order keys use row id as a stable tie-breaker"
            );

            let mut window = client
                .subscribe(
                    jazz::query::Query::from("todos")
                        .order_by("title", OrderDirection::Asc)
                        .limit(2),
                )
                .await
                .expect("subscribe ordered window");
            let mut updates = Vec::new();
            wait_for_subscription_update(
                &mut window,
                &mut updates,
                Duration::from_secs(10),
                "initial ordered window reset",
                |deltas| deltas.iter().any(|delta| delta.added.len() == 2),
            )
            .await;
            let initial = updates
                .iter()
                .find(|delta| delta.added.len() == 2)
                .expect("initial ordered window delta");
            assert_eq!(
                initial
                    .added
                    .iter()
                    .map(|change| {
                        change
                            .id
                            .row_id()
                            .expect("plain-table result key contains one row")
                    })
                    .collect::<Vec<_>>(),
                tied[..2],
                "maintained tied rows use the same stable row-id tie-breaker"
            );

            let promoted = tied[2];
            let tx = client
                .update(
                    promoted,
                    vec![("title".to_owned(), Value::Text("ahead".to_owned()))],
                )
                .expect("move row into window");
            support::wait_for_edge_txs(
                &client,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            wait_for_subscription_update(
                &mut window,
                &mut updates,
                Duration::from_secs(10),
                "promoted row enters maintained window",
                |deltas| has_added_id(deltas, promoted) && has_removed(deltas, tied[1]),
            )
            .await;
            let promotion = updates
                .iter()
                .find(|delta| has_added_id(std::slice::from_ref(delta), promoted))
                .expect("promotion delta is recorded");
            assert_eq!(
                promotion
                    .added
                    .iter()
                    .find(|change| change.id == promoted)
                    .expect("promoted row is added")
                    .index,
                0,
                "a newly promoted row is inserted at its authoritative TopBy position"
            );
            assert_eq!(
                promotion
                    .removed
                    .iter()
                    .find(|change| change.id == tied[1])
                    .expect("displaced row is removed")
                    .index,
                1,
                "the displaced row retains its pre-update position"
            );

            let tx = client
                .update(
                    promoted,
                    vec![("title".to_owned(), Value::Text("zulu".to_owned()))],
                )
                .expect("move row out of window");
            support::wait_for_edge_txs(
                &client,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            wait_for_subscription_update(
                &mut window,
                &mut updates,
                Duration::from_secs(10),
                "demoted row leaves maintained window",
                |deltas| has_removed(deltas, promoted) && has_added_id(deltas, tied[1]),
            )
            .await;

            client.shutdown().await.expect("shutdown test client");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_subscription_stream_yields_delta_items_for_normal_changes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000101")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;

            let mut stream = client
                .subscribe(jazz::query::Query::from("todos"))
                .await
                .expect("normal subscription should open");
            let (todo_id, _, transaction_id) = client
                .insert(
                    "todos",
                    row_input!("title" => "delta item", "done" => false),
                )
                .expect("insert todo");
            support::wait_for_edge_txs(
                &client,
                &[transaction_id.expect("ordinary mutation commits immediately")],
            )
            .await;

            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let now = tokio::time::Instant::now();
                assert!(now < deadline, "timed out waiting for delta item");
                let item = tokio::time::timeout(deadline - now, stream.next())
                    .await
                    .expect("subscription item should arrive")
                    .expect("subscription stream should stay open");
                match item {
                    SubscriptionStreamItem::Delta(delta) => {
                        if delta.added.iter().any(|row| row.id == todo_id) {
                            break;
                        }
                    }
                    SubscriptionStreamItem::Rejected { reason } => {
                        panic!("normal subscription was rejected: {reason:?}")
                    }
                }
            }
        })
        .await;
}

fn policy_graph_policy_schema() -> Schema {
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
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(mapping_rule_access_policy())
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("mapping_rule_entries")
                .fk_column("mapping_rule", "mapping_rules")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "mapping_rule"))
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
        .table(
            TableSchema::builder("mapping_rule_access_edges")
                .fk_column("mapping_rule", "mapping_rules")
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
                            right: RelValueRef::SessionRef(vec![
                                "claims".to_owned(),
                                "sub".to_owned(),
                            ]),
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

fn mapping_rule_access_policy() -> PolicyExpr {
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
                            right: RelValueRef::SessionRef(vec![
                                "claims".to_owned(),
                                "sub".to_owned(),
                            ]),
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
                    table: "mapping_rule_access_edges".into(),
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
                        column: "mapping_rule".to_owned(),
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

fn todo_query() -> jazz::query::Query {
    jazz::query::Query::from("todos").select(["title", "done"])
}

fn reserve_local_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("reserve local port");
    listener.local_addr().expect("reserved local addr").port()
}

async fn connect_user(server: &JazzServer, schema: Schema, user_id: &str) -> JazzClient {
    let client = jazz_testkit::connect(server.make_client_context_for_user(schema, user_id))
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

struct PolicyGraphSeedRows {
    resource: ObjectId,
    data_entry: ObjectId,
    mapping_rule: ObjectId,
    data_entry_entry: ObjectId,
    mapping_rule_entry: ObjectId,
}

async fn seed_policy_graph_rows(admin: &JazzClient) -> PolicyGraphSeedRows {
    let (seed_team, _, seed_tx) = admin
        .insert(
            "teams",
            row_input!("identity_key" => "00000000-0000-4000-8000-0000000000b0"),
        )
        .expect("insert seed team");
    let (resource_team, _, resource_team_tx) = admin
        .insert("teams", row_input!("identity_key" => "other-sub"))
        .expect("insert resource team");
    let (_, _, edge_tx) = admin
        .insert(
            "team_team_edges",
            row_input!("child_team" => seed_team, "parent_team" => resource_team),
        )
        .expect("insert team edge");
    let (resource, _, resource_tx) = admin
        .insert("resources", row_input!("label" => "visible resource"))
        .expect("insert resource");
    let (_, _, access_tx) = admin
        .insert(
            "resource_access_edges",
            row_input!("resource" => resource, "team" => resource_team, "grant_role" => "viewer"),
        )
        .expect("insert resource access edge");
    let (data_entry, _, data_entry_tx) = admin
        .insert(
            "data_entries",
            row_input!("resource" => resource, "label" => "visible data entry"),
        )
        .expect("insert data entry");
    let (mapping_rule, _, mapping_rule_tx) = admin
        .insert(
            "mapping_rules",
            row_input!("label" => "visible mapping rule"),
        )
        .expect("insert mapping rule");
    let (_, _, mapping_rule_access_tx) = admin
        .insert(
            "mapping_rule_access_edges",
            row_input!("mapping_rule" => mapping_rule, "team" => resource_team, "grant_role" => "viewer"),
        )
        .expect("insert mapping rule access edge");
    let (data_entry_entry, _, data_entry_entry_tx) = admin
        .insert(
            "data_entry_entries",
            row_input!("data_entry" => data_entry, "label" => "visible data entry child"),
        )
        .expect("insert data entry child");
    let (mapping_rule_entry, _, mapping_rule_entry_tx) = admin
        .insert(
            "mapping_rule_entries",
            row_input!("mapping_rule" => mapping_rule, "label" => "visible mapping rule child"),
        )
        .expect("insert mapping rule child");
    support::wait_for_edge_txs(
        admin,
        &[
            seed_tx.expect("ordinary mutation commits immediately"),
            resource_team_tx.expect("ordinary mutation commits immediately"),
            edge_tx.expect("ordinary mutation commits immediately"),
            resource_tx.expect("ordinary mutation commits immediately"),
            access_tx.expect("ordinary mutation commits immediately"),
            data_entry_tx.expect("ordinary mutation commits immediately"),
            mapping_rule_tx.expect("ordinary mutation commits immediately"),
            mapping_rule_access_tx.expect("ordinary mutation commits immediately"),
            data_entry_entry_tx.expect("ordinary mutation commits immediately"),
            mapping_rule_entry_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;

    PolicyGraphSeedRows {
        resource,
        data_entry,
        mapping_rule,
        data_entry_entry,
        mapping_rule_entry,
    }
}

async fn assert_policy_graph_member_rows(member: &JazzClient, rows: &PolicyGraphSeedRows) {
    let member_rows = wait_for_query(
        member,
        jazz::query::Query::from("resources"),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees resource through seeded recursive access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.resource).then_some(query_rows)
        },
    )
    .await;
    assert_eq!(
        member_rows[0].1,
        vec![Value::Text("visible resource".to_owned())]
    );
    wait_for_query(
        member,
        jazz::query::Query::from("data_entries"),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees data entry through seeded recursive access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.data_entry).then_some(query_rows)
        },
    )
    .await;
    wait_for_query(
        member,
        jazz::query::Query::from("mapping_rules"),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees sibling mapping rule through same seeded recursive access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.mapping_rule).then_some(query_rows)
        },
    )
    .await;
    wait_for_query(
        member,
        jazz::query::Query::from("data_entry_entries"),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees grandchild through inherits over seeded access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.data_entry_entry)
                .then_some(query_rows)
        },
    )
    .await;
    wait_for_query(
        member,
        jazz::query::Query::from("mapping_rule_entries"),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees mapping rule child through inherits over sibling seeded access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.mapping_rule_entry)
                .then_some(query_rows)
        },
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_server_publishes_seeded_reachable_policy_and_serves_member_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = policy_graph_policy_schema();
            let app_id = server.app_id();
            let response = reqwest::Client::new()
                .post(format!("{}/apps/{app_id}/admin/schemas", server.base_url()))
                .header("X-Jazz-Admin-Secret", server.admin_secret())
                .json(&json!({ "schema": schema }))
                .send()
                .await
                .expect("publish policy graph-shaped schema");
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.expect("schema publish error body");
                panic!("policy graph-shaped schema publish failed: {status} {body}");
            }

            publish_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &schema,
                schema
                    .iter()
                    .map(|(table_name, table_schema)| (*table_name, table_schema.policies.clone()))
                    .collect::<Vec<_>>(),
                None,
            )
            .await;

            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000a0")
                .as_admin()
                .connect()
                .await;
            let rows = seed_policy_graph_rows(&admin).await;

            let member = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000b0")
                .with_claims(json!({}))
                .connect()
                .await;
            assert_policy_graph_member_rows(&member, &rows).await;

            let spy = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000c0")
                .with_claims(json!({}))
                .connect()
                .await;
            wait_for_query(
                &spy,
                jazz::query::Query::from("resources"),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(30),
                "spy sees no resources through seeded recursive access policy",
                |rows| rows.is_empty().then_some(rows),
            )
            .await;
            wait_for_query(
                &spy,
                jazz::query::Query::from("data_entries"),
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
async fn fixed_schema_data_dir_reopen_bootstraps_policy_graph_policy_serving_state() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let data_dir = TempDir::new().expect("server data dir");
            let schema = policy_graph_policy_schema();
            let rows = {
                let server = JazzServer::builder()
                    .with_schema(schema.clone())
                    .with_data_dir(data_dir.path())
                    .with_storage_factory(jazz_testkit::persistent_storage_factory())
                    .start()
                    .await;
                let admin = TestingClient::builder()
                    .with_server(&server)
                    .with_schema(schema.clone())
                    .with_user_id("00000000-0000-4000-8000-0000000000a0")
                    .as_admin()
                    .connect()
                    .await;
                let rows = seed_policy_graph_rows(&admin).await;
                admin.shutdown().await.expect("shutdown seeding admin");
                server.shutdown().await;
                rows
            };

            let reopened = JazzServer::builder()
                .with_schema(schema.clone())
                .with_data_dir(data_dir.path())
                .with_storage_factory(jazz_testkit::persistent_storage_factory())
                .start()
                .await;
            let member = TestingClient::builder()
                .with_server(&reopened)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000b0")
                .with_claims(json!({}))
                .connect()
                .await;
            assert_policy_graph_member_rows(&member, &rows).await;

            member.shutdown().await.expect("shutdown member");
            reopened.shutdown().await;
        })
        .await;
}

/// End-to-end receipt for the ordinary native edge relay. A write propagates
/// and settles globally through the initially bootstrapped link. After that
/// established link drops, a second edge-local write is retained, replayed
/// through the replacement link, materialized by Core, and settled globally.
#[tokio::test(flavor = "current_thread")]
async fn edge_to_core_relay_propagates_settles_and_reconnects_after_established_drop() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let app_id = AppId::random();
            let core_port = reserve_local_port();
            let core_data_dir = TempDir::new().expect("Core data dir");
            let core = tokio::time::timeout(
                Duration::from_secs(10),
                JazzServer::builder()
                    .with_app_id(app_id)
                    .with_port(core_port)
                    .with_schema(schema.clone())
                    .with_data_dir(core_data_dir.path())
                    .with_storage_factory(jazz_testkit::persistent_storage_factory())
                    .start(),
            )
            .await
            .expect("initial Core start timed out");
            let edge = tokio::time::timeout(
                Duration::from_secs(10),
                JazzServer::builder()
                    .with_app_id(app_id)
                    .with_schema(schema.clone())
                    .with_native_transport_connector(jazz_testkit::native_connector())
                    .with_upstream_url(core.base_url())
                    .start(),
            )
            .await
            .expect("Edge start timed out");
            let edge_state = edge.server_state();
            tokio::time::timeout(Duration::from_secs(5), async {
                while edge_state.edge_upstream_health()
                    != jazz_server::EdgeUpstreamHealth::Connected
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("edge initially bootstraps and attaches its ordinary upstream");

            let alice = tokio::time::timeout(
                Duration::from_secs(15),
                connect_user(&edge, schema.clone(), "alice-edge-server-mode"),
            )
            .await
            .expect("Alice-to-Edge connection/readiness timed out");
            let first_core_observer = tokio::time::timeout(
                Duration::from_secs(15),
                connect_user(&core, schema.clone(), "first-direct-core-observer"),
            )
            .await
            .expect("first direct Core observer connection/readiness timed out");
            let (first_todo, first_expected, first_tx) = alice
                .insert(
                    "todos",
                    row_input!("title" => "edge through initial link", "done" => false),
                )
                .expect("alice writes through the initially bootstrapped edge");
            let first_tx = first_tx.expect("ordinary mutation commits immediately");

            support::wait_for_edge_txs(&alice, &[first_tx]).await;
            tokio::time::timeout(
                Duration::from_secs(15),
                alice.wait_for_transaction(first_tx, DurabilityTier::GlobalServer),
            )
            .await
            .expect("initial edge-to-core Global waiter timed out")
            .expect("edge write settles globally through the initial link");
            tokio::time::timeout(
                Duration::from_secs(15),
                wait_for_row(
                    &first_core_observer,
                    DurabilityTier::GlobalServer,
                    first_todo,
                    first_expected,
                    "direct core observer receives the edge-origin write",
                ),
            )
            .await
            .expect("initial direct-Core visibility wait timed out");

            // Reconnect is a separate responsibility from initial bootstrap:
            // first prove the ordinary link, then stop that exact Core and wait
            // until the Edge has observed the established connection's drop.
            tokio::time::timeout(Duration::from_secs(5), first_core_observer.shutdown())
                .await
                .expect("first direct Core observer shutdown timed out")
                .expect("shutdown first direct core observer");
            tokio::time::timeout(Duration::from_secs(15), core.shutdown())
                .await
                .expect("established Core shutdown timed out while dropping the upstream link");
            tokio::time::timeout(Duration::from_secs(5), async {
                while !matches!(
                    edge_state.edge_upstream_health(),
                    jazz_server::EdgeUpstreamHealth::Reconnecting { .. }
                ) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("edge observes established upstream drop");

            let (second_todo, second_expected, second_tx) = alice
                .insert(
                    "todos",
                    row_input!("title" => "edge during reconnect", "done" => true),
                )
                .expect("alice inserts while the established upstream is down");
            let second_tx = second_tx.expect("ordinary mutation commits immediately");
            assert_ne!(
                first_tx, second_tx,
                "the reconnect receipt must wait on a distinct post-drop transaction"
            );
            support::wait_for_edge_txs(&alice, &[second_tx]).await;

            let restarted_core = tokio::time::timeout(
                Duration::from_secs(10),
                JazzServer::builder()
                    .with_app_id(app_id)
                    .with_port(core_port)
                    .with_schema(schema.clone())
                    .with_data_dir(core_data_dir.path())
                    .with_storage_factory(jazz_testkit::persistent_storage_factory())
                    .start(),
            )
            .await
            .expect("replacement Core start timed out");
            tokio::time::timeout(Duration::from_secs(5), async {
                while edge_state.edge_upstream_health()
                    != jazz_server::EdgeUpstreamHealth::Connected
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("edge attaches a replacement upstream after the established drop");
            let replacement_core_observer = tokio::time::timeout(
                Duration::from_secs(15),
                connect_user(&restarted_core, schema, "replacement-direct-core-observer"),
            )
            .await
            .expect("replacement direct Core observer connection/readiness timed out");

            tokio::time::timeout(
                Duration::from_secs(15),
                alice.wait_for_transaction(second_tx, DurabilityTier::GlobalServer),
            )
            .await
            .expect("replacement-link Global waiter timed out")
            .expect("write retained during the drop settles through the replacement link");
            tokio::time::timeout(
                Duration::from_secs(15),
                wait_for_row(
                    &replacement_core_observer,
                    DurabilityTier::GlobalServer,
                    second_todo,
                    second_expected,
                    "replacement core materializes the replayed edge write",
                ),
            )
            .await
            .expect("replacement direct-Core visibility wait timed out");

            tokio::time::timeout(Duration::from_secs(5), replacement_core_observer.shutdown())
                .await
                .expect("replacement Core observer shutdown timed out")
                .expect("shutdown replacement Core observer");
            tokio::time::timeout(Duration::from_secs(5), alice.shutdown())
                .await
                .expect("Alice shutdown timed out")
                .expect("shutdown alice");
            tokio::time::timeout(Duration::from_secs(15), edge.shutdown())
                .await
                .expect("Edge cleanup timed out");
            tokio::time::timeout(Duration::from_secs(15), restarted_core.shutdown())
                .await
                .expect("restarted Core cleanup timed out");
        })
        .await;
}

/// A connected Edge must turn a denied optimistic write into a terminal Global
/// rejection, and the rejected row must never appear to a client observing
/// Core directly.
#[tokio::test(flavor = "current_thread")]
async fn edge_to_core_relay_returns_global_rejection() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let core = tokio::time::timeout(Duration::from_secs(10), JazzServer::start())
                .await
                .expect("denial Core start timed out");
            let app_id = core.app_id();
            push_catalogue_in_memory(
                core.server_state(),
                app_id,
                "dev",
                std::slice::from_ref(&schema),
                &[],
            )
            .await
            .expect("publish denial schema catalogue");
            publish_permissions(
                &core.base_url(),
                app_id,
                core.admin_secret(),
                &schema,
                rejected_todo_permissions(&schema),
                None,
            )
            .await;
            let direct_denied = tokio::time::timeout(
                Duration::from_secs(15),
                TestingClient::builder()
                    .with_server(&core)
                    .with_schema(schema.clone())
                    .with_user_id("direct-core-denial-precondition")
                    .as_user()
                    .connect(),
            )
            .await
            .expect("direct Core denial client connection timed out");
            let (_, _, direct_tx) = direct_denied
                .insert(
                    "todos",
                    row_input!("title" => "direct denial precondition", "done" => false),
                )
                .expect("direct Core client stages denial precondition");
            let direct_tx = direct_tx.expect("direct denial precondition commits locally");
            tokio::time::timeout(
                Duration::from_secs(15),
                direct_denied.wait_for_transaction(direct_tx, DurabilityTier::GlobalServer),
            )
            .await
            .expect("direct Core denial precondition waiter timed out")
            .expect_err("published Core permissions must deny a direct user insert");
            tokio::time::timeout(Duration::from_secs(5), direct_denied.shutdown())
                .await
                .expect("direct Core denial client shutdown timed out")
                .expect("shutdown direct Core denial client");

            let edge = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .with_upstream_url(core.base_url())
                .start()
                .await;
            let edge_state = edge.server_state();
            tokio::time::timeout(Duration::from_secs(5), async {
                while edge_state.edge_upstream_health()
                    != jazz_server::EdgeUpstreamHealth::Connected
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("edge establishes its ordinary upstream before the rejection receipt");

            let alice = TestingClient::builder()
                .with_server(&edge)
                .with_schema(schema.clone())
                .with_user_id("alice-rejected-at-core")
                .as_user()
                .connect()
                .await;
            let core_observer = TestingClient::builder()
                .with_server(&core)
                .with_schema(schema)
                .with_user_id("direct-core-rejection-observer")
                .as_user()
                .connect()
                .await;
            let (rejected_todo, _, rejected_tx) = alice
                .insert(
                    "todos",
                    row_input!("title" => "must be rejected", "done" => false),
                )
                .expect("the client stages the optimistic write");
            let rejected_tx = rejected_tx.expect("optimistic mutation commits locally");

            let rejection = tokio::time::timeout(
                Duration::from_secs(15),
                alice.wait_for_transaction(rejected_tx, DurabilityTier::GlobalServer),
            )
            .await
            .expect("denied write's Global waiter timed out");
            rejection.expect_err("the denied write resolves the global waiter with rejection");
            let core_rows = core_observer
                .query(todo_query(), Some(DurabilityTier::GlobalServer))
                .await
                .expect("query Core after the global rejection");
            assert!(
                core_rows.iter().all(|(id, _)| *id != rejected_todo),
                "authority-rejected row must not appear at Core"
            );

            core_observer
                .shutdown()
                .await
                .expect("shutdown direct core observer");
            alice.shutdown().await.expect("shutdown alice");
            edge.shutdown().await;
            core.shutdown().await;
        })
        .await;
}

/// An unavailable Core does not prevent a fixed-schema Edge from accepting at
/// its own durability tier. Once Core appears, the retained write propagates,
/// materializes at Core, and resolves its exact Global waiter.
#[tokio::test(flavor = "current_thread")]
async fn edge_to_core_relay_retains_write_while_upstream_is_unavailable() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let app_id = AppId::random();
            let core_port = reserve_local_port();
            let edge = tokio::time::timeout(
                Duration::from_secs(10),
                JazzServer::builder()
                    .with_app_id(app_id)
                    .with_schema(schema.clone())
                    .with_native_transport_connector(jazz_testkit::native_connector())
                    .with_upstream_url(format!("http://127.0.0.1:{core_port}"))
                    .start(),
            )
            .await
            .expect("offline-ready Edge start timed out");
            // `connect_user` waits for a settled remote query, which is
            // intentionally unavailable in this scenario. The fixed-schema
            // client can connect and exercise write durability without that
            // unrelated read precondition.
            let alice = tokio::time::timeout(
                Duration::from_secs(15),
                jazz_testkit::connect(
                    edge.make_client_context_for_user(schema.clone(), "alice-upstream-unavailable"),
                ),
            )
            .await
            .expect("offline-ready Alice connection timed out")
            .expect("connect Alice to offline-ready Edge");
            let (todo_id, expected, tx) = alice
                .insert(
                    "todos",
                    row_input!("title" => "retained without core", "done" => false),
                )
                .expect("edge accepts while its upstream is unavailable");
            let tx = tx.expect("ordinary mutation commits immediately");
            tokio::time::timeout(
                Duration::from_secs(15),
                support::wait_for_edge_txs(&alice, &[tx]),
            )
            .await
            .expect("offline-ready Edge durability wait timed out");
            assert_ne!(
                edge.server_state().edge_upstream_health(),
                jazz_server::EdgeUpstreamHealth::Connected,
                "the edge-tier receipt precedes any upstream connection"
            );

            let core = tokio::time::timeout(
                Duration::from_secs(10),
                JazzServer::builder()
                    .with_app_id(app_id)
                    .with_port(core_port)
                    .with_schema(schema.clone())
                    .start(),
            )
            .await
            .expect("newly available Core start timed out");

            let edge_state = edge.server_state();
            tokio::time::timeout(Duration::from_secs(5), async {
                while edge_state.edge_upstream_health()
                    != jazz_server::EdgeUpstreamHealth::Connected
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("offline-ready Edge did not attach the newly available Core");
            let core_observer = tokio::time::timeout(
                Duration::from_secs(15),
                connect_user(&core, schema, "direct-unavailable-core-observer"),
            )
            .await
            .expect("newly available Core observer connection/readiness timed out");

            tokio::time::timeout(
                Duration::from_secs(15),
                alice.wait_for_transaction(tx, DurabilityTier::GlobalServer),
            )
            .await
            .expect("unavailable-upstream Global waiter timed out")
            .expect("retained edge write settles when core becomes available");
            tokio::time::timeout(
                Duration::from_secs(15),
                wait_for_row(
                    &core_observer,
                    DurabilityTier::GlobalServer,
                    todo_id,
                    expected,
                    "newly available core materializes the retained edge write",
                ),
            )
            .await
            .expect("newly available direct-Core visibility wait timed out");

            tokio::time::timeout(Duration::from_secs(5), core_observer.shutdown())
                .await
                .expect("newly available Core observer shutdown timed out")
                .expect("shutdown direct core observer");
            tokio::time::timeout(Duration::from_secs(5), alice.shutdown())
                .await
                .expect("offline-ready Alice shutdown timed out")
                .expect("shutdown alice");
            tokio::time::timeout(Duration::from_secs(15), edge.shutdown())
                .await
                .expect("offline-ready Edge shutdown timed out");
            tokio::time::timeout(Duration::from_secs(15), core.shutdown())
                .await
                .expect("newly available Core shutdown timed out");
        })
        .await;
}

/// A core-origin write reaches subscribers connected through two independent
/// edge servers.
///
/// Actors: carol writes directly to `core`; alice is connected to `edge_us`
/// and bob to `edge_eu`.
///
/// ```text
///                 /--upstream--> edge_us --> alice
/// carol --> core -|
///                 \--upstream--> edge_eu --> bob
/// ```
#[tokio::test(flavor = "current_thread")]
async fn core_write_reaches_clients_on_both_edges() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let app_id = AppId::random();
            let core = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .start()
                .await;
            let edge_us = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .with_upstream_url(core.base_url())
                .start()
                .await;
            let edge_eu = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .with_upstream_url(core.base_url())
                .start()
                .await;

            let alice = connect_user(&edge_us, schema.clone(), "alice-edge-us").await;
            let bob = connect_user(&edge_eu, schema.clone(), "bob-edge-eu").await;
            let carol = connect_user(&core, schema, "carol-core").await;
            let mut alice_stream = alice
                .subscribe(todo_query())
                .await
                .expect("alice subscribes through edge_us");
            let mut bob_stream = bob
                .subscribe(todo_query())
                .await
                .expect("bob subscribes through edge_eu");
            let mut alice_log = Vec::new();
            let mut bob_log = Vec::new();

            let (todo_id, expected, transaction_id) = carol
                .insert(
                    "todos",
                    row_input!("title" => "core write for both edges", "done" => false),
                )
                .expect("carol writes directly to core");
            carol
                .wait_for_transaction(
                    transaction_id.expect("ordinary mutation commits immediately"),
                    DurabilityTier::GlobalServer,
                )
                .await
                .expect("core write settles globally");

            wait_for_subscription_update(
                &mut alice_stream,
                &mut alice_log,
                Duration::from_secs(30),
                "alice receives the core write through edge_us",
                |deltas| has_added_id(deltas, todo_id),
            )
            .await;
            wait_for_subscription_update(
                &mut bob_stream,
                &mut bob_log,
                Duration::from_secs(30),
                "bob receives the core write through edge_eu",
                |deltas| has_added_id(deltas, todo_id),
            )
            .await;
            wait_for_row(
                &alice,
                DurabilityTier::EdgeServer,
                todo_id,
                expected.clone(),
                "alice's edge query contains the core write",
            )
            .await;
            wait_for_row(
                &bob,
                DurabilityTier::EdgeServer,
                todo_id,
                expected,
                "bob's edge query contains the core write",
            )
            .await;

            carol.shutdown().await.expect("shutdown carol");
            bob.shutdown().await.expect("shutdown bob");
            alice.shutdown().await.expect("shutdown alice");
            edge_eu.shutdown().await;
            edge_us.shutdown().await;
            core.shutdown().await;
        })
        .await;
}

/// A write accepted through one edge becomes globally visible through a peer
/// edge, including its existing subscription.
///
/// Actors: alice writes through `edge_us`; bob is subscribed through
/// `edge_eu`.
///
/// ```text
/// alice --> edge_us --upstream--> core --upstream--> edge_eu --> bob
/// ```
#[tokio::test(flavor = "current_thread")]
#[ignore = "#1787: manual peer-edge delivery topology canary; deterministic no-retry regression covers the scheduling defect"]
async fn edge_write_reaches_client_on_peer_edge() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let app_id = AppId::random();
            let core = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .start()
                .await;
            let edge_us = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .with_upstream_url(core.base_url())
                .start()
                .await;
            let edge_eu = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .with_upstream_url(core.base_url())
                .start()
                .await;

            let alice = connect_user(&edge_us, schema.clone(), "alice-edge-us-writer").await;
            let bob = connect_user(&edge_eu, schema, "bob-edge-eu-reader").await;
            let mut bob_stream = bob
                .subscribe(todo_query())
                .await
                .expect("bob subscribes through edge_eu");
            let mut bob_log = Vec::new();

            let (todo_id, expected, transaction_id) = alice
                .insert(
                    "todos",
                    row_input!("title" => "edge write for peer", "done" => true),
                )
                .expect("alice writes through edge_us");
            alice
                .wait_for_transaction(
                    transaction_id.expect("ordinary mutation commits immediately"),
                    DurabilityTier::GlobalServer,
                )
                .await
                .expect("edge_us write settles globally through core");

            wait_for_subscription_update(
                &mut bob_stream,
                &mut bob_log,
                Duration::from_secs(30),
                "bob receives edge_us write through edge_eu",
                |deltas| has_added_id(deltas, todo_id),
            )
            .await;
            wait_for_row(
                &bob,
                DurabilityTier::EdgeServer,
                todo_id,
                expected,
                "bob's edge query contains the peer-edge write",
            )
            .await;

            bob.shutdown().await.expect("shutdown bob");
            alice.shutdown().await.expect("shutdown alice");
            edge_eu.shutdown().await;
            edge_us.shutdown().await;
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
            coverage: "clients_sync::wait_for_transaction_reaches_edge_and_global_tiers",
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
            topology: Topology::ClientEdgeCore,
            scenario: Scenario::MergeableWrite,
            coverage: "edge_server_mode::edge_to_core_relay_propagates_settles_and_reconnects_after_established_drop",
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
