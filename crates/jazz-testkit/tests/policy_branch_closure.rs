use jazz_testkit as support;

use std::time::Duration;

use jazz::row_input;
use jazz::tools::public_schema::{
    RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind, RelKeyRef, RelPredicateCmpOp,
    RelPredicateExpr, RelRecursionBound, RelValueRef, RowIdRef, TablePolicies,
};
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, PolicyExpr, Schema, SchemaBuilder, Session,
    TableSchema, Value,
};
use jazz_server::JazzServer;
use support::{TestingClient, wait_for_query};

const MEMBER_ID: &str = "00000000-0000-4000-8000-0000000000b0";

fn member_identity() -> String {
    Session::new("urn:jazz:test", MEMBER_ID)
        .author_subject()
        .expect("member identity")
        .canonical()
        .to_owned()
}

#[derive(Clone, Copy)]
enum BranchOrder {
    PlainFirst,
    GatherFirst,
    ThreeBranchMix,
}

fn policy_branch_closure_schema(order: BranchOrder) -> Schema {
    let plain_branch = PolicyExpr::eq_session("direct_user_id", vec!["user".to_owned()]);
    let gather_branch = gathered_resource_access_policy();
    let third_branch = PolicyExpr::eq_literal("route_key", Value::Text("third".to_owned()));
    let branches = match order {
        BranchOrder::PlainFirst => vec![plain_branch, gather_branch],
        BranchOrder::GatherFirst => vec![gather_branch, plain_branch],
        BranchOrder::ThreeBranchMix => vec![plain_branch, gather_branch, third_branch],
    };

    SchemaBuilder::new()
        .table(
            TableSchema::builder("resources")
                .column("label", ColumnType::Text)
                .column("direct_user_id", ColumnType::Text)
                .column("route_key", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::Or(branches))
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

fn gathered_resource_access_policy() -> PolicyExpr {
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
                            right: RelValueRef::SessionRef(vec!["user".to_owned()]),
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

async fn seed_policy_branch_closure_rows(admin: &JazzClient) {
    let (seed_team, _, seed_tx) = admin
        .insert("teams", row_input!("identity_key" => member_identity()))
        .expect("insert member seed team");
    let (parent_team, _, parent_tx) = admin
        .insert("teams", row_input!("identity_key" => "unrelated-parent"))
        .expect("insert parent team");
    let (_, _, team_edge_tx) = admin
        .insert(
            "team_team_edges",
            row_input!("child_team" => seed_team, "parent_team" => parent_team),
        )
        .expect("insert team closure edge");

    let (_, _, plain_tx) = admin
        .insert(
            "resources",
            row_input!(
                "label" => "plain",
                "direct_user_id" => member_identity(),
                "route_key" => "plain"
            ),
        )
        .expect("insert plain-visible resource");
    let (gathered, _, gathered_tx) = admin
        .insert(
            "resources",
            row_input!(
                "label" => "gathered",
                "direct_user_id" => "nobody",
                "route_key" => "gathered"
            ),
        )
        .expect("insert gather-visible resource");
    let (_, _, third_tx) = admin
        .insert(
            "resources",
            row_input!(
                "label" => "third",
                "direct_user_id" => "nobody",
                "route_key" => "third"
            ),
        )
        .expect("insert third-branch resource");
    let (hidden, _, hidden_tx) = admin
        .insert(
            "resources",
            row_input!(
                "label" => "hidden",
                "direct_user_id" => "nobody",
                "route_key" => "hidden"
            ),
        )
        .expect("insert hidden resource");

    let (_, _, access_tx) = admin
        .insert(
            "resource_access_edges",
            row_input!("resource" => gathered, "team" => parent_team, "grant_role" => "viewer"),
        )
        .expect("insert gathered resource access edge");
    let (_, _, hidden_access_tx) = admin
        .insert(
            "resource_access_edges",
            row_input!("resource" => hidden, "team" => seed_team, "grant_role" => "blocked"),
        )
        .expect("insert non-matching hidden resource access edge");
    support::wait_for_edge_txs(
        admin,
        &[
            seed_tx.expect("ordinary mutation commits immediately"),
            parent_tx.expect("ordinary mutation commits immediately"),
            team_edge_tx.expect("ordinary mutation commits immediately"),
            plain_tx.expect("ordinary mutation commits immediately"),
            gathered_tx.expect("ordinary mutation commits immediately"),
            third_tx.expect("ordinary mutation commits immediately"),
            hidden_tx.expect("ordinary mutation commits immediately"),
            access_tx.expect("ordinary mutation commits immediately"),
            hidden_access_tx.expect("ordinary mutation commits immediately"),
        ],
    )
    .await;
}

async fn visible_resource_labels(client: &JazzClient, expected_labels: &[&str]) -> Vec<String> {
    let rows = wait_for_query(
        client,
        jazz::query::Query::from("resources").select(["label"]),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(5),
        "member sees all resource policy branch results",
        |rows| {
            let labels = labels_from_rows(rows.clone());
            expected_labels
                .iter()
                .all(|expected| labels.iter().any(|label| label == expected))
                .then_some(rows)
        },
    )
    .await;
    labels_from_rows(rows)
}

fn labels_from_rows(mut rows: Vec<(jazz::tools::ObjectId, Vec<Value>)>) -> Vec<String> {
    let mut labels = rows
        .drain(..)
        .map(|(_, values)| match values.as_slice() {
            [Value::Text(label)] => label.clone(),
            other => panic!("unexpected resource projection: {other:?}"),
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

async fn assert_policy_branch_closure(order: BranchOrder, expected_labels: Vec<&str>) {
    let schema = policy_branch_closure_schema(order);
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("00000000-0000-4000-8000-0000000000a0")
        .as_admin()
        .ready_on("resources", Duration::from_secs(30))
        .connect()
        .await;
    seed_policy_branch_closure_rows(&admin).await;
    let member = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id(MEMBER_ID)
        .ready_on("resources", Duration::from_secs(30))
        .connect()
        .await;

    let labels = visible_resource_labels(&member, &expected_labels).await;
    assert_eq!(labels, expected_labels);

    member.shutdown().await.expect("shutdown member");
    admin.shutdown().await.expect("shutdown admin");
    server.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn mixed_policy_branch_union_keeps_gather_results_in_all_orders() {
    tokio::task::LocalSet::new()
        .run_until(async {
            assert_policy_branch_closure(BranchOrder::PlainFirst, vec!["gathered", "plain"]).await;
            assert_policy_branch_closure(BranchOrder::GatherFirst, vec!["gathered", "plain"]).await;
            assert_policy_branch_closure(
                BranchOrder::ThreeBranchMix,
                vec!["gathered", "plain", "third"],
            )
            .await;
        })
        .await;
}
