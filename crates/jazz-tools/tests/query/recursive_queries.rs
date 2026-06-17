#![cfg(feature = "test")]

use std::time::Duration;

use crate::support::{
    QueryRows, TestingClient, collect_stream_deltas, has_added, wait_for_rows,
    wait_for_subscription_update,
};
use jazz_tools::row_input;
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(100);

fn integer_frontier_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("teams").column("team_id", ColumnType::Integer))
        .table(
            TableSchema::builder("team_edges")
                .column("child_team", ColumnType::Integer)
                .column("parent_team", ColumnType::Integer),
        )
        .build()
}

fn team_graph_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("teams")
                .column("name", ColumnType::Text)
                .nullable_fk_column("parent_id", "teams"),
        )
        .table(
            TableSchema::builder("team_edges")
                .column("child_team", ColumnType::Uuid)
                .column("parent_team", ColumnType::Uuid),
        )
        .build()
}

struct Clients {
    server: TestingServer,
    alice: JazzClient,
    bob: JazzClient,
}

impl Clients {
    async fn start(schema: Schema) -> Self {
        let server = TestingServer::start_with_schema(schema.clone()).await;
        let alice = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema.clone())
            .with_user_id("recursive-query-alice")
            .ready_on("teams", READY_TIMEOUT)
            .connect()
            .await;
        let bob = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema)
            .with_user_id("recursive-query-bob")
            .ready_on("teams", READY_TIMEOUT)
            .connect()
            .await;

        Self { server, alice, bob }
    }

    async fn shutdown(self) {
        self.alice.shutdown().await.expect("shutdown alice");
        self.bob.shutdown().await.expect("shutdown bob");
        self.server.shutdown().await;
    }
}

async fn create_numbered_team(client: &JazzClient, team_id: i32) -> ObjectId {
    client
        .insert("teams", row_input!("team_id" => team_id))
        .expect("create numbered team")
        .0
}

async fn create_numbered_team_edge(client: &JazzClient, child_team: i32, parent_team: i32) {
    client
        .insert(
            "team_edges",
            row_input!("child_team" => child_team, "parent_team" => parent_team),
        )
        .expect("create numbered team edge");
}

async fn create_team(client: &JazzClient, name: &str, parent_id: Option<ObjectId>) -> ObjectId {
    client
        .insert(
            "teams",
            row_input!("name" => name, "parent_id" => parent_id),
        )
        .expect("create team")
        .0
}

async fn create_team_edge(client: &JazzClient, child_team: ObjectId, parent_team: ObjectId) {
    client
        .insert(
            "team_edges",
            row_input!("child_team" => child_team, "parent_team" => parent_team),
        )
        .expect("create team edge");
}

fn sorted_integer_frontier_values(rows: &QueryRows) -> Vec<i32> {
    let mut values = rows
        .iter()
        .filter_map(|(_, values)| match values.first() {
            Some(Value::Integer(team_id)) => Some(*team_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn sorted_team_names(rows: &QueryRows) -> Vec<String> {
    let mut names = rows
        .iter()
        .filter_map(|(_, values)| match values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

/// Verifies that a recursive gather query seeds on a matching row and
/// transitively reaches all ancestor rows by following the edge table.
///
/// Actors and flow:
///
/// alice writes leaf -> mid -> root in `team_edges`
/// bob subscribes to the recursive query from leaf and sees all three teams
#[tokio::test]
async fn recursive_gather_query_returns_seed_and_ancestors_from_edge_table() {
    let clients = Clients::start(team_graph_schema()).await;
    let query = QueryBuilder::new("teams")
        .filter_eq("name", Value::Text("leaf".to_string()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();

    let mut stream = clients
        .bob
        .subscribe(query.clone())
        .await
        .expect("subscribe to recursive gather query");
    let mut log = Vec::new();

    let root_id = create_team(&clients.alice, "root", None).await;
    let mid_id = create_team(&clients.alice, "mid", None).await;
    let leaf_id = create_team(&clients.alice, "leaf", None).await;
    create_team_edge(&clients.alice, leaf_id, mid_id).await;
    create_team_edge(&clients.alice, mid_id, root_id).await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "recursive gather add delta",
        |log| has_added(log, leaf_id),
    )
    .await;

    let rows = wait_for_rows(&clients.bob, query, "recursive gather rows", |rows| {
        (sorted_team_names(&rows) == vec!["leaf", "mid", "root"]).then_some(rows)
    })
    .await;
    assert_eq!(sorted_team_names(&rows), vec!["leaf", "mid", "root"]);

    clients.shutdown().await;
}

/// Verifies that recursive gather can use a scalar column frontier and dedupe a
/// cycle without requiring every reached value to be backed by a row in the seed
/// table.
///
/// Actors and flow:
///
/// alice writes team 1 plus cyclic edges 1 -> 2 -> 3 -> 1
/// bob queries from seed team_id=1 and sees the recursive closure {1, 2, 3}
#[tokio::test]
async fn recursive_query_expands_column_frontier_through_cycle() {
    let clients = Clients::start(integer_frontier_schema()).await;

    create_numbered_team(&clients.alice, 1).await;
    create_numbered_team_edge(&clients.alice, 1, 2).await;
    create_numbered_team_edge(&clients.alice, 2, 3).await;
    create_numbered_team_edge(&clients.alice, 3, 1).await;

    let query = QueryBuilder::new("teams")
        .select(&["team_id"])
        .filter_eq("team_id", Value::Integer(1))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "team_id")
                .select(&["parent_team"])
                .max_depth(10)
        })
        .build();

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees recursive integer closure",
        |rows| (sorted_integer_frontier_values(&rows) == vec![1, 2, 3]).then_some(rows),
    )
    .await;
    assert_eq!(sorted_integer_frontier_values(&rows), vec![1, 2, 3]);

    clients.shutdown().await;
}

/// Verifies that a recursive hop subscription emits a live add when a new edge
/// extends an already-subscribed closure.
///
/// Actors and flow:
///
/// alice writes team-1 -> team-2, bob subscribes from team-1
/// alice adds team-2 -> team-3, bob receives team-3 and the query has all teams
#[tokio::test]
async fn recursive_hop_subscription_updates_when_new_edge_extends_closure() {
    let clients = Clients::start(team_graph_schema()).await;

    let team1 = create_team(&clients.alice, "team-1", None).await;
    let team2 = create_team(&clients.alice, "team-2", None).await;
    let team3 = create_team(&clients.alice, "team-3", None).await;
    create_team_edge(&clients.alice, team1, team2).await;

    let query = QueryBuilder::new("teams")
        .filter_eq("name", Value::Text("team-1".to_string()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();

    wait_for_rows(
        &clients.bob,
        query.clone(),
        "bob sees initial recursive hop closure",
        |rows| (sorted_team_names(&rows) == vec!["team-1", "team-2"]).then_some(()),
    )
    .await;

    let mut stream = clients
        .bob
        .subscribe(query.clone())
        .await
        .expect("subscribe");
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "initial recursive closure add",
        |log| has_added(log, team2),
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    create_team_edge(&clients.alice, team2, team3).await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "team-3 add after recursive edge insert",
        |log| has_added(log, team3),
    )
    .await;

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees expanded recursive hop closure",
        |rows| (sorted_team_names(&rows) == vec!["team-1", "team-2", "team-3"]).then_some(rows),
    )
    .await;
    assert_eq!(sorted_team_names(&rows), vec!["team-1", "team-2", "team-3"]);

    clients.shutdown().await;
}

/// Verifies that recursive gather can traverse a self-referential parent
/// foreign key using the public query builder.
///
/// Actors and flow:
///
/// alice writes root <- mid <- leaf
/// bob seeds on leaf and follows parent_id until root, seeing all ancestors
#[tokio::test]
async fn recursive_query_expands_self_parent_ancestors() {
    let clients = Clients::start(team_graph_schema()).await;

    let root = create_team(&clients.alice, "root", None).await;
    let mid = create_team(&clients.alice, "mid", Some(root)).await;
    let _leaf = create_team(&clients.alice, "leaf", Some(mid)).await;

    let query = QueryBuilder::new("teams")
        .filter_eq("name", Value::Text("leaf".to_string()))
        .with_recursive(|r| {
            r.from("teams")
                .correlate("id", "parent_id")
                .select(&["name", "parent_id"])
                .max_depth(10)
        })
        .build();

    let rows = wait_for_rows(
        &clients.bob,
        query,
        "bob sees self-parent ancestor closure",
        |rows| (sorted_team_names(&rows) == vec!["leaf", "mid", "root"]).then_some(rows),
    )
    .await;

    assert_eq!(sorted_team_names(&rows), vec!["leaf", "mid", "root"]);

    clients.shutdown().await;
}
