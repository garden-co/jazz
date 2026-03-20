#![cfg(feature = "test-utils")]

mod support;

use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, JazzClient, ObjectId, Query, QueryBuilder, Schema, SchemaBuilder, TableSchema,
    Value,
};
use support::{
    collect_stream_deltas, connect_admin_client, has_added, has_any_change, has_removed,
    has_updated, wait_for_rows, wait_for_subscription_update,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const NO_DELTA_WINDOW: Duration = Duration::from_millis(500);

fn subscription_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("orgs").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("teams")
                .column("name", ColumnType::Text)
                .nullable_fk_column("org_id", "orgs")
                .nullable_fk_column("parent_id", "teams"),
        )
        .table(
            TableSchema::builder("team_edges")
                .column("child_team", ColumnType::Uuid)
                .column("parent_team", ColumnType::Uuid),
        )
        .table(
            TableSchema::builder("users")
                .column("name", ColumnType::Text)
                .nullable_fk_column("team_id", "teams"),
        )
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .nullable_column("priority", ColumnType::Integer)
                .nullable_fk_column("owner_id", "users")
                .column(
                    "tags",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Text),
                    },
                )
                .nullable_column("payload", ColumnType::Bytea),
        )
        .table(TableSchema::builder("file_parts").column("label", ColumnType::Text))
        .table(
            TableSchema::builder("files")
                .column("name", ColumnType::Text)
                .column(
                    "parts",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Uuid),
                    },
                ),
        )
        .build()
}

struct ClientPair {
    server: TestingServer,
    writer: JazzClient,
    subscriber: JazzClient,
}

impl ClientPair {
    async fn start() -> Self {
        let server = TestingServer::start().await;
        let schema = subscription_schema();
        let writer = connect_admin_client(
            &server,
            schema.clone(),
            "subscribe-all-writer",
            "todos",
            READY_TIMEOUT,
        )
        .await;
        let subscriber = connect_admin_client(
            &server,
            schema,
            "subscribe-all-subscriber",
            "todos",
            READY_TIMEOUT,
        )
        .await;

        Self {
            server,
            writer,
            subscriber,
        }
    }

    async fn shutdown(self) {
        self.writer.shutdown().await.expect("shutdown writer");
        self.subscriber
            .shutdown()
            .await
            .expect("shutdown subscriber");
        self.server.shutdown().await;
    }
}

#[derive(Clone, Copy)]
struct TodoSeed {
    title: &'static str,
    done: bool,
    priority: Option<i32>,
    tags: &'static [&'static str],
    payload: Option<&'static [u8]>,
}

impl TodoSeed {
    fn values(self) -> Vec<Value> {
        vec![
            Value::Text(self.title.to_string()),
            Value::Boolean(self.done),
            self.priority.map(Value::Integer).unwrap_or(Value::Null),
            Value::Null,
            Value::Array(
                self.tags
                    .iter()
                    .map(|tag| Value::Text((*tag).to_string()))
                    .collect(),
            ),
            self.payload
                .map(|bytes| Value::Bytea(bytes.to_vec()))
                .unwrap_or(Value::Null),
        ]
    }
}

async fn create_org(client: &JazzClient, name: &str) -> ObjectId {
    client
        .create("orgs", vec![Value::Text(name.to_string())])
        .await
        .expect("create org")
        .0
}

async fn create_team(
    client: &JazzClient,
    name: &str,
    org_id: Option<ObjectId>,
    parent_id: Option<ObjectId>,
) -> ObjectId {
    client
        .create(
            "teams",
            vec![
                Value::Text(name.to_string()),
                org_id.map(Value::Uuid).unwrap_or(Value::Null),
                parent_id.map(Value::Uuid).unwrap_or(Value::Null),
            ],
        )
        .await
        .expect("create team")
        .0
}

async fn create_user(client: &JazzClient, name: &str, team_id: Option<ObjectId>) -> ObjectId {
    client
        .create(
            "users",
            vec![
                Value::Text(name.to_string()),
                team_id.map(Value::Uuid).unwrap_or(Value::Null),
            ],
        )
        .await
        .expect("create user")
        .0
}

async fn create_team_edge(
    client: &JazzClient,
    child_team: ObjectId,
    parent_team: ObjectId,
) -> ObjectId {
    client
        .create(
            "team_edges",
            vec![Value::Uuid(child_team), Value::Uuid(parent_team)],
        )
        .await
        .expect("create team edge")
        .0
}

async fn create_todo(client: &JazzClient, seed: TodoSeed) -> ObjectId {
    client
        .create("todos", seed.values())
        .await
        .expect("create todo")
        .0
}

async fn create_file_part(client: &JazzClient, label: &str) -> ObjectId {
    client
        .create("file_parts", vec![Value::Text(label.to_string())])
        .await
        .expect("create file part")
        .0
}

async fn create_file(client: &JazzClient, name: &str, parts: &[ObjectId]) -> ObjectId {
    client
        .create(
            "files",
            vec![
                Value::Text(name.to_string()),
                Value::Array(parts.iter().copied().map(Value::Uuid).collect()),
            ],
        )
        .await
        .expect("create file")
        .0
}

/// Verifies that a subscription emits add, update, and remove deltas as a row
/// enters, changes within, and leaves the query result set, and that the
/// materialized query result stays consistent throughout.
///
/// The writer creates a todo filtered by `done=false`, updates its title, then
/// marks it done. The subscriber observes the full lifecycle. Setting `done=true`
/// moves the row out of the filter, which must produce a remove delta.
///
/// ```text
/// writer ──insert (done=false)──► server ──► subscriber (add ✓)
/// writer ──update title──────────► server ──► subscriber (update ✓)
/// writer ──update done=true──────► server ──► subscriber (remove ✓)
///                                               query result: empty
/// ```
#[tokio::test]
async fn subscribe_all_emits_add_update_remove_and_tracks_current_results() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to todos");
    let mut log = Vec::new();

    let todo_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "watch-me",
            done: false,
            priority: Some(1),
            tags: &["x"],
            payload: None,
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "todo add delta",
        |log| has_added(log, todo_id),
    )
    .await;

    pair.writer
        .update(
            todo_id,
            vec![(
                "title".to_string(),
                Value::Text("watch-me-updated".to_string()),
            )],
        )
        .await
        .expect("update todo title");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "todo update delta",
        |log| has_updated(log, todo_id),
    )
    .await;

    pair.writer
        .update(todo_id, vec![("done".to_string(), Value::Boolean(true))])
        .await
        .expect("mark todo done");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "todo remove delta",
        |log| has_removed(log, todo_id),
    )
    .await;

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "todo removed from current results",
        |rows| (!rows.iter().any(|(id, _)| *id == todo_id)).then_some(rows),
    )
    .await;
    assert!(
        !rows.iter().any(|(id, _)| *id == todo_id),
        "latest query results should no longer include the removed todo"
    );

    pair.shutdown().await;
}

/// Verifies that each supported filter operator emits an add delta and returns
/// the inserted row in query results when a matching row is written.
///
/// Operators covered: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `is_null`,
/// `contains` on an array column, `contains` on a text column (substring),
/// `contains` with an empty string (matches all), and `eq` on a bytea column.
///
/// Each case uses a dedicated subscriber so subscriptions are isolated. A shared
/// writer is reused to reduce server startup overhead. The writer's inserts
/// accumulate across cases, so each subscriber's query result may contain rows
/// from earlier cases — assertions only check for the presence of the expected row,
/// not an exact total count.
#[tokio::test]
async fn subscribe_all_supports_condition_filters() {
    struct ConditionCase {
        name: &'static str,
        query: Query,
        insert: TodoSeed,
    }

    let server = TestingServer::start().await;
    let schema = subscription_schema();
    let writer = connect_admin_client(
        &server,
        schema.clone(),
        "condition-writer",
        "todos",
        READY_TIMEOUT,
    )
    .await;

    let cases = vec![
        ConditionCase {
            name: "eq",
            query: QueryBuilder::new("todos")
                .filter_eq("title", Value::Text("eq-hit".to_string()))
                .build(),
            insert: TodoSeed {
                title: "eq-hit",
                done: false,
                priority: Some(1),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "ne",
            query: QueryBuilder::new("todos")
                .filter_ne("title", Value::Text("blocked".to_string()))
                .build(),
            insert: TodoSeed {
                title: "ne-hit",
                done: false,
                priority: Some(2),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "gt",
            query: QueryBuilder::new("todos")
                .filter_gt("priority", Value::Integer(10))
                .build(),
            insert: TodoSeed {
                title: "gt-hit",
                done: false,
                priority: Some(11),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "gte",
            query: QueryBuilder::new("todos")
                .filter_ge("priority", Value::Integer(10))
                .build(),
            insert: TodoSeed {
                title: "gte-hit",
                done: false,
                priority: Some(10),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "lt",
            query: QueryBuilder::new("todos")
                .filter_lt("priority", Value::Integer(0))
                .build(),
            insert: TodoSeed {
                title: "lt-hit",
                done: false,
                priority: Some(-1),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "lte",
            query: QueryBuilder::new("todos")
                .filter_le("priority", Value::Integer(0))
                .build(),
            insert: TodoSeed {
                title: "lte-hit",
                done: false,
                priority: Some(0),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "is_null",
            query: QueryBuilder::new("todos")
                .filter_is_null("priority")
                .build(),
            insert: TodoSeed {
                title: "null-hit",
                done: false,
                priority: None,
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "contains_array",
            query: QueryBuilder::new("todos")
                .filter_contains("tags", Value::Text("needle".to_string()))
                .build(),
            insert: TodoSeed {
                title: "contains-array-hit",
                done: false,
                priority: Some(1),
                tags: &["needle", "hay"],
                payload: None,
            },
        },
        ConditionCase {
            name: "contains_text",
            query: QueryBuilder::new("todos")
                .filter_contains("title", Value::Text("needle".to_string()))
                .build(),
            insert: TodoSeed {
                title: "hay-needle-title",
                done: false,
                priority: Some(1),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "contains_text_empty",
            query: QueryBuilder::new("todos")
                .filter_contains("title", Value::Text(String::new()))
                .build(),
            insert: TodoSeed {
                title: "any-title",
                done: false,
                priority: Some(1),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "eq_bytea",
            query: QueryBuilder::new("todos")
                .filter_eq("payload", Value::Bytea(vec![1, 2, 3]))
                .build(),
            insert: TodoSeed {
                title: "eq-bytea-hit",
                done: false,
                priority: Some(1),
                tags: &["x"],
                payload: Some(&[1, 2, 3]),
            },
        },
    ];

    for case in cases {
        let subscriber = connect_admin_client(
            &server,
            schema.clone(),
            &format!("condition-subscriber-{}", case.name),
            "todos",
            READY_TIMEOUT,
        )
        .await;
        let mut stream = subscriber
            .subscribe(case.query.clone())
            .await
            .expect("subscribe for condition case");
        let mut log = Vec::new();

        let inserted_id = create_todo(&writer, case.insert).await;

        wait_for_subscription_update(
            &mut stream,
            &mut log,
            QUERY_TIMEOUT,
            format!("condition {} add delta", case.name),
            |log| has_added(log, inserted_id),
        )
        .await;

        let rows = wait_for_rows(
            &subscriber,
            case.query,
            format!("condition {} query rows", case.name),
            |rows| {
                rows.iter()
                    .any(|(id, _)| *id == inserted_id)
                    .then_some(rows)
            },
        )
        .await;
        assert!(
            rows.iter().any(|(id, _)| *id == inserted_id),
            "condition {} should include the inserted row",
            case.name
        );

        subscriber
            .shutdown()
            .await
            .expect("shutdown condition subscriber");
    }

    writer.shutdown().await.expect("shutdown condition writer");
    server.shutdown().await;
}

/// Verifies that a `Bytea` column value, including interior zero bytes, survives
/// the write → sync → subscription delta → query result round-trip unmodified.
///
/// The writer inserts a todo with `payload = [9, 8, 7, 0]`. The subscriber
/// receives the add delta and queries the row. The payload byte sequence must
/// be identical at every stage — zero bytes must not truncate the value.
#[tokio::test]
async fn subscribe_all_preserves_bytea_values() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .filter_eq("title", Value::Text("bytes-hit".to_string()))
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to bytea query");
    let mut log = Vec::new();

    let todo_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "bytes-hit",
            done: false,
            priority: Some(1),
            tags: &["x"],
            payload: Some(&[9, 8, 7, 0]),
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "bytea add delta",
        |log| has_added(log, todo_id),
    )
    .await;

    let row = wait_for_rows(&pair.subscriber, query, "bytea query row", |rows| {
        rows.into_iter().find(|(id, _)| *id == todo_id)
    })
    .await;
    assert_eq!(row.1[5], Value::Bytea(vec![9, 8, 7, 0]));

    pair.shutdown().await;
}

/// Verifies that a subscription with `ORDER BY DESC`, `OFFSET 1`, and `LIMIT 1`
/// surfaces the correct single row once enough data is inserted to fill the window.
///
/// The writer inserts todos with priorities 1, 2, and 3. Sorted descending that
/// is [3, 2, 1]; offset 1 skips the highest, limit 1 takes the next. The
/// subscriber must eventually see exactly the priority-2 row as the sole result.
#[tokio::test]
async fn subscribe_all_supports_order_by_limit_and_offset() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .order_by_desc("priority")
        .offset(1)
        .limit(1)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to ordered query");
    let mut log = Vec::new();

    create_todo(
        &pair.writer,
        TodoSeed {
            title: "p1",
            done: false,
            priority: Some(1),
            tags: &["x"],
            payload: None,
        },
    )
    .await;
    create_todo(
        &pair.writer,
        TodoSeed {
            title: "p2",
            done: false,
            priority: Some(2),
            tags: &["x"],
            payload: None,
        },
    )
    .await;
    create_todo(
        &pair.writer,
        TodoSeed {
            title: "p3",
            done: false,
            priority: Some(3),
            tags: &["x"],
            payload: None,
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "ordered query delta",
        |log| log.iter().any(|delta| !delta.is_empty()),
    )
    .await;

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "ordered page current row",
        |rows| {
            (rows.len() == 1 && rows[0].1.first() == Some(&Value::Text("p2".to_string())))
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[0], Value::Text("p2".to_string()));

    pair.shutdown().await;
}

/// Verifies that inserting a row whose text value does not contain the filter
/// string does not emit a spurious add delta on a `contains` subscription.
///
/// The writer inserts a todo with a title that does not include "needle". The
/// subscriber's query result must remain empty and no add delta must appear.
/// An EdgeServer query on the subscriber is used as the causal barrier before
/// draining the stream: once the server confirms the empty result set, any
/// notification it was going to send has already been sent or withheld.
///
/// ```text
/// writer ──insert "completely unrelated"──► server
///                                              │
///                              contains("needle") filter ──✗── subscriber stream (no add)
/// ```
#[tokio::test]
async fn subscribe_all_does_not_emit_add_for_non_matching_contains_query() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .filter_contains("title", Value::Text("needle".to_string()))
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to contains query");
    let mut log = Vec::new();

    let inserted_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "completely unrelated",
            done: false,
            priority: Some(1),
            tags: &["x"],
            payload: None,
        },
    )
    .await;

    // The EdgeServer query returning empty is the causal barrier: by the time
    // the server confirms no matching rows, it has already decided whether to
    // send a subscription notification. The drain then flushes any buffered
    // messages before the negative assertion.
    wait_for_rows(
        &pair.subscriber,
        query,
        "empty contains query results",
        |rows| rows.is_empty().then_some(()),
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;

    assert!(
        !has_added(&log, inserted_id),
        "non-matching text contains insert should not emit an add delta"
    );

    pair.shutdown().await;
}

/// Verifies that a `with_array` projected include delivers a correctly typed
/// sub-array column, starting empty when the parent row has no related rows.
///
/// The query fetches users with an inline `todos_via_owner` array correlated on
/// `owner_id`. The writer creates a user with no todos. The subscriber must
/// receive an add delta for the user and the include column must be an empty
/// array — not null or missing.
#[tokio::test]
async fn subscribe_all_supports_array_include_queries() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("users")
        .with_array("todos_via_owner", |sub| {
            sub.from("todos").correlate("owner_id", "_id")
        })
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to include query");
    let mut log = Vec::new();

    let user_id = create_user(&pair.writer, "Owner", None).await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "include query add delta",
        |log| has_added(log, user_id),
    )
    .await;

    let rows = wait_for_rows(&pair.subscriber, query, "include query row", |rows| {
        rows.iter().any(|(id, _)| *id == user_id).then_some(rows)
    })
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, user_id);
    assert_eq!(rows[0].1[0], Value::Text("Owner".to_string()));
    assert_eq!(rows[0].1[1], Value::Null);
    assert!(
        rows[0].1[2]
            .as_array()
            .expect("include column should be an array")
            .is_empty(),
        "new owner should start with an empty included todos array"
    );

    pair.shutdown().await;
}

/// Verifies that a two-hop projected join traverses users → teams → orgs and
/// emits the org row (result_element_index 2) as the subscription result.
///
/// The writer creates an org, a team in that org, and a user in that team. The
/// subscription must surface the org as an add delta once all three rows are
/// present, and the query result must contain the org's data.
///
/// ```text
/// writer ──create org──────────────────► server
/// writer ──create team (org_id=org)───► server
/// writer ──create user (team_id=team)─► server
///                                          │
///              query: users → teams → orgs [result=orgs]
///                                          │
///                                          └──► subscriber (add delta: org ✓)
/// ```
#[tokio::test]
async fn subscribe_all_supports_hop_queries_via_projected_joins() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("users")
        .join("teams")
        .on("users.team_id", "teams._id")
        .join("orgs")
        .on("teams.org_id", "orgs._id")
        .result_element_index(2)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to hop query");
    let mut log = Vec::new();

    let org_id = create_org(&pair.writer, "Hop Org").await;
    let team_id = create_team(&pair.writer, "Hop Team", Some(org_id), None).await;
    let _user_id = create_user(&pair.writer, "Hop User", Some(team_id)).await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "hop query add delta",
        |log| has_added(log, org_id),
    )
    .await;

    let rows = wait_for_rows(&pair.subscriber, query, "hop query rows", |rows| {
        (rows.len() == 1 && rows[0].0 == org_id).then_some(rows)
    })
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, org_id);
    assert_eq!(rows[0].1[0], Value::Text("Hop Org".to_string()));

    pair.shutdown().await;
}

/// Verifies that updating a scalar foreign key causes a projected join
/// subscription to swap from the old joined row to the new one.
///
/// A user "Mover" starts assigned to Team A. The subscription projects
/// users → teams (result_element_index 1), so it surfaces the team row. The
/// writer reassigns the user to Team B. Both Team A and Team B must appear
/// somewhere in the stream log (one exits, one enters), and the final query
/// result must contain only Team B.
///
/// ```text
/// writer ──create user (team_id=team_a)──► server
/// subscriber query result: [team_a]
///
/// writer ──update user.team_id → team_b──► server ──► subscriber
///   stream: change for team_a AND team_b
///   query result: [team_b]
/// ```
#[tokio::test]
async fn subscribe_all_reacts_to_scalar_fk_updates_in_projected_join_queries() {
    let pair = ClientPair::start().await;

    let org_a = create_org(&pair.writer, "Org A").await;
    let org_b = create_org(&pair.writer, "Org B").await;
    let team_a = create_team(&pair.writer, "Team A", Some(org_a), None).await;
    let team_b = create_team(&pair.writer, "Team B", Some(org_b), None).await;
    let user_id = create_user(&pair.writer, "Mover", Some(team_a)).await;

    let query = QueryBuilder::new("users")
        .join("teams")
        .on("users.team_id", "teams._id")
        .result_element_index(1)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to team hop query");
    let mut log = Vec::new();

    wait_for_rows(
        &pair.subscriber,
        query.clone(),
        "initial team row",
        |rows| (rows.len() == 1 && rows[0].0 == team_a).then_some(()),
    )
    .await;

    pair.writer
        .update(user_id, vec![("team_id".to_string(), Value::Uuid(team_b))])
        .await
        .expect("move user to new team");

    let rows = wait_for_rows(&pair.subscriber, query, "updated team row", |rows| {
        (rows.len() == 1 && rows[0].0 == team_b).then_some(rows)
    })
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, team_b);
    assert_eq!(rows[0].1[0], Value::Text("Team B".to_string()));

    // The query result transitioning to team_b is the causal barrier above.
    // Draining the stream afterwards captures any remaining delta batches
    // before asserting both teams were touched.
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    // Both the old and new joined rows must appear in the log: the join
    // re-evaluated when team_id changed, removing team_a and adding team_b.
    assert!(has_any_change(&log, team_a));
    assert!(has_any_change(&log, team_b));

    pair.shutdown().await;
}

/// Verifies that replacing a UUID array foreign key causes a projected join
/// subscription to react to both the departing and arriving joined rows.
///
/// A file starts with `parts = [part_a]`. The subscription projects
/// files → file_parts (result_element_index 1), surfacing part rows. The
/// writer replaces the array with `[part_b]`. Both part_a and part_b must
/// appear somewhere in the stream log, and the final query result must
/// contain only part_b.
///
/// ```text
/// writer ──create file (parts=[part_a])────► server
/// subscriber query result: [part_a]
///
/// writer ──update file.parts → [part_b]───► server ──► subscriber
///   stream: change for part_a AND part_b
///   query result: [part_b]
/// ```
#[tokio::test]
async fn subscribe_all_reacts_to_uuid_array_fk_updates_in_projected_join_queries() {
    let pair = ClientPair::start().await;

    let part_a = create_file_part(&pair.writer, "A").await;
    let part_b = create_file_part(&pair.writer, "B").await;
    let file_id = create_file(&pair.writer, "File", &[part_a]).await;

    let query = QueryBuilder::new("files")
        .join("file_parts")
        .on("files.parts", "file_parts._id")
        .result_element_index(1)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to file parts hop query");
    let mut log = Vec::new();

    wait_for_rows(
        &pair.subscriber,
        query.clone(),
        "initial file part row",
        |rows| (rows.len() == 1 && rows[0].0 == part_a).then_some(()),
    )
    .await;

    pair.writer
        .update(
            file_id,
            vec![("parts".to_string(), Value::Array(vec![Value::Uuid(part_b)]))],
        )
        .await
        .expect("swap file part ids");

    let rows = wait_for_rows(&pair.subscriber, query, "updated file part row", |rows| {
        (rows.len() == 1 && rows[0].0 == part_b).then_some(rows)
    })
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, part_b);
    assert_eq!(rows[0].1[0], Value::Text("B".to_string()));

    // Same pattern as the scalar FK test: drain after query convergence,
    // then assert both the old part and the new part were affected.
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    assert!(has_any_change(&log, part_a));
    assert!(has_any_change(&log, part_b));

    pair.shutdown().await;
}

/// Verifies that a recursive gather query seeds on a matching row and
/// transitively reaches all ancestor rows by following the edge table.
///
/// Three teams form a chain: leaf → mid → root, connected via `team_edges`
/// (child_team, parent_team). The query seeds on `teams(name="leaf")` and
/// gathers upward. The subscriber must eventually see all three teams in the
/// result set, regardless of insertion order.
///
/// ```text
/// teams:   leaf ──edge──► mid ──edge──► root
///
/// query: seed = teams WHERE name="leaf"
///        gather via team_edges(child_team=_id) → select parent_team → hop teams
///
/// expected result: {leaf, mid, root}
/// ```
#[tokio::test]
async fn subscribe_all_supports_recursive_gather_queries() {
    let pair = ClientPair::start().await;
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

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to recursive gather query");
    let mut log = Vec::new();

    let root_id = create_team(&pair.writer, "root", None, None).await;
    let mid_id = create_team(&pair.writer, "mid", None, None).await;
    let leaf_id = create_team(&pair.writer, "leaf", None, None).await;
    let _edge_one = create_team_edge(&pair.writer, leaf_id, mid_id).await;
    let _edge_two = create_team_edge(&pair.writer, mid_id, root_id).await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "recursive gather add delta",
        |log| has_added(log, leaf_id),
    )
    .await;

    let rows = wait_for_rows(&pair.subscriber, query, "recursive gather rows", |rows| {
        let mut names = rows
            .iter()
            .filter_map(|(_, values)| match values.first() {
                Some(Value::Text(name)) => Some(name.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        names.sort();

        (names == vec!["leaf".to_string(), "mid".to_string(), "root".to_string()]).then_some(rows)
    })
    .await;
    let mut names = rows
        .iter()
        .filter_map(|(_, values)| match values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["leaf", "mid", "root"]);

    pair.shutdown().await;
}
