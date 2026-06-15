#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::query_manager::encoding::decode_row;
use jazz_tools::query_manager::types::RowDescriptor;
use jazz_tools::row_input;
use jazz_tools::server::TestingServer;
use jazz_tools::{
    AppContext, AppId, ClientStorage, ColumnType, JazzClient, ObjectId, OrderedRowDelta, Query,
    QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};
use support::{
    TestingClient, collect_stream_deltas, has_added, has_any_change, has_removed, has_updated,
    wait_for_query, wait_for_rows, wait_for_subscription_update,
};
use tempfile::TempDir;

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
            TableSchema::builder("posts")
                .column("id", ColumnType::Integer)
                .column("title", ColumnType::Text)
                .column("author_name", ColumnType::Text),
        )
        .table(
            TableSchema::builder("comments")
                .column("id", ColumnType::Integer)
                .column("text", ColumnType::Text)
                .column("post_id", ColumnType::Integer),
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
        Self::start_inner(None).await
    }

    async fn start_traced(tracer: &jazz_tools::sync_tracer::SyncTracer) -> Self {
        Self::start_inner(Some(tracer)).await
    }

    async fn start_inner(tracer: Option<&jazz_tools::sync_tracer::SyncTracer>) -> Self {
        let schema = subscription_schema();
        let server = if let Some(t) = tracer {
            TestingServer::builder()
                .with_schema(schema.clone())
                .with_tracer(t.clone())
                .start()
                .await
        } else {
            TestingServer::start_with_schema(schema.clone()).await
        };
        let mut writer_builder = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema.clone())
            .with_user_id("subscribe-all-writer")
            .ready_on("todos", READY_TIMEOUT);
        if let Some(t) = tracer {
            writer_builder = writer_builder.with_tracer(t, "alice");
        }
        let writer = writer_builder.connect().await;

        let mut subscriber_builder = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema)
            .with_user_id("subscribe-all-subscriber")
            .ready_on("todos", READY_TIMEOUT);
        if let Some(t) = tracer {
            subscriber_builder = subscriber_builder.with_tracer(t, "bob");
        }
        let subscriber = subscriber_builder.connect().await;

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
    fn values(self) -> HashMap<String, Value> {
        row_input!(
            "title" => self.title,
            "done" => self.done,
            "priority" => self.priority.map(Value::Integer).unwrap_or(Value::Null),
            "owner_id" => Value::Null,
            "tags" => Value::Array(
                self.tags
                    .iter()
                    .map(|tag| Value::Text((*tag).to_string()))
                    .collect(),
            ),
            "payload" => self
                .payload
                .map(|bytes| Value::Bytea(bytes.to_vec()))
                .unwrap_or(Value::Null),
        )
    }
}

async fn create_org(client: &JazzClient, name: &str) -> ObjectId {
    client
        .insert(
            "orgs",
            HashMap::from([("name".to_string(), Value::Text(name.to_string()))]),
        )
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
        .insert(
            "teams",
            HashMap::from([
                ("name".to_string(), Value::Text(name.to_string())),
                (
                    "org_id".to_string(),
                    org_id.map(Value::Uuid).unwrap_or(Value::Null),
                ),
                (
                    "parent_id".to_string(),
                    parent_id.map(Value::Uuid).unwrap_or(Value::Null),
                ),
            ]),
        )
        .expect("create team")
        .0
}

async fn create_user(client: &JazzClient, name: &str, team_id: Option<ObjectId>) -> ObjectId {
    client
        .insert(
            "users",
            HashMap::from([
                ("name".to_string(), Value::Text(name.to_string())),
                (
                    "team_id".to_string(),
                    team_id.map(Value::Uuid).unwrap_or(Value::Null),
                ),
            ]),
        )
        .expect("create user")
        .0
}

async fn create_post(client: &JazzClient, id: i32, title: &str, author_name: &str) -> ObjectId {
    client
        .insert(
            "posts",
            row_input!("id" => id, "title" => title, "author_name" => author_name),
        )
        .expect("create post")
        .0
}

async fn create_comment(client: &JazzClient, id: i32, text: &str, post_id: i32) -> ObjectId {
    client
        .insert(
            "comments",
            row_input!("id" => id, "text" => text, "post_id" => post_id),
        )
        .expect("create comment")
        .0
}

async fn create_team_edge(
    client: &JazzClient,
    child_team: ObjectId,
    parent_team: ObjectId,
) -> ObjectId {
    client
        .insert(
            "team_edges",
            HashMap::from([
                ("child_team".to_string(), Value::Uuid(child_team)),
                ("parent_team".to_string(), Value::Uuid(parent_team)),
            ]),
        )
        .expect("create team edge")
        .0
}

async fn create_todo(client: &JazzClient, seed: TodoSeed) -> ObjectId {
    client
        .insert("todos", seed.values())
        .expect("create todo")
        .0
}

async fn create_file_part(client: &JazzClient, label: &str) -> ObjectId {
    client
        .insert(
            "file_parts",
            HashMap::from([("label".to_string(), Value::Text(label.to_string()))]),
        )
        .expect("create file part")
        .0
}

async fn create_file(client: &JazzClient, name: &str, parts: &[ObjectId]) -> ObjectId {
    client
        .insert(
            "files",
            HashMap::from([
                ("name".to_string(), Value::Text(name.to_string())),
                (
                    "parts".to_string(),
                    Value::Array(parts.iter().copied().map(Value::Uuid).collect()),
                ),
            ]),
        )
        .expect("create file")
        .0
}

fn todo_descriptor(schema: &Schema) -> RowDescriptor {
    schema
        .get(&"todos".into())
        .expect("todos table should exist in runtime schema")
        .columns
        .clone()
}

fn last_updated_todo_title(
    log: &[OrderedRowDelta],
    descriptor: &RowDescriptor,
    todo_id: ObjectId,
) -> Option<String> {
    let title_index = descriptor.column_index("title")?;

    log.iter().rev().find_map(|delta| {
        delta.updated.iter().rev().find_map(|change| {
            if change.id != todo_id {
                return None;
            }

            let row = change.row.as_ref()?;
            let values = decode_row(descriptor, &row.data).ok()?;
            match values.get(title_index) {
                Some(Value::Text(title)) => Some(title.clone()),
                _ => None,
            }
        })
    })
}

fn last_row_bearing_todo_title(
    log: &[OrderedRowDelta],
    descriptor: &RowDescriptor,
    todo_id: ObjectId,
) -> Option<String> {
    let title_index = descriptor.column_index("title")?;

    log.iter().rev().find_map(|delta| {
        delta
            .updated
            .iter()
            .rev()
            .find_map(|change| {
                if change.id != todo_id {
                    return None;
                }

                let row = change.row.as_ref()?;
                let values = decode_row(descriptor, &row.data).ok()?;
                match values.get(title_index) {
                    Some(Value::Text(title)) => Some(title.clone()),
                    _ => None,
                }
            })
            .or_else(|| {
                delta.added.iter().rev().find_map(|change| {
                    if change.id != todo_id {
                        return None;
                    }

                    let values = decode_row(descriptor, &change.row.data).ok()?;
                    match values.get(title_index) {
                        Some(Value::Text(title)) => Some(title.clone()),
                        _ => None,
                    }
                })
            })
    })
}

async fn start_local_client(schema: Schema) -> (TempDir, JazzClient) {
    let temp_dir = TempDir::new().expect("create local client temp dir");
    let context = AppContext {
        app_id: AppId::from_name("subscribe-all-local-overflow"),
        client_id: None,
        schema,
        server_url: String::new(),
        data_dir: temp_dir.path().to_path_buf(),
        storage: ClientStorage::Memory,
        jwt_token: None,
        backend_secret: None,
        admin_secret: None,
        sync_tracer: None,
    };

    let client = JazzClient::connect(context)
        .await
        .expect("connect local test client");

    (temp_dir, client)
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

/// Alice seeds three todos before Bob subscribes. Bob subscribes to
/// `priority > 50` and receives only the two matching rows in his initial
/// subscription result.
#[tokio::test]
async fn subscribe_all_only_returns_rows_that_match_query() {
    let schema = subscription_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("cold-filter-writer")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let alice_id = create_todo(
        &writer,
        TodoSeed {
            title: "Alice",
            done: false,
            priority: Some(75),
            tags: &["score"],
            payload: None,
        },
    )
    .await;
    let bob_id = create_todo(
        &writer,
        TodoSeed {
            title: "Bob",
            done: false,
            priority: Some(30),
            tags: &["score"],
            payload: None,
        },
    )
    .await;
    let charlie_id = create_todo(
        &writer,
        TodoSeed {
            title: "Charlie",
            done: false,
            priority: Some(90),
            tags: &["score"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &writer,
        QueryBuilder::new("todos").build(),
        "writer sees all seeded todos before cold subscriber connects",
        |rows| {
            (rows.iter().any(|(id, _)| *id == alice_id)
                && rows.iter().any(|(id, _)| *id == bob_id)
                && rows.iter().any(|(id, _)| *id == charlie_id))
            .then_some(rows)
        },
    )
    .await;

    let subscriber = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("cold-filter-subscriber")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new("todos")
        .filter_gt("priority", Value::Integer(50))
        .build();
    let mut stream = subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to cold filtered query");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "cold filtered subscription receives existing matching rows",
        |log| {
            has_added(log, alice_id) && has_added(log, charlie_id) && !has_any_change(log, bob_id)
        },
    )
    .await;

    let rows = wait_for_rows(
        &subscriber,
        query,
        "cold filtered query result contains only matching rows",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == alice_id)
                && rows.iter().any(|(id, _)| *id == charlie_id)
                && rows.iter().all(|(id, _)| *id != bob_id))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 2);

    writer.shutdown().await.expect("shutdown writer");
    subscriber.shutdown().await.expect("shutdown subscriber");
    server.shutdown().await;
}

/// Verifies that subscriptions with both `OFFSET` and `LIMIT`
/// only return rows for that page.
///
/// ```text
/// alice ──insert priorities [1, 2, 3, 4]──► server
/// bob   ──subscribe order asc offset 2 limit 1──► stream add C
/// ```
#[tokio::test]
async fn subscribe_all_cold_ordered_subscription_supports_offset_and_limit() {
    let schema = subscription_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("cold-page-writer")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let a_id = create_todo(
        &writer,
        TodoSeed {
            title: "A",
            done: false,
            priority: Some(1),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let b_id = create_todo(
        &writer,
        TodoSeed {
            title: "B",
            done: false,
            priority: Some(2),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let c_id = create_todo(
        &writer,
        TodoSeed {
            title: "C",
            done: false,
            priority: Some(3),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let d_id = create_todo(
        &writer,
        TodoSeed {
            title: "D",
            done: false,
            priority: Some(4),
            tags: &["page"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &writer,
        QueryBuilder::new("todos").build(),
        "writer sees all rows before cold paginated subscriber connects",
        |rows| {
            (rows.iter().any(|(id, _)| *id == a_id)
                && rows.iter().any(|(id, _)| *id == b_id)
                && rows.iter().any(|(id, _)| *id == c_id)
                && rows.iter().any(|(id, _)| *id == d_id))
            .then_some(rows)
        },
    )
    .await;

    let subscriber = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("cold-page-subscriber")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new("todos")
        .order_by("priority")
        .offset(2)
        .limit(1)
        .build();
    let mut stream = subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to cold paginated query");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "cold paginated subscription receives requested row",
        |log| {
            has_added(log, c_id)
                && !has_any_change(log, a_id)
                && !has_any_change(log, b_id)
                && !has_any_change(log, d_id)
        },
    )
    .await;

    let rows = wait_for_rows(
        &subscriber,
        query,
        "cold paginated query result contains only the requested row",
        |rows| (rows.len() == 1 && rows[0].0 == c_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1[0], Value::Text("C".to_string()));
    assert_eq!(rows[0].1[2], Value::Integer(3));

    writer.shutdown().await.expect("shutdown writer");
    subscriber.shutdown().await.expect("shutdown subscriber");
    server.shutdown().await;
}

/// Verifies that a subscription with `OFFSET` but no `LIMIT`
/// returns all rows after the requested offset.
#[tokio::test]
async fn subscribe_all_cold_ordered_subscription_supports_offset_without_limit() {
    let schema = subscription_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("cold-offset-writer")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let a_id = create_todo(
        &writer,
        TodoSeed {
            title: "A",
            done: false,
            priority: Some(1),
            tags: &["offset"],
            payload: None,
        },
    )
    .await;
    let b_id = create_todo(
        &writer,
        TodoSeed {
            title: "B",
            done: false,
            priority: Some(2),
            tags: &["offset"],
            payload: None,
        },
    )
    .await;
    let c_id = create_todo(
        &writer,
        TodoSeed {
            title: "C",
            done: false,
            priority: Some(3),
            tags: &["offset"],
            payload: None,
        },
    )
    .await;
    let d_id = create_todo(
        &writer,
        TodoSeed {
            title: "D",
            done: false,
            priority: Some(4),
            tags: &["offset"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &writer,
        QueryBuilder::new("todos").build(),
        "writer sees all rows before cold offset subscriber connects",
        |rows| {
            (rows.iter().any(|(id, _)| *id == a_id)
                && rows.iter().any(|(id, _)| *id == b_id)
                && rows.iter().any(|(id, _)| *id == c_id)
                && rows.iter().any(|(id, _)| *id == d_id))
            .then_some(rows)
        },
    )
    .await;

    let subscriber = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("cold-offset-subscriber")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new("todos")
        .order_by("priority")
        .offset(2)
        .build();
    let mut stream = subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to cold offset-only query");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "cold offset-only subscription receives trailing rows",
        |log| {
            has_added(log, c_id)
                && has_added(log, d_id)
                && !has_any_change(log, a_id)
                && !has_any_change(log, b_id)
        },
    )
    .await;

    let rows = wait_for_rows(
        &subscriber,
        query,
        "cold offset-only query result contains trailing rows",
        |rows| (rows.len() == 2 && rows[0].0 == c_id && rows[1].0 == d_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1[0], Value::Text("C".to_string()));
    assert_eq!(rows[1].1[0], Value::Text("D".to_string()));

    writer.shutdown().await.expect("shutdown writer");
    subscriber.shutdown().await.expect("shutdown subscriber");
    server.shutdown().await;
}

/// Verifies that a live ordered/limited subscription evicts the old tail row
/// when a new row sorts ahead of the existing page.
#[tokio::test]
async fn subscribe_all_sorted_limited_subscription_reorders_when_new_top_row_arrives() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .order_by_desc("priority")
        .limit(2)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to sorted limited query");
    let mut log = Vec::new();

    let alice_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "Alice",
            done: false,
            priority: Some(100),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let bob_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "Bob",
            done: false,
            priority: Some(50),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let charlie_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "Charlie",
            done: false,
            priority: Some(75),
            tags: &["page"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &pair.subscriber,
        query.clone(),
        "initial sorted limited page contains Alice and Charlie",
        |rows| {
            (rows.len() == 2 && rows[0].0 == alice_id && rows[1].0 == charlie_id).then_some(rows)
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    let diana_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "Diana",
            done: false,
            priority: Some(125),
            tags: &["page"],
            payload: None,
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "new top row replaces previous limited page tail",
        |log| {
            log.iter().any(|delta| {
                delta
                    .added
                    .iter()
                    .any(|change| change.id == diana_id && change.index == 0)
                    && delta.removed.iter().any(|change| change.id == charlie_id)
            })
        },
    )
    .await;
    assert!(
        !has_any_change(&log, bob_id),
        "Bob should remain outside the limited page"
    );

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "sorted limited page reorders around the new top row",
        |rows| (rows.len() == 2 && rows[0].0 == diana_id && rows[1].0 == alice_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1[0], Value::Text("Diana".to_string()));
    assert_eq!(rows[1].1[0], Value::Text("Alice".to_string()));

    pair.shutdown().await;
}

/// Verifies that deleting a row before an offset/limited page shifts the live
/// window forward and emits the newly visible row.
#[tokio::test]
async fn subscribe_all_offset_limited_subscription_shifts_window_when_deleting_row_before_window() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .order_by("priority")
        .offset(1)
        .limit(2)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to offset limited query");
    let mut log = Vec::new();

    let a_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "A",
            done: false,
            priority: Some(1),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let b_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "B",
            done: false,
            priority: Some(2),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let c_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "C",
            done: false,
            priority: Some(3),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let d_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "D",
            done: false,
            priority: Some(4),
            tags: &["page"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &pair.subscriber,
        query.clone(),
        "initial offset limited page contains B and C",
        |rows| (rows.len() == 2 && rows[0].0 == b_id && rows[1].0 == c_id).then_some(rows),
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    pair.writer
        .delete(a_id)
        .expect("delete row before offset window");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "offset limited page shifts after deleting row before window",
        |log| {
            log.iter().any(|delta| {
                delta.removed.iter().any(|change| change.id == b_id)
                    && delta
                        .added
                        .iter()
                        .any(|change| change.id == d_id && change.index == 1)
            })
        },
    )
    .await;
    assert!(
        !has_any_change(&log, a_id),
        "A was before the window and should not appear in the page delta"
    );

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "offset limited page contains C and D after deleting A",
        |rows| (rows.len() == 2 && rows[0].0 == c_id && rows[1].0 == d_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1[0], Value::Text("C".to_string()));
    assert_eq!(rows[1].1[0], Value::Text("D".to_string()));

    pair.shutdown().await;
}

/// Verifies that rapid overwrites on a single subscribed row still leave the
/// subscriber with a last delta that carries the final value.
///
/// Alice creates one todo and Bob subscribes before the hot update burst. Alice
/// then overwrites `title` 500 times in a tight loop. Intermediate deltas may
/// be coalesced, but once the subscriber's EdgeServer query returns the final
/// title, the last row-bearing update delta in Bob's stream must decode to that
/// same final title.
///
/// TODO:
/// Keep this test ignored until the client preserves "latest write wins" during
/// a burst of updates. The assertion is fine; the remaining burst-load bug is
/// in the writer's outbound sync path:
///
/// 1. Outgoing updates are sent in separate spawned tasks, so a fast sequence of
///    writes can reach the server in a different order than the writer produced
///    them.
///
/// The subscriber-side drop case is avoided by using an unbounded local stream,
/// but that still does not fix out-of-order delivery to the server.
///
/// ```text
/// intended
/// --------
/// Alice writes:  title-001 -> title-002 -> ... -> title-500
/// Server state:  title-001 -> title-002 -> ... -> title-500
/// Bob stream:    add ...... update ...... last row-bearing delta = title-500
///
/// current failure modes
/// ---------------------
/// send tasks can race:
/// Alice writes:  title-001 -> title-002 -> title-003
/// Server sees:   title-002 -> title-001 -> title-003
///
/// result:
/// Bob snapshot query    = title-500
/// Bob last stream delta != title-500
/// ```
#[tokio::test]
async fn subscription_reflects_final_state_after_rapid_bulk_updates() {
    const RAPID_UPDATES: usize = 500;

    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos").build();
    let runtime_schema = pair
        .subscriber
        .schema()
        .expect("load subscriber runtime schema");
    let descriptor = todo_descriptor(&runtime_schema);

    let todo_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "bulk-000",
            done: false,
            priority: Some(1),
            tags: &["burst"],
            payload: None,
        },
    )
    .await;

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to bulk-updated todo");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "initial add before rapid updates",
        |log| has_added(log, todo_id),
    )
    .await;
    log.clear();

    let final_title = format!("bulk-{RAPID_UPDATES:03}");
    for revision in 1..=RAPID_UPDATES {
        pair.writer
            .update(
                todo_id,
                vec![(
                    "title".to_string(),
                    Value::Text(format!("bulk-{revision:03}")),
                )],
            )
            .expect("apply rapid bulk update");
    }

    let rows = wait_for_rows(
        &pair.subscriber,
        query.clone(),
        format!("subscriber sees final bulk title {final_title}"),
        |rows| {
            (rows.len() == 1
                && rows[0].0 == todo_id
                && rows[0].1.first() == Some(&Value::Text(final_title.clone())))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[0], Value::Text(final_title.clone()));

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "final row-bearing bulk update delta",
        |log| {
            last_updated_todo_title(log, &descriptor, todo_id).as_deref()
                == Some(final_title.as_str())
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;

    let latest_delta_title = last_updated_todo_title(&log, &descriptor, todo_id);
    assert_eq!(
        latest_delta_title.as_deref(),
        Some(final_title.as_str()),
        "last row-bearing update delta should decode to the final rapid-update title"
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

    let schema = subscription_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("condition-writer")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
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
        let subscriber = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema.clone())
            .with_user_id(format!("condition-subscriber-{}", case.name))
            .ready_on("todos", READY_TIMEOUT)
            .connect()
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

/// Verifies that a rapid burst of local updates still leaves the subscription
/// stream carrying the final row state.
///
/// This test uses a single local client so it isolates the subscription
/// delivery path from server sync ordering.
#[tokio::test]
async fn local_subscription_preserves_final_state_under_rapid_updates() {
    const RAPID_UPDATES: usize = 100;

    let (_temp_dir, client) = start_local_client(subscription_schema()).await;
    let query = QueryBuilder::new("todos").build();
    let runtime_schema = client.schema().expect("load local runtime schema");
    let descriptor = todo_descriptor(&runtime_schema);

    let mut stream = client
        .subscribe(query.clone())
        .await
        .expect("subscribe to local todos");
    let mut log = Vec::new();

    let todo_id = create_todo(
        &client,
        TodoSeed {
            title: "local-bulk-000",
            done: false,
            priority: Some(1),
            tags: &["burst"],
            payload: None,
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "initial local add before rapid updates",
        |log| has_added(log, todo_id),
    )
    .await;
    log.clear();

    let final_title = format!("local-bulk-{RAPID_UPDATES:03}");
    for revision in 1..=RAPID_UPDATES {
        client
            .update(
                todo_id,
                vec![(
                    "title".to_string(),
                    Value::Text(format!("local-bulk-{revision:03}")),
                )],
            )
            .expect("apply rapid local update");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let rows = wait_for_query(
        &client,
        query.clone(),
        None,
        QUERY_TIMEOUT,
        format!("local client sees final bulk title {final_title}"),
        |rows| {
            (rows.len() == 1
                && rows[0].0 == todo_id
                && rows[0].1.first() == Some(&Value::Text(final_title.clone())))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[0], Value::Text(final_title.clone()));

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "local stream carries the final row-bearing delta after rapid updates",
        |log| {
            last_row_bearing_todo_title(log, &descriptor, todo_id).as_deref()
                == Some(final_title.as_str())
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;

    let latest_delta_title = last_row_bearing_todo_title(&log, &descriptor, todo_id);
    assert_eq!(
        latest_delta_title.as_deref(),
        Some(final_title.as_str()),
        "last row-bearing local delta should converge to the final title after rapid updates"
    );

    client.shutdown().await.expect("shutdown local client");
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

#[tokio::test]
async fn subscribe_all_join_emits_when_matching_joined_row_is_inserted() {
    let pair = ClientPair::start().await;

    let user_id = create_user(&pair.writer, "Alice", None).await;
    wait_for_rows(
        &pair.subscriber,
        QueryBuilder::new("users").build(),
        "subscriber sees base join user before joined-table insert",
        |rows| rows.iter().any(|(id, _)| *id == user_id).then_some(rows),
    )
    .await;

    let query = QueryBuilder::new("users")
        .join("posts")
        .on("users.name", "posts.author_name")
        .build();
    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to join query");
    let mut log = Vec::new();

    create_post(&pair.writer, 100, "Test Post", "Alice").await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "joined-table insert emits joined result",
        |log| has_added(log, user_id),
    )
    .await;

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "join query contains newly matched row",
        |rows| (rows.len() == 1 && rows[0].0 == user_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![
            Value::Text("Alice".to_string()),
            Value::Null,
            Value::Integer(100),
            Value::Text("Test Post".to_string()),
            Value::Text("Alice".to_string()),
        ]
    );

    pair.shutdown().await;
}

/// Verifies that a default join result is keyed by the base row and contains
/// values from both the base and joined tables.
#[tokio::test]
async fn subscribe_all_join_returns_base_and_joined_table_values() {
    let pair = ClientPair::start().await;

    let user_id = create_user(&pair.writer, "Alice", None).await;
    let post_id = create_post(&pair.writer, 100, "Hello World", "Alice").await;

    let query = QueryBuilder::new("users")
        .join("posts")
        .on("users.name", "posts.author_name")
        .build();
    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "join query returns combined tuple",
        |rows| (rows.len() == 1 && rows[0].0 == user_id).then_some(rows),
    )
    .await;

    assert_ne!(
        rows[0].0, post_id,
        "default join output should be keyed by the base row id"
    );
    assert_eq!(
        rows[0].1,
        vec![
            Value::Text("Alice".to_string()),
            Value::Null,
            Value::Integer(100),
            Value::Text("Hello World".to_string()),
            Value::Text("Alice".to_string()),
        ]
    );

    pair.shutdown().await;
}

/// Verifies that filters can target a column supplied by the joined table.
#[tokio::test]
async fn subscribe_all_join_filter_on_joined_table_column() {
    let pair = ClientPair::start().await;

    create_user(&pair.writer, "Alice", None).await;
    let bob_id = create_user(&pair.writer, "Bob", None).await;
    create_post(&pair.writer, 100, "Hello World", "Alice").await;
    create_post(&pair.writer, 101, "Learning Rust", "Bob").await;

    let query = QueryBuilder::new("users")
        .join("posts")
        .on("users.name", "posts.author_name")
        .filter_eq("title", Value::Text("Learning Rust".to_string()))
        .build();
    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "join query filters by joined table title",
        |rows| (rows.len() == 1 && rows[0].0 == bob_id).then_some(rows),
    )
    .await;

    assert_eq!(rows[0].1[0], Value::Text("Bob".to_string()));
    assert_eq!(rows[0].1[3], Value::Text("Learning Rust".to_string()));

    pair.shutdown().await;
}

/// Verifies that alias-qualified filters resolve against the intended side of
/// a join.
#[tokio::test]
async fn subscribe_all_join_filter_on_scoped_alias_columns() {
    let pair = ClientPair::start().await;

    create_user(&pair.writer, "Alice", None).await;
    let bob_id = create_user(&pair.writer, "Bob", None).await;
    create_post(&pair.writer, 100, "Hello World", "Alice").await;
    create_post(&pair.writer, 101, "Learning Rust", "Bob").await;

    let query = QueryBuilder::new("users")
        .alias("u")
        .join("posts")
        .alias("p")
        .on("u.name", "p.author_name")
        .filter_eq("u.name", Value::Text("Bob".to_string()))
        .filter_eq("p.title", Value::Text("Learning Rust".to_string()))
        .build();
    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "join query filters by scoped aliases",
        |rows| (rows.len() == 1 && rows[0].0 == bob_id).then_some(rows),
    )
    .await;

    assert_eq!(rows[0].1[0], Value::Text("Bob".to_string()));
    assert_eq!(rows[0].1[3], Value::Text("Learning Rust".to_string()));

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

/// Verifies that a projected array include can contain a joined subquery.
#[tokio::test]
async fn subscribe_all_array_subquery_with_join() {
    let pair = ClientPair::start().await;

    let user_id = create_user(&pair.writer, "Alice", None).await;
    create_post(&pair.writer, 100, "Post A", "Alice").await;
    create_post(&pair.writer, 101, "Post B", "Alice").await;
    create_comment(&pair.writer, 1000, "Comment on A", 100).await;
    create_comment(&pair.writer, 1001, "Another on A", 100).await;
    create_comment(&pair.writer, 1002, "Comment on B", 101).await;

    let query = QueryBuilder::new("users")
        .with_array("post_comments", |sub| {
            sub.from("posts")
                .join("comments")
                .on("posts.id", "comments.post_id")
                .correlate("author_name", "users.name")
        })
        .build();
    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "array include with joined subquery returns all post/comment pairs",
        |rows| (rows.len() == 1 && rows[0].0 == user_id).then_some(rows),
    )
    .await;

    assert_eq!(rows[0].1[0], Value::Text("Alice".to_string()));
    assert_eq!(rows[0].1[1], Value::Null);
    let post_comments = rows[0].1[2]
        .as_array()
        .expect("post_comments should be an array");
    assert_eq!(
        post_comments.len(),
        3,
        "Post A has two comments and Post B has one"
    );
    for pair in post_comments {
        let values = pair
            .as_row()
            .expect("joined subquery element should be a row");
        assert_eq!(values.len(), 6);
        assert!(pair.row_id().is_some(), "joined row should retain an id");
        let Value::Integer(post_id) = values[0] else {
            panic!("joined post id should be an integer");
        };
        assert!(post_id == 100 || post_id == 101);
        assert_eq!(values[5], Value::Integer(post_id));
    }

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
