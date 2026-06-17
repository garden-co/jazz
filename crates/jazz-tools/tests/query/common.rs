use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::query_manager::encoding::decode_row;
use jazz_tools::query_manager::types::RowDescriptor;
use jazz_tools::row_input;
use jazz_tools::server::TestingServer;
use jazz_tools::{
    AppContext, AppId, ClientStorage, ColumnType, JazzClient, ObjectId, OrderedRowDelta, Schema,
    SchemaBuilder, TableSchema, Value,
};
use tempfile::TempDir;

use crate::support::TestingClient;

pub(crate) const READY_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
pub(crate) const NO_DELTA_WINDOW: Duration = Duration::from_millis(500);

pub(crate) fn subscription_schema() -> Schema {
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

pub(crate) struct ClientPair {
    pub(crate) server: TestingServer,
    pub(crate) writer: JazzClient,
    pub(crate) subscriber: JazzClient,
}

impl ClientPair {
    pub(crate) async fn start() -> Self {
        let schema = subscription_schema();
        let server = TestingServer::start_with_schema(schema.clone()).await;
        let writer = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema.clone())
            .with_user_id("subscribe-all-writer")
            .ready_on("todos", READY_TIMEOUT)
            .connect()
            .await;

        let subscriber_builder = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema)
            .with_user_id("subscribe-all-subscriber")
            .ready_on("todos", READY_TIMEOUT);
        let subscriber = subscriber_builder.connect().await;

        Self {
            server,
            writer,
            subscriber,
        }
    }

    pub(crate) async fn shutdown(self) {
        self.writer.shutdown().await.expect("shutdown writer");
        self.subscriber
            .shutdown()
            .await
            .expect("shutdown subscriber");
        self.server.shutdown().await;
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TodoSeed {
    pub(crate) title: &'static str,
    pub(crate) done: bool,
    pub(crate) priority: Option<i32>,
    pub(crate) tags: &'static [&'static str],
    pub(crate) payload: Option<&'static [u8]>,
}

impl TodoSeed {
    pub(crate) fn values(self) -> HashMap<String, Value> {
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

pub(crate) async fn create_org(client: &JazzClient, name: &str) -> ObjectId {
    client
        .insert(
            "orgs",
            HashMap::from([("name".to_string(), Value::Text(name.to_string()))]),
        )
        .expect("create org")
        .0
}

pub(crate) async fn create_team(
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

pub(crate) async fn create_user(
    client: &JazzClient,
    name: &str,
    team_id: Option<ObjectId>,
) -> ObjectId {
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

pub(crate) async fn create_post(
    client: &JazzClient,
    id: i32,
    title: &str,
    author_name: &str,
) -> ObjectId {
    client
        .insert(
            "posts",
            row_input!("id" => id, "title" => title, "author_name" => author_name),
        )
        .expect("create post")
        .0
}

pub(crate) async fn create_todo(client: &JazzClient, seed: TodoSeed) -> ObjectId {
    client
        .insert("todos", seed.values())
        .expect("create todo")
        .0
}

pub(crate) async fn create_file_part(client: &JazzClient, label: &str) -> ObjectId {
    client
        .insert(
            "file_parts",
            HashMap::from([("label".to_string(), Value::Text(label.to_string()))]),
        )
        .expect("create file part")
        .0
}

pub(crate) async fn create_file(client: &JazzClient, name: &str, parts: &[ObjectId]) -> ObjectId {
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

pub(crate) fn todo_descriptor(schema: &Schema) -> RowDescriptor {
    schema
        .get(&"todos".into())
        .expect("todos table should exist in runtime schema")
        .columns
        .clone()
}

pub(crate) fn last_updated_todo_title(
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

pub(crate) fn last_row_bearing_todo_title(
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

pub(crate) async fn start_local_client(schema: Schema) -> (TempDir, JazzClient) {
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
