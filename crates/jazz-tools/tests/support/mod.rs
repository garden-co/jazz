use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jazz_tools::query_manager::types::SchemaHash;
use jazz_tools::runtime_tokio::TokioRuntime;
use jazz_tools::schema_manager::{AppId, Lens, SchemaManager};
use jazz_tools::server::{ServerState, TestingServer};
use jazz_tools::storage::MemoryStorage;
use jazz_tools::sync_manager::{ClientId, Destination, OutboxEntry, ServerId, SyncManager};
use jazz_tools::transport_protocol::encode_outbox_entry_payload;
use jazz_tools::{
    AppContext, ClientStorage, DurabilityTier, JazzClient, ObjectId, OrderedRowDelta, Query,
    QueryBuilder, Schema, SubscriptionStream, Value,
};
use jsonwebtoken::{EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

mod permissions;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(8);
#[allow(dead_code)]
const DEFAULT_ROWS_TIMEOUT: Duration = Duration::from_secs(25);
#[allow(dead_code)]
const DEFAULT_STREAM_POLL_INTERVAL: Duration = Duration::from_millis(50);
const TEST_JWT_SECRET: &str = "test-jwt-secret-for-integration";
const TEST_JWT_KID: &str = "test-jwks-kid";

/// Convenience shape for query results returned by test helpers.
pub type QueryRows = Vec<(ObjectId, Vec<Value>)>;

#[allow(unused_imports)]
pub use permissions::{
    PublishedPermissionsHead, allow_all_permissions, deny_all_select_permissions,
    publish_allow_all_permissions, publish_permissions,
};

fn split_base_url(server_url: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let mut url = reqwest::Url::parse(server_url)?;
    let route_prefix = match url.path().trim_end_matches('/') {
        "" | "/" => String::new(),
        path => path.to_string(),
    };

    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);

    Ok((
        url.to_string().trim_end_matches('/').to_string(),
        route_prefix,
    ))
}

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    claims: JsonValue,
    exp: u64,
}

#[allow(dead_code)]
enum TestingClientAuth {
    Admin,
    User,
    Claims(JsonValue),
}

#[derive(Clone, Copy)]
enum TestingClientStorage {
    Memory,
    Persistent,
}

/// Builder-style helper for test clients backed by `TestingServer`.
///
/// Supports the three common auth shapes used across the integration suite:
/// admin-capable clients, normal JWT-only clients, and JWT-only clients with
/// custom claims.
pub struct TestingClient<'a> {
    server: Option<&'a TestingServer>,
    schema: Option<Schema>,
    user_id: Option<String>,
    auth: TestingClientAuth,
    storage: TestingClientStorage,
    ready_table: Option<String>,
    ready_timeout: Option<Duration>,
    sync_tracer: Option<(jazz_tools::sync_tracer::SyncTracer, String)>,
}

#[allow(dead_code)]
impl<'a> TestingClient<'a> {
    pub fn builder() -> Self {
        Self {
            server: None,
            schema: None,
            user_id: None,
            auth: TestingClientAuth::User,
            storage: TestingClientStorage::Memory,
            ready_table: None,
            ready_timeout: None,
            sync_tracer: None,
        }
    }

    pub fn with_server(mut self, server: &'a TestingServer) -> Self {
        self.server = Some(server);
        self
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    #[allow(dead_code)]
    pub fn as_admin(mut self) -> Self {
        self.auth = TestingClientAuth::Admin;
        self
    }

    #[allow(dead_code)]
    pub fn as_user(mut self) -> Self {
        self.auth = TestingClientAuth::User;
        self
    }

    #[allow(dead_code)]
    pub fn with_claims(mut self, claims: JsonValue) -> Self {
        self.auth = TestingClientAuth::Claims(claims);
        self
    }

    pub fn ready_on(mut self, table: impl Into<String>, timeout: Duration) -> Self {
        self.ready_table = Some(table.into());
        self.ready_timeout = Some(timeout);
        self
    }

    pub fn with_memory_storage(mut self) -> Self {
        self.storage = TestingClientStorage::Memory;
        self
    }

    pub fn with_persistent_storage(mut self) -> Self {
        self.storage = TestingClientStorage::Persistent;
        self
    }

    pub fn with_tracer(
        mut self,
        tracer: &jazz_tools::sync_tracer::SyncTracer,
        name: impl Into<String>,
    ) -> Self {
        self.sync_tracer = Some((tracer.clone(), name.into()));
        self
    }

    pub async fn connect(self) -> JazzClient {
        self.connect_with_context().await.1
    }

    /// Connects the client and also returns the exact `AppContext` used for
    /// the connection so callers can later reconnect with the same configuration.
    ///
    /// Persistent storage reuses local state across reconnects; memory storage
    /// does not.
    pub async fn connect_with_context(self) -> (AppContext, JazzClient) {
        let ready_table = self.ready_table.clone();
        let ready_timeout = self.ready_timeout;
        let context = self.build_context();

        let client = JazzClient::connect(context.clone())
            .await
            .expect("connect test client");

        if let Some(ready_table) = ready_table {
            wait_for_edge_query_ready(
                &client,
                &ready_table,
                ready_timeout.expect("ready timeout should be set when ready table is set"),
            )
            .await;
        }

        (context, client)
    }

    /// Builds a fresh test-client context.
    ///
    /// Each call allocates a new client data directory. If you need both the
    /// connected client and the matching context for a later reconnect, prefer
    /// `connect_with_context`.
    pub fn build_context(&self) -> AppContext {
        self.build_context_for_reuse()
    }

    fn build_context_for_reuse(&self) -> AppContext {
        let user_id = self
            .user_id
            .as_deref()
            .expect("TestingClient requires `with_user_id(...)` before building");
        let mut context = self
            .server
            .expect("TestingClient requires `with_server(...)` before building")
            .make_client_context_for_user(
                self.schema
                    .clone()
                    .expect("TestingClient requires `with_schema(...)` before building"),
                user_id,
            );

        match &self.auth {
            TestingClientAuth::Admin => {
                context.admin_secret = Some(
                    self.server
                        .expect("TestingClient requires `with_server(...)` before building")
                        .admin_secret()
                        .to_string(),
                );
            }
            TestingClientAuth::User => {
                context.backend_secret = None;
                context.admin_secret = None;
            }
            TestingClientAuth::Claims(claims) => {
                context.jwt_token = Some(make_jwt(user_id, claims.clone()));
                context.backend_secret = None;
                context.admin_secret = None;
            }
        }

        context.storage = match self.storage {
            TestingClientStorage::Memory => ClientStorage::Memory,
            TestingClientStorage::Persistent => ClientStorage::Persistent,
        };

        context.sync_tracer = self.sync_tracer.clone();

        context
    }
}

#[allow(dead_code)]
pub async fn connect_ready_client(
    server: &TestingServer,
    schema: &Schema,
    user_id: &str,
    ready_table: &str,
    ready_timeout: Duration,
) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id(user_id)
        .ready_on(ready_table, ready_timeout)
        .connect()
        .await
}

#[allow(dead_code)]
pub async fn connect_ready_user(
    server: &TestingServer,
    schema: &Schema,
    user_id: &str,
    ready_table: &str,
    ready_timeout: Duration,
) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id(user_id)
        .as_user()
        .ready_on(ready_table, ready_timeout)
        .connect()
        .await
}

#[allow(dead_code)]
pub async fn connect_ready_claims(
    server: &TestingServer,
    schema: &Schema,
    user_id: &str,
    claims: JsonValue,
    ready_table: &str,
    ready_timeout: Duration,
) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id(user_id)
        .with_claims(claims)
        .ready_on(ready_table, ready_timeout)
        .connect()
        .await
}

fn make_jwt(sub: &str, claims: JsonValue) -> String {
    let jwt_claims = JwtClaims {
        sub: sub.to_string(),
        claims,
        exp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_secs()
            + 3600,
    };

    let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some(TEST_JWT_KID.to_string());

    encode(
        &header,
        &jwt_claims,
        &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
    )
    .expect("encode jwt")
}

fn build_catalogue_runtime(
    schema_manager: SchemaManager,
    storage: MemoryStorage,
    state: Arc<ServerState>,
    client_id: ClientId,
    in_flight_pushes: Arc<AtomicUsize>,
    push_errors: Arc<Mutex<Vec<String>>>,
) -> TokioRuntime<MemoryStorage> {
    TokioRuntime::new(schema_manager, storage, move |entry: OutboxEntry| {
        let OutboxEntry {
            destination,
            payload,
        } = entry;
        if let Destination::Server(_) = destination {
            in_flight_pushes.fetch_add(1, Ordering::AcqRel);
            let state = state.clone();
            let push_errors = push_errors.clone();
            let in_flight_pushes = in_flight_pushes.clone();
            tokio::spawn(async move {
                let entry = jazz_tools::sync_manager::OutboxEntry {
                    destination: Destination::Server(ServerId::default()),
                    payload,
                };
                let frame = match encode_outbox_entry_payload(&entry) {
                    Ok(frame) => frame,
                    Err(error) => {
                        if let Ok(mut errors) = push_errors.lock() {
                            errors.push(format!("encode sync frame: {error}"));
                        }
                        in_flight_pushes.fetch_sub(1, Ordering::AcqRel);
                        return;
                    }
                };
                if let Err(error) = state.process_ws_client_frame(client_id, &frame).await {
                    if let Ok(mut errors) = push_errors.lock() {
                        errors.push(error);
                    }
                }
                in_flight_pushes.fetch_sub(1, Ordering::AcqRel);
            });
        }
    })
}

async fn wait_for_in_flight_pushes(in_flight_pushes: &Arc<AtomicUsize>) {
    while in_flight_pushes.load(Ordering::Acquire) > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn push_catalogue_in_memory(
    state: Arc<ServerState>,
    app_id: AppId,
    env: &str,
    user_branch: &str,
    schemas: &[Schema],
    lenses: &[Lens],
) -> Result<(), Box<dyn std::error::Error>> {
    let client_id = ClientId::new();
    state
        .runtime
        .ensure_client_as_admin(client_id)
        .map_err(|e| format!("register admin client: {e:?}"))?;

    let in_flight_pushes = Arc::new(AtomicUsize::new(0));
    let push_errors = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut schema_by_hash: std::collections::HashMap<SchemaHash, &Schema> =
        std::collections::HashMap::with_capacity(schemas.len());
    for schema in schemas {
        schema_by_hash.insert(SchemaHash::compute(schema), schema);
        let schema_manager =
            SchemaManager::new(SyncManager::new(), schema.clone(), app_id, env, user_branch)
                .map_err(|error| {
                    format!("Failed to initialize schema manager for schema push: {error:?}")
                })?;
        let runtime = build_catalogue_runtime(
            schema_manager,
            MemoryStorage::default(),
            state.clone(),
            client_id,
            in_flight_pushes.clone(),
            push_errors.clone(),
        );

        runtime.persist_schema()?;
        runtime.add_server(ServerId::default())?;
        runtime.flush().await?;
    }

    for lens in lenses {
        let source_schema = schema_by_hash.get(&lens.source_hash).ok_or_else(|| {
            format!(
                "No schema provided for lens source hash {}",
                lens.source_hash
            )
        })?;

        let mut storage = MemoryStorage::default();
        let mut schema_manager = SchemaManager::new(
            SyncManager::new(),
            (*source_schema).clone(),
            app_id,
            env,
            user_branch,
        )
        .map_err(|error| format!("Failed to initialize schema manager for lens push: {error:?}"))?;
        schema_manager.persist_lens(&mut storage, lens);
        let runtime = build_catalogue_runtime(
            schema_manager,
            storage,
            state.clone(),
            client_id,
            in_flight_pushes.clone(),
            push_errors.clone(),
        );

        runtime.add_server(ServerId::default())?;
        runtime.flush().await?;
    }

    wait_for_in_flight_pushes(&in_flight_pushes).await;

    let errors = push_errors.lock().unwrap().clone();
    if !errors.is_empty() {
        return Err(format!(
            "Schema push encountered {} sync error(s): {}",
            errors.len(),
            errors.join("; ")
        )
        .into());
    }

    Ok(())
}

#[allow(dead_code)]
/// Polls an async predicate until it returns a value or the timeout expires.
///
/// This is the lowest-level waiting primitive used by the test helpers in this
/// module.
pub async fn wait_for<T, F, Fut>(
    timeout: Duration,
    description: impl Into<String>,
    mut check: F,
) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let description = description.into();
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if let Some(value) = check().await {
            return value;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {description}");
        }

        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

/// Re-runs a query until its rows satisfy the provided matcher or the timeout
/// expires.
///
/// Per-attempt query timeouts and transient query errors are retried until the
/// outer deadline is reached.
pub async fn wait_for_query<T, F>(
    client: &JazzClient,
    query: Query,
    durability_tier: Option<DurabilityTier>,
    timeout: Duration,
    description: impl Into<String>,
    mut check_rows: F,
) -> T
where
    F: FnMut(QueryRows) -> Option<T>,
{
    let description = description.into();
    let deadline = tokio::time::Instant::now() + timeout;

    let mut last_error: Option<String> = None;
    let mut last_rows: Option<QueryRows> = None;

    loop {
        match tokio::time::timeout(
            DEFAULT_QUERY_TIMEOUT,
            client.query(query.clone(), durability_tier),
        )
        .await
        {
            Ok(Ok(rows)) => {
                if let Some(value) = check_rows(rows.clone()) {
                    return value;
                }
                last_rows = Some(rows);
                last_error = None;
            }
            Ok(Err(e)) => last_error = Some(e.to_string()),
            Err(_) => {} // per-attempt timeout, will retry
        }

        if tokio::time::Instant::now() >= deadline {
            match last_error {
                Some(e) => panic!("timed out waiting for {description}: last query error: {e}"),
                None => panic!(
                    "timed out waiting for {description}: last rows: {:?}",
                    last_rows
                ),
            }
        }

        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::split_base_url;

    #[test]
    fn split_base_url_handles_plain_origin() {
        let (base_url, route_prefix) =
            split_base_url("http://127.0.0.1:31337").expect("split base url");

        assert_eq!(base_url, "http://127.0.0.1:31337");
        assert_eq!(route_prefix, "");
    }

    #[test]
    fn split_base_url_preserves_route_prefix_without_trailing_slash() {
        let (base_url, route_prefix) =
            split_base_url("http://127.0.0.1:31337/api/v1/").expect("split base url");

        assert_eq!(base_url, "http://127.0.0.1:31337");
        assert_eq!(route_prefix, "/api/v1");
    }
}

#[allow(dead_code)]
/// Waits until a trivial EdgeServer query against `table` succeeds.
///
/// Tests use this after connecting a client so subscription and query checks do
/// not race the initial schema/catalogue sync.
pub async fn wait_for_edge_query_ready(client: &JazzClient, table: &str, timeout: Duration) {
    wait_for_query(
        client,
        QueryBuilder::new(table).build(),
        Some(DurabilityTier::EdgeServer),
        timeout,
        format!("EdgeServer query readiness for {table}"),
        |_| Some(()),
    )
    .await;
}

#[allow(dead_code)]
/// Re-runs an EdgeServer query until its rows satisfy the matcher, using the
/// module's default row timeout.
pub async fn wait_for_rows<T, F>(
    client: &JazzClient,
    query: Query,
    description: impl Into<String>,
    check_rows: F,
) -> T
where
    F: FnMut(QueryRows) -> Option<T>,
{
    wait_for_query(
        client,
        query,
        Some(DurabilityTier::EdgeServer),
        DEFAULT_ROWS_TIMEOUT,
        description,
        check_rows,
    )
    .await
}

#[allow(dead_code)]
pub async fn wait_for_visible_row(
    client: &JazzClient,
    query: Query,
    description: impl Into<String>,
    row_id: ObjectId,
    expected: Vec<Value>,
) -> QueryRows {
    wait_for_rows(client, query, description, |rows| {
        has_row(&rows, row_id, &expected).then_some(rows)
    })
    .await
}

#[allow(dead_code)]
pub async fn wait_for_hidden_row(
    client: &JazzClient,
    query: Query,
    description: impl Into<String>,
    row_id: ObjectId,
) -> QueryRows {
    wait_for_rows(client, query, description, |rows| {
        lacks_row(&rows, row_id).then_some(rows)
    })
    .await
}

#[allow(dead_code)]
/// Reads subscription deltas until the accumulated log satisfies the provided
/// predicate or the timeout expires.
///
/// The matching delta is appended to `log` before the predicate is checked
/// again, so callers can assert against the full sequence of observed changes.
pub async fn wait_for_subscription_update<F>(
    stream: &mut SubscriptionStream,
    log: &mut Vec<OrderedRowDelta>,
    timeout: Duration,
    description: impl Into<String>,
    mut predicate: F,
) where
    F: FnMut(&[OrderedRowDelta]) -> bool,
{
    let description = description.into();
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if predicate(log) {
            return;
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            panic!("timed out waiting for {description}; observed log: {log:#?}");
        }

        let delta = tokio::time::timeout(deadline - now, stream.next())
            .await
            .unwrap_or_else(|_| {
                panic!("timed out waiting for {description}; observed log: {log:#?}")
            })
            .unwrap_or_else(|| {
                panic!("subscription stream closed while waiting for {description}")
            });

        log.push(delta);
    }
}

#[allow(dead_code)]
/// Collects any subscription deltas that arrive within a fixed window.
///
/// This is useful for asserting that no extra updates were broadcast after an
/// operation, while still recording any unexpected deltas for debug output.
pub async fn collect_stream_deltas(
    stream: &mut SubscriptionStream,
    log: &mut Vec<OrderedRowDelta>,
    duration: Duration,
) {
    let deadline = tokio::time::Instant::now() + duration;

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return;
        }

        let next_wait = (deadline - now).min(DEFAULT_STREAM_POLL_INTERVAL);
        match tokio::time::timeout(next_wait, stream.next()).await {
            Ok(Some(delta)) => log.push(delta),
            Ok(None) => return,
            Err(_) => continue,
        }
    }
}

#[allow(dead_code)]
/// Returns true if any logged subscription delta contains `id` in its added set.
pub fn has_added(log: &[OrderedRowDelta], id: ObjectId) -> bool {
    log.iter()
        .any(|delta| delta.added.iter().any(|change| change.id == id))
}

#[allow(dead_code)]
/// Returns true if any logged subscription delta contains `id` in its removed set.
pub fn has_removed(log: &[OrderedRowDelta], id: ObjectId) -> bool {
    log.iter()
        .any(|delta| delta.removed.iter().any(|change| change.id == id))
}

#[allow(dead_code)]
/// Returns true if any logged subscription delta contains `id` in its updated set.
pub fn has_updated(log: &[OrderedRowDelta], id: ObjectId) -> bool {
    log.iter()
        .any(|delta| delta.updated.iter().any(|change| change.id == id))
}

#[allow(dead_code)]
/// Returns true if any logged subscription delta references `id` as an add,
/// update, or removal.
pub fn has_any_change(log: &[OrderedRowDelta], id: ObjectId) -> bool {
    has_added(log, id) || has_updated(log, id) || has_removed(log, id)
}

#[allow(dead_code)]
pub fn has_row(rows: &QueryRows, row_id: ObjectId, expected: &[Value]) -> bool {
    rows.iter()
        .any(|(id, values)| *id == row_id && values.as_slice() == expected)
}

#[allow(dead_code)]
pub fn lacks_row(rows: &QueryRows, row_id: ObjectId) -> bool {
    rows.iter().all(|(id, _)| *id != row_id)
}
