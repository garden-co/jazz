//! JazzClient implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::jazz_tokio::{SubscriptionHandle as RuntimeSubHandle, TokioRuntime};
use crate::jazz_transport::ServerEvent;
use crate::query_manager::manager::LocalUpdates;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::{OrderedRowDelta, RowDescriptor, Schema, TableName, Value};
use crate::runtime_core::ReadDurabilityOptions;
use crate::schema_manager::{SchemaManager, rehydrate_schema_manager_from_manifest};
#[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
use crate::storage::FjallStorage;
#[cfg(feature = "rocksdb")]
use crate::storage::RocksDBStorage;
use crate::storage::{MemoryStorage, Storage, StorageError};
use crate::sync_manager::{
    ClientId, Destination, DurabilityTier, InboxEntry, ServerId, Source, SyncManager, SyncPayload,
};
use base64::Engine;
use bytes::BytesMut;
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::{RwLock, mpsc, oneshot};

use crate::transport::{AuthConfig, ServerConnection};
use crate::{
    AppContext, ClientStorage, JazzError, ObjectId, Result, SubscriptionHandle, SubscriptionStream,
};

type DynStorage = Box<dyn Storage + Send>;
type ClientRuntime = TokioRuntime<DynStorage>;

#[derive(Debug, Deserialize)]
struct UnverifiedJwtClaims {
    sub: String,
    #[serde(default)]
    claims: serde_json::Value,
}

/// Jazz client for building applications.
///
/// Combines local storage with server sync.
pub struct JazzClient {
    /// Schema as declared by the client/app code.
    declared_schema: Schema,
    /// Session inferred from client auth context for user-scoped operations.
    default_session: Option<Session>,
    /// Handle to the local runtime.
    runtime: ClientRuntime,
    /// Connection to the server (shared for event processor).
    server_connection: Option<Arc<ServerConnection>>,
    /// Active subscriptions (metadata).
    subscriptions: Arc<RwLock<HashMap<SubscriptionHandle, SubscriptionState>>>,
    /// Next subscription handle ID.
    next_handle: std::sync::atomic::AtomicU64,
    /// Handle for the stream listener task.
    stream_listener_task: Option<tokio::task::JoinHandle<()>>,
}

/// State for an active subscription.
struct SubscriptionState {
    runtime_handle: RuntimeSubHandle,
}

fn build_client_schema_manager<S: Storage + ?Sized>(
    storage: &S,
    context: &AppContext,
) -> Result<SchemaManager> {
    let sync_manager = SyncManager::new();
    let mut schema_manager = SchemaManager::new(
        sync_manager,
        context.schema.clone(),
        context.app_id,
        "client",
        "main",
    )
    .map_err(|e| JazzError::Schema(format!("{:?}", e)))?;

    rehydrate_schema_manager_from_manifest(&mut schema_manager, storage, context.app_id)
        .map_err(JazzError::Storage)?;

    Ok(schema_manager)
}

fn session_from_unverified_jwt(token: &str) -> Option<Session> {
    let payload = token.split('.').nth(1)?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let claims: UnverifiedJwtClaims = serde_json::from_slice(&payload).ok()?;
    let user_id = claims.sub.trim();
    if user_id.is_empty() {
        return None;
    }

    Some(Session {
        user_id: user_id.to_string(),
        claims: claims.claims,
    })
}

fn default_session_from_context(context: &AppContext) -> Option<Session> {
    if context.backend_secret.is_some() || context.admin_secret.is_some() {
        return None;
    }

    context
        .jwt_token
        .as_deref()
        .and_then(session_from_unverified_jwt)
}

impl JazzClient {
    /// Connect to Jazz with the given configuration.
    ///
    /// This will:
    /// 1. Open local storage
    /// 2. Initialize the runtime
    /// 3. Connect to the server (if URL provided)
    /// 4. Start syncing
    pub async fn connect(context: AppContext) -> Result<Self> {
        let declared_schema = context.schema.clone();
        let default_session = default_session_from_context(&context);
        let client_id = match context.storage {
            ClientStorage::Persistent => load_or_create_persistent_client_id(&context)?,
            ClientStorage::Memory => context.client_id.unwrap_or_default(),
        };

        // Connect to server if URL provided (before creating runtime so we have the connection)
        let auth_config = AuthConfig::from_context(&context);
        let server_connection = if !context.server_url.is_empty() {
            match ServerConnection::connect(&context.server_url, auth_config).await {
                Ok(conn) => Some(Arc::new(conn)),
                Err(e) => {
                    tracing::warn!("Failed to connect to server: {}", e);
                    None
                }
            }
        } else {
            None
        };

        let storage: DynStorage = match context.storage {
            ClientStorage::Persistent => open_persistent_storage(&context.data_dir).await?,
            ClientStorage::Memory => Box::new(MemoryStorage::new()),
        };

        let schema_manager = build_client_schema_manager(&storage, &context)?;

        // Clone server connection for sync callback
        let server_conn_for_sync = server_connection.clone();
        let client_id_for_sync = client_id;
        let server_id = ServerId::default();

        // Create runtime with sync callback
        let runtime = TokioRuntime::new(schema_manager, storage, move |entry| {
            // Send to server if connected and destination is server
            if let Destination::Server(_) = entry.destination
                && let Some(ref conn) = server_conn_for_sync
            {
                let conn = conn.clone();
                let payload = entry.payload.clone();
                let cid = client_id_for_sync;
                tokio::spawn(async move {
                    if let Some(delay) = test_send_delay_for_object_updated(&payload) {
                        tokio::time::sleep(delay).await;
                    }

                    if let Err(e) = conn.push_sync(payload, cid).await {
                        tracing::warn!("Failed to push sync to server: {}", e);
                    }
                });
            }
        });

        // Persist schema to catalogue for server sync
        runtime
            .persist_schema()
            .map_err(|e| JazzError::Storage(e.to_string()))?;

        // Spawn binary stream listener if connected to server
        let (initial_stream_ready_tx, initial_stream_ready_rx) = if server_connection.is_some() {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let stream_listener_task = if let Some(ref conn) = server_connection {
            let conn_for_stream = conn.clone();
            let client_id_str = client_id.to_string();
            let runtime_for_stream = runtime.clone();
            let stream_headers = conn.build_stream_headers();
            let server_id_for_stream = server_id;
            let mut initial_stream_ready_tx = initial_stream_ready_tx;

            Some(tokio::spawn(async move {
                let http_client = reqwest::Client::new();
                loop {
                    let url = conn_for_stream.stream_url(&client_id_str);

                    tracing::info!("Connecting to server event stream: {}", url);

                    match http_client
                        .get(&url)
                        .headers(stream_headers.clone())
                        .send()
                        .await
                    {
                        Ok(response) => {
                            if !response.status().is_success() {
                                tracing::warn!(
                                    "Event stream connection failed: {}",
                                    response.status()
                                );
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                continue;
                            }

                            tracing::info!("Event stream connected");

                            let mut body = response.bytes_stream();
                            let mut buffer = BytesMut::new();

                            while let Some(chunk_result) = body.next().await {
                                match chunk_result {
                                    Ok(chunk) => {
                                        buffer.extend_from_slice(&chunk);

                                        // Read complete frames from buffer
                                        while buffer.len() >= 4 {
                                            let len =
                                                u32::from_be_bytes(buffer[..4].try_into().unwrap())
                                                    as usize;
                                            if buffer.len() < 4 + len {
                                                break; // Incomplete frame
                                            }
                                            let json = &buffer[4..4 + len];

                                            match serde_json::from_slice::<ServerEvent>(json) {
                                                Ok(event) => {
                                                    if matches!(
                                                        &event,
                                                        ServerEvent::Connected { .. }
                                                    ) && let Some(tx) =
                                                        initial_stream_ready_tx.take()
                                                    {
                                                        let catalogue_state_hash = match &event {
                                                            ServerEvent::Connected {
                                                                catalogue_state_hash,
                                                                ..
                                                            } => catalogue_state_hash.clone(),
                                                            _ => None,
                                                        };
                                                        let _ = tx.send(catalogue_state_hash);
                                                    }
                                                    if let Err(e) = handle_server_event(
                                                        event,
                                                        &runtime_for_stream,
                                                        server_id_for_stream,
                                                    ) {
                                                        tracing::warn!(
                                                            "Error handling server event: {}",
                                                            e
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::warn!(
                                                        "Failed to parse server event: {}",
                                                        e
                                                    );
                                                }
                                            }

                                            // Advance buffer past this frame
                                            let _ = buffer.split_to(4 + len);
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("Stream chunk error: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Event stream connection error: {}", e);
                        }
                    }

                    // Reconnect after delay
                    tracing::info!("Event stream disconnected, reconnecting in 5s...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }))
        } else {
            None
        };

        let initial_server_catalogue_state_hash =
            if let Some(initial_stream_ready_rx) = initial_stream_ready_rx {
                tokio::time::timeout(Duration::from_secs(10), initial_stream_ready_rx)
                    .await
                    .map_err(|_| {
                        JazzError::Connection(
                            "timed out waiting for server event stream to connect".to_string(),
                        )
                    })?
                    .map_err(|_| {
                        JazzError::Connection(
                            "server event stream ended before sending Connected".to_string(),
                        )
                    })?
            } else {
                None
            };

        // Register server with sync manager if connected.
        //
        // The initial Connected event carries the server's catalogue digest, so
        // we wait for it before deciding whether catalogue replay can be skipped.
        if server_connection.is_some()
            && let Err(e) = runtime.add_server_with_catalogue_state_hash(
                server_id,
                initial_server_catalogue_state_hash.as_deref(),
            )
        {
            tracing::warn!("Failed to register server with sync manager: {}", e);
        }

        Ok(Self {
            declared_schema,
            default_session,
            runtime,
            server_connection,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            next_handle: std::sync::atomic::AtomicU64::new(1),
            stream_listener_task,
        })
    }

    /// Subscribe to a query.
    ///
    /// Returns a stream of row deltas as the data changes.
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        self.subscribe_internal(query, self.default_session.clone())
            .await
    }

    /// Internal subscribe with optional session.
    async fn subscribe_internal(
        &self,
        query: Query,
        session: Option<Session>,
    ) -> Result<SubscriptionStream> {
        let handle = SubscriptionHandle(
            self.next_handle
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        // Create channel for this subscription's deltas.
        // tx is moved directly into the callback so the delta is never dropped due
        // to the race where immediate_tick fires the callback before we can insert
        // tx into a shared map.
        let (tx, rx) = mpsc::unbounded_channel::<OrderedRowDelta>();

        // Register with runtime using callback pattern
        // The callback bridges runtime updates to the channel
        let runtime_handle = self
            .runtime
            .subscribe(
                query.clone(),
                move |delta| {
                    // Route delta to the subscription stream without dropping
                    // updates when the consumer falls briefly behind.
                    let _ = tx.send(delta.ordered_delta);
                },
                session,
            )
            .map_err(|e| JazzError::Query(e.to_string()))?;

        // Track subscription metadata
        {
            let mut subs = self.subscriptions.write().await;
            subs.insert(handle, SubscriptionState { runtime_handle });
        }

        Ok(SubscriptionStream::new(rx))
    }

    /// One-shot query, optionally waiting for a durability tier.
    ///
    /// Returns the current results as `Vec<(ObjectId, Vec<Value>)>`.
    pub async fn query(
        &self,
        query: Query,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        let query_for_alignment = query.clone();
        let future = self
            .runtime
            .query(
                query,
                self.default_session.clone(),
                ReadDurabilityOptions {
                    tier: durability_tier,
                    local_updates: LocalUpdates::Immediate,
                },
            )
            .map_err(|e| JazzError::Query(e.to_string()))?;
        future
            .await
            .map(|rows| self.align_query_rows_to_declared_schema(&query_for_alignment, rows))
            .map_err(|e| JazzError::Query(format!("{:?}", e)))
    }

    /// Create a new row in a table.
    pub async fn create(
        &self,
        table: &str,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>)> {
        let (object_id, row_values) = self
            .runtime
            .insert(table, values, None)
            .map_err(|e| JazzError::Write(e.to_string()))?;
        let row_values = match self.runtime.current_schema() {
            Ok(schema) => align_row_values_to_declared_schema(
                &self.declared_schema,
                &schema,
                &TableName::new(table),
                row_values,
            ),
            Err(_) => row_values,
        };
        Ok((object_id, row_values))
    }

    /// Update a row.
    pub async fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<()> {
        self.runtime
            .update(object_id, updates, None)
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Delete a row.
    pub async fn delete(&self, object_id: ObjectId) -> Result<()> {
        self.runtime
            .delete(object_id, None)
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        if let Some(state) = subs.remove(&handle) {
            let _ = self.runtime.unsubscribe(state.runtime_handle);
        }
        Ok(())
    }

    /// Get the current schema.
    pub async fn schema(&self) -> Result<crate::query_manager::types::Schema> {
        self.runtime
            .current_schema()
            .map_err(|e| JazzError::Query(e.to_string()))
    }

    /// Check if connected to server.
    pub fn is_connected(&self) -> bool {
        self.server_connection.is_some()
    }

    /// Create a session-scoped client for backend operations.
    pub fn for_session(&self, session: Session) -> SessionClient<'_> {
        SessionClient {
            client: self,
            session,
        }
    }

    /// Shutdown the client and release resources.
    pub async fn shutdown(mut self) -> Result<()> {
        // Abort stream listener first (it holds TokioRuntime clone)
        if let Some(handle) = self.stream_listener_task.take() {
            handle.abort();
            let _ = handle.await;
        }

        // Flush pending operations
        self.runtime
            .flush()
            .await
            .map_err(|e| JazzError::Connection(e.to_string()))?;

        // Flush storage state to disk for persistence
        self.runtime
            .with_storage(|storage| {
                storage.flush();
                storage.close()
            })
            .map_err(|e| JazzError::Storage(e.to_string()))?
            .map_err(|e| JazzError::Storage(e.to_string()))?;

        Ok(())
    }

    fn align_query_rows_to_declared_schema(
        &self,
        query: &Query,
        rows: Vec<(ObjectId, Vec<Value>)>,
    ) -> Vec<(ObjectId, Vec<Value>)> {
        if !query_rows_can_be_schema_aligned(query) {
            return rows;
        }

        let runtime_schema = match self.runtime.current_schema() {
            Ok(schema) => schema,
            Err(_) => return rows,
        };

        rows.into_iter()
            .map(|(id, values)| {
                (
                    id,
                    align_row_values_to_declared_schema(
                        &self.declared_schema,
                        &runtime_schema,
                        &query.table,
                        values,
                    ),
                )
            })
            .collect()
    }
}

/// Session-scoped client for backend operations.
pub struct SessionClient<'a> {
    client: &'a JazzClient,
    session: Session,
}

impl<'a> SessionClient<'a> {
    pub async fn create(
        &self,
        table: &str,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>)> {
        let (object_id, row_values) = self
            .client
            .runtime
            .insert(table, values, Some(&self.session))
            .map_err(|e| JazzError::Write(e.to_string()))?;
        let row_values = match self.client.runtime.current_schema() {
            Ok(schema) => align_row_values_to_declared_schema(
                &self.client.declared_schema,
                &schema,
                &TableName::new(table),
                row_values,
            ),
            Err(_) => row_values,
        };
        Ok((object_id, row_values))
    }

    pub async fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<()> {
        self.client
            .runtime
            .update(object_id, updates, Some(&self.session))
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    pub async fn delete(&self, object_id: ObjectId) -> Result<()> {
        self.client
            .runtime
            .delete(object_id, Some(&self.session))
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    pub async fn query(
        &self,
        query: Query,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        let query_for_alignment = query.clone();
        let future = self
            .client
            .runtime
            .query(
                query,
                Some(self.session.clone()),
                ReadDurabilityOptions {
                    tier: durability_tier,
                    local_updates: LocalUpdates::Immediate,
                },
            )
            .map_err(|e| JazzError::Query(e.to_string()))?;
        future
            .await
            .map(|rows| {
                self.client
                    .align_query_rows_to_declared_schema(&query_for_alignment, rows)
            })
            .map_err(|e| JazzError::Query(format!("{:?}", e)))
    }

    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        self.client
            .subscribe_internal(query, Some(self.session.clone()))
            .await
    }
}

fn query_rows_can_be_schema_aligned(query: &Query) -> bool {
    query.joins.is_empty()
        && query.array_subqueries.is_empty()
        && query.recursive.is_none()
        && query.select_columns.is_none()
        && query.result_element_index.is_none()
}

fn align_row_values_to_declared_schema(
    declared_schema: &Schema,
    runtime_schema: &Schema,
    table: &TableName,
    values: Vec<Value>,
) -> Vec<Value> {
    let Some(declared_table) = declared_schema.get(table) else {
        return values;
    };
    let Some(runtime_table) = runtime_schema.get(table) else {
        return values;
    };

    reorder_values_by_column_name(&runtime_table.columns, &declared_table.columns, &values)
        .unwrap_or(values)
}

fn reorder_values_by_column_name(
    source_descriptor: &RowDescriptor,
    target_descriptor: &RowDescriptor,
    values: &[Value],
) -> Option<Vec<Value>> {
    if values.len() != source_descriptor.columns.len()
        || source_descriptor.columns.len() != target_descriptor.columns.len()
    {
        return None;
    }

    let mut values_by_column = HashMap::with_capacity(values.len());
    for (column, value) in source_descriptor.columns.iter().zip(values.iter()) {
        values_by_column.insert(column.name, value.clone());
    }

    let mut reordered_values = Vec::with_capacity(values.len());
    for column in &target_descriptor.columns {
        reordered_values.push(values_by_column.remove(&column.name)?);
    }

    Some(reordered_values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::policy::PolicyExpr;
    use crate::query_manager::types::{SchemaHash, TablePolicies};
    use crate::runtime_core::{NoopScheduler, RuntimeCore, VecSyncSender};
    use crate::schema_manager::AppId;
    use crate::storage::CatalogueManifestOp;
    #[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
    use crate::storage::FjallStorage;
    #[cfg(feature = "rocksdb")]
    use crate::storage::RocksDBStorage;
    use crate::{ColumnType, ObjectId, SchemaBuilder, TableSchema};
    use serde_json::json;
    use tempfile::TempDir;

    fn declared_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean),
            )
            .build()
    }

    fn runtime_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("completed", ColumnType::Boolean)
                    .column("title", ColumnType::Text),
            )
            .build()
    }

    fn learned_runtime_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean)
                    .nullable_column("description", ColumnType::Text),
            )
            .build()
    }

    fn make_offline_context(
        app_id: AppId,
        data_dir: std::path::PathBuf,
        schema: Schema,
    ) -> AppContext {
        AppContext {
            app_id,
            client_id: None,
            schema,
            server_url: String::new(),
            data_dir,
            storage: ClientStorage::default(),
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
        }
    }

    fn make_test_jwt(sub: &str, claims: serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "sub": sub,
                "claims": claims,
            }))
            .expect("serialize jwt payload"),
        );
        format!("{header}.{payload}.sig")
    }

    fn seed_rehydrated_client_storage(
        data_dir: &std::path::Path,
        app_id: AppId,
        publish_permissions: bool,
    ) -> (SchemaHash, SchemaHash) {
        std::fs::create_dir_all(data_dir).expect("create seeded client data dir");

        #[cfg(feature = "rocksdb")]
        let storage = {
            let db_path = data_dir.join("jazz.rocksdb");
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage")
        };
        #[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
        let storage = {
            let db_path = data_dir.join("jazz.fjall");
            FjallStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage")
        };

        let bundled_schema = declared_todo_schema();
        let learned_schema = learned_runtime_todo_schema();
        let bundled_hash = SchemaHash::compute(&bundled_schema);
        let learned_hash = SchemaHash::compute(&learned_schema);

        let schema_manager = SchemaManager::new(
            SyncManager::new(),
            learned_schema.clone(),
            app_id,
            "seed",
            "main",
        )
        .expect("seed schema manager");
        let mut runtime =
            RuntimeCore::new(schema_manager, storage, NoopScheduler, VecSyncSender::new());
        let learned_schema_object_id = runtime.persist_schema();
        let bundled_schema_object_id = runtime.publish_schema(bundled_schema.clone());
        let lens = runtime
            .schema_manager()
            .generate_lens(&bundled_schema, &learned_schema);
        assert!(!lens.is_draft(), "seed lens should be publishable");
        let lens_object_id = runtime.publish_lens(&lens).expect("persist learned lens");

        if publish_permissions {
            runtime
                .publish_permissions_bundle(
                    learned_hash,
                    HashMap::from([(
                        TableName::new("todos"),
                        TablePolicies::new().with_select(PolicyExpr::True),
                    )]),
                    None,
                )
                .expect("seed permissions bundle");
        }

        let mut storage = runtime.into_storage();
        storage
            .append_catalogue_manifest_ops(
                app_id.as_object_id(),
                &[
                    CatalogueManifestOp::SchemaSeen {
                        object_id: learned_schema_object_id,
                        schema_hash: learned_hash,
                    },
                    CatalogueManifestOp::SchemaSeen {
                        object_id: bundled_schema_object_id,
                        schema_hash: bundled_hash,
                    },
                    CatalogueManifestOp::LensSeen {
                        object_id: lens_object_id,
                        source_hash: bundled_hash,
                        target_hash: learned_hash,
                    },
                ],
            )
            .expect("append seeded client catalogue manifest ops");
        storage.flush();
        storage.close().expect("close seeded client storage");

        (bundled_hash, learned_hash)
    }

    fn expected_client_catalogue_hash(context: &AppContext) -> String {
        #[cfg(feature = "rocksdb")]
        let storage = {
            let db_path = context.data_dir.join("jazz.rocksdb");
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage")
        };
        #[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
        let storage = {
            let db_path = context.data_dir.join("jazz.fjall");
            FjallStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage")
        };
        let schema_manager = build_client_schema_manager(&storage, context)
            .expect("rehydrate client schema manager");
        let catalogue_hash = schema_manager.catalogue_state_hash();
        storage.close().expect("close seeded client storage");
        catalogue_hash
    }

    #[test]
    fn query_rows_are_reordered_back_to_declared_schema() {
        let aligned = align_row_values_to_declared_schema(
            &declared_todo_schema(),
            &runtime_todo_schema(),
            &TableName::new("todos"),
            vec![Value::Boolean(true), Value::Text("done".to_string())],
        );

        assert_eq!(
            aligned,
            vec![Value::Text("done".to_string()), Value::Boolean(true)]
        );
    }

    #[test]
    fn default_session_from_context_uses_jwt_claims_for_user_clients() {
        let app_id = AppId::from_name("client-jwt-session");
        let mut context = make_offline_context(
            app_id,
            TempDir::new().expect("tempdir").into_path(),
            declared_todo_schema(),
        );
        context.jwt_token = Some(make_test_jwt("alice", json!({ "join_code": "secret-123" })));

        let session = default_session_from_context(&context).expect("derive session from jwt");
        assert_eq!(session.user_id, "alice");
        assert_eq!(session.claims["join_code"], "secret-123");
    }

    #[test]
    fn default_session_from_context_skips_backend_capable_clients() {
        let app_id = AppId::from_name("client-backend-session");
        let mut context = make_offline_context(
            app_id,
            TempDir::new().expect("tempdir").into_path(),
            declared_todo_schema(),
        );
        context.jwt_token = Some(make_test_jwt("alice", json!({ "role": "user" })));
        context.backend_secret = Some("backend-secret".to_string());

        assert!(
            default_session_from_context(&context).is_none(),
            "backend/admin clients should keep using explicit SessionClient scopes"
        );
    }

    #[test]
    fn simple_queries_are_schema_alignable() {
        let query = Query::new("todos");
        assert!(query_rows_can_be_schema_aligned(&query));
    }

    #[test]
    fn join_queries_are_not_schema_alignable() {
        let mut query = Query::new("todos");
        query.joins.push(crate::query_manager::query::JoinSpec {
            table: TableName::new("projects"),
            alias: None,
            on: Some(("project_id".to_string(), "id".to_string())),
        });

        assert!(!query_rows_can_be_schema_aligned(&query));
    }

    #[test]
    fn query_alignment_preserves_row_identity() {
        let object_id = ObjectId::new();
        let aligned = vec![(
            object_id,
            align_row_values_to_declared_schema(
                &declared_todo_schema(),
                &runtime_todo_schema(),
                &TableName::new("todos"),
                vec![Value::Boolean(false), Value::Text("keep-id".to_string())],
            ),
        )];

        assert_eq!(aligned[0].0, object_id);
        assert_eq!(
            aligned[0].1,
            vec![Value::Text("keep-id".to_string()), Value::Boolean(false)]
        );
    }

    #[tokio::test]
    async fn client_rehydrates_learned_lens_from_local_catalogue_on_restart() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-rehydrate-lens");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, false);
        let context = make_offline_context(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
        );

        let client = JazzClient::connect(context).await.expect("connect client");

        let has_learned_schema = client
            .runtime
            .known_schema_hashes()
            .expect("read known schema hashes")
            .contains(&learned_hash);
        assert!(
            has_learned_schema,
            "client should restore newer learned schema"
        );

        let lens_path_len = client
            .runtime
            .with_schema_manager(|manager| manager.lens_path(&learned_hash).map(|path| path.len()))
            .expect("read client schema manager")
            .expect("lens path to bundled schema");
        assert_eq!(
            lens_path_len, 1,
            "client should restore learned migration lens"
        );

        client.shutdown().await.expect("shutdown client");
    }

    #[tokio::test]
    async fn client_rehydrates_permissions_head_and_bundle_from_local_catalogue_on_restart() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-rehydrate-permissions");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, true);
        let context = make_offline_context(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
        );
        let expected_catalogue_hash = expected_client_catalogue_hash(&context);

        let client = JazzClient::connect(context).await.expect("connect client");

        let actual_catalogue_hash = client
            .runtime
            .catalogue_state_hash()
            .expect("read client catalogue hash");
        assert_eq!(
            actual_catalogue_hash, expected_catalogue_hash,
            "client should restore learned permissions head and bundle before any network sync"
        );

        let lens_path_exists = client
            .runtime
            .with_schema_manager(|manager| manager.lens_path(&learned_hash).is_ok())
            .expect("read client schema manager");
        assert!(
            lens_path_exists,
            "permissions rehydrate should preserve the target schema's learned lens context"
        );

        client.shutdown().await.expect("shutdown client");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn open_persistent_storage_retries_on_lock_contention() {
        let data_dir = TempDir::new().expect("temp dir");
        std::fs::create_dir_all(data_dir.path()).unwrap();

        let db_path = data_dir.path().join("jazz.rocksdb");
        // Hold the DB open so the next open hits a lock error.
        let _holder =
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("first open should succeed");

        // Spawn a task that drops the holder after a short delay, unblocking the retry.
        let holder_handle = tokio::task::spawn_blocking({
            let holder = _holder;
            move || {
                std::thread::sleep(Duration::from_millis(150));
                drop(holder);
            }
        });

        // open_persistent_storage retries up to 100 times at 25ms intervals.
        // The holder is released after ~150ms, so this should succeed within a few retries.
        let storage = open_persistent_storage(data_dir.path()).await;
        assert!(
            storage.is_ok(),
            "should succeed after lock is released: {:?}",
            storage.err()
        );

        holder_handle.await.expect("holder task should complete");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn open_persistent_storage_fails_on_non_lock_error() {
        // Point at a file (not a directory) so RocksDB gets a non-lock IO error.
        let data_dir = TempDir::new().expect("temp dir");
        let db_path = data_dir.path().join("jazz.rocksdb");
        // Create a regular file where rocksdb expects a directory.
        std::fs::write(&db_path, b"not a database").unwrap();

        let result = open_persistent_storage(data_dir.path()).await;
        assert!(
            result.is_err(),
            "non-lock errors should not be retried and should fail immediately"
        );
    }
}

fn parse_delay_ms(raw: &str) -> Option<Duration> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some((min_raw, max_raw)) = trimmed.split_once('-') {
        let min = min_raw.trim().parse::<u64>().ok()?;
        let max = max_raw.trim().parse::<u64>().ok()?;
        if min > max {
            return None;
        }
        return Some(Duration::from_millis(min + ((max - min) / 2)));
    }

    trimmed.parse::<u64>().ok().map(Duration::from_millis)
}

fn test_send_delay_for_object_updated(payload: &SyncPayload) -> Option<Duration> {
    if !matches!(payload, SyncPayload::ObjectUpdated { .. }) {
        return None;
    }

    let delay = parse_delay_ms(&std::env::var("JAZZ_TEST_DELAY_SEND_OBJECT_UPDATED_MS").ok()?)?;
    let every_n = std::env::var("JAZZ_TEST_DELAY_SEND_OBJECT_UPDATED_EVERY")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(2);

    static OBJECT_UPDATED_SEND_COUNT: AtomicU64 = AtomicU64::new(0);
    let seq = OBJECT_UPDATED_SEND_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if !seq.is_multiple_of(every_n) {
        return None;
    }

    Some(delay)
}

/// Handle incoming server events.
fn handle_server_event(
    event: ServerEvent,
    runtime: &ClientRuntime,
    server_id: ServerId,
) -> Result<()> {
    fn short_hash(hash: &impl ToString) -> String {
        let hash = hash.to_string();
        hash.chars().take(12).collect()
    }

    match event {
        ServerEvent::Connected {
            connection_id,
            client_id,
            next_sync_seq,
            ..
        } => {
            tracing::info!(
                "Stream connected with id: {:?}, client_id: {}",
                connection_id,
                client_id
            );
            if let Some(next_sequence) = next_sync_seq {
                runtime
                    .set_server_next_sequence(server_id, next_sequence)
                    .map_err(|e| JazzError::Sync(e.to_string()))?;
            }
            Ok(())
        }
        ServerEvent::SyncUpdate { seq, payload } => {
            if let SyncPayload::SchemaWarning(warning) = payload.as_ref() {
                tracing::warn!(
                    query_id = warning.query_id.0,
                    table = warning.table_name,
                    row_count = warning.row_count,
                    from_hash = %warning.from_hash,
                    to_hash = %warning.to_hash,
                    "Detected {} rows of {} with differing schema versions. To ensure data visibility and forward/backward compatibility please create a new migration with `npx jazz-tools@alpha migrations create {} {}`",
                    warning.row_count,
                    warning.table_name,
                    short_hash(&warning.from_hash),
                    short_hash(&warning.to_hash),
                );
            }
            let entry = InboxEntry {
                source: Source::Server(server_id),
                payload: *payload,
            };
            if let Some(sequence) = seq {
                runtime
                    .push_sync_inbox_with_sequence(entry, sequence)
                    .map_err(|e| JazzError::Sync(e.to_string()))?;
            } else {
                runtime
                    .push_sync_inbox(entry)
                    .map_err(|e| JazzError::Sync(e.to_string()))?;
            }
            Ok(())
        }
        ServerEvent::Subscribed { query_id } => {
            tracing::debug!("Server acknowledged subscription: {:?}", query_id);
            Ok(())
        }
        ServerEvent::Error { message, code } => {
            tracing::error!("Server error {:?}: {}", code, message);
            Ok(())
        }
        ServerEvent::Heartbeat => {
            tracing::trace!("Heartbeat received");
            Ok(())
        }
    }
}

fn load_or_create_persistent_client_id(context: &AppContext) -> Result<ClientId> {
    std::fs::create_dir_all(&context.data_dir)?;

    let client_id_path = context.data_dir.join("client_id");
    let client_id = if client_id_path.exists() {
        let id_str = std::fs::read_to_string(&client_id_path)?;
        ClientId::parse(id_str.trim()).unwrap_or_else(|| {
            let id = context.client_id.unwrap_or_default();
            let _ = std::fs::write(&client_id_path, id.to_string());
            id
        })
    } else if let Some(id) = context.client_id {
        std::fs::write(&client_id_path, id.to_string())?;
        id
    } else {
        let id = ClientId::new();
        std::fs::write(&client_id_path, id.to_string())?;
        id
    };

    Ok(client_id)
}

async fn open_persistent_storage(data_dir: &std::path::Path) -> Result<DynStorage> {
    #[cfg(feature = "rocksdb")]
    {
        Ok(Box::new(open_rocksdb_storage(data_dir).await?))
    }
    #[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
    {
        Ok(Box::new(open_fjall_storage(data_dir).await?))
    }
    #[cfg(not(any(feature = "rocksdb", feature = "fjall")))]
    {
        tracing::warn!("no persistent storage backend enabled, falling back to MemoryStorage");
        Ok(Box::new(MemoryStorage::new()))
    }
}

#[cfg(feature = "rocksdb")]
async fn open_rocksdb_storage(data_dir: &std::path::Path) -> Result<RocksDBStorage> {
    const MAX_ATTEMPTS: usize = 100;
    const RETRY_DELAY_MS: u64 = 25;

    std::fs::create_dir_all(data_dir)?;

    let db_path = data_dir.join("jazz.rocksdb");
    let mut opened = None;
    let mut last_err = None;

    for attempt in 0..MAX_ATTEMPTS {
        match RocksDBStorage::open(&db_path, 64 * 1024 * 1024) {
            Ok(storage) => {
                opened = Some(storage);
                break;
            }
            Err(err) => {
                let is_lock_error = matches!(
                    &err,
                    StorageError::IoError(msg)
                        if msg.contains("lock") || msg.contains("Lock") || msg.contains("busy")
                );
                if !is_lock_error || attempt + 1 == MAX_ATTEMPTS {
                    last_err = Some(err);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }
    }

    opened.ok_or_else(|| {
        JazzError::Connection(format!(
            "failed to open rocksdb storage '{}': {:?}",
            db_path.display(),
            last_err
        ))
    })
}

#[cfg(all(feature = "fjall", not(feature = "rocksdb")))]
async fn open_fjall_storage(data_dir: &std::path::Path) -> Result<FjallStorage> {
    const MAX_ATTEMPTS: usize = 100;
    const RETRY_DELAY_MS: u64 = 25;

    std::fs::create_dir_all(data_dir)?;

    let db_path = data_dir.join("jazz.fjall");
    let mut opened = None;
    let mut last_err = None;

    for attempt in 0..MAX_ATTEMPTS {
        match FjallStorage::open(&db_path, 64 * 1024 * 1024) {
            Ok(storage) => {
                opened = Some(storage);
                break;
            }
            Err(err) => {
                let is_lock_error = matches!(
                    &err,
                    StorageError::IoError(msg)
                        if msg.contains("lock") || msg.contains("Lock") || msg.contains("busy")
                );
                if !is_lock_error || attempt + 1 == MAX_ATTEMPTS {
                    last_err = Some(err);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }
    }

    if let Some(storage) = opened {
        Ok(storage)
    } else {
        let err = last_err.unwrap_or_else(|| {
            StorageError::IoError("fjall open failed without error details".to_string())
        });
        Err(JazzError::Storage(format!("{:?}", err)))
    }
}
