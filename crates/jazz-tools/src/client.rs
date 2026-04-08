//! JazzClient implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::jazz_tokio::{SubscriptionHandle as RuntimeSubHandle, TokioRuntime};
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
use crate::sync_manager::{ClientId, DurabilityTier, ServerId, SyncManager};
use base64::Engine;
use serde::Deserialize;
use tokio::sync::{RwLock, mpsc};

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
    /// Active subscriptions (metadata).
    subscriptions: Arc<RwLock<HashMap<SubscriptionHandle, SubscriptionState>>>,
    /// Next subscription handle ID.
    next_handle: std::sync::atomic::AtomicU64,
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

        let storage: DynStorage = match context.storage {
            ClientStorage::Persistent => open_persistent_storage(&context.data_dir).await?,
            ClientStorage::Memory => Box::new(MemoryStorage::new()),
        };

        let schema_manager = build_client_schema_manager(&storage, &context)?;
        let server_id = ServerId::default();

        // Create runtime (outbox entries are sent via TransportHandle channels)
        let runtime = TokioRuntime::new(schema_manager, storage);

        // Persist schema to catalogue for server sync
        runtime
            .persist_schema()
            .map_err(|e| JazzError::Storage(e.to_string()))?;

        // Connect to server via WebSocket if URL provided
        if !context.server_url.is_empty() {
            let ws_url = if context.server_url.starts_with("ws://")
                || context.server_url.starts_with("wss://")
            {
                format!("{}/ws", context.server_url)
            } else {
                // Convert http:// to ws:// for backward compatibility
                let base = context
                    .server_url
                    .replace("https://", "wss://")
                    .replace("http://", "ws://");
                format!("{}/ws", base)
            };

            let ws_auth = crate::transport_ws::WsAuthConfig {
                client_id: Some(client_id.to_string()),
                auth: if let Some(ref secret) = context.backend_secret {
                    crate::transport_ws::AuthPayload::Backend {
                        secret: secret.clone(),
                        session: default_session
                            .as_ref()
                            .map(|s| serde_json::to_string(s).unwrap_or_default())
                            .unwrap_or_default(),
                    }
                } else if let Some(ref token) = context.jwt_token {
                    crate::transport_ws::AuthPayload::Jwt {
                        token: token.clone(),
                    }
                } else {
                    crate::transport_ws::AuthPayload::None
                },
                admin_secret: context.admin_secret.clone(),
                catalogue_state_hash: None,
            };

            let signal = runtime
                .connect(ws_url, ws_auth)
                .map_err(|e| JazzError::Connection(e.to_string()))?;

            // Wait for the server's Connected event (carries catalogue_state_hash)
            let catalogue_state_hash = tokio::time::timeout(Duration::from_secs(10), signal.rx)
                .await
                .map_err(|_| {
                    JazzError::Connection("timed out waiting for WebSocket Connected event".into())
                })?
                .map_err(|_| {
                    JazzError::Connection(
                        "WebSocket connection closed before Connected event".into(),
                    )
                })?;

            // Register server with sync manager, using the catalogue hash for delta sync
            if let Err(e) = runtime
                .add_server_with_catalogue_state_hash(server_id, catalogue_state_hash.as_deref())
            {
                tracing::warn!("Failed to register server with sync manager: {}", e);
            }
        }

        Ok(Self {
            declared_schema,
            default_session,
            runtime,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            next_handle: std::sync::atomic::AtomicU64::new(1),
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
        // With WebSocket transport, the connection is managed by TransportManager
        // inside RuntimeCore. For now, return true if we have a runtime.
        true
    }

    /// Create a session-scoped client for backend operations.
    pub fn for_session(&self, session: Session) -> SessionClient<'_> {
        SessionClient {
            client: self,
            session,
        }
    }

    /// Shutdown the client and release resources.
    pub async fn shutdown(self) -> Result<()> {
        // Disconnect WebSocket transport (drops channels, TransportManager exits)
        let _ = self.runtime.disconnect();

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
    use crate::runtime_core::{NoopScheduler, RuntimeCore};
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
        let mut runtime = RuntimeCore::new(schema_manager, storage, NoopScheduler);
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
