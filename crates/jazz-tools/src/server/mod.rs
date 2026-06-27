use std::sync::{Arc, RwLock as StdRwLock};

use crate::middleware::AuthConfig;
use crate::middleware::auth::JwtVerifier;
use crate::schema_manager::AppId;
use jazz_server::StorageConfig;

mod builder;
mod catalogue;
mod catalogue_storage;
mod core_server;
pub mod direct_client;
pub(crate) mod direct_schema;
pub mod routes;
mod shutdown;
#[cfg(feature = "test-utils")]
mod testing;

pub use builder::{BuiltServer, ServerBuilder, StorageBackend};
pub(crate) use catalogue::{DirectCatalogueStore, ServerCatalogue};
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
pub(crate) use catalogue_storage::CatalogueRocksDbStorage;
#[cfg(test)]
pub(crate) use catalogue_storage::CatalogueStorage;
pub(crate) use catalogue_storage::{CatalogueMemoryStorage, DynCatalogueStorage};
pub use shutdown::{ShutdownController, ShutdownPhase};
#[cfg(feature = "test-utils")]
pub use testing::{JazzServer, JazzServerBuilder, ServerDataDir, TestJwtIssuer, TestJwtOptions};

/// Cap on concurrent connections sharing a single `client_id`. When a new
/// connection would exceed this cap, the oldest connection(s) for the same
/// `client_id` are evicted so a reconnecting client is never locked out by
/// its own zombies. Bounds the fan-out memory described in jaz0-a803.
///
/// Value of 4 gives headroom for the realistic legitimate case (a brief
/// overlap between an old half-open socket and a new reconnect, plus a
/// small amount of slack for unusual topologies) without giving an
/// attacker meaningful amplification before the cap bites.
pub(crate) const PER_CLIENT_CONNECTION_CAP: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServerTopology {
    #[default]
    Core,
    Edge,
}

impl ServerTopology {
    pub fn is_edge(self) -> bool {
        matches!(self, Self::Edge)
    }
}

/// Server state shared across request handlers.
pub struct ServerState {
    /// Direct, storage-backed admin catalogue store. Production websocket sync,
    /// row storage, query execution, and client lifecycle are owned by
    /// the local-owner core server handle.
    pub(crate) catalogue_store: DirectCatalogueStore,
    pub(crate) catalogue: ServerCatalogue,
    #[allow(dead_code)]
    pub app_id: AppId,
    /// Authentication configuration.
    pub auth_config: AuthConfig,
    /// Upstream HTTP base URL used by edge servers to forward catalogue HTTP requests.
    pub upstream_http_url: Option<String>,
    /// Whether this process is the core/global node or an edge syncing upstream.
    pub topology: ServerTopology,
    /// Shared HTTP client for forwarding admin requests to a remote authority.
    pub http_client: reqwest::Client,
    /// Configured verifier for external JWTs.
    pub jwt_verifier: Option<Arc<JwtVerifier>>,
    /// Sendable handle to the local-owner jazz_core peer loop for the direct websocket route.
    pub(crate) core_server: StdRwLock<Option<core_server::LocalCoreServerHandle>>,
    pub(crate) core_server_storage_config: Option<StorageConfig>,
    pub shutdown: ShutdownController,
}

impl ServerState {
    pub(crate) fn core_server(&self) -> Option<core_server::LocalCoreServerHandle> {
        self.core_server.read().unwrap().clone()
    }

    pub(crate) fn start_core_server(
        &self,
        schema: jazz::schema::JazzSchema,
    ) -> Result<core_server::LocalCoreServerHandle, String> {
        if let Some(core_server) = self.core_server() {
            return Ok(core_server);
        }

        let storage_config = self
            .core_server_storage_config
            .clone()
            .ok_or_else(|| "core server storage is not configured".to_owned())?;
        let mut core_server = self.core_server.write().unwrap();
        if let Some(existing) = core_server.clone() {
            return Ok(existing);
        }
        let started =
            core_server::LocalCoreServerHandle::start_with_storage(schema, storage_config)?;
        *core_server = Some(started.clone());
        Ok(started)
    }

    pub async fn run_shutdown_finalization(&self) -> ShutdownPhase {
        if !self.shutdown.try_begin_finalization() {
            return self.shutdown.phase();
        }

        self.shutdown.set_phase(ShutdownPhase::DrainingConnections);
        let mut failed = false;
        let websockets_drained = self.shutdown.wait_for_websocket_drain().await;
        if !websockets_drained {
            tracing::warn!(
                active_websockets = self.shutdown.active_websockets(),
                "shutdown websocket drain timed out"
            );
            failed = true;
        }

        let app_requests_drained = self.shutdown.wait_for_app_request_drain().await;
        if !app_requests_drained {
            tracing::warn!(
                active_app_requests = self.shutdown.active_app_requests(),
                "shutdown app request drain timed out"
            );
            failed = true;
        }

        if failed {
            self.shutdown.set_phase(ShutdownPhase::Failed);
            return ShutdownPhase::Failed;
        }

        self.shutdown.set_phase(ShutdownPhase::FlushingRuntime);
        if let Err(error) = self.catalogue.flush(&self.catalogue_store) {
            tracing::error!(%error, "shutdown catalogue store flush failed");
            failed = true;
        }

        self.shutdown.set_phase(ShutdownPhase::ClosingStorage);
        if let Err(error) = self.catalogue.close(&self.catalogue_store) {
            tracing::error!(%error, "shutdown catalogue storage close failed");
            failed = true;
        }

        if failed {
            self.shutdown.set_phase(ShutdownPhase::Failed);
            ShutdownPhase::Failed
        } else {
            self.shutdown.set_phase(ShutdownPhase::StorageClosed);
            ShutdownPhase::StorageClosed
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::middleware::AuthConfig;
    use crate::query_manager::types::{ColumnType, Schema, SchemaBuilder, TableSchema};
    use crate::schema_manager::AppId;
    use crate::server::builder::{ServerBuilder, StorageBackend};
    use crate::server::catalogue_storage::CatalogueStorageResult;

    struct CloseObservingStorage {
        close_calls: Arc<AtomicUsize>,
    }

    impl CatalogueStorage for CloseObservingStorage {
        fn scan_catalogue_entries(
            &self,
        ) -> CatalogueStorageResult<Vec<crate::catalogue::CatalogueEntry>> {
            Ok(Vec::new())
        }

        fn upsert_catalogue_entry(
            &mut self,
            _entry: &crate::catalogue::CatalogueEntry,
        ) -> CatalogueStorageResult<()> {
            Ok(())
        }

        fn flush(&self) -> CatalogueStorageResult<()> {
            Ok(())
        }

        fn flush_wal(&self) -> CatalogueStorageResult<()> {
            Ok(())
        }

        fn close(&self) -> CatalogueStorageResult<()> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn shutdown_test_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build()
    }

    async fn build_test_state_with_shutdown_timeout(timeout: Duration) -> Arc<ServerState> {
        let app_id = AppId::from_name("lifecycle-test");
        let built = ServerBuilder::new(app_id)
            .with_storage(StorageBackend::InMemory)
            .with_shutdown_timeout(timeout)
            .build()
            .await
            .expect("build test server");
        built.state
    }

    fn build_test_state_with_storage(
        storage: DynCatalogueStorage,
        timeout: Duration,
    ) -> Arc<ServerState> {
        let app_id = AppId::from_name("shutdown-storage-test");
        Arc::new(ServerState {
            catalogue_store: DirectCatalogueStore::with_test_observability(
                app_id,
                Some(shutdown_test_schema()),
                storage,
                Vec::new(),
                std::collections::HashSet::new(),
            ),
            catalogue: ServerCatalogue,
            app_id,
            auth_config: AuthConfig::default(),
            upstream_http_url: None,
            topology: ServerTopology::Core,
            http_client: reqwest::Client::builder()
                .build()
                .expect("build HTTP client"),
            jwt_verifier: None,
            core_server: StdRwLock::new(None),
            core_server_storage_config: None,
            shutdown: ShutdownController::new(timeout),
        })
    }

    #[tokio::test]
    async fn shutdown_finalization_marks_failed_after_app_request_drain_timeout() {
        let state = build_test_state_with_shutdown_timeout(Duration::from_millis(10)).await;
        let _request_guard = state
            .shutdown
            .try_enter_app_request()
            .expect("running server accepts request");

        state.shutdown.request_shutdown();
        let phase = state.run_shutdown_finalization().await;

        assert_eq!(phase, ShutdownPhase::Failed);
        assert_eq!(state.shutdown.phase(), ShutdownPhase::Failed);
    }

    #[tokio::test]
    async fn shutdown_finalization_does_not_close_storage_when_app_requests_remain_active() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let state = build_test_state_with_storage(
            Box::new(CloseObservingStorage {
                close_calls: Arc::clone(&close_calls),
            }),
            Duration::from_millis(10),
        );
        let _request_guard = state
            .shutdown
            .try_enter_app_request()
            .expect("running server accepts request");

        state.shutdown.request_shutdown();
        let phase = state.run_shutdown_finalization().await;

        assert_eq!(phase, ShutdownPhase::Failed);
        assert_eq!(
            close_calls.load(Ordering::SeqCst),
            0,
            "storage must not be closed while app request guards are still active"
        );
    }
}
