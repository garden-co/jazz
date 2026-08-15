use std::sync::{Arc, RwLock as StdRwLock};
use std::thread;

use crate::serving::StorageConfig;
use crate::tools::AppId;
use crate::tools::middleware::AuthConfig;
use crate::tools::middleware::auth::JwtVerifier;

mod builder;
mod catalogue;
mod catalogue_entry;
mod catalogue_storage;
mod core_server_shell;
pub mod core_websocket_transport;
pub(crate) mod public_schema_convert;
pub mod routes;
pub(crate) mod runtime_catalogue;
mod shutdown;
#[cfg(feature = "test-utils")]
mod testing;

pub use builder::{BuiltServer, ServerBuilder, StorageBackend};
pub(crate) use catalogue::{PermissionsHeadSummary, ServerCatalogue, StoredCatalogue};
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
    /// Direct, storage-backed admin catalogue store.
    pub(crate) catalogue_store: StoredCatalogue,
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
    /// Sendable handle to the local-owner server shell for the websocket route.
    pub(crate) core_server_shell: StdRwLock<Option<core_server_shell::ServerShellHandle>>,
    pub(crate) core_server_shell_storage_config: Option<StorageConfig>,
    pub shutdown: ShutdownController,
}

impl ServerState {
    pub(crate) fn core_server_shell(&self) -> Option<core_server_shell::ServerShellHandle> {
        self.core_server_shell.read().unwrap().clone()
    }

    pub(crate) fn start_core_server_shell(
        &self,
        schema: crate::schema::JazzSchema,
    ) -> Result<core_server_shell::ServerShellHandle, String> {
        if let Some(core_server_shell) = self.core_server_shell() {
            return Ok(core_server_shell);
        }

        let storage_config = self
            .core_server_shell_storage_config
            .clone()
            .ok_or_else(|| "server shell storage is not configured".to_owned())?;
        let mut core_server_shell = self.core_server_shell.write().unwrap();
        if let Some(existing) = core_server_shell.clone() {
            return Ok(existing);
        }
        let started =
            core_server_shell::ServerShellHandle::start_with_storage(schema, storage_config)?;
        *core_server_shell = Some(started.clone());
        Ok(started)
    }

    /// Start (once) and await the server-wide teardown barrier.
    ///
    /// The first caller only launches the owned finalizer; it does not own the
    /// finalizer future. The finalizer runs on a dedicated lifecycle thread
    /// with its own Tokio runtime, so HTTP request cancellation, test-task
    /// abortion, and shutdown of the initiating runtime cannot strand another
    /// lifecycle caller in a transient phase while the same durable storage
    /// path is reopened.
    pub async fn run_shutdown_finalization(self: &Arc<Self>) -> ShutdownPhase {
        if self.shutdown.try_begin_finalization() {
            self.shutdown.set_phase(ShutdownPhase::ShuttingDown);
            let state = Arc::clone(self);
            if let Err(error) = thread::Builder::new()
                .name("jazz-server-finalizer".to_owned())
                .spawn(move || run_shutdown_finalizer(state))
            {
                tracing::error!(%error, "failed to start shutdown finalizer thread");
                // The finalizer never started, so `Failed` is the only
                // truthful terminal result. It must not be mistaken for a
                // storage-close/reopen barrier.
                self.shutdown.set_phase(ShutdownPhase::Failed);
            }
        }
        self.shutdown.wait_for_finalization().await
    }

    async fn finalize_shutdown(&self) -> ShutdownPhase {
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
        // All websocket work has drained, so no route may still be using the
        // shell. Join its dedicated owner thread before exposing this server's
        // durable paths to a reopen or allowing process teardown to proceed.
        let shell = self.core_server_shell.write().unwrap().take();
        if let Some(shell) = shell
            && let Err(error) = shell.shutdown().await
        {
            tracing::error!(%error, "shutdown server shell storage failed");
            failed = true;
        }
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

fn run_shutdown_finalizer(state: Arc<ServerState>) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to create shutdown runtime: {error}"))?;
        Ok::<_, String>(runtime.block_on(state.finalize_shutdown()))
    }));

    match result {
        Ok(Ok(_terminal)) => {}
        Ok(Err(error)) => {
            tracing::error!(%error, "shutdown finalizer setup failed");
            state.shutdown.set_phase(ShutdownPhase::Failed);
        }
        Err(_) => {
            tracing::error!("shutdown finalizer panicked");
            // A panic may leave resources live. Do not publish
            // `StorageClosed`; callers must treat Failed as unsafe to reopen.
            state.shutdown.set_phase(ShutdownPhase::Failed);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::tools::AppId;
    use crate::tools::middleware::AuthConfig;
    use crate::tools::public_api::types::{ColumnType, Schema, SchemaBuilder, TableSchema};
    use crate::tools::server::builder::{ServerBuilder, StorageBackend};
    use crate::tools::server::catalogue_storage::CatalogueStorageResult;

    struct CloseObservingStorage {
        close_calls: Arc<AtomicUsize>,
    }

    struct PanicFlushStorage;

    impl CatalogueStorage for CloseObservingStorage {
        fn scan_catalogue_entries(
            &self,
        ) -> CatalogueStorageResult<Vec<crate::tools::server::catalogue_entry::CatalogueEntry>>
        {
            Ok(Vec::new())
        }

        fn upsert_catalogue_entry(
            &mut self,
            _entry: &crate::tools::server::catalogue_entry::CatalogueEntry,
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

    impl CatalogueStorage for PanicFlushStorage {
        fn scan_catalogue_entries(
            &self,
        ) -> CatalogueStorageResult<Vec<crate::tools::server::catalogue_entry::CatalogueEntry>>
        {
            Ok(Vec::new())
        }

        fn upsert_catalogue_entry(
            &mut self,
            _entry: &crate::tools::server::catalogue_entry::CatalogueEntry,
        ) -> CatalogueStorageResult<()> {
            Ok(())
        }

        fn flush(&self) -> CatalogueStorageResult<()> {
            panic!("test finalizer panic")
        }

        fn flush_wal(&self) -> CatalogueStorageResult<()> {
            Ok(())
        }

        fn close(&self) -> CatalogueStorageResult<()> {
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
            catalogue_store: StoredCatalogue::with_test_observability(
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
            core_server_shell: StdRwLock::new(None),
            core_server_shell_storage_config: None,
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

    #[tokio::test]
    async fn shutdown_finalization_closes_dynamic_catalogue_storage_after_drain() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let state = build_test_state_with_storage(
            Box::new(CloseObservingStorage {
                close_calls: Arc::clone(&close_calls),
            }),
            Duration::from_millis(10),
        );

        state.shutdown.request_shutdown();
        let phase = state.run_shutdown_finalization().await;

        assert_eq!(phase, ShutdownPhase::StorageClosed);
        assert_eq!(state.shutdown.phase(), ShutdownPhase::StorageClosed);
        assert_eq!(
            close_calls.load(Ordering::SeqCst),
            1,
            "drained shutdown must explicitly close dynamic catalogue storage"
        );
    }

    #[tokio::test]
    async fn concurrent_shutdown_finalizers_wait_for_the_same_storage_close() {
        let state = build_test_state_with_shutdown_timeout(Duration::from_secs(1)).await;
        let request = state
            .shutdown
            .try_enter_app_request()
            .expect("running server accepts request");
        state.shutdown.request_shutdown();

        let first_state = Arc::clone(&state);
        let first = tokio::spawn(async move { first_state.run_shutdown_finalization().await });

        let mut phases = state.shutdown.subscribe();
        while *phases.borrow_and_update() != ShutdownPhase::DrainingConnections {
            phases
                .changed()
                .await
                .expect("first finalizer remains alive");
        }

        let second_state = Arc::clone(&state);
        let second = tokio::spawn(async move { second_state.run_shutdown_finalization().await });
        tokio::task::yield_now().await;
        assert!(
            !second.is_finished(),
            "a reentrant finalizer must wait for the durable-close barrier"
        );

        drop(request);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), first)
                .await
                .expect("first finalizer completes")
                .expect("first finalizer task"),
            ShutdownPhase::StorageClosed
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .expect("second finalizer completes")
                .expect("second finalizer task"),
            ShutdownPhase::StorageClosed
        );
    }

    #[tokio::test]
    async fn concurrent_shutdown_finalizers_share_failed_result() {
        let state = build_test_state_with_shutdown_timeout(Duration::from_millis(10)).await;
        let _request = state
            .shutdown
            .try_enter_app_request()
            .expect("running server accepts request");
        state.shutdown.request_shutdown();

        let first_state = Arc::clone(&state);
        let first = tokio::spawn(async move { first_state.run_shutdown_finalization().await });
        let second_state = Arc::clone(&state);
        let second = tokio::spawn(async move { second_state.run_shutdown_finalization().await });

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), first)
                .await
                .expect("first failed finalizer completes")
                .expect("first finalizer task"),
            ShutdownPhase::Failed
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .expect("second failed finalizer completes")
                .expect("second finalizer task"),
            ShutdownPhase::Failed
        );
    }

    #[tokio::test]
    async fn aborting_first_shutdown_waiter_does_not_strand_finalization() {
        let state = build_test_state_with_shutdown_timeout(Duration::from_secs(1)).await;
        let request = state
            .shutdown
            .try_enter_app_request()
            .expect("running server accepts request");
        state.shutdown.request_shutdown();

        let first_state = Arc::clone(&state);
        let first = tokio::spawn(async move { first_state.run_shutdown_finalization().await });
        let mut phases = state.shutdown.subscribe();
        while *phases.borrow_and_update() != ShutdownPhase::DrainingConnections {
            phases
                .changed()
                .await
                .expect("detached finalizer remains alive");
        }

        first.abort();
        let _ = first.await;
        drop(request);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), state.run_shutdown_finalization())
                .await
                .expect("later shutdown caller reaches terminal result"),
            ShutdownPhase::StorageClosed
        );
    }

    #[tokio::test]
    async fn panicked_shutdown_finalizer_publishes_failed_to_later_callers() {
        let state =
            build_test_state_with_storage(Box::new(PanicFlushStorage), Duration::from_secs(1));
        state.shutdown.request_shutdown();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), state.run_shutdown_finalization())
                .await
                .expect("initial waiter receives panic result"),
            ShutdownPhase::Failed
        );
        assert_eq!(
            state.run_shutdown_finalization().await,
            ShutdownPhase::Failed,
            "later callers share Failed rather than a transient phase"
        );
    }

    #[tokio::test]
    async fn foreign_executor_shutdown_uses_dedicated_lifecycle_thread() {
        let state = build_test_state_with_shutdown_timeout(Duration::from_secs(1)).await;
        // The caller's executor is irrelevant: the finalizer has its own
        // current-thread Tokio runtime.
        state.shutdown.request_shutdown();
        let foreign_state = Arc::clone(&state);
        assert_eq!(
            std::thread::spawn(move || {
                futures::executor::block_on(foreign_state.run_shutdown_finalization())
            })
            .join()
            .expect("foreign executor thread"),
            ShutdownPhase::StorageClosed,
            "foreign executor waits for the real durable-close barrier"
        );
    }
}
