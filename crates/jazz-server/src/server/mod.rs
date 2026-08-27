use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::thread;

use crate::middleware::AuthConfig;
use crate::middleware::auth::JwtVerifier;
use jazz::serving::StorageConfig;
use jazz::tools::AppId;

mod builder;
mod catalogue;
mod catalogue_entry;
mod catalogue_payload_codec;
mod catalogue_storage;
pub use jazz::serving::{ServerRuntimeActivity, ServerRuntimeFrameStream, ServerRuntimeHandle};
pub mod routes;
pub(crate) mod runtime_catalogue;
mod shutdown;
#[cfg(feature = "embedded-server")]
mod testing;

pub use builder::{BuiltServer, ServerBuilder, StorageBackend};
pub(crate) use catalogue::{PermissionsHeadSummary, ServerCatalogue, StoredCatalogue};
#[cfg(test)]
pub(crate) use catalogue_storage::CatalogueStorage;
pub(crate) use catalogue_storage::{
    CatalogueKvStorage, CatalogueMemoryStorage, DynCatalogueStorage,
};
pub use shutdown::{ShutdownController, ShutdownPhase};
#[cfg(feature = "embedded-server")]
pub use testing::{JazzServer, JazzServerBuilder, ServerDataDir, TestJwtIssuer, TestJwtOptions};

/// Publish catalogue inputs directly into an in-process test server.
#[cfg(feature = "embedded-server")]
pub async fn push_catalogue_in_memory(
    state: Arc<ServerState>,
    _app_id: AppId,
    _env: &str,
    schemas: &[jazz::tools::Schema],
    lenses: &[jazz::tools::Lens],
) -> Result<(), Box<dyn std::error::Error>> {
    for schema in schemas {
        state
            .catalogue
            .publish_schema(&state.catalogue_store, schema.clone())
            .map_err(|error| format!("publish schema to server catalogue: {error}"))?;
    }
    for lens in lenses {
        state
            .catalogue
            .publish_lens(&state.catalogue_store, lens)
            .map_err(|error| format!("publish lens to server catalogue: {error}"))?;
    }
    runtime_catalogue::publish_runtime_catalogue(&state, schemas, lenses)
        .await
        .map_err(|error| format!("bridge catalogue into server runtime: {error}"))?;
    state
        .catalogue
        .flush(&state.catalogue_store)
        .map_err(|error| format!("flush server catalogue: {error}"))?;
    Ok(())
}

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

/// Operational state of an edge server's owned upstream connector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EdgeUpstreamHealth {
    /// This server has no owned upstream connector.
    NotConfigured,
    /// Bootstrap or socket establishment is in progress.
    Connecting,
    /// The ordinary upstream wire is attached.
    Connected,
    /// A recoverable outcome is waiting for its next attempt.
    Reconnecting { reason: String },
    /// A fatal outcome stopped the connector generation.
    Failed { reason: String },
    /// Shutdown cancelled and joined the connector.
    Stopped,
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
    pub(crate) core_server_shell: StdRwLock<Option<ServerRuntimeHandle>>,
    pub(crate) core_server_shell_storage_config: Option<StorageConfig>,
    pub(crate) storage_factory: Option<Arc<dyn jazz::groove::storage::StorageFactory>>,
    /// Serializes durable-catalogue reconciliation into the local runtime shell.
    ///
    /// Catalogue storage can advance while a previous bridge is in flight, but
    /// bridge installs must reach the shell in the same order so an older head
    /// cannot overwrite a newer one.
    pub(crate) runtime_catalogue_publication: tokio::sync::Mutex<()>,
    #[cfg(test)]
    runtime_catalogue_before_publication_hook: StdMutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    runtime_catalogue_after_permissions_read_hook: StdMutex<Option<Box<dyn FnOnce() + Send>>>,
    /// Whether the current Edge shell generation has a fully installed,
    /// validated catalogue and local projection registry. A durable Ready
    /// generation remains usable offline; blank and refreshing generations do
    /// not admit new downstream sessions.
    dynamic_edge_catalogue_ready: AtomicBool,
    edge_upstream_health: StdRwLock<EdgeUpstreamHealth>,
    edge_upstream_task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    pub shutdown: ShutdownController,
}

impl Drop for ServerState {
    fn drop(&mut self) {
        let task = self
            .edge_upstream_task
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only synchronization point immediately before the production
    /// snapshot helper acquires the shell lock.
    static CLIENT_SHELL_SNAPSHOT_BEFORE_LOCK_HOOK: std::cell::RefCell<Option<Box<dyn FnMut()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn run_client_shell_snapshot_before_lock_hook() {
    CLIENT_SHELL_SNAPSHOT_BEFORE_LOCK_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook();
        }
    });
}

#[cfg(test)]
fn with_client_shell_snapshot_before_lock_hook<T>(
    hook: impl FnMut() + 'static,
    callback: impl FnOnce() -> T,
) -> T {
    CLIENT_SHELL_SNAPSHOT_BEFORE_LOCK_HOOK.with(|slot| {
        assert!(
            slot.borrow().is_none(),
            "snapshot hook is already installed"
        );
        *slot.borrow_mut() = Some(Box::new(hook));
    });
    let result = callback();
    CLIENT_SHELL_SNAPSHOT_BEFORE_LOCK_HOOK.with(|slot| {
        slot.borrow_mut().take();
    });
    result
}

/// Snapshot a shell and its dynamic-admission generation under one lock.
///
/// The readiness flag is atomic because the connector publishes it from an
/// async task, but it is read while the shell lock is held. Dynamic bootstrap
/// updates both values under that same write lock, so a client cannot pair the
/// old generation's `true` with the newly published shell.
fn client_shell_snapshot<T: Clone>(
    topology: ServerTopology,
    shell: &StdRwLock<Option<T>>,
    dynamic_edge_catalogue_ready: &AtomicBool,
) -> Option<T> {
    #[cfg(test)]
    run_client_shell_snapshot_before_lock_hook();
    let shell = shell.read().unwrap();
    if topology == ServerTopology::Edge && !dynamic_edge_catalogue_ready.load(Ordering::Acquire) {
        return None;
    }
    shell.clone()
}

impl ServerState {
    #[cfg(test)]
    pub(crate) fn set_runtime_catalogue_before_publication_hook_for_test(
        &self,
        hook: Box<dyn FnOnce() + Send>,
    ) {
        *self
            .runtime_catalogue_before_publication_hook
            .lock()
            .expect("runtime catalogue test hook lock") = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn run_runtime_catalogue_before_publication_hook_for_test(&self) {
        let hook = self
            .runtime_catalogue_before_publication_hook
            .lock()
            .expect("runtime catalogue test hook lock")
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    pub(crate) fn set_runtime_catalogue_after_permissions_read_hook_for_test(
        &self,
        hook: Box<dyn FnOnce() + Send>,
    ) {
        *self
            .runtime_catalogue_after_permissions_read_hook
            .lock()
            .expect("runtime catalogue test hook lock") = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn run_runtime_catalogue_after_permissions_read_hook_for_test(&self) {
        let hook = self
            .runtime_catalogue_after_permissions_read_hook
            .lock()
            .expect("runtime catalogue test hook lock")
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Test-only observation of whether an edge has installed a runtime shell.
    #[cfg(feature = "test")]
    #[doc(hidden)]
    pub fn has_core_server_shell_for_test(&self) -> bool {
        self.runtime().is_some()
    }

    /// Test-only observation of whether an edge is ready for downstream clients.
    #[cfg(feature = "test")]
    #[doc(hidden)]
    pub fn has_core_server_shell_for_client_for_test(&self) -> bool {
        self.runtime_for_client().is_some()
    }

    /// Test-only adoption hook for exercising the interval between catalogue
    /// installation and normal upstream-peer admission with a real server.
    #[cfg(feature = "test")]
    #[doc(hidden)]
    pub fn start_dynamic_edge_shell_for_test(
        &self,
        snapshot: jazz::protocol::CatalogueSnapshot,
        edge_cache_budget: Option<jazz::node::EdgeCacheBudget>,
    ) -> Result<(), String> {
        self.start_dynamic_edge_shell(snapshot, edge_cache_budget)
            .map(|_| ())
    }

    /// Test-only readback of the installed authority catalogue.
    #[cfg(feature = "test")]
    #[doc(hidden)]
    pub async fn trusted_catalogue_snapshot_for_test(
        &self,
    ) -> Result<jazz::protocol::CatalogueSnapshot, String> {
        self.runtime()
            .ok_or_else(|| "server has no runtime shell".to_owned())?
            .trusted_catalogue_snapshot_for_test()
            .await
    }

    /// Return the installed semantic runtime, including bootstrap-only access.
    pub fn runtime(&self) -> Option<ServerRuntimeHandle> {
        self.core_server_shell.read().unwrap().clone()
    }

    /// Return the runtime eligible for an ordinary downstream client session.
    /// Bootstrap code may inspect [`Self::runtime`], but a blank or refreshing dynamic Edge
    /// remains RetryLater until a complete catalogue generation is installed.
    pub fn runtime_for_client(&self) -> Option<ServerRuntimeHandle> {
        client_shell_snapshot(
            self.topology,
            &self.core_server_shell,
            &self.dynamic_edge_catalogue_ready,
        )
    }

    pub(crate) fn mark_dynamic_edge_catalogue_ready(&self) -> Result<(), String> {
        // Serialize this Ready transition with dynamic shell publication and
        // client snapshots. Callers reach this only after catalogue adoption
        // and any required projection-registry rebuild have returned.
        let _shell = self.core_server_shell.read().unwrap();
        if self.shutdown.is_shutting_down() {
            return Err("server shutdown started before edge readiness publication".to_owned());
        }
        self.dynamic_edge_catalogue_ready
            .store(true, Ordering::Release);
        Ok(())
    }

    fn mark_dynamic_edge_catalogue_refreshing(&self) {
        // Serialize the transition with downstream shell snapshots. A failed
        // registry install leaves this generation gated until a later complete
        // authenticated refresh succeeds.
        let _shell = self.core_server_shell.write().unwrap();
        self.dynamic_edge_catalogue_ready
            .store(false, Ordering::Release);
    }

    pub(crate) async fn refresh_dynamic_edge_catalogue(
        &self,
        shell: &ServerRuntimeHandle,
        snapshot: jazz::protocol::CatalogueSnapshot,
    ) -> Result<(), String> {
        if self.shutdown.is_shutting_down() {
            return Err("server shutdown started before edge catalogue refresh".to_owned());
        }
        self.mark_dynamic_edge_catalogue_refreshing();
        shell.apply_trusted_catalogue_snapshot(snapshot).await?;
        // Applying the snapshot returns only after a semantic transition has
        // rebuilt its local projections. The newly validated durable
        // generation may therefore serve offline even if normal upstream
        // attachment is still retrying.
        self.mark_dynamic_edge_catalogue_ready()
    }

    pub fn edge_upstream_health(&self) -> EdgeUpstreamHealth {
        self.edge_upstream_health.read().unwrap().clone()
    }

    pub(crate) fn set_edge_upstream_health(&self, health: EdgeUpstreamHealth) {
        *self.edge_upstream_health.write().unwrap() = health;
    }

    pub(crate) fn own_edge_upstream_task(&self, task: tokio::task::JoinHandle<()>) {
        let replaced = self.edge_upstream_task.lock().unwrap().replace(task);
        debug_assert!(replaced.is_none(), "edge upstream task is installed once");
    }

    async fn stop_edge_upstream_task(&self) {
        let task = self.edge_upstream_task.lock().unwrap().take();
        if let Some(task) = task
            && let Err(error) = task.await
            && !error.is_cancelled()
        {
            tracing::error!(%error, "edge upstream lifecycle task failed");
        }
        if self.topology == ServerTopology::Edge
            && !matches!(
                self.edge_upstream_health(),
                EdgeUpstreamHealth::Failed { .. }
            )
        {
            self.set_edge_upstream_health(EdgeUpstreamHealth::Stopped);
        }
    }

    pub(crate) fn start_core_server_shell(
        &self,
        schema: jazz::schema::JazzSchema,
    ) -> Result<ServerRuntimeHandle, String> {
        if let Some(core_server_shell) = self.runtime() {
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
        let started = ServerRuntimeHandle::start_with_storage(
            schema,
            storage_config,
            self.storage_factory.clone(),
        )?;
        *core_server_shell = Some(started.clone());
        Ok(started)
    }

    /// Atomically publish a normal edge runtime after its independent
    /// authenticated catalogue bootstrap completed. Holding the state lock
    /// across construction makes downstream admission observe either no shell
    /// (retry later) or the fully adopted ready shell, never a half-ready one.
    pub(crate) fn start_dynamic_edge_shell(
        &self,
        snapshot: jazz::protocol::CatalogueSnapshot,
        edge_cache_budget: Option<jazz::node::EdgeCacheBudget>,
    ) -> Result<ServerRuntimeHandle, String> {
        if self.topology != ServerTopology::Edge {
            return Err("dynamic catalogue bootstrap is only valid for edge topology".to_owned());
        }
        if self.shutdown.is_shutting_down() {
            return Err("server shutdown started before edge shell publication".to_owned());
        }
        let storage_config = self
            .core_server_shell_storage_config
            .clone()
            .ok_or_else(|| "server shell storage is not configured".to_owned())?;
        let mut core_server_shell = self.core_server_shell.write().unwrap();
        if let Some(existing) = core_server_shell.clone() {
            return Ok(existing);
        }
        if self.shutdown.is_shutting_down() {
            return Err("server shutdown started before edge shell publication".to_owned());
        }
        let started = ServerRuntimeHandle::start_dynamic_edge_with_catalogue_snapshot(
            storage_config,
            self.storage_factory.clone(),
            edge_cache_budget,
            snapshot,
        )?;
        self.dynamic_edge_catalogue_ready
            .store(false, Ordering::Release);
        if self.shutdown.is_shutting_down() {
            drop(core_server_shell);
            drop(started);
            return Err("server shutdown started before edge shell publication".to_owned());
        }
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
        self.stop_edge_upstream_task().await;
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::middleware::AuthConfig;
    use crate::server::builder::{ServerBuilder, StorageBackend};
    use crate::server::catalogue_storage::CatalogueStorageResult;
    use jazz::tools::AppId;
    use jazz::tools::public_schema::{ColumnType, Schema, SchemaBuilder, TableSchema};

    struct CloseObservingStorage {
        close_calls: Arc<AtomicUsize>,
    }

    struct PanicFlushStorage;

    impl CatalogueStorage for CloseObservingStorage {
        fn scan_catalogue_entries(
            &self,
        ) -> CatalogueStorageResult<Vec<crate::server::catalogue_entry::CatalogueEntry>> {
            Ok(Vec::new())
        }

        fn upsert_catalogue_entry(
            &mut self,
            _entry: &crate::server::catalogue_entry::CatalogueEntry,
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
        ) -> CatalogueStorageResult<Vec<crate::server::catalogue_entry::CatalogueEntry>> {
            Ok(Vec::new())
        }

        fn upsert_catalogue_entry(
            &mut self,
            _entry: &crate::server::catalogue_entry::CatalogueEntry,
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
            )
            .expect("build shutdown test catalogue"),
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
            storage_factory: None,
            runtime_catalogue_publication: tokio::sync::Mutex::new(()),
            runtime_catalogue_before_publication_hook: StdMutex::new(None),
            runtime_catalogue_after_permissions_read_hook: StdMutex::new(None),
            dynamic_edge_catalogue_ready: AtomicBool::new(true),
            edge_upstream_health: StdRwLock::new(EdgeUpstreamHealth::NotConfigured),
            edge_upstream_task: StdMutex::new(None),
            shutdown: ShutdownController::new(timeout),
        })
    }

    /// Dynamic publication must not let a downstream reader pair the prior
    /// generation's readiness with the newly published shell. The test hook
    /// synchronizes the actual production snapshot helper at its pre-lock
    /// boundary; it is not a hand-written model of that helper.
    #[test]
    fn dynamic_client_shell_snapshot_cannot_mix_ready_generation_with_new_shell() {
        let shell = Arc::new(RwLock::new(Some("old")));
        let ready = Arc::new(AtomicBool::new(true));
        let mut write = shell.write().unwrap();
        let (at_lock_boundary_tx, at_lock_boundary_rx) = mpsc::channel();
        let (continue_tx, continue_rx) = mpsc::channel();
        let fixed_shell = Arc::clone(&shell);
        let fixed_ready = Arc::clone(&ready);
        let fixed_reader = thread::spawn(move || {
            with_client_shell_snapshot_before_lock_hook(
                move || {
                    at_lock_boundary_tx
                        .send(())
                        .expect("tell publisher reader reached production lock boundary");
                    continue_rx
                        .recv()
                        .expect("publisher releases production reader");
                },
                || client_shell_snapshot(ServerTopology::Edge, &fixed_shell, &fixed_ready),
            )
        });
        at_lock_boundary_rx
            .recv()
            .expect("reader reached production helper lock boundary");
        *write = Some("new");
        ready.store(false, Ordering::Release);
        drop(write);
        continue_tx
            .send(())
            .expect("release reader after publication");
        assert_eq!(
            fixed_reader.join().expect("fixed reader joins"),
            None,
            "the lock-first production helper observes the new generation as unready"
        );
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
