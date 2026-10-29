#[cfg(test)]
use std::sync::Mutex as StdMutex;
use std::sync::{Arc, RwLock as StdRwLock};
use std::thread;

use crate::middleware::AuthConfig;
use crate::middleware::auth::JwtVerifier;
use jazz::tools::AppId;

mod account_store;
mod accounts;
mod builder;
mod catalogue;
mod catalogue_storage;
#[cfg(test)]
mod data_dir_corpus;
pub use crate::runtime::{ServerRuntimeActivity, ServerRuntimeFrameStream, ServerRuntimeHandle};
pub(crate) mod deployment;
mod permissions_validation;
pub mod routes;
pub(crate) mod runtime_catalogue;
mod shutdown;
#[cfg(feature = "embedded-server")]
mod testing;

pub use builder::{BuiltServer, ServerBuilder, StorageBackend};
pub(crate) use catalogue::{ActiveSchemaSummary, ServerCatalogue, StoredCatalogue};
pub(crate) use catalogue_storage::CatalogueStorage;
pub use shutdown::{ShutdownController, ShutdownPhase, WebSocketEntryRefused};
#[cfg(feature = "embedded-server")]
pub use testing::{
    JazzServer, JazzServerBuilder, ServerDataDir, TEST_JWT_AUDIENCE, TEST_JWT_ISSUER,
    TestJwtIssuer, TestJwtOptions,
};

/// Publish catalogue inputs directly into an in-process test server.
#[cfg(feature = "embedded-server")]
pub async fn push_catalogue_in_memory(
    state: Arc<ServerState>,
    _app_id: AppId,
    _env: &str,
    schemas: &[jazz::tools::Schema],
    lenses: &[jazz::tools::Lens],
) -> Result<(), Box<dyn std::error::Error>> {
    let _publication = state.runtime_catalogue_publication.lock().await;
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
    runtime_catalogue::publish_runtime_catalogue_locked(&state, schemas, lenses)
        .await
        .map_err(|error| format!("bridge catalogue into server runtime: {error}"))?;
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
/// Default cap on live WebSocket connections per server process.
///
/// Every connection may hold up to its per-session ingress and egress
/// budgets, so an uncapped listener lets connection count alone exhaust the
/// process. Upgrades beyond the cap get `503 Service Unavailable` with
/// `Retry-After`, and clients reconnect with backoff.
pub const DEFAULT_MAX_WEBSOCKET_CONNECTIONS: usize = 1024;
pub(crate) const MAX_CATALOGUE_REQUEST_BODY_BYTES: usize = 8 << 20;
/// Server state shared across request handlers.
pub struct ServerState {
    pub(crate) accounts: Option<accounts::AccountRegistryOwner>,
    /// Direct, storage-backed admin catalogue store.
    pub(crate) catalogue_store: StoredCatalogue,
    pub(crate) catalogue: ServerCatalogue,
    #[allow(dead_code)]
    pub app_id: AppId,
    /// Authentication configuration.
    pub auth_config: AuthConfig,
    pub jwt_verifier: Option<Arc<JwtVerifier>>,
    /// Sendable handle to the local-owner server shell for the websocket route.
    pub(crate) core_server_shell: StdRwLock<Option<ServerRuntimeHandle>>,
    /// Per-state admission owner for policy-scoped WebSocket connections.
    ///
    /// The owner is shared by every router and connection task that clones
    /// this `ServerState`, while independently built states remain isolated.
    pub(crate) websocket_admissions: Arc<routes::WebSocketAdmissionState>,
    /// Live WebSocket connections this process admits before refusing
    /// upgrades with `503`.
    pub(crate) max_websocket_connections: usize,
    /// The app's store: the one file (and thread) the catalogue, the account
    /// registry and, once started, the runtime share.
    pub(crate) store: Option<crate::runtime::AppStore>,
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
    /// Tests: the shutdown finalizer panics.
    #[cfg(test)]
    panic_during_shutdown_for_test: std::sync::atomic::AtomicBool,
    pub shutdown: ShutdownController,
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

    /// Test-only observation of whether Core has installed a runtime shell.
    #[cfg(feature = "test")]
    #[doc(hidden)]
    pub fn has_core_server_shell_for_test(&self) -> bool {
        self.runtime().is_some()
    }

    /// Test-only observation of whether Core is ready for downstream clients.
    #[cfg(feature = "test")]
    #[doc(hidden)]
    pub fn has_core_server_shell_for_client_for_test(&self) -> bool {
        self.runtime_for_client().is_some()
    }

    /// Test-only readback of the store's catalogue.
    #[cfg(feature = "test")]
    #[doc(hidden)]
    pub async fn catalogue_for_test(&self) -> Result<jazz::catalogue::CatalogueRows, String> {
        self.runtime()
            .ok_or_else(|| "server has no runtime shell".to_owned())?
            .catalogue()
            .await
    }

    /// Return the installed semantic runtime, including bootstrap-only access.
    pub fn runtime(&self) -> Option<ServerRuntimeHandle> {
        self.core_server_shell.read().unwrap().clone()
    }

    /// Return the Core runtime eligible for a client session.
    pub fn runtime_for_client(&self) -> Option<ServerRuntimeHandle> {
        self.runtime()
    }

    pub(crate) fn start_core_server_shell(
        &self,
        schema: jazz::schema::JazzSchema,
    ) -> Result<ServerRuntimeHandle, String> {
        if let Some(core_server_shell) = self.runtime() {
            return Ok(core_server_shell);
        }

        let store = self
            .store
            .as_ref()
            .ok_or_else(|| "server shell storage is not configured".to_owned())?;
        let mut core_server_shell = self.core_server_shell.write().unwrap();
        if let Some(existing) = core_server_shell.clone() {
            return Ok(existing);
        }
        let started = store.start_runtime(schema, crate::runtime::NodeRole::Core, false, true)?;
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
        #[cfg(test)]
        if self
            .panic_during_shutdown_for_test
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            panic!("test finalizer panic");
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
        if let Some(accounts) = &self.accounts
            && let Err(error) = accounts.close()
        {
            tracing::error!(%error, "shutdown account registry close failed");
            failed = true;
        }
        // The store outlives the shell's sessions but not the server: join
        // its thread (already joined if the shell started) so the file is
        // closed before the data dir can be reopened.
        if let Some(store) = &self.store {
            let store = store.clone();
            let closed = tokio::task::spawn_blocking(move || store.close())
                .await
                .map_err(|error| error.to_string())
                .and_then(|closed| closed);
            if let Err(error) = closed {
                tracing::error!(%error, "shutdown store close failed");
                failed = true;
            }
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
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;
    use crate::server::builder::{ServerBuilder, StorageBackend};
    use jazz::tools::AppId;

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

    /// Whether the server's store still takes writes.
    fn store_open(state: &ServerState) -> bool {
        state
            .store
            .as_ref()
            .expect("store")
            .kv(jazz::host::HostTable::new("probe"))
            .and_then(|kv| kv.write_many(&[]))
            .is_ok()
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
        let state = build_test_state_with_shutdown_timeout(Duration::from_millis(10)).await;
        let _request_guard = state
            .shutdown
            .try_enter_app_request()
            .expect("running server accepts request");

        state.shutdown.request_shutdown();
        let phase = state.run_shutdown_finalization().await;

        assert_eq!(phase, ShutdownPhase::Failed);
        assert!(
            store_open(&state),
            "storage must not be closed while app request guards are still active"
        );
    }

    #[tokio::test]
    async fn shutdown_finalization_closes_storage_after_drain() {
        let state = build_test_state_with_shutdown_timeout(Duration::from_millis(10)).await;

        state.shutdown.request_shutdown();
        let phase = state.run_shutdown_finalization().await;

        assert_eq!(phase, ShutdownPhase::StorageClosed);
        assert_eq!(state.shutdown.phase(), ShutdownPhase::StorageClosed);
        assert!(
            !store_open(&state),
            "drained shutdown must explicitly close the store"
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
        let state = build_test_state_with_shutdown_timeout(Duration::from_secs(1)).await;
        state
            .panic_during_shutdown_for_test
            .store(true, Ordering::SeqCst);
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
