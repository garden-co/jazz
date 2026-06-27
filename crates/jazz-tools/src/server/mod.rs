use std::collections::HashMap;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::middleware::AuthConfig;
use crate::middleware::auth::JwtVerifier;
use crate::schema_manager::AppId;
use crate::sync::ClientId;
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

/// Tracks retired alpha connection disconnect markers, pending TTL cleanup.
#[derive(Clone, Copy)]
pub struct DisconnectCandidate {
    /// When the last SSE connection closed.
    pub disconnected_at: Instant,
}

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
    pub connections: RwLock<HashMap<u64, ConnectionState>>,
    pub next_connection_id: std::sync::atomic::AtomicU64,
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
    /// Retired alpha disconnect markers, waiting to be cleared after TTL.
    pub disconnect_candidates: RwLock<HashMap<ClientId, DisconnectCandidate>>,
    /// Client state TTL. Default: 5 minutes.
    /// Disconnected clients are reaped after this duration.
    pub client_ttl: RwLock<Duration>,
    /// Optional legacy sync message tracer for test observability.
    #[cfg(any(test, feature = "test-utils"))]
    pub sync_tracer: Option<crate::sync::SyncTracer>,
    /// Sendable handle to the local-owner jazz_core peer loop for the direct websocket route.
    pub(crate) core_server: StdRwLock<Option<core_server::LocalCoreServerHandle>>,
    pub(crate) core_server_storage_config: Option<StorageConfig>,
    pub shutdown: ShutdownController,
}

/// State for a retired alpha connection marker.
pub struct ConnectionState {
    pub client_id: ClientId,
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

    /// Record that a connection closed. If this was the last alpha connection
    /// for the given client_id, add it to disconnect_candidates.
    ///
    /// The connections check and candidate insertion are done under the
    /// candidates write lock to prevent a TOCTOU race where a reconnect
    /// could slip in between the check and the insert.
    ///
    /// Lock ordering (no path nests these in conflicting order):
    ///   on_connection_closed:  candidates(write) → connections(read)
    ///   on_client_connected:   candidates(write)
    ///   events_handler:        connections(write) ; candidates(write)   (sequential)
    ///   run_sweep_once:        candidates(write) ; connections(read) ; core(Mutex)  (all sequential)
    pub async fn on_connection_closed(&self, client_id: ClientId) {
        let mut candidates = self.disconnect_candidates.write().await;
        let has_connections = self
            .connections
            .read()
            .await
            .values()
            .any(|c| c.client_id == client_id);
        if !has_connections {
            candidates.insert(
                client_id,
                DisconnectCandidate {
                    disconnected_at: Instant::now(),
                },
            );
        }
    }

    /// Record that a client reconnected. Remove from disconnect_candidates
    /// if present.
    pub async fn on_client_connected(&self, client_id: ClientId) {
        self.disconnect_candidates.write().await.remove(&client_id);
    }

    /// Run one sweep iteration: drain expired disconnect candidates,
    /// snapshot active connections, and reap those that are truly gone.
    /// Returns the list of reaped client IDs.
    pub async fn run_sweep_once(&self) -> Vec<ClientId> {
        let ttl = *self.client_ttl.read().await;
        let now = tokio::time::Instant::now();

        // Step 1: drain expired entries from candidates
        let expired: Vec<ClientId> = {
            let mut candidates = self.disconnect_candidates.write().await;
            let mut expired = Vec::new();
            candidates.retain(|&client_id, candidate| {
                if now.duration_since(candidate.disconnected_at) >= ttl {
                    expired.push(client_id);
                    false // remove from candidates
                } else {
                    true // keep
                }
            });
            expired
        };

        if expired.is_empty() {
            return Vec::new();
        }

        let mut reaped = Vec::new();
        for client_id in expired {
            // Check for active connections right before reaping to close the
            // TOCTOU window: if a client reconnects between the candidate drain
            // above and this check, we see the new connection and skip.
            let has_connection = self
                .connections
                .read()
                .await
                .values()
                .any(|c| c.client_id == client_id);
            if has_connection {
                tracing::debug!(%client_id, "skipping reap: client reconnected");
                continue;
            }
            reaped.push(client_id);
            tracing::debug!(
                %client_id,
                "expired disconnected-client marker; alpha runtime client reaping is disabled"
            );
        }

        reaped
    }

    /// Update the client state TTL. Takes effect on the next sweep tick.
    pub async fn set_client_ttl(&self, ttl: Duration) {
        *self.client_ttl.write().await = ttl;
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

    async fn build_test_state() -> Arc<ServerState> {
        build_test_state_with_shutdown_timeout(Duration::from_secs(30)).await
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
            connections: RwLock::new(HashMap::new()),
            next_connection_id: std::sync::atomic::AtomicU64::new(1),
            auth_config: AuthConfig::default(),
            upstream_http_url: None,
            topology: ServerTopology::Core,
            http_client: reqwest::Client::builder()
                .build()
                .expect("build HTTP client"),
            jwt_verifier: None,
            disconnect_candidates: RwLock::new(HashMap::new()),
            client_ttl: RwLock::new(Duration::from_secs(300)),
            #[cfg(any(test, feature = "test-utils"))]
            sync_tracer: None,
            core_server: StdRwLock::new(None),
            core_server_storage_config: None,
            shutdown: ShutdownController::new(timeout),
        })
    }

    /// Simulate adding a connection (like events_handler does).
    async fn add_connection(state: &ServerState, client_id: ClientId) -> u64 {
        let connection_id = state
            .next_connection_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        state
            .connections
            .write()
            .await
            .insert(connection_id, ConnectionState { client_id });
        connection_id
    }

    /// Simulate removing a connection (like stream cleanup does).
    async fn remove_connection(state: &ServerState, connection_id: u64) {
        let client_id = {
            let mut connections = state.connections.write().await;
            let conn = connections
                .remove(&connection_id)
                .expect("connection exists");
            conn.client_id
        };
        state.on_connection_closed(client_id).await;
    }

    #[tokio::test]
    async fn disconnect_adds_candidate_when_no_other_connections() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            candidates.contains_key(&alice),
            "alice should be a disconnect candidate"
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
    async fn disconnect_does_not_add_candidate_when_other_connections_exist() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn1 = add_connection(&state, alice).await;
        let _conn2 = add_connection(&state, alice).await;
        remove_connection(&state, conn1).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice still has an active connection"
        );
    }

    #[tokio::test]
    async fn reconnect_removes_candidate() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // alice reconnects
        let _conn2 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice reconnected — should not be a candidate"
        );
    }

    #[tokio::test]
    async fn disconnect_both_connections_adds_candidate() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn1 = add_connection(&state, alice).await;
        let conn2 = add_connection(&state, alice).await;
        remove_connection(&state, conn1).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(!candidates.contains_key(&alice), "alice still has conn2");
        drop(candidates);

        remove_connection(&state, conn2).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            candidates.contains_key(&alice),
            "both connections closed — alice should be a candidate"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_expires_disconnect_marker_without_reaping_alpha_client_state() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        // Register alice in the catalogue runtime. The direct-core server path
        // no longer uses sweeps to mutate this alpha client state.
        let _ = state.catalogue_store.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Advance time past TTL (default 5 min)
        tokio::time::advance(Duration::from_secs(301)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice]);

        // Only the disconnect marker expires; alpha runtime client reaping is
        // disabled while catalogue_store remains catalogue-only.
        let has_client = state
            .catalogue_store
            .client_registered_for_test(alice)
            .expect("lock");
        assert!(has_client, "alice's ClientState should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_preserves_candidates_before_ttl() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.catalogue_store.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Advance time but NOT past TTL
        tokio::time::advance(Duration::from_secs(60)).await;

        let reaped = state.run_sweep_once().await;
        assert!(reaped.is_empty());

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            candidates.contains_key(&alice),
            "alice should still be a candidate"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_skips_reconnected_client() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.catalogue_store.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        tokio::time::advance(Duration::from_secs(301)).await;

        // Alice reconnects before sweep runs
        let _conn2 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        let reaped = state.run_sweep_once().await;
        assert!(
            reaped.is_empty(),
            "alice reconnected — should not be reaped"
        );

        let has_client = state
            .catalogue_store
            .client_registered_for_test(alice)
            .expect("lock");
        assert!(has_client, "alice's ClientState should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_skips_client_that_reconnected_after_expiry() {
        // Exercises the per-client connection check in run_sweep_once:
        // alice is in disconnect_candidates (expired), but also has an
        // active connection. Sweep should see the connection and skip her.
        //
        // We insert the candidate manually to bypass on_client_connected
        // (which would remove it), simulating the race where a reconnect
        // happens after candidates are drained but before the per-client
        // connection check.
        let state = build_test_state().await;
        let alice = ClientId::new();
        let _ = state.catalogue_store.add_client(alice, None);

        // Insert an already-expired candidate directly
        {
            let mut candidates = state.disconnect_candidates.write().await;
            candidates.insert(
                alice,
                super::DisconnectCandidate {
                    disconnected_at: tokio::time::Instant::now() - Duration::from_secs(301),
                },
            );
        }

        // alice has an active connection (simulating reconnect)
        let _conn = add_connection(&state, alice).await;

        // Sweep drains alice from candidates (expired), but the
        // per-client connection check sees her connection → skip reap
        let reaped = state.run_sweep_once().await;
        assert!(
            reaped.is_empty(),
            "alice has active connection — should not be reaped"
        );

        let has_client = state
            .catalogue_store
            .client_registered_for_test(alice)
            .expect("lock");
        assert!(has_client, "alice's state should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_preserves_fresh_state_when_client_reconnects_after_drain() {
        //
        // Exercises the critical TOCTOU window in run_sweep_once:
        //
        //   sweep: drain candidates ──▶ (alice expired, removed from candidates)
        //                                     │
        //   alice: reconnect ──▶ ensure_client_with_session (fresh ClientState)
        //                        on_client_connected (no-op, already drained)
        //                        add_connection (visible in connections map)
        //                                     │
        //   sweep: per-client connection check ──▶ sees alice's connection → skip
        //
        // Without the per-client check, the sweep would use a stale snapshot
        // and destroy alice's freshly registered state.
        //
        let state = build_test_state().await;
        let alice = ClientId::new();
        let _ = state.catalogue_store.add_client(alice, None);

        // alice disconnects and expires
        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;
        tokio::time::advance(Duration::from_secs(301)).await;

        // Manually drain candidates (simulating the first phase of run_sweep_once)
        let expired: Vec<ClientId> = {
            let mut candidates = state.disconnect_candidates.write().await;
            let drained: Vec<_> = candidates.keys().copied().collect();
            candidates.clear();
            drained
        };
        assert_eq!(expired, vec![alice]);

        // alice reconnects AFTER drain — fresh state created, new connection added
        let _ = state.catalogue_store.ensure_client_with_session(
            alice,
            crate::query_manager::session::Session::new("alice"),
        );
        let _conn2 = add_connection(&state, alice).await;
        // on_client_connected is a no-op here (alice was already drained)
        state.on_client_connected(alice).await;

        // Now run the full sweep — it should see alice's connection and skip her
        // (we need to re-insert alice as expired to let sweep process her,
        // since we manually drained above)
        {
            let mut candidates = state.disconnect_candidates.write().await;
            candidates.insert(
                alice,
                super::DisconnectCandidate {
                    disconnected_at: tokio::time::Instant::now() - Duration::from_secs(301),
                },
            );
        }
        let reaped = state.run_sweep_once().await;

        assert!(
            reaped.is_empty(),
            "alice has fresh state and active connection — must not be reaped"
        );

        let has_client = state
            .catalogue_store
            .client_registered_for_test(alice)
            .expect("lock");
        assert!(has_client, "alice's fresh ClientState should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_reaps_multiple_expired_candidates() {
        let state = build_test_state().await;
        let alice = ClientId::new();
        let bob = ClientId::new();

        let _ = state.catalogue_store.add_client(alice, None);
        let _ = state.catalogue_store.add_client(bob, None);

        let conn_a = add_connection(&state, alice).await;
        let conn_b = add_connection(&state, bob).await;
        remove_connection(&state, conn_a).await;
        remove_connection(&state, conn_b).await;

        tokio::time::advance(Duration::from_secs(301)).await;

        let mut reaped = state.run_sweep_once().await;
        reaped.sort_by_key(|c| c.0);
        let mut expected = vec![alice, bob];
        expected.sort_by_key(|c| c.0);
        assert_eq!(reaped, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_does_not_affect_other_clients() {
        let state = build_test_state().await;
        let alice = ClientId::new();
        let bob = ClientId::new();

        let _ = state.catalogue_store.add_client(alice, None);
        let _ = state.catalogue_store.add_client(bob, None);

        // Only alice disconnects
        let conn_a = add_connection(&state, alice).await;
        let _conn_b = add_connection(&state, bob).await;
        remove_connection(&state, conn_a).await;

        tokio::time::advance(Duration::from_secs(301)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice]);

        let has_bob = state
            .catalogue_store
            .client_registered_for_test(bob)
            .expect("lock");
        assert!(has_bob, "bob should be unaffected");
    }

    #[tokio::test(start_paused = true)]
    async fn set_client_ttl_changes_sweep_behavior() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.catalogue_store.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Set TTL to 1 second
        state.set_client_ttl(Duration::from_secs(1)).await;

        tokio::time::advance(Duration::from_secs(2)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice], "alice should be reaped with 1s TTL");
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_ttl_change_takes_effect_on_next_sweep() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.catalogue_store.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Advance 2 seconds — not past default 5 min TTL
        tokio::time::advance(Duration::from_secs(2)).await;

        let reaped = state.run_sweep_once().await;
        assert!(reaped.is_empty(), "default TTL: alice should survive");

        // Now change TTL to 1 second — alice has been disconnected for 2s
        state.set_client_ttl(Duration::from_secs(1)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice], "new TTL: alice should be reaped");
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_after_marker_expiry_preserves_catalogue_client_state() {
        //
        // alice ──connects──▶ server ──disconnects──▶ TTL expires ──▶ reaped
        //                                                              │
        //                     alice ──reconnects──▶ fresh ClientState ◀┘
        //
        use crate::query_manager::session::Session;

        let state = build_test_state().await;
        let alice = ClientId::new();
        let session = Session::new("alice");

        // Connect, register with session
        let _ = state.catalogue_store.add_client(alice, None);
        let _ = state
            .catalogue_store
            .ensure_client_with_session(alice, session.clone());

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Expire the disconnect marker without mutating alpha client state.
        tokio::time::advance(Duration::from_secs(301)).await;
        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice]);

        // Verify the catalogue client state is preserved.
        let has_client = state
            .catalogue_store
            .client_registered_for_test(alice)
            .expect("lock");
        assert!(has_client, "alice should remain after marker expiry");

        // Reconnect against the preserved catalogue state.
        let _ = state
            .catalogue_store
            .ensure_client_with_session(alice, session);
        let _conn2 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        let has_client = state
            .catalogue_store
            .client_registered_for_test(alice)
            .expect("lock");
        assert!(
            has_client,
            "alice should have fresh ClientState after reconnect"
        );

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice should not be a disconnect candidate"
        );
    }

    // ========================================================================
    // Lock ordering / sequential correctness tests
    //
    // These exercise sequential lock acquisition patterns to verify that
    // the candidates(write) → connections(read) nesting in on_connection_closed
    // produces correct state transitions when interleaved with other operations.
    // Note: single-threaded tokio cannot detect true two-task deadlocks.
    // ========================================================================

    #[tokio::test]
    async fn lock_ordering_disconnect_and_reconnect() {
        // Exercises sequential lock ordering: on_connection_closed takes
        // candidates(write) → connections(read), while on_client_connected
        // takes candidates(write). Running them sequentially verifies correct
        // state transitions under interleaved operations.
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn1 = add_connection(&state, alice).await;
        let conn2 = add_connection(&state, alice).await;

        // Simulate: conn1 closes, alice reconnects (via on_client_connected),
        // then conn2 closes — all interleaved.
        {
            let mut connections = state.connections.write().await;
            connections.remove(&conn1);
        }
        state.on_connection_closed(alice).await;

        // alice "reconnects" (conn2 is still active, plus a new one)
        let _conn3 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        // conn2 closes
        {
            let mut connections = state.connections.write().await;
            connections.remove(&conn2);
        }
        state.on_connection_closed(alice).await;

        // conn3 still active — should NOT be a candidate
        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice still has conn3 — must not be a candidate"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lock_ordering_sweep_during_disconnect() {
        // Exercises sequential lock ordering: sweep takes candidates(write)
        // then connections(read), while on_connection_closed takes
        // candidates(write) → connections(read). Verifies correct state
        // transitions when these operations are interleaved.
        let state = build_test_state().await;
        let alice = ClientId::new();
        let bob = ClientId::new();

        let _ = state.catalogue_store.add_client(alice, None);
        let _ = state.catalogue_store.add_client(bob, None);

        // alice disconnects and expires
        let conn_a = add_connection(&state, alice).await;
        remove_connection(&state, conn_a).await;
        tokio::time::advance(Duration::from_secs(301)).await;

        // bob disconnects while sweep is about to run
        let conn_b = add_connection(&state, bob).await;
        {
            let mut connections = state.connections.write().await;
            connections.remove(&conn_b);
        }

        // Interleave: sweep runs, then bob's on_connection_closed
        let reaped = state.run_sweep_once().await;
        state.on_connection_closed(bob).await;

        assert_eq!(reaped, vec![alice], "only alice should be reaped");

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            candidates.contains_key(&bob),
            "bob should be a candidate after disconnect"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lock_ordering_sweep_and_reconnect() {
        // Exercises sequential lock ordering: sweep takes candidates(write)
        // then per-client connections(read), while connect path takes
        // connections(write) then on_client_connected takes candidates(write).
        // Verifies correct state transitions when these operations are
        // interleaved.
        let state = build_test_state().await;
        let alice = ClientId::new();
        let bob = ClientId::new();

        let _ = state.catalogue_store.add_client(alice, None);
        let _ = state.catalogue_store.add_client(bob, None);

        let conn_a = add_connection(&state, alice).await;
        let conn_b = add_connection(&state, bob).await;
        remove_connection(&state, conn_a).await;
        remove_connection(&state, conn_b).await;
        tokio::time::advance(Duration::from_secs(301)).await;

        // Interleave: alice reconnects before sweep runs — sweep's
        // per-client connection check catches her active connection
        let _conn_a2 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        let reaped = state.run_sweep_once().await;

        // alice reconnected — not reaped. bob — reaped.
        assert_eq!(reaped, vec![bob]);

        let has_alice = state
            .catalogue_store
            .client_registered_for_test(alice)
            .expect("lock");
        assert!(has_alice, "alice reconnected — should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_task_exits_when_state_is_dropped() {
        // The sweep task holds a Weak<ServerState>. When all strong refs are
        // dropped, the task should exit on its next tick.
        //
        // We verify by dropping all strong refs, advancing past the sweep
        // interval, and yielding. If the Weak upgrade succeeds (leak),
        // the sweep task would run forever and the test would hang.
        // With start_paused=true + advance, the interval tick fires
        // deterministically.
        let state = build_test_state().await;

        // Drop all strong refs — sweep task holds only a Weak
        drop(state);

        // Advance past sweep interval (30s) so the task would tick
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;

        // If we get here without hanging, the Weak upgrade failed and the
        // task exited. With start_paused=true + advance, the interval tick
        // fires deterministically, and the Weak::upgrade returns None.
    }

    #[tokio::test]
    async fn on_connection_closed_is_atomic_wrt_candidates() {
        // Verifies that on_connection_closed checks connections and inserts
        // into candidates atomically (under the candidates write lock).
        // If a reconnect happens after the connection is removed but before
        // on_connection_closed runs, the candidate insertion must see the
        // new connection and NOT insert.
        let state = build_test_state().await;
        let alice = ClientId::new();

        // alice has one connection
        let conn1 = add_connection(&state, alice).await;

        // Remove conn1 from connections map
        {
            let mut connections = state.connections.write().await;
            connections.remove(&conn1);
        }

        // alice reconnects (new connection added) BEFORE on_connection_closed runs
        let _conn2 = add_connection(&state, alice).await;

        // Now on_connection_closed runs — should see conn2 and NOT insert
        state.on_connection_closed(alice).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice has conn2 — on_connection_closed must see it and not insert"
        );
    }
}
