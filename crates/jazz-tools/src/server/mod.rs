use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio::sync::{RwLock, broadcast};
use tokio::time::Instant;

use crate::middleware::AuthConfig;
use crate::middleware::auth::JwksCache;
use crate::runtime_tokio::TokioRuntime;
use crate::schema_manager::AppId;
use crate::storage::Storage;
use crate::sync_manager::{ClientId, SyncPayload};

mod builder;
mod external_identity_store;
#[cfg(feature = "test-utils")]
mod testing;

pub use builder::{BuiltServer, ServerBuilder};
pub use external_identity_store::{ExternalIdentityRow, ExternalIdentityStore};
#[cfg(feature = "test-utils")]
pub use testing::{TestingJwksServer, TestingServer, TestingServerBuilder};

pub type DynStorage = Box<dyn Storage + Send>;

/// Server state shared across request handlers.
pub struct ServerState {
    pub runtime: TokioRuntime<DynStorage>,
    #[allow(dead_code)]
    pub app_id: AppId,
    pub connections: RwLock<HashMap<u64, ConnectionState>>,
    pub next_connection_id: std::sync::atomic::AtomicU64,
    /// Broadcast channel for sending sync payloads to SSE clients.
    pub sync_broadcast: broadcast::Sender<(ClientId, SyncPayload)>,
    /// Authentication configuration.
    pub auth_config: AuthConfig,
    /// JWKS cache with TTL and on-demand refresh for key rotation.
    pub jwks_cache: Option<JwksCache>,
    /// Persistent external identity mapping store.
    pub external_identity_store: Arc<ExternalIdentityStore>,
    /// In-memory cache: (issuer, subject) -> principal_id.
    pub external_identities: RwLock<HashMap<(String, String), String>>,
    /// Clients that lost their SSE stream, waiting to be reaped after TTL.
    /// Maps client_id → instant when the last SSE connection closed.
    pub disconnect_candidates: RwLock<HashMap<ClientId, Instant>>,
    /// Client state TTL in milliseconds. Default: 5 minutes (300_000ms).
    /// Disconnected clients are reaped after this duration.
    pub client_ttl: Arc<AtomicU64>,
}

/// State for a single SSE connection.
pub struct ConnectionState {
    pub client_id: ClientId,
}

impl ServerState {
    /// Record that a connection closed. If this was the last SSE connection
    /// for the given client_id, add it to disconnect_candidates.
    pub async fn on_connection_closed(&self, client_id: ClientId) {
        let has_connections = self
            .connections
            .read()
            .await
            .values()
            .any(|c| c.client_id == client_id);

        if !has_connections {
            self.disconnect_candidates
                .write()
                .await
                .insert(client_id, Instant::now());
        }
    }

    /// Record that a client reconnected. Remove from disconnect_candidates
    /// if present.
    pub async fn on_client_connected(&self, client_id: ClientId) {
        self.disconnect_candidates.write().await.remove(&client_id);
    }

    /// Check if a client_id has any active SSE connections.
    pub async fn has_active_connections(&self, client_id: ClientId) -> bool {
        self.connections
            .read()
            .await
            .values()
            .any(|c| c.client_id == client_id)
    }

    /// Run one sweep iteration: drain expired disconnect candidates,
    /// check each for active connections, and reap those that are truly gone.
    /// Returns the list of reaped client IDs.
    pub async fn run_sweep_once(&self) -> Vec<ClientId> {
        let ttl_ms = self.client_ttl.load(std::sync::atomic::Ordering::Relaxed);
        let ttl = std::time::Duration::from_millis(ttl_ms);
        let now = tokio::time::Instant::now();

        // Step 1-3: drain expired entries from candidates
        let expired: Vec<ClientId> = {
            let mut candidates = self.disconnect_candidates.write().await;
            let mut expired = Vec::new();
            candidates.retain(|&client_id, &mut disconnected_at| {
                if now.duration_since(disconnected_at) >= ttl {
                    expired.push(client_id);
                    false // remove from candidates
                } else {
                    true // keep
                }
            });
            expired
        };

        // Step 4: for each expired client, check connections then reap
        let mut reaped = Vec::new();
        for client_id in expired {
            if self.has_active_connections(client_id).await {
                // Client reconnected between drain and reap — skip
                tracing::debug!(%client_id, "skipping reap: client reconnected");
                continue;
            }
            if let Err(e) = self.runtime.remove_client(client_id) {
                tracing::warn!(%client_id, error = %e, "failed to reap client");
                continue;
            }
            reaped.push(client_id);
            tracing::debug!(%client_id, "reaped disconnected client");
        }

        reaped
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::schema_manager::AppId;
    use crate::server::builder::ServerBuilder;

    async fn build_test_state() -> Arc<ServerState> {
        let app_id = AppId::from_name("lifecycle-test");
        let built = ServerBuilder::new(app_id)
            .with_in_memory_storage()
            .build()
            .await
            .expect("build test server");
        built.state
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
    async fn sweep_reaps_expired_candidates() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        // Register alice in the runtime so there's state to reap
        let _ = state.runtime.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Advance time past TTL (default 5 min)
        tokio::time::advance(Duration::from_secs(301)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice]);

        // Verify client state was actually removed
        let has_client = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(alice).is_some())
            .expect("lock");
        assert!(!has_client, "alice's ClientState should be gone");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_preserves_candidates_before_ttl() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.runtime.add_client(alice, None);

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

        let _ = state.runtime.add_client(alice, None);

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
            .runtime
            .with_sync_manager(|sm| sm.get_client(alice).is_some())
            .expect("lock");
        assert!(has_client, "alice's ClientState should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_race_guard_checks_connections() {
        // Simulate the race: sweep drains candidates, then alice reconnects,
        // then sweep checks connections before reaping.
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.runtime.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        tokio::time::advance(Duration::from_secs(301)).await;

        // Manually drain candidates (simulating sweep step 1-3)
        let expired: Vec<ClientId> = {
            let mut candidates = state.disconnect_candidates.write().await;
            let ttl_ms = state.client_ttl.load(std::sync::atomic::Ordering::Relaxed);
            let ttl = Duration::from_millis(ttl_ms);
            let now = tokio::time::Instant::now();
            let mut expired = Vec::new();
            candidates.retain(|&client_id, &mut disconnected_at| {
                if now.duration_since(disconnected_at) >= ttl {
                    expired.push(client_id);
                    false
                } else {
                    true
                }
            });
            expired
        };
        assert_eq!(expired, vec![alice]);

        // Alice reconnects between drain and reap
        let _conn2 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        // Now the sweep would check connections before reaping
        assert!(
            state.has_active_connections(alice).await,
            "alice has a connection — sweep should skip"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_reaps_multiple_expired_candidates() {
        let state = build_test_state().await;
        let alice = ClientId::new();
        let bob = ClientId::new();

        let _ = state.runtime.add_client(alice, None);
        let _ = state.runtime.add_client(bob, None);

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

        let _ = state.runtime.add_client(alice, None);
        let _ = state.runtime.add_client(bob, None);

        // Only alice disconnects
        let conn_a = add_connection(&state, alice).await;
        let _conn_b = add_connection(&state, bob).await;
        remove_connection(&state, conn_a).await;

        tokio::time::advance(Duration::from_secs(301)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice]);

        let has_bob = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(bob).is_some())
            .expect("lock");
        assert!(has_bob, "bob should be unaffected");
    }
}
