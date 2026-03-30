use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast};

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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CatalogueAuthorityMode {
    #[default]
    Local,
    Forward {
        base_url: String,
        admin_secret: String,
    },
}

impl CatalogueAuthorityMode {
    pub fn forward_target(&self) -> Option<(&str, &str)> {
        match self {
            Self::Local => None,
            Self::Forward {
                base_url,
                admin_secret,
            } => Some((base_url.as_str(), admin_secret.as_str())),
        }
    }
}

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
    /// Whether catalogue admin requests are handled locally or forwarded to an authority.
    pub catalogue_authority: CatalogueAuthorityMode,
    /// Shared HTTP client for forwarding admin requests to a remote authority.
    pub http_client: reqwest::Client,
    /// JWKS cache with TTL and on-demand refresh for key rotation.
    pub jwks_cache: Option<JwksCache>,
    /// Persistent external identity mapping store.
    pub external_identity_store: Arc<ExternalIdentityStore>,
    /// In-memory cache: (issuer, subject) -> principal_id.
    pub external_identities: RwLock<HashMap<(String, String), String>>,
    /// Optional sync message tracer for test observability.
    pub sync_tracer: Option<crate::sync_tracer::SyncTracer>,
}

/// State for a single SSE connection.
pub struct ConnectionState {
    pub _client_id: ClientId,
}
