use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Persistence tier — declaration order defines Ord (Local < EdgeServer < GlobalServer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum DurabilityTier {
    Local,
    EdgeServer,
    GlobalServer,
}

/// Product-level consistency choice for reads.
///
/// Read tiers deliberately do not expose the storage/protocol durability
/// lattice.  [`ReadTier::LocalFirst`] reads what is locally known,
/// [`ReadTier::Remote`] waits for the ordinary remote view, and
/// [`ReadTier::RemoteIfPossible`] may start locally only when a host has been
/// explicitly disconnected.  A transport timeout, connection error, or slow
/// remote is never an offline fallback signal.
///
/// The Rust native facade has no public explicit-offline toggle, so
/// `RemoteIfPossible` has the same strict waiting behavior as `Remote` there.
/// Browser and other host bindings apply the explicit-disconnect fallback at
/// their connection boundary before lowering this choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadTier {
    /// Read immediately from local knowledge.
    LocalFirst,
    /// Wait for the ordinary remote view.
    Remote,
    /// Use local knowledge only while the host is explicitly disconnected;
    /// otherwise wait for the ordinary remote view.
    RemoteIfPossible,
}

impl ReadTier {
    /// Lower this product-level choice to the legacy facade durability tier.
    ///
    /// This is intentionally read-only. Writes and write settlement keep using
    /// [`DurabilityTier`] directly.
    pub const fn legacy_durability_tier(self) -> DurabilityTier {
        match self {
            Self::LocalFirst => DurabilityTier::Local,
            Self::Remote | Self::RemoteIfPossible => DurabilityTier::EdgeServer,
        }
    }
}

/// Unique identifier for a client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientId(pub Uuid);

impl ClientId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Parse from UUID string.
    pub fn parse(s: &str) -> Option<Self> {
        Uuid::parse_str(s).ok().map(ClientId)
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
