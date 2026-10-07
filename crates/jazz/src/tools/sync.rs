use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Persistence tier: local storage or the authoritative Core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum DurabilityTier {
    Local,
    GlobalServer,
}

/// Product-level consistency choice for reads.
///
/// Read tiers deliberately do not expose the storage/protocol durability
/// lattice. There are two: [`ReadTier::LocalFirst`] reads what is locally
/// known, and [`ReadTier::Remote`] waits for the ordinary remote view. A
/// local-first read may additionally wait a bounded time for the server's
/// answer on its opening; see `JazzClient::query_local_first` and
/// `JazzClient::subscribe_local_first`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadTier {
    /// Read immediately from local knowledge.
    LocalFirst,
    /// Wait for the ordinary remote view.
    Remote,
}

impl ReadTier {
    /// Lower this product-level choice to the legacy facade durability tier.
    ///
    /// This is intentionally read-only. Writes and write settlement keep using
    /// [`DurabilityTier`] directly.
    pub const fn legacy_durability_tier(self) -> DurabilityTier {
        match self {
            Self::LocalFirst => DurabilityTier::Local,
            Self::Remote => DurabilityTier::GlobalServer,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durability_v2_has_only_local_and_global_tags() {
        for (bytes, expected, encoded) in [
            (vec![0], DurabilityTier::Local, vec![0]),
            (vec![1], DurabilityTier::GlobalServer, vec![1]),
        ] {
            assert_eq!(
                postcard::from_bytes::<DurabilityTier>(&bytes).unwrap(),
                expected
            );
            assert_eq!(postcard::to_allocvec(&expected).unwrap(), encoded);
        }
        assert!(postcard::from_bytes::<DurabilityTier>(&[2]).is_err());
        assert_eq!(
            ReadTier::Remote.legacy_durability_tier(),
            DurabilityTier::GlobalServer
        );
    }

    /// The serialized read-tier encoding is not observable through the
    /// client API, so it is pinned here: only the two tiers decode, and the
    /// removed tiers' names and postcard index 2 are rejected.
    #[test]
    fn only_the_two_read_tiers_decode() {
        for (tier, bytes) in [(ReadTier::LocalFirst, vec![0]), (ReadTier::Remote, vec![1])] {
            assert_eq!(postcard::to_allocvec(&tier).unwrap(), bytes);
            assert_eq!(postcard::from_bytes::<ReadTier>(&bytes).unwrap(), tier);
        }
        assert!(postcard::from_bytes::<ReadTier>(&[2]).is_err());
        for removed in ["\"RemoteIfPossible\"", "\"LocalFirstUnlessEmpty\""] {
            assert!(serde_json::from_str::<ReadTier>(removed).is_err());
        }
    }
}
