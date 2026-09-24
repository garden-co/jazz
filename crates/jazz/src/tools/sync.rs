use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Persistence tier: local storage or the authoritative Core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(from = "DurabilityEncoding", into = "DurabilityEncoding")]
pub enum DurabilityTier {
    Local,
    GlobalServer,
}

// Preserve the facade's existing serialized tags, independently of the core
// transaction encoding. The removed intermediate tier is decode-only.
#[derive(Serialize, Deserialize)]
enum DurabilityEncoding {
    Local,
    EdgeServer,
    GlobalServer,
}

impl From<DurabilityEncoding> for DurabilityTier {
    fn from(value: DurabilityEncoding) -> Self {
        match value {
            DurabilityEncoding::Local | DurabilityEncoding::EdgeServer => Self::Local,
            DurabilityEncoding::GlobalServer => Self::GlobalServer,
        }
    }
}

impl From<DurabilityTier> for DurabilityEncoding {
    fn from(value: DurabilityTier) -> Self {
        match value {
            DurabilityTier::Local => Self::Local,
            DurabilityTier::GlobalServer => Self::GlobalServer,
        }
    }
}

/// Product-level consistency choice for reads.
///
/// Read tiers deliberately do not expose the storage/protocol durability
/// lattice.  [`ReadTier::LocalFirst`] reads what is locally known,
/// [`ReadTier::Remote`] waits for the ordinary remote view, and
/// [`ReadTier::LocalFirstUnlessEmpty`] reads locally but withholds an *empty*
/// local opening until the first remote view arrives, when a remote could
/// supply matching data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadTier {
    /// Read immediately from local knowledge.
    LocalFirst,
    /// Wait for the ordinary remote view.
    Remote,
    /// Read like [`ReadTier::LocalFirst`], except that an empty local opening
    /// waits for the first remote view while an upstream link is live.
    ///
    /// A non-empty local result is delivered immediately. An empty one is
    /// withheld until the remote view first settles, the local result becomes
    /// non-empty, or the upstream link is (or becomes) unavailable, whichever
    /// comes first; it never waits without a live link. After its opening the
    /// read behaves exactly like `LocalFirst`.
    #[serde(alias = "RemoteIfPossible")]
    LocalFirstUnlessEmpty,
}

impl ReadTier {
    /// Former name of [`ReadTier::LocalFirstUnlessEmpty`].
    #[deprecated(note = "RemoteIfPossible was replaced; use ReadTier::LocalFirstUnlessEmpty")]
    #[allow(non_upper_case_globals)]
    pub const RemoteIfPossible: ReadTier = ReadTier::LocalFirstUnlessEmpty;

    /// Lower this product-level choice to the legacy facade durability tier.
    ///
    /// This is intentionally read-only. Writes and write settlement keep using
    /// [`DurabilityTier`] directly. `LocalFirstUnlessEmpty` lowers to the
    /// local-first tier; its empty-opening gate is applied by the reader.
    pub const fn legacy_durability_tier(self) -> DurabilityTier {
        match self {
            Self::LocalFirst | Self::LocalFirstUnlessEmpty => DurabilityTier::Local,
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
    fn durability_preserves_facade_encoding_without_an_edge_api() {
        for (bytes, expected, encoded) in [
            (vec![0], DurabilityTier::Local, vec![0]),
            (vec![1], DurabilityTier::Local, vec![0]),
            (vec![2], DurabilityTier::GlobalServer, vec![2]),
        ] {
            assert_eq!(
                postcard::from_bytes::<DurabilityTier>(&bytes).unwrap(),
                expected
            );
            assert_eq!(postcard::to_allocvec(&expected).unwrap(), encoded);
        }
        assert_eq!(
            ReadTier::Remote.legacy_durability_tier(),
            DurabilityTier::GlobalServer
        );
    }

    /// The serialized read-tier encoding is not observable through the
    /// client API, so it is pinned here: the replacement keeps the retired
    /// variant's postcard index and still decodes its textual name.
    #[test]
    fn local_first_unless_empty_keeps_the_retired_remote_if_possible_encoding() {
        for (tier, bytes) in [
            (ReadTier::LocalFirst, vec![0]),
            (ReadTier::Remote, vec![1]),
            (ReadTier::LocalFirstUnlessEmpty, vec![2]),
        ] {
            assert_eq!(postcard::to_allocvec(&tier).unwrap(), bytes);
            assert_eq!(postcard::from_bytes::<ReadTier>(&bytes).unwrap(), tier);
        }
        assert_eq!(
            serde_json::from_str::<ReadTier>("\"RemoteIfPossible\"").unwrap(),
            ReadTier::LocalFirstUnlessEmpty
        );
        assert_eq!(
            serde_json::to_string(&ReadTier::LocalFirstUnlessEmpty).unwrap(),
            "\"LocalFirstUnlessEmpty\""
        );
        #[allow(deprecated)]
        let alias = ReadTier::RemoteIfPossible;
        assert_eq!(alias, ReadTier::LocalFirstUnlessEmpty);
        assert_eq!(
            ReadTier::LocalFirstUnlessEmpty.legacy_durability_tier(),
            DurabilityTier::Local
        );
    }
}
