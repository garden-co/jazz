//! Wire-stable identifier newtypes for nodes, rows, schemas, branches, lenses,
//! and compact storage aliases. This module owns identity vocabulary and UUID
//! byte ordering only; allocation, alias persistence, and recovery live in
//! [`crate::node::codec`] and [`crate::node::recovery`]. These ids are shared
//! across every layer from `Db` facade calls through protocol messages to groove
//! storage keys.

/// Globally stable node identity used on the wire.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct NodeUuid(pub uuid::Uuid);

impl NodeUuid {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Node-local integer alias for compact storage.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct NodeAlias(pub u64);

/// Content-addressed schema version identity.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SchemaVersionId(pub uuid::Uuid);

impl SchemaVersionId {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Node-local integer alias for compact schema-version storage.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SchemaVersionAlias(pub u64);

/// Content-addressed migration-lens identity.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct MigrationLensId(pub uuid::Uuid);

impl MigrationLensId {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Stable branch identity used to address snapshot-overlay branches.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct BranchId(pub uuid::Uuid);

impl BranchId {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Stable row identity shared by every historical version of a row.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct RowUuid(pub uuid::Uuid);

impl RowUuid {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Authenticated user identity recorded on transactions.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct AuthorId(pub uuid::Uuid);

impl AuthorId {
    /// Internal/system author with unrestricted policy identity.
    ///
    /// Derived as `Uuid::new_v5(&Uuid::NAMESPACE_OID, b"jazz:system-author")`.
    pub const SYSTEM: Self = Self(uuid::uuid!("93c209ee-dbae-5071-a90d-02f8c0bbcf6a"));

    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::AuthorId;

    #[test]
    fn system_author_uuid_matches_v5_derivation() {
        assert_eq!(
            AuthorId::SYSTEM.0,
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"jazz:system-author")
        );
    }
}
