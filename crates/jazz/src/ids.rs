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

/// Node-local identity for one shared physical table lineage.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct PhysicalTableId(pub u64);

/// Node-local identity for one physical column epoch.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct PhysicalColumnId(pub u64);

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

/// Content-addressed atomic schema-lineage publication identity.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SchemaLineagePublicationId(pub uuid::Uuid);

impl SchemaLineagePublicationId {
    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Stable identity for a schema family across catalogue projections.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SchemaFamilyId(pub uuid::Uuid);

impl SchemaFamilyId {
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

/// Authenticated subject recorded on transactions and row provenance.
///
/// The portable identity is canonical JSON `[issuer, subject]`. Authenticated
/// values are interned in memory; the intern handle is never persisted, sent,
/// exposed to queries, or used as public ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AuthorSubject {
    /// Internal authority capability; never admitted from JWT claims.
    System,
    /// One canonical, interned `[issuer, subject]` JSON string.
    Authenticated(internment::Intern<String>),
}

impl AuthorSubject {
    /// Internal authority subject that bypasses policy checks.
    pub const SYSTEM: Self = Self::System;
    /// Reserved issuer namespace for internal authority work.
    pub const SYSTEM_ISSUER: &'static str = "urn:jazz:system";
    /// Subject component of the internal authority identity.
    pub const SYSTEM_SUBJECT: &'static str = "system";
    /// Portable canonical representation of the internal authority identity.
    pub const SYSTEM_CANONICAL: &'static str = r#"["urn:jazz:system","system"]"#;

    /// Construct a subject from already authenticated JWT components.
    pub fn authenticated(issuer: &str, subject: &str) -> Self {
        assert_ne!(issuer, Self::SYSTEM_ISSUER, "system issuer is reserved");
        let canonical = serde_json::to_string(&(issuer, subject))
            .expect("two strings always have a canonical JSON encoding");
        Self::Authenticated(internment::Intern::new(canonical))
    }

    /// Deterministic identity for internal fixtures and simulations.
    pub fn for_test_bytes(bytes: [u8; 16]) -> Self {
        Self::authenticated("urn:jazz:test", &uuid::Uuid::from_bytes(bytes).to_string())
    }

    /// Deterministic identity for fixtures that already use UUID values.
    pub fn for_test_uuid(value: uuid::Uuid) -> Self {
        Self::authenticated("urn:jazz:test", &value.to_string())
    }

    /// Recover the UUID subject used by deterministic legacy fixtures.
    ///
    /// This is test support only; production identity semantics use the full
    /// canonical issuer-and-subject string.
    #[doc(hidden)]
    pub fn test_uuid(&self) -> uuid::Uuid {
        let (issuer, subject): (String, String) =
            serde_json::from_str(self.canonical()).expect("authenticated fixture subject");
        assert_eq!(issuer, "urn:jazz:test", "not a UUID-backed test subject");
        uuid::Uuid::parse_str(&subject).expect("test subject is a UUID")
    }

    /// Parse a portable canonical subject, rejecting alternate JSON spellings.
    pub fn from_canonical(canonical: &str) -> Result<Self, String> {
        if canonical == Self::SYSTEM_CANONICAL {
            return Ok(Self::SYSTEM);
        }
        let (issuer, subject): (String, String) =
            serde_json::from_str(canonical).map_err(|error| error.to_string())?;
        if issuer == Self::SYSTEM_ISSUER {
            return Err("system issuer is reserved".to_owned());
        }
        let author = Self::authenticated(&issuer, &subject);
        if author.canonical() != canonical {
            return Err("author subject is not canonically JSON encoded".to_owned());
        }
        Ok(author)
    }

    /// Return the portable canonical JSON string.
    pub fn canonical(&self) -> &str {
        match self {
            Self::System => Self::SYSTEM_CANONICAL,
            Self::Authenticated(value) => value.as_str(),
        }
    }
}

impl PartialOrd for AuthorSubject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AuthorSubject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.canonical().cmp(other.canonical())
    }
}

impl serde::Serialize for AuthorSubject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.canonical())
    }
}

impl<'de> serde::Deserialize<'de> for AuthorSubject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let canonical = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_canonical(&canonical).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::AuthorSubject;

    #[test]
    fn author_subject_is_canonical_json_and_interned() {
        let first = AuthorSubject::authenticated("https://issuer.example", "opaque:subject");
        let second = AuthorSubject::authenticated("https://issuer.example", "opaque:subject");
        assert_eq!(first, second);
        assert_eq!(
            first.canonical(),
            r#"["https://issuer.example","opaque:subject"]"#
        );
        assert_eq!(
            AuthorSubject::SYSTEM.canonical(),
            AuthorSubject::SYSTEM_CANONICAL
        );
    }
}
