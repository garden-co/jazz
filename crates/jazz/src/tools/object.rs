use internment::Intern;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;
use uuid::Uuid;

/// Interned UUID identifying an object.
/// Pointer-sized (8 bytes), Copy, fast equality via pointer comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId(pub Intern<Uuid>);

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.uuid().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let uuid = Uuid::deserialize(deserializer)?;
        Ok(ObjectId::from_uuid(uuid))
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ObjectId {
    /// Generate a new time-ordered UUIDv7 object id.
    pub fn new() -> Self {
        Self(Intern::new(Uuid::now_v7()))
    }

    /// Get the underlying UUID reference.
    pub fn uuid(&self) -> &Uuid {
        &self.0
    }

    /// Create an ObjectId from a raw Uuid.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(Intern::new(uuid))
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialOrd for ObjectId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ObjectId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.uuid().cmp(other.uuid())
    }
}

/// Stable identity of one rendered query-output occurrence.
///
/// The root source row is always first. `joined` contains contributing source
/// rows in the query's declared join order. This is deliberately distinct from
/// a source [`ObjectId`]: one source row can contribute to more than one output
/// occurrence.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OutputOccurrenceId {
    root: ObjectId,
    joined: SmallVec<[ObjectId; 2]>,
}

impl OutputOccurrenceId {
    /// Construct an occurrence from its root and joined source rows.
    ///
    /// `joined` must be in declared join order. The ordinary one- and two-hop
    /// cases stay inline.
    pub fn new(root: ObjectId, joined: impl IntoIterator<Item = ObjectId>) -> Self {
        Self {
            root,
            joined: joined.into_iter().collect(),
        }
    }

    /// Construct the single-source occurrence used by plain-table output.
    pub fn single_source(root: ObjectId) -> Self {
        Self {
            root,
            joined: SmallVec::new(),
        }
    }

    /// Canonical positional bytes for terminal-state keys and consolidation.
    ///
    /// Each component is a fixed-width UUID, so concatenating root followed by
    /// joined rows is unambiguous; byte length records the number of joined
    /// sources and component position records declared join position.
    pub fn canonical_bytes(&self) -> SmallVec<[u8; 48]> {
        let mut bytes = SmallVec::with_capacity((self.joined.len() + 1) * 16);
        bytes.extend_from_slice(self.root.uuid().as_bytes());
        for id in &self.joined {
            bytes.extend_from_slice(id.uuid().as_bytes());
        }
        bytes
    }
}

impl From<ObjectId> for OutputOccurrenceId {
    fn from(root: ObjectId) -> Self {
        Self::single_source(root)
    }
}

/// Single-source occurrences compare equal to their compatibility root id.
/// Multi-source occurrences intentionally do not: treating either as a plain
/// row id would recreate the flat-join collapse this type prevents.
impl PartialEq<ObjectId> for OutputOccurrenceId {
    fn eq(&self, other: &ObjectId) -> bool {
        self.joined.is_empty() && self.root == *other
    }
}

impl PartialEq<OutputOccurrenceId> for ObjectId {
    fn eq(&self, other: &OutputOccurrenceId) -> bool {
        other == self
    }
}

/// Stable, opaque identity of one query result.
///
/// A key may identify either one source row or one particular combination of
/// rows produced by a join. Callers should treat it as an indivisible address:
/// its representation is deliberately not part of the public API.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResultKey(OutputOccurrenceId);

const RESULT_KEY_WIRE_VERSION: u8 = 1;

impl Serialize for ResultKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let identity = self.0.canonical_bytes();
        let mut encoded = Vec::with_capacity(identity.len() + 1);
        encoded.push(RESULT_KEY_WIRE_VERSION);
        encoded.extend_from_slice(&identity);
        serde_bytes::Bytes::new(&encoded).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ResultKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = serde_bytes::ByteBuf::deserialize(deserializer)?.into_vec();
        if encoded.first().copied() != Some(RESULT_KEY_WIRE_VERSION) {
            return Err(serde::de::Error::custom("unsupported ResultKey version"));
        }
        let identity = &encoded[1..];
        if identity.is_empty() || identity.len() % 16 != 0 {
            return Err(serde::de::Error::custom("malformed ResultKey identity"));
        }
        let mut rows = identity.chunks_exact(16).map(|bytes| {
            let mut uuid = [0_u8; 16];
            uuid.copy_from_slice(bytes);
            ObjectId::from_uuid(Uuid::from_bytes(uuid))
        });
        let root = rows
            .next()
            .ok_or_else(|| serde::de::Error::custom("ResultKey is missing its root"))?;
        Ok(Self(OutputOccurrenceId::new(root, rows)))
    }
}

impl ResultKey {
    #[cfg(feature = "client")]
    pub(crate) fn from_occurrence(value: OutputOccurrenceId) -> Self {
        Self(value)
    }

    #[cfg(feature = "client")]
    pub(crate) fn as_occurrence(&self) -> &OutputOccurrenceId {
        &self.0
    }

    /// Return the source row id when this result is a plain, single-row result.
    pub fn row_id(&self) -> Option<ObjectId> {
        self.0.joined.is_empty().then_some(self.0.root)
    }
}

impl From<ObjectId> for ResultKey {
    fn from(value: ObjectId) -> Self {
        Self(OutputOccurrenceId::single_source(value))
    }
}

impl PartialEq<ObjectId> for ResultKey {
    fn eq(&self, other: &ObjectId) -> bool {
        self.0 == *other
    }
}

impl PartialEq<ResultKey> for ObjectId {
    fn eq(&self, other: &ResultKey) -> bool {
        other == self
    }
}

/// Interned name identifying a branch within an object.
/// Pointer-sized (8 bytes), Copy, fast equality via pointer comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BranchName(pub Intern<String>);

impl Serialize for BranchName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BranchName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(BranchName::new(s))
    }
}

impl BranchName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Intern::new(name.into()))
    }

    /// Get the underlying string reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Into<String>> From<T> for BranchName {
    fn from(s: T) -> Self {
        Self(Intern::new(s.into()))
    }
}

impl std::fmt::Display for BranchName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_id_generates_unique_values() {
        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        assert_ne!(id1, id2);
    }
}
