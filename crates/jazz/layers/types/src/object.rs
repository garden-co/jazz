use crate::{ids::NodeUuid, time::TxTime};
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

// A public type in a private module: nameable inside the crate, reachable
// from outside only through `ResultKey` and row fields, as before the move.
mod occurrence {
    use super::*;

    /// Complete identity of one occurrence emitted by a query source.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct SourceOccurrenceId {
        row: ObjectId,
        version: Option<(TxTime, NodeUuid)>,
    }

    impl SourceOccurrenceId {
        pub fn current(row: ObjectId) -> Self {
            Self { row, version: None }
        }

        pub fn history(row: ObjectId, time: TxTime, node: NodeUuid) -> Self {
            Self {
                row,
                version: Some((time, node)),
            }
        }

        pub fn row_id(&self) -> ObjectId {
            self.row
        }

        pub fn version(&self) -> Option<(TxTime, NodeUuid)> {
            self.version
        }
    }

    /// Stable identity of one rendered query-output occurrence.
    ///
    /// The root source row is always first. `joined` contains contributing source
    /// rows in the query's declared join order. This is deliberately distinct from
    /// a source [`ObjectId`]: one source row can contribute to more than one output
    /// occurrence. Serialization delegates to ResultKey so every identity field
    /// survives a round trip through the single result-key encoding.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    #[serde(from = "ResultKey", into = "ResultKey")]
    pub struct OutputOccurrenceId {
        root: SourceOccurrenceId,
        joined: SmallVec<[SourceOccurrenceId; 2]>,
        /// Stable typed derivation discriminators keyed by joined-source position.
        union_arms: SmallVec<[(usize, String); 1]>,
    }

    impl OutputOccurrenceId {
        pub fn has_typed_discriminators(&self) -> bool {
            !self.union_arms.is_empty()
                || self
                    .source_occurrences()
                    .any(|source| source.version().is_some())
        }

        pub fn root_source(&self) -> ObjectId {
            self.root.row_id()
        }

        pub fn union_arms(&self) -> &[(usize, String)] {
            &self.union_arms
        }

        /// Construct an occurrence from its root and joined source rows.
        ///
        /// `joined` must be in declared join order. The ordinary one- and two-hop
        /// cases stay inline.
        pub fn new(root: ObjectId, joined: impl IntoIterator<Item = ObjectId>) -> Self {
            Self {
                root: SourceOccurrenceId::current(root),
                joined: joined
                    .into_iter()
                    .map(SourceOccurrenceId::current)
                    .collect(),
                union_arms: SmallVec::new(),
            }
        }

        pub fn with_union_arms(
            root: ObjectId,
            joined: impl IntoIterator<Item = ObjectId>,
            union_arms: impl IntoIterator<Item = (usize, String)>,
        ) -> Option<Self> {
            Self::from_source_occurrences(
                SourceOccurrenceId::current(root),
                joined.into_iter().map(SourceOccurrenceId::current),
                union_arms,
            )
        }

        /// Compose complete source identities in declared join order.
        pub fn from_source_occurrences(
            root: SourceOccurrenceId,
            joined: impl IntoIterator<Item = SourceOccurrenceId>,
            union_arms: impl IntoIterator<Item = (usize, String)>,
        ) -> Option<Self> {
            let joined = joined.into_iter().collect::<SmallVec<[_; 2]>>();
            let mut union_arms = union_arms.into_iter().collect::<SmallVec<[_; 1]>>();
            union_arms.sort_by_key(|(position, _)| *position);
            let valid = union_arms
                .iter()
                .all(|(position, label)| *position <= joined.len() && !label.is_empty())
                && union_arms.windows(2).all(|pair| pair[0].0 != pair[1].0);
            valid.then_some(Self {
                root,
                joined,
                union_arms,
            })
        }

        /// Construct the single-source occurrence used by plain-table output.
        pub fn single_source(root: ObjectId) -> Self {
            Self {
                root: SourceOccurrenceId::current(root),
                joined: SmallVec::new(),
                union_arms: SmallVec::new(),
            }
        }

        /// Identify one retained content version without synthesizing a row UUID.
        pub fn history(root: ObjectId, time: TxTime, node: NodeUuid) -> Self {
            Self {
                root: SourceOccurrenceId::history(root, time, node),
                joined: SmallVec::new(),
                union_arms: SmallVec::new(),
            }
        }

        /// Full portable transaction identity of a historical root, if any.
        pub fn root_version(&self) -> Option<(TxTime, NodeUuid)> {
            self.root.version()
        }

        /// Source rows contributed after the root, in declared join order.
        pub fn joined_sources(&self) -> impl ExactSizeIterator<Item = ObjectId> + '_ {
            self.joined.iter().map(SourceOccurrenceId::row_id)
        }

        /// Complete source keys in declared join order, including the root.
        pub fn source_occurrences(&self) -> impl Iterator<Item = SourceOccurrenceId> + '_ {
            std::iter::once(self.root).chain(self.joined.iter().copied())
        }

        /// Canonical positional bytes for terminal-state keys and consolidation.
        ///
        /// Each component is a fixed-width UUID, so concatenating root followed by
        /// joined rows is unambiguous; byte length records the number of joined
        /// sources and component position records declared join position.
        /// Historical occurrences use their complete ResultKey bytes so every
        /// contributing version remains part of identity, independently of payload.
        pub fn canonical_bytes(&self) -> SmallVec<[u8; 48]> {
            if self.has_typed_discriminators() {
                let mut bytes = self.typed_canonical_bytes();
                if self
                    .source_occurrences()
                    .any(|source| source.version().is_some())
                {
                    bytes.insert(0, RESULT_KEY_WIRE_VERSION);
                }
                return bytes;
            }
            let mut bytes = SmallVec::with_capacity((self.joined.len() + 1) * 16);
            bytes.extend_from_slice(self.root.row_id().uuid().as_bytes());
            for id in &self.joined {
                bytes.extend_from_slice(id.row_id().uuid().as_bytes());
            }
            bytes
        }

        pub(super) fn typed_canonical_bytes(&self) -> SmallVec<[u8; 48]> {
            let mut bytes = SmallVec::new();
            bytes.extend_from_slice(self.root.row_id().uuid().as_bytes());
            bytes.extend_from_slice(&(self.joined.len() as u32).to_be_bytes());
            for id in &self.joined {
                bytes.extend_from_slice(id.row_id().uuid().as_bytes());
            }
            bytes.extend_from_slice(&(self.union_arms.len() as u32).to_be_bytes());
            for (position, label) in &self.union_arms {
                bytes.extend_from_slice(&(*position as u32).to_be_bytes());
                bytes.extend_from_slice(&(label.len() as u32).to_be_bytes());
                bytes.extend_from_slice(label.as_bytes());
            }
            for source in self.source_occurrences() {
                bytes.push(u8::from(source.version().is_some()));
                if let Some((time, node)) = source.version() {
                    bytes.extend_from_slice(&time.0.to_be_bytes());
                    bytes.extend_from_slice(node.0.as_bytes());
                }
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
            self.joined.is_empty()
                && !self.has_typed_discriminators()
                && self.root.row_id() == *other
        }
    }

    impl PartialEq<OutputOccurrenceId> for ObjectId {
        fn eq(&self, other: &OutputOccurrenceId) -> bool {
            other == self
        }
    }
}

#[doc(hidden)]
pub use occurrence::{OutputOccurrenceId, SourceOccurrenceId};

/// Stable, opaque identity of one query result.
///
/// A key may identify either one source row or one particular combination of
/// rows produced by a join. Callers should treat it as an indivisible address:
/// its representation is deliberately not part of the public API.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResultKey(OutputOccurrenceId);

const RESULT_KEY_WIRE_VERSION: u8 = 2;

impl Serialize for ResultKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let identity = self.0.typed_canonical_bytes();
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
        let Some(version) = encoded.first().copied() else {
            return Err(serde::de::Error::custom("unsupported ResultKey version"));
        };
        let identity = &encoded[1..];
        let key = (version == RESULT_KEY_WIRE_VERSION)
            .then(|| decode_typed_result_key(identity))
            .flatten();
        key.ok_or_else(|| serde::de::Error::custom("unsupported or malformed ResultKey identity"))
    }
}

fn decode_typed_result_key(identity: &[u8]) -> Option<ResultKey> {
    const MAX_JOINED_COMPONENTS: usize = 256;
    const MAX_DISCRIMINATOR_BYTES: usize = 4 * 1024;
    fn take<'a>(identity: &'a [u8], cursor: &mut usize, len: usize) -> Option<&'a [u8]> {
        let end = cursor.checked_add(len)?;
        let value = identity.get(*cursor..end)?;
        *cursor = end;
        Some(value)
    }
    let mut cursor = 0usize;
    let object_id = |bytes: &[u8]| {
        let bytes: [u8; 16] = bytes.try_into().ok()?;
        Some(ObjectId::from_uuid(Uuid::from_bytes(bytes)))
    };
    let root = object_id(take(identity, &mut cursor, 16)?)?;
    let joined_count =
        u32::from_be_bytes(take(identity, &mut cursor, 4)?.try_into().ok()?) as usize;
    if joined_count > MAX_JOINED_COMPONENTS
        || identity.len().saturating_sub(cursor) < joined_count.checked_mul(16)?.checked_add(4)?
    {
        return None;
    }
    let mut joined = Vec::with_capacity(joined_count);
    for _ in 0..joined_count {
        joined.push(object_id(take(identity, &mut cursor, 16)?)?);
    }
    let discriminator_count =
        u32::from_be_bytes(take(identity, &mut cursor, 4)?.try_into().ok()?) as usize;
    if discriminator_count > joined_count.checked_add(1)? {
        return None;
    }
    let mut union_arms = Vec::with_capacity(discriminator_count);
    let mut previous_position = None;
    for _ in 0..discriminator_count {
        let position =
            u32::from_be_bytes(take(identity, &mut cursor, 4)?.try_into().ok()?) as usize;
        let len = u32::from_be_bytes(take(identity, &mut cursor, 4)?.try_into().ok()?) as usize;
        if len == 0 || len > MAX_DISCRIMINATOR_BYTES || len > identity.len().saturating_sub(cursor)
        {
            return None;
        }
        if position > joined_count || previous_position.is_some_and(|previous| position <= previous)
        {
            return None;
        }
        let label = std::str::from_utf8(take(identity, &mut cursor, len)?)
            .ok()?
            .to_owned();
        previous_position = Some(position);
        union_arms.push((position, label));
    }
    let mut sources = Vec::with_capacity(joined_count + 1);
    for row in std::iter::once(root).chain(joined) {
        sources.push(match take(identity, &mut cursor, 1)?[0] {
            0 => SourceOccurrenceId::current(row),
            1 => {
                let time = TxTime(u64::from_be_bytes(
                    take(identity, &mut cursor, 8)?.try_into().ok()?,
                ));
                let node = NodeUuid(Uuid::from_slice(take(identity, &mut cursor, 16)?).ok()?);
                SourceOccurrenceId::history(row, time, node)
            }
            _ => return None,
        });
    }
    if cursor != identity.len() {
        return None;
    }
    let mut sources = sources.into_iter();
    OutputOccurrenceId::from_source_occurrences(sources.next()?, sources, union_arms).map(ResultKey)
}

impl ResultKey {
    /// Wrap a fully qualified output occurrence as its opaque transport key.
    pub fn from_occurrence(value: OutputOccurrenceId) -> Self {
        Self(value)
    }

    #[doc(hidden)]
    pub fn from_union_occurrence(
        root: ObjectId,
        joined: impl IntoIterator<Item = ObjectId>,
        union_arms: impl IntoIterator<Item = (usize, String)>,
    ) -> Option<Self> {
        OutputOccurrenceId::with_union_arms(root, joined, union_arms).map(Self)
    }

    pub fn as_occurrence(&self) -> &OutputOccurrenceId {
        &self.0
    }

    /// Return the logical source row id for a single-source result, including history.
    pub fn row_id(&self) -> Option<ObjectId> {
        (self.0.joined_sources().len() == 0).then(|| self.0.root_source())
    }
}

impl From<OutputOccurrenceId> for ResultKey {
    fn from(value: OutputOccurrenceId) -> Self {
        Self(value)
    }
}

impl From<ResultKey> for OutputOccurrenceId {
    fn from(value: ResultKey) -> Self {
        value.0
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

    /// Alice's two retained versions keep distinct portable keys even when
    /// their logical row is the same. This lower-layer test pins exact binding
    /// bytes, which public subscription tests deliberately treat as opaque.
    #[test]
    fn result_key_preserves_full_transaction_identity() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/result-key.json")).unwrap();
        let root = ObjectId::from_uuid(Uuid::parse_str(fixture["root"].as_str().unwrap()).unwrap());
        let node = NodeUuid(Uuid::parse_str(fixture["node"].as_str().unwrap()).unwrap());
        let time = TxTime(fixture["time"].as_str().unwrap().parse().unwrap());
        let occurrence = OutputOccurrenceId::history(root, time, node);
        let key = ResultKey::from_occurrence(occurrence.clone());
        let bytes: Vec<u8> = serde_json::from_value(fixture["key"].clone()).unwrap();
        assert_eq!(serde_json::to_value(&key).unwrap(), fixture["key"]);
        assert_eq!(
            serde_json::from_value::<ResultKey>(fixture["key"].clone()).unwrap(),
            key
        );
        // Postcard's byte-string length prefix is 50, followed by the same bytes.
        let mut postcard = vec![50];
        postcard.extend_from_slice(&bytes);
        assert_eq!(postcard::to_allocvec(&key).unwrap(), postcard);
        assert_eq!(postcard::from_bytes::<ResultKey>(&postcard).unwrap(), key);
        assert_eq!(occurrence.root_version(), Some((time, node)));
        assert_eq!(occurrence.canonical_bytes().as_slice(), bytes);
        assert_eq!(key.row_id(), Some(root));
        assert_ne!(key, root);
        assert_ne!(key, ResultKey::from(root));
        for other in [
            OutputOccurrenceId::history(root, TxTime(time.0 - 1), node),
            OutputOccurrenceId::history(root, time, NodeUuid(Uuid::from_bytes([0x33; 16]))),
        ] {
            assert_ne!(occurrence.canonical_bytes(), other.canonical_bytes());
            assert_ne!(key, ResultKey::from_occurrence(other));
        }
        for length in 0..bytes.len() {
            assert!(
                serde_json::from_value::<ResultKey>(serde_json::json!(&bytes[..length])).is_err()
            );
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(serde_json::from_value::<ResultKey>(serde_json::json!(trailing)).is_err());
        let mut invalid_flag = bytes.clone();
        invalid_flag[25] = 2;
        assert!(serde_json::from_value::<ResultKey>(serde_json::json!(invalid_flag)).is_err());
        // Corrupt counts cannot manufacture missing joined/union components.
        for offset in [17, 21] {
            let mut invalid = bytes.clone();
            invalid[offset] = 1;
            assert!(serde_json::from_value::<ResultKey>(serde_json::json!(invalid)).is_err());
        }
    }

    /// Alice's joined historical versions round-trip with both source keys.
    /// Byte fixtures belong here because public queries treat keys as opaque.
    #[test]
    fn joined_history_result_key_preserves_each_source_version() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/result-key.json")).unwrap();
        let root = SourceOccurrenceId::history(
            ObjectId::from_uuid(Uuid::from_bytes([0x11; 16])),
            TxTime(u64::MAX),
            NodeUuid(Uuid::from_bytes([0x22; 16])),
        );
        let joined = SourceOccurrenceId::history(
            ObjectId::from_uuid(Uuid::from_bytes([0x33; 16])),
            TxTime(7),
            NodeUuid(Uuid::from_bytes([0x44; 16])),
        );
        let occurrence = OutputOccurrenceId::from_source_occurrences(root, [joined], []).unwrap();
        let key = ResultKey::from_occurrence(occurrence.clone());
        assert_eq!(serde_json::to_value(&key).unwrap(), fixture["joined_key"]);
        assert_eq!(
            serde_json::from_value::<ResultKey>(fixture["joined_key"].clone()).unwrap(),
            key
        );
        assert_eq!(
            postcard::from_bytes::<ResultKey>(&postcard::to_allocvec(&key).unwrap()).unwrap(),
            key
        );
        assert_eq!(
            occurrence.source_occurrences().collect::<Vec<_>>(),
            vec![root, joined]
        );
        let replaced = OutputOccurrenceId::from_source_occurrences(
            root,
            [SourceOccurrenceId::history(
                joined.row_id(),
                TxTime(8),
                joined.version().unwrap().1,
            )],
            [],
        )
        .unwrap();
        assert_ne!(occurrence.canonical_bytes(), replaced.canonical_bytes());
    }

    #[test]
    fn result_key_is_complete_for_ordinary_joined_and_union_occurrences() {
        let root = ObjectId::from_uuid(Uuid::from_u128(1));
        let joined = ObjectId::from_uuid(Uuid::from_u128(2));
        let plain = ResultKey(OutputOccurrenceId::new(root, [joined]));
        let plain_wire: Vec<u8> =
            serde_json::from_slice(&serde_json::to_vec(&plain).expect("encode plain key"))
                .expect("inspect plain key");
        assert_eq!(plain_wire[0], RESULT_KEY_WIRE_VERSION);
        let mut expected = Vec::from(root.uuid().as_bytes());
        expected.extend_from_slice(&1_u32.to_be_bytes());
        expected.extend_from_slice(joined.uuid().as_bytes());
        expected.extend_from_slice(&0_u32.to_be_bytes());
        expected.extend_from_slice(&[0, 0]);
        assert_eq!(&plain_wire[1..], expected);

        let typed = ResultKey(
            OutputOccurrenceId::with_union_arms(root, [joined], [(0, "direct".to_owned())])
                .expect("valid typed occurrence"),
        );
        let typed_encoded = serde_json::to_vec(&typed).expect("encode typed key");
        let typed_wire: Vec<u8> =
            serde_json::from_slice(&typed_encoded).expect("inspect typed key");
        assert_eq!(typed_wire[0], RESULT_KEY_WIRE_VERSION);
        assert_eq!(
            serde_json::from_slice::<ResultKey>(&typed_encoded).expect("decode typed key"),
            typed
        );
        let typed_postcard = postcard::to_allocvec(&typed).expect("postcard encode typed key");
        assert_eq!(
            postcard::from_bytes::<ResultKey>(&typed_postcard).expect("postcard decode typed key"),
            typed
        );
        assert_ne!(typed, plain);
    }

    /// Alice's current, joined, union, and history identities must survive
    /// direct serialization. This lower-layer test checks that all callers use
    /// the same complete ResultKey codec, rather than a lossy field subset.
    #[test]
    fn output_occurrence_serializes_through_result_key() {
        let root = ObjectId::from_uuid(Uuid::from_u128(1));
        let joined = ObjectId::from_uuid(Uuid::from_u128(2));
        for occurrence in [
            OutputOccurrenceId::single_source(root),
            OutputOccurrenceId::new(root, [joined]),
            OutputOccurrenceId::with_union_arms(root, [joined], [(0, "direct".to_owned())])
                .unwrap(),
            OutputOccurrenceId::history(root, TxTime(u64::MAX), NodeUuid(Uuid::from_u128(3))),
        ] {
            let key = ResultKey::from_occurrence(occurrence.clone());
            let bytes = postcard::to_allocvec(&occurrence).unwrap();
            assert_eq!(bytes, postcard::to_allocvec(&key).unwrap());
            assert_eq!(
                postcard::from_bytes::<OutputOccurrenceId>(&bytes).unwrap(),
                occurrence
            );
            let json = serde_json::to_value(&occurrence).unwrap();
            assert_eq!(json, serde_json::to_value(&key).unwrap());
            assert_eq!(
                serde_json::from_value::<OutputOccurrenceId>(json).unwrap(),
                occurrence
            );
        }
    }

    #[test]
    fn typed_result_key_rejects_empty_duplicate_and_trailing_discriminators() {
        let root = ObjectId::from_uuid(Uuid::from_u128(1));
        let joined = ObjectId::from_uuid(Uuid::from_u128(2));
        assert!(
            OutputOccurrenceId::with_union_arms(root, [joined], [(0, String::new())]).is_none()
        );
        assert!(
            OutputOccurrenceId::with_union_arms(
                root,
                [joined],
                [(0, "a".to_owned()), (0, "b".to_owned())]
            )
            .is_none()
        );

        let valid = OutputOccurrenceId::with_union_arms(root, [joined], [(0, "a".to_owned())])
            .expect("valid typed occurrence");
        let mut malformed = vec![RESULT_KEY_WIRE_VERSION];
        malformed.extend_from_slice(&valid.typed_canonical_bytes());
        malformed.push(0);
        assert!(serde_json::from_value::<ResultKey>(serde_json::json!(malformed)).is_err());

        let mut zero_arm = vec![RESULT_KEY_WIRE_VERSION];
        zero_arm.extend_from_slice(&valid.typed_canonical_bytes());
        zero_arm[37..41].copy_from_slice(&0_u32.to_be_bytes());
        zero_arm.truncate(41);
        zero_arm.extend_from_slice(&[0, 0]);
        assert_eq!(
            serde_json::from_value::<ResultKey>(serde_json::json!(zero_arm))
                .expect("ordinary joined key is valid"),
            ResultKey(OutputOccurrenceId::new(root, [joined]))
        );

        let second_joined = ObjectId::from_uuid(Uuid::from_u128(3));
        let mut reordered = vec![RESULT_KEY_WIRE_VERSION];
        reordered.extend_from_slice(root.uuid().as_bytes());
        reordered.extend_from_slice(&2_u32.to_be_bytes());
        reordered.extend_from_slice(joined.uuid().as_bytes());
        reordered.extend_from_slice(second_joined.uuid().as_bytes());
        reordered.extend_from_slice(&2_u32.to_be_bytes());
        reordered.extend_from_slice(&1_u32.to_be_bytes());
        reordered.extend_from_slice(&1_u32.to_be_bytes());
        reordered.push(b"b"[0]);
        reordered.extend_from_slice(&0_u32.to_be_bytes());
        reordered.extend_from_slice(&1_u32.to_be_bytes());
        reordered.push(b"a"[0]);
        assert!(
            serde_json::from_value::<ResultKey>(serde_json::json!(reordered)).is_err(),
            "typed discriminator records must use strictly ascending positions"
        );

        let mut invalid_utf8 = vec![RESULT_KEY_WIRE_VERSION];
        invalid_utf8.extend_from_slice(&valid.typed_canonical_bytes());
        let label_end = invalid_utf8.len() - 3;
        invalid_utf8[label_end] = 0xff;
        assert!(
            serde_json::from_value::<ResultKey>(serde_json::json!(invalid_utf8)).is_err(),
            "typed discriminator labels must be valid UTF-8"
        );

        let typed = ResultKey(valid);
        let mut oversized: Vec<u8> =
            serde_json::from_slice(&serde_json::to_vec(&typed).expect("encode typed key fixture"))
                .expect("inspect typed key fixture");
        oversized[17..21].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(serde_json::from_value::<ResultKey>(serde_json::json!(oversized)).is_err());

        let mut superseded_uuid_vector = vec![RESULT_KEY_WIRE_VERSION];
        superseded_uuid_vector.extend_from_slice(root.uuid().as_bytes());
        superseded_uuid_vector.extend_from_slice(joined.uuid().as_bytes());
        assert!(
            serde_json::from_value::<ResultKey>(serde_json::json!(superseded_uuid_vector)).is_err()
        );
        assert!(serde_json::from_value::<ResultKey>(serde_json::json!([2_u8])).is_err());
        let mut old = vec![1];
        old.extend_from_slice(root.uuid().as_bytes());
        old.extend_from_slice(&[0; 8]);
        assert!(serde_json::from_value::<ResultKey>(serde_json::json!(old)).is_err());
    }

    #[test]
    fn union_arms_address_equal_source_rows_independently() {
        let root = ObjectId::from_uuid(Uuid::from_u128(1));
        let joined = ObjectId::from_uuid(Uuid::from_u128(2));
        let direct =
            OutputOccurrenceId::with_union_arms(root, [joined], [(0, "direct".to_owned())])
                .expect("direct occurrence");
        let inherited =
            OutputOccurrenceId::with_union_arms(root, [joined], [(0, "inherited".to_owned())])
                .expect("inherited occurrence");
        let mut maintained = std::collections::BTreeSet::from([direct.clone(), inherited.clone()]);
        assert_eq!(maintained.len(), 2, "UNION ALL arms retain multiplicity");
        assert!(maintained.remove(&direct));
        assert_eq!(maintained, std::collections::BTreeSet::from([inherited]));
    }

    #[test]
    fn union_arm_can_discriminate_the_root_source_position() {
        let root = ObjectId::from_uuid(Uuid::from_u128(1));
        let occurrence = OutputOccurrenceId::with_union_arms(
            root,
            std::iter::empty(),
            [(0, "direct".to_owned())],
        )
        .expect("root source position is addressable");
        let key = ResultKey::from_occurrence(occurrence);
        assert_eq!(
            serde_json::from_slice::<ResultKey>(&serde_json::to_vec(&key).unwrap()).unwrap(),
            key
        );
    }
}
