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
    /// Stable typed derivation discriminators keyed by joined-source position.
    /// Empty for the ordinary row-only identity and omitted from its wire form.
    // The postcard OutputOccurrenceId carrier remains exactly `(root, joined)`.
    // ResultKey owns the complete typed V1 sidecar envelope and must never add
    // a positional postcard field here.
    #[serde(skip)]
    union_arms: SmallVec<[(usize, String); 1]>,
}

impl OutputOccurrenceId {
    pub(crate) fn has_typed_discriminators(&self) -> bool {
        !self.union_arms.is_empty()
    }

    pub(crate) fn root_source(&self) -> ObjectId {
        self.root
    }

    pub(crate) fn union_arms(&self) -> &[(usize, String)] {
        &self.union_arms
    }

    /// Construct an occurrence from its root and joined source rows.
    ///
    /// `joined` must be in declared join order. The ordinary one- and two-hop
    /// cases stay inline.
    pub fn new(root: ObjectId, joined: impl IntoIterator<Item = ObjectId>) -> Self {
        Self {
            root,
            joined: joined.into_iter().collect(),
            union_arms: SmallVec::new(),
        }
    }

    pub(crate) fn with_union_arms(
        root: ObjectId,
        joined: impl IntoIterator<Item = ObjectId>,
        union_arms: impl IntoIterator<Item = (usize, String)>,
    ) -> Option<Self> {
        let joined = joined.into_iter().collect::<SmallVec<[_; 2]>>();
        let mut union_arms = union_arms.into_iter().collect::<SmallVec<[_; 1]>>();
        union_arms.sort_by_key(|(position, _)| *position);
        let valid = union_arms
            .iter()
            .all(|(position, label)| *position < joined.len() && !label.is_empty())
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
            root,
            joined: SmallVec::new(),
            union_arms: SmallVec::new(),
        }
    }

    /// Source rows contributed after the root, in declared join order.
    pub(crate) fn joined_sources(&self) -> &[ObjectId] {
        &self.joined
    }

    /// Canonical positional bytes for terminal-state keys and consolidation.
    ///
    /// Each component is a fixed-width UUID, so concatenating root followed by
    /// joined rows is unambiguous; byte length records the number of joined
    /// sources and component position records declared join position.
    pub fn canonical_bytes(&self) -> SmallVec<[u8; 48]> {
        if !self.union_arms.is_empty() {
            return self.typed_canonical_bytes();
        }
        let mut bytes = SmallVec::with_capacity((self.joined.len() + 1) * 16);
        bytes.extend_from_slice(self.root.uuid().as_bytes());
        for id in &self.joined {
            bytes.extend_from_slice(id.uuid().as_bytes());
        }
        bytes
    }

    fn typed_canonical_bytes(&self) -> SmallVec<[u8; 48]> {
        let mut bytes = SmallVec::new();
        bytes.extend_from_slice(self.root.uuid().as_bytes());
        bytes.extend_from_slice(&(self.joined.len() as u32).to_be_bytes());
        for id in &self.joined {
            bytes.extend_from_slice(id.uuid().as_bytes());
        }
        bytes.extend_from_slice(&(self.union_arms.len() as u32).to_be_bytes());
        for (position, label) in &self.union_arms {
            bytes.extend_from_slice(&(*position as u32).to_be_bytes());
            bytes.extend_from_slice(&(label.len() as u32).to_be_bytes());
            bytes.extend_from_slice(label.as_bytes());
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
        self.joined.is_empty() && self.union_arms.is_empty() && self.root == *other
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
        if version != RESULT_KEY_WIRE_VERSION {
            return Err(serde::de::Error::custom(
                "unsupported or malformed ResultKey identity",
            ));
        }
        decode_typed_result_key(identity)
            .ok_or_else(|| serde::de::Error::custom("malformed ResultKey V1 identity"))
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
    if discriminator_count > joined_count {
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
        if position >= joined_count
            || previous_position.is_some_and(|previous| position <= previous)
        {
            return None;
        }
        let label = std::str::from_utf8(take(identity, &mut cursor, len)?)
            .ok()?
            .to_owned();
        previous_position = Some(position);
        union_arms.push((position, label));
    }
    if cursor != identity.len() {
        return None;
    }
    OutputOccurrenceId::with_union_arms(root, joined, union_arms).map(ResultKey)
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

    #[test]
    fn result_key_v1_is_complete_for_ordinary_joined_and_union_occurrences() {
        let root = ObjectId::from_uuid(Uuid::from_u128(1));
        let joined = ObjectId::from_uuid(Uuid::from_u128(2));
        let plain = ResultKey(OutputOccurrenceId::new(root, [joined]));
        let plain_wire: Vec<u8> =
            serde_json::from_slice(&serde_json::to_vec(&plain).expect("encode plain key"))
                .expect("inspect plain key");
        assert_eq!(plain_wire[0], RESULT_KEY_WIRE_VERSION);
        let mut expected_v1 = Vec::from(root.uuid().as_bytes());
        expected_v1.extend_from_slice(&1_u32.to_be_bytes());
        expected_v1.extend_from_slice(joined.uuid().as_bytes());
        expected_v1.extend_from_slice(&0_u32.to_be_bytes());
        assert_eq!(&plain_wire[1..], expected_v1);

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

    #[test]
    fn output_occurrence_postcard_remains_exact_legacy_two_field_wire() {
        #[derive(Serialize)]
        struct LegacyOccurrence {
            root: ObjectId,
            joined: SmallVec<[ObjectId; 2]>,
        }

        let root = ObjectId::from_uuid(Uuid::from_u128(1));
        let joined = ObjectId::from_uuid(Uuid::from_u128(2));
        let legacy = postcard::to_allocvec(&LegacyOccurrence {
            root,
            joined: SmallVec::from_slice(&[joined]),
        })
        .expect("encode legacy fixture");
        let mut golden = vec![16];
        golden.extend_from_slice(&[0_u8; 15]);
        golden.push(1);
        golden.push(1);
        golden.push(16);
        golden.extend_from_slice(&[0_u8; 15]);
        golden.push(2);
        assert_eq!(legacy, golden, "pin the historical postcard fixture");
        let current = postcard::to_allocvec(&OutputOccurrenceId::new(root, [joined]))
            .expect("encode current occurrence");
        assert_eq!(current, legacy, "row-only postcard bytes are unchanged");
        assert_eq!(
            postcard::from_bytes::<OutputOccurrenceId>(&legacy)
                .expect("decode exact legacy two-field bytes"),
            OutputOccurrenceId::new(root, [joined])
        );

        let typed = OutputOccurrenceId::with_union_arms(root, [joined], [(0, "direct".to_owned())])
            .expect("typed occurrence");
        assert_eq!(
            postcard::to_allocvec(&typed).expect("encode legacy occurrence carrier"),
            legacy,
            "typed derivation is deliberately not smuggled into the legacy wire struct"
        );
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
        assert_eq!(
            serde_json::from_value::<ResultKey>(serde_json::json!(zero_arm))
                .expect("ordinary joined V1 is valid"),
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
        *invalid_utf8.last_mut().expect("typed key has arm label") = 0xff;
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
}
