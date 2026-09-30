//! Durable numeric identities for schema indexes.
//!
//! Every durable secondary-index entry lives in the shared logical `indices`
//! family under a compact numeric index id instead of its table and index
//! names. The id is the minimal unsigned LEB128 encoding of a `u32 >= 1`, so
//! it is self-delimiting (prefix-free) and never starts with `0x00`: the
//! `0x00` first byte is reserved for Groove's index metadata (this registry,
//! the layout marker and the declared-index generation).
//!
//! The registry maps `(table, index)` to its id together with the index
//! definition it was assigned for. Ids are allocated monotonically and are
//! never reused: a redefined index (same name, different columns or
//! uniqueness) receives a fresh id, so entries written under the old
//! definition can never be read as the new one. Allocation is in memory and
//! synchronous (schema registration is synchronous); the registry entry is
//! written in the same atomic storage write as, or before, the first index
//! entry that uses the id.

use std::collections::{BTreeMap, BTreeSet};

use crate::schema::IndexSchema;
use crate::storage::{OrderedKvStorage, OwnedWriteOperation, ScanRequest};

/// The logical storage family that holds every durable schema index.
pub(crate) const INDICES_CF: &str = "indices";
/// Registry key prefix: `00 "groove-index-id" 00`, then `u16 BE` table-name
/// length, the table name and the index name.
pub(crate) const INDEX_ID_REGISTRY_PREFIX: &[u8] = b"\0groove-index-id\0";
/// Durable-index layout marker key in the `indices` family.
pub(crate) const INDEX_LAYOUT_MARKER_KEY: &[u8] = b"\0groove-index-layout";
/// The only admitted marker value: compact numeric-id keys with no value
/// record (`groove.durable-index.v2`).
pub(crate) const INDEX_LAYOUT_MARKER_VALUE: &[u8] = b"groove-durable-index-v2";

/// Shared registry handle. Runtime clones (registry checkpoints, staged index
/// registration) share one registry, so an id handed out once is never handed
/// out again for a different index within this process.
pub(crate) type SharedIndexIds = std::rc::Rc<std::cell::RefCell<IndexIdRegistry>>;

#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexIdEntry {
    id: u32,
    definition: Vec<u8>,
}

/// Durable `(table, index) -> id` assignments plus their persistence state.
#[derive(Debug, Default)]
pub(crate) struct IndexIdRegistry {
    entries: BTreeMap<(String, String), IndexIdEntry>,
    names: BTreeMap<u32, (String, String)>,
    /// Highest id ever observed or allocated; the next allocation is one more.
    max_id: u32,
    /// Assignments not yet known to be durable.
    unpersisted: BTreeSet<(String, String)>,
    /// Whether the layout marker is known to be durable.
    marker_persisted: bool,
}

/// The registry writes one flush must make durable, and what to mark once it
/// has.
#[derive(Debug, Default)]
pub(crate) struct PendingIndexIds {
    operations: Vec<OwnedWriteOperation>,
    marker: bool,
    entries: Vec<((String, String), u32)>,
}

impl PendingIndexIds {
    pub(crate) fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub(crate) fn operations(&self) -> &[OwnedWriteOperation] {
        &self.operations
    }
}

impl IndexIdRegistry {
    /// Load the registry from the `indices` family and validate its layout
    /// marker. A store whose `indices` family holds entries but no marker was
    /// written by an earlier durable-index layout and is refused with
    /// [`crate::storage::Error::InvalidStorageLayout`]; its keys are never
    /// interpreted. A missing `indices` family yields an empty registry.
    pub(crate) async fn load<S>(storage: &S) -> Result<Self, crate::storage::Error>
    where
        S: OrderedKvStorage + ?Sized,
    {
        let marker = match storage
            .get(INDICES_CF.to_owned(), INDEX_LAYOUT_MARKER_KEY.to_vec())
            .await
        {
            Ok(marker) => marker,
            Err(crate::storage::Error::ColumnFamilyNotFound(_)) => return Ok(Self::default()),
            Err(error) => return Err(error),
        };
        let mut registry = Self::default();
        match marker {
            Some(value) if value == INDEX_LAYOUT_MARKER_VALUE => {
                registry.marker_persisted = true;
            }
            Some(_) => {
                return Err(crate::storage::Error::InvalidStorageLayout(
                    "unsupported durable-index layout marker".to_owned(),
                ));
            }
            None => {
                // Metadata keys start with 0x00 and sort first, so the last
                // key is an index entry whenever any exists.
                let last = crate::storage::collect_scan(
                    storage
                        .scan(
                            ScanRequest::prefix(INDICES_CF.to_owned(), Vec::new())
                                .with_max_items(1)
                                .reversed(),
                        )
                        .await?,
                )
                .await?;
                if last.iter().any(|(key, _)| key.first() != Some(&0)) {
                    return Err(crate::storage::Error::InvalidStorageLayout(
                        "durable-index entries without the groove-durable-index-v2 layout marker"
                            .to_owned(),
                    ));
                }
                return Ok(registry);
            }
        }
        let entries = crate::storage::collect_scan(
            storage
                .scan(ScanRequest::prefix(
                    INDICES_CF.to_owned(),
                    INDEX_ID_REGISTRY_PREFIX.to_vec(),
                ))
                .await?,
        )
        .await?;
        for (key, value) in entries {
            let (table, index) = decode_registry_key(&key)?;
            let (id, definition) = decode_registry_value(&value)?;
            if registry
                .names
                .insert(id, (table.clone(), index.clone()))
                .is_some()
            {
                return Err(crate::storage::Error::InvalidStorageLayout(format!(
                    "durable index id {id} is assigned twice"
                )));
            }
            registry.max_id = registry.max_id.max(id);
            registry
                .entries
                .insert((table, index), IndexIdEntry { id, definition });
        }
        Ok(registry)
    }

    /// Return the id for `index` on `table`, allocating a fresh id when the
    /// pair is unknown or its stored definition differs.
    pub(crate) fn resolve(&mut self, table: &str, index: &IndexSchema) -> u32 {
        let definition = index_definition_bytes(index);
        let key = (table.to_owned(), index.name.clone());
        if let Some(entry) = self.entries.get(&key)
            && entry.definition == definition
        {
            return entry.id;
        }
        let id = self
            .max_id
            .checked_add(1)
            .expect("durable index ids must not wrap");
        self.max_id = id;
        if let Some(previous) = self
            .entries
            .insert(key.clone(), IndexIdEntry { id, definition })
        {
            // The superseded id stays retired: `max_id` never decreases.
            self.names.remove(&previous.id);
        }
        self.names.insert(id, key.clone());
        self.unpersisted.insert(key);
        id
    }

    /// Retire the id currently assigned to `index` on `table`, so its next
    /// resolution allocates a fresh one. An index (re)created at runtime is
    /// backfilled under a prefix no earlier incarnation wrote, so entries a
    /// dropped index left behind are never read, whatever its definition.
    pub(crate) fn retire(&mut self, table: &str, index: &str) {
        let key = (table.to_owned(), index.to_owned());
        if let Some(previous) = self.entries.remove(&key) {
            self.names.remove(&previous.id);
            self.unpersisted.remove(&key);
        }
    }

    /// The id currently assigned to `index` on `table`, if any.
    #[cfg(test)]
    pub(crate) fn id(&self, table: &str, index: &str) -> Option<u32> {
        self.entries
            .get(&(table.to_owned(), index.to_owned()))
            .map(|entry| entry.id)
    }

    /// The id `index` on `table` has, or the one its registration would
    /// receive next.
    #[cfg(test)]
    pub(crate) fn id_or_next(&self, table: &str, index: &str) -> u32 {
        self.id(table, index).unwrap_or(self.max_id + 1)
    }

    /// The `(table, index)` names an id was assigned to, if any.
    pub(crate) fn names(&self, id: u32) -> Option<(&str, &str)> {
        self.names
            .get(&id)
            .map(|(table, index)| (table.as_str(), index.as_str()))
    }

    /// Registry writes not yet known to be durable: the layout marker for a
    /// fresh store and every unpersisted assignment.
    pub(crate) fn pending(&self) -> PendingIndexIds {
        let mut pending = PendingIndexIds::default();
        if self.unpersisted.is_empty() {
            return pending;
        }
        if !self.marker_persisted {
            pending.marker = true;
            pending.operations.push(OwnedWriteOperation::Set {
                cf: INDICES_CF.to_owned(),
                key: INDEX_LAYOUT_MARKER_KEY.to_vec(),
                value: INDEX_LAYOUT_MARKER_VALUE.to_vec(),
            });
        }
        for key in &self.unpersisted {
            let entry = &self.entries[key];
            pending.operations.push(OwnedWriteOperation::Set {
                cf: INDICES_CF.to_owned(),
                key: encode_registry_key(&key.0, &key.1),
                value: encode_registry_value(entry.id, &entry.definition),
            });
            pending.entries.push((key.clone(), entry.id));
        }
        pending
    }

    /// Record that `pending` is durable (or is part of a write whose failure
    /// poisons this database instance).
    pub(crate) fn mark_persisted(&mut self, pending: &PendingIndexIds) {
        if pending.marker {
            self.marker_persisted = true;
        }
        for (key, id) in &pending.entries {
            if self.entries.get(key).is_some_and(|entry| entry.id == *id) {
                self.unpersisted.remove(key);
            }
        }
    }
}

/// Canonical bytes of an index definition: `unique` flag, `u16 BE` column
/// count, then each column name as `u16 BE` length plus UTF-8.
fn index_definition_bytes(index: &IndexSchema) -> Vec<u8> {
    let mut bytes = vec![u8::from(index.unique)];
    bytes.extend(
        u16::try_from(index.columns.len())
            .expect("index column count fits u16")
            .to_be_bytes(),
    );
    for column in &index.columns {
        bytes.extend(
            u16::try_from(column.len())
                .expect("column names fit u16")
                .to_be_bytes(),
        );
        bytes.extend(column.as_bytes());
    }
    bytes
}

fn encode_registry_key(table: &str, index: &str) -> Vec<u8> {
    let mut key = INDEX_ID_REGISTRY_PREFIX.to_vec();
    key.extend(
        u16::try_from(table.len())
            .expect("table names fit u16")
            .to_be_bytes(),
    );
    key.extend(table.as_bytes());
    key.extend(index.as_bytes());
    key
}

fn decode_registry_key(key: &[u8]) -> Result<(String, String), crate::storage::Error> {
    let malformed =
        || crate::storage::Error::InvalidStorageLayout("malformed durable index id key".to_owned());
    let rest = key
        .strip_prefix(INDEX_ID_REGISTRY_PREFIX)
        .ok_or_else(malformed)?;
    let (len, rest) = rest.split_first_chunk::<2>().ok_or_else(malformed)?;
    let len = usize::from(u16::from_be_bytes(*len));
    if rest.len() < len {
        return Err(malformed());
    }
    let (table, index) = rest.split_at(len);
    Ok((
        String::from_utf8(table.to_vec()).map_err(|_| malformed())?,
        String::from_utf8(index.to_vec()).map_err(|_| malformed())?,
    ))
}

fn encode_registry_value(id: u32, definition: &[u8]) -> Vec<u8> {
    let mut value = id.to_be_bytes().to_vec();
    value.extend(definition);
    value
}

fn decode_registry_value(value: &[u8]) -> Result<(u32, Vec<u8>), crate::storage::Error> {
    let (id, definition) = value.split_first_chunk::<4>().ok_or_else(|| {
        crate::storage::Error::InvalidStorageLayout("malformed durable index id value".to_owned())
    })?;
    let id = u32::from_be_bytes(*id);
    if id == 0 {
        return Err(crate::storage::Error::InvalidStorageLayout(
            "durable index id 0 is reserved".to_owned(),
        ));
    }
    Ok((id, definition.to_vec()))
}

/// The storage-key prefix of one durable index: the minimal unsigned LEB128
/// encoding of its id.
pub(crate) fn durable_index_key_prefix(id: u32) -> Vec<u8> {
    debug_assert!(id != 0, "durable index id 0 is reserved for metadata");
    let mut prefix = Vec::with_capacity(5);
    let mut rest = id;
    loop {
        let byte = (rest & 0x7f) as u8;
        rest >>= 7;
        if rest == 0 {
            prefix.push(byte);
            return prefix;
        }
        prefix.push(byte | 0x80);
    }
}

/// Split a durable index storage key into its id and logical key. Returns
/// `None` for metadata keys (first byte `0x00`) and malformed or non-minimal
/// id encodings.
pub(crate) fn split_durable_index_key(key: &[u8]) -> Option<(u32, &[u8])> {
    let mut id = 0_u32;
    for (position, byte) in key.iter().enumerate().take(5) {
        let bits = u32::from(byte & 0x7f);
        let shift = 7 * position as u32;
        if position == 4 && bits > 0x0f {
            return None;
        }
        id |= bits << shift;
        if byte & 0x80 == 0 {
            // Minimal: a final byte of zero is only valid as the sole byte,
            // and id 0 is metadata.
            if (position > 0 && *byte == 0) || id == 0 {
                return None;
            }
            return Some((id, &key[position + 1..]));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_id_prefixes_are_minimal_prefix_free_and_never_metadata() {
        let ids = [
            1,
            2,
            127,
            128,
            129,
            255,
            16_383,
            16_384,
            2_097_152,
            u32::MAX,
        ];
        let prefixes = ids.map(durable_index_key_prefix);
        assert_eq!(prefixes[0], [0x01]);
        assert_eq!(prefixes[3], [0x80, 0x01]);
        assert_eq!(prefixes[9], [0xff, 0xff, 0xff, 0xff, 0x0f]);
        for (id, prefix) in ids.iter().zip(&prefixes) {
            assert_ne!(prefix[0], 0, "id {id} must not collide with metadata");
            let mut key = prefix.clone();
            key.extend([0x00, 0xff]);
            assert_eq!(
                split_durable_index_key(&key),
                Some((*id, &[0x00, 0xff][..]))
            );
            for other in &prefixes {
                if other != prefix {
                    assert!(!other.starts_with(prefix), "{prefix:?} prefixes {other:?}");
                }
            }
        }
        assert_eq!(split_durable_index_key(b"\0groove-index-layout"), None);
        assert_eq!(split_durable_index_key(&[0x81, 0x00]), None, "non-minimal");
        assert_eq!(
            split_durable_index_key(&[0xff, 0xff, 0xff, 0xff, 0x1f]),
            None
        );
    }

    #[test]
    fn registry_reuses_ids_per_definition_and_never_reuses_a_retired_id() {
        let mut registry = IndexIdRegistry::default();
        let by_title = IndexSchema::new("by_title", ["title"]);
        let a = registry.resolve("albums", &by_title);
        let b = registry.resolve("albums", &IndexSchema::new("by_year", ["year"]));
        assert_eq!((a, b), (1, 2));
        assert_eq!(registry.resolve("albums", &by_title), a);
        // Same name, new definition: a fresh id; the old one is retired.
        let redefined =
            registry.resolve("albums", &IndexSchema::new("by_title", ["title", "year"]));
        assert_eq!(redefined, 3);
        assert_eq!(registry.names(a), None);
        assert_eq!(registry.names(redefined), Some(("albums", "by_title")));
        assert_eq!(registry.resolve("tracks", &by_title), 4);
        // An explicit (re)creation retires even an unchanged definition.
        registry.retire("tracks", "by_title");
        assert_eq!(registry.names(4), None);
        assert_eq!(registry.resolve("tracks", &by_title), 5);
    }

    #[test]
    fn registry_key_and_value_bytes_are_exact() {
        assert_eq!(
            encode_registry_key("albums", "by_title"),
            b"\0groove-index-id\0\x00\x06albumsby_title"
        );
        assert_eq!(
            decode_registry_key(b"\0groove-index-id\0\x00\x06albumsby_title").unwrap(),
            ("albums".to_owned(), "by_title".to_owned())
        );
        let definition = index_definition_bytes(&IndexSchema::new("by_title", ["title"]).unique());
        assert_eq!(definition, b"\x01\x00\x01\x00\x05title");
        assert_eq!(
            encode_registry_value(7, &definition),
            b"\x00\x00\x00\x07\x01\x00\x01\x00\x05title"
        );
        assert!(decode_registry_value(b"\x00\x00\x00\x00").is_err());
        assert!(decode_registry_key(b"\0groove-index-id\0\x00\x09albums").is_err());
    }
}
