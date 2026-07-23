//! Durable persist operator writes.
//!
//! This module owns the write-through step for `Persist` nodes: translating
//! weighted record deltas into ordered storage keys, consolidating same-tick
//! updates by durable key, and enforcing unique-index conflicts. It does not
//! decide when persist nodes run; the runtime tick loop calls into this module
//! after evaluating the input node. Base table commits and schema-aware row
//! encoding live above in [`crate::db`] and [`crate::records`].

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::ivm::DurableStorage;
use crate::records::RecordDescriptor;
use crate::storage::{OrderedKvStorage, RecordStore};

use super::{
    IvmRuntimeError, RecordDeltas, encode_key_part, encode_ordered_bytes, index_record_descriptor,
};

/// The running net effect on one durable key while consolidating a tick.
///
/// `weight` sums the incoming deltas for the key; `positive_record` remembers
/// the most recent inserted row so a net-positive key knows which bytes to
/// write. A net weight of zero with a positive record means "-old, +new" for
/// the same key — an in-place update that must leave the entry present.
#[derive(Default)]
struct PendingPersistKey {
    weight: i64,
    positive_record: Option<Vec<u8>>,
}

/// Write-through for a `Persist` node: turns this tick's record deltas into
/// durable key writes.
///
/// * `storage` — the backing store.
/// * `durable_storage` — the column family and key prefix to write under.
/// * `key_fields` — output field indices forming the durable key.
/// * `unique` — when set, a second distinct row on an existing key is a
///   [`IvmRuntimeError::UniqueIndexViolation`].
/// * `delta` — this tick's weighted record changes.
///
/// Deltas are consolidated per key first (so `-old, +new` on one key nets to
/// a single write regardless of order), then applied as one batch. Index
/// entries (the `(key, value)` shape) take the specialized
/// [`apply_index_persist_delta`] path.
pub(super) fn apply_persist_delta(
    storage: &impl OrderedKvStorage,
    durable_storage: &DurableStorage,
    key_fields: &[usize],
    unique: bool,
    delta: &RecordDeltas,
) -> Result<(), IvmRuntimeError> {
    if key_fields == [0] && delta.descriptor == index_record_descriptor() {
        return apply_index_persist_delta(storage, durable_storage, unique, delta);
    }

    let store = RecordStore::new(storage, &durable_storage.column_family, &delta.descriptor);
    // Multiple deltas in one tick may touch the same durable key. Consolidate
    // by persisted key before writing: an update whose indexed key is
    // unchanged appears as `-old, +new` for the same key, and the final durable
    // entry must remain present regardless of delta order.
    let mut pending = HashMap::<Vec<u8>, PendingPersistKey>::new();
    for record_delta in &delta.deltas {
        let keys = persist_record_keys(
            &delta.descriptor,
            record_delta.raw(),
            key_fields,
            durable_storage,
        )?;

        for key in keys {
            if record_delta.weight > 0 {
                if unique {
                    // A unique index key may be rewritten by the same record, but
                    // not by a different record.
                    let current = if let Some(record) = pending
                        .get(&key)
                        .and_then(|entry| entry.positive_record.clone())
                    {
                        Some(record)
                    } else {
                        store.get_raw(&key)?
                    };
                    if current
                        .as_deref()
                        .is_some_and(|record| record != record_delta.raw())
                    {
                        return Err(IvmRuntimeError::UniqueIndexViolation {
                            index: durable_storage_name(durable_storage),
                        });
                    }
                }
                let entry = pending.entry(key).or_default();
                entry.weight += record_delta.weight;
                entry.positive_record = Some(record_delta.raw().to_vec());
            } else if record_delta.weight < 0 {
                if unique {
                    let current = store.get_raw(&key)?;
                    if current
                        .as_deref()
                        .is_some_and(|record| record != record_delta.raw())
                    {
                        continue;
                    }
                }
                pending.entry(key).or_default().weight += record_delta.weight;
            }
        }
    }

    let mut final_writes = BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
    for (key, entry) in pending {
        if entry.weight > 0 {
            let record = entry
                .positive_record
                .ok_or(IvmRuntimeError::PersistRecordMismatch)?;
            final_writes.insert(key, Some(record));
        } else if entry.weight < 0 {
            final_writes.insert(key, None);
        } else if let Some(record) = entry.positive_record
            && store.get_raw(&key)?.is_some()
        {
            final_writes.insert(key, Some(record));
        }
    }
    let operations = final_writes
        .iter()
        .map(|(key, record)| match record {
            Some(record) => store.set(key, record),
            None => store.delete(key),
        })
        .collect::<Vec<_>>();
    Ok(store.write_many(&operations)?)
}

/// [`apply_persist_delta`] specialized for index entries, whose key is
/// already the encoded index key in field 0. Same consolidate-then-batch
/// logic, but the durable key comes from wrapping the entry's logical key in
/// the storage prefix rather than re-encoding record fields.
fn apply_index_persist_delta(
    storage: &impl OrderedKvStorage,
    durable_storage: &DurableStorage,
    unique: bool,
    delta: &RecordDeltas,
) -> Result<(), IvmRuntimeError> {
    let store = RecordStore::new(storage, &durable_storage.column_family, &delta.descriptor);
    let mut pending = BTreeMap::<Vec<u8>, PendingPersistKey>::new();

    for record_delta in &delta.deltas {
        let record = record_delta.borrowed(&delta.descriptor);
        let logical_key = record
            .field_bytes_unchecked(0)
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let key = persisted_index_record_key(durable_storage, logical_key);

        if record_delta.weight > 0 {
            if unique {
                let current = if let Some(record) = pending
                    .get(&key)
                    .and_then(|entry| entry.positive_record.clone())
                {
                    Some(record)
                } else {
                    store.get_raw(&key)?
                };
                if current
                    .as_deref()
                    .is_some_and(|record| record != record_delta.raw())
                {
                    return Err(IvmRuntimeError::UniqueIndexViolation {
                        index: durable_storage_name(durable_storage),
                    });
                }
            }
            let entry = pending.entry(key).or_default();
            entry.weight += record_delta.weight;
            entry.positive_record = Some(record_delta.raw().to_vec());
        } else if record_delta.weight < 0 {
            if unique {
                let current = store.get_raw(&key)?;
                if current
                    .as_deref()
                    .is_some_and(|record| record != record_delta.raw())
                {
                    continue;
                }
            }
            pending.entry(key).or_default().weight += record_delta.weight;
        }
    }

    let mut final_writes = BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
    for (key, entry) in pending {
        if entry.weight > 0 {
            let record = entry
                .positive_record
                .ok_or(IvmRuntimeError::PersistRecordMismatch)?;
            final_writes.insert(key, Some(record));
        } else if entry.weight < 0 {
            final_writes.insert(key, None);
        } else if let Some(record) = entry.positive_record
            && store.get_raw(&key)?.is_some()
        {
            final_writes.insert(key, Some(record));
        }
    }
    let operations = final_writes
        .iter()
        .map(|(key, record)| match record {
            Some(record) => store.set(key, record),
            None => store.delete(key),
        })
        .collect::<Vec<_>>();
    Ok(store.write_many(&operations)?)
}

/// Builds the full storage key for one index entry: the node's key prefix, a
/// `7` bytes-type tag, then the NUL-escaped logical index key.
fn persisted_index_record_key(durable_storage: &DurableStorage, logical_key: &[u8]) -> Vec<u8> {
    let mut key = durable_storage.key_prefix.clone();
    key.push(7);
    encode_ordered_bytes(&mut key, logical_key);
    key
}

/// A human-readable `table.index` name derived from the key prefix, for
/// unique-violation error messages.
fn durable_storage_name(durable_storage: &DurableStorage) -> String {
    String::from_utf8_lossy(&durable_storage.key_prefix)
        .trim_end_matches('\0')
        .replace('\0', ".")
}

/// Builds the durable storage key(s) for one persisted record.
///
/// Usually one key (the prefix followed by the encoded key fields). An
/// array-valued key field fans out to one key per element, so a row can be
/// indexed under several keys. An empty key set means the row contributes no
/// key and is skipped.
fn persist_record_keys(
    descriptor: &RecordDescriptor,
    record: &[u8],
    key_fields: &[usize],
    durable_storage: &DurableStorage,
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    let mut keys = vec![durable_storage.key_prefix.clone()];
    let mut seen = HashSet::new();

    for field_idx in key_fields {
        let field = descriptor
            .fields()
            .get(*field_idx)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(*field_idx))?;
        let field_name = field
            .name
            .as_deref()
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
        let value = descriptor.get(record, field_name)?;
        let parts = arrangement_key_parts(value);

        if parts.is_empty() {
            return Ok(Vec::new());
        }

        let mut next_keys = Vec::with_capacity(keys.len() * parts.len());
        for key in &keys {
            for value in &parts {
                let mut next = key.clone();
                encode_key_part(&mut next, value)?;
                if seen.insert(next.clone()) {
                    next_keys.push(next);
                }
            }
        }
        keys = next_keys;
        seen.clear();
    }

    Ok(keys)
}

/// Splits a key-field value into the individual key parts it contributes: an
/// array yields one part per element (nested nullability preserved), a scalar
/// yields itself. This is what makes array-valued index keys fan out.
fn arrangement_key_parts(value: crate::records::Value) -> Vec<crate::records::Value> {
    match value {
        crate::records::Value::Array(values) => values,
        crate::records::Value::Nullable(Some(value)) => match *value {
            crate::records::Value::Array(values) => values
                .into_iter()
                .map(|value| crate::records::Value::Nullable(Some(Box::new(value))))
                .collect(),
            value => vec![crate::records::Value::Nullable(Some(Box::new(value)))],
        },
        value => vec![value],
    }
}
