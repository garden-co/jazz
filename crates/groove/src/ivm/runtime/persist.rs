//! Durable persist operator writes.
//!
//! This module owns the write-through step for `Persist` nodes: translating
//! weighted record deltas into ordered storage keys, consolidating same-tick
//! updates by durable key, and enforcing unique-index conflicts. It does not
//! decide when persist nodes run; the runtime tick loop calls into this module
//! after evaluating the input node. Base table commits and schema-aware row
//! encoding live above in [`crate::db`] and [`crate::records`].

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::ivm::DurableStorage;
use crate::records::RecordDescriptor;
use crate::storage::{OrderedKvStorage, OwnedWriteOperation, RecordStore};

use super::{IvmRuntimeError, RecordDeltas, encode_key_part, index_record_descriptor};

#[derive(Default)]
struct PendingPersistKey {
    weight: i64,
    positive_record: Option<Vec<u8>>,
    unique_record_deltas: HashMap<Bytes, i64>,
}

pub(super) async fn apply_persist_delta(
    storage: &dyn OrderedKvStorage,
    durable_storage: &DurableStorage,
    key_fields: &[usize],
    unique: bool,
    delta: &RecordDeltas,
) -> Result<(), IvmRuntimeError> {
    apply_persist_delta_with(
        storage,
        durable_storage,
        key_fields,
        unique,
        delta,
        Vec::new(),
    )
    .await
}

/// Like [`apply_persist_delta`], and submit `leading` in the same atomic
/// storage write (used to make an index's id registration durable together
/// with its first entries).
pub(super) async fn apply_persist_delta_with(
    storage: &dyn OrderedKvStorage,
    durable_storage: &DurableStorage,
    key_fields: &[usize],
    unique: bool,
    delta: &RecordDeltas,
    leading: Vec<OwnedWriteOperation>,
) -> Result<(), IvmRuntimeError> {
    if key_fields == [0] && delta.descriptor == index_record_descriptor() {
        return apply_index_persist_delta(storage, durable_storage, unique, delta, leading).await;
    }

    let store = RecordStore::new(storage, &durable_storage.column_family, &delta.descriptor);
    // Multiple deltas in one tick may touch the same durable key. Consolidate
    // by persisted key before writing: an update whose indexed key is
    // unchanged appears as `-old, +new` for the same key, and the final durable
    // entry must remain present regardless of delta order.
    let mut pending = BTreeMap::<Vec<u8>, PendingPersistKey>::new();
    for record_delta in &delta.deltas {
        let keys = persist_record_keys(
            &delta.descriptor,
            record_delta.raw(),
            key_fields,
            durable_storage,
        )?;

        for key in keys {
            if record_delta.weight == 0 {
                continue;
            }
            add_pending_delta(
                pending.entry(key).or_default(),
                &record_delta.record,
                record_delta.weight,
                unique,
            );
        }
    }

    let mut operations = leading;
    if unique {
        operations.extend(unique_pending_operations(&store, durable_storage, pending).await?);
        return Ok(store.write_many(operations).await?);
    }

    operations.reserve(pending.len());
    for (key, entry) in pending {
        if entry.weight > 0 {
            let record = entry
                .positive_record
                .ok_or(IvmRuntimeError::PersistRecordMismatch)?;
            operations.push(OwnedWriteOperation::Set {
                cf: durable_storage.column_family.clone(),
                key,
                value: record,
            });
        } else if entry.weight < 0 {
            operations.push(OwnedWriteOperation::Delete {
                cf: durable_storage.column_family.clone(),
                key,
            });
        } else if let Some(record) = entry.positive_record
            && store.get_raw(&key).await?.is_some()
        {
            operations.push(OwnedWriteOperation::Set {
                cf: durable_storage.column_family.clone(),
                key,
                value: record,
            });
        }
    }
    Ok(store.write_many(operations).await?)
}

/// Persist `IndexBy` records as compact durable index entries.
///
/// The storage key is the index's numeric id prefix followed directly by the
/// record's logical `key` bytes (already a prefix-free, order-preserving
/// concatenation of typed parts, so no second escaping). The storage value is
/// the record's raw `value` bytes: empty for non-unique indexes, and the
/// primary-key columns missing from the key for unique ones. The logical key
/// is never repeated in the value.
async fn apply_index_persist_delta(
    storage: &dyn OrderedKvStorage,
    durable_storage: &DurableStorage,
    unique: bool,
    delta: &RecordDeltas,
    leading: Vec<OwnedWriteOperation>,
) -> Result<(), IvmRuntimeError> {
    let store = RecordStore::new(storage, &durable_storage.column_family, &delta.descriptor);
    let mut pending = BTreeMap::<Vec<u8>, PendingPersistKey>::new();

    for record_delta in &delta.deltas {
        if record_delta.weight == 0 {
            continue;
        }
        let record = record_delta.borrowed(&delta.descriptor);
        let logical_key = record
            .get_bytes(0)
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let value = record
            .get_bytes(1)
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let mut key = durable_storage.key_prefix.clone();
        key.extend_from_slice(logical_key);
        add_pending_delta(
            pending.entry(key).or_default(),
            &Bytes::copy_from_slice(value),
            record_delta.weight,
            unique,
        );
    }

    let mut operations = leading;
    if unique {
        operations.extend(unique_pending_operations(&store, durable_storage, pending).await?);
        return Ok(store.write_many(operations).await?);
    }

    // `pending` is already ordered. Consume it directly into owned storage
    // operations instead of building a second BTreeMap and then asking
    // RecordStore to clone every key and value once more.
    operations.reserve(pending.len());
    for (key, entry) in pending {
        if entry.weight > 0 {
            let value = entry
                .positive_record
                .ok_or(IvmRuntimeError::PersistRecordMismatch)?;
            operations.push(OwnedWriteOperation::Set {
                cf: durable_storage.column_family.clone(),
                key,
                value,
            });
        } else if entry.weight < 0 {
            operations.push(OwnedWriteOperation::Delete {
                cf: durable_storage.column_family.clone(),
                key,
            });
        } else if let Some(value) = entry.positive_record
            && store.get_raw(&key).await?.is_some()
        {
            operations.push(OwnedWriteOperation::Set {
                cf: durable_storage.column_family.clone(),
                key,
                value,
            });
        }
    }
    Ok(store.write_many(operations).await?)
}

/// Rebuild the in-memory `IndexBy` record of one durable index entry from
/// its storage key (minus the `prefix_len`-byte id prefix) and value.
pub(super) fn index_record_from_storage(
    prefix_len: usize,
    key: &[u8],
    value: Vec<u8>,
) -> Result<Bytes, IvmRuntimeError> {
    let logical_key = key.get(prefix_len..).ok_or_else(|| {
        IvmRuntimeError::InvalidPersistedIndex("index key shorter than its id prefix".to_owned())
    })?;
    Ok(index_record_descriptor()
        .create(&[
            crate::records::Value::Bytes(logical_key.to_vec()),
            crate::records::Value::Bytes(value),
        ])?
        .into())
}

fn add_pending_delta(entry: &mut PendingPersistKey, record: &Bytes, weight: i64, unique: bool) {
    if weight == 0 {
        return;
    }
    entry.weight += weight;
    if unique {
        if let Some(current_weight) = entry.unique_record_deltas.get_mut(record) {
            *current_weight += weight;
        } else {
            entry.unique_record_deltas.insert(record.clone(), weight);
        }
    } else if weight > 0 {
        entry.positive_record = Some(record.to_vec());
    }
}

async fn unique_pending_operations<S>(
    store: &RecordStore<'_, S>,
    durable_storage: &DurableStorage,
    pending: BTreeMap<Vec<u8>, PendingPersistKey>,
) -> Result<Vec<OwnedWriteOperation>, IvmRuntimeError>
where
    S: OrderedKvStorage + ?Sized,
{
    let mut operations = Vec::with_capacity(pending.len());
    for (key, entry) in pending {
        let record = resolve_unique_owner(store, durable_storage, &key, entry).await?;
        match record {
            Some(record) => operations.push(OwnedWriteOperation::Set {
                cf: durable_storage.column_family.clone(),
                key,
                value: record,
            }),
            None => operations.push(OwnedWriteOperation::Delete {
                cf: durable_storage.column_family.clone(),
                key,
            }),
        }
    }
    Ok(operations)
}

async fn resolve_unique_owner<S>(
    store: &RecordStore<'_, S>,
    durable_storage: &DurableStorage,
    key: &[u8],
    entry: PendingPersistKey,
) -> Result<Option<Vec<u8>>, IvmRuntimeError>
where
    S: OrderedKvStorage + ?Sized,
{
    let durable_owner = store.get_raw(key).await?;
    let mut record_deltas = entry.unique_record_deltas;
    let durable_owner_survives = durable_owner
        .as_ref()
        .is_some_and(|record| record_deltas.remove(record.as_slice()).unwrap_or_default() >= 0);
    let mut owner = durable_owner.filter(|_| durable_owner_survives);

    for (record, delta) in record_deltas {
        if delta <= 0 {
            continue;
        }
        if owner.is_some() {
            return Err(IvmRuntimeError::UniqueIndexViolation {
                index: durable_storage_name(durable_storage),
            });
        }
        owner = Some(record.to_vec());
    }
    Ok(owner)
}

fn durable_storage_name(durable_storage: &DurableStorage) -> String {
    durable_storage.name.clone()
}

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
        let field_idx = super::record_projection::resolve_field_name(descriptor, field_name)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field_name.to_owned()))?;
        let value = descriptor.bind(record).get_idx(field_idx)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::RecordDelta;
    use crate::storage::{TestStorage, TestStorageOperation};

    /// `alice` and `bob` submit conflicting positive index entries in both
    /// orders; pre-submit resolution rejects them without writing storage.
    #[futures_test::test]
    async fn unique_index_positive_conflict_is_pre_submit_in_both_orders() {
        for records in [
            vec![
                (b"same-key".to_vec(), b"record-a".to_vec()),
                (b"same-key".to_vec(), b"record-b".to_vec()),
            ],
            vec![
                (b"same-key".to_vec(), b"record-b".to_vec()),
                (b"same-key".to_vec(), b"record-a".to_vec()),
            ],
        ] {
            let (storage, control) = TestStorage::controlled(&["indices"]);
            let durable_storage = DurableStorage {
                column_family: "indices".to_owned(),
                key_prefix: super::super::durable_index_key_prefix(1),
                name: "albums.unique_albums_by_title".to_owned(),
            };
            let descriptor = index_record_descriptor();
            let delta = RecordDeltas {
                descriptor,
                deltas: records
                    .into_iter()
                    .map(|(key, value)| RecordDelta {
                        record: descriptor
                            .create(&[
                                crate::records::Value::Bytes(key),
                                crate::records::Value::Bytes(value),
                            ])
                            .unwrap()
                            .into(),
                        weight: 1,
                    })
                    .collect(),
            };
            let before = storage
                .prefix("indices".to_owned(), Vec::new())
                .await
                .unwrap();
            control.take_observed();

            let error =
                apply_index_persist_delta(&storage, &durable_storage, true, &delta, Vec::new())
                    .await
                    .unwrap_err();
            assert!(matches!(
                error,
                IvmRuntimeError::UniqueIndexViolation { index }
                    if index == "albums.unique_albums_by_title"
            ));
            assert!(
                !control
                    .take_observed()
                    .contains(&TestStorageOperation::WriteMany)
            );
            assert_eq!(
                storage
                    .prefix("indices".to_owned(), Vec::new())
                    .await
                    .unwrap(),
                before
            );
        }
    }
}
