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

use super::{
    IvmRuntimeError, RecordDeltas, encode_key_part, encode_ordered_bytes, index_record_descriptor,
};

#[derive(Default)]
struct UniqueRecordDeltas {
    first: Option<(Bytes, i64)>,
    additional: Option<HashMap<Bytes, i64>>,
}

impl UniqueRecordDeltas {
    fn add(&mut self, record: &Bytes, weight: i64) {
        match &mut self.first {
            Some((first_record, first_weight)) if first_record == record => {
                *first_weight += weight;
            }
            Some(_) => {
                let additional = self.additional.get_or_insert_default();
                *additional.entry(record.clone()).or_default() += weight;
            }
            None => self.first = Some((record.clone(), weight)),
        }
    }

    fn remove(&mut self, record: &[u8]) -> Option<i64> {
        if self
            .first
            .as_ref()
            .is_some_and(|(first_record, _)| first_record.as_ref() == record)
        {
            return self.first.take().map(|(_, weight)| weight);
        }
        self.additional.as_mut()?.remove(record)
    }

    fn into_iter(self) -> impl Iterator<Item = (Bytes, i64)> {
        self.first
            .into_iter()
            .chain(self.additional.into_iter().flatten())
    }
}

#[derive(Default)]
struct PendingPersistKey {
    weight: i64,
    positive_record: Option<Vec<u8>>,
    unique_record_deltas: UniqueRecordDeltas,
}

pub(super) async fn apply_persist_delta(
    storage: &dyn OrderedKvStorage,
    durable_storage: &DurableStorage,
    key_fields: &[usize],
    unique: bool,
    delta: &RecordDeltas,
) -> Result<(), IvmRuntimeError> {
    if key_fields == [0] && delta.descriptor == index_record_descriptor() {
        return apply_index_persist_delta(storage, durable_storage, unique, delta).await;
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

    if unique {
        return apply_unique_pending(&store, durable_storage, pending).await;
    }

    let mut operations = Vec::with_capacity(pending.len());
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

async fn apply_index_persist_delta(
    storage: &dyn OrderedKvStorage,
    durable_storage: &DurableStorage,
    unique: bool,
    delta: &RecordDeltas,
) -> Result<(), IvmRuntimeError> {
    let store = RecordStore::new(storage, &durable_storage.column_family, &delta.descriptor);
    let mut pending = BTreeMap::<Vec<u8>, PendingPersistKey>::new();

    for record_delta in &delta.deltas {
        let record = record_delta.borrowed(&delta.descriptor);
        let logical_key = record
            .get_bytes(0)
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let key = persisted_index_record_key(durable_storage, logical_key);
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

    if unique {
        return apply_unique_pending(&store, durable_storage, pending).await;
    }

    // `pending` is already ordered. Consume it directly into owned storage
    // operations instead of building a second BTreeMap and then asking
    // RecordStore to clone every key and record once more.
    let mut operations = Vec::with_capacity(pending.len());
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

fn add_pending_delta(entry: &mut PendingPersistKey, record: &Bytes, weight: i64, unique: bool) {
    if weight == 0 {
        return;
    }
    entry.weight += weight;
    if unique {
        entry.unique_record_deltas.add(record, weight);
    } else if weight > 0 {
        entry.positive_record = Some(record.to_vec());
    }
}

async fn apply_unique_pending<S>(
    store: &RecordStore<'_, S>,
    durable_storage: &DurableStorage,
    pending: BTreeMap<Vec<u8>, PendingPersistKey>,
) -> Result<(), IvmRuntimeError>
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
    Ok(store.write_many(operations).await?)
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

    for (record, delta) in record_deltas.into_iter() {
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

fn persisted_index_record_key(durable_storage: &DurableStorage, logical_key: &[u8]) -> Vec<u8> {
    let mut key = durable_storage.key_prefix.clone();
    key.push(7);
    encode_ordered_bytes(&mut key, logical_key);
    key
}

fn durable_storage_name(durable_storage: &DurableStorage) -> String {
    String::from_utf8_lossy(&durable_storage.key_prefix)
        .trim_end_matches('\0')
        .replace('\0', ".")
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
