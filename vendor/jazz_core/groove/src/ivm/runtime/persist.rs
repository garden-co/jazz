//! Durable persist operator writes.
//!
//! This module owns the write-through step for `Persist` nodes: translating
//! weighted record deltas into ordered storage keys, consolidating same-tick
//! updates by durable key, and enforcing unique-index conflicts. It does not
//! decide when persist nodes run; the runtime tick loop calls into this module
//! after evaluating the input node. Base table commits and schema-aware row
//! encoding live above in [`crate::db`] and [`crate::records`].

use std::collections::{HashMap, HashSet};

use crate::ivm::DurableStorage;
use crate::records::RecordDescriptor;
use crate::storage::{OrderedKvStorage, RecordStore};

use super::{IvmRuntimeError, RecordDeltas, encode_key_part};

#[derive(Default)]
struct PendingPersistKey {
    weight: i64,
    positive_record: Option<Vec<u8>>,
}

pub(super) fn apply_persist_delta(
    storage: &impl OrderedKvStorage,
    durable_storage: &DurableStorage,
    key_fields: &[usize],
    unique: bool,
    delta: &RecordDeltas,
) -> Result<(), IvmRuntimeError> {
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

    let mut final_writes = Vec::<(Vec<u8>, Option<Vec<u8>>)>::new();
    for (key, entry) in pending {
        if entry.weight > 0 {
            let record = entry
                .positive_record
                .ok_or(IvmRuntimeError::PersistRecordMismatch)?;
            final_writes.push((key, Some(record)));
        } else if entry.weight < 0 {
            final_writes.push((key, None));
        } else if let Some(record) = entry.positive_record
            && store.get_raw(&key)?.is_some()
        {
            final_writes.push((key, Some(record)));
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
