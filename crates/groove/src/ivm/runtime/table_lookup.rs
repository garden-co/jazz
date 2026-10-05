//! A live primary-key semijoin with demand-driven target hydration.
//!
//! The target is never scanned in full. A new input key loads its current row;
//! subsequent target deltas update only admitted keys. Missing rows are tracked
//! too, so a later insert is observable. State changes are staged until all
//! point reads complete, including when a cold read suspends the evaluator.

use super::evaluation_session::StorageRequestKey;
use super::*;

#[derive(Clone, Debug)]
struct Entry {
    references: i64,
    row: Option<Bytes>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct TableLookupState {
    base: Rc<HashMap<Vec<u8>, Entry>>,
    overlay: HashMap<Vec<u8>, Option<Entry>>,
}

impl TableLookupState {
    fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.overlay
            .get(key)
            .map(Option::as_ref)
            .unwrap_or_else(|| self.base.get(key))
    }

    pub(super) fn commit_overlay(&mut self) {
        if self.overlay.is_empty() {
            return;
        }
        let base = Rc::make_mut(&mut self.base);
        for (key, entry) in self.overlay.drain() {
            if let Some(entry) = entry {
                base.insert(key, entry);
            } else {
                base.remove(&key);
            }
        }
    }
}

pub(super) fn decode_target(
    lookup: &TableLookupOp,
    output: &RecordDescriptor,
    key: &[u8],
    stored: &[u8],
) -> Result<Bytes, IvmRuntimeError> {
    // Homogeneous tables may retain any schema-version tag; their descriptor
    // is shared across tags, as with the ordinary table-source decoder.
    let (_, record) = records::split_variant_record(stored)?;
    if primary_key_value_bytes(output, record, &lookup.target_key_fields)? != key {
        return Err(IvmRuntimeError::GraphOutputMismatch);
    }
    Ok(Bytes::copy_from_slice(record))
}

impl TickEvaluator<'_> {
    pub(super) fn update_table_lookup(
        &mut self,
        node: NodeId,
        lookup: &TableLookupOp,
        output: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let operator_key = self.operator_key(node);
        let hydrate = self.context.eval_mode == EvalMode::Hydrate;
        let state = if hydrate {
            None
        } else {
            match self.operator_states.get(&operator_key) {
                Some(OperatorState::TableLookup(state)) => Some(state.value()),
                _ => return Err(IvmRuntimeError::NodeStateMissing(node)),
            }
        };
        let mut changes = HashMap::<Vec<u8>, i64>::default();
        for delta in &input.deltas {
            let key = primary_key_value_bytes(&input.descriptor, delta.raw(), &lookup.key_fields)?;
            *changes.entry(key).or_default() += delta.weight;
        }
        // Amortize the request bookkeeping for large batches against an empty
        // target (the usual deletion table in a new app). This reads at most
        // one row, regardless of unrelated target history. Small lookups keep
        // their single round of point requests. Missing keys are still retained
        // below, so later target inserts remain ordinary live updates.
        let target_empty = hydrate
            && changes.len() >= 64
            && self
                .evaluation_inputs
                .as_deref_mut()
                .ok_or(IvmRuntimeError::StorageUnavailable)?
                .rows(StorageRequestKey::ScanPrefixLimit {
                    family: lookup.table.clone(),
                    prefix: Vec::new(),
                    max_items: 1,
                    reversed: false,
                })?
                .is_empty();
        let mut targets = HashMap::<Vec<u8>, Option<Bytes>>::default();
        if !hydrate {
            for delta in self
                .table_deltas
                .iter()
                .filter(|delta| delta.table == lookup.table)
            {
                if !delta.descriptor.registry_compatible_with(&output) {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                for row in &delta.deltas {
                    let key =
                        primary_key_value_bytes(&output, row.raw(), &lookup.target_key_fields)?;
                    if state.and_then(|state| state.get(&key)).is_none()
                        && !changes.contains_key(&key)
                    {
                        continue;
                    }
                    changes.entry(key.clone()).or_default();
                    // A replacement has a retraction and an insertion; the
                    // insertion wins regardless of their order in the batch.
                    let target = targets.entry(key).or_default();
                    if row.weight > 0 {
                        *target = Some(row.record.clone());
                    }
                }
            }
        }
        let mut updates = Vec::with_capacity(changes.len());
        let mut blocked = false;
        for (key, delta) in changes {
            let before = state.and_then(|state| state.get(&key));
            let references = before
                .map_or(0, |entry| entry.references)
                .checked_add(delta)
                .ok_or(IvmRuntimeError::AggregateOverflow)?;
            if references < 0 {
                return Err(IvmRuntimeError::UnsupportedOperator);
            }
            let row = if references == 0 || target_empty {
                None
            } else if let Some(target) = targets.get(&key) {
                target.clone()
            } else if let Some(before) = before {
                before.row.clone()
            } else {
                let inputs = self
                    .evaluation_inputs
                    .as_deref_mut()
                    .ok_or(IvmRuntimeError::StorageUnavailable)?;
                match inputs.value(StorageRequestKey::Get {
                    family: lookup.table.clone(),
                    key: key.clone(),
                }) {
                    Ok(Some(stored)) => Some(decode_target(lookup, &output, &key, stored)?),
                    Ok(None) => None,
                    Err(IvmRuntimeError::EvaluationBlocked) => {
                        blocked = true;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            };
            updates.push((key, references, row));
        }
        // Register every missing key before yielding, without mutating state.
        if blocked {
            return Err(IvmRuntimeError::EvaluationBlocked);
        }
        let mut operator = if hydrate {
            OperatorState::TableLookup(AsOf::default())
        } else {
            self.operator_states
                .remove(&operator_key)
                .ok_or(IvmRuntimeError::NodeStateMissing(node))?
        };
        let OperatorState::TableLookup(state) = &mut operator else {
            unreachable!()
        };
        let mut deltas = Vec::new();
        for (key, references, row) in updates {
            let before = state.value().get(&key).and_then(|entry| entry.row.clone());
            if before != row {
                if let Some(record) = before {
                    deltas.push(RecordDelta { record, weight: -1 });
                }
                if let Some(record) = &row {
                    deltas.push(RecordDelta {
                        record: record.clone(),
                        weight: 1,
                    });
                }
            }
            state
                .value_mut()
                .overlay
                .insert(key, (references > 0).then_some(Entry { references, row }));
        }
        state.mark_forward_as_of(SubTick {
            tick: self.current_tick,
            sub_tick: if operator_key.scope == ScopeId::root() {
                0
            } else {
                self.context.sub_tick
            },
        })?;
        self.operator_states.insert(operator_key, operator);
        Ok(RecordDeltas {
            descriptor: output,
            deltas,
        })
    }
}
