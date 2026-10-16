//! Keyed lookup: a table source probed by the keys of its only input
//! ([`GraphBuilder::lookup`](crate::ivm::GraphBuilder::lookup)).
//!
//! Hydration reads one storage prefix per distinct live key. A tick probes
//! only keys that become live, retracts the rows of keys that stop being
//! live, and applies the table's own deltas to keys that stay live. Nothing
//! here is proportional to the table's size.

use std::rc::Rc;

use super::*;

/// Maintained state of one lookup node.
///
/// Evaluations copy the operator states they may touch, so the contents sit
/// behind an `Rc` and are cloned only when a tick actually changes them.
#[derive(Clone, Debug, Default)]
pub(in crate::ivm::runtime) struct LookupState(Rc<LookupStateContents>);

#[derive(Clone, Debug, Default)]
struct LookupStateContents {
    /// Multiplicity of each key in the key relation. A key is live while its
    /// count is positive; zero counts are not kept.
    keys: HashMap<Vec<u8>, i64>,
    /// The matched rows of each live key, by encoded primary key.
    rows: HashMap<Vec<u8>, BTreeMap<Vec<u8>, Bytes>>,
}

impl LookupState {
    /// Live keys, held rows, and the rows' encoded bytes.
    pub(in crate::ivm::runtime) fn footprint(&self) -> (usize, usize, usize) {
        let rows = self.0.rows.values().map(BTreeMap::len).sum();
        let bytes = self
            .0
            .rows
            .values()
            .flat_map(|rows| rows.iter())
            .map(|(key, row)| key.len() + row.len())
            .sum();
        (self.0.keys.len(), rows, bytes)
    }
}

impl LookupStateContents {
    fn live(&self, key: &[u8]) -> bool {
        self.keys.get(key).is_some_and(|count| *count > 0)
    }
}

/// The probed rows of each requested key, by encoded primary key.
type ProbedRows = HashMap<Vec<u8>, BTreeMap<Vec<u8>, Bytes>>;

/// Encode the given fields of a record as storage key parts.
fn encoded_key(
    descriptor: &RecordDescriptor,
    record: &[u8],
    fields: &[usize],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for &field in fields {
        encode_record_field_key_part(&mut key, descriptor, record, field)?;
    }
    Ok(key)
}

/// Where a lookup's probe key sits inside the table's primary key.
struct LookupLayout {
    /// Storage-key prefix of the static scan (empty without one).
    prefix: Vec<u8>,
    /// Output field indices of the whole primary key.
    primary_key: Vec<usize>,
    /// Output field indices of the primary-key columns the keys name.
    key_columns: Vec<usize>,
}

impl TickEvaluator<'_> {
    fn lookup_layout(
        &self,
        lookup: &LookupOp,
        output: &RecordDescriptor,
    ) -> Result<LookupLayout, IvmRuntimeError> {
        let table = self
            .schema
            .table(&lookup.source.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(lookup.source.table.clone()))?;
        let primary_key = primary_key_field_indices(table, output)?;
        let prefix_len = match &lookup.source.scan {
            None => 0,
            Some(StaticScanSpec::Prefix(values) | StaticScanSpec::Point(values)) => values.len(),
            Some(_) => {
                return Err(IvmRuntimeError::UnsupportedLookup(
                    "a lookup's static scan must be a point or prefix",
                ));
            }
        };
        let prefix = match lookup.source.scan.as_ref().map(scan_bounds).transpose()? {
            None => Vec::new(),
            Some(StaticScanBounds::Prefix(prefix)) => prefix,
            Some(StaticScanBounds::Range { .. }) => {
                unreachable!("point and prefix scans have prefix bounds")
            }
        };
        let key_columns = primary_key
            .get(prefix_len..prefix_len + lookup.key_fields.len())
            .ok_or(IvmRuntimeError::UnsupportedLookup(
                "a lookup's prefix and keys exceed the table's primary key",
            ))?
            .to_vec();
        Ok(LookupLayout {
            prefix,
            primary_key,
            key_columns,
        })
    }

    /// Read every row of `keys` from storage through the evaluation
    /// session. Returns `EvaluationBlocked` (and registers every missing
    /// probe at once) until all of them are loaded.
    fn probe_lookup_keys<'k>(
        &mut self,
        lookup: &LookupOp,
        output: &RecordDescriptor,
        layout: &LookupLayout,
        keys: impl IntoIterator<Item = &'k Vec<u8>> + Clone,
    ) -> Result<ProbedRows, IvmRuntimeError> {
        let schema = self.schema;
        let variant_projections = self.variant_projections;
        let inputs =
            self.evaluation_inputs
                .as_deref_mut()
                .ok_or(IvmRuntimeError::UnsupportedLookup(
                    "a lookup reads storage and cannot run in a recursive step or durable node",
                ))?;
        let request = |key: &[u8]| {
            let mut prefix = layout.prefix.clone();
            prefix.extend_from_slice(key);
            super::super::evaluation_session::StorageRequestKey::ScanPrefix {
                family: lookup.source.table.clone(),
                prefix,
            }
        };
        let mut blocked = false;
        for key in keys.clone() {
            match inputs.rows(request(key)) {
                Ok(_) => {}
                Err(IvmRuntimeError::EvaluationBlocked) => blocked = true,
                Err(error) => return Err(error),
            }
        }
        if blocked {
            return Err(IvmRuntimeError::EvaluationBlocked);
        }
        let table = schema
            .table(&lookup.source.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(lookup.source.table.clone()))?;
        // The probe prefix already bounds the rows; project them like the
        // table source would.
        let source = TableSourceOp {
            scan: None,
            ..lookup.source.clone()
        };
        let mut probed = ProbedRows::default();
        for key in keys {
            let stored = inputs.rows(request(key))?;
            let grouped =
                NodeState::group_source_rows(table, stored.iter().map(|(_, row)| row.as_slice()))?;
            let projected = NodeState::update_table_source(
                &source,
                schema,
                variant_projections,
                output,
                &grouped,
            )?;
            let rows = probed.entry(key.clone()).or_default();
            for delta in projected.deltas {
                let primary_key = encoded_key(output, delta.raw(), &layout.primary_key)?;
                rows.insert(primary_key, delta.record);
            }
        }
        Ok(probed)
    }

    pub(super) fn update_lookup(
        &mut self,
        node: NodeId,
        lookup: &LookupOp,
        output_desc: RecordDescriptor,
        input: &Arc<RecordDeltas>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if self.context.scope != ScopeId::root() {
            return Err(IvmRuntimeError::UnsupportedLookup(
                "a lookup cannot run in a recursive step",
            ));
        }
        let layout = self.lookup_layout(lookup, &output_desc)?;
        let input = self.materialize_indirect_field_indices(input, &lookup.key_fields)?;
        let mut key_deltas = BTreeMap::<Vec<u8>, i64>::new();
        for delta in &input.deltas {
            let key = encoded_key(&input.descriptor, delta.raw(), &lookup.key_fields)?;
            *key_deltas.entry(key).or_default() += delta.weight;
        }

        let operator_key = self.operator_key(node);
        let sub_tick = SubTick {
            tick: self.current_tick,
            sub_tick: 0,
        };
        let replace = self.context.arrangement_update_mode == ArrangementUpdateMode::Replace;
        let empty = LookupStateContents::default();
        let state = match self.operator_states.get(&operator_key) {
            None => None,
            Some(OperatorState::Lookup(state)) => Some(state),
            Some(_) => return Err(IvmRuntimeError::NodeStateOperatorMismatch(node)),
        };
        if let Some(state) = state {
            if state.as_of().is_some_and(|current| current > sub_tick) {
                return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                    current: format!("{:?}", state.as_of().expect("checked above")),
                    next: format!("{sub_tick:?}"),
                });
            }
            if !replace && state.as_of() == Some(sub_tick) {
                return Ok(RecordDeltas::empty(output_desc));
            }
        }
        // Hydration supplies the complete key relation; a rebuild starts
        // from nothing and reads current storage for every live key.
        let before = match state {
            Some(state) if !replace => state.value().0.as_ref(),
            _ => &empty,
        };
        let mut entering = Vec::new();
        let mut leaving = Vec::new();
        for (key, delta) in &key_deltas {
            let old = before.keys.get(key).copied().unwrap_or_default();
            let new = old + delta;
            match (old > 0, new > 0) {
                (false, true) => entering.push(key.clone()),
                (true, false) => leaving.push(key.clone()),
                _ => {}
            }
        }
        // Read every entering key before touching state: a blocked probe
        // re-polls this node with the same inputs.
        let probed = self.probe_lookup_keys(lookup, &output_desc, &layout, entering.iter())?;
        // The table's own changes, under the static prefix, for keys that
        // stay live. Entering keys were read from storage, which already
        // holds this tick's writes; leaving keys are retracted whole.
        let table_rows = if replace {
            RecordDeltas::empty(output_desc)
        } else {
            NodeState::update_table_source(
                &lookup.source,
                self.schema,
                self.variant_projections,
                &output_desc,
                self.table_deltas,
            )?
        };

        let mut operator = match self.operator_states.remove(&operator_key) {
            Some(operator) if !replace => operator,
            _ => OperatorState::Lookup(AsOf::default()),
        };
        let OperatorState::Lookup(state) = &mut operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        // Changes to rows of keys that stay live: the table's changes under
        // the static prefix, by primary key.
        let mut changes = Vec::new();
        {
            let current = state.value().0.as_ref();
            for delta in &table_rows.deltas {
                let key = encoded_key(&output_desc, delta.raw(), &layout.key_columns)?;
                if !current.live(&key) || leaving.contains(&key) {
                    continue;
                }
                let primary_key = encoded_key(&output_desc, delta.raw(), &layout.primary_key)?;
                changes.push((delta.weight, key, primary_key, delta.record.clone()));
            }
        }
        let mut output = Vec::new();
        if !(leaving.is_empty() && changes.is_empty() && probed.is_empty() && key_deltas.is_empty())
        {
            let contents = Rc::make_mut(&mut state.value_mut().0);
            for key in &leaving {
                if let Some(rows) = contents.rows.remove(key) {
                    output.extend(
                        rows.into_values()
                            .map(|record| RecordDelta { record, weight: -1 }),
                    );
                }
            }
            // Apply table changes per primary key, retractions first. A row
            // is removed only if it is the stored row and inserted only if
            // its key is absent, so a probe that already saw a later write
            // converges instead of double counting.
            changes.sort_by_key(|(weight, ..)| *weight > 0);
            for (weight, key, primary_key, record) in changes {
                let rows = contents.rows.entry(key).or_default();
                if weight < 0 {
                    if rows.get(&primary_key) == Some(&record) {
                        rows.remove(&primary_key);
                        output.push(RecordDelta { record, weight: -1 });
                    }
                } else if weight > 0 && !rows.contains_key(&primary_key) {
                    rows.insert(primary_key, record.clone());
                    output.push(RecordDelta { record, weight: 1 });
                }
            }
            for (key, rows) in probed {
                output.extend(
                    rows.values()
                        .cloned()
                        .map(|record| RecordDelta { record, weight: 1 }),
                );
                contents.rows.insert(key, rows);
            }
            for (key, delta) in key_deltas {
                let count = contents.keys.entry(key.clone()).or_default();
                *count += delta;
                if *count == 0 {
                    contents.keys.remove(&key);
                }
                if !contents.live(&key) {
                    contents.rows.remove(&key);
                }
            }
            contents.rows.retain(|_, rows| !rows.is_empty());
        }
        state.mark_forward_as_of(sub_tick)?;
        self.operator_states.insert(operator_key, operator);
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: output,
        })
    }
}
