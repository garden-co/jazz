//! Keyed lookup: a table source probed by the keys of its only input
//! ([`GraphBuilder::lookup`](crate::ivm::GraphBuilder::lookup)), through its
//! primary key or through a secondary index.
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

/// Where a lookup's probe key sits inside the probed key.
#[derive(Clone)]
struct LookupLayout {
    /// The static scan's key prefix (empty without one). For a primary-key
    /// lookup this is also the storage-key prefix; for an index lookup it is
    /// the logical index-key prefix inside the index's storage envelope.
    prefix: Vec<u8>,
    probe: LookupProbe,
}

#[derive(Clone)]
enum LookupProbe {
    Table {
        /// Output field indices of the whole primary key.
        primary_key: Vec<usize>,
        /// Output field indices of the primary-key columns the keys name.
        key_columns: Vec<usize>,
    },
    Index {
        /// `durable_index_key_prefix` of the probed index.
        storage_base: Vec<u8>,
        /// Per key field, how many nullable envelopes its index column adds
        /// around the key's own type.
        envelopes: Vec<usize>,
    },
}

/// A probed row: the key it was found under, its encoded row identity and
/// its output record.
type KeyedRow = (Vec<u8>, Vec<u8>, Bytes);

impl TickEvaluator<'_> {
    fn lookup_layout(
        &self,
        lookup: &LookupOp,
        output: &RecordDescriptor,
    ) -> Result<LookupLayout, IvmRuntimeError> {
        let scan = match &lookup.index {
            Some(index) => index.scan.as_ref(),
            None => lookup.source.scan.as_ref(),
        };
        let prefix_len = match scan {
            None => 0,
            Some(StaticScanSpec::Prefix(values) | StaticScanSpec::Point(values)) => values.len(),
            Some(_) => {
                return Err(IvmRuntimeError::UnsupportedLookup(
                    "a lookup's static scan must be a point or prefix",
                ));
            }
        };
        let prefix = match scan.map(scan_bounds).transpose()? {
            None => Vec::new(),
            Some(StaticScanBounds::Prefix(prefix)) => prefix,
            Some(StaticScanBounds::Range { .. }) => {
                unreachable!("point and prefix scans have prefix bounds")
            }
        };
        if let Some(index) = &lookup.index {
            let columns = index
                .key_fields
                .get(prefix_len..prefix_len + lookup.key_fields.len())
                .ok_or(IvmRuntimeError::UnsupportedLookup(
                    "a lookup's prefix and keys exceed the index key",
                ))?;
            let envelopes = lookup
                .key_fields
                .iter()
                .zip(columns)
                .map(|(key, column)| {
                    index_lookup_key_envelopes(
                        &lookup.key_descriptor.fields()[*key].value_type,
                        &index.input_descriptor.fields()[*column].value_type,
                    )
                    .ok_or(IvmRuntimeError::UnsupportedLookup(
                        "an index lookup key field must have its index column's type or that type's non-null type",
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(LookupLayout {
                prefix,
                probe: LookupProbe::Index {
                    storage_base: durable_index_key_prefix(index.durable_ids.index),
                    envelopes,
                },
            });
        }
        let table = self
            .schema
            .table(&lookup.source.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(lookup.source.table.clone()))?;
        let primary_key = primary_key_field_indices(table, output)?;
        let key_columns = primary_key
            .get(prefix_len..prefix_len + lookup.key_fields.len())
            .ok_or(IvmRuntimeError::UnsupportedLookup(
                "a lookup's prefix and keys exceed the table's primary key",
            ))?
            .to_vec();
        Ok(LookupLayout {
            prefix,
            probe: LookupProbe::Table {
                primary_key,
                key_columns,
            },
        })
    }

    /// The probe key of one key record: the storage-key encoding of its key
    /// fields as the probed key columns hold them.
    fn lookup_probe_key(
        lookup: &LookupOp,
        layout: &LookupLayout,
        descriptor: &RecordDescriptor,
        record: &[u8],
    ) -> Result<Vec<u8>, IvmRuntimeError> {
        match &layout.probe {
            LookupProbe::Table { .. } => encoded_key(descriptor, record, &lookup.key_fields),
            LookupProbe::Index { envelopes, .. } => {
                // A nullable index column stores a present value as the
                // nullable tag followed by the value's own encoding.
                let mut key = Vec::new();
                for (&field, &envelopes) in lookup.key_fields.iter().zip(envelopes) {
                    key.extend(std::iter::repeat_n(9, envelopes));
                    encode_record_field_key_part(&mut key, descriptor, record, field)?;
                }
                Ok(key)
            }
        }
    }

    /// Key, identity and output record of each of the probed table's rows
    /// that lies under the lookup's static prefix.
    fn lookup_keyed_rows(
        &self,
        lookup: &LookupOp,
        layout: &LookupLayout,
        output: &RecordDescriptor,
        table_deltas: &[TableDelta],
    ) -> Result<Vec<(i64, KeyedRow)>, IvmRuntimeError> {
        match (&lookup.index, &layout.probe) {
            (
                None,
                LookupProbe::Table {
                    primary_key,
                    key_columns,
                },
            ) => {
                let rows = NodeState::update_table_source(
                    &lookup.source,
                    self.schema,
                    self.variant_projections,
                    output,
                    table_deltas,
                )?;
                rows.deltas
                    .into_iter()
                    .map(|delta| {
                        let key = encoded_key(output, delta.raw(), key_columns)?;
                        let identity = encoded_key(output, delta.raw(), primary_key)?;
                        Ok((delta.weight, (key, identity, delta.record)))
                    })
                    .collect()
            }
            (Some(index), LookupProbe::Index { envelopes, .. }) => {
                let key_len = layout_prefix_len(index) + envelopes.len();
                let index_input = TableSourceOp {
                    table: index.table.clone(),
                    scan: None,
                    variant_projection: index.variant_projection.clone(),
                };
                let mut rows = Vec::new();
                // Project each row on its own: a row whose variant one
                // projection omits must not shift the other's alignment.
                for table_delta in table_deltas
                    .iter()
                    .filter(|delta| delta.table == index.table)
                {
                    for record_delta in &table_delta.deltas {
                        let one = [TableDelta {
                            table: table_delta.table.clone(),
                            variant_tag: table_delta.variant_tag,
                            descriptor: table_delta.descriptor,
                            deltas: vec![record_delta.clone()],
                        }];
                        let indexed = NodeState::update_table_source(
                            &index_input,
                            self.schema,
                            self.variant_projections,
                            &index.input_descriptor,
                            &one,
                        )?;
                        let Some(indexed) = indexed.deltas.first() else {
                            continue;
                        };
                        let index_key = encoded_key(
                            &index.input_descriptor,
                            indexed.raw(),
                            &index.key_fields[..key_len],
                        )?;
                        let Some(key) = index_key.strip_prefix(layout.prefix.as_slice()) else {
                            continue;
                        };
                        let identity = encoded_key(
                            &index.input_descriptor,
                            indexed.raw(),
                            &index.value_fields,
                        )?;
                        let projected = NodeState::update_table_source(
                            &lookup.source,
                            self.schema,
                            self.variant_projections,
                            output,
                            &one,
                        )?;
                        for delta in projected.deltas {
                            rows.push((
                                delta.weight,
                                (key.to_vec(), identity.clone(), delta.record),
                            ));
                        }
                    }
                }
                Ok(rows)
            }
            _ => unreachable!("a lookup's layout follows its source"),
        }
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
        let request = |key: &[u8]| {
            let mut prefix = layout.prefix.clone();
            prefix.extend_from_slice(key);
            match (&lookup.index, &layout.probe) {
                (Some(index), LookupProbe::Index { storage_base, .. }) => {
                    // The same storage key `persisted_index_scan_bounds` builds
                    // for a static index prefix: the logical key follows the
                    // index's durable id directly.
                    let mut storage_key = storage_base.clone();
                    storage_key.extend_from_slice(&prefix);
                    super::super::evaluation_session::StorageRequestKey::IndexedRowsPrefix {
                        table: index.table.clone(),
                        index: index.index.clone(),
                        prefix: storage_key,
                    }
                }
                _ => super::super::evaluation_session::StorageRequestKey::ScanPrefix {
                    family: lookup.source.table.clone(),
                    prefix,
                },
            }
        };
        let inputs =
            self.evaluation_inputs
                .as_deref_mut()
                .ok_or(IvmRuntimeError::UnsupportedLookup(
                    "a lookup reads storage and cannot run in a recursive step or durable node",
                ))?;
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
        let mut loaded = Vec::new();
        for key in keys {
            let inputs = self
                .evaluation_inputs
                .as_deref_mut()
                .expect("checked above");
            let stored = inputs.rows(request(key))?;
            let grouped =
                NodeState::group_source_rows(table, stored.iter().map(|(_, row)| row.as_slice()))?;
            loaded.push((key.clone(), grouped));
        }
        // The probe prefix already bounds the rows; project them like the
        // source would.
        let unscanned = LookupOp {
            source: TableSourceOp {
                scan: None,
                ..lookup.source.clone()
            },
            ..lookup.clone()
        };
        let unscanned_layout = LookupLayout {
            prefix: Vec::new(),
            ..layout.clone()
        };
        let mut probed = ProbedRows::default();
        for (key, grouped) in loaded {
            let layout = match &lookup.index {
                // An index row's own key is still checked against the
                // static prefix, which the probe prefix includes.
                Some(_) => layout,
                None => &unscanned_layout,
            };
            let found = self.lookup_keyed_rows(&unscanned, layout, output, &grouped)?;
            let rows = probed.entry(key.clone()).or_default();
            for (_, (row_key, identity, record)) in found {
                // An index entry and the row it names are separate reads; a
                // row that has since left this key is not one of its rows.
                if lookup.index.is_some() && row_key != key {
                    continue;
                }
                rows.insert(identity, record);
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
            let key = Self::lookup_probe_key(lookup, &layout, &input.descriptor, delta.raw())?;
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
        // The table's own changes, under the static prefix. Leaving keys are
        // retracted whole. An entering key's probe may or may not already
        // reflect this tick's writes (a secondary index is written after the
        // rows it names), so its rows take the changes too: a row is removed
        // only if it is the probed row and inserted over any probed version,
        // which converges either way.
        let table_rows = if replace {
            Vec::new()
        } else {
            self.lookup_keyed_rows(lookup, &layout, &output_desc, self.table_deltas)?
        };
        let mut probed = probed;
        for (weight, (key, identity, record)) in &table_rows {
            let Some(rows) = probed.get_mut(key) else {
                continue;
            };
            if *weight < 0 && rows.get(identity) == Some(record) {
                rows.remove(identity);
            }
        }
        for (weight, (key, identity, record)) in &table_rows {
            if *weight > 0
                && let Some(rows) = probed.get_mut(key)
            {
                rows.insert(identity.clone(), record.clone());
            }
        }

        let mut operator = match self.operator_states.remove(&operator_key) {
            Some(operator) if !replace => operator,
            _ => OperatorState::Lookup(AsOf::default()),
        };
        let OperatorState::Lookup(state) = &mut operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        // Changes to rows of keys that stay live, by row identity.
        let mut changes = Vec::new();
        {
            let current = state.value().0.as_ref();
            for (weight, (key, identity, record)) in table_rows {
                if !current.live(&key) || leaving.contains(&key) {
                    continue;
                }
                changes.push((weight, key, identity, record));
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

/// Static-prefix length of an index lookup.
fn layout_prefix_len(index: &IndexSourceOp) -> usize {
    match &index.scan {
        Some(StaticScanSpec::Prefix(values) | StaticScanSpec::Point(values)) => values.len(),
        _ => 0,
    }
}
