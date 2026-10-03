//! Retained ordered-index prefixes. A storage snapshot includes the current
//! tick's writes; re-reading a bounded prefix therefore also supplies refills.
//! No state is installed until both the page and its completeness proof exist.

use super::evaluation_session::StorageRequestKey;
use super::*;
use crate::ivm::{IndexWindow, IndexWindowExclusionOp, IndexWindowOp};

const MAX_WINDOW_CANDIDATES: usize = 4096;

fn scalar_window_key(value_type: &ValueType) -> bool {
    match value_type {
        ValueType::Nullable(inner) => scalar_window_key(inner),
        ValueType::U8
        | ValueType::U16
        | ValueType::U32
        | ValueType::U64
        | ValueType::I32
        | ValueType::I64
        | ValueType::Bool
        | ValueType::String
        | ValueType::Bytes
        | ValueType::Uuid => true,
        _ => false,
    }
}

/// A page witness is valid only when projecting a stored row preserves the
/// index's first non-prefix key. Recheck registered cases on each evaluation:
/// a schema case may be added while a subscription remains alive.
fn validate_window_order(
    source: &IndexSourceOp,
    output: RecordDescriptor,
    order_field: usize,
    projections: &HashMap<VariantProjectionKey, VariantProjection>,
) -> Result<(), IvmRuntimeError> {
    let unsupported = || {
        IvmRuntimeError::UnsupportedIndexWindow(
            "an ordering projection that differs from the index key",
        )
    };
    let prefix_len = match source.scan.as_ref() {
        Some(
            StaticScanSpec::PrefixLimit { prefix, .. }
            | StaticScanSpec::ReversePrefixLimit { prefix, .. },
        ) => prefix.len(),
        _ => return Err(unsupported()),
    };
    let index_field = *source.key_fields.get(prefix_len).ok_or_else(unsupported)?;
    if output.fields()[order_field].value_type
        != source.input_descriptor.fields()[index_field].value_type
    {
        return Err(unsupported());
    }
    let projection = projections
        .get(&VariantProjectionKey {
            table: source.table.clone(),
            target: source.row_projection.clone().ok_or_else(unsupported)?,
        })
        .ok_or_else(unsupported)?;
    for (tag, case) in &projection.cases {
        let (row_source, row_project) = match case {
            VariantProjectionCase::Ignore { .. } => continue,
            VariantProjectionCase::Project {
                source, project, ..
            } => (source, project),
            _ => return Err(unsupported()),
        };
        let ProjectExpr::Field(row_field) = &row_project.expressions[order_field].expression else {
            return Err(unsupported());
        };
        let row_field = resolve_field_ref(row_source, row_field)?;
        let physical_index_field = if let Some(target) = &source.variant_projection {
            let case = projections
                .get(&VariantProjectionKey {
                    table: source.table.clone(),
                    target: target.clone(),
                })
                .and_then(|projection| projection.cases.get(tag))
                .ok_or_else(unsupported)?;
            let (index_source, index_project) = match case {
                VariantProjectionCase::Ignore { .. } => continue,
                VariantProjectionCase::Project {
                    source, project, ..
                } => (source, project),
                _ => return Err(unsupported()),
            };
            if !row_source.registry_compatible_with(index_source) {
                return Err(unsupported());
            }
            let ProjectExpr::Field(field) = &index_project.expressions[index_field].expression
            else {
                return Err(unsupported());
            };
            resolve_field_ref(index_source, field)?
        } else {
            index_field
        };
        if row_field != physical_index_field {
            return Err(unsupported());
        }
    }
    Ok(())
}

impl IvmRuntime {
    pub(super) fn compile_index_window(
        &self,
        source: &IndexSourceOp,
        output: RecordDescriptor,
        window: &IndexWindow,
    ) -> Result<IndexWindowOp, IvmRuntimeError> {
        if window.limit == 0
            || source.row_projection.is_none()
            || !source.intersections.is_empty()
            || source.candidate_filter.is_some()
            || !matches!(
                source.scan,
                Some(
                    StaticScanSpec::PrefixLimit { .. } | StaticScanSpec::ReversePrefixLimit { .. }
                )
            )
        {
            return Err(IvmRuntimeError::UnsupportedIndexWindow(
                "this index source shape",
            ));
        }
        // A multi-valued index can have several entries for one table row;
        // counting hydrated rows would not prove exhaustion of its prefix.
        if source
            .key_fields
            .iter()
            .any(|&field| !scalar_window_key(&source.input_descriptor.fields()[field].value_type))
        {
            return Err(IvmRuntimeError::UnsupportedIndexWindow(
                "non-scalar or non-order-preserving index keys",
            ));
        }
        let order_field = resolve_field_ref(&output, &FieldRef::name(&window.order_field))?;
        validate_window_order(source, output, order_field, &self.variant_projections)?;
        let exclusion = window
            .exclusion
            .as_ref()
            .map(|exclusion| {
                let table = self
                    .schema
                    .table(&exclusion.table)
                    .ok_or_else(|| IvmRuntimeError::TableNotFound(exclusion.table.clone()))?;
                if table.has_variants() || exclusion.predicate.has_template_arguments() {
                    return Err(IvmRuntimeError::UnsupportedIndexWindow(
                        "heterogeneous or parameterized exclusion records",
                    ));
                }
                let descriptor = table.record_schema();
                let target_key_fields = primary_key_field_indices(table, &descriptor)?;
                let key_fields = exclusion
                    .key_fields
                    .iter()
                    .map(|field| resolve_field_ref(&output, &FieldRef::name(field)))
                    .collect::<Result<Vec<_>, _>>()?;
                if target_key_fields.len() != exclusion.key_prefix.len() + key_fields.len() {
                    return Err(IvmRuntimeError::UnsupportedIndexWindow(
                        "incomplete exclusion primary keys",
                    ));
                }
                for (value, &field) in exclusion.key_prefix.iter().zip(&target_key_fields) {
                    if value.value_type().as_ref() != Some(&descriptor.fields()[field].value_type) {
                        return Err(IvmRuntimeError::GraphOutputMismatch);
                    }
                }
                for (&field, &target) in key_fields
                    .iter()
                    .zip(&target_key_fields[exclusion.key_prefix.len()..])
                {
                    if output.fields()[field].value_type != descriptor.fields()[target].value_type {
                        return Err(IvmRuntimeError::GraphOutputMismatch);
                    }
                }
                let StaticScanBounds::Prefix(key_prefix) =
                    scan_bounds(&StaticScanSpec::Prefix(exclusion.key_prefix.clone()))?
                else {
                    unreachable!()
                };
                Ok(IndexWindowExclusionOp {
                    table: exclusion.table.clone(),
                    descriptor,
                    key_prefix,
                    key_fields,
                    target_key_fields,
                    predicate: exclusion.predicate.clone(),
                })
            })
            .transpose()?;
        Ok(IndexWindowOp {
            limit: window.limit,
            order_field,
            exclusion,
        })
    }
}

impl TickEvaluator<'_> {
    pub(super) fn update_index_window(
        &mut self,
        node: NodeId,
        source: &IndexSourceOp,
        output: RecordDescriptor,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let key = self.operator_key(node);
        if key.scope != ScopeId::root() {
            return Err(IvmRuntimeError::UnsupportedIndexWindow(
                "recursive source scopes",
            ));
        }
        let window = source.window.as_ref().expect("window source");
        validate_window_order(source, output, window.order_field, self.variant_projections)?;
        let mut cap = window.limit.saturating_add(1);
        let max_cap = cap.max(MAX_WINDOW_CANDIDATES);
        let reversed = scan_reversed(source.scan.as_ref());
        let mut probe = source.clone();
        let inputs = self
            .evaluation_inputs
            .as_deref_mut()
            .ok_or(IvmRuntimeError::StorageUnavailable)?;
        let rows = loop {
            match probe.scan.as_mut() {
                Some(
                    StaticScanSpec::PrefixLimit { max_items, .. }
                    | StaticScanSpec::ReversePrefixLimit { max_items, .. },
                ) => *max_items = cap,
                _ => return Err(IvmRuntimeError::UnsupportedIndexWindow("uncapped sources")),
            }
            let request = NodeState::index_source_request(&probe)?
                .ok_or(IvmRuntimeError::StorageUnavailable)?;
            let raw_count = inputs.rows(request)?.len();
            let projected = NodeState::update_index_source_from_inputs(
                &probe,
                self.schema,
                self.variant_projections,
                &output,
                inputs,
            )?;
            let mut visible = Vec::with_capacity(projected.deltas.len());
            let mut blocked = false;
            for delta in projected.deltas {
                if let Some(exclusion) = &window.exclusion {
                    let mut target_key = exclusion.key_prefix.clone();
                    target_key.extend(primary_key_value_bytes(
                        &output,
                        delta.raw(),
                        &exclusion.key_fields,
                    )?);
                    let stored = match inputs.value(StorageRequestKey::Get {
                        family: exclusion.table.clone(),
                        key: target_key.clone(),
                    }) {
                        Ok(stored) => stored,
                        Err(IvmRuntimeError::EvaluationBlocked) => {
                            blocked = true;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    if let Some(stored) = stored {
                        let (_, record) = records::split_variant_record(stored)?;
                        if primary_key_value_bytes(
                            &exclusion.descriptor,
                            record,
                            &exclusion.target_key_fields,
                        )? != target_key
                        {
                            return Err(IvmRuntimeError::GraphOutputMismatch);
                        }
                        if exclusion.predicate.matches(
                            BorrowedRecord::new(record, &exclusion.descriptor),
                            ValueComparison::Exact,
                        )? {
                            continue;
                        }
                    }
                }
                let order = primary_key_value_bytes(&output, delta.raw(), &[window.order_field])?;
                visible.push((order, delta.record));
            }
            if blocked {
                return Err(IvmRuntimeError::EvaluationBlocked);
            }
            visible.sort_by(|a, b| {
                if reversed {
                    b.0.cmp(&a.0)
                } else {
                    a.0.cmp(&b.0)
                }
            });
            // Retain the complete boundary tie group, plus any strictly worse
            // witness. Downstream query ordering supplies its own UUID tie-break.
            let proved = visible.len() > window.limit
                && visible[window.limit - 1].0 != visible.last().unwrap().0;
            if raw_count < cap || proved {
                let mut rows = HashMap::default();
                for (_, record) in visible {
                    *rows.entry(record).or_insert(0_i64) += 1;
                }
                break rows;
            }
            if cap >= max_cap {
                return Err(IvmRuntimeError::UnsupportedIndexWindow(
                    "a sparse prefix or boundary tie beyond the bounded candidate budget",
                ));
            }
            cap = cap.saturating_mul(4).min(max_cap);
        };
        let mut state = if self.context.eval_mode == EvalMode::Hydrate {
            AsOf::default()
        } else {
            match self.operator_states.remove(&key) {
                Some(OperatorState::IndexWindow(state)) => state,
                _ => return Err(IvmRuntimeError::NodeStateMissing(node)),
            }
        };
        let mut deltas = Vec::new();
        for (record, before) in state.value().iter() {
            let weight = rows.get(record).copied().unwrap_or(0) - before;
            if weight != 0 {
                deltas.push(RecordDelta {
                    record: record.clone(),
                    weight,
                });
            }
        }
        for (record, &weight) in &rows {
            if !state.value().contains_key(record) {
                deltas.push(RecordDelta {
                    record: record.clone(),
                    weight,
                });
            }
        }
        *state.value_mut() = Rc::new(rows);
        state.mark_forward_as_of(SubTick {
            tick: self.current_tick,
            sub_tick: 0,
        })?;
        self.operator_states
            .insert(key, OperatorState::IndexWindow(state));
        Ok(RecordDeltas {
            descriptor: output,
            deltas,
        })
    }
}
