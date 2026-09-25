//! Synchronous batch kernels. Inputs have been resolved by the execution driver;
//! no kernel asks the graph to execute a predecessor or owns an async frame.

use super::*;

impl TickEvaluator<'_> {
    pub(super) fn compute_batch(
        &mut self,
        node: NodeId,
        inputs: &[Arc<RecordDeltas>],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let output_desc = graph_node.descriptor.output.records();
        debug_assert_eq!(inputs.len(), graph_node.descriptor.inputs.len());
        match &graph_node.descriptor.operator {
            OpType::TableSource(input)
                if self.context.eval_mode == EvalMode::Hydrate
                    && self.evaluation_inputs.is_some() =>
            {
                NodeState::update_table_source_from_inputs(
                    input,
                    self.schema,
                    self.variant_projections,
                    &output_desc,
                    self.evaluation_inputs
                        .as_deref_mut()
                        .expect("guarded evaluation inputs"),
                )
            }
            OpType::TableSource(input) => NodeState::update_table_source(
                input,
                self.schema,
                self.variant_projections,
                &output_desc,
                self.table_deltas,
            ),
            OpType::IndexSource(input)
                if self.context.eval_mode == EvalMode::Hydrate
                    && self.evaluation_inputs.is_some() =>
            {
                NodeState::update_index_source_from_inputs(
                    input,
                    self.schema,
                    self.variant_projections,
                    &output_desc,
                    self.evaluation_inputs
                        .as_deref_mut()
                        .expect("guarded evaluation inputs"),
                )
            }
            OpType::InlineRecords(inline) if self.context.eval_mode == EvalMode::Hydrate => {
                Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas: inline
                        .records
                        .iter()
                        .cloned()
                        .map(|record| RecordDelta {
                            record: record.into(),
                            weight: 1,
                        })
                        .collect(),
                })
            }
            OpType::InlineRecords(_) => Ok(RecordDeltas::empty(output_desc)),
            OpType::BindingSource(input) => NodeState::update_binding_source(
                input,
                &output_desc,
                self.binding_deltas,
                self.binding_snapshots,
                self.context.arrangement_update_mode,
            ),
            OpType::Arrange(spec) => {
                let [_] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let input = Arc::clone(&inputs[0]);
                if self.arrangement_needs_index(node) {
                    let fields = spec.fields.iter().cloned().collect();
                    let input = self.materialize_indirect_fields(&input, &fields)?;
                    let key =
                        self.arrangement_key(node, output_desc, &spec.fields, spec.comparison)?;
                    let stamp = self.arrangement_sub_tick(&key);
                    #[cfg(feature = "cold-settle-attribution")]
                    self.trace_arrangement_snapshot(&key, &input.deltas);
                    let mut state = self.arrangement_states.remove(&key).unwrap_or_default();
                    super::join::prepare_arrangement(
                        &mut state,
                        &output_desc,
                        &spec.fields,
                        spec.comparison,
                        JoinInput::snapshot(&input),
                        stamp,
                        self.context.arrangement_update_mode,
                    )?;
                    self.insert_arrangement(key, state);
                    Ok(input.as_ref().clone())
                } else {
                    Ok(input.as_ref().clone())
                }
            }
            OpType::FrontierSource(frontier_source) => {
                self.frontier_source(frontier_source, &output_desc)
            }
            OpType::Filter(filter) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                self.compute_filter(node, filter, output_desc, &input)
            }
            OpType::MapProject(project) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                self.compute_projection(node, project, output_desc, &input)
            }
            OpType::UnwrapNullable(unwrap) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                NodeState::update_unwrap_nullable(unwrap, output_desc, &input)
            }
            OpType::Unnest(unnest) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                NodeState::update_unnest(unnest, output_desc, &input)
            }
            OpType::VariantProject(variant_project) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                NodeState::update_variant_project(variant_project, output_desc, &input)
            }
            OpType::ArgMaxBy(arg_max_by) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                let input = self.materialize_indirect_field_indices(
                    &input,
                    &arg_max_by.comparison_field_indices,
                )?;
                self.update_arg_by(
                    node,
                    ArgBySpec {
                        group_field_indices: &arg_max_by.group_field_indices,
                        comparison_field_indices: &arg_max_by.comparison_field_indices,
                        direction: ArgByDirection::Max,
                    },
                    output_desc,
                    &input,
                )
            }
            OpType::ArgMinBy(arg_min_by) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                let input = self.materialize_indirect_field_indices(
                    &input,
                    &arg_min_by.comparison_field_indices,
                )?;
                self.update_arg_by(
                    node,
                    ArgBySpec {
                        group_field_indices: &arg_min_by.group_field_indices,
                        comparison_field_indices: &arg_min_by.comparison_field_indices,
                        direction: ArgByDirection::Min,
                    },
                    output_desc,
                    &input,
                )
            }
            OpType::TopBy(top_by) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                let mut fields = top_by.group_field_indices.clone();
                fields.extend(top_by.sort_field_indices.iter().copied());
                fields.sort_unstable();
                fields.dedup();
                let input = self.materialize_indirect_field_indices(&input, &fields)?;
                self.update_top_by(node, top_by, output_desc, &input)
            }
            OpType::CollectBy(collect_by) => {
                let canonical = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                let input = self.materialize_indirect_input(&canonical)?;
                self.update_collect_by(node, collect_by, output_desc, &input, &canonical)
            }
            OpType::Aggregate(aggregate) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                let canonical = Arc::clone(&input);
                // COUNT(*) without grouping observes only row weights. Its
                // exact result cannot depend on any scalar bytes, so retain
                // indirect columns and issue no chunk requests.
                let needs_values = !aggregate.group_key.is_empty()
                    || aggregate.aggregates.iter().any(|expr| {
                        expr.function != AggregateFunction::Count
                            || expr.expression.is_some()
                            || expr.distinct
                    });
                let input = if needs_values {
                    let mut fields = aggregate.group_field_indices.clone();
                    let expression_fields = aggregate
                        .aggregates
                        .iter()
                        .filter_map(|aggregate| aggregate.expression.as_ref())
                        .cloned()
                        .collect::<Vec<_>>();
                    for field in plan_expr_fields(&expression_fields) {
                        fields.push(
                            super::record_projection::resolve_field_name(&input.descriptor, &field)
                                .ok_or_else(|| {
                                    IvmRuntimeError::GraphFieldNotFound(field.clone())
                                })?,
                        );
                    }
                    fields.sort_unstable();
                    fields.dedup();
                    self.materialize_indirect_field_indices(&input, &fields)?
                } else {
                    input
                };
                self.update_aggregate(node, aggregate, output_desc, &input, &canonical)
            }
            OpType::IndexBy(index_by) => {
                let input = inputs
                    .first()
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
                let mut fields = index_by.key_fields.clone();
                if index_by.append_value_to_key {
                    fields.extend(index_by.value_fields.iter().copied());
                }
                fields.sort_unstable();
                fields.dedup();
                let input = self.materialize_indirect_field_indices(&input, &fields)?;
                let trace = std::env::var_os("GROOVE_TRACE_INDEX_BY").is_some();
                let start = trace.then(std::time::Instant::now);
                let input_len = input.deltas.len();
                let result = NodeState::update_index_by(index_by, output_desc, &input);
                if trace && input_len > 0 {
                    let output_len = result
                        .as_ref()
                        .map(|records| records.deltas.len())
                        .unwrap_or(0);
                    let index_name = index_by
                        .explicit_index
                        .as_ref()
                        .map(|index| index.name.as_str())
                        .unwrap_or("<derived>");
                    let key_fields = index_by
                        .key_expressions
                        .iter()
                        .map(|expr| format!("{expr:?}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    eprintln!(
                        "GROOVE_TRACE_INDEX_BY node={node:?} index={index_name} input={input_len} output={output_len} unique={} append_value_to_key={} store_value={} scan={} key_fields=[{}] elapsed_ms={:.3}",
                        index_by.unique,
                        index_by.append_value_to_key,
                        index_by.store_value,
                        index_by.scan.is_some(),
                        key_fields,
                        start.expect("trace start").elapsed().as_secs_f64() * 1000.0
                    );
                }
                result
            }
            OpType::Union => {
                let ready_inputs = inputs.to_vec();
                NodeState::update_union(output_desc, ready_inputs)
            }
            OpType::Join(join) => {
                let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = Arc::clone(&inputs[0]);
                let right = Arc::clone(&inputs[1]);
                // A first-result left input may have skipped its arrangement.
                // Resolve only its join keys before probing, including after
                // a missing-chunk suspension. Indexed inputs already did this.
                let left = if self.stream_snapshot_joins() {
                    self.materialize_indirect_fields(&left, &plan_expr_fields(&join.left_key))?
                } else {
                    left
                };
                self.update_join(
                    node,
                    join,
                    output_desc,
                    *left_input,
                    *right_input,
                    &left,
                    &right,
                )
            }
            OpType::SemiJoin(join) => {
                let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = Arc::clone(&inputs[0]);
                let right = Arc::clone(&inputs[1]);
                let left_fields = plan_expr_fields(&join.left_key);
                let right_fields = plan_expr_fields(&join.right_key);
                let left = self.materialize_indirect_fields(&left, &left_fields)?;
                let right = self.materialize_indirect_fields(&right, &right_fields)?;
                self.update_semi_join(
                    node,
                    join,
                    output_desc,
                    *left_input,
                    *right_input,
                    &left,
                    &right,
                )
            }
            OpType::AntiJoin(join) => {
                let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = Arc::clone(&inputs[0]);
                let right = Arc::clone(&inputs[1]);
                let left_fields = plan_expr_fields(&join.left_key);
                let right_fields = plan_expr_fields(&join.right_key);
                let left = self.materialize_indirect_fields(&left, &left_fields)?;
                let right = self.materialize_indirect_fields(&right, &right_fields)?;
                self.update_anti_join(
                    node,
                    join,
                    output_desc,
                    *left_input,
                    *right_input,
                    &left,
                    &right,
                )
            }
            OpType::RecursiveStepWitness(_) => {
                let [recursive] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                // Drive the owner first. Its generic side state is then
                // the only source of this output; never re-evaluate a
                // recursive step independently.
                self.update_recursive_step_witness(*recursive, output_desc)
            }
            // Durable writes are an async preparation boundary driven outside
            // this borrowed evaluator frame by `tick_durable_nodes`.
            OpType::Persist(_) => Err(IvmRuntimeError::UnsupportedOperator),
            _ => Err(IvmRuntimeError::UnsupportedOperator),
        }
    }
}
