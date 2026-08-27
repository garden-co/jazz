//! Hash-consed GraphBuilder compilation into executable IVM nodes.

use super::*;

impl IvmRuntime {
    fn add_arrangement_node(
        &mut self,
        input: NodeId,
        records: RecordDescriptor,
        fields: Vec<String>,
        comparison: ValueComparison,
    ) -> NodeId {
        self.logical_nodes_requested += 1;
        let node = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::Arrange(ArrangeOp { fields, comparison }),
                [input],
                NodeOutput::Arrangement(ArrangementDescriptor { records }),
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(node);
        node
    }

    pub(super) fn add_dedup_graph(
        &mut self,
        graph: &GraphBuilder,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        validate_collect_by_terminality(graph)?;
        let mut output_memo = HashMap::default();
        // Precompute descriptors once for the complete graph. The postorder
        // compiler below can then reuse those descriptors without repeatedly
        // traversing a long policy graph from each parent.
        self.infer_builder_output_cached(graph, &mut output_memo)?;
        let mut compiled_memo = HashMap::default();
        for builder in graph.postorder() {
            self.add_dedup_graph_cached(builder, &mut output_memo, &mut compiled_memo)?;
        }
        compiled_memo
            .remove(&graph_builder_key(graph))
            .ok_or(IvmRuntimeError::UnsupportedOperator)
    }

    fn add_dedup_graph_cached(
        &mut self,
        graph: &GraphBuilder,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
        compiled_memo: &mut HashMap<usize, CompiledNode>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        let key = graph_builder_key(graph);
        if let Some(compiled) = compiled_memo.get(&key) {
            return Ok(compiled.clone());
        }
        let inferred_output = self.infer_builder_output_cached(graph, output_memo)?;
        let compiled = match graph {
            GraphBuilder::Table { .. }
            | GraphBuilder::InlineRecords { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::FrontierSource { .. }
            | GraphBuilder::BindingSource { .. }
            | GraphBuilder::Recursive { .. }
            | GraphBuilder::CollectBy { .. } => {
                self.add_dedup_source_graph(graph, inferred_output, output_memo, compiled_memo)
            }
            GraphBuilder::ArgMaxBy { .. }
            | GraphBuilder::ArgMinBy { .. }
            | GraphBuilder::TopBy { .. } => {
                self.add_dedup_ordering_graph(graph, inferred_output, output_memo, compiled_memo)
            }
            GraphBuilder::Aggregate { .. }
            | GraphBuilder::Filter { .. }
            | GraphBuilder::Project { .. }
            | GraphBuilder::StreamingChecksum { .. }
            | GraphBuilder::UnwrapNullable { .. }
            | GraphBuilder::Unnest { .. }
            | GraphBuilder::VariantProject { .. }
            | GraphBuilder::Union { .. } => {
                self.add_dedup_unary_graph(graph, inferred_output, output_memo, compiled_memo)
            }
            GraphBuilder::Join { .. }
            | GraphBuilder::SemiJoin { .. }
            | GraphBuilder::AntiJoin { .. } => {
                self.add_dedup_join_graph(graph, inferred_output, output_memo, compiled_memo)
            }
        }?;
        compiled_memo.insert(key, compiled.clone());
        Ok(compiled)
    }

    #[inline(never)]
    fn add_dedup_source_graph(
        &mut self,
        graph: &GraphBuilder,
        inferred_output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
        compiled_memo: &mut HashMap<usize, CompiledNode>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        match graph {
            GraphBuilder::Table {
                table,
                scan,
                variant_projection,
            } => {
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::TableSource(TableSourceOp {
                            table: table.clone(),
                            scan: scan.clone(),
                            variant_projection: variant_projection
                                .clone()
                                .map(VariantProjectionTarget::Named),
                        }),
                        [],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::InlineRecords { output, records } => {
                if !inferred_output.registry_compatible_with(output) {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::InlineRecords(InlineRecordsOp {
                            records: records.clone(),
                        }),
                        [],
                        *output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: *output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::Index {
                table,
                index,
                scan,
                intersections,
                row_projection,
            } => {
                let table = self
                    .schema
                    .table(table)
                    .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?
                    .clone();
                let index = table
                    .indices
                    .iter()
                    .find(|candidate| candidate.name == *index)
                    .ok_or_else(|| IvmRuntimeError::IndexNotFound(index.clone()))?
                    .clone();
                let source = self.index_source_op(
                    &table,
                    &index,
                    scan.clone(),
                    intersections.clone(),
                    row_projection.clone(),
                )?;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(OpType::IndexSource(source), [], output),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::FrontierSource { binding, output } => {
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::FrontierSource(FrontierSourceOp {
                            binding: binding.clone(),
                        }),
                        [],
                        *output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: inferred_output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::BindingSource { shape, output } => {
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::BindingSource(BindingSourceOp {
                            shape: shape.clone(),
                        }),
                        [],
                        *output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: inferred_output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::Recursive {
                seed,
                step,
                frontier,
                max_iters,
            } => {
                if builder_contains_recursive(seed) || builder_contains_recursive(step) {
                    return Err(IvmRuntimeError::UnsupportedNestedRecursion);
                }
                let compiled_seed =
                    self.add_dedup_graph_cached(seed, output_memo, compiled_memo)?;
                let compiled_step =
                    self.add_dedup_graph_cached(step, output_memo, compiled_memo)?;
                if !compiled_seed
                    .output
                    .registry_compatible_with(&compiled_step.output)
                    || !compiled_seed
                        .output
                        .registry_compatible_with(&inferred_output)
                {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Recursive(RecursiveOp {
                            frontier: frontier.clone(),
                            max_iters: *max_iters,
                            read_tables: recursive_read_tables(
                                &self.graph,
                                compiled_seed.node,
                                compiled_step.node,
                            )?,
                        }),
                        [compiled_seed.node, compiled_step.node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::CollectBy { input, collect } => self.add_collect_by_graph(
                input,
                collect,
                inferred_output,
                output_memo,
                compiled_memo,
            ),
            _ => unreachable!("dispatcher routes only source graph builders here"),
        }
    }

    #[inline(never)]
    fn add_dedup_ordering_graph(
        &mut self,
        graph: &GraphBuilder,
        inferred_output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
        compiled_memo: &mut HashMap<usize, CompiledNode>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        match graph {
            GraphBuilder::ArgMaxBy {
                input,
                group_cols,
                order_cols,
            } => {
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_indices = order_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let primary_key_field_indices =
                    if let GraphBuilder::Table { table, .. } = input.as_ref() {
                        let table_schema = self
                            .schema
                            .table(table)
                            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?
                            .clone();
                        let primary_key = table_schema
                            .primary_key
                            .as_ref()
                            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.clone()))?;
                        let primary_key_field_indices = primary_key
                            .columns
                            .iter()
                            .map(|column| {
                                output.field_index(&column.column).ok_or_else(|| {
                                    IvmRuntimeError::GraphFieldNotFound(column.column.clone())
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        validate_arg_by_primary_key_indices(
                            "arg_max_by",
                            &table_schema,
                            &group_field_indices,
                            &order_field_indices,
                            &primary_key_field_indices,
                        )?;
                        primary_key_field_indices
                    } else {
                        group_field_indices
                            .iter()
                            .chain(&order_field_indices)
                            .copied()
                            .collect()
                    };
                let group_field_names = group_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_names = order_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let arrangement = self.add_arrangement_node(
                    compiled_input.node,
                    output,
                    group_field_names.clone(),
                    ValueComparison::Exact,
                );
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::ArgMaxBy(ArgMaxByOp {
                            group_fields: group_field_names,
                            order_fields: order_field_names,
                            group_field_indices,
                            primary_key_field_indices,
                        }),
                        [arrangement],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::ArgMinBy {
                input,
                group_cols,
                order_cols,
            } => {
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_indices = order_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let primary_key_field_indices =
                    if let GraphBuilder::Table { table, .. } = input.as_ref() {
                        let table_schema = self
                            .schema
                            .table(table)
                            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?
                            .clone();
                        let primary_key = table_schema
                            .primary_key
                            .as_ref()
                            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.clone()))?;
                        let primary_key_field_indices = primary_key
                            .columns
                            .iter()
                            .map(|column| {
                                output.field_index(&column.column).ok_or_else(|| {
                                    IvmRuntimeError::GraphFieldNotFound(column.column.clone())
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        validate_arg_by_primary_key_indices(
                            "arg_min_by",
                            &table_schema,
                            &group_field_indices,
                            &order_field_indices,
                            &primary_key_field_indices,
                        )?;
                        primary_key_field_indices
                    } else {
                        group_field_indices
                            .iter()
                            .chain(&order_field_indices)
                            .copied()
                            .collect()
                    };
                let group_field_names = group_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_names = order_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let arrangement = self.add_arrangement_node(
                    compiled_input.node,
                    output,
                    group_field_names.clone(),
                    ValueComparison::Exact,
                );
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::ArgMinBy(ArgMinByOp {
                            group_fields: group_field_names,
                            order_fields: order_field_names,
                            group_field_indices,
                            primary_key_field_indices,
                        }),
                        [arrangement],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::TopBy {
                input,
                group_cols,
                order_cols,
                tie_cols,
                offset,
                limit,
            } => {
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_indices = order_cols
                    .iter()
                    .map(|order| resolve_field_ref(&output, &order.field))
                    .collect::<Result<Vec<_>, _>>()?;
                let tie_field_indices = tie_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let group_field_names = group_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_fields = order_cols
                    .iter()
                    .zip(&order_field_indices)
                    .map(|(order, field_idx)| {
                        Ok(TopByOrderField {
                            field: field_name_at(&output, *field_idx)?,
                            direction: order.direction,
                        })
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let tie_field_names = tie_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let sort_field_indices = order_field_indices
                    .iter()
                    .chain(&tie_field_indices)
                    .copied()
                    .collect::<Vec<_>>();
                let sort_directions = order_cols
                    .iter()
                    .map(|order| order.direction)
                    .chain(std::iter::repeat_n(
                        TopByDirection::Asc,
                        tie_field_indices.len(),
                    ))
                    .collect::<Vec<_>>();
                let arrangement = self.add_arrangement_node(
                    compiled_input.node,
                    output,
                    group_field_names.clone(),
                    ValueComparison::Exact,
                );
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::TopBy(TopByOp {
                            group_fields: group_field_names,
                            group_field_indices,
                            order_fields,
                            tie_fields: tie_field_names,
                            sort_field_indices,
                            sort_directions,
                            offset: *offset,
                            limit: *limit,
                        }),
                        [arrangement],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: Some(node),
                })
            }
            _ => unreachable!("dispatcher routes only ordering graph builders here"),
        }
    }

    #[inline(never)]
    fn add_dedup_unary_graph(
        &mut self,
        graph: &GraphBuilder,
        inferred_output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
        compiled_memo: &mut HashMap<usize, CompiledNode>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        match graph {
            GraphBuilder::Aggregate {
                input,
                group_cols,
                aggregates,
            } => {
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&input_output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let group_key = group_field_indices
                    .iter()
                    .map(|field| Ok(PlanExpr::Field(field_name_at(&input_output, *field)?)))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let aggregates = aggregates
                    .iter()
                    .map(|aggregate| resolve_aggregate_expr(&input_output, aggregate))
                    .collect::<Result<Vec<_>, _>>()?;
                let arrangement = self.add_arrangement_node(
                    input_node,
                    input_output,
                    plan_expr_names(&group_key),
                    ValueComparison::Exact,
                );
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Aggregate(AggregateOp {
                            group_key,
                            group_field_indices,
                            aggregates,
                        }),
                        [arrangement],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::Filter {
                input,
                predicate,
                comparison,
            } => {
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let input_node = compiled_input.node;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Filter(FilterOp {
                            predicate: predicate.clone(),
                            comparison: *comparison,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::Project { input, fields } => {
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let output = inferred_output;
                let mapping = fields
                    .iter()
                    .filter_map(|field| {
                        field.source().map(|source| {
                            resolve_field_ref(&input_output, source).map(|idx| (0, idx))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::MapProject(MapProjectOp {
                            expressions: fields
                                .iter()
                                .map(|field| {
                                    project_field_expr(&input_output, field).map(|expression| {
                                        ProjectionExpr {
                                            expression,
                                            output_name: Some(field.output_name.clone()),
                                        }
                                    })
                                })
                                .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
                            mapping,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::StreamingChecksum {
                input,
                field,
                output_field,
                window_bytes,
                max_bytes_per_turn,
            } => {
                if *window_bytes == 0 || *max_bytes_per_turn == 0 {
                    return Err(IvmRuntimeError::InvalidStreamingChecksumBudget);
                }
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let field_idx = resolve_field_ref(&compiled_input.output, field)?;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::StreamingChecksum(StreamingChecksumOp {
                            field: field_ref_name(&compiled_input.output, field)?,
                            field_idx,
                            output_field: output_field.clone(),
                            window_bytes: *window_bytes,
                            max_bytes_per_turn: *max_bytes_per_turn,
                        }),
                        [compiled_input.node],
                        inferred_output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: inferred_output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::UnwrapNullable { input, field } => {
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let field_idx = resolve_field_ref(&input_output, field)?;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::UnwrapNullable(UnwrapNullableOp {
                            field: field_ref_name(&input_output, field)?,
                            field_idx,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::Unnest {
                input,
                array_field,
                element_field,
            } => {
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let array_field_idx = resolve_field_ref(&input_output, array_field)?;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Unnest(UnnestOp {
                            array_field: field_ref_name(&input_output, array_field)?,
                            array_field_idx,
                            element_field: element_field.clone(),
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::VariantProject { input, field, case } => {
                let compiled_input =
                    self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let field_idx = resolve_field_ref(&input_output, field)?;
                let ValueType::Enum(schema) = &input_output.fields()[field_idx].value_type else {
                    return Err(IvmRuntimeError::VariantProjectFieldTypeMismatch {
                        field: field_ref_name(&input_output, field)?,
                    });
                };
                let tag = schema.tag(case)?;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::VariantProject(VariantProjectOp {
                            field: field_ref_name(&input_output, field)?,
                            field_idx,
                            tag,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::Union { inputs } => {
                let mut input_nodes = Vec::with_capacity(inputs.len());
                let mut public_root_ordering = None;
                for input in inputs {
                    let compiled_input =
                        self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
                    if input_nodes.is_empty() {
                        // Structured lowering places the public/root anchor
                        // first; later arms carry association rows and may
                        // contain their own nested TopBy nodes.
                        public_root_ordering = compiled_input.root_ordering_node;
                    }
                    let input_node = compiled_input.node;
                    let input_output = compiled_input.output;
                    if !inferred_output.registry_compatible_with(&input_output) {
                        return Err(IvmRuntimeError::GraphOutputMismatch);
                    }
                    input_nodes.push(input_node);
                }
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(OpType::Union, input_nodes, output),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: public_root_ordering,
                })
            }
            _ => unreachable!("dispatcher routes only unary graph builders here"),
        }
    }

    #[inline(never)]
    fn add_dedup_join_graph(
        &mut self,
        graph: &GraphBuilder,
        inferred_output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
        compiled_memo: &mut HashMap<usize, CompiledNode>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        match graph {
            GraphBuilder::Join {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => {
                let compiled_left =
                    self.add_dedup_graph_cached(left, output_memo, compiled_memo)?;
                let compiled_right =
                    self.add_dedup_graph_cached(right, output_memo, compiled_memo)?;
                let output = inferred_output;
                let left_descriptor = compiled_left.output;
                let right_descriptor = compiled_right.output;
                let left_key = left_on
                    .iter()
                    .map(|field| field_ref_name(&left_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let right_key = right_on
                    .iter()
                    .map(|field| field_ref_name(&right_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let left_arrangement = self.add_arrangement_node(
                    compiled_left.node,
                    left_descriptor,
                    plan_expr_names(&left_key),
                    *comparison,
                );
                let right_arrangement = self.add_arrangement_node(
                    compiled_right.node,
                    right_descriptor,
                    plan_expr_names(&right_key),
                    *comparison,
                );
                let node_descriptor = NodeDescriptor::new(
                    OpType::Join(JoinOp {
                        kind: JoinOpKind::Inner,
                        left_key,
                        right_key,
                        left_descriptor,
                        right_descriptor,
                        residual_predicate: None,
                        comparison: *comparison,
                    }),
                    [left_arrangement, right_arrangement],
                    output,
                );
                let node = self
                    .graph
                    .dedup_node(node_descriptor, NodeDurability::Ephemeral);
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    // Jazz lowers the public/root relation on the left and
                    // nested relation inputs on the right. Prefer the public
                    // side so a nested TopBy cannot become root ordering.
                    root_ordering_node: compiled_left
                        .root_ordering_node
                        .or(compiled_right.root_ordering_node),
                })
            }
            GraphBuilder::SemiJoin {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => {
                let compiled_left =
                    self.add_dedup_graph_cached(left, output_memo, compiled_memo)?;
                let compiled_right =
                    self.add_dedup_graph_cached(right, output_memo, compiled_memo)?;
                let output = inferred_output;
                let left_descriptor = compiled_left.output;
                let right_descriptor = compiled_right.output;
                let left_key = left_on
                    .iter()
                    .map(|field| field_ref_name(&left_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let right_key = right_on
                    .iter()
                    .map(|field| field_ref_name(&right_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let left_arrangement = self.add_arrangement_node(
                    compiled_left.node,
                    left_descriptor,
                    plan_expr_names(&left_key),
                    *comparison,
                );
                let right_arrangement = self.add_arrangement_node(
                    compiled_right.node,
                    right_descriptor,
                    plan_expr_names(&right_key),
                    *comparison,
                );
                let node_descriptor = NodeDescriptor::new(
                    OpType::SemiJoin(JoinOp {
                        kind: JoinOpKind::Inner,
                        left_key,
                        right_key,
                        left_descriptor,
                        right_descriptor,
                        residual_predicate: None,
                        comparison: *comparison,
                    }),
                    [left_arrangement, right_arrangement],
                    output,
                );
                let node = self
                    .graph
                    .dedup_node(node_descriptor, NodeDurability::Ephemeral);
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_left
                        .root_ordering_node
                        .or(compiled_right.root_ordering_node),
                })
            }
            GraphBuilder::AntiJoin {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => {
                let compiled_left =
                    self.add_dedup_graph_cached(left, output_memo, compiled_memo)?;
                let compiled_right =
                    self.add_dedup_graph_cached(right, output_memo, compiled_memo)?;
                let output = inferred_output;
                let left_descriptor = compiled_left.output;
                let right_descriptor = compiled_right.output;
                let left_key = left_on
                    .iter()
                    .map(|field| field_ref_name(&left_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let right_key = right_on
                    .iter()
                    .map(|field| field_ref_name(&right_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let left_arrangement = self.add_arrangement_node(
                    compiled_left.node,
                    left_descriptor,
                    plan_expr_names(&left_key),
                    *comparison,
                );
                let right_arrangement = self.add_arrangement_node(
                    compiled_right.node,
                    right_descriptor,
                    plan_expr_names(&right_key),
                    *comparison,
                );
                let node_descriptor = NodeDescriptor::new(
                    OpType::AntiJoin(JoinOp {
                        kind: JoinOpKind::Inner,
                        left_key,
                        right_key,
                        left_descriptor,
                        right_descriptor,
                        residual_predicate: None,
                        comparison: *comparison,
                    }),
                    [left_arrangement, right_arrangement],
                    output,
                );
                let node = self
                    .graph
                    .dedup_node(node_descriptor, NodeDurability::Ephemeral);
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_left
                        .root_ordering_node
                        .or(compiled_right.root_ordering_node),
                })
            }
            _ => unreachable!("dispatcher routes only join graph builders here"),
        }
    }

    fn add_collect_by_graph(
        &mut self,
        input: &GraphBuilder,
        collect: &CollectByBuilder,
        output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
        compiled_memo: &mut HashMap<usize, CompiledNode>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        let compiled_input = self.add_dedup_graph_cached(input, output_memo, compiled_memo)?;
        let input_output = compiled_input.output;
        let group_field_indices = collect
            .group_cols
            .iter()
            .map(|field| resolve_field_ref(&input_output, field))
            .collect::<Result<Vec<_>, _>>()?;
        let parent_fields = collect_by_projections(&input_output, &collect.parent_fields)?;
        if matches!(collect.mode, CollectByMode::Collect | CollectByMode::Root)
            && (!collect.slots.is_empty() || collect.mode == CollectByMode::Root)
        {
            if parent_fields.is_empty() {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "tree collect requires a parent projection".into(),
                ));
            }
            validate_collect_by_key_types(&input_output, &group_field_indices)?;
            let slots = collect_by_slots(&input_output, &collect.slots, &group_field_indices, 1)?;
            let order_field_indices = if collect.order_cols.is_empty() {
                group_field_indices.clone()
            } else {
                collect
                    .order_cols
                    .iter()
                    .map(|order| resolve_field_ref(&input_output, &order.field))
                    .collect::<Result<Vec<_>, _>>()?
            };
            let tie_field_indices = if collect.tie_cols.is_empty() {
                group_field_indices.clone()
            } else {
                collect
                    .tie_cols
                    .iter()
                    .map(|field| resolve_field_ref(&input_output, field))
                    .collect::<Result<Vec<_>, _>>()?
            };
            validate_collect_by_key_types(&input_output, &order_field_indices)?;
            validate_collect_by_key_types(&input_output, &tie_field_indices)?;
            let group_fields = group_field_indices
                .iter()
                .map(|field| field_name_at(&input_output, *field))
                .collect::<Result<Vec<_>, _>>()?;
            let arrangement = self.add_arrangement_node(
                compiled_input.node,
                input_output,
                group_fields.clone(),
                ValueComparison::Exact,
            );
            let node = self.graph.dedup_node(
                NodeDescriptor::new(
                    OpType::CollectBy(Box::new(CollectByOp {
                        mode: collect.mode,
                        group_fields,
                        group_field_indices,
                        parent_fields,
                        child_fields: Vec::new(),
                        child_descriptor: RecordDescriptor::new(Vec::<(String, ValueType)>::new()),
                        collection_field: String::new(),
                        collection_field_index: 0,
                        slots,
                        tuple_fields: Vec::new(),
                        occurrence_id_fields: Vec::new(),
                        occurrence_id_field_indices: Vec::new(),
                        order_fields: order_field_indices
                            .iter()
                            .enumerate()
                            .map(|(index, field_idx)| {
                                Ok(TopByOrderField {
                                    field: field_name_at(&input_output, *field_idx)?,
                                    direction: collect
                                        .order_cols
                                        .get(index)
                                        .map_or(TopByDirection::Asc, |order| order.direction),
                                })
                            })
                            .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
                        tie_fields: tie_field_indices
                            .iter()
                            .map(|field| field_name_at(&input_output, *field))
                            .collect::<Result<Vec<_>, _>>()?,
                        sort_field_indices: order_field_indices
                            .iter()
                            .chain(&tie_field_indices)
                            .copied()
                            .collect(),
                        sort_directions: order_field_indices
                            .iter()
                            .enumerate()
                            .map(|(index, _)| {
                                collect
                                    .order_cols
                                    .get(index)
                                    .map_or(TopByDirection::Asc, |order| order.direction)
                            })
                            .chain(std::iter::repeat_n(
                                TopByDirection::Asc,
                                tie_field_indices.len(),
                            ))
                            .collect(),
                        offset: collect.offset,
                        limit: collect.limit,
                    })),
                    [arrangement],
                    output,
                ),
                NodeDurability::Ephemeral,
            );
            self.initialize_node_runtime(node);
            return Ok(CompiledNode {
                output,
                node,
                root_ordering_node: compiled_input.root_ordering_node,
            });
        }
        let child_fields = collect_by_projections(&input_output, &collect.child_fields)?;
        let tuple_fields = collect_by_projections(&input_output, &collect.tuple_fields)?;
        let occurrence_id_field_indices = collect
            .occurrence_id_cols
            .iter()
            .map(|field| resolve_field_ref(&input_output, field))
            .collect::<Result<Vec<_>, _>>()?;
        // Parent values must be determined by the group, rather than by
        // whichever child happens to be selected.
        if collect.mode == CollectByMode::Collect
            && parent_fields
                .iter()
                .any(|field| !group_field_indices.contains(&field.field_idx))
        {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "parent projection fields must be grouping fields".into(),
            ));
        }
        validate_collect_by_key_types(&input_output, &group_field_indices)?;
        validate_collect_by_key_types(&input_output, &occurrence_id_field_indices)?;
        let order_field_indices = collect
            .order_cols
            .iter()
            .map(|order| resolve_field_ref(&input_output, &order.field))
            .collect::<Result<Vec<_>, _>>()?;
        let tie_field_indices = collect
            .tie_cols
            .iter()
            .map(|field| resolve_field_ref(&input_output, field))
            .collect::<Result<Vec<_>, _>>()?;
        if order_field_indices.is_empty() || tie_field_indices.is_empty() {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "order and tie fields must both be complete and non-empty".into(),
            ));
        }
        validate_collect_by_key_types(&input_output, &order_field_indices)?;
        validate_collect_by_key_types(&input_output, &tie_field_indices)?;
        let group_fields = group_field_indices
            .iter()
            .map(|field| field_name_at(&input_output, *field))
            .collect::<Result<Vec<_>, _>>()?;
        let order_fields = collect
            .order_cols
            .iter()
            .zip(&order_field_indices)
            .map(|(order, field_idx)| {
                Ok(TopByOrderField {
                    field: field_name_at(&input_output, *field_idx)?,
                    direction: order.direction,
                })
            })
            .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
        let tie_fields = tie_field_indices
            .iter()
            .map(|field| field_name_at(&input_output, *field))
            .collect::<Result<Vec<_>, _>>()?;
        let sort_field_indices = order_field_indices
            .iter()
            .chain(&tie_field_indices)
            .copied()
            .collect::<Vec<_>>();
        let sort_directions = collect
            .order_cols
            .iter()
            .map(|order| order.direction)
            .chain(std::iter::repeat_n(
                TopByDirection::Asc,
                tie_field_indices.len(),
            ))
            .collect::<Vec<_>>();
        let child_descriptor = RecordDescriptor::new(child_fields.iter().map(|field| {
            let value_type = input_output.fields()[field.field_idx].value_type.clone();
            (field.output_name.clone(), value_type)
        }));
        if collect.mode == CollectByMode::Expand
            && (tuple_fields.is_empty() || occurrence_id_field_indices.is_empty())
        {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "expand mode requires a tuple projection and ordered occurrence-id fields".into(),
            ));
        }
        let collection_field_index = parent_fields.len();
        let arrangement = self.add_arrangement_node(
            compiled_input.node,
            input_output,
            group_fields.clone(),
            ValueComparison::Exact,
        );
        let node = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::CollectBy(Box::new(CollectByOp {
                    mode: collect.mode,
                    group_fields,
                    group_field_indices,
                    parent_fields,
                    child_fields,
                    child_descriptor,
                    collection_field: collect.collection_field.clone(),
                    collection_field_index,
                    slots: Vec::new(),
                    tuple_fields,
                    occurrence_id_fields: occurrence_id_field_indices
                        .iter()
                        .map(|field| field_name_at(&input_output, *field))
                        .collect::<Result<Vec<_>, _>>()?,
                    occurrence_id_field_indices,
                    order_fields,
                    tie_fields,
                    sort_field_indices,
                    sort_directions,
                    offset: collect.offset,
                    limit: collect.limit,
                })),
                [arrangement],
                output,
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(node);
        Ok(CompiledNode {
            output,
            node,
            // CollectBy changes representation, not the identity/order of
            // public roots selected upstream.
            root_ordering_node: compiled_input.root_ordering_node,
        })
    }

    pub(super) fn add_dedup_schema_index(
        &mut self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<NodeId, IvmRuntimeError> {
        self.logical_nodes_requested += 3;
        let (table_descriptor, variant_projection) = if table.has_variants() {
            (
                schema_index_input_descriptor(table, index)?,
                Some(VariantProjectionTarget::SchemaIndex(index.name.clone())),
            )
        } else {
            (table.record_schema(), None)
        };
        let input = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::TableSource(TableSourceOp {
                    table: table.name.clone(),
                    scan: None,
                    variant_projection,
                }),
                [],
                table_descriptor,
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(input);

        let CompiledNode {
            output: index_descriptor,
            node: index_by,
            ..
        } = self.add_dedup_index_by_from_input(table, index, input, table_descriptor, None)?;

        let storage = DurableStorage {
            column_family: "indices".to_owned(),
            key_prefix: durable_index_key_prefix(&table.name, &index.name),
        };
        let persist = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::Persist(PersistOp {
                    name: index.name.clone(),
                    storage: storage.clone(),
                    key_fields: vec![0],
                    unique: index.unique,
                }),
                [index_by],
                index_descriptor,
            ),
            NodeDurability::Durable { storage },
        );
        self.add_retainer(
            persist,
            Retainer::DurableSchemaObject(format!("{}.{}", table.name, index.name)),
        );
        self.initialize_node_runtime(persist);

        Ok(persist)
    }

    fn index_source_op(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        scan: Option<StaticScanSpec>,
        intersections: Vec<(String, StaticScanSpec)>,
        row_projection: Option<String>,
    ) -> Result<IndexSourceOp, IvmRuntimeError> {
        for (intersection, _) in &intersections {
            if !table
                .indices
                .iter()
                .any(|index| index.name == *intersection)
            {
                return Err(IvmRuntimeError::IndexNotFound(intersection.clone()));
            }
        }
        let (table_descriptor, variant_projection) = if table.has_variants() {
            (
                schema_index_input_descriptor(table, index)?,
                Some(VariantProjectionTarget::SchemaIndex(index.name.clone())),
            )
        } else {
            (table.record_schema(), None)
        };
        let key_fields = index
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let primary_key = table
            .primary_key
            .as_ref()
            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.name.clone()))?;
        let value_fields = primary_key
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(&column.column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let index_key_covers_primary_key = primary_key
            .columns
            .iter()
            .all(|primary_key_column| index.columns.contains(&primary_key_column.column));

        Ok(IndexSourceOp {
            table: table.name.clone(),
            index: index.name.clone(),
            intersections,
            input_descriptor: table_descriptor,
            variant_projection,
            row_projection: row_projection.map(VariantProjectionTarget::Named),
            key_fields,
            value_fields,
            unique: index.unique || index_key_covers_primary_key,
            append_value_to_key: !index.unique && !index_key_covers_primary_key,
            store_value: index.unique && !index_key_covers_primary_key,
            scan,
        })
    }

    fn add_dedup_index_by_from_input(
        &mut self,
        table: &TableSchema,
        index: &IndexSchema,
        input: NodeId,
        table_descriptor: RecordDescriptor,
        scan: Option<StaticScanSpec>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        let index_descriptor = index_record_descriptor();
        let key_fields = index
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let primary_key = table
            .primary_key
            .as_ref()
            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.name.clone()))?;
        let primary_key_fields = primary_key
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(&column.column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let index_key_covers_primary_key = primary_key
            .columns
            .iter()
            .all(|primary_key_column| index.columns.contains(&primary_key_column.column));

        let node = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::IndexBy(IndexByOp {
                    key_expressions: index
                        .columns
                        .iter()
                        .map(|column| PlanExpr::field(column.clone()))
                        .collect(),
                    value_expressions: primary_key
                        .columns
                        .iter()
                        .map(|column| PlanExpr::field(column.column.clone()))
                        .collect(),
                    explicit_index: Some(index.clone()),
                    key_fields,
                    value_fields: primary_key_fields,
                    unique: index.unique || index_key_covers_primary_key,
                    append_value_to_key: !index.unique && !index_key_covers_primary_key,
                    store_value: index.unique && !index_key_covers_primary_key,
                    scan,
                }),
                [input],
                index_descriptor,
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(node);
        Ok(CompiledNode {
            output: index_descriptor,
            node,
            root_ordering_node: None,
        })
    }
}

fn graph_builder_key(graph: &GraphBuilder) -> usize {
    std::ptr::from_ref(graph).addr()
}
