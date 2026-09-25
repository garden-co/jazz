//! Compile a whole typed family; bind sources and predicates without re-lowering.
use super::*;
use crate::ivm::template::{
    CompiledForest, TemplateNode, TemplateNodeRef, TemplateSlice, TypedGraphTemplate,
    TypedGraphTemplateCache,
};

static NEXT_TEMPLATE: AtomicU64 = AtomicU64::new(1);

/// All terminal families of one lowered program. Terminals share fragments;
/// compiling them together preserves that sharing, as ordinary installation
/// does, instead of recompiling each common fragment once per terminal.
struct Forest {
    roots: Vec<GraphBuilder>,
    inputs: Vec<Arc<GraphBuilder>>,
    contracts: Vec<RecordDescriptor>,
    predicates: Vec<PredicateExpr>,
    markers: Vec<PredicateExpr>,
}

/// One terminal's view of a forest: its local fallback graph and the forest
/// slots it owns, in local slot order. Nothing here is value-dependent.
struct Slice {
    fallback: GraphBuilder,
    inputs: Vec<usize>,
    predicates: Vec<usize>,
}

// Preserve dependency information while distinguishing equal-valued arguments.
// These are compiler expressions, never valid executable constant predicates.
fn predicate_marker(slot: usize, predicate: &PredicateExpr) -> PredicateExpr {
    let mut fields = BTreeSet::new();
    predicate.referenced_fields(&mut fields);
    PredicateExpr::TemplateArgument {
        slot: slot as u32,
        fields: fields.into_iter().collect(),
    }
}

fn marker_slot(predicate: &PredicateExpr) -> Option<usize> {
    match predicate {
        PredicateExpr::TemplateArgument { slot, .. } => Some(*slot as usize),
        _ => None,
    }
}

fn forest(
    roots: &[&GraphBuilder],
    describe: &impl Fn(&GraphBuilder) -> Result<RecordDescriptor, IvmRuntimeError>,
) -> Result<Forest, IvmRuntimeError> {
    let mut forest = Forest {
        roots: Vec::with_capacity(roots.len()),
        inputs: Vec::new(),
        contracts: Vec::new(),
        predicates: Vec::new(),
        markers: Vec::new(),
    };
    // One pointer memo across roots: a fragment shared by several terminals
    // keeps one slot numbering and one rewritten node.
    let mut rewritten = HashMap::<usize, Arc<GraphBuilder>>::default();
    for root in roots {
        let mut pending = vec![(*root, false)];
        while let Some((node, expanded)) = pending.pop() {
            let key = node as *const GraphBuilder as usize;
            if rewritten.contains_key(&key) {
                continue;
            }
            let output = match node {
                GraphBuilder::TemplateInput {
                    output,
                    input: None,
                    ..
                }
                | GraphBuilder::BindingSource { output, .. }
                | GraphBuilder::InlineRecords { output, .. }
                | GraphBuilder::InputSource { output, .. }
                | GraphBuilder::FrontierSource { output, .. } => Some(*output),
                GraphBuilder::ArgMaxBy { input, .. } | GraphBuilder::ArgMinBy { input, .. }
                    if matches!(input.as_ref(), GraphBuilder::Table { .. }) =>
                {
                    Some(describe(node)?)
                }
                GraphBuilder::Table { .. }
                | GraphBuilder::Index { .. }
                | GraphBuilder::Recursive { .. }
                | GraphBuilder::RecursiveStepWitness { .. }
                | GraphBuilder::VariantProject { .. } => Some(describe(node)?),
                GraphBuilder::TypedTemplate { .. } | GraphBuilder::CollectBy { .. } => {
                    return Err(IvmRuntimeError::UnsupportedOperator);
                }
                _ => None,
            };
            if let Some(output) = output {
                let slot = forest.inputs.len() as u32;
                forest.inputs.push(Arc::new(node.clone()));
                forest.contracts.push(output);
                rewritten.insert(
                    key,
                    Arc::new(GraphBuilder::TemplateInput {
                        slot,
                        output,
                        input: None,
                    }),
                );
                continue;
            }
            if !expanded {
                pending.push((node, true));
                node.visit_inputs(|child| pending.push((child, false)));
                continue;
            }
            let mut bound =
                node.map_inputs(|child| rewritten[&(Arc::as_ptr(child) as usize)].clone());
            if let GraphBuilder::Filter { predicate, .. } = &mut bound {
                let marker = predicate_marker(forest.predicates.len(), predicate);
                forest
                    .predicates
                    .push(std::mem::replace(predicate, marker.clone()));
                forest.markers.push(marker);
            }
            rewritten.insert(key, Arc::new(bound));
        }
        forest
            .roots
            .push((*rewritten[&(*root as *const GraphBuilder as usize)]).clone());
    }
    Ok(forest)
}

/// Renumber one terminal's forest slots densely. Every forest filter carries
/// a unique marker, and every unbound leaf is a forest input slot.
fn slice(root: &GraphBuilder, markers: &[PredicateExpr]) -> Result<Slice, IvmRuntimeError> {
    let mut inputs = HashMap::<usize, usize>::default();
    let mut predicates = HashMap::<usize, usize>::default();
    let mut input_order = Vec::new();
    let mut predicate_order = Vec::new();
    let mut rewritten = HashMap::<usize, Arc<GraphBuilder>>::default();
    let mut pending = vec![(root, false)];
    while let Some((node, expanded)) = pending.pop() {
        let key = node as *const GraphBuilder as usize;
        if rewritten.contains_key(&key) {
            continue;
        }
        if let GraphBuilder::TemplateInput {
            slot,
            output,
            input: None,
        } = node
        {
            let global = *slot as usize;
            let local = *inputs.entry(global).or_insert_with(|| {
                input_order.push(global);
                input_order.len() - 1
            });
            rewritten.insert(
                key,
                Arc::new(GraphBuilder::TemplateInput {
                    slot: local as u32,
                    output: *output,
                    input: None,
                }),
            );
            continue;
        }
        if !expanded {
            pending.push((node, true));
            node.visit_inputs(|child| pending.push((child, false)));
            continue;
        }
        let mut bound = node.map_inputs(|child| rewritten[&(Arc::as_ptr(child) as usize)].clone());
        if let GraphBuilder::Filter { predicate, .. } = &mut bound {
            let global = marker_slot(predicate)
                .filter(|slot| markers.get(*slot) == Some(predicate))
                .ok_or(IvmRuntimeError::UnsupportedOperator)?;
            let local = *predicates.entry(global).or_insert_with(|| {
                predicate_order.push(global);
                predicate_order.len() - 1
            });
            *predicate = predicate_marker_with_slot(local, predicate);
        }
        rewritten.insert(key, Arc::new(bound));
    }
    Ok(Slice {
        fallback: (*rewritten[&(root as *const GraphBuilder as usize)]).clone(),
        inputs: input_order,
        predicates: predicate_order,
    })
}

fn predicate_marker_with_slot(slot: usize, marker: &PredicateExpr) -> PredicateExpr {
    match marker {
        PredicateExpr::TemplateArgument { fields, .. } => PredicateExpr::TemplateArgument {
            slot: slot as u32,
            fields: fields.clone(),
        },
        other => other.clone(),
    }
}

fn forest_fingerprint(roots: &[GraphBuilder]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    roots.len().hash(&mut hasher);
    for root in roots {
        graph_builder_fingerprint(root).hash(&mut hasher);
    }
    hasher.finish()
}

impl TypedGraphTemplateCache {
    /// Compiler reuse has one exact whole-program lookup, not a recipe lookup
    /// per operator. Each returned graph owns fresh source/predicate arguments.
    pub fn compile(
        &mut self,
        graphs: &[GraphBuilder],
    ) -> Result<Vec<GraphBuilder>, IvmRuntimeError> {
        self.compile_with_sources(graphs, |_| Err(IvmRuntimeError::UnsupportedOperator))
    }

    /// Extend families through source projections while leaving catalogue-
    /// sensitive leaves on their ordinary, validated installation path.
    /// Descriptors supplied here are declarations, checked again at install.
    pub fn compile_with_sources(
        &mut self,
        graphs: &[GraphBuilder],
        describe: impl Fn(&GraphBuilder) -> Result<RecordDescriptor, IvmRuntimeError>,
    ) -> Result<Vec<GraphBuilder>, IvmRuntimeError> {
        // Binding-route filters must remain below an explicit terminal.
        let mut roots = Vec::with_capacity(graphs.len());
        for graph in graphs {
            validate_collect_by_terminality(graph)?;
            roots.push(match graph {
                GraphBuilder::CollectBy { input, .. } => {
                    validate_collect_by_terminality(input)?;
                    input.as_ref()
                }
                graph => graph,
            });
        }
        let forest = forest(&roots, &describe)?;
        let fingerprint = forest_fingerprint(&forest.roots);
        let compiled = if let Some((_, _, compiled)) =
            self.entries.iter().find(|(hash, roots, _)| {
                *hash == fingerprint
                    && roots.len() == forest.roots.len()
                    && roots
                        .iter()
                        .zip(&forest.roots)
                        .all(|(a, b)| graph_builders_equal(a, b))
            }) {
            self.reuses += 1;
            compiled.clone()
        } else {
            let mut compiler = IvmRuntime::new(DatabaseSchema::new([]))?;
            let compiled: CompiledForest = compiler.compile_typed_forest(&forest)?.into();
            self.compilations += 1;
            if self.entries.len() == 128 {
                self.entries.pop_front();
            }
            self.entries
                .push_back((fingerprint, forest.roots, compiled.clone()));
            compiled
        };
        Ok(graphs
            .iter()
            .zip(compiled.iter())
            .map(|(graph, (program, slice))| {
                let typed = GraphBuilder::TypedTemplate {
                    program: program.clone(),
                    inputs: slice
                        .inputs
                        .iter()
                        .map(|slot| forest.inputs[*slot].clone())
                        .collect(),
                    predicates: slice
                        .predicates
                        .iter()
                        .map(|slot| forest.predicates[*slot].clone())
                        .collect(),
                    scalars: Arc::from([]),
                };
                match graph {
                    GraphBuilder::CollectBy { .. } => {
                        let typed = Arc::new(typed);
                        graph.map_inputs(|_| typed.clone())
                    }
                    _ => typed,
                }
            })
            .collect())
    }
}

impl IvmRuntime {
    fn compile_typed_forest(
        &mut self,
        forest: &Forest,
    ) -> Result<Vec<(Arc<TypedGraphTemplate>, TemplateSlice)>, IvmRuntimeError> {
        let mut sources = HashMap::<NodeId, usize>::default();
        let mut replacements = Vec::new();
        for (slot, output) in forest.contracts.iter().enumerate() {
            let id = self.allocate_input_source(*output);
            let replacement = GraphBuilder::input_source(id, *output);
            sources.insert(self.add_dedup_graph(&replacement)?.node, slot);
            replacements.push(crate::ivm::TemplateGraphInput::with_output_contract(
                replacement,
                *output,
            ));
        }
        // One forest bind and one scratch graph: shared fragments keep one
        // builder identity, so the compilation cache compiles them once.
        let bound = crate::ivm::template::bind_template_graphs_inner(
            &forest.roots,
            &replacements,
            true,
            None,
        )
        .map_err(|_| IvmRuntimeError::GraphOutputMismatch)?;
        let mut programs = Vec::with_capacity(bound.len());
        for (root, graph) in forest.roots.iter().zip(&bound) {
            let compiled = self.add_dedup_template_graph(graph)?;
            let slice = slice(root, &forest.markers)?;
            let program = self.extract_typed_template(&compiled, &sources, &slice, forest)?;
            programs.push((
                Arc::new(program),
                TemplateSlice {
                    inputs: slice.inputs,
                    predicates: slice.predicates,
                },
            ));
        }
        Ok(programs)
    }

    fn extract_typed_template(
        &self,
        compiled: &CompiledNode,
        sources: &HashMap<NodeId, usize>,
        slice: &Slice,
        forest: &Forest,
    ) -> Result<TypedGraphTemplate, IvmRuntimeError> {
        let local_inputs = slice
            .inputs
            .iter()
            .enumerate()
            .map(|(local, global)| (*global, local))
            .collect::<HashMap<_, _>>();
        let local_predicates = slice
            .predicates
            .iter()
            .enumerate()
            .map(|(local, global)| (*global, local))
            .collect::<HashMap<_, _>>();
        let mut references = HashMap::<NodeId, TemplateNodeRef>::default();
        let mut nodes = Vec::new();
        let mut pending = vec![(compiled.node, false)];
        if let Some(ordering) = compiled.root_ordering_node {
            pending.push((ordering, false));
        }
        while let Some((id, expanded)) = pending.pop() {
            if references.contains_key(&id) {
                continue;
            }
            if let Some(global) = sources.get(&id) {
                // A source outside this terminal's slice cannot be reachable:
                // input source ids are unique per forest slot.
                let local = local_inputs
                    .get(global)
                    .ok_or(IvmRuntimeError::UnsupportedOperator)?;
                references.insert(id, TemplateNodeRef::Input(*local));
                continue;
            }
            let descriptor = &self
                .graph
                .node(id)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(id))?
                .descriptor;
            if !expanded {
                pending.push((id, true));
                pending.extend(descriptor.inputs.iter().map(|id| (*id, false)));
                continue;
            }
            let inputs = descriptor.inputs.iter().map(|id| references[id]).collect();
            let mut descriptor = descriptor.clone();
            descriptor.inputs.clear();
            let predicate = if let OpType::Filter(filter) = &mut descriptor.operator {
                let local = marker_slot(&filter.predicate)
                    .filter(|slot| forest.markers.get(*slot) == Some(&filter.predicate))
                    .and_then(|slot| local_predicates.get(&slot))
                    .ok_or(IvmRuntimeError::UnsupportedOperator)?;
                filter.predicate = PredicateExpr::And(Vec::new());
                Some(*local)
            } else {
                None
            };
            references.insert(id, TemplateNodeRef::Node(nodes.len()));
            nodes.push(TemplateNode {
                descriptor,
                inputs,
                predicate,
            });
        }
        Ok(TypedGraphTemplate {
            identity: NEXT_TEMPLATE.fetch_add(1, Ordering::Relaxed),
            nodes,
            inputs: slice
                .inputs
                .iter()
                .map(|slot| forest.contracts[*slot])
                .collect(),
            output: compiled.output,
            root: references[&compiled.node],
            ordering: compiled.root_ordering_node.map(|id| references[&id]),
            terminal: false,
            fallback: slice.fallback.clone(),
            predicate_markers: slice
                .predicates
                .iter()
                .enumerate()
                .map(|(local, global)| predicate_marker_with_slot(local, &forest.markers[*global]))
                .collect(),
        })
    }

    pub(super) fn install_typed_template(
        &mut self,
        program: &TypedGraphTemplate,
        inputs: &[CompiledNode],
        graphs: &[Arc<GraphBuilder>],
        predicates: &[PredicateExpr],
        scalars: &[crate::ivm::TemplateScalarArgument],
    ) -> Result<CompiledNode, IvmRuntimeError> {
        if inputs.len() != program.inputs.len()
            || program
                .predicate_markers
                .iter()
                .zip(predicates)
                .any(|(marker, argument)| !marker.accepts_template_argument(argument))
            || predicates.len() != program.predicate_markers.len()
            || inputs
                .iter()
                .zip(&program.inputs)
                .any(|(input, output)| input.output != *output)
        {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        // Until inherited root ordering has an explicit relocation, preserve
        // the full compiler rather than silently dropping source ordering.
        if inputs
            .iter()
            .any(|input| input.root_ordering_node.is_some())
        {
            let bound = program
                .bind_declarative_with_arguments(graphs, predicates, scalars)
                .map_err(|_| IvmRuntimeError::GraphOutputMismatch)?;
            return self.add_dedup_graph(&bound);
        }
        let mut installed = Vec::with_capacity(program.nodes.len());
        let resolve = |reference, installed: &[NodeId]| match reference {
            TemplateNodeRef::Input(i) => inputs[i].node,
            TemplateNodeRef::Node(i) => installed[i],
        };
        for instruction in &program.nodes {
            let inputs = instruction
                .inputs
                .iter()
                .map(|r| resolve(*r, &installed))
                .collect();
            // Only argument-consuming operators need an instance copy; every
            // other node is hashed and compared from the program itself.
            let binds_scalars = matches!(
                &instruction.descriptor.operator,
                OpType::MapProject(project) if project.expressions.iter().any(|expression| {
                    matches!(expression.expression, ProjectExpr::TemplateArgument { .. })
                })
            );
            let bound;
            let operator = if instruction.predicate.is_some() || binds_scalars {
                let mut operator = instruction.descriptor.operator.clone();
                if let (Some(slot), OpType::Filter(filter)) = (instruction.predicate, &mut operator)
                {
                    filter.predicate = predicates[slot].clone();
                }
                if let OpType::MapProject(project) = &mut operator {
                    for expression in &mut project.expressions {
                        crate::ivm::template::arguments::bind_project_expression(
                            &mut expression.expression,
                            scalars,
                        )
                        .map_err(|_| IvmRuntimeError::GraphOutputMismatch)?;
                    }
                }
                bound = operator;
                &bound
            } else {
                &instruction.descriptor.operator
            };
            self.logical_nodes_requested += 1;
            let id = self.graph.dedup_node_parts(
                operator,
                inputs,
                instruction.descriptor.output,
                NodeDurability::Ephemeral,
            );
            self.initialize_node_runtime(id);
            installed.push(id);
        }
        Ok(CompiledNode {
            node: resolve(program.root, &installed),
            output: program.output,
            root_ordering_node: program.ordering.map(|r| resolve(r, &installed)),
        })
    }
}
