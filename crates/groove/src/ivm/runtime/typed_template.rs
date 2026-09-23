//! Compile a whole typed family; bind sources and predicates without re-lowering.
use super::*;
use crate::ivm::template::{
    TemplateNode, TemplateNodeRef, TypedGraphTemplate, TypedGraphTemplateCache,
};

static NEXT_TEMPLATE: AtomicU64 = AtomicU64::new(1);

struct Family {
    graph: GraphBuilder,
    inputs: Vec<Arc<GraphBuilder>>,
    contracts: Vec<RecordDescriptor>,
    predicates: Vec<PredicateExpr>,
    markers: Vec<PredicateExpr>,
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

fn family(
    graph: &GraphBuilder,
    describe: &impl Fn(&GraphBuilder) -> Result<RecordDescriptor, IvmRuntimeError>,
) -> Result<Family, IvmRuntimeError> {
    let mut family = Family {
        graph: graph.clone(),
        inputs: Vec::new(),
        contracts: Vec::new(),
        predicates: Vec::new(),
        markers: Vec::new(),
    };
    let mut rewritten = HashMap::<usize, Arc<GraphBuilder>>::default();
    let mut pending = vec![(graph, false)];
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
            let slot = family.inputs.len() as u32;
            family.inputs.push(Arc::new(node.clone()));
            family.contracts.push(output);
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
        let mut bound = node.map_inputs(|child| rewritten[&(Arc::as_ptr(child) as usize)].clone());
        if let GraphBuilder::Filter { predicate, .. } = &mut bound {
            let marker = predicate_marker(family.predicates.len(), predicate);
            family
                .predicates
                .push(std::mem::replace(predicate, marker.clone()));
            family.markers.push(marker);
        }
        rewritten.insert(key, Arc::new(bound));
    }
    family.graph = (*rewritten[&(graph as *const GraphBuilder as usize)]).clone();
    Ok(family)
}

impl TypedGraphTemplateCache {
    /// Compiler reuse has one exact whole-family lookup, not a recipe lookup
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
        graphs
            .iter()
            .map(|graph| self.compile_graph(graph, &describe))
            .collect()
    }

    fn compile_graph(
        &mut self,
        graph: &GraphBuilder,
        describe: &impl Fn(&GraphBuilder) -> Result<RecordDescriptor, IvmRuntimeError>,
    ) -> Result<GraphBuilder, IvmRuntimeError> {
        validate_collect_by_terminality(graph)?;
        // Binding-route filters must remain below this explicit terminal.
        if let GraphBuilder::CollectBy { input, .. } = graph {
            let input = Arc::new(self.compile_graph(input, describe)?);
            return Ok(graph.map_inputs(|_| input.clone()));
        }
        let family = family(graph, describe)?;
        let fingerprint = graph_builder_fingerprint(&family.graph);
        let program = if let Some((_, _, program)) = self.entries.iter().find(|(hash, graph, _)| {
            *hash == fingerprint && graph_builders_equal(graph, &family.graph)
        }) {
            self.reuses += 1;
            program.clone()
        } else {
            let mut compiler = IvmRuntime::new(DatabaseSchema::new([]))?;
            let program = Arc::new(compiler.compile_typed_family(&family)?);
            self.compilations += 1;
            if self.entries.len() == 128 {
                self.entries.pop_front();
            }
            self.entries
                .push_back((fingerprint, family.graph, program.clone()));
            program
        };
        Ok(GraphBuilder::TypedTemplate {
            program,
            inputs: family.inputs,
            predicates: family.predicates,
            scalars: Arc::from([]),
        })
    }
}

impl IvmRuntime {
    fn compile_typed_family(
        &mut self,
        family: &Family,
    ) -> Result<TypedGraphTemplate, IvmRuntimeError> {
        let mut source_nodes = Vec::new();
        let mut replacements = Vec::new();
        for output in &family.contracts {
            let id = self.allocate_input_source(*output);
            let replacement = GraphBuilder::input_source(id, *output);
            source_nodes.push(self.add_dedup_graph(&replacement)?.node);
            replacements.push(crate::ivm::TemplateGraphInput::with_output_contract(
                replacement,
                *output,
            ));
        }
        let bound = crate::ivm::template::bind_template_graphs_inner(
            std::slice::from_ref(&family.graph),
            &replacements,
            true,
            None,
        )
        .map_err(|_| IvmRuntimeError::GraphOutputMismatch)?
        .remove(0);
        let compiled = self.add_dedup_template_graph(&bound)?;
        let mut references = source_nodes
            .into_iter()
            .enumerate()
            .map(|(i, node)| (node, TemplateNodeRef::Input(i)))
            .collect::<HashMap<_, _>>();
        let mut nodes = Vec::new();
        let mut pending = vec![(compiled.node, false)];
        if let Some(ordering) = compiled.root_ordering_node {
            pending.push((ordering, false));
        }
        while let Some((id, expanded)) = pending.pop() {
            if references.contains_key(&id) {
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
                let slot = family
                    .markers
                    .iter()
                    .position(|marker| marker == &filter.predicate)
                    .ok_or(IvmRuntimeError::UnsupportedOperator)?;
                filter.predicate = PredicateExpr::And(Vec::new());
                Some(slot)
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
            inputs: family.contracts.clone(),
            output: compiled.output,
            root: references[&compiled.node],
            ordering: compiled.root_ordering_node.map(|id| references[&id]),
            terminal: false,
            fallback: family.graph.clone(),
            predicate_markers: family.markers.clone(),
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
            let mut descriptor = instruction.descriptor.clone();
            descriptor.inputs = instruction
                .inputs
                .iter()
                .map(|r| resolve(*r, &installed))
                .collect();
            if let (Some(slot), OpType::Filter(filter)) =
                (instruction.predicate, &mut descriptor.operator)
            {
                filter.predicate = predicates[slot].clone();
            }
            if let OpType::MapProject(project) = &mut descriptor.operator {
                for expression in &mut project.expressions {
                    crate::ivm::template::arguments::bind_project_expression(
                        &mut expression.expression,
                        scalars,
                    )
                    .map_err(|_| IvmRuntimeError::GraphOutputMismatch)?;
                }
            }
            self.logical_nodes_requested += 1;
            let id = self.graph.dedup_node(descriptor, NodeDurability::Ephemeral);
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
