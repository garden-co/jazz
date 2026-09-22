//! Compile topology once; installation only binds validated sources and nodes.
use super::*;
use crate::ivm::template::{TemplateNode, TemplateNodeRef, TypedGraphTemplate};

static NEXT_TEMPLATE: AtomicU64 = AtomicU64::new(1);

impl IvmRuntime {
    pub(crate) fn compile_template_graphs(
        graphs: &[GraphBuilder],
    ) -> Result<Vec<GraphBuilder>, IvmRuntimeError> {
        let mut compiler = Self::new(DatabaseSchema::new([]))?;
        graphs
            .iter()
            .map(|graph| compiler.compile_template_graph(graph))
            .collect()
    }

    fn compile_template_graph(
        &mut self,
        graph: &GraphBuilder,
    ) -> Result<GraphBuilder, IvmRuntimeError> {
        validate_collect_by_terminality(graph)?;
        // Jazz attaches binding-route filters immediately below terminal
        // collectors. Keep that explicit terminal boundary visible; compile
        // the relational input, not an opaque collector that a caller could
        // accidentally filter after collection.
        if let GraphBuilder::CollectBy { input, .. } = graph {
            let input = Arc::new(self.compile_template_graph(input)?);
            return Ok(graph.map_inputs(|_| input.clone()));
        }
        let mut inputs = Vec::new();
        let mut contracts = Vec::new();
        let mut replacements = Vec::new();
        let mut source_nodes = Vec::new();
        let mut rewritten = HashMap::<usize, Arc<GraphBuilder>>::default();
        for (node, _) in compilation_cache::compilation_order(graph) {
            let output = match node {
                GraphBuilder::TemplateInput {
                    output,
                    input: None,
                    ..
                }
                | GraphBuilder::BindingSource { output, .. }
                | GraphBuilder::InlineRecords { output, .. } => Some(*output),
                // These require catalogue or recursion-context proofs outside
                // this source-independent artifact. Keep ordinary compilation.
                GraphBuilder::Table { .. }
                | GraphBuilder::Index { .. }
                | GraphBuilder::InputSource { .. }
                | GraphBuilder::FrontierSource { .. }
                | GraphBuilder::Recursive { .. }
                | GraphBuilder::RecursiveStepWitness { .. }
                | GraphBuilder::TypedTemplate { .. }
                | GraphBuilder::TemplateInput { .. } => {
                    return Err(IvmRuntimeError::UnsupportedOperator);
                }
                _ => None,
            };
            let bound = if let Some(output) = output {
                let id = self.allocate_input_source(output);
                let replacement = GraphBuilder::input_source(id, output);
                let compiled = self.add_dedup_graph(&replacement)?;
                source_nodes.push(compiled.node);
                replacements.push(crate::ivm::TemplateGraphInput::with_output_contract(
                    replacement,
                    output,
                ));
                contracts.push(output);
                inputs.push(Arc::new(node.clone()));
                GraphBuilder::TemplateInput {
                    slot: (inputs.len() - 1) as u32,
                    output,
                    input: None,
                }
            } else {
                node.map_inputs(|child| rewritten[&(Arc::as_ptr(child) as usize)].clone())
            };
            rewritten.insert(node as *const GraphBuilder as usize, Arc::new(bound));
        }
        let fallback = (*rewritten[&(graph as *const GraphBuilder as usize)]).clone();
        let bound =
            crate::ivm::bind_template_graphs(std::slice::from_ref(&fallback), &replacements)
                .map_err(|_| IvmRuntimeError::GraphOutputMismatch)?
                .remove(0);
        let compiled = self.add_dedup_graph(&bound)?;
        let mut references = source_nodes
            .into_iter()
            .enumerate()
            .map(|(i, node)| (node, TemplateNodeRef::Input(i)))
            .collect::<HashMap<_, _>>();
        let mut nodes = Vec::new();
        let mut pending = vec![(compiled.node, false)];
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
            references.insert(id, TemplateNodeRef::Node(nodes.len()));
            nodes.push(TemplateNode { descriptor, inputs });
        }
        let program = TypedGraphTemplate {
            identity: NEXT_TEMPLATE.fetch_add(1, Ordering::Relaxed),
            nodes,
            inputs: contracts,
            output: compiled.output,
            root: references[&compiled.node],
            ordering: compiled.root_ordering_node.map(|id| references[&id]),
            terminal: matches!(graph, GraphBuilder::CollectBy { .. }),
            fallback,
        };
        Ok(GraphBuilder::TypedTemplate {
            program: Arc::new(program),
            inputs,
        })
    }

    pub(super) fn install_typed_template(
        &mut self,
        program: &TypedGraphTemplate,
        inputs: &[CompiledNode],
        graphs: &[Arc<GraphBuilder>],
    ) -> Result<CompiledNode, IvmRuntimeError> {
        if inputs.len() != program.inputs.len()
            || inputs
                .iter()
                .zip(&program.inputs)
                .any(|(input, output)| input.output != *output)
        {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        // Ordering can be inherited from an external source through projections
        // and joins. Until that is an explicit relocation, preserve the complete
        // ordinary compiler path for ordered sources rather than dropping it.
        if inputs
            .iter()
            .any(|input| input.root_ordering_node.is_some())
        {
            let bindings = graphs
                .iter()
                .zip(&program.inputs)
                .map(|(graph, output)| {
                    crate::ivm::TemplateGraphInput::with_output_contract((**graph).clone(), *output)
                })
                .collect::<Vec<_>>();
            let bound = crate::ivm::bind_template_graphs(
                std::slice::from_ref(&program.fallback),
                &bindings,
            )
            .map_err(|_| IvmRuntimeError::GraphOutputMismatch)?
            .remove(0);
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
