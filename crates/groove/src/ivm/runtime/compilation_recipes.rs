//! Bounded, immutable installation recipes, not retained executions.
//!
//! Recipes use typed input slots, never concrete source IDs. Their shallow
//! keys include every operator parameter, input descriptor and input-aliasing /
//! ordering pattern. Binding supplies the actual nodes at each installation.
//! After GC, replay recreates nodes through ordinary validation and initialization;
//! no row state, authorization, subscription, or retainer survives in a recipe.

use super::*;

const SLOTS: usize = 512;
const MAX_PAYLOAD: usize = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct RecipeKey {
    // Children are empty typed leaves, never executed. Keeping the original
    // builder's parameter representation avoids a second operator-definition
    // language which could omit a future semantic field.
    definition: GraphBuilder,
    inputs: Vec<(usize, bool, RecordDescriptor)>,
    projection_context: Vec<(OpType, NodeOutput)>,
}

impl RecipeKey {
    pub(super) fn for_builder(
        graph: &GraphBuilder,
        compiled: &HashMap<usize, CompiledNode>,
        runtime_graph: &IvmGraph,
    ) -> Option<(Self, Vec<CompiledNode>)> {
        match graph {
            // These leaves are cheap, registry-sensitive or contain row data.
            // Recursive/collector compilation additionally has graph-context
            // validation; leave it on the ordinary compiler path.
            GraphBuilder::Table { .. }
            | GraphBuilder::InlineRecords { .. }
            | GraphBuilder::InputSource { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::FrontierSource { .. }
            | GraphBuilder::BindingSource { .. }
            | GraphBuilder::Recursive { .. }
            | GraphBuilder::RecursiveStepWitness { .. }
            | GraphBuilder::CollectBy { .. }
            | GraphBuilder::Filter { .. }
            | GraphBuilder::Union { .. } => return None,
            // Direct-table ArgBy also validates the schema's primary key, not
            // just its record descriptor. Never memoize that validation here.
            GraphBuilder::ArgMaxBy { input, .. } | GraphBuilder::ArgMinBy { input, .. }
                if matches!(input.as_ref(), GraphBuilder::Table { .. }) =>
            {
                return None;
            }
            _ => {}
        }
        let mut definition = graph.clone();
        let mut inputs = Vec::new();
        let mut bindings: Vec<CompiledNode> = Vec::new();
        let mut bind = |input: &mut Arc<GraphBuilder>| -> Option<()> {
            let value = compiled.get(&(input.as_ref() as *const GraphBuilder as usize))?;
            let alias = bindings
                .iter()
                .position(|other| other.node == value.node)
                .unwrap_or(bindings.len());
            inputs.push((alias, value.root_ordering_node.is_some(), value.output));
            bindings.push(value.clone());
            *input = Arc::new(GraphBuilder::InlineRecords {
                output: value.output,
                records: Vec::new(),
            });
            Some(())
        };
        match &mut definition {
            GraphBuilder::Project { input, .. }
            | GraphBuilder::StreamingChecksum { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::Unnest { input, .. }
            | GraphBuilder::VariantProject { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. }
            | GraphBuilder::Aggregate { input, .. } => bind(input)?,
            GraphBuilder::Join { left, right, .. }
            | GraphBuilder::SemiJoin { left, right, .. }
            | GraphBuilder::AntiJoin { left, right, .. } => {
                bind(left)?;
                bind(right)?;
            }
            _ => return None,
        }
        let mut projection_context = Vec::new();
        if matches!(graph, GraphBuilder::Project { .. }) {
            // A projection's recipe also depends on the exact static context
            // inspected by composition / join pruning. Bind those ancestors
            // as extra slots; never retain their concrete IDs in the key.
            let mut cursor = bindings[0].node;
            loop {
                let descriptor = &runtime_graph.node(cursor)?.descriptor;
                match &descriptor.operator {
                    OpType::MapProject(project) => {
                        if projection_context.len() >= 64 {
                            return None;
                        }
                        projection_context.push((descriptor.operator.clone(), descriptor.output));
                        if project.expressions.is_empty()
                            || !project
                                .expressions
                                .iter()
                                .all(|expr| matches!(expr.expression, ProjectExpr::Field(_)))
                        {
                            break;
                        }
                        cursor = *descriptor.inputs.first()?;
                        Self::bind_context_input(
                            runtime_graph,
                            cursor,
                            &mut inputs,
                            &mut bindings,
                        )?;
                    }
                    OpType::Join(join)
                        if join.residual_predicate.is_none()
                            && matches!(join.kind, JoinOpKind::Inner) =>
                    {
                        projection_context.push((descriptor.operator.clone(), descriptor.output));
                        for arrangement in &descriptor.inputs {
                            let descriptor = &runtime_graph.node(*arrangement)?.descriptor;
                            projection_context
                                .push((descriptor.operator.clone(), descriptor.output));
                            Self::bind_context_input(
                                runtime_graph,
                                *arrangement,
                                &mut inputs,
                                &mut bindings,
                            )?;
                            Self::bind_context_input(
                                runtime_graph,
                                *descriptor.inputs.first()?,
                                &mut inputs,
                                &mut bindings,
                            )?;
                        }
                        break;
                    }
                    // Composition stops here and pruning does nothing. The
                    // operator's predicate/source identity is a binding, not
                    // part of the projection's static recipe.
                    _ => break,
                }
            }
        }
        Some((
            Self {
                definition,
                inputs,
                projection_context,
            },
            bindings,
        ))
    }

    fn bind_context_input(
        graph: &IvmGraph,
        node: NodeId,
        inputs: &mut Vec<(usize, bool, RecordDescriptor)>,
        bindings: &mut Vec<CompiledNode>,
    ) -> Option<()> {
        let output = graph.node(node)?.descriptor.output.records();
        let alias = bindings
            .iter()
            .position(|input| input.node == node)
            .unwrap_or(bindings.len());
        inputs.push((alias, false, output));
        bindings.push(CompiledNode {
            node,
            output,
            root_ordering_node: None,
        });
        Some(())
    }
}

#[derive(Clone, Copy, Debug, Hash)]
enum RecipeInput {
    Input(usize),
    Node(usize),
}

#[derive(Clone, Debug, Hash)]
struct RecipeNode {
    // The operator/output are typed; inputs are relocations below. This
    // descriptor is never inserted before its input slots have been bound.
    descriptor: NodeDescriptor,
    inputs: Vec<RecipeInput>,
}

#[derive(Clone, Debug)]
pub(super) struct CompilationRecipe {
    key: RecipeKey,
    nodes: Vec<RecipeNode>,
    output: RecordDescriptor,
    node: RecipeInput,
    // Input means inherit that input's root ordering, not its root node.
    ordering: Option<RecipeInput>,
    logical_nodes: u64,
}

impl CompilationRecipe {
    fn capture(
        key: RecipeKey,
        bindings: &[CompiledNode],
        descriptors: Vec<NodeDescriptor>,
        compiled: CompiledNode,
        logical_nodes: u64,
    ) -> Option<Self> {
        let mut references = bindings
            .iter()
            .enumerate()
            .rev()
            .map(|(index, input)| (input.node, RecipeInput::Input(index)))
            .collect::<HashMap<_, _>>();
        let mut nodes = Vec::new();
        for mut descriptor in descriptors {
            let id = descriptor.node_id();
            let inputs = descriptor
                .inputs
                .iter()
                .map(|id| references.get(id).copied())
                .collect::<Option<Vec<_>>>()?;
            descriptor.inputs.clear();
            references.insert(id, RecipeInput::Node(nodes.len()));
            nodes.push(RecipeNode { descriptor, inputs });
        }
        let node = *references.get(&compiled.node)?;
        let ordering = if let Some(ordering) = compiled.root_ordering_node {
            Some(
                bindings
                    .iter()
                    .position(|input| input.root_ordering_node == Some(ordering))
                    .map(RecipeInput::Input)
                    .or_else(|| references.get(&ordering).copied())?,
            )
        } else {
            None
        };
        Some(Self {
            key,
            nodes,
            output: compiled.output,
            node,
            ordering,
            logical_nodes,
        })
    }

    pub(super) fn install(
        &self,
        runtime: &mut IvmRuntime,
        bindings: &[CompiledNode],
    ) -> CompiledNode {
        let mut nodes = Vec::with_capacity(self.nodes.len());
        let resolve = |input: RecipeInput, nodes: &[NodeId]| match input {
            RecipeInput::Input(index) => bindings[index].node,
            RecipeInput::Node(index) => nodes[index],
        };
        for template in &self.nodes {
            let mut descriptor = template.descriptor.clone();
            descriptor.inputs = template
                .inputs
                .iter()
                .map(|input| resolve(*input, &nodes))
                .collect();
            let node = runtime
                .graph
                .dedup_node(descriptor, NodeDurability::Ephemeral);
            runtime.initialize_node_runtime(node);
            nodes.push(node);
        }
        runtime.logical_nodes_requested += self.logical_nodes;
        CompiledNode {
            output: self.output,
            node: resolve(self.node, &nodes),
            root_ordering_node: self.ordering.and_then(|ordering| match ordering {
                RecipeInput::Input(index) => bindings[index].root_ordering_node,
                RecipeInput::Node(index) => Some(nodes[index]),
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct CompilationRecipes {
    slots: Vec<Option<Rc<CompilationRecipe>>>,
    #[cfg(test)]
    pub(super) hits: usize,
}

impl Default for CompilationRecipes {
    fn default() -> Self {
        Self {
            slots: vec![None; SLOTS],
            #[cfg(test)]
            hits: 0,
        }
    }
}

#[derive(Default)]
struct PayloadHasher {
    inner: DefaultHasher,
    bytes: usize,
}
impl Hasher for PayloadHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.bytes = self.bytes.saturating_add(bytes.len());
        self.inner.write(bytes);
    }
    fn finish(&self) -> u64 {
        self.inner.finish()
    }
}

impl CompilationRecipes {
    pub(super) fn inferred_output(
        &self,
        graph: &GraphBuilder,
        outputs: &HashMap<usize, RecordDescriptor>,
    ) -> Option<RecordDescriptor> {
        // Passthrough outputs are cheaper than a lookup. Project already has
        // its source-independent typed plan; recursive/collector validation
        // remains on the ordinary path.
        if !matches!(
            graph,
            GraphBuilder::Join { .. }
                | GraphBuilder::Aggregate { .. }
                | GraphBuilder::UnwrapNullable { .. }
                | GraphBuilder::Unnest { .. }
                | GraphBuilder::VariantProject { .. }
        ) {
            return None;
        }
        let mut definition = graph.clone();
        let bind = |input: &mut Arc<GraphBuilder>| -> Option<()> {
            let output = *outputs.get(&(input.as_ref() as *const GraphBuilder as usize))?;
            *input = Arc::new(GraphBuilder::InlineRecords {
                output,
                records: Vec::new(),
            });
            Some(())
        };
        match &mut definition {
            GraphBuilder::Join { left, right, .. } => {
                bind(left)?;
                bind(right)?;
            }
            GraphBuilder::Aggregate { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::Unnest { input, .. }
            | GraphBuilder::VariantProject { input, .. } => bind(input)?,
            _ => return None,
        }
        let mut hash = PayloadHasher::default();
        definition.hash(&mut hash);
        let recipe = self.slots[hash.finish() as usize % SLOTS].as_ref()?;
        // Aliasing and root ordering affect node relocation, never this typed
        // output. Exact definition equality includes input enum registries.
        (recipe.key.definition == definition).then_some(recipe.output)
    }

    pub(super) fn lookup(&mut self, key: &RecipeKey) -> Option<Rc<CompilationRecipe>> {
        let mut hash = PayloadHasher::default();
        key.definition.hash(&mut hash);
        let value = self.slots[hash.finish() as usize % SLOTS].as_ref()?;
        if value.key != *key {
            return None;
        }
        #[cfg(test)]
        {
            self.hits += 1;
        }
        Some(Rc::clone(value))
    }

    pub(super) fn insert(
        &mut self,
        key: RecipeKey,
        bindings: &[CompiledNode],
        nodes: Vec<NodeDescriptor>,
        compiled: CompiledNode,
        logical_nodes: u64,
    ) {
        let mut hash = PayloadHasher::default();
        key.definition.hash(&mut hash);
        let slot = hash.finish() as usize % SLOTS;
        key.inputs.hash(&mut hash);
        key.projection_context.hash(&mut hash);
        nodes.hash(&mut hash);
        // Bound retained definitions as well as entry count. Descriptors are
        // interned immutable handles; this is not a live-heap accounting claim.
        if hash.bytes <= MAX_PAYLOAD
            && let Some(recipe) =
                CompilationRecipe::capture(key, bindings, nodes, compiled, logical_nodes)
        {
            self.slots[slot] = Some(Rc::new(recipe));
        }
    }
}

impl IvmRuntime {
    pub(super) fn dedup_compilation_node(
        &mut self,
        descriptor: NodeDescriptor,
        durability: NodeDurability,
    ) -> NodeId {
        if let Some(nodes) = &mut self.compilation_capture {
            debug_assert_eq!(durability, NodeDurability::Ephemeral);
            nodes.push(descriptor.clone());
        }
        self.graph.dedup_node(descriptor, durability)
    }
}
