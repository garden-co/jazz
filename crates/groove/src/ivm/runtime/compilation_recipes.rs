//! Bounded, immutable installation recipes, not retained executions.
//!
//! A recipe is scoped to exact compiled inputs in this runtime. Its shallow
//! builder key includes every operator parameter, input descriptor and root
//! ordering identity. It contains no row state, subscription or retainer.
//! After GC, replay recreates nodes through ordinary graph validation and
//! runtime initialization; after input/schema changes the key no longer matches.

use super::*;

const SLOTS: usize = 512;
const MAX_PAYLOAD: usize = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct RecipeKey {
    // Children are empty typed leaves, never executed. Keeping the original
    // builder's parameter representation avoids a second operator-definition
    // language which could omit a future semantic field.
    definition: GraphBuilder,
    inputs: Vec<(NodeId, Option<NodeId>)>,
}

impl RecipeKey {
    pub(super) fn for_builder(
        graph: &GraphBuilder,
        compiled: &HashMap<usize, CompiledNode>,
    ) -> Option<Self> {
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
        let mut bind = |input: &mut Arc<GraphBuilder>| -> Option<()> {
            let value = compiled.get(&(input.as_ref() as *const GraphBuilder as usize))?;
            inputs.push((value.node, value.root_ordering_node));
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
        Some(Self { definition, inputs })
    }
}

#[derive(Clone, Debug)]
pub(super) struct CompilationRecipe {
    key: RecipeKey,
    pub(super) nodes: Vec<NodeDescriptor>,
    pub(super) compiled: CompiledNode,
    pub(super) logical_nodes: u64,
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
    pub(super) fn lookup(&mut self, key: &RecipeKey) -> Option<Rc<CompilationRecipe>> {
        let mut hash = PayloadHasher::default();
        key.hash(&mut hash);
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
        nodes: Vec<NodeDescriptor>,
        compiled: CompiledNode,
        logical_nodes: u64,
    ) {
        let mut hash = PayloadHasher::default();
        key.hash(&mut hash);
        let slot = hash.finish() as usize % SLOTS;
        nodes.hash(&mut hash);
        // Bound retained definitions as well as entry count. Descriptors are
        // interned immutable handles; this is not a live-heap accounting claim.
        if hash.bytes <= MAX_PAYLOAD {
            self.slots[slot] = Some(Rc::new(CompilationRecipe {
                key,
                nodes,
                compiled,
                logical_nodes,
            }));
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
