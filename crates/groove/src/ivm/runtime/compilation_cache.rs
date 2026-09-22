//! Reuse compilation of the *same immutable fragment*, never its evaluation.
//!
//! Weak ownership pins pointer identity without retaining graph definitions or
//! row data. Inputs are compiled/validated before lookup, including mutable
//! source ownership and retirement checks. A hit requires their exact compiled
//! identities, inferred descriptors and root-ordering metadata, and a still
//! installed output. GC therefore cannot resurrect an execution through this
//! cache. No structural hash or cross-source equivalence is inferred here.

use super::*;

const SLOTS: usize = 4096;

#[derive(Clone, Debug)]
struct Entry {
    owner: Weak<GraphBuilder>,
    inputs: Vec<(CompiledNode, RecordDescriptor)>,
    inferred: RecordDescriptor,
    compiled: CompiledNode,
    logical_nodes: u64,
}

#[derive(Debug, Default)]
pub(super) struct CompilationCache {
    slots: HashMap<usize, Entry>,
    #[cfg(test)]
    pub(super) hits: usize,
}

impl Clone for CompilationCache {
    fn clone(&self) -> Self {
        // Runtime forks copy authoritative execution state, not disposable
        // installation hints or their weak allocation owners.
        Self::default()
    }
}

fn slot(owner: &Arc<GraphBuilder>) -> usize {
    // Allocations are aligned. Mix the remaining address bits; this is only a
    // bounded lookup hint, with exact ownership checked below.
    (Arc::as_ptr(owner) as usize >> 4).wrapping_mul(0x9e3779b1) % SLOTS
}

fn eligible(graph: &GraphBuilder) -> bool {
    match graph {
        // Source validation must run for every install. Recursive/collector
        // compilation has additional context validation and stays explicit.
        GraphBuilder::Table { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::InputSource { .. }
        | GraphBuilder::BindingSource { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::Recursive { .. }
        | GraphBuilder::RecursiveStepWitness { .. }
        | GraphBuilder::CollectBy { .. } => false,
        // This case validates the current table primary-key contract as well
        // as its record descriptor.
        GraphBuilder::ArgMaxBy { input, .. } | GraphBuilder::ArgMinBy { input, .. }
            if matches!(input.as_ref(), GraphBuilder::Table { .. }) =>
        {
            false
        }
        _ => true,
    }
}

impl CompilationCache {
    pub(super) fn get(
        &mut self,
        owner: &Arc<GraphBuilder>,
        compiled: &HashMap<usize, CompiledNode>,
        outputs: &HashMap<usize, RecordDescriptor>,
        graph: &IvmGraph,
    ) -> Option<(CompiledNode, RecordDescriptor, u64)> {
        let entry = self.slots.get(&slot(owner))?;
        // Keeping the Weak alive prevents allocation-address reuse; upgrading
        // additionally makes the ownership proof explicit.
        if !Arc::ptr_eq(&entry.owner.upgrade()?, owner)
            || graph.node(entry.compiled.node).is_none()
            || entry
                .compiled
                .root_ordering_node
                .is_some_and(|id| graph.node(id).is_none())
        {
            return None;
        }
        let mut index = 0;
        let mut matches = true;
        owner.visit_inputs(|input| {
            let key = Arc::as_ptr(input) as usize;
            matches &= entry.inputs.get(index).is_some_and(|(expected, output)| {
                compiled.get(&key) == Some(expected) && outputs.get(&key) == Some(output)
            });
            index += 1;
        });
        if !matches || index != entry.inputs.len() {
            return None;
        }
        #[cfg(test)]
        {
            self.hits += 1;
        }
        Some((entry.compiled.clone(), entry.inferred, entry.logical_nodes))
    }

    pub(super) fn insert(
        &mut self,
        owner: &Arc<GraphBuilder>,
        compiled: &HashMap<usize, CompiledNode>,
        outputs: &HashMap<usize, RecordDescriptor>,
        result: CompiledNode,
        inferred: RecordDescriptor,
        logical_nodes: u64,
    ) {
        if !eligible(owner) {
            return;
        }
        let mut inputs = Vec::new();
        owner.visit_inputs(|input| {
            let key = Arc::as_ptr(input) as usize;
            inputs.push((compiled[&key].clone(), outputs[&key]));
        });
        // Bound retained metadata as well as entry count (notably wide Union).
        if inputs.len() > 64 {
            return;
        }
        self.slots.insert(
            slot(owner),
            Entry {
                owner: Arc::downgrade(owner),
                inputs,
                inferred,
                compiled: result,
                logical_nodes,
            },
        );
    }
}

/// One visit per immutable DAG fragment. Owners are borrowed for this install;
/// only Weak references may escape it. Deep valid graphs remain iterative.
pub(super) fn compilation_order(
    graph: &GraphBuilder,
) -> Vec<(&GraphBuilder, Option<&Arc<GraphBuilder>>)> {
    let mut pending = vec![(graph, None, false)];
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    while let Some((graph, owner, visited)) = pending.pop() {
        if visited {
            ordered.push((graph, owner));
        } else if seen.insert(graph as *const GraphBuilder as usize) {
            pending.push((graph, owner, true));
            graph.visit_inputs(|input| pending.push((input.as_ref(), Some(input), false)));
        }
    }
    ordered
}
