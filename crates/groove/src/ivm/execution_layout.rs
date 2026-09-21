//! Reusable graph wiring, deliberately independent of evaluation state.
//!
//! Layouts contain no rows, bindings, memo validity, requests, or readiness.
//! They neither retain graph nodes nor change their lifetime. Each evaluation
//! allocates its own frame over these compact slots.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use rustc_hash::FxHashMap;

use super::graph::{IvmGraph, NodeId, OpType};

#[derive(Debug)]
pub(crate) struct ExecutionLayout {
    pub nodes: Vec<NodeId>,
    pub slots: FxHashMap<NodeId, usize>,
    pub roots: Vec<NodeId>,
    pub input_counts: Vec<usize>,
    input_offsets: Vec<usize>,
    input_slots: Vec<usize>,
    pub source_slots: Vec<usize>,
    /// Structural unary edges only. Live sharing/retainers are checked when a
    /// frame contracts them; those mutable facts never enter this cache.
    pub pipeline_predecessors: Vec<Option<usize>>,
    dependent_offsets: Vec<usize>,
    dependent_slots: Vec<usize>,
}

impl ExecutionLayout {
    fn compile(graph: &IvmGraph, roots: Vec<NodeId>) -> Result<Self, NodeId> {
        let mut nodes = Vec::new();
        let mut slots = FxHashMap::default();
        let mut input_counts = Vec::new();
        let mut source_slots = Vec::new();
        let mut pending = VecDeque::from(roots.clone());
        while let Some(id) = pending.pop_front() {
            if slots.contains_key(&id) {
                continue;
            }
            let node = graph.node(id).ok_or(id)?;
            let slot = nodes.len();
            slots.insert(id, slot);
            nodes.push(id);
            input_counts.push(node.descriptor.inputs.len());
            if matches!(
                node.descriptor.operator,
                OpType::TableSource(_) | OpType::IndexSource(_)
            ) {
                source_slots.push(slot);
            }
            pending.extend(node.descriptor.inputs.iter().copied());
        }

        // Compressed adjacency: one allocation for every reverse edge, with
        // repeated inputs preserved (a self-join has two dependency edges).
        let mut dependent_offsets = vec![0; nodes.len() + 1];
        let mut input_offsets = Vec::with_capacity(nodes.len() + 1);
        let mut input_slots = Vec::new();
        for id in &nodes {
            input_offsets.push(input_slots.len());
            for input in &graph.node(*id).ok_or(*id)?.descriptor.inputs {
                input_slots.push(slots[input]);
                dependent_offsets[slots[input] + 1] += 1;
            }
        }
        input_offsets.push(input_slots.len());
        for slot in 0..nodes.len() {
            dependent_offsets[slot + 1] += dependent_offsets[slot];
        }
        let mut positions = dependent_offsets.clone();
        let mut dependent_slots = vec![0; *dependent_offsets.last().unwrap()];
        for (slot, id) in nodes.iter().enumerate() {
            for input in &graph.node(*id).ok_or(*id)?.descriptor.inputs {
                let position = &mut positions[slots[input]];
                dependent_slots[*position] = slot;
                *position += 1;
            }
        }
        let unary = nodes
            .iter()
            .map(|id| super::runtime::pipeline::supports_node(graph, *id))
            .collect::<Vec<_>>();
        let pipeline_predecessors = nodes
            .iter()
            .enumerate()
            .map(|(slot, id)| {
                if !unary[slot] {
                    return None;
                }
                let input = graph.node(*id)?.descriptor.inputs.first()?;
                let predecessor = slots[input];
                (unary[predecessor]
                    && roots.binary_search(input).is_err()
                    && dependent_offsets[predecessor + 1] - dependent_offsets[predecessor] == 1)
                    .then_some(predecessor)
            })
            .collect();
        Ok(Self {
            nodes,
            slots,
            roots,
            input_counts,
            input_offsets,
            input_slots,
            source_slots,
            pipeline_predecessors,
            dependent_offsets,
            dependent_slots,
        })
    }

    pub fn dependents(&self, slot: usize) -> &[usize] {
        &self.dependent_slots[self.dependent_offsets[slot]..self.dependent_offsets[slot + 1]]
    }

    pub fn inputs(&self, slot: usize) -> &[usize] {
        &self.input_slots[self.input_offsets[slot]..self.input_offsets[slot + 1]]
    }

    pub(super) fn weight(&self) -> usize {
        self.nodes.len() + self.dependent_slots.len() + self.input_slots.len()
    }
}

#[derive(Debug, Default)]
struct CachedLayouts {
    layouts: VecDeque<Arc<ExecutionLayout>>,
    weight: usize,
    builds: u64,
    hits: u64,
}

/// Bounded, graph-owned acceleration only. Cloning a graph starts a fresh
/// cache so its later mutations cannot invalidate another graph's layouts.
#[derive(Debug, Default)]
pub(crate) struct ExecutionLayoutCache(Mutex<CachedLayouts>);

impl Clone for ExecutionLayoutCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl ExecutionLayoutCache {
    pub fn get(
        &self,
        graph: &IvmGraph,
        roots: impl IntoIterator<Item = NodeId>,
    ) -> Result<Arc<ExecutionLayout>, NodeId> {
        let mut roots = roots.into_iter().collect::<Vec<_>>();
        roots.sort_unstable();
        roots.dedup();
        let mut cache = self.0.lock().expect("execution layout cache poisoned");
        if let Some(index) = cache
            .layouts
            .iter()
            .position(|layout| layout.roots == roots)
        {
            let layout = cache.layouts.remove(index).unwrap();
            cache.layouts.push_back(Arc::clone(&layout));
            cache.hits += 1;
            return Ok(layout);
        }
        let layout = Arc::new(ExecutionLayout::compile(graph, roots)?);
        cache.builds += 1;
        // Bound both entry count and topology size; giant one-off plans are
        // usable but not retained by the cache. Frames can outlive eviction.
        const MAX_LAYOUTS: usize = 16;
        const MAX_WEIGHT: usize = 65_536;
        let weight = layout.weight();
        if weight <= MAX_WEIGHT {
            while cache.layouts.len() >= MAX_LAYOUTS || cache.weight + weight > MAX_WEIGHT {
                cache.weight -= cache.layouts.pop_front().unwrap().weight();
            }
            cache.weight += weight;
            cache.layouts.push_back(Arc::clone(&layout));
        }
        Ok(layout)
    }

    pub fn invalidate(&mut self, node: Option<NodeId>) {
        let cache = self.0.get_mut().expect("execution layout cache poisoned");
        cache
            .layouts
            .retain(|layout| node.is_some_and(|node| !layout.slots.contains_key(&node)));
        cache.weight = cache.layouts.iter().map(|layout| layout.weight()).sum();
    }

    pub fn counters(&self) -> (u64, u64) {
        let cache = self.0.lock().expect("execution layout cache poisoned");
        (cache.builds, cache.hits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::{NodeDescriptor, NodeDurability, TableSourceOp};
    use crate::records::{RecordDescriptor, ValueType};

    // Deliberately internal: public row results cannot prove topology reuse,
    // edge-slot multiplicity, or bounded cache ownership across graph mutation.
    // Subscription/write behavior is tested separately through Database.
    fn source(graph: &mut IvmGraph, table: &str) -> NodeId {
        graph.dedup_node(
            NodeDescriptor::new(
                OpType::TableSource(TableSourceOp {
                    table: table.into(),
                    scan: None,
                    variant_projection: None,
                }),
                [],
                RecordDescriptor::new([("id", ValueType::U64)]),
            ),
            NodeDurability::Ephemeral,
        )
    }

    #[test]
    fn layout_reuse_tracks_ancestors_not_unrelated_graph_changes() {
        let mut graph = IvmGraph::new();
        let a = source(&mut graph, "a");
        let first = graph.execution_layout([a]).unwrap();
        let b = source(&mut graph, "b");
        assert!(Arc::ptr_eq(
            &first,
            &graph.execution_layout([a, a]).unwrap()
        ));
        let both = graph.execution_layout([b, a]).unwrap();
        assert!(Arc::ptr_eq(&both, &graph.execution_layout([a, b]).unwrap()));
        graph.remove_node(b);
        assert!(Arc::ptr_eq(&first, &graph.execution_layout([a]).unwrap()));
        assert_eq!(graph.execution_layout([a, b]).unwrap_err(), b);

        let union = graph.dedup_node(
            NodeDescriptor::new(
                OpType::Union,
                [a, a],
                RecordDescriptor::new([("id", ValueType::U64)]),
            ),
            NodeDurability::Ephemeral,
        );
        let doubled = graph.execution_layout([union]).unwrap();
        let input_slot = doubled.slots[&a];
        let union_slot = doubled.slots[&union];
        assert_eq!(doubled.input_counts[union_slot], 2);
        assert_eq!(doubled.dependents(input_slot), [union_slot, union_slot]);
        graph.remove_node(a);
        assert_eq!(graph.execution_layout([union]).unwrap_err(), a);
        assert_eq!(source(&mut graph, "a"), a);
        assert!(!Arc::ptr_eq(
            &doubled,
            &graph.execution_layout([union]).unwrap()
        ));
        let restored = graph.execution_layout([a]).unwrap();
        graph.node_mut(a).unwrap();
        assert!(!Arc::ptr_eq(
            &restored,
            &graph.execution_layout([a]).unwrap()
        ));

        let clone = graph.clone();
        assert_eq!(clone.execution_layout_counters(), (0, 0));
        assert!(!Arc::ptr_eq(
            &graph.execution_layout([a]).unwrap(),
            &clone.execution_layout([a]).unwrap()
        ));
    }

    #[test]
    fn cache_eviction_does_not_invalidate_an_owned_layout() {
        let mut graph = IvmGraph::new();
        let first_id = source(&mut graph, "first");
        let first = graph.execution_layout([first_id]).unwrap();
        let weak = Arc::downgrade(&first);
        for index in 0..32 {
            let id = source(&mut graph, &format!("source-{index}"));
            graph.execution_layout([id]).unwrap();
        }
        assert_eq!(first.nodes, [first_id]);
        assert!(!Arc::ptr_eq(
            &first,
            &graph.execution_layout([first_id]).unwrap()
        ));
        drop(first);
        assert!(
            weak.upgrade().is_none(),
            "evicted layout has no cache ownership"
        );
    }
}
