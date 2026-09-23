//! Source-keyed tick programs. Only immutable topology is retained: no rows,
//! subscriptions, frontiers, authorization answers, or producer readiness.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use super::execution_layout::ExecutionLayout;
use super::{BindingSourceKey, IvmGraph, NodeId, OpType};

#[derive(Debug)]
pub(crate) struct ActivationPlan {
    sources: Vec<NodeId>,
    pub affected: Arc<HashSet<NodeId>>,
    pub relevant: Arc<HashSet<NodeId>>,
    pub layout: Arc<ExecutionLayout>,
    pub durable: Vec<NodeId>,
    pub ephemeral: Vec<NodeId>,
    pub tables: Vec<String>,
    pub bindings: Vec<BindingSourceKey>,
}

impl ActivationPlan {
    fn compile(graph: &IvmGraph, sources: Vec<NodeId>) -> Result<Self, NodeId> {
        let mut affected = HashSet::new();
        let mut pending = sources.clone();
        while let Some(id) = pending.pop() {
            if affected.insert(id) {
                pending.extend(graph.node(id).ok_or(id)?.children.iter().copied());
            }
        }
        let layout = graph.execution_layout(affected.iter().copied())?;
        let relevant = Arc::new(layout.nodes.iter().copied().collect());
        let mut durable = Vec::new();
        let mut ephemeral = Vec::new();
        let mut tables = HashSet::new();
        let mut bindings = HashSet::new();
        for &id in &layout.nodes {
            let node = graph.node(id).ok_or(id)?;
            if affected.contains(&id) {
                if node.is_durable() {
                    durable.push(id);
                } else {
                    ephemeral.push(id);
                }
            }
            match &node.descriptor.operator {
                OpType::TableSource(source) => {
                    tables.insert(source.table.clone());
                }
                OpType::BindingSource(source) => {
                    bindings.insert(source.key.clone());
                }
                _ => {}
            }
        }
        Ok(Self {
            sources,
            affected: Arc::new(affected),
            relevant,
            layout,
            durable,
            ephemeral,
            tables: tables.into_iter().collect(),
            bindings: bindings.into_iter().collect(),
        })
    }

    fn weight(&self) -> usize {
        self.sources.len()
            + self.affected.len()
            + self.relevant.len()
            + self.layout.weight()
            + self.durable.len()
            + self.ephemeral.len()
            + self.tables.len()
            + self.bindings.len()
    }
}

#[derive(Debug, Default)]
struct CachedActivations {
    plans: VecDeque<Arc<ActivationPlan>>,
    weight: usize,
    builds: u64,
    hits: u64,
}

#[derive(Debug, Default)]
pub(crate) struct ActivationCache(Mutex<CachedActivations>);

impl Clone for ActivationCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl ActivationCache {
    pub fn get(
        &self,
        graph: &IvmGraph,
        mut sources: Vec<NodeId>,
    ) -> Result<Arc<ActivationPlan>, NodeId> {
        sources.sort_unstable();
        sources.dedup();
        let mut cache = self.0.lock().expect("activation cache poisoned");
        if let Some(index) = cache.plans.iter().position(|plan| plan.sources == sources) {
            let plan = cache.plans.remove(index).unwrap();
            cache.plans.push_back(Arc::clone(&plan));
            cache.hits += 1;
            return Ok(plan);
        }
        let plan = Arc::new(ActivationPlan::compile(graph, sources)?);
        cache.builds += 1;
        const MAX_PLANS: usize = 16;
        // A repeated activation over roughly a thousand routed subscriptions
        // exceeds the former 65K-weight cap. Rebuilding it on every write is
        // more expensive than retaining a bounded plan for the live graph.
        const MAX_WEIGHT: usize = 1_000_000;
        let weight = plan.weight();
        if weight <= MAX_WEIGHT {
            while cache.plans.len() >= MAX_PLANS || cache.weight + weight > MAX_WEIGHT {
                cache.weight -= cache.plans.pop_front().unwrap().weight();
            }
            cache.weight += weight;
            cache.plans.push_back(Arc::clone(&plan));
        }
        Ok(plan)
    }

    /// A new consumer changes descendant reachability even when every cached
    /// ancestor layout is still valid. Unrelated insertions keep their plans.
    pub fn added(&mut self, inputs: &[NodeId]) {
        self.retain(|plan| !inputs.iter().any(|id| plan.affected.contains(id)));
    }

    pub fn removed(&mut self, node: NodeId) {
        self.retain(|plan| !plan.relevant.contains(&node));
    }

    pub fn clear(&mut self) {
        self.retain(|_| false);
    }

    fn retain(&mut self, keep: impl FnMut(&Arc<ActivationPlan>) -> bool) {
        let cache = self.0.get_mut().expect("activation cache poisoned");
        cache.plans.retain(keep);
        cache.weight = cache.plans.iter().map(|plan| plan.weight()).sum();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::{NodeDescriptor, NodeDurability, TableSourceOp};
    use crate::records::{RecordDescriptor, ValueType};

    // Internal topology receipt: exact public rows cannot prove cache reuse,
    // bounded cache ownership, or unchanged-sibling capture. Public runtime
    // tests separately exercise subscription churn, binding edits and rollback.
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

    fn plan(graph: &IvmGraph, tables: &[&str]) -> Arc<ActivationPlan> {
        graph
            .activation_plan(tables.iter().copied(), std::iter::empty())
            .unwrap()
    }

    #[test]
    fn activation_reuses_topology_but_new_consumers_invalidate_descendants() {
        let mut graph = IvmGraph::new();
        let a = source(&mut graph, "a");
        let before = plan(&graph, &["a"]);
        let b = source(&mut graph, "b");
        assert!(Arc::ptr_eq(&before, &plan(&graph, &["a", "a"])));
        let union = graph.dedup_node(
            NodeDescriptor::new(
                OpType::Union,
                [a, b],
                RecordDescriptor::new([("id", ValueType::U64)]),
            ),
            NodeDurability::Ephemeral,
        );
        let after = plan(&graph, &["a"]);
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(*after.affected, HashSet::from([a, union]));
        assert_eq!(*after.relevant, HashSet::from([a, b, union]));
        assert_eq!(
            after.tables.iter().cloned().collect::<HashSet<_>>(),
            HashSet::from(["a".into(), "b".into()])
        );
        assert!(Arc::ptr_eq(
            &plan(&graph, &["a", "b"]),
            &plan(&graph, &["b", "a"])
        ));

        graph.remove_node(union);
        let removed = plan(&graph, &["a"]);
        assert_eq!(*removed.affected, HashSet::from([a]));
        assert_eq!(
            *after.affected,
            HashSet::from([a, union]),
            "owned old plan stays immutable"
        );
        graph.node_mut(a).unwrap();
        assert!(!Arc::ptr_eq(&removed, &plan(&graph, &["a"])));
        assert!(!Arc::ptr_eq(
            &plan(&graph, &["a"]),
            &plan(&graph.clone(), &["a"])
        ));
    }

    #[test]
    fn activation_keys_follow_new_sources_and_cache_eviction_preserves_live_plans() {
        let mut graph = IvmGraph::new();
        let empty = plan(&graph, &["later"]);
        assert!(empty.affected.is_empty());
        let later = source(&mut graph, "later");
        let active = plan(&graph, &["later"]);
        assert_eq!(*active.affected, HashSet::from([later]));
        let weak = Arc::downgrade(&active);
        for i in 0..32 {
            let name = format!("unrelated-{i}");
            source(&mut graph, &name);
            plan(&graph, &[&name]);
        }
        assert_eq!(*active.affected, HashSet::from([later]));
        assert!(!Arc::ptr_eq(&active, &plan(&graph, &["later"])));
        drop(active);
        assert!(weak.upgrade().is_none());
        graph.remove_node(later);
        assert!(plan(&graph, &["later"]).affected.is_empty());
    }
}
