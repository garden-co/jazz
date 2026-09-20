//! Exact-version evaluator snapshots over the graph's reusable dense layout.
//!
//! Installed state is sparse; a frame captures only its reachable root slots.
//! Payloads are immutable until first mutation, and only changed slots are
//! installed. Recursive scopes use a sparse sidecar with the same isolation. No global
//! runtime snapshot, live-state reference, or storage/wire encoding is involved.

use super::*;
use crate::ivm::execution_layout::ExecutionLayout;

pub(super) trait StateKey: Clone + Eq + Hash {
    fn root(node: NodeId) -> Self;
    fn root_node(&self) -> Option<NodeId>;
}

impl StateKey for NodeId {
    fn root(node: NodeId) -> Self {
        node
    }
    fn root_node(&self) -> Option<NodeId> {
        Some(*self)
    }
}

impl StateKey for OperatorStateKey {
    fn root(node: NodeId) -> Self {
        Self {
            scope: ScopeId::root(),
            node,
        }
    }
    fn root_node(&self) -> Option<NodeId> {
        (self.scope == ScopeId::root()).then_some(self.node)
    }
}

impl StateKey for ArrangementKey {
    fn root(input: NodeId) -> Self {
        Self {
            scope: ScopeId::root(),
            input,
        }
    }
    fn root_node(&self) -> Option<NodeId> {
        (self.scope == ScopeId::root()).then_some(self.input)
    }
}

#[derive(Clone, Debug)]
struct Slot<K, V> {
    key: K,
    value: Option<Arc<V>>,
    changed: bool,
}

#[derive(Clone, Debug)]
pub(super) struct FrameState<K, V> {
    layout: Option<Arc<ExecutionLayout>>,
    slots: Vec<Slot<K, V>>,
    changed_slots: Vec<usize>,
    sparse: HashMap<K, Arc<V>>,
    changed_sparse: HashSet<K>,
}

impl<K, V> Default for FrameState<K, V> {
    fn default() -> Self {
        Self {
            layout: None,
            slots: Vec::new(),
            changed_slots: Vec::new(),
            sparse: HashMap::default(),
            changed_sparse: HashSet::default(),
        }
    }
}

impl<K: StateKey, V: Clone> FrameState<K, V> {
    pub(super) fn snapshot(&self, layout: Arc<ExecutionLayout>) -> Self {
        assert!(
            self.layout.is_none(),
            "capture installed state, not another frame"
        );
        let slots = layout
            .nodes
            .iter()
            .map(|node| {
                let key = K::root(*node);
                let value = self.sparse.get(&key).cloned();
                Slot {
                    key,
                    value,
                    changed: false,
                }
            })
            .collect();
        Self {
            layout: Some(layout),
            slots,
            ..Self::default()
        }
    }

    pub(super) fn capture_scoped(&mut self, source: &Self, key: &K) {
        assert!(self.layout.is_some());
        assert!(key.root_node().is_none());
        if let Some(value) = source.sparse.get(key) {
            self.sparse.insert(key.clone(), Arc::clone(value));
        }
    }

    fn slot(&self, key: &K) -> Option<usize> {
        self.layout.as_ref()?.slots.get(&key.root_node()?).copied()
    }

    fn mark_slot(&mut self, index: usize) {
        if !self.slots[index].changed {
            self.slots[index].changed = true;
            self.changed_slots.push(index);
        }
    }

    pub(super) fn get(&self, key: &K) -> Option<&V> {
        match self.slot(key) {
            Some(index) => self.slots[index].value.as_deref(),
            None => self.sparse.get(key).map(Arc::as_ref),
        }
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub(super) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if let Some(index) = self.slot(key) {
            self.slots[index].value.as_ref()?;
            self.mark_slot(index);
            return self.slots[index].value.as_mut().map(Arc::make_mut);
        }
        let value = self.sparse.get_mut(key)?;
        if self.layout.is_some() {
            self.changed_sparse.insert(key.clone());
        }
        Some(Arc::make_mut(value))
    }

    pub(super) fn insert(&mut self, key: K, value: V) {
        if let Some(index) = self.slot(&key) {
            self.mark_slot(index);
            self.slots[index].value = Some(Arc::new(value));
        } else {
            if self.layout.is_some() {
                self.changed_sparse.insert(key.clone());
            }
            self.sparse.insert(key, Arc::new(value));
        }
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        let previous = if let Some(index) = self.slot(key) {
            self.mark_slot(index);
            self.slots[index].value.take()
        } else {
            if self.layout.is_some() {
                self.changed_sparse.insert(key.clone());
            }
            self.sparse.remove(key)
        };
        previous.map(Arc::unwrap_or_clone)
    }

    pub(super) fn entry(&mut self, key: K) -> StateEntry<'_, K, V> {
        StateEntry { state: self, key }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.slots
            .iter()
            .filter_map(|slot| slot.value.as_deref().map(|v| (&slot.key, v)))
            .chain(self.sparse.iter().map(|(k, v)| (k, v.as_ref())))
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, value)| value)
    }

    #[cfg(test)]
    pub(super) fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(key, _)| key)
    }

    pub(super) fn len(&self) -> usize {
        self.sparse.len()
            + self
                .slots
                .iter()
                .filter(|slot| slot.value.is_some())
                .count()
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        for index in 0..self.slots.len() {
            let slot = &self.slots[index];
            if slot.value.as_ref().is_some_and(|v| !keep(&slot.key, v)) {
                self.slots[index].value = None;
                // A scoped failure discards this owner, rather than publishing
                // a deletion over the live failure-invalidated version.
                self.slots[index].changed = false;
            }
        }
        self.sparse.retain(|key, value| {
            let keep = keep(key, value);
            if !keep {
                self.changed_sparse.remove(key);
            }
            keep
        });
    }

    /// Iterate only staged writes, leaving read-only captured versions alone.
    pub(super) fn changed_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.slots
            .iter_mut()
            .filter(|slot| slot.changed)
            .filter_map(|slot| slot.value.as_mut().map(|v| (&slot.key, Arc::make_mut(v))))
            .chain(
                self.sparse
                    .iter_mut()
                    .filter(|(k, _)| self.changed_sparse.contains(*k))
                    .map(|(k, v)| (k, Arc::make_mut(v))),
            )
    }

    /// Drop the old installed owner before folding payload overlays, preserving
    /// their unique-owner fast path. Suspended readers retain their own versions.
    pub(super) fn install(&mut self, frame: &mut Self, mut fold: impl FnMut(&mut V)) {
        assert!(self.layout.is_none());
        assert!(frame.layout.is_some());
        for index in frame.changed_slots.drain(..) {
            let slot = &mut frame.slots[index];
            if !slot.changed {
                continue;
            }
            self.sparse.remove(&slot.key);
            if let Some(mut value) = slot.value.take() {
                fold(Arc::make_mut(&mut value));
                self.sparse.insert(slot.key.clone(), value);
            }
            slot.changed = false;
        }
        for key in frame.changed_sparse.drain() {
            self.sparse.remove(&key);
            if let Some(mut value) = frame.sparse.remove(&key) {
                fold(Arc::make_mut(&mut value));
                self.sparse.insert(key, value);
            }
        }
    }
}

pub(super) struct StateEntry<'a, K, V> {
    state: &'a mut FrameState<K, V>,
    key: K,
}

impl<'a, K: StateKey, V: Clone> StateEntry<'a, K, V> {
    pub(super) fn or_insert_with(self, make: impl FnOnce() -> V) -> &'a mut V {
        if let Some(index) = self.state.slot(&self.key) {
            self.state.mark_slot(index);
            return Arc::make_mut(
                self.state.slots[index]
                    .value
                    .get_or_insert_with(|| Arc::new(make())),
            );
        }
        if self.state.layout.is_some() {
            self.state.changed_sparse.insert(self.key.clone());
        }
        Arc::make_mut(
            self.state
                .sparse
                .entry(self.key)
                .or_insert_with(|| Arc::new(make())),
        )
    }
    pub(super) fn or_insert(self, value: V) -> &'a mut V {
        self.or_insert_with(|| value)
    }
    pub(super) fn or_default(self) -> &'a mut V
    where
        V: Default,
    {
        self.or_insert_with(V::default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, ColumnType};

    // Internal tests are intentional: public row assertions cannot prove that
    // capture avoids payload clones or that installation visits only writes.
    fn layout() -> (Arc<ExecutionLayout>, NodeId, NodeId) {
        let schema = DatabaseSchema::new(
            ["a", "b"]
                .map(|name| TableSchema::new(name, [ColumnSchema::new("id", ColumnType::U64)])),
        );
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let a = runtime
            .add_dedup_graph(&GraphBuilder::table("a"))
            .unwrap()
            .node;
        let b = runtime
            .add_dedup_graph(&GraphBuilder::table("b"))
            .unwrap()
            .node;
        (runtime.graph.execution_layout([a, b]).unwrap(), a, b)
    }

    #[derive(Debug)]
    struct Counted {
        value: u64,
        clones: Rc<Cell<usize>>,
    }

    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.clones.set(self.clones.get() + 1);
            Self {
                value: self.value,
                clones: Rc::clone(&self.clones),
            }
        }
    }

    #[test]
    fn capture_is_clone_free_and_publication_visits_only_private_changes() {
        let (layout, a, b) = layout();
        let clones = Rc::new(Cell::new(0));
        let mut live = FrameState::default();
        for (node, value) in [(a, 1), (b, 2)] {
            live.insert(
                node,
                Counted {
                    value,
                    clones: Rc::clone(&clones),
                },
            );
        }
        let mut frame = live.snapshot(Arc::clone(&layout));
        let earlier = live.snapshot(Arc::clone(&layout));
        assert_eq!(clones.get(), 0);
        frame.get_mut(&a).unwrap().value = 3;
        frame.get_mut(&a).unwrap().value = 4;
        assert_eq!(clones.get(), 1, "clone only on the first private write");
        assert_eq!(live.get(&a).unwrap().value, 1);
        assert_eq!(earlier.get(&a).unwrap().value, 1);
        live.get_mut(&b).unwrap().value = 5;
        assert_eq!(
            frame.get(&b).unwrap().value,
            2,
            "capture retains exact version"
        );
        let before_install = clones.get();
        let mut folded = Vec::new();
        live.install(&mut frame, |state| folded.push(state.value));
        assert_eq!(folded, [4]);
        assert_eq!(
            clones.get(),
            before_install,
            "publication doesn't recopy private state"
        );
        assert_eq!(live.get(&a).unwrap().value, 4);
        assert_eq!(
            live.get(&b).unwrap().value,
            5,
            "read-only capture cannot restore old state"
        );
        assert_eq!(earlier.get(&a).unwrap().value, 1);
        let mut cancelled = live.snapshot(layout);
        cancelled.get_mut(&a).unwrap().value = 99;
        drop(cancelled);
        assert_eq!(live.get(&a).unwrap().value, 4);
    }

    #[test]
    fn abandonment_discards_changes_but_explicit_removal_publishes_tombstones() {
        let (layout, a, b) = layout();
        let mut live = FrameState::default();
        live.insert(a, 1);
        live.insert(b, 2);
        let mut frame = live.snapshot(Arc::clone(&layout));
        frame.insert(a, 3);
        frame.retain(|node, _| *node != a);
        live.insert(a, 4); // e.g. live failure invalidation
        frame.remove(&b);
        live.install(&mut frame, |_| {});
        assert_eq!(live.get(&a), Some(&4));
        assert_eq!(live.get(&b), None);

        // Discarding and then recreating a slot must not publish it twice.
        let mut frame = live.snapshot(layout);
        frame.insert(a, 5);
        frame.retain(|_, _| false);
        frame.insert(a, 6);
        let mut folds = 0;
        live.install(&mut frame, |_| folds += 1);
        assert_eq!(folds, 1);
        assert_eq!(live.get(&a), Some(&6));
    }

    #[test]
    fn scoped_arrangements_capture_and_publish_independent_versions() {
        let (layout, a, b) = layout();
        let root = ArrangementKey::root(a);
        let left = ArrangementKey {
            scope: ScopeId::root().child(a),
            input: a,
        };
        let right = ArrangementKey {
            scope: ScopeId::root().child(b),
            input: a,
        };
        let mut live = FrameState::default();
        live.insert(root.clone(), 1);
        live.insert(left.clone(), 2);
        live.insert(right.clone(), 3);
        let mut frame = live.snapshot(layout);
        frame.capture_scoped(&live, &left);
        frame.capture_scoped(&live, &right);
        frame.insert(left.clone(), 4);
        live.insert(right.clone(), 5);
        assert_eq!(frame.get(&right), Some(&3));
        live.install(&mut frame, |_| {});
        assert_eq!(live.get(&root), Some(&1));
        assert_eq!(live.get(&left), Some(&4));
        assert_eq!(live.get(&right), Some(&5));
    }
}
