//! One owner for evaluated batches: dense frame slots for ordinary execution,
//! keyed overflow for recursive scopes and alternate contexts. The retained
//! runtime uses the same interface without a layout (sparse hydration memos).
//!
//! A slot is an address, never a validity proof. Consumers still compare the
//! complete key/watermark and check live producer readiness before reuse.

use super::{EvalMemoEntry, EvalMemoKey, HashMap, ScopeId};
use crate::ivm::execution_layout::ExecutionLayout;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub(super) struct EvaluationMemo {
    layout: Option<Arc<ExecutionLayout>>,
    slots: Vec<Option<(EvalMemoKey, EvalMemoEntry)>>,
    occupied: usize,
    overflow: HashMap<EvalMemoKey, EvalMemoEntry>,
    /// Entries keyed to one tick (`tick_epoch.is_some()`). Kept so callers can
    /// ask whether any exist without scanning every entry.
    tick_entries: usize,
}

impl EvaluationMemo {
    pub(super) fn for_layout(layout: Arc<ExecutionLayout>) -> Self {
        Self {
            slots: vec![None; layout.nodes.len()],
            layout: Some(layout),
            ..Self::default()
        }
    }

    /// Durable evaluation may precede discovery of the active subscription
    /// layout. Move its results into that layout without copying their batches.
    pub(super) fn set_layout(&mut self, layout: Arc<ExecutionLayout>) {
        let previous = std::mem::replace(self, Self::for_layout(layout));
        self.extend(previous.into_entries());
    }

    fn slot_for(&self, key: &EvalMemoKey) -> Option<usize> {
        if key.scope != ScopeId::root() {
            return None;
        }
        self.layout.as_ref()?.slots.get(&key.node).copied()
    }

    pub(super) fn slot(&self, slot: usize) -> Option<&(EvalMemoKey, EvalMemoEntry)> {
        self.slots.get(slot)?.as_ref()
    }

    pub(super) fn slot_mut(&mut self, slot: usize) -> Option<(&EvalMemoKey, &mut EvalMemoEntry)> {
        self.slots
            .get_mut(slot)?
            .as_mut()
            .map(|(key, entry)| (&*key, entry))
    }

    pub(super) fn get(&self, key: &EvalMemoKey) -> Option<&EvalMemoEntry> {
        if let Some(slot) = self.slot_for(key)
            && let Some((stored, entry)) = &self.slots[slot]
            && stored == key
        {
            return Some(entry);
        }
        self.overflow.get(key)
    }

    pub(super) fn get_mut(&mut self, key: &EvalMemoKey) -> Option<&mut EvalMemoEntry> {
        if let Some(slot) = self.slot_for(key)
            && self.slots[slot]
                .as_ref()
                .is_some_and(|(stored, _)| stored == key)
        {
            return self.slots[slot].as_mut().map(|(_, entry)| entry);
        }
        self.overflow.get_mut(key)
    }

    pub(super) fn insert(
        &mut self,
        key: EvalMemoKey,
        entry: EvalMemoEntry,
    ) -> Option<EvalMemoEntry> {
        let tick_keyed = key.tick_epoch.is_some();
        let previous = self.insert_entry(key, entry);
        if tick_keyed && previous.is_none() {
            self.tick_entries += 1;
        }
        previous
    }

    /// Returns the previous entry for exactly this key, if any.
    fn insert_entry(&mut self, key: EvalMemoKey, entry: EvalMemoEntry) -> Option<EvalMemoEntry> {
        let Some(slot) = self.slot_for(&key) else {
            return self.overflow.insert(key, entry);
        };
        if let Some((stored, current)) = &mut self.slots[slot]
            && *stored == key
        {
            return Some(std::mem::replace(current, entry));
        }
        // A different context must not overwrite a still-reusable result.
        // Spill the displaced context; each full key still has exactly one owner.
        let replaced = self.overflow.remove(&key);
        match self.slots[slot].replace((key, entry)) {
            Some((old_key, old_entry)) => {
                let duplicate = self.overflow.insert(old_key, old_entry);
                debug_assert!(duplicate.is_none());
                replaced
            }
            None => {
                self.occupied += 1;
                replaced
            }
        }
    }

    pub(super) fn remove(&mut self, key: &EvalMemoKey) -> Option<EvalMemoEntry> {
        let removed = self.remove_entry(key);
        if removed.is_some() && key.tick_epoch.is_some() {
            self.tick_entries -= 1;
        }
        removed
    }

    fn remove_entry(&mut self, key: &EvalMemoKey) -> Option<EvalMemoEntry> {
        if let Some(slot) = self.slot_for(key)
            && self.slots[slot]
                .as_ref()
                .is_some_and(|(stored, _)| stored == key)
        {
            self.occupied -= 1;
            return self.slots[slot].take().map(|(_, entry)| entry);
        }
        self.overflow.remove(key)
    }

    pub(super) fn retain(
        &mut self,
        mut keep: impl FnMut(&EvalMemoKey, &mut EvalMemoEntry) -> bool,
    ) {
        let mut removed_tick_entries = 0;
        for slot in &mut self.slots {
            if let Some((key, entry)) = slot
                && !keep(key, entry)
            {
                removed_tick_entries += usize::from(key.tick_epoch.is_some());
                *slot = None;
                self.occupied -= 1;
            }
        }
        self.overflow.retain(|key, entry| {
            let kept = keep(key, entry);
            removed_tick_entries += usize::from(!kept && key.tick_epoch.is_some());
            kept
        });
        self.tick_entries -= removed_tick_entries;
    }

    /// Number of entries keyed to a single tick.
    pub(super) fn tick_entries(&self) -> usize {
        debug_assert_eq!(
            self.tick_entries,
            self.keys().filter(|key| key.tick_epoch.is_some()).count()
        );
        self.tick_entries
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&EvalMemoKey, &EvalMemoEntry)> {
        self.slots
            .iter()
            .flatten()
            .map(|(key, entry)| (key, entry))
            .chain(self.overflow.iter())
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &EvalMemoKey> {
        self.iter().map(|(key, _)| key)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &EvalMemoEntry> {
        self.iter().map(|(_, entry)| entry)
    }

    pub(super) fn len(&self) -> usize {
        self.occupied + self.overflow.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn extend(
        &mut self,
        entries: impl IntoIterator<Item = (EvalMemoKey, EvalMemoEntry)>,
    ) {
        for (key, entry) in entries {
            self.insert(key, entry);
        }
    }

    pub(super) fn into_entries(self) -> impl Iterator<Item = (EvalMemoKey, EvalMemoEntry)> {
        self.slots.into_iter().flatten().chain(self.overflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::runtime::{IvmRuntime, RecordDeltas};
    use crate::ivm::{GraphBuilder, NodeId};
    use crate::records::{RecordDescriptor, Value, ValueType};
    use crate::schema::DatabaseSchema;

    // Internal ownership receipt: public result equality cannot prove that a
    // batch has just one stored Arc or that overflow keeps every full key.
    // Public subscription, recursive, suspension and eviction suites exercise
    // the observable behavior of this storage through the runtime.
    fn fixture() -> (Arc<ExecutionLayout>, NodeId, NodeId) {
        let mut runtime = IvmRuntime::new(DatabaseSchema::new([])).unwrap();
        let mut node = |value| {
            runtime
                .add_dedup_graph(
                    &GraphBuilder::values(
                        RecordDescriptor::new([("id", ValueType::U64)]),
                        [vec![Value::U64(value)]],
                    )
                    .unwrap(),
                )
                .unwrap()
                .node
        };
        let a = node(1);
        let b = node(2);
        (runtime.graph.execution_layout([a]).unwrap(), a, b)
    }

    fn key(node: NodeId) -> EvalMemoKey {
        EvalMemoKey {
            scope: ScopeId::root(),
            node,
            input_signature_hash: 1,
            tick_epoch: Some(1),
            sub_tick: 0,
            context_digest: 0,
        }
    }

    fn entry(serial: u64) -> EvalMemoEntry {
        EvalMemoEntry::new(
            Arc::new(RecordDeltas::empty(RecordDescriptor::new([(
                "id",
                ValueType::U64,
            )]))),
            serial,
            serial as usize,
            serial,
        )
    }

    #[test]
    fn frame_slots_own_batches_once_and_preserve_displaced_contexts() {
        let (layout, node, outside) = fixture();
        let slot = layout.slots[&node];
        let mut memo = EvaluationMemo::for_layout(layout);
        let root = key(node);
        let original = entry(1);
        let records = Arc::clone(&original.records);
        assert!(memo.insert(root.clone(), original).is_none());
        assert_eq!(
            Arc::strong_count(&records),
            2,
            "caller plus one frame owner"
        );
        assert!(memo.overflow.is_empty());
        assert!(Arc::ptr_eq(&memo.slot(slot).unwrap().1.records, &records));

        let alternate = EvalMemoKey {
            context_digest: 9,
            ..root.clone()
        };
        memo.insert(alternate.clone(), entry(2));
        assert_eq!(memo.get(&root).unwrap().last_used, 1);
        assert_eq!(memo.get(&alternate).unwrap().last_used, 2);
        assert_eq!(Arc::strong_count(&records), 2, "spilling moves, not clones");
        assert_eq!(memo.insert(root.clone(), entry(3)).unwrap().last_used, 1);
        assert_eq!(Arc::strong_count(&records), 1);
        assert_eq!(memo.len(), 2);

        let child = EvalMemoKey {
            scope: ScopeId::root().child(node),
            ..root.clone()
        };
        memo.insert(child.clone(), entry(4));
        memo.insert(key(outside), entry(5));
        assert_eq!(memo.len(), 4);
        assert_eq!(memo.slot(slot).unwrap().1.last_used, 3);
        assert_eq!(memo.get(&child).unwrap().last_used, 4);
        assert_eq!(memo.remove(&alternate).unwrap().last_used, 2);
        assert_eq!(memo.remove(&root).unwrap().last_used, 3);
        assert!(memo.slot(slot).is_none());
        memo.retain(|key, _| key.node == node);
        assert_eq!(memo.len(), 1);
        assert_eq!(memo.into_entries().next().unwrap().0, child);
    }

    #[test]
    fn layout_transition_moves_existing_entries_and_keeps_complete_validity_keys() {
        let (layout, node, outside) = fixture();
        let base = key(node);
        let keys = [
            base.clone(),
            EvalMemoKey {
                input_signature_hash: 7,
                ..base.clone()
            },
            EvalMemoKey {
                tick_epoch: None,
                ..base.clone()
            },
            EvalMemoKey {
                tick_epoch: Some(2),
                ..base.clone()
            },
            EvalMemoKey {
                sub_tick: 3,
                ..base.clone()
            },
            EvalMemoKey {
                context_digest: 4,
                ..base.clone()
            },
            EvalMemoKey {
                scope: ScopeId::root().child(node),
                ..base
            },
            key(outside),
        ];
        let mut memo = EvaluationMemo::default();
        for (index, key) in keys.iter().enumerate() {
            memo.insert(key.clone(), entry(index as u64));
        }
        memo.set_layout(Arc::clone(&layout));
        assert_eq!(memo.len(), keys.len());
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(memo.get(key).unwrap().last_used, index as u64);
            memo.get_mut(key).unwrap().last_used += 10;
        }
        // Relayout is also legal after a frame already owns dense entries.
        memo.set_layout(layout);
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(memo.get(key).unwrap().last_used, index as u64 + 10);
            assert_eq!(memo.remove(key).unwrap().input_watermark, index as u64);
        }
        assert!(memo.is_empty());
    }
}
