use ahash::{AHashMap, AHashSet};

use crate::object::ObjectId;
use crate::query_manager::types::{RowDescriptor, Tuple, TupleDelta};

use super::RowNode;

/// Selects a single tuple element from joined tuples.
///
/// This is used by hop-style query lowering to project join output to the
/// target relation rows while preserving stable dedupe semantics by row id.
#[derive(Debug)]
pub struct SelectElementNode {
    element_index: usize,
    output_descriptor: RowDescriptor,
    current_tuples: AHashSet<Tuple>,
    source_to_selected: AHashMap<Vec<ObjectId>, Tuple>,
    selected_refcounts: AHashMap<ObjectId, usize>,
    selected_latest: AHashMap<ObjectId, Tuple>,
    dirty: bool,
}

impl SelectElementNode {
    pub fn new(
        input_descriptor: crate::query_manager::types::TupleDescriptor,
        element_index: usize,
    ) -> Option<Self> {
        if !input_descriptor
            .materialization()
            .is_materialized(element_index)
        {
            return None;
        }
        let output_descriptor = input_descriptor.element(element_index)?.descriptor.clone();
        Some(Self {
            element_index,
            output_descriptor,
            current_tuples: AHashSet::new(),
            source_to_selected: AHashMap::new(),
            selected_refcounts: AHashMap::new(),
            selected_latest: AHashMap::new(),
            dirty: true,
        })
    }

    fn select_tuple(&self, tuple: &Tuple) -> Option<Tuple> {
        let element = tuple.get(self.element_index)?.clone();
        Some(Tuple::new(vec![element]).with_provenance(tuple.provenance().clone()))
    }

    pub fn select_tuple_for_output(&self, tuple: &Tuple) -> Option<Tuple> {
        self.select_tuple(tuple)
    }

    fn tuple_content_changed(old_tuple: &Tuple, new_tuple: &Tuple) -> bool {
        match (old_tuple.to_single_row(), new_tuple.to_single_row()) {
            (Some(old_row), Some(new_row)) => {
                old_row.data != new_row.data || old_row.commit_id != new_row.commit_id
            }
            _ => false,
        }
    }
}

impl RowNode for SelectElementNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.output_descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        let mut result = TupleDelta::new();

        let mut removals = input.removed;
        let mut additions = input.added;
        for (old_tuple, new_tuple) in input.updated {
            removals.push(old_tuple);
            additions.push(new_tuple);
        }

        for source_tuple in removals {
            let source_ids = source_tuple.ids();
            let selected = self
                .source_to_selected
                .remove(&source_ids)
                .or_else(|| self.select_tuple(&source_tuple));

            let Some(selected_tuple) = selected else {
                continue;
            };
            let Some(selected_id) = selected_tuple.first_id() else {
                continue;
            };

            let Some(count) = self.selected_refcounts.get_mut(&selected_id) else {
                continue;
            };
            if *count > 1 {
                *count -= 1;
                continue;
            }

            self.selected_refcounts.remove(&selected_id);
            let removed_tuple = self
                .selected_latest
                .remove(&selected_id)
                .unwrap_or(selected_tuple);
            self.current_tuples.remove(&removed_tuple);
            result.removed.push(removed_tuple);
        }

        for source_tuple in additions {
            let Some(selected_tuple) = self.select_tuple(&source_tuple) else {
                continue;
            };
            let Some(selected_id) = selected_tuple.first_id() else {
                continue;
            };

            let source_ids = source_tuple.ids();
            self.source_to_selected
                .insert(source_ids, selected_tuple.clone());

            let count = self.selected_refcounts.entry(selected_id).or_insert(0);
            if *count == 0 {
                *count = 1;
                self.selected_latest
                    .insert(selected_id, selected_tuple.clone());
                self.current_tuples.insert(selected_tuple.clone());
                result.added.push(selected_tuple);
                continue;
            }

            *count += 1;

            if let Some(previous) = self.selected_latest.get(&selected_id).cloned()
                && Self::tuple_content_changed(&previous, &selected_tuple)
            {
                self.selected_latest
                    .insert(selected_id, selected_tuple.clone());
                self.current_tuples.remove(&previous);
                self.current_tuples.insert(selected_tuple.clone());
                result.updated.push((previous, selected_tuple));
            }
        }

        self.dirty = false;
        result
    }

    fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitId;
    use crate::query_manager::encoding::encode_row;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, TupleDescriptor, TupleElement, Value,
    };

    fn left_desc() -> RowDescriptor {
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)])
    }

    fn right_desc() -> RowDescriptor {
        RowDescriptor::new(vec![ColumnDescriptor::new("title", ColumnType::Text)])
    }

    fn make_tuple(
        left_id: ObjectId,
        left_name: &str,
        right_id: ObjectId,
        right_title: &str,
    ) -> Tuple {
        let left_data = encode_row(&left_desc(), &[Value::Text(left_name.into())]).unwrap();
        let right_data = encode_row(&right_desc(), &[Value::Text(right_title.into())]).unwrap();
        Tuple::new(vec![
            TupleElement::Row {
                id: left_id,
                content: left_data,
                commit_id: CommitId([0; 32]),
            },
            TupleElement::Row {
                id: right_id,
                content: right_data,
                commit_id: CommitId([0; 32]),
            },
        ])
    }

    #[test]
    fn select_element_dedupes_shared_targets() {
        let desc = TupleDescriptor::from_tables(&[
            ("users".to_string(), left_desc()),
            ("projects".to_string(), right_desc()),
        ])
        .with_all_materialized();
        let mut node = SelectElementNode::new(desc, 1).expect("node");

        let left_a = ObjectId::new();
        let left_b = ObjectId::new();
        let right = ObjectId::new();

        let tuple_a = make_tuple(left_a, "Alice", right, "Project X");
        let tuple_b = make_tuple(left_b, "Bob", right, "Project X");

        let first = node.process(TupleDelta {
            added: vec![tuple_a],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });
        assert_eq!(first.added.len(), 1);

        let second = node.process(TupleDelta {
            added: vec![tuple_b],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });
        assert!(
            second.added.is_empty(),
            "same target id should not be emitted twice"
        );
        assert_eq!(node.current_tuples().len(), 1);
    }
}
