use ahash::{AHashMap, AHashSet};

use crate::object::ObjectId;
use crate::query_manager::encoding::column_bytes;
use crate::query_manager::types::{RowDescriptor, Tuple, TupleDelta, TupleDescriptor};

/// Join node for equi-joins.
///
/// Performs a nested loop join with hash lookup optimization.
/// Combines tuples from left and right inputs where join columns match.
///
/// Example: `users JOIN posts ON users.id = posts.author_id`
#[derive(Debug)]
pub struct JoinNode {
    /// Left side tuple descriptor (for element count).
    left_descriptor: TupleDescriptor,
    /// Output tuple descriptor (concat of left and right).
    output_descriptor: TupleDescriptor,
    /// Left side row descriptor (for column extraction).
    left_row_descriptor: RowDescriptor,
    /// Right side row descriptor (for column extraction).
    right_row_descriptor: RowDescriptor,
    /// Left element index that contains the join column.
    left_element_index: usize,
    /// Right element index that contains the join column.
    right_element_index: usize,
    /// Local column index within left element.
    left_local_col_index: usize,
    /// Local column index within right element.
    right_local_col_index: usize,

    /// Current left tuples.
    left_tuples: AHashSet<Tuple>,
    /// Current right tuples.
    right_tuples: AHashSet<Tuple>,
    /// Current joined output tuples.
    current_tuples: AHashSet<Tuple>,

    /// Index: join key -> left tuples with that key.
    left_by_key: AHashMap<Vec<u8>, AHashSet<Tuple>>,
    /// Index: join key -> right tuples with that key.
    right_by_key: AHashMap<Vec<u8>, AHashSet<Tuple>>,

    /// Track which output tuples came from which left tuple.
    left_to_output: AHashMap<Vec<ObjectId>, AHashSet<Tuple>>,
    /// Track which output tuples came from which right tuple.
    right_to_output: AHashMap<Vec<ObjectId>, AHashSet<Tuple>>,

    dirty: bool,
}

impl JoinNode {
    /// Create a new join node with TupleDescriptors.
    ///
    /// # Arguments
    /// * `left_desc` - Tuple descriptor for left side
    /// * `right_desc` - Tuple descriptor for right side
    /// * `left_col` - Column name on left side for join
    /// * `right_col` - Column name on right side for join
    ///
    /// # Returns
    /// None if:
    /// - Join columns don't exist
    /// - Left join column is not materialized (needed to extract join key)
    pub fn new(
        left_desc: TupleDescriptor,
        right_desc: TupleDescriptor,
        left_col: &str,
        right_col: &str,
    ) -> Option<Self> {
        // Find left column global index
        let left_col_index = left_desc.column_index(left_col)?;
        // Find right column global index
        let right_col_index = right_desc.column_index(right_col)?;

        // Resolve left column to element and local index
        let (left_elem_idx, left_local_idx) = left_desc.resolve_column(left_col_index)?;
        // Resolve right column to element and local index
        let (right_elem_idx, right_local_idx) = right_desc.resolve_column(right_col_index)?;

        // Validate: left join column must be materialized to extract join key
        if !left_desc.materialization().is_materialized(left_elem_idx) {
            return None;
        }

        // Get row descriptors for column extraction
        let left_row_desc = left_desc.element(left_elem_idx)?.descriptor.clone();
        let right_row_desc = right_desc.element(right_elem_idx)?.descriptor.clone();

        // Output descriptor is concat of left and right
        let output_descriptor = TupleDescriptor::concat(&left_desc, &right_desc);

        Some(Self {
            left_descriptor: left_desc,
            output_descriptor,
            left_row_descriptor: left_row_desc,
            right_row_descriptor: right_row_desc,
            left_element_index: left_elem_idx,
            right_element_index: right_elem_idx,
            left_local_col_index: left_local_idx,
            right_local_col_index: right_local_idx,
            left_tuples: AHashSet::new(),
            right_tuples: AHashSet::new(),
            current_tuples: AHashSet::new(),
            left_by_key: AHashMap::new(),
            right_by_key: AHashMap::new(),
            left_to_output: AHashMap::new(),
            right_to_output: AHashMap::new(),
            dirty: true,
        })
    }

    /// Create a new join node with RowDescriptors (convenience for single-element tuples).
    /// Creates TupleDescriptors internally with materialized state.
    pub fn from_row_descriptors(
        left_table: &str,
        left_desc: RowDescriptor,
        right_table: &str,
        right_desc: RowDescriptor,
        left_col: &str,
        right_col: &str,
    ) -> Option<Self> {
        // Create tuple descriptors with left materialized, right as ID-only
        let left_tuple_desc =
            TupleDescriptor::single_with_materialization(left_table, left_desc, true);
        let right_tuple_desc = TupleDescriptor::single(right_table, right_desc);
        Self::new(left_tuple_desc, right_tuple_desc, left_col, right_col)
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_descriptor
    }

    /// Extract join key from a left tuple.
    fn extract_left_key(&self, tuple: &Tuple) -> Option<Vec<u8>> {
        let element = tuple.get(self.left_element_index)?;
        let content = element.content()?;
        column_bytes(
            &self.left_row_descriptor,
            content,
            self.left_local_col_index,
        )
        .ok()
        .flatten()
        .map(|b| b.to_vec())
    }

    /// Extract join key from a right tuple.
    fn extract_right_key(&self, tuple: &Tuple) -> Option<Vec<u8>> {
        let element = tuple.get(self.right_element_index)?;
        let content = element.content()?;
        column_bytes(
            &self.right_row_descriptor,
            content,
            self.right_local_col_index,
        )
        .ok()
        .flatten()
        .map(|b| b.to_vec())
    }

    /// Create a combined tuple from left and right tuples.
    fn combine_tuples(&self, left: &Tuple, right: &Tuple) -> Tuple {
        let mut elements = Vec::with_capacity(left.len() + right.len());
        elements.extend(left.iter().cloned());
        elements.extend(right.iter().cloned());
        Tuple::new(elements)
    }

    /// Add a left tuple and compute new output tuples.
    fn add_left_tuple(&mut self, tuple: Tuple) -> Vec<Tuple> {
        let mut new_outputs = Vec::new();

        if let Some(key) = self.extract_left_key(&tuple) {
            // Add to left index
            self.left_by_key
                .entry(key.clone())
                .or_default()
                .insert(tuple.clone());

            // Find matching right tuples
            if let Some(right_matches) = self.right_by_key.get(&key) {
                for right_tuple in right_matches {
                    let combined = self.combine_tuples(&tuple, right_tuple);

                    // Track provenance
                    self.left_to_output
                        .entry(tuple.ids())
                        .or_default()
                        .insert(combined.clone());
                    self.right_to_output
                        .entry(right_tuple.ids())
                        .or_default()
                        .insert(combined.clone());

                    self.current_tuples.insert(combined.clone());
                    new_outputs.push(combined);
                }
            }
        }

        self.left_tuples.insert(tuple);
        new_outputs
    }

    /// Remove a left tuple and compute removed output tuples.
    fn remove_left_tuple(&mut self, tuple: &Tuple) -> Vec<Tuple> {
        let mut removed_outputs = Vec::new();

        if let Some(key) = self.extract_left_key(tuple) {
            // Remove from left index
            if let Some(set) = self.left_by_key.get_mut(&key) {
                set.remove(tuple);
                if set.is_empty() {
                    self.left_by_key.remove(&key);
                }
            }
        }

        // Remove output tuples that came from this left tuple
        if let Some(outputs) = self.left_to_output.remove(&tuple.ids()) {
            for output in outputs {
                self.current_tuples.remove(&output);

                // Also remove from right_to_output tracking
                let right_ids = output
                    .iter()
                    .skip(tuple.len())
                    .map(|e| e.id())
                    .collect::<Vec<_>>();
                if let Some(right_outputs) = self.right_to_output.get_mut(&right_ids) {
                    right_outputs.remove(&output);
                }

                removed_outputs.push(output);
            }
        }

        self.left_tuples.remove(tuple);
        removed_outputs
    }

    /// Add a right tuple and compute new output tuples.
    fn add_right_tuple(&mut self, tuple: Tuple) -> Vec<Tuple> {
        let mut new_outputs = Vec::new();

        if let Some(key) = self.extract_right_key(&tuple) {
            // Add to right index
            self.right_by_key
                .entry(key.clone())
                .or_default()
                .insert(tuple.clone());

            // Find matching left tuples
            if let Some(left_matches) = self.left_by_key.get(&key) {
                for left_tuple in left_matches {
                    let combined = self.combine_tuples(left_tuple, &tuple);

                    // Track provenance
                    self.left_to_output
                        .entry(left_tuple.ids())
                        .or_default()
                        .insert(combined.clone());
                    self.right_to_output
                        .entry(tuple.ids())
                        .or_default()
                        .insert(combined.clone());

                    self.current_tuples.insert(combined.clone());
                    new_outputs.push(combined);
                }
            }
        }

        self.right_tuples.insert(tuple);
        new_outputs
    }

    /// Remove a right tuple and compute removed output tuples.
    fn remove_right_tuple(&mut self, tuple: &Tuple) -> Vec<Tuple> {
        let mut removed_outputs = Vec::new();

        if let Some(key) = self.extract_right_key(tuple) {
            // Remove from right index
            if let Some(set) = self.right_by_key.get_mut(&key) {
                set.remove(tuple);
                if set.is_empty() {
                    self.right_by_key.remove(&key);
                }
            }
        }

        // Remove output tuples that came from this right tuple
        if let Some(outputs) = self.right_to_output.remove(&tuple.ids()) {
            for output in outputs {
                self.current_tuples.remove(&output);

                // Also remove from left_to_output tracking
                // Left tuple IDs are the first N elements
                let left_len = self.left_descriptor.element_count();
                let left_ids = output
                    .iter()
                    .take(left_len)
                    .map(|e| e.id())
                    .collect::<Vec<_>>();
                if let Some(left_outputs) = self.left_to_output.get_mut(&left_ids) {
                    left_outputs.remove(&output);
                }

                removed_outputs.push(output);
            }
        }

        self.right_tuples.remove(tuple);
        removed_outputs
    }

    /// Process left side delta.
    pub fn process_left(&mut self, delta: TupleDelta) -> TupleDelta {
        let input_size = delta.added.len() + delta.removed.len() + delta.updated.len();
        let mut result = TupleDelta::new();

        // Handle removals first
        for tuple in delta.removed {
            result.removed.extend(self.remove_left_tuple(&tuple));
        }

        // Handle additions
        for tuple in delta.added {
            result.added.extend(self.add_left_tuple(tuple));
        }

        // Handle updates (remove old, add new)
        for (old_tuple, new_tuple) in delta.updated {
            result.removed.extend(self.remove_left_tuple(&old_tuple));
            result.added.extend(self.add_left_tuple(new_tuple));
        }

        let output_size = result.added.len() + result.removed.len() + result.updated.len();
        tracing::trace!(
            input_size,
            output_size,
            side = "left",
            "join node processed"
        );

        self.dirty = false;
        result
    }

    /// Process right side delta.
    pub fn process_right(&mut self, delta: TupleDelta) -> TupleDelta {
        let input_size = delta.added.len() + delta.removed.len() + delta.updated.len();
        let mut result = TupleDelta::new();

        // Handle removals first
        for tuple in delta.removed {
            result.removed.extend(self.remove_right_tuple(&tuple));
        }

        // Handle additions
        for tuple in delta.added {
            result.added.extend(self.add_right_tuple(tuple));
        }

        // Handle updates (remove old, add new)
        for (old_tuple, new_tuple) in delta.updated {
            result.removed.extend(self.remove_right_tuple(&old_tuple));
            result.added.extend(self.add_right_tuple(new_tuple));
        }

        let output_size = result.added.len() + result.removed.len() + result.updated.len();
        tracing::trace!(
            input_size,
            output_size,
            side = "right",
            "join node processed"
        );

        self.dirty = false;
        result
    }

    /// Get current joined tuples.
    pub fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    /// Mark this node as dirty.
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Check if this node needs reprocessing.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitId;
    use crate::query_manager::encoding::encode_row;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, TupleElement, Value};

    fn users_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
    }

    fn posts_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
    }

    fn make_user_tuple(id: ObjectId, user_id: i32, name: &str) -> Tuple {
        let descriptor = users_descriptor();
        let data = encode_row(
            &descriptor,
            &[Value::Integer(user_id), Value::Text(name.into())],
        )
        .unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }])
    }

    fn make_post_tuple(id: ObjectId, post_id: i32, title: &str, author_id: i32) -> Tuple {
        let descriptor = posts_descriptor();
        let data = encode_row(
            &descriptor,
            &[
                Value::Integer(post_id),
                Value::Text(title.into()),
                Value::Integer(author_id),
            ],
        )
        .unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }])
    }

    #[test]
    fn join_matches_on_key() {
        let mut node = JoinNode::from_row_descriptors(
            "users",
            users_descriptor(),
            "posts",
            posts_descriptor(),
            "id",        // users.id
            "author_id", // posts.author_id
        )
        .unwrap();

        let user_oid = ObjectId::new();
        let post_oid = ObjectId::new();

        let user = make_user_tuple(user_oid, 1, "Alice");
        let post = make_post_tuple(post_oid, 100, "Hello World", 1);

        // Add user
        let result1 = node.process_left(TupleDelta {
            added: vec![user.clone()],
            removed: vec![],
            updated: vec![],
        });
        assert!(result1.added.is_empty(), "No match yet");

        // Add post with matching author_id
        let result2 = node.process_right(TupleDelta {
            added: vec![post.clone()],
            removed: vec![],
            updated: vec![],
        });

        assert_eq!(result2.added.len(), 1);
        let joined = &result2.added[0];
        assert_eq!(joined.len(), 2); // Combined: [user_element, post_element]
        assert_eq!(joined.ids(), vec![user_oid, post_oid]);
    }

    #[test]
    fn join_no_match() {
        let mut node = JoinNode::from_row_descriptors(
            "users",
            users_descriptor(),
            "posts",
            posts_descriptor(),
            "id",
            "author_id",
        )
        .unwrap();

        let user_oid = ObjectId::new();
        let post_oid = ObjectId::new();

        let user = make_user_tuple(user_oid, 1, "Alice");
        let post = make_post_tuple(post_oid, 100, "Hello World", 999); // Different author_id

        node.process_left(TupleDelta {
            added: vec![user],
            removed: vec![],
            updated: vec![],
        });

        let result = node.process_right(TupleDelta {
            added: vec![post],
            removed: vec![],
            updated: vec![],
        });

        assert!(result.added.is_empty(), "No match - different keys");
    }

    #[test]
    fn join_multiple_matches() {
        let mut node = JoinNode::from_row_descriptors(
            "users",
            users_descriptor(),
            "posts",
            posts_descriptor(),
            "id",
            "author_id",
        )
        .unwrap();

        let user_oid = ObjectId::new();
        let post1_oid = ObjectId::new();
        let post2_oid = ObjectId::new();

        let user = make_user_tuple(user_oid, 1, "Alice");
        let post1 = make_post_tuple(post1_oid, 100, "Post 1", 1);
        let post2 = make_post_tuple(post2_oid, 101, "Post 2", 1);

        // Add user
        node.process_left(TupleDelta {
            added: vec![user],
            removed: vec![],
            updated: vec![],
        });

        // Add two posts with same author_id
        let result = node.process_right(TupleDelta {
            added: vec![post1, post2],
            removed: vec![],
            updated: vec![],
        });

        assert_eq!(result.added.len(), 2);
        assert_eq!(node.current_tuples().len(), 2);
    }

    #[test]
    fn join_removal_removes_output() {
        let mut node = JoinNode::from_row_descriptors(
            "users",
            users_descriptor(),
            "posts",
            posts_descriptor(),
            "id",
            "author_id",
        )
        .unwrap();

        let user_oid = ObjectId::new();
        let post_oid = ObjectId::new();

        let user = make_user_tuple(user_oid, 1, "Alice");
        let post = make_post_tuple(post_oid, 100, "Hello", 1);

        // Add both
        node.process_left(TupleDelta {
            added: vec![user.clone()],
            removed: vec![],
            updated: vec![],
        });
        node.process_right(TupleDelta {
            added: vec![post.clone()],
            removed: vec![],
            updated: vec![],
        });
        assert_eq!(node.current_tuples().len(), 1);

        // Remove post
        let result = node.process_right(TupleDelta {
            added: vec![],
            removed: vec![post],
            updated: vec![],
        });

        assert_eq!(result.removed.len(), 1);
        assert!(node.current_tuples().is_empty());
    }

    #[test]
    fn join_order_independent() {
        // Adding right before left should produce same result
        let mut node = JoinNode::from_row_descriptors(
            "users",
            users_descriptor(),
            "posts",
            posts_descriptor(),
            "id",
            "author_id",
        )
        .unwrap();

        let user_oid = ObjectId::new();
        let post_oid = ObjectId::new();

        let user = make_user_tuple(user_oid, 1, "Alice");
        let post = make_post_tuple(post_oid, 100, "Hello", 1);

        // Add post first
        let result1 = node.process_right(TupleDelta {
            added: vec![post],
            removed: vec![],
            updated: vec![],
        });
        assert!(result1.added.is_empty(), "No match yet");

        // Add user
        let result2 = node.process_left(TupleDelta {
            added: vec![user],
            removed: vec![],
            updated: vec![],
        });

        assert_eq!(result2.added.len(), 1);
        assert_eq!(node.current_tuples().len(), 1);
    }
}
