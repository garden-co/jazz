use ahash::{AHashMap, AHashSet};

use crate::object::ObjectId;
use crate::query_manager::encoding::{column_bytes, decode_column, encode_value};
use crate::query_manager::types::{
    ColumnType, RowDescriptor, Tuple, TupleDelta, TupleDescriptor, Value,
};

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
    /// Left side key extraction spec.
    left_key_spec: JoinKeySpec,
    /// Right side key extraction spec.
    right_key_spec: JoinKeySpec,

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

/// Parsed join column reference.
///
/// Supports either unqualified (`id`) or qualified (`users.id`, `u.id`) forms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinColumnRef {
    /// Optional table/alias qualifier.
    pub qualifier: Option<String>,
    /// Column name.
    pub column: String,
}

impl JoinColumnRef {
    /// Parse a join column reference from a string.
    pub fn parse(raw: &str) -> Self {
        let trimmed = raw.trim();
        if let Some((qualifier, column)) = trimmed.rsplit_once('.') {
            let qualifier = qualifier.trim();
            let column = column.trim();
            if !qualifier.is_empty() && !column.is_empty() {
                return Self {
                    qualifier: Some(qualifier.to_string()),
                    column: column.to_string(),
                };
            }
        }
        Self {
            qualifier: None,
            column: trimmed.to_string(),
        }
    }

    fn is_id_like(&self) -> bool {
        self.column == "id" || self.column == "_id"
    }
}

#[derive(Debug, Clone)]
enum JoinKeySpec {
    /// Join using the tuple element's object identity (implicit id).
    TupleId { element_index: usize },
    /// Join using an explicit column from a materialized row.
    Column {
        element_index: usize,
        row_descriptor: RowDescriptor,
        local_col_index: usize,
        match_array_elements: bool,
    },
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
        Self::new_with_refs(
            left_desc,
            right_desc,
            JoinColumnRef::parse(left_col),
            JoinColumnRef::parse(right_col),
        )
    }

    /// Create a new join node from parsed column references.
    pub fn new_with_refs(
        left_desc: TupleDescriptor,
        right_desc: TupleDescriptor,
        left_ref: JoinColumnRef,
        right_ref: JoinColumnRef,
    ) -> Option<Self> {
        let left_key_spec = Self::resolve_join_key_spec(&left_desc, &left_ref)?;
        let right_key_spec = Self::resolve_join_key_spec(&right_desc, &right_ref)?;

        // Output descriptor is concat of left and right
        let output_descriptor = TupleDescriptor::concat(&left_desc, &right_desc);

        Some(Self {
            left_descriptor: left_desc,
            output_descriptor,
            left_key_spec,
            right_key_spec,
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
        // Create tuple descriptors with both sides materialized.
        let left_tuple_desc =
            TupleDescriptor::single_with_materialization(left_table, left_desc, true);
        let right_tuple_desc =
            TupleDescriptor::single_with_materialization(right_table, right_desc, true);
        Self::new(left_tuple_desc, right_tuple_desc, left_col, right_col)
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_descriptor
    }

    /// Extract join keys from a left tuple.
    fn extract_left_keys(&self, tuple: &Tuple) -> Vec<Vec<u8>> {
        self.extract_keys(tuple, &self.left_key_spec)
    }

    /// Extract join keys from a right tuple.
    fn extract_right_keys(&self, tuple: &Tuple) -> Vec<Vec<u8>> {
        self.extract_keys(tuple, &self.right_key_spec)
    }

    fn encode_unique_values(values: &[Value]) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(values.len());
        let mut seen = AHashSet::new();
        for value in values {
            let encoded = encode_value(value);
            if seen.insert(encoded.clone()) {
                out.push(encoded);
            }
        }
        out
    }

    fn extract_keys(&self, tuple: &Tuple, spec: &JoinKeySpec) -> Vec<Vec<u8>> {
        match spec {
            JoinKeySpec::TupleId { element_index } => {
                let Some(id) = tuple.get(*element_index).map(|element| element.id()) else {
                    return Vec::new();
                };
                vec![id.uuid().as_bytes().to_vec()]
            }
            JoinKeySpec::Column {
                element_index,
                row_descriptor,
                local_col_index,
                match_array_elements,
            } => {
                let Some(element) = tuple.get(*element_index) else {
                    return Vec::new();
                };
                let Some(content) = element.content() else {
                    return Vec::new();
                };
                if !match_array_elements {
                    return column_bytes(row_descriptor, content, *local_col_index)
                        .ok()
                        .flatten()
                        .map(|bytes| vec![bytes.to_vec()])
                        .unwrap_or_default();
                }

                let Ok(Value::Array(values)) =
                    decode_column(row_descriptor, content, *local_col_index)
                else {
                    return Vec::new();
                };
                Self::encode_unique_values(&values)
            }
        }
    }

    fn resolve_join_key_spec(
        tuple_descriptor: &TupleDescriptor,
        column_ref: &JoinColumnRef,
    ) -> Option<JoinKeySpec> {
        if let Some(qualifier) = column_ref.qualifier.as_deref() {
            return Self::resolve_qualified_key_spec(tuple_descriptor, qualifier, column_ref);
        }

        Self::resolve_unqualified_key_spec(tuple_descriptor, column_ref)
    }

    fn resolve_qualified_key_spec(
        tuple_descriptor: &TupleDescriptor,
        qualifier: &str,
        column_ref: &JoinColumnRef,
    ) -> Option<JoinKeySpec> {
        let mut matched_element_indices = tuple_descriptor
            .iter()
            .enumerate()
            .filter_map(|(index, element)| (element.table == qualifier).then_some(index));
        let element_index = matched_element_indices.next()?;
        if matched_element_indices.next().is_some() {
            return None;
        }

        let element = tuple_descriptor.element(element_index)?;
        if let Some(local_col_index) = element.descriptor.column_index(&column_ref.column) {
            let match_array_elements = matches!(
                element.descriptor.columns[local_col_index].column_type,
                ColumnType::Array { element: _ }
            );
            return Some(JoinKeySpec::Column {
                element_index,
                row_descriptor: element.descriptor.clone(),
                local_col_index,
                match_array_elements,
            });
        }

        if column_ref.is_id_like() {
            return Some(JoinKeySpec::TupleId { element_index });
        }

        None
    }

    fn resolve_unqualified_key_spec(
        tuple_descriptor: &TupleDescriptor,
        column_ref: &JoinColumnRef,
    ) -> Option<JoinKeySpec> {
        let matches: Vec<(usize, usize, RowDescriptor)> = tuple_descriptor
            .iter()
            .enumerate()
            .filter_map(|(index, element)| {
                element
                    .descriptor
                    .column_index(&column_ref.column)
                    .map(|local_index| (index, local_index, element.descriptor.clone()))
            })
            .collect();

        if matches.len() == 1 {
            let (element_index, local_col_index, row_descriptor) = matches[0].clone();
            let match_array_elements = matches!(
                row_descriptor.columns[local_col_index].column_type,
                ColumnType::Array { element: _ }
            );
            return Some(JoinKeySpec::Column {
                element_index,
                row_descriptor,
                local_col_index,
                match_array_elements,
            });
        }

        if matches.is_empty() && column_ref.is_id_like() && tuple_descriptor.element_count() == 1 {
            return Some(JoinKeySpec::TupleId { element_index: 0 });
        }

        None
    }

    /// Create a combined tuple from left and right tuples.
    fn combine_tuples(&self, left: &Tuple, right: &Tuple) -> Tuple {
        let mut elements = Vec::with_capacity(left.len() + right.len());
        elements.extend(left.iter().cloned());
        elements.extend(right.iter().cloned());
        let mut combined = Tuple::new(elements)
            .with_provenance(left.provenance().clone())
            .with_batch_provenance(left.batch_provenance().clone());
        combined.merge_provenance(right.provenance());
        combined.merge_batch_provenance(right.batch_provenance());
        combined
    }

    /// Add a left tuple and compute new output tuples.
    fn add_left_tuple(&mut self, tuple: Tuple) -> Vec<Tuple> {
        let mut new_outputs = Vec::new();

        let keys = self.extract_left_keys(&tuple);
        let mut right_matches = AHashSet::new();
        for key in &keys {
            self.left_by_key
                .entry(key.clone())
                .or_default()
                .insert(tuple.clone());
            if let Some(matches_for_key) = self.right_by_key.get(key) {
                right_matches.extend(matches_for_key.iter().cloned());
            }
        }

        for right_tuple in right_matches {
            let combined = self.combine_tuples(&tuple, &right_tuple);

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

        self.left_tuples.insert(tuple);
        new_outputs
    }

    /// Remove a left tuple and compute removed output tuples.
    fn remove_left_tuple(&mut self, tuple: &Tuple) -> Vec<Tuple> {
        let mut removed_outputs = Vec::new();

        for key in self.extract_left_keys(tuple) {
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

        let keys = self.extract_right_keys(&tuple);
        let mut left_matches = AHashSet::new();
        for key in &keys {
            self.right_by_key
                .entry(key.clone())
                .or_default()
                .insert(tuple.clone());
            if let Some(matches_for_key) = self.left_by_key.get(key) {
                left_matches.extend(matches_for_key.iter().cloned());
            }
        }

        for left_tuple in left_matches {
            let combined = self.combine_tuples(&left_tuple, &tuple);

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

        self.right_tuples.insert(tuple);
        new_outputs
    }

    /// Remove a right tuple and compute removed output tuples.
    fn remove_right_tuple(&mut self, tuple: &Tuple) -> Vec<Tuple> {
        let mut removed_outputs = Vec::new();

        for key in self.extract_right_keys(tuple) {
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

    fn users_without_explicit_id_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)])
    }

    fn posts_uuid_fk_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Uuid),
        ])
    }

    fn files_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![ColumnDescriptor::new(
            "parts",
            ColumnType::Array {
                element: Box::new(ColumnType::Uuid),
            },
        )])
    }

    fn file_parts_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)])
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
            content: data.into(),
            version_id: CommitId([0; 32]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
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
            content: data.into(),
            version_id: CommitId([0; 32]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }])
    }

    fn make_user_tuple_without_explicit_id(id: ObjectId, name: &str) -> Tuple {
        let descriptor = users_without_explicit_id_descriptor();
        let data = encode_row(&descriptor, &[Value::Text(name.into())]).unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data.into(),
            version_id: CommitId([0; 32]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }])
    }

    fn make_post_uuid_fk_tuple(id: ObjectId, title: &str, author_id: ObjectId) -> Tuple {
        let descriptor = posts_uuid_fk_descriptor();
        let data = encode_row(
            &descriptor,
            &[Value::Text(title.into()), Value::Uuid(author_id)],
        )
        .unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data.into(),
            version_id: CommitId([0; 32]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }])
    }

    fn make_file_tuple(id: ObjectId, parts: Vec<ObjectId>) -> Tuple {
        let descriptor = files_descriptor();
        let data = encode_row(
            &descriptor,
            &[Value::Array(parts.into_iter().map(Value::Uuid).collect())],
        )
        .unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data.into(),
            version_id: CommitId([0; 32]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }])
    }

    fn make_file_part_tuple(id: ObjectId, name: &str) -> Tuple {
        let descriptor = file_parts_descriptor();
        let data = encode_row(&descriptor, &[Value::Text(name.into())]).unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data.into(),
            version_id: CommitId([0; 32]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }])
    }

    #[test]
    fn join_matches_array_membership_for_forward_fk_hops() {
        let mut node = JoinNode::from_row_descriptors(
            "files",
            files_descriptor(),
            "file_parts",
            file_parts_descriptor(),
            "parts",
            "id",
        )
        .expect("array-fk forward join should compile");

        let file_id = ObjectId::new();
        let part_a = ObjectId::new();
        let part_b = ObjectId::new();

        let file = make_file_tuple(file_id, vec![part_a, part_b, part_a]);
        let row_a = make_file_part_tuple(part_a, "A");
        let row_b = make_file_part_tuple(part_b, "B");

        node.process_left(TupleDelta {
            added: vec![file],
            ..Default::default()
        });
        let delta = node.process_right(TupleDelta {
            added: vec![row_a, row_b],
            ..Default::default()
        });

        assert_eq!(
            delta.added.len(),
            2,
            "membership join should match both part ids"
        );
        assert_eq!(
            node.current_tuples().len(),
            2,
            "join outputs are row-set semantics (deduped by tuple identity)"
        );
    }

    #[test]
    fn join_matches_array_membership_for_reverse_fk_hops() {
        let mut node = JoinNode::from_row_descriptors(
            "file_parts",
            file_parts_descriptor(),
            "files",
            files_descriptor(),
            "id",
            "parts",
        )
        .expect("array-fk reverse join should compile");

        let file_id = ObjectId::new();
        let part_a = ObjectId::new();
        let part_b = ObjectId::new();

        let file = make_file_tuple(file_id, vec![part_a, part_b]);
        let row_a = make_file_part_tuple(part_a, "A");
        let row_b = make_file_part_tuple(part_b, "B");

        node.process_right(TupleDelta {
            added: vec![file],
            ..Default::default()
        });
        let delta = node.process_left(TupleDelta {
            added: vec![row_a, row_b],
            ..Default::default()
        });

        assert_eq!(
            delta.added.len(),
            2,
            "reverse membership join should match file rows containing each part id"
        );
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
            moved: vec![],
            updated: vec![],
        });
        assert!(result1.added.is_empty(), "No match yet");

        // Add post with matching author_id
        let result2 = node.process_right(TupleDelta {
            added: vec![post.clone()],
            removed: vec![],
            moved: vec![],
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
            moved: vec![],
            updated: vec![],
        });

        let result = node.process_right(TupleDelta {
            added: vec![post],
            removed: vec![],
            moved: vec![],
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
            moved: vec![],
            updated: vec![],
        });

        // Add two posts with same author_id
        let result = node.process_right(TupleDelta {
            added: vec![post1, post2],
            removed: vec![],
            moved: vec![],
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
            moved: vec![],
            updated: vec![],
        });
        node.process_right(TupleDelta {
            added: vec![post.clone()],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });
        assert_eq!(node.current_tuples().len(), 1);

        // Remove post
        let result = node.process_right(TupleDelta {
            added: vec![],
            removed: vec![post],
            moved: vec![],
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
            moved: vec![],
            updated: vec![],
        });
        assert!(result1.added.is_empty(), "No match yet");

        // Add user
        let result2 = node.process_left(TupleDelta {
            added: vec![user],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });

        assert_eq!(result2.added.len(), 1);
        assert_eq!(node.current_tuples().len(), 1);
    }

    #[test]
    fn join_supports_implicit_id_on_left() {
        let mut node = JoinNode::from_row_descriptors(
            "users",
            users_without_explicit_id_descriptor(),
            "posts",
            posts_uuid_fk_descriptor(),
            "users.id",
            "posts.author_id",
        )
        .expect("Join with implicit id should compile");

        let user_oid = ObjectId::new();
        let post_oid = ObjectId::new();

        let user = make_user_tuple_without_explicit_id(user_oid, "Alice");
        let post = make_post_uuid_fk_tuple(post_oid, "Implicit Id Join", user_oid);

        node.process_left(TupleDelta {
            added: vec![user],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });
        let result = node.process_right(TupleDelta {
            added: vec![post],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });

        assert_eq!(result.added.len(), 1, "Implicit id should match uuid FK");
        assert_eq!(result.added[0].ids(), vec![user_oid, post_oid]);
    }

    #[test]
    fn join_rejects_ambiguous_unqualified_implicit_id() {
        let left = TupleDescriptor::from_tables(&[
            ("users".to_string(), users_without_explicit_id_descriptor()),
            ("teams".to_string(), users_without_explicit_id_descriptor()),
        ])
        .with_all_materialized();
        let right =
            TupleDescriptor::single_with_materialization("posts", posts_uuid_fk_descriptor(), true);

        let node = JoinNode::new_with_refs(
            left,
            right,
            JoinColumnRef::parse("id"),
            JoinColumnRef::parse("author_id"),
        );
        assert!(
            node.is_none(),
            "Unqualified implicit id should fail when multiple left elements exist"
        );
    }
}
