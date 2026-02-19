use ahash::AHashSet;
use std::cmp::Ordering;
use std::collections::HashSet;

use crate::query_manager::encoding::{column_bytes, column_is_null, compare_column_to_value};
use crate::query_manager::types::{Row, RowDescriptor, Tuple, TupleDelta, TupleDescriptor};

use super::RowNode;

/// A single predicate condition for filtering.
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Column equals value.
    Eq { col_index: usize, value: Vec<u8> },
    /// Column not equals value.
    Ne { col_index: usize, value: Vec<u8> },
    /// Column less than value.
    Lt { col_index: usize, value: Vec<u8> },
    /// Column less than or equal to value.
    Le { col_index: usize, value: Vec<u8> },
    /// Column greater than value.
    Gt { col_index: usize, value: Vec<u8> },
    /// Column greater than or equal to value.
    Ge { col_index: usize, value: Vec<u8> },
    /// Column is null.
    IsNull { col_index: usize },
    /// Column is not null.
    IsNotNull { col_index: usize },
    /// Logical AND of predicates.
    And(Vec<Predicate>),
    /// Logical OR of predicates.
    Or(Vec<Predicate>),
    /// Logical NOT of a predicate.
    Not(Box<Predicate>),
    /// Always true.
    True,
}

impl Predicate {
    /// Returns all column indices referenced by this predicate.
    pub fn required_columns(&self) -> HashSet<usize> {
        match self {
            Predicate::Eq { col_index, .. }
            | Predicate::Ne { col_index, .. }
            | Predicate::Lt { col_index, .. }
            | Predicate::Le { col_index, .. }
            | Predicate::Gt { col_index, .. }
            | Predicate::Ge { col_index, .. } => [*col_index].into_iter().collect(),
            Predicate::IsNull { col_index } | Predicate::IsNotNull { col_index } => {
                [*col_index].into_iter().collect()
            }
            Predicate::And(preds) | Predicate::Or(preds) => {
                preds.iter().flat_map(|p| p.required_columns()).collect()
            }
            Predicate::Not(pred) => pred.required_columns(),
            Predicate::True => HashSet::new(),
        }
    }

    /// Evaluate the predicate against a row.
    pub fn evaluate(&self, descriptor: &RowDescriptor, row: &Row) -> bool {
        match self {
            Predicate::Eq { col_index, value } => {
                match column_bytes(descriptor, &row.data, *col_index) {
                    Ok(Some(bytes)) => bytes == value.as_slice(),
                    _ => false,
                }
            }
            Predicate::Ne { col_index, value } => {
                match column_bytes(descriptor, &row.data, *col_index) {
                    Ok(Some(bytes)) => bytes != value.as_slice(),
                    Ok(None) => true, // null != value
                    Err(_) => false,
                }
            }
            Predicate::Lt { col_index, value } => {
                matches!(
                    compare_column_to_value(descriptor, &row.data, *col_index, value),
                    Ok(Ordering::Less)
                )
            }
            Predicate::Le { col_index, value } => {
                matches!(
                    compare_column_to_value(descriptor, &row.data, *col_index, value),
                    Ok(Ordering::Less) | Ok(Ordering::Equal)
                )
            }
            Predicate::Gt { col_index, value } => {
                matches!(
                    compare_column_to_value(descriptor, &row.data, *col_index, value),
                    Ok(Ordering::Greater)
                )
            }
            Predicate::Ge { col_index, value } => {
                matches!(
                    compare_column_to_value(descriptor, &row.data, *col_index, value),
                    Ok(Ordering::Greater) | Ok(Ordering::Equal)
                )
            }
            Predicate::IsNull { col_index } => {
                column_is_null(descriptor, &row.data, *col_index).unwrap_or(false)
            }
            Predicate::IsNotNull { col_index } => {
                !column_is_null(descriptor, &row.data, *col_index).unwrap_or(true)
            }
            Predicate::And(predicates) => predicates.iter().all(|p| p.evaluate(descriptor, row)),
            Predicate::Or(predicates) => predicates.iter().any(|p| p.evaluate(descriptor, row)),
            Predicate::Not(predicate) => !predicate.evaluate(descriptor, row),
            Predicate::True => true,
        }
    }
}

/// Filter node for in-memory row filtering.
///
/// Takes a TupleDescriptor and validates that all elements required
/// for predicate evaluation are materialized.
#[derive(Debug)]
pub struct FilterNode {
    /// Tuple descriptor for multi-element tuple support.
    tuple_descriptor: TupleDescriptor,
    /// Output tuple descriptor (same as input - pass-through).
    output_tuple_descriptor: TupleDescriptor,
    /// Combined row descriptor (for output_descriptor trait method).
    combined_descriptor: RowDescriptor,
    predicate: Predicate,
    /// Cached set of element indices required for predicate evaluation.
    required_elements: HashSet<usize>,
    /// Current tuples that pass the filter.
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
}

impl FilterNode {
    /// Create a FilterNode with a full TupleDescriptor, validating materialization.
    /// Returns Err if required elements are not materialized.
    pub fn try_new(
        tuple_descriptor: TupleDescriptor,
        predicate: Predicate,
    ) -> Result<Self, String> {
        let required_cols = predicate.required_columns();
        let required_elements = tuple_descriptor.elements_for_columns(&required_cols);

        // Validate materialization
        tuple_descriptor.assert_materialized(&required_elements)?;

        let combined_descriptor = tuple_descriptor.combined_descriptor();
        let output_tuple_descriptor = tuple_descriptor.clone();
        Ok(Self {
            tuple_descriptor,
            output_tuple_descriptor,
            combined_descriptor,
            predicate,
            required_elements,
            current_tuples: AHashSet::new(),
            dirty: true,
        })
    }

    /// Create a FilterNode with a full TupleDescriptor for multi-element tuples.
    /// Does NOT validate materialization - use try_new for validation.
    pub fn with_tuple_descriptor(tuple_descriptor: TupleDescriptor, predicate: Predicate) -> Self {
        let required_cols = predicate.required_columns();
        let required_elements = tuple_descriptor.elements_for_columns(&required_cols);
        let combined_descriptor = tuple_descriptor.combined_descriptor();
        let output_tuple_descriptor = tuple_descriptor.clone();
        Self {
            tuple_descriptor,
            output_tuple_descriptor,
            combined_descriptor,
            predicate,
            required_elements,
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    /// Get the predicate.
    pub fn predicate(&self) -> &Predicate {
        &self.predicate
    }

    /// Get the required element indices for predicate evaluation.
    pub fn required_elements(&self) -> &HashSet<usize> {
        &self.required_elements
    }

    /// Evaluate predicate against a tuple.
    /// Supports multi-element tuples by resolving column indices to correct elements.
    fn evaluate_tuple(&self, tuple: &Tuple) -> bool {
        self.evaluate_predicate_on_tuple(&self.predicate, tuple)
    }

    /// Recursively evaluate a predicate on a tuple.
    fn evaluate_predicate_on_tuple(&self, predicate: &Predicate, tuple: &Tuple) -> bool {
        match predicate {
            Predicate::Eq { col_index, value } => match self.get_column_bytes(tuple, *col_index) {
                Some(bytes) => bytes == value.as_slice(),
                None => false,
            },
            Predicate::Ne { col_index, value } => {
                match self.get_column_bytes(tuple, *col_index) {
                    Some(bytes) => bytes != value.as_slice(),
                    None => true, // null != value
                }
            }
            Predicate::Lt { col_index, value } => {
                matches!(
                    self.compare_column_to_value(tuple, *col_index, value),
                    Some(Ordering::Less)
                )
            }
            Predicate::Le { col_index, value } => {
                matches!(
                    self.compare_column_to_value(tuple, *col_index, value),
                    Some(Ordering::Less) | Some(Ordering::Equal)
                )
            }
            Predicate::Gt { col_index, value } => {
                matches!(
                    self.compare_column_to_value(tuple, *col_index, value),
                    Some(Ordering::Greater)
                )
            }
            Predicate::Ge { col_index, value } => {
                matches!(
                    self.compare_column_to_value(tuple, *col_index, value),
                    Some(Ordering::Greater) | Some(Ordering::Equal)
                )
            }
            Predicate::IsNull { col_index } => {
                self.is_column_null(tuple, *col_index).unwrap_or(false)
            }
            Predicate::IsNotNull { col_index } => {
                !self.is_column_null(tuple, *col_index).unwrap_or(true)
            }
            Predicate::And(preds) => preds
                .iter()
                .all(|p| self.evaluate_predicate_on_tuple(p, tuple)),
            Predicate::Or(preds) => preds
                .iter()
                .any(|p| self.evaluate_predicate_on_tuple(p, tuple)),
            Predicate::Not(pred) => !self.evaluate_predicate_on_tuple(pred, tuple),
            Predicate::True => true,
        }
    }

    /// Get column bytes from the correct tuple element using global column index.
    fn get_column_bytes(&self, tuple: &Tuple, global_col_index: usize) -> Option<Vec<u8>> {
        let (elem_idx, local_col_idx) = self.tuple_descriptor.resolve_column(global_col_index)?;
        let elem = tuple.get(elem_idx)?;
        let content = elem.content()?;
        let descriptor = &self.tuple_descriptor.element(elem_idx)?.descriptor;
        column_bytes(descriptor, content, local_col_idx)
            .ok()
            .flatten()
            .map(|b| b.to_vec())
    }

    /// Compare a column to a value using global column index.
    fn compare_column_to_value(
        &self,
        tuple: &Tuple,
        global_col_index: usize,
        value: &[u8],
    ) -> Option<Ordering> {
        let (elem_idx, local_col_idx) = self.tuple_descriptor.resolve_column(global_col_index)?;
        let elem = tuple.get(elem_idx)?;
        let content = elem.content()?;
        let descriptor = &self.tuple_descriptor.element(elem_idx)?.descriptor;
        compare_column_to_value(descriptor, content, local_col_idx, value).ok()
    }

    /// Check if a column is null using global column index.
    fn is_column_null(&self, tuple: &Tuple, global_col_index: usize) -> Option<bool> {
        let (elem_idx, local_col_idx) = self.tuple_descriptor.resolve_column(global_col_index)?;
        let elem = tuple.get(elem_idx)?;
        let content = elem.content()?;
        let descriptor = &self.tuple_descriptor.element(elem_idx)?.descriptor;
        column_is_null(descriptor, content, local_col_idx).ok()
    }
}

impl RowNode for FilterNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.combined_descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        let input_size = input.added.len() + input.removed.len() + input.updated.len();
        let mut result = TupleDelta::new();

        // Filter removed tuples
        for tuple in input.removed {
            if self.current_tuples.remove(&tuple) {
                result.removed.push(tuple);
            }
        }

        // Filter added tuples
        for tuple in input.added {
            if self.evaluate_tuple(&tuple) {
                self.current_tuples.insert(tuple.clone());
                result.added.push(tuple);
            }
        }

        // Handle updated tuples
        for (old_tuple, new_tuple) in input.updated {
            let old_passes = self.evaluate_tuple(&old_tuple);
            let new_passes = self.evaluate_tuple(&new_tuple);

            match (old_passes, new_passes) {
                (true, true) => {
                    // Update in place
                    self.current_tuples.remove(&old_tuple);
                    self.current_tuples.insert(new_tuple.clone());
                    result.updated.push((old_tuple, new_tuple));
                }
                (true, false) => {
                    // Was passing, now failing - treat as removal
                    self.current_tuples.remove(&old_tuple);
                    result.removed.push(old_tuple);
                }
                (false, true) => {
                    // Was failing, now passing - treat as addition
                    self.current_tuples.insert(new_tuple.clone());
                    result.added.push(new_tuple);
                }
                (false, false) => {
                    // Neither passes, ignore
                }
            }
        }

        let output_size = result.added.len() + result.removed.len() + result.updated.len();
        tracing::trace!(input_size, output_size, "filter node processed");

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
    use crate::object::ObjectId;
    use crate::query_manager::encoding::encode_row;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, TupleElement, Value};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
    }

    fn make_tuple(id: ObjectId, values: &[Value]) -> Tuple {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, values).unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }])
    }

    fn contains_id(tuples: &[Tuple], id: ObjectId) -> bool {
        tuples.iter().any(|t| t.ids().contains(&id))
    }

    fn make_filter_node(predicate: Predicate) -> FilterNode {
        let descriptor = test_descriptor();
        let tuple_desc = TupleDescriptor::single_with_materialization("", descriptor, true);
        FilterNode::with_tuple_descriptor(tuple_desc, predicate)
    }

    #[test]
    fn filter_eq() {
        let predicate = Predicate::Eq {
            col_index: 2,
            value: 100i32.to_le_bytes().to_vec(),
        };
        let mut node = make_filter_node(predicate);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(100),
            ],
        );
        let tuple2 = make_tuple(
            id2,
            &[
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(50),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1, tuple2],
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 1);
        assert!(contains_id(&result.added, id1));
    }

    #[test]
    fn filter_and() {
        let predicate = Predicate::And(vec![
            Predicate::Ge {
                col_index: 2,
                value: 50i32.to_le_bytes().to_vec(),
            },
            Predicate::Le {
                col_index: 2,
                value: 100i32.to_le_bytes().to_vec(),
            },
        ]);
        let mut node = make_filter_node(predicate);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let id3 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(30),
            ],
        );
        let tuple2 = make_tuple(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(75),
            ],
        );
        let tuple3 = make_tuple(
            id3,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(150),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1, tuple2, tuple3],
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 1);
        assert!(contains_id(&result.added, id2));
    }

    #[test]
    fn filter_or() {
        let predicate = Predicate::Or(vec![
            Predicate::Eq {
                col_index: 2,
                value: 30i32.to_le_bytes().to_vec(),
            },
            Predicate::Eq {
                col_index: 2,
                value: 150i32.to_le_bytes().to_vec(),
            },
        ]);
        let mut node = make_filter_node(predicate);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let id3 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(30),
            ],
        );
        let tuple2 = make_tuple(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(75),
            ],
        );
        let tuple3 = make_tuple(
            id3,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(150),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1, tuple2, tuple3],
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 2);
        assert!(contains_id(&result.added, id1));
        assert!(contains_id(&result.added, id3));
    }

    #[test]
    fn filter_update_passes_to_fails() {
        let predicate = Predicate::Ge {
            col_index: 2,
            value: 50i32.to_le_bytes().to_vec(),
        };
        let mut node = make_filter_node(predicate);

        let id1 = ObjectId::new();
        let old_tuple = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        let new_tuple = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(30),
            ],
        );

        // First add the tuple
        let add_delta = TupleDelta {
            added: vec![old_tuple.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(add_delta);

        // Then update it to fail the filter
        let update_delta = TupleDelta {
            added: vec![],
            removed: vec![],
            updated: vec![(old_tuple, new_tuple)],
        };
        let result = node.process(update_delta);

        // Should be treated as a removal
        assert_eq!(result.removed.len(), 1);
        assert!(contains_id(&result.removed, id1));
        assert!(result.added.is_empty());
        assert!(result.updated.is_empty());
    }

    #[test]
    fn filter_update_fails_to_passes() {
        let predicate = Predicate::Ge {
            col_index: 2,
            value: 50i32.to_le_bytes().to_vec(),
        };
        let mut node = make_filter_node(predicate);

        let id1 = ObjectId::new();
        let old_tuple = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(30),
            ],
        );
        let new_tuple = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );

        // Tuple doesn't pass filter initially, so not added
        let add_delta = TupleDelta {
            added: vec![old_tuple.clone()],
            removed: vec![],
            updated: vec![],
        };
        let result1 = node.process(add_delta);
        assert!(result1.added.is_empty());

        // Update makes it pass
        let update_delta = TupleDelta {
            added: vec![],
            removed: vec![],
            updated: vec![(old_tuple, new_tuple)],
        };
        let result = node.process(update_delta);

        // Should be treated as an addition
        assert_eq!(result.added.len(), 1);
        assert!(contains_id(&result.added, id1));
        assert!(result.removed.is_empty());
        assert!(result.updated.is_empty());
    }

    // ========================================================================
    // Predicate::required_columns() tests
    // ========================================================================

    #[test]
    fn required_columns_eq() {
        let pred = Predicate::Eq {
            col_index: 3,
            value: vec![],
        };
        assert_eq!(pred.required_columns(), [3].into_iter().collect());
    }

    #[test]
    fn required_columns_and() {
        let pred = Predicate::And(vec![
            Predicate::Eq {
                col_index: 1,
                value: vec![],
            },
            Predicate::Gt {
                col_index: 5,
                value: vec![],
            },
        ]);
        assert_eq!(pred.required_columns(), [1, 5].into_iter().collect());
    }

    #[test]
    fn required_columns_or() {
        let pred = Predicate::Or(vec![
            Predicate::Lt {
                col_index: 0,
                value: vec![],
            },
            Predicate::Ge {
                col_index: 2,
                value: vec![],
            },
        ]);
        assert_eq!(pred.required_columns(), [0, 2].into_iter().collect());
    }

    #[test]
    fn required_columns_not() {
        let pred = Predicate::Not(Box::new(Predicate::IsNull { col_index: 7 }));
        assert_eq!(pred.required_columns(), [7].into_iter().collect());
    }

    #[test]
    fn required_columns_true() {
        let pred = Predicate::True;
        assert!(pred.required_columns().is_empty());
    }

    // ========================================================================
    // Multi-element tuple filtering tests (for joins)
    // ========================================================================

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

    fn make_user_element(id: ObjectId, user_id: i32, name: &str) -> TupleElement {
        let descriptor = users_descriptor();
        let data = encode_row(
            &descriptor,
            &[Value::Integer(user_id), Value::Text(name.into())],
        )
        .unwrap();
        TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }
    }

    fn make_post_element(id: ObjectId, post_id: i32, title: &str, author_id: i32) -> TupleElement {
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
        TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }
    }

    #[test]
    fn filter_on_joined_table_column() {
        // Create a TupleDescriptor for users JOIN posts
        // Combined columns: [users.id(0), users.name(1), posts.id(2), posts.title(3), posts.author_id(4)]
        let tuple_descriptor = TupleDescriptor::from_tables(&[
            ("users".to_string(), users_descriptor()),
            ("posts".to_string(), posts_descriptor()),
        ]);

        // Filter on posts.title (global column index 3)
        // Text values need to be encoded properly
        let title_bytes = {
            let desc = RowDescriptor::new(vec![ColumnDescriptor::new("title", ColumnType::Text)]);
            let data = encode_row(&desc, &[Value::Text("Learning Rust".into())]).unwrap();
            // Extract the title bytes from the encoded row
            column_bytes(&desc, &data, 0).unwrap().unwrap().to_vec()
        };

        let predicate = Predicate::Eq {
            col_index: 3, // posts.title
            value: title_bytes,
        };

        let mut node = FilterNode::with_tuple_descriptor(tuple_descriptor, predicate);

        // Verify required_elements - should only need element 1 (posts)
        assert_eq!(
            node.required_elements(),
            &[1].into_iter().collect::<HashSet<usize>>()
        );

        // Create joined tuples (two-element tuples)
        let user1_oid = ObjectId::new();
        let user2_oid = ObjectId::new();
        let post1_oid = ObjectId::new();
        let post2_oid = ObjectId::new();

        // Tuple 1: User 1 + Post 1 (title = "Hello World") - should NOT match
        let tuple1 = Tuple::new(vec![
            make_user_element(user1_oid, 1, "Alice"),
            make_post_element(post1_oid, 100, "Hello World", 1),
        ]);

        // Tuple 2: User 2 + Post 2 (title = "Learning Rust") - SHOULD match
        let tuple2 = Tuple::new(vec![
            make_user_element(user2_oid, 2, "Bob"),
            make_post_element(post2_oid, 101, "Learning Rust", 2),
        ]);

        let delta = TupleDelta {
            added: vec![tuple1, tuple2],
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        // Only tuple2 should pass the filter
        assert_eq!(result.added.len(), 1);
        assert!(contains_id(&result.added, user2_oid));
        assert!(contains_id(&result.added, post2_oid));
    }

    #[test]
    fn filter_on_left_table_column_in_join() {
        // Filter on users.name (global column index 1)
        let tuple_descriptor = TupleDescriptor::from_tables(&[
            ("users".to_string(), users_descriptor()),
            ("posts".to_string(), posts_descriptor()),
        ]);

        let name_bytes = {
            let desc = RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
            let data = encode_row(&desc, &[Value::Text("Alice".into())]).unwrap();
            column_bytes(&desc, &data, 0).unwrap().unwrap().to_vec()
        };

        let predicate = Predicate::Eq {
            col_index: 1, // users.name
            value: name_bytes,
        };

        let mut node = FilterNode::with_tuple_descriptor(tuple_descriptor, predicate);

        // Required elements should only include element 0 (users)
        assert_eq!(
            node.required_elements(),
            &[0].into_iter().collect::<HashSet<usize>>()
        );

        let user1_oid = ObjectId::new();
        let user2_oid = ObjectId::new();
        let post1_oid = ObjectId::new();
        let post2_oid = ObjectId::new();

        // Tuple with Alice - should match
        let tuple1 = Tuple::new(vec![
            make_user_element(user1_oid, 1, "Alice"),
            make_post_element(post1_oid, 100, "Post 1", 1),
        ]);

        // Tuple with Bob - should NOT match
        let tuple2 = Tuple::new(vec![
            make_user_element(user2_oid, 2, "Bob"),
            make_post_element(post2_oid, 101, "Post 2", 2),
        ]);

        let delta = TupleDelta {
            added: vec![tuple1, tuple2],
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 1);
        assert!(contains_id(&result.added, user1_oid));
    }
}
