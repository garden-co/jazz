use std::cmp::Ordering;

use crate::query_manager::encoding::{column_bytes, column_is_null, compare_column_to_value};
use crate::query_manager::types::{Row, RowDelta, RowDescriptor};

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
#[derive(Debug)]
pub struct FilterNode {
    descriptor: RowDescriptor,
    predicate: Predicate,
    /// Current rows that pass the filter.
    current_rows: Vec<Row>,
    dirty: bool,
}

impl FilterNode {
    pub fn new(descriptor: RowDescriptor, predicate: Predicate) -> Self {
        Self {
            descriptor,
            predicate,
            current_rows: Vec::new(),
            dirty: true,
        }
    }

    /// Get the predicate.
    pub fn predicate(&self) -> &Predicate {
        &self.predicate
    }
}

impl RowNode for FilterNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: RowDelta) -> RowDelta {
        let mut result = RowDelta::new();

        // Propagate pending flag from input
        result.pending = input.pending;

        // Filter removed rows
        for row in input.removed {
            if let Some(pos) = self.current_rows.iter().position(|r| r.id == row.id) {
                let removed = self.current_rows.remove(pos);
                result.removed.push(removed);
            }
        }

        // Filter added rows
        for row in input.added {
            if self.predicate.evaluate(&self.descriptor, &row) {
                self.current_rows.push(row.clone());
                result.added.push(row);
            }
        }

        // Handle updated rows
        for (old_row, new_row) in input.updated {
            let old_passes = self.predicate.evaluate(&self.descriptor, &old_row);
            let new_passes = self.predicate.evaluate(&self.descriptor, &new_row);

            match (old_passes, new_passes) {
                (true, true) => {
                    // Update in place
                    if let Some(pos) = self.current_rows.iter().position(|r| r.id == old_row.id) {
                        self.current_rows[pos] = new_row.clone();
                    }
                    result.updated.push((old_row, new_row));
                }
                (true, false) => {
                    // Was passing, now failing - treat as removal
                    if let Some(pos) = self.current_rows.iter().position(|r| r.id == old_row.id) {
                        self.current_rows.remove(pos);
                    }
                    result.removed.push(old_row);
                }
                (false, true) => {
                    // Was failing, now passing - treat as addition
                    self.current_rows.push(new_row.clone());
                    result.added.push(new_row);
                }
                (false, false) => {
                    // Neither passes, ignore
                }
            }
        }

        self.dirty = false;
        result
    }

    fn current_result(&self) -> &[Row] {
        &self.current_rows
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
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, Value};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
    }

    fn make_row(id: ObjectId, values: &[Value]) -> Row {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, values).unwrap();
        Row::new(id, data, CommitId([0; 32]))
    }

    #[test]
    fn filter_eq() {
        let descriptor = test_descriptor();
        let predicate = Predicate::Eq {
            col_index: 2,
            value: 100i32.to_le_bytes().to_vec(),
        };
        let mut node = FilterNode::new(descriptor, predicate);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let row1 = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(100),
            ],
        );
        let row2 = make_row(
            id2,
            &[
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(50),
            ],
        );

        let delta = RowDelta {
            pending: false,
            added: vec![row1.clone(), row2],
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 1);
        assert_eq!(result.added[0].id, id1);
    }

    #[test]
    fn filter_and() {
        let descriptor = test_descriptor();
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
        let mut node = FilterNode::new(descriptor, predicate);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let id3 = ObjectId::new();
        let row1 = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(30),
            ],
        );
        let row2 = make_row(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(75),
            ],
        );
        let row3 = make_row(
            id3,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(150),
            ],
        );

        let delta = RowDelta {
            pending: false,
            added: vec![row1, row2.clone(), row3],
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 1);
        assert_eq!(result.added[0].id, id2);
    }

    #[test]
    fn filter_or() {
        let descriptor = test_descriptor();
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
        let mut node = FilterNode::new(descriptor, predicate);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let id3 = ObjectId::new();
        let row1 = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(30),
            ],
        );
        let row2 = make_row(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(75),
            ],
        );
        let row3 = make_row(
            id3,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(150),
            ],
        );

        let delta = RowDelta {
            pending: false,
            added: vec![row1.clone(), row2, row3.clone()],
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 2);
        let ids: Vec<_> = result.added.iter().map(|r| r.id).collect();
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id3));
    }

    #[test]
    fn filter_update_passes_to_fails() {
        let descriptor = test_descriptor();
        let predicate = Predicate::Ge {
            col_index: 2,
            value: 50i32.to_le_bytes().to_vec(),
        };
        let mut node = FilterNode::new(descriptor, predicate);

        let id1 = ObjectId::new();
        let old_row = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        let new_row = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(30),
            ],
        );

        // First add the row
        let add_delta = RowDelta {
            pending: false,
            added: vec![old_row.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(add_delta);

        // Then update it to fail the filter
        let update_delta = RowDelta {
            pending: false,
            added: vec![],
            removed: vec![],
            updated: vec![(old_row.clone(), new_row)],
        };
        let result = node.process(update_delta);

        // Should be treated as a removal
        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.removed[0].id, id1);
        assert!(result.added.is_empty());
        assert!(result.updated.is_empty());
    }

    #[test]
    fn filter_update_fails_to_passes() {
        let descriptor = test_descriptor();
        let predicate = Predicate::Ge {
            col_index: 2,
            value: 50i32.to_le_bytes().to_vec(),
        };
        let mut node = FilterNode::new(descriptor, predicate);

        let id1 = ObjectId::new();
        let old_row = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(30),
            ],
        );
        let new_row = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );

        // Row doesn't pass filter initially, so not added
        let add_delta = RowDelta {
            pending: false,
            added: vec![old_row.clone()],
            removed: vec![],
            updated: vec![],
        };
        let result1 = node.process(add_delta);
        assert!(result1.added.is_empty());

        // Update makes it pass
        let update_delta = RowDelta {
            pending: false,
            added: vec![],
            removed: vec![],
            updated: vec![(old_row, new_row.clone())],
        };
        let result = node.process(update_delta);

        // Should be treated as an addition
        assert_eq!(result.added.len(), 1);
        assert_eq!(result.added[0].id, id1);
        assert!(result.removed.is_empty());
        assert!(result.updated.is_empty());
    }
}
