use std::collections::{HashMap, HashSet};

use crate::commit::CommitId;
use crate::object::ObjectId;

/// Name identifying a table in the schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableName(pub String);

impl TableName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl<T: Into<String>> From<T> for TableName {
    fn from(s: T) -> Self {
        Self(s.into())
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Column data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// 4-byte signed integer (i32), like PostgreSQL INTEGER.
    Integer,
    /// 8-byte signed integer (i64), like PostgreSQL BIGINT.
    BigInt,
    /// 1-byte boolean.
    Boolean,
    /// Variable-length UTF-8 text.
    Text,
    /// 8-byte unsigned timestamp (microseconds since Unix epoch).
    Timestamp,
    /// 16-byte UUID (ObjectId).
    Uuid,
}

impl ColumnType {
    /// Returns the fixed byte size for this type, or None for variable-length types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            ColumnType::Integer => Some(4),
            ColumnType::BigInt => Some(8),
            ColumnType::Boolean => Some(1),
            ColumnType::Timestamp => Some(8),
            ColumnType::Uuid => Some(16),
            ColumnType::Text => None,
        }
    }

    /// Returns true if this type is variable-length.
    pub fn is_variable(&self) -> bool {
        self.fixed_size().is_none()
    }
}

/// Descriptor for a single column in a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDescriptor {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    /// Optional foreign key reference to another table.
    pub references: Option<TableName>,
}

impl ColumnDescriptor {
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
            references: None,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn references(mut self, table: impl Into<TableName>) -> Self {
        self.references = Some(table.into());
        self
    }
}

/// Descriptor for a row's schema, defining column order and types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDescriptor {
    pub columns: Vec<ColumnDescriptor>,
}

impl RowDescriptor {
    pub fn new(columns: Vec<ColumnDescriptor>) -> Self {
        Self { columns }
    }

    /// Find column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get column descriptor by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDescriptor> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Count of fixed-size columns.
    pub fn fixed_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| !c.column_type.is_variable())
            .count()
    }

    /// Count of variable-length columns.
    pub fn variable_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.column_type.is_variable())
            .count()
    }
}

/// Schema mapping table names to their row descriptors.
pub type Schema = HashMap<TableName, RowDescriptor>;

/// Value type for API boundary (insert input, query output).
/// Internally, rows are stored as binary.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
    Null,
}

impl Value {
    /// Returns the column type this value represents, or None for Null.
    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Value::Integer(_) => Some(ColumnType::Integer),
            Value::BigInt(_) => Some(ColumnType::BigInt),
            Value::Boolean(_) => Some(ColumnType::Boolean),
            Value::Text(_) => Some(ColumnType::Text),
            Value::Timestamp(_) => Some(ColumnType::Timestamp),
            Value::Uuid(_) => Some(ColumnType::Uuid),
            Value::Null => None,
        }
    }

    /// Returns true if this is a Null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// A row with its object ID, binary data, and commit reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: ObjectId,
    /// Binary encoded row data.
    pub data: Vec<u8>,
    pub commit_id: CommitId,
}

impl Row {
    pub fn new(id: ObjectId, data: Vec<u8>, commit_id: CommitId) -> Self {
        Self {
            id,
            data,
            commit_id,
        }
    }
}

/// Delta for ID-level changes (before materialization).
/// Lightweight - only tracks ObjectIds, not full row data.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdDelta {
    pub added: HashSet<ObjectId>,
    pub removed: HashSet<ObjectId>,
}

impl IdDelta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }

    /// Merge another IdDelta into this one.
    pub fn merge(&mut self, other: IdDelta) {
        // If something was added and then removed, it cancels out
        // If something was removed and then added, it remains added
        for id in other.added {
            if !self.removed.remove(&id) {
                self.added.insert(id);
            }
        }
        for id in other.removed {
            if !self.added.remove(&id) {
                self.removed.insert(id);
            }
        }
    }

    /// Union of two IdDeltas (for OR operations at ID level).
    pub fn union(&self, other: &IdDelta) -> IdDelta {
        IdDelta {
            added: self.added.union(&other.added).copied().collect(),
            removed: self.removed.intersection(&other.removed).copied().collect(),
        }
    }
}

/// Delta for row-level changes (after materialization).
/// Contains full row data for processing by filter/sort/output nodes.
#[derive(Debug, Clone, Default)]
pub struct RowDelta {
    pub added: Vec<Row>,
    pub removed: Vec<Row>,
    /// Updated rows as (old, new) pairs.
    pub updated: Vec<(Row, Row)>,
}

impl RowDelta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.updated.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn column_type_fixed_sizes() {
        assert_eq!(ColumnType::Integer.fixed_size(), Some(4));
        assert_eq!(ColumnType::BigInt.fixed_size(), Some(8));
        assert_eq!(ColumnType::Boolean.fixed_size(), Some(1));
        assert_eq!(ColumnType::Timestamp.fixed_size(), Some(8));
        assert_eq!(ColumnType::Uuid.fixed_size(), Some(16));
        assert_eq!(ColumnType::Text.fixed_size(), None);
    }

    #[test]
    fn column_descriptor_builder() {
        let col = ColumnDescriptor::new("email", ColumnType::Text)
            .nullable()
            .references("users");

        assert_eq!(col.name, "email");
        assert_eq!(col.column_type, ColumnType::Text);
        assert!(col.nullable);
        assert_eq!(col.references, Some(TableName::new("users")));
    }

    #[test]
    fn row_descriptor_column_lookup() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("age", ColumnType::Integer),
        ]);

        assert_eq!(descriptor.column_index("id"), Some(0));
        assert_eq!(descriptor.column_index("name"), Some(1));
        assert_eq!(descriptor.column_index("age"), Some(2));
        assert_eq!(descriptor.column_index("unknown"), None);

        assert_eq!(descriptor.fixed_column_count(), 2); // id (uuid) + age (integer)
        assert_eq!(descriptor.variable_column_count(), 1); // name (text)
    }

    #[test]
    fn value_column_type() {
        assert_eq!(Value::Integer(42).column_type(), Some(ColumnType::Integer));
        assert_eq!(Value::BigInt(42).column_type(), Some(ColumnType::BigInt));
        assert_eq!(
            Value::Boolean(true).column_type(),
            Some(ColumnType::Boolean)
        );
        assert_eq!(
            Value::Text("hello".into()).column_type(),
            Some(ColumnType::Text)
        );
        assert_eq!(
            Value::Timestamp(123).column_type(),
            Some(ColumnType::Timestamp)
        );
        assert_eq!(
            Value::Uuid(ObjectId(Uuid::nil())).column_type(),
            Some(ColumnType::Uuid)
        );
        assert_eq!(Value::Null.column_type(), None);
    }

    #[test]
    fn id_delta_merge() {
        let mut delta1 = IdDelta {
            added: [ObjectId(Uuid::from_u128(1)), ObjectId(Uuid::from_u128(2))]
                .into_iter()
                .collect(),
            removed: HashSet::new(),
        };

        let delta2 = IdDelta {
            added: [ObjectId(Uuid::from_u128(3))].into_iter().collect(),
            removed: [ObjectId(Uuid::from_u128(1))].into_iter().collect(), // cancels add
        };

        delta1.merge(delta2);

        assert!(delta1.added.contains(&ObjectId(Uuid::from_u128(2))));
        assert!(delta1.added.contains(&ObjectId(Uuid::from_u128(3))));
        assert!(!delta1.added.contains(&ObjectId(Uuid::from_u128(1)))); // cancelled
        assert!(delta1.removed.is_empty());
    }

    #[test]
    fn id_delta_union() {
        let delta1 = IdDelta {
            added: [ObjectId(Uuid::from_u128(1)), ObjectId(Uuid::from_u128(2))]
                .into_iter()
                .collect(),
            removed: [ObjectId(Uuid::from_u128(10))].into_iter().collect(),
        };

        let delta2 = IdDelta {
            added: [ObjectId(Uuid::from_u128(2)), ObjectId(Uuid::from_u128(3))]
                .into_iter()
                .collect(),
            removed: [ObjectId(Uuid::from_u128(10)), ObjectId(Uuid::from_u128(11))]
                .into_iter()
                .collect(),
        };

        let union = delta1.union(&delta2);

        // Added is union: 1, 2, 3
        assert_eq!(union.added.len(), 3);
        assert!(union.added.contains(&ObjectId(Uuid::from_u128(1))));
        assert!(union.added.contains(&ObjectId(Uuid::from_u128(2))));
        assert!(union.added.contains(&ObjectId(Uuid::from_u128(3))));

        // Removed is intersection: only 10
        assert_eq!(union.removed.len(), 1);
        assert!(union.removed.contains(&ObjectId(Uuid::from_u128(10))));
    }
}
