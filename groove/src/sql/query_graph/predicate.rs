//! Predicate types for filtering rows.

use crate::object::ObjectId;
use crate::sql::row_buffer::{RowDescriptor, RowRef, RowValue};

/// A minimal value type for predicate and policy literals.
///
/// Unlike the full `Value` enum, this only contains primitive types suitable
/// for comparison. No `Row`, `Array`, or nullable wrappers - null is explicit.
#[derive(Clone, Debug, PartialEq)]
pub enum PredicateValue {
    Bool(bool),
    I32(i32),
    U32(u32),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Ref(ObjectId),
    Null,
}

impl PredicateValue {
    /// Create from a legacy Value type.
    ///
    /// Converts Value to PredicateValue, stripping nullable wrappers.
    pub fn from_value(value: &crate::sql::row::Value) -> Self {
        use crate::sql::row::Value;
        match value {
            Value::Bool(v) => PredicateValue::Bool(*v),
            Value::I32(v) => PredicateValue::I32(*v),
            Value::U32(v) => PredicateValue::U32(*v),
            Value::I64(v) => PredicateValue::I64(*v),
            Value::F64(v) => PredicateValue::F64(*v),
            Value::String(v) => PredicateValue::String(v.clone()),
            Value::Bytes(v) => PredicateValue::Bytes(v.clone()),
            Value::Ref(v) => PredicateValue::Ref(*v),
            Value::NullableNone => PredicateValue::Null,
            Value::NullableSome(inner) => PredicateValue::from_value(inner),
            // Complex types not supported in predicates - treat as null
            Value::Array(_) | Value::Row(_) | Value::Blob(_) | Value::BlobArray(_) => {
                PredicateValue::Null
            }
        }
    }

    /// Check if this value matches a RowValue from a buffer.
    pub fn matches(&self, row_value: &RowValue<'_>) -> bool {
        match (self, row_value) {
            (PredicateValue::Null, RowValue::Null) => true,
            (PredicateValue::Null, _) => false,
            (_, RowValue::Null) => false,
            (PredicateValue::Bool(a), RowValue::Bool(b)) => *a == *b,
            (PredicateValue::I32(a), RowValue::I32(b)) => *a == *b,
            (PredicateValue::U32(a), RowValue::U32(b)) => *a == *b,
            (PredicateValue::I64(a), RowValue::I64(b)) => *a == *b,
            (PredicateValue::F64(a), RowValue::F64(b)) => *a == *b,
            (PredicateValue::Ref(a), RowValue::Ref(b)) => *a == *b,
            (PredicateValue::String(a), RowValue::String(b)) => a.as_str() == *b,
            (PredicateValue::Bytes(a), RowValue::Bytes(b)) => a.as_slice() == *b,
            // Type mismatch - not equal
            _ => false,
        }
    }

    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, PredicateValue::Null)
    }

    /// Convert to a display string for predicates.
    pub fn to_display_string(&self) -> String {
        match self {
            PredicateValue::Bool(b) => b.to_string().to_uppercase(),
            PredicateValue::I32(n) => n.to_string(),
            PredicateValue::U32(n) => n.to_string(),
            PredicateValue::I64(n) => n.to_string(),
            PredicateValue::F64(n) => n.to_string(),
            PredicateValue::String(s) => format!("'{}'", s),
            PredicateValue::Bytes(b) => format!("<{} bytes>", b.len()),
            PredicateValue::Ref(id) => format!("@{}", id),
            PredicateValue::Null => "NULL".to_string(),
        }
    }

    /// Convert to legacy Value type (for backwards compatibility with policy system).
    pub fn to_value(&self) -> crate::sql::row::Value {
        use crate::sql::row::Value;
        match self {
            PredicateValue::Bool(v) => Value::Bool(*v),
            PredicateValue::I32(v) => Value::I32(*v),
            PredicateValue::U32(v) => Value::U32(*v),
            PredicateValue::I64(v) => Value::I64(*v),
            PredicateValue::F64(v) => Value::F64(*v),
            PredicateValue::String(v) => Value::String(v.clone()),
            PredicateValue::Bytes(v) => Value::Bytes(v.clone()),
            PredicateValue::Ref(v) => Value::Ref(*v),
            PredicateValue::Null => Value::NullableNone,
        }
    }
}

/// A predicate for filtering rows.
#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    /// Always true.
    True,
    /// Always false.
    False,
    /// Column equals value.
    Eq { column: String, value: PredicateValue },
    /// Column not equals value.
    Ne { column: String, value: PredicateValue },
    /// Logical AND of predicates.
    And(Vec<Predicate>),
    /// Logical OR of predicates.
    Or(Vec<Predicate>),
    /// Logical NOT.
    Not(Box<Predicate>),
}

impl Predicate {
    /// Create an equality predicate.
    pub fn eq(column: impl Into<String>, value: PredicateValue) -> Self {
        Predicate::Eq {
            column: column.into(),
            value,
        }
    }

    /// Create a not-equals predicate.
    pub fn ne(column: impl Into<String>, value: PredicateValue) -> Self {
        Predicate::Ne {
            column: column.into(),
            value,
        }
    }

    /// Evaluate the predicate against a buffer row.
    ///
    /// This is the buffer-based equivalent of `matches`, working with `RowRef`
    /// instead of the legacy `Row` type. The row_id is passed separately since
    /// `RowRef` doesn't contain the ObjectId.
    pub fn matches_buffer(&self, row_id: ObjectId, row: RowRef<'_>, descriptor: &RowDescriptor) -> bool {
        match self {
            Predicate::True => true,
            Predicate::False => false,

            Predicate::Eq { column, value } => {
                // Check for id column (unqualified "id" or qualified "Table.id")
                let is_id_column = column == "id" || column.ends_with(".id");
                if is_id_column {
                    // Special case: implicit id column
                    match value {
                        PredicateValue::Ref(id) => row_id == *id,
                        // Also allow matching against string representation
                        PredicateValue::String(s) => {
                            if let Ok(id) = s.parse::<ObjectId>() {
                                row_id == id
                            } else {
                                false
                            }
                        }
                        _ => false,
                    }
                } else if let Some(idx) = descriptor.column_index(column) {
                    if let Some(row_value) = row.get(idx) {
                        value.matches(&row_value)
                    } else {
                        false
                    }
                } else {
                    false // Unknown column
                }
            }

            Predicate::Ne { column, value } => {
                // Check for id column (unqualified "id" or qualified "Table.id")
                let is_id_column = column == "id" || column.ends_with(".id");
                if is_id_column {
                    match value {
                        PredicateValue::Ref(id) => row_id != *id,
                        PredicateValue::String(s) => {
                            if let Ok(id) = s.parse::<ObjectId>() {
                                row_id != id
                            } else {
                                true // Can't parse, so definitely not equal
                            }
                        }
                        _ => true,
                    }
                } else if let Some(idx) = descriptor.column_index(column) {
                    if let Some(row_value) = row.get(idx) {
                        !value.matches(&row_value)
                    } else {
                        false // Unknown column - can't evaluate
                    }
                } else {
                    false // Unknown column - can't evaluate
                }
            }

            Predicate::And(preds) => preds.iter().all(|p| p.matches_buffer(row_id, row, descriptor)),
            Predicate::Or(preds) => preds.iter().any(|p| p.matches_buffer(row_id, row, descriptor)),
            Predicate::Not(pred) => !pred.matches_buffer(row_id, row, descriptor),
        }
    }

    /// Combine this predicate with another using AND.
    pub fn and(self, other: Predicate) -> Predicate {
        match (self, other) {
            // Identity: True AND x = x
            (Predicate::True, p) | (p, Predicate::True) => p,
            // Annihilation: False AND x = False
            (Predicate::False, _) | (_, Predicate::False) => Predicate::False,
            // Flatten nested ANDs
            (Predicate::And(mut a), Predicate::And(b)) => {
                a.extend(b);
                Predicate::And(a)
            }
            (Predicate::And(mut a), p) => {
                a.push(p);
                Predicate::And(a)
            }
            (p, Predicate::And(mut a)) => {
                a.insert(0, p);
                Predicate::And(a)
            }
            // General case
            (a, b) => Predicate::And(vec![a, b]),
        }
    }

    /// Combine this predicate with another using OR.
    pub fn or(self, other: Predicate) -> Predicate {
        match (self, other) {
            // Identity: False OR x = x
            (Predicate::False, p) | (p, Predicate::False) => p,
            // Annihilation: True OR x = True
            (Predicate::True, _) | (_, Predicate::True) => Predicate::True,
            // Flatten nested ORs
            (Predicate::Or(mut a), Predicate::Or(b)) => {
                a.extend(b);
                Predicate::Or(a)
            }
            (Predicate::Or(mut a), p) => {
                a.push(p);
                Predicate::Or(a)
            }
            (p, Predicate::Or(mut a)) => {
                a.insert(0, p);
                Predicate::Or(a)
            }
            // General case
            (a, b) => Predicate::Or(vec![a, b]),
        }
    }

    /// Negate this predicate.
    pub fn not(self) -> Predicate {
        match self {
            Predicate::True => Predicate::False,
            Predicate::False => Predicate::True,
            Predicate::Not(inner) => *inner,
            other => Predicate::Not(Box::new(other)),
        }
    }

    /// Estimate the selectivity of this predicate (lower = more selective).
    ///
    /// Used for ordering predicates in AND expressions to evaluate
    /// the most selective ones first for better performance.
    pub fn selectivity(&self) -> u32 {
        match self {
            // Constants - trivial to evaluate
            Predicate::True => 0,
            Predicate::False => 0,

            // Equality on id - most selective (unique)
            Predicate::Eq { column, .. } if column == "id" => 1,

            // Equality on Ref columns - very selective (indexed)
            Predicate::Eq { column, .. } if column.ends_with("_id") => 2,

            // Equality on other columns - moderately selective
            Predicate::Eq { .. } => 3,

            // Inequality - slightly less selective
            Predicate::Ne { .. } => 4,

            // NOT - depends on inner, add small penalty
            Predicate::Not(inner) => inner.selectivity() + 1,

            // AND - use min selectivity (will short-circuit on first failure)
            Predicate::And(preds) => {
                preds.iter().map(|p| p.selectivity()).min().unwrap_or(10)
            }

            // OR - use max selectivity (must evaluate until first success)
            Predicate::Or(preds) => {
                preds.iter().map(|p| p.selectivity()).max().unwrap_or(10)
            }
        }
    }

    /// Optimize this predicate by reordering AND clauses by selectivity.
    ///
    /// More selective predicates are evaluated first, allowing early cutoff.
    pub fn optimize(self) -> Predicate {
        match self {
            Predicate::And(mut preds) => {
                // Recursively optimize children
                preds = preds.into_iter().map(|p| p.optimize()).collect();
                // Sort by selectivity (most selective first)
                preds.sort_by_key(|p| p.selectivity());
                Predicate::And(preds)
            }
            Predicate::Or(preds) => {
                // Recursively optimize children (but don't reorder OR)
                Predicate::Or(preds.into_iter().map(|p| p.optimize()).collect())
            }
            Predicate::Not(inner) => Predicate::Not(Box::new(inner.optimize())),
            other => other,
        }
    }

    /// Convert the predicate to a human-readable display string.
    ///
    /// Used for diagram rendering and debugging output.
    pub fn to_display_string(&self) -> String {
        match self {
            Predicate::True => "TRUE".to_string(),
            Predicate::False => "FALSE".to_string(),
            Predicate::Eq { column, value } => {
                format!("{} = {}", column, value.to_display_string())
            }
            Predicate::Ne { column, value } => {
                format!("{} != {}", column, value.to_display_string())
            }
            Predicate::And(preds) => {
                if preds.is_empty() {
                    "TRUE".to_string()
                } else {
                    preds.iter()
                        .map(|p| p.to_display_string())
                        .collect::<Vec<_>>()
                        .join(" AND ")
                }
            }
            Predicate::Or(preds) => {
                if preds.is_empty() {
                    "FALSE".to_string()
                } else {
                    format!("({})", preds.iter()
                        .map(|p| p.to_display_string())
                        .collect::<Vec<_>>()
                        .join(" OR "))
                }
            }
            Predicate::Not(inner) => {
                format!("NOT ({})", inner.to_display_string())
            }
        }
    }

    /// Qualify column names with a table prefix.
    ///
    /// For example, `eq("owner_id", ...)` becomes `eq("workspaces.owner_id", ...)`.
    /// This is used when predicates need to match against joined row schemas.
    pub fn qualify(&self, table: &str) -> Predicate {
        match self {
            Predicate::True => Predicate::True,
            Predicate::False => Predicate::False,
            Predicate::Eq { column, value } => {
                if column.contains('.') {
                    // Already qualified
                    Predicate::Eq {
                        column: column.clone(),
                        value: value.clone(),
                    }
                } else {
                    Predicate::Eq {
                        column: format!("{}.{}", table, column),
                        value: value.clone(),
                    }
                }
            }
            Predicate::Ne { column, value } => {
                if column.contains('.') {
                    Predicate::Ne {
                        column: column.clone(),
                        value: value.clone(),
                    }
                } else {
                    Predicate::Ne {
                        column: format!("{}.{}", table, column),
                        value: value.clone(),
                    }
                }
            }
            Predicate::And(preds) => {
                Predicate::And(preds.iter().map(|p| p.qualify(table)).collect())
            }
            Predicate::Or(preds) => {
                Predicate::Or(preds.iter().map(|p| p.qualify(table)).collect())
            }
            Predicate::Not(inner) => Predicate::Not(Box::new(inner.qualify(table))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sql::row_buffer::{RowBuilder, RowDescriptor};
    use crate::sql::schema::ColumnType;
    use std::sync::Arc;

    fn make_buffer_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::new([
            ("name".to_string(), ColumnType::String, false),
            ("active".to_string(), ColumnType::Bool, false),
            ("age".to_string(), ColumnType::I64, true),
        ]))
    }

    fn make_buffer_row(
        descriptor: &Arc<RowDescriptor>,
        name: &str,
        active: bool,
        age: Option<i64>,
    ) -> crate::sql::row_buffer::OwnedRow {
        let name_idx = descriptor.column_index("name").unwrap();
        let active_idx = descriptor.column_index("active").unwrap();
        let age_idx = descriptor.column_index("age").unwrap();

        let mut builder = RowBuilder::new(descriptor.clone())
            .set_string(name_idx, name)
            .set_bool(active_idx, active);

        if let Some(a) = age {
            builder = builder.set_i64(age_idx, a);
        } else {
            builder = builder.set_null(age_idx);
        }

        builder.build()
    }

    #[test]
    fn buffer_predicate_true_false() {
        let descriptor = make_buffer_descriptor();
        let row = make_buffer_row(&descriptor, "Alice", true, Some(30));
        let row_id = ObjectId::new(1);

        assert!(Predicate::True.matches_buffer(row_id, row.as_ref(), &descriptor));
        assert!(!Predicate::False.matches_buffer(row_id, row.as_ref(), &descriptor));
    }

    #[test]
    fn buffer_predicate_eq() {
        let descriptor = make_buffer_descriptor();
        let row = make_buffer_row(&descriptor, "Alice", true, Some(30));
        let row_id = ObjectId::new(1);

        // Match by string column
        assert!(Predicate::eq("name", PredicateValue::String("Alice".to_string()))
            .matches_buffer(row_id, row.as_ref(), &descriptor));
        assert!(!Predicate::eq("name", PredicateValue::String("Bob".to_string()))
            .matches_buffer(row_id, row.as_ref(), &descriptor));

        // Match by bool column
        assert!(Predicate::eq("active", PredicateValue::Bool(true))
            .matches_buffer(row_id, row.as_ref(), &descriptor));
        assert!(!Predicate::eq("active", PredicateValue::Bool(false))
            .matches_buffer(row_id, row.as_ref(), &descriptor));

        // Match by id
        assert!(Predicate::eq("id", PredicateValue::Ref(ObjectId::new(1)))
            .matches_buffer(row_id, row.as_ref(), &descriptor));
        assert!(!Predicate::eq("id", PredicateValue::Ref(ObjectId::new(2)))
            .matches_buffer(row_id, row.as_ref(), &descriptor));
    }

    #[test]
    fn buffer_predicate_ne() {
        let descriptor = make_buffer_descriptor();
        let row = make_buffer_row(&descriptor, "Alice", true, Some(30));
        let row_id = ObjectId::new(1);

        assert!(!Predicate::ne("name", PredicateValue::String("Alice".to_string()))
            .matches_buffer(row_id, row.as_ref(), &descriptor));
        assert!(Predicate::ne("name", PredicateValue::String("Bob".to_string()))
            .matches_buffer(row_id, row.as_ref(), &descriptor));
    }

    #[test]
    fn buffer_predicate_and() {
        let descriptor = make_buffer_descriptor();
        let row = make_buffer_row(&descriptor, "Alice", true, Some(30));
        let row_id = ObjectId::new(1);

        let pred = Predicate::eq("name", PredicateValue::String("Alice".to_string()))
            .and(Predicate::eq("active", PredicateValue::Bool(true)));

        assert!(pred.matches_buffer(row_id, row.as_ref(), &descriptor));

        let pred2 = Predicate::eq("name", PredicateValue::String("Alice".to_string()))
            .and(Predicate::eq("active", PredicateValue::Bool(false)));

        assert!(!pred2.matches_buffer(row_id, row.as_ref(), &descriptor));
    }

    #[test]
    fn buffer_predicate_or() {
        let descriptor = make_buffer_descriptor();
        let row = make_buffer_row(&descriptor, "Alice", true, Some(30));
        let row_id = ObjectId::new(1);

        let pred = Predicate::eq("name", PredicateValue::String("Alice".to_string()))
            .or(Predicate::eq("name", PredicateValue::String("Bob".to_string())));

        assert!(pred.matches_buffer(row_id, row.as_ref(), &descriptor));

        let pred2 = Predicate::eq("name", PredicateValue::String("Bob".to_string()))
            .or(Predicate::eq("name", PredicateValue::String("Carol".to_string())));

        assert!(!pred2.matches_buffer(row_id, row.as_ref(), &descriptor));
    }

    #[test]
    fn buffer_predicate_not() {
        let descriptor = make_buffer_descriptor();
        let row = make_buffer_row(&descriptor, "Alice", true, Some(30));
        let row_id = ObjectId::new(1);

        let pred = Predicate::eq("active", PredicateValue::Bool(false)).not();
        assert!(pred.matches_buffer(row_id, row.as_ref(), &descriptor));

        let pred2 = Predicate::eq("active", PredicateValue::Bool(true)).not();
        assert!(!pred2.matches_buffer(row_id, row.as_ref(), &descriptor));
    }

    #[test]
    fn buffer_predicate_nullable_column() {
        let descriptor = make_buffer_descriptor();
        let row_with_age = make_buffer_row(&descriptor, "Alice", true, Some(30));
        let row_null_age = make_buffer_row(&descriptor, "Bob", true, None);
        let row_id = ObjectId::new(1);

        // Match non-null value - PredicateValue has no NullableSome, just use the value directly
        let pred = Predicate::eq("age", PredicateValue::I64(30));
        assert!(pred.matches_buffer(row_id, row_with_age.as_ref(), &descriptor));
        assert!(!pred.matches_buffer(row_id, row_null_age.as_ref(), &descriptor));

        // Match null
        let null_pred = Predicate::eq("age", PredicateValue::Null);
        assert!(!null_pred.matches_buffer(row_id, row_with_age.as_ref(), &descriptor));
        assert!(null_pred.matches_buffer(row_id, row_null_age.as_ref(), &descriptor));
    }
}
