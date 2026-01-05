//! Predicate types for filtering rows.

use crate::sql::row::{Row, Value};
use crate::sql::schema::TableSchema;
use crate::object::ObjectId;

/// Convert a Value to a display string for predicates.
fn value_to_display(value: &Value) -> String {
    match value {
        Value::Bool(b) => b.to_string().to_uppercase(),
        Value::I32(n) => n.to_string(),
        Value::U32(n) => n.to_string(),
        Value::I64(n) => n.to_string(),
        Value::F64(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s),
        Value::Bytes(b) => format!("<{} bytes>", b.len()),
        Value::Ref(id) => format!("@{}", id),
        Value::NullableNone => "NULL".to_string(),
        Value::NullableSome(inner) => value_to_display(inner),
        Value::Array(arr) => format!("[{} items]", arr.len()),
        Value::Row(row) => format!("<Row {}>", row.id),
    }
}

/// Compare two values, unwrapping NullableSome wrappers as needed.
/// Returns true if the inner values are equal.
fn values_equal(row_value: &Value, pred_value: &Value) -> bool {
    // Unwrap NullableSome from the row value
    let row_inner = match row_value {
        Value::NullableSome(inner) => inner.as_ref(),
        Value::NullableNone => return pred_value.is_null(),
        other => other,
    };

    // Unwrap NullableSome from the predicate value (in case it's wrapped too)
    let pred_inner = match pred_value {
        Value::NullableSome(inner) => inner.as_ref(),
        Value::NullableNone => return row_value.is_null(),
        other => other,
    };

    row_inner == pred_inner
}

/// A predicate for filtering rows.
#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    /// Always true.
    True,
    /// Always false.
    False,
    /// Column equals value.
    Eq { column: String, value: Value },
    /// Column not equals value.
    Ne { column: String, value: Value },
    /// Logical AND of predicates.
    And(Vec<Predicate>),
    /// Logical OR of predicates.
    Or(Vec<Predicate>),
    /// Logical NOT.
    Not(Box<Predicate>),
}

impl Predicate {
    /// Create an equality predicate.
    pub fn eq(column: impl Into<String>, value: Value) -> Self {
        Predicate::Eq {
            column: column.into(),
            value,
        }
    }

    /// Create a not-equals predicate.
    pub fn ne(column: impl Into<String>, value: Value) -> Self {
        Predicate::Ne {
            column: column.into(),
            value,
        }
    }

    /// Evaluate the predicate against a row.
    pub fn matches(&self, row: &Row, schema: &TableSchema) -> bool {
        match self {
            Predicate::True => true,
            Predicate::False => false,

            Predicate::Eq { column, value } => {
                if column == "id" {
                    // Special case: implicit id column
                    match value {
                        Value::Ref(id) => row.id == *id,
                        // Also allow matching against string representation
                        Value::String(s) => {
                            if let Ok(id) = s.parse::<ObjectId>() {
                                row.id == id
                            } else {
                                false
                            }
                        }
                        _ => false,
                    }
                } else if let Some(idx) = schema.column_index(column) {
                    values_equal(&row.values[idx], value)
                } else {
                    false // Unknown column
                }
            }

            Predicate::Ne { column, value } => {
                if column == "id" {
                    match value {
                        Value::Ref(id) => row.id != *id,
                        Value::String(s) => {
                            if let Ok(id) = s.parse::<ObjectId>() {
                                row.id != id
                            } else {
                                true // Can't parse, so definitely not equal
                            }
                        }
                        _ => true,
                    }
                } else if let Some(idx) = schema.column_index(column) {
                    !values_equal(&row.values[idx], value)
                } else {
                    false // Unknown column - can't evaluate
                }
            }

            Predicate::And(preds) => preds.iter().all(|p| p.matches(row, schema)),
            Predicate::Or(preds) => preds.iter().any(|p| p.matches(row, schema)),
            Predicate::Not(pred) => !pred.matches(row, schema),
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
                format!("{} = {}", column, value_to_display(value))
            }
            Predicate::Ne { column, value } => {
                format!("{} != {}", column, value_to_display(value))
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
    use crate::sql::schema::{ColumnDef, ColumnType};

    fn test_schema() -> TableSchema {
        TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("active", ColumnType::Bool),
                ColumnDef::optional("age", ColumnType::I64),
            ],
        )
    }

    fn make_row(id: u128, name: &str, active: bool, age: Option<i64>) -> Row {
        Row::new(
            ObjectId::new(id),
            vec![
                Value::String(name.to_string()),
                Value::Bool(active),
                age.map(|a| Value::NullableSome(Box::new(Value::I64(a)))).unwrap_or(Value::NullableNone),
            ],
        )
    }

    #[test]
    fn predicate_true_false() {
        let schema = test_schema();
        let row = make_row(1, "Alice", true, Some(30));

        assert!(Predicate::True.matches(&row, &schema));
        assert!(!Predicate::False.matches(&row, &schema));
    }

    #[test]
    fn predicate_eq() {
        let schema = test_schema();
        let row = make_row(1, "Alice", true, Some(30));

        // Match by string column
        assert!(Predicate::eq("name", Value::String("Alice".to_string())).matches(&row, &schema));
        assert!(!Predicate::eq("name", Value::String("Bob".to_string())).matches(&row, &schema));

        // Match by bool column
        assert!(Predicate::eq("active", Value::Bool(true)).matches(&row, &schema));
        assert!(!Predicate::eq("active", Value::Bool(false)).matches(&row, &schema));

        // Match by id
        assert!(Predicate::eq("id", Value::Ref(ObjectId::new(1))).matches(&row, &schema));
        assert!(!Predicate::eq("id", Value::Ref(ObjectId::new(2))).matches(&row, &schema));
    }

    #[test]
    fn predicate_ne() {
        let schema = test_schema();
        let row = make_row(1, "Alice", true, Some(30));

        assert!(!Predicate::ne("name", Value::String("Alice".to_string())).matches(&row, &schema));
        assert!(Predicate::ne("name", Value::String("Bob".to_string())).matches(&row, &schema));
    }

    #[test]
    fn predicate_and() {
        let schema = test_schema();
        let row = make_row(1, "Alice", true, Some(30));

        let pred = Predicate::eq("name", Value::String("Alice".to_string()))
            .and(Predicate::eq("active", Value::Bool(true)));

        assert!(pred.matches(&row, &schema));

        let pred2 = Predicate::eq("name", Value::String("Alice".to_string()))
            .and(Predicate::eq("active", Value::Bool(false)));

        assert!(!pred2.matches(&row, &schema));
    }

    #[test]
    fn predicate_or() {
        let schema = test_schema();
        let row = make_row(1, "Alice", true, Some(30));

        let pred = Predicate::eq("name", Value::String("Alice".to_string()))
            .or(Predicate::eq("name", Value::String("Bob".to_string())));

        assert!(pred.matches(&row, &schema));

        let pred2 = Predicate::eq("name", Value::String("Bob".to_string()))
            .or(Predicate::eq("name", Value::String("Carol".to_string())));

        assert!(!pred2.matches(&row, &schema));
    }

    #[test]
    fn predicate_not() {
        let schema = test_schema();
        let row = make_row(1, "Alice", true, Some(30));

        let pred = Predicate::eq("active", Value::Bool(false)).not();
        assert!(pred.matches(&row, &schema));

        let pred2 = Predicate::eq("active", Value::Bool(true)).not();
        assert!(!pred2.matches(&row, &schema));
    }

    #[test]
    fn predicate_and_simplification() {
        // True AND x = x
        let p = Predicate::True.and(Predicate::eq("name", Value::String("Alice".to_string())));
        assert!(matches!(p, Predicate::Eq { .. }));

        // False AND x = False
        let p = Predicate::False.and(Predicate::eq("name", Value::String("Alice".to_string())));
        assert!(matches!(p, Predicate::False));

        // Flatten nested ANDs
        let p1 = Predicate::eq("a", Value::I64(1)).and(Predicate::eq("b", Value::I64(2)));
        let p2 = Predicate::eq("c", Value::I64(3)).and(Predicate::eq("d", Value::I64(4)));
        let combined = p1.and(p2);

        if let Predicate::And(preds) = combined {
            assert_eq!(preds.len(), 4);
        } else {
            panic!("Expected And");
        }
    }

    #[test]
    fn predicate_or_simplification() {
        // False OR x = x
        let p = Predicate::False.or(Predicate::eq("name", Value::String("Alice".to_string())));
        assert!(matches!(p, Predicate::Eq { .. }));

        // True OR x = True
        let p = Predicate::True.or(Predicate::eq("name", Value::String("Alice".to_string())));
        assert!(matches!(p, Predicate::True));
    }

    #[test]
    fn predicate_not_simplification() {
        // NOT True = False
        assert!(matches!(Predicate::True.not(), Predicate::False));

        // NOT False = True
        assert!(matches!(Predicate::False.not(), Predicate::True));

        // NOT NOT x = x
        let p = Predicate::eq("a", Value::I64(1)).not().not();
        assert!(matches!(p, Predicate::Eq { .. }));
    }

    #[test]
    fn selectivity_ordering() {
        // id is most selective
        assert!(Predicate::eq("id", Value::I64(1)).selectivity() <
                Predicate::eq("name", Value::String("x".into())).selectivity());

        // _id columns (Refs) are more selective than regular columns
        assert!(Predicate::eq("owner_id", Value::I64(1)).selectivity() <
                Predicate::eq("name", Value::String("x".into())).selectivity());

        // Equality is more selective than inequality
        assert!(Predicate::eq("name", Value::String("x".into())).selectivity() <
                Predicate::ne("name", Value::String("x".into())).selectivity());
    }

    #[test]
    fn predicate_optimize_reorders_and() {
        // Create an AND with less selective predicate first
        let p = Predicate::And(vec![
            Predicate::eq("name", Value::String("Alice".into())), // selectivity 3
            Predicate::eq("owner_id", Value::I64(1)),              // selectivity 2
            Predicate::eq("id", Value::I64(42)),                   // selectivity 1
        ]);

        let optimized = p.optimize();

        // After optimization, should be ordered by selectivity
        if let Predicate::And(preds) = optimized {
            // id should be first
            assert!(matches!(&preds[0], Predicate::Eq { column, .. } if column == "id"));
            // owner_id should be second
            assert!(matches!(&preds[1], Predicate::Eq { column, .. } if column == "owner_id"));
            // name should be last
            assert!(matches!(&preds[2], Predicate::Eq { column, .. } if column == "name"));
        } else {
            panic!("Expected And predicate");
        }
    }

    #[test]
    fn predicate_optimize_nested() {
        // Nested AND within OR should optimize the AND
        let inner_and = Predicate::And(vec![
            Predicate::eq("name", Value::String("x".into())),
            Predicate::eq("id", Value::I64(1)),
        ]);
        let p = Predicate::Or(vec![inner_and, Predicate::True]);

        let optimized = p.optimize();

        if let Predicate::Or(preds) = optimized {
            if let Predicate::And(inner) = &preds[0] {
                // id should now be first in the inner AND
                assert!(matches!(&inner[0], Predicate::Eq { column, .. } if column == "id"));
            } else {
                panic!("Expected And predicate");
            }
        } else {
            panic!("Expected Or predicate");
        }
    }
}
