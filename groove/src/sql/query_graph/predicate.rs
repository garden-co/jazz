//! Predicate types for filtering rows.

use crate::sql::row::{Row, Value};
use crate::sql::schema::TableSchema;
use crate::sql::ObjectId;

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
                    &row.values[idx] == value
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
                    &row.values[idx] != value
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
                age.map(Value::I64).unwrap_or(Value::Null),
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
}
