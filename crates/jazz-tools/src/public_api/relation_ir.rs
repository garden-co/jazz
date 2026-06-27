use serde::{Deserialize, Serialize};

use crate::public_api::types::{TableName, Value};

/// Fully-qualified or scoped column reference inside relation IR.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Optional scope/alias. When omitted, the active relation scope is used.
    pub scope: Option<String>,
    /// Column name within the scope.
    pub column: String,
}

impl ColumnRef {
    pub fn unscoped(column: impl Into<String>) -> Self {
        Self {
            scope: None,
            column: column.into(),
        }
    }
}

/// Value references that predicates can compare against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueRef {
    Literal(Value),
    SessionRef(Vec<String>),
    OuterColumn(ColumnRef),
}

/// Predicate comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredicateCmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Boolean predicate expression over relation rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredicateExpr {
    Cmp {
        left: ColumnRef,
        op: PredicateCmpOp,
        right: ValueRef,
    },
    IsNull {
        column: ColumnRef,
    },
    And(Vec<PredicateExpr>),
    Or(Vec<PredicateExpr>),
    True,
    False,
}

/// Relation expression used by relation-backed policy `exists(...)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelExpr {
    TableScan {
        table: TableName,
    },
    Filter {
        input: Box<RelExpr>,
        predicate: PredicateExpr,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // Internal serialization tests are kept here because relation IR is a private
    // payload shape nested inside public policy expressions.
    #[test]
    fn rel_expr_roundtrip_json() {
        let expr = RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("teams"),
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::unscoped("id"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::Literal(Value::Integer(1)),
            },
        };

        let encoded = serde_json::to_string(&expr).expect("serialize relation IR");
        let decoded: RelExpr = serde_json::from_str(&encoded).expect("deserialize relation");
        assert_eq!(decoded, expr);
    }

    #[test]
    fn rel_expr_preserves_nested_predicates() {
        let expr = RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("resources"),
            }),
            predicate: PredicateExpr::And(vec![
                PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("owner_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["sub".to_string()]),
                },
                PredicateExpr::Or(vec![
                    PredicateExpr::IsNull {
                        column: ColumnRef::unscoped("team_id"),
                    },
                    PredicateExpr::Cmp {
                        left: ColumnRef::unscoped("team_id"),
                        op: PredicateCmpOp::Ne,
                        right: ValueRef::OuterColumn(ColumnRef::unscoped("id")),
                    },
                ]),
                PredicateExpr::IsNull {
                    column: ColumnRef::unscoped("team_id"),
                },
            ]),
        };

        let encoded = serde_json::to_vec(&expr).expect("serialize relation predicate");
        let decoded: RelExpr = serde_json::from_slice(&encoded).expect("deserialize relation");
        assert_eq!(decoded, expr);
    }
}
