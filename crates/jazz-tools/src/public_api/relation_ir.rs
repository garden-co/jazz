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
    RowId(RowIdRef),
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
    IsNotNull {
        column: ColumnRef,
    },
    In {
        left: ColumnRef,
        values: Vec<ValueRef>,
    },
    Contains {
        left: ColumnRef,
        right: ValueRef,
    },
    And(Vec<PredicateExpr>),
    Or(Vec<PredicateExpr>),
    Not(Box<PredicateExpr>),
    True,
    False,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RowIdRef {
    Current,
    Outer,
    Frontier,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KeyRef {
    Column(ColumnRef),
    RowId(RowIdRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProjectExpr {
    Column(ColumnRef),
    RowId(RowIdRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProjectColumn {
    pub alias: String,
    pub expr: ProjectExpr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinCondition {
    pub left: ColumnRef,
    pub right: ColumnRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RecursionBound {
    Fixpoint,
    MaxDepth(usize),
}

/// Relation expression used by relation-backed policy `exists(...)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelExpr {
    TableScan {
        table: TableName,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alias: Option<String>,
    },
    Filter {
        input: Box<RelExpr>,
        predicate: PredicateExpr,
    },
    Union {
        inputs: Vec<RelExpr>,
    },
    Join {
        left: Box<RelExpr>,
        right: Box<RelExpr>,
        on: Vec<JoinCondition>,
        join_kind: JoinKind,
    },
    Project {
        input: Box<RelExpr>,
        columns: Vec<ProjectColumn>,
    },
    Gather {
        seed: Box<RelExpr>,
        step: Box<RelExpr>,
        frontier_key: KeyRef,
        bound: RecursionBound,
        dedupe_key: Vec<KeyRef>,
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
                alias: None,
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
                alias: None,
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
