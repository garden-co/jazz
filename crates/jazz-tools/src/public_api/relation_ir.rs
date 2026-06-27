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

    pub fn scoped(scope: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            scope: Some(scope.into()),
            column: column.into(),
        }
    }
}

/// Stable row-id source used by explicit identity-aware joins/correlation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RowIdRef {
    Current,
    Outer,
    Frontier,
}

/// Value references that predicates can compare against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueRef {
    Literal(Value),
    SessionRef(Vec<String>),
    OuterColumn(ColumnRef),
    FrontierColumn(ColumnRef),
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
    Contains {
        left: ColumnRef,
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
    And(Vec<PredicateExpr>),
    Or(Vec<PredicateExpr>),
    Not(Box<PredicateExpr>),
    True,
    False,
}

/// Join kind for relation composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    Left,
}

/// Join condition using explicit column references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinCondition {
    pub left: ColumnRef,
    pub right: ColumnRef,
}

/// Projection key used for dedupe/canonicalization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyRef {
    Column(ColumnRef),
    RowId(RowIdRef),
}

/// Expression forms supported in projection lists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProjectExpr {
    Column(ColumnRef),
    RowId(RowIdRef),
}

/// One projected output column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectColumn {
    pub alias: String,
    pub expr: ProjectExpr,
}

/// Ordering direction for relation sorting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// One order-by term.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub column: ColumnRef,
    pub direction: OrderDirection,
}

/// Unified relation IR shared by query and policy compilers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelExpr {
    TableScan {
        table: TableName,
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
        max_depth: usize,
        dedupe_key: Vec<KeyRef>,
    },
    Distinct {
        input: Box<RelExpr>,
        key: Vec<KeyRef>,
    },
    OrderBy {
        input: Box<RelExpr>,
        terms: Vec<OrderByExpr>,
    },
    Offset {
        input: Box<RelExpr>,
        offset: usize,
    },
    Limit {
        input: Box<RelExpr>,
        limit: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hop_lowered_step() -> RelExpr {
        // Canonicalized hopTo(...) shape: Join + Project.
        RelExpr::Project {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::TableScan {
                    table: TableName::new("team_edges"),
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("teams"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("team_edges", "parent_team"),
                    right: ColumnRef::scoped("teams", "id"),
                }],
                join_kind: JoinKind::Inner,
            }),
            columns: vec![
                ProjectColumn {
                    alias: "id".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("teams", "id")),
                },
                ProjectColumn {
                    alias: "name".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("teams", "name")),
                },
            ],
        }
    }

    #[test]
    fn rel_expr_roundtrip_json() {
        let expr = RelExpr::Gather {
            seed: Box::new(RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("teams"),
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Integer(1)),
                },
            }),
            step: Box::new(hop_lowered_step()),
            frontier_key: KeyRef::RowId(RowIdRef::Current),
            max_depth: 10,
            dedupe_key: vec![KeyRef::RowId(RowIdRef::Current)],
        };

        let encoded = serde_json::to_string(&expr).expect("serialize relation IR");
        let decoded: RelExpr = serde_json::from_str(&encoded).expect("deserialize relation IR");
        assert_eq!(decoded, expr);
    }

    #[test]
    fn rel_expr_union_roundtrip_json() {
        let expr = RelExpr::Union {
            inputs: vec![
                RelExpr::TableScan {
                    table: TableName::new("teams"),
                },
                RelExpr::Filter {
                    input: Box::new(RelExpr::TableScan {
                        table: TableName::new("teams"),
                    }),
                    predicate: PredicateExpr::Cmp {
                        left: ColumnRef::unscoped("kind"),
                        op: PredicateCmpOp::Eq,
                        right: ValueRef::Literal(Value::Text("individual".to_string())),
                    },
                },
            ],
        };

        let encoded = serde_json::to_string(&expr).expect("serialize union relation IR");
        let decoded: RelExpr = serde_json::from_str(&encoded).expect("deserialize union relation");
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
                PredicateExpr::Not(Box::new(PredicateExpr::IsNull {
                    column: ColumnRef::unscoped("team_id"),
                })),
            ]),
        };

        let encoded = serde_json::to_vec(&expr).expect("serialize relation predicate");
        let decoded: RelExpr = serde_json::from_slice(&encoded).expect("deserialize relation");
        assert_eq!(decoded, expr);
    }
}
