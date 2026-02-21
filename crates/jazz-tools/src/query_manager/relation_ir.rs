use serde::{Deserialize, Serialize};

use crate::query_manager::types::{TableName, Value};

pub const RELATION_GATHER_MAX_DEPTH_DEFAULT: usize = 10;
pub const RELATION_GATHER_MAX_DEPTH_HARD_CAP: usize = 64;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationIrError {
    GatherDepthOutOfRange { depth: usize, hard_cap: usize },
    EmptyKeySet(&'static str),
}

impl std::fmt::Display for RelationIrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelationIrError::GatherDepthOutOfRange { depth, hard_cap } => {
                write!(
                    f,
                    "gather max_depth {depth} is out of range (must be 1..={hard_cap})"
                )
            }
            RelationIrError::EmptyKeySet(ctx) => write!(f, "{ctx} requires at least one key"),
        }
    }
}

impl std::error::Error for RelationIrError {}

pub fn normalize_gather_depth(requested: Option<usize>) -> Result<usize, RelationIrError> {
    let depth = requested.unwrap_or(RELATION_GATHER_MAX_DEPTH_DEFAULT);
    if depth == 0 || depth > RELATION_GATHER_MAX_DEPTH_HARD_CAP {
        return Err(RelationIrError::GatherDepthOutOfRange {
            depth,
            hard_cap: RELATION_GATHER_MAX_DEPTH_HARD_CAP,
        });
    }
    Ok(depth)
}

/// Canonicalize a relation expression into a deterministic normalized form.
///
/// This pass intentionally focuses on invariants and shape consistency.
/// It does not perform schema-aware validation.
pub fn canonicalize_rel_expr(expr: RelExpr) -> Result<RelExpr, RelationIrError> {
    match expr {
        RelExpr::TableScan { .. } => Ok(expr),
        RelExpr::Filter { input, predicate } => Ok(RelExpr::Filter {
            input: Box::new(canonicalize_rel_expr(*input)?),
            predicate: canonicalize_predicate(predicate),
        }),
        RelExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => Ok(RelExpr::Join {
            left: Box::new(canonicalize_rel_expr(*left)?),
            right: Box::new(canonicalize_rel_expr(*right)?),
            on: canonicalize_join_conditions(on),
            join_kind,
        }),
        RelExpr::Project { input, columns } => Ok(RelExpr::Project {
            input: Box::new(canonicalize_rel_expr(*input)?),
            columns,
        }),
        RelExpr::Gather {
            seed,
            step,
            frontier_key,
            max_depth,
            mut dedupe_key,
        } => {
            let depth = normalize_gather_depth(Some(max_depth))?;
            if dedupe_key.is_empty() {
                dedupe_key.push(frontier_key.clone());
            }
            dedupe_key = canonicalize_keys(dedupe_key);
            Ok(RelExpr::Gather {
                seed: Box::new(canonicalize_rel_expr(*seed)?),
                step: Box::new(canonicalize_rel_expr(*step)?),
                frontier_key,
                max_depth: depth,
                dedupe_key,
            })
        }
        RelExpr::Distinct { input, key } => {
            let key = canonicalize_keys(key);
            if key.is_empty() {
                return Err(RelationIrError::EmptyKeySet("distinct"));
            }
            Ok(RelExpr::Distinct {
                input: Box::new(canonicalize_rel_expr(*input)?),
                key,
            })
        }
        RelExpr::OrderBy { input, terms } => Ok(RelExpr::OrderBy {
            input: Box::new(canonicalize_rel_expr(*input)?),
            terms,
        }),
        RelExpr::Offset { input, offset } => Ok(RelExpr::Offset {
            input: Box::new(canonicalize_rel_expr(*input)?),
            offset,
        }),
        RelExpr::Limit { input, limit } => Ok(RelExpr::Limit {
            input: Box::new(canonicalize_rel_expr(*input)?),
            limit,
        }),
    }
}

fn canonicalize_predicate(predicate: PredicateExpr) -> PredicateExpr {
    match predicate {
        PredicateExpr::And(exprs) => {
            let mut flattened = Vec::new();
            for expr in exprs.into_iter().map(canonicalize_predicate) {
                match expr {
                    PredicateExpr::And(inner) => flattened.extend(inner),
                    PredicateExpr::True => {}
                    other => flattened.push(other),
                }
            }
            if flattened.is_empty() {
                PredicateExpr::True
            } else if flattened.len() == 1 {
                flattened.into_iter().next().expect("single predicate")
            } else {
                PredicateExpr::And(flattened)
            }
        }
        PredicateExpr::Or(exprs) => {
            let mut flattened = Vec::new();
            for expr in exprs.into_iter().map(canonicalize_predicate) {
                match expr {
                    PredicateExpr::Or(inner) => flattened.extend(inner),
                    PredicateExpr::False => {}
                    other => flattened.push(other),
                }
            }
            if flattened.is_empty() {
                PredicateExpr::False
            } else if flattened.len() == 1 {
                flattened.into_iter().next().expect("single predicate")
            } else {
                PredicateExpr::Or(flattened)
            }
        }
        PredicateExpr::Not(inner) => match canonicalize_predicate(*inner) {
            PredicateExpr::True => PredicateExpr::False,
            PredicateExpr::False => PredicateExpr::True,
            other => PredicateExpr::Not(Box::new(other)),
        },
        other => other,
    }
}

fn canonicalize_join_conditions(mut conditions: Vec<JoinCondition>) -> Vec<JoinCondition> {
    conditions.sort_by(|a, b| {
        let left = (
            a.left.scope.as_deref().unwrap_or(""),
            a.left.column.as_str(),
            a.right.scope.as_deref().unwrap_or(""),
            a.right.column.as_str(),
        );
        let right = (
            b.left.scope.as_deref().unwrap_or(""),
            b.left.column.as_str(),
            b.right.scope.as_deref().unwrap_or(""),
            b.right.column.as_str(),
        );
        left.cmp(&right)
    });
    conditions.dedup();
    conditions
}

fn canonicalize_keys(mut keys: Vec<KeyRef>) -> Vec<KeyRef> {
    keys.sort_by_key(key_ref_sort_key);
    keys.dedup();
    keys
}

fn key_ref_sort_key(key: &KeyRef) -> (u8, String, String) {
    match key {
        KeyRef::RowId(row_id) => (0, format!("{row_id:?}"), String::new()),
        KeyRef::Column(column) => (
            1,
            column.scope.as_deref().unwrap_or("").to_string(),
            column.column.clone(),
        ),
    }
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

    #[test]
    fn canonicalize_gather_applies_default_dedupe_key() {
        let expr = RelExpr::Gather {
            seed: Box::new(RelExpr::TableScan {
                table: TableName::new("teams"),
            }),
            step: Box::new(RelExpr::TableScan {
                table: TableName::new("team_edges"),
            }),
            frontier_key: KeyRef::RowId(RowIdRef::Current),
            max_depth: RELATION_GATHER_MAX_DEPTH_DEFAULT,
            dedupe_key: vec![],
        };

        let canonical = canonicalize_rel_expr(expr).expect("canonicalize gather");
        let RelExpr::Gather { dedupe_key, .. } = canonical else {
            panic!("expected gather expression")
        };
        assert_eq!(dedupe_key, vec![KeyRef::RowId(RowIdRef::Current)]);
    }

    #[test]
    fn canonicalize_rejects_invalid_gather_depth() {
        let expr = RelExpr::Gather {
            seed: Box::new(RelExpr::TableScan {
                table: TableName::new("teams"),
            }),
            step: Box::new(RelExpr::TableScan {
                table: TableName::new("team_edges"),
            }),
            frontier_key: KeyRef::RowId(RowIdRef::Current),
            max_depth: RELATION_GATHER_MAX_DEPTH_HARD_CAP + 1,
            dedupe_key: vec![KeyRef::RowId(RowIdRef::Current)],
        };

        let err = canonicalize_rel_expr(expr).expect_err("should reject depth > hard cap");
        assert!(matches!(err, RelationIrError::GatherDepthOutOfRange { .. }));
    }

    #[test]
    fn canonicalize_flattens_predicate_boolean_forms() {
        let expr = RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("resources"),
            }),
            predicate: PredicateExpr::And(vec![
                PredicateExpr::True,
                PredicateExpr::And(vec![PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("owner_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Text("alice".to_string())),
                }]),
            ]),
        };

        let canonical = canonicalize_rel_expr(expr).expect("canonicalize predicate");
        let RelExpr::Filter { predicate, .. } = canonical else {
            panic!("expected filter expression")
        };
        assert!(matches!(predicate, PredicateExpr::Cmp { .. }));
    }
}
