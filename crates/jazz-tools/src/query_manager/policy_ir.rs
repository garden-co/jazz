use serde::{Deserialize, Serialize};

use super::policy::Operation;
use super::relation_ir::{PredicateExpr, RelExpr};

/// Operation enum for policy-v2 serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PolicyOperation {
    Select,
    Insert,
    Update,
    Delete,
}

impl From<Operation> for PolicyOperation {
    fn from(value: Operation) -> Self {
        match value {
            Operation::Select => Self::Select,
            Operation::Insert => Self::Insert,
            Operation::Update => Self::Update,
            Operation::Delete => Self::Delete,
        }
    }
}

impl From<PolicyOperation> for Operation {
    fn from(value: PolicyOperation) -> Self {
        match value {
            PolicyOperation::Select => Self::Select,
            PolicyOperation::Insert => Self::Insert,
            PolicyOperation::Update => Self::Update,
            PolicyOperation::Delete => Self::Delete,
        }
    }
}

/// Unified policy IR that can embed shared relation IR for exists checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyExprV2 {
    Predicate(PredicateExpr),
    ExistsRel {
        rel: RelExpr,
    },
    Inherits {
        operation: PolicyOperation,
        via_column: String,
        max_depth: Option<usize>,
    },
    And(Vec<PolicyExprV2>),
    Or(Vec<PolicyExprV2>),
    Not(Box<PolicyExprV2>),
    True,
    False,
}

impl PolicyExprV2 {
    pub fn and(exprs: Vec<PolicyExprV2>) -> Self {
        if exprs.is_empty() {
            Self::True
        } else if exprs.len() == 1 {
            exprs.into_iter().next().expect("single element")
        } else {
            Self::And(exprs)
        }
    }

    pub fn or(exprs: Vec<PolicyExprV2>) -> Self {
        if exprs.is_empty() {
            Self::False
        } else if exprs.len() == 1 {
            exprs.into_iter().next().expect("single element")
        } else {
            Self::Or(exprs)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::query_manager::relation_ir::{
        ColumnRef, KeyRef, PredicateCmpOp, ProjectColumn, ProjectExpr, RowIdRef, ValueRef,
    };
    use crate::query_manager::types::{TableName, Value};

    use super::*;

    #[test]
    fn policy_expr_v2_roundtrip_json() {
        let rel = RelExpr::Gather {
            seed: Box::new(RelExpr::TableScan {
                table: TableName::new("teams"),
            }),
            step: Box::new(RelExpr::Project {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("team_edges"),
                }),
                columns: vec![ProjectColumn {
                    alias: "id".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::unscoped("parent_team")),
                }],
            }),
            frontier_key: KeyRef::RowId(RowIdRef::Current),
            max_depth: 10,
            dedupe_key: vec![KeyRef::RowId(RowIdRef::Current)],
        };

        let expr = PolicyExprV2::and(vec![
            PolicyExprV2::ExistsRel { rel },
            PolicyExprV2::Predicate(PredicateExpr::Cmp {
                left: ColumnRef::unscoped("owner_id"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::Literal(Value::Text("alice".to_string())),
            }),
        ]);

        let encoded = serde_json::to_string(&expr).expect("serialize policy v2");
        let decoded: PolicyExprV2 = serde_json::from_str(&encoded).expect("deserialize policy v2");
        assert_eq!(decoded, expr);
    }

    #[test]
    fn policy_operation_conversion_matches_existing_operation() {
        for op in [
            Operation::Select,
            Operation::Insert,
            Operation::Update,
            Operation::Delete,
        ] {
            let converted = PolicyOperation::from(op);
            let back: Operation = converted.into();
            assert_eq!(back, op);
        }
    }
}
