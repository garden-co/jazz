//! Policy expressions for row-level security (ReBAC).
//!
//! Policies control access to rows based on session context and relationships
//! between rows in the database. Each operation (SELECT, INSERT, UPDATE, DELETE)
//! can have its own policy expression.

use super::relation_ir::RelExpr;
use super::types::Value;
use serde::{Deserialize, Serialize};

/// Comparison operators for policy expressions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A value in a policy expression - either a literal or a session reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "PolicyValueSerde", into = "PolicyValueSerde")]
pub enum PolicyValue {
    /// A literal value.
    Literal(Value),
    /// Reference to a session variable, e.g., ["user_id"] or ["claims", "teams"].
    SessionRef(Vec<String>),
}

/// Database operation type for policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Operation {
    Select,
    Insert,
    Update,
    Delete,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Select => write!(f, "SELECT"),
            Operation::Insert => write!(f, "INSERT"),
            Operation::Update => write!(f, "UPDATE"),
            Operation::Delete => write!(f, "DELETE"),
        }
    }
}

/// Policy expression tree.
///
/// Policies are boolean expressions evaluated against rows and session context.
/// They can reference row columns, session variables, and related rows via INHERITS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "PolicyExprSerde", into = "PolicyExprSerde")]
pub enum PolicyExpr {
    /// Compare a column value against a policy value.
    Cmp {
        column: String,
        op: CmpOp,
        value: PolicyValue,
    },

    /// Compare a session value against a literal value.
    SessionCmp {
        path: Vec<String>,
        op: CmpOp,
        value: Value,
    },

    /// Check if a column is NULL.
    IsNull { column: String },

    /// Check if a session value is NULL.
    SessionIsNull { path: Vec<String> },

    /// Check if a column is NOT NULL.
    IsNotNull { column: String },

    /// Check if a session value is NOT NULL.
    SessionIsNotNull { path: Vec<String> },

    /// Check if a column contains a value.
    ///
    /// - For TEXT columns this means substring containment.
    /// - For ARRAY columns this means element membership.
    Contains { column: String, value: PolicyValue },

    /// Check if a session value contains a literal value.
    ///
    /// - For TEXT session values this means substring containment.
    /// - For ARRAY session values this means element membership.
    SessionContains { path: Vec<String>, value: Value },

    /// Check if a column value is in a session array.
    /// The session_path must point to an array in the session claims.
    In {
        column: String,
        session_path: Vec<String>,
    },

    /// Check if a column value is contained in a list of values.
    InList {
        column: String,
        values: Vec<PolicyValue>,
    },

    /// Check if a scalar session value is contained in a list of literal values.
    SessionInList {
        path: Vec<String>,
        values: Vec<Value>,
    },

    /// Check if a subquery returns any rows.
    /// Used internally for INHERITS transformation.
    Exists {
        table: String,
        condition: Box<PolicyExpr>,
    },

    /// Check if a relation expression returns any rows.
    ///
    /// This is the declarative relation-IR form emitted by policy.exists(relation).
    ExistsRel { rel: RelExpr },

    /// Inherit permission from a related row.
    /// Looks up the row referenced by `via_column` (foreign key) and checks
    /// if that row passes the specified operation's policy.
    Inherits {
        operation: Operation,
        via_column: String,
        /// Optional recursion depth override for recursive INHERITS evaluation.
        ///
        /// If omitted, the core policy evaluator uses its default recursion depth.
        max_depth: Option<usize>,
    },

    /// Inherit permission from rows in `source_table` that reference the current row.
    ///
    /// This is the reverse direction of `Inherits`: it scans source rows where
    /// `source_table.via_column` points at the current row id, then checks whether
    /// any such source row passes `operation` policy.
    InheritsReferencing {
        operation: Operation,
        source_table: String,
        via_column: String,
        /// Optional recursion depth override for recursive INHERITS evaluation.
        ///
        /// If omitted, the core policy evaluator uses its default recursion depth.
        max_depth: Option<usize>,
    },

    /// Logical AND of multiple expressions.
    And(Vec<PolicyExpr>),

    /// Logical OR of multiple expressions.
    Or(Vec<PolicyExpr>),

    /// Logical NOT of an expression.
    Not(Box<PolicyExpr>),

    /// Always true - allows all rows.
    True,

    /// Always false - denies all rows.
    False,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
enum PolicyValueSerde {
    Literal { value: Value },
    SessionRef { path: Vec<String> },
}

impl From<PolicyValueSerde> for PolicyValue {
    fn from(value: PolicyValueSerde) -> Self {
        match value {
            PolicyValueSerde::Literal { value } => PolicyValue::Literal(value),
            PolicyValueSerde::SessionRef { path } => PolicyValue::SessionRef(path),
        }
    }
}

impl From<PolicyValue> for PolicyValueSerde {
    fn from(value: PolicyValue) -> Self {
        match value {
            PolicyValue::Literal(value) => PolicyValueSerde::Literal { value },
            PolicyValue::SessionRef(path) => PolicyValueSerde::SessionRef { path },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
enum PolicyExprSerde {
    Cmp {
        column: String,
        op: CmpOp,
        value: PolicyValue,
    },
    SessionCmp {
        path: Vec<String>,
        op: CmpOp,
        value: Value,
    },
    IsNull {
        column: String,
    },
    SessionIsNull {
        path: Vec<String>,
    },
    IsNotNull {
        column: String,
    },
    SessionIsNotNull {
        path: Vec<String>,
    },
    Contains {
        column: String,
        value: PolicyValue,
    },
    SessionContains {
        path: Vec<String>,
        value: Value,
    },
    In {
        column: String,
        session_path: Vec<String>,
    },
    InList {
        column: String,
        values: Vec<PolicyValue>,
    },
    SessionInList {
        path: Vec<String>,
        values: Vec<Value>,
    },
    Exists {
        table: String,
        condition: Box<PolicyExprSerde>,
    },
    ExistsRel {
        rel: RelExpr,
    },
    Inherits {
        operation: Operation,
        via_column: String,
        max_depth: Option<usize>,
    },
    InheritsReferencing {
        operation: Operation,
        source_table: String,
        via_column: String,
        max_depth: Option<usize>,
    },
    And {
        exprs: Vec<PolicyExprSerde>,
    },
    Or {
        exprs: Vec<PolicyExprSerde>,
    },
    Not {
        expr: Box<PolicyExprSerde>,
    },
    True {},
    False {},
}

impl From<PolicyExprSerde> for PolicyExpr {
    fn from(value: PolicyExprSerde) -> Self {
        match value {
            PolicyExprSerde::Cmp { column, op, value } => PolicyExpr::Cmp { column, op, value },
            PolicyExprSerde::SessionCmp { path, op, value } => {
                PolicyExpr::SessionCmp { path, op, value }
            }
            PolicyExprSerde::IsNull { column } => PolicyExpr::IsNull { column },
            PolicyExprSerde::SessionIsNull { path } => PolicyExpr::SessionIsNull { path },
            PolicyExprSerde::IsNotNull { column } => PolicyExpr::IsNotNull { column },
            PolicyExprSerde::SessionIsNotNull { path } => PolicyExpr::SessionIsNotNull { path },
            PolicyExprSerde::Contains { column, value } => PolicyExpr::Contains { column, value },
            PolicyExprSerde::SessionContains { path, value } => {
                PolicyExpr::SessionContains { path, value }
            }
            PolicyExprSerde::In {
                column,
                session_path,
            } => PolicyExpr::In {
                column,
                session_path,
            },
            PolicyExprSerde::InList { column, values } => PolicyExpr::InList { column, values },
            PolicyExprSerde::SessionInList { path, values } => {
                PolicyExpr::SessionInList { path, values }
            }
            PolicyExprSerde::Exists { table, condition } => PolicyExpr::Exists {
                table,
                condition: Box::new((*condition).into()),
            },
            PolicyExprSerde::ExistsRel { rel } => PolicyExpr::ExistsRel { rel },
            PolicyExprSerde::Inherits {
                operation,
                via_column,
                max_depth,
            } => PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            },
            PolicyExprSerde::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            },
            PolicyExprSerde::And { exprs } => {
                PolicyExpr::And(exprs.into_iter().map(PolicyExpr::from).collect())
            }
            PolicyExprSerde::Or { exprs } => {
                PolicyExpr::Or(exprs.into_iter().map(PolicyExpr::from).collect())
            }
            PolicyExprSerde::Not { expr } => PolicyExpr::Not(Box::new((*expr).into())),
            PolicyExprSerde::True {} => PolicyExpr::True,
            PolicyExprSerde::False {} => PolicyExpr::False,
        }
    }
}

impl From<PolicyExpr> for PolicyExprSerde {
    fn from(value: PolicyExpr) -> Self {
        match value {
            PolicyExpr::Cmp { column, op, value } => PolicyExprSerde::Cmp { column, op, value },
            PolicyExpr::SessionCmp { path, op, value } => {
                PolicyExprSerde::SessionCmp { path, op, value }
            }
            PolicyExpr::IsNull { column } => PolicyExprSerde::IsNull { column },
            PolicyExpr::SessionIsNull { path } => PolicyExprSerde::SessionIsNull { path },
            PolicyExpr::IsNotNull { column } => PolicyExprSerde::IsNotNull { column },
            PolicyExpr::SessionIsNotNull { path } => PolicyExprSerde::SessionIsNotNull { path },
            PolicyExpr::Contains { column, value } => PolicyExprSerde::Contains { column, value },
            PolicyExpr::SessionContains { path, value } => {
                PolicyExprSerde::SessionContains { path, value }
            }
            PolicyExpr::In {
                column,
                session_path,
            } => PolicyExprSerde::In {
                column,
                session_path,
            },
            PolicyExpr::InList { column, values } => PolicyExprSerde::InList { column, values },
            PolicyExpr::SessionInList { path, values } => {
                PolicyExprSerde::SessionInList { path, values }
            }
            PolicyExpr::Exists { table, condition } => PolicyExprSerde::Exists {
                table,
                condition: Box::new((*condition).into()),
            },
            PolicyExpr::ExistsRel { rel } => PolicyExprSerde::ExistsRel { rel },
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => PolicyExprSerde::Inherits {
                operation,
                via_column,
                max_depth,
            },
            PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => PolicyExprSerde::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            },
            PolicyExpr::And(exprs) => PolicyExprSerde::And {
                exprs: exprs.into_iter().map(PolicyExprSerde::from).collect(),
            },
            PolicyExpr::Or(exprs) => PolicyExprSerde::Or {
                exprs: exprs.into_iter().map(PolicyExprSerde::from).collect(),
            },
            PolicyExpr::Not(expr) => PolicyExprSerde::Not {
                expr: Box::new((*expr).into()),
            },
            PolicyExpr::True => PolicyExprSerde::True {},
            PolicyExpr::False => PolicyExprSerde::False {},
        }
    }
}

impl PolicyExpr {
    /// Create a comparison expression: column = @session.path
    pub fn eq_session(column: impl Into<String>, session_path: Vec<String>) -> Self {
        PolicyExpr::Cmp {
            column: column.into(),
            op: CmpOp::Eq,
            value: PolicyValue::SessionRef(session_path),
        }
    }

    /// Create a comparison expression: column = literal
    pub fn eq_literal(column: impl Into<String>, value: Value) -> Self {
        PolicyExpr::Cmp {
            column: column.into(),
            op: CmpOp::Eq,
            value: PolicyValue::Literal(value),
        }
    }

    /// Create an IN expression: column IN @session.path
    pub fn in_session(column: impl Into<String>, session_path: Vec<String>) -> Self {
        PolicyExpr::In {
            column: column.into(),
            session_path,
        }
    }

    /// Create an INHERITS expression.
    pub fn inherits(operation: Operation, via_column: impl Into<String>) -> Self {
        PolicyExpr::Inherits {
            operation,
            via_column: via_column.into(),
            max_depth: None,
        }
    }

    /// Create an INHERITS expression with an explicit recursion depth.
    pub fn inherits_with_depth(
        operation: Operation,
        via_column: impl Into<String>,
        max_depth: usize,
    ) -> Self {
        PolicyExpr::Inherits {
            operation,
            via_column: via_column.into(),
            max_depth: Some(max_depth),
        }
    }

    /// Create an INHERITS REFERENCING expression.
    pub fn inherits_referencing(
        operation: Operation,
        source_table: impl Into<String>,
        via_column: impl Into<String>,
    ) -> Self {
        PolicyExpr::InheritsReferencing {
            operation,
            source_table: source_table.into(),
            via_column: via_column.into(),
            max_depth: None,
        }
    }

    /// Create an INHERITS REFERENCING expression with an explicit recursion depth.
    pub fn inherits_referencing_with_depth(
        operation: Operation,
        source_table: impl Into<String>,
        via_column: impl Into<String>,
        max_depth: usize,
    ) -> Self {
        PolicyExpr::InheritsReferencing {
            operation,
            source_table: source_table.into(),
            via_column: via_column.into(),
            max_depth: Some(max_depth),
        }
    }

    /// Combine expressions with AND.
    pub fn and(exprs: Vec<PolicyExpr>) -> Self {
        if exprs.is_empty() {
            PolicyExpr::True
        } else if exprs.len() == 1 {
            exprs.into_iter().next().unwrap()
        } else {
            PolicyExpr::And(exprs)
        }
    }

    /// Combine expressions with OR.
    pub fn or(exprs: Vec<PolicyExpr>) -> Self {
        if exprs.is_empty() {
            PolicyExpr::False
        } else if exprs.len() == 1 {
            exprs.into_iter().next().unwrap()
        } else {
            PolicyExpr::Or(exprs)
        }
    }

    /// Negate an expression.
    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: PolicyExpr) -> Self {
        PolicyExpr::Not(Box::new(expr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::public_api::relation_ir::{ColumnRef, PredicateCmpOp, PredicateExpr, ValueRef};
    use serde_json::json;

    #[test]
    fn test_policy_expr_builders() {
        let expr = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
        assert!(matches!(
            expr,
            PolicyExpr::Cmp {
                column,
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(path)
            } if column == "owner_id" && path == vec!["user_id"]
        ));

        let expr = PolicyExpr::eq_literal("status", Value::Text("active".into()));
        assert!(matches!(
            expr,
            PolicyExpr::Cmp {
                column,
                op: CmpOp::Eq,
                value: PolicyValue::Literal(Value::Text(s))
            } if column == "status" && s == "active"
        ));

        let expr = PolicyExpr::in_session("team_id", vec!["claims".into(), "teams".into()]);
        assert!(matches!(
            expr,
            PolicyExpr::In { column, session_path }
            if column == "team_id" && session_path == vec!["claims", "teams"]
        ));

        let expr = PolicyExpr::inherits(Operation::Select, "folder_id");
        assert!(matches!(
            expr,
            PolicyExpr::Inherits {
                operation: Operation::Select,
                via_column,
                max_depth: None,
            } if via_column == "folder_id"
        ));

        let expr = PolicyExpr::inherits_with_depth(Operation::Select, "folder_id", 7);
        assert!(matches!(
            expr,
            PolicyExpr::Inherits {
                operation: Operation::Select,
                via_column,
                max_depth: Some(7),
            } if via_column == "folder_id"
        ));
    }

    #[test]
    fn test_policy_expr_combinators() {
        assert_eq!(PolicyExpr::and(vec![]), PolicyExpr::True);

        let inner = PolicyExpr::True;
        assert_eq!(PolicyExpr::and(vec![inner.clone()]), inner);

        assert_eq!(PolicyExpr::or(vec![]), PolicyExpr::False);
        assert_eq!(PolicyExpr::or(vec![inner.clone()]), inner);

        let and_expr = PolicyExpr::and(vec![PolicyExpr::True, PolicyExpr::False]);
        assert!(matches!(and_expr, PolicyExpr::And(v) if v.len() == 2));

        let or_expr = PolicyExpr::or(vec![PolicyExpr::True, PolicyExpr::False]);
        assert!(matches!(or_expr, PolicyExpr::Or(v) if v.len() == 2));
    }

    #[test]
    fn policy_expr_serde_preserves_relation_ir_shape() {
        let expr = PolicyExpr::ExistsRel {
            rel: RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: "todo_shares".into(),
                    alias: None,
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("todo_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["user_id".into()]),
                },
            },
        };

        let encoded = serde_json::to_value(&expr).unwrap();
        assert_eq!(encoded["type"], json!("ExistsRel"));
        let decoded: PolicyExpr = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, expr);
    }
}
