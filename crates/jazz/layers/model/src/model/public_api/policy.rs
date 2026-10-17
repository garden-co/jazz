//! Policy expressions for row-level security (ReBAC).
//!
//! Policies control access to rows based on session context and relationships
//! between rows in the database. Each operation (SELECT, INSERT, UPDATE, DELETE)
//! can have its own policy expression.

use super::types::Value;
use crate::query::RelationExpr;
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
#[serde(tag = "type")]
pub enum PolicyValue {
    /// A literal value.
    Literal(#[serde(with = "value_payload")] Value),
    /// Reference to a session variable, e.g., ["user_id"] or ["claims", "teams"].
    SessionRef(#[serde(with = "path_payload")] Vec<String>),
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
#[serde(tag = "type")]
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
    ExistsRel { rel: RelationExpr },

    /// Inherit permission from a related row.
    /// Looks up the row referenced by `via_column` (foreign key) and checks
    /// if that row passes the specified operation's policy.
    Inherits {
        operation: Operation,
        via_column: String,
        /// Optional recursion depth override for recursive INHERITS evaluation.
        ///
        /// If omitted, the core policy evaluator uses its default recursion depth.
        /// Zero performs no inheritance hop and therefore cannot grant access.
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
        /// Zero performs no inheritance hop and therefore cannot grant access.
        max_depth: Option<usize>,
    },

    /// Logical AND of multiple expressions.
    And(#[serde(with = "exprs_payload")] Vec<PolicyExpr>),

    /// Logical OR of multiple expressions.
    Or(#[serde(with = "exprs_payload")] Vec<PolicyExpr>),

    /// Logical NOT of an expression.
    Not(#[serde(with = "expr_payload")] Box<PolicyExpr>),

    /// Always true - allows all rows.
    True,

    /// Always false - denies all rows.
    False,
}

/// Serde adapter for an internally tagged newtype variant: its payload is
/// written as a one-field struct, so the variant reads
/// `{"type": <variant>, <field>: <payload>}`.
macro_rules! one_field_payload {
    ($module:ident, $field:ident: $ty:ty) => {
        mod $module {
            use super::*;

            #[derive(Serialize)]
            struct Out<'a> {
                $field: &'a $ty,
            }

            #[derive(Deserialize)]
            struct In {
                $field: $ty,
            }

            pub(super) fn serialize<S: serde::Serializer>(
                $field: &$ty,
                serializer: S,
            ) -> Result<S::Ok, S::Error> {
                Out { $field }.serialize(serializer)
            }

            pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<$ty, D::Error> {
                Ok(In::deserialize(deserializer)?.$field)
            }
        }
    };
}

one_field_payload!(value_payload, value: Value);
one_field_payload!(path_payload, path: Vec<String>);
one_field_payload!(exprs_payload, exprs: Vec<PolicyExpr>);
one_field_payload!(expr_payload, expr: Box<PolicyExpr>);

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
    use crate::query::{
        RelationCmpOp as PredicateCmpOp, RelationColumnRef as ColumnRef,
        RelationPredicate as PredicateExpr, RelationValueRef as ValueRef,
    };
    use serde_json::json;

    #[test]
    fn test_policy_expr_builders() {
        let expr = PolicyExpr::eq_session("owner_id", vec!["claims".into(), "user_id".into()]);
        assert!(matches!(
            expr,
            PolicyExpr::Cmp {
                column,
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(path)
            } if column == "owner_id" && path == vec!["claims", "user_id"]
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
            rel: RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "todo_shares".into(),
                    alias: None,
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("todo_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["claims".into(), "user_id".into()]),
                },
            },
        };

        let encoded = serde_json::to_value(&expr).unwrap();
        assert_eq!(encoded["type"], json!("ExistsRel"));
        let decoded: PolicyExpr = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, expr);
    }

    fn every_policy_variant() -> PolicyExpr {
        let path = || vec!["claims".to_owned(), "team".to_owned()];
        let rel = RelationExpr::TableScan {
            table: "shares".into(),
            alias: None,
        };
        PolicyExpr::Or(vec![
            PolicyExpr::Cmp {
                column: "a".into(),
                op: CmpOp::Le,
                value: PolicyValue::Literal(Value::Integer(3)),
            },
            PolicyExpr::SessionCmp {
                path: path(),
                op: CmpOp::Ne,
                value: Value::Text("x".into()),
            },
            PolicyExpr::IsNull { column: "b".into() },
            PolicyExpr::SessionIsNull { path: path() },
            PolicyExpr::IsNotNull { column: "c".into() },
            PolicyExpr::SessionIsNotNull { path: path() },
            PolicyExpr::Contains {
                column: "d".into(),
                value: PolicyValue::SessionRef(path()),
            },
            PolicyExpr::SessionContains {
                path: path(),
                value: Value::BigInt(-9),
            },
            PolicyExpr::In {
                column: "e".into(),
                session_path: path(),
            },
            PolicyExpr::InList {
                column: "f".into(),
                values: vec![
                    PolicyValue::Literal(Value::Boolean(true)),
                    PolicyValue::SessionRef(path()),
                ],
            },
            PolicyExpr::SessionInList {
                path: path(),
                values: vec![Value::Null, Value::Double(1.5)],
            },
            PolicyExpr::Exists {
                table: "t".into(),
                condition: Box::new(PolicyExpr::Not(Box::new(PolicyExpr::True))),
            },
            PolicyExpr::ExistsRel { rel },
            PolicyExpr::Inherits {
                operation: Operation::Select,
                via_column: "g".into(),
                max_depth: Some(2),
            },
            PolicyExpr::InheritsReferencing {
                operation: Operation::Update,
                source_table: "s".into(),
                via_column: "h".into(),
                max_depth: None,
            },
            PolicyExpr::And(vec![PolicyExpr::False, PolicyExpr::Or(vec![])]),
        ])
    }

    #[test]
    fn policy_expr_json_is_pinned() {
        let expr = every_policy_variant();
        let json = r#"{"type":"Or","exprs":[{"type":"Cmp","column":"a","op":"Le","value":{"type":"Literal","value":{"type":"Integer","value":3}}},{"type":"SessionCmp","path":["claims","team"],"op":"Ne","value":{"type":"Text","value":"x"}},{"type":"IsNull","column":"b"},{"type":"SessionIsNull","path":["claims","team"]},{"type":"IsNotNull","column":"c"},{"type":"SessionIsNotNull","path":["claims","team"]},{"type":"Contains","column":"d","value":{"type":"SessionRef","path":["claims","team"]}},{"type":"SessionContains","path":["claims","team"],"value":{"type":"BigInt","value":"-9"}},{"type":"In","column":"e","session_path":["claims","team"]},{"type":"InList","column":"f","values":[{"type":"Literal","value":{"type":"Boolean","value":true}},{"type":"SessionRef","path":["claims","team"]}]},{"type":"SessionInList","path":["claims","team"],"values":[{"type":"Null"},{"type":"Double","value":1.5}]},{"type":"Exists","table":"t","condition":{"type":"Not","expr":{"type":"True"}}},{"type":"ExistsRel","rel":{"TableScan":{"table":"shares"}}},{"type":"Inherits","operation":"Select","via_column":"g","max_depth":2},{"type":"InheritsReferencing","operation":"Update","source_table":"s","via_column":"h","max_depth":null},{"type":"And","exprs":[{"type":"False"},{"type":"Or","exprs":[]}]}]}"#;
        assert_eq!(serde_json::to_string(&expr).unwrap(), json);
        assert_eq!(serde_json::from_str::<PolicyExpr>(json).unwrap(), expr);
        let session_ref = |path: &str| PolicyExpr::Cmp {
            column: "a".into(),
            op: CmpOp::Eq,
            value: PolicyValue::SessionRef(vec![path.into()]),
        };
        for (input, decoded) in [
            (r#"{"type":"True","extra":1}"#, PolicyExpr::True),
            (
                r#"{"type":"And","exprs":[],"extra":1}"#,
                PolicyExpr::And(vec![]),
            ),
            (r#"{"exprs":[],"type":"Or"}"#, PolicyExpr::Or(vec![])),
            (r#"["True"]"#, PolicyExpr::True),
            (r#"["And",[]]"#, PolicyExpr::And(vec![])),
            (
                r#"["Not",{"type":"True"}]"#,
                PolicyExpr::not(PolicyExpr::True),
            ),
            (
                r#"{"type":"Cmp","column":"a","op":"Eq","value":{"type":"SessionRef","path":["x"],"more":2}}"#,
                session_ref("x"),
            ),
            (
                r#"{"type":"Cmp","column":"a","op":"Eq","value":["SessionRef",["x"]]}"#,
                session_ref("x"),
            ),
        ] {
            assert_eq!(
                serde_json::from_str::<PolicyExpr>(input).unwrap(),
                decoded,
                "{input}"
            );
        }
        for (input, error) in [
            (r#"{"type":"And"}"#, "missing field `exprs`"),
            (r#"{"type":"Not"}"#, "missing field `expr`"),
            (
                r#"{"type":"Cmp","column":"a","op":"Eq","value":{"type":"Literal"}}"#,
                "missing field `value`",
            ),
            (
                r#"{"type":"And","exprs":3}"#,
                "invalid type: integer `3`, expected a sequence",
            ),
            (
                r#"{"type":"Nope"}"#,
                "unknown variant `Nope`, expected one of `Cmp`, `SessionCmp`, `IsNull`, \
                 `SessionIsNull`, `IsNotNull`, `SessionIsNotNull`, `Contains`, \
                 `SessionContains`, `In`, `InList`, `SessionInList`, `Exists`, `ExistsRel`, \
                 `Inherits`, `InheritsReferencing`, `And`, `Or`, `Not`, `True`, `False` at line \
                 1 column 14",
            ),
        ] {
            let actual = serde_json::from_str::<PolicyExpr>(input)
                .unwrap_err()
                .to_string();
            assert_eq!(actual, error, "{input}");
        }
        for input in [
            r#""True""#,
            r#"{"type":"Not","expr":3}"#,
            r#"{"type":"Cmp","column":"a","op":"Eq","value":"x"}"#,
        ] {
            assert!(
                serde_json::from_str::<PolicyExpr>(input).is_err(),
                "{input}"
            );
        }
    }
}
