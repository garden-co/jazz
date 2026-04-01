//! Policy expressions for row-level security (ReBAC).
//!
//! Policies control access to rows based on session context and relationships
//! between rows in the database. Each operation (SELECT, INSERT, UPDATE, DELETE)
//! can have its own policy expression.

use super::encoding::{
    column_bytes, column_is_null, compare_column_to_value, decode_column, encode_value,
};
use super::magic_columns::{MagicColumnKind, magic_column_kind};
use super::relation_ir::{PredicateExpr, RelExpr, RowIdRef, ValueRef};
use super::session::Session;
use super::types::{RowDescriptor, Value};
use crate::metadata::RowProvenance;
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

/// Reserved session path prefix used to encode outer-row references in correlated EXISTS clauses.
pub const OUTER_ROW_SESSION_PREFIX: &str = "__jazz_outer_row";

/// Default recursion depth for recursive permission checks.
pub const RECURSIVE_POLICY_MAX_DEPTH_DEFAULT: usize = 10;
/// Hard cap recursion depth for recursive permission checks.
pub const RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP: usize = 64;

/// Resolve requested recursion depth for policy recursion.
///
/// Returns `None` when depth is invalid or exceeds hard cap.
pub fn normalize_recursive_max_depth(requested: Option<usize>) -> Option<usize> {
    let depth = requested.unwrap_or(RECURSIVE_POLICY_MAX_DEPTH_DEFAULT);
    if depth == 0 || depth > RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
        None
    } else {
        Some(depth)
    }
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
        /// If omitted, runtime uses [`RECURSIVE_POLICY_MAX_DEPTH_DEFAULT`].
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
        /// If omitted, runtime uses [`RECURSIVE_POLICY_MAX_DEPTH_DEFAULT`].
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

// ============================================================================
// Shared policy evaluation functions
// ============================================================================

use std::collections::HashSet;

use crate::object::ObjectId;

use super::types::{Schema, TableName};

/// Context for policy evaluation with INHERITS support.
pub struct EvalContext<'a, F>
where
    F: FnMut(ObjectId) -> Option<Vec<u8>>,
{
    pub session: &'a Session,
    pub schema: &'a Schema,
    pub table_name: &'a TableName,
    pub row_loader: F,
    /// Track visited ObjectIds to detect cycles in INHERITS chains.
    visited: HashSet<ObjectId>,
}

impl<'a, F> EvalContext<'a, F>
where
    F: FnMut(ObjectId) -> Option<Vec<u8>>,
{
    pub fn new(
        session: &'a Session,
        schema: &'a Schema,
        table_name: &'a TableName,
        row_loader: F,
    ) -> Self {
        Self {
            session,
            schema,
            table_name,
            row_loader,
            visited: HashSet::new(),
        }
    }
}

/// Evaluate a policy expression with full INHERITS support.
///
/// This requires a row loader to fetch parent rows for INHERITS evaluation.
pub fn evaluate_with_context<F>(
    expr: &PolicyExpr,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    ctx: &mut EvalContext<F>,
) -> bool
where
    F: FnMut(ObjectId) -> Option<Vec<u8>>,
{
    evaluate_recursive(expr, content, provenance, descriptor, ctx, 0)
}

fn evaluate_recursive<F>(
    expr: &PolicyExpr,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    ctx: &mut EvalContext<F>,
    depth: usize,
) -> bool
where
    F: FnMut(ObjectId) -> Option<Vec<u8>>,
{
    // Prevent infinite recursion
    if depth > RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
        return false;
    }

    match expr {
        PolicyExpr::True => true,
        PolicyExpr::False => false,

        PolicyExpr::Cmp { column, op, value } => evaluate_cmp(
            column,
            op,
            value,
            content,
            provenance,
            descriptor,
            ctx.session,
        ),
        PolicyExpr::SessionCmp { path, op, value } => {
            evaluate_session_cmp(path, op, value, ctx.session)
        }

        PolicyExpr::IsNull { column } => magic_column_value(column, provenance)
            .map(|value| value.is_null())
            .unwrap_or_else(|| {
                if let Some(col_index) = descriptor.column_index(column) {
                    column_is_null(descriptor, content, col_index).unwrap_or(false)
                } else {
                    false
                }
            }),
        PolicyExpr::SessionIsNull { path } => {
            matches!(resolve_session_value(path, ctx.session), Some(Value::Null))
        }

        PolicyExpr::IsNotNull { column } => magic_column_value(column, provenance)
            .map(|value| !value.is_null())
            .unwrap_or_else(|| {
                if let Some(col_index) = descriptor.column_index(column) {
                    !column_is_null(descriptor, content, col_index).unwrap_or(true)
                } else {
                    false
                }
            }),
        PolicyExpr::SessionIsNotNull { path } => {
            matches!(resolve_session_value(path, ctx.session), Some(value) if !value.is_null())
        }

        PolicyExpr::Contains { column, value } => {
            evaluate_contains(column, value, content, provenance, descriptor, ctx.session)
        }
        PolicyExpr::SessionContains { path, value } => {
            evaluate_session_contains(path, value, ctx.session)
        }

        PolicyExpr::In {
            column,
            session_path,
        } => evaluate_in(
            column,
            session_path,
            content,
            provenance,
            descriptor,
            ctx.session,
        ),

        PolicyExpr::InList { column, values } => {
            evaluate_in_list(column, values, content, provenance, descriptor, ctx.session)
        }
        PolicyExpr::SessionInList { path, values } => {
            evaluate_session_in_list(path, values, ctx.session)
        }

        PolicyExpr::And(exprs) => exprs
            .iter()
            .all(|e| evaluate_recursive(e, content, provenance, descriptor, ctx, depth)),

        PolicyExpr::Or(exprs) => exprs
            .iter()
            .any(|e| evaluate_recursive(e, content, provenance, descriptor, ctx, depth)),

        PolicyExpr::Not(inner) => {
            !evaluate_recursive(inner, content, provenance, descriptor, ctx, depth)
        }

        PolicyExpr::Exists { .. } => {
            // EXISTS is an internal representation, not directly used
            true
        }
        PolicyExpr::ExistsRel { .. } => {
            // EXISTS REL requires graph/context evaluation, not direct scalar eval.
            true
        }

        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => evaluate_inherits(
            *operation, via_column, *max_depth, content, provenance, descriptor, ctx, depth,
        ),
        PolicyExpr::InheritsReferencing { .. } => {
            // Requires table/index context not available in EvalContext.
            false
        }
    }
}

/// Evaluate INHERITS by loading the parent row and checking its policy.
#[allow(clippy::too_many_arguments)]
fn evaluate_inherits<F>(
    operation: Operation,
    via_column: &str,
    max_depth: Option<usize>,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    ctx: &mut EvalContext<F>,
    depth: usize,
) -> bool
where
    F: FnMut(ObjectId) -> Option<Vec<u8>>,
{
    let Some(effective_max_depth) = normalize_recursive_max_depth(max_depth) else {
        return false;
    };
    if depth >= effective_max_depth {
        return false;
    }

    // Get the FK column index
    let col_index = match descriptor.column_index(via_column) {
        Some(idx) => idx,
        None => return false, // Column not found
    };

    // Check if FK is NULL - if so, INHERITS passes (no parent to check)
    if column_is_null(descriptor, content, col_index).unwrap_or(false) {
        return true;
    }

    // Get the FK column descriptor to find the referenced table
    let col_desc = &descriptor.columns[col_index];
    let parent_table = match &col_desc.references {
        Some(table) => table,
        None => return false, // No FK reference defined - schema error
    };

    // Decode the FK value to get the parent ObjectId
    let parent_id = match decode_column(descriptor, content, col_index) {
        Ok(Value::Uuid(id)) => id,
        _ => return false, // FK must be UUID type
    };

    // Check for cycles - if we've already visited this object, deny
    if ctx.visited.contains(&parent_id) {
        return false;
    }
    ctx.visited.insert(parent_id);

    // Load the parent row
    let parent_content = match (ctx.row_loader)(parent_id) {
        Some(content) => content,
        None => return false, // Parent not found - deny access
    };

    // Get the parent table's schema
    let parent_schema = match ctx.schema.get(parent_table) {
        Some(schema) => schema,
        None => return false, // Parent table not in schema
    };

    // Get the parent's policy for the specified operation
    let parent_policy = match operation {
        Operation::Select => parent_schema.policies.select.using.as_ref(),
        Operation::Insert => parent_schema.policies.insert.with_check.as_ref(),
        Operation::Update => parent_schema.policies.update.using.as_ref(),
        Operation::Delete => parent_schema.policies.effective_delete_using(),
    };

    // If parent has no policy, allow access
    let parent_policy = match parent_policy {
        Some(p) => p,
        None => return true,
    };

    // Recursively evaluate the parent's policy
    evaluate_recursive(
        parent_policy,
        &parent_content,
        provenance,
        &parent_schema.columns,
        ctx,
        depth + 1,
    )
}

/// Simple evaluation without INHERITS support (for backwards compatibility).
/// INHERITS expressions return true (permissive).
pub fn evaluate_policy_expr(
    expr: &PolicyExpr,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    evaluate_expr_simple(expr, content, provenance, descriptor, session, 0)
}

fn evaluate_expr_simple(
    expr: &PolicyExpr,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    session: &Session,
    depth: usize,
) -> bool {
    if depth > RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
        return false;
    }

    match expr {
        PolicyExpr::True => true,
        PolicyExpr::False => false,
        PolicyExpr::Cmp { column, op, value } => {
            evaluate_cmp(column, op, value, content, provenance, descriptor, session)
        }
        PolicyExpr::SessionCmp { path, op, value } => {
            evaluate_session_cmp(path, op, value, session)
        }
        PolicyExpr::IsNull { column } => magic_column_value(column, provenance)
            .map(|value| value.is_null())
            .unwrap_or_else(|| {
                descriptor
                    .column_index(column)
                    .map(|i| column_is_null(descriptor, content, i).unwrap_or(false))
                    .unwrap_or(false)
            }),
        PolicyExpr::SessionIsNull { path } => {
            matches!(resolve_session_value(path, session), Some(Value::Null))
        }
        PolicyExpr::IsNotNull { column } => magic_column_value(column, provenance)
            .map(|value| !value.is_null())
            .unwrap_or_else(|| {
                descriptor
                    .column_index(column)
                    .map(|i| !column_is_null(descriptor, content, i).unwrap_or(true))
                    .unwrap_or(false)
            }),
        PolicyExpr::SessionIsNotNull { path } => {
            matches!(resolve_session_value(path, session), Some(value) if !value.is_null())
        }
        PolicyExpr::Contains { column, value } => {
            evaluate_contains(column, value, content, provenance, descriptor, session)
        }
        PolicyExpr::SessionContains { path, value } => {
            evaluate_session_contains(path, value, session)
        }
        PolicyExpr::In {
            column,
            session_path,
        } => evaluate_in(
            column,
            session_path,
            content,
            provenance,
            descriptor,
            session,
        ),
        PolicyExpr::InList { column, values } => {
            evaluate_in_list(column, values, content, provenance, descriptor, session)
        }
        PolicyExpr::SessionInList { path, values } => {
            evaluate_session_in_list(path, values, session)
        }
        PolicyExpr::And(exprs) => exprs
            .iter()
            .all(|e| evaluate_expr_simple(e, content, provenance, descriptor, session, depth)),
        PolicyExpr::Or(exprs) => exprs
            .iter()
            .any(|e| evaluate_expr_simple(e, content, provenance, descriptor, session, depth)),
        PolicyExpr::Not(inner) => {
            !evaluate_expr_simple(inner, content, provenance, descriptor, session, depth)
        }
        PolicyExpr::Exists { .. } => true,
        PolicyExpr::ExistsRel { .. } => true,
        PolicyExpr::Inherits { .. } => true, // No row loader - permissive
        PolicyExpr::InheritsReferencing { .. } => true, // Requires table/index context
    }
}

/// Recursive evaluation with depth tracking. Public for use by PolicyFilterNode.
pub fn evaluate_expr_recursive(
    expr: &PolicyExpr,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    session: &Session,
    depth: usize,
) -> bool {
    evaluate_expr_simple(expr, content, provenance, descriptor, session, depth)
}

fn provenance_value(kind: MagicColumnKind, provenance: &RowProvenance) -> Value {
    match kind {
        MagicColumnKind::CreatedBy => Value::Text(provenance.created_by.clone()),
        MagicColumnKind::CreatedAt => Value::Timestamp(provenance.created_at),
        MagicColumnKind::UpdatedBy => Value::Text(provenance.updated_by.clone()),
        MagicColumnKind::UpdatedAt => Value::Timestamp(provenance.updated_at),
        MagicColumnKind::CanRead | MagicColumnKind::CanEdit | MagicColumnKind::CanDelete => {
            Value::Null
        }
    }
}

fn magic_column_value(column: &str, provenance: &RowProvenance) -> Option<Value> {
    match magic_column_kind(column)? {
        MagicColumnKind::CreatedBy
        | MagicColumnKind::CreatedAt
        | MagicColumnKind::UpdatedBy
        | MagicColumnKind::UpdatedAt => {
            Some(provenance_value(magic_column_kind(column)?, provenance))
        }
        MagicColumnKind::CanRead | MagicColumnKind::CanEdit | MagicColumnKind::CanDelete => None,
    }
}

fn decode_policy_column_value(
    column: &str,
    provenance: &RowProvenance,
    content: &[u8],
    descriptor: &RowDescriptor,
) -> Option<Value> {
    if let Some(value) = magic_column_value(column, provenance) {
        return Some(value);
    }

    let col_index = descriptor.column_index(column)?;
    decode_column(descriptor, content, col_index).ok()
}

/// Evaluate a comparison expression. Public for use by PolicyFilterNode.
pub fn evaluate_cmp(
    column: &str,
    op: &CmpOp,
    value: &PolicyValue,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    if let Some(left) = magic_column_value(column, provenance) {
        let Some(right) = resolve_policy_value(value, session) else {
            return false;
        };
        return compare_values(&left, op, &right);
    }

    let col_index = match descriptor.column_index(column) {
        Some(idx) => idx,
        None => return false,
    };

    // Get the comparison value (either literal or from session)
    let cmp_value = match resolve_policy_value(value, session) {
        Some(v) => v,
        None => return false,
    };

    if cmp_value.is_null() {
        return match column_is_null(descriptor, content, col_index) {
            Ok(is_null) => match op {
                CmpOp::Eq => is_null,
                CmpOp::Ne => !is_null,
                CmpOp::Lt => false,
                CmpOp::Le => is_null,
                CmpOp::Gt => !is_null,
                CmpOp::Ge => true,
            },
            Err(_) => false,
        };
    }

    // Encode the comparison value to bytes
    let encoded = encode_value(&cmp_value);

    match op {
        CmpOp::Eq => match column_bytes(descriptor, content, col_index) {
            Ok(Some(bytes)) => bytes == encoded.as_slice(),
            _ => false,
        },
        CmpOp::Ne => match column_bytes(descriptor, content, col_index) {
            Ok(Some(bytes)) => bytes != encoded.as_slice(),
            Ok(None) => true, // null != value
            Err(_) => false,
        },
        CmpOp::Lt => {
            matches!(
                compare_column_to_value(descriptor, content, col_index, &encoded),
                Ok(std::cmp::Ordering::Less)
            )
        }
        CmpOp::Le => {
            matches!(
                compare_column_to_value(descriptor, content, col_index, &encoded),
                Ok(std::cmp::Ordering::Less) | Ok(std::cmp::Ordering::Equal)
            )
        }
        CmpOp::Gt => {
            matches!(
                compare_column_to_value(descriptor, content, col_index, &encoded),
                Ok(std::cmp::Ordering::Greater)
            )
        }
        CmpOp::Ge => {
            matches!(
                compare_column_to_value(descriptor, content, col_index, &encoded),
                Ok(std::cmp::Ordering::Greater) | Ok(std::cmp::Ordering::Equal)
            )
        }
    }
}

fn evaluate_session_cmp(path: &[String], op: &CmpOp, value: &Value, session: &Session) -> bool {
    let Some(session_value) = resolve_session_value(path, session) else {
        return false;
    };
    compare_values(&session_value, op, value)
}

fn compare_values(left: &Value, op: &CmpOp, right: &Value) -> bool {
    match op {
        CmpOp::Eq => left == right,
        CmpOp::Ne => left != right,
        CmpOp::Lt => compare_values_for_ordering(left, right)
            .map(|ordering| ordering == std::cmp::Ordering::Less)
            .unwrap_or(false),
        CmpOp::Le => compare_values_for_ordering(left, right)
            .map(|ordering| {
                matches!(
                    ordering,
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                )
            })
            .unwrap_or(false),
        CmpOp::Gt => compare_values_for_ordering(left, right)
            .map(|ordering| ordering == std::cmp::Ordering::Greater)
            .unwrap_or(false),
        CmpOp::Ge => compare_values_for_ordering(left, right)
            .map(|ordering| {
                matches!(
                    ordering,
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                )
            })
            .unwrap_or(false),
    }
}

fn compare_values_for_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::BigInt(a), Value::BigInt(b)) => Some(a.cmp(b)),
        (Value::Double(a), Value::Double(b)) => Some(a.total_cmp(b)),
        (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        (Value::Null, _) => Some(std::cmp::Ordering::Less),
        (_, Value::Null) => Some(std::cmp::Ordering::Greater),
        _ => None,
    }
}

fn resolve_policy_value(value: &PolicyValue, session: &Session) -> Option<Value> {
    match value {
        PolicyValue::Literal(v) => Some(v.clone()),
        PolicyValue::SessionRef(path) => resolve_session_value(path, session),
    }
}

/// Resolve an outer-row column reference to a literal `Value`.
///
/// Tries the named column first; if the descriptor has no column called `"id"`,
/// falls back to the row's `ObjectId` (UUID), which is stored separately from the
/// content bytes. Returns `None` on any failure.
fn resolve_outer_col(
    outer_col: &str,
    outer_content: &[u8],
    outer_descriptor: &RowDescriptor,
    outer_row_id: Option<ObjectId>,
) -> Option<Value> {
    if let Some(col_index) = outer_descriptor.column_index(outer_col) {
        decode_column(outer_descriptor, outer_content, col_index).ok()
    } else if outer_col == "id" {
        // `@session.__jazz_outer_row.id` refers to the row's ObjectId (UUID),
        // which is not a content column.
        Some(Value::Uuid(outer_row_id?))
    } else {
        None
    }
}

/// Bind outer-row references encoded as `@session.__jazz_outer_row.<column>` to literals.
///
/// `outer_row_id` must be supplied when the policy may reference
/// `@session.__jazz_outer_row.id` (the row's UUID primary key).
///
/// Returns `None` if a referenced outer column cannot be resolved.
pub fn bind_outer_row_refs(
    expr: &PolicyExpr,
    outer_content: &[u8],
    outer_descriptor: &RowDescriptor,
    outer_row_id: Option<ObjectId>,
) -> Option<PolicyExpr> {
    match expr {
        PolicyExpr::Cmp { column, op, value } => {
            let bound_value = match value {
                PolicyValue::Literal(v) => PolicyValue::Literal(v.clone()),
                PolicyValue::SessionRef(path) => {
                    if let Some(outer_col) = outer_row_ref_column(path) {
                        let resolved = resolve_outer_col(
                            outer_col,
                            outer_content,
                            outer_descriptor,
                            outer_row_id,
                        )?;
                        PolicyValue::Literal(resolved)
                    } else {
                        PolicyValue::SessionRef(path.clone())
                    }
                }
            };

            Some(PolicyExpr::Cmp {
                column: column.clone(),
                op: op.clone(),
                value: bound_value,
            })
        }
        PolicyExpr::SessionCmp { path, op, value } => Some(PolicyExpr::SessionCmp {
            path: path.clone(),
            op: op.clone(),
            value: value.clone(),
        }),
        PolicyExpr::IsNull { column } => Some(PolicyExpr::IsNull {
            column: column.clone(),
        }),
        PolicyExpr::SessionIsNull { path } => {
            Some(PolicyExpr::SessionIsNull { path: path.clone() })
        }
        PolicyExpr::IsNotNull { column } => Some(PolicyExpr::IsNotNull {
            column: column.clone(),
        }),
        PolicyExpr::SessionIsNotNull { path } => {
            Some(PolicyExpr::SessionIsNotNull { path: path.clone() })
        }
        PolicyExpr::Contains { column, value } => {
            let bound_value = match value {
                PolicyValue::Literal(v) => PolicyValue::Literal(v.clone()),
                PolicyValue::SessionRef(path) => {
                    if let Some(outer_col) = outer_row_ref_column(path) {
                        let resolved = resolve_outer_col(
                            outer_col,
                            outer_content,
                            outer_descriptor,
                            outer_row_id,
                        )?;
                        PolicyValue::Literal(resolved)
                    } else {
                        PolicyValue::SessionRef(path.clone())
                    }
                }
            };
            Some(PolicyExpr::Contains {
                column: column.clone(),
                value: bound_value,
            })
        }
        PolicyExpr::SessionContains { path, value } => Some(PolicyExpr::SessionContains {
            path: path.clone(),
            value: value.clone(),
        }),
        PolicyExpr::In {
            column,
            session_path,
        } => {
            if outer_row_ref_column(session_path).is_some() {
                return None;
            }
            Some(PolicyExpr::In {
                column: column.clone(),
                session_path: session_path.clone(),
            })
        }
        PolicyExpr::InList { column, values } => {
            let bound_values = values
                .iter()
                .map(|value| match value {
                    PolicyValue::Literal(v) => Some(PolicyValue::Literal(v.clone())),
                    PolicyValue::SessionRef(path) => {
                        if let Some(outer_col) = outer_row_ref_column(path) {
                            let resolved = resolve_outer_col(
                                outer_col,
                                outer_content,
                                outer_descriptor,
                                outer_row_id,
                            )?;
                            Some(PolicyValue::Literal(resolved))
                        } else {
                            Some(PolicyValue::SessionRef(path.clone()))
                        }
                    }
                })
                .collect::<Option<Vec<_>>>()?;
            Some(PolicyExpr::InList {
                column: column.clone(),
                values: bound_values,
            })
        }
        PolicyExpr::SessionInList { path, values } => Some(PolicyExpr::SessionInList {
            path: path.clone(),
            values: values.clone(),
        }),
        PolicyExpr::Exists { table, condition } => Some(PolicyExpr::Exists {
            table: table.clone(),
            // Keep nested EXISTS conditions unbound at this level so they can be
            // correlated against their immediate outer row when evaluated.
            condition: condition.clone(),
        }),
        PolicyExpr::ExistsRel { rel } => Some(PolicyExpr::ExistsRel { rel: rel.clone() }),
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => Some(PolicyExpr::Inherits {
            operation: *operation,
            via_column: via_column.clone(),
            max_depth: *max_depth,
        }),
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth,
        } => Some(PolicyExpr::InheritsReferencing {
            operation: *operation,
            source_table: source_table.clone(),
            via_column: via_column.clone(),
            max_depth: *max_depth,
        }),
        PolicyExpr::And(exprs) => Some(PolicyExpr::And(
            exprs
                .iter()
                .map(|expr| {
                    bind_outer_row_refs(expr, outer_content, outer_descriptor, outer_row_id)
                })
                .collect::<Option<Vec<_>>>()?,
        )),
        PolicyExpr::Or(exprs) => Some(PolicyExpr::Or(
            exprs
                .iter()
                .map(|expr| {
                    bind_outer_row_refs(expr, outer_content, outer_descriptor, outer_row_id)
                })
                .collect::<Option<Vec<_>>>()?,
        )),
        PolicyExpr::Not(expr) => Some(PolicyExpr::Not(Box::new(bind_outer_row_refs(
            expr,
            outer_content,
            outer_descriptor,
            outer_row_id,
        )?))),
        PolicyExpr::True => Some(PolicyExpr::True),
        PolicyExpr::False => Some(PolicyExpr::False),
    }
}

fn outer_row_ref_column(path: &[String]) -> Option<&str> {
    if path.len() != 2 {
        return None;
    }
    if path[0] != OUTER_ROW_SESSION_PREFIX {
        return None;
    }
    Some(path[1].as_str())
}

/// Bind relation references that depend on session or outer-row context.
///
/// Rewrites:
/// - `SessionRef(path)` => `Literal(resolve_session_value(path))`
/// - `OuterColumn(col)` => `Literal(outer_row[col])`
/// - `RowId(Outer)` => `Literal(outer_row_id)` when provided
///
/// Returns `None` on any unresolved reference.
pub fn bind_relation_refs(
    rel: &RelExpr,
    outer_content: &[u8],
    outer_descriptor: &RowDescriptor,
    session: &Session,
    outer_row_id: Option<ObjectId>,
) -> Option<RelExpr> {
    fn bind_value_ref(
        value_ref: &ValueRef,
        outer_content: &[u8],
        outer_descriptor: &RowDescriptor,
        session: &Session,
        outer_row_id: Option<ObjectId>,
    ) -> Option<ValueRef> {
        match value_ref {
            ValueRef::Literal(value) => Some(ValueRef::Literal(value.clone())),
            ValueRef::SessionRef(path) => {
                let resolved = resolve_session_value(path, session)?;
                Some(ValueRef::Literal(resolved))
            }
            ValueRef::OuterColumn(column) => {
                let col_index = outer_descriptor.column_index(&column.column)?;
                let resolved = decode_column(outer_descriptor, outer_content, col_index).ok()?;
                Some(ValueRef::Literal(resolved))
            }
            ValueRef::FrontierColumn(column) => Some(ValueRef::FrontierColumn(column.clone())),
            ValueRef::RowId(RowIdRef::Outer) => {
                let outer_id = outer_row_id?;
                Some(ValueRef::Literal(Value::Uuid(outer_id)))
            }
            ValueRef::RowId(source) => Some(ValueRef::RowId(*source)),
        }
    }

    fn bind_predicate(
        predicate: &PredicateExpr,
        outer_content: &[u8],
        outer_descriptor: &RowDescriptor,
        session: &Session,
        outer_row_id: Option<ObjectId>,
    ) -> Option<PredicateExpr> {
        match predicate {
            PredicateExpr::Cmp { left, op, right } => Some(PredicateExpr::Cmp {
                left: left.clone(),
                op: *op,
                right: bind_value_ref(
                    right,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?,
            }),
            PredicateExpr::Contains { left, right } => Some(PredicateExpr::Contains {
                left: left.clone(),
                right: bind_value_ref(
                    right,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?,
            }),
            PredicateExpr::IsNull { column } => Some(PredicateExpr::IsNull {
                column: column.clone(),
            }),
            PredicateExpr::IsNotNull { column } => Some(PredicateExpr::IsNotNull {
                column: column.clone(),
            }),
            PredicateExpr::In { left, values } => Some(PredicateExpr::In {
                left: left.clone(),
                values: values
                    .iter()
                    .map(|value| {
                        bind_value_ref(
                            value,
                            outer_content,
                            outer_descriptor,
                            session,
                            outer_row_id,
                        )
                    })
                    .collect::<Option<Vec<_>>>()?,
            }),
            PredicateExpr::And(exprs) => Some(PredicateExpr::And(
                exprs
                    .iter()
                    .map(|expr| {
                        bind_predicate(expr, outer_content, outer_descriptor, session, outer_row_id)
                    })
                    .collect::<Option<Vec<_>>>()?,
            )),
            PredicateExpr::Or(exprs) => Some(PredicateExpr::Or(
                exprs
                    .iter()
                    .map(|expr| {
                        bind_predicate(expr, outer_content, outer_descriptor, session, outer_row_id)
                    })
                    .collect::<Option<Vec<_>>>()?,
            )),
            PredicateExpr::Not(inner) => Some(PredicateExpr::Not(Box::new(bind_predicate(
                inner,
                outer_content,
                outer_descriptor,
                session,
                outer_row_id,
            )?))),
            PredicateExpr::True => Some(PredicateExpr::True),
            PredicateExpr::False => Some(PredicateExpr::False),
        }
    }

    fn bind_rel_expr(
        rel: &RelExpr,
        outer_content: &[u8],
        outer_descriptor: &RowDescriptor,
        session: &Session,
        outer_row_id: Option<ObjectId>,
    ) -> Option<RelExpr> {
        match rel {
            RelExpr::TableScan { table } => Some(RelExpr::TableScan { table: *table }),
            RelExpr::Filter { input, predicate } => Some(RelExpr::Filter {
                input: Box::new(bind_rel_expr(
                    input,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                predicate: bind_predicate(
                    predicate,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?,
            }),
            RelExpr::Join {
                left,
                right,
                on,
                join_kind,
            } => Some(RelExpr::Join {
                left: Box::new(bind_rel_expr(
                    left,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                right: Box::new(bind_rel_expr(
                    right,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                on: on.clone(),
                join_kind: *join_kind,
            }),
            RelExpr::Project { input, columns } => Some(RelExpr::Project {
                input: Box::new(bind_rel_expr(
                    input,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                columns: columns.clone(),
            }),
            RelExpr::Gather {
                seed,
                step,
                frontier_key,
                max_depth,
                dedupe_key,
            } => Some(RelExpr::Gather {
                seed: Box::new(bind_rel_expr(
                    seed,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                step: Box::new(bind_rel_expr(
                    step,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                frontier_key: frontier_key.clone(),
                max_depth: *max_depth,
                dedupe_key: dedupe_key.clone(),
            }),
            RelExpr::Distinct { input, key } => Some(RelExpr::Distinct {
                input: Box::new(bind_rel_expr(
                    input,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                key: key.clone(),
            }),
            RelExpr::OrderBy { input, terms } => Some(RelExpr::OrderBy {
                input: Box::new(bind_rel_expr(
                    input,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                terms: terms.clone(),
            }),
            RelExpr::Offset { input, offset } => Some(RelExpr::Offset {
                input: Box::new(bind_rel_expr(
                    input,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                offset: *offset,
            }),
            RelExpr::Limit { input, limit } => Some(RelExpr::Limit {
                input: Box::new(bind_rel_expr(
                    input,
                    outer_content,
                    outer_descriptor,
                    session,
                    outer_row_id,
                )?),
                limit: *limit,
            }),
        }
    }

    bind_rel_expr(rel, outer_content, outer_descriptor, session, outer_row_id)
}

/// Evaluate an IN expression. Public for use by PolicyFilterNode.
pub fn evaluate_in(
    column: &str,
    session_path: &[String],
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    // Get the column value
    let col_value = match decode_policy_column_value(column, provenance, content, descriptor) {
        Some(v) if !matches!(v, Value::Null) => v,
        _ => return false,
    };

    // Get the session array
    let session_array = match session.get_array(session_path) {
        Some(arr) => arr,
        None => return false,
    };

    // Check if the column value is in the session array
    match &col_value {
        Value::Text(s) => session_array.iter().any(|v| v.as_str() == Some(s.as_str())),
        Value::Integer(i) => session_array.iter().any(|v| v.as_i64() == Some(*i as i64)),
        Value::BigInt(i) => session_array.iter().any(|v| v.as_i64() == Some(*i)),
        Value::Uuid(id) => {
            let id_str = id.to_string();
            session_array.iter().any(|v| v.as_str() == Some(&id_str))
        }
        _ => false,
    }
}

/// Evaluate a CONTAINS expression.
pub fn evaluate_contains(
    column: &str,
    value: &PolicyValue,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    let right_value = match resolve_policy_value(value, session) {
        Some(v) => v,
        None => return false,
    };

    let column_value = match decode_policy_column_value(column, provenance, content, descriptor) {
        Some(v) => v,
        None => return false,
    };

    match column_value {
        Value::Array(elements) => elements.iter().any(|element| element == &right_value),
        Value::Text(text) => match right_value {
            Value::Text(substr) => text.contains(&substr),
            _ => false,
        },
        _ => false,
    }
}

/// Evaluate an IN-list expression.
pub fn evaluate_in_list(
    column: &str,
    values: &[PolicyValue],
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    if values.is_empty() {
        return false;
    }

    let column_value = match decode_policy_column_value(column, provenance, content, descriptor) {
        Some(v) if !matches!(v, Value::Null) => v,
        _ => return false,
    };

    values.iter().any(|candidate| {
        resolve_policy_value(candidate, session)
            .map(|resolved| resolved == column_value)
            .unwrap_or(false)
    })
}

fn evaluate_session_in_list(path: &[String], values: &[Value], session: &Session) -> bool {
    let Some(session_value) = resolve_session_value(path, session) else {
        return false;
    };
    if matches!(session_value, Value::Null) {
        return false;
    }

    values.iter().any(|candidate| candidate == &session_value)
}

/// Resolve a session path to a Value. Public for use by PolicyFilterNode.
pub fn resolve_session_value(path: &[String], session: &Session) -> Option<Value> {
    if path.is_empty() {
        return None;
    }

    if is_user_id_path(path) {
        return Some(Value::Text(session.user_id.clone()));
    }

    // For claims paths, convert JSON to Value
    let json_value = session.get_path(path)?;
    json_to_value(json_value)
}

fn is_user_id_path(path: &[String]) -> bool {
    matches!(path, [segment] if segment == "user_id" || segment == "userId")
}

fn json_to_value(json: &serde_json::Value) -> Option<Value> {
    match json {
        serde_json::Value::String(s) => Some(Value::Text(s.clone())),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Some(Value::Integer(i as i32))
                } else {
                    Some(Value::BigInt(i))
                }
            } else {
                None
            }
        }
        serde_json::Value::Bool(b) => Some(Value::Boolean(*b)),
        serde_json::Value::Null => Some(Value::Null),
        _ => None,
    }
}

fn evaluate_session_contains(path: &[String], value: &Value, session: &Session) -> bool {
    if let Some(text) = session.get_string(path) {
        return matches!(value, Value::Text(substr) if text.contains(substr));
    }

    let Some(json_value) = session.get_path(path) else {
        return false;
    };

    match json_value {
        serde_json::Value::Array(values) => values
            .iter()
            .any(|entry| json_to_value(entry).is_some_and(|candidate| candidate == *value)),
        serde_json::Value::String(text) => {
            matches!(value, Value::Text(substr) if text.contains(substr))
        }
        _ => false,
    }
}

// ============================================================================
// Simple parts evaluation for write permission checks
// ============================================================================

/// A complex clause that requires graph evaluation (INHERITS or EXISTS).
#[derive(Debug, Clone)]
pub enum ComplexClause {
    /// INHERITS clause - check parent row's policy.
    Inherits {
        operation: Operation,
        via_column: String,
        max_depth: Option<usize>,
    },
    /// INHERITS REFERENCING clause - check policies on source rows that reference this row.
    InheritsReferencing {
        operation: Operation,
        source_table: String,
        via_column: String,
        max_depth: Option<usize>,
    },
    /// EXISTS clause - check if subquery returns rows.
    Exists {
        table: String,
        condition: Box<PolicyExpr>,
    },
    /// EXISTS relation clause with declarative relation IR.
    ExistsRel { rel: RelExpr },
}

/// Result of evaluating simple parts of a policy expression.
#[derive(Debug)]
pub struct SimpleEvalResult {
    /// True if all simple parts passed.
    pub passed: bool,
    /// Complex clauses that need graph evaluation (only if passed=true).
    pub complex_clauses: Vec<ComplexClause>,
}

impl SimpleEvalResult {
    fn pass() -> Self {
        Self {
            passed: true,
            complex_clauses: vec![],
        }
    }

    fn fail() -> Self {
        Self {
            passed: false,
            complex_clauses: vec![],
        }
    }

    fn with_complex(clause: ComplexClause) -> Self {
        Self {
            passed: true,
            complex_clauses: vec![clause],
        }
    }
}

/// Evaluate simple parts of a policy expression against row content.
///
/// This function evaluates simple expressions (Cmp, IsNull, In, And, Or, Not)
/// synchronously by reading values from the row content. Complex expressions
/// (INHERITS, EXISTS) that require graph evaluation are collected and returned
/// for later async evaluation.
///
/// This enables a fast path: if simple parts fail, we can reject immediately
/// without creating policy graphs.
///
/// Returns:
/// - `passed: false` if any simple part fails (immediate rejection)
/// - `passed: true, complex_clauses: []` if all parts pass and no complex clauses
/// - `passed: true, complex_clauses: [...]` if simple parts pass but complex clauses need evaluation
pub fn evaluate_simple_parts(
    expr: &PolicyExpr,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    session: &Session,
) -> SimpleEvalResult {
    evaluate_simple_recursive(expr, content, provenance, descriptor, session, 0)
}

fn evaluate_simple_recursive(
    expr: &PolicyExpr,
    content: &[u8],
    provenance: &RowProvenance,
    descriptor: &RowDescriptor,
    session: &Session,
    depth: usize,
) -> SimpleEvalResult {
    // Prevent infinite recursion
    if depth > RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
        return SimpleEvalResult::fail();
    }

    match expr {
        PolicyExpr::True => SimpleEvalResult::pass(),
        PolicyExpr::False => SimpleEvalResult::fail(),

        PolicyExpr::Cmp { column, op, value } => {
            if evaluate_cmp(column, op, value, content, provenance, descriptor, session) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }
        PolicyExpr::SessionCmp { path, op, value } => {
            if evaluate_session_cmp(path, op, value, session) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::IsNull { column } => {
            let result = magic_column_value(column, provenance)
                .map(|value| value.is_null())
                .unwrap_or_else(|| {
                    descriptor
                        .column_index(column)
                        .map(|i| column_is_null(descriptor, content, i).unwrap_or(false))
                        .unwrap_or(false)
                });
            if result {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }
        PolicyExpr::SessionIsNull { path } => {
            if matches!(resolve_session_value(path, session), Some(Value::Null)) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::IsNotNull { column } => {
            let result = magic_column_value(column, provenance)
                .map(|value| !value.is_null())
                .unwrap_or_else(|| {
                    descriptor
                        .column_index(column)
                        .map(|i| !column_is_null(descriptor, content, i).unwrap_or(true))
                        .unwrap_or(false)
                });
            if result {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }
        PolicyExpr::SessionIsNotNull { path } => {
            if matches!(resolve_session_value(path, session), Some(value) if !value.is_null()) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::Contains { column, value } => {
            if evaluate_contains(column, value, content, provenance, descriptor, session) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }
        PolicyExpr::SessionContains { path, value } => {
            if evaluate_session_contains(path, value, session) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::In {
            column,
            session_path,
        } => {
            if evaluate_in(
                column,
                session_path,
                content,
                provenance,
                descriptor,
                session,
            ) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }
        PolicyExpr::SessionInList { path, values } => {
            if evaluate_session_in_list(path, values, session) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::InList { column, values } => {
            if evaluate_in_list(column, values, content, provenance, descriptor, session) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::And(exprs) => {
            let mut all_complex = Vec::new();
            for e in exprs {
                let result =
                    evaluate_simple_recursive(e, content, provenance, descriptor, session, depth);
                if !result.passed {
                    return SimpleEvalResult::fail();
                }
                all_complex.extend(result.complex_clauses);
            }
            SimpleEvalResult {
                passed: true,
                complex_clauses: all_complex,
            }
        }

        PolicyExpr::Or(exprs) => {
            // For OR, we need to find at least one branch that passes.
            // If a branch passes with no complex clauses, we're done (pass).
            // If all simple branches fail, we fail.
            // If some branches have complex clauses, we need to evaluate them.
            let mut has_simple_pass = false;
            let mut all_complex = Vec::new();

            for e in exprs {
                let result =
                    evaluate_simple_recursive(e, content, provenance, descriptor, session, depth);
                if result.passed && result.complex_clauses.is_empty() {
                    // Simple pass - entire OR passes
                    has_simple_pass = true;
                    break;
                } else if result.passed {
                    // Has complex clauses - collect them
                    all_complex.extend(result.complex_clauses);
                }
                // If !result.passed, this branch fails, try next
            }

            if has_simple_pass {
                SimpleEvalResult::pass()
            } else if !all_complex.is_empty() {
                // Some branches have complex clauses that might pass
                SimpleEvalResult {
                    passed: true,
                    complex_clauses: all_complex,
                }
            } else {
                // All branches failed
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::Not(inner) => {
            let result =
                evaluate_simple_recursive(inner, content, provenance, descriptor, session, depth);
            if !result.complex_clauses.is_empty() {
                // NOT of complex clause is complex - can't evaluate simply
                // Return as needing graph evaluation
                // Note: This is conservative - we could be smarter here
                SimpleEvalResult {
                    passed: true,
                    complex_clauses: result.complex_clauses,
                }
            } else if result.passed {
                SimpleEvalResult::fail()
            } else {
                SimpleEvalResult::pass()
            }
        }

        // Complex clauses - collect for graph evaluation
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => SimpleEvalResult::with_complex(ComplexClause::Inherits {
            operation: *operation,
            via_column: via_column.clone(),
            max_depth: *max_depth,
        }),
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth,
        } => SimpleEvalResult::with_complex(ComplexClause::InheritsReferencing {
            operation: *operation,
            source_table: source_table.clone(),
            via_column: via_column.clone(),
            max_depth: *max_depth,
        }),

        PolicyExpr::Exists { table, condition } => {
            let bound = match bind_outer_row_refs(condition, content, descriptor, None) {
                Some(expr) => expr,
                None => return SimpleEvalResult::fail(),
            };

            SimpleEvalResult::with_complex(ComplexClause::Exists {
                table: table.clone(),
                condition: Box::new(bound),
            })
        }
        PolicyExpr::ExistsRel { rel } => {
            let bound = match bind_relation_refs(rel, content, descriptor, session, None) {
                Some(expr) => expr,
                None => return SimpleEvalResult::fail(),
            };
            SimpleEvalResult::with_complex(ComplexClause::ExistsRel { rel: bound })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_row_provenance() -> RowProvenance {
        RowProvenance::for_insert("jazz:test", 0)
    }

    fn evaluate_policy_expr(
        expr: &PolicyExpr,
        content: &[u8],
        descriptor: &RowDescriptor,
        session: &Session,
    ) -> bool {
        super::evaluate_policy_expr(expr, content, &test_row_provenance(), descriptor, session)
    }

    fn evaluate_simple_parts(
        expr: &PolicyExpr,
        content: &[u8],
        descriptor: &RowDescriptor,
        session: &Session,
    ) -> SimpleEvalResult {
        super::evaluate_simple_parts(expr, content, &test_row_provenance(), descriptor, session)
    }

    #[test]
    fn test_policy_expr_builders() {
        // eq_session
        let expr = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
        assert!(matches!(
            expr,
            PolicyExpr::Cmp {
                column,
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(path)
            } if column == "owner_id" && path == vec!["user_id"]
        ));

        // eq_literal
        let expr = PolicyExpr::eq_literal("status", Value::Text("active".into()));
        assert!(matches!(
            expr,
            PolicyExpr::Cmp {
                column,
                op: CmpOp::Eq,
                value: PolicyValue::Literal(Value::Text(s))
            } if column == "status" && s == "active"
        ));

        // in_session
        let expr = PolicyExpr::in_session("team_id", vec!["claims".into(), "teams".into()]);
        assert!(matches!(
            expr,
            PolicyExpr::In { column, session_path }
            if column == "team_id" && session_path == vec!["claims", "teams"]
        ));

        // inherits
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
        // Empty AND is True
        assert_eq!(PolicyExpr::and(vec![]), PolicyExpr::True);

        // Single-element AND unwraps
        let inner = PolicyExpr::True;
        assert_eq!(PolicyExpr::and(vec![inner.clone()]), inner);

        // Empty OR is False
        assert_eq!(PolicyExpr::or(vec![]), PolicyExpr::False);

        // Single-element OR unwraps
        assert_eq!(PolicyExpr::or(vec![inner.clone()]), inner);

        // Multi-element AND/OR wraps
        let and_expr = PolicyExpr::and(vec![PolicyExpr::True, PolicyExpr::False]);
        assert!(matches!(and_expr, PolicyExpr::And(v) if v.len() == 2));

        let or_expr = PolicyExpr::or(vec![PolicyExpr::True, PolicyExpr::False]);
        assert!(matches!(or_expr, PolicyExpr::Or(v) if v.len() == 2));
    }

    #[test]
    fn test_exists_outer_row_refs_are_bound_to_literals() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Text),
            ColumnDescriptor::new("owner_id", ColumnType::Text),
        ]);
        let content = encode_row(
            &descriptor,
            &[Value::Text("todo-1".into()), Value::Text("user-1".into())],
        )
        .unwrap();
        let session = Session::new("user-1");

        let expr = PolicyExpr::Exists {
            table: "todo_shares".into(),
            condition: Box::new(PolicyExpr::Cmp {
                column: "todo_id".into(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec![OUTER_ROW_SESSION_PREFIX.into(), "id".into()]),
            }),
        };

        let result = evaluate_simple_parts(&expr, &content, &descriptor, &session);
        assert!(result.passed);
        assert_eq!(result.complex_clauses.len(), 1);

        match &result.complex_clauses[0] {
            ComplexClause::Exists { table, condition } => {
                assert_eq!(table, "todo_shares");
                assert!(matches!(
                    condition.as_ref(),
                    PolicyExpr::Cmp {
                        column,
                        op: CmpOp::Eq,
                        value: PolicyValue::Literal(Value::Text(value))
                    } if column == "todo_id" && value == "todo-1"
                ));
            }
            _ => panic!("expected EXISTS complex clause"),
        }
    }

    #[test]
    fn test_exists_outer_row_ref_to_missing_column_fails() {
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new("id", ColumnType::Text)]);
        let content = encode_row(&descriptor, &[Value::Text("todo-1".into())]).unwrap();
        let session = Session::new("user-1");

        let expr = PolicyExpr::Exists {
            table: "todo_shares".into(),
            condition: Box::new(PolicyExpr::Cmp {
                column: "todo_id".into(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec![
                    OUTER_ROW_SESSION_PREFIX.into(),
                    "missing_col".into(),
                ]),
            }),
        };

        let result = evaluate_simple_parts(&expr, &content, &descriptor, &session);
        assert!(!result.passed);
    }

    #[test]
    fn test_bind_outer_row_refs_id_resolves_to_object_id_when_not_a_column() {
        // Regression test: `@session.__jazz_outer_row.id` must resolve to the row's
        // ObjectId (UUID) even when "id" is not a named column in the descriptor.
        // This is the common case — e.g. `EXISTS (SELECT FROM chatMembers WHERE chat =
        // @session.__jazz_outer_row.id AND userId = @session.user_id)`.
        use crate::object::ObjectId;

        let descriptor =
            RowDescriptor::new(vec![ColumnDescriptor::new("owner_id", ColumnType::Text)]);
        let content = encode_row(&descriptor, &[Value::Text("user-1".into())]).unwrap();

        let row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(
            0xdead_beef_cafe_0000_0000_0000_0000_0001,
        ));

        let expr = PolicyExpr::Cmp {
            column: "chat".into(),
            op: CmpOp::Eq,
            value: PolicyValue::SessionRef(vec![OUTER_ROW_SESSION_PREFIX.into(), "id".into()]),
        };

        // Without a row_id: "id" is not a column, so binding must fail.
        assert!(
            bind_outer_row_refs(&expr, &content, &descriptor, None).is_none(),
            "should fail when id is not a column and no row_id is provided"
        );

        // With a row_id: should resolve to the UUID literal.
        let bound = bind_outer_row_refs(&expr, &content, &descriptor, Some(row_id));
        assert!(
            matches!(
                &bound,
                Some(PolicyExpr::Cmp {
                    value: PolicyValue::Literal(Value::Uuid(id)),
                    ..
                }) if *id == row_id
            ),
            "expected @session.__jazz_outer_row.id to resolve to Value::Uuid(row_id), got {:?}",
            bound
        );
    }

    #[test]
    fn test_exists_rel_outer_column_binds_to_literal() {
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new("id", ColumnType::Text)]);
        let content = encode_row(&descriptor, &[Value::Text("todo-1".into())]).unwrap();
        let session = Session::new("user-1");

        let expr = PolicyExpr::ExistsRel {
            rel: RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("todo_shares"),
                }),
                predicate: PredicateExpr::Cmp {
                    left: crate::query_manager::relation_ir::ColumnRef::unscoped("todo_id"),
                    op: crate::query_manager::relation_ir::PredicateCmpOp::Eq,
                    right: ValueRef::OuterColumn(
                        crate::query_manager::relation_ir::ColumnRef::unscoped("id"),
                    ),
                },
            },
        };

        let result = evaluate_simple_parts(&expr, &content, &descriptor, &session);
        assert!(result.passed);
        assert_eq!(result.complex_clauses.len(), 1);
        match &result.complex_clauses[0] {
            ComplexClause::ExistsRel { rel } => {
                let RelExpr::Filter { predicate, .. } = rel else {
                    panic!("expected bound filter relation")
                };
                assert!(matches!(
                    predicate,
                    PredicateExpr::Cmp {
                        left,
                        op: crate::query_manager::relation_ir::PredicateCmpOp::Eq,
                        right: ValueRef::Literal(Value::Text(value)),
                    } if left.column == "todo_id" && value == "todo-1"
                ));
            }
            _ => panic!("expected ExistsRel complex clause"),
        }
    }

    #[test]
    fn test_nested_exists_outer_refs_bind_per_level() {
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new("id", ColumnType::Text)]);
        let content = encode_row(&descriptor, &[Value::Text("todo-1".into())]).unwrap();
        let session = Session::new("user-1");

        let expr = PolicyExpr::Exists {
            table: "todo_shares".into(),
            condition: Box::new(PolicyExpr::And(vec![
                PolicyExpr::Cmp {
                    column: "todo_id".into(),
                    op: CmpOp::Eq,
                    value: PolicyValue::SessionRef(vec![
                        OUTER_ROW_SESSION_PREFIX.into(),
                        "id".into(),
                    ]),
                },
                PolicyExpr::Exists {
                    table: "team_edges".into(),
                    condition: Box::new(PolicyExpr::Cmp {
                        column: "child_team".into(),
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(vec![
                            OUTER_ROW_SESSION_PREFIX.into(),
                            "team_id".into(),
                        ]),
                    }),
                },
            ])),
        };

        let result = evaluate_simple_parts(&expr, &content, &descriptor, &session);
        assert!(result.passed);
        assert_eq!(result.complex_clauses.len(), 1);

        match &result.complex_clauses[0] {
            ComplexClause::Exists { condition, .. } => {
                let PolicyExpr::And(exprs) = condition.as_ref() else {
                    panic!("expected bound EXISTS condition to be an AND");
                };
                assert!(matches!(
                    &exprs[0],
                    PolicyExpr::Cmp {
                        column,
                        op: CmpOp::Eq,
                        value: PolicyValue::Literal(Value::Text(v))
                    } if column == "todo_id" && v == "todo-1"
                ));

                assert!(matches!(
                    &exprs[1],
                    PolicyExpr::Exists { condition, .. }
                        if matches!(
                            condition.as_ref(),
                            PolicyExpr::Cmp {
                                column,
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(path),
                            } if column == "child_team"
                                && path == &vec![
                                    OUTER_ROW_SESSION_PREFIX.to_string(),
                                    "team_id".to_string()
                                ]
                        )
                ));
            }
            _ => panic!("expected EXISTS complex clause"),
        }
    }

    // ========================================================================
    // evaluate_simple_parts tests
    // ========================================================================

    use crate::query_manager::encoding::encode_row;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("owner_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Text),
            ColumnDescriptor::new("status", ColumnType::Text),
        ])
    }

    fn make_row_content(owner: &str, team: &str, status: &str) -> Vec<u8> {
        let desc = test_descriptor();
        encode_row(
            &desc,
            &[
                Value::Text(owner.into()),
                Value::Text(team.into()),
                Value::Text(status.into()),
            ],
        )
        .unwrap()
    }

    fn nullable_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("owner_id", ColumnType::Text),
            ColumnDescriptor::new("deleted_at", ColumnType::Text).nullable(),
        ])
    }

    fn make_nullable_row_content(owner: &str, deleted_at: Option<&str>) -> Vec<u8> {
        let desc = nullable_descriptor();
        encode_row(
            &desc,
            &[
                Value::Text(owner.into()),
                match deleted_at {
                    Some(value) => Value::Text(value.into()),
                    None => Value::Null,
                },
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_simple_parts_true_false() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1");

        let result = evaluate_simple_parts(&PolicyExpr::True, &content, &desc, &session);
        assert!(result.passed);
        assert!(result.complex_clauses.is_empty());

        let result = evaluate_simple_parts(&PolicyExpr::False, &content, &desc, &session);
        assert!(!result.passed);
    }

    #[test]
    fn test_simple_parts_eq_session() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1");

        // owner_id = @session.user_id (should pass)
        let expr = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(result.passed);
        assert!(result.complex_clauses.is_empty());

        // Different owner (should fail)
        let content2 = make_row_content("user2", "eng", "active");
        let result = evaluate_simple_parts(&expr, &content2, &desc, &session);
        assert!(!result.passed);
    }

    #[test]
    fn test_row_null_literal_comparisons_use_value_level_semantics() {
        let desc = nullable_descriptor();
        let session = Session::new("user1");
        let null_content = make_nullable_row_content("user1", None);
        let non_null_content = make_nullable_row_content("user1", Some("2026-03-30T12:00:00Z"));

        let eq_null = PolicyExpr::eq_literal("deleted_at", Value::Null);
        let eq_simple = evaluate_simple_parts(&eq_null, &null_content, &desc, &session);
        assert!(eq_simple.passed);
        assert!(eq_simple.complex_clauses.is_empty());
        assert!(evaluate_policy_expr(
            &eq_null,
            &null_content,
            &desc,
            &session
        ));
        assert!(!evaluate_policy_expr(
            &eq_null,
            &non_null_content,
            &desc,
            &session,
        ));

        let ne_null = PolicyExpr::Cmp {
            column: "deleted_at".into(),
            op: CmpOp::Ne,
            value: PolicyValue::Literal(Value::Null),
        };
        assert!(!evaluate_policy_expr(
            &ne_null,
            &null_content,
            &desc,
            &session
        ));
        assert!(evaluate_policy_expr(
            &ne_null,
            &non_null_content,
            &desc,
            &session,
        ));

        let lt_null = PolicyExpr::Cmp {
            column: "deleted_at".into(),
            op: CmpOp::Lt,
            value: PolicyValue::Literal(Value::Null),
        };
        assert!(!evaluate_policy_expr(
            &lt_null,
            &null_content,
            &desc,
            &session
        ));
        assert!(!evaluate_policy_expr(
            &lt_null,
            &non_null_content,
            &desc,
            &session,
        ));

        let le_null = PolicyExpr::Cmp {
            column: "deleted_at".into(),
            op: CmpOp::Le,
            value: PolicyValue::Literal(Value::Null),
        };
        assert!(evaluate_policy_expr(
            &le_null,
            &null_content,
            &desc,
            &session
        ));
        assert!(!evaluate_policy_expr(
            &le_null,
            &non_null_content,
            &desc,
            &session,
        ));

        let gt_null = PolicyExpr::Cmp {
            column: "deleted_at".into(),
            op: CmpOp::Gt,
            value: PolicyValue::Literal(Value::Null),
        };
        assert!(!evaluate_policy_expr(
            &gt_null,
            &null_content,
            &desc,
            &session
        ));
        assert!(evaluate_policy_expr(
            &gt_null,
            &non_null_content,
            &desc,
            &session,
        ));

        let ge_null = PolicyExpr::Cmp {
            column: "deleted_at".into(),
            op: CmpOp::Ge,
            value: PolicyValue::Literal(Value::Null),
        };
        assert!(evaluate_policy_expr(
            &ge_null,
            &null_content,
            &desc,
            &session
        ));
        assert!(evaluate_policy_expr(
            &ge_null,
            &non_null_content,
            &desc,
            &session,
        ));
    }

    #[test]
    fn test_row_explicit_null_checks() {
        let desc = nullable_descriptor();
        let session = Session::new("user1");
        let null_content = make_nullable_row_content("user1", None);
        let non_null_content = make_nullable_row_content("user1", Some("2026-03-30T12:00:00Z"));

        let is_null = PolicyExpr::IsNull {
            column: "deleted_at".into(),
        };
        assert!(evaluate_policy_expr(
            &is_null,
            &null_content,
            &desc,
            &session
        ));
        assert!(!evaluate_policy_expr(
            &is_null,
            &non_null_content,
            &desc,
            &session,
        ));

        let is_not_null = PolicyExpr::IsNotNull {
            column: "deleted_at".into(),
        };
        assert!(!evaluate_policy_expr(
            &is_not_null,
            &null_content,
            &desc,
            &session,
        ));
        assert!(evaluate_policy_expr(
            &is_not_null,
            &non_null_content,
            &desc,
            &session,
        ));
    }

    #[test]
    fn test_simple_parts_contains_text_and_array() {
        let desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new(
                "tags",
                ColumnType::Array {
                    element: Box::new(ColumnType::Text),
                },
            ),
        ]);
        let content = encode_row(
            &desc,
            &[
                Value::Text("hello world".into()),
                Value::Array(vec![
                    Value::Text("admin".into()),
                    Value::Text("editor".into()),
                ]),
            ],
        )
        .unwrap();
        let session = Session::new("user1");

        let title_contains = PolicyExpr::Contains {
            column: "title".into(),
            value: PolicyValue::Literal(Value::Text("world".into())),
        };
        let result = evaluate_simple_parts(&title_contains, &content, &desc, &session);
        assert!(result.passed);

        let tags_contains = PolicyExpr::Contains {
            column: "tags".into(),
            value: PolicyValue::Literal(Value::Text("admin".into())),
        };
        let result = evaluate_simple_parts(&tags_contains, &content, &desc, &session);
        assert!(result.passed);

        let missing = PolicyExpr::Contains {
            column: "tags".into(),
            value: PolicyValue::Literal(Value::Text("viewer".into())),
        };
        let result = evaluate_simple_parts(&missing, &content, &desc, &session);
        assert!(!result.passed);
    }

    #[test]
    fn test_simple_parts_in_list_literals_and_session_refs() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1");

        let literal_in_list = PolicyExpr::InList {
            column: "status".into(),
            values: vec![
                PolicyValue::Literal(Value::Text("inactive".into())),
                PolicyValue::Literal(Value::Text("active".into())),
            ],
        };
        let result = evaluate_simple_parts(&literal_in_list, &content, &desc, &session);
        assert!(result.passed);

        let session_ref_in_list = PolicyExpr::InList {
            column: "owner_id".into(),
            values: vec![PolicyValue::SessionRef(vec!["user_id".into()])],
        };
        let result = evaluate_simple_parts(&session_ref_in_list, &content, &desc, &session);
        assert!(result.passed);

        let empty_in_list = PolicyExpr::InList {
            column: "owner_id".into(),
            values: vec![],
        };
        let result = evaluate_simple_parts(&empty_in_list, &content, &desc, &session);
        assert!(!result.passed);
    }

    #[test]
    fn test_session_left_scalar_comparisons_and_identity_aliases() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1").with_claims(json!({
            "role": "manager",
            "login_count": 7,
            "beta": true,
        }));

        assert!(evaluate_policy_expr(
            &PolicyExpr::SessionCmp {
                path: vec!["claims".into(), "role".into()],
                op: CmpOp::Eq,
                value: Value::Text("manager".into()),
            },
            &content,
            &desc,
            &session,
        ));
        assert!(evaluate_policy_expr(
            &PolicyExpr::SessionCmp {
                path: vec!["claims".into(), "login_count".into()],
                op: CmpOp::Gt,
                value: Value::Integer(3),
            },
            &content,
            &desc,
            &session,
        ));
        assert!(evaluate_policy_expr(
            &PolicyExpr::SessionCmp {
                path: vec!["claims".into(), "beta".into()],
                op: CmpOp::Eq,
                value: Value::Boolean(true),
            },
            &content,
            &desc,
            &session,
        ));
        assert!(evaluate_policy_expr(
            &PolicyExpr::SessionCmp {
                path: vec!["user_id".into()],
                op: CmpOp::Eq,
                value: Value::Text("user1".into()),
            },
            &content,
            &desc,
            &session,
        ));
        assert!(evaluate_policy_expr(
            &PolicyExpr::SessionCmp {
                path: vec!["userId".into()],
                op: CmpOp::Eq,
                value: Value::Text("user1".into()),
            },
            &content,
            &desc,
            &session,
        ));
        assert!(!evaluate_policy_expr(
            &PolicyExpr::SessionCmp {
                path: vec!["claims".into(), "role".into()],
                op: CmpOp::Eq,
                value: Value::Text("viewer".into()),
            },
            &content,
            &desc,
            &session,
        ));
    }

    #[test]
    fn test_session_left_in_list_contains_and_null_semantics() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1").with_claims(json!({
            "plan": "pro",
            "teamIds": ["team_a", "team_b"],
            "bio": "manager for the platform team",
            "deleted_at": null,
        }));

        assert!(evaluate_policy_expr(
            &PolicyExpr::SessionInList {
                path: vec!["claims".into(), "plan".into()],
                values: vec![
                    Value::Text("free".into()),
                    Value::Text("pro".into()),
                    Value::Text("enterprise".into()),
                ],
            },
            &content,
            &desc,
            &session,
        ));
        assert!(evaluate_policy_expr(
            &PolicyExpr::SessionContains {
                path: vec!["claims".into(), "teamIds".into()],
                value: Value::Text("team_a".into()),
            },
            &content,
            &desc,
            &session,
        ));
        assert!(evaluate_policy_expr(
            &PolicyExpr::SessionContains {
                path: vec!["claims".into(), "bio".into()],
                value: Value::Text("platform".into()),
            },
            &content,
            &desc,
            &session,
        ));
        assert!(evaluate_policy_expr(
            &PolicyExpr::SessionIsNull {
                path: vec!["claims".into(), "deleted_at".into()],
            },
            &content,
            &desc,
            &session,
        ));
        assert!(!evaluate_policy_expr(
            &PolicyExpr::SessionInList {
                path: vec!["claims".into(), "deleted_at".into()],
                values: vec![Value::Null, Value::Text("fallback".into())],
            },
            &content,
            &desc,
            &session,
        ));
        assert!(!evaluate_policy_expr(
            &PolicyExpr::SessionIsNull {
                path: vec!["claims".into(), "missing".into()],
            },
            &content,
            &desc,
            &session,
        ));
        assert!(!evaluate_policy_expr(
            &PolicyExpr::SessionContains {
                path: vec!["claims".into(), "teamIds".into()],
                value: Value::Text("team_c".into()),
            },
            &content,
            &desc,
            &session,
        ));
    }

    #[test]
    fn test_exists_outer_row_refs_bind_for_contains_and_in_list() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Text),
            ColumnDescriptor::new("owner_id", ColumnType::Text),
        ]);
        let content = encode_row(
            &descriptor,
            &[Value::Text("todo-1".into()), Value::Text("user-1".into())],
        )
        .unwrap();
        let session = Session::new("user-1");

        let expr = PolicyExpr::Exists {
            table: "todo_shares".into(),
            condition: Box::new(PolicyExpr::And(vec![
                PolicyExpr::Contains {
                    column: "todo_id".into(),
                    value: PolicyValue::SessionRef(vec![
                        OUTER_ROW_SESSION_PREFIX.into(),
                        "id".into(),
                    ]),
                },
                PolicyExpr::InList {
                    column: "user_id".into(),
                    values: vec![
                        PolicyValue::SessionRef(vec![
                            OUTER_ROW_SESSION_PREFIX.into(),
                            "owner_id".into(),
                        ]),
                        PolicyValue::Literal(Value::Text("fallback".into())),
                    ],
                },
            ])),
        };

        let result = evaluate_simple_parts(&expr, &content, &descriptor, &session);
        assert!(result.passed);
        assert_eq!(result.complex_clauses.len(), 1);
        let ComplexClause::Exists { condition, .. } = &result.complex_clauses[0] else {
            panic!("expected EXISTS complex clause");
        };
        let PolicyExpr::And(exprs) = condition.as_ref() else {
            panic!("expected bound EXISTS condition to be an AND");
        };
        assert!(matches!(
            &exprs[0],
            PolicyExpr::Contains {
                column,
                value: PolicyValue::Literal(Value::Text(value))
            } if column == "todo_id" && value == "todo-1"
        ));
        assert!(matches!(
            &exprs[1],
            PolicyExpr::InList { column, values }
                if column == "user_id"
                    && values
                        == &vec![
                            PolicyValue::Literal(Value::Text("user-1".into())),
                            PolicyValue::Literal(Value::Text("fallback".into())),
                        ]
        ));
    }

    #[test]
    fn test_simple_parts_and() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1");

        // owner_id = @session.user_id AND status = 'active' (both pass)
        let expr = PolicyExpr::and(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            PolicyExpr::eq_literal("status", Value::Text("active".into())),
        ]);
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(result.passed);

        // owner_id = @session.user_id AND status = 'inactive' (second fails)
        let expr = PolicyExpr::and(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            PolicyExpr::eq_literal("status", Value::Text("inactive".into())),
        ]);
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(!result.passed);
    }

    #[test]
    fn test_simple_parts_treats_session_left_predicates_as_simple_clauses() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1").with_claims(json!({
            "role": "manager",
        }));

        let expr = PolicyExpr::And(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            PolicyExpr::SessionCmp {
                path: vec!["claims".into(), "role".into()],
                op: CmpOp::Eq,
                value: Value::Text("manager".into()),
            },
        ]);
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(result.passed);
        assert!(result.complex_clauses.is_empty());

        let denied = evaluate_simple_parts(
            &expr,
            &make_row_content("user2", "eng", "active"),
            &desc,
            &session,
        );
        assert!(!denied.passed);
        assert!(denied.complex_clauses.is_empty());
    }

    #[test]
    fn test_simple_parts_or() {
        let desc = test_descriptor();
        let content = make_row_content("user2", "eng", "active");
        let session = Session::new("user1");

        // owner_id = @session.user_id OR status = 'active' (first fails, second passes)
        let expr = PolicyExpr::or(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            PolicyExpr::eq_literal("status", Value::Text("active".into())),
        ]);
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(result.passed);

        // owner_id = @session.user_id OR status = 'inactive' (both fail)
        let expr = PolicyExpr::or(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            PolicyExpr::eq_literal("status", Value::Text("inactive".into())),
        ]);
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(!result.passed);
    }

    #[test]
    fn test_simple_parts_inherits_collected() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1");

        // INHERITS SELECT VIA parent_id
        let expr = PolicyExpr::inherits(Operation::Select, "parent_id");
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(result.passed); // Simple part passes (returns as needing eval)
        assert_eq!(result.complex_clauses.len(), 1);
        assert!(matches!(
            &result.complex_clauses[0],
            ComplexClause::Inherits {
                operation: Operation::Select,
                via_column,
                max_depth: None,
            } if via_column == "parent_id"
        ));
    }

    #[test]
    fn test_simple_parts_and_with_inherits() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1");

        // owner_id = @session.user_id AND INHERITS SELECT VIA parent_id
        let expr = PolicyExpr::and(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            PolicyExpr::inherits(Operation::Select, "parent_id"),
        ]);
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(result.passed);
        assert_eq!(result.complex_clauses.len(), 1);

        // Different owner AND INHERITS (simple part fails immediately)
        let content2 = make_row_content("user2", "eng", "active");
        let result = evaluate_simple_parts(&expr, &content2, &desc, &session);
        assert!(!result.passed);
        assert!(result.complex_clauses.is_empty()); // No need to check INHERITS
    }

    #[test]
    fn test_simple_parts_not() {
        let desc = test_descriptor();
        let content = make_row_content("user1", "eng", "active");
        let session = Session::new("user1");

        // NOT (status = 'inactive') should pass
        let expr = PolicyExpr::not(PolicyExpr::eq_literal(
            "status",
            Value::Text("inactive".into()),
        ));
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(result.passed);

        // NOT (status = 'active') should fail
        let expr = PolicyExpr::not(PolicyExpr::eq_literal(
            "status",
            Value::Text("active".into()),
        ));
        let result = evaluate_simple_parts(&expr, &content, &desc, &session);
        assert!(!result.passed);
    }
}
