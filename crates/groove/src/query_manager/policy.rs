//! Policy expressions for row-level security (ReBAC).
//!
//! Policies control access to rows based on session context and relationships
//! between rows in the database. Each operation (SELECT, INSERT, UPDATE, DELETE)
//! can have its own policy expression.

use super::encoding::{
    column_bytes, column_is_null, compare_column_to_value, decode_column, encode_value,
};
use super::session::Session;
use super::types::{RowDescriptor, Value};

/// Comparison operators for policy expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A value in a policy expression - either a literal or a session reference.
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyValue {
    /// A literal value.
    Literal(Value),
    /// Reference to a session variable, e.g., ["user_id"] or ["claims", "teams"].
    SessionRef(Vec<String>),
}

/// Database operation type for policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyExpr {
    /// Compare a column value against a policy value.
    Cmp {
        column: String,
        op: CmpOp,
        value: PolicyValue,
    },

    /// Check if a column is NULL.
    IsNull { column: String },

    /// Check if a column is NOT NULL.
    IsNotNull { column: String },

    /// Check if a column value is in a session array.
    /// The session_path must point to an array in the session claims.
    In {
        column: String,
        session_path: Vec<String>,
    },

    /// Check if a subquery returns any rows.
    /// Used internally for INHERITS transformation.
    Exists {
        table: String,
        condition: Box<PolicyExpr>,
    },

    /// Inherit permission from a related row.
    /// Looks up the row referenced by `via_column` (foreign key) and checks
    /// if that row passes the specified operation's policy.
    Inherits {
        operation: Operation,
        via_column: String,
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
    descriptor: &RowDescriptor,
    ctx: &mut EvalContext<F>,
) -> bool
where
    F: FnMut(ObjectId) -> Option<Vec<u8>>,
{
    evaluate_recursive(expr, content, descriptor, ctx, 0)
}

fn evaluate_recursive<F>(
    expr: &PolicyExpr,
    content: &[u8],
    descriptor: &RowDescriptor,
    ctx: &mut EvalContext<F>,
    depth: usize,
) -> bool
where
    F: FnMut(ObjectId) -> Option<Vec<u8>>,
{
    // Prevent infinite recursion
    if depth > 32 {
        return false;
    }

    match expr {
        PolicyExpr::True => true,
        PolicyExpr::False => false,

        PolicyExpr::Cmp { column, op, value } => {
            evaluate_cmp(column, op, value, content, descriptor, ctx.session)
        }

        PolicyExpr::IsNull { column } => {
            if let Some(col_index) = descriptor.column_index(column) {
                column_is_null(descriptor, content, col_index).unwrap_or(false)
            } else {
                false
            }
        }

        PolicyExpr::IsNotNull { column } => {
            if let Some(col_index) = descriptor.column_index(column) {
                !column_is_null(descriptor, content, col_index).unwrap_or(true)
            } else {
                false
            }
        }

        PolicyExpr::In {
            column,
            session_path,
        } => evaluate_in(column, session_path, content, descriptor, ctx.session),

        PolicyExpr::And(exprs) => exprs
            .iter()
            .all(|e| evaluate_recursive(e, content, descriptor, ctx, depth)),

        PolicyExpr::Or(exprs) => exprs
            .iter()
            .any(|e| evaluate_recursive(e, content, descriptor, ctx, depth)),

        PolicyExpr::Not(inner) => !evaluate_recursive(inner, content, descriptor, ctx, depth),

        PolicyExpr::Exists { .. } => {
            // EXISTS is an internal representation, not directly used
            true
        }

        PolicyExpr::Inherits {
            operation,
            via_column,
        } => evaluate_inherits(*operation, via_column, content, descriptor, ctx, depth),
    }
}

/// Evaluate INHERITS by loading the parent row and checking its policy.
fn evaluate_inherits<F>(
    operation: Operation,
    via_column: &str,
    content: &[u8],
    descriptor: &RowDescriptor,
    ctx: &mut EvalContext<F>,
    depth: usize,
) -> bool
where
    F: FnMut(ObjectId) -> Option<Vec<u8>>,
{
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
        &parent_schema.descriptor,
        ctx,
        depth + 1,
    )
}

/// Simple evaluation without INHERITS support (for backwards compatibility).
/// INHERITS expressions return true (permissive).
pub fn evaluate_policy_expr(
    expr: &PolicyExpr,
    content: &[u8],
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    evaluate_expr_simple(expr, content, descriptor, session, 0)
}

fn evaluate_expr_simple(
    expr: &PolicyExpr,
    content: &[u8],
    descriptor: &RowDescriptor,
    session: &Session,
    depth: usize,
) -> bool {
    if depth > 32 {
        return false;
    }

    match expr {
        PolicyExpr::True => true,
        PolicyExpr::False => false,
        PolicyExpr::Cmp { column, op, value } => {
            evaluate_cmp(column, op, value, content, descriptor, session)
        }
        PolicyExpr::IsNull { column } => descriptor
            .column_index(column)
            .map(|i| column_is_null(descriptor, content, i).unwrap_or(false))
            .unwrap_or(false),
        PolicyExpr::IsNotNull { column } => descriptor
            .column_index(column)
            .map(|i| !column_is_null(descriptor, content, i).unwrap_or(true))
            .unwrap_or(false),
        PolicyExpr::In {
            column,
            session_path,
        } => evaluate_in(column, session_path, content, descriptor, session),
        PolicyExpr::And(exprs) => exprs
            .iter()
            .all(|e| evaluate_expr_simple(e, content, descriptor, session, depth)),
        PolicyExpr::Or(exprs) => exprs
            .iter()
            .any(|e| evaluate_expr_simple(e, content, descriptor, session, depth)),
        PolicyExpr::Not(inner) => !evaluate_expr_simple(inner, content, descriptor, session, depth),
        PolicyExpr::Exists { .. } => true,
        PolicyExpr::Inherits { .. } => true, // No row loader - permissive
    }
}

/// Recursive evaluation with depth tracking. Public for use by PolicyFilterNode.
pub fn evaluate_expr_recursive(
    expr: &PolicyExpr,
    content: &[u8],
    descriptor: &RowDescriptor,
    session: &Session,
    depth: usize,
) -> bool {
    evaluate_expr_simple(expr, content, descriptor, session, depth)
}

/// Evaluate a comparison expression. Public for use by PolicyFilterNode.
pub fn evaluate_cmp(
    column: &str,
    op: &CmpOp,
    value: &PolicyValue,
    content: &[u8],
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    let col_index = match descriptor.column_index(column) {
        Some(idx) => idx,
        None => return false,
    };

    // Get the comparison value (either literal or from session)
    let cmp_value = match value {
        PolicyValue::Literal(v) => v.clone(),
        PolicyValue::SessionRef(path) => match resolve_session_value(path, session) {
            Some(v) => v,
            None => return false,
        },
    };

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

/// Evaluate an IN expression. Public for use by PolicyFilterNode.
pub fn evaluate_in(
    column: &str,
    session_path: &[String],
    content: &[u8],
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    let col_index = match descriptor.column_index(column) {
        Some(idx) => idx,
        None => return false,
    };

    // Get the column value
    let col_value = match decode_column(descriptor, content, col_index) {
        Ok(v) if !matches!(v, Value::Null) => v,
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
            let id_str = format!("{:?}", id);
            session_array.iter().any(|v| v.as_str() == Some(&id_str))
        }
        _ => false,
    }
}

/// Resolve a session path to a Value. Public for use by PolicyFilterNode.
pub fn resolve_session_value(path: &[String], session: &Session) -> Option<Value> {
    if path.is_empty() {
        return None;
    }

    if path[0] == "user_id" && path.len() == 1 {
        return Some(Value::Text(session.user_id.clone()));
    }

    // For claims paths, convert JSON to Value
    let json_value = session.get_path(path)?;
    json_to_value(json_value)
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
    },
    /// EXISTS clause - check if subquery returns rows.
    Exists {
        table: String,
        condition: Box<PolicyExpr>,
    },
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
    descriptor: &RowDescriptor,
    session: &Session,
) -> SimpleEvalResult {
    evaluate_simple_recursive(expr, content, descriptor, session, 0)
}

fn evaluate_simple_recursive(
    expr: &PolicyExpr,
    content: &[u8],
    descriptor: &RowDescriptor,
    session: &Session,
    depth: usize,
) -> SimpleEvalResult {
    // Prevent infinite recursion
    if depth > 32 {
        return SimpleEvalResult::fail();
    }

    match expr {
        PolicyExpr::True => SimpleEvalResult::pass(),
        PolicyExpr::False => SimpleEvalResult::fail(),

        PolicyExpr::Cmp { column, op, value } => {
            if evaluate_cmp(column, op, value, content, descriptor, session) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::IsNull { column } => {
            let result = descriptor
                .column_index(column)
                .map(|i| column_is_null(descriptor, content, i).unwrap_or(false))
                .unwrap_or(false);
            if result {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::IsNotNull { column } => {
            let result = descriptor
                .column_index(column)
                .map(|i| !column_is_null(descriptor, content, i).unwrap_or(true))
                .unwrap_or(false);
            if result {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::In {
            column,
            session_path,
        } => {
            if evaluate_in(column, session_path, content, descriptor, session) {
                SimpleEvalResult::pass()
            } else {
                SimpleEvalResult::fail()
            }
        }

        PolicyExpr::And(exprs) => {
            let mut all_complex = Vec::new();
            for e in exprs {
                let result = evaluate_simple_recursive(e, content, descriptor, session, depth);
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
                let result = evaluate_simple_recursive(e, content, descriptor, session, depth);
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
            let result = evaluate_simple_recursive(inner, content, descriptor, session, depth);
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
        } => SimpleEvalResult::with_complex(ComplexClause::Inherits {
            operation: *operation,
            via_column: via_column.clone(),
        }),

        PolicyExpr::Exists { table, condition } => {
            SimpleEvalResult::with_complex(ComplexClause::Exists {
                table: table.clone(),
                condition: condition.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            PolicyExpr::Inherits { operation: Operation::Select, via_column }
            if via_column == "folder_id"
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
            ComplexClause::Inherits { operation: Operation::Select, via_column }
            if via_column == "parent_id"
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
