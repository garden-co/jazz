//! ReBAC policy types and evaluation.
//!
//! Policies define who can SELECT, INSERT, UPDATE, or DELETE rows in a table.
//! Permissions can be inherited through foreign key references using INHERITS clauses.

use crate::object::ObjectId;
use crate::sql::row::Value;
use std::collections::HashMap;

/// Policy action type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PolicyAction {
    Select,
    Insert,
    Update,
    Delete,
}

impl PolicyAction {
    /// Returns the action tag for serialization.
    fn tag(&self) -> u8 {
        match self {
            PolicyAction::Select => 0,
            PolicyAction::Insert => 1,
            PolicyAction::Update => 2,
            PolicyAction::Delete => 3,
        }
    }

    /// Parse from tag.
    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(PolicyAction::Select),
            1 => Some(PolicyAction::Insert),
            2 => Some(PolicyAction::Update),
            3 => Some(PolicyAction::Delete),
            _ => None,
        }
    }
}

impl std::fmt::Display for PolicyAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyAction::Select => write!(f, "SELECT"),
            PolicyAction::Insert => write!(f, "INSERT"),
            PolicyAction::Update => write!(f, "UPDATE"),
            PolicyAction::Delete => write!(f, "DELETE"),
        }
    }
}

/// A complete policy definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Policy {
    /// Table this policy applies to.
    pub table: String,
    /// Action this policy governs.
    pub action: PolicyAction,
    /// WHERE clause (for SELECT, UPDATE, DELETE).
    /// Defines which existing rows can be accessed/modified.
    pub where_clause: Option<PolicyExpr>,
    /// CHECK clause (for INSERT, UPDATE).
    /// Validates the new row data.
    pub check_clause: Option<PolicyExpr>,
}

impl Policy {
    /// Create a new policy.
    pub fn new(table: impl Into<String>, action: PolicyAction) -> Self {
        Policy {
            table: table.into(),
            action,
            where_clause: None,
            check_clause: None,
        }
    }

    /// Set the WHERE clause.
    pub fn with_where(mut self, expr: PolicyExpr) -> Self {
        self.where_clause = Some(expr);
        self
    }

    /// Set the CHECK clause.
    pub fn with_check(mut self, expr: PolicyExpr) -> Self {
        self.check_clause = Some(expr);
        self
    }
}

/// Policy expression for conditions.
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyExpr {
    /// Equality comparison: left = right
    Eq(PolicyValue, PolicyValue),
    /// Inequality comparison: left != right
    Ne(PolicyValue, PolicyValue),
    /// Less than: left < right
    Lt(PolicyValue, PolicyValue),
    /// Less than or equal: left <= right
    Le(PolicyValue, PolicyValue),
    /// Greater than: left > right
    Gt(PolicyValue, PolicyValue),
    /// Greater than or equal: left >= right
    Ge(PolicyValue, PolicyValue),

    /// IS NULL check
    IsNull(PolicyValue),
    /// IS NOT NULL check
    IsNotNull(PolicyValue),

    /// Logical AND of expressions
    And(Vec<PolicyExpr>),
    /// Logical OR of expressions
    Or(Vec<PolicyExpr>),
    /// Logical NOT of expression
    Not(Box<PolicyExpr>),

    /// Inherit permission from a referenced row.
    /// The column must be a Ref type pointing to another table.
    Inherits {
        /// Which action to check on the referenced table
        action: PolicyAction,
        /// Column reference (either column name or @new.column)
        column: PolicyColumnRef,
    },
}

impl PolicyExpr {
    /// Create an equality comparison.
    pub fn eq(left: PolicyValue, right: PolicyValue) -> Self {
        PolicyExpr::Eq(left, right)
    }

    /// Create a logical AND.
    pub fn and(exprs: Vec<PolicyExpr>) -> Self {
        PolicyExpr::And(exprs)
    }

    /// Create a logical OR.
    pub fn or(exprs: Vec<PolicyExpr>) -> Self {
        PolicyExpr::Or(exprs)
    }

    /// Create a logical NOT.
    pub fn not(expr: PolicyExpr) -> Self {
        PolicyExpr::Not(Box::new(expr))
    }

    /// Create an INHERITS expression.
    pub fn inherits(action: PolicyAction, column: PolicyColumnRef) -> Self {
        PolicyExpr::Inherits { action, column }
    }
}

/// A value in a policy expression.
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyValue {
    /// Column on current row (for WHERE clauses)
    Column(String),
    /// Column on old row (@old.column, for UPDATE CHECK)
    OldColumn(String),
    /// Column on new row (@new.column, for INSERT/UPDATE CHECK)
    NewColumn(String),
    /// The viewer's user ID (@viewer)
    Viewer,
    /// A literal value
    Literal(Value),
}

impl PolicyValue {
    /// Create a column reference.
    pub fn column(name: impl Into<String>) -> Self {
        PolicyValue::Column(name.into())
    }

    /// Create an @old column reference.
    pub fn old_column(name: impl Into<String>) -> Self {
        PolicyValue::OldColumn(name.into())
    }

    /// Create an @new column reference.
    pub fn new_column(name: impl Into<String>) -> Self {
        PolicyValue::NewColumn(name.into())
    }

    /// Create a literal value.
    pub fn literal(value: Value) -> Self {
        PolicyValue::Literal(value)
    }
}

/// Column reference in INHERITS clause.
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyColumnRef {
    /// Column on current row (for WHERE)
    Current(String),
    /// Column on new row (@new.column, for CHECK)
    New(String),
}

impl PolicyColumnRef {
    /// Get the column name.
    pub fn column_name(&self) -> &str {
        match self {
            PolicyColumnRef::Current(name) => name,
            PolicyColumnRef::New(name) => name,
        }
    }
}

/// Collection of policies for a table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TablePolicies {
    policies: HashMap<PolicyAction, Policy>,
}

impl TablePolicies {
    /// Create an empty policy collection.
    pub fn new() -> Self {
        TablePolicies {
            policies: HashMap::new(),
        }
    }

    /// Add a policy. Returns error if a policy for this action already exists.
    pub fn add(&mut self, policy: Policy) -> Result<(), PolicyError> {
        if self.policies.contains_key(&policy.action) {
            return Err(PolicyError::DuplicatePolicy {
                table: policy.table.clone(),
                action: policy.action,
            });
        }
        self.policies.insert(policy.action, policy);
        Ok(())
    }

    /// Get policy for an action.
    pub fn get(&self, action: PolicyAction) -> Option<&Policy> {
        self.policies.get(&action)
    }

    /// Check if any policies are defined.
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// Iterate over all policies.
    pub fn iter(&self) -> impl Iterator<Item = &Policy> {
        self.policies.values()
    }

    /// Serialize policies to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Number of policies (u8)
        buf.push(self.policies.len() as u8);

        for policy in self.policies.values() {
            // Action tag (u8)
            buf.push(policy.action.tag());

            // WHERE clause
            serialize_option_expr(&mut buf, &policy.where_clause);

            // CHECK clause
            serialize_option_expr(&mut buf, &policy.check_clause);
        }

        buf
    }

    /// Deserialize policies from bytes.
    pub fn from_bytes(data: &[u8], table: &str) -> Result<Self, PolicyError> {
        let mut pos = 0;
        let mut policies = TablePolicies::new();

        if data.is_empty() {
            return Ok(policies);
        }

        // Number of policies
        let count = data[pos] as usize;
        pos += 1;

        for _ in 0..count {
            if pos >= data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }

            // Action tag
            let action = PolicyAction::from_tag(data[pos])
                .ok_or_else(|| PolicyError::DeserializationError("invalid action tag".into()))?;
            pos += 1;

            // WHERE clause
            let (where_clause, new_pos) = deserialize_option_expr(data, pos)?;
            pos = new_pos;

            // CHECK clause
            let (check_clause, new_pos) = deserialize_option_expr(data, pos)?;
            pos = new_pos;

            let policy = Policy {
                table: table.to_string(),
                action,
                where_clause,
                check_clause,
            };
            policies.policies.insert(action, policy);
        }

        Ok(policies)
    }
}

/// Policy-related errors.
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyError {
    /// Attempted to add a duplicate policy for the same action.
    DuplicatePolicy {
        table: String,
        action: PolicyAction,
    },
    /// Error during deserialization.
    DeserializationError(String),
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyError::DuplicatePolicy { table, action } => {
                write!(f, "duplicate {} policy for table '{}'", action, table)
            }
            PolicyError::DeserializationError(msg) => {
                write!(f, "policy deserialization error: {}", msg)
            }
        }
    }
}

impl std::error::Error for PolicyError {}

// Serialization helpers

fn serialize_option_expr(buf: &mut Vec<u8>, expr: &Option<PolicyExpr>) {
    match expr {
        None => buf.push(0),
        Some(e) => {
            buf.push(1);
            serialize_expr(buf, e);
        }
    }
}

fn serialize_expr(buf: &mut Vec<u8>, expr: &PolicyExpr) {
    match expr {
        PolicyExpr::Eq(left, right) => {
            buf.push(0);
            serialize_value(buf, left);
            serialize_value(buf, right);
        }
        PolicyExpr::Ne(left, right) => {
            buf.push(1);
            serialize_value(buf, left);
            serialize_value(buf, right);
        }
        PolicyExpr::Lt(left, right) => {
            buf.push(2);
            serialize_value(buf, left);
            serialize_value(buf, right);
        }
        PolicyExpr::Le(left, right) => {
            buf.push(3);
            serialize_value(buf, left);
            serialize_value(buf, right);
        }
        PolicyExpr::Gt(left, right) => {
            buf.push(4);
            serialize_value(buf, left);
            serialize_value(buf, right);
        }
        PolicyExpr::Ge(left, right) => {
            buf.push(5);
            serialize_value(buf, left);
            serialize_value(buf, right);
        }
        PolicyExpr::IsNull(val) => {
            buf.push(6);
            serialize_value(buf, val);
        }
        PolicyExpr::IsNotNull(val) => {
            buf.push(7);
            serialize_value(buf, val);
        }
        PolicyExpr::And(exprs) => {
            buf.push(8);
            buf.push(exprs.len() as u8);
            for e in exprs {
                serialize_expr(buf, e);
            }
        }
        PolicyExpr::Or(exprs) => {
            buf.push(9);
            buf.push(exprs.len() as u8);
            for e in exprs {
                serialize_expr(buf, e);
            }
        }
        PolicyExpr::Not(inner) => {
            buf.push(10);
            serialize_expr(buf, inner);
        }
        PolicyExpr::Inherits { action, column } => {
            buf.push(11);
            buf.push(action.tag());
            serialize_column_ref(buf, column);
        }
    }
}

fn serialize_value(buf: &mut Vec<u8>, val: &PolicyValue) {
    match val {
        PolicyValue::Column(name) => {
            buf.push(0);
            serialize_string(buf, name);
        }
        PolicyValue::OldColumn(name) => {
            buf.push(1);
            serialize_string(buf, name);
        }
        PolicyValue::NewColumn(name) => {
            buf.push(2);
            serialize_string(buf, name);
        }
        PolicyValue::Viewer => {
            buf.push(3);
        }
        PolicyValue::Literal(v) => {
            buf.push(4);
            serialize_literal(buf, v);
        }
    }
}

fn serialize_column_ref(buf: &mut Vec<u8>, col: &PolicyColumnRef) {
    match col {
        PolicyColumnRef::Current(name) => {
            buf.push(0);
            serialize_string(buf, name);
        }
        PolicyColumnRef::New(name) => {
            buf.push(1);
            serialize_string(buf, name);
        }
    }
}

fn serialize_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn serialize_literal(buf: &mut Vec<u8>, val: &Value) {
    match val {
        Value::Null => buf.push(0),
        Value::Bool(b) => {
            buf.push(1);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::I64(n) => {
            buf.push(2);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::F64(n) => {
            buf.push(3);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::String(s) => {
            buf.push(4);
            serialize_string(buf, s);
        }
        Value::Bytes(b) => {
            buf.push(5);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Ref(id) => {
            buf.push(6);
            buf.extend_from_slice(&id.inner().to_le_bytes());
        }
    }
}

// Deserialization helpers

fn deserialize_option_expr(data: &[u8], pos: usize) -> Result<(Option<PolicyExpr>, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError("unexpected end of data".into()));
    }

    match data[pos] {
        0 => Ok((None, pos + 1)),
        1 => {
            let (expr, new_pos) = deserialize_expr(data, pos + 1)?;
            Ok((Some(expr), new_pos))
        }
        _ => Err(PolicyError::DeserializationError("invalid option tag".into())),
    }
}

fn deserialize_expr(data: &[u8], pos: usize) -> Result<(PolicyExpr, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError("unexpected end of data".into()));
    }

    let tag = data[pos];
    let mut pos = pos + 1;

    match tag {
        0..=5 => {
            // Binary comparisons
            let (left, new_pos) = deserialize_value(data, pos)?;
            let (right, new_pos) = deserialize_value(data, new_pos)?;
            let expr = match tag {
                0 => PolicyExpr::Eq(left, right),
                1 => PolicyExpr::Ne(left, right),
                2 => PolicyExpr::Lt(left, right),
                3 => PolicyExpr::Le(left, right),
                4 => PolicyExpr::Gt(left, right),
                5 => PolicyExpr::Ge(left, right),
                _ => unreachable!(),
            };
            Ok((expr, new_pos))
        }
        6 => {
            // IsNull
            let (val, new_pos) = deserialize_value(data, pos)?;
            Ok((PolicyExpr::IsNull(val), new_pos))
        }
        7 => {
            // IsNotNull
            let (val, new_pos) = deserialize_value(data, pos)?;
            Ok((PolicyExpr::IsNotNull(val), new_pos))
        }
        8 => {
            // And
            if pos >= data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }
            let count = data[pos] as usize;
            pos += 1;
            let mut exprs = Vec::with_capacity(count);
            for _ in 0..count {
                let (expr, new_pos) = deserialize_expr(data, pos)?;
                exprs.push(expr);
                pos = new_pos;
            }
            Ok((PolicyExpr::And(exprs), pos))
        }
        9 => {
            // Or
            if pos >= data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }
            let count = data[pos] as usize;
            pos += 1;
            let mut exprs = Vec::with_capacity(count);
            for _ in 0..count {
                let (expr, new_pos) = deserialize_expr(data, pos)?;
                exprs.push(expr);
                pos = new_pos;
            }
            Ok((PolicyExpr::Or(exprs), pos))
        }
        10 => {
            // Not
            let (inner, new_pos) = deserialize_expr(data, pos)?;
            Ok((PolicyExpr::Not(Box::new(inner)), new_pos))
        }
        11 => {
            // Inherits
            if pos >= data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }
            let action = PolicyAction::from_tag(data[pos])
                .ok_or_else(|| PolicyError::DeserializationError("invalid action tag".into()))?;
            pos += 1;
            let (column, new_pos) = deserialize_column_ref(data, pos)?;
            Ok((PolicyExpr::Inherits { action, column }, new_pos))
        }
        _ => Err(PolicyError::DeserializationError(format!("invalid expr tag: {}", tag))),
    }
}

fn deserialize_value(data: &[u8], pos: usize) -> Result<(PolicyValue, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError("unexpected end of data".into()));
    }

    let tag = data[pos];
    let pos = pos + 1;

    match tag {
        0 => {
            let (name, new_pos) = deserialize_string(data, pos)?;
            Ok((PolicyValue::Column(name), new_pos))
        }
        1 => {
            let (name, new_pos) = deserialize_string(data, pos)?;
            Ok((PolicyValue::OldColumn(name), new_pos))
        }
        2 => {
            let (name, new_pos) = deserialize_string(data, pos)?;
            Ok((PolicyValue::NewColumn(name), new_pos))
        }
        3 => Ok((PolicyValue::Viewer, pos)),
        4 => {
            let (lit, new_pos) = deserialize_literal(data, pos)?;
            Ok((PolicyValue::Literal(lit), new_pos))
        }
        _ => Err(PolicyError::DeserializationError(format!("invalid value tag: {}", tag))),
    }
}

fn deserialize_column_ref(data: &[u8], pos: usize) -> Result<(PolicyColumnRef, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError("unexpected end of data".into()));
    }

    let tag = data[pos];
    let pos = pos + 1;

    match tag {
        0 => {
            let (name, new_pos) = deserialize_string(data, pos)?;
            Ok((PolicyColumnRef::Current(name), new_pos))
        }
        1 => {
            let (name, new_pos) = deserialize_string(data, pos)?;
            Ok((PolicyColumnRef::New(name), new_pos))
        }
        _ => Err(PolicyError::DeserializationError(format!("invalid column ref tag: {}", tag))),
    }
}

fn deserialize_string(data: &[u8], pos: usize) -> Result<(String, usize), PolicyError> {
    if pos + 2 > data.len() {
        return Err(PolicyError::DeserializationError("unexpected end of data".into()));
    }

    let len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
    let pos = pos + 2;

    if pos + len > data.len() {
        return Err(PolicyError::DeserializationError("unexpected end of data".into()));
    }

    let s = std::str::from_utf8(&data[pos..pos + len])
        .map_err(|_| PolicyError::DeserializationError("invalid utf8".into()))?
        .to_string();

    Ok((s, pos + len))
}

fn deserialize_literal(data: &[u8], pos: usize) -> Result<(Value, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError("unexpected end of data".into()));
    }

    let tag = data[pos];
    let pos = pos + 1;

    match tag {
        0 => Ok((Value::Null, pos)),
        1 => {
            if pos >= data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }
            Ok((Value::Bool(data[pos] != 0), pos + 1))
        }
        2 => {
            if pos + 8 > data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }
            let n = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            Ok((Value::I64(n), pos + 8))
        }
        3 => {
            if pos + 8 > data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }
            let n = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            Ok((Value::F64(n), pos + 8))
        }
        4 => {
            let (s, new_pos) = deserialize_string(data, pos)?;
            Ok((Value::String(s), new_pos))
        }
        5 => {
            if pos + 4 > data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            let pos = pos + 4;
            if pos + len > data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }
            Ok((Value::Bytes(data[pos..pos + len].to_vec()), pos + len))
        }
        6 => {
            if pos + 16 > data.len() {
                return Err(PolicyError::DeserializationError("unexpected end of data".into()));
            }
            let id = u128::from_le_bytes(data[pos..pos + 16].try_into().unwrap());
            Ok((Value::Ref(ObjectId::from(id)), pos + 16))
        }
        _ => Err(PolicyError::DeserializationError(format!("invalid literal tag: {}", tag))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_action_roundtrip() {
        for action in [PolicyAction::Select, PolicyAction::Insert, PolicyAction::Update, PolicyAction::Delete] {
            assert_eq!(PolicyAction::from_tag(action.tag()), Some(action));
        }
    }

    #[test]
    fn test_simple_policy_serialization() {
        let mut policies = TablePolicies::new();

        // Simple SELECT policy: WHERE owner_id = @viewer
        let policy = Policy::new("documents", PolicyAction::Select)
            .with_where(PolicyExpr::Eq(
                PolicyValue::Column("owner_id".into()),
                PolicyValue::Viewer,
            ));

        policies.add(policy).unwrap();

        let bytes = policies.to_bytes();
        let deserialized = TablePolicies::from_bytes(&bytes, "documents").unwrap();

        assert_eq!(policies, deserialized);
    }

    #[test]
    fn test_inherits_policy_serialization() {
        let mut policies = TablePolicies::new();

        // SELECT policy with INHERITS: WHERE INHERITS SELECT FROM folder_id
        let policy = Policy::new("documents", PolicyAction::Select)
            .with_where(PolicyExpr::Inherits {
                action: PolicyAction::Select,
                column: PolicyColumnRef::Current("folder_id".into()),
            });

        policies.add(policy).unwrap();

        let bytes = policies.to_bytes();
        let deserialized = TablePolicies::from_bytes(&bytes, "documents").unwrap();

        assert_eq!(policies, deserialized);
    }

    #[test]
    fn test_complex_policy_serialization() {
        let mut policies = TablePolicies::new();

        // SELECT: WHERE owner_id = @viewer OR INHERITS SELECT FROM folder_id
        let select_policy = Policy::new("documents", PolicyAction::Select)
            .with_where(PolicyExpr::Or(vec![
                PolicyExpr::Eq(
                    PolicyValue::Column("owner_id".into()),
                    PolicyValue::Viewer,
                ),
                PolicyExpr::Inherits {
                    action: PolicyAction::Select,
                    column: PolicyColumnRef::Current("folder_id".into()),
                },
            ]));

        // INSERT: CHECK (@new.author_id = @viewer AND INHERITS UPDATE FROM @new.folder_id)
        let insert_policy = Policy::new("documents", PolicyAction::Insert)
            .with_check(PolicyExpr::And(vec![
                PolicyExpr::Eq(
                    PolicyValue::NewColumn("author_id".into()),
                    PolicyValue::Viewer,
                ),
                PolicyExpr::Inherits {
                    action: PolicyAction::Update,
                    column: PolicyColumnRef::New("folder_id".into()),
                },
            ]));

        policies.add(select_policy).unwrap();
        policies.add(insert_policy).unwrap();

        let bytes = policies.to_bytes();
        let deserialized = TablePolicies::from_bytes(&bytes, "documents").unwrap();

        assert_eq!(policies, deserialized);
    }

    #[test]
    fn test_duplicate_policy_error() {
        let mut policies = TablePolicies::new();

        let policy1 = Policy::new("documents", PolicyAction::Select)
            .with_where(PolicyExpr::Eq(
                PolicyValue::Column("owner_id".into()),
                PolicyValue::Viewer,
            ));

        let policy2 = Policy::new("documents", PolicyAction::Select)
            .with_where(PolicyExpr::Eq(
                PolicyValue::Column("author_id".into()),
                PolicyValue::Viewer,
            ));

        policies.add(policy1).unwrap();
        let result = policies.add(policy2);

        assert!(matches!(result, Err(PolicyError::DuplicatePolicy { .. })));
    }

    #[test]
    fn test_literal_values_serialization() {
        let mut policies = TablePolicies::new();

        // Test various literal types
        let policy = Policy::new("test", PolicyAction::Select)
            .with_where(PolicyExpr::And(vec![
                PolicyExpr::Eq(
                    PolicyValue::Column("status".into()),
                    PolicyValue::Literal(Value::String("active".into())),
                ),
                PolicyExpr::Eq(
                    PolicyValue::Column("count".into()),
                    PolicyValue::Literal(Value::I64(42)),
                ),
                PolicyExpr::Eq(
                    PolicyValue::Column("enabled".into()),
                    PolicyValue::Literal(Value::Bool(true)),
                ),
            ]));

        policies.add(policy).unwrap();

        let bytes = policies.to_bytes();
        let deserialized = TablePolicies::from_bytes(&bytes, "test").unwrap();

        assert_eq!(policies, deserialized);
    }
}
