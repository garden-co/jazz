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
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
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
    DuplicatePolicy { table: String, action: PolicyAction },
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
        Value::NullableNone => buf.push(0),
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
        Value::I32(n) => {
            buf.push(7);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::U32(n) => {
            buf.push(8);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        // NullableSome: serialize the inner value
        Value::NullableSome(inner) => serialize_literal(buf, inner),
        // Row, Array, Blob, BlobArray are not valid in policy literals - they're only for query results
        Value::Row(_) | Value::Array(_) | Value::Blob(_) | Value::BlobArray(_) => {
            panic!("Row, Array, Blob, and BlobArray values cannot be used in policy literals");
        }
    }
}

// Deserialization helpers

fn deserialize_option_expr(
    data: &[u8],
    pos: usize,
) -> Result<(Option<PolicyExpr>, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError(
            "unexpected end of data".into(),
        ));
    }

    match data[pos] {
        0 => Ok((None, pos + 1)),
        1 => {
            let (expr, new_pos) = deserialize_expr(data, pos + 1)?;
            Ok((Some(expr), new_pos))
        }
        _ => Err(PolicyError::DeserializationError(
            "invalid option tag".into(),
        )),
    }
}

fn deserialize_expr(data: &[u8], pos: usize) -> Result<(PolicyExpr, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError(
            "unexpected end of data".into(),
        ));
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
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
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
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
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
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
            }
            let action = PolicyAction::from_tag(data[pos])
                .ok_or_else(|| PolicyError::DeserializationError("invalid action tag".into()))?;
            pos += 1;
            let (column, new_pos) = deserialize_column_ref(data, pos)?;
            Ok((PolicyExpr::Inherits { action, column }, new_pos))
        }
        _ => Err(PolicyError::DeserializationError(format!(
            "invalid expr tag: {}",
            tag
        ))),
    }
}

fn deserialize_value(data: &[u8], pos: usize) -> Result<(PolicyValue, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError(
            "unexpected end of data".into(),
        ));
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
        _ => Err(PolicyError::DeserializationError(format!(
            "invalid value tag: {}",
            tag
        ))),
    }
}

fn deserialize_column_ref(
    data: &[u8],
    pos: usize,
) -> Result<(PolicyColumnRef, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError(
            "unexpected end of data".into(),
        ));
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
        _ => Err(PolicyError::DeserializationError(format!(
            "invalid column ref tag: {}",
            tag
        ))),
    }
}

fn deserialize_string(data: &[u8], pos: usize) -> Result<(String, usize), PolicyError> {
    if pos + 2 > data.len() {
        return Err(PolicyError::DeserializationError(
            "unexpected end of data".into(),
        ));
    }

    let len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
    let pos = pos + 2;

    if pos + len > data.len() {
        return Err(PolicyError::DeserializationError(
            "unexpected end of data".into(),
        ));
    }

    let s = std::str::from_utf8(&data[pos..pos + len])
        .map_err(|_| PolicyError::DeserializationError("invalid utf8".into()))?
        .to_string();

    Ok((s, pos + len))
}

fn deserialize_literal(data: &[u8], pos: usize) -> Result<(Value, usize), PolicyError> {
    if pos >= data.len() {
        return Err(PolicyError::DeserializationError(
            "unexpected end of data".into(),
        ));
    }

    let tag = data[pos];
    let pos = pos + 1;

    match tag {
        0 => Ok((Value::NullableNone, pos)),
        1 => {
            if pos >= data.len() {
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
            }
            Ok((Value::Bool(data[pos] != 0), pos + 1))
        }
        2 => {
            if pos + 8 > data.len() {
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
            }
            let n = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            Ok((Value::I64(n), pos + 8))
        }
        3 => {
            if pos + 8 > data.len() {
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
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
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
            }
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            let pos = pos + 4;
            if pos + len > data.len() {
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
            }
            Ok((Value::Bytes(data[pos..pos + len].to_vec()), pos + len))
        }
        6 => {
            if pos + 16 > data.len() {
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
            }
            let id = u128::from_le_bytes(data[pos..pos + 16].try_into().unwrap());
            Ok((Value::Ref(ObjectId::from(id)), pos + 16))
        }
        7 => {
            if pos + 4 > data.len() {
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
            }
            let n = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            Ok((Value::I32(n), pos + 4))
        }
        8 => {
            if pos + 4 > data.len() {
                return Err(PolicyError::DeserializationError(
                    "unexpected end of data".into(),
                ));
            }
            let n = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            Ok((Value::U32(n), pos + 4))
        }
        _ => Err(PolicyError::DeserializationError(format!(
            "invalid literal tag: {}",
            tag
        ))),
    }
}

// ========== Policy Evaluation ==========

use crate::sql::row::Row;
use crate::sql::schema::TableSchema;
use std::collections::HashSet;

/// Configuration for policy evaluation.
#[derive(Debug, Clone)]
pub struct PolicyConfig {
    /// Maximum depth for recursive INHERITS evaluation.
    pub max_inheritance_depth: usize,
    /// Whether to log warnings for missing policies.
    pub warn_on_missing_policy: bool,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        PolicyConfig {
            max_inheritance_depth: 100,
            warn_on_missing_policy: true,
        }
    }
}

/// Trait for looking up rows by table and ID.
pub trait RowLookup {
    /// Get a row by table name and ID.
    fn get_row(&self, table: &str, id: ObjectId) -> Option<Row>;

    /// Get a table schema by name.
    fn get_schema(&self, table: &str) -> Option<TableSchema>;
}

/// Trait for looking up policies.
pub trait PolicyLookup {
    /// Get policies for a table.
    fn get_policies(&self, table: &str) -> Option<TablePolicies>;
}

/// Context for evaluating a policy expression.
#[derive(Debug)]
pub struct EvalContext<'a> {
    /// The current row (for WHERE clauses).
    pub row: Option<&'a Row>,
    /// The new row data (for INSERT/UPDATE CHECK).
    pub new_row: Option<&'a Row>,
    /// The old row data (for UPDATE CHECK).
    pub old_row: Option<&'a Row>,
    /// The table schema (for column lookups).
    pub schema: &'a TableSchema,
}

impl<'a> EvalContext<'a> {
    /// Create context for SELECT/DELETE (just current row).
    pub fn for_select(row: &'a Row, schema: &'a TableSchema) -> Self {
        EvalContext {
            row: Some(row),
            new_row: None,
            old_row: None,
            schema,
        }
    }

    /// Create context for INSERT (just new row).
    pub fn for_insert(new_row: &'a Row, schema: &'a TableSchema) -> Self {
        EvalContext {
            row: None,
            new_row: Some(new_row),
            old_row: None,
            schema,
        }
    }

    /// Create context for UPDATE (old row, new row, and current = old for WHERE).
    pub fn for_update(old_row: &'a Row, new_row: &'a Row, schema: &'a TableSchema) -> Self {
        EvalContext {
            row: Some(old_row), // WHERE evaluates against existing row
            new_row: Some(new_row),
            old_row: Some(old_row),
            schema,
        }
    }

    /// Get column value from current row.
    fn get_column(&self, name: &str) -> Option<&Value> {
        let row = self.row?;
        let idx = self.schema.column_index(name)?;
        row.values.get(idx)
    }

    /// Get column value from @old row.
    fn get_old_column(&self, name: &str) -> Option<&Value> {
        let row = self.old_row?;
        let idx = self.schema.column_index(name)?;
        row.values.get(idx)
    }

    /// Get column value from @new row.
    fn get_new_column(&self, name: &str) -> Option<&Value> {
        let row = self.new_row?;
        let idx = self.schema.column_index(name)?;
        row.values.get(idx)
    }
}

/// Result of policy evaluation with explanation.
#[derive(Debug, Clone)]
pub enum PolicyResult {
    /// Access allowed.
    Allowed { reason: String },
    /// Access denied.
    Denied { reason: String },
}

impl PolicyResult {
    /// Returns true if access was allowed.
    pub fn is_allowed(&self) -> bool {
        matches!(self, PolicyResult::Allowed { .. })
    }

    /// Returns true if access was denied.
    pub fn is_denied(&self) -> bool {
        matches!(self, PolicyResult::Denied { .. })
    }
}

/// Policy evaluator with cycle detection and depth limiting.
pub struct PolicyEvaluator<'a, R: RowLookup, P: PolicyLookup> {
    /// Row lookup implementation.
    row_lookup: &'a R,
    /// Policy lookup implementation.
    policy_lookup: &'a P,
    /// The viewer's user ID.
    viewer: ObjectId,
    /// Configuration.
    config: PolicyConfig,
    /// Visited (table, row_id) pairs for cycle detection.
    visited: HashSet<(String, ObjectId)>,
    /// Current recursion depth.
    depth: usize,
    /// Tables that have already warned about missing policies.
    warned_tables: &'a std::sync::Mutex<HashSet<String>>,
}

/// Global set of tables that have warned about missing policies.
/// This ensures we only warn once per table per process lifetime.
static WARNED_TABLES: std::sync::LazyLock<std::sync::Mutex<HashSet<String>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashSet::new()));

impl<'a, R: RowLookup, P: PolicyLookup> PolicyEvaluator<'a, R, P> {
    /// Create a new policy evaluator.
    pub fn new(
        row_lookup: &'a R,
        policy_lookup: &'a P,
        viewer: ObjectId,
        config: PolicyConfig,
    ) -> Self {
        PolicyEvaluator {
            row_lookup,
            policy_lookup,
            viewer,
            config,
            visited: HashSet::new(),
            depth: 0,
            warned_tables: &WARNED_TABLES,
        }
    }

    /// Check if viewer can SELECT the given row.
    pub fn check_select(&mut self, table: &str, row: &Row) -> PolicyResult {
        let schema = match self.row_lookup.get_schema(table) {
            Some(s) => s,
            None => {
                return PolicyResult::Denied {
                    reason: format!("table '{}' not found", table),
                };
            }
        };

        let policies = self.policy_lookup.get_policies(table);
        let policy = policies.as_ref().and_then(|p| p.get(PolicyAction::Select));

        match policy {
            Some(p) => {
                let ctx = EvalContext::for_select(row, &schema);
                self.eval_where_clause(table, &p.where_clause, &ctx)
            }
            None => self.default_allow(table, PolicyAction::Select),
        }
    }

    /// Check if viewer can INSERT the given row.
    pub fn check_insert(&mut self, table: &str, new_row: &Row) -> PolicyResult {
        let schema = match self.row_lookup.get_schema(table) {
            Some(s) => s,
            None => {
                return PolicyResult::Denied {
                    reason: format!("table '{}' not found", table),
                };
            }
        };

        let policies = self.policy_lookup.get_policies(table);
        let policy = policies.as_ref().and_then(|p| p.get(PolicyAction::Insert));

        match policy {
            Some(p) => {
                let ctx = EvalContext::for_insert(new_row, &schema);
                self.eval_check_clause(table, &p.check_clause, &ctx)
            }
            None => self.default_allow(table, PolicyAction::Insert),
        }
    }

    /// Check if viewer can UPDATE the given row with new values.
    pub fn check_update(&mut self, table: &str, old_row: &Row, new_row: &Row) -> PolicyResult {
        let schema = match self.row_lookup.get_schema(table) {
            Some(s) => s,
            None => {
                return PolicyResult::Denied {
                    reason: format!("table '{}' not found", table),
                };
            }
        };

        let policies = self.policy_lookup.get_policies(table);
        let policy = policies.as_ref().and_then(|p| p.get(PolicyAction::Update));

        match policy {
            Some(p) => {
                let ctx = EvalContext::for_update(old_row, new_row, &schema);

                // Check WHERE clause first (which rows can be modified)
                let where_result = self.eval_where_clause(table, &p.where_clause, &ctx);
                if where_result.is_denied() {
                    return where_result;
                }

                // Then check CHECK clause (validate changes)
                self.eval_check_clause(table, &p.check_clause, &ctx)
            }
            None => self.default_allow(table, PolicyAction::Update),
        }
    }

    /// Check if viewer can DELETE the given row.
    pub fn check_delete(&mut self, table: &str, row: &Row) -> PolicyResult {
        let schema = match self.row_lookup.get_schema(table) {
            Some(s) => s,
            None => {
                return PolicyResult::Denied {
                    reason: format!("table '{}' not found", table),
                };
            }
        };

        let policies = self.policy_lookup.get_policies(table);

        // DELETE defaults to UPDATE policy if not specified
        let policy = policies.as_ref().and_then(|p| {
            p.get(PolicyAction::Delete)
                .or_else(|| p.get(PolicyAction::Update))
        });

        match policy {
            Some(p) => {
                let ctx = EvalContext::for_select(row, &schema);
                self.eval_where_clause(table, &p.where_clause, &ctx)
            }
            None => self.default_allow(table, PolicyAction::Delete),
        }
    }

    /// Evaluate a WHERE clause.
    fn eval_where_clause(
        &mut self,
        table: &str,
        where_clause: &Option<PolicyExpr>,
        ctx: &EvalContext,
    ) -> PolicyResult {
        match where_clause {
            Some(expr) => {
                if self.eval_expr(expr, ctx, table) {
                    PolicyResult::Allowed {
                        reason: "policy WHERE matched".into(),
                    }
                } else {
                    PolicyResult::Denied {
                        reason: "policy WHERE not satisfied".into(),
                    }
                }
            }
            None => PolicyResult::Allowed {
                reason: "no WHERE clause".into(),
            },
        }
    }

    /// Evaluate a CHECK clause.
    fn eval_check_clause(
        &mut self,
        table: &str,
        check_clause: &Option<PolicyExpr>,
        ctx: &EvalContext,
    ) -> PolicyResult {
        match check_clause {
            Some(expr) => {
                if self.eval_expr(expr, ctx, table) {
                    PolicyResult::Allowed {
                        reason: "policy CHECK passed".into(),
                    }
                } else {
                    PolicyResult::Denied {
                        reason: "policy CHECK failed".into(),
                    }
                }
            }
            None => PolicyResult::Allowed {
                reason: "no CHECK clause".into(),
            },
        }
    }

    /// Default allow with warning for missing policy.
    fn default_allow(&self, table: &str, action: PolicyAction) -> PolicyResult {
        if self.config.warn_on_missing_policy {
            let mut warned = self.warned_tables.lock().unwrap();
            let key = format!("{}:{}", table, action);
            if !warned.contains(&key) {
                eprintln!(
                    "WARNING: No {} policy defined for table '{}'. Allowing access by default.",
                    action, table
                );
                warned.insert(key);
            }
        }
        PolicyResult::Allowed {
            reason: format!("no {} policy defined (default allow)", action),
        }
    }

    /// Evaluate a policy expression.
    fn eval_expr(&mut self, expr: &PolicyExpr, ctx: &EvalContext, table: &str) -> bool {
        match expr {
            PolicyExpr::Eq(left, right) => self.compare_values(left, right, ctx, |a, b| a == b),
            PolicyExpr::Ne(left, right) => self.compare_values(left, right, ctx, |a, b| a != b),
            PolicyExpr::Lt(left, right) => {
                self.compare_ordered(left, right, ctx, |ord| ord.is_lt())
            }
            PolicyExpr::Le(left, right) => {
                self.compare_ordered(left, right, ctx, |ord| ord.is_le())
            }
            PolicyExpr::Gt(left, right) => {
                self.compare_ordered(left, right, ctx, |ord| ord.is_gt())
            }
            PolicyExpr::Ge(left, right) => {
                self.compare_ordered(left, right, ctx, |ord| ord.is_ge())
            }
            PolicyExpr::IsNull(val) => self
                .resolve_value(val, ctx)
                .map(|v| v.is_null())
                .unwrap_or(true),
            PolicyExpr::IsNotNull(val) => self
                .resolve_value(val, ctx)
                .map(|v| !v.is_null())
                .unwrap_or(false),
            PolicyExpr::And(exprs) => exprs.iter().all(|e| self.eval_expr(e, ctx, table)),
            PolicyExpr::Or(exprs) => exprs.iter().any(|e| self.eval_expr(e, ctx, table)),
            PolicyExpr::Not(inner) => !self.eval_expr(inner, ctx, table),
            PolicyExpr::Inherits { action, column } => {
                self.eval_inherits(*action, column, ctx, table)
            }
        }
    }

    /// Resolve a PolicyValue to an actual Value.
    fn resolve_value<'b>(&self, pv: &'b PolicyValue, ctx: &'b EvalContext) -> Option<Value> {
        match pv {
            PolicyValue::Column(name) => ctx.get_column(name).cloned(),
            PolicyValue::OldColumn(name) => ctx.get_old_column(name).cloned(),
            PolicyValue::NewColumn(name) => ctx.get_new_column(name).cloned(),
            PolicyValue::Viewer => Some(Value::Ref(self.viewer)),
            PolicyValue::Literal(v) => Some(v.clone()),
        }
    }

    /// Compare two values with a predicate.
    fn compare_values<F>(
        &self,
        left: &PolicyValue,
        right: &PolicyValue,
        ctx: &EvalContext,
        pred: F,
    ) -> bool
    where
        F: Fn(&Value, &Value) -> bool,
    {
        match (
            self.resolve_value(left, ctx),
            self.resolve_value(right, ctx),
        ) {
            (Some(l), Some(r)) => pred(&l, &r),
            _ => false, // If either value can't be resolved, comparison fails
        }
    }

    /// Compare two values with ordering.
    fn compare_ordered<F>(
        &self,
        left: &PolicyValue,
        right: &PolicyValue,
        ctx: &EvalContext,
        pred: F,
    ) -> bool
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        let l = self.resolve_value(left, ctx);
        let r = self.resolve_value(right, ctx);

        match (l, r) {
            (Some(Value::I64(a)), Some(Value::I64(b))) => pred(a.cmp(&b)),
            (Some(Value::F64(a)), Some(Value::F64(b))) => {
                a.partial_cmp(&b).map(|o| pred(o)).unwrap_or(false)
            }
            (Some(Value::String(a)), Some(Value::String(b))) => pred(a.cmp(&b)),
            _ => false,
        }
    }

    /// Evaluate an INHERITS clause.
    fn eval_inherits(
        &mut self,
        action: PolicyAction,
        column: &PolicyColumnRef,
        ctx: &EvalContext,
        current_table: &str,
    ) -> bool {
        // Check depth limit
        if self.depth >= self.config.max_inheritance_depth {
            eprintln!(
                "WARNING: Max inheritance depth ({}) exceeded while evaluating policy on '{}'",
                self.config.max_inheritance_depth, current_table
            );
            return false;
        }

        // Get the referenced ID from the column
        let ref_id = match column {
            PolicyColumnRef::Current(name) => ctx.get_column(name).and_then(|v| v.as_ref()),
            PolicyColumnRef::New(name) => ctx.get_new_column(name).and_then(|v| v.as_ref()),
        };

        let ref_id = match ref_id {
            Some(id) => id,
            None => return false, // NULL reference = no access
        };

        // Get the target table from the schema
        let col_name = column.column_name();
        let target_table = match ctx.schema.column(col_name) {
            Some(col) => match &col.ty {
                crate::sql::schema::ColumnType::Ref(t) => t.clone(),
                _ => return false, // Column is not a reference
            },
            None => return false,
        };

        // Check for cycles
        let visit_key = (target_table.clone(), ref_id);
        if self.visited.contains(&visit_key) {
            eprintln!(
                "WARNING: Cycle detected in policy inheritance: {}:{} -> {}:{}",
                current_table,
                ctx.row.map(|r| r.id).unwrap_or_default(),
                target_table,
                ref_id
            );
            return false;
        }

        // Look up the referenced row
        let ref_row = match self.row_lookup.get_row(&target_table, ref_id) {
            Some(r) => r,
            None => return false, // Referenced row doesn't exist
        };

        let ref_schema = match self.row_lookup.get_schema(&target_table) {
            Some(s) => s,
            None => return false,
        };

        // Get the policy for the target table
        let policies = self.policy_lookup.get_policies(&target_table);
        let policy = policies.as_ref().and_then(|p| p.get(action));

        let result = match policy {
            Some(p) => {
                // Mark as visited and increment depth
                self.visited.insert(visit_key.clone());
                self.depth += 1;

                let ref_ctx = EvalContext::for_select(&ref_row, &ref_schema);
                let result = match &p.where_clause {
                    Some(expr) => self.eval_expr(expr, &ref_ctx, &target_table),
                    None => true, // No WHERE = allow
                };

                // Restore state
                self.depth -= 1;
                self.visited.remove(&visit_key);

                result
            }
            None => {
                // No policy on target = default allow (with warning)
                if self.config.warn_on_missing_policy {
                    let mut warned = self.warned_tables.lock().unwrap();
                    let key = format!("{}:{}", target_table, action);
                    if !warned.contains(&key) {
                        eprintln!(
                            "WARNING: No {} policy defined for table '{}' (inherited from '{}'). Allowing access by default.",
                            action, target_table, current_table
                        );
                        warned.insert(key);
                    }
                }
                true
            }
        };

        result
    }
}

/// Clear the global warned tables set (useful for testing).
pub fn clear_policy_warnings() {
    WARNED_TABLES.lock().unwrap().clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_action_roundtrip() {
        for action in [
            PolicyAction::Select,
            PolicyAction::Insert,
            PolicyAction::Update,
            PolicyAction::Delete,
        ] {
            assert_eq!(PolicyAction::from_tag(action.tag()), Some(action));
        }
    }

    #[test]
    fn test_simple_policy_serialization() {
        let mut policies = TablePolicies::new();

        // Simple SELECT policy: WHERE owner_id = @viewer
        let policy = Policy::new("documents", PolicyAction::Select).with_where(PolicyExpr::Eq(
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
        let policy =
            Policy::new("documents", PolicyAction::Select).with_where(PolicyExpr::Inherits {
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
        let select_policy =
            Policy::new("documents", PolicyAction::Select).with_where(PolicyExpr::Or(vec![
                PolicyExpr::Eq(PolicyValue::Column("owner_id".into()), PolicyValue::Viewer),
                PolicyExpr::Inherits {
                    action: PolicyAction::Select,
                    column: PolicyColumnRef::Current("folder_id".into()),
                },
            ]));

        // INSERT: CHECK (@new.author_id = @viewer AND INHERITS UPDATE FROM @new.folder_id)
        let insert_policy =
            Policy::new("documents", PolicyAction::Insert).with_check(PolicyExpr::And(vec![
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

        let policy1 = Policy::new("documents", PolicyAction::Select).with_where(PolicyExpr::Eq(
            PolicyValue::Column("owner_id".into()),
            PolicyValue::Viewer,
        ));

        let policy2 = Policy::new("documents", PolicyAction::Select).with_where(PolicyExpr::Eq(
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
        let policy = Policy::new("test", PolicyAction::Select).with_where(PolicyExpr::And(vec![
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

    // ========== Evaluator Tests ==========

    use crate::sql::schema::{ColumnDef, ColumnType};

    /// Mock implementation for testing.
    struct MockLookup {
        schemas: HashMap<String, TableSchema>,
        rows: HashMap<(String, ObjectId), Row>,
        policies: HashMap<String, TablePolicies>,
    }

    impl MockLookup {
        fn new() -> Self {
            MockLookup {
                schemas: HashMap::new(),
                rows: HashMap::new(),
                policies: HashMap::new(),
            }
        }

        fn add_table(&mut self, name: &str, columns: Vec<(&str, ColumnType)>) {
            let cols: Vec<ColumnDef> = columns
                .into_iter()
                .map(|(n, ty)| ColumnDef::new(n, ty, true))
                .collect();
            self.schemas
                .insert(name.to_string(), TableSchema::new(name.to_string(), cols));
        }

        fn add_row(&mut self, table: &str, row: Row) {
            self.rows.insert((table.to_string(), row.id), row);
        }

        fn add_policy(&mut self, policy: Policy) {
            let table = policy.table.clone();
            self.policies
                .entry(table)
                .or_insert_with(TablePolicies::new)
                .add(policy)
                .unwrap();
        }
    }

    impl RowLookup for MockLookup {
        fn get_row(&self, table: &str, id: ObjectId) -> Option<Row> {
            self.rows.get(&(table.to_string(), id)).cloned()
        }

        fn get_schema(&self, table: &str) -> Option<TableSchema> {
            self.schemas.get(table).cloned()
        }
    }

    impl PolicyLookup for MockLookup {
        fn get_policies(&self, table: &str) -> Option<TablePolicies> {
            self.policies.get(table).cloned()
        }
    }

    #[test]
    fn test_eval_simple_select_owner() {
        clear_policy_warnings();

        let mut lookup = MockLookup::new();

        // Create users table
        lookup.add_table("users", vec![("name", ColumnType::String)]);

        // Create documents table with owner_id
        lookup.add_table(
            "documents",
            vec![
                ("title", ColumnType::String),
                ("owner_id", ColumnType::Ref("users".into())),
            ],
        );

        // Create a user
        let user_id = ObjectId::new(1);
        let other_user_id = ObjectId::new(2);
        lookup.add_row(
            "users",
            Row::new(user_id, vec![Value::String("Alice".into())]),
        );
        lookup.add_row(
            "users",
            Row::new(other_user_id, vec![Value::String("Bob".into())]),
        );

        // Create a document owned by user 1
        let doc_id = ObjectId::new(100);
        lookup.add_row(
            "documents",
            Row::new(
                doc_id,
                vec![Value::String("My Doc".into()), Value::Ref(user_id)],
            ),
        );

        // Add policy: owner can read
        lookup.add_policy(Policy::new("documents", PolicyAction::Select).with_where(
            PolicyExpr::Eq(PolicyValue::Column("owner_id".into()), PolicyValue::Viewer),
        ));

        let doc = lookup.get_row("documents", doc_id).unwrap();
        let config = PolicyConfig {
            warn_on_missing_policy: false,
            ..Default::default()
        };

        // User 1 (owner) can read
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, user_id, config.clone());
        let result = eval.check_select("documents", &doc);
        assert!(
            result.is_allowed(),
            "owner should be able to read: {:?}",
            result
        );

        // User 2 (not owner) cannot read
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, other_user_id, config);
        let result = eval.check_select("documents", &doc);
        assert!(
            result.is_denied(),
            "non-owner should not be able to read: {:?}",
            result
        );
    }

    #[test]
    fn test_eval_inherits() {
        clear_policy_warnings();

        let mut lookup = MockLookup::new();

        // Create users table
        lookup.add_table("users", vec![("name", ColumnType::String)]);

        // Create folders table
        lookup.add_table(
            "folders",
            vec![
                ("name", ColumnType::String),
                ("owner_id", ColumnType::Ref("users".into())),
            ],
        );

        // Create documents table
        lookup.add_table(
            "documents",
            vec![
                ("title", ColumnType::String),
                ("folder_id", ColumnType::Ref("folders".into())),
            ],
        );

        // Create users
        let alice_id = ObjectId::new(1);
        let bob_id = ObjectId::new(2);
        lookup.add_row(
            "users",
            Row::new(alice_id, vec![Value::String("Alice".into())]),
        );
        lookup.add_row("users", Row::new(bob_id, vec![Value::String("Bob".into())]));

        // Create a folder owned by Alice
        let folder_id = ObjectId::new(10);
        lookup.add_row(
            "folders",
            Row::new(
                folder_id,
                vec![Value::String("Alice's Folder".into()), Value::Ref(alice_id)],
            ),
        );

        // Create a document in that folder
        let doc_id = ObjectId::new(100);
        lookup.add_row(
            "documents",
            Row::new(
                doc_id,
                vec![Value::String("Doc in Folder".into()), Value::Ref(folder_id)],
            ),
        );

        // Add folder policy: owner can read
        lookup.add_policy(
            Policy::new("folders", PolicyAction::Select).with_where(PolicyExpr::Eq(
                PolicyValue::Column("owner_id".into()),
                PolicyValue::Viewer,
            )),
        );

        // Add document policy: inherit from folder
        lookup.add_policy(Policy::new("documents", PolicyAction::Select).with_where(
            PolicyExpr::Inherits {
                action: PolicyAction::Select,
                column: PolicyColumnRef::Current("folder_id".into()),
            },
        ));

        let doc = lookup.get_row("documents", doc_id).unwrap();
        let config = PolicyConfig {
            warn_on_missing_policy: false,
            ..Default::default()
        };

        // Alice (folder owner) can read the document
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, alice_id, config.clone());
        let result = eval.check_select("documents", &doc);
        assert!(
            result.is_allowed(),
            "folder owner should be able to read doc: {:?}",
            result
        );

        // Bob cannot read the document
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, bob_id, config);
        let result = eval.check_select("documents", &doc);
        assert!(
            result.is_denied(),
            "non-owner should not be able to read doc: {:?}",
            result
        );
    }

    #[test]
    fn test_eval_recursive_inherits() {
        clear_policy_warnings();

        let mut lookup = MockLookup::new();

        // Create users table
        lookup.add_table("users", vec![("name", ColumnType::String)]);

        // Create folders table with parent_id (self-referential)
        lookup.add_table(
            "folders",
            vec![
                ("name", ColumnType::String),
                ("parent_id", ColumnType::Ref("folders".into())),
                ("owner_id", ColumnType::Ref("users".into())),
            ],
        );

        // Create user
        let alice_id = ObjectId::new(1);
        let bob_id = ObjectId::new(2);
        lookup.add_row(
            "users",
            Row::new(alice_id, vec![Value::String("Alice".into())]),
        );
        lookup.add_row("users", Row::new(bob_id, vec![Value::String("Bob".into())]));

        // Create root folder owned by Alice
        let root_folder_id = ObjectId::new(10);
        lookup.add_row(
            "folders",
            Row::new(
                root_folder_id,
                vec![
                    Value::String("Root".into()),
                    Value::NullableNone, // no parent
                    Value::Ref(alice_id),
                ],
            ),
        );

        // Create child folder
        let child_folder_id = ObjectId::new(11);
        lookup.add_row(
            "folders",
            Row::new(
                child_folder_id,
                vec![
                    Value::String("Child".into()),
                    Value::Ref(root_folder_id), // parent is root
                    Value::NullableNone,                // no direct owner
                ],
            ),
        );

        // Create grandchild folder
        let grandchild_folder_id = ObjectId::new(12);
        lookup.add_row(
            "folders",
            Row::new(
                grandchild_folder_id,
                vec![
                    Value::String("Grandchild".into()),
                    Value::Ref(child_folder_id), // parent is child
                    Value::NullableNone,                 // no direct owner
                ],
            ),
        );

        // Add folder policy: owner OR inherit from parent
        lookup.add_policy(
            Policy::new("folders", PolicyAction::Select).with_where(PolicyExpr::Or(vec![
                PolicyExpr::Eq(PolicyValue::Column("owner_id".into()), PolicyValue::Viewer),
                PolicyExpr::Inherits {
                    action: PolicyAction::Select,
                    column: PolicyColumnRef::Current("parent_id".into()),
                },
            ])),
        );

        let grandchild = lookup.get_row("folders", grandchild_folder_id).unwrap();
        let config = PolicyConfig {
            warn_on_missing_policy: false,
            ..Default::default()
        };

        // Alice can read grandchild (via root -> child -> grandchild)
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, alice_id, config.clone());
        let result = eval.check_select("folders", &grandchild);
        assert!(
            result.is_allowed(),
            "root owner should be able to read grandchild: {:?}",
            result
        );

        // Bob cannot read grandchild
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, bob_id, config);
        let result = eval.check_select("folders", &grandchild);
        assert!(
            result.is_denied(),
            "non-owner should not be able to read grandchild: {:?}",
            result
        );
    }

    #[test]
    fn test_eval_insert_check() {
        clear_policy_warnings();

        let mut lookup = MockLookup::new();

        // Create users table
        lookup.add_table("users", vec![("name", ColumnType::String)]);

        // Create documents table
        lookup.add_table(
            "documents",
            vec![
                ("title", ColumnType::String),
                ("author_id", ColumnType::Ref("users".into())),
            ],
        );

        let alice_id = ObjectId::new(1);
        let bob_id = ObjectId::new(2);
        lookup.add_row(
            "users",
            Row::new(alice_id, vec![Value::String("Alice".into())]),
        );
        lookup.add_row("users", Row::new(bob_id, vec![Value::String("Bob".into())]));

        // Add INSERT policy: author must be viewer
        lookup.add_policy(Policy::new("documents", PolicyAction::Insert).with_check(
            PolicyExpr::Eq(
                PolicyValue::NewColumn("author_id".into()),
                PolicyValue::Viewer,
            ),
        ));

        let config = PolicyConfig {
            warn_on_missing_policy: false,
            ..Default::default()
        };

        // Alice can insert doc with herself as author
        let new_doc = Row::new(
            ObjectId::new(100),
            vec![Value::String("Alice's Doc".into()), Value::Ref(alice_id)],
        );
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, alice_id, config.clone());
        let result = eval.check_insert("documents", &new_doc);
        assert!(
            result.is_allowed(),
            "should allow insert with self as author: {:?}",
            result
        );

        // Alice cannot insert doc with Bob as author
        let new_doc = Row::new(
            ObjectId::new(101),
            vec![Value::String("Forged Doc".into()), Value::Ref(bob_id)],
        );
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, alice_id, config);
        let result = eval.check_insert("documents", &new_doc);
        assert!(
            result.is_denied(),
            "should deny insert with other as author: {:?}",
            result
        );
    }

    #[test]
    fn test_eval_update_where_and_check() {
        clear_policy_warnings();

        let mut lookup = MockLookup::new();

        // Create users table
        lookup.add_table("users", vec![("name", ColumnType::String)]);

        // Create documents table
        lookup.add_table(
            "documents",
            vec![
                ("title", ColumnType::String),
                ("author_id", ColumnType::Ref("users".into())),
            ],
        );

        let alice_id = ObjectId::new(1);
        let bob_id = ObjectId::new(2);
        lookup.add_row(
            "users",
            Row::new(alice_id, vec![Value::String("Alice".into())]),
        );
        lookup.add_row("users", Row::new(bob_id, vec![Value::String("Bob".into())]));

        let doc_id = ObjectId::new(100);
        lookup.add_row(
            "documents",
            Row::new(
                doc_id,
                vec![Value::String("Original".into()), Value::Ref(alice_id)],
            ),
        );

        // Add UPDATE policy: author can update, but cannot change author
        lookup.add_policy(
            Policy::new("documents", PolicyAction::Update)
                .with_where(PolicyExpr::Eq(
                    PolicyValue::Column("author_id".into()),
                    PolicyValue::Viewer,
                ))
                .with_check(PolicyExpr::Eq(
                    PolicyValue::NewColumn("author_id".into()),
                    PolicyValue::OldColumn("author_id".into()),
                )),
        );

        let old_doc = lookup.get_row("documents", doc_id).unwrap();
        let config = PolicyConfig {
            warn_on_missing_policy: false,
            ..Default::default()
        };

        // Alice can update title
        let new_doc = Row::new(
            doc_id,
            vec![
                Value::String("Updated".into()),
                Value::Ref(alice_id), // same author
            ],
        );
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, alice_id, config.clone());
        let result = eval.check_update("documents", &old_doc, &new_doc);
        assert!(
            result.is_allowed(),
            "author should be able to update title: {:?}",
            result
        );

        // Alice cannot change author to Bob
        let new_doc = Row::new(
            doc_id,
            vec![
                Value::String("Updated".into()),
                Value::Ref(bob_id), // changed author!
            ],
        );
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, alice_id, config.clone());
        let result = eval.check_update("documents", &old_doc, &new_doc);
        assert!(
            result.is_denied(),
            "should deny changing author: {:?}",
            result
        );

        // Bob cannot update at all
        let new_doc = Row::new(
            doc_id,
            vec![Value::String("Hacked".into()), Value::Ref(alice_id)],
        );
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, bob_id, config);
        let result = eval.check_update("documents", &old_doc, &new_doc);
        assert!(
            result.is_denied(),
            "non-author should not be able to update: {:?}",
            result
        );
    }

    #[test]
    fn test_eval_default_allow_without_policy() {
        clear_policy_warnings();

        let mut lookup = MockLookup::new();

        lookup.add_table("items", vec![("name", ColumnType::String)]);

        let item_id = ObjectId::new(1);
        lookup.add_row(
            "items",
            Row::new(item_id, vec![Value::String("Item".into())]),
        );

        // No policy defined
        let item = lookup.get_row("items", item_id).unwrap();
        let config = PolicyConfig {
            warn_on_missing_policy: false,
            ..Default::default()
        };

        let mut eval = PolicyEvaluator::new(&lookup, &lookup, ObjectId::new(999), config);
        let result = eval.check_select("items", &item);

        // Default allow
        assert!(result.is_allowed(), "should allow by default: {:?}", result);
    }

    #[test]
    fn test_eval_delete_falls_back_to_update() {
        clear_policy_warnings();

        let mut lookup = MockLookup::new();

        lookup.add_table("users", vec![("name", ColumnType::String)]);

        lookup.add_table(
            "items",
            vec![
                ("name", ColumnType::String),
                ("owner_id", ColumnType::Ref("users".into())),
            ],
        );

        let alice_id = ObjectId::new(1);
        let bob_id = ObjectId::new(2);
        lookup.add_row(
            "users",
            Row::new(alice_id, vec![Value::String("Alice".into())]),
        );

        let item_id = ObjectId::new(100);
        lookup.add_row(
            "items",
            Row::new(
                item_id,
                vec![Value::String("Item".into()), Value::Ref(alice_id)],
            ),
        );

        // Only add UPDATE policy, no DELETE policy
        lookup.add_policy(
            Policy::new("items", PolicyAction::Update).with_where(PolicyExpr::Eq(
                PolicyValue::Column("owner_id".into()),
                PolicyValue::Viewer,
            )),
        );

        let item = lookup.get_row("items", item_id).unwrap();
        let config = PolicyConfig {
            warn_on_missing_policy: false,
            ..Default::default()
        };

        // Alice can delete (falls back to UPDATE policy)
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, alice_id, config.clone());
        let result = eval.check_delete("items", &item);
        assert!(
            result.is_allowed(),
            "owner should be able to delete via UPDATE fallback: {:?}",
            result
        );

        // Bob cannot delete
        let mut eval = PolicyEvaluator::new(&lookup, &lookup, bob_id, config);
        let result = eval.check_delete("items", &item);
        assert!(
            result.is_denied(),
            "non-owner should not be able to delete: {:?}",
            result
        );
    }
}
