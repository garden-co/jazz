use super::*;
use crate::object::ObjectId;
use crate::query_manager::policy::{CmpOp, Operation, PolicyValue};
use crate::query_manager::relation_ir::{
    ColumnRef, PredicateCmpOp, PredicateExpr, RelExpr, ValueRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RowPolicyMode {
    #[default]
    PermissiveLocal,
    Enforcing,
}

impl RowPolicyMode {
    pub fn denies_missing_explicit_policy(self) -> bool {
        matches!(self, Self::Enforcing)
    }
}

/// Policy for a specific operation (SELECT, INSERT, UPDATE, DELETE).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct OperationPolicy {
    /// USING clause - filters rows for SELECT/UPDATE/DELETE.
    /// For SELECT: rows not matching are silently filtered out.
    /// For UPDATE/DELETE: rows not matching cannot be modified.
    pub using: Option<PolicyExpr>,
    /// WITH CHECK clause - validates new row data for INSERT/UPDATE.
    /// For INSERT: new row must satisfy this expression.
    /// For UPDATE: updated row must satisfy this expression.
    pub with_check: Option<PolicyExpr>,
}

impl OperationPolicy {
    /// Create a policy with just a USING clause.
    pub fn using(expr: PolicyExpr) -> Self {
        Self {
            using: Some(expr),
            with_check: None,
        }
    }

    /// Create a policy with just a WITH CHECK clause.
    pub fn with_check(expr: PolicyExpr) -> Self {
        Self {
            using: None,
            with_check: Some(expr),
        }
    }

    /// Create a policy with both USING and WITH CHECK clauses.
    pub fn using_and_check(using: PolicyExpr, check: PolicyExpr) -> Self {
        Self {
            using: Some(using),
            with_check: Some(check),
        }
    }
}

/// Policies for all operations on a table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TablePolicies {
    pub select: OperationPolicy,
    pub insert: OperationPolicy,
    pub update: OperationPolicy,
    pub delete: OperationPolicy,
}

impl TablePolicies {
    /// Create empty policies.
    ///
    /// Runtimes without a loaded policy bundle stay permissive locally; runtimes
    /// with a loaded bundle treat missing clauses as deny-by-default.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the SELECT policy (USING only).
    pub fn with_select(mut self, using: PolicyExpr) -> Self {
        self.select = OperationPolicy::using(using);
        self
    }

    /// Set the INSERT policy (WITH CHECK only).
    pub fn with_insert(mut self, with_check: PolicyExpr) -> Self {
        self.insert = OperationPolicy::with_check(with_check);
        self
    }

    /// Set the UPDATE policy (USING and/or WITH CHECK).
    pub fn with_update(mut self, using: Option<PolicyExpr>, with_check: PolicyExpr) -> Self {
        self.update = OperationPolicy {
            using,
            with_check: Some(with_check),
        };
        self
    }

    /// Set the DELETE policy (USING only).
    /// If not set, defaults to UPDATE's USING policy.
    pub fn with_delete(mut self, using: PolicyExpr) -> Self {
        self.delete = OperationPolicy::using(using);
        self
    }

    /// Get the effective DELETE USING policy.
    /// Falls back to UPDATE's USING if DELETE has none.
    pub fn effective_delete_using(&self) -> Option<&PolicyExpr> {
        self.delete.using.as_ref().or(self.update.using.as_ref())
    }

    pub fn has_any_explicit_policy(&self) -> bool {
        self.select.using.is_some()
            || self.insert.with_check.is_some()
            || self.update.using.is_some()
            || self.update.with_check.is_some()
            || self.delete.using.is_some()
    }

    pub fn select_policy(&self) -> Option<&PolicyExpr> {
        self.select.using.as_ref()
    }

    pub fn insert_policy(&self) -> Option<&PolicyExpr> {
        self.insert.with_check.as_ref()
    }

    pub fn update_using_policy(&self) -> Option<&PolicyExpr> {
        self.update.using.as_ref()
    }

    pub fn update_check_policy(&self) -> Option<&PolicyExpr> {
        self.update.with_check.as_ref()
    }

    pub fn has_explicit_update_policy(&self) -> bool {
        self.update.using.is_some() || self.update.with_check.is_some()
    }
}

/// Build table permissions with a TypeScript-DSL-like API.
///
/// ```
/// # use jazz_tools::query_manager::types::{permissions, policy_expr as expr};
/// let policies = permissions(|p| {
///     p.allow_read()
///         .where_(expr::eq("owner_id", expr::session("user_id")));
///     p.allow_update()
///         .where_old(expr::eq("owner_id", expr::session("user_id")))
///         .where_new(expr::eq("owner_id", expr::session("user_id")));
/// });
/// ```
pub fn permissions(build: impl FnOnce(&mut TablePolicyBuilder)) -> TablePolicies {
    let mut builder = TablePolicyBuilder::default();
    build(&mut builder);
    builder.build()
}

#[derive(Debug, Default)]
pub struct TablePolicyBuilder {
    policies: TablePolicies,
}

impl TablePolicyBuilder {
    pub fn allow_read(&mut self) -> ActionPolicyBuilder<'_> {
        ActionPolicyBuilder::new(&mut self.policies, PolicyAction::Read)
    }

    pub fn allow_insert(&mut self) -> ActionPolicyBuilder<'_> {
        ActionPolicyBuilder::new(&mut self.policies, PolicyAction::Insert)
    }

    pub fn allow_delete(&mut self) -> ActionPolicyBuilder<'_> {
        ActionPolicyBuilder::new(&mut self.policies, PolicyAction::Delete)
    }

    pub fn allow_update(&mut self) -> UpdatePolicyBuilder<'_> {
        UpdatePolicyBuilder::new(&mut self.policies)
    }

    pub fn build(self) -> TablePolicies {
        self.policies
    }
}

#[derive(Debug, Clone, Copy)]
enum PolicyAction {
    Read,
    Insert,
    Delete,
}

#[derive(Debug)]
pub struct ActionPolicyBuilder<'a> {
    policies: &'a mut TablePolicies,
    action: PolicyAction,
}

impl<'a> ActionPolicyBuilder<'a> {
    fn new(policies: &'a mut TablePolicies, action: PolicyAction) -> Self {
        Self { policies, action }
    }

    pub fn where_(self, expr: PolicyExpr) {
        match self.action {
            PolicyAction::Read => merge_expr(&mut self.policies.select.using, expr),
            PolicyAction::Insert => merge_expr(&mut self.policies.insert.with_check, expr),
            PolicyAction::Delete => merge_expr(&mut self.policies.delete.using, expr),
        }
    }

    pub fn always(self) {
        self.where_(PolicyExpr::True);
    }

    pub fn never(self) {
        self.where_(PolicyExpr::False);
    }
}

#[derive(Debug)]
pub struct UpdatePolicyBuilder<'a> {
    policies: &'a mut TablePolicies,
    using: Option<PolicyExpr>,
    with_check: Option<PolicyExpr>,
}

impl<'a> UpdatePolicyBuilder<'a> {
    fn new(policies: &'a mut TablePolicies) -> Self {
        Self {
            policies,
            using: None,
            with_check: None,
        }
    }

    pub fn where_(mut self, expr: PolicyExpr) {
        self.using = Some(expr.clone());
        self.with_check = Some(expr);
    }

    pub fn always(self) {
        self.where_(PolicyExpr::True);
    }

    pub fn never(self) {
        self.where_(PolicyExpr::False);
    }

    pub fn where_old(mut self, expr: PolicyExpr) -> Self {
        self.using = Some(expr);
        self
    }

    pub fn where_new(mut self, expr: PolicyExpr) -> Self {
        self.with_check = Some(expr);
        self
    }
}

impl Drop for UpdatePolicyBuilder<'_> {
    fn drop(&mut self) {
        let using = self
            .using
            .take()
            .or_else(|| self.with_check.as_ref().cloned());
        let with_check = self.with_check.take().or_else(|| using.as_ref().cloned());

        if let Some(expr) = using {
            merge_expr(&mut self.policies.update.using, expr);
        }
        if let Some(expr) = with_check {
            merge_expr(&mut self.policies.update.with_check, expr);
        }
    }
}

fn merge_expr(target: &mut Option<PolicyExpr>, expr: PolicyExpr) {
    *target = Some(match target.take() {
        Some(existing) => PolicyExpr::or(vec![existing, expr]),
        None => expr,
    });
}

pub mod policy_expr {
    use super::*;

    pub trait IntoSessionPath {
        fn into_session_path(self) -> Vec<String>;
    }

    impl IntoSessionPath for &str {
        fn into_session_path(self) -> Vec<String> {
            self.split('.').map(str::to_string).collect()
        }
    }

    impl IntoSessionPath for String {
        fn into_session_path(self) -> Vec<String> {
            self.split('.').map(str::to_string).collect()
        }
    }

    impl IntoSessionPath for Vec<String> {
        fn into_session_path(self) -> Vec<String> {
            self
        }
    }

    impl IntoSessionPath for Vec<&str> {
        fn into_session_path(self) -> Vec<String> {
            self.into_iter().map(str::to_string).collect()
        }
    }

    impl IntoSessionPath for &[&str] {
        fn into_session_path(self) -> Vec<String> {
            self.iter().map(|part| (*part).to_string()).collect()
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum PolicyValueInput {
        Literal(Value),
        Session(Vec<String>),
    }

    impl From<PolicyValueInput> for PolicyValue {
        fn from(value: PolicyValueInput) -> Self {
            match value {
                PolicyValueInput::Literal(value) => PolicyValue::Literal(value),
                PolicyValueInput::Session(path) => PolicyValue::SessionRef(path),
            }
        }
    }

    impl From<PolicyValue> for PolicyValueInput {
        fn from(value: PolicyValue) -> Self {
            match value {
                PolicyValue::Literal(value) => PolicyValueInput::Literal(value),
                PolicyValue::SessionRef(path) => PolicyValueInput::Session(path),
            }
        }
    }

    impl From<Value> for PolicyValueInput {
        fn from(value: Value) -> Self {
            PolicyValueInput::Literal(value)
        }
    }

    impl From<&str> for PolicyValueInput {
        fn from(value: &str) -> Self {
            PolicyValueInput::Literal(value.into())
        }
    }

    impl From<String> for PolicyValueInput {
        fn from(value: String) -> Self {
            PolicyValueInput::Literal(value.into())
        }
    }

    impl From<bool> for PolicyValueInput {
        fn from(value: bool) -> Self {
            PolicyValueInput::Literal(value.into())
        }
    }

    impl From<i32> for PolicyValueInput {
        fn from(value: i32) -> Self {
            PolicyValueInput::Literal(value.into())
        }
    }

    impl From<i64> for PolicyValueInput {
        fn from(value: i64) -> Self {
            PolicyValueInput::Literal(value.into())
        }
    }

    impl From<f64> for PolicyValueInput {
        fn from(value: f64) -> Self {
            PolicyValueInput::Literal(value.into())
        }
    }

    impl From<ObjectId> for PolicyValueInput {
        fn from(value: ObjectId) -> Self {
            PolicyValueInput::Literal(value.into())
        }
    }

    pub fn session(path: impl IntoSessionPath) -> PolicyValueInput {
        PolicyValueInput::Session(path.into_session_path())
    }

    pub fn literal(value: impl Into<Value>) -> PolicyValueInput {
        PolicyValueInput::Literal(value.into())
    }

    pub fn null() -> PolicyValueInput {
        PolicyValueInput::Literal(Value::Null)
    }

    pub fn always() -> PolicyExpr {
        PolicyExpr::True
    }

    pub fn never() -> PolicyExpr {
        PolicyExpr::False
    }

    pub fn eq(column: impl Into<String>, value: impl Into<PolicyValueInput>) -> PolicyExpr {
        cmp(column, CmpOp::Eq, value)
    }

    pub fn ne(column: impl Into<String>, value: impl Into<PolicyValueInput>) -> PolicyExpr {
        cmp(column, CmpOp::Ne, value)
    }

    pub fn cmp(
        column: impl Into<String>,
        op: CmpOp,
        value: impl Into<PolicyValueInput>,
    ) -> PolicyExpr {
        PolicyExpr::Cmp {
            column: column.into(),
            op,
            value: value.into().into(),
        }
    }

    pub fn is_null(column: impl Into<String>) -> PolicyExpr {
        PolicyExpr::IsNull {
            column: column.into(),
        }
    }

    pub fn is_not_null(column: impl Into<String>) -> PolicyExpr {
        PolicyExpr::IsNotNull {
            column: column.into(),
        }
    }

    pub fn contains(column: impl Into<String>, value: impl Into<PolicyValueInput>) -> PolicyExpr {
        PolicyExpr::Contains {
            column: column.into(),
            value: value.into().into(),
        }
    }

    pub fn in_session(column: impl Into<String>, path: impl IntoSessionPath) -> PolicyExpr {
        PolicyExpr::In {
            column: column.into(),
            session_path: path.into_session_path(),
        }
    }

    pub fn all_of(exprs: impl IntoIterator<Item = PolicyExpr>) -> PolicyExpr {
        PolicyExpr::and(exprs.into_iter().collect())
    }

    pub fn any_of(exprs: impl IntoIterator<Item = PolicyExpr>) -> PolicyExpr {
        PolicyExpr::or(exprs.into_iter().collect())
    }

    pub fn not(expr: PolicyExpr) -> PolicyExpr {
        PolicyExpr::not(expr)
    }

    /// Start a relation expression for `exists(table(...).where_(...))` policies.
    ///
    /// This mirrors the TypeScript `policy.exists(policy.some_table.where(...))`.
    /// Passing a normal policy expression to `where_` builds a plain table
    /// `EXISTS`; passing a relation predicate builds relation-backed `EXISTS`.
    pub fn table(table: impl Into<TableName>) -> Table {
        Table {
            table: table.into(),
        }
    }

    /// Build an `EXISTS` policy expression.
    pub fn exists(exists: impl IntoExistsExpr) -> PolicyExpr {
        exists.into_exists_expr()
    }

    pub trait IntoExistsExpr {
        fn into_exists_expr(self) -> PolicyExpr;
    }

    pub trait IntoTableWhere {
        type Output;

        fn into_table_where(self, table: TableName) -> Self::Output;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Table {
        table: TableName,
    }

    impl Table {
        pub fn where_<W: IntoTableWhere>(self, condition: W) -> W::Output {
            condition.into_table_where(self.table)
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct TableExists {
        table: TableName,
        condition: PolicyExpr,
    }

    impl IntoTableWhere for PolicyExpr {
        type Output = TableExists;

        fn into_table_where(self, table: TableName) -> Self::Output {
            TableExists {
                table,
                condition: self,
            }
        }
    }

    impl IntoExistsExpr for TableExists {
        fn into_exists_expr(self) -> PolicyExpr {
            PolicyExpr::Exists {
                table: self.table.as_str().to_string(),
                condition: Box::new(self.condition),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Relation {
        rel: RelExpr,
    }

    impl Relation {
        fn new(rel: RelExpr) -> Self {
            Self { rel }
        }

        pub fn where_(self, predicate: PredicateExpr) -> Self {
            Self::new(RelExpr::Filter {
                input: Box::new(self.rel),
                predicate,
            })
        }

        pub fn into_rel_expr(self) -> RelExpr {
            self.rel
        }
    }

    impl IntoTableWhere for PredicateExpr {
        type Output = Relation;

        fn into_table_where(self, table: TableName) -> Self::Output {
            Relation::new(RelExpr::Filter {
                input: Box::new(RelExpr::TableScan { table }),
                predicate: self,
            })
        }
    }

    impl From<RelExpr> for Relation {
        fn from(rel: RelExpr) -> Self {
            Self::new(rel)
        }
    }

    impl IntoExistsExpr for Relation {
        fn into_exists_expr(self) -> PolicyExpr {
            PolicyExpr::ExistsRel {
                rel: self.into_rel_expr(),
            }
        }
    }

    impl IntoExistsExpr for RelExpr {
        fn into_exists_expr(self) -> PolicyExpr {
            PolicyExpr::ExistsRel { rel: self }
        }
    }

    pub fn allowed_to_read(via_column: impl Into<String>) -> PolicyExpr {
        allowed_to(Operation::Select, via_column)
    }

    pub fn allowed_to_insert(via_column: impl Into<String>) -> PolicyExpr {
        allowed_to(Operation::Insert, via_column)
    }

    pub fn allowed_to_update(via_column: impl Into<String>) -> PolicyExpr {
        allowed_to(Operation::Update, via_column)
    }

    pub fn allowed_to_delete(via_column: impl Into<String>) -> PolicyExpr {
        allowed_to(Operation::Delete, via_column)
    }

    pub fn allowed_to_read_with_depth(
        via_column: impl Into<String>,
        max_depth: usize,
    ) -> PolicyExpr {
        allowed_to_with_depth(Operation::Select, via_column, max_depth)
    }

    pub fn allowed_to_update_with_depth(
        via_column: impl Into<String>,
        max_depth: usize,
    ) -> PolicyExpr {
        allowed_to_with_depth(Operation::Update, via_column, max_depth)
    }

    pub fn allowed_to(operation: Operation, via_column: impl Into<String>) -> PolicyExpr {
        PolicyExpr::Inherits {
            operation,
            via_column: via_column.into(),
            max_depth: None,
        }
    }

    pub fn allowed_to_with_depth(
        operation: Operation,
        via_column: impl Into<String>,
        max_depth: usize,
    ) -> PolicyExpr {
        PolicyExpr::Inherits {
            operation,
            via_column: via_column.into(),
            max_depth: Some(max_depth),
        }
    }

    pub fn allowed_to_read_referencing(
        source_table: impl Into<String>,
        via_column: impl Into<String>,
    ) -> PolicyExpr {
        allowed_to_referencing(Operation::Select, source_table, via_column)
    }

    pub fn allowed_to_update_referencing(
        source_table: impl Into<String>,
        via_column: impl Into<String>,
    ) -> PolicyExpr {
        allowed_to_referencing(Operation::Update, source_table, via_column)
    }

    pub fn allowed_to_referencing(
        operation: Operation,
        source_table: impl Into<String>,
        via_column: impl Into<String>,
    ) -> PolicyExpr {
        PolicyExpr::InheritsReferencing {
            operation,
            source_table: source_table.into(),
            via_column: via_column.into(),
            max_depth: None,
        }
    }

    pub mod rel {
        use super::*;

        pub fn eq_session(column: impl Into<String>, path: impl IntoSessionPath) -> PredicateExpr {
            cmp(
                column,
                PredicateCmpOp::Eq,
                ValueRef::SessionRef(path.into_session_path()),
            )
        }

        pub fn eq_outer(
            column: impl Into<String>,
            outer_column: impl Into<String>,
        ) -> PredicateExpr {
            cmp(
                column,
                PredicateCmpOp::Eq,
                ValueRef::OuterColumn(ColumnRef::unscoped(outer_column)),
            )
        }

        pub fn eq_literal(column: impl Into<String>, value: impl Into<Value>) -> PredicateExpr {
            cmp(column, PredicateCmpOp::Eq, ValueRef::Literal(value.into()))
        }

        pub fn is_null(column: impl Into<String>) -> PredicateExpr {
            PredicateExpr::IsNull {
                column: ColumnRef::unscoped(column),
            }
        }

        pub fn all_of(exprs: impl IntoIterator<Item = PredicateExpr>) -> PredicateExpr {
            let exprs = exprs.into_iter().collect::<Vec<_>>();
            match exprs.len() {
                0 => PredicateExpr::True,
                1 => exprs.into_iter().next().unwrap(),
                _ => PredicateExpr::And(exprs),
            }
        }

        pub fn any_of(exprs: impl IntoIterator<Item = PredicateExpr>) -> PredicateExpr {
            let exprs = exprs.into_iter().collect::<Vec<_>>();
            match exprs.len() {
                0 => PredicateExpr::False,
                1 => exprs.into_iter().next().unwrap(),
                _ => PredicateExpr::Or(exprs),
            }
        }

        pub fn cmp(
            column: impl Into<String>,
            op: PredicateCmpOp,
            right: ValueRef,
        ) -> PredicateExpr {
            PredicateExpr::Cmp {
                left: ColumnRef::unscoped(column),
                op,
                right,
            }
        }
    }
}
