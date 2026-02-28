use super::*;
use serde::{Deserialize, Serialize};

/// Policy for a specific operation (SELECT, INSERT, UPDATE, DELETE).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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
pub struct TablePolicies {
    pub select: OperationPolicy,
    pub insert: OperationPolicy,
    pub update: OperationPolicy,
    pub delete: OperationPolicy,
}

impl TablePolicies {
    /// Create empty policies (allow all by default).
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
}
