//! Policy filter node for row-level security.
//!
//! Evaluates policy expressions against rows, filtering based on session context.
//! SELECT policies silently filter rows; write policies are handled separately.

use ahash::AHashSet;
use std::collections::{HashMap, HashSet};

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::object_manager::ObjectManager;
use crate::query_manager::encoding::{column_is_null, decode_column};
use crate::query_manager::index::BTreeIndex;
use crate::query_manager::policy::{Operation, PolicyExpr, evaluate_expr_recursive};
use crate::query_manager::policy_graph::PolicyGraph;
use crate::query_manager::session::Session;
use crate::query_manager::types::{
    Row, RowDescriptor, Schema, Tuple, TupleDelta, TupleElement, Value,
};

use super::RowNode;

/// Policy filter node that evaluates row-level security policies.
///
/// For SELECT operations, rows that don't match the policy are silently filtered.
/// This node requires a session context to resolve @session references.
#[derive(Debug)]
pub struct PolicyFilterNode {
    descriptor: RowDescriptor,
    policy: PolicyExpr,
    session: Session,
    /// Schema for INHERITS lookups (resolving foreign key references).
    schema: Schema,
    /// Table name for this node (for INHERITS resolution).
    table_name: String,
    /// Current tuples that pass the policy.
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
    /// Whether the policy contains INHERITS clauses that need context for evaluation.
    has_inherits: bool,
    /// Tables referenced by INHERITS clauses (for dirty tracking).
    inherits_tables: HashSet<String>,
    /// Whether any INHERITS-referenced table has changed.
    inherits_dirty: bool,
}

impl PolicyFilterNode {
    /// Create a new policy filter node.
    pub fn new(
        descriptor: RowDescriptor,
        policy: PolicyExpr,
        session: Session,
        schema: Schema,
        table_name: impl Into<String>,
    ) -> Self {
        let table_name = table_name.into();
        let inherits_tables = collect_inherits_tables(&policy, &descriptor);
        let has_inherits = !inherits_tables.is_empty();
        Self {
            descriptor,
            policy,
            session,
            schema,
            table_name,
            current_tuples: AHashSet::new(),
            dirty: true,
            has_inherits,
            inherits_tables,
            inherits_dirty: false,
        }
    }

    /// Returns true if this policy contains INHERITS clauses.
    pub fn has_inherits(&self) -> bool {
        self.has_inherits
    }

    /// Returns the tables referenced by INHERITS clauses.
    pub fn inherits_tables(&self) -> &HashSet<String> {
        &self.inherits_tables
    }

    /// Mark that an INHERITS-referenced table has changed.
    pub fn mark_inherits_dirty(&mut self) {
        self.inherits_dirty = true;
    }

    /// Process with context for INHERITS evaluation.
    /// Similar to ArraySubqueryNode::process_with_context().
    pub fn process_with_context<F>(
        &mut self,
        input: TupleDelta,
        indices: &HashMap<(String, String), BTreeIndex>,
        om: &ObjectManager,
        mut row_loader: F,
    ) -> TupleDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        // If inherits tables changed, we need to reevaluate all current tuples
        if self.inherits_dirty {
            self.inherits_dirty = false;
            return self.reevaluate_all_with_context(indices, om, &mut row_loader);
        }

        if !self.dirty
            && input.added.is_empty()
            && input.removed.is_empty()
            && input.updated.is_empty()
        {
            return TupleDelta::default();
        }

        let mut result = TupleDelta::default();

        // Process added tuples
        for tuple in input.added {
            if let Some(row) = tuple_to_row(&tuple)
                && self.evaluate_with_context(&row, indices, om, &mut row_loader)
            {
                self.current_tuples.insert(tuple.clone());
                result.added.push(tuple);
            }
        }

        // Process removed tuples
        for tuple in input.removed {
            if self.current_tuples.remove(&tuple) {
                result.removed.push(tuple);
            }
        }

        // Process updated tuples
        for (old_tuple, new_tuple) in input.updated {
            let old_row = tuple_to_row(&old_tuple);
            let new_row = tuple_to_row(&new_tuple);

            let old_passes = old_row
                .map(|r| self.evaluate_with_context(&r, indices, om, &mut row_loader))
                .unwrap_or(false);
            let new_passes = new_row
                .map(|r| self.evaluate_with_context(&r, indices, om, &mut row_loader))
                .unwrap_or(false);

            match (old_passes, new_passes) {
                (true, true) => {
                    self.current_tuples.remove(&old_tuple);
                    self.current_tuples.insert(new_tuple.clone());
                    result.updated.push((old_tuple, new_tuple));
                }
                (true, false) => {
                    self.current_tuples.remove(&old_tuple);
                    result.removed.push(old_tuple);
                }
                (false, true) => {
                    self.current_tuples.insert(new_tuple.clone());
                    result.added.push(new_tuple);
                }
                (false, false) => {}
            }
        }

        self.dirty = false;
        result
    }

    /// Re-evaluate all current tuples when INHERITS-referenced tables change.
    fn reevaluate_all_with_context<F>(
        &mut self,
        indices: &HashMap<(String, String), BTreeIndex>,
        om: &ObjectManager,
        row_loader: &mut F,
    ) -> TupleDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let mut result = TupleDelta::default();
        let old_tuples: Vec<_> = self.current_tuples.iter().cloned().collect();

        for tuple in old_tuples {
            if let Some(row) = tuple_to_row(&tuple) {
                let still_passes = self.evaluate_with_context(&row, indices, om, row_loader);
                if !still_passes {
                    self.current_tuples.remove(&tuple);
                    result.removed.push(tuple);
                }
            }
        }

        self.dirty = false;
        result
    }

    /// Evaluate with context - uses PolicyGraph for INHERITS evaluation.
    fn evaluate_with_context(
        &self,
        row: &Row,
        indices: &HashMap<(String, String), BTreeIndex>,
        om: &ObjectManager,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    ) -> bool {
        self.evaluate_expr_with_context(&self.policy, row, indices, om, row_loader, 0)
    }

    /// Evaluate a policy expression with context for INHERITS.
    /// Uses trait object for row_loader to avoid generic recursion limit.
    fn evaluate_expr_with_context(
        &self,
        expr: &PolicyExpr,
        row: &Row,
        indices: &HashMap<(String, String), BTreeIndex>,
        om: &ObjectManager,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
        depth: usize,
    ) -> bool {
        if depth > 32 {
            return false;
        }

        match expr {
            PolicyExpr::Inherits {
                operation,
                via_column,
            } => self.evaluate_inherits_with_context(
                *operation, via_column, row, indices, om, row_loader, depth,
            ),
            PolicyExpr::And(exprs) => exprs
                .iter()
                .all(|e| self.evaluate_expr_with_context(e, row, indices, om, row_loader, depth)),
            PolicyExpr::Or(exprs) => exprs
                .iter()
                .any(|e| self.evaluate_expr_with_context(e, row, indices, om, row_loader, depth)),
            PolicyExpr::Not(inner) => {
                !self.evaluate_expr_with_context(inner, row, indices, om, row_loader, depth)
            }
            // All other expressions delegate to shared evaluation
            _ => evaluate_expr_recursive(expr, &row.data, &self.descriptor, &self.session, depth),
        }
    }

    /// Evaluate INHERITS by creating and settling a PolicyGraph for the parent row.
    /// Uses trait object for row_loader to avoid generic recursion limit.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_inherits_with_context(
        &self,
        operation: Operation,
        via_column: &str,
        row: &Row,
        indices: &HashMap<(String, String), BTreeIndex>,
        om: &ObjectManager,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
        depth: usize,
    ) -> bool {
        // Depth limit to prevent infinite recursion
        if depth >= 32 {
            return false;
        }

        // Get the FK column index
        let col_index = match self.descriptor.column_index(via_column) {
            Some(idx) => idx,
            None => return false,
        };

        // Check if FK is NULL - if so, INHERITS passes (no parent to check)
        if column_is_null(&self.descriptor, &row.data, col_index).unwrap_or(false) {
            return true;
        }

        // Get the FK column descriptor to find the referenced table
        let col_desc = &self.descriptor.columns[col_index];
        let parent_table = match &col_desc.references {
            Some(table) => table,
            None => return false, // No FK reference - schema error
        };

        // Self-INHERITS check: disallow for now
        if parent_table.as_str() == self.table_name {
            // Self-referential INHERITS not yet supported
            return false;
        }

        // Decode the FK value to get the parent ObjectId
        let parent_id = match decode_column(&self.descriptor, &row.data, col_index) {
            Ok(Value::Uuid(id)) => id,
            _ => return false,
        };

        // Get the parent table's schema
        let parent_table_name = *parent_table;
        let parent_schema = match self.schema.get(&parent_table_name) {
            Some(schema) => schema,
            None => return false,
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

        // Create a PolicyGraph to evaluate the parent's policy
        let mut graph = match PolicyGraph::for_inherits(
            &parent_table_name,
            parent_id,
            parent_policy,
            &self.session,
            &self.schema,
        ) {
            Some(g) => g,
            None => return false,
        };

        // Settle the graph until complete
        for _ in 0..100 {
            // Max iterations to prevent infinite loop
            if graph.settle(indices, om, row_loader) {
                break;
            }
        }

        graph.result()
    }

    /// Evaluate the policy expression against a row.
    pub fn evaluate(&self, row: &Row) -> bool {
        self.evaluate_expr(&self.policy, row, 0)
    }

    /// Evaluate a policy expression with recursion depth tracking.
    ///
    /// Uses shared functions from policy.rs for basic expressions,
    /// handles INHERITS locally since it requires schema access.
    fn evaluate_expr(&self, expr: &PolicyExpr, row: &Row, depth: usize) -> bool {
        // Prevent infinite recursion in INHERITS
        if depth > 32 {
            return false;
        }

        match expr {
            // INHERITS requires schema access, so handle locally
            PolicyExpr::Inherits {
                operation,
                via_column,
            } => self.evaluate_inherits(*operation, via_column, row, depth),

            // And/Or/Not need to recurse through this method for INHERITS support
            PolicyExpr::And(exprs) => exprs.iter().all(|e| self.evaluate_expr(e, row, depth)),
            PolicyExpr::Or(exprs) => exprs.iter().any(|e| self.evaluate_expr(e, row, depth)),
            PolicyExpr::Not(inner) => !self.evaluate_expr(inner, row, depth),

            // All other expressions delegate to shared evaluation
            _ => evaluate_expr_recursive(expr, &row.data, &self.descriptor, &self.session, depth),
        }
    }

    /// Evaluate INHERITS without context - fails closed.
    ///
    /// INHERITS requires ObjectManager access to load parent rows.
    /// When called without context (via regular process()), we fail closed
    /// for security. Use process_with_context() for proper INHERITS evaluation.
    ///
    /// - NULL FK: returns true (row has no parent, so INHERITS passes)
    /// - Non-NULL FK without context: returns false (fail closed)
    #[allow(unused_variables)]
    fn evaluate_inherits(
        &self,
        operation: Operation,
        via_column: &str,
        row: &Row,
        depth: usize,
    ) -> bool {
        // Depth limit to prevent infinite recursion
        if depth >= 32 {
            return false;
        }

        // Get the FK column index
        let col_index = match self.descriptor.column_index(via_column) {
            Some(idx) => idx,
            None => return false, // Column not found
        };

        // Check if FK is NULL - if so, INHERITS passes (no parent to check)
        if column_is_null(&self.descriptor, &row.data, col_index).unwrap_or(false) {
            return true;
        }

        // Non-NULL FK but no context - fail closed for security.
        // The graph settlement loop should use process_with_context() for PolicyFilters
        // that have INHERITS clauses.
        false
    }
}

/// Collect all tables referenced by INHERITS clauses in a policy expression.
/// Traverses the policy tree recursively to find all INHERITS via_column references
/// and resolves them to their target tables through the descriptor's FK references.
fn collect_inherits_tables(policy: &PolicyExpr, descriptor: &RowDescriptor) -> HashSet<String> {
    let mut tables = HashSet::new();
    collect_inherits_tables_recursive(policy, descriptor, &mut tables);
    tables
}

fn collect_inherits_tables_recursive(
    policy: &PolicyExpr,
    descriptor: &RowDescriptor,
    tables: &mut HashSet<String>,
) {
    match policy {
        PolicyExpr::Inherits { via_column, .. } => {
            // Look up the FK column to find the referenced table
            if let Some(col_index) = descriptor.column_index(via_column)
                && let Some(ref references) = descriptor.columns[col_index].references
            {
                tables.insert(references.as_str().to_string());
            }
        }
        PolicyExpr::And(exprs) | PolicyExpr::Or(exprs) => {
            for expr in exprs {
                collect_inherits_tables_recursive(expr, descriptor, tables);
            }
        }
        PolicyExpr::Not(inner) => {
            collect_inherits_tables_recursive(inner, descriptor, tables);
        }
        _ => {}
    }
}

impl RowNode for PolicyFilterNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        if !self.dirty
            && input.added.is_empty()
            && input.removed.is_empty()
            && input.updated.is_empty()
        {
            return TupleDelta::default();
        }

        let mut result = TupleDelta::default();

        // Process added tuples
        for tuple in input.added {
            if let Some(row) = tuple_to_row(&tuple)
                && self.evaluate(&row)
            {
                self.current_tuples.insert(tuple.clone());
                result.added.push(tuple);
            }
        }

        // Process removed tuples
        for tuple in input.removed {
            if self.current_tuples.remove(&tuple) {
                result.removed.push(tuple);
            }
        }

        // Process updated tuples
        for (old_tuple, new_tuple) in input.updated {
            let old_row = tuple_to_row(&old_tuple);
            let new_row = tuple_to_row(&new_tuple);

            let old_passes = old_row.map(|r| self.evaluate(&r)).unwrap_or(false);
            let new_passes = new_row.map(|r| self.evaluate(&r)).unwrap_or(false);

            match (old_passes, new_passes) {
                (true, true) => {
                    // Both pass: update
                    self.current_tuples.remove(&old_tuple);
                    self.current_tuples.insert(new_tuple.clone());
                    result.updated.push((old_tuple, new_tuple));
                }
                (true, false) => {
                    // Was visible, now hidden: remove
                    self.current_tuples.remove(&old_tuple);
                    result.removed.push(old_tuple);
                }
                (false, true) => {
                    // Was hidden, now visible: add
                    self.current_tuples.insert(new_tuple.clone());
                    result.added.push(new_tuple);
                }
                (false, false) => {
                    // Neither passes: no change in output
                }
            }
        }

        self.dirty = false;
        result
    }

    fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}

/// Extract a Row from a Tuple (assumes single materialized element).
fn tuple_to_row(tuple: &Tuple) -> Option<Row> {
    if tuple.0.is_empty() {
        return None;
    }

    match &tuple.0[0] {
        TupleElement::Row {
            id,
            content,
            commit_id,
        } => Some(Row::new(*id, content.clone(), *commit_id)),
        TupleElement::Id(_) => None, // Not materialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitId;
    use crate::object::ObjectId;
    use crate::query_manager::encoding::encode_row;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, Value};
    use serde_json::json;

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("owner_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Text),
            ColumnDescriptor::new("title", ColumnType::Text),
        ])
    }

    fn make_row(owner: &str, team: &str, title: &str) -> Row {
        let desc = test_descriptor();
        let data = encode_row(
            &desc,
            &[
                Value::Text(owner.into()),
                Value::Text(team.into()),
                Value::Text(title.into()),
            ],
        )
        .unwrap();
        Row::new(ObjectId::new(), data, CommitId([0; 32]))
    }

    fn test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            crate::query_manager::types::TableName::new("documents"),
            test_descriptor().into(),
        );
        schema
    }

    #[test]
    fn test_policy_true() {
        let session = Session::new("user1");
        let node = PolicyFilterNode::new(
            test_descriptor(),
            PolicyExpr::True,
            session,
            test_schema(),
            "documents",
        );

        let row = make_row("user1", "eng", "Doc 1");
        assert!(node.evaluate(&row));
    }

    #[test]
    fn test_policy_false() {
        let session = Session::new("user1");
        let node = PolicyFilterNode::new(
            test_descriptor(),
            PolicyExpr::False,
            session,
            test_schema(),
            "documents",
        );

        let row = make_row("user1", "eng", "Doc 1");
        assert!(!node.evaluate(&row));
    }

    #[test]
    fn test_policy_eq_session_user_id() {
        let session = Session::new("user1");
        let policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
        let node = PolicyFilterNode::new(
            test_descriptor(),
            policy,
            session,
            test_schema(),
            "documents",
        );

        // Owner matches session user_id
        let row1 = make_row("user1", "eng", "Doc 1");
        assert!(node.evaluate(&row1));

        // Owner doesn't match
        let row2 = make_row("user2", "eng", "Doc 2");
        assert!(!node.evaluate(&row2));
    }

    #[test]
    fn test_policy_in_session_array() {
        let session = Session::new("user1").with_claims(json!({"teams": ["eng", "design"]}));

        let policy = PolicyExpr::in_session("team_id", vec!["claims".into(), "teams".into()]);
        let node = PolicyFilterNode::new(
            test_descriptor(),
            policy,
            session,
            test_schema(),
            "documents",
        );

        // Team is in session teams
        let row1 = make_row("user1", "eng", "Doc 1");
        assert!(node.evaluate(&row1));

        let row2 = make_row("user1", "design", "Doc 2");
        assert!(node.evaluate(&row2));

        // Team not in session teams
        let row3 = make_row("user1", "sales", "Doc 3");
        assert!(!node.evaluate(&row3));
    }

    #[test]
    fn test_policy_or() {
        let session = Session::new("user1").with_claims(json!({"teams": ["eng"]}));

        // owner_id = @session.user_id OR team_id IN @session.claims.teams
        let policy = PolicyExpr::or(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            PolicyExpr::in_session("team_id", vec!["claims".into(), "teams".into()]),
        ]);

        let node = PolicyFilterNode::new(
            test_descriptor(),
            policy,
            session,
            test_schema(),
            "documents",
        );

        // Owned by user1
        let row1 = make_row("user1", "sales", "Doc 1");
        assert!(node.evaluate(&row1));

        // In user's team
        let row2 = make_row("user2", "eng", "Doc 2");
        assert!(node.evaluate(&row2));

        // Neither owned nor in team
        let row3 = make_row("user2", "sales", "Doc 3");
        assert!(!node.evaluate(&row3));
    }

    #[test]
    fn test_policy_and() {
        let session = Session::new("user1").with_claims(json!({"teams": ["eng"]}));

        // owner_id = @session.user_id AND team_id IN @session.claims.teams
        let policy = PolicyExpr::and(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            PolicyExpr::in_session("team_id", vec!["claims".into(), "teams".into()]),
        ]);

        let node = PolicyFilterNode::new(
            test_descriptor(),
            policy,
            session,
            test_schema(),
            "documents",
        );

        // Both conditions met
        let row1 = make_row("user1", "eng", "Doc 1");
        assert!(node.evaluate(&row1));

        // Only owned
        let row2 = make_row("user1", "sales", "Doc 2");
        assert!(!node.evaluate(&row2));

        // Only in team
        let row3 = make_row("user2", "eng", "Doc 3");
        assert!(!node.evaluate(&row3));
    }
}
