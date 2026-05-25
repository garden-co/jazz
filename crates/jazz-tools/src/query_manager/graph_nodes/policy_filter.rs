//! Policy filter node for row-level security.
//!
//! Evaluates policy expressions against rows, filtering based on session context.
//! SELECT policies silently filter rows; write policies are handled separately.

use ahash::AHashSet;
use std::collections::HashSet;

use crate::metadata::RowProvenance;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::encoding::column_is_null;
use crate::query_manager::graph_nodes::policy_eval::{
    PolicyContextEvaluator, collect_policy_dependency_tables,
};
use crate::query_manager::permission_routing::{branch_main_name, branch_policy_scope};
use crate::query_manager::policy::{
    BranchPolicyContext, Operation, PolicyExpr, evaluate_expr_recursive,
    normalize_recursive_max_depth,
};
use crate::query_manager::session::Session;
use crate::query_manager::types::{
    ComposedBranchName, LoadedRow, PermissionPhase, Row, RowBytes, RowDescriptor, RowPolicyMode,
    Schema, TableName, TablePolicies, TableSchema, Tuple, TupleDelta, TupleElement,
};

use crate::storage::Storage;

use super::RowNode;

/// Policy filter node that evaluates row-level security policies.
///
/// For SELECT operations, rows that don't match the policy are silently filtered.
/// This node requires a session context to resolve @session references.
#[derive(Debug)]
pub struct PolicyFilterNode {
    descriptor: RowDescriptor,
    policy: PolicyExpr,
    policy_operation: Operation,
    session: Session,
    /// Schema for INHERITS lookups (resolving foreign key references).
    schema: Schema,
    /// Table name for this node (for INHERITS resolution).
    table_name: String,
    /// Branch name for index lookups.
    branch: String,
    row_policy_mode: RowPolicyMode,
    /// Initial recursion depth used for policy evaluation.
    initial_depth: usize,
    /// Current tuples that pass the policy.
    current_tuples: AHashSet<Tuple>,
    /// All current input tuples (including rows hidden by policy).
    input_tuples: AHashSet<Tuple>,
    dirty: bool,
    /// Whether the policy contains clauses that need graph-backed context evaluation.
    has_inherits: bool,
    /// Tables referenced by INHERITS / INHERITS REFERENCING / EXISTS clauses.
    inherits_tables: HashSet<String>,
    /// Whether any dependency table has changed.
    inherits_dirty: bool,
}

#[derive(Debug)]
pub(crate) struct PolicyFilterOptions {
    branch: String,
    initial_depth: usize,
    row_policy_mode: RowPolicyMode,
    policy_operation: Operation,
}

impl PolicyFilterOptions {
    pub(crate) fn for_branch(branch: impl Into<String>) -> Self {
        Self {
            branch: branch.into(),
            ..Self::default()
        }
    }

    pub(crate) fn with_initial_depth(mut self, initial_depth: usize) -> Self {
        self.initial_depth = initial_depth;
        self
    }

    pub(crate) fn with_row_policy_mode(mut self, row_policy_mode: RowPolicyMode) -> Self {
        self.row_policy_mode = row_policy_mode;
        self
    }

    pub(crate) fn with_policy_operation(mut self, policy_operation: Operation) -> Self {
        self.policy_operation = policy_operation;
        self
    }
}

impl Default for PolicyFilterOptions {
    fn default() -> Self {
        Self {
            branch: "main".to_string(),
            initial_depth: 0,
            row_policy_mode: RowPolicyMode::PermissiveLocal,
            policy_operation: Operation::Select,
        }
    }
}

struct BranchPolicyBacking<'a> {
    backing_table: &'a TableName,
    branch_policies: &'a TablePolicies,
    row_id: ObjectId,
    descriptor: RowDescriptor,
    content: RowBytes,
    provenance: RowProvenance,
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
        Self::new_with_options(
            descriptor,
            policy,
            session,
            schema,
            table_name,
            PolicyFilterOptions::default(),
        )
    }

    /// Create a new policy filter node with explicit branch.
    pub fn new_with_branch(
        descriptor: RowDescriptor,
        policy: PolicyExpr,
        session: Session,
        schema: Schema,
        table_name: impl Into<String>,
        branch: impl Into<String>,
    ) -> Self {
        Self::new_with_options(
            descriptor,
            policy,
            session,
            schema,
            table_name,
            PolicyFilterOptions::for_branch(branch),
        )
    }

    pub fn new_with_branch_and_policy_mode(
        descriptor: RowDescriptor,
        policy: PolicyExpr,
        session: Session,
        schema: Schema,
        table_name: impl Into<String>,
        branch: impl Into<String>,
        row_policy_mode: RowPolicyMode,
    ) -> Self {
        Self::new_with_options(
            descriptor,
            policy,
            session,
            schema,
            table_name,
            PolicyFilterOptions::for_branch(branch).with_row_policy_mode(row_policy_mode),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_branch_policy_mode_and_operation(
        descriptor: RowDescriptor,
        policy: PolicyExpr,
        session: Session,
        schema: Schema,
        table_name: impl Into<String>,
        branch: impl Into<String>,
        row_policy_mode: RowPolicyMode,
        policy_operation: Operation,
    ) -> Self {
        Self::new_with_options(
            descriptor,
            policy,
            session,
            schema,
            table_name,
            PolicyFilterOptions::for_branch(branch)
                .with_row_policy_mode(row_policy_mode)
                .with_policy_operation(policy_operation),
        )
    }

    /// Create a new policy filter node with explicit branch and initial recursion depth.
    pub fn new_with_branch_and_depth(
        descriptor: RowDescriptor,
        policy: PolicyExpr,
        session: Session,
        schema: Schema,
        table_name: impl Into<String>,
        branch: impl Into<String>,
        initial_depth: usize,
    ) -> Self {
        Self::new_with_options(
            descriptor,
            policy,
            session,
            schema,
            table_name,
            PolicyFilterOptions::for_branch(branch).with_initial_depth(initial_depth),
        )
    }

    pub(crate) fn new_with_options(
        descriptor: RowDescriptor,
        policy: PolicyExpr,
        session: Session,
        schema: Schema,
        table_name: impl Into<String>,
        options: PolicyFilterOptions,
    ) -> Self {
        let PolicyFilterOptions {
            branch,
            initial_depth,
            row_policy_mode,
            policy_operation,
        } = options;
        let table_name = table_name.into();
        let mut inherits_tables = collect_policy_dependency_tables(&policy, &descriptor);
        let (has_branch_policy, branch_dependency_tables) = branch_policy_dependency_tables(
            &schema,
            &descriptor,
            &table_name,
            &branch,
            policy_operation,
        );
        inherits_tables.extend(branch_dependency_tables);
        let has_inherits = has_branch_policy || !inherits_tables.is_empty();
        Self {
            descriptor,
            policy,
            policy_operation,
            session,
            schema,
            table_name,
            branch,
            row_policy_mode,
            initial_depth,
            current_tuples: AHashSet::new(),
            input_tuples: AHashSet::new(),
            dirty: true,
            has_inherits,
            inherits_tables,
            inherits_dirty: false,
        }
    }

    /// Returns true if this policy contains clauses requiring context evaluation.
    pub fn has_inherits(&self) -> bool {
        self.has_inherits
    }

    /// Returns tables that can affect policy outcome for this node.
    pub fn inherits_tables(&self) -> &HashSet<String> {
        &self.inherits_tables
    }

    /// Mark that a dependency table has changed.
    pub fn mark_inherits_dirty(&mut self) {
        self.inherits_dirty = true;
    }

    /// Process with context for INHERITS evaluation.
    /// Similar to ArraySubqueryNode::process_with_context().
    pub fn process_with_context<F>(
        &mut self,
        input: TupleDelta,
        io: &dyn Storage,
        mut row_loader: F,
    ) -> TupleDelta
    where
        F: FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    {
        let mut result = TupleDelta::default();

        // If dependency tables changed, re-check current visible tuples.
        // Keep processing incoming delta in the same call to avoid dropping it.
        if self.inherits_dirty {
            self.inherits_dirty = false;
            result = self.reevaluate_all_with_context(io, &mut row_loader);
        }

        if !self.dirty
            && input.added.is_empty()
            && input.removed.is_empty()
            && input.updated.is_empty()
        {
            return result;
        }

        // Process added tuples
        for tuple in input.added {
            self.input_tuples.insert(tuple.clone());
            let Some(row) = tuple_to_row(&tuple) else {
                continue;
            };

            if self.evaluate_with_context(&row, io, &mut row_loader) {
                self.current_tuples.insert(tuple.clone());
                result.added.push(tuple);
            }
        }

        // Process removed tuples
        for tuple in input.removed {
            self.input_tuples.remove(&tuple);
            if self.current_tuples.remove(&tuple) {
                result.removed.push(tuple);
            }
        }

        // Process updated tuples
        for (old_tuple, new_tuple) in input.updated {
            self.input_tuples.remove(&old_tuple);
            self.input_tuples.insert(new_tuple.clone());

            let old_row = tuple_to_row(&old_tuple);
            let new_row = tuple_to_row(&new_tuple);

            let old_passes = old_row
                .map(|r| self.evaluate_with_context(&r, io, &mut row_loader))
                .unwrap_or(false);
            let new_passes = new_row
                .map(|r| self.evaluate_with_context(&r, io, &mut row_loader))
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
    fn reevaluate_all_with_context<F>(&mut self, io: &dyn Storage, row_loader: &mut F) -> TupleDelta
    where
        F: FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    {
        let mut result = TupleDelta::default();
        let all_tuples: Vec<_> = self.input_tuples.iter().cloned().collect();

        for tuple in all_tuples {
            let passes = tuple_to_row(&tuple)
                .map(|row| self.evaluate_with_context(&row, io, row_loader))
                .unwrap_or(false);
            let currently_visible = self.current_tuples.contains(&tuple);

            match (currently_visible, passes) {
                (true, false) => {
                    self.current_tuples.remove(&tuple);
                    result.removed.push(tuple);
                }
                (false, true) => {
                    self.current_tuples.insert(tuple.clone());
                    result.added.push(tuple);
                }
                _ => {}
            }
        }

        self.dirty = false;
        result
    }

    /// Evaluate with context - supports recursive INHERITS and EXISTS evaluation.
    fn evaluate_with_context(
        &self,
        row: &Row,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> bool {
        if let Some(result) = self.evaluate_branch_scoped_with_context(row, io, row_loader) {
            return result;
        }

        let mut evaluator = PolicyContextEvaluator::new(
            &self.schema,
            &self.session,
            &self.branch,
            self.row_policy_mode,
        );
        let mut visited_referencing = HashSet::new();
        evaluator.evaluate_row_access(
            self.policy_operation,
            row,
            &self.descriptor,
            &self.table_name,
            Some(&self.policy),
            io,
            row_loader,
            self.initial_depth,
            &mut visited_referencing,
        )
    }

    fn evaluate_branch_scoped_with_context(
        &self,
        row: &Row,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> Option<bool> {
        let composed = branch_policy_scope(&BranchName::new(&self.branch))?;
        let target_table = TableName::new(&self.table_name);
        let target_schema = self.schema.get(&target_table)?;
        if target_schema.policies.for_branch.is_empty() {
            return Some(!self.row_policy_mode.denies_missing_explicit_policy());
        }

        let Some(backing) = self.resolve_branch_policy_backing(io, &composed, target_schema) else {
            return Some(false);
        };
        let policy = backing
            .branch_policies
            .policy_for_operation(self.policy_operation, PermissionPhase::Using);
        let Some(policy) = policy else {
            return Some(!self.row_policy_mode.denies_missing_explicit_policy());
        };

        let branch_context = BranchPolicyContext {
            table_name: backing.backing_table,
            row_id: backing.row_id,
            descriptor: &backing.descriptor,
            content: &backing.content,
            provenance: &backing.provenance,
        };
        let mut evaluator = PolicyContextEvaluator::new(
            &self.schema,
            &self.session,
            &self.branch,
            self.row_policy_mode,
        )
        .with_branch_context(&branch_context);
        let mut visited_referencing = HashSet::new();
        Some(evaluator.evaluate_row_access(
            self.policy_operation,
            row,
            &self.descriptor,
            &self.table_name,
            Some(policy),
            io,
            row_loader,
            self.initial_depth,
            &mut visited_referencing,
        ))
    }

    fn resolve_branch_policy_backing<'a>(
        &'a self,
        io: &dyn Storage,
        composed: &ComposedBranchName,
        target_schema: &'a TableSchema,
    ) -> Option<BranchPolicyBacking<'a>> {
        let Ok(branch_uuid) = uuid::Uuid::parse_str(&composed.user_branch) else {
            return None;
        };
        let branch_object_id = ObjectId::from_uuid(branch_uuid);
        let current_branch = branch_main_name(composed);

        for (backing_table, branch_policies) in &target_schema.policies.for_branch {
            let Ok(Some(backing_row)) = io.load_visible_region_row(
                backing_table.as_str(),
                current_branch.as_str(),
                branch_object_id,
            ) else {
                continue;
            };
            if backing_row.is_hard_deleted() {
                return None;
            }

            let backing_schema = self.schema.get(backing_table)?;
            let backing_provenance = backing_row.row_provenance();
            let backing_policy = backing_schema.policies.select_policy();
            let backing_allowed = if let Some(policy) = backing_policy {
                let backing_row_for_policy = Row::new(
                    branch_object_id,
                    backing_row.data.clone(),
                    backing_row.batch_id,
                    backing_provenance.clone(),
                );
                let mut evaluator = PolicyContextEvaluator::new(
                    &self.schema,
                    &self.session,
                    current_branch.as_str(),
                    self.row_policy_mode,
                );
                let mut visited_referencing = HashSet::new();
                let mut backing_dependency_loader =
                    |id: ObjectId, table_hint: Option<TableName>| -> Option<LoadedRow> {
                        let table_hint = table_hint?;
                        let Ok(Some(row)) = io.load_visible_region_row(
                            table_hint.as_str(),
                            current_branch.as_str(),
                            id,
                        ) else {
                            return None;
                        };
                        if row.is_hard_deleted() {
                            return None;
                        }
                        Some(LoadedRow::new(
                            row.data.clone(),
                            row.row_provenance(),
                            [(id, BranchName::new(row.branch.as_str()))]
                                .into_iter()
                                .collect(),
                            row.batch_id,
                        ))
                    };
                evaluator.evaluate_row_access(
                    Operation::Select,
                    &backing_row_for_policy,
                    &backing_schema.columns,
                    backing_table.as_str(),
                    Some(policy),
                    io,
                    &mut backing_dependency_loader,
                    0,
                    &mut visited_referencing,
                )
            } else {
                !self.row_policy_mode.denies_missing_explicit_policy()
            };
            if !backing_allowed {
                return None;
            }

            return Some(BranchPolicyBacking {
                backing_table,
                branch_policies,
                row_id: branch_object_id,
                descriptor: backing_schema.columns.clone(),
                content: backing_row.data.clone(),
                provenance: backing_provenance,
            });
        }

        None
    }

    /// Evaluate the policy expression against a row.
    pub fn evaluate(&self, row: &Row) -> bool {
        self.evaluate_expr(&self.policy, row, self.initial_depth)
    }

    /// Evaluate a policy expression with recursion depth tracking.
    ///
    /// Uses shared functions from policy.rs for basic expressions,
    /// handles INHERITS locally since it requires schema access.
    fn evaluate_expr(&self, expr: &PolicyExpr, row: &Row, depth: usize) -> bool {
        // Prevent infinite recursion in INHERITS
        if depth > crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return false;
        }

        match expr {
            // INHERITS requires schema access, so handle locally
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => self.evaluate_inherits(*operation, via_column, *max_depth, row, depth),
            PolicyExpr::InheritsReferencing { .. } => false, // Without context, fail closed.
            PolicyExpr::Exists { .. } => false,              // Without context, fail closed.
            PolicyExpr::ExistsRel { .. } => false,           // Without context, fail closed.

            // And/Or/Not need to recurse through this method for INHERITS support
            PolicyExpr::And(exprs) => exprs.iter().all(|e| self.evaluate_expr(e, row, depth)),
            PolicyExpr::Or(exprs) => exprs.iter().any(|e| self.evaluate_expr(e, row, depth)),
            PolicyExpr::Not(inner) => !self.evaluate_expr(inner, row, depth),

            // All other expressions delegate to shared evaluation
            _ => evaluate_expr_recursive(
                expr,
                &row.data,
                &row.provenance,
                &self.descriptor,
                &self.session,
                depth,
            ),
        }
    }

    /// Evaluate INHERITS without context - fails closed.
    ///
    /// INHERITS requires storage-backed access to load parent rows.
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
        max_depth: Option<usize>,
        row: &Row,
        depth: usize,
    ) -> bool {
        let Some(effective_max_depth) = normalize_recursive_max_depth(max_depth) else {
            return false;
        };
        if depth >= effective_max_depth {
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

fn branch_policy_dependency_tables(
    schema: &Schema,
    descriptor: &RowDescriptor,
    table_name: &str,
    branch: &str,
    policy_operation: Operation,
) -> (bool, HashSet<String>) {
    if branch_policy_scope(&BranchName::new(branch)).is_none() {
        return (false, HashSet::new());
    }

    let Some(table_schema) = schema.get(&TableName::new(table_name)) else {
        return (false, HashSet::new());
    };
    if table_schema.policies.for_branch.is_empty() {
        return (false, HashSet::new());
    }

    let mut dependency_tables = HashSet::new();
    for (backing_table, branch_policies) in &table_schema.policies.for_branch {
        if let Some(branch_policy) =
            branch_policies.policy_for_operation(policy_operation, PermissionPhase::Using)
        {
            dependency_tables.extend(collect_policy_dependency_tables(branch_policy, descriptor));
        }
        dependency_tables.insert(backing_table.as_str().to_string());
        if let Some(backing_select) = schema.get(backing_table).and_then(|backing_schema| {
            backing_schema
                .policies
                .select_policy()
                .map(|policy| collect_policy_dependency_tables(policy, &backing_schema.columns))
        }) {
            dependency_tables.extend(backing_select);
        }
    }

    (true, dependency_tables)
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
            self.input_tuples.insert(tuple.clone());
            let Some(row) = tuple_to_row(&tuple) else {
                continue;
            };
            if self.evaluate(&row) {
                self.current_tuples.insert(tuple.clone());
                result.added.push(tuple);
            }
        }

        // Process removed tuples
        for tuple in input.removed {
            self.input_tuples.remove(&tuple);
            if self.current_tuples.remove(&tuple) {
                result.removed.push(tuple);
            }
        }

        // Process updated tuples
        for (old_tuple, new_tuple) in input.updated {
            self.input_tuples.remove(&old_tuple);
            self.input_tuples.insert(new_tuple.clone());

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
            batch_id,
            row_provenance,
        } => Some(Row::new(
            *id,
            content.clone(),
            *batch_id,
            row_provenance.clone(),
        )),
        TupleElement::Id(_) => None, // Not materialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;
    use crate::query_manager::encoding::encode_row;
    use crate::query_manager::relation_ir::RelExpr;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, ComposedBranchName, SchemaHash, TableName, TablePolicies,
        TableSchema, Value,
    };
    use serde_json::json;
    use std::collections::HashMap;

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
        Row::new(
            ObjectId::new(),
            data,
            crate::row_histories::BatchId([0; 16]),
            crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        )
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

    #[test]
    fn test_policy_exists_fails_closed_without_context() {
        let session = Session::new("user1");
        let policy = PolicyExpr::Exists {
            table: "memberships".into(),
            condition: Box::new(PolicyExpr::True),
        };
        let node = PolicyFilterNode::new(
            test_descriptor(),
            policy,
            session,
            test_schema(),
            "documents",
        );

        let row = make_row("user1", "eng", "Doc 1");
        assert!(!node.evaluate(&row));
    }

    #[test]
    fn inherits_context_passes_parent_table_hint_to_loader() {
        let parent_id = ObjectId::new();
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor {
                name: "parent_id".into(),
                column_type: ColumnType::Uuid,
                nullable: false,
                references: Some(TableName::new("folders")),
                default: None,
                merge_strategy: None,
            },
            ColumnDescriptor::new("title", ColumnType::Text),
        ]);

        let mut schema = Schema::new();
        schema.insert(TableName::new("documents"), descriptor.clone().into());
        schema.insert(
            TableName::new("folders"),
            RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]).into(),
        );

        let node = PolicyFilterNode::new(
            descriptor.clone(),
            PolicyExpr::Inherits {
                operation: Operation::Select,
                via_column: "parent_id".into(),
                max_depth: Some(1),
            },
            Session::new("user1"),
            schema,
            "documents",
        );

        let data = encode_row(
            &descriptor,
            &[Value::Uuid(parent_id), Value::Text("Doc 1".into())],
        )
        .unwrap();
        let row = Row::new(
            ObjectId::new(),
            data,
            crate::row_histories::BatchId([0; 16]),
            crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        );
        let storage = crate::storage::MemoryStorage::new();
        let mut seen = Vec::new();

        let allowed = node.evaluate_with_context(&row, &storage, &mut |id, hint| {
            seen.push((id, hint));
            None
        });

        assert!(!allowed);
        assert_eq!(seen, vec![(parent_id, Some(TableName::new("folders")))]);
    }

    #[test]
    fn test_policy_exists_registers_dependency_table() {
        let session = Session::new("user1");
        let policy = PolicyExpr::Exists {
            table: "memberships".into(),
            condition: Box::new(PolicyExpr::True),
        };
        let node = PolicyFilterNode::new(
            test_descriptor(),
            policy,
            session,
            test_schema(),
            "documents",
        );

        assert!(node.has_inherits());
        assert!(node.inherits_tables().contains("memberships"));
    }

    #[test]
    fn branch_policy_filter_registers_backing_select_dependency_tables() {
        let mut doc_policies = TablePolicies::default();
        doc_policies.for_branch = HashMap::from([(
            TableName::new("branches"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("documents"),
            TableSchema::with_policies(test_descriptor(), doc_policies),
        );
        schema.insert(
            TableName::new("branches"),
            TableSchema::builder("branches")
                .column("owner_id", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::Exists {
                    table: "branch_access".into(),
                    condition: Box::new(PolicyExpr::True),
                }))
                .build(),
        );
        schema.insert(
            TableName::new("branch_access"),
            TableSchema::builder("branch_access")
                .column("owner_id", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::True))
                .build(),
        );
        let branch = ComposedBranchName::new(
            "dev",
            SchemaHash::compute(&schema),
            &ObjectId::new().to_string(),
        )
        .to_branch_name()
        .as_str()
        .to_string();

        let node = PolicyFilterNode::new_with_options(
            test_descriptor(),
            PolicyExpr::True,
            Session::new("user1"),
            schema,
            "documents",
            PolicyFilterOptions::for_branch(branch).with_row_policy_mode(RowPolicyMode::Enforcing),
        );

        assert!(node.inherits_tables().contains("branches"));
        assert!(node.inherits_tables().contains("branch_access"));
    }

    #[test]
    fn test_policy_exists_rel_fails_closed_without_context() {
        let session = Session::new("user1");
        let policy = PolicyExpr::ExistsRel {
            rel: RelExpr::TableScan {
                table: TableName::new("memberships"),
            },
        };
        let node = PolicyFilterNode::new(
            test_descriptor(),
            policy,
            session,
            test_schema(),
            "documents",
        );

        let row = make_row("user1", "eng", "Doc 1");
        assert!(!node.evaluate(&row));
    }

    #[test]
    fn test_policy_exists_rel_registers_dependency_table() {
        let session = Session::new("user1");
        let policy = PolicyExpr::ExistsRel {
            rel: RelExpr::TableScan {
                table: TableName::new("memberships"),
            },
        };
        let node = PolicyFilterNode::new(
            test_descriptor(),
            policy,
            session,
            test_schema(),
            "documents",
        );

        assert!(node.has_inherits());
        assert!(node.inherits_tables().contains("memberships"));
    }
}
