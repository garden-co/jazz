use ahash::AHashMap;
use std::collections::HashMap;
use std::sync::Arc;

use crate::commit::{CommitId, StoredState};
use crate::object::{BranchName, ObjectId, ObjectState};
use crate::object_manager::AllObjectUpdate;
use crate::sync_manager::{PendingPermissionCheck, PendingUpdateId, SyncManager};

use super::encoding::{decode_row, encode_row};
use super::graph::QueryGraph;
use super::graph_nodes::output::QuerySubscriptionId;
use super::index::{BTreeIndex, IndexError};
use super::policy::{ComplexClause, Operation, evaluate_simple_parts, resolve_session_value};
use super::policy_graph::PolicyGraph;
use super::query::{Query, QueryBuilder};
use super::session::Session;
use super::types::{Row, RowDelta, RowDescriptor, Schema, TableName, TableSchema, Value};

/// Row branch name (all row data goes on "main" branch).
const ROW_BRANCH: &str = "main";

/// Error types for QueryManager operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    TableNotFound(TableName),
    ColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    EncodingError(String),
    ObjectNotFound(ObjectId),
    QueryCompilationError(String),
    IndexError(String),
    /// Cannot undelete or truncate a row that is not soft-deleted.
    RowNotDeleted(ObjectId),
    /// Cannot delete an already-deleted row.
    RowAlreadyDeleted(ObjectId),
    /// Cannot operate on a hard-deleted row (it no longer exists).
    RowHardDeleted(ObjectId),
    /// Policy denied the operation.
    PolicyDenied {
        table: TableName,
        operation: Operation,
    },
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::TableNotFound(t) => write!(f, "table not found: {}", t),
            QueryError::ColumnCountMismatch { expected, actual } => {
                write!(
                    f,
                    "column count mismatch: expected {expected}, got {actual}"
                )
            }
            QueryError::EncodingError(msg) => write!(f, "encoding error: {msg}"),
            QueryError::ObjectNotFound(id) => write!(f, "object not found: {:?}", id),
            QueryError::QueryCompilationError(msg) => write!(f, "query compilation error: {msg}"),
            QueryError::IndexError(msg) => write!(f, "index error: {msg}"),
            QueryError::RowNotDeleted(id) => write!(f, "row not deleted: {:?}", id),
            QueryError::RowAlreadyDeleted(id) => write!(f, "row already deleted: {:?}", id),
            QueryError::RowHardDeleted(id) => write!(f, "row hard deleted: {:?}", id),
            QueryError::PolicyDenied { table, operation } => {
                write!(f, "policy denied {} on table {}", operation, table)
            }
        }
    }
}

impl From<IndexError> for QueryError {
    fn from(e: IndexError) -> Self {
        QueryError::IndexError(format!("{:?}", e))
    }
}

impl std::error::Error for QueryError {}

/// Handle for tracking insert completion.
///
/// Poll via `is_complete()` to check if the row is persisted.
/// Poll via `is_indexed()` to check if the row is indexed.
#[derive(Debug, Clone)]
pub struct InsertHandle {
    /// The row's ObjectId.
    pub row_id: ObjectId,
    /// CommitId of the row data.
    pub row_commit_id: CommitId,
}

/// Handle for tracking delete completion.
#[derive(Debug, Clone)]
pub struct DeleteHandle {
    /// The row's ObjectId.
    pub row_id: ObjectId,
    /// CommitId of the delete tombstone commit.
    pub delete_commit_id: CommitId,
}

impl InsertHandle {
    /// Check if the row data is durable (persisted to storage).
    ///
    /// Must call `QueryManager::process()` between checks to drive storage operations.
    pub fn is_complete(&self, qm: &QueryManager) -> bool {
        qm.is_commit_stored(self.row_id, &self.row_commit_id)
    }

    /// Check if the row is indexed (appears in the _id index).
    ///
    /// After insert + process(), the row should be indexed.
    pub fn is_indexed(&self, qm: &QueryManager, table: &str) -> bool {
        qm.row_is_indexed(table, self.row_id)
    }
}

/// Query subscription info.
#[derive(Debug)]
struct QuerySubscription {
    graph: QueryGraph,
    #[allow(dead_code)]
    mode: SubscriptionMode,
}

/// Subscription mode.
#[derive(Debug, Clone, Copy)]
pub enum SubscriptionMode {
    Delta,
    Full,
}

/// Update for a query subscription.
#[derive(Debug, Clone)]
pub struct QueryUpdate {
    pub subscription_id: QuerySubscriptionId,
    pub delta: RowDelta,
}

/// State for an active policy check (graphs and associated data).
#[derive(Debug)]
struct PolicyCheckState {
    /// Policy graphs that need to settle.
    graphs: Vec<PolicyGraph>,
    /// Table name for error messages.
    table: TableName,
    /// The original pending permission check.
    pending_check: PendingPermissionCheck,
}

/// Manages reactive SQL queries over object-based storage.
///
/// No global Setup/Ready state machine - indices and data are loaded lazily
/// from ObjectManager. Operations work immediately; queries return empty/Pending
/// results until data is available.
///
/// ObjectManager is the source of truth for row data - no caching layer on top.
pub struct QueryManager {
    sync_manager: SyncManager,
    schema: Arc<Schema>,

    /// Indices: (table, column) -> BTreeIndex
    indices: AHashMap<(String, String), BTreeIndex>,

    /// Active query subscriptions
    subscriptions: HashMap<QuerySubscriptionId, QuerySubscription>,
    next_subscription_id: u64,

    /// Pending query updates
    update_outbox: Vec<QueryUpdate>,

    /// Active policy checks being evaluated.
    active_policy_checks: HashMap<PendingUpdateId, PolicyCheckState>,
}

impl QueryManager {
    /// Create a new QueryManager with the given schema.
    ///
    /// Indices are initialized immediately for use. In a real storage scenario,
    /// metadata would be loaded from storage; here we initialize with empty state.
    ///
    /// Row-level security is evaluated via `process()` which handles pending
    /// permission checks from SyncManager.
    pub fn new(sync_manager: SyncManager, schema: Schema) -> Self {
        // Initialize indices for all tables
        let mut indices = AHashMap::new();
        for (table_name, table_schema) in &schema {
            let table_str = table_name.as_str();

            // Primary "_id" index (for live rows)
            let mut id_index = BTreeIndex::new(table_str, "_id");
            id_index.process_meta_load(None); // Initialize empty
            indices.insert((table_str.to_string(), "_id".to_string()), id_index);

            // Soft-deleted rows index
            let mut deleted_index = BTreeIndex::new(table_str, "_id_deleted");
            deleted_index.process_meta_load(None);
            indices.insert(
                (table_str.to_string(), "_id_deleted".to_string()),
                deleted_index,
            );

            // Index for each column
            for col in &table_schema.descriptor.columns {
                let col_str = col.name.as_str();
                let mut col_index = BTreeIndex::new(table_str, col_str);
                col_index.process_meta_load(None);
                indices.insert((table_str.to_string(), col_str.to_string()), col_index);
            }
        }

        Self {
            sync_manager,
            schema: Arc::new(schema),
            indices,
            subscriptions: HashMap::new(),
            next_subscription_id: 0,
            update_outbox: Vec::new(),
            active_policy_checks: HashMap::new(),
        }
    }

    /// Get the underlying SyncManager.
    pub fn sync_manager(&self) -> &SyncManager {
        &self.sync_manager
    }

    /// Get mutable reference to the underlying SyncManager.
    pub fn sync_manager_mut(&mut self) -> &mut SyncManager {
        &mut self.sync_manager
    }

    /// Check if a row is indexed (appears in the _id index for its table).
    pub fn row_is_indexed(&self, table: &str, row_id: ObjectId) -> bool {
        let id_key = (table.to_string(), "_id".to_string());
        if let Some(index) = self.indices.get(&id_key) {
            index.contains_row(row_id)
        } else {
            false
        }
    }

    /// Check if a row is soft-deleted (appears in _id_deleted but not _id).
    pub fn row_is_deleted(&self, table: &str, row_id: ObjectId) -> bool {
        let deleted_key = (table.to_string(), "_id_deleted".to_string());
        if let Some(index) = self.indices.get(&deleted_key) {
            index.contains_row(row_id)
        } else {
            false
        }
    }

    /// Check if a row has a hard delete tombstone (empty content + delete: hard metadata).
    fn is_hard_deleted(&self, id: ObjectId) -> bool {
        let Some(state) = self.sync_manager.object_manager.get_state(id) else {
            return false;
        };
        let obj = match state {
            ObjectState::Creating(obj) | ObjectState::Available(obj) => obj,
            ObjectState::Loading => return false,
        };
        let Some(branch) = obj.branches.get(&BranchName::new(ROW_BRANCH)) else {
            return false;
        };
        let Some(tip_id) = branch.tips.iter().next() else {
            return false;
        };
        let Some(commit) = branch.commits.get(tip_id) else {
            return false;
        };
        // Hard delete: empty content + delete: hard metadata
        commit.content.is_empty()
            && commit
                .metadata
                .as_ref()
                .and_then(|m| m.get("delete"))
                .map(|v| v == "hard")
                .unwrap_or(false)
    }

    /// Check if the current tip has `delete: soft` metadata.
    fn is_soft_delete_commit(&self, id: ObjectId) -> bool {
        let Some(state) = self.sync_manager.object_manager.get_state(id) else {
            return false;
        };
        let obj = match state {
            ObjectState::Creating(obj) | ObjectState::Available(obj) => obj,
            ObjectState::Loading => return false,
        };
        let Some(branch) = obj.branches.get(&BranchName::new(ROW_BRANCH)) else {
            return false;
        };
        let Some(tip_id) = branch.tips.iter().next() else {
            return false;
        };
        let Some(commit) = branch.commits.get(tip_id) else {
            return false;
        };
        // Soft delete: has delete: soft metadata (content is preserved)
        commit
            .metadata
            .as_ref()
            .and_then(|m| m.get("delete"))
            .map(|v| v == "soft")
            .unwrap_or(false)
    }

    /// Check if a commit has been stored to disk.
    ///
    /// Used by `InsertHandle::is_complete()` to check durability.
    pub fn is_commit_stored(&self, object_id: ObjectId, commit_id: &CommitId) -> bool {
        if let Some(state) = self.sync_manager.object_manager.get_state(object_id) {
            match state {
                ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                    // Check all branches for the commit
                    for branch in obj.branches.values() {
                        if let Some(commit) = branch.commits.get(commit_id) {
                            return matches!(commit.stored_state, StoredState::Stored);
                        }
                    }
                }
                ObjectState::Loading => {}
            }
        }
        false
    }

    /// Insert a new row into a table.
    ///
    /// Returns an `InsertHandle` that can be polled to check durability.
    /// Index updates happen immediately (creating sentinels if needed).
    pub fn insert(&mut self, table: &str, values: &[Value]) -> Result<InsertHandle, QueryError> {
        self.insert_with_session(table, values, None)
    }

    /// Insert a new row with session-based policy checking.
    ///
    /// If the table has an INSERT WITH CHECK policy and a session is provided,
    /// the policy is evaluated against the new row values. If the policy
    /// denies the insert, `PolicyDenied` is returned.
    pub fn insert_with_session(
        &mut self,
        table: &str,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<InsertHandle, QueryError> {
        let table_name = TableName::new(table);
        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();
        let insert_policy = table_schema.policies.insert.with_check.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        // Check INSERT WITH CHECK policy
        if let (Some(session), Some(policy)) = (session, insert_policy)
            && !self.evaluate_policy_for_values(&policy, values, &descriptor, session, table)
        {
            return Err(QueryError::PolicyDenied {
                table: table_name,
                operation: Operation::Insert,
            });
        }

        // Encode to binary
        let data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        // Create object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), table.to_string());

        let object_id = self.sync_manager.object_manager.create(Some(metadata));
        let author = object_id; // Self-authored

        // Add commit with row data
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(object_id, ROW_BRANCH, vec![], data.clone(), author, None)
            .map_err(|_| QueryError::ObjectNotFound(object_id))?;

        // Update indices immediately and persist
        self.update_indices_for_insert(table, object_id, &data, &descriptor)?;

        // Mark subscriptions dirty
        self.mark_subscriptions_dirty(table);

        Ok(InsertHandle {
            row_id: object_id,
            row_commit_id,
        })
    }

    /// Evaluate a policy expression against row values (pre-encoding).
    ///
    /// This is used for write policy checking (INSERT/UPDATE WITH CHECK).
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_policy_for_values(
        &self,
        policy: &crate::query_manager::policy::PolicyExpr,
        values: &[Value],
        descriptor: &RowDescriptor,
        session: &Session,
        table: &str,
    ) -> bool {
        use crate::query_manager::policy::PolicyExpr;

        match policy {
            PolicyExpr::True => true,
            PolicyExpr::False => false,

            PolicyExpr::Cmp { column, op, value } => {
                let col_index = match descriptor.column_index(column) {
                    Some(idx) => idx,
                    None => return false,
                };
                let col_value = &values[col_index];
                let cmp_value = match value {
                    crate::query_manager::policy::PolicyValue::Literal(v) => v.clone(),
                    crate::query_manager::policy::PolicyValue::SessionRef(path) => {
                        match resolve_session_value(path, session) {
                            Some(v) => v,
                            None => return false,
                        }
                    }
                };
                self.compare_values(col_value, &cmp_value, op)
            }

            PolicyExpr::IsNull { column } => {
                let col_index = match descriptor.column_index(column) {
                    Some(idx) => idx,
                    None => return false,
                };
                matches!(values[col_index], Value::Null)
            }

            PolicyExpr::IsNotNull { column } => {
                let col_index = match descriptor.column_index(column) {
                    Some(idx) => idx,
                    None => return false,
                };
                !matches!(values[col_index], Value::Null)
            }

            PolicyExpr::In {
                column,
                session_path,
            } => {
                let col_index = match descriptor.column_index(column) {
                    Some(idx) => idx,
                    None => return false,
                };
                let col_value = &values[col_index];
                let session_array = match session.get_array(session_path) {
                    Some(arr) => arr,
                    None => return false,
                };
                self.value_in_json_array(col_value, session_array)
            }

            PolicyExpr::And(exprs) => exprs
                .iter()
                .all(|e| self.evaluate_policy_for_values(e, values, descriptor, session, table)),

            PolicyExpr::Or(exprs) => exprs
                .iter()
                .any(|e| self.evaluate_policy_for_values(e, values, descriptor, session, table)),

            PolicyExpr::Not(expr) => {
                !self.evaluate_policy_for_values(expr, values, descriptor, session, table)
            }

            PolicyExpr::Exists { .. } | PolicyExpr::Inherits { .. } => {
                // EXISTS and INHERITS require actual row data - for writes, return true
                // (TODO: implement for write policies that need these)
                true
            }
        }
    }

    /// Compare two Values with the given operator.
    fn compare_values(
        &self,
        a: &Value,
        b: &Value,
        op: &crate::query_manager::policy::CmpOp,
    ) -> bool {
        use crate::query_manager::policy::CmpOp;
        use std::cmp::Ordering;

        let ord = match (a, b) {
            (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
            (Value::BigInt(x), Value::BigInt(y)) => x.cmp(y),
            (Value::Integer(x), Value::BigInt(y)) => (*x as i64).cmp(y),
            (Value::BigInt(x), Value::Integer(y)) => x.cmp(&(*y as i64)),
            (Value::Text(x), Value::Text(y)) => x.cmp(y),
            (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
            (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
            (Value::Uuid(x), Value::Uuid(y)) => x.0.cmp(&y.0),
            _ => return false, // Type mismatch
        };

        match op {
            CmpOp::Eq => ord == Ordering::Equal,
            CmpOp::Ne => ord != Ordering::Equal,
            CmpOp::Lt => ord == Ordering::Less,
            CmpOp::Le => ord != Ordering::Greater,
            CmpOp::Gt => ord == Ordering::Greater,
            CmpOp::Ge => ord != Ordering::Less,
        }
    }

    /// Check if a Value is in a JSON array.
    fn value_in_json_array(&self, value: &Value, array: &[serde_json::Value]) -> bool {
        match value {
            Value::Text(s) => array.iter().any(|v| v.as_str() == Some(s.as_str())),
            Value::Integer(i) => array.iter().any(|v| v.as_i64() == Some(*i as i64)),
            Value::BigInt(i) => array.iter().any(|v| v.as_i64() == Some(*i)),
            _ => false,
        }
    }

    /// Update a row.
    pub fn update(&mut self, id: ObjectId, values: &[Value]) -> Result<(), QueryError> {
        self.update_with_session(id, values, None)
    }

    /// Update a row with session-based policy checking.
    ///
    /// If the table has policies and a session is provided:
    /// - USING policy is checked against the old row (if exists)
    /// - WITH CHECK policy is checked against the new values
    pub fn update_with_session(
        &mut self,
        id: ObjectId,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<(), QueryError> {
        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get("table").cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Get old data from ObjectManager
        let (old_data, commit_id) = self
            .load_row_from_object(id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();
        let using_policy = table_schema.policies.update.using.clone();
        let check_policy = table_schema.policies.update.with_check.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        // Check UPDATE USING policy against old row
        if let (Some(session), Some(policy)) = (session, &using_policy) {
            let old_row = crate::query_manager::types::Row::new(id, old_data.clone(), commit_id);
            if !self.evaluate_policy_for_row(policy, &old_row, &descriptor, session, &table) {
                return Err(QueryError::PolicyDenied {
                    table: table_name,
                    operation: Operation::Update,
                });
            }
        }

        // Check UPDATE WITH CHECK policy against new values
        if let (Some(session), Some(policy)) = (session, check_policy)
            && !self.evaluate_policy_for_values(&policy, values, &descriptor, session, &table)
        {
            return Err(QueryError::PolicyDenied {
                table: table_name,
                operation: Operation::Update,
            });
        }

        // Encode new data
        let new_data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, ROW_BRANCH)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Add commit with new data
        let _commit_id = self
            .sync_manager
            .object_manager
            .add_commit(id, ROW_BRANCH, parents, new_data.clone(), author, None)
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices and persist modified nodes
        self.update_indices_for_update(&table_name.0, id, &old_data, &new_data, &descriptor)?;

        // Mark subscriptions dirty and notify about content update
        self.mark_subscriptions_dirty(&table_name.0);
        self.mark_row_updated_in_subscriptions(&table_name.0, id);

        Ok(())
    }

    /// Evaluate a policy expression against an encoded row.
    fn evaluate_policy_for_row(
        &self,
        policy: &crate::query_manager::policy::PolicyExpr,
        row: &crate::query_manager::types::Row,
        descriptor: &RowDescriptor,
        session: &Session,
        table: &str,
    ) -> bool {
        use crate::query_manager::graph_nodes::policy_filter::PolicyFilterNode;

        // Create a temporary PolicyFilterNode to evaluate the policy
        let filter = PolicyFilterNode::new(
            descriptor.clone(),
            policy.clone(),
            session.clone(),
            (*self.schema).clone(),
            table,
        );
        filter.evaluate(row)
    }

    /// Soft delete a row.
    ///
    /// Creates a commit with the same content as the previous tip, plus `delete: soft` metadata.
    /// This preserves the row data for queries with `include_deleted`.
    /// Removes from `_id` and all column indices, adds to `_id_deleted` index.
    pub fn delete(&mut self, id: ObjectId) -> Result<DeleteHandle, QueryError> {
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get("table").cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Check if already soft-deleted
        if self.row_is_deleted(&table, id) {
            return Err(QueryError::RowAlreadyDeleted(id));
        }

        // Get old data from ObjectManager (for index removal and content preservation)
        let (old_data, _) = self
            .load_row_from_object(id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, ROW_BRANCH)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create delete metadata
        let mut delete_metadata = std::collections::BTreeMap::new();
        delete_metadata.insert("delete".to_string(), "soft".to_string());

        // Add commit with preserved content + delete: soft metadata
        // Content is copied from previous tip so soft-deleted rows can still be read
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                id,
                ROW_BRANCH,
                parents,
                old_data.clone(), // Preserve content for soft deletes
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices: remove from _id and column indices, add to _id_deleted
        self.update_indices_for_soft_delete(&table, id, &old_data, &descriptor)?;

        // Mark subscriptions dirty and mark row as deleted
        self.mark_subscriptions_dirty(&table);
        self.mark_row_deleted_in_subscriptions(&table, id);

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    /// Soft delete a row with session-based policy checking.
    ///
    /// Checks DELETE USING policy against the existing row before allowing deletion.
    /// Falls back to UPDATE's USING policy if no DELETE policy is defined.
    pub fn delete_with_session(
        &mut self,
        id: ObjectId,
        session: Option<&Session>,
    ) -> Result<DeleteHandle, QueryError> {
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get("table").cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Check if already soft-deleted
        if self.row_is_deleted(&table, id) {
            return Err(QueryError::RowAlreadyDeleted(id));
        }

        // Get old data from ObjectManager (for index removal and content preservation)
        let (old_data, commit_id) = self
            .load_row_from_object(id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        // Check DELETE USING policy (falls back to UPDATE's USING)
        let using_policy = table_schema.policies.effective_delete_using().cloned();
        if let (Some(session), Some(policy)) = (session, using_policy) {
            let old_row = Row::new(id, old_data.clone(), commit_id);
            if !self.evaluate_policy_for_row(&policy, &old_row, &descriptor, session, &table) {
                return Err(QueryError::PolicyDenied {
                    table: table_name,
                    operation: Operation::Delete,
                });
            }
        }

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, ROW_BRANCH)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create delete metadata
        let mut delete_metadata = std::collections::BTreeMap::new();
        delete_metadata.insert("delete".to_string(), "soft".to_string());

        // Add commit with preserved content + delete: soft metadata
        // Content is copied from previous tip so soft-deleted rows can still be read
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                id,
                ROW_BRANCH,
                parents,
                old_data.clone(), // Preserve content for soft deletes
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices: remove from _id and column indices, add to _id_deleted
        self.update_indices_for_soft_delete(&table, id, &old_data, &descriptor)?;

        // Mark subscriptions dirty and mark row as deleted
        self.mark_subscriptions_dirty(&table);
        self.mark_row_deleted_in_subscriptions(&table, id);

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    /// Undelete a soft-deleted row.
    ///
    /// Restores a row from the `_id_deleted` index back to the `_id` and column indices.
    /// Creates a new commit with the provided values (no `delete` metadata).
    pub fn undelete(&mut self, id: ObjectId, values: &[Value]) -> Result<InsertHandle, QueryError> {
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get("table").cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Verify row is in _id_deleted index (soft-deleted)
        if !self.row_is_deleted(&table, id) {
            return Err(QueryError::RowNotDeleted(id));
        }

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        // Encode new row data
        let new_data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, ROW_BRANCH)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Add commit with row data (no delete metadata = undelete)
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(id, ROW_BRANCH, parents, new_data.clone(), author, None)
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices: remove from _id_deleted, add to _id and column indices
        self.update_indices_for_undelete(&table, id, &new_data, &descriptor)?;

        // Mark subscriptions dirty
        self.mark_subscriptions_dirty(&table);

        Ok(InsertHandle {
            row_id: id,
            row_commit_id,
        })
    }

    /// Hard delete a row.
    ///
    /// Creates a commit with empty content and `delete: hard` metadata.
    /// Removes from ALL indices including `_id_deleted`.
    /// Truncates history: only the hard delete tombstone remains.
    /// Hard deletes are authoritative and override any concurrent or subsequent commits.
    pub fn hard_delete(&mut self, id: ObjectId) -> Result<DeleteHandle, QueryError> {
        // Check if already hard-deleted
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get("table").cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Try to get old data (may be empty if already soft-deleted)
        // Treat empty content as no data (tombstone)
        let old_data = self
            .load_row_from_object(id)
            .map(|(data, _)| data)
            .filter(|data| !data.is_empty());

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, ROW_BRANCH)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create hard delete metadata
        let mut delete_metadata = std::collections::BTreeMap::new();
        delete_metadata.insert("delete".to_string(), "hard".to_string());

        // Add commit with empty content + delete: hard metadata
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                id,
                ROW_BRANCH,
                parents,
                vec![], // Empty content for tombstone
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices: remove from ALL indices including _id_deleted
        self.update_indices_for_hard_delete(&table, id, old_data.as_deref(), &descriptor)?;

        // Truncate branch: set tails = [delete_commit_id], removing all history
        // (In ObjectManager, this would be done via set_tails or similar)
        // For now, we just record the hard delete tombstone
        let mut tail_ids = std::collections::HashSet::new();
        tail_ids.insert(delete_commit_id);
        let _ = self
            .sync_manager
            .object_manager
            .truncate_branch(id, ROW_BRANCH, tail_ids);

        // Mark subscriptions dirty and mark row as deleted
        self.mark_subscriptions_dirty(&table);
        self.mark_row_deleted_in_subscriptions(&table, id);

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    /// Truncate a soft-deleted row (upgrade to hard delete).
    ///
    /// Can only be called on rows that are already soft-deleted.
    /// Removes the row from `_id_deleted` and truncates history.
    pub fn truncate(&mut self, id: ObjectId) -> Result<DeleteHandle, QueryError> {
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get("table").cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        // Verify row is in _id_deleted index (soft-deleted)
        if !self.row_is_deleted(&table, id) {
            return Err(QueryError::RowNotDeleted(id));
        }

        // Upgrade to hard delete
        self.hard_delete(id)
    }

    /// Test helper: get a row by ID if loaded in ObjectManager.
    ///
    /// Production code should use queries to read data, not this method.
    /// This exists only to verify test expectations about what's loaded.
    #[cfg(test)]
    pub fn test_get_row_if_loaded(&self, id: ObjectId) -> Option<Vec<Value>> {
        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)?
            .metadata
            .get("table")?
            .clone();
        let table_name = TableName::new(&table);

        // Get row data from ObjectManager
        let (data, _) = self.load_row_from_object(id)?;

        let table_schema = self.schema.get(&table_name)?;
        decode_row(&table_schema.descriptor, &data).ok()
    }

    /// Create a query builder for a table.
    pub fn query(&self, table: &str) -> QueryBuilder {
        QueryBuilder::new(table)
    }

    /// Execute a query and return results (one-shot).
    pub fn execute(&mut self, query: Query) -> Result<Vec<Vec<Value>>, QueryError> {
        let table_schema = self
            .schema
            .get(&query.table)
            .ok_or(QueryError::TableNotFound(query.table))?;
        let descriptor = table_schema.descriptor.clone();

        let mut graph = QueryGraph::compile(&query, &self.schema)
            .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

        // Settle the graph - row_loader reads directly from ObjectManager
        // Returns None for empty content (hard delete tombstones) so they're not materialized
        // Soft deletes have preserved content and can be materialized normally
        let row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            self.load_row_from_object(id)
                .filter(|(data, _)| !data.is_empty())
        };

        graph.settle(&self.indices, &self.sync_manager.object_manager, row_loader);

        // Decode results
        let rows = graph.current_result();
        let results = rows
            .iter()
            .filter_map(|row| decode_row(&descriptor, &row.data).ok())
            .collect();

        Ok(results)
    }

    /// Subscribe to query results (delta mode).
    pub fn subscribe(&mut self, query: Query) -> Result<QuerySubscriptionId, QueryError> {
        self.subscribe_with_session(query, None)
    }

    /// Subscribe to query results with session-based policy filtering.
    ///
    /// When a session is provided and the table has a SELECT policy, rows are
    /// filtered based on the policy expression evaluated against the session context.
    pub fn subscribe_with_session(
        &mut self,
        query: Query,
        session: Option<Session>,
    ) -> Result<QuerySubscriptionId, QueryError> {
        let graph = QueryGraph::compile_with_session(&query, &self.schema, session)
            .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        self.subscriptions.insert(
            id,
            QuerySubscription {
                graph,
                mode: SubscriptionMode::Delta,
            },
        );

        Ok(id)
    }

    /// Unsubscribe from a query.
    pub fn unsubscribe(&mut self, id: QuerySubscriptionId) {
        self.subscriptions.remove(&id);
    }

    /// Take pending query updates.
    pub fn take_updates(&mut self) -> Vec<QueryUpdate> {
        std::mem::take(&mut self.update_outbox)
    }

    /// Process pending changes and settle all subscription graphs.
    ///
    /// This method drives async progress:
    /// - Processes SyncManager inbox (receives client writes)
    /// - Evaluates pending permission checks
    /// - Settles policy graphs and finalizes completed checks
    /// - Processes object updates from SyncManager
    /// - Flushes pending index updates when indices become ready
    /// - Marks subscriptions with pending IDs dirty when objects become available
    /// - Settles all subscription graphs (row data loaded on-demand from ObjectManager)
    pub fn process(&mut self) {
        // 1. Process SyncManager inbox (receives client writes)
        self.sync_manager.process_inbox();

        // 2. Pick up new permission check intents from SyncManager
        self.pick_up_pending_permission_checks();

        // 3. Settle policy graphs and finalize completed checks
        self.settle_policy_checks();

        // 4. Process object updates from SyncManager
        let updates = self.sync_manager.object_manager.take_all_object_updates();
        for update in updates {
            self.handle_object_update(update);
        }

        // 5. Process index storage (handles pending page loads in noop mode)
        self.process_index_storage();

        // 6. Mark subscriptions dirty if they have pending IDs that might now be available
        // This ensures settle() will be called to check pending rows
        self.mark_subscriptions_with_pending_dirty();

        // 7. Settle all subscriptions - row_loader reads directly from ObjectManager
        // Extract references to avoid borrowing self in the closure
        let om = &self.sync_manager.object_manager;
        let indices = &self.indices;

        for (sub_id, subscription) in &mut self.subscriptions {
            // Row loader returns None for empty content (hard delete tombstones)
            // Soft deletes have preserved content and can be materialized normally
            let row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
                let state = om.get_state(id)?;
                match state {
                    ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                        let branch = obj.branches.get(&BranchName::new(ROW_BRANCH))?;
                        let tip_id = branch.tips.iter().next()?;
                        let commit = branch.commits.get(tip_id)?;
                        // Filter out empty content (hard delete tombstones only)
                        if commit.content.is_empty() {
                            return None;
                        }
                        Some((commit.content.clone(), *tip_id))
                    }
                    ObjectState::Loading => None,
                }
            };

            let delta = subscription.graph.settle(indices, om, row_loader);
            if !delta.is_empty() {
                self.update_outbox.push(QueryUpdate {
                    subscription_id: *sub_id,
                    delta,
                });
            }
        }

        // 8. Clear index deltas after all subscriptions have settled
        // This increments the epoch so the next process() cycle uses fresh deltas
        for index in self.indices.values_mut() {
            if index.has_deltas() {
                index.clear_deltas();
            }
        }
    }

    /// Pick up pending permission checks from SyncManager and evaluate them.
    fn pick_up_pending_permission_checks(&mut self) {
        let pending = self.sync_manager.take_pending_permission_checks();

        for check in pending {
            self.evaluate_write_permission(check);
        }
    }

    /// Evaluate a write permission check.
    ///
    /// If the simple parts of the policy fail, reject immediately.
    /// If there are complex clauses (INHERITS/EXISTS), create policy graphs.
    /// If all simple parts pass and no complex clauses, approve immediately.
    ///
    /// For UPDATE operations, we evaluate two policies:
    /// - USING against old_content (can the session see the old row?)
    /// - WITH CHECK against new_content (is the new row valid?)
    fn evaluate_write_permission(&mut self, check: PendingPermissionCheck) {
        // Get table name from metadata
        let table_name = match check.metadata.get("table") {
            Some(t) => TableName::new(t),
            None => {
                // Not a row object, allow
                self.sync_manager.approve_permission_check(check);
                return;
            }
        };

        // Look up table schema - clone to avoid borrowing self
        let table_schema = match self.schema.get(&table_name).cloned() {
            Some(s) => s,
            None => {
                // Unknown table, allow
                self.sync_manager.approve_permission_check(check);
                return;
            }
        };

        // Handle UPDATE specially - needs both USING and WITH CHECK
        if check.operation == Operation::Update {
            self.evaluate_update_permission(check, table_name, table_schema);
            return;
        }

        // Get the appropriate policy based on operation
        let policy = match check.operation {
            Operation::Insert => table_schema.policies.insert.with_check.as_ref(),
            Operation::Update => unreachable!(), // Handled above
            Operation::Delete => table_schema.policies.effective_delete_using(),
            Operation::Select => {
                // SELECT not checked via write permission
                self.sync_manager.approve_permission_check(check);
                return;
            }
        };

        // If no policy defined, allow
        let policy = match policy {
            Some(p) => p.clone(),
            None => {
                self.sync_manager.approve_permission_check(check);
                return;
            }
        };

        // Get the content to evaluate
        let content = match check.operation {
            Operation::Insert => check.new_content.as_ref(),
            Operation::Update => unreachable!(), // Handled above
            Operation::Delete => check.old_content.as_ref(),
            Operation::Select => {
                self.sync_manager.approve_permission_check(check);
                return;
            }
        };

        let content = match content {
            Some(c) => c,
            None => {
                // No content to evaluate - allow
                self.sync_manager.approve_permission_check(check);
                return;
            }
        };

        // Evaluate simple parts of the policy
        let result =
            evaluate_simple_parts(&policy, content, &table_schema.descriptor, &check.session);

        if !result.passed {
            // Simple parts failed - reject immediately
            let reason = format!(
                "{:?} denied by policy on table {}",
                check.operation, table_name.0
            );
            self.sync_manager.reject_permission_check(check, reason);
            return;
        }

        if result.complex_clauses.is_empty() {
            // All simple parts passed and no complex clauses - approve immediately
            self.sync_manager.approve_permission_check(check);
            return;
        }

        // Has complex clauses - create policy graphs for them
        let graphs = self.create_policy_graphs_for_complex_clauses(
            &result.complex_clauses,
            content,
            &table_schema.descriptor,
            &table_name,
            &check.session,
        );

        if graphs.is_empty() {
            // No graphs created (maybe missing tables) - allow
            self.sync_manager.approve_permission_check(check);
            return;
        }

        // Store for settling
        let check_id = check.id;
        self.active_policy_checks.insert(
            check_id,
            PolicyCheckState {
                graphs,
                table: table_name,
                pending_check: check,
            },
        );
    }

    /// Evaluate UPDATE permission with both USING (old row) and WITH CHECK (new row).
    ///
    /// For UPDATE, we need to check:
    /// 1. USING policy against old_content - can the session see the row being updated?
    /// 2. WITH CHECK policy against new_content - is the resulting row valid?
    ///
    /// Both must pass for the update to be allowed.
    fn evaluate_update_permission(
        &mut self,
        check: PendingPermissionCheck,
        table_name: TableName,
        table_schema: TableSchema,
    ) {
        let using_policy = table_schema.policies.update.using.as_ref();
        let check_policy = table_schema.policies.update.with_check.as_ref();

        // If no policies defined, allow
        if using_policy.is_none() && check_policy.is_none() {
            self.sync_manager.approve_permission_check(check);
            return;
        }

        let mut all_complex_clauses: Vec<(ComplexClause, Vec<u8>)> = Vec::new();

        // Step 1: Evaluate USING policy against old_content
        if let Some(using) = using_policy {
            let old_content = match check.old_content.as_ref() {
                Some(c) if !c.is_empty() => c,
                _ => {
                    // No old content means this is actually an INSERT, not UPDATE
                    // Reject - UPDATE USING requires seeing the old row
                    let reason = format!(
                        "Update denied by USING policy on table {} - no old content",
                        table_name.0
                    );
                    self.sync_manager.reject_permission_check(check, reason);
                    return;
                }
            };

            let result =
                evaluate_simple_parts(using, old_content, &table_schema.descriptor, &check.session);

            if !result.passed {
                // USING check failed - session cannot see the old row
                let reason = format!(
                    "Update denied by USING policy on table {} - cannot see old row",
                    table_name.0
                );
                self.sync_manager.reject_permission_check(check, reason);
                return;
            }

            // Collect complex clauses with old_content for USING
            for clause in result.complex_clauses {
                all_complex_clauses.push((clause, old_content.clone()));
            }
        }

        // Step 2: Evaluate WITH CHECK policy against new_content
        if let Some(with_check) = check_policy {
            let new_content = match check.new_content.as_ref() {
                Some(c) => c,
                None => {
                    // No new content - allow (shouldn't happen for UPDATE)
                    self.sync_manager.approve_permission_check(check);
                    return;
                }
            };

            let result = evaluate_simple_parts(
                with_check,
                new_content,
                &table_schema.descriptor,
                &check.session,
            );

            if !result.passed {
                // WITH CHECK failed - new row is not valid
                let reason = format!(
                    "Update denied by WITH CHECK policy on table {}",
                    table_name.0
                );
                self.sync_manager.reject_permission_check(check, reason);
                return;
            }

            // Collect complex clauses with new_content for WITH CHECK
            for clause in result.complex_clauses {
                all_complex_clauses.push((clause, new_content.clone()));
            }
        }

        // If no complex clauses, both simple checks passed - approve
        if all_complex_clauses.is_empty() {
            self.sync_manager.approve_permission_check(check);
            return;
        }

        // Create policy graphs for all complex clauses
        let mut graphs = Vec::new();
        for (clause, content) in &all_complex_clauses {
            let clause_graphs = self.create_policy_graphs_for_complex_clauses(
                std::slice::from_ref(clause),
                content,
                &table_schema.descriptor,
                &table_name,
                &check.session,
            );
            graphs.extend(clause_graphs);
        }

        if graphs.is_empty() {
            // No graphs created (maybe missing tables) - allow
            self.sync_manager.approve_permission_check(check);
            return;
        }

        // Store for settling
        let check_id = check.id;
        self.active_policy_checks.insert(
            check_id,
            PolicyCheckState {
                graphs,
                table: table_name,
                pending_check: check,
            },
        );
    }

    /// Create policy graphs for complex clauses (INHERITS/EXISTS).
    fn create_policy_graphs_for_complex_clauses(
        &self,
        clauses: &[ComplexClause],
        content: &[u8],
        descriptor: &RowDescriptor,
        _table: &TableName,
        session: &Session,
    ) -> Vec<PolicyGraph> {
        let mut graphs = Vec::new();

        for clause in clauses {
            match clause {
                ComplexClause::Inherits {
                    operation,
                    via_column,
                } => {
                    // Get the FK column to find the parent
                    let col_idx = match descriptor.column_index(via_column) {
                        Some(idx) => idx,
                        None => continue, // Column not found
                    };

                    // Get the referenced table
                    let parent_table = match &descriptor.columns[col_idx].references {
                        Some(t) => *t,
                        None => continue, // No FK reference
                    };

                    // Check if FK is NULL - if so, INHERITS passes
                    if super::encoding::column_is_null(descriptor, content, col_idx)
                        .unwrap_or(false)
                    {
                        continue; // NULL FK passes INHERITS
                    }

                    // Decode the FK value to get parent ObjectId
                    let parent_id =
                        match super::encoding::decode_column(descriptor, content, col_idx) {
                            Ok(Value::Uuid(id)) => id,
                            _ => continue, // Can't decode FK
                        };

                    // Get parent's policy for the specified operation
                    let parent_schema = match self.schema.get(&parent_table) {
                        Some(s) => s,
                        None => continue, // Parent table not in schema
                    };

                    let parent_policy = match operation {
                        Operation::Select => parent_schema.policies.select.using.as_ref(),
                        Operation::Insert => parent_schema.policies.insert.with_check.as_ref(),
                        Operation::Update => parent_schema.policies.update.using.as_ref(),
                        Operation::Delete => parent_schema.policies.effective_delete_using(),
                    };

                    // If parent has no policy, INHERITS passes
                    let parent_policy = match parent_policy {
                        Some(p) => p,
                        None => continue,
                    };

                    // Create policy graph for INHERITS
                    if let Some(graph) = PolicyGraph::for_inherits(
                        &parent_table,
                        parent_id,
                        parent_policy,
                        session,
                        &self.schema,
                    ) {
                        graphs.push(graph);
                    }
                }
                ComplexClause::Exists { table, condition } => {
                    let target_table = TableName::new(table);
                    if let Some(graph) =
                        PolicyGraph::for_exists(&target_table, condition, session, &self.schema)
                    {
                        graphs.push(graph);
                    }
                }
            }
        }

        graphs
    }

    /// Settle active policy checks and finalize completed ones.
    fn settle_policy_checks(&mut self) {
        // Collect IDs to finalize
        let mut to_approve = Vec::new();
        let mut to_reject = Vec::new();

        // Create row loader for settling
        let om = &self.sync_manager.object_manager;
        let indices = &self.indices;

        // Settle each active policy check
        for (pending_id, state) in &mut self.active_policy_checks {
            let mut row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
                let obj_state = om.get_state(id)?;
                match obj_state {
                    ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                        let branch = obj.branches.get(&BranchName::new(ROW_BRANCH))?;
                        let tip_id = branch.tips.iter().next()?;
                        let commit = branch.commits.get(tip_id)?;
                        if commit.content.is_empty() {
                            return None;
                        }
                        Some((commit.content.clone(), *tip_id))
                    }
                    ObjectState::Loading => None,
                }
            };

            // Settle all graphs
            let all_complete = state
                .graphs
                .iter_mut()
                .all(|g| g.settle(indices, om, &mut row_loader));

            if all_complete {
                // All graphs settled - check results
                let all_pass = state.graphs.iter().all(|g| g.result());

                if all_pass {
                    to_approve.push(*pending_id);
                } else {
                    let reason = format!(
                        "{:?} denied by policy on table {} (INHERITS check failed)",
                        state.pending_check.operation, state.table.0
                    );
                    to_reject.push((*pending_id, reason));
                }
            }
        }

        // Finalize completed checks
        for id in to_approve {
            if let Some(state) = self.active_policy_checks.remove(&id) {
                self.sync_manager
                    .approve_permission_check(state.pending_check);
            }
        }

        for (id, reason) in to_reject {
            if let Some(state) = self.active_policy_checks.remove(&id) {
                self.sync_manager
                    .reject_permission_check(state.pending_check, reason);
            }
        }
    }

    /// Mark subscriptions dirty if they have pending IDs.
    /// This ensures settle() will re-check pending rows on each process() call.
    fn mark_subscriptions_with_pending_dirty(&mut self) {
        for subscription in self.subscriptions.values_mut() {
            // Check if the MaterializeNode has any pending IDs
            for compact in &subscription.graph.nodes {
                if let super::graph::GraphNode::Materialize(mat_node) = &compact.node
                    && mat_node.has_pending()
                {
                    // Mark the graph dirty so settle() will be called
                    subscription.graph.mark_materialize_dirty();
                    break;
                }
            }
        }
    }

    /// Process index storage - collects requests from indices and handles noop responses.
    ///
    /// For real storage backends, this would forward requests to storage and
    /// process responses. For now (tests), we use noop responses.
    fn process_index_storage(&mut self) {
        use super::index::PageId;
        use crate::storage::StorageRequest;

        // Collect storage requests from all indices
        let mut all_requests = Vec::new();
        for index in self.indices.values_mut() {
            all_requests.extend(index.take_storage_requests());
        }

        // Generate noop responses and route them back to indices
        for request in all_requests {
            match request {
                StorageRequest::LoadIndexMeta { table, column } => {
                    // New index - return None (index will initialize empty)
                    if let Some(index) = self.indices.get_mut(&(table.clone(), column.clone())) {
                        index.process_meta_load(None);
                    }
                }
                StorageRequest::LoadIndexPage {
                    table,
                    column,
                    page_id,
                } => {
                    // Page doesn't exist - return None (index will create new page)
                    if let Some(index) = self.indices.get_mut(&(table.clone(), column.clone())) {
                        index.process_page_load(PageId(page_id), None);
                    }
                }
                StorageRequest::StoreIndexMeta { .. }
                | StorageRequest::StoreIndexPage { .. }
                | StorageRequest::DeleteIndexPage { .. } => {
                    // Store/delete requests are fire-and-forget in noop mode
                }
                _ => {}
            }
        }
    }

    /// Load a row's data from ObjectManager using LWW (last-writer-wins by timestamp).
    /// When multiple concurrent tips exist, returns content from the tip with highest timestamp.
    fn load_row_from_object(&self, row_id: ObjectId) -> Option<(Vec<u8>, CommitId)> {
        let state = self.sync_manager.object_manager.get_state(row_id)?;
        match state {
            ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                let branch = obj.branches.get(&BranchName::new(ROW_BRANCH))?;
                // Sort tips by timestamp (oldest first), take last (newest = LWW winner)
                let mut tips: Vec<_> = branch.tips.iter().copied().collect();
                tips.sort_by_key(|id| branch.commits.get(id).map(|c| c.timestamp).unwrap_or(0));
                let tip_id = tips.last()?;
                let commit = branch.commits.get(tip_id)?;
                Some((commit.content.clone(), *tip_id))
            }
            ObjectState::Loading => None,
        }
    }

    /// Handle an object update from the global subscription.
    fn handle_object_update(&mut self, update: AllObjectUpdate) {
        // Check if this is a row object
        let table = match update.metadata.get("table") {
            Some(t) => t.clone(),
            None => return,
        };

        let table_name = TableName::new(&table);
        let table_schema = match self.schema.get(&table_name) {
            Some(schema) => schema,
            None => return,
        };
        let descriptor = table_schema.descriptor.clone();

        // Check if we have a local hard delete tombstone - if so, ignore incoming updates
        if self.is_hard_deleted(update.object_id) {
            // Hard delete is authoritative - ignore incoming updates
            return;
        }

        // Check if incoming update is a hard delete
        if self.is_incoming_hard_delete(update.object_id) {
            // Apply hard delete unconditionally
            let old_data = update.old_content.as_deref();
            let _ = self.update_indices_for_hard_delete(
                &table,
                update.object_id,
                old_data,
                &descriptor,
            );
            self.mark_subscriptions_dirty(&table);
            self.mark_row_deleted_in_subscriptions(&table, update.object_id);
            return;
        }

        // Check if incoming update is a soft delete
        if self.is_soft_delete_commit(update.object_id) {
            // Apply soft delete - remove from _id and column indices, add to _id_deleted
            if let Some(old_data) = &update.old_content {
                let _ = self.update_indices_for_soft_delete(
                    &table,
                    update.object_id,
                    old_data,
                    &descriptor,
                );
            } else {
                // No old content - just remove from _id and add to _id_deleted
                let id_key = (table.to_string(), "_id".to_string());
                if let Some(index) = self.indices.get_mut(&id_key) {
                    let _ = index.remove(update.object_id.uuid().as_bytes(), update.object_id);
                }
                let deleted_key = (table.to_string(), "_id_deleted".to_string());
                if let Some(index) = self.indices.get_mut(&deleted_key) {
                    let _ = index.insert(update.object_id.uuid().as_bytes(), update.object_id);
                }
            }
            self.mark_subscriptions_dirty(&table);
            self.mark_row_deleted_in_subscriptions(&table, update.object_id);
            return;
        }

        // Check if this is an undelete (non-empty content for previously soft-deleted row)
        let was_soft_deleted = self.row_is_deleted(&table, update.object_id);

        // Extract current (new) data from the object
        let new_data = match self.load_row_from_object(update.object_id) {
            Some((data, _)) => data,
            None => return,
        };

        if was_soft_deleted {
            // This is an undelete - remove from _id_deleted, add to _id and column indices
            let _ =
                self.update_indices_for_undelete(&table, update.object_id, &new_data, &descriptor);
            self.mark_subscriptions_dirty(&table);
            return;
        }

        // Normal update handling
        if update.is_new_object || update.previous_commit_ids.is_empty() {
            // First commit on branch (new object or synced first commit) - insert into all indices
            let _ =
                self.update_indices_for_insert(&table, update.object_id, &new_data, &descriptor);
        } else if let Some(old_data) = update.old_content {
            // Synced update - compute index delta using old_content
            // TODO: Future merge strategies - currently last-writer-wins by timestamp
            let _ = self.update_indices_for_update(
                &table,
                update.object_id,
                &old_data,
                &new_data,
                &descriptor,
            );
        }
        // If old_content is None with previous_commit_ids: truncated old data, accept staleness

        self.mark_subscriptions_dirty(&table);
        self.mark_row_updated_in_subscriptions(&table, update.object_id);
    }

    /// Check if an incoming update has hard delete metadata.
    fn is_incoming_hard_delete(&self, id: ObjectId) -> bool {
        let Some(state) = self.sync_manager.object_manager.get_state(id) else {
            return false;
        };
        let obj = match state {
            ObjectState::Creating(obj) | ObjectState::Available(obj) => obj,
            ObjectState::Loading => return false,
        };
        let Some(branch) = obj.branches.get(&BranchName::new(ROW_BRANCH)) else {
            return false;
        };
        let Some(tip_id) = branch.tips.iter().next() else {
            return false;
        };
        let Some(commit) = branch.commits.get(tip_id) else {
            return false;
        };
        // Hard delete: empty content + delete: hard metadata
        commit.content.is_empty()
            && commit
                .metadata
                .as_ref()
                .and_then(|m| m.get("delete"))
                .map(|v| v == "hard")
                .unwrap_or(false)
    }

    /// Update indices when a row is inserted.
    fn update_indices_for_insert(
        &mut self,
        table: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Update "_id" index
        let id_key = (table.to_string(), "_id".to_string());
        if let Some(index) = self.indices.get_mut(&id_key) {
            index.insert(object_id.uuid().as_bytes(), object_id)?;
        }

        // Update column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            let col_key = (table.to_string(), col.name.to_string());
            if let Some(index) = self.indices.get_mut(&col_key)
                && let Ok(Some(value_bytes)) =
                    super::encoding::column_bytes(descriptor, data, col_idx)
            {
                index.insert(value_bytes, object_id)?;
            }
        }

        Ok(())
    }

    /// Update indices when a row is updated.
    fn update_indices_for_update(
        &mut self,
        table: &str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // "_id" index doesn't change on update

        // Update column indices (remove old value, add new value)
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            let col_key = (table.to_string(), col.name.to_string());
            if let Some(index) = self.indices.get_mut(&col_key) {
                // Remove old value
                if let Ok(Some(old_bytes)) =
                    super::encoding::column_bytes(descriptor, old_data, col_idx)
                {
                    index.remove(old_bytes, object_id)?;
                }
                // Add new value
                if let Ok(Some(new_bytes)) =
                    super::encoding::column_bytes(descriptor, new_data, col_idx)
                {
                    index.insert(new_bytes, object_id)?;
                }
            }
        }

        Ok(())
    }

    /// Update indices for soft delete: remove from _id and column indices, add to _id_deleted.
    fn update_indices_for_soft_delete(
        &mut self,
        table: &str,
        object_id: ObjectId,
        old_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id" index
        let id_key = (table.to_string(), "_id".to_string());
        if let Some(index) = self.indices.get_mut(&id_key) {
            index.remove(object_id.uuid().as_bytes(), object_id)?;
        }

        // Remove from all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            let col_key = (table.to_string(), col.name.to_string());
            if let Some(index) = self.indices.get_mut(&col_key)
                && let Ok(Some(value_bytes)) =
                    super::encoding::column_bytes(descriptor, old_data, col_idx)
            {
                index.remove(value_bytes, object_id)?;
            }
        }

        // Add to "_id_deleted" index
        let deleted_key = (table.to_string(), "_id_deleted".to_string());
        if let Some(index) = self.indices.get_mut(&deleted_key) {
            index.insert(object_id.uuid().as_bytes(), object_id)?;
        }

        Ok(())
    }

    /// Update indices for hard delete: remove from ALL indices including _id_deleted.
    fn update_indices_for_hard_delete(
        &mut self,
        table: &str,
        object_id: ObjectId,
        old_data: Option<&[u8]>,
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id" index (may not be present if already soft-deleted)
        let id_key = (table.to_string(), "_id".to_string());
        if let Some(index) = self.indices.get_mut(&id_key) {
            // Ignore errors - row may not be in _id if already soft-deleted
            let _ = index.remove(object_id.uuid().as_bytes(), object_id);
        }

        // Remove from all column indices (if we have old data)
        if let Some(data) = old_data {
            for (col_idx, col) in descriptor.columns.iter().enumerate() {
                let col_key = (table.to_string(), col.name.to_string());
                if let Some(index) = self.indices.get_mut(&col_key)
                    && let Ok(Some(value_bytes)) =
                        super::encoding::column_bytes(descriptor, data, col_idx)
                {
                    // Ignore errors - row may not be in column index if already soft-deleted
                    let _ = index.remove(value_bytes, object_id);
                }
            }
        }

        // Remove from "_id_deleted" index (handles soft→hard upgrade)
        let deleted_key = (table.to_string(), "_id_deleted".to_string());
        if let Some(index) = self.indices.get_mut(&deleted_key) {
            // Ignore errors - row may not be in _id_deleted if it was never soft-deleted
            let _ = index.remove(object_id.uuid().as_bytes(), object_id);
        }

        Ok(())
    }

    /// Update indices for undelete: remove from _id_deleted, add to _id and column indices.
    fn update_indices_for_undelete(
        &mut self,
        table: &str,
        object_id: ObjectId,
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id_deleted" index
        let deleted_key = (table.to_string(), "_id_deleted".to_string());
        if let Some(index) = self.indices.get_mut(&deleted_key) {
            index.remove(object_id.uuid().as_bytes(), object_id)?;
        }

        // Add to "_id" index
        let id_key = (table.to_string(), "_id".to_string());
        if let Some(index) = self.indices.get_mut(&id_key) {
            index.insert(object_id.uuid().as_bytes(), object_id)?;
        }

        // Add to all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            let col_key = (table.to_string(), col.name.to_string());
            if let Some(index) = self.indices.get_mut(&col_key)
                && let Ok(Some(value_bytes)) =
                    super::encoding::column_bytes(descriptor, new_data, col_idx)
            {
                index.insert(value_bytes, object_id)?;
            }
        }

        Ok(())
    }

    /// Mark subscriptions dirty for a table.
    /// Checks all tables involved in the subscription (including joined tables).
    fn mark_subscriptions_dirty(&mut self, table: &str) {
        for subscription in self.subscriptions.values_mut() {
            if Self::subscription_involves_table(&subscription.graph, table) {
                subscription.graph.mark_dirty_for_table(table);
            }
        }
    }

    /// Mark a row as updated in all subscriptions for a table.
    /// This triggers content change detection during settle().
    /// Checks all tables involved in the subscription (including joined tables).
    fn mark_row_updated_in_subscriptions(&mut self, table: &str, id: ObjectId) {
        for subscription in self.subscriptions.values_mut() {
            if Self::subscription_involves_table(&subscription.graph, table) {
                subscription.graph.mark_row_updated(id);
            }
        }
    }

    /// Mark a row as deleted in all subscriptions for a table.
    /// This triggers removal delta emission during settle().
    /// Checks all tables involved in the subscription (including joined tables).
    fn mark_row_deleted_in_subscriptions(&mut self, table: &str, id: ObjectId) {
        for subscription in self.subscriptions.values_mut() {
            if Self::subscription_involves_table(&subscription.graph, table) {
                subscription.graph.mark_row_deleted(id);
            }
        }
    }

    /// Check if a subscription involves a given table (base table, joined table, or array subquery inner table).
    fn subscription_involves_table(graph: &super::graph::QueryGraph, table: &str) -> bool {
        graph.involves_table(table)
    }

    // ========================================================================
    // No-op storage driver (for tests)
    // ========================================================================

    /// Process all pending storage requests with successful no-op responses.
    ///
    /// This is useful for tests and benchmarks that don't have a real storage backend.
    /// Delegates to SyncManager::drain_storage_noop().
    pub fn drain_storage_noop(&mut self) {
        self.sync_manager.drain_storage_noop();
    }

    /// Reset all indices to unloaded state for cold start scenarios.
    ///
    /// Call this on a new QueryManager created with an existing SyncManager
    /// to allow indices to load their data from storage rather than starting empty.
    pub fn reset_indices_for_cold_start(&mut self) {
        for index in self.indices.values_mut() {
            index.reset_for_cold_start();
        }
    }

    /// Load indices from storage using a real driver.
    ///
    /// This is a convenience method that calls `reset_indices_for_cold_start()` and then
    /// processes storage in a loop until all indices are loaded. Use this after creating
    /// a new QueryManager with an existing SyncManager (cold start scenario).
    pub fn load_indices_from_driver(&mut self, driver: &mut impl crate::driver::Driver) {
        self.reset_indices_for_cold_start();
        // Loop until no more pending requests
        for _ in 0..10 {
            self.process_storage_with_driver(driver);
        }
    }

    /// Process storage requests through a real driver.
    ///
    /// This handles both ObjectManager requests and index storage requests.
    /// Use this for tests that need actual persistence (e.g., cold_start tests).
    /// Note: May need to be called multiple times when loading indices (meta first, then pages).
    pub fn process_storage_with_driver(&mut self, driver: &mut impl crate::driver::Driver) {
        use super::index::PageId;
        use crate::storage::StorageResponse;

        // Collect all storage requests: ObjectManager + indices
        let mut all_requests = self.sync_manager.object_manager.take_requests();
        for index in self.indices.values_mut() {
            all_requests.extend(index.take_storage_requests());
        }

        if all_requests.is_empty() {
            return;
        }

        // Process through driver
        let responses = driver.process(all_requests);

        // Route responses to appropriate handlers
        for response in responses {
            match &response {
                // Index responses - route to indices
                StorageResponse::LoadIndexMeta {
                    table,
                    column,
                    result,
                } => {
                    if let Some(index) = self.indices.get_mut(&(table.clone(), column.clone())) {
                        index.process_meta_load(result.as_ref().ok().and_then(|o| o.clone()));
                    }
                }
                StorageResponse::LoadIndexPage {
                    table,
                    column,
                    page_id,
                    result,
                } => {
                    if let Some(index) = self.indices.get_mut(&(table.clone(), column.clone())) {
                        index.process_page_load(
                            PageId(*page_id),
                            result.as_ref().ok().and_then(|o| o.clone()),
                        );
                    }
                }
                StorageResponse::StoreIndexMeta { .. }
                | StorageResponse::StoreIndexPage { .. }
                | StorageResponse::DeleteIndexPage { .. } => {
                    // Store/delete responses don't need routing
                }

                // Object/blob responses - route to ObjectManager
                _ => {
                    self.sync_manager.object_manager.push_response(response);
                }
            }
        }

        // Process ObjectManager responses
        self.sync_manager.object_manager.process_storage_responses();
    }

    // ========================================================================
    // Memory profiling
    // ========================================================================

    /// Calculate memory usage breakdown for profiling.
    ///
    /// Returns a tuple: (indices, subscriptions, policy_checks, total)
    pub fn memory_size(&self) -> (usize, usize, usize, usize) {
        // Indices state (mostly just the pending_index_updates queue)
        let mut indices = 0usize;
        for ((table, col), index) in &self.indices {
            indices += table.len() + col.len() + 48; // Key size + HashMap entry
            indices += index.memory_size();
        }

        // Subscriptions (QueryGraph can be large)
        let mut subscriptions = 0usize;
        for (id, sub) in &self.subscriptions {
            subscriptions += std::mem::size_of_val(id);
            subscriptions += std::mem::size_of::<QuerySubscription>();
            // QueryGraph size estimation - it has maps and sets
            subscriptions += sub.graph.estimate_memory_size();
            subscriptions += 48; // HashMap entry overhead
        }
        subscriptions += self.update_outbox.len() * 256; // QueryUpdate overhead

        // Active policy checks
        let mut policy_checks = 0usize;
        for state in self.active_policy_checks.values() {
            policy_checks += 48; // HashMap entry
            policy_checks += state.graphs.len() * 1024; // Rough estimate per PolicyGraph
            policy_checks += state.table.0.len();
        }

        let total = indices + subscriptions + policy_checks;
        (indices, subscriptions, policy_checks, total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::TestDriver;
    use crate::query_manager::types::ColumnDescriptor;
    use crate::query_manager::types::ColumnType;
    use smallvec::smallvec;

    fn test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("score", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    #[test]
    fn insert_and_get() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        let row = qm.test_get_row_if_loaded(handle.row_id).unwrap();
        assert_eq!(row[0], Value::Text("Alice".into()));
        assert_eq!(row[1], Value::Integer(100));
    }

    #[test]
    fn insert_and_query() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        qm.insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        qm.insert(
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

        // Query all
        let query = qm.query("users").build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 3);

        // Query with filter
        let query = qm
            .query("users")
            .filter_ge("score", Value::Integer(75))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn query_with_sort_and_limit() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        qm.insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        qm.insert(
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

        let query = qm.query("users").order_by_desc("score").limit(2).build();
        let results = qm.execute(query).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0][0], Value::Text("Alice".into())); // 100
        assert_eq!(results[1][0], Value::Text("Charlie".into())); // 75
    }

    #[test]
    fn update_row() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        qm.update(
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(150)],
        )
        .unwrap();

        let row = qm.test_get_row_if_loaded(handle.row_id).unwrap();
        assert_eq!(row[0], Value::Text("Alice Updated".into()));
        assert_eq!(row[1], Value::Integer(150));
    }

    #[test]
    fn table_not_found_error() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let result = qm.insert("nonexistent", &[Value::Text("test".into())]);
        assert!(matches!(result, Err(QueryError::TableNotFound(_))));
    }

    #[test]
    fn column_count_mismatch_error() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let result = qm.insert("users", &[Value::Text("Alice".into())]);
        assert!(matches!(
            result,
            Err(QueryError::ColumnCountMismatch { .. })
        ));
    }

    #[test]
    fn insert_returns_handle_with_commit_id() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Handle should have the row ID
        assert!(qm.test_get_row_if_loaded(handle.row_id).is_some());

        // Handle should have a valid row commit ID
        assert!(handle.row_commit_id.0 != [0; 32]);
    }

    #[test]
    fn row_is_indexed_after_insert() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Row should be indexed immediately after insert
        assert!(handle.is_indexed(&qm, "users"));
    }

    #[test]
    fn index_persistence_via_insert() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Test".into()), Value::Integer(42)])
            .unwrap();

        // Verify row is indexed
        assert!(handle.is_indexed(&qm, "users"));
    }

    // ========================================================================
    // Lazy loading and subscription tests
    // ========================================================================

    #[test]
    fn can_register_query_immediately() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Can register a query subscription immediately
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query);
        assert!(sub_id.is_ok());
    }

    #[test]
    fn subscription_updates_after_insert_and_process() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Register subscription
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Insert a row
        qm.insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Process - should settle subscriptions
        qm.process();

        // Now we should have subscription updates
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(updates[0].delta.added.len(), 1);
    }

    #[test]
    fn multiple_inserts_all_visible_in_query() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Multiple inserts
        let h1 = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let h2 = qm
            .insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        let h3 = qm
            .insert(
                "users",
                &[Value::Text("Charlie".into()), Value::Integer(75)],
            )
            .unwrap();

        // All rows visible via get() immediately
        assert!(qm.test_get_row_if_loaded(h1.row_id).is_some());
        assert!(qm.test_get_row_if_loaded(h2.row_id).is_some());
        assert!(qm.test_get_row_if_loaded(h3.row_id).is_some());

        // Query returns all rows
        let query = qm.query("users").build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 3);

        // Sorted query works
        let query = qm.query("users").order_by_desc("score").limit(2).build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0][0], Value::Text("Alice".into())); // 100
        assert_eq!(results[1][0], Value::Text("Charlie".into())); // 75
    }

    #[test]
    fn cold_start_loads_persisted_indices_and_rows() {
        // Shared driver persists index pages across QM instances
        let mut driver = TestDriver::new();

        // Phase 1: Create QM, insert rows, persist indices
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm1 = QueryManager::new(sync_manager, schema.clone());

        // Insert some rows
        let h1 = qm1
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let h2 = qm1
            .insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();

        // Persist storage (row objects + index pages)
        qm1.process_storage_with_driver(&mut driver);

        // Rows are indexed
        assert!(h1.is_indexed(&qm1, "users"));
        assert!(h2.is_indexed(&qm1, "users"));

        // Phase 2: "Cold start" - create new QM with same underlying ObjectManager
        let sync_manager2 = std::mem::replace(qm1.sync_manager_mut(), SyncManager::new());
        let mut qm2 = QueryManager::new(sync_manager2, schema);

        // Load indices from driver (cold start)
        qm2.load_indices_from_driver(&mut driver);

        // Verify rows are discoverable via get()
        assert!(qm2.test_get_row_if_loaded(h1.row_id).is_some());
        assert!(qm2.test_get_row_if_loaded(h2.row_id).is_some());

        // Verify queries work
        let query = qm2.query("users").build();
        let results = qm2.execute(query).unwrap();
        assert_eq!(results.len(), 2);

        // Verify filtered query works (proves indices were loaded)
        let query = qm2
            .query("users")
            .filter_ge("score", Value::Integer(75))
            .build();
        let results = qm2.execute(query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Text("Alice".into()));
    }

    #[test]
    fn cold_start_only_loads_queried_rows() {
        // This test verifies that after cold start:
        // 1. process() does NOT eagerly load all rows into a cache
        // 2. Queries access ObjectManager directly (no redundant row_cache)
        // 3. Rows not yet in ObjectManager return None gracefully

        // Shared driver persists index pages across QM instances
        let mut driver = TestDriver::new();

        // Phase 1: Create QM, insert multiple rows
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm1 = QueryManager::new(sync_manager, schema.clone());

        let h1 = qm1
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let h2 = qm1
            .insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        let h3 = qm1
            .insert(
                "users",
                &[Value::Text("Charlie".into()), Value::Integer(75)],
            )
            .unwrap();

        // Persist storage
        qm1.process_storage_with_driver(&mut driver);

        // Phase 2: Simulate cold start with new QM
        let sync_manager2 = std::mem::replace(qm1.sync_manager_mut(), SyncManager::new());
        let mut qm2 = QueryManager::new(sync_manager2, schema);

        // Load indices from driver (cold start)
        qm2.load_indices_from_driver(&mut driver);

        // Query for specific rows (filter: score >= 75)
        let query = qm2
            .query("users")
            .filter_ge("score", Value::Integer(75))
            .build();
        let results = qm2.execute(query).unwrap();

        // Should find 2 rows (Alice: 100, Charlie: 75)
        assert_eq!(results.len(), 2);

        // Verify all rows are accessible via get() - reads from ObjectManager directly
        assert!(qm2.test_get_row_if_loaded(h1.row_id).is_some()); // Alice
        assert!(qm2.test_get_row_if_loaded(h2.row_id).is_some()); // Bob
        assert!(qm2.test_get_row_if_loaded(h3.row_id).is_some()); // Charlie
    }

    #[test]
    fn cold_start_with_sorted_query() {
        // Shared driver persists index pages across QM instances
        let mut driver = TestDriver::new();

        // Phase 1: Insert rows
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm1 = QueryManager::new(sync_manager, schema.clone());

        qm1.insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm1.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();
        qm1.insert(
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

        // Persist storage
        qm1.process_storage_with_driver(&mut driver);

        // Phase 2: Cold start
        let sync_manager2 = std::mem::replace(qm1.sync_manager_mut(), SyncManager::new());
        let mut qm2 = QueryManager::new(sync_manager2, schema);

        // Load indices from driver (cold start)
        qm2.load_indices_from_driver(&mut driver);

        // Sorted query should work
        let query = qm2.query("users").order_by_desc("score").build();
        let results = qm2.execute(query).unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0][0], Value::Text("Alice".into())); // 100
        assert_eq!(results[1][0], Value::Text("Charlie".into())); // 75
        assert_eq!(results[2][0], Value::Text("Bob".into())); // 50
    }

    #[test]
    fn local_update_updates_all_column_indices() {
        // Verifies that local update() correctly:
        // 1. Removes old values from column indices
        // 2. Adds new values to column indices
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with name="Alice", score=100
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Query by name="Alice" → finds row
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1);

        // Query by score=100 → finds row
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(100))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1);

        // Update to name="Bob", score=200
        qm.update(
            handle.row_id,
            &[Value::Text("Bob".into()), Value::Integer(200)],
        )
        .unwrap();

        // Query by name="Alice" → empty (old value removed from index)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Old name value should be removed from index"
        );

        // Query by name="Bob" → finds row (new value in index)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Bob".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "New name value should be in index");

        // Query by score=100 → empty (old value removed from index)
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(100))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Old score value should be removed from index"
        );

        // Query by score=200 → finds row (new value in index)
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(200))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "New score value should be in index");
    }

    #[test]
    fn synced_update_updates_column_indices() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::encode_row;
        use std::collections::HashMap;

        // This test verifies that updates received via sync (receive_commit)
        // correctly update column indices using old_content from AllObjectUpdate.

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Simulate receiving a new object from sync
        let row_id = crate::object::ObjectId::new();
        let author = row_id;

        // Receive object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .receive_object(row_id, metadata);

        // Subscribe to all objects so we get AllObjectUpdate notifications
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Encode the initial row data (name="Alice", score=100)
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let initial_data = encode_row(
            &descriptor,
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

        // Receive the first commit (insert)
        let commit1 = Commit {
            parents: smallvec![],
            content: initial_data.clone(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        let commit1_id = qm
            .sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, commit1)
            .unwrap();

        // Process to handle the AllObjectUpdate
        qm.process();

        // Query by name="Alice" → finds row
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "Should find row by name=Alice after sync insert"
        );

        // Query by score=100 → finds row
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(100))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "Should find row by score=100 after sync insert"
        );

        // Encode updated row data (name="Bob", score=200)
        let updated_data = encode_row(
            &descriptor,
            &[Value::Text("Bob".into()), Value::Integer(200)],
        )
        .unwrap();

        // Receive the second commit (update)
        let commit2 = Commit {
            parents: smallvec![commit1_id],
            content: updated_data.clone(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, commit2)
            .unwrap();

        // Process to handle the AllObjectUpdate with old_content
        qm.process();

        // Query by name="Alice" → empty (old value removed from index)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Old name value should be removed from index after sync update"
        );

        // Query by name="Bob" → finds row (new value in index)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Bob".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "New name value should be in index after sync update"
        );

        // Query by score=100 → empty (old value removed from index)
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(100))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Old score value should be removed from index after sync update"
        );

        // Query by score=200 → finds row (new value in index)
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(200))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "New score value should be in index after sync update"
        );
    }

    #[test]
    fn synced_insert_appears_in_subscription_delta() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::{decode_row, encode_row};
        use std::collections::HashMap;

        // Verify that a synced insert appears in subscription deltas

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Simulate receiving a new row from sync BEFORE subscribing
        // (similar to existing synced_update_updates_column_indices pattern)
        let row_id = crate::object::ObjectId::new();
        let author = row_id;

        // Receive object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .receive_object(row_id, metadata);

        // Subscribe to all objects so we get AllObjectUpdate notifications
        qm.sync_manager_mut().object_manager.subscribe_all();

        // NOW subscribe to query (after subscribe_all but before receive_commit)
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Encode the row data (name="SyncedUser", score=42)
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let row_data = encode_row(
            &descriptor,
            &[Value::Text("SyncedUser".into()), Value::Integer(42)],
        )
        .unwrap();

        // Receive the commit (insert)
        let commit = Commit {
            parents: smallvec![],
            content: row_data,
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, commit)
            .unwrap();

        // Process to handle the AllObjectUpdate
        qm.process();

        // Verify subscription delta contains the added row
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Delta should contain one added row"
        );

        // Decode the row to verify contents
        let row = &updates[0].delta.added[0];
        let values = decode_row(&descriptor, &row.data).unwrap();
        assert_eq!(values[0], Value::Text("SyncedUser".into()));
        assert_eq!(values[1], Value::Integer(42));
    }

    #[test]
    fn synced_update_is_visible_in_query() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::encode_row;

        // Verify that synced updates (same row, new content) update indices correctly
        // and are visible in subsequent queries.
        //
        // Note: Currently, row content updates for existing IDs don't emit subscription
        // deltas because the graph tracks ID changes, not content changes. The
        // MaterializeNode has a check_update() method for this, but it's not wired
        // into the settle() flow yet. For now, we verify that queries see the updated data.

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Subscribe to all objects for sync updates
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Insert a row locally first
        let insert_handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let row_id = insert_handle.row_id;
        let first_commit_id = insert_handle.row_commit_id;

        // Process to settle the initial insert
        qm.process();

        // Verify initial data is queryable
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "Should find initial row");
        assert_eq!(results[0][0], Value::Text("Alice".into()));
        assert_eq!(results[0][1], Value::Integer(100));

        // Now simulate a synced update to this row (e.g., from another peer)
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let updated_data = encode_row(
            &descriptor,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .unwrap();

        let author = row_id; // Self-authored for simplicity
        let update_commit = Commit {
            parents: smallvec![first_commit_id],
            content: updated_data,
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, update_commit)
            .unwrap();

        // Process to handle the synced update
        qm.process();

        // Old data should no longer be in index
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 0, "Old name should not be found");

        // New data should be queryable
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("Alice Updated".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "Should find updated row by new name");
        assert_eq!(results[0][0], Value::Text("Alice Updated".into()));
        assert_eq!(results[0][1], Value::Integer(200));

        // Score index should also be updated
        let query = qm
            .query("users")
            .filter_eq("score", Value::Integer(200))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1, "Should find updated row by new score");
    }

    #[test]
    fn synced_row_visible_in_filtered_subscription() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::{decode_row, encode_row};
        use std::collections::HashMap;

        // Verify that synced rows are correctly filtered by subscription predicates.
        // Rows matching the filter appear in deltas; rows not matching are excluded.

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Subscribe to all objects for sync updates
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Subscribe to filtered query: users with score > 25
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(25))
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        // Row descriptor for encoding
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);

        // --- Test 1: Synced row that matches filter (score=30 > 25) ---

        let row_id_1 = crate::object::ObjectId::new();
        let author_1 = row_id_1;

        let mut metadata_1 = HashMap::new();
        metadata_1.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .receive_object(row_id_1, metadata_1);

        let data_1 = encode_row(
            &descriptor,
            &[Value::Text("HighScorer".into()), Value::Integer(30)],
        )
        .unwrap();

        let commit_1 = Commit {
            parents: smallvec![],
            content: data_1,
            timestamp: 1000,
            author: author_1,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id_1, ROW_BRANCH, commit_1)
            .unwrap();

        qm.process();

        let updates = qm.take_updates();
        assert_eq!(
            updates.len(),
            1,
            "Should have subscription update for matching row"
        );
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Delta should contain the matching row"
        );

        // Verify the row data
        let row = &updates[0].delta.added[0];
        let values = decode_row(&descriptor, &row.data).unwrap();
        assert_eq!(values[0], Value::Text("HighScorer".into()));
        assert_eq!(values[1], Value::Integer(30));

        // --- Test 2: Synced row that does NOT match filter (score=20 < 25) ---

        let row_id_2 = crate::object::ObjectId::new();
        let author_2 = row_id_2;

        let mut metadata_2 = HashMap::new();
        metadata_2.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .receive_object(row_id_2, metadata_2);

        let data_2 = encode_row(
            &descriptor,
            &[Value::Text("LowScorer".into()), Value::Integer(20)],
        )
        .unwrap();

        let commit_2 = Commit {
            parents: smallvec![],
            content: data_2,
            timestamp: 2000,
            author: author_2,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id_2, ROW_BRANCH, commit_2)
            .unwrap();

        qm.process();

        let updates = qm.take_updates();
        // Should have NO updates because the row doesn't match the filter
        assert_eq!(
            updates.len(),
            0,
            "Should have no subscription update for non-matching row"
        );

        // But verify it's in the index (just not in the filtered subscription)
        let query = qm
            .query("users")
            .filter_eq("name", Value::Text("LowScorer".into()))
            .build();
        let results = qm.execute(query).unwrap();
        assert_eq!(
            results.len(),
            1,
            "Non-matching row should still be in index"
        );
    }

    // ========================================================================
    // Row content update propagation tests
    // ========================================================================

    #[test]
    fn local_update_emits_subscription_delta() {
        // Verify that local qm.update() causes subscription to emit an update delta
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Subscribe to all users
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to get the initial add
        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].delta.added.len(), 1);

        // Update the row
        qm.update(
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .unwrap();

        // Process
        qm.process();

        // Should have an update delta
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.updated.len(),
            1,
            "Delta should contain one updated row"
        );

        // Verify old and new values
        let (old_row, new_row) = &updates[0].delta.updated[0];
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let old_values =
            crate::query_manager::encoding::decode_row(&descriptor, &old_row.data).unwrap();
        let new_values =
            crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();

        assert_eq!(old_values[0], Value::Text("Alice".into()));
        assert_eq!(old_values[1], Value::Integer(100));
        assert_eq!(new_values[0], Value::Text("Alice Updated".into()));
        assert_eq!(new_values[1], Value::Integer(200));
    }

    #[test]
    fn synced_update_emits_subscription_delta() {
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::encode_row;

        // Verify that synced updates (receive_commit) cause subscription to emit update delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Subscribe to all objects for sync updates
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Insert a row locally first
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let row_id = handle.row_id;
        let first_commit_id = handle.row_commit_id;

        // Subscribe to all users
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to get the initial add
        qm.process();
        let _updates = qm.take_updates(); // Clear initial add

        // Now simulate a synced update
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let updated_data = encode_row(
            &descriptor,
            &[Value::Text("Alice Synced".into()), Value::Integer(300)],
        )
        .unwrap();

        let author = row_id;
        let update_commit = Commit {
            parents: smallvec![first_commit_id],
            content: updated_data,
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };
        qm.sync_manager_mut()
            .object_manager
            .receive_commit(row_id, ROW_BRANCH, update_commit)
            .unwrap();

        // Process
        qm.process();

        // Should have an update delta
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.updated.len(),
            1,
            "Delta should contain one updated row"
        );

        // Verify new values
        let (_old_row, new_row) = &updates[0].delta.updated[0];
        let new_values =
            crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();
        assert_eq!(new_values[0], Value::Text("Alice Synced".into()));
        assert_eq!(new_values[1], Value::Integer(300));
    }

    #[test]
    fn multiple_updates_same_row_single_delta() {
        // Verify that marking a row updated multiple times before process()
        // results in a single update delta reflecting final state

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert and subscribe
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        let query = qm.query("users").build();
        let _sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let _updates = qm.take_updates(); // Clear initial add

        // Update twice before process()
        qm.update(
            handle.row_id,
            &[Value::Text("Alice V2".into()), Value::Integer(200)],
        )
        .unwrap();
        qm.update(
            handle.row_id,
            &[Value::Text("Alice V3".into()), Value::Integer(300)],
        )
        .unwrap();

        // Single process()
        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(
            updates[0].delta.updated.len(),
            1,
            "Should have single update delta, not two"
        );

        // Verify it reflects final state
        let (_old_row, new_row) = &updates[0].delta.updated[0];
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let new_values =
            crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();
        assert_eq!(new_values[0], Value::Text("Alice V3".into()));
        assert_eq!(new_values[1], Value::Integer(300));
    }

    #[test]
    fn update_fails_filter_emits_removal() {
        // Verify: row passes filter, then update fails filter -> removal delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with score=100
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Subscribe to score > 50
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(50))
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Row should be added initially"
        );

        // Update score to 30 (fails filter)
        qm.update(
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(30)],
        )
        .unwrap();

        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.removed.len(),
            1,
            "Row should be removed when it fails filter"
        );
    }

    #[test]
    fn update_passes_filter_emits_addition() {
        // Verify: row fails filter initially, then update passes filter -> addition delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with score=30 (fails filter)
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(30)])
            .unwrap();

        // Subscribe to score > 50
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(50))
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let updates = qm.take_updates();
        // Row doesn't match filter, so no delta or empty delta
        assert!(updates.is_empty() || updates[0].delta.added.is_empty());

        // Update score to 100 (passes filter)
        qm.update(
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Row should be added when it now passes filter"
        );
    }

    #[test]
    fn update_still_passes_filter_emits_update() {
        // Verify: row passes filter, update still passes filter -> update delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with score=100
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Subscribe to score > 50
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(50))
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let _updates = qm.take_updates(); // Clear initial add

        // Update score to 200 (still passes filter)
        qm.update(
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .unwrap();

        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.updated.len(),
            1,
            "Row should be updated when it still passes filter"
        );
    }

    #[test]
    fn update_to_untracked_row_is_silent() {
        // Verify: row doesn't match filter, update still doesn't match -> no delta

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert row with score=30 (fails filter)
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(30)])
            .unwrap();

        // Subscribe to score > 50
        let query = qm
            .query("users")
            .filter_gt("score", Value::Integer(50))
            .build();
        let _sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let _updates = qm.take_updates();

        // Update score to 40 (still fails filter)
        qm.update(
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(40)],
        )
        .unwrap();

        qm.process();

        let updates = qm.take_updates();
        // Should be no updates (or empty delta)
        assert!(
            updates.is_empty()
                || (updates[0].delta.added.is_empty()
                    && updates[0].delta.removed.is_empty()
                    && updates[0].delta.updated.is_empty()),
            "No delta for row that doesn't match filter before or after update"
        );
    }

    #[test]
    fn insert_then_update_same_cycle() {
        // Verify: insert + update before process() -> single added delta with final values

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Subscribe first
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Insert
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Update before process()
        qm.update(
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .unwrap();

        // Single process()
        qm.process();

        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);

        // Should be added (not updated, since this is new to subscription)
        assert_eq!(updates[0].delta.added.len(), 1, "Row should be added");
        assert!(
            updates[0].delta.updated.is_empty(),
            "No spurious update delta"
        );

        // Verify final values
        let row = &updates[0].delta.added[0];
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let values = crate::query_manager::encoding::decode_row(&descriptor, &row.data).unwrap();
        assert_eq!(values[0], Value::Text("Alice Updated".into()));
        assert_eq!(values[1], Value::Integer(200));
    }

    // ========================================================================
    // End-to-End Sync Integration Tests (Followup 9)
    // ========================================================================

    #[test]
    fn sync_inbox_insert_flows_to_subscription_delta() {
        // End-to-end test: sync message → SyncManager inbox → QueryManager subscription
        // This tests the full path through push_inbox() → process_inbox() → process()
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::{decode_row, encode_row};
        use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Add a "server" that we'll receive updates from
        let server_id = ServerId::new();
        qm.sync_manager_mut().add_server(server_id);

        // Subscribe to all objects for sync updates
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Subscribe to users table
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to initialize (no updates yet)
        qm.process();
        let updates = qm.take_updates();
        assert!(updates.is_empty(), "No updates before sync message");

        // Construct the sync message payload
        let row_id = crate::object::ObjectId::new();
        let author = row_id;

        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let row_data = encode_row(
            &descriptor,
            &[Value::Text("SyncedUser".into()), Value::Integer(42)],
        )
        .unwrap();

        let commit = Commit {
            parents: smallvec![],
            content: row_data,
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: StoredState::Stored,
        };

        // Object metadata marking it as a "users" table row
        let mut obj_metadata = std::collections::HashMap::new();
        obj_metadata.insert("table".to_string(), "users".to_string());

        // Push the sync message through SyncManager's inbox
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(server_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: row_id,
                metadata: Some(crate::sync_manager::ObjectMetadata {
                    id: row_id,
                    metadata: obj_metadata,
                }),
                branch_name: ROW_BRANCH.into(),
                commits: vec![commit],
            },
        });

        // Process the inbox (SyncManager level)
        qm.sync_manager_mut().process_inbox();

        // Process (QueryManager level) - this should pick up the object update
        qm.process();

        // Verify subscription received the delta
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Delta should contain one added row"
        );

        // Verify the row contents
        let row = &updates[0].delta.added[0];
        let values = decode_row(&descriptor, &row.data).unwrap();
        assert_eq!(values[0], Value::Text("SyncedUser".into()));
        assert_eq!(values[1], Value::Integer(42));
    }

    #[test]
    fn sync_inbox_update_flows_to_subscription_delta() {
        // End-to-end test: sync update message → subscription emits update delta
        use crate::commit::{Commit, StoredState};
        use crate::query_manager::encoding::{decode_row, encode_row};
        use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Add a "server"
        let server_id = ServerId::new();
        qm.sync_manager_mut().add_server(server_id);
        qm.sync_manager_mut().object_manager.subscribe_all();

        // Insert a row locally first
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        let row_id = handle.row_id;
        let first_commit_id = handle.row_commit_id;

        // Subscribe to users
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to get initial state
        qm.process();
        let _ = qm.take_updates(); // Clear initial delta

        // Now simulate receiving an update from sync (as if another peer modified the row)
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let updated_data = encode_row(
            &descriptor,
            &[Value::Text("Alice Updated".into()), Value::Integer(999)],
        )
        .unwrap();

        let update_commit = Commit {
            parents: smallvec![first_commit_id],
            content: updated_data,
            timestamp: 2000,
            author: row_id,
            metadata: None,
            stored_state: StoredState::Stored,
        };

        // Push the update through SyncManager inbox
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(server_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: row_id,
                metadata: None, // No metadata needed for existing object
                branch_name: ROW_BRANCH.into(),
                commits: vec![update_commit],
            },
        });

        // Process both layers
        qm.sync_manager_mut().process_inbox();
        qm.process();

        // Verify subscription received update delta
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1, "Should have one subscription update");
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.updated.len(),
            1,
            "Delta should contain one updated row"
        );

        // Verify the new values
        let (_old_row, new_row) = &updates[0].delta.updated[0];
        let values = decode_row(&descriptor, &new_row.data).unwrap();
        assert_eq!(values[0], Value::Text("Alice Updated".into()));
        assert_eq!(values[1], Value::Integer(999));
    }

    #[test]
    fn two_peer_sync_insert_reaches_subscription() {
        // Full two-peer test: Peer A inserts → (simulated sync) → Peer B subscription delta
        // This demonstrates the conceptual flow even though we construct the payload manually
        use crate::commit::{Commit, StoredState};
        use crate::object::{BranchName, ObjectState};
        use crate::query_manager::encoding::decode_row;
        use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

        // Create two peers
        let sync_manager_a = SyncManager::new();
        let sync_manager_b = SyncManager::new();
        let schema = test_schema();
        let mut peer_a = QueryManager::new(sync_manager_a, schema.clone());
        let mut peer_b = QueryManager::new(sync_manager_b, schema);

        // Peer B subscribes to all objects and sets up query subscription
        peer_b.sync_manager_mut().object_manager.subscribe_all();
        let query = peer_b.query("users").build();
        let sub_id = peer_b.subscribe(query).unwrap();

        // Peer B adds a "server" (representing Peer A)
        let peer_a_as_server = ServerId::new();
        peer_b.sync_manager_mut().add_server(peer_a_as_server);

        // Process both to initialize
        peer_a.process();
        peer_b.process();
        let _ = peer_b.take_updates();

        // Peer A inserts a row
        let handle = peer_a
            .insert(
                "users",
                &[Value::Text("FromPeerA".into()), Value::Integer(123)],
            )
            .unwrap();
        let row_id = handle.row_id;

        // Get the actual commit data from Peer A's ObjectManager
        // This simulates "what would be sent over the wire"
        let (row_data, metadata) = {
            let state = peer_a
                .sync_manager()
                .object_manager
                .get_state(row_id)
                .unwrap();
            match state {
                ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                    let branch = obj.branches.get(&BranchName::new(ROW_BRANCH)).unwrap();
                    let tip_id = branch.tips.iter().next().unwrap();
                    let commit = branch.commits.get(tip_id).unwrap();
                    (commit.content.clone(), obj.metadata.clone())
                }
                _ => panic!("Object should be available"),
            }
        };

        // Construct the sync payload as it would appear on the wire
        let commit = Commit {
            parents: smallvec![],
            content: row_data,
            timestamp: 1000,
            author: row_id,
            metadata: None,
            stored_state: StoredState::Stored,
        };

        // Send to Peer B via SyncManager inbox
        peer_b.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(peer_a_as_server),
            payload: SyncPayload::ObjectUpdated {
                object_id: row_id,
                metadata: Some(crate::sync_manager::ObjectMetadata {
                    id: row_id,
                    metadata,
                }),
                branch_name: ROW_BRANCH.into(),
                commits: vec![commit],
            },
        });

        // Peer B processes the sync message
        peer_b.sync_manager_mut().process_inbox();
        peer_b.process();

        // Verify Peer B's subscription received the row
        let updates = peer_b.take_updates();
        assert_eq!(
            updates.len(),
            1,
            "Peer B should have one subscription update"
        );
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(
            updates[0].delta.added.len(),
            1,
            "Delta should contain one added row"
        );

        // Verify the row came from Peer A
        let row = &updates[0].delta.added[0];
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let values = decode_row(&descriptor, &row.data).unwrap();
        assert_eq!(values[0], Value::Text("FromPeerA".into()));
        assert_eq!(values[1], Value::Integer(123));
    }

    // ========================================================================
    // Soft Delete Tests
    // ========================================================================

    #[test]
    fn soft_delete_removes_from_id_index() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Verify row is in _id index
        assert!(qm.row_is_indexed("users", handle.row_id));

        // Delete the row
        let delete_handle = qm.delete(handle.row_id).unwrap();
        assert_eq!(delete_handle.row_id, handle.row_id);

        // Verify row is no longer in _id index
        assert!(!qm.row_is_indexed("users", handle.row_id));
    }

    #[test]
    fn soft_delete_adds_to_id_deleted_index() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Verify row is NOT in _id_deleted index
        assert!(!qm.row_is_deleted("users", handle.row_id));

        // Delete the row
        qm.delete(handle.row_id).unwrap();

        // Verify row IS in _id_deleted index
        assert!(qm.row_is_deleted("users", handle.row_id));
    }

    #[test]
    fn soft_deleted_row_not_in_query_results() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert rows
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();

        // Verify both rows are visible
        let query = qm.query("users").build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 2);

        // Delete Alice
        qm.delete(handle.row_id).unwrap();

        // Verify only Bob is visible
        let query = qm.query("users").build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Text("Bob".into()));
    }

    #[test]
    fn delete_already_deleted_row_fails() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Delete the row
        qm.delete(handle.row_id).unwrap();

        // Try to delete again - should fail
        let result = qm.delete(handle.row_id);
        assert!(matches!(result, Err(QueryError::RowAlreadyDeleted(_))));
    }

    #[test]
    fn soft_delete_with_concurrent_tips_uses_lww() {
        // Test that soft deleting an object with two concurrent tips results
        // in a soft delete commit with content from the LWW winner (highest timestamp).
        use crate::commit::{Commit, StoredState};
        use crate::object::{BranchName, ObjectState};
        use crate::query_manager::encoding::encode_row;

        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert(
                "users",
                &[Value::Text("Original".into()), Value::Integer(0)],
            )
            .unwrap();
        qm.process();

        // Get the initial commit as the common parent
        let branch_name = BranchName::new(ROW_BRANCH);
        let initial_tips: Vec<_> = qm
            .sync_manager
            .object_manager
            .get_tip_ids(handle.row_id, ROW_BRANCH)
            .unwrap()
            .iter()
            .copied()
            .collect();
        assert_eq!(initial_tips.len(), 1);
        let parent = initial_tips[0];

        // Create two concurrent updates with different timestamps and content.
        // Both have the same parent, creating diverging tips.
        let descriptor = qm
            .schema
            .get(&TableName::new("users"))
            .unwrap()
            .descriptor
            .clone();

        // Commit A: lower timestamp, content "TipA"
        let content_a = encode_row(
            &descriptor,
            &[Value::Text("TipA".into()), Value::Integer(100)],
        )
        .unwrap();
        let commit_a = Commit {
            author: handle.row_id,
            parents: smallvec![parent],
            content: content_a,
            timestamp: 1000, // Lower timestamp
            metadata: None,
            stored_state: StoredState::Pending,
        };

        // Commit B: higher timestamp, content "TipB" - this should win
        let content_b = encode_row(
            &descriptor,
            &[Value::Text("TipB".into()), Value::Integer(200)],
        )
        .unwrap();
        let commit_b = Commit {
            author: handle.row_id,
            parents: smallvec![parent],
            content: content_b.clone(),
            timestamp: 2000, // Higher timestamp - LWW winner
            metadata: None,
            stored_state: StoredState::Pending,
        };

        // Add both commits to create concurrent tips
        // We need to receive these as synced commits
        let commit_a_id = qm
            .sync_manager
            .object_manager
            .receive_commit(handle.row_id, ROW_BRANCH, commit_a)
            .unwrap();
        let commit_b_id = qm
            .sync_manager
            .object_manager
            .receive_commit(handle.row_id, ROW_BRANCH, commit_b)
            .unwrap();

        // Verify we now have concurrent tips
        let tips: Vec<_> = qm
            .sync_manager
            .object_manager
            .get_tip_ids(handle.row_id, ROW_BRANCH)
            .unwrap()
            .iter()
            .copied()
            .collect();
        assert_eq!(tips.len(), 2, "Should have 2 concurrent tips");
        assert!(tips.contains(&commit_a_id));
        assert!(tips.contains(&commit_b_id));

        // Process updates
        qm.process();

        // Now soft delete - should preserve content from LWW winner (commit_b, TipB)
        let delete_handle = qm.delete(handle.row_id).unwrap();

        // Get the delete commit and verify its content
        let state = qm
            .sync_manager
            .object_manager
            .get_state(handle.row_id)
            .unwrap();
        match state {
            ObjectState::Available(obj) | ObjectState::Creating(obj) => {
                let branch = obj.branches.get(&branch_name).unwrap();
                let delete_commit = branch.commits.get(&delete_handle.delete_commit_id).unwrap();

                // Verify the soft delete commit has content from the LWW winner (TipB)
                assert_eq!(
                    delete_commit.content, content_b,
                    "Soft delete should preserve content from LWW winner"
                );

                // Also verify metadata
                assert_eq!(
                    delete_commit
                        .metadata
                        .as_ref()
                        .and_then(|m| m.get("delete")),
                    Some(&"soft".to_string())
                );
            }
            _ => panic!("Object should be available"),
        }

        // Additionally verify that querying with include_deleted shows the correct content
        let query = qm.query("users").include_deleted().build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Text("TipB".into()));
        assert_eq!(results[0][1], Value::Integer(200));
    }

    // ========================================================================
    // Undelete Tests
    // ========================================================================

    #[test]
    fn undelete_adds_to_id_index() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Delete the row
        qm.delete(handle.row_id).unwrap();

        // Verify row is not in _id index
        assert!(!qm.row_is_indexed("users", handle.row_id));

        // Undelete with new values
        qm.undelete(
            handle.row_id,
            &[Value::Text("Alice Restored".into()), Value::Integer(150)],
        )
        .unwrap();

        // Verify row is back in _id index
        assert!(qm.row_is_indexed("users", handle.row_id));
    }

    #[test]
    fn undelete_removes_from_id_deleted_index() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Delete the row
        qm.delete(handle.row_id).unwrap();
        assert!(qm.row_is_deleted("users", handle.row_id));

        // Undelete
        qm.undelete(
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

        // Verify row is NOT in _id_deleted index
        assert!(!qm.row_is_deleted("users", handle.row_id));
    }

    #[test]
    fn undelete_row_appears_in_query_results() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Delete the row
        qm.delete(handle.row_id).unwrap();

        // Verify not visible
        let query = qm.query("users").build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 0);

        // Undelete with new values
        qm.undelete(
            handle.row_id,
            &[Value::Text("Alice Restored".into()), Value::Integer(200)],
        )
        .unwrap();

        // Verify visible again with new values
        let query = qm.query("users").build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Text("Alice Restored".into()));
        assert_eq!(results[0][1], Value::Integer(200));
    }

    #[test]
    fn undelete_nondeleted_row_fails() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Try to undelete a non-deleted row - should fail
        let result = qm.undelete(
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(100)],
        );
        assert!(matches!(result, Err(QueryError::RowNotDeleted(_))));
    }

    // ========================================================================
    // Hard Delete Tests
    // ========================================================================

    #[test]
    fn hard_delete_removes_from_id_index() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Hard delete the row
        qm.hard_delete(handle.row_id).unwrap();

        // Verify row is not in _id index
        assert!(!qm.row_is_indexed("users", handle.row_id));
    }

    #[test]
    fn hard_delete_removes_from_id_deleted_index() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Soft delete first (puts it in _id_deleted)
        qm.delete(handle.row_id).unwrap();
        assert!(qm.row_is_deleted("users", handle.row_id));

        // Then hard delete (removes from _id_deleted)
        qm.hard_delete(handle.row_id).unwrap();

        // Verify row is NOT in _id_deleted index
        assert!(!qm.row_is_deleted("users", handle.row_id));
    }

    #[test]
    fn hard_deleted_row_not_in_any_index() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Hard delete
        qm.hard_delete(handle.row_id).unwrap();

        // Verify row is not in _id index
        assert!(!qm.row_is_indexed("users", handle.row_id));
        // Verify row is not in _id_deleted index
        assert!(!qm.row_is_deleted("users", handle.row_id));
    }

    #[test]
    fn soft_then_hard_delete_removes_from_id_deleted() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Soft delete - row should be in _id_deleted
        qm.delete(handle.row_id).unwrap();
        assert!(qm.row_is_deleted("users", handle.row_id));

        // Hard delete - row should be removed from _id_deleted
        qm.hard_delete(handle.row_id).unwrap();
        assert!(!qm.row_is_deleted("users", handle.row_id));
    }

    #[test]
    fn undelete_hard_deleted_row_fails() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Hard delete
        qm.hard_delete(handle.row_id).unwrap();

        // Try to undelete - should fail
        let result = qm.undelete(
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(100)],
        );
        assert!(matches!(result, Err(QueryError::RowHardDeleted(_))));
    }

    // ========================================================================
    // Truncate Tests
    // ========================================================================

    #[test]
    fn truncate_soft_deleted_row() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Soft delete
        qm.delete(handle.row_id).unwrap();
        assert!(qm.row_is_deleted("users", handle.row_id));

        // Truncate (upgrade to hard delete)
        qm.truncate(handle.row_id).unwrap();

        // Verify row is completely gone
        assert!(!qm.row_is_indexed("users", handle.row_id));
        assert!(!qm.row_is_deleted("users", handle.row_id));
    }

    #[test]
    fn truncate_nondeleted_row_fails() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Try to truncate a non-deleted row - should fail
        let result = qm.truncate(handle.row_id);
        assert!(matches!(result, Err(QueryError::RowNotDeleted(_))));
    }

    // ========================================================================
    // Include Deleted Query Tests
    // ========================================================================

    #[test]
    fn include_deleted_query_returns_soft_deleted_rows() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert rows
        let handle1 = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();

        // Delete Alice
        qm.delete(handle1.row_id).unwrap();

        // Normal query - only Bob (Alice is in _id_deleted, not _id)
        let query = qm.query("users").build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Text("Bob".into()));

        // Include deleted query - scans both _id and _id_deleted indices
        // Soft-deleted rows have preserved content, so both Alice and Bob are returned
        let query = qm.query("users").include_deleted().build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 2);

        // Verify Alice's data is preserved
        let alice_result = results.iter().find(|r| r[0] == Value::Text("Alice".into()));
        assert!(alice_result.is_some());
        assert_eq!(alice_result.unwrap()[1], Value::Integer(100));

        // Verify that Alice is in the _id_deleted index
        assert!(qm.row_is_deleted("users", handle1.row_id));
    }

    #[test]
    fn include_deleted_query_does_not_return_hard_deleted_rows() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert rows
        let handle1 = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();

        // Hard delete Alice
        qm.hard_delete(handle1.row_id).unwrap();

        // Include deleted query - only Bob (Alice is hard deleted)
        let query = qm.query("users").include_deleted().build();
        let results = qm.execute(query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Text("Bob".into()));
    }

    // ========================================================================
    // Delete Subscription Delta Tests
    // ========================================================================

    #[test]
    fn soft_delete_emits_removal_delta() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Subscribe to all users
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to get initial delta
        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].delta.added.len(), 1); // Alice added

        // Delete Alice
        qm.delete(handle.row_id).unwrap();

        // Process and check for removal delta
        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(updates[0].delta.removed.len(), 1);
        assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
    }

    #[test]
    fn hard_delete_emits_removal_delta() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a row
        let handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();

        // Subscribe to all users
        let query = qm.query("users").build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to get initial delta
        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].delta.added.len(), 1); // Alice added

        // Hard delete Alice
        qm.hard_delete(handle.row_id).unwrap();

        // Process and check for removal delta
        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(updates[0].delta.removed.len(), 1);
        assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
    }

    #[test]
    fn delete_row_not_in_subscription_no_delta() {
        let sync_manager = SyncManager::new();
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert rows
        let alice_handle = qm
            .insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm.insert("users", &[Value::Text("Bob".into()), Value::Integer(50)])
            .unwrap();

        // Subscribe to users with score >= 75 (only Alice)
        let query = qm
            .query("users")
            .filter_ge("score", Value::Integer(75))
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to get initial delta
        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].delta.added.len(), 1); // Only Alice

        // Delete Alice (who IS in subscription) - should emit removal delta
        qm.delete(alice_handle.row_id).unwrap();

        // Process and verify we got removal delta
        qm.process();
        let updates = qm.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(updates[0].delta.removed.len(), 1);
    }

    // ========================================================================
    // Join integration tests
    // ========================================================================

    fn join_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    #[test]
    fn join_compiles_but_not_executed_yet() {
        // This test validates that join queries compile and don't panic,
        // even though full join execution is not yet implemented.
        // Once execute() supports joins, this test can be extended.
        let sync_manager = SyncManager::new();
        let schema = join_schema();
        let qm = QueryManager::new(sync_manager, schema);

        // Build a join query
        let query = qm
            .query("users")
            .join("posts")
            .on("id", "author_id")
            .build();

        // The query should compile successfully
        assert!(query.is_join());
        assert_eq!(query.joins.len(), 1);
    }

    #[test]
    fn join_query_with_projection_compiles() {
        let sync_manager = SyncManager::new();
        let schema = join_schema();
        let qm = QueryManager::new(sync_manager, schema);

        let query = qm
            .query("users")
            .join("posts")
            .on("id", "author_id")
            .select(&["name", "title"])
            .build();

        assert!(query.is_join());
        assert_eq!(
            query.select_columns,
            Some(vec!["name".to_string(), "title".to_string()])
        );
    }

    #[test]
    fn join_query_with_alias_compiles() {
        let sync_manager = SyncManager::new();
        let schema = join_schema();
        let qm = QueryManager::new(sync_manager, schema);

        let query = qm
            .query("users")
            .alias("u")
            .join("posts")
            .alias("p")
            .on("u.id", "p.author_id")
            .build();

        assert!(query.is_join());
        assert_eq!(query.alias, Some("u".to_string()));
        assert_eq!(query.joins[0].alias, Some("p".to_string()));
    }

    #[test]
    fn self_join_query_compiles() {
        // Self-join: employees with their managers
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("employees"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("manager_id", ColumnType::Integer).nullable(),
            ])
            .into(),
        );

        let sync_manager = SyncManager::new();
        let qm = QueryManager::new(sync_manager, schema);

        let query = qm
            .query("employees")
            .alias("e")
            .join("employees")
            .alias("m")
            .on("e.manager_id", "m.id")
            .build();

        assert!(query.is_join());
        assert_eq!(query.alias, Some("e".to_string()));
        assert_eq!(query.joins[0].table.as_str(), "employees");
        assert_eq!(query.joins[0].alias, Some("m".to_string()));
    }

    #[test]
    fn multi_join_query_compiles() {
        // Three-way join: orders -> customers, orders -> products
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("orders"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("customer_id", ColumnType::Integer),
                ColumnDescriptor::new("product_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("customers"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("products"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );

        let sync_manager = SyncManager::new();
        let qm = QueryManager::new(sync_manager, schema);

        let query = qm
            .query("orders")
            .join("customers")
            .on("customer_id", "id")
            .join("products")
            .on("product_id", "id")
            .build();

        assert!(query.is_join());
        assert_eq!(query.joins.len(), 2);
        assert_eq!(query.joins[0].table.as_str(), "customers");
        assert_eq!(query.joins[1].table.as_str(), "products");
    }

    #[test]
    fn join_subscription_marks_dirty_for_joined_table() {
        // This test verifies that inserts into a JOINED table (not the base table)
        // mark the join subscription as dirty. This is a regression test for a bug
        // where only the base table would trigger reactivity.
        //
        // We test this by checking that the subscription's index scan nodes for the
        // joined table get marked dirty when we insert into that table.
        let sync_manager = SyncManager::new();
        let schema = join_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Subscribe to a join query: users JOIN posts ON users.id = posts.author_id
        let query = qm
            .query("users")
            .join("posts")
            .on("id", "author_id")
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process once to settle initial state
        qm.process();
        let _ = qm.take_updates();

        // Verify the subscription has index scan nodes for BOTH tables
        let subscription = qm.subscriptions.get(&sub_id).unwrap();
        let tables_in_subscription: Vec<&str> = subscription
            .graph
            .index_scan_nodes
            .iter()
            .map(|(_, table, _)| table.as_str())
            .collect();
        assert!(
            tables_in_subscription.contains(&"users"),
            "Subscription should have index scan for users"
        );
        assert!(
            tables_in_subscription.contains(&"posts"),
            "Subscription should have index scan for posts"
        );

        // Clear dirty nodes
        qm.subscriptions
            .get_mut(&sub_id)
            .unwrap()
            .graph
            .clear_dirty();

        // Insert into the JOINED table (posts), not the base table (users)
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Test Post".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

        // BUG: mark_subscriptions_dirty() only checks subscription.graph.table.0 == "posts"
        // but the base table is "users", so the subscription won't be marked dirty.
        let subscription = qm.subscriptions.get(&sub_id).unwrap();
        assert!(
            subscription.graph.has_dirty_nodes(),
            "Join subscription should be marked dirty when joined table is modified"
        );
    }

    #[test]
    fn join_produces_combined_tuples() {
        // Test that a join produces tuples with elements from both tables.
        // This verifies basic join functionality and tuple structure.
        let sync_manager = SyncManager::new();
        let schema = join_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert a user
        let user_id = qm
            .insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();

        // Insert a post by that user
        let post_id = qm
            .insert(
                "posts",
                &[
                    Value::Integer(100),
                    Value::Text("Hello World".into()),
                    Value::Integer(1), // author_id matches user id
                ],
            )
            .unwrap();

        // Subscribe to a join query
        let query = qm
            .query("users")
            .join("posts")
            .on("id", "author_id")
            .build();
        let sub_id = qm.subscribe(query).unwrap();

        // Process to get join results
        qm.process();
        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have updates for subscription");

        // Should have one joined row
        assert_eq!(delta.added.len(), 1, "Should have one joined result");

        // The delta should contain columns from both tables
        // For now we can verify the row exists; combined descriptor testing
        // requires more infrastructure
        let row = &delta.added[0];
        assert!(!row.data.is_empty());

        // Verify we got a result (the join produced data)
        // Note: With joins, output currently uses base table descriptor,
        // so row.id will be the base table object ID (user_id).
        let _ = (user_id, post_id); // Both IDs were used in the join
    }

    #[test]
    fn join_filter_on_joined_table_column() {
        // Test filtering on a column from the JOINED table (not the base table).
        // FilterNode now uses TupleDescriptor to resolve column indices to correct tuple elements.
        let sync_manager = SyncManager::new();
        let schema = join_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert users
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();
        qm.insert("users", &[Value::Integer(2), Value::Text("Bob".into())])
            .unwrap();

        // Insert posts - one should match filter, one should not
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Hello World".into()), // Should NOT match "Rust"
                Value::Integer(1),
            ],
        )
        .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Learning Rust".into()), // SHOULD match filter
                Value::Integer(2),
            ],
        )
        .unwrap();

        // Join with filter on posts.title
        // TODO: This filter won't work correctly - it will try to match "Rust"
        // against users.id column because evaluate_tuple only looks at element[0]
        let query = qm
            .query("users")
            .join("posts")
            .on("id", "author_id")
            // This filter SHOULD match posts.title containing "Rust"
            // but currently it compares against users table
            .filter_eq("title", Value::Text("Learning Rust".into()))
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();
        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have updates");

        // Should only have one result (the "Learning Rust" post)
        assert_eq!(
            delta.added.len(),
            1,
            "Filter on joined table column should work"
        );
    }

    // ========================================================================
    // Array subquery (correlated subquery) tests
    // ========================================================================

    fn users_posts_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    /// Output descriptor for users with posts array subquery.
    fn users_with_posts_descriptor() -> RowDescriptor {
        // Posts row descriptor: [id, title, author_id]
        let posts_row_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ]);
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new(
                "posts",
                ColumnType::Array(Box::new(ColumnType::Row(Box::new(posts_row_desc)))),
            ),
        ])
    }

    #[test]
    fn array_subquery_single_user_with_posts() {
        let sync_manager = SyncManager::new();
        let schema = users_posts_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert one user: Alice with id=1
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();

        // Insert two posts for Alice
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Alice Post 1".into()),
                Value::Integer(1), // author_id = 1 (Alice)
            ],
        )
        .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Alice Post 2".into()),
                Value::Integer(1), // author_id = 1 (Alice)
            ],
        )
        .unwrap();

        // Query users with their posts as array
        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have subscription update");

        // Should have exactly 1 user row
        assert_eq!(delta.added.len(), 1, "Expected 1 user row");

        // Decode the output row
        let output_descriptor = users_with_posts_descriptor();
        let row_data = &delta.added[0].data;
        let values = decode_row(&output_descriptor, row_data).expect("Should decode output row");

        // Verify user fields
        assert_eq!(values[0], Value::Integer(1), "User id should be 1");
        assert_eq!(
            values[1],
            Value::Text("Alice".into()),
            "User name should be Alice"
        );

        // Verify posts array
        let posts = values[2].as_array().expect("Third column should be array");
        assert_eq!(posts.len(), 2, "Alice should have 2 posts");

        // Each post is a Row of [id, title, author_id]
        for post in posts {
            let post_values = post.as_row().expect("Each post should be a Row");
            assert_eq!(post_values.len(), 3, "Post should have 3 fields");
            // Verify author_id matches Alice
            assert_eq!(
                post_values[2],
                Value::Integer(1),
                "Post author_id should be 1 (Alice)"
            );
        }
    }

    #[test]
    fn array_subquery_user_with_no_posts() {
        let sync_manager = SyncManager::new();
        let schema = users_posts_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert user with no posts
        qm.insert("users", &[Value::Integer(1), Value::Text("Lonely".into())])
            .unwrap();

        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have subscription update");

        assert_eq!(delta.added.len(), 1, "Should have 1 user");

        let output_descriptor = users_with_posts_descriptor();
        let values =
            decode_row(&output_descriptor, &delta.added[0].data).expect("Should decode output row");

        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Text("Lonely".into()));

        // Posts array should be empty
        let posts = values[2].as_array().expect("Should have posts array");
        assert_eq!(posts.len(), 0, "User with no posts should have empty array");
    }

    #[test]
    fn array_subquery_multiple_users_correct_correlation() {
        let sync_manager = SyncManager::new();
        let schema = users_posts_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert users
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();
        qm.insert("users", &[Value::Integer(2), Value::Text("Bob".into())])
            .unwrap();

        // Alice's posts (author_id = 1)
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Alice Post".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

        // Bob's posts (author_id = 2)
        qm.insert(
            "posts",
            &[
                Value::Integer(200),
                Value::Text("Bob Post".into()),
                Value::Integer(2),
            ],
        )
        .unwrap();

        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have updates");

        assert_eq!(delta.added.len(), 2, "Should have 2 users");

        let output_descriptor = users_with_posts_descriptor();

        // Build a map of user_id -> posts for verification
        let mut user_posts: std::collections::HashMap<i32, Vec<i32>> =
            std::collections::HashMap::new();
        for row in &delta.added {
            let values = decode_row(&output_descriptor, &row.data).expect("decode");
            let user_id = match &values[0] {
                Value::Integer(id) => *id,
                _ => panic!("User id should be integer"),
            };
            let posts = values[2].as_array().expect("posts array");
            let post_ids: Vec<i32> = posts
                .iter()
                .filter_map(|p| {
                    let row_vals = p.as_row()?;
                    match &row_vals[0] {
                        Value::Integer(id) => Some(*id),
                        _ => None,
                    }
                })
                .collect();
            user_posts.insert(user_id, post_ids);
        }

        // Alice (id=1) should have post 100
        assert_eq!(
            user_posts.get(&1),
            Some(&vec![100]),
            "Alice should have post 100"
        );

        // Bob (id=2) should have post 200
        assert_eq!(
            user_posts.get(&2),
            Some(&vec![200]),
            "Bob should have post 200"
        );
    }

    #[test]
    fn array_subquery_delta_on_inner_insert() {
        // Test: after subscription, inserting a new post should emit a delta
        // with the updated user row containing the new post in the array.
        let sync_manager = SyncManager::new();
        let schema = users_posts_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert user Alice
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();

        // Insert initial post
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Post 1".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

        // Subscribe to users with posts
        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        // Consume initial update
        let initial_updates = qm.take_updates();
        let initial_delta = initial_updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have initial update");
        assert_eq!(initial_delta.added.len(), 1, "Initial: 1 user");

        // Verify initial state: Alice has 1 post
        let output_descriptor = users_with_posts_descriptor();
        let initial_values =
            decode_row(&output_descriptor, &initial_delta.added[0].data).expect("decode initial");
        let initial_posts = initial_values[2].as_array().expect("posts array");
        assert_eq!(initial_posts.len(), 1, "Initially Alice has 1 post");

        // NOW: Insert a new post for Alice
        qm.insert(
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Post 2".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
        qm.process();

        // Check delta after inner insert
        let updates_after_insert = qm.take_updates();
        let delta_after = updates_after_insert
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have delta after post insert");

        // Should have an update (old row removed, new row with updated array added)
        // or just updated entries
        let total_changes = delta_after.added.len() + delta_after.updated.len();
        assert!(
            total_changes > 0,
            "Should have changes after inserting post"
        );

        // Find the new state - either in added or as new part of updated
        let new_row_data = if !delta_after.added.is_empty() {
            &delta_after.added[0].data
        } else if !delta_after.updated.is_empty() {
            &delta_after.updated[0].1.data
        } else {
            panic!("Expected added or updated row");
        };

        let new_values = decode_row(&output_descriptor, new_row_data).expect("decode new");
        let new_posts = new_values[2].as_array().expect("posts array");
        assert_eq!(
            new_posts.len(),
            2,
            "After insert, Alice should have 2 posts"
        );

        // Verify both post IDs are present
        let post_ids: Vec<i32> = new_posts
            .iter()
            .filter_map(|p| match &p.as_row()?[0] {
                Value::Integer(id) => Some(*id),
                _ => None,
            })
            .collect();
        assert!(post_ids.contains(&100), "Should contain post 100");
        assert!(post_ids.contains(&101), "Should contain post 101");
    }

    #[test]
    fn array_subquery_delta_on_outer_insert() {
        // Test: after subscription, inserting a new user should emit a delta
        // with the new user row (with their posts array).
        let sync_manager = SyncManager::new();
        let schema = users_posts_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert user Alice with a post
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Alice Post".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

        // Also insert a post for Bob (who doesn't exist yet)
        qm.insert(
            "posts",
            &[
                Value::Integer(200),
                Value::Text("Bob Post".into()),
                Value::Integer(2),
            ],
        )
        .unwrap();

        // Subscribe
        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        // Consume initial update (just Alice)
        let initial_updates = qm.take_updates();
        let initial_delta = initial_updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have initial update");
        assert_eq!(initial_delta.added.len(), 1, "Initial: only Alice");

        // NOW: Insert Bob
        qm.insert("users", &[Value::Integer(2), Value::Text("Bob".into())])
            .unwrap();
        qm.process();

        // Check delta after outer insert
        let updates_after = qm.take_updates();
        let delta_after = updates_after
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have delta after user insert");

        // Should have Bob added
        assert_eq!(delta_after.added.len(), 1, "Bob should be added");

        let output_descriptor = users_with_posts_descriptor();
        let bob_values =
            decode_row(&output_descriptor, &delta_after.added[0].data).expect("decode Bob");

        assert_eq!(bob_values[0], Value::Integer(2), "Should be Bob (id=2)");
        assert_eq!(
            bob_values[1],
            Value::Text("Bob".into()),
            "Name should be Bob"
        );

        // Bob should have his post (id=200)
        let bob_posts = bob_values[2].as_array().expect("posts array");
        assert_eq!(bob_posts.len(), 1, "Bob should have 1 post");

        let post_row = bob_posts[0].as_row().expect("post should be Row");
        assert_eq!(
            post_row[0],
            Value::Integer(200),
            "Bob's post should be id=200"
        );
    }

    #[test]
    fn array_subquery_with_order_by() {
        // Test: posts should be ordered by id descending
        let sync_manager = SyncManager::new();
        let schema = users_posts_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert user
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();

        // Insert posts in random order
        qm.insert(
            "posts",
            &[
                Value::Integer(102),
                Value::Text("Middle".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("First".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Last".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

        // Query with order_by_desc on id
        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .order_by_desc("id")
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have update");

        let output_descriptor = users_with_posts_descriptor();
        let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
        let posts = values[2].as_array().expect("posts array");

        assert_eq!(posts.len(), 3, "Should have 3 posts");

        // Verify order: should be 102, 101, 100 (descending by id)
        let post_ids: Vec<i32> = posts
            .iter()
            .filter_map(|p| match &p.as_row()?[0] {
                Value::Integer(id) => Some(*id),
                _ => None,
            })
            .collect();
        assert_eq!(
            post_ids,
            vec![102, 101, 100],
            "Posts should be ordered by id desc"
        );
    }

    #[test]
    fn array_subquery_with_limit() {
        // Test: limit should restrict number of posts returned
        let sync_manager = SyncManager::new();
        let schema = users_posts_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert user
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();

        // Insert 5 posts
        for i in 100..105 {
            qm.insert(
                "posts",
                &[
                    Value::Integer(i),
                    Value::Text(format!("Post {}", i).into()),
                    Value::Integer(1),
                ],
            )
            .unwrap();
        }

        // Query with limit 2
        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .order_by("id")
                    .limit(2)
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have update");

        let output_descriptor = users_with_posts_descriptor();
        let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
        let posts = values[2].as_array().expect("posts array");

        assert_eq!(posts.len(), 2, "Limit should restrict to 2 posts");

        // Verify first 2 posts by id ascending
        let post_ids: Vec<i32> = posts
            .iter()
            .filter_map(|p| match &p.as_row()?[0] {
                Value::Integer(id) => Some(*id),
                _ => None,
            })
            .collect();
        assert_eq!(post_ids, vec![100, 101], "Should get first 2 posts by id");
    }

    #[test]
    fn array_subquery_with_select_columns() {
        // Test: select specific columns from inner query
        let sync_manager = SyncManager::new();
        let schema = users_posts_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert user and post
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Post Title".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

        // Query selecting only id and title (not author_id)
        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .select(&["id", "title"])
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have update");

        // Build descriptor for selected columns only
        let posts_row_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
        ]);
        let output_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new(
                "posts",
                ColumnType::Array(Box::new(ColumnType::Row(Box::new(posts_row_desc)))),
            ),
        ]);

        let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
        let posts = values[2].as_array().expect("posts array");

        assert_eq!(posts.len(), 1, "Should have 1 post");

        let post_row = posts[0].as_row().expect("post Row");
        assert_eq!(post_row.len(), 2, "Post should have 2 columns (id, title)");
        assert_eq!(post_row[0], Value::Integer(100));
        assert_eq!(post_row[1], Value::Text("Post Title".into()));
    }

    #[test]
    fn array_subquery_with_join() {
        // Test: join inside array subquery
        // users with_array of (posts joined with comments)
        let sync_manager = SyncManager::new();
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("comments"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("text", ColumnType::Text),
                ColumnDescriptor::new("post_id", ColumnType::Integer),
            ])
            .into(),
        );

        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert user
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();

        // Insert posts
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Post A".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Post B".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

        // Insert comments
        qm.insert(
            "comments",
            &[
                Value::Integer(1000),
                Value::Text("Comment on A".into()),
                Value::Integer(100), // post_id = 100
            ],
        )
        .unwrap();
        qm.insert(
            "comments",
            &[
                Value::Integer(1001),
                Value::Text("Another on A".into()),
                Value::Integer(100), // post_id = 100
            ],
        )
        .unwrap();
        qm.insert(
            "comments",
            &[
                Value::Integer(1002),
                Value::Text("Comment on B".into()),
                Value::Integer(101), // post_id = 101
            ],
        )
        .unwrap();

        // Query users with (posts joined with comments)
        // This should give us: for each user, an array of (post, comment) pairs
        let query = qm
            .query("users")
            .with_array("post_comments", |sub| {
                sub.from("posts")
                    .join("comments")
                    .on("posts.id", "comments.post_id")
                    .correlate("author_id", "users.id")
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have update");

        // Build descriptor for joined output:
        // posts columns + comments columns
        let joined_row_desc = RowDescriptor::new(vec![
            // posts columns
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
            // comments columns
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("post_id", ColumnType::Integer),
        ]);
        let output_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new(
                "post_comments",
                ColumnType::Array(Box::new(ColumnType::Row(Box::new(joined_row_desc)))),
            ),
        ]);

        assert_eq!(delta.added.len(), 1, "Should have 1 user");
        let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
        assert_eq!(values[0], Value::Integer(1)); // user id
        assert_eq!(values[1], Value::Text("Alice".into())); // user name

        let post_comments = values[2].as_array().expect("post_comments array");
        // Each (post, comment) pair - Post A has 2 comments, Post B has 1
        assert_eq!(post_comments.len(), 3, "Should have 3 post-comment pairs");

        // Verify the joined rows contain both post and comment data
        for pc in post_comments {
            let row = pc.as_row().expect("joined row");
            assert_eq!(row.len(), 6, "Joined row should have 6 columns");
            // Post id should be either 100 or 101
            let post_id = match &row[0] {
                Value::Integer(id) => *id,
                _ => panic!("Expected integer for post id"),
            };
            assert!(post_id == 100 || post_id == 101);
            // Comment post_id should match the post id
            assert_eq!(row[5], Value::Integer(post_id));
        }
    }

    #[test]
    fn array_subquery_nested() {
        // Test: nested array subqueries
        // users with_array(posts with_array(comments))
        let sync_manager = SyncManager::new();
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("comments"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("text", ColumnType::Text),
                ColumnDescriptor::new("post_id", ColumnType::Integer),
            ])
            .into(),
        );

        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert user
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();

        // Insert posts
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Post A".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Post B".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

        // Insert comments - 2 on Post A, 1 on Post B
        qm.insert(
            "comments",
            &[
                Value::Integer(1000),
                Value::Text("Comment 1 on A".into()),
                Value::Integer(100),
            ],
        )
        .unwrap();
        qm.insert(
            "comments",
            &[
                Value::Integer(1001),
                Value::Text("Comment 2 on A".into()),
                Value::Integer(100),
            ],
        )
        .unwrap();
        qm.insert(
            "comments",
            &[
                Value::Integer(1002),
                Value::Text("Comment on B".into()),
                Value::Integer(101),
            ],
        )
        .unwrap();

        // Query: users with posts, where each post has its comments
        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .with_array("comments", |sub2| {
                        sub2.from("comments").correlate("post_id", "posts.id")
                    })
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have update");

        // Build nested descriptor:
        // comments row: [id, text, post_id]
        let comments_row_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("post_id", ColumnType::Integer),
        ]);
        // posts row with comments array: [id, title, author_id, comments[]]
        let posts_row_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
            ColumnDescriptor::new(
                "comments",
                ColumnType::Array(Box::new(ColumnType::Row(Box::new(comments_row_desc)))),
            ),
        ]);
        // users row with posts array: [id, name, posts[]]
        let output_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new(
                "posts",
                ColumnType::Array(Box::new(ColumnType::Row(Box::new(posts_row_desc)))),
            ),
        ]);

        assert_eq!(delta.added.len(), 1, "Should have 1 user");
        let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
        assert_eq!(values[0], Value::Integer(1)); // user id
        assert_eq!(values[1], Value::Text("Alice".into())); // user name

        let posts = values[2].as_array().expect("posts array");
        assert_eq!(posts.len(), 2, "Alice should have 2 posts");

        // Check each post has its comments
        for post in posts {
            let post_row = post.as_row().expect("post row");
            assert_eq!(
                post_row.len(),
                4,
                "Post should have 4 columns (id, title, author_id, comments)"
            );

            let post_id = match &post_row[0] {
                Value::Integer(id) => *id,
                _ => panic!("Expected integer for post id"),
            };

            let comments = post_row[3].as_array().expect("comments array");

            if post_id == 100 {
                // Post A has 2 comments
                assert_eq!(comments.len(), 2, "Post A should have 2 comments");
                for comment in comments {
                    let comment_row = comment.as_row().expect("comment row");
                    assert_eq!(comment_row[2], Value::Integer(100)); // post_id
                }
            } else if post_id == 101 {
                // Post B has 1 comment
                assert_eq!(comments.len(), 1, "Post B should have 1 comment");
                let comment_row = comments[0].as_row().expect("comment row");
                assert_eq!(comment_row[2], Value::Integer(101)); // post_id
            } else {
                panic!("Unexpected post id: {}", post_id);
            }
        }
    }

    #[test]
    fn array_subquery_multiple_columns() {
        // Test: two separate (non-nested) array subquery columns
        // users with posts[] and with comments[] (comments directly on user)
        let sync_manager = SyncManager::new();
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("comments"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("text", ColumnType::Text),
                ColumnDescriptor::new("user_id", ColumnType::Integer),
            ])
            .into(),
        );

        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert users
        qm.insert("users", &[Value::Integer(1), Value::Text("Alice".into())])
            .unwrap();
        qm.insert("users", &[Value::Integer(2), Value::Text("Bob".into())])
            .unwrap();

        // Insert posts - Alice has 2, Bob has 1
        qm.insert(
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Alice Post 1".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Alice Post 2".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
        qm.insert(
            "posts",
            &[
                Value::Integer(102),
                Value::Text("Bob Post".into()),
                Value::Integer(2),
            ],
        )
        .unwrap();

        // Insert comments (directly on users) - Alice has 1, Bob has 2
        qm.insert(
            "comments",
            &[
                Value::Integer(1000),
                Value::Text("Alice comment".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
        qm.insert(
            "comments",
            &[
                Value::Integer(1001),
                Value::Text("Bob comment 1".into()),
                Value::Integer(2),
            ],
        )
        .unwrap();
        qm.insert(
            "comments",
            &[
                Value::Integer(1002),
                Value::Text("Bob comment 2".into()),
                Value::Integer(2),
            ],
        )
        .unwrap();

        // Query: users with both posts[] and comments[]
        let query = qm
            .query("users")
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .with_array("comments", |sub| {
                sub.from("comments").correlate("user_id", "users.id")
            })
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let updates = qm.take_updates();
        let delta = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .map(|u| &u.delta)
            .expect("Should have update");

        // Build descriptor: users + posts[] + comments[]
        let posts_row_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ]);
        let comments_row_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("user_id", ColumnType::Integer),
        ]);
        let output_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new(
                "posts",
                ColumnType::Array(Box::new(ColumnType::Row(Box::new(posts_row_desc)))),
            ),
            ColumnDescriptor::new(
                "comments",
                ColumnType::Array(Box::new(ColumnType::Row(Box::new(comments_row_desc)))),
            ),
        ]);

        assert_eq!(delta.added.len(), 2, "Should have 2 users");

        // Decode and verify each user
        for row in &delta.added {
            let values = decode_row(&output_descriptor, &row.data).expect("decode");
            let user_id = match &values[0] {
                Value::Integer(id) => *id,
                _ => panic!("Expected integer for user id"),
            };

            let posts = values[2].as_array().expect("posts array");
            let comments = values[3].as_array().expect("comments array");

            if user_id == 1 {
                // Alice: 2 posts, 1 comment
                assert_eq!(values[1], Value::Text("Alice".into()));
                assert_eq!(posts.len(), 2, "Alice should have 2 posts");
                assert_eq!(comments.len(), 1, "Alice should have 1 comment");
            } else if user_id == 2 {
                // Bob: 1 post, 2 comments
                assert_eq!(values[1], Value::Text("Bob".into()));
                assert_eq!(posts.len(), 1, "Bob should have 1 post");
                assert_eq!(comments.len(), 2, "Bob should have 2 comments");
            } else {
                panic!("Unexpected user id: {}", user_id);
            }
        }
    }

    // ========================================================================
    // Policy (ReBAC) integration tests
    // ========================================================================

    use crate::query_manager::policy::PolicyExpr;
    use crate::query_manager::session::Session as PolicySession;
    use crate::query_manager::types::{TablePolicies, TableSchema};
    use serde_json::json;

    fn policy_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("documents"),
            TableSchema::with_policies(
                RowDescriptor::new(vec![
                    ColumnDescriptor::new("owner_id", ColumnType::Text),
                    ColumnDescriptor::new("team_id", ColumnType::Text),
                    ColumnDescriptor::new("title", ColumnType::Text),
                ]),
                TablePolicies::new().with_select(
                    // owner_id = @session.user_id OR team_id IN @session.claims.teams
                    PolicyExpr::or(vec![
                        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                        PolicyExpr::in_session("team_id", vec!["claims".into(), "teams".into()]),
                    ]),
                ),
            ),
        );
        schema
    }

    #[test]
    fn policy_filters_select_results() {
        let sync_manager = SyncManager::new();
        let schema = policy_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert documents
        qm.insert(
            "documents",
            &[
                Value::Text("alice".into()),
                Value::Text("eng".into()),
                Value::Text("Alice's eng doc".into()),
            ],
        )
        .unwrap();
        qm.insert(
            "documents",
            &[
                Value::Text("bob".into()),
                Value::Text("eng".into()),
                Value::Text("Bob's eng doc".into()),
            ],
        )
        .unwrap();
        qm.insert(
            "documents",
            &[
                Value::Text("bob".into()),
                Value::Text("sales".into()),
                Value::Text("Bob's sales doc".into()),
            ],
        )
        .unwrap();
        qm.insert(
            "documents",
            &[
                Value::Text("charlie".into()),
                Value::Text("design".into()),
                Value::Text("Charlie's design doc".into()),
            ],
        )
        .unwrap();

        // Alice can see: her own doc + all eng docs = 2 docs
        let alice_session = PolicySession::new("alice").with_claims(json!({"teams": ["eng"]}));

        let query = qm.query("documents").build();
        let sub_id = qm
            .subscribe_with_session(query, Some(alice_session))
            .unwrap();

        qm.process();
        let updates = qm.take_updates();
        let alice_update = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .unwrap();

        assert_eq!(
            alice_update.delta.added.len(),
            2,
            "Alice should see 2 docs (her own + Bob's eng doc)"
        );

        // Bob on sales team can see: his 2 docs + no team docs (sales only) = 2 docs
        let bob_session = PolicySession::new("bob").with_claims(json!({"teams": ["sales"]}));

        let query2 = qm.query("documents").build();
        let sub_id2 = qm
            .subscribe_with_session(query2, Some(bob_session))
            .unwrap();

        qm.process();
        let updates2 = qm.take_updates();
        let bob_update = updates2
            .iter()
            .find(|u| u.subscription_id == sub_id2)
            .unwrap();

        assert_eq!(
            bob_update.delta.added.len(),
            2,
            "Bob should see 2 docs (his own 2 docs)"
        );
    }

    #[test]
    fn no_session_returns_all_rows() {
        let sync_manager = SyncManager::new();
        let schema = policy_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        // Insert documents
        qm.insert(
            "documents",
            &[
                Value::Text("alice".into()),
                Value::Text("eng".into()),
                Value::Text("Doc 1".into()),
            ],
        )
        .unwrap();
        qm.insert(
            "documents",
            &[
                Value::Text("bob".into()),
                Value::Text("sales".into()),
                Value::Text("Doc 2".into()),
            ],
        )
        .unwrap();

        // Without session, all rows should be returned (policy not applied)
        let query = qm.query("documents").build();
        let sub_id = qm.subscribe(query).unwrap();

        qm.process();
        let updates = qm.take_updates();
        let update = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .unwrap();

        assert_eq!(
            update.delta.added.len(),
            2,
            "Without session, should see all 2 docs"
        );
    }

    #[test]
    fn table_without_policy_returns_all_rows() {
        let sync_manager = SyncManager::new();
        // Use the regular test_schema which has no policies
        let schema = test_schema();
        let mut qm = QueryManager::new(sync_manager, schema);

        qm.insert("users", &[Value::Text("Alice".into()), Value::Integer(100)])
            .unwrap();
        qm.insert("users", &[Value::Text("Bob".into()), Value::Integer(200)])
            .unwrap();

        // Even with session, table without policy returns all rows
        let session = PolicySession::new("some_user");
        let query = qm.query("users").build();
        let sub_id = qm.subscribe_with_session(query, Some(session)).unwrap();

        qm.process();
        let updates = qm.take_updates();
        let update = updates
            .iter()
            .find(|u| u.subscription_id == sub_id)
            .unwrap();

        assert_eq!(
            update.delta.added.len(),
            2,
            "Table without policy should return all rows"
        );
    }
}
