use std::collections::HashMap;
use std::sync::Arc;

use crate::commit::{CommitId, StoredState};
use crate::object::{BranchName, ObjectId, ObjectState};
use crate::object_manager::AllObjectUpdate;
use crate::sync_manager::{PendingPermissionCheck, PendingUpdateId, SyncManager};

use super::encoding::{decode_row, encode_row};
use super::graph::QueryGraph;
use super::graph_nodes::output::QuerySubscriptionId;
use super::graph_nodes::{IndexKey, IndicesMap};
use super::index::{BTreeIndex, IndexError};
use super::policy::{ComplexClause, Operation, evaluate_simple_parts, resolve_session_value};
use super::policy_graph::PolicyGraph;
use super::query::{Query, QueryBuilder};
use super::session::Session;
use super::types::{Row, RowDelta, RowDescriptor, Schema, TableName, TableSchema, Value};

/// Default row branch name for backward compatibility during migration.
/// TODO: Remove once all APIs explicitly specify branch.
const DEFAULT_ROW_BRANCH: &str = "main";

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
pub(crate) struct QuerySubscription {
    pub(crate) graph: QueryGraph,
    #[allow(dead_code)]
    pub(crate) mode: SubscriptionMode,
    /// Branches to read from (inherited from query at subscription time).
    pub(crate) branches: Vec<String>,
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

    /// Indices: (table, column, branch) -> BTreeIndex
    /// Each branch maintains its own set of indices.
    indices: IndicesMap,

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
        // Initialize indices for all tables on the default branch
        let mut indices = IndicesMap::default();
        for (table_name, table_schema) in &schema {
            Self::ensure_table_indices_for_branch(
                &mut indices,
                table_name.as_str(),
                DEFAULT_ROW_BRANCH,
                table_schema,
            );
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

    /// Ensure indices exist for a table on a specific branch.
    fn ensure_table_indices_for_branch(
        indices: &mut IndicesMap,
        table: &str,
        branch: &str,
        table_schema: &TableSchema,
    ) {
        let key =
            |col: &str| -> IndexKey { (table.to_string(), col.to_string(), branch.to_string()) };

        // Primary "_id" index (for live rows)
        if !indices.contains_key(&key("_id")) {
            let mut id_index = BTreeIndex::new(table, "_id");
            id_index.process_meta_load(None);
            indices.insert(key("_id"), id_index);
        }

        // Soft-deleted rows index
        if !indices.contains_key(&key("_id_deleted")) {
            let mut deleted_index = BTreeIndex::new(table, "_id_deleted");
            deleted_index.process_meta_load(None);
            indices.insert(key("_id_deleted"), deleted_index);
        }

        // Index for each column
        for col in &table_schema.descriptor.columns {
            let col_str = col.name.as_str();
            if !indices.contains_key(&key(col_str)) {
                let mut col_index = BTreeIndex::new(table, col_str);
                col_index.process_meta_load(None);
                indices.insert(key(col_str), col_index);
            }
        }
    }

    /// Get a reference to an index by (table, column, branch).
    fn get_index(&self, table: &str, column: &str, branch: &str) -> Option<&BTreeIndex> {
        let key: IndexKey = (table.to_string(), column.to_string(), branch.to_string());
        self.indices.get(&key)
    }

    /// Get a mutable reference to an index by (table, column, branch).
    fn get_index_mut(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
    ) -> Option<&mut BTreeIndex> {
        let key: IndexKey = (table.to_string(), column.to_string(), branch.to_string());
        self.indices.get_mut(&key)
    }

    /// Get the underlying SyncManager.
    pub fn sync_manager(&self) -> &SyncManager {
        &self.sync_manager
    }

    /// Get mutable reference to the underlying SyncManager.
    pub fn sync_manager_mut(&mut self) -> &mut SyncManager {
        &mut self.sync_manager
    }

    /// Get the schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Test accessor for subscriptions (internal testing only).
    #[cfg(test)]
    pub fn test_subscriptions_mut(
        &mut self,
    ) -> &mut HashMap<super::graph_nodes::output::QuerySubscriptionId, QuerySubscription> {
        &mut self.subscriptions
    }

    /// Test accessor for subscriptions (internal testing only).
    #[cfg(test)]
    pub fn test_subscriptions(
        &self,
    ) -> &HashMap<super::graph_nodes::output::QuerySubscriptionId, QuerySubscription> {
        &self.subscriptions
    }

    /// Check if a row is indexed on a specific branch (appears in the _id index).
    pub fn row_is_indexed_on_branch(&self, table: &str, branch: &str, row_id: ObjectId) -> bool {
        self.get_index(table, "_id", branch)
            .is_some_and(|index| index.contains_row(row_id))
    }

    /// Check if a row is indexed on the default branch (appears in the _id index).
    pub fn row_is_indexed(&self, table: &str, row_id: ObjectId) -> bool {
        self.row_is_indexed_on_branch(table, DEFAULT_ROW_BRANCH, row_id)
    }

    /// Check if a row is soft-deleted on a specific branch.
    pub fn row_is_deleted_on_branch(&self, table: &str, branch: &str, row_id: ObjectId) -> bool {
        self.get_index(table, "_id_deleted", branch)
            .is_some_and(|index| index.contains_row(row_id))
    }

    /// Check if a row is soft-deleted (appears in _id_deleted but not _id).
    pub fn row_is_deleted(&self, table: &str, row_id: ObjectId) -> bool {
        self.row_is_deleted_on_branch(table, DEFAULT_ROW_BRANCH, row_id)
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
        let Some(branch) = obj.branches.get(&BranchName::new(DEFAULT_ROW_BRANCH)) else {
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
        let Some(branch) = obj.branches.get(&BranchName::new(DEFAULT_ROW_BRANCH)) else {
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
            .add_commit(
                object_id,
                DEFAULT_ROW_BRANCH,
                vec![],
                data.clone(),
                author,
                None,
            )
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
            .get_tip_ids(id, DEFAULT_ROW_BRANCH)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Add commit with new data
        let _commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                id,
                DEFAULT_ROW_BRANCH,
                parents,
                new_data.clone(),
                author,
                None,
            )
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
            .get_tip_ids(id, DEFAULT_ROW_BRANCH)
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
                DEFAULT_ROW_BRANCH,
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
            .get_tip_ids(id, DEFAULT_ROW_BRANCH)
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
                DEFAULT_ROW_BRANCH,
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
            .get_tip_ids(id, DEFAULT_ROW_BRANCH)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Add commit with row data (no delete metadata = undelete)
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                id,
                DEFAULT_ROW_BRANCH,
                parents,
                new_data.clone(),
                author,
                None,
            )
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
            .get_tip_ids(id, DEFAULT_ROW_BRANCH)
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
                DEFAULT_ROW_BRANCH,
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
            .truncate_branch(id, DEFAULT_ROW_BRANCH, tail_ids);

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

        // Get branches from query (default to "main" for backward compatibility)
        let branches = if query.branches.is_empty() {
            vec!["main".to_string()]
        } else {
            query.branches.clone()
        };

        let mut graph = QueryGraph::compile(&query, &self.schema)
            .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

        // Settle the graph - row_loader reads from the query's branches
        // For multi-branch queries, uses LWW to pick the winning branch
        // Returns None for empty content (hard delete tombstones) so they're not materialized
        // Soft deletes have preserved content and can be materialized normally
        let row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            if branches.len() == 1 {
                self.load_row_from_object_on_branch(id, &branches[0])
            } else {
                self.load_row_from_object_multi_branch(id, &branches)
            }
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
        // Get branches from query (default to "main" for backward compatibility)
        let branches = if query.branches.is_empty() {
            vec!["main".to_string()]
        } else {
            query.branches.clone()
        };

        let graph = QueryGraph::compile_with_session(&query, &self.schema, session)
            .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        self.subscriptions.insert(
            id,
            QuerySubscription {
                graph,
                mode: SubscriptionMode::Delta,
                branches,
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

        // 7. Settle all subscriptions - row_loader reads from subscription's branches
        // Extract references to avoid borrowing self in the closure
        let om = &self.sync_manager.object_manager;
        let indices = &self.indices;

        for (sub_id, subscription) in &mut self.subscriptions {
            let branches = &subscription.branches;

            // Row loader returns None for empty content (hard delete tombstones)
            // Soft deletes have preserved content and can be materialized normally
            // For single-branch subscriptions, reads from that branch
            // For multi-branch subscriptions, uses LWW across branches
            let row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
                let state = om.get_state(id)?;
                match state {
                    ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                        // Find the newest commit across all subscription branches (LWW)
                        let mut best: Option<(u64, Vec<u8>, CommitId)> = None;

                        for branch_name in branches {
                            if let Some(branch) = obj.branches.get(&BranchName::new(branch_name)) {
                                for &tip_id in &branch.tips {
                                    if let Some(commit) = branch.commits.get(&tip_id) {
                                        match &best {
                                            None => {
                                                best = Some((
                                                    commit.timestamp,
                                                    commit.content.clone(),
                                                    tip_id,
                                                ));
                                            }
                                            Some((best_ts, _, _))
                                                if commit.timestamp > *best_ts =>
                                            {
                                                best = Some((
                                                    commit.timestamp,
                                                    commit.content.clone(),
                                                    tip_id,
                                                ));
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }

                        // Filter out empty content (hard delete tombstones only)
                        best.filter(|(_, content, _)| !content.is_empty())
                            .map(|(_, content, commit_id)| (content, commit_id))
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
                        let branch = obj.branches.get(&BranchName::new(DEFAULT_ROW_BRANCH))?;
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
    ///
    /// TODO: Storage requests need branch awareness - currently uses default branch.
    fn process_index_storage(&mut self) {
        use super::index::PageId;
        use crate::storage::StorageRequest;

        // Collect storage requests from all indices
        let mut all_requests = Vec::new();
        for index in self.indices.values_mut() {
            all_requests.extend(index.take_storage_requests());
        }

        // Generate noop responses and route them back to indices
        // TODO: Add branch to StorageRequest for proper branch-aware storage
        for request in all_requests {
            match request {
                StorageRequest::LoadIndexMeta { table, column } => {
                    // New index - return None (index will initialize empty)
                    // Try default branch first
                    let key: IndexKey = (
                        table.clone(),
                        column.clone(),
                        DEFAULT_ROW_BRANCH.to_string(),
                    );
                    if let Some(index) = self.indices.get_mut(&key) {
                        index.process_meta_load(None);
                    }
                }
                StorageRequest::LoadIndexPage {
                    table,
                    column,
                    page_id,
                } => {
                    // Page doesn't exist - return None (index will create new page)
                    let key: IndexKey = (
                        table.clone(),
                        column.clone(),
                        DEFAULT_ROW_BRANCH.to_string(),
                    );
                    if let Some(index) = self.indices.get_mut(&key) {
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

    /// Load a row's data from a specific branch using LWW (last-writer-wins by timestamp).
    /// When multiple concurrent tips exist, returns content from the tip with highest timestamp.
    fn load_row_from_object_on_branch(
        &self,
        row_id: ObjectId,
        branch_name: &str,
    ) -> Option<(Vec<u8>, CommitId)> {
        let state = self.sync_manager.object_manager.get_state(row_id)?;
        match state {
            ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                let branch = obj.branches.get(&BranchName::new(branch_name))?;
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

    /// Load a row's data from ObjectManager using the default branch.
    fn load_row_from_object(&self, row_id: ObjectId) -> Option<(Vec<u8>, CommitId)> {
        self.load_row_from_object_on_branch(row_id, DEFAULT_ROW_BRANCH)
    }

    /// Load a row's data from multiple branches, using LWW (last-writer-wins) to select
    /// the branch with the highest timestamp when the same ObjectId exists on multiple branches.
    ///
    /// Returns the content and commit ID from the branch with the newest commit.
    fn load_row_from_object_multi_branch(
        &self,
        row_id: ObjectId,
        branches: &[String],
    ) -> Option<(Vec<u8>, CommitId)> {
        let state = self.sync_manager.object_manager.get_state(row_id)?;
        let obj = match state {
            ObjectState::Creating(obj) | ObjectState::Available(obj) => obj,
            ObjectState::Loading => return None,
        };

        // Collect the newest tip from each branch
        let mut best: Option<(u64, Vec<u8>, CommitId)> = None; // (timestamp, content, commit_id)

        for branch_name in branches {
            if let Some(branch) = obj.branches.get(&BranchName::new(branch_name)) {
                // Find the tip with the highest timestamp on this branch
                for &tip_id in &branch.tips {
                    if let Some(commit) = branch.commits.get(&tip_id) {
                        match &best {
                            None => {
                                best = Some((commit.timestamp, commit.content.clone(), tip_id));
                            }
                            Some((best_ts, _, _)) if commit.timestamp > *best_ts => {
                                best = Some((commit.timestamp, commit.content.clone(), tip_id));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        best.map(|(_, content, commit_id)| (content, commit_id))
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
            Some(schema) => schema.clone(),
            None => return,
        };
        let descriptor = table_schema.descriptor.clone();
        let branch = update.branch_name.as_str();

        // Ensure indices exist for this branch
        Self::ensure_table_indices_for_branch(&mut self.indices, &table, branch, &table_schema);

        // Check if we have a local hard delete tombstone - if so, ignore incoming updates
        if self.is_hard_deleted(update.object_id) {
            // Hard delete is authoritative - ignore incoming updates
            return;
        }

        // Check if incoming update is a hard delete
        if self.is_incoming_hard_delete(update.object_id) {
            // Apply hard delete unconditionally
            let old_data = update.old_content.as_deref();
            let _ = self.update_indices_for_hard_delete_on_branch(
                &table,
                branch,
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
                let _ = self.update_indices_for_soft_delete_on_branch(
                    &table,
                    branch,
                    update.object_id,
                    old_data,
                    &descriptor,
                );
            } else {
                // No old content - just remove from _id and add to _id_deleted
                if let Some(index) = self.get_index_mut(&table, "_id", branch) {
                    let _ = index.remove(update.object_id.uuid().as_bytes(), update.object_id);
                }
                if let Some(index) = self.get_index_mut(&table, "_id_deleted", branch) {
                    let _ = index.insert(update.object_id.uuid().as_bytes(), update.object_id);
                }
            }
            self.mark_subscriptions_dirty(&table);
            self.mark_row_deleted_in_subscriptions(&table, update.object_id);
            return;
        }

        // Check if this is an undelete (non-empty content for previously soft-deleted row)
        let was_soft_deleted = self.row_is_deleted_on_branch(&table, branch, update.object_id);

        // Extract current (new) data from the object on this branch
        let new_data = match self.load_row_from_object_on_branch(update.object_id, branch) {
            Some((data, _)) => data,
            None => return,
        };

        if was_soft_deleted {
            // This is an undelete - remove from _id_deleted, add to _id and column indices
            let _ = self.update_indices_for_undelete_on_branch(
                &table,
                branch,
                update.object_id,
                &new_data,
                &descriptor,
            );
            self.mark_subscriptions_dirty(&table);
            return;
        }

        // Normal update handling
        if update.is_new_object || update.previous_commit_ids.is_empty() {
            // First commit on branch (new object or synced first commit) - insert into all indices
            let _ = self.update_indices_for_insert_on_branch(
                &table,
                branch,
                update.object_id,
                &new_data,
                &descriptor,
            );
        } else if let Some(old_data) = update.old_content {
            // Synced update - compute index delta using old_content
            // TODO: Future merge strategies - currently last-writer-wins by timestamp
            let _ = self.update_indices_for_update_on_branch(
                &table,
                branch,
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
        let Some(branch) = obj.branches.get(&BranchName::new(DEFAULT_ROW_BRANCH)) else {
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

    /// Update indices when a row is inserted on a specific branch.
    fn update_indices_for_insert_on_branch(
        &mut self,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Update "_id" index
        if let Some(index) = self.get_index_mut(table, "_id", branch) {
            index.insert(object_id.uuid().as_bytes(), object_id)?;
        }

        // Update column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Some(index) = self.get_index_mut(table, col.name.as_str(), branch)
                && let Ok(Some(value_bytes)) =
                    super::encoding::column_bytes(descriptor, data, col_idx)
            {
                index.insert(value_bytes, object_id)?;
            }
        }

        Ok(())
    }

    /// Update indices when a row is inserted (on the default branch).
    fn update_indices_for_insert(
        &mut self,
        table: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        self.update_indices_for_insert_on_branch(
            table,
            DEFAULT_ROW_BRANCH,
            object_id,
            data,
            descriptor,
        )
    }

    /// Update indices when a row is updated on a specific branch.
    fn update_indices_for_update_on_branch(
        &mut self,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // "_id" index doesn't change on update

        // Update column indices (remove old value, add new value)
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Some(index) = self.get_index_mut(table, col.name.as_str(), branch) {
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

    /// Update indices when a row is updated (on the default branch).
    fn update_indices_for_update(
        &mut self,
        table: &str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        self.update_indices_for_update_on_branch(
            table,
            DEFAULT_ROW_BRANCH,
            object_id,
            old_data,
            new_data,
            descriptor,
        )
    }

    /// Update indices for soft delete on a specific branch.
    fn update_indices_for_soft_delete_on_branch(
        &mut self,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id" index
        if let Some(index) = self.get_index_mut(table, "_id", branch) {
            index.remove(object_id.uuid().as_bytes(), object_id)?;
        }

        // Remove from all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Some(index) = self.get_index_mut(table, col.name.as_str(), branch)
                && let Ok(Some(value_bytes)) =
                    super::encoding::column_bytes(descriptor, old_data, col_idx)
            {
                index.remove(value_bytes, object_id)?;
            }
        }

        // Add to "_id_deleted" index
        if let Some(index) = self.get_index_mut(table, "_id_deleted", branch) {
            index.insert(object_id.uuid().as_bytes(), object_id)?;
        }

        Ok(())
    }

    /// Update indices for soft delete (on the default branch).
    fn update_indices_for_soft_delete(
        &mut self,
        table: &str,
        object_id: ObjectId,
        old_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        self.update_indices_for_soft_delete_on_branch(
            table,
            DEFAULT_ROW_BRANCH,
            object_id,
            old_data,
            descriptor,
        )
    }

    /// Update indices for hard delete on a specific branch.
    fn update_indices_for_hard_delete_on_branch(
        &mut self,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: Option<&[u8]>,
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id" index (may not be present if already soft-deleted)
        if let Some(index) = self.get_index_mut(table, "_id", branch) {
            // Ignore errors - row may not be in _id if already soft-deleted
            let _ = index.remove(object_id.uuid().as_bytes(), object_id);
        }

        // Remove from all column indices (if we have old data)
        if let Some(data) = old_data {
            for (col_idx, col) in descriptor.columns.iter().enumerate() {
                if let Some(index) = self.get_index_mut(table, col.name.as_str(), branch)
                    && let Ok(Some(value_bytes)) =
                        super::encoding::column_bytes(descriptor, data, col_idx)
                {
                    // Ignore errors - row may not be in column index if already soft-deleted
                    let _ = index.remove(value_bytes, object_id);
                }
            }
        }

        // Remove from "_id_deleted" index (handles soft→hard upgrade)
        if let Some(index) = self.get_index_mut(table, "_id_deleted", branch) {
            // Ignore errors - row may not be in _id_deleted if it was never soft-deleted
            let _ = index.remove(object_id.uuid().as_bytes(), object_id);
        }

        Ok(())
    }

    /// Update indices for hard delete (on the default branch).
    fn update_indices_for_hard_delete(
        &mut self,
        table: &str,
        object_id: ObjectId,
        old_data: Option<&[u8]>,
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        self.update_indices_for_hard_delete_on_branch(
            table,
            DEFAULT_ROW_BRANCH,
            object_id,
            old_data,
            descriptor,
        )
    }

    /// Update indices for undelete on a specific branch.
    fn update_indices_for_undelete_on_branch(
        &mut self,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id_deleted" index
        if let Some(index) = self.get_index_mut(table, "_id_deleted", branch) {
            index.remove(object_id.uuid().as_bytes(), object_id)?;
        }

        // Add to "_id" index
        if let Some(index) = self.get_index_mut(table, "_id", branch) {
            index.insert(object_id.uuid().as_bytes(), object_id)?;
        }

        // Add to all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Some(index) = self.get_index_mut(table, col.name.as_str(), branch)
                && let Ok(Some(value_bytes)) =
                    super::encoding::column_bytes(descriptor, new_data, col_idx)
            {
                index.insert(value_bytes, object_id)?;
            }
        }

        Ok(())
    }

    /// Update indices for undelete (on the default branch).
    fn update_indices_for_undelete(
        &mut self,
        table: &str,
        object_id: ObjectId,
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        self.update_indices_for_undelete_on_branch(
            table,
            DEFAULT_ROW_BRANCH,
            object_id,
            new_data,
            descriptor,
        )
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
        // TODO: Add branch to StorageResponse for proper branch-aware storage
        for response in responses {
            match &response {
                // Index responses - route to indices (using default branch for now)
                StorageResponse::LoadIndexMeta {
                    table,
                    column,
                    result,
                } => {
                    let key: IndexKey = (
                        table.clone(),
                        column.clone(),
                        DEFAULT_ROW_BRANCH.to_string(),
                    );
                    if let Some(index) = self.indices.get_mut(&key) {
                        index.process_meta_load(result.as_ref().ok().and_then(|o| o.clone()));
                    }
                }
                StorageResponse::LoadIndexPage {
                    table,
                    column,
                    page_id,
                    result,
                } => {
                    let key: IndexKey = (
                        table.clone(),
                        column.clone(),
                        DEFAULT_ROW_BRANCH.to_string(),
                    );
                    if let Some(index) = self.indices.get_mut(&key) {
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
        for ((table, col, branch), index) in &self.indices {
            indices += table.len() + col.len() + branch.len() + 64; // Key size + HashMap entry
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
