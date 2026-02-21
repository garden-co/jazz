use std::collections::HashMap;

use crate::commit::CommitId;
use crate::metadata::{DeleteKind, MetadataKey, hard_delete_metadata, soft_delete_metadata};
use crate::object::{BranchName, ObjectId};
use crate::storage::Storage;

use super::encoding::{decode_row, encode_row};
use super::manager::{DeleteHandle, InsertHandle, QueryError, QueryManager};
use super::policy::{Operation, resolve_session_value};
use super::session::Session;
use super::types::{Row, RowDescriptor, TableName, Value};

impl QueryManager {
    /// Insert a new row into a table.
    ///
    /// Returns an `InsertHandle` that can be polled to check durability.
    /// Index updates happen immediately (creating sentinels if needed).
    pub fn insert<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: &[Value],
    ) -> Result<InsertHandle, QueryError> {
        self.insert_with_session(storage, table, values, None)
    }

    /// Insert a new row with session-based policy checking.
    ///
    /// If the table has an INSERT WITH CHECK policy and a session is provided,
    /// the policy is evaluated against the new row values. If the policy
    /// denies the insert, `PolicyDenied` is returned.
    pub fn insert_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<InsertHandle, QueryError> {
        let _span = tracing::debug_span!("QM::insert", table).entered();
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
        metadata.insert(MetadataKey::Table.to_string(), table.to_string());

        let object_id = self
            .sync_manager
            .object_manager
            .create(storage, Some(metadata));
        let author = object_id; // Self-authored

        // Add commit with row data
        let branch = self.current_branch();
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                object_id,
                &branch,
                vec![],
                data.clone(),
                author,
                None,
            )
            .map_err(|_| QueryError::ObjectNotFound(object_id))?;

        // Forward new row to all connected servers
        tracing::trace!(%object_id, ?row_commit_id, "forward to servers");
        self.sync_manager
            .forward_update_to_servers(object_id, branch.into());

        // Update indices immediately and persist
        self.update_indices_for_insert(storage, table, object_id, &data, &descriptor)?;
        tracing::trace!(%object_id, table, "index_insert complete");

        // Mark subscriptions dirty
        self.mark_subscriptions_dirty(table);
        tracing::trace!(table, "mark_subscriptions_dirty");

        tracing::debug!(%object_id, ?row_commit_id, branch = self.current_branch(), "row created");
        Ok(InsertHandle {
            row_id: object_id,
            row_commit_id,
        })
    }

    /// Insert a new row into a table on a specific branch.
    ///
    /// Used by SchemaManager for schema-aware inserts.
    pub fn insert_on_branch<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        branch: &str,
        values: &[Value],
    ) -> Result<InsertHandle, QueryError> {
        self.insert_on_branch_with_session(storage, table, branch, values, None)
    }

    /// Insert a new row on a specific branch with session-based policy checking.
    pub fn insert_on_branch_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        branch: &str,
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
        metadata.insert(MetadataKey::Table.to_string(), table.to_string());

        let object_id = self
            .sync_manager
            .object_manager
            .create(storage, Some(metadata));
        let author = object_id; // Self-authored

        // Add commit with row data to specified branch
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                object_id,
                branch,
                vec![],
                data.clone(),
                author,
                None,
            )
            .map_err(|_| QueryError::ObjectNotFound(object_id))?;

        // Forward new row to all connected servers
        self.sync_manager
            .forward_update_to_servers(object_id, branch.into());

        // Update indices on specified branch
        Self::update_indices_for_insert_on_branch(
            storage,
            table,
            branch,
            object_id,
            &data,
            &descriptor,
        )?;

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
    pub(super) fn evaluate_policy_for_values(
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

            PolicyExpr::Exists { .. }
            | PolicyExpr::ExistsRel { .. }
            | PolicyExpr::Inherits { .. } => {
                // EXISTS and INHERITS require actual row data - for writes, return true
                // (TODO: implement for write policies that need these)
                true
            }
        }
    }

    /// Compare two Values with the given operator.
    pub(super) fn compare_values(
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
    pub(super) fn value_in_json_array(&self, value: &Value, array: &[serde_json::Value]) -> bool {
        match value {
            Value::Text(s) => array.iter().any(|v| v.as_str() == Some(s.as_str())),
            Value::Integer(i) => array.iter().any(|v| v.as_i64() == Some(*i as i64)),
            Value::BigInt(i) => array.iter().any(|v| v.as_i64() == Some(*i)),
            _ => false,
        }
    }

    /// Update a row.
    pub fn update<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        values: &[Value],
    ) -> Result<CommitId, QueryError> {
        self.update_with_session(storage, id, values, None)
    }

    /// Update a row with session-based policy checking.
    ///
    /// If the table has policies and a session is provided:
    /// - USING policy is checked against the old row (if exists)
    /// - WITH CHECK policy is checked against the new values
    pub fn update_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<CommitId, QueryError> {
        let _span = tracing::debug_span!("QM::update", %id).entered();
        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
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
            .get_tip_ids(id, self.current_branch())
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Add commit with new data
        let commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                self.current_branch(),
                parents,
                new_data.clone(),
                author,
                None,
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Forward update to all connected servers
        let branch = self.current_branch();
        tracing::trace!(%id, ?commit_id, "forward update to servers");
        self.sync_manager
            .forward_update_to_servers(id, branch.into());

        // Update indices and persist modified nodes
        self.update_indices_for_update(
            storage,
            &table_name.0,
            id,
            &old_data,
            &new_data,
            &descriptor,
        )?;
        tracing::trace!(%id, table = %table_name.0, "index_update complete");

        // Mark subscriptions dirty and notify about content update
        self.mark_subscriptions_dirty(&table_name.0);
        self.mark_row_updated_in_subscriptions(&table_name.0, id);
        tracing::trace!(table = %table_name.0, "mark_subscriptions_dirty");

        Ok(commit_id)
    }

    /// Evaluate a policy expression against an encoded row.
    pub(super) fn evaluate_policy_for_row(
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
    pub fn delete<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
    ) -> Result<DeleteHandle, QueryError> {
        self.delete_with_session(storage, id, None)
    }

    /// Soft delete a row with session-based policy checking.
    ///
    /// Checks DELETE USING policy against the existing row before allowing deletion.
    /// Falls back to UPDATE's USING policy if no DELETE policy is defined.
    pub fn delete_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        session: Option<&Session>,
    ) -> Result<DeleteHandle, QueryError> {
        let _span = tracing::debug_span!("QM::delete", %id).entered();
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Check if already soft-deleted
        if self.row_is_deleted(storage, &table, id) {
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
            .get_tip_ids(id, self.current_branch())
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create delete metadata
        let delete_metadata = soft_delete_metadata();

        // Add commit with preserved content + delete: soft metadata
        // Content is copied from previous tip so soft-deleted rows can still be read
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                self.current_branch(),
                parents,
                old_data.clone(), // Preserve content for soft deletes
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Forward delete to all connected servers
        tracing::trace!(%id, ?delete_commit_id, "forward delete to servers");
        {
            let branch = self.current_branch();
            self.sync_manager
                .forward_update_to_servers(id, branch.into());
        }

        // Update indices: remove from _id and column indices, add to _id_deleted
        self.update_indices_for_soft_delete(storage, &table, id, &old_data, &descriptor)?;
        tracing::trace!(%id, table = %table, "index_remove complete (soft delete)");

        // Mark subscriptions dirty and mark row as deleted
        self.mark_subscriptions_dirty(&table);
        self.mark_row_deleted_in_subscriptions(&table, id);
        tracing::trace!(table = %table, "mark_subscriptions_dirty (delete)");

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    /// Soft delete a row on a specific branch.
    ///
    /// Used by SchemaManager for schema-aware deletes.
    pub fn delete_on_branch<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        branch: &str,
        id: ObjectId,
    ) -> Result<DeleteHandle, QueryError> {
        // Check for hard delete first (checks default branch)
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        let table_name = TableName::new(table);

        // Check if already soft-deleted on this branch
        if self.row_is_deleted_on_branch(storage, table, branch, id) {
            return Err(QueryError::RowAlreadyDeleted(id));
        }

        // Get old data from ObjectManager on this branch
        let (old_data, _) = self
            .load_row_from_object_on_branch(id, branch)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        // Get parent commit on this branch
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, branch)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create delete metadata
        let delete_metadata = soft_delete_metadata();

        // Add commit with preserved content + delete: soft metadata
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                branch,
                parents,
                old_data.clone(),
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices on this branch
        Self::update_indices_for_soft_delete_on_branch(
            storage,
            table,
            branch,
            id,
            &old_data,
            &descriptor,
        )?;

        // Mark subscriptions dirty
        self.mark_subscriptions_dirty(table);
        self.mark_row_deleted_in_subscriptions(table, id);

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    /// Undelete a soft-deleted row.
    ///
    /// Restores a row from the `_id_deleted` index back to the `_id` and column indices.
    /// Creates a new commit with the provided values (no `delete` metadata).
    pub fn undelete<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        values: &[Value],
    ) -> Result<InsertHandle, QueryError> {
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Verify row is in _id_deleted index (soft-deleted)
        if !self.row_is_deleted(storage, &table, id) {
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
            .get_tip_ids(id, self.current_branch())
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Add commit with row data (no delete metadata = undelete)
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                self.current_branch(),
                parents,
                new_data.clone(),
                author,
                None,
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices: remove from _id_deleted, add to _id and column indices
        self.update_indices_for_undelete(storage, &table, id, &new_data, &descriptor)?;

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
    pub fn hard_delete<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
    ) -> Result<DeleteHandle, QueryError> {
        // Check if already hard-deleted
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
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
            .get_tip_ids(id, self.current_branch())
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create hard delete metadata
        let delete_metadata = hard_delete_metadata();

        // Add commit with empty content + delete: hard metadata
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                self.current_branch(),
                parents,
                vec![], // Empty content for tombstone
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices: remove from ALL indices including _id_deleted
        self.update_indices_for_hard_delete(storage, &table, id, old_data.as_deref(), &descriptor)?;

        // Truncate branch: set tails = [delete_commit_id], removing all history
        // (In ObjectManager, this would be done via set_tails or similar)
        // For now, we just record the hard delete tombstone
        let mut tail_ids = std::collections::HashSet::new();
        tail_ids.insert(delete_commit_id);
        let _ = self.sync_manager.object_manager.truncate_branch(
            storage,
            id,
            self.current_branch(),
            tail_ids,
        );

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
    pub fn truncate<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
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
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        // Verify row is in _id_deleted index (soft-deleted)
        if !self.row_is_deleted(storage, &table, id) {
            return Err(QueryError::RowNotDeleted(id));
        }

        // Upgrade to hard delete
        self.hard_delete(storage, id)
    }

    /// Get a row by ID if loaded in ObjectManager.
    ///
    /// Returns decoded values and the table name if the row exists.
    pub fn get_row(&self, id: ObjectId) -> Option<(String, Vec<Value>)> {
        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)?
            .metadata
            .get(MetadataKey::Table.as_str())?
            .clone();
        let table_name = TableName::new(&table);

        // Get row data from ObjectManager
        let (data, _) = self.load_row_from_object(id)?;

        let table_schema = self.schema.get(&table_name)?;
        let values = decode_row(&table_schema.descriptor, &data).ok()?;
        Some((table, values))
    }

    /// Check if a row is indexed on a specific branch (appears in the _id index).
    pub fn row_is_indexed_on_branch(
        &self,
        storage: &dyn Storage,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> bool {
        let ids = storage.index_lookup(table, "_id", branch, &Value::Uuid(row_id));
        ids.contains(&row_id)
    }

    /// Check if a row is indexed on the default branch (appears in the _id index).
    pub fn row_is_indexed(&self, storage: &dyn Storage, table: &str, row_id: ObjectId) -> bool {
        self.row_is_indexed_on_branch(storage, table, &self.current_branch(), row_id)
    }

    /// Check if a row is soft-deleted on a specific branch.
    pub fn row_is_deleted_on_branch(
        &self,
        storage: &dyn Storage,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> bool {
        let ids = storage.index_lookup(table, "_id_deleted", branch, &Value::Uuid(row_id));
        ids.contains(&row_id)
    }

    /// Check if a row is soft-deleted (appears in _id_deleted but not _id).
    pub fn row_is_deleted(&self, storage: &dyn Storage, table: &str, row_id: ObjectId) -> bool {
        self.row_is_deleted_on_branch(storage, table, &self.current_branch(), row_id)
    }

    /// Check if a row has a hard delete tombstone (empty content + delete: hard metadata).
    pub(super) fn is_hard_deleted(&self, id: ObjectId) -> bool {
        let Some(obj) = self.sync_manager.object_manager.get(id) else {
            return false;
        };
        let Some(branch) = obj.branches.get(&BranchName::new(self.current_branch())) else {
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
                .and_then(|m| m.get(MetadataKey::Delete.as_str()))
                .map(|v| v == DeleteKind::Hard.as_str())
                .unwrap_or(false)
    }

    /// Check if the current tip has `delete: soft` metadata.
    pub(super) fn is_soft_delete_commit(&self, id: ObjectId) -> bool {
        let Some(obj) = self.sync_manager.object_manager.get(id) else {
            return false;
        };
        let Some(branch) = obj.branches.get(&BranchName::new(self.current_branch())) else {
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
            .and_then(|m| m.get(MetadataKey::Delete.as_str()))
            .map(|v| v == DeleteKind::Soft.as_str())
            .unwrap_or(false)
    }

    /// Check if an incoming update has hard delete metadata.
    pub(super) fn is_incoming_hard_delete(&self, id: ObjectId) -> bool {
        let Some(obj) = self.sync_manager.object_manager.get(id) else {
            return false;
        };
        let Some(branch) = obj.branches.get(&BranchName::new(self.current_branch())) else {
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
                .and_then(|m| m.get(MetadataKey::Delete.as_str()))
                .map(|v| v == DeleteKind::Hard.as_str())
                .unwrap_or(false)
    }

    /// Check if a commit has been stored to disk.
    ///
    /// With sync storage, commits are stored immediately.
    /// Used by `InsertHandle::is_complete()` to check durability.
    pub fn is_commit_stored(&self, object_id: ObjectId, commit_id: &CommitId) -> bool {
        if let Some(obj) = self.sync_manager.object_manager.get(object_id) {
            // Check all branches for the commit
            for branch in obj.branches.values() {
                if let Some(commit) = branch.commits.get(commit_id) {
                    return matches!(commit.stored_state, crate::commit::StoredState::Stored);
                }
            }
        }
        false
    }
}
