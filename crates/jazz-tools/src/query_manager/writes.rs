use std::collections::{HashMap, HashSet};

use crate::commit::CommitId;
use crate::metadata::{MetadataKey, hard_delete_metadata, soft_delete_metadata};
use crate::object::{BranchName, ObjectId};
use crate::storage::Storage;

use super::encoding::{decode_column, decode_row, encode_row};
use super::manager::{
    DeleteHandle, InsertResult, QueryError, QueryManager, SchemaWarningAccumulator,
};
use super::policy::{ComplexClause, Operation, evaluate_simple_parts};
use super::server_queries::{AuthorizationPolicyRequest, RowTransformContext};
use super::session::Session;
use super::types::{ColumnType, LoadedRow, RowDescriptor, Schema, TableName, Value};

pub struct RowBranchWrite<'a> {
    pub table: &'a str,
    pub branch: &'a str,
    pub id: ObjectId,
    pub values: &'a [Value],
    pub old_data_for_policy: &'a [u8],
}

struct PreparedUpdateWrite {
    table_name: TableName,
    descriptor: RowDescriptor,
    new_data: Vec<u8>,
}

pub struct RowBranchDelete<'a> {
    pub table: &'a str,
    pub branch: &'a str,
    pub id: ObjectId,
    pub old_data_for_policy: &'a [u8],
}

impl QueryManager {
    fn prepare_update_write<H: Storage>(
        &mut self,
        storage: &mut H,
        write: RowBranchWrite<'_>,
        session: Option<&Session>,
    ) -> Result<PreparedUpdateWrite, QueryError> {
        let RowBranchWrite {
            table,
            branch,
            id,
            values,
            old_data_for_policy,
        } = write;
        let table_name = TableName::new(table);
        let (descriptor, using_policy, check_policy) = {
            let table_schema = self
                .schema
                .get(&table_name)
                .ok_or(QueryError::TableNotFound(table_name))?;
            (
                table_schema.columns.clone(),
                table_schema.policies.update.using.clone(),
                table_schema.policies.update.with_check.clone(),
            )
        };

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        self.validate_json_for_values(&descriptor, values)?;
        Self::validate_write_index_values_on_branch(table, branch, values, &descriptor)?;

        let new_data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        if let Some(session) = session {
            if let Some((auth_schema, auth_context)) =
                self.local_write_authorization_context(branch, Some(session))
            {
                let Some(auth_table_schema) = auth_schema.get(&table_name) else {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Update,
                    });
                };

                if let Some(policy) = auth_table_schema.policies.update.using.as_ref()
                    && !self.evaluate_current_authorization_policy_for_content(
                        storage,
                        id,
                        branch,
                        table_name,
                        policy,
                        old_data_for_policy,
                        session,
                        Operation::Update,
                        &auth_schema,
                        &auth_context,
                    )
                {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Update,
                    });
                }

                if let Some(policy) = auth_table_schema.policies.update.with_check.as_ref()
                    && !self.evaluate_current_authorization_policy_for_content(
                        storage,
                        id,
                        branch,
                        table_name,
                        policy,
                        &new_data,
                        session,
                        Operation::Update,
                        &auth_schema,
                        &auth_context,
                    )
                {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Update,
                    });
                }
            } else if let Some(policy) = &using_policy {
                let mut visited = HashSet::new();
                if !self.evaluate_policy_for_content_with_context_for_row(
                    storage,
                    policy,
                    old_data_for_policy,
                    &descriptor,
                    session,
                    table,
                    branch,
                    id,
                    0,
                    &mut visited,
                ) {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Update,
                    });
                }
            }

            if self
                .local_write_authorization_context(branch, Some(session))
                .is_none()
                && let Some(policy) = check_policy
            {
                let mut visited = HashSet::new();
                if !self.evaluate_policy_for_content_with_context_for_row(
                    storage,
                    &policy,
                    &new_data,
                    &descriptor,
                    session,
                    table,
                    branch,
                    id,
                    0,
                    &mut visited,
                ) {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Update,
                    });
                }
            }
        }

        Ok(PreparedUpdateWrite {
            table_name,
            descriptor,
            new_data,
        })
    }

    fn commit_prepared_update_write<H: Storage>(
        &mut self,
        storage: &mut H,
        branch: &str,
        id: ObjectId,
        new_data: &[u8],
    ) -> Result<CommitId, QueryError> {
        let parents = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, branch)
            .map(|tips| tips.iter().copied().collect())
            .unwrap_or_default();

        let commit_id = self
            .sync_manager
            .object_manager
            .add_commit(storage, id, branch, parents, new_data.to_vec(), id, None)
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        self.sync_manager
            .forward_update_to_servers(id, branch.into());

        Ok(commit_id)
    }

    /// Load a row for schema-aware updates.
    ///
    /// If the row exists on the current schema branch, use that version.
    /// Otherwise, fall back to the newest visible version across sibling
    /// schema-version branches for the same logical user branch.
    pub fn load_row_for_schema_update<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        branches: &[String],
    ) -> Option<(String, String, Vec<u8>, CommitId)> {
        let branch_schema_map = Self::branch_schema_map_for_context(&self.schema_context);
        let obj = self
            .sync_manager
            .object_manager
            .get_or_load(id, storage, branches)?;
        let table = obj.metadata.get(MetadataKey::Table.as_str())?.clone();
        let mut schema_warnings = SchemaWarningAccumulator::default();
        let mut transform_context = RowTransformContext {
            table: &table,
            branch_schema_map: &branch_schema_map,
            schema_context: &self.schema_context,
            schema_warnings: &mut schema_warnings,
        };
        Self::resolve_latest_row_with_schema_transform(id, obj, branches, &mut transform_context)
            .map(|resolved| {
                (
                    table,
                    resolved.branch_name.as_str().to_string(),
                    resolved.content,
                    resolved.commit_id,
                )
            })
    }

    /// Insert a new row into a table.
    ///
    /// Returns an `InsertResult` that can be polled to check durability.
    /// Index updates happen immediately (creating sentinels if needed).
    pub fn insert<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: &[Value],
    ) -> Result<InsertResult, QueryError> {
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
    ) -> Result<InsertResult, QueryError> {
        let _span = tracing::debug_span!("QM::insert", table).entered();
        let table_name = TableName::new(table);
        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.columns.clone();
        let insert_policy = table_schema.policies.insert.with_check.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        self.validate_json_for_values(&descriptor, values)?;
        Self::validate_write_index_values_on_branch(
            table,
            self.current_branch().as_str(),
            values,
            &descriptor,
        )?;

        // Encode to binary
        let data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;
        let object_id = ObjectId::new();

        // Check INSERT WITH CHECK policy
        if let Some(session) = session {
            if let Some((auth_schema, auth_context)) = self
                .local_write_authorization_context(self.current_branch().as_str(), Some(session))
            {
                let allowed = auth_schema
                    .get(&table_name)
                    .and_then(|table_schema| table_schema.policies.insert.with_check.as_ref())
                    .map(|policy| {
                        self.evaluate_current_authorization_policy_for_content(
                            storage,
                            object_id,
                            self.current_branch().as_str(),
                            table_name,
                            policy,
                            &data,
                            session,
                            Operation::Insert,
                            &auth_schema,
                            &auth_context,
                        )
                    })
                    .unwrap_or_else(|| auth_schema.contains_key(&table_name));
                if !allowed {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Insert,
                    });
                }
            } else if let Some(policy) = insert_policy
                && !self.evaluate_policy_for_content_with_context(
                    storage,
                    &policy,
                    &data,
                    &descriptor,
                    session,
                    table,
                    self.current_branch().as_str(),
                )
            {
                return Err(QueryError::PolicyDenied {
                    table: table_name,
                    operation: Operation::Insert,
                });
            }
        }

        // Create object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert(MetadataKey::Table.to_string(), table.to_string());

        let object_id =
            self.sync_manager
                .object_manager
                .create_with_id(storage, object_id, Some(metadata));
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
        self.mark_subscriptions_dirty_local(table);
        tracing::trace!(table, "mark_subscriptions_dirty");

        tracing::debug!(%object_id, ?row_commit_id, branch = self.current_branch(), "row created");
        Ok(InsertResult {
            row_id: object_id,
            row_commit_id,
            row_values: values.to_vec(),
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
    ) -> Result<InsertResult, QueryError> {
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
    ) -> Result<InsertResult, QueryError> {
        let table_name = TableName::new(table);
        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.columns.clone();
        let insert_policy = table_schema.policies.insert.with_check.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        self.validate_json_for_values(&descriptor, values)?;
        Self::validate_write_index_values_on_branch(table, branch, values, &descriptor)?;

        // Encode to binary
        let data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;
        let object_id = ObjectId::new();

        // Check INSERT WITH CHECK policy
        if let Some(session) = session {
            if let Some((auth_schema, auth_context)) =
                self.local_write_authorization_context(branch, Some(session))
            {
                let allowed = auth_schema
                    .get(&table_name)
                    .and_then(|table_schema| table_schema.policies.insert.with_check.as_ref())
                    .map(|policy| {
                        self.evaluate_current_authorization_policy_for_content(
                            storage,
                            object_id,
                            branch,
                            table_name,
                            policy,
                            &data,
                            session,
                            Operation::Insert,
                            &auth_schema,
                            &auth_context,
                        )
                    })
                    .unwrap_or_else(|| auth_schema.contains_key(&table_name));
                if !allowed {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Insert,
                    });
                }
            } else if let Some(policy) = insert_policy
                && !self.evaluate_policy_for_content_with_context(
                    storage,
                    &policy,
                    &data,
                    &descriptor,
                    session,
                    table,
                    branch,
                )
            {
                return Err(QueryError::PolicyDenied {
                    table: table_name,
                    operation: Operation::Insert,
                });
            }
        }

        // Create object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert(MetadataKey::Table.to_string(), table.to_string());

        let object_id =
            self.sync_manager
                .object_manager
                .create_with_id(storage, object_id, Some(metadata));
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
        self.mark_subscriptions_dirty_local(table);

        Ok(InsertResult {
            row_id: object_id,
            row_commit_id,
            row_values: values.to_vec(),
        })
    }

    fn validate_json_for_values(
        &self,
        descriptor: &RowDescriptor,
        values: &[Value],
    ) -> Result<(), QueryError> {
        for (column, value) in descriptor.columns.iter().zip(values.iter()) {
            Self::validate_json_value_for_type(
                &column.column_type,
                value,
                column.name.as_str().to_string(),
            )?;
        }
        Ok(())
    }

    fn validate_json_value_for_type(
        column_type: &ColumnType,
        value: &Value,
        column_path: String,
    ) -> Result<(), QueryError> {
        match (column_type, value) {
            (_, Value::Null) => Ok(()),
            (ColumnType::Json { schema }, Value::Text(raw)) => {
                let parsed: serde_json::Value = serde_json::from_str(raw).map_err(|err| {
                    QueryError::EncodingError(format!(
                        "invalid JSON for column `{column_path}`: {err}"
                    ))
                })?;

                if let Some(schema) = schema {
                    let validator = jsonschema::validator_for(schema).map_err(|err| {
                        QueryError::EncodingError(format!(
                            "invalid JSON schema for column `{column_path}`: {err}"
                        ))
                    })?;

                    if let Err(err) = validator.validate(&parsed) {
                        return Err(QueryError::EncodingError(format!(
                            "JSON schema validation failed for column `{column_path}`: {err}"
                        )));
                    }
                }

                Ok(())
            }
            (
                ColumnType::Array {
                    element: element_type,
                },
                Value::Array(elements),
            ) => {
                for (idx, element) in elements.iter().enumerate() {
                    Self::validate_json_value_for_type(
                        element_type,
                        element,
                        format!("{column_path}[{idx}]"),
                    )?;
                }
                Ok(())
            }
            (
                ColumnType::Row { columns: desc },
                Value::Row {
                    values: row_values, ..
                },
            ) => {
                for (idx, row_col) in desc.columns.iter().enumerate() {
                    let Some(row_value) = row_values.get(idx) else {
                        break;
                    };
                    Self::validate_json_value_for_type(
                        &row_col.column_type,
                        row_value,
                        format!("{column_path}.{}", row_col.name.as_str()),
                    )?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(super) fn validate_json_for_content(
        &self,
        descriptor: &RowDescriptor,
        content: &[u8],
    ) -> Result<(), QueryError> {
        let values = decode_row(descriptor, content)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;
        self.validate_json_for_values(descriptor, &values)
    }

    fn local_write_authorization_context(
        &self,
        branch: &str,
        session: Option<&Session>,
    ) -> Option<(std::sync::Arc<Schema>, crate::schema_manager::SchemaContext)> {
        self.local_subscription_uses_explicit_authorization(session)
            .then(|| self.authorization_schema_for_branch(&BranchName::new(branch)))
            .flatten()
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_current_authorization_policy_for_content<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        branch: &str,
        table_name: TableName,
        policy: &crate::query_manager::policy::PolicyExpr,
        content: &[u8],
        session: &Session,
        operation: Operation,
        auth_schema: &Schema,
        auth_context: &crate::schema_manager::SchemaContext,
    ) -> bool {
        let source_branch_schema_map = self.branch_schema_map.clone();
        self.evaluate_authorization_policy(
            storage,
            AuthorizationPolicyRequest {
                object_id,
                branch_name: BranchName::new(branch),
                table_name,
                policy,
                content,
                session,
                auth_schema,
                auth_context,
                source_branch_schema_map: &source_branch_schema_map,
                operation,
            },
        )
    }

    /// Evaluate a policy expression against encoded row content using full policy context.
    ///
    /// This uses the same simple/complex split as server-side permission checks:
    /// - Evaluate simple predicates directly from row bytes.
    /// - Materialize and settle policy graphs for complex clauses.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_policy_for_content_with_context<H: Storage>(
        &mut self,
        storage: &mut H,
        policy: &crate::query_manager::policy::PolicyExpr,
        content: &[u8],
        descriptor: &RowDescriptor,
        session: &Session,
        table: &str,
        branch: &str,
    ) -> bool {
        let mut visited = HashSet::new();
        self.evaluate_policy_for_content_with_context_inner(
            storage,
            policy,
            content,
            descriptor,
            session,
            table,
            branch,
            None,
            0,
            &mut visited,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_policy_for_content_with_context_for_row<H: Storage>(
        &mut self,
        storage: &mut H,
        policy: &crate::query_manager::policy::PolicyExpr,
        content: &[u8],
        descriptor: &RowDescriptor,
        session: &Session,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        depth: usize,
        visited: &mut HashSet<(TableName, ObjectId, Operation)>,
    ) -> bool {
        self.evaluate_policy_for_content_with_context_inner(
            storage,
            policy,
            content,
            descriptor,
            session,
            table,
            branch,
            Some(row_id),
            depth,
            visited,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_policy_for_content_with_context_inner<H: Storage>(
        &mut self,
        storage: &mut H,
        policy: &crate::query_manager::policy::PolicyExpr,
        content: &[u8],
        descriptor: &RowDescriptor,
        session: &Session,
        table: &str,
        branch: &str,
        row_id: Option<ObjectId>,
        depth: usize,
        visited: &mut HashSet<(TableName, ObjectId, Operation)>,
    ) -> bool {
        if depth > crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return false;
        }
        let simple_result = evaluate_simple_parts(policy, content, descriptor, session);
        if !simple_result.passed {
            return false;
        }
        if simple_result.complex_clauses.is_empty() {
            return true;
        }

        let table_name = TableName::new(table);
        let mut graph_clauses = Vec::new();
        for clause in simple_result.complex_clauses {
            match clause {
                ComplexClause::InheritsReferencing {
                    operation,
                    source_table,
                    via_column,
                    max_depth,
                } => {
                    let Some(target_row_id) = row_id else {
                        return false;
                    };
                    if !self.evaluate_referencing_inherited_access_recursive(
                        storage,
                        table_name,
                        target_row_id,
                        operation,
                        &source_table,
                        &via_column,
                        max_depth,
                        session,
                        branch,
                        depth,
                        visited,
                    ) {
                        return false;
                    }
                }
                other => graph_clauses.push(other),
            }
        }

        if graph_clauses.is_empty() {
            return true;
        }

        let mut graphs = self.create_policy_graphs_for_complex_clauses(
            &graph_clauses,
            content,
            descriptor,
            &table_name,
            session,
            branch,
        );
        if graphs.is_empty() {
            return true;
        }

        let branches = vec![branch.to_string()];
        let storage_ref: &dyn Storage = storage;
        let om = &mut self.sync_manager.object_manager;
        let branch_name = BranchName::new(branch);
        let mut row_loader = |id: ObjectId| -> Option<LoadedRow> {
            let obj = om.get_or_load(id, storage_ref, &branches)?;
            let branch_state = obj.branches.get(&branch_name)?;
            let tip_id = branch_state.tips.iter().next()?;
            let commit = branch_state.commits.get(tip_id)?;
            if commit.content.is_empty() {
                return None;
            }
            Some(LoadedRow::new(
                commit.content.clone(),
                *tip_id,
                [(id, branch_name)].into_iter().collect(),
            ))
        };

        for graph in &mut graphs {
            for _ in 0..100 {
                if graph.settle(storage_ref, &mut row_loader) {
                    break;
                }
            }
            if !graph.result() {
                return false;
            }
        }

        true
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_referencing_inherited_access_recursive<H: Storage>(
        &mut self,
        storage: &mut H,
        target_table: TableName,
        target_row_id: ObjectId,
        operation: Operation,
        source_table: &str,
        via_column: &str,
        max_depth: Option<usize>,
        session: &Session,
        branch: &str,
        depth: usize,
        visited: &mut HashSet<(TableName, ObjectId, Operation)>,
    ) -> bool {
        if depth > crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return false;
        }
        let Some(effective_max_depth) =
            crate::query_manager::policy::normalize_recursive_max_depth(max_depth)
        else {
            return false;
        };
        if depth >= effective_max_depth {
            return false;
        }

        let source_table_name = TableName::new(source_table);
        let Some(source_schema) = self.schema.get(&source_table_name) else {
            return false;
        };
        let source_descriptor = source_schema.columns.clone();

        let Some(col_idx) = source_descriptor.column_index(via_column) else {
            return false;
        };
        let col = &source_descriptor.columns[col_idx];
        if col.references != Some(target_table) {
            return false;
        }

        match &col.column_type {
            crate::query_manager::types::ColumnType::Uuid => {
                let candidate_ids = storage.index_lookup(
                    source_table_name.as_str(),
                    col.name.as_str(),
                    branch,
                    &Value::Uuid(target_row_id),
                );
                for source_row_id in candidate_ids {
                    if self.evaluate_source_row_access_for_operation(
                        storage,
                        source_table_name,
                        source_row_id,
                        operation,
                        session,
                        branch,
                        depth + 1,
                        visited,
                        None,
                    ) {
                        return true;
                    }
                }
            }
            crate::query_manager::types::ColumnType::Array { element }
                if **element == crate::query_manager::types::ColumnType::Uuid =>
            {
                let candidate_ids =
                    storage.index_scan_all(source_table_name.as_str(), col.name.as_str(), branch);
                for source_row_id in candidate_ids {
                    let Some(source_content) =
                        self.load_row_content_on_branch(storage, source_row_id, branch)
                    else {
                        continue;
                    };

                    if !declared_edge_references_target(
                        &source_descriptor,
                        &source_content,
                        col_idx,
                        target_row_id,
                    ) {
                        continue;
                    }

                    if self.evaluate_source_row_access_for_operation(
                        storage,
                        source_table_name,
                        source_row_id,
                        operation,
                        session,
                        branch,
                        depth + 1,
                        visited,
                        Some(source_content),
                    ) {
                        return true;
                    }
                }
            }
            _ => {}
        }

        false
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_source_row_access_for_operation<H: Storage>(
        &mut self,
        storage: &mut H,
        table_name: TableName,
        row_id: ObjectId,
        operation: Operation,
        session: &Session,
        branch: &str,
        depth: usize,
        visited: &mut HashSet<(TableName, ObjectId, Operation)>,
        preloaded_content: Option<Vec<u8>>,
    ) -> bool {
        if depth > crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP {
            return false;
        }

        let key = (table_name, row_id, operation);
        if !visited.insert(key) {
            // Cycle detected for this recursion branch.
            return false;
        }

        let Some(content) =
            preloaded_content.or_else(|| self.load_row_content_on_branch(storage, row_id, branch))
        else {
            visited.remove(&(table_name, row_id, operation));
            return false;
        };

        let Some(table_schema) = self.schema.get(&table_name).cloned() else {
            visited.remove(&(table_name, row_id, operation));
            return false;
        };

        let local_policy = match operation {
            Operation::Select => table_schema.policies.select.using.clone(),
            Operation::Insert => table_schema.policies.insert.with_check.clone(),
            Operation::Update => table_schema.policies.update.using.clone(),
            Operation::Delete => table_schema.policies.effective_delete_using().cloned(),
        };

        let local_allow = local_policy
            .as_ref()
            .map(|policy| {
                self.evaluate_policy_for_content_with_context_for_row(
                    storage,
                    policy,
                    &content,
                    &table_schema.columns,
                    session,
                    table_name.as_str(),
                    branch,
                    row_id,
                    depth,
                    visited,
                )
            })
            .unwrap_or(true);

        visited.remove(&(table_name, row_id, operation));
        local_allow
    }

    fn load_row_content_on_branch<H: Storage>(
        &mut self,
        storage: &mut H,
        row_id: ObjectId,
        branch: &str,
    ) -> Option<Vec<u8>> {
        let branches = vec![branch.to_string()];
        let obj = self
            .sync_manager
            .object_manager
            .get_or_load(row_id, storage, &branches)?;
        let branch_state = obj.branches.get(&BranchName::new(branch))?;
        let tip_id = branch_state.tips.iter().next()?;
        let commit = branch_state.commits.get(tip_id)?;
        if commit.content.is_empty() {
            return None;
        }
        Some(commit.content.clone())
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
        // Ensure object is loaded from storage (cold-start: may only exist on disk)
        let branch = self.current_branch();
        self.sync_manager
            .object_manager
            .get_or_load(id, storage, &[branch]);

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        // Get old data from ObjectManager
        let (old_data, _commit_id) = self
            .load_row_from_object(id)
            .ok_or(QueryError::ObjectNotFound(id))?;
        let branch = self.current_branch();
        let prepared = self.prepare_update_write(
            storage,
            RowBranchWrite {
                table: &table,
                branch: branch.as_str(),
                id,
                values,
                old_data_for_policy: &old_data,
            },
            session,
        )?;
        let commit_id =
            self.commit_prepared_update_write(storage, branch.as_str(), id, &prepared.new_data)?;

        // Update indices and persist modified nodes
        self.update_indices_for_update(
            storage,
            &prepared.table_name.0,
            id,
            &old_data,
            &prepared.new_data,
            &prepared.descriptor,
        )?;
        tracing::trace!(%id, table = %prepared.table_name.0, "index_update complete");

        // Mark subscriptions dirty and notify about content update
        self.mark_subscriptions_dirty_local(&prepared.table_name.0);
        self.mark_row_updated_in_subscriptions(&prepared.table_name.0, id);
        tracing::trace!(table = %prepared.table_name.0, "mark_subscriptions_dirty");

        Ok(commit_id)
    }

    /// Write new row content for an existing object onto a specific branch.
    ///
    /// Used for schema-aware copy-on-write updates where the row currently
    /// lives on an older schema branch and must be written onto the current
    /// branch without creating a new object id.
    pub fn write_existing_row_on_branch_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        write: RowBranchWrite<'_>,
        session: Option<&Session>,
    ) -> Result<CommitId, QueryError> {
        let RowBranchWrite {
            table,
            branch,
            id,
            values: _values,
            old_data_for_policy: _old_data_for_policy,
        } = write;
        let prepared = self.prepare_update_write(storage, write, session)?;

        let existing_branch_data = self
            .load_row_from_object_on_branch(id, branch)
            .map(|(data, _)| data)
            .filter(|data| !data.is_empty());
        let was_soft_deleted = self.row_is_deleted_on_branch(storage, table, branch, id);
        let commit_id =
            self.commit_prepared_update_write(storage, branch, id, &prepared.new_data)?;

        match existing_branch_data {
            Some(old_data) => Self::update_indices_for_update_on_branch(
                storage,
                table,
                branch,
                id,
                &old_data,
                &prepared.new_data,
                &prepared.descriptor,
            )?,
            None if was_soft_deleted => Self::update_indices_for_undelete_on_branch(
                storage,
                table,
                branch,
                id,
                &prepared.new_data,
                &prepared.descriptor,
            )?,
            None => Self::update_indices_for_insert_on_branch(
                storage,
                table,
                branch,
                id,
                &prepared.new_data,
                &prepared.descriptor,
            )?,
        }

        self.mark_subscriptions_dirty_local(table);
        self.mark_row_updated_in_subscriptions(table, id);

        Ok(commit_id)
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
        // Ensure object is loaded from storage (cold-start: may only exist on disk)
        let branch = self.current_branch();
        self.sync_manager
            .object_manager
            .get_or_load(id, storage, &[branch]);

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
        let (old_data, _commit_id) = self
            .load_row_from_object(id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let (descriptor, using_policy) = {
            let table_schema = self
                .schema
                .get(&table_name)
                .ok_or(QueryError::TableNotFound(table_name))?;
            (
                table_schema.columns.clone(),
                table_schema.policies.effective_delete_using().cloned(),
            )
        };

        let current_branch = self.current_branch().to_string();

        if let Some(session) = session {
            if let Some((auth_schema, auth_context)) =
                self.local_write_authorization_context(&current_branch, Some(session))
            {
                let Some(auth_table_schema) = auth_schema.get(&table_name) else {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Delete,
                    });
                };

                if let Some(policy) = auth_table_schema.policies.effective_delete_using()
                    && !self.evaluate_current_authorization_policy_for_content(
                        storage,
                        id,
                        &current_branch,
                        table_name,
                        policy,
                        &old_data,
                        session,
                        Operation::Delete,
                        &auth_schema,
                        &auth_context,
                    )
                {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Delete,
                    });
                }
            } else if let Some(policy) = using_policy
                && {
                    let mut visited = HashSet::new();
                    !self.evaluate_policy_for_content_with_context_for_row(
                        storage,
                        &policy,
                        &old_data,
                        &descriptor,
                        session,
                        &table,
                        &current_branch,
                        id,
                        0,
                        &mut visited,
                    )
                }
            {
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
                old_data.clone(),
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Forward delete to all connected servers
        let branch = self.current_branch();
        tracing::trace!(%id, ?delete_commit_id, "forward delete to servers");
        self.sync_manager
            .forward_update_to_servers(id, branch.into());

        // Update indices: remove from _id and column indices, add to _id_deleted
        self.update_indices_for_soft_delete(storage, &table, id, &old_data, &descriptor)?;
        tracing::trace!(%id, table = %table, "index_remove complete (soft delete)");

        // Mark subscriptions dirty and mark row as deleted
        self.mark_subscriptions_dirty_local(&table);
        self.mark_row_deleted_in_subscriptions(&table, id);
        tracing::trace!(table = %table, "mark_subscriptions_dirty (delete)");

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    pub fn delete_existing_row_on_branch_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        delete: RowBranchDelete<'_>,
        session: Option<&Session>,
    ) -> Result<DeleteHandle, QueryError> {
        let RowBranchDelete {
            table,
            branch,
            id,
            old_data_for_policy,
        } = delete;
        // Check for hard delete first (checks default branch)
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        let table_name = TableName::new(table);
        // Check if already soft-deleted on this branch
        if self.row_is_deleted_on_branch(storage, table, branch, id) {
            return Err(QueryError::RowAlreadyDeleted(id));
        }

        let (descriptor, using_policy) = {
            let table_schema = self
                .schema
                .get(&table_name)
                .ok_or(QueryError::TableNotFound(table_name))?;
            (
                table_schema.columns.clone(),
                table_schema.policies.effective_delete_using().cloned(),
            )
        };

        if let Some(session) = session {
            if let Some((auth_schema, auth_context)) =
                self.local_write_authorization_context(branch, Some(session))
            {
                let Some(auth_table_schema) = auth_schema.get(&table_name) else {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Delete,
                    });
                };

                if let Some(policy) = auth_table_schema.policies.effective_delete_using()
                    && !self.evaluate_current_authorization_policy_for_content(
                        storage,
                        id,
                        branch,
                        table_name,
                        policy,
                        old_data_for_policy,
                        session,
                        Operation::Delete,
                        &auth_schema,
                        &auth_context,
                    )
                {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Delete,
                    });
                }
            } else if let Some(policy) = using_policy {
                let mut visited = HashSet::new();
                if !self.evaluate_policy_for_content_with_context_for_row(
                    storage,
                    &policy,
                    old_data_for_policy,
                    &descriptor,
                    session,
                    table,
                    branch,
                    id,
                    0,
                    &mut visited,
                ) {
                    return Err(QueryError::PolicyDenied {
                        table: table_name,
                        operation: Operation::Delete,
                    });
                }
            }
        }

        // Get old data from ObjectManager on this branch
        let old_branch_data = self
            .load_row_from_object_on_branch(id, branch)
            .map(|(data, _)| data)
            .filter(|data| !data.is_empty());
        let parents = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, branch)
            .map(|tips| tips.iter().copied().collect())
            .unwrap_or_default();

        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                branch,
                parents,
                old_data_for_policy.to_vec(),
                id,
                Some(soft_delete_metadata()),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        self.sync_manager
            .forward_update_to_servers(id, branch.into());

        Self::update_indices_for_soft_delete_on_branch(
            storage,
            table,
            branch,
            id,
            old_branch_data.as_deref().unwrap_or(old_data_for_policy),
            &descriptor,
        )?;

        self.mark_subscriptions_dirty_local(table);
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
    ) -> Result<InsertResult, QueryError> {
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
        let descriptor = table_schema.columns.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        self.validate_json_for_values(&descriptor, values)?;
        Self::validate_write_index_values_on_branch(
            &table,
            self.current_branch().as_str(),
            values,
            &descriptor,
        )?;

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
        self.mark_subscriptions_dirty_local(&table);

        Ok(InsertResult {
            row_id: id,
            row_commit_id,
            row_values: values.to_vec(),
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
        let descriptor = table_schema.columns.clone();
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
        self.mark_subscriptions_dirty_local(&table);
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
        let values = decode_row(&table_schema.columns, &data).ok()?;
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
        commit.content.is_empty() && commit.is_hard_deleted()
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
        commit.is_soft_deleted()
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
        commit.content.is_empty() && commit.is_hard_deleted()
    }

    /// Check if a commit has been stored to disk.
    ///
    /// With sync storage, commits are stored immediately.
    /// Used by `InsertResult::is_complete()` to check durability.
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

fn declared_edge_references_target(
    descriptor: &RowDescriptor,
    content: &[u8],
    column_index: usize,
    target_row_id: ObjectId,
) -> bool {
    match decode_column(descriptor, content, column_index) {
        Ok(Value::Uuid(id)) => id == target_row_id,
        Ok(Value::Array(values)) => values
            .iter()
            .any(|value| matches!(value, Value::Uuid(id) if *id == target_row_id)),
        _ => false,
    }
}
