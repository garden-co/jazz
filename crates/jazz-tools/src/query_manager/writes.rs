use std::collections::HashSet;

use crate::commit::CommitId;
use crate::metadata::{DeleteKind, RowProvenance, SYSTEM_PRINCIPAL_ID, row_provenance_metadata};
use crate::object::{BranchName, ObjectId};
use crate::row_histories::{
    QueryRowVersion, RowHistoryError, RowState, RowVisibilityChange, StoredRowVersion,
    apply_row_version,
};
use crate::schema_manager::resolve_current_table_name;
use crate::storage::{RowLocator, Storage, metadata_from_row_locator};

use super::encoding::{decode_column, decode_row, encode_row};
use super::manager::{
    DeleteHandle, InsertResult, QueryError, QueryManager, SchemaWarningAccumulator,
};
use super::policy::{ComplexClause, Operation, evaluate_simple_parts};
use super::server_queries::{AuthorizationPolicyRequest, RowTransformContext};
use super::session::{Session, WriteContext};
use super::types::{
    ColumnType, ComposedBranchName, LoadedRow, RowDescriptor, Schema, SchemaHash, TableName, Value,
};

pub struct RowBranchWrite<'a> {
    pub table: &'a str,
    pub branch: &'a str,
    pub id: ObjectId,
    pub values: &'a [Value],
    pub old_data_for_policy: &'a [u8],
    pub old_provenance_for_policy: &'a RowProvenance,
}

struct PreparedUpdateWrite {
    new_data: Vec<u8>,
    descriptor: RowDescriptor,
}

struct PreparedUpdateCommit<'a> {
    table: &'a str,
    branch: &'a str,
    id: ObjectId,
    index_mutations: &'a [crate::storage::IndexMutation<'a>],
}

pub struct RowBranchDelete<'a> {
    pub table: &'a str,
    pub branch: &'a str,
    pub id: ObjectId,
    pub old_data_for_policy: &'a [u8],
    pub old_provenance_for_policy: &'a RowProvenance,
}

impl QueryManager {
    fn resolve_write_author(write_context: Option<&WriteContext>) -> String {
        write_context
            .map(|write_context| write_context.author_principal().to_string())
            .unwrap_or_else(|| SYSTEM_PRINCIPAL_ID.to_string())
    }

    fn reserve_write_timestamp(&mut self) -> u64 {
        self.sync_manager.reserve_timestamp()
    }

    fn row_provenance_for_insert(
        &self,
        write_context: Option<&WriteContext>,
        timestamp: u64,
    ) -> RowProvenance {
        RowProvenance::for_insert(Self::resolve_write_author(write_context), timestamp)
    }

    fn row_provenance_for_update(
        &self,
        existing: &RowProvenance,
        write_context: Option<&WriteContext>,
        timestamp: u64,
    ) -> RowProvenance {
        RowProvenance::for_update(
            existing,
            Self::resolve_write_author(write_context),
            timestamp,
        )
    }

    fn row_commit_metadata(
        provenance: &RowProvenance,
        delete_kind: Option<DeleteKind>,
    ) -> std::collections::BTreeMap<String, String> {
        row_provenance_metadata(provenance, delete_kind)
    }

    fn authored_row_version(
        &self,
        row_id: ObjectId,
        branch_name: &str,
        parents: impl IntoIterator<Item = CommitId>,
        data: Vec<u8>,
        provenance: &RowProvenance,
        delete_kind: Option<DeleteKind>,
    ) -> StoredRowVersion {
        StoredRowVersion::new(
            row_id,
            branch_name,
            parents,
            data,
            provenance.clone(),
            Self::row_commit_metadata(provenance, delete_kind)
                .into_iter()
                .collect(),
            RowState::VisibleDirect,
            self.sync_manager.max_local_durability_tier(),
        )
    }

    #[cfg(test)]
    fn stored_row_version_for_tip(
        &self,
        storage: &dyn Storage,
        row_id: ObjectId,
        branch_name: &str,
    ) -> Option<StoredRowVersion> {
        let table = self.load_row_table_name(storage, row_id)?;
        storage
            .load_visible_region_row(&table, branch_name, row_id)
            .ok()
            .flatten()
    }

    #[cfg(test)]
    pub(super) fn persist_row_region_tip<H: Storage>(
        &self,
        storage: &mut H,
        table: &str,
        row_id: ObjectId,
        branch_name: &str,
    ) -> Option<StoredRowVersion> {
        let version = self.stored_row_version_for_tip(storage, row_id, branch_name)?;
        let visible_entry = storage
            .load_visible_region_entry(table, branch_name, row_id)
            .ok()
            .flatten()?;

        if let Err(error) =
            storage.append_history_region_rows(table, std::slice::from_ref(&version))
        {
            tracing::warn!(
                table,
                branch = branch_name,
                row_id = %row_id,
                %error,
                "failed to append row-history version"
            );
        }

        if let Err(error) =
            storage.upsert_visible_region_rows(table, std::slice::from_ref(&visible_entry))
        {
            tracing::warn!(
                table,
                branch = branch_name,
                row_id = %row_id,
                %error,
                "failed to upsert visible row entry"
            );
        }

        Some(version)
    }

    fn apply_local_row_version<H: Storage>(
        &mut self,
        storage: &mut H,
        row_id: ObjectId,
        update: RowVisibilityChange,
    ) -> Result<StoredRowVersion, QueryError> {
        let row = update.row.clone();
        self.handle_row_update_with_origin(storage, update, true, false);
        if let Ok(Some(row_locator)) = storage.load_row_locator(row_id) {
            self.sync_manager.forward_row_version_to_servers(
                row_id,
                metadata_from_row_locator(&row_locator),
                row.clone(),
            );
        }
        Ok(row)
    }

    fn persist_row_locator<H: Storage>(
        &mut self,
        storage: &mut H,
        row_id: ObjectId,
        row_locator: &RowLocator,
    ) {
        let _ = storage.put_row_locator(row_id, Some(row_locator));
    }

    fn apply_local_row_history_write<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        branch_name: &BranchName,
        row_id: ObjectId,
        row: StoredRowVersion,
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(CommitId, RowVisibilityChange), QueryError> {
        self.ensure_known_schemas_catalogued(storage)
            .map_err(|err| QueryError::EncodingError(format!("persist known schemas: {err}")))?;

        if storage
            .load_row_locator(row_id)
            .map_err(|err| QueryError::EncodingError(format!("load row locator: {err}")))?
            .is_none()
        {
            let row_locator = self.row_locator_for_branch(table, branch_name.as_str());
            self.persist_row_locator(storage, row_id, &row_locator);
        }

        let applied = apply_row_version(storage, row_id, branch_name, row, index_mutations)
            .map_err(|error| match error {
                RowHistoryError::ObjectNotFound(id) => QueryError::ObjectNotFound(id),
                RowHistoryError::ParentNotFound(parent) => QueryError::EncodingError(format!(
                    "missing row-history parent {parent:?} while applying local write for {row_id:?}"
                )),
                RowHistoryError::StorageError(error) => {
                    QueryError::EncodingError(format!("apply row version: {error}"))
                }
            })?;

        let version_id = applied.version_id;
        let visibility_change = applied.visibility_change.ok_or_else(|| {
            QueryError::EncodingError(format!(
                "missing visible-row update for local row version {:?} on {}",
                version_id, row_id
            ))
        })?;

        Ok((version_id, visibility_change))
    }

    fn origin_schema_hash_for_branch(&self, branch: &str) -> Option<SchemaHash> {
        if branch == self.current_branch() {
            return Some(self.schema_context.current_hash);
        }

        let composed = ComposedBranchName::parse(&BranchName::new(branch))?;
        if composed.schema_hash.short() == self.schema_context.current_hash.short() {
            return Some(self.schema_context.current_hash);
        }

        if let Some(hash) = self
            .schema_context
            .live_schemas
            .keys()
            .copied()
            .find(|hash| hash.short() == composed.schema_hash.short())
        {
            return Some(hash);
        }

        self.find_schema_by_short_hash(&composed.schema_hash)
    }

    fn row_locator_for_branch(&self, table: &str, branch: &str) -> RowLocator {
        RowLocator {
            table: table.to_string().into(),
            origin_schema_hash: self.origin_schema_hash_for_branch(branch),
        }
    }

    fn load_row_table_name(&self, storage: &dyn Storage, row_id: ObjectId) -> Option<String> {
        let locator = storage.load_row_locator(row_id).ok().flatten()?;
        let table = locator.table.as_str();
        resolve_current_table_name(
            &self.schema_context,
            table,
            locator.origin_schema_hash.as_ref(),
        )
        .or(Some(locator.table.to_string()))
    }

    fn load_visible_row_on_branch(
        &self,
        storage: &dyn Storage,
        row_id: ObjectId,
        branch_name: &str,
    ) -> Option<(String, QueryRowVersion)> {
        let branch_schema_map = Self::branch_schema_map_for_context(&self.schema_context);
        self.load_best_visible_row_version(
            storage,
            row_id,
            &[branch_name.to_string()],
            None,
            &self.schema_context,
            &branch_schema_map,
        )
    }

    fn load_row_provenance_on_branch(
        &self,
        storage: &dyn Storage,
        row_id: ObjectId,
        branch_name: &str,
    ) -> Option<RowProvenance> {
        let (_, row) = self.load_visible_row_on_branch(storage, row_id, branch_name)?;
        Some(row.row_provenance())
    }

    fn load_branch_tip_ids(
        &self,
        storage: &dyn Storage,
        table: &str,
        row_id: ObjectId,
        branch: &str,
    ) -> Vec<CommitId> {
        if let Ok(Some(entry)) = storage.load_visible_region_entry(table, branch, row_id) {
            return entry.branch_frontier;
        }

        storage
            .scan_row_branch_tip_ids(table, branch, row_id)
            .unwrap_or_default()
    }

    fn prepare_update_write<H: Storage>(
        &mut self,
        storage: &mut H,
        write: RowBranchWrite<'_>,
        write_context: Option<&WriteContext>,
        new_provenance: &RowProvenance,
    ) -> Result<PreparedUpdateWrite, QueryError> {
        let RowBranchWrite {
            table,
            branch,
            id,
            values,
            old_data_for_policy,
            old_provenance_for_policy,
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

        if let Some(session) = write_context.and_then(WriteContext::session) {
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
                        old_provenance_for_policy,
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
                        new_provenance,
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
                    old_provenance_for_policy,
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
                    new_provenance,
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

        let _ = table_name;
        let _ = descriptor;
        Ok(PreparedUpdateWrite {
            new_data,
            descriptor: descriptor.clone(),
        })
    }

    fn commit_prepared_update_write<H: Storage>(
        &mut self,
        storage: &mut H,
        commit: PreparedUpdateCommit<'_>,
        prepared: &PreparedUpdateWrite,
        provenance: &RowProvenance,
    ) -> Result<CommitId, QueryError> {
        let PreparedUpdateCommit {
            table,
            branch,
            id,
            index_mutations,
        } = commit;
        let parents = self.load_branch_tip_ids(storage, table, id, branch);

        let row = self.authored_row_version(
            id,
            branch,
            parents,
            prepared.new_data.clone(),
            provenance,
            None,
        );
        let branch_name = BranchName::new(branch);
        let (version_id, visibility_change) = self.apply_local_row_history_write(
            storage,
            table,
            &branch_name,
            id,
            row,
            index_mutations,
        )?;

        let _ = self.apply_local_row_version(storage, id, visibility_change)?;

        Ok(version_id)
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
    ) -> Option<(String, String, Vec<u8>, CommitId, RowProvenance)> {
        let branch_schema_map = Self::branch_schema_map_for_context(&self.schema_context);
        let (table, row) = self.load_best_visible_row_version(
            storage,
            id,
            branches,
            None,
            &self.schema_context,
            &branch_schema_map,
        )?;
        let mut schema_warnings = SchemaWarningAccumulator::default();
        let mut transform_context = RowTransformContext {
            table: &table,
            branch_schema_map: &branch_schema_map,
            schema_context: &self.schema_context,
            schema_warnings: &mut schema_warnings,
        };
        if row.data.is_empty() {
            return None;
        }

        Self::transform_row_with_schema(
            id,
            row.data.to_vec(),
            row.version_id(),
            BranchName::new(&row.branch),
            &mut transform_context,
        )
        .map(|resolved| {
            (
                table,
                resolved.branch_name.as_str().to_string(),
                resolved.content,
                resolved.version_id,
                row.row_provenance(),
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
        self.insert_with_write_context(storage, table, values, None)
    }

    /// Insert a new row with session-based policy checking.
    ///
    /// If the table has an INSERT WITH CHECK policy and a session is provided,
    /// the policy is evaluated against the new row values. If the policy
    /// denies the insert, `PolicyDenied` is returned.
    pub fn insert_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: &[Value],
        write_context: Option<&WriteContext>,
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
        let timestamp = self.reserve_write_timestamp();
        let provenance = self.row_provenance_for_insert(write_context, timestamp);

        // Check INSERT WITH CHECK policy
        if let Some(session) = write_context.and_then(WriteContext::session) {
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
                            &provenance,
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
                    &provenance,
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

        // Create row locator for the new row object
        let row_locator = self.row_locator_for_branch(table, self.current_branch().as_str());

        self.persist_row_locator(storage, object_id, &row_locator);

        // Add commit with row data
        let branch = self.current_branch();
        let index_mutations = Self::index_mutations_for_insert_on_branch(
            table,
            branch.as_str(),
            object_id,
            &data,
            &descriptor,
        );
        let row = self.authored_row_version(
            object_id,
            branch.as_str(),
            vec![],
            data.clone(),
            &provenance,
            None,
        );
        let branch_name = BranchName::new(branch.as_str());
        let (row_version_id, visibility_change) = self.apply_local_row_history_write(
            storage,
            table,
            &branch_name,
            object_id,
            row,
            &index_mutations,
        )?;

        tracing::trace!(%object_id, ?row_version_id, "apply local row insert");
        let _ = self.apply_local_row_version(storage, object_id, visibility_change)?;

        tracing::debug!(%object_id, ?row_version_id, branch = self.current_branch(), "row created");
        Ok(InsertResult {
            row_id: object_id,
            row_version_id,
            row_values: values.to_vec(),
        })
    }

    pub fn insert_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<InsertResult, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.insert_with_write_context(storage, table, values, owned.as_ref())
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
        self.insert_on_branch_with_write_context(storage, table, branch, values, None)
    }

    /// Insert a new row on a specific branch with session-based policy checking.
    pub fn insert_on_branch_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        branch: &str,
        values: &[Value],
        write_context: Option<&WriteContext>,
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
        let timestamp = self.reserve_write_timestamp();
        let provenance = self.row_provenance_for_insert(write_context, timestamp);

        // Check INSERT WITH CHECK policy
        if let Some(session) = write_context.and_then(WriteContext::session) {
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
                            &provenance,
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
                    &provenance,
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

        // Create row locator for the new row object
        let row_locator = self.row_locator_for_branch(table, branch);

        self.persist_row_locator(storage, object_id, &row_locator);

        // Add commit with row data to specified branch
        let index_mutations = Self::index_mutations_for_insert_on_branch(
            table,
            branch,
            object_id,
            &data,
            &descriptor,
        );
        let row =
            self.authored_row_version(object_id, branch, vec![], data.clone(), &provenance, None);
        let branch_name = BranchName::new(branch);
        let (row_version_id, visibility_change) = self.apply_local_row_history_write(
            storage,
            table,
            &branch_name,
            object_id,
            row,
            &index_mutations,
        )?;

        let _ = self.apply_local_row_version(storage, object_id, visibility_change)?;

        Ok(InsertResult {
            row_id: object_id,
            row_version_id,
            row_values: values.to_vec(),
        })
    }

    pub fn insert_on_branch_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        branch: &str,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<InsertResult, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.insert_on_branch_with_write_context(storage, table, branch, values, owned.as_ref())
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
        provenance: &RowProvenance,
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
                provenance,
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
        provenance: &RowProvenance,
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
            provenance,
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
        provenance: &RowProvenance,
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
            provenance,
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
        provenance: &RowProvenance,
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
        let simple_result = evaluate_simple_parts(policy, content, provenance, descriptor, session);
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

        let storage_ref: &dyn Storage = storage;
        let branch_schema_map = Self::branch_schema_map_for_context(&self.schema_context);
        let mut row_loader = |id: ObjectId, table_hint: Option<TableName>| -> Option<LoadedRow> {
            let (_, row) = Self::load_best_visible_row_version_with_hint_or_locator(
                storage_ref,
                id,
                table_hint.as_ref().map(TableName::as_str),
                &[branch.to_string()],
                None,
                &self.schema_context,
                &branch_schema_map,
            )?;
            if row.is_hard_deleted() {
                return None;
            }
            let version_id = row.version_id();
            let provenance = row.row_provenance();
            let source_branch = BranchName::new(&row.branch);
            Some(LoadedRow::new(
                row.data,
                version_id,
                provenance,
                [(id, source_branch)].into_iter().collect(),
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
        let Some(provenance) = self.load_row_provenance_on_branch(storage, row_id, branch) else {
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
                    &provenance,
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
        let (_, row) = self.load_visible_row_on_branch(storage, row_id, branch)?;
        if row.is_hard_deleted() {
            return None;
        }
        Some(row.data.to_vec())
    }

    pub(super) fn visible_row_is_hard_deleted(
        &self,
        storage: &dyn Storage,
        row_id: ObjectId,
        branch: &str,
    ) -> bool {
        self.load_visible_row_on_branch(storage, row_id, branch)
            .map(|(_, row)| row.is_hard_deleted())
            .unwrap_or(false)
    }

    /// Update a row.
    pub fn update<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        values: &[Value],
    ) -> Result<CommitId, QueryError> {
        self.update_with_write_context(storage, id, values, None)
    }

    /// Update a row with session-based policy checking.
    ///
    /// If the table has policies and a session is provided:
    /// - USING policy is checked against the old row (if exists)
    /// - WITH CHECK policy is checked against the new values
    pub fn update_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        values: &[Value],
        write_context: Option<&WriteContext>,
    ) -> Result<CommitId, QueryError> {
        let _span = tracing::debug_span!("QM::update", %id).entered();
        let table = self
            .load_row_table_name(storage, id)
            .ok_or(QueryError::ObjectNotFound(id))?;
        let (_, current_row) = self
            .load_visible_row_on_branch(storage, id, self.current_branch().as_str())
            .ok_or(QueryError::ObjectNotFound(id))?;
        let old_data = current_row.data.clone();
        let old_provenance = current_row.row_provenance();
        let branch = self.current_branch();
        let timestamp = self.reserve_write_timestamp();
        let new_provenance =
            self.row_provenance_for_update(&old_provenance, write_context, timestamp);
        let prepared = self.prepare_update_write(
            storage,
            RowBranchWrite {
                table: &table,
                branch: branch.as_str(),
                id,
                values,
                old_data_for_policy: &old_data,
                old_provenance_for_policy: &old_provenance,
            },
            write_context,
            &new_provenance,
        )?;
        let index_mutations = Self::index_mutations_for_update_on_branch(
            &table,
            branch.as_str(),
            id,
            &old_data,
            &prepared.new_data,
            &prepared.descriptor,
        );
        let version_id = self.commit_prepared_update_write(
            storage,
            PreparedUpdateCommit {
                table: &table,
                branch: branch.as_str(),
                id,
                index_mutations: &index_mutations,
            },
            &prepared,
            &new_provenance,
        )?;

        Ok(version_id)
    }

    pub fn update_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<CommitId, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.update_with_write_context(storage, id, values, owned.as_ref())
    }

    /// Write new row content for an existing object onto a specific branch.
    ///
    /// Used for schema-aware copy-on-write updates where the row currently
    /// lives on an older schema branch and must be written onto the current
    /// branch without creating a new object id.
    pub fn write_existing_row_on_branch_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        write: RowBranchWrite<'_>,
        write_context: Option<&WriteContext>,
    ) -> Result<CommitId, QueryError> {
        let RowBranchWrite {
            table,
            branch,
            id,
            values: _values,
            old_data_for_policy: _old_data_for_policy,
            old_provenance_for_policy,
        } = write;
        let timestamp = self.reserve_write_timestamp();
        let new_provenance =
            self.row_provenance_for_update(old_provenance_for_policy, write_context, timestamp);
        let prepared = self.prepare_update_write(storage, write, write_context, &new_provenance)?;

        let existing_branch_data = self
            .load_visible_row_on_branch(storage, id, branch)
            .map(|(_, row)| row.data)
            .filter(|data| !data.is_empty());
        let was_soft_deleted = self.row_is_deleted_on_branch(storage, table, branch, id);
        let index_mutations = if was_soft_deleted {
            Self::index_mutations_for_undelete_on_branch(
                table,
                branch,
                id,
                &prepared.new_data,
                &prepared.descriptor,
            )
        } else if let Some(old_branch_data) = existing_branch_data.as_deref() {
            Self::index_mutations_for_update_on_branch(
                table,
                branch,
                id,
                old_branch_data,
                &prepared.new_data,
                &prepared.descriptor,
            )
        } else {
            Self::index_mutations_for_insert_on_branch(
                table,
                branch,
                id,
                &prepared.new_data,
                &prepared.descriptor,
            )
        };
        let version_id = self.commit_prepared_update_write(
            storage,
            PreparedUpdateCommit {
                table,
                branch,
                id,
                index_mutations: &index_mutations,
            },
            &prepared,
            &new_provenance,
        )?;

        let _ = existing_branch_data;
        let _ = was_soft_deleted;

        Ok(version_id)
    }

    pub fn write_existing_row_on_branch_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        write: RowBranchWrite<'_>,
        session: Option<&Session>,
    ) -> Result<CommitId, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.write_existing_row_on_branch_with_write_context(storage, write, owned.as_ref())
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
        self.delete_with_write_context(storage, id, None)
    }

    /// Soft delete a row with session-based policy checking.
    ///
    /// Checks DELETE USING policy against the existing row before allowing deletion.
    /// Falls back to UPDATE's USING policy if no DELETE policy is defined.
    pub fn delete_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        write_context: Option<&WriteContext>,
    ) -> Result<DeleteHandle, QueryError> {
        let _span = tracing::debug_span!("QM::delete", %id).entered();
        // Check for hard delete first
        if self.visible_row_is_hard_deleted(storage, id, self.current_branch().as_str()) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .load_row_table_name(storage, id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Check if already soft-deleted
        if self.row_is_deleted(storage, &table, id) {
            return Err(QueryError::RowAlreadyDeleted(id));
        }

        // Get old data from the current visible row (for index removal and content preservation)
        let (_, current_row) = self
            .load_visible_row_on_branch(storage, id, self.current_branch().as_str())
            .ok_or(QueryError::ObjectNotFound(id))?;
        let old_data = current_row.data.clone();
        let old_provenance = current_row.row_provenance();

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

        if let Some(session) = write_context.and_then(WriteContext::session) {
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
                        &old_provenance,
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
                        &old_provenance,
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
        let branch = self.current_branch();
        let parents = self.load_branch_tip_ids(storage, &table, id, branch.as_str());
        let timestamp = self.reserve_write_timestamp();
        let delete_provenance =
            self.row_provenance_for_update(&old_provenance, write_context, timestamp);

        // Add commit with preserved content + delete: soft metadata
        // Content is copied from previous tip so soft-deleted rows can still be read
        let delete_row = self.authored_row_version(
            id,
            branch.as_str(),
            parents,
            old_data.to_vec(),
            &delete_provenance,
            Some(DeleteKind::Soft),
        );
        let index_mutations = Self::index_mutations_for_soft_delete_on_branch(
            &table,
            branch.as_str(),
            id,
            &old_data,
            &descriptor,
        );
        let branch_name = BranchName::new(branch.as_str());
        let (delete_version_id, visibility_change) = self.apply_local_row_history_write(
            storage,
            &table,
            &branch_name,
            id,
            delete_row,
            &index_mutations,
        )?;

        tracing::trace!(%id, ?delete_version_id, "apply local soft delete");
        let _ = self.apply_local_row_version(storage, id, visibility_change)?;

        Ok(DeleteHandle {
            row_id: id,
            delete_version_id,
        })
    }

    pub fn delete_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        session: Option<&Session>,
    ) -> Result<DeleteHandle, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.delete_with_write_context(storage, id, owned.as_ref())
    }

    pub fn delete_existing_row_on_branch_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        delete: RowBranchDelete<'_>,
        write_context: Option<&WriteContext>,
    ) -> Result<DeleteHandle, QueryError> {
        let RowBranchDelete {
            table,
            branch,
            id,
            old_data_for_policy,
            old_provenance_for_policy,
        } = delete;
        // Check for hard delete first (checks default branch)
        if self.visible_row_is_hard_deleted(storage, id, branch) {
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

        if let Some(session) = write_context.and_then(WriteContext::session) {
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
                        old_provenance_for_policy,
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
                    old_provenance_for_policy,
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

        // Get old data from the current visible row on this branch
        let old_branch_data = self
            .load_visible_row_on_branch(storage, id, branch)
            .map(|(_, row)| row.data)
            .filter(|data| !data.is_empty());
        let parents = self.load_branch_tip_ids(storage, table, id, branch);
        let timestamp = self.reserve_write_timestamp();
        let delete_provenance =
            self.row_provenance_for_update(old_provenance_for_policy, write_context, timestamp);

        let delete_row = self.authored_row_version(
            id,
            branch,
            parents,
            old_data_for_policy.to_vec(),
            &delete_provenance,
            Some(DeleteKind::Soft),
        );
        let index_mutations = Self::index_mutations_for_soft_delete_on_branch(
            table,
            branch,
            id,
            old_data_for_policy,
            &descriptor,
        );
        let branch_name = BranchName::new(branch);
        let (delete_version_id, visibility_change) = self.apply_local_row_history_write(
            storage,
            table,
            &branch_name,
            id,
            delete_row,
            &index_mutations,
        )?;

        let _ = old_branch_data;
        let _ = descriptor;
        let _ = self.apply_local_row_version(storage, id, visibility_change)?;

        Ok(DeleteHandle {
            row_id: id,
            delete_version_id,
        })
    }

    pub fn delete_existing_row_on_branch_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        delete: RowBranchDelete<'_>,
        session: Option<&Session>,
    ) -> Result<DeleteHandle, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.delete_existing_row_on_branch_with_write_context(storage, delete, owned.as_ref())
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
        if self.visible_row_is_hard_deleted(storage, id, self.current_branch().as_str()) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .load_row_table_name(storage, id)
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
        let branch = self.current_branch();
        let parents = self.load_branch_tip_ids(storage, &table, id, branch.as_str());
        let old_provenance = self
            .load_row_provenance_on_branch(storage, id, branch.as_str())
            .ok_or_else(|| {
                QueryError::EncodingError("missing row provenance on current tip".to_string())
            })?;
        let timestamp = self.reserve_write_timestamp();
        let row_provenance = self.row_provenance_for_update(&old_provenance, None, timestamp);

        // Add commit with row data (no delete metadata = undelete)
        let row = self.authored_row_version(
            id,
            branch.as_str(),
            parents,
            new_data.clone(),
            &row_provenance,
            None,
        );
        let index_mutations = Self::index_mutations_for_undelete_on_branch(
            &table,
            branch.as_str(),
            id,
            &new_data,
            &descriptor,
        );
        let branch_name = BranchName::new(branch.as_str());
        let (row_version_id, visibility_change) = self.apply_local_row_history_write(
            storage,
            &table,
            &branch_name,
            id,
            row,
            &index_mutations,
        )?;

        let _ = self.apply_local_row_version(storage, id, visibility_change)?;

        Ok(InsertResult {
            row_id: id,
            row_version_id,
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
        if self.visible_row_is_hard_deleted(storage, id, self.current_branch().as_str()) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .load_row_table_name(storage, id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Try to get old data (may be empty if already soft-deleted)
        // Treat empty content as no data (tombstone)
        let old_data = self
            .load_visible_row_on_branch(storage, id, self.current_branch().as_str())
            .map(|(_, row)| row.data)
            .filter(|data| !data.is_empty());

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.columns.clone();
        // Get parent commit
        let branch = self.current_branch();
        let parents = self.load_branch_tip_ids(storage, &table, id, branch.as_str());
        let old_provenance = self
            .load_row_provenance_on_branch(storage, id, branch.as_str())
            .ok_or_else(|| {
                QueryError::EncodingError("missing row provenance on current tip".to_string())
            })?;
        let timestamp = self.reserve_write_timestamp();
        let delete_provenance = self.row_provenance_for_update(&old_provenance, None, timestamp);

        // Add commit with empty content + delete: hard metadata
        let delete_row = self.authored_row_version(
            id,
            branch.as_str(),
            parents,
            vec![],
            &delete_provenance,
            Some(DeleteKind::Hard),
        );
        let index_mutations = Self::index_mutations_for_hard_delete_on_branch(
            &table,
            branch.as_str(),
            id,
            old_data.as_deref(),
            &descriptor,
        );
        let branch_name = BranchName::new(branch.as_str());
        let (delete_version_id, visibility_change) = self.apply_local_row_history_write(
            storage,
            &table,
            &branch_name,
            id,
            delete_row,
            &index_mutations,
        )?;

        let _ = old_data;
        let _ = descriptor;
        let _ = self.apply_local_row_version(storage, id, visibility_change)?;

        Ok(DeleteHandle {
            row_id: id,
            delete_version_id,
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
        if self.visible_row_is_hard_deleted(storage, id, self.current_branch().as_str()) {
            return Err(QueryError::RowHardDeleted(id));
        }

        let table = self
            .load_row_table_name(storage, id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        // Verify row is in _id_deleted index (soft-deleted)
        if !self.row_is_deleted(storage, &table, id) {
            return Err(QueryError::RowNotDeleted(id));
        }

        // Upgrade to hard delete
        self.hard_delete(storage, id)
    }

    /// Get a row by ID from storage-backed row histories.
    pub fn get_row(&self, storage: &dyn Storage, id: ObjectId) -> Option<(String, Vec<Value>)> {
        let table = self.load_row_table_name(storage, id)?;
        let table_name = TableName::new(&table);

        let (data, _) = self
            .load_visible_row_on_branch(storage, id, self.current_branch().as_str())
            .map(|(_, row)| {
                let version_id = row.version_id();
                (row.data, version_id)
            })?;

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

    /// Check if a commit has been stored to disk.
    ///
    /// With sync storage, commits are stored immediately.
    /// Used by `InsertResult::is_complete()` to check durability.
    pub fn is_version_stored(
        &self,
        storage: &dyn Storage,
        object_id: ObjectId,
        version_id: &CommitId,
    ) -> bool {
        let Some(table) = self.load_row_table_name(storage, object_id) else {
            return false;
        };
        storage
            .row_version_exists(
                &table,
                self.current_branch().as_str(),
                object_id,
                *version_id,
            )
            .unwrap_or(false)
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
