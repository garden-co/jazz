//! Durable branch metadata and branch-local write/read helpers for
//! `jazz/BRANCHES.md`. This module owns branch creation, lifecycle records, and
//! partitioned branch storage access; base snapshot semantics use
//! [`crate::tx::Snapshot`], recovery lives in [`super::recovery`], and ordinary
//! global/local currency logic remains in [`super::currency`]. It is a node
//! sublayer beside the main global history path.

use super::policy::{policy_join_row_value, policy_value_key};
use super::*;
use crate::schema::{
    branch_metadata_table_schema, branch_partition_history_table_name,
    branch_partition_register_table_name,
};

/// Durable branch metadata recovered from `jazz_branches`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchRecord {
    /// Branch identity.
    pub branch_id: BranchId,
    /// Parent branch, or `None` for a root branch.
    pub parent: Option<BranchId>,
    /// Frozen parent settled cut. Root branches have no base.
    pub base: Option<Snapshot>,
    /// Branch lifecycle state.
    pub state: codec::BranchState,
}

#[derive(Default)]
struct BranchEvaluationContext {
    policy_join_rows_by_value:
        BTreeMap<BranchPolicyJoinRowsKey, BTreeMap<Vec<u8>, Vec<CurrentRow>>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BranchPolicyJoinRowsKey {
    branch_id: BranchId,
    table: String,
    on_column: String,
}

struct BranchPolicyRequest<'a> {
    table: &'a TableSchema,
    policy: &'a crate::query::Query,
    row_uuid: RowUuid,
    identity: AuthorId,
    branch: &'a BranchRecord,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Create a snapshot-base branch over this node's current settled watermark.
    ///
    /// Creation writes only one metadata row; overlay tables are created lazily
    /// on the first branch write.
    pub fn create_branch(&mut self, branch_id: BranchId) -> Result<BranchRecord, Error> {
        let record = BranchRecord {
            branch_id,
            parent: None,
            base: Some(
                Snapshot::exclusive_base(
                    self.node_uuid,
                    self.clock.applied_global_watermark,
                    self.clock.tx_time,
                    Vec::new(),
                )
                .map_err(Error::InvalidStoredValue)?,
            ),
            state: codec::BranchState::Open,
        };
        self.persist_branch_record(&record)?;
        self.branches.branches.insert(branch_id, record.clone());
        Ok(record)
    }

    /// Declare a root branch with no parent fallback.
    pub fn create_root_branch(&mut self, branch_id: BranchId) -> Result<BranchRecord, Error> {
        let record = BranchRecord {
            branch_id,
            parent: None,
            base: None,
            state: codec::BranchState::Open,
        };
        self.persist_branch_record(&record)?;
        self.branches.branches.insert(branch_id, record.clone());
        Ok(record)
    }

    /// Return recovered branch metadata.
    pub fn branch_record(&self, branch_id: BranchId) -> Option<&BranchRecord> {
        self.branches.branches.get(&branch_id)
    }

    /// Discard an open branch without deleting its overlay history.
    pub fn discard_branch(&mut self, branch_id: BranchId) -> Result<(), Error> {
        let mut record = self
            .branches
            .branches
            .get(&branch_id)
            .cloned()
            .ok_or(Error::BranchNotFound(branch_id))?;
        if record.state != codec::BranchState::Open {
            return Err(Error::BranchClosed(branch_id));
        }
        record.state = codec::BranchState::Discarded;
        self.persist_branch_record(&record)?;
        self.branches.branches.insert(branch_id, record);
        Ok(())
    }

    /// Merge an open branch overlay back into its parent as one mergeable squash.
    pub fn merge_back_branch(&mut self, branch_id: BranchId) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        let branch = self
            .branches
            .branches
            .get(&branch_id)
            .cloned()
            .ok_or(Error::BranchNotFound(branch_id))?;
        if branch.state != codec::BranchState::Open {
            return Err(Error::BranchClosed(branch_id));
        }

        let mut versions = Vec::new();
        let write_schema_version = self.catalogue.current_write_schema.schema;
        for table in self.catalogue.schema.tables.clone() {
            let table_schema = self.table_in_schema(&table.name, write_schema_version)?;
            for row_uuid in self.branch_overlay_row_ids(&table.name, branch_id)? {
                for layer in [VersionLayer::Content, VersionLayer::Deletion] {
                    let Some(winner) =
                        self.branch_overlay_layer_winner(&table.name, row_uuid, layer, branch_id)?
                    else {
                        continue;
                    };
                    let branch_tip = self.version_tx_id(&winner)?;
                    let parent_tip = self
                        .query_local_layer_winner(&table.name, row_uuid, layer)?
                        .map(|version| self.version_tx_id(&version))
                        .transpose()?;
                    let mut parents = parent_tip.into_iter().collect::<Vec<_>>();
                    if !parents.contains(&branch_tip) {
                        parents.push(branch_tip);
                    }
                    parents.sort();
                    let cells = table_schema
                        .columns
                        .iter()
                        .map(|column| winner.cell(&table_schema, &column.name))
                        .collect::<Result<Vec<_>, _>>()?;
                    versions.push(
                        VersionRecord::encode(
                            &table_schema,
                            write_schema_version,
                            row_uuid,
                            parents,
                            winner.created_by(),
                            winner.created_at(),
                            winner.updated_by(),
                            winner.updated_at(),
                            &cells,
                            winner.deletion(),
                        )
                        .map_err(Error::from)?,
                    );
                }
            }
        }

        if versions.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "merge-back requires at least one branch overlay write",
            ));
        }
        let tx_id = self.commit_merge_back_squash(branch_id, versions)?;
        let mut record = branch;
        record.state = codec::BranchState::Merged;
        self.persist_branch_record(&record)?;
        self.branches.branches.insert(branch_id, record);
        Ok(tx_id)
    }

    /// Branch-scoped exclusives are intentionally not implemented in v1.
    pub fn open_exclusive_on_branch(&mut self, _branch_id: BranchId) -> Result<OpenTxId, Error> {
        Err(Error::UnsupportedBranchExclusive)
    }

    /// Commit a mergeable write into a branch overlay partition.
    pub fn commit_mergeable_on_branch(
        &mut self,
        branch_id: BranchId,
        commit: MergeableCommit,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        commit.validate()?;
        self.ensure_branch_open(branch_id)?;
        let mut context = BranchEvaluationContext::default();
        if !self.branch_write_policy_allows(branch_id, commit.made_by, &mut context)? {
            return Err(Error::AuthorizationDenied);
        }
        let made_at = self.mint_tx_time(commit.now_ms);
        self.commit_mergeable_on_branch_at(branch_id, commit, made_at)
    }

    fn commit_mergeable_on_branch_at(
        &mut self,
        branch_id: BranchId,
        commit: MergeableCommit,
        made_at: TxTime,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table_in_schema(&commit.table, write_schema_version)?;
        let branch = self
            .branches
            .branches
            .get(&branch_id)
            .cloned()
            .ok_or(Error::BranchNotFound(branch_id))?;
        let version = VersionRecord::from_commit(&commit, &table_schema, write_schema_version)?;
        let mut context = BranchEvaluationContext::default();
        if !self.branch_table_write_policy_allows_version_record(
            &branch,
            &table_schema,
            &version,
            commit.made_by,
            &mut context,
        )? {
            return Err(Error::AuthorizationDenied);
        }
        self.persist_branch_partition(commit.table.clone(), write_schema_version, branch_id)?;
        let tx_id = TxId::new(made_at, self.node_uuid);
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: 1,
            made_by: commit.made_by,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: commit.user_metadata_json.clone(),
            source_branch: None,
        };
        let tx_node_alias = self.ensure_node_alias(tx_id.node)?;
        let schema_version_alias = self.ensure_schema_version_alias(write_schema_version)?;
        let stored = VersionRow::from_parts_with_schema_version(
            &table_schema,
            VersionRowParts {
                table: commit.table.clone(),
                row_uuid: commit.row_uuid,
                tx_node_alias,
                schema_version_alias,
                tx_time: made_at,
                parents: commit.parents,
                created_by: commit.made_by,
                created_at: TxTime(commit.now_ms),
                updated_by: commit.made_by,
                updated_at: TxTime(commit.now_ms),
                cells: commit.cells,
                deletion: commit.deletion,
            },
            None,
        )?;
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_transactions",
            transaction_values(
                tx_node_alias,
                &tx,
                Fate::Pending,
                None,
                DurabilityTier::Local,
            ),
        );
        batch.insert_raw(
            branch_version_storage_table_name(
                &table_schema.name,
                stored.layer(),
                write_schema_version,
                branch_id,
            ),
            history_primary_key(&stored),
            stored.record.raw().to_vec(),
        );
        self.database.commit_batch(batch)?;
        Ok(tx_id)
    }

    /// Read a validated query in a branch view: overlay rows first, then the
    /// frozen parent `at(base)` read for rows absent from the overlay.
    pub fn query_rows_on_branch(
        &mut self,
        branch_id: BranchId,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_on_branch_for_link(branch_id, shape, binding, AuthorId::SYSTEM)
    }

    /// Read a validated query in a branch view for a peer identity. The branch
    /// metadata row is the first-level access symbol; if it is not readable, no
    /// branch overlay/base view is exposed. Rows that pass the branch row gate
    /// are then narrowed by ordinary table read policy evaluated in the branch
    /// view.
    pub fn query_rows_on_branch_for_link(
        &mut self,
        branch_id: BranchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let branch = self
            .branches
            .branches
            .get(&branch_id)
            .cloned()
            .ok_or(Error::BranchNotFound(branch_id))?;
        let mut context = BranchEvaluationContext::default();
        if !self.branch_read_policy_allows(&branch, identity, &mut context)? {
            return Ok(Vec::new());
        }
        let mut rows =
            self.query_rows_on_branch_query_engine(branch_id, shape, binding, identity)?;
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    fn branch_read_policy_allows(
        &mut self,
        branch: &BranchRecord,
        identity: AuthorId,
        context: &mut BranchEvaluationContext,
    ) -> Result<bool, Error> {
        if identity == AuthorId::SYSTEM {
            return Ok(true);
        }
        let Some(policy) = self.catalogue.schema.branch_read_policy.clone() else {
            return Ok(true);
        };
        let table = branch_metadata_table_schema();
        self.branch_policy_allows(
            BranchPolicyRequest {
                table: &table,
                policy: &policy,
                row_uuid: RowUuid(branch.branch_id.0),
                identity,
                branch,
            },
            context,
            |column| branch_metadata_value(branch, column),
        )
    }

    fn branch_write_policy_allows(
        &mut self,
        branch_id: BranchId,
        identity: AuthorId,
        context: &mut BranchEvaluationContext,
    ) -> Result<bool, Error> {
        if identity == AuthorId::SYSTEM {
            return Ok(true);
        }
        let Some(policy) = self.catalogue.schema.branch_write_policy.clone() else {
            return Ok(true);
        };
        let branch = self
            .branches
            .branches
            .get(&branch_id)
            .cloned()
            .ok_or(Error::BranchNotFound(branch_id))?;
        let table = branch_metadata_table_schema();
        self.branch_policy_allows(
            BranchPolicyRequest {
                table: &table,
                policy: &policy,
                row_uuid: RowUuid(branch.branch_id.0),
                identity,
                branch: &branch,
            },
            context,
            |column| branch_metadata_value(&branch, column),
        )
    }

    fn branch_table_write_policy_allows_version_record(
        &mut self,
        branch: &BranchRecord,
        table: &TableSchema,
        version: &VersionRecord,
        author: AuthorId,
        context: &mut BranchEvaluationContext,
    ) -> Result<bool, Error> {
        if author == AuthorId::SYSTEM {
            return Ok(true);
        }
        if version.deletion().is_some() {
            let Some(policy) = table.write_policies.delete_using.clone() else {
                return Ok(false);
            };
            let Some(row) = self.branch_delete_subject_row(branch, table, version)? else {
                return Ok(false);
            };
            return self.branch_policy_allows(
                BranchPolicyRequest {
                    table,
                    policy: &policy,
                    row_uuid: row.row_uuid(),
                    identity: author,
                    branch,
                },
                context,
                |column| row.cell(table, column),
            );
        }
        let previous = self.branch_delete_subject_row(branch, table, version)?;
        if let Some(previous) = previous {
            if let Some(policy) = table.write_policies.update_using.clone() {
                if !self.branch_policy_allows(
                    BranchPolicyRequest {
                        table,
                        policy: &policy,
                        row_uuid: previous.row_uuid(),
                        identity: author,
                        branch,
                    },
                    context,
                    |column| previous.cell(table, column),
                )? {
                    return Ok(false);
                }
            }
            let Some(policy) = table.write_policies.update_check.clone() else {
                return Ok(false);
            };
            return self.branch_policy_allows(
                BranchPolicyRequest {
                    table,
                    policy: &policy,
                    row_uuid: version.row_uuid(),
                    identity: author,
                    branch,
                },
                context,
                |column| {
                    table
                        .columns
                        .iter()
                        .position(|candidate| candidate.name == column)
                        .and_then(|idx| version.cell_at(idx))
                },
            );
        }
        let Some(policy) = table.write_policies.insert_check.clone() else {
            return Ok(true);
        };
        self.branch_policy_allows(
            BranchPolicyRequest {
                table,
                policy: &policy,
                row_uuid: version.row_uuid(),
                identity: author,
                branch,
            },
            context,
            |column| {
                table
                    .columns
                    .iter()
                    .position(|candidate| candidate.name == column)
                    .and_then(|idx| version.cell_at(idx))
            },
        )
    }

    fn branch_delete_subject_row(
        &mut self,
        branch: &BranchRecord,
        table: &TableSchema,
        version: &VersionRecord,
    ) -> Result<Option<CurrentRow>, Error> {
        if let Some(row) = self
            .branch_current_rows(&table.name, branch)?
            .into_iter()
            .find(|row| row.row_uuid() == version.row_uuid())
        {
            return Ok(Some(row));
        }

        for parent in version.parents() {
            for parent_version in self.query_versions_for_tx(parent)? {
                if parent_version.table() != table.name
                    || parent_version.row_uuid() != version.row_uuid()
                    || parent_version.layer() != VersionLayer::Content
                {
                    continue;
                }
                return current_row_from_version_projection(table, &parent_version).map(Some);
            }
        }

        Ok(None)
    }

    fn branch_policy_allows(
        &mut self,
        request: BranchPolicyRequest<'_>,
        context: &mut BranchEvaluationContext,
        mut column_value: impl FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        if !request.policy.policy_branches.is_empty() {
            if self.branch_policy_base_allows(
                BranchPolicyRequest {
                    table: request.table,
                    policy: request.policy,
                    row_uuid: request.row_uuid,
                    identity: request.identity,
                    branch: request.branch,
                },
                context,
                &mut column_value,
            )? {
                return Ok(true);
            }
            for branch in &request.policy.policy_branches {
                let branch_policy = branch.as_query(&request.policy.table);
                if self.branch_policy_base_allows(
                    BranchPolicyRequest {
                        table: request.table,
                        policy: &branch_policy,
                        row_uuid: request.row_uuid,
                        identity: request.identity,
                        branch: request.branch,
                    },
                    context,
                    &mut column_value,
                )? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        self.branch_policy_base_allows(request, context, &mut column_value)
    }

    fn branch_policy_base_allows(
        &mut self,
        request: BranchPolicyRequest<'_>,
        context: &mut BranchEvaluationContext,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        if !self.policy_filters_allow(
            request.table,
            request.policy,
            request.identity,
            &mut *column_value,
        )? {
            return Ok(false);
        }
        self.branch_policy_joins_allow(request, context, column_value)
    }

    fn branch_policy_joins_allow(
        &mut self,
        request: BranchPolicyRequest<'_>,
        context: &mut BranchEvaluationContext,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        for join in &request.policy.joins {
            let join_table = self.table(&join.table)?.clone();
            let target = self.branch_policy_join_target_value(&request, join, column_value)?;
            let Some(target) = target else {
                return Ok(false);
            };
            let Some(target_key) = policy_value_key(&target) else {
                return Ok(false);
            };
            let join_policy = crate::query::Query {
                table: join.table.clone(),
                filters: join.filters.clone(),
                joins: join.nested_joins.clone(),
                policy_branches: Vec::new(),
                reachable: Vec::new(),
                includes: Vec::new(),
                array_subqueries: Vec::new(),
                select: None,
                order_by: Vec::new(),
                aggregate: None,
                limit: None,
                offset: 0,
            };
            let mut found = false;
            let candidates = self.branch_policy_join_rows_by_target_memo(
                &join_table,
                join,
                request.branch,
                context,
            )?;
            if let Some(rows) = candidates.get(&target_key).cloned() {
                for row in rows {
                    if self.branch_policy_base_allows(
                        BranchPolicyRequest {
                            branch: request.branch,
                            table: &join_table,
                            policy: &join_policy,
                            row_uuid: row.row_uuid(),
                            identity: request.identity,
                        },
                        context,
                        &mut |column| row.cell(&join_table, column),
                    )? {
                        found = true;
                        break;
                    }
                }
            }
            if !found {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn branch_policy_join_target_value(
        &mut self,
        request: &BranchPolicyRequest<'_>,
        join: &crate::query::JoinVia,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Result<Option<Value>, Error> {
        if let Some(lookup) = &join.source_lookup {
            let Some(Value::Uuid(parent_row_uuid)) = column_value(&lookup.row_id_source_column)
            else {
                return Ok(None);
            };
            let lookup_table = self.table(&lookup.table)?.clone();
            let Some(parent_row) = self
                .branch_current_rows(&lookup.table, request.branch)?
                .into_iter()
                .find(|row| row.row_uuid() == RowUuid(parent_row_uuid))
            else {
                return Ok(None);
            };
            if lookup.value_column == "id" {
                return Ok(Some(Value::Uuid(parent_row.row_uuid().0)));
            }
            return Ok(parent_row.cell(&lookup_table, &lookup.value_column));
        }
        if let Some(source_column) = &join.source_column {
            if source_column == "id" {
                return Ok(Some(Value::Uuid(request.row_uuid.0)));
            }
            return Ok(column_value(source_column));
        }
        Ok(Some(Value::Uuid(request.row_uuid.0)))
    }

    fn branch_policy_join_rows_by_target_memo<'a>(
        &mut self,
        join_table: &TableSchema,
        join: &crate::query::JoinVia,
        branch: &BranchRecord,
        context: &'a mut BranchEvaluationContext,
    ) -> Result<&'a BTreeMap<Vec<u8>, Vec<CurrentRow>>, Error> {
        let key = BranchPolicyJoinRowsKey {
            branch_id: branch.branch_id,
            table: join.table.clone(),
            on_column: join.on_column.clone(),
        };
        if !context.policy_join_rows_by_value.contains_key(&key) {
            let mut rows_by_target: BTreeMap<Vec<u8>, Vec<CurrentRow>> = BTreeMap::new();
            for row in self.branch_current_rows(&join.table, branch)? {
                if let Some(value) = policy_join_row_value(&row, join_table, &join.on_column)
                    && let Some(key) = policy_value_key(&value)
                {
                    rows_by_target.entry(key).or_default().push(row);
                }
            }
            context
                .policy_join_rows_by_value
                .insert(key.clone(), rows_by_target);
        }
        Ok(context
            .policy_join_rows_by_value
            .get(&key)
            .expect("branch policy join rows memo populated"))
    }

    pub(super) fn branch_current_rows(
        &mut self,
        table: &str,
        branch: &BranchRecord,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table(table)?.clone();
        let overlay = self.branch_overlay_rows(table, &table_schema, branch.branch_id)?;
        let overlay_row_ids = overlay
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<BTreeSet<_>>();
        let mut by_row = overlay
            .into_iter()
            .map(|row| (row.row_uuid(), row))
            .collect::<BTreeMap<_, _>>();
        if let Some(base) = &branch.base {
            for row in self.current_rows_at(table, base.global_base)? {
                if !overlay_row_ids.contains(&row.row_uuid()) {
                    by_row.insert(row.row_uuid(), row);
                }
            }
        }
        let mut rows = by_row.into_values().collect::<Vec<_>>();
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    fn branch_overlay_rows(
        &mut self,
        table: &str,
        table_schema: &TableSchema,
        branch_id: BranchId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = Vec::new();
        for row_uuid in self.branch_overlay_row_ids(table, branch_id)? {
            if self
                .branch_overlay_layer_winner(table, row_uuid, VersionLayer::Deletion, branch_id)?
                .is_some_and(|version| version.deletion() == Some(DeletionEvent::Deleted))
            {
                continue;
            }
            let Some(content) = self.branch_overlay_layer_winner(
                table,
                row_uuid,
                VersionLayer::Content,
                branch_id,
            )?
            else {
                continue;
            };
            rows.push(current_row_from_version_projection(table_schema, &content)?);
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    fn branch_overlay_row_ids(
        &mut self,
        table: &str,
        branch_id: BranchId,
    ) -> Result<BTreeSet<RowUuid>, Error> {
        let mut row_ids = BTreeSet::new();
        for (logical_table, schema_version, candidate_branch) in
            self.branches.branch_partitions.clone()
        {
            if logical_table != table || candidate_branch != branch_id {
                continue;
            }
            for storage_table in [
                branch_partition_history_table_name(table, schema_version, branch_id),
                branch_partition_register_table_name(table, schema_version, branch_id),
            ] {
                for raw in self.database.primary_key_scan_raw(&storage_table, &[])? {
                    row_ids.insert(RowUuid(
                        raw.record()
                            .get_uuid(HistoryRowRecord::FIELD_ROW_UUID_IDX)?,
                    ));
                }
            }
        }
        Ok(row_ids)
    }

    fn branch_overlay_layer_winner(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        branch_id: BranchId,
    ) -> Result<Option<VersionRow>, Error> {
        let mut versions = Vec::new();
        for (logical_table, schema_version, candidate_branch) in
            self.branches.branch_partitions.clone()
        {
            if logical_table != table || candidate_branch != branch_id {
                continue;
            }
            let schema_table = self.table_in_schema(table, schema_version)?;
            let storage_table =
                branch_version_storage_table_name(table, layer, schema_version, branch_id);
            let descriptor = match layer {
                VersionLayer::Content => schema_table.history_storage_table().record_schema(),
                VersionLayer::Deletion => schema_table.register_storage_table().record_schema(),
            };
            for raw in self
                .database
                .primary_key_scan_raw(&storage_table, &[Value::Uuid(row_uuid.0)])?
            {
                versions.push(VersionRow {
                    table: groove::Intern::new(table.to_owned()),
                    record: OwnedRecord::new(raw.raw().to_vec(), descriptor),
                });
            }
        }
        let candidates = (0..versions.len()).collect::<Vec<_>>();
        Ok(
            current_version_index(&versions, &candidates, layer, &self.node_aliases)
                .map(|idx| versions[idx].clone()),
        )
    }

    fn ensure_branch_open(&self, branch_id: BranchId) -> Result<(), Error> {
        match self.branches.branches.get(&branch_id) {
            Some(record) if record.state == codec::BranchState::Open => Ok(()),
            Some(_) => Err(Error::BranchClosed(branch_id)),
            None => Err(Error::BranchNotFound(branch_id)),
        }
    }

    fn commit_merge_back_squash(
        &mut self,
        branch_id: BranchId,
        versions: Vec<VersionRecord>,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        let made_at = self.mint_tx_time(0);
        let tx_id = TxId::new(made_at, self.node_uuid);
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: versions.len().try_into().map_err(|_| {
                Error::InvalidMergeableCommit("transaction write count exceeds u32")
            })?,
            made_by: AuthorId::SYSTEM,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            source_branch: Some(branch_id),
        };
        self.ingest_edge_authority_mergeable_commit_unit(tx, versions, made_at.physical_ms())?;
        Ok(tx_id)
    }

    fn persist_branch_record(&mut self, record: &BranchRecord) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_branches",
            vec![
                Value::Uuid(record.branch_id.0),
                Value::Nullable(record.parent.map(|id| Box::new(Value::Uuid(id.0)))),
                Value::Nullable(
                    record
                        .base
                        .as_ref()
                        .map(|base| Box::new(Value::U64(base.global_base.0))),
                ),
                Value::String(branch_state_string(record.state).to_owned()),
            ],
        );
        self.database.commit_batch(batch)?;
        Ok(())
    }

    pub(super) fn recover_branch_record(
        &mut self,
        record: BorrowedRecord<'_>,
    ) -> Result<(), Error> {
        let branch_id = BranchId(record.get_uuid(BranchRowRecord::FIELD_BRANCH_ID_IDX)?);
        let parent = record
            .get_nullable_uuid(BranchRowRecord::FIELD_PARENT_IDX)?
            .map(BranchId);
        let base = record
            .get_nullable_u64(BranchRowRecord::FIELD_BASE_GLOBAL_IDX)?
            .map(|global| {
                Snapshot::exclusive_base(
                    self.node_uuid,
                    GlobalSeq(global),
                    TxTime::default(),
                    Vec::new(),
                )
                .map_err(Error::InvalidStoredValue)
            })
            .transpose()?;
        let state =
            branch_state_from_discriminant(record.get_enum(BranchRowRecord::FIELD_STATE_IDX)?)?;
        self.branches.branches.insert(
            branch_id,
            BranchRecord {
                branch_id,
                parent,
                base,
                state,
            },
        );
        Ok(())
    }

    fn persist_branch_partition(
        &mut self,
        table: String,
        schema_version: SchemaVersionId,
        branch_id: BranchId,
    ) -> Result<(), Error>
    where
        S: ReopenableStorage,
    {
        if !self
            .branches
            .branch_partitions
            .insert((table.clone(), schema_version, branch_id))
        {
            return Ok(());
        }
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_branch_partitions",
            vec![
                Value::Bytes(table.as_bytes().to_vec()),
                Value::Uuid(schema_version.0),
                Value::Uuid(branch_id.0),
            ],
        );
        self.database.commit_batch(batch)?;
        self.rebuild_database_slot()?;
        Ok(())
    }
}

fn branch_state_string(state: codec::BranchState) -> &'static str {
    match state {
        codec::BranchState::Open => "open",
        codec::BranchState::Merged => "merged",
        codec::BranchState::Discarded => "discarded",
    }
}

fn branch_state_from_discriminant(value: u8) -> Result<codec::BranchState, Error> {
    match value {
        0 => Ok(codec::BranchState::Open),
        1 => Ok(codec::BranchState::Merged),
        2 => Ok(codec::BranchState::Discarded),
        _ => Err(Error::InvalidStoredValue("unknown branch state")),
    }
}

fn branch_metadata_value(branch: &BranchRecord, column: &str) -> Option<Value> {
    match column {
        "branch_id" => Some(Value::Uuid(branch.branch_id.0)),
        "parent" => Some(Value::Nullable(
            branch.parent.map(|id| Box::new(Value::Uuid(id.0))),
        )),
        "base_global" => Some(Value::Nullable(
            branch
                .base
                .as_ref()
                .map(|base| Box::new(Value::U64(base.global_base.0))),
        )),
        "state" => Some(Value::String(branch_state_string(branch.state).to_owned())),
        _ => None,
    }
}
