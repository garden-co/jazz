use super::*;
use crate::batch_fate::{
    BatchMode, BatchSettlement, LocalBatchMember, LocalBatchRecord, SealedBatchMember,
    SealedBatchSubmission, VisibleBatchMember,
};
use crate::object::BranchName;
use crate::query_manager::types::SchemaHash;
use crate::row_histories::BatchId;
use crate::storage::RowRawTableKind;
use crate::sync_manager::RowBatchKey;

impl<S: Storage, Sch: Scheduler, Sy: SyncSender> RuntimeCore<S, Sch, Sy> {
    fn ack_watcher_key(
        &self,
        row_id: ObjectId,
        batch_id: BatchId,
        write_context: Option<&WriteContext>,
    ) -> RowBatchKey {
        let branch_name = write_context
            .and_then(WriteContext::target_branch_name)
            .map(BranchName::new)
            .unwrap_or_else(|| self.schema_manager.branch_name());
        RowBatchKey::new(row_id, branch_name, batch_id)
    }

    fn default_requested_tier_for_transaction(&self) -> DurabilityTier {
        self.schema_manager
            .query_manager()
            .sync_manager()
            .max_local_durability_tier()
            .unwrap_or(DurabilityTier::Worker)
    }

    fn ensure_transactional_batch_is_writable(
        &self,
        write_context: Option<&WriteContext>,
    ) -> Result<(), RuntimeError> {
        let Some(write_context) = write_context else {
            return Ok(());
        };
        if write_context.batch_mode() != BatchMode::Transactional {
            return Ok(());
        }
        let Some(batch_id) = write_context.batch_id() else {
            return Ok(());
        };

        let Some(record) = self
            .storage
            .load_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("load local batch record: {err}")))?
        else {
            return Ok(());
        };

        if record.mode != BatchMode::Transactional {
            return Err(RuntimeError::WriteError(format!(
                "batch {batch_id:?} reused with conflicting modes"
            )));
        }
        if record.sealed {
            return Err(RuntimeError::WriteError(format!(
                "transactional batch {batch_id:?} is already sealed"
            )));
        }

        Ok(())
    }

    fn track_local_batch(
        &mut self,
        row_id: ObjectId,
        batch_id: BatchId,
        mode: BatchMode,
        requested_tier: DurabilityTier,
    ) -> Result<(), RuntimeError> {
        let branch_name = self.schema_manager.branch_name();
        let visible_members = vec![VisibleBatchMember {
            object_id: row_id,
            branch_name,
            batch_id,
        }];
        let latest_settlement = match mode {
            BatchMode::Direct => self
                .schema_manager
                .query_manager()
                .sync_manager()
                .max_local_durability_tier()
                .map(|confirmed_tier| BatchSettlement::DurableDirect {
                    batch_id,
                    confirmed_tier,
                    visible_members: visible_members.clone(),
                }),
            BatchMode::Transactional => None,
        };

        let mut record = self
            .storage
            .load_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("load local batch record: {err}")))?
            .unwrap_or_else(|| {
                LocalBatchRecord::new(
                    batch_id,
                    mode,
                    requested_tier,
                    matches!(mode, BatchMode::Direct),
                    None,
                )
            });
        if record.mode != mode {
            return Err(RuntimeError::WriteError(format!(
                "batch {batch_id:?} reused with conflicting modes"
            )));
        }
        for member in self.local_batch_members_for_row(row_id, batch_id)? {
            record.upsert_member(member);
        }
        if requested_tier > record.requested_tier {
            record.requested_tier = requested_tier;
        }
        if let Some(settlement) = latest_settlement {
            record.apply_settlement(settlement);
        }

        self.storage
            .upsert_local_batch_record(&record)
            .map_err(|err| RuntimeError::WriteError(format!("persist local batch record: {err}")))
    }

    fn local_batch_members_for_row(
        &self,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Vec<LocalBatchMember>, RuntimeError> {
        let row_locator = self
            .storage
            .load_row_locator(row_id)
            .map_err(|err| RuntimeError::WriteError(format!("load row locator: {err}")))?
            .ok_or_else(|| {
                RuntimeError::WriteError(format!(
                    "missing row locator while tracking local batch {batch_id:?} for {row_id:?}"
                ))
            })?;
        let mut members = self
            .storage
            .scan_history_row_batches(row_locator.table.as_str(), row_id)
            .map_err(|err| RuntimeError::WriteError(format!("scan history rows: {err}")))?
            .into_iter()
            .filter(|row| row.batch_id == batch_id)
            .map(|row| {
                let branch_name = BranchName::new(&row.branch);
                Ok(LocalBatchMember {
                    object_id: row_id,
                    table_name: row_locator.table.to_string(),
                    branch_name,
                    schema_hash: self.local_batch_member_schema_hash(
                        row_locator.table.as_str(),
                        &row_locator,
                        branch_name,
                    )?,
                    row_digest: row.content_digest(),
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        members.sort_by(|left, right| {
            left.object_id
                .uuid()
                .as_bytes()
                .cmp(right.object_id.uuid().as_bytes())
                .then_with(|| left.table_name.cmp(&right.table_name))
                .then_with(|| left.branch_name.as_str().cmp(right.branch_name.as_str()))
                .then_with(|| {
                    left.schema_hash
                        .as_bytes()
                        .cmp(right.schema_hash.as_bytes())
                })
                .then_with(|| left.row_digest.0.cmp(&right.row_digest.0))
        });
        members.dedup();
        if members.is_empty() {
            return Err(RuntimeError::WriteError(format!(
                "missing local batch member rows for {batch_id:?} / {row_id:?}"
            )));
        }
        Ok(members)
    }

    fn local_batch_member_schema_hash(
        &self,
        table: &str,
        row_locator: &crate::storage::RowLocator,
        branch_name: BranchName,
    ) -> Result<SchemaHash, RuntimeError> {
        if let Some(schema_hash) = row_locator.origin_schema_hash {
            return Ok(schema_hash);
        }

        let row_raw_tables = crate::storage::scan_row_raw_table_headers_with_storage(&self.storage)
            .map_err(|err| {
                RuntimeError::WriteError(format!("scan row raw table headers: {err}"))
            })?;

        let matching_hashes: Vec<_> = row_raw_tables
            .into_iter()
            .map(|(row_raw_table_id, _)| row_raw_table_id)
            .filter(|row_raw_table_id| {
                row_raw_table_id.kind == RowRawTableKind::History
                    && row_raw_table_id.table_name == table
            })
            .map(|row_raw_table_id| row_raw_table_id.schema_hash)
            .collect::<Vec<_>>();

        if let [schema_hash] = matching_hashes.as_slice() {
            return Ok(*schema_hash);
        }

        Err(RuntimeError::WriteError(format!(
            "missing schema hash for local batch member table {table} branch {branch_name}"
        )))
    }

    pub(crate) fn transactional_batch_members(
        &self,
        batch_id: BatchId,
    ) -> Result<(crate::object::BranchName, Vec<SealedBatchMember>), RuntimeError> {
        let Some(record) = self
            .storage
            .load_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("load local batch record: {err}")))?
        else {
            return Err(RuntimeError::WriteError(format!(
                "missing local batch record for {batch_id:?}"
            )));
        };
        let Some(first_member) = record.members.first() else {
            return Err(RuntimeError::WriteError(format!(
                "cannot seal empty transactional batch {batch_id:?}"
            )));
        };
        let target_branch_name = first_member.branch_name;
        if record
            .members
            .iter()
            .any(|member| member.branch_name != target_branch_name)
        {
            return Err(RuntimeError::WriteError(format!(
                "transactional batch {batch_id:?} spans multiple target branches"
            )));
        }

        let mut members: Vec<_> = record
            .members
            .into_iter()
            .map(|member| SealedBatchMember {
                object_id: member.object_id,
                row_digest: member.row_digest,
            })
            .collect();
        members.sort_by(|left, right| {
            left.object_id
                .uuid()
                .as_bytes()
                .cmp(right.object_id.uuid().as_bytes())
                .then_with(|| left.row_digest.0.cmp(&right.row_digest.0))
        });
        Ok((target_branch_name, members))
    }

    pub(crate) fn transactional_batch_submission(
        &self,
        batch_id: BatchId,
    ) -> Result<SealedBatchSubmission, RuntimeError> {
        let (target_branch_name, members) = self.transactional_batch_members(batch_id)?;
        let captured_frontier = self
            .storage
            .capture_family_visible_frontier(target_branch_name)
            .map_err(|err| {
                RuntimeError::WriteError(format!("capture family visible frontier: {err}"))
            })?;
        Ok(SealedBatchSubmission::new(
            batch_id,
            target_branch_name,
            members,
            captured_frontier,
        ))
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    /// Insert a row into a table.
    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
    ) -> Result<InsertedRow, RuntimeError> {
        let _span = debug_span!("insert", table).entered();
        self.ensure_transactional_batch_is_writable(write_context)?;
        let result = self
            .schema_manager
            .insert_with_write_context(&mut self.storage, table, values, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let row_id = result.row_id;
        let row_values = result.row_values;
        if write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct)
            == BatchMode::Transactional
        {
            self.track_local_batch(
                row_id,
                result.batch_id,
                BatchMode::Transactional,
                self.default_requested_tier_for_transaction(),
            )?;
        }
        debug!(object_id = %row_id, "inserted");
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok((row_id, row_values))
    }

    /// Update a row (partial update by column name).
    pub fn update(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        write_context: Option<&WriteContext>,
    ) -> Result<(), RuntimeError> {
        let _span = debug_span!("update", %object_id).entered();
        self.ensure_transactional_batch_is_writable(write_context)?;
        let batch_id = self
            .schema_manager
            .update_with_write_context(&mut self.storage, object_id, &values, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        if write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct)
            == BatchMode::Transactional
        {
            self.track_local_batch(
                object_id,
                batch_id,
                BatchMode::Transactional,
                self.default_requested_tier_for_transaction(),
            )?;
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(())
    }

    /// Delete a row.
    pub fn delete(
        &mut self,
        object_id: ObjectId,
        write_context: Option<&WriteContext>,
    ) -> Result<(), RuntimeError> {
        let _span = debug_span!("delete", %object_id).entered();
        self.ensure_transactional_batch_is_writable(write_context)?;
        let handle = self
            .schema_manager
            .delete(&mut self.storage, object_id, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        if write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct)
            == BatchMode::Transactional
        {
            self.track_local_batch(
                object_id,
                handle.batch_id,
                BatchMode::Transactional,
                self.default_requested_tier_for_transaction(),
            )?;
        }
        debug!("deleted");
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(())
    }

    // =========================================================================
    // Persisted CRUD Operations
    // =========================================================================

    /// Insert a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn insert_persisted(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<(InsertedRow, oneshot::Receiver<PersistedWriteAck>), RuntimeError> {
        let (result, _batch_id, receiver) =
            self.insert_persisted_with_batch_id(table, values, write_context, tier)?;
        Ok((result, receiver))
    }

    /// Insert a row and return the logical batch id plus a receiver that
    /// resolves when the requested persistence tier (or higher) acknowledges.
    pub fn insert_persisted_with_batch_id(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<(InsertedRow, BatchId, oneshot::Receiver<PersistedWriteAck>), RuntimeError> {
        self.ensure_transactional_batch_is_writable(write_context)?;
        let result = self
            .schema_manager
            .insert_with_write_context(&mut self.storage, table, values, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let row_id = result.row_id;
        let batch_mode = write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct);
        let batch_id = result.batch_id;
        let row_values = result.row_values;
        self.track_local_batch(row_id, batch_id, batch_mode, tier)?;

        let (sender, receiver) = oneshot::channel();
        if self
            .schema_manager
            .query_manager()
            .sync_manager()
            .has_local_durability_at_least(tier)
        {
            let _ = sender.send(Ok(()));
        } else {
            let row_batch_key = self.ack_watcher_key(row_id, batch_id, write_context);
            self.ack_watchers
                .entry(row_batch_key)
                .or_default()
                .push((tier, sender));
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(((row_id, row_values), batch_id, receiver))
    }

    /// Update a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn update_persisted(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<oneshot::Receiver<PersistedWriteAck>, RuntimeError> {
        let (_batch_id, receiver) =
            self.update_persisted_with_batch_id(object_id, values, write_context, tier)?;
        Ok(receiver)
    }

    /// Update a row and return the logical batch id plus a receiver that
    /// resolves when the requested persistence tier (or higher) acknowledges.
    pub fn update_persisted_with_batch_id(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<(BatchId, oneshot::Receiver<PersistedWriteAck>), RuntimeError> {
        self.ensure_transactional_batch_is_writable(write_context)?;
        let batch_id = self
            .schema_manager
            .update_with_write_context(&mut self.storage, object_id, &values, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let batch_mode = write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct);
        self.track_local_batch(object_id, batch_id, batch_mode, tier)?;

        let (sender, receiver) = oneshot::channel();
        if self
            .schema_manager
            .query_manager()
            .sync_manager()
            .has_local_durability_at_least(tier)
        {
            let _ = sender.send(Ok(()));
        } else {
            let row_batch_key = self.ack_watcher_key(object_id, batch_id, write_context);
            self.ack_watchers
                .entry(row_batch_key)
                .or_default()
                .push((tier, sender));
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok((batch_id, receiver))
    }

    /// Delete a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn delete_persisted(
        &mut self,
        object_id: ObjectId,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<oneshot::Receiver<PersistedWriteAck>, RuntimeError> {
        let (_batch_id, receiver) =
            self.delete_persisted_with_batch_id(object_id, write_context, tier)?;
        Ok(receiver)
    }

    /// Delete a row and return the logical batch id plus a receiver that
    /// resolves when the requested persistence tier (or higher) acknowledges.
    pub fn delete_persisted_with_batch_id(
        &mut self,
        object_id: ObjectId,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<(BatchId, oneshot::Receiver<PersistedWriteAck>), RuntimeError> {
        self.ensure_transactional_batch_is_writable(write_context)?;
        let handle = self
            .schema_manager
            .delete(&mut self.storage, object_id, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let batch_mode = write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct);
        let batch_id = handle.batch_id;
        self.track_local_batch(object_id, batch_id, batch_mode, tier)?;

        let (sender, receiver) = oneshot::channel();
        if self
            .schema_manager
            .query_manager()
            .sync_manager()
            .has_local_durability_at_least(tier)
        {
            let _ = sender.send(Ok(()));
        } else {
            let row_batch_key = self.ack_watcher_key(object_id, batch_id, write_context);
            self.ack_watchers
                .entry(row_batch_key)
                .or_default()
                .push((tier, sender));
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok((batch_id, receiver))
    }

    /// Load one replayable local batch record by logical batch id.
    pub fn local_batch_record(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<LocalBatchRecord>, RuntimeError> {
        self.storage
            .load_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("load local batch record: {err}")))
    }

    /// Scan all replayable local batch records currently retained by this
    /// runtime.
    pub fn local_batch_records(&self) -> Result<Vec<LocalBatchRecord>, RuntimeError> {
        self.storage
            .scan_local_batch_records()
            .map_err(|err| RuntimeError::WriteError(format!("scan local batch records: {err}")))
    }

    /// Acknowledge a replayable rejected batch outcome and prune the local
    /// batch record that kept it alive across reconnect and restart.
    pub fn acknowledge_rejected_batch(&mut self, batch_id: BatchId) -> Result<bool, RuntimeError> {
        let Some(record) = self
            .storage
            .load_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("load local batch record: {err}")))?
        else {
            return Ok(false);
        };

        if !matches!(
            record.latest_settlement,
            Some(BatchSettlement::Rejected { .. })
        ) {
            return Ok(false);
        }

        self.storage
            .delete_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("delete local batch record: {err}")))?;
        self.ack_watchers.retain(|key, _| key.batch_id != batch_id);
        self.mark_storage_write_pending_flush();
        Ok(true)
    }

    pub fn seal_batch(&mut self, batch_id: BatchId) -> Result<(), RuntimeError> {
        let Some(mut record) = self
            .storage
            .load_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("load local batch record: {err}")))?
        else {
            return Err(RuntimeError::WriteError(format!(
                "missing local batch record for {batch_id:?}"
            )));
        };

        if record.mode != BatchMode::Transactional {
            return Err(RuntimeError::WriteError(format!(
                "batch {batch_id:?} is not transactional"
            )));
        }

        let submission = self.transactional_batch_submission(batch_id)?;

        record.mark_sealed(submission.clone());
        self.storage
            .upsert_local_batch_record(&record)
            .map_err(|err| {
                RuntimeError::WriteError(format!("persist local batch record: {err}"))
            })?;
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .seal_batch_to_servers(submission);
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(())
    }
}
