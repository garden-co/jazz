use super::*;
use crate::batch_fate::{
    BatchFate, BatchMode, LocalBatchMember, LocalBatchRecord, SealedBatchMember,
    SealedBatchSubmission,
};
use crate::object::BranchName;
use crate::query_manager::types::SchemaHash;
use crate::row_histories::BatchId;

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    fn local_write_confirmed_tier(&self) -> DurabilityTier {
        self.schema_manager
            .query_manager()
            .sync_manager()
            .max_local_durability_tier()
            .unwrap_or(DurabilityTier::Local)
    }

    fn completed_batch_wait_receiver(
        outcome: PersistedWriteAck,
    ) -> oneshot::Receiver<PersistedWriteAck> {
        let (sender, receiver) = oneshot::channel();
        let _ = sender.send(outcome);
        receiver
    }

    fn batch_wait_outcome(
        fate: Option<&BatchFate>,
        tier: DurabilityTier,
    ) -> Option<PersistedWriteAck> {
        match fate {
            Some(BatchFate::Rejected {
                batch_id,
                code,
                reason,
            }) => Some(Err(PersistedWriteRejection {
                batch_id: *batch_id,
                code: code.clone(),
                reason: reason.clone(),
            })),
            Some(fate) => match fate.confirmed_tier() {
                Some(confirmed_tier) if confirmed_tier >= tier => Some(Ok(())),
                _ => None,
            },
            None => None,
        }
    }

    fn local_batch_record_for_wait(
        &self,
        batch_id: BatchId,
    ) -> Result<LocalBatchRecord, RuntimeError> {
        self.storage
            .load_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("load local batch record: {err}")))?
            .or_else(|| self.local_batch_record_cache.get(&batch_id).cloned())
            .ok_or_else(|| {
                RuntimeError::WriteError(format!("missing local batch record for {batch_id:?}"))
            })
    }

    fn register_batch_waiter(
        &mut self,
        batch_id: BatchId,
        tier: DurabilityTier,
    ) -> oneshot::Receiver<PersistedWriteAck> {
        let (sender, receiver) = oneshot::channel();
        self.durability
            .register_batch_watcher(batch_id, tier, sender);
        receiver
    }

    fn should_auto_seal_direct_write(
        batch_mode: BatchMode,
        write_context: Option<&WriteContext>,
    ) -> bool {
        batch_mode == BatchMode::Direct && write_context.and_then(WriteContext::batch_id).is_none()
    }

    fn ensure_batch_is_writable(
        &mut self,
        write_context: Option<&WriteContext>,
    ) -> Result<(), RuntimeError> {
        let Some(write_context) = write_context else {
            return Ok(());
        };
        let mode = write_context.batch_mode();
        let Some(batch_id) = write_context.batch_id() else {
            return Ok(());
        };

        if let Some(record) = self.local_batch_record_cache.get(&batch_id) {
            if record.mode != mode {
                return Err(RuntimeError::WriteError(format!(
                    "batch {batch_id:?} reused with conflicting modes"
                )));
            }
            if record.sealed {
                return Err(RuntimeError::WriteError(format!(
                    "batch {batch_id:?} is already sealed"
                )));
            }
            return Ok(());
        }

        let Some(record) = self
            .storage
            .load_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("load local batch record: {err}")))?
        else {
            self.local_batch_record_cache
                .insert(batch_id, LocalBatchRecord::new(batch_id, mode, false, None));
            return Ok(());
        };

        if record.mode != mode {
            return Err(RuntimeError::WriteError(format!(
                "batch {batch_id:?} reused with conflicting modes"
            )));
        }
        if record.sealed {
            return Err(RuntimeError::WriteError(format!(
                "batch {batch_id:?} is already sealed"
            )));
        }

        self.local_batch_record_cache.insert(batch_id, record);
        Ok(())
    }

    fn track_local_batch(
        &mut self,
        row_id: ObjectId,
        batch_id: BatchId,
        mode: BatchMode,
    ) -> Result<(), RuntimeError> {
        let mut record = self
            .local_batch_record_cache
            .remove(&batch_id)
            .map(Ok)
            .unwrap_or_else(|| {
                self.storage
                    .load_local_batch_record(batch_id)
                    .map_err(|err| {
                        RuntimeError::WriteError(format!("load local batch record: {err}"))
                    })
                    .map(|record| {
                        record.unwrap_or_else(|| LocalBatchRecord::new(batch_id, mode, false, None))
                    })
            })?;
        if record.mode != mode {
            return Err(RuntimeError::WriteError(format!(
                "batch {batch_id:?} reused with conflicting modes"
            )));
        }
        for member in self.local_batch_members_for_row(row_id, batch_id)? {
            record.upsert_member(member);
        }

        self.local_batch_record_cache.insert(batch_id, record);
        Ok(())
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
                        branch_name,
                        row_id,
                        row.batch_id(),
                    )?,
                    row_digest: row.content_digest(),
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        if members.len() > 1 {
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
        }
        if members.is_empty() {
            return Err(RuntimeError::WriteError(format!(
                "missing local batch member rows for {batch_id:?} / {row_id:?}"
            )));
        }
        Ok(members)
    }

    fn local_batch_member_schema_hash(
        &self,
        branch_name: BranchName,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<SchemaHash, RuntimeError> {
        if let Some(locator) = self
            .storage
            .load_history_row_batch_table_locator(branch_name.as_str(), row_id, batch_id)
            .map_err(|err| {
                RuntimeError::WriteError(format!("load history row batch locator: {err}"))
            })?
        {
            return Ok(locator.schema_hash);
        }

        if let Some(origin_schema_hash) = self
            .storage
            .load_row_locator(row_id)
            .map_err(|err| RuntimeError::WriteError(format!("load row locator: {err}")))?
            .and_then(|locator| locator.origin_schema_hash)
        {
            return Ok(origin_schema_hash);
        }

        Err(RuntimeError::WriteError(format!(
            "missing schema hash for local batch member branch {branch_name} batch {batch_id:?}"
        )))
    }

    pub(crate) fn sealed_batch_members_from_record(
        record: &LocalBatchRecord,
    ) -> Result<(crate::object::BranchName, Vec<SealedBatchMember>), RuntimeError> {
        let Some(first_member) = record.members.first() else {
            return Err(RuntimeError::WriteError(format!(
                "cannot seal empty batch {:?}",
                record.batch_id
            )));
        };
        let target_branch_name = first_member.branch_name;
        if record
            .members
            .iter()
            .any(|member| member.branch_name != target_branch_name)
        {
            return Err(RuntimeError::WriteError(format!(
                "batch {:?} spans multiple target branches",
                record.batch_id
            )));
        }

        let mut members: Vec<_> = record
            .members
            .iter()
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

    pub(crate) fn sealed_batch_submission(
        &self,
        record: &LocalBatchRecord,
    ) -> Result<SealedBatchSubmission, RuntimeError> {
        let (target_branch_name, members) = Self::sealed_batch_members_from_record(record)?;
        let captured_frontier = if record.mode == BatchMode::Transactional {
            self.storage
                .capture_family_visible_frontier(target_branch_name)
                .map_err(|err| {
                    RuntimeError::WriteError(format!("capture family visible frontier: {err}"))
                })?
        } else {
            Vec::new()
        };
        Ok(SealedBatchSubmission::new(
            record.batch_id,
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
    ) -> Result<DirectInsertResult, RuntimeError> {
        let _span = debug_span!("insert", table).entered();
        self.ensure_batch_is_writable(write_context)?;
        let result = self
            .schema_manager
            .insert_with_write_context(&mut self.storage, table, values, write_context)
            .map_err(crate::runtime_core::write_error_from_query)?;
        let row_id = result.row_id;
        let row_values = result.row_values;
        let batch_id = result.batch_id;
        let batch_mode = write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct);
        self.track_local_batch(row_id, batch_id, batch_mode)?;
        if Self::should_auto_seal_direct_write(batch_mode, write_context) {
            self.seal_batch(batch_id)?;
        }
        debug!(object_id = %row_id, "inserted");
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(((row_id, row_values), batch_id))
    }

    /// Compatibility shim for callers that pass an explicit row id.
    pub fn insert_with_id(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        object_id: Option<ObjectId>,
        write_context: Option<&WriteContext>,
    ) -> Result<DirectInsertResult, RuntimeError> {
        self.ensure_batch_is_writable(write_context)?;
        let result = self
            .schema_manager
            .insert_with_write_context_and_id(
                &mut self.storage,
                table,
                values,
                object_id,
                write_context,
            )
            .map_err(crate::runtime_core::write_error_from_query)?;
        let row_id = result.row_id;
        let row_values = result.row_values;
        let batch_id = result.batch_id;
        let batch_mode = write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct);
        self.track_local_batch(row_id, batch_id, batch_mode)?;
        if Self::should_auto_seal_direct_write(batch_mode, write_context) {
            self.seal_batch(batch_id)?;
        }
        debug!(object_id = %row_id, "inserted");
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(((row_id, row_values), batch_id))
    }

    /// Update a row (partial update by column name).
    pub fn update(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        write_context: Option<&WriteContext>,
    ) -> Result<BatchId, RuntimeError> {
        let _span = debug_span!("update", %object_id).entered();
        self.ensure_batch_is_writable(write_context)?;
        let batch_id = self
            .schema_manager
            .update_with_write_context(&mut self.storage, object_id, &values, write_context)
            .map_err(crate::runtime_core::write_error_from_query)?;
        let batch_mode = write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct);
        self.track_local_batch(object_id, batch_id, batch_mode)?;
        if Self::should_auto_seal_direct_write(batch_mode, write_context) {
            self.seal_batch(batch_id)?;
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(batch_id)
    }

    /// Compatibility shim for callers that expect explicit-id upserts.
    pub fn upsert_with_id(
        &mut self,
        table: &str,
        object_id: ObjectId,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
    ) -> Result<BatchId, RuntimeError> {
        let _span = debug_span!("upsert", table, %object_id).entered();
        self.ensure_batch_is_writable(write_context)?;
        let batch_id = self
            .schema_manager
            .upsert_with_write_context_and_id(
                &mut self.storage,
                table,
                object_id,
                values,
                write_context,
            )
            .map_err(crate::runtime_core::write_error_from_query)?;
        let batch_mode = write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct);
        self.track_local_batch(object_id, batch_id, batch_mode)?;

        if Self::should_auto_seal_direct_write(batch_mode, write_context) {
            self.seal_batch(batch_id)?;
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(batch_id)
    }

    /// Delete a row.
    pub fn delete(
        &mut self,
        object_id: ObjectId,
        write_context: Option<&WriteContext>,
    ) -> Result<BatchId, RuntimeError> {
        let _span = debug_span!("delete", %object_id).entered();
        self.ensure_batch_is_writable(write_context)?;
        let handle = self
            .schema_manager
            .delete(&mut self.storage, object_id, write_context)
            .map_err(crate::runtime_core::write_error_from_query)?;
        let batch_id = handle.batch_id;
        let batch_mode = write_context
            .map(WriteContext::batch_mode)
            .unwrap_or(BatchMode::Direct);
        self.track_local_batch(object_id, batch_id, batch_mode)?;
        if Self::should_auto_seal_direct_write(batch_mode, write_context) {
            self.seal_batch(batch_id)?;
        }
        debug!("deleted");
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(batch_id)
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

    /// Wait for a batch to settle at `tier` or higher.
    pub fn wait_for_batch(
        &mut self,
        batch_id: BatchId,
        tier: DurabilityTier,
    ) -> Result<oneshot::Receiver<PersistedWriteAck>, RuntimeError> {
        let record = self.local_batch_record_for_wait(batch_id)?;

        if let Some(outcome) = Self::batch_wait_outcome(record.latest_fate.as_ref(), tier) {
            return Ok(Self::completed_batch_wait_receiver(outcome));
        }

        Ok(self.register_batch_waiter(batch_id, tier))
    }

    /// Drain replayable rejected batch ids that should be surfaced by bindings.
    pub fn drain_rejected_batch_ids(&mut self) -> Vec<BatchId> {
        self.durability.drain_rejected()
    }

    /// Acknowledge a replayable rejected batch outcome and prune the local
    /// batch record that kept it alive across reconnect and restart.
    pub fn acknowledge_rejected_batch(&mut self, batch_id: BatchId) -> Result<bool, RuntimeError> {
        self.local_batch_record_cache.remove(&batch_id);
        let Some(record) = self
            .storage
            .load_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("load local batch record: {err}")))?
        else {
            return Ok(false);
        };

        if !matches!(record.latest_fate, Some(BatchFate::Rejected { .. })) {
            return Ok(false);
        }

        self.storage
            .delete_local_batch_record(batch_id)
            .map_err(|err| RuntimeError::WriteError(format!("delete local batch record: {err}")))?;
        self.durability.forget_batch(batch_id);
        self.mark_storage_write_pending_flush();
        Ok(true)
    }

    pub fn seal_batch(&mut self, batch_id: BatchId) -> Result<(), RuntimeError> {
        let mut record = if let Some(record) = self.local_batch_record_cache.remove(&batch_id) {
            record
        } else {
            let Some(record) = self
                .storage
                .load_local_batch_record(batch_id)
                .map_err(|err| {
                    RuntimeError::WriteError(format!("load local batch record: {err}"))
                })?
            else {
                return Err(RuntimeError::WriteError(format!(
                    "missing local batch record for {batch_id:?}"
                )));
            };
            record
        };

        if record.sealed {
            self.local_batch_record_cache.insert(batch_id, record);
            return Ok(());
        }

        let submission = self.sealed_batch_submission(&record)?;

        record.mark_sealed(submission.clone());
        if record.mode == BatchMode::Direct {
            let confirmed_tier = self.local_write_confirmed_tier();
            let settlement = BatchFate::DurableDirect {
                batch_id,
                confirmed_tier,
            };
            record.apply_fate(settlement.clone());
        }
        self.storage
            .upsert_local_batch_record(&record)
            .map_err(|err| {
                RuntimeError::WriteError(format!("persist local batch record: {err}"))
            })?;
        self.local_batch_record_cache.insert(batch_id, record);
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .seal_batch_to_servers(submission);
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(())
    }
}
