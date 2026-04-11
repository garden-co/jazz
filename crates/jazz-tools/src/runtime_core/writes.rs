use super::*;
use crate::batch_fate::{BatchMode, BatchSettlement, LocalBatchRecord, VisibleBatchMember};
use crate::row_histories::BatchId;

impl<S: Storage, Sch: Scheduler, Sy: SyncSender> RuntimeCore<S, Sch, Sy> {
    fn current_visible_batch_id(&self, row_id: ObjectId) -> Result<BatchId, RuntimeError> {
        let row_locator = self
            .storage
            .load_row_locator(row_id)
            .map_err(|err| RuntimeError::WriteError(format!("load row locator: {err}")))?
            .ok_or_else(|| RuntimeError::WriteError(format!("missing row locator for {row_id}")))?;
        let visible = self
            .storage
            .load_visible_region_row(
                row_locator.table.as_str(),
                self.schema_manager.branch_name().as_str(),
                row_id,
            )
            .map_err(|err| RuntimeError::WriteError(format!("load visible row: {err}")))?
            .ok_or_else(|| RuntimeError::WriteError(format!("missing visible row for {row_id}")))?;
        Ok(visible.batch_id)
    }

    fn track_local_direct_batch(
        &mut self,
        row_id: ObjectId,
        batch_id: BatchId,
        requested_tier: DurabilityTier,
    ) -> Result<(), RuntimeError> {
        let branch_name = self.schema_manager.branch_name();
        let visible_members = vec![VisibleBatchMember {
            object_id: row_id,
            branch_name,
            batch_id,
        }];
        let latest_settlement = self
            .schema_manager
            .query_manager()
            .sync_manager()
            .max_local_durability_tier()
            .map(|confirmed_tier| BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier,
                visible_members: visible_members.clone(),
            });
        let record = LocalBatchRecord::new(
            batch_id,
            BatchMode::Direct,
            requested_tier,
            latest_settlement,
        );
        self.storage
            .upsert_local_batch_record(&record)
            .map_err(|err| RuntimeError::WriteError(format!("persist local batch record: {err}")))
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
        let result = self
            .schema_manager
            .insert_with_write_context(&mut self.storage, table, values, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let row_id = result.row_id;
        let row_values = result.row_values;
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
        self.schema_manager
            .update_with_write_context(&mut self.storage, object_id, &values, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;

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
        self.schema_manager
            .delete(&mut self.storage, object_id, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
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
    ) -> Result<(InsertedRow, oneshot::Receiver<()>), RuntimeError> {
        let result = self
            .schema_manager
            .insert_with_write_context(&mut self.storage, table, values, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let row_id = result.row_id;
        let batch_id = self.current_visible_batch_id(row_id)?;
        let row_values = result.row_values;
        self.track_local_direct_batch(row_id, batch_id, tier)?;

        let (sender, receiver) = oneshot::channel();
        if self
            .schema_manager
            .query_manager()
            .sync_manager()
            .has_local_durability_at_least(tier)
        {
            let _ = sender.send(());
        } else {
            self.ack_watchers
                .entry(batch_id)
                .or_default()
                .push((tier, sender));
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(((row_id, row_values), receiver))
    }

    /// Update a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn update_persisted(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<oneshot::Receiver<()>, RuntimeError> {
        let version_id = self
            .schema_manager
            .update_with_write_context(&mut self.storage, object_id, &values, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let _ = version_id;
        let batch_id = self.current_visible_batch_id(object_id)?;
        self.track_local_direct_batch(object_id, batch_id, tier)?;

        let (sender, receiver) = oneshot::channel();
        if self
            .schema_manager
            .query_manager()
            .sync_manager()
            .has_local_durability_at_least(tier)
        {
            let _ = sender.send(());
        } else {
            self.ack_watchers
                .entry(batch_id)
                .or_default()
                .push((tier, sender));
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(receiver)
    }

    /// Delete a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn delete_persisted(
        &mut self,
        object_id: ObjectId,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<oneshot::Receiver<()>, RuntimeError> {
        let handle = self
            .schema_manager
            .delete(&mut self.storage, object_id, write_context)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let _ = handle;
        let batch_id = self.current_visible_batch_id(object_id)?;
        self.track_local_direct_batch(object_id, batch_id, tier)?;

        let (sender, receiver) = oneshot::channel();
        if self
            .schema_manager
            .query_manager()
            .sync_manager()
            .has_local_durability_at_least(tier)
        {
            let _ = sender.send(());
        } else {
            self.ack_watchers
                .entry(batch_id)
                .or_default()
                .push((tier, sender));
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(receiver)
    }
}
