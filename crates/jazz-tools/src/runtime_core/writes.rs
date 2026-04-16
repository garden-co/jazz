use super::*;

impl<S: Storage, Sch: Scheduler, Sy: SyncSender> RuntimeCore<S, Sch, Sy> {
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
        self.insert_with_id(table, values, None, write_context)
    }

    /// Insert a row into a table with an optional external row id.
    pub fn insert_with_id(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        object_id: Option<ObjectId>,
        write_context: Option<&WriteContext>,
    ) -> Result<InsertedRow, RuntimeError> {
        let _span = debug_span!("insert", table).entered();
        let result = self
            .schema_manager
            .insert_with_write_context_and_id(
                &mut self.storage,
                table,
                values,
                object_id,
                write_context,
            )
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

    /// Create or update a row with a caller-supplied external row id.
    pub fn upsert_with_id(
        &mut self,
        table: &str,
        object_id: ObjectId,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
    ) -> Result<(), RuntimeError> {
        let _span = debug_span!("upsert", table, %object_id).entered();
        self.schema_manager
            .upsert_with_write_context_and_id(
                &mut self.storage,
                table,
                object_id,
                values,
                write_context,
            )
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
        self.insert_persisted_with_id(table, values, None, write_context, tier)
    }

    /// Insert a row with an optional external row id and durability tracking.
    pub fn insert_persisted_with_id(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        object_id: Option<ObjectId>,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<(InsertedRow, oneshot::Receiver<()>), RuntimeError> {
        let result = self
            .schema_manager
            .insert_with_write_context_and_id(
                &mut self.storage,
                table,
                values,
                object_id,
                write_context,
            )
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let row_id = result.row_id;
        let row_version_id = result.row_version_id;
        let row_values = result.row_values;
        let row_version_key =
            RowVersionKey::new(row_id, self.schema_manager.branch_name(), row_version_id);

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
                .entry(row_version_key)
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
        let row_version_key =
            RowVersionKey::new(object_id, self.schema_manager.branch_name(), version_id);

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
                .entry(row_version_key)
                .or_default()
                .push((tier, sender));
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(receiver)
    }

    /// Create or update a row and wait for durability at the requested tier.
    pub fn upsert_persisted_with_id(
        &mut self,
        table: &str,
        object_id: ObjectId,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
        tier: DurabilityTier,
    ) -> Result<oneshot::Receiver<()>, RuntimeError> {
        let version_id = self
            .schema_manager
            .upsert_with_write_context_and_id(
                &mut self.storage,
                table,
                object_id,
                values,
                write_context,
            )
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let row_version_key =
            RowVersionKey::new(object_id, self.schema_manager.branch_name(), version_id);

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
                .entry(row_version_key)
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
        let row_version_key = RowVersionKey::new(
            handle.row_id,
            self.schema_manager.branch_name(),
            handle.delete_version_id,
        );

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
                .entry(row_version_key)
                .or_default()
                .push((tier, sender));
        }

        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(receiver)
    }
}
