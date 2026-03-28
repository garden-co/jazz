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
        session: Option<&Session>,
    ) -> Result<InsertedRow, RuntimeError> {
        let _span = debug_span!("insert", table).entered();
        let result = self
            .schema_manager
            .insert_with_session(&mut self.storage, table, values, session)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let row_id = result.row_id;
        let row_values = result.row_values;
        debug!(object_id = %row_id, "inserted");
        self.immediate_tick();
        Ok((row_id, row_values))
    }

    /// Update a row (partial update by column name).
    pub fn update(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<&Session>,
    ) -> Result<(), RuntimeError> {
        let _span = debug_span!("update", %object_id).entered();
        self.schema_manager
            .update_with_session(&mut self.storage, object_id, &values, session)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;

        self.immediate_tick();
        Ok(())
    }

    /// Delete a row.
    pub fn delete(
        &mut self,
        object_id: ObjectId,
        session: Option<&Session>,
    ) -> Result<(), RuntimeError> {
        let _span = debug_span!("delete", %object_id).entered();
        self.schema_manager
            .delete(&mut self.storage, object_id, session)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        debug!("deleted");
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
        session: Option<&Session>,
        tier: DurabilityTier,
    ) -> Result<(InsertedRow, oneshot::Receiver<()>), RuntimeError> {
        let result = self
            .schema_manager
            .insert_with_session(&mut self.storage, table, values, session)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;
        let row_id = result.row_id;
        let row_commit_id = result.row_commit_id;
        let row_values = result.row_values;

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
                .entry(row_commit_id)
                .or_default()
                .push((tier, sender));
        }

        self.immediate_tick();
        Ok(((row_id, row_values), receiver))
    }

    /// Update a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn update_persisted(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<&Session>,
        tier: DurabilityTier,
    ) -> Result<oneshot::Receiver<()>, RuntimeError> {
        let commit_id = self
            .schema_manager
            .update_with_session(&mut self.storage, object_id, &values, session)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;

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
                .entry(commit_id)
                .or_default()
                .push((tier, sender));
        }

        self.immediate_tick();
        Ok(receiver)
    }

    /// Delete a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn delete_persisted(
        &mut self,
        object_id: ObjectId,
        session: Option<&Session>,
        tier: DurabilityTier,
    ) -> Result<oneshot::Receiver<()>, RuntimeError> {
        let handle = self
            .schema_manager
            .delete(&mut self.storage, object_id, session)
            .map_err(|e| RuntimeError::WriteError(e.to_string()))?;

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
                .entry(handle.delete_commit_id)
                .or_default()
                .push((tier, sender));
        }

        self.immediate_tick();
        Ok(receiver)
    }
}
