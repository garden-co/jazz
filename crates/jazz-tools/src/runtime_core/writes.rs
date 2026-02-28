use super::*;

impl<S: Storage, Sch: Scheduler, Sy: SyncSender> RuntimeCore<S, Sch, Sy> {
    // =========================================================================
    // CRUD Operations
    // =========================================================================

    /// Insert a row into a table.
    pub fn insert(
        &mut self,
        table: &str,
        values: Vec<Value>,
        session: Option<&Session>,
    ) -> Result<ObjectId, RuntimeError> {
        let _span = debug_span!("insert", table).entered();
        let result = self
            .schema_manager
            .insert_with_session(&mut self.storage, table, &values, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;
        debug!(object_id = %result.row_id, "inserted");
        self.immediate_tick();
        Ok(result.row_id)
    }

    /// Update a row (partial update by column name).
    pub fn update(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<&Session>,
    ) -> Result<(), RuntimeError> {
        let _span = debug_span!("update", %object_id).entered();
        let current_values = self.merge_row_update_values(object_id, values)?;

        self.schema_manager
            .query_manager_mut()
            .update_with_session(&mut self.storage, object_id, &current_values, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;

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
            .query_manager_mut()
            .delete_with_session(&mut self.storage, object_id, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;
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
        values: Vec<Value>,
        session: Option<&Session>,
        tier: PersistenceTier,
    ) -> Result<(ObjectId, oneshot::Receiver<()>), RuntimeError> {
        let result = self
            .schema_manager
            .insert_with_session(&mut self.storage, table, &values, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;

        let (sender, receiver) = oneshot::channel();
        self.ack_watchers
            .entry(result.row_commit_id)
            .or_default()
            .push((tier, sender));

        self.immediate_tick();
        Ok((result.row_id, receiver))
    }

    /// Update a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn update_persisted(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<&Session>,
        tier: PersistenceTier,
    ) -> Result<oneshot::Receiver<()>, RuntimeError> {
        let current_values = self.merge_row_update_values(object_id, values)?;

        let commit_id = self
            .schema_manager
            .query_manager_mut()
            .update_with_session(&mut self.storage, object_id, &current_values, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;

        let (sender, receiver) = oneshot::channel();
        self.ack_watchers
            .entry(commit_id)
            .or_default()
            .push((tier, sender));

        self.immediate_tick();
        Ok(receiver)
    }

    fn merge_row_update_values(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
    ) -> Result<Vec<Value>, RuntimeError> {
        let (table, mut current_values) = self
            .schema_manager
            .query_manager_mut()
            .get_row(object_id)
            .ok_or(RuntimeError::NotFound)?;

        let schema = self.schema_manager.current_schema();
        let table_name = TableName::new(&table);
        let table_schema = schema
            .get(&table_name)
            .ok_or_else(|| RuntimeError::WriteError("Table not found".to_string()))?;

        for (col_name, new_value) in values {
            if let Some(idx) = table_schema.columns.column_index(&col_name) {
                current_values[idx] = new_value;
            } else {
                return Err(RuntimeError::WriteError(format!(
                    "Column '{}' not found",
                    col_name
                )));
            }
        }

        Ok(current_values)
    }

    /// Delete a row and return a receiver that resolves when the requested
    /// persistence tier (or higher) acknowledges.
    pub fn delete_persisted(
        &mut self,
        object_id: ObjectId,
        session: Option<&Session>,
        tier: PersistenceTier,
    ) -> Result<oneshot::Receiver<()>, RuntimeError> {
        let handle = self
            .schema_manager
            .query_manager_mut()
            .delete_with_session(&mut self.storage, object_id, session)
            .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))?;

        let (sender, receiver) = oneshot::channel();
        self.ack_watchers
            .entry(handle.delete_commit_id)
            .or_default()
            .push((tier, sender));

        self.immediate_tick();
        Ok(receiver)
    }
}
