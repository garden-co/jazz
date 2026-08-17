use super::*;

impl<S> Database<S>
where
    S: ResidentStorage,
{
    /// Run one IVM tick without base-table writes.
    ///
    /// Commiting a batch ticks automatically; `flush` is useful after creating
    /// subscriptions when callers want to drain any pending initial work through
    /// the same public tick path.
    ///
    /// ```rust
    /// # use groove::db::{Database, GraphBuilder};
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// let subscription = database.subscribe_one_sink(GraphBuilder::table("albums"))?;
    /// assert!(subscription.recv()?.is_empty());
    ///
    /// database.flush()?;
    /// assert!(database.last_tick_metrics().is_some());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn flush(&mut self) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        let tick = self
            .ivm_runtime
            .tick(Vec::new(), &storage)
            .map_err(Error::IvmRuntime)?;
        self.last_tick_metrics = Some(tick);
        Ok(())
    }

    pub fn last_commit_metrics(&self) -> Option<&CommitMetrics> {
        self.last_commit_metrics.as_ref()
    }

    pub fn last_tick_metrics(&self) -> Option<&TickMetrics> {
        self.last_tick_metrics.as_ref()
    }

    pub fn storage_read_metrics(&self) -> StorageReadMetrics {
        *self.storage_read_metrics.borrow()
    }

    pub fn reset_storage_read_metrics(&self) {
        *self.storage_read_metrics.borrow_mut() = StorageReadMetrics::default();
    }

    pub fn take_storage_read_metrics(&self) -> StorageReadMetrics {
        let metrics = self.storage_read_metrics();
        self.reset_storage_read_metrics();
        metrics
    }

    /// Commit a batch of table writes and synchronously tick maintained views.
    ///
    /// ```rust
    /// # use groove::db::Database;
    /// # use groove::records::Value;
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]))?;
    /// let mut batch = database.open_batch();
    /// batch.insert(
    ///     "albums",
    ///     vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)],
    /// );
    /// database.commit_batch(batch)?;
    ///
    /// let rows = database.primary_key_scan("albums", &[Value::U64(1)])?;
    /// assert_eq!(rows[0].get("title")?, Value::String("Kind of Blue".into()));
    /// assert_eq!(database.last_commit_metrics().unwrap().storage_write_count, 2);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn commit_batch(&mut self, batch: DatabaseBatch) -> Result<(), Error> {
        let pending_writes = self.pending_writes_from_batch(batch)?;
        self.commit_pending_writes(pending_writes, false).map(drop)
    }

    /// Apply a batch to resident state and synchronously publish its IVM
    /// effects, returning the identical owned operations for later durable
    /// persistence by an async-capable host.
    #[doc(hidden)]
    pub fn commit_batch_for_async_persistence(
        &mut self,
        batch: DatabaseBatch,
    ) -> Result<PendingPersistenceBatch, Error> {
        let pending_writes = self.pending_writes_from_batch(batch)?;
        self.commit_pending_writes(pending_writes, true)
            .map(|receipt| receipt.expect("async persistence requested a receipt"))
    }

    /// Publish one storage-resolved batch through the real resident IVM.
    #[doc(hidden)]
    pub fn commit_prepared_batch_for_async_persistence(
        &mut self,
        prepared: PreparedDatabaseBatch,
    ) -> Result<PendingPersistenceBatch, Error> {
        self.commit_pending_writes(prepared.pending_writes, true)
            .map(|receipt| receipt.expect("async persistence requested a receipt"))
    }

    /// Acquire every durable input that a subsequent live IVM tick can read.
    ///
    /// This deliberately does not evaluate the graph. The real runtime is
    /// ticked exactly once, after this storage-only preparation succeeds.
    #[doc(hidden)]
    pub fn prepare_batch_storage_inputs(
        &self,
        batch: &DatabaseBatch,
    ) -> Result<PreparedDatabaseBatch, Error> {
        self.ensure_not_poisoned()?;
        let prepared = self.prepare_resident_batch(batch)?;
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        self.ivm_runtime
            .tick_storage_requirements()
            .and_then(|requirements| requirements.ensure_resident(&storage))
            .map_err(Error::IvmRuntime)?;
        // Keep the operation conversion in preparation: Delta writes can read
        // the resident value while being converted to an owned durable write.
        for write in &prepared.pending_writes {
            self.owned_storage_operation_for_pending(write)?;
        }
        Ok(prepared)
    }

    /// Resolve base-table writes without acquiring async-only IVM closure
    /// inputs. Fully resident Memory/RocksDB callers use this path and retain
    /// their existing point-read cost model.
    #[doc(hidden)]
    pub fn prepare_resident_batch(
        &self,
        batch: &DatabaseBatch,
    ) -> Result<PreparedDatabaseBatch, Error> {
        self.ensure_not_poisoned()?;
        Ok(PreparedDatabaseBatch {
            pending_writes: self.pending_writes_from_operations(&batch.operations)?,
        })
    }

    /// Fail closed after a resident commit could not be durably persisted.
    #[doc(hidden)]
    pub fn mark_async_persistence_failed(&mut self) {
        self.ivm_runtime.discard_staged_subscription_notifications();
        self.poisoned.store(true, Ordering::Release);
    }

    pub fn update_raw(
        &mut self,
        table: &str,
        key: PrimaryKeyValue,
        record: impl Into<RawRecordInput>,
    ) -> Result<(), Error> {
        let pending = self.pending_write_from_operation(&BatchOperation::UpdateRaw {
            table: table.to_owned(),
            key,
            record: record.into(),
        })?;
        self.commit_pending_writes(vec![pending], false).map(drop)
    }

    pub(super) fn commit_pending_writes(
        &mut self,
        pending_writes: Vec<PendingTableWrite>,
        retain_persistence_batch: bool,
    ) -> Result<Option<PendingPersistenceBatch>, Error> {
        let descriptors = pending_writes
            .iter()
            .map(PendingTableWrite::descriptor)
            .collect::<Vec<_>>();
        let stores = pending_writes
            .iter()
            .zip(&descriptors)
            .map(|(write, descriptor)| {
                let key_descriptor = self
                    .table(write.table())
                    .ok()
                    .and_then(|table| table.primary_key.as_ref().map(primary_key_descriptor));
                record_store_for_table(&self.storage, write.table(), key_descriptor, descriptor)
            })
            .collect::<Vec<_>>();
        let table_deltas =
            compute_table_deltas(&pending_writes, &stores, self.ivm_runtime.schema())?;
        let mut staged_operations = pending_writes
            .iter()
            .map(|write| match write {
                PendingTableWrite::Set { key, .. } => OwnedWriteOperation::Set {
                    cf: write.table().to_owned(),
                    key: key.clone(),
                    value: write.stored_record().expect("set has a stored record"),
                },
                PendingTableWrite::Delete { key, .. } => OwnedWriteOperation::Delete {
                    cf: write.table().to_owned(),
                    key: key.clone(),
                },
            })
            .collect::<Vec<_>>();
        let tick_start = Instant::now();
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        let tick = self
            .ivm_runtime
            .tick_staged(table_deltas, &storage, &mut staged_operations)
            .map_err(Error::IvmRuntime)?;
        let ivm_tick_time = tick_start.elapsed();
        let operations = staged_operations
            .iter()
            .map(OwnedWriteOperation::as_write_operation)
            .collect::<Vec<_>>();
        let storage_writes = StorageWriteMetrics::from_operations(&operations);
        let storage_write_count = storage_writes.total.count;
        let storage_write_bytes = storage_writes.total.bytes;
        let persistence_batch = retain_persistence_batch.then(|| PendingPersistenceBatch {
            operations: staged_operations.clone(),
        });
        let storage_start = Instant::now();
        let txn = self.storage.begin_txn();
        drop(operations);
        txn.stage_owned_operations(staged_operations);
        if let Err(error) = txn.commit() {
            // The runtime has already advanced in memory by this point. The v0
            // policy is to make the Database instance fatal on final commit
            // failure rather than serve possibly torn in-memory state.
            self.ivm_runtime.discard_staged_subscription_notifications();
            self.poisoned.store(true, Ordering::Release);
            return Err(Error::from(error));
        }
        if self
            .durable_publication_state
            .lock()
            .expect("durable publication state mutex poisoned")
            .depth
            == 0
        {
            self.ivm_runtime.publish_staged_subscription_notifications();
        }
        let storage_write_time = storage_start.elapsed();
        self.last_tick_metrics = Some(tick.clone());
        self.last_commit_metrics = Some(CommitMetrics {
            storage_write_time,
            ivm_tick_time,
            storage_write_count,
            storage_write_bytes,
            storage_writes,
            tick,
        });
        Ok(persistence_batch)
    }

    pub(super) fn pending_writes_from_batch(
        &self,
        batch: DatabaseBatch,
    ) -> Result<Vec<PendingTableWrite>, Error> {
        self.pending_writes_from_operations(&batch.operations)
    }

    pub(super) fn pending_writes_from_operations(
        &self,
        operations: &[BatchOperation],
    ) -> Result<Vec<PendingTableWrite>, Error> {
        let mut pending_writes = Vec::with_capacity(operations.len());

        for operation in operations {
            pending_writes.push(self.pending_write_from_operation(operation)?);
        }

        Ok(pending_writes)
    }

    pub(super) fn ensure_batch_storage_txn(&self, batch: &DatabaseBatch) -> Result<(), Error> {
        let mut txn_operations = batch.txn_operations.borrow_mut();
        while batch.txn_indexed_operations.get() < batch.operations.len() {
            let operation = &batch.operations[batch.txn_indexed_operations.get()];
            let pending = self.pending_write_from_operation(operation)?;
            txn_operations.stage(self.owned_storage_operation_for_pending(&pending)?);
            batch
                .txn_indexed_operations
                .set(batch.txn_indexed_operations.get() + 1);
        }
        Ok(())
    }

    pub(super) fn owned_storage_operation_for_pending(
        &self,
        pending: &PendingTableWrite,
    ) -> Result<OwnedWriteOperation, Error> {
        Ok(match pending {
            PendingTableWrite::Set { key, .. } => OwnedWriteOperation::Set {
                cf: pending.table().to_owned(),
                key: key.clone(),
                value: pending.stored_record().expect("set has a stored record"),
            },
            PendingTableWrite::Delete { key, .. } => OwnedWriteOperation::Delete {
                cf: pending.table().to_owned(),
                key: key.clone(),
            },
        })
    }

    pub(super) fn pending_write_from_operation(
        &self,
        operation: &BatchOperation,
    ) -> Result<PendingTableWrite, Error> {
        match operation {
            BatchOperation::Insert { table, record } => {
                let table_schema = self.table(table)?;
                let (variant_tag, descriptor, record) = resolve_record_input(table_schema, record)?;
                let key = primary_key_bytes(table_schema, variant_tag, descriptor, &record)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Insert,
                    table: table.clone(),
                    key,
                    variant_tag,
                    descriptor,
                    record,
                })
            }
            BatchOperation::InsertRaw { table, key, record } => {
                let table_schema = self.table(table)?;
                let (variant_tag, descriptor, record) =
                    resolve_raw_record_input(table_schema, record)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Insert,
                    table: table.clone(),
                    key: key.clone().into_bytes(),
                    variant_tag,
                    descriptor,
                    record: record.clone(),
                })
            }
            BatchOperation::InsertRawFresh { table, key, record } => {
                let table_schema = self.table(table)?;
                let (variant_tag, descriptor, record) =
                    resolve_raw_record_input(table_schema, record)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::InsertFresh,
                    table: table.clone(),
                    key: key.clone().into_bytes(),
                    variant_tag,
                    descriptor,
                    record: record.clone(),
                })
            }
            BatchOperation::Update { table, record } => {
                let table_schema = self.table(table)?;
                let (variant_tag, descriptor, record) = resolve_record_input(table_schema, record)?;
                let key = primary_key_bytes(table_schema, variant_tag, descriptor, &record)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Update,
                    table: table.clone(),
                    key,
                    variant_tag,
                    descriptor,
                    record,
                })
            }
            BatchOperation::UpdateRaw { table, key, record } => {
                let table_schema = self.table(table)?;
                let (variant_tag, descriptor, record) =
                    resolve_raw_record_input(table_schema, record)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Update,
                    table: table.clone(),
                    key: key.clone().into_bytes(),
                    variant_tag,
                    descriptor,
                    record: record.clone(),
                })
            }
            BatchOperation::Delete { table, key } => {
                let table_schema = self.table(table)?;
                Ok(PendingTableWrite::Delete {
                    table: table.clone(),
                    key: key.clone().into_bytes(),
                    descriptor: table_schema.record_schema(),
                })
            }
        }
    }

    pub(super) fn table(&self, table: &str) -> Result<&TableSchema, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .table(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    pub(super) fn ensure_not_poisoned(&self) -> Result<(), Error> {
        if self.poisoned.load(Ordering::Acquire)
            || self
                .durable_publication_state
                .lock()
                .expect("durable publication state mutex poisoned")
                .aborted
        {
            Err(Error::DatabasePoisoned)
        } else {
            Ok(())
        }
    }
}
