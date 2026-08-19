use super::*;
use crate::storage::WriteManyOutcome;

impl<S> Database<S>
where
    S: OrderedKvStorage,
{
    /// Run one IVM tick without base-table writes.
    ///
    /// Commiting a batch ticks automatically; `flush` is useful after creating
    /// subscriptions when callers want to drain any pending initial work through
    /// the same public tick path.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
    /// # use groove::db::{Database, GraphBuilder};
    /// # use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema};
    /// # use groove::storage::MemoryStorage;
    /// # let schema = DatabaseSchema::new([TableSchema::new("albums", [
    /// #     ColumnSchema::new("id", ColumnType::U64),
    /// #     ColumnSchema::new("title", ColumnType::String),
    /// #     ColumnSchema::new("year", ColumnType::U64),
    /// # ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// #   .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"])).await?;
    /// let subscription = database.subscribe_one_sink(GraphBuilder::table("albums")).await?;
    /// assert!(subscription.recv()?.is_empty());
    ///
    /// database.flush().await?;
    /// assert!(database.last_tick_metrics().is_some());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn flush(&mut self) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .drive_pending_incremental()
            .await
            .map_err(Error::IvmRuntime)?;
        let storage = MeteredStorage::new(&self.storage, &self.storage_read_metrics);
        let tick = self
            .ivm_runtime
            .tick(Vec::new(), &storage)
            .await
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
    /// # futures::executor::block_on(async {
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
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"])).await?;
    /// let mut batch = database.open_batch();
    /// batch.insert(
    ///     "albums",
    ///     vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)],
    /// );
    /// database.commit_batch(batch).await?;
    ///
    /// let rows = database.primary_key_scan("albums", &[Value::U64(1)]).await?;
    /// assert_eq!(rows[0].get("title")?, Value::String("Kind of Blue".into()));
    /// assert_eq!(database.last_commit_metrics().unwrap().storage_write_count, 2);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn commit_batch(&mut self, batch: DatabaseBatch) -> Result<(), Error> {
        let pending_writes = self.pending_writes_from_batch(batch)?;
        self.commit_pending_writes(pending_writes).await
    }

    /// Publish resident rows and unblocked terminal deltas before ordered
    /// persistence. The returned handle owns persistence and no longer borrows
    /// this database, so resident queries may continue while storage suspends.
    pub async fn publish_batch(&mut self, batch: DatabaseBatch) -> Result<PublishedBatch<S>, Error>
    where
        S: 'static,
    {
        self.ensure_not_poisoned()?;
        let pending_writes = self.pending_writes_from_batch(batch)?;
        let descriptors = pending_writes
            .iter()
            .map(PendingTableWrite::descriptor)
            .collect::<Vec<_>>();
        let overlay = StagedWriteOverlay::new(&self.storage, &self.resident_writes);
        let stores = pending_writes
            .iter()
            .zip(&descriptors)
            .map(|(write, descriptor)| {
                let key_descriptor = self
                    .table(write.table())
                    .ok()
                    .and_then(|table| table.primary_key.as_ref().map(primary_key_descriptor));
                record_store_for_table(&overlay, write.table(), key_descriptor, descriptor)
            })
            .collect::<Vec<_>>();
        let table_deltas =
            compute_table_deltas(&pending_writes, &stores, self.ivm_runtime.schema()).await?;
        let staged_operations = pending_writes
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
        let resident_overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let staged_state = Rc::new(RefCell::new(StagedWriteState::from(staged_operations)));
        let storage = Rc::new(StagedWriteOverlay::new_owned(
            resident_overlay,
            Rc::clone(&staged_state),
        ));
        let publication = PublicationId(self.next_publication_id);
        self.next_publication_id = self.next_publication_id.saturating_add(1);
        if let Err(error) = self
            .ivm_runtime
            .tick_resident_staged(table_deltas, OwnedStorage::new(storage), publication)
            .await
        {
            self.poisoned = true;
            return Err(Error::IvmRuntime(error));
        }
        let staged_operations = std::mem::take(&mut *staged_state.borrow_mut()).into_operations();

        self.resident_writes
            .borrow_mut()
            .extend(staged_operations.iter().cloned());
        self.resident_publications
            .insert(publication, staged_operations.clone());
        Ok(PublishedBatch {
            publication,
            storage: Rc::clone(&self.storage),
            operations: staged_operations,
            order: Rc::clone(&self.publication_persistence),
        })
    }

    /// Install one persistence result and advance only the contiguous durable
    /// publication frontier.
    pub fn settle_publication(
        &mut self,
        persistence: PublicationPersistence,
    ) -> Result<PublicationId, Error> {
        if let Err(error) = persistence.result {
            self.poisoned = true;
            return Err(Error::from(error));
        }
        if !self
            .resident_publications
            .contains_key(&persistence.publication)
        {
            return Err(Error::PublicationNotFound(persistence.publication));
        }
        self.persisted_publications.insert(persistence.publication);
        let mut frontier = self
            .durable_publication_frontier
            .map_or(1, |publication| publication.0.saturating_add(1));
        while self.persisted_publications.remove(&PublicationId(frontier)) {
            self.resident_publications.remove(&PublicationId(frontier));
            self.durable_publication_frontier = Some(PublicationId(frontier));
            frontier = frontier.saturating_add(1);
        }
        let mut resident_writes = StagedWriteState::default();
        for operations in self.resident_publications.values() {
            resident_writes.extend(operations.iter().cloned());
        }
        *self.resident_writes.borrow_mut() = resident_writes;
        Ok(persistence.publication)
    }

    pub async fn update_raw(
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
        self.commit_pending_writes(vec![pending]).await
    }

    pub(super) async fn commit_pending_writes(
        &mut self,
        pending_writes: Vec<PendingTableWrite>,
    ) -> Result<(), Error> {
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
            compute_table_deltas(&pending_writes, &stores, self.ivm_runtime.schema()).await?;
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
        let mut staged_runtime = self.ivm_runtime.clone();
        let tick = staged_runtime
            .tick_staged(table_deltas, &storage, &mut staged_operations)
            .await
            .map_err(Error::IvmRuntime)?;
        let publication = PublicationId(self.next_publication_id);
        self.next_publication_id = self.next_publication_id.saturating_add(1);
        staged_runtime.tag_staged_subscription_notifications(publication);
        let ivm_tick_time = tick_start.elapsed();
        let operations = staged_operations
            .iter()
            .map(OwnedWriteOperation::as_write_operation)
            .collect::<Vec<_>>();
        let storage_writes = StorageWriteMetrics::from_operations(&operations);
        let storage_write_count = storage_writes.total.count;
        let storage_write_bytes = storage_writes.total.bytes;
        let storage_start = Instant::now();
        let txn = self.storage.begin_txn();
        drop(operations);
        txn.stage_owned_operations(staged_operations);
        if let Err(error) = txn.commit().await {
            staged_runtime.discard_staged_subscription_notifications();
            self.poisoned = true;
            return Err(Error::from(error));
        }
        self.ivm_runtime = staged_runtime;
        self.durable_publication_frontier = Some(publication);
        if self
            .durable_publication_state
            .lock()
            .expect("durable publication state mutex poisoned")
            .depth
            == 0
        {
            self.ivm_runtime.publish_staged_subscription_notifications();
        } else {
            self.durable_publication_state
                .lock()
                .expect("durable publication state mutex poisoned")
                .successful_commits += 1;
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
        Ok(())
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
        if self.poisoned
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
