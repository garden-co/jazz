use super::*;
impl Database {
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
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families")).await?;
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
        if let Err(error) = self.ivm_runtime.drive_pending_incremental().await {
            self.poisoned = true;
            return Err(Error::IvmRuntime(error));
        }
        self.refresh_resident_writes();
        let resident = StagedWriteOverlay::new(&self.storage, &self.resident_writes);
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        let tick = match self.ivm_runtime.tick(Vec::new(), &storage).await {
            Ok(tick) => tick,
            Err(error) => {
                self.poisoned = true;
                return Err(Error::IvmRuntime(error));
            }
        };
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
    /// # let mut database = Database::new(schema, MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families")).await?;
    /// let mut batch = database.open_batch();
    /// batch.insert(
    ///     "albums",
    ///     vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)],
    /// );
    /// let applied = database.apply_batch(batch).await?;
    /// let persisted = applied.persist().await;
    /// database.finish_persistence(persisted)?;
    ///
    /// let rows = database.primary_key_scan("albums", &[Value::U64(1)]).await?;
    /// assert_eq!(rows[0].get("title")?, Value::String("Kind of Blue".into()));
    /// assert_eq!(database.last_commit_metrics().unwrap().storage_write_count, 2);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    #[cfg(any(test, feature = "test"))]
    pub async fn commit_batch(&mut self, batch: DatabaseBatch) -> Result<(), Error> {
        let publication = self.apply_batch(batch).await?;
        let persistence = publication.persist().await;
        self.finish_persistence(persistence)?;
        Ok(())
    }

    /// Apply resident rows and unblocked terminal deltas before ordered
    /// persistence. The returned handle owns the pending persistence work and
    /// no longer borrows this database, so resident queries may continue while
    /// storage suspends.
    pub async fn apply_batch(&mut self, batch: DatabaseBatch) -> Result<AppliedBatch, Error> {
        self.ensure_not_poisoned()?;
        let accepted_large_values = batch.accepted_large_values.clone();
        let defer_notifications_until_durable =
            batch.notification_timing == NotificationTiming::AfterPersistence;
        let pending_writes = self.pending_writes_from_batch(batch)?;
        let mut accepted_staging = Vec::new();
        for staged_id in &accepted_large_values {
            let key = staged_large_value_key(*staged_id);
            let encoded = self
                .storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
                .await?
                .ok_or_else(|| {
                    Error::InvalidLargeValueMetadata(
                        "accepted staging id is missing or already consumed".to_owned(),
                    )
                })?;
            let staged = decode_staged_large_value_at_key(&key, &encoded)?;
            accepted_staging.push(staged);
        }
        for staged in &accepted_staging {
            let mut found = false;
            for write in &pending_writes {
                let PendingTableWrite::Set {
                    descriptor, record, ..
                } = write
                else {
                    continue;
                };
                for value in descriptor.bind(record).to_values()? {
                    if value_contains_large_ref(&value, &staged.value_ref) {
                        found = true;
                        break;
                    }
                }
                if found {
                    break;
                }
            }
            if !found {
                return Err(Error::InvalidLargeValueMetadata(
                    "accepted staging root is not referenced by this physical-record batch"
                        .to_owned(),
                ));
            }
        }
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
        let mut durable_root_deltas = BTreeMap::<crate::large_values::NodeRef, i64>::new();
        for table_delta in &table_deltas {
            for delta in &table_delta.deltas {
                for value in table_delta.descriptor.bind(&delta.record).to_values()? {
                    collect_large_root_deltas(&value, delta.weight, &mut durable_root_deltas);
                }
            }
        }
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
        for staged_id in accepted_large_values {
            let key = staged_large_value_key(staged_id);
            if self
                .storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
                .await?
                .is_none()
            {
                return Err(Error::InvalidLargeValueMetadata(
                    "accepted staging id is missing or already consumed".to_owned(),
                ));
            }
            staged_operations.push(OwnedWriteOperation::Delete {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key,
            });
            staged_operations.extend(
                super::facade::completed_large_value_cleanup_operations(&self.storage, staged_id)
                    .await?,
            );
        }
        let mut accepted_roots = BTreeMap::<crate::large_values::NodeRef, u64>::new();
        for staged in &accepted_staging {
            *accepted_roots
                .entry(staged.value_ref.root.clone())
                .or_default() += 1;
        }
        let roots = durable_root_deltas
            .keys()
            .chain(accepted_roots.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let resident_overlay = Rc::new(StagedWriteOverlay::new_owned(
            Rc::clone(&self.storage),
            Rc::clone(&self.resident_writes),
        ));
        let staged_state = Rc::new(RefCell::new(StagedWriteState::from(staged_operations)));
        let storage = Rc::new(StagedWriteOverlay::new_owned(
            resident_overlay,
            Rc::clone(&staged_state),
        ));
        let resident_install_durable = Rc::new(Cell::new(false));
        let resident_install_failures = crate::chunks::PublicationInstallFailures::default();
        let resident_install_observer = Rc::new(MetadataChunkInstallObserver {
            storage: Rc::downgrade(&self.storage),
            lifecycle: std::sync::Arc::downgrade(&self.large_value_lifecycle),
            resident_install: Some(ResidentLifecycleInstall {
                storage: OwnedStorage::new(Rc::clone(&storage)),
                staged: Rc::clone(&staged_state),
                lifecycle_held: Rc::clone(&self.large_value_lifecycle_held),
                durable: Rc::clone(&resident_install_durable),
                install_failures: resident_install_failures.clone(),
            }),
        }) as Rc<dyn crate::chunks::ChunkInstallObserver>;
        let tick_start = Instant::now();
        let resident_tick = match self
            .ivm_runtime
            .tick_resident_staged(
                table_deltas,
                OwnedStorage::new(Rc::clone(&storage)),
                defer_notifications_until_durable,
                Some((resident_install_observer, resident_install_failures)),
            )
            .await
        {
            Ok(tick) => tick,
            Err(error) => {
                self.poisoned = true;
                return Err(Error::IvmRuntime(error));
            }
        };
        // The public direct write APIs are themselves the runtime owner for
        // CPU-only continuations which this tick scheduled.  In particular,
        // a bounded recursive evaluation may yield after making resident
        // progress and self-wake for another slice.  Finish those slices
        // before returning the publication so its subscription output is
        // observable in the same direct write turn. A genuinely cold input
        // remains pending for an external owner even if its storage future
        // eagerly wakes, rather than making a write wait on storage.
        self.drain_self_scheduled_resident_progress()?;
        let ivm_tick_time = tick_start.elapsed();
        // The IVM's resident observer uses the staged overlay. It takes the
        // lifecycle lock itself only when another resident publication does
        // not already hold it. After the tick settles, compute from the
        // complete overlay. No publication id exists yet, so cancellation
        // while waiting for this lock cannot leave an ordered-persistence hole.
        let lifecycle_guard =
            if roots.is_empty() || self.large_value_publication_lifecycle_guard.is_some() {
                None
            } else {
                Some(self.large_value_lifecycle.clone().lock_owned().await)
            };
        if !roots.is_empty() {
            let mut node_transitions = Vec::<(crate::large_values::NodeRef, i8)>::new();
            let mut lifecycle_operations = Vec::new();
            for root in &roots {
                let key = large_value_root_key(root)?;
                let mut references = match storage
                    .get(LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
                    .await?
                {
                    Some(encoded) => decode_large_value_root_references(&encoded)?,
                    None => LargeValueRootReferences::default(),
                };
                let previous_total = references.durable.saturating_add(references.staged);
                let durable_delta = durable_root_deltas.get(root).copied().unwrap_or_default();
                references.durable = if durable_delta >= 0 {
                    references.durable.checked_add(durable_delta as u64)
                } else {
                    references.durable.checked_sub(durable_delta.unsigned_abs())
                }
                .ok_or_else(|| {
                    Error::InvalidLargeValueMetadata(
                        "durable root count overflow/underflow".to_owned(),
                    )
                })?;
                references.staged = references
                    .staged
                    .checked_sub(accepted_roots.get(root).copied().unwrap_or_default())
                    .ok_or_else(|| {
                        Error::InvalidLargeValueMetadata("staged root count underflow".to_owned())
                    })?;
                let next_total = references.durable.saturating_add(references.staged);
                if previous_total == 0 && next_total > 0 && !references.node_active {
                    if storage
                        .get(
                            LARGE_VALUE_METADATA_CF.to_owned(),
                            large_value_node_key(root)?,
                        )
                        .await?
                        .is_some()
                    {
                        references.node_active = true;
                        node_transitions.push((root.clone(), 1));
                    }
                } else if previous_total > 0 && next_total == 0 && references.node_active {
                    references.node_active = false;
                    node_transitions.push((root.clone(), -1));
                }
                lifecycle_operations.push(OwnedWriteOperation::Set {
                    cf: LARGE_VALUE_METADATA_CF.to_owned(),
                    key,
                    value: encode_large_value_root_references(&references)?,
                });
            }
            lifecycle_operations.extend(
                large_value_node_transition_operations(
                    storage.as_ref(),
                    BTreeMap::new(),
                    node_transitions,
                    false,
                )
                .await?,
            );
            staged_state.borrow_mut().extend(lifecycle_operations);
        }
        // Every fallible/cancellable operation is complete. Allocate the id,
        // bind buffered notifications, and register the publication without an
        // intervening await.
        let publication = PublicationId(self.next_publication_id);
        self.next_publication_id = self.next_publication_id.saturating_add(1);
        let tick = self
            .ivm_runtime
            .assign_resident_publication(resident_tick, publication);
        let staged_operations = staged_state.borrow().clone().into_operations();

        self.resident_writes
            .borrow_mut()
            .extend(staged_operations.iter().cloned());
        self.resident_publications
            .insert(publication, Rc::clone(&staged_state));
        if !roots.is_empty() {
            if let Some(guard) = lifecycle_guard {
                self.large_value_publication_lifecycle_guard = Some(guard);
                self.large_value_lifecycle_held.set(true);
            }
            self.large_value_lifecycle_publications.insert(publication);
        }
        Ok(AppliedBatch {
            publication,
            storage: Rc::clone(&self.storage),
            operations: staged_state,
            resident_install_durable: Some(resident_install_durable),
            order: Rc::clone(&self.publication_persistence),
            ivm_tick_time,
            tick,
            notifications_deferred: defer_notifications_until_durable,
            lifecycle: Rc::new(Cell::new(AppliedBatchLifecycle::Applied)),
            abandoned_application: Rc::clone(&self.abandoned_application),
        })
    }

    /// Install one persistence result and advance only the contiguous durable
    /// publication frontier.
    pub fn finish_persistence(
        &mut self,
        persistence: PersistedBatch,
    ) -> Result<PublicationId, Error> {
        if !Rc::ptr_eq(&self.publication_persistence, &persistence.receipt.order) {
            return Err(Error::PublicationNotFound(persistence.publication));
        }
        persistence.receipt.finish();
        self.last_tick_metrics = Some(persistence.metrics.tick.clone());
        self.last_commit_metrics = Some(persistence.metrics.clone());
        if let Err(error) = persistence.result {
            if persistence.notifications_deferred {
                self.ivm_runtime
                    .discard_deferred_notifications(persistence.publication);
            }
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
            self.large_value_lifecycle_publications
                .remove(&PublicationId(frontier));
            self.durable_publication_frontier = Some(PublicationId(frontier));
            frontier = frontier.saturating_add(1);
        }
        if self.large_value_lifecycle_publications.is_empty() {
            let guard = self.large_value_publication_lifecycle_guard.take();
            self.large_value_lifecycle_held.set(false);
            drop(guard);
        }
        self.refresh_resident_writes();
        if persistence.notifications_deferred {
            self.ivm_runtime
                .settle_deferred_notifications(persistence.publication);
        }
        Ok(persistence.publication)
    }

    pub(super) fn pending_writes_from_batch(
        &self,
        batch: DatabaseBatch,
    ) -> Result<Vec<PendingTableWrite>, Error> {
        let mut pending_writes = Vec::with_capacity(batch.operations.len());
        for operation in batch.operations {
            pending_writes.push(self.pending_write_from_owned_operation(operation)?);
        }
        Ok(pending_writes)
    }

    pub(super) fn refresh_resident_writes(&mut self) {
        let mut resident_writes = StagedWriteState::default();
        for operations in self.resident_publications.values() {
            resident_writes.extend(operations.borrow().clone().into_operations());
        }
        *self.resident_writes.borrow_mut() = resident_writes;
    }

    #[cfg(any(test, feature = "test"))]
    #[allow(dead_code)] // Used by the lib-test batch overlay helper, not non-test builds.
    pub(super) fn pending_writes_from_operations(
        &self,
        operations: &[BatchOperation],
    ) -> Result<Vec<PendingTableWrite>, Error> {
        operations
            .iter()
            .map(|operation| self.pending_write_from_operation(operation))
            .collect()
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
                let descriptor = self.record_descriptor_for_input(table, record)?;
                let (variant_tag, descriptor, record) =
                    resolve_record_input(table_schema, record, descriptor)?;
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
                let descriptor = self.record_descriptor_for_raw_input(table, record)?;
                let (variant_tag, descriptor, record) =
                    resolve_raw_record_input(table_schema, record, descriptor)?;
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
                let descriptor = self.record_descriptor_for_raw_input(table, record)?;
                let (variant_tag, descriptor, record) =
                    resolve_raw_record_input(table_schema, record, descriptor)?;
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
                let descriptor = self.record_descriptor_for_input(table, record)?;
                let (variant_tag, descriptor, record) =
                    resolve_record_input(table_schema, record, descriptor)?;
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
                let descriptor = self.record_descriptor_for_raw_input(table, record)?;
                let (variant_tag, descriptor, record) =
                    resolve_raw_record_input(table_schema, record, descriptor)?;
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

    fn pending_write_from_owned_operation(
        &self,
        operation: BatchOperation,
    ) -> Result<PendingTableWrite, Error> {
        match operation {
            BatchOperation::Insert { table, record } => {
                let table_schema = self.table(&table)?;
                let descriptor = self.record_descriptor_for_input(&table, &record)?;
                let (variant_tag, descriptor, record) =
                    resolve_owned_record_input(table_schema, record, descriptor)?;
                let key = primary_key_bytes(table_schema, variant_tag, descriptor, &record)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Insert,
                    table,
                    key,
                    variant_tag,
                    descriptor,
                    record,
                })
            }
            BatchOperation::InsertRaw { table, key, record } => {
                let table_schema = self.table(&table)?;
                let descriptor = self.record_descriptor_for_raw_input(&table, &record)?;
                let (variant_tag, descriptor, record) =
                    resolve_owned_raw_record_input(table_schema, record, descriptor)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Insert,
                    table,
                    key: key.into_bytes(),
                    variant_tag,
                    descriptor,
                    record,
                })
            }
            BatchOperation::InsertRawFresh { table, key, record } => {
                let table_schema = self.table(&table)?;
                let descriptor = self.record_descriptor_for_raw_input(&table, &record)?;
                let (variant_tag, descriptor, record) =
                    resolve_owned_raw_record_input(table_schema, record, descriptor)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::InsertFresh,
                    table,
                    key: key.into_bytes(),
                    variant_tag,
                    descriptor,
                    record,
                })
            }
            BatchOperation::Update { table, record } => {
                let table_schema = self.table(&table)?;
                let descriptor = self.record_descriptor_for_input(&table, &record)?;
                let (variant_tag, descriptor, record) =
                    resolve_owned_record_input(table_schema, record, descriptor)?;
                let key = primary_key_bytes(table_schema, variant_tag, descriptor, &record)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Update,
                    table,
                    key,
                    variant_tag,
                    descriptor,
                    record,
                })
            }
            BatchOperation::UpdateRaw { table, key, record } => {
                let table_schema = self.table(&table)?;
                let descriptor = self.record_descriptor_for_raw_input(&table, &record)?;
                let (variant_tag, descriptor, record) =
                    resolve_owned_raw_record_input(table_schema, record, descriptor)?;
                Ok(PendingTableWrite::Set {
                    mode: WriteMode::Update,
                    table,
                    key: key.into_bytes(),
                    variant_tag,
                    descriptor,
                    record,
                })
            }
            BatchOperation::Delete { table, key } => {
                let descriptor = self.table(&table)?.record_schema();
                Ok(PendingTableWrite::Delete {
                    table,
                    key: key.into_bytes(),
                    descriptor,
                })
            }
        }
    }

    fn record_descriptor_for_input(
        &self,
        table: &str,
        record: &RecordInput,
    ) -> Result<RecordDescriptor, Error> {
        let variant_tag = match record {
            RecordInput::Values(_) => 0,
            RecordInput::Record(record) => record.variant_tag(),
        };
        self.record_descriptor(table, variant_tag)
    }

    fn record_descriptor_for_raw_input(
        &self,
        table: &str,
        record: &RawRecordInput,
    ) -> Result<RecordDescriptor, Error> {
        let variant_tag = match record {
            RawRecordInput::Payload(_) => 0,
            RawRecordInput::Record(record) => record.variant_tag(),
            RawRecordInput::ValidatedRecord(record) => record.variant_tag(),
        };
        self.record_descriptor(table, variant_tag)
    }

    fn record_descriptor(&self, table: &str, variant_tag: u32) -> Result<RecordDescriptor, Error> {
        if let Some(descriptor) = self
            .ivm_runtime
            .record_descriptor(table, variant_tag)
            .copied()
        {
            return Ok(descriptor);
        }
        self.table(table)?
            .record_schema_for_variant(variant_tag)
            .ok_or_else(|| Error::UnknownTableVariant {
                table: table.to_owned(),
                version: u64::from(variant_tag),
            })
    }

    pub(super) fn table_storage_descriptor(&self, table: &str) -> Result<RecordDescriptor, Error> {
        self.ivm_runtime
            .table_storage_descriptor(table)
            .copied()
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    pub(super) fn table(&self, table: &str) -> Result<&TableSchema, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .table(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    pub(super) fn ensure_not_poisoned(&self) -> Result<(), Error> {
        if self.poisoned || self.abandoned_application.get() {
            Err(Error::DatabasePoisoned)
        } else {
            Ok(())
        }
    }
}

fn value_contains_large_ref(value: &Value, expected: &crate::large_values::LargeValueRef) -> bool {
    match value {
        Value::Large(value_ref) => value_ref == expected,
        Value::Tuple(values) | Value::Array(values) => values
            .iter()
            .any(|value| value_contains_large_ref(value, expected)),
        Value::Nullable(Some(value)) => value_contains_large_ref(value, expected),
        Value::Record(record) => record.to_values().is_ok_and(|values| {
            values
                .iter()
                .any(|value| value_contains_large_ref(value, expected))
        }),
        Value::Enum(value) => value.record().to_values().is_ok_and(|values| {
            values
                .iter()
                .any(|value| value_contains_large_ref(value, expected))
        }),
        _ => false,
    }
}

fn collect_large_root_deltas(
    value: &Value,
    weight: i64,
    deltas: &mut BTreeMap<crate::large_values::NodeRef, i64>,
) {
    match value {
        Value::Large(value_ref) => {
            *deltas.entry(value_ref.root.clone()).or_default() += weight;
        }
        Value::Tuple(values) | Value::Array(values) => {
            for value in values {
                collect_large_root_deltas(value, weight, deltas);
            }
        }
        Value::Nullable(Some(value)) => collect_large_root_deltas(value, weight, deltas),
        Value::Record(record) => {
            if let Ok(values) = record.to_values() {
                for value in values {
                    collect_large_root_deltas(&value, weight, deltas);
                }
            }
        }
        Value::Enum(value) => {
            if let Ok(values) = value.record().to_values() {
                for value in values {
                    collect_large_root_deltas(&value, weight, deltas);
                }
            }
        }
        _ => {}
    }
}
