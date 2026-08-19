use super::*;

impl<S> Database<S>
where
    S: OrderedKvStorage,
{
    /// Open a schema-aware database over an ordered key/value store.
    ///
    /// `Database::new` does not create storage column families itself. The
    /// caller supplies storage that already has the table/index families needed
    /// by the schema; [`crate::storage::MemoryStorage`] is convenient for tests
    /// and examples.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
    /// use groove::db::Database;
    /// use groove::schema::{
    ///     ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType,
    ///     PrimaryKey, TableSchema,
    /// };
    /// use groove::storage::MemoryStorage;
    ///
    /// let schema = DatabaseSchema::new([TableSchema::new(
    ///     "albums",
    ///     [
    ///         ColumnSchema::new("id", ColumnType::U64),
    ///         ColumnSchema::new("title", ColumnType::String),
    ///         ColumnSchema::new("year", ColumnType::U64),
    ///     ],
    /// )
    /// .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// let storage = MemoryStorage::new(&["albums", "indices"]);
    ///
    /// let database = Database::new(schema, storage).await?;
    /// assert!(database.last_commit_metrics().is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn new(schema: DatabaseSchema, storage: S) -> Result<Self, Error> {
        Self::new_with_storage_layout(schema, storage, StorageLayout::Identity).await
    }

    pub async fn new_with_storage_layout(
        schema: DatabaseSchema,
        storage: S,
        storage_layout: StorageLayout,
    ) -> Result<Self, Error> {
        validate_durable_key_schema(&schema)?;
        let ivm_runtime = IvmRuntime::new(schema)?;
        Ok(Self {
            storage: Rc::new(LayoutStorage::new(storage, storage_layout).await?),
            ivm_runtime,
            last_commit_metrics: None,
            last_tick_metrics: None,
            storage_read_metrics: Rc::new(RefCell::new(StorageReadMetrics::default())),
            durable_publication_state: Arc::new(Mutex::new(DurablePublicationState::default())),
            next_publication_id: 1,
            durable_publication_frontier: None,
            resident_publications: BTreeMap::new(),
            persisted_publications: BTreeSet::new(),
            resident_writes: Rc::new(RefCell::new(StagedWriteState::default())),
            publication_persistence: Rc::new(RefCell::new(PublicationPersistenceOrder {
                next: 1,
                waiters: BTreeMap::new(),
            })),
            poisoned: false,
        })
    }

    pub fn durable_publication_frontier(&self) -> Option<PublicationId> {
        self.durable_publication_frontier
    }

    /// Begin a host transaction whose subscription publication boundary spans
    /// one or more calls to [`Database::commit_batch`].
    ///
    /// Jazz uses this for durable finalization that writes canonical state and
    /// cleanup/consistency metadata in separate storage batches. Nested scopes
    /// are supported; only the outermost successful completion publishes.
    #[doc(hidden)]
    pub fn begin_durable_publication_scope(&mut self) -> Result<DurablePublicationScope, Error> {
        self.ensure_not_poisoned()?;
        let mut state = self
            .durable_publication_state
            .lock()
            .expect("durable publication state mutex poisoned");
        state.depth = state
            .depth
            .checked_add(1)
            .expect("durable publication scope depth exhausted");
        drop(state);
        Ok(DurablePublicationScope {
            state: Arc::clone(&self.durable_publication_state),
            resolved: false,
        })
    }

    /// Reject any host operation after an ambiguous durable finalization.
    #[doc(hidden)]
    pub fn ensure_usable(&self) -> Result<(), Error> {
        self.ensure_not_poisoned()
    }

    pub(super) fn settle_durable_publication_scopes(&mut self) {
        let state = self
            .durable_publication_state
            .lock()
            .expect("durable publication state mutex poisoned");
        let depth = state.depth;
        let aborted = state.aborted;
        let successful_commits = state.successful_commits;
        drop(state);
        if aborted && successful_commits != 0 {
            self.ivm_runtime.discard_staged_subscription_notifications();
            self.poisoned = true;
        } else if aborted {
            self.ivm_runtime.discard_staged_subscription_notifications();
        } else if depth == 0 {
            self.ivm_runtime.publish_staged_subscription_notifications();
        }
        if depth == 0 {
            let mut state = self
                .durable_publication_state
                .lock()
                .expect("durable publication state mutex poisoned");
            state.aborted = false;
            state.successful_commits = 0;
        }
    }

    /// Return approximate live bytes for one backing class/column family when
    /// the storage backend exposes that optional capability.
    pub async fn approximate_class_bytes(&self, cf: &str) -> Result<Option<u64>, Error> {
        Ok(self.storage.approximate_class_bytes(cf.to_owned()).await?)
    }

    pub fn into_storage(self) -> S {
        Rc::try_unwrap(self.storage)
            .unwrap_or_else(|_| panic!("database storage still has an outstanding operation"))
            .into_inner()
    }

    pub async fn close(&self) -> Result<(), Error> {
        Ok(self.storage.close().await?)
    }

    /// Configure explicit storage durability boundaries for future committed
    /// write batches.
    pub async fn set_write_flush_cadence(&self, every: usize) -> Result<(), Error> {
        Ok(self.storage.set_write_flush_cadence(every).await?)
    }

    /// Complete the current storage durability boundary.
    pub async fn flush_write_boundary(&self) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        Ok(self.storage.flush_write_boundary().await?)
    }

    pub fn set_auto_direct_family_enabled(&mut self, enabled: bool) {
        self.ivm_runtime.set_auto_direct_family_enabled(enabled);
    }
}

impl<S> Database<S>
where
    S: OrderedKvStorage,
{
    /// Include arrangement and recursive-state size walks in future tick metrics.
    ///
    /// The default is `false` because those walks are diagnostic-only and scale
    /// with retained runtime state rather than with the current commit.
    pub fn set_tick_runtime_stats_enabled(&mut self, enabled: bool) {
        self.ivm_runtime.set_tick_runtime_stats_enabled(enabled);
    }

    /// Compute full runtime stats on demand.
    pub fn runtime_stats(&self) -> RuntimeStats {
        self.ivm_runtime.stats()
    }

    pub(super) fn durable_indices_store_with_storage<'a, T>(
        &'a self,
        storage: &'a T,
        descriptor: &'a RecordDescriptor,
    ) -> RecordStore<'a, T>
    where
        T: OrderedKvStorage,
    {
        RecordStore::new(storage, "indices", descriptor)
    }

    pub fn open_batch(&self) -> DatabaseBatch {
        DatabaseBatch::default()
    }

    /// Open a staged batch whose reads observe writes already added to the
    /// batch. Committing the staged batch runs exactly one IVM tick and one
    /// storage write, just like [`Database::commit_batch`].
    pub fn open_staged_batch(&mut self) -> StagedDatabaseBatch<'_, S> {
        StagedDatabaseBatch {
            database: self,
            batch: DatabaseBatch::default(),
        }
    }

    /// Return a typed handle for a schema-declared direct record store.
    ///
    /// Direct stores use record encoding and order-preserving typed primary
    /// keys, but bypass table batches, index maintenance, query planning, and
    /// IVM ticks.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
    /// use groove::db::Database;
    /// use groove::records::{RecordDescriptor, Value, ValueType};
    /// use groove::schema::{DatabaseSchema, DirectRecordStoreSchema};
    /// use groove::storage::MemoryStorage;
    ///
    /// let schema = DatabaseSchema::new([]).with_direct_record_store(
    ///     DirectRecordStoreSchema::new(
    ///         "album_art",
    ///         RecordDescriptor::new([("album_id", ValueType::U64), ("side", ValueType::String)]),
    ///         RecordDescriptor::new([("bytes", ValueType::Bytes)]),
    ///     ),
    /// );
    /// let column_families = schema.column_families();
    /// let storage = MemoryStorage::new(&column_families);
    /// let database = Database::new(schema, storage).await?;
    ///
    /// let art = database.direct_record_store("album_art")?;
    /// art.set(
    ///     &[Value::U64(1), Value::String("front".into())],
    ///     &[Value::Bytes(b"front-cover-bytes".to_vec())],
    /// ).await?;
    ///
    /// let stored = art.get(&[Value::U64(1), Value::String("front".into())]).await?;
    /// assert_eq!(stored.unwrap().get("bytes")?, Value::Bytes(b"front-cover-bytes".to_vec()));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub fn direct_record_store(&self, name: &str) -> Result<DirectRecordStore<'_, S>, Error> {
        let schema = self.direct_record_store_schema(name)?;
        Ok(DirectRecordStore {
            storage: &self.storage,
            name: schema.name.clone(),
            key: schema.key_descriptor(),
            value: schema.value_descriptor(),
        })
    }
}
