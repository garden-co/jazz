use super::*;

impl Database {
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
    pub async fn new<S>(schema: DatabaseSchema, storage: S) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        Self::new_with_storage_layout(schema, storage, StorageLayout::Identity).await
    }

    pub async fn new_with_storage_layout<S>(
        schema: DatabaseSchema,
        storage: S,
        storage_layout: StorageLayout,
    ) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        validate_durable_key_schema(&schema)?;
        let ivm_runtime = IvmRuntime::new(schema)?;
        Ok(Self {
            storage: Rc::new(LayoutStorage::new(storage, storage_layout).await?),
            ivm_runtime,
            last_commit_metrics: None,
            last_tick_metrics: None,
            storage_read_metrics: Rc::new(RefCell::new(StorageReadMetrics::default())),
            next_publication_id: 1,
            durable_publication_frontier: None,
            resident_publications: BTreeMap::new(),
            persisted_publications: BTreeSet::new(),
            resident_writes: Rc::new(RefCell::new(StagedWriteState::default())),
            publication_persistence: Rc::new(RefCell::new(PersistenceOrder {
                next: 1,
                waiters: BTreeMap::new(),
                failure: None,
            })),
            abandoned_application: Rc::new(Cell::new(false)),
            poisoned: false,
        })
    }

    pub fn durable_publication_frontier(&self) -> Option<PublicationId> {
        self.durable_publication_frontier
    }

    pub(super) fn resident_storage(&self) -> StagedWriteOverlay<'_, LayoutStorage> {
        StagedWriteOverlay::new(&self.storage, &self.resident_writes)
    }

    /// Reject any host operation after an ambiguous durable finalization.
    #[doc(hidden)]
    pub fn ensure_usable(&self) -> Result<(), Error> {
        self.ensure_not_poisoned()
    }

    /// Return approximate live bytes for one backing class/column family when
    /// the storage backend exposes that optional capability.
    pub async fn approximate_class_bytes(&self, cf: &str) -> Result<Option<u64>, Error> {
        Ok(self.storage.approximate_class_bytes(cf.to_owned()).await?)
    }

    pub fn into_storage(self) -> BoxedStorage {
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

impl Database {
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

    /// Test helper whose reads observe writes already added to the batch.
    #[cfg(test)]
    pub(crate) fn open_staged_batch(&mut self) -> StagedDatabaseBatch<'_> {
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
    pub fn direct_record_store(&self, name: &str) -> Result<DirectRecordStore<'_>, Error> {
        let schema = self.direct_record_store_schema(name)?;
        Ok(DirectRecordStore {
            storage: &self.storage,
            name: schema.name.clone(),
            key: schema.key_descriptor(),
            value: schema.value_descriptor(),
        })
    }
}
