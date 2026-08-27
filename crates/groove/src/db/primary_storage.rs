use super::*;

impl Database {
    /// Return decoded records whose explicit schema index exactly matches the
    /// supplied index-column key.
    ///
    /// The read observes all applied resident batches, including publications
    /// awaiting persistence. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub async fn index_get(
        &self,
        table: &str,
        index_name: &str,
        key: &[Value],
    ) -> Result<Vec<VariantRecord>, Error> {
        let index = self.index(table, index_name)?;
        if key.len() != index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: key.len(),
            });
        }
        self.index_scan(table, index_name, key).await
    }

    /// Return decoded records whose explicit schema index starts with the
    /// supplied index-column prefix, in persisted index-key order.
    ///
    /// The read observes all applied resident batches, including publications
    /// awaiting persistence. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
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
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # batch.insert("albums", vec![Value::U64(2), Value::String("Blue Train".into()), Value::U64(1957)]);
    /// # let applied = database.apply_batch(batch).await?;
    /// # let persisted = applied.persist().await;
    /// # database.finish_persistence(persisted)?;
    /// let rows = database.index_scan("albums", "albums_by_year", &[Value::U64(1959)]).await?;
    ///
    /// assert_eq!(rows.len(), 1);
    /// assert_eq!(rows[0].get("title")?, Value::String("Kind of Blue".into()));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn index_scan(
        &self,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<VariantRecord>, Error> {
        let index = self.index(table, index_name)?;
        if prefix.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: prefix.len(),
            });
        }
        let raw_entries = self.index_scan_raw(table, index_name, prefix).await?;
        self.decode_index_records(table, index_name, raw_entries)
    }

    /// Return decoded records whose explicit schema index is in the supplied
    /// logical index-key range.
    ///
    /// The lower bound is inclusive. The upper bound is exclusive at the
    /// logical-key level and includes non-unique primary-key suffixes for that
    /// logical prefix.
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
    /// # let mut batch = database.open_batch();
    /// # batch.insert("albums", vec![Value::U64(1), Value::String("Kind of Blue".into()), Value::U64(1959)]);
    /// # batch.insert("albums", vec![Value::U64(2), Value::String("Blue Train".into()), Value::U64(1957)]);
    /// # batch.insert("albums", vec![Value::U64(3), Value::String("A Love Supreme".into()), Value::U64(1965)]);
    /// # let applied = database.apply_batch(batch).await?;
    /// # let persisted = applied.persist().await;
    /// # database.finish_persistence(persisted)?;
    /// let rows = database.index_scan_range(
    ///     "albums",
    ///     "albums_by_year",
    ///     &[Value::U64(1957)],
    ///     &[Value::U64(1960)],
    /// ).await?;
    ///
    /// let titles = rows
    ///     .iter()
    ///     .map(|row| row.get("title"))
    ///     .collect::<Result<Vec<_>, _>>()?;
    /// assert_eq!(
    ///     titles,
    ///     vec![Value::String("Blue Train".into()), Value::String("Kind of Blue".into())]
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn index_scan_range(
        &self,
        table: &str,
        index_name: &str,
        start: &[Value],
        end: &[Value],
    ) -> Result<Vec<VariantRecord>, Error> {
        let index = self.index(table, index_name)?;
        if start.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: start.len(),
            });
        }
        if end.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: end.len(),
            });
        }
        let raw_entries = self
            .index_scan_range_raw(table, index_name, start, end)
            .await?;
        self.decode_index_records(table, index_name, raw_entries)
    }

    pub(super) fn decode_index_records(
        &self,
        table: &str,
        index_name: &str,
        raw_entries: Vec<EncodedKeyValue<'_>>,
    ) -> Result<Vec<VariantRecord>, Error> {
        self.table(table)?;
        let _ = index_name;
        Ok(raw_entries
            .into_iter()
            .map(|entry| entry.into_variant_parts().1)
            .collect())
    }

    /// Return decoded records whose primary key starts with the supplied key
    /// prefix, in primary-key order.
    ///
    /// The read observes all applied resident batches, including publications
    /// awaiting persistence. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub async fn primary_key_scan(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<VariantRecord>, Error> {
        let resident = self.resident_storage();
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        self.primary_key_scan_with_storage(&storage, table, prefix)
            .await
    }

    pub(super) async fn primary_key_scan_with_storage<T>(
        &self,
        storage: &T,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<VariantRecord>, Error>
    where
        T: OrderedKvStorage,
    {
        let raw = self
            .primary_key_scan_raw_with_storage(storage, table, prefix)
            .await?;
        Ok(raw
            .into_iter()
            .map(|entry| entry.into_variant_parts().1)
            .collect())
    }

    /// Return encoded records whose primary key starts with the supplied key
    /// prefix, in primary-key order.
    ///
    /// The read observes all applied resident batches, including publications
    /// awaiting persistence. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub async fn primary_key_scan_raw(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let resident = self.resident_storage();
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        self.primary_key_scan_raw_with_storage(&storage, table, prefix)
            .await
    }

    /// Return encoded primary-key records while also observing writes already
    /// staged in `batch`.
    pub async fn primary_key_scan_raw_in_batch(
        &self,
        batch: &DatabaseBatch,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        self.ensure_batch_storage_txn(batch)?;
        let resident = self.resident_storage();
        let overlay = StagedWriteOverlay::new(&resident, &batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.primary_key_scan_raw_with_storage(&storage, table, prefix)
            .await
    }

    /// Return one encoded record by its full primary key.
    ///
    /// This is the point-read counterpart to [`Self::primary_key_scan_raw`].
    /// `key` must provide every primary-key column; callers that need a prefix
    /// or range must use the scan APIs.
    pub async fn primary_key_get_raw(
        &self,
        table: &str,
        key: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        let resident = self.resident_storage();
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        self.primary_key_get_raw_with_storage(&storage, table, key)
            .await
    }

    /// Return one schema-bound record by its complete primary key.
    pub async fn primary_key_get(
        &self,
        table: &str,
        key: &[Value],
    ) -> Result<Option<VariantRecord>, Error> {
        Ok(self
            .primary_key_get_raw(table, key)
            .await?
            .map(|entry| entry.into_variant_parts().1))
    }

    /// Return one encoded primary-key record while also observing writes
    /// already staged in `batch`.
    pub async fn primary_key_get_raw_in_batch(
        &self,
        batch: &DatabaseBatch,
        table: &str,
        key: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        self.ensure_batch_storage_txn(batch)?;
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if key.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: key.len(),
            });
        }
        let descriptor = self.table_storage_descriptor(table)?;
        let mut encoded_key = Vec::new();
        for (value, column) in key.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut encoded_key, value)?;
        }
        let staged_contains_key = batch
            .txn_operations
            .borrow_mut()
            .contains_key(table, &encoded_key);
        if !staged_contains_key {
            let resident = self.resident_storage();
            let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
            let key_descriptor = primary_key_descriptor(primary_key);
            let store = record_store_for_table(&storage, table, Some(key_descriptor), &descriptor);
            return store
                .get_raw(&encoded_key)
                .await?
                .map(|value| self.decode_stored_key_value(table_schema, encoded_key, value))
                .transpose();
        }

        let resident = self.resident_storage();
        let overlay = StagedWriteOverlay::new(&resident, &batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(&storage, table, Some(key_descriptor), &descriptor);
        store
            .get_raw(&encoded_key)
            .await?
            .map(|value| self.decode_stored_key_value(table_schema, encoded_key, value))
            .transpose()
    }

    pub(super) async fn primary_key_get_raw_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        key: &[Value],
    ) -> Result<Option<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if key.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: key.len(),
            });
        }
        let descriptor = self.table_storage_descriptor(table)?;
        let mut encoded_key = Vec::new();
        for (value, column) in key.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut encoded_key, value)?;
        }
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(storage, table, Some(key_descriptor), &descriptor);
        store
            .get_raw(&encoded_key)
            .await?
            .map(|value| self.decode_stored_key_value(table_schema, encoded_key, value))
            .transpose()
    }

    pub(super) async fn primary_key_scan_raw_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if prefix.len() > primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: prefix.len(),
            });
        }
        let descriptor = self.table_storage_descriptor(table)?;
        let mut key_prefix = Vec::new();
        for (value, column) in prefix.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut key_prefix, value)?;
        }
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(storage, table, Some(key_descriptor), &descriptor);
        store
            .prefix(&key_prefix)
            .await?
            .into_iter()
            .map(|(key, value)| self.decode_stored_key_value(table_schema, key, value))
            .collect()
    }

    pub(super) async fn primary_key_last_raw_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if prefix.len() > primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: prefix.len(),
            });
        }
        let descriptor = self.table_storage_descriptor(table)?;
        let mut key_prefix = Vec::new();
        for (value, column) in prefix.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut key_prefix, value)?;
        }
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(storage, table, Some(key_descriptor), &descriptor);
        store
            .last_with_prefix(&key_prefix)
            .await?
            .map(|(key, value)| self.decode_stored_key_value(table_schema, key, value))
            .transpose()
    }

    /// Return encoded records for an explicit primary-key logical range.
    ///
    /// The lower bound is inclusive. The upper bound is exclusive. Bounds must
    /// provide the full primary key.
    pub async fn primary_key_scan_range_raw(
        &self,
        table: &str,
        start: &[Value],
        end: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if start.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: start.len(),
            });
        }
        if end.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: end.len(),
            });
        }
        let descriptor = self.table_storage_descriptor(table)?;
        let mut start_key = Vec::new();
        for (value, column) in start.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut start_key, value)?;
        }
        let mut end_key = Vec::new();
        for (value, column) in end.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut end_key, value)?;
        }
        let resident = self.resident_storage();
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(&storage, table, Some(key_descriptor), &descriptor);
        store
            .range(&start_key, &end_key)
            .await?
            .into_iter()
            .map(|(key, value)| self.decode_stored_key_value(table_schema, key, value))
            .collect()
    }

    /// Return the last encoded record whose primary key starts with the
    /// supplied key prefix.
    ///
    /// The read observes all applied resident batches, including publications
    /// awaiting persistence. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub async fn primary_key_last_raw(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        let resident = self.resident_storage();
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        self.primary_key_last_raw_with_storage(&storage, table, prefix)
            .await
    }

    /// Return the last encoded primary-key record while also observing writes
    /// already staged in `batch`.
    pub async fn primary_key_last_raw_in_batch(
        &self,
        batch: &DatabaseBatch,
        table: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        self.ensure_batch_storage_txn(batch)?;
        let resident = self.resident_storage();
        let overlay = StagedWriteOverlay::new(&resident, &batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.primary_key_last_raw_with_storage(&storage, table, prefix)
            .await
    }

    /// Return the last encoded record whose primary key starts with `prefix`
    /// and whose full primary key is less than or equal to `upper`.
    ///
    /// `upper` must provide the full primary key. The read observes all
    /// applied resident batches, including publications awaiting persistence.
    /// Reads while the caller still holds an uncommitted
    /// [`DatabaseBatch`] observe the pre-batch state.
    pub async fn primary_key_last_before_or_at_raw(
        &self,
        table: &str,
        prefix: &[Value],
        upper: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        let table_schema = self.table(table)?;
        let primary_key = table_schema
            .primary_key
            .as_ref()
            .ok_or_else(|| Error::MissingPrimaryKey(table.to_owned()))?;
        if prefix.len() > primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: prefix.len(),
            });
        }
        if upper.len() != primary_key.columns.len() {
            return Err(Error::PrimaryKeyArity {
                table: table.to_owned(),
                expected: primary_key.columns.len(),
                actual: upper.len(),
            });
        }
        let descriptor = self.table_storage_descriptor(table)?;
        let mut key_prefix = Vec::new();
        for (value, column) in prefix.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut key_prefix, value)?;
        }
        let mut upper_key = Vec::new();
        for (value, column) in upper.iter().zip(&primary_key.columns) {
            ensure_primary_key_value_type(table_schema, column, value)?;
            encode_primary_key_part(&mut upper_key, value)?;
        }
        if !upper_key.starts_with(&key_prefix) {
            return Ok(None);
        }
        let resident = self.resident_storage();
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        let key_descriptor = primary_key_descriptor(primary_key);
        let store = record_store_for_table(&storage, table, Some(key_descriptor), &descriptor);
        store
            .last_with_prefix_before_or_at(&key_prefix, &upper_key)
            .await?
            .map(|(key, value)| self.decode_stored_key_value(table_schema, key, value))
            .transpose()
    }

    /// Return encoded records whose explicit schema index exactly matches the
    /// supplied index-column key.
    ///
    /// The read observes all applied resident batches, including publications
    /// awaiting persistence. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub async fn index_get_raw(
        &self,
        table: &str,
        index_name: &str,
        key: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let index = self.index(table, index_name)?;
        if key.len() != index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: key.len(),
            });
        }
        self.index_scan_raw(table, index_name, key).await
    }

    /// Return encoded records whose explicit schema index starts with the
    /// supplied index-column prefix.
    ///
    /// The read observes all applied resident batches, including publications
    /// awaiting persistence. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub async fn index_scan_raw(
        &self,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let resident = self.resident_storage();
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        self.index_scan_raw_with_storage(&storage, table, index_name, prefix)
            .await
    }

    /// Return encoded index-probe records while also observing writes already
    /// staged in `batch`.
    pub async fn index_scan_raw_in_batch(
        &self,
        batch: &DatabaseBatch,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        self.ensure_batch_storage_txn(batch)?;
        let resident = self.resident_storage();
        let overlay = StagedWriteOverlay::new(&resident, &batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.storage_read_metrics);
        self.index_scan_raw_with_storage(&storage, table, index_name, prefix)
            .await
    }

    pub(super) async fn index_scan_raw_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let index = self.index(table, index_name)?;
        if prefix.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: prefix.len(),
            });
        }
        let storage_prefix = self.persisted_index_scan_prefix(table, index_name, prefix)?;
        let index_descriptor = index_record_descriptor();
        let raw_entries = self
            .durable_indices_store_with_storage(storage, &index_descriptor)
            .prefix(&storage_prefix)
            .await?;
        self.decode_raw_index_entries_with_storage(storage, table, index_name, raw_entries)
            .await
    }

    /// Return the last encoded record whose explicit schema index starts with
    /// the supplied index-column prefix.
    pub async fn index_last_raw(
        &self,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        let index = self.index(table, index_name)?;
        if prefix.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: prefix.len(),
            });
        }
        let storage_prefix = self.persisted_index_scan_prefix(table, index_name, prefix)?;
        let index_descriptor = index_record_descriptor();
        let resident = self.resident_storage();
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        let Some(raw_entry) = self
            .durable_indices_store_with_storage(&storage, &index_descriptor)
            .last_with_prefix(&storage_prefix)
            .await?
        else {
            return Ok(None);
        };
        Ok(self
            .decode_raw_index_entries_with_storage(&storage, table, index_name, vec![raw_entry])
            .await?
            .into_iter()
            .next())
    }

    /// Return encoded records for an explicit schema index logical-key range.
    ///
    /// The read observes all applied resident batches, including publications
    /// awaiting persistence. Reads while the caller still
    /// holds an uncommitted [`DatabaseBatch`] observe the pre-batch state.
    pub async fn index_scan_range_raw(
        &self,
        table: &str,
        index_name: &str,
        start: &[Value],
        end: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        let index = self.index(table, index_name)?;
        if start.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: start.len(),
            });
        }
        if end.len() > index.columns.len() {
            return Err(Error::IndexKeyArity {
                index: index_name.to_owned(),
                expected: index.columns.len(),
                actual: end.len(),
            });
        }
        let start = self.persisted_index_scan_prefix(table, index_name, start)?;
        let end = self.persisted_index_scan_prefix(table, index_name, end)?;
        let index_descriptor = index_record_descriptor();
        let resident = self.resident_storage();
        let storage = MeteredStorage::new(&resident, &self.storage_read_metrics);
        let raw_entries = self
            .durable_indices_store_with_storage(&storage, &index_descriptor)
            .range(&start, &end)
            .await?;
        self.decode_raw_index_entries_with_storage(&storage, table, index_name, raw_entries)
            .await
    }

    pub(super) async fn decode_raw_index_entries_with_storage<'a, T>(
        &'a self,
        storage: &T,
        table: &str,
        index_name: &str,
        raw_entries: Vec<crate::storage::KeyValue>,
    ) -> Result<Vec<EncodedKeyValue<'a>>, Error>
    where
        T: OrderedKvStorage,
    {
        let table_schema = self.table(table)?;
        let storage_descriptor = self.table_storage_descriptor(table)?;
        let key_descriptor = table_schema
            .primary_key
            .as_ref()
            .map(primary_key_descriptor);
        let store = record_store_for_table(storage, table, key_descriptor, &storage_descriptor);
        let index_descriptor = index_record_descriptor();
        let mut records = Vec::new();
        for (storage_key, persisted_record) in raw_entries {
            let index_record = index_descriptor.bind(&persisted_record);
            let primary_key = persisted_index_primary_key(
                table_schema,
                index_name,
                self.index(table, index_name)?,
                &storage_key,
                &index_record.get("value")?,
            )?;
            if let Some(record) = store.get_raw(&primary_key).await? {
                records.push(self.decode_stored_key_value(table_schema, primary_key, record)?);
            } else if table_schema.primary_key.is_some() {
                return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
            }
        }
        Ok(records)
    }

    pub(super) fn index(
        &self,
        table: &str,
        index_name: &str,
    ) -> Result<&crate::schema::IndexSchema, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .index(table, index_name)
            .ok_or_else(|| Error::IndexNotFound {
                table: table.to_owned(),
                index: index_name.to_owned(),
            })
    }

    pub(super) fn direct_record_store_schema(
        &self,
        store: &str,
    ) -> Result<&DirectRecordStoreSchema, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .direct_record_store(store)
            .ok_or_else(|| Error::DirectRecordStoreNotFound(store.to_owned()))
    }

    pub(super) fn persisted_index_scan_prefix(
        &self,
        table: &str,
        index_name: &str,
        prefix: &[Value],
    ) -> Result<Vec<u8>, Error> {
        let table_schema = self.table(table)?;
        let index = self.index(table, index_name)?;
        let mut logical_key = Vec::new();
        for (value, column_name) in prefix.iter().zip(&index.columns) {
            let column = table_schema
                .columns
                .iter()
                .find(|column| column.name == *column_name)
                .ok_or_else(|| {
                    Error::InvalidPersistedIndex(format!(
                        "index {index_name} references unknown column {column_name}"
                    ))
                })?;
            encode_index_prefix_part(&mut logical_key, value, &column.column_type)?;
        }
        let mut storage_prefix = durable_index_key_prefix(table, index_name);
        if !logical_key.is_empty() {
            // Persist stores IndexBy's logical bytes as a Value::Bytes key field.
            // For prefix scans we emit the Bytes tag and escaped payload bytes
            // without the terminal 00 00, so longer non-unique keys remain in range.
            storage_prefix.push(7);
            for byte in logical_key {
                if byte == 0 {
                    storage_prefix.extend([0, 0xff]);
                } else {
                    storage_prefix.push(byte);
                }
            }
        }
        Ok(storage_prefix)
    }
}
