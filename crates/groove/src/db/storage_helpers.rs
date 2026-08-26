use super::*;

/// Typed facade over one schema-declared direct record store.
///
/// ```
/// # futures::executor::block_on(async {
/// use groove::db::Database;
/// use groove::records::{RecordDescriptor, Value, ValueType};
/// use groove::schema::{DatabaseSchema, DirectRecordStoreSchema};
/// use groove::storage::MemoryStorage;
///
/// let schema = DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
///     "album_art",
///     RecordDescriptor::new([("album_id", ValueType::U64)]),
///     RecordDescriptor::new([("bytes", ValueType::Bytes)]),
/// ));
/// let column_families = schema.column_families();
/// let storage = MemoryStorage::new(&column_families);
/// let database = Database::new(schema, storage).await?;
/// let art = database.direct_record_store("album_art")?;
///
/// art.set(&[Value::U64(1)], &[Value::Bytes(b"front-cover-bytes".to_vec())]).await?;
/// assert_eq!(
///     art.get(&[Value::U64(1)]).await?.unwrap().get("bytes")?,
///     Value::Bytes(b"front-cover-bytes".to_vec())
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # }).unwrap();
/// ```
pub struct DirectRecordStore<'a> {
    pub(super) storage: &'a LayoutStorage,
    pub(super) name: String,
    pub(super) key: RecordDescriptor,
    pub(super) value: RecordDescriptor,
}

impl DirectRecordStore<'_> {
    /// Return the schema-declared column family name backing this direct store.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_descriptor(&self) -> &RecordDescriptor {
        &self.key
    }

    pub fn value_descriptor(&self) -> &RecordDescriptor {
        &self.value
    }

    pub async fn set(&self, key: &[Value], value: &[Value]) -> Result<(), Error> {
        let key = self.key_bytes(key)?;
        let record = self.value.create(value)?;
        self.storage
            .write_many(vec![OwnedWriteOperation::Set {
                cf: self.name.clone(),
                key,
                value: record,
            }])
            .await
            .map_err(Error::from)
    }

    pub async fn get(&self, key: &[Value]) -> Result<Option<Record<'_>>, Error> {
        let key = self.key_bytes(key)?;
        Ok(self
            .record_store()
            .get_raw(&key)
            .await?
            .map(|record| self.value.bind_owned(record)))
    }

    pub async fn delete(&self, key: &[Value]) -> Result<(), Error> {
        let key = self.key_bytes(key)?;
        self.storage
            .write_many(vec![OwnedWriteOperation::Delete {
                cf: self.name.clone(),
                key,
            }])
            .await
            .map_err(Error::from)
    }

    pub async fn range(&self, start: &[Value], end: &[Value]) -> Result<Vec<Record<'_>>, Error> {
        let start = self.key_prefix_bytes(start)?;
        let end = self.key_prefix_bytes(end)?;
        self.record_store()
            .range(&start, &end)
            .await?
            .into_iter()
            .map(|(_, value)| Ok(self.value.bind_owned(value)))
            .collect()
    }

    pub async fn range_entries(
        &self,
        start: &[Value],
        end: &[Value],
    ) -> Result<Vec<DirectRecordStoreEntry<'_>>, Error> {
        let start = self.key_prefix_bytes(start)?;
        let end = self.key_prefix_bytes(end)?;
        self.record_store()
            .range(&start, &end)
            .await?
            .into_iter()
            .map(|(key, value)| {
                Ok(DirectRecordStoreEntry {
                    key: self.decode_key(&key)?,
                    value: self.value.bind_owned(value),
                })
            })
            .collect()
    }

    pub async fn prefix(&self, prefix: &[Value]) -> Result<Vec<Record<'_>>, Error> {
        let prefix = self.key_prefix_bytes(prefix)?;
        self.record_store()
            .prefix(&prefix)
            .await?
            .into_iter()
            .map(|(_, value)| Ok(self.value.bind_owned(value)))
            .collect()
    }

    pub async fn prefix_entries(
        &self,
        prefix: &[Value],
    ) -> Result<Vec<DirectRecordStoreEntry<'_>>, Error> {
        let prefix = self.key_prefix_bytes(prefix)?;
        self.record_store()
            .prefix(&prefix)
            .await?
            .into_iter()
            .map(|(key, value)| {
                Ok(DirectRecordStoreEntry {
                    key: self.decode_key(&key)?,
                    value: self.value.bind_owned(value),
                })
            })
            .collect()
    }

    pub async fn write_many(&self, operations: &[DirectRecordStoreWrite]) -> Result<(), Error> {
        let mut encoded = Vec::with_capacity(operations.len());
        for operation in operations {
            match operation {
                DirectRecordStoreWrite::Set { key, value } => {
                    encoded.push(OwnedWriteOperation::Set {
                        cf: self.name.clone(),
                        key: self.key_bytes(key)?,
                        value: self.value.create(value)?,
                    });
                }
                DirectRecordStoreWrite::Delete { key } => {
                    encoded.push(OwnedWriteOperation::Delete {
                        cf: self.name.clone(),
                        key: self.key_bytes(key)?,
                    });
                }
            }
        }
        self.storage.write_many(encoded).await.map_err(Error::from)
    }

    pub(super) fn key_bytes(&self, values: &[Value]) -> Result<Vec<u8>, Error> {
        if values.len() != self.key.fields().len() {
            return Err(records::Error::ArityMismatch {
                expected: self.key.fields().len(),
                actual: values.len(),
            }
            .into());
        }
        self.key_prefix_bytes(values)
    }

    pub(super) fn key_prefix_bytes(&self, values: &[Value]) -> Result<Vec<u8>, Error> {
        if values.len() > self.key.fields().len() {
            return Err(records::Error::ArityMismatch {
                expected: self.key.fields().len(),
                actual: values.len(),
            }
            .into());
        }
        let prefix_descriptor =
            RecordDescriptor::new(self.key.fields().iter().take(values.len()).map(|field| {
                (
                    field.name.clone().expect("direct store fields are named"),
                    field.value_type.clone(),
                )
            }));
        let _ = prefix_descriptor.create(values)?;
        let mut bytes = Vec::new();
        for value in values {
            encode_primary_key_part(&mut bytes, value)?;
        }
        Ok(bytes)
    }

    pub(super) fn record_store(&self) -> RecordStore<'_, LayoutStorage> {
        RecordStore::new(self.storage, &self.name, &self.value)
    }

    pub(super) fn decode_key(&self, key: &[u8]) -> Result<Vec<Value>, Error> {
        let mut remaining = key;
        let mut values = Vec::with_capacity(self.key.fields().len());
        for field in self.key.fields() {
            values.push(decode_primary_key_part(&mut remaining, &field.value_type)?);
        }
        if !remaining.is_empty() {
            return Err(Error::InvalidDirectRecordStoreKey(self.name.clone()));
        }
        Ok(values)
    }
}

pub struct DirectRecordStoreEntry<'a> {
    pub key: Vec<Value>,
    pub value: Record<'a>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DirectRecordStoreWrite {
    Set { key: Vec<Value>, value: Vec<Value> },
    Delete { key: Vec<Value> },
}

/// Prepared parameterized subscription shape produced from a SQL-ish query.
#[derive(Clone, Debug)]
pub struct PreparedShape {
    pub(super) id: PreparedShapeId,
    pub(super) parameters: Vec<QueryParameter>,
    pub(super) output: RecordDescriptor,
}

impl PreparedShape {
    pub fn id(&self) -> PreparedShapeId {
        self.id
    }

    pub fn parameters(&self) -> &[QueryParameter] {
        &self.parameters
    }

    pub fn output(&self) -> &RecordDescriptor {
        &self.output
    }
}

/// Timing and write-size split for the most recent committed batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitMetrics {
    pub storage_write_time: Duration,
    pub ivm_tick_time: Duration,
    pub storage_write_count: usize,
    pub storage_write_bytes: usize,
    pub storage_writes: StorageWriteMetrics,
    pub tick: TickMetrics,
}

/// Durable storage-write counts split by stable Jazz logical destinations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageWriteMetrics {
    pub total: StorageWriteBucket,
    pub history_rows: StorageWriteBucket,
    pub history_indexes: StorageWriteBucket,
    pub global_current_rows: StorageWriteBucket,
    pub global_current_indexes: StorageWriteBucket,
    pub register_global_current_rows: StorageWriteBucket,
    pub global_changes_rows: StorageWriteBucket,
    pub global_changes_indexes: StorageWriteBucket,
    pub transactions_rows: StorageWriteBucket,
    pub transactions_indexes: StorageWriteBucket,
    pub other: StorageWriteBucket,
}

impl StorageWriteMetrics {
    pub(super) fn from_operations(operations: &[crate::storage::WriteOperation<'_>]) -> Self {
        let mut metrics = Self::default();
        for operation in operations {
            metrics.record(operation);
        }
        metrics
    }

    pub(super) fn record(&mut self, operation: &crate::storage::WriteOperation<'_>) {
        let bytes = write_operation_bytes(operation);
        self.total.record(bytes);
        match storage_write_destination(operation) {
            StorageWriteDestination::HistoryRows => self.history_rows.record(bytes),
            StorageWriteDestination::HistoryIndexes => self.history_indexes.record(bytes),
            StorageWriteDestination::GlobalCurrentRows => self.global_current_rows.record(bytes),
            StorageWriteDestination::GlobalCurrentIndexes => {
                self.global_current_indexes.record(bytes)
            }
            StorageWriteDestination::RegisterGlobalCurrentRows => {
                self.register_global_current_rows.record(bytes)
            }
            StorageWriteDestination::GlobalChangesRows => self.global_changes_rows.record(bytes),
            StorageWriteDestination::GlobalChangesIndexes => {
                self.global_changes_indexes.record(bytes)
            }
            StorageWriteDestination::TransactionsRows => self.transactions_rows.record(bytes),
            StorageWriteDestination::TransactionsIndexes => self.transactions_indexes.record(bytes),
            StorageWriteDestination::Other => self.other.record(bytes),
        }
    }
}

/// Count and encoded key/value bytes for one storage-write bucket.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageWriteBucket {
    pub count: usize,
    pub bytes: usize,
}

impl StorageWriteBucket {
    pub(super) fn record(&mut self, bytes: usize) {
        self.count += 1;
        self.bytes += bytes;
    }
}

/// Durable storage-read counts split by stable Jazz logical destinations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageReadMetrics {
    pub total: StorageReadBucket,
    pub history_rows: StorageReadBucket,
    pub history_indexes: StorageReadBucket,
    pub global_current_rows: StorageReadBucket,
    pub global_current_indexes: StorageReadBucket,
    pub register_global_current_rows: StorageReadBucket,
    pub global_changes_rows: StorageReadBucket,
    pub global_changes_indexes: StorageReadBucket,
    pub transactions_rows: StorageReadBucket,
    pub transactions_indexes: StorageReadBucket,
    pub other: StorageReadBucket,
}

impl StorageReadMetrics {
    pub(super) fn record_point(&mut self, cf: &str, key: &[u8]) {
        self.record_destination(storage_read_destination(cf, key), 1, 1);
    }

    pub(super) fn record_range(&mut self, cf: &str, key: &[u8]) {
        self.record_destination(storage_read_destination(cf, key), 0, 1);
    }

    pub(super) fn record_range_row(&mut self, cf: &str, key: &[u8]) {
        self.record_destination(storage_read_destination(cf, key), 1, 0);
    }

    pub(super) fn record_destination(
        &mut self,
        destination: StorageReadDestination,
        reads: usize,
        ranges: usize,
    ) {
        self.total.record(reads, ranges);
        match destination {
            StorageReadDestination::HistoryRows => self.history_rows.record(reads, ranges),
            StorageReadDestination::HistoryIndexes => self.history_indexes.record(reads, ranges),
            StorageReadDestination::GlobalCurrentRows => {
                self.global_current_rows.record(reads, ranges)
            }
            StorageReadDestination::GlobalCurrentIndexes => {
                self.global_current_indexes.record(reads, ranges)
            }
            StorageReadDestination::RegisterGlobalCurrentRows => {
                self.register_global_current_rows.record(reads, ranges)
            }
            StorageReadDestination::GlobalChangesRows => {
                self.global_changes_rows.record(reads, ranges)
            }
            StorageReadDestination::GlobalChangesIndexes => {
                self.global_changes_indexes.record(reads, ranges)
            }
            StorageReadDestination::TransactionsRows => {
                self.transactions_rows.record(reads, ranges)
            }
            StorageReadDestination::TransactionsIndexes => {
                self.transactions_indexes.record(reads, ranges)
            }
            StorageReadDestination::Other => self.other.record(reads, ranges),
        }
    }
}

/// Count of logical storage records read and logical key ranges touched.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageReadBucket {
    pub reads: usize,
    pub ranges: usize,
}

impl StorageReadBucket {
    pub(super) fn record(&mut self, reads: usize, ranges: usize) {
        self.reads += reads;
        self.ranges += ranges;
    }
}

enum LocalHandle<'a, T> {
    Borrowed(&'a T),
    Owned(Rc<T>),
}

impl<T> std::ops::Deref for LocalHandle<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value,
        }
    }
}

pub(crate) struct MeteredStorage<'a, S> {
    storage: LocalHandle<'a, S>,
    metrics: LocalHandle<'a, RefCell<StorageReadMetrics>>,
}

impl<'a, S> MeteredStorage<'a, S> {
    pub(crate) fn new(storage: &'a S, metrics: &'a RefCell<StorageReadMetrics>) -> Self {
        Self {
            storage: LocalHandle::Borrowed(storage),
            metrics: LocalHandle::Borrowed(metrics),
        }
    }

    pub(crate) fn new_owned(
        storage: Rc<S>,
        metrics: Rc<RefCell<StorageReadMetrics>>,
    ) -> MeteredStorage<'static, S>
    where
        S: 'static,
    {
        MeteredStorage {
            storage: LocalHandle::Owned(storage),
            metrics: LocalHandle::Owned(metrics),
        }
    }
}

struct MeteredStorageCursor<'a> {
    inner: crate::storage::StorageScan<'a>,
    column_family: String,
    metrics: &'a RefCell<StorageReadMetrics>,
}

impl crate::storage::StorageCursor for MeteredStorageCursor<'_> {
    fn next_batch(
        &mut self,
    ) -> crate::storage::StorageFuture<
        '_,
        Result<Option<Vec<crate::storage::KeyValue>>, crate::storage::Error>,
    > {
        Box::pin(async move {
            let batch = self.inner.next_batch().await?;
            if let Some(batch) = &batch {
                let mut metrics = self.metrics.borrow_mut();
                for (key, _) in batch {
                    metrics.record_range_row(&self.column_family, key);
                }
            }
            Ok(batch)
        })
    }
}

impl<S> OrderedKvStorage for MeteredStorage<'_, S>
where
    S: OrderedKvStorage,
{
    fn scan(
        &self,
        request: crate::storage::ScanRequest,
    ) -> crate::storage::StorageFuture<
        '_,
        Result<crate::storage::StorageScan<'_>, crate::storage::Error>,
    > {
        let cf = request.cf.clone();
        let metric_key = match &request.bounds {
            crate::storage::ScanBounds::Prefix(prefix) => prefix.clone(),
            crate::storage::ScanBounds::Range { start, .. } => start.clone(),
        };
        self.metrics.borrow_mut().record_range(&cf, &metric_key);
        Box::pin(async move {
            Ok(Box::new(MeteredStorageCursor {
                inner: self.storage.scan(request).await?,
                column_family: cf,
                metrics: &self.metrics,
            }) as crate::storage::StorageScan<'_>)
        })
    }

    fn close(&self) -> crate::storage::StorageFuture<'_, Result<(), crate::storage::Error>> {
        self.storage.close()
    }

    fn set_write_flush_cadence(
        &self,
        every: usize,
    ) -> crate::storage::StorageFuture<'_, Result<(), crate::storage::Error>> {
        self.storage.set_write_flush_cadence(every)
    }

    fn flush_write_boundary(
        &self,
    ) -> crate::storage::StorageFuture<'_, Result<(), crate::storage::Error>> {
        self.storage.flush_write_boundary()
    }

    fn approximate_class_bytes(
        &self,
        cf: String,
    ) -> crate::storage::StorageFuture<'_, Result<Option<u64>, crate::storage::Error>> {
        self.storage.approximate_class_bytes(cf)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.storage.column_family_names()
    }

    fn get(
        &self,
        cf: String,
        key: Vec<u8>,
    ) -> crate::storage::StorageFuture<
        '_,
        Result<Option<crate::storage::Value>, crate::storage::Error>,
    > {
        self.metrics.borrow_mut().record_point(&cf, &key);
        self.storage.get(cf, key)
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> crate::storage::StorageFuture<'_, Result<(), crate::storage::Error>> {
        self.storage.set(cf, key, value)
    }

    fn delete(
        &self,
        cf: String,
        key: Vec<u8>,
    ) -> crate::storage::StorageFuture<'_, Result<(), crate::storage::Error>> {
        self.storage.delete(cf, key)
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> crate::storage::StorageFuture<
        '_,
        Result<Option<crate::storage::KeyValue>, crate::storage::Error>,
    > {
        self.metrics.borrow_mut().record_range(&cf, &prefix);
        Box::pin(async move {
            let value = self.storage.last_with_prefix(cf.clone(), prefix).await?;
            if let Some((key, _)) = &value {
                self.metrics.borrow_mut().record_range_row(&cf, key);
            }
            Ok(value)
        })
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: String,
        prefix: Vec<u8>,
        upper: Vec<u8>,
    ) -> crate::storage::StorageFuture<
        '_,
        Result<Option<crate::storage::KeyValue>, crate::storage::Error>,
    > {
        self.metrics.borrow_mut().record_range(&cf, &prefix);
        Box::pin(async move {
            let value = self
                .storage
                .last_with_prefix_before_or_at(cf.clone(), prefix, upper)
                .await?;
            if let Some((key, _)) = &value {
                self.metrics.borrow_mut().record_range_row(&cf, key);
            }
            Ok(value)
        })
    }

    fn write_many(
        &self,
        operations: Vec<crate::storage::OwnedWriteOperation>,
    ) -> crate::storage::StorageFuture<'_, Result<(), crate::storage::Error>> {
        self.storage.write_many(operations)
    }
}

/// Owned raw database entry with a lazy encoded-record view over the value.
#[derive(Clone, Debug)]
pub struct EncodedKeyValue<'a> {
    key: Vec<u8>,
    record: VariantRecord,
    marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> EncodedKeyValue<'a> {
    pub fn new(key: Vec<u8>, value: Vec<u8>, descriptor: &'a RecordDescriptor) -> Self {
        Self {
            key,
            record: VariantRecord::new(0, OwnedRecord::new(value, *descriptor)),
            marker: std::marker::PhantomData,
        }
    }

    pub(super) fn from_variant(key: Vec<u8>, record: VariantRecord) -> Self {
        Self {
            key,
            record,
            marker: std::marker::PhantomData,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn raw(&self) -> &[u8] {
        self.record.record().raw()
    }

    pub fn variant_tag(&self) -> u32 {
        self.record.variant_tag()
    }

    pub fn variant_record(&self) -> &VariantRecord {
        &self.record
    }

    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>) {
        (self.key, self.record.into_record().into_raw())
    }

    pub fn into_variant_parts(self) -> (Vec<u8>, VariantRecord) {
        (self.key, self.record)
    }

    pub fn record(&self) -> BorrowedRecord<'_> {
        self.record.record().borrowed()
    }

    pub fn owned_record(self) -> OwnedRecord {
        self.record.into_record()
    }
}

pub(super) fn write_operation_bytes(operation: &crate::storage::WriteOperation<'_>) -> usize {
    match operation {
        crate::storage::WriteOperation::Set { key, value, .. } => key.len() + value.len(),
        crate::storage::WriteOperation::Delete { key, .. } => key.len(),
        crate::storage::WriteOperation::Delta { key, delta, .. } => key.len() + delta.payload.len(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StorageWriteDestination {
    HistoryRows,
    HistoryIndexes,
    GlobalCurrentRows,
    GlobalCurrentIndexes,
    RegisterGlobalCurrentRows,
    GlobalChangesRows,
    GlobalChangesIndexes,
    TransactionsRows,
    TransactionsIndexes,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StorageReadDestination {
    HistoryRows,
    HistoryIndexes,
    GlobalCurrentRows,
    GlobalCurrentIndexes,
    RegisterGlobalCurrentRows,
    GlobalChangesRows,
    GlobalChangesIndexes,
    TransactionsRows,
    TransactionsIndexes,
    Other,
}

pub(super) fn storage_write_destination(
    operation: &crate::storage::WriteOperation<'_>,
) -> StorageWriteDestination {
    match operation {
        crate::storage::WriteOperation::Set { cf, key, .. }
        | crate::storage::WriteOperation::Delete { cf, key }
        | crate::storage::WriteOperation::Delta { cf, key, .. } => {
            if *cf == "indices" {
                storage_index_write_destination(key)
            } else {
                storage_table_write_destination(cf)
            }
        }
    }
}

pub(super) fn storage_table_write_destination(table: &str) -> StorageWriteDestination {
    if table == "jazz_global_changes" {
        StorageWriteDestination::GlobalChangesRows
    } else if table == "jazz_transactions" {
        StorageWriteDestination::TransactionsRows
    } else if table.starts_with("jazz_")
        && table.ends_with("_register_global_current")
        && !table.contains("_ahead_current")
    {
        StorageWriteDestination::RegisterGlobalCurrentRows
    } else if table.starts_with("jazz_")
        && table.ends_with("_global_current")
        && !table.contains("_register_global_current")
        && !table.contains("_ahead_current")
    {
        StorageWriteDestination::GlobalCurrentRows
    } else if table.starts_with("jazz_") && table.ends_with("_history") {
        StorageWriteDestination::HistoryRows
    } else {
        StorageWriteDestination::Other
    }
}

pub(super) fn storage_index_write_destination(key: &[u8]) -> StorageWriteDestination {
    let Some((table, index)) = durable_index_table_and_name(key) else {
        return StorageWriteDestination::Other;
    };
    if table == "jazz_global_changes"
        && (index == "by_global_time" || index == "by_table_global_time")
    {
        StorageWriteDestination::GlobalChangesIndexes
    } else if table == "jazz_transactions" && index == "by_global_time" {
        StorageWriteDestination::TransactionsIndexes
    } else if table.starts_with("jazz_")
        && table.ends_with("_global_current")
        && !table.contains("_register_global_current")
        && (index.starts_with("by_user_") || index.starts_with("by_physical_user_"))
    {
        StorageWriteDestination::GlobalCurrentIndexes
    } else if table.starts_with("jazz_") && table.ends_with("_history") && index == "by_tx" {
        StorageWriteDestination::HistoryIndexes
    } else {
        StorageWriteDestination::Other
    }
}

pub(super) fn storage_read_destination(cf: &str, key: &[u8]) -> StorageReadDestination {
    if cf == "indices" {
        storage_index_read_destination(key)
    } else {
        storage_table_read_destination(cf)
    }
}

pub(super) fn storage_table_read_destination(table: &str) -> StorageReadDestination {
    match storage_table_write_destination(table) {
        StorageWriteDestination::HistoryRows => StorageReadDestination::HistoryRows,
        StorageWriteDestination::GlobalCurrentRows => StorageReadDestination::GlobalCurrentRows,
        StorageWriteDestination::RegisterGlobalCurrentRows => {
            StorageReadDestination::RegisterGlobalCurrentRows
        }
        StorageWriteDestination::GlobalChangesRows => StorageReadDestination::GlobalChangesRows,
        StorageWriteDestination::TransactionsRows => StorageReadDestination::TransactionsRows,
        _ => StorageReadDestination::Other,
    }
}

pub(super) fn storage_index_read_destination(key: &[u8]) -> StorageReadDestination {
    match storage_index_write_destination(key) {
        StorageWriteDestination::HistoryIndexes => StorageReadDestination::HistoryIndexes,
        StorageWriteDestination::GlobalCurrentIndexes => {
            StorageReadDestination::GlobalCurrentIndexes
        }
        StorageWriteDestination::GlobalChangesIndexes => {
            StorageReadDestination::GlobalChangesIndexes
        }
        StorageWriteDestination::TransactionsIndexes => StorageReadDestination::TransactionsIndexes,
        _ => StorageReadDestination::Other,
    }
}

pub(super) fn durable_index_table_and_name(key: &[u8]) -> Option<(&str, &str)> {
    let table_end = key.iter().position(|byte| *byte == 0)?;
    let rest = key.get(table_end + 1..)?;
    let index_end = rest.iter().position(|byte| *byte == 0)?;
    let table = str::from_utf8(&key[..table_end]).ok()?;
    let index = str::from_utf8(&rest[..index_end]).ok()?;
    Some((table, index))
}

pub(super) enum PendingTableWrite {
    /// Insert and update share the same storage operation after validation.
    /// Delta computation decides whether an old record must be retracted first.
    Set {
        mode: WriteMode,
        table: String,
        key: Vec<u8>,
        variant_tag: u32,
        descriptor: RecordDescriptor,
        record: Vec<u8>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
        descriptor: RecordDescriptor,
    },
}

#[derive(Clone, Copy)]
pub(super) enum WriteMode {
    Insert,
    InsertFresh,
    Update,
}

impl PendingTableWrite {
    pub(super) fn table(&self) -> &str {
        match self {
            Self::Set { table, .. } | Self::Delete { table, .. } => table,
        }
    }

    pub(super) fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    pub(super) fn descriptor(&self) -> RecordDescriptor {
        match self {
            Self::Set { descriptor, .. } | Self::Delete { descriptor, .. } => *descriptor,
        }
    }

    pub(super) fn stored_record(&self) -> Option<Vec<u8>> {
        match self {
            Self::Set {
                variant_tag,
                record,
                ..
            } => Some(encode_variant_record(*variant_tag, record)),
            Self::Delete { .. } => None,
        }
    }
}

pub(super) async fn compute_table_deltas<S>(
    pending_writes: &[PendingTableWrite],
    stores: &[RecordStore<'_, S>],
    schema: &DatabaseSchema,
) -> Result<Vec<TableDelta>, Error>
where
    S: OrderedKvStorage,
{
    // Reads see earlier writes in the same batch through this overlay. Without
    // it, same-key insert/update/delete sequences emit deltas against stale
    // pre-batch storage and corrupt maintained views.
    // The keys already live for the duration of this computation. Borrow them
    // instead of allocating a second table name and primary-key buffer for
    // every write merely to track same-batch visibility.
    let mut overlay =
        HashMap::<(&str, &[u8]), Option<Vec<u8>>>::with_capacity(pending_writes.len());
    // Accumulate directly into the homogeneous groups consumed by IVM. The
    // previous path allocated a singleton TableDelta (and Vec) per old/new
    // record, then hashed every group and record again in a second pass.
    let mut by_table = HashMap::<(&str, u32, RecordDescriptor), HashMap<bytes::Bytes, i64>>::new();

    for (write, store) in pending_writes.iter().zip(stores) {
        let overlay_key = (write.table(), write.key());
        let current = if let Some(record) = overlay.get(&overlay_key) {
            record.clone()
        } else if matches!(
            write,
            PendingTableWrite::Set {
                mode: WriteMode::InsertFresh,
                ..
            }
        ) {
            None
        } else {
            store.get_raw(write.key()).await?
        };
        if matches!(
            write,
            PendingTableWrite::Set {
                mode: WriteMode::Insert,
                ..
            }
        ) && current.is_some()
        {
            return Err(Error::DuplicatePrimaryKey {
                table: write.table().to_owned(),
                key: write.key().to_vec(),
            });
        }
        let table_schema = schema
            .table(write.table())
            .ok_or_else(|| Error::TableNotFound(write.table().to_owned()))?;
        if let Some(current) = current.as_deref() {
            let (variant_tag, payload) = split_variant_record(current)?;
            let descriptor = table_schema
                .record_schema_for_variant(variant_tag)
                .ok_or_else(|| Error::UnknownTableVariant {
                    table: table_schema.name.clone(),
                    version: u64::from(variant_tag),
                })?;
            *by_table
                .entry((write.table(), variant_tag, descriptor))
                .or_default()
                .entry(bytes::Bytes::copy_from_slice(payload))
                .or_default() -= 1;
        }
        if let PendingTableWrite::Set {
            variant_tag,
            descriptor,
            record,
            ..
        } = write
        {
            *by_table
                .entry((write.table(), *variant_tag, *descriptor))
                .or_default()
                .entry(bytes::Bytes::copy_from_slice(record))
                .or_default() += 1;
        }
        let next = write.stored_record();
        overlay.insert(overlay_key, next);
    }

    Ok(by_table
        .into_iter()
        .filter_map(|((table, variant_tag, descriptor), records)| {
            let deltas = records
                .into_iter()
                .filter_map(|(record, weight)| {
                    (weight != 0).then_some(RecordDelta { record, weight })
                })
                .collect::<Vec<_>>();
            (!deltas.is_empty()).then_some(TableDelta {
                table: table.to_owned(),
                variant_tag,
                descriptor,
                deltas,
            })
        })
        .collect())
}

pub(super) fn record_store_for_table<'a, S>(
    storage: &'a S,
    table: &'a str,
    key_descriptor: Option<RecordDescriptor>,
    descriptor: &'a RecordDescriptor,
) -> RecordStore<'a, S>
where
    S: OrderedKvStorage,
{
    let _ = key_descriptor;
    RecordStore::new(storage, table, descriptor)
}

pub(super) fn primary_key_descriptor(primary_key: &PrimaryKey) -> RecordDescriptor {
    RecordDescriptor::new(
        primary_key
            .columns
            .iter()
            .map(|column| (column.column.clone(), column.key_type.column_type().clone())),
    )
}
