use super::*;

/// Typed facade over one schema-declared direct record store.
///
/// ```
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
/// let database = Database::new(schema, storage)?;
/// let art = database.direct_record_store("album_art")?;
///
/// art.set(&[Value::U64(1)], &[Value::Bytes(b"front-cover-bytes".to_vec())])?;
/// assert_eq!(
///     art.get(&[Value::U64(1)])?.unwrap().get("bytes")?,
///     Value::Bytes(b"front-cover-bytes".to_vec())
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct DirectRecordStore<'a, S> {
    pub(super) storage: &'a LayoutStorage<S>,
    pub(super) name: String,
    pub(super) key: RecordDescriptor,
    pub(super) value: RecordDescriptor,
}

impl<S> DirectRecordStore<'_, S>
where
    S: ResidentStorage,
{
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

    pub fn set(&self, key: &[Value], value: &[Value]) -> Result<(), Error> {
        let key = self.key_bytes(key)?;
        let record = self.value.create(value)?;
        self.storage
            .write_many(&[WriteOperation::set(&self.name, &key, &record)])
            .map_err(Error::from)
    }

    /// Stage a typed direct-store value in the same atomic database batch as
    /// ordinary table/IVM writes.
    #[doc(hidden)]
    pub fn stage_set(
        &self,
        batch: &mut DatabaseBatch,
        key: &[Value],
        value: &[Value],
    ) -> Result<(), Error> {
        batch.push_direct_operation(OwnedWriteOperation::Set {
            cf: self.name.clone(),
            key: self.key_bytes(key)?,
            value: self.value.create(value)?,
        });
        Ok(())
    }

    pub fn get(&self, key: &[Value]) -> Result<Option<Record<'_>>, Error> {
        let key = self.key_bytes(key)?;
        Ok(self
            .record_store()
            .get_raw(&key)?
            .map(|record| self.value.bind_owned(record)))
    }

    pub fn delete(&self, key: &[Value]) -> Result<(), Error> {
        let key = self.key_bytes(key)?;
        self.storage
            .write_many(&[WriteOperation::delete(&self.name, &key)])
            .map_err(Error::from)
    }

    pub fn range(&self, start: &[Value], end: &[Value]) -> Result<Vec<Record<'_>>, Error> {
        let start = self.key_prefix_bytes(start)?;
        let end = self.key_prefix_bytes(end)?;
        self.record_store()
            .range(&start, &end)?
            .into_iter()
            .map(|(_, value)| Ok(self.value.bind_owned(value)))
            .collect()
    }

    pub fn range_entries(
        &self,
        start: &[Value],
        end: &[Value],
    ) -> Result<Vec<DirectRecordStoreEntry<'_>>, Error> {
        let start = self.key_prefix_bytes(start)?;
        let end = self.key_prefix_bytes(end)?;
        self.record_store()
            .range(&start, &end)?
            .into_iter()
            .map(|(key, value)| {
                Ok(DirectRecordStoreEntry {
                    key: self.decode_key(&key)?,
                    value: self.value.bind_owned(value),
                })
            })
            .collect()
    }

    pub fn prefix(&self, prefix: &[Value]) -> Result<Vec<Record<'_>>, Error> {
        let prefix = self.key_prefix_bytes(prefix)?;
        self.record_store()
            .prefix(&prefix)?
            .into_iter()
            .map(|(_, value)| Ok(self.value.bind_owned(value)))
            .collect()
    }

    pub fn prefix_entries(
        &self,
        prefix: &[Value],
    ) -> Result<Vec<DirectRecordStoreEntry<'_>>, Error> {
        let prefix = self.key_prefix_bytes(prefix)?;
        self.record_store()
            .prefix(&prefix)?
            .into_iter()
            .map(|(key, value)| {
                Ok(DirectRecordStoreEntry {
                    key: self.decode_key(&key)?,
                    value: self.value.bind_owned(value),
                })
            })
            .collect()
    }

    pub fn write_many(&self, operations: &[DirectRecordStoreWrite]) -> Result<(), Error> {
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
        let borrowed = encoded
            .iter()
            .map(OwnedWriteOperation::as_write_operation)
            .collect::<Vec<_>>();
        self.storage.write_many(&borrowed).map_err(Error::from)
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

    pub(super) fn record_store(&self) -> RecordStore<'_, LayoutStorage<S>> {
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

pub(crate) struct MeteredStorage<'a, S> {
    storage: &'a S,
    metrics: &'a RefCell<StorageReadMetrics>,
}

impl<'a, S> MeteredStorage<'a, S> {
    pub(crate) fn new(storage: &'a S, metrics: &'a RefCell<StorageReadMetrics>) -> Self {
        Self { storage, metrics }
    }
}

impl<S> ResidentStorage for MeteredStorage<'_, S>
where
    S: ResidentStorage,
{
    fn get(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        key: &crate::storage::Key,
    ) -> Result<Option<crate::storage::Value>, crate::storage::Error> {
        self.metrics.borrow_mut().record_point(cf, key);
        self.storage.get(cf, key)
    }

    fn set(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        key: &crate::storage::Key,
        value: &[u8],
    ) -> Result<(), crate::storage::Error> {
        self.storage.set(cf, key, value)
    }

    fn delete(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        key: &crate::storage::Key,
    ) -> Result<(), crate::storage::Error> {
        self.storage.delete(cf, key)
    }

    fn scan_range(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        start: &crate::storage::Key,
        end: &crate::storage::Key,
        visit: &mut crate::storage::ScanVisitor<'_>,
    ) -> Result<(), crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, start);
        self.storage.scan_range(cf, start, end, &mut |key, value| {
            self.metrics.borrow_mut().record_range_row(cf, key);
            visit(key, value)
        })
    }

    fn scan_prefix(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        prefix: &crate::storage::Key,
        visit: &mut crate::storage::ScanVisitor<'_>,
    ) -> Result<(), crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, prefix);
        self.storage.scan_prefix(cf, prefix, &mut |key, value| {
            self.metrics.borrow_mut().record_range_row(cf, key);
            visit(key, value)
        })
    }

    fn scan_prefix_reverse(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        prefix: &crate::storage::Key,
        visit: &mut crate::storage::ScanVisitor<'_>,
    ) -> Result<(), crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, prefix);
        self.storage
            .scan_prefix_reverse(cf, prefix, &mut |key, value| {
                self.metrics.borrow_mut().record_range_row(cf, key);
                visit(key, value)
            })
    }

    fn last_with_prefix(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        prefix: &crate::storage::Key,
    ) -> Result<Option<crate::storage::KeyValue>, crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, prefix);
        let value = self.storage.last_with_prefix(cf, prefix)?;
        if let Some((key, _)) = &value {
            self.metrics.borrow_mut().record_range_row(cf, key);
        }
        Ok(value)
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &crate::storage::ColumnFamilyName,
        prefix: &crate::storage::Key,
        upper: &crate::storage::Key,
    ) -> Result<Option<crate::storage::KeyValue>, crate::storage::Error> {
        self.metrics.borrow_mut().record_range(cf, prefix);
        let value = self
            .storage
            .last_with_prefix_before_or_at(cf, prefix, upper)?;
        if let Some((key, _)) = &value {
            self.metrics.borrow_mut().record_range_row(cf, key);
        }
        Ok(value)
    }

    fn write_many(
        &self,
        operations: &[crate::storage::WriteOperation<'_>],
    ) -> Result<(), crate::storage::Error> {
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
        && (index == "by_global_seq" || index == "by_table_global_seq")
    {
        StorageWriteDestination::GlobalChangesIndexes
    } else if table == "jazz_transactions" && index == "by_global_seq" {
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

pub(super) fn compute_table_deltas<S>(
    pending_writes: &[PendingTableWrite],
    stores: &[RecordStore<'_, S>],
    schema: &DatabaseSchema,
) -> Result<Vec<TableDelta>, Error>
where
    S: ResidentStorage,
{
    // Reads see earlier writes in the same batch through this overlay. Without
    // it, same-key insert/update/delete sequences emit deltas against stale
    // pre-batch storage and corrupt maintained views.
    let mut overlay = HashMap::<(String, Vec<u8>), Option<Vec<u8>>>::new();
    let mut table_deltas = Vec::with_capacity(pending_writes.len());

    for (write, store) in pending_writes.iter().zip(stores) {
        let overlay_key = (write.table().to_owned(), write.key().to_vec());
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
            store.get_raw(write.key())?
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
            table_deltas.push(table_delta_from_stored(table_schema, current, -1)?);
        }
        if let PendingTableWrite::Set {
            variant_tag,
            descriptor,
            record,
            ..
        } = write
        {
            table_deltas.push(TableDelta {
                table: write.table().to_owned(),
                variant_tag: *variant_tag,
                descriptor: *descriptor,
                deltas: vec![RecordDelta {
                    record: record.clone().into(),
                    weight: 1,
                }],
            });
        }
        let next = write.stored_record();
        overlay.insert(overlay_key, next);
    }

    Ok(consolidate_table_deltas(table_deltas))
}

pub(super) fn table_delta_from_stored(
    table: &TableSchema,
    stored: &[u8],
    weight: i64,
) -> Result<TableDelta, Error> {
    let (variant_tag, payload) = split_variant_record(stored)?;
    let descriptor = table
        .record_schema_for_variant(variant_tag)
        .ok_or_else(|| Error::UnknownTableVariant {
            table: table.name.clone(),
            version: u64::from(variant_tag),
        })?;
    Ok(TableDelta {
        table: table.name.clone(),
        variant_tag,
        descriptor,
        deltas: vec![RecordDelta {
            record: bytes::Bytes::copy_from_slice(payload),
            weight,
        }],
    })
}

pub(super) fn record_store_for_table<'a, S>(
    storage: &'a S,
    table: &'a str,
    key_descriptor: Option<RecordDescriptor>,
    descriptor: &'a RecordDescriptor,
) -> RecordStore<'a, S>
where
    S: ResidentStorage,
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

pub(super) fn consolidate_table_deltas(table_deltas: Vec<TableDelta>) -> Vec<TableDelta> {
    let mut by_table =
        HashMap::<(String, u32, RecordDescriptor), HashMap<bytes::Bytes, i64>>::new();
    for table_delta in table_deltas {
        let records = by_table
            .entry((
                table_delta.table,
                table_delta.variant_tag,
                table_delta.descriptor,
            ))
            .or_default();
        for delta in table_delta.deltas {
            *records.entry(delta.record).or_default() += delta.weight;
        }
    }
    by_table
        .into_iter()
        .filter_map(|((table, variant_tag, descriptor), records)| {
            let deltas = records
                .into_iter()
                .filter_map(|(record, weight)| {
                    (weight != 0).then_some(RecordDelta { record, weight })
                })
                .collect::<Vec<_>>();
            (!deltas.is_empty()).then_some(TableDelta {
                table,
                variant_tag,
                descriptor,
                deltas,
            })
        })
        .collect()
}

impl Database<crate::storage::DemandLoadedStorage> {
    /// Drain the operation journal owned by the real resident database.
    #[doc(hidden)]
    pub fn take_demand_loaded_pending_writes(&self) -> Vec<OwnedWriteOperation> {
        self.storage.take_pending_writes()
    }

    /// Drain column-family requests owned by the real resident database.
    #[doc(hidden)]
    pub fn take_demand_loaded_pending_column_families(&self) -> Vec<String> {
        self.storage.take_pending_column_families()
    }
}
