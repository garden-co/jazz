use super::*;

/// Mutable staged table writes whose reads observe writes already added to the
/// stage. Commit runs one normal database batch commit, so current callers of
/// [`Database::commit_batch`] and staged callers share the final tick/write path.
pub struct StagedDatabaseBatch<'a> {
    pub(super) database: &'a mut Database,
    pub(super) batch: DatabaseBatch,
}

impl StagedDatabaseBatch<'_> {
    pub fn reserve(&mut self, additional: usize) {
        self.batch.reserve(additional);
    }

    pub fn insert(&mut self, table: impl Into<String>, record: impl Into<RecordInput>) {
        self.batch.insert(table, record);
    }

    pub fn insert_raw(
        &mut self,
        table: impl Into<String>,
        key: PrimaryKeyValue,
        record: impl Into<RawRecordInput>,
    ) {
        self.batch.insert_raw(table, key, record);
    }

    pub fn update(&mut self, table: impl Into<String>, record: impl Into<RecordInput>) {
        self.batch.update(table, record);
    }

    pub fn update_raw(
        &mut self,
        table: impl Into<String>,
        key: PrimaryKeyValue,
        record: impl Into<RawRecordInput>,
    ) {
        self.batch.update_raw(table, key, record);
    }

    pub fn delete(&mut self, table: impl Into<String>, key: PrimaryKeyValue) {
        self.batch.delete(table, key);
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    pub async fn primary_key_scan(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<VariantRecord>, Error> {
        self.database.ensure_batch_storage_txn(&self.batch)?;
        let overlay = StagedWriteOverlay::new(&self.database.storage, &self.batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.database.storage_read_metrics);
        self.database
            .primary_key_scan_with_storage(&storage, table, prefix)
            .await
    }

    pub async fn primary_key_scan_raw(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Vec<EncodedKeyValue<'_>>, Error> {
        self.database.ensure_batch_storage_txn(&self.batch)?;
        let overlay = StagedWriteOverlay::new(&self.database.storage, &self.batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.database.storage_read_metrics);
        self.database
            .primary_key_scan_raw_with_storage(&storage, table, prefix)
            .await
    }

    pub async fn primary_key_last_raw(
        &self,
        table: &str,
        prefix: &[Value],
    ) -> Result<Option<EncodedKeyValue<'_>>, Error> {
        self.database.ensure_batch_storage_txn(&self.batch)?;
        let overlay = StagedWriteOverlay::new(&self.database.storage, &self.batch.txn_operations);
        let storage = MeteredStorage::new(&overlay, &self.database.storage_read_metrics);
        self.database
            .primary_key_last_raw_with_storage(&storage, table, prefix)
            .await
    }

    pub async fn commit(self) -> Result<(), Error> {
        self.database.commit_batch(self.batch).await
    }
}

/// Mutable collection of table writes committed atomically at storage level.
#[derive(Clone, Debug, Default)]
pub struct DatabaseBatch {
    pub(super) operations: Vec<BatchOperation>,
    pub(super) txn_operations: RefCell<StagedWriteState>,
    pub(super) txn_indexed_operations: Cell<usize>,
}

impl PartialEq for DatabaseBatch {
    fn eq(&self, other: &Self) -> bool {
        self.operations == other.operations
    }
}

impl DatabaseBatch {
    pub fn reserve(&mut self, additional: usize) {
        self.operations.reserve(additional);
    }

    pub fn insert(&mut self, table: impl Into<String>, record: impl Into<RecordInput>) {
        self.push_operation(BatchOperation::Insert {
            table: table.into(),
            record: record.into(),
        });
    }

    pub fn insert_raw(
        &mut self,
        table: impl Into<String>,
        key: PrimaryKeyValue,
        record: impl Into<RawRecordInput>,
    ) {
        self.push_operation(BatchOperation::InsertRaw {
            table: table.into(),
            key,
            record: record.into(),
        });
    }

    /// Stage a raw insert whose caller has already proven that the key is absent.
    ///
    /// This avoids a storage lookup during delta computation. It is only sound for
    /// internal append-only tables whose enclosing transaction identity proves
    /// freshness; ordinary insert callers must use [`Self::insert_raw`].
    pub fn insert_raw_fresh(
        &mut self,
        table: impl Into<String>,
        key: PrimaryKeyValue,
        record: impl Into<RawRecordInput>,
    ) {
        self.push_operation(BatchOperation::InsertRawFresh {
            table: table.into(),
            key,
            record: record.into(),
        });
    }

    pub fn update(&mut self, table: impl Into<String>, record: impl Into<RecordInput>) {
        self.push_operation(BatchOperation::Update {
            table: table.into(),
            record: record.into(),
        });
    }

    pub fn update_raw(
        &mut self,
        table: impl Into<String>,
        key: PrimaryKeyValue,
        record: impl Into<RawRecordInput>,
    ) {
        self.push_operation(BatchOperation::UpdateRaw {
            table: table.into(),
            key,
            record: record.into(),
        });
    }

    pub fn delete(&mut self, table: impl Into<String>, key: PrimaryKeyValue) {
        self.push_operation(BatchOperation::Delete {
            table: table.into(),
            key,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub(super) fn push_operation(&mut self, operation: BatchOperation) {
        self.operations.push(operation);
    }
}

/// Logical or already-encoded row supplied to an ordinary table write.
///
/// `Vec<Value>` converts to the reserved single-layout schema version `0`.
/// Callers that need another discriminator bind an [`OwnedRecord`] to it once
/// and pass the resulting [`VariantRecord`] through the same write API.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordInput {
    Values(Vec<Value>),
    Record(VariantRecord),
}

impl From<Vec<Value>> for RecordInput {
    fn from(values: Vec<Value>) -> Self {
        Self::Values(values)
    }
}

impl From<VariantRecord> for RecordInput {
    fn from(record: VariantRecord) -> Self {
        Self::Record(record)
    }
}

/// Encoded row supplied with an explicit primary key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RawRecordInput {
    Payload(Vec<u8>),
    Record(VariantRecord),
}

impl From<Vec<u8>> for RawRecordInput {
    fn from(payload: Vec<u8>) -> Self {
        Self::Payload(payload)
    }
}

impl From<VariantRecord> for RawRecordInput {
    fn from(record: VariantRecord) -> Self {
        Self::Record(record)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum BatchOperation {
    Insert {
        table: String,
        record: RecordInput,
    },
    InsertRaw {
        table: String,
        key: PrimaryKeyValue,
        record: RawRecordInput,
    },
    InsertRawFresh {
        table: String,
        key: PrimaryKeyValue,
        record: RawRecordInput,
    },
    Update {
        table: String,
        record: RecordInput,
    },
    UpdateRaw {
        table: String,
        key: PrimaryKeyValue,
        record: RawRecordInput,
    },
    Delete {
        table: String,
        key: PrimaryKeyValue,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrimaryKeyValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Composite(Vec<PrimaryKeyValue>),
}

impl PrimaryKeyValue {
    pub(super) fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        match self {
            Self::U8(value) => encode_primary_key_part(&mut bytes, &Value::U8(value))
                .expect("PrimaryKeyValue only contains encodable primary-key values"),
            Self::U16(value) => encode_primary_key_part(&mut bytes, &Value::U16(value))
                .expect("PrimaryKeyValue only contains encodable primary-key values"),
            Self::U32(value) => encode_primary_key_part(&mut bytes, &Value::U32(value))
                .expect("PrimaryKeyValue only contains encodable primary-key values"),
            Self::U64(value) => encode_primary_key_part(&mut bytes, &Value::U64(value))
                .expect("PrimaryKeyValue only contains encodable primary-key values"),
            Self::Bool(value) => encode_primary_key_part(&mut bytes, &Value::Bool(value))
                .expect("PrimaryKeyValue only contains encodable primary-key values"),
            Self::String(value) => encode_primary_key_part(&mut bytes, &Value::String(value))
                .expect("PrimaryKeyValue only contains encodable primary-key values"),
            Self::Bytes(value) => encode_primary_key_part(&mut bytes, &Value::Bytes(value))
                .expect("PrimaryKeyValue only contains encodable primary-key values"),
            Self::Uuid(value) => encode_primary_key_part(&mut bytes, &Value::Uuid(value))
                .expect("PrimaryKeyValue only contains encodable primary-key values"),
            Self::Composite(values) => {
                for value in values {
                    bytes.extend(value.into_bytes());
                }
            }
        }
        bytes
    }
}
