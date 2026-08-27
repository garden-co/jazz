use super::*;
use crate::records::ValidatedVariantRecord;

/// Mutable staged table writes whose reads observe writes already added to the
/// stage. Commit runs one normal database batch commit, so current callers of
/// [`Database::commit_batch`] and staged callers share the final tick/write path.
#[cfg(test)]
pub(crate) struct StagedDatabaseBatch<'a> {
    pub(super) database: &'a mut Database,
    pub(super) batch: DatabaseBatch,
}

#[cfg(test)]
impl StagedDatabaseBatch<'_> {
    pub fn insert(&mut self, table: impl Into<String>, record: impl Into<RecordInput>) {
        self.batch.insert(table, record);
    }

    pub fn update(&mut self, table: impl Into<String>, record: impl Into<RecordInput>) {
        self.batch.update(table, record);
    }

    pub fn delete(&mut self, table: impl Into<String>, key: PrimaryKeyValue) {
        self.batch.delete(table, key);
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
    pub(super) notification_timing: NotificationTiming,
    pub(super) accepted_large_values: Vec<crate::large_values::StagedLargeValueId>,
}

impl PartialEq for DatabaseBatch {
    fn eq(&self, other: &Self) -> bool {
        self.operations == other.operations
            && self.notification_timing == other.notification_timing
            && self.accepted_large_values == other.accepted_large_values
    }
}

/// When subscription notifications produced by a batch become observable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NotificationTiming {
    /// Publish every resident terminal reached before [`Database::apply_batch`]
    /// returns.
    #[default]
    Immediate,
    /// Hold notifications until the batch's persistence receipt is finished;
    /// discard them if persistence fails.
    AfterPersistence,
}

impl DatabaseBatch {
    pub fn deliver_notifications(&mut self, timing: NotificationTiming) {
        self.notification_timing = timing;
    }

    pub fn reserve(&mut self, additional: usize) {
        self.operations.reserve(additional);
    }

    /// Atomically consume a Groove staging root with this physical-record
    /// batch. The id is opaque to callers and acceptance is idempotent only as
    /// part of retrying the same uncommitted batch.
    pub fn accept_large_value(&mut self, id: crate::large_values::StagedLargeValueId) {
        if !self.accepted_large_values.contains(&id) {
            self.accepted_large_values.push(id);
        }
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
    /// # Safety
    ///
    /// The caller must guarantee that `key` is absent from both persisted storage
    /// and earlier operations in this batch. A false proof would omit the previous
    /// record's negative maintained-view delta.
    pub unsafe fn insert_raw_fresh(
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
    ValidatedRecord(ValidatedVariantRecord),
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

impl From<ValidatedVariantRecord> for RawRecordInput {
    fn from(record: ValidatedVariantRecord) -> Self {
        Self::ValidatedRecord(record)
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
    /// Encode this logical primary key exactly as Groove stores it.
    pub fn into_bytes(self) -> Vec<u8> {
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
