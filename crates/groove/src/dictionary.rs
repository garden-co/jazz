//! Dictionary tables: compact durable codes for values that repeat.
//!
//! A dictionary maps each distinct byte string stored in it to a `u32` code,
//! so a column that would repeat the same long value on many rows stores the
//! 4-byte code instead. The mapping is an ordinary Groove table, created with
//! [`table_schema`], keyed by code with a unique index on the value; it is
//! append-only, so a code never changes meaning once it is durable.
//!
//! [`Dictionary`] resolves values against one such table and stages new
//! entries into the caller's batch, so an entry becomes durable atomically
//! with the first row that stores its code. Until an entry is seen in
//! storage it is provisional: a later batch that needs it stages it again
//! under the same code, so a dropped batch never leaves a row naming a code
//! without an entry.

use rustc_hash::FxHashMap;

use crate::db::{Database, DatabaseBatch, Error};
use crate::records::Value;
use crate::schema::{
    ColumnSchema, ColumnType, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
};

/// Field index of the code in a dictionary record.
pub const FIELD_CODE_IDX: usize = 0;
/// Field index of the value in a dictionary record.
pub const FIELD_VALUE_IDX: usize = 1;
/// Name of the unique value index of a dictionary table.
pub const BY_VALUE_INDEX: &str = "by_value";

/// Resident entries kept per direction before the cache is cleared.
const RESIDENT_CACHE_MAX_ENTRIES: usize = 16 * 1024;

/// The schema of a dictionary table named `name`.
pub fn table_schema(name: impl Into<String>) -> TableSchema {
    TableSchema::new(
        name,
        [
            ColumnSchema::new("code", ColumnType::U32),
            ColumnSchema::new("value", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("code", IntegerKeyType::U32))
    .with_index(IndexSchema::new(BY_VALUE_INDEX, ["value"]).unique())
}

/// Interns and resolves values of one dictionary table.
#[derive(Debug)]
pub struct Dictionary {
    table: String,
    codes: FxHashMap<Vec<u8>, u32>,
    values: FxHashMap<u32, Vec<u8>>,
    /// Codes handed out by [`Dictionary::stage`] whose entries were not yet
    /// seen in storage.
    provisional: FxHashMap<Vec<u8>, u32>,
}

impl Dictionary {
    /// A dictionary over the table `table`, which must use [`table_schema`].
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            codes: FxHashMap::default(),
            values: FxHashMap::default(),
            provisional: FxHashMap::default(),
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    /// The code of `value`, if it was ever interned.
    pub async fn code(&mut self, database: &Database, value: &[u8]) -> Result<Option<u32>, Error> {
        if let Some(code) = self.codes.get(value) {
            return Ok(Some(*code));
        }
        let mut found = None;
        for raw in database
            .index_scan_raw(&self.table, BY_VALUE_INDEX, &[Value::Bytes(value.to_vec())])
            .await?
        {
            let record = raw.record();
            if record.get_bytes(FIELD_VALUE_IDX)? != value {
                continue;
            }
            if found.is_some() {
                return Err(self.invalid("a value has two codes"));
            }
            found = Some(record.get_u32(FIELD_CODE_IDX)?);
        }
        if let Some(code) = found {
            self.provisional.remove(value);
            self.remember(code, value.to_vec());
        }
        Ok(found)
    }

    /// The code of `value`, staging its entry into `batch` unless the entry
    /// is already durable. A new value gets the next unused code.
    pub async fn stage(
        &mut self,
        database: &Database,
        batch: &mut DatabaseBatch,
        value: &[u8],
    ) -> Result<u32, Error> {
        if let Some(code) = self.code(database, value).await? {
            return Ok(code);
        }
        let code = match self.provisional.get(value) {
            Some(code) => *code,
            None => {
                let last = match database.primary_key_last_raw(&self.table, &[]).await? {
                    None => None,
                    Some(raw) => Some(raw.record().get_u32(FIELD_CODE_IDX)?),
                };
                let code = match last.max(self.provisional.values().max().copied()) {
                    None => 0,
                    Some(code) => code
                        .checked_add(1)
                        .ok_or_else(|| Error::DictionaryExhausted(self.table.clone()))?,
                };
                self.provisional.insert(value.to_vec(), code);
                code
            }
        };
        batch.update(
            self.table.clone(),
            vec![Value::U32(code), Value::Bytes(value.to_vec())],
        );
        Ok(code)
    }

    /// The value that `code` stands for. A code without an entry is refused:
    /// an entry is durable no later than the first row that stores its code.
    pub async fn value(&mut self, database: &Database, code: u32) -> Result<Vec<u8>, Error> {
        if let Some(value) = self.values.get(&code) {
            return Ok(value.clone());
        }
        let raw = database
            .primary_key_get_raw(&self.table, &[Value::U32(code)])
            .await?
            .ok_or_else(|| self.invalid("a stored code has no entry"))?;
        let value = raw.record().get_bytes(FIELD_VALUE_IDX)?.to_vec();
        self.remember(code, value.clone());
        Ok(value)
    }

    /// Forget every resident entry, for instance after the storage under the
    /// database was replaced.
    pub fn clear(&mut self) {
        self.codes.clear();
        self.values.clear();
        self.provisional.clear();
    }

    fn remember(&mut self, code: u32, value: Vec<u8>) {
        if self.codes.len() >= RESIDENT_CACHE_MAX_ENTRIES {
            self.codes.clear();
            self.values.clear();
        }
        self.codes.insert(value.clone(), code);
        self.values.insert(code, value);
    }

    fn invalid(&self, reason: &'static str) -> Error {
        Error::InvalidDictionary {
            table: self.table.clone(),
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DatabaseSchema;
    use crate::storage::MemoryStorage;

    fn schema() -> DatabaseSchema {
        DatabaseSchema::new([table_schema("authors")])
    }

    async fn commit(database: &mut Database, batch: DatabaseBatch) {
        let applied = database.apply_batch(batch).await.unwrap();
        let persisted = database.persist_with_progress(&applied).await;
        database.finish_persistence(persisted).unwrap();
    }

    /// Actors: one database staging author strings into its batches, then a
    /// reopen of the same storage with a fresh dictionary.
    ///
    /// Codes are dense from 0, staging a known value reuses its code, and a
    /// reopened database resolves both directions from storage alone.
    #[futures_test::test]
    async fn codes_are_stable_across_reopen() {
        let schema = schema();
        let storage = MemoryStorage::new(&schema.column_families()).unwrap();
        let mut database = Database::new(schema.clone(), storage.clone())
            .await
            .unwrap();
        let mut dictionary = Dictionary::new("authors");
        assert_eq!(dictionary.code(&database, b"alice").await.unwrap(), None);
        let mut batch = database.open_batch();
        assert_eq!(
            dictionary
                .stage(&database, &mut batch, b"alice")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            dictionary
                .stage(&database, &mut batch, b"bob")
                .await
                .unwrap(),
            1
        );
        commit(&mut database, batch).await;
        let mut batch = database.open_batch();
        assert_eq!(
            dictionary
                .stage(&database, &mut batch, b"alice")
                .await
                .unwrap(),
            0
        );
        assert!(batch.is_empty(), "a durable entry is not staged again");
        drop(database);

        let reopened = Database::new(schema, storage).await.unwrap();
        let mut dictionary = Dictionary::new("authors");
        assert_eq!(dictionary.value(&reopened, 1).await.unwrap(), b"bob");
        assert_eq!(dictionary.code(&reopened, b"alice").await.unwrap(), Some(0));
        let mut batch = reopened.open_batch();
        assert_eq!(
            dictionary
                .stage(&reopened, &mut batch, b"carol")
                .await
                .unwrap(),
            2
        );
    }

    /// Actors: one database whose first batch naming a new value is dropped
    /// before it is applied, then a second value and a retry of the first.
    ///
    /// The dropped value keeps its provisional code, so no code is handed to
    /// two values, and the retry stages the entry again so it becomes durable
    /// with the row that uses it.
    #[futures_test::test]
    async fn a_dropped_batch_keeps_codes_unique_and_restages_the_entry() {
        let schema = schema();
        let storage = MemoryStorage::new(&schema.column_families()).unwrap();
        let mut database = Database::new(schema, storage).await.unwrap();
        let mut dictionary = Dictionary::new("authors");
        let mut dropped = database.open_batch();
        assert_eq!(
            dictionary
                .stage(&database, &mut dropped, b"alice")
                .await
                .unwrap(),
            0
        );
        drop(dropped);

        let mut batch = database.open_batch();
        assert_eq!(
            dictionary
                .stage(&database, &mut batch, b"bob")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            dictionary
                .stage(&database, &mut batch, b"alice")
                .await
                .unwrap(),
            0
        );
        commit(&mut database, batch).await;
        let mut fresh = Dictionary::new("authors");
        assert_eq!(fresh.code(&database, b"alice").await.unwrap(), Some(0));
        assert_eq!(fresh.code(&database, b"bob").await.unwrap(), Some(1));
    }

    /// Actors: one database whose dictionary is asked for a code no row
    /// could have stored.
    ///
    /// Entries are durable before their code is handed out, so a missing
    /// entry is corruption and is refused rather than read as empty.
    #[futures_test::test]
    async fn an_unknown_code_is_refused() {
        let schema = schema();
        let storage = MemoryStorage::new(&schema.column_families()).unwrap();
        let database = Database::new(schema, storage).await.unwrap();
        let mut dictionary = Dictionary::new("authors");
        assert!(matches!(
            dictionary.value(&database, 7).await,
            Err(Error::InvalidDictionary { .. })
        ));
    }
}
