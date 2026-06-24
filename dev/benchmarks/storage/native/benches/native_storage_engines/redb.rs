use std::path::PathBuf;

use ::redb::{
    Database, Durability, ReadOnlyTable, ReadableDatabase, ReadableTable, TableDefinition,
    WriteTransaction,
};
use bench_core::{BenchEngine, EngineError, PhaseKind};
use tempfile::TempDir;

use super::{NativeEngine, eng};

const REDB_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

pub(super) struct RedbEngine {
    path: PathBuf,
    db: Option<Database>,
    write_txn: Option<WriteTransaction>,
    read_table: Option<ReadOnlyTable<&'static [u8], &'static [u8]>>,
    _dir: TempDir,
}

impl RedbEngine {
    fn db(&self) -> Result<&Database, EngineError> {
        self.db
            .as_ref()
            .ok_or_else(|| EngineError::new("redb is closed"))
    }

    fn active_write_txn(&self) -> Result<&WriteTransaction, EngineError> {
        self.write_txn
            .as_ref()
            .ok_or_else(|| EngineError::new("redb write transaction is not open"))
    }
}

impl NativeEngine for RedbEngine {
    const NAME: &'static str = "redb";

    fn open_fresh() -> Result<Self, EngineError> {
        let dir = tempfile::tempdir().map_err(eng)?;
        let path = dir.path().join("bench.redb");
        let db = Database::create(&path).map_err(eng)?;
        Ok(Self {
            path,
            db: Some(db),
            write_txn: None,
            read_table: None,
            _dir: dir,
        })
    }
}

impl BenchEngine for RedbEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        let mut table = self
            .active_write_txn()?
            .open_table(REDB_TABLE)
            .map_err(eng)?;
        table.insert(key, value).map_err(eng)?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError> {
        if let Some(table) = &self.read_table {
            return Ok(table
                .get(key)
                .map_err(eng)?
                .and_then(|value| value.value().first().copied()));
        }

        let table = self
            .active_write_txn()?
            .open_table(REDB_TABLE)
            .map_err(eng)?;
        Ok(table
            .get(key)
            .map_err(eng)?
            .and_then(|value| value.value().first().copied()))
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        let mut table = self
            .active_write_txn()?
            .open_table(REDB_TABLE)
            .map_err(eng)?;
        table.remove(key).map_err(eng)?;
        Ok(())
    }

    fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError> {
        if limit == 0 {
            return Ok(0);
        }

        let count_rows = |table: &ReadOnlyTable<&'static [u8], &'static [u8]>| {
            let mut rows = 0usize;
            for item in table.range(lo..hi).map_err(eng)? {
                item.map_err(eng)?;
                rows += 1;
                if rows == limit {
                    break;
                }
            }
            Ok(rows)
        };

        if let Some(table) = &self.read_table {
            return count_rows(table);
        }

        let table = self
            .active_write_txn()?
            .open_table(REDB_TABLE)
            .map_err(eng)?;
        let mut rows = 0usize;
        for item in table.range(lo..hi).map_err(eng)? {
            item.map_err(eng)?;
            rows += 1;
            if rows == limit {
                break;
            }
        }
        Ok(rows)
    }

    fn begin_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError> {
        if kind.is_write() {
            if self.write_txn.is_some() {
                return Err(EngineError::new("redb write transaction is already open"));
            }
            let mut txn = self.db()?.begin_write().map_err(eng)?;
            txn.set_durability(Durability::None).map_err(eng)?;
            self.write_txn = Some(txn);
        } else {
            if self.read_table.is_some() {
                return Err(EngineError::new("redb read table is already open"));
            }
            let txn = self.db()?.begin_read().map_err(eng)?;
            self.read_table = Some(txn.open_table(REDB_TABLE).map_err(eng)?);
        }
        Ok(())
    }

    fn end_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError> {
        if kind.is_write() {
            let txn = self
                .write_txn
                .take()
                .ok_or_else(|| EngineError::new("redb write transaction is not open"))?;
            txn.commit().map_err(eng)?;
        } else {
            self.read_table = None;
        }
        Ok(())
    }

    async fn reopen(&mut self) -> Result<(), EngineError> {
        self.write_txn = None;
        self.read_table = None;
        self.db = None;
        self.db = Some(Database::open(&self.path).map_err(eng)?);
        Ok(())
    }
}
