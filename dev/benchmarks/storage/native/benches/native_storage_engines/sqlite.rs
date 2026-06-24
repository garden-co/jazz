use std::path::{Path, PathBuf};

use bench_core::{BenchEngine, EngineError, PhaseKind};
use tempfile::TempDir;

use super::{CACHE_BYTES, NativeEngine, eng};

const SQLITE_CACHE_KIB: i64 = -(CACHE_BYTES as i64 / 1024);

pub(super) struct SqliteEngine {
    path: PathBuf,
    conn: Option<rusqlite::Connection>,
    _dir: TempDir,
}

impl SqliteEngine {
    fn conn(&self) -> Result<&rusqlite::Connection, EngineError> {
        self.conn
            .as_ref()
            .ok_or_else(|| EngineError::new("sqlite connection is closed"))
    }
}

impl NativeEngine for SqliteEngine {
    const NAME: &'static str = "rusqlite";

    fn open_fresh() -> Result<Self, EngineError> {
        let dir = tempfile::tempdir().map_err(eng)?;
        let path = dir.path().join("bench.sqlite");
        let conn = open_sqlite_conn(&path)?;
        Ok(Self {
            path,
            conn: Some(conn),
            _dir: dir,
        })
    }
}

impl BenchEngine for SqliteEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        self.conn()?
            .prepare_cached("INSERT OR REPLACE INTO kv(k, v) VALUES(?1, ?2)")
            .map_err(eng)?
            .execute(rusqlite::params![key, value])
            .map_err(eng)?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError> {
        let mut stmt = self
            .conn()?
            .prepare_cached("SELECT v FROM kv WHERE k = ?1")
            .map_err(eng)?;
        match stmt.query_row(rusqlite::params![key], |row| row.get::<_, Vec<u8>>(0)) {
            Ok(value) => Ok(value.first().copied()),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(eng(error)),
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        self.conn()?
            .prepare_cached("DELETE FROM kv WHERE k = ?1")
            .map_err(eng)?
            .execute(rusqlite::params![key])
            .map_err(eng)?;
        Ok(())
    }

    fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError> {
        let mut stmt = self
            .conn()?
            .prepare_cached("SELECT 1 FROM kv WHERE k >= ?1 AND k < ?2 ORDER BY k LIMIT ?3")
            .map_err(eng)?;
        let rows = stmt
            .query_map(rusqlite::params![lo, hi, limit as i64], |_| Ok(()))
            .map_err(eng)?
            .count();
        Ok(rows)
    }

    fn begin_phase(&mut self, _kind: PhaseKind) -> Result<(), EngineError> {
        self.conn()?.execute_batch("BEGIN").map_err(eng)
    }

    fn end_phase(&mut self, _kind: PhaseKind) -> Result<(), EngineError> {
        self.conn()?.execute_batch("COMMIT").map_err(eng)
    }

    async fn reopen(&mut self) -> Result<(), EngineError> {
        self.conn = None;
        self.conn = Some(open_sqlite_conn(&self.path)?);
        Ok(())
    }
}

fn open_sqlite_conn(path: &Path) -> Result<rusqlite::Connection, EngineError> {
    let conn = rusqlite::Connection::open(path).map_err(eng)?;
    conn.execute_batch(&format!(
        "PRAGMA page_size = 16384;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA cache_size = {SQLITE_CACHE_KIB};
         PRAGMA temp_store = MEMORY;
         PRAGMA busy_timeout = 5000;
         CREATE TABLE IF NOT EXISTS kv(
             k BLOB PRIMARY KEY,
             v BLOB NOT NULL
         ) WITHOUT ROWID;",
    ))
    .map_err(eng)?;
    Ok(conn)
}
