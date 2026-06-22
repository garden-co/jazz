//! The SQLite side of the benchmark: a [`BenchEngine`] over an in-process
//! `rusqlite` connection backed by the OPFS sahpool VFS. All workload, timing,
//! and checksum logic lives in `bench-core`; this file only maps the contract
//! onto SQLite, keeping its native idioms (one transaction per phase, cached
//! prepared statements) so the comparison stays faithful.

use bench_core::{BenchEngine, EngineError, PhaseKind};
use rusqlite::Connection;
use sqlite_wasm_rs as ffi; // force-links the sqlite3 C symbols
use sqlite_wasm_vfs::sahpool::{OpfsSAHPoolCfg, install as install_opfs_sahpool};

const DB_PATH: &str = "sqlite.db";

fn eng<E: ToString>(e: E) -> EngineError {
    EngineError::new(e.to_string())
}

fn open_conn() -> Result<Connection, EngineError> {
    let conn = Connection::open(DB_PATH).map_err(eng)?;
    conn.execute_batch(
        "PRAGMA page_size=16384; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-32768; PRAGMA temp_store=MEMORY;",
    )
    .map_err(eng)?;
    Ok(conn)
}

pub struct SqliteEngine {
    // `Option` so a cold reopen can drop (close) the connection before opening a
    // fresh one, giving a cold page cache while the data persists in OPFS.
    conn: Option<Connection>,
}

impl SqliteEngine {
    /// Install the OPFS VFS and open a fresh, empty `WITHOUT ROWID` k/v table.
    pub async fn open() -> Result<Self, EngineError> {
        install_opfs_sahpool::<ffi::WasmOsCallback>(&OpfsSAHPoolCfg::default(), true)
            .await
            .map_err(|e| EngineError::new(format!("install sahpool: {e:?}")))?;

        let conn = open_conn()?;
        conn.execute_batch(
            "DROP TABLE IF EXISTS kv; CREATE TABLE kv(k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;",
        )
        .map_err(eng)?;
        Ok(Self { conn: Some(conn) })
    }

    fn conn(&self) -> &Connection {
        self.conn.as_ref().expect("sqlite engine is open")
    }
}

impl BenchEngine for SqliteEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        self.conn()
            .prepare_cached("INSERT OR REPLACE INTO kv(k,v) VALUES(?1,?2)")
            .map_err(eng)?
            .execute((key, value))
            .map_err(eng)?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError> {
        let mut stmt = self
            .conn()
            .prepare_cached("SELECT v FROM kv WHERE k=?1")
            .map_err(eng)?;
        match stmt.query_row([key], |row| row.get::<_, Vec<u8>>(0)) {
            Ok(v) => Ok(Some(v.first().copied().unwrap_or(0))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(eng(e)),
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        self.conn()
            .prepare_cached("DELETE FROM kv WHERE k=?1")
            .map_err(eng)?
            .execute([key])
            .map_err(eng)?;
        Ok(())
    }

    fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError> {
        let mut stmt = self
            .conn()
            .prepare_cached("SELECT v FROM kv WHERE k>=?1 AND k<?2 ORDER BY k LIMIT ?3")
            .map_err(eng)?;
        let rows = stmt
            .query_map(rusqlite::params![lo, hi, limit as i64], |_| Ok(()))
            .map_err(eng)?
            .count();
        Ok(rows)
    }

    fn begin_phase(&mut self, _kind: PhaseKind) -> Result<(), EngineError> {
        self.conn().execute_batch("BEGIN").map_err(eng)
    }

    fn end_phase(&mut self, _kind: PhaseKind) -> Result<(), EngineError> {
        self.conn().execute_batch("COMMIT").map_err(eng)
    }

    async fn reopen(&mut self) -> Result<(), EngineError> {
        // Drop (close) the current connection before reopening; the OPFS-backed
        // database file persists, but the new connection starts cache-cold.
        self.conn = None;
        self.conn = Some(open_conn()?);
        Ok(())
    }
}
