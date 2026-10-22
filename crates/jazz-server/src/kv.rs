//! A small ordered key-value store on SQLite for the server's own
//! administrative state (the schema catalogue and the account journal).
//!
//! Each store is one SQLite file with one table. Writes commit with
//! `synchronous = FULL`, so a returned write is durable and `flush` has
//! nothing left to do.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, params};

/// Errors are reported as text: callers wrap them in their own error types.
pub(crate) type KvResult<T> = Result<T, String>;

pub(crate) struct SqliteKv {
    conn: Mutex<Connection>,
    /// Tests: the next conditional write commits and then reports an error,
    /// the ambiguous outcome a failed flush has.
    #[cfg(test)]
    fail_next_write: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl SqliteKv {
    /// Open (creating) the store at `path`.
    pub(crate) fn open(path: &Path) -> KvResult<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("create {}: {error}", parent.display()))?;
        }
        let conn = Connection::open(path).map_err(sql)?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;")
            .map_err(sql)?;
        Self::with(conn)
    }

    /// A store that lives only as long as this value.
    pub(crate) fn memory() -> KvResult<Self> {
        Self::with(Connection::open_in_memory().map_err(sql)?)
    }

    fn with(conn: Connection) -> KvResult<Self> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID",
        )
        .map_err(sql)?;
        Ok(Self {
            conn: Mutex::new(conn),
            #[cfg(test)]
            fail_next_write: Default::default(),
        })
    }

    pub(crate) fn get(&self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        self.conn
            .lock()
            .expect("kv lock")
            .prepare_cached("SELECT v FROM kv WHERE k = ?1")
            .and_then(|mut statement| statement.query_row([key], |row| row.get(0)).optional())
            .map_err(sql)
    }

    /// Write `value` unless `key` exists; returns the existing value if so.
    pub(crate) fn put_if_absent(&self, key: &[u8], value: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let mut conn = self.conn.lock().expect("kv lock");
        let tx = conn.transaction().map_err(sql)?;
        let existing: Option<Vec<u8>> = tx
            .query_row("SELECT v FROM kv WHERE k = ?1", [key], |row| row.get(0))
            .optional()
            .map_err(sql)?;
        if existing.is_none() {
            tx.execute("INSERT INTO kv (k, v) VALUES (?1, ?2)", params![key, value])
                .map_err(sql)?;
        }
        tx.commit().map_err(sql)?;
        #[cfg(test)]
        if self
            .fail_next_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err("injected failure after the write committed".to_owned());
        }
        Ok(existing)
    }

    /// Tests: the switch that fails the next conditional write.
    #[cfg(test)]
    pub(crate) fn failpoint(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        std::sync::Arc::clone(&self.fail_next_write)
    }

    /// Write every pair in one transaction: all or none.
    pub(crate) fn write_many(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> KvResult<()> {
        let mut conn = self.conn.lock().expect("kv lock");
        let tx = conn.transaction().map_err(sql)?;
        {
            let mut statement = tx
                .prepare_cached("INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)")
                .map_err(sql)?;
            for (key, value) in pairs {
                statement.execute(params![key, value]).map_err(sql)?;
            }
        }
        tx.commit().map_err(sql)
    }

    #[cfg(test)]
    pub(crate) fn delete(&self, key: &[u8]) -> KvResult<()> {
        self.conn
            .lock()
            .expect("kv lock")
            .execute("DELETE FROM kv WHERE k = ?1", [key])
            .map(|_| ())
            .map_err(sql)
    }

    /// Every pair whose key starts with `prefix`, in key order.
    pub(crate) fn prefix(&self, prefix: &[u8]) -> KvResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let conn = self.conn.lock().expect("kv lock");
        let mut statement = conn
            .prepare_cached("SELECT k, v FROM kv WHERE k >= ?1 ORDER BY k")
            .map_err(sql)?;
        let rows = statement
            .query_map([prefix], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(sql)?;
        let mut out = Vec::new();
        for row in rows {
            let (key, value): (Vec<u8>, Vec<u8>) = row.map_err(sql)?;
            if !key.starts_with(prefix) {
                break;
            }
            out.push((key, value));
        }
        Ok(out)
    }
}

fn sql(error: rusqlite::Error) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_are_atomic_ordered_and_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kv.sqlite");
        {
            let kv = SqliteKv::open(&path).unwrap();
            kv.write_many(&[
                (b"a:2".to_vec(), b"two".to_vec()),
                (b"a:1".to_vec(), b"one".to_vec()),
            ])
            .unwrap();
            assert_eq!(
                kv.put_if_absent(b"a:1", b"x").unwrap(),
                Some(b"one".to_vec())
            );
            assert_eq!(kv.put_if_absent(b"b:1", b"b").unwrap(), None);
        }
        let kv = SqliteKv::open(&path).unwrap();
        assert_eq!(
            kv.prefix(b"a:").unwrap(),
            vec![
                (b"a:1".to_vec(), b"one".to_vec()),
                (b"a:2".to_vec(), b"two".to_vec())
            ]
        );
        assert_eq!(kv.get(b"b:1").unwrap(), Some(b"b".to_vec()));
        assert_eq!(kv.get(b"c").unwrap(), None);
    }
}
