use crate::Result;
use rusqlite::Connection;
use std::path::PathBuf;

pub const STORAGE_FORMAT_VERSION: i64 = 7;

#[derive(Clone, Debug)]
pub enum Storage {
    Memory,
    File(PathBuf),
}

pub(crate) fn open(storage: Storage) -> Result<Connection> {
    let durable = matches!(storage, Storage::File(_));
    let conn = match storage {
        Storage::Memory => Connection::open_in_memory()?,
        Storage::File(path) => Connection::open(path)?,
    };
    apply_tuning_pragmas(&conn, durable)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    ensure_storage_version(&conn)?;
    Ok(conn)
}

pub(crate) fn storage_version(conn: &Connection) -> Result<i64> {
    Ok(conn.pragma_query_value(None, "user_version", |row| row.get(0))?)
}

fn apply_tuning_pragmas(conn: &Connection, durable: bool) -> Result<()> {
    if durable {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
    }
    Ok(())
}

fn ensure_storage_version(conn: &Connection) -> Result<()> {
    let version = storage_version(conn)?;
    if version == 0 {
        conn.pragma_update(None, "user_version", STORAGE_FORMAT_VERSION)?;
        return Ok(());
    }
    if version != STORAGE_FORMAT_VERSION {
        return Err(crate::Error::new(format!(
            "unsupported storage format version {version}"
        )));
    }
    Ok(())
}
