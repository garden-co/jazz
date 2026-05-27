use crate::Result;
use rusqlite::Connection;
use std::env;
use std::path::PathBuf;

pub const STORAGE_FORMAT_VERSION: i64 = 6;

#[derive(Clone, Debug)]
pub enum Storage {
    Memory,
    File(PathBuf),
}

pub(crate) fn open(storage: Storage) -> Result<Connection> {
    let conn = match storage {
        Storage::Memory => Connection::open_in_memory()?,
        Storage::File(path) => Connection::open(path)?,
    };
    apply_tuning_pragmas(&conn)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    ensure_storage_version(&conn)?;
    Ok(conn)
}

pub(crate) fn storage_version(conn: &Connection) -> Result<i64> {
    Ok(conn.pragma_query_value(None, "user_version", |row| row.get(0))?)
}

fn apply_tuning_pragmas(conn: &Connection) -> Result<()> {
    if let Some(page_size) = env_i64("MINI_JAZZ_SQLITE_PAGE_SIZE")? {
        conn.pragma_update(None, "page_size", page_size)?;
    }
    if let Some(cache_size) = env_i64("MINI_JAZZ_SQLITE_CACHE_SIZE")? {
        conn.pragma_update(None, "cache_size", cache_size)?;
    }
    if let Some(journal_mode) = env_one_of(
        "MINI_JAZZ_SQLITE_JOURNAL_MODE",
        &["DELETE", "WAL", "MEMORY", "OFF"],
    )? {
        conn.pragma_update(None, "journal_mode", journal_mode)?;
    }
    if let Some(synchronous) = env_one_of(
        "MINI_JAZZ_SQLITE_SYNCHRONOUS",
        &["EXTRA", "FULL", "NORMAL", "OFF"],
    )? {
        conn.pragma_update(None, "synchronous", synchronous)?;
    }
    if let Some(temp_store) = env_one_of(
        "MINI_JAZZ_SQLITE_TEMP_STORE",
        &["DEFAULT", "FILE", "MEMORY"],
    )? {
        conn.pragma_update(None, "temp_store", temp_store)?;
    }
    Ok(())
}

fn env_i64(name: &str) -> Result<Option<i64>> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };
    value
        .parse::<i64>()
        .map(Some)
        .map_err(|err| crate::Error::new(format!("invalid {name}: {err}")))
}

fn env_one_of(name: &str, allowed: &[&str]) -> Result<Option<String>> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };
    let normalized = value.to_ascii_uppercase();
    if allowed.contains(&normalized.as_str()) {
        return Ok(Some(normalized));
    }
    Err(crate::Error::new(format!(
        "invalid {name}: expected one of {}",
        allowed.join(", ")
    )))
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
