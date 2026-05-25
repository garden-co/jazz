use crate::Result;
use rusqlite::Connection;
use std::path::PathBuf;

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
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(conn)
}
