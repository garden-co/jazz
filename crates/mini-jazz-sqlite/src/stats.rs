use crate::schema::{current_table, history_table, SchemaDef};
use crate::tx;
use crate::types::{StorageFileBytes, StorageStats};
use crate::Result;
use rusqlite::{params, Connection};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn collect(conn: &Connection, schema: &SchemaDef) -> Result<StorageStats> {
    let mut history_rows = 0;
    let mut current_rows = 0;
    for table in schema.tables() {
        history_rows += count_rows(conn, &history_table(&table.name))?;
        current_rows += count_rows(conn, &current_table(&table.name))?;
    }
    let rejected_transactions: i64 = conn.query_row(
        "SELECT COUNT(*) FROM jazz_tx WHERE outcome = ?",
        params![tx::OUTCOME_REJECTED],
        |row| row.get(0),
    )?;
    let mut stmt = conn.prepare("SELECT tx_id, tx_num FROM jazz_tx")?;
    let tx_nums = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<std::result::Result<BTreeMap<_, _>, _>>()?;
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let file_sizes = sqlite_file_sizes(conn)?;
    Ok(StorageStats::new(
        history_rows,
        current_rows,
        rejected_transactions,
        page_count,
        page_size,
        file_sizes,
        tx_nums,
    ))
}

fn count_rows(conn: &Connection, table: &str) -> Result<i64> {
    Ok(
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?,
    )
}

fn sqlite_file_sizes(conn: &Connection) -> Result<StorageFileBytes> {
    let path: String = conn.query_row("PRAGMA database_list", [], |row| row.get(2))?;
    if path.is_empty() {
        return Ok(StorageFileBytes {
            main: 0,
            wal: 0,
            shm: 0,
        });
    }
    let path = PathBuf::from(path);
    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    let shm_path = PathBuf::from(format!("{}-shm", path.display()));
    Ok(StorageFileBytes {
        main: file_len(&path),
        wal: file_len(&wal_path),
        shm: file_len(&shm_path),
    })
}

fn file_len(path: &Path) -> i64 {
    fs::metadata(path)
        .map(|metadata| metadata.len() as i64)
        .unwrap_or(0)
}
