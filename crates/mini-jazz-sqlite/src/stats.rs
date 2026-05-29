use crate::schema::{current_table, history_table, SchemaDef};
use crate::tx;
use crate::types::{StorageFileBytes, StorageHistoryCounts, StoragePageBytes, StorageStats};
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
    let history_blocks = count_rows(conn, "history_blocks")?;
    let sealed_history_rows: i64 = conn.query_row(
        "SELECT COALESCE(SUM(row_count), 0) FROM history_blocks",
        [],
        |row| row.get(0),
    )?;
    let (history_block_uncompressed_bytes, history_block_compressed_bytes): (i64, i64) = conn
        .query_row(
            "SELECT COALESCE(SUM(uncompressed_bytes), 0),
                    COALESCE(SUM(compressed_bytes), 0)
             FROM history_blocks",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
    let open_rejected_transactions: i64 = conn.query_row(
        "SELECT COUNT(*) FROM jazz_tx WHERE outcome = ?",
        params![tx::OUTCOME_REJECTED],
        |row| row.get(0),
    )?;
    let sealed_rejected_transactions: i64 = conn.query_row(
        "SELECT COALESCE(SUM(tx_count), 0) FROM history_blocks WHERE block_kind = 2",
        [],
        |row| row.get(0),
    )?;
    let rejected_transactions = open_rejected_transactions + sealed_rejected_transactions;
    let mut stmt = conn.prepare("SELECT tx_id, tx_num FROM jazz_tx_public")?;
    let tx_nums = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<std::result::Result<BTreeMap<_, _>, _>>()?;
    let page_bytes = StoragePageBytes {
        count: conn.query_row("PRAGMA page_count", [], |row| row.get(0))?,
        size: conn.query_row("PRAGMA page_size", [], |row| row.get(0))?,
        freelist: conn.query_row("PRAGMA freelist_count", [], |row| row.get(0))?,
        object_bytes: table_page_bytes(conn)?,
    };
    let file_sizes = sqlite_file_sizes(conn)?;
    Ok(StorageStats::new(
        StorageHistoryCounts {
            open_rows: history_rows,
            sealed_rows: sealed_history_rows,
            blocks: history_blocks,
            block_uncompressed_bytes: history_block_uncompressed_bytes,
            block_compressed_bytes: history_block_compressed_bytes,
        },
        current_rows,
        rejected_transactions,
        page_bytes,
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

fn table_page_bytes(conn: &Connection) -> Result<BTreeMap<String, i64>> {
    let mut stmt = match conn.prepare("SELECT name, SUM(pgsize) FROM dbstat GROUP BY name") {
        Ok(stmt) => stmt,
        Err(_) => return Ok(BTreeMap::new()),
    };
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
    })?;
    let mut bytes = BTreeMap::new();
    for row in rows {
        let (name, size) = row?;
        bytes.insert(name, size.unwrap_or(0));
    }
    Ok(bytes)
}
