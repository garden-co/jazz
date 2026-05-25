use crate::{Error, Result};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) const KIND_DATA: i64 = 1;
pub(crate) const MODE_MERGEABLE: i64 = 1;
pub(crate) const OUTCOME_PENDING: i64 = 1;
pub(crate) const OUTCOME_REJECTED: i64 = 3;

pub(crate) fn ensure_node(conn: &Connection, node_id: &str) -> Result<i64> {
    conn.execute(
        "INSERT OR IGNORE INTO jazz_node (node_id) VALUES (?)",
        params![node_id],
    )?;
    Ok(conn.query_row(
        "SELECT node_num FROM jazz_node WHERE node_id = ?",
        params![node_id],
        |row| row.get(0),
    )?)
}

pub(crate) fn create_tx(conn: &Connection, node_num: i64, now: i64) -> Result<(i64, String)> {
    let next_epoch = conn
        .query_row(
            "SELECT COALESCE(MAX(local_epoch), 0) + 1 FROM jazz_tx WHERE node_num = ?",
            params![node_num],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(1);
    let tx_id = format!("tx-{node_num}-{next_epoch}");
    conn.execute(
        "INSERT INTO jazz_tx
          (tx_id, node_num, local_epoch, kind, conflict_mode, outcome, created_at, metadata_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, '{}')",
        params![
            tx_id,
            node_num,
            next_epoch,
            KIND_DATA,
            MODE_MERGEABLE,
            OUTCOME_PENDING,
            now
        ],
    )?;
    let tx_num = conn.last_insert_rowid();
    Ok((tx_num, tx_id))
}

pub(crate) fn tx_num(conn: &Connection, tx_id: &str) -> Result<i64> {
    conn.query_row(
        "SELECT tx_num FROM jazz_tx WHERE tx_id = ?",
        params![tx_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| Error::new(format!("unknown transaction {tx_id}")))
}

pub(crate) fn reject(conn: &Connection, tx_id: &str, code: &str) -> Result<i64> {
    let tx_num = tx_num(conn, tx_id)?;
    conn.execute(
        "UPDATE jazz_tx SET outcome = ? WHERE tx_num = ?",
        params![OUTCOME_REJECTED, tx_num],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO jazz_tx_rejection (tx_num, code, detail_json)
         VALUES (?, ?, '{}')",
        params![tx_num, code],
    )?;
    Ok(tx_num)
}
