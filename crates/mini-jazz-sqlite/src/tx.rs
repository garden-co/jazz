use crate::{Error, Result};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) const KIND_DATA: i64 = 1;
pub(crate) const MODE_MERGEABLE: i64 = 1;
pub(crate) const MODE_EXCLUSIVE: i64 = 2;
pub(crate) const OUTCOME_PENDING: i64 = 1;
pub(crate) const OUTCOME_ACCEPTED: i64 = 2;
pub(crate) const OUTCOME_REJECTED: i64 = 3;
pub(crate) const TIER_EDGE: i64 = 2;
pub(crate) const TIER_GLOBAL: i64 = 3;

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

pub(crate) fn create_tx(
    conn: &Connection,
    node_num: i64,
    node_id: &str,
    now: i64,
) -> Result<(i64, String)> {
    create_tx_with_options(
        conn,
        node_num,
        node_id,
        now,
        MODE_MERGEABLE,
        OUTCOME_PENDING,
        None,
    )
}

pub(crate) fn create_tx_with_options(
    conn: &Connection,
    node_num: i64,
    _node_id: &str,
    now: i64,
    conflict_mode: i64,
    outcome: i64,
    global_epoch: Option<i64>,
) -> Result<(i64, String)> {
    let next_epoch = conn
        .query_row(
            "SELECT COALESCE(MAX(local_epoch), 0) + 1 FROM jazz_tx WHERE node_num = ?",
            params![node_num],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(1);
    let tx_id = uuid::Uuid::now_v7().to_string();
    conn.execute(
        "INSERT INTO jazz_tx
          (tx_id, node_num, local_epoch, global_epoch, kind, conflict_mode, outcome, created_at, metadata_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}')",
        params![
            tx_id,
            node_num,
            next_epoch,
            global_epoch,
            KIND_DATA,
            conflict_mode,
            outcome,
            now
        ],
    )?;
    let tx_num = conn.last_insert_rowid();
    if let Some(global_epoch) = global_epoch {
        conn.execute(
            "INSERT OR REPLACE INTO jazz_tx_receipt
             (tx_num, tier, observed_at, receipt_json)
             VALUES (?, ?, ?, '{}')",
            params![tx_num, TIER_GLOBAL, global_epoch],
        )?;
    }
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
    reject_with_detail_json(conn, tx_id, code, "null")
}

pub(crate) fn reject_with_detail_json(
    conn: &Connection,
    tx_id: &str,
    code: &str,
    detail_json: &str,
) -> Result<i64> {
    let tx_num = tx_num(conn, tx_id)?;
    conn.execute(
        "UPDATE jazz_tx SET outcome = ? WHERE tx_num = ?",
        params![OUTCOME_REJECTED, tx_num],
    )?;
    conn.execute(
        "INSERT INTO jazz_tx_rejection (tx_num, code, detail_json)
         VALUES (?, ?, ?)
         ON CONFLICT(tx_num) DO UPDATE SET
           code = excluded.code,
           detail_json = CASE
             WHEN excluded.detail_json = 'null' AND jazz_tx_rejection.detail_json != 'null' THEN jazz_tx_rejection.detail_json
             ELSE excluded.detail_json
           END",
        params![tx_num, code, detail_json],
    )?;
    Ok(tx_num)
}

pub(crate) fn accept_global(conn: &Connection, tx_id: &str, global_epoch: i64) -> Result<i64> {
    let tx_num = tx_num(conn, tx_id)?;
    conn.execute(
        "UPDATE jazz_tx
         SET outcome = MAX(outcome, ?),
             global_epoch = CASE
               WHEN global_epoch IS NULL THEN ?
               ELSE MAX(global_epoch, ?)
             END
         WHERE tx_num = ?",
        params![OUTCOME_ACCEPTED, global_epoch, global_epoch, tx_num],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO jazz_tx_receipt
         (tx_num, tier, observed_at, receipt_json)
         VALUES (?, ?, ?, '{}')",
        params![tx_num, TIER_GLOBAL, global_epoch],
    )?;
    Ok(tx_num)
}

pub(crate) fn accept_edge(conn: &Connection, tx_id: &str, observed_at: i64) -> Result<i64> {
    let tx_num = tx_num(conn, tx_id)?;
    conn.execute(
        "UPDATE jazz_tx SET outcome = MAX(outcome, ?) WHERE tx_num = ?",
        params![OUTCOME_ACCEPTED, tx_num],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO jazz_tx_receipt
         (tx_num, tier, observed_at, receipt_json)
         VALUES (?, ?, ?, '{}')",
        params![tx_num, TIER_EDGE, observed_at],
    )?;
    Ok(tx_num)
}
