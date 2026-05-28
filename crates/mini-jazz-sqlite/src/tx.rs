use crate::{Error, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

pub(crate) const KIND_DATA: i64 = 1;
pub(crate) const MODE_MERGEABLE: i64 = 1;
pub(crate) const MODE_EXCLUSIVE: i64 = 2;
pub(crate) const OUTCOME_PENDING: i64 = 1;
pub(crate) const OUTCOME_ACCEPTED: i64 = 2;
pub(crate) const OUTCOME_REJECTED: i64 = 3;
pub(crate) const TIER_EDGE: i64 = 2;
pub(crate) const TIER_GLOBAL: i64 = 3;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PackedWrite(pub(crate) i64, pub(crate) i64, pub(crate) i64);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PackedRead(
    pub(crate) i64,
    pub(crate) i64,
    pub(crate) i64,
    pub(crate) Option<i64>,
);

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
    node_id: &str,
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
    let tx_id = format!("tx-{node_id}-{next_epoch}");
    conn.execute(
        "INSERT INTO jazz_tx
          (node_num, local_epoch, global_epoch, kind, conflict_mode, outcome, created_at, metadata_json, writes_json, reads_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, NULL, '[]', '[]')",
        params![
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

pub(crate) fn append_write(
    conn: &Connection,
    tx_num: i64,
    table_num: i64,
    row_num: i64,
    op: i64,
) -> Result<()> {
    let mut writes = packed_writes(conn, tx_num)?;
    let write = PackedWrite(table_num, row_num, op);
    if !writes.contains(&write) {
        writes.push(write);
    }
    let writes_json = serde_json::to_string(&writes).map_err(|err| Error::new(err.to_string()))?;
    conn.execute(
        "UPDATE jazz_tx SET writes_json = ? WHERE tx_num = ?",
        params![writes_json, tx_num],
    )?;
    Ok(())
}

pub(crate) fn append_read(
    conn: &Connection,
    tx_num: i64,
    table_num: i64,
    row_num: i64,
    reason: i64,
    observed_tx_num: Option<i64>,
) -> Result<()> {
    let read = PackedRead(table_num, row_num, reason, observed_tx_num);
    let implicit = implicit_previous_reads(conn, tx_num)?;
    let reads_json = conn.query_row(
        "SELECT reads_json FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| row.get::<_, Option<String>>(0),
    )?;
    let mut reads = match reads_json {
        Some(json) => serde_json::from_str::<Vec<PackedRead>>(&json)
            .map_err(|err| Error::new(err.to_string()))?,
        None => implicit.clone(),
    };
    if !reads.contains(&read) {
        reads.push(read);
    }
    let next_json = if !implicit.is_empty() && same_read_set(&reads, &implicit) {
        None
    } else {
        Some(serde_json::to_string(&reads).map_err(|err| Error::new(err.to_string()))?)
    };
    conn.execute(
        "UPDATE jazz_tx SET reads_json = ? WHERE tx_num = ?",
        params![next_json, tx_num],
    )?;
    Ok(())
}

pub(crate) fn fill_observed_read(
    conn: &Connection,
    tx_num: i64,
    table_num: i64,
    row_num: i64,
    observed_tx_num: i64,
) -> Result<()> {
    let reads_json = conn.query_row(
        "SELECT reads_json FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| row.get::<_, Option<String>>(0),
    )?;
    let mut reads = match reads_json {
        Some(json) => serde_json::from_str::<Vec<PackedRead>>(&json)
            .map_err(|err| Error::new(err.to_string()))?,
        None => implicit_previous_reads(conn, tx_num)?,
    };
    for read in &mut reads {
        if read.0 == table_num && read.1 == row_num && read.3.is_none() {
            read.3 = Some(observed_tx_num);
        }
    }
    let reads_json = serde_json::to_string(&reads).map_err(|err| Error::new(err.to_string()))?;
    conn.execute(
        "UPDATE jazz_tx SET reads_json = ? WHERE tx_num = ?",
        params![reads_json, tx_num],
    )?;
    Ok(())
}

fn packed_writes(conn: &Connection, tx_num: i64) -> Result<Vec<PackedWrite>> {
    let json = conn.query_row(
        "SELECT writes_json FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| row.get::<_, String>(0),
    )?;
    serde_json::from_str(&json).map_err(|err| Error::new(err.to_string()))
}

fn implicit_previous_reads(conn: &Connection, tx_num: i64) -> Result<Vec<PackedRead>> {
    let previous_tx_num = conn
        .query_row(
            "SELECT previous.tx_num
             FROM jazz_tx tx
             JOIN jazz_tx previous
               ON previous.node_num = tx.node_num
              AND previous.local_epoch = tx.local_epoch - 1
             WHERE tx.tx_num = ?",
            params![tx_num],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    let Some(previous_tx_num) = previous_tx_num else {
        return Ok(Vec::new());
    };
    Ok(packed_writes(conn, tx_num)?
        .into_iter()
        .map(|write| PackedRead(write.0, write.1, 2, Some(previous_tx_num)))
        .collect())
}

fn same_read_set(left: &[PackedRead], right: &[PackedRead]) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_by_key(|read| (read.0, read.1, read.2, read.3));
    right.sort_by_key(|read| (read.0, read.1, read.2, read.3));
    left == right
}

pub(crate) fn tx_num(conn: &Connection, tx_id: &str) -> Result<i64> {
    let (node_id, local_epoch) = parse_tx_id(tx_id)?;
    conn.query_row(
        "SELECT tx.tx_num
         FROM jazz_tx_public tx
         WHERE tx.node_id = ? AND tx.local_epoch = ?",
        params![node_id, local_epoch],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| Error::new(format!("unknown transaction {tx_id}")))
}

pub(crate) fn tx_id_for_num(conn: &Connection, tx_num: i64) -> Result<String> {
    conn.query_row(
        "SELECT tx_id FROM jazz_tx_public WHERE tx_num = ?",
        params![tx_num],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| Error::new(format!("unknown transaction num {tx_num}")))
}

fn parse_tx_id(tx_id: &str) -> Result<(&str, i64)> {
    let rest = tx_id
        .strip_prefix("tx-")
        .ok_or_else(|| Error::new(format!("invalid transaction id {tx_id}")))?;
    let Some((node_id, epoch)) = rest.rsplit_once('-') else {
        return Err(Error::new(format!("invalid transaction id {tx_id}")));
    };
    let local_epoch = epoch
        .parse::<i64>()
        .map_err(|_| Error::new(format!("invalid transaction id {tx_id}")))?;
    Ok((node_id, local_epoch))
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
        "INSERT OR REPLACE INTO jazz_tx_rejection (tx_num, code, detail_json)
         VALUES (?, ?, ?)",
        params![tx_num, code, detail_json],
    )?;
    Ok(tx_num)
}

pub(crate) fn accept_global(conn: &Connection, tx_id: &str, global_epoch: i64) -> Result<i64> {
    let tx_num = tx_num(conn, tx_id)?;
    conn.execute(
        "UPDATE jazz_tx SET outcome = MAX(outcome, ?), global_epoch = ? WHERE tx_num = ?",
        params![OUTCOME_ACCEPTED, global_epoch, tx_num],
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
