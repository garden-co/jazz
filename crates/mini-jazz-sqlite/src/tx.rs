use crate::value::{bytes_to_hex, WireValue};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PackedWrite(pub(crate) i64, pub(crate) i64, pub(crate) i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PackedRead(
    pub(crate) i64,
    pub(crate) i64,
    pub(crate) i64,
    pub(crate) Option<i64>,
);

pub(crate) fn ensure_node(conn: &Connection, node_id: &str) -> Result<i64> {
    conn.prepare_cached("INSERT OR IGNORE INTO jazz_node (node_id) VALUES (?)")?
        .execute(params![node_id])?;
    let mut stmt = conn.prepare_cached("SELECT node_num FROM jazz_node WHERE node_id = ?")?;
    Ok(stmt.query_row(params![node_id], |row| row.get(0))?)
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

pub(crate) fn next_local_epoch(conn: &Connection, node_num: i64) -> Result<i64> {
    Ok(conn
        .prepare_cached("SELECT COALESCE(MAX(local_epoch), 0) + 1 FROM jazz_tx WHERE node_num = ?")?
        .query_row(params![node_num], |row| row.get::<_, i64>(0))
        .unwrap_or(1))
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
    let local_epoch = next_local_epoch(conn, node_num)?;
    create_tx_at_local_epoch(
        conn,
        node_num,
        node_id,
        local_epoch,
        now,
        conflict_mode,
        outcome,
        global_epoch,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_tx_at_local_epoch(
    conn: &Connection,
    node_num: i64,
    node_id: &str,
    local_epoch: i64,
    now: i64,
    conflict_mode: i64,
    outcome: i64,
    global_epoch: Option<i64>,
) -> Result<(i64, String)> {
    let tx_id = format!("tx-{node_id}-{local_epoch}");
    conn.prepare_cached(
        "INSERT INTO jazz_tx
          (node_num, local_epoch, global_epoch, kind, conflict_mode, outcome, created_at, metadata, writes_json, reads_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, NULL, jsonb('[]'), NULL)",
    )?
    .execute(params![
        node_num,
        local_epoch,
        global_epoch,
        KIND_DATA,
        conflict_mode,
        outcome,
        now
    ])?;
    let tx_num = conn.last_insert_rowid();
    if let Some(global_epoch) = global_epoch {
        conn.prepare_cached(
            "INSERT OR REPLACE INTO jazz_tx_receipt
             (tx_num, tier, observed_at, receipt)
             VALUES (?, ?, ?, '{}')",
        )?
        .execute(params![tx_num, TIER_GLOBAL, global_epoch])?;
    }
    Ok((tx_num, tx_id))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_tx_at_local_epoch_with_single_row_read_write(
    conn: &Connection,
    node_num: i64,
    node_id: &str,
    local_epoch: i64,
    now: i64,
    conflict_mode: i64,
    outcome: i64,
    global_epoch: Option<i64>,
    table_num: i64,
    row_num: i64,
    op: i64,
    read_reason: i64,
    observed_tx_num: Option<i64>,
) -> Result<(i64, String)> {
    let tx_id = format!("tx-{node_id}-{local_epoch}");
    let writes = encode_writes(&[PackedWrite(table_num, row_num, op)]);
    let reads = encode_reads(&[PackedRead(table_num, row_num, read_reason, observed_tx_num)]);
    conn.prepare_cached(
        "INSERT INTO jazz_tx
          (node_num, local_epoch, global_epoch, kind, conflict_mode, outcome, created_at, metadata, writes_json, reads_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, NULL, jsonb(?), jsonb(?))",
    )?
    .execute(params![
        node_num,
        local_epoch,
        global_epoch,
        KIND_DATA,
        conflict_mode,
        outcome,
        now,
        writes,
        reads
    ])?;
    let tx_num = conn.last_insert_rowid();
    if let Some(global_epoch) = global_epoch {
        conn.prepare_cached(
            "INSERT OR REPLACE INTO jazz_tx_receipt
             (tx_num, tier, observed_at, receipt)
             VALUES (?, ?, ?, '{}')",
        )?
        .execute(params![tx_num, TIER_GLOBAL, global_epoch])?;
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
    append_writes(conn, tx_num, [PackedWrite(table_num, row_num, op)])
}

pub(crate) fn set_single_row_read_write(
    conn: &Connection,
    tx_num: i64,
    table_num: i64,
    row_num: i64,
    op: i64,
    read_reason: i64,
    observed_tx_num: Option<i64>,
) -> Result<()> {
    let writes = format!("[[{table_num},{row_num},{op}]]");
    let observed = observed_tx_num
        .map(|tx_num| tx_num.to_string())
        .unwrap_or_else(|| "null".to_owned());
    let reads = format!("[[{table_num},{row_num},{read_reason},{observed}]]");
    conn.prepare_cached(
        "UPDATE jazz_tx
            SET writes_json = jsonb(?),
                reads_json = jsonb(?)
          WHERE tx_num = ?",
    )?
    .execute(params![writes, reads, tx_num])?;
    Ok(())
}

pub(crate) fn set_received_read_write_tuple_batch(
    conn: &Connection,
    tuples: &[ReceivedReadWriteTuple],
) -> Result<()> {
    for chunk in tuples.chunks(500) {
        if chunk.is_empty() {
            continue;
        }
        let placeholders = (0..chunk.len())
            .map(|_| "(?, ?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "WITH incoming(tx_num, writes_json, reads_json) AS (VALUES {placeholders})
             UPDATE jazz_tx
                SET writes_json = jsonb(incoming.writes_json),
                    reads_json = CASE
                      WHEN incoming.reads_json IS NULL THEN NULL
                      ELSE jsonb(incoming.reads_json)
                    END
               FROM incoming
              WHERE jazz_tx.tx_num = incoming.tx_num"
        );
        let mut values = Vec::with_capacity(chunk.len() * 3);
        for tuple in chunk {
            values.push(rusqlite::types::Value::Integer(tuple.tx_num));
            values.push(rusqlite::types::Value::Text(encode_writes(&tuple.writes)));
            values.push(
                tuple
                    .reads
                    .as_deref()
                    .map(encode_reads)
                    .map(rusqlite::types::Value::Text)
                    .unwrap_or(rusqlite::types::Value::Null),
            );
        }
        conn.prepare_cached(&sql)?
            .execute(rusqlite::params_from_iter(values.iter()))?;
    }
    Ok(())
}

pub(crate) struct ReceivedReadWriteTuple {
    pub(crate) tx_num: i64,
    pub(crate) writes: Vec<PackedWrite>,
    pub(crate) reads: Option<Vec<PackedRead>>,
}

pub(crate) fn append_writes(
    conn: &Connection,
    tx_num: i64,
    new_writes: impl IntoIterator<Item = PackedWrite>,
) -> Result<()> {
    let new_writes = new_writes.into_iter().collect::<Vec<_>>();
    if new_writes.is_empty() {
        return Ok(());
    }
    if new_writes.len() == 1 {
        let PackedWrite(table_num, row_num, op) = new_writes[0];
        let writes = format!("[[{table_num},{row_num},{op}]]");
        let changed = conn
            .prepare_cached(
                "UPDATE jazz_tx
                SET writes_json = jsonb(?)
              WHERE tx_num = ?
                AND writes_json = jsonb('[]')",
            )?
            .execute(params![writes, tx_num])?;
        if changed > 0 {
            return Ok(());
        }
    }
    let mut writes = packed_writes(conn, tx_num)?;
    let mut changed = false;
    for write in new_writes {
        if !writes.contains(&write) {
            writes.push(write);
            changed = true;
        }
    }
    if changed {
        writes.sort_by_key(|write| (write.0, write.1, write.2));
        let encoded = serde_json::to_string(&writes)
            .map_err(|err| Error::new(format!("encode tx writes: {err}")))?;
        conn.prepare_cached("UPDATE jazz_tx SET writes_json = jsonb(?) WHERE tx_num = ?")?
            .execute(params![encoded, tx_num])?;
    }
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
    append_reads(
        conn,
        tx_num,
        [PackedRead(table_num, row_num, reason, observed_tx_num)],
    )
}

pub(crate) fn append_reads(
    conn: &Connection,
    tx_num: i64,
    new_reads: impl IntoIterator<Item = PackedRead>,
) -> Result<()> {
    let new_reads = new_reads.into_iter().collect::<Vec<_>>();
    if new_reads.is_empty() {
        return Ok(());
    }
    if new_reads.len() == 1 {
        let PackedRead(table_num, row_num, reason, observed_tx_num) = new_reads[0];
        let observed = observed_tx_num
            .map(|tx_num| tx_num.to_string())
            .unwrap_or_else(|| "null".to_owned());
        let reads = format!("[[{table_num},{row_num},{reason},{observed}]]");
        let changed = conn
            .prepare_cached(
                "UPDATE jazz_tx
                    SET reads_json = jsonb(?)
                  WHERE tx_num = ?
                    AND reads_json IS NULL
                    AND writes_json = jsonb('[]')",
            )?
            .execute(params![reads, tx_num])?;
        if changed > 0 {
            return Ok(());
        }
    }
    let state = tuple_state(conn, tx_num)?;
    let implicit: Vec<PackedRead> = state
        .previous_tx_num
        .map(|previous_tx_num| {
            state
                .writes
                .iter()
                .map(|write| PackedRead(write.0, write.1, 2, Some(previous_tx_num)))
                .collect()
        })
        .unwrap_or_default();
    let explicit = state.reads;
    let had_explicit = explicit.is_some();
    let mut reads = explicit.unwrap_or_else(|| implicit.clone());
    for read in new_reads {
        if !reads.contains(&read) {
            reads.push(read);
        }
    }
    if !implicit.is_empty() && same_read_set(&reads, &implicit) {
        if had_explicit {
            conn.prepare_cached("UPDATE jazz_tx SET reads_json = NULL WHERE tx_num = ?")?
                .execute(params![tx_num])?;
        }
    } else {
        replace_explicit_reads(conn, tx_num, &reads)?;
    }
    Ok(())
}

pub(crate) fn fill_observed_read(
    conn: &Connection,
    tx_num: i64,
    table_num: i64,
    row_num: i64,
    observed_tx_num: i64,
) -> Result<()> {
    let mut reads = explicit_reads(conn, tx_num)?
        .unwrap_or_else(|| implicit_previous_reads(conn, tx_num).unwrap_or_default());
    for read in &mut reads {
        if read.0 == table_num && read.1 == row_num && read.3.is_none() {
            read.3 = Some(observed_tx_num);
        }
    }
    replace_explicit_reads(conn, tx_num, &reads)?;
    Ok(())
}

fn packed_writes(conn: &Connection, tx_num: i64) -> Result<Vec<PackedWrite>> {
    let encoded: String = conn
        .prepare_cached("SELECT json(writes_json) FROM jazz_tx WHERE tx_num = ?")?
        .query_row(params![tx_num], |row| row.get(0))?;
    serde_json::from_str(&encoded).map_err(|err| Error::new(format!("decode tx writes: {err}")))
}

fn explicit_reads(conn: &Connection, tx_num: i64) -> Result<Option<Vec<PackedRead>>> {
    let encoded = conn
        .prepare_cached("SELECT json(reads_json) FROM jazz_tx WHERE tx_num = ?")?
        .query_row(params![tx_num], |row| row.get::<_, Option<String>>(0))?;
    encoded
        .map(|encoded| {
            serde_json::from_str(&encoded)
                .map_err(|err| Error::new(format!("decode tx reads: {err}")))
        })
        .transpose()
}

struct TxTupleState {
    previous_tx_num: Option<i64>,
    writes: Vec<PackedWrite>,
    reads: Option<Vec<PackedRead>>,
}

fn tuple_state(conn: &Connection, tx_num: i64) -> Result<TxTupleState> {
    let (previous_tx_num, writes, reads) = conn
        .prepare_cached(
            "SELECT previous.tx_num, json(tx.writes_json), json(tx.reads_json)
             FROM jazz_tx tx
             LEFT JOIN jazz_tx previous
               ON previous.node_num = tx.node_num
              AND previous.local_epoch = tx.local_epoch - 1
             WHERE tx.tx_num = ?",
        )?
        .query_row(params![tx_num], |row| {
            Ok((
                row.get::<_, Option<i64>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
    Ok(TxTupleState {
        previous_tx_num,
        writes: serde_json::from_str(&writes)
            .map_err(|err| Error::new(format!("decode tx writes: {err}")))?,
        reads: reads
            .map(|reads| {
                serde_json::from_str(&reads)
                    .map_err(|err| Error::new(format!("decode tx reads: {err}")))
            })
            .transpose()?,
    })
}

fn replace_explicit_reads(conn: &Connection, tx_num: i64, reads: &[PackedRead]) -> Result<()> {
    let mut reads = reads.to_vec();
    reads.sort_by_key(|read| (read.0, read.1, read.2, read.3));
    let encoded = serde_json::to_string(&reads)
        .map_err(|err| Error::new(format!("encode tx reads: {err}")))?;
    conn.prepare_cached("UPDATE jazz_tx SET reads_json = jsonb(?) WHERE tx_num = ?")?
        .execute(params![encoded, tx_num])?;
    Ok(())
}

fn encode_writes(writes: &[PackedWrite]) -> String {
    if writes.is_empty() {
        return "[]".to_owned();
    }
    let mut encoded = String::with_capacity(writes.len() * 20 + 2);
    encoded.push('[');
    for (index, PackedWrite(table_num, row_num, op)) in writes.iter().enumerate() {
        if index > 0 {
            encoded.push(',');
        }
        encoded.push('[');
        encoded.push_str(&table_num.to_string());
        encoded.push(',');
        encoded.push_str(&row_num.to_string());
        encoded.push(',');
        encoded.push_str(&op.to_string());
        encoded.push(']');
    }
    encoded.push(']');
    encoded
}

fn encode_reads(reads: &[PackedRead]) -> String {
    if reads.is_empty() {
        return "[]".to_owned();
    }
    let mut encoded = String::with_capacity(reads.len() * 28 + 2);
    encoded.push('[');
    for (index, PackedRead(table_num, row_num, reason, observed_tx_num)) in reads.iter().enumerate()
    {
        if index > 0 {
            encoded.push(',');
        }
        encoded.push('[');
        encoded.push_str(&table_num.to_string());
        encoded.push(',');
        encoded.push_str(&row_num.to_string());
        encoded.push(',');
        encoded.push_str(&reason.to_string());
        encoded.push(',');
        match observed_tx_num {
            Some(tx_num) => encoded.push_str(&tx_num.to_string()),
            None => encoded.push_str("null"),
        }
        encoded.push(']');
    }
    encoded.push(']');
    encoded
}

fn implicit_previous_reads(conn: &Connection, tx_num: i64) -> Result<Vec<PackedRead>> {
    let writes = packed_writes(conn, tx_num)?;
    let previous_tx_num = conn
        .prepare_cached(
            "SELECT previous.tx_num
             FROM jazz_tx tx
             JOIN jazz_tx previous
               ON previous.node_num = tx.node_num
              AND previous.local_epoch = tx.local_epoch - 1
             WHERE tx.tx_num = ?",
        )?
        .query_row(params![tx_num], |row| row.get::<_, i64>(0))
        .optional()?;
    let Some(previous_tx_num) = previous_tx_num else {
        return Ok(Vec::new());
    };
    Ok(writes
        .iter()
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
    conn.prepare_cached(
        "SELECT tx.tx_num
         FROM jazz_tx_public tx
         WHERE tx.node_id = ? AND tx.local_epoch = ?",
    )?
    .query_row(params![node_id, local_epoch], |row| row.get(0))
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
    let detail = bytes_to_hex(
        &bincode::serialize(&WireValue::Null)
            .map_err(|err| Error::new(format!("encode rejection detail: {err}")))?,
    );
    reject_with_detail(conn, tx_id, code, &detail)
}

pub(crate) fn reject_with_detail(
    conn: &Connection,
    tx_id: &str,
    code: &str,
    detail: &str,
) -> Result<i64> {
    let tx_num = tx_num(conn, tx_id)?;
    conn.execute(
        "UPDATE jazz_tx SET outcome = ? WHERE tx_num = ?",
        params![OUTCOME_REJECTED, tx_num],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO jazz_tx_rejection (tx_num, code, detail)
         VALUES (?, ?, ?)",
        params![tx_num, code, detail],
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
         (tx_num, tier, observed_at, receipt)
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
         (tx_num, tier, observed_at, receipt)
         VALUES (?, ?, ?, '{}')",
        params![tx_num, TIER_EDGE, observed_at],
    )?;
    Ok(tx_num)
}
