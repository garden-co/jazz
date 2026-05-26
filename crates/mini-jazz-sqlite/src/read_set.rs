use crate::rows::ensure_row_id;
use crate::sync::Bundle;
use crate::{branch, schema, tx, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn record_tx_read(
    conn: &Connection,
    tx_num: i64,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
    reason: i64,
) -> Result<()> {
    let observed_tx_num = current_visible_tx_num(conn, table_name, row_num, branch_num)?;
    record_tx_read_with_observed(conn, tx_num, table_name, row_num, reason, observed_tx_num)
}

pub(crate) fn record_tx_read_with_observed(
    conn: &Connection,
    tx_num: i64,
    table_name: &str,
    row_num: i64,
    reason: i64,
    observed_tx_num: Option<i64>,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO jazz_tx_read
         (tx_num, table_name, row_num, reason, observed_tx_num)
         VALUES (?, ?, ?, ?, ?)",
        params![tx_num, table_name, row_num, reason, observed_tx_num],
    )?;
    Ok(())
}

pub(crate) fn tx_read_set_is_stale(
    conn: &Connection,
    tx_num: i64,
    branch_id: &str,
) -> Result<bool> {
    let branch_num = branch::ensure(conn, branch_id, None, now_ms())?;
    let mut stmt = conn.prepare(
        "SELECT table_name, row_num, observed_tx_num
         FROM jazz_tx_read
         WHERE tx_num = ?
           AND observed_tx_num IS NOT NULL
         ORDER BY table_name, row_num",
    )?;
    let reads = stmt.query_map(params![tx_num], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Option<i64>>(2)?,
        ))
    })?;
    for read in reads {
        let (table_name, row_num, observed_tx_num) = read?;
        let Some(observed_tx_num) = observed_tx_num else {
            continue;
        };
        let current_tx_num = current_visible_tx_num(conn, &table_name, row_num, branch_num)?;
        if current_tx_num != Some(observed_tx_num) && current_tx_num != Some(tx_num) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn stale_exclusive_tx_ids_in_bundle(
    conn: &Connection,
    bundle: &Bundle,
) -> Result<BTreeSet<String>> {
    let exclusive_pending = bundle
        .txs
        .iter()
        .filter(|tx_record| {
            tx_record.conflict_mode == tx::MODE_EXCLUSIVE
                && tx_record.outcome == tx::OUTCOME_PENDING
        })
        .map(|tx_record| tx_record.tx_id.as_str())
        .collect::<BTreeSet<_>>();
    let branch_by_tx = bundle
        .history
        .iter()
        .map(|record| (record.tx_id.as_str(), record.branch_id.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut stale = BTreeSet::new();
    for read in &bundle.reads {
        if !exclusive_pending.contains(read.tx_id.as_str()) {
            continue;
        }
        let Some(observed_tx_id) = &read.observed_tx_id else {
            continue;
        };
        let Some(branch_id) = branch_by_tx.get(read.tx_id.as_str()) else {
            continue;
        };
        let base_global_epoch = bundle
            .branches
            .iter()
            .find(|branch| branch.branch_id == *branch_id)
            .and_then(|branch| branch.base_global_epoch);
        let branch_num = branch::ensure(conn, branch_id, base_global_epoch, now_ms())?;
        let row_num = ensure_row_id(conn, &read.table, &read.row_id)?;
        let current_tx_num = current_visible_tx_num(conn, &read.table, row_num, branch_num)?;
        let current_tx_id = current_tx_num
            .map(|tx_num| tx_id_for_num(conn, tx_num))
            .transpose()?;
        if current_tx_id.as_deref() != Some(observed_tx_id.as_str()) {
            stale.insert(read.tx_id.clone());
        }
    }
    Ok(stale)
}

fn current_visible_tx_num(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<i64>> {
    if branch_num != 1 {
        if let Some(base_epoch) = branch::base_global_epoch(conn, branch_num)? {
            let branch_tx = conn
                .query_row(
                    &format!(
                        "SELECT visible_tx_num
                         FROM {}
                         WHERE row_num = ? AND j_branch_num = ?",
                        schema::current_table(table_name)
                    ),
                    params![row_num, branch_num],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            if branch_tx.is_none() {
                return snapshot_visible_tx_num(conn, table_name, row_num, base_epoch);
            }
        }
    }
    conn.query_row(
        &format!(
            "SELECT visible_tx_num
             FROM {}
             WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
            schema::current_table(table_name)
        ),
        params![row_num, branch_num],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn snapshot_visible_tx_num(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    base_epoch: i64,
) -> Result<Option<i64>> {
    conn.query_row(
        &format!(
            "SELECT h.tx_num
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.row_num = ?
               AND h.j_branch_num = 1
               AND h.op != 3
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
             ORDER BY tx.global_epoch DESC, h.tx_num DESC
             LIMIT 1",
            schema::history_table(table_name)
        ),
        params![row_num, tx::OUTCOME_REJECTED, base_epoch],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn tx_id_for_num(conn: &Connection, tx_num: i64) -> Result<String> {
    Ok(conn.query_row(
        "SELECT tx_id FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| row.get(0),
    )?)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
