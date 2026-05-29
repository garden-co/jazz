use crate::apply::{
    apply_branch_records, apply_query_read_records, apply_read_records, apply_tx_records,
    tx_apply_info, ApplyCaches, ApplyTxInfo, BundleApplyPlan,
};
use crate::auth::RuntimeAuth;
use crate::profile::ProfileTimer;
use crate::query_api::{
    predicate_query, BuiltQuery, QueryCondition, QueryConditionOp, QueryDirection, QueryOrderBy,
};
use crate::query_observation::{
    built_query_from_read, built_query_read_value, observed_ids_from_query_value, observed_row_ids,
    support_window_query,
};
use crate::query_refresh::QueryRefreshPlan;
use crate::read_visibility::ReadVisibility;
use crate::rows::{ensure_row_id, ensure_row_id_with_status, public_row_id, row_num};
use crate::schema::{FieldDef, FieldKind, PolicyDef, SchemaDef};
use crate::subscription::{RejectionSubscription, RowsSubscription, RowsSubscriptionQuery};
use crate::sync::{
    BranchRecord, Bundle, HistoryRecord, QueryReadRecord, ReadRecord, TxRecord,
    BUNDLE_PROTOCOL_VERSION,
};
use crate::time::now_ms;
use crate::types::{
    ApplyBundleProfile, BranchInfo, QueryExportProfile, ReadTier, RejectionInfo, RowView,
    StorageStats, TransactionInfo,
};
use crate::{
    branch, effective, policy, policy_read_set, projection, query, query_predicate, read_set,
    schema, stats, storage, tx, users, Result, Storage,
};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{json, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet};

mod branches;
mod history_export;
mod query_export;
mod query_refresh;
mod query_scope_export;
mod reads;
mod session;
mod storage_admin;
mod subscriptions;
mod sync_apply;
mod sync_export;
mod transaction_builder;
mod transaction_status;
mod write_batch;
mod write_core;
mod writes;
use history_export::*;
#[allow(unused_imports)]
pub use transaction_builder::TransactionBuilder;
use write_core::{
    exclusive_write_conflict_exists, insert_row_in_tx, record_tx_write_num,
    row_has_current_branch_value, row_id_used_by_other_table, stage_delete_row_in_tx,
    InsertRowInTx, StageDeleteInTx,
};

pub struct Runtime {
    conn: Connection,
    schema: SchemaDef,
    node_id: String,
    auth: RuntimeAuth,
    node_num: i64,
    branch_num: i64,
}

struct AwaitingDependencyTx {
    tx_num: i64,
    tx_id: String,
    auth_user: String,
}

pub(crate) struct QueryScopeOptions<'a> {
    ref_include_fields: &'a [&'a str],
    extra_row_ids: &'a [String],
}

struct BatchedQueryScopeItem {
    op: String,
    value: JsonValue,
    rows: Vec<RowView>,
    extra_row_ids: Vec<String>,
}

impl QueryScopeOptions<'_> {
    fn empty() -> Self {
        Self {
            ref_include_fields: &[],
            extra_row_ids: &[],
        }
    }
}

fn policy_denial_detail_for_history_record(
    conn: &Connection,
    table: &crate::schema::TableDef,
    record: &HistoryRecord,
    tx_num: i64,
) -> Result<JsonValue> {
    let branch_num = branch::checkout(conn, &record.branch_id)?;
    if let Some(dependency) = unavailable_recorded_policy_dependency(conn, tx_num, branch_num)? {
        return Ok(json!({
            "reason": "policy_dependency_unavailable",
            "table": record.table,
            "row_id": record.row_id,
            "dependency_table": dependency.0,
            "dependency_row_id": dependency.1,
        }));
    }
    if let PolicyDef::RefReadable { field } = &table.write_policy {
        if let Some(dependency) = unavailable_policy_dependency(conn, table, record, tx_num, field)?
        {
            return Ok(json!({
                "reason": "policy_dependency_unavailable",
                "table": record.table,
                "row_id": record.row_id,
                "dependency_table": dependency.0,
                "dependency_row_id": dependency.1,
            }));
        }
    }
    Ok(json!({
        "reason": "write_policy_denied",
        "table": record.table,
        "row_id": record.row_id,
    }))
}

fn is_policy_dependency_unavailable(detail: &JsonValue) -> bool {
    detail.get("reason").and_then(JsonValue::as_str) == Some("policy_dependency_unavailable")
}

fn mark_transaction_awaiting_dependency(
    conn: &Connection,
    tx_num: i64,
    auth_user: &str,
    detail: &JsonValue,
) -> Result<()> {
    let detail_json =
        serde_json::to_string(detail).map_err(|err| crate::Error::new(err.to_string()))?;
    conn.execute(
        "INSERT OR REPLACE INTO jazz_tx_awaiting_dependency
         (tx_num, auth_user, detail_json, updated_at)
         VALUES (?, ?, ?, ?)",
        params![tx_num, auth_user, detail_json, now_ms()],
    )?;
    Ok(())
}

fn remove_current_for_awaiting_dependency(
    conn: &Connection,
    record: &HistoryRecord,
    row_num: i64,
) -> Result<()> {
    let branch_num = branch::ensure(conn, &record.branch_id, None, now_ms())?;
    conn.execute(
        &format!(
            "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
            crate::schema::current_table(&record.table)
        ),
        params![row_num, branch_num],
    )?;
    Ok(())
}

fn clear_transaction_awaiting_dependency(conn: &Connection, tx_num: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM jazz_tx_awaiting_dependency WHERE tx_num = ?",
        params![tx_num],
    )?;
    Ok(())
}

fn awaiting_dependency_transactions(conn: &Connection) -> Result<Vec<AwaitingDependencyTx>> {
    let mut stmt = conn.prepare(
        "SELECT tx.tx_num, tx.tx_id, awaiting.auth_user
         FROM jazz_tx_awaiting_dependency awaiting
         JOIN jazz_tx tx ON tx.tx_num = awaiting.tx_num
         ORDER BY tx.tx_num",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(AwaitingDependencyTx {
            tx_num: row.get(0)?,
            tx_id: row.get(1)?,
            auth_user: row.get(2)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn history_records_for_tx(
    conn: &Connection,
    schema: &SchemaDef,
    tx_num: i64,
    tx_id: &str,
) -> Result<Vec<HistoryRecord>> {
    let mut records = Vec::new();
    for table in schema.tables() {
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "ids.row_id".to_owned(),
            "branch.branch_id".to_owned(),
            "h.op".to_owned(),
        ];
        select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
        select_columns.extend([
            "h.j_created_at".to_owned(),
            "h.j_updated_at".to_owned(),
            format!(
                "{} AS j_created_by",
                users::user_id_expr("h", "j_created_by")
            ),
            format!(
                "{} AS j_updated_by",
                users::user_id_expr("h", "j_updated_by")
            ),
        ]);
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
             WHERE h.tx_num = ?
             ORDER BY h.row_num",
            select_columns.join(", "),
            crate::schema::history_table(&table.name)
        );
        let mut stmt = conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 4;
        let mut rows = stmt.query(params![tx_num])?;
        while let Some(row) = rows.next()? {
            let raw = (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut values = BTreeMap::new();
            for (idx, field) in table.fields.iter().enumerate() {
                values.insert(
                    field.name.clone(),
                    sql_value_to_json(conn, field, &raw[idx + 3])?,
                );
            }
            let sys = 3 + table.fields.len();
            records.push(HistoryRecord {
                table: table.name.clone(),
                row_id: text_value(&raw[0], "row_id")?,
                branch_id: text_value(&raw[1], "branch_id")?,
                tx_id: tx_id.to_owned(),
                op: integer_value(&raw[2], "op")?,
                values,
                created_at: integer_value(&raw[sys], "j_created_at")?,
                updated_at: integer_value(&raw[sys + 1], "j_updated_at")?,
                created_by: text_value(&raw[sys + 2], "j_created_by")?,
                updated_by: text_value(&raw[sys + 3], "j_updated_by")?,
            });
        }
    }
    Ok(records)
}

fn unavailable_recorded_policy_dependency(
    conn: &Connection,
    tx_num: i64,
    branch_num: i64,
) -> Result<Option<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT tables.table_name, ids.row_id, reads.row_num, reads.observed_tx_num
         FROM jazz_tx_read reads
         JOIN jazz_table tables ON tables.table_num = reads.table_num
         JOIN jazz_row_id ids ON ids.row_num = reads.row_num
         WHERE reads.tx_num = ?
           AND reads.reason = ?
         ORDER BY tables.table_name, ids.row_id",
    )?;
    let rows = stmt.query_map(params![tx_num, 1], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<i64>>(3)?,
        ))
    })?;
    for row in rows {
        let (table_name, row_id, row_num, observed_tx_num) = row?;
        let visible_count: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(*)
                 FROM {}
                 WHERE row_num = ?
                   AND j_branch_num = ?
                   AND is_deleted = 0",
                crate::schema::current_table(&table_name)
            ),
            params![row_num, branch_num],
            |row| row.get(0),
        )?;
        if visible_count == 0 {
            return Ok(Some((table_name, row_id)));
        }
        if observed_tx_num.is_none() {
            repair_missing_observed_policy_read(conn, tx_num, &table_name, row_num, branch_num)?;
        }
    }
    Ok(None)
}

fn unavailable_policy_dependency(
    conn: &Connection,
    table: &crate::schema::TableDef,
    record: &HistoryRecord,
    tx_num: i64,
    field_name: &str,
) -> Result<Option<(String, String)>> {
    let Some(field) = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
    else {
        return Ok(None);
    };
    let FieldKind::Ref {
        table: ref_table_name,
    } = &field.kind
    else {
        return Ok(None);
    };
    let Some(row_id) = record.values.get(&field.name).and_then(JsonValue::as_str) else {
        return Ok(None);
    };
    let dependency_row_num = ensure_row_id(conn, ref_table_name, row_id)?;
    let branch_num = branch::checkout(conn, &record.branch_id)?;
    let visible_count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {}
             WHERE row_num = ?
               AND j_branch_num = ?
               AND is_deleted = 0",
            crate::schema::current_table(ref_table_name)
        ),
        params![dependency_row_num, branch_num],
        |row| row.get(0),
    )?;
    if visible_count == 0 {
        return Ok(Some((ref_table_name.clone(), row_id.to_owned())));
    }
    let missing_observed_read_count: i64 = conn.query_row(
        "SELECT COUNT(*)
         FROM jazz_tx_read
         WHERE tx_num = ?
           AND table_num = ?
           AND row_num = ?
           AND observed_tx_num IS NULL",
        params![
            tx_num,
            crate::schema::table_num(conn, ref_table_name)?,
            dependency_row_num
        ],
        |row| row.get(0),
    )?;
    if missing_observed_read_count > 0 {
        repair_missing_observed_policy_read(
            conn,
            tx_num,
            ref_table_name,
            dependency_row_num,
            branch_num,
        )?;
    }
    Ok(None)
}

fn repair_missing_observed_policy_read(
    conn: &Connection,
    tx_num: i64,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<()> {
    let observed_tx_num: Option<i64> = conn
        .query_row(
            &format!(
                "SELECT visible_tx_num
                 FROM {}
                 WHERE row_num = ?
                   AND j_branch_num = ?
                   AND is_deleted = 0",
                crate::schema::current_table(table_name)
            ),
            params![row_num, branch_num],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(observed_tx_num) = observed_tx_num {
        conn.execute(
            "UPDATE jazz_tx_read
             SET observed_tx_num = ?
             WHERE tx_num = ?
               AND table_num = ?
               AND row_num = ?
               AND observed_tx_num IS NULL",
            params![
                observed_tx_num,
                tx_num,
                crate::schema::table_num(conn, table_name)?,
                row_num
            ],
        )?;
    }
    Ok(())
}

fn tx_outcome(conn: &Connection, tx_num: i64) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT outcome FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| row.get(0),
    )?)
}

fn tx_conflict_mode(conn: &Connection, tx_num: i64) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT conflict_mode FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| row.get(0),
    )?)
}

struct ApplyHistoryContext<'a> {
    schema: &'a SchemaDef,
    db: &'a Connection,
    local_node_num: i64,
    tx_nums_by_id: &'a BTreeMap<String, i64>,
    tx_info_by_num: &'a BTreeMap<i64, ApplyTxInfo>,
    branch_nums_by_id: &'a BTreeMap<String, i64>,
    table_nums_by_name: &'a BTreeMap<String, i64>,
    apply_caches: &'a mut ApplyCaches,
}

fn next_global_epoch(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COALESCE(MAX(global_epoch), 0) + 1 FROM jazz_tx",
        [],
        |row| row.get(0),
    )?)
}

fn durable_version_exists_for_row(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.row_num = ?
               AND h.j_branch_num = ?
               AND tx.outcome != ?
               AND (tx.outcome = ? OR tx.global_epoch IS NOT NULL)",
            crate::schema::history_table(table_name)
        ),
        params![
            row_num,
            branch_num,
            tx::OUTCOME_REJECTED,
            tx::OUTCOME_ACCEPTED
        ],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn write_allowed_for_history_record(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    row_num: i64,
    record: &HistoryRecord,
    auth_user: Option<&str>,
) -> Result<bool> {
    let user = auth_user
        .ok_or_else(|| crate::Error::new("untrusted policy validation requires auth user"))?;
    let branch_num = branch::ensure(conn, &record.branch_id, None, now_ms())?;
    if record.op == 3 && matches!(table.write_policy, PolicyDef::CreatedByUser) {
        return Ok(record.created_by == user);
    }
    policy::write_allowed(policy::WriteCheck {
        db: conn,
        schema,
        table,
        row_num,
        branch_num,
        values: &record.values,
        user,
    })
}

fn is_newest_version_for_current(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
    tx_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             JOIN jazz_tx current_tx ON current_tx.tx_num = ?
             WHERE h.row_num = ?
               AND h.j_branch_num = ?
               AND tx.outcome != ?
               AND (
                 (tx.global_epoch IS NOT NULL AND current_tx.global_epoch IS NOT NULL
                  AND (tx.global_epoch > current_tx.global_epoch
                       OR (tx.global_epoch = current_tx.global_epoch AND tx.tx_num > current_tx.tx_num)))
                 OR ((tx.global_epoch IS NOT NULL) = (current_tx.global_epoch IS NOT NULL)
                     AND tx.global_epoch IS NULL
                     AND tx.tx_num > current_tx.tx_num)
               )",
            crate::schema::history_table(table_name)
        ),
        params![tx_num, row_num, branch_num, tx::OUTCOME_REJECTED],
        |row| row.get(0),
    )?;
    Ok(count == 0)
}

fn current_visible_tx_num(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<i64>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT visible_tx_num
         FROM {}
         WHERE row_num = ?
           AND j_branch_num = ?",
        crate::schema::current_table(table_name)
    ))?;
    let mut rows = stmt.query(params![row_num, branch_num])?;
    rows.next()?
        .map(|row| row.get(0))
        .transpose()
        .map_err(Into::into)
}

fn history_record_exists(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    tx_num: i64,
) -> Result<bool> {
    let mut stmt = conn.prepare(&format!(
        "SELECT 1
         FROM {}
         WHERE row_num = ?
           AND tx_num = ?
         LIMIT 1",
        crate::schema::history_table(table_name)
    ))?;
    let mut rows = stmt.query(params![row_num, tx_num])?;
    Ok(rows.next()?.is_some())
}

fn tx_is_newer_than_current_fast_path(
    conn: &Connection,
    candidate_tx_num: i64,
    current_tx_num: i64,
) -> Result<Option<bool>> {
    let comparison: Option<i64> = conn.query_row(
        "SELECT CASE
           WHEN (candidate.global_epoch IS NULL) != (current.global_epoch IS NULL)
             THEN NULL
           WHEN candidate.global_epoch IS NOT NULL
             THEN candidate.global_epoch > current.global_epoch
               OR (candidate.global_epoch = current.global_epoch AND candidate.tx_num > current.tx_num)
           ELSE candidate.tx_num > current.tx_num
         END
         FROM jazz_tx candidate
         JOIN jazz_tx current ON current.tx_num = ?
         WHERE candidate.tx_num = ?",
        params![current_tx_num, candidate_tx_num],
        |row| row.get(0),
    )?;
    Ok(comparison.map(|is_newer| is_newer != 0))
}

fn tier_name(tier: i64) -> rusqlite::Result<String> {
    Ok(match tier {
        tx::TIER_EDGE => "edge",
        tx::TIER_GLOBAL => "global",
        _ => "unknown",
    }
    .to_owned())
}

fn conflict_mode_name(mode: i64) -> String {
    match mode {
        tx::MODE_EXCLUSIVE => "exclusive",
        tx::MODE_MERGEABLE => "mergeable",
        _ => "unknown",
    }
    .to_owned()
}

fn insert_dynamic(
    conn: &Connection,
    table: &str,
    columns: &[String],
    values: &[rusqlite::types::Value],
) -> Result<()> {
    let placeholders = (0..values.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn.prepare(&format!(
        "INSERT OR REPLACE INTO {table} ({}) VALUES ({placeholders})",
        columns.join(", ")
    ))?;
    stmt.execute(params_from_iter(values.iter()))?;
    Ok(())
}
