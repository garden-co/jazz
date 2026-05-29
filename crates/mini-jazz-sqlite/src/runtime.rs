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
mod writes;
#[allow(unused_imports)]
pub use transaction_builder::TransactionBuilder;

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

impl Runtime {
    fn apply_query_scope_repair(
        schema: &SchemaDef,
        db: &Connection,
        query_read: &QueryReadRecord,
        user: &str,
        bypass_policy: bool,
    ) -> Result<()> {
        // Query-scope repair keeps a receiver's current projection from retaining
        // rows that used to satisfy an observed query but are no longer covered by
        // the refresh bundle.
        //
        // Export side:
        //
        //   +------------------+      +---------------------+
        //   | current results  | ---> | visible row history |
        //   +------------------+      +---------------------+
        //            |                         ^
        //            v                         |
        //   +------------------+      +---------------------+
        //   | repair row nums  | ---> | old matching rows   |
        //   +------------------+      +---------------------+
        //
        // Apply side:
        //
        //   +-------------------+      +----------------------+
        //   | local current row | ---> | still has matching   |
        //   | matches read      |      | history in bundle?   |
        //   +-------------------+      +----------------------+
        //             | yes                     | no
        //             v                         v
        //        keep current             delete current
        //
        // `apply_bundle` runs this both before and after applying incoming
        // history. The first pass can remove stale rows using repair history
        // already present locally; the second pass observes repair history that
        // arrived in the bundle and catches page-boundary changes after current
        // projection updates.
        if query_read.op == "absent" {
            let table = schema.table_def(&query_read.table)?;
            if query_read.field != "id"
                && !table
                    .fields
                    .iter()
                    .any(|field| field.name == query_read.field)
            {
                return Err(crate::Error::new(format!(
                    "unknown query field {}",
                    query_read.field
                )));
            }
            return Ok(());
        }
        if query_read.op == "recursive_refs" {
            let table = schema.table_def(&query_read.table)?;
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == query_read.field)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown query field {}", query_read.field))
                })?;
            if !matches!(field.kind, FieldKind::Ref { .. }) {
                return Err(crate::Error::new(format!(
                    "recursive refs expects ref field {}",
                    query_read.field
                )));
            }
            if !query_read.value.is_string() {
                return Err(crate::Error::new("recursive refs expects root id string"));
            }
            return Ok(());
        }
        if query_read.op == "query" {
            let query = built_query_from_read(query_read)?;
            return Self::apply_built_query_scope_repair(
                schema,
                db,
                query_read,
                &query,
                user,
                bypass_policy,
            );
        }
        if query_read.op == "eq_top_created_at_desc" {
            let value = query_read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
            let limit = query_read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
            let table = schema.table_def(&query_read.table)?;
            if matches!(query_read.field.as_str(), "id" | "$createdBy") {
                let branch_num = branch::checkout(db, &query_read.branch_id)?;
                let observed_row_nums = observed_ids_from_query_value(&query_read.value)?
                    .into_iter()
                    .map(|row_id| row_num(db, &row_id))
                    .collect::<Result<Vec<_>>>()?;
                let observed_filter = if observed_row_nums.is_empty() {
                    String::new()
                } else {
                    format!(
                        "AND row_num NOT IN ({})",
                        sql_placeholders(observed_row_nums.len())
                    )
                };
                let mut params = vec![rusqlite::types::Value::Integer(branch_num)];
                let predicate_sql = if query_read.field == "id" {
                    let row_id = value
                        .as_str()
                        .ok_or_else(|| crate::Error::new("id equality expects a string value"))?;
                    params.push(rusqlite::types::Value::Integer(ensure_row_id(
                        db,
                        &query_read.table,
                        row_id,
                    )?));
                    "row_num = ?".to_owned()
                } else {
                    let user_id = value.as_str().ok_or_else(|| {
                        crate::Error::new("$createdBy equality expects a string value")
                    })?;
                    let Ok(user_num) = users::user_num(db, user_id) else {
                        return Ok(());
                    };
                    params.push(rusqlite::types::Value::Integer(user_num));
                    "j_created_by = ?".to_owned()
                };
                params.extend(
                    observed_row_nums
                        .into_iter()
                        .map(rusqlite::types::Value::Integer),
                );
                db.execute(
                    &format!(
                        "DELETE FROM {}
                         WHERE j_branch_num = ?
                           AND is_deleted = 0
                           AND {predicate_sql}
                           {observed_filter}",
                        crate::schema::current_table(&query_read.table),
                    ),
                    params_from_iter(params.iter()),
                )?;
                return Ok(());
            }
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == query_read.field)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown query field {}", query_read.field))
                })?;
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            let predicate_column =
                crate::schema::quote_ident(&crate::schema::storage_column(field));
            let predicate_sql = query_predicate::sql(field, &predicate_column, "eq")?;
            let predicate_value = query_predicate::value(field, "eq", value, db)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND is_deleted = 0
                       AND {predicate_sql}
                       AND row_num NOT IN (
                         SELECT current.row_num
                         FROM {current_table} current
                         JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                         WHERE current.j_branch_num = ?
                           AND current.is_deleted = 0
                           AND tx.outcome != ?
                           AND {current_predicate_sql}
                         ORDER BY current.j_created_at DESC, current.row_num
                         LIMIT ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    current_table = crate::schema::current_table(&query_read.table),
                    current_predicate_sql =
                        query_predicate::sql(field, &format!("current.{predicate_column}"), "eq")?,
                ),
                params![
                    branch_num,
                    predicate_value.clone(),
                    branch_num,
                    tx::OUTCOME_REJECTED,
                    predicate_value,
                    limit as i64
                ],
            )?;
            return Ok(());
        }
        if query_read.op == "eq_top_field_desc" {
            let value = query_read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
            let order_field_name = query_read
                .value
                .get("order_field")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
            let limit = query_read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
            let table = schema.table_def(&query_read.table)?;
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == query_read.field)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown query field {}", query_read.field))
                })?;
            let order_field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == order_field_name)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown order field {order_field_name}"))
                })?;
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            let predicate_column =
                crate::schema::quote_ident(&crate::schema::storage_column(field));
            let order_column =
                crate::schema::quote_ident(&crate::schema::storage_column(order_field));
            let predicate_sql = query_predicate::sql(field, &predicate_column, "eq")?;
            let predicate_value = query_predicate::value(field, "eq", value, db)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND is_deleted = 0
                       AND {predicate_sql}
                       AND row_num NOT IN (
                         SELECT current.row_num
                         FROM {current_table} current
                         JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                         WHERE current.j_branch_num = ?
                           AND current.is_deleted = 0
                           AND tx.outcome != ?
                           AND {current_predicate_sql}
                         ORDER BY current.{order_column} DESC, current.row_num
                         LIMIT ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    current_table = crate::schema::current_table(&query_read.table),
                    current_predicate_sql =
                        query_predicate::sql(field, &format!("current.{predicate_column}"), "eq")?,
                ),
                params![
                    branch_num,
                    predicate_value.clone(),
                    branch_num,
                    tx::OUTCOME_REJECTED,
                    predicate_value,
                    limit as i64
                ],
            )?;
            return Ok(());
        }
        if query_read.op == "in" && query_read.field != "id" {
            for value in query_read
                .value
                .as_array()
                .ok_or_else(|| crate::Error::new("in predicate expects an array value"))?
            {
                let eq_read = QueryReadRecord {
                    branch_id: query_read.branch_id.clone(),
                    table: query_read.table.clone(),
                    field: query_read.field.clone(),
                    op: "eq".to_owned(),
                    value: value.clone(),
                };
                Self::apply_query_scope_repair(schema, db, &eq_read, user, bypass_policy)?;
            }
            return Ok(());
        }
        if query_read.field == "id" {
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            if query_read.op == "ne" {
                let excluded_id = query_read
                    .value
                    .as_str()
                    .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
                db.execute(
                    &format!(
                        "DELETE FROM {current_table}
                         WHERE j_branch_num = ?
                           AND row_num != (SELECT row_num FROM jazz_row_id WHERE row_id = ?)
                           AND row_num NOT IN (
                             SELECT h.row_num
                             FROM {history_table} h
                             JOIN jazz_row_id ids ON ids.row_num = h.row_num
                             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                             WHERE ids.row_id != ?
                               AND h.j_branch_num = ?
                               AND h.op != 3
                               AND tx.outcome != ?
                           )",
                        current_table = crate::schema::current_table(&query_read.table),
                        history_table = crate::schema::history_table(&query_read.table),
                    ),
                    params![
                        branch_num,
                        excluded_id,
                        excluded_id,
                        branch_num,
                        tx::OUTCOME_REJECTED
                    ],
                )?;
                return Ok(());
            }
            let row_ids = id_predicate_values(&query_read.op, &query_read.value)?;
            for row_id in row_ids {
                let row_num = ensure_row_id(db, &query_read.table, &row_id)?;
                db.execute(
                    &format!(
                        "DELETE FROM {}
                         WHERE j_branch_num = ?
                           AND row_num = ?
                           AND row_num NOT IN (
                             SELECT h.row_num
                             FROM {history_table} h
                             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                             WHERE h.row_num = ?
                               AND h.j_branch_num = ?
                               AND h.op != 3
                               AND tx.outcome != ?
                           )",
                        crate::schema::current_table(&query_read.table),
                        history_table = crate::schema::history_table(&query_read.table),
                    ),
                    params![
                        branch_num,
                        row_num,
                        row_num,
                        branch_num,
                        tx::OUTCOME_REJECTED
                    ],
                )?;
            }
            return Ok(());
        }
        if query_read.field == "$createdBy" {
            let Some(created_by) = query_read.value.as_str() else {
                return Err(crate::Error::new(
                    "$createdBy predicate expects a string value",
                ));
            };
            let created_by_num = users::ensure_user(db, created_by)?;
            let created_by_sql = match query_read.op.as_str() {
                "eq" => "j_created_by = ?",
                "ne" => "j_created_by != ?",
                op => {
                    return Err(crate::Error::new(format!(
                        "unsupported $createdBy predicate op {op}"
                    )));
                }
            };
            let history_created_by_sql = match query_read.op.as_str() {
                "eq" => "h.j_created_by = ?",
                "ne" => "h.j_created_by != ?",
                _ => unreachable!("validated above"),
            };
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND {created_by_sql}
                       AND row_num NOT IN (
                         SELECT h.row_num
                         FROM {history_table} h
                         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                         WHERE h.j_branch_num = ?
                           AND {history_created_by_sql}
                           AND h.op != 3
                           AND tx.outcome != ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    history_table = crate::schema::history_table(&query_read.table),
                ),
                params![
                    branch_num,
                    created_by_num,
                    branch_num,
                    created_by_num,
                    tx::OUTCOME_REJECTED
                ],
            )?;
            return Ok(());
        }
        let table = schema.table_def(&query_read.table)?;
        let field = table
            .fields
            .iter()
            .find(|candidate| candidate.name == query_read.field)
            .ok_or_else(|| {
                crate::Error::new(format!("unknown query field {}", query_read.field))
            })?;
        let branch_num = branch::checkout(db, &query_read.branch_id)?;
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let predicate_sql = query_predicate::sql(field, &predicate_column, &query_read.op)?;
        let predicate_value = query_predicate::value(field, &query_read.op, &query_read.value, db)?;
        db.execute(
            &format!(
                "DELETE FROM {}
                 WHERE j_branch_num = ?
                   AND is_deleted = 0
                   AND {predicate_sql}
                   AND row_num NOT IN (
                     SELECT ids.row_num
                     FROM jazz_row_id ids
                     JOIN {history_table} h ON h.row_num = ids.row_num
                     JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                     WHERE h.j_branch_num = ?
                       AND h.op != 3
                       AND tx.outcome != ?
                       AND {history_predicate_sql}
                   )",
                crate::schema::current_table(&query_read.table),
                history_table = crate::schema::history_table(&query_read.table),
                history_predicate_sql =
                    query_predicate::sql(field, &format!("h.{predicate_column}"), &query_read.op)?,
            ),
            params![
                branch_num,
                predicate_value.clone(),
                branch_num,
                tx::OUTCOME_REJECTED,
                predicate_value
            ],
        )?;
        Ok(())
    }

    fn apply_built_query_scope_repair(
        schema: &SchemaDef,
        db: &Connection,
        query_read: &QueryReadRecord,
        built_query: &BuiltQuery,
        user: &str,
        bypass_policy: bool,
    ) -> Result<()> {
        // Built queries are recorded as one opaque query-read value so they can
        // be replayed exactly after reconnect:
        //
        //   jazz_query_read
        //   field="$query", op="query", value=<BuiltQuery JSON>
        //
        // Repair keeps the old narrow fast path for simple predicates, then
        // falls back to a generic SQL-lowered pass for the wider built-query
        // language:
        //
        //   +-----------------------------+
        //   | BuiltQuery descriptor       |
        //   +-----------------------------+
        //        | one predicate only
        //        v
        //   +-----------------------------+      +--------------------------+
        //   | QueryReadRecord predicate   | ---> | apply_query_scope_repair |
        //   +-----------------------------+      +--------------------------+
        //
        //        | every other SQL-lowered shape
        //        v
        //   +-----------------------------+
        //   | generic SQL-lowered repair  |
        //   +-----------------------------+
        match built_query_repair_scope(built_query)? {
            BuiltQueryRepairScope::Predicate(condition) => {
                let predicate_read = QueryReadRecord {
                    branch_id: query_read.branch_id.clone(),
                    table: built_query.table.clone(),
                    field: condition.column.clone(),
                    op: condition.op.as_str().to_owned(),
                    value: condition.value.clone(),
                };
                Self::apply_query_scope_repair(schema, db, &predicate_read, user, bypass_policy)
            }
            BuiltQueryRepairScope::Generic => Self::apply_generic_built_query_scope_repair(
                schema,
                db,
                query_read,
                built_query,
                user,
                bypass_policy,
            ),
        }
    }

    fn apply_generic_built_query_scope_repair(
        schema: &SchemaDef,
        db: &Connection,
        query_read: &QueryReadRecord,
        built_query: &BuiltQuery,
        user: &str,
        bypass_policy: bool,
    ) -> Result<()> {
        if built_query.limit.is_none() && built_query.offset.unwrap_or(0) == 0 {
            return Ok(());
        }

        let branch_num = branch::checkout(db, &query_read.branch_id)?;
        let context = query::QueryContext {
            conn: db,
            schema,
            branch_num,
            user,
            bypass_policy,
            read_tier: ReadTier::Local,
        };
        let mut scope_query = built_query.clone();
        scope_query.limit = None;
        scope_query.offset = None;
        let scope_row_nums = context
            .read_rows_for_built_query(&scope_query)?
            .iter()
            .map(|row| row_num(db, &row.id))
            .collect::<Result<Vec<_>>>()?;
        if scope_row_nums.is_empty() {
            return Ok(());
        }

        let keep_query = built_query_repair_keep_query(built_query)?;
        let keep_row_nums = context
            .read_rows_for_built_query(&keep_query)?
            .iter()
            .map(|row| row_num(db, &row.id))
            .collect::<Result<Vec<_>>>()?;
        delete_current_rows_outside_keep_set(
            db,
            &built_query.table,
            branch_num,
            &scope_row_nums,
            &keep_row_nums,
        )
    }

    fn apply_history_record(
        context: &mut ApplyHistoryContext<'_>,
        record: &HistoryRecord,
    ) -> Result<()> {
        let table = context.schema.table_def(&record.table)?;
        let row_num = context.apply_caches.ensure_row_id_with_status(
            context.db,
            &record.table,
            &record.row_id,
        )?;
        if row_id_used_by_other_table(context.db, context.schema, &record.table, row_num)? {
            return Err(crate::Error::new(format!(
                "row id {} is already used by another table",
                record.row_id
            )));
        }
        let tx_num = context
            .tx_nums_by_id
            .get(&record.tx_id)
            .copied()
            .map(Ok)
            .unwrap_or_else(|| tx::tx_num(context.db, &record.tx_id))?;
        let branch_num = context
            .branch_nums_by_id
            .get(&record.branch_id)
            .copied()
            .map(Ok)
            .unwrap_or_else(|| branch::ensure(context.db, &record.branch_id, None, now_ms()))?;
        let tx_info = context
            .tx_info_by_num
            .get(&tx_num)
            .copied()
            .map(Ok)
            .unwrap_or_else(|| tx_apply_info(context.db, tx_num))?;
        let outcome = tx_info.outcome;
        let history_exists = history_record_exists(context.db, &record.table, row_num, tx_num)?;
        if history_exists
            && current_visible_tx_num(context.db, &record.table, row_num, branch_num)?
                .is_some_and(|current_tx_num| current_tx_num == tx_num)
        {
            return Ok(());
        }

        let mut columns = vec![
            "row_num".to_owned(),
            "tx_num".to_owned(),
            "j_branch_num".to_owned(),
            "op".to_owned(),
        ];
        let mut values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(tx_num),
            rusqlite::types::Value::Integer(branch_num),
            rusqlite::types::Value::Integer(record.op),
        ];
        for field in &table.fields {
            let value = record
                .values
                .get(&field.name)
                .or_else(|| record.values.get(&field.storage_name))
                .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
            columns.push(crate::schema::quote_ident(&crate::schema::storage_column(
                field,
            )));
            values.push(crate::schema::field_sql_value(
                field,
                value,
                |ref_table, row_id| {
                    context
                        .apply_caches
                        .ensure_row_id(context.db, ref_table, row_id)
                },
            )?);
        }
        columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);
        let created_by_num = context
            .apply_caches
            .ensure_user(context.db, &record.created_by)?;
        let updated_by_num = context
            .apply_caches
            .ensure_user(context.db, &record.updated_by)?;
        values.extend([
            rusqlite::types::Value::Integer(record.created_at),
            rusqlite::types::Value::Integer(record.updated_at),
            rusqlite::types::Value::Integer(created_by_num),
            rusqlite::types::Value::Integer(updated_by_num),
        ]);
        if !history_exists {
            insert_dynamic(
                context.db,
                &crate::schema::history_table(&record.table),
                &columns,
                &values,
            )?;
        }
        let table_num = context
            .table_nums_by_name
            .get(&record.table)
            .copied()
            .ok_or_else(|| crate::Error::new("history record references missing table"))?;
        if !history_exists {
            record_tx_write_num(context.db, tx_num, table_num, row_num, record.op)?;
        }
        if outcome == tx::OUTCOME_PENDING && tx_info.conflict_mode == tx::MODE_EXCLUSIVE {
            return Ok(());
        }
        if tx_info.outcome == tx::OUTCOME_PENDING
            && tx_info.node_num != context.local_node_num
            && durable_version_exists_for_row(context.db, &record.table, row_num, branch_num)?
        {
            return Ok(());
        }
        if outcome != tx::OUTCOME_REJECTED {
            if let Some(current_tx_num) =
                current_visible_tx_num(context.db, &record.table, row_num, branch_num)?
            {
                if let Some(is_newer) =
                    tx_is_newer_than_current_fast_path(context.db, tx_num, current_tx_num)?
                {
                    if !is_newer {
                        return Ok(());
                    }
                } else if !is_newest_version_for_current(
                    context.db,
                    &record.table,
                    row_num,
                    branch_num,
                    tx_num,
                )? {
                    return Ok(());
                }
            } else if !context.apply_caches.row_created_in_apply(row_num)
                && !is_newest_version_for_current(
                    context.db,
                    &record.table,
                    row_num,
                    branch_num,
                    tx_num,
                )?
            {
                return Ok(());
            }
        }
        if outcome != tx::OUTCOME_REJECTED && record.op == 3 {
            context.db.execute(
                &format!(
                    "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
                    crate::schema::current_table(&record.table)
                ),
                params![row_num, branch_num],
            )?;
            if branch_num != 1 {
                let mut current_columns = vec![
                    "row_num".to_owned(),
                    "j_branch_num".to_owned(),
                    "visible_tx_num".to_owned(),
                    "is_deleted".to_owned(),
                ];
                let mut current_values = vec![
                    rusqlite::types::Value::Integer(row_num),
                    rusqlite::types::Value::Integer(branch_num),
                    rusqlite::types::Value::Integer(tx_num),
                    rusqlite::types::Value::Integer(1),
                ];
                current_columns.extend(columns.iter().skip(4).cloned());
                current_values.extend(values.iter().skip(4).cloned());
                insert_dynamic(
                    context.db,
                    &crate::schema::current_table(&record.table),
                    &current_columns,
                    &current_values,
                )?;
            }
        } else if outcome != tx::OUTCOME_REJECTED {
            let mut current_columns = vec![
                "row_num".to_owned(),
                "j_branch_num".to_owned(),
                "visible_tx_num".to_owned(),
                "is_deleted".to_owned(),
            ];
            let mut current_values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(branch_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(0),
            ];
            current_columns.extend(columns.iter().skip(4).cloned());
            current_values.extend(values.iter().skip(4).cloned());
            insert_dynamic(
                context.db,
                &crate::schema::current_table(&record.table),
                &current_columns,
                &current_values,
            )?;
        }
        Ok(())
    }

    fn export_many_predicate_query_refreshes(
        &self,
        table_name: &str,
        field_name: &str,
        op: &str,
        values: Vec<(JsonValue, Vec<String>)>,
    ) -> Result<Bundle> {
        let mut items = Vec::new();
        for (value, extra_row_ids) in values {
            let rows = match op {
                "eq" => self.read_rows_where_eq(table_name, field_name, value.clone())?,
                "ne" => self.read_rows_where_ne(table_name, field_name, value.clone())?,
                "contains" => {
                    let Some(needle) = value.as_str() else {
                        return Err(crate::Error::new("contains expects a string value"));
                    };
                    self.read_rows_where_contains(table_name, field_name, needle)?
                }
                "in" => {
                    let Some(values) = value.as_array() else {
                        return Err(crate::Error::new("in predicate expects an array value"));
                    };
                    self.read_rows_where_in(table_name, field_name, values.clone())?
                }
                op => {
                    return Err(crate::Error::new(format!(
                        "unsupported batched predicate refresh {op}"
                    )));
                }
            };
            items.push(BatchedQueryScopeItem {
                op: op.to_owned(),
                value,
                rows,
                extra_row_ids,
            });
        }
        self.export_batched_query_scopes(table_name, field_name, items, &[])
    }

    fn export_query_where_eq_top_created_at_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
        previous_observed_ids: Vec<String>,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_created_at_desc(
            table_name,
            field_name,
            value.clone(),
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_created_at_desc",
            json!({
                "eq": value.clone(),
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[],
                extra_row_ids: &previous_observed_ids,
            },
        )
    }

    fn export_many_query_where_eq_top_created_at_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<(JsonValue, Vec<String>)>,
        limit: usize,
    ) -> Result<Bundle> {
        let value_only = values
            .iter()
            .map(|(value, _)| value.clone())
            .collect::<Vec<_>>();
        let rows_by_value = self
            .query_context()
            .read_many_rows_where_eq_top_created_at_desc(
                table_name,
                field_name,
                &value_only,
                limit,
            )?;
        let mut items = Vec::new();
        for ((value, previous_observed_ids), rows) in values.into_iter().zip(rows_by_value) {
            items.push(BatchedQueryScopeItem {
                op: "eq_top_created_at_desc".to_owned(),
                value: json!({
                    "eq": value.clone(),
                    "limit": limit,
                    "observed_ids": observed_row_ids(&rows),
                }),
                rows,
                extra_row_ids: previous_observed_ids,
            });
        }
        self.export_batched_query_scopes(table_name, field_name, items, &[])
    }

    fn export_many_query_where_eq_top_field_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<(JsonValue, Vec<String>)>,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_many_query_where_eq_top_field_desc_inner(
            table_name,
            field_name,
            values,
            order_field_name,
            limit,
            &[],
        )
    }

    fn export_many_query_where_eq_top_field_desc_inner(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<(JsonValue, Vec<String>)>,
        order_field_name: &str,
        limit: usize,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let value_only = values
            .iter()
            .map(|(value, _)| value.clone())
            .collect::<Vec<_>>();
        let rows_by_value = self
            .query_context()
            .read_many_rows_where_eq_top_field_desc(
                table_name,
                field_name,
                &value_only,
                order_field_name,
                limit,
            )?;
        let mut items = Vec::new();
        for ((value, previous_observed_ids), rows) in values.into_iter().zip(rows_by_value) {
            items.push(BatchedQueryScopeItem {
                op: "eq_top_field_desc".to_owned(),
                value: json!({
                    "eq": value.clone(),
                    "order_field": order_field_name,
                    "limit": limit,
                    "observed_ids": observed_row_ids(&rows),
                }),
                rows,
                extra_row_ids: previous_observed_ids,
            });
        }
        self.export_batched_query_scopes(table_name, field_name, items, ref_include_fields)
    }

    pub(crate) fn export_built_query_scope(
        &self,
        query: BuiltQuery,
        rows: Vec<RowView>,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        self.export_built_query_scope_with_previous_observed(
            query,
            rows,
            ref_include_fields,
            Vec::new(),
        )
    }

    fn export_built_query_scope_with_previous_observed(
        &self,
        query: BuiltQuery,
        rows: Vec<RowView>,
        ref_include_fields: &[&str],
        previous_observed_ids: Vec<String>,
    ) -> Result<Bundle> {
        let query_read = QueryReadRecord {
            branch_id: branch::id_for_num(&self.conn, self.branch_num)?,
            table: query.table.clone(),
            field: "$query".to_owned(),
            op: "query".to_owned(),
            value: built_query_read_value(&query, &rows),
        };
        let support_query = support_window_query(&query)?;
        if let Some(row_scope) = self
            .query_context()
            .lower_built_query_row_scope(&support_query)?
        {
            return self.export_built_query_read_scope_sql(
                query_read,
                &query,
                &row_scope,
                rows,
                QueryScopeOptions {
                    ref_include_fields,
                    extra_row_ids: &previous_observed_ids,
                },
            );
        }
        self.export_query_read_scope(
            query_read,
            rows,
            QueryScopeOptions {
                ref_include_fields,
                extra_row_ids: &previous_observed_ids,
            },
        )
    }

    fn export_query_read_scope(
        &self,
        query_read: QueryReadRecord,
        rows: Vec<RowView>,
        options: QueryScopeOptions<'_>,
    ) -> Result<Bundle> {
        // Query-scope exports carry more than the rows currently visible in the
        // query result. They also carry repair candidates: rows whose history
        // previously satisfied the same query scope and may need to be removed
        // or updated on a receiver.
        //
        //   +---------------------+
        //   | query result rows   |
        //   +---------------------+
        //             |
        //             v
        //   +---------------------+      +---------------------+
        //   | result row nums     | ---> | exported history    |
        //   +---------------------+      +---------------------+
        //             ^
        //             |
        //   +---------------------+
        //   | repair row nums     |
        //   +---------------------+
        //
        // Without the repair rows, query-scoped sync would only add or update
        // rows that are still in the result. A receiver would not learn that a
        // previously synced row left the predicate or page boundary.
        let table_name = &query_read.table;
        let table = self.schema.table_def(table_name)?;
        let user = self.policy_user();
        let bypass_policy = self.bypasses_policy();
        let visible_row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let mut repair_row_nums = Vec::new();
        for row_id in options.extra_row_ids {
            repair_row_nums.push(row_num(&self.conn, row_id)?);
        }
        repair_row_nums.extend(query_scope_repair_row_nums_for_read(
            &self.conn,
            &self.schema,
            table,
            &query_read,
            self.branch_num,
            user,
            bypass_policy,
        )?);
        let visible_row_num_set = visible_row_nums.iter().copied().collect::<BTreeSet<_>>();
        repair_row_nums.retain(|row_num| !visible_row_num_set.contains(row_num));
        repair_row_nums.sort();
        repair_row_nums.dedup();
        let mut row_nums = visible_row_nums.clone();
        row_nums.extend(repair_row_nums.iter());
        row_nums.sort();
        row_nums.dedup();
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut history = export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            Some(&visible_row_nums),
            None,
            &branch_nums,
        )?;
        if !repair_row_nums.is_empty() {
            history.extend(export_visible_table_history(
                &visibility,
                table_name,
                &branch_nums,
                Some(&repair_row_nums),
            )?);
            history.extend(export_history_versions_for_rows_in_branches(
                &self.conn,
                &self.schema,
                table_name,
                Some(&repair_row_nums),
                None,
                &branch_nums,
            )?);
        }
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &table.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        for ref_field_name in options.ref_include_fields {
            history.extend(self.export_ref_include_history(
                table,
                &rows,
                ref_field_name,
                &branch_nums,
            )?);
        }
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let rejected_tx_ids = query_scope_rejected_tx_ids_for_read(
            &self.conn,
            &self.schema,
            table,
            &query_read,
            self.branch_num,
            user,
            bypass_policy,
        )?;
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        let query_reads = vec![query_read];
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            query_reads,
            history,
        ))
    }

    fn export_query_where_eq_top_field_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
        previous_observed_ids: Vec<String>,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_field_desc(
            table_name,
            field_name,
            value.clone(),
            order_field_name,
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_field_desc",
            json!({
                "eq": value.clone(),
                "order_field": order_field_name,
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[],
                extra_row_ids: &previous_observed_ids,
            },
        )
    }

    pub(crate) fn export_query_scope(
        &self,
        table_name: &str,
        field_name: &str,
        op: &str,
        value: JsonValue,
        rows: Vec<RowView>,
        options: QueryScopeOptions<'_>,
    ) -> Result<Bundle> {
        let query_read = QueryReadRecord {
            branch_id: branch::id_for_num(&self.conn, self.branch_num)?,
            table: table_name.to_owned(),
            field: field_name.to_owned(),
            op: op.to_owned(),
            value,
        };
        self.export_query_read_scope(query_read, rows, options)
    }

    fn export_batched_query_scopes(
        &self,
        table_name: &str,
        field_name: &str,
        items: Vec<BatchedQueryScopeItem>,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let table = self.schema.table_def(table_name)?;
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut all_rows = Vec::new();
        let mut visible_row_nums = Vec::new();
        let mut repair_row_nums = Vec::new();
        let mut rejected_tx_ids = Vec::new();
        let mut query_reads = Vec::new();

        for item in items {
            let row_nums = item
                .rows
                .iter()
                .map(|row| row_num(&self.conn, &row.id))
                .collect::<Result<Vec<_>>>()?;
            for row_id in &item.extra_row_ids {
                repair_row_nums.push(row_num(&self.conn, row_id)?);
            }
            repair_row_nums.extend(query_scope_repair_row_nums(
                &self.conn,
                table,
                field_name,
                &item.op,
                &item.value,
            )?);
            rejected_tx_ids.extend(query_scope_rejected_tx_ids(
                &self.conn,
                table,
                field_name,
                &item.op,
                &item.value,
            )?);
            query_reads.push(QueryReadRecord {
                branch_id: branch::id_for_num(&self.conn, self.branch_num)?,
                table: table_name.to_owned(),
                field: field_name.to_owned(),
                op: item.op,
                value: item.value,
            });
            visible_row_nums.extend(row_nums);
            all_rows.extend(item.rows);
        }

        visible_row_nums.sort();
        visible_row_nums.dedup();
        let visible_row_num_set = visible_row_nums.iter().copied().collect::<BTreeSet<_>>();
        repair_row_nums.retain(|row_num| !visible_row_num_set.contains(row_num));
        repair_row_nums.sort();
        repair_row_nums.dedup();
        let mut row_nums = visible_row_nums.clone();
        row_nums.extend(repair_row_nums.iter());
        row_nums.sort();
        row_nums.dedup();
        rejected_tx_ids.sort();
        rejected_tx_ids.dedup();

        let mut history = export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            Some(&visible_row_nums),
            None,
            &branch_nums,
        )?;
        if !repair_row_nums.is_empty() {
            history.extend(export_visible_table_history(
                &visibility,
                table_name,
                &branch_nums,
                Some(&repair_row_nums),
            )?);
            history.extend(export_history_versions_for_rows_in_branches(
                &self.conn,
                &self.schema,
                table_name,
                Some(&repair_row_nums),
                None,
                &branch_nums,
            )?);
        }
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &table.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        for ref_field_name in ref_include_fields {
            history.extend(self.export_ref_include_history(
                table,
                &all_rows,
                ref_field_name,
                &branch_nums,
            )?);
        }
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            query_reads,
            history,
        ))
    }

    fn export_ref_include_history(
        &self,
        table: &crate::schema::TableDef,
        rows: &[RowView],
        ref_field_name: &str,
        branch_nums: &[i64],
    ) -> Result<Vec<HistoryRecord>> {
        let field = table
            .fields
            .iter()
            .find(|field| field.name == ref_field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown include field {ref_field_name}")))?;
        let FieldKind::Ref {
            table: ref_table_name,
        } = &field.kind
        else {
            return Err(crate::Error::new(format!(
                "include field {ref_field_name} is not a ref"
            )));
        };
        let ref_row_nums = rows
            .iter()
            .filter_map(|row| row.values.get(ref_field_name).and_then(JsonValue::as_str))
            .map(|id| row_num(&self.conn, id))
            .collect::<Result<Vec<_>>>()?;
        let mut ref_row_nums = ref_row_nums;
        ref_row_nums.sort();
        ref_row_nums.dedup();
        if ref_row_nums.is_empty() {
            return Ok(Vec::new());
        }
        let visibility = self.read_visibility();
        let mut history = export_visible_table_history(
            &visibility,
            ref_table_name,
            branch_nums,
            Some(&ref_row_nums),
        )?;
        history.extend(export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            ref_table_name,
            Some(&ref_row_nums),
            None,
            branch_nums,
        )?);
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name: ref_table_name,
                policy: &self.schema.table_def(ref_table_name)?.read_policy,
                branch_nums,
                child_row_nums: Some(&ref_row_nums),
            },
        )?);
        Ok(history)
    }

    fn query_context(&self) -> query::QueryContext<'_> {
        self.query_context_at_tier(ReadTier::Local)
    }

    pub(crate) fn query_context_at_tier(&self, read_tier: ReadTier) -> query::QueryContext<'_> {
        query::QueryContext {
            conn: &self.conn,
            schema: &self.schema,
            branch_num: self.branch_num,
            user: self.policy_user(),
            bypass_policy: self.bypasses_policy(),
            read_tier,
        }
    }

    fn read_visibility(&self) -> ReadVisibility<'_> {
        ReadVisibility {
            conn: &self.conn,
            schema: &self.schema,
            branch_num: self.branch_num,
            user: self.policy_user(),
            bypass_policy: self.bypasses_policy(),
        }
    }
}

struct InsertRowInTx<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table_name: &'a str,
    id: &'a str,
    values: &'a BTreeMap<String, JsonValue>,
    tx_num: i64,
    branch_num: i64,
    now: i64,
    user: &'a str,
    bypass_policy: bool,
    op: i64,
    base_values: Option<&'a BTreeMap<String, JsonValue>>,
}

struct EffectiveWriteValues<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table_name: &'a str,
    id: &'a str,
    row_num: i64,
    branch_num: i64,
    patch_values: &'a BTreeMap<String, JsonValue>,
    op: i64,
    base_values: Option<&'a BTreeMap<String, JsonValue>>,
}

fn effective_write_values(args: EffectiveWriteValues<'_>) -> Result<BTreeMap<String, JsonValue>> {
    let table = args.schema.table_def(args.table_name)?;
    if args.op == 1 {
        let mut values = args.patch_values.clone();
        for field in &table.fields {
            if !values.contains_key(&field.name) {
                if let Some(default_value) = &field.default_value {
                    values.insert(field.name.clone(), default_value.clone());
                }
            }
        }
        return Ok(values);
    }
    let mut current = if let Some(base_values) = args.base_values {
        base_values.clone()
    } else {
        effective::row_values(
            args.db,
            args.schema,
            args.table_name,
            args.row_num,
            args.branch_num,
        )?
        .ok_or_else(|| crate::Error::new(format!("row {} is not visible", args.id)))?
    };
    current.extend(args.patch_values.clone());
    Ok(current)
}

fn insert_row_in_tx(args: InsertRowInTx<'_>) -> Result<bool> {
    let table = args.schema.table_def(args.table_name)?;
    validate_write_fields(table, args.values)?;
    let (row_num, row_id_created) = ensure_row_id_with_status(args.db, args.id)?;
    if args.op == 1 && !row_id_created {
        if row_id_used_by_other_table(args.db, args.schema, args.table_name, row_num)? {
            return Err(crate::Error::new(format!(
                "row id {} is already used by another table",
                args.id
            )));
        }
        if row_has_current_branch_value(args.db, args.table_name, row_num, args.branch_num)? {
            return Err(crate::Error::new(format!(
                "row id {} already exists in table {}",
                args.id, args.table_name
            )));
        }
    }
    let effective_values = effective_write_values(EffectiveWriteValues {
        db: args.db,
        schema: args.schema,
        table_name: args.table_name,
        id: args.id,
        row_num,
        branch_num: args.branch_num,
        patch_values: args.values,
        op: args.op,
        base_values: args.base_values,
    })?;
    if args.op == 1 {
        if row_id_created {
            read_set::record_tx_absent_read(args.db, args.tx_num, args.table_name, row_num)?;
        } else {
            read_set::record_tx_create_read(
                args.db,
                args.tx_num,
                args.table_name,
                row_num,
                args.branch_num,
            )?;
        }
    } else {
        read_set::record_tx_read(
            args.db,
            args.tx_num,
            args.table_name,
            row_num,
            args.branch_num,
            2,
        )?;
    }
    policy_read_set::record_for_write(policy_read_set::WritePolicyReadSet {
        conn: args.db,
        schema: args.schema,
        table,
        policy: &table.write_policy,
        values: &effective_values,
        branch_num: args.branch_num,
        tx_num: args.tx_num,
    })?;
    let allowed = args.bypass_policy
        || local_write_allowed(LocalWriteCheck {
            db: args.db,
            schema: args.schema,
            table,
            row_num,
            branch_num: args.branch_num,
            values: &effective_values,
            user: args.user,
            op: args.op,
        })?;

    let mut columns = vec![
        "row_num".to_owned(),
        "tx_num".to_owned(),
        "j_branch_num".to_owned(),
        "op".to_owned(),
    ];
    let mut sql_values = vec![
        rusqlite::types::Value::Integer(row_num),
        rusqlite::types::Value::Integer(args.tx_num),
        rusqlite::types::Value::Integer(args.branch_num),
        rusqlite::types::Value::Integer(args.op),
    ];

    for field in &table.fields {
        let value = effective_values
            .get(&field.name)
            .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
        columns.push(crate::schema::quote_ident(&crate::schema::storage_column(
            field,
        )));
        sql_values.push(crate::schema::field_sql_value(
            field,
            value,
            |ref_table, row_id| ensure_row_id(args.db, ref_table, row_id),
        )?);
    }
    columns.extend([
        "j_created_at".to_owned(),
        "j_updated_at".to_owned(),
        "j_created_by".to_owned(),
        "j_updated_by".to_owned(),
    ]);
    let (created_at, created_by) = if args.op == 1 {
        (args.now, args.user.to_owned())
    } else {
        current_creation_metadata(args.db, &table.name, row_num, args.branch_num)?
            .unwrap_or((args.now, args.user.to_owned()))
    };
    let created_by_num = users::ensure_user(args.db, &created_by)?;
    let updated_by_num = users::ensure_user(args.db, args.user)?;
    sql_values.extend([
        rusqlite::types::Value::Integer(created_at),
        rusqlite::types::Value::Integer(args.now),
        rusqlite::types::Value::Integer(created_by_num),
        rusqlite::types::Value::Integer(updated_by_num),
    ]);
    insert_dynamic(
        args.db,
        &crate::schema::history_table(&table.name),
        &columns,
        &sql_values,
    )?;
    record_tx_write(args.db, args.tx_num, &table.name, row_num, args.op)?;

    if allowed {
        let mut current_columns = vec![
            "row_num".to_owned(),
            "j_branch_num".to_owned(),
            "visible_tx_num".to_owned(),
            "is_deleted".to_owned(),
        ];
        let mut current_values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(args.branch_num),
            rusqlite::types::Value::Integer(args.tx_num),
            rusqlite::types::Value::Integer(0),
        ];
        current_columns.extend(columns.iter().skip(4).cloned());
        current_values.extend(sql_values.iter().skip(4).cloned());
        insert_dynamic(
            args.db,
            &crate::schema::current_table(&table.name),
            &current_columns,
            &current_values,
        )?;
    }
    Ok(allowed)
}

fn row_has_current_branch_value(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {}
             WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
            crate::schema::current_table(table_name)
        ),
        params![row_num, branch_num],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn row_id_used_by_other_table(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_num: i64,
) -> Result<bool> {
    for table in schema.tables() {
        if table.name == table_name {
            continue;
        }
        let history_sql = format!(
            "SELECT 1 FROM {} WHERE row_num = ? LIMIT 1",
            crate::schema::history_table(&table.name)
        );
        if conn
            .query_row(&history_sql, params![row_num], |_| Ok(()))
            .optional()?
            .is_some()
        {
            return Ok(true);
        }
        let current_sql = format!(
            "SELECT 1 FROM {} WHERE row_num = ? LIMIT 1",
            crate::schema::current_table(&table.name)
        );
        if conn
            .query_row(&current_sql, params![row_num], |_| Ok(()))
            .optional()?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_write_fields(
    table: &crate::schema::TableDef,
    values: &BTreeMap<String, JsonValue>,
) -> Result<()> {
    let schema_fields = table
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    for field_name in values.keys() {
        if !schema_fields.contains(field_name.as_str()) {
            return Err(crate::Error::new(format!(
                "unknown field {} on table {}",
                field_name, table.name
            )));
        }
    }
    Ok(())
}

struct LocalWriteCheck<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table: &'a crate::schema::TableDef,
    row_num: i64,
    branch_num: i64,
    values: &'a BTreeMap<String, JsonValue>,
    user: &'a str,
    op: i64,
}

fn local_write_allowed(check: LocalWriteCheck<'_>) -> Result<bool> {
    if check.op == 1 && matches!(check.table.write_policy, PolicyDef::CreatedByUser) {
        return Ok(true);
    }
    policy::write_allowed(policy::WriteCheck {
        db: check.db,
        schema: check.schema,
        table: check.table,
        row_num: check.row_num,
        branch_num: check.branch_num,
        values: check.values,
        user: check.user,
    })
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

fn current_creation_metadata(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<(i64, String)>> {
    let metadata = conn
        .query_row(
            &format!(
                "SELECT j_created_at, j_created_by
             FROM {}
             WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
                crate::schema::current_table(table_name)
            ),
            params![row_num, branch_num],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    metadata
        .map(|(created_at, created_by_num)| {
            users::user_id(conn, created_by_num).map(|created_by| (created_at, created_by))
        })
        .transpose()
}

fn exclusive_write_conflict_exists(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*)
         FROM jazz_tx_write writes
         JOIN jazz_tx tx ON tx.tx_num = writes.tx_num
         WHERE writes.table_num = ?
           AND writes.row_num = ?
           AND tx.conflict_mode = ?
           AND tx.outcome = ?",
        params![
            crate::schema::table_num(conn, table_name)?,
            row_num,
            tx::MODE_EXCLUSIVE,
            tx::OUTCOME_ACCEPTED
        ],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn record_tx_write(
    conn: &Connection,
    tx_num: i64,
    table_name: &str,
    row_num: i64,
    op: i64,
) -> Result<()> {
    let table_num = crate::schema::table_num(conn, table_name)?;
    record_tx_write_num(conn, tx_num, table_num, row_num, op)
}

fn record_tx_write_num(
    conn: &Connection,
    tx_num: i64,
    table_num: i64,
    row_num: i64,
    op: i64,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT OR REPLACE INTO jazz_tx_write (tx_num, table_num, row_num, op)
         VALUES (?, ?, ?, ?)",
    )?;
    stmt.execute(params![tx_num, table_num, row_num, op])?;
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

fn export_txs(conn: &Connection) -> Result<Vec<TxRecord>> {
    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, node.node_id, tx.local_epoch, tx.global_epoch, tx.conflict_mode, tx.outcome, rejection.code, rejection.detail_json, tx.created_at, tx.metadata_json
         FROM jazz_tx tx
         JOIN jazz_node node ON node.node_num = tx.node_num
         LEFT JOIN jazz_tx_rejection rejection ON rejection.tx_num = tx.tx_num
         ORDER BY tx.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        let tx_id = row.get::<_, String>(0)?;
        let mut receipt_stmt = conn.prepare(
            "SELECT receipt.tier
             FROM jazz_tx_receipt receipt
             JOIN jazz_tx tx ON tx.tx_num = receipt.tx_num
             WHERE tx.tx_id = ?
             ORDER BY receipt.tier",
        )?;
        let receipt_tiers = receipt_stmt
            .query_map(params![tx_id], |row| row.get::<_, i64>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(TxRecord {
            tx_id,
            node_id: row.get(1)?,
            local_epoch: row.get(2)?,
            global_epoch: row.get(3)?,
            conflict_mode: row.get(4)?,
            outcome: row.get(5)?,
            auth_user: parse_tx_auth_user_for_sqlite(&row.get::<_, String>(9)?, 9)?,
            rejection_code: row.get(6)?,
            rejection_detail: row
                .get::<_, Option<String>>(7)?
                .map(|detail_json| parse_rejection_detail_for_sqlite(&detail_json, 7))
                .transpose()?
                .flatten(),
            receipt_tiers,
            created_at: row.get(8)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn export_txs_for_query_scope(
    conn: &Connection,
    _table_name: &str,
    history: &[HistoryRecord],
    reads: &[ReadRecord],
    extra_tx_ids: &[String],
) -> Result<Vec<TxRecord>> {
    let mut needed_tx_ids = history
        .iter()
        .map(|record| record.tx_id.as_str())
        .collect::<BTreeSet<_>>();
    for tx_id in extra_tx_ids {
        needed_tx_ids.insert(tx_id.as_str());
    }
    for record in reads {
        needed_tx_ids.insert(record.tx_id.as_str());
        if let Some(observed_tx_id) = &record.observed_tx_id {
            needed_tx_ids.insert(observed_tx_id.as_str());
        }
    }
    export_txs_by_ids(conn, needed_tx_ids)
}

fn export_txs_by_ids(conn: &Connection, tx_ids: BTreeSet<&str>) -> Result<Vec<TxRecord>> {
    if tx_ids.is_empty() {
        return Ok(Vec::new());
    }
    let tx_ids = tx_ids.into_iter().collect::<Vec<_>>();

    let mut receipt_stmt = conn.prepare(
        "SELECT tx.tx_id, receipt.tier
         FROM jazz_tx tx
         JOIN jazz_tx_receipt receipt ON receipt.tx_num = tx.tx_num
         ORDER BY tx.tx_num, receipt.tier",
    )?;
    let receipt_rows = receipt_stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut receipt_tiers_by_tx = BTreeMap::<String, Vec<i64>>::new();
    for receipt_row in receipt_rows {
        let (tx_id, tier) = receipt_row?;
        if tx_ids.contains(&tx_id.as_str()) {
            receipt_tiers_by_tx.entry(tx_id).or_default().push(tier);
        }
    }

    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, node.node_id, tx.local_epoch, tx.global_epoch, tx.conflict_mode, tx.outcome, rejection.code, rejection.detail_json, tx.created_at, tx.metadata_json
         FROM jazz_tx tx
         JOIN jazz_node node ON node.node_num = tx.node_num
         LEFT JOIN jazz_tx_rejection rejection ON rejection.tx_num = tx.tx_num
         ORDER BY tx.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        let tx_id = row.get::<_, String>(0)?;
        let receipt_tiers = receipt_tiers_by_tx.get(&tx_id).cloned().unwrap_or_default();
        Ok(TxRecord {
            tx_id,
            node_id: row.get(1)?,
            local_epoch: row.get(2)?,
            global_epoch: row.get(3)?,
            conflict_mode: row.get(4)?,
            outcome: row.get(5)?,
            auth_user: parse_tx_auth_user_for_sqlite(&row.get::<_, String>(9)?, 9)?,
            rejection_code: row.get(6)?,
            rejection_detail: row
                .get::<_, Option<String>>(7)?
                .map(|detail_json| parse_rejection_detail_for_sqlite(&detail_json, 7))
                .transpose()?
                .flatten(),
            receipt_tiers,
            created_at: row.get(8)?,
        })
    })?;
    let mut tx_records = Vec::new();
    for record in records {
        let record = record?;
        if tx_ids.contains(&record.tx_id.as_str()) {
            tx_records.push(record);
        }
    }
    Ok(tx_records)
}

fn parse_rejection_detail(detail_json: &str) -> Result<Option<JsonValue>> {
    let detail = serde_json::from_str::<JsonValue>(detail_json)
        .map_err(|err| crate::Error::new(format!("invalid rejection detail JSON: {err}")))?;
    if detail.is_null() {
        Ok(None)
    } else {
        Ok(Some(detail))
    }
}

fn parse_tx_auth_user_for_sqlite(
    metadata_json: &str,
    column: usize,
) -> rusqlite::Result<Option<String>> {
    let metadata = serde_json::from_str::<JsonValue>(metadata_json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(err),
        )
    })?;
    Ok(metadata
        .get("auth_user")
        .and_then(JsonValue::as_str)
        .map(str::to_owned))
}

fn parse_rejection_detail_for_sqlite(
    detail_json: &str,
    column: usize,
) -> rusqlite::Result<Option<JsonValue>> {
    let detail = serde_json::from_str::<JsonValue>(detail_json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(err),
        )
    })?;
    if detail.is_null() {
        Ok(None)
    } else {
        Ok(Some(detail))
    }
}

fn export_reads_for_history(
    conn: &Connection,
    history: &[HistoryRecord],
) -> Result<Vec<ReadRecord>> {
    let mut tx_ids = history
        .iter()
        .map(|record| record.tx_id.clone())
        .collect::<Vec<_>>();
    tx_ids.sort();
    tx_ids.dedup();
    if tx_ids.is_empty() {
        return Ok(Vec::new());
    }
    if tx_ids.len() > crate::SQL_VARIABLE_CHUNK_SIZE {
        return export_reads_for_history_with_temp_scope(conn, history);
    }
    let candidate_read_count = count_read_rows_for_tx_ids(conn, &tx_ids)?;
    if candidate_read_count <= (history.len() * 4).max(256) {
        return export_reads_for_history_simple(conn, history, &tx_ids);
    }
    export_reads_for_history_with_temp_scope(conn, history)
}

fn export_reads_for_history_simple(
    conn: &Connection,
    history: &[HistoryRecord],
    tx_ids: &[String],
) -> Result<Vec<ReadRecord>> {
    let history_keys = history
        .iter()
        .map(|record| {
            (
                record.tx_id.as_str(),
                record.table.as_str(),
                record.row_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    let mut stmt = conn.prepare(&format!(
        "SELECT tx.tx_id, tables.table_name, ids.row_id, reads.reason, observed.tx_id
         FROM jazz_tx_read reads
         JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
         JOIN jazz_table tables ON tables.table_num = reads.table_num
         LEFT JOIN jazz_tx observed ON observed.tx_num = reads.observed_tx_num
         JOIN jazz_row_id ids ON ids.row_num = reads.row_num
         WHERE tx.tx_id IN ({placeholders})
         ORDER BY tx.tx_num, tables.table_name, ids.row_id, reads.reason",
        placeholders = (0..tx_ids.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", "),
    ))?;
    let records = stmt.query_map(params_from_iter(tx_ids.iter()), |row| {
        Ok(ReadRecord {
            tx_id: row.get(0)?,
            table: row.get(1)?,
            row_id: row.get(2)?,
            reason: row.get(3)?,
            observed_tx_id: row.get(4)?,
        })
    })?;
    let records = records
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|record| {
            record.reason != read_set::REASON_ABSENT
                || history_keys.contains(&(
                    record.tx_id.as_str(),
                    record.table.as_str(),
                    record.row_id.as_str(),
                ))
        })
        .collect();
    Ok(records)
}

fn count_read_rows_for_tx_ids(conn: &Connection, tx_ids: &[String]) -> Result<usize> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM jazz_tx_read reads
             JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
             WHERE tx.tx_id IN ({placeholders})",
            placeholders = (0..tx_ids.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", "),
        ),
        params_from_iter(tx_ids.iter()),
        |row| row.get(0),
    )?;
    Ok(count as usize)
}

fn export_reads_for_history_with_temp_scope(
    conn: &Connection,
    history: &[HistoryRecord],
) -> Result<Vec<ReadRecord>> {
    if history.is_empty() {
        return Ok(Vec::new());
    }
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS jazz_export_tx_scope (
           tx_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         CREATE TEMP TABLE IF NOT EXISTS jazz_export_history_scope (
           tx_id TEXT NOT NULL,
           table_name TEXT NOT NULL,
           row_id TEXT NOT NULL,
           PRIMARY KEY (tx_id, table_name, row_id)
         ) WITHOUT ROWID;
         DELETE FROM jazz_export_tx_scope;
         DELETE FROM jazz_export_history_scope;",
    )?;
    {
        let mut tx_stmt =
            conn.prepare("INSERT OR IGNORE INTO jazz_export_tx_scope (tx_id) VALUES (?)")?;
        let mut scope_stmt = conn.prepare(
            "INSERT OR IGNORE INTO jazz_export_history_scope (tx_id, table_name, row_id)
             VALUES (?, ?, ?)",
        )?;
        for record in history {
            tx_stmt.execute(params![record.tx_id])?;
            scope_stmt.execute(params![record.tx_id, record.table, record.row_id])?;
        }
    }
    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, tables.table_name, ids.row_id, reads.reason, observed.tx_id
         FROM jazz_export_tx_scope tx_scope
         JOIN jazz_tx tx ON tx.tx_id = tx_scope.tx_id
         JOIN jazz_tx_read reads ON reads.tx_num = tx.tx_num
         JOIN jazz_table tables ON tables.table_num = reads.table_num
         LEFT JOIN jazz_tx observed ON observed.tx_num = reads.observed_tx_num
         JOIN jazz_row_id ids ON ids.row_num = reads.row_num
         LEFT JOIN jazz_export_history_scope history_scope
           ON history_scope.tx_id = tx.tx_id
          AND history_scope.table_name = tables.table_name
          AND history_scope.row_id = ids.row_id
         WHERE reads.reason != ?
            OR history_scope.tx_id IS NOT NULL
         ORDER BY tx.tx_num, tables.table_name, ids.row_id, reads.reason",
    )?;
    let records = stmt.query_map(params![read_set::REASON_ABSENT], |row| {
        Ok(ReadRecord {
            tx_id: row.get(0)?,
            table: row.get(1)?,
            row_id: row.get(2)?,
            reason: row.get(3)?,
            observed_tx_id: row.get(4)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn export_branch_records_for_history(
    conn: &Connection,
    history: &[HistoryRecord],
) -> Result<Vec<BranchRecord>> {
    let mut branch_ids = history
        .iter()
        .map(|record| record.branch_id.clone())
        .collect::<Vec<_>>();
    branch_ids.sort();
    branch_ids.dedup();

    let mut records = Vec::new();
    for branch_id in branch_ids {
        let (branch_num, base_global_epoch, source_version) = conn.query_row(
            "SELECT branch_num, base_global_epoch, source_version FROM jazz_branch WHERE branch_id = ?",
            params![branch_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, i64>(2)?)),
        )?;
        let mut stmt = conn.prepare(
            "SELECT source.branch_id
             FROM jazz_branch_source branch_source
             JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
             WHERE branch_source.branch_num = ?
             ORDER BY source.branch_id",
        )?;
        let source_branch_ids = stmt
            .query_map(params![branch_num], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        records.push(BranchRecord {
            branch_id,
            base_global_epoch,
            source_branch_ids,
            source_version,
        });
    }
    Ok(records)
}

fn include_branch_record(
    conn: &Connection,
    records: &mut Vec<BranchRecord>,
    branch_num: i64,
) -> Result<()> {
    let (branch_id, base_global_epoch, source_version) = conn.query_row(
        "SELECT branch_id, base_global_epoch, source_version FROM jazz_branch WHERE branch_num = ?",
        params![branch_num],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    )?;
    let mut stmt = conn.prepare(
        "SELECT source.branch_num, source.branch_id
         FROM jazz_branch_source branch_source
         JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
         WHERE branch_source.branch_num = ?
         ORDER BY source.branch_id",
    )?;
    let source_branches = stmt
        .query_map(params![branch_num], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let source_branch_ids = source_branches
        .iter()
        .map(|(_, branch_id)| branch_id.clone())
        .collect();
    if !records.iter().any(|record| record.branch_id == branch_id) {
        records.push(BranchRecord {
            branch_id,
            base_global_epoch,
            source_branch_ids,
            source_version,
        });
    }
    for (source_branch_num, _) in source_branches {
        include_branch_record(conn, records, source_branch_num)?;
    }
    Ok(())
}

fn export_table_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    user: &str,
    bypass_policy: bool,
    branch_num: i64,
) -> Result<Vec<HistoryRecord>> {
    let branch_nums = branch::scope_nums(conn, branch_num)?;
    let visibility = ReadVisibility {
        conn,
        schema,
        branch_num,
        user,
        bypass_policy,
    };
    let mut records = export_visible_table_history(&visibility, table_name, &branch_nums, None)?;
    records.extend(export_deleted_table_history(
        conn,
        schema,
        table_name,
        &branch_nums,
    )?);
    records.extend(export_policy_dependency_history(
        &visibility,
        PolicyDependencyExport {
            table_name,
            policy: &schema.table_def(table_name)?.read_policy,
            branch_nums: &branch_nums,
            child_row_nums: None,
        },
    )?);
    records.extend(export_policy_dependency_history(
        &visibility,
        PolicyDependencyExport {
            table_name,
            policy: &schema.table_def(table_name)?.write_policy,
            branch_nums: &branch_nums,
            child_row_nums: None,
        },
    )?);
    if branch_num != 1 {
        if let Some(base_epoch) = branch::base_global_epoch(conn, branch_num)? {
            records.extend(export_main_base_snapshot_history(
                &visibility,
                table_name,
                base_epoch,
            )?);
        }
    }
    Ok(records)
}

fn export_main_base_snapshot_history(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let row_nums =
        visibility.base_snapshot_row_nums_visible_in_branch(table_name, base_epoch, None)?;
    let mut records = export_history_versions_for_rows(
        conn,
        schema,
        table_name,
        Some(&row_nums),
        Some(base_epoch),
    )?;
    records.extend(export_snapshot_policy_dependency_history(
        visibility,
        table_name,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

fn export_snapshot_policy_dependency_history(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
    child_row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(table_name)?;
    let PolicyDef::RefReadable { field } = &table.read_policy else {
        return Ok(Vec::new());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = visibility.snapshot_policy_sql(table, "h", base_epoch)?;
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let sql = format!(
        "SELECT DISTINCT h.{ref_column}
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {row_filter}
           AND h.j_branch_num = 1
           AND h.op != 3
           AND tx.outcome != {}
           AND tx.global_epoch IS NOT NULL
           AND tx.global_epoch <= {base_epoch}
           AND {policy_sql}
           AND NOT EXISTS (
             SELECT 1
             FROM {history_table} newer
             JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
             WHERE newer.row_num = h.row_num
               AND newer.j_branch_num = 1
               AND newer_tx.outcome != {}
               AND newer_tx.global_epoch IS NOT NULL
               AND newer_tx.global_epoch <= {base_epoch}
               AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
           )",
        crate::schema::history_table(table_name),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        row_filter = history_row_filter_sql("h", child_row_nums),
        history_table = crate::schema::history_table(table_name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut records = export_history_versions_for_rows(
        conn,
        schema,
        parent_table,
        Some(&row_nums),
        Some(base_epoch),
    )?;
    records.extend(export_snapshot_policy_dependency_history(
        visibility,
        parent_table,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

fn export_snapshot_policy_dependency_history_for_query_scope(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
    child_scope: &query::LoweredQueryRowScope,
) -> Result<Vec<HistoryRecord>> {
    export_snapshot_policy_dependency_history_for_query_scope_at_depth(
        visibility,
        table_name,
        base_epoch,
        child_scope,
        0,
    )
}

fn export_snapshot_policy_dependency_history_for_query_scope_at_depth(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
    child_scope: &query::LoweredQueryRowScope,
    depth: usize,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(table_name)?;
    let PolicyDef::RefReadable { field } = &table.read_policy else {
        return Ok(Vec::new());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = visibility.snapshot_policy_sql(table, "h", base_epoch)?;
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let child_scope_name = format!("snapshot_policy_child_scope_{depth}");
    let parent_scope_name = format!("snapshot_policy_parent_scope_{depth}");
    let mut ctes = child_scope.ctes.clone();
    ctes.push(format!(
        "{child_scope_name}(row_num) AS ({})",
        child_scope.select_sql
    ));
    ctes.push(format!(
        "{parent_scope_name}(row_num) AS (
           SELECT DISTINCT h.{ref_column}
           FROM {} h
           JOIN {child_scope_name} child_scope ON child_scope.row_num = h.row_num
           JOIN jazz_tx tx ON tx.tx_num = h.tx_num
           WHERE h.j_branch_num = 1
             AND h.op != 3
             AND tx.outcome != {}
             AND tx.global_epoch IS NOT NULL
             AND tx.global_epoch <= {base_epoch}
             AND {policy_sql}
             AND NOT EXISTS (
               SELECT 1
               FROM {history_table} newer
               JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
               WHERE newer.row_num = h.row_num
                 AND newer.j_branch_num = 1
                 AND newer_tx.outcome != {}
                 AND newer_tx.global_epoch IS NOT NULL
                 AND newer_tx.global_epoch <= {base_epoch}
                 AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
             )
         )",
        crate::schema::history_table(table_name),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        history_table = crate::schema::history_table(table_name),
    ));
    let parent_scope = query::LoweredQueryRowScope {
        ctes,
        select_sql: format!("SELECT row_num FROM {parent_scope_name} WHERE row_num IS NOT NULL"),
        params: child_scope.params.clone(),
    };
    let mut records = export_history_versions_for_query_scope(
        conn,
        schema,
        parent_table,
        &parent_scope,
        Some(base_epoch),
    )?;
    records.extend(
        export_snapshot_policy_dependency_history_for_query_scope_at_depth(
            visibility,
            parent_table,
            base_epoch,
            &parent_scope,
            depth + 1,
        )?,
    );
    Ok(records)
}

struct PolicyDependencyExport<'a> {
    table_name: &'a str,
    policy: &'a PolicyDef,
    branch_nums: &'a [i64],
    child_row_nums: Option<&'a [i64]>,
}

struct PolicyDependencyQueryScopeExport<'a> {
    table_name: &'a str,
    policy: &'a PolicyDef,
    branch_nums: &'a [i64],
    child_scope: &'a query::LoweredQueryRowScope,
}

fn export_policy_dependency_history(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyExport<'_>,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(args.table_name)?;
    let branch_policy_records =
        export_branch_policy_dependency_history(visibility, table, args.branch_nums)?;
    let PolicyDef::RefReadable { field } = args.policy else {
        return Ok(branch_policy_records);
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = if args.child_row_nums.is_some() {
        "1 = 1".to_owned()
    } else {
        visibility.current_policy_sql(table, "current")?
    };
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let row_nums = if let Some(child_row_nums) = args.child_row_nums {
        scoped_policy_parent_row_nums(
            conn,
            args.table_name,
            &ref_column,
            args.branch_nums,
            child_row_nums,
        )?
    } else {
        let sql = format!(
            "SELECT DISTINCT current.{ref_column}
             FROM {} current
             JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
             WHERE current.is_deleted = 0
               AND {}
               AND current_tx.outcome != {}
               AND {policy_sql}",
            crate::schema::current_table(args.table_name),
            branch_filter_sql("current", args.branch_nums),
            tx::OUTCOME_REJECTED,
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, i64>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows
    };
    let mut records = if args.child_row_nums.is_some() {
        export_history_versions_for_rows(conn, schema, parent_table, Some(&row_nums), None)?
    } else {
        export_visible_table_history(visibility, parent_table, args.branch_nums, Some(&row_nums))?
    };
    records.extend(branch_policy_records);
    records.extend(export_policy_dependency_history(
        visibility,
        PolicyDependencyExport {
            table_name: parent_table,
            policy: &schema.table_def(parent_table)?.read_policy,
            branch_nums: args.branch_nums,
            child_row_nums: Some(&row_nums),
        },
    )?);
    Ok(records)
}

fn export_policy_dependency_history_for_query_scope(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyQueryScopeExport<'_>,
) -> Result<Vec<HistoryRecord>> {
    export_policy_dependency_history_for_query_scope_at_depth(visibility, args, 0)
}

fn export_policy_dependency_history_for_query_scope_at_depth(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyQueryScopeExport<'_>,
    depth: usize,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(args.table_name)?;
    let branch_policy_records =
        export_branch_policy_dependency_history(visibility, table, args.branch_nums)?;
    let PolicyDef::RefReadable { field } = args.policy else {
        return Ok(branch_policy_records);
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let child_scope_name = format!("policy_child_scope_{depth}");
    let parent_scope_name = format!("policy_parent_scope_{depth}");
    let mut ctes = args.child_scope.ctes.clone();
    ctes.push(format!(
        "{child_scope_name}(row_num) AS ({})",
        args.child_scope.select_sql
    ));
    ctes.push(format!(
        "{parent_scope_name}(row_num) AS (
           SELECT DISTINCT current.{ref_column}
           FROM {} current
           JOIN {child_scope_name} child_scope ON child_scope.row_num = current.row_num
           JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
           WHERE current.is_deleted = 0
             AND {}
             AND current_tx.outcome != {}
         )",
        crate::schema::current_table(args.table_name),
        branch_filter_sql("current", args.branch_nums),
        tx::OUTCOME_REJECTED,
    ));
    let parent_scope = query::LoweredQueryRowScope {
        ctes,
        select_sql: format!("SELECT row_num FROM {parent_scope_name} WHERE row_num IS NOT NULL"),
        params: args.child_scope.params.clone(),
    };
    let mut records =
        export_history_versions_for_query_scope(conn, schema, parent_table, &parent_scope, None)?;
    records.extend(branch_policy_records);
    records.extend(export_policy_dependency_history_for_query_scope_at_depth(
        visibility,
        PolicyDependencyQueryScopeExport {
            table_name: parent_table,
            policy: &schema.table_def(parent_table)?.read_policy,
            branch_nums: args.branch_nums,
            child_scope: &parent_scope,
        },
        depth + 1,
    )?);
    Ok(records)
}

fn export_branch_policy_dependency_history(
    visibility: &ReadVisibility<'_>,
    table: &crate::schema::TableDef,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    if table.branch_policies.is_empty() || branch_nums.is_empty() {
        return Ok(Vec::new());
    }
    let conn = visibility.conn;
    let mut records = Vec::new();
    let main_visibility = ReadVisibility {
        conn: visibility.conn,
        schema: visibility.schema,
        branch_num: 1,
        user: visibility.user,
        bypass_policy: visibility.bypass_policy,
    };
    for branch_table_name in table.branch_policies.keys() {
        let row_nums = branch_backing_row_nums(conn, branch_nums)?;
        if row_nums.is_empty() {
            continue;
        }
        records.extend(export_visible_table_history(
            &main_visibility,
            branch_table_name,
            &[1],
            Some(&row_nums),
        )?);
    }
    dedupe_history_records(&mut records);
    Ok(records)
}

fn branch_backing_row_nums(conn: &Connection, branch_nums: &[i64]) -> Result<Vec<i64>> {
    let branch_nums = sorted_unique_row_nums(branch_nums);
    let mut row_nums = BTreeSet::new();
    for chunk in branch_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let placeholders = sql_placeholders(chunk.len());
        let mut stmt = conn.prepare(&format!(
            "SELECT ids.row_num
             FROM jazz_branch branch
             JOIN jazz_row_id ids ON ids.row_id = branch.branch_id
             WHERE branch.branch_num IN ({placeholders})
             ORDER BY ids.row_num"
        ))?;
        let rows = stmt.query_map(params_from_iter(chunk.iter()), |row| row.get::<_, i64>(0))?;
        for row_num in rows {
            row_nums.insert(row_num?);
        }
    }
    Ok(row_nums.into_iter().collect())
}

fn scoped_policy_parent_row_nums(
    conn: &Connection,
    table_name: &str,
    ref_column: &str,
    branch_nums: &[i64],
    child_row_nums: &[i64],
) -> Result<Vec<i64>> {
    if child_row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let mut parent_row_nums = BTreeSet::new();
    let child_row_nums = sorted_unique_row_nums(child_row_nums);
    for child_chunk in child_row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let child_placeholders = sql_placeholders(child_chunk.len());
        let mut stmt = conn.prepare(&format!(
            "SELECT current.{ref_column}
             FROM {} current
             JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
             WHERE current.row_num IN ({child_placeholders})
               AND current.is_deleted = 0
               AND {}
               AND current_tx.outcome != ?",
            crate::schema::current_table(table_name),
            branch_filter_sql("current", branch_nums),
        ))?;
        let mut params = child_chunk
            .iter()
            .copied()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| row.get::<_, i64>(0))?;
        for row in rows {
            parent_row_nums.insert(row?);
        }
    }
    Ok(parent_row_nums.into_iter().collect())
}

fn export_deleted_table_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    let sql = format!(
        "SELECT h.row_num
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE h.op = 3
           AND {}
           AND tx.outcome != {}
           AND NOT EXISTS (
             SELECT 1
             FROM {history_table} newer
             JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
             WHERE newer.row_num = h.row_num
               AND newer.j_branch_num = h.j_branch_num
               AND newer_tx.outcome != {}
               AND newer.tx_num > h.tx_num
           )",
        crate::schema::history_table(table_name),
        branch_filter_sql("h", branch_nums),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        history_table = crate::schema::history_table(table_name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

fn export_deleted_recursive_descendant_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    parent_field: &str,
    branch_nums: &[i64],
    parent_row_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    if parent_row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let parent_row_nums = sorted_unique_row_nums(parent_row_nums);
    let table = schema.table_def(table_name)?;
    let field = table
        .fields
        .iter()
        .find(|field| field.name == parent_field)
        .ok_or_else(|| crate::Error::new(format!("unknown ref field {parent_field}")))?;
    let parent_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let mut row_nums = BTreeSet::new();
    for parent_chunk in parent_row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let sql = format!(
            "WITH RECURSIVE deleted_tree(row_num) AS (
               SELECT h.row_num
               FROM {history_table} h
               JOIN jazz_tx tx ON tx.tx_num = h.tx_num
               WHERE h.op = 3
                 AND {branch_filter}
                 AND h.{parent_column} IN ({parent_placeholders})
                 AND tx.outcome != {rejected}
                 AND NOT EXISTS (
                   SELECT 1
                   FROM {history_table} newer
                   JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                   WHERE newer.row_num = h.row_num
                     AND newer.j_branch_num = h.j_branch_num
                     AND newer_tx.outcome != {rejected}
                     AND newer.tx_num > h.tx_num
                 )
               UNION
               SELECT child.row_num
               FROM {history_table} child
               JOIN jazz_tx child_tx ON child_tx.tx_num = child.tx_num
               JOIN deleted_tree parent ON child.{parent_column} = parent.row_num
               WHERE child.op = 3
                 AND {child_branch_filter}
                 AND child_tx.outcome != {rejected}
                 AND NOT EXISTS (
                   SELECT 1
                   FROM {history_table} newer
                   JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                   WHERE newer.row_num = child.row_num
                     AND newer.j_branch_num = child.j_branch_num
                     AND newer_tx.outcome != {rejected}
                     AND newer.tx_num > child.tx_num
                 )
             )
             SELECT row_num FROM deleted_tree",
            history_table = crate::schema::history_table(table_name),
            branch_filter = branch_filter_sql("h", branch_nums),
            child_branch_filter = branch_filter_sql("child", branch_nums),
            rejected = tx::OUTCOME_REJECTED,
            parent_placeholders = sql_placeholders(parent_chunk.len()),
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(parent_chunk.iter()), |row| {
            row.get::<_, i64>(0)
        })?;
        for row in rows {
            row_nums.insert(row?);
        }
    }
    let row_nums = row_nums.into_iter().collect::<Vec<_>>();
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

fn export_recursive_scope_repair_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    parent_field: &str,
    branch_nums: &[i64],
    parent_row_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    if parent_row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let parent_row_nums = sorted_unique_row_nums(parent_row_nums);
    let table = schema.table_def(table_name)?;
    let field = table
        .fields
        .iter()
        .find(|field| field.name == parent_field)
        .ok_or_else(|| crate::Error::new(format!("unknown ref field {parent_field}")))?;
    let parent_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let mut row_nums = BTreeSet::new();
    for parent_chunk in parent_row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let sql = format!(
            "WITH RECURSIVE historical_tree(row_num) AS (
               SELECT h.row_num
               FROM {history_table} h
               JOIN jazz_tx tx ON tx.tx_num = h.tx_num
               WHERE {branch_filter}
                 AND h.{parent_column} IN ({parent_placeholders})
                 AND tx.outcome != {rejected}
               UNION
               SELECT child.row_num
               FROM {history_table} child
               JOIN jazz_tx child_tx ON child_tx.tx_num = child.tx_num
               JOIN historical_tree parent ON child.{parent_column} = parent.row_num
               WHERE {child_branch_filter}
                 AND child_tx.outcome != {rejected}
             )
             SELECT DISTINCT row_num FROM historical_tree",
            history_table = crate::schema::history_table(table_name),
            branch_filter = branch_filter_sql("h", branch_nums),
            child_branch_filter = branch_filter_sql("child", branch_nums),
            rejected = tx::OUTCOME_REJECTED,
            parent_placeholders = sql_placeholders(parent_chunk.len()),
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(parent_chunk.iter()), |row| {
            row.get::<_, i64>(0)
        })?;
        for row in rows {
            row_nums.insert(row?);
        }
    }
    let row_nums = row_nums.into_iter().collect::<Vec<_>>();
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

fn query_scope_repair_row_nums(
    conn: &Connection,
    table: &crate::schema::TableDef,
    field_name: &str,
    op: &str,
    value: &JsonValue,
) -> Result<Vec<i64>> {
    // Return the physical rows whose history can affect a query-scope refresh.
    // These are not necessarily current result rows.
    //
    // Predicate repair:
    //
    //   history rows ever matching predicate
    //        |
    //        v
    //   exported repair history
    //        |
    //        v
    //   receiver deletes stale current rows no longer justified by history
    //
    // `id` is special because the row id lives in `jazz_row_id`, not the user
    // table history. `$createdBy` is special because it is a system column on
    // history/current tables. User fields lower through `query_predicate`.
    if op == "eq_top_created_at_desc" || op == "eq_top_field_desc" {
        let observed_ids = observed_ids_from_query_value(value)?;
        if observed_ids.is_empty() {
            let value = value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top query expects eq value"))?;
            return query_scope_repair_row_nums(conn, table, field_name, "eq", value);
        }
        return observed_ids
            .into_iter()
            .map(|row_id| row_num(conn, &row_id))
            .collect();
    }
    if field_name == "id" {
        if op == "ne" {
            let excluded_id = value
                .as_str()
                .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
            let mut stmt = conn.prepare(&format!(
                "SELECT DISTINCT h.row_num
                 FROM {} h
                 JOIN jazz_row_id ids ON ids.row_num = h.row_num
                 WHERE ids.row_id != ?
                 ORDER BY h.row_num",
                crate::schema::history_table(&table.name)
            ))?;
            let rows = stmt.query_map(params![excluded_id], |row| row.get(0))?;
            return rows
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into);
        }
        return id_predicate_values(op, value)?
            .into_iter()
            .map(|row_id| ensure_row_id(conn, &table.name, &row_id))
            .collect();
    }
    if field_name == "$createdBy" {
        let Some(created_by) = value.as_str() else {
            return Err(crate::Error::new(
                "$createdBy predicate expects a string value",
            ));
        };
        let created_by_num = match users::user_num(conn, created_by) {
            Ok(user_num) => user_num,
            Err(_) if op == "eq" => return Ok(Vec::new()),
            Err(_) => -1,
        };
        let created_by_sql = match op {
            "eq" => "h.j_created_by = ?",
            "ne" => "h.j_created_by != ?",
            op => {
                return Err(crate::Error::new(format!(
                    "unsupported $createdBy predicate op {op}"
                )));
            }
        };
        let sql = format!(
            "SELECT DISTINCT h.row_num
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE {created_by_sql}
               AND tx.outcome != ?",
            crate::schema::history_table(&table.name),
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![created_by_num, tx::OUTCOME_REJECTED], |row| {
            row.get::<_, i64>(0)
        })?;
        return rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into);
    }
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
        .ok_or_else(|| crate::Error::new(format!("unknown query field {field_name}")))?;
    if op == "in" {
        let mut row_nums = Vec::new();
        for value in value
            .as_array()
            .ok_or_else(|| crate::Error::new("in predicate expects an array value"))?
        {
            row_nums.extend(query_scope_repair_row_nums(
                conn, table, field_name, "eq", value,
            )?);
        }
        row_nums.sort();
        row_nums.dedup();
        return Ok(row_nums);
    }
    let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let predicate_sql = query_predicate::sql(field, &format!("h.{predicate_column}"), op)?;
    let predicate_value = query_predicate::value(field, op, value, conn)?;
    let sql = format!(
        "SELECT DISTINCT h.row_num
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {predicate_sql}
           AND tx.outcome != ?",
        crate::schema::history_table(&table.name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![predicate_value, tx::OUTCOME_REJECTED], |row| {
        row.get::<_, i64>(0)
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_scope_rejected_tx_ids(
    conn: &Connection,
    table: &crate::schema::TableDef,
    field_name: &str,
    op: &str,
    value: &JsonValue,
) -> Result<Vec<String>> {
    if op == "eq_top_created_at_desc" || op == "eq_top_field_desc" {
        let observed_ids = observed_ids_from_query_value(value)?;
        if observed_ids.is_empty() {
            let value = value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top query expects eq value"))?;
            return query_scope_rejected_tx_ids(conn, table, field_name, "eq", value);
        }
        let row_nums = observed_ids
            .into_iter()
            .map(|row_id| row_num(conn, &row_id))
            .collect::<Result<Vec<_>>>()?;
        return rejected_tx_ids_for_row_nums(conn, &table.name, &row_nums);
    }
    if op == "in" {
        let mut tx_ids = Vec::new();
        for value in value
            .as_array()
            .ok_or_else(|| crate::Error::new("in predicate expects an array value"))?
        {
            tx_ids.extend(query_scope_rejected_tx_ids(
                conn, table, field_name, "eq", value,
            )?);
        }
        tx_ids.sort();
        tx_ids.dedup();
        return Ok(tx_ids);
    }
    if field_name == "id" {
        if op == "ne" {
            let excluded_id = value
                .as_str()
                .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
            let sql = format!(
                "SELECT DISTINCT tx.tx_id
                 FROM {} h
                 JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                 JOIN jazz_row_id ids ON ids.row_num = h.row_num
                 WHERE ids.row_id != ?
                   AND tx.outcome = ?
                 ORDER BY tx.tx_num",
                crate::schema::history_table(&table.name),
            );
            let mut stmt = conn.prepare(&sql)?;
            let tx_ids = stmt.query_map(params![excluded_id, tx::OUTCOME_REJECTED], |row| {
                row.get::<_, String>(0)
            })?;
            return tx_ids
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into);
        }
        let row_nums = id_predicate_values(op, value)?
            .into_iter()
            .map(|row_id| ensure_row_id(conn, &table.name, &row_id))
            .collect::<Result<Vec<_>>>()?;
        return rejected_tx_ids_for_row_nums(conn, &table.name, &row_nums);
    }
    if field_name == "$createdBy" {
        let created_by = value
            .as_str()
            .ok_or_else(|| crate::Error::new("$createdBy predicate expects a string value"))?;
        let created_by_num = match users::user_num(conn, created_by) {
            Ok(user_num) => user_num,
            Err(_) if op == "eq" => return Ok(Vec::new()),
            Err(_) => -1,
        };
        let created_by_sql = match op {
            "eq" => "h.j_created_by = ?",
            "ne" => "h.j_created_by != ?",
            op => {
                return Err(crate::Error::new(format!(
                    "unsupported $createdBy predicate op {op}"
                )));
            }
        };
        let sql = format!(
            "SELECT DISTINCT tx.tx_id
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE {created_by_sql}
               AND tx.outcome = ?
             ORDER BY tx.tx_num",
            crate::schema::history_table(&table.name),
        );
        let mut stmt = conn.prepare(&sql)?;
        let tx_ids = stmt.query_map(params![created_by_num, tx::OUTCOME_REJECTED], |row| {
            row.get::<_, String>(0)
        })?;
        return tx_ids
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into);
    }
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
        .ok_or_else(|| crate::Error::new(format!("unknown query field {field_name}")))?;
    let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let predicate_sql = query_predicate::sql(field, &format!("h.{predicate_column}"), op)?;
    let predicate_value = query_predicate::value(field, op, value, conn)?;
    let sql = format!(
        "SELECT DISTINCT tx.tx_id
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {predicate_sql}
           AND tx.outcome = ?
         ORDER BY tx.tx_num",
        crate::schema::history_table(&table.name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let tx_ids = stmt.query_map(params![predicate_value, tx::OUTCOME_REJECTED], |row| {
        row.get::<_, String>(0)
    })?;
    tx_ids
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn rejected_tx_ids_for_row_nums(
    conn: &Connection,
    table_name: &str,
    row_nums: &[i64],
) -> Result<Vec<String>> {
    if row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT DISTINCT tx.tx_id
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {}
           AND tx.outcome = ?
         ORDER BY tx.tx_num",
        crate::schema::history_table(table_name),
        history_row_filter_sql("h", Some(row_nums)),
    );
    let mut stmt = conn.prepare(&sql)?;
    let tx_ids = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| row.get::<_, String>(0))?;
    tx_ids
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_scope_repair_row_nums_for_read(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    query_read: &QueryReadRecord,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<i64>> {
    // Dispatch from the serialized query-read shape used in bundles to the row
    // collection shape used by export. Built queries are stored opaquely, so
    // they need a small adapter before repair candidates can be collected.
    if query_read.op == "query" {
        let query = built_query_from_read(query_read)?;
        return query_scope_repair_row_nums_for_built_query(
            conn,
            schema,
            table,
            &query,
            branch_num,
            user,
            bypass_policy,
        );
    }
    query_scope_repair_row_nums(
        conn,
        table,
        &query_read.field,
        &query_read.op,
        &query_read.value,
    )
}

fn query_scope_repair_row_nums_for_built_query(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    built_query: &BuiltQuery,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<i64>> {
    // Built-query repair row collection mirrors `apply_built_query_scope_repair`:
    //
    //   built query
    //       |
    //       +-- one predicate ------------------------+
    //       |                                         v
    //       +-- eq + createdAt desc + limit --> predicate repair rows
    //       |
    //       +-- other SQL-lowered shape ------> SQL-lowered history scope
    //
    // Generic built-query repair asks SQLite for rows whose history matched the
    // query conditions. Export then sends those row histories so peers learn
    // about rows that left a multi-filter or custom-ordered query.
    if built_query.table != table.name {
        return Err(crate::Error::new(
            "query read table does not match descriptor",
        ));
    }
    let context = query::QueryContext {
        conn,
        schema,
        branch_num,
        user,
        bypass_policy,
        read_tier: ReadTier::Local,
    };
    context.repair_row_nums_for_built_query(built_query)
}

fn query_scope_rejected_tx_ids_for_read(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    query_read: &QueryReadRecord,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<String>> {
    if query_read.op == "query" {
        let query = built_query_from_read(query_read)?;
        return query_scope_rejected_tx_ids_for_built_query(
            conn,
            schema,
            table,
            &query,
            branch_num,
            user,
            bypass_policy,
        );
    }
    query_scope_rejected_tx_ids(
        conn,
        table,
        &query_read.field,
        &query_read.op,
        &query_read.value,
    )
}

fn query_scope_rejected_tx_ids_for_built_query(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    built_query: &BuiltQuery,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<String>> {
    if built_query.table != table.name {
        return Err(crate::Error::new(
            "query read table does not match descriptor",
        ));
    }
    let context = query::QueryContext {
        conn,
        schema,
        branch_num,
        user,
        bypass_policy,
        read_tier: ReadTier::Local,
    };
    let row_nums = context.repair_row_nums_for_built_query(built_query)?;
    rejected_tx_ids_for_row_nums(conn, &built_query.table, &row_nums)
}

enum BuiltQueryRepairScope<'a> {
    Predicate(&'a QueryCondition),
    Generic,
}

fn built_query_repair_scope(query: &BuiltQuery) -> Result<BuiltQueryRepairScope<'_>> {
    if query.conditions.len() == 1 && query.offset.unwrap_or(0) == 0 {
        let condition = &query.conditions[0];
        match (query.order_by.as_slice(), query.limit) {
            ([], None) if legacy_predicate_repair_supports(condition) => {
                return Ok(BuiltQueryRepairScope::Predicate(condition));
            }
            _ => {}
        }
    }
    Ok(BuiltQueryRepairScope::Generic)
}

fn legacy_predicate_repair_supports(condition: &QueryCondition) -> bool {
    match condition.column.as_str() {
        "id" => matches!(
            condition.op,
            QueryConditionOp::Eq | QueryConditionOp::Ne | QueryConditionOp::In
        ),
        "$createdBy" => matches!(condition.op, QueryConditionOp::Eq | QueryConditionOp::Ne),
        "$createdAt" | "$updatedAt" => false,
        _ => !query_condition_value_contains_null(&condition.value),
    }
}

fn query_condition_value_contains_null(value: &JsonValue) -> bool {
    value.is_null()
        || value
            .as_array()
            .is_some_and(|values| values.iter().any(JsonValue::is_null))
}

fn built_query_repair_keep_query(query: &BuiltQuery) -> Result<BuiltQuery> {
    let offset = query.offset.unwrap_or(0);
    if offset == 0 {
        return Ok(query.clone());
    }

    let mut keep_query = query.clone();
    keep_query.offset = None;
    keep_query.limit = query
        .limit
        .map(|limit| {
            offset
                .checked_add(limit)
                .ok_or_else(|| crate::Error::new("query limit plus offset is too large"))
        })
        .transpose()?;
    Ok(keep_query)
}

fn delete_current_rows_outside_keep_set(
    db: &Connection,
    table_name: &str,
    branch_num: i64,
    scope_row_nums: &[i64],
    keep_row_nums: &[i64],
) -> Result<()> {
    if scope_row_nums.is_empty() {
        return Ok(());
    }

    // Generic window repair is a contraction pass:
    //
    //   rows matching query filters, without LIMIT/OFFSET
    //             |
    //             v
    //   +------------------+        +----------------------+
    //   | scope row nums   |  minus | rows to keep locally |
    //   +------------------+        +----------------------+
    //             |
    //             v
    //   DELETE stale current rows from the observed branch
    //
    // For offset queries, "keep" is the exported support window
    // [0, offset + limit). Those prefix rows must stay local so SQLite can
    // still evaluate the original OFFSET query correctly after the refresh.
    let keep_row_nums = keep_row_nums.iter().copied().collect::<BTreeSet<_>>();
    let delete_row_nums = scope_row_nums
        .iter()
        .copied()
        .filter(|row_num| !keep_row_nums.contains(row_num))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for delete_chunk in delete_row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let sql = format!(
            "DELETE FROM {}
             WHERE j_branch_num = ?
               AND is_deleted = 0
               AND row_num IN ({})",
            crate::schema::current_table(table_name),
            sql_placeholders(delete_chunk.len()),
        );
        let mut params = Vec::with_capacity(1 + delete_chunk.len());
        params.push(rusqlite::types::Value::Integer(branch_num));
        params.extend(
            delete_chunk
                .iter()
                .copied()
                .map(rusqlite::types::Value::Integer),
        );
        db.execute(&sql, params_from_iter(params.iter()))?;
    }
    Ok(())
}

fn id_predicate_values(op: &str, value: &JsonValue) -> Result<Vec<String>> {
    match op {
        "eq" => value
            .as_str()
            .map(|row_id| vec![row_id.to_owned()])
            .ok_or_else(|| crate::Error::new("id equality expects a string value")),
        "in" => value
            .as_array()
            .ok_or_else(|| crate::Error::new("id in expects an array value"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| crate::Error::new("id in expects string values"))
            })
            .collect(),
        _ => Err(crate::Error::new(format!("unsupported id predicate {op}"))),
    }
}

fn dedupe_history_records(records: &mut Vec<HistoryRecord>) {
    let mut seen = BTreeSet::new();
    records.retain(|record| {
        seen.insert((
            record.table.clone(),
            record.row_id.clone(),
            record.branch_id.clone(),
            record.tx_id.clone(),
            record.op,
        ))
    });
}

fn export_visible_table_history(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    branch_nums: &[i64],
    row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    if let Some(row_nums) = row_nums {
        if row_nums.is_empty() {
            return Ok(Vec::new());
        }
        if row_nums.len() > crate::SQL_VARIABLE_CHUNK_SIZE {
            let row_nums = sorted_unique_row_nums(row_nums);
            let mut records = Vec::new();
            for row_chunk in row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
                records.extend(export_visible_table_history(
                    visibility,
                    table_name,
                    branch_nums,
                    Some(row_chunk),
                )?);
            }
            return Ok(records);
        }
    }
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(table_name)?;
    let policy_sql = visibility.current_policy_sql(table, "current")?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
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
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {row_filter}
           AND EXISTS (
           SELECT 1
           FROM {} current
           JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
           WHERE current.row_num = h.row_num
             AND current.j_branch_num = h.j_branch_num
             AND current.is_deleted = 0
             AND {}
             AND current_tx.outcome != {}
             AND {policy_sql}
         )
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        crate::schema::current_table(table_name),
        branch_filter_sql("current", branch_nums),
        tx::OUTCOME_REJECTED,
        row_filter = row_filter_sql(row_nums),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut records = Vec::new();
    let mut rows = match row_nums {
        Some(row_nums) => stmt.query(params_from_iter(row_nums.iter()))?,
        None => stmt.query([])?,
    };
    let mut public_row_id_cache = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json_cached(conn, field, &row[idx + 4], &mut public_row_id_cache)?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

fn export_history_versions_for_rows(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_rows_with_branch_filter(
        conn,
        schema,
        table_name,
        row_nums,
        max_global_epoch,
        None,
    )
}

fn export_history_versions_for_rows_in_branches(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_rows_with_branch_filter(
        conn,
        schema,
        table_name,
        row_nums,
        max_global_epoch,
        Some(branch_nums),
    )
}

fn export_history_versions_for_query_scope(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_scope: &query::LoweredQueryRowScope,
    max_global_epoch: Option<i64>,
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_query_scope_with_branch_filter(
        conn,
        schema,
        table_name,
        row_scope,
        max_global_epoch,
        None,
    )
}

fn export_history_versions_for_query_scope_in_branches(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_scope: &query::LoweredQueryRowScope,
    max_global_epoch: Option<i64>,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_query_scope_with_branch_filter(
        conn,
        schema,
        table_name,
        row_scope,
        max_global_epoch,
        Some(branch_nums),
    )
}

fn export_history_versions_for_query_scope_with_branch_filter(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_scope: &query::LoweredQueryRowScope,
    max_global_epoch: Option<i64>,
    branch_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
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
        "{}
         SELECT {}
         FROM {} h
         JOIN query_scope scope ON scope.row_num = h.row_num
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {branch_filter}
           AND {epoch_filter}
         ORDER BY h.row_num, h.tx_num",
        row_scope.with_scope_cte("query_scope"),
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        branch_filter = history_branch_filter_sql("h", branch_nums),
        epoch_filter = history_epoch_filter_sql(max_global_epoch),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut rows = stmt.query(params_from_iter(row_scope.params.iter()))?;
    let mut records = Vec::new();
    let mut public_row_id_cache = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json_cached(conn, field, &row[idx + 4], &mut public_row_id_cache)?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

fn export_history_versions_for_rows_with_branch_filter(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
    branch_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    if let Some(row_nums) = row_nums {
        if row_nums.is_empty() {
            return Ok(Vec::new());
        }
        if row_nums.len() > crate::SQL_VARIABLE_CHUNK_SIZE {
            let row_nums = sorted_unique_row_nums(row_nums);
            let mut records = Vec::new();
            for row_chunk in row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
                records.extend(export_history_versions_for_rows_with_branch_filter(
                    conn,
                    schema,
                    table_name,
                    Some(row_chunk),
                    max_global_epoch,
                    branch_nums,
                )?);
            }
            return Ok(records);
        }
    }
    let table = schema.table_def(table_name)?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
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
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {row_filter}
           AND {branch_filter}
           AND {epoch_filter}
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        row_filter = row_filter_sql(row_nums),
        branch_filter = history_branch_filter_sql("h", branch_nums),
        epoch_filter = history_epoch_filter_sql(max_global_epoch),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut rows = match row_nums {
        Some(row_nums) => stmt.query(params_from_iter(row_nums.iter()))?,
        None => stmt.query([])?,
    };
    let mut records = Vec::new();
    let mut public_row_id_cache = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json_cached(conn, field, &row[idx + 4], &mut public_row_id_cache)?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

fn history_epoch_filter_sql(max_global_epoch: Option<i64>) -> String {
    match max_global_epoch {
        Some(epoch) => format!("tx.global_epoch IS NOT NULL AND tx.global_epoch <= {epoch}"),
        None => "1 = 1".to_owned(),
    }
}

fn row_filter_sql(row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!("h.row_num IN ({})", sql_placeholders(row_nums.len())),
        None => "1 = 1".to_owned(),
    }
}

fn history_row_filter_sql(alias: &str, row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!(
            "{alias}.row_num IN ({})",
            row_nums
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}

fn history_branch_filter_sql(alias: &str, branch_nums: Option<&[i64]>) -> String {
    match branch_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(branch_nums) => format!(
            "{alias}.j_branch_num IN ({})",
            branch_nums
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}

fn branch_filter_sql(alias: &str, branch_nums: &[i64]) -> String {
    if branch_nums.is_empty() {
        return "0 = 1".to_owned();
    }
    format!(
        "{alias}.j_branch_num IN ({})",
        branch_nums
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn sorted_unique_row_nums(row_nums: &[i64]) -> Vec<i64> {
    let mut row_nums = row_nums.to_vec();
    row_nums.sort();
    row_nums.dedup();
    row_nums
}

fn sql_placeholders(count: usize) -> String {
    (0..count).map(|_| "?").collect::<Vec<_>>().join(", ")
}

fn sql_value_to_json(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
) -> Result<JsonValue> {
    let mut public_row_id_cache = BTreeMap::new();
    sql_value_to_json_cached(conn, field, value, &mut public_row_id_cache)
}

fn sql_value_to_json_cached(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
    public_row_id_cache: &mut BTreeMap<i64, String>,
) -> Result<JsonValue> {
    match (&field.kind, value) {
        (_, rusqlite::types::Value::Null) if field.nullable => Ok(JsonValue::Null),
        (FieldKind::Text, rusqlite::types::Value::Text(value)) => {
            Ok(JsonValue::String(value.clone()))
        }
        (FieldKind::Bool, rusqlite::types::Value::Integer(value)) => {
            Ok(JsonValue::Bool(*value != 0))
        }
        (FieldKind::Ref { .. }, rusqlite::types::Value::Integer(row_num)) => Ok(JsonValue::String(
            cached_public_row_id(conn, public_row_id_cache, *row_num)?,
        )),
        _ => Err(crate::Error::new(format!(
            "unexpected SQL value for field {}",
            field.name
        ))),
    }
}

fn cached_public_row_id(
    conn: &Connection,
    cache: &mut BTreeMap<i64, String>,
    row_num: i64,
) -> Result<String> {
    if let Some(row_id) = cache.get(&row_num) {
        return Ok(row_id.clone());
    }
    let row_id = public_row_id(conn, row_num)?;
    cache.insert(row_num, row_id.clone());
    Ok(row_id)
}

fn text_value(value: &rusqlite::types::Value, name: &str) -> Result<String> {
    match value {
        rusqlite::types::Value::Text(value) => Ok(value.clone()),
        _ => Err(crate::Error::new(format!("expected text {name}"))),
    }
}

fn integer_value(value: &rusqlite::types::Value, name: &str) -> Result<i64> {
    match value {
        rusqlite::types::Value::Integer(value) => Ok(*value),
        _ => Err(crate::Error::new(format!("expected integer {name}"))),
    }
}

fn scoped_policy_fingerprint(
    schema: &SchemaDef,
    history: &[HistoryRecord],
    query_reads: &[QueryReadRecord],
) -> String {
    let mut tables = BTreeSet::new();
    for record in history {
        tables.insert(record.table.clone());
    }
    for query_read in query_reads {
        tables.insert(query_read.table.clone());
    }
    schema.policy_fingerprint_for_tables(tables.iter())
}

fn make_bundle(
    schema: &SchemaDef,
    branches: Vec<BranchRecord>,
    txs: Vec<TxRecord>,
    reads: Vec<ReadRecord>,
    query_reads: Vec<QueryReadRecord>,
    history: Vec<HistoryRecord>,
) -> Bundle {
    Bundle {
        protocol_version: BUNDLE_PROTOCOL_VERSION,
        schema_fingerprint: schema.compatibility_fingerprint(),
        policy_fingerprint: scoped_policy_fingerprint(schema, &history, &query_reads),
        branches,
        txs,
        reads,
        query_reads,
        history,
    }
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
