use crate::rows::ensure_row_id;
use crate::schema::{FieldDef, FieldKind, PolicyDef, SchemaDef, TableDef};
use crate::{branch, read_set, schema, tx, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) struct WritePolicyReadSet<'a> {
    pub(crate) conn: &'a Connection,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) table: &'a TableDef,
    pub(crate) policy: &'a PolicyDef,
    pub(crate) values: &'a BTreeMap<String, JsonValue>,
    pub(crate) branch_num: i64,
    pub(crate) tx_num: i64,
}

pub(crate) fn record_for_write(args: WritePolicyReadSet<'_>) -> Result<()> {
    let mut policy = args.policy;
    if args.branch_num != 1 {
        if let Some((branch_table_name, branch_policy)) = args.table.branch_policies.iter().next() {
            if let Some(branch_write_policy) = branch_policy.write_policy.as_ref() {
                let branch_id = branch::id_for_num(args.conn, args.branch_num)?;
                let branch_row_num = ensure_row_id(args.conn, branch_table_name, &branch_id)?;
                read_set::record_tx_read(
                    args.conn,
                    args.tx_num,
                    branch_table_name,
                    branch_row_num,
                    1,
                    1,
                )?;
                let branch_table = args.schema.table_def(branch_table_name)?;
                record_dependencies_for_row(
                    args.conn,
                    args.schema,
                    branch_table,
                    &branch_table.read_policy,
                    branch_row_num,
                    1,
                    args.tx_num,
                )?;
                policy = branch_write_policy;
            }
        }
    }
    record_policy_dependencies(
        args.conn,
        args.schema,
        args.table,
        policy,
        args.values,
        args.branch_num,
        args.tx_num,
    )
}

fn record_policy_dependencies(
    conn: &Connection,
    schema: &SchemaDef,
    table: &TableDef,
    policy: &PolicyDef,
    values: &BTreeMap<String, JsonValue>,
    branch_num: i64,
    tx_num: i64,
) -> Result<()> {
    let PolicyDef::RefReadable { field } = policy else {
        return Ok(());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: ref_table_name,
    } = &field.kind
    else {
        return Ok(());
    };
    let Some(row_id) = values.get(&field.name).and_then(JsonValue::as_str) else {
        return Ok(());
    };
    let row_num = ensure_row_id(conn, ref_table_name, row_id)?;
    read_set::record_tx_read(conn, tx_num, ref_table_name, row_num, branch_num, 1)?;
    let ref_table = schema.table_def(ref_table_name)?;
    record_dependencies_for_row(
        conn,
        schema,
        ref_table,
        &ref_table.read_policy,
        row_num,
        branch_num,
        tx_num,
    )
}

fn record_dependencies_for_row(
    conn: &Connection,
    schema: &SchemaDef,
    table: &TableDef,
    policy: &PolicyDef,
    row_num: i64,
    branch_num: i64,
    tx_num: i64,
) -> Result<()> {
    let PolicyDef::RefReadable { field } = policy else {
        return Ok(());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: ref_table_name,
    } = &field.kind
    else {
        return Ok(());
    };
    let Some(parent_row_num) =
        current_ref_field_row_num(conn, &table.name, field, row_num, branch_num)?
    else {
        return Ok(());
    };
    read_set::record_tx_read(conn, tx_num, ref_table_name, parent_row_num, branch_num, 1)?;
    let parent_table = schema.table_def(ref_table_name)?;
    record_dependencies_for_row(
        conn,
        schema,
        parent_table,
        &parent_table.read_policy,
        parent_row_num,
        branch_num,
        tx_num,
    )
}

fn current_ref_field_row_num(
    conn: &Connection,
    table_name: &str,
    field: &FieldDef,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<i64>> {
    if branch_num != 1 {
        if let Some(base_epoch) = branch::base_global_epoch(conn, branch_num)? {
            if !current_row_exists_on_branch(conn, table_name, row_num, branch_num)? {
                return snapshot_ref_field_row_num(conn, table_name, field, row_num, base_epoch);
            }
        }
    }
    let column = schema::quote_ident(&schema::storage_column(field));
    conn.query_row(
        &format!(
            "SELECT current.{column}
             FROM {} current
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE current.row_num = ?
               AND {}
               AND current.is_deleted = 0
               AND tx.outcome != ?
             ORDER BY CASE WHEN current.j_branch_num = ? THEN 0 ELSE 1 END
             LIMIT 1",
            schema::current_table(table_name),
            current_effective_branch_sql("current", table_name, branch_num)
        ),
        params![row_num, tx::OUTCOME_REJECTED, branch_num],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(Into::into)
}

fn current_row_exists_on_branch(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {}
             WHERE row_num = ?
               AND j_branch_num = ?",
            schema::current_table(table_name)
        ),
        params![row_num, branch_num],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn snapshot_ref_field_row_num(
    conn: &Connection,
    table_name: &str,
    field: &FieldDef,
    row_num: i64,
    base_epoch: i64,
) -> Result<Option<i64>> {
    let column = schema::quote_ident(&schema::storage_column(field));
    conn.query_row(
        &format!(
            "SELECT h.{column}
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.row_num = ?
               AND h.j_branch_num = 1
               AND h.op != 3
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = 1
                   AND newer_tx.outcome != ?
                   AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?
                   AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
               )
             LIMIT 1",
            schema::history_table(table_name),
            history_table = schema::history_table(table_name),
        ),
        params![
            row_num,
            tx::OUTCOME_REJECTED,
            base_epoch,
            tx::OUTCOME_REJECTED,
            base_epoch
        ],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(Into::into)
}

fn current_effective_branch_sql(alias: &str, table_name: &str, branch_num: i64) -> String {
    if branch_num == 1 {
        return format!("{alias}.j_branch_num = 1");
    }
    format!(
        "({alias}.j_branch_num = {branch_num}
          OR (
            {alias}.j_branch_num = 1
            AND NOT EXISTS (
              SELECT 1
              FROM {} branch_shadow
              WHERE branch_shadow.row_num = {alias}.row_num
                AND branch_shadow.j_branch_num = {branch_num}
            )
          ))",
        schema::current_table(table_name)
    )
}
