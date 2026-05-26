use crate::schema::{FieldKind, PolicyDef, SchemaDef, TableDef};
use crate::{tx, Result};
use rusqlite::{params, Connection};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) struct WriteCheck<'a> {
    pub(crate) db: &'a Connection,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) table: &'a TableDef,
    pub(crate) row_num: i64,
    pub(crate) branch_num: i64,
    pub(crate) values: &'a BTreeMap<String, JsonValue>,
    pub(crate) principal: &'a str,
}

pub(crate) fn write_allowed(check: WriteCheck<'_>) -> Result<bool> {
    match &check.table.write_policy {
        PolicyDef::AllowAll => Ok(true),
        PolicyDef::CreatedByPrincipal => {
            let count: i64 = check.db.query_row(
                &format!(
                    "SELECT COUNT(*)
                     FROM {} current
                     WHERE current.row_num = ?
                       AND current.j_branch_num = ?
                       AND current.j_created_by = ?",
                    crate::schema::current_table(&check.table.name)
                ),
                params![check.row_num, check.branch_num, check.principal],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        }
        PolicyDef::RefReadable { field } => {
            let field_def = check
                .table
                .fields
                .iter()
                .find(|candidate| candidate.name == *field)
                .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
            let FieldKind::Ref {
                table: ref_table_name,
            } = &field_def.kind
            else {
                return Err(crate::Error::new(format!(
                    "policy field {} is not a ref",
                    field_def.name
                )));
            };
            let ref_id = check
                .values
                .get(field)
                .or_else(|| check.values.get(&field_def.storage_name))
                .and_then(JsonValue::as_str)
                .ok_or_else(|| crate::Error::new(format!("expected ref id for {field}")))?;
            let ref_row_num = crate::rows::row_num(check.db, ref_id)?;
            let ref_table = check.schema.table_def(ref_table_name)?;
            let policy_sql = lower_policy(
                check.schema,
                ref_table,
                "current",
                &ref_table.read_policy,
                check.principal,
                Some(check.branch_num),
                0,
            )?;
            let count: i64 = check.db.query_row(
                &format!(
                    "SELECT COUNT(*)
                     FROM {} current
                     JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                     WHERE current.row_num = ?
                       AND current.j_branch_num = ?
                       AND current.is_deleted = 0
                       AND tx.outcome != {}
                       AND {policy_sql}",
                    crate::schema::current_table(ref_table_name),
                    tx::OUTCOME_REJECTED,
                ),
                params![ref_row_num, check.branch_num],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        }
    }
}

pub(crate) fn read_policy_sql(
    schema: &SchemaDef,
    table: &TableDef,
    principal: &str,
) -> Result<String> {
    lower_policy(
        schema,
        table,
        "current",
        &table.read_policy,
        principal,
        None,
        0,
    )
}

pub(crate) fn branch_read_policy_sql_for_alias(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    principal: &str,
    branch_num: i64,
) -> Result<String> {
    lower_policy(
        schema,
        table,
        alias,
        &table.read_policy,
        principal,
        Some(branch_num),
        0,
    )
}

pub(crate) fn snapshot_read_policy_sql_for_alias(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    principal: &str,
    base_epoch: i64,
) -> Result<String> {
    lower_snapshot_policy(
        schema,
        table,
        alias,
        &table.read_policy,
        principal,
        base_epoch,
        0,
    )
}

fn lower_policy(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    policy: &PolicyDef,
    principal: &str,
    branch_num: Option<i64>,
    depth: usize,
) -> Result<String> {
    if depth > 16 {
        return Err(crate::Error::new("policy recursion depth exceeded"));
    }
    match policy {
        PolicyDef::AllowAll => Ok("1 = 1".to_owned()),
        PolicyDef::CreatedByPrincipal => Ok(format!(
            "{alias}.j_created_by = '{}'",
            principal.replace('\'', "''")
        )),
        PolicyDef::RefReadable { field } => {
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == *field)
                .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
            let FieldKind::Ref {
                table: ref_table_name,
            } = &field.kind
            else {
                return Err(crate::Error::new(format!(
                    "policy field {} is not a ref",
                    field.name
                )));
            };
            let ref_table = schema.table_def(ref_table_name)?;
            let parent_alias = format!("policy_parent_{depth}");
            let parent_tx_alias = format!("policy_parent_tx_{depth}");
            let parent_policy = lower_policy(
                schema,
                ref_table,
                &parent_alias,
                &ref_table.read_policy,
                principal,
                branch_num,
                depth + 1,
            )?;
            let branch_filter = branch_num
                .map(|branch_num| format!("AND {parent_alias}.j_branch_num = {branch_num}"))
                .unwrap_or_default();
            Ok(format!(
                "EXISTS (
                   SELECT 1
                   FROM {} {parent_alias}
                   JOIN jazz_tx {parent_tx_alias}
                     ON {parent_tx_alias}.tx_num = {parent_alias}.visible_tx_num
                   WHERE {parent_alias}.row_num = {alias}.{}
                     {branch_filter}
                     AND {parent_alias}.is_deleted = 0
                     AND {parent_tx_alias}.outcome != {}
                     AND {parent_policy}
                 )",
                crate::schema::current_table(&ref_table.name),
                crate::schema::quote_ident(&crate::schema::storage_column(field)),
                tx::OUTCOME_REJECTED,
            ))
        }
    }
}

fn lower_snapshot_policy(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    policy: &PolicyDef,
    principal: &str,
    base_epoch: i64,
    depth: usize,
) -> Result<String> {
    if depth > 16 {
        return Err(crate::Error::new(
            "snapshot policy recursion depth exceeded",
        ));
    }
    match policy {
        PolicyDef::AllowAll => Ok("1 = 1".to_owned()),
        PolicyDef::CreatedByPrincipal => Ok(format!(
            "{alias}.j_created_by = '{}'",
            principal.replace('\'', "''")
        )),
        PolicyDef::RefReadable { field } => {
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == *field)
                .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
            let FieldKind::Ref {
                table: ref_table_name,
            } = &field.kind
            else {
                return Err(crate::Error::new(format!(
                    "policy field {} is not a ref",
                    field.name
                )));
            };
            let ref_table = schema.table_def(ref_table_name)?;
            let parent_alias = format!("snapshot_policy_parent_{depth}");
            let parent_tx_alias = format!("snapshot_policy_parent_tx_{depth}");
            let newer_alias = format!("snapshot_policy_newer_{depth}");
            let newer_tx_alias = format!("snapshot_policy_newer_tx_{depth}");
            let parent_policy = lower_snapshot_policy(
                schema,
                ref_table,
                &parent_alias,
                &ref_table.read_policy,
                principal,
                base_epoch,
                depth + 1,
            )?;
            Ok(format!(
                "EXISTS (
                   SELECT 1
                   FROM {} {parent_alias}
                   JOIN jazz_tx {parent_tx_alias}
                     ON {parent_tx_alias}.tx_num = {parent_alias}.tx_num
                   WHERE {parent_alias}.row_num = {alias}.{}
                     AND {parent_alias}.j_branch_num = 1
                     AND {parent_alias}.op != 3
                     AND {parent_tx_alias}.outcome != {}
                     AND {parent_tx_alias}.global_epoch IS NOT NULL
                     AND {parent_tx_alias}.global_epoch <= {base_epoch}
                     AND NOT EXISTS (
                       SELECT 1
                       FROM {} {newer_alias}
                       JOIN jazz_tx {newer_tx_alias}
                         ON {newer_tx_alias}.tx_num = {newer_alias}.tx_num
                       WHERE {newer_alias}.row_num = {parent_alias}.row_num
                         AND {newer_alias}.j_branch_num = 1
                         AND {newer_tx_alias}.outcome != {}
                         AND {newer_tx_alias}.global_epoch IS NOT NULL
                         AND {newer_tx_alias}.global_epoch <= {base_epoch}
                         AND ({newer_tx_alias}.global_epoch > {parent_tx_alias}.global_epoch OR ({newer_tx_alias}.global_epoch = {parent_tx_alias}.global_epoch AND {newer_tx_alias}.tx_num > {parent_tx_alias}.tx_num))
                     )
                     AND {parent_policy}
                 )",
                crate::schema::history_table(&ref_table.name),
                crate::schema::quote_ident(&crate::schema::storage_column(field)),
                tx::OUTCOME_REJECTED,
                crate::schema::history_table(&ref_table.name),
                tx::OUTCOME_REJECTED,
            ))
        }
    }
}
