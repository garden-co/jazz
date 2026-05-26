use crate::schema::{FieldKind, PolicyDef, SchemaDef, TableDef};
use crate::{tx, Result};
use rusqlite::{params, Connection};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) fn write_allowed(
    db: &Connection,
    schema: &SchemaDef,
    table: &TableDef,
    policy: &PolicyDef,
    row_num: i64,
    values: &BTreeMap<String, JsonValue>,
    principal: &str,
) -> Result<bool> {
    match policy {
        PolicyDef::AllowAll => Ok(true),
        PolicyDef::CreatedByPrincipal => {
            let count: i64 = db.query_row(
                &format!(
                    "SELECT COUNT(*)
                     FROM {} current
                     WHERE current.row_num = ?
                       AND current.j_created_by = ?",
                    crate::schema::current_table(&table.name)
                ),
                params![row_num, principal],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        }
        PolicyDef::RefReadable { field } => {
            let field_def = table
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
            let ref_id = values
                .get(field)
                .and_then(JsonValue::as_str)
                .ok_or_else(|| crate::Error::new(format!("expected ref id for {field}")))?;
            let ref_row_num = crate::rows::row_num(db, ref_id)?;
            let ref_table = schema.table_def(ref_table_name)?;
            let policy_sql = lower_policy(
                schema,
                ref_table,
                "current",
                &ref_table.read_policy,
                principal,
            )?;
            let count: i64 = db.query_row(
                &format!(
                    "SELECT COUNT(*)
                     FROM {} current
                     JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                     WHERE current.row_num = ?
                       AND current.is_deleted = 0
                       AND tx.outcome != {}
                       AND {policy_sql}",
                    crate::schema::current_table(ref_table_name),
                    tx::OUTCOME_REJECTED,
                ),
                params![ref_row_num],
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
    lower_policy(schema, table, "current", &table.read_policy, principal)
}

fn lower_policy(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    policy: &PolicyDef,
    principal: &str,
) -> Result<String> {
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
            let parent_policy = match &ref_table.read_policy {
                PolicyDef::AllowAll => "1 = 1".to_owned(),
                PolicyDef::CreatedByPrincipal => {
                    format!("parent.j_created_by = '{}'", principal.replace('\'', "''"))
                }
                PolicyDef::RefReadable { .. } => {
                    return Err(crate::Error::new(
                        "recursive ref-readable policy not implemented yet",
                    ))
                }
            };
            Ok(format!(
                "EXISTS (
                   SELECT 1
                   FROM {} parent
                   JOIN jazz_tx parent_tx ON parent_tx.tx_num = parent.visible_tx_num
                   WHERE parent.row_num = {alias}.{}
                     AND parent.is_deleted = 0
                     AND parent_tx.outcome != {}
                     AND {parent_policy}
                 )",
                crate::schema::current_table(&ref_table.name),
                crate::schema::quote_ident(&crate::schema::storage_column(field)),
                tx::OUTCOME_REJECTED,
            ))
        }
    }
}
