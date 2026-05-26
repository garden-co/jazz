use crate::schema::{FieldKind, PolicyDef, SchemaDef, TableDef};
use crate::{tx, Result};
use rusqlite::{params, Connection};

pub(crate) fn write_allowed(
    db: &Connection,
    table_name: &str,
    policy: &PolicyDef,
    row_num: i64,
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
                    crate::schema::current_table(table_name)
                ),
                params![row_num, principal],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        }
        PolicyDef::RefReadable { .. } => Ok(true),
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
