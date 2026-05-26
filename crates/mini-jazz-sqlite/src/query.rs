use crate::rows::{public_row_id, row_num};
use crate::schema::{FieldDef, FieldKind, SchemaDef};
use crate::types::RowView;
use crate::{branch, policy, tx, Result};
use rusqlite::{params, Connection};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) struct QueryContext<'a> {
    pub(crate) conn: &'a Connection,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) branch_num: i64,
    pub(crate) principal: &'a str,
    pub(crate) trusted: bool,
}

impl QueryContext<'_> {
    pub(crate) fn read_rows(&self, table_name: &str) -> Result<Vec<RowView>> {
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
                let mut rows = self.read_rows_from_current(table_name, false)?;
                rows.extend(self.read_main_snapshot_rows(table_name, base_epoch)?);
                return Ok(rows);
            }
        }
        self.read_rows_from_current(table_name, true)
    }

    pub(crate) fn read_row_candidates(&self, table_name: &str, id: &str) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let row_num = row_num(self.conn, id)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push("current.j_created_by".to_owned());
        let sql = format!(
            "SELECT {}
             FROM jazz_branch_source source
             JOIN {} current ON current.j_branch_num = source.source_branch_num
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE source.branch_num = ?
               AND current.row_num = ?
               AND current.is_deleted = 0
               AND tx.outcome != ?
             ORDER BY source.source_branch_num",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(
            params![self.branch_num, row_num, tx::OUTCOME_REJECTED],
            |row| {
                (0..row_width)
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
        rows.map(|row| row_to_view(self.conn, table_name, table, row?))
            .collect()
    }

    fn read_policy_sql(&self, table: &crate::schema::TableDef) -> Result<String> {
        if self.trusted {
            Ok("1 = 1".to_owned())
        } else {
            policy::read_policy_sql(self.schema, table, self.principal)
        }
    }

    fn read_rows_from_current(&self, table_name: &str, overlay_main: bool) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push("current.j_created_by".to_owned());
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
            WHERE current.is_deleted = 0
               AND (
                 current.j_branch_num = ?
                 OR (
                   ? = 1
                   AND ? != 1
                   AND current.j_branch_num = 1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM {current_table} branch_current
                     WHERE branch_current.row_num = current.row_num
                       AND branch_current.j_branch_num = ?
                   )
                 )
               )
               AND tx.outcome != ?
               AND {policy_sql}
             ORDER BY current.j_created_at DESC, current.row_num",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(
            params![
                self.branch_num,
                if overlay_main { 1 } else { 0 },
                self.branch_num,
                self.branch_num,
                tx::OUTCOME_REJECTED
            ],
            |row| {
                (0..row_width)
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
        rows.map(|row| row_to_view(self.conn, table_name, table, row?))
            .collect()
    }

    fn read_main_snapshot_rows(&self, table_name: &str, base_epoch: i64) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
        select_columns.push("h.j_created_by".to_owned());
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num = 1
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND h.op != 3
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = 1
                   AND newer_tx.outcome != ?
                   AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?
                   AND newer_tx.global_epoch > tx.global_epoch
               )
               AND NOT EXISTS (
                 SELECT 1
                 FROM {current_table} branch_current
                 WHERE branch_current.row_num = h.row_num
                   AND branch_current.j_branch_num = ?
               )
             ORDER BY h.j_created_at DESC, h.row_num",
            select_columns.join(", "),
            crate::schema::history_table(table_name),
            history_table = crate::schema::history_table(table_name),
            current_table = crate::schema::current_table(table_name),
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(
            params![
                tx::OUTCOME_REJECTED,
                base_epoch,
                tx::OUTCOME_REJECTED,
                base_epoch,
                self.branch_num
            ],
            |row| {
                (0..row_width)
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
        rows.map(|row| row_to_view(self.conn, table_name, table, row?))
            .collect()
    }
}

fn row_to_view(
    conn: &Connection,
    table_name: &str,
    table: &crate::schema::TableDef,
    raw: Vec<rusqlite::types::Value>,
) -> Result<RowView> {
    let mut values = BTreeMap::new();
    for (idx, field) in table.fields.iter().enumerate() {
        values.insert(
            field.name.clone(),
            sql_value_to_json(conn, field, &raw[idx + 2])?,
        );
    }
    Ok(RowView {
        table: table_name.to_owned(),
        id: text_value(&raw[0], "row_id")?,
        tx_id: text_value(&raw[1], "tx_id")?,
        values,
        created_by: text_value(&raw[2 + table.fields.len()], "j_created_by")?,
    })
}

pub(crate) fn sql_value_to_json(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
) -> Result<JsonValue> {
    match (&field.kind, value) {
        (FieldKind::Text, rusqlite::types::Value::Text(value)) => {
            Ok(JsonValue::String(value.clone()))
        }
        (FieldKind::Bool, rusqlite::types::Value::Integer(value)) => {
            Ok(JsonValue::Bool(*value != 0))
        }
        (FieldKind::Ref { .. }, rusqlite::types::Value::Integer(row_num)) => {
            Ok(JsonValue::String(public_row_id(conn, *row_num)?))
        }
        _ => Err(crate::Error::new(format!(
            "unexpected SQL value for field {}",
            field.name
        ))),
    }
}

pub(crate) fn text_value(value: &rusqlite::types::Value, name: &str) -> Result<String> {
    match value {
        rusqlite::types::Value::Text(value) => Ok(value.clone()),
        _ => Err(crate::Error::new(format!("expected text {name}"))),
    }
}
