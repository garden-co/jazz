use crate::schema::{FieldDef, FieldKind, IndexDef, TableDef};
use crate::{Error, Result};
use rusqlite::types::Value as SqlValue;
use serde_json::Value as JsonValue;

pub(crate) struct TablePlan<'a> {
    pub(crate) table: &'a TableDef,
    pub(crate) history: String,
    pub(crate) current: String,
    pub(crate) user_columns: Vec<String>,
}

impl<'a> TablePlan<'a> {
    pub(crate) fn new(table: &'a TableDef) -> Self {
        Self {
            table,
            history: quote_ident(&format!("{}__schema_v1_history", table.name)),
            current: quote_ident(&format!("{}__schema_v1_current", table.name)),
            user_columns: table
                .fields
                .iter()
                .map(|field| quote_ident(&field.name))
                .collect(),
        }
    }

    pub(crate) fn user_column_defs(&self) -> String {
        self.table
            .fields
            .iter()
            .zip(&self.user_columns)
            .map(|(field, column)| format!("{column} {}", sql_type(&field.kind)))
            .collect::<Vec<_>>()
            .join(",\n  ")
    }

    pub(crate) fn index_name(&self, suffix: &str) -> String {
        quote_ident(&format!("{}__schema_v1_{suffix}", self.table.name))
    }

    pub(crate) fn current_index_name(&self, index: &IndexDef) -> String {
        quote_ident(&format!(
            "{}__schema_v1_current_{}",
            self.table.name, index.name
        ))
    }

    pub(crate) fn physical_column(&self, column: &str) -> String {
        system_column(column)
            .map(str::to_owned)
            .unwrap_or_else(|| quote_ident(column))
    }

    pub(crate) fn aliased_column(&self, alias: &str, column: &str) -> String {
        format!("{alias}.{}", self.physical_column(column))
    }
}

pub(crate) fn read_field(
    row: &rusqlite::Row<'_>,
    idx: usize,
    field: &FieldDef,
) -> rusqlite::Result<JsonValue> {
    match field.kind {
        FieldKind::Text | FieldKind::Ref { .. } => {
            let value: String = row.get(idx)?;
            Ok(JsonValue::String(value))
        }
        FieldKind::Bool => {
            let value: i64 = row.get(idx)?;
            Ok(JsonValue::Bool(value != 0))
        }
    }
}

pub(crate) fn json_to_sql(value: &JsonValue, kind: &FieldKind) -> Result<SqlValue> {
    match kind {
        FieldKind::Text | FieldKind::Ref { .. } => value
            .as_str()
            .map(|value| SqlValue::Text(value.to_owned()))
            .ok_or_else(|| Error::new("expected string value")),
        FieldKind::Bool => value
            .as_bool()
            .map(|value| SqlValue::Integer(i64::from(value)))
            .ok_or_else(|| Error::new("expected bool value")),
    }
}

fn sql_type(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Text | FieldKind::Ref { .. } => "TEXT",
        FieldKind::Bool => "INTEGER",
    }
}

pub(crate) fn system_column(column: &str) -> Option<&'static str> {
    match column {
        "$createdAt" => Some("j_created_at"),
        "$updatedAt" => Some("j_updated_at"),
        "$rowId" => Some("j_row_id"),
        "$txId" => Some("j_visible_tx_id"),
        _ => None,
    }
}

pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub(crate) fn placeholders(count: usize) -> String {
    (0..count).map(|_| "?").collect::<Vec<_>>().join(", ")
}
