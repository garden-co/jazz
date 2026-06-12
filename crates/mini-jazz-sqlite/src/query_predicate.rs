use crate::rows::ensure_row_id;
use crate::schema::{FieldDef, FieldKind};
use crate::Result;
use rusqlite::Connection;
use serde_json::Value as JsonValue;

pub(crate) fn sql(field: &FieldDef, column: &str, op: &str) -> Result<String> {
    match op {
        "eq" => Ok(format!("{column} = ?")),
        "ne" => Ok(format!("{column} IS NOT ?")),
        "contains" if matches!(field.kind, FieldKind::Text) => {
            Ok(format!("instr({column}, ?) > 0"))
        }
        "contains" => Err(crate::Error::new(format!(
            "contains only supports text fields, got {}",
            field.name
        ))),
        _ => Err(crate::Error::new(format!(
            "unsupported query predicate {op}"
        ))),
    }
}

pub(crate) fn value(
    field: &FieldDef,
    op: &str,
    value: &JsonValue,
    conn: &Connection,
) -> Result<rusqlite::types::Value> {
    match op {
        "eq" | "ne" => crate::schema::field_sql_value(field, value, |ref_table, row_id| {
            ensure_row_id(conn, ref_table, row_id)
        }),
        "contains" if matches!(field.kind, FieldKind::Text) => Ok(rusqlite::types::Value::Text(
            value
                .as_str()
                .ok_or_else(|| crate::Error::new("contains expects a string value"))?
                .to_owned(),
        )),
        "contains" => Err(crate::Error::new(format!(
            "contains only supports text fields, got {}",
            field.name
        ))),
        _ => Err(crate::Error::new(format!(
            "unsupported query predicate {op}"
        ))),
    }
}
