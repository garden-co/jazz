use crate::views::refresh_views;
use anyhow::{Result, bail};
use duckdb::Connection;
use duckdb::types::ValueRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    pub query: String,
}

#[derive(Debug, Serialize)]
pub struct SqlResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub async fn execute_sql(data_dir: PathBuf, request: SqlRequest) -> Result<SqlResponse> {
    if request.query.trim().is_empty() {
        bail!("missing query");
    }

    tokio::task::spawn_blocking(move || query_inner(data_dir, request.query))
        .await
        .map_err(|err| anyhow::anyhow!(err))?
}

fn query_inner(data_dir: PathBuf, query: String) -> Result<SqlResponse> {
    let conn = Connection::open_in_memory()?;
    refresh_views(&conn, &data_dir)?;

    let mut stmt = conn.prepare(&query)?;
    // The DuckDB Rust driver exposes column metadata only after the statement has run.
    stmt.execute([])?;
    let columns = stmt.column_names().into_iter().collect::<Vec<_>>();
    let column_count = columns.len();
    let mut rows = stmt.query([])?;
    let mut out_rows = Vec::new();

    while let Some(row) = rows.next()? {
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            values.push(value_ref_to_json(row.get_ref(index)?));
        }
        out_rows.push(values);
    }

    Ok(SqlResponse {
        columns,
        rows: out_rows,
    })
}

fn value_ref_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(value) => Value::Bool(value),
        ValueRef::TinyInt(value) => Value::from(value),
        ValueRef::SmallInt(value) => Value::from(value),
        ValueRef::Int(value) => Value::from(value),
        ValueRef::BigInt(value) => Value::from(value),
        ValueRef::HugeInt(value) => Value::from(value.to_string()),
        ValueRef::UTinyInt(value) => Value::from(value),
        ValueRef::USmallInt(value) => Value::from(value),
        ValueRef::UInt(value) => Value::from(value),
        ValueRef::UBigInt(value) => Value::from(value),
        ValueRef::Float(value) => Value::from(value),
        ValueRef::Double(value) => Value::from(value),
        ValueRef::Text(value) => Value::from(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::from(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Timestamp(_, value) => Value::from(value.to_string()),
        ValueRef::Date32(value) => Value::from(value),
        ValueRef::Time64(_, value) => Value::from(value),
        ValueRef::Interval {
            months,
            days,
            nanos,
        } => Value::from(format!("{months} months {days} days {nanos} ns")),
        ValueRef::Decimal(value) => Value::from(value.to_string()),
        ValueRef::Enum(_, index) => Value::from(index),
        ValueRef::List(..)
        | ValueRef::Struct(..)
        | ValueRef::Array(..)
        | ValueRef::Map(..)
        | ValueRef::Union(..) => Value::from(format!("{value:?}")),
    }
}
