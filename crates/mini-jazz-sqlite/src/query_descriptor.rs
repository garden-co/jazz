use crate::sync::{Bundle, QueryReadRecord};
use crate::time::now_ms;
use crate::Result;
use rusqlite::{params, Connection};
use serde_json::Value as JsonValue;

pub(crate) fn clear(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM jazz_query_read", [])?;
    Ok(())
}

pub(crate) fn apply_records(conn: &Connection, bundle: &Bundle) -> Result<()> {
    for query_read in &bundle.query_reads {
        record(conn, query_read)?;
    }
    Ok(())
}

pub(crate) fn record(conn: &Connection, query_read: &QueryReadRecord) -> Result<()> {
    forget_same_descriptor(conn, query_read)?;
    conn.execute(
        "INSERT OR REPLACE INTO jazz_query_read
         (branch_id, table_name, field_name, op, value_json, observed_at)
         VALUES (?, ?, ?, ?, ?, ?)",
        params![
            query_read.branch_id,
            query_read.table,
            query_read.field,
            query_read.op,
            serde_json::to_string(&query_read.value)
                .map_err(|err| crate::Error::new(err.to_string()))?,
            now_ms()
        ],
    )?;
    Ok(())
}

fn forget_same_descriptor(conn: &Connection, query_read: &QueryReadRecord) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT value_json
         FROM jazz_query_read
         WHERE branch_id = ?
           AND table_name = ?
           AND field_name = ?
           AND op = ?",
    )?;
    let rows = stmt.query_map(
        params![
            query_read.branch_id,
            query_read.table,
            query_read.field,
            query_read.op
        ],
        |row| row.get::<_, String>(0),
    )?;
    let existing_values = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    let new_identity = crate::observed_query::identity_value(query_read)?;
    for existing_value_json in existing_values {
        let existing_value = serde_json::from_str::<JsonValue>(&existing_value_json)
            .map_err(|err| crate::Error::new(err.to_string()))?;
        let existing_read = QueryReadRecord {
            branch_id: query_read.branch_id.clone(),
            table: query_read.table.clone(),
            field: query_read.field.clone(),
            op: query_read.op.clone(),
            value: existing_value,
        };
        if crate::observed_query::identity_value(&existing_read)? == new_identity {
            conn.execute(
                "DELETE FROM jazz_query_read
                 WHERE branch_id = ?
                   AND table_name = ?
                   AND field_name = ?
                   AND op = ?
                   AND value_json = ?",
                params![
                    query_read.branch_id,
                    query_read.table,
                    query_read.field,
                    query_read.op,
                    existing_value_json
                ],
            )?;
        }
    }
    Ok(())
}

pub(crate) fn list(conn: &Connection) -> Result<Vec<QueryReadRecord>> {
    let mut stmt = conn.prepare(
        "SELECT branch_id, table_name, field_name, op, value_json
         FROM jazz_query_read
         ORDER BY branch_id, table_name, field_name, op, value_json",
    )?;
    let rows = stmt.query_map([], |row| {
        let value_json: String = row.get(4)?;
        let value = serde_json::from_str(&value_json).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(err))
        })?;
        Ok(QueryReadRecord {
            branch_id: row.get(0)?,
            table: row.get(1)?,
            field: row.get(2)?,
            op: row.get(3)?,
            value,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(crate) fn forget(conn: &Connection, read: &QueryReadRecord) -> Result<()> {
    conn.execute(
        "DELETE FROM jazz_query_read
         WHERE branch_id = ?
           AND table_name = ?
           AND field_name = ?
           AND op = ?
           AND value_json = ?",
        params![
            read.branch_id,
            read.table,
            read.field,
            read.op,
            serde_json::to_string(&read.value).map_err(|err| crate::Error::new(err.to_string()))?
        ],
    )?;
    Ok(())
}
