//! Derived membership role `jazz.sqlite.uuid-array-index.v2`.
//! Current and historical images have separate kinds so moving an image to
//! history cannot erase its membership when the current image is replaced.
use std::collections::BTreeSet;

use rusqlite::Connection;

use crate::options::{Error, ErrorCode};
use crate::records::ValueType;
use crate::values::{STATE_AS_SENT, Schema, Sql, Table, quote, storage};

pub(crate) fn uuid_array(table: &Table, column: &str) -> bool {
    let Some(column) = table.column(column) else {
        return false;
    };
    fn unwrap(ty: &ValueType) -> &ValueType {
        match ty {
            ValueType::Nullable(inner) => inner.as_ref(),
            ty => ty,
        }
    }
    matches!(unwrap(&column.ty), ValueType::Array(inner) if matches!(unwrap(inner), ValueType::Uuid))
}

pub(crate) fn name(table: &Table, column: &str) -> Option<String> {
    if !uuid_array(table, column)
        || !table
            .indexes
            .iter()
            .any(|index| index.first().is_some_and(|first| first == column))
    {
        return None;
    }
    let stored = table.stored_name(column);
    Some(format!(
        "_jazz_array_members_v2_{}:{}_{}:{}",
        table.stored.len(),
        table.stored,
        stored.len(),
        stored
    ))
}

fn ddl(table: &Table, column: &str, name: &str) -> String {
    let index = quote(name);
    let row_index = quote(&format!("{name}_row"));
    let column = quote(table.stored_name(column));
    let mut sql = format!(
        "CREATE TABLE {index} (member BLOB NOT NULL CHECK(length(member)=16), kind INTEGER NOT NULL CHECK(kind IN(0,1)), id BLOB NOT NULL CHECK(length(id)=16), branch BLOB NOT NULL, clock INTEGER NOT NULL CHECK(clock>=0), writer BLOB NOT NULL, PRIMARY KEY(member,kind,id,branch,clock,writer)) WITHOUT ROWID;\nCREATE INDEX {row_index} ON {index}(kind,id,branch,clock,writer);\n"
    );
    for (kind, source) in [
        (0, table.stored.clone()),
        (1, format!("{}__h", table.stored)),
    ] {
        let source = quote(&source);
        let add = |row: &str, from: &str| {
            format!(
                "INSERT OR IGNORE INTO {index}(member,kind,id,branch,clock,writer) SELECT unhex(replace(e.value,'-','')), {kind}, {row}.id, {row}._branch, {row}._clock, {row}._writer FROM {from}json_each({row}.{column}) e WHERE ({row}._state & {STATE_AS_SENT})=0 AND e.type='text' AND length(unhex(replace(e.value,'-','')))=16;"
            )
        };
        let remove = format!(
            "DELETE FROM {index} WHERE kind={kind} AND id=old.id AND branch=old._branch AND clock=old._clock AND writer=old._writer;"
        );
        for (suffix, event, body) in [
            ("insert", "INSERT".to_owned(), add("new", "")),
            ("delete", "DELETE".to_owned(), remove.clone()),
            (
                "update",
                format!("UPDATE OF {column},id,_branch,_clock,_writer,_state"),
                format!("{remove} {}", add("new", "")),
            ),
        ] {
            let trigger = quote(&format!("{name}_{kind}_{suffix}"));
            sql.push_str(&format!(
                "CREATE TRIGGER {trigger} AFTER {event} ON {source} BEGIN {body} END;\n"
            ));
        }
        sql.push_str(&add("s", &format!("{source} s, ")));
        sql.push('\n');
    }
    sql
}

/// Installation/backfill is atomic per column. Reopening an installed index
/// does not re-enumerate rows. Triggers maintain it in the row write's savepoint,
/// including sync replay, compaction, schema views and rollback.
pub(crate) fn install(conn: &Connection, schema: &Schema) -> Result<(), Error> {
    for table in schema.tables.values() {
        for column in &table.columns {
            let Some(name) = name(table, &column.name) else {
                continue;
            };
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
                    [&name],
                    |row| row.get(0),
                )
                .map_err(storage)?;
            if exists {
                continue;
            }
            conn.execute_batch("SAVEPOINT jazz_uuid_array_index_v2")
                .map_err(storage)?;
            let result = crate::db::execute_script(conn, &ddl(table, &column.name, &name));
            if result.is_err() {
                conn.execute_batch(
                    "ROLLBACK TO jazz_uuid_array_index_v2; RELEASE jazz_uuid_array_index_v2",
                )
                .map_err(storage)?;
            } else {
                conn.execute_batch("RELEASE jazz_uuid_array_index_v2")
                    .map_err(storage)?;
            }
            result?;
        }
    }
    Ok(())
}

pub(crate) fn members(value: &Sql) -> Result<Vec<Sql>, Error> {
    if matches!(value, Sql::Null) {
        return Ok(Vec::new());
    }
    let Sql::Text(bytes) = value else {
        return Err(Error::new(
            ErrorCode::Storage,
            "UUID array evidence is not JSON text",
        ));
    };
    let values: Vec<Option<uuid::Uuid>> = serde_json::from_slice(bytes).map_err(|error| {
        Error::new(
            ErrorCode::Storage,
            format!("invalid UUID array evidence: {error}"),
        )
    })?;
    Ok(values
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|value| Sql::Blob(value.as_bytes().to_vec()))
        .collect())
}

#[cfg(test)]
#[path = "array_index_tests.rs"]
mod tests;
