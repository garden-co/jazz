use crate::schema::{current_table, history_table, quote_ident, storage_column, SchemaDef};
use crate::value::Value as JsonValue;
use crate::{branch, query, tx, Result};
use rusqlite::{params, Connection};
use std::collections::BTreeMap;

pub(crate) fn row_values(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<BTreeMap<String, JsonValue>>> {
    let table = schema.table_def(table_name)?;
    if branch_num != 1 {
        if let Some(values) = current_row_values_exact(conn, table, row_num, branch_num)? {
            return Ok(Some(values));
        }
        let mut source_candidates = Vec::new();
        for source_branch_num in branch::direct_source_nums(conn, branch_num)? {
            if let Some(values) = row_values(conn, schema, table_name, row_num, source_branch_num)?
            {
                source_candidates.push(values);
            }
        }
        if source_candidates.len() > 1 {
            return Err(crate::Error::new("ambiguous branch row source candidates"));
        }
        if let Some(values) = source_candidates.into_iter().next() {
            return Ok(Some(values));
        }
        if let Some(base_epoch) = branch::base_global_epoch(conn, branch_num)? {
            if !current_row_exists_on_branch(conn, table_name, row_num, branch_num)? {
                return snapshot_row_values(conn, table, row_num, base_epoch);
            }
        }
        return Ok(None);
    }
    current_row_values_exact(conn, table, row_num, branch_num)
}

fn current_row_values_exact(
    conn: &Connection,
    table: &crate::schema::TableDef,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<BTreeMap<String, JsonValue>>> {
    let field_columns = table
        .fields
        .iter()
        .map(|field| quote_ident(&storage_column(field)))
        .collect::<Vec<_>>();
    let sql = format!(
        "SELECT {}
         FROM {} current
         JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
         WHERE current.row_num = ?
           AND current.j_branch_num = ?
           AND current.is_deleted = 0
           AND tx.outcome != ?
         LIMIT 1",
        field_columns.join(", "),
        current_table(&table.name)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query_map(params![row_num, branch_num, tx::OUTCOME_REJECTED], |row| {
        (0..table.fields.len())
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()
    })?;
    let Some(row) = rows.next().transpose()? else {
        return Ok(None);
    };
    row_values_from_sql(conn, table, row)
}

fn snapshot_row_values(
    conn: &Connection,
    table: &crate::schema::TableDef,
    row_num: i64,
    base_epoch: i64,
) -> Result<Option<BTreeMap<String, JsonValue>>> {
    let field_columns = table
        .fields
        .iter()
        .map(|field| format!("h.{}", quote_ident(&storage_column(field))))
        .collect::<Vec<_>>();
    let sql = format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_tx_public tx ON tx.tx_num = h.tx_num
         WHERE h.row_num = ?
           AND h.j_branch_num = 1
           AND h.op != 3
           AND tx.outcome != ?
           AND tx.global_epoch IS NOT NULL
           AND tx.global_epoch <= ?
           AND NOT EXISTS (
             SELECT 1
             FROM {history_table} newer
             JOIN jazz_tx_public newer_tx ON newer_tx.tx_num = newer.tx_num
             WHERE newer.row_num = h.row_num
               AND newer.j_branch_num = 1
               AND newer_tx.outcome != ?
               AND newer_tx.global_epoch IS NOT NULL
               AND newer_tx.global_epoch <= ?
               AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
           )
         LIMIT 1",
        field_columns.join(", "),
        history_table(&table.name),
        history_table = history_table(&table.name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query_map(
        params![
            row_num,
            tx::OUTCOME_REJECTED,
            base_epoch,
            tx::OUTCOME_REJECTED,
            base_epoch
        ],
        |row| {
            (0..table.fields.len())
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        },
    )?;
    let Some(row) = rows.next().transpose()? else {
        return Ok(None);
    };
    row_values_from_sql(conn, table, row)
}

fn row_values_from_sql(
    conn: &Connection,
    table: &crate::schema::TableDef,
    row: Vec<rusqlite::types::Value>,
) -> Result<Option<BTreeMap<String, JsonValue>>> {
    let mut values = BTreeMap::new();
    for (idx, field) in table.fields.iter().enumerate() {
        values.insert(
            field.name.clone(),
            query::sql_value_to_json(conn, field, &row[idx])?,
        );
    }
    Ok(Some(values))
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
            current_table(table_name)
        ),
        params![row_num, branch_num],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}
