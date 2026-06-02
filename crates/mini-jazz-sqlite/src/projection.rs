use crate::schema::{current_table, history_table, quote_ident, storage_column, SchemaDef};
use crate::sync::history_op;
use crate::{tx, Result};
use rusqlite::{params, params_from_iter, Connection};

pub(crate) fn clear(conn: &Connection, schema: &SchemaDef) -> Result<()> {
    for table in schema.tables() {
        conn.execute(&format!("DELETE FROM {}", current_table(&table.name)), [])?;
    }
    Ok(())
}

pub(crate) fn rebuild(conn: &Connection, schema: &SchemaDef, local_node_num: i64) -> Result<()> {
    clear(conn, schema)?;
    for table in schema.tables() {
        rebuild_table(conn, table, local_node_num)?;
    }
    Ok(())
}

fn rebuild_table(
    conn: &Connection,
    table: &crate::schema::TableDef,
    local_node_num: i64,
) -> Result<()> {
    let field_columns = table
        .fields
        .iter()
        .map(|field| quote_ident(&storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "h.row_num".to_owned(),
        "h.tx_num".to_owned(),
        "h.j_branch_num".to_owned(),
        "h.op".to_owned(),
    ];
    select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
    select_columns.extend([
        "h.j_created_at".to_owned(),
        "h.j_updated_at".to_owned(),
        "h.j_created_by".to_owned(),
        "h.j_updated_by".to_owned(),
    ]);
    let sql = format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE tx.outcome != ?
           AND NOT (tx.outcome = ? AND tx.conflict_mode = ?)
           AND NOT EXISTS (
             SELECT 1
             FROM jazz_tx_awaiting_dependency awaiting
             WHERE awaiting.tx_num = tx.tx_num
           )
         ORDER BY h.row_num,
                  h.j_branch_num,
                  CASE
                    WHEN tx.outcome = ? AND tx.global_epoch IS NULL AND tx.node_num != ? THEN 0
                    WHEN tx.global_epoch IS NOT NULL OR tx.outcome = ? THEN 1
                    ELSE 2
                  END,
                  tx.global_epoch,
                  tx.tx_num",
        select_columns.join(", "),
        history_table(&table.name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let rows = stmt.query_map(
        params![
            tx::OUTCOME_REJECTED,
            tx::OUTCOME_PENDING,
            tx::MODE_EXCLUSIVE,
            tx::OUTCOME_PENDING,
            local_node_num,
            tx::OUTCOME_ACCEPTED
        ],
        |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        },
    )?;

    for row in rows {
        let values = row?;
        let row_num = integer_value(&values[0], "row_num")?;
        let tx_num = integer_value(&values[1], "tx_num")?;
        let branch_num = integer_value(&values[2], "j_branch_num")?;
        let op = integer_value(&values[3], "op")?;
        if op == history_op::DELETE {
            conn.execute(
                &format!(
                    "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
                    current_table(&table.name)
                ),
                params![row_num, branch_num],
            )?;
            if branch_num != 1 {
                let mut columns = vec![
                    "row_num".to_owned(),
                    "j_branch_num".to_owned(),
                    "visible_tx_num".to_owned(),
                    "is_deleted".to_owned(),
                ];
                columns.extend(field_columns.iter().cloned());
                columns.extend([
                    "j_created_at".to_owned(),
                    "j_updated_at".to_owned(),
                    "j_created_by".to_owned(),
                    "j_updated_by".to_owned(),
                ]);

                let mut current_values = vec![
                    rusqlite::types::Value::Integer(row_num),
                    rusqlite::types::Value::Integer(branch_num),
                    rusqlite::types::Value::Integer(tx_num),
                    rusqlite::types::Value::Integer(1),
                ];
                current_values.extend(values.into_iter().skip(4));
                insert_dynamic(conn, &current_table(&table.name), &columns, &current_values)?;
            }
            continue;
        }

        let mut columns = vec![
            "row_num".to_owned(),
            "j_branch_num".to_owned(),
            "visible_tx_num".to_owned(),
            "is_deleted".to_owned(),
        ];
        columns.extend(field_columns.iter().cloned());
        columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);

        let mut current_values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(branch_num),
            rusqlite::types::Value::Integer(tx_num),
            rusqlite::types::Value::Integer(0),
        ];
        current_values.extend(values.into_iter().skip(4));
        insert_dynamic(conn, &current_table(&table.name), &columns, &current_values)?;
    }
    Ok(())
}

fn integer_value(value: &rusqlite::types::Value, name: &str) -> Result<i64> {
    match value {
        rusqlite::types::Value::Integer(value) => Ok(*value),
        _ => Err(crate::Error::new(format!("expected integer {name}"))),
    }
}

fn insert_dynamic(
    conn: &Connection,
    table: &str,
    columns: &[String],
    values: &[rusqlite::types::Value],
) -> Result<()> {
    let placeholders = (0..values.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO {table} ({}) VALUES ({placeholders})",
            columns.join(", ")
        ),
        params_from_iter(values.iter()),
    )?;
    Ok(())
}
