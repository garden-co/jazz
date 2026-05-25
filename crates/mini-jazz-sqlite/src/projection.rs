use crate::{tx, Result};
use rusqlite::{params, Connection};

pub(crate) fn clear(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM todos__schema_v1_current", [])?;
    conn.execute("DELETE FROM projects__schema_v1_current", [])?;
    Ok(())
}

pub(crate) fn rebuild(conn: &Connection) -> Result<()> {
    clear(conn)?;
    rebuild_projects(conn)?;
    rebuild_todos(conn)?;
    Ok(())
}

fn rebuild_projects(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT h.row_num, h.tx_num, h.op, h.title, h.j_created_at, h.j_updated_at, h.j_created_by, h.j_updated_by
         FROM projects__schema_v1_history h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE tx.outcome != ?
         ORDER BY h.row_num, h.tx_num",
    )?;
    let rows = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
        ))
    })?;
    for row in rows {
        let (row_num, tx_num, op, title, created_at, updated_at, created_by, updated_by) = row?;
        if op == 3 {
            conn.execute(
                "DELETE FROM projects__schema_v1_current WHERE row_num = ?",
                params![row_num],
            )?;
        } else {
            conn.execute(
                "INSERT OR REPLACE INTO projects__schema_v1_current
                 (row_num, visible_tx_num, is_deleted, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
                 VALUES (?, ?, 0, ?, ?, ?, ?, ?)",
                params![row_num, tx_num, title, created_at, updated_at, created_by, updated_by],
            )?;
        }
    }
    Ok(())
}

fn rebuild_todos(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT h.row_num, h.tx_num, h.op, h.title, h.done, h.project_row_num, h.j_created_at, h.j_updated_at, h.j_created_by, h.j_updated_by
         FROM todos__schema_v1_history h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE tx.outcome != ?
         ORDER BY h.row_num, h.tx_num",
    )?;
    let rows = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, String>(8)?,
            row.get::<_, String>(9)?,
        ))
    })?;
    for row in rows {
        let (
            row_num,
            tx_num,
            op,
            title,
            done,
            project_row_num,
            created_at,
            updated_at,
            created_by,
            updated_by,
        ) = row?;
        if op == 3 {
            conn.execute(
                "DELETE FROM todos__schema_v1_current WHERE row_num = ?",
                params![row_num],
            )?;
        } else {
            conn.execute(
                "INSERT OR REPLACE INTO todos__schema_v1_current
                 (row_num, visible_tx_num, is_deleted, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
                 VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    row_num,
                    tx_num,
                    title,
                    done,
                    project_row_num,
                    created_at,
                    updated_at,
                    created_by,
                    updated_by
                ],
            )?;
        }
    }
    Ok(())
}
