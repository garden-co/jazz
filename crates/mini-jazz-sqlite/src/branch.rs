use crate::Result;
use rusqlite::{params, Connection};

pub(crate) fn ensure(
    conn: &Connection,
    branch_id: &str,
    base_global_epoch: Option<i64>,
    now: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT OR IGNORE INTO jazz_branch (branch_id, base_global_epoch, created_at)
         VALUES (?, ?, ?)",
        params![branch_id, base_global_epoch, now],
    )?;
    Ok(conn.query_row(
        "SELECT branch_num FROM jazz_branch WHERE branch_id = ?",
        params![branch_id],
        |row| row.get(0),
    )?)
}

pub(crate) fn checkout(conn: &Connection, branch_id: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT branch_num FROM jazz_branch WHERE branch_id = ?",
        params![branch_id],
        |row| row.get(0),
    )?)
}

pub(crate) fn base_global_epoch(conn: &Connection, branch_num: i64) -> Result<Option<i64>> {
    Ok(conn.query_row(
        "SELECT base_global_epoch FROM jazz_branch WHERE branch_num = ?",
        params![branch_num],
        |row| row.get(0),
    )?)
}

pub(crate) fn add_source(conn: &Connection, branch_num: i64, source_branch_id: &str) -> Result<()> {
    let source_branch_num = ensure(conn, source_branch_id, None, 0)?;
    conn.execute(
        "INSERT OR IGNORE INTO jazz_branch_source (branch_num, source_branch_num)
         VALUES (?, ?)",
        params![branch_num, source_branch_num],
    )?;
    Ok(())
}

pub(crate) fn scope_nums(conn: &Connection, branch_num: i64) -> Result<Vec<i64>> {
    let mut nums = vec![branch_num];
    let mut stmt = conn.prepare(
        "SELECT source_branch_num
         FROM jazz_branch_source
         WHERE branch_num = ?
         ORDER BY source_branch_num",
    )?;
    let sources = stmt
        .query_map(params![branch_num], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    nums.extend(sources);
    nums.sort();
    nums.dedup();
    Ok(nums)
}
