use crate::Result;
use rusqlite::{params, Connection};
use std::collections::{BTreeMap, BTreeSet};

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
    let branch_num = conn.query_row(
        "SELECT branch_num FROM jazz_branch WHERE branch_id = ?",
        params![branch_id],
        |row| row.get(0),
    )?;
    let stored_base: Option<i64> = conn.query_row(
        "SELECT base_global_epoch FROM jazz_branch WHERE branch_num = ?",
        params![branch_num],
        |row| row.get(0),
    )?;
    if let Some(base_global_epoch) = base_global_epoch {
        if stored_base.is_none() {
            conn.execute(
                "UPDATE jazz_branch
                 SET base_global_epoch = ?
                 WHERE branch_num = ? AND base_global_epoch IS NULL",
                params![base_global_epoch, branch_num],
            )?;
        } else if stored_base != Some(base_global_epoch) {
            return Err(crate::Error::new(format!(
                "branch base mismatch for {branch_id}"
            )));
        }
    }
    sync_backing_row(conn, branch_num)?;
    Ok(branch_num)
}

pub(crate) fn checkout(conn: &Connection, branch_id: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT branch_num FROM jazz_branch WHERE branch_id = ?",
        params![branch_id],
        |row| row.get(0),
    )?)
}

pub(crate) fn id_for_num(conn: &Connection, branch_num: i64) -> Result<String> {
    conn.query_row(
        "SELECT branch_id FROM jazz_branch WHERE branch_num = ?",
        params![branch_num],
        |row| row.get(0),
    )
    .map_err(Into::into)
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
    if source_reaches_branch(conn, source_branch_num, branch_num, None)? {
        return Err(crate::Error::new(format!(
            "branch source cycle involving {source_branch_id}"
        )));
    }
    conn.execute(
        "INSERT OR IGNORE INTO jazz_branch_source (branch_num, source_branch_num)
         VALUES (?, ?)",
        params![branch_num, source_branch_num],
    )?;
    bump_source_version(conn, branch_num)?;
    sync_backing_row(conn, branch_num)?;
    Ok(())
}

pub(crate) fn remove_source(
    conn: &Connection,
    branch_num: i64,
    source_branch_id: &str,
) -> Result<()> {
    let source_branch_num = checkout(conn, source_branch_id)?;
    conn.execute(
        "DELETE FROM jazz_branch_source
         WHERE branch_num = ? AND source_branch_num = ?",
        params![branch_num, source_branch_num],
    )?;
    bump_source_version(conn, branch_num)?;
    sync_backing_row(conn, branch_num)?;
    Ok(())
}

pub(crate) fn set_sources(
    conn: &Connection,
    branch_num: i64,
    source_branch_ids: &[String],
) -> Result<()> {
    let source_branch_nums = source_branch_ids
        .iter()
        .map(|source_branch_id| ensure(conn, source_branch_id, None, 0))
        .collect::<Result<Vec<_>>>()?;
    for source_branch_num in &source_branch_nums {
        if source_reaches_branch(
            conn,
            *source_branch_num,
            branch_num,
            Some(&source_branch_nums),
        )? {
            return Err(crate::Error::new("branch source cycle"));
        }
    }
    conn.execute(
        "DELETE FROM jazz_branch_source WHERE branch_num = ?",
        params![branch_num],
    )?;
    for source_branch_num in source_branch_nums {
        conn.execute(
            "INSERT OR IGNORE INTO jazz_branch_source (branch_num, source_branch_num)
             VALUES (?, ?)",
            params![branch_num, source_branch_num],
        )?;
    }
    sync_backing_row(conn, branch_num)?;
    Ok(())
}

pub(crate) fn source_version(conn: &Connection, branch_num: i64) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT source_version FROM jazz_branch WHERE branch_num = ?",
        params![branch_num],
        |row| row.get(0),
    )?)
}

pub(crate) fn direct_source_nums(conn: &Connection, branch_num: i64) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare(
        "SELECT source_branch_num
         FROM jazz_branch_source
         WHERE branch_num = ?
         ORDER BY source_branch_num",
    )?;
    let sources = stmt
        .query_map(params![branch_num], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(sources)
}

pub(crate) fn set_sources_from_sync(
    conn: &Connection,
    branch_num: i64,
    source_branch_ids: &[String],
    source_version: i64,
) -> Result<()> {
    if source_version < self::source_version(conn, branch_num)? {
        return Ok(());
    }
    set_sources(conn, branch_num, source_branch_ids)?;
    conn.execute(
        "UPDATE jazz_branch SET source_version = ? WHERE branch_num = ?",
        params![source_version, branch_num],
    )?;
    sync_backing_row(conn, branch_num)?;
    Ok(())
}

fn bump_source_version(conn: &Connection, branch_num: i64) -> Result<()> {
    conn.execute(
        "UPDATE jazz_branch SET source_version = source_version + 1 WHERE branch_num = ?",
        params![branch_num],
    )?;
    Ok(())
}

pub(crate) fn scope_nums(conn: &Connection, branch_num: i64) -> Result<Vec<i64>> {
    Ok(scope_depths(conn, branch_num)?
        .into_keys()
        .collect::<Vec<_>>())
}

pub(crate) fn scope_depths(conn: &Connection, branch_num: i64) -> Result<BTreeMap<i64, i64>> {
    let mut depths = BTreeMap::new();
    let mut stack = vec![(branch_num, 0)];
    while let Some(current_branch_num) = stack.pop() {
        let (current_branch_num, depth) = current_branch_num;
        if depths
            .get(&current_branch_num)
            .is_some_and(|existing_depth| *existing_depth <= depth)
        {
            continue;
        }
        depths.insert(current_branch_num, depth);
        let mut stmt = conn.prepare(
            "SELECT source_branch_num
             FROM jazz_branch_source
             WHERE branch_num = ?
             ORDER BY source_branch_num DESC",
        )?;
        let sources = stmt
            .query_map(params![current_branch_num], |row| row.get::<_, i64>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        stack.extend(
            sources
                .into_iter()
                .map(|source_branch_num| (source_branch_num, depth + 1)),
        );
    }
    Ok(depths)
}

fn source_reaches_branch(
    conn: &Connection,
    source_branch_num: i64,
    target_branch_num: i64,
    target_replacement_sources: Option<&[i64]>,
) -> Result<bool> {
    let mut stack = vec![source_branch_num];
    let mut visited = BTreeSet::new();
    while let Some(branch_num) = stack.pop() {
        if branch_num == target_branch_num {
            return Ok(true);
        }
        if !visited.insert(branch_num) {
            continue;
        }
        if let Some(replacement_sources) = target_replacement_sources {
            if branch_num == target_branch_num {
                stack.extend(replacement_sources.iter().copied());
                continue;
            }
        }
        let mut stmt = conn.prepare(
            "SELECT source_branch_num
             FROM jazz_branch_source
             WHERE branch_num = ?",
        )?;
        let sources = stmt
            .query_map(params![branch_num], |row| row.get::<_, i64>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        stack.extend(sources);
    }
    Ok(false)
}

fn sync_backing_row(conn: &Connection, branch_num: i64) -> Result<()> {
    let (branch_id, base_global_epoch, created_at): (String, Option<i64>, i64) = conn.query_row(
        "SELECT branch_id, base_global_epoch, created_at
         FROM jazz_branch
         WHERE branch_num = ?",
        params![branch_num],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let mut stmt = conn.prepare(
        "SELECT source.branch_id
         FROM jazz_branch_source branch_source
         JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
         WHERE branch_source.branch_num = ?
         ORDER BY source.branch_id",
    )?;
    let source_branch_ids = stmt
        .query_map(params![branch_num], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    conn.execute(
        "INSERT INTO jazz_branch_backing
         (branch_id, base_global_epoch, source_branch_ids_json, created_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(branch_id) DO UPDATE SET
           base_global_epoch = excluded.base_global_epoch,
           source_branch_ids_json = excluded.source_branch_ids_json,
           created_at = excluded.created_at",
        params![
            branch_id,
            base_global_epoch,
            serde_json::to_string(&source_branch_ids)
                .map_err(|err| crate::Error::new(err.to_string()))?,
            created_at
        ],
    )?;
    Ok(())
}
