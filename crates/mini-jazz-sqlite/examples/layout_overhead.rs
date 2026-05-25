use rusqlite::{params, Connection};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const ROWS: i64 = 50_000;
const UPDATES: i64 = 10_000;
const QUERY_REPS: usize = 200;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("rows={ROWS}, updates={UPDATES}, query_reps={QUERY_REPS}");
    println!();
    println!(
        "layout,page_size,file_bytes,page_count,gzip_bytes,zstd_bytes,insert_ms,current_query_ms,snapshot_query_ms"
    );

    let mut runs = Vec::new();
    for page_size in [4096, 8192, 16384] {
        runs.push(run_raw(page_size)?);
        runs.push(run_text_system(page_size)?);
        runs.push(run_compact_system(page_size)?);
    }

    for run in &runs {
        println!(
            "{},{},{},{},{},{},{:.3},{:.3},{:.3}",
            run.layout,
            run.page_size,
            run.file_bytes,
            run.page_count,
            run.gzip_bytes,
            run.zstd_bytes,
            ms(run.insert),
            ms(run.current_query),
            ms(run.snapshot_query)
        );
    }

    println!();
    for page_size in [4096, 8192, 16384] {
        let raw = runs
            .iter()
            .find(|run| run.layout == "raw" && run.page_size == page_size)
            .unwrap();
        for layout in ["text_system", "compact_system"] {
            let run = runs
                .iter()
                .find(|run| run.layout == layout && run.page_size == page_size)
                .unwrap();
            println!(
                "{layout}@{page_size}: file/raw={:.2}x gzip/raw={:.2}x zstd/raw={:.2}x current_query/raw={:.2}x snapshot_query/raw={:.2}x",
                run.file_bytes as f64 / raw.file_bytes as f64,
                run.gzip_bytes as f64 / raw.gzip_bytes as f64,
                run.zstd_bytes as f64 / raw.zstd_bytes as f64,
                run.current_query.as_secs_f64() / raw.current_query.as_secs_f64(),
                run.snapshot_query.as_secs_f64() / raw.snapshot_query.as_secs_f64(),
            );
        }
    }

    Ok(())
}

struct Run {
    layout: &'static str,
    page_size: i64,
    file_bytes: u64,
    page_count: i64,
    gzip_bytes: u64,
    zstd_bytes: u64,
    insert: Duration,
    current_query: Duration,
    snapshot_query: Duration,
}

fn run_raw(page_size: i64) -> Result<Run, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path(), page_size)?;
    conn.execute_batch(
        "
        CREATE TABLE todo (
          row_id INTEGER PRIMARY KEY,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          project_id INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        CREATE INDEX todo_done_created ON todo(done, created_at DESC);
        ",
    )?;

    let insert_start = Instant::now();
    {
        let tx = conn.unchecked_transaction()?;
        for row in 1..=ROWS {
            tx.execute(
                "
                INSERT INTO todo (row_id, title, done, project_id, created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                ",
                params![row, title(row), row % 3 == 0, row % 1_000, row],
            )?;
        }
        for row in 1..=UPDATES {
            tx.execute(
                "UPDATE todo SET title = ?2, updated_at = ?3 WHERE row_id = ?1",
                params![row, format!("updated {row}"), ROWS + row],
            )?;
        }
        tx.commit()?;
    }
    let insert = insert_start.elapsed();
    let current_query = repeat_query(&conn, &current_sql("todo"), &[])?;
    let snapshot_query = current_query;
    finish(
        "raw",
        page_size,
        file.path(),
        &conn,
        insert,
        current_query,
        snapshot_query,
    )
}

fn run_text_system(page_size: i64) -> Result<Run, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path(), page_size)?;
    conn.execute_batch(
        "
        CREATE TABLE jazz_tx (
          tx_id TEXT PRIMARY KEY,
          node_id TEXT NOT NULL,
          local_epoch INTEGER NOT NULL,
          global_epoch INTEGER,
          kind TEXT NOT NULL,
          status TEXT NOT NULL,
          metadata_json TEXT NOT NULL
        );
        CREATE TABLE todo_history (
          j_row_id TEXT NOT NULL,
          j_branch_id TEXT NOT NULL,
          j_tx_id TEXT NOT NULL,
          j_op TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          project_id TEXT NOT NULL,
          j_conflicts_json TEXT NOT NULL,
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          PRIMARY KEY (j_row_id, j_branch_id, j_tx_id)
        );
        CREATE INDEX todo_history_tx ON todo_history(j_branch_id, j_tx_id, j_row_id);
        CREATE INDEX todo_history_row_updated
          ON todo_history(j_branch_id, j_row_id, j_updated_at DESC, j_tx_id);
        CREATE INDEX todo_history_visible_order
          ON todo_history(j_branch_id, done, j_created_at DESC, j_row_id, j_tx_id);
        CREATE TABLE todo_current (
          j_row_id TEXT NOT NULL,
          j_branch_id TEXT NOT NULL,
          j_visible_tx_id TEXT NOT NULL,
          j_is_deleted INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          project_id TEXT NOT NULL,
          j_conflicts_json TEXT NOT NULL,
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          PRIMARY KEY (j_row_id, j_branch_id)
        );
        CREATE INDEX todo_current_done_created
          ON todo_current(j_branch_id, done, j_created_at DESC);
        CREATE TABLE jazz_tx_write (
          tx_id TEXT NOT NULL,
          table_name TEXT NOT NULL,
          row_id TEXT NOT NULL,
          PRIMARY KEY (tx_id, table_name, row_id)
        );
        ",
    )?;

    let insert_start = Instant::now();
    {
        let tx = conn.unchecked_transaction()?;
        for row in 1..=ROWS {
            let tx_id = format!("node:{row}");
            let row_id = format!("todo:{row}");
            insert_text_tx(&tx, &tx_id, row, Some(row))?;
            insert_text_history(
                &tx,
                &row_id,
                &tx_id,
                "insert",
                &title(row),
                row % 3 == 0,
                row,
            )?;
            upsert_text_current(&tx, &row_id, &tx_id, &title(row), row % 3 == 0, row, row)?;
        }
        for row in 1..=UPDATES {
            let epoch = ROWS + row;
            let tx_id = format!("node:{epoch}");
            let row_id = format!("todo:{row}");
            let title = format!("updated {row}");
            insert_text_tx(&tx, &tx_id, epoch, Some(epoch))?;
            insert_text_history(&tx, &row_id, &tx_id, "update", &title, row % 3 == 0, row)?;
            upsert_text_current(&tx, &row_id, &tx_id, &title, row % 3 == 0, row, epoch)?;
        }
        tx.commit()?;
    }
    let insert = insert_start.elapsed();
    let current_query = repeat_query(&conn, text_current_sql(), &[])?;
    let snapshot_query = repeat_query(&conn, text_snapshot_sql(), &[&ROWS])?;
    finish(
        "text_system",
        page_size,
        file.path(),
        &conn,
        insert,
        current_query,
        snapshot_query,
    )
}

fn run_compact_system(page_size: i64) -> Result<Run, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path(), page_size)?;
    conn.execute_batch(
        "
        CREATE TABLE tx (
          tx_num INTEGER PRIMARY KEY,
          node_num INTEGER NOT NULL,
          local_epoch INTEGER NOT NULL,
          global_epoch INTEGER,
          kind INTEGER NOT NULL,
          status INTEGER NOT NULL,
          metadata_json TEXT NOT NULL
        );
        CREATE TABLE todo_h (
          row_num INTEGER NOT NULL,
          branch_num INTEGER NOT NULL,
          tx_num INTEGER NOT NULL,
          op INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          project_num INTEGER NOT NULL,
          conflicts_json TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (row_num, branch_num, tx_num)
        );
        CREATE INDEX todo_h_tx ON todo_h(branch_num, tx_num, row_num);
        CREATE INDEX todo_h_row_updated
          ON todo_h(branch_num, row_num, updated_at DESC, tx_num);
        CREATE INDEX todo_h_visible_order
          ON todo_h(branch_num, done, created_at DESC, row_num, tx_num);
        CREATE TABLE todo_c (
          row_num INTEGER NOT NULL,
          branch_num INTEGER NOT NULL,
          visible_tx_num INTEGER NOT NULL,
          is_deleted INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          project_num INTEGER NOT NULL,
          conflicts_json TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (row_num, branch_num)
        );
        CREATE INDEX todo_c_done_created
          ON todo_c(branch_num, done, created_at DESC);
        CREATE TABLE tx_write (
          tx_num INTEGER NOT NULL,
          table_num INTEGER NOT NULL,
          row_num INTEGER NOT NULL,
          PRIMARY KEY (tx_num, table_num, row_num)
        );
        ",
    )?;

    let insert_start = Instant::now();
    {
        let tx = conn.unchecked_transaction()?;
        for row in 1..=ROWS {
            insert_compact_tx(&tx, row, Some(row))?;
            insert_compact_history(&tx, row, row, 0, &title(row), row % 3 == 0, row)?;
            upsert_compact_current(&tx, row, row, &title(row), row % 3 == 0, row, row)?;
        }
        for row in 1..=UPDATES {
            let epoch = ROWS + row;
            let title = format!("updated {row}");
            insert_compact_tx(&tx, epoch, Some(epoch))?;
            insert_compact_history(&tx, row, epoch, 1, &title, row % 3 == 0, row)?;
            upsert_compact_current(&tx, row, epoch, &title, row % 3 == 0, row, epoch)?;
        }
        tx.commit()?;
    }
    let insert = insert_start.elapsed();
    let current_query = repeat_query(&conn, compact_current_sql(), &[])?;
    let snapshot_query = repeat_query(&conn, compact_snapshot_sql(), &[&ROWS])?;
    finish(
        "compact_system",
        page_size,
        file.path(),
        &conn,
        insert,
        current_query,
        snapshot_query,
    )
}

fn open(path: &Path, page_size: i64) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "page_size", page_size)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = MEMORY;
        ",
    )?;
    Ok(conn)
}

fn finish(
    layout: &'static str,
    page_size: i64,
    path: &Path,
    conn: &Connection,
    insert: Duration,
    current_query: Duration,
    snapshot_query: Duration,
) -> Result<Run, Box<dyn std::error::Error>> {
    conn.execute_batch("VACUUM")?;
    let page_count = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    Ok(Run {
        layout,
        page_size,
        file_bytes: path.metadata()?.len(),
        page_count,
        gzip_bytes: compressed_size("gzip", path)?,
        zstd_bytes: compressed_size("zstd", path)?,
        insert,
        current_query,
        snapshot_query,
    })
}

fn compressed_size(program: &str, path: &Path) -> Result<u64, Box<dyn std::error::Error>> {
    let output = match program {
        "gzip" => Command::new("gzip").arg("-c").arg(path).output()?,
        "zstd" => Command::new("zstd")
            .args(["-q", "-c", "-3"])
            .arg(path)
            .output()?,
        _ => unreachable!(),
    };
    if !output.status.success() {
        return Err(format!("{program} failed").into());
    }
    Ok(output.stdout.len() as u64)
}

fn repeat_query(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::ToSql],
) -> rusqlite::Result<Duration> {
    let start = Instant::now();
    for _ in 0..QUERY_REPS {
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        let mut count = 0;
        while let Some(row) = rows.next()? {
            let _: String = row.get(1)?;
            count += 1;
        }
        assert_eq!(count, 50);
    }
    Ok(start.elapsed())
}

fn current_sql(table: &str) -> String {
    format!(
        "
        SELECT rowid, title
        FROM {table}
        WHERE done = 0
        ORDER BY created_at DESC
        LIMIT 50
        "
    )
}

fn text_current_sql() -> &'static str {
    "
    SELECT j_row_id, title
    FROM todo_current
    WHERE j_branch_id = 'main' AND done = 0
    ORDER BY j_created_at DESC
    LIMIT 50
    "
}

fn compact_current_sql() -> &'static str {
    "
    SELECT row_num, title
    FROM todo_c
    WHERE branch_num = 1 AND done = 0
    ORDER BY created_at DESC
    LIMIT 50
    "
}

fn text_snapshot_sql() -> &'static str {
    "
    SELECT h.j_row_id, h.title
    FROM todo_history h
    JOIN jazz_tx tx ON tx.tx_id = h.j_tx_id
    WHERE h.j_branch_id = 'main'
      AND h.done = 0
      AND tx.status = 'global_durable_accepted'
      AND tx.global_epoch <= ?1
      AND NOT EXISTS (
        SELECT 1
        FROM todo_history newer
        JOIN jazz_tx newer_tx ON newer_tx.tx_id = newer.j_tx_id
        WHERE newer.j_branch_id = h.j_branch_id
          AND newer.j_row_id = h.j_row_id
          AND newer_tx.status = 'global_durable_accepted'
          AND newer_tx.global_epoch <= ?1
          AND (newer_tx.global_epoch, newer.j_tx_id) > (tx.global_epoch, h.j_tx_id)
      )
    ORDER BY h.j_created_at DESC
    LIMIT 50
    "
}

fn compact_snapshot_sql() -> &'static str {
    "
    SELECT h.row_num, h.title
    FROM todo_h h
    JOIN tx ON tx.tx_num = h.tx_num
    WHERE h.branch_num = 1
      AND h.done = 0
      AND tx.status = 2
      AND tx.global_epoch <= ?1
      AND NOT EXISTS (
        SELECT 1
        FROM todo_h newer
        JOIN tx newer_tx ON newer_tx.tx_num = newer.tx_num
        WHERE newer.branch_num = h.branch_num
          AND newer.row_num = h.row_num
          AND newer_tx.status = 2
          AND newer_tx.global_epoch <= ?1
          AND (newer_tx.global_epoch, newer.tx_num) > (tx.global_epoch, h.tx_num)
      )
    ORDER BY h.created_at DESC
    LIMIT 50
    "
}

fn insert_text_tx(
    tx: &rusqlite::Transaction<'_>,
    tx_id: &str,
    epoch: i64,
    global_epoch: Option<i64>,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT INTO jazz_tx
          (tx_id, node_id, local_epoch, global_epoch, kind, status, metadata_json)
        VALUES (?1, 'node', ?2, ?3, 'data', 'global_durable_accepted', '{}')
        ",
        params![tx_id, epoch, global_epoch],
    )?;
    tx.execute(
        "INSERT INTO jazz_tx_write (tx_id, table_name, row_id) VALUES (?1, 'todos', ?2)",
        params![tx_id, format!("todo:{epoch}")],
    )?;
    Ok(())
}

fn insert_text_history(
    tx: &rusqlite::Transaction<'_>,
    row_id: &str,
    tx_id: &str,
    op: &str,
    title: &str,
    done: bool,
    row: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT INTO todo_history
          (j_row_id, j_branch_id, j_tx_id, j_op, title, done, project_id,
           j_conflicts_json, j_created_at, j_updated_at)
        VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6, '{}', ?7, ?8)
        ",
        params![
            row_id,
            tx_id,
            op,
            title,
            done,
            format!("project:{}", row % 1_000),
            row,
            row
        ],
    )?;
    Ok(())
}

fn upsert_text_current(
    tx: &rusqlite::Transaction<'_>,
    row_id: &str,
    tx_id: &str,
    title: &str,
    done: bool,
    created_at: i64,
    updated_at: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT OR REPLACE INTO todo_current
          (j_row_id, j_branch_id, j_visible_tx_id, j_is_deleted, title, done,
           project_id, j_conflicts_json, j_created_at, j_updated_at)
        VALUES (?1, 'main', ?2, 0, ?3, ?4, ?5, '{}', ?6, ?7)
        ",
        params![
            row_id,
            tx_id,
            title,
            done,
            "project:1",
            created_at,
            updated_at
        ],
    )?;
    Ok(())
}

fn insert_compact_tx(
    tx: &rusqlite::Transaction<'_>,
    tx_num: i64,
    global_epoch: Option<i64>,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT INTO tx
          (tx_num, node_num, local_epoch, global_epoch, kind, status, metadata_json)
        VALUES (?1, 1, ?1, ?2, 1, 2, '{}')
        ",
        params![tx_num, global_epoch],
    )?;
    tx.execute(
        "INSERT INTO tx_write (tx_num, table_num, row_num) VALUES (?1, 1, ?1)",
        params![tx_num],
    )?;
    Ok(())
}

fn insert_compact_history(
    tx: &rusqlite::Transaction<'_>,
    row_num: i64,
    tx_num: i64,
    op: i64,
    title: &str,
    done: bool,
    created_at: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT INTO todo_h
          (row_num, branch_num, tx_num, op, title, done, project_num,
           conflicts_json, created_at, updated_at)
        VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, '{}', ?7, ?2)
        ",
        params![
            row_num,
            tx_num,
            op,
            title,
            done,
            row_num % 1_000,
            created_at
        ],
    )?;
    Ok(())
}

fn upsert_compact_current(
    tx: &rusqlite::Transaction<'_>,
    row_num: i64,
    tx_num: i64,
    title: &str,
    done: bool,
    created_at: i64,
    updated_at: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT OR REPLACE INTO todo_c
          (row_num, branch_num, visible_tx_num, is_deleted, title, done,
           project_num, conflicts_json, created_at, updated_at)
        VALUES (?1, 1, ?2, 0, ?3, ?4, ?5, '{}', ?6, ?7)
        ",
        params![
            row_num,
            tx_num,
            title,
            done,
            row_num % 1_000,
            created_at,
            updated_at
        ],
    )?;
    Ok(())
}

fn title(row: i64) -> String {
    format!("todo title {row}")
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
