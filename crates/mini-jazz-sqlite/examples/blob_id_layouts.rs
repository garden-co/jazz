use rusqlite::{params, Connection};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const ROWS: i64 = 40_000;
const UPDATES: i64 = 8_000;
const QUERY_REPS: usize = 200;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("rows={ROWS}, updates={UPDATES}, query_reps={QUERY_REPS}");
    println!("layout,file_bytes,page_count,insert_ms,current_ms,snapshot_ms,public_lookup_ms");

    let text = run_text_ids()?;
    let blob = run_blob_ids()?;

    print_run(&text);
    print_run(&blob);

    println!();
    println!(
        "blob/text: file={:.2}x insert={:.2}x current={:.2}x snapshot={:.2}x lookup={:.2}x",
        blob.file_bytes as f64 / text.file_bytes as f64,
        blob.insert.as_secs_f64() / text.insert.as_secs_f64(),
        blob.current.as_secs_f64() / text.current.as_secs_f64(),
        blob.snapshot.as_secs_f64() / text.snapshot.as_secs_f64(),
        blob.public_lookup.as_secs_f64() / text.public_lookup.as_secs_f64(),
    );

    Ok(())
}

struct Run {
    layout: &'static str,
    file_bytes: u64,
    page_count: i64,
    insert: Duration,
    current: Duration,
    snapshot: Duration,
    public_lookup: Duration,
}

fn print_run(run: &Run) {
    println!(
        "{},{},{},{:.3},{:.3},{:.3},{:.3}",
        run.layout,
        run.file_bytes,
        run.page_count,
        ms(run.insert),
        ms(run.current),
        ms(run.snapshot),
        ms(run.public_lookup)
    );
}

fn run_text_ids() -> Result<Run, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    conn.execute_batch(&schema("TEXT", "TEXT", "'main'"))?;
    let insert = populate_text(&conn)?;
    finish(
        "long_text_ids",
        file.path(),
        &conn,
        insert,
        repeat_scan(&conn, text_current_sql(), &[])?,
        repeat_scan(&conn, text_snapshot_sql(), &[&ROWS])?,
        repeat_text_lookup(&conn)?,
    )
}

fn run_blob_ids() -> Result<Run, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    conn.execute_batch(&schema(
        "BLOB",
        "BLOB",
        "x'00000000000000000000000000000001'",
    ))?;
    let insert = populate_blob(&conn)?;
    finish(
        "blob16_ids",
        file.path(),
        &conn,
        insert,
        repeat_scan(&conn, blob_current_sql(), &[])?,
        repeat_scan(&conn, blob_snapshot_sql(), &[&ROWS])?,
        repeat_blob_lookup(&conn)?,
    )
}

fn schema(id_type: &str, branch_type: &str, _branch_literal: &str) -> String {
    format!(
        "
        CREATE TABLE tx (
          tx_id {id_type} PRIMARY KEY,
          global_epoch INTEGER NOT NULL,
          status INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE h (
          row_id {id_type} NOT NULL,
          branch_id {branch_type} NOT NULL,
          tx_id {id_type} NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (row_id, branch_id, tx_id)
        ) WITHOUT ROWID;
        CREATE INDEX h_visible ON h(branch_id, done, created_at DESC, row_id, tx_id, title);
        CREATE TABLE c (
          row_id {id_type} NOT NULL,
          branch_id {branch_type} NOT NULL,
          visible_tx_id {id_type} NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (row_id, branch_id)
        ) WITHOUT ROWID;
        CREATE INDEX c_visible ON c(branch_id, done, created_at DESC, row_id, title);
        "
    )
}

fn populate_text(conn: &Connection) -> rusqlite::Result<Duration> {
    let start = Instant::now();
    let tx = conn.unchecked_transaction()?;
    for row in 1..=ROWS {
        tx.execute(
            "INSERT INTO tx (tx_id, global_epoch, status) VALUES (?1, ?2, 2)",
            params![text_tx_id(row), row],
        )?;
        insert_text_version(&tx, row, row)?;
    }
    for row in 1..=UPDATES {
        let tx_num = ROWS + row;
        tx.execute(
            "INSERT INTO tx (tx_id, global_epoch, status) VALUES (?1, ?2, 2)",
            params![text_tx_id(tx_num), tx_num],
        )?;
        insert_text_version(&tx, row, tx_num)?;
    }
    tx.commit()?;
    Ok(start.elapsed())
}

fn populate_blob(conn: &Connection) -> rusqlite::Result<Duration> {
    let start = Instant::now();
    let tx = conn.unchecked_transaction()?;
    for row in 1..=ROWS {
        tx.execute(
            "INSERT INTO tx (tx_id, global_epoch, status) VALUES (?1, ?2, 2)",
            params![blob_id(2, row), row],
        )?;
        insert_blob_version(&tx, row, row)?;
    }
    for row in 1..=UPDATES {
        let tx_num = ROWS + row;
        tx.execute(
            "INSERT INTO tx (tx_id, global_epoch, status) VALUES (?1, ?2, 2)",
            params![blob_id(2, tx_num), tx_num],
        )?;
        insert_blob_version(&tx, row, tx_num)?;
    }
    tx.commit()?;
    Ok(start.elapsed())
}

fn insert_text_version(
    tx: &rusqlite::Transaction<'_>,
    row: i64,
    tx_num: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT INTO h (row_id, branch_id, tx_id, title, done, created_at, updated_at)
        VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6)
        ",
        params![
            text_row_id(row),
            text_tx_id(tx_num),
            title(row, tx_num),
            row % 3 == 0,
            row,
            tx_num
        ],
    )?;
    tx.execute(
        "
        INSERT OR REPLACE INTO c
          (row_id, branch_id, visible_tx_id, title, done, created_at, updated_at)
        VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6)
        ",
        params![
            text_row_id(row),
            text_tx_id(tx_num),
            title(row, tx_num),
            row % 3 == 0,
            row,
            tx_num
        ],
    )?;
    Ok(())
}

fn insert_blob_version(
    tx: &rusqlite::Transaction<'_>,
    row: i64,
    tx_num: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT INTO h (row_id, branch_id, tx_id, title, done, created_at, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ",
        params![
            blob_id(1, row),
            blob_id(3, 1),
            blob_id(2, tx_num),
            title(row, tx_num),
            row % 3 == 0,
            row,
            tx_num
        ],
    )?;
    tx.execute(
        "
        INSERT OR REPLACE INTO c
          (row_id, branch_id, visible_tx_id, title, done, created_at, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ",
        params![
            blob_id(1, row),
            blob_id(3, 1),
            blob_id(2, tx_num),
            title(row, tx_num),
            row % 3 == 0,
            row,
            tx_num
        ],
    )?;
    Ok(())
}

fn repeat_text_lookup(conn: &Connection) -> rusqlite::Result<Duration> {
    let start = Instant::now();
    for row in lookup_rows() {
        let _: String = conn.query_row(
            "SELECT title FROM c WHERE branch_id = 'main' AND row_id = ?1",
            params![text_row_id(row)],
            |row| row.get(0),
        )?;
    }
    Ok(start.elapsed())
}

fn repeat_blob_lookup(conn: &Connection) -> rusqlite::Result<Duration> {
    let start = Instant::now();
    for row in lookup_rows() {
        let _: String = conn.query_row(
            "SELECT title FROM c WHERE branch_id = ?1 AND row_id = ?2",
            params![blob_id(3, 1), blob_id(1, row)],
            |row| row.get(0),
        )?;
    }
    Ok(start.elapsed())
}

fn lookup_rows() -> impl Iterator<Item = i64> {
    (0..QUERY_REPS).map(|idx| ((idx as i64 * 7919) % ROWS) + 1)
}

fn text_current_sql() -> &'static str {
    "
    SELECT row_id, title FROM c
    WHERE branch_id = 'main' AND done = 0
    ORDER BY created_at DESC LIMIT 50
    "
}

fn blob_current_sql() -> &'static str {
    "
    SELECT row_id, title FROM c
    WHERE branch_id = x'03000000000000000000000000000001' AND done = 0
    ORDER BY created_at DESC LIMIT 50
    "
}

fn text_snapshot_sql() -> &'static str {
    "
    SELECT h.row_id, h.title
    FROM h JOIN tx ON tx.tx_id = h.tx_id
    WHERE h.branch_id = 'main' AND h.done = 0 AND tx.status = 2 AND tx.global_epoch <= ?1
      AND NOT EXISTS (
        SELECT 1 FROM h newer JOIN tx newer_tx ON newer_tx.tx_id = newer.tx_id
        WHERE newer.branch_id = h.branch_id
          AND newer.row_id = h.row_id
          AND newer_tx.status = 2
          AND newer_tx.global_epoch <= ?1
          AND (newer_tx.global_epoch, newer.tx_id) > (tx.global_epoch, h.tx_id)
      )
    ORDER BY h.created_at DESC LIMIT 50
    "
}

fn blob_snapshot_sql() -> &'static str {
    "
    SELECT h.row_id, h.title
    FROM h JOIN tx ON tx.tx_id = h.tx_id
    WHERE h.branch_id = x'03000000000000000000000000000001'
      AND h.done = 0 AND tx.status = 2 AND tx.global_epoch <= ?1
      AND NOT EXISTS (
        SELECT 1 FROM h newer JOIN tx newer_tx ON newer_tx.tx_id = newer.tx_id
        WHERE newer.branch_id = h.branch_id
          AND newer.row_id = h.row_id
          AND newer_tx.status = 2
          AND newer_tx.global_epoch <= ?1
          AND (newer_tx.global_epoch, newer.tx_id) > (tx.global_epoch, h.tx_id)
      )
    ORDER BY h.created_at DESC LIMIT 50
    "
}

fn repeat_scan(
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

fn finish(
    layout: &'static str,
    path: &Path,
    conn: &Connection,
    insert: Duration,
    current: Duration,
    snapshot: Duration,
    public_lookup: Duration,
) -> Result<Run, Box<dyn std::error::Error>> {
    conn.execute_batch("VACUUM")?;
    let page_count = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    Ok(Run {
        layout,
        file_bytes: path.metadata()?.len(),
        page_count,
        insert,
        current,
        snapshot,
        public_lookup,
    })
}

fn open(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = MEMORY;
        ",
    )?;
    Ok(conn)
}

fn blob_id(kind: u8, num: i64) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[0] = kind;
    bytes[8..16].copy_from_slice(&num.to_be_bytes());
    bytes
}

fn text_row_id(num: i64) -> String {
    format!("co_z4v7s9abcedfghijklmno/todos/row_{num:012x}")
}

fn text_tx_id(num: i64) -> String {
    format!("tx_z4v7s9abcedfghijklmno/node_alpha/{num:012x}")
}

fn title(row: i64, tx_num: i64) -> String {
    format!("todo {row} at {tx_num}")
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
