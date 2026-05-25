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

    let text = run_text_everywhere()?;
    let interned = run_interned_ids()?;

    print_run(&text);
    print_run(&interned);

    println!();
    println!(
        "interned/text: file={:.2}x insert={:.2}x current={:.2}x snapshot={:.2}x public_lookup={:.2}x",
        interned.file_bytes as f64 / text.file_bytes as f64,
        interned.insert.as_secs_f64() / text.insert.as_secs_f64(),
        interned.current.as_secs_f64() / text.current.as_secs_f64(),
        interned.snapshot.as_secs_f64() / text.snapshot.as_secs_f64(),
        interned.public_lookup.as_secs_f64() / text.public_lookup.as_secs_f64(),
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

fn run_text_everywhere() -> Result<Run, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    conn.execute_batch(
        "
        CREATE TABLE tx (
          tx_id TEXT PRIMARY KEY,
          global_epoch INTEGER NOT NULL,
          status INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE h (
          row_id TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          tx_id TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (row_id, branch_id, tx_id)
        ) WITHOUT ROWID;
        CREATE INDEX h_visible ON h(branch_id, done, created_at DESC, row_id, tx_id, title);
        CREATE TABLE c (
          row_id TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          visible_tx_id TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (row_id, branch_id)
        ) WITHOUT ROWID;
        CREATE INDEX c_visible ON c(branch_id, done, created_at DESC, row_id, title);
        ",
    )?;

    let insert_start = Instant::now();
    {
        let tx = conn.unchecked_transaction()?;
        for row in 1..=ROWS {
            let tx_id = tx_id(row);
            tx.execute(
                "INSERT INTO tx (tx_id, global_epoch, status) VALUES (?1, ?2, 2)",
                params![tx_id, row],
            )?;
            insert_text_history(&tx, row, row)?;
            upsert_text_current(&tx, row, row)?;
        }
        for row in 1..=UPDATES {
            let tx_num = ROWS + row;
            let tx_id = tx_id(tx_num);
            tx.execute(
                "INSERT INTO tx (tx_id, global_epoch, status) VALUES (?1, ?2, 2)",
                params![tx_id, tx_num],
            )?;
            insert_text_history(&tx, row, tx_num)?;
            upsert_text_current(&tx, row, tx_num)?;
        }
        tx.commit()?;
    }
    let insert = insert_start.elapsed();
    finish(
        "text_everywhere",
        file.path(),
        &conn,
        insert,
        repeat_scan(&conn, text_current_sql(), &[])?,
        repeat_scan(&conn, text_snapshot_sql(), &[&ROWS])?,
        repeat_public_lookup_text(&conn)?,
    )
}

fn run_interned_ids() -> Result<Run, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    conn.execute_batch(
        "
        CREATE TABLE row_id_map (
          row_num INTEGER PRIMARY KEY,
          row_id TEXT NOT NULL UNIQUE
        );
        CREATE TABLE tx_id_map (
          tx_num INTEGER PRIMARY KEY,
          tx_id TEXT NOT NULL UNIQUE
        );
        CREATE TABLE tx (
          tx_num INTEGER PRIMARY KEY,
          global_epoch INTEGER NOT NULL,
          status INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE h (
          row_num INTEGER NOT NULL,
          branch_num INTEGER NOT NULL,
          tx_num INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (row_num, branch_num, tx_num)
        ) WITHOUT ROWID;
        CREATE INDEX h_visible ON h(branch_num, done, created_at DESC, row_num, tx_num, title);
        CREATE TABLE c (
          row_num INTEGER NOT NULL,
          branch_num INTEGER NOT NULL,
          visible_tx_num INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (row_num, branch_num)
        ) WITHOUT ROWID;
        CREATE INDEX c_visible ON c(branch_num, done, created_at DESC, row_num, title);
        ",
    )?;

    let insert_start = Instant::now();
    {
        let tx = conn.unchecked_transaction()?;
        for row in 1..=ROWS {
            tx.execute(
                "INSERT INTO row_id_map (row_num, row_id) VALUES (?1, ?2)",
                params![row, row_id(row)],
            )?;
            tx.execute(
                "INSERT INTO tx_id_map (tx_num, tx_id) VALUES (?1, ?2)",
                params![row, tx_id(row)],
            )?;
            tx.execute(
                "INSERT INTO tx (tx_num, global_epoch, status) VALUES (?1, ?1, 2)",
                params![row],
            )?;
            insert_interned_history(&tx, row, row)?;
            upsert_interned_current(&tx, row, row)?;
        }
        for row in 1..=UPDATES {
            let tx_num = ROWS + row;
            tx.execute(
                "INSERT INTO tx_id_map (tx_num, tx_id) VALUES (?1, ?2)",
                params![tx_num, tx_id(tx_num)],
            )?;
            tx.execute(
                "INSERT INTO tx (tx_num, global_epoch, status) VALUES (?1, ?1, 2)",
                params![tx_num],
            )?;
            insert_interned_history(&tx, row, tx_num)?;
            upsert_interned_current(&tx, row, tx_num)?;
        }
        tx.commit()?;
    }
    let insert = insert_start.elapsed();
    finish(
        "interned_ids",
        file.path(),
        &conn,
        insert,
        repeat_scan(&conn, interned_current_sql(), &[])?,
        repeat_scan(&conn, interned_snapshot_sql(), &[&ROWS])?,
        repeat_public_lookup_interned(&conn)?,
    )
}

fn insert_text_history(
    tx: &rusqlite::Transaction<'_>,
    row_num: i64,
    tx_num: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT INTO h (row_id, branch_id, tx_id, title, done, created_at, updated_at)
        VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6)
        ",
        params![
            row_id(row_num),
            tx_id(tx_num),
            title(row_num, tx_num),
            row_num % 3 == 0,
            row_num,
            tx_num
        ],
    )?;
    Ok(())
}

fn upsert_text_current(
    tx: &rusqlite::Transaction<'_>,
    row_num: i64,
    tx_num: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT OR REPLACE INTO c
          (row_id, branch_id, visible_tx_id, title, done, created_at, updated_at)
        VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6)
        ",
        params![
            row_id(row_num),
            tx_id(tx_num),
            title(row_num, tx_num),
            row_num % 3 == 0,
            row_num,
            tx_num
        ],
    )?;
    Ok(())
}

fn insert_interned_history(
    tx: &rusqlite::Transaction<'_>,
    row_num: i64,
    tx_num: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT INTO h (row_num, branch_num, tx_num, title, done, created_at, updated_at)
        VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6)
        ",
        params![
            row_num,
            tx_num,
            title(row_num, tx_num),
            row_num % 3 == 0,
            row_num,
            tx_num
        ],
    )?;
    Ok(())
}

fn upsert_interned_current(
    tx: &rusqlite::Transaction<'_>,
    row_num: i64,
    tx_num: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT OR REPLACE INTO c
          (row_num, branch_num, visible_tx_num, title, done, created_at, updated_at)
        VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6)
        ",
        params![
            row_num,
            tx_num,
            title(row_num, tx_num),
            row_num % 3 == 0,
            row_num,
            tx_num
        ],
    )?;
    Ok(())
}

fn repeat_public_lookup_text(conn: &Connection) -> rusqlite::Result<Duration> {
    let start = Instant::now();
    for idx in lookup_rows() {
        let _: String = conn.query_row(
            "SELECT title FROM c WHERE branch_id = 'main' AND row_id = ?1",
            params![row_id(idx)],
            |row| row.get(0),
        )?;
    }
    Ok(start.elapsed())
}

fn repeat_public_lookup_interned(conn: &Connection) -> rusqlite::Result<Duration> {
    let start = Instant::now();
    for idx in lookup_rows() {
        let _: String = conn.query_row(
            "
            SELECT c.title
            FROM row_id_map row_id
            JOIN c ON c.row_num = row_id.row_num
            WHERE c.branch_num = 1 AND row_id.row_id = ?1
            ",
            params![row_id(idx)],
            |row| row.get(0),
        )?;
    }
    Ok(start.elapsed())
}

fn lookup_rows() -> impl Iterator<Item = i64> {
    (0..QUERY_REPS).map(|idx| ((idx as i64 * 7919) % ROWS) + 1)
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

fn text_current_sql() -> &'static str {
    "
    SELECT row_id, title
    FROM c
    WHERE branch_id = 'main' AND done = 0
    ORDER BY created_at DESC
    LIMIT 50
    "
}

fn interned_current_sql() -> &'static str {
    "
    SELECT row_num, title
    FROM c
    WHERE branch_num = 1 AND done = 0
    ORDER BY created_at DESC
    LIMIT 50
    "
}

fn text_snapshot_sql() -> &'static str {
    "
    SELECT h.row_id, h.title
    FROM h JOIN tx ON tx.tx_id = h.tx_id
    WHERE h.branch_id = 'main'
      AND h.done = 0
      AND tx.status = 2
      AND tx.global_epoch <= ?1
      AND NOT EXISTS (
        SELECT 1
        FROM h newer JOIN tx newer_tx ON newer_tx.tx_id = newer.tx_id
        WHERE newer.branch_id = h.branch_id
          AND newer.row_id = h.row_id
          AND newer_tx.status = 2
          AND newer_tx.global_epoch <= ?1
          AND (newer_tx.global_epoch, newer.tx_id) > (tx.global_epoch, h.tx_id)
      )
    ORDER BY h.created_at DESC
    LIMIT 50
    "
}

fn interned_snapshot_sql() -> &'static str {
    "
    SELECT h.row_num, h.title
    FROM h JOIN tx ON tx.tx_num = h.tx_num
    WHERE h.branch_num = 1
      AND h.done = 0
      AND tx.status = 2
      AND tx.global_epoch <= ?1
      AND NOT EXISTS (
        SELECT 1
        FROM h newer JOIN tx newer_tx ON newer_tx.tx_num = newer.tx_num
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

fn row_id(row_num: i64) -> String {
    format!("co_z4v7s9abcedfghijklmno/todos/row_{row_num:012x}")
}

fn tx_id(tx_num: i64) -> String {
    format!("tx_z4v7s9abcedfghijklmno/node_alpha/{tx_num:012x}")
}

fn title(row_num: i64, tx_num: i64) -> String {
    format!("todo {row_num} at {tx_num}")
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
