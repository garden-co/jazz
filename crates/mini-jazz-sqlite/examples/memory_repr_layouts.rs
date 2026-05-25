use rusqlite::{ffi, params, Connection};
use std::mem::size_of;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const ROWS: i64 = 40_000;
const UPDATES: i64 = 8_000;
const QUERY_REPS: usize = 200;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("rows={ROWS}, updates={UPDATES}, query_reps={QUERY_REPS}");
    println!();
    print_rust_size_estimates();
    println!();
    println!("layout,file_bytes,sqlite_mem_after_load,sqlite_mem_peak,db_cache_used,current_ms,snapshot_ms");

    let text = run_layout(Layout::Text)?;
    let blob = run_layout(Layout::Blob16)?;
    let int = run_layout(Layout::InternedInt)?;

    print_run(&text);
    print_run(&blob);
    print_run(&int);

    println!();
    for run in [&blob, &int] {
        println!(
            "{}/text: file={:.2}x sqlite_mem_after_load={:.2}x sqlite_mem_peak={:.2}x db_cache={:.2}x current={:.2}x snapshot={:.2}x",
            run.layout,
            run.file_bytes as f64 / text.file_bytes as f64,
            run.sqlite_mem_after_load as f64 / text.sqlite_mem_after_load as f64,
            run.sqlite_mem_peak as f64 / text.sqlite_mem_peak as f64,
            run.db_cache_used as f64 / text.db_cache_used as f64,
            run.current.as_secs_f64() / text.current.as_secs_f64(),
            run.snapshot.as_secs_f64() / text.snapshot.as_secs_f64(),
        );
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum Layout {
    Text,
    Blob16,
    InternedInt,
}

struct Run {
    layout: &'static str,
    file_bytes: u64,
    sqlite_mem_after_load: i64,
    sqlite_mem_peak: i64,
    db_cache_used: i64,
    current: Duration,
    snapshot: Duration,
}

fn print_rust_size_estimates() {
    println!("rust-side rough key sizes:");
    println!(
        "String struct: {} bytes plus heap bytes",
        size_of::<String>()
    );
    println!(
        "Vec<u8> struct: {} bytes plus heap bytes",
        size_of::<Vec<u8>>()
    );
    println!("[u8; 16]: {} bytes inline", size_of::<[u8; 16]>());
    println!("i64/u64: {} bytes inline", size_of::<i64>());
    println!(
        "BTreeMap key effect: text/blob heap allocations per key unless interned; integer/fixed blob keys can be inline in compact structs"
    );
}

fn print_run(run: &Run) {
    println!(
        "{},{},{},{},{},{:.3},{:.3}",
        run.layout,
        run.file_bytes,
        run.sqlite_mem_after_load,
        run.sqlite_mem_peak,
        run.db_cache_used,
        ms(run.current),
        ms(run.snapshot)
    );
}

fn run_layout(layout: Layout) -> Result<Run, Box<dyn std::error::Error>> {
    unsafe {
        let mut current = 0;
        let mut highwater = 0;
        ffi::sqlite3_status64(
            ffi::SQLITE_STATUS_MEMORY_USED,
            &mut current,
            &mut highwater,
            1,
        );
    }

    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    create_schema(&conn, layout)?;
    populate(&conn, layout)?;
    conn.execute_batch("VACUUM")?;

    let sqlite_mem_after_load = sqlite_memory_used();
    let current = repeat_scan(&conn, current_sql(layout), &[])?;
    let snapshot = repeat_scan(&conn, snapshot_sql(layout), &[&ROWS])?;
    let sqlite_mem_peak = sqlite_memory_highwater();
    let db_cache_used = db_status(&conn, ffi::SQLITE_DBSTATUS_CACHE_USED)?;

    Ok(Run {
        layout: layout.name(),
        file_bytes: file.path().metadata()?.len(),
        sqlite_mem_after_load,
        sqlite_mem_peak,
        db_cache_used,
        current,
        snapshot,
    })
}

fn create_schema(conn: &Connection, layout: Layout) -> rusqlite::Result<()> {
    match layout {
        Layout::Text => conn.execute_batch(
            "
            CREATE TABLE tx (tx_id TEXT PRIMARY KEY, global_epoch INTEGER NOT NULL, status INTEGER NOT NULL) WITHOUT ROWID;
            CREATE TABLE h (
              row_id TEXT NOT NULL, branch_id TEXT NOT NULL, tx_id TEXT NOT NULL,
              title TEXT NOT NULL, done INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
              PRIMARY KEY (row_id, branch_id, tx_id)
            ) WITHOUT ROWID;
            CREATE INDEX h_visible ON h(branch_id, done, created_at DESC, row_id, tx_id, title);
            CREATE TABLE c (
              row_id TEXT NOT NULL, branch_id TEXT NOT NULL, visible_tx_id TEXT NOT NULL,
              title TEXT NOT NULL, done INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
              PRIMARY KEY (row_id, branch_id)
            ) WITHOUT ROWID;
            CREATE INDEX c_visible ON c(branch_id, done, created_at DESC, row_id, title);
            ",
        ),
        Layout::Blob16 => conn.execute_batch(
            "
            CREATE TABLE tx (tx_id BLOB PRIMARY KEY, global_epoch INTEGER NOT NULL, status INTEGER NOT NULL) WITHOUT ROWID;
            CREATE TABLE h (
              row_id BLOB NOT NULL, branch_id BLOB NOT NULL, tx_id BLOB NOT NULL,
              title TEXT NOT NULL, done INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
              PRIMARY KEY (row_id, branch_id, tx_id)
            ) WITHOUT ROWID;
            CREATE INDEX h_visible ON h(branch_id, done, created_at DESC, row_id, tx_id, title);
            CREATE TABLE c (
              row_id BLOB NOT NULL, branch_id BLOB NOT NULL, visible_tx_id BLOB NOT NULL,
              title TEXT NOT NULL, done INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
              PRIMARY KEY (row_id, branch_id)
            ) WITHOUT ROWID;
            CREATE INDEX c_visible ON c(branch_id, done, created_at DESC, row_id, title);
            ",
        ),
        Layout::InternedInt => conn.execute_batch(
            "
            CREATE TABLE row_id_map (row_num INTEGER PRIMARY KEY, row_id TEXT NOT NULL UNIQUE);
            CREATE TABLE tx_id_map (tx_num INTEGER PRIMARY KEY, tx_id TEXT NOT NULL UNIQUE);
            CREATE TABLE tx (tx_num INTEGER PRIMARY KEY, global_epoch INTEGER NOT NULL, status INTEGER NOT NULL) WITHOUT ROWID;
            CREATE TABLE h (
              row_num INTEGER NOT NULL, branch_num INTEGER NOT NULL, tx_num INTEGER NOT NULL,
              title TEXT NOT NULL, done INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
              PRIMARY KEY (row_num, branch_num, tx_num)
            ) WITHOUT ROWID;
            CREATE INDEX h_visible ON h(branch_num, done, created_at DESC, row_num, tx_num, title);
            CREATE TABLE c (
              row_num INTEGER NOT NULL, branch_num INTEGER NOT NULL, visible_tx_num INTEGER NOT NULL,
              title TEXT NOT NULL, done INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
              PRIMARY KEY (row_num, branch_num)
            ) WITHOUT ROWID;
            CREATE INDEX c_visible ON c(branch_num, done, created_at DESC, row_num, title);
            ",
        ),
    }
}

fn populate(conn: &Connection, layout: Layout) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;
    for row in 1..=ROWS {
        insert_version(&tx, layout, row, row, true)?;
    }
    for row in 1..=UPDATES {
        insert_version(&tx, layout, row, ROWS + row, false)?;
    }
    tx.commit()
}

fn insert_version(
    tx: &rusqlite::Transaction<'_>,
    layout: Layout,
    row: i64,
    tx_num: i64,
    insert_public_row_id: bool,
) -> rusqlite::Result<()> {
    match layout {
        Layout::Text => {
            tx.execute(
                "INSERT INTO tx (tx_id, global_epoch, status) VALUES (?1, ?2, 2)",
                params![text_tx_id(tx_num), tx_num],
            )?;
            tx.execute(
                "INSERT INTO h (row_id, branch_id, tx_id, title, done, created_at, updated_at) VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6)",
                params![text_row_id(row), text_tx_id(tx_num), title(row, tx_num), row % 3 == 0, row, tx_num],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO c (row_id, branch_id, visible_tx_id, title, done, created_at, updated_at) VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6)",
                params![text_row_id(row), text_tx_id(tx_num), title(row, tx_num), row % 3 == 0, row, tx_num],
            )?;
        }
        Layout::Blob16 => {
            tx.execute(
                "INSERT INTO tx (tx_id, global_epoch, status) VALUES (?1, ?2, 2)",
                params![blob_id(2, tx_num), tx_num],
            )?;
            tx.execute(
                "INSERT INTO h (row_id, branch_id, tx_id, title, done, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![blob_id(1, row), blob_id(3, 1), blob_id(2, tx_num), title(row, tx_num), row % 3 == 0, row, tx_num],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO c (row_id, branch_id, visible_tx_id, title, done, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![blob_id(1, row), blob_id(3, 1), blob_id(2, tx_num), title(row, tx_num), row % 3 == 0, row, tx_num],
            )?;
        }
        Layout::InternedInt => {
            if insert_public_row_id {
                tx.execute(
                    "INSERT INTO row_id_map (row_num, row_id) VALUES (?1, ?2)",
                    params![row, text_row_id(row)],
                )?;
            }
            tx.execute(
                "INSERT INTO tx_id_map (tx_num, tx_id) VALUES (?1, ?2)",
                params![tx_num, text_tx_id(tx_num)],
            )?;
            tx.execute(
                "INSERT INTO tx (tx_num, global_epoch, status) VALUES (?1, ?2, 2)",
                params![tx_num, tx_num],
            )?;
            tx.execute(
                "INSERT INTO h (row_num, branch_num, tx_num, title, done, created_at, updated_at) VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6)",
                params![row, tx_num, title(row, tx_num), row % 3 == 0, row, tx_num],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO c (row_num, branch_num, visible_tx_num, title, done, created_at, updated_at) VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6)",
                params![row, tx_num, title(row, tx_num), row % 3 == 0, row, tx_num],
            )?;
        }
    }
    Ok(())
}

fn current_sql(layout: Layout) -> &'static str {
    match layout {
        Layout::Text => {
            "SELECT row_id, title FROM c WHERE branch_id = 'main' AND done = 0 ORDER BY created_at DESC LIMIT 50"
        }
        Layout::Blob16 => {
            "SELECT row_id, title FROM c WHERE branch_id = x'03000000000000000000000000000001' AND done = 0 ORDER BY created_at DESC LIMIT 50"
        }
        Layout::InternedInt => {
            "SELECT row_num, title FROM c WHERE branch_num = 1 AND done = 0 ORDER BY created_at DESC LIMIT 50"
        }
    }
}

fn snapshot_sql(layout: Layout) -> &'static str {
    match layout {
        Layout::Text => {
            "
            SELECT h.row_id, h.title FROM h JOIN tx ON tx.tx_id = h.tx_id
            WHERE h.branch_id = 'main' AND h.done = 0 AND tx.status = 2 AND tx.global_epoch <= ?1
              AND NOT EXISTS (
                SELECT 1 FROM h newer JOIN tx newer_tx ON newer_tx.tx_id = newer.tx_id
                WHERE newer.branch_id = h.branch_id AND newer.row_id = h.row_id
                  AND newer_tx.status = 2 AND newer_tx.global_epoch <= ?1
                  AND (newer_tx.global_epoch, newer.tx_id) > (tx.global_epoch, h.tx_id)
              )
            ORDER BY h.created_at DESC LIMIT 50
            "
        }
        Layout::Blob16 => {
            "
            SELECT h.row_id, h.title FROM h JOIN tx ON tx.tx_id = h.tx_id
            WHERE h.branch_id = x'03000000000000000000000000000001' AND h.done = 0 AND tx.status = 2 AND tx.global_epoch <= ?1
              AND NOT EXISTS (
                SELECT 1 FROM h newer JOIN tx newer_tx ON newer_tx.tx_id = newer.tx_id
                WHERE newer.branch_id = h.branch_id AND newer.row_id = h.row_id
                  AND newer_tx.status = 2 AND newer_tx.global_epoch <= ?1
                  AND (newer_tx.global_epoch, newer.tx_id) > (tx.global_epoch, h.tx_id)
              )
            ORDER BY h.created_at DESC LIMIT 50
            "
        }
        Layout::InternedInt => {
            "
            SELECT h.row_num, h.title FROM h JOIN tx ON tx.tx_num = h.tx_num
            WHERE h.branch_num = 1 AND h.done = 0 AND tx.status = 2 AND tx.global_epoch <= ?1
              AND NOT EXISTS (
                SELECT 1 FROM h newer JOIN tx newer_tx ON newer_tx.tx_num = newer.tx_num
                WHERE newer.branch_num = h.branch_num AND newer.row_num = h.row_num
                  AND newer_tx.status = 2 AND newer_tx.global_epoch <= ?1
                  AND (newer_tx.global_epoch, newer.tx_num) > (tx.global_epoch, h.tx_num)
              )
            ORDER BY h.created_at DESC LIMIT 50
            "
        }
    }
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

fn open(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -8192;
        ",
    )?;
    Ok(conn)
}

fn sqlite_memory_used() -> i64 {
    sqlite_status(ffi::SQLITE_STATUS_MEMORY_USED).0
}

fn sqlite_memory_highwater() -> i64 {
    sqlite_status(ffi::SQLITE_STATUS_MEMORY_USED).1
}

fn sqlite_status(op: i32) -> (i64, i64) {
    let mut current = 0;
    let mut highwater = 0;
    unsafe {
        ffi::sqlite3_status64(op, &mut current, &mut highwater, 0);
    }
    (current, highwater)
}

fn db_status(conn: &Connection, op: i32) -> rusqlite::Result<i64> {
    let mut current = 0;
    let mut highwater = 0;
    let rc = unsafe { ffi::sqlite3_db_status(conn.handle(), op, &mut current, &mut highwater, 0) };
    if rc == ffi::SQLITE_OK {
        Ok(i64::from(current))
    } else {
        Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rc),
            Some(format!("sqlite3_db_status({op}) failed")),
        ))
    }
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Text => "long_text_ids",
            Self::Blob16 => "blob16_ids",
            Self::InternedInt => "interned_int_ids",
        }
    }
}

fn text_row_id(num: i64) -> String {
    format!("co_z4v7s9abcedfghijklmno/todos/row_{num:012x}")
}

fn text_tx_id(num: i64) -> String {
    format!("tx_z4v7s9abcedfghijklmno/node_alpha/{num:012x}")
}

fn blob_id(kind: u8, num: i64) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[0] = kind;
    bytes[8..16].copy_from_slice(&num.to_be_bytes());
    bytes
}

fn title(row: i64, tx_num: i64) -> String {
    format!("todo {row} at {tx_num}")
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
