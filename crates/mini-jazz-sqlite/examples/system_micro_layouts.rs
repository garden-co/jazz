use rusqlite::{params, Connection};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const ROWS: i64 = 30_000;
const UPDATES: i64 = 6_000;
const QUERY_REPS: usize = 150;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("rows={ROWS}, updates={UPDATES}, query_reps={QUERY_REPS}");
    println!("variant,file_bytes,page_count,insert_ms,current_ms,snapshot_ms,tx_lookup_ms,notes");

    let variants = [
        Variant {
            name: "text_rowid",
            ids: Ids::Text,
            without_rowid: false,
            conflict: Conflict::TextEmptyObject,
        },
        Variant {
            name: "text_without_rowid",
            ids: Ids::Text,
            without_rowid: true,
            conflict: Conflict::TextEmptyObject,
        },
        Variant {
            name: "int_rowid",
            ids: Ids::Int,
            without_rowid: false,
            conflict: Conflict::TextEmptyObject,
        },
        Variant {
            name: "int_without_rowid",
            ids: Ids::Int,
            without_rowid: true,
            conflict: Conflict::TextEmptyObject,
        },
        Variant {
            name: "int_without_rowid_null_conflict",
            ids: Ids::Int,
            without_rowid: true,
            conflict: Conflict::Null,
        },
        Variant {
            name: "int_without_rowid_no_conflict",
            ids: Ids::Int,
            without_rowid: true,
            conflict: Conflict::Omitted,
        },
    ];

    let mut runs = Vec::new();
    for variant in variants {
        let run = run_variant(variant)?;
        println!(
            "{},{},{},{:.3},{:.3},{:.3},{:.3},{}",
            run.name,
            run.file_bytes,
            run.page_count,
            ms(run.insert),
            ms(run.current),
            ms(run.snapshot),
            ms(run.tx_lookup),
            run.notes
        );
        runs.push(run);
    }

    println!();
    let baseline = runs.iter().find(|run| run.name == "text_rowid").unwrap();
    for run in &runs {
        println!(
            "{}: size/text_rowid={:.2}x current/text_rowid={:.2}x snapshot/text_rowid={:.2}x",
            run.name,
            run.file_bytes as f64 / baseline.file_bytes as f64,
            run.current.as_secs_f64() / baseline.current.as_secs_f64(),
            run.snapshot.as_secs_f64() / baseline.snapshot.as_secs_f64(),
        );
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct Variant {
    name: &'static str,
    ids: Ids,
    without_rowid: bool,
    conflict: Conflict,
}

#[derive(Clone, Copy)]
enum Ids {
    Text,
    Int,
}

#[derive(Clone, Copy)]
enum Conflict {
    TextEmptyObject,
    Null,
    Omitted,
}

struct Run {
    name: &'static str,
    file_bytes: u64,
    page_count: i64,
    insert: Duration,
    current: Duration,
    snapshot: Duration,
    tx_lookup: Duration,
    notes: &'static str,
}

fn run_variant(variant: Variant) -> Result<Run, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    create_schema(&conn, variant)?;

    let insert_start = Instant::now();
    {
        let tx = conn.unchecked_transaction()?;
        for row in 1..=ROWS {
            insert_tx(&tx, variant.ids, row, row)?;
            insert_history(&tx, variant, row, row, 0, row)?;
            upsert_current(&tx, variant, row, row, row)?;
        }
        for row in 1..=UPDATES {
            let tx_num = ROWS + row;
            insert_tx(&tx, variant.ids, tx_num, tx_num)?;
            insert_history(&tx, variant, row, tx_num, 1, row)?;
            upsert_current(&tx, variant, row, tx_num, row)?;
        }
        tx.commit()?;
    }
    let insert = insert_start.elapsed();

    conn.execute_batch("VACUUM")?;
    let current = repeat_current_query(&conn, variant.ids)?;
    let snapshot = repeat_snapshot_query(&conn, variant.ids)?;
    let tx_lookup = repeat_tx_lookup(&conn, variant.ids)?;
    let page_count = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let notes = match (variant.ids, variant.without_rowid, variant.conflict) {
        (Ids::Text, false, _) => "text ids, normal tables",
        (Ids::Text, true, _) => "text ids, without rowid",
        (Ids::Int, false, _) => "integer ids, normal tables",
        (Ids::Int, true, Conflict::TextEmptyObject) => "integer ids, without rowid",
        (Ids::Int, true, Conflict::Null) => "integer ids, null conflict",
        (Ids::Int, true, Conflict::Omitted) => "integer ids, conflict omitted",
    };

    Ok(Run {
        name: variant.name,
        file_bytes: file.path().metadata()?.len(),
        page_count,
        insert,
        current,
        snapshot,
        tx_lookup,
        notes,
    })
}

fn create_schema(conn: &Connection, variant: Variant) -> rusqlite::Result<()> {
    match variant.ids {
        Ids::Text => create_text_schema(conn, variant),
        Ids::Int => create_int_schema(conn, variant),
    }
}

fn create_text_schema(conn: &Connection, variant: Variant) -> rusqlite::Result<()> {
    let without = without_rowid(variant);
    let conflict_col = conflict_col(variant.conflict);
    conn.execute_batch(&format!(
        "
        CREATE TABLE tx (
          tx_id TEXT PRIMARY KEY,
          global_epoch INTEGER NOT NULL,
          status TEXT NOT NULL
        ) {without};
        CREATE TABLE h (
          row_id TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          tx_id TEXT NOT NULL,
          op TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
          {conflict_col},
          PRIMARY KEY (row_id, branch_id, tx_id)
        ) {without};
        CREATE INDEX h_visible_order ON h(branch_id, done, created_at DESC, row_id, tx_id);
        CREATE INDEX h_row_tx ON h(branch_id, row_id, tx_id);
        CREATE TABLE c (
          row_id TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          visible_tx_id TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
          {conflict_col},
          PRIMARY KEY (row_id, branch_id)
        ) {without};
        CREATE INDEX c_done_created ON c(branch_id, done, created_at DESC);
        "
    ))
}

fn create_int_schema(conn: &Connection, variant: Variant) -> rusqlite::Result<()> {
    let without = without_rowid(variant);
    let conflict_col = conflict_col(variant.conflict);
    conn.execute_batch(&format!(
        "
        CREATE TABLE tx (
          tx_num INTEGER PRIMARY KEY,
          global_epoch INTEGER NOT NULL,
          status INTEGER NOT NULL
        ) {without};
        CREATE TABLE h (
          row_num INTEGER NOT NULL,
          branch_num INTEGER NOT NULL,
          tx_num INTEGER NOT NULL,
          op INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
          {conflict_col},
          PRIMARY KEY (row_num, branch_num, tx_num)
        ) {without};
        CREATE INDEX h_visible_order ON h(branch_num, done, created_at DESC, row_num, tx_num);
        CREATE INDEX h_row_tx ON h(branch_num, row_num, tx_num);
        CREATE TABLE c (
          row_num INTEGER NOT NULL,
          branch_num INTEGER NOT NULL,
          visible_tx_num INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
          {conflict_col},
          PRIMARY KEY (row_num, branch_num)
        ) {without};
        CREATE INDEX c_done_created ON c(branch_num, done, created_at DESC);
        "
    ))
}

fn without_rowid(variant: Variant) -> &'static str {
    if variant.without_rowid {
        "WITHOUT ROWID"
    } else {
        ""
    }
}

fn conflict_col(conflict: Conflict) -> &'static str {
    match conflict {
        Conflict::TextEmptyObject => ", conflict_json TEXT NOT NULL",
        Conflict::Null => ", conflict_json TEXT",
        Conflict::Omitted => "",
    }
}

fn insert_tx(
    tx: &rusqlite::Transaction<'_>,
    ids: Ids,
    tx_num: i64,
    global_epoch: i64,
) -> rusqlite::Result<()> {
    match ids {
        Ids::Text => tx.execute(
            "INSERT INTO tx (tx_id, global_epoch, status) VALUES (?1, ?2, 'accepted')",
            params![format!("node:{tx_num}"), global_epoch],
        ),
        Ids::Int => tx.execute(
            "INSERT INTO tx (tx_num, global_epoch, status) VALUES (?1, ?2, 2)",
            params![tx_num, global_epoch],
        ),
    }?;
    Ok(())
}

fn insert_history(
    tx: &rusqlite::Transaction<'_>,
    variant: Variant,
    row_num: i64,
    tx_num: i64,
    op_num: i64,
    created_at: i64,
) -> rusqlite::Result<()> {
    match (variant.ids, variant.conflict) {
        (Ids::Text, Conflict::Omitted) => tx.execute(
            "
            INSERT INTO h
              (row_id, branch_id, tx_id, op, title, done, created_at, updated_at)
            VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6, ?7)
            ",
            params![
                format!("todo:{row_num}"),
                format!("node:{tx_num}"),
                if op_num == 0 { "insert" } else { "update" },
                title(row_num, tx_num),
                row_num % 3 == 0,
                created_at,
                tx_num
            ],
        ),
        (Ids::Text, Conflict::Null) => tx.execute(
            "
            INSERT INTO h
              (row_id, branch_id, tx_id, op, title, done, created_at, updated_at, conflict_json)
            VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6, ?7, NULL)
            ",
            params![
                format!("todo:{row_num}"),
                format!("node:{tx_num}"),
                if op_num == 0 { "insert" } else { "update" },
                title(row_num, tx_num),
                row_num % 3 == 0,
                created_at,
                tx_num
            ],
        ),
        (Ids::Text, Conflict::TextEmptyObject) => tx.execute(
            "
            INSERT INTO h
              (row_id, branch_id, tx_id, op, title, done, created_at, updated_at, conflict_json)
            VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6, ?7, '{}')
            ",
            params![
                format!("todo:{row_num}"),
                format!("node:{tx_num}"),
                if op_num == 0 { "insert" } else { "update" },
                title(row_num, tx_num),
                row_num % 3 == 0,
                created_at,
                tx_num
            ],
        ),
        (Ids::Int, Conflict::Omitted) => tx.execute(
            "
            INSERT INTO h
              (row_num, branch_num, tx_num, op, title, done, created_at, updated_at)
            VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7)
            ",
            params![
                row_num,
                tx_num,
                op_num,
                title(row_num, tx_num),
                row_num % 3 == 0,
                created_at,
                tx_num
            ],
        ),
        (Ids::Int, Conflict::Null) => tx.execute(
            "
            INSERT INTO h
              (row_num, branch_num, tx_num, op, title, done, created_at, updated_at, conflict_json)
            VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)
            ",
            params![
                row_num,
                tx_num,
                op_num,
                title(row_num, tx_num),
                row_num % 3 == 0,
                created_at,
                tx_num
            ],
        ),
        (Ids::Int, Conflict::TextEmptyObject) => tx.execute(
            "
            INSERT INTO h
              (row_num, branch_num, tx_num, op, title, done, created_at, updated_at, conflict_json)
            VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, '{}')
            ",
            params![
                row_num,
                tx_num,
                op_num,
                title(row_num, tx_num),
                row_num % 3 == 0,
                created_at,
                tx_num
            ],
        ),
    }?;
    Ok(())
}

fn upsert_current(
    tx: &rusqlite::Transaction<'_>,
    variant: Variant,
    row_num: i64,
    tx_num: i64,
    created_at: i64,
) -> rusqlite::Result<()> {
    match (variant.ids, variant.conflict) {
        (Ids::Text, Conflict::Omitted) => tx.execute(
            "
            INSERT OR REPLACE INTO c
              (row_id, branch_id, visible_tx_id, title, done, created_at, updated_at)
            VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6)
            ",
            params![
                format!("todo:{row_num}"),
                format!("node:{tx_num}"),
                title(row_num, tx_num),
                row_num % 3 == 0,
                created_at,
                tx_num
            ],
        ),
        (Ids::Text, Conflict::Null) => tx.execute(
            "
            INSERT OR REPLACE INTO c
              (row_id, branch_id, visible_tx_id, title, done, created_at, updated_at, conflict_json)
            VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6, NULL)
            ",
            params![
                format!("todo:{row_num}"),
                format!("node:{tx_num}"),
                title(row_num, tx_num),
                row_num % 3 == 0,
                created_at,
                tx_num
            ],
        ),
        (Ids::Text, Conflict::TextEmptyObject) => tx.execute(
            "
            INSERT OR REPLACE INTO c
              (row_id, branch_id, visible_tx_id, title, done, created_at, updated_at, conflict_json)
            VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6, '{}')
            ",
            params![
                format!("todo:{row_num}"),
                format!("node:{tx_num}"),
                title(row_num, tx_num),
                row_num % 3 == 0,
                created_at,
                tx_num
            ],
        ),
        (Ids::Int, Conflict::Omitted) => tx.execute(
            "
            INSERT OR REPLACE INTO c
              (row_num, branch_num, visible_tx_num, title, done, created_at, updated_at)
            VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6)
            ",
            params![row_num, tx_num, title(row_num, tx_num), row_num % 3 == 0, created_at, tx_num],
        ),
        (Ids::Int, Conflict::Null) => tx.execute(
            "
            INSERT OR REPLACE INTO c
              (row_num, branch_num, visible_tx_num, title, done, created_at, updated_at, conflict_json)
            VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, NULL)
            ",
            params![row_num, tx_num, title(row_num, tx_num), row_num % 3 == 0, created_at, tx_num],
        ),
        (Ids::Int, Conflict::TextEmptyObject) => tx.execute(
            "
            INSERT OR REPLACE INTO c
              (row_num, branch_num, visible_tx_num, title, done, created_at, updated_at, conflict_json)
            VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, '{}')
            ",
            params![row_num, tx_num, title(row_num, tx_num), row_num % 3 == 0, created_at, tx_num],
        ),
    }?;
    Ok(())
}

fn repeat_current_query(conn: &Connection, ids: Ids) -> rusqlite::Result<Duration> {
    let sql = match ids {
        Ids::Text => {
            "SELECT row_id, title FROM c WHERE branch_id = 'main' AND done = 0 ORDER BY created_at DESC LIMIT 50"
        }
        Ids::Int => {
            "SELECT row_num, title FROM c WHERE branch_num = 1 AND done = 0 ORDER BY created_at DESC LIMIT 50"
        }
    };
    repeat_scan(conn, sql, &[])
}

fn repeat_snapshot_query(conn: &Connection, ids: Ids) -> rusqlite::Result<Duration> {
    let sql = match ids {
        Ids::Text => {
            "
            SELECT h.row_id, h.title
            FROM h JOIN tx ON tx.tx_id = h.tx_id
            WHERE h.branch_id = 'main'
              AND h.done = 0
              AND tx.status = 'accepted'
              AND tx.global_epoch <= ?1
              AND NOT EXISTS (
                SELECT 1
                FROM h newer JOIN tx newer_tx ON newer_tx.tx_id = newer.tx_id
                WHERE newer.branch_id = h.branch_id
                  AND newer.row_id = h.row_id
                  AND newer_tx.status = 'accepted'
                  AND newer_tx.global_epoch <= ?1
                  AND (newer_tx.global_epoch, newer.tx_id) > (tx.global_epoch, h.tx_id)
              )
            ORDER BY h.created_at DESC
            LIMIT 50
            "
        }
        Ids::Int => {
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
    };
    repeat_scan(conn, sql, &[&ROWS])
}

fn repeat_tx_lookup(conn: &Connection, ids: Ids) -> rusqlite::Result<Duration> {
    let start = Instant::now();
    for idx in 0..QUERY_REPS {
        let tx_num = ((idx as i64 * 7919) % (ROWS + UPDATES)) + 1;
        match ids {
            Ids::Text => {
                let _: i64 = conn.query_row(
                    "SELECT global_epoch FROM tx WHERE tx_id = ?1",
                    params![format!("node:{tx_num}")],
                    |row| row.get(0),
                )?;
            }
            Ids::Int => {
                let _: i64 = conn.query_row(
                    "SELECT global_epoch FROM tx WHERE tx_num = ?1",
                    params![tx_num],
                    |row| row.get(0),
                )?;
            }
        }
    }
    Ok(start.elapsed())
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
        ",
    )?;
    Ok(conn)
}

fn title(row_num: i64, tx_num: i64) -> String {
    format!("todo {row_num} at {tx_num}")
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
