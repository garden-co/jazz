use rusqlite::{params, Connection};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const ROWS: i64 = 50_000;
const UPDATES: i64 = 10_000;
const QUERY_REPS: usize = 250;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("rows={ROWS}, updates={UPDATES}, query_reps={QUERY_REPS}");
    println!("shape,file_bytes,page_count,insert_ms,current_ms,snapshot_ms,plan_note");

    let shapes = [
        Shape {
            name: "minimal_indexes",
            current_covering: false,
            history_covering: false,
            partial_current: false,
        },
        Shape {
            name: "covering_current",
            current_covering: true,
            history_covering: false,
            partial_current: false,
        },
        Shape {
            name: "covering_history",
            current_covering: false,
            history_covering: true,
            partial_current: false,
        },
        Shape {
            name: "covering_both",
            current_covering: true,
            history_covering: true,
            partial_current: false,
        },
        Shape {
            name: "partial_current_covering_history",
            current_covering: false,
            history_covering: true,
            partial_current: true,
        },
    ];

    let mut runs = Vec::new();
    for shape in shapes {
        let run = run_shape(shape)?;
        println!(
            "{},{},{},{:.3},{:.3},{:.3},{}",
            run.name,
            run.file_bytes,
            run.page_count,
            ms(run.insert),
            ms(run.current),
            ms(run.snapshot),
            run.plan_note
        );
        runs.push(run);
    }

    println!();
    let baseline = runs
        .iter()
        .find(|run| run.name == "minimal_indexes")
        .unwrap();
    for run in &runs {
        println!(
            "{}: size/minimal={:.2}x current/minimal={:.2}x snapshot/minimal={:.2}x",
            run.name,
            run.file_bytes as f64 / baseline.file_bytes as f64,
            run.current.as_secs_f64() / baseline.current.as_secs_f64(),
            run.snapshot.as_secs_f64() / baseline.snapshot.as_secs_f64(),
        );
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct Shape {
    name: &'static str,
    current_covering: bool,
    history_covering: bool,
    partial_current: bool,
}

struct Run {
    name: &'static str,
    file_bytes: u64,
    page_count: i64,
    insert: Duration,
    current: Duration,
    snapshot: Duration,
    plan_note: &'static str,
}

fn run_shape(shape: Shape) -> Result<Run, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    create_schema(&conn, shape)?;

    let insert_start = Instant::now();
    {
        let tx = conn.unchecked_transaction()?;
        for row in 1..=ROWS {
            insert_tx(&tx, row, row)?;
            insert_history(&tx, row, row, row, row)?;
            upsert_current(&tx, row, row, row, row)?;
        }
        for row in 1..=UPDATES {
            let tx_num = ROWS + row;
            insert_tx(&tx, tx_num, tx_num)?;
            insert_history(&tx, row, tx_num, row, tx_num)?;
            upsert_current(&tx, row, tx_num, row, tx_num)?;
        }
        tx.commit()?;
    }
    let insert = insert_start.elapsed();
    conn.execute_batch("VACUUM")?;

    let current = repeat_scan(&conn, current_sql(), &[])?;
    let snapshot = repeat_scan(&conn, snapshot_sql(), &[&ROWS])?;
    let page_count = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let plan_note = if shape.current_covering && shape.history_covering {
        "cover current and history order"
    } else if shape.history_covering {
        "cover history snapshot order"
    } else if shape.current_covering {
        "cover current page"
    } else if shape.partial_current {
        "partial open-current index"
    } else {
        "minimal order indexes"
    };

    Ok(Run {
        name: shape.name,
        file_bytes: file.path().metadata()?.len(),
        page_count,
        insert,
        current,
        snapshot,
        plan_note,
    })
}

fn create_schema(conn: &Connection, shape: Shape) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
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
        CREATE INDEX h_row_tx ON h(branch_num, row_num, tx_num);
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
        ",
    )?;

    let current_index = if shape.partial_current {
        "CREATE INDEX c_open_created ON c(created_at DESC, row_num, title) WHERE branch_num = 1 AND done = 0"
    } else if shape.current_covering {
        "CREATE INDEX c_done_created ON c(branch_num, done, created_at DESC, row_num, title)"
    } else {
        "CREATE INDEX c_done_created ON c(branch_num, done, created_at DESC)"
    };
    conn.execute(current_index, [])?;

    let history_index = if shape.history_covering {
        "CREATE INDEX h_visible_order ON h(branch_num, done, created_at DESC, row_num, tx_num, title)"
    } else {
        "CREATE INDEX h_visible_order ON h(branch_num, done, created_at DESC, row_num, tx_num)"
    };
    conn.execute(history_index, [])?;
    Ok(())
}

fn insert_tx(
    tx: &rusqlite::Transaction<'_>,
    tx_num: i64,
    global_epoch: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO tx (tx_num, global_epoch, status) VALUES (?1, ?2, 2)",
        params![tx_num, global_epoch],
    )?;
    Ok(())
}

fn insert_history(
    tx: &rusqlite::Transaction<'_>,
    row_num: i64,
    tx_num: i64,
    created_at: i64,
    title_num: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT INTO h (row_num, branch_num, tx_num, title, done, created_at, updated_at)
        VALUES (?1, 1, ?2, ?3, ?4, ?5, ?2)
        ",
        params![
            row_num,
            tx_num,
            title(title_num),
            row_num % 3 == 0,
            created_at
        ],
    )?;
    Ok(())
}

fn upsert_current(
    tx: &rusqlite::Transaction<'_>,
    row_num: i64,
    tx_num: i64,
    created_at: i64,
    title_num: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "
        INSERT OR REPLACE INTO c
          (row_num, branch_num, visible_tx_num, title, done, created_at, updated_at)
        VALUES (?1, 1, ?2, ?3, ?4, ?5, ?2)
        ",
        params![
            row_num,
            tx_num,
            title(title_num),
            row_num % 3 == 0,
            created_at
        ],
    )?;
    Ok(())
}

fn current_sql() -> &'static str {
    "
    SELECT row_num, title
    FROM c
    WHERE branch_num = 1 AND done = 0
    ORDER BY created_at DESC
    LIMIT 50
    "
}

fn snapshot_sql() -> &'static str {
    "
    SELECT h.row_num, h.title
    FROM h
    JOIN tx ON tx.tx_num = h.tx_num
    WHERE h.branch_num = 1
      AND h.done = 0
      AND tx.status = 2
      AND tx.global_epoch <= ?1
      AND NOT EXISTS (
        SELECT 1
        FROM h newer
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

fn title(num: i64) -> String {
    format!("todo {num}")
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
