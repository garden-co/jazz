use rusqlite::{params, Connection};
use serde_json::json;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const TX_COUNT: usize = 20_000;
const ROW_COUNT: usize = 5_000;
const VALIDATION_COUNT: usize = 5_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let json = run_json_layout()?;
    let indexed = run_indexed_layout()?;
    let hybrid = run_hybrid_layout()?;

    println!(
        "layout,txs,rows,file_bytes,page_count,page_size,insert_ms,validate_ms,validations_ok"
    );
    print_metrics(&json);
    print_metrics(&indexed);
    print_metrics(&hybrid);

    println!();
    println!(
        "indexed/json file size: {:.2}x",
        indexed.file_bytes as f64 / json.file_bytes as f64
    );
    println!(
        "hybrid/json file size: {:.2}x",
        hybrid.file_bytes as f64 / json.file_bytes as f64
    );
    println!(
        "indexed/json validation time: {:.2}x",
        indexed.validate.as_secs_f64() / json.validate.as_secs_f64()
    );
    println!(
        "hybrid/json validation time: {:.2}x",
        hybrid.validate.as_secs_f64() / json.validate.as_secs_f64()
    );
    println!();
    println!("validation query plans:");
    println!("json:    SEARCH tx USING INDEX sqlite_autoindex_tx_1 (tx_id=?) + JSON parse in Rust");
    println!("indexed: SEARCH tx_read USING INDEX sqlite_autoindex_tx_read_1 (tx_id=? AND table_name=? AND row_id=?)");
    println!("hybrid:  SEARCH tx USING INDEX sqlite_autoindex_tx_1 (tx_id=?) + JSON parse in Rust");

    Ok(())
}

struct Metrics {
    layout: &'static str,
    file_bytes: u64,
    page_count: i64,
    page_size: i64,
    insert: Duration,
    validate: Duration,
    validations_ok: usize,
}

fn print_metrics(metrics: &Metrics) {
    println!(
        "{},{TX_COUNT},{ROW_COUNT},{},{},{},{:.3},{:.3},{}",
        metrics.layout,
        metrics.file_bytes,
        metrics.page_count,
        metrics.page_size,
        metrics.insert.as_secs_f64() * 1000.0,
        metrics.validate.as_secs_f64() * 1000.0,
        metrics.validations_ok
    );
}

fn run_json_layout() -> Result<Metrics, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    conn.execute_batch(
        "
        CREATE TABLE tx (
          tx_id TEXT PRIMARY KEY,
          metadata_json TEXT NOT NULL
        );
        ",
    )?;

    let insert_start = Instant::now();
    {
        let sql_tx = conn.unchecked_transaction()?;
        for tx_idx in 0..TX_COUNT {
            let tx_id = tx_id(tx_idx);
            let read_row = row_id(tx_idx);
            let write_row = row_id(tx_idx + 17);
            let metadata = json!({
                "read_set": [{
                    "table": "todos",
                    "row_id": read_row,
                    "visible_tx_id": visible_tx(tx_idx),
                }],
                "write_set": [{
                    "table": "todos",
                    "row_id": write_row,
                    "columns": columns(tx_idx),
                }],
            });
            sql_tx.execute(
                "INSERT INTO tx (tx_id, metadata_json) VALUES (?1, ?2)",
                params![tx_id, metadata.to_string()],
            )?;
        }
        sql_tx.commit()?;
    }
    let insert = insert_start.elapsed();

    let validate_start = Instant::now();
    let mut ok = 0;
    for tx_idx in validation_tx_indices() {
        let tx_id = tx_id(tx_idx);
        let metadata_json: String = conn.query_row(
            "SELECT metadata_json FROM tx WHERE tx_id = ?1",
            params![tx_id],
            |row| row.get(0),
        )?;
        let metadata: serde_json::Value = serde_json::from_str(&metadata_json)?;
        let expected = visible_tx(tx_idx);
        if metadata["read_set"][0]["visible_tx_id"] == expected {
            ok += 1;
        }
    }
    let validate = validate_start.elapsed();

    metrics("json", file.path(), &conn, insert, validate, ok)
}

fn run_indexed_layout() -> Result<Metrics, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    conn.execute_batch(
        "
        CREATE TABLE tx (
          tx_id TEXT PRIMARY KEY
        );
        CREATE TABLE tx_read (
          tx_id TEXT NOT NULL,
          table_name TEXT NOT NULL,
          row_id TEXT NOT NULL,
          visible_tx_id TEXT NOT NULL,
          PRIMARY KEY (tx_id, table_name, row_id)
        );
        CREATE INDEX tx_read_row ON tx_read(table_name, row_id, tx_id);
        CREATE TABLE tx_write_column (
          tx_id TEXT NOT NULL,
          table_name TEXT NOT NULL,
          row_id TEXT NOT NULL,
          column_name TEXT NOT NULL,
          PRIMARY KEY (tx_id, table_name, row_id, column_name)
        );
        CREATE INDEX tx_write_column_row
          ON tx_write_column(table_name, row_id, column_name, tx_id);
        ",
    )?;

    let insert_start = Instant::now();
    {
        let sql_tx = conn.unchecked_transaction()?;
        for tx_idx in 0..TX_COUNT {
            let tx_id = tx_id(tx_idx);
            sql_tx.execute("INSERT INTO tx (tx_id) VALUES (?1)", params![tx_id])?;
            sql_tx.execute(
                "
                INSERT INTO tx_read (tx_id, table_name, row_id, visible_tx_id)
                VALUES (?1, 'todos', ?2, ?3)
                ",
                params![tx_id, row_id(tx_idx), visible_tx(tx_idx)],
            )?;
            for column in columns(tx_idx) {
                sql_tx.execute(
                    "
                    INSERT INTO tx_write_column (tx_id, table_name, row_id, column_name)
                    VALUES (?1, 'todos', ?2, ?3)
                    ",
                    params![tx_id, row_id(tx_idx + 17), column],
                )?;
            }
        }
        sql_tx.commit()?;
    }
    let insert = insert_start.elapsed();

    let validate_start = Instant::now();
    let mut ok = 0;
    for tx_idx in validation_tx_indices() {
        let tx_id = tx_id(tx_idx);
        let visible_tx_id: String = conn.query_row(
            "
            SELECT visible_tx_id
            FROM tx_read
            WHERE tx_id = ?1 AND table_name = 'todos' AND row_id = ?2
            ",
            params![tx_id, row_id(tx_idx)],
            |row| row.get(0),
        )?;
        if visible_tx_id == visible_tx(tx_idx) {
            ok += 1;
        }
    }
    let validate = validate_start.elapsed();

    metrics("indexed", file.path(), &conn, insert, validate, ok)
}

fn run_hybrid_layout() -> Result<Metrics, Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let conn = open(file.path())?;
    conn.execute_batch(
        "
        CREATE TABLE tx (
          tx_id TEXT PRIMARY KEY,
          metadata_json TEXT NOT NULL
        );
        CREATE TABLE pending_tx_write_row (
          tx_id TEXT NOT NULL,
          table_name TEXT NOT NULL,
          row_id TEXT NOT NULL,
          PRIMARY KEY (tx_id, table_name, row_id)
        );
        CREATE INDEX pending_tx_write_row_lookup
          ON pending_tx_write_row(table_name, row_id, tx_id);
        ",
    )?;

    let insert_start = Instant::now();
    {
        let sql_tx = conn.unchecked_transaction()?;
        for tx_idx in 0..TX_COUNT {
            let tx_id = tx_id(tx_idx);
            let write_row = row_id(tx_idx + 17);
            let metadata = json!({
                "read_set": [{
                    "table": "todos",
                    "row_id": row_id(tx_idx),
                    "visible_tx_id": visible_tx(tx_idx),
                }],
                "write_set": [{
                    "table": "todos",
                    "row_id": write_row,
                    "columns": columns(tx_idx),
                }],
            });
            sql_tx.execute(
                "INSERT INTO tx (tx_id, metadata_json) VALUES (?1, ?2)",
                params![tx_id, metadata.to_string()],
            )?;
            sql_tx.execute(
                "
                INSERT INTO pending_tx_write_row (tx_id, table_name, row_id)
                VALUES (?1, 'todos', ?2)
                ",
                params![tx_id, write_row],
            )?;
        }
        sql_tx.commit()?;
    }
    let insert = insert_start.elapsed();

    let validate_start = Instant::now();
    let mut ok = 0;
    for tx_idx in validation_tx_indices() {
        let tx_id = tx_id(tx_idx);
        let metadata_json: String = conn.query_row(
            "SELECT metadata_json FROM tx WHERE tx_id = ?1",
            params![tx_id],
            |row| row.get(0),
        )?;
        let metadata: serde_json::Value = serde_json::from_str(&metadata_json)?;
        let expected = visible_tx(tx_idx);
        if metadata["read_set"][0]["visible_tx_id"] == expected {
            ok += 1;
        }
    }
    let validate = validate_start.elapsed();

    metrics("hybrid", file.path(), &conn, insert, validate, ok)
}

fn open(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;
        ",
    )?;
    Ok(conn)
}

fn metrics(
    layout: &'static str,
    path: &Path,
    conn: &Connection,
    insert: Duration,
    validate: Duration,
    validations_ok: usize,
) -> Result<Metrics, Box<dyn std::error::Error>> {
    checkpoint(conn)?;
    let page_count = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    Ok(Metrics {
        layout,
        file_bytes: path.metadata()?.len(),
        page_count,
        page_size,
        insert,
        validate,
        validations_ok,
    })
}

fn checkpoint(conn: &Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
}

fn validation_tx_indices() -> impl Iterator<Item = usize> {
    (0..VALIDATION_COUNT).map(|idx| (idx * 7919) % TX_COUNT)
}

fn tx_id(idx: usize) -> String {
    format!("node:{}", idx + 1)
}

fn row_id(idx: usize) -> String {
    format!("todos:{}", (idx % ROW_COUNT) + 1)
}

fn visible_tx(idx: usize) -> String {
    format!("base:{}", (idx % ROW_COUNT) + 1)
}

fn columns(idx: usize) -> Vec<&'static str> {
    match idx % 4 {
        0 => vec!["title"],
        1 => vec!["done"],
        2 => vec!["title", "done"],
        _ => vec!["project_id"],
    }
}
