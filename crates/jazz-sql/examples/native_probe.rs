//! Local SQL-boundary microbenchmark, not an end-to-end Jazz benchmark.
//! cargo run --release -p jazz-sql --features native,rusqlite/bundled --example native_probe
//! Both arms use cached statements, identical SQL/bindings, and identical
//! owned outputs. Differences include row/parameter adapters, not just await.
use jazz_sql::{Param, Row, Sql, inline, native::Native};
use rusqlite::Connection;
use std::{hint::black_box, time::Instant};

type Rows = Vec<(i64, String)>;
const QUERY: &str = "SELECT id, body FROM notes WHERE id > ?1 ORDER BY id LIMIT ?2";
const UPDATE: &str = "UPDATE notes SET counter = counter + 1 WHERE id = ?1";

#[inline(never)]
fn sync_query(conn: &Connection) -> rusqlite::Result<Rows> {
    conn.prepare_cached(QUERY)?
        .query_map([0i64, 8], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

#[inline(never)]
fn async_query(conn: &Connection) -> rusqlite::Result<Rows> {
    inline(
        Native(conn).query_rows(QUERY, &[Param::Integer(0), Param::Integer(8)], |row| {
            Ok((row.get(0)?, row.get(1)?))
        }),
    )
}

#[inline(never)]
fn sync_execute(conn: &Connection) -> rusqlite::Result<usize> {
    conn.prepare_cached(UPDATE)?.execute([1i64])
}

#[inline(never)]
fn async_execute(conn: &Connection) -> rusqlite::Result<usize> {
    inline(Native(conn).execute(UPDATE, &[Param::Integer(1)]))
}

fn run<T>(conn: &Connection, call: fn(&Connection) -> rusqlite::Result<T>, n: u32) -> f64 {
    let started = Instant::now();
    for _ in 0..n {
        black_box(call(black_box(conn)).unwrap());
    }
    started.elapsed().as_nanos() as f64 / f64::from(n)
}

fn compare<T>(
    name: &str,
    conn: &Connection,
    sync: fn(&Connection) -> rusqlite::Result<T>,
    asynchronous: fn(&Connection) -> rusqlite::Result<T>,
) {
    run(conn, sync, 10_000);
    run(conn, asynchronous, 10_000);
    for sample in 0..8 {
        let (a, b) = if sample % 2 == 0 {
            (run(conn, sync, 100_000), run(conn, asynchronous, 100_000))
        } else {
            let b = run(conn, asynchronous, 100_000);
            (run(conn, sync, 100_000), b)
        };
        println!("{name},{sample},{a:.1},{b:.1}");
    }
}

fn main() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, counter INTEGER);
        WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<16)
        INSERT INTO notes SELECT n, 'hello', 0 FROM ids;
        BEGIN",
    )
    .unwrap();
    assert_eq!(sync_query(&conn).unwrap(), async_query(&conn).unwrap());
    assert_eq!(sync_execute(&conn).unwrap(), 1);
    assert_eq!(async_execute(&conn).unwrap(), 1);
    println!("case,sample,sync_ns,async_ns");
    compare("execute", &conn, sync_execute, async_execute);
    compare("query_8_rows", &conn, sync_query, async_query);
    conn.execute_batch("ROLLBACK").unwrap();
}
