//! Local microbenchmark, not an end-to-end performance result.
//! Run: cargo run --release -p jazz --example async_host_probe
//! Compares the shared async conditional insert with an equivalent synchronous
//! body, both inside the same public host savepoint API on the same store.
//! The hot key already exists, so this measures cached SQL and control overhead.
use jazz::{
    db::{Error, ErrorCode, block_on},
    host::{HostTable, HostTables, StoreFile},
};
use rusqlite::{OptionalExtension, params};
use std::{hint::black_box, time::Instant};

const TABLE: HostTable = HostTable::new("async_probe");

fn storage(error: rusqlite::Error) -> Error {
    Error::new(ErrorCode::Storage, error.to_string())
}

#[inline(never)]
fn synchronous(file: &StoreFile) -> Result<Option<Vec<u8>>, Error> {
    let mut prior = None;
    let name = TABLE.sql_name();
    block_on(file.host_sql(&mut |conn| {
        conn.prepare_cached(&format!(
            "CREATE TABLE IF NOT EXISTS {name} (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID",
        ))
        .and_then(|mut statement| statement.execute([]))
        .map_err(storage)?;
        prior = conn
            .prepare_cached(&format!("SELECT v FROM {name} WHERE k = ?1"))
            .and_then(|mut statement| {
                statement
                    .query_row([b"key".as_slice()], |row| row.get(0))
                    .optional()
            })
            .map_err(storage)?;
        if prior.is_none() {
            conn.prepare_cached(&format!("INSERT INTO {name} (k, v) VALUES (?1, ?2)"))
                .and_then(|mut statement| {
                    statement.execute(params![b"key".as_slice(), b"value".as_slice()])
                })
                .map_err(storage)?;
        }
        Ok(())
    }))?;
    Ok(prior)
}

#[inline(never)]
fn shared_async(file: &StoreFile) -> Result<Option<Vec<u8>>, Error> {
    block_on(file.host_put_if_absent(TABLE, b"key", b"value"))
}

fn run(file: &StoreFile, f: fn(&StoreFile) -> Result<Option<Vec<u8>>, Error>, n: u32) -> f64 {
    let started = Instant::now();
    for _ in 0..n {
        black_box(f(black_box(file)).unwrap());
    }
    started.elapsed().as_nanos() as f64 / f64::from(n)
}

fn main() {
    let file = StoreFile::memory().unwrap();
    assert_eq!(shared_async(&file).unwrap(), None);
    assert_eq!(synchronous(&file).unwrap(), Some(b"value".to_vec()));
    run(&file, synchronous, 10_000);
    run(&file, shared_async, 10_000);
    println!("sample,sync_ns,async_ns");
    for sample in 0..6 {
        let (sync, asynchronous) = if sample % 2 == 0 {
            (
                run(&file, synchronous, 100_000),
                run(&file, shared_async, 100_000),
            )
        } else {
            let asynchronous = run(&file, shared_async, 100_000);
            (run(&file, synchronous, 100_000), asynchronous)
        };
        println!("{sample},{sync:.1},{asynchronous:.1}");
    }
}
