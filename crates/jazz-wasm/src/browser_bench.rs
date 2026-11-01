//! Browser store comparison probes (`browser-bench` feature, never in the
//! shipping bundle): the same workload on the IndexedDB and OPFS VFSes, or
//! in memory, timed from inside the worker.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{col, eq, lit, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use wasm_bindgen::prelude::*;
use web_time::Instant;

use crate::browser::{self, Store, StoreSpec};

thread_local! {
    static OPEN: RefCell<Option<(Rc<Db>, Option<Store>)>> = const { RefCell::new(None) };
}

fn schema() -> JazzSchema {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("n", ColumnType::BigInt)
                    .policies(policies.clone()),
            )
            .table(
                TableSchemaBuilder::new("docs")
                    .column("body", ColumnType::Text)
                    .policies(policies.clone()),
            )
            .table(
                TableSchemaBuilder::new("logs")
                    .column("content", ColumnType::Bytea)
                    .policies(policies),
            )
            .build(),
    )
    .expect("bench schema")
}

fn config() -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn err(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn result(pairs: &[(&str, f64)]) -> JsValue {
    let out = js_sys::Object::new();
    for (key, value) in pairs {
        let _ = js_sys::Reflect::set(&out, &JsValue::from_str(key), &JsValue::from_f64(*value));
    }
    out.into()
}

fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// The `q` quantile of `samples` (sorted in place).
fn quantile(samples: &mut [f64], q: f64) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples
        .get(((samples.len() as f64 - 1.0) * q).round() as usize)
        .copied()
        .unwrap_or(0.0)
}

fn todo(i: u32) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(format!("todo {i:06}"))),
        ("done".to_owned(), Value::Bool(i % 3 == 0)),
        ("n".to_owned(), Value::I64(i as i64)),
    ])
}

fn opened() -> Result<(Rc<Db>, Option<Store>), JsValue> {
    OPEN.with(|open| open.borrow().clone().ok_or_else(|| err("not open")))
}

/// `n` todos, each awaited until durable (commit, then the store's flush):
/// the latency a user's write waits for before it survives a crash.
#[wasm_bindgen(js_name = browserBenchWriteDurable)]
pub async fn browser_bench_write_durable(n: u32) -> Result<JsValue, JsValue> {
    let (db, store) = opened()?;
    let mut commits = Vec::new();
    let mut durable = Vec::new();
    for i in 0..n {
        let start = Instant::now();
        db.insert("todos", todo(1_000_000 + i), Default::default())
            .await
            .map_err(err)?;
        commits.push(ms(start));
        if let Some(store) = &store {
            store.flush().await?;
        }
        durable.push(ms(start));
    }
    Ok(result(&[
        ("commitP50Ms", quantile(&mut commits, 0.5)),
        ("commitP95Ms", quantile(&mut commits, 0.95)),
        ("durableP50Ms", quantile(&mut durable, 0.5)),
        ("durableP95Ms", quantile(&mut durable, 0.95)),
    ]))
}

/// One `bytes` value (a document's content log), committed and flushed.
#[wasm_bindgen(js_name = browserBenchWriteBytes)]
pub async fn browser_bench_write_bytes(bytes: Vec<u8>) -> Result<JsValue, JsValue> {
    let (db, store) = opened()?;
    let start = Instant::now();
    db.insert(
        "logs",
        BTreeMap::from([("content".to_owned(), Value::Bytes(bytes))]),
        Default::default(),
    )
    .await
    .map_err(err)?;
    let commit_ms = ms(start);
    if let Some(store) = &store {
        store.flush().await?;
    }
    Ok(result(&[
        ("bytesCommitMs", commit_ms),
        ("bytesDurableMs", ms(start)),
    ]))
}

/// Read every log back whole.
#[wasm_bindgen(js_name = browserBenchReadBytes)]
pub fn browser_bench_read_bytes() -> Result<JsValue, JsValue> {
    with_db(|db| {
        let start = Instant::now();
        let prepared = db.prepare_query(&Query::from("logs")).map_err(err)?;
        let rows = jazz::db::block_on(db.all(&prepared, ReadOpts::default())).map_err(err)?;
        let mut total = 0usize;
        for row in &rows {
            if let Some((_, Some(Value::Bytes(bytes)))) =
                row.cells().find(|(name, _)| *name == "content")
            {
                total += bytes.len();
            }
        }
        Ok(result(&[
            ("logBytes", total as f64),
            ("logReadMs", ms(start)),
        ]))
    })
}

/// Open `{ backend: "indexeddb" | "opfs" | "memory", path? }`.
#[wasm_bindgen(js_name = browserBenchOpen)]
pub async fn browser_bench_open(storage: JsValue) -> Result<JsValue, JsValue> {
    let memory = js_sys::Reflect::get(&storage, &JsValue::from_str("backend"))?
        .as_string()
        .as_deref()
        == Some("memory");
    let start = Instant::now();
    let opened = if memory {
        (Db::open(config()).await.map_err(err)?, None)
    } else {
        let spec = StoreSpec::from_js(&storage, "bench")?;
        let (db, store) = browser::open(&spec, config()).await?;
        (db, Some(store))
    };
    let open_ms = ms(start);
    OPEN.with(|open| *open.borrow_mut() = Some((Rc::new(opened.0), opened.1)));
    Ok(result(&[("openMs", open_ms)]))
}

fn with_db<T>(f: impl FnOnce(&Db) -> Result<T, JsValue>) -> Result<T, JsValue> {
    OPEN.with(|open| {
        let open = open.borrow();
        let (db, _) = open.as_ref().ok_or_else(|| err("not open"))?;
        f(db)
    })
}

/// `n` todos, one commit each (the app's normal write path).
#[wasm_bindgen(js_name = browserBenchWrite)]
pub fn browser_bench_write(n: u32) -> Result<JsValue, JsValue> {
    with_db(|db| {
        let start = Instant::now();
        let mut each = Vec::with_capacity(n as usize);
        for i in 0..n {
            let one = Instant::now();
            jazz::db::block_on(db.insert("todos", todo(i), Default::default())).map_err(err)?;
            each.push(ms(one));
        }
        let total = ms(start);
        Ok(result(&[
            ("writeMs", total),
            ("perWriteUs", total * 1000.0 / n.max(1) as f64),
            ("writeP95Us", quantile(&mut each, 0.95) * 1000.0),
        ]))
    })
}

/// `n` documents of `kib` KiB each: exercises the large-value chunk store
/// and its reader connection on the VFS.
#[wasm_bindgen(js_name = browserBenchWriteLarge)]
pub fn browser_bench_write_large(n: u32, kib: u32) -> Result<JsValue, JsValue> {
    with_db(|db| {
        let start = Instant::now();
        for i in 0..n {
            let body: String = (0..kib as usize * 1024)
                .map(|k| (b'a' + ((k * 7 + i as usize) % 26) as u8) as char)
                .collect();
            jazz::db::block_on(db.insert(
                "docs",
                BTreeMap::from([("body".to_owned(), Value::String(body))]),
                Default::default(),
            ))
            .map_err(err)?;
        }
        Ok(result(&[("writeLargeMs", ms(start))]))
    })
}

/// Full scan of todos, a filtered read, and the large docs read back whole.
#[wasm_bindgen(js_name = browserBenchRead)]
pub fn browser_bench_read() -> Result<JsValue, JsValue> {
    with_db(|db| {
        let all = |query: Query| -> Result<Vec<_>, JsValue> {
            let prepared = db.prepare_query(&query).map_err(err)?;
            jazz::db::block_on(db.all(&prepared, ReadOpts::default())).map_err(err)
        };
        let start = Instant::now();
        let rows = all(Query::from("todos"))?.len();
        let scan_ms = ms(start);
        let start = Instant::now();
        let done = all(Query::from("todos").filter(eq(col("done"), lit(Value::Bool(true)))))?.len();
        let filter_ms = ms(start);
        let start = Instant::now();
        let docs = all(Query::from("docs"))?;
        let mut doc_bytes = 0usize;
        for row in &docs {
            if let Some((_, Some(Value::String(body)))) =
                row.cells().find(|(name, _)| *name == "body")
            {
                doc_bytes += body.len();
            }
        }
        let docs_ms = ms(start);
        Ok(result(&[
            ("rows", rows as f64),
            ("scanMs", scan_ms),
            ("doneRows", done as f64),
            ("filterMs", filter_ms),
            ("docs", docs.len() as f64),
            ("docBytes", doc_bytes as f64),
            ("docsMs", docs_ms),
        ]))
    })
}

/// Wait for the backing storage, then close.
#[wasm_bindgen(js_name = browserBenchClose)]
pub async fn browser_bench_close() -> Result<JsValue, JsValue> {
    let opened = OPEN.with(|open| open.borrow_mut().take());
    let Some((db, store)) = opened else {
        return Ok(result(&[]));
    };
    let start = Instant::now();
    if let Some(store) = &store {
        store.flush().await?;
    }
    let flush_ms = ms(start);
    let db = Rc::try_unwrap(db).map_err(|_| err("still in use"))?;
    db.close().await.map_err(err)?;
    Ok(result(&[("flushMs", flush_ms)]))
}

#[wasm_bindgen(js_name = browserBenchDestroy)]
pub async fn browser_bench_destroy(storage: JsValue) -> Result<(), JsValue> {
    let spec = StoreSpec::from_js(&storage, "bench")?;
    browser::destroy(&spec).await
}
