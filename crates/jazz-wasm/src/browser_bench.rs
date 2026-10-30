//! Browser store comparison probes (`browser-bench` feature, never in the
//! shipping bundle): the same workload on the IndexedDB and OPFS VFSes, or
//! in memory, timed from inside the worker.

use std::cell::RefCell;
use std::collections::BTreeMap;

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
    static OPEN: RefCell<Option<(Db, Option<Store>)>> = const { RefCell::new(None) };
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
    OPEN.with(|open| *open.borrow_mut() = Some(opened));
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
        for i in 0..n {
            jazz::db::block_on(db.insert(
                "todos",
                BTreeMap::from([
                    ("title".to_owned(), Value::String(format!("todo {i:06}"))),
                    ("done".to_owned(), Value::Bool(i % 3 == 0)),
                    ("n".to_owned(), Value::I64(i as i64)),
                ]),
                Default::default(),
            ))
            .map_err(err)?;
        }
        let total = ms(start);
        Ok(result(&[
            ("writeMs", total),
            ("perWriteUs", total * 1000.0 / n.max(1) as f64),
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
    db.close().await.map_err(err)?;
    Ok(result(&[("flushMs", flush_ms)]))
}

#[wasm_bindgen(js_name = browserBenchDestroy)]
pub async fn browser_bench_destroy(storage: JsValue) -> Result<(), JsValue> {
    let spec = StoreSpec::from_js(&storage, "bench")?;
    browser::destroy(&spec).await
}
