//! Durable Object KV store probes (`do-kv-bench` feature, never in a
//! shipping bundle): a write/read workload on [`crate::do_kv`], timed from
//! inside the object, with the storage operations it made. The workerd
//! smoke test (`tests/durable-object-kv`) drives it.

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

use crate::do_kv::{self, Store};
use jazz::kv_vfs::KvStats;

thread_local! {
    static OPEN: RefCell<Option<(Db, Store)>> = const { RefCell::new(None) };
}

fn config() -> DbConfig {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("n", ColumnType::BigInt)
                    .policies(policies),
            )
            .build(),
    )
    .expect("bench schema");
    DbConfig::new(
        schema,
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

fn with_open<T>(f: impl FnOnce(&Db, &Store) -> Result<T, JsValue>) -> Result<T, JsValue> {
    OPEN.with(|open| {
        let open = open.borrow();
        let (db, store) = open.as_ref().ok_or_else(|| err("not open"))?;
        f(db, store)
    })
}

/// Storage operations between `before` and `after`.
fn ops(before: KvStats, after: KvStats) -> [(&'static str, f64); 4] {
    [
        ("gets", (after.gets - before.gets) as f64),
        ("puts", (after.puts - before.puts) as f64),
        ("deletes", (after.deletes - before.deletes) as f64),
        ("commits", (after.commits - before.commits) as f64),
    ]
}

/// Open the object's store; a new one gets `page_size` (default: Jazz's).
#[wasm_bindgen(js_name = doKvBenchOpen)]
pub async fn do_kv_bench_open(ctx: JsValue, page_size: Option<u32>) -> Result<JsValue, JsValue> {
    let start = Instant::now();
    let page_size = page_size.unwrap_or(jazz::kv_vfs::PAGE_SIZE);
    let (db, store) = do_kv::open_with_page_size(ctx, config(), page_size).await?;
    let open_ms = ms(start);
    let mut out = vec![("openMs", open_ms)];
    out.extend(ops(KvStats::default(), store.stats()));
    OPEN.with(|open| *open.borrow_mut() = Some((db, store)));
    Ok(result(&out))
}

/// `n` todos from `first`, one commit each (the normal write path).
#[wasm_bindgen(js_name = doKvBenchWrite)]
pub fn do_kv_bench_write(first: u32, n: u32) -> Result<JsValue, JsValue> {
    with_open(|db, store| {
        let before = store.stats();
        let start = Instant::now();
        for i in first..first + n {
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
        let mut out = vec![
            ("writeMs", total),
            ("rowsPerS", n as f64 / (total / 1000.0)),
        ];
        out.extend(ops(before, store.stats()));
        Ok(result(&out))
    })
}

/// A full scan of todos and a filtered read.
#[wasm_bindgen(js_name = doKvBenchRead)]
pub fn do_kv_bench_read() -> Result<JsValue, JsValue> {
    with_open(|db, store| {
        let all = |query: Query| -> Result<usize, JsValue> {
            let prepared = db.prepare_query(&query).map_err(err)?;
            Ok(jazz::db::block_on(db.all(&prepared, ReadOpts::default()))
                .map_err(err)?
                .len())
        };
        let before = store.stats();
        let start = Instant::now();
        let rows = all(Query::from("todos"))?;
        let scan_ms = ms(start);
        let start = Instant::now();
        let done = all(Query::from("todos").filter(eq(col("done"), lit(Value::Bool(true)))))?;
        let filter_ms = ms(start);
        let mut out = vec![
            ("rows", rows as f64),
            ("scanMs", scan_ms),
            ("rowsPerS", rows as f64 / (scan_ms / 1000.0)),
            ("doneRows", done as f64),
            ("filterMs", filter_ms),
        ];
        out.extend(ops(before, store.stats()));
        Ok(result(&out))
    })
}

/// Close the store (it is on disk already: nothing to flush).
#[wasm_bindgen(js_name = doKvBenchClose)]
pub async fn do_kv_bench_close() -> Result<(), JsValue> {
    let opened = OPEN.with(|open| open.borrow_mut().take());
    if let Some((db, store)) = opened {
        db.close().await.map_err(err)?;
        drop(db);
        drop(store);
    }
    Ok(())
}

/// Delete the object's store (closed).
#[wasm_bindgen(js_name = doKvBenchDestroy)]
pub fn do_kv_bench_destroy(ctx: JsValue) -> Result<(), JsValue> {
    do_kv::destroy(ctx)
}
