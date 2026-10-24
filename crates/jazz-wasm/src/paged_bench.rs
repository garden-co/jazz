//! Preload vs page-fault-as-retry probes (`browser-bench` feature, never in
//! the shipping bundle). The same file in IndexedDB opens either through
//! the shipping `jazz-idb` VFS (whole file preloaded) or through the paged
//! spike in [`crate::idb_paged`], and the same app-shaped operations run on
//! it: a text-editor document read, a chat room's first and second page,
//! point reads and single-row commits.
//!
//! On the paged VFS every operation runs in a retry loop: a read of a page
//! that is not resident fails the operation, the recorded pages load in one
//! IndexedDB transaction, and the operation runs again.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::path::Path;

use jazz::db::{Db, DbConfig, DbIdentity, Error, FileOptions, InsertOptions, Journal, ReadOpts};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{col, eq, lit, lt, OrderDirection, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use wasm_bindgen::prelude::*;
use web_time::Instant;

use crate::browser::{self, Store, StoreSpec};
use crate::idb_paged::{self, HintPolicy, PagedUtil, PAGED_VFS};

const PAGE: u32 = 65536;
const ROOM_PAGE: usize = 50;
/// A faulting operation that needs more rounds than this is a bug.
const MAX_ROUNDS: u32 = 10_000;

enum Mode {
    Preload(Store),
    Paged(PagedUtil),
}

thread_local! {
    static OPEN: RefCell<Option<(Db, Mode)>> = const { RefCell::new(None) };
    /// Operations whose run succeeded although a read faulted inside it:
    /// something swallowed the I/O error. Writes cannot be retried then.
    static SWALLOWED: Cell<u32> = const { Cell::new(0) };
    /// The open file, for saving its hot-page hint at close.
    static PATH: RefCell<String> = const { RefCell::new(String::new()) };
    static HINT_BYTES: Cell<usize> = const { Cell::new(0) };
    /// [`paged_bench_hint_timing`]'s settings: store the hint every this
    /// many ms, age LFU every this many ms.
    static HINT_TIMING: Cell<(f64, f64)> = const { Cell::new((5000.0, 3_600_000.0)) };
}

/// How often the next opens store their hint, and how long an LFU epoch
/// lasts (ms).
#[wasm_bindgen(js_name = pagedBenchHintTiming)]
pub fn paged_bench_hint_timing(every_ms: f64, epoch_ms: f64) {
    HINT_TIMING.with(|t| t.set((every_ms, epoch_ms)));
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
                TableSchemaBuilder::new("messages")
                    .column("room", ColumnType::Text)
                    .column("author", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .column("at", ColumnType::BigInt)
                    .composite_index(["room", "at"])
                    .index_only(Vec::<String>::new())
                    .policies(policies.clone()),
            )
            .table(
                TableSchemaBuilder::new("logs")
                    .column("content", ColumnType::Bytea)
                    .policies(policies),
            )
            .build(),
    )
    .expect("paged bench schema")
}

fn config() -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([2; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn err(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn quantile(samples: &mut [f64], q: f64) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples
        .get(((samples.len() as f64 - 1.0) * q).round() as usize)
        .copied()
        .unwrap_or(0.0)
}

fn id(kind: u8, n: u64) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn room(r: u64) -> Value {
    Value::String(format!("room-{r:05}"))
}

fn message(i: u64, rooms: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("room".to_owned(), room(i % rooms)),
        (
            "author".to_owned(),
            Value::String(format!("user-{:04}", i % 97)),
        ),
        (
            "body".to_owned(),
            Value::String(format!(
                "message {i}: the quick brown fox jumps over the lazy dog, {} and so on",
                i.wrapping_mul(2654435761) % 1_000_000
            )),
        ),
        ("at".to_owned(), Value::I64(i as i64)),
    ])
}

struct Out(js_sys::Object);

impl Out {
    fn new() -> Self {
        Self(js_sys::Object::new())
    }

    fn set(&self, key: &str, value: f64) {
        let _ = js_sys::Reflect::set(&self.0, &JsValue::from_str(key), &JsValue::from_f64(value));
    }
}

/// One operation and what it cost.
struct Ran<T> {
    value: T,
    ms: f64,
    rounds: u32,
    idb_txs: u64,
}

fn paged_util() -> Option<PagedUtil> {
    OPEN.with(|open| match open.borrow().as_ref() {
        Some((_, Mode::Paged(util))) => Some(*util),
        _ => None,
    })
}

/// Run `f` against the open database. On the paged VFS a faulting run is
/// rolled back by the caller's error path, the missing pages load, and `f`
/// runs again. `retry_success` says whether a run that succeeded despite a
/// fault may run again (reads) or not (writes: it committed).
async fn run<T>(
    retry_success: bool,
    mut f: impl FnMut(&Db) -> Result<T, Error>,
) -> Result<Ran<T>, JsValue> {
    let util = paged_util();
    let start = Instant::now();
    let txs_before = util.map(|u| u.stats().idb_txs).unwrap_or(0);
    let mut rounds = 0;
    if let Some(util) = util {
        util.begin_op();
    }
    let value = loop {
        let result = OPEN.with(|open| {
            let open = open.borrow();
            let (db, _) = open.as_ref().ok_or_else(|| err("not open"))?;
            Ok::<_, JsValue>(f(db))
        })?;
        let Some(util) = util else {
            break result.map_err(err)?;
        };
        if !util.faulted() {
            break result.map_err(err)?;
        }
        if result.is_ok() {
            SWALLOWED.with(|s| s.set(s.get() + 1));
            if !retry_success {
                util.load().await?;
                break result.map_err(err)?;
            }
        }
        rounds += 1;
        if rounds > MAX_ROUNDS {
            util.end_op();
            return Err(err("too many fault rounds"));
        }
        util.load().await?;
    };
    if let Some(util) = util {
        util.end_op();
    }
    Ok(Ran {
        value,
        ms: ms(start),
        rounds,
        idb_txs: util.map(|u| u.stats().idb_txs).unwrap_or(0) - txs_before,
    })
}

fn spec(path: &str) -> Result<StoreSpec, JsValue> {
    let storage = js_sys::Object::new();
    js_sys::Reflect::set(&storage, &"backend".into(), &"indexeddb".into())?;
    js_sys::Reflect::set(&storage, &"path".into(), &path.into())?;
    StoreSpec::from_js(&storage.into(), "paged-bench")
}

fn absolute(path: &str) -> String {
    if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    }
}

/// Open `path` with `mode` "preload" or "paged". `budget_kib` is the paged
/// VFS's page budget (0: unlimited) and `readahead` the pages loaded after
/// each missing one.
#[wasm_bindgen(js_name = pagedBenchOpen)]
pub async fn paged_bench_open(
    path: String,
    mode: String,
    budget_kib: f64,
    readahead: u32,
    hot_kib: f64,
    policy: String,
) -> Result<JsValue, JsValue> {
    let out = Out::new();
    let start = Instant::now();
    let (db, mode) = match mode.as_str() {
        "preload" => {
            let (db, store) = browser::open(&spec(&path)?, config()).await?;
            (db, Mode::Preload(store))
        }
        "paged" => {
            let util = idb_paged::install().await?;
            let budget = if budget_kib > 0.0 {
                (budget_kib * 1024.0) as usize
            } else {
                usize::MAX
            };
            let policy = match policy.as_str() {
                "mru" => HintPolicy::Mru,
                "lfu" => HintPolicy::LfuHalving,
                "lfu-slow" => HintPolicy::LfuSlow,
                "sieve" | "" => HintPolicy::Sieve,
                other => return Err(err(format!("hint policy {other:?} is not supported"))),
            };
            util.configure(budget, readahead as usize, policy);
            let (every, epoch) = HINT_TIMING.with(Cell::get);
            util.configure_hint(every, epoch, 4096);
            util.reset_stats();
            let path = absolute(&path);
            PATH.with(|p| *p.borrow_mut() = path.clone());
            let hot_start = Instant::now();
            util.attach(&path, (hot_kib * 1024.0) as usize).await?;
            out.set("attachMs", ms(hot_start));
            out.set("hotPages", util.stats().hot_pages as f64);
            let mut rounds = 0;
            util.begin_op();
            let db = loop {
                let opened = Db::open_at(
                    config(),
                    Path::new(&path),
                    FileOptions {
                        vfs: Some(PAGED_VFS),
                        journal: Journal::Memory,
                        page_size: Some(PAGE),
                        // The VFS's budget is the cache under test; SQLite's
                        // own stays small on top of it.
                        cache_kib: 1024,
                        ..FileOptions::default()
                    },
                )
                .await;
                if !util.faulted() {
                    break opened.map_err(err)?;
                }
                if opened.is_ok() {
                    SWALLOWED.with(|s| s.set(s.get() + 1));
                }
                drop(opened);
                rounds += 1;
                if rounds > MAX_ROUNDS {
                    return Err(err("too many fault rounds opening"));
                }
                util.load().await?;
            };
            util.end_op();
            out.set("openRounds", rounds as f64);
            out.set("openIdbTxs", util.stats().idb_txs as f64);
            (db, Mode::Paged(util))
        }
        other => return Err(err(format!("mode {other:?} is not supported"))),
    };
    out.set("openMs", ms(start));
    OPEN.with(|open| *open.borrow_mut() = Some((db, mode)));
    paged_stats(&out);
    Ok(out.0.into())
}

fn paged_stats(out: &Out) {
    if let Some(util) = paged_util() {
        let stats = util.stats();
        out.set("residentBytes", util.resident() as f64);
        out.set("peakResidentBytes", stats.peak_resident as f64);
        out.set("faults", stats.faults as f64);
        out.set("rounds", stats.rounds as f64);
        out.set("idbTxs", stats.idb_txs as f64);
        out.set("pagesLoaded", stats.pages_loaded as f64);
        out.set("pagesEvicted", stats.pages_evicted as f64);
        out.set("recordCounts", stats.record_counts as f64);
        let (hinted, used, touched) = util.hint_use();
        out.set("hintPages", hinted as f64);
        out.set("hintPagesUsed", used as f64);
        out.set("pagesTouched", touched as f64);
        out.set("hintPersists", stats.hint_persists as f64);
        out.set("hintPersistBytes", stats.hint_persist_bytes as f64);
        out.set("hintSoloPersists", stats.hint_solo_persists as f64);
        out.set("hintSoloMs", stats.hint_solo_ms);
        out.set("hintBuildMs", stats.hint_build_ms);
    }
    out.set("swallowed", SWALLOWED.with(Cell::get) as f64);
}

/// Seed (on the preload VFS): `messages` chat messages over `rooms` rooms,
/// and `logs` copies of `fixture` (each made distinct, so large-value
/// chunks do not deduplicate).
#[wasm_bindgen(js_name = pagedBenchSeed)]
pub async fn paged_bench_seed(
    path: String,
    messages: f64,
    rooms: f64,
    logs: u32,
    fixture: Vec<u8>,
) -> Result<JsValue, JsValue> {
    browser::destroy(&spec(&path)?).await?;
    let (db, store) = browser::open(&spec(&path)?, config()).await?;
    let start = Instant::now();
    let (messages, rooms) = (messages as u64, rooms as u64);
    let batch = 500;
    let mut i = 0;
    while i < messages {
        let end = (i + batch).min(messages);
        db.transaction(async |tx| {
            for k in i..end {
                tx.insert(
                    "messages",
                    message(k, rooms),
                    InsertOptions {
                        row_id: Some(id(3, k)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        })
        .await
        .map_err(err)?;
        i = end;
        if i % 50_000 == 0 {
            store.flush().await?;
        }
    }
    let messages_ms = ms(start);
    let start = Instant::now();
    for k in 0..logs {
        let content: Vec<u8> = fixture.iter().map(|b| b ^ (k as u8)).collect();
        db.insert(
            "logs",
            BTreeMap::from([("content".to_owned(), Value::Bytes(content))]),
            InsertOptions {
                row_id: Some(id(4, k as u64)),
                ..Default::default()
            },
        )
        .await
        .map_err(err)?;
        store.flush().await?;
    }
    let logs_ms = ms(start);
    store.flush().await?;
    db.close().await.map_err(err)?;
    let out = Out::new();
    out.set("messagesMs", messages_ms);
    out.set("logsMs", logs_ms);
    Ok(out.0.into())
}

/// The text-editor reload: one log read back whole.
#[wasm_bindgen(js_name = pagedBenchDoc)]
pub async fn paged_bench_doc(k: u32) -> Result<JsValue, JsValue> {
    let query = Query::from("logs").filter(eq(col("id"), lit(Value::Uuid(id(4, k as u64).0))));
    let ran = run(true, |db| {
        let prepared = db.prepare_query(&query)?;
        let rows = jazz::db::block_on(db.all(&prepared, ReadOpts::default()))?;
        let mut bytes = 0;
        for row in &rows {
            if let Some((_, Some(Value::Bytes(content)))) =
                row.cells().find(|(name, _)| *name == "content")
            {
                bytes += content.len();
            }
        }
        Ok(bytes)
    })
    .await?;
    let out = Out::new();
    out.set("docBytes", ran.value as f64);
    out.set("docMs", ran.ms);
    out.set("docRounds", ran.rounds as f64);
    out.set("docIdbTxs", ran.idb_txs as f64);
    paged_stats(&out);
    Ok(out.0.into())
}

fn room_query(r: u64, before: Option<i64>) -> Query {
    let mut query = Query::from("messages").filter(eq(col("room"), lit(room(r))));
    if let Some(before) = before {
        query = query.filter(lt(col("at"), lit(Value::I64(before))));
    }
    query.order_by("at", OrderDirection::Desc).limit(ROOM_PAGE)
}

/// The chat room open: its newest page, then the page before it.
#[wasm_bindgen(js_name = pagedBenchRoom)]
pub async fn paged_bench_room(r: f64) -> Result<JsValue, JsValue> {
    let r = r as u64;
    let page = |before: Option<i64>| {
        let query = room_query(r, before);
        move |db: &Db| {
            let prepared = db.prepare_query(&query)?;
            let rows = jazz::db::block_on(db.all(&prepared, ReadOpts::default()))?;
            let oldest = rows
                .iter()
                .filter_map(|row| match row.cells().find(|(name, _)| *name == "at") {
                    Some((_, Some(Value::I64(at)))) => Some(*at),
                    _ => None,
                })
                .min();
            Ok((rows.len(), oldest))
        }
    };
    let first = run(true, page(None)).await?;
    let second = run(true, page(first.value.1)).await?;
    let out = Out::new();
    out.set("page1Rows", first.value.0 as f64);
    out.set("page1Ms", first.ms);
    out.set("page1Rounds", first.rounds as f64);
    out.set("page1IdbTxs", first.idb_txs as f64);
    out.set("page2Rows", second.value.0 as f64);
    out.set("page2Ms", second.ms);
    out.set("page2Rounds", second.rounds as f64);
    out.set("page2IdbTxs", second.idb_txs as f64);
    paged_stats(&out);
    Ok(out.0.into())
}

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n.max(1)
    }
}

fn summary(out: &Out, name: &str, ran: &mut [Ran<()>]) {
    let mut times: Vec<f64> = ran.iter().map(|r| r.ms).collect();
    let n = ran.len().max(1) as f64;
    out.set(&format!("{name}P50Ms"), quantile(&mut times, 0.5));
    out.set(&format!("{name}P95Ms"), quantile(&mut times, 0.95));
    out.set(
        &format!("{name}RoundsPerOp"),
        ran.iter().map(|r| r.rounds as f64).sum::<f64>() / n,
    );
    out.set(
        &format!("{name}IdbTxsPerOp"),
        ran.iter().map(|r| r.idb_txs as f64).sum::<f64>() / n,
    );
}

/// `n` point reads of random messages (by id) out of `messages`, then the
/// same ids again.
#[wasm_bindgen(js_name = pagedBenchPoints)]
pub async fn paged_bench_points(n: u32, messages: f64, seed: f64) -> Result<JsValue, JsValue> {
    let mut rng = Lcg(seed as u64);
    let ids: Vec<u64> = (0..n).map(|_| rng.below(messages as u64)).collect();
    let mut passes = Vec::new();
    for _ in 0..2 {
        let mut ran = Vec::with_capacity(ids.len());
        for &k in &ids {
            let query = Query::from("messages").filter(eq(col("id"), lit(Value::Uuid(id(3, k).0))));
            let one = run(true, |db| {
                let prepared = db.prepare_query(&query)?;
                let rows = jazz::db::block_on(db.all(&prepared, ReadOpts::default()))?;
                if rows.len() == 1 {
                    Ok(())
                } else {
                    Err(Error::new(
                        jazz::db::ErrorCode::Storage,
                        format!("message {k}: {} rows", rows.len()),
                    ))
                }
            })
            .await?;
            ran.push(Ran {
                value: (),
                ms: one.ms,
                rounds: one.rounds,
                idb_txs: one.idb_txs,
            });
        }
        passes.push(ran);
    }
    let out = Out::new();
    summary(&out, "cold", &mut passes[0]);
    summary(&out, "warm", &mut passes[1]);
    paged_stats(&out);
    Ok(out.0.into())
}

/// `n` single-row commits into random rooms, then a flush.
#[wasm_bindgen(js_name = pagedBenchWrites)]
pub async fn paged_bench_writes(
    n: u32,
    messages: f64,
    rooms: f64,
    seed: f64,
) -> Result<JsValue, JsValue> {
    let mut rng = Lcg(seed as u64);
    let base = messages as u64 + 1_000_000 * (seed as u64 % 1000 + 1);
    let mut ran = Vec::with_capacity(n as usize);
    for i in 0..n as u64 {
        let k = base + i;
        let mut cells = message(k, rooms as u64);
        cells.insert("room".to_owned(), room(rng.below(rooms as u64)));
        let one = run(false, |db| {
            jazz::db::block_on(db.insert(
                "messages",
                cells.clone(),
                InsertOptions {
                    row_id: Some(id(3, k)),
                    ..Default::default()
                },
            ))
            .map(|_| ())
        })
        .await?;
        ran.push(one);
    }
    let start = Instant::now();
    flush().await?;
    let out = Out::new();
    summary(&out, "write", &mut ran);
    out.set("flushMs", ms(start));
    paged_stats(&out);
    Ok(out.0.into())
}

async fn flush() -> Result<(), JsValue> {
    let store = OPEN.with(|open| match open.borrow().as_ref() {
        Some((_, Mode::Preload(store))) => Some(store.clone()),
        _ => None,
    });
    match (store, paged_util()) {
        (Some(store), _) => store.flush().await,
        (None, Some(util)) => util.flush().await,
        (None, None) => Err(err("not open")),
    }
}

/// Count the rows (to check no retried write committed twice).
#[wasm_bindgen(js_name = pagedBenchCount)]
pub async fn paged_bench_count() -> Result<JsValue, JsValue> {
    let ran = run(true, |db| {
        let prepared = db.prepare_query(&Query::from("messages"))?;
        Ok(jazz::db::block_on(db.all(&prepared, ReadOpts::default()))?.len())
    })
    .await?;
    let out = Out::new();
    out.set("messages", ran.value as f64);
    out.set("countMs", ran.ms);
    out.set("countRounds", ran.rounds as f64);
    paged_stats(&out);
    Ok(out.0.into())
}

#[wasm_bindgen(js_name = pagedBenchClose)]
pub async fn paged_bench_close(hint_pages: u32) -> Result<JsValue, JsValue> {
    // Closing reads too (it can fault like any operation).
    let ran = run(true, |db| jazz::db::block_on(db.close())).await?;
    flush().await?;
    if let Some(util) = paged_util() {
        let path = PATH.with(|p| p.borrow().clone());
        let (every, epoch) = HINT_TIMING.with(Cell::get);
        util.configure_hint(every, epoch, hint_pages as usize);
        let bytes = util.save_hint(&path).await?;
        HINT_BYTES.with(|b| b.set(bytes));
    }
    let out = Out::new();
    paged_stats(&out);
    OPEN.with(|open| open.borrow_mut().take());
    out.set("closeMs", ran.ms);
    out.set("closeRounds", ran.rounds as f64);
    out.set("hintBytes", HINT_BYTES.with(Cell::get) as f64);
    Ok(out.0.into())
}
