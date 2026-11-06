//! Durable Object SQL storage: typed parameters, a forward-only cursor and
//! callback-owned transactions on one explicit storage owner. No SQLite C ABI
//! emulation, global current database or separately registered scope driver.
use crate::WasmDb;
use jazz::storage::{self, Backend, Cursor, Storage, Value};
use std::cell::RefCell;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[wasm_bindgen]
extern "C" {
    #[derive(Clone)]
    pub type ObjectStorage;
}

#[wasm_bindgen(inline_js = r#"const decoder = new TextDecoder();
const encoder = new TextEncoder();
let out = new Uint8Array(1 << 16);
let view = new DataView(out.buffer);
function room(pos, n) {
  if (pos + n <= out.length) return;
  let size = out.length * 2;
  while (size < pos + n) size *= 2;
  const next = new Uint8Array(size);
  next.set(out.subarray(0, pos));
  out = next;
  view = new DataView(out.buffer);
}
function text(pos, s) {
  room(pos, 5 + s.length * 3);
  out[pos] = 3;
  const { written } = encoder.encodeInto(s, out.subarray(pos + 5));
  view.setUint32(pos + 1, written, true);
  return pos + 5 + written;
}
function bytes(pos, b) {
  room(pos, 5 + b.length);
  out[pos] = 4;
  view.setUint32(pos + 1, b.length, true);
  out.set(b, pos + 5);
  return pos + 5 + b.length;
}
function args(input) {
  const v = new DataView(input.buffer, input.byteOffset, input.byteLength);
  const list = [];
  let i = 0;
  while (i < input.length) {
    const tag = input[i++];
    if (tag === 0) {
      list.push(null);
    } else if (tag === 1) {
      list.push(v.getFloat64(i, true));
      i += 8;
    } else {
      const n = v.getUint32(i, true);
      i += 4;
      const part = input.subarray(i, i + n);
      i += n;
      list.push(tag === 3 ? decoder.decode(part) : part.slice().buffer);
    }
  }
  return list;
}
// The marker belongs to this invocation, so nested scopes cannot swallow
// one another's rollback or a real storage exception.
export function jazz_do_scope(storage, body) {
  const rollback = {};
  try {
    storage.transactionSync(() => {
      if (!body()) throw rollback;
    });
  } catch (error) {
    if (error !== rollback) throw error;
  }
}

export function jazz_do_query(storage, query, bindings) {
  const cursor = storage.sql.exec(query, ...args(bindings));
  return { cursor, rows: cursor.raw(), columns: cursor.columnNames };
}
export function jazz_do_columns(result) { return result.columns; }
export function jazz_do_next(result) {
  const { value: row, done } = result.rows.next();
  if (done) return new Uint8Array();
  room(0, 4);
  view.setUint32(0, row.length, true);
  let pos = 4;
  for (const cell of row) {
    if (cell === null) { room(pos, 1); out[pos++] = 0; }
    else if (typeof cell === "number") {
      if (Number.isInteger(cell) && !Number.isSafeInteger(cell)) throw new Error("wide SQL integers require exact projections");
      room(pos, 9); out[pos++] = Number.isInteger(cell) ? 1 : 2;
      view.setFloat64(pos, cell, true); pos += 8;
    } else if (typeof cell === "string") { pos = text(pos, cell); }
    else { pos = bytes(pos, new Uint8Array(cell)); }
  }
  return out.slice(0, pos);
}
export function jazz_do_counts(result) { return [result.cursor.rowsRead, result.cursor.rowsWritten]; }
"#)]
extern "C" {
    #[wasm_bindgen(catch)]
    fn jazz_do_scope(storage: &ObjectStorage, body: &js_sys::Function) -> Result<(), JsValue>;
    #[wasm_bindgen(catch)]
    fn jazz_do_query(
        storage: &ObjectStorage,
        query: &str,
        bindings: &[u8],
    ) -> Result<JsValue, JsValue>;
    fn jazz_do_columns(result: &JsValue) -> js_sys::Array;
    #[wasm_bindgen(catch)]
    fn jazz_do_next(result: &JsValue) -> Result<Vec<u8>, JsValue>;
    fn jazz_do_counts(result: &JsValue) -> js_sys::Array;
}

struct ObjectSql {
    storage: ObjectStorage,
}
struct ObjectCursor {
    result: JsValue,
    columns: Vec<String>,
    row: Vec<Value>,
}
impl Cursor for ObjectCursor {
    fn columns(&self) -> &[String] {
        &self.columns
    }
    fn next(&mut self) -> storage::Result<Option<&[Value]>> {
        let bytes = jazz_do_next(&self.result).map_err(sql_error)?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let mut cells = Cells {
            bytes: &bytes,
            at: 0,
        };
        let n = cells.u32().map_err(bridge_error)?;
        self.row.clear();
        for _ in 0..n {
            self.row.push(cells.value().map_err(bridge_error)?);
        }
        Ok(Some(&self.row))
    }
}
impl Drop for ObjectCursor {
    fn drop(&mut self) {
        let counts = jazz_do_counts(&self.result);
        STATS.with(|s| {
            let mut s = s.borrow_mut();
            s.rows_read += counts.get(0).as_f64().unwrap_or(0.0);
            s.rows_written += counts.get(1).as_f64().unwrap_or(0.0);
        });
    }
}
impl ObjectSql {
    fn cursor(&self, query: &str, bindings: &[Value]) -> storage::Result<ObjectCursor> {
        traced(query);
        let result = jazz_do_query(&self.storage, query, &encode(bindings)?).map_err(sql_error)?;
        STATS.with(|s| s.borrow_mut().execs += 1.0);
        let columns = jazz_do_columns(&result)
            .iter()
            .map(|v| v.as_string().unwrap())
            .collect();
        Ok(ObjectCursor {
            result,
            columns,
            row: Vec::new(),
        })
    }
    fn scalar(&self, query: &str) -> storage::Result<i64> {
        STATS.with(|s| s.borrow_mut().lookups += 1.0);
        let mut cursor = self.cursor(query, &[])?;
        match cursor.next()?.and_then(|row| row.first()) {
            Some(Value::Integer(n)) => Ok(*n),
            _ => Err(bridge_error("expected integer SQL result".into())),
        }
    }
}
impl Backend for ObjectSql {
    fn query(&self, sql: &str, parameters: &[Value]) -> storage::Result<Box<dyn Cursor + '_>> {
        Ok(Box::new(self.cursor(sql, parameters)?))
    }
    fn execute(&self, sql: &str, parameters: &[Value]) -> storage::Result<usize> {
        let mut cursor = self.cursor(sql, parameters)?;
        while cursor.next()?.is_some() {}
        drop(cursor);
        let first = sql
            .trim_start()
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if matches!(
            first.as_str(),
            "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "WITH"
        ) {
            Ok(self.scalar("SELECT changes()")? as usize)
        } else {
            Ok(0)
        }
    }
    fn execute_script(&self, script: &str) -> storage::Result<()> {
        let mut start = 0;
        for (end, _) in script.match_indices(';') {
            let statement = &script[start..=end];
            if complete(statement) {
                self.execute(statement, &[])?;
                start = end + 1;
            }
        }
        if !script[start..].trim().is_empty() {
            self.execute(&script[start..], &[])?;
        }
        Ok(())
    }
    fn transaction(&self, body: &mut dyn FnMut() -> bool) -> storage::Result<()> {
        STATS.with(|s| s.borrow_mut().savepoints += 1.0);
        let mut callback = || body();
        let closure = wasm_bindgen::closure::ScopedClosure::borrow_mut_aborting(&mut callback);
        jazz_do_scope(&self.storage, closure.as_ref().unchecked_ref()).map_err(sql_error)
    }
    fn last_insert_rowid(&self) -> storage::Result<i64> {
        self.scalar("SELECT last_insert_rowid()")
    }
    fn total_changes(&self) -> storage::Result<u64> {
        self.scalar("SELECT total_changes()").map(|n| n as u64)
    }
    fn max_parameters(&self) -> usize {
        100
    }
}
fn bridge_error(message: String) -> storage::Error {
    storage::backend_error(1, message)
}
fn sql_error(error: JsValue) -> storage::Error {
    let message = js_message(&error);
    storage::backend_error(error_code(&message), message)
}
const MAX_SAFE: i64 = (1 << 53) - 1;
const SQLITE_ERROR: i32 = 1;
const SQLITE_INTERRUPT: i32 = 9;
const SQLITE_TOOBIG: i32 = 18;
const SQLITE_CONSTRAINT: i32 = 19;
const SQLITE_CONSTRAINT_CHECK: i32 = 275;
const SQLITE_CONSTRAINT_FOREIGNKEY: i32 = 787;
const SQLITE_CONSTRAINT_NOTNULL: i32 = 1299;
const SQLITE_CONSTRAINT_PRIMARYKEY: i32 = 1555;
const SQLITE_CONSTRAINT_UNIQUE: i32 = 2067;
struct Cells<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cells<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], String> {
        let part = self
            .bytes
            .get(self.at..self.at + n)
            .ok_or("Durable Object SQLite result is cut short")?;
        self.at += n;
        Ok(part)
    }

    fn u32(&mut self) -> Result<usize, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize)
    }

    fn f64(&mut self) -> Result<f64, String> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn value(&mut self) -> Result<Value, String> {
        Ok(match self.take(1)?[0] {
            0 => Value::Null,
            1 => {
                let n = self.f64()?;
                if n.abs() > MAX_SAFE as f64 {
                    return Err(format!(
                        "the integer {n} is beyond 2^53, which Durable Object SQLite returns inexactly: not supported yet"
                    ));
                }
                Value::Integer(n as i64)
            }
            2 => Value::Real(self.f64()?),
            3 => {
                let n = self.u32()?;
                let mut text = Vec::with_capacity(n + 1);
                text.extend_from_slice(self.take(n)?);
                Value::Text(text)
            }
            4 => {
                let n = self.u32()?;
                Value::Blob(self.take(n)?.to_vec())
            }
            _ => return Err("Durable Object SQLite result has an unknown cell".into()),
        })
    }
}

fn encode(bindings: &[Value]) -> storage::Result<Vec<u8>> {
    let mut out = Vec::new();
    for value in bindings {
        match value {
            Value::Null => out.push(0),
            Value::Integer(n) => {
                if !(-MAX_SAFE..=MAX_SAFE).contains(n) {
                    STATS.with(|stats| stats.borrow_mut().wide_int_binds += 1.0);
                    return Err(bridge_error(
                        "SQL INTEGER outside Durable Object safe integer range".into(),
                    ));
                }
                out.push(1);
                out.extend_from_slice(&(*n as f64).to_le_bytes());
            }
            Value::Real(n) => {
                out.push(1);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Value::Text(bytes) | Value::Blob(bytes) => {
                let text = matches!(value, Value::Text(_));
                if text {
                    std::str::from_utf8(bytes)
                        .map_err(|_| bridge_error("non-UTF-8 SQL text".into()))?;
                }
                out.push(if text { 3 } else { 4 });
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }
    Ok(out)
}
fn js_message(error: &JsValue) -> String {
    error
        .dyn_ref::<js_sys::Error>()
        .map(|error| String::from(error.message()))
        .or_else(|| error.as_string())
        .unwrap_or_else(|| format!("{error:?}"))
}

/// The SQLite result code a Durable Object's error message names.
fn error_code(message: &str) -> i32 {
    if message.contains("UNIQUE constraint failed") {
        SQLITE_CONSTRAINT_UNIQUE
    } else if message.contains("PRIMARY KEY") && message.contains("constraint") {
        SQLITE_CONSTRAINT_PRIMARYKEY
    } else if message.contains("NOT NULL constraint failed") {
        SQLITE_CONSTRAINT_NOTNULL
    } else if message.contains("FOREIGN KEY constraint failed") {
        SQLITE_CONSTRAINT_FOREIGNKEY
    } else if message.contains("CHECK constraint failed") {
        SQLITE_CONSTRAINT_CHECK
    } else if message.contains("SQLITE_CONSTRAINT") {
        SQLITE_CONSTRAINT
    } else if message.contains("interrupted") {
        SQLITE_INTERRUPT
    } else if message.contains("SQLITE_TOOBIG") || message.contains("too big") {
        SQLITE_TOOBIG
    } else {
        SQLITE_ERROR
    }
}

fn complete(query: &str) -> bool {
    let bytes = query.as_bytes();
    let mut words: Vec<String> = Vec::new();
    let mut ended = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'\'' | b'"' | b'`' => {
                i += 1;
                while i < bytes.len() && !(bytes[i] == c && bytes.get(i + 1) != Some(&c)) {
                    i += if bytes[i] == c { 2 } else { 1 };
                }
                ended = false;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 1;
            }
            b';' => {
                let trigger = matches!(
                    words.iter().map(String::as_str).collect::<Vec<_>>()[..],
                    ["CREATE", "TRIGGER", ..]
                        | ["CREATE", "TEMP", "TRIGGER", ..]
                        | ["CREATE", "TEMPORARY", "TRIGGER", ..]
                );
                // As SQLite's own: a trigger ends at `; END ;`.
                if !trigger || words.ends_with(&[";".to_string(), "END".to_string()]) {
                    ended = true;
                    words.clear();
                } else {
                    words.push(";".into());
                }
            }
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                words.push(query[start..i].to_ascii_uppercase());
                ended = false;
                continue;
            }
            c if c.is_ascii_whitespace() => {}
            _ => ended = false,
        }
        i += 1;
    }
    ended
}

thread_local! {
    static STATS: RefCell<Stats> = RefCell::new(Stats::default());
    static QUERIES: RefCell<Option<HashMap<String, f64>>> = const { RefCell::new(None) };
}
fn traced(query: &str) {
    QUERIES.with(|queries| {
        if let Some(queries) = queries.borrow_mut().as_mut() {
            let key: String = query.split_whitespace().collect::<Vec<_>>().join(" ");
            let key: String = key.chars().take(160).collect();
            *queries.entry(key).or_default() += 1.0;
        }
    });
}

/// Count execs by statement text from now on (`on`), or stop.
#[wasm_bindgen(js_name = durableObjectSqlTrace)]
pub fn durable_object_sql_trace(on: bool) {
    QUERIES.with(|queries| *queries.borrow_mut() = on.then(HashMap::new));
}

/// The execs counted since tracing started, `{ text: count }`, and start
/// counting again.
#[wasm_bindgen(js_name = durableObjectSqlQueries)]
pub fn durable_object_sql_queries() -> JsValue {
    let out = js_sys::Object::new();
    QUERIES.with(|queries| {
        if let Some(queries) = queries.borrow_mut().as_mut() {
            for (key, count) in queries.drain() {
                let _ = js_sys::Reflect::set(&out, &key.into(), &count.into());
            }
        }
    });
    out.into()
}

#[derive(Default)]
struct Stats {
    execs: f64,
    rows_read: f64,
    rows_written: f64,
    /// Execs only to answer `changes()`, `total_changes()` or
    /// `last_insert_rowid()`.
    lookups: f64,
    savepoints: f64,
    wide_int_binds: f64,
}

/// What the Durable Object SQL backend did so far: `{ execs, rowsRead,
/// rowsWritten, lookups, savepoints, wideIntBinds }`.
#[wasm_bindgen(js_name = durableObjectSqlStats)]
pub fn durable_object_sql_stats() -> JsValue {
    STATS.with(|stats| {
        let stats = stats.borrow();
        let out = js_sys::Object::new();
        for (key, value) in [
            ("execs", stats.execs),
            ("rowsRead", stats.rows_read),
            ("rowsWritten", stats.rows_written),
            ("lookups", stats.lookups),
            ("savepoints", stats.savepoints),
            ("wideIntBinds", stats.wide_int_binds),
        ] {
            let _ = js_sys::Reflect::set(&out, &key.into(), &value.into());
        }
        out.into()
    })
}

// ---- SQLite's constants ----------------------------------------------------

#[wasm_bindgen]
impl WasmDb {
    /// Open the app's authority on a Durable Object's SQLite: `storage` is
    /// the object's `ctx.storage` (including `sql` and `transactionSync`).
    /// One per object; it writes as SYSTEM and serves subscribers, with
    /// nothing upstream.
    #[wasm_bindgen(js_name = openDurableObject)]
    pub fn open_durable_object(
        storage: ObjectStorage,
        schema: Vec<u8>,
        config: Vec<u8>,
    ) -> Result<WasmDb, JsValue> {
        use crate::binding_host;
        crate::initialize_wasm_tracing();
        let (schema, mut config) =
            binding_host::decode_open(&schema, &config).map_err(crate::host_error)?;
        let (identity, trusted) = binding_host::open_identity(&mut config, crate::OpenAs::Backend)
            .map_err(crate::host_error)?;
        let db_config = jazz::tools::deployment::published::with_client_migrations(
            binding_host::db_config(schema, &config, identity),
            config.migrations.as_deref(),
        )
        .map_err(|error| crate::host_error(binding_host::HostError::Argument(error)))?;
        let db = crate::block_on(jazz::db::Db::open_storage(
            db_config,
            Storage::from_backend(ObjectSql { storage }),
        ))
        .map_err(crate::to_js_error)?;
        db.set_authority();
        Ok(Self::runtime_owner(crate::Binding::new(
            db, identity, trusted,
        )))
    }
}

#[cfg(feature = "durable-object-tests")]
#[wasm_bindgen]
impl WasmDb {
    #[wasm_bindgen(js_name = checkRejectedRow)]
    pub fn check_rejected_row(&self, row: Vec<u8>) -> Result<(), JsValue> {
        let row = jazz::ids::RowUuid::from_bytes(
            row.try_into()
                .map_err(|_| JsValue::from_str("expected row UUID"))?,
        );
        self.binding()?
            .db
            .check_rejected_row_for_test("tasks", row)
            .map_err(crate::to_js_error)
    }

    #[wasm_bindgen(js_name = checkSqlScopes)]
    pub fn check_sql_scopes(&self) -> Result<(), JsValue> {
        let binding = self.binding()?;
        let db = &binding.db;
        db.check_sql_scopes_for_test().map_err(crate::to_js_error)?;
        // Unsupported lifetimes must fail before a callback runs or an
        // overlay modifies storage, rather than count fictitious savepoints.
        let error = crate::block_on(
            db.transaction::<()>(async |_| panic!("unsupported async transaction callback ran")),
        )
        .unwrap_err();
        assert_eq!(error.code, jazz::db::ErrorCode::Query);
        assert!(error.message.contains("SQL savepoint `tx`"));
        let id = jazz::tools::OpenTransactionId::new();
        crate::block_on(db.begin_mergeable(id)).map_err(crate::to_js_error)?;
        let query = db
            .prepare_query(&jazz::query::Query::from("tasks"))
            .map_err(crate::to_js_error)?;
        let error = crate::block_on(db.tx_ref(id).all_prepared(&query)).unwrap_err();
        assert_eq!(error.code, jazz::db::ErrorCode::Query);
        assert!(error.message.contains("SQL savepoint `overlay`"));
        db.abandon_transaction_handle(id)
            .map_err(crate::to_js_error)?;
        Ok(())
    }
}
