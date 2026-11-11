//! Durable Object SQL storage: typed parameters, a forward-only cursor and
//! callback-owned transactions on one explicit storage owner. No SQLite C ABI
//! emulation, global current database or separately registered scope driver.
//!
//! `do-value-bridge-v2` is transient Rust/JS framing, never stored or synced:
//! tags 0=NULL, 1=i64 LE, 2=f64 LE, 3=UTF-8 bytes, 4=blob bytes. Variable
//! payloads have a u32 LE length; result rows start with a u32 LE cell count.
//! Known result types decode JS numbers directly; unknown physical cells use
//! tagged SQL projections. Unsafe integers are refused. Database bytes are unchanged.
use crate::WasmDb;
use jazz::storage::{self, Backend, Cursor, ResultType, ResultTypes, Storage, Value};
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
  const types = [];
  let i = 0;
  while (i < input.length) {
    const tag = input[i++];
    types.push(tag);
    if (tag === 0) {
      list.push(null);
    } else if (tag === 1) {
      list.push(Number(v.getBigInt64(i, true)));
      i += 8;
    } else if (tag === 2) {
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
  return { list, types };
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
    // A rolled-back schema change can invalidate metadata learned inside it.
    projections.delete(storage);
    if (error !== rollback) throw error;
  }
}

// A bounded, per-owner projection cache. Metadata probes use LIMIT 0: no rows
// are consumed and no write statement can be nested in the read subquery.
const projections = new WeakMap();
const unknownResults = { positional: [], named: {} };
function exactQuery(storage, query, bindings, description) {
  const hints = description ? JSON.parse(description) : unknownResults;
  if (hints.positional.length && hints.positional.every(t => t !== "Dynamic")) {
    const cursor = storage.sql.exec(query, ...bindings);
    if (cursor.columnNames.length !== hints.positional.length) throw new Error("incorrect SQL result type count");
    return { cursor, rows: cursor.raw(), columns: cursor.columnNames, exact: true, types: hints.positional, probes: 0 };
  }
  const cacheKey = description ? query + "\0" + description : query;
  let cache = projections.get(storage);
  if (!cache) projections.set(storage, cache = { entries: new Map(), bytes: 0 });
  let entry = cache.entries.get(cacheKey);
  let probes = 0;
  if (!entry) {
    const metadata = storage.sql.exec(`SELECT * FROM (${query}\n) LIMIT 0`, ...bindings);
    const columns = metadata.columnNames;
    const types = columns.map((name, i) => hints.positional[i] ?? (Object.hasOwn(hints.named, name) ? hints.named[name] : "Dynamic"));
    const names = columns.map((_, i) => `c${i}`);
    // Avoid shadowing identifiers in the original statement.
    let alias = "_jazz_do_result";
    while (query.includes(alias)) alias += "_";
    const cells = names.map((c, i) => types[i] !== "Dynamic" ? c : `CASE typeof(${c}) WHEN 'integer' THEN CAST(X'01' || CAST(${c} AS TEXT) AS BLOB) WHEN 'text' THEN CAST(X'03' || CAST(${c} AS BLOB) AS BLOB) WHEN 'blob' THEN CAST(X'04' || ${c} AS BLOB) ELSE ${c} END`);
    const sql = `WITH ${alias}(${names.join(",")}) AS (${query}\n) SELECT ${cells.join(",")} FROM ${alias}`;
    entry = { sql: types.every(t => t !== "Dynamic") ? query : sql, columns, types };
    const size = 2 * (cacheKey.length + sql.length + columns.join("").length);
    while (cache.entries.size && (cache.entries.size >= 64 || cache.bytes + size > 1048576)) {
      const key = cache.entries.keys().next().value;
      cache.bytes -= cache.entries.get(key).size;
      cache.entries.delete(key);
    }
    if (size <= 1048576) {
      entry.size = size;
      cache.entries.set(cacheKey, entry);
      cache.bytes += size;
    }
    probes = 1;
  }
  const cursor = storage.sql.exec(entry.sql, ...bindings);
  return { cursor, rows: cursor.raw(), columns: entry.columns, types: entry.types, exact: true, probes };
}
// Rewrite parameter tokens only, never strings, identifiers, or comments.
// Jazz uses positional parameters. Refuse other parameter syntaxes explicitly.
function numericBindings(query, types) {
  let out = "", i = 0, highest = 0;
  while (i < query.length) {
    const start = i, c = query[i++];
    if (c === "'" || c === '"' || c === '`' || c === '[') {
      const end = c === '[' ? ']' : c;
      while (i < query.length) {
        if (query[i++] === end) {
          if (c !== '[' && query[i] === end) { i++; continue; }
          break;
        }
      }
    } else if (c === '-' && query[i] === '-') {
      while (i < query.length && query[i] !== '\n') i++;
    } else if (c === '/' && query[i] === '*') {
      i++;
      while (i < query.length && !(query[i-1] === '*' && query[i] === '/')) i++;
      if (i < query.length) i++;
    } else if (c === '?') {
      while (i < query.length && /[0-9]/.test(query[i])) i++;
      const index = i === start + 1 ? highest + 1 : Number(query.slice(start + 1, i));
      if (!Number.isSafeInteger(index) || index < 1 || index > types.length) throw new Error("invalid SQL parameter index");
      highest = Math.max(highest, index);
      out += types[index - 1] === 1 ? `+CAST(?${index} AS INTEGER)` : `?${index}`;
      continue;
    } else if (c === ':' || c === '@' || c === '$') {
      throw new Error("named SQL parameters are not supported by Durable Object storage");
    }
    out += query.slice(start, i);
  }
  if (highest !== types.length) throw new Error("incorrect SQL parameter count");
  // Read wrappers cannot contain a trailing terminator. Preserve newlines
  // before wrapper syntax, so a trailing line comment cannot swallow it.
  return out.trimEnd().replace(/;$/, '');
}
export function jazz_do_query(storage, query, bindings, exact, description) {
  const { list, types } = args(bindings);
  query = numericBindings(query, types);
  if (exact) {
    // Jazz introspection uses these read-only PRAGMAs. Their table-valued
    // forms can participate in the same typed projection as other reads.
    const info = /^\s*PRAGMA\s+table_info\s*\(\s*("(?:[^"]|"")*"|'(?:[^']|'')*'|[a-zA-Z_][a-zA-Z_0-9]*)\s*\)\s*$/i.exec(query);
    if (info) {
      let name = info[1];
      if (name[0] === '"' || name[0] === "'") {
        const quote = name[0];
        name = name.slice(1, -1).split(quote + quote).join(quote);
      }
      query = `SELECT * FROM pragma_table_info('${name.replaceAll("'", "''")}')`;
    } else if (/^\s*PRAGMA\s+schema_version\s*$/i.test(query)) {
      query = "SELECT * FROM pragma_schema_version";
    }
    if (/^\s*(INSERT|UPDATE|DELETE|REPLACE)\b/i.test(query)) {
      throw new Error("reading data-modifying RETURNING statements is not supported by Durable Object storage");
    }
    return exactQuery(storage, query, list, description);
  }
  // Schema changes invalidate SELECT * metadata. Ordinary writes keep it.
  if (!/^\s*(INSERT|UPDATE|DELETE|REPLACE)\b/i.test(query)) projections.delete(storage);
  const cursor = storage.sql.exec(query, ...list);
  return { cursor, rows: cursor.raw(), columns: cursor.columnNames, exact: false, probes: 0 };
}
export function jazz_do_probes(result) { return result.probes; }
export function jazz_do_columns(result) { return result.columns; }
export function jazz_do_next(result) {
  const { value: row, done } = result.rows.next();
  if (done) return new Uint8Array();
  // execute() drains results but never exposes their cells.
  if (!result.exact) return new Uint8Array(4);
  room(0, 4);
  view.setUint32(0, row.length, true);
  let pos = 4;
  for (let i = 0; i < row.length; i++) {
    const cell = row[i], type = result.types[i];
    if (cell === null) { room(pos, 1); out[pos++] = 0; }
    else if (typeof cell === "number") {
      if (type === "NonNumeric") throw new Error("unexpected numeric SQL result");
      room(pos, 9);
      if (type === "Integer") {
        if (!Number.isSafeInteger(cell)) throw new Error("SQL INTEGER outside Durable Object safe integer range");
        out[pos++] = 1; view.setBigInt64(pos, BigInt(cell), true);
      } else { out[pos++] = 2; view.setFloat64(pos, cell, true); }
      pos += 8;
    } else if (type !== "Dynamic") {
      const start = pos;
      pos = bytes(pos, typeof cell === "string" ? encoder.encode(cell) : new Uint8Array(cell));
      out[start] = typeof cell === "string" ? 3 : 4;
    } else {
      const b = new Uint8Array(cell), tag = b[0];
      if (tag === 1) {
        const n = BigInt(decoder.decode(b.subarray(1)));
        if (n < -9007199254740991n || n > 9007199254740991n) throw new Error("SQL INTEGER outside Durable Object safe integer range");
        room(pos, 9); out[pos++] = 1;
        view.setBigInt64(pos, n, true); pos += 8;
      } else if (tag === 3 || tag === 4) {
        const start = pos;
        pos = bytes(pos, b.subarray(1));
        out[start] = tag;
      } else { throw new Error("invalid typed SQL result"); }
    }
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
        exact: bool,
        description: &str,
    ) -> Result<JsValue, JsValue>;
    fn jazz_do_probes(result: &JsValue) -> u32;
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
    fn cursor(
        &self,
        query: &str,
        bindings: &[Value],
        exact: bool,
    ) -> storage::Result<ObjectCursor> {
        self.typed_cursor(query, bindings, exact, &ResultTypes::default())
    }
    fn typed_cursor(
        &self,
        query: &str,
        bindings: &[Value],
        exact: bool,
        types: &ResultTypes,
    ) -> storage::Result<ObjectCursor> {
        traced(query);
        let description = if types.positional.is_empty() && types.named.is_empty() {
            String::new()
        } else {
            serde_json::to_string(types).map_err(|e| bridge_error(e.to_string()))?
        };
        let result = jazz_do_query(
            &self.storage,
            query,
            &encode(bindings)?,
            exact,
            &description,
        )
        .map_err(sql_error)?;
        STATS.with(|s| s.borrow_mut().execs += 1.0 + jazz_do_probes(&result) as f64);
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
        let mut cursor = self.typed_cursor(
            query,
            &[],
            true,
            &ResultTypes::positional([ResultType::Integer]),
        )?;
        match cursor.next()?.and_then(|row| row.first()) {
            Some(Value::Integer(n)) => Ok(*n),
            _ => Err(bridge_error("expected integer SQL result".into())),
        }
    }
}
impl Backend for ObjectSql {
    fn query(&self, sql: &str, parameters: &[Value]) -> storage::Result<Box<dyn Cursor + '_>> {
        Ok(Box::new(self.cursor(sql, parameters, true)?))
    }
    fn query_typed(
        &self,
        sql: &str,
        parameters: &[Value],
        types: &ResultTypes,
    ) -> storage::Result<Box<dyn Cursor + '_>> {
        Ok(Box::new(self.typed_cursor(sql, parameters, true, types)?))
    }
    fn execute(&self, sql: &str, parameters: &[Value]) -> storage::Result<usize> {
        let mut cursor = self.cursor(sql, parameters, false)?;
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
            1 => Value::Integer(i64::from_le_bytes(self.take(8)?.try_into().unwrap())),
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
                out.extend_from_slice(&n.to_le_bytes());
            }
            Value::Real(n) => {
                out.push(2);
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

/// Construct the managed storage owner for a Rust host such as ServerCore.
pub fn durable_object_storage(storage: ObjectStorage) -> Storage {
    crate::initialize_wasm_tracing();
    Storage::from_backend(ObjectSql { storage })
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
            durable_object_storage(storage),
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
    pub fn check_rejected_row(&self, row: Vec<u8>, table: Option<String>) -> Result<(), JsValue> {
        let row = jazz::ids::RowUuid::from_bytes(
            row.try_into()
                .map_err(|_| JsValue::from_str("expected row UUID"))?,
        );
        self.binding()?
            .db
            .check_rejected_row_for_test(table.as_deref().unwrap_or("tasks"), row)
            .map_err(crate::to_js_error)
    }

    #[wasm_bindgen(js_name = checkSqlScopes)]
    pub fn check_sql_scopes(&self) -> Result<(), JsValue> {
        check_durable_object_scopes(&self.binding()?.db)
    }
}

#[cfg(feature = "durable-object-tests")]
pub fn check_durable_object_scopes(db: &jazz::db::Db) -> Result<(), JsValue> {
    db.check_sql_scopes_for_test().map_err(crate::to_js_error)?;
    // Rust callbacks use staged transactions too. Managed storage holds no
    // SQL scope between engine calls: each read replays the batch in one
    // `transactionSync` scope and rolls it back.
    let (value, _) =
        crate::block_on(db.transaction(async |_| Ok(42))).map_err(crate::to_js_error)?;
    assert_eq!(value, 42);
    let query = db
        .prepare_query(&jazz::query::Query::from("tasks"))
        .map_err(crate::to_js_error)?;
    let outside = db.read(&query).map_err(crate::to_js_error)?.len();
    let (inside, _) =
        crate::block_on(db.transaction(async |_| db.read(&query))).map_err(crate::to_js_error)?;
    assert_eq!(inside.len(), outside);
    // An error abandons the callback's batch and leaves ordinary reads usable.
    let error = crate::block_on(db.transaction(async |_| -> Result<(), jazz::db::Error> {
        Err(jazz::db::Error::new(jazz::db::ErrorCode::Query, "abandon"))
    }))
    .unwrap_err();
    assert_eq!(error.message, "abandon");
    db.read(&query).map_err(crate::to_js_error)?;
    // An open handle reads through its overlay between separate engine calls.
    let id = jazz::tools::OpenTransactionId::new();
    crate::block_on(db.begin_mergeable(id)).map_err(crate::to_js_error)?;
    let staged = crate::block_on(db.tx_ref(id).all_prepared(&query)).map_err(crate::to_js_error)?;
    assert_eq!(staged.len(), outside);
    db.abandon_transaction_handle(id)
        .map_err(crate::to_js_error)?;
    Ok(())
}

/// Exercise the actual Rust/JS value bridge in workerd. A backend-level test
/// is necessary because application decoding can hide INTEGER/REAL confusion.
#[cfg(feature = "durable-object-tests")]
#[wasm_bindgen]
impl WasmDb {
    #[wasm_bindgen(js_name = checkDurableObjectValues)]
    pub fn check_durable_object_values(storage: ObjectStorage) -> Result<(), JsValue> {
        fn check(storage: ObjectStorage) -> storage::Result<()> {
            assert_eq!(
                encode(&[Value::Integer(-MAX_SAFE)])?,
                vec![1, 1, 0, 0, 0, 0, 0, 224, 255]
            );
            assert_eq!(
                encode(&[Value::Real(1.0)])?,
                vec![2, 0, 0, 0, 0, 0, 0, 240, 63]
            );
            let db = ObjectSql { storage };
            for n in [i64::MIN, -MAX_SAFE - 1, MAX_SAFE + 1, i64::MAX] {
                let error = db.execute("SELECT ?1", &[Value::Integer(n)]).unwrap_err();
                assert!(error.to_string().contains("safe integer range"));
            }
            let integer = ResultTypes::positional([ResultType::Integer]);
            let real = ResultTypes::positional([ResultType::Real]);
            // A SQL calculation can exceed the range without a wide binding.
            for sql in [
                "SELECT 9007199254740992",
                "SELECT sum(x) FROM (SELECT 9007199254740991 AS x UNION ALL SELECT 1)",
            ] {
                for typed in [false, true] {
                    let mut cursor = if typed {
                        db.query_typed(sql, &[], &integer)?
                    } else {
                        db.query(sql, &[])?
                    };
                    assert!(cursor
                        .next()
                        .unwrap_err()
                        .to_string()
                        .contains("safe integer range"));
                }
            }
            for number in [1.0, 1e100] {
                let mut cursor = db.query_typed("SELECT ?1", &[Value::Real(number)], &real)?;
                assert_eq!(cursor.next()?.unwrap(), &[Value::Real(number)]);
            }
            let mixed = ResultTypes::positional([
                ResultType::Integer,
                ResultType::Real,
                ResultType::Dynamic,
            ]);
            let mut cursor = db.query_typed("SELECT 1, 1.0, 1.0", &[], &mixed)?;
            assert_eq!(
                cursor.next()?.unwrap(),
                &[Value::Integer(1), Value::Real(1.0), Value::Real(1.0)]
            );
            drop(cursor);

            let read = |sql: &str, params: &[Value]| -> storage::Result<Vec<Vec<Value>>> {
                let mut cursor = db.query(sql, params)?;
                let mut rows = Vec::new();
                while let Some(row) = cursor.next()? {
                    rows.push(row.to_vec());
                }
                Ok(rows)
            };
            assert_eq!(
                read("SELECT 1 AS constructor, 1.0 AS toString", &[])?,
                vec![vec![Value::Integer(1), Value::Real(1.0)]]
            );
            db.execute("CREATE TABLE exact_values (id INTEGER PRIMARY KEY, v)", &[])?;
            let values = vec![
                Value::Integer(-MAX_SAFE),
                Value::Integer(-9007199254740990),
                Value::Integer(-9007199254740989),
                Value::Integer(-9007199254740988),
                Value::Integer(0),
                Value::Integer(9007199254740988),
                Value::Integer(9007199254740989),
                Value::Integer(9007199254740990),
                Value::Integer(MAX_SAFE),
                Value::Real(1.0),
                Value::Real(1e100),
                Value::Text(b"9007199254740990".to_vec()),
                Value::Text("a\0λ🙂".as_bytes().to_vec()),
                Value::Blob(vec![0, 1, 3, 4, 128, 255]),
                Value::Blob(vec![]),
                Value::Null,
            ];
            for (id, value) in values.iter().enumerate() {
                assert_eq!(
                    db.execute(
                        "INSERT INTO exact_values VALUES (?1, ?2)",
                        &[Value::Integer(id as i64), value.clone()]
                    )?,
                    1
                );
            }
            assert_eq!(
                read("SELECT v FROM exact_values ORDER BY id", &[])?,
                values.iter().cloned().map(|v| vec![v]).collect::<Vec<_>>()
            );
            assert_eq!(
                read(
                    "SELECT v AS value FROM exact_values WHERE id < 9 ORDER BY value DESC LIMIT 2",
                    &[]
                )?,
                vec![
                    vec![Value::Integer(MAX_SAFE)],
                    vec![Value::Integer(9007199254740990)]
                ]
            );
            assert_eq!(
                read("SELECT min(v), max(v) FROM exact_values WHERE id < 9", &[])?,
                vec![vec![Value::Integer(-MAX_SAFE), Value::Integer(MAX_SAFE)]]
            );
            assert_eq!(
                read("SELECT sum(v) FROM exact_values WHERE id IN (4, 7)", &[])?,
                vec![vec![Value::Integer(9007199254740990)]]
            );
            assert_eq!(
                read(
                    "SELECT id FROM exact_values WHERE v = ?",
                    &[Value::Integer(9007199254740990)]
                )?,
                vec![vec![Value::Integer(7)]]
            );
            assert_eq!(
                read(
                    "SELECT ?2, ?1, ?2, '?' /* ?3 */ -- ?4\n",
                    &[Value::Real(1.0), Value::Integer(MAX_SAFE)]
                )?,
                vec![vec![
                    Value::Integer(MAX_SAFE),
                    Value::Real(1.0),
                    Value::Integer(MAX_SAFE),
                    Value::Text(b"?".to_vec())
                ]]
            );
            assert_eq!(
                read(
                    "WITH q AS (SELECT v FROM exact_values WHERE id=7) SELECT v, v FROM q;",
                    &[]
                )?,
                vec![vec![Value::Integer(9007199254740990); 2]]
            );
            assert!(db
                .query("UPDATE exact_values SET v = 1 RETURNING v", &[])
                .is_err());
            assert_eq!(
                read("SELECT v FROM exact_values WHERE id=0", &[])?,
                vec![vec![Value::Integer(-MAX_SAFE)]]
            );
            let info = read("PRAGMA table_info(\"exact_values\")", &[])?;
            assert_eq!(info.len(), 2);
            assert_eq!(info[0][0], Value::Integer(0));
            // Cache entries must not outlive DDL that changes a result shape.
            assert_eq!(
                read("SELECT * FROM exact_values WHERE id=0", &[])?[0].len(),
                2
            );
            db.execute("ALTER TABLE exact_values ADD COLUMN extra", &[])?;
            assert_eq!(
                read("SELECT * FROM exact_values WHERE id=0", &[])?[0].len(),
                3
            );
            db.transaction(&mut || {
                db.execute("ALTER TABLE exact_values ADD COLUMN temporary", &[])
                    .unwrap();
                assert_eq!(
                    read("SELECT * FROM exact_values WHERE id=0", &[]).unwrap()[0].len(),
                    4
                );
                false
            })?;
            assert_eq!(
                read("SELECT * FROM exact_values WHERE id=0", &[])?[0].len(),
                3
            );
            // A full-width result still fits DO's 100-result-column limit.
            let wide = format!(
                "SELECT {}",
                (0..100)
                    .map(|i| format!("{i} AS c{i}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            assert_eq!(read(&wide, &[])?[0].len(), 100);
            db.execute("DROP TABLE exact_values", &[])?;
            Ok(())
        }
        check(storage.clone())
            .map_err(|error| JsValue::from_str(&format!("backend check: {error}")))?;
        // Alice's authority writes and queries exact values through the public
        // engine, including its generated SQL and handwritten history writes.
        use jazz::db::{Db, DbConfig, DbIdentity};
        use jazz::ids::{AuthorSubject, NodeUuid};
        use jazz::records::Value as Cell;
        use jazz::tools::{
            ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
        };
        use std::collections::BTreeMap;
        let schema = jazz::schema::JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("exact_items")
                        .column("n", ColumnType::BigInt)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::True)
                                .with_insert(PolicyExpr::Cmp {
                                    column: "n".into(),
                                    op: jazz::tools::CmpOp::Gt,
                                    value: jazz::tools::PolicyValue::Literal(
                                        jazz::tools::Value::BigInt(9007199254740989),
                                    ),
                                })
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                        ),
                )
                .table(
                    TableSchemaBuilder::new("real_items")
                        .column("v", ColumnType::Double)
                        .policies(TablePolicies::new().with_select(PolicyExpr::True)),
                )
                .build(),
        )
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let db = crate::block_on(Db::open_storage(
            DbConfig::new(
                schema,
                DbIdentity {
                    node: NodeUuid::from_bytes([17; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ),
            durable_object_storage(storage),
        ))
        .map_err(crate::to_js_error)?;
        db.set_authority();
        for v in [1.0, 1e100] {
            crate::block_on(db.insert(
                "real_items",
                BTreeMap::from([("v".into(), Cell::F64(v))]),
                Default::default(),
            ))
            .map_err(crate::to_js_error)?;
        }
        let real_query = db
            .prepare_query(
                &jazz::query::Query::from("real_items")
                    .select(["v"])
                    .order_by("v", jazz::query::OrderDirection::Asc),
            )
            .map_err(crate::to_js_error)?;
        let real_rows = db.read(&real_query).map_err(crate::to_js_error)?;
        assert_eq!(real_rows[0].cell("v"), Some(Cell::F64(1.0)));
        assert_eq!(real_rows[1].cell("v"), Some(Cell::F64(1e100)));
        let avg = db
            .prepare_query(
                &jazz::query::Query::from("real_items")
                    .aggregate([jazz::query::Aggregate::avg("v").alias("mean")]),
            )
            .map_err(crate::to_js_error)?;
        let averages = db.read(&avg).map_err(crate::to_js_error)?;
        assert_eq!(
            averages[0].cell("mean"),
            Some(Cell::Nullable(Some(Box::new(Cell::F64(5e99)))))
        );
        let values = [-MAX_SAFE, -9007199254740990, 9007199254740990, MAX_SAFE];
        for n in values {
            crate::block_on(db.insert(
                "exact_items",
                BTreeMap::from([("n".into(), Cell::I64(n))]),
                Default::default(),
            ))
            .map_err(crate::to_js_error)?;
        }
        let query = db
            .prepare_query(
                &jazz::query::Query::from("exact_items")
                    .order_by("n", jazz::query::OrderDirection::Asc),
            )
            .map_err(crate::to_js_error)?;
        let rows = db.read(&query).map_err(crate::to_js_error)?;
        let actual = rows
            .iter()
            .map(|row| {
                row.cells()
                    .find(|(name, _)| *name == "n")
                    .unwrap()
                    .1
                    .cloned()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            values
                .into_iter()
                .map(|n| Some(Cell::I64(n)))
                .collect::<Vec<_>>()
        );
        let bob = AuthorSubject::for_test_bytes([18; 16]);
        let refused = crate::block_on(db.insert(
            "exact_items",
            BTreeMap::from([("n".into(), Cell::I64(9007199254740989))]),
            jazz::db::InsertOptions {
                identity: jazz::db::WriteIdentity::Session(bob),
                ..Default::default()
            },
        ));
        assert!(
            refused.is_err(),
            "safe integer policy boundary must reject equal values"
        );
        let accepted = crate::block_on(db.insert(
            "exact_items",
            BTreeMap::from([("n".into(), Cell::I64(9007199254740990))]),
            jazz::db::InsertOptions {
                identity: jazz::db::WriteIdentity::Session(bob),
                ..Default::default()
            },
        ))
        .map_err(crate::to_js_error)?;
        let history = db
            .prepare_query(
                &jazz::query::Query::from("history.exact_items")
                    .select(["n"])
                    .filter(jazz::query::eq(
                        jazz::query::col("id"),
                        jazz::query::lit(Cell::Uuid(accepted.row_uuid().0)),
                    )),
            )
            .map_err(crate::to_js_error)?;
        let history = db.read(&history).map_err(crate::to_js_error)?;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].cell("n"), Some(Cell::I64(9007199254740990)));
        let history_time = db
            .prepare_query(
                &jazz::query::Query::from("history.exact_items")
                    .select(["$acceptedAt"])
                    .filter(jazz::query::eq(
                        jazz::query::col("id"),
                        jazz::query::lit(Cell::Uuid(accepted.row_uuid().0)),
                    )),
            )
            .map_err(crate::to_js_error)?;
        let history_time = db.read(&history_time).map_err(crate::to_js_error)?;
        assert_eq!(history_time.len(), 1);
        assert_eq!(
            history_time[0].cell("$acceptedAt"),
            Some(Cell::U64(jazz::db::shard::hlc_ms(
                accepted.mergeable_tx_id()
            )))
        );
        let before = db.read(&query).map_err(crate::to_js_error)?.len();
        for n in [i64::MIN, -MAX_SAFE - 1, MAX_SAFE + 1, i64::MAX] {
            assert!(crate::block_on(db.insert(
                "exact_items",
                BTreeMap::from([("n".into(), Cell::I64(n))]),
                Default::default()
            ))
            .is_err());
        }
        assert_eq!(db.read(&query).map_err(crate::to_js_error)?.len(), before);
        Ok(())
    }
}
