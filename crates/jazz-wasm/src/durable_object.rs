//! Durable Object SQL storage: typed parameters, a forward-only cursor and
//! callback-owned transactions on one explicit storage owner. No SQLite C ABI
//! emulation, global current database or separately registered scope driver.
//!
//! `do-value-bridge-v2` is transient Rust/JS framing, never stored or synced:
//! tags 0=NULL, 1=i64 LE, 2=f64 LE, 3=UTF-8 bytes, 4=blob bytes. Variable
//! payloads have a u32 LE length; result rows start with a u32 LE cell count.
//! Result cells of unknown type are read through tagged SQL projections, so
//! integers and reals stay apart although JavaScript numbers do not. Unsafe
//! integers (beyond ±(2^53−1)) are refused. Database bytes are unchanged.
use jazz::storage::{self, Backend, Cursor, SqlError, SqlResult, Storage, Value};
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
    fn jazz_do_columns(result: &JsValue) -> js_sys::Array;
    #[wasm_bindgen(catch)]
    fn jazz_do_next(result: &JsValue) -> Result<Vec<u8>, JsValue>;
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
    fn next(&mut self) -> SqlResult<Option<&[Value]>> {
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
impl ObjectSql {
    /// Run `query`; `exact` reads its cells (a query), else only drains it.
    fn cursor(&self, query: &str, bindings: &[Value], exact: bool) -> SqlResult<ObjectCursor> {
        let result = jazz_do_query(&self.storage, query, &encode(bindings)?, exact, "")
            .map_err(sql_error)?;
        let columns = jazz_do_columns(&result)
            .iter()
            .map(|v| v.as_string().unwrap_or_default())
            .collect();
        Ok(ObjectCursor {
            result,
            columns,
            row: Vec::new(),
        })
    }

    fn scalar(&self, query: &str) -> SqlResult<i64> {
        let mut cursor = self.cursor(query, &[], true)?;
        match cursor.next()?.and_then(|row| row.first()) {
            Some(Value::Integer(n)) => Ok(*n),
            _ => Err(bridge_error("expected integer SQL result".into())),
        }
    }
}

impl Backend for ObjectSql {
    fn query(&self, sql: &str, parameters: &[Value]) -> SqlResult<Box<dyn Cursor + '_>> {
        Ok(Box::new(self.cursor(sql, parameters, true)?))
    }
    fn execute(&self, sql: &str, parameters: &[Value]) -> SqlResult<usize> {
        let mut cursor = self.cursor(sql, parameters, false)?;
        while cursor.next()?.is_some() {}
        drop(cursor);
        let first = sql
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
    fn execute_script(&self, script: &str) -> SqlResult<()> {
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
    fn transaction(&self, body: &mut dyn FnMut() -> bool) -> SqlResult<()> {
        let mut callback = || body();
        let closure = wasm_bindgen::closure::ScopedClosure::borrow_mut_aborting(&mut callback);
        jazz_do_scope(&self.storage, closure.as_ref().unchecked_ref()).map_err(sql_error)
    }
    fn last_insert_rowid(&self) -> SqlResult<i64> {
        self.scalar("SELECT last_insert_rowid()")
    }
    fn total_changes(&self) -> SqlResult<u64> {
        self.scalar("SELECT total_changes()").map(|n| n as u64)
    }
    fn max_parameters(&self) -> usize {
        100
    }
}
fn bridge_error(message: String) -> SqlError {
    storage::backend_error(1, message)
}
fn sql_error(error: JsValue) -> SqlError {
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

fn encode(bindings: &[Value]) -> SqlResult<Vec<u8>> {
    let mut out = Vec::new();
    for value in bindings {
        match value {
            Value::Null => out.push(0),
            Value::Integer(n) => {
                if !(-MAX_SAFE..=MAX_SAFE).contains(n) {
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

/// A Durable Object's SQL (`ctx.storage`, with `sql.exec` and
/// `transactionSync`) as a Jazz store.
pub fn durable_object_storage(storage: ObjectStorage) -> Storage {
    Storage::from_backend(ObjectSql { storage })
}

/// Test-only checks the workerd smoke test runs inside a Durable Object
/// (`crates/jazz-wasm/tests/durable-objects`), through Jazz's Rust API on
/// the object's own SQL.
#[cfg(feature = "durable-object-tests")]
mod smoke {
    use super::{durable_object_storage, ObjectStorage};
    use jazz::db::{block_on, Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier};
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
    use jazz::query::Query;
    use jazz::records::Value;
    use jazz::schema::JazzSchema;
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
    use std::collections::BTreeMap;
    use wasm_bindgen::prelude::*;

    fn open(storage: ObjectStorage) -> Result<Db, JsValue> {
        let policies = TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True);
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("title", ColumnType::Text)
                        .nullable_column("body", ColumnType::Text)
                        .policies(policies),
                )
                .build(),
        )
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        };
        block_on(Db::open_storage(
            DbConfig::new(schema, identity),
            durable_object_storage(storage),
        ))
        .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Deterministic text without repeats: chunked into many leaves.
    fn text(len: usize) -> String {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (b'a' + (state % 26) as u8) as char
            })
            .collect()
    }

    fn js(error: impl std::fmt::Display) -> JsValue {
        JsValue::from_str(&error.to_string())
    }

    /// Run `step` on this object's storage; a JSON summary of what it saw.
    /// - `write`: insert a short note and a 300 KiB one.
    /// - `read`: every note's title and body length (after a restart too).
    /// - `transaction`: a callback transaction, which a Durable Object core
    ///   refuses; the refusal's message.
    #[wasm_bindgen(js_name = durableObjectSmoke)]
    pub fn durable_object_smoke(storage: ObjectStorage, step: &str) -> Result<String, JsValue> {
        let db = open(storage)?;
        let note = |title: &str, body: &str| {
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.to_owned())),
                ("body".to_owned(), Value::String(body.to_owned())),
            ])
        };
        let row = |id: u8| InsertOptions {
            row_id: Some(RowUuid::from_bytes([id; 16])),
            ..Default::default()
        };
        match step {
            "write" => {
                block_on(db.insert("notes", note("short", "hello"), row(1))).map_err(js)?;
                block_on(db.insert("notes", note("large", &text(300 * 1024)), row(2)))
                    .map_err(js)?;
                Ok("{\"written\":2}".to_owned())
            }
            "read" => {
                let prepared = db.prepare_query(&Query::from("notes")).map_err(js)?;
                let rows =
                    block_on(db.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
                        .map_err(js)?;
                let mut seen: Vec<(String, usize, bool)> = rows
                    .iter()
                    .map(|row| {
                        let title = match row.cell("title") {
                            Some(Value::String(title)) => title,
                            other => format!("{other:?}"),
                        };
                        let body = match row.cell("body") {
                            Some(Value::Nullable(Some(body))) => match *body {
                                Value::String(body) => body,
                                other => format!("{other:?}"),
                            },
                            Some(Value::String(body)) => body,
                            other => format!("{other:?}"),
                        };
                        let expected = title != "large" || body == text(300 * 1024);
                        (title, body.len(), expected)
                    })
                    .collect();
                seen.sort();
                serde_json::to_string(&seen).map_err(js)
            }
            "transaction" => {
                let refused = block_on(db.transaction(async |tx| {
                    tx.insert("notes", note("inside", "x"), row(3)).await?;
                    Ok(())
                }));
                match refused {
                    Ok(_) => Err(js("a callback transaction committed on a Durable Object")),
                    Err(error) => serde_json::to_string(&error.to_string()).map_err(js),
                }
            }
            other => Err(js(format!("unknown smoke step {other}"))),
        }
    }
}
