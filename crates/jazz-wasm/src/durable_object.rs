//! Durable Object SQL storage: typed parameters, a forward-only cursor and
//! callback-owned transactions on one explicit storage owner. No SQLite C ABI
//! emulation, global current database or separately registered scope driver.
//!
//! `do-value-bridge-v2` is transient Rust/JS framing, never stored or synced:
//! tags 0=NULL, 1=i64 LE, 2=f64 LE, 3=UTF-8 bytes, 4=blob bytes. Variable
//! payloads have a u32 LE length; result rows start with a u32 LE cell count.
//! Result cells of unknown type are read through tagged SQL projections, so
//! integers and reals stay apart although JavaScript numbers do not.
//! Integers keep their whole 64-bit range: beyond a JavaScript number's
//! exact range they are bound as decimal text (under the statement's
//! `CAST(? AS INTEGER)`) and read through that projection's text. Database
//! bytes are unchanged.
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
      // A number where it holds the integer exactly; else decimal text,
      // which the statement's CAST(? AS INTEGER) reads exactly.
      const n = v.getBigInt64(i, true);
      list.push(n >= -9007199254740991n && n <= 9007199254740991n ? Number(n) : n.toString());
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
        storage::durable_object_refusal(sql, true)?;
        Ok(Box::new(self.cursor(sql, parameters, true)?))
    }
    fn execute(&self, sql: &str, parameters: &[Value]) -> SqlResult<usize> {
        storage::durable_object_refusal(sql, false)?;
        let first = sql
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        // `changes()` is the last data-modifying statement's count, which a
        // `WITH … SELECT` does not reset: count those by `total_changes()`.
        let before = match first.as_str() {
            "WITH" => Some(self.scalar("SELECT total_changes()")?),
            _ => None,
        };
        let mut cursor = self.cursor(sql, parameters, false)?;
        while cursor.next()?.is_some() {}
        drop(cursor);
        match (first.as_str(), before) {
            (_, Some(before)) => Ok((self.scalar("SELECT total_changes()")? - before) as usize),
            ("INSERT" | "UPDATE" | "DELETE" | "REPLACE", None) => {
                Ok(self.scalar("SELECT changes()")? as usize)
            }
            _ => Ok(0),
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

    /// Scopes on the object's own `transactionSync`, through Jazz's
    /// storage: an inner scope that fails is undone while its outer scope
    /// keeps the rest, and a scope that fails is undone whole. The rows
    /// left, as JSON.
    fn scopes(storage: ObjectStorage) -> Result<String, jazz::db::Error> {
        use jazz::db::{Error, ErrorCode};
        let storage = durable_object_storage(storage);
        let sql = |e: jazz::storage::SqlError| Error::new(ErrorCode::Storage, e.to_string());
        let insert = |s: &jazz::storage::Storage, v: i64| {
            s.execute("INSERT INTO smoke_scopes (v) VALUES (?1)", [v])
                .map(drop)
                .map_err(sql)
        };
        let refused = || Error::new(ErrorCode::Storage, "refused by the smoke test");
        storage
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS smoke_scopes (v INTEGER); DELETE FROM smoke_scopes",
            )
            .map_err(sql)?;
        storage.scope("outer", |s| {
            insert(s, 1)?;
            let inner = s.scope("inner", |s| {
                insert(s, 2)?;
                Err::<(), _>(refused())
            });
            assert!(inner.is_err());
            insert(s, 3)
        })?;
        let failed = storage.scope("whole", |s| {
            insert(s, 4)?;
            Err::<(), _>(refused())
        });
        assert!(failed.is_err());
        let rows: Vec<i64> = storage
            .prepare("SELECT v FROM smoke_scopes ORDER BY v")
            .map_err(sql)?
            .query_map([], |row| row.get(0))
            .map_err(sql)?
            .collect::<Result<_, _>>()
            .map_err(sql)?;
        serde_json::to_string(&rows).map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))
    }

    /// Integers across the bridge at the ends of their 64-bit range and
    /// just past a JavaScript number's exact one: each bound, stored,
    /// matched and read back. Those that came back unchanged, as JSON
    /// strings.
    fn integers(storage: ObjectStorage) -> Result<String, jazz::db::Error> {
        use jazz::db::{Error, ErrorCode};
        let storage = durable_object_storage(storage);
        let sql = |e: jazz::storage::SqlError| Error::new(ErrorCode::Storage, e.to_string());
        storage
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS smoke_integers (v INTEGER); DELETE FROM smoke_integers",
            )
            .map_err(sql)?;
        let mut same = Vec::new();
        for n in [i64::MIN, -(1 << 53) - 1, (1 << 53) + 1, i64::MAX] {
            storage
                .execute("INSERT INTO smoke_integers (v) VALUES (?1)", [n])
                .map_err(sql)?;
            let read: i64 = storage
                .query_row("SELECT v FROM smoke_integers WHERE v = ?1", [n], |r| {
                    r.get(0)
                })
                .map_err(sql)?;
            if read == n {
                same.push(n.to_string());
            }
        }
        serde_json::to_string(&same).map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))
    }

    /// A thin client's wire, for the host test (`host.test.mjs`): one
    /// WebSocket message carrying `frames`.
    fn batch(frames: &[jazz::wire::WireFrame]) -> Vec<u8> {
        let encoded: Vec<Vec<u8>> = frames
            .iter()
            .map(|frame| jazz::wire::encode_frame(frame).expect("encode frame"))
            .collect();
        jazz_server::portable::websocket_codec::encode_ws_frame_batches(&encoded)
            .expect("batch")
            .pop()
            .expect("one batch")
    }

    /// The fixture's schema (a `notes` table with a `title`), as the
    /// public schema JSON a core is opened with and a thin client declares.
    #[wasm_bindgen(js_name = testNotesSchema)]
    pub fn test_notes_schema() -> String {
        let policies = TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True);
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(policies),
            )
            .build();
        serde_json::to_string(&schema).expect("schema JSON")
    }

    /// The deploy request `createJazz` sends for `schema` (public schema
    /// JSON carrying its permissions as table policies): its structure,
    /// and its policies as the bundle's permissions.
    #[wasm_bindgen(js_name = testDeployRequest)]
    pub fn test_deploy_request(schema: &str) -> String {
        use jazz_server::portable::deployment::{DeployRequest, DeploySchema};
        let source: jazz::tools::public_schema::Schema =
            serde_json::from_str(schema).expect("schema JSON");
        let mut structural = source.clone();
        for table in structural.values_mut() {
            table.policies = TablePolicies::default();
        }
        let hash = jazz::tools::public_schema::SchemaHash::compute(&structural).to_string();
        let request = DeployRequest {
            target_schema_hash: hash.clone(),
            schemas: vec![DeploySchema {
                hash,
                schema: structural,
            }],
            migrations: Vec::new(),
            permissions: source
                .iter()
                .map(|(name, table)| (*name, table.policies.clone()))
                .collect(),
            allow_dropping_history: false,
        };
        serde_json::to_string(&request).expect("deploy request JSON")
    }

    /// The deploy body an app that encrypts a column publishes, from its
    /// loaded files (a task's title is encrypted in its project's space);
    /// with `retyped`, the same schema whose declaration types the title as
    /// an integer instead of text.
    #[wasm_bindgen(js_name = testEncryptedDeployBody)]
    pub fn test_encrypted_deploy_body(retyped: bool) -> String {
        let app = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {
                "projects": { "columns": { "name": "string", "ownerId": "uuid" } },
                "tasks": {
                    "columns": { "projectId": "uuid", "title": "bytes", "__e2ee_eq_title": "bytes" },
                    "relations": { "project": { "rel": "projects", "column": "projectId" } },
                    "encryption": { "space": "projectId", "scope": "projects",
                        "columns": { "title": "string" }, "equality": ["title"] }
                }
            } }"#
            .replace(r#""title": "string""#, if retyped { r#""title": "int""# } else { r#""title": "string""# });
        let members = "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)";
        let permissions = serde_json::json!({
            "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
            "tables": {
                "projects": {
                    "read": members,
                    "insert": "$createdBy.account = session.user",
                    "update": "ownerId = session.user",
                },
                "tasks": { "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" },
            },
        })
        .to_string();
        jazz::tools::deployment::artifact::load(app.as_bytes(), permissions.as_bytes())
            .expect("the app's files load")
            .deployment_body(false)
            .to_string()
    }

    /// A local-first guest (keys from `seed`, a byte repeated) proving
    /// itself for `audience` at `now_seconds`: `{ proof, peer, subject }`,
    /// its self-signed proof, the peer identity its client names (with the
    /// account founded in `audience`'s namespace) and its subject.
    #[wasm_bindgen(js_name = testGuest)]
    pub fn test_guest(seed: u8, audience: &str, now_seconds: u64) -> String {
        use jazz::tools::identity;
        let seed = [seed; 32];
        let proof = identity::mint_jazz_self_signed_token_at(
            &seed,
            identity::DEVICE_ACCOUNT_ISSUER,
            audience,
            3600,
            now_seconds,
        )
        .expect("a proof");
        let subject = identity::derive_user_id(&seed).to_string();
        let founding = jazz_server::portable::accounts::local_first_founding_app(audience);
        let peer = AuthorSubject::reserved(identity::DEVICE_ACCOUNT_ISSUER, &subject)
            .expect("a local-first author")
            .with_account(jazz::account_registry::device_account_id(
                *founding.uuid(),
                &subject,
            ));
        serde_json::json!({
            "proof": proof,
            "peer": peer.canonical(),
            "subject": subject,
            "issuer": identity::DEVICE_ACCOUNT_ISSUER,
        })
        .to_string()
    }

    /// A user's identity (`issuer`, `subject`), as a prelude names its peer.
    #[wasm_bindgen(js_name = testUserIdentity)]
    pub fn test_user_identity(issuer: &str, subject: &str) -> String {
        AuthorSubject::authenticated(issuer, subject)
            .expect("a user identity")
            .canonical()
            .to_owned()
    }

    /// The system identity, as a prelude names its peer.
    #[wasm_bindgen(js_name = testSystemIdentity)]
    pub fn test_system_identity() -> String {
        AuthorSubject::SYSTEM.canonical().to_owned()
    }

    /// A thin client's opening `Hello`.
    #[wasm_bindgen(js_name = testThinHello)]
    pub fn test_thin_hello() -> Vec<u8> {
        batch(&[jazz::wire::WireFrame::Hello(
            jazz::wire::WireHello::current(
                jazz::wire::WirePeerRole::Client,
                jazz::wire::current_wire_features() | jazz::wire::FEATURE_THIN_RESULTS,
            ),
        )])
    }

    fn thin(id: u64, op: jazz::thin::ThinOp) -> Vec<u8> {
        batch(&[jazz::wire::WireFrame::ThinRequest(
            jazz::thin::ThinRequest { id, op },
        )])
    }

    /// A thin request declaring `schema` (public schema JSON).
    #[wasm_bindgen(js_name = testThinSchema)]
    pub fn test_thin_schema(id: u64, schema: &str) -> Vec<u8> {
        thin(
            id,
            jazz::thin::ThinOp::Schema {
                schema: schema.as_bytes().to_vec(),
            },
        )
    }

    /// A thin request inserting a note titled `title`.
    #[wasm_bindgen(js_name = testThinInsertNote)]
    pub fn test_thin_insert_note(id: u64, title: &str) -> Vec<u8> {
        let descriptor =
            jazz::records::RecordDescriptor::new([("title", jazz::records::ValueType::String)]);
        let record = jazz::records::OwnedRecord::new(
            descriptor
                .create(&[Value::String(title.to_owned())])
                .expect("note record"),
            descriptor,
        );
        thin(
            id,
            jazz::thin::ThinOp::Insert {
                table: "notes".to_owned(),
                cells: jazz::binding_codec::encode_named_cells(&record).expect("cells"),
                options: Vec::new(),
                transaction: None,
            },
        )
    }

    /// A thin request reading every note.
    #[wasm_bindgen(js_name = testThinQueryNotes)]
    pub fn test_thin_query_notes(id: u64) -> Vec<u8> {
        thin(
            id,
            jazz::thin::ThinOp::Query {
                query: postcard::to_allocvec(&Query::from("notes")).expect("query"),
                options: Vec::new(),
                transaction: None,
            },
        )
    }

    /// The answers in a message the core sent, as JSON: `{"hello":true}`,
    /// `{"id":n,"ok":true}`, `{"id":n,"written":true}`,
    /// `{"id":n,"titles":[…]}` or `{"error":"…"}`.
    #[wasm_bindgen(js_name = testDecodeAnswers)]
    pub fn test_decode_answers(message: &[u8]) -> Result<String, JsValue> {
        let frames = jazz_server::portable::websocket_codec::decode_ws_encoded_frame_batch(message)
            .map_err(js)?;
        let mut out = Vec::new();
        for frame in frames {
            out.push(match jazz::wire::decode_frame(&frame).map_err(js)? {
                jazz::wire::WireFrame::Hello(_) => serde_json::json!({ "hello": true }),
                jazz::wire::WireFrame::Error(error) => {
                    serde_json::json!({ "error": error.message })
                }
                jazz::wire::WireFrame::ThinResponse(response) => match response.body {
                    jazz::thin::ThinBody::Ok => serde_json::json!({ "id": response.id, "ok": true }),
                    jazz::thin::ThinBody::Written { .. } => {
                        serde_json::json!({ "id": response.id, "written": true })
                    }
                    jazz::thin::ThinBody::Rows { rows, .. } => {
                        let rows = jazz::binding_codec::decode_rows(&rows).map_err(js)?;
                        let mut titles: Vec<String> = rows
                            .iter()
                            .flat_map(|group| group.rows.iter())
                            .filter_map(|row| match row.cells.get("title") {
                                Some(Value::String(title)) => Some(title.clone()),
                                _ => None,
                            })
                            .collect();
                        titles.sort();
                        serde_json::json!({ "id": response.id, "titles": titles })
                    }
                    jazz::thin::ThinBody::Error { code, message } => {
                        serde_json::json!({ "id": response.id, "error": format!("{code}: {message}") })
                    }
                    other => serde_json::json!({ "id": response.id, "other": format!("{other:?}") }),
                },
                other => serde_json::json!({ "other": format!("{other:?}") }),
            });
        }
        serde_json::to_string(&out).map_err(js)
    }

    /// Run `step` on this object's storage; a JSON summary of what it saw.
    /// - `write`: insert a short note and a 300 KiB one.
    /// - `read`: every note's title and body length (after a restart too).
    /// - `transaction`: a callback transaction, which a Durable Object core
    ///   refuses; the refusal's message.
    #[wasm_bindgen(js_name = durableObjectSmoke)]
    pub fn durable_object_smoke(storage: ObjectStorage, step: &str) -> Result<String, JsValue> {
        match step {
            "scopes" => return scopes(storage).map_err(js),
            "integers" => return integers(storage).map_err(js),
            _ => {}
        }
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

/// The Jazz core a Durable Object hosts: jazz-server's engine
/// ([`jazz_server::runtime::ServerCore`]) on the object's own SQL, with the
/// native server's WebSocket handshake and admission by the app's core
/// secret. The object's JavaScript (`durable-object-host.js`) owns the
/// sockets and calls in as they deliver: `accept` a new connection's
/// prelude and first frames, `receive` its later messages, `tick`, and
/// send what `output` returns.
mod core_host {
    use super::{durable_object_storage, ObjectStorage};
    use jazz_server::portable::accounts::{
        self, AccountRoute, RouteAuthority, RouteBody, RouteContext,
    };
    use jazz_server::portable::admission::{
        admit_by_core_secret, same_secret, SessionRevocation, UpgradeCredential,
        WebSocketCredential, WebSocketPrelude,
    };
    use jazz_server::portable::catalogue::{CatalogueIndex, DeploymentResponse};
    use jazz_server::portable::deployment::{self, CoreHost as _, DeployBody, DeployError};
    use jazz_server::portable::handshake::{link, negotiate_hello};
    use jazz_server::portable::websocket_codec::{
        decode_ws_encoded_frame_batch, encode_ws_frame_batches, ws_foreign_version_refusal,
    };
    use jazz_server::runtime::{ServerCore, ServerSession};
    use std::collections::{HashMap, VecDeque};
    use wasm_bindgen::prelude::*;

    fn js(error: impl std::fmt::Display) -> JsValue {
        JsValue::from_str(&error.to_string())
    }

    #[cfg(feature = "durable-object-tests")]
    thread_local! {
        /// Tests only: a thread-local `test/panic` leaves borrowed, as a
        /// trap leaves the core's (an instance that trapped is not reused).
        static TRAPPED_BORROW: std::cell::RefCell<()> = const { std::cell::RefCell::new(()) };
    }

    /// What a connection's socket is.
    struct Socket {
        session: ServerSession,
        thin: bool,
        /// Split thin requests being joined.
        assembler: jazz::thin::ThinAssembler,
        /// Input its session's budget had no room for yet, oldest first,
        /// and its bytes: a Durable Object cannot stop reading a socket, so
        /// the object holds it, up to [`HELD_LIMIT`].
        held: VecDeque<Held>,
        held_bytes: usize,
        /// Why the socket ends: its error frame goes out with its output.
        failed: Option<jazz::wire::WireError>,
        /// For a user session the backend stated: what a revocation of its
        /// user's sessions is checked against.
        revocation: Option<SessionRevocation>,
        /// For a guest admitted by its own proof: its identity, whose
        /// account assignment is checked again whenever assignments change
        /// (as native's sockets watch the registry).
        guest: Option<jazz::ids::AuthorSubject>,
    }

    /// The most input a socket may have held for room (one session's
    /// ingress budget, 8 MiB): a peer sending faster than the engine takes
    /// it is ended with `Backpressure` (it reconnects later) instead of
    /// growing the object's memory toward the isolate's limit.
    const HELD_LIMIT: usize = 8 * 1024 * 1024;

    /// The most input all sockets together may have held (an isolate has
    /// about 128 MiB, and a saturated socket also has a session's ingress
    /// and taken egress besides): past it the socket holding most is ended
    /// with `Backpressure`, as the engine sheds egress.
    const GLOBAL_HELD_LIMIT: usize = 32 * 1024 * 1024;

    impl Socket {
        fn hold(&mut self, input: Held, bytes: usize, limit: usize) {
            self.held.push_back(input);
            self.held_bytes += bytes;
            if self.held_bytes > limit {
                self.shed();
            }
        }

        /// End the socket for sending more than the core could take.
        fn shed(&mut self) {
            self.held.clear();
            self.held_bytes = 0;
            self.failed.get_or_insert_with(|| {
                jazz::wire::WireError::new(
                    jazz::wire::WireErrorCode::Backpressure,
                    jazz::wire::WireRetry::Later,
                    "the connection sent more than the core could take: reconnect later",
                )
            });
        }

        fn fail(&mut self, message: impl std::fmt::Display) {
            self.held.clear();
            self.held_bytes = 0;
            self.failed.get_or_insert_with(|| {
                jazz::wire::WireError::new(
                    jazz::wire::WireErrorCode::MalformedFrame,
                    jazz::wire::WireRetry::Never,
                    message.to_string(),
                )
            });
        }
    }

    enum Held {
        Frames(Vec<Vec<u8>>, usize),
        Thin(jazz::thin::ThinRequest, usize, bool),
    }

    /// The core (see the module docs). Sockets are numbered by `accept`.
    #[wasm_bindgen]
    pub struct DurableObjectCore {
        storage: ObjectStorage,
        app_id: String,
        /// Open once a schema is known: published to the store (or
        /// configured), else `None` and sockets are told to wait.
        core: Option<ServerCore>,
        secret: String,
        sockets: HashMap<u32, Socket>,
        next_socket: u32,
        /// [`HELD_LIMIT`] and [`GLOBAL_HELD_LIMIT`] (smaller in tests).
        held_limit: usize,
        global_held_limit: usize,
        /// Tests only: the engine takes no input and does not tick, so
        /// input piles up as held.
        paused: bool,
        /// The account registry's tables exist (made on first use).
        accounts_open: bool,
    }

    /// What `accept` answers: the frames to send, and the socket's number
    /// (absent when the connection was refused and is to be closed after
    /// sending them).
    struct Accepted {
        socket: Option<u32>,
        frames: Vec<js_sys::Uint8Array>,
    }

    // What the host is handed back is plain JS, never an object owned by
    // the wasm instance: after a trap the host drops the instance for a
    // fresh one, and nothing of the old one may outlive it (its finalizer
    // would free into the new instance's memory).
    fn object(fields: &[(&str, JsValue)]) -> JsValue {
        let object = js_sys::Object::new();
        for (key, value) in fields {
            let _ = js_sys::Reflect::set(&object, &JsValue::from_str(key), value);
        }
        object.into()
    }

    fn array(items: Vec<js_sys::Uint8Array>) -> JsValue {
        items.into_iter().collect::<js_sys::Array>().into()
    }

    impl Accepted {
        fn into_js(self) -> JsValue {
            object(&[
                (
                    "socket",
                    self.socket.map_or(JsValue::UNDEFINED, JsValue::from),
                ),
                ("frames", array(self.frames)),
            ])
        }
    }

    fn error_frame(error: &jazz::wire::WireError) -> Vec<u8> {
        jazz::wire::encode_frame(&jazz::wire::WireFrame::Error(error.clone()))
            .and_then(|frame| encode_ws_frame_batches(&[frame]))
            .ok()
            .and_then(|mut batches| batches.pop())
            .unwrap_or_default()
    }

    fn refused(frame: Vec<u8>) -> Accepted {
        Accepted {
            socket: None,
            frames: vec![js_sys::Uint8Array::from(frame.as_slice())],
        }
    }

    #[wasm_bindgen]
    impl DurableObjectCore {
        /// Open the core on the object's SQL (`ctx.storage`). `secret`
        /// admits the app's backend; `schema` (the app's public schema as
        /// JSON) is served until a published catalogue replaces it.
        pub fn open(
            storage: ObjectStorage,
            secret: String,
            schema: Option<String>,
        ) -> Result<DurableObjectCore, JsValue> {
            if secret.len() < 32 {
                return Err(js("the core secret must be at least 32 characters"));
            }
            #[cfg(feature = "durable-object-tests")]
            TRAPPED_BORROW.with(|cell| {
                cell.try_borrow_mut().map(drop).map_err(|_| {
                    js("this wasm instance trapped holding a borrow: reinstantiate it")
                })
            })?;
            // Admit the store (stamping a new one as Jazz's) before keeping
            // anything of the host's in it.
            jazz::host::StoreFile::from_storage(durable_object_storage(storage.clone()))
                .map_err(js)?;
            let app_id = stored_app_id(&durable_object_storage(storage.clone())).map_err(js)?;
            let mut this = Self {
                storage,
                app_id,
                core: None,
                secret,
                sockets: HashMap::new(),
                next_socket: 1,
                held_limit: HELD_LIMIT,
                global_held_limit: GLOBAL_HELD_LIMIT,
                paused: false,
                accounts_open: false,
            };
            this.open_core(schema.as_deref())?;
            Ok(this)
        }

        /// Admit a new connection: its prelude (JSON) and first frame batch
        /// (its `Hello`), and the credential its upgrade carried, if any.
        pub fn accept(
            &mut self,
            prelude: &str,
            first: &[u8],
            credential: Option<String>,
            access: Option<String>,
        ) -> JsValue {
            self.admit(prelude, first, credential, access).into_js()
        }

        fn admit(
            &mut self,
            prelude: &str,
            first: &[u8],
            credential: Option<String>,
            access: Option<String>,
        ) -> Accepted {
            // A client on another wire version is refused in its own layout.
            if let Some(refusal) = ws_foreign_version_refusal(first) {
                return refused(
                    encode_ws_frame_batches(&[refusal])
                        .ok()
                        .and_then(|mut b| b.pop())
                        .unwrap_or_default(),
                );
            }
            let fail = |code, message: String| {
                refused(error_frame(&jazz::wire::WireError::new(
                    code,
                    jazz::wire::WireRetry::Never,
                    message,
                )))
            };
            let prelude: WebSocketPrelude = match serde_json::from_str(prelude) {
                Ok(prelude) => prelude,
                Err(error) => {
                    return fail(
                        jazz::wire::WireErrorCode::AuthFailed,
                        format!("invalid websocket prelude: {error}"),
                    );
                }
            };
            let upgrade = credential.as_deref().map(|credential| UpgradeCredential {
                credential,
                access: access.as_deref(),
            });
            let now = js_sys::Date::now() as u64;
            // A guest the backend relays proves who it is itself: its proof
            // is checked, and its account founded or found, in the object's
            // account registry, as native admits it.
            let secret = self.secret.clone();
            let app_id = jazz::tools::AppId::from_string(&self.app_id);
            let encrypts = self.encrypts();
            let mut guest = |token: &str, peer, audience: Option<&str>| {
                let app_id = app_id.clone().map_err(|error| error.to_string())?;
                let storage = self.registry()?;
                jazz_server::portable::guest::admit_guest(
                    &storage,
                    token,
                    peer,
                    audience,
                    app_id,
                    now / 1000,
                    &|| encrypts,
                )
                .map_err(|refusal| refusal.to_string())
            };
            let admission = match admit_by_core_secret(&prelude, upgrade, &secret, now, &mut guest)
            {
                Ok(admission) => admission,
                Err(message) => return fail(jazz::wire::WireErrorCode::AuthFailed, message),
            };
            let hello = match negotiate_hello(first, prelude.bootstrap_catalogue) {
                Ok(hello) => hello,
                Err(error) => return refused(error_frame(&error)),
            };
            let link = match link(&admission, &hello) {
                Ok(link) => link,
                Err(error) => return refused(error_frame(&error)),
            };
            // A stated session of a user whose sessions were revoked after
            // the backend authenticated it is refused, as native refuses it.
            let revocation = admission.revocation.clone();
            let guest = (admission.credential == WebSocketCredential::Session)
                .then_some(admission.identity);
            if let Some(revocation) = &revocation {
                match self.revocation_mark(&revocation.principal) {
                    Ok(mark) if accounts::session_revoked(mark, revocation.authenticated_at_ms) => {
                        return refused(error_frame(&revoked()));
                    }
                    Ok(_) => {}
                    Err(error) => {
                        return refused(error_frame(&jazz::wire::WireError::new(
                            jazz::wire::WireErrorCode::NotReady,
                            jazz::wire::WireRetry::Later,
                            error,
                        )));
                    }
                }
            }
            let Some(core) = self.core.as_mut() else {
                return refused(error_frame(&jazz::wire::WireError::new(
                    jazz::wire::WireErrorCode::NotReady,
                    jazz::wire::WireRetry::Later,
                    "no schema has been published for this app; start the app's createJazz backend, which publishes its schema, before connecting",
                )));
            };
            let opened = if link.thin {
                core.open_thin_session(
                    admission.identity,
                    admission.claims,
                    admission.bundle,
                    admission.trust,
                )
            } else {
                core.open_session(
                    admission.identity,
                    admission.claims,
                    admission.bundle,
                    admission.trust,
                )
            };
            let session = match opened {
                Ok(session) => session,
                Err(error) => {
                    return fail(jazz::wire::WireErrorCode::NotReady, error.to_string());
                }
            };
            let socket = self.next_socket;
            self.next_socket += 1;
            self.sockets.insert(
                socket,
                Socket {
                    session,
                    thin: link.thin,
                    assembler: jazz::thin::ThinAssembler::default(),
                    held: VecDeque::new(),
                    held_bytes: 0,
                    failed: None,
                    revocation,
                    guest,
                },
            );
            let hello = encode_ws_frame_batches(&[link.server_hello])
                .ok()
                .and_then(|mut b| b.pop())
                .unwrap_or_default();
            Accepted {
                socket: Some(socket),
                frames: vec![js_sys::Uint8Array::from(hello.as_slice())],
            }
        }

        /// Take a message `socket` delivered (a batch of encoded frames).
        /// A malformed message, or input past what the socket may hold,
        /// ends that socket alone: its error frame comes with its `output`.
        pub fn receive(&mut self, socket: u32, message: &[u8]) -> Result<(), JsValue> {
            let Some(state) = self.sockets.get_mut(&socket) else {
                return Err(js("unknown socket"));
            };
            if state.failed.is_some() {
                return Ok(());
            }
            if let Err(error) = Self::read(state, message, self.held_limit) {
                state.fail(error);
            }
            self.admit_held(socket);
            self.shed_held();
            Ok(())
        }

        /// Ticks the engine once, admitting input held for room as room
        /// frees. Take every socket's `output` after it, and tick again
        /// while `wantsTick`: a host returns to its event loop between
        /// ticks, so output goes out as it is made.
        pub fn tick(&mut self) {
            if self.paused {
                return;
            }
            let Some(core) = self.core.as_mut() else {
                return;
            };
            core.tick();
            let sockets: Vec<u32> = self.sockets.keys().copied().collect();
            for socket in sockets {
                self.admit_held(socket);
            }
        }

        /// Whether the core wants a tick now (see `ServerCore::wants_tick`).
        #[wasm_bindgen(js_name = wantsTick)]
        pub fn wants_tick(&self) -> bool {
            !self.paused && self.core.as_ref().is_some_and(ServerCore::wants_tick)
        }

        /// How often to tick while sockets are open, in milliseconds.
        #[wasm_bindgen(js_name = idleTickMs)]
        pub fn idle_tick_ms() -> u32 {
            ServerCore::IDLE_TICK.as_millis() as u32
        }

        /// What to send on `socket` now: frame batches, in order. `ended`
        /// says whether the session ended (close the socket after sending).
        pub fn output(&mut self, socket: u32) -> JsValue {
            let Output { batches, ended } = self.take(socket);
            object(&[("batches", array(batches)), ("ended", JsValue::from(ended))])
        }

        fn take(&mut self, socket: u32) -> Output {
            let Some(state) = self.sockets.get(&socket) else {
                return Output {
                    batches: Vec::new(),
                    ended: true,
                };
            };
            let mut batches = Vec::new();
            let mut ended = false;
            let failed = state.failed.clone();
            let taken = match self.core.as_mut() {
                Some(core) => core.take_output(state.session),
                None => Vec::new(),
            };
            for out in taken {
                match out {
                    Ok(frames) => {
                        for batch in encode_ws_frame_batches(&frames).unwrap_or_default() {
                            batches.push(js_sys::Uint8Array::from(batch.as_slice()));
                        }
                    }
                    Err(error) => {
                        batches.push(js_sys::Uint8Array::from(error_frame(&error).as_slice()));
                        ended = true;
                    }
                }
            }
            if let Some(error) = failed {
                batches.push(js_sys::Uint8Array::from(error_frame(&error).as_slice()));
                ended = true;
            }
            if ended {
                self.close(socket);
            }
            Output { batches, ended }
        }

        /// Every open socket's number.
        pub fn sockets(&self) -> Vec<u32> {
            self.sockets.keys().copied().collect()
        }

        /// `socket` closed.
        pub fn close(&mut self, socket: u32) {
            if let Some(state) = self.sockets.remove(&socket) {
                if let Some(core) = self.core.as_mut() {
                    core.close_session(state.session);
                }
            }
        }
    }

    impl DurableObjectCore {
        /// Decode `message` into the socket's held input.
        fn read(state: &mut Socket, message: &[u8], limit: usize) -> Result<(), String> {
            let frames = decode_ws_encoded_frame_batch(message).map_err(|e| e.to_string())?;
            if !state.thin {
                state.hold(Held::Frames(frames, message.len()), message.len(), limit);
                return Ok(());
            }
            for frame in frames {
                let len = frame.len();
                let request = match jazz::wire::decode_frame(&frame).map_err(|e| e.to_string())? {
                    jazz::wire::WireFrame::ThinRequest(request) => Some((request, len, false)),
                    jazz::wire::WireFrame::ThinPart(part) => {
                        let total = part.total()?;
                        match state.assembler.push(part)? {
                            Some(joined) => match jazz::wire::decode_postcard_frame(&joined)
                                .map_err(|e| e.to_string())?
                            {
                                jazz::wire::WireFrame::ThinRequest(request) => {
                                    Some((request, total, true))
                                }
                                _ => return Err("a split thin frame is not a request".to_owned()),
                            },
                            None => None,
                        }
                    }
                    _ => return Err("a thin connection sends only thin requests".to_owned()),
                };
                if let Some((request, bytes, split)) = request {
                    state.hold(Held::Thin(request, bytes, split), bytes, limit);
                }
            }
            Ok(())
        }

        /// Test-only routes (`durable-object-tests` builds), for the admin:
        /// `POST test/panic` panics in the core; `POST test/limits` sets
        /// the held limits and pauses or resumes the engine.
        #[cfg(feature = "durable-object-tests")]
        fn test_route(
            &mut self,
            method: &str,
            route: &str,
            credentials: &HttpCredentials,
            body: &[u8],
        ) -> Option<HttpAnswer> {
            #[derive(serde::Deserialize)]
            struct Limits {
                socket: usize,
                global: usize,
                paused: bool,
            }
            if method != "POST" || !route.starts_with("test/") {
                return None;
            }
            if let Err(refused) = credentials.admin(&self.secret) {
                return Some(refused);
            }
            let storage = durable_object_storage(self.storage.clone());
            match route {
                // Panic mid-write, inside a storage transaction (the
                // object's `transactionSync`, its callback live and the
                // storage bridge borrowed): the write must not stay, and
                // the object must start afresh on a new instance.
                "test/panic" => {
                    let _ = storage.scope::<()>("jazz_test_panic", |storage| {
                        // As a panic inside the core leaves a thread-local
                        // borrowed (the trap never unwinds it): only a new
                        // instance has it free again (see `open`).
                        TRAPPED_BORROW.with(|cell| std::mem::forget(cell.borrow_mut()));
                        storage
                            .execute_batch(
                                "CREATE TABLE IF NOT EXISTS _jazz_test_panic (x INTEGER); \
                                 INSERT INTO _jazz_test_panic (x) VALUES (1);",
                            )
                            .map_err(|error| {
                                jazz::db::Error::new(
                                    jazz::db::ErrorCode::Storage,
                                    error.to_string(),
                                )
                            })?;
                        panic!("a test asked the core to panic inside a storage transaction")
                    });
                    unreachable!("the scope panics")
                }
                // How many rows a panicked write left (none).
                "test/panic-rows" => {
                    use jazz::storage::OptionalExtension;
                    let rows = storage
                        .prepare_cached("SELECT count(*) FROM _jazz_test_panic")
                        .and_then(|mut statement| {
                            statement
                                .query_row([], |row| row.get::<_, i64>(0))
                                .optional()
                        })
                        .ok()
                        .flatten()
                        .unwrap_or(0);
                    Some(answer(200, serde_json::json!({ "rows": rows })))
                }
                // Set the held limits and pause or resume the engine; the
                // answer is the limits that were in force.
                "test/limits" => {
                    let limits: Limits = serde_json::from_slice(body).expect("limits JSON");
                    let before = serde_json::json!({
                        "socket": self.held_limit,
                        "global": self.global_held_limit,
                    });
                    self.held_limit = limits.socket;
                    self.global_held_limit = limits.global;
                    self.paused = limits.paused;
                    Some(answer(200, before))
                }
                _ => None,
            }
        }

        /// While all sockets together hold more than the object may, end
        /// the one holding most.
        fn shed_held(&mut self) {
            while self.sockets.values().map(|s| s.held_bytes).sum::<usize>()
                > self.global_held_limit
            {
                let Some(largest) = self.sockets.values_mut().max_by_key(|s| s.held_bytes) else {
                    return;
                };
                largest.shed();
            }
        }

        /// Hand the engine what `socket` holds, oldest first, while its
        /// session's budget has room. An input the engine refuses (a thin
        /// request over the most one takes) ends that socket alone.
        fn admit_held(&mut self, socket: u32) {
            if self.paused {
                return;
            }
            let Some(core) = self.core.as_mut() else {
                return;
            };
            let Some(state) = self.sockets.get_mut(&socket) else {
                return;
            };
            while let Some(next) = state.held.pop_front() {
                let (admitted, bytes) = match &next {
                    Held::Frames(frames, bytes) => (
                        core.receive_frames(state.session, frames.clone(), *bytes),
                        *bytes,
                    ),
                    Held::Thin(request, bytes, split) => (
                        core.receive_thin_request(state.session, request.clone(), *bytes, *split),
                        *bytes,
                    ),
                };
                match admitted {
                    Ok(true) => state.held_bytes -= bytes,
                    Ok(false) => {
                        state.held.push_front(next);
                        break;
                    }
                    Err(error) => {
                        state.fail(error);
                        break;
                    }
                }
            }
        }

        /// Open the core if a schema is known: the store's published
        /// catalogue (the engine then serves its current revision), else
        /// `configured` (a public schema's JSON). Without either, the core
        /// waits for a deployment.
        fn open_core(&mut self, configured: Option<&str>) -> Result<(), JsValue> {
            if self.core.is_some() {
                return Ok(());
            }
            let file =
                jazz::host::StoreFile::from_storage(durable_object_storage(self.storage.clone()))
                    .map_err(js)?;
            let rows = jazz::host::HostTables::catalogue(&file).map_err(js)?;
            let schema = match (rows.schemas.first(), configured) {
                (Some(stored), _) => jazz::schema::JazzSchema::new(&stored.schema).map_err(js)?,
                (None, Some(json)) => {
                    let source: jazz::tools::public_schema::Schema =
                        serde_json::from_str(json).map_err(js)?;
                    jazz::schema::JazzSchema::new(&source).map_err(js)?
                }
                (None, None) => return Ok(()),
            };
            let any_stored = !rows.schemas.is_empty();
            let core = ServerCore::open(schema, file, any_stored, Some(self.app_id.clone()))
                .map_err(js)?;
            self.install(core).map_err(js)?;
            Ok(())
        }
    }

    /// What `http` answers: a status and a JSON body (empty for 204).
    struct HttpAnswer {
        status: u16,
        body: String,
        /// The body's content type (empty for no body).
        content_type: &'static str,
    }

    impl HttpAnswer {
        fn into_js(self) -> JsValue {
            object(&[
                ("status", JsValue::from(self.status)),
                ("body", JsValue::from_str(&self.body)),
                ("contentType", JsValue::from_str(self.content_type)),
            ])
        }
    }

    fn answer(status: u16, body: impl serde::Serialize) -> HttpAnswer {
        HttpAnswer {
            status,
            body: serde_json::to_string(&body).unwrap_or_default(),
            content_type: "application/json",
        }
    }

    /// What a revoked session is told, as native tells it.
    fn revoked() -> jazz::wire::WireError {
        jazz::wire::WireError::new(
            jazz::wire::WireErrorCode::AuthFailed,
            jazz::wire::WireRetry::AfterAuth,
            "session revoked: jazz.revokeUser ended this user's sessions",
        )
    }

    fn no_content() -> HttpAnswer {
        HttpAnswer {
            status: 204,
            body: String::new(),
            content_type: "",
        }
    }

    fn refusal(status: u16, error: jazz::tools::transport_error::ErrorResponse) -> HttpAnswer {
        answer(status, error)
    }

    /// The secrets an HTTP request carried: native's headers
    /// (`X-Jazz-Admin-Secret`, `X-Jazz-Backend-Secret`) or the credential
    /// pair a trusted caller vouches with (`X-Jazz-Backend-Credential` +
    /// `X-Jazz-Backend-Access`).
    #[derive(Default)]
    struct HttpCredentials {
        admin_secret: Option<String>,
        backend_secret: Option<String>,
        credential: Option<String>,
        access: Option<String>,
    }

    /// What a backend credential (`X-Jazz-Backend-Credential`) vouched
    /// for: the access stated next to it, `session` when none is.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Vouched {
        Admin,
        Backend,
        Session,
    }

    impl HttpCredentials {
        /// The credential pair, if one was sent: as native's workload gate,
        /// a credential that holds decides the access and the secret
        /// headers are ignored; one that does not, or an access no route
        /// knows, is refused.
        fn vouched(&self, secret: &str) -> Result<Option<Vouched>, HttpAnswer> {
            use jazz::tools::transport_error::ErrorResponse;
            let Some(credential) = &self.credential else {
                return Ok(None);
            };
            if !same_secret(secret, credential) {
                return Err(refusal(
                    401,
                    ErrorResponse::unauthorized("Invalid backend credential"),
                ));
            }
            match self.access.as_deref() {
                Some("admin") => Ok(Some(Vouched::Admin)),
                Some("backend") => Ok(Some(Vouched::Backend)),
                None => Ok(Some(Vouched::Session)),
                Some(other) => Err(refusal(
                    401,
                    ErrorResponse::unauthorized(format!("unknown backend access `{other}`")),
                )),
            }
        }

        /// As native's `validate_admin_secret`, by the core secret.
        fn admin(&self, secret: &str) -> Result<(), HttpAnswer> {
            use jazz::tools::transport_error::ErrorResponse;
            match self.vouched(secret)? {
                Some(Vouched::Admin) => return Ok(()),
                Some(_) => {
                    return Err(refusal(
                        401,
                        ErrorResponse::unauthorized("Admin secret required for this operation"),
                    ));
                }
                None => {}
            }
            match &self.admin_secret {
                Some(given) if same_secret(secret, given) => Ok(()),
                Some(_) => Err(refusal(
                    401,
                    ErrorResponse::unauthorized("Invalid admin secret"),
                )),
                None => Err(refusal(
                    401,
                    ErrorResponse::unauthorized("Admin secret required for this operation"),
                )),
            }
        }

        /// As native's `validate_backend_secret`, by the core secret.
        fn backend(&self, secret: &str) -> Result<(), HttpAnswer> {
            use jazz::tools::transport_error::ErrorResponse;
            match self.vouched(secret)? {
                Some(Vouched::Admin | Vouched::Backend) => return Ok(()),
                Some(Vouched::Session) => {
                    return Err(refusal(
                        401,
                        ErrorResponse::unauthorized("Backend secret required for backend access"),
                    ));
                }
                None => {}
            }
            match &self.backend_secret {
                Some(given) if same_secret(secret, given) => Ok(()),
                Some(_) => Err(refusal(
                    401,
                    ErrorResponse::unauthorized("Invalid backend secret"),
                )),
                None => Err(refusal(
                    401,
                    ErrorResponse::unauthorized("Backend secret required for backend access"),
                )),
            }
        }

        /// Whoever holds the core secret, in any role: the backend's
        /// credential with any access, or either secret header.
        fn holder(&self, secret: &str) -> Result<(), HttpAnswer> {
            if self.vouched(secret)?.is_some() {
                return Ok(());
            }
            if self.admin_secret.is_some() {
                return self.admin(secret);
            }
            self.backend(secret)
        }
    }

    /// The route a request's `path` names: `app` for `/app`, else the part
    /// after `/apps/{x}/`. The object serves its one app, whatever `x` is.
    fn route_of(path: &str) -> Option<&str> {
        if path == "/app" {
            return Some("app");
        }
        let rest = path.strip_prefix("/apps/")?;
        let (app, route) = rest.split_once('/')?;
        (!app.is_empty() && !route.is_empty()).then_some(route)
    }

    /// The app id the object keeps in its own SQL, made on its first open
    /// (as a native core keeps `<data-dir>/app-id`): nobody configures it.
    fn stored_app_id(storage: &jazz::storage::Storage) -> Result<String, String> {
        use jazz::storage::OptionalExtension;
        storage
            .execute_batch("CREATE TABLE IF NOT EXISTS _jazz_host_app (id TEXT NOT NULL)")
            .map_err(|error| error.to_string())?;
        let stored = storage
            .prepare_cached("SELECT id FROM _jazz_host_app LIMIT 1")
            .and_then(|mut statement| {
                statement
                    .query_row([], |row| row.get::<_, String>(0))
                    .optional()
            })
            .map_err(|error| error.to_string())?;
        if let Some(id) = stored {
            return Ok(id);
        }
        let id = uuid::Uuid::new_v4().to_string();
        storage
            .execute("INSERT INTO _jazz_host_app (id) VALUES (?1)", [&id])
            .map_err(|error| error.to_string())?;
        Ok(id)
    }

    #[wasm_bindgen]
    impl DurableObjectCore {
        /// The app id this object serves (see `stored_app_id`).
        #[wasm_bindgen(js_name = appId)]
        pub fn app_id(&self) -> String {
            self.app_id.clone()
        }

        /// Check an HTTP request to `path` before its body is read: the
        /// refusal to answer (404, 401), or `undefined` when `http` may
        /// serve it. The secrets are what it carried: native's headers
        /// (`X-Jazz-Admin-Secret`, `X-Jazz-Backend-Secret`) or the
        /// backend's credential (`X-Jazz-Backend-Credential`, with
        /// `X-Jazz-Backend-Access`).
        pub fn authorize(
            &self,
            method: &str,
            path: &str,
            admin_secret: Option<String>,
            backend_secret: Option<String>,
            credential: Option<String>,
            access: Option<String>,
        ) -> JsValue {
            let credentials = HttpCredentials {
                admin_secret,
                backend_secret,
                credential,
                access,
            };
            match self.check(method, path, &credentials) {
                Ok(_) => JsValue::UNDEFINED,
                Err(refused) => refused.into_js(),
            }
        }

        /// Serve an HTTP request to `path` as the native server serves it
        /// (checked again as `authorize` checks it).
        #[allow(clippy::too_many_arguments)]
        pub fn http(
            &mut self,
            method: &str,
            path: &str,
            admin_secret: Option<String>,
            backend_secret: Option<String>,
            credential: Option<String>,
            access: Option<String>,
            body: &[u8],
        ) -> JsValue {
            let credentials = HttpCredentials {
                admin_secret,
                backend_secret,
                credential,
                access,
            };
            match self.check(method, path, &credentials) {
                Ok(route) => self.serve(method, route, &credentials, body).into_js(),
                Err(refused) => refused.into_js(),
            }
        }
    }

    impl DurableObjectCore {
        /// The route `path` names, if it exists and the request's secrets
        /// grant it.
        fn check<'p>(
            &self,
            method: &str,
            path: &'p str,
            credentials: &HttpCredentials,
        ) -> Result<&'p str, HttpAnswer> {
            use jazz::tools::transport_error::ErrorResponse;
            let not_found = || refusal(404, ErrorResponse::not_found("route not found"));
            let route = route_of(path).ok_or_else(not_found)?;
            let account = route
                .strip_prefix("admin/accounts/")
                .and_then(AccountRoute::from_path);
            match (method, route) {
                ("GET", "app") => credentials.holder(&self.secret)?,
                ("POST", "backend/admit") => credentials.backend(&self.secret)?,
                // As native: the admin secret, or for the routes a backend
                // vouches for (resolve, login-or-register,
                // found-local-first), the backend's when no admin secret is
                // sent; link, revoke and revoke-sessions are the admin's.
                ("POST", _) if account.is_some() => match account.expect("matched").authority() {
                    RouteAuthority::VouchingBackend if credentials.admin_secret.is_none() => {
                        credentials.backend(&self.secret)?
                    }
                    _ => credentials.admin(&self.secret)?,
                },
                ("GET", "admin/migrations/graph" | "admin/bundles") | ("POST", "admin/bundles") => {
                    credentials.admin(&self.secret)?
                }
                #[cfg(feature = "durable-object-tests")]
                ("POST", "test/panic" | "test/panic-rows" | "test/limits") => {
                    credentials.admin(&self.secret)?
                }
                _ => return Err(not_found()),
            }
            Ok(route)
        }

        fn serve(
            &mut self,
            method: &str,
            route: &str,
            credentials: &HttpCredentials,
            body: &[u8],
        ) -> HttpAnswer {
            use jazz::tools::transport_error::ErrorResponse;
            #[cfg(feature = "durable-object-tests")]
            if let Some(answer) = self.test_route(method, route, credentials, body) {
                return answer;
            }
            #[cfg(not(feature = "durable-object-tests"))]
            let _ = credentials;
            // `check` found the route and its secrets hold.
            if let Some(account) = route
                .strip_prefix("admin/accounts/")
                .and_then(AccountRoute::from_path)
            {
                return self.account(account, body);
            }
            match (method, route) {
                ("GET", "app") => answer(200, serde_json::json!({ "appId": self.app_id })),
                // The backend's credential holds: it may open sessions.
                ("POST", "backend/admit") => no_content(),
                ("GET", "admin/migrations/graph") => match self.catalogue_index() {
                    Ok(index) => answer(200, index.migration_graph()),
                    Err(error) => refusal(500, ErrorResponse::internal(error)),
                },
                ("GET", "admin/bundles") => match self.catalogue_index() {
                    Ok(index) => answer(200, published_bundles(&index)),
                    Err(error) => refusal(500, ErrorResponse::internal(error)),
                },
                _ => {
                    if let Err(error) = serde_json::from_slice::<DeployBody>(body) {
                        return refusal(400, ErrorResponse::bad_request(error.to_string()));
                    }
                    match self.deploy(body) {
                        Ok(response) => answer(200, response),
                        Err(error) => answer(error.status.as_u16(), error.body),
                    }
                }
            }
        }
    }

    /// The published-bundles answer, as native's `GET admin/bundles`:
    /// oldest first, the last what a backend booting now pins to.
    fn published_bundles(index: &CatalogueIndex) -> serde_json::Value {
        let bundles = index
            .published_bundles()
            .iter()
            .map(|bundle| {
                serde_json::json!({
                    "bundle": bundle.id.to_string(),
                    "schemaHash": bundle.schema.to_string(),
                    "publishedAt": bundle.published_at,
                    "permissions": bundle
                        .permissions
                        .iter()
                        .map(|(table, policies)| (table.to_string(), policies))
                        .collect::<HashMap<_, _>>(),
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({ "bundles": bundles })
    }

    impl DurableObjectCore {
        /// The store's catalogue: the open core's, else read from the
        /// store itself (nothing published yet, or only a configured
        /// schema).
        fn catalogue_rows(&self) -> Result<jazz::catalogue::CatalogueRows, String> {
            match &self.core {
                Some(core) => core.db().catalogue().map_err(|error| error.to_string()),
                None => {
                    let file = jazz::host::StoreFile::from_storage(durable_object_storage(
                        self.storage.clone(),
                    ))
                    .map_err(|error| error.to_string())?;
                    jazz::host::HostTables::catalogue(&file).map_err(|error| error.to_string())
                }
            }
        }

        /// The account registry over the object's SQL, its tables made on
        /// first use.
        fn registry(&mut self) -> Result<jazz::storage::Storage, String> {
            let storage = durable_object_storage(self.storage.clone());
            if !self.accounts_open {
                accounts::open(&storage).map_err(|error| error.to_string())?;
                self.accounts_open = true;
            }
            Ok(storage)
        }

        /// The revocation mark of `principal`'s sessions, if any.
        fn revocation_mark(
            &mut self,
            principal: &jazz::account_registry::Principal,
        ) -> Result<Option<u64>, String> {
            let storage = self.registry()?;
            accounts::sessions_revoked_before(&storage, principal)
                .map_err(|error| error.to_string())
        }

        /// After an assignment may have changed: end every guest socket
        /// whose identity no longer holds its account, as native ends it.
        fn recheck_guests(&mut self, storage: &jazz::storage::Storage) {
            use jazz::wire::{WireError, WireErrorCode, WireRetry};
            for state in self.sockets.values_mut() {
                let Some(identity) = state.guest else {
                    continue;
                };
                let (issuer, subject) = identity.principal_parts();
                let principal = jazz::account_registry::Principal { issuer, subject };
                // As native's `account_still_admitted`: the assignment must
                // still be the account the guest was admitted as; a refusal
                // is told as the registry words it, and a registry that
                // cannot be read closes the socket for a later retry.
                let ends = match (identity.account_id(), accounts::login(storage, &principal)) {
                    (Some(account), Ok(assignment)) if assignment.account == account => None,
                    (_, Err(accounts::RegistryError::Unavailable(_))) => Some(WireError::new(
                        WireErrorCode::NotReady,
                        WireRetry::Later,
                        "account registry unavailable",
                    )),
                    (_, Err(accounts::RegistryError::Decision(error))) => Some(WireError::new(
                        WireErrorCode::AuthFailed,
                        WireRetry::Never,
                        error.to_string(),
                    )),
                    _ => Some(WireError::new(
                        WireErrorCode::AuthFailed,
                        WireRetry::Never,
                        "account identity revoked",
                    )),
                };
                if let Some(error) = ends {
                    state.held.clear();
                    state.held_bytes = 0;
                    state.failed.get_or_insert(error);
                }
            }
        }

        /// Serve an account route (`admin/accounts/...`) on the store's
        /// account registry, as native does. A revocation of a user's
        /// sessions ends the stated sessions of that user authenticated
        /// before its mark (their sockets get native's error and close);
        /// later ones are refused at admission.
        fn account(&mut self, route: AccountRoute, body: &[u8]) -> HttpAnswer {
            use jazz::tools::transport_error::ErrorResponse;
            let storage = match self.registry() {
                Ok(storage) => storage,
                Err(error) => return refusal(503, ErrorResponse::internal(error)),
            };
            let Ok(app_id) = uuid::Uuid::parse_str(&self.app_id) else {
                return refusal(500, ErrorResponse::internal("the app id is not a UUID"));
            };
            let encrypts = self.encrypts();
            let (reply, effects) = accounts::serve(
                &storage,
                route,
                body,
                RouteContext {
                    app_id,
                    now_ms: js_sys::Date::now() as u64,
                    fresh_id: &mut uuid::Uuid::new_v4,
                    encrypts: &|| encrypts,
                },
            );
            if effects.assignments_changed {
                self.recheck_guests(&storage);
            }
            if let Some((principal, mark)) = effects.sessions_revoked {
                for state in self.sockets.values_mut() {
                    let ends = state.revocation.as_ref().is_some_and(|revocation| {
                        revocation.principal == principal
                            && accounts::session_revoked(Some(mark), revocation.authenticated_at_ms)
                    });
                    if ends {
                        state.held.clear();
                        state.held_bytes = 0;
                        state.failed.get_or_insert_with(revoked);
                    }
                }
            }
            let (body, content_type) = match reply.body {
                RouteBody::Empty => (String::new(), ""),
                RouteBody::Json(body) => (body, "application/json"),
                RouteBody::Text(body) => (body, "text/plain; charset=utf-8"),
            };
            HttpAnswer {
                status: reply.status,
                body,
                content_type,
            }
        }

        /// Whether the app has Jazz's system tables, for the rule on
        /// foreign founding (`refuse_foreign_founding`): no core is no
        /// schema, and a schema that cannot be read counts as encrypting.
        fn encrypts(&self) -> bool {
            self.core
                .as_ref()
                .is_some_and(|core| core.encrypts().unwrap_or(true))
        }

        fn catalogue_index(&self) -> Result<CatalogueIndex, String> {
            Ok(CatalogueIndex::from_rows(&self.catalogue_rows()?, None))
        }

        /// Publish the deploy body in `body` (valid JSON, checked by the
        /// caller; a request and its encryption declarations), as native's `POST admin/bundles`: validate it against
        /// the core's catalogue and apply its rows and admissions in one
        /// transaction, refused if the catalogue moved since it was staged
        /// (`apply_catalogue`). Without a core yet, the deployment is
        /// probed on a scratch store first, so one the catalogue refuses
        /// never opens the app's store on its schema, and the core is
        /// opened on its first schema; it is then staged again on that
        /// core's catalogue (which admitted the schema it opened on). A
        /// bundle new to the store ends the sessions pinned to others.
        fn deploy(&mut self, body: &[u8]) -> Result<DeploymentResponse, DeployError> {
            let host = ObjectHost {
                storage: self.storage.clone(),
            };
            let now = js_sys::Date::now() as u64;
            let deployed = deployment::deploy(&mut self.core, &host, &self.app_id, body, now);
            // A core it opened has the registry installed, its tables made.
            self.accounts_open |= self.core.is_some();
            deployed
        }

        /// Keep `core` as the open core, with the account registry its
        /// checks read installed (see [`ObjectHost`]).
        fn install(&mut self, mut core: ServerCore) -> Result<(), String> {
            deployment::install_registry(
                &ObjectHost {
                    storage: self.storage.clone(),
                },
                &mut core,
            )?;
            // Installing it made the registry's tables (`registry`).
            self.accounts_open = true;
            self.core = Some(core);
            Ok(())
        }
    }

    /// The object's SQL as the portable deploy reaches it: store handles,
    /// and the account registry its checks read, installed over the same
    /// SQL, as native installs the one beside its store.
    struct ObjectHost {
        storage: ObjectStorage,
    }

    impl deployment::CoreHost for ObjectHost {
        fn store(&self) -> Result<jazz::host::StoreFile, String> {
            jazz::host::StoreFile::from_storage(durable_object_storage(self.storage.clone()))
                .map_err(|error| error.to_string())
        }

        /// The registry's tables are made first, so a check never meets a
        /// store without them.
        fn install_registry(&self, core: &mut ServerCore) -> Result<(), String> {
            accounts::open(&durable_object_storage(self.storage.clone()))
                .map_err(|error| error.to_string())?;
            // The core's checks read the registry on the object's own SQL,
            // which sees what the registry wrote.
            if core.install_account_registry(durable_object_storage(self.storage.clone())) {
                Ok(())
            } else {
                Err("the account registry needs the core opened for an app".to_owned())
            }
        }
    }

    /// What `output` answers.
    struct Output {
        batches: Vec<js_sys::Uint8Array>,
        ended: bool,
    }
}
