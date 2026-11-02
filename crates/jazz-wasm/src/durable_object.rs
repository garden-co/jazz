//! The engine on a Cloudflare Durable Object's own SQLite.
//!
//! This build has no SQLite compiled in: the `sqlite3_*` functions rusqlite
//! calls are defined here, over `ctx.storage.sql.exec`. A statement keeps
//! its text and bound values; its first step runs it and holds the rows,
//! later steps walk them. Every connection rusqlite opens is the object's
//! one database.
//!
//! What Durable Object SQLite cannot do, this refuses loudly rather than
//! approximate:
//! - `SAVEPOINT` and `RELEASE` are counted, not run: the object commits a
//!   turn's writes atomically, and `sqlite3_get_autocommit` reports whether
//!   one is open. Rolling one back (`ROLLBACK TO`) is not supported yet: it
//!   needs the engine's scopes to run inside `transactionSync`.
//! - Integers come back as JS numbers: one beyond 2^53 is refused. Bound
//!   ones beyond 2^53 go as decimal text, which an INTEGER column's
//!   affinity turns back into an integer.
//! - Text comes back as JS strings, so bound text must be UTF-8.
//! - A number with no fractional part reads as an INTEGER, whatever the
//!   column held.
//!
//! Proof-of-concept code: see dev/durable-objects/README.md.

use std::cell::{Cell as StdCell, RefCell};
use std::ffi::{c_char, c_int, c_uchar, c_uint, c_void, CStr};
use std::ptr;

use js_sys::{Array, ArrayBuffer, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::WasmDb;

#[wasm_bindgen]
extern "C" {
    /// `ctx.storage.sql`.
    #[derive(Clone)]
    pub type SqlStorage;
    #[wasm_bindgen(method, catch, variadic)]
    fn exec(this: &SqlStorage, query: &str, bindings: &Array) -> Result<SqlCursor, JsValue>;

    type SqlCursor;
    #[wasm_bindgen(method)]
    fn raw(this: &SqlCursor) -> JsValue;
    #[wasm_bindgen(method, getter, js_name = columnNames)]
    fn column_names(this: &SqlCursor) -> Array;
    #[wasm_bindgen(method, getter, js_name = rowsRead)]
    fn rows_read(this: &SqlCursor) -> f64;
    #[wasm_bindgen(method, getter, js_name = rowsWritten)]
    fn rows_written(this: &SqlCursor) -> f64;
}

thread_local! {
    static STORAGE: RefCell<Option<SqlStorage>> = const { RefCell::new(None) };
    static STATS: RefCell<Stats> = RefCell::new(Stats::default());
    /// Savepoints open on the object's database.
    static DEPTH: StdCell<u32> = const { StdCell::new(0) };
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

#[wasm_bindgen]
impl WasmDb {
    /// Open the app's authority on a Durable Object's SQLite: `sql` is the
    /// object's `ctx.storage.sql`. One per object; it writes as SYSTEM and
    /// serves subscribers, with nothing upstream.
    #[wasm_bindgen(js_name = openDurableObject)]
    pub fn open_durable_object(
        sql: SqlStorage,
        schema: Vec<u8>,
        config: Vec<u8>,
    ) -> Result<WasmDb, JsValue> {
        use crate::binding_host;
        STORAGE.with(|storage| *storage.borrow_mut() = Some(sql));
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
        let db =
            crate::block_on(binding_host::open_memory(db_config)).map_err(crate::to_js_error)?;
        db.set_authority();
        Ok(Self::runtime_owner(crate::Binding::new(
            db, identity, trusted,
        )))
    }
}

/// What the Durable Object SQLite shim did so far: `{ execs, rowsRead,
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

const SQLITE_OK: c_int = 0;
const SQLITE_ERROR: c_int = 1;
const SQLITE_INTERRUPT: c_int = 9;
const SQLITE_CANTOPEN: c_int = 14;
const SQLITE_TOOBIG: c_int = 18;
const SQLITE_CONSTRAINT: c_int = 19;
const SQLITE_MISUSE: c_int = 21;
const SQLITE_RANGE: c_int = 25;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;
const SQLITE_CONSTRAINT_CHECK: c_int = SQLITE_CONSTRAINT | (1 << 8);
const SQLITE_CONSTRAINT_FOREIGNKEY: c_int = SQLITE_CONSTRAINT | (3 << 8);
const SQLITE_CONSTRAINT_NOTNULL: c_int = SQLITE_CONSTRAINT | (5 << 8);
const SQLITE_CONSTRAINT_PRIMARYKEY: c_int = SQLITE_CONSTRAINT | (6 << 8);
const SQLITE_CONSTRAINT_UNIQUE: c_int = SQLITE_CONSTRAINT | (8 << 8);

const SQLITE_INTEGER: c_int = 1;
const SQLITE_FLOAT: c_int = 2;
const SQLITE_TEXT: c_int = 3;
const SQLITE_BLOB: c_int = 4;
const SQLITE_NULL: c_int = 5;

/// Durable Object SQLite's limits, by `SQLITE_LIMIT_*` id (see
/// jazz-db's `durable_object_limits`).
fn limit(id: c_int) -> c_int {
    match id {
        0 => 2 * 1024 * 1024, // LENGTH
        1 => 100_000,         // SQL_LENGTH
        2 => 100,             // COLUMN
        3 => 100,             // EXPR_DEPTH
        4 => 5,               // COMPOUND_SELECT
        6 => 127,             // FUNCTION_ARG
        8 => 50,              // LIKE_PATTERN_LENGTH
        9 => 100,             // VARIABLE_NUMBER
        _ => -1,
    }
}

const MAX_SAFE: i64 = (1 << 53) - 1;

// ---- connections and statements --------------------------------------------

type CommitHook = unsafe extern "C" fn(*mut c_void) -> c_int;

struct Conn {
    sql: SqlStorage,
    code: c_int,
    message: Vec<u8>,
    commit_hook: Option<(CommitHook, *mut c_void)>,
}

impl Conn {
    fn fail(&mut self, code: c_int, message: impl Into<String>) -> c_int {
        self.code = code;
        self.message = message.into().into_bytes();
        self.message.push(0);
        code
    }

    fn ok(&mut self) {
        self.code = SQLITE_OK;
        self.message.clear();
    }

    /// Run `query`; a refusal is recorded as this connection's error.
    fn run(&mut self, query: &str, bindings: &Array) -> Result<SqlCursor, c_int> {
        match self.sql.exec(query, bindings) {
            Ok(cursor) => {
                STATS.with(|stats| stats.borrow_mut().execs += 1.0);
                Ok(cursor)
            }
            Err(error) => {
                let message = js_message(&error);
                let code = error_code(&message);
                let shown: String = query.chars().take(300).collect();
                Err(self.fail(code, format!("{message} (in `{shown}`)")))
            }
        }
    }

    /// One integer from a query of one row and column.
    fn lookup(&mut self, query: &str) -> i64 {
        STATS.with(|stats| stats.borrow_mut().lookups += 1.0);
        let Ok(cursor) = self.run(query, &Array::new()) else {
            return 0;
        };
        let rows = Array::from(&cursor.raw());
        rows.get(0)
            .dyn_into::<Array>()
            .ok()
            .and_then(|row| row.get(0).as_f64())
            .unwrap_or(0.0) as i64
    }

    fn committed(&mut self) {
        if let Some((hook, arg)) = self.commit_hook {
            // A hook asking to turn the commit into a rollback has nothing to
            // undo: Durable Object SQLite already holds the writes.
            unsafe { hook(arg) };
        }
    }
}

/// What a statement's text does, beyond running it.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Run,
    Empty,
    Savepoint,
    Release,
    RollbackTo,
}

enum Value {
    Null,
    Integer(i64),
    Real(f64),
    /// With a trailing NUL, which `sqlite3_column_bytes` leaves out.
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

struct Stmt {
    conn: *mut Conn,
    query: String,
    kind: Kind,
    bindings: Vec<JsValue>,
    columns: Vec<Vec<u8>>,
    rows: Vec<Vec<Value>>,
    /// The row a step last returned, plus one; 0 before the first step.
    at: usize,
    ran: bool,
}

impl Stmt {
    /// The connection lives until its statements are finalized.
    fn conn<'a>(&self) -> &'a mut Conn {
        unsafe { &mut *self.conn }
    }

    fn cell(&self, col: c_int) -> Option<&Value> {
        self.rows.get(self.at.checked_sub(1)?)?.get(col as usize)
    }

    fn bind(&mut self, index: c_int, value: JsValue) -> c_int {
        let Some(slot) = (index as usize)
            .checked_sub(1)
            .and_then(|i| self.bindings.get_mut(i))
        else {
            return SQLITE_RANGE;
        };
        *slot = value;
        SQLITE_OK
    }

    fn execute(&mut self) -> c_int {
        let conn = self.conn();
        match self.kind {
            Kind::Empty => return SQLITE_DONE,
            Kind::Savepoint => {
                DEPTH.with(|depth| depth.set(depth.get() + 1));
                STATS.with(|stats| stats.borrow_mut().savepoints += 1.0);
                return SQLITE_DONE;
            }
            Kind::Release => {
                let open = DEPTH.with(|depth| {
                    let open = depth.get().saturating_sub(1);
                    depth.set(open);
                    open
                });
                if open == 0 {
                    conn.committed();
                }
                return SQLITE_DONE;
            }
            Kind::RollbackTo => {
                return conn.fail(
                    SQLITE_ERROR,
                    "rolling back a savepoint is not supported on Durable Object SQLite yet",
                );
            }
            Kind::Run => {}
        }
        let bindings: Array = self.bindings.iter().collect();
        let cursor = match conn.run(&self.query, &bindings) {
            Ok(cursor) => cursor,
            Err(code) => return code,
        };
        let raw = Array::from(&cursor.raw());
        STATS.with(|stats| {
            let mut stats = stats.borrow_mut();
            stats.rows_read += cursor.rows_read();
            stats.rows_written += cursor.rows_written();
        });
        self.columns = cursor
            .column_names()
            .iter()
            .map(|name| {
                let mut name = name.as_string().unwrap_or_default().into_bytes();
                name.push(0);
                name
            })
            .collect();
        let mut rows = Vec::with_capacity(raw.length() as usize);
        for row in raw.iter() {
            let row: Array = row.unchecked_into();
            let mut cells = Vec::with_capacity(row.length() as usize);
            for cell in row.iter() {
                match value(cell) {
                    Ok(cell) => cells.push(cell),
                    Err(message) => return conn.fail(SQLITE_TOOBIG, message),
                }
            }
            rows.push(cells);
        }
        self.rows = rows;
        if DEPTH.with(StdCell::get) == 0 {
            conn.committed();
        }
        conn.ok();
        SQLITE_OK
    }
}

fn value(cell: JsValue) -> Result<Value, String> {
    if cell.is_null() || cell.is_undefined() {
        return Ok(Value::Null);
    }
    if let Some(n) = cell.as_f64() {
        if n.fract() != 0.0 || !n.is_finite() {
            return Ok(Value::Real(n));
        }
        if n.abs() > MAX_SAFE as f64 {
            return Err(format!(
                "the integer {n} is beyond 2^53, which Durable Object SQLite returns inexactly: not supported yet"
            ));
        }
        return Ok(Value::Integer(n as i64));
    }
    if let Some(text) = cell.as_string() {
        let mut bytes = text.into_bytes();
        bytes.push(0);
        return Ok(Value::Text(bytes));
    }
    if let Some(buffer) = cell.dyn_ref::<ArrayBuffer>() {
        return Ok(Value::Blob(Uint8Array::new(buffer).to_vec()));
    }
    if let Some(bytes) = cell.dyn_ref::<Uint8Array>() {
        return Ok(Value::Blob(bytes.to_vec()));
    }
    Err("Durable Object SQLite returned a value of unknown type".into())
}

fn js_message(error: &JsValue) -> String {
    error
        .dyn_ref::<js_sys::Error>()
        .map(|error| String::from(error.message()))
        .or_else(|| error.as_string())
        .unwrap_or_else(|| format!("{error:?}"))
}

/// The SQLite result code a Durable Object's error message names.
fn error_code(message: &str) -> c_int {
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

/// How many parameters `query` takes: SQLite numbers a bare `?` one past
/// the highest so far, and `?NNN` explicitly. Named parameters are not
/// supported yet (jazz-db binds by position).
fn parameter_count(query: &str) -> usize {
    let bytes = query.as_bytes();
    let (mut i, mut highest) = (0, 0usize);
    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    i += 1;
                }
            }
            b'[' => {
                while i < bytes.len() && bytes[i] != b']' {
                    i += 1;
                }
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
            b'?' => {
                let start = i + 1;
                let mut end = start;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                highest = if end > start {
                    highest.max(query[start..end].parse().unwrap_or(0))
                } else {
                    highest + 1
                };
                i = end - 1;
            }
            _ => {}
        }
        i += 1;
    }
    highest
}

/// SQLite's `sqlite3_complete`, for the statements jazz-db writes: the
/// text ends in a `;` outside literals and comments, and a `CREATE
/// TRIGGER` only at `; END;`.
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

fn kind(query: &str) -> Kind {
    let words: Vec<String> = query
        .split(|c: char| c.is_whitespace() || c == ';')
        .filter(|word| !word.is_empty())
        .take(2)
        .map(str::to_ascii_uppercase)
        .collect();
    match words.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        [] => Kind::Empty,
        ["SAVEPOINT", ..] => Kind::Savepoint,
        ["RELEASE", ..] => Kind::Release,
        ["ROLLBACK", "TO"] => Kind::RollbackTo,
        _ => Kind::Run,
    }
}

// ---- the C API rusqlite links against ----------------------------------------
//
// Signatures match sqlite-wasm-rs's declarations exactly: wasm-ld turns a
// mismatched one into a trap.

unsafe fn conn<'a>(db: *mut c_void) -> &'a mut Conn {
    &mut *(db as *mut Conn)
}

unsafe fn stmt<'a>(s: *mut c_void) -> Option<&'a mut Stmt> {
    (s as *mut Stmt).as_mut()
}

/// Run a bound value's destructor, unless it is `SQLITE_STATIC` (null) or
/// `SQLITE_TRANSIENT` (-1): the value was copied already.
unsafe fn destroy(data: *const c_void, destructor: Option<unsafe extern "C" fn(*mut c_void)>) {
    if let Some(destructor) = destructor {
        if destructor as usize != usize::MAX {
            destructor(data as *mut c_void);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_open_v2(
    _filename: *const c_char,
    pp_db: *mut *mut c_void,
    _flags: c_int,
    _vfs: *const c_char,
) -> c_int {
    let Some(sql) = STORAGE.with(|storage| storage.borrow().clone()) else {
        *pp_db = ptr::null_mut();
        return SQLITE_CANTOPEN;
    };
    *pp_db = Box::into_raw(Box::new(Conn {
        sql,
        code: SQLITE_OK,
        message: Vec::new(),
        commit_hook: None,
    })) as *mut c_void;
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_close(db: *mut c_void) -> c_int {
    if !db.is_null() {
        drop(Box::from_raw(db as *mut Conn));
    }
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_prepare_v3(
    db: *mut c_void,
    z_sql: *const c_char,
    n_byte: c_int,
    _flags: c_uint,
    pp_stmt: *mut *mut c_void,
    pz_tail: *mut *const c_char,
) -> c_int {
    let bytes = if n_byte < 0 {
        CStr::from_ptr(z_sql).to_bytes()
    } else {
        let len = n_byte as usize;
        let all = std::slice::from_raw_parts(z_sql as *const u8, len);
        // SQLite stops at a NUL inside `nByte`.
        &all[..all.iter().position(|&b| b == 0).unwrap_or(len)]
    };
    // The whole text is one statement: Durable Object `exec` runs several.
    if !pz_tail.is_null() {
        *pz_tail = z_sql.add(bytes.len());
    }
    let conn = conn(db);
    let Ok(query) = std::str::from_utf8(bytes) else {
        *pp_stmt = ptr::null_mut();
        return conn.fail(SQLITE_MISUSE, "statement text is not UTF-8");
    };
    let kind = kind(query);
    if kind == Kind::Empty {
        *pp_stmt = ptr::null_mut();
        conn.ok();
        return SQLITE_OK;
    }
    let count = if kind == Kind::Run {
        parameter_count(query)
    } else {
        0
    };
    *pp_stmt = Box::into_raw(Box::new(Stmt {
        conn: conn as *mut Conn,
        query: query.to_owned(),
        kind,
        bindings: vec![JsValue::NULL; count],
        columns: Vec::new(),
        rows: Vec::new(),
        at: 0,
        ran: false,
    })) as *mut c_void;
    conn.ok();
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_finalize(s: *mut c_void) -> c_int {
    if !s.is_null() {
        drop(Box::from_raw(s as *mut Stmt));
    }
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_step(s: *mut c_void) -> c_int {
    let Some(stmt) = stmt(s) else {
        return SQLITE_MISUSE;
    };
    if !stmt.ran {
        stmt.ran = true;
        let code = stmt.execute();
        if code != SQLITE_OK && code != SQLITE_DONE {
            return code;
        }
    }
    if stmt.at < stmt.rows.len() {
        stmt.at += 1;
        SQLITE_ROW
    } else {
        stmt.at = stmt.rows.len() + 1;
        SQLITE_DONE
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_reset(s: *mut c_void) -> c_int {
    if let Some(stmt) = stmt(s) {
        stmt.ran = false;
        stmt.at = 0;
        stmt.rows.clear();
    }
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_clear_bindings(s: *mut c_void) -> c_int {
    if let Some(stmt) = stmt(s) {
        stmt.bindings.fill(JsValue::NULL);
    }
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_count(s: *mut c_void) -> c_int {
    stmt(s).map_or(0, |stmt| stmt.bindings.len() as c_int)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_null(s: *mut c_void, i: c_int) -> c_int {
    stmt(s).map_or(SQLITE_MISUSE, |stmt| stmt.bind(i, JsValue::NULL))
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_int64(s: *mut c_void, i: c_int, v: i64) -> c_int {
    let value = if (-MAX_SAFE..=MAX_SAFE).contains(&v) {
        JsValue::from_f64(v as f64)
    } else {
        // Durable Object SQLite refuses BigInt; an INTEGER column's
        // affinity turns the text back into this integer.
        STATS.with(|stats| stats.borrow_mut().wide_int_binds += 1.0);
        JsValue::from_str(&v.to_string())
    };
    stmt(s).map_or(SQLITE_MISUSE, |stmt| stmt.bind(i, value))
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_double(s: *mut c_void, i: c_int, v: f64) -> c_int {
    stmt(s).map_or(SQLITE_MISUSE, |stmt| stmt.bind(i, JsValue::from_f64(v)))
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_text64(
    s: *mut c_void,
    i: c_int,
    data: *const c_char,
    len: u64,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
    _encoding: c_uchar,
) -> c_int {
    let bytes = std::slice::from_raw_parts(data as *const u8, len as usize);
    let text = std::str::from_utf8(bytes).map(JsValue::from_str);
    destroy(data as *const c_void, destructor);
    let Some(stmt) = stmt(s) else {
        return SQLITE_MISUSE;
    };
    match text {
        Ok(text) => stmt.bind(i, text),
        Err(_) => stmt.conn().fail(
            SQLITE_MISUSE,
            "text that is not UTF-8 is not supported on Durable Object SQLite yet",
        ),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_blob64(
    s: *mut c_void,
    i: c_int,
    data: *const c_void,
    len: u64,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
) -> c_int {
    let bytes = if len == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(data as *const u8, len as usize)
    };
    let value: JsValue = Uint8Array::from(bytes).buffer().into();
    destroy(data, destructor);
    stmt(s).map_or(SQLITE_MISUSE, |stmt| stmt.bind(i, value))
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_zeroblob(s: *mut c_void, i: c_int, n: c_int) -> c_int {
    let value: JsValue = ArrayBuffer::new(n.max(0) as u32).into();
    stmt(s).map_or(SQLITE_MISUSE, |stmt| stmt.bind(i, value))
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_count(s: *mut c_void) -> c_int {
    stmt(s).map_or(0, |stmt| stmt.columns.len() as c_int)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_name(s: *mut c_void, n: c_int) -> *const c_char {
    stmt(s)
        .and_then(|stmt| stmt.columns.get(n as usize))
        .map_or(ptr::null(), |name| name.as_ptr() as *const c_char)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_type(s: *mut c_void, col: c_int) -> c_int {
    match stmt(s).and_then(|stmt| stmt.cell(col)) {
        Some(Value::Integer(_)) => SQLITE_INTEGER,
        Some(Value::Real(_)) => SQLITE_FLOAT,
        Some(Value::Text(_)) => SQLITE_TEXT,
        Some(Value::Blob(_)) => SQLITE_BLOB,
        Some(Value::Null) | None => SQLITE_NULL,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_int64(s: *mut c_void, col: c_int) -> i64 {
    match stmt(s).and_then(|stmt| stmt.cell(col)) {
        Some(Value::Integer(v)) => *v,
        Some(Value::Real(v)) => *v as i64,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_double(s: *mut c_void, col: c_int) -> f64 {
    match stmt(s).and_then(|stmt| stmt.cell(col)) {
        Some(Value::Integer(v)) => *v as f64,
        Some(Value::Real(v)) => *v,
        _ => 0.0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_text(s: *mut c_void, col: c_int) -> *const c_uchar {
    match stmt(s).and_then(|stmt| stmt.cell(col)) {
        Some(Value::Text(bytes)) => bytes.as_ptr(),
        _ => ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_blob(s: *mut c_void, col: c_int) -> *const c_void {
    match stmt(s).and_then(|stmt| stmt.cell(col)) {
        Some(Value::Blob(bytes)) if !bytes.is_empty() => bytes.as_ptr() as *const c_void,
        Some(Value::Text(bytes)) => bytes.as_ptr() as *const c_void,
        _ => ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_bytes(s: *mut c_void, col: c_int) -> c_int {
    match stmt(s).and_then(|stmt| stmt.cell(col)) {
        Some(Value::Blob(bytes)) => bytes.len() as c_int,
        Some(Value::Text(bytes)) => bytes.len() as c_int - 1,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_changes64(db: *mut c_void) -> i64 {
    conn(db).lookup("SELECT changes()")
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_total_changes64(db: *mut c_void) -> i64 {
    conn(db).lookup("SELECT total_changes()")
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_last_insert_rowid(db: *mut c_void) -> i64 {
    conn(db).lookup("SELECT last_insert_rowid()")
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_get_autocommit(_db: *mut c_void) -> c_int {
    (DEPTH.with(StdCell::get) == 0) as c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_errcode(db: *mut c_void) -> c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    conn(db).code
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_errmsg(db: *mut c_void) -> *const c_char {
    if db.is_null() || conn(db).message.is_empty() {
        return c"not an error".as_ptr();
    }
    conn(db).message.as_ptr() as *const c_char
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_errstr(code: c_int) -> *const c_char {
    match code & 0xff {
        SQLITE_OK => c"not an error",
        SQLITE_CONSTRAINT => c"constraint failed",
        SQLITE_MISUSE => c"bad parameter or other API misuse",
        SQLITE_TOOBIG => c"string or blob too big",
        SQLITE_RANGE => c"column index out of range",
        SQLITE_CANTOPEN => c"unable to open database file",
        SQLITE_INTERRUPT => c"interrupted",
        SQLITE_ROW => c"another row available",
        SQLITE_DONE => c"no more rows available",
        _ => c"SQL logic error",
    }
    .as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_error_offset(_db: *mut c_void) -> c_int {
    -1
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_extended_result_codes(_db: *mut c_void, _on: c_int) -> c_int {
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_busy_timeout(_db: *mut c_void, _ms: c_int) -> c_int {
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_limit(_db: *mut c_void, id: c_int, _new: c_int) -> c_int {
    limit(id)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_libversion_number() -> c_int {
    3_050_004
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_complete(sql: *const c_char) -> c_int {
    complete(&String::from_utf8_lossy(CStr::from_ptr(sql).to_bytes())) as c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_status(_s: *mut c_void, _op: c_int, _reset: c_int) -> c_int {
    0
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_commit_hook(
    db: *mut c_void,
    hook: Option<CommitHook>,
    arg: *mut c_void,
) -> *mut c_void {
    let conn = conn(db);
    let previous = conn.commit_hook.map_or(ptr::null_mut(), |(_, arg)| arg);
    conn.commit_hook = hook.map(|hook| (hook, arg));
    previous
}

/// Never called: a rollback is refused before it happens.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_rollback_hook(
    _db: *mut c_void,
    _hook: Option<unsafe extern "C" fn(*mut c_void)>,
    _arg: *mut c_void,
) -> *mut c_void {
    ptr::null_mut()
}

/// Only rusqlite's own bookkeeping installs these: jazz-db sets none.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_update_hook(
    _db: *mut c_void,
    hook: Option<unsafe extern "C" fn(*mut c_void, c_int, *const c_char, *const c_char, i64)>,
    _arg: *mut c_void,
) -> *mut c_void {
    assert!(
        hook.is_none(),
        "update hooks are not supported on Durable Object SQLite yet"
    );
    ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_progress_handler(
    _db: *mut c_void,
    _n: c_int,
    hook: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    _arg: *mut c_void,
) {
    assert!(
        hook.is_none(),
        "progress handlers are not supported on Durable Object SQLite yet"
    );
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_set_authorizer(
    _db: *mut c_void,
    hook: Option<
        unsafe extern "C" fn(
            *mut c_void,
            c_int,
            *const c_char,
            *const c_char,
            *const c_char,
            *const c_char,
        ) -> c_int,
    >,
    _arg: *mut c_void,
) -> c_int {
    assert!(
        hook.is_none(),
        "authorizers are not supported on Durable Object SQLite yet"
    );
    SQLITE_OK
}
