//! Statement trace for checking jazz-db's SQL against restricted SQLite
//! hosts (Cloudflare Durable Objects). With `JAZZ_SQL_TRACE=<file>` set,
//! every top-level statement appends one JSON line: the connection, the
//! statement text, its bound-parameter count, and what its bound values
//! need from the host (largest blob or text, integers beyond 2^53). With
//! `JAZZ_SQL_TRACE_EXPANDED=1` it also logs the text with parameters
//! expanded, for replaying writes.

use std::ffi::{CStr, c_char, c_int, c_uint, c_void};
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::{Connection, ffi};

static SINK: Mutex<Option<std::fs::File>> = Mutex::new(None);
static CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn install(conn: &Connection) {
    let Some(path) = std::env::var_os("JAZZ_SQL_TRACE") else {
        return;
    };
    let mut sink = SINK.lock().unwrap();
    if sink.is_none() {
        *sink = Some(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .expect("open JAZZ_SQL_TRACE"),
        );
    }
    // Connection ids are unique across test binaries sharing one file.
    let id = format!(
        "{}:{}",
        std::process::id(),
        CONNECTIONS.fetch_add(1, Ordering::Relaxed)
    );
    let context = Box::into_raw(Box::new(id)) as *mut c_void;
    // SAFETY: the callback only reads the statement it is handed; the
    // context string is leaked for the connection's (and process's) life.
    unsafe {
        ffi::sqlite3_trace_v2(
            conn.handle(),
            ffi::SQLITE_TRACE_STMT as c_uint,
            Some(record),
            context,
        );
    }
}

unsafe extern "C" fn record(
    _event: c_uint,
    context: *mut c_void,
    statement: *mut c_void,
    text: *mut c_void,
) -> c_int {
    // SAFETY: SQLITE_TRACE_STMT hands the statement and its text.
    let (id, sql) = unsafe {
        (
            &*(context as *const String),
            CStr::from_ptr(text as *const c_char).to_string_lossy(),
        )
    };
    // Trigger bodies report as `-- TRIGGER name`; their SQL is in the DDL.
    if sql.starts_with("--") {
        return 0;
    }
    let statement = statement as *mut ffi::sqlite3_stmt;
    // SAFETY: the statement is live for the duration of the callback.
    let (params, expanded) = unsafe {
        let params = ffi::sqlite3_bind_parameter_count(statement);
        let raw = ffi::sqlite3_expanded_sql(statement);
        let expanded = if raw.is_null() {
            None
        } else {
            let owned = CStr::from_ptr(raw).to_string_lossy().into_owned();
            ffi::sqlite3_free(raw as *mut c_void);
            Some(owned)
        };
        (params, expanded)
    };
    let (max_value, big_int) = expanded.as_deref().map(scan).unwrap_or((0, false));
    let mut line = serde_json::json!({
        "conn": id,
        "sql": sql,
        "params": params,
        "max_value": max_value,
        "big_int": big_int,
    });
    if std::env::var_os("JAZZ_SQL_TRACE_EXPANDED").is_some() {
        line["expanded"] = expanded.into();
    }
    let mut line = line.to_string().into_bytes();
    line.push(b'\n');
    if let Some(file) = SINK.lock().unwrap().as_mut() {
        let _ = file.write_all(&line);
    }
    0
}

/// The largest literal (blob bytes or text bytes) in expanded SQL, and
/// whether an integer literal is beyond 2^53 (lossy as a JS number).
fn scan(sql: &str) -> (usize, bool) {
    let bytes = sql.as_bytes();
    let (mut max, mut big) = (0, false);
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                let blob = i > 0 && (bytes[i - 1] == b'x' || bytes[i - 1] == b'X');
                let start = i + 1;
                i = start;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                let len = i - start;
                max = max.max(if blob { len / 2 } else { len });
                i += 1;
            }
            b'0'..=b'9'
                if i == 0
                    || !(bytes[i - 1].is_ascii_alphanumeric()
                        || bytes[i - 1] == b'_'
                        || bytes[i - 1] == b'.') =>
            {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                let float =
                    i < bytes.len() && (bytes[i] == b'.' || bytes[i] == b'e' || bytes[i] == b'E');
                if !float && i - start >= 16 {
                    if let Ok(value) = sql[start..i].parse::<u64>() {
                        big |= value > (1u64 << 53);
                    } else {
                        big = true;
                    }
                }
            }
            _ => i += 1,
        }
    }
    (max, big)
}
