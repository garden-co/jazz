//! Read-only connections beside the writer, for reads too heavy to run on
//! it (a prototype: one-shot remote reads only).
//!
//! A WAL file serves readers beside its writer: a statement on a reader
//! sees every commit that returned before it began, so a read dispatched
//! after a session's earlier commits applied never misses them (#4289).
//! The writer compiles the read (sessions, claims and policies live
//! there) and finishes it once its rows come back.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OpenFlags};

use crate::compile::Compiled;
use crate::db::{Error, ErrorCode};
use crate::large::Chunks;
use crate::values::{CurrentRow, begin_savepoint, end_savepoint, storage};

/// VM instructions between two progress-handler calls.
const PROGRESS_PERIOD: i32 = 1_000;

/// No deadline.
const UNBOUNDED: u64 = u64::MAX;

/// A statement's time budget, checked by the connection's progress
/// handler every [`PROGRESS_PERIOD`] instructions: once it is past, the
/// statement is interrupted. Time, not instructions: one instruction can
/// be a function call doing real work (`instr` over a long text).
#[derive(Clone)]
pub(crate) struct Budget {
    /// Nanoseconds after `origin` the statement must end by.
    deadline: Arc<AtomicU64>,
    origin: Instant,
}

impl Budget {
    /// A budget with no deadline, enforced on `conn`.
    pub(crate) fn install(conn: &Connection) -> Result<Self, Error> {
        let budget = Self {
            deadline: Arc::new(AtomicU64::new(UNBOUNDED)),
            origin: Instant::now(),
        };
        let (deadline, origin) = (Arc::clone(&budget.deadline), budget.origin);
        conn.progress_handler(
            PROGRESS_PERIOD,
            Some(move || {
                let deadline = deadline.load(Ordering::Relaxed);
                deadline != UNBOUNDED && origin.elapsed().as_nanos() as u64 >= deadline
            }),
        )
        .map_err(storage)?;
        Ok(budget)
    }

    /// Run `f` for at most `slice`; `Ok(None)` if it ran out first.
    pub(crate) fn within<T>(
        &self,
        slice: Duration,
        f: impl FnOnce() -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        let deadline = (self.origin.elapsed() + slice).as_nanos() as u64;
        self.deadline.store(deadline, Ordering::Relaxed);
        let result = f();
        self.deadline.store(UNBOUNDED, Ordering::Relaxed);
        let exhausted = self.origin.elapsed().as_nanos() as u64 >= deadline;
        match result {
            Err(_) if exhausted => Ok(None),
            result => result.map(Some),
        }
    }
}

const SCRATCH: &str = "CREATE TEMP TABLE _jazz_lv_scratch (slot INTEGER NOT NULL, \
 cell BLOB NOT NULL, v BLOB, PRIMARY KEY (slot, cell)) WITHOUT ROWID";

/// Run a compiled read on a reader: its preps first, into the reader's
/// scratch table, undone afterwards.
fn run(
    conn: &Connection,
    chunks: &Chunks,
    compiled: &Compiled,
    table: &str,
) -> Result<Vec<CurrentRow>, Error> {
    if compiled.preps.is_empty() {
        return crate::db::execute_rows(conn, chunks, &compiled.sql, compiled, table);
    }
    begin_savepoint(conn, "jazz_scratch")?;
    let result = (|| {
        let mut ties = false;
        for prep in &compiled.preps {
            ties |= crate::large::answer(conn, chunks, prep)?;
        }
        let sql = match &compiled.ranked_sql {
            Some(ranked) if ties => ranked,
            _ => &compiled.sql,
        };
        crate::db::execute_rows(conn, chunks, sql, compiled, table)
    })();
    end_savepoint(conn, "jazz_scratch", false)?;
    result
}

struct Job {
    compiled: Compiled,
    table: String,
    reply: mpsc::Sender<Result<Vec<CurrentRow>, Error>>,
}

/// Reader threads over one store file.
pub(crate) struct ReaderPool {
    jobs: Mutex<Option<mpsc::Sender<Job>>>,
    threads: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl ReaderPool {
    pub(crate) fn open(path: &Path, readers: usize, chunks: Arc<Chunks>) -> Result<Self, Error> {
        let (jobs, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut threads = Vec::new();
        for n in 0..readers.max(1) {
            let conn = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(storage)?;
            conn.busy_timeout(Duration::from_secs(5)).map_err(storage)?;
            // Preps answer into a scratch table a reader cannot write in
            // the file: its own TEMP one shadows that name.
            conn.execute_batch(SCRATCH).map_err(storage)?;
            let (receiver, chunks) = (Arc::clone(&receiver), Arc::clone(&chunks));
            let thread = thread::Builder::new()
                .name(format!("jazz-db-reader-{n}"))
                .spawn(move || {
                    loop {
                        let job = match receiver.lock() {
                            Ok(receiver) => receiver.recv(),
                            Err(_) => return,
                        };
                        let Ok(job) = job else { return };
                        let rows = run(&conn, &chunks, &job.compiled, &job.table);
                        let _ = job.reply.send(rows);
                    }
                })
                .map_err(|error| Error::new(ErrorCode::Storage, error.to_string()))?;
            threads.push(thread);
        }
        Ok(Self {
            jobs: Mutex::new(Some(jobs)),
            threads: Mutex::new(threads),
        })
    }

    /// Run `compiled` (no preps) on a reader; its rows arrive on the
    /// returned receiver.
    pub(crate) fn run(
        &self,
        compiled: Compiled,
        table: String,
    ) -> Result<mpsc::Receiver<Result<Vec<CurrentRow>, Error>>, Error> {
        let (reply, rows) = mpsc::channel();
        let closed = || Error::new(ErrorCode::Storage, "reader pool is closed");
        self.jobs
            .lock()
            .map_err(|_| closed())?
            .as_ref()
            .ok_or_else(closed)?
            .send(Job {
                compiled,
                table,
                reply,
            })
            .map_err(|_| closed())?;
        Ok(rows)
    }
}

impl Drop for ReaderPool {
    fn drop(&mut self) {
        if let Ok(mut jobs) = self.jobs.lock() {
            jobs.take();
        }
        if let Ok(mut threads) = self.threads.lock() {
            for thread in threads.drain(..) {
                let _ = thread.join();
            }
        }
    }
}
