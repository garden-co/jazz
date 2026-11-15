//! Tests: a [`WorkerSql`] over an in-memory SQLite connection whose replies
//! can be held on demand, to model an asynchronous storage worker.
//!
//! Every command is admitted and executed when it is issued (as a real worker
//! runs a command it has accepted even if its waiter is dropped); only the
//! reply can be held. [`SuspendingWorker::hold`] arms a one-shot predicate:
//! the next command whose SQL matches it executes, then its future stays
//! Pending until [`SuspendingWorker::release`].
use crate::backend::{WorkerCell, WorkerRows, WorkerSql};
use crate::options::{Error, ErrorCode};
use futures::future::LocalBoxFuture;
use jazz_sql::Param;
use std::cell::RefCell;
use std::rc::Rc;
use std::task::{Poll, Waker};

#[derive(Default)]
struct Gate {
    released: bool,
    waker: Option<Waker>,
}

struct Shared {
    conn: rusqlite::Connection,
    log: RefCell<Vec<String>>,
    hold: RefCell<Option<Box<dyn Fn(&str) -> bool>>>,
    gate: RefCell<Option<Rc<RefCell<Gate>>>>,
}

/// A cloneable handle: one clone goes into the store, the test keeps another.
#[derive(Clone)]
pub struct SuspendingWorker(Rc<Shared>);

impl SuspendingWorker {
    pub fn memory() -> Self {
        Self(Rc::new(Shared {
            conn: rusqlite::Connection::open_in_memory().expect("in-memory SQLite"),
            log: Default::default(),
            hold: Default::default(),
            gate: Default::default(),
        }))
    }

    /// Hold the reply of the next command whose SQL satisfies `matches`.
    pub fn hold(&self, matches: impl Fn(&str) -> bool + 'static) {
        *self.0.hold.borrow_mut() = Some(Box::new(matches));
    }

    /// Hold the reply of the next command, whatever it is.
    pub fn hold_next(&self) {
        self.hold(|_| true);
    }

    /// Whether a reply is currently held.
    pub fn holding(&self) -> bool {
        self.0
            .gate
            .borrow()
            .as_ref()
            .is_some_and(|gate| !gate.borrow().released)
    }

    /// Let the held reply (if any) complete.
    pub fn release(&self) {
        if let Some(gate) = self.0.gate.borrow_mut().take() {
            let mut gate = gate.borrow_mut();
            gate.released = true;
            if let Some(waker) = gate.waker.take() {
                waker.wake();
            }
        }
    }

    /// Every command the worker executed, in order.
    pub fn log(&self) -> Vec<String> {
        self.0.log.borrow().clone()
    }

    /// Whether the worker's connection is inside a transaction.
    pub fn in_transaction(&self) -> bool {
        !self.0.conn.is_autocommit()
    }

    /// Direct access to the worker's database, for assertions.
    pub fn with_conn<T>(&self, f: impl FnOnce(&rusqlite::Connection) -> T) -> T {
        f(&self.0.conn)
    }

    /// Record `sql` and return the gate its reply must wait on, if held.
    fn admit(&self, sql: &str) -> Option<Rc<RefCell<Gate>>> {
        self.0.log.borrow_mut().push(sql.to_owned());
        let held = self.0.hold.borrow().as_ref().is_some_and(|m| m(sql));
        if !held {
            return None;
        }
        self.0.hold.borrow_mut().take();
        let gate = Rc::new(RefCell::new(Gate::default()));
        *self.0.gate.borrow_mut() = Some(gate.clone());
        Some(gate)
    }

    fn reply<'a, T: 'a>(
        gate: Option<Rc<RefCell<Gate>>>,
        result: Result<T, Error>,
    ) -> LocalBoxFuture<'a, Result<T, Error>> {
        let mut result = Some(result);
        Box::pin(std::future::poll_fn(move |cx| {
            if let Some(gate) = &gate {
                let mut gate = gate.borrow_mut();
                if !gate.released {
                    gate.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }
            Poll::Ready(result.take().expect("polled after completion"))
        }))
    }
}

fn storage(error: rusqlite::Error) -> Error {
    Error::new(ErrorCode::Storage, error.to_string())
}

struct Bound<'a>(&'a Param<'a>);
impl rusqlite::ToSql for Bound<'_> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, ValueRef};
        Ok(ToSqlOutput::Borrowed(match self.0 {
            Param::Null => ValueRef::Null,
            Param::Integer(value) => ValueRef::Integer(*value),
            Param::Real(value) => ValueRef::Real(*value),
            Param::Blob(value) => ValueRef::Blob(value),
            Param::Text(value) => ValueRef::Text(value.as_bytes()),
            Param::TextBytes(value) => ValueRef::Text(value),
        }))
    }
}

impl SuspendingWorker {
    fn run_query(&self, sql: &str, params: &[Param<'_>]) -> Result<WorkerRows, Error> {
        let mut stmt = self.0.conn.prepare_cached(sql).map_err(storage)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let bound: Vec<Bound<'_>> = params.iter().map(Bound).collect();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(bound.iter()))
            .map_err(storage)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(storage)? {
            let mut cells = Vec::with_capacity(columns.len());
            for index in 0..columns.len() {
                use rusqlite::types::ValueRef;
                cells.push(match row.get_ref(index).map_err(storage)? {
                    ValueRef::Null => WorkerCell::Null,
                    ValueRef::Integer(v) => WorkerCell::Integer(v),
                    ValueRef::Real(v) => WorkerCell::Real(v),
                    ValueRef::Text(v) => WorkerCell::Text(v.to_vec()),
                    ValueRef::Blob(v) => WorkerCell::Blob(v.to_vec()),
                });
            }
            out.push(cells);
        }
        Ok(WorkerRows { columns, rows: out })
    }
}

impl WorkerSql for SuspendingWorker {
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<WorkerRows, Error>> {
        let gate = self.admit(sql);
        Self::reply(gate, self.run_query(sql, params))
    }
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<usize, Error>> {
        let gate = self.admit(sql);
        let bound: Vec<Bound<'_>> = params.iter().map(Bound).collect();
        let result = self
            .0
            .conn
            .prepare_cached(sql)
            .and_then(|mut stmt| stmt.execute(rusqlite::params_from_iter(bound.iter())))
            .map_err(storage);
        Self::reply(gate, result)
    }
    fn execute_script<'a>(&'a self, sql: &'a str) -> LocalBoxFuture<'a, Result<(), Error>> {
        let gate = self.admit(sql);
        Self::reply(gate, self.0.conn.execute_batch(sql).map_err(storage))
    }
    fn transaction_open(&self) -> bool {
        self.in_transaction()
    }
    fn parameter_limit(&self) -> Result<usize, Error> {
        self.0
            .conn
            .limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER)
            .map(|limit| limit as usize)
            .map_err(storage)
    }
    fn request_close(&self) {}
    fn close(&self) -> LocalBoxFuture<'_, Result<(), Error>> {
        Box::pin(std::future::ready(Ok(())))
    }
}
