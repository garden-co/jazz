//! Owned connection boundary: native execution stays concrete and inline;
//! worker drivers own their rows and asynchronous command transport.
use crate::options::{Error, ErrorCode};
use crate::values::{TransactionConnection, TransactionJournal};
use futures::future::LocalBoxFuture;
#[cfg(feature = "native")]
use jazz_sql::Connection as _;
use jazz_sql::{Cell, Param, QueryStats, Row, Sql};
use std::cell::{Cell as Flag, RefCell};
use std::ops::ControlFlow;
use std::sync::Arc;

/// A worker result owns its bytes until the synchronous row callback returns.
/// This is an in-memory representation, never a durable or wire encoding.
#[derive(Debug)]
pub enum WorkerCell {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}
#[derive(Debug)]
pub struct WorkerRows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<WorkerCell>>,
}

/// SQL execution for one Jazz owner. A provider may own the connection or lease
/// transactions on a shared database. Admitted commands must finish in order,
/// even when their waiter is dropped; state is published before each reply.
/// Closing drains/releases Jazz's storage resources. Shared providers leave
/// final database closure to their host, which keeps the runtime alive.
pub trait WorkerSql {
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<WorkerRows, Error>>;
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<usize, Error>>;
    fn execute_script<'a>(&'a self, sql: &'a str) -> LocalBoxFuture<'a, Result<(), Error>>;
    /// Provider transaction boundaries. Defaults use SQLite savepoints; shared
    /// providers can acquire their own transaction lease at the outer boundary.
    fn begin_savepoint<'a>(&'a self, name: &'a str) -> LocalBoxFuture<'a, Result<(), Error>> {
        Box::pin(async move { self.execute_script(&format!("SAVEPOINT {name}")).await })
    }
    fn end_savepoint<'a>(
        &'a self,
        name: &'a str,
        keep: bool,
    ) -> LocalBoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let sql = if keep {
                format!("RELEASE {name}")
            } else {
                format!("ROLLBACK TO {name}; RELEASE {name}")
            };
            self.execute_script(&sql).await
        })
    }
    fn transaction_open(&self) -> bool;
    fn parameter_limit(&self) -> Result<usize, Error>;
    fn request_close(&self);
    fn close(&self) -> LocalBoxFuture<'_, Result<(), Error>>;
}

enum Backend {
    #[cfg(feature = "native")]
    Native(crate::host::OwnedConnection),
    Worker(Box<dyn WorkerSql>),
}

pub(crate) struct DbConnection {
    backend: Backend,
    pub(crate) journal: TransactionJournal,
    chunks: RefCell<Option<Arc<crate::large::Chunks>>>,
    cleanup: RefCell<Vec<String>>,
    poisoned: Flag<bool>,
}
impl DbConnection {
    /// Configure Jazz-owned native SQLite; a worker's host owns its connection settings.
    pub(crate) fn stable_plans(&self) -> Result<(), Error> {
        #[cfg(feature = "native")]
        if let Backend::Native(native) = &self.backend {
            crate::host::stable_plans(native)?;
        }
        Ok(())
    }

    #[cfg(feature = "native")]
    pub(crate) fn new(native: crate::host::OwnedConnection) -> Self {
        Self::from_backend(Backend::Native(native))
    }
    pub(crate) fn worker(worker: impl WorkerSql + 'static) -> Self {
        Self::from_backend(Backend::Worker(Box::new(worker)))
    }
    fn from_backend(backend: Backend) -> Self {
        Self {
            backend,
            journal: Default::default(),
            chunks: Default::default(),
            cleanup: Default::default(),
            poisoned: Flag::new(false),
        }
    }
    pub(crate) fn sql(&self) -> ConnectionSql<'_> {
        ConnectionSql {
            owner: Some(self),
            #[cfg(feature = "native")]
            reader: None,
        }
    }
    pub(crate) fn single_connection(&self) -> bool {
        match &self.backend {
            #[cfg(feature = "native")]
            Backend::Native(_) => false,
            Backend::Worker(_) => true,
        }
    }
    #[cfg(feature = "native")]
    pub(crate) fn native(&self) -> Result<&rusqlite::Connection, Error> {
        match &self.backend {
            Backend::Native(native) => Ok(native),
            Backend::Worker(_) => Err(Error::new(
                ErrorCode::Storage,
                "Synchronous native SQL is not supported by a worker connection",
            )),
        }
    }
    pub(crate) fn set_statement_cache_capacity(&self, capacity: usize) {
        #[cfg(feature = "native")]
        if let Backend::Native(native) = &self.backend {
            native.set_prepared_statement_cache_capacity(capacity);
        }
        let _ = capacity;
    }
    pub(crate) fn observe_transactions(
        &self,
        chunks: &Arc<crate::large::Chunks>,
    ) -> Result<(), Error> {
        #[cfg(feature = "native")]
        if let Backend::Native(native) = &self.backend {
            let committed = chunks.clone();
            native.commit_hook(Some(move || {
                committed.unpin();
                false
            }))?;
            let rolled_back = chunks.clone();
            let rollbacks = self.journal.rollback_counter();
            native.rollback_hook(Some(move || {
                rolled_back.unpin();
                rollbacks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }))?;
        }
        *self.chunks.borrow_mut() = Some(chunks.clone());
        Ok(())
    }
    fn observed(&self, was_open: bool, failed: bool) {
        match &self.backend {
            #[cfg(feature = "native")]
            Backend::Native(_) => {}
            Backend::Worker(worker) => {
                if was_open && !worker.transaction_open() {
                    if let Some(chunks) = self.chunks.borrow().as_ref() {
                        chunks.unpin();
                    }
                    if failed {
                        self.journal.note_rollback();
                    }
                }
            }
        }
    }
    async fn raw_script(&self, sql: &str) -> Result<(), Error> {
        let before = self.transaction_open();
        let result = match &self.backend {
            #[cfg(feature = "native")]
            Backend::Native(native) => native.sql().execute_script(sql).await.map_err(Error::from),
            Backend::Worker(worker) => worker.execute_script(sql).await,
        };
        self.observed(before, result.is_err());
        result
    }
    async fn end_savepoint_raw(&self, name: &str, keep: bool) -> Result<(), Error> {
        let before = self.transaction_open();
        let result = match &self.backend {
            #[cfg(feature = "native")]
            Backend::Native(native) => {
                let sql = if keep {
                    format!("RELEASE {name}")
                } else {
                    format!("ROLLBACK TO {name}; RELEASE {name}")
                };
                native.sql().execute_script(&sql).await.map_err(Error::from)
            }
            Backend::Worker(worker) => worker.end_savepoint(name, keep).await,
        };
        self.observed(before, result.is_err());
        result
    }
    pub(crate) async fn settle(&self) -> Result<(), Error> {
        if self.poisoned.get() {
            return Err(Error::new(
                ErrorCode::Storage,
                "connection cleanup failed; reopen the database",
            ));
        }
        loop {
            let name = self.cleanup.borrow().last().cloned();
            let Some(name) = name else {
                return Ok(());
            };
            // Keep the entry until the worker has replied. Cancellation cannot
            // let another operation skip a cleanup command already in flight.
            let result = self.end_savepoint_raw(&name, false).await;
            if let Err(error) = crate::values::finish_savepoint(self, false, result) {
                self.poisoned.set(true);
                return Err(error);
            }
            self.cleanup.borrow_mut().pop();
        }
    }
    pub(crate) async fn close_worker(&self) -> Result<(), Error> {
        match &self.backend {
            #[cfg(feature = "native")]
            Backend::Native(_) => {}
            Backend::Worker(worker) => {
                worker.close().await?;
            }
        }
        Ok(())
    }
}
impl Drop for DbConnection {
    fn drop(&mut self) {
        match &self.backend {
            #[cfg(feature = "native")]
            Backend::Native(_) => {}
            Backend::Worker(worker) => {
                worker.request_close();
            }
        }
    }
}
#[cfg(feature = "native")]
impl From<crate::host::OwnedConnection> for DbConnection {
    fn from(native: crate::host::OwnedConnection) -> Self {
        Self::new(native)
    }
}
#[cfg(feature = "native")]
impl From<rusqlite::Connection> for DbConnection {
    fn from(native: rusqlite::Connection) -> Self {
        Self::new(native.into())
    }
}
#[cfg(feature = "native")]
impl std::ops::Deref for DbConnection {
    type Target = rusqlite::Connection;
    fn deref(&self) -> &Self::Target {
        self.native().expect("native-only connection access")
    }
}
impl TransactionConnection for DbConnection {
    fn transaction_open(&self) -> bool {
        match &self.backend {
            #[cfg(feature = "native")]
            Backend::Native(native) => native.sql().transaction_open(),
            Backend::Worker(worker) => worker.transaction_open(),
        }
    }
    async fn transaction_sql(&self, sql: &str) -> Result<(), Error> {
        self.settle().await?;
        self.raw_script(sql).await
    }
    async fn begin_savepoint(&self, name: &str) -> Result<(), Error> {
        self.settle().await?;
        match &self.backend {
            #[cfg(feature = "native")]
            Backend::Native(native) => native
                .sql()
                .execute_script(&format!("SAVEPOINT {name}"))
                .await
                .map_err(Error::from),
            Backend::Worker(worker) => worker.begin_savepoint(name).await,
        }
    }
    async fn end_savepoint(&self, name: &str, keep: bool) -> Result<(), Error> {
        self.settle().await?;
        self.end_savepoint_raw(name, keep).await
    }
    fn journal(&self) -> Option<&TransactionJournal> {
        Some(&self.journal)
    }
    fn abandon_savepoint(&self, name: &str) {
        match &self.backend {
            #[cfg(feature = "native")]
            Backend::Native(_) => {
                let _ = crate::values::end_savepoint_inline(self, name, false);
            }
            Backend::Worker(_) => self.cleanup.borrow_mut().push(name.into()),
        }
    }
}
#[cfg(feature = "native")]
impl crate::values::NativeTransactionConnection for DbConnection {
    fn sqlite(&self) -> &rusqlite::Connection {
        self.native().expect("native-only transaction access")
    }
}

pub enum ConnectionRow<'a> {
    #[cfg(feature = "native")]
    Native(jazz_sql::native::NativeRow<'a>),
    Worker(&'a [WorkerCell], &'a [String]),
}
impl Row for ConnectionRow<'_> {
    fn len(&self) -> usize {
        match self {
            #[cfg(feature = "native")]
            Self::Native(row) => row.len(),
            Self::Worker(row, _) => row.len(),
        }
    }
    fn column_name(&self, index: usize) -> Option<&str> {
        match self {
            #[cfg(feature = "native")]
            Self::Native(row) => row.column_name(index),
            Self::Worker(_, names) => names.get(index).map(String::as_str),
        }
    }
    fn cell(&self, index: usize) -> Option<Cell<'_>> {
        match self {
            #[cfg(feature = "native")]
            Self::Native(row) => row.cell(index),
            Self::Worker(row, _) => Some(match row.get(index)? {
                WorkerCell::Null => Cell::Null,
                WorkerCell::Integer(v) => Cell::Integer(*v),
                WorkerCell::Real(v) => Cell::Real(*v),
                WorkerCell::Text(v) => Cell::Text(v),
                WorkerCell::Blob(v) => Cell::Blob(v),
            }),
        }
    }
}
pub struct ConnectionSql<'a> {
    owner: Option<&'a DbConnection>,
    #[cfg(feature = "native")]
    reader: Option<&'a rusqlite::Connection>,
}
impl<'a> ConnectionSql<'a> {
    #[cfg(feature = "native")]
    pub(crate) fn reader(reader: &'a rusqlite::Connection) -> Self {
        Self {
            owner: None,
            reader: Some(reader),
        }
    }
    fn owner(&self) -> &'a DbConnection {
        self.owner.expect("owned SQL connection")
    }
}
impl Sql for ConnectionSql<'_> {
    type Error = Error;
    type Row<'a> = ConnectionRow<'a>;
    fn invalid_data(message: &'static str) -> Error {
        Error::new(ErrorCode::Storage, message)
    }
    async fn execute_script(&mut self, sql: &str) -> Result<(), Error> {
        #[cfg(feature = "native")]
        if let Some(reader) = self.reader {
            return jazz_sql::native::Native(reader)
                .execute_script(sql)
                .await
                .map_err(Error::from);
        }
        self.owner().settle().await?;
        self.owner().raw_script(sql).await
    }
    async fn execute(&mut self, sql: &str, params: &[Param<'_>]) -> Result<usize, Error> {
        #[cfg(feature = "native")]
        if let Some(reader) = self.reader {
            return jazz_sql::native::Native(reader)
                .execute(sql, params)
                .await
                .map_err(Error::from);
        }
        self.owner().settle().await?;
        let before = self.owner().transaction_open();
        let result = match &self.owner().backend {
            #[cfg(feature = "native")]
            Backend::Native(native) => native.sql().execute(sql, params).await.map_err(Error::from),
            Backend::Worker(worker) => worker.execute(sql, params).await,
        };
        self.owner().observed(before, result.is_err());
        result
    }
    async fn query<F>(&mut self, sql: &str, params: &[Param<'_>], visit: F) -> Result<(), Error>
    where
        F: FnMut(Self::Row<'_>) -> Result<ControlFlow<()>, Error>,
    {
        self.query_with_stats(sql, params, visit).await.map(|_| ())
    }
    async fn query_with_stats<F>(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
        mut visit: F,
    ) -> Result<QueryStats, Error>
    where
        F: FnMut(Self::Row<'_>) -> Result<ControlFlow<()>, Error>,
    {
        #[cfg(feature = "native")]
        if let Some(reader) = self.reader {
            let mut callback_error = None;
            let result = jazz_sql::native::Native(reader)
                .query_with_stats(sql, params, |row| match visit(ConnectionRow::Native(row)) {
                    Ok(flow) => Ok(flow),
                    Err(error) => {
                        callback_error = Some(error);
                        Ok(ControlFlow::Break(()))
                    }
                })
                .await
                .map_err(Error::from);
            return match callback_error {
                Some(error) => Err(error),
                None => result,
            };
        }
        self.owner().settle().await?;
        let before = self.owner().transaction_open();
        let result = match &self.owner().backend {
            #[cfg(feature = "native")]
            Backend::Native(native) => {
                let mut callback_error = None;
                let result = native
                    .sql()
                    .query_with_stats(sql, params, |row| match visit(ConnectionRow::Native(row)) {
                        Ok(flow) => Ok(flow),
                        Err(error) => {
                            callback_error = Some(error);
                            Ok(ControlFlow::Break(()))
                        }
                    })
                    .await
                    .map_err(Error::from);
                match callback_error {
                    Some(error) => Err(error),
                    None => result,
                }
            }
            Backend::Worker(worker) => match worker.query(sql, params).await {
                Err(error) => Err(error),
                Ok(rows) => (|| {
                    for row in &rows.rows {
                        if visit(ConnectionRow::Worker(row, &rows.columns))?.is_break() {
                            break;
                        }
                    }
                    Ok(QueryStats::default())
                })(),
            },
        };
        self.owner().observed(before, result.is_err());
        result
    }
    async fn optional_blob(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.query_optional(sql, params, |row| row.get(0)).await
    }
}
impl jazz_sql::Connection for ConnectionSql<'_> {
    fn transaction_open(&self) -> bool {
        #[cfg(feature = "native")]
        if let Some(reader) = self.reader {
            return jazz_sql::native::Native(reader).transaction_open();
        }
        self.owner().transaction_open()
    }
    fn parameter_limit(&self) -> Result<usize, Error> {
        #[cfg(feature = "native")]
        if let Some(reader) = self.reader {
            return jazz_sql::native::Native(reader)
                .parameter_limit()
                .map_err(Error::from);
        }
        match &self.owner().backend {
            #[cfg(feature = "native")]
            Backend::Native(native) => native.sql().parameter_limit().map_err(Error::from),
            Backend::Worker(worker) => worker.parameter_limit(),
        }
    }
    async fn total_changes(&mut self) -> Result<u64, Error> {
        #[cfg(feature = "native")]
        if let Some(reader) = self.reader {
            return jazz_sql::native::Native(reader)
                .total_changes()
                .await
                .map_err(Error::from);
        }
        self.owner().settle().await?;
        match &self.owner().backend {
            #[cfg(feature = "native")]
            Backend::Native(native) => native.sql().total_changes().await.map_err(Error::from),
            Backend::Worker(_) => {
                self.query_one("SELECT total_changes()", &[], |row| row.get(0))
                    .await
            }
        }
    }
}
