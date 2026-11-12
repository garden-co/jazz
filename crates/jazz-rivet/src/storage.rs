//! Full Jazz Db adapter for Rivet's public async SQLite worker.
use crate::sql::{bind, parameter_limit};
use futures::{FutureExt, future::LocalBoxFuture};
use jazz_db::{
    backend::{WorkerCell, WorkerRows, WorkerSql},
    db::{Db, DbConfig, Error, ErrorCode},
};
use jazz_sql::Param;
use rivetkit::{ColumnValue, SqliteDb, SqliteTransaction};
use std::cell::{Cell, RefCell};

fn storage(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Storage, error.to_string())
}

pub struct RivetConnection {
    db: SqliteDb,
    transaction: RefCell<Option<SqliteTransaction>>,
    savepoints: RefCell<Vec<String>>,
    poisoned: Cell<bool>,
    failure: std::sync::Arc<dyn Fn(String) + Send + Sync>,
    parameters: usize,
    runtime: tokio::runtime::Handle,
}
impl RivetConnection {
    async fn in_runtime<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        futures::future::poll_fn(|cx| {
            let _runtime = self.runtime.enter();
            future.as_mut().poll(cx)
        })
        .await
    }
    fn check(&self) -> Result<(), Error> {
        if self.poisoned.get() {
            Err(storage("Rivet transaction failed; restart the actor"))
        } else {
            Ok(())
        }
    }
    // The public SDK does not expose SQLite autocommit state. On SQL errors,
    // fail closed and explicitly roll back the entire lease. Never guess state
    // from SQL text or reuse an ambiguously committed connection.
    async fn checked<T>(&self, result: anyhow::Result<T>) -> Result<T, Error> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                let first_failure = !self.poisoned.replace(true);
                let tx = self.transaction.borrow().clone();
                if let Some(tx) = tx
                    && self.in_runtime(tx.rollback()).await.is_ok()
                {
                    self.transaction.borrow_mut().take();
                    self.savepoints.borrow_mut().clear();
                }
                if first_failure {
                    (self.failure)(format!("{error:#}"));
                }
                Err(storage(format!("{error:#}")))
            }
        }
    }
}
impl WorkerSql for RivetConnection {
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<WorkerRows, Error>> {
        async move {
            self.check()?;
            let tx = self.transaction.borrow().clone();
            let params = bind(params).map_err(storage)?;
            let (columns, rows) = if let Some(tx) = tx {
                let result = self
                    .checked(self.in_runtime(tx.execute(sql, params)).await)
                    .await?;
                (result.columns, result.rows)
            } else {
                let result = self
                    .checked(self.in_runtime(self.db.query(sql, params)).await)
                    .await?;
                (result.columns, result.rows)
            };
            Ok(WorkerRows {
                columns,
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|cell| match cell {
                                ColumnValue::Null => WorkerCell::Null,
                                ColumnValue::Integer(v) => WorkerCell::Integer(v),
                                ColumnValue::Float(v) => WorkerCell::Real(v),
                                ColumnValue::Text(v) => WorkerCell::Text(v.into_bytes()),
                                ColumnValue::Blob(v) => WorkerCell::Blob(v),
                            })
                            .collect()
                    })
                    .collect(),
            })
        }
        .boxed_local()
    }
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<usize, Error>> {
        async move {
            self.check()?;
            let tx = self.transaction.borrow().clone();
            let params = bind(params).map_err(storage)?;
            let changes = if let Some(tx) = tx {
                self.checked(self.in_runtime(tx.execute(sql, params)).await)
                    .await?
                    .changes
            } else {
                self.checked(self.in_runtime(self.db.run(sql, params)).await)
                    .await?
                    .changes
            };
            usize::try_from(changes).map_err(storage)
        }
        .boxed_local()
    }
    fn execute_script<'a>(&'a self, sql: &'a str) -> LocalBoxFuture<'a, Result<(), Error>> {
        async move {
            self.check()?;
            let tx = self.transaction.borrow().clone();
            let result = if let Some(tx) = tx {
                self.in_runtime(tx.exec(sql)).await
            } else {
                self.in_runtime(self.db.exec(sql)).await
            };
            self.checked(result).await?;
            Ok(())
        }
        .boxed_local()
    }
    fn begin_savepoint<'a>(&'a self, name: &'a str) -> LocalBoxFuture<'a, Result<(), Error>> {
        async move {
            self.check()?;
            let tx = self.transaction.borrow().clone();
            if let Some(tx) = tx {
                self.checked(self.in_runtime(tx.exec(format!("SAVEPOINT {name}"))).await)
                    .await?;
            } else {
                let tx = self
                    .checked(self.in_runtime(self.db.begin_transaction(None)).await)
                    .await?;
                *self.transaction.borrow_mut() = Some(tx);
            }
            self.savepoints.borrow_mut().push(name.into());
            Ok(())
        }
        .boxed_local()
    }
    fn end_savepoint<'a>(
        &'a self,
        name: &'a str,
        keep: bool,
    ) -> LocalBoxFuture<'a, Result<(), Error>> {
        async move {
            // Cleanup is still allowed after an error, but no later SQL is.
            let tx = self.transaction.borrow().clone();
            let Some(tx) = tx else {
                return if keep {
                    Err(storage("Rivet transaction was rolled back"))
                } else {
                    Ok(())
                };
            };
            if self.savepoints.borrow().last().map(String::as_str) != Some(name) {
                return Err(storage("Rivet savepoints must finish in reverse order"));
            }
            if self.savepoints.borrow().len() == 1 {
                let result = if keep {
                    self.in_runtime(tx.commit()).await
                } else {
                    self.in_runtime(tx.rollback()).await
                };
                self.checked(result).await?;
                self.transaction.borrow_mut().take();
            } else {
                let sql = if keep {
                    format!("RELEASE {name}")
                } else {
                    format!("ROLLBACK TO {name}; RELEASE {name}")
                };
                self.checked(self.in_runtime(tx.exec(sql)).await).await?;
            }
            self.savepoints.borrow_mut().pop();
            Ok(())
        }
        .boxed_local()
    }
    fn transaction_open(&self) -> bool {
        self.transaction.borrow().is_some()
    }
    fn parameter_limit(&self) -> Result<usize, Error> {
        Ok(self.parameters)
    }
    fn request_close(&self) {
        self.poisoned.set(true);
        if let Some(tx) = self.transaction.borrow_mut().take() {
            self.runtime.spawn(async move {
                let _ = tx.rollback().await;
            });
        }
    }
    fn close(&self) -> LocalBoxFuture<'_, Result<(), Error>> {
        async move {
            self.poisoned.set(true);
            let tx = self.transaction.borrow_mut().take();
            if let Some(tx) = tx {
                self.in_runtime(tx.rollback()).await.map_err(storage)?;
            }
            // RivetKit owns the database, including its final state save and close.
            Ok(())
        }
        .boxed_local()
    }
}

/// Borrow the actor's public database API. Call only once per Jazz owner;
/// the actor runtime owns database initialization and shutdown.
pub async fn open_store(db: SqliteDb) -> anyhow::Result<jazz_db::host::StoreFile> {
    open_store_with_failure(db, std::sync::Arc::new(|_| {})).await
}
pub(crate) async fn open_store_with_failure(
    db: SqliteDb,
    failure: std::sync::Arc<dyn Fn(String) + Send + Sync>,
) -> anyhow::Result<jazz_db::host::StoreFile> {
    let parameters = parameter_limit(&db).await?;
    let mut connection = connection_with_limit(db, parameters);
    connection.failure = failure;
    Ok(jazz_db::host::StoreFile::worker(connection).await?)
}
fn connection_with_limit(db: SqliteDb, parameters: usize) -> RivetConnection {
    RivetConnection {
        db,
        parameters,
        runtime: tokio::runtime::Handle::current(),
        transaction: RefCell::new(None),
        savepoints: RefCell::new(Vec::new()),
        poisoned: Cell::new(false),
        failure: std::sync::Arc::new(|_| {}),
    }
}
#[cfg(feature = "testing")]
pub async fn test_connection(db: SqliteDb) -> anyhow::Result<RivetConnection> {
    let parameters = parameter_limit(&db).await?;
    Ok(connection_with_limit(db, parameters))
}

pub async fn open_jazz(db: SqliteDb, config: DbConfig) -> anyhow::Result<Db> {
    Ok(Db::open_file(config, open_store(db).await?).await?)
}
