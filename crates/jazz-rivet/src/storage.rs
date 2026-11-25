//! Jazz's managed SQLite interface over the actor's transaction-scoped SQL API.
use jazz::storage::{Backend, Cursor, SqlError, SqlResult, TransactionBody, Value, backend_error};
use rivetkit::{BindParam, ColumnValue, SqliteDb, SqliteTransaction};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::runtime::Handle;

#[derive(Clone)]
pub struct Connection {
    db: SqliteDb,
    runtime: Handle,
    transaction: Option<SqliteTransaction>,
    depth: usize,
    failed: Arc<AtomicBool>,
    stop: Arc<dyn Fn(String) + Send + Sync>,
}

fn error(cause: impl std::fmt::Display) -> SqlError {
    backend_error(1, cause.to_string())
}

impl Connection {
    pub fn new(
        db: SqliteDb,
        runtime: Handle,
        stop: impl Fn(String) + Send + Sync + 'static,
    ) -> Self {
        Self {
            db,
            runtime,
            transaction: None,
            depth: 0,
            failed: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(stop),
        }
    }

    // Jazz polls on its store thread. Rivet schedules I/O on the actor runtime.
    async fn run<T>(
        &self,
        future: impl Future<Output = rivetkit::prelude::Result<T>>,
    ) -> SqlResult<T> {
        if self.failed.load(Ordering::Acquire) {
            return Err(error("Rivet transaction failed; reopen the actor database"));
        }
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|cx| {
            let _entered = self.runtime.enter();
            future.as_mut().poll(cx)
        })
        .await
        .map_err(|cause| {
            let cause = error(cause);
            if self.transaction.is_some() {
                self.transaction_error(cause)
            } else {
                cause
            }
        })
    }

    // Rivet does not expose SQLite autocommit state after a statement failure.
    // Resume from durable state before admitting any further Jazz operations.
    fn transaction_error(&self, cause: SqlError) -> SqlError {
        if !self.failed.swap(true, Ordering::AcqRel) {
            (self.stop)(format!("Jazz transaction failed: {cause}"));
        }
        cause
    }

    async fn scalar(&self, sql: &str) -> SqlResult<i64> {
        let mut rows = self.query(sql, &[]).await?;
        match rows.next().await? {
            Some([Value::Integer(n)]) => Ok(*n),
            _ => Err(error("expected one SQLite integer")),
        }
    }
}

fn bind(params: &[Value]) -> SqlResult<Option<Vec<BindParam>>> {
    params
        .iter()
        .map(|value| {
            Ok(match value {
                Value::Null => BindParam::Null,
                Value::Integer(v) => BindParam::Integer(*v),
                Value::Real(v) => BindParam::Float(*v),
                Value::Text(v) => BindParam::Text(std::str::from_utf8(v).map_err(error)?.into()),
                Value::Blob(v) => BindParam::Blob(v.clone()),
            })
        })
        .collect::<SqlResult<Vec<_>>>()
        .map(Some)
}

struct Rows {
    columns: Vec<String>,
    rows: std::vec::IntoIter<Vec<ColumnValue>>,
    current: Vec<Value>,
}

#[async_trait::async_trait(?Send)]
impl Cursor for Rows {
    fn columns(&self) -> &[String] {
        &self.columns
    }
    async fn next(&mut self) -> SqlResult<Option<&[Value]>> {
        let Some(row) = self.rows.next() else {
            return Ok(None);
        };
        self.current.clear();
        self.current.extend(row.into_iter().map(|v| match v {
            ColumnValue::Null => Value::Null,
            ColumnValue::Integer(v) => Value::Integer(v),
            ColumnValue::Float(v) => Value::Real(v),
            ColumnValue::Text(v) => Value::Text(v.into_bytes()),
            ColumnValue::Blob(v) => Value::Blob(v),
        }));
        Ok(Some(&self.current))
    }
}

#[async_trait::async_trait(?Send)]
impl Backend for Connection {
    async fn query(&self, sql: &str, params: &[Value]) -> SqlResult<Box<dyn Cursor + '_>> {
        // Rivet returns a complete result. Bound it inside SQLite, before transfer.
        const MAX_ROWS: usize = 4096;
        let sql = sql.trim().trim_end_matches(';');
        let upper = sql
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        let query = match upper.as_str() {
            "SELECT" | "WITH" => format!("SELECT * FROM ({sql}\n) LIMIT {}", MAX_ROWS + 1),
            "PRAGMA"
                if sql.to_ascii_lowercase().starts_with("pragma table_info(")
                    || sql.eq_ignore_ascii_case("PRAGMA schema_version") =>
            {
                sql.to_string()
            }
            _ => {
                return Err(error(format!(
                    "Reading {upper} statements is not supported in Rivet storage yet"
                )));
            }
        };
        let args = bind(params)?;
        let result = match &self.transaction {
            Some(tx) => self.run(tx.execute(query, args)).await?.into_query_result(),
            None => self.run(self.db.query(query, args)).await?,
        };
        if result.rows.len() > MAX_ROWS {
            return Err(backend_error(
                18,
                format!(
                    "Reading more than {MAX_ROWS} rows in one SQL cursor is not supported in Rivet storage yet"
                ),
            ));
        }
        Ok(Box::new(Rows {
            columns: result.columns,
            rows: result.rows.into_iter(),
            current: Vec::new(),
        }))
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> SqlResult<usize> {
        let params = bind(params)?;
        let changes = match &self.transaction {
            Some(tx) => self.run(tx.execute(sql, params)).await?.changes,
            None => self.run(self.db.run(sql, params)).await?.changes,
        };
        usize::try_from(changes).map_err(error)
    }

    async fn execute_script(&self, script: &str) -> SqlResult<()> {
        match &self.transaction {
            Some(tx) => self.run(tx.exec(script)).await?,
            None => self.run(self.db.exec(script)).await?,
        };
        Ok(())
    }

    async fn transaction(&self, body: TransactionBody<'_>) -> SqlResult<()> {
        let outer = self.transaction.is_none();
        let name = format!("jazz_scope_{}", self.depth);
        let tx = match &self.transaction {
            Some(tx) => {
                self.run(tx.exec(format!("SAVEPOINT {name}"))).await?;
                tx.clone()
            }
            None => self.run(self.db.begin_transaction(None)).await?,
        };
        let mut guard = Rollback {
            tx: Some(tx.clone()),
            runtime: self.runtime.clone(),
        };
        let keep = body(Arc::new(Self {
            transaction: Some(tx.clone()),
            depth: self.depth + 1,
            ..self.clone()
        }))
        .await;
        if outer {
            if keep {
                self.run(tx.commit())
                    .await
                    .map_err(|cause| self.transaction_error(cause))?;
            } else {
                self.run(tx.rollback())
                    .await
                    .map_err(|cause| self.transaction_error(cause))?;
            }
        } else {
            if !keep {
                self.run(tx.exec(format!("ROLLBACK TO {name}"))).await?;
            }
            self.run(tx.exec(format!("RELEASE {name}"))).await?;
        }
        guard.tx = None;
        Ok(())
    }
    async fn last_insert_rowid(&self) -> SqlResult<i64> {
        self.scalar("SELECT last_insert_rowid()").await
    }
    async fn total_changes(&self) -> SqlResult<u64> {
        self.scalar("SELECT total_changes()")
            .await?
            .try_into()
            .map_err(error)
    }
    fn max_parameters(&self) -> usize {
        999
    }
}

// Cancellation or panic aborts the whole Rivet lease, including nested work.
struct Rollback {
    tx: Option<SqliteTransaction>,
    runtime: Handle,
}
impl Drop for Rollback {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            self.runtime.spawn(async move {
                let _ = tx.rollback().await;
            });
        }
    }
}
