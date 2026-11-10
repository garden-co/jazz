//! Lower-level SQL transactions and scalar mapping for the Rivet worker.
use anyhow::{Result, ensure};
use jazz_sql::{HostTable, Param, Sql};
use sqlite_embedded::{
    database::NativeDatabaseHandle,
    query::{BindParam, ColumnValue},
    vfs::SqliteTransportHandle,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Clone)]
pub struct Store {
    db: NativeDatabaseHandle,
    runtime: tokio::runtime::Handle,
    lease: Arc<Mutex<()>>,
    poisoned: Arc<AtomicBool>,
    parameter_limit: usize,
}

impl Store {
    pub async fn open(
        transport: SqliteTransportHandle,
        actor_id: String,
        generation: u64,
    ) -> Result<Self> {
        let db = sqlite_embedded::database::open_database_from_transport(
            transport,
            actor_id,
            generation,
            tokio::runtime::Handle::current(),
            None,
        )
        .await?;
        let parameter_limit = match parameter_limit(&db).await {
            Ok(limit) => limit,
            Err(error) => {
                db.close_and_wait().await?;
                return Err(error);
            }
        };
        Ok(Self {
            db,
            parameter_limit,
            runtime: tokio::runtime::Handle::current(),
            lease: Arc::new(Mutex::new(())),
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn begin(&self) -> Result<Transaction> {
        let lease = self.lease.clone().lock_owned().await;
        ensure!(
            !self.poisoned.load(Ordering::SeqCst),
            "SQLite transaction cleanup failed; reopen the actor"
        );
        // Arm before awaiting BEGIN: cancellation can occur after the worker
        // accepted the command but before the caller observed its completion.
        let mut tx = Transaction {
            store: self.clone(),
            lease: Some(lease),
        };
        tx.execute("SAVEPOINT jazz_async_host", &[]).await?;
        Ok(tx)
    }

    pub async fn put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let mut tx = self.begin().await?;
        match jazz_sql::put_if_absent_in_transaction(&mut tx, table, key, value).await {
            Ok(prior) => {
                tx.commit().await?;
                Ok(prior)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut tx = self.begin().await?;
        let result = jazz_sql::get(&mut tx, table, key).await;
        tx.rollback().await?;
        result
    }

    pub async fn close(self) -> Result<()> {
        let _lease = self.lease.lock().await;
        self.db.close_and_wait().await
    }
}

pub struct Transaction {
    store: Store,
    lease: Option<OwnedMutexGuard<()>>,
}

impl Transaction {
    pub async fn commit(mut self) -> Result<()> {
        self.execute("RELEASE jazz_async_host", &[]).await?;
        self.lease.take();
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<()> {
        rollback(&self.store.db).await?;
        self.lease.take();
        Ok(())
    }
}

async fn rollback(db: &NativeDatabaseHandle) -> Result<()> {
    // Always enqueue cleanup: a cancelled BEGIN may be accepted but not yet
    // executed, so checking cached state before this command would race it.
    // Once cleanup completes, the SDK publishes state before delivering its
    // result. SQLite may already have rolled back the whole transaction.
    match db
        .exec("ROLLBACK TO jazz_async_host; RELEASE jazz_async_host".into())
        .await
    {
        Ok(_) => Ok(()),
        Err(_) if !db.explicit_transaction_open() => Ok(()),
        Err(error) => Err(error),
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        let store = self.store.clone();
        // If the cleanup task itself is cancelled, the connection stays poisoned.
        store.poisoned.store(true, Ordering::SeqCst);
        self.store.runtime.spawn(async move {
            let _lease = lease;
            if rollback(&store.db).await.is_ok() {
                store.poisoned.store(false, Ordering::SeqCst);
            }
        });
    }
}

pub(crate) async fn parameter_limit(db: &NativeDatabaseHandle) -> Result<usize> {
    // This worker does not change SQLITE_LIMIT_VARIABLE_NUMBER. Read the
    // engine's compiled bound once rather than assume a SQLite version's
    // default. The SDK guard only changes the value-length limit.
    let limits = db.query(
        "SELECT compile_options FROM pragma_compile_options WHERE compile_options LIKE 'MAX_VARIABLE_NUMBER=%' LIMIT 1".into(),
        None,
    ).await?;
    let parameter_limit = match limits.rows.first().and_then(|row| row.first()) {
        Some(ColumnValue::Text(option)) => option
            .strip_prefix("MAX_VARIABLE_NUMBER=")
            .ok_or_else(|| anyhow::anyhow!("invalid SQLite parameter limit"))?
            .parse::<usize>()?,
        _ => anyhow::bail!("Rivet SQLite did not report its parameter limit"),
    };
    ensure!(parameter_limit > 0, "invalid SQLite parameter limit");
    Ok(parameter_limit)
}

pub(crate) fn bind(params: &[Param<'_>]) -> Result<Option<Vec<BindParam>>> {
    Ok(Some(
        params
            .iter()
            .map(|param| {
                Ok(match param {
                    Param::Null => BindParam::Null,
                    Param::Integer(value) => BindParam::Integer(*value),
                    Param::Real(value) => BindParam::Float(*value),
                    Param::Blob(value) => BindParam::Blob(value.to_vec()),
                    Param::Text(value) => BindParam::Text((*value).into()),
                    Param::TextBytes(value) => BindParam::Text(
                        std::str::from_utf8(value)
                            .map_err(|_| {
                                jazz_sql::CellError(
                                    "Binding non-UTF-8 TEXT is not supported by Rivet SQLite",
                                )
                            })?
                            .into(),
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

pub struct RivetRow<'a>(&'a [ColumnValue], &'a [String]);

impl jazz_sql::Row for RivetRow<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn column_name(&self, index: usize) -> Option<&str> {
        self.1.get(index).map(String::as_str)
    }
    fn cell(&self, index: usize) -> Option<jazz_sql::Cell<'_>> {
        use jazz_sql::Cell;
        Some(match self.0.get(index)? {
            ColumnValue::Null => Cell::Null,
            ColumnValue::Integer(value) => Cell::Integer(*value),
            ColumnValue::Float(value) => Cell::Real(*value),
            ColumnValue::Text(value) => Cell::Text(value.as_bytes()),
            ColumnValue::Blob(value) => Cell::Blob(value),
        })
    }
}

impl Sql for Transaction {
    type Error = anyhow::Error;
    fn invalid_data(message: &'static str) -> Self::Error {
        anyhow::anyhow!(message)
    }
    type Row<'a> = RivetRow<'a>;

    async fn execute_script(&mut self, script: &str) -> Result<()> {
        self.store.db.exec(script.into()).await?;
        Ok(())
    }

    async fn query<F>(&mut self, sql: &str, params: &[Param<'_>], mut visit: F) -> Result<()>
    where
        F: FnMut(Self::Row<'_>) -> Result<std::ops::ControlFlow<()>>,
    {
        let result = self.store.db.query(sql.into(), bind(params)?).await?;
        for row in &result.rows {
            if visit(RivetRow(row, &result.columns))?.is_break() {
                break;
            }
        }
        Ok(())
    }

    async fn execute(&mut self, sql: &str, params: &[Param<'_>]) -> Result<usize> {
        let result = self.store.db.run(sql.into(), bind(params)?).await?;
        Ok(usize::try_from(result.changes)?)
    }
    async fn optional_blob(&mut self, sql: &str, params: &[Param<'_>]) -> Result<Option<Vec<u8>>> {
        let result = self.store.db.query(sql.into(), bind(params)?).await?;
        ensure!(
            result.rows.len() <= 1,
            "host query returned more than one row"
        );
        match result.rows.into_iter().next() {
            None => Ok(None),
            Some(mut row) => {
                ensure!(row.len() == 1, "host query returned more than one column");
                match row.pop() {
                    Some(ColumnValue::Blob(bytes)) => Ok(Some(bytes)),
                    _ => anyhow::bail!("host query did not return a BLOB"),
                }
            }
        }
    }
}

impl jazz_sql::Connection for Transaction {
    fn transaction_open(&self) -> bool {
        self.store.db.explicit_transaction_open()
    }
    fn parameter_limit(&self) -> Result<usize> {
        Ok(self.store.parameter_limit)
    }
}
