//! Full Jazz Db adapter for Rivet's public async SQLite worker.
use crate::sql::{bind, parameter_limit};
use futures::{FutureExt, future::LocalBoxFuture};
use jazz_db::{
    backend::{WorkerCell, WorkerRows, WorkerSql},
    db::{Db, DbConfig, Error, ErrorCode},
};
use jazz_sql::Param;
use sqlite_embedded::{
    database::NativeDatabaseHandle, query::ColumnValue, vfs::SqliteTransportHandle,
};

fn storage(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Storage, error.to_string())
}

struct RivetConnection {
    db: NativeDatabaseHandle,
    parameters: usize,
    runtime: tokio::runtime::Handle,
}
impl WorkerSql for RivetConnection {
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<WorkerRows, Error>> {
        async move {
            let result = self
                .db
                .query(sql.into(), bind(params).map_err(storage)?)
                .await
                .map_err(storage)?;
            Ok(WorkerRows {
                columns: result.columns,
                rows: result
                    .rows
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
            let result = self
                .db
                .run(sql.into(), bind(params).map_err(storage)?)
                .await
                .map_err(storage)?;
            usize::try_from(result.changes).map_err(storage)
        }
        .boxed_local()
    }
    fn execute_script<'a>(&'a self, sql: &'a str) -> LocalBoxFuture<'a, Result<(), Error>> {
        async move {
            self.db.exec(sql.into()).await.map_err(storage)?;
            Ok(())
        }
        .boxed_local()
    }
    fn transaction_open(&self) -> bool {
        self.db.explicit_transaction_open()
    }
    fn parameter_limit(&self) -> Result<usize, Error> {
        Ok(self.parameters)
    }
    fn request_close(&self) {
        self.db.request_close();
    }
    fn close(&self) -> LocalBoxFuture<'_, Result<(), Error>> {
        // The Jazz owner thread parks outside Tokio. Enter the worker runtime
        // for each poll so the SDK's close watchdog can create its timer.
        let mut close = Box::pin(self.db.close_and_wait());
        futures::future::poll_fn(move |cx| {
            let _runtime = self.runtime.enter();
            close
                .as_mut()
                .poll(cx)
                .map(|result| result.map_err(storage))
        })
        .boxed_local()
    }
}

/// Open admitted storage for the existing Jazz server's catalogue, accounts,
/// and core Db. Rivet owns the only SQLite connection in this build.
pub async fn open_store(
    transport: SqliteTransportHandle,
    actor_id: String,
    generation: u64,
) -> anyhow::Result<jazz_db::host::StoreFile> {
    let db = sqlite_embedded::database::open_database_from_transport(
        transport,
        actor_id,
        generation,
        tokio::runtime::Handle::current(),
        None,
    )
    .await?;
    let result = async {
        let parameters = parameter_limit(&db).await?;
        jazz_db::host::StoreFile::worker(RivetConnection {
            db: db.clone(),
            parameters,
            runtime: tokio::runtime::Handle::current(),
        })
        .await
        .map_err(anyhow::Error::from)
    }
    .await;
    if result.is_err() {
        db.close_and_wait().await?;
    }
    result
}

/// Open a full Jazz database using Rivet-owned SQLite. Drive complete operations
/// through DbOwner, or the server's dedicated owner thread, and drain on shutdown.
pub async fn open_jazz(
    transport: SqliteTransportHandle,
    actor_id: String,
    generation: u64,
    config: DbConfig,
) -> anyhow::Result<Db> {
    Ok(Db::open_file(config, open_store(transport, actor_id, generation).await?).await?)
}
