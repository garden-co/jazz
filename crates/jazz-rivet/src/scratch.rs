//! Disposable schema-validation database. Never used for actor persistence.
use futures::future::LocalBoxFuture;
use jazz_db::{
    backend::{WorkerCell, WorkerRows, WorkerSql},
    db::{Error, ErrorCode},
    host::StoreFile,
};
use jazz_sql::Param;
use rusqlite::{
    Connection,
    types::{Value, ValueRef},
};

fn error(e: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Storage, e.to_string())
}
fn params(values: &[Param<'_>]) -> Result<Vec<Value>, Error> {
    values
        .iter()
        .map(|v| {
            Ok(match v {
                Param::Null => Value::Null,
                Param::Integer(v) => Value::Integer(*v),
                Param::Real(v) => Value::Real(*v),
                Param::Text(v) => Value::Text((*v).into()),
                Param::TextBytes(v) => Value::Text(std::str::from_utf8(v).map_err(error)?.into()),
                Param::Blob(v) => Value::Blob(v.to_vec()),
            })
        })
        .collect()
}
struct Scratch(Connection);
impl WorkerSql for Scratch {
    fn query<'a>(
        &'a self,
        sql: &'a str,
        values: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<WorkerRows, Error>> {
        Box::pin(async move {
            let values = params(values)?;
            let mut stmt = self.0.prepare(sql).map_err(error)?;
            let columns = stmt
                .column_names()
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>();
            let mut rows = stmt
                .query(rusqlite::params_from_iter(values))
                .map_err(error)?;
            let mut result = Vec::new();
            while let Some(row) = rows.next().map_err(error)? {
                let mut cells = Vec::new();
                for i in 0..columns.len() {
                    cells.push(match row.get_ref(i).map_err(error)? {
                        ValueRef::Null => WorkerCell::Null,
                        ValueRef::Integer(v) => WorkerCell::Integer(v),
                        ValueRef::Real(v) => WorkerCell::Real(v),
                        ValueRef::Text(v) => WorkerCell::Text(v.to_vec()),
                        ValueRef::Blob(v) => WorkerCell::Blob(v.to_vec()),
                    });
                }
                result.push(cells);
            }
            Ok(WorkerRows {
                columns,
                rows: result,
            })
        })
    }
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        values: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<usize, Error>> {
        Box::pin(async move {
            self.0
                .execute(sql, rusqlite::params_from_iter(params(values)?))
                .map_err(error)
        })
    }
    fn execute_script<'a>(&'a self, sql: &'a str) -> LocalBoxFuture<'a, Result<(), Error>> {
        Box::pin(async move { self.0.execute_batch(sql).map_err(error) })
    }
    fn transaction_open(&self) -> bool {
        !self.0.is_autocommit()
    }
    fn parameter_limit(&self) -> Result<usize, Error> {
        Ok(self
            .0
            .limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize)
    }
    fn request_close(&self) {}
    fn close(&self) -> LocalBoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
}
pub async fn open() -> Result<StoreFile, Error> {
    let connection = Connection::open_in_memory().map_err(error)?;
    connection
        .execute_batch("PRAGMA max_page_count = 256")
        .map_err(error)?;
    StoreFile::worker(Scratch(connection)).await
}
