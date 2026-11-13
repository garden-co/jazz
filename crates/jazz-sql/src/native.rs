//! Native SQLite completes each operation inline and returns `Ready`.
//! Query rows and parameters borrow SQLite/caller memory; neither is copied
//! merely to cross the async boundary.
use rusqlite::Connection;

pub struct NativeRow<'a>(&'a rusqlite::Row<'a>);

impl crate::Row for NativeRow<'_> {
    fn len(&self) -> usize {
        self.0.as_ref().column_count()
    }
    fn column_name(&self, index: usize) -> Option<&str> {
        self.0.as_ref().column_name(index).ok()
    }
    fn cell(&self, index: usize) -> Option<crate::Cell<'_>> {
        use rusqlite::types::ValueRef;
        Some(match self.0.get_ref(index).ok()? {
            ValueRef::Null => crate::Cell::Null,
            ValueRef::Integer(value) => crate::Cell::Integer(value),
            ValueRef::Real(value) => crate::Cell::Real(value),
            ValueRef::Text(value) => crate::Cell::Text(value),
            ValueRef::Blob(value) => crate::Cell::Blob(value),
        })
    }
}

/// Borrow the existing owner connection. Both futures complete in their first
/// poll; prepared statement caching and borrowed parameter binding stay native.
pub struct Native<'a>(pub &'a Connection);

struct NativeParam<'a>(crate::Param<'a>);
impl rusqlite::types::ToSql for NativeParam<'_> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, ValueRef};
        Ok(ToSqlOutput::Borrowed(match self.0 {
            crate::Param::Null => ValueRef::Null,
            crate::Param::Integer(value) => ValueRef::Integer(value),
            crate::Param::Real(value) => ValueRef::Real(value),
            crate::Param::Blob(value) => ValueRef::Blob(value),
            crate::Param::Text(value) => ValueRef::Text(value.as_bytes()),
            crate::Param::TextBytes(value) => ValueRef::Text(value),
        }))
    }
}

impl crate::Sql for Native<'_> {
    type Error = rusqlite::Error;
    type Row<'a> = NativeRow<'a>;
    fn invalid_data(message: &'static str) -> Self::Error {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Blob,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                message,
            )),
        )
    }

    fn execute_script(
        &mut self,
        script: &str,
    ) -> impl std::future::Future<Output = rusqlite::Result<()>> {
        std::future::ready((|| {
            // SQLite applies the statement-length limit to the entire prepare
            // input. Split with SQLite's parser so large generated scripts also
            // work with restricted hosts, without splitting trigger bodies.
            let mut start = 0;
            for (end, _) in script.match_indices(';') {
                let statement = &script[start..=end];
                let text = std::ffi::CString::new(statement)
                    .map_err(|_| Self::invalid_data("NUL in SQL"))?;
                // SAFETY: a NUL-terminated string read only by SQLite.
                if unsafe { rusqlite::ffi::sqlite3_complete(text.as_ptr()) } != 0 {
                    self.0.execute_batch(statement)?;
                    start = end + 1;
                }
            }
            let rest = &script[start..];
            if !rest.trim().is_empty() {
                self.0.execute_batch(rest)?;
            }
            Ok(())
        })())
    }

    fn query<F>(
        &mut self,
        sql: &str,
        params: &[crate::Param<'_>],
        mut visit: F,
    ) -> impl std::future::Future<Output = rusqlite::Result<()>>
    where
        F: FnMut(Self::Row<'_>) -> rusqlite::Result<std::ops::ControlFlow<()>>,
    {
        std::future::ready((|| {
            let mut statement = self.0.prepare_cached(sql)?;
            let mut rows = statement.query(rusqlite::params_from_iter(
                params.iter().copied().map(NativeParam),
            ))?;
            while let Some(row) = rows.next()? {
                if visit(NativeRow(row))?.is_break() {
                    break;
                }
            }
            Ok(())
        })())
    }

    fn query_with_stats<F>(
        &mut self,
        sql: &str,
        params: &[crate::Param<'_>],
        mut visit: F,
    ) -> impl std::future::Future<Output = rusqlite::Result<crate::QueryStats>>
    where
        F: FnMut(Self::Row<'_>) -> rusqlite::Result<std::ops::ControlFlow<()>>,
    {
        std::future::ready((|| {
            use rusqlite::StatementStatus;
            let mut statement = self.0.prepare_cached(sql)?;
            statement.reset_status(StatementStatus::FullscanStep);
            statement.reset_status(StatementStatus::Sort);
            let mut rows = statement.query(rusqlite::params_from_iter(
                params.iter().copied().map(NativeParam),
            ))?;
            while let Some(row) = rows.next()? {
                if visit(NativeRow(row))?.is_break() {
                    break;
                }
            }
            drop(rows);
            Ok(crate::QueryStats {
                fullscan_steps: Some(statement.reset_status(StatementStatus::FullscanStep) as u64),
                sorted: Some(statement.reset_status(StatementStatus::Sort) > 0),
            })
        })())
    }

    fn execute(
        &mut self,
        sql: &str,
        params: &[crate::Param<'_>],
    ) -> impl std::future::Future<Output = rusqlite::Result<usize>> {
        // Complete synchronously and return Ready, without building a second
        // async state machine around each native SQLite call.
        std::future::ready(self.0.prepare_cached(sql).and_then(|mut statement| {
            statement.execute(rusqlite::params_from_iter(
                params.iter().copied().map(NativeParam),
            ))
        }))
    }
}

impl From<crate::CellError> for rusqlite::Error {
    fn from(error: crate::CellError) -> Self {
        Self::FromSqlConversionFailure(0, rusqlite::types::Type::Null, Box::new(error))
    }
}

impl crate::Connection for Native<'_> {
    fn transaction_open(&self) -> bool {
        !self.0.is_autocommit()
    }
    fn parameter_limit(&self) -> rusqlite::Result<usize> {
        let limit = self
            .0
            .limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER)?;
        usize::try_from(limit)
            .map_err(|_| <Self as crate::Sql>::invalid_data("invalid SQLite parameter limit"))
    }
    fn total_changes(&mut self) -> impl std::future::Future<Output = rusqlite::Result<u64>> {
        std::future::ready(Ok(self.0.total_changes()))
    }
}
