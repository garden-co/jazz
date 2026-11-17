// SQL test provider that yields before every statement.
use jazz_sql::{Param, Sql, native::Native};
use std::ops::ControlFlow;
use std::task::Poll;

pub(crate) struct Suspends<'a>(pub(crate) Native<'a>);

async fn yield_once() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

impl Sql for Suspends<'_> {
    type Error = rusqlite::Error;
    type Row<'a> = <Native<'a> as Sql>::Row<'a>;
    fn invalid_data(message: &'static str) -> Self::Error {
        Native::invalid_data(message)
    }

    async fn execute_script(&mut self, script: &str) -> Result<(), Self::Error> {
        yield_once().await;
        self.0.execute_script(script).await
    }
    async fn execute(&mut self, sql: &str, params: &[Param<'_>]) -> Result<usize, Self::Error> {
        yield_once().await;
        self.0.execute(sql, params).await
    }
    async fn query<F>(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
        visit: F,
    ) -> Result<(), Self::Error>
    where
        F: FnMut(Self::Row<'_>) -> Result<ControlFlow<()>, Self::Error>,
    {
        yield_once().await;
        self.0.query(sql, params, visit).await
    }
}

/// Diagnostic adapter for whole-Db suspension tests. Transaction control still
/// belongs to native SQLite; this does not simulate an async transaction owner.
pub(crate) struct ObservedSql<'a> {
    pub(crate) native: Native<'a>,
    pub(crate) suspensions: Option<&'a std::sync::atomic::AtomicUsize>,
}
impl ObservedSql<'_> {
    async fn before(&self) {
        if let Some(count) = self.suspensions {
            yield_once().await;
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}
impl Sql for ObservedSql<'_> {
    type Error = rusqlite::Error;
    type Row<'a> = <Native<'a> as Sql>::Row<'a>;
    fn invalid_data(message: &'static str) -> Self::Error {
        Native::invalid_data(message)
    }
    async fn execute_script(&mut self, script: &str) -> Result<(), Self::Error> {
        self.before().await;
        self.native.execute_script(script).await
    }
    async fn execute(&mut self, sql: &str, params: &[Param<'_>]) -> Result<usize, Self::Error> {
        self.before().await;
        self.native.execute(sql, params).await
    }
    async fn query<F>(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
        visit: F,
    ) -> Result<(), Self::Error>
    where
        F: FnMut(Self::Row<'_>) -> Result<ControlFlow<()>, Self::Error>,
    {
        self.before().await;
        self.native.query(sql, params, visit).await
    }
    async fn query_with_stats<F>(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
        visit: F,
    ) -> Result<jazz_sql::QueryStats, Self::Error>
    where
        F: FnMut(Self::Row<'_>) -> Result<ControlFlow<()>, Self::Error>,
    {
        self.before().await;
        self.native.query_with_stats(sql, params, visit).await
    }
}

impl jazz_sql::Connection for ObservedSql<'_> {
    fn transaction_open(&self) -> bool {
        jazz_sql::Connection::transaction_open(&self.native)
    }
    fn parameter_limit(&self) -> Result<usize, Self::Error> {
        jazz_sql::Connection::parameter_limit(&self.native)
    }
    async fn total_changes(&mut self) -> Result<u64, Self::Error> {
        self.before().await;
        jazz_sql::Connection::total_changes(&mut self.native).await
    }
}
