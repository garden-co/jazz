//! Real RivetKit actor SQLite, with faults injected at Jazz's provider boundary.
//! We never mock Rivet's page transport or depend on its internal crates.
#![allow(dead_code)]
use futures::{FutureExt, future::LocalBoxFuture};
use jazz_db::{
    backend::{WorkerRows, WorkerSql},
    db::{Db, DbConfig, Error, ErrorCode},
    host::StoreFile,
};
use jazz_sql::Param;
use rivetkit::{Actor, Ctx, SqliteDb};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

static DATABASES: OnceLock<Mutex<HashMap<String, SqliteDb>>> = OnceLock::new();
struct DatabaseActor;
#[derive(serde::Serialize, serde::Deserialize)]
struct Ready;
impl rivetkit::Action for Ready {
    type Output = String;
    const NAME: &'static str = "ready";
}
#[async_trait::async_trait]
impl Actor for DatabaseActor {
    type Input = ();
    type Actions = (Ready,);
    type Events = ();
    type ConnParams = ();
    type Action = rivetkit::action::Raw;
    async fn create(ctx: &Ctx<Self>) -> anyhow::Result<Self> {
        DATABASES
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(ctx.actor_id().into(), ctx.sql().clone());
        Ok(Self)
    }
}
impl rivetkit::Handler<Ready> for DatabaseActor {
    async fn handle(self: Arc<Self>, ctx: Ctx<Self>, _: Ready) -> anyhow::Result<String> {
        Ok(ctx.actor_id().into())
    }
}
#[derive(Default)]
pub struct DatabaseFixture {
    runtime: tokio::sync::OnceCell<rivetkit::test::TestHandle>,
    prefix: OnceLock<String>,
    pub fail_next: AtomicU8,
    pause_commit: AtomicBool,
    pub commit_entered: tokio::sync::Notify,
    pub commit_release: tokio::sync::Notify,
}
impl DatabaseFixture {
    pub async fn database(&self, key: &str) -> anyhow::Result<SqliteDb> {
        let runtime = self
            .runtime
            .get_or_try_init(|| async {
                let mut registry = rivetkit::Registry::new();
                registry.register_actor::<DatabaseActor>("database");
                rivetkit::test::setup(registry).await
            })
            .await?;
        let key = format!(
            "{}-{key}",
            self.prefix.get_or_init(|| uuid::Uuid::new_v4().to_string())
        );
        let actor = runtime.actor_with_key::<DatabaseActor>("database", [key.as_str()]);
        let id = actor.send(Ready).await?;
        Ok(DATABASES.get().unwrap().lock().unwrap()[&id].clone())
    }
    pub fn pause_next_commit(self: &Arc<Self>) -> CommitPause {
        self.pause_commit.store(true, Ordering::SeqCst);
        CommitPause(self.clone())
    }
}
pub struct CommitPause(Arc<DatabaseFixture>);
impl Drop for CommitPause {
    fn drop(&mut self) {
        self.0.commit_release.notify_one();
    }
}
struct Faults<T> {
    inner: T,
    pages: Arc<DatabaseFixture>,
    depth: std::cell::Cell<usize>,
}
impl<T: WorkerSql> WorkerSql for Faults<T> {
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<WorkerRows, Error>> {
        self.inner.query(sql, params)
    }
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Param<'a>],
    ) -> LocalBoxFuture<'a, Result<usize, Error>> {
        self.inner.execute(sql, params)
    }
    fn execute_script<'a>(&'a self, sql: &'a str) -> LocalBoxFuture<'a, Result<(), Error>> {
        self.inner.execute_script(sql)
    }
    fn begin_savepoint<'a>(&'a self, name: &'a str) -> LocalBoxFuture<'a, Result<(), Error>> {
        async move {
            self.inner.begin_savepoint(name).await?;
            self.depth.set(self.depth.get() + 1);
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
            let outer = self.depth.get() == 1 && keep;
            if outer && self.pages.pause_commit.swap(false, Ordering::SeqCst) {
                self.pages.commit_entered.notify_one();
                self.pages.commit_release.notified().await;
            }
            let failure = if outer {
                self.pages.fail_next.swap(0, Ordering::SeqCst)
            } else {
                0
            };
            if failure == 1 {
                return Err(Error::new(ErrorCode::Storage, "injected refused commit"));
            }
            self.inner.end_savepoint(name, keep).await?;
            self.depth.set(self.depth.get().saturating_sub(1));
            if failure == 2 {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "injected lost commit acknowledgement",
                ));
            }
            Ok(())
        }
        .boxed_local()
    }
    fn transaction_open(&self) -> bool {
        self.inner.transaction_open()
    }
    fn parameter_limit(&self) -> Result<usize, Error> {
        self.inner.parameter_limit()
    }
    fn request_close(&self) {
        self.inner.request_close();
    }
    fn close(&self) -> LocalBoxFuture<'_, Result<(), Error>> {
        self.inner.close()
    }
}
async fn file(pages: Arc<DatabaseFixture>, db: SqliteDb) -> anyhow::Result<StoreFile> {
    Ok(StoreFile::worker(Faults {
        inner: jazz_rivet::test_connection(db).await?,
        pages,
        depth: Default::default(),
    })
    .await?)
}
pub async fn open_jazz(
    pages: Arc<DatabaseFixture>,
    actor: String,
    _: u64,
    config: DbConfig,
) -> anyhow::Result<Db> {
    let db = pages.database(&actor).await?;
    Ok(Db::open_file(config, file(pages, db).await?).await?)
}
pub async fn worker_store(
    pages: Arc<DatabaseFixture>,
    actor: String,
) -> anyhow::Result<jazz_server::runtime::WorkerStore> {
    let db = pages.database(&actor).await?;
    let runtime = tokio::runtime::Handle::current();
    Ok(jazz_server::runtime::WorkerStore::new(move || {
        let _runtime = runtime.enter();
        futures::executor::block_on(file(pages.clone(), db.clone())).map_err(|e| e.to_string())
    }))
}
