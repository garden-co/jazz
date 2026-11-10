//! Runtime-neutral, thread-affine ownership of complete database operations.

use super::{Db, Error, ErrorCode};
use futures::channel::{mpsc, oneshot};
use futures::future::{LocalBoxFuture, Shared};
use futures::lock::Mutex;
use futures::task::{LocalSpawn, LocalSpawnExt};
use futures::{FutureExt, Sink, StreamExt};
use jazz_sql::Connection as _;
use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::rc::Rc;

type Completion = Shared<LocalBoxFuture<'static, Result<(), Error>>>;

trait Command {
    fn run(self: Box<Self>, db: &Db) -> LocalBoxFuture<'_, Result<(), Error>>;
}

struct Operation<F, T> {
    operation: F,
    reply: oneshot::Sender<Result<T, Error>>,
}

impl<F, T> Command for Operation<F, T>
where
    F: AsyncFnOnce(&Db) -> Result<T, Error> + 'static,
    T: 'static,
{
    fn run(self: Box<Self>, db: &Db) -> LocalBoxFuture<'_, Result<(), Error>> {
        async move {
            let Self { operation, reply } = *self;
            // Receiver cancellation never cancels execution or skips cleanup.
            let result = operation(db).await;
            // A cached handle overlay belongs to its batch and may survive
            // between operations. Keep it so repeated staging stays linear.
            // Anything else left open must stop the owner before another call.
            let clean = {
                let inner = db.inner.metadata();
                !inner.conn.sql().transaction_open()
                    || (inner.overlay.is_some() && inner.conn.journal.depth() == 1)
            };
            if !clean {
                let error =
                    stopped("database operation left an open SQL transaction; owner stopped");
                let _ = reply.send(Err(error.clone()));
                return Err(error);
            }
            let _ = reply.send(result);
            Ok(())
        }
        .boxed_local()
    }
}

struct Admission {
    // Exactly one sender: futures mpsc grants a slot per sender in addition to
    // its configured buffer. Cloned handles share this sender and its lock.
    sender: Mutex<mpsc::Sender<Box<dyn Command>>>,
    closing: Cell<bool>,
    stop: RefCell<Option<oneshot::Sender<()>>>,
}

/// A bounded queue whose independently spawned task owns a full [`Db`].
///
/// The host must drive the local executor until [`Self::close`] completes.
/// One admitted operation runs to completion before the next starts, even if
/// its request future is dropped. This makes request cancellation independent
/// of storage suspension; it does not protect against terminating the runtime.
///
/// Move the database into this owner before sharing schema views or transaction
/// handles. Operations must not export database aliases to other execution
/// lanes. Return owned results instead. An operation that waits for another
/// queued operation (including a tick needed for a remote read) would deadlock:
/// enqueue finite database steps, and wait for network events outside the lane.
#[derive(Clone)]
pub struct DbOwner {
    admission: Rc<Admission>,
    completion: Completion,
}

impl DbOwner {
    /// Start a database owner on a host's local executor. `capacity` bounds
    /// queued operations, excluding the one currently running. Waiting callers
    /// retain their own input until space becomes available.
    pub fn spawn(db: Db, capacity: usize, spawner: &impl LocalSpawn) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(stopped("database queue capacity must be positive"));
        }
        let (sender, receiver) = mpsc::channel(capacity - 1);
        let (stop, shutdown) = oneshot::channel();
        let (done, completion) = oneshot::channel();
        spawner
            .spawn_local(async move {
                let result = run(db, receiver, shutdown).await;
                let _ = done.send(result);
            })
            .map_err(|error| Error::new(ErrorCode::Storage, error.to_string()))?;
        Ok(Self {
            admission: Rc::new(Admission {
                sender: Mutex::new(sender),
                closing: Cell::new(false),
                stop: RefCell::new(Some(stop)),
            }),
            completion: async move {
                completion
                    .await
                    .map_err(|_| stopped("database owner stopped before closing"))?
            }
            .boxed_local()
            .shared(),
        })
    }

    /// Run one complete operation. Admission happens at the synchronous enqueue
    /// after capacity is acquired. Cancellation before admission does no work;
    /// cancellation afterwards discards only the reply.
    pub async fn call<T, F>(&self, operation: F) -> Result<T, Error>
    where
        F: AsyncFnOnce(&Db) -> Result<T, Error> + 'static,
        T: 'static,
    {
        if self.admission.closing.get() {
            return Err(stopped("database owner is closing"));
        }
        let mut sender = self.admission.sender.lock().await;
        std::future::poll_fn(|cx| Pin::new(&mut *sender).poll_ready(cx))
            .await
            .map_err(|_| stopped("database owner stopped accepting operations"))?;
        let (reply, answer) = oneshot::channel();
        if self.admission.closing.get() {
            return Err(stopped("database owner is closing"));
        }
        Pin::new(&mut *sender)
            .start_send(Box::new(Operation { operation, reply }))
            .map_err(|_| stopped("database owner stopped accepting operations"))?;
        drop(sender);
        answer
            .await
            .map_err(|_| stopped("database owner stopped before replying"))?
    }

    /// Stop admission immediately, drain admitted work, then close and drop Db.
    /// The returned waiter may be dropped; shutdown continues independently.
    /// All handles observe the same completion (including a close error).
    pub fn close(&self) -> impl Future<Output = Result<(), Error>> + 'static {
        self.admission.closing.set(true);
        if let Some(stop) = self.admission.stop.borrow_mut().take() {
            let _ = stop.send(());
        }
        self.completion.clone()
    }
}

fn stopped(message: &str) -> Error {
    Error::new(ErrorCode::Storage, message)
}

async fn run(
    db: Db,
    mut receiver: mpsc::Receiver<Box<dyn Command>>,
    stop: oneshot::Receiver<()>,
) -> Result<(), Error> {
    // Always await worker shutdown, including a failed operation invariant.
    let result = async {
        let mut stop = stop.fuse();
        loop {
            futures::select_biased! {
                _ = stop => {
                    receiver.close();
                    while let Some(command) = receiver.next().await {
                        command.run(&db).await?;
                    }
                    break;
                },
                command = receiver.next().fuse() => match command {
                    Some(command) => command.run(&db).await?,
                    None => break,
                },
            }
        }
        db.close().await
    }
    .await;
    let closed = db.close_worker().await;
    drop(db);
    result.and(closed)
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use super::*;
    use crate::db::{DbConfig, DbIdentity};
    use crate::ids::{AuthorSubject, NodeUuid};
    use crate::query::Query;
    use crate::records::Value;
    use crate::schema::JazzSchema;
    use crate::tools::{ColumnType, PolicyExpr, TablePolicies};
    use futures::executor::LocalPool;
    use jazz_model::model::public_schema::{SchemaBuilder, TableSchemaBuilder};
    use std::collections::BTreeMap;
    use std::sync::{Arc, atomic::AtomicUsize};

    // Internal fixture because the forced-suspension SQL adapter is private.
    // Assertions use Db's public transaction and query APIs.
    async fn database(node: u8) -> Db {
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("body", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::True)
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .build(),
        )
        .unwrap();
        let mut conn: crate::host::OwnedConnection =
            rusqlite::Connection::open_in_memory().unwrap().into();
        conn.sql_suspensions = Some(Arc::new(AtomicUsize::new(0)));
        crate::format::admit(&mut conn.sql()).await.unwrap();
        Db::open_on(
            DbConfig::new(
                schema,
                DbIdentity {
                    node: NodeUuid::from_bytes([node; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ),
            conn.into(),
            None,
        )
        .await
        .unwrap()
    }

    fn cells(body: &str) -> BTreeMap<String, Value> {
        BTreeMap::from([("body".into(), Value::String(body.into()))])
    }

    async fn bodies(db: &Db) -> Result<Vec<String>, Error> {
        let query = db.prepare_query(&Query::from("notes"))?;
        Ok(db
            .read(&query)
            .await?
            .into_iter()
            .map(|row| match row.cell("body") {
                Some(Value::String(body)) => body,
                other => panic!("expected body, got {other:?}"),
            })
            .collect())
    }

    fn disconnected_transaction(fail: bool) {
        let mut pool = LocalPool::new();
        let owner = DbOwner::spawn(pool.run_until(database(71)), 1, &pool.spawner()).unwrap();
        let (resume, paused) = oneshot::channel();
        let (started, mut inserted) = oneshot::channel();
        let mut alice = Box::pin(owner.call(async move |db: &Db| {
            db.transaction(async move |tx| {
                tx.insert("notes", cells("alice"), Default::default())
                    .await?;
                let _ = started.send(());
                paused.await.unwrap();
                if fail {
                    Err(stopped("deliberate failure"))
                } else {
                    Ok(())
                }
            })
            .await
            .map(|_| ())
        }));
        assert!(alice.as_mut().now_or_never().is_none());
        pool.run_until_stalled();
        assert_eq!(inserted.try_recv().unwrap(), Some(()));
        drop(alice);

        let read_started = Rc::new(Cell::new(false));
        let observed = read_started.clone();
        let mut bob = Box::pin(owner.call(async move |db: &Db| {
            observed.set(true);
            bodies(db).await
        }));
        assert!(bob.as_mut().now_or_never().is_none());
        pool.run_until_stalled();
        assert!(
            !read_started.get(),
            "Bob cannot enter Alice's live transaction"
        );
        // Closing is immediate and independent of the shutdown waiter.
        drop(owner.close());
        assert!(pool.run_until(owner.call(async |_db| Ok(()))).is_err());
        resume.send(()).unwrap();
        let rows = pool.run_until(bob).unwrap();
        assert_eq!(
            rows,
            if fail {
                vec![]
            } else {
                vec!["alice".to_owned()]
            }
        );
        pool.run_until(owner.close()).unwrap();
        pool.run_until(owner.close()).unwrap();
    }

    /// Alice disconnects after a SQL write; Bob waits until her commit completes.
    /// Internal to select suspending SQL before Db opens.
    /// alice: insert -> pause -> drop waiter -> resume -> commit -> bob reads
    #[test]
    fn disconnected_writer_finishes_before_the_next_operation() {
        disconnected_transaction(false);
    }

    /// Alice disconnects, then her operation fails; Bob sees the rolled-back
    /// database. Internal to exercise full Db with forced storage suspension.
    #[test]
    fn disconnected_failed_writer_rolls_back_before_the_next_operation() {
        disconnected_transaction(true);
    }

    /// Alice occupies the owner, Bob fills its one queued slot, and Mallory
    /// cancels while waiting for capacity. Only Bob's admitted write runs.
    /// Internal to exercise admission alongside forced SQL suspension.
    #[test]
    fn cancellation_before_admission_never_runs_the_operation() {
        let mut pool = LocalPool::new();
        let owner = DbOwner::spawn(pool.run_until(database(71)), 1, &pool.spawner()).unwrap();
        let (resume, paused) = oneshot::channel();
        let mut alice = Box::pin(owner.call(async move |_db: &Db| {
            paused.await.unwrap();
            Ok(())
        }));
        assert!(alice.as_mut().now_or_never().is_none());
        pool.run_until_stalled();
        let mut bob = Box::pin(owner.call(async |db: &Db| {
            db.insert("notes", cells("bob"), Default::default())
                .await
                .map(|_| ())
        }));
        assert!(bob.as_mut().now_or_never().is_none());
        let mut mallory = Box::pin(owner.call(async |db: &Db| {
            db.insert("notes", cells("mallory"), Default::default())
                .await
                .map(|_| ())
        }));
        assert!(mallory.as_mut().now_or_never().is_none());
        drop(mallory);
        resume.send(()).unwrap();
        pool.run_until(alice).unwrap();
        pool.run_until(bob).unwrap();
        assert_eq!(
            pool.run_until(owner.call(async |db: &Db| bodies(db).await))
                .unwrap(),
            vec!["bob"]
        );
        pool.run_until(owner.close()).unwrap();
    }
    /// Alice is executing and Bob is queued when shutdown starts. Mallory,
    /// waiting for capacity, must be refused even when Alice frees that slot.
    /// Internal to select the suspending database fixture.
    #[test]
    fn shutdown_drains_admitted_work_and_refuses_waiting_callers() {
        let mut pool = LocalPool::new();
        let owner = DbOwner::spawn(pool.run_until(database(71)), 1, &pool.spawner()).unwrap();
        let (resume, paused) = oneshot::channel();
        let mut alice = Box::pin(owner.call(async move |_db: &Db| {
            paused.await.unwrap();
            Ok(())
        }));
        assert!(alice.as_mut().now_or_never().is_none());
        pool.run_until_stalled();
        let mut bob = Box::pin(owner.call(async |db: &Db| {
            db.insert("notes", cells("bob"), Default::default()).await?;
            bodies(db).await
        }));
        assert!(bob.as_mut().now_or_never().is_none());
        let ran = Rc::new(Cell::new(false));
        let observed = ran.clone();
        let mut mallory = Box::pin(owner.call(async move |_db: &Db| {
            observed.set(true);
            Ok(())
        }));
        assert!(mallory.as_mut().now_or_never().is_none());
        drop(owner.close());
        resume.send(()).unwrap();
        pool.run_until(alice).unwrap();
        assert_eq!(pool.run_until(bob).unwrap(), vec!["bob"]);
        assert!(pool.run_until(mallory).is_err());
        assert!(!ran.get());
        pool.run_until(owner.close()).unwrap();
    }

    /// Alice and Bob drop all owner handles and request waiters while work is
    /// admitted. The owner still drains Bob's write and closes its database.
    /// Internal to observe completion without retaining an admission handle.
    #[test]
    fn last_handle_drop_still_drains_and_closes() {
        let mut pool = LocalPool::new();
        let owner = DbOwner::spawn(pool.run_until(database(71)), 1, &pool.spawner()).unwrap();
        let completed = owner.completion.clone();
        let (resume, paused) = oneshot::channel();
        let mut alice = Box::pin(owner.call(async move |_db: &Db| {
            paused.await.unwrap();
            Ok(())
        }));
        assert!(alice.as_mut().now_or_never().is_none());
        pool.run_until_stalled();
        let (written, observed) = oneshot::channel();
        let mut bob = Box::pin(owner.call(async move |db: &Db| {
            db.insert("notes", cells("bob"), Default::default()).await?;
            let _ = written.send(bodies(db).await?);
            Ok(())
        }));
        assert!(bob.as_mut().now_or_never().is_none());
        drop(alice);
        drop(bob);
        drop(owner);
        resume.send(()).unwrap();
        pool.run_until(completed).unwrap();
        assert_eq!(pool.run_until(observed).unwrap(), vec!["bob"]);
    }
    /// Alice returns while a failed/incomplete boundary leaves SQL open. The
    /// owner refuses Bob rather than allowing him to inherit her transaction.
    /// Internal fault injection is needed to create this invalid owner state.
    #[test]
    fn an_unfinished_sql_transaction_stops_the_owner() {
        let mut pool = LocalPool::new();
        let owner = DbOwner::spawn(pool.run_until(database(71)), 2, &pool.spawner()).unwrap();
        let mut alice = Box::pin(owner.call(async |db: &Db| {
            db.inner()
                .await
                .conn
                .execute_batch("SAVEPOINT incomplete")
                .unwrap();
            Ok(())
        }));
        assert!(alice.as_mut().now_or_never().is_none());
        let ran = Rc::new(Cell::new(false));
        let observed = ran.clone();
        let mut bob = Box::pin(owner.call(async move |_db: &Db| {
            observed.set(true);
            Ok(())
        }));
        assert!(bob.as_mut().now_or_never().is_none());
        assert!(
            pool.run_until(alice)
                .unwrap_err()
                .message
                .contains("open SQL transaction")
        );
        assert!(pool.run_until(bob).is_err());
        assert!(!ran.get());
        assert!(pool.run_until(owner.close()).is_err());
    }
    /// Alice and Bob own their databases on independent lanes. Finite sync
    /// ticks run through each queue, and Bob reads Alice's committed row.
    /// Internal to exercise both full databases with suspending storage.
    #[test]
    fn owned_databases_sync_through_queued_operations() {
        let mut pool = LocalPool::new();
        let alice = DbOwner::spawn(pool.run_until(database(71)), 2, &pool.spawner()).unwrap();
        let bob = DbOwner::spawn(pool.run_until(database(72)), 2, &pool.spawner()).unwrap();
        pool.run_until(bob.call(async |db: &Db| {
            db.set_authority().await;
            Ok(())
        }))
        .unwrap();
        let (up, down) = super::super::sync::duplex();
        let _upstream = pool
            .run_until(alice.call(async move |db: &Db| Ok(db.connect_upstream(up).await)))
            .unwrap();
        pool.run_until(
            bob.call(async move |db: &Db| db.accept_subscriber(down, AuthorSubject::SYSTEM).await),
        )
        .unwrap();
        pool.run_until(alice.call(async |db: &Db| {
            db.insert("notes", cells("alice"), Default::default())
                .await
                .map(|_| ())
        }))
        .unwrap();
        for _ in 0..8 {
            pool.run_until(alice.call(async |db: &Db| db.tick().await))
                .unwrap();
            pool.run_until(bob.call(async |db: &Db| db.tick().await))
                .unwrap();
        }
        assert_eq!(
            pool.run_until(bob.call(async |db: &Db| bodies(db).await))
                .unwrap(),
            vec!["alice"]
        );
        pool.run_until(alice.close()).unwrap();
        pool.run_until(bob.close()).unwrap();
    }
    /// Alice stages several writes across owner calls. Metadata access and
    /// owner completion retain her cached overlay; Bob's ordinary read awaits
    /// rollback and sees none of her uncommitted rows. Alice can still commit.
    /// Internal inspection pins cache reuse, which row results alone cannot
    /// distinguish from replaying the whole batch on every stage.
    #[test]
    fn cached_transaction_overlay_survives_owner_calls_but_not_other_sql() {
        use crate::tools::OpenTransactionId;
        let mut pool = LocalPool::new();
        let owner = DbOwner::spawn(pool.run_until(database(71)), 2, &pool.spawner()).unwrap();
        let id = OpenTransactionId::new();
        pool.run_until(owner.call(async move |db: &Db| db.begin_mergeable(id).await))
            .unwrap();
        for n in 0..3 {
            pool.run_until(owner.call(async move |db: &Db| {
                if n > 0 {
                    assert_eq!(
                        db.inner.metadata().overlay.as_ref().map(|o| o.applied),
                        Some(n)
                    );
                    let _ = db.seq_high_water();
                    let _ = db.prepare_query(&Query::from("notes"))?;
                    assert_eq!(
                        db.inner.metadata().overlay.as_ref().map(|o| o.applied),
                        Some(n)
                    );
                }
                db.tx_ref(id)
                    .insert("notes", cells(&format!("alice {n}")), Default::default())
                    .await?;
                assert_eq!(
                    db.inner.metadata().overlay.as_ref().map(|o| o.applied),
                    Some(n + 1)
                );
                Ok(())
            }))
            .unwrap();
        }
        assert!(
            pool.run_until(owner.call(async |db: &Db| {
                let rows = bodies(db).await?;
                assert!(db.inner.metadata().overlay.is_none());
                Ok(rows)
            }))
            .unwrap()
            .is_empty()
        );
        pool.run_until(owner.call(async move |db: &Db| {
            db.tx_ref(id)
                .insert("notes", cells("alice 3"), Default::default())
                .await?;
            db.commit_mergeable_handle(id).await.map(|_| ())
        }))
        .unwrap();
        let mut rows = pool
            .run_until(owner.call(async |db: &Db| bodies(db).await))
            .unwrap();
        rows.sort();
        assert_eq!(rows, vec!["alice 0", "alice 1", "alice 2", "alice 3"]);
        pool.run_until(owner.close()).unwrap();
    }
}
