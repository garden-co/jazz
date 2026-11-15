//! Savepoint cancellation over an asynchronous worker (#5342 review findings
//! 2 and 3). Native SQL never suspends, so these run the savepoint machinery
//! over [`SuspendingWorker`], which executes each admitted command and can
//! hold its reply, as a real worker does when the waiter is dropped.
use crate::host::DbConnection;
use crate::names::Names;
use crate::test_worker::SuspendingWorker;
use crate::values::{begin_savepoint, end_savepoint, in_savepoint_async};
use jazz_sql::Sql as _;
use std::future::Future;
use std::task::{Context, Waker};

fn block_on<T>(future: impl Future<Output = T>) -> T {
    crate::options::block_on(future)
}

fn poll_pending<T>(future: std::pin::Pin<&mut impl Future<Output = T>>) {
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        future.poll(&mut cx).is_pending(),
        "expected the operation to suspend"
    );
}

fn worker_with_notes() -> (SuspendingWorker, DbConnection) {
    let worker = SuspendingWorker::memory();
    let conn = DbConnection::worker(worker.clone());
    block_on(conn.sql().execute_script("CREATE TABLE notes (v TEXT)")).unwrap();
    (worker, conn)
}

async fn note(conn: &DbConnection, value: &str) -> Result<(), crate::options::Error> {
    conn.sql()
        .execute("INSERT INTO notes VALUES (?1)", jazz_sql::params![value])
        .await
        .map(|_| ())
        .map_err(crate::values::provider_error)
}

/// The worker's commands from the first one equal to `from` on.
fn log_from(worker: &SuspendingWorker, from: &str) -> Vec<String> {
    let log = worker.log();
    let start = log.iter().position(|sql| sql == from).unwrap_or(0);
    log[start..].to_vec()
}

fn notes(worker: &SuspendingWorker) -> Vec<String> {
    worker.with_conn(|conn| {
        conn.prepare("SELECT v FROM notes ORDER BY rowid")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    })
}

/// Finding 2a (values.rs:285-310). Alice's write is dropped after the worker
/// accepted her `SAVEPOINT` but before `in_savepoint_async` armed its guard.
/// Bob's next write must not run inside Alice's leaked savepoint: after it,
/// the worker is back in autocommit and Bob's row is committed.
///
/// alice: SAVEPOINT (accepted, reply held) -> drop
/// bob:   write -> commit -> autocommit
#[test]
fn a_savepoint_accepted_before_its_waiter_was_dropped_does_not_leak() {
    let (worker, conn) = worker_with_notes();
    worker.hold(|sql| sql == "SAVEPOINT alice");
    let mut alice = Box::pin(in_savepoint_async(&conn, "alice", async || {
        note(&conn, "alice").await
    }));
    poll_pending(alice.as_mut());
    assert!(worker.holding(), "the worker holds Alice's SAVEPOINT reply");
    drop(alice);
    worker.release();

    block_on(in_savepoint_async(&conn, "bob", async || {
        note(&conn, "bob").await
    }))
    .unwrap();
    assert!(
        !worker.in_transaction(),
        "Alice's dropped SAVEPOINT leaked: Bob's write ran inside it and was never committed \
         (worker log: {:?})",
        worker.log()
    );
    assert_eq!(notes(&worker), ["bob"]);
}

/// Finding 2b (values.rs:285-310). Alice's write is dropped after the worker
/// accepted her `RELEASE`: her transaction, including an enum tag interned in
/// it, is committed. The next operation must not queue a `ROLLBACK TO` for
/// it, and the in-memory tag cache must keep the committed tag.
///
/// alice: SAVEPOINT -> intern "committed" -> RELEASE (accepted, reply held) -> drop
/// bob:   next write settles cleanup -> tag "committed" still cached
#[test]
fn a_savepoint_released_before_its_waiter_was_dropped_stays_committed() {
    let worker = SuspendingWorker::memory();
    let conn = DbConnection::worker(worker.clone());
    let names = block_on(Names::load(&mut conn.sql())).unwrap();
    let tag = |name: &str| names.tags().column("t", "c").borrow().tag(name);

    worker.hold(|sql| sql == "RELEASE alice");
    let mut alice = Box::pin(in_savepoint_async(&conn, "alice", async || {
        names.intern(&conn, "t", "c", &["committed"]).await
    }));
    poll_pending(alice.as_mut());
    assert!(worker.holding(), "the worker holds Alice's RELEASE reply");
    drop(alice);
    worker.release();
    // The worker ran the RELEASE: the commit landed.
    assert!(!worker.in_transaction());
    let stored: Vec<String> = worker.with_conn(|conn| {
        conn.prepare("SELECT name FROM _jazz_enum_tags")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    });
    assert_eq!(stored, ["committed"]);

    // Bob's write is the next operation, so it settles Alice's cleanup.
    block_on(in_savepoint_async(&conn, "bob", async || Ok(()))).unwrap();
    assert_eq!(
        tag("committed"),
        Some(0),
        "the committed tag was rolled back in memory though it is in the file \
         (worker log: {:?})",
        log_from(&worker, "SAVEPOINT alice")
    );
    assert!(
        !worker
            .log()
            .iter()
            .any(|sql| sql.starts_with("ROLLBACK TO alice")),
        "a ROLLBACK TO was queued for a released savepoint: {:?}",
        log_from(&worker, "SAVEPOINT alice")
    );
}

/// Finding 3 (backend.rs:220-238). Alice's nested savepoints `outer` and
/// `inner` are cancelled inside an open `tx`. Cleanup must undo `inner`
/// before `outer`, leaving `tx` usable: it commits with only its own row.
///
/// tx: write "kept" -> outer: write -> inner: write -> Pending -> drop
///     -> cleanup inner, outer -> RELEASE tx -> ["kept"]
#[test]
fn nested_cancelled_savepoints_are_cleaned_up_inner_first() {
    let (worker, conn) = worker_with_notes();
    block_on(begin_savepoint(&conn, "tx")).unwrap();
    block_on(note(&conn, "kept")).unwrap();
    let mut nested = Box::pin(in_savepoint_async(&conn, "outer", async || {
        note(&conn, "outer").await?;
        in_savepoint_async(&conn, "inner", async || {
            note(&conn, "inner").await?;
            std::future::pending::<()>().await;
            Ok(())
        })
        .await
    }));
    poll_pending(nested.as_mut());
    drop(nested);

    let released = block_on(end_savepoint(&conn, "tx", true));
    assert!(
        released.is_ok(),
        "cleaning up the cancelled savepoints poisoned the connection: {released:?} \
         (worker log: {:?})",
        worker.log()
    );
    assert!(!worker.in_transaction());
    assert_eq!(notes(&worker), ["kept"]);
}
