mod support;

use jazz_rivet::{Store, Transaction};
use jazz_sql::{HostTable, Param, Sql, put_if_absent_in_transaction};
use std::sync::{Arc, atomic::Ordering};
use support::Pages;

const NOTES: HostTable = HostTable::new("async_probe");

/// Alice binds every SQLite scalar kind without coercing integers through
/// floating point or truncating text at NUL. This lower-level test is needed
/// because the host-table API only accepts BLOB keys and values.
#[tokio::test(flavor = "multi_thread")]
async fn scalar_parameters_preserve_sqlite_types() -> anyhow::Result<()> {
    let store = Store::open(
        Arc::new(Pages::default()),
        uuid::Uuid::new_v4().to_string(),
        1,
    )
    .await?;
    let mut tx = store.begin().await?;
    let result = tx
        .optional_blob(
            "SELECT ?6 WHERE ?1 IS NULL
             AND typeof(?2) = 'integer' AND ?2 = 9223372036854775807
             AND typeof(?3) = 'integer' AND ?3 = -9223372036854775808
             AND typeof(?4) = 'real' AND ?4 = 1.25
             AND typeof(?5) = 'text' AND hex(?5) = '6100C3A9'
             AND typeof(?6) = 'blob'",
            &[
                Param::Null,
                Param::Integer(i64::MAX),
                Param::Integer(i64::MIN),
                Param::Real(1.25),
                Param::Text("a\0é"),
                Param::Blob(&[0, 255]),
            ],
        )
        .await?;
    assert_eq!(result, Some(vec![0, 255]));
    tx.rollback().await?;
    store.close().await
}

// Force a suspension inside the shared read-dependent algorithm. The real
// SELECT executes first; cancellation must drop its still-owned transaction.
struct PauseAfterRead {
    tx: Transaction,
    entered: Arc<tokio::sync::Notify>,
}
impl Sql for PauseAfterRead {
    type Error = anyhow::Error;
    fn invalid_data(message: &'static str) -> Self::Error {
        anyhow::anyhow!(message)
    }
    type Row<'a> = <Transaction as Sql>::Row<'a>;
    async fn execute_script(&mut self, script: &str) -> anyhow::Result<()> {
        self.tx.execute_script(script).await
    }
    async fn query<F>(&mut self, sql: &str, params: &[Param<'_>], visit: F) -> anyhow::Result<()>
    where
        F: FnMut(Self::Row<'_>) -> anyhow::Result<std::ops::ControlFlow<()>>,
    {
        self.tx.query(sql, params, visit).await
    }

    async fn execute(&mut self, sql: &str, params: &[Param<'_>]) -> anyhow::Result<usize> {
        self.tx.execute(sql, params).await
    }
    async fn optional_blob(
        &mut self,
        sql: &str,
        params: &[Param<'_>],
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let result = self.tx.optional_blob(sql, params).await?;
        self.entered.notify_one();
        std::future::pending::<()>().await;
        Ok(result)
    }
}

/// Alice is cancelled between the shared operation's SELECT and INSERT. Bob
/// can then claim the key without observing a partially executed operation.
/// This lower-level timing fixture checks re-entry/cancellation in shared code.
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_inside_shared_algorithm_releases_transaction() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let store = Store::open(
            Arc::new(Pages::default()),
            uuid::Uuid::new_v4().to_string(),
            1,
        )
        .await?;
        let entered = Arc::new(tokio::sync::Notify::new());
        let writer = {
            let store = store.clone();
            let entered = entered.clone();
            tokio::spawn(async move {
                let mut sql = PauseAfterRead {
                    tx: store.begin().await.unwrap(),
                    entered,
                };
                put_if_absent_in_transaction(&mut sql, NOTES, b"key", b"alice")
                    .await
                    .unwrap();
                sql.tx.commit().await.unwrap();
            })
        };
        entered.notified().await;
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert_eq!(store.put_if_absent(NOTES, b"key", b"bob").await?, None);
        assert_eq!(store.get(NOTES, b"key").await?, Some(b"bob".to_vec()));
        store.close().await
    })
    .await?
}

/// Alice claims a key; Bob gets her prior value rather than overwriting it.
/// The real shared Jazz host-table algorithm runs over Rivet's unmodified
/// worker/VFS. This lower-level test isolates SQL suspension from actor routing.
/// alice -> conditional insert -> commit -> reopen -> bob reads large BLOB
#[tokio::test(flavor = "multi_thread")]
async fn conditional_insert_and_large_blob_survive_reopen() -> anyhow::Result<()> {
    let pages = Arc::new(Pages::default());
    let actor = uuid::Uuid::new_v4().to_string();
    let body = vec![0xa5; 128 * 1024];
    let store = Store::open(pages.clone(), actor.clone(), 1).await?;
    assert_eq!(store.get(NOTES, b"missing").await?, None);
    assert_eq!(store.put_if_absent(NOTES, b"", b"").await?, None);
    assert_eq!(store.get(NOTES, b"").await?, Some(Vec::new()));
    assert_eq!(store.put_if_absent(NOTES, b"alice", &body).await?, None);
    assert_eq!(
        store.put_if_absent(NOTES, b"alice", b"bob").await?,
        Some(body.clone())
    );
    let mut tx = store.begin().await?;
    put_if_absent_in_transaction(&mut tx, NOTES, b"rolled-back", b"no").await?;
    tx.rollback().await?;
    store.close().await?;
    let store = Store::open(pages, actor, 1).await?;
    assert_eq!(store.get(NOTES, b"alice").await?, Some(body));
    assert_eq!(store.get(NOTES, b"").await?, Some(Vec::new()));
    assert_eq!(store.get(NOTES, b"rolled-back").await?, None);
    store.close().await
}

/// Alice's task is cancelled after a write. Bob waits for cleanup, then sees
/// none of Alice's uncommitted data and can commit his own claim. This tests
/// the lease/rollback boundary directly because a higher layer masks timing.
/// alice -> write -> suspend -> cancel -> rollback -> bob acquires lease
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_rolls_back_before_next_operation() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let store = Store::open(
            Arc::new(Pages::default()),
            uuid::Uuid::new_v4().to_string(),
            1,
        )
        .await?;
        let entered = Arc::new(tokio::sync::Notify::new());
        let writer = {
            let store = store.clone();
            let entered = entered.clone();
            tokio::spawn(async move {
                let mut tx = store.begin().await.unwrap();
                put_if_absent_in_transaction(&mut tx, NOTES, b"key", b"alice")
                    .await
                    .unwrap();
                entered.notify_one();
                std::future::pending::<()>().await;
                tx.commit().await.unwrap();
            })
        };
        entered.notified().await;
        let bob = {
            let store = store.clone();
            tokio::spawn(async move { store.put_if_absent(NOTES, b"key", b"bob").await })
        };
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert_eq!(bob.await??, None);
        assert_eq!(store.get(NOTES, b"key").await?, Some(b"bob".to_vec()));
        store.close().await
    })
    .await?
}

/// Alice and Bob race for the same host key; exactly one inserts, and the
/// loser observes the winner. Serialized transactions must span every await.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_claims_are_atomic() -> anyhow::Result<()> {
    let store = Store::open(
        Arc::new(Pages::default()),
        uuid::Uuid::new_v4().to_string(),
        1,
    )
    .await?;
    let (alice, bob) = tokio::join!(
        store.put_if_absent(NOTES, b"key", b"alice"),
        store.put_if_absent(NOTES, b"key", b"bob"),
    );
    let (alice, bob) = (alice?, bob?);
    assert!(matches!((&alice, &bob), (None, Some(_)) | (Some(_), None)));
    let winner = if alice.is_none() {
        b"alice".to_vec()
    } else {
        b"bob".to_vec()
    };
    assert_eq!(alice.or(bob), Some(winner.clone()));
    assert_eq!(store.get(NOTES, b"key").await?, Some(winner));
    store.close().await
}

/// Alice receives no success for a refused or ambiguously committed write.
/// After cleanup/reopen Bob reads the authoritative page-service outcome.
/// This fixture tests failure semantics, not distributed WarpDrive replication.
#[tokio::test(flavor = "multi_thread")]
async fn failed_commit_preserves_authoritative_outcome() -> anyhow::Result<()> {
    for failure in [1, 2] {
        let pages = Arc::new(Pages::default());
        let actor = uuid::Uuid::new_v4().to_string();
        let store = Store::open(pages.clone(), actor.clone(), 1).await?;
        store.put_if_absent(NOTES, b"baseline", b"yes").await?;
        pages.fail_next.store(failure, Ordering::SeqCst);
        assert!(store.put_if_absent(NOTES, b"key", b"alice").await.is_err());
        if failure == 2 {
            assert!(
                store.get(NOTES, b"key").await.is_err(),
                "an ambiguous commit must fail closed until reopen"
            );
        }
        // Close waits for cancellation/error cleanup before freeing SQLite.
        store.close().await?;
        let store = Store::open(pages, actor, 1).await?;
        assert_eq!(store.get(NOTES, b"baseline").await?, Some(b"yes".to_vec()));
        assert_eq!(
            store.get(NOTES, b"key").await?,
            if failure == 1 {
                None
            } else {
                Some(b"alice".to_vec())
            }
        );
        store.close().await?;
    }
    Ok(())
}

/// Alice writes binary prefixes and all SQLite scalar types; Bob reads through
/// the same generic row visitor as native SQLite. This lower-level test checks
/// the public unmodified worker API, including its materialized query results.
#[tokio::test(flavor = "multi_thread")]
async fn shared_queries_and_binary_prefix_bounds() -> anyhow::Result<()> {
    use jazz_sql::{Cell, Row};
    use std::ops::ControlFlow;
    let store = Store::open(
        Arc::new(Pages::default()),
        uuid::Uuid::new_v4().to_string(),
        1,
    )
    .await?;
    let mut tx = store.begin().await?;
    let pairs: Vec<_> = [b"".as_slice(), &[0], &[0, 255], &[1], &[255], &[255, 255]]
        .into_iter()
        .map(|key| (key.to_vec(), key.to_vec()))
        .collect();
    jazz_sql::put_many_in_transaction(&mut tx, NOTES, &pairs).await?;
    for prefix in [
        b"".as_slice(),
        &[0],
        &[0, 255],
        &[1],
        &[255],
        &[255, 255],
        &[2],
    ] {
        let expected: Vec<_> = pairs
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .cloned()
            .collect();
        assert_eq!(jazz_sql::prefix(&mut tx, NOTES, prefix).await?, expected);
    }
    let mut seen = 0;
    tx.query(
        "SELECT ?1, ?2, ?3, ?4, ?5",
        &[
            Param::Null,
            Param::Integer(i64::MAX),
            Param::Real(1.25),
            Param::Text("a\0é"),
            Param::Blob(&[0, 255]),
        ],
        |row| {
            assert_eq!(row.len(), 5);
            assert_eq!(row.cell(0), Some(Cell::Null));
            assert_eq!(row.cell(1), Some(Cell::Integer(i64::MAX)));
            assert_eq!(row.cell(2), Some(Cell::Real(1.25)));
            assert_eq!(row.cell(3), Some(Cell::Text("a\0é".as_bytes())));
            assert_eq!(row.cell(4), Some(Cell::Blob(&[0, 255])));
            seen += 1;
            Ok(ControlFlow::Continue(()))
        },
    )
    .await?;
    assert_eq!(seen, 1);
    jazz_sql::delete(&mut tx, NOTES, &[0]).await?;
    tx.commit().await?;
    assert_eq!(store.get(NOTES, &[0]).await?, None);
    assert_eq!(store.get(NOTES, &[0, 255]).await?, Some(vec![0, 255]));
    store.close().await
}

/// Alice attempts a raw non-UTF-8 TEXT binding; Rivet rejects it before SQL
/// execution instead of replacing bytes. This low-level test covers a SQLite
/// representation not expressible through the normal Jazz string API.
#[tokio::test(flavor = "multi_thread")]
async fn unsupported_text_bytes_are_not_silently_changed() -> anyhow::Result<()> {
    let store = Store::open(
        Arc::new(Pages::default()),
        uuid::Uuid::new_v4().to_string(),
        1,
    )
    .await?;
    let mut tx = store.begin().await?;
    tx.execute("CREATE TABLE raw_text (value TEXT)", &[])
        .await?;
    assert!(
        tx.execute(
            "INSERT INTO raw_text VALUES (?1)",
            &[Param::TextBytes(&[255])]
        )
        .await
        .is_err()
    );
    assert_eq!(
        tx.optional_blob("SELECT X'01' FROM raw_text", &[]).await?,
        None
    );
    assert_eq!(
        tx.execute("INSERT INTO raw_text VALUES (?1)", &[Param::Text("alice")])
            .await?,
        1
    );
    assert_eq!(
        tx.execute(
            "DELETE FROM raw_text WHERE value = ?1",
            &[Param::Text("bob")]
        )
        .await?,
        0
    );
    tx.rollback().await?;
    store.close().await
}

/// Alice disconnects during a real worker/VFS commit. The actor finishes her
/// admitted write; even cancelling the shutdown waiter cannot abandon drain.
/// Bob reopens the durable store and sees her value. This lower-level test
/// pauses transport I/O because the full Jazz actor is not integrated yet.
/// alice -> enqueue -> commit pauses -> caller drops -> shutdown -> commit -> close
#[tokio::test(flavor = "multi_thread")]
async fn admitted_write_outlives_its_caller_and_shutdown_waiter() -> anyhow::Result<()> {
    use jazz_rivet::ActorStore;
    use std::{
        future::Future,
        task::{Context, Waker},
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let pages = Arc::new(Pages::default());
        let id = uuid::Uuid::new_v4().to_string();
        let actor = ActorStore::open(pages.clone(), id.clone(), 1, 1).await?;
        let pause = pages.pause_next_commit();
        let mut write = Box::pin(actor.put_if_absent(NOTES, b"key", b"alice"));
        assert!(
            write
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        pages.commit_entered.notified().await;
        drop(write);

        let mut shutdown = Box::pin(actor.close());
        assert!(
            shutdown
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(shutdown);
        assert!(actor.put_if_absent(NOTES, b"late", b"no").await.is_err());
        drop(pause);
        actor.close().await?;
        // Completion is shared and repeatable; a clone cannot reopen admission.
        actor.clone().close().await?;
        assert!(actor.get(NOTES, b"key").await.is_err());

        let reopened = Store::open(pages, id, 1).await?;
        assert_eq!(reopened.get(NOTES, b"key").await?, Some(b"alice".to_vec()));
        assert_eq!(reopened.get(NOTES, b"late").await?, None);
        // Retrying a lost reply uses the same conditional-write semantics.
        assert_eq!(
            reopened.put_if_absent(NOTES, b"key", b"bob").await?,
            Some(b"alice".to_vec())
        );
        reopened.close().await
    })
    .await?
}

/// Alice's first write holds commit. Bob's queued write is admitted even if
/// he stops waiting, while Mallory's cancelled capacity waiter admits nothing.
/// Shutdown drains both admitted writes and leaves the unadmitted key absent.
/// This low-level bounded-mailbox test uses the real SQLite worker, not fake SQL.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_abandoned_queued_writes_but_not_unadmitted_requests() -> anyhow::Result<()>
{
    use jazz_rivet::ActorStore;
    use std::{
        future::Future,
        task::{Context, Waker},
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let pages = Arc::new(Pages::default());
        let id = uuid::Uuid::new_v4().to_string();
        let actor = ActorStore::open(pages.clone(), id.clone(), 1, 1).await?;
        let pause = pages.pause_next_commit();
        let mut alice = Box::pin(actor.put_if_absent(NOTES, b"first", b"alice"));
        assert!(
            alice
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        pages.commit_entered.notified().await;

        // The owner is blocked and the one queue slot is empty. Bob's first
        // poll takes it and waits for a reply; Mallory must wait for capacity.
        let mut bob = Box::pin(actor.put_if_absent(NOTES, b"second", b"bob"));
        assert!(
            bob.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let mut mallory = Box::pin(actor.put_if_absent(NOTES, b"never", b"mallory"));
        assert!(
            mallory
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(bob);
        drop(mallory);
        let mut shutdown = Box::pin(actor.close());
        assert!(
            shutdown
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(actor.get(NOTES, b"first").await.is_err());
        drop(pause);
        assert_eq!(alice.await?, None);
        shutdown.await?;

        let reopened = Store::open(pages, id, 1).await?;
        assert_eq!(
            reopened.get(NOTES, b"first").await?,
            Some(b"alice".to_vec())
        );
        assert_eq!(reopened.get(NOTES, b"second").await?, Some(b"bob".to_vec()));
        assert_eq!(reopened.get(NOTES, b"never").await?, None);
        reopened.close().await
    })
    .await?
}

/// Alice's commit fails before persistence or loses its acknowledgement after
/// persistence. Actor-owned execution still returns an error, never success.
/// Bob's reopened store supplies the authoritative outcome. This lower-level
/// fault fixture exercises the actual worker/VFS commit boundary.
#[tokio::test(flavor = "multi_thread")]
async fn owned_execution_does_not_turn_commit_errors_into_success() -> anyhow::Result<()> {
    use jazz_rivet::ActorStore;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for failure in [1, 2] {
            let pages = Arc::new(Pages::default());
            let id = uuid::Uuid::new_v4().to_string();
            let actor = ActorStore::open(pages.clone(), id.clone(), 1, 2).await?;
            // Seed the table so the injected failure concerns the new value.
            actor.put_if_absent(NOTES, b"seed", b"existing").await?;
            pages.fail_next.store(failure, Ordering::SeqCst);
            assert!(actor.put_if_absent(NOTES, b"key", b"alice").await.is_err());
            actor.close().await?;
            let reopened = Store::open(pages, id, 1).await?;
            assert_eq!(
                reopened.get(NOTES, b"key").await?,
                (failure == 2).then(|| b"alice".to_vec())
            );
            reopened.close().await?;
        }
        anyhow::Ok(())
    })
    .await?
}

/// Alice creates a trigger through Rivet's async script API. Its body and
/// quoted semicolons survive parsing, and Bob sees no tables after rollback.
/// This lower-level test covers the provider operation used by Jazz schema DDL.
#[tokio::test(flavor = "multi_thread")]
async fn scripts_preserve_triggers_and_the_callers_transaction() -> anyhow::Result<()> {
    use jazz_sql::Row;
    let store = Store::open(
        Arc::new(Pages::default()),
        uuid::Uuid::new_v4().to_string(),
        1,
    )
    .await?;
    let mut tx = store.begin().await?;
    tx.execute_script(
        "CREATE TABLE notes (body TEXT); CREATE TABLE audit (body TEXT);
        CREATE TRIGGER note_added AFTER INSERT ON notes BEGIN
            INSERT INTO audit VALUES ('first;entry');
            INSERT INTO audit VALUES (NEW.body);
        END;
        INSERT INTO notes VALUES ('hello;world')",
    )
    .await?;
    assert_eq!(
        tx.query_rows("SELECT body FROM audit ORDER BY rowid", &[], |row| row
            .get::<String>(
            0
        ))
        .await?,
        ["first;entry", "hello;world"]
    );
    tx.rollback().await?;
    let mut tx = store.begin().await?;
    assert_eq!(
        tx.query_one(
            "SELECT count(*) FROM sqlite_master WHERE name IN ('notes', 'audit', 'note_added')",
            &[],
            |row| row.get::<i64>(0)
        )
        .await?,
        0
    );
    tx.rollback().await?;
    store.close().await
}

/// Alice's constraint violation causes SQLite to roll back the entire
/// transaction. Rivet publishes that state before returning the error; cleanup
/// must release the lease so Bob can write next. This exercises the actual
/// worker API, not the suspending native adapter used by the full Db tests.
#[tokio::test(flavor = "multi_thread")]
async fn automatic_rollback_publishes_state_and_allows_the_next_owner() -> anyhow::Result<()> {
    use jazz_sql::{Connection as _, Row as _};
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let store = Store::open(
            Arc::new(Pages::default()),
            uuid::Uuid::new_v4().to_string(),
            1,
        )
        .await?;
        let mut setup = store.begin().await?;
        setup
            .execute_script("CREATE TABLE notes (id INTEGER PRIMARY KEY)")
            .await?;
        setup.commit().await?;
        let mut alice = store.begin().await?;
        assert!(alice.transaction_open());
        assert!(alice.parameter_limit()? > 0);
        let before = alice.total_changes().await?;
        alice.execute("INSERT INTO notes VALUES (1)", &[]).await?;
        assert_eq!(alice.total_changes().await?, before + 1);
        assert!(
            alice
                .execute("INSERT OR ROLLBACK INTO notes VALUES (1)", &[])
                .await
                .is_err()
        );
        assert!(!alice.transaction_open());
        assert_eq!(alice.total_changes().await?, before + 1);
        // Drop still schedules ordered cleanup; an already-rolled-back
        // transaction must not poison the connection permanently.
        drop(alice);
        let mut bob = store.begin().await?;
        assert_eq!(
            bob.query_one::<i64, _>("SELECT count(*) FROM notes", &[], |r| r.get(0))
                .await?,
            0
        );
        bob.execute("INSERT INTO notes VALUES (2)", &[]).await?;
        bob.commit().await?;
        let mut check = store.begin().await?;
        assert_eq!(
            check
                .query_one::<i64, _>("SELECT id FROM notes", &[], |r| r.get(0))
                .await?,
            2
        );
        check.rollback().await?;
        store.close().await
    })
    .await?
}
