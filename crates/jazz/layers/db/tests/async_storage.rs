#[cfg(feature = "testing")]
use futures::FutureExt;
use futures::{executor::block_on, future::pending, poll};
use jazz_db::{options::Error, storage::Storage};
use std::task::Poll;

/// A storage-level test is needed to drop a transaction at a specific suspension point.
/// Alice writes, suspends before commit, and drops the future. Bob sees an empty table.
#[test]
fn dropping_a_pending_native_scope_rolls_back() {
    block_on(async {
        let db = Storage::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE notes(id INTEGER)")
            .await
            .unwrap();
        let mut writing = Box::pin(db.scope("write", async |tx| {
            tx.execute("INSERT INTO notes VALUES (1)", [])
                .await
                .unwrap();
            pending::<Result<(), Error>>().await
        }));
        assert!(matches!(poll!(writing.as_mut()), Poll::Pending));
        drop(writing);
        assert!(db.is_autocommit());
        assert_eq!(
            db.query_row("SELECT count(*) FROM notes", [], |row| row.get::<_, i64>(0))
                .await
                .unwrap(),
            0
        );
    });
}

/// A synchronous host can only decide commit before its callback returns.
/// Alice's unexpected suspension must undo the insert and leave Bob able to write.
#[cfg(feature = "testing")]
#[test]
fn a_synchronous_host_rolls_back_if_its_body_suspends() {
    block_on(async {
        let db = Storage::managed_emulation_in_memory().unwrap();
        db.execute_batch("CREATE TABLE notes(id INTEGER)")
            .await
            .unwrap();
        let result = db
            .scope("write", async |tx| {
                tx.execute("INSERT INTO notes VALUES (1)", [])
                    .await
                    .unwrap();
                pending::<Result<(), Error>>().await
            })
            .now_or_never()
            .expect("the host must decide synchronously");
        assert!(result.is_err());
        db.scope("again", async |tx| {
            tx.execute("INSERT INTO notes VALUES (2)", [])
                .await
                .unwrap();
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            db.query_row("SELECT sum(id) FROM notes", [], |row| row.get::<_, i64>(0))
                .await
                .unwrap(),
            2
        );
    });
}

/// A backend-level test distinguishes the connection handed to Alice's transaction
/// from the root connection. Both starting Bob's query and reading its row suspend.
#[test]
fn managed_transaction_uses_the_scoped_backend_across_suspension() {
    use jazz_db::storage::{Backend, Cursor, SqlResult, TransactionBody, Value};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    struct Counter {
        committed: Arc<AtomicI64>,
        transaction: Option<AtomicI64>,
    }

    struct CounterRow {
        value: Option<[Value; 1]>,
        returned: bool,
    }

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
        .await
    }

    #[async_trait::async_trait(?Send)]
    impl Cursor for CounterRow {
        async fn next(&mut self) -> SqlResult<Option<&[Value]>> {
            yield_once().await;
            if self.returned {
                return Ok(None);
            }
            self.returned = true;
            Ok(self.value.as_ref().map(|row| row.as_slice()))
        }

        fn columns(&self) -> &[String] {
            &[]
        }
    }

    #[async_trait::async_trait(?Send)]
    impl Backend for Counter {
        async fn execute(&self, sql: &str, params: &[Value]) -> SqlResult<usize> {
            assert_eq!(sql, "UPDATE counter SET value = ?1");
            let [Value::Integer(value)] = params else {
                panic!("expected integer")
            };
            yield_once().await;
            self.transaction
                .as_ref()
                .expect("write outside transaction")
                .store(*value, Ordering::SeqCst);
            Ok(1)
        }

        async fn query(&self, sql: &str, _: &[Value]) -> SqlResult<Box<dyn Cursor + '_>> {
            assert_eq!(sql, "SELECT value FROM counter");
            yield_once().await;
            let value = self
                .transaction
                .as_ref()
                .unwrap_or(&self.committed)
                .load(Ordering::SeqCst);
            Ok(Box::new(CounterRow {
                value: Some([Value::Integer(value)]),
                returned: false,
            }))
        }

        async fn transaction(&self, body: TransactionBody<'_>) -> SqlResult<()> {
            let scoped = Arc::new(Self {
                committed: self.committed.clone(),
                transaction: Some(AtomicI64::new(self.committed.load(Ordering::SeqCst))),
            });
            if body(scoped.clone()).await {
                self.committed.store(
                    scoped.transaction.as_ref().unwrap().load(Ordering::SeqCst),
                    Ordering::SeqCst,
                );
            }
            Ok(())
        }

        async fn execute_script(&self, _: &str) -> SqlResult<()> {
            unreachable!()
        }
        async fn last_insert_rowid(&self) -> SqlResult<i64> {
            unreachable!()
        }
        async fn total_changes(&self) -> SqlResult<u64> {
            unreachable!()
        }
        fn max_parameters(&self) -> usize {
            1
        }
    }

    block_on(async {
        let committed = Arc::new(AtomicI64::new(41));
        let storage = Storage::from_backend(Counter {
            committed: committed.clone(),
            transaction: None,
        });
        let operation = storage.scope("increment", async |tx| {
            let old: i64 = tx
                .query_row("SELECT value FROM counter", [], |r| r.get(0))
                .await
                .unwrap();
            tx.execute("UPDATE counter SET value = ?1", [old + 1])
                .await
                .unwrap();
            Ok(())
        });
        let mut operation = std::pin::pin!(operation);
        assert!(poll!(operation.as_mut()).is_pending());
        assert_eq!(committed.load(Ordering::SeqCst), 41);
        operation.await.unwrap();
        assert_eq!(committed.load(Ordering::SeqCst), 42);
    });
}
