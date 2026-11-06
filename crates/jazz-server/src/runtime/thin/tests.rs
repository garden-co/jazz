use super::*;
use crate::runtime::config;
use jazz::chunk_store::{ChunkStore, FileChunkStore, NodeKey, StoreFuture};
use jazz::db::{FileOptions, block_on};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{ColumnType, SchemaBuilder, TableSchema};
use std::sync::Mutex;

struct DelayedReads {
    files: FileChunkStore,
    released: AtomicBool,
    wakes: Mutex<Vec<Waker>>,
}
impl ChunkStore for DelayedReads {
    fn put<'a>(&'a self, key: &'a NodeKey, bytes: &'a [u8]) -> StoreFuture<'a, ()> {
        self.files.put(key, bytes)
    }
    fn get<'a>(&'a self, key: &'a NodeKey) -> StoreFuture<'a, Option<bytes::Bytes>> {
        Box::pin(async move {
            futures::future::poll_fn(|cx| {
                self.wakes.lock().unwrap().push(cx.waker().clone());
                if self.released.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            self.files.get(key).await
        })
    }
    fn delete<'a>(&'a self, key: &'a NodeKey) -> StoreFuture<'a, ()> {
        self.files.delete(key)
    }
}

fn room(budget: &Arc<tokio::sync::Semaphore>) -> ThinIngress {
    ThinIngress {
        _session: Arc::clone(budget).try_acquire_owned().unwrap(),
        _server: None,
    }
}

/// Alice's offloaded read suspends while Bob and sync input keep progressing.
/// Her following request stays ordered; disconnect instead cancels the read,
/// releases queued/active ingress permits, and abandons her open transaction.
/// This lower-level test controls real chunk I/O and polls the host explicitly
/// so no timeout or native scheduler can conceal a blocking implementation.
#[test]
fn suspended_thin_reads_preserve_order_progress_and_cancellation() {
    for cancel in [false, true] {
        block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db.sqlite");
            let store = Arc::new(DelayedReads {
                files: FileChunkStore::new(dir.path().join("chunks")).unwrap(),
                released: AtomicBool::new(false),
                wakes: Mutex::default(),
            });
            let source = SchemaBuilder::new()
                .table(TableSchema::builder("docs").column("body", ColumnType::Bytea))
                .build();
            let schema = JazzSchema::new(&source).unwrap();
            let configuration = || {
                config(schema.clone())
                    .with_chunk_store(store.clone())
                    .with_host_chunk_io()
            };
            {
                let db = Db::open_at(configuration(), &path, FileOptions::default())
                    .await
                    .unwrap();
                db.set_authority();
                db.insert(
                    "docs",
                    BTreeMap::from([(
                        "body".to_owned(),
                        Value::Bytes(
                            (0..400 * 1024u32)
                                .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
                                .collect(),
                        ),
                    )]),
                    Default::default(),
                )
                .await
                .unwrap();
                let io = db.take_chunk_io().unwrap().expect("leaves to offload");
                db.finish_chunk_io(io.run(&*store).await).unwrap();
            }
            let db = Db::open_at(configuration(), &path, FileOptions::default())
                .await
                .unwrap();
            let mut owner = Owner::from_db(db, None, Waker::noop().clone()).unwrap();
            let alice = ServerSession(1);
            let bob = ServerSession(2);
            let (output, mut received) = tokio_mpsc::unbounded_channel();
            for id in [alice, bob] {
                owner
                    .open_thin_session(
                        id,
                        AuthorSubject::SYSTEM,
                        BTreeMap::new(),
                        None,
                        output.clone(),
                        Arc::default(),
                    )
                    .unwrap();
            }
            let budget = Arc::new(tokio::sync::Semaphore::new(10));
            let schema_request = || ThinRequest {
                id: 0,
                op: ThinOp::Schema {
                    schema: serde_json::to_vec(&source).unwrap(),
                },
            };
            owner.thin_request(alice, schema_request(), room(&budget));
            const TX: &str = "0190f3e4000070008000000000000009";
            owner.thin_request(
                alice,
                ThinRequest {
                    id: 1,
                    op: ThinOp::Begin {
                        transaction: TX.to_owned(),
                        kind: "mergeable".to_owned(),
                    },
                },
                room(&budget),
            );
            owner.thin_request(
                alice,
                ThinRequest {
                    id: 2,
                    op: ThinOp::Query {
                        query: postcard::to_allocvec(&jazz::query::Query::from("docs")).unwrap(),
                        options: Vec::new(),
                        transaction: None,
                    },
                },
                room(&budget),
            );
            assert!(
                owner.thin[&alice].task.is_some(),
                "the read awaits chunk I/O"
            );
            let fetch = owner
                .db
                .take_chunk_fetches()
                .unwrap()
                .expect("read requests offloaded leaves");
            let mut fetching = Box::pin(fetch.run(&*store));
            assert!(
                fetching
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            owner.thin_request(
                alice,
                ThinRequest {
                    id: 3,
                    op: ThinOp::CaptureSnapshot,
                },
                room(&budget),
            );
            assert_eq!(
                budget.available_permits(),
                8,
                "active and queued requests retain permits"
            );
            owner.thin_request(bob, schema_request(), room(&budget));
            assert!(
                owner.thin[&bob].task.is_none(),
                "Bob progresses while Alice waits"
            );

            let sync = ServerSession(3);
            owner
                .open_session(
                    sync,
                    AuthorSubject::SYSTEM,
                    BTreeMap::new(),
                    None,
                    output.clone(),
                    Arc::default(),
                )
                .unwrap();
            owner
                .input(sync)
                .unwrap()
                .receive(
                    jazz::db::encode_frames(jazz::db::SyncMessage::Unsubscribe { id: 1 }).unwrap(),
                )
                .unwrap();
            owner.advance().await;
            owner.flush();
            assert!(owner.sessions[&sync].queues.inbound.borrow().is_empty());
            let mut earlier = Vec::new();
            while let Ok((frames, _)) = received.try_recv() {
                for frame in frames.unwrap() {
                    if let WireFrame::ThinResponse(response) =
                        jazz::wire::decode_frame(&frame).unwrap()
                    {
                        assert!(matches!(response.body, ThinBody::Ok));
                        earlier.push(response.id);
                    }
                }
            }
            earlier.sort();
            assert_eq!(
                earlier,
                [0, 0, 1],
                "earlier replies drain even during a pending read"
            );
            if cancel {
                owner.close_thin_session(alice);
                assert_eq!(budget.available_permits(), 10);
                assert!(
                    owner
                        .db
                        .abandon_transaction_handle(open_transaction(TX).unwrap())
                        .is_err(),
                    "the transaction was already abandoned"
                );
            }
            store.released.store(true, Ordering::Release);
            for wake in store.wakes.lock().unwrap().drain(..) {
                wake.wake();
            }
            owner.db.finish_chunk_fetches(fetching.await).unwrap();
            owner.poll_thin();
            if !cancel {
                // Completing chunk I/O can require another engine pass before
                // the query resolves; the host remains free to provide it.
                for _ in 0..16 {
                    if owner.thin[&alice].task.is_none() {
                        break;
                    }
                    owner.advance().await;
                    if let Some(io) = owner.db.take_chunk_fetches().unwrap() {
                        owner
                            .db
                            .finish_chunk_fetches(io.run(&*store).await)
                            .unwrap();
                    }
                    owner.poll_thin();
                }
                assert!(owner.thin[&alice].task.is_none());
                let held = owner.thin[&alice].held.borrow();
                assert_eq!(
                    held.iter().map(|response| response.id).collect::<Vec<_>>(),
                    [2, 3]
                );
                assert!(matches!(held[0].body, ThinBody::Rows { .. }));
                assert!(matches!(held[1].body, ThinBody::Snapshot { .. }));
                assert_eq!(budget.available_permits(), 10);
            } else {
                owner.flush();
                assert!(
                    received.try_recv().is_err(),
                    "cancelled work emits no late answer"
                );
            }
            owner.close();
        });
    }
}
