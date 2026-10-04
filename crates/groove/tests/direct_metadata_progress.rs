//! Exercise the public Groove seam: JazzClient does not expose the point at
//! which an IVM-owned chunk journal write holds the storage mutation gate.
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, poll_fn};
use std::rc::Rc;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};

use bytes::Bytes;
use futures::executor::block_on;
use groove::chunks::{
    ChunkError, ChunkFuture, ChunkRequest, MemoryChunkStorage, MissingChunkResolver,
};
use groove::db::{Database, DirectRecordStoreWrite, GraphBuilder, LARGE_VALUE_METADATA_CF};
use groove::ivm::MultisinkSubscription;
use groove::large_values::{LargeValueKind, prepare};
use groove::records::{RecordDescriptor, Value, ValueType};
use groove::schema::{DatabaseSchema, DirectRecordStoreSchema};
use groove::storage::IdbStorage;
use idb_tree::{BoxFuture, Commit, MemoryPageStore, Metadata, PageStore};

#[derive(Default)]
struct Pause {
    started: Cell<bool>,
    released: Cell<bool>,
    wake: RefCell<Option<Waker>>,
}
impl Pause {
    async fn wait(&self) {
        self.started.set(true);
        poll_fn(|cx| {
            if self.released.get() {
                Poll::Ready(())
            } else {
                *self.wake.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }
    fn release(&self) {
        self.released.set(true);
        self.wake
            .borrow_mut()
            .take()
            .expect("pause was polled")
            .wake();
    }
}

#[derive(Clone, Default)]
struct ControlledStore {
    inner: MemoryPageStore,
    pause_next: Rc<Cell<bool>>,
    commit_ack: Rc<Pause>,
}
impl PageStore for ControlledStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.inner.load_metadata()
    }
    fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.inner.read_page(page_id)
    }
    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        Box::pin(async move {
            let metadata = self.inner.commit(commit).await?;
            if self.pause_next.replace(false) {
                self.commit_ack.wait().await;
            }
            Ok(metadata)
        })
    }
}

struct Resolver {
    chunks: BTreeMap<ChunkRequest, Bytes>,
    blocked: BTreeSet<ChunkRequest>,
    pause: Rc<Pause>,
}
impl MissingChunkResolver for Resolver {
    fn resolve(&self, request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
        Box::pin(async move {
            if self.blocked.contains(&request) {
                self.pause.wait().await;
            }
            self.chunks
                .get(&request)
                .cloned()
                .ok_or(ChunkError::Unavailable)
        })
    }
}

fn fixture() -> (Database, ControlledStore, [GraphBuilder; 2], Rc<Pause>) {
    let schema = DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
        "ledger",
        RecordDescriptor::new([("id", ValueType::U64)]),
        RecordDescriptor::new([("accepted", ValueType::Bool)]),
    ));
    let page_store = ControlledStore::default();
    let storage = block_on(IdbStorage::open(
        page_store.clone(),
        &["ledger", LARGE_VALUE_METADATA_CF],
    ))
    .unwrap();
    let mut database = block_on(Database::new(schema, storage)).unwrap();
    let pause = Rc::new(Pause::default());
    let mut resolver = Resolver {
        chunks: BTreeMap::new(),
        blocked: BTreeSet::new(),
        pause: pause.clone(),
    };
    let graphs = [0x73, 0x74].map(|fill| {
        let prepared = prepare(LargeValueKind::Bytes, &vec![fill; 128 * 1024]).unwrap();
        for chunk in prepared.staged_chunks {
            let request = ChunkRequest {
                object_hash: chunk.node_ref.object_hash.0,
                locator: chunk.node_ref.locator,
            };
            if fill == 0x74 {
                resolver.blocked.insert(request.clone());
            }
            resolver.chunks.insert(request, Bytes::from(chunk.encoded));
        }
        GraphBuilder::values(
            RecordDescriptor::new([("contents", ValueType::Bytes)]),
            [vec![Value::Large(Box::new(prepared.value_ref))]],
        )
        .unwrap()
        .project(["contents"])
    });
    database.set_chunk_storage(Rc::new(MemoryChunkStorage::new()));
    database.set_missing_chunk_resolver(Rc::new(resolver));
    page_store.pause_next.set(true);
    (database, page_store, graphs, pause)
}

fn operations() -> [DirectRecordStoreWrite; 1] {
    [DirectRecordStoreWrite::Set {
        key: vec![Value::U64(1)],
        value: vec![Value::Bool(true)],
    }]
}

// All underlying I/O is ready or explicitly controlled by this fixture. Bound
// executor turns so a regression fails deterministically instead of hanging.
fn finish<T>(future: impl Future<Output = T>) -> T {
    let mut future = Box::pin(future);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..128 {
        if let Poll::Ready(result) = future.as_mut().poll(&mut cx) {
            return result;
        }
    }
    panic!("metadata/query work stalled despite ready I/O");
}

fn assert_contents(database: &mut Database, subscription: &MultisinkSubscription, fill: u8) {
    let result = finish(database.next_multisink_subscription(subscription)).unwrap();
    assert_eq!(
        result.sinks["rows"].to_values().unwrap(),
        vec![(vec![Value::Bytes(vec![fill; 128 * 1024])], 1)]
    );
}

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn direct_metadata_write_advances_the_chunk_install_holding_its_storage_gate() {
    let (mut database, page_store, [first, _], _) = fixture();
    let subscription = database.subscribe([("rows", first)]).unwrap();
    assert!(
        page_store.commit_ack.started.get(),
        "hydration must start the real chunk install journal write"
    );
    assert!(subscription.try_recv().is_err());
    page_store.commit_ack.release();
    // Its I/O is ready, but the retained hydration has not yet been polled to
    // finish the write and release IdbStorage's mutation gate.
    finish(database.write_direct_records_with_progress("ledger", &operations(), None)).unwrap();
    let store = database.direct_record_store("ledger").unwrap();
    assert_eq!(
        block_on(store.get(&[Value::U64(1)]))
            .unwrap()
            .unwrap()
            .get("accepted")
            .unwrap(),
        Value::Bool(true)
    );
    assert_contents(&mut database, &subscription, 0x73);
}

#[test]
fn metadata_completion_leaves_unrelated_cold_query_on_its_durable_owner_waker() {
    let (mut database, page_store, [first, second], resolver_pause) = fixture();
    let first = database.subscribe([("rows", first)]).unwrap();
    let second = database.subscribe([("rows", second)]).unwrap();
    assert!(resolver_pause.started.get());
    page_store.commit_ack.release();
    let owner = Arc::new(WakeCount::default());
    let owner_waker = Waker::from(owner.clone());
    finish(database.write_direct_records_with_progress(
        "ledger",
        &operations(),
        Some(&owner_waker),
    ))
    .unwrap();
    // A later externally-driven turn must not replace the retained owner.
    finish(database.drive_ready_progress()).unwrap();
    assert!(
        second.try_recv().is_err(),
        "metadata must complete without awaiting unrelated chunks"
    );
    let wakes_before = owner.0.load(Ordering::Relaxed);
    resolver_pause.release();
    assert!(
        owner.0.load(Ordering::Relaxed) > wakes_before,
        "the durable owner must still wake after the metadata operation returns"
    );
    assert_contents(&mut database, &first, 0x73);
    assert_contents(&mut database, &second, 0x74);
}

#[test]
fn cancelling_metadata_wait_preserves_chunk_progress_and_does_not_write_the_ledger() {
    let (mut database, page_store, [first, _], _) = fixture();
    let subscription = database.subscribe([("rows", first)]).unwrap();
    let owner = Arc::new(WakeCount::default());
    let owner_waker = Waker::from(owner.clone());
    let operations = operations();
    let mut write = Box::pin(database.write_direct_records_with_progress(
        "ledger",
        &operations,
        Some(&owner_waker),
    ));
    assert!(
        write
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(write);
    let wakes_before = owner.0.load(Ordering::Relaxed);
    page_store.commit_ack.release();
    assert!(
        owner.0.load(Ordering::Relaxed) > wakes_before,
        "cancelling a gate waiter must not strand its owner"
    );
    assert_contents(&mut database, &subscription, 0x73);
    let store = database.direct_record_store("ledger").unwrap();
    assert!(block_on(store.get(&[Value::U64(1)])).unwrap().is_none());
}
