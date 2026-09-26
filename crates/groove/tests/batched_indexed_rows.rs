#![cfg(feature = "test")]
//! The page-count property is observed at PageStore while rows are checked
//! through a public indexed subscription. This catches forwarding adapters
//! silently falling back to individual point reads.
use futures::{executor::block_on, future::poll_fn};
use groove::{
    db::{Database, GraphBuilder},
    ivm::{LiteralValue, ProjectField, StaticScanSpec},
    records::{RecordDescriptor, Value, ValueType, VariantRecord},
    schema::{
        ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey,
        TableSchema,
    },
    storage::{BoxedStorage, IdbStorage},
};
use idb_tree::{BoxFuture, Commit, MemoryPageStore, Metadata, PageStore};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    future::Future,
    rc::Rc,
    task::Poll,
};
#[derive(Clone, Default)]
struct CountingStore {
    memory: MemoryPageStore,
    reads: Rc<Cell<usize>>,
    ids: Rc<RefCell<BTreeSet<u64>>>,
    turn: Rc<Cell<u64>>,
    delayed: Rc<Cell<bool>>,
}
impl CountingStore {
    // A page completion arrives on a later driver turn, just as an IndexedDB
    // callback does. Re-polling within the initiating turn cannot finish it.
    async fn drive<F: Future>(&self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        poll_fn(|cx| {
            let result = future.as_mut().poll(cx);
            self.turn.set(self.turn.get() + 1);
            result
        })
        .await
    }
}
impl PageStore for CountingStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.memory.load_metadata()
    }
    fn read_page(&self, id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.reads.set(self.reads.get() + 1);
        self.ids.borrow_mut().insert(id);
        Box::pin(async move {
            if self.delayed.get() {
                let started = self.turn.get();
                poll_fn(|cx| {
                    if self.turn.get() != started {
                        Poll::Ready(())
                    } else {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
            }
            self.memory.read_page(id).await
        })
    }
    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        self.memory.commit(commit)
    }
}
#[test]
fn indexed_subscriptions_batch_cold_pages_through_storage_adapters() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("group", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("items_by_group", ["group"]))
    .with_variant(1, ["group", "id"])]);
    let store = CountingStore::default();
    let storage = block_on(IdbStorage::open(store.clone(), &["items", "indices"])).unwrap();
    let mut database = block_on(Database::new(schema.clone(), storage.clone())).unwrap();
    let physical = RecordDescriptor::new([("group", ValueType::String), ("id", ValueType::U64)]);
    database
        .define_variant_projection(
            "items",
            "logical-item",
            RecordDescriptor::new([("id", ValueType::U64), ("group", ValueType::String)]),
        )
        .unwrap();
    database
        .register_variant_projection_case(
            "items",
            "logical-item",
            1,
            [ProjectField::named("id"), ProjectField::named("group")],
        )
        .unwrap();
    let mut batch = database.open_batch();
    for id in 0..1_000 {
        batch.insert(
            "items",
            VariantRecord::create(
                1,
                physical,
                &[Value::String("shared".into()), Value::U64(id)],
            )
            .unwrap(),
        );
    }
    block_on(database.commit_batch(batch)).unwrap();

    drop(database);
    drop(storage);
    let storage = block_on(IdbStorage::open(store.clone(), &["items", "indices"])).unwrap();
    let storage = BoxedStorage::new(storage);
    let mut database = block_on(Database::new(schema, storage)).unwrap();
    database
        .define_variant_projection(
            "items",
            "logical-item",
            RecordDescriptor::new([("id", ValueType::U64), ("group", ValueType::String)]),
        )
        .unwrap();
    database
        .register_variant_projection_case(
            "items",
            "logical-item",
            1,
            [ProjectField::named("id"), ProjectField::named("group")],
        )
        .unwrap();

    store.reads.set(0);
    store.ids.borrow_mut().clear();
    store.delayed.set(true);
    let subscription = block_on(store.drive(database.subscribe_one_sink(
        GraphBuilder::variant_index_scan(
            "items",
            "items_by_group",
            "logical-item",
            StaticScanSpec::Prefix(vec![LiteralValue::String("shared".into())]),
        ),
    )))
    .unwrap();
    let rows = block_on(store.drive(database.next_subscription(&subscription))).unwrap();
    assert_eq!(
        rows.to_values().unwrap(),
        (0..1_000)
            .map(|id| (vec![Value::U64(id), Value::String("shared".into())], 1))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        store.reads.get(),
        store.ids.borrow().len(),
        "indexed hydration must not read the same cold page repeatedly"
    );
    assert!(
        store.reads.get() < 100,
        "expected page-proportional reads for 1,000 rows, got {}",
        store.reads.get()
    );
}
