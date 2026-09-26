//! Whole public indexed-query receipt, including the storage adapter chain.
//! Page completions are delivered on a later executor turn, with no fake delay.
//! Cold and resident-page runs separate I/O fan-out from ordinary query overhead.
use futures::{executor::block_on, future::poll_fn};
use groove::{
    db::{Database, GraphBuilder},
    ivm::{LiteralValue, ProjectField, StaticScanSpec},
    records::{RecordDescriptor, Value, ValueType, VariantRecord},
    schema::{
        ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey,
        TableSchema,
    },
    storage::IdbStorage,
};
use idb_tree::{BoxFuture, Commit, MemoryPageStore, Metadata, PageStore};
use std::{cell::Cell, future::Future, rc::Rc, task::Poll, time::Instant};

#[derive(Clone, Default)]
struct Store {
    memory: MemoryPageStore,
    turn: Rc<Cell<u64>>,
    reads: Rc<Cell<usize>>,
    bytes: Rc<Cell<usize>>,
    delayed: Rc<Cell<bool>>,
}
impl Store {
    async fn drive<F: Future>(&self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        poll_fn(|cx| {
            let result = future.as_mut().poll(cx);
            self.turn.set(self.turn.get() + 1);
            result
        })
        .await
    }
    fn reset(&self) {
        self.reads.set(0);
        self.bytes.set(0);
    }
}
impl PageStore for Store {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.memory.load_metadata()
    }
    fn read_page(&self, id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.reads.set(self.reads.get() + 1);
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
            let result = self.memory.read_page(id).await?;
            self.bytes
                .set(self.bytes.get() + result.as_ref().map_or(0, Vec::len));
            Ok(result)
        })
    }
    fn read_pages<'a>(
        &'a self,
        ids: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, String>> {
        // Browser read_pages sends one IndexedDB request group concurrently.
        Box::pin(async move {
            futures::future::join_all(ids.iter().map(|id| self.read_page(*id)))
                .await
                .into_iter()
                .collect()
        })
    }
    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        self.memory.commit(commit)
    }
}
fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("group", ColumnType::String),
            ColumnSchema::new("payload", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("items_by_group", ["group"]))
    .with_variant(1, ["group", "id", "payload"])])
}
fn projection(database: &mut Database) {
    database
        .define_variant_projection(
            "items",
            "logical-item",
            RecordDescriptor::new([
                ("id", ValueType::U64),
                ("group", ValueType::String),
                ("payload", ValueType::String),
            ]),
        )
        .unwrap();
    database
        .register_variant_projection_case(
            "items",
            "logical-item",
            1,
            [
                ProjectField::named("id"),
                ProjectField::named("group"),
                ProjectField::named("payload"),
            ],
        )
        .unwrap();
}
fn milliseconds(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.
}
fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let samples = std::env::args()
        .nth(1)
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(3);
    for rows in [1_000_u64, 4_000] {
        for payload_bytes in [80, 1500] {
            let seeded = Instant::now();
            let store = Store::default();
            let storage = block_on(IdbStorage::open(store.clone(), &["items", "indices"])).unwrap();
            let mut database = block_on(Database::new(schema(), storage.clone())).unwrap();
            projection(&mut database);
            let descriptor = RecordDescriptor::new([
                ("group", ValueType::String),
                ("id", ValueType::U64),
                ("payload", ValueType::String),
            ]);
            let payload = "x".repeat(payload_bytes);
            let mut batch = database.open_batch();
            for id in 0..rows {
                batch.insert(
                    "items",
                    VariantRecord::create(
                        1,
                        descriptor,
                        &[
                            Value::String(format!("group{}", id % 4)),
                            Value::U64(id),
                            Value::String(payload.clone()),
                        ],
                    )
                    .unwrap(),
                );
            }
            block_on(database.commit_batch(batch)).unwrap();
            drop(database);
            drop(storage);
            let seed_ms = milliseconds(seeded);
            store.delayed.set(true);
            for subscriptions in [1_usize, 4] {
                for sample in 0..samples {
                    let reopened = Instant::now();
                    let storage = block_on(
                        store.drive(IdbStorage::open(store.clone(), &["items", "indices"])),
                    )
                    .unwrap();
                    let reopen_ms = milliseconds(reopened);
                    for resident in [false, true] {
                        let mut database =
                            block_on(store.drive(Database::new(schema(), storage.clone())))
                                .unwrap();
                        projection(&mut database);
                        store.reset();
                        let started = Instant::now();
                        let mut sinks = Vec::new();
                        for group in 0..subscriptions {
                            sinks.push(
                                block_on(store.drive(database.subscribe_one_sink(
                                    GraphBuilder::variant_index_scan(
                                        "items",
                                        "items_by_group",
                                        "logical-item",
                                        StaticScanSpec::Prefix(vec![LiteralValue::String(
                                            format!("group{group}"),
                                        )]),
                                    ),
                                )))
                                .unwrap(),
                            );
                        }
                        let attach_ms = milliseconds(started);
                        let hydrate_started = Instant::now();
                        block_on(store.drive(database.drive_progress())).unwrap();
                        let hydrate_ms = milliseconds(hydrate_started);
                        let query_ms = milliseconds(started);
                        let check_started = Instant::now();
                        let mut rows_match = true;
                        for (group, sink) in sinks.iter().enumerate() {
                            let actual = sink.recv().unwrap().to_values().unwrap();
                            rows_match &= actual.len() == rows as usize / 4;
                            rows_match &= actual.iter().enumerate().all(|(i, (value, weight))| {
                                *weight == 1
                                    && *value
                                        == vec![
                                            Value::U64(i as u64 * 4 + group as u64),
                                            Value::String(format!("group{group}")),
                                            Value::String(payload.clone()),
                                        ]
                            });
                        }
                        let check_ms = milliseconds(check_started);
                        println!(
                            "{{\"benchmark\":\"indexed_cold_pages\",\"rows\":{rows},\"payload_bytes\":{payload_bytes},\"subscriptions\":{subscriptions},\"sample\":{sample},\"resident\":{resident},\"seed_ms\":{seed_ms:.3},\"reopen_ms\":{reopen_ms:.3},\"attach_ms\":{attach_ms:.3},\"hydrate_ms\":{hydrate_ms:.3},\"query_ms\":{query_ms:.3},\"check_ms\":{check_ms:.3},\"page_reads\":{},\"page_bytes\":{},\"rows_match\":{rows_match}}}",
                            store.reads.get(),
                            store.bytes.get()
                        );
                        if !rows_match {
                            std::process::exit(1);
                        }
                    }
                }
            }
        }
    }
}
