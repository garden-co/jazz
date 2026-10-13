//! Spilling join arrangements to scratch storage under memory pressure must
//! never change query results: the same writes produce the same deltas
//! whether arrangements stay in memory or move to scratch runs between
//! ticks, including for subscriptions that hydrate against an arrangement
//! that is already spilled.

use std::collections::BTreeMap;
use std::rc::Rc;

use groove::db::{Database, GraphBuilder, PrimaryKeyValue, Subscription};
use groove::ivm::MemorySpillStorage;
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "artists",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("label", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

async fn open_db() -> Database {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    Database::new(schema(), storage).await.unwrap()
}

fn queries() -> Vec<GraphBuilder> {
    let albums = || GraphBuilder::table("albums");
    let artists = || GraphBuilder::table("artists");
    vec![
        GraphBuilder::join(albums(), artists(), ["artist_id"], ["id"]),
        GraphBuilder::semi_join(albums(), artists(), ["artist_id"], ["id"]),
        GraphBuilder::anti_join(albums(), artists(), ["artist_id"], ["id"]),
    ]
}

fn drain(sub: &Subscription, into: &mut BTreeMap<String, i64>) {
    while let Ok(deltas) = sub.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            *into.entry(format!("{values:?}")).or_default() += weight;
        }
    }
    into.retain(|_, weight| *weight != 0);
}

struct Copy {
    db: Database,
    subs: Vec<Subscription>,
    views: Vec<BTreeMap<String, i64>>,
}

impl Copy {
    async fn open(spill: Option<Rc<MemorySpillStorage>>) -> Self {
        let mut db = open_db().await;
        if let Some(storage) = spill {
            db.set_arrangement_spill_storage(storage);
        }
        let mut subs = Vec::new();
        for query in queries() {
            subs.push(db.subscribe_one_sink(query).await.unwrap());
        }
        let views = vec![BTreeMap::new(); subs.len()];
        Self { db, subs, views }
    }

    async fn subscribe(&mut self, query: GraphBuilder) {
        self.subs
            .push(self.db.subscribe_one_sink(query).await.unwrap());
        self.views.push(BTreeMap::new());
    }

    async fn apply(&mut self, ops: &[Op]) {
        let mut batch = self.db.open_batch();
        for op in ops {
            match *op {
                Op::Album(id, artist) => {
                    batch.update("albums", vec![Value::U64(id), Value::U64(artist)])
                }
                Op::Artist(id, label) => {
                    batch.update("artists", vec![Value::U64(id), Value::U64(label)])
                }
                Op::DeleteAlbum(id) => batch.delete("albums", PrimaryKeyValue::U64(id)),
                Op::DeleteArtist(id) => batch.delete("artists", PrimaryKeyValue::U64(id)),
            }
        }
        let applied = self.db.apply_batch(batch).await.unwrap();
        let persisted = applied.persist().await;
        self.db.finish_persistence(persisted).unwrap();
        self.drain();
    }

    fn drain(&mut self) {
        for (sub, view) in self.subs.iter().zip(&mut self.views) {
            drain(sub, view);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Album(u64, u64),
    Artist(u64, u64),
    DeleteAlbum(u64),
    DeleteArtist(u64),
}

#[futures_test::test]
async fn spilled_arrangements_keep_join_results_identical() {
    let scratch = Rc::new(MemorySpillStorage::new());
    let mut memory = Copy::open(None).await;
    let mut spilled = Copy::open(Some(scratch.clone())).await;

    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = |bound: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % bound
    };
    let mut albums = BTreeMap::<u64, u64>::new();
    let mut artists = BTreeMap::<u64, u64>::new();

    for round in 0..40 {
        let mut ops = Vec::new();
        for _ in 0..1 + next(8) {
            let op = match next(4) {
                0 | 1 => {
                    let id = next(40);
                    if albums.contains_key(&id) && next(3) == 0 {
                        albums.remove(&id);
                        Op::DeleteAlbum(id)
                    } else {
                        let artist = next(10);
                        albums.insert(id, artist);
                        Op::Album(id, artist)
                    }
                }
                _ => {
                    let id = next(10);
                    if artists.contains_key(&id) && next(2) == 0 {
                        artists.remove(&id);
                        Op::DeleteArtist(id)
                    } else {
                        let label = next(5);
                        artists.insert(id, label);
                        Op::Artist(id, label)
                    }
                }
            };
            ops.push(op);
        }
        memory.apply(&ops).await;
        spilled.apply(&ops).await;
        assert_eq!(memory.views, spilled.views, "round {round} diverged");

        if next(3) != 0 {
            let report = spilled.db.spill_arrangements(0).unwrap();
            assert_eq!(report.resident_bytes_after, 0, "round {round}: {report:?}");
        }

        // A late subscriber hydrates against arrangements already spilled.
        if round == 20 {
            let late = GraphBuilder::join(
                GraphBuilder::table("albums"),
                GraphBuilder::table("artists"),
                ["artist_id"],
                ["id"],
            );
            memory.subscribe(late.clone()).await;
            spilled.subscribe(late).await;
            memory.drain();
            spilled.drain();
            assert_eq!(memory.views, spilled.views, "late subscriber diverged");
        }
    }

    let stats = spilled.db.runtime_stats();
    assert!(stats.spilled_arrangements > 0, "{stats:?}");
    assert!(!scratch.is_empty());
}

#[futures_test::test]
async fn without_spill_storage_spilling_keeps_everything_resident() {
    let mut db = open_db().await;
    let _sub = db
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert("artists", vec![Value::U64(1), Value::U64(1)]);
    batch.insert("albums", vec![Value::U64(1), Value::U64(1)]);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    let report = db.spill_arrangements(0).unwrap();
    assert_eq!(report.spilled_arrangements, 0);
    assert_eq!(report.resident_bytes_after, report.resident_bytes_before);
    assert!(report.resident_bytes_after > 0);
}
