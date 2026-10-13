//! Keyed lookup through a secondary index (`GraphBuilder::lookup` over a
//! row-projected `variant_index_scan`): table rows whose index key, after
//! the scan's static prefix, matches a key of another relation.
//!
//! Every maintained case is checked against an oracle computed from the
//! whole prefix scan, and the cost tests read the storage counter directly.

use std::collections::{BTreeMap, BTreeSet};

use groove::db::{Database, GraphBuilder, PrimaryKeyValue};
use groove::ivm::{LiteralValue, ProjectField, StaticScanSpec};
use groove::records::{RecordDescriptor, Value, ValueType, VariantRecord};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

/// `posts(id, room, chat, body)` is the probed table, indexed by
/// `(room, chat)` with a nullable `chat`; `room` is a static prefix.
/// `follows(id, chat)` supplies the keys, possibly duplicated.
fn schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "posts",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("room", ColumnType::U64),
                ColumnSchema::new("chat", ValueType::Nullable(Box::new(ValueType::U64))),
                ColumnSchema::new("body", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64).user_supplied())
        .with_index(IndexSchema::new("posts_by_room_chat", ["room", "chat"]))
        .with_variant(1, ["id", "room", "chat", "body"]),
        TableSchema::new(
            "follows",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("chat", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64).user_supplied()),
    ])
}

fn physical() -> RecordDescriptor {
    RecordDescriptor::new([
        ("id", ValueType::U64),
        ("room", ValueType::U64),
        ("chat", ValueType::Nullable(Box::new(ValueType::U64))),
        ("body", ValueType::String),
    ])
}

async fn open_db() -> Database {
    let schema = schema();
    let storage = MemoryStorage::new(&schema.column_families()).expect("valid families");
    let mut db = Database::new(schema, storage).await.unwrap();
    db.define_variant_projection("posts", "logical-post", physical())
        .unwrap();
    db.register_variant_projection_case(
        "posts",
        "logical-post",
        1,
        ["id", "room", "chat", "body"].map(ProjectField::named),
    )
    .unwrap();
    db
}

fn post(id: u64, room: u64, chat: Option<u64>, body: &str) -> VariantRecord {
    VariantRecord::create(
        1,
        physical(),
        &[
            Value::U64(id),
            Value::U64(room),
            Value::Nullable(chat.map(|chat| Box::new(Value::U64(chat)))),
            Value::String(body.into()),
        ],
    )
    .unwrap()
}

fn follow(id: u64, chat: u64) -> Vec<Value> {
    vec![Value::U64(id), Value::U64(chat)]
}

fn posts_of_room(room: u64) -> GraphBuilder {
    GraphBuilder::variant_index_scan(
        "posts",
        "posts_by_room_chat",
        "logical-post",
        StaticScanSpec::Prefix(vec![LiteralValue::from(Value::U64(room))]),
    )
}

fn followed_chats() -> GraphBuilder {
    GraphBuilder::table("follows").project(["chat"])
}

fn lookup() -> GraphBuilder {
    GraphBuilder::lookup(posts_of_room(1), followed_chats(), ["chat"])
        .expect("a row-projected index scan is a lookup source")
}

type Bag = BTreeMap<String, i64>;

/// One batch of writes in a scripted sequence.
type Edit = Box<dyn FnOnce(&mut groove::db::DatabaseBatch)>;

fn add(bag: &mut Bag, rows: Vec<(Vec<Value>, i64)>) {
    for (values, weight) in rows {
        *bag.entry(format!("{values:?}")).or_default() += weight;
    }
    bag.retain(|_, weight| *weight != 0);
}

/// The posts of room 1 whose chat is followed, from the whole prefix scan.
async fn oracle(db: &mut Database) -> Bag {
    let chats = db
        .query_graph(followed_chats())
        .await
        .unwrap()
        .to_values()
        .unwrap()
        .into_iter()
        .filter_map(|(values, _)| match values[0] {
            Value::U64(chat) => Some(chat),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let mut bag = Bag::new();
    let posts = db
        .query_graph(posts_of_room(1))
        .await
        .unwrap()
        .to_values()
        .unwrap();
    add(
        &mut bag,
        posts
            .into_iter()
            .filter(|(values, _)| match &values[2] {
                Value::Nullable(Some(chat)) => {
                    matches!(chat.as_ref(), Value::U64(chat) if chats.contains(chat))
                }
                _ => false,
            })
            .collect(),
    );
    bag
}

async fn snapshot(db: &mut Database, graph: GraphBuilder) -> Bag {
    let mut bag = Bag::new();
    add(
        &mut bag,
        db.query_graph(graph).await.unwrap().to_values().unwrap(),
    );
    bag
}

async fn apply(db: &mut Database, edit: impl FnOnce(&mut groove::db::DatabaseBatch)) {
    let mut batch = db.open_batch();
    edit(&mut batch);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
}

/// One-shot: the posts of followed chats in the prefix room, each once
/// however often its chat is followed; null chats and other rooms never.
#[futures_test::test]
async fn one_shot_index_lookup_reads_the_rows_of_its_keys() {
    let mut db = open_db().await;
    apply(&mut db, |batch| {
        batch.insert("follows", follow(1, 10));
        batch.insert("follows", follow(2, 10));
        batch.insert("follows", follow(3, 20));
        batch.insert("posts", post(1, 1, Some(10), "a"));
        batch.insert("posts", post(2, 1, Some(10), "b"));
        batch.insert("posts", post(3, 1, Some(20), "c"));
        batch.insert("posts", post(4, 1, Some(30), "unfollowed"));
        batch.insert("posts", post(5, 1, None, "no chat"));
        batch.insert("posts", post(6, 2, Some(10), "other room"));
    })
    .await;

    let found = snapshot(&mut db, lookup()).await;
    assert_eq!(found, oracle(&mut db).await);
    assert_eq!(found.len(), 3, "{found:?}");
    assert!(found.values().all(|weight| *weight == 1), "{found:?}");
}

/// A maintained index lookup follows both sides, including rows that move
/// between keys, into or out of the static prefix, or to a null key.
#[futures_test::test]
async fn maintained_index_lookup_follows_both_sides() {
    let mut db = open_db().await;
    apply(&mut db, |batch| {
        batch.insert("follows", follow(1, 10));
        batch.insert("posts", post(1, 1, Some(10), "a"));
        batch.insert("posts", post(2, 1, Some(20), "b"));
    })
    .await;
    let sub = db.subscribe_one_sink(lookup()).await.unwrap();
    let mut live = Bag::new();
    add(&mut live, sub.recv().unwrap().to_values().unwrap());
    assert_eq!(live, oracle(&mut db).await);

    let edits: Vec<Edit> = vec![
        // A row of a live key, and rows of other keys or rooms.
        Box::new(|b| b.insert("posts", post(3, 1, Some(10), "a2"))),
        Box::new(|b| {
            b.insert("posts", post(4, 1, Some(30), "x"));
            b.insert("posts", post(5, 2, Some(10), "y"));
        }),
        // Update in place, then move a row to an unfollowed chat and back.
        Box::new(|b| b.update("posts", post(1, 1, Some(10), "a-updated"))),
        Box::new(|b| b.update("posts", post(3, 1, Some(30), "a2 moved"))),
        Box::new(|b| b.update("posts", post(3, 1, Some(10), "a2 back"))),
        // A row loses its key, and leaves the static prefix.
        Box::new(|b| b.update("posts", post(3, 1, None, "a2 no chat"))),
        Box::new(|b| b.update("posts", post(1, 2, Some(10), "a other room"))),
        Box::new(|b| b.update("posts", post(1, 1, Some(10), "a home"))),
        // A key enters: its stored rows appear.
        Box::new(|b| b.insert("follows", follow(2, 20))),
        // A duplicate key, then one of the duplicates leaves.
        Box::new(|b| b.insert("follows", follow(3, 20))),
        Box::new(|b| b.delete("follows", PrimaryKeyValue::U64(2))),
        // A row moves between two live keys.
        Box::new(|b| b.update("posts", post(2, 1, Some(10), "b moved"))),
        // A key enters with a row moving into it in the same batch.
        Box::new(|b| {
            b.insert("follows", follow(4, 30));
            b.update("posts", post(2, 1, Some(30), "b to 30"));
        }),
        // A key leaves in the same batch as a change of its rows.
        Box::new(|b| {
            b.delete("follows", PrimaryKeyValue::U64(1));
            b.update("posts", post(1, 1, Some(10), "a while unfollowed"));
        }),
        // Delete a row and its key together.
        Box::new(|b| {
            b.delete("posts", PrimaryKeyValue::U64(4));
            b.delete("follows", PrimaryKeyValue::U64(4));
        }),
        Box::new(|b| b.insert("follows", follow(5, 10))),
    ];
    for (step, edit) in edits.into_iter().enumerate() {
        apply(&mut db, edit).await;
        while let Ok(deltas) = sub.try_recv() {
            add(&mut live, deltas.to_values().unwrap());
        }
        assert!(
            live.values().all(|weight| *weight == 1),
            "step {step}: {live:?}"
        );
        assert_eq!(live, oracle(&mut db).await, "step {step}");
    }
}

/// Hydration reads one index prefix per key: the same number of rows for a
/// probed table of 100 and of 2,000 rows. A maintained lookup reads nothing
/// for a write to an unrelated row, and only an entering key's own rows.
#[futures_test::test]
async fn index_lookup_storage_reads_scale_with_keys_not_table_size() {
    let mut reads = Vec::new();
    for rows in [100, 2_000] {
        let mut db = open_db().await;
        apply(&mut db, |batch| {
            for id in 1..=rows {
                batch.insert("posts", post(id, 1, Some(id), "p"));
            }
            for chat in 1..=3 {
                batch.insert("follows", follow(chat, chat));
            }
        })
        .await;
        db.reset_storage_read_metrics();
        let found = snapshot(&mut db, lookup()).await;
        assert_eq!(found.len(), 3);
        reads.push(db.take_storage_read_metrics().total.reads);
        if rows == 2_000 {
            let sub = db.subscribe_one_sink(lookup()).await.unwrap();
            let mut live = Bag::new();
            add(&mut live, sub.recv().unwrap().to_values().unwrap());
            db.take_storage_read_metrics();
            apply(&mut db, |b| {
                b.update("posts", post(1_500, 1, Some(1_500), "unrelated"))
            })
            .await;
            let unrelated = db.take_storage_read_metrics().total.reads;
            apply(&mut db, |b| b.insert("follows", follow(900, 900))).await;
            let entering = db.take_storage_read_metrics().total.reads;
            assert!(
                entering <= unrelated + 2,
                "an entering key reads its own rows: {entering} vs {unrelated}"
            );
            while let Ok(deltas) = sub.try_recv() {
                add(&mut live, deltas.to_values().unwrap());
            }
            assert_eq!(live, oracle(&mut db).await);
            assert_eq!(live.len(), 4);
        }
    }
    assert_eq!(
        reads[0], reads[1],
        "lookup reads grew with the table: {reads:?}"
    );
}

/// Only plain row-projected index scans are lookup sources, and the key
/// must have the index column's type or its non-null type.
#[futures_test::test]
async fn index_lookup_rejects_shapes_it_cannot_probe() {
    let mut db = open_db().await;
    let room = StaticScanSpec::Prefix(vec![LiteralValue::from(Value::U64(1))]);
    assert!(
        GraphBuilder::lookup(
            GraphBuilder::index_scan("posts", "posts_by_room_chat", room.clone()),
            followed_chats(),
            ["chat"],
        )
        .is_none(),
        "an encoded index-entry source has no rows to return"
    );
    assert!(
        GraphBuilder::lookup(
            GraphBuilder::variant_index_intersection_scan(
                "posts",
                "posts_by_room_chat",
                room.clone(),
                [("posts_by_room_chat".to_owned(), room)],
                "logical-post",
            ),
            followed_chats(),
            ["chat"],
        )
        .is_none(),
        "an intersection is not one probe per key"
    );
    let wrong_type = GraphBuilder::lookup(
        posts_of_room(1),
        GraphBuilder::table("follows")
            .project(["id"])
            .project_fields([ProjectField::renamed("id", "chat")]),
        ["chat"],
    )
    .unwrap();
    assert!(
        db.query_graph(wrong_type).await.is_ok(),
        "u64 keys a nullable u64 column"
    );
    let string_keys = GraphBuilder::lookup(
        posts_of_room(1),
        GraphBuilder::variant_index_scan(
            "posts",
            "posts_by_room_chat",
            "logical-post",
            StaticScanSpec::Prefix(vec![LiteralValue::from(Value::U64(1))]),
        )
        .project(["body"]),
        ["body"],
    )
    .unwrap();
    assert!(db.query_graph(string_keys).await.is_err());
    let too_long = GraphBuilder::lookup(
        posts_of_room(1),
        GraphBuilder::table("follows"),
        ["chat", "id"],
    )
    .unwrap();
    assert!(db.query_graph(too_long).await.is_err());
}
