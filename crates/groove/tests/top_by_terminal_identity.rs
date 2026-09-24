//! Terminal edits for a windowed `TopBy` must identify rows, not their first
//! output field. Here field 0 is the partition column `user`, shared by every
//! row of one feed (#3290). Root keys are the TopBy row identity (group plus
//! tie fields); root indices are positions within the row's own group.

use std::collections::BTreeMap;

use groove::db::{Database, GraphBuilder};
use groove::ivm::runtime::{RoutedMultisinkTerminal, TerminalEdit, TerminalOperation};
use groove::ivm::{TopByLimit, TopByOrder};
use groove::records::{BorrowedRecord, RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

const WINDOW: usize = 2;
const USERS: u64 = 3;

/// Ordered visible roots per group (`user`), each `(key, [user, id, created])`.
type Groups = BTreeMap<u64, Vec<(Vec<u8>, Vec<Value>)>>;

fn user_of(values: &[Value]) -> u64 {
    let Value::U64(user) = values[0] else {
        panic!("unexpected user {values:?}");
    };
    user
}

fn locate(groups: &Groups, key: &[u8]) -> (u64, usize) {
    groups
        .iter()
        .find_map(|(user, rows)| {
            rows.iter()
                .position(|(candidate, _)| candidate == key)
                .map(|index| (*user, index))
        })
        .unwrap_or_else(|| panic!("terminal key {key:?} is not visible"))
}

/// Apply edits sequentially, as a consumer would. An insert carries the root's
/// final index, clamped to the current length; trailing moves settle order.
fn apply(groups: &mut Groups, operations: &[TerminalOperation]) {
    for operation in operations {
        assert!(operation.path.is_empty());
        let decode = |bytes: &[u8]| {
            let record = BorrowedRecord::new(bytes, &operation.root_descriptor);
            (0..3)
                .map(|field| record.get_idx(field).unwrap())
                .collect::<Vec<_>>()
        };
        match &operation.edit {
            TerminalEdit::Insert { key, index, value } => {
                assert!(
                    !groups.values().flatten().any(|(k, _)| k == key),
                    "insert of an already visible terminal key {key:?}: {operations:?}"
                );
                let values = decode(value);
                let rows = groups.entry(user_of(&values)).or_default();
                rows.insert((*index).min(rows.len()), (key.clone(), values));
            }
            TerminalEdit::Remove { key } => {
                let (user, index) = locate(groups, key);
                groups.get_mut(&user).unwrap().remove(index);
            }
            TerminalEdit::Move { key, index } => {
                let (user, from) = locate(groups, key);
                let rows = groups.get_mut(&user).unwrap();
                let row = rows.remove(from);
                rows.insert((*index).min(rows.len()), row);
            }
            TerminalEdit::Update { key, value } => {
                let (user, index) = locate(groups, key);
                let values = decode(value);
                assert_eq!(user_of(&values), user, "an update cannot change the group");
                groups.get_mut(&user).unwrap()[index].1 = values;
            }
        }
    }
    groups.retain(|_, rows| !rows.is_empty());
}

/// Per user, the `WINDOW` newest posts (ties by id), in window order.
fn expected(oracle: &BTreeMap<u64, (u64, u64)>) -> BTreeMap<u64, Vec<Vec<Value>>> {
    let mut by_user = BTreeMap::<u64, Vec<(u64, u64)>>::new();
    for (id, (user, created)) in oracle {
        by_user.entry(*user).or_default().push((*created, *id));
    }
    by_user
        .into_iter()
        .map(|(user, mut posts)| {
            posts.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            let window = posts
                .into_iter()
                .take(WINDOW)
                .map(|(created, id)| vec![Value::U64(user), Value::U64(id), Value::U64(created)])
                .collect();
            (user, window)
        })
        .collect()
}

fn visible(groups: &Groups) -> BTreeMap<u64, Vec<Vec<Value>>> {
    groups
        .iter()
        .map(|(user, rows)| {
            (
                *user,
                rows.iter().map(|(_, values)| values.clone()).collect(),
            )
        })
        .collect()
}

async fn database() -> Database {
    let schema = DatabaseSchema::new([TableSchema::new(
        "posts",
        [
            ColumnSchema::new("user", ColumnType::U64),
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("created", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    Database::new(schema, MemoryStorage::new(&["posts"]).unwrap())
        .await
        .unwrap()
}

fn feed() -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("posts"),
        ["user"],
        [TopByOrder::desc("created")],
        ["id"],
        0,
        TopByLimit::Finite(WINDOW as u64),
    )
}

async fn commit(db: &mut Database, rows: impl IntoIterator<Item = (u64, u64, u64)>, update: bool) {
    let mut batch = db.open_batch();
    for (id, user, created) in rows {
        let values = vec![Value::U64(user), Value::U64(id), Value::U64(created)];
        if update {
            batch.update("posts", values);
        } else {
            batch.insert("posts", values);
        }
    }
    let persistence = db.apply_batch(batch).await.unwrap().persist().await;
    db.finish_persistence(persistence).unwrap();
}

/// Three users, three posts each: every window holds two rows sharing field 0.
const SEED: [(u64, u64, u64); 9] = [
    (1, 0, 10),
    (2, 0, 20),
    (3, 0, 30),
    (4, 1, 15),
    (5, 1, 25),
    (6, 1, 5),
    (7, 2, 40),
    (8, 2, 12),
    (9, 2, 22),
];

/// A deterministic post to bump: it becomes its user's newest.
fn bump(random: &mut u64) -> u64 {
    *random ^= *random << 13;
    *random ^= *random >> 7;
    *random ^= *random << 17;
    1 + *random % SEED.len() as u64
}

#[futures_test::test]
async fn partitioned_top_by_terminal_edits_track_rows_sharing_field_zero() {
    let mut db = database().await;
    let subscription = db.subscribe([("feed", feed())]).unwrap();
    subscription.try_recv().unwrap();
    let mut groups = Groups::new();
    let mut oracle = BTreeMap::new();
    commit(&mut db, SEED, false).await;
    for (id, user, created) in SEED {
        oracle.insert(id, (user, created));
    }
    apply(
        &mut groups,
        &subscription.try_recv().unwrap().terminal_sinks["feed"].operations,
    );
    assert_eq!(visible(&groups), expected(&oracle));

    let mut random = 0x3290_u64;
    for step in 0..60 {
        let id = bump(&mut random);
        let (user, _) = oracle[&id];
        commit(&mut db, [(id, user, 100 + step)], true).await;
        oracle.insert(id, (user, 100 + step));
        if let Ok(tick) = subscription.try_recv() {
            apply(&mut groups, &tick.terminal_sinks["feed"].operations);
        }
        assert_eq!(visible(&groups), expected(&oracle), "step {step}");
    }
}

/// The production shape: one prepared TopBy, a binding per user, each
/// subscription seeing exactly its own ordered window.
#[futures_test::test]
async fn routed_top_by_bindings_see_their_exact_ordered_window() {
    let mut db = database().await;
    let shape = db
        .prepare(
            [RoutedMultisinkTerminal::new(
                "feed",
                feed(),
                ["user"],
                ["user", "id", "created"],
            )],
            "user",
            RecordDescriptor::new([("user", ColumnType::U64)]),
        )
        .await
        .unwrap();
    let mut bindings = Vec::new();
    for user in 0..USERS {
        let subscription = db
            .bind_shape(shape.id(), &[Value::U64(user)])
            .await
            .unwrap();
        bindings.push((user, subscription, Groups::new()));
    }
    db.drive_progress().await.unwrap();
    for (_, subscription, _) in &bindings {
        while subscription.try_recv().is_ok() {}
    }
    // Every row arrives through terminal edits, so the model holds real keys.
    commit(&mut db, SEED, false).await;
    let mut oracle = BTreeMap::new();
    for (id, user, created) in SEED {
        oracle.insert(id, (user, created));
    }
    db.drive_progress().await.unwrap();
    let opening = expected(&oracle);
    for (user, subscription, groups) in &mut bindings {
        while let Ok(tick) = subscription.try_recv() {
            if let Some(terminal) = tick.terminal_sinks.get("feed") {
                apply(groups, &terminal.operations);
            }
        }
        assert_eq!(
            visible(groups).get(user),
            opening.get(user),
            "user {user} opening"
        );
    }

    let mut random = 0x3291_u64;
    for step in 0..60 {
        let id = bump(&mut random);
        let (user, _) = oracle[&id];
        commit(&mut db, [(id, user, 100 + step)], true).await;
        oracle.insert(id, (user, 100 + step));
        db.drive_progress().await.unwrap();
        let expected = expected(&oracle);
        for (user, subscription, groups) in &mut bindings {
            while let Ok(tick) = subscription.try_recv() {
                if let Some(terminal) = tick.terminal_sinks.get("feed") {
                    apply(groups, &terminal.operations);
                }
            }
            assert_eq!(
                visible(groups).get(user),
                expected.get(user),
                "user {user} at step {step}"
            );
            assert!(
                groups.keys().all(|group| group == user),
                "user {user}: {groups:?}"
            );
        }
    }
}
