//! Terminal edits for a windowed `TopBy` must identify rows, not their first
//! output field. Here field 0 is the partition column `user`, shared by every
//! row of one feed (#3290).

use std::collections::BTreeMap;

use groove::db::{Database, GraphBuilder};
use groove::ivm::runtime::{TerminalEdit, TerminalOperation};
use groove::ivm::{TopByLimit, TopByOrder};
use groove::records::{BorrowedRecord, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

type VisibleRows = Vec<(Vec<u8>, Vec<Value>)>;

const WINDOW: usize = 2;

fn apply(rows: &mut VisibleRows, operations: &[TerminalOperation]) {
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
                    !rows.iter().any(|(k, _)| k == key),
                    "insert of an already visible terminal key {key:?}: {operations:?}"
                );
                assert!(*index <= rows.len(), "insert index out of range");
                rows.insert(*index, (key.clone(), decode(value)));
            }
            TerminalEdit::Remove { key } => {
                let position = rows
                    .iter()
                    .position(|(k, _)| k == key)
                    .expect("remove of an invisible terminal key");
                rows.remove(position);
            }
            TerminalEdit::Move { key, index } => {
                let position = rows
                    .iter()
                    .position(|(k, _)| k == key)
                    .expect("move of an invisible terminal key");
                let row = rows.remove(position);
                rows.insert(*index, row);
            }
            TerminalEdit::Update { key, value } => {
                rows.iter_mut()
                    .find(|(k, _)| k == key)
                    .expect("update of an invisible terminal key")
                    .1 = decode(value);
            }
        }
    }
}

/// Expected rows: per user, the `WINDOW` newest posts (ties by id).
fn expected(oracle: &BTreeMap<u64, (u64, u64)>) -> Vec<Vec<Value>> {
    let mut by_user = BTreeMap::<u64, Vec<(u64, u64)>>::new();
    for (id, (user, created)) in oracle {
        by_user.entry(*user).or_default().push((*created, *id));
    }
    let mut rows = Vec::new();
    for (user, mut posts) in by_user {
        posts.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        rows.extend(
            posts
                .into_iter()
                .take(WINDOW)
                .map(|(created, id)| vec![Value::U64(user), Value::U64(id), Value::U64(created)]),
        );
    }
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

fn visible_set(rows: &VisibleRows) -> Vec<Vec<Value>> {
    let mut values = rows.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>();
    values.sort_by_key(|row| format!("{row:?}"));
    values
}

#[futures_test::test]
#[ignore = "#3290: TopBy terminal edits are keyed by output field 0, collapsing rows that share it"]
async fn partitioned_top_by_terminal_edits_track_rows_sharing_field_zero() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "posts",
        [
            ColumnSchema::new("user", ColumnType::U64),
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("created", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut db = Database::new(schema, MemoryStorage::new(&["posts"]).unwrap())
        .await
        .unwrap();
    let graph = GraphBuilder::top_by(
        GraphBuilder::table("posts"),
        ["user"],
        [TopByOrder::desc("created")],
        ["id"],
        0,
        TopByLimit::Finite(WINDOW as u64),
    );
    let subscription = db.subscribe([("feed", graph)]).unwrap();
    subscription.try_recv().unwrap();
    let mut visible = VisibleRows::new();
    let mut oracle = BTreeMap::new();

    // Two users, three posts each: every window holds two rows sharing
    // field 0.
    let mut seed = db.open_batch();
    for (id, user, created) in [
        (1, 1, 10),
        (2, 1, 20),
        (3, 1, 30),
        (4, 2, 15),
        (5, 2, 25),
        (6, 2, 5),
    ] {
        seed.insert(
            "posts",
            vec![Value::U64(user), Value::U64(id), Value::U64(created)],
        );
        oracle.insert(id, (user, created));
    }
    let persistence = db.apply_batch(seed).await.unwrap().persist().await;
    db.finish_persistence(persistence).unwrap();
    apply(
        &mut visible,
        &subscription.try_recv().unwrap().terminal_sinks["feed"].operations,
    );
    assert_eq!(visible_set(&visible), expected(&oracle));

    // Bumping user 1's oldest post evicts another user-1 row from the window.
    let mut random = 0x3290_u64;
    for step in 0..40 {
        let mut batch = db.open_batch();
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let id = 1 + random % 6;
        let (user, _) = oracle[&id];
        let created = 100 + step;
        batch.update(
            "posts",
            vec![Value::U64(user), Value::U64(id), Value::U64(created)],
        );
        oracle.insert(id, (user, created));
        let persistence = db.apply_batch(batch).await.unwrap().persist().await;
        db.finish_persistence(persistence).unwrap();
        if let Ok(tick) = subscription.try_recv() {
            apply(&mut visible, &tick.terminal_sinks["feed"].operations);
        }
        assert_eq!(visible_set(&visible), expected(&oracle), "step {step}");
    }
}
