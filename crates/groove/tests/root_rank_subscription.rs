//! Public terminal edits must apply sequentially to the exact ordered result.

use std::collections::BTreeMap;

use groove::db::{Database, GraphBuilder, PrimaryKeyValue};
use groove::ivm::runtime::{TerminalEdit, TerminalOperation};
use groove::ivm::{CollectByField, TopByLimit, TopByOrder};
use groove::records::{BorrowedRecord, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

type VisibleRows = Vec<(Vec<u8>, Vec<Value>)>;

fn graph(descending: bool) -> GraphBuilder {
    GraphBuilder::collect_root_ordered(
        GraphBuilder::table("items"),
        ["id"],
        [
            CollectByField::named("id"),
            CollectByField::named("title"),
            CollectByField::named("rank"),
        ],
        [if descending {
            TopByOrder::desc("rank")
        } else {
            TopByOrder::asc("rank")
        }],
        ["id"],
        0,
        TopByLimit::Unbounded,
    )
}

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
                assert!(!rows.iter().any(|(k, _)| k == key));
                rows.insert(*index, (key.clone(), decode(value)));
            }
            TerminalEdit::Remove { key } => {
                rows.remove(rows.iter().position(|(k, _)| k == key).unwrap());
            }
            TerminalEdit::Move { key, index } => {
                let row = rows.remove(rows.iter().position(|(k, _)| k == key).unwrap());
                rows.insert(*index, row);
            }
            TerminalEdit::Update { key, value } => {
                rows.iter_mut().find(|(k, _)| k == key).unwrap().1 = decode(value);
            }
        }
    }
}

fn check(rows: &VisibleRows, oracle: &BTreeMap<u64, (String, u64)>, descending: bool) {
    let mut sorted = oracle.iter().collect::<Vec<_>>();
    sorted.sort_by(|(a, (.., ar)), (b, (.., br))| {
        (if descending { br.cmp(ar) } else { ar.cmp(br) }).then(a.cmp(b))
    });
    let expected = sorted
        .into_iter()
        .map(|(id, (title, rank))| {
            vec![
                Value::U64(*id),
                Value::String(title.clone()),
                Value::U64(*rank),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows.iter()
            .map(|(_, values)| values.clone())
            .collect::<Vec<_>>(),
        expected
    );
}

#[futures_test::test]
async fn mixed_root_edits_and_shared_subscribers_match_exact_sorted_rows() {
    for descending in [false, true] {
        let schema = DatabaseSchema::new([TableSchema::new(
            "items",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("rank", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
        let mut db = Database::new(schema, MemoryStorage::new(&["items"]).unwrap())
            .await
            .unwrap();
        let subscription = db.subscribe([("rows", graph(descending))]).unwrap();
        let second = db.subscribe([("rows", graph(descending))]).unwrap();
        subscription.try_recv().unwrap();
        second.try_recv().unwrap();
        let mut visible = Vec::new();
        let mut other = Vec::new();
        let mut oracle = BTreeMap::new();
        let mut seed = db.open_batch();
        for id in 1..=4 {
            seed.insert(
                "items",
                vec![
                    Value::U64(id),
                    Value::String(format!("item-{id}")),
                    Value::U64(id * 10),
                ],
            );
            oracle.insert(id, (format!("item-{id}"), id * 10));
        }
        let persistence = db.apply_batch(seed).await.unwrap().persist().await;
        db.finish_persistence(persistence).unwrap();
        apply(
            &mut visible,
            &subscription.try_recv().unwrap().terminal_sinks["rows"].operations,
        );
        apply(
            &mut other,
            &second.try_recv().unwrap().terminal_sinks["rows"].operations,
        );
        check(&visible, &oracle, descending);
        assert_eq!(visible, other);

        // A->25 and D->22: [A,B,C,D] becomes [B,D,A,C]. Moving D to
        // its final rank 1 and then A to 2 would incorrectly produce [D,B,A,C].
        let mut crossing = db.open_batch();
        for (id, rank) in [(1, 25), (4, 22)] {
            crossing.update(
                "items",
                vec![
                    Value::U64(id),
                    Value::String(format!("moved-{id}")),
                    Value::U64(rank),
                ],
            );
            oracle.insert(id, (format!("moved-{id}"), rank));
        }
        let persistence = db.apply_batch(crossing).await.unwrap().persist().await;
        db.finish_persistence(persistence).unwrap();
        apply(
            &mut visible,
            &subscription.try_recv().unwrap().terminal_sinks["rows"].operations,
        );
        apply(
            &mut other,
            &second.try_recv().unwrap().terminal_sinks["rows"].operations,
        );
        check(&visible, &oracle, descending);
        assert_eq!(visible, other);

        let mut random = 0x89abcdef_u64;
        for step in 0..150 {
            let mut batch = db.open_batch();
            for edit in 0..4 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let id = 1 + random % 48;
                let rank = (random >> 16) % 12; // Deliberate ties, resolved by identity.
                let title = format!("{step}-{edit}-{id}");
                if random % 5 == 0 && oracle.remove(&id).is_some() {
                    batch.delete("items", PrimaryKeyValue::U64(id));
                } else {
                    let values = vec![
                        Value::U64(id),
                        Value::String(title.clone()),
                        Value::U64(rank),
                    ];
                    if oracle.contains_key(&id) {
                        batch.update("items", values);
                    } else {
                        batch.insert("items", values);
                    }
                    oracle.insert(id, (title, rank));
                }
            }
            let persistence = db.apply_batch(batch).await.unwrap().persist().await;
            db.finish_persistence(persistence).unwrap();
            if let Ok(tick) = subscription.try_recv() {
                apply(&mut visible, &tick.terminal_sinks["rows"].operations);
                apply(
                    &mut other,
                    &second.try_recv().unwrap().terminal_sinks["rows"].operations,
                );
            }
            check(&visible, &oracle, descending);
            assert_eq!(visible, other);
        }
    }
}
