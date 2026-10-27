use std::collections::BTreeMap;

use jazz::block_on;
use jazz::db::CurrentRow;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionOutputRow,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

type Groups = Vec<(Value, u64)>;

fn open_db(group_type: ColumnType) -> Db {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("items")
                .column("group", group_type)
                .policies(policies),
        )
        .build();
    let schema = JazzSchema::new(&source).expect("compile public schema");
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x71; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open db")
}

fn local() -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    }
}

fn insert(db: &Db, group: Value, byte: u8) {
    block_on(db.insert(
        "items",
        BTreeMap::from([("group".to_owned(), group)]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([byte; 16])),
            ..InsertOptions::default()
        },
    ))
    .expect("insert item");
}

fn groups(rows: impl IntoIterator<Item = CurrentRow>) -> Groups {
    rows.into_iter()
        .map(|row| match (row.cell_at(0), row.cell_at(1)) {
            (Some(group), Some(Value::U64(count))) => (group, count),
            other => panic!("expected grouped key and count, got {other:?}"),
        })
        .collect()
}

fn text(rows: &[(&str, u64)]) -> Groups {
    rows.iter()
        .map(|(key, count)| (Value::String((*key).to_owned()), *count))
        .collect()
}

fn apply_event(rows: &mut Vec<SubscriptionOutputRow>, event: SubscriptionEvent, initial: bool) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } => {
            assert_eq!(reset, initial, "only the initial event resets the result");
            if reset {
                rows.clear();
            }
            rows.retain(|row| {
                !removed.iter().any(|item| item.row_uuid == row.row_uuid())
                    && !updated.iter().any(|item| item.row_uuid() == row.row_uuid())
            });
            let mut incoming = added.into_iter().chain(updated).collect::<Vec<_>>();
            incoming.sort_by_key(|row| row.index);
            for row in incoming {
                rows.insert(row.index, row);
            }
        }
        other => panic!("expected subscription delta, got {other:?}"),
    }
}

fn remember_identities(history: &mut Vec<SubscriptionOutputRow>, rows: &[SubscriptionOutputRow]) {
    for row in rows {
        if let Some(previous) = history
            .iter()
            .find(|previous| previous.row.cell_at(0) == row.row.cell_at(0))
        {
            assert_eq!(
                previous.row_uuid(),
                row.row_uuid(),
                "surviving or reappearing group keeps its public occurrence identity"
            );
        } else {
            assert!(
                history
                    .iter()
                    .all(|previous| previous.row_uuid() != row.row_uuid())
            );
            history.push(row.clone());
        }
    }
}

/// Alice reads and subscribes to grouped counts. Logical keys, not opaque
/// identities, order defaults and count ties before pagination; descending
/// group terms still win. Signed keys follow numeric rather than encoded order.
///
/// alice -- seed groups --> initial reset / read --> logical full and page
/// alice -- grow delta --> live count reorder --> stable group occurrences
/// alice -- add aardvark --> live page shift --> stable surviving occurrences
/// alice -- grow bravo --> page re-entry --> original occurrence restored
#[test]
fn grouped_aggregate_ordering_is_logical_before_windows() {
    let db = open_db(ColumnType::Text);
    for (index, group) in ["alpha", "bravo", "charlie", "delta"].iter().enumerate() {
        insert(&db, Value::String((*group).to_owned()), index as u8 + 1);
    }
    let base = Query::from("items").count().group_by("group");
    let tied = base.clone().order_by("count", OrderDirection::Desc);
    let descending = base.clone().order_by("group", OrderDirection::Desc);
    // Each stage supplies independent literal full/page expectations.
    let expected = [
        [
            text(&[("alpha", 1), ("bravo", 1), ("charlie", 1), ("delta", 1)]),
            text(&[("bravo", 1), ("charlie", 1)]),
            text(&[("alpha", 1), ("bravo", 1), ("charlie", 1), ("delta", 1)]),
            text(&[("bravo", 1), ("charlie", 1)]),
            text(&[("delta", 1), ("charlie", 1), ("bravo", 1), ("alpha", 1)]),
            text(&[("charlie", 1), ("bravo", 1)]),
        ],
        [
            text(&[("alpha", 1), ("bravo", 1), ("charlie", 1), ("delta", 2)]),
            text(&[("bravo", 1), ("charlie", 1)]),
            text(&[("delta", 2), ("alpha", 1), ("bravo", 1), ("charlie", 1)]),
            text(&[("alpha", 1), ("bravo", 1)]),
            text(&[("delta", 2), ("charlie", 1), ("bravo", 1), ("alpha", 1)]),
            text(&[("charlie", 1), ("bravo", 1)]),
        ],
        [
            text(&[
                ("aardvark", 1),
                ("alpha", 1),
                ("bravo", 1),
                ("charlie", 1),
                ("delta", 2),
            ]),
            text(&[("alpha", 1), ("bravo", 1)]),
            text(&[
                ("delta", 2),
                ("aardvark", 1),
                ("alpha", 1),
                ("bravo", 1),
                ("charlie", 1),
            ]),
            text(&[("aardvark", 1), ("alpha", 1)]),
            text(&[
                ("delta", 2),
                ("charlie", 1),
                ("bravo", 1),
                ("alpha", 1),
                ("aardvark", 1),
            ]),
            text(&[("charlie", 1), ("bravo", 1)]),
        ],
        [
            text(&[
                ("aardvark", 1),
                ("alpha", 1),
                ("bravo", 2),
                ("charlie", 1),
                ("delta", 2),
            ]),
            text(&[("alpha", 1), ("bravo", 2)]),
            text(&[
                ("bravo", 2),
                ("delta", 2),
                ("aardvark", 1),
                ("alpha", 1),
                ("charlie", 1),
            ]),
            text(&[("delta", 2), ("aardvark", 1)]),
            text(&[
                ("delta", 2),
                ("charlie", 1),
                ("bravo", 2),
                ("alpha", 1),
                ("aardvark", 1),
            ]),
            text(&[("charlie", 1), ("bravo", 2)]),
        ],
        [
            text(&[
                ("aardvark", 2),
                ("alpha", 1),
                ("bravo", 2),
                ("charlie", 1),
                ("delta", 2),
            ]),
            text(&[("alpha", 1), ("bravo", 2)]),
            text(&[
                ("aardvark", 2),
                ("bravo", 2),
                ("delta", 2),
                ("alpha", 1),
                ("charlie", 1),
            ]),
            text(&[("bravo", 2), ("delta", 2)]),
            text(&[
                ("delta", 2),
                ("charlie", 1),
                ("bravo", 2),
                ("alpha", 1),
                ("aardvark", 2),
            ]),
            text(&[("charlie", 1), ("bravo", 2)]),
        ],
    ];
    let queries = [
        ("default full", base.clone()),
        ("default page", base.offset(1).limit(2)),
        ("count ties full", tied.clone()),
        ("count ties page", tied.offset(1).limit(2)),
        ("group descending full", descending.clone()),
        ("group descending page", descending.offset(1).limit(2)),
    ];
    let mut views = Vec::new();
    for (label, query) in queries {
        let prepared = db.prepare_query(&query).expect("prepare query");
        let subscription = block_on(db.subscribe(&prepared, local())).expect("subscribe");
        views.push((label, prepared, subscription, Vec::new(), Vec::new()));
    }
    let mut failures = Vec::new();
    for (stage, expectations) in expected.iter().enumerate() {
        match stage {
            1 => insert(&db, Value::String("delta".to_owned()), 5),
            2 => insert(&db, Value::String("aardvark".to_owned()), 6),
            3 => insert(&db, Value::String("bravo".to_owned()), 7),
            4 => insert(&db, Value::String("aardvark".to_owned()), 8),
            _ => {}
        }
        for ((label, prepared, subscription, rows, history), expected) in
            views.iter_mut().zip(expectations)
        {
            let actual = groups(block_on(db.all(prepared, local())).expect("one-shot read"));
            println!("stage {stage} read {label}: {actual:?}");
            if actual != *expected {
                failures.push(format!(
                    "stage {stage} read {label}: {actual:?} != {expected:?}"
                ));
            }
            if stage == 0 {
                let event = block_on(subscription.next_event()).expect("initial event");
                apply_event(rows, event, true);
            } else {
                let mut events = 0;
                while let Some(event) = subscription.try_next_event() {
                    apply_event(rows, event, false);
                    events += 1;
                }
                if label.ends_with("full") {
                    assert!(events > 0, "{label} must deliver the live change");
                }
            }
            remember_identities(history, rows);
            let actual = groups(rows.iter().map(|row| row.row.clone()));
            println!("stage {stage} subscription {label}: {actual:?}");
            if actual != *expected {
                failures.push(format!(
                    "stage {stage} subscription {label}: {actual:?} != {expected:?}"
                ));
            }
        }
    }

    let numeric = open_db(ColumnType::Integer);
    for (index, key) in [2, -1, -10, 0].into_iter().enumerate() {
        insert(&numeric, Value::I32(key), index as u8 + 1);
    }
    let base = Query::from("items").count().group_by("group");
    for (label, query, keys) in [
        ("signed default", base.clone(), vec![-10, -1, 0, 2]),
        (
            "signed count ties",
            base.clone().order_by("count", OrderDirection::Desc),
            vec![-10, -1, 0, 2],
        ),
        (
            "signed descending",
            base.order_by("group", OrderDirection::Desc),
            vec![2, 0, -1, -10],
        ),
    ] {
        let expected = keys
            .into_iter()
            .map(|key| (Value::I32(key), 1))
            .collect::<Groups>();
        let prepared = numeric
            .prepare_query(&query)
            .expect("prepare numeric query");
        let read = groups(block_on(numeric.all(&prepared, local())).expect("numeric read"));
        let mut subscription =
            block_on(numeric.subscribe(&prepared, local())).expect("numeric subscription");
        let mut rows = Vec::new();
        apply_event(
            &mut rows,
            block_on(subscription.next_event()).expect("numeric initial event"),
            true,
        );
        let subscribed = groups(rows.into_iter().map(|row| row.row));
        println!("{label}: read={read:?} subscription={subscribed:?}");
        if read != expected || subscribed != expected {
            failures.push(format!(
                "{label}: read={read:?}, subscription={subscribed:?} != {expected:?}"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "logical group ordering violations:\n{}",
        failures.join("\n")
    );
}
