use std::collections::BTreeMap;

use jazz::block_on;
use jazz::db::CurrentRow;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionOutputRow, UpdateOptions,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Aggregate, OrderDirection, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

fn nullable(value: Option<Value>) -> Value {
    Value::Nullable(value.map(Box::new))
}

fn local() -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    }
}

fn apply_event(rows: &mut Vec<SubscriptionOutputRow>, event: SubscriptionEvent, initial: bool) {
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        ..
    } = event
    else {
        panic!("expected subscription delta");
    };
    assert_eq!(reset, initial);
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

fn assert_rows<'a>(
    context: &str,
    rows: impl IntoIterator<Item = &'a CurrentRow>,
    schema: &JazzSchema,
    expected: &[(Option<&str>, u64, Option<i32>)],
) {
    let rows = rows.into_iter().collect::<Vec<_>>();
    assert_eq!(rows.len(), expected.len(), "{context}: row count");
    for (row, (key, count, sum)) in rows.into_iter().zip(expected) {
        let key = key.map(|key| Value::String(key.to_owned()));
        assert_eq!(
            row.cell(&schema.tables[0], "bucket"),
            Some(nullable(key.clone())),
            "{context}: a NULL group key remains a present logical nullable value"
        );
        assert_eq!(
            row.cell_at(0),
            Some(nullable(key)),
            "{context}: a positional key has the same declared type as the named one"
        );
        assert_eq!(row.cell_at(1), Some(Value::U64(*count)), "{context}: count");
        assert_eq!(
            row.cell_at(2),
            Some(nullable(sum.map(Value::I32))),
            "{context}: sum"
        );
    }
}

/// Alice reads and subscribes to nullable groups and sums. NULL keys remain
/// present, all read paths expose the same cells, and live changes preserve
/// group identity while sums become NULL or a group disappears and returns.
///
/// alice -- seed NULL/alpha --> read / subscription --> same cells
/// alice -- change score --> NULL / non-NULL sums --> same group occurrence
/// alice -- move row to alpha and back --> group removal / original identity
#[test]
fn nullable_group_keys_agree_across_reads_and_live_subscription() {
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("items")
                .nullable_column("bucket", ColumnType::Text)
                .nullable_column("score", ColumnType::Integer)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                ),
        )
        .build();
    let schema = JazzSchema::new(&source).unwrap();
    let db = block_on(Db::open(DbConfig::new(
        schema.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([0x72; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    for (id, bucket, score) in [(1, None, None), (2, Some("alpha"), Some(5))] {
        block_on(db.insert(
            "items",
            BTreeMap::from([
                (
                    "bucket".to_owned(),
                    nullable(bucket.map(|key| Value::String(key.to_owned()))),
                ),
                ("score".to_owned(), nullable(score.map(Value::I32))),
            ]),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([id; 16])),
                ..InsertOptions::default()
            },
        ))
        .unwrap();
    }
    let query = db
        .prepare_query(
            &Query::from("items")
                .aggregate([Aggregate::count(), Aggregate::sum("score").alias("total")])
                .group_by("bucket")
                .order_by("bucket", OrderDirection::Asc),
        )
        .unwrap();
    let mut subscription = block_on(db.subscribe(&query, local())).unwrap();
    let mut rows = Vec::new();
    let mut identities = BTreeMap::new();
    let stages = [
        (None, vec![(None, 1, None), (Some("alpha"), 1, Some(5))]),
        (
            Some(("score", Some(Value::I32(7)))),
            vec![(None, 1, Some(7)), (Some("alpha"), 1, Some(5))],
        ),
        (
            Some(("score", None)),
            vec![(None, 1, None), (Some("alpha"), 1, Some(5))],
        ),
        (
            Some(("bucket", Some(Value::String("alpha".to_owned())))),
            vec![(Some("alpha"), 2, Some(5))],
        ),
        (
            Some(("bucket", None)),
            vec![(None, 1, None), (Some("alpha"), 1, Some(5))],
        ),
    ];
    for (stage, (patch, expected)) in stages.into_iter().enumerate() {
        if let Some((column, value)) = patch {
            block_on(db.update(
                "items",
                RowUuid::from_bytes([1; 16]),
                BTreeMap::from([(column.to_owned(), nullable(value))]),
                UpdateOptions::default(),
            ))
            .unwrap();
        }
        let event = if stage == 0 {
            block_on(subscription.next_event()).unwrap()
        } else {
            subscription
                .try_next_event()
                .expect("live aggregate change")
        };
        apply_event(&mut rows, event, stage == 0);
        while let Some(event) = subscription.try_next_event() {
            apply_event(&mut rows, event, false);
        }
        assert_rows(
            &format!("stage {stage} subscription"),
            rows.iter().map(|row| &row.row),
            &schema,
            &expected,
        );
        for row in &rows {
            let key = match row.cell_at(0) {
                Some(Value::Nullable(None)) => None,
                Some(Value::Nullable(Some(key))) => match *key {
                    Value::String(key) => Some(key),
                    other => panic!("unexpected group key: {other:?}"),
                },
                other => panic!("unexpected group key: {other:?}"),
            };
            let original = identities.entry(key).or_insert(row.row_uuid());
            assert_eq!(*original, row.row_uuid());
        }
        let read = block_on(db.all(&query, local())).unwrap();
        assert_rows(&format!("stage {stage} read"), &read, &schema, &expected);
    }
}
