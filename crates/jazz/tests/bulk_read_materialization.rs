//! Bulk reads preserve each row's payload, publication bindings and provenance.
//! The fixture includes names that overlap generated storage carriers, nullable
//! values and selected large values. All data enters through the public API.

use std::collections::{BTreeMap, HashMap};

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::groove::{records::Value, storage::TestStorage};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder, Value as PublicValue};
use jazz::tx::DurabilityTier;

mod common;
use common::{allow_all_policies, compile_schema};

fn cells(input: HashMap<String, PublicValue>) -> BTreeMap<String, Value> {
    input
        .into_iter()
        .map(|(name, value)| {
            let value = match value {
                PublicValue::Text(value) => Value::String(value),
                PublicValue::Integer(value) => Value::I32(value),
                PublicValue::Bytea(value) => Value::Bytes(value),
                PublicValue::Null => Value::Nullable(None),
                other => panic!("unexpected fixture value: {other:?}"),
            };
            let value = if name == "note" && !matches!(value, Value::Nullable(_)) {
                Value::Nullable(Some(Box::new(value)))
            } else {
                value
            };
            (name, value)
        })
        .collect()
}

#[test]
fn bulk_reads_preserve_values_names_nulls_and_per_row_provenance() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .column("title", ColumnType::Text)
                    .column("user_title", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .nullable_column("note", ColumnType::Text)
                    .column("contents", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let table = schema
        .tables()
        .iter()
        .find(|t| t.name == "documents")
        .unwrap()
        .clone();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let author = AuthorSubject::for_test_bytes([0x41; 16]);
    let db = block_on(Db::open(DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0x42; 16]),
            author,
        },
    )))
    .unwrap();
    let mut expected = BTreeMap::new();
    for n in 1..=8_u8 {
        let id = RowUuid::from_bytes([n; 16]);
        let title = format!("document {n} — 🦀");
        let carrier_name = format!("literal user_title {n}");
        let note = if n % 2 == 0 {
            PublicValue::Text(format!("note {n}"))
        } else {
            PublicValue::Null
        };
        let contents = vec![n; if n == 8 { 150_000 } else { n as usize * 13 }];
        let input = cells(jazz::row_input!(
            "title" => title,
            "user_title" => carrier_name,
            "rank" => i32::from(n),
            "note" => note,
            "contents" => PublicValue::Bytea(contents)
        ));
        let write = block_on(db.insert(
            "documents",
            input.clone(),
            InsertOptions {
                row_id: Some(id),
                updated_at_ms: Some(1000 + u64::from(n)),
                ..Default::default()
            },
        ))
        .unwrap();
        db.finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
            .unwrap();
        expected.insert(id, input);
    }
    let opts = ReadOpts {
        tier: DurabilityTier::Global,
        ..Default::default()
    };
    for query in [
        Query::from("documents"),
        Query::from("documents").order_by("rank", OrderDirection::Desc),
        Query::from("documents").select(["user_title", "note", "$createdAt", "$updatedAt"]),
    ] {
        let prepared = db.prepare_query(&query).unwrap();
        let rows = block_on(db.all_for_identity(&prepared, opts.clone(), author)).unwrap();
        assert_eq!(rows.len(), expected.len());
        let mut expected_ids = expected.keys().copied().collect::<Vec<_>>();
        if !query.order_by.is_empty() {
            expected_ids.reverse();
        }
        assert_eq!(
            rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
            expected_ids
        );
        for row in &rows {
            for (name, value) in &expected[&row.row_uuid()] {
                if query
                    .select
                    .as_ref()
                    .is_none_or(|columns| columns.contains(name))
                {
                    assert_eq!(row.cell(&table, name), Some(value.clone()), "cell {name}");
                } else {
                    assert_eq!(row.cell(&table, name), None, "unselected cell {name}");
                }
            }
            let provenance = db.row_provenance(row).unwrap().unwrap();
            assert_eq!(provenance.created_by, author);
            assert_eq!(provenance.updated_by, author);
            assert_eq!(
                provenance.created_at,
                1000 + u64::from(row.row_uuid().to_bytes()[0])
            );
            assert_eq!(provenance.updated_at, provenance.created_at);
            // A single-row result also pins exact encoded layout and metadata,
            // independently of the bulk conversion's prepared projection.
            let point = db
                .prepare_query(
                    &query
                        .clone()
                        .filter(eq(col("id"), lit(Value::Uuid(row.row_uuid().0)))),
                )
                .unwrap();
            let single = block_on(db.all_for_identity(&point, opts.clone(), author)).unwrap();
            assert_eq!(single.as_slice(), std::slice::from_ref(row));
        }
    }
    block_on(db.close()).unwrap();
}
