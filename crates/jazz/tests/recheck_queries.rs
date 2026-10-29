//! Old query and write reports rechecked against the SQLite engine: each
//! test pins the behaviour its issue asked for.
use std::collections::BTreeMap;

use crate::common;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SubscriptionEvent, UpdateOptions,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, in_list, lit, lt, not};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn open(schema: JazzSchema) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x31; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
    )))
    .unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn cells(pairs: Vec<(&str, Value)>) -> BTreeMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn insert(db: &Db, table: &str, n: u8, pairs: Vec<(&str, Value)>) {
    block_on(db.insert(
        table,
        cells(pairs),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn read(db: &Db, query: &Query) -> Vec<jazz::db::CurrentRow> {
    block_on(db.all(&db.prepare_query(query).unwrap(), ReadOpts::default())).unwrap()
}

fn ids(db: &Db, query: &Query) -> Vec<u8> {
    let mut ids: Vec<u8> = read(db, query)
        .iter()
        .map(|row| row.row_uuid().as_bytes()[0])
        .collect();
    ids.sort();
    ids
}

fn cell(row: &jazz::db::CurrentRow, name: &str) -> Option<Value> {
    row.cells().find(|(n, _)| *n == name)?.1.cloned()
}

fn some(value: Value) -> Value {
    Value::Nullable(Some(Box::new(value)))
}

/// #3992: a flat join on two array columns used to pair unequal arrays
/// and double weights. It is refused as not supported yet, so it can't
/// answer wrong rows.
#[test]
fn an_array_equality_join_is_refused_rather_than_answered_wrong() {
    let codes = || ColumnType::Array {
        element: Box::new(ColumnType::Integer),
    };
    let db = open(compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("left_items")
                    .column("codes", codes())
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("right_items")
                    .column("codes", codes())
                    .policies(allow_all_policies()),
            )
            .build(),
    ));
    let array = |items: &[i32]| Value::Array(items.iter().map(|i| Value::I32(*i)).collect());
    insert(&db, "left_items", 1, vec![("codes", array(&[1, 2]))]);
    insert(&db, "right_items", 2, vec![("codes", array(&[2, 1]))]);
    let join =
        Query::from("left_items").flat_join("right_items", "left_items.codes", "right_items.codes");
    let error = refusal(&db, &join);
    assert_eq!(error.code, jazz::db::ErrorCode::Query);
    assert!(
        error.message.contains("not supported yet"),
        "{}",
        error.message
    );
}

/// The error a query is refused with, at prepare or read time.
fn refusal(db: &Db, query: &Query) -> jazz::db::Error {
    match db.prepare_query(query) {
        Err(error) => error,
        Ok(prepared) => match block_on(db.all(&prepared, ReadOpts::default())) {
            Err(error) => error,
            Ok(rows) => panic!("expected a refusal, read {} rows", rows.len()),
        },
    }
}

/// #4026: a comparison with NaN is false whether the other side is a
/// literal or a column, so its negation keeps every row, folded or not.
#[test]
fn nan_comparisons_agree_for_literals_and_columns_under_not() {
    let db = open(compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("points")
                    .column("x", ColumnType::Double)
                    .policies(allow_all_policies()),
            )
            .build(),
    ));
    insert(&db, "points", 1, vec![("x", Value::F64(0.5))]);
    insert(&db, "points", 2, vec![("x", Value::F64(2.0))]);
    let points = || Query::from("points");
    let nan = || lit(f64::NAN);
    let all = vec![1, 2];
    assert_eq!(
        ids(&db, &points().filter(lt(nan(), lit(1.0)))),
        Vec::<u8>::new()
    );
    assert_eq!(ids(&db, &points().filter(not(lt(nan(), lit(1.0))))), all);
    assert_eq!(
        ids(&db, &points().filter(lt(col("x"), nan()))),
        Vec::<u8>::new()
    );
    assert_eq!(ids(&db, &points().filter(not(lt(col("x"), nan())))), all);
    assert_eq!(
        ids(&db, &points().filter(not(in_list(col("x"), [nan()])))),
        all
    );
}

/// #1864: a predicate may compare two columns of the same row.
#[test]
fn a_predicate_compares_two_columns_of_a_row() {
    let db = open(compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("ranges")
                    .column("low", ColumnType::Integer)
                    .column("high", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .build(),
    ));
    insert(
        &db,
        "ranges",
        1,
        vec![("low", Value::I32(1)), ("high", Value::I32(5))],
    );
    insert(
        &db,
        "ranges",
        2,
        vec![("low", Value::I32(7)), ("high", Value::I32(3))],
    );
    let ranges = Query::from("ranges");
    assert_eq!(
        ids(&db, &ranges.clone().filter(lt(col("low"), col("high")))),
        [1]
    );
    assert_eq!(
        ids(&db, &ranges.filter(not(lt(col("low"), col("high"))))),
        [2]
    );
}

fn json_db() -> Db {
    open(compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .nullable_column("meta", ColumnType::Json { schema: None })
                    .policies(allow_all_policies()),
            )
            .build(),
    ))
}

/// #2733: an optional JSON column takes an explicit null on insert, and an
/// update can clear it.
#[test]
fn an_optional_json_column_takes_null_on_insert_and_clears_on_update() {
    let db = json_db();
    insert(
        &db,
        "notes",
        1,
        vec![
            ("title", Value::String("a".into())),
            ("meta", Value::Nullable(None)),
        ],
    );
    insert(
        &db,
        "notes",
        2,
        vec![
            ("title", Value::String("b".into())),
            ("meta", some(Value::String(r#"{"k":1}"#.into()))),
        ],
    );
    block_on(db.update(
        "notes",
        row(2),
        cells(vec![("meta", Value::Nullable(None))]),
        UpdateOptions::default(),
    ))
    .unwrap();
    for row in read(&db, &Query::from("notes")) {
        assert!(
            matches!(cell(&row, "meta"), None | Some(Value::Nullable(None))),
            "{:?}",
            cell(&row, "meta")
        );
    }
}

/// #3980: a stored value in an optional JSON column reaches a subscription
/// and a read, before and after an update.
#[test]
fn an_optional_json_value_reaches_subscriptions_and_reads() {
    let db = json_db();
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    insert(
        &db,
        "notes",
        1,
        vec![
            ("title", Value::String("a".into())),
            ("meta", some(Value::String(r#"{"k":1}"#.into()))),
        ],
    );
    block_on(db.update(
        "notes",
        row(1),
        cells(vec![("meta", some(Value::String(r#"{"k":[2]}"#.into())))]),
        UpdateOptions::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    let mut seen = Vec::new();
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta { added, updated, .. } => seen.extend(
                added
                    .iter()
                    .chain(&updated)
                    .filter_map(|row| row.cell("meta")),
            ),
            SubscriptionEvent::Rejected { code: reason } => {
                panic!("subscription failed: {reason:?}")
            }
            _ => {}
        }
    }
    assert_eq!(
        seen.last(),
        Some(&some(Value::String(r#"{"k":[2]}"#.into())))
    );
    let rows = read(&db, &Query::from("notes"));
    assert_eq!(
        cell(&rows[0], "meta"),
        Some(some(Value::String(r#"{"k":[2]}"#.into())))
    );
}

/// #3312: a value of the wrong type is refused, not stored.
#[test]
fn a_wrong_typed_insert_is_refused() {
    let db = json_db();
    let refused = block_on(db.insert(
        "notes",
        cells(vec![("title", Value::I32(42))]),
        InsertOptions::default(),
    ));
    assert!(refused.is_err(), "a number went into a text column");
    assert!(read(&db, &Query::from("notes")).is_empty());
}

/// #2744: rows inserted without an id, streamed or not, get RFC 9562
/// version 7 ids.
#[test]
fn generated_row_ids_are_version_7_uuids() {
    let db = json_db();
    let plain = block_on(db.insert(
        "notes",
        cells(vec![("title", Value::String("plain".into()))]),
        InsertOptions::default(),
    ))
    .unwrap()
    .row_uuid();
    let streamed = block_on(db.insert_streaming_value(
        "notes",
        BTreeMap::new(),
        "title",
        std::io::Cursor::new(b"streamed".to_vec()),
    ))
    .unwrap()
    .row_uuid();
    for id in [plain, streamed] {
        assert_eq!(id.0.get_version_num(), 7, "{id:?}");
        assert_eq!(id.0.get_variant(), uuid::Variant::RFC4122, "{id:?}");
    }
}

/// #4192: a grouped aggregate projected to its key used to answer
/// different fields and NULL keys through a read and through a snapshot.
/// The snapshot path is gone, and the projection is refused as not supported
/// yet, alike through a read and the bindings' read entry point.
#[test]
fn a_projected_aggregate_is_refused_alike_through_both_read_paths() {
    let db = open(compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("metrics")
                    .nullable_column("bucket", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .build(),
    ));
    for (n, bucket) in [None, Some(-1), Some(0), Some(-1)].into_iter().enumerate() {
        insert(
            &db,
            "metrics",
            n as u8 + 1,
            vec![(
                "bucket",
                Value::Nullable(bucket.map(|b| Box::new(Value::I32(b)))),
            )],
        );
    }
    let grouped = Query::from("metrics")
        .count()
        .group_by("bucket")
        .order_by("bucket", OrderDirection::Asc);
    assert_eq!(read(&db, &grouped).len(), 3);
    let projected = grouped.select(["bucket"]);
    let direct = refusal(&db, &projected);
    let bytes = postcard::to_allocvec(&projected).unwrap();
    let serialized = block_on(db.all_serialized_query(
        &bytes,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
        None,
        None,
        None,
    ))
    .expect_err("the bindings' read refuses it too");
    assert_eq!(direct.code, jazz::db::ErrorCode::Query);
    assert!(
        direct.message.contains("not supported yet"),
        "{}",
        direct.message
    );
    assert_eq!(
        (serialized.code, serialized.message),
        (direct.code, direct.message)
    );
}

/// #3553, #3917, #3924: a JSON value is checked against its column's
/// declared JSON Schema wherever the store accepts a write, not only by the
/// writing client's own validator: a plain insert, an update, an upsert, a
/// transaction's write, a nullable column, the elements of a JSON array and
/// a value large enough to be stored in chunks. Text that is not JSON is
/// refused too.
#[test]
fn json_values_are_validated_against_the_column_schema_on_every_write() {
    let db = open(compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column(
                        "payload",
                        ColumnType::Json {
                            schema: Some(serde_json::json!({
                                "type": "object",
                                "properties": { "state": { "enum": ["open", "closed"] } },
                                "required": ["state"],
                            })),
                        },
                    )
                    .nullable_column("notes", ColumnType::Json { schema: None })
                    .column(
                        "counts",
                        ColumnType::Array {
                            element: Box::new(ColumnType::Json {
                                schema: Some(serde_json::json!({ "type": "integer" })),
                            }),
                        },
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    ));
    let text = |s: &str| Value::String(s.to_owned());
    let valid = || {
        cells(vec![
            ("payload", text(r#"{"state":"open"}"#)),
            ("notes", some(text(r#"{"any":"thing"}"#))),
            ("counts", Value::Array(vec![text("1"), text("2")])),
        ])
    };
    let with = |name: &str, value: Value| {
        let mut cells = valid();
        cells.insert(name.to_owned(), value);
        cells
    };
    let refusal = |result: Result<jazz::db::WriteHandle, jazz::db::Error>, needle: &str| {
        let error = result.expect_err("an invalid JSON value was stored");
        assert_eq!(error.code, jazz::db::ErrorCode::WriteRejected, "{error}");
        assert!(error.message.contains(needle), "{error}");
    };
    let insert_at = |n: u8, cells| {
        block_on(db.insert(
            "docs",
            cells,
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
    };
    insert_at(1, valid()).unwrap();

    let mismatch = "JSON schema validation failed for column `payload`";
    refusal(
        insert_at(2, with("payload", text(r#"{"state":"not-a-state"}"#))),
        mismatch,
    );
    refusal(
        insert_at(2, with("notes", some(text("not json")))),
        "invalid JSON for column `notes`",
    );
    refusal(
        insert_at(
            2,
            with("counts", Value::Array(vec![text("1"), text("\"x\"")])),
        ),
        "JSON schema validation failed for column `counts[1]`",
    );
    refusal(
        block_on(db.update(
            "docs",
            row(1),
            cells(vec![("payload", text(r#"{"state":1}"#))]),
            UpdateOptions::default(),
        )),
        mismatch,
    );
    refusal(
        block_on(db.upsert(
            "docs",
            row(3),
            with("payload", text("{}")),
            Default::default(),
        )),
        mismatch,
    );
    // A value large enough to be stored in chunks is checked whole.
    let pad = "a".repeat(jazz::large_values::INLINE_VALUE_MAX_BYTES + 1024);
    let large = |state: &str| text(&format!(r#"{{"state":{state},"pad":"{pad}"}}"#));
    insert_at(4, with("payload", large("\"open\""))).unwrap();
    refusal(insert_at(5, with("payload", large("\"nope\""))), mismatch);
    refusal(
        block_on(db.update(
            "docs",
            row(4),
            cells(vec![("payload", large("1"))]),
            UpdateOptions::default(),
        )),
        mismatch,
    );
    let open = jazz::tools::OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    refusal(
        block_on(db.tx_ref(open).insert(
            "docs",
            with("payload", text(r#"{"state":"maybe"}"#)),
            InsertOptions::default(),
        )),
        mismatch,
    );
    db.abandon_transaction_handle(open).unwrap();

    let rows = read(&db, &Query::from("docs"));
    assert_eq!(rows.len(), 2);
    assert_eq!(cell(&rows[0], "payload"), Some(text(r#"{"state":"open"}"#)));
    assert_eq!(cell(&rows[1], "payload"), Some(large("\"open\"")));
}
