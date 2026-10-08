//! Whole-value array equality through Groove's public SQL-ish query API.

use groove::db::Database;
use groove::queries::{
    BinaryOp, ColumnRef, Expr, JoinConstraint, JoinKind, Query, Select, SelectItem, TableRef,
};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

type WeightedRows = Vec<(Vec<Value>, i64)>;

fn equality(column: &str) -> Expr {
    Expr::binary(
        Expr::Column(ColumnRef::qualified(["l"], column)),
        BinaryOp::Eq,
        Expr::Column(ColumnRef::qualified(["r"], column)),
    )
}

fn query(join_column: &str) -> Query {
    let select = Select::new([
        SelectItem::aliased(
            Expr::Column(ColumnRef::qualified(["l"], "row_key")),
            "left_id",
        ),
        SelectItem::aliased(
            Expr::Column(ColumnRef::qualified(["r"], "row_key")),
            "right_id",
        ),
    ])
    .from([TableRef::Join {
        left: Box::new(TableRef::named("left_items").aliased("l")),
        right: Box::new(TableRef::named("right_items").aliased("r")),
        kind: JoinKind::Inner,
        constraint: JoinConstraint::On(equality(join_column)),
    }]);
    Query::Select(Box::new(select))
}

fn literal_query(right_codes: Value) -> Query {
    Query::Select(Box::new(
        Select::new([SelectItem::aliased(
            Expr::Column(ColumnRef::qualified(["l"], "row_key")),
            "left_id",
        )])
        .from([TableRef::named("left_items").aliased("l")])
        .where_(Expr::binary(
            Expr::Column(ColumnRef::qualified(["l"], "codes")),
            BinaryOp::Eq,
            Expr::Literal(right_codes),
        )),
    ))
}

async fn observe(
    codes_type: ColumnType,
    left: Value,
    right: Value,
    compare_where: bool,
) -> (
    Result<WeightedRows, String>,
    Option<Result<WeightedRows, String>>,
) {
    let schema = DatabaseSchema::new(["left_items", "right_items"].map(|name| {
        TableSchema::new(
            name,
            [
                ColumnSchema::new("row_key", ColumnType::U64),
                ColumnSchema::new("codes", codes_type.clone()),
                ColumnSchema::new("bucket", ColumnType::I32),
            ],
        )
        .with_primary_key(PrimaryKey::new("row_key", IntegerKeyType::U64))
    }));
    let storage = MemoryStorage::new(&["left_items", "right_items"])
        .expect("valid isolated memory storage families");
    let mut database = Database::new(schema, storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert("left_items", vec![Value::U64(1), left, Value::I32(7)]);
    batch.insert(
        "right_items",
        vec![Value::U64(2), right.clone(), Value::I32(7)],
    );
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();

    let on_rows = match database.query(query("codes")).await {
        Ok(rows) => rows
            .to_values()
            .map_err(|error| format!("decode: {error:?}")),
        Err(error) => Err(format!("public SQL ON rejected: {error:?}")),
    };
    let literal_rows = if compare_where {
        Some(match database.query(literal_query(right)).await {
            Ok(rows) => rows
                .to_values()
                .map_err(|error| format!("decode: {error:?}")),
            Err(error) => Err(format!("public SQL literal WHERE rejected: {error:?}")),
        })
    } else {
        None
    };
    (on_rows, literal_rows)
}

fn array(values: &[i32]) -> Value {
    Value::Array(values.iter().copied().map(Value::I32).collect())
}

/// Alice inserts a left row and Bob a right row: SQL equality compares complete
/// required arrays, preserving order, emptiness and multiplicity, with one
/// weight-one ID pair only for equal values. Scalar equality is the control.
#[futures_test::test]
async fn sql_array_equijoin_compares_whole_values() {
    let pair = vec![(vec![Value::U64(1), Value::U64(2)], 1)];
    let cases: [(&str, &[i32], &[i32], bool); 7] = [
        ("overlapping_unequal", &[1, 2], &[2, 3], false),
        ("reversed", &[1, 2], &[2, 1], false),
        ("equal", &[1, 2], &[1, 2], true),
        ("equal_empty", &[], &[], true),
        ("disjoint", &[1, 2], &[3, 4], false),
        ("duplicate_varying", &[1, 1], &[1], false),
        ("equal_duplicates", &[1, 1], &[1, 1], true),
    ];
    let mut observations = Vec::new();
    for (name, left, right, equal) in cases {
        let expected = if equal { pair.clone() } else { Vec::new() };
        let literal_expected = if equal {
            vec![(vec![Value::U64(1)], 1)]
        } else {
            Vec::new()
        };
        let (on_rows, literal_rows) =
            observe(ColumnType::I32.array_of(), array(left), array(right), true).await;
        println!(
            "CASE {name}: left={left:?} right={right:?} expected={expected:?} ON={on_rows:?} LITERAL_EXPECTED={literal_expected:?} LITERAL_WHERE={literal_rows:?}"
        );
        observations.push((name, expected, on_rows, literal_expected, literal_rows));
    }
    for (name, left, right, equal) in [
        ("scalar_equal", 1, 1, true),
        ("scalar_mismatch", 1, 2, false),
    ] {
        let expected = if equal { pair.clone() } else { Vec::new() };
        let (on_rows, literal_rows) =
            observe(ColumnType::I32, Value::I32(left), Value::I32(right), false).await;
        println!("CASE {name}: left={left:?} right={right:?} expected={expected:?} ON={on_rows:?}");
        observations.push((name, expected, on_rows, Vec::new(), literal_rows));
    }

    // Gather every outcome before failing so one false positive cannot hide
    // empty-array omission, duplicate weights, or the passing scalar controls.
    let mut failures = Vec::new();
    for (name, expected, on_rows, literal_expected, literal_rows) in observations {
        if on_rows.as_ref() != Ok(&expected) {
            failures.push(format!(
                "{name}: expected {expected:?}, ON actual {on_rows:?}"
            ));
        }
        if let Some(literal_rows) = literal_rows {
            if literal_rows.as_ref() != Ok(&literal_expected) {
                failures.push(format!(
                    "{name}: expected {literal_expected:?}, literal WHERE actual {literal_rows:?}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "SQL whole-value equality violations:\n{}",
        failures.join("\n")
    );
}
