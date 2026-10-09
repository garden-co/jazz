//! Whole-value array equality through Groove's public SQL-ish query API.

use groove::db::{
    Database, DatabaseBatch, Error as DatabaseError, GraphBuilder, IvmRuntimeError, PrimaryKeyValue,
};
use groove::queries::{
    BinaryOp, ColumnRef, Cte, Expr, JoinConstraint, JoinKind, Query, Select, SelectItem,
    SetOperator, SetQuantifier, SetQuery, TableRef, WithQuery,
};
use groove::records::{EnumCase, EnumSchema, RecordDescriptor, ScalarEnumSchema, Value};
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
    query_keys(&[join_column])
}

fn query_keys(columns: &[&str]) -> Query {
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
        constraint: JoinConstraint::On(
            columns
                .iter()
                .map(|column| equality(column))
                .reduce(|left, right| Expr::binary(left, BinaryOp::And, right))
                .unwrap(),
        ),
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
    let mut database = open_database(codes_type.clone(), codes_type, ColumnType::I32).await;
    let mut batch = database.open_batch();
    batch.insert("left_items", vec![Value::U64(1), left, Value::I32(7)]);
    batch.insert(
        "right_items",
        vec![Value::U64(2), right.clone(), Value::I32(7)],
    );
    commit(&mut database, batch).await;

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
        if let Some(literal_rows) = literal_rows
            && literal_rows.as_ref() != Ok(&literal_expected)
        {
            failures.push(format!(
                "{name}: expected {literal_expected:?}, literal WHERE actual {literal_rows:?}"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "SQL whole-value equality violations:\n{}",
        failures.join("\n")
    );
}

fn pair() -> WeightedRows {
    vec![(vec![Value::U64(1), Value::U64(2)], 1)]
}

fn some(value: Value) -> Value {
    Value::Nullable(Some(Box::new(value)))
}

async fn open_database(left: ColumnType, right: ColumnType, bucket: ColumnType) -> Database {
    let schema = DatabaseSchema::new([("left_items", left), ("right_items", right)].map(
        |(name, codes)| {
            TableSchema::new_with_bound_registries(
                name,
                [
                    ColumnSchema::new("row_key", ColumnType::U64),
                    ColumnSchema::new("codes", codes),
                    ColumnSchema::new("bucket", bucket.clone()),
                ],
            )
            .with_primary_key(PrimaryKey::new("row_key", IntegerKeyType::U64))
        },
    ));
    Database::new(
        schema,
        MemoryStorage::new(&["left_items", "right_items"]).unwrap(),
    )
    .await
    .unwrap()
}

async fn commit(database: &mut Database, batch: DatabaseBatch) {
    let persisted = database.apply_batch(batch).await.unwrap().persist().await;
    database.finish_persistence(persisted).unwrap();
}

async fn insert(database: &mut Database, table: &str, id: u64, value: Value) {
    let mut batch = database.open_batch();
    batch.insert(table, vec![Value::U64(id), value, Value::I32(7)]);
    commit(database, batch).await;
}

fn parameter_query() -> Query {
    Query::Select(Box::new(
        Select::new([SelectItem::expr(Expr::column("row_key"))])
            .from([TableRef::named("left_items")])
            .where_(Expr::binary(
                Expr::column("codes"),
                BinaryOp::Eq,
                Expr::parameter("codes"),
            )),
    ))
}

/// Alice and Bob compare complete scalar and recursive keys. Framing preserves
/// array, tuple and variable-width boundaries, including empty children and NUL
/// bytes; numeric equality treats signed zeros alike without changing order.
#[futures_test::test]
async fn sql_recursive_keys_preserve_structure_and_variable_boundaries() {
    let nested =
        |values: &[&[i32]]| Value::Array(values.iter().map(|values| array(values)).collect());
    let strings = |values: &[&str]| {
        Value::Array(
            values
                .iter()
                .map(|value| Value::String((*value).into()))
                .collect(),
        )
    };
    let bytes = |values: &[&[u8]]| {
        Value::Array(
            values
                .iter()
                .map(|value| Value::Bytes(value.to_vec()))
                .collect(),
        )
    };
    let tuple_type = ColumnType::Tuple(vec![ColumnType::I32, ColumnType::U64]);
    let tuple = |a, b| Value::Tuple(vec![Value::I32(a), Value::U64(b)]);
    let enum_type = ColumnType::EnumTag(
        ScalarEnumSchema::new("state", ["open", "closed"])
            .unwrap()
            .with_registry_id(41),
    );
    for (name, ty, left, right, equal) in [
        (
            "nested_empty",
            ColumnType::I32.array_of().array_of(),
            nested(&[&[], &[]]),
            nested(&[&[], &[]]),
            true,
        ),
        (
            "nested_length",
            ColumnType::I32.array_of().array_of(),
            nested(&[&[], &[]]),
            nested(&[&[]]),
            false,
        ),
        (
            "nested_order",
            ColumnType::I32.array_of().array_of(),
            nested(&[&[1], &[2, 3]]),
            nested(&[&[2, 3], &[1]]),
            false,
        ),
        (
            "nested_duplicates",
            ColumnType::I32.array_of().array_of(),
            nested(&[&[1, 1], &[]]),
            nested(&[&[1, 1], &[]]),
            true,
        ),
        (
            "string_equal",
            ColumnType::String.array_of(),
            strings(&["", "a\0b", "c"]),
            strings(&["", "a\0b", "c"]),
            true,
        ),
        (
            "string_boundary",
            ColumnType::String.array_of(),
            strings(&["a\0", "b"]),
            strings(&["a", "\0b"]),
            false,
        ),
        (
            "bytes_equal",
            ColumnType::Bytes.array_of(),
            bytes(&[&[], &[0, 1], &[2]]),
            bytes(&[&[], &[0, 1], &[2]]),
            true,
        ),
        (
            "bytes_boundary",
            ColumnType::Bytes.array_of(),
            bytes(&[&[0], &[1, 2]]),
            bytes(&[&[0, 1], &[2]]),
            false,
        ),
        (
            "tuple_equal",
            tuple_type.clone().array_of(),
            Value::Array(vec![tuple(-1, 2), tuple(3, 4)]),
            Value::Array(vec![tuple(-1, 2), tuple(3, 4)]),
            true,
        ),
        (
            "tuple_mismatch",
            tuple_type.array_of(),
            Value::Array(vec![tuple(-1, 2)]),
            Value::Array(vec![tuple(-1, 3)]),
            false,
        ),
        (
            "bool_array",
            ColumnType::Bool.array_of(),
            Value::Array(vec![Value::Bool(true), Value::Bool(false)]),
            Value::Array(vec![Value::Bool(true), Value::Bool(false)]),
            true,
        ),
        (
            "enum_array",
            enum_type.array_of(),
            Value::Array(vec![Value::EnumTag(0), Value::EnumTag(1)]),
            Value::Array(vec![Value::EnumTag(0), Value::EnumTag(1)]),
            true,
        ),
    ] {
        let (actual, _) = observe(ty, left, right, false).await;
        assert_eq!(
            actual.unwrap(),
            if equal { pair() } else { vec![] },
            "{name}"
        );
    }

    for (ty, positive, negative) in zero_cases() {
        let (actual, _) = observe(ty, positive, negative, false).await;
        assert_eq!(actual.unwrap(), pair());
    }
    let (actual, _) = observe(
        ColumnType::F64.array_of(),
        float_array(&[0.0, 1.0]),
        float_array(&[1.0, -0.0]),
        false,
    )
    .await;
    assert!(actual.unwrap().is_empty());

    for (ty, value) in [
        (ColumnType::Tuple(vec![]), Value::Tuple(vec![])),
        (
            ColumnType::Tuple(vec![ColumnType::Tuple(vec![]), ColumnType::I32]).array_of(),
            Value::Array(vec![Value::Tuple(vec![
                Value::Tuple(vec![]),
                Value::I32(7),
            ])]),
        ),
    ] {
        let (actual, _) = observe(ty, value.clone(), value, false).await;
        assert_eq!(actual.unwrap(), pair());
    }
}

/// Alice and Bob use nullable whole-array keys. SQL NULL never joins; mixed
/// declared nullability remains rejected rather than coerced.
#[futures_test::test]
async fn sql_nullable_arrays_guard_null_and_preserve_declared_types() {
    for (left, right, equal) in [
        (some(array(&[])), some(array(&[])), true),
        (some(array(&[1, 2])), some(array(&[1, 2])), true),
        (some(array(&[1, 2])), some(array(&[2, 1])), false),
        (Value::Nullable(None), Value::Nullable(None), false),
        (Value::Nullable(None), some(array(&[])), false),
        (some(array(&[])), Value::Nullable(None), false),
    ] {
        let (actual, _) = observe(ColumnType::I32.array_of().nullable(), left, right, false).await;
        assert_eq!(actual.unwrap(), if equal { pair() } else { vec![] });
    }
    let mut database = open_database(
        ColumnType::I32.array_of().nullable(),
        ColumnType::I32.array_of(),
        ColumnType::I32,
    )
    .await;
    assert!(matches!(
        database.query(query("codes")).await,
        Err(DatabaseError::QueryPlanning(_))
    ));
}

/// Alice and Bob join on two keys. Neither scalar-plus-array nor array-plus-
/// array keys expand memberships or multiply the one matching pair's weight.
#[futures_test::test]
async fn sql_composite_keys_compare_each_complete_value_once() {
    for (bucket_type, left_bucket, right_bucket, equal) in [
        (ColumnType::I32, Value::I32(7), Value::I32(7), true),
        (ColumnType::I32, Value::I32(7), Value::I32(8), false),
        (
            ColumnType::I32.array_of(),
            array(&[3, 4]),
            array(&[3, 4]),
            true,
        ),
        (
            ColumnType::I32.array_of(),
            array(&[3, 4]),
            array(&[4, 3]),
            false,
        ),
        (ColumnType::I32.array_of(), array(&[]), array(&[]), true),
    ] {
        let ty = ColumnType::I32.array_of();
        let mut database = open_database(ty.clone(), ty, bucket_type).await;
        let mut batch = database.open_batch();
        batch.insert(
            "left_items",
            vec![Value::U64(1), array(&[1, 2]), left_bucket],
        );
        batch.insert(
            "right_items",
            vec![Value::U64(2), array(&[1, 2]), right_bucket],
        );
        commit(&mut database, batch).await;
        assert_eq!(
            database
                .query(query_keys(&["codes", "bucket"]))
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            if equal { pair() } else { vec![] }
        );
    }
}

/// Alice declares unsupported key children; Bob cannot make them eligible by
/// supplying empty arrays. Both one-shot and prepared SQL reject by type.
#[futures_test::test]
async fn sql_unsupported_declared_children_reject_even_without_values() {
    let payload = RecordDescriptor::new([("value", ColumnType::I32)]);
    let tagged = EnumSchema::new("payload", [EnumCase::new("item", payload)])
        .unwrap()
        .with_registry_id(42);
    for ty in [
        ColumnType::I32.nullable().array_of(),
        ColumnType::I32.nullable().array_of().array_of(),
        ColumnType::Tuple(vec![ColumnType::I32.nullable()]).array_of(),
        ColumnType::Record(Box::new(payload)).array_of(),
        ColumnType::Enum(Box::new(tagged)).array_of(),
        ColumnType::Tuple(vec![ColumnType::F64]).array_of(),
        ColumnType::Tuple(vec![
            ColumnType::I32,
            ColumnType::Tuple(vec![ColumnType::F64]),
        ])
        .array_of(),
    ] {
        for insert_empty in [false, true] {
            let mut database = open_database(ty.clone(), ty.clone(), ColumnType::I32).await;
            if insert_empty {
                insert(&mut database, "left_items", 1, Value::Array(vec![])).await;
                insert(&mut database, "right_items", 2, Value::Array(vec![])).await;
            }
            assert!(matches!(database.query(query("codes")).await,
                Err(DatabaseError::IvmRuntime(IvmRuntimeError::UnsupportedWholeValueType(actual))) if actual == ty));
            assert!(matches!(database.prepare_query(parameter_query()).await,
                Err(DatabaseError::IvmRuntime(IvmRuntimeError::UnsupportedWholeValueType(actual))) if actual == ty));
        }
    }
    // Zero-width elements cannot even round-trip an empty array through the
    // existing codec. Exercise declaration rejection without writing rows.
    for ty in [
        ColumnType::Tuple(vec![]).array_of(),
        ColumnType::Tuple(vec![ColumnType::Tuple(vec![])]).array_of(),
        ColumnType::Tuple(vec![]).array_of().array_of().nullable(),
    ] {
        let mut database = open_database(ty.clone(), ty.clone(), ColumnType::I32).await;
        assert!(matches!(database.query(query("codes")).await,
            Err(DatabaseError::IvmRuntime(IvmRuntimeError::UnsupportedWholeValueType(actual))) if actual == ty));
        assert!(matches!(database.prepare_query(parameter_query()).await,
            Err(DatabaseError::IvmRuntime(IvmRuntimeError::UnsupportedWholeValueType(actual))) if actual == ty));
    }
}

fn float_array(values: &[f64]) -> Value {
    Value::Array(values.iter().copied().map(Value::F64).collect())
}

fn zero_cases() -> Vec<(ColumnType, Value, Value)> {
    vec![
        (ColumnType::F64, Value::F64(0.0), Value::F64(-0.0)),
        (
            ColumnType::F64.array_of(),
            float_array(&[0.0, 1.0]),
            float_array(&[-0.0, 1.0]),
        ),
        (
            ColumnType::F64.array_of().array_of(),
            Value::Array(vec![float_array(&[0.0]), float_array(&[])]),
            Value::Array(vec![float_array(&[-0.0]), float_array(&[])]),
        ),
        (
            ColumnType::F64.array_of().nullable(),
            some(float_array(&[0.0])),
            some(float_array(&[-0.0])),
        ),
    ]
}

fn id_pairs(rows: WeightedRows) -> std::collections::BTreeMap<(u64, u64), i64> {
    let mut pairs = std::collections::BTreeMap::new();
    for (values, weight) in rows {
        let [Value::U64(left), Value::U64(right)] = values.as_slice() else {
            panic!("expected public ID pair")
        };
        *pairs.entry((*left, *right)).or_insert(0) += weight;
    }
    pairs.retain(|_, weight| *weight != 0);
    pairs
}

/// Alice and Bob insert both sides in one tick, then update and delete keys.
/// Each maintained delta is exact and accumulated state equals fresh SQL.
#[futures_test::test]
async fn sql_array_join_maintenance_matches_fresh_queries() {
    let ty = ColumnType::I32.array_of();
    let mut database = open_database(ty.clone(), ty, ColumnType::I32).await;
    let subscription = database.subscribe_query(query("codes")).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    let mut current = std::collections::BTreeMap::new();
    for step in 0..5 {
        let mut batch = database.open_batch();
        let expected = match step {
            0 => {
                batch.insert("left_items", vec![Value::U64(1), array(&[]), Value::I32(7)]);
                batch.insert(
                    "right_items",
                    vec![Value::U64(2), array(&[]), Value::I32(7)],
                );
                vec![((1, 2), 1)]
            }
            1 => {
                batch.insert("left_items", vec![Value::U64(3), array(&[]), Value::I32(7)]);
                batch.insert(
                    "right_items",
                    vec![Value::U64(4), array(&[]), Value::I32(7)],
                );
                vec![((1, 4), 1), ((3, 2), 1), ((3, 4), 1)]
            }
            2 => {
                batch.update(
                    "right_items",
                    vec![Value::U64(2), array(&[1, 2]), Value::I32(7)],
                );
                vec![((1, 2), -1), ((3, 2), -1)]
            }
            3 => {
                batch.update(
                    "left_items",
                    vec![Value::U64(1), array(&[1, 2]), Value::I32(7)],
                );
                vec![((1, 4), -1), ((1, 2), 1)]
            }
            _ => {
                batch.delete("right_items", PrimaryKeyValue::U64(4));
                vec![((3, 4), -1)]
            }
        };
        commit(&mut database, batch).await;
        let deltas = id_pairs(subscription.recv().unwrap().to_values().unwrap());
        assert_eq!(deltas, expected.into_iter().collect(), "step {step}");
        for (key, weight) in deltas {
            *current.entry(key).or_insert(0) += weight;
        }
        current.retain(|_, weight| *weight != 0);
        assert_eq!(
            current,
            id_pairs(
                database
                    .query(query("codes"))
                    .await
                    .unwrap()
                    .to_values()
                    .unwrap()
            )
        );
    }
}

/// Alice and Bob bind opposite signed zeros to the same SQL shape. Raw binding
/// identities stay isolated through named/positional binds and drop/rebind,
/// while each subscriber receives SQL-equal rows exactly once.
#[futures_test::test]
async fn sql_binding_routes_isolate_signed_zero_through_lifecycle() {
    for (ty, positive, negative) in zero_cases() {
        let mut database = open_database(ty.clone(), ty, ColumnType::I32).await;
        insert(&mut database, "left_items", 1, positive.clone()).await;
        let prepared = database.prepare_query(parameter_query()).await.unwrap();
        let alice = database
            .bind(&prepared, &[("codes", positive.clone())])
            .await
            .unwrap();
        assert_eq!(
            alice.recv().unwrap().to_values().unwrap(),
            [(vec![Value::U64(1)], 1)]
        );
        let bob = database
            .bind_shape_one_sink_with_output(
                prepared.id(),
                std::slice::from_ref(&negative),
                *prepared.output(),
            )
            .await
            .unwrap();
        assert_eq!(
            bob.recv().unwrap().to_values().unwrap(),
            [(vec![Value::U64(1)], 1)]
        );
        assert!(
            alice.try_recv().is_err(),
            "attaching Bob must not alter Alice's weight"
        );
        let alice_again = database
            .bind(&prepared, &[("codes", positive.clone())])
            .await
            .unwrap();
        assert_eq!(
            alice_again.recv().unwrap().to_values().unwrap(),
            [(vec![Value::U64(1)], 1)]
        );
        assert!(database.unsubscribe(alice.id()));
        assert!(database.unsubscribe(alice_again.id()));
        let rebound = database
            .bind(&prepared, &[("codes", positive.clone())])
            .await
            .unwrap();
        assert_eq!(
            rebound.recv().unwrap().to_values().unwrap(),
            [(vec![Value::U64(1)], 1)]
        );
        insert(&mut database, "left_items", 2, negative.clone()).await;
        for subscription in [&bob, &rebound] {
            assert_eq!(
                subscription.recv().unwrap().to_values().unwrap(),
                [(vec![Value::U64(2)], 1)]
            );
        }
        drop(rebound);
        insert(&mut database, "left_items", 3, positive.clone()).await;
        assert_eq!(
            bob.recv().unwrap().to_values().unwrap(),
            [(vec![Value::U64(3)], 1)]
        );
        let rebound = database
            .bind(&prepared, &[("codes", positive)])
            .await
            .unwrap();
        let mut rows = rebound.recv().unwrap().to_values().unwrap();
        rows.sort_by_key(|(values, _)| match values[0] {
            Value::U64(id) => id,
            _ => unreachable!(),
        });
        assert_eq!(
            rows,
            [
                (vec![Value::U64(1)], 1),
                (vec![Value::U64(2)], 1),
                (vec![Value::U64(3)], 1)
            ]
        );
        let mut batch = database.open_batch();
        batch.delete("left_items", PrimaryKeyValue::U64(1));
        commit(&mut database, batch).await;
        for subscription in [&bob, &rebound] {
            assert_eq!(
                subscription.recv().unwrap().to_values().unwrap(),
                [(vec![Value::U64(1)], -1)]
            );
        }
    }
}

/// Alice's graph-based reference relation and Bob's policy correlation still
/// use element membership; SQL whole-value keys must not change either route.
#[futures_test::test]
async fn generic_exact_and_policy_joins_keep_array_membership() {
    for policy in [false, true] {
        let mut database =
            open_database(ColumnType::I32.array_of(), ColumnType::I32, ColumnType::I32).await;
        insert(&mut database, "left_items", 1, array(&[1, 2, 2])).await;
        insert(&mut database, "right_items", 2, Value::I32(2)).await;
        let left = GraphBuilder::table("left_items");
        let right = GraphBuilder::table("right_items");
        let join = if policy {
            GraphBuilder::policy_join(left, right, ["codes"], ["codes"])
        } else {
            GraphBuilder::join(left, right, ["codes"], ["codes"])
        };
        assert_eq!(
            database
                .query_graph(join.project(["left.row_key", "right.row_key"]))
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            pair()
        );
    }
}

/// Alice and Bob prepare ordinary graph membership queries with different
/// array bindings. Generic routing retains complete literal ownership while
/// its upstream membership weights remain distinct from SQL whole equality.
#[futures_test::test]
async fn generic_prepared_array_routes_preserve_membership_weights() {
    let ty = ColumnType::I32.array_of();
    let mut database = open_database(ty.clone(), ty.clone(), ColumnType::I32).await;
    insert(&mut database, "left_items", 1, array(&[1, 2])).await;
    let descriptor = RecordDescriptor::new([("codes", ty)]);
    let graph = GraphBuilder::join(
        GraphBuilder::table("left_items"),
        GraphBuilder::binding_source("membership", descriptor),
        ["codes"],
        ["codes"],
    )
    .project_fields([
        groove::ivm::ProjectField::renamed("left.row_key", "row_key"),
        groove::ivm::ProjectField::renamed("right.codes", "route"),
    ]);
    let shape = database
        .prepare_one_sink(graph, "membership", descriptor, ["route"])
        .await
        .unwrap();
    let output = RecordDescriptor::new([("row_key", ColumnType::U64)]);
    let alice = database
        .bind_shape_one_sink_with_output(shape.id(), &[array(&[1, 2])], output)
        .await
        .unwrap();
    assert_eq!(
        alice.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1)], 2)]
    );
    let bob = database
        .bind_shape_one_sink_with_output(shape.id(), &[array(&[2, 3])], output)
        .await
        .unwrap();
    assert_eq!(
        bob.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1)], 1)]
    );
    assert!(alice.try_recv().is_err());
    insert(&mut database, "left_items", 2, array(&[1, 2])).await;
    assert_eq!(
        alice.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(2)], 2)]
    );
    assert_eq!(
        bob.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(2)], 1)]
    );
}

fn payload_query(value: Expr) -> Query {
    Query::Select(Box::new(
        Select::new([SelectItem::expr(Expr::Column(ColumnRef::qualified(
            ["left_items"],
            "codes",
        )))])
        .from([TableRef::named("left_items")])
        .where_(Expr::binary(
            Expr::Column(ColumnRef::qualified(["left_items"], "codes")),
            BinaryOp::Eq,
            value,
        )),
    ))
}

fn float_bits(value: &Value, bits: &mut Vec<u64>) {
    match value {
        Value::F64(value) => bits.push(value.to_bits()),
        Value::Array(values) | Value::Tuple(values) => {
            for value in values {
                float_bits(value, bits);
            }
        }
        Value::Nullable(Some(value)) => float_bits(value, bits),
        _ => panic!("expected present floating-point payload"),
    }
}

fn float_rows(rows: WeightedRows) -> Vec<(Vec<u64>, i64)> {
    let mut rows = rows
        .into_iter()
        .map(|(values, weight)| {
            let mut bits = Vec::new();
            for value in values {
                float_bits(&value, &mut bits);
            }
            (bits, weight)
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// Alice projects the constrained column under its parameter's name. Bob
/// binds the opposite zero: routing must use Bob's original binding carrier,
/// while the public payload keeps Alice's stored bits.
/// table +0 -> SQL-equal join <- bob -0 -> table +0 at weight 1
#[futures_test::test]
async fn sql_same_name_float_projection_keeps_raw_binding_and_table_payload() {
    for (ty, positive, negative) in zero_cases() {
        let mut database = open_database(ty.clone(), ty, ColumnType::I32).await;
        insert(&mut database, "left_items", 1, positive.clone()).await;
        let prepared = database
            .prepare_query(payload_query(Expr::parameter("codes")))
            .await
            .unwrap();
        let alice = database
            .bind(&prepared, &[("codes", positive.clone())])
            .await
            .unwrap();
        let bob = database
            .bind_shape_one_sink_with_output(
                prepared.id(),
                std::slice::from_ref(&negative),
                *prepared.output(),
            )
            .await
            .unwrap();
        let mut plus = Vec::new();
        float_bits(&positive, &mut plus);
        let mut minus = Vec::new();
        float_bits(&negative, &mut minus);
        for subscription in [&alice, &bob] {
            assert_eq!(
                float_rows(subscription.recv().unwrap().to_values().unwrap()),
                [(plus.clone(), 1)]
            );
        }
        assert!(database.unsubscribe(bob.id()));
        let bob = database
            .bind(&prepared, &[("codes", negative.clone())])
            .await
            .unwrap();
        assert_eq!(
            float_rows(bob.recv().unwrap().to_values().unwrap()),
            [(plus.clone(), 1)]
        );
        let mut batch = database.open_batch();
        batch.update(
            "left_items",
            vec![Value::U64(1), negative.clone(), Value::I32(7)],
        );
        commit(&mut database, batch).await;
        let mut expected = vec![(plus, -1), (minus.clone(), 1)];
        expected.sort();
        for subscription in [&alice, &bob] {
            assert_eq!(
                float_rows(subscription.recv().unwrap().to_values().unwrap()),
                expected
            );
        }
        assert_eq!(
            float_rows(
                database
                    .query(payload_query(Expr::Literal(negative)))
                    .await
                    .unwrap()
                    .to_values()
                    .unwrap()
            ),
            [(minus, 1)]
        );
    }
}

/// Alice uses enum ordinals and Bob uses accepted labels for the same raw
/// binding. Scalar, array, tuple and top-nullable labels share one derivation,
/// not extra refcount weights; invalid labels remain errors.
#[futures_test::test]
async fn sql_enum_labels_share_canonical_binding_identity_and_refcounts() {
    let tag = ColumnType::EnumTag(
        ScalarEnumSchema::new("state", ["open", "closed"])
            .unwrap()
            .with_registry_id(43),
    );
    for shape in 0..4 {
        let (ty, wrap): (ColumnType, fn(Value) -> Value) = match shape {
            0 => (tag.clone(), |value| value),
            1 => (tag.clone().array_of(), |value| Value::Array(vec![value])),
            2 => (
                ColumnType::Tuple(vec![tag.clone(), ColumnType::I32]),
                |value| Value::Tuple(vec![value, Value::I32(7)]),
            ),
            _ => (tag.clone().array_of().nullable(), |value| {
                some(Value::Array(vec![value]))
            }),
        };
        let ordinal = wrap(Value::EnumTag(0));
        let label = wrap(Value::String("open".into()));
        let mut database = open_database(ty.clone(), ty, ColumnType::I32).await;
        insert(&mut database, "left_items", 1, ordinal.clone()).await;
        let prepared = database.prepare_query(parameter_query()).await.unwrap();
        let alice = database
            .bind(&prepared, &[("codes", ordinal.clone())])
            .await
            .unwrap();
        let bob = database
            .bind_shape_one_sink_with_output(
                prepared.id(),
                std::slice::from_ref(&label),
                *prepared.output(),
            )
            .await
            .unwrap();
        for subscription in [&alice, &bob] {
            assert_eq!(
                subscription.recv().unwrap().to_values().unwrap(),
                [(vec![Value::U64(1)], 1)],
                "shape {shape}"
            );
        }
        assert!(alice.try_recv().is_err());
        assert!(
            database
                .bind(
                    &prepared,
                    &[("codes", wrap(Value::String("unknown".into())))]
                )
                .await
                .is_err()
        );
        insert(&mut database, "left_items", 2, ordinal.clone()).await;
        for subscription in [&alice, &bob] {
            assert_eq!(
                subscription.recv().unwrap().to_values().unwrap(),
                [(vec![Value::U64(2)], 1)]
            );
        }
        assert!(database.unsubscribe(alice.id()));
        insert(&mut database, "left_items", 3, ordinal.clone()).await;
        assert_eq!(
            bob.recv().unwrap().to_values().unwrap(),
            [(vec![Value::U64(3)], 1)]
        );
        let rebound = database
            .bind(&prepared, &[("codes", ordinal)])
            .await
            .unwrap();
        let mut rows = rebound.recv().unwrap().to_values().unwrap();
        rows.sort_by_key(|(values, _)| match values[0] {
            Value::U64(id) => id,
            _ => unreachable!(),
        });
        assert_eq!(
            rows,
            [
                (vec![Value::U64(1)], 1),
                (vec![Value::U64(2)], 1),
                (vec![Value::U64(3)], 1)
            ]
        );
    }
}

fn repeated_origin_query(value: Expr) -> Query {
    let filtered = Query::Select(Box::new(
        Select::new([SelectItem::expr(Expr::column("row_key"))])
            .from([TableRef::named("left_items")])
            .where_(Expr::binary(Expr::column("codes"), BinaryOp::Eq, value)),
    ));
    let joined = Query::Select(Box::new(
        Select::new([
            SelectItem::aliased(Expr::Column(ColumnRef::qualified(["l"], "id")), "left_id"),
            SelectItem::aliased(Expr::Column(ColumnRef::qualified(["r"], "id")), "right_id"),
        ])
        .from([TableRef::Join {
            left: Box::new(TableRef::named("filtered").aliased("l")),
            right: Box::new(TableRef::named("filtered").aliased("r")),
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(Expr::binary(
                Expr::Column(ColumnRef::qualified(["l"], "id")),
                BinaryOp::Eq,
                Expr::Column(ColumnRef::qualified(["r"], "id")),
            )),
        }]),
    ));
    Query::With(Box::new(WithQuery::new(
        [Cte::new("filtered", filtered).with_columns(["id"])],
        joined,
    )))
}

/// Alice and Bob bind opposite zeros to two independent occurrences of the
/// same CTE/parameter. Both original carriers must select the same binding.
/// p -> CTE l --join-- CTE r <- p; AND both identities -> one ID pair
#[futures_test::test]
async fn sql_repeated_parameter_origins_route_every_carrier_through_lifecycle() {
    for (ty, positive, negative) in zero_cases() {
        let mut database = open_database(ty.clone(), ty, ColumnType::I32).await;
        insert(&mut database, "left_items", 1, positive.clone()).await;
        let prepared = database
            .prepare_query(repeated_origin_query(Expr::parameter("p")))
            .await
            .unwrap();
        let alice = database
            .bind(&prepared, &[("p", positive.clone())])
            .await
            .unwrap();
        assert_eq!(
            id_pairs(alice.recv().unwrap().to_values().unwrap()),
            [((1, 1), 1)].into()
        );
        let bob = database
            .bind_shape_one_sink_with_output(
                prepared.id(),
                std::slice::from_ref(&negative),
                *prepared.output(),
            )
            .await
            .unwrap();
        assert_eq!(
            id_pairs(bob.recv().unwrap().to_values().unwrap()),
            [((1, 1), 1)].into()
        );
        assert!(
            alice.try_recv().is_err(),
            "other binding must not inject a mixed-origin pair"
        );
        assert!(database.unsubscribe(alice.id()));
        let alice = database
            .bind(&prepared, &[("p", positive.clone())])
            .await
            .unwrap();
        assert_eq!(
            id_pairs(alice.recv().unwrap().to_values().unwrap()),
            [((1, 1), 1)].into()
        );
        insert(&mut database, "left_items", 2, positive.clone()).await;
        for subscription in [&alice, &bob] {
            assert_eq!(
                id_pairs(subscription.recv().unwrap().to_values().unwrap()),
                [((2, 2), 1)].into()
            );
        }
        let mut batch = database.open_batch();
        batch.delete("left_items", PrimaryKeyValue::U64(1));
        commit(&mut database, batch).await;
        for subscription in [&alice, &bob] {
            assert_eq!(
                id_pairs(subscription.recv().unwrap().to_values().unwrap()),
                [((1, 1), -1)].into()
            );
        }
        drop(bob);
        insert(&mut database, "left_items", 3, positive.clone()).await;
        assert_eq!(
            id_pairs(alice.recv().unwrap().to_values().unwrap()),
            [((3, 3), 1)].into()
        );
        let bob = database.bind(&prepared, &[("p", negative)]).await.unwrap();
        assert_eq!(
            id_pairs(bob.recv().unwrap().to_values().unwrap()),
            [((2, 2), 1), ((3, 3), 1)].into()
        );
        assert_eq!(
            id_pairs(
                database
                    .query(repeated_origin_query(Expr::Literal(positive)))
                    .await
                    .unwrap()
                    .to_values()
                    .unwrap()
            ),
            [((2, 2), 1), ((3, 3), 1)].into()
        );
    }
}

fn binding_cte(name: &str, value: Expr) -> Cte {
    Cte::new(
        name,
        Query::Select(Box::new(
            Select::new([
                SelectItem::expr(Expr::column("row_key")),
                SelectItem::expr(Expr::column("bucket")),
            ])
            .from([TableRef::named("left_items")])
            .where_(Expr::binary(Expr::column("codes"), BinaryOp::Eq, value)),
        )),
    )
    .with_columns(["id", "bucket"])
}

fn origin_arm(names: &[&str]) -> Query {
    let mut table = TableRef::named(names[0]).aliased("a0");
    for (index, name) in names.iter().enumerate().skip(1) {
        let alias = format!("a{index}");
        table = TableRef::Join {
            left: Box::new(table),
            right: Box::new(TableRef::named(*name).aliased(&alias)),
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(Expr::binary(
                Expr::Column(ColumnRef::qualified(["a0"], "bucket")),
                BinaryOp::Eq,
                Expr::Column(ColumnRef::qualified([alias.as_str()], "bucket")),
            )),
        };
    }
    Query::Select(Box::new(
        Select::new([SelectItem::aliased(
            Expr::Column(ColumnRef::qualified(["a0"], "id")),
            "id",
        )])
        .from([table]),
    ))
}

fn union_origins(p: Expr, q: Option<Expr>) -> Query {
    let (left, right) = if q.is_some() {
        (
            origin_arm(&["fp", "fp", "fq"]),
            origin_arm(&["fp", "fq", "fq"]),
        )
    } else {
        (origin_arm(&["fp", "fp"]), origin_arm(&["fp"]))
    };
    let union = Query::Set(Box::new(SetQuery {
        left,
        right,
        op: SetOperator::Union,
        quantifier: SetQuantifier::All,
    }));
    let mut ctes = vec![binding_cte("fp", p)];
    if let Some(q) = q {
        ctes.push(binding_cte("fq", q));
    }
    ctes.push(Cte::new("merged", union));
    Query::With(Box::new(WithQuery::new(
        ctes,
        Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("id"))]).from([TableRef::named("merged")]),
        )),
    )))
}

fn id_weights(rows: WeightedRows) -> std::collections::BTreeMap<u64, i64> {
    let mut result = std::collections::BTreeMap::new();
    for (values, weight) in rows {
        let [Value::U64(id)] = values.as_slice() else {
            panic!("expected one public ID")
        };
        *result.entry(*id).or_insert(0) += weight;
    }
    result.retain(|_, weight| *weight != 0);
    result
}

/// Alice and Bob bind opposite zeros across a UNION whose arms retain two
/// versus one actual origins. Outer CTE/SELECT retention must preserve both
/// logical lanes; both arms contribute once, never mixed-binding derivations.
#[futures_test::test]
async fn sql_union_unequal_origin_counts_preserve_wrapped_raw_ownership() {
    let mut database = open_database(ColumnType::F64, ColumnType::F64, ColumnType::I32).await;
    insert(&mut database, "left_items", 1, Value::F64(0.0)).await;
    let prepared = database
        .prepare_query(union_origins(Expr::parameter("p"), None))
        .await
        .unwrap();
    let alice = database
        .bind(&prepared, &[("p", Value::F64(0.0))])
        .await
        .unwrap();
    assert_eq!(
        id_weights(alice.recv().unwrap().to_values().unwrap()),
        [(1, 2)].into()
    );
    let bob = database
        .bind_shape_one_sink_with_output(prepared.id(), &[Value::F64(-0.0)], *prepared.output())
        .await
        .unwrap();
    assert_eq!(
        id_weights(bob.recv().unwrap().to_values().unwrap()),
        [(1, 2)].into()
    );
    assert!(alice.try_recv().is_err());
    assert!(database.unsubscribe(bob.id()));
    let bob = database
        .bind(&prepared, &[("p", Value::F64(-0.0))])
        .await
        .unwrap();
    assert_eq!(
        id_weights(bob.recv().unwrap().to_values().unwrap()),
        [(1, 2)].into()
    );
    let mut batch = database.open_batch();
    batch.delete("left_items", PrimaryKeyValue::U64(1));
    commit(&mut database, batch).await;
    for subscription in [&alice, &bob] {
        assert_eq!(
            id_weights(subscription.recv().unwrap().to_values().unwrap()),
            [(1, -2)].into()
        );
    }
    assert!(
        database
            .query(union_origins(Expr::Literal(Value::F64(0.0)), None))
            .await
            .unwrap()
            .is_empty()
    );
}

/// Alice binds p=10 and q=20 to UNION arms with [p,p,q] and [p,q,q].
/// Equal total carrier counts must not align q with p; UNION weights and
/// maintained/fresh results remain exact through replacement and rebind.
#[futures_test::test]
async fn sql_union_lanes_align_by_parameter_not_position() {
    let mut database = open_database(ColumnType::I32, ColumnType::I32, ColumnType::I32).await;
    insert(&mut database, "left_items", 1, Value::I32(10)).await;
    insert(&mut database, "left_items", 2, Value::I32(20)).await;
    let prepared = database
        .prepare_query(union_origins(
            Expr::parameter("p"),
            Some(Expr::parameter("q")),
        ))
        .await
        .unwrap();
    let alice = database
        .bind(&prepared, &[("p", Value::I32(10)), ("q", Value::I32(20))])
        .await
        .unwrap();
    assert_eq!(
        id_weights(alice.recv().unwrap().to_values().unwrap()),
        [(1, 2)].into()
    );
    assert_eq!(
        id_weights(
            database
                .query(union_origins(
                    Expr::Literal(Value::I32(10)),
                    Some(Expr::Literal(Value::I32(20)))
                ))
                .await
                .unwrap()
                .to_values()
                .unwrap()
        ),
        [(1, 2)].into()
    );
    let mut batch = database.open_batch();
    batch.delete("left_items", PrimaryKeyValue::U64(2));
    commit(&mut database, batch).await;
    assert_eq!(
        id_weights(alice.recv().unwrap().to_values().unwrap()),
        [(1, -2)].into()
    );
    insert(&mut database, "left_items", 2, Value::I32(20)).await;
    assert_eq!(
        id_weights(alice.recv().unwrap().to_values().unwrap()),
        [(1, 2)].into()
    );
    assert!(database.unsubscribe(alice.id()));
    let rebound = database
        .bind(&prepared, &[("p", Value::I32(10)), ("q", Value::I32(20))])
        .await
        .unwrap();
    assert_eq!(
        id_weights(rebound.recv().unwrap().to_values().unwrap()),
        [(1, 2)].into()
    );
}

/// Alice's earlier CTE allocates private carriers before Bob's later legal
/// parameter is encountered. User spelling __sql_binding_0 must never be
/// interpreted as private origin metadata or silently reserve a namespace.
#[futures_test::test]
async fn sql_later_parameter_names_do_not_capture_private_carriers() {
    let mut database = open_database(ColumnType::I32, ColumnType::I32, ColumnType::I32).await;
    insert(&mut database, "left_items", 1, Value::I32(10)).await;
    let query = Query::With(Box::new(WithQuery::new(
        [
            binding_cte("early", Expr::parameter("p")),
            binding_cte("later", Expr::parameter("__sql_binding_0")),
        ],
        origin_arm(&["early", "later"]),
    )));
    let prepared = database.prepare_query(query).await.unwrap();
    let subscription = database
        .bind(
            &prepared,
            &[("p", Value::I32(10)), ("__sql_binding_0", Value::I32(10))],
        )
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1)], 1)]
    );
}

/// Alice composes a SQL-planned array join inside a recursive step. Bob's
/// unequal overlapping row never enters the closure; maintained deletion
/// removes the actual fact through the same public recursive graph.
#[futures_test::test]
async fn sql_whole_array_join_composes_with_recursive_evaluation() {
    let ty = ColumnType::I32.array_of();
    let mut database = open_database(ty.clone(), ty, ColumnType::I32).await;
    insert(&mut database, "left_items", 1, array(&[1, 2])).await;
    insert(&mut database, "right_items", 2, array(&[1, 2])).await;
    insert(&mut database, "right_items", 3, array(&[2, 3])).await;
    let schema = DatabaseSchema::new(["left_items", "right_items"].map(|name| {
        TableSchema::new(
            name,
            [
                ColumnSchema::new("row_key", ColumnType::U64),
                ColumnSchema::new("codes", ColumnType::I32.array_of()),
                ColumnSchema::new("bucket", ColumnType::I32),
            ],
        )
        .with_primary_key(PrimaryKey::new("row_key", IntegerKeyType::U64))
    }));
    let sql = groove::ivm::plan_query(&query("codes"), &schema)
        .unwrap()
        .graph;
    let output =
        RecordDescriptor::new([("left_id", ColumnType::U64), ("right_id", ColumnType::U64)]);
    let frontier = GraphBuilder::frontier_source("pairs", output);
    let step = GraphBuilder::join(
        frontier,
        sql.clone(),
        ["left_id", "right_id"],
        ["left_id", "right_id"],
    )
    .project_fields([
        groove::ivm::ProjectField::renamed("right.left_id", "left_id"),
        groove::ivm::ProjectField::renamed("right.right_id", "right_id"),
    ]);
    let recursive = GraphBuilder::recursive(sql, step, "pairs", 8);
    let subscription = database
        .subscribe_one_sink(recursive.clone())
        .await
        .unwrap();
    assert_eq!(subscription.recv().unwrap().to_values().unwrap(), pair());
    assert_eq!(
        database
            .query_graph(recursive)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        pair()
    );
    let mut batch = database.open_batch();
    batch.delete("right_items", PrimaryKeyValue::U64(2));
    commit(&mut database, batch).await;
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(2)], -1)]
    );
}
