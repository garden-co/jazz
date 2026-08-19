use jazz::row_input;
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
    WriteContext,
};
use uuid::Uuid;

fn nullable_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("profiles")
                .nullable_column("name", ColumnType::Text)
                .nullable_column("age", ColumnType::Integer)
                .nullable_column("visits", ColumnType::BigInt)
                .nullable_column("active", ColumnType::Boolean)
                .nullable_fk_column("manager_id", "profiles")
                .nullable_column(
                    "tags",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Text),
                    },
                ),
        )
        .build()
}

fn profiles_query() -> jazz::tools::Query {
    QueryBuilder::new("profiles")
        .select(&["name", "age", "visits", "active", "manager_id", "tags"])
        .build()
}

async fn profile_values(client: &JazzClient, row_id: ObjectId) -> Vec<Value> {
    client
        .query(profiles_query(), None)
        .await
        .expect("query profiles")
        .into_iter()
        .find(|(id, _)| *id == row_id)
        .map(|(_, values)| values)
        .unwrap_or_else(|| panic!("profile row {row_id} should be visible"))
}

fn full_values(manager_id: ObjectId, name: &str, age: i32) -> Vec<Value> {
    vec![
        Value::Text(name.to_owned()),
        Value::Integer(age),
        Value::BigInt(9_007_199_254_740_993),
        Value::Boolean(true),
        Value::Uuid(manager_id),
        Value::Array(vec![
            Value::Text("alpha".to_owned()),
            Value::Text("beta".to_owned()),
        ]),
    ]
}

#[tokio::test]
async fn insert_reads_back_non_null_values_in_nullable_columns() {
    let client = JazzClient::test_client(nullable_schema()).await;
    let manager_id = ObjectId::new();
    let expected = full_values(manager_id, "inserted", 41);

    let (row_id, _, _) = client
        .insert(
            "profiles",
            row_input!(
                "name" => "inserted",
                "age" => 41,
                "visits" => Value::BigInt(9_007_199_254_740_993),
                "active" => true,
                "manager_id" => manager_id,
                "tags" => Value::Array(vec![Value::Text("alpha".to_owned()), Value::Text("beta".to_owned())])
            ),
        )
        .expect("insert non-null values into nullable columns");

    assert_eq!(profile_values(&client, row_id).await, expected);
}

#[tokio::test]
async fn insert_with_id_reads_back_non_null_values_in_nullable_columns() {
    let client = JazzClient::test_client(nullable_schema()).await;
    let row_uuid = Uuid::now_v7();
    let manager_id = ObjectId::new();
    let expected = full_values(manager_id, "with id", 42);

    let (row_id, _, _) = client
        .insert_with_id(
            "profiles",
            row_uuid,
            row_input!(
                "name" => "with id",
                "age" => 42,
                "visits" => Value::BigInt(9_007_199_254_740_993),
                "active" => true,
                "manager_id" => manager_id,
                "tags" => Value::Array(vec![Value::Text("alpha".to_owned()), Value::Text("beta".to_owned())])
            ),
        )
        .expect("insert_with_id non-null values into nullable columns");

    assert_eq!(*row_id.uuid(), row_uuid);
    assert_eq!(profile_values(&client, row_id).await, expected);
}

#[tokio::test]
async fn upsert_reads_back_non_null_values_in_nullable_columns() {
    let client = JazzClient::test_client(nullable_schema()).await;
    let row_uuid = Uuid::now_v7();
    let row_id = ObjectId::from_uuid(row_uuid);
    let manager_id = ObjectId::new();
    let expected = full_values(manager_id, "upserted", 43);

    client
        .upsert(
            "profiles",
            row_uuid,
            row_input!(
                "name" => "upserted",
                "age" => 43,
                "visits" => Value::BigInt(9_007_199_254_740_993),
                "active" => true,
                "manager_id" => manager_id,
                "tags" => Value::Array(vec![Value::Text("alpha".to_owned()), Value::Text("beta".to_owned())])
            ),
        )
        .expect("upsert non-null values into nullable columns");

    assert_eq!(profile_values(&client, row_id).await, expected);
}

#[tokio::test]
async fn update_reads_back_non_null_values_in_nullable_columns() {
    let client = JazzClient::test_client(nullable_schema()).await;
    let (row_id, _, _) = client
        .insert(
            "profiles",
            row_input!(
                "name" => Value::Null,
                "age" => Value::Null,
                "visits" => Value::Null,
                "active" => Value::Null,
                "manager_id" => Value::Null,
                "tags" => Value::Null
            ),
        )
        .expect("insert null values into nullable columns");
    let manager_id = ObjectId::new();
    let expected = full_values(manager_id, "updated", 44);

    client
        .update(
            row_id,
            vec![
                ("name".to_owned(), Value::Text("updated".to_owned())),
                ("age".to_owned(), Value::Integer(44)),
                ("visits".to_owned(), Value::BigInt(9_007_199_254_740_993)),
                ("active".to_owned(), Value::Boolean(true)),
                ("manager_id".to_owned(), Value::Uuid(manager_id)),
                (
                    "tags".to_owned(),
                    Value::Array(vec![
                        Value::Text("alpha".to_owned()),
                        Value::Text("beta".to_owned()),
                    ]),
                ),
            ],
        )
        .expect("update non-null values into nullable columns");

    assert_eq!(profile_values(&client, row_id).await, expected);
}

#[tokio::test]
async fn staged_insert_and_update_read_back_non_null_values_in_nullable_columns_after_commit() {
    let client = JazzClient::test_client(nullable_schema()).await;
    let transaction_id = client
        .begin_transaction()
        .expect("begin transaction")
        .transaction_id();
    let tx = client.with_write_context(WriteContext::default().with_transaction_id(transaction_id));
    let manager_id = ObjectId::new();
    let expected = full_values(manager_id, "staged updated", 46);

    let (row_id, _, _) = tx
        .insert(
            "profiles",
            row_input!(
                "name" => "staged inserted",
                "age" => 45,
                "visits" => Value::BigInt(9_007_199_254_740_993),
                "active" => false,
                "manager_id" => manager_id,
                "tags" => Value::Array(vec![Value::Text("alpha".to_owned()), Value::Text("beta".to_owned())])
            ),
        )
        .expect("stage insert non-null values into nullable columns");

    tx.update(
        row_id,
        vec![
            ("name".to_owned(), Value::Text("staged updated".to_owned())),
            ("age".to_owned(), Value::Integer(46)),
            ("active".to_owned(), Value::Boolean(true)),
        ],
    )
    .expect("stage update non-null values into nullable columns");

    client
        .commit_transaction(transaction_id)
        .expect("commit staged nullable writes");
    assert_eq!(profile_values(&client, row_id).await, expected);
}

#[tokio::test]
async fn staged_upsert_reads_back_non_null_values_in_nullable_columns_after_commit() {
    let client = JazzClient::test_client(nullable_schema()).await;
    let transaction_id = client
        .begin_transaction()
        .expect("begin transaction")
        .transaction_id();
    let tx = client.with_write_context(WriteContext::default().with_transaction_id(transaction_id));
    let row_uuid = Uuid::now_v7();
    let row_id = ObjectId::from_uuid(row_uuid);
    let manager_id = ObjectId::new();
    let expected = full_values(manager_id, "staged upsert", 47);

    tx.upsert(
        "profiles",
        row_uuid,
        row_input!(
            "name" => "staged upsert",
            "age" => 47,
            "visits" => Value::BigInt(9_007_199_254_740_993),
            "active" => true,
            "manager_id" => manager_id,
            "tags" => Value::Array(vec![Value::Text("alpha".to_owned()), Value::Text("beta".to_owned())])
        ),
    )
    .expect("stage upsert non-null values into nullable columns");

    client
        .commit_transaction(transaction_id)
        .expect("commit staged nullable upsert");
    assert_eq!(profile_values(&client, row_id).await, expected);
}
