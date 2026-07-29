#![cfg(feature = "test")]

use jazz_tools::{
    ColumnType, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
    row_input,
};

fn membership_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("items")
                .column("title", ColumnType::Text)
                .column("count", ColumnType::Integer)
                .column("score", ColumnType::Double)
                .column("active", ColumnType::Boolean)
                .fk_column("owner_id", "users")
                .column(
                    "counts",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Integer),
                    },
                )
                .column(
                    "flags",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Boolean),
                    },
                )
                .array_fk_column("watcher_ids", "users"),
        )
        .build()
}

#[tokio::test(flavor = "current_thread")]
async fn in_filters_match_integer_float_boolean_and_reference_columns() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(membership_schema()).await;
            let (owner_a, _, _) = client
                .insert("users", row_input!("name" => "owner a"))
                .expect("insert owner a");
            let (owner_b, _, _) = client
                .insert("users", row_input!("name" => "owner b"))
                .expect("insert owner b");
            let (match_a, _, _) = client
                .insert(
                    "items",
                    row_input!(
                        "title" => "alpha",
                        "count" => 1,
                        "score" => 1.5,
                        "active" => true,
                        "owner_id" => owner_a,
                        "counts" => Value::Array(vec![Value::Integer(1), Value::Integer(3)]),
                        "flags" => Value::Array(vec![Value::Boolean(true)]),
                        "watcher_ids" => Value::Array(vec![Value::Uuid(owner_a)]),
                    ),
                )
                .expect("insert matching item a");
            let (match_b, _, _) = client
                .insert(
                    "items",
                    row_input!(
                        "title" => "beta",
                        "count" => 2,
                        "score" => 2.5,
                        "active" => false,
                        "owner_id" => owner_b,
                        "counts" => Value::Array(vec![Value::Integer(2), Value::Integer(3)]),
                        "flags" => Value::Array(vec![Value::Boolean(false)]),
                        "watcher_ids" => Value::Array(vec![Value::Uuid(owner_b)]),
                    ),
                )
                .expect("insert matching item b");
            client
                .insert(
                    "items",
                    row_input!(
                        "title" => "gamma",
                        "count" => 3,
                        "score" => 3.5,
                        "active" => true,
                        "owner_id" => owner_b,
                        "counts" => Value::Array(vec![Value::Integer(5)]),
                        "flags" => Value::Array(vec![Value::Boolean(true)]),
                        "watcher_ids" => Value::Array(vec![Value::Uuid(owner_b)]),
                    ),
                )
                .expect("insert non-matching item");

            let query = QueryBuilder::new("items")
                .filter_in("count", vec![Value::Integer(1), Value::Integer(2)])
                .filter_in("score", vec![Value::Double(1.5), Value::Double(2.5)])
                .filter_in("active", vec![Value::Boolean(true), Value::Boolean(false)])
                .filter_in("owner_id", vec![Value::Uuid(owner_a), Value::Uuid(owner_b)])
                .select(&["title"])
                .build();

            let mut rows = client.query(query, None).await.expect("query items");
            rows.sort_by_key(|(id, _)| *id);

            assert_eq!(
                rows,
                vec![
                    (match_a, vec![Value::Text("alpha".to_owned())]),
                    (match_b, vec![Value::Text("beta".to_owned())]),
                ]
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn contains_filters_match_non_text_array_elements_and_text_substrings() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(membership_schema()).await;
            let (owner_a, _, _) = client
                .insert("users", row_input!("name" => "owner a"))
                .expect("insert owner a");
            let (owner_b, _, _) = client
                .insert("users", row_input!("name" => "owner b"))
                .expect("insert owner b");
            let (match_a, _, _) = client
                .insert(
                    "items",
                    row_input!(
                        "title" => "needle alpha",
                        "count" => 1,
                        "score" => 1.5,
                        "active" => true,
                        "owner_id" => owner_a,
                        "counts" => Value::Array(vec![Value::Integer(1), Value::Integer(3)]),
                        "flags" => Value::Array(vec![Value::Boolean(true)]),
                        "watcher_ids" => Value::Array(vec![Value::Uuid(owner_a)]),
                    ),
                )
                .expect("insert matching item a");
            client
                .insert(
                    "items",
                    row_input!(
                        "title" => "plain beta",
                        "count" => 2,
                        "score" => 2.5,
                        "active" => false,
                        "owner_id" => owner_b,
                        "counts" => Value::Array(vec![Value::Integer(2)]),
                        "flags" => Value::Array(vec![Value::Boolean(false)]),
                        "watcher_ids" => Value::Array(vec![Value::Uuid(owner_b)]),
                    ),
                )
                .expect("insert non-matching item");

            let count_rows = client
                .query(
                    QueryBuilder::new("items")
                        .filter_contains("counts", Value::Integer(3))
                        .select(&["title"])
                        .build(),
                    None,
                )
                .await
                .expect("query integer array contains");
            assert_eq!(
                count_rows,
                vec![(match_a, vec![Value::Text("needle alpha".to_owned())])]
            );

            let flag_rows = client
                .query(
                    QueryBuilder::new("items")
                        .filter_contains("flags", Value::Boolean(true))
                        .select(&["title"])
                        .build(),
                    None,
                )
                .await
                .expect("query boolean array contains");
            assert_eq!(flag_rows, count_rows);

            let ref_rows = client
                .query(
                    QueryBuilder::new("items")
                        .filter_contains("watcher_ids", Value::Uuid(owner_a))
                        .select(&["title"])
                        .build(),
                    None,
                )
                .await
                .expect("query uuid array contains");
            assert_eq!(ref_rows, count_rows);

            let text_rows = client
                .query(
                    QueryBuilder::new("items")
                        .filter_contains("title", Value::Text("needle".to_owned()))
                        .select(&["title"])
                        .build(),
                    None,
                )
                .await
                .expect("query text substring contains");
            assert_eq!(text_rows, count_rows);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_membership_filters_return_type_errors() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(membership_schema()).await;

            let scalar_contains = client
                .query(
                    QueryBuilder::new("items")
                        .filter_contains("count", Value::Integer(1))
                        .build(),
                    None,
                )
                .await
                .expect_err("contains on scalar integer column should fail");
            assert!(
                scalar_contains
                    .to_string()
                    .contains("operand type mismatch"),
                "unexpected contains error: {scalar_contains}"
            );

            let wrong_in_type = client
                .query(
                    QueryBuilder::new("items")
                        .filter_in("count", vec![Value::Text("not an integer".to_owned())])
                        .build(),
                    None,
                )
                .await
                .expect_err("in with mismatched candidate type should fail");
            assert!(
                wrong_in_type.to_string().contains("operand type mismatch"),
                "unexpected in error: {wrong_in_type}"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn in_filter_rejects_scalar_candidate_for_array_column() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(membership_schema()).await;
            client
                .insert(
                    "items",
                    row_input!(
                        "title" => "array member",
                        "count" => 1,
                        "score" => 1.5,
                        "active" => true,
                        "owner_id" => ObjectId::new(),
                        "counts" => Value::Array(vec![Value::Integer(3)]),
                        "flags" => Value::Array(vec![Value::Boolean(true)]),
                        "watcher_ids" => Value::Array(Vec::new()),
                    ),
                )
                .expect("insert item with array value");

            let error = client
                .query(
                    QueryBuilder::new("items")
                        .filter_in("counts", vec![Value::Integer(3)])
                        .build(),
                    None,
                )
                .await
                .expect_err(
                    "a scalar in candidate for an array column must fail validation, not return no rows",
                );

            let message = error.to_string();
            assert!(
                message.contains("counts"),
                "validation error should name the array column: {message}"
            );
            assert!(
                message.contains("Array") && message.contains("I32"),
                "validation error should name the array/scalar type mismatch: {message}"
            );
        })
        .await;
}
