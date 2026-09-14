mod common;

use jazz::query::Query;
use jazz::tools::sync::ReadTier;
use jazz::tools::{
    ColumnDescriptor, ColumnType, JazzClient, RowDescriptor, Schema, TableName, TableSchema, Value,
};

/// Object-only JSON validation treats nullable root null exactly like column
/// null, including defaults. Required JSON retains its original source semantics.
#[tokio::test]
async fn nullable_json_defaults_and_object_schema_share_null_semantics() {
    let object_json = ColumnType::Json {
        schema: Some(serde_json::json!({"type":"object"})),
    };
    let schema = Schema::from([
        (
            TableName::new("documents"),
            TableSchema::with_policies(
                RowDescriptor::new(vec![
                    ColumnDescriptor::new("payload", object_json.clone())
                        .nullable()
                        .default(Value::Text(" \nnull\t".into())),
                ]),
                common::allow_all_policies(),
            ),
        ),
        (
            TableName::new("required"),
            TableSchema::with_policies(
                RowDescriptor::new(vec![ColumnDescriptor::new("payload", object_json)]),
                common::allow_all_policies(),
            ),
        ),
        (
            TableName::new("unrestricted"),
            TableSchema::with_policies(
                RowDescriptor::new(vec![ColumnDescriptor::new(
                    "payload",
                    ColumnType::Json { schema: None },
                )]),
                common::allow_all_policies(),
            ),
        ),
    ]);
    let client = JazzClient::test_client(schema).await;
    let id = uuid::Uuid::from_u128(1);
    client.upsert("documents", id, jazz::row_input!()).unwrap();
    for source in [None, Some(" \nnull\t"), Some("{\"nested\":null}")] {
        if let Some(source) = source {
            client
                .upsert(
                    "documents",
                    id,
                    jazz::row_input!("payload" => Value::Text(source.into())),
                )
                .unwrap();
        }
        let rows = client
            .query_results_with_read_tier(Query::from("documents"), ReadTier::LocalFirst)
            .await
            .unwrap();
        let expected = match source {
            Some(source) if source.trim() != "null" => Value::Text(source.into()),
            _ => Value::Null,
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("payload"), Some(&expected));
    }
    client
        .upsert("documents", id, jazz::row_input!("payload" => Value::Null))
        .unwrap();
    let rows = client
        .query_results_with_read_tier(Query::from("documents"), ReadTier::LocalFirst)
        .await
        .unwrap();
    assert_eq!(rows[0].get("payload"), Some(&Value::Null));
    assert!(
        client
            .upsert(
                "required",
                id,
                jazz::row_input!("payload" => Value::Text("null".into()))
            )
            .is_err()
    );
    client
        .upsert(
            "unrestricted",
            id,
            jazz::row_input!("payload" => Value::Text("null".into())),
        )
        .unwrap();
    let rows = client
        .query_results_with_read_tier(Query::from("unrestricted"), ReadTier::LocalFirst)
        .await
        .unwrap();
    assert_eq!(rows[0].get("payload"), Some(&Value::Text("null".into())));
    client.shutdown().await.unwrap();
}

/// Reused policy plans must keep each admitted identity's original claim
/// carrier while interpreting a present JSON root-null claim contextually.
#[tokio::test]
async fn nullable_json_policy_claims_preserve_identity_and_missing_claim_denial() {
    use jazz::tools::{SchemaBuilder, Session, TableSchemaBuilder};
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("documents")
                .nullable_column("payload", ColumnType::Json { schema: None })
                .policies(common::read_and_allow_all_writes(common::session_eq(
                    "payload",
                    &["claims", "selector"],
                ))),
        )
        .build();
    let root = JazzClient::test_client(schema).await;
    for (index, source) in ["null", " \nnull\t", "{\"nested\":null}", "\"null\""]
        .into_iter()
        .enumerate()
    {
        root.upsert(
            "documents",
            uuid::Uuid::from_u128(index as u128 + 1),
            jazz::row_input!("payload" => Value::Text(source.into())),
        )
        .unwrap();
    }
    for _ in 0..2 {
        for (subject, selector, expected) in [
            ("null-reader", Some("null"), 2),
            ("object-reader", Some("{\"nested\":null}"), 1),
            ("spaced-null-reader", Some(" \nnull\t"), 2),
            ("string-reader", Some("\"null\""), 1),
            ("missing-reader", None, 0),
            ("null-reader", Some("{\"nested\":null}"), 1),
        ] {
            let mut session = Session::new("https://issuer.example", subject);
            session.claims = selector
                .map(|selector| serde_json::json!({"selector":selector}))
                .unwrap_or(serde_json::json!({}));
            let client = root.for_session(session);
            let rows = client
                .query_results_with_read_tier(Query::from("documents"), ReadTier::LocalFirst)
                .await
                .unwrap();
            assert_eq!(rows.len(), expected, "claim {selector:?} for {subject}");
            for row in &rows {
                let expected = match selector {
                    Some(value) if value.trim() != "null" => Value::Text(value.into()),
                    _ => Value::Null,
                };
                assert_eq!(row.get("payload"), Some(&expected));
            }
        }
    }
    root.shutdown().await.unwrap();
}
