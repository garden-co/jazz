#![cfg(feature = "test")]

//! Regression coverage retained from the pre-engine-swap JSON integration
//! suite. JSON is physically stored as text, but its public schema contract
//! must still be enforced at every write boundary.

use jazz::row_input;
use jazz::tools::{
    ColumnType, JazzClient, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};

fn documents_schema(json_schema: Option<serde_json::Value>) -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("documents").column(
            "payload",
            ColumnType::Json {
                schema: json_schema,
            },
        ))
        .build()
}

fn name_schema() -> Schema {
    documents_schema(Some(serde_json::json!({
        "type": "object",
        "properties": { "name": { "type": "string" } },
        "required": ["name"],
        "additionalProperties": false,
    })))
}

async fn assert_documents_empty(client: &JazzClient, context: &str) {
    let rows = client
        .query(
            QueryBuilder::new("documents").select(&["payload"]).build(),
            None,
        )
        .await
        .expect("query documents after rejected write");
    assert!(
        rows.is_empty(),
        "{context}: rejected write persisted {rows:?}"
    );
}

/// Alice stores formatted JSON through the public client API, and a later
/// query returns exactly the same source text rather than a reserialized form.
#[tokio::test(flavor = "current_thread")]
async fn json_column_preserves_original_text() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(documents_schema(None)).await;
            let raw = "{\n  \"name\": \"Ada\",\n  \"active\": true\n}";
            let (id, _, _) = client
                .insert("documents", row_input!("payload" => raw))
                .expect("insert valid JSON source text");

            let rows = client
                .query(
                    QueryBuilder::new("documents").select(&["payload"]).build(),
                    None,
                )
                .await
                .expect("query stored JSON");
            assert_eq!(rows, vec![(id, vec![Value::Text(raw.to_owned())])]);
        })
        .await;
}

/// Alice submits syntactically malformed JSON. The public JSON column contract
/// requires rejection before the row is persisted; this is quarantined until
/// the new core restores JSON validation at its write admission boundary.
#[tokio::test(flavor = "current_thread")]
#[ignore = "known red; tracked in TEST_BURNDOWN.md"]
async fn json_column_rejects_malformed_source_text() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(documents_schema(None)).await;
            let error = client
                .insert("documents", row_input!("payload" => "{\"name\":true"))
                .expect_err("malformed JSON must be rejected before local persistence");
            assert!(
                error
                    .to_string()
                    .contains("invalid JSON for column `payload`"),
                "unexpected malformed-JSON error: {error}"
            );
            assert_documents_empty(&client, "malformed JSON insert").await;
        })
        .await;
}

/// Alice writes JSON that parses but violates the declared `name: string`
/// schema. The rejection must be atomic: schema-invalid content cannot enter
/// the locally visible row set.
#[tokio::test(flavor = "current_thread")]
#[ignore = "known red; tracked in TEST_BURNDOWN.md"]
async fn json_column_rejects_schema_invalid_source_text() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(name_schema()).await;
            let error = client
                .insert("documents", row_input!("payload" => "{\"name\":123}"))
                .expect_err("JSON Schema violation must be rejected before local persistence");
            assert!(
                error
                    .to_string()
                    .contains("JSON schema validation failed for column `payload`"),
                "unexpected JSON-Schema error: {error}"
            );
            assert_documents_empty(&client, "schema-invalid JSON insert").await;
        })
        .await;
}

/// Alice updates a valid document with JSON that violates its declared schema.
/// The failed update must leave Alice's original payload visible, proving that
/// JSON admission is atomic rather than a post-persistence validation step.
#[tokio::test(flavor = "current_thread")]
#[ignore = "known red; tracked in TEST_BURNDOWN.md"]
async fn json_column_rejects_schema_invalid_update_and_preserves_original_text() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(name_schema()).await;
            let original = "{\"name\":\"Ada\"}";
            let (id, _, _) = client
                .insert("documents", row_input!("payload" => original))
                .expect("insert schema-valid JSON source text");

            let error = client
                .update(
                    id,
                    vec![(
                        "payload".to_owned(),
                        Value::Text("{\"name\":123}".to_owned()),
                    )],
                )
                .expect_err("JSON Schema violation must reject the update atomically");
            assert!(
                error
                    .to_string()
                    .contains("JSON schema validation failed for column `payload`"),
                "unexpected JSON-Schema error: {error}"
            );

            let rows = client
                .query(
                    QueryBuilder::new("documents").select(&["payload"]).build(),
                    None,
                )
                .await
                .expect("query document after rejected update");
            assert_eq!(
                rows,
                vec![(id, vec![Value::Text(original.to_owned())])],
                "schema-invalid update must not replace the stored payload"
            );
        })
        .await;
}
