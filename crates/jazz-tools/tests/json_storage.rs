#![cfg(feature = "test")]

use jazz_tools::row_input;
use jazz_tools::{
    ColumnType, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};
use serde_json::{Value as JsonValue, json};

fn documents_schema(payload_schema: Option<JsonValue>) -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("documents").column(
            "payload",
            ColumnType::Json {
                schema: payload_schema,
            },
        ))
        .build()
}

fn schema_requiring_string_name() -> Schema {
    documents_schema(Some(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    })))
}

async fn query_documents(client: &JazzClient) -> Vec<(ObjectId, Vec<Value>)> {
    client
        .query(QueryBuilder::new("documents").build(), None)
        .await
        .expect("query documents")
}

/// Verifies that a JSON column stores the exact text the user inserted rather
/// than normalizing or reserializing it.
///
/// Actor: alice inserts a formatted JSON string into `documents.payload` and
/// reads the same text back through a public query.
#[tokio::test]
async fn insert_json_preserves_original_text() {
    let client = JazzClient::test_client(documents_schema(None)).await;

    let raw = "{\n  \"name\": \"Ada\",\n  \"active\": true\n}";
    let document_id = client
        .insert("documents", row_input!("payload" => raw))
        .expect("insert valid json")
        .0;

    let rows = query_documents(&client).await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, document_id);
    assert_eq!(rows[0].1, vec![Value::Text(raw.to_string())]);
}

/// Verifies that writes to a JSON column reject syntactically invalid JSON
/// before the row is accepted.
///
/// Actor: alice attempts to insert malformed JSON into `documents.payload`.
#[tokio::test]
async fn insert_rejects_invalid_json_text() {
    let client = JazzClient::test_client(documents_schema(None)).await;

    let error = client
        .insert("documents", row_input!("payload" => "{\"name\":true"))
        .expect_err("invalid JSON must be rejected");

    assert!(
        error
            .to_string()
            .contains("invalid JSON for column `payload`"),
        "unexpected error: {error:?}"
    );
}

/// Verifies that a JSON column with an attached JSON Schema rejects inserts
/// whose payload parses as JSON but does not satisfy the schema.
///
/// Actor: alice inserts a document whose `name` field is not a string.
#[tokio::test]
async fn insert_rejects_json_schema_violation() {
    let client = JazzClient::test_client(schema_requiring_string_name()).await;

    let error = client
        .insert("documents", row_input!("payload" => "{\"name\":123}"))
        .expect_err("schema-invalid JSON must be rejected");

    assert!(
        error
            .to_string()
            .contains("JSON schema validation failed for column `payload`"),
        "unexpected error: {error:?}"
    );
}

/// Verifies that a JSON Schema violation during update is rejected and the
/// previous valid JSON payload remains visible.
///
/// Actor: alice inserts a valid document, attempts an invalid update, then
/// queries the row and sees the original payload.
#[tokio::test]
async fn update_rejects_json_schema_violation_and_preserves_existing_payload() {
    let client = JazzClient::test_client(schema_requiring_string_name()).await;

    let document_id = client
        .insert("documents", row_input!("payload" => "{\"name\":\"ok\"}"))
        .expect("insert valid row first")
        .0;

    let error = client
        .update(
            document_id,
            vec![(
                "payload".to_string(),
                Value::Text("{\"name\":42}".to_string()),
            )],
        )
        .expect_err("invalid update payload must be rejected");

    assert!(
        error
            .to_string()
            .contains("JSON schema validation failed for column `payload`"),
        "unexpected error: {error:?}"
    );

    let rows = query_documents(&client).await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, document_id);
    assert_eq!(
        rows[0].1,
        vec![Value::Text("{\"name\":\"ok\"}".to_string())]
    );
}
