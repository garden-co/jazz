#![cfg(feature = "test")]

//! Regression coverage retained from the pre-engine-swap JSON integration
//! suite. JSON is physically stored as text, but its public schema contract
//! must still be enforced at every write boundary.

use jazz::row_input;
use jazz::tools::{
    AppContext, ColumnType, JazzClient, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
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
    documents_schema(Some(name_json_schema()))
}

fn name_json_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": { "name": { "type": "string" } },
        "required": ["name"],
        "additionalProperties": false,
    })
}

fn default_documents_schema(default: Value, json_schema: Option<serde_json::Value>) -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("documents").column_with_default(
            "payload",
            ColumnType::Json {
                schema: json_schema,
            },
            default,
        ))
        .build()
}

fn json_array_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("documents").column(
            "payloads",
            ColumnType::Array {
                element: Box::new(ColumnType::Json {
                    schema: Some(serde_json::json!({
                        "type": "object",
                        "properties": { "name": { "type": "string" } },
                        "required": ["name"],
                        "additionalProperties": false,
                    })),
                }),
            },
        ))
        .build()
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

async fn assert_json_array_documents_empty(client: &JazzClient, context: &str) {
    let rows = client
        .query(
            QueryBuilder::new("documents").select(&["payloads"]).build(),
            None,
        )
        .await
        .expect("query documents after rejected nested JSON write");
    assert!(
        rows.is_empty(),
        "{context}: rejected nested JSON write persisted {rows:?}"
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
/// requires rejection before the row is persisted.
#[tokio::test(flavor = "current_thread")]
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

/// Alice's declared JSON default is validated while the schema is admitted, so
/// an empty insert can never materialize malformed source text from the core's
/// later default-injection path.
#[tokio::test(flavor = "current_thread")]
async fn json_column_rejects_malformed_default_before_empty_insert() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = default_documents_schema(Value::Text("{\"name\":true".to_owned()), None);
            let error = match JazzClient::connect(AppContext::test(schema)).await {
                Ok(_) => panic!("malformed JSON default must reject schema admission"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("invalid JSON for column `payload`"),
                "unexpected malformed-default error: {error}"
            );
        })
        .await;
}

/// Alice's JSON Schema-invalid default is rejected at schema admission, before
/// any empty document insert could inject it through core default handling.
#[tokio::test(flavor = "current_thread")]
async fn json_column_rejects_schema_invalid_default_before_empty_insert() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = default_documents_schema(
                Value::Text("{\"name\":123}".to_owned()),
                Some(name_json_schema()),
            );
            let error = match JazzClient::connect(AppContext::test(schema)).await {
                Ok(_) => panic!("JSON Schema-invalid default must reject schema admission"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("JSON schema validation failed for column `payload`"),
                "unexpected schema-invalid-default error: {error}"
            );
        })
        .await;
}

/// Alice omits a valid JSON default, and the core injects exactly that already
/// admitted source text into the new document.
#[tokio::test(flavor = "current_thread")]
async fn json_column_valid_default_is_inserted_when_omitted() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let default = "{\"name\":\"Ada\"}";
            let client = JazzClient::test_client(default_documents_schema(
                Value::Text(default.to_owned()),
                Some(serde_json::json!({
                    "type": "object",
                    "properties": { "name": { "type": "string" } },
                    "required": ["name"],
                    "additionalProperties": false,
                })),
            ))
            .await;
            let (id, _, _) = client
                .insert("documents", row_input!())
                .expect("empty insert uses admitted JSON default");
            let rows = client
                .query(
                    QueryBuilder::new("documents").select(&["payload"]).build(),
                    None,
                )
                .await
                .expect("query defaulted JSON document");
            assert_eq!(rows, vec![(id, vec![Value::Text(default.to_owned())])]);
        })
        .await;
}

/// Alice exercises ordinary and exclusive insert, upsert, and update paths
/// with an ARRAY<JSON>; malformed or schema-invalid nested elements must be
/// rejected before either path can create or alter a document.
#[tokio::test(flavor = "current_thread")]
async fn json_array_elements_are_admitted_at_every_write_boundary() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(json_array_schema()).await;
            let malformed = Value::Array(vec![Value::Text("{\"name\":true".to_owned())]);
            let invalid = Value::Array(vec![Value::Text("{\"name\":123}".to_owned())]);
            let valid = Value::Array(vec![Value::Text("{\"name\":\"Ada\"}".to_owned())]);

            let error = client
                .insert("documents", row_input!("payloads" => malformed.clone()))
                .expect_err("ordinary insert rejects malformed nested JSON");
            assert!(
                error
                    .to_string()
                    .contains("invalid JSON for column `payloads[0]`")
            );

            let tx = client
                .begin_transaction()
                .expect("open exclusive transaction");
            let error = tx
                .insert("documents", row_input!("payloads" => invalid.clone()))
                .expect_err("exclusive insert rejects schema-invalid nested JSON");
            assert!(
                error
                    .to_string()
                    .contains("JSON schema validation failed for column `payloads[0]`")
            );
            tx.rollback()
                .expect("roll back rejected exclusive transaction");
            assert_json_array_documents_empty(&client, "rejected nested JSON inserts").await;

            let (id, _, _) = client
                .insert("documents", row_input!("payloads" => valid.clone()))
                .expect("ordinary insert accepts valid nested JSON");
            client
                .upsert(
                    "documents",
                    *id.uuid(),
                    row_input!("payloads" => valid.clone()),
                )
                .expect("ordinary upsert accepts valid nested JSON");
            let error = client
                .update(id, vec![("payloads".to_owned(), invalid.clone())])
                .expect_err("ordinary update rejects schema-invalid nested JSON");
            assert!(error.to_string().contains("payloads[0]"));

            let tx = client
                .begin_transaction()
                .expect("open exclusive transaction");
            let error = tx
                .upsert(
                    "documents",
                    *id.uuid(),
                    row_input!("payloads" => malformed.clone()),
                )
                .expect_err("exclusive upsert rejects malformed nested JSON");
            assert!(error.to_string().contains("payloads[0]"));
            let error = tx
                .update(id, vec![("payloads".to_owned(), invalid)])
                .expect_err("exclusive update rejects schema-invalid nested JSON");
            assert!(error.to_string().contains("payloads[0]"));
            tx.rollback()
                .expect("roll back rejected exclusive transaction");
        })
        .await;
}

/// Alice declares an invalid JSON Schema on a nullable column with no default.
/// Schema admission rejects it before a null write could otherwise defer the
/// failure until some later non-null document.
#[tokio::test(flavor = "current_thread")]
async fn invalid_json_schema_rejects_nullable_column_without_default() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("documents").nullable_column(
                    "payload",
                    ColumnType::Json {
                        schema: Some(serde_json::json!({ "type": 42 })),
                    },
                ))
                .build();
            let error = match JazzClient::connect(AppContext::test(schema)).await {
                Ok(_) => panic!("invalid declared JSON Schema must reject schema admission"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("invalid JSON schema for column `payload`"),
                "unexpected invalid-schema error: {error}"
            );
        })
        .await;
}
