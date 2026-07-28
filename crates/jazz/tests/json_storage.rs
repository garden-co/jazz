use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::schema::{ColumnSchema, JazzSchema, Policy, TableSchema};
use serde_json::{Value as JsonValue, json};

fn documents_schema(payload_schema: Option<JsonValue>) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "documents",
        [ColumnSchema::json("payload", payload_schema)],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn schema_requiring_string_name() -> JazzSchema {
    documents_schema(Some(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    })))
}

fn open_db(schema: JazzSchema) -> Db<MemoryStorage> {
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    jazz::db::block_on(Db::open(DbConfig {
        schema,
        storage: MemoryStorage::new(&column_family_refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorId::from_bytes([0xa1; 16]),
        },
        id_source: None,
        large_value_checkpoint_op_interval: 1024,
    }))
    .expect("open db")
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn payload(raw: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("payload".to_owned(), Value::String(raw.to_owned()))])
}

fn query_documents(db: &Db<MemoryStorage>, schema: &JazzSchema) -> Vec<(RowUuid, Vec<Value>)> {
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "documents")
        .expect("documents table");
    let prepared = db
        .prepare_query(&db.table("documents"))
        .expect("prepare documents query");

    db.read(&prepared)
        .expect("query documents")
        .into_iter()
        .map(|row| {
            let row_id = row.row_uuid();
            let payload = row.cell(table, "payload").expect("stored document payload");
            (row_id, vec![payload])
        })
        .collect()
}

/// Verifies that a JSON column stores the exact text the user inserted rather
/// than normalizing or reserializing it.
///
/// Actor: alice inserts a formatted JSON string into `documents.payload` and
/// reads the same text back through the public database API.
#[test]
fn insert_json_preserves_original_text() {
    let schema = documents_schema(None);
    let db = open_db(schema.clone());

    let raw = "{\n  \"name\": \"Ada\",\n  \"active\": true\n}";
    let document_id = row(1);
    db.insert_with_id("documents", document_id, payload(raw))
        .expect("insert valid json");

    let rows = query_documents(&db, &schema);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, document_id);
    assert_eq!(rows[0].1, vec![Value::String(raw.to_owned())]);
}

/// Verifies that writes to a JSON column reject syntactically invalid JSON
/// before the row is accepted.
///
/// Actor: alice attempts to insert malformed JSON into `documents.payload`.
#[test]
fn insert_rejects_invalid_json_text() {
    let db = open_db(documents_schema(None));

    let error = match db.insert_with_id("documents", row(1), payload("{\"name\":true")) {
        Ok(_) => panic!("invalid JSON must be rejected"),
        Err(error) => error,
    };

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
#[test]
fn insert_rejects_json_schema_violation() {
    let db = open_db(schema_requiring_string_name());

    let error = match db.insert_with_id("documents", row(1), payload("{\"name\":123}")) {
        Ok(_) => panic!("schema-invalid JSON must be rejected"),
        Err(error) => error,
    };

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
#[test]
fn update_rejects_json_schema_violation_and_preserves_existing_payload() {
    let schema = schema_requiring_string_name();
    let db = open_db(schema.clone());
    let document_id = row(1);

    db.insert_with_id("documents", document_id, payload("{\"name\":\"ok\"}"))
        .expect("insert valid row first");

    let error = match db.update("documents", document_id, payload("{\"name\":42}")) {
        Ok(_) => panic!("invalid update payload must be rejected"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("JSON schema validation failed for column `payload`"),
        "unexpected error: {error:?}"
    );

    let rows = query_documents(&db, &schema);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, document_id);
    assert_eq!(
        rows[0].1,
        vec![Value::String("{\"name\":\"ok\"}".to_owned())]
    );
}
