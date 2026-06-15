//! Black-box tests for E2EE schema support (public API only).

use jazz_tools::query_manager::types::{ColumnType, SchemaBuilder, TableSchemaBuilder};

fn base_schema() -> SchemaBuilder {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("projects")
                .column("name", ColumnType::Text)
                .encryption_space(),
        )
        .table(
            TableSchemaBuilder::new("todos")
                .encrypted_column("title", ColumnType::Text, "projectId")
                .column("done", ColumnType::Boolean)
                .fk_column("projectId", "projects"),
        )
}

#[test]
fn builder_sets_e2ee_fields() {
    let schema = base_schema().build();
    let projects = &schema[&"projects".into()];
    assert!(projects.encryption_space);

    let todos = &schema[&"todos".into()];
    assert!(!todos.encryption_space);
    let title = todos
        .columns
        .columns
        .iter()
        .find(|c| c.name.as_str() == "title")
        .unwrap();
    assert_eq!(
        title.encrypted_with.as_ref().map(|c| c.as_str()),
        Some("projectId")
    );
}

#[test]
fn e2ee_fields_serialize_only_when_set() {
    let schema = base_schema().build();
    let json = serde_json::to_value(&schema).unwrap();
    assert_eq!(json["projects"]["encryption_space"], true);
    assert!(json["todos"].get("encryption_space").is_none());

    let todos_cols = json["todos"]["columns"].as_array().unwrap();
    let title = todos_cols.iter().find(|c| c["name"] == "title").unwrap();
    assert_eq!(title["encrypted_with"], "projectId");
    let done = todos_cols.iter().find(|c| c["name"] == "done").unwrap();
    assert!(done.get("encrypted_with").is_none());
}

#[test]
fn pre_e2ee_schema_json_still_deserializes() {
    let json = r#"{
        "plain": {
            "columns": [
                {"name": "title", "column_type": {"type": "Text"}, "nullable": false}
            ]
        }
    }"#;
    let schema: jazz_tools::query_manager::types::Schema = serde_json::from_str(json).unwrap();
    let plain = &schema[&"plain".into()];
    assert!(!plain.encryption_space);
    assert!(plain.columns.columns[0].encrypted_with.is_none());
}

#[test]
fn e2ee_markers_round_trip_through_storage_encoding() {
    use jazz_tools::schema_manager::encoding::{decode_schema, encode_schema};

    let schema = base_schema().build();
    let decoded = decode_schema(&encode_schema(&schema)).unwrap();

    assert!(decoded[&"projects".into()].encryption_space);
    let title = decoded[&"todos".into()]
        .columns
        .columns
        .iter()
        .find(|c| c.name.as_str() == "title")
        .unwrap()
        .clone();
    assert_eq!(
        title.encrypted_with.as_ref().map(|c| c.as_str()),
        Some("projectId")
    );
}

#[test]
fn e2ee_markers_change_the_schema_hash() {
    let without = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text))
        .hash();
    let with = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("projects")
                .column("name", ColumnType::Text)
                .encryption_space(),
        )
        .hash();
    assert_ne!(without, with);
}

use jazz_tools::query_manager::types::SchemaHash;
use jazz_tools::query_manager::types::e2ee_schema::{
    E2EE_KEYS_TABLE_SUFFIX, e2ee_keys_table_name, validate_e2ee_schema,
};

#[test]
fn keys_table_name_appends_suffix() {
    assert_eq!(E2EE_KEYS_TABLE_SUFFIX, "$keys");
    assert_eq!(e2ee_keys_table_name("projects"), "projects$keys");
}

#[test]
fn build_expands_companion_keys_table() {
    let schema = base_schema().build();
    let keys = schema
        .get(&"projects$keys".into())
        .expect("companion table generated");

    let names: Vec<&str> = keys
        .columns
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "space_id",
            "key_id",
            "recipient_user_id",
            "recipient_public_key",
            "sealed_key"
        ]
    );
    let space_id = &keys.columns.columns[0];
    assert_eq!(
        space_id.references.as_ref().map(|t| t.as_str()),
        Some("projects")
    );
    assert!(!space_id.nullable);
    assert!(!keys.encryption_space);

    // v1 policies: world-read, authenticated insert/delete, NO update clause.
    let policies = serde_json::to_value(&keys.policies).unwrap();
    assert_eq!(policies["select"]["using"]["type"], "True");
    assert_eq!(policies["insert"]["with_check"]["type"], "SessionIsNotNull");
    assert_eq!(policies["insert"]["with_check"]["path"][0], "user_id");
    assert_eq!(policies["delete"]["using"]["type"], "SessionIsNotNull");
    assert!(policies["update"]["using"].is_null());
    assert!(policies["update"]["with_check"].is_null());
}

#[test]
fn expansion_is_idempotent_and_hash_stable() {
    let a = base_schema().build();
    let b = base_schema().build();
    assert_eq!(SchemaHash::compute(&a), SchemaHash::compute(&b));
    assert_eq!(a.len(), 3); // projects, todos, projects$keys
}

#[test]
fn tables_without_spaces_get_no_companion() {
    let schema = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("plain").column("name", ColumnType::Text))
        .build();
    assert_eq!(schema.len(), 1);
}

fn expect_invalid(builder: SchemaBuilder, needle: &str) {
    let schema = builder.build();
    let err = validate_e2ee_schema(&schema).expect_err("schema should be invalid");
    assert!(
        err.contains(needle),
        "error {err:?} should mention {needle:?}"
    );
}

#[test]
fn valid_e2ee_schema_passes() {
    assert_eq!(validate_e2ee_schema(&base_schema().build()), Ok(()));
}

#[test]
fn encrypted_column_must_name_existing_ref() {
    expect_invalid(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .encryption_space(),
            )
            .table(TableSchemaBuilder::new("todos").encrypted_column(
                "title",
                ColumnType::Text,
                "missing",
            )),
        "missing",
    );
}

#[test]
fn encrypted_ref_must_be_non_nullable() {
    expect_invalid(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .encryption_space(),
            )
            .table(
                TableSchemaBuilder::new("todos")
                    .encrypted_column("title", ColumnType::Text, "projectId")
                    .nullable_fk_column("projectId", "projects"),
            ),
        "non-nullable",
    );
}

#[test]
fn encrypted_ref_target_must_be_encryption_space() {
    expect_invalid(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("todos")
                    .encrypted_column("title", ColumnType::Text, "projectId")
                    .fk_column("projectId", "projects"),
            ),
        "encryption space",
    );
}

#[test]
fn user_tables_may_not_use_dollar_names() {
    expect_invalid(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new("nope$keys").column("name", ColumnType::Text)),
        "reserved",
    );
}

#[test]
fn encrypted_columns_are_excluded_from_indexing() {
    // Build's normalization must populate indexed_columns excluding encrypted
    // columns when the table previously indexed everything (None).
    let schema = base_schema().build();
    let todos = &schema[&"todos".into()];
    let indexed = todos.indexed_columns.as_ref().expect("normalized");
    assert!(indexed.iter().all(|c| c.as_str() != "title"));
    assert!(indexed.iter().any(|c| c.as_str() == "done"));
    assert!(indexed.iter().any(|c| c.as_str() == "projectId"));
}

#[test]
fn explicitly_indexed_encrypted_column_is_rejected() {
    let mut schema = base_schema().build();
    let todos = schema.get_mut(&"todos".into()).unwrap();
    todos.indexed_columns = Some(vec!["title".into()]);
    let err = validate_e2ee_schema(&schema).expect_err("indexed encrypted column");
    assert!(err.contains("index"));
}

#[test]
fn policies_may_not_reference_encrypted_columns() {
    use jazz_tools::query_manager::policy::{CmpOp, PolicyExpr, PolicyValue};
    use jazz_tools::query_manager::types::{OperationPolicy, Value};

    let mut schema = base_schema().build();
    let todos = schema.get_mut(&"todos".into()).unwrap();
    todos.policies.select = OperationPolicy::using(PolicyExpr::Cmp {
        column: "title".to_string(),
        op: CmpOp::Eq,
        value: PolicyValue::Literal(Value::Text("x".into())),
    });
    let err = validate_e2ee_schema(&schema).expect_err("policy on encrypted column");
    assert!(err.contains("policy"));
}
