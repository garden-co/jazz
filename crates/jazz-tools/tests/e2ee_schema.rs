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
