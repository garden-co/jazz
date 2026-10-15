use jazz_model::groove::schema::ColumnType as RuntimeType;
use jazz_model::model::public_schema::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_model::model::public_schema_convert::decode_public_schema_json;
use jazz_model::query::{Query, QueryError};
use jazz_model::schema::JazzSchema;
use jazz_model::schema::history::{SEQUENCE_COLUMN, TRANSACTION_ID_COLUMN};

fn schema(tables: impl IntoIterator<Item = TableSchemaBuilder>) -> JazzSchema {
    let mut builder = SchemaBuilder::new();
    for table in tables {
        builder = builder.table(table);
    }
    JazzSchema::new(&builder.build()).unwrap()
}

/// Alice resolves a query-only history descriptor without adding a writable
/// table or changing her source schema. This model-level test is appropriate
/// because this increment defines catalogue resolution, not query execution.
#[test]
fn history_descriptor_preserves_source_and_adds_non_null_metadata() {
    let schema = schema([TableSchemaBuilder::new("tasks")
        .column("title", ColumnType::Text)
        .nullable_column("notes", ColumnType::Text)]);
    let before = schema.runtime().clone();
    let history = schema
        .runtime()
        .resolve_history_relation("history.tasks")
        .unwrap();

    assert_eq!(history.name(), "history.tasks");
    assert!(std::ptr::eq(history.source_table(), &schema.tables()[0]));
    assert_eq!(
        &history.columns()[..2],
        schema.tables()[0].columns.as_slice()
    );
    assert_eq!(history.columns()[2].name(), TRANSACTION_ID_COLUMN);
    assert_eq!(
        history.columns()[2].column_type(),
        &RuntimeType::Tuple(vec![RuntimeType::U64, RuntimeType::Uuid]),
    );
    assert_eq!(history.columns()[3].name(), SEQUENCE_COLUMN);
    assert_eq!(history.columns()[3].column_type(), &RuntimeType::U64);
    assert_eq!(schema.runtime(), &before);
    assert_eq!(schema.tables().len(), 1);
    // Resolving a descriptor must not prematurely admit an unimplemented query.
    assert!(Query::from("history.tasks").validate(&schema).is_err());
}

/// Alice receives the ordinary unknown-table error for unresolved history names.
/// This model-level test exercises the public descriptor resolver before any
/// storage or execution capability is involved.
#[test]
fn history_names_are_resolved_exactly_without_recursive_expansion() {
    let schema = schema([TableSchemaBuilder::new("tasks").column("title", ColumnType::Text)]);
    for name in [
        "tasks",
        "history.",
        "history.history.tasks",
        "history.missing",
    ] {
        assert_eq!(
            schema.runtime().resolve_history_relation(name).unwrap_err(),
            QueryError::UnknownTable(name.to_owned()),
        );
    }
}

/// Alice cannot declare application tables in the virtual history namespace,
/// including through serialized schema admission used by catalogue loading.
/// This model-level test targets the shared compiler that owns name admission.
#[test]
fn schema_admission_reserves_history_table_names() {
    for name in ["history.tasks", "history.", "history.history.tasks"] {
        let source = SchemaBuilder::new()
            .table(TableSchemaBuilder::new(name).column("title", ColumnType::Text))
            .build();
        let error = JazzSchema::new(&source).unwrap_err().to_string();
        assert!(error.contains(name), "{error}");
        assert!(
            error.contains("reserved for virtual history relations"),
            "{error}"
        );
        let bytes = serde_json::to_vec(&source).unwrap();
        let error = decode_public_schema_json(&bytes).unwrap_err();
        assert!(error.contains(name), "{error}");
        assert!(
            error.contains("reserved for virtual history relations"),
            "{error}"
        );
    }
    // Only the exact prefix is reserved; ordinary names remain available.
    for name in ["history", "history_tasks", "archive.history.tasks"] {
        let schema = schema([TableSchemaBuilder::new(name).column("title", ColumnType::Text)]);
        let relation = format!("history.{name}");
        assert_eq!(
            schema
                .runtime()
                .resolve_history_relation(&relation)
                .unwrap()
                .source_table()
                .name,
            name,
        );
    }
}

/// Alice cannot declare columns shadowing history metadata. The model-level
/// schema admission boundary already reserves every `$` magic-column name.
#[test]
fn schema_admission_reserves_history_metadata_names() {
    for column in [TRANSACTION_ID_COLUMN, SEQUENCE_COLUMN] {
        let source = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("tasks").column(column, ColumnType::Text))
            .build();
        let error = JazzSchema::new(&source).unwrap_err().to_string();
        assert!(error.contains(column), "{error}");
        assert!(error.contains("reserved for magic columns"), "{error}");
        let bytes = serde_json::to_vec(&source).unwrap();
        let error = decode_public_schema_json(&bytes).unwrap_err();
        assert!(error.contains(column), "{error}");
        assert!(error.contains("reserved for magic columns"), "{error}");
    }
}
