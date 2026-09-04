/// A schema-agnostic physical current-row target used only until the
/// Global/Ahead winner has been selected. Its enum tags retain their durable
/// physical meaning; authored decoding belongs strictly after that selection.
fn physical_current_winner_projection_target(
    table_id: PhysicalTableId,
    physical_fields: &[String],
) -> String {
    format!(
        "physical_{}_current_winner_{}",
        table_id.0,
        physical_fields.join("_")
    )
}

/// A query-local current-source target.  The ordinary target projects every
/// authored enum occurrence, which is correct for a whole-row read but makes
/// an older schema fail while decoding an enum cell that the query never
/// consumes.  A narrowed target keeps the fixed logical row shape while
/// replacing unneeded enum cells with typed nulls; the query compiler's
/// requirement closure is therefore the only route by which an enum value is
/// materialized into an authored descriptor.
fn physical_current_projection_target_for_enum_columns(
    schema_alias: SchemaVersionAlias,
    logical_table: &str,
    enum_columns: &BTreeSet<PhysicalColumnId>,
) -> String {
    let base = physical_current_projection_target(schema_alias, logical_table);
    let suffix = enum_columns
        .iter()
        .map(|column| column.0.to_string())
        .collect::<Vec<_>>()
        .join("_");
    format!("{base}_enum_fields_{suffix}")
}

#[derive(Clone, Copy)]
pub(super) enum PhysicalCurrentClass {
    Global,
    Ahead,
}

#[derive(Clone, Copy)]
enum ContentProjectionShape {
    History,
    Current,
}

/// The history storage schema owns physical enum registry identities so it
/// can store every compatible authored version. A query-local history source
/// instead crosses into the reader's authored descriptor, matching current
/// row sources and inline frozen branch bases.
fn authored_history_projection_descriptor(table: &TableSchema) -> records::RecordDescriptor {
    records::RecordDescriptor::new(
        table
            .history_storage_table()
            .record_schema()
            .fields()
            .iter()
            .map(|field| {
                let name = field
                    .name
                    .clone()
                    .expect("Jazz history storage fields are named");
                let value_type = table
                    .columns
                    .iter()
                    .find(|column| app_column_field(&column.name) == name)
                    .map(|column| {
                        records::ValueType::Nullable(Box::new(column.column_type.clone()))
                    })
                    .unwrap_or_else(|| field.value_type.clone());
                (name, value_type)
            }),
    )
}
