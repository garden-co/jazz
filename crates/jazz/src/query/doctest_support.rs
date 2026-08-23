#[doc(hidden)]
pub mod doctest_support {
    use crate::schema::JazzSchema;
    use crate::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

    /// Example schema used by query-builder doctests.
    pub fn schema() -> JazzSchema {
        let source = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("issues")
                    .column("title", ColumnType::Text)
                    .column("state", ColumnType::Text)
                    .fk_column("assignee", "users")
                    .fk_column("project", "projects")
                    .column("priority", ColumnType::Timestamp)
                    .column(
                        "labels",
                        ColumnType::Array {
                            element: Box::new(ColumnType::Text),
                        },
                    )
                    .nullable_column("snoozed_until", ColumnType::Timestamp),
            )
            .table(
                TableSchemaBuilder::new("issue_tags")
                    .fk_column("issue", "issues")
                    .fk_column("tag", "tags"),
            )
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .fk_column("org", "orgs"),
            )
            .table(TableSchemaBuilder::new("orgs").column("name", ColumnType::Text))
            .table(TableSchemaBuilder::new("users").column("name", ColumnType::Text))
            .table(TableSchemaBuilder::new("tags").column("name", ColumnType::Text))
            .build();
        JazzSchema::new(&source).expect("query doctest public schema compiles")
    }
}
