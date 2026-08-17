#[doc(hidden)]
pub mod doctest_support {
    use groove::schema::{ColumnSchema, ColumnType};

    use crate::schema::{JazzSchema, TableSchema};

    /// Example schema used by query-builder doctests.
    pub fn schema() -> JazzSchema {
        JazzSchema::new([
            TableSchema::new(
                "issues",
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("state", ColumnType::String),
                    ColumnSchema::new("assignee", ColumnType::Uuid),
                    ColumnSchema::new("project", ColumnType::Uuid),
                    ColumnSchema::new("priority", ColumnType::U64),
                    ColumnSchema::new("labels", ColumnType::String.array_of()),
                    ColumnSchema::new("snoozed_until", ColumnType::U64.nullable()),
                ],
            )
            .with_reference("assignee", "users")
            .with_reference("project", "projects"),
            TableSchema::new(
                "issue_tags",
                [
                    ColumnSchema::new("issue", ColumnType::Uuid),
                    ColumnSchema::new("tag", ColumnType::Uuid),
                ],
            )
            .with_reference("issue", "issues")
            .with_reference("tag", "tags"),
            TableSchema::new(
                "projects",
                [
                    ColumnSchema::new("name", ColumnType::String),
                    ColumnSchema::new("org", ColumnType::Uuid),
                ],
            )
            .with_reference("org", "orgs"),
            TableSchema::new("orgs", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("tags", [ColumnSchema::new("name", ColumnType::String)]),
        ])
    }
}
