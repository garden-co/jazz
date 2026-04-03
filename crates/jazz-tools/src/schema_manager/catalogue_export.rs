#[cfg(test)]
mod tests {
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema, Value};

    #[test]
    fn catalogue_schema_response_serializes_tables_columns_and_defaults() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column_with_default("active", ColumnType::Boolean, Value::Boolean(true))
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let response = schema.clone();
        let json = serde_json::to_value(response).expect("serialize schema response");

        let users = &json["users"];
        assert_eq!(users["columns"][0]["name"], "id");
        assert_eq!(users["columns"][0]["column_type"]["type"], "Uuid");
        assert_eq!(users["columns"][1]["name"], "active");
        assert_eq!(users["columns"][1]["default"]["type"], "Boolean");
        assert_eq!(users["columns"][1]["default"]["value"], true);
        assert_eq!(users["columns"][2]["name"], "email");
        assert_eq!(users["columns"][2]["column_type"]["type"], "Text");
        assert_eq!(users["columns"][2]["nullable"], true);
        assert!(users["columns"][0].get("references").is_none());
        assert!(users["columns"][1].get("references").is_none());
        assert!(users["columns"][2].get("references").is_none());
        assert!(users["columns"][2].get("default").is_none());
    }
}
