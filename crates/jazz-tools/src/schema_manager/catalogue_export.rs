#[cfg(test)]
mod tests {
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};

    #[test]
    fn catalogue_schema_response_serializes_tables_and_columns() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let response = schema.clone();
        let json = serde_json::to_value(response).expect("serialize schema response");

        let users = &json["users"];
        assert_eq!(users["columns"][0]["name"], "id");
        assert_eq!(users["columns"][0]["column_type"]["type"], "Uuid");
        assert_eq!(users["columns"][1]["name"], "email");
        assert_eq!(users["columns"][1]["column_type"]["type"], "Text");
        assert_eq!(users["columns"][1]["nullable"], true);
    }
}
