//! Schema validation of a policy that joins two tables row id to row id:
//! accepted only when both are system tables, whose records keep a private
//! and a public half under one id (E2EE protocol R8); refused between app
//! tables, and between an app table and a system table.
//!
//! ```text
//! __e2ee_halves ◄──(id = this.id)── __e2ee_public_halves   ✓
//! notes         ◄──(id = this.id)── public_notes           ✗
//! notes         ◄──(id = this.id)── __e2ee_public_halves   ✗
//! ```

use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

/// `checked` may insert a row only when `source` has a row with its id.
fn schema(source: &str, checked: &str) -> Result<JazzSchema, String> {
    let same_id = PolicyExpr::Exists {
        table: source.to_owned(),
        condition: Box::new(PolicyExpr::Cmp {
            column: "id".to_owned(),
            op: jazz::tools::CmpOp::Eq,
            value: PolicyValue::SessionRef(vec!["__jazz_outer_row".to_owned(), "id".to_owned()]),
        }),
    };
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new(source).column("body", ColumnType::Text))
            .table(
                TableSchemaBuilder::new(checked)
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_insert(same_id)),
            )
            .build(),
    )
    .map_err(|error| error.to_string())
}

#[test]
fn two_system_tables_join_row_id_to_row_id() {
    schema("__e2ee_halves", "__e2ee_public_halves").unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn app_tables_never_join_row_id_to_row_id() {
    for (source, checked) in [
        ("notes", "public_notes"),
        ("notes", "__e2ee_public_halves"),
        ("__e2ee_halves", "public_notes"),
    ] {
        let error = schema(source, checked).expect_err("refused");
        assert!(
            error.contains("its ids are matched where the ids of"),
            "{source} → {checked}: {error}"
        );
    }
}
