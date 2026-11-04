//! Public schema admission at the Durable Object physical column boundary.
#![cfg(feature = "durable-object-limits")]

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

fn schema(columns: usize) -> JazzSchema {
    schema_range(0..columns)
}

fn schema_range(columns: std::ops::Range<usize>) -> JazzSchema {
    let mut table = TableSchemaBuilder::new("items").policies(allow_all_policies());
    for index in columns {
        table = table.column_with_default(
            &format!("field_{index}"),
            ColumnType::Integer,
            jazz::tools::Value::Integer(0),
        );
    }
    compile_schema(&SchemaBuilder::new().table(table).build())
}

fn config(columns: usize) -> DbConfig {
    DbConfig::new(
        schema(columns),
        DbIdentity {
            node: NodeUuid::from_bytes([42; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

/// Alice opens a store at the 100-column boundary; Bob's wider schema is
/// rejected with the table and physical limit identified.
/// alice: 88 app + 12 engine -> open; bob: 89 app + 12 engine -> schema error
#[test]
fn physical_column_limit_is_reported_before_ddl() {
    let db = block_on(Db::open(config(88))).expect("88 application columns fit");
    drop(db);
    let error = match block_on(Db::open(config(89))) {
        Ok(_) => panic!("89 application columns exceed the physical limit"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::Schema);
    assert!(error.message.contains("items"), "{error}");
    assert!(error.message.contains("101 physical columns"), "{error}");
    assert!(
        error.message.contains("not supported in Durable Objects"),
        "{error}"
    );
}

/// Alice's live store remains usable under its original schema when Bob
/// proposes an evolution beyond the physical column limit.
/// alice: open -> bob: add column -> reject -> alice: original view still prepares
#[test]
fn oversized_schema_evolution_preserves_the_current_view() {
    let db = block_on(Db::open(config(88))).unwrap();
    let error = block_on(db.activate_schema_with_lenses(schema(89), &[])).unwrap_err();
    assert_eq!(error.code, ErrorCode::Schema);
    assert!(error.message.contains("101 physical columns"), "{error}");
    db.prepare_query(&jazz::query::Query::from("items"))
        .unwrap();
}

/// Alice drops a logical column, but its history still occupies a physical
/// column; Bob cannot use that slot for a new column in a later schema.
/// alice: 88 -> drop field_0 -> bob: add field_88 -> physical union rejected
#[test]
fn retained_migration_columns_count_toward_the_limit() {
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    let db = block_on(Db::open(config(88))).unwrap();
    let dropped = schema_range(1..88);
    let lens = MigrationLens::new(
        schema(88).version_id(),
        dropped.version_id(),
        vec![TableLens {
            source_table: "items".into(),
            target_table: "items".into(),
            ops: vec![LensOp::DropColumn {
                column: "field_0".into(),
                backwards_default: jazz::records::Value::I32(0),
            }],
        }],
    )
    .unwrap();
    block_on(db.activate_schema_with_lenses(dropped.clone(), &[lens])).unwrap();
    let error = block_on(db.activate_schema_with_lenses(schema_range(1..89), &[])).unwrap_err();
    assert_eq!(error.code, ErrorCode::Schema);
    assert!(error.message.contains("101 physical columns"), "{error}");
    assert_eq!(db.current_schema().version_id(), dropped.version_id());
}
