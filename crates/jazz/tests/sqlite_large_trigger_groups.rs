//! Large-value trigger groups preserve values across schema growth and reopen.
mod common;

use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn schema(added: bool) -> JazzSchema {
    let mut notes = TableSchemaBuilder::new("notes").policies(common::allow_all_policies());
    for index in 0..65 {
        notes = notes.nullable_column(&format!("value{index}"), ColumnType::Text);
    }
    if added {
        notes = notes.nullable_column("later", ColumnType::Text);
    }
    common::compile_schema(&SchemaBuilder::new().table(notes).build())
}
fn config(added: bool) -> DbConfig {
    DbConfig::new(
        schema(added),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

/// Alice writes the same large tree in columns on both sides of trigger-group
/// boundaries. Updates and a later schema column must preserve every surviving
/// reference across close/reopen, including restoration after deletion.
#[test]
fn wide_trigger_groups_survive_schema_growth_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let original = "original-é".repeat(10_000);
    let replacement = "replacement-😀".repeat(8_000);
    let row = {
        let db = block_on(Db::open_at(config(false), &path, Default::default())).unwrap();
        let row = block_on(
            db.insert(
                "notes",
                [0, 31, 32, 64]
                    .into_iter()
                    .map(|index| (format!("value{index}"), Value::String(original.clone())))
                    .collect(),
                Default::default(),
            ),
        )
        .unwrap()
        .row_uuid();
        block_on(db.update(
            "notes",
            row,
            BTreeMap::from([("value32".into(), Value::String(replacement.clone()))]),
            Default::default(),
        ))
        .unwrap();
        block_on(db.activate_schema(schema(true))).unwrap();
        block_on(db.update(
            "notes",
            row,
            BTreeMap::from([("later".into(), Value::String(original.clone()))]),
            Default::default(),
        ))
        .unwrap();
        block_on(db.close()).unwrap();
        row
    };
    for restore in [false, true] {
        let db = block_on(Db::open_at(config(true), &path, Default::default())).unwrap();
        if restore {
            block_on(db.restore("notes", row, None, Default::default())).unwrap();
        }
        for column in ["value0", "value31", "value32", "value64", "later"] {
            let expected = if column == "value32" {
                &replacement
            } else {
                &original
            };
            let actual =
                block_on(db.read_value_range("notes", row, column, 0..expected.len() as u64))
                    .unwrap();
            assert_eq!(actual, expected.as_bytes(), "{column}, restored: {restore}");
        }
        block_on(db.delete("notes", row, Default::default())).unwrap();
        block_on(db.close()).unwrap();
    }
}
