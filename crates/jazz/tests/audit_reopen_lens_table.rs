//! #4769: a table a lens adds is written by the version that adds it. A
//! store reopened under an earlier version (a server whose bundles never
//! activated a revision reopens under its genesis) must still take that
//! version's writes: the stored columns keep the types they were created
//! with.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{Schema, SchemaHash, TableName};
use jazz::tools::schema_lens::runtime::compile_lens;
use jazz::tools::schema_lens::{Lens, LensOp, LensTransform};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn before() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("parents")
                .column("title", ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .build()
}

fn after() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("parents")
                .column("title", ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .table(
            TableSchemaBuilder::new("children")
                .column("parentId", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .build()
}

fn open_at(schema: JazzSchema, path: &std::path::Path) -> Db {
    let db = block_on(Db::open_at(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x5e; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        path,
        FileOptions::default(),
    ))
    .unwrap();
    db.set_authority();
    db
}

fn insert_child(db: &Db, n: u8) -> Result<(), jazz::db::Error> {
    block_on(db.insert(
        "children",
        BTreeMap::from([
            (
                "parentId".to_owned(),
                Value::Uuid(uuid::Uuid::from_bytes([n; 16])),
            ),
            ("name".to_owned(), Value::String(format!("c{n}"))),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([n; 16])),
            ..Default::default()
        },
    ))
    .map(|_| ())
}

#[test]
fn a_lens_added_table_takes_writes_after_reopening_under_an_earlier_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let (v1, v2) = (before(), after());
    let lens = Lens::new(
        SchemaHash::compute(&v1),
        SchemaHash::compute(&v2),
        LensTransform::with_ops(vec![LensOp::AddTable {
            table: "children".into(),
            schema: {
                let mut table = v2[&TableName::new("children")].clone();
                table.policies = Default::default();
                table
            },
        }]),
    );
    let migration = compile_lens(&lens, &v1, &v2).unwrap();
    {
        let db = open_at(compile_schema(&v1), &path);
        block_on(db.admit_schema_with_lenses(compile_schema(&v2), &[migration.clone()])).unwrap();
        let view = block_on(db.register_schema_view(compile_schema(&v2))).unwrap();
        insert_child(&view, 1).expect("first start");
    }
    let db = open_at(compile_schema(&v1), &path);
    let view = block_on(db.register_schema_view(compile_schema(&v2))).unwrap();
    insert_child(&view, 2).expect("after reopen");
}
