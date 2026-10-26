//! Policy values a session supplies beyond its identity and claims, and
//! inheritance through an array reference.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
    TableSchemaBuilder, Value as PublicValue,
};

use common::compile_schema;

fn schema() -> JazzSchema {
    let owned = PolicyExpr::Cmp {
        column: "owner".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec![
            "user".to_owned(),
            "identity".to_owned(),
            "subject".to_owned(),
        ]),
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("guest_notes")
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::SessionInList {
                        path: vec!["authMode".to_owned()],
                        values: vec![PublicValue::Text("local-first".to_owned())],
                    })),
            )
            .table(
                TableSchemaBuilder::new("authors")
                    .column("owner", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(owned)),
            )
            .table(
                TableSchemaBuilder::new("bundles")
                    .array_fk_column("writers", "authors")
                    .column("label", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "writers".to_owned(),
                        max_depth: None,
                    })),
            )
            .build(),
    )
}

fn authority() -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn read(db: &Db, table: &str, who: AuthorSubject) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all_for_identity(&prepared, ReadOpts::default(), who))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect()
}

#[test]
fn auth_mode_is_derived_from_the_identitys_issuer() {
    let db = authority();
    let note = block_on(db.insert(
        "guest_notes",
        BTreeMap::from([("body".to_owned(), Value::String("hi".into()))]),
        Default::default(),
    ))
    .unwrap()
    .row_uuid();
    let guest = AuthorSubject::reserved(AuthorSubject::LOCAL_FIRST_ISSUER, "guest-device").unwrap();
    let staff = AuthorSubject::for_test_bytes([0x51; 16]);
    assert_eq!(read(&db, "guest_notes", guest), [note]);
    assert_eq!(read(&db, "guest_notes", staff), Vec::<RowUuid>::new());
}

#[test]
fn an_array_reference_inherits_from_any_row_it_names() {
    let db = authority();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let author = |n: u8, who: AuthorSubject| {
        let id = RowUuid::from_bytes([n; 16]);
        block_on(db.insert(
            "authors",
            BTreeMap::from([("owner".to_owned(), Value::String(who.principal_parts().1))]),
            InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        ))
        .unwrap();
        id
    };
    let a = author(0x0a, alice);
    let b = author(0x0b, bob);
    let bundle = |label: &str, writers: &[RowUuid]| {
        block_on(db.insert(
            "bundles",
            BTreeMap::from([
                (
                    "writers".to_owned(),
                    Value::Array(writers.iter().map(|w| Value::Uuid(w.0)).collect()),
                ),
                ("label".to_owned(), Value::String(label.to_owned())),
            ]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid()
    };
    let mixed = bundle("mixed", &[b, a, a]);
    bundle("empty", &[]);
    let bobs = bundle("bob-only", &[b]);
    assert_eq!(read(&db, "bundles", alice), [mixed]);
    let mut seen = read(&db, "bundles", bob);
    seen.sort();
    let mut expected = vec![mixed, bobs];
    expected.sort();
    assert_eq!(seen, expected);
}
