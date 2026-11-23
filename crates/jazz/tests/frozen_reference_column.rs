//! Freezing one column of a table whose other columns stay writable: an
//! insert must leave the column empty, and an update must keep it as it was
//! (`updateTo` comparing with the old row, which compiles to
//! `exists comments(id = this.id, taskId = this.taskId)`). The encryption
//! guide relies on this to keep an old reference column from changing after
//! the switch to a new one.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, UpdateOptions, WriteIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

use common::{compile_schema, exists, outer_eq};

/// `comments`: inserts leave `taskId` empty, and an update keeps it, by
/// `exists comments(id = this.id, taskId = this.taskId)`.
fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("comments")
                    .column("body", ColumnType::Text)
                    .nullable_column("taskId", ColumnType::Uuid)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::IsNull {
                                column: "taskId".to_owned(),
                            })
                            .with_update(
                                Some(PolicyExpr::True),
                                exists(
                                    "comments",
                                    vec![outer_eq("id", "id"), outer_eq("taskId", "taskId")],
                                ),
                            ),
                    ),
            )
            .build(),
    )
}

fn open() -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn task(n: u8) -> Value {
    Value::Nullable(Some(Box::new(Value::Uuid(uuid::Uuid::from_bytes([n; 16])))))
}

const NONE: Value = Value::Nullable(None);

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([9, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

/// A row of `table` whose `taskId` is `task`, written by the system.
fn seed(db: &Db, table: &str, n: u8, task: Value) {
    block_on(db.insert(
        table,
        BTreeMap::from([
            ("body".to_owned(), Value::String("hello".into())),
            ("taskId".to_owned(), task),
        ]),
        InsertOptions {
            row_id: Some(row(n)),
            identity: WriteIdentity::Attribution(AuthorSubject::SYSTEM),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x61; 16])
}

/// Whether a session may update row `n` of `table` with `cells`.
fn may_update(db: &Db, table: &str, n: u8, cells: &[(&str, Value)]) -> bool {
    block_on(
        db.update(
            table,
            row(n),
            cells
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.clone()))
                .collect(),
            UpdateOptions {
                identity: WriteIdentity::Session(alice()),
                ..Default::default()
            },
        ),
    )
    .is_ok()
}

/// Whether a session may insert a row of `table` with `taskId` set to `task`.
fn may_insert(db: &Db, table: &str, task: Value) -> bool {
    block_on(db.insert(
        table,
        BTreeMap::from([
            ("body".to_owned(), Value::String("new".into())),
            ("taskId".to_owned(), task),
        ]),
        InsertOptions {
            identity: WriteIdentity::Session(alice()),
            ..Default::default()
        },
    ))
    .is_ok()
}

/// The other columns stay writable, including on a row whose `taskId` is
/// empty (empty equals empty here); `taskId` itself can't be set, changed
/// or cleared, and an insert can't set it.
#[test]
fn a_column_kept_by_its_update_rule_and_left_empty_by_inserts_stays_frozen() {
    let db = open();
    for (n, task) in [
        (1, task(1)),
        (2, NONE),
        (3, task(1)),
        (4, task(1)),
        (5, NONE),
    ] {
        seed(&db, "comments", n, task);
    }
    let edit = [("body", Value::String("edited".into()))];
    assert!(may_update(&db, "comments", 1, &edit), "a row with a task");
    assert!(may_update(&db, "comments", 2, &edit), "a row without one");
    assert!(
        !may_update(&db, "comments", 3, &[("taskId", task(2))]),
        "changed"
    );
    assert!(
        !may_update(&db, "comments", 4, &[("taskId", NONE)]),
        "cleared"
    );
    assert!(
        !may_update(&db, "comments", 5, &[("taskId", task(1))]),
        "set"
    );
    assert!(may_insert(&db, "comments", NONE), "inserted without a task");
    assert!(!may_insert(&db, "comments", task(1)), "inserted with one");
}
