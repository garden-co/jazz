//! The identities a JavaScript host exposes are the authority's stored
//! identities, scoped to the binding's schema view, without granting row access.

use crate::common::{compile_schema, read_and_allow_all_writes};
use jazz::binding_host::{Binding, HostError, Link};
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, FileOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::tools::{ColumnType, PolicyExpr, Schema, SchemaBuilder, TableSchemaBuilder};
use serde_json::json;

fn schema(table: &str, column: &str) -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new(table)
                .column(column, ColumnType::Text)
                .policies(read_and_allow_all_writes(PolicyExpr::False)),
        )
        .build()
}

fn identity(node: u8, backend: bool) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author: if backend {
            AuthorSubject::SYSTEM
        } else {
            AuthorSubject::for_test_bytes([node; 16])
        },
    }
}

fn open(node: u8, backend: bool) -> Binding {
    let id = identity(node, backend);
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&schema("notes", "title")),
        id,
    )))
    .unwrap();
    if backend {
        db.set_authority();
    }
    Binding::new(db, id, backend)
}

fn not_observed(result: Result<Option<[u8; 16]>, HostError>) {
    match result.unwrap_err() {
        HostError::Engine(error) => assert_eq!(error.code, ErrorCode::NotObserved),
        other => panic!("expected NotObserved, got {other:?}"),
    }
}

fn pump(client: &Link, server: &Link) {
    for _ in 0..8 {
        server.receive(client.take_outbound());
        client.receive(server.take_outbound());
        block_on(client.binding().tick()).unwrap();
        block_on(server.binding().tick()).unwrap();
    }
}

/// Alice's host gets canonical UUID bytes, Bob's independent app gets
/// different identities, and unknown names stay absent after admission.
#[test]
fn authority_bytes_are_app_specific_and_unknown_names_stay_absent() {
    let alice = open(1, true);
    let bob = open(2, true);
    let table = alice.table_identity("notes").unwrap().unwrap();
    let column = alice.column_identity("notes", "title").unwrap().unwrap();
    assert_eq!(
        table,
        *alice
            .db
            .table_identity("notes")
            .unwrap()
            .unwrap()
            .0
            .as_bytes()
    );
    assert_eq!(
        column,
        *alice
            .db
            .column_identity("notes", "title")
            .unwrap()
            .unwrap()
            .0
            .as_bytes()
    );
    assert_ne!(table, column);
    assert_ne!(Some(table), bob.table_identity("notes").unwrap());
    assert_ne!(Some(column), bob.column_identity("notes", "title").unwrap());
    assert_eq!(alice.table_identity("missing").unwrap(), None);
    assert_eq!(alice.column_identity("notes", "missing").unwrap(), None);
    assert_eq!(alice.column_identity("missing", "title").unwrap(), None);
}

/// Alice's replica refuses identities before authority admission (including
/// unknown names), learns the authority's IDs, then retains them offline.
#[test]
fn replica_requires_admission_and_keeps_ids_after_reopen() {
    let server = open(1, true);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let id = identity(2, false);
    let open_file = || {
        let db = block_on(Db::open_at(
            DbConfig::new(compile_schema(&schema("notes", "title")), id),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        Binding::new(db, id, false)
    };
    {
        let alice = open_file();
        not_observed(alice.table_identity("notes"));
        not_observed(alice.column_identity("notes", "title"));
        not_observed(alice.table_identity("missing"));
        not_observed(alice.column_identity("missing", "missing"));
        let up = alice.connect_upstream();
        let down = server
            .accept_subscriber(id.author, Default::default())
            .unwrap();
        pump(&up, &down);
        assert_eq!(
            alice.table_identity("notes").unwrap(),
            server.table_identity("notes").unwrap()
        );
        assert_eq!(
            alice.column_identity("notes", "title").unwrap(),
            server.column_identity("notes", "title").unwrap()
        );
    }
    let offline = open_file();
    assert_eq!(
        offline.table_identity("notes").unwrap(),
        server.table_identity("notes").unwrap()
    );
    assert_eq!(
        offline.column_identity("notes", "title").unwrap(),
        server.column_identity("notes", "title").unwrap()
    );
}

/// Alice renames her table and column; old and current typed bindings keep
/// the same stored identities while accepting only their own visible names.
#[test]
fn typed_bindings_follow_rename_lenses() {
    let alice = open(1, true);
    let old_schema = schema("notes", "title");
    let new_schema = schema("tasks", "body");
    let v1 = compile_schema(&old_schema);
    let v2 = compile_schema(&new_schema);
    let table = alice.table_identity("notes").unwrap();
    let column = alice.column_identity("notes", "title").unwrap();
    let lens = MigrationLens::new(
        v1.version_id(),
        v2.version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "tasks".into(),
            ops: vec![LensOp::RenameColumn {
                from: "title".into(),
                to: "body".into(),
            }],
        }],
    )
    .unwrap();
    block_on(alice.db.activate_schema_with_lenses(v2, &[lens])).unwrap();
    let current = alice
        .register_schema(&serde_json::to_vec(&new_schema).unwrap())
        .unwrap();
    let old = alice
        .register_schema(&serde_json::to_vec(&old_schema).unwrap())
        .unwrap();
    assert_eq!(current.table_identity("tasks").unwrap(), table);
    assert_eq!(current.column_identity("tasks", "body").unwrap(), column);
    assert_eq!(current.table_identity("notes").unwrap(), None);
    assert_eq!(old.table_identity("notes").unwrap(), table);
    assert_eq!(old.column_identity("notes", "title").unwrap(), column);
    assert_eq!(old.table_identity("tasks").unwrap(), None);
}

/// Mallory can learn catalogue identities but still cannot read Alice's
/// rows when SELECT is denied by the authority's policy.
#[test]
fn learning_identity_does_not_grant_row_access() {
    let alice = open(1, true);
    block_on(
        alice.db.insert(
            "notes",
            [(
                "title".into(),
                jazz::records::Value::String("private".into()),
            )]
            .into(),
            Default::default(),
        ),
    )
    .unwrap();
    let mallory = open(2, false);
    let up = mallory.connect_upstream();
    let down = alice
        .accept_subscriber(identity(2, false).author, Default::default())
        .unwrap();
    pump(&up, &down);
    assert!(mallory.table_identity("notes").unwrap().is_some());
    assert!(mallory.column_identity("notes", "title").unwrap().is_some());
    let mut rows = mallory
        .all(
            postcard::to_allocvec(&Query::from("notes")).unwrap(),
            &json!({"tier":"remote"}),
            None,
            None,
            None,
        )
        .unwrap();
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..30 {
        if let Poll::Ready(bytes) = Pin::new(&mut rows).poll(&mut cx) {
            assert!(
                jazz::binding_codec::decode_rows(&bytes.unwrap())
                    .unwrap()
                    .iter()
                    .all(|batch| batch.rows.is_empty())
            );
            return;
        }
        pump(&up, &down);
    }
    panic!("permission-filtered read did not settle");
}
