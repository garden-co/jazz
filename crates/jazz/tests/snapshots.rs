//! Snapshot references on replicas: a capture freezes the authority state the
//! replica has synced plus its own writes made before the capture, settled or
//! not. Reads of a snapshot are answered from the replica's own store, and a
//! write under a frozen base reaches the authority as a frozen view.

mod common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ExactWriteTarget, InsertOptions, ReadOpts, RemoteLinkHint,
    UpdateOptions, WriteTarget, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{
    BranchSelector, BranchViewBase, ReadViewSourceSpec, ReadViewSpec, SnapshotRef,
};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn alice_subject() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn server() -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn client(node: u8) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice_subject(),
        },
    )))
    .unwrap()
}

fn connect(client: &Db, server: &Db) {
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    client.set_remote_link_hint(RemoteLinkHint::Live);
    server.accept_subscriber(server_side, alice_subject());
}

fn settle(dbs: &[&Db]) {
    for _ in 0..8 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn branch(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

fn title(value: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(value.to_owned()))])
}

fn insert(db: &Db, row: RowUuid, on: Option<&BranchSelector>, value: &str) {
    block_on(db.insert(
        if on.is_some() { "todos" } else { "notes" },
        title(value),
        InsertOptions {
            row_id: Some(row),
            target: on.map_or_else(Default::default, |b| ExactWriteTarget::Branch(b.clone())),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn update(db: &Db, row: RowUuid, target: WriteTarget, value: &str) {
    let table = if target == WriteTarget::default() {
        "notes"
    } else {
        "todos"
    };
    block_on(db.update(
        table,
        row,
        title(value),
        UpdateOptions {
            target,
            ..Default::default()
        },
    ))
    .unwrap();
}

fn on(branch: &BranchSelector) -> WriteTarget {
    WriteTarget::BranchView {
        head: branch.clone(),
        base: None,
    }
}

fn current() -> WriteTarget {
    WriteTarget::default()
}

fn at(snapshot: &SnapshotRef) -> ReadOpts {
    ReadOpts {
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::Snapshot {
                snapshot: snapshot.clone(),
            },
            include_deleted: false,
        },
        ..ReadOpts::default()
    }
}

/// Titles by row, read with `opts`.
fn titles(db: &Db, opts: ReadOpts) -> BTreeMap<RowUuid, String> {
    let table = match opts.read_view.source {
        ReadViewSourceSpec::BranchView { .. } => "todos",
        _ => "notes",
    };
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| {
            let title = row
                .cells()
                .find(|(name, _)| *name == "title")
                .and_then(|(_, value)| value.cloned());
            match title {
                Some(Value::String(title)) => (row.row_uuid(), title),
                other => panic!("unexpected title {other:?}"),
            }
        })
        .collect()
}

fn one(row: RowUuid, value: &str) -> BTreeMap<RowUuid, String> {
    BTreeMap::from([(row, value.to_owned())])
}

#[test]
fn a_snapshot_keeps_reading_the_rows_it_froze_after_later_writes() {
    let db = server();
    let kept = RowUuid::from_bytes([0x11; 16]);
    let gone = RowUuid::from_bytes([0x12; 16]);
    let later = RowUuid::from_bytes([0x13; 16]);
    insert(&db, kept, None, "before");
    insert(&db, gone, None, "doomed");
    let snapshot = block_on(db.capture_snapshot()).unwrap();
    // The reference outlives the process as a string.
    let snapshot = SnapshotRef::from_persisted(&snapshot.to_persisted()).unwrap();

    update(&db, kept, current(), "after");
    insert(&db, later, None, "later");
    block_on(db.delete("notes", gone, Default::default())).unwrap();

    assert_eq!(
        titles(&db, at(&snapshot)),
        BTreeMap::from([(kept, "before".to_owned()), (gone, "doomed".to_owned())])
    );
    assert_eq!(
        titles(&db, ReadOpts::default()),
        BTreeMap::from([(kept, "after".to_owned()), (later, "later".to_owned())])
    );
}

/// A connected replica lets its writes settle before capturing, so the
/// reference is one authority cut that names nothing individually.
#[test]
fn an_online_capture_waits_for_the_replicas_writes_to_settle() {
    let server = server();
    let alice = client(0xa1);
    connect(&alice, &server);
    let row = RowUuid::from_bytes([0x21; 16]);
    insert(&alice, row, None, "before");

    let snapshot = drive(&[&alice, &server, &alice], alice.capture_snapshot()).unwrap();
    assert!(snapshot.dots.is_empty());
    update(&alice, row, current(), "after");
    settle(&[&alice, &server]);

    for db in [&alice, &server] {
        assert_eq!(titles(db, at(&snapshot)), one(row, "before"));
        assert_eq!(titles(db, ReadOpts::default()), one(row, "after"));
    }
}

/// Offline, the replica's unsettled writes are named in the reference. They
/// stay part of the snapshot before and after they settle, while writes made
/// after the capture, its own or another replica's, never are.
#[test]
fn an_offline_capture_names_its_unsettled_writes_and_they_stay_in_after_settling() {
    let server = server();
    let alice = client(0xa1);
    let bob = client(0xb0);
    connect(&bob, &server);
    let row = RowUuid::from_bytes([0x31; 16]);
    let theirs = RowUuid::from_bytes([0x32; 16]);

    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    insert(&alice, row, None, "before");
    let snapshot = block_on(alice.capture_snapshot()).unwrap();
    assert_eq!(snapshot.dots.len(), 1);
    update(&alice, row, current(), "after");
    assert_eq!(titles(&alice, at(&snapshot)), one(row, "before"));
    // Bob's store has not seen Alice's commit yet.
    let error = block_on(bob.all(
        &bob.prepare_query(&Query::from("notes")).unwrap(),
        at(&snapshot),
    ))
    .unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::NotObserved, "{error}");

    insert(&bob, theirs, None, "bob's");
    settle(&[&bob, &server]);
    connect(&alice, &server);
    settle(&[&alice, &server, &bob]);

    assert_eq!(titles(&alice, at(&snapshot)), one(row, "before"));
    assert_eq!(titles(&server, at(&snapshot)), one(row, "before"));
    assert_eq!(
        titles(&server, ReadOpts::default()),
        BTreeMap::from([(row, "after".to_owned()), (theirs, "bob's".to_owned())])
    );
}

/// A draft over a frozen base copies the base row as it was at the capture,
/// and the authority applies the same copy.
#[test]
fn a_write_under_a_frozen_base_copies_the_frozen_row_at_the_authority_too() {
    let server = server();
    let alice = client(0xa1);
    connect(&alice, &server);
    let main = branch(0x04);
    let draft = branch(0x05);
    let row = RowUuid::from_bytes([0x41; 16]);
    let untouched = RowUuid::from_bytes([0x42; 16]);
    insert(&alice, row, Some(&main), "base");
    insert(&alice, untouched, Some(&main), "base");
    let snapshot = drive(&[&alice, &server, &alice], alice.capture_snapshot()).unwrap();
    update(&alice, untouched, on(&main), "later base");
    settle(&[&alice, &server]);

    let frozen = Some(BranchViewBase::snapshot(main.clone(), snapshot));
    block_on(alice.update(
        "todos",
        row,
        BTreeMap::new(),
        UpdateOptions {
            target: WriteTarget::BranchView {
                head: draft.clone(),
                base: frozen.clone(),
            },
            ..Default::default()
        },
    ))
    .unwrap();
    settle(&[&alice, &server]);

    let view = ReadOpts::default().branch_view(draft.clone(), frozen);
    let expected = BTreeMap::from([(row, "base".to_owned()), (untouched, "base".to_owned())]);
    for db in [&alice, &server] {
        assert_eq!(titles(db, view.clone()), expected);
        assert_eq!(
            titles(db, ReadOpts::default().branch_view(draft.clone(), None)),
            one(row, "base"),
            "the copy lands in the draft"
        );
        assert_eq!(
            titles(db, ReadOpts::default().branch_view(main.clone(), None)),
            BTreeMap::from([
                (row, "base".to_owned()),
                (untouched, "later base".to_owned())
            ]),
        );
    }
}
