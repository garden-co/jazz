//! The retry store (INV-TX-9): a write the authority refuses leaves the
//! originating store's rows and history, and its payload stays in that
//! store's retry store until the host that reports refusals has reported
//! it, across a reopen too. No other store keeps it.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::path::PathBuf;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, WireOp, WireWrite, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

use common::compile_schema;

/// `notes(owner, title)`: anyone reads, a note is inserted only by its
/// owner.
fn schema() -> JazzSchema {
    let mine = PolicyExpr::Cmp {
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
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(mine),
                    ),
            )
            .build(),
    )
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn identity(node: u8, author: AuthorSubject) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author,
    }
}

fn authority() -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        identity(0xee, AuthorSubject::SYSTEM),
    )))
    .unwrap()
}

fn connect(client: &Db, server: &Db) {
    let (client_side, server_side) = duplex_transport::duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
}

fn pump(client: &Db, server: &Db) {
    for _ in 0..8 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

/// Alice writes a note in bob's name, which the authority refuses.
fn refused_note(client: &Db, row: RowUuid) -> u64 {
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    block_on(client.insert(
        "notes",
        BTreeMap::from([
            ("owner".to_owned(), subject(bob)),
            ("title".to_owned(), Value::String("in bob's name".into())),
        ]),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap()
    .mergeable_tx_id()
}

fn count(db: &Db, query: Query) -> usize {
    let prepared = db.prepare_query(&query).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .len()
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("jazz-retry-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

/// ```text
/// alice ──insert(note in bob's name)──► core ──✗ refused
///   └── rows and history: without it; retry store: its payload
/// core: keeps nothing of it
/// ```
#[test]
fn the_originating_store_keeps_a_refused_payload_until_it_is_reported() {
    let server = authority();
    let client = block_on(Db::open(DbConfig::new(schema(), identity(0xa1, alice())))).unwrap();
    client.track_upstream_rejections();
    connect(&client, &server);
    let row = RowUuid::from_bytes([0x11; 16]);
    let seq = refused_note(&client, row);
    pump(&client, &server);

    assert_eq!(count(&client, Query::from("notes")), 0);
    let history = Query::from("history.notes").filter(eq(col("id"), lit(Value::Uuid(row.0))));
    assert_eq!(
        count(&client, history),
        0,
        "the refused version left history"
    );
    let [rejected] = client.rejected_transactions().unwrap().try_into().unwrap();
    assert_eq!(rejected.seq, seq);
    assert!(
        rejected.error.is_authorization_denied(),
        "{}",
        rejected.error
    );
    let [
        WireOp {
            write:
                WireWrite::Insert {
                    table,
                    row: written,
                    cells,
                    ..
                },
            ..
        },
    ] = rejected.ops.as_slice()
    else {
        panic!("the payload is the refused insert: {:?}", rejected.ops);
    };
    assert_eq!((table.as_str(), written), ("notes", row.0.as_bytes()));
    assert_eq!(
        cells.get("title"),
        Some(&Value::String("in bob's name".into()))
    );
    assert!(server.rejected_transactions().unwrap().is_empty());

    // The host reports it, then lets the payload go.
    let reported = client.take_upstream_rejections();
    assert_eq!(reported.iter().map(|r| r.seq).collect::<Vec<_>>(), [seq]);
    client.discard_rejected_transaction(seq).unwrap();
    assert!(client.rejected_transactions().unwrap().is_empty());
}

/// A refusal the host had not reported when the store closed is kept and
/// reported again once the reopened store's host tracks refusals and asks
/// for the kept ones; a store reopened without such a host leaves it
/// alone.
#[test]
fn an_unreported_refusal_survives_a_reopen() {
    let path = scratch("reopen");
    let server = authority();
    let open = || {
        block_on(Db::open_at(
            DbConfig::new(schema(), identity(0xa1, alice())),
            &path,
            FileOptions::default(),
        ))
        .unwrap()
    };
    let seq = {
        let client = open();
        client.track_upstream_rejections();
        connect(&client, &server);
        let seq = refused_note(&client, RowUuid::from_bytes([0x12; 16]));
        pump(&client, &server);
        assert_eq!(client.rejected_transactions().unwrap().len(), 1);
        seq
    };

    let untracked = open();
    untracked.requeue_rejected_transactions().unwrap();
    assert!(untracked.take_upstream_rejections().is_empty());
    drop(untracked);

    let reopened = open();
    reopened.track_upstream_rejections();
    reopened.requeue_rejected_transactions().unwrap();
    reopened.requeue_rejected_transactions().unwrap();
    assert_eq!(
        reopened
            .take_upstream_rejections()
            .iter()
            .map(|r| r.seq)
            .collect::<Vec<_>>(),
        [seq],
        "reported again after the reopen"
    );
    assert_eq!(reopened.rejected_transactions().unwrap()[0].seq, seq);
    reopened.discard_rejected_transaction(seq).unwrap();
    drop(reopened);
    let again = open();
    again.track_upstream_rejections();
    again.requeue_rejected_transactions().unwrap();
    assert!(again.take_upstream_rejections().is_empty());
}

/// A store whose host does not report refusals keeps no payloads.
#[test]
fn without_a_reporting_host_nothing_is_kept() {
    let server = authority();
    let client = block_on(Db::open(DbConfig::new(schema(), identity(0xa1, alice())))).unwrap();
    connect(&client, &server);
    refused_note(&client, RowUuid::from_bytes([0x13; 16]));
    pump(&client, &server);
    assert_eq!(count(&client, Query::from("notes")), 0);
    assert!(client.rejected_transactions().unwrap().is_empty());
}

/// The retry store comes with the first refusal it keeps: a store written
/// before it existed, reopened and read without a write, stays
/// byte-identical.
#[test]
fn reopening_a_store_without_a_refusal_writes_nothing() {
    let path = scratch("untouched");
    let open = || {
        block_on(Db::open_at(
            DbConfig::new(schema(), identity(0xa1, alice())),
            &path,
            FileOptions::default(),
        ))
        .unwrap()
    };
    {
        let client = open();
        client.track_upstream_rejections();
        block_on(client.insert(
            "notes",
            BTreeMap::from([
                ("owner".to_owned(), subject(alice())),
                ("title".to_owned(), Value::String("mine".into())),
            ]),
            InsertOptions::default(),
        ))
        .unwrap();
    }
    // As written by a build before the retry store.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE IF EXISTS _jazz_rejected")
        .unwrap();
    let before = std::fs::read(&path).unwrap();

    {
        let reopened = open();
        reopened.track_upstream_rejections();
        assert_eq!(count(&reopened, Query::from("notes")), 1);
        assert!(reopened.rejected_transactions().unwrap().is_empty());
    }
    assert!(
        std::fs::read(&path).unwrap() == before,
        "a reopen without a write changed the file"
    );
}
