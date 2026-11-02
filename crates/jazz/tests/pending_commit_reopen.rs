//! A replica's pending commit is stored a row per op and list item (SPEC
//! 8.3): a serializable commit with reads, chunked-cell deltas, sources and
//! bases must reload every part after a restart and settle as written.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, FileOptions, InsertOptions, LargeValueUpdate,
    LargeValueUpdatePage, LargeValueUpdateSplice, ReadOpts, RemoteLinkHint, WireOp, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

const BODY: usize = 200 * 1024;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn config(node: u8) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn open_file(path: &Path) -> Db {
    block_on(Db::open_at(config(0x61), path, FileOptions::default())).unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

/// Non-repeating text, so the body chunks into an ordinary tree.
fn text(len: usize, seed: u64) -> String {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn connect(client: &Db, server: &Db) {
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    client.set_remote_link_hint(RemoteLinkHint::Live);
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
}

fn docs(db: &Db) -> BTreeMap<[u8; 16], (String, String)> {
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|current| {
            let cell = |name: &str| match current.cells().find(|(n, _)| *n == name) {
                Some((_, Some(Value::String(value)))) => value.clone(),
                other => panic!("{name}: {other:?}"),
            };
            (
                *current.row_uuid().as_bytes(),
                (cell("title"), cell("body")),
            )
        })
        .collect()
}

/// The stored ops of the pending commits, in order.
fn stored_ops(path: &Path) -> Vec<WireOp> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut statement = conn
        .prepare("SELECT item FROM _jazz_pending_items WHERE part = 0 ORDER BY tx, n")
        .unwrap();
    statement
        .query_map([], |r| r.get::<_, Vec<u8>>(0))
        .unwrap()
        .map(|item| postcard::from_bytes(&item.unwrap()).unwrap())
        .collect()
}

fn stored_parts(path: &Path) -> BTreeSet<i64> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut statement = conn
        .prepare("SELECT DISTINCT part FROM _jazz_pending_items")
        .unwrap();
    statement
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn a_pending_serializable_commit_reloads_every_stored_part_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let server = block_on(Db::open_history_complete(config(0xee))).unwrap();
    let (first, second, replaced) = (text(BODY, 3), text(BODY, 5), text(BODY, 7));
    {
        let client = open_file(&path);
        connect(&client, &server);
        for (n, body) in [(1, &first), (2, &second)] {
            block_on(client.insert(
                "docs",
                BTreeMap::from([
                    ("title".to_owned(), Value::String(format!("doc {n}"))),
                    ("body".to_owned(), Value::String(body.clone())),
                ]),
                InsertOptions {
                    row_id: Some(row(n)),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        drive(
            &[&client, &server, &client],
            client.wait_for_pending_writes(DurabilityTier::Global),
        )
        .unwrap();
    }

    // Offline, one serializable commit: it reads the docs, splices row 1's
    // body (a delta over its settled version) and gives row 2 a new body,
    // which travels as a tree with its settled version as its access proof. Both rows
    // were held, so each write has a base.
    let tx = {
        let client = open_file(&path);
        let open = OpenTransactionId::new();
        block_on(client.begin_serializable(open)).unwrap();
        let all = client.prepare_query(&Query::from("docs")).unwrap();
        assert_eq!(
            block_on(client.tx_ref(open).all_prepared(&all))
                .unwrap()
                .len(),
            2
        );
        block_on(client.tx_ref(open).update_with_large_value_mutations(
            "docs",
            row(1),
            BTreeMap::new(),
            vec![LargeValueUpdate::Splice {
                column: "body".into(),
                within: LargeValueUpdatePage::TextUtf16 {
                    from: 0,
                    to: BODY as u64,
                },
                splices: vec![LargeValueUpdateSplice {
                    at: 10,
                    delete: 2,
                    insert: b"EDIT".to_vec(),
                }],
            }],
            Default::default(),
        ))
        .unwrap();
        block_on(client.tx_ref(open).update(
            "docs",
            row(2),
            BTreeMap::from([
                ("title".to_owned(), Value::String("renamed".to_owned())),
                ("body".to_owned(), Value::String(replaced.clone())),
            ]),
            Default::default(),
        ))
        .unwrap();
        block_on(client.commit_serializable_handle(open)).unwrap().0
    };
    // Ops and reads (see `PendingPart`); each op keeps its base, and its
    // delta or access proof.
    assert_eq!(
        stored_parts(&path),
        BTreeSet::from([0, 1]),
        "every list of the commit is stored"
    );
    let ops = stored_ops(&path);
    assert_eq!(ops.len(), 2, "{ops:?}");
    assert!(ops.iter().all(|op| op.bases.len() == 1), "{ops:?}");
    assert!(
        ops[0].deltas.len() == 1 && ops[0].access_proof.is_none(),
        "{ops:?}"
    );
    assert!(
        ops[1].deltas.is_empty() && ops[1].access_proof.is_some(),
        "{ops:?}"
    );

    let client = open_file(&path);
    connect(&client, &server);
    drive(
        &[&client, &server, &client],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    let edited = format!("{}EDIT{}", &first[..10], &first[12..]);
    let expected = BTreeMap::from([
        ([1; 16], ("doc 1".to_owned(), edited)),
        ([2; 16], ("renamed".to_owned(), replaced)),
    ]);
    assert_eq!(
        docs(&server),
        expected,
        "the reloaded commit applies as written"
    );
    assert_eq!(docs(&client), expected);
    drop(client);
    assert!(
        stored_parts(&path).is_empty(),
        "its rows go once it settles"
    );
}

/// A development store from before pending uploads were stored a row per
/// op is refused as another format, and left as it was.
#[test]
fn a_store_with_the_old_message_outbox_is_refused_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.sqlite");
    drop(open_file(&path));
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "DROP TABLE _jazz_pending; CREATE TABLE _jazz_pending \
             (tx INTEGER PRIMARY KEY, commit_msg BLOB NOT NULL, rows BLOB NOT NULL);",
        )
        .unwrap();
    let before = std::fs::read(&path).unwrap();
    let Err(error) = block_on(Db::open_at(config(0x61), &path, FileOptions::default())) else {
        panic!("the old store opened");
    };
    assert_eq!(error.code, ErrorCode::UnsupportedStorageCodecs, "{error}");
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "the file is unchanged"
    );
}

/// An update that changes nothing makes no commit, even right after the
/// write that made the row's current version: it must not record a second
/// upload under that commit's transaction id, which would fail the write,
/// or (stored over the first) upload the empty update in its place after
/// a restart.
#[test]
fn an_update_that_changes_nothing_records_no_upload() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let server = block_on(Db::open_history_complete(config(0xee))).unwrap();
    let doc = |title: &str| {
        BTreeMap::from([
            ("title".to_owned(), Value::String(title.to_owned())),
            ("body".to_owned(), Value::String("short".to_owned())),
        ])
    };
    let (inserted, unchanged) = {
        let client = open_file(&path);
        client.become_replica();
        let inserted = block_on(client.insert(
            "docs",
            doc("kept"),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap()
        .mergeable_tx_id();
        let unchanged =
            block_on(client.update("docs", row(1), BTreeMap::new(), Default::default()))
                .unwrap()
                .mergeable_tx_id();
        block_on(client.insert(
            "docs",
            doc("next"),
            InsertOptions {
                row_id: Some(row(2)),
                ..Default::default()
            },
        ))
        .unwrap();
        (inserted, unchanged)
    };
    assert_eq!(
        unchanged, inserted,
        "the handle names the row's current version"
    );
    let client = open_file(&path);
    connect(&client, &server);
    drive(
        &[&client, &server, &client],
        client.wait_for_pending_writes(DurabilityTier::Global),
    )
    .unwrap();
    let titles: Vec<String> = docs(&server)
        .into_values()
        .map(|(title, _)| title)
        .collect();
    assert_eq!(
        titles,
        ["kept", "next"],
        "both real commits reach the server"
    );
}

/// A store whose pending rows this build does not lay out that way (from
/// another development build of format 1) is refused as another format,
/// never misread or reported as a damaged store.
#[test]
fn pending_rows_in_another_layout_are_refused_as_another_format() {
    for (case, edit) in [
        (
            "an unknown item part",
            "UPDATE _jazz_pending_items SET part = 7",
        ),
        (
            "a mergeable commit with reads",
            "UPDATE _jazz_pending_items SET part = 1",
        ),
        (
            "a row id that is not 16 bytes",
            "UPDATE _jazz_pending_rows SET id = x'0102'",
        ),
        (
            "a header that is not a commit",
            "UPDATE _jazz_pending SET header = x'00'",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replica.sqlite");
        {
            let server = block_on(Db::open_history_complete(config(0xee))).unwrap();
            let client = open_file(&path);
            connect(&client, &server);
            // Offline: the write stays pending.
            drop(server);
            block_on(client.insert(
                "docs",
                BTreeMap::from([
                    ("title".to_owned(), Value::String("doc".to_owned())),
                    ("body".to_owned(), Value::String("short".to_owned())),
                ]),
                InsertOptions {
                    row_id: Some(row(1)),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        let conn = rusqlite::Connection::open(&path).unwrap();
        let pending: i64 = conn
            .query_row("SELECT count(*) FROM _jazz_pending", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pending, 1, "{case}: the write is pending");
        conn.execute_batch(edit).unwrap();
        drop(conn);
        let Err(error) = block_on(Db::open_at(config(0x61), &path, FileOptions::default())) else {
            panic!("{case}: the store opened");
        };
        assert_eq!(
            error.code,
            ErrorCode::UnsupportedStorageCodecs,
            "{case}: {error}"
        );
    }
}
