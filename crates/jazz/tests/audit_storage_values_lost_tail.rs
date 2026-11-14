//! State-space audit (storage-values): a replica whose durable file lost its
//! newest commits after they were uploaded.
//!
//! A browser replica's IndexedDB store applies commits asynchronously
//! (`crates/jazz-wasm/src/browser.rs`: "a crash can lose the newest ones
//! until `Store::flush` resolves"), while the core already sent them
//! upstream. After such a crash the reopened file is a prefix of what the
//! authority holds. The replica numbers its commits `seq@node` from what its
//! file holds (`load_client`), so the next commit may reuse a transaction id
//! the authority already applied and dedupes (INV-EDGE-16).
//!
//! These are failing repros, not decisions: they assert what a user would
//! expect (the new write lands).

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, RemoteLinkHint, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
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

fn titles(db: &Db) -> BTreeMap<u8, String> {
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|current| {
            let title = match current.cells().find(|(n, _)| *n == "title") {
                Some((_, Some(Value::String(value)))) => value.clone(),
                other => panic!("title: {other:?}"),
            };
            (current.row_uuid().as_bytes()[0], title)
        })
        .collect()
}

fn insert_and_settle(client: &Db, server: &Db, n: u8, title: &str) {
    block_on(client.insert(
        "docs",
        BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
    drive(
        &[client, server, client],
        client.wait_for_pending_writes(DurabilityTier::Global),
    )
    .unwrap();
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

fn replace_dir(from: &Path, to: &Path) {
    std::fs::remove_dir_all(to).unwrap();
    copy_dir(from, to);
}

/// alice's browser replica uploads a commit, the authority accepts it, and
/// the tab dies before IndexedDB persisted that commit. On reload her file
/// is the state before it. Her next, different write must still land.
///
/// ```
/// alice ──insert "one"──► server (accepted)      file: {one}  ← backup
/// alice ──insert "two"──► server (accepted)      file lost "two" (crash)
/// alice (reopened from {one}) ──insert "three"──► server
///                                     expect server: {one, two, three}
/// ```
#[test]
fn audit_storage_values_a_replica_that_lost_its_uploaded_tail_still_lands_new_writes() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("live");
    let backup = dir.path().join("backup");
    std::fs::create_dir_all(&live).unwrap();
    let path = live.join("replica.sqlite");
    let server = block_on(Db::open_history_complete(config(0xee))).unwrap();

    {
        let client = open_file(&path);
        connect(&client, &server);
        insert_and_settle(&client, &server, 1, "one");
    }
    // The durable prefix the crash will leave behind.
    copy_dir(&live, &backup);
    {
        let client = open_file(&path);
        connect(&client, &server);
        insert_and_settle(&client, &server, 2, "two");
    }
    assert_eq!(
        titles(&server),
        BTreeMap::from([(1, "one".to_owned()), (2, "two".to_owned())])
    );

    // The crash: IndexedDB never applied the commit of "two".
    replace_dir(&backup, &live);

    let client = open_file(&path);
    connect(&client, &server);
    insert_and_settle(&client, &server, 3, "three");

    let on_server = titles(&server);
    eprintln!("client holds {:?}", titles(&client));
    assert_eq!(
        on_server.get(&3).map(String::as_str),
        Some("three"),
        "alice's new write reached the authority (server holds {on_server:?})"
    );
    // And her replica converges with the authority.
    drive(&[&client, &server, &client], async {});
    assert_eq!(titles(&client).get(&3).map(String::as_str), Some("three"));
}
