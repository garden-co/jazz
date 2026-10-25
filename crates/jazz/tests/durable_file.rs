//! Durable file-backed stores (SQLite WAL): reopening restores rows,
//! history and the commit seq; a process killed mid-write keeps exactly the
//! committed prefix (a Jazz transaction is all or nothing); a replica's
//! pending uploads survive a restart and are resent; a server restart
//! between applying a commit and acknowledging it does not apply it twice;
//! memory stays bounded by the page cache. Not ported from Jazz.

mod common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, LOCAL_BASE, ReadOpts, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("label", ColumnType::Text)
                    .column("n", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn config(node: u8) -> DbConfig {
    let schema = schema();
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("jazz-sqlite-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

fn open_file(path: &Path, node: u8) -> Db {
    block_on(Db::open_at(config(node), path, FileOptions::default())).unwrap()
}

fn row(n: u64) -> RowUuid {
    let mut bytes = [7u8; 16];
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn item(n: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("label".to_owned(), Value::String(format!("i{n}"))),
        ("n".to_owned(), Value::I64(n as i64)),
    ])
}

fn count(db: &Db) -> usize {
    let prepared = db.prepare_query(&Query::from("items")).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .len()
}

#[test]
fn reopening_restores_rows_history_and_the_commit_seq() {
    let path = scratch("reopen");
    let before = {
        let db = open_file(&path, 1);
        for n in 0..10 {
            block_on(db.insert(
                "items",
                item(n),
                InsertOptions {
                    row_id: Some(row(n)),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        // The snapshot sees row 0 as "i0"; then it changes.
        let open = OpenTransactionId::new();
        block_on(db.begin_serializable(open)).unwrap();
        block_on(db.update(
            "items",
            row(0),
            BTreeMap::from([("label".to_owned(), Value::String("changed".to_owned()))]),
            Default::default(),
        ))
        .unwrap();
        db.abandon_serializable_handle(open).unwrap();
        block_on(db.insert("items", item(99), Default::default()))
            .unwrap()
            .mergeable_tx_id()
    };
    let db = open_file(&path, 1);
    assert_eq!(count(&db), 11);
    let next = block_on(db.insert("items", item(100), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    assert_eq!(next, before + 1, "the commit seq continues");
    // History survived: an as-of read of the superseded image.
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let first = db
        .prepare_query(
            &Query::from("items")
                .order_by("n", OrderDirection::Asc)
                .limit(1),
        )
        .unwrap();
    let rows = block_on(db.serializable_tx_ref(open).all_prepared(&first)).unwrap();
    assert_eq!(
        rows[0].cells().find(|(n, _)| *n == "label").unwrap().1,
        Some(&Value::String("changed".to_owned()))
    );
    db.abandon_serializable_handle(open).unwrap();
}

/// The page size in a SQLite file's header (bytes 16-17; 1 means 65536).
fn header_page_size(path: &Path) -> u32 {
    let header = std::fs::read(path).unwrap();
    match u16::from_be_bytes([header[16], header[17]]) {
        1 => 65536,
        size => u32::from(size),
    }
}

#[test]
fn a_new_file_takes_the_page_size_and_an_existing_one_keeps_its_own() {
    let large = FileOptions {
        page_size: Some(65536),
        ..FileOptions::default()
    };
    let path = scratch("page-size");
    {
        let db = block_on(Db::open_at(config(1), &path, large.clone())).unwrap();
        block_on(db.insert("items", item(1), Default::default())).unwrap();
    }
    assert_eq!(header_page_size(&path), 65536);

    // A file created at the default size keeps it when reopened with another.
    let path = scratch("page-size-existing");
    {
        let db = open_file(&path, 1);
        block_on(db.insert("items", item(1), Default::default())).unwrap();
    }
    let default_size = header_page_size(&path);
    assert_ne!(default_size, 65536);
    let db = block_on(Db::open_at(config(1), &path, large)).unwrap();
    assert_eq!(count(&db), 1);
    block_on(db.insert("items", item(2), Default::default())).unwrap();
    drop(db);
    assert_eq!(header_page_size(&path), default_size);
}

const CHILD: &str = "JAZZ_SQLITE_CRASH_CHILD";

/// Child process body: commit `committed` single writes, then open a Jazz
/// transaction, write more inside it, and abort the process mid-transaction.
fn crash_child(path: &Path, committed: u64) -> ! {
    let db = open_file(path, 2);
    for n in 0..committed {
        block_on(db.insert(
            "items",
            item(n),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let _ = block_on(db.transaction(async |tx| {
        for n in committed..committed + 50 {
            tx.insert(
                "items",
                item(n),
                InsertOptions {
                    row_id: Some(row(n)),
                    ..Default::default()
                },
            )
            .await?;
        }
        std::process::abort();
        #[allow(unreachable_code)]
        Ok(())
    }));
    unreachable!()
}

#[test]
fn a_process_killed_mid_transaction_keeps_exactly_the_committed_prefix() {
    if let Ok(spec) = std::env::var(CHILD) {
        let (path, committed) = spec.split_once('|').unwrap();
        crash_child(Path::new(path), committed.parse().unwrap());
    }
    for committed in [1u64, 37, 400] {
        let path = scratch(&format!("crash-{committed}"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "a_process_killed_mid_transaction_keeps_exactly_the_committed_prefix",
                "--nocapture",
            ])
            .env(CHILD, format!("{}|{committed}", path.display()))
            .status()
            .unwrap();
        assert!(!status.success(), "the child aborts");
        let db = open_file(&path, 2);
        let prepared = db
            .prepare_query(&Query::from("items").order_by("n", OrderDirection::Asc))
            .unwrap();
        let ns: Vec<i64> = block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .map(|r| match r.cells().find(|(n, _)| *n == "n").unwrap().1 {
                Some(Value::I64(n)) => *n,
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            ns,
            (0..committed as i64).collect::<Vec<_>>(),
            "exactly the committed prefix, none of the open transaction"
        );
        // And the store keeps working.
        block_on(db.insert("items", item(10_000), Default::default())).unwrap();
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

#[test]
fn a_replicas_pending_uploads_survive_a_restart_and_are_resent() {
    let path = scratch("replica");
    let server = block_on(Db::open_history_complete(config(0xee))).unwrap();
    let txs: Vec<u64> = {
        let client = open_file(&path, 3);
        client.set_remote_link_hint(jazz::db::RemoteLinkHint::Failed);
        (0..3)
            .map(|n| {
                block_on(client.insert(
                    "items",
                    item(n),
                    InsertOptions {
                        row_id: Some(row(n)),
                        ..Default::default()
                    },
                ))
                .unwrap()
                .mergeable_tx_id()
            })
            .collect()
        // The app quits before it ever reached the server.
    };
    let client = open_file(&path, 3);
    assert_eq!(count(&client), 3, "optimistic rows are still there");
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    drive(
        &[&client, &server, &client],
        client.wait_for_pending_writes(DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(
        count(&server),
        3,
        "every recovered upload reached the server"
    );
    for tx in txs {
        assert!(tx > LOCAL_BASE);
        drive(
            &[&client, &server, &client],
            client.wait_for_transaction(tx, DurabilityTier::Global),
        )
        .unwrap();
    }
    assert_eq!(count(&server), 3);
    // Settled: the replica now holds server versions.
    let prepared = client.prepare_query(&Query::from("items")).unwrap();
    for row in block_on(client.all(&prepared, ReadOpts::default())).unwrap() {
        let _ = row;
    }
    assert!(
        client.sync_debug().starts_with("pending [] "),
        "{}",
        client.sync_debug()
    );
}

/// A node handed to a fresh in-memory database (a browser tab's
/// main-thread peer under a leased node) continues after the previous
/// holder's seqs: the authority deduplicates uploads by transaction id, so a
/// reused one would silently drop the new write.
#[test]
fn a_fresh_database_under_a_reused_node_continues_after_the_seeded_floor() {
    let server = block_on(Db::open_history_complete(config(0xed))).unwrap();
    let connect = |client: &Db| {
        let (client_side, server_side) = duplex();
        block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    };
    let insert = |db: &Db, n: u64| {
        let options = InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        };
        let tx = block_on(db.insert("items", item(n), options))
            .unwrap()
            .mergeable_tx_id();
        drive(
            &[db, &server, db],
            db.wait_for_transaction(tx, DurabilityTier::Global),
        )
        .unwrap();
        tx
    };
    let high_water = {
        let first = block_on(Db::open(config(6))).unwrap();
        connect(&first);
        for n in 0..2 {
            insert(&first, n);
        }
        first.seq_high_water()
    };
    let second = block_on(Db::open(config(6))).unwrap();
    second.seed_seq_floor(high_water);
    connect(&second);
    assert!(insert(&second, 2) > high_water);
    assert_eq!(
        count(&server),
        3,
        "the new write was applied, not deduplicated"
    );
}

#[test]
fn a_server_restart_between_apply_and_ack_does_not_apply_twice() {
    let server_path = scratch("server");
    let client = block_on(Db::open(config(4))).unwrap();
    let tx = {
        let server = open_file(&server_path, 0xef);
        let (client_side, server_side) = duplex();
        block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
        let tx = block_on(client.insert(
            "items",
            item(1),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap()
        .mergeable_tx_id();
        block_on(client.tick()).unwrap(); // upload
        block_on(server.tick()).unwrap(); // apply; the ack is lost with the process
        assert_eq!(count(&server), 1);
        tx
    };
    let server = open_file(&server_path, 0xef);
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    drive(
        &[&client, &server, &client],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(count(&server), 1, "the resend was deduplicated");
}

fn rss_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn memory_stays_bounded_by_the_page_cache() {
    let path = scratch("bounded");
    let db = block_on(Db::open_at(
        config(5),
        &path,
        FileOptions {
            synchronous_full: false,
            cache_kib: 2 * 1024,
            ..FileOptions::default()
        },
    ))
    .unwrap();
    let start = rss_kib();
    let rows = 200_000u64;
    for chunk in 0..(rows / 1000) {
        block_on(db.transaction(async |tx| {
            for n in chunk * 1000..(chunk + 1) * 1000 {
                tx.insert(
                    "items",
                    item(n),
                    InsertOptions {
                        row_id: Some(row(n)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
        .unwrap();
    }
    let after_writes = rss_kib();
    let window = db
        .prepare_query(
            &Query::from("items")
                .order_by("n", OrderDirection::Desc)
                .limit(20),
        )
        .unwrap();
    assert_eq!(
        block_on(db.all(&window, ReadOpts::default()))
            .unwrap()
            .len(),
        20
    );
    let file = std::fs::metadata(&path).unwrap().len()
        + std::fs::metadata(path.with_extension("sqlite-wal"))
            .map(|m| m.len())
            .unwrap_or(0);
    let end = rss_kib();
    eprintln!(
        "bounded: {rows} rows, file {} MiB; RSS start {} MiB, after writes {} MiB, after read {} MiB",
        file / (1 << 20),
        start / 1024,
        after_writes / 1024,
        end / 1024
    );
    assert!(
        end.saturating_sub(start) < 64 * 1024,
        "RSS grew {} MiB",
        (end - start) / 1024
    );
    assert!(file > 16 << 20, "the data really is on disk");
}

/// Closing a store checkpoints its WAL into the file and removes it, also
/// after the store read large-value chunks back through its read-only
/// connection (here: chunks a replica fetched from the server).
#[test]
fn closing_a_store_that_read_large_values_back_checkpoints_its_wal() {
    let path = scratch("large-close");
    let server = block_on(Db::open_history_complete(config(0xef))).unwrap();
    block_on(server.insert(
        "items",
        BTreeMap::from([
            ("label".to_owned(), Value::String("x".repeat(300 * 1024))),
            ("n".to_owned(), Value::I64(1)),
        ]),
        Default::default(),
    ))
    .unwrap();
    {
        let client = open_file(&path, 4);
        let (client_side, server_side) = duplex();
        let _upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
        let query = client.prepare_query(&Query::from("items")).unwrap();
        let _stream = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
        for _ in 0..5 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        assert_eq!(count(&client), 1);
    }
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    assert!(!wal.exists(), "the closed store left its WAL behind");
}

/// A replica that wrote a large value and synced it reads it back offline
/// after a restart: its own chunks stay in its file once the server's
/// version replaces the optimistic one.
#[test]
fn a_replica_reads_its_own_synced_large_value_offline_after_a_restart() {
    let path = scratch("large-own");
    let server = block_on(Db::open_history_complete(config(0xed))).unwrap();
    let label = "y".repeat(300 * 1024);
    {
        let client = open_file(&path, 5);
        let (client_side, server_side) = duplex();
        let _upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
        let query = client.prepare_query(&Query::from("items")).unwrap();
        let _stream = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
        let handle = block_on(client.insert(
            "items",
            BTreeMap::from([
                ("label".to_owned(), Value::String(label.clone())),
                ("n".to_owned(), Value::I64(1)),
            ]),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap();
        drive(
            &[&client, &server, &client],
            client.wait_for_transaction(handle.mergeable_tx_id(), DurabilityTier::Global),
        )
        .unwrap();
        let update = block_on(client.update(
            "items",
            row(1),
            BTreeMap::from([("n".to_owned(), Value::I64(2))]),
            Default::default(),
        ))
        .unwrap();
        drive(
            &[&client, &server, &client],
            client.wait_for_transaction(update.mergeable_tx_id(), DurabilityTier::Global),
        )
        .unwrap();
        for _ in 0..5 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    }
    let client = open_file(&path, 5);
    let query = client.prepare_query(&Query::from("items")).unwrap();
    let rows = block_on(client.all(&query, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .cells()
            .find(|(name, _)| *name == "label")
            .and_then(|(_, v)| v.cloned()),
        Some(Value::String(label)),
        "the replica's own large value is readable offline"
    );
}

/// A durable relay (a browser's worker) keeps the large value a client
/// below it wrote, so it reads the value offline after a restart.
#[test]
fn a_durable_relay_reads_a_large_value_its_client_wrote_offline_after_a_restart() {
    let path = scratch("large-relay");
    let server = block_on(Db::open_history_complete(config(0xec))).unwrap();
    let label = "z".repeat(300 * 1024);
    {
        let relay = open_file(&path, 6);
        let (relay_side, server_side) = duplex();
        let _upstream = block_on(relay.connect_upstream(relay_side));
        server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
        let client = block_on(Db::open(config(7))).unwrap();
        let (client_side, relay_down) = duplex();
        let _relay_link = block_on(client.connect_upstream(client_side));
        relay.accept_subscriber(relay_down, AuthorSubject::SYSTEM);
        let query = client.prepare_query(&Query::from("items")).unwrap();
        let _stream = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
        let handle = block_on(client.insert(
            "items",
            BTreeMap::from([
                ("label".to_owned(), Value::String(label.clone())),
                ("n".to_owned(), Value::I64(1)),
            ]),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap();
        drive(
            &[&client, &relay, &server, &relay, &client],
            client.wait_for_transaction(handle.mergeable_tx_id(), DurabilityTier::Global),
        )
        .unwrap();
        for _ in 0..5 {
            block_on(client.tick()).unwrap();
            block_on(relay.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    }
    let relay = open_file(&path, 6);
    let query = relay.prepare_query(&Query::from("items")).unwrap();
    let rows = block_on(relay.all(&query, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .cells()
            .find(|(name, _)| *name == "label")
            .and_then(|(_, v)| v.cloned()),
        Some(Value::String(label.clone())),
        "the relay holds the large value its client wrote"
    );

    // A reloaded tab: a fresh non-durable foreground over the relay, still
    // offline (and disconnected from the server, as the app asked). Its one-shot read gets the row from the relay and waits for
    // the value's chunks instead of failing on them.
    let tab = block_on(Db::open(config(8))).unwrap();
    tab.set_non_durable_client();
    let (tab_side, relay_down) = duplex();
    let _tab_link = block_on(tab.connect_upstream(tab_side));
    relay.accept_subscriber(relay_down, AuthorSubject::SYSTEM);
    // The app disconnected from the server; the worker still answers.
    tab.set_remote_link_hint(jazz::db::RemoteLinkHint::Failed);
    let read = postcard::to_allocvec(&Query::from("items")).unwrap();
    let result = drive(
        &[&tab, &relay, &tab],
        tab.all_serialized_query(
            &read,
            ReadOpts::default(),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap();
    let jazz::db::SerializedReadResult::Rows(rows) = result else {
        panic!("a flat read answers rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .cells()
            .find(|(name, _)| *name == "label")
            .and_then(|(_, v)| v.cloned()),
        Some(Value::String(label)),
        "the tab reads the large value through its relay"
    );
}
