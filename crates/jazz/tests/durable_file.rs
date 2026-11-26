//! Durable file-backed stores (SQLite WAL): reopening restores rows,
//! history and the commit seq; a process killed mid-write keeps exactly the
//! committed prefix (a Jazz transaction is all or nothing); a replica's
//! pending uploads survive a restart and are resent; a server restart
//! between applying a commit and acknowledging it does not apply it twice;
//! memory stays bounded by the page cache.

use jazz_types::legacy_test_future::ResultFutureExt;
mod common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

/// One step of the nightly older-core round trip (jazz#6367); a no-op
/// unless `dev/gates/commit-seq-older-core.mjs` runs it.
#[path = "compat/older_core_step.rs"]
mod older_core_step;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, block_on};
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
    let dir = std::env::temp_dir().join(format!("jazz-durable-file-{}-{name}", std::process::id()));
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
        db.abandon_transaction_handle(open).unwrap();
        block_on(db.insert("items", item(99), Default::default()))
            .unwrap()
            .mergeable_tx_id()
    };
    let db = open_file(&path, 1);
    assert_eq!(count(&db), 11);
    let next = block_on(db.insert("items", item(100), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    assert!(
        next > before,
        "the commit seq continues above what it numbered"
    );
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
    let rows = block_on(db.tx_ref(open).all_prepared(&first)).unwrap();
    assert_eq!(
        rows[0].cells().find(|(n, _)| *n == "label").unwrap().1,
        Some(&Value::String("changed".to_owned()))
    );
    db.abandon_transaction_handle(open).unwrap();
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
    let _ = block_on(async {
        let tx = db.mergeable_tx().await?;
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
        tx.commit().await
    });
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
        jazz::db::block_on(client.set_remote_link_hint(jazz::db::RemoteLinkHint::Failed));
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
    jazz::db::block_on(client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live));
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
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
        assert!(tx > 0);
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

/// A replica whose uploads all settled holds only server seqs; reopened, it
/// must still continue after the local seqs it handed out, or its next write
/// reuses a transaction id the authority already applied and is dropped as a
/// resend.
#[test]
fn a_reopened_replica_whose_uploads_settled_continues_after_its_own_seqs() {
    let path = scratch("settled-replica");
    let server = block_on(Db::open_history_complete(config(0xec))).unwrap();
    let connect = |client: &Db| {
        let (client_side, server_side) = duplex();
        block_on(client.connect_upstream(client_side));
        jazz::db::block_on(client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live));
        server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM)
            .unwrap();
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
    let last = {
        let client = open_file(&path, 5);
        connect(&client);
        insert(&client, 0);
        insert(&client, 1)
    };
    let client = open_file(&path, 5);
    connect(&client);
    assert!(insert(&client, 2) > last, "the commit seq continues");
    assert_eq!(
        count(&server),
        3,
        "the new write was applied, not deduplicated"
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
        server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM)
            .unwrap();
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
        server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM)
            .unwrap();
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
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
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
        block_on(async {
            let tx = db.mergeable_tx().await?;
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
            tx.commit().await
        })
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
        server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM)
            .unwrap();
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
        server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM)
            .unwrap();
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
        server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM)
            .unwrap();
        let client = block_on(Db::open(config(7))).unwrap();
        let (client_side, relay_down) = duplex();
        let _relay_link = block_on(client.connect_upstream(client_side));
        relay
            .accept_subscriber(relay_down, AuthorSubject::SYSTEM)
            .unwrap();
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
    jazz::db::block_on(tab.set_non_durable_client());
    let (tab_side, relay_down) = duplex();
    let _tab_link = block_on(tab.connect_upstream(tab_side));
    relay
        .accept_subscriber(relay_down, AuthorSubject::SYSTEM)
        .unwrap();
    // The app disconnected from the server; the worker still answers.
    jazz::db::block_on(tab.set_remote_link_hint(jazz::db::RemoteLinkHint::Failed));
    let read = postcard::to_allocvec(&Query::from("items")).unwrap();
    let result = drive(
        &[&tab, &relay, &tab],
        tab.all_serialized_query(&read, ReadOpts::default(), None, None, None),
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

/// #4189: a replica's games list subscribes in batches of up to 256 ids,
/// `id IN batch ORDER BY n DESC LIMIT window`, growing the window 20 rows
/// a step (closing and reopening each batch's subscription) to the end of
/// 600 rows, then shrinking back to 41. After a shrink the subscriptions on
/// both sides are back to the cold-launch count, and crawling again does
/// not grow the process: what a deep crawl holds is released or reused.
#[test]
fn a_deep_crawl_then_shrink_releases_subscription_state() {
    let client_path = scratch("crawl");
    let server = block_on(Db::open_history_complete(config(0xc1))).unwrap();
    let total = 600u64;
    for n in 0..total {
        let mut cells = item(n);
        cells.insert("label".to_owned(), Value::String(format!("{n:>1024}")));
        block_on(server.insert(
            "items",
            cells,
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let client = open_file(&client_path, 6);
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    let pump = || {
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
    };
    let ids: Vec<u64> = (0..total).collect();
    let open_window = |limit: usize| {
        ids.chunks(256)
            .map(|batch| {
                let query = Query::from("items")
                    .filter(jazz::query::in_list(
                        jazz::query::col("id"),
                        batch
                            .iter()
                            .map(|n| jazz::query::lit(Value::Uuid(row(*n).0))),
                    ))
                    .order_by("n", OrderDirection::Desc)
                    .limit(limit);
                let prepared = client.prepare_query(&query).unwrap();
                block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap()
            })
            .collect::<Vec<_>>()
    };
    let mut window = open_window(40);
    pump();
    let cold = (
        client.runtime_stats_for_test().active_subscriptions,
        server.runtime_stats_for_test().active_subscriptions,
    );
    let crawl = |window: &mut Vec<_>| {
        let mut limit = 40;
        while limit < total as usize {
            limit += 20;
            window.clear();
            pump();
            *window = open_window(limit);
            pump();
        }
        window.clear();
        pump();
        *window = open_window(41);
        pump();
    };
    crawl(&mut window);
    let after_first = rss_kib();
    let shrunk = (
        client.runtime_stats_for_test().active_subscriptions,
        server.runtime_stats_for_test().active_subscriptions,
    );
    assert_eq!(
        shrunk, cold,
        "(client, server) subscriptions after the shrink"
    );
    for _ in 0..3 {
        crawl(&mut window);
    }
    let after_more = rss_kib();
    eprintln!(
        "crawl: RSS after one crawl {} MiB, after four {} MiB",
        after_first / 1024,
        after_more / 1024
    );
    assert!(
        after_more.saturating_sub(after_first) < 32 * 1024,
        "repeated crawls grew the process by {} MiB",
        (after_more - after_first) / 1024
    );
    assert_eq!(
        (
            client.runtime_stats_for_test().active_subscriptions,
            server.runtime_stats_for_test().active_subscriptions,
        ),
        cold
    );
}

// ---- the commit seq row (jazz#6367) ----------------------------------------
//
// A store keeps the newest commit seq it numbered in one `_jazz_meta` row,
// `commit_seq.v1`, raised by triggers on every table, history table and
// `_jazz_transactions`. An open reads the row and scans only tables the row
// does not cover yet. The raw SQLite reads below are white-box on purpose:
// the row and its triggers are a durable encoding, and what an open reads is
// not visible through the public API.

const SEQ_KEY: &str = "commit_seq.v1";

fn raw(path: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(path).unwrap()
}

/// The stored tables that carry a commit seq in `_clock`: each table and
/// its history table.
fn clocked_tables(conn: &rusqlite::Connection) -> Vec<String> {
    let mut statement = conn
        .prepare(
            "SELECT m.name FROM sqlite_master AS m WHERE m.type = 'table' \
             AND m.name NOT GLOB '_jazz_*' \
             AND EXISTS (SELECT 1 FROM pragma_table_info(m.name) WHERE name = '_clock') \
             ORDER BY m.name",
        )
        .unwrap();
    statement
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Every table the row covers, sorted.
fn seq_targets(path: &Path) -> Vec<String> {
    let mut targets = clocked_tables(&raw(path));
    targets.push("_jazz_transactions".to_owned());
    targets.sort();
    targets
}

fn sorted(mut tables: Vec<String>) -> Vec<String> {
    tables.sort();
    tables
}

/// The row's raw value and SQLite type, if present.
fn seq_row(path: &Path) -> Option<(String, String)> {
    use rusqlite::OptionalExtension;
    raw(path)
        .query_row(
            "SELECT quote(v), typeof(v) FROM _jazz_meta WHERE k = ?1",
            [SEQ_KEY],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .unwrap()
}

fn seq_row_value(path: &Path) -> Option<u64> {
    seq_row(path).map(|(value, kind)| {
        assert_eq!(kind, "integer", "commit_seq.v1 is an SQLite INTEGER");
        value.parse().unwrap()
    })
}

/// The commit seq as opens recovered it before the row: the newest
/// accepted `_clock` of every table and history table, and the newest `t`
/// of `_jazz_transactions`, below the pending band.
fn scanned_seq(path: &Path) -> u64 {
    let conn = raw(path);
    let mut seq = 0i64;
    for table in clocked_tables(&conn) {
        let max: Option<i64> = conn
            .query_row(
                &format!(
                    "SELECT MAX(_clock) FROM \"{table}\" WHERE _clock >= 0 AND _clock < {PENDING_BAND}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        seq = seq.max(max.unwrap_or(0));
    }
    let settled: Option<i64> = conn
        .query_row(
            &format!("SELECT MAX(t) FROM _jazz_transactions WHERE t < {PENDING_BAND}"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    seq.max(settled.unwrap_or(0)) as u64
}

/// Where the pending band of `_clock` starts (`band_start(PENDING)`, 1 <<
/// 52); the accepted seqs are below it. Pinned in the triggers' text below.
const PENDING_BAND: i64 = 4_503_599_627_370_496;

fn insert_items(db: &Db, ns: std::ops::Range<u64>) {
    for n in ns {
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
}

/// What an older core's commit writes, as SQL that knows nothing of the
/// row: a version of an existing row of `table` into its history table, at
/// accepted seq `seq`; with `dedupe`, also an accepted upload's dedupe
/// entry at `seq`.
fn older_core_commit(conn: &rusqlite::Connection, table: &str, seq: i64, dedupe: bool) {
    let history = format!("{table}__h");
    let mut columns: Vec<String> = conn
        .prepare(&format!("SELECT name FROM pragma_table_info('{history}')"))
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    columns.sort();
    let values: Vec<String> = columns
        .iter()
        .map(|c| match c.as_str() {
            "_clock" => seq.to_string(),
            other => format!("\"{other}\""),
        })
        .collect();
    let columns: Vec<String> = columns.iter().map(|c| format!("\"{c}\"")).collect();
    conn.execute_batch(&format!(
        "INSERT INTO \"{history}\" ({}) SELECT {} FROM \"{table}\" LIMIT 1;",
        columns.join(", "),
        values.join(", "),
    ))
    .unwrap();
    if dedupe {
        conn.execute(
            "INSERT INTO _jazz_transactions (n, tx, t, result) VALUES (987654, ?1, ?1, NULL)",
            [seq],
        )
        .unwrap();
    }
}

/// The stored name of `items` (its rows live there; history in `__h`).
fn items_table(path: &Path) -> String {
    clocked_tables(&raw(path))
        .into_iter()
        .find(|t| !t.ends_with("__h"))
        .unwrap()
}

/// A store opened once records its commit seq; every later open resumes
/// from the row and scans no table, and its next commit numbers above.
///
/// ```text
/// open ─► scan (all tables, empty) ─► row = 0
///   commits ─► triggers raise the row
/// reopen ─► row only ─► resumes after the newest seq
/// ```
#[test]
fn a_reopened_store_resumes_from_the_commit_seq_row_without_scanning() {
    let path = scratch("seq-row");
    let before = {
        let db = open_file(&path, 1);
        assert_eq!(
            sorted(db.seq_scans_for_test()),
            seq_targets(&path),
            "a new store covers every table at its first open"
        );
        insert_items(&db, 0..20);
        db.seq_high_water()
    };
    let recorded = seq_row_value(&path).expect("the row is written");
    assert_eq!(recorded, scanned_seq(&path), "the row is what a scan finds");
    assert!(recorded > 0 && recorded <= before);
    for _ in 0..2 {
        let db = open_file(&path, 1);
        assert_eq!(db.seq_scans_for_test(), Vec::<String>::new(), "no scan");
        assert!(db.seq_high_water() >= recorded);
        let next = block_on(db.insert("items", item(500), Default::default()))
            .unwrap()
            .mergeable_tx_id();
        assert!(
            next > before,
            "the commit seq continues above what it numbered"
        );
    }
    assert_eq!(seq_row_value(&path), Some(scanned_seq(&path)));
}

/// Mixed versions on one store: after this build wrote the row, an older
/// core that knows nothing of it commits; this build reopens and resumes
/// above the older core's commit, still without a scan, because the
/// triggers in the file raised the row inside the older core's commit.
///
/// ```text
/// new ──open, commit──► row = s
/// old ──commit at s + 1000 (no row write)──► trigger: row = s + 1000
/// new ──reopen──► resumes after s + 1000, no scan
/// ```
#[test]
fn an_older_cores_commit_raises_the_row_through_its_triggers() {
    let path = scratch("seq-older-core");
    {
        let db = open_file(&path, 1);
        insert_items(&db, 0..5);
    }
    let recorded = seq_row_value(&path).unwrap();
    let older = recorded as i64 + 1000;
    older_core_commit(&raw(&path), &items_table(&path), older, true);
    assert_eq!(scanned_seq(&path), older as u64);
    assert_eq!(seq_row_value(&path), Some(older as u64));
    let db = open_file(&path, 1);
    assert_eq!(db.seq_scans_for_test(), Vec::<String>::new());
    assert!(db.seq_high_water() >= older as u64);
    let next = block_on(db.insert("items", item(99), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    let _ = next;
    assert!(
        seq_row_value(&path).unwrap() > older as u64,
        "numbered above it"
    );
}

/// A table without its triggers (one an older core created, or whose
/// triggers are gone) is not covered by the row: the next open scans that
/// table alone; the first write gives it its triggers and raises the row
/// past what it holds, so the open after scans nothing.
#[test]
fn a_table_without_triggers_is_scanned_once_and_covered() {
    let path = scratch("seq-uncovered");
    {
        let db = open_file(&path, 1);
        insert_items(&db, 0..5);
    }
    let items = items_table(&path);
    let history = format!("{items}__h");
    let recorded = seq_row_value(&path).unwrap();
    let older = recorded as i64 + 5000;
    {
        let conn = raw(&path);
        conn.execute_batch(&format!(
            "DROP TRIGGER \"{history}__seq_v1_ins\"; DROP TRIGGER \"{history}__seq_v1_upd\";"
        ))
        .unwrap();
        older_core_commit(&conn, &items, older, false);
    }
    assert_eq!(
        seq_row_value(&path),
        Some(recorded),
        "nothing raised the row"
    );
    assert_eq!(
        scanned_seq(&path),
        older as u64,
        "only the history holds it"
    );
    {
        let db = open_file(&path, 1);
        assert_eq!(db.seq_scans_for_test(), vec![history.clone()]);
        assert!(db.seq_high_water() >= older as u64);
        assert_eq!(
            seq_row_value(&path),
            Some(recorded),
            "an open writes nothing"
        );
        // The first write covers it: scanned again, in its transaction.
        insert_items(&db, 50..51);
        assert_eq!(
            db.seq_scans_for_test(),
            vec![history.clone(), history.clone()]
        );
    }
    assert!(seq_row_value(&path).unwrap() > older as u64);
    let db = open_file(&path, 1);
    assert_eq!(db.seq_scans_for_test(), Vec::<String>::new());
}

/// A table a new schema version adds joins the row when it is created:
/// the open that adds it scans only the new (empty) tables, and the next
/// open scans nothing.
#[test]
fn a_table_a_new_schema_adds_is_covered_from_its_creation() {
    let path = scratch("seq-new-table");
    {
        let db = open_file(&path, 1);
        insert_items(&db, 0..3);
    }
    let with_notes = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("label", ColumnType::Text)
                    .column("n", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = || {
        block_on(Db::open_at(
            DbConfig::new(
                with_notes.clone(),
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ),
            &path,
            FileOptions::default(),
        ))
        .unwrap()
    };
    let before: Vec<String> = clocked_tables(&raw(&path));
    {
        let db = open();
        let added: Vec<String> = clocked_tables(&raw(&path))
            .into_iter()
            .filter(|t| !before.contains(t))
            .collect();
        assert_eq!(added.len(), 2, "notes and its history");
        assert_eq!(sorted(db.seq_scans_for_test()), added);
        block_on(db.insert(
            "notes",
            BTreeMap::from([("body".to_owned(), Value::String("hi".to_owned()))]),
            Default::default(),
        ))
        .unwrap();
    }
    assert_eq!(seq_row_value(&path), Some(scanned_seq(&path)));
    let db = open();
    assert_eq!(db.seq_scans_for_test(), Vec::<String>::new());
}

const SEQ_CRASH_CHILD: &str = "JAZZ_SEQ_CRASH_CHILD";

/// Child process body: inside one SQLite transaction, write an accepted
/// seq the way a commit does (a history version and a dedupe entry), check
/// the row already moved with it, then either commit or abort the process
/// before the transaction commits.
fn seq_crash_child(path: &Path, seq: i64, commit: bool) -> ! {
    let conn = raw(path);
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    older_core_commit(&conn, &items_table(path), seq, false);
    let inside: i64 = conn
        .query_row("SELECT v FROM _jazz_meta WHERE k = ?1", [SEQ_KEY], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(inside, seq, "the row moves in the commit's own transaction");
    if commit {
        conn.execute_batch("COMMIT").unwrap();
        std::process::exit(0);
    }
    std::process::abort();
}

/// The row and the data it covers commit together: a process killed after
/// a commit wrote its seq but before its transaction committed leaves
/// neither, and the store resumes from the last committed seq; one that
/// commits leaves both. There is no point between a commit's data write
/// and its row write to crash at: the trigger runs inside the write.
///
/// ```text
/// child: BEGIN ─► write seq s+77 (row = s+77 inside) ─► abort
/// parent: reopen ─► row = s, data has no s+77, no scan
/// child: BEGIN ─► write seq s+77 ─► COMMIT
/// parent: reopen ─► row = s+77
/// ```
#[test]
fn a_crash_between_a_commits_write_and_its_commit_keeps_row_and_data_together() {
    if let Ok(spec) = std::env::var(SEQ_CRASH_CHILD) {
        let mut parts = spec.split('|');
        let path = PathBuf::from(parts.next().unwrap());
        let seq = parts.next().unwrap().parse().unwrap();
        let commit = parts.next().unwrap() == "commit";
        seq_crash_child(&path, seq, commit);
    }
    let path = scratch("seq-crash");
    {
        let db = open_file(&path, 1);
        insert_items(&db, 0..5);
    }
    let recorded = seq_row_value(&path).unwrap();
    let crashed = recorded as i64 + 77;
    let run = |mode: &str| {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "a_crash_between_a_commits_write_and_its_commit_keeps_row_and_data_together",
                "--nocapture",
            ])
            .env(
                SEQ_CRASH_CHILD,
                format!("{}|{crashed}|{mode}", path.display()),
            )
            .status()
            .unwrap()
    };
    assert_aborted(run("abort"));
    {
        let db = open_file(&path, 1);
        assert_eq!(db.seq_scans_for_test(), Vec::<String>::new());
        assert_eq!(scanned_seq(&path), recorded, "the data has no trace of it");
        assert_eq!(seq_row_value(&path), Some(recorded), "nor has the row");
    }
    assert!(run("commit").success());
    let db = open_file(&path, 1);
    assert_eq!(db.seq_scans_for_test(), Vec::<String>::new());
    assert_eq!(seq_row_value(&path), Some(crashed as u64));
    assert!(db.seq_high_water() >= crashed as u64);
}

/// The encoding `commit_seq.v1`, pinned byte for byte: the row is
/// `('commit_seq.v1', <INTEGER>)` and the triggers' SQL as stored in the
/// file. A store whose row holds anything else is refused with a storage
/// error rather than resumed from.
#[test]
fn the_commit_seq_row_and_its_triggers_are_pinned() {
    let path = scratch("seq-fixture");
    {
        let db = open_file(&path, 1);
        insert_items(&db, 0..1);
    }
    let conn = raw(&path);
    conn.execute_batch(&format!(
        "UPDATE _jazz_meta SET v = 1234567890123 WHERE k = '{SEQ_KEY}';"
    ))
    .unwrap();
    assert_eq!(
        seq_row(&path),
        Some(("1234567890123".to_owned(), "integer".to_owned()))
    );
    let trigger = |name: &str| -> String {
        conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
            [name],
            |r| r.get(0),
        )
        .unwrap()
    };
    let items = items_table(&path);
    assert_eq!(
        trigger(&format!("{items}__h__seq_v1_ins")),
        format!(
            "CREATE TRIGGER \"{items}__h__seq_v1_ins\" AFTER INSERT ON \"{items}__h\" \
             WHEN NEW._clock >= 0 AND NEW._clock < {PENDING_BAND} BEGIN UPDATE _jazz_meta \
             SET v = NEW._clock WHERE k = 'commit_seq.v1' AND v < NEW._clock; END"
        )
    );
    assert_eq!(
        trigger("_jazz_transactions__seq_v1_upd"),
        format!(
            "CREATE TRIGGER \"_jazz_transactions__seq_v1_upd\" AFTER UPDATE OF t ON \
             \"_jazz_transactions\" WHEN NEW.t >= 0 AND NEW.t < {PENDING_BAND} BEGIN UPDATE \
             _jazz_meta SET v = NEW.t WHERE k = 'commit_seq.v1' AND v < NEW.t; END"
        )
    );
    drop(conn);
    // A hand-written row in the pinned encoding is resumed from as is.
    {
        let db = open_file(&path, 1);
        assert_eq!(db.seq_scans_for_test(), Vec::<String>::new());
        assert!(db.seq_high_water() >= 1_234_567_890_123);
    }
    for corrupt in ["x'01'", "'12'", "-1", "1.5", &PENDING_BAND.to_string()] {
        raw(&path)
            .execute_batch(&format!(
                "UPDATE _jazz_meta SET v = {corrupt} WHERE k = '{SEQ_KEY}';"
            ))
            .unwrap();
        let error = block_on(Db::open_at(config(1), &path, FileOptions::default()))
            .err()
            .unwrap_or_else(|| panic!("{corrupt} is refused"));
        assert_eq!(error.code, jazz::db::ErrorCode::Storage, "{corrupt}");
        assert!(
            error.message.contains("corrupt commit seq")
                && error.message.contains("Deleting that row is safe"),
            "{corrupt}: {}",
            error.message
        );
    }
    // The refusal's fix works: without the row, an open scans every table
    // and the next write records the row again.
    raw(&path)
        .execute_batch(&format!("DELETE FROM _jazz_meta WHERE k = '{SEQ_KEY}';"))
        .unwrap();
    {
        let db = open_file(&path, 1);
        assert_eq!(sorted(db.seq_scans_for_test()), seq_targets(&path));
        insert_items(&db, 10..11);
    }
    assert_eq!(seq_row_value(&path), Some(scanned_seq(&path)));
    assert_eq!(
        open_file(&path, 1).seq_scans_for_test(),
        Vec::<String>::new()
    );
}

/// A trigger under the encoding's name whose SQL is not the one this build
/// writes is not trusted to keep the row: the open fails with a storage
/// error naming it, rather than skip it as if it covered its table. Its
/// fix, dropping it, is safe: the next open scans that table and the next
/// write recreates the trigger.
#[test]
fn a_foreign_trigger_under_the_encodings_name_is_refused() {
    let path = scratch("seq-foreign-trigger");
    {
        let db = open_file(&path, 1);
        insert_items(&db, 0..3);
    }
    let history = format!("{}__h", items_table(&path));
    let name = format!("{history}__seq_v1_ins");
    raw(&path)
        .execute_batch(&format!(
            "DROP TRIGGER \"{name}\"; \
             CREATE TRIGGER \"{name}\" AFTER INSERT ON \"{history}\" BEGIN SELECT 1; END;"
        ))
        .unwrap();
    let error = block_on(Db::open_at(config(1), &path, FileOptions::default()))
        .err()
        .expect("a foreign trigger is refused");
    assert_eq!(error.code, jazz::db::ErrorCode::Storage);
    assert!(
        error.message.contains(&name)
            && error.message.contains("is not the one this build writes")
            && error.message.contains("Dropping it is safe"),
        "{}",
        error.message
    );
    raw(&path)
        .execute_batch(&format!("DROP TRIGGER \"{name}\";"))
        .unwrap();
    {
        let db = open_file(&path, 1);
        assert_eq!(db.seq_scans_for_test(), vec![history.clone()]);
        insert_items(&db, 3..4);
    }
    assert_eq!(
        open_file(&path, 1).seq_scans_for_test(),
        Vec::<String>::new()
    );
    let stored: String = raw(&path)
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
            [&name],
            |r| r.get(0),
        )
        .unwrap();
    assert!(stored.contains("UPDATE _jazz_meta"), "recreated: {stored}");
}

/// The child died at its injected `abort()`, not by a panic (which exits
/// with 101 and would pass any "not success" check).
fn assert_aborted(status: std::process::ExitStatus) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(6), "the child aborts: {status:?}");
    }
    #[cfg(not(unix))]
    assert!(
        !status.success() && status.code() != Some(101),
        "{status:?}"
    );
}

const COVER_CRASH_CHILD: &str = "JAZZ_SEQ_COVER_CRASH_CHILD";

/// Remove the encoding from a store: every `seq_v1` trigger and the row, as
/// a store from before it has neither.
fn strip_commit_seq(path: &Path) {
    let conn = raw(path);
    let names: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name GLOB '*__seq_v1_*'",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    for name in names {
        conn.execute_batch(&format!("DROP TRIGGER \"{name}\";"))
            .unwrap();
    }
    conn.execute_batch(&format!("DELETE FROM _jazz_meta WHERE k = '{SEQ_KEY}';"))
        .unwrap();
}

fn seq_trigger_count(path: &Path) -> i64 {
    raw(path)
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name GLOB '*__seq_v1_*'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// Jazz's own cover, killed: a process aborted inside the first write's
/// cover (after it created the triggers and wrote the row, before its
/// transaction committed) leaves neither, and no data of the write; the
/// store is as uncovered as before, its next open scans every table, and
/// its next write covers it.
///
/// ```text
/// child: open (uncovered) ─► insert ─► cover writes triggers + row ─► abort
/// parent: no triggers, no row, no new row; reopen scans all; write covers
/// ```
#[test]
fn a_process_killed_inside_the_cover_leaves_the_store_uncovered() {
    if let Ok(path) = std::env::var(COVER_CRASH_CHILD) {
        let db = open_file(Path::new(&path), 1);
        db.abort_in_seq_cover_for_test();
        let _ = block_on(db.insert("items", item(77), Default::default()));
        unreachable!("the cover aborts");
    }
    let path = scratch("seq-cover-crash");
    {
        let db = open_file(&path, 1);
        insert_items(&db, 0..5);
    }
    strip_commit_seq(&path);
    let before = scanned_seq(&path);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "a_process_killed_inside_the_cover_leaves_the_store_uncovered",
            "--nocapture",
        ])
        .env(COVER_CRASH_CHILD, path.display().to_string())
        .status()
        .unwrap();
    assert_aborted(status);
    assert_eq!(
        seq_trigger_count(&path),
        0,
        "the cover's triggers rolled back"
    );
    assert_eq!(seq_row(&path), None, "and its row");
    assert_eq!(scanned_seq(&path), before, "the write never landed");
    {
        let db = open_file(&path, 1);
        assert_eq!(sorted(db.seq_scans_for_test()), seq_targets(&path));
        assert!(db.seq_high_water() >= before);
        insert_items(&db, 5..6);
    }
    assert_eq!(
        seq_trigger_count(&path) as usize,
        2 * seq_targets(&path).len()
    );
    assert_eq!(seq_row_value(&path), Some(scanned_seq(&path)));
    assert_eq!(
        open_file(&path, 1).seq_scans_for_test(),
        Vec::<String>::new()
    );
}

const COMMIT_CRASH_CHILD: &str = "JAZZ_SEQ_COMMIT_CRASH_CHILD";

/// Jazz's own commit, killed: on a covered store, a process aborted in the
/// middle of a Jazz transaction (its writes staged, not committed) leaves
/// the row exactly at the newest seq the data holds, and the next open
/// reads it without a scan and numbers above everything committed.
#[test]
fn a_process_killed_mid_commit_leaves_the_row_matching_the_data() {
    if let Ok(path) = std::env::var(COMMIT_CRASH_CHILD) {
        let db = open_file(Path::new(&path), 1);
        let _ = block_on(async {
            let tx = db.mergeable_tx().await?;
            for n in 100..150 {
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
            tx.commit().await
        });
        unreachable!()
    }
    let path = scratch("seq-commit-crash");
    let committed = {
        let db = open_file(&path, 1);
        insert_items(&db, 0..20);
        db.seq_high_water()
    };
    let recorded = seq_row_value(&path).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "a_process_killed_mid_commit_leaves_the_row_matching_the_data",
            "--nocapture",
        ])
        .env(COMMIT_CRASH_CHILD, path.display().to_string())
        .status()
        .unwrap();
    assert_aborted(status);
    assert_eq!(seq_row_value(&path), Some(recorded), "the row is unchanged");
    assert_eq!(scanned_seq(&path), recorded, "and matches the data");
    let db = open_file(&path, 1);
    assert_eq!(db.seq_scans_for_test(), Vec::<String>::new());
    let next = block_on(db.insert("items", item(500), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    assert!(next > committed, "numbers above every committed seq");
}
