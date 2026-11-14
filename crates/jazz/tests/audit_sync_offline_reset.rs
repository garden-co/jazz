//! Audit (sync-offline): lifecycle transitions of a replica's link that the
//! happy path from a fresh start never reaches.
//!
//! A replica keeps per-read watermarks (`_jazz_watermarks`, SPEC 8 §8.11)
//! naming positions of the authority it last synced with. Nothing on the
//! wire identifies *which* authority history those positions belong to, so a
//! server whose store was reset (a dev server restarted in memory, a
//! deployment restored from a backup, a fresh app environment behind the same
//! URL) is resumed from as if its seqs continued the old history.

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::{BTreeMap, BTreeSet};

use crate::common;

use duplex_transport::duplex;
use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SubscriptionEvent, Upstream};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn row(generation: u8, index: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7000u64.to_be_bytes());
    bytes[8] = generation;
    bytes[9..].copy_from_slice(&(index as u64 + 1).to_be_bytes()[1..]);
    RowUuid::from_bytes(bytes)
}

fn server_with(generation: u8, count: usize) -> Db {
    let server = block_on(Db::open_history_complete(config(
        0x51,
        AuthorSubject::SYSTEM,
    )))
    .expect("open server");
    for index in 0..count {
        server
            .seed_settled_mergeable_for_bootstrap(
                "items",
                row(generation, index),
                AuthorSubject::SYSTEM,
                BTreeMap::from([(
                    "label".to_owned(),
                    Value::String(format!("g{generation}-{index}")),
                )]),
            )
            .expect("seed");
    }
    server
}

fn connect(client: &Db, server: &Db) -> Upstream {
    let (client_side, server_side) = duplex();
    let upstream = block_on(client.connect_upstream(client_side));
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    upstream
}

fn pump(client: &Db, server: &Db) {
    for _ in 0..40 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn local_ids(db: &Db) -> BTreeSet<RowUuid> {
    let prepared = db.prepare_query(&Query::from("items")).unwrap();
    block_on(db.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalFirst)))
        .unwrap()
        .into_iter()
        .map(|r| r.row_uuid())
        .collect()
}

fn server_ids(db: &Db) -> BTreeSet<RowUuid> {
    let prepared = db.prepare_query(&Query::from("items")).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|r| r.row_uuid())
        .collect()
}

/// A replica that synced `items` from one server history and later reaches a
/// server whose store was wiped (dev server `./data` deleted, a new app
/// environment behind the same URL) must end with the new server's rows, or
/// at least be told to reset its store. On main the wiped server minted a new
/// authority namespace, the replica refuses its catalogue ("another app's"),
/// the subscription is rejected with a Schema error and the replica never
/// syncs again (finding 1 of the sync-offline audit).
///
/// ```text
/// alice ── subscribe items ──► server₁ (rows g1-0..g1-9, seq ≈ 10)
///   │   settles, watermark W ≈ 10
///   ╳ link drops; server is reset/restored and gets 15 new rows (seq ≈ 15 > W)
/// alice ── Read{since: W} ──► server₂ ── "Since": ships only seq > W
/// expected: alice holds g2-0..g2-14
/// ```
#[test]
fn a_resumed_read_against_a_reset_server_converges_to_the_new_history() {
    let server1 = server_with(1, 10);
    let alice = block_on(Db::open(config(
        0x61,
        AuthorSubject::for_test_bytes([0x61; 16]),
    )))
    .expect("open alice");
    let upstream = connect(&alice, &server1);
    let prepared = alice.prepare_query(&Query::from("items")).unwrap();
    let mut stream =
        block_on(alice.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::LocalFirst)))
            .unwrap();
    pump(&alice, &server1);
    while let Some(event) = stream.try_next_event() {
        let _ = matches!(event, SubscriptionEvent::Delta { .. });
    }
    assert_eq!(
        local_ids(&alice),
        server_ids(&server1),
        "synced from server₁"
    );
    assert!(alice.detach_connection(&upstream));

    // The server comes back from a reset store, and grows past alice's
    // watermark before she reconnects.
    let server2 = server_with(2, 15);
    let _upstream = connect(&alice, &server2);
    pump(&alice, &server2);
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                settled,
                ..
            } => eprintln!(
                "after reconnect: delta reset={reset} added={} removed={} settled={settled}",
                added.len(),
                removed.len()
            ),
            other => eprintln!("after reconnect: {other:?}"),
        }
    }

    let have = local_ids(&alice);
    let want = server_ids(&server2);
    let missing: Vec<_> = want.difference(&have).collect();
    let ghosts: Vec<_> = have.difference(&want).collect();
    eprintln!("missing={} ghosts={}", missing.len(), ghosts.len());
    assert!(
        missing.is_empty(),
        "after reaching a reset server alice should hold its rows: \
         {} missing (seq <= old watermark), {} ghost rows from the old history",
        missing.len(),
        ghosts.len()
    );
}

fn file_server(path: &std::path::Path) -> Db {
    let db = block_on(Db::open_at(
        config(0x51, AuthorSubject::SYSTEM),
        path,
        jazz::db::FileOptions::default(),
    ))
    .expect("open file server");
    db.set_authority();
    db
}

fn seed_rows(server: &Db, generation: u8, range: std::ops::Range<usize>) {
    for index in range {
        server
            .seed_settled_mergeable_for_bootstrap(
                "items",
                row(generation, index),
                AuthorSubject::SYSTEM,
                BTreeMap::from([(
                    "label".to_owned(),
                    Value::String(format!("g{generation}-{index}")),
                )]),
            )
            .expect("seed");
    }
}

fn copy_store(from: &std::path::Path, to: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let src = std::path::PathBuf::from(format!("{}{suffix}", from.display()));
        let dst = std::path::PathBuf::from(format!("{}{suffix}", to.display()));
        let _ = std::fs::remove_file(&dst);
        if src.exists() {
            std::fs::copy(&src, &dst).unwrap();
        }
    }
}

/// A server restored from a backup keeps its namespace but its positions
/// go back. A replica that synced past the backup point and reconnects
/// after the restored server has grown past the replica's watermark must
/// end with the restored server's rows.
///
/// ```text
/// server: rows g1-0..4 ── backup ── rows g1-5..9 (seq ≈ 10)
/// alice ── subscribe, settles through W ≈ 10
///   ╳ link drops; server restored from backup (seq ≈ 5) and gets
///     rows g2-0..14 (seq ≈ 20 > W)
/// alice ── Read{since: W} ──► server ── "Since": ships only seq > W
/// expected: alice holds g1-0..4 and g2-0..14
/// ```
#[test]
fn a_resumed_read_against_a_server_restored_from_backup_converges() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("server.sqlite");
    let backup = dir.path().join("backup.sqlite");

    let server = file_server(&live);
    seed_rows(&server, 1, 0..5);
    // A first client connects before the backup, so the backup holds the
    // authority's namespace (minted when first served) as a live server's would.
    {
        let probe = block_on(Db::open(config(
            0x71,
            AuthorSubject::for_test_bytes([0x71; 16]),
        )))
        .expect("open probe");
        let _up = connect(&probe, &server);
        pump(&probe, &server);
    }
    block_on(server.close()).unwrap();
    drop(server);
    copy_store(&live, &backup);
    let server = file_server(&live);
    seed_rows(&server, 1, 5..10);

    let alice = block_on(Db::open(config(
        0x61,
        AuthorSubject::for_test_bytes([0x61; 16]),
    )))
    .expect("open alice");
    let upstream = connect(&alice, &server);
    let prepared = alice.prepare_query(&Query::from("items")).unwrap();
    let mut stream =
        block_on(alice.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::LocalFirst)))
            .unwrap();
    pump(&alice, &server);
    assert_eq!(local_ids(&alice).len(), 10, "synced from the server");
    assert!(alice.detach_connection(&upstream));
    while stream.try_next_event().is_some() {}

    // Restore from the backup; the restored server takes new writes.
    block_on(server.close()).unwrap();
    drop(server);
    copy_store(&backup, &live);
    let server = file_server(&live);
    seed_rows(&server, 2, 0..15);

    let _upstream = connect(&alice, &server);
    pump(&alice, &server);
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Rejected { reason, .. } = &event {
            eprintln!("after restore: rejected {reason}");
        }
    }
    let have = local_ids(&alice);
    let want = server_ids(&server);
    let missing: Vec<_> = want.difference(&have).collect();
    let ghosts: Vec<_> = have.difference(&want).collect();
    eprintln!(
        "restore: want={} missing={} ghosts={}",
        want.len(),
        missing.len(),
        ghosts.len()
    );
    assert!(
        missing.is_empty(),
        "alice lacks {} of the restored server's {} rows ({} ghost rows kept)",
        missing.len(),
        want.len(),
        ghosts.len()
    );
}
