//! A replica whose server's store was recreated, or restored from a backup.
//!
//! A replica keeps per-read watermarks (`_jazz_watermarks`, SPEC 8 §8.11)
//! and the authority namespace it mirrored (§8.10). An authority mints its
//! namespace once, when it is opened as the authority, and keeps it in its
//! file. A server whose store was recreated (its data directory deleted, an
//! in-memory server restarted, a new environment behind the same URL) has a
//! new namespace: the replica's rows, watermarks and unsynced writes belong
//! to the old history. The replica is told so with a typed, terminal
//! `AuthorityChanged` on that link (INV-SYNC-54), and syncs again once its
//! store is reset. A store restored from a backup keeps its namespace, so its
//! replicas resume against it.

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll};

use crate::common;

use duplex_transport::duplex;
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, InsertOptions, ReadOpts, ReadTier,
    SubscriptionEvent, SubscriptionStream, Upstream,
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

/// An in-memory server holding `count` rows of `generation`.
fn server_with(generation: u8, count: usize) -> Db {
    let server = block_on(Db::open_history_complete(config(
        0x51,
        AuthorSubject::SYSTEM,
    )))
    .expect("open server");
    seed_rows(&server, generation, 0..count);
    server
}

fn alice() -> Db {
    block_on(Db::open(config(
        0x61,
        AuthorSubject::for_test_bytes([0x61; 16]),
    )))
    .expect("open alice")
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

fn subscribe(db: &Db) -> SubscriptionStream {
    let prepared = db.prepare_query(&Query::from("items")).unwrap();
    block_on(db.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::LocalFirst))).unwrap()
}

/// The code of the last rejection `stream` delivered since the last call.
fn rejection(stream: &mut SubscriptionStream) -> Option<ErrorCode> {
    let mut code = None;
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Rejected { code: c, .. } = event {
            code = Some(c);
        }
    }
    code
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

fn insert(db: &Db, generation: u8, index: usize) -> u64 {
    block_on(db.insert(
        "items",
        BTreeMap::from([(
            "label".to_owned(),
            Value::String(format!("offline g{generation}-{index}")),
        )]),
        InsertOptions {
            row_id: Some(row(generation, index)),
            ..Default::default()
        },
    ))
    .unwrap()
    .mergeable_tx_id()
}

/// Poll `client`'s `Global` wait for `tx`, calling `pump` between polls, and
/// return its outcome, or `None` while it is still pending after `rounds`.
fn global_wait(
    client: &Db,
    tx: u64,
    rounds: usize,
    mut pump: impl FnMut(),
) -> Option<Result<u64, Error>> {
    let mut wait = pin!(client.wait_for_transaction(tx, DurabilityTier::Global));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..rounds {
        if let Poll::Ready(result) = wait.as_mut().poll(&mut cx) {
            return Some(result);
        }
        pump();
    }
    match wait.as_mut().poll(&mut cx) {
        Poll::Ready(result) => Some(result),
        Poll::Pending => None,
    }
}

/// A replica that synced from one server history and reaches a server whose
/// store was recreated is told so, once, on that link: its subscription is
/// rejected with `AuthorityChanged` (not a generic schema error), the link
/// reports the refusal for its host to stop it, and a `Global` wait on a
/// write it made offline fails at once instead of hanging. Its local-first
/// reads keep what it held, and the write is neither lost nor uploaded to
/// the new history: commits wait for the new link's `Welcome`. A store
/// reset (here: a fresh store, as `deleteClientStorage()` gives a browser)
/// converges to the new history.
///
/// ```text
/// alice ── subscribe items ──► server₁ (rows g1-0..g1-9)
///   │   settles; link drops; alice writes g9-0 offline
///   ╳ server₁'s store is recreated as server₂ (new namespace, rows g2-0..14)
/// alice ── Join ──► server₂ ── Welcome{namespace₂} ──► alice
///   alice: AuthorityChanged → link refused, subscription rejected,
///          Global wait on g9-0 fails; local reads still show g1-*
/// alice' (reset store) ── subscribe ──► server₂: holds g2-0..14
/// ```
#[test]
fn a_replica_reaching_a_recreated_server_store_is_told_its_authority_changed() {
    let server1 = server_with(1, 10);
    let alice = alice();
    let first = connect(&alice, &server1);
    let mut stream = subscribe(&alice);
    pump(&alice, &server1);
    assert_eq!(rejection(&mut stream), None);
    assert_eq!(local_ids(&alice), server_ids(&server1), "synced");
    assert!(alice.upstream_refusal(&first).is_none(), "a healthy link");
    assert!(alice.detach_connection(&first));
    let offline = insert(&alice, 9, 0);

    let server2 = server_with(2, 15);
    let second = connect(&alice, &server2);
    pump(&alice, &server2);

    assert_eq!(
        rejection(&mut stream),
        Some(ErrorCode::AuthorityChanged),
        "the subscription says why"
    );
    let refusal = alice
        .upstream_refusal(&second)
        .expect("the link reports its refusal");
    assert_eq!(refusal.code, ErrorCode::AuthorityChanged);
    assert!(
        refusal.message.contains("deleteClientStorage()"),
        "the message names the recovery: {}",
        refusal.message
    );
    assert!(
        alice.upstream_refusal(&first).is_none(),
        "an earlier link reports nothing"
    );
    let waited = global_wait(&alice, offline, 1, || pump(&alice, &server2))
        .expect("the Global wait ends at once");
    assert_eq!(
        waited.expect_err("nothing uploads").code,
        ErrorCode::AuthorityChanged
    );
    let mut held = server_ids(&server1);
    held.insert(row(9, 0));
    assert_eq!(
        local_ids(&alice),
        held,
        "local-first reads keep the old history and the unsynced write"
    );
    assert_eq!(
        alice.pending_upload_count(),
        1,
        "the write is kept, not lost"
    );
    assert!(
        !server_ids(&server2).contains(&row(9, 0)),
        "the old history's write never reaches the recreated server"
    );

    // A subscription opened after the refusal fails at once with it.
    let mut late = subscribe(&alice);
    block_on(alice.tick()).unwrap();
    assert_eq!(rejection(&mut late), Some(ErrorCode::AuthorityChanged));

    // The explicit recovery: reset the local store and reconnect.
    drop((stream, late));
    drop(alice);
    let reset = self::alice();
    let _upstream = connect(&reset, &server2);
    let mut stream = subscribe(&reset);
    pump(&reset, &server2);
    assert_eq!(rejection(&mut stream), None);
    assert_eq!(local_ids(&reset), server_ids(&server2), "converged");
}

/// `AuthorityChanged` is terminal for the link it came on, not for the
/// store: an explicit reconnect to the server whose history the store
/// follows (the old server back behind the URL) syncs again, and the write
/// made while refused uploads and settles.
///
/// ```text
/// alice ── server₁ ── settles ── ╳
/// alice ── server₂ (recreated) ── AuthorityChanged
/// alice writes g9-0 ── reconnects to server₁ ── g9-0 settles Global
/// ```
#[test]
fn the_old_server_back_behind_the_link_syncs_again() {
    let server1 = server_with(1, 3);
    let alice = alice();
    let first = connect(&alice, &server1);
    let mut stream = subscribe(&alice);
    pump(&alice, &server1);
    assert!(alice.detach_connection(&first));

    let server2 = server_with(2, 3);
    let second = connect(&alice, &server2);
    pump(&alice, &server2);
    assert_eq!(rejection(&mut stream), Some(ErrorCode::AuthorityChanged));
    let tx = insert(&alice, 9, 0);
    // The refused link is already detached.
    assert!(!alice.detach_connection(&second));

    let third = connect(&alice, &server1);
    assert!(alice.upstream_refusal(&third).is_none());
    let waited = global_wait(&alice, tx, 20, || pump(&alice, &server1))
        .expect("the write settles against its own authority");
    assert_eq!(waited.unwrap(), tx);
    assert_eq!(rejection(&mut stream), None);
    let mut want = server_ids(&server1);
    assert!(want.contains(&row(9, 0)), "uploaded");
    want.insert(row(9, 0));
    assert_eq!(local_ids(&alice), want);
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

/// Run the backup-and-restore scenario: `served_before_backup` says whether
/// a client connected before the backup was taken.
fn restored_from_backup_converges(served_before_backup: bool) {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("server.sqlite");
    let backup = dir.path().join("backup.sqlite");

    let server = file_server(&live);
    seed_rows(&server, 1, 0..5);
    if served_before_backup {
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

    let alice = alice();
    let upstream = connect(&alice, &server);
    let mut stream = subscribe(&alice);
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

    let restored = connect(&alice, &server);
    pump(&alice, &server);
    assert_eq!(
        rejection(&mut stream),
        None,
        "the namespace is the backup's"
    );
    assert!(alice.upstream_refusal(&restored).is_none());
    let have = local_ids(&alice);
    let want = server_ids(&server);
    let missing: Vec<_> = want.difference(&have).collect();
    let ghosts: Vec<_> = have.difference(&want).collect();
    assert!(
        missing.is_empty(),
        "alice lacks {} of the restored server's {} rows ({} ghost rows kept)",
        missing.len(),
        want.len(),
        ghosts.len()
    );
}

/// A server restored from a backup keeps its namespace but its positions
/// go back. A replica that synced past the backup point and reconnects
/// after the restored server has grown past the replica's watermark ends
/// with the restored server's rows.
///
/// ```text
/// server: rows g1-0..4 ── a client syncs ── backup ── rows g1-5..9
/// alice ── subscribe, settles through W ≈ 10
///   ╳ link drops; server restored from backup (seq ≈ 5) and gets
///     rows g2-0..14 (seq ≈ 20 > W)
/// alice ── Read{since: W} ──► server
/// expected: alice holds g1-0..4 and g2-0..14
/// ```
#[test]
fn a_resumed_read_against_a_server_restored_from_backup_converges() {
    restored_from_backup_converges(true);
}

/// The namespace is minted when the store is opened as the authority, not
/// when it first serves, so a backup taken before any client connected
/// carries it: the restored server is the same authority to alice.
///
/// ```text
/// server: rows g1-0..4 ── backup (never served) ── rows g1-5..9
/// alice ── syncs ── ╳ ── server restored from backup, rows g2-0..14
/// alice ── reconnects: same namespace, converges
/// ```
#[test]
fn a_backup_taken_before_any_client_connected_keeps_the_namespace() {
    restored_from_backup_converges(false);
}

/// An authority store written before the namespace was minted at open (no
/// namespace in its file, as a store that was never served) mints one when
/// it is next opened as the authority, keeps it across reopens, and serves
/// it to replicas.
///
/// ```text
/// store opened without the authority role (no namespace) ── closed
/// reopened as authority: mints N ── reopened: still N ── alice syncs
/// ```
#[test]
fn an_authority_store_without_a_namespace_mints_one_when_opened() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server.sqlite");
    {
        let plain = block_on(Db::open_at(
            config(0x51, AuthorSubject::SYSTEM),
            &path,
            jazz::db::FileOptions::default(),
        ))
        .unwrap();
        block_on(plain.close()).unwrap();
    }
    let server = file_server(&path);
    seed_rows(&server, 1, 0..3);
    let identity = server.table_identity("items").unwrap();
    block_on(server.close()).unwrap();
    drop(server);
    let server = file_server(&path);
    assert_eq!(server.table_identity("items").unwrap(), identity);

    let alice = alice();
    let upstream = connect(&alice, &server);
    let mut stream = subscribe(&alice);
    pump(&alice, &server);
    assert_eq!(rejection(&mut stream), None);
    assert!(alice.upstream_refusal(&upstream).is_none());
    assert_eq!(local_ids(&alice), server_ids(&server));
    assert_eq!(alice.table_identity("items").unwrap(), identity);
}

/// A wait that only needs this store (`Local`) is not failed by a refused
/// upstream: the write is stored locally whatever the server is.
#[test]
fn a_local_wait_is_unaffected_by_a_changed_authority() {
    let server1 = server_with(1, 1);
    let alice = alice();
    let first = connect(&alice, &server1);
    pump(&alice, &server1);
    assert!(alice.detach_connection(&first));
    let server2 = server_with(2, 1);
    let _second = connect(&alice, &server2);
    pump(&alice, &server2);
    let tx = insert(&alice, 9, 0);
    assert_eq!(
        block_on(alice.wait_for_transaction(tx, DurabilityTier::Local)).unwrap(),
        tx
    );
}

/// On a new link a store that follows an authority holds its offline
/// commits until the `Welcome` shows the same authority, and a read asked on
/// that link waits behind them: the server still answers it after the
/// commits it follows (INV-SYNC-49), so it sees the offline write.
///
/// ```text
/// alice ── server ── ╳ ── writes g9-0 offline
/// alice reconnects and asks a Remote read at once
/// alice ── Join ──► server ── Welcome (same namespace) ──► alice
/// alice ── Commit g9-0, then Read ──► server: the answer holds g9-0
/// ```
#[test]
fn a_read_asked_before_the_welcome_still_sees_the_writes_held_for_it() {
    let server = server_with(1, 2);
    let alice = alice();
    let first = connect(&alice, &server);
    pump(&alice, &server);
    assert!(alice.detach_connection(&first));
    insert(&alice, 9, 0);

    // The read is asked on the new link before anything was sent on it.
    let _second = connect(&alice, &server);
    let query = postcard::to_allocvec(&Query::from("items")).unwrap();
    let mut read = pin!(alice.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None,
    ));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(
        read.as_mut().poll(&mut cx).is_pending(),
        "waits for the server"
    );
    let mut answer = None;
    for _ in 0..40 {
        if let Poll::Ready(result) = read.as_mut().poll(&mut cx) {
            let jazz::db::SerializedReadResult::Rows(rows) = result.unwrap() else {
                panic!("a flat read");
            };
            answer = Some(rows);
            break;
        }
        block_on(alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    let ids: BTreeSet<RowUuid> = answer
        .expect("answered")
        .into_iter()
        .map(|r| r.row_uuid())
        .collect();
    assert!(
        ids.contains(&row(9, 0)),
        "read-your-writes across the reconnect"
    );
    assert_eq!(ids, server_ids(&server));
}
