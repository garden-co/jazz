//! Durable relay units retain the original writer and the authority's fate.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, RemoteLinkHint, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
use std::{
    collections::BTreeMap,
    future::Future,
    path::Path,
    pin::pin,
    task::{Context, Poll, Waker},
};
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn bob() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "bob").unwrap()
}
fn config(node: u8, author: AuthorSubject) -> DbConfig {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .column("allowed", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::eq_literal(
                                "allowed",
                                jazz::tools::Value::Boolean(true),
                            ))
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    );
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}
fn open(node: u8, author: AuthorSubject, path: Option<&Path>) -> Db {
    let config = config(node, author);
    match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap()
}
fn authority() -> Db {
    let db = open(90, AuthorSubject::SYSTEM, None);
    db.set_authority();
    db
}
fn connect(client: &Db, server: &Db, identity: AuthorSubject) {
    let (a, b) = duplex_transport::duplex();
    block_on(client.connect_upstream(a));
    server.accept_subscriber(b, identity);
}
fn turns(dbs: &[&Db]) {
    for _ in 0..12 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..150 {
        if let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return result;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("write did not settle")
}
fn insert(db: &Db, text: &str, allowed: bool) -> u64 {
    block_on(db.insert(
        "notes",
        BTreeMap::from([
            ("text".into(), Value::String(text.into())),
            ("allowed".into(), Value::Bool(allowed)),
        ]),
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id()
}
fn texts(db: &Db) -> Vec<String> {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let mut values = block_on(db.all(&query, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            match row
                .cells()
                .find(|(name, _)| *name == "text")
                .unwrap()
                .1
                .unwrap()
            {
                Value::String(text) => text.clone(),
                other => panic!("{other:?}"),
            }
        })
        .collect::<Vec<_>>();
    values.sort();
    values
}

/// Alice and Bob use the same transaction number at different nodes. Alice's
/// accepted write and Bob's rejected write must not consume each other's fate.
#[test]
fn equal_transaction_numbers_are_routed_by_original_node() {
    let server = authority();
    let relay = open(50, AuthorSubject::SYSTEM, None);
    let a = open(1, alice(), None);
    let b = open(2, bob(), None);
    connect(&relay, &server, AuthorSubject::SYSTEM);
    connect(&a, &relay, alice());
    connect(&b, &relay, bob());
    let x = insert(&a, "alice", true);
    let y = insert(&b, "bob", false);
    assert_eq!(x, y);
    let dbs = [&a, &b, &relay, &server, &relay, &a, &b];
    drive(&dbs, a.wait_for_transaction(x, DurabilityTier::Global)).unwrap();
    drive(&dbs, b.wait_for_transaction(y, DurabilityTier::Global)).unwrap_err();
    assert_eq!(a.write_state(x).unwrap().fate, Fate::Accepted);
    assert!(matches!(b.write_state(y).unwrap().fate, Fate::Rejected(_)));
    assert_eq!(texts(&server), ["alice"]);
    assert!(texts(&b).is_empty());
}

/// Alice's durable replica sends while Bob's relay is offline, then both
/// restart their connection. The journal resumes without a second server row.
#[test]
fn an_offline_relay_replays_after_restart_without_a_connected_sender() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relay.sqlite");
    let client = open(1, alice(), None);
    let server = authority();
    let tx = {
        let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
        relay.set_remote_link_hint(RemoteLinkHint::Failed);
        connect(&client, &relay, alice());
        let tx = insert(&client, "restart", true);
        turns(&[&client, &relay]);
        let mut wait = pin!(client.wait_for_transaction(tx, DurabilityTier::Global));
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        tx
    };
    {
        let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
        connect(&relay, &server, AuthorSubject::SYSTEM);
        turns(&[&relay, &server]);
        assert_eq!(texts(&server), ["restart"]);
    }
    let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
    connect(&relay, &server, AuthorSubject::SYSTEM);
    connect(&client, &relay, alice());
    drive(
        &[&client, &relay, &server, &relay, &client],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(texts(&server), ["restart"]);
    assert_eq!(texts(&client), ["restart"]);
    assert_eq!(server.seq_high_water(), 1);
}

/// Alice's upload crosses two trusted relays while Bob's relay also submits
/// an equal-numbered own write; both original origins settle independently.
#[test]
fn multi_hop_forwarding_keeps_local_and_relay_receipts_separate() {
    let server = authority();
    let near = open(50, AuthorSubject::SYSTEM, None);
    let far = open(51, AuthorSubject::SYSTEM, None);
    let client = open(1, alice(), None);
    connect(&far, &server, AuthorSubject::SYSTEM);
    connect(&near, &far, AuthorSubject::SYSTEM);
    connect(&client, &near, alice());
    let own = insert(&near, "relay-own", true);
    let tx = insert(&client, "alice", true);
    assert_eq!(own, tx);
    let dbs = [&client, &near, &far, &server, &far, &near, &client];
    drive(
        &dbs,
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    drive(&dbs, near.wait_for_transaction(own, DurabilityTier::Global)).unwrap();
    assert_eq!(texts(&server), ["alice", "relay-own"]);
}

/// Alice's large upload remains pinned in Bob's offline relay until forwarding;
/// reopening sends the real chunk tree and the authority validates its value.
#[test]
fn large_upload_survives_offline_relay_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relay.sqlite");
    let client = open(1, alice(), None);
    let server = authority();
    let text = "large-payload-".repeat(12000);
    let tx = {
        let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
        relay.set_remote_link_hint(RemoteLinkHint::Failed);
        connect(&client, &relay, alice());
        let tx = insert(&client, &text, true);
        turns(&[&client, &relay]);
        tx
    };
    let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
    connect(&relay, &server, AuthorSubject::SYSTEM);
    connect(&client, &relay, alice());
    drive(
        &[&client, &relay, &server, &relay, &client],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(texts(&server), [text]);
}

fn captured_upload(node: u8, text: &str, allowed: bool) -> Vec<jazz::db::SyncMessage> {
    let client = open(node, alice(), None);
    let (client_link, mut recorder) = duplex_transport::duplex();
    block_on(client.connect_upstream(client_link));
    insert(&client, text, allowed);
    block_on(client.tick()).unwrap();
    std::iter::from_fn(|| recorder.try_recv())
        .filter(|m| {
            matches!(
                m,
                jazz::db::SyncMessage::Commit { .. } | jazz::db::SyncMessage::PushChunks { .. }
            )
        })
        .collect()
}
fn send_upload(peer: &mut dyn jazz::db::Transport, messages: &[jazz::db::SyncMessage]) {
    for message in messages {
        peer.send(message.clone()).unwrap();
    }
}
fn receipts(peer: &mut dyn jazz::db::Transport) -> Vec<Result<(), jazz::db::WireError>> {
    std::iter::from_fn(|| peer.try_recv())
        .filter_map(|message| match message {
            jazz::db::SyncMessage::FateV1 { result, .. }
            | jazz::db::SyncMessage::OriginFateV2 {
                result,
                global: true,
                ..
            } => Some(result),
            jazz::db::SyncMessage::OriginFateV2 {
                result: Err(error), ..
            } => Some(Err(error)),
            _ => None,
        })
        .collect()
}

/// Bob names Alice on an ordinary session link. The relay normalizes the
/// upload to authenticated Bob, and the authority records Bob as its author.
#[test]
fn ordinary_connection_uses_its_authenticated_author() {
    let server = authority();
    let relay = open(50, AuthorSubject::SYSTEM, None);
    connect(&relay, &server, AuthorSubject::SYSTEM);
    let (mut peer, link) = duplex_transport::duplex();
    relay.accept_subscriber(link, bob());
    let upload = captured_upload(1, "normalized", true);
    send_upload(peer.as_mut(), &upload);
    turns(&[&relay, &server]);
    assert!(matches!(receipts(peer.as_mut()).as_slice(), [Ok(())]));
    assert_eq!(texts(&server), ["normalized"]);
    let rows = block_on(server.all(
        &server.prepare_query(&Query::from("notes")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    let provenance = server
        .row_provenance("notes", rows[0].row_uuid())
        .unwrap()
        .unwrap();
    assert_eq!(provenance.1, bob().canonical());
    assert_eq!(provenance.3, bob().canonical());
}

/// Alice retries the same pending identity; Mallory and a changed Alice unit
/// cannot overwrite it. After reconnect only Alice's original row is accepted.
#[test]
fn retries_cannot_replace_a_queued_sender_or_payload() {
    let server = authority();
    let relay = open(50, AuthorSubject::SYSTEM, None);
    relay.set_remote_link_hint(RemoteLinkHint::Failed);
    let original = captured_upload(1, "original", true);
    let changed = captured_upload(1, "replacement", true);
    let (mut a, link) = duplex_transport::duplex();
    relay.accept_subscriber(link, alice());
    send_upload(a.as_mut(), &original);
    turns(&[&relay]);
    send_upload(a.as_mut(), &changed);
    turns(&[&relay]);
    assert!(matches!(receipts(a.as_mut()).as_slice(), [Err(_)]));
    let (mut mallory, link) = duplex_transport::duplex();
    relay.accept_subscriber(link, bob());
    let mut stolen = original.clone();
    for message in &mut stolen {
        if let jazz::db::SyncMessage::Commit { author, .. } = message {
            *author = Some(bob().canonical().to_owned());
        }
    }
    send_upload(mallory.as_mut(), &stolen);
    turns(&[&relay]);
    assert!(matches!(receipts(mallory.as_mut()).as_slice(), [Err(_)]));
    send_upload(a.as_mut(), &original);
    connect(&relay, &server, AuthorSubject::SYSTEM);
    turns(&[&relay, &server]);
    assert!(matches!(receipts(a.as_mut()).as_slice(), [Ok(())]));
    assert_eq!(texts(&server), ["original"]);
    assert_eq!(server.seq_high_water(), 1);
}

/// Mallory cannot reuse Bob's relay node identity and settle its own pending
/// write; only Bob's actual owner transaction reaches the authority.
#[test]
fn downstream_cannot_alias_the_relay_owner_node() {
    let server = authority();
    let relay = open(50, AuthorSubject::SYSTEM, None);
    relay.set_remote_link_hint(RemoteLinkHint::Failed);
    let own = insert(&relay, "owner", true);
    let (mut peer, link) = duplex_transport::duplex();
    relay.accept_subscriber(link, alice());
    send_upload(peer.as_mut(), &captured_upload(50, "forged", true));
    turns(&[&relay]);
    assert!(matches!(receipts(peer.as_mut()).as_slice(), [Err(_)]));
    connect(&relay, &server, AuthorSubject::SYSTEM);
    drive(
        &[&relay, &server],
        relay.wait_for_transaction(own, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(texts(&server), ["owner"]);
}

/// Alice supplies forged provider claims; Bob's relay forwards only the claims
/// from Alice's authenticated connection, and the authority enforces them.
#[test]
fn ordinary_connection_cannot_supply_delegated_claims() {
    let server = authority();
    let relay = open(50, AuthorSubject::SYSTEM, None);
    let (relay_link, mut upstream) = duplex_transport::duplex();
    block_on(relay.connect_upstream(relay_link));
    let (mut peer, link) = duplex_transport::duplex();
    let key = jazz::query::provider_claim_key("role");
    let actual = BTreeMap::from([(key.clone(), Value::String("reader".into()))]);
    relay.accept_subscriber_with_claims(link, alice(), Some(actual.clone()));
    let mut upload = captured_upload(1, "claims", false);
    for message in &mut upload {
        if let jazz::db::SyncMessage::Commit { session, .. } = message {
            *session = Some(BTreeMap::from([(
                key.clone(),
                Value::String("admin".into()),
            )]));
        }
    }
    send_upload(peer.as_mut(), &upload);
    turns(&[&relay]);
    let forwarded = std::iter::from_fn(|| upstream.try_recv())
        .find(|m| matches!(m, jazz::db::SyncMessage::Commit { .. }))
        .unwrap();
    if let jazz::db::SyncMessage::Commit {
        author, session, ..
    } = &forwarded
    {
        assert_eq!(author.as_deref(), Some(alice().canonical()));
        assert_eq!(session.as_ref(), Some(&actual));
    }
    let (mut authority_peer, link) = duplex_transport::duplex();
    server.accept_subscriber(link, AuthorSubject::SYSTEM);
    authority_peer.send(forwarded).unwrap();
    turns(&[&server]);
    assert!(matches!(
        receipts(authority_peer.as_mut()).as_slice(),
        [Err(_)]
    ));
    assert!(texts(&server).is_empty());
}

/// Alice's rejected upload remains rejected after Bob's relay restarts, even
/// when the rejection receipt never reached Alice's original connection.
#[test]
fn rejection_survives_a_lost_receipt_and_relay_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relay.sqlite");
    let server = authority();
    let upload = captured_upload(1, "denied", false);
    {
        let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
        connect(&relay, &server, AuthorSubject::SYSTEM);
        let (mut lost, link) = duplex_transport::duplex();
        relay.accept_subscriber(link, alice());
        send_upload(lost.as_mut(), &upload);
        turns(&[&relay, &server]);
        // Drop the connection without reading its terminal receipt.
    }
    let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
    relay.set_remote_link_hint(RemoteLinkHint::Failed);
    let (mut peer, link) = duplex_transport::duplex();
    relay.accept_subscriber(link, alice());
    send_upload(peer.as_mut(), &upload);
    turns(&[&relay]);
    assert!(matches!(receipts(peer.as_mut()).as_slice(), [Err(_)]));
    assert!(texts(&server).is_empty());
    assert_eq!(server.seq_high_water(), 0);
}

/// Alice's read-only serializable unit crosses Bob's relay. A concurrent
/// accepted write invalidates the stale read, while a fresh read-only unit settles.
#[test]
fn serializable_read_validation_stays_at_the_authority() {
    let server = authority();
    let relay = open(50, AuthorSubject::SYSTEM, None);
    let client = open(1, alice(), None);
    let writer = open(2, bob(), None);
    connect(&relay, &server, AuthorSubject::SYSTEM);
    connect(&client, &relay, alice());
    connect(&writer, &server, bob());
    let dbs = [&client, &relay, &server, &writer];
    let id = jazz::tools::OpenTransactionId([1; 16]);
    block_on(client.begin_serializable(id)).unwrap();
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    assert!(
        drive(&dbs, client.tx_ref(id).all_prepared(&query))
            .unwrap()
            .is_empty()
    );
    let other = insert(&writer, "concurrent", true);
    drive(
        &dbs,
        writer.wait_for_transaction(other, DurabilityTier::Global),
    )
    .unwrap();
    let stale = block_on(client.commit_serializable_handle(id)).unwrap().0;
    let error = drive(
        &dbs,
        client.wait_for_transaction(stale, DurabilityTier::Global),
    )
    .unwrap_err();
    assert!(error.is_serialization_conflict(), "{error}");
    assert!(error.is_conflict(), "authority conflicts remain retryable");
    let _watch =
        block_on(client.subscribe(&query, ReadOpts::for_read_tier(jazz::db::ReadTier::Remote)))
            .unwrap();
    turns(&dbs);
    let id = jazz::tools::OpenTransactionId([2; 16]);
    block_on(client.begin_serializable_for_identity(id, alice())).unwrap();
    assert_eq!(
        drive(&dbs, client.tx_ref(id).all_prepared(&query))
            .unwrap()
            .len(),
        1
    );
    let fresh = block_on(client.commit_serializable_handle(id)).unwrap().0;
    drive(
        &dbs,
        client.wait_for_transaction(fresh, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(client.write_state(fresh).unwrap().fate, Fate::Accepted);
}

/// Alice's upload bytes and the authority's origin-qualified receipts are pinned
/// independently of serialization defaults. Raw SQL inspects the new durable
/// envelope because the public API deliberately does not expose storage bytes.
#[test]
fn relay_journal_and_fates_have_frozen_versioned_bytes() {
    use jazz::db::{SyncMessage, WireError};
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v3/relay-upload.json")).unwrap();
    assert_eq!(fixture["journalRole"], "jazz.sqlite.relay-upload.v2");
    assert_eq!(fixture["fateRole"], "jazz.sqlite.sync.origin-fate.v2");
    assert_eq!(fixture["version"], 3);
    for (name, result) in [
        ("accepted", Ok(())),
        ("conflict", Err(WireError::Conflict("stale".into()))),
        (
            "rejected",
            Err(WireError::Rejected {
                reason: None,
                message: "denied".into(),
            }),
        ),
        ("other", Err(WireError::Other("oops".into()))),
    ] {
        let message = SyncMessage::OriginFateV2 {
            node: [17; 16],
            tx: 127,
            through: 128,
            result,
            held: vec![],
            global: true,
        };
        let bytes = hex::decode(fixture["fates"][name].as_str().unwrap()).unwrap();
        assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
        let decoded = jazz::sync_codec::decode_sync_message(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relay.sqlite");
    let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
    relay.set_remote_link_hint(RemoteLinkHint::Failed);
    let (mut peer, link) = duplex_transport::duplex();
    relay.accept_subscriber(link, alice());
    let mut message = captured_upload(17, "fixture", true).pop().unwrap();
    if let SyncMessage::Commit {
        tx,
        ops,
        author,
        session,
        expect,
        ..
    } = &mut message
    {
        *tx = 127;
        *ops = serde_json::from_value(serde_json::json!([
            {"Delete": {"table": "notes", "row": vec![0x66_u8;16], "target": "Root", "at": 300}}
        ]))
        .unwrap();
        *author = Some(alice().canonical().to_owned());
        *session = None;
        expect.clear();
    } else {
        panic!("expected commit");
    }
    peer.send(message).unwrap();
    turns(&[&relay]);
    let sql = rusqlite::Connection::open(&path).unwrap();
    type JournalColumns = (i64, Vec<u8>, i64, String, Vec<u8>, Vec<u8>, Vec<u8>, i64);
    let (ordinal, node, tx, source, schema, request, normalized, pending): JournalColumns = sql.query_row("SELECT ordinal,node,tx,source,schema_id,request,commit_msg,pending FROM _jazz_relay_upload_v1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?))).unwrap();
    assert_eq!((ordinal, tx, pending), (1, 127, 1));
    assert_eq!(node, [17; 16]);
    assert_eq!(source, alice().canonical());
    assert_eq!(schema.len(), 16);
    assert_eq!(hex::encode(request), fixture["request"]);
    assert_eq!(hex::encode(normalized), fixture["normalized"]);
}

/// Alice's large upload outlives Bob's relay receipt TTL, then authority rejection
/// releases its pins. Raw SQLite only expires the clock-based lease deterministically;
/// public forwarding and rejection still drive the actual state transitions.
#[test]
fn journal_pins_outlive_receipt_expiry_and_release_after_rejection() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relay.sqlite");
    let server = authority();
    let upload = captured_upload(1, &"denied-large".repeat(40000), false);
    {
        let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
        relay.set_remote_link_hint(RemoteLinkHint::Failed);
        let (mut peer, link) = duplex_transport::duplex();
        relay.accept_subscriber(link, alice());
        send_upload(peer.as_mut(), &upload);
        turns(&[&relay]);
    }
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute("UPDATE _jazz_lv_receipt SET expires=0", [])
        .unwrap();
    let count = |table: &str| {
        sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
    };
    assert!(count("_jazz_lv_node") > 1);
    assert_eq!(count("_jazz_lv_relay_root_v1"), 1);
    let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
    relay.set_remote_link_hint(RemoteLinkHint::Failed);
    turns(&[&relay]);
    block_on(relay.evict_expired_staged_large_values()).unwrap();
    turns(&[&relay]);
    assert_eq!(count("_jazz_lv_receipt"), 0);
    assert!(count("_jazz_lv_node") > 1);
    connect(&relay, &server, AuthorSubject::SYSTEM);
    turns(&[&relay, &server]);
    assert_eq!(count("_jazz_lv_relay_root_v1"), 0);
    assert_eq!(count("_jazz_lv_node"), 0);
    assert!(texts(&server).is_empty());
}

/// Alice's journal and large-value pin are one durable unit. A lower-level SQL
/// trigger injects a pin failure; the public relay must reject without forwarding
/// a half-journalled upload, and a later retry can succeed.
#[test]
fn pin_storage_failure_rolls_back_the_entire_journal_unit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relay.sqlite");
    let server = authority();
    let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
    connect(&relay, &server, AuthorSubject::SYSTEM);
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_pin BEFORE INSERT ON _jazz_lv_relay_root_v1 BEGIN SELECT RAISE(ABORT,'injected pin failure'); END").unwrap();
    let upload = captured_upload(1, &"large".repeat(30000), true);
    let (mut peer, link) = duplex_transport::duplex();
    relay.accept_subscriber(link, alice());
    send_upload(peer.as_mut(), &upload);
    turns(&[&relay, &server]);
    assert!(matches!(receipts(peer.as_mut()).as_slice(), [Err(_)]));
    let count: i64 = sql
        .query_row("SELECT count(*) FROM _jazz_relay_upload_v1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 0);
    assert!(texts(&server).is_empty());
    sql.execute_batch("DROP TRIGGER fail_pin").unwrap();
    send_upload(peer.as_mut(), &upload);
    turns(&[&relay, &server]);
    assert!(matches!(receipts(peer.as_mut()).as_slice(), [Ok(())]));
    assert_eq!(texts(&server), ["large".repeat(30000)]);
}

/// Bob's relay fails closed when Alice's pending journal contains an unknown
/// version, trailing bytes, another transaction identity, or a changed scope.
/// Raw SQL corruption is needed to reach states that public writes cannot create.
#[test]
fn corrupt_journal_cannot_be_forwarded_after_restart() {
    for corruption in [
        "UPDATE _jazz_relay_upload_v1 SET commit_msg=x'00'",
        "UPDATE _jazz_relay_upload_v1 SET commit_msg=CAST(commit_msg || x'00' AS BLOB)",
        "UPDATE _jazz_relay_upload_v1 SET tx=tx+1",
        "UPDATE _jazz_relay_upload_v1 SET source='invalid'",
        "UPDATE _jazz_relay_upload_v1 SET request=x'00'",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("relay.sqlite");
        {
            let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
            relay.set_remote_link_hint(RemoteLinkHint::Failed);
            let (mut peer, link) = duplex_transport::duplex();
            relay.accept_subscriber(link, alice());
            send_upload(peer.as_mut(), &captured_upload(1, "pending", true));
            turns(&[&relay]);
        }
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(corruption)
            .unwrap();
        let result = block_on(Db::open_at(
            config(50, AuthorSubject::SYSTEM),
            &path,
            Default::default(),
        ));
        let error = result
            .err()
            .expect("corrupt relay journal must refuse recovery");
        assert_eq!(
            error.code,
            jazz::db::ErrorCode::Storage,
            "{corruption}: {error}"
        );
    }
}

/// Bob's relay cannot publish Alice's fate before the whole incoming SQLite tick
/// commits. A SQL fault after fate insertion and a controlled upstream are needed
/// to exercise rollback after the inner receipt savepoint has already released.
#[test]
fn failed_tick_does_not_publish_a_relay_fate_or_consume_its_retry() {
    use jazz::db::{LOCAL_BASE, SyncMessage};
    for (table, condition) in [("_jazz_meta", "WHEN NEW.k='known'"), ("_jazz_applied", "")] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("relay.sqlite");
        let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
        relay.set_remote_link_hint(RemoteLinkHint::Failed);
        let (mut peer, link) = duplex_transport::duplex();
        relay.accept_subscriber(link, alice());
        send_upload(peer.as_mut(), &captured_upload(1, "pending", true));
        turns(&[&relay]);
        let (link, mut authority) = duplex_transport::duplex();
        block_on(relay.connect_upstream(link));
        turns(&[&relay]);
        while authority.try_recv().is_some() {}
        let sql = rusqlite::Connection::open(&path).unwrap();
        sql.execute_batch(&format!("CREATE TRIGGER fail_receipt BEFORE INSERT ON {table} {condition} BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END")).unwrap();
        let fate = SyncMessage::FateV1 {
            node: [1; 16],
            tx: LOCAL_BASE + 1,
            through: 7,
            result: Ok(()),
        };
        authority.send(fate.clone()).unwrap();
        let error = block_on(relay.tick()).unwrap_err();
        assert!(
            error.message.contains("injected receipt failure"),
            "{error}"
        );
        let pending: i64 = sql
            .query_row("SELECT pending FROM _jazz_relay_upload_v1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(pending, 1);
        let recorded: i64 = sql
            .query_row("SELECT count(*) FROM _jazz_applied", [], |r| r.get(0))
            .unwrap();
        assert_eq!(recorded, 0);
        assert!(receipts(peer.as_mut()).is_empty());
        sql.execute_batch("DROP TRIGGER fail_receipt").unwrap();
        turns(&[&relay]);
        assert!(std::iter::from_fn(|| authority.try_recv()).any(
            |m| matches!(m,SyncMessage::Commit { node,tx,.. } if node==[1;16] && tx==LOCAL_BASE+1)
        ));
        assert!(receipts(peer.as_mut()).is_empty());
        authority.send(fate).unwrap();
        turns(&[&relay]);
        assert!(matches!(receipts(peer.as_mut()).as_slice(), [Ok(())]));
    }
}

/// Alice queues an insert and an empty serializable commit that reads that own
/// pending insert. Bob's relay preserves their order and original dependency IDs.
#[test]
fn own_pending_dependencies_keep_original_order_through_relay() {
    let client = open(1, alice(), None);
    client.set_remote_link_hint(RemoteLinkHint::Failed);
    let parent = insert(&client, "own parent", true);
    let id = jazz::tools::OpenTransactionId([3; 16]);
    block_on(client.begin_serializable(id)).unwrap();
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        block_on(client.tx_ref(id).all_prepared(&query))
            .unwrap()
            .len(),
        1
    );
    let child = block_on(client.commit_serializable_handle(id)).unwrap().0;
    let relay = open(50, AuthorSubject::SYSTEM, None);
    relay.set_remote_link_hint(RemoteLinkHint::Failed);
    connect(&client, &relay, alice());
    turns(&[&client, &relay]);
    assert_eq!(client.write_state(child).unwrap().fate, Fate::Pending);
    let server = authority();
    connect(&relay, &server, AuthorSubject::SYSTEM);
    drive(
        &[&client, &relay, &server],
        client.wait_for_transaction(child, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(client.write_state(parent).unwrap().fate, Fate::Accepted);
    assert_eq!(texts(&server), ["own parent"]);
}

struct LimitedTransport {
    inner: Box<dyn jazz::db::Transport>,
}
impl jazz::db::Transport for LimitedTransport {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        if matches!(&message, jazz::db::SyncMessage::Commit { .. }) {
            return Err(jazz::db::TransportError::Backpressure);
        }
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        self.inner.try_recv()
    }
}
/// Bob's transport stops between Alice's large upload and its commit. A new
/// connection retries the complete unit and settles it exactly once.
#[test]
fn reconnect_replays_a_partially_backpressured_upload() {
    let server = authority();
    let relay = open(50, AuthorSubject::SYSTEM, None);
    relay.set_remote_link_hint(RemoteLinkHint::Failed);
    let client = open(1, alice(), None);
    connect(&client, &relay, alice());
    let text = "backpressure".repeat(40000);
    let tx = insert(&client, &text, true);
    turns(&[&client, &relay]);
    let (a, b) = duplex_transport::duplex();
    block_on(relay.connect_upstream(Box::new(LimitedTransport { inner: a })));
    server.accept_subscriber(b, AuthorSubject::SYSTEM);
    // The transport accepts chunks, then backpressures the Commit.
    turns(&[&relay, &server]);
    assert_eq!(client.write_state(tx).unwrap().fate, Fate::Pending);
    assert!(texts(&server).is_empty());
    connect(&relay, &server, AuthorSubject::SYSTEM);
    drive(
        &[&client, &relay, &server],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(texts(&server), [text]);
    assert_eq!(server.seq_high_water(), 1);
}

/// Alice's private owner refuses a foreground payload with a missing or changed
/// author/session binding before journalling. Raw transport is necessary to
/// alter that otherwise host-bound payload; reopening and retrying the original
/// unit prove the rejected payload neither persisted nor consumed its identity.
#[test]
fn private_owner_upload_keeps_its_bound_author_before_and_after_reopen() {
    use jazz::db::SyncMessage;
    for case in ["different author", "missing author", "missing session"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("private-owner.sqlite");
        let original = captured_upload(1, "alice's original", true);
        {
            let owner = open(50, alice(), Some(&path));
            owner.set_scope_isolated_storage_owner().unwrap();
            let (mut peer, link) = duplex_transport::duplex();
            owner.accept_subscriber(link, alice());
            let mut changed = original.clone();
            for message in &mut changed {
                if let SyncMessage::Commit {
                    author, session, ..
                } = message
                {
                    match case {
                        "different author" => *author = Some(bob().canonical().to_owned()),
                        "missing author" => *author = None,
                        "missing session" => *session = None,
                        _ => unreachable!(),
                    }
                }
            }
            send_upload(peer.as_mut(), &changed);
            turns(&[&owner]);
            let fates = receipts(peer.as_mut());
            assert!(matches!(fates.as_slice(), [Err(_)]), "{case}: {fates:?}");
            assert!(texts(&owner).is_empty(), "{case}");
            block_on(owner.close()).unwrap();
        }
        let owner = open(50, alice(), Some(&path));
        owner.set_scope_isolated_storage_owner().unwrap();
        let server = authority();
        connect(&owner, &server, alice());
        turns(&[&owner, &server]);
        assert!(
            texts(&server).is_empty(),
            "{case}: rejected upload survived reopen"
        );
        let (mut peer, link) = duplex_transport::duplex();
        owner.accept_subscriber(link, alice());
        send_upload(peer.as_mut(), &original);
        turns(&[&owner, &server]);
        assert!(
            matches!(receipts(peer.as_mut()).as_slice(), [Ok(())]),
            "{case}"
        );
        assert_eq!(texts(&server), ["alice's original"]);
        let rows = block_on(server.all(
            &server.prepare_query(&Query::from("notes")).unwrap(),
            ReadOpts::default(),
        ))
        .unwrap();
        let provenance = server
            .row_provenance("notes", rows[0].row_uuid())
            .unwrap()
            .unwrap();
        assert_eq!(provenance.1, alice().canonical());
        assert_eq!(provenance.3, alice().canonical());
        block_on(owner.close()).unwrap();
    }
}

/// Bob cannot adopt Alice's already-journalled unit after the relay restarts,
/// even by changing its author to match his valid connection. Raw upload replay
/// controls the original transaction identity; public rows and provenance prove
/// that only Alice's retained unit reaches the authority.
#[test]
fn durable_relay_retry_cannot_change_the_admitted_sender() {
    use jazz::db::SyncMessage;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relay.sqlite");
    let original = captured_upload(1, "alice's retained unit", true);
    {
        let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
        relay.set_remote_link_hint(RemoteLinkHint::Failed);
        let (mut peer, link) = duplex_transport::duplex();
        relay.accept_subscriber(link, alice());
        send_upload(peer.as_mut(), &original);
        turns(&[&relay]);
        assert!(receipts(peer.as_mut()).is_empty());
        block_on(relay.close()).unwrap();
    }
    let relay = open(50, AuthorSubject::SYSTEM, Some(&path));
    let (mut bob_peer, link) = duplex_transport::duplex();
    relay.accept_subscriber(link, bob());
    for change_author in [false, true] {
        let mut substituted = original.clone();
        if change_author {
            for message in &mut substituted {
                if let SyncMessage::Commit { author, .. } = message {
                    *author = Some(bob().canonical().to_owned());
                }
            }
        }
        send_upload(bob_peer.as_mut(), &substituted);
        turns(&[&relay]);
        let fates = receipts(bob_peer.as_mut());
        assert!(matches!(fates.as_slice(), [Err(_)]), "{fates:?}");
    }
    let server = authority();
    connect(&relay, &server, AuthorSubject::SYSTEM);
    turns(&[&relay, &server]);
    assert_eq!(texts(&server), ["alice's retained unit"]);
    let rows = block_on(server.all(
        &server.prepare_query(&Query::from("notes")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    let provenance = server
        .row_provenance("notes", rows[0].row_uuid())
        .unwrap()
        .unwrap();
    assert_eq!(provenance.1, alice().canonical());
    assert_eq!(provenance.3, alice().canonical());
    block_on(relay.close()).unwrap();
}

/// Alice's private relay cannot substitute or omit the original author, including
/// on a replay. An inadmissible frame cannot poison the original transaction:
/// the valid upload still settles once and retains its original accepted fate.
#[test]
fn private_upload_scope_is_checked_before_replay_without_poisoning_the_original() {
    use jazz::db::SyncMessage;
    let server = authority();
    let admitted = BTreeMap::from([("tenant".into(), Value::String("alice".into()))]);
    let (mut peer, link) = duplex_transport::duplex();
    server
        .accept_scope_isolated_subscriber(link, alice(), admitted.clone())
        .unwrap();
    let mut expected = Vec::new();
    for scenario in 0..4 {
        let text = format!("allowed-{scenario}");
        let mut valid = captured_upload(70 + scenario, &text, true);
        for message in &mut valid {
            if let SyncMessage::Commit { session, .. } = message {
                *session = Some(admitted.clone());
            }
        }
        let mut forged = valid.clone();
        for message in &mut forged {
            if let SyncMessage::Commit { author, .. } = message {
                match scenario {
                    0 => *author = Some(bob().canonical().to_owned()),
                    1 => *author = None,
                    2 => *author = Some(AuthorSubject::SYSTEM.canonical().to_owned()),
                    3 => {
                        *author = Some(
                            AuthorSubject::system_at(NodeUuid::from_bytes([99; 16]))
                                .canonical()
                                .to_owned(),
                        )
                    }
                    _ => unreachable!(),
                }
            }
        }
        for already_accepted in [false, true] {
            send_upload(peer.as_mut(), &forged);
            turns(&[&server]);
            assert!(matches!(receipts(peer.as_mut()).as_slice(), [Err(_)]));
            assert_eq!(
                texts(&server),
                expected,
                "a forged upload cannot change rows"
            );
            send_upload(peer.as_mut(), &valid);
            turns(&[&server]);
            assert!(matches!(receipts(peer.as_mut()).as_slice(), [Ok(())]));
            if !already_accepted {
                expected.push(text.clone());
            }
            assert_eq!(
                texts(&server),
                expected,
                "the valid upload applies only once"
            );
        }
    }
}

/// Alice can move her original writer between a direct connection and Bob's
/// trusted relay without changing its identity or losing authority settlement.
/// Alice -> authority; Alice -> trusted relay -> authority (and the reverse).
#[test]
fn a_writer_keeps_its_identity_when_switching_between_direct_and_trusted_relay() {
    for direct_first in [true, false] {
        let server = authority();
        let relay = open(50, AuthorSubject::SYSTEM, None);
        let client = open(1, alice(), None);
        connect(&relay, &server, AuthorSubject::SYSTEM);
        if direct_first {
            connect(&client, &server, alice());
        } else {
            connect(&client, &relay, alice());
        }
        let first = insert(&client, "before", true);
        let dbs = [&client, &relay, &server, &relay, &client];
        drive(
            &dbs,
            client.wait_for_transaction(first, DurabilityTier::Global),
        )
        .unwrap();
        if direct_first {
            connect(&client, &relay, alice());
        } else {
            connect(&client, &server, alice());
        }
        let next = insert(&client, "after", true);
        drive(
            &dbs,
            client.wait_for_transaction(next, DurabilityTier::Global),
        )
        .unwrap_or_else(|error| panic!("direct_first={direct_first}: {error}"));
        assert_eq!(texts(&server), ["after", "before"]);
        assert_eq!(client.write_state(next).unwrap().fate, Fate::Accepted);
    }
}

struct WriterOwnerBudgetTransport {
    inner: Box<dyn jazz::db::Transport>,
    budget: std::rc::Rc<std::cell::Cell<usize>>,
    sent: std::rc::Rc<std::cell::RefCell<Vec<jazz::db::SyncMessage>>>,
}
impl jazz::db::Transport for WriterOwnerBudgetTransport {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        if matches!(
            &message,
            jazz::db::SyncMessage::WriterOwnerV1 { .. } | jazz::db::SyncMessage::Commit { .. }
        ) {
            if self.budget.get() == 0 {
                return Err(jazz::db::TransportError::Backpressure);
            }
            self.budget.set(self.budget.get() - 1);
            self.sent.borrow_mut().push(message.clone());
        }
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        self.inner.try_recv()
    }
}

/// Alice's upload and Bob's relay's own write each drain through a one-message
/// budget after repeated stalls. Forwarded ownership is sent once; Bob's own
/// writer uses the connection's admitted identity without a declaration.
/// Alice -> relay --[owner; pause; commit]--> authority -> real receipt.
#[test]
fn writer_owner_backpressure_drains_without_duplicate_declarations() {
    use jazz::db::SyncMessage;
    for forwarded in [false, true] {
        let server = authority();
        let relay = open(50, AuthorSubject::SYSTEM, None);
        let client = open(1, alice(), None);
        relay.set_remote_link_hint(RemoteLinkHint::Failed);
        connect(&client, &relay, alice());
        let writer = if forwarded { &client } else { &relay };
        let tx = insert(writer, "one complete upload", true);
        turns(&[&client, &relay]);
        let budget = std::rc::Rc::new(std::cell::Cell::new(0));
        let sent = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let (up, down) = duplex_transport::duplex();
        block_on(relay.connect_upstream(Box::new(WriterOwnerBudgetTransport {
            inner: up,
            budget: budget.clone(),
            sent: sent.clone(),
        })));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        for _ in 0..32 {
            turns(&[&relay, &server]);
        }
        assert!(sent.borrow().is_empty());
        assert!(texts(&server).is_empty());
        assert_eq!(writer.write_state(tx).unwrap().fate, Fate::Pending);
        for turn in 0..64 {
            budget.set(1);
            for db in [&relay, &server, &client] {
                block_on(db.tick()).unwrap();
            }
            if turn == 0 && forwarded {
                assert_eq!(writer.write_state(tx).unwrap().fate, Fate::Pending);
                assert!(
                    texts(&server).is_empty(),
                    "a declaration does not publish or settle the write"
                );
            }
            if writer.write_state(tx).unwrap().fate == Fate::Accepted {
                break;
            }
        }
        assert_eq!(
            sent.borrow().len(),
            if forwarded { 2 } else { 1 },
            "forwarded={forwarded}: one commit, with one declaration only for a forwarded writer"
        );
        let messages = sent.borrow();
        if forwarded {
            assert!(
                matches!(&messages[0], SyncMessage::WriterOwnerV1 { node, owner } if *node == [1; 16] && owner == alice().canonical())
            );
            assert!(matches!(&messages[1], SyncMessage::Commit { node, .. } if *node == [1; 16]));
        } else {
            assert!(matches!(&messages[0], SyncMessage::Commit { node, .. } if *node == [50; 16]));
        }
        assert_eq!(writer.write_state(tx).unwrap().fate, Fate::Accepted);
        assert_eq!(texts(&server), ["one complete upload"]);
        assert_eq!(server.seq_high_water(), 1);
    }
}

/// Mallory cannot attest Alice's writer on an ordinary session, even before its
/// first write; refusal leaves that node available to authenticated Alice.
#[test]
fn writer_owner_ordinary_attestation_is_rejected_before_node_claim() {
    let server = authority();
    let (mut peer, link) = duplex_transport::duplex();
    server.accept_subscriber(link, bob());
    peer.send(jazz::db::SyncMessage::WriterOwnerV1 {
        node: [1; 16],
        owner: alice().canonical().to_owned(),
    })
    .unwrap();
    send_upload(peer.as_mut(), &captured_upload(1, "forged", true));
    turns(&[&server]);
    let closed = server.take_closed_subscribers();
    assert_eq!(closed.len(), 1);
    assert!(closed[0].1.message.contains("trusted SYSTEM"));
    assert!(texts(&server).is_empty());
    let client = open(1, alice(), None);
    connect(&client, &server, alice());
    let tx = insert(&client, "legitimate", true);
    drive(
        &[&client, &server],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(texts(&server), ["legitimate"]);
}

/// Bob's trusted carrier cannot apply a declaration to a different writer node;
/// the rejected frame neither writes rows nor reserves Alice's node.
#[test]
fn writer_owner_wrong_node_attestation_cannot_claim_a_writer() {
    let server = authority();
    let (mut peer, link) = duplex_transport::duplex();
    server.accept_subscriber(link, AuthorSubject::SYSTEM);
    peer.send(jazz::db::SyncMessage::WriterOwnerV1 {
        node: [2; 16],
        owner: bob().canonical().to_owned(),
    })
    .unwrap();
    send_upload(peer.as_mut(), &captured_upload(1, "wrong node", true));
    turns(&[&server]);
    let closed = server.take_closed_subscribers();
    assert_eq!(closed.len(), 1);
    assert!(closed[0].1.message.contains("different node"));
    assert!(texts(&server).is_empty());
    let client = open(1, alice(), None);
    connect(&client, &server, alice());
    let tx = insert(&client, "legitimate", true);
    drive(
        &[&client, &server],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(texts(&server), ["legitimate"]);
}

/// Alice's previously admitted node stays hers when a trusted carrier asserts
/// Bob as its owner. Replaying Alice's real unit still returns its original fate.
#[test]
fn writer_owner_conflicting_attestation_preserves_existing_owner_and_receipt() {
    use jazz::db::SyncMessage;
    let server = authority();
    let original = captured_upload(1, "original", true);
    let (mut alice_link, accepted) = duplex_transport::duplex();
    server.accept_subscriber(accepted, alice());
    send_upload(alice_link.as_mut(), &original);
    turns(&[&server]);
    assert!(matches!(receipts(alice_link.as_mut()).as_slice(), [Ok(())]));
    let (mut peer, link) = duplex_transport::duplex();
    server.accept_subscriber(link, AuthorSubject::SYSTEM);
    peer.send(SyncMessage::WriterOwnerV1 {
        node: [1; 16],
        owner: bob().canonical().to_owned(),
    })
    .unwrap();
    send_upload(peer.as_mut(), &original);
    turns(&[&server]);
    assert!(matches!(receipts(peer.as_mut()).as_slice(), [Err(_)]));
    send_upload(alice_link.as_mut(), &original);
    turns(&[&server]);
    assert!(matches!(receipts(alice_link.as_mut()).as_slice(), [Ok(())]));
    assert_eq!(texts(&server), ["original"]);
    assert_eq!(server.seq_high_water(), 1);
}

/// Alice starts directly, queues a second write through two offline durable
/// relays, then reopens every server and returns to the direct route.
/// Alice -> authority; Alice -> near -> far (restart) -> authority; Alice -> authority.
#[test]
fn writer_owner_survives_multihop_reopen_and_direct_route_changes() {
    let account = jazz::account_registry::AccountId(uuid::Uuid::from_bytes([0x11; 16]));
    for author in [alice(), alice().with_account(account)] {
        let directory = tempfile::tempdir().unwrap();
        let server_path = directory.path().join("authority.sqlite");
        let near_path = directory.path().join("near.sqlite");
        let far_path = directory.path().join("far.sqlite");
        let client = open(1, author, None);
        let second;
        {
            let server = open(90, AuthorSubject::SYSTEM, Some(&server_path));
            server.set_authority();
            connect(&client, &server, author);
            let first = insert(&client, "first direct", true);
            drive(
                &[&client, &server],
                client.wait_for_transaction(first, DurabilityTier::Global),
            )
            .unwrap();
            let near = open(50, AuthorSubject::SYSTEM, Some(&near_path));
            let far = open(51, AuthorSubject::SYSTEM, Some(&far_path));
            far.set_remote_link_hint(RemoteLinkHint::Failed);
            connect(&near, &far, AuthorSubject::SYSTEM);
            connect(&client, &near, author);
            second = insert(&client, "second relayed", true);
            turns(&[&client, &near, &far]);
            assert_eq!(client.write_state(second).unwrap().fate, Fate::Pending);
            assert_eq!(texts(&server), ["first direct"]);
        }
        let server = open(90, AuthorSubject::SYSTEM, Some(&server_path));
        server.set_authority();
        let near = open(50, AuthorSubject::SYSTEM, Some(&near_path));
        let far = open(51, AuthorSubject::SYSTEM, Some(&far_path));
        connect(&far, &server, AuthorSubject::SYSTEM);
        connect(&near, &far, AuthorSubject::SYSTEM);
        connect(&client, &near, author);
        drive(
            &[&client, &near, &far, &server],
            client.wait_for_transaction(second, DurabilityTier::Global),
        )
        .unwrap();
        connect(&client, &server, author);
        let third = insert(&client, "third direct", true);
        drive(
            &[&client, &server],
            client.wait_for_transaction(third, DurabilityTier::Global),
        )
        .unwrap();
        assert_eq!(
            texts(&server),
            ["first direct", "second relayed", "third direct"]
        );
        assert_eq!(server.seq_high_water(), 3);
        let query = server.prepare_query(&Query::from("notes")).unwrap();
        for row in block_on(server.all(&query, ReadOpts::default())).unwrap() {
            let provenance = server
                .row_provenance("notes", row.row_uuid())
                .unwrap()
                .unwrap();
            assert_eq!(provenance.1, author.canonical());
        }
    }
}

/// Bob's trusted carrier uses Alice's writer declaration for one commit only.
/// A missing declaration on the next commit cannot reuse Alice's ownership.
#[test]
fn writer_owner_declaration_is_consumed_by_its_next_commit() {
    use jazz::db::SyncMessage;
    let server = authority();
    let original = captured_upload(1, "original", true);
    let (mut peer, link) = duplex_transport::duplex();
    server.accept_subscriber(link, AuthorSubject::SYSTEM);
    peer.send(SyncMessage::WriterOwnerV1 {
        node: [1; 16],
        owner: alice().canonical().to_owned(),
    })
    .unwrap();
    send_upload(peer.as_mut(), &original);
    turns(&[&server]);
    assert!(matches!(receipts(peer.as_mut()).as_slice(), [Ok(())]));
    send_upload(peer.as_mut(), &original);
    turns(&[&server]);
    assert!(matches!(receipts(peer.as_mut()).as_slice(), [Err(_)]));
    peer.send(SyncMessage::WriterOwnerV1 {
        node: [1; 16],
        owner: alice().canonical().to_owned(),
    })
    .unwrap();
    send_upload(peer.as_mut(), &original);
    turns(&[&server]);
    assert!(matches!(receipts(peer.as_mut()).as_slice(), [Ok(())]));
    assert_eq!(texts(&server), ["original"]);
    assert_eq!(server.seq_high_water(), 1);
}

/// Alice's ownership message has independently assembled bytes. Both public
/// decoders preserve its exact fields; malformed frames never become messages.
/// This boundary-level test pins the wire role that a normal row API cannot inspect.
#[test]
fn writer_owner_wire_bytes_match_independent_field_fixtures() {
    use jazz::db::{SyncMessage, decode_message, encode_message};
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_writer_owner_v1.json")).unwrap();
    assert_eq!(fixture["role"], "jazz.sqlite.sync.writer-owner.v1");
    assert_eq!(fixture["outer_encoding"], "jazz.sqlite.sync.v3");
    for case in fixture["cases"].as_array().unwrap() {
        let bytes = hex::decode(case["bytes_hex"].as_str().unwrap()).unwrap();
        let node = hex::decode(case["node_hex"].as_str().unwrap()).unwrap();
        let owner = case["owner"].as_str().unwrap();
        AuthorSubject::from_canonical(owner).unwrap();
        let message = SyncMessage::WriterOwnerV1 {
            node: node.try_into().unwrap(),
            owner: owner.to_owned(),
        };
        assert_eq!(encode_message(&message).unwrap(), bytes);
        for decoded in [
            decode_message(&bytes).unwrap(),
            jazz::sync_codec::decode_sync_message(&bytes).unwrap(),
        ] {
            assert!(
                matches!(decoded, SyncMessage::WriterOwnerV1 { node: decoded_node, owner: decoded_owner } if decoded_node == [1;16] && decoded_owner == owner)
            );
        }
        for end in 0..bytes.len() {
            assert!(jazz::sync_codec::decode_sync_message(&bytes[..end]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(jazz::sync_codec::decode_sync_message(&trailing).is_err());
    }
}

/// Bob's trusted carrier cannot install a malformed owner spelling, and its
/// declaration is scoped to that connection rather than a later SYSTEM link.
#[test]
fn writer_owner_invalid_or_abandoned_declarations_do_not_leak() {
    use jazz::db::SyncMessage;
    for owner in ["not json", "[]", r#"[ "https://issuer.example", "alice" ]"#] {
        let server = authority();
        let (mut peer, link) = duplex_transport::duplex();
        server.accept_subscriber(link, AuthorSubject::SYSTEM);
        peer.send(SyncMessage::WriterOwnerV1 {
            node: [1; 16],
            owner: owner.to_owned(),
        })
        .unwrap();
        send_upload(peer.as_mut(), &captured_upload(1, "invalid", true));
        turns(&[&server]);
        assert_eq!(server.take_closed_subscribers().len(), 1);
        assert!(texts(&server).is_empty());
    }
    let server = authority();
    let (mut abandoned, link) = duplex_transport::duplex();
    server.accept_subscriber(link, AuthorSubject::SYSTEM);
    abandoned
        .send(SyncMessage::WriterOwnerV1 {
            node: [2; 16],
            owner: bob().canonical().to_owned(),
        })
        .unwrap();
    turns(&[&server]);
    let (mut current, link) = duplex_transport::duplex();
    server.accept_subscriber(link, AuthorSubject::SYSTEM);
    send_upload(
        current.as_mut(),
        &captured_upload(1, "fresh connection", true),
    );
    turns(&[&server]);
    assert!(matches!(receipts(current.as_mut()).as_slice(), [Ok(())]));
    assert_eq!(texts(&server), ["fresh connection"]);
}

/// Alice owns the writer, while Bob is the delegated permission subject and
/// row author. Ownership cannot bypass Bob's policy or replace his provenance.
#[test]
fn writer_owner_attestation_keeps_permission_scope_and_row_attribution_separate() {
    use jazz::db::SyncMessage;
    for allowed in [false, true] {
        let server = authority();
        let (mut peer, link) = duplex_transport::duplex();
        server.accept_subscriber(link, AuthorSubject::SYSTEM);
        let mut upload = captured_upload(1, "delegated", allowed);
        for message in &mut upload {
            if let SyncMessage::Commit {
                author, session, ..
            } = message
            {
                *author = Some(bob().canonical().to_owned());
                *session = Some(BTreeMap::new());
            }
        }
        peer.send(SyncMessage::WriterOwnerV1 {
            node: [1; 16],
            owner: alice().canonical().to_owned(),
        })
        .unwrap();
        send_upload(peer.as_mut(), &upload);
        turns(&[&server]);
        let receipts = receipts(peer.as_mut());
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].is_ok(), allowed);
        if allowed {
            let query = server.prepare_query(&Query::from("notes")).unwrap();
            let rows = block_on(server.all(&query, ReadOpts::default())).unwrap();
            assert_eq!(rows.len(), 1);
            let provenance = server
                .row_provenance("notes", rows[0].row_uuid())
                .unwrap()
                .unwrap();
            assert_eq!(provenance.1, bob().canonical());
        } else {
            assert!(texts(&server).is_empty());
        }
    }
}
