//! Storage-owner receipt barriers, independent of foreground mutation admission.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use jazz::db::{Db, DbConfig, DbIdentity, RemoteLinkHint, SyncMessage, Transport, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::pin,
    task::{Context, Waker},
};
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn open(node: u8) -> Db {
    block_on(Db::open(DbConfig::new(
        common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("text", ColumnType::Text)
                        .policies(common::allow_all_policies()),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    )))
    .unwrap()
}
fn offline_owner() -> Db {
    let db = open(50);
    db.set_remote_link_hint(RemoteLinkHint::Failed);
    db.set_scope_isolated_storage_owner().unwrap();
    db.require_owner_durability_flush();
    db
}
fn insert(db: &Db, text: &str) -> u64 {
    block_on(db.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String(text.into()))]),
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id()
}
fn upload(node: u8, text: &str) -> Vec<SyncMessage> {
    let db = open(node);
    let (a, mut b) = duplex_transport::duplex();
    block_on(db.connect_upstream(a));
    insert(&db, text);
    block_on(db.tick()).unwrap();
    std::iter::from_fn(|| b.try_recv())
        .filter(|m| {
            matches!(
                m,
                SyncMessage::Commit { .. } | SyncMessage::PushChunks { .. }
            )
        })
        .collect()
}
fn peer(owner: &Db) -> Box<dyn Transport> {
    let (a, b) = duplex_transport::duplex();
    owner.accept_subscriber(b, alice());
    a
}
fn send(peer: &mut dyn Transport, messages: &[SyncMessage]) {
    for m in messages {
        peer.send(m.clone()).unwrap();
    }
}
fn acks(peer: &mut dyn Transport) -> Vec<([u8; 16], u64)> {
    std::iter::from_fn(|| peer.try_recv())
        .filter_map(|m| {
            if let SyncMessage::OwnerStoredV1 { node, tx } = m {
                Some((node, tx))
            } else {
                None
            }
        })
        .collect()
}
fn tick(db: &Db) {
    block_on(db.tick()).unwrap();
}
fn pending<T>(future: impl Future<Output = T>) {
    let mut future = pin!(future);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

/// Alice arrives before Bob while the backing store flush is pending. Bob's
/// later transaction, with the same number at another node, needs its own barrier.
#[test]
fn an_owner_barrier_never_acknowledges_later_arrivals() {
    let owner = offline_owner();
    let mut a = peer(&owner);
    let mut b = peer(&owner);
    send(a.as_mut(), &upload(1, "alice"));
    tick(&owner);
    assert!(acks(a.as_mut()).is_empty());
    let first = owner.prepare_owner_durability().unwrap().unwrap();
    send(b.as_mut(), &upload(2, "bob"));
    tick(&owner);
    owner.confirm_owner_durability(first).unwrap();
    owner.flush_owner_durability().unwrap();
    let first = acks(a.as_mut());
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, [1; 16]);
    assert!(acks(b.as_mut()).is_empty());
    owner
        .confirm_owner_durability(owner.prepare_owner_durability().unwrap().unwrap())
        .unwrap();
    owner.flush_owner_durability().unwrap();
    let second = acks(b.as_mut());
    assert_eq!(second, vec![([2; 16], first[0].1)]);
    tick(&owner);
    assert!(acks(a.as_mut()).is_empty());
    assert!(acks(b.as_mut()).is_empty());
}

/// Alice's memory replica has a queued write when it enters foreground mode.
/// This transition fixture isolates receipt semantics while new foreground
/// mutation admission remains guarded pending recovery/overlay qualification.
#[test]
fn foreground_local_wait_requires_its_owners_receipt() {
    let owner = offline_owner();
    let tab = open(1);
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let tx = insert(&tab, "pending");
    tab.set_non_durable_client();
    assert_eq!(
        tab.write_state(tx).unwrap().durability,
        DurabilityTier::None
    );
    let (a, b) = duplex_transport::duplex();
    block_on(tab.connect_upstream(a));
    owner.accept_subscriber(b, alice());
    tick(&tab);
    tick(&owner);
    tick(&tab);
    pending(tab.wait_for_transaction(tx, DurabilityTier::Local));
    pending(tab.wait_for_pending_writes(DurabilityTier::Local));
    block_on(tab.wait_for_transaction(tx, DurabilityTier::None)).unwrap();
    owner
        .confirm_owner_durability(owner.prepare_owner_durability().unwrap().unwrap())
        .unwrap();
    owner.flush_owner_durability().unwrap();
    tick(&tab);
    assert_eq!(
        tab.write_state(tx).unwrap().durability,
        DurabilityTier::Local
    );
    assert_eq!(tab.write_state(tx).unwrap().fate, Fate::Pending);
    block_on(tab.wait_for_transaction(tx, DurabilityTier::Local)).unwrap();
    block_on(tab.wait_for_pending_writes(DurabilityTier::Local)).unwrap();
    pending(tab.wait_for_transaction(tx, DurabilityTier::Global));
}

/// Bob's receipt cannot settle Alice's equal-numbered write; an unknown receipt
/// also cannot create a transaction. Only the matching original node qualifies.
#[test]
fn unknown_and_other_writer_storage_receipts_are_ignored() {
    let tab = open(1);
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let tx = insert(&tab, "alice");
    tab.set_non_durable_client();
    let (a, mut b) = duplex_transport::duplex();
    block_on(tab.connect_upstream(a));
    for (node, number) in [([2; 16], tx), ([1; 16], tx + 1)] {
        b.send(SyncMessage::OwnerStoredV1 { node, tx: number })
            .unwrap();
    }
    tick(&tab);
    assert_eq!(
        tab.write_state(tx).unwrap().durability,
        DurabilityTier::None
    );
    b.send(SyncMessage::OwnerStoredV1 { node: [1; 16], tx })
        .unwrap();
    tick(&tab);
    assert_eq!(
        tab.write_state(tx).unwrap().durability,
        DurabilityTier::Local
    );
}

/// Bob cannot confirm Alice's owner token against a different database or a
/// retired file. Neither operation may emit a stored receipt to Alice.
#[test]
fn storage_barriers_are_bound_to_the_live_owner() {
    let owner = offline_owner();
    let other = offline_owner();
    let mut a = peer(&owner);
    send(a.as_mut(), &upload(1, "pending"));
    tick(&owner);
    let token = owner.prepare_owner_durability().unwrap().unwrap();
    assert!(other.confirm_owner_durability(token).is_err());
    assert!(acks(a.as_mut()).is_empty());
    let token = owner.prepare_owner_durability().unwrap().unwrap();
    owner.retire_storage_owner().unwrap();
    assert!(owner.confirm_owner_durability(token).is_err());
    assert!(acks(a.as_mut()).is_empty());
}

/// Alice sends a descriptor before its large-value chunks. Bob cannot claim
/// local retention until every required chunk arrives and the value validates.
#[test]
fn incomplete_large_upload_has_no_storage_receipt() {
    let owner = offline_owner();
    let mut a = peer(&owner);
    let messages = upload(1, &"large payload".repeat(40000));
    let (commits, chunks): (Vec<_>, Vec<_>) = messages
        .into_iter()
        .partition(|m| matches!(m, SyncMessage::Commit { .. }));
    send(a.as_mut(), &commits);
    tick(&owner);
    assert!(owner.prepare_owner_durability().unwrap().is_none());
    assert!(acks(a.as_mut()).is_empty());
    send(a.as_mut(), &chunks);
    tick(&owner);
    owner
        .confirm_owner_durability(owner.prepare_owner_durability().unwrap().unwrap())
        .unwrap();
    owner.flush_owner_durability().unwrap();
    assert_eq!(acks(a.as_mut()).len(), 1);
}

/// Alice's synchronous SQLite owner confirms its committed journal without an
/// asynchronous host hook; a deferred owner never takes that implicit path.
#[test]
fn synchronous_owner_uses_the_sqlite_commit_barrier() {
    let owner = open(50);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let mut a = peer(&owner);
    send(a.as_mut(), &upload(1, "sync"));
    tick(&owner);
    assert_eq!(acks(a.as_mut()).len(), 1);
}

fn binding_owner() -> jazz::binding_host::Binding {
    jazz::binding_host::Binding::new(
        offline_owner(),
        DbIdentity {
            node: NodeUuid::from_bytes([50; 16]),
            author: alice(),
        },
        false,
    )
}

/// Alice's host flush fails before durable storage completes. Bob must see no
/// acknowledgement; retrying the real host boundary can later confirm the unit.
#[test]
fn a_failed_host_flush_emits_no_receipt_and_can_retry() {
    use jazz::binding_host::HostError;
    let owner = binding_owner();
    let mut a = peer(&owner.db);
    send(a.as_mut(), &upload(1, "retry"));
    *owner.hooks.flush_owner.borrow_mut() = Some(Box::new(|| {
        Box::pin(async { Err(HostError::Argument("injected storage failure".into())) })
    }));
    assert!(block_on(owner.tick()).is_err());
    assert!(acks(a.as_mut()).is_empty());
    *owner.hooks.flush_owner.borrow_mut() = Some(Box::new(|| Box::pin(async { Ok(()) })));
    block_on(owner.tick()).unwrap();
    assert_eq!(acks(a.as_mut()).len(), 1);
    assert!(owner.db.prepare_owner_durability().unwrap().is_none());
}

/// Alice reaches the asynchronous flush before Bob sends his write. Completion
/// of Alice's flush cannot acknowledge Bob's later upload, even on a nested tick.
#[test]
fn host_flush_captures_only_preceding_uploads() {
    use std::{cell::Cell, rc::Rc, task::Poll};
    let owner = binding_owner();
    let mut a = peer(&owner.db);
    let mut b = peer(&owner.db);
    send(a.as_mut(), &upload(1, "first"));
    let ready = Rc::new(Cell::new(false));
    let flush_ready = ready.clone();
    *owner.hooks.flush_owner.borrow_mut() = Some(Box::new(move || {
        let ready = flush_ready.clone();
        Box::pin(std::future::poll_fn(move |_| {
            if ready.get() {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }))
    }));
    let mut first_tick = pin!(owner.tick());
    let mut context = Context::from_waker(Waker::noop());
    assert!(first_tick.as_mut().poll(&mut context).is_pending());
    assert!(acks(a.as_mut()).is_empty());
    send(b.as_mut(), &upload(2, "second"));
    tick(&owner.db);
    ready.set(true);
    assert!(matches!(
        first_tick.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(acks(a.as_mut()).len(), 1);
    assert!(acks(b.as_mut()).is_empty());
    block_on(owner.tick()).unwrap();
    assert_eq!(acks(b.as_mut()).len(), 1);
}

/// Alice retires the owner while Bob's upload flush is in flight. A successful
/// old flush cannot publish a receipt after that owner loses its storage lease.
#[test]
fn retirement_during_host_flush_fences_the_receipt() {
    use std::{cell::Cell, rc::Rc, task::Poll};
    let owner = binding_owner();
    let mut a = peer(&owner.db);
    send(a.as_mut(), &upload(1, "retire"));
    let ready = Rc::new(Cell::new(false));
    let flush_ready = ready.clone();
    *owner.hooks.flush_owner.borrow_mut() = Some(Box::new(move || {
        let ready = flush_ready.clone();
        Box::pin(std::future::poll_fn(move |_| {
            if ready.get() {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }))
    }));
    let mut first_tick = pin!(owner.tick());
    let mut context = Context::from_waker(Waker::noop());
    assert!(first_tick.as_mut().poll(&mut context).is_pending());
    owner.db.retire_storage_owner().unwrap();
    ready.set(true);
    assert!(matches!(
        first_tick.as_mut().poll(&mut context),
        Poll::Ready(Err(_))
    ));
    assert!(acks(a.as_mut()).is_empty());
}

/// Alice's deferred host has no durable flush implementation. Bob's upload may
/// be retained in memory, but a missing hook must fail without claiming Local.
#[test]
fn a_missing_host_flush_hook_cannot_acknowledge_storage() {
    let owner = binding_owner();
    let mut a = peer(&owner.db);
    send(a.as_mut(), &upload(1, "no hook"));
    assert!(block_on(owner.tick()).is_err());
    assert!(acks(a.as_mut()).is_empty());
}

/// Alice reconnects after losing Bob's first local receipt. The same immutable
/// journal unit is acknowledged again on the new session without another flush.
#[test]
fn reconnect_replays_a_confirmed_local_receipt() {
    let owner = offline_owner();
    let messages = upload(1, "reconnect");
    let mut a = peer(&owner);
    send(a.as_mut(), &messages);
    tick(&owner);
    owner
        .confirm_owner_durability(owner.prepare_owner_durability().unwrap().unwrap())
        .unwrap();
    owner.flush_owner_durability().unwrap();
    let first = acks(a.as_mut());
    assert_eq!(first.len(), 1);
    drop(a);
    let mut replacement = peer(&owner);
    send(replacement.as_mut(), &messages);
    tick(&owner);
    assert_eq!(acks(replacement.as_mut()), first);
    assert!(owner.prepare_owner_durability().unwrap().is_none());
}

/// Alice's versioned owner receipt has fixed bytes, including transaction
/// varint boundaries. Direct encoding is necessary to freeze the wire contract.
#[test]
fn owner_receipts_have_frozen_versioned_bytes() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v2/owner-stored.json")).unwrap();
    assert_eq!(fixture["version"], 2);
    assert_eq!(fixture["role"], "jazz.sqlite.sync.owner-stored.v1");
    for case in fixture["cases"].as_array().unwrap() {
        let message = SyncMessage::OwnerStoredV1 {
            node: [17; 16],
            tx: case["tx"].as_u64().unwrap(),
        };
        let bytes = hex::decode(case["bytes"].as_str().unwrap()).unwrap();
        assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
        let decoded = jazz::db::decode_message(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }
}

/// Alice's private foreground cannot use Bob's ordinary-worker Stored message
/// as a durability receipt. Only its owner's completed backing-store barrier
/// makes the original write Local; neither receipt makes it Global.
#[test]
fn ordinary_stored_cannot_substitute_for_a_private_owner_barrier() {
    let owner = offline_owner();
    let tab = open(1);
    tab.set_private_owner_client().unwrap();
    let (up, mut to_tab) = duplex_transport::duplex();
    let (mut to_owner, down) = duplex_transport::duplex();
    block_on(tab.connect_upstream(up));
    owner.accept_subscriber(down, alice());
    let pump = |to_tab: &mut Box<dyn Transport>, to_owner: &mut Box<dyn Transport>| {
        for _ in 0..12 {
            tick(&tab);
            while let Some(message) = to_tab.try_recv() {
                to_owner.send(message).unwrap();
            }
            tick(&owner);
            while let Some(message) = to_owner.try_recv() {
                to_tab.send(message).unwrap();
            }
        }
        tick(&tab);
    };
    pump(&mut to_tab, &mut to_owner);
    let tx = insert(&tab, "Alice's pending note");
    pump(&mut to_tab, &mut to_owner);
    assert_eq!(
        tab.write_state(tx).unwrap().durability,
        DurabilityTier::None
    );
    to_tab.send(SyncMessage::Stored { tx }).unwrap();
    tick(&tab);
    assert_eq!(
        tab.write_state(tx).unwrap().durability,
        DurabilityTier::None
    );
    pending(tab.wait_for_transaction(tx, DurabilityTier::Local));
    owner
        .confirm_owner_durability(owner.prepare_owner_durability().unwrap().unwrap())
        .unwrap();
    owner.flush_owner_durability().unwrap();
    pump(&mut to_tab, &mut to_owner);
    assert_eq!(
        tab.write_state(tx).unwrap().durability,
        DurabilityTier::Local
    );
    assert_eq!(tab.write_state(tx).unwrap().fate, Fate::Pending);
    pending(tab.wait_for_transaction(tx, DurabilityTier::Global));
}
