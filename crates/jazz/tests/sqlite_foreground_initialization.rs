//! Foreground seals reserve durable identities before host journal publication.
mod common;
use jazz::db::{
    Db, DbConfig, DbIdentity, InitializationTransactionStatus, ReadOpts, RemoteLinkHint, block_on,
    duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
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
fn open(node: u8, path: Option<&Path>) -> Db {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    );
    match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap()
}
fn owner(path: Option<&Path>) -> Db {
    let db = open(50, path);
    db.set_remote_link_hint(RemoteLinkHint::Failed);
    db.set_scope_isolated_storage_owner().unwrap();
    db.require_owner_durability_flush();
    db
}
fn foreground(node: u8, owner: &Db) -> Db {
    let db = open(node, None);
    db.set_private_owner_client().unwrap();
    connect(&db, owner);
    turns(&[&db, owner]);
    block_on(db.wait_for_storage_owner()).unwrap();
    db
}
fn connect(a: &Db, b: &Db) {
    let (up, down) = duplex();
    block_on(a.connect_upstream(up));
    b.accept_subscriber(down, alice());
}
fn turns(dbs: &[&Db]) {
    for _ in 0..12 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}
fn flush(owner: &Db) {
    let barrier = owner
        .prepare_owner_durability()
        .unwrap()
        .expect("owner has storage work");
    owner.confirm_owner_durability(barrier).unwrap();
    owner.flush_owner_durability().unwrap();
}
fn stage(db: &Db, n: u8) -> OpenTransactionId {
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable(id)).unwrap();
    db.prepare_initialization_insert(id, "notes", RowUuid::from_bytes([n; 16]))
        .unwrap();
    block_on(db.tx_ref(id).insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String("founder".into()))]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid::from_bytes([n; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
    id
}
fn rows(db: &Db) -> usize {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    block_on(db.all(&query, ReadOpts::default())).unwrap().len()
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return result;
        }
        turns(dbs)
    }
    panic!("operation did not settle")
}

/// Alice journals a reservation only after the private owner's flush. Publishing
/// survives owner/tab restart as Pending/Local, then Bob's authority decides it.
#[test]
fn foreground_initialization_waits_for_storage_and_recovers_original_fate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let storage = owner(Some(&path));
    let tab = foreground(1, &storage);
    let open_id = stage(&tab, 1);
    let mut sealing = Box::pin(tab.seal_initialization_transaction_with_owner(open_id));
    assert!(
        sealing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&[&tab, &storage]);
    assert!(
        sealing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(rows(&tab), 0);
    assert_eq!(rows(&storage), 0);
    flush(&storage);
    let seal = drive(&[&tab, &storage], sealing).unwrap();
    let reserved = seal.reservation();
    assert_eq!(
        storage
            .initialization_transaction_status(&[reserved])
            .unwrap(),
        [InitializationTransactionStatus::NotObserved]
    );
    let receipt = tab.publish_initialization_transaction(seal).unwrap().0;
    assert_eq!(
        tab.committed_transaction_id(receipt).unwrap(),
        reserved.transaction_id()
    );
    turns(&[&tab, &storage]);
    let mut local = Box::pin(tab.wait_for_transaction(receipt, DurabilityTier::Local));
    assert!(
        local
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    flush(&storage);
    drive(&[&tab, &storage], local).unwrap();
    let statuses = storage
        .initialization_transaction_status(&[reserved])
        .unwrap();
    let InitializationTransactionStatus::Complete(state) = &statuses[0] else {
        panic!("{statuses:?}")
    };
    assert_eq!(state.fate, Fate::Pending);
    assert_eq!(state.durability, DurabilityTier::Local);
    block_on(storage.close()).unwrap();
    drop(storage);
    drop(tab);
    let storage = owner(Some(&path));
    assert_eq!(
        storage
            .initialization_transaction_status(&[reserved])
            .unwrap(),
        statuses
    );
    let fresh_tab = foreground(2, &storage);
    if storage.prepare_owner_durability().unwrap().is_some() {
        flush(&storage);
    }
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let read = drive(
        &[&fresh_tab, &storage],
        fresh_tab.all_serialized_query_as(
            &query,
            ReadOpts::for_read_tier(jazz::db::ReadTier::LocalFirst),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap();
    let jazz::db::SerializedReadResult::Rows(found) = read else {
        panic!("expected rows")
    };
    assert_eq!(
        found.len(),
        1,
        "a fresh tab recovers the pending initialization"
    );
    drop(fresh_tab);
    let server = open(90, None);
    server.set_authority();
    connect(&storage, &server);
    turns(&[&storage, &server]);
    let statuses = storage
        .initialization_transaction_status(&[reserved])
        .unwrap();
    let InitializationTransactionStatus::Complete(state) = &statuses[0] else {
        panic!("{statuses:?}")
    };
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.durability, DurabilityTier::Global);
    assert_eq!(rows(&server), 1);
    block_on(storage.close()).unwrap();
    drop(storage);
    let storage = owner(Some(&path));
    assert_eq!(
        storage
            .initialization_transaction_status(&[reserved])
            .unwrap(),
        statuses
    );
}

/// Alice cancels a seal, restarts the worker and even reuses the foreground node.
/// The next reservation must not reuse the old journal identity.
#[test]
fn cancelled_reservations_are_never_reused_after_owner_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let reserved = {
        let storage = owner(Some(&path));
        let tab = foreground(1, &storage);
        let open_id = stage(&tab, 1);
        let mut sealing = Box::pin(tab.seal_initialization_transaction_with_owner(open_id));
        assert!(
            sealing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        turns(&[&tab, &storage]);
        flush(&storage);
        let seal = drive(&[&tab, &storage], sealing).unwrap();
        let reserved = seal.reservation();
        tab.cancel_initialization_transaction(seal).unwrap();
        assert_eq!(rows(&tab), 0);
        assert_eq!(rows(&storage), 0);
        block_on(storage.close()).unwrap();
        reserved
    };
    let storage = owner(Some(&path));
    let tab = foreground(1, &storage);
    let open_id = stage(&tab, 1);
    let mut sealing = Box::pin(tab.seal_initialization_transaction_with_owner(open_id));
    assert!(
        sealing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&[&tab, &storage]);
    flush(&storage);
    let seal = drive(&[&tab, &storage], sealing).unwrap();
    assert!(seal.reservation().transaction_id().time.0 > reserved.transaction_id().time.0);
    assert_eq!(
        storage
            .initialization_transaction_status(&[reserved])
            .unwrap(),
        [InitializationTransactionStatus::NotObserved]
    );
    tab.cancel_initialization_transaction(seal).unwrap();
}

/// Alice's first reservation is in a captured flush; her second tab's request is
/// not. Completing the first barrier cannot release the later seal.
#[test]
fn reservation_barrier_covers_only_the_requests_it_captured() {
    let storage = owner(None);
    let alice_tab = foreground(1, &storage);
    let bob_tab = foreground(2, &storage);
    let mut first =
        Box::pin(alice_tab.seal_initialization_transaction_with_owner(stage(&alice_tab, 1)));
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&[&alice_tab, &storage]);
    let barrier = storage.prepare_owner_durability().unwrap().unwrap();
    let mut second =
        Box::pin(bob_tab.seal_initialization_transaction_with_owner(stage(&bob_tab, 2)));
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&[&bob_tab, &storage]);
    storage.confirm_owner_durability(barrier).unwrap();
    storage.flush_owner_durability().unwrap();
    let first = drive(&[&alice_tab, &storage], first).unwrap();
    turns(&[&bob_tab, &storage]);
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    flush(&storage);
    let second = drive(&[&bob_tab, &storage], second).unwrap();
    assert_ne!(first.reservation(), second.reservation());
    alice_tab.cancel_initialization_transaction(first).unwrap();
    bob_tab.cancel_initialization_transaction(second).unwrap();
    assert_eq!(rows(&storage), 0);
}

struct Trap {
    wire: std::rc::Rc<std::cell::RefCell<Box<dyn jazz::db::Transport>>>,
    captured: std::rc::Rc<std::cell::RefCell<Vec<jazz::db::SyncMessage>>>,
}

/// Alice cannot give an ordinary write an identity reserved by her worker while
/// its reply is delayed. Cancelling and reconnecting still learns that floor
/// before ordinary publication resumes; the cancelled batch publishes nothing.
#[test]
fn pending_or_cancelled_reservations_cannot_alias_ordinary_writes() {
    for cancel in [false, true] {
        let storage = owner(None);
        // Retain an earlier reservation under the same node so the worker's
        // allocator floor is higher than the restarted foreground's floor.
        let previous_tab = foreground(1, &storage);
        let mut first = Box::pin(
            previous_tab.seal_initialization_transaction_with_owner(stage(&previous_tab, 1)),
        );
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        turns(&[&previous_tab, &storage]);
        flush(&storage);
        let first = drive(&[&previous_tab, &storage], first).unwrap();
        let previous_id = first.reservation().transaction_id();
        previous_tab
            .cancel_initialization_transaction(first)
            .unwrap();
        drop(previous_tab);

        let tab = foreground(1, &storage);
        let mut sealing = Box::pin(tab.seal_initialization_transaction_with_owner(stage(&tab, 2)));
        assert!(
            sealing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        turns(&[&tab, &storage]);
        let ordinary = || {
            block_on(tab.insert(
                "notes",
                BTreeMap::from([("text".into(), Value::String("ordinary".into()))]),
                Default::default(),
            ))
        };
        assert_eq!(ordinary().unwrap_err().code, jazz::db::ErrorCode::Query);
        if cancel {
            drop(sealing);
            connect(&tab, &storage);
            turns(&[&tab, &storage]);
            assert_eq!(ordinary().unwrap_err().code, jazz::db::ErrorCode::Query);
            flush(&storage);
            turns(&[&tab, &storage]);
        } else {
            flush(&storage);
            let seal = drive(&[&tab, &storage], sealing).unwrap();
            assert!(seal.reservation().transaction_id().time > previous_id.time);
            tab.cancel_initialization_transaction(seal).unwrap();
        }
        let receipt = ordinary().unwrap();
        let origin = tab
            .committed_transaction_id(receipt.mergeable_tx_id())
            .unwrap();
        assert!(origin.time.0 > previous_id.time.0 + 1);
        turns(&[&tab, &storage]);
        flush(&storage);
        drive(
            &[&tab, &storage],
            tab.wait_for_transaction(receipt.mergeable_tx_id(), DurabilityTier::Local),
        )
        .unwrap();
        assert_eq!(rows(&storage), 1);
    }
}
impl jazz::db::Transport for Trap {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        if matches!(
            message,
            jazz::db::SyncMessage::OwnerInitializationPublishV1 { .. }
        ) {
            self.captured.borrow_mut().push(message);
            Ok(())
        } else {
            self.wire.borrow_mut().send(message)
        }
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        self.wire.borrow_mut().try_recv()
    }
}

/// Mallory knows Alice's public journal ID but cannot publish it without the
/// reservation capability or substitute another author or transaction.
#[test]
fn owner_rejects_forged_initialization_publications() {
    use jazz::db::SyncMessage;
    use std::{cell::RefCell, rc::Rc};
    for case in ["nonce", "ordinary", "mergeable", "author", "transaction"] {
        let storage = owner(None);
        let tab = open(1, None);
        tab.set_private_owner_client().unwrap();
        let (up, down) = duplex();
        let wire = Rc::new(RefCell::new(up));
        let captured = Rc::new(RefCell::new(Vec::new()));
        block_on(tab.connect_upstream(Box::new(Trap {
            wire: wire.clone(),
            captured: captured.clone(),
        })));
        storage.accept_subscriber(down, alice());
        turns(&[&tab, &storage]);
        let mut sealing = Box::pin(tab.seal_initialization_transaction_with_owner(stage(&tab, 1)));
        assert!(
            sealing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        turns(&[&tab, &storage]);
        flush(&storage);
        let seal = drive(&[&tab, &storage], sealing).unwrap();
        let reserved = seal.reservation();
        tab.publish_initialization_transaction(seal).unwrap();
        turns(&[&tab, &storage]);
        let SyncMessage::OwnerInitializationPublishV1 { mut nonce, commit } =
            captured.borrow_mut().pop().expect("publication")
        else {
            unreachable!()
        };
        let mut commit: SyncMessage = postcard::from_bytes(&commit).unwrap();
        let SyncMessage::Commit {
            kind, author, tx, ..
        } = &mut commit
        else {
            unreachable!()
        };
        match case {
            "nonce" => nonce = [0x77; 16],
            "mergeable" => *kind = postcard::from_bytes(&[0]).unwrap(),
            "author" => {
                *author = Some(
                    AuthorSubject::authenticated("https://issuer.example", "mallory")
                        .unwrap()
                        .canonical()
                        .into(),
                )
            }
            "transaction" => *tx += 1,
            "ordinary" => {}
            _ => unreachable!(),
        }
        let message = if case == "ordinary" {
            commit
        } else {
            SyncMessage::OwnerInitializationPublishV1 {
                nonce,
                commit: postcard::to_allocvec(&commit).unwrap(),
            }
        };
        wire.borrow_mut().send(message).unwrap();
        for _ in 0..12 {
            let _ = block_on(storage.tick());
        }
        assert_eq!(rows(&storage), 0, "{case}");
        assert_eq!(
            storage
                .initialization_transaction_status(&[reserved])
                .unwrap(),
            [InitializationTransactionStatus::NotObserved],
            "{case}"
        );
    }
}

/// Alice's published initialization cannot become "not observed" if its retained
/// unit disappears. Physical corruption is injected only to exercise recovery.
#[test]
fn missing_published_unit_is_incomplete_and_cannot_escape_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let reserved = {
        let storage = owner(Some(&path));
        let tab = foreground(1, &storage);
        let mut sealing = Box::pin(tab.seal_initialization_transaction_with_owner(stage(&tab, 1)));
        assert!(
            sealing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        turns(&[&tab, &storage]);
        flush(&storage);
        let seal = drive(&[&tab, &storage], sealing).unwrap();
        let reserved = seal.reservation();
        tab.publish_initialization_transaction(seal).unwrap();
        turns(&[&tab, &storage]);
        flush(&storage);
        block_on(storage.close()).unwrap();
        reserved
    };
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute("DELETE FROM _jazz_relay_upload_v1", [])
        .unwrap();
    drop(sql);
    let storage = owner(Some(&path));
    assert_eq!(
        storage
            .initialization_transaction_status(&[reserved])
            .unwrap(),
        [InitializationTransactionStatus::Incomplete]
    );
    let (up, mut authority) = duplex();
    block_on(storage.connect_upstream(up));
    assert!(block_on(storage.tick()).is_err());
    assert!(
        !std::iter::from_fn(|| authority.try_recv())
            .any(|m| matches!(m, jazz::db::SyncMessage::Commit { .. }))
    );
}

/// Mallory adds a claim to Alice's reserved upload. The private owner uses the
/// authenticated connection claims; Bob's authority rejects the attempted grant
/// and that real rejection survives the owner's restart.
#[test]
fn supplied_claims_cannot_grant_initialization_permission() {
    use jazz::db::SyncMessage;
    use std::{cell::RefCell, rc::Rc};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let storage = owner(Some(&path));
    let tab = open(1, None);
    tab.set_private_owner_client().unwrap();
    let (up, down) = duplex();
    let wire = Rc::new(RefCell::new(up));
    let captured = Rc::new(RefCell::new(Vec::new()));
    block_on(tab.connect_upstream(Box::new(Trap {
        wire: wire.clone(),
        captured: captured.clone(),
    })));
    storage.accept_subscriber(down, alice());
    turns(&[&tab, &storage]);
    let mut sealing = Box::pin(tab.seal_initialization_transaction_with_owner(stage(&tab, 1)));
    assert!(
        sealing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&[&tab, &storage]);
    flush(&storage);
    let seal = drive(&[&tab, &storage], sealing).unwrap();
    let reserved = seal.reservation();
    tab.publish_initialization_transaction(seal).unwrap();
    turns(&[&tab, &storage]);
    let SyncMessage::OwnerInitializationPublishV1 { nonce, commit } =
        captured.borrow_mut().pop().unwrap()
    else {
        unreachable!()
    };
    let mut commit: SyncMessage = postcard::from_bytes(&commit).unwrap();
    let SyncMessage::Commit { session, .. } = &mut commit else {
        unreachable!()
    };
    *session = Some(BTreeMap::from([(
        jazz::query::provider_claim_key("text"),
        Value::String("founder".into()),
    )]));
    wire.borrow_mut()
        .send(SyncMessage::OwnerInitializationPublishV1 {
            nonce,
            commit: postcard::to_allocvec(&commit).unwrap(),
        })
        .unwrap();
    turns(&[&storage]);
    flush(&storage);
    let server = open(90, None);
    server.set_authority();
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        jazz::tools::TablePolicies::new()
                            .with_select(jazz::tools::PolicyExpr::True)
                            .with_insert(jazz::tools::PolicyExpr::eq_session(
                                "text",
                                vec!["claims".into(), "text".into()],
                            )),
                    ),
            )
            .build(),
    );
    block_on(server.activate_schema(schema)).unwrap();
    connect(&storage, &server);
    turns(&[&storage, &server]);
    let statuses = storage
        .initialization_transaction_status(&[reserved])
        .unwrap();
    let InitializationTransactionStatus::Complete(state) = &statuses[0] else {
        panic!("{statuses:?}")
    };
    assert!(matches!(state.fate, Fate::Rejected(_)));
    assert_eq!(rows(&server), 0);
    block_on(storage.close()).unwrap();
    drop(storage);
    let storage = owner(Some(&path));
    assert_eq!(
        storage
            .initialization_transaction_status(&[reserved])
            .unwrap(),
        statuses
    );
}

/// Alice closes the foreground binding while its worker flush is pending. A
/// later durability acknowledgement cannot return a usable seal to that owner.
#[test]
fn closing_the_binding_retires_a_seal_waiting_for_owner_storage() {
    let storage = owner(None);
    let tab = foreground(1, &storage);
    let binding = jazz::binding_host::Binding::new(
        tab,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: alice(),
        },
        false,
    );
    let open_id = stage(&binding.db, 1).to_string();
    let mut sealing = Box::pin(binding.seal_initialization_transaction_with_owner(&open_id));
    assert!(
        sealing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&[&binding.db, &storage]);
    binding.close_initializations();
    flush(&storage);
    assert!(drive(&[&binding.db, &storage], sealing).is_err());
    assert_eq!(rows(&storage), 0);
    assert_eq!(rows(&binding.db), 0);
}
