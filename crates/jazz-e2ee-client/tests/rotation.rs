mod common;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use common::*;
use jazz::db::{Db, DbConfig, DbIdentity, block_on, duplex};
use jazz::e2ee::{EncryptionSettings, SpaceAddress};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::space::members::{DELIVERY_BATCH, holders};
use jazz_e2ee::{OsRandom, RandomSource};
use jazz_e2ee_client::host::bridge::JsStore;
use jazz_e2ee_client::host::runtime::{Built, Encryption};
use jazz_e2ee_client::testing::StoreWithSaveHook;
use jazz_e2ee_client::{
    AccountStatus, ClientConfig, E2eeClient, MemoryStore, PrivateStore, Report,
};

/// Alice's rotation reads all deliveries for the current epoch only.
/// A later batch reads just Alice's deliveries and cannot decide who else
/// holds that epoch. The partial ledger is internal to the rotation.
#[test]
fn rotation_reads_do_not_claim_unread_epoch_coverage() {
    let settings = EncryptionSettings::default();
    let server = secure_authority_with(&settings, None);
    let alice = account(0xa1);
    let client = open_account(deferred_memory_store(), alice, settings.clone(), None);
    let (db, _link) = session(&client, &server, alice, &settings, 1, None);
    assert_eq!(
        drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
        AccountStatus::Active
    );
    let space = project(&db, "plans");
    pump(&server, &db);
    let scope = block_on(db.table_identity("projects")).unwrap().unwrap().0;
    block_on(server.insert(
        "__e2ee_members",
        BTreeMap::from([
            ("scopeId".to_owned(), Value::Uuid(scope)),
            ("identifier".to_owned(), Value::Uuid(space)),
            (
                "accountId".to_owned(),
                Value::Uuid(alice.account_id().unwrap().0),
            ),
        ]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &db);
    drive(
        &server,
        &[&db],
        host_write(&client, &db, async || {
            db.insert("notes", note(space, SECRET), Default::default())
                .await
        }),
    )
    .unwrap();
    let address = SpaceAddress {
        scope_id: scope.to_string(),
        identifier: space.to_string(),
    };
    drive(&server, &[&db], client.rotate(&db, &address)).unwrap();

    let read = |batch| {
        let (chain, ledger) = drive(
            &server,
            &[&db],
            client.rotation_read_for_test(&db, &address, batch),
        )
        .unwrap();
        (chain.unwrap(), ledger)
    };
    let (chain, ledger) = read(None);
    assert_eq!(
        holders(&chain, &ledger, 1).unwrap(),
        BTreeSet::from([account_id(alice)])
    );
    assert_eq!(
        holders(&chain, &ledger, 0),
        Err(ProtocolError::InconsistentEvidence)
    );

    let (chain, ledger) = read(Some(&[]));
    assert_eq!(
        holders(&chain, &ledger, 1),
        Err(ProtocolError::InconsistentEvidence)
    );
}

struct FailingDeliveryIds(Rc<Cell<bool>>);

impl RandomSource for FailingDeliveryIds {
    fn fill(&mut self, destination: &mut [u8]) -> jazz_e2ee::Result<()> {
        // Fail the 16-byte delivery ID draw. The pass can still save held
        // keys, whose HPKE envelopes use 32-byte randomness.
        if destination.len() == 16 && self.0.get() {
            return Err(jazz_e2ee::Error::RandomUnavailable);
        }
        OsRandom.fill(destination)
    }
}

/// Alice rotates a space with one more member than fits in the first batch.
/// The call returns after the first batch and Alice can immediately write
/// under the new key. The background pass serves the remaining member in
/// this unwatched space. A failure on the next rotation reaches her once.
/// At depth four, the foreground and background saves share one sealing,
/// and the writing and empty batches share one verified chain.
///
/// ```text
/// alice ──successor + first batch──► authority ──accepts──► rotate returns
/// alice ──write under new key──────► authority
/// background ──remaining delivery─► authority
/// next rotation ──background failure──► alice's report listener
/// ```
#[test]
fn rotation_dispatches_the_remaining_deliveries_after_the_successor_commits() {
    let settings = EncryptionSettings::default();
    let server = secure_authority_with(&settings, None);
    let alice = account(0xa1);
    let fails = Rc::new(Cell::new(false));
    let saves = Rc::new(RefCell::new(Vec::new()));
    let store = StoreWithSaveHook::new(MemoryStore::new(), {
        let saves = saves.clone();
        move |_, doc, bytes, _| {
            if doc.starts_with("held-") {
                saves.borrow_mut().push(bytes.to_vec());
            }
            Box::pin(async { Ok(()) })
        }
    });
    let client = block_on(E2eeClient::open(ClientConfig {
        store: Rc::new(store),
        declarations: declarations_with(settings.clone()),
        account_id: account_id(alice),
        registry: jazz_e2ee::registry::Registry::builtin(),
        random: Box::new(FailingDeliveryIds(fails.clone())),
        device_account: None,
    }))
    .unwrap();
    let (db, _link) = session(&client, &server, alice, &settings, 0xa1, None);
    assert_eq!(
        drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
        AccountStatus::Active
    );
    let space = project(&db, "plans");
    pump(&server, &db);
    let scope = block_on(db.table_identity("projects")).unwrap().unwrap().0;
    let add_member = |who: AuthorSubject| {
        let id = who.account_id().unwrap().0;
        block_on(server.insert(
            "__e2ee_members",
            BTreeMap::from([
                ("scopeId".to_owned(), Value::Uuid(scope)),
                ("identifier".to_owned(), Value::Uuid(space)),
                ("accountId".to_owned(), Value::Uuid(id)),
            ]),
            Default::default(),
        ))
        .unwrap();
        id
    };
    add_member(alice);
    pump(&server, &db);
    drive(
        &server,
        &[&db],
        host_write(&client, &db, async || {
            db.insert("notes", note(space, SECRET), Default::default())
                .await
        }),
    )
    .unwrap();
    pump(&server, &db);
    drive(&server, &[&db], client.reconcile(&db)).unwrap();
    client.advance_settle_clock(std::time::Duration::ZERO);
    let address = SpaceAddress {
        scope_id: scope.to_string(),
        identifier: space.to_string(),
    };
    for _ in 0..3 {
        assert_eq!(
            drive(&server, &[&db], client.rotate(&db, &address)).unwrap(),
            0
        );
    }
    saves.borrow_mut().clear();

    let mut members = BTreeSet::new();
    let schema = secure_schema_with(&settings);
    for node in 1..=u8::try_from(DELIVERY_BATCH + 1).unwrap() {
        let member = account(node);
        let runtime = open_account(Rc::new(MemoryStore::new()), member, settings.clone(), None);
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: member,
        };
        let peer = block_on(Db::open(
            DbConfig::new(schema.clone(), identity).with_e2ee(runtime.hooks()),
        ))
        .unwrap();
        let (up, down) = duplex();
        let _link = block_on(peer.connect_upstream(up));
        let subscriber = block_on(server.accept_subscriber(down, member)).unwrap();
        pump(&server, &peer);
        assert!(block_on(runtime.prepare(&peer)).unwrap());
        assert_eq!(
            drive(&server, &[&peer], runtime.ensure_account(&peer)).unwrap(),
            AccountStatus::Active
        );
        server.disconnect_subscriber(subscriber);
        members.insert(add_member(member));
    }
    pump(&server, &db);
    let written = drive(&server, &[&db], client.rotate(&db, &address)).unwrap();
    assert_eq!(written, DELIVERY_BATCH);

    let successors = all(
        &server,
        Query::from("__e2ee_space_successors")
            .filter(eq(col("identifier"), lit(Value::Uuid(space)))),
    )
    .unwrap();
    assert_eq!(successors.len(), 4);
    let successor = successors.iter().max_by_key(|row| row.seq()).unwrap();
    let epoch = successor.cell("epochId").unwrap();
    let rotation_at = successor.seq();
    assert!(rotation_at.accepted());
    let deliveries = || {
        all(
            &server,
            Query::from("__e2ee_space_deliveries")
                .filter(eq(col("keyEpochId"), lit(epoch.clone()))),
        )
        .unwrap()
    };
    let first = deliveries();
    assert_eq!(first.len(), DELIVERY_BATCH);
    assert!(first.iter().all(|delivery| delivery.seq() == rotation_at));

    let written =
        block_on(db.insert("notes", note(space, "after rotation"), Default::default())).unwrap();
    pump(&server, &db);
    let rows = all(
        &server,
        Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(written.row_uuid().0)))),
    )
    .unwrap();
    let Some(Value::Bytes(bytes)) = rows[0].cell("body") else {
        panic!("expected a sealed note");
    };
    let sealed = jazz_e2ee::records::read_stored_record(&bytes).unwrap();
    assert_eq!(Value::Uuid(sealed.epoch_id.parse().unwrap()), epoch);

    let mut watch = block_on(client.watch_spaces(&db)).unwrap();
    let mut sync = || drive(&server, &[&db], client.sync_spaces(&db, &mut watch)).unwrap();
    let replays_before = client.delivery_chain_replays();
    assert!(matches!(
        sync().as_slice(),
        [(served, Ok(jazz_e2ee_client::SpaceKeyState::Held))] if *served == address
    ));
    let mut later = 0;
    for delivery in deliveries() {
        let Some(Value::Uuid(recipient)) = delivery.cell("recipientAccountId") else {
            panic!("a delivery has no recipient");
        };
        assert!(
            members.remove(&recipient),
            "unexpected delivery to {recipient}"
        );
        let delivered_at = delivery.seq();
        assert!(delivered_at.accepted());
        if delivered_at > rotation_at {
            later += 1;
        }
    }
    assert_eq!(later, 1);
    assert!(
        members.is_empty(),
        "members missing a delivery: {members:?}"
    );
    {
        let saved = saves.borrow();
        assert_eq!(saved.len(), 2, "both saves must use compare-and-swap");
        assert_eq!(
            (
                client.delivery_chain_replays() - replays_before,
                saved[0] == saved[1]
            ),
            (1, true),
            "expected one chain replay and reuse of the first sealing",
        );
    }
    let reports = Rc::new(RefCell::new(Vec::new()));
    client.on_report({
        let reports = reports.clone();
        move |report| {
            if let Report::DeliveryFailed { space, reason } = report {
                reports.borrow_mut().push((space.clone(), reason.clone()));
            }
        }
    });
    assert_eq!(
        drive(&server, &[&db], client.rotate(&db, &address)).unwrap(),
        DELIVERY_BATCH
    );
    fails.set(true);
    let failed = sync();
    let [(failed_space, Err(error))] = failed.as_slice() else {
        panic!("expected a background delivery failure, got {failed:?}");
    };
    assert_eq!(failed_space, &address);
    assert_eq!(*reports.borrow(), [(address, error.to_string())]);
    assert!(sync().is_empty());
    assert_eq!(reports.borrow().len(), 1);
}

/// Alice starts with a held key for an unwatched space. When she cannot
/// reach the authority for delivery, the startup sweep reports that space once.
#[test]
fn startup_reports_delivery_failure_for_an_unwatched_space() {
    let settings = EncryptionSettings::default();
    let server = secure_authority_with(&settings, None);
    let alice = account(0xa1);
    let client = Rc::new(open_account(
        Rc::new(MemoryStore::new()),
        alice,
        settings.clone(),
        None,
    ));
    let (db, _link) = session(&client, &server, alice, &settings, 0xa1, None);
    let project = project(&db, "plans");
    hold_new_space(&client, &db, project);
    let at = address(&db, project);
    // Account verification can still use the fixture's link. Only delivery
    // is refused by the client's offline state.
    client.set_online(false);
    let reports = observe_reports(&client);
    let db = Rc::new(db);
    let runtime = Encryption::new(
        Built {
            spaces: serde_json::json!({}),
            client,
            reports: Rc::default(),
        },
        Rc::new(JsStore::default()),
        db.clone(),
    );
    drive(&server, &[&db], async {
        loop {
            runtime.drive();
            let state = runtime.state();
            assert!(state["error"].is_null(), "{state}");
            if state["status"] == "active" && state["stage"] == "idle" {
                break;
            }
            futures::pending!();
        }
    });
    runtime.drive();
    runtime.take_reports();
    let reports = reports.borrow();
    let failures: Vec<_> = reports
        .iter()
        .filter(|report| matches!(report, Report::DeliveryFailed { .. }))
        .collect();
    assert!(
        matches!(failures.as_slice(), [Report::DeliveryFailed { space, reason }]
        if space.scope_id == at.scope_id && space.identifier == at.identifier
        && reason.contains("this client is offline")),
        "{failures:?}"
    );
    runtime.stop();
}

/// Alice rotates her space. Her device rewrites the held-key document with
/// the new key, keeping the records of her earlier keys byte for byte
/// rather than sealing and signing them again (sealing is randomised, so a
/// resealed record would differ).
#[test]
fn a_rotation_rewrites_only_the_new_key_record() {
    let settings = EncryptionSettings::default();
    let server = secure_authority_with(&settings, None);
    let alice = account(0xa1);
    let store = MemoryStore::new();
    let client = open_account(Rc::new(store.clone()), alice, settings.clone(), None);
    let (db, _link) = session(&client, &server, alice, &settings, 1, None);
    assert_eq!(
        drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
        AccountStatus::Active
    );
    let space = project(&db, "plans");
    pump(&server, &db);
    let scope = block_on(db.table_identity("projects")).unwrap().unwrap().0;
    block_on(server.insert(
        "__e2ee_members",
        BTreeMap::from([
            ("scopeId".to_owned(), Value::Uuid(scope)),
            ("identifier".to_owned(), Value::Uuid(space)),
            (
                "accountId".to_owned(),
                Value::Uuid(alice.account_id().unwrap().0),
            ),
        ]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &db);
    drive(
        &server,
        &[&db],
        host_write(&client, &db, async || {
            db.insert("notes", note(space, SECRET), Default::default())
                .await
        }),
    )
    .unwrap();
    let records = || {
        let mut records = Vec::new();
        for scope in store.scopes() {
            for name in block_on(store.list(&scope)).unwrap().into_iter() {
                if !name.starts_with("held-") {
                    continue;
                }
                let stored = block_on(store.load(&scope, &name)).unwrap().unwrap();
                let value: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
                records.extend(
                    value["keys"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|key| key.to_string()),
                );
            }
        }
        records
    };
    let address = SpaceAddress {
        scope_id: scope.to_string(),
        identifier: space.to_string(),
    };
    // The first rotation also publishes the creation's key, so its record
    // changes; the second changes neither earlier key.
    drive(&server, &[&db], client.rotate(&db, &address)).unwrap();
    let before = records();
    assert_eq!(before.len(), 2);
    assert!(before.iter().all(|record| !record.contains("unpublished")));

    drive(&server, &[&db], client.rotate(&db, &address)).unwrap();
    let after = records();
    assert_eq!(after.len(), 3);
    assert_eq!(after[..2], before[..], "an earlier key was sealed again");
}
