mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use common::*;
use jazz::db::{Db, DbConfig, DbIdentity, block_on, duplex};
use jazz::e2ee::{EncryptionSettings, SpaceAddress};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee::protocol::space::members::DELIVERY_BATCH;
use jazz_e2ee_client::{AccountStatus, MemoryStore};

/// Alice rotates a space with one more member than fits in the first batch.
/// The client commits the successor with the first deliveries, then sends
/// the remaining member's delivery in its own transaction.
///
/// ```text
/// alice ──successor + first batch──► authority ──accepts
/// alice ──remaining delivery──────► authority ──accepts
/// ```
#[test]
fn rotation_dispatches_the_remaining_deliveries_after_the_successor_commits() {
    let settings = EncryptionSettings::default();
    let server = secure_authority_with(&settings, None);
    let alice = account(0xa1);
    let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
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
        block_on(db.insert(
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
    let address = SpaceAddress {
        scope_id: scope.to_string(),
        identifier: space.to_string(),
    };
    let written = drive(&server, &[&db], client.rotate(&db, &address)).unwrap();

    let successors = all(
        &server,
        Query::from("__e2ee_space_successors")
            .filter(eq(col("identifier"), lit(Value::Uuid(space)))),
    )
    .unwrap();
    assert_eq!(successors.len(), 1);
    let successor = &successors[0];
    let epoch = successor.cell("epochId").unwrap();
    let rotation_at = successor.seq();
    assert!(rotation_at.accepted());
    let deliveries = all(
        &server,
        Query::from("__e2ee_space_deliveries").filter(eq(col("keyEpochId"), lit(epoch))),
    )
    .unwrap();
    let mut first = 0;
    for delivery in deliveries {
        let Some(Value::Uuid(recipient)) = delivery.cell("recipientAccountId") else {
            panic!("a delivery has no recipient");
        };
        assert!(
            members.remove(&recipient),
            "unexpected delivery to {recipient}"
        );
        let delivered_at = delivery.seq();
        assert!(delivered_at.accepted());
        if delivered_at == rotation_at {
            first += 1;
        } else {
            assert!(delivered_at > rotation_at);
        }
    }
    assert_eq!(first, DELIVERY_BATCH);
    assert!(
        members.is_empty(),
        "members missing a delivery: {members:?}"
    );
    assert_eq!(written, DELIVERY_BATCH + 1);
}
