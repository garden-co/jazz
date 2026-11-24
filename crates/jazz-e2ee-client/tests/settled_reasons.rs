mod common;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use common::*;
use jazz::db::{Db, SerializedReadResult, SubscriptionEvent, block_on};
use jazz::e2ee::{EncryptionSettings, UnreadableCode};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee_client::{AccountStatus, E2eeClient, MemoryStore, Report, SpaceKeyState};

const MEMBERS: &str = "__e2ee_members";

fn active(server: &Db, who: AuthorSubject, node: u8) -> (E2eeClient, Db, jazz::db::Upstream) {
    let settings = EncryptionSettings::default();
    let client = open_account(Rc::new(MemoryStore::new()), who, settings.clone(), None);
    let (db, link) = session(&client, server, who, &settings, node, None);
    assert_eq!(
        drive(server, &[&db], client.ensure_account(&db)).unwrap(),
        AccountStatus::Active
    );
    (client, db, link)
}

fn add_member(server: &Db, db: &Db, space: uuid::Uuid, who: AuthorSubject) -> RowUuid {
    let scope = block_on(db.table_identity("projects")).unwrap().unwrap().0;
    let cells = BTreeMap::from([
        ("scopeId".to_owned(), Value::Uuid(scope)),
        ("identifier".to_owned(), Value::Uuid(space)),
        (
            "accountId".to_owned(),
            Value::Uuid(who.account_id().unwrap().0),
        ),
    ]);
    let member = block_on(db.insert(MEMBERS, cells, Default::default())).unwrap();
    pump(server, db);
    member.row_uuid()
}

fn notes(space: uuid::Uuid) -> Query {
    Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))))
}

/// Bob first reads Alice's sealed row before he knows why the key is missing.
/// Settling the space as a member with no root tells Bob's open subscription
/// `space-pending` for that row and leaves its readable rows alone. His next
/// read reports the same reason.
///
/// ```text
/// alice ──sealed note, no root──► server ◄── bob: key-pending
///                                      ◄── settle: reason-only delta to space-pending
/// ```
#[test]
fn a_read_names_the_settled_reason_for_a_row_with_no_space_root() {
    let server = secure_authority_with(&EncryptionSettings::default(), None);
    let alice = account(0xa1);
    let bob = account(0xb0);
    let (writer, alice_db, _alice_link) = active(&server, alice, 1);
    let (reader, bob_db, _bob_link) = active(&server, bob, 2);
    let space = project(&alice_db, "plans");
    pump(&server, &alice_db);
    add_member(&server, &alice_db, space, alice);
    add_member(&server, &alice_db, space, bob);
    let key = space_key_of_account(&writer, &alice_db, space);
    writer.record_fixture_rotation_check(key.address());
    block_on(writer.keep(key)).unwrap();
    let row = block_on(alice_db.insert("notes", note(space, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    pump(&server, &alice_db);
    assert_eq!(count(&server, "notes"), 1);
    assert_eq!(count(&server, "__e2ee_spaces"), 0);

    let query = notes(space);
    let prepared = bob_db.prepare_query(&query).unwrap();
    let mut live = block_on(bob_db.subscribe(&prepared, Default::default())).unwrap();
    let unrelated = bob_db.prepare_query(&notes(uuid::Uuid::now_v7())).unwrap();
    let mut unrelated = block_on(bob_db.subscribe(&unrelated, Default::default())).unwrap();
    pump(&server, &bob_db);
    let initial = std::iter::from_fn(|| live.try_next_event()).last().unwrap();
    let SubscriptionEvent::Delta {
        unreadable: Some(unreadable),
        ..
    } = initial
    else {
        panic!("{initial:?}");
    };
    assert_eq!(unreadable.len(), 1);
    assert_eq!(unreadable[0].id, row);
    assert_eq!(unreadable[0].code, "key-pending");
    while unrelated.try_next_event().is_some() {}
    let query = postcard::to_allocvec(&query).unwrap();
    let read = || {
        let (result, unreadable) =
            block_on(bob_db.all_serialized_query_reporting(&query, local(), None, None, None))
                .unwrap();
        assert!(matches!(result, SerializedReadResult::Rows(rows) if rows.is_empty()));
        assert_eq!(unreadable.len(), 1, "{unreadable:?}");
        assert_eq!(unreadable[0].id, row);
        unreadable[0].code
    };
    assert_eq!(read(), "key-pending");
    assert!(!drive(&server, &[&bob_db], reader.settle_reads(&bob_db)).unwrap());
    let delta = live
        .try_next_event()
        .expect("settlement changes the subscribed reason");
    let SubscriptionEvent::Delta {
        reset: false,
        added,
        updated,
        removed,
        unreadable: Some(unreadable),
        ..
    } = delta
    else {
        panic!("{delta:?}");
    };
    assert!(added.is_empty() && updated.is_empty() && removed.is_empty());
    assert_eq!(unreadable.len(), 1);
    assert_eq!(unreadable[0].id, row);
    assert_eq!(unreadable[0].code, "space-pending");
    assert!(unrelated.try_next_event().is_none());
    assert_eq!(read(), "space-pending");
}

/// Bob holds Alice's first key but lacks the one she rotated to while he
/// was away. Executing his equality query reports the space once while its
/// matches are partial. A non-member gets `key-unavailable` for the same
/// partial results. A delivery after rejoining makes the query complete again.
///
/// ```text
/// alice ──first note and key──► bob
/// alice ──rotate without bob──► second matching note
/// bob ──rejoin, acquire──► learns the new epoch without its key
/// bob ──leave, query──► first note, key-unavailable
/// bob ──rejoin, query──► first note, key-pending
/// alice ──deliver──► bob queries both notes, report withdrawn
/// ```
#[test]
fn executing_an_equality_query_reports_the_space_when_it_cannot_search_every_epoch() {
    let server = secure_authority_with(&EncryptionSettings::default(), None);
    let alice = account(0xa1);
    let bob = account(0xb0);
    let (writer, alice_db, _alice_link) = active(&server, alice, 1);
    let (reader, bob_db, _bob_link) = active(&server, bob, 2);
    let space = project(&alice_db, "plans");
    pump(&server, &alice_db);
    add_member(&server, &alice_db, space, alice);
    let bob_member = add_member(&server, &alice_db, space, bob);
    let address = jazz::e2ee::SpaceAddress {
        scope_id: address(&alice_db, space).scope_id,
        identifier: space.to_string(),
    };
    let write = || {
        let row = drive(
            &server,
            &[&alice_db],
            host_write(&writer, &alice_db, async || {
                alice_db
                    .insert("notes", note(space, SECRET), Default::default())
                    .await
            }),
        )
        .unwrap();
        pump(&server, &alice_db);
        row.row_uuid()
    };
    let deliver = || {
        drive(
            &server,
            &[&alice_db],
            deliver_for(&writer, &alice_db, &address),
        )
        .unwrap();
        pump(&server, &bob_db);
    };
    let acquire = || drive(&server, &[&bob_db], reader.acquire(&bob_db, &address)).unwrap();
    let first = write();
    deliver();
    assert_eq!(acquire(), SpaceKeyState::Held);
    block_on(server.delete(MEMBERS, bob_member, Default::default())).unwrap();
    drive(&server, &[&alice_db], writer.rotate(&alice_db, &address)).unwrap();
    let second = write();
    let bob_member = add_member(&server, &alice_db, space, bob);
    assert_eq!(acquire(), SpaceKeyState::KeyPending);
    block_on(bob_db.delete(MEMBERS, bob_member, Default::default())).unwrap();
    pump(&server, &bob_db);
    assert_eq!(count(&server, MEMBERS), 1);
    assert_eq!(acquire(), SpaceKeyState::NoRoot);

    let prepared = bob_db.prepare_query(&notes(space)).unwrap();
    let _live = block_on(bob_db.subscribe(&prepared, Default::default())).unwrap();
    pump(&server, &bob_db);
    let reports = Rc::new(RefCell::new(Vec::new()));
    let events = reports.clone();
    reader.on_report(move |report| events.borrow_mut().push(report.clone()));
    let query = notes(space).filter(eq(col("body"), lit(text(SECRET))));
    let nonmember = bob_db.prepare_query(&query).unwrap();
    let matches = |prepared| {
        let mut ids: Vec<_> = block_on(bob_db.all(prepared, local()))
            .unwrap()
            .iter()
            .map(|row| row.row_uuid())
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(matches(&nonmember), [first]);
    reader.flush_reports();
    assert!(reports.borrow().iter().any(|report| matches!(
        report,
        Report::Unreadable(u)
            if u.table == "notes" && u.space.as_ref() == Some(&address)
                && u.id.is_none() && u.column.is_none() && u.code == UnreadableCode::KeyUnavailable
    )));

    add_member(&server, &alice_db, space, bob);
    assert_eq!(acquire(), SpaceKeyState::KeyPending);
    reader.flush_reports();
    let pending_count = || {
        reports
            .borrow()
            .iter()
            .filter(|report| {
                matches!(report, Report::Unreadable(u)
                if u.table == "notes" && u.space.as_ref() == Some(&address)
                && u.id.is_none() && u.column.is_none() && u.code == UnreadableCode::KeyPending)
            })
            .count()
    };
    let partial = bob_db.prepare_query(&query).unwrap();
    reader.flush_reports();
    assert_eq!(
        pending_count(),
        0,
        "preparation does not report a partial result"
    );
    assert_eq!(matches(&partial), [first]);
    reader.flush_reports();
    assert_eq!(pending_count(), 1);
    assert_eq!(acquire(), SpaceKeyState::KeyPending);
    assert_eq!(matches(&partial), [first]);
    reader.flush_reports();
    assert_eq!(pending_count(), 1);

    deliver();
    assert_eq!(acquire(), SpaceKeyState::Held);
    let mut expected = [first, second];
    expected.sort();
    assert_eq!(matches(&bob_db.prepare_query(&query).unwrap()), expected);
    reader.flush_reports();
    assert_eq!(pending_count(), 1);
    assert!(reports.borrow().iter().any(|report| matches!(
        report,
        Report::Withdrawn { table, id: None, column: None, space, code: UnreadableCode::KeyPending }
            if table == "notes" && space == &address
    )));
}
