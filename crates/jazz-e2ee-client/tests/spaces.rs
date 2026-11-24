//! Spaces on a device against a real authority with the system tables
//! (`E2EE_PROTOCOL.md` §3.2, §3.3, §4.2, §4.3, §5.4): a space is created by
//! the first encrypted write, its keys are acquired from the rows the
//! server holds, never built by the test, and what a read cannot open is
//! settled into the reports of §4.3.

mod common;
#[allow(dead_code)]
#[path = "../../jazz-e2ee/tests/common/mechanisms.rs"]
mod mechanisms;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use common::*;
use jazz::db::{Db, block_on};
use jazz::e2ee::{EncryptionSettings, UnreadableCode};
use jazz::ids::AuthorSubject;
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::content::KeyMark;
use jazz_e2ee::protocol::error::RefusalCode;
use jazz_e2ee_client::{
    AccountStatus, ClientError, E2eeClient, MEMBER_LIMIT, MemoryStore, PrivateStore, Report,
    SpaceKeyState,
};

const SPACES: &str = "__e2ee_spaces";
const MEMBERS: &str = "__e2ee_members";

/// Runs `test` on its own thread; a hang fails after a minute, saying so.
fn within(test: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        test();
        let _ = done.send(());
    });
    match finished.recv_timeout(std::time::Duration::from_secs(60)) {
        Ok(()) => thread.join().unwrap(),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            thread.join().unwrap();
        }
        Err(_) => panic!(
            "the test did not finish in a minute: something waited for the server or a store forever"
        ),
    }
}

/// Alice's active device on `server`: her runtime and her client database.
struct Device {
    who: AuthorSubject,
    client: E2eeClient,
    db: Db,
    _link: Option<jazz::db::Upstream>,
    live: RefCell<Vec<jazz::db::SubscriptionStream>>,
}

fn device(server: &Db, who: AuthorSubject, store: MemoryStore, node: u8) -> Device {
    device_with_wire(server, who, store, node).0
}

fn device_with_wire(
    server: &Db,
    who: AuthorSubject,
    store: MemoryStore,
    node: u8,
) -> (Device, Rc<RefCell<Vec<u8>>>) {
    let settings = EncryptionSettings::default();
    let client = open_account(Rc::new(store), who, settings.clone(), None);
    let (db, link, wire) = recorded_session(&client, server, who, &settings, node, None);
    let device = Device {
        who,
        client,
        db,
        _link: Some(link),
        live: Default::default(),
    };
    (device, wire)
}

fn bob_with_failing_store(server: &Db) -> (Device, FaultyStore) {
    let settings = EncryptionSettings::default();
    let store = FaultyStore::default();
    let client = open_account(store.store(), account(0xb0), settings.clone(), None);
    let (db, link) = session(&client, server, account(0xb0), &settings, 2, None);
    let bob = Device {
        who: account(0xb0),
        client,
        db,
        _link: Some(link),
        live: Default::default(),
    };
    bob.activate(server);
    (bob, store)
}

impl Device {
    /// Verifies (and, the first time, founds) the account.
    fn activate(&self, server: &Db) {
        let status = drive(server, &[&self.db], self.client.ensure_account(&self.db));
        assert_eq!(status.unwrap(), AccountStatus::Active);
    }

    /// Starts pulling the notes into this device's replica.
    fn follow_notes(&self, server: &Db) {
        let prepared = self.db.prepare_query(&Query::from("notes")).unwrap();
        let stream = block_on(self.db.subscribe(&prepared, Default::default())).unwrap();
        self.live.borrow_mut().push(stream);
        pump(server, &self.db);
    }

    fn id(&self) -> String {
        account_id(self.who)
    }

    fn rotate(
        &self,
        server: &Db,
        address: &jazz::e2ee::SpaceAddress,
    ) -> Result<usize, ClientError> {
        drive(server, &[&self.db], self.client.rotate(&self.db, address))
    }

    fn acquire(
        &self,
        server: &Db,
        address: &jazz::e2ee::SpaceAddress,
    ) -> Result<SpaceKeyState, ClientError> {
        drive(server, &[&self.db], self.client.acquire(&self.db, address))
    }

    fn sync_spaces(
        &self,
        server: &Db,
        watch: &mut jazz_e2ee_client::SpaceWatch,
    ) -> Result<Vec<(jazz::e2ee::SpaceAddress, Result<SpaceKeyState, ClientError>)>, ClientError>
    {
        drive(
            server,
            &[&self.db],
            self.client.sync_spaces(&self.db, watch),
        )
    }
}

/// A project row and the creator's member row, accepted.
fn new_project(server: &Db, creator: &Device, name: &str) -> uuid::Uuid {
    let space = project(&creator.db, name);
    pump(server, &creator.db);
    add_member(server, creator, space, &creator.id());
    space
}

fn add_member(server: &Db, by: &Device, space: uuid::Uuid, account: &str) {
    let scope = jazz::db::block_on(by.db.table_identity("projects"))
        .unwrap()
        .unwrap()
        .0;
    let cells = BTreeMap::from([
        ("scopeId".to_owned(), Value::Uuid(scope)),
        ("identifier".to_owned(), Value::Uuid(space)),
        (
            "accountId".to_owned(),
            Value::Uuid(uuid::Uuid::parse_str(account).unwrap()),
        ),
    ]);
    block_on(by.db.insert(MEMBERS, cells, Default::default())).unwrap();
    pump(server, &by.db);
}

/// A note written through the host write path.
fn write_note(
    server: &Db,
    device: &Device,
    space: uuid::Uuid,
    body: &str,
) -> Result<jazz::db::WriteHandle, ClientError> {
    drive(
        server,
        &[&device.db],
        host_write(&device.client, &device.db, async || {
            device
                .db
                .insert("notes", note(space, body), Default::default())
                .await
        }),
    )
}

/// The notes of `space`, read through the host read path.
fn read_notes(
    server: &Db,
    device: &Device,
    space: uuid::Uuid,
) -> Result<Vec<jazz::db::CurrentRow>, ClientError> {
    let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
    drive(
        server,
        &[&device.db],
        host_read(&device.client, &device.db, async || {
            let prepared = device.db.prepare_query(&query)?;
            device.db.all(&prepared, local()).await
        }),
    )
}

fn space_address(device: &Device, space: uuid::Uuid) -> jazz::e2ee::SpaceAddress {
    let scope = jazz::db::block_on(device.db.table_identity("projects"))
        .unwrap()
        .unwrap()
        .0;
    jazz::e2ee::SpaceAddress {
        scope_id: scope.to_string(),
        identifier: space.to_string(),
    }
}

/// The marks of this device's keys of `space`.
fn marks(device: &Device, space: uuid::Uuid) -> Vec<KeyMark> {
    let address = address(&device.db, space);
    device.client.with_held(|held| {
        held.roots_of(&address)
            .into_iter()
            .flat_map(|root| root.held().map(|(_, mark)| mark.clone()))
            .collect()
    })
}

/// Contract: alice's first encrypted write to a space she can update
/// creates the space in the same transaction: the server holds one root of
/// the address and the encrypted row, the root's key is alice's held key,
/// and plaintext reaches neither the server nor the wire. Once the root is
/// accepted the key stops being `unpublished`.
///
/// ```text
/// alice ──insert note──► hooks: NeedsKey ──► runtime: no root, prepare
///       ──insert note + root (one transaction)──► server
///       ◄──accepted── reconcile: the mark is cleared
/// ```
#[test]
fn a_first_encrypted_write_creates_its_space() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, wire) = device_with_wire(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");

        wire.borrow_mut().clear();
        write_note(&server, &alice, space, SECRET).unwrap();
        assert!(matches!(
            marks(&alice, space)[..],
            [KeyMark::Unpublished { .. }]
        ));
        // A second write before the creation settles seals under the same
        // key and stages no second root.
        write_note(&server, &alice, space, "second").unwrap();
        pump(&server, &alice.db);

        assert_eq!(count(&server, SPACES), 1);
        assert_eq!(count(&server, "notes"), 2);
        let wire = wire.borrow();
        assert!(contains(&wire, SPACES.as_bytes()), "the space root went up");
        assert!(!contains(&wire, SECRET.as_bytes()), "plaintext on the wire");
        drop(wire);
        for row in all(&server, Query::from("notes")).unwrap() {
            let Some(Value::Bytes(body)) = row.cell("body") else {
                panic!("the authority must hold a sealed body");
            };
            assert!(body.starts_with(b"JE2E"));
            assert!(!contains(&body, SECRET.as_bytes()));
        }
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(marks(&alice, space), [KeyMark::Published]);
        let rows = read_notes(&server, &alice, space).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .any(|row| row.cell("body") == Some(text(SECRET)))
        );
    });
}

/// A copy of `store` without its held keys: the same device, a device that
/// lost its keys.
fn without_held_keys(store: &MemoryStore) -> MemoryStore {
    let copy = MemoryStore::new();
    for scope in store.scopes() {
        for doc in block_on(store.list(&scope)).unwrap() {
            if doc.starts_with("held-") {
                continue;
            }
            let stored = block_on(store.load(&scope, &doc)).unwrap().unwrap();
            copy.overwrite(&scope, &doc, stored.bytes);
        }
    }
    copy
}

/// A space alice created with one note, on `server`; her store.
fn alice_with_a_note(server: &Db) -> (uuid::Uuid, MemoryStore) {
    let store = MemoryStore::new();
    let alice = device(server, account(0xa1), store.clone(), 1);
    alice.activate(server);
    let space = new_project(server, &alice, "plans");
    write_note(server, &alice, space, SECRET).unwrap();
    pump(server, &alice.db);
    drive(server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
    (space, store)
}

/// Alice adds Bob after rotating each room. While he waits for a delivery,
/// each acquisition reads the current epoch and checks his older deliveries once.
/// The authority's plan must seek to Bob's deliveries in this room for that second read.
#[test]
fn a_keyless_members_delivery_queries_stay_constant_across_rotations() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let bob = device(&server, account(0xb0), MemoryStore::new(), 2);
        bob.activate(&server);
        let table = jazz::tools::deployment::artifact::system_tables::SPACE_DELIVERIES;
        let [scope, room, recipient, epoch] =
            ["scopeId", "identifier", "recipientAccountId", "keyEpochId"]
                .map(|column| server.stored_column_for_test(table, column).1);
        let fallback_seek = format!(
            "USING INDEX {table}__ix({scope},{room},{recipient},{epoch}) ({scope}=? AND {room}=? AND {recipient}=?)"
        );
        let mut counts = Vec::new();
        for rotations in [1, 6] {
            let space = new_project(&server, &alice, "plans");
            write_note(&server, &alice, space, SECRET).unwrap();
            pump(&server, &alice.db);
            let address = space_address(&alice, space);
            for _ in 0..rotations {
                alice.rotate(&server, &address).unwrap();
            }
            add_member(&server, &alice, space, &bob.id());
            bob.client.take_delivery_reads();
            server.take_slow_queries();
            server.set_slow_query_threshold(Some(std::time::Duration::ZERO));
            let state = bob.acquire(&server, &address).unwrap();
            assert_eq!(state, SpaceKeyState::KeyPending);
            server.set_slow_query_threshold(None);
            let plans: Vec<_> = server
                .take_slow_queries()
                .0
                .into_iter()
                .filter(|query| query.table == table)
                .map(|query| query.plan)
                .collect();
            assert!(
                plans.iter().any(|plan| plan.contains(&fallback_seek)),
                "{plans:#?}"
            );
            counts.push(bob.client.take_delivery_reads().2);
        }
        assert_eq!(counts, [2, 2]);
    });
}

/// Bob received the root key but had not opened it when Alice removed him
/// and rotated. Rejoining lets him recover the first note from his old
/// delivery while he still waits for a delivery of the new key.
#[test]
fn a_rejoining_member_opens_an_older_delivery_while_waiting_for_the_current_key() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        assert_eq!(deliver(&server, &alice, &[], space), 1);
        block_on(server.delete(MEMBERS, member_row(&server, &bob.id()), Default::default()))
            .unwrap();
        let address = space_address(&alice, space);
        alice.rotate(&server, &address).unwrap();
        write_note(&server, &alice, space, "while Bob was away").unwrap();
        pump(&server, &alice.db);
        server_member(&server, &alice, space, &bob.id());
        pump(&server, &bob.db);

        assert!(marks(&bob, space).is_empty());
        assert_eq!(
            bob.acquire(&server, &address).unwrap(),
            SpaceKeyState::KeyPending
        );
        assert_eq!(marks(&bob, space), [KeyMark::Published]);
        let rows = read_notes(&server, &bob, space).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    });
}

/// Contract: a device that lost its held keys gets the space's key back
/// from the rows the server holds (its own account envelope in the root),
/// whichever way it first needs it: a read that left rows out, an equality
/// query on a space it holds nothing of, or a write; the write stages no
/// second root.
///
/// ```text
/// alice (keys lost) ──read / eq query / write──► NeedsKey or missing epoch
///       ──ledger read: root, account histories──► chain, K₀ from her envelope
///       ──retry──► plaintext
/// ```
#[test]
fn a_lost_key_is_acquired_from_the_roots_own_envelope() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (space, store) = alice_with_a_note(&server);

        let alice = device(&server, account(0xa1), without_held_keys(&store), 2);
        alice.activate(&server);
        alice.follow_notes(&server);
        let rows = read_notes(&server, &alice, space).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
        assert_eq!(marks(&alice, space), [KeyMark::Published]);

        let alice = device(&server, account(0xa1), without_held_keys(&store), 3);
        alice.activate(&server);
        alice.follow_notes(&server);
        let query = Query::from("notes")
            .filter(eq(col("projectId"), lit(Value::Uuid(space))))
            .filter(eq(col("body"), lit(text(SECRET))));
        let address = space_address(&alice, space);
        assert_eq!(
            alice.acquire(&server, &address).unwrap(),
            SpaceKeyState::Held
        );
        let found = drive(
            &server,
            &[&alice.db],
            host_read(&alice.client, &alice.db, async || {
                let prepared = alice.db.prepare_query(&query)?;
                alice.db.all(&prepared, local()).await
            }),
        )
        .unwrap();
        assert_eq!(found.len(), 1);

        let alice = device(&server, account(0xa1), without_held_keys(&store), 4);
        alice.activate(&server);
        alice.follow_notes(&server);
        write_note(&server, &alice, space, "second").unwrap();
        pump(&server, &alice.db);
        assert_eq!(count(&server, SPACES), 1, "no second root");
        assert_eq!(count(&server, "notes"), 2);
    });
}

fn reports_of(device: &Device) -> Rc<RefCell<Vec<Report>>> {
    observe_reports(&device.client)
}

fn codes(seen: &Rc<RefCell<Vec<Report>>>) -> Vec<(UnreadableCode, bool)> {
    seen.borrow()
        .iter()
        .filter_map(|report| match report {
            Report::Unreadable(u) => Some((u.code, u.id.is_some())),
            _ => None,
        })
        .collect()
}

/// Contract: a member of a space whose delivery has not arrived reads no
/// encrypted rows and sees the space `key-pending` once, however often it
/// reads, and a listener registered later is told the same; an account that
/// is not a member sees each row it cannot open `key-unavailable`.
///
/// ```text
/// alice ──space, note──► server ◄──read── bob (member, no delivery): key-pending
///                               ◄──read── carol (not a member): key-unavailable
/// ```
#[test]
fn a_member_without_a_delivery_sees_key_pending_once() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (space, store) = alice_with_a_note(&server);
        let alice = device(&server, account(0xa1), store, 5);
        alice.activate(&server);
        let bob = device(&server, account(0xb0), MemoryStore::new(), 6);
        bob.activate(&server);
        add_member(&server, &alice, space, &bob.id());
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);

        let bob_seen = reports_of(&bob);
        bob.follow_notes(&server);
        assert!(read_notes(&server, &bob, space).unwrap().is_empty());
        assert!(read_notes(&server, &bob, space).unwrap().is_empty());
        assert_eq!(codes(&bob_seen), [(UnreadableCode::KeyPending, false)]);
        assert_eq!(
            codes(&reports_of(&bob)),
            [(UnreadableCode::KeyPending, false)]
        );
        assert_eq!(
            bob.acquire(&server, &space_address(&bob, space)).unwrap(),
            SpaceKeyState::KeyPending
        );

        let carol_seen = reports_of(&carol);
        carol.follow_notes(&server);
        assert!(read_notes(&server, &carol, space).unwrap().is_empty());
        // One report per encrypted column of the one row (protocol §4.3).
        assert_eq!(
            codes(&carol_seen),
            [(UnreadableCode::KeyUnavailable, true); 4]
        );
    });
}

/// Alice holds both space keys. Bob belongs only to the second space.
/// A note stored there with its original ciphertext is stale for Alice
/// and unreadable for Bob. Restoring its original space restores the read.
///
/// ```text
/// Stored space    Alice                         Bob
/// other           reads with stale-space        key-pending → invalid-ciphertext
/// original        finds and reads the note
/// ```
#[test]
fn readers_of_a_moved_note_before_and_after_its_space_is_restored() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        let bob = device(&server, account(0xb0), MemoryStore::new(), 2);
        alice.activate(&server);
        bob.activate(&server);
        let original = new_project(&server, &alice, "original");
        let other = new_project(&server, &alice, "other");
        let moved = write_note(&server, &alice, original, SECRET)
            .unwrap()
            .row_uuid();
        let control = write_note(&server, &alice, other, SECRET)
            .unwrap()
            .row_uuid();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        add_member(&server, &alice, other, &bob.id());

        // Set the fixture's stored coordinate directly so the same ciphertext
        // and equality token are read before and after restoring the coordinate.
        let set_stored_space = |space| {
            let patch = BTreeMap::from([("projectId".to_owned(), Value::Uuid(space))]);
            block_on(server.update("notes", moved, patch, Default::default())).unwrap();
        };
        set_stored_space(other);
        let alice_seen = reports_of(&alice);
        alice.follow_notes(&server);
        bob.follow_notes(&server);
        let in_space =
            |space| Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
        let matching = |space| in_space(space).filter(eq(col("body"), lit(text(SECRET))));
        let rows = read_notes(&server, &alice, other).unwrap();
        let stale = rows.iter().find(|r| r.row_uuid() == moved).unwrap();
        assert_eq!(stale.cell("body"), Some(text(SECRET)));
        let original_address = space_address(&alice, original);
        let reports = alice_seen.borrow();
        assert!(
            reports.iter().any(|report| {
                matches!(report, Report::Unreadable(u) if u.id == Some(moved)
                && u.column.as_deref() == Some("body")
                && u.space.as_ref() == Some(&original_address)
                && u.code == UnreadableCode::StaleSpace)
            }),
            "{reports:?}"
        );
        drop(reports);

        // Both notes have the same body. Only the correctly indexed note matches.
        let found = all(&alice.db, matching(other)).unwrap();
        assert_eq!(
            found.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
            [control]
        );

        let query = postcard::to_allocvec(&in_space(other)).unwrap();
        let bob_read = || {
            block_on(
                bob.db
                    .all_serialized_query_reporting(&query, local(), None, None, None),
            )
            .unwrap()
        };
        let (before, missing) = bob_read();
        assert!(matches!(before, jazz::db::SerializedReadResult::Rows(rows) if rows.is_empty()));
        assert!(
            missing
                .iter()
                .any(|r| r.id == moved && r.code == "key-pending"),
            "{missing:?}"
        );

        assert_eq!(deliver(&server, &alice, &[&bob], other), 1);
        let rows = read_notes(&server, &bob, other).unwrap();
        assert_eq!(
            rows.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
            [control]
        );
        let (_, unreadable) = bob_read();
        assert_eq!(unreadable.len(), 1, "{unreadable:?}");
        assert_eq!(unreadable[0].id, moved);
        assert_eq!(unreadable[0].code, "invalid-ciphertext");

        set_stored_space(original);
        pump(&server, &alice.db);
        let prepared = alice.db.prepare_query(&matching(original)).unwrap();
        let repaired = block_on(alice.db.all_strict(&prepared, local())).unwrap();
        assert_eq!(repaired.len(), 1);
        assert_eq!(repaired[0].row_uuid(), moved);
        assert_eq!(repaired[0].cell("body"), Some(text(SECRET)));
    });
}

/// Contract: settling what reads left out settles every space, even when
/// acquiring one fails: the others are reported, the failed space's
/// entries are kept for the next settle, and its error is returned.
///
/// ```text
/// bob reads notes of `first` (member, delivered) and `second` (not a member)
/// settle: first ──acquire: key save fails──✗ kept    second ──► key-unavailable
/// settle again (store healthy): first ──► key held
/// ```
#[test]
fn one_failed_space_does_not_drop_the_others_reports() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let mut spaces = [
            new_project(&server, &alice, "one"),
            new_project(&server, &alice, "two"),
        ];
        spaces.sort();
        let [first, second] = spaces;
        for space in spaces {
            write_note(&server, &alice, space, SECRET).unwrap();
        }
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();

        let (bob, store) = bob_with_failing_store(&server);
        add_member(&server, &alice, first, &bob.id());
        assert_eq!(deliver(&server, &alice, &[&bob], first), 1);

        let seen = reports_of(&bob);
        bob.follow_notes(&server);
        let prepared = bob.db.prepare_query(&Query::from("notes")).unwrap();
        assert!(block_on(bob.db.all(&prepared, local())).unwrap().is_empty());

        store.failing.set(true);
        let failed = drive(&server, &[&bob.db], bob.client.settle_reads(&bob.db));
        assert!(
            matches!(failed, Err(ClientError::Store(_))),
            "the failed space's error is returned: {failed:?}"
        );
        let unreadable: Vec<_> = seen
            .borrow()
            .iter()
            .filter_map(|report| match report {
                Report::Unreadable(u) => Some((u.code, u.space.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            unreadable,
            vec![
                (
                    UnreadableCode::KeyUnavailable,
                    Some(space_address(&bob, second))
                );
                4
            ],
            "the other space is settled"
        );

        store.failing.set(false);
        assert!(
            drive(&server, &[&bob.db], bob.client.settle_reads(&bob.db)).unwrap(),
            "the kept entries are settled on the next call"
        );
        assert_eq!(read_notes(&server, &bob, first).unwrap().len(), 1);
    });
}

/// Contract: a space whose acquisition keeps failing is not read at every
/// settle (the host loop settles at every turn): after its first failure it
/// is retried at once, after the next it waits, longer each time, keeping
/// its entries. A success ends the wait.
///
/// ```text
/// bob reads notes of `first` (member, delivered); the key save keeps failing
/// settle ×10 at once ──► first read twice (fail, retry, fail), then waits
/// store healthy, wait over ──► settled, key held
/// ```
#[test]
fn a_failing_space_is_not_read_at_every_settle() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let first = new_project(&server, &alice, "one");
        write_note(&server, &alice, first, SECRET).unwrap();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();

        let (bob, store) = bob_with_failing_store(&server);
        add_member(&server, &alice, first, &bob.id());
        assert_eq!(deliver(&server, &alice, &[&bob], first), 1);

        bob.follow_notes(&server);
        let prepared = bob.db.prepare_query(&Query::from("notes")).unwrap();
        assert!(block_on(bob.db.all(&prepared, local())).unwrap().is_empty());

        bob.client.advance_settle_clock(std::time::Duration::ZERO);
        store.failing.set(true);
        let before = store.saves.get();
        let settle = || drive(&server, &[&bob.db], bob.client.settle_reads(&bob.db));
        for _ in 0..10 {
            let _ = settle();
        }
        assert_eq!(
            store.saves.get() - before,
            2,
            "read once, retried once, then left to wait"
        );

        store.failing.set(false);
        bob.client
            .advance_settle_clock(std::time::Duration::from_millis(300));
        assert!(settle().unwrap(), "settled once the wait is over");
        assert_eq!(read_notes(&server, &bob, first).unwrap().len(), 1);
    });
}

/// Contract: while a space waits out its back-off, its rows are unresolved,
/// not absent: a host read of them leaves them out and reports them
/// missing without failing, their entries are kept, and a read of another
/// space neither drops them nor ends the wait.
///
/// ```text
/// bob reads notes of `first` (member, delivered); the key save keeps failing
/// read first ×2 ──► error (fail, retry, fail: now waiting)
/// read first ──► no rows, reported missing; nothing read
/// read second (not a member) ──► settled; first still waits, entries kept
/// store healthy, wait over ──► the kept entries settle, key held
/// ```
#[test]
fn a_backed_off_space_is_unresolved_not_absent() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let first = new_project(&server, &alice, "one");
        let second = new_project(&server, &alice, "two");
        for space in [first, second] {
            write_note(&server, &alice, space, SECRET).unwrap();
        }
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();

        let (bob, store) = bob_with_failing_store(&server);
        add_member(&server, &alice, first, &bob.id());
        assert_eq!(deliver(&server, &alice, &[&bob], first), 1);
        bob.follow_notes(&server);

        bob.client.advance_settle_clock(std::time::Duration::ZERO);
        let events = reports_of(&bob);
        store.failing.set(true);
        for _ in 0..2 {
            let failed = read_notes(&server, &bob, first);
            assert!(matches!(failed, Err(ClientError::Store(_))), "{failed:?}");
        }
        let saves = store.saves.get();

        let waiting = read_notes(&server, &bob, first);
        assert!(
            waiting.as_ref().is_ok_and(Vec::is_empty),
            "a backed-off space fails no read: {waiting:?}"
        );
        assert!(
            missing_key_of(&events.borrow(), &bob, first),
            "a backed-off space's rows are not silently left out"
        );

        assert!(read_notes(&server, &bob, second).unwrap().is_empty());
        let settle = || drive(&server, &[&bob.db], bob.client.settle_reads(&bob.db));
        assert!(!settle().unwrap(), "first still waits");
        assert_eq!(
            store.saves.get(),
            saves,
            "another space's read does not end first's wait"
        );

        store.failing.set(false);
        bob.client
            .advance_settle_clock(std::time::Duration::from_millis(300));
        assert!(
            settle().unwrap(),
            "the kept entries settle once the wait is over"
        );
        assert_eq!(read_notes(&server, &bob, first).unwrap().len(), 1);
    });
}

/// Whether the reports since the last look include a row of `space` left
/// out for want of a key.
fn missing_key_of(reports: &[Report], device: &Device, space: uuid::Uuid) -> bool {
    let address = space_address(device, space);
    reports
        .iter()
        .any(|report| matches!(report, Report::MissingKey { space, .. } if *space == address))
}

/// Contract: one backed-off space does not fail a host read of several:
/// the read returns the healthy spaces' rows and leaves the backed-off
/// space's rows out, reported missing and kept for a later settle, and no
/// read during the wait (of all spaces or of the other one) ends it.
///
/// ```text
/// bob is a member of `first` and `second`, both delivered
/// read second ──► key held, 1 row
/// key save fails; read first ×2 ──► error (fail, retry, fail: now waiting)
/// read all ──► second's row; first's reported missing; first not read
/// read second, read first ──► second's row; none; first still not read
/// store healthy, wait over ──► read all ──► both rows
/// ```
#[test]
fn a_backed_off_space_does_not_fail_a_read_of_the_others() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let first = new_project(&server, &alice, "one");
        let second = new_project(&server, &alice, "two");
        for space in [first, second] {
            write_note(&server, &alice, space, SECRET).unwrap();
        }
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();

        let (bob, store) = bob_with_failing_store(&server);
        for space in [first, second] {
            add_member(&server, &alice, space, &bob.id());
            assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
        }
        let events = reports_of(&bob);
        bob.follow_notes(&server);

        let healthy = space_address(&bob, second);
        assert_eq!(bob.acquire(&server, &healthy).unwrap(), SpaceKeyState::Held);
        let first_notes =
            Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(first))));
        assert!(all(&bob.db, first_notes).unwrap().is_empty());
        bob.client.advance_settle_clock(std::time::Duration::ZERO);
        store.failing.set(true);
        for _ in 0..2 {
            let failed = drive(&server, &[&bob.db], bob.client.settle_reads(&bob.db));
            assert!(matches!(failed, Err(ClientError::Store(_))), "{failed:?}");
        }
        let saves = store.saves.get();

        let read_all = || {
            drive(
                &server,
                &[&bob.db],
                host_read(&bob.client, &bob.db, async || {
                    let prepared = bob.db.prepare_query(&Query::from("notes"))?;
                    bob.db.all(&prepared, local()).await
                }),
            )
        };
        let all = read_all();
        assert_eq!(
            all.as_ref().map(Vec::len).ok(),
            Some(1),
            "the healthy space's row is returned: {all:?}"
        );
        assert!(
            missing_key_of(&events.borrow(), &bob, first),
            "the backed-off space's row is reported missing"
        );
        assert_eq!(read_notes(&server, &bob, second).unwrap().len(), 1);
        assert!(read_notes(&server, &bob, first).unwrap().is_empty());
        assert_eq!(store.saves.get(), saves, "no read during the wait ends it");

        store.failing.set(false);
        bob.client
            .advance_settle_clock(std::time::Duration::from_millis(300));
        assert_eq!(read_all().unwrap().len(), 2, "both once the wait is over");
    });
}

/// Contract: a device that enrols again after its account was reset
/// forgets what was reported to the device it was: the listener is told
/// the withdrawal of its `key-pending`, a listener registered later is
/// told nothing, and the space it waited on is no longer watched.
///
/// ```text
/// carol ──space, alice a member (no delivery)──► alice's device 1: key-pending
/// alice's device 2 ──request, reset──► server
/// device 1 ──verify──► re-enrolled: withdrawn; nothing held; nothing watched
/// ```
#[test]
fn reenrolment_forgets_the_old_devices_reports() {
    within(|| {
        let settings = EncryptionSettings {
            allow_account_reset: true,
            ..Default::default()
        };
        let server = secure_authority_with(&settings, None);
        // A device under the reset settings.
        let reset_device = |who, node| {
            let client = open_account(Rc::new(MemoryStore::new()), who, settings.clone(), None);
            let (db, link) = session(&client, &server, who, &settings, node, None);
            Device {
                who,
                client,
                db,
                _link: Some(link),
                live: Default::default(),
            }
        };
        let first = reset_device(account(0xa1), 1);
        first.activate(&server);
        let carol = reset_device(account(0xc0), 2);
        carol.activate(&server);
        let space = new_project(&server, &carol, "plans");
        add_member(&server, &carol, space, &first.id());
        write_note(&server, &carol, space, SECRET).unwrap();
        pump(&server, &carol.db);

        let seen = reports_of(&first);
        let mut watch = drive(&server, &[&first.db], first.client.watch_spaces(&first.db)).unwrap();
        first.follow_notes(&server);
        assert!(read_notes(&server, &first, space).unwrap().is_empty());
        assert_eq!(codes(&seen), [(UnreadableCode::KeyPending, false)]);

        let second = reset_device(first.who, 3);
        assert_eq!(
            drive(
                &server,
                &[&second.db],
                second.client.ensure_account(&second.db)
            )
            .unwrap(),
            AccountStatus::AwaitingApproval
        );
        assert_eq!(
            drive(
                &server,
                &[&second.db],
                second.client.reset_account(&second.db)
            )
            .unwrap(),
            AccountStatus::Active
        );
        pump(&server, &first.db);
        assert_eq!(
            drive(
                &server,
                &[&first.db],
                first.client.ensure_account(&first.db)
            )
            .unwrap(),
            AccountStatus::Reenrolled
        );
        first.client.flush_reports();
        assert!(
            seen.borrow().iter().any(|report| matches!(
                report,
                Report::Withdrawn {
                    code: UnreadableCode::KeyPending,
                    ..
                }
            )),
            "{:?}",
            seen.borrow()
        );
        assert!(reports_of(&first).borrow().is_empty());
        let synced = first.sync_spaces(&server, &mut watch).unwrap();
        assert!(synced.is_empty(), "{synced:?}");
    });
}

/// Alice's database at `path`, not linked to any server.
fn offline_db(client: &E2eeClient, who: AuthorSubject, node: u8, path: &std::path::Path) -> Db {
    let config = jazz::db::DbConfig::new(
        secure_schema_with(&EncryptionSettings::default()),
        jazz::db::DbIdentity {
            node: jazz::ids::NodeUuid::from_bytes([node; 16]),
            author: who,
        },
    )
    .with_e2ee(client.hooks());
    let db = block_on(Db::open_at(config, path, jazz::db::FileOptions::default())).unwrap();
    assert!(jazz::db::block_on(client.prepare(&db)).unwrap());
    db
}

/// Contract: a creation whose outcome is unknown when the device restarts
/// is reconciled by its root, never by staging another. The key stays
/// `unpublished` until the authority accepts the root.
///
/// ```text
/// alice ──note + root (not uploaded)──► her store      (restart)
/// alice ──reconcile: root pending──► key unpublished; no second root
/// alice ──upload──► server accepts ──reconcile──► key published, one root
/// ```
#[test]
fn an_unknown_outcome_is_reconciled_after_a_restart() {
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.sqlite");
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let who = account(0xa1);
        let store = MemoryStore::new();
        let settings = EncryptionSettings::default();
        let space = {
            let client = open_account(Rc::new(store.clone()), who, settings.clone(), None);
            let (db, _link) = session(&client, &server, who, &settings, 1, Some(&path));
            let alice = Device {
                who,
                client,
                db,
                _link: Some(_link),
                live: Default::default(),
            };
            alice.activate(&server);
            let space = new_project(&server, &alice, "plans");
            // Written offline, with no turn of the connection: the root is
            // staged, not uploaded.
            alice.client.set_online(false);
            block_on(host_write(&alice.client, &alice.db, async || {
                alice
                    .db
                    .insert("notes", note(space, SECRET), Default::default())
                    .await
            }))
            .unwrap();
            assert_eq!(count(&server, SPACES), 0);
            space
        };

        let client = open_account(Rc::new(store), who, settings.clone(), None);
        let db = offline_db(&client, who, 1, &path);
        block_on(client.reconcile(&db)).unwrap();
        let alice = Device {
            who,
            client,
            db,
            _link: None,
            live: Default::default(),
        };
        assert!(matches!(
            marks(&alice, space)[..],
            [KeyMark::Unpublished { .. }]
        ));
        // A write while the creation is pending seals under its key and
        // stages no second root.
        alice.client.set_online(false);
        block_on(host_write(&alice.client, &alice.db, async || {
            alice
                .db
                .insert("notes", note(space, "again"), Default::default())
                .await
        }))
        .unwrap();
        alice.client.set_online(true);
        let _link = link_as(&alice.db, &server, who);
        pump(&server, &alice.db);
        alice.activate(&server);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(count(&server, SPACES), 1, "one root, never a second");
        assert_eq!(marks(&alice, space), [KeyMark::Published]);
    });
}

/// The test app with `projects` that only their owner updates: a member
/// who is not the owner may not create the space (R15).
fn owned_schema() -> jazz::schema::JazzSchema {
    use jazz::tools::deployment::artifact::system_tables::{
        derive_e2ee_policies, with_e2ee_fragment,
    };
    use jazz::tools::{
        CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TableSchemaBuilder,
    };
    let owner = || PolicyExpr::Cmp {
        column: "ownerId".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["user".to_owned()]),
    };
    let builder = SchemaBuilder::new().table(
        TableSchemaBuilder::new("projects")
            .column("name", ColumnType::Text)
            .column("ownerId", ColumnType::Uuid)
            .policies(allow_all().with_update(Some(owner()), PolicyExpr::True)),
    );
    let builder = builder
        .table(
            TableSchemaBuilder::new("notes")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Text)
                .column("body", ColumnType::Bytea)
                .nullable_column("__e2ee_eq_body", ColumnType::Bytea)
                .nullable_column("__e2ee_eq_note", ColumnType::Bytea)
                .column("note", ColumnType::Bytea)
                .column("attachment", ColumnType::Bytea)
                .column("rank", ColumnType::Bytea)
                .nullable_column("plainNote", ColumnType::Text)
                .policies(allow_all()),
        )
        .table(
            TableSchemaBuilder::new("vaults")
                .column("label", ColumnType::Text)
                .column("secret", ColumnType::Bytea)
                .policies(allow_all()),
        );
    let mut schema = with_e2ee_fragment(&builder.build());
    for (table, policies) in derive_e2ee_policies(
        &schema,
        &declarations().tables,
        &EncryptionSettings::default(),
    )
    .unwrap()
    {
        schema.get_mut(&table).unwrap().policies = policies;
    }
    jazz::schema::JazzSchema::new(&schema).unwrap()
}

fn owned_device(server: &Db, who: AuthorSubject, store: MemoryStore, node: u8) -> Device {
    let client = open_account(Rc::new(store), who, EncryptionSettings::default(), None);
    let config = jazz::db::DbConfig::new(
        owned_schema(),
        jazz::db::DbIdentity {
            node: jazz::ids::NodeUuid::from_bytes([node; 16]),
            author: who,
        },
    )
    .with_e2ee(client.hooks());
    let db = block_on(Db::open(config)).unwrap();
    let (link, _wire) = link_as(&db, server, who);
    pump(server, &db);
    assert!(jazz::db::block_on(client.prepare(&db)).unwrap());
    Device {
        who,
        client,
        db,
        _link: Some(link),
        live: Default::default(),
    }
}

/// Bob watches a shared room before he has its key. Once he acquires it
/// he can evaluate rotations until the authority refuses his first attempt.
#[test]
fn only_an_eligible_holder_checks_the_mark_and_watches_public_successors() {
    within(|| {
        let server = block_on(Db::open(jazz::db::DbConfig::new(
            owned_schema(),
            identity(0),
        )))
        .unwrap();
        jazz::db::block_on(server.set_authority()).unwrap();
        let alice = owned_device(&server, account(0xa1), MemoryStore::new(), 1);
        let bob = owned_device(&server, account(0xb0), MemoryStore::new(), 2);
        alice.activate(&server);
        bob.activate(&server);
        let space = block_on(alice.db.insert(
            "projects",
            BTreeMap::from([
                ("name".to_owned(), text("plans")),
                (
                    "ownerId".to_owned(),
                    Value::Uuid(uuid::Uuid::parse_str(&alice.id()).unwrap()),
                ),
            ]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid()
        .0;
        pump(&server, &alice.db);
        add_member(&server, &alice, space, &alice.id());
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        add_member(&server, &alice, space, &bob.id());
        let address = space_address(&bob, space);
        let mut watch = block_on(bob.client.watch_spaces(&bob.db)).unwrap();
        watch.use_space(&address);
        let sync = |watch: &mut jazz_e2ee_client::SpaceWatch| {
            for _ in 0..2 {
                bob.sync_spaces(&server, watch).unwrap();
                pump(&server, &bob.db);
            }
        };
        let watched = || bob.client.watched_member_accounts(&address);
        let successors = ("__e2ee_public_account_successors", alice.id());
        sync(&mut watch);
        assert!(watched().contains(&("__e2ee_account_roots", alice.id())));
        let before = bob.client.delivery_mark_checks();
        let deliveries = drive(&server, &[&bob.db], bob.client.deliver_keys(&bob.db)).unwrap();
        assert!(deliveries.is_empty());
        sync(&mut watch);
        assert_eq!(bob.client.delivery_mark_checks(), before, "Bob has no key");
        assert!(!watched().contains(&successors));
        assert!(watched().contains(&("__e2ee_account_roots", alice.id())));
        deliver(&server, &alice, &[&bob], space);
        sync(&mut watch);
        assert!(bob.client.delivery_mark_checks() > before);
        assert!(watched().contains(&successors));
        assert!(!watched().contains(&("__e2ee_account_keys", alice.id())));
        let dave = owned_device(&server, account(0xd0), MemoryStore::new(), 3);
        server_member(&server, &alice, space, &dave.id());
        sync(&mut watch);
        let daves_keys = ("__e2ee_account_keys", dave.id());
        assert!(watched().contains(&daves_keys));
        dave.activate(&server);
        sync(&mut watch);
        assert!(!watched().contains(&daves_keys));
        bob.rotate(&server, &address).unwrap_err();
        let refused = bob.client.delivery_mark_checks();
        deliver(&server, &bob, &[], space);
        sync(&mut watch);
        assert_eq!(bob.client.delivery_mark_checks(), refused);
        assert!(!watched().contains(&successors));
        assert!(watched().contains(&("__e2ee_account_roots", alice.id())));
    });
}

fn rotation_refusal_members() -> (Db, Device, Device, Device, uuid::Uuid) {
    let authority = block_on(Db::open(jazz::db::DbConfig::new(
        owned_schema(),
        identity(0),
    )))
    .unwrap();
    jazz::db::block_on(authority.set_authority()).unwrap();
    let server = authority;
    let alice = owned_device(&server, account(0xa1), MemoryStore::new(), 1);
    alice.activate(&server);
    let bob = owned_device(&server, account(0xb0), MemoryStore::new(), 2);
    bob.activate(&server);
    let carol = owned_device(&server, account(0xc0), MemoryStore::new(), 3);
    carol.activate(&server);
    let owner = Value::Uuid(uuid::Uuid::parse_str(&alice.id()).unwrap());
    let space = block_on(alice.db.insert(
        "projects",
        BTreeMap::from([
            ("name".to_owned(), text("plans")),
            ("ownerId".to_owned(), owner),
        ]),
        Default::default(),
    ))
    .unwrap()
    .row_uuid()
    .0;
    pump(&server, &alice.db);
    for member in [&alice, &bob, &carol] {
        add_member(&server, &alice, space, &member.id());
    }
    (server, alice, bob, carol, space)
}

/// Alice and bob, both members of one new project, both active, each
/// following the notes.
fn two_members(server: &Db) -> (Device, Device, uuid::Uuid) {
    let alice = device(server, account(0xa1), MemoryStore::new(), 1);
    alice.activate(server);
    let bob = device(server, account(0xb0), MemoryStore::new(), 2);
    bob.activate(server);
    let space = new_project(server, &alice, "plans");
    add_member(server, &alice, space, &bob.id());
    alice.follow_notes(server);
    bob.follow_notes(server);
    pump(server, &bob.db);
    (alice, bob, space)
}

/// Contract: when the server shows alice no root of a space (it has not
/// reached her, or is withheld) and bob's root is accepted first, alice's
/// creation is a lost creation: her key is marked `refused` once she sees
/// the space's root, and no write is ever sealed under it, so her next
/// write waits for bob's key (`key-pending`) instead of leaving rows no
/// member can open.
///
/// ```text
/// bob ──note + root R_b──► server      (alice's replica does not see it)
/// alice ──note + root R_a──► server    (R_a is the later root)
/// alice ──acquire──► chain root R_b: her key refused; write: key-pending
/// ```
#[test]
fn a_lost_creation_is_refused_and_never_written_under() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = two_members(&server);
        write_note(&server, &bob, space, "bob").unwrap();
        pump(&server, &bob.db);
        assert_eq!(count(&server, SPACES), 1);

        // Alice, offline, does not know bob's root: she stages her own.
        alice.client.set_online(false);
        block_on(host_write(&alice.client, &alice.db, async || {
            alice
                .db
                .insert("notes", note(space, "alice"), Default::default())
                .await
        }))
        .unwrap();
        alice.client.set_online(true);
        pump(&server, &alice.db);
        assert_eq!(count(&server, SPACES), 2);

        let state = drive(
            &server,
            &[&alice.db, &bob.db],
            alice
                .client
                .acquire(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        assert_eq!(state, SpaceKeyState::KeyPending);
        assert_eq!(marks(&alice, space), [KeyMark::Refused]);
        let error = match write_note(&server, &alice, space, "again") {
            Err(ClientError::Database(error)) => error,
            other => panic!("{other:?}"),
        };
        assert_eq!(error.reason, Some(jazz::db::Reason::KeyPending));
        assert_eq!(count(&server, "notes"), 2, "nothing was written");
    });
}

/// Contract: a root the server inserts that no account's device signed
/// (an earlier position, an invalid signature) never blocks a valid later
/// root: alice's creation is the space's root and her key is published.
///
/// ```text
/// server ──forged root (first position)──► alice's replica
/// alice ──note + root──► accepted; chain root = hers, the forgery ignored
/// ```
#[test]
fn a_forged_earlier_root_does_not_block_a_valid_root() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        let scope = jazz::db::block_on(alice.db.table_identity("projects"))
            .unwrap()
            .unwrap()
            .0;
        let id = |n: u8| Value::Uuid(uuid::Uuid::from_bytes([n; 16]));
        let forged = BTreeMap::from([
            ("scopeId".to_owned(), Value::Uuid(scope)),
            ("identifier".to_owned(), Value::Uuid(space)),
            (
                "creatorAccountId".to_owned(),
                Value::Uuid(uuid::Uuid::parse_str(&alice.id()).unwrap()),
            ),
            ("deviceId".to_owned(), id(1)),
            ("accountEpochId".to_owned(), id(2)),
            ("epochId".to_owned(), id(3)),
            ("mechanism".to_owned(), text("forged")),
            ("version".to_owned(), Value::I64(1)),
            ("verification".to_owned(), Value::Bytes(vec![1; 8])),
            ("accountEnvelope".to_owned(), Value::Bytes(vec![2; 8])),
            ("signature".to_owned(), Value::Bytes(vec![3; 8])),
        ]);
        block_on(server.insert(SPACES, forged, Default::default())).unwrap();
        alice.follow_notes(&server);
        pump(&server, &alice.db);

        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(count(&server, SPACES), 2);
        assert_eq!(marks(&alice, space), [KeyMark::Published]);
        let rows = read_notes(&server, &alice, space).unwrap();
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    });
}

/// Contract: a server restored from an older copy cannot show alice's own
/// account at an earlier state when she reads a space: acquiring is
/// `stale-state`, so a key is never taken from a view in which a device she
/// removed is still active.
///
/// ```text
/// alice ──verify (cut c1)──► server (copy)   ──verify (cut c2)──► server
/// alice ──acquire──► restored copy (c1) ──✗ stale-state
/// ```
#[test]
fn acquiring_from_a_rolled_back_server_is_stale_state() {
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let old = dir.path().join("old.sqlite");
        let settings = EncryptionSettings::default();
        let who = account(0xa1);
        let client = open_account(Rc::new(MemoryStore::new()), who, settings.clone(), None);
        let (space, scope) = {
            let server = secure_authority_with(&settings, Some(&live));
            let (db, _link) = session(&client, &server, who, &settings, 1, None);
            drive(&server, &[&db], client.ensure_account(&db)).unwrap();
            (
                uuid::Uuid::new_v4(),
                jazz::db::block_on(db.table_identity("projects"))
                    .unwrap()
                    .unwrap()
                    .0,
            )
        };
        for suffix in ["", "-wal", "-shm"] {
            let source = format!("{}{suffix}", live.display());
            if std::path::Path::new(&source).exists() {
                std::fs::copy(&source, format!("{}{suffix}", old.display())).unwrap();
            }
        }
        {
            let server = secure_authority_with(&settings, Some(&live));
            let (db, _link) = session(&client, &server, who, &settings, 2, None);
            project(&db, "a later write");
            pump(&server, &db);
            drive(&server, &[&db], client.ensure_account(&db)).unwrap();
        }
        let restored = secure_authority_with(&settings, Some(&old));
        let (db, _link) = session(&client, &restored, who, &settings, 3, None);
        let address = jazz::e2ee::SpaceAddress {
            scope_id: scope.to_string(),
            identifier: space.to_string(),
        };
        let refused = drive(&restored, &[&db], client.acquire(&db, &address));
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(
                    jazz_e2ee::protocol::ProtocolError::StaleState
                ))
            ),
            "{refused:?}"
        );
    });
}

/// Contract: a server restored from an older copy cannot show the author
/// of a space's root at an earlier state than this device verified (§6.3a):
/// once bob has read alice's account at a later cut, acquiring her space
/// from the restored copy is `stale-state`, so a key is never taken from a
/// view in which a device alice removed could still be active. Bob's own
/// account passes its floor on the same copy: his own space is acquired.
///
/// ```text
/// alice ──space A (root)──► server   bob ──space B (root)──► server (copy)
/// bob ──later write; acquire A (floor of alice at c2)──► server
/// bob ──acquire B──► restored copy: held
/// bob ──acquire A──► restored copy (alice at c1) ──✗ stale-state
/// ```
#[test]
fn acquiring_with_a_rolled_back_author_is_stale_state() {
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let old = dir.path().join("old.sqlite");
        let settings = EncryptionSettings::default();
        let (alice_id, bob) = {
            let server = secure_authority_with(&settings, Some(&live));
            let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
            alice.activate(&server);
            let bob = device(&server, account(0xb0), MemoryStore::new(), 2);
            bob.activate(&server);
            let theirs = new_project(&server, &alice, "plans");
            add_member(&server, &alice, theirs, &bob.id());
            write_note(&server, &alice, theirs, SECRET).unwrap();
            pump(&server, &alice.db);
            let own = new_project(&server, &bob, "mine");
            write_note(&server, &bob, own, "bob's").unwrap();
            pump(&server, &bob.db);
            let theirs = space_address(&bob, theirs);
            let own = space_address(&bob, own);
            (alice.id(), (bob.who, bob.client, theirs, own))
        };
        let (who, client, theirs, own) = bob;
        for suffix in ["", "-wal", "-shm"] {
            let source = format!("{}{suffix}", live.display());
            if std::path::Path::new(&source).exists() {
                std::fs::copy(&source, format!("{}{suffix}", old.display())).unwrap();
            }
        }
        {
            let server = secure_authority_with(&settings, Some(&live));
            let (db, _link) = session(&client, &server, who, &settings, 3, None);
            project(&db, "a later write");
            pump(&server, &db);
            drive(&server, &[&db], client.acquire(&db, &theirs)).unwrap();
        }
        let floor = block_on(client.floor(&alice_id)).expect("alice's account is floored");

        let restored = secure_authority_with(&settings, Some(&old));
        let (db, _link) = session(&client, &restored, who, &settings, 4, None);
        let held = drive(&restored, &[&db], client.acquire(&db, &own));
        assert!(matches!(held, Ok(SpaceKeyState::Held)), "{held:?}");
        let refused = drive(&restored, &[&db], client.acquire(&db, &theirs));
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::StaleState))
            ),
            "{refused:?}"
        );
        assert_eq!(
            block_on(client.floor(&alice_id)),
            Some(floor),
            "the floor stays"
        );
    });
}

/// Bob's space watch picks up Alice's delivery and withdraws the report
/// left by his earlier read.
#[test]
fn watched_rows_acquire_and_withdraw_reports() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let events = reports_of(&bob);
        assert!(read_notes(&server, &bob, space).unwrap().is_empty());
        assert_eq!(codes(&events), [(UnreadableCode::KeyPending, false)]);

        let mut watch = block_on(bob.client.watch_spaces(&bob.db)).unwrap();
        bob.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
        pump(&server, &bob.db);
        let results = bob.sync_spaces(&server, &mut watch).unwrap();
        assert!(
            results
                .iter()
                .any(|(_, state)| matches!(state, Ok(SpaceKeyState::Held))),
            "{results:?}"
        );
        assert!(events.borrow().iter().any(|report| matches!(
            report,
            Report::Withdrawn {
                code: UnreadableCode::KeyPending,
                ..
            }
        )));
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 1);
    });
}

/// Contract: a creation the authority refused as a whole (root and note
/// together: neither exists on the server) is found refused after the
/// device restarts, from its own store: the key is `refused` and never
/// writes, and nothing stopped the next write from preparing a new root.
///
/// ```text
/// bob (not owner) ──note + root──► server ✗ refused, nothing stored
/// bob ──write──✗ space-pending      (restart) bob ──write──► reconciles, one new root
/// ```
#[test]
fn a_refused_creation_is_found_after_a_restart() {
    within(|| {
        let authority = block_on(Db::open(jazz::db::DbConfig::new(
            owned_schema(),
            identity(0),
        )))
        .unwrap();
        jazz::db::block_on(authority.set_authority()).unwrap();
        let server = authority;
        let alice = owned_device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let store = MemoryStore::new();
        let bob = owned_device(&server, account(0xb0), store.clone(), 2);
        bob.activate(&server);
        let owner = Value::Uuid(uuid::Uuid::parse_str(&alice.id()).unwrap());
        let space = block_on(alice.db.insert(
            "projects",
            BTreeMap::from([
                ("name".to_owned(), text("plans")),
                ("ownerId".to_owned(), owner),
            ]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid()
        .0;
        pump(&server, &alice.db);
        add_member(&server, &alice, space, &alice.id());
        add_member(&server, &alice, space, &bob.id());
        bob.follow_notes(&server);
        pump(&server, &bob.db);

        write_note(&server, &bob, space, SECRET).unwrap();
        pump(&server, &bob.db);
        pump(&server, &bob.db);
        assert_eq!((count(&server, SPACES), count(&server, "notes")), (0, 0));
        assert!(matches!(
            marks(&bob, space)[..],
            [KeyMark::Unpublished { .. }]
        ));
        // The refusal of the transaction that carried her root stops the
        // automatic path while the client runs: no second root is staged.
        let address = space_address(&bob, space);
        assert!(
            !drive(
                &server,
                &[&bob.db],
                bob.client.prepare_host_write(&bob.db, &address, None),
            )
            .unwrap()
        );
        let stopped = match write_note(&server, &bob, space, "again") {
            Err(ClientError::Database(error)) => error,
            other => panic!("{other:?}"),
        };
        assert_eq!(stopped.reason, Some(jazz::db::Reason::SpacePending));
        assert_eq!(marks(&bob, space), [KeyMark::Refused]);
        drop(bob);

        // After a restart no `reconcile` is called: the first write does it,
        // finds the old key refused, and makes one more attempt.
        let bob = owned_device(&server, account(0xb0), store, 3);
        bob.activate(&server);
        bob.follow_notes(&server);
        write_note(&server, &bob, space, "again").unwrap();
        let mut now = marks(&bob, space);
        now.sort_by_key(|mark| format!("{mark:?}"));
        assert!(matches!(
            now[..],
            [KeyMark::Refused, KeyMark::Unpublished { .. }]
        ));
        // The authority refuses it too, and the automatic paths stop again.
        pump(&server, &bob.db);
        pump(&server, &bob.db);
        let address = space_address(&bob, space);
        assert!(
            !drive(
                &server,
                &[&bob.db],
                bob.client.prepare_host_write(&bob.db, &address, None),
            )
            .unwrap()
        );
        let stopped = match write_note(&server, &bob, space, "third") {
            Err(ClientError::Database(error)) => error,
            other => panic!("{other:?}"),
        };
        assert_eq!(stopped.reason, Some(jazz::db::Reason::SpacePending));
        assert_eq!((count(&server, SPACES), count(&server, "notes")), (0, 0));
    });
}

/// A root row of `space` that this client cannot decode: `version` is out
/// of its range (as a newer format's would be, or junk).
fn undecodable_root(server: &Db, by: &Device, space: uuid::Uuid, version: i64) {
    let scope = jazz::db::block_on(by.db.table_identity("projects"))
        .unwrap()
        .unwrap()
        .0;
    let id = |n: u8| Value::Uuid(uuid::Uuid::from_bytes([n; 16]));
    let cells = BTreeMap::from([
        ("scopeId".to_owned(), Value::Uuid(scope)),
        ("identifier".to_owned(), Value::Uuid(space)),
        (
            "creatorAccountId".to_owned(),
            Value::Uuid(uuid::Uuid::parse_str(&by.id()).unwrap()),
        ),
        ("deviceId".to_owned(), id(1)),
        ("accountEpochId".to_owned(), id(2)),
        ("epochId".to_owned(), id(3)),
        ("mechanism".to_owned(), text("newer")),
        ("version".to_owned(), Value::I64(version)),
        ("verification".to_owned(), Value::Bytes(vec![1; 8])),
        ("accountEnvelope".to_owned(), Value::Bytes(vec![2; 8])),
        ("signature".to_owned(), Value::Bytes(vec![3; 8])),
    ]);
    block_on(server.insert(SPACES, cells, Default::default())).unwrap();
}

/// Contract: a root this client cannot decode (junk, or a newer format's)
/// is not an absent root: the space reads `Incomplete`, no write creates a
/// competing root, and the space is reported unsupported, never
/// `space-pending`.
///
/// ```text
/// server ──root with an undecodable version──► alice (member)
/// alice ──write──✗ (no creation)    alice ──acquire──► Incomplete
/// ```
#[test]
fn rows_this_client_cannot_read_are_not_an_absent_root() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        undecodable_root(&server, &alice, space, -1);
        undecodable_root(&server, &alice, space, 1 << 40);
        alice.follow_notes(&server);
        let events = reports_of(&alice);

        assert!(write_note(&server, &alice, space, SECRET).is_err());
        pump(&server, &alice.db);
        assert_eq!(count(&server, SPACES), 2, "no root of hers");
        assert_eq!(count(&server, "notes"), 0);
        assert!(marks(&alice, space).is_empty());
        let state = drive(
            &server,
            &[&alice.db],
            alice
                .client
                .acquire(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        assert_eq!(state, SpaceKeyState::Incomplete);
        assert!(
            codes(&events)
                .iter()
                .all(|(code, _)| *code != UnreadableCode::SpacePending)
        );
    });
}

/// Contract: a read that shows no root of a space whose key this device
/// holds (the server hides the root rows) never gets a second root when
/// alice attempts another encrypted write.
///
/// ```text
/// alice (holds K₀) ──write──► server has hidden the root ✗ no root staged
/// ```
#[test]
fn a_hidden_root_does_not_get_a_second_root() {
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let old = dir.path().join("old.sqlite");
        let settings = EncryptionSettings::default();
        let store = MemoryStore::new();
        let space = {
            let server = secure_authority_with(&settings, Some(&live));
            let alice = device(&server, account(0xa1), store.clone(), 1);
            alice.activate(&server);
            let space = new_project(&server, &alice, "plans");
            pump(&server, &alice.db);
            for suffix in ["", "-wal", "-shm"] {
                let source = format!("{}{suffix}", live.display());
                if std::path::Path::new(&source).exists() {
                    std::fs::copy(&source, format!("{}{suffix}", old.display())).unwrap();
                }
            }
            write_note(&server, &alice, space, SECRET).unwrap();
            pump(&server, &alice.db);
            drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
            assert_eq!(marks(&alice, space), [KeyMark::Published]);
            space
        };

        // The server she reaches now shows no root of the space.
        let hiding = secure_authority_with(&settings, Some(&old));
        assert_eq!(count(&hiding, SPACES), 0);
        let alice = device(&hiding, account(0xa1), store, 2);
        alice.activate(&hiding);
        let address = space_address(&alice, space);
        assert!(
            !drive(
                &hiding,
                &[&alice.db],
                alice.client.prepare_host_write(&alice.db, &address, None),
            )
            .unwrap()
        );
        pump(&hiding, &alice.db);
        assert_eq!(count(&hiding, SPACES), 0, "no second root");
        assert_eq!(marks(&alice, space), [KeyMark::Published]);
    });
}

/// Contract: a creation is settled `refused` only after the authority
/// shows its root absent, never because this device's own store has no
/// record of it: a store that lost the commit (a fresh database) must not
/// mark an accepted root's key refused for good, and a root that never
/// reached the authority is refused once the authority is asked.
///
/// ```text
/// alice ──note + root──► accepted   (her database is lost; restart)
/// alice ──reconcile: root not in her store, in the authority's──► published
/// alice ──note + root, offline (lost)──► reconcile: absent there ──► refused
/// ```
#[test]
fn a_creation_is_settled_from_the_authority_not_the_local_store() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let store = MemoryStore::new();
        let alice = device(&server, account(0xa1), store.clone(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        assert_eq!(count(&server, SPACES), 1);
        drop(alice);

        let alice = device(&server, account(0xa1), store.clone(), 2);
        alice.activate(&server);
        assert!(matches!(
            marks(&alice, space)[..],
            [KeyMark::Unpublished { .. }]
        ));
        // The first write of the run reconciles by itself.
        write_note(&server, &alice, space, "after").unwrap();
        assert_eq!(marks(&alice, space), [KeyMark::Published]);
        pump(&server, &alice.db);
        assert_eq!(count(&server, SPACES), 1);

        // A second space whose creation never reached the authority.
        let other = new_project(&server, &alice, "later");
        alice.client.set_online(false);
        block_on(host_write(&alice.client, &alice.db, async || {
            alice
                .db
                .insert("notes", note(other, "lost"), Default::default())
                .await
        }))
        .unwrap();
        alice.client.set_online(true);
        drop(alice);
        let alice = device(&server, account(0xa1), store, 3);
        alice.activate(&server);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(marks(&alice, other), [KeyMark::Refused]);
        assert_eq!(marks(&alice, space), [KeyMark::Published]);
    });
}

/// Contract: reports that no longer hold are withdrawn, per row too: when
/// carol, who was not a member and saw each row `key-unavailable`, becomes
/// a member, those reports are withdrawn (a late listener is not told
/// them) and the space is `key-pending`.
#[test]
fn per_row_reports_are_withdrawn_when_they_no_longer_hold() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (space, store) = alice_with_a_note(&server);
        let alice = device(&server, account(0xa1), store, 5);
        alice.activate(&server);
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        carol.follow_notes(&server);
        let seen = reports_of(&carol);
        assert!(read_notes(&server, &carol, space).unwrap().is_empty());
        assert_eq!(codes(&seen), [(UnreadableCode::KeyUnavailable, true); 4]);

        add_member(&server, &alice, space, &carol.id());
        pump(&server, &carol.db);
        assert!(read_notes(&server, &carol, space).unwrap().is_empty());
        let withdrawn = seen
            .borrow()
            .iter()
            .filter(|report| {
                matches!(
                    report,
                    Report::Withdrawn {
                        id: Some(_),
                        code: UnreadableCode::KeyUnavailable,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(withdrawn, 4);
        assert_eq!(
            codes(&reports_of(&carol)),
            [(UnreadableCode::KeyPending, false)],
            "a late listener is told only what still holds"
        );
    });
}

/// Contract: through the host path (the database call alone, then
/// `prepare_host_write` after a key failure), a write the database fails after
/// it staged the space's root leaves no creation: the next write does not
/// seal under that root's key, but stages a fresh root with itself.
///
/// ```text
/// host insert ──NeedsKey──► prepare_host_write: root R prepared
/// host insert (bad cell) ──stages R, core fails──► R dropped
/// host insert ──NeedsKey (R's key held back)──► prepare_host_write: R refused, root R' ──► note + R'
/// ```
#[test]
fn a_host_write_that_fails_after_staging_its_root_gets_a_fresh_root() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        let mut bad = note(space, SECRET);
        bad.insert("title".to_owned(), Value::I32(1));
        let first = block_on(alice.db.insert("notes", bad.clone(), Default::default()))
            .map(|_| ())
            .unwrap_err();
        let Some(jazz::e2ee::E2eeRefusal::NeedsKey { space: at }) =
            jazz::e2ee::E2eeRefusal::of(&first)
        else {
            panic!("the first write needs the space's key: {first:?}");
        };
        assert!(
            drive(
                &server,
                &[&alice.db],
                alice.client.prepare_host_write(&alice.db, &at, None)
            )
            .unwrap()
        );
        let failed = block_on(alice.db.insert("notes", bad, Default::default()))
            .map(|_| ())
            .unwrap_err();
        assert!(
            jazz::e2ee::E2eeRefusal::of(&failed).is_none(),
            "the database refused the write itself: {failed:?}"
        );

        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        assert_eq!(
            (count(&server, SPACES), count(&server, "notes")),
            (1, 1),
            "the note came with a root"
        );
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        // Roots are listed by ID, which is random: compare the marks as a set.
        let mut held = marks(&alice, space);
        held.sort_by_key(|mark| format!("{mark:?}"));
        assert_eq!(held, [KeyMark::Published, KeyMark::Refused]);
        let rows = read_notes(&server, &alice, space).unwrap();
        assert_eq!(rows.len(), 1);
    });
}

/// Contract: rows of an account's history that this client cannot decode
/// make that account's history odd, not the space incomplete: bob, a
/// member awaiting a delivery, still sees the space `key-pending`, not
/// `unsupported-format`, when alice's history holds a junk row.
#[test]
fn junk_in_an_account_history_does_not_make_the_space_incomplete() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (space, store) = alice_with_a_note(&server);
        let alice = device(&server, account(0xa1), store, 5);
        alice.activate(&server);
        let bob = device(&server, account(0xb0), MemoryStore::new(), 6);
        bob.activate(&server);
        add_member(&server, &alice, space, &bob.id());
        block_on(server.insert(
            "__e2ee_account_roots",
            BTreeMap::from([
                ("ledgerVersion".to_owned(), Value::I64(-1)),
                ("generation".to_owned(), Value::I64(0)),
                (
                    "accountId".to_owned(),
                    Value::Uuid(uuid::Uuid::parse_str(&alice.id()).unwrap()),
                ),
                ("deviceId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
                ("epochId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
                ("kind".to_owned(), text("app")),
            ]),
            Default::default(),
        ))
        .unwrap();
        bob.follow_notes(&server);
        let state = bob.acquire(&server, &space_address(&bob, space)).unwrap();
        assert_eq!(state, SpaceKeyState::KeyPending);
    });
}

/// Alice's space with one note, bob a member of it who has no key yet; both
/// active and following the notes.
fn shared_space(server: &Db) -> (Device, Device, uuid::Uuid) {
    let (alice, bob, space) = two_members(server);
    write_note(server, &alice, space, SECRET).unwrap();
    pump(server, &alice.db);
    drive(server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
    pump(server, &bob.db);
    (alice, bob, space)
}

fn deliver(server: &Db, holder: &Device, other: &[&Device], space: uuid::Uuid) -> usize {
    let mut dbs: Vec<&Db> = vec![&holder.db];
    dbs.extend(other.iter().map(|device| &device.db));
    drive(
        server,
        &dbs,
        deliver_for(&holder.client, &holder.db, &space_address(holder, space)),
    )
    .unwrap()
}

/// Alice starts an attachment, rotates the space key, then publishes. Its
/// body, optional metadata and stream all carry the epoch chosen at begin.
///
/// ```text
/// begin (root epoch) ──► rotate ──► push and finish ──► inspect the stored row
/// ```
#[test]
fn stream_metadata_keeps_the_epoch_chosen_before_pulling() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, _bob, space) = shared_space(&server);
        let db = &alice.db;
        let row = jazz::ids::RowUuid(uuid::Uuid::now_v7());
        let mut cells = note(space, "caption");
        let mut upload = drive(
            &server,
            &[db],
            host_write(&alice.client, db, async || {
                db.begin_streaming_value_upload_for(
                    jazz::db::StreamingMutationKind::Insert,
                    "notes",
                    row,
                    &mut cells,
                    "attachment",
                    &jazz::db::WriteTarget::Root,
                )
                .await
            }),
        )
        .unwrap();
        drive(
            &server,
            &[db],
            alice.client.rotate(db, &space_address(&alice, space)),
        )
        .unwrap();
        assert_eq!(count(&server, "__e2ee_space_successors"), 1);
        block_on(db.push_streaming_value_upload(&mut upload, b"contents")).unwrap();
        block_on(db.finish_streaming_value_upload(
            upload,
            jazz::db::StreamingMutationKind::Insert,
            "notes",
            row,
            cells,
            "attachment",
            Default::default(),
            None,
            None,
            None,
        ))
        .unwrap();
        pump(&server, db);
        let stored = all(&server, Query::from("notes"))
            .unwrap()
            .into_iter()
            .find(|note| note.row_uuid() == row)
            .unwrap();
        let root_epoch = all(&server, Query::from(SPACES)).unwrap()[0].cell("epochId");
        for name in ["attachment", "body", "note", "rank"] {
            let Some(Value::Bytes(bytes)) = stored.cell(name) else {
                panic!("missing stored {name}");
            };
            let record = jazz_e2ee::records::read_stored_record(&bytes).unwrap();
            assert_eq!(
                Some(Value::Uuid(uuid::Uuid::parse_str(record.epoch_id).unwrap())),
                root_epoch,
                "{name}"
            );
        }
    });
}

/// Alice can rotate her new project before its first encrypted write.
#[test]
fn explicit_rotation_of_a_space_without_a_key_writes_nothing() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        assert_eq!(
            drive(
                &server,
                &[&alice.db],
                alice
                    .client
                    .rotate(&alice.db, &space_address(&alice, space))
            )
            .unwrap(),
            0
        );
        assert_eq!(count(&server, SPACES), 0);
        assert_eq!(count(&server, "__e2ee_space_successors"), 0);
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 0);
    });
}

/// Alice refuses to rotate when the server hides a root whose key she holds;
/// Bob sees no evidence of the root and gets the same zero as a keyless space.
///
/// ```text
/// alice ──write──► server creates root
/// server (copy before the write) ──no root──► alice: rotation-needs-key
///                               └──no root──► bob: zero deliveries
/// ```
#[test]
fn explicit_rotation_of_a_hidden_root_depends_on_held_key_evidence() {
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let old = dir.path().join("old.sqlite");
        let settings = EncryptionSettings::default();
        let alice_store = MemoryStore::new();
        let bob_store = MemoryStore::new();
        let space = {
            let server = secure_authority_with(&settings, Some(&live));
            let alice = device(&server, account(0xa1), alice_store.clone(), 1);
            alice.activate(&server);
            let bob = device(&server, account(0xb0), bob_store.clone(), 2);
            bob.activate(&server);
            let space = new_project(&server, &alice, "plans");
            add_member(&server, &alice, space, &bob.id());
            for suffix in ["", "-wal", "-shm"] {
                let source = format!("{}{suffix}", live.display());
                if std::path::Path::new(&source).exists() {
                    std::fs::copy(&source, format!("{}{suffix}", old.display())).unwrap();
                }
            }
            write_note(&server, &alice, space, SECRET).unwrap();
            pump(&server, &alice.db);
            drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
            assert_eq!(count(&server, SPACES), 1);
            assert_eq!(marks(&alice, space), [KeyMark::Published]);
            space
        };

        let hiding = secure_authority_with(&settings, Some(&old));
        assert_eq!(count(&hiding, SPACES), 0);
        let alice = device(&hiding, account(0xa1), alice_store, 3);
        alice.activate(&hiding);
        let address = space_address(&alice, space);
        let error = drive(
            &hiding,
            &[&alice.db],
            alice.client.rotate(&alice.db, &address),
        )
        .expect_err("Alice holds the key of the hidden root");
        let ClientError::Protocol(ProtocolError::Refused(code)) = error else {
            panic!("unexpected rotation error: {error}");
        };
        assert_eq!(code.code(), "rotation-needs-key");

        let bob = device(&hiding, account(0xb0), bob_store, 4);
        bob.activate(&hiding);
        assert!(marks(&bob, space).is_empty());
        assert_eq!(
            drive(&hiding, &[&bob.db], bob.client.rotate(&bob.db, &address)).unwrap(),
            0
        );
        assert_eq!(count(&hiding, "__e2ee_space_successors"), 0);
    });
}

/// Alice refuses to rotate when the server shows a root row she cannot decode.
#[test]
fn explicit_rotation_refuses_when_a_root_row_is_skipped() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        undecodable_root(&server, &alice, space, -1);
        assert!(marks(&alice, space).is_empty());
        let address = space_address(&alice, space);
        let error = alice
            .rotate(&server, &address)
            .expect_err("the skipped row could be the root");
        let ClientError::Protocol(ProtocolError::Refused(code)) = error else {
            panic!("unexpected rotation error: {error}");
        };
        assert_eq!(code.code(), "rotation-needs-key");
        assert_eq!(count(&server, "__e2ee_space_successors"), 0);
    });
}

/// Bob is a member, but Alice has yet to deliver the project's key to him.
#[test]
fn explicit_rotation_tells_a_member_to_wait_for_the_key() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (_alice, bob, space) = shared_space(&server);
        let error = bob
            .rotate(&server, &space_address(&bob, space))
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::Protocol(ProtocolError::Refused(RefusalCode::RotationNeedsKey))
        ));
        assert_eq!(
            error.to_string(),
            "This device does not hold the space's current key yet"
        );
        assert_eq!(count(&server, "__e2ee_space_successors"), 0);
    });
}

/// Bob keeps the key Alice delivered, but losing his membership prevents him from rotating.
#[test]
fn explicit_rotation_refuses_a_removed_member_who_holds_the_key() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
        let address = space_address(&bob, space);
        assert_eq!(bob.acquire(&server, &address).unwrap(), SpaceKeyState::Held);
        block_on(
            alice
                .db
                .delete(MEMBERS, member_row(&server, &bob.id()), Default::default()),
        )
        .unwrap();
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        assert_eq!(count(&server, MEMBERS), 1, "only Alice is a member");
        // A fresh replica sees the removal. Bob's original replica can
        // still show his former member row alongside an invisible root.
        let (fresh, _link) = session(
            &bob.client,
            &server,
            bob.who,
            &EncryptionSettings::default(),
            3,
            None,
        );
        let refusal = |db| {
            let error = drive(&server, &[db], bob.client.rotate(db, &address)).unwrap_err();
            let ClientError::Protocol(error) = error else {
                panic!("unexpected error: {error}")
            };
            (error.code(), error.to_string())
        };
        let removed = (
            Some("not-a-member"),
            "Your account is not a member of this space".to_owned(),
        );
        assert_eq!(refusal(&fresh), removed);
        let original = refusal(&bob.db);
        assert!(
            original == removed
                || original
                    == (
                        Some("rotation-needs-key"),
                        "This device cannot confirm the space's key state".to_owned()
                    ),
            "{original:?}"
        );
        assert_eq!(count(&server, "__e2ee_space_successors"), 0);
    });
}

/// Alice's second device must be approved even when her project has no key yet.
#[test]
fn explicit_rotation_on_a_device_awaiting_approval_explains_why() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        let second = device(&server, alice.who, MemoryStore::new(), 2);
        let db = Rc::new(second.db);
        let runtime = jazz_e2ee_client::host::runtime::Encryption::new(
            jazz_e2ee_client::host::runtime::Built {
                spaces: serde_json::json!({}),
                client: Rc::new(second.client),
                reports: Rc::default(),
            },
            Rc::new(jazz_e2ee_client::host::bridge::JsStore::default()),
            Rc::clone(&db),
        );
        assert_eq!(
            drive(
                &server,
                &[&db],
                runtime.call("ready".into(), serde_json::json!({}))
            )
            .unwrap(),
            "awaiting-approval"
        );
        let args = serde_json::json!({ "table": "projects", "id": space.to_string() });
        assert_eq!(
            drive(&server, &[&db], runtime.call("keysRotate".into(), args)).unwrap_err(),
            "no-account: This device is waiting for approval"
        );
        assert_eq!(count(&server, SPACES), 0);
    });
}

/// Contract: alice and bob share a space end to end: alice's pass delivers
/// the current version to bob, who acquires it from the delivery row and
/// reads what alice wrote; a second pass writes nothing.
///
/// ```text
/// alice (holder) ──pass: bob lacks the version──► delivery ──► server
/// bob ──acquire (his sealing key opens it)──► reads alice's note
/// ```
#[test]
fn a_holder_delivers_the_current_version_to_a_new_member() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        assert!(read_notes(&server, &bob, space).unwrap().is_empty());
        assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 1);
        pump(&server, &bob.db);
        let rows = read_notes(&server, &bob, space).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
        assert_eq!(deliver(&server, &alice, &[&bob], space), 0);
    });
}

/// Contract: two holders racing to deliver to the same new member produce
/// exactly one delivery: the first to commit wins, and the other, whose read
/// of the space's deliveries changed, reads again and stops.
///
/// ```text
/// alice, bob (holders) ──pass──┬──► delivery to carol (first)
///                              └──► conflict ──re-read──► nothing to do
/// ```
#[test]
fn two_holders_racing_write_exactly_one_delivery() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        deliver(&server, &alice, &[&bob], space);
        pump(&server, &bob.db);
        bob.acquire(&server, &space_address(&bob, space)).unwrap();
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        add_member(&server, &alice, space, &carol.id());
        pump(&server, &bob.db);
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 1);

        let (a, b) = drive(
            &server,
            &[&alice.db, &bob.db],
            futures::future::join(
                deliver_for(&alice.client, &alice.db, &space_address(&alice, space)),
                deliver_for(&bob.client, &bob.db, &space_address(&bob, space)),
            ),
        );
        assert_eq!(a.unwrap() + b.unwrap(), 1);
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 2);
    });
}

/// Once Alice has seen the project tombstone she can still write retained
/// notes where policy allows, without attempting a rotation of the deleted project.
#[test]
fn a_deleted_project_skips_automatic_rotation() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        deliver(&server, &alice, &[&bob], space);
        block_on(
            alice
                .db
                .delete(MEMBERS, member_row(&server, &bob.id()), Default::default()),
        )
        .unwrap();
        pump(&server, &alice.db);
        let address = space_address(&alice, space);
        deliver(&server, &alice, &[], space);
        assert!(alice.client.needs_rotation(&address).unwrap());
        block_on(
            alice
                .db
                .delete("projects", jazz::ids::RowUuid(space), Default::default()),
        )
        .unwrap();
        pump(&server, &alice.db);
        let events = reports_of(&alice);
        write_note(&server, &alice, space, "retained note").unwrap();
        pump(&server, &alice.db);
        assert_eq!(count(&server, "__e2ee_space_successors"), 0);
        assert!(
            events
                .borrow()
                .iter()
                .all(|report| !matches!(report, Report::RotationFailed { .. }))
        );
        assert!(
            alice.client.needs_rotation(&address).unwrap(),
            "restoring the row can resume rotation"
        );
    });
}

/// Contract: after bob's member row is deleted, the space needs rotation;
/// alice's next encrypted write rotates it first (a successor and a delivery
/// to carol, none to bob), and carol reads what alice writes under the new
/// version while bob, who keeps the old key, reads only the old rows.
///
/// ```text
/// alice ──delete bob's member row──► pass: needs rotation
/// alice ──write──► rotate (successor + delivery to carol) ──► note under K'
/// carol reads both notes;  bob reads only the first
/// ```
#[test]
fn removing_a_member_rotates_the_space_before_the_next_write() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        add_member(&server, &alice, space, &carol.id());
        carol.follow_notes(&server);
        assert_eq!(deliver(&server, &alice, &[&bob, &carol], space), 2);
        pump(&server, &bob.db);
        pump(&server, &carol.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 1);
        assert_eq!(read_notes(&server, &carol, space).unwrap().len(), 1);

        let member = all(&server, Query::from(MEMBERS))
            .unwrap()
            .into_iter()
            .find(|row| {
                row.cell("accountId")
                    == Some(Value::Uuid(uuid::Uuid::parse_str(&bob.id()).unwrap()))
            })
            .unwrap()
            .row_uuid();
        block_on(alice.db.delete(MEMBERS, member, Default::default())).unwrap();
        pump(&server, &alice.db);
        assert_eq!(deliver(&server, &alice, &[], space), 0);
        assert!(
            alice
                .client
                .needs_rotation(&space_address(&alice, space))
                .unwrap()
        );

        write_note(&server, &alice, space, "after").unwrap();
        pump(&server, &alice.db);
        assert_eq!(count(&server, "__e2ee_space_successors"), 1);
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 3);
        assert!(
            !alice
                .client
                .needs_rotation(&space_address(&alice, space))
                .unwrap()
        );

        pump(&server, &carol.db);
        assert_eq!(read_notes(&server, &carol, space).unwrap().len(), 2);
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 1);
    });
}

/// Contract: removing a member is enough for the next write to rotate:
/// with no pass and no `rotate` called by the app, the remover's next write,
/// wrapped or a host's, is sealed under a new version that the removed
/// member never receives, and the queue of removals is served.
///
/// ```text
/// alice deletes bob's member row ──write──► pass: marked ─► rotate ─► note
/// bob reads only the note from before
/// ```
#[test]
fn a_removal_alone_rotates_the_space_before_the_next_write() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 1);

        let member = member_row(&server, &bob.id());
        block_on(alice.db.delete(MEMBERS, member, Default::default())).unwrap();
        assert_eq!(alice.client.pending_member_removals(), 1);

        write_note(&server, &alice, space, "after").unwrap();
        pump(&server, &alice.db);
        assert_eq!(
            count(&server, "__e2ee_space_successors"),
            1,
            "the write rotated first"
        );
        assert_eq!(alice.client.pending_member_removals(), 0);
        assert!(
            !alice
                .client
                .needs_rotation(&space_address(&alice, space))
                .unwrap()
        );

        pump(&server, &bob.db);
        assert_eq!(
            read_notes(&server, &bob, space).unwrap().len(),
            1,
            "the note after the removal is not under bob's version"
        );
    });
}

/// The member row of `account` in the server's members table.
fn member_row(server: &Db, account: &str) -> jazz::ids::RowUuid {
    all(server, Query::from(MEMBERS))
        .unwrap()
        .into_iter()
        .find(|row| {
            row.cell("accountId") == Some(Value::Uuid(uuid::Uuid::parse_str(account).unwrap()))
        })
        .unwrap()
        .row_uuid()
}

/// Alice has observed that Bob, who held her project's key, was removed.
fn observed_removal(server: &Db) -> (Device, uuid::Uuid) {
    let (alice, bob, space) = shared_space(server);
    assert_eq!(deliver(server, &alice, &[&bob], space), 1);
    block_on(server.delete(MEMBERS, member_row(server, &bob.id()), Default::default())).unwrap();
    pump(server, &alice.db);
    deliver(server, &alice, &[], space);
    assert!(
        alice
            .client
            .needs_rotation(&space_address(&alice, space))
            .unwrap()
    );
    (alice, space)
}

/// Read the ciphertext header at the authority to distinguish the old key
/// from the successor even though Alice can decrypt both.
fn note_epoch(server: &Db, note: &jazz::db::WriteHandle) -> Value {
    let query = Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(note.row_uuid().0))));
    let rows = all(server, query).unwrap();
    let Some(Value::Bytes(bytes)) = rows[0].cell("body") else {
        panic!("expected sealed note")
    };
    let epoch = jazz_e2ee::records::read_stored_record(&bytes)
        .unwrap()
        .epoch_id;
    Value::Uuid(epoch.parse().unwrap())
}

/// Alice writes offline after observing Bob's removal. The stored note uses
/// the root key and the next online write uses the successor.
#[test]
fn an_offline_write_after_an_observed_removal_uses_the_current_key() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, space) = observed_removal(&server);
        let root = all(&server, Query::from(SPACES)).unwrap()[0]
            .cell("epochId")
            .unwrap();
        alice.client.set_online(false);
        let offline = block_on(alice.db.insert(
            "notes",
            note(space, "offline"),
            Default::default(),
        ))
        .unwrap();
        assert_eq!(count(&server, "notes"), 1);
        alice.client.set_online(true);
        pump(&server, &alice.db);
        assert_eq!(note_epoch(&server, &offline), root);
        assert_eq!(count(&server, "__e2ee_space_successors"), 0);
        let online = write_note(&server, &alice, space, "online").unwrap();
        pump(&server, &alice.db);
        assert_eq!(count(&server, "__e2ee_space_successors"), 1);
        assert_ne!(note_epoch(&server, &online), root);
    });
}

/// Alice's host times out while the authority has yet to answer a rotation
/// read. Resuming that preparation after the write cannot publish a rotation.
///
/// Alice observes Bob's removal → prepare waits → write under old key
/// → authority answers → no successor → next write rotates first.
#[test]
fn a_write_whose_rotation_is_undecided_uses_the_current_key_once() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, space) = observed_removal(&server);
        let at = space_address(&alice, space);
        let root = all(&server, Query::from(SPACES)).unwrap()[0]
            .cell("epochId")
            .unwrap();
        let reports = reports_of(&alice);
        let insert = || {
            alice
                .db
                .insert("notes", note(space, "waiting"), Default::default())
        };
        assert!(block_on(insert()).is_err());
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
        let mut prepare = std::pin::pin!(alice.client.prepare_host_write(
            &alice.db,
            &at,
            Some(deadline)
        ));
        assert!(
            prepare
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        std::thread::sleep(deadline.saturating_duration_since(std::time::Instant::now()));
        assert!(
            alice
                .client
                .retry_with_current_key(&at, "rotation undecided at the write deadline")
                .unwrap()
        );
        let written = block_on(insert()).unwrap();
        assert!(reports.borrow().iter().any(|report| matches!(report,
            Report::RotationFailed { space, reason } if space == &at && reason.contains("deadline"))));
        drive(&server, &[&alice.db], prepare).unwrap();
        pump(&server, &alice.db);
        assert_eq!(note_epoch(&server, &written), root);
        assert_eq!(
            count(&server, "__e2ee_space_successors"),
            0,
            "expired preparation submitted a rotation"
        );
        assert!(alice.client.needs_rotation(&at).unwrap());
        assert!(
            block_on(insert()).is_err(),
            "the late preparation left a retry mark"
        );
        let next = write_note(&server, &alice, space, "next").unwrap();
        pump(&server, &alice.db);
        assert_eq!(count(&server, "__e2ee_space_successors"), 1);
        assert_ne!(note_epoch(&server, &next), root);
    });
}

/// Contract: a host binding's writes, which `write` does not wrap, rotate
/// the space after every removal of the run, as wrapped writes do: the
/// first removal's rotation does not keep the second one's from happening.
///
/// ```text
/// alice removes bob ──host write──► NeedsKey ─ prepare_host_write: rotate ─► note
/// alice removes carol ──host write──► NeedsKey ─ prepare_host_write: rotate ─► note
/// ```
#[test]
fn a_host_write_rotates_after_each_removal_of_the_run() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        add_member(&server, &alice, space, &carol.id());
        carol.follow_notes(&server);
        assert_eq!(deliver(&server, &alice, &[&bob, &carol], space), 2);
        let at = space_address(&alice, space);

        for (removed, successors) in [(&bob, 1), (&carol, 2)] {
            let member = member_row(&server, &removed.id());
            block_on(alice.db.delete(MEMBERS, member, Default::default())).unwrap();
            pump(&server, &alice.db);
            deliver(&server, &alice, &[], space);
            assert!(alice.client.needs_rotation(&at).unwrap());

            write_note(&server, &alice, space, "after").unwrap();
            pump(&server, &alice.db);
            assert_eq!(
                count(&server, "__e2ee_space_successors"),
                successors,
                "the write after removing {} rotated first",
                removed.id()
            );
            assert!(!alice.client.needs_rotation(&at).unwrap());
        }
    });
}

/// A delivery row from `by` to `to` of `epoch` with a junk envelope.
fn junk_delivery(by: &Device, to: &Device, space: uuid::Uuid, epoch: Value) {
    let scope = jazz::db::block_on(by.db.table_identity("projects"))
        .unwrap()
        .unwrap()
        .0;
    let account = |device: &Device| Value::Uuid(uuid::Uuid::parse_str(&device.id()).unwrap());
    let to_epoch = to
        .client
        .with_account(|account| account.state.epoch_id().to_owned())
        .unwrap();
    let cells = BTreeMap::from([
        ("scopeId".to_owned(), Value::Uuid(scope)),
        ("identifier".to_owned(), Value::Uuid(space)),
        ("keyEpochId".to_owned(), epoch),
        ("senderAccountId".to_owned(), account(by)),
        ("recipientAccountId".to_owned(), account(to)),
        (
            "recipientAccountEpochId".to_owned(),
            Value::Uuid(uuid::Uuid::parse_str(&to_epoch).unwrap()),
        ),
        ("envelope".to_owned(), Value::Bytes(vec![9; 40])),
    ]);
    block_on(
        by.db
            .insert("__e2ee_space_deliveries", cells, Default::default()),
    )
    .unwrap();
}

/// Contract: a member that receives a garbage delivery reports
/// `invalid-delivery` naming the sender, once, and writes nothing; the
/// holder's pass counts it as delivered and writes nothing either, so bob
/// stays `key-pending` until alice rotates, which also delivers to him.
///
/// ```text
/// alice ──junk delivery to bob──► bob: invalid-delivery (once), key-pending
/// alice ──pass──► nothing    alice ──rotate──► bob reads
/// ```
#[test]
fn a_garbage_delivery_is_reported_and_a_rotation_repairs_it() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let root = all(&server, Query::from(SPACES)).unwrap().remove(0);
        junk_delivery(&alice, &bob, space, root.cell("epochId").unwrap());
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        let events = reports_of(&bob);
        for _ in 0..2 {
            let state = bob.acquire(&server, &space_address(&bob, space)).unwrap();
            assert_eq!(state, SpaceKeyState::KeyPending);
        }
        let bad: Vec<_> = events
            .borrow()
            .iter()
            .filter(|report| matches!(report, Report::InvalidDelivery { .. }))
            .cloned()
            .collect();
        assert_eq!(bad.len(), 1, "once per delivery");
        assert!(matches!(
            &bad[0],
            Report::InvalidDelivery { sender, .. } if *sender == alice.id()
        ));
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 1);
        assert_eq!(
            deliver(&server, &alice, &[&bob], space),
            0,
            "counted as delivered"
        );

        drive(
            &server,
            &[&alice.db, &bob.db],
            alice
                .client
                .rotate(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        pump(&server, &bob.db);
        let rows = read_notes(&server, &bob, space).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    });
}

/// Contract: a device that holds live keys of two roots of one address
/// (here a second root's key kept beside the real one) seals no write
/// before it has read the space's chain: the write reads it, and seals under
/// the key of the chain's root, never the other's. A device that lost its
/// keys reads both notes, so both were sealed under the real root.
///
/// ```text
/// alice holds K(real), K(other)  ──write──► NeedsChainRead ──read chain──► K(real)
/// ```
#[test]
fn competing_roots_make_the_write_read_the_chain_first() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (space, store) = alice_with_a_note(&server);
        let alice = device(&server, account(0xa1), store.clone(), 2);
        alice.activate(&server);
        alice.follow_notes(&server);
        let other = space_key_of_account(&alice.client, &alice.db, space);
        block_on(alice.client.keep(other)).unwrap();
        assert_eq!(marks(&alice, space).len(), 2);

        write_note(&server, &alice, space, "second").unwrap();
        pump(&server, &alice.db);
        assert_eq!(count(&server, SPACES), 1);

        let fresh = device(&server, account(0xa1), without_held_keys(&store), 3);
        fresh.activate(&server);
        fresh.follow_notes(&server);
        assert_eq!(read_notes(&server, &fresh, space).unwrap().len(), 2);
    });
}

/// Contract: a server that hides a delivery row from a holder (here: an
/// older copy of its data) costs a duplicate delivery at most: the holder
/// delivers again, the pass then finds nothing to do and never loops, and
/// the member's key still opens.
///
/// ```text
/// server (copy before the delivery) ──no delivery shown──► alice's pass
/// alice ──delivers again──► second pass: nothing
/// ```
#[test]
fn a_hidden_delivery_costs_a_duplicate_not_a_loop() {
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let old = dir.path().join("old.sqlite");
        let settings = EncryptionSettings::default();
        let store = MemoryStore::new();
        let bob_store = MemoryStore::new();
        let space = {
            let server = secure_authority_with(&settings, Some(&live));
            let alice = device(&server, account(0xa1), store.clone(), 1);
            alice.activate(&server);
            let bob = device(&server, account(0xb0), bob_store.clone(), 2);
            bob.activate(&server);
            let space = new_project(&server, &alice, "plans");
            add_member(&server, &alice, space, &bob.id());
            write_note(&server, &alice, space, SECRET).unwrap();
            pump(&server, &alice.db);
            drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
            for suffix in ["", "-wal", "-shm"] {
                let source = format!("{}{suffix}", live.display());
                if std::path::Path::new(&source).exists() {
                    std::fs::copy(&source, format!("{}{suffix}", old.display())).unwrap();
                }
            }
            assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
            space
        };
        let hiding = secure_authority_with(&settings, Some(&old));
        assert_eq!(count(&hiding, "__e2ee_space_deliveries"), 0);
        let alice = device(&hiding, account(0xa1), store, 3);
        alice.activate(&hiding);
        assert_eq!(deliver(&hiding, &alice, &[], space), 1);
        assert_eq!(deliver(&hiding, &alice, &[], space), 0);
        assert_eq!(count(&hiding, "__e2ee_space_deliveries"), 1);
    });
}

/// `count` member rows of `space` for accounts that have no history, as the
/// authority itself.
fn crowd(server: &Db, by: &Device, space: uuid::Uuid, count: usize) {
    let scope = jazz::db::block_on(by.db.table_identity("projects"))
        .unwrap()
        .unwrap()
        .0;
    for _ in 0..count {
        let cells = BTreeMap::from([
            ("scopeId".to_owned(), Value::Uuid(scope)),
            ("identifier".to_owned(), Value::Uuid(space)),
            ("accountId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
        ]);
        block_on(server.insert(MEMBERS, cells, Default::default())).unwrap();
    }
}

/// Contract: a space with more than 1,000 member accounts is reported once,
/// `space-over-member-limit` with the member count, and the pass goes on.
#[test]
fn a_space_over_the_member_limit_is_reported_once() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, _bob, space) = shared_space(&server);
        crowd(&server, &alice, space, MEMBER_LIMIT);
        for _ in 0..20 {
            pump(&server, &alice.db);
        }
        let events = reports_of(&alice);
        let started = std::time::Instant::now();
        for _ in 0..2 {
            deliver(&server, &alice, &[], space);
        }
        eprintln!(
            "pass over one space of {} members, twice: {:?}",
            MEMBER_LIMIT + 2,
            started.elapsed()
        );
        let over: Vec<usize> = events
            .borrow()
            .iter()
            .filter_map(|report| match report {
                Report::SpaceOverMemberLimit { members, .. } => Some(*members),
                _ => None,
            })
            .collect();
        assert_eq!(over, [MEMBER_LIMIT + 2]);
    });
}

/// Contract: a space in use is watched, and a change of its members runs
/// the pass for it: after another writer adds a member, alice's next
/// `sync_spaces` delivers to him without any call of the pass; a space no subscription uses is not
/// watched, and one that stops being used leaves the watched set.
///
/// ```text
/// alice: use_space ──sync──► (watching)   add bob ──sync──► delivery written
/// ```
#[test]
fn a_member_change_in_a_watched_space_runs_the_pass() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = two_members(&server);
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        let address = space_address(&alice, space);
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        // Nothing uses the space: nothing is watched. Bob, whom alice added
        // herself, is served once that write committed.
        alice.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 1);

        watch.use_space(&address);
        watch.use_space(&address);
        alice.sync_spaces(&server, &mut watch).unwrap();
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        add_member(&server, &alice, space, &carol.id());
        pump(&server, &alice.db);
        alice.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(
            count(&server, "__e2ee_space_deliveries"),
            2,
            "bob and carol"
        );
        let _ = bob;

        // Two subscriptions used it: one ending leaves it watched.
        watch.release_space(&address);
        let dave = device(&server, account(0xd0), MemoryStore::new(), 8);
        dave.activate(&server);
        server_member(&server, &alice, space, &dave.id());
        pump(&server, &alice.db);
        alice.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(
            count(&server, "__e2ee_space_deliveries"),
            3,
            "still used by the other"
        );

        watch.release_space(&address);
        let erin = device(&server, account(0xe0), MemoryStore::new(), 9);
        erin.activate(&server);
        server_member(&server, &alice, space, &erin.id());
        pump(&server, &alice.db);
        alice.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(
            count(&server, "__e2ee_space_deliveries"),
            3,
            "no longer watched"
        );
    });
}

/// Contract: a member this client adds to a space that is not in use is
/// served by the next pass after the write commits, with no call of the pass
/// and no watch: the client's own change of a space's members is a natural
/// moment (protocol §3.4).
///
/// ```text
/// alice: adds carol to a space nothing uses ──sync──► delivery written to carol
/// ```
#[test]
fn a_member_this_client_adds_to_a_space_not_in_use_is_served() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        add_member(&server, &alice, space, &carol.id());
        alice.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 1);
    });
}

/// Contract: a member row this client staged that never commits (its
/// transaction was abandoned or rejected) is waited for only a while, then
/// dropped from the queue, so it costs no query on later turns; while it is
/// waited for the queue holds it.
///
/// ```text
/// alice: stages a member row, abandons it ──sync──► waited for ──sync, after the wait──► dropped
/// ```
#[test]
fn a_member_write_that_never_commits_is_dropped_after_the_wait() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        // What the setup committed is served and off the queue.
        alice.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(alice.client.pending_member_changes(), 0);
        let scope = jazz::db::block_on(alice.db.table_identity("projects"))
            .unwrap()
            .unwrap()
            .0;
        alice.client.advance_settle_clock(std::time::Duration::ZERO);
        let id = jazz::tools::OpenTransactionId::new();
        block_on(alice.db.begin_serializable(id)).unwrap();
        block_on(alice.db.tx_ref(id).insert(
            MEMBERS,
            BTreeMap::from([
                ("scopeId".to_owned(), Value::Uuid(scope)),
                ("identifier".to_owned(), Value::Uuid(space)),
                (
                    "accountId".to_owned(),
                    Value::Uuid(uuid::Uuid::parse_str(&account_id(account(0xc0))).unwrap()),
                ),
            ]),
            Default::default(),
        ))
        .unwrap();
        alice.db.abandon_transaction_handle(id).unwrap();
        assert_eq!(alice.client.pending_member_changes(), 1);

        alice
            .client
            .advance_settle_clock(std::time::Duration::from_secs(29));
        alice.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(alice.client.pending_member_changes(), 1, "still waited for");

        alice
            .client
            .advance_settle_clock(std::time::Duration::from_secs(1));
        alice.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(alice.client.pending_member_changes(), 0, "dropped");
    });
}

/// Contract: one space in use whose address is not a pair of UUIDs does not
/// fail the sync of the others: it is skipped and reported with its error,
/// and the other space is still acquired and served.
///
/// ```text
/// alice: use_space(bad address), use_space(plans) ──sync──► bad: error, plans: delivery to bob
/// ```
#[test]
fn one_bad_space_address_is_reported_and_the_others_still_sync() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, _bob, space) = two_members(&server);
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        let bad = jazz::e2ee::SpaceAddress {
            scope_id: "not a UUID".to_owned(),
            identifier: "nor this".to_owned(),
        };
        watch.use_space(&bad);
        watch.use_space(&space_address(&alice, space));
        let results = alice.sync_spaces(&server, &mut watch).unwrap();
        assert_eq!(results.len(), 2, "{results:?}");
        for (address, state) in &results {
            assert_eq!(state.is_err(), *address == bad, "{address:?}: {state:?}");
        }
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 1);
    });
}

/// Contract: `recoverRotation` writes a successor whose history wraps the
/// newest version alice holds, with a delivery to bob, who then holds the
/// new current version and reads old and new rows.
#[test]
fn a_recovery_rotation_extends_the_chain_and_delivers() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        deliver(&server, &alice, &[&bob], space);
        let written = drive(
            &server,
            &[&alice.db, &bob.db],
            alice
                .client
                .recover_rotation(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        assert_eq!(written, 1);
        assert_eq!(count(&server, "__e2ee_space_successors"), 1);
        write_note(&server, &alice, space, "after").unwrap();
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 2);
    });
}

/// Contract: a member who may not update the space row has her rotation
/// refused, and her write still goes through under the key she holds (a
/// write is never gated on rotation): Jazz permissions alone decide.
///
/// ```text
/// alice (owner) removes carol ──► bob's pass: needs rotation
/// bob ──write──► rotation refused (not an updater) ──► note under the old key
/// ```
#[test]
fn a_refused_rotation_never_blocks_the_write() {
    within(|| {
        let (server, alice, bob, carol, space) = rotation_refusal_members();
        alice.follow_notes(&server);
        bob.follow_notes(&server);
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        deliver(&server, &alice, &[&bob, &carol], space);
        pump(&server, &bob.db);
        bob.acquire(&server, &space_address(&bob, space)).unwrap();

        let member = all(&server, Query::from(MEMBERS))
            .unwrap()
            .into_iter()
            .find(|row| {
                row.cell("accountId")
                    == Some(Value::Uuid(uuid::Uuid::parse_str(&carol.id()).unwrap()))
            })
            .unwrap()
            .row_uuid();
        block_on(alice.db.delete(MEMBERS, member, Default::default())).unwrap();
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        assert_eq!(deliver(&server, &bob, &[&alice], space), 0);
        assert!(
            bob.client
                .needs_rotation(&space_address(&bob, space))
                .unwrap()
        );
        write_note(&server, &bob, space, "from bob").unwrap();
        pump(&server, &bob.db);
        assert_eq!(count(&server, "notes"), 2, "the write went through");
        assert_eq!(
            count(&server, "__e2ee_space_successors"),
            0,
            "rotation refused"
        );
        assert!(
            !bob.client
                .needs_rotation(&space_address(&bob, space))
                .unwrap()
        );
        // Her explicit rotation attempts again (and is refused again).
        let again = bob.rotate(&server, &space_address(&bob, space));
        assert!(again.is_err());
        // The owner's rotation works.
        drive(
            &server,
            &[&alice.db, &bob.db],
            alice
                .client
                .rotate(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        assert_eq!(count(&server, "__e2ee_space_successors"), 1);
    });
}

/// Contract: a rotation that fails before a write is reported with its
/// reason, and the write goes on under the current version as before
/// (protocol §3.5): the app learns that a removed member may still read it.
///
/// ```text
/// alice (owner) removes carol ──► bob's pass: needs rotation
/// bob ──write──► rotation refused ──► report `RotationFailed` ──► note
///                                     under the version carol holds
/// ```
#[test]
fn a_failed_rotation_before_a_write_is_reported_with_its_reason() {
    within(|| {
        let (server, alice, bob, carol, space) = rotation_refusal_members();
        alice.follow_notes(&server);
        bob.follow_notes(&server);
        carol.follow_notes(&server);
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        deliver(&server, &alice, &[&bob, &carol], space);
        pump(&server, &bob.db);
        pump(&server, &carol.db);
        bob.acquire(&server, &space_address(&bob, space)).unwrap();
        assert_eq!(read_notes(&server, &carol, space).unwrap().len(), 1);

        block_on(alice.db.delete(
            MEMBERS,
            member_row(&server, &carol.id()),
            Default::default(),
        ))
        .unwrap();
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        deliver(&server, &bob, &[&alice], space);
        assert!(
            bob.client
                .needs_rotation(&space_address(&bob, space))
                .unwrap()
        );
        let events = reports_of(&bob);
        write_note(&server, &bob, space, "from bob").unwrap();
        pump(&server, &bob.db);

        let failed: Vec<(jazz::e2ee::SpaceAddress, String)> = events
            .borrow()
            .iter()
            .filter_map(|report| match report {
                Report::RotationFailed { space, reason } => Some((space.clone(), reason.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            failed.len(),
            1,
            "one report for the one failed attempt: {failed:?}"
        );
        assert_eq!(failed[0].0, space_address(&bob, space));
        assert!(
            failed[0].1.contains("authorization_denied"),
            "the reason names the refusal: {}",
            failed[0].1
        );
        // What the write does is unchanged: it went through, under the
        // version carol, removed, still holds.
        assert_eq!(count(&server, "notes"), 2, "the write went through");
        assert_eq!(count(&server, "__e2ee_space_successors"), 0);
        pump(&server, &carol.db);
        assert_eq!(read_notes(&server, &carol, space).unwrap().len(), 2);
    });
}

/// A member row of `space` for `account`, written by the authority.
fn server_member(server: &Db, by: &Device, space: uuid::Uuid, account: &str) {
    let scope = jazz::db::block_on(by.db.table_identity("projects"))
        .unwrap()
        .unwrap()
        .0;
    let cells = BTreeMap::from([
        ("scopeId".to_owned(), Value::Uuid(scope)),
        ("identifier".to_owned(), Value::Uuid(space)),
        (
            "accountId".to_owned(),
            Value::Uuid(uuid::Uuid::parse_str(account).unwrap()),
        ),
    ]);
    block_on(server.insert(MEMBERS, cells, Default::default())).unwrap();
}

/// Contract: a writer may seal under a stale held key: after alice rotates,
/// bob, who has not acquired the new version, still writes with the newest
/// one he holds, nothing gates the write on his key state, and alice reads
/// his note (the new key opens the old version through its history).
#[test]
fn a_write_under_a_stale_key_is_not_gated() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        deliver(&server, &alice, &[&bob], space);
        pump(&server, &bob.db);
        bob.acquire(&server, &space_address(&bob, space)).unwrap();
        drive(
            &server,
            &[&alice.db],
            alice
                .client
                .rotate(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        assert_eq!(count(&server, "__e2ee_space_successors"), 1);

        write_note(&server, &bob, space, "stale").unwrap();
        pump(&server, &bob.db);
        pump(&server, &alice.db);
        assert_eq!(count(&server, "notes"), 2);
        assert_eq!(read_notes(&server, &alice, space).unwrap().len(), 2);
    });
}

/// Contract: the store serves the queries the runtime's shared watch really
/// subscribes to (one per system table) from its `(scopeId, identifier, …)`
/// indexes, with no scan of other spaces' rows.
#[test]
fn the_watch_queries_are_served_by_an_index_not_a_scan() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let scope = uuid::Uuid::new_v4().to_string();
        let spaces: Vec<jazz::e2ee::SpaceAddress> = (0..3)
            .map(|_| jazz::e2ee::SpaceAddress {
                scope_id: scope.clone(),
                identifier: uuid::Uuid::new_v4().to_string(),
            })
            .collect();
        let versions = vec![uuid::Uuid::new_v4().to_string()];
        let queries = jazz_e2ee_client::watch_queries(&spaces, &versions);
        assert_eq!(queries.len(), 4);
        let accounts: Vec<String> = (0..3).map(|_| uuid::Uuid::new_v4().to_string()).collect();
        let queries = queries
            .into_iter()
            .chain(jazz_e2ee_client::account_watch_queries(&accounts))
            .collect::<Vec<_>>();
        assert_eq!(queries.len(), 7);
        for (table, query) in queries {
            let (_, plan) = jazz::db::block_on(
                server.explain_query_plan(&server.prepare_query(&query).unwrap(), None),
            )
            .unwrap();
            eprintln!("{table}: {plan}");
            assert!(
                plan.contains("SEARCH") && plan.contains("USING INDEX"),
                "{table}: {plan}"
            );
            assert!(!plan.contains("SCAN"), "{table}: {plan}");
        }
    });
}

/// Contract: the shared watch covers only the deliveries of the current
/// version: after a rotation a delivery of the earlier version that syncs in
/// is not watched and runs no pass.
#[test]
fn deliveries_of_earlier_versions_are_not_watched() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let address = space_address(&alice, space);
        let root = all(&server, Query::from(SPACES)).unwrap().remove(0);
        let first_epoch = root.cell("epochId").unwrap();
        deliver(&server, &alice, &[&bob], space);
        alice.rotate(&server, &address).unwrap();
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        watch.use_space(&address);
        // Opens the watch, then settles on the new current version.
        for _ in 0..3 {
            alice.sync_spaces(&server, &mut watch).unwrap();
            pump(&server, &alice.db);
        }
        let quiet = alice.sync_spaces(&server, &mut watch).unwrap();
        assert!(quiet.is_empty(), "nothing changed");

        // A delivery of the first version, written by the server.
        let scope = jazz::db::block_on(alice.db.table_identity("projects"))
            .unwrap()
            .unwrap()
            .0;
        let account = |text: &str| Value::Uuid(uuid::Uuid::parse_str(text).unwrap());
        block_on(server.insert(
            "__e2ee_space_deliveries",
            BTreeMap::from([
                ("scopeId".to_owned(), Value::Uuid(scope)),
                ("identifier".to_owned(), Value::Uuid(space)),
                ("keyEpochId".to_owned(), first_epoch),
                ("senderAccountId".to_owned(), account(&bob.id())),
                ("recipientAccountId".to_owned(), account(&bob.id())),
                (
                    "recipientAccountEpochId".to_owned(),
                    Value::Uuid(uuid::Uuid::new_v4()),
                ),
                ("envelope".to_owned(), Value::Bytes(vec![1; 8])),
            ]),
            Default::default(),
        ))
        .unwrap();
        pump(&server, &alice.db);
        let after = alice.sync_spaces(&server, &mut watch).unwrap();
        assert!(
            after.is_empty(),
            "an earlier version's delivery is not watched"
        );
    });
}

/// Contract: a delivery pass serves a member added to a space
/// nothing watches, after a quiet pass: the pass reads the authority.
#[test]
fn an_explicit_pass_serves_a_member_the_store_has_not_seen() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let address = space_address(&alice, space);
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        watch.use_space(&address);
        alice.sync_spaces(&server, &mut watch).unwrap();
        watch.release_space(&address);
        alice.sync_spaces(&server, &mut watch).unwrap();
        let _ = bob;
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        server_member(&server, &alice, space, &carol.id());
        assert_eq!(deliver(&server, &alice, &[], space), 1);
    });
}

/// Contract: the shared watch covers the member accounts' roots and
/// sealing keys, so a member whose account (and first sealing key) appears
/// after a quiet pass is delivered to at the next pass the watch runs.
///
/// ```text
/// dave (member, no account yet): pass finds him waiting (quiet)
/// dave founds his account ──► the account watch fires ──► delivery to dave
/// ```
#[test]
fn a_member_whose_sealing_key_appears_after_a_quiet_pass_is_served() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, _bob, space) = shared_space(&server);
        let address = space_address(&alice, space);
        let dave_id = account_id(account(0xd0));
        server_member(&server, &alice, space, &dave_id);
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        watch.use_space(&address);
        for _ in 0..2 {
            alice.sync_spaces(&server, &mut watch).unwrap();
            pump(&server, &alice.db);
        }
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 1, "bob only");

        let dave = device(&server, account(0xd0), MemoryStore::new(), 8);
        dave.activate(&server);
        pump(&server, &alice.db);
        let results = alice.sync_spaces(&server, &mut watch).unwrap();
        assert!(!results.is_empty(), "the account watch saw the new rows");
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 2, "and dave");
    });
}

/// Contract: a public account successor of a member (what a device removal
/// writes) is seen by the watch after a quiet pass: the pass runs again.
#[test]
fn a_members_public_successor_after_a_quiet_pass_runs_the_pass() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let address = space_address(&alice, space);
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        watch.use_space(&address);
        for _ in 0..2 {
            alice.sync_spaces(&server, &mut watch).unwrap();
            pump(&server, &alice.db);
        }
        let quiet = alice.sync_spaces(&server, &mut watch).unwrap();
        assert!(quiet.is_empty());

        let id = |n: u8| Value::Uuid(uuid::Uuid::from_bytes([n; 16]));
        block_on(server.insert(
            "__e2ee_public_account_successors",
            BTreeMap::from([
                (
                    "accountId".to_owned(),
                    Value::Uuid(uuid::Uuid::parse_str(&bob.id()).unwrap()),
                ),
                ("predecessor".to_owned(), id(1)),
                ("epochId".to_owned(), id(2)),
                ("signerId".to_owned(), id(3)),
                ("action".to_owned(), text("remove-device")),
                ("removedDeviceId".to_owned(), id(4)),
                ("retiredRecoveryRootIds".to_owned(), Value::Bytes(vec![0])),
                ("membership".to_owned(), Value::Bytes(vec![0])),
                ("revision".to_owned(), Value::Bytes(vec![0])),
                ("signature".to_owned(), Value::Bytes(vec![0])),
            ]),
            Default::default(),
        ))
        .unwrap();
        pump(&server, &alice.db);
        let seen = alice.sync_spaces(&server, &mut watch).unwrap();
        assert!(!seen.is_empty(), "the successor row ran the pass");
    });
}

/// Contract: a lazy rotation that fails for a reason other than a refusal
/// (here the rotator is no longer a member, so the rotation cannot be made)
/// never blocks the write, and keeps the mark: once she can, the next write
/// rotates.
#[test]
fn a_lazy_rotation_that_could_not_be_made_is_retried_on_the_next_write() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        deliver(&server, &alice, &[&bob], space);
        let member_of = |who: &Device| {
            all(&server, Query::from(MEMBERS))
                .unwrap()
                .into_iter()
                .find(|row| {
                    row.cell("accountId")
                        == Some(Value::Uuid(uuid::Uuid::parse_str(&who.id()).unwrap()))
                })
                .unwrap()
                .row_uuid()
        };
        block_on(
            alice
                .db
                .delete(MEMBERS, member_of(&bob), Default::default()),
        )
        .unwrap();
        pump(&server, &alice.db);
        assert_eq!(deliver(&server, &alice, &[], space), 0);
        let address = space_address(&alice, space);
        assert!(alice.client.needs_rotation(&address).unwrap());

        // Her own member row goes (the authority removes it): a rotation by
        // her cannot be made, and fails without being a refusal.
        let own = member_of(&alice);
        block_on(server.delete(MEMBERS, own, Default::default())).unwrap();
        pump(&server, &alice.db);
        write_note(&server, &alice, space, "unrotated").unwrap();
        pump(&server, &alice.db);
        assert_eq!(count(&server, "__e2ee_space_successors"), 0);
        assert!(
            alice.client.needs_rotation(&address).unwrap(),
            "the mark stays"
        );

        // She is a member again: the next write rotates.
        server_member(&server, &alice, space, &alice.id());
        pump(&server, &alice.db);
        write_note(&server, &alice, space, "rotated").unwrap();
        pump(&server, &alice.db);
        assert_eq!(
            count(&server, "__e2ee_space_successors"),
            1,
            "retried and made"
        );
        assert!(!alice.client.needs_rotation(&address).unwrap());
    });
}

/// Consecutive host writes each get their own rotation after a removal.
#[test]
fn consecutive_host_writes_each_get_their_own_rotation() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        add_member(&server, &alice, space, &carol.id());
        carol.follow_notes(&server);
        assert_eq!(deliver(&server, &alice, &[&bob, &carol], space), 2);
        pump(&server, &carol.db);
        assert_eq!(read_notes(&server, &carol, space).unwrap().len(), 1);

        for (removed, body) in [(&bob, "after bob"), (&carol, "after carol")] {
            let member = member_row(&server, &removed.id());
            block_on(alice.db.delete(MEMBERS, member, Default::default())).unwrap();
            write_note(&server, &alice, space, body).unwrap();
        }
        pump(&server, &alice.db);
        assert_eq!(
            count(&server, "__e2ee_space_successors"),
            2,
            "each removal's host write rotated first"
        );
        pump(&server, &carol.db);
        assert_eq!(
            read_notes(&server, &carol, space).unwrap().len(),
            1,
            "carol reads only the note from before"
        );
    });
}

/// Contract: a device whose account is no longer a member never delivers,
/// though it still holds the key: its pass writes nothing and fails nothing.
#[test]
fn a_holder_that_is_no_longer_a_member_delivers_nothing() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        deliver(&server, &alice, &[&bob], space);
        pump(&server, &bob.db);
        bob.acquire(&server, &space_address(&bob, space)).unwrap();
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        add_member(&server, &alice, space, &carol.id());
        let member = all(&server, Query::from(MEMBERS))
            .unwrap()
            .into_iter()
            .find(|row| {
                row.cell("accountId")
                    == Some(Value::Uuid(uuid::Uuid::parse_str(&bob.id()).unwrap()))
            })
            .unwrap()
            .row_uuid();
        block_on(alice.db.delete(MEMBERS, member, Default::default())).unwrap();
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        let before = count(&server, "__e2ee_space_deliveries");
        assert_eq!(deliver(&server, &bob, &[&alice], space), 0);
        assert_eq!(count(&server, "__e2ee_space_deliveries"), before);
    });
}

/// Contract: at its natural moment a device runs the pass over every space it
/// holds keys of, each reading the authority, so a member added to a space
/// nothing was watching is served by that pass.
///
/// ```text
/// alice: pass (both spaces quiet)     server: carol added to space 1
/// alice: deliver_keys ──real pass of each space──► carol served
/// ```
#[test]
fn a_member_added_to_an_unwatched_space_is_served_at_the_next_natural_moment() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let bob = device(&server, account(0xb0), MemoryStore::new(), 2);
        bob.activate(&server);
        let carol = device(&server, account(0xc0), MemoryStore::new(), 3);
        carol.activate(&server);
        let mut spaces = Vec::new();
        for name in ["one", "two"] {
            let space = new_project(&server, &alice, name);
            add_member(&server, &alice, space, &bob.id());
            write_note(&server, &alice, space, SECRET).unwrap();
            pump(&server, &alice.db);
            spaces.push(space);
        }
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        let first = drive(&server, &[&alice.db], alice.client.deliver_keys(&alice.db)).unwrap();
        assert_eq!(first.into_iter().map(|(_, n)| n.unwrap()).sum::<usize>(), 2);
        let again = drive(&server, &[&alice.db], alice.client.deliver_keys(&alice.db)).unwrap();
        assert_eq!(again.into_iter().map(|(_, n)| n.unwrap()).sum::<usize>(), 0);

        // While nothing watches either space, the server learns of carol.
        server_member(&server, &alice, spaces[0], &carol.id());
        let served = drive(&server, &[&alice.db], alice.client.deliver_keys(&alice.db)).unwrap();
        assert_eq!(
            served.into_iter().map(|(_, n)| n.unwrap()).sum::<usize>(),
            1
        );
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 3);
    });
}

/// Contract: a member whose first sealing key appears after a quiet pass is
/// served by a delivery pass.
#[test]
fn deliver_space_sees_a_sealing_key_that_appeared_after_a_quiet_pass() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, _bob, space) = shared_space(&server);
        let dave_id = account_id(account(0xd0));
        server_member(&server, &alice, space, &dave_id);
        // Bob is served; dave, with no account yet, waits: a quiet pass.
        deliver(&server, &alice, &[], space);
        assert_eq!(deliver(&server, &alice, &[], space), 0);
        assert_eq!(count(&server, "__e2ee_space_deliveries"), 1);

        let dave = device(&server, account(0xd0), MemoryStore::new(), 8);
        dave.activate(&server);
        assert_eq!(deliver(&server, &alice, &[], space), 1, "and dave");
    });
}

/// Contract: a member that holds only an older version (it was removed,
/// the space was rotated, and it was added again) delivers nothing: the
/// version new members lack is one it does not hold.
#[test]
fn a_member_holding_only_an_older_version_does_not_deliver() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        add_member(&server, &alice, space, &carol.id());
        deliver(&server, &alice, &[&bob, &carol], space);
        pump(&server, &bob.db);
        bob.acquire(&server, &space_address(&bob, space)).unwrap();
        let member_of = |who: &Device| {
            all(&server, Query::from(MEMBERS))
                .unwrap()
                .into_iter()
                .find(|row| {
                    row.cell("accountId")
                        == Some(Value::Uuid(uuid::Uuid::parse_str(&who.id()).unwrap()))
                })
                .unwrap()
                .row_uuid()
        };
        // Bob leaves, the space rotates (to carol only), bob returns.
        block_on(
            alice
                .db
                .delete(MEMBERS, member_of(&bob), Default::default()),
        )
        .unwrap();
        pump(&server, &alice.db);
        drive(
            &server,
            &[&alice.db],
            alice
                .client
                .rotate(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        add_member(&server, &alice, space, &bob.id());
        pump(&server, &bob.db);
        let dave = device(&server, account(0xd0), MemoryStore::new(), 8);
        dave.activate(&server);
        server_member(&server, &alice, space, &dave.id());
        let before = count(&server, "__e2ee_space_deliveries");

        // Bob holds the first version only: he has nothing dave lacks.
        assert_eq!(deliver(&server, &bob, &[&alice], space), 0);
        assert_eq!(count(&server, "__e2ee_space_deliveries"), before);
        assert_eq!(
            deliver(&server, &alice, &[&bob], space),
            2,
            "alice serves dave and bob"
        );
    });
}

/// Contract: after a recovery rotation a member that lacks a skipped
/// version gets it from a holder of that version that does not hold the
/// current one: carol, removed while bob (who holds only the first version)
/// made the recovery rotation and added again, holds the skipped version and
/// delivers it to dave and bob, though she lacks the current version.
///
/// ```text
/// K0 ── K1 (rotation, bob not a member) ── K2 (bob's recovery, history K0: K1 skipped)
/// carol holds K1, not K2 ──pass──► K1 to dave and bob
/// ```
#[test]
fn a_holder_of_a_skipped_version_delivers_it_without_the_current_one() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = shared_space(&server);
        let carol = device(&server, account(0xc0), MemoryStore::new(), 7);
        carol.activate(&server);
        add_member(&server, &alice, space, &carol.id());
        deliver(&server, &alice, &[&bob, &carol], space);
        let member_of = |who: &Device| {
            all(&server, Query::from(MEMBERS))
                .unwrap()
                .into_iter()
                .find(|row| {
                    row.cell("accountId")
                        == Some(Value::Uuid(uuid::Uuid::parse_str(&who.id()).unwrap()))
                })
                .unwrap()
                .row_uuid()
        };
        let acquire = |who: &Device| {
            pump(&server, &who.db);
            who.acquire(&server, &space_address(who, space)).unwrap()
        };
        acquire(&bob);
        acquire(&carol);

        // K1: made while bob is not a member, so he never gets it.
        block_on(
            alice
                .db
                .delete(MEMBERS, member_of(&bob), Default::default()),
        )
        .unwrap();
        pump(&server, &alice.db);
        drive(
            &server,
            &[&alice.db],
            alice
                .client
                .rotate(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        pump(&server, &carol.db);
        acquire(&carol);
        add_member(&server, &alice, space, &bob.id());

        // K2: bob, who holds K0 only, recovers; carol is out meanwhile, so
        // she does not get it, and K1 is skipped.
        block_on(
            alice
                .db
                .delete(MEMBERS, member_of(&carol), Default::default()),
        )
        .unwrap();
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        drive(
            &server,
            &[&bob.db, &alice.db],
            bob.client
                .recover_rotation(&bob.db, &space_address(&bob, space)),
        )
        .unwrap();
        add_member(&server, &alice, space, &carol.id());
        pump(&server, &carol.db);
        let dave = device(&server, account(0xd0), MemoryStore::new(), 8);
        dave.activate(&server);
        server_member(&server, &alice, space, &dave.id());
        let before = count(&server, "__e2ee_space_deliveries");

        // Carol holds the skipped K1 but not the current K2: she delivers
        // K1 to dave and to bob, who lack it.
        assert_eq!(deliver(&server, &carol, &[&alice, &bob], space), 2);
        assert_eq!(count(&server, "__e2ee_space_deliveries"), before + 2);
        pump(&server, &dave.db);
        dave.acquire(&server, &space_address(&dave, space)).unwrap();
        let depths: Vec<usize> = dave.client.with_held(|held| {
            held.roots_of(&address(&dave.db, space))
                .into_iter()
                .flat_map(|root| root.held().map(|(depth, _)| depth))
                .collect()
        });
        // The skipped K1 and, through its history, K0; not the current K2.
        assert_eq!(depths, [0, 1], "dave holds the skipped version");
    });
}

/// Alice and bob, both members of one project, each writing a note offline:
/// each creates the space, bob's root reaches the authority first, so
/// alice's creation is lost. Returns the project.
fn offline_creators(
    server: &Db,
    alice: &Device,
    bob: &Device,
    space: uuid::Uuid,
) -> jazz::ids::RowUuid {
    let mut alices = None;
    for (device, body) in [(alice, "alice's note"), (bob, "bob's note")] {
        device.client.set_online(false);
        let handle = block_on(host_write(&device.client, &device.db, async || {
            device
                .db
                .insert("notes", note(space, body), Default::default())
                .await
        }))
        .unwrap();
        if body.starts_with("alice") {
            alices = Some(handle.row_uuid());
        }
        device.client.set_online(true);
    }
    // Bob's root is accepted first, then alice's.
    pump(server, &bob.db);
    pump(server, &alice.db);
    alices.expect("alice wrote a note")
}

/// Contract: two offline creators race; the loser's rows, which every other
/// member reports `invalid-ciphertext`, are repaired once she holds the real
/// key (by delivery) and are then readable by both.
///
/// ```text
/// alice, bob offline: each creates the space and writes a note
/// bob's root first ──► alice's key refused; alice's note unreadable to bob
/// bob delivers K ──► alice repairs (rewrites her note under K) ──► both read both
/// ```
#[test]
fn a_lost_creations_rows_are_repaired_and_readable_by_both() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = two_members(&server);
        offline_creators(&server, &alice, &bob, space);
        assert_eq!(count(&server, SPACES), 2);
        drive(&server, &[&bob.db], bob.client.reconcile(&bob.db)).unwrap();
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(marks(&alice, space), [KeyMark::Refused]);
        // Bob reads only his own note: alice's is under a key he cannot
        // verify.
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 1);

        // Alice waits for the real key: nothing to repair with yet.
        assert_eq!(
            drive(
                &server,
                &[&alice.db],
                alice
                    .client
                    .repair_space(&alice.db, &space_address(&alice, space))
            )
            .unwrap(),
            0
        );
        assert_eq!(deliver(&server, &bob, &[&alice], space), 1);
        pump(&server, &alice.db);
        drive(
            &server,
            &[&alice.db],
            alice
                .client
                .acquire(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        let repaired = drive(
            &server,
            &[&alice.db],
            alice
                .client
                .repair_space(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        assert_eq!(repaired, 1);
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 2);
        assert_eq!(read_notes(&server, &alice, space).unwrap().len(), 2);
        // Nothing left to repair.
        assert_eq!(
            drive(
                &server,
                &[&alice.db],
                alice
                    .client
                    .repair_space(&alice.db, &space_address(&alice, space))
            )
            .unwrap(),
            0
        );
    });
}

/// Contract: a second repairer never overwrites the first: two repairs of
/// one space run at once (as two tabs of one device would), both read the
/// row, the first commits, the second's read changed, so it conflicts, reads
/// again and finds nothing left under the losing key; the rows are rewritten
/// once. A concurrent edit of a cell is left alone the same way, since the
/// repair rewrites only cells still under the losing key.
#[test]
fn a_second_repairer_conflicts_and_overwrites_nothing() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = two_members(&server);
        let alices_note = offline_creators(&server, &alice, &bob, space);
        drive(&server, &[&bob.db], bob.client.reconcile(&bob.db)).unwrap();
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        deliver(&server, &bob, &[&alice], space);
        pump(&server, &alice.db);
        drive(
            &server,
            &[&alice.db],
            alice
                .client
                .acquire(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();

        // Bob rewrites the body of her note first; the repair leaves it.
        write_update(&server, &bob, alices_note, "bob's edit");
        pump(&server, &bob.db);
        pump(&server, &alice.db);
        let address = space_address(&alice, space);
        let (first, second) = drive(
            &server,
            &[&alice.db],
            futures::future::join(
                alice.client.repair_space(&alice.db, &address),
                alice.client.repair_space(&alice.db, &address),
            ),
        );
        assert_eq!(first.unwrap() + second.unwrap(), 1, "rewritten once");
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        let bodies = |device: &Device| {
            let mut bodies: Vec<_> = read_notes(&server, device, space)
                .unwrap()
                .iter()
                .map(|row| row.cell("body"))
                .collect();
            bodies.sort_by_key(|body| format!("{body:?}"));
            bodies
        };
        let expected = vec![Some(text("bob's edit")), Some(text("bob's note"))];
        assert_eq!(bodies(&bob), expected);
        assert_eq!(bodies(&alice), expected);
    });
}

/// Contract: a repair interrupted between batches keeps the losing key and
/// finishes after a restart (protocol §3.2): alice lost the creation race
/// with more rows than one batch rewrites; her repair is stopped once the
/// first batch has committed (the second batch's transaction never
/// commits), her runtime and store are opened again, and the repair
/// rewrites the rest; bob reads every row.
///
/// ```text
/// alice, bob offline: each creates the space; alice writes 120 notes
/// bob's root first ──► alice's key refused; bob delivers K
/// alice repairs: batch 1 (100 rows) commits ──✗ interrupted
/// alice restarts ──► key still held `refused` ──► repair: the other 20
/// ```
#[test]
fn a_repair_interrupted_between_batches_finishes_after_a_restart() {
    const ROWS: usize = 120;
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.sqlite");
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let who = account(0xa1);
        let store = MemoryStore::new();
        let settings = EncryptionSettings::default();
        let open_alice = |store: &MemoryStore| {
            let client = open_account(Rc::new(store.clone()), who, settings.clone(), None);
            let (db, link) = session(&client, &server, who, &settings, 1, Some(&path));
            let alice = Device {
                who,
                client,
                db,
                _link: Some(link),
                live: Default::default(),
            };
            alice.activate(&server);
            alice.follow_notes(&server);
            alice
        };
        let alice = open_alice(&store);
        let bob = device(&server, account(0xb0), MemoryStore::new(), 2);
        bob.activate(&server);
        let space = new_project(&server, &alice, "plans");
        add_member(&server, &alice, space, &bob.id());
        bob.follow_notes(&server);
        pump(&server, &bob.db);

        // Both create the space offline; bob's root reaches the authority
        // first, so alice's creation, and her rows, are lost.
        for (device, rows) in [(&alice, ROWS), (&bob, 1)] {
            device.client.set_online(false);
            for n in 0..rows {
                block_on(host_write(&device.client, &device.db, async || {
                    device
                        .db
                        .insert(
                            "notes",
                            note(space, &format!("note {n}")),
                            Default::default(),
                        )
                        .await
                }))
                .unwrap();
            }
            device.client.set_online(true);
        }
        pump(&server, &bob.db);
        pump(&server, &alice.db);
        drive(&server, &[&bob.db], bob.client.reconcile(&bob.db)).unwrap();
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(marks(&alice, space), [KeyMark::Refused]);
        assert_eq!(deliver(&server, &bob, &[&alice], space), 1);
        pump(&server, &alice.db);
        let address = space_address(&alice, space);
        alice.acquire(&server, &address).unwrap();

        // What bob's store shows him: his note, and alice's rows once they
        // are under the real key.
        let readable = || {
            let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
            all(&bob.db, query).unwrap().len()
        };
        assert_eq!(readable(), 1);

        // The repair runs until bob sees its first batch, and is then
        // dropped, as a closed tab or a killed process stops it.
        {
            let repair = alice.client.repair_space(&alice.db, &address);
            let mut repair = std::pin::pin!(repair);
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            let mut interrupted = false;
            for _ in 0..2_000 {
                if let std::task::Poll::Ready(done) = repair.as_mut().poll(&mut cx) {
                    panic!("the repair finished before it was interrupted: {done:?}");
                }
                block_on(alice.db.tick()).unwrap();
                block_on(server.tick()).unwrap();
                block_on(bob.db.tick()).unwrap();
                if readable() > 1 {
                    interrupted = true;
                    break;
                }
            }
            assert!(interrupted, "the first batch committed");
        }
        assert_eq!(readable(), 1 + 100, "one batch of the repair committed");

        // A restart: the runtime and the store are opened again.
        drop(alice);
        let alice = open_alice(&store);
        assert!(
            marks(&alice, space).contains(&KeyMark::Refused),
            "the losing key is kept: it still opens the rows not yet repaired"
        );
        let rest = drive(
            &server,
            &[&alice.db],
            alice.client.repair_space(&alice.db, &address),
        )
        .unwrap();
        assert_eq!(rest, ROWS - 100);
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        assert_eq!(readable(), 1 + ROWS, "bob reads every row");
        assert_eq!(read_notes(&server, &alice, space).unwrap().len(), 1 + ROWS);
    });
}

/// An update of a note's body through the host write path.
fn write_update(server: &Db, device: &Device, row: jazz::ids::RowUuid, body: &str) {
    drive(
        server,
        &[&device.db],
        host_write(&device.client, &device.db, async || {
            device
                .db
                .update(
                    "notes",
                    row,
                    BTreeMap::from([("body".to_owned(), text(body))]),
                    Default::default(),
                )
                .await
        }),
    )
    .unwrap();
}

fn reset_settings() -> EncryptionSettings {
    EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    }
}

/// [`device`] under `settings`.
fn device_with(
    server: &Db,
    who: AuthorSubject,
    store: MemoryStore,
    node: u8,
    settings: &EncryptionSettings,
) -> Device {
    let client = open_account(Rc::new(store), who, settings.clone(), None);
    let (db, link) = session(&client, server, who, settings, node, None);
    Device {
        who,
        client,
        db,
        _link: Some(link),
        live: Default::default(),
    }
}

/// Bob's second device resets his account (his first device is lost): the
/// account is on a new generation and holds nothing.
fn reset_account_on(server: &Db, device: &Device) {
    assert_eq!(
        drive(
            server,
            &[&device.db],
            device.client.ensure_account(&device.db)
        )
        .unwrap(),
        AccountStatus::AwaitingApproval
    );
    assert_eq!(
        drive(
            server,
            &[&device.db],
            device.client.reset_account(&device.db)
        )
        .unwrap(),
        AccountStatus::Active
    );
}

fn resets(events: &Rc<RefCell<Vec<Report>>>) -> Vec<(String, u64)> {
    events
        .borrow()
        .iter()
        .filter_map(|report| match report {
            Report::AccountReset {
                account,
                generation,
                ..
            } => Some((account.clone(), *generation)),
            _ => None,
        })
        .collect()
}

/// Bob resets after Alice starts watching their space. Until its first
/// encrypted write there is no space root, but Alice still hears the reset once.
#[test]
fn a_watched_rootless_space_reports_a_members_reset() {
    within(|| {
        let settings = reset_settings();
        let server = secure_authority_with(&settings, None);
        let alice = device_with(&server, account(0xa1), MemoryStore::new(), 1, &settings);
        let bob = device_with(&server, account(0xb0), MemoryStore::new(), 2, &settings);
        alice.activate(&server);
        bob.activate(&server);
        let space = new_project(&server, &alice, "plans");
        add_member(&server, &alice, space, &bob.id());
        let address = space_address(&alice, space);
        let other = new_project(&server, &alice, "notes");
        add_member(&server, &alice, other, &bob.id());
        let other_address = space_address(&alice, other);
        let events = reports_of(&alice);
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        watch.use_space(&address);
        watch.use_space(&other_address);
        let mut sync = || {
            pump(&server, &alice.db);
            drive(
                &server,
                &[&alice.db],
                alice.client.sync_spaces(&alice.db, &mut watch),
            )
            .unwrap();
        };
        sync();
        sync();
        assert!(resets(&events).is_empty());
        let bob2 = device_with(&server, bob.who, MemoryStore::new(), 3, &settings);
        reset_account_on(&server, &bob2);
        sync();
        sync();
        let events = events.borrow();
        assert_eq!(events.len(), 2);
        for space in [address, other_address] {
            assert!(events.contains(&Report::AccountReset {
                account: bob.id(),
                space,
                generation: 1
            }));
        }
        assert_eq!(count(&server, SPACES), 0);
    });
}

/// Alice skips Carol's unsupported account ledger and still delivers the
/// space key to Bob, who can then read Alice's note.
#[test]
fn an_unsupported_member_does_not_block_delivery_to_another_member() {
    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.sqlite");
        let carol = account(0xc0);
        {
            let server = secure_authority_with(&settings, Some(&path));
            device(&server, carol, MemoryStore::new(), 3).activate(&server);
        }
        tamper(&path, "__e2ee_account_roots", "ledgerVersion", 2i64);

        let server = secure_authority_with(&settings, Some(&path));
        let (alice, bob, space) = shared_space(&server);
        add_member(&server, &alice, space, &account_id(carol));
        assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
        pump(&server, &bob.db);
        let rows = read_notes(&server, &bob, space).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    });
}

/// Alice skips Carol's account because its root has no device keys and still
/// delivers the space key to Bob, who can then read Alice's note.
#[test]
fn a_member_missing_root_device_keys_does_not_block_delivery_to_another_member() {
    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.sqlite");
        let carol = account(0xc0);
        {
            let server = secure_authority_with(&settings, Some(&path));
            device(&server, carol, MemoryStore::new(), 3).activate(&server);
        }
        let deleted = rusqlite::Connection::open(&path)
            .unwrap()
            .execute("DELETE FROM __e2ee_device_keys", [])
            .unwrap();
        assert!(deleted > 0);

        let server = secure_authority_with(&settings, Some(&path));
        let (alice, bob, space) = shared_space(&server);
        add_member(&server, &alice, space, &account_id(carol));
        assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
        pump(&server, &bob.db);
        let rows = read_notes(&server, &bob, space).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    });
}

/// When Alice's provider cannot verify Bob's device approval, delivery in
/// their rootless space returns the provider error. A retry after the provider
/// recovers finds that the space still has no root.
///
/// ```text
/// bob ──approves──► second device
/// alice ──deliver──► provider failure ──retry──► zero deliveries
/// ```
#[test]
fn a_rootless_delivery_pass_preserves_a_members_replay_failure() {
    use jazz_e2ee::OsRandom;
    use jazz_e2ee::registry::Registry;
    use jazz_e2ee_client::ClientConfig;
    use mechanisms::{FLAKY_SIGN, FlakySigner};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    within(|| {
        let signing = serde_json::json!({ "id": FLAKY_SIGN.id, "version": FLAKY_SIGN.version });
        let settings = EncryptionSettings {
            mechanisms: Some(serde_json::json!({
                "selected": { "signature": signing }, "allowed": { "signature": [signing] }
            })),
            ..Default::default()
        };
        let server = secure_authority_with(&settings, None);
        let make_device = |who, node, fails| {
            let mut registry = Registry::builtin();
            registry
                .register_signer(Arc::new(FlakySigner {
                    fails,
                    record_marker: Some(b"jazz.e2ee.public-device-approval.v1"),
                }))
                .unwrap();
            let client = block_on(E2eeClient::open(ClientConfig {
                store: Rc::new(MemoryStore::new()),
                declarations: declarations_with(settings.clone()),
                account_id: account_id(who),
                registry,
                random: Box::new(OsRandom),
                device_account: None,
            }))
            .unwrap();
            let (db, link) = session(&client, &server, who, &settings, node, None);
            Device {
                who,
                client,
                db,
                _link: Some(link),
                live: Default::default(),
            }
        };
        let alice_fails = Arc::new(AtomicBool::new(false));
        let alice = make_device(account(0xa1), 1, alice_fails.clone());
        let bob_fails = Arc::new(AtomicBool::new(false));
        let bob = make_device(account(0xb0), 2, bob_fails.clone());
        let second = make_device(bob.who, 3, bob_fails);
        alice.activate(&server);
        bob.activate(&server);

        let bob_dbs = [&bob.db, &second.db];
        assert_eq!(
            drive(&server, &bob_dbs, second.client.ensure_account(&second.db)).unwrap(),
            AccountStatus::AwaitingApproval
        );
        let pending = drive(
            &server,
            &bob_dbs,
            bob.client
                .challenge_device(&bob.db, &second.client.device().unwrap().id),
        )
        .unwrap();
        let code = drive(
            &server,
            &bob_dbs,
            second.client.answer_challenge(&second.db),
        )
        .unwrap();
        drive(
            &server,
            &bob_dbs,
            bob.client.approve_device(&bob.db, &pending, &code),
        )
        .unwrap();

        let space = new_project(&server, &alice, "plans");
        add_member(&server, &alice, space, &bob.id());
        let address = space_address(&alice, space);
        let sync = || {
            let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
            watch.use_space(&address);
            alice
                .sync_spaces(&server, &mut watch)
                .unwrap()
                .into_iter()
                .find(|(space, _)| space == &address)
                .unwrap()
                .1
        };
        alice_fails.store(true, Ordering::SeqCst);
        assert!(matches!(
            sync(),
            Err(ClientError::Protocol(ProtocolError::ProviderFailure))
        ));
        alice_fails.store(false, Ordering::SeqCst);
        assert_eq!(sync().unwrap(), SpaceKeyState::NoRoot);
        assert_eq!(count(&server, SPACES), 0);
    });
}

/// Alice delivers to Bob's signed sealing record. Bob's first space read
/// finds the record conflicts with his epoch key. Later space and account
/// reads report neither a duplicate nor a faulty delivery by Alice.
#[test]
fn a_space_read_reports_the_conflicting_sealing_record_once() {
    use jazz_e2ee::mechanisms::key_seal;
    use jazz_e2ee::protocol::derive::account_sealing_key_pair;
    use jazz_e2ee::protocol::domain::AppDomain;
    use jazz_e2ee::registry::Registry;

    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.sqlite");
        let who = account(0xb0);
        let client = open_account(Rc::new(MemoryStore::new()), who, settings.clone(), None);
        {
            let server = secure_authority_with(&settings, Some(&path));
            let (db, _link) = session(&client, &server, who, &settings, 1, None);
            drive(&server, &[&db], client.ensure_account(&db)).unwrap();
        }
        let mut record = client
            .with_account(|a| a.public.account_keys[0].row.clone())
            .unwrap();
        let domain = AppDomain::new(
            &app_id().as_object_id().uuid().to_string(),
            Some(ENVIRONMENT),
        )
        .unwrap()
        .account(client.account_id())
        .unwrap();
        let signing_device = client.device().unwrap().clone();
        let registry = Registry::builtin();
        let signer = registry
            .signer(signing_device.signing.public.mechanism.as_mechanism())
            .unwrap();
        record.public_key = account_sealing_key_pair(&key_seal::Builtin, &[9; 32])
            .unwrap()
            .public
            .bytes;
        record.signature = signer
            .sign(
                &signing_device.signing,
                &record.signed_bytes(&domain).unwrap(),
            )
            .unwrap();
        // Model a co-published, correctly signed record with the wrong public key.
        tamper(
            &path,
            "__e2ee_account_keys",
            "publicKey",
            &record.public_key,
        );
        tamper(&path, "__e2ee_account_keys", "signature", &record.signature);
        let server = secure_authority_with(&settings, Some(&path));
        let (db, link) = session(&client, &server, who, &settings, 2, None);
        let bob = Device {
            who,
            client,
            db,
            _link: Some(link),
            live: Default::default(),
        };
        let alice = device(&server, account(0xa1), MemoryStore::new(), 3);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        add_member(&server, &alice, space, &bob.id());
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
        let events = reports_of(&bob);
        let expected = [Report::ConflictingAccountKey {
            record_id: record.id,
            signer_id: record.signer_id,
            epoch_id: record.epoch_id,
        }];
        let address = space_address(&bob, space);
        for _ in 0..2 {
            assert_eq!(
                drive(&server, &[&bob.db], bob.client.acquire(&bob.db, &address)).unwrap(),
                SpaceKeyState::KeyPending
            );
            assert_eq!(&*events.borrow(), &expected);
        }
        bob.activate(&server);
        assert_eq!(&*events.borrow(), &expected);
    });
}

/// Contract: a reset account reads again after re-delivery, and the other
/// members see `account-reset`, once: bob's account is reset on a second
/// device (his first is lost); alice, a holder, finds him lacking the
/// version for his new generation, delivers it by her pass, and reports the
/// reset once per space, account and generation; bob's new device then
/// reads what alice wrote.
///
/// ```text
/// bob (generation 0) holds K ──reset on a new device──► generation 1, no key
/// alice: pass ──account-reset {bob, 1} (once)──► delivery to bob's new key
/// bob's new device ──acquire──► reads alice's note
/// ```
#[test]
fn a_reset_account_reads_again_after_redelivery_and_members_see_the_reset() {
    within(|| {
        let settings = reset_settings();
        let server = secure_authority_with(&settings, None);
        let alice = device_with(&server, account(0xa1), MemoryStore::new(), 1, &settings);
        alice.activate(&server);
        let bob = device_with(&server, account(0xb0), MemoryStore::new(), 2, &settings);
        bob.activate(&server);
        let space = new_project(&server, &alice, "plans");
        add_member(&server, &alice, space, &bob.id());
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
        let events = reports_of(&alice);
        assert!(resets(&events).is_empty());

        let bob2 = device_with(&server, account(0xb0), MemoryStore::new(), 3, &settings);
        reset_account_on(&server, &bob2);
        pump(&server, &alice.db);
        assert_eq!(
            deliver(&server, &alice, &[&bob2], space),
            1,
            "to his new generation"
        );
        assert_eq!(resets(&events), [(bob2.id(), 1)]);
        assert_eq!(deliver(&server, &alice, &[&bob2], space), 0);
        assert_eq!(resets(&events).len(), 1, "once");

        bob2.follow_notes(&server);
        let rows = read_notes(&server, &bob2, space).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    });
}

/// Contract: a server that hides the reset from a holder (an older copy)
/// costs nothing: the holder sees bob's old generation with its delivery,
/// delivers and reports nothing; once the holder reaches the real state it
/// delivers to the new generation and reports the reset.
#[test]
fn a_hidden_reset_is_seen_once_the_holder_reaches_the_real_state() {
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let old = dir.path().join("old.sqlite");
        let settings = reset_settings();
        let alice_store = MemoryStore::new();
        let (space, bob2) = {
            let server = secure_authority_with(&settings, Some(&live));
            let alice = device_with(&server, account(0xa1), alice_store.clone(), 1, &settings);
            alice.activate(&server);
            let bob = device_with(&server, account(0xb0), MemoryStore::new(), 2, &settings);
            bob.activate(&server);
            let space = new_project(&server, &alice, "plans");
            add_member(&server, &alice, space, &bob.id());
            write_note(&server, &alice, space, SECRET).unwrap();
            pump(&server, &alice.db);
            drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
            assert_eq!(deliver(&server, &alice, &[&bob], space), 1);
            for suffix in ["", "-wal", "-shm"] {
                let source = format!("{}{suffix}", live.display());
                if std::path::Path::new(&source).exists() {
                    std::fs::copy(&source, format!("{}{suffix}", old.display())).unwrap();
                }
            }
            let bob2 = device_with(&server, account(0xb0), MemoryStore::new(), 3, &settings);
            reset_account_on(&server, &bob2);
            (space, bob2.id())
        };
        // The older copy knows no reset.
        let hiding = secure_authority_with(&settings, Some(&old));
        let alice = device_with(&hiding, account(0xa1), alice_store.clone(), 4, &settings);
        alice.activate(&hiding);
        let events = reports_of(&alice);
        assert_eq!(deliver(&hiding, &alice, &[], space), 0);
        assert!(resets(&events).is_empty());
        drop(alice);

        let server = secure_authority_with(&settings, Some(&live));
        let alice = device_with(&server, account(0xa1), alice_store, 5, &settings);
        alice.activate(&server);
        let events = reports_of(&alice);
        assert_eq!(deliver(&server, &alice, &[], space), 1);
        assert_eq!(resets(&events), [(bob2, 1)]);
    });
}

/// Contract: the natural-moment call repairs by itself: once bob has
/// delivered the real key, alice's `deliver_keys` (start, reconnect or the
/// app's trigger) acquires it and rewrites her lost creation's rows, with no
/// call of the repair.
#[test]
fn the_natural_moment_repairs_a_lost_creations_rows() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = two_members(&server);
        offline_creators(&server, &alice, &bob, space);
        drive(&server, &[&bob.db], bob.client.reconcile(&bob.db)).unwrap();
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(deliver(&server, &bob, &[&alice], space), 1);
        pump(&server, &alice.db);
        drive(
            &server,
            &[&alice.db],
            alice
                .client
                .acquire(&alice.db, &space_address(&alice, space)),
        )
        .unwrap();
        drive(&server, &[&alice.db], alice.client.deliver_keys(&alice.db)).unwrap();
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 2);
    });
}

/// Contract: a server that hides bob's delivery from alice (an older copy)
/// only delays the repair: she has no real key, so her repair waits and
/// writes nothing, her rows stay unreadable to bob; once she reaches the
/// real state she repairs them.
#[test]
fn a_hidden_delivery_makes_the_repair_wait() {
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let old = dir.path().join("old.sqlite");
        let settings = EncryptionSettings::default();
        let alice_store = MemoryStore::new();
        let bob_store = MemoryStore::new();
        let space = {
            let server = secure_authority_with(&settings, Some(&live));
            let alice = device(&server, account(0xa1), alice_store.clone(), 1);
            alice.activate(&server);
            let bob = device(&server, account(0xb0), bob_store.clone(), 2);
            bob.activate(&server);
            let space = new_project(&server, &alice, "plans");
            add_member(&server, &alice, space, &bob.id());
            alice.follow_notes(&server);
            bob.follow_notes(&server);
            pump(&server, &bob.db);
            offline_creators(&server, &alice, &bob, space);
            drive(&server, &[&bob.db], bob.client.reconcile(&bob.db)).unwrap();
            drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
            for suffix in ["", "-wal", "-shm"] {
                let source = format!("{}{suffix}", live.display());
                if std::path::Path::new(&source).exists() {
                    std::fs::copy(&source, format!("{}{suffix}", old.display())).unwrap();
                }
            }
            assert_eq!(deliver(&server, &bob, &[&alice], space), 1);
            space
        };

        let hiding = secure_authority_with(&settings, Some(&old));
        let alice = device(&hiding, account(0xa1), alice_store.clone(), 3);
        alice.activate(&hiding);
        let address = space_address(&alice, space);
        drive(
            &hiding,
            &[&alice.db],
            alice.client.acquire(&alice.db, &address),
        )
        .unwrap();
        assert_eq!(
            drive(
                &hiding,
                &[&alice.db],
                alice.client.repair_space(&alice.db, &address)
            )
            .unwrap(),
            0,
            "no real key: the repair waits"
        );
        drop(alice);

        let server = secure_authority_with(&settings, Some(&live));
        let alice = device(&server, account(0xa1), alice_store, 4);
        alice.activate(&server);
        alice.follow_notes(&server);
        alice.acquire(&server, &address).unwrap();
        assert_eq!(
            drive(
                &server,
                &[&alice.db],
                alice.client.repair_space(&alice.db, &address)
            )
            .unwrap(),
            1
        );
        let bob = device(&server, account(0xb0), bob_store, 5);
        bob.activate(&server);
        bob.follow_notes(&server);
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 2);
    });
}

/// Contract: the watch-run pass repairs too: a watched space whose real key
/// arrives has its lost creation's rows rewritten by `sync_spaces`, with no
/// call of the repair.
#[test]
fn a_watched_space_is_repaired_when_its_real_key_arrives() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = two_members(&server);
        offline_creators(&server, &alice, &bob, space);
        drive(&server, &[&bob.db], bob.client.reconcile(&bob.db)).unwrap();
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        let address = space_address(&alice, space);
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        watch.use_space(&address);
        alice.sync_spaces(&server, &mut watch).unwrap();
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 1);

        // Bob delivers; alice's watch sees it and repairs.
        assert_eq!(deliver(&server, &bob, &[&alice], space), 1);
        pump(&server, &alice.db);
        let results = alice.sync_spaces(&server, &mut watch).unwrap();
        assert!(
            results.iter().all(|(_, state)| state.is_ok()),
            "{results:?}"
        );
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 2);
    });
}

/// The test app where notes can be written and read but never updated:
/// whoever wrote a note may not rewrite it, so a repair is refused.
fn locked_notes_schema() -> jazz::schema::JazzSchema {
    use jazz::tools::deployment::artifact::system_tables::{
        derive_e2ee_policies, with_e2ee_fragment,
    };
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
    let locked = allow_all().with_update(Some(PolicyExpr::False), PolicyExpr::False);
    let builder = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("projects")
                .column("name", ColumnType::Text)
                .policies(allow_all()),
        )
        .table(
            TableSchemaBuilder::new("notes")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Text)
                .column("body", ColumnType::Bytea)
                .nullable_column("__e2ee_eq_body", ColumnType::Bytea)
                .nullable_column("__e2ee_eq_note", ColumnType::Bytea)
                .column("note", ColumnType::Bytea)
                .column("attachment", ColumnType::Bytea)
                .column("rank", ColumnType::Bytea)
                .nullable_column("plainNote", ColumnType::Text)
                .policies(locked),
        )
        .table(
            TableSchemaBuilder::new("vaults")
                .column("label", ColumnType::Text)
                .column("secret", ColumnType::Bytea)
                .policies(allow_all()),
        );
    let mut schema = with_e2ee_fragment(&builder.build());
    for (table, policies) in derive_e2ee_policies(
        &schema,
        &declarations().tables,
        &EncryptionSettings::default(),
    )
    .unwrap()
    {
        schema.get_mut(&table).unwrap().policies = policies;
    }
    jazz::schema::JazzSchema::new(&schema).unwrap()
}

fn locked_device(server: &Db, who: AuthorSubject, node: u8) -> Device {
    let client = open_account(
        Rc::new(MemoryStore::new()),
        who,
        EncryptionSettings::default(),
        None,
    );
    let config = jazz::db::DbConfig::new(
        locked_notes_schema(),
        jazz::db::DbIdentity {
            node: jazz::ids::NodeUuid::from_bytes([node; 16]),
            author: who,
        },
    )
    .with_e2ee(client.hooks());
    let db = block_on(Db::open(config)).unwrap();
    let (link, _wire) = link_as(&db, server, who);
    pump(server, &db);
    assert!(jazz::db::block_on(client.prepare(&db)).unwrap());
    Device {
        who,
        client,
        db,
        _link: Some(link),
        live: Default::default(),
    }
}

/// Contract: a creator that may not update the rows it wrote has its repair
/// refused (the application's own rules decide), and the natural moments
/// stop repairing the space for the rest of the run: the first
/// `deliver_keys` reports the refusal, the next does not try again, and the
/// rows stay unreadable to the other member.
#[test]
fn a_refused_repair_stops_at_the_natural_moments() {
    within(|| {
        let authority = block_on(Db::open(jazz::db::DbConfig::new(
            locked_notes_schema(),
            identity(0),
        )))
        .unwrap();
        jazz::db::block_on(authority.set_authority()).unwrap();
        let server = authority;
        let alice = locked_device(&server, account(0xa1), 1);
        alice.activate(&server);
        let bob = locked_device(&server, account(0xb0), 2);
        bob.activate(&server);
        let space = new_project(&server, &alice, "plans");
        add_member(&server, &alice, space, &bob.id());
        alice.follow_notes(&server);
        bob.follow_notes(&server);
        pump(&server, &bob.db);
        offline_creators(&server, &alice, &bob, space);
        drive(&server, &[&bob.db], bob.client.reconcile(&bob.db)).unwrap();
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(deliver(&server, &bob, &[&alice], space), 1);
        pump(&server, &alice.db);
        let address = space_address(&alice, space);
        alice.acquire(&server, &address).unwrap();

        let first = drive(&server, &[&alice.db], alice.client.deliver_keys(&alice.db)).unwrap();
        assert!(
            matches!(
                &first[..],
                [(_, Err(ClientError::Database(error)))]
                    if error.reason == Some(jazz::db::Reason::AuthorizationDenied)
            ),
            "{first:?}"
        );
        let second = drive(&server, &[&alice.db], alice.client.deliver_keys(&alice.db)).unwrap();
        assert!(matches!(&second[..], [(_, Ok(0))]), "{second:?}");
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 1);

        // An explicit call always attempts, and is refused again.
        let explicit = drive(
            &server,
            &[&alice.db],
            alice.client.repair_space(&alice.db, &address),
        );
        assert!(explicit.is_err());
    });
}

/// Contract: reads running while a repair reads cannot drop or add what the
/// repair finds: with app reads of the same rows in flight, the repair
/// still rewrites its one row exactly once.
#[test]
fn concurrent_reads_do_not_disturb_a_repair() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = two_members(&server);
        offline_creators(&server, &alice, &bob, space);
        drive(&server, &[&bob.db], bob.client.reconcile(&bob.db)).unwrap();
        drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();
        assert_eq!(deliver(&server, &bob, &[&alice], space), 1);
        pump(&server, &alice.db);
        let address = space_address(&alice, space);
        alice.acquire(&server, &address).unwrap();

        let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
        let app_read = async || {
            let prepared = alice.db.prepare_query(&query).unwrap();
            alice.db.all(&prepared, local()).await.unwrap().len()
        };
        let (repaired, first, second) = drive(
            &server,
            &[&alice.db],
            futures::future::join3(
                alice.client.repair_space(&alice.db, &address),
                app_read(),
                app_read(),
            ),
        );
        assert_eq!(repaired.unwrap(), 1);
        assert_eq!((first, second), (2, 2));
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        assert_eq!(read_notes(&server, &bob, space).unwrap().len(), 2);
    });
}
