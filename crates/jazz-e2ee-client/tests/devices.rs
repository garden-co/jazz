//! Devices in an account against a real authority with the system tables
//! (`E2EE_PROTOCOL.md` §2.5, §2.8, §2.9): a second device is approved by
//! challenge, response and an approval code; a device is removed and the
//! account rotated; a device is restored with a recovery secret. Every test
//! runs on its own thread with a time limit.

mod common;

use std::rc::Rc;

use common::*;
use jazz::db::{Db, SubscriptionStream, block_on};
use jazz::e2ee::EncryptionSettings;
use jazz::ids::AuthorSubject;
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee_client::PrivateStore as _;
use jazz_e2ee_client::{AccountStatus, ClientError, E2eeClient, MemoryStore, PendingApproval};

/// Runs `test` on its own thread; a hang fails after a minute, saying so.
fn within(test: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        test();
        let _ = done.send(());
    });
    match finished.recv_timeout(std::time::Duration::from_secs(60)) {
        Ok(()) => thread.join().unwrap(),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => thread.join().unwrap(),
        Err(_) => panic!("the test did not finish in a minute: something waited forever"),
    }
}

/// One device of an account: its runtime and its client database.
struct Device {
    notes: Option<SubscriptionStream>,
    client: E2eeClient,
    db: Db,
    _link: jazz::db::Upstream,
}

fn device(
    server: &Db,
    who: AuthorSubject,
    store: Rc<dyn jazz_e2ee_client::PrivateStore>,
    node: u8,
    settings: &EncryptionSettings,
) -> Device {
    let client = open_account(store, who, settings.clone(), None);
    let (db, link) = session(&client, server, who, settings, node, None);
    Device {
        client,
        db,
        notes: None,
        _link: link,
    }
}

fn memory() -> Rc<dyn jazz_e2ee_client::PrivateStore> {
    Rc::new(MemoryStore::new())
}

impl Device {
    fn id(&self) -> String {
        self.client.device().unwrap().id.clone()
    }

    fn status(&self, server: &Db, others: &[&Device]) -> AccountStatus {
        let mut dbs: Vec<&Db> = vec![&self.db];
        dbs.extend(others.iter().map(|device| &device.db));
        drive(server, &dbs, self.client.ensure_account(&self.db)).unwrap()
    }

    fn follow_notes(&mut self, server: &Db) {
        let prepared = self.db.prepare_query(&Query::from("notes")).unwrap();
        self.notes = Some(block_on(self.db.subscribe(&prepared, Default::default())).unwrap());
        pump(server, &self.db);
    }
}

fn notes_of(server: &Db, device: &Device, space: uuid::Uuid) -> Vec<jazz::db::CurrentRow> {
    let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
    drive(
        server,
        &[&device.db],
        device.client.read(&device.db, async || {
            let prepared = device.db.prepare_query(&query)?;
            device.db.all(&prepared, local()).await
        }),
    )
    .unwrap()
}

/// A project alice's first device created, with its creator member row and
/// one note, so the space exists and its key is the account's.
fn space_with_a_note(server: &Db, first: &Device, who: &str) -> uuid::Uuid {
    let space = project(&first.db, "plans");
    pump(server, &first.db);
    let scope = jazz::db::block_on(first.db.table_identity("projects"))
        .unwrap()
        .unwrap()
        .0;
    let member = std::collections::BTreeMap::from([
        ("scopeId".to_owned(), Value::Uuid(scope)),
        ("identifier".to_owned(), Value::Uuid(space)),
        (
            "accountId".to_owned(),
            Value::Uuid(uuid::Uuid::parse_str(who).unwrap()),
        ),
    ]);
    block_on(
        first
            .db
            .insert("__e2ee_members", member, Default::default()),
    )
    .unwrap();
    pump(server, &first.db);
    drive(
        server,
        &[&first.db],
        first.client.write(&first.db, async || {
            first
                .db
                .insert("notes", note(space, SECRET), Default::default())
                .await
        }),
    )
    .unwrap();
    pump(server, &first.db);
    drive(server, &[&first.db], first.client.reconcile(&first.db)).unwrap();
    space
}

/// Approves `second` from `first`: challenge, answer, both codes shown and
/// compared, approval.
fn approve(server: &Db, first: &Device, second: &Device) -> PendingApproval {
    let both = [&first.db, &second.db];
    let pending = drive(
        server,
        &both,
        first.client.challenge_device(&first.db, &second.id()),
    )
    .unwrap();
    let shown = drive(server, &both, second.client.answer_challenge(&second.db)).unwrap();
    let computed = drive(
        server,
        &both,
        first.client.approval_code_of(&first.db, &pending),
    )
    .unwrap()
    .expect("the proof is there");
    assert_eq!(shown, computed, "the two devices show one code");
    drive(
        server,
        &both,
        first.client.approve_device(&first.db, &pending, &shown),
    )
    .unwrap();
    pending
}

/// Contract: a second device of alice's account is approved and reads: it
/// publishes its request, the first device challenges it, it answers with
/// its possession proof and shows an approval code, the first device shows
/// the same code and approves; the second device then acquires the account
/// key and reads, from the rows the server holds, what the first wrote.
///
/// ```text
/// second ──request──► server ◄──challenge── first
/// second ──proof, code──► first (same code) ──approval, delivery──► second
/// second ──account key──► space key (from the root's own envelope) ──► reads
/// ```
#[test]
fn a_second_device_is_approved_and_reads() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        assert_eq!(first.status(&server, &[]), AccountStatus::Active);
        let space = space_with_a_note(&server, &first, &account_id(alice));

        let mut second = device(&server, alice, memory(), 2, &settings);
        assert_eq!(
            second.status(&server, &[&first]),
            AccountStatus::AwaitingApproval
        );
        approve(&server, &first, &second);
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
        second.follow_notes(&server);
        let rows = notes_of(&server, &second, space);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));

        // Keep the database alive to check that dropping the device ends its read.
        assert_eq!(second.db.runtime_stats_for_test().active_subscriptions, 1);
        let db = {
            let second = second;
            second.db
        };
        assert_eq!(db.runtime_stats_for_test().active_subscriptions, 0);
    });
}

/// Contract: a device awaiting approval, which holds no key of the account's
/// space, is not told its write can be tried again. Its check of the space
/// before the write fails (the account is not verified here), which marks
/// the space; a write under the current version still needs that version.
///
/// ```text
/// second (awaiting approval) ──prepare write to space──► not ready
/// ```
#[test]
fn a_device_awaiting_approval_is_not_told_to_write_again() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        assert_eq!(first.status(&server, &[]), AccountStatus::Active);
        let space = space_with_a_note(&server, &first, &account_id(alice));

        let second = device(&server, alice, memory(), 2, &settings);
        assert_eq!(
            second.status(&server, &[&first]),
            AccountStatus::AwaitingApproval
        );
        let address = space_address(&second, space);
        let prepared = drive(
            &server,
            &[&second.db],
            second.client.prepare_space(&second.db, &address),
        );
        assert!(
            !matches!(prepared, Ok(true)),
            "a device without the space's key cannot write under it: {prepared:?}"
        );
    });
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

/// Contract: a removed device cannot read rows written after the rotation:
/// alice's first device removes her second, which holds the space's key;
/// the removal marks the space for rotation without a pass by hand
/// (protocol §2.8), her next write rotates it, and
/// the second device, which keeps what it held, reads the old note and not
/// the new one, and is no longer active.
///
/// ```text
/// first ──remove-device (account rotation)──► second is not active
/// first ──write──► space rotated, note under K'
/// second: old note readable (held key); new note not; acquire fails
/// ```
#[test]
fn a_removed_device_cannot_read_rows_written_after_the_rotation() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let space = space_with_a_note(&server, &first, &account_id(alice));
        let mut second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        second.status(&server, &[&first]);
        second.follow_notes(&server);
        assert_eq!(notes_of(&server, &second, space).len(), 1);

        drive(
            &server,
            &[&first.db, &second.db],
            first.client.remove_device(&first.db, &second.id()),
        )
        .unwrap();
        let address = space_address(&first, space);
        assert!(
            first.client.needs_rotation(&address).unwrap(),
            "the removal itself marks the space: no pass by hand"
        );
        drive(
            &server,
            &[&first.db],
            first.client.write(&first.db, async || {
                first
                    .db
                    .insert("notes", note(space, "after"), Default::default())
                    .await
            }),
        )
        .unwrap();
        pump(&server, &first.db);
        assert_eq!(count(&server, "__e2ee_space_successors"), 1);

        pump(&server, &second.db);
        let held = all(&second.db, Query::from("notes")).unwrap();
        assert_eq!(held.len(), 1, "only the old note opens: {held:?}");
        assert_eq!(held[0].cell("body"), Some(text(SECRET)));
        let refused = drive(
            &server,
            &[&second.db],
            second.client.acquire(&second.db, &address),
        );
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::DeviceNoLongerActive))
            ),
            "{refused:?}"
        );
    });
}

/// What the authority shows of a write made after `removed` was removed:
/// the epoch its body is sealed under, which must be a successor of the
/// space's root sealed to the account epoch the removal made, an epoch whose
/// devices are `remaining` and never `removed`.
fn assert_sealed_without(
    server: &Db,
    writer: &Device,
    space: uuid::Uuid,
    removed: &str,
    remaining: &[String],
) {
    let notes = all(server, Query::from("notes")).unwrap();
    let roots = all(server, Query::from("__e2ee_spaces")).unwrap();
    assert_eq!(roots.len(), 1);
    let root_epoch = match roots[0].cell("epochId") {
        Some(Value::Uuid(id)) => id.to_string(),
        other => panic!("root epoch: {other:?}"),
    };
    // The note written after the removal is the newest one.
    let newest = notes
        .iter()
        .filter(|row| row.cell("projectId") == Some(Value::Uuid(space)))
        .max_by_key(|row| row.row_uuid())
        .expect("the later note is on the server");
    let Some(Value::Bytes(stored)) = newest.cell("body") else {
        panic!("the body is stored as bytes: {newest:?}");
    };
    let sealed = jazz_e2ee::records::read_stored_record(&stored)
        .unwrap()
        .epoch_id
        .to_owned();
    let successors = all(server, Query::from("__e2ee_space_successors")).unwrap();
    assert_ne!(
        sealed,
        root_epoch,
        "the write after the removal is sealed under the version the removed device holds \
         ({} space successors)",
        successors.len()
    );
    let successor = successors
        .iter()
        .find(|row| {
            row.cell("epochId") == Some(Value::Uuid(uuid::Uuid::parse_str(&sealed).unwrap()))
        })
        .expect("the write's epoch is a successor of the space");
    // That version is sealed to the account epoch the removal made.
    let removals = all(server, Query::from("__e2ee_public_account_successors")).unwrap();
    let removal = removals
        .iter()
        .find(|row| {
            row.cell("removedDeviceId")
                == Some(Value::Nullable(Some(Box::new(Value::Uuid(
                    uuid::Uuid::parse_str(removed).unwrap(),
                )))))
                || row.cell("removedDeviceId")
                    == Some(Value::Uuid(uuid::Uuid::parse_str(removed).unwrap()))
        })
        .expect("the removal is an account successor naming the removed device");
    assert_eq!(successor.cell("authorEpochId"), removal.cell("epochId"));
    // And that account epoch's devices are the remaining ones only.
    let devices = drive(server, &[&writer.db], writer.client.devices(&writer.db)).unwrap();
    let mut active: Vec<String> = devices
        .iter()
        .filter(|device| device.status == jazz_e2ee_client::DeviceStatus::Active)
        .map(|device| device.id.clone())
        .collect();
    active.sort();
    let mut expected = remaining.to_vec();
    expected.sort();
    assert_eq!(active, expected);
    assert!(!active.iter().any(|id| id == removed));
}

/// Contract: a runtime opened after the removal, on the remover's own
/// device (an app that signs in again opens a new one), does not seal under
/// the version the removed device holds, even before it has verified the
/// account in this run: its first write rotates the space,
/// to a version sealed to the account epoch the removal made, and the
/// removed device, which kept what it held, cannot read it.
#[test]
fn a_runtime_opened_after_a_removal_rotates_before_its_first_write() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first_store = memory();
        let first = device(&server, alice, first_store.clone(), 1, &settings);
        first.status(&server, &[]);
        let space = space_with_a_note(&server, &first, &account_id(alice));
        let mut second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        second.status(&server, &[&first]);
        second.follow_notes(&server);
        assert_eq!(notes_of(&server, &second, space).len(), 1);

        drive(
            &server,
            &[&first.db, &second.db],
            first.client.remove_device(&first.db, &second.id()),
        )
        .unwrap();
        let first_id = first.id();
        drop(first);
        let again = device(&server, alice, first_store, 3, &settings);
        assert_eq!(again.id(), first_id, "the same device");
        // It writes before it has verified the account in this run.
        drive(
            &server,
            &[&again.db],
            again.client.write(&again.db, async || {
                again
                    .db
                    .insert("notes", note(space, "after"), Default::default())
                    .await
            }),
        )
        .unwrap();
        pump(&server, &again.db);
        assert_sealed_without(&server, &again, space, &second.id(), &[first_id]);

        pump(&server, &second.db);
        let held = all(&second.db, Query::from("notes")).unwrap();
        assert_eq!(held.len(), 1, "only the old note opens: {held:?}");
        assert_eq!(held[0].cell("body"), Some(text(SECRET)));
    });
}

/// Contract: a device of the account that did not make the removal (it
/// runs on while another device removes a third) does not seal under the
/// version the removed device holds: its next write rotates the space.
#[test]
fn another_device_of_the_account_rotates_after_a_removal_it_did_not_make() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let space = space_with_a_note(&server, &first, &account_id(alice));
        let mut second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        second.status(&server, &[&first]);
        second.follow_notes(&server);
        assert_eq!(notes_of(&server, &second, space).len(), 1);
        let third = device(&server, alice, memory(), 3, &settings);
        third.status(&server, &[&first]);
        approve(&server, &first, &third);
        assert_eq!(third.status(&server, &[&first]), AccountStatus::Active);

        drive(
            &server,
            &[&third.db, &first.db, &second.db],
            third.client.remove_device(&third.db, &second.id()),
        )
        .unwrap();
        pump(&server, &first.db);
        assert_eq!(first.status(&server, &[&third]), AccountStatus::Active);
        drive(
            &server,
            &[&first.db],
            first.client.write(&first.db, async || {
                first
                    .db
                    .insert("notes", note(space, "after"), Default::default())
                    .await
            }),
        )
        .unwrap();
        pump(&server, &first.db);
        assert_sealed_without(
            &server,
            &first,
            space,
            &second.id(),
            &[first.id(), third.id()],
        );

        pump(&server, &second.db);
        let held = all(&second.db, Query::from("notes")).unwrap();
        assert_eq!(held.len(), 1, "only the old note opens: {held:?}");
    });
}

/// Alice's space with bob as a member who holds its key on two devices,
/// the second of which has read alice's note; alice's runtime runs its
/// space watch as the host does, one sync per turn.
struct SharedSpace {
    server: Db,
    settings: EncryptionSettings,
    alice_store: Rc<dyn jazz_e2ee_client::PrivateStore>,
    alice: Device,
    bob_first: Device,
    bob_second: Device,
    space: uuid::Uuid,
    watch: jazz_e2ee_client::SpaceWatch,
}

impl SharedSpace {
    fn new() -> Self {
        Self::with_store(memory())
    }

    fn with_store(alice_store: Rc<dyn jazz_e2ee_client::PrivateStore>) -> Self {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let (alice_id, bob_id) = (account(0xa1), account(0xb0));
        let alice = device(&server, alice_id, alice_store.clone(), 1, &settings);
        assert_eq!(alice.status(&server, &[]), AccountStatus::Active);
        let watch = drive(&server, &[&alice.db], alice.client.watch_spaces(&alice.db)).unwrap();
        let space = space_with_a_note(&server, &alice, &account_id(alice_id));
        let bob_first = device(&server, bob_id, memory(), 2, &settings);
        assert_eq!(bob_first.status(&server, &[]), AccountStatus::Active);
        let mut bob_second = device(&server, bob_id, memory(), 3, &settings);
        bob_second.status(&server, &[&bob_first]);
        approve(&server, &bob_first, &bob_second);
        assert_eq!(
            bob_second.status(&server, &[&bob_first]),
            AccountStatus::Active
        );
        let scope = jazz::db::block_on(alice.db.table_identity("projects"))
            .unwrap()
            .unwrap()
            .0;
        let member = std::collections::BTreeMap::from([
            ("scopeId".to_owned(), Value::Uuid(scope)),
            ("identifier".to_owned(), Value::Uuid(space)),
            (
                "accountId".to_owned(),
                Value::Uuid(uuid::Uuid::parse_str(&account_id(bob_id)).unwrap()),
            ),
        ]);
        block_on(
            alice
                .db
                .insert("__e2ee_members", member, Default::default()),
        )
        .unwrap();
        pump(&server, &alice.db);
        let address = space_address(&alice, space);
        drive(
            &server,
            &[&alice.db],
            alice.client.deliver_space(&alice.db, &address),
        )
        .unwrap();
        bob_second.follow_notes(&server);
        assert_eq!(notes_of(&server, &bob_second, space).len(), 1);
        let mut shared = Self {
            server,
            settings,
            alice_store,
            alice,
            bob_first,
            bob_second,
            space,
            watch,
        };
        shared.turns();
        shared
    }

    /// Turns of alice's runtime: a watch sync, then what the server sends.
    fn turns(&mut self) {
        for _ in 0..3 {
            drive(
                &self.server,
                &[&self.alice.db],
                self.alice
                    .client
                    .sync_spaces(&self.alice.db, &mut self.watch),
            )
            .unwrap();
            pump(&self.server, &self.alice.db);
        }
    }

    fn bob_removes_his_second_device(&self) {
        drive(
            &self.server,
            &[&self.bob_first.db, &self.bob_second.db],
            self.bob_first
                .client
                .remove_device(&self.bob_first.db, &self.bob_second.id()),
        )
        .unwrap();
    }

    fn alice_writes(&self, body: &str) {
        drive(
            &self.server,
            &[&self.alice.db],
            self.alice.client.write(&self.alice.db, async || {
                self.alice
                    .db
                    .insert("notes", note(self.space, body), Default::default())
                    .await
            }),
        )
        .unwrap();
        pump(&self.server, &self.alice.db);
    }

    /// The newest note is sealed under a successor of the root, sealed to
    /// bob's account epoch after the removal; bob's removed device opens
    /// only the first note.
    fn assert_removed_device_left_out(&self) {
        let roots = all(&self.server, Query::from("__e2ee_spaces")).unwrap();
        let root_epoch = roots[0].cell("epochId");
        let notes = all(&self.server, Query::from("notes")).unwrap();
        let newest = notes.iter().max_by_key(|row| row.row_uuid()).unwrap();
        let Some(Value::Bytes(stored)) = newest.cell("body") else {
            panic!("the body is stored as bytes: {newest:?}");
        };
        let sealed = jazz_e2ee::records::read_stored_record(&stored)
            .unwrap()
            .epoch_id
            .to_owned();
        let sealed = Some(Value::Uuid(uuid::Uuid::parse_str(&sealed).unwrap()));
        pump(&self.server, &self.bob_second.db);
        let held = all(&self.bob_second.db, Query::from("notes")).unwrap();
        assert_ne!(
            sealed,
            root_epoch,
            "alice's write after bob's device removal is sealed under the version it holds \
             ({} space successors; the removed device opens {} notes)",
            count(&self.server, "__e2ee_space_successors"),
            held.len()
        );
        let successors = all(&self.server, Query::from("__e2ee_space_successors")).unwrap();
        assert!(
            successors.iter().any(|row| row.cell("epochId") == sealed),
            "the write's epoch is a successor of the space"
        );
        let removal = all(
            &self.server,
            Query::from("__e2ee_public_account_successors"),
        )
        .unwrap()
        .into_iter()
        .find(|row| row.cell("action") == Some(text("remove-device")))
        .expect("bob's removal");
        let deliveries = all(&self.server, Query::from("__e2ee_space_deliveries")).unwrap();
        assert!(
            deliveries.iter().any(|row| row.cell("keyEpochId") == sealed
                && row.cell("recipientAccountEpochId") == removal.cell("epochId")),
            "the new version reaches bob at the account epoch the removal made"
        );
        assert_eq!(held.len(), 1, "only the old note opens: {held:?}");
    }
}

/// Contract: a member's removed device cannot read what another member
/// writes after the removal: bob removes his second device while alice's
/// runtime runs on; alice's runtime watches the member accounts of a space
/// it wrote to, so its next write rotates the space first.
#[test]
fn a_members_removed_device_cannot_read_what_another_member_writes_after() {
    within(|| {
        let mut shared = SharedSpace::new();
        shared.bob_removes_his_second_device();
        shared.turns();
        shared.alice_writes("after");
        shared.assert_removed_device_left_out();
    });
}

/// Alice keeps the space open while offline. Bob removes his second device
/// during the gap. Alice's first write after reconnecting must rotate even
/// before her subscriptions have received the removal.
#[test]
fn the_first_write_after_reconnecting_checks_a_subscribed_space() {
    within(|| {
        let mut shared = SharedSpace::new();
        let address = space_address(&shared.alice, shared.space);
        shared.watch.use_space(&address);
        shared.turns();

        shared.alice.client.set_online(false);
        shared.bob_removes_his_second_device();
        shared.alice.client.set_online(true);
        shared.alice_writes("after reconnect");
        assert_eq!(count(&shared.server, "__e2ee_space_successors"), 1);
        shared.assert_removed_device_left_out();
    });
}

/// Alice's member subscription fails while her link stays online. Bob removes
/// a device before she receives that change, so her first write must check
/// the authority and rotate.
#[test]
fn the_first_write_after_losing_a_space_watch_checks_the_subscribed_space() {
    within(|| {
        let mut shared = SharedSpace::new();
        let address = space_address(&shared.alice, shared.space);
        shared.watch.use_space(&address);
        shared.turns();

        let db = &shared.alice.db;
        let (_, members) = jazz_e2ee_client::watch_queries(&[address], &[])
            .into_iter()
            .find(|(table, _)| *table == "__e2ee_members")
            .unwrap();
        let mut witness =
            block_on(db.subscribe(&db.prepare_query(&members).unwrap(), Default::default()))
                .unwrap();
        pump(&shared.server, db);
        while witness.try_next_event().is_some() {}

        // A change in another room makes the shared member query recheck
        // without changing this room's membership.
        let other = project(db, "other room");
        pump(&shared.server, db);
        let member = std::collections::BTreeMap::from([
            (
                "scopeId".to_owned(),
                Value::Uuid(
                    jazz::db::block_on(db.table_identity("projects"))
                        .unwrap()
                        .unwrap()
                        .0,
                ),
            ),
            ("identifier".to_owned(), Value::Uuid(other)),
            (
                "accountId".to_owned(),
                Value::Uuid(account(0xa1).account_id().unwrap().0),
            ),
        ]);
        jazz::db::fail_rechecks_for_test(u32::MAX);
        block_on(db.insert("__e2ee_members", member, Default::default())).unwrap();
        jazz::db::fail_rechecks_for_test(0);
        assert!(matches!(
            witness.try_next_event(),
            Some(jazz::db::SubscriptionEvent::Rejected { .. })
        ));

        shared.bob_removes_his_second_device();
        shared.alice_writes("after losing the member watch");
        assert_eq!(count(&shared.server, "__e2ee_space_successors"), 1);
        shared.assert_removed_device_left_out();
    });
}

/// Alice resumes her device and uses the space before its first write.
enum BeforeWrite {
    Deliver,
    Rotate,
    Read,
}

fn reopen_and_write(first: BeforeWrite) {
    let mut shared = SharedSpace::new();
    shared.alice = device(
        &shared.server,
        account(0xa1),
        shared.alice_store.clone(),
        4,
        &shared.settings,
    );
    assert_eq!(
        shared.alice.status(&shared.server, &[]),
        AccountStatus::Active
    );
    shared.watch = block_on(shared.alice.client.watch_spaces(&shared.alice.db)).unwrap();
    let address = space_address(&shared.alice, shared.space);
    match first {
        BeforeWrite::Deliver => {
            drive(
                &shared.server,
                &[&shared.alice.db],
                shared
                    .alice
                    .client
                    .deliver_space(&shared.alice.db, &address),
            )
            .unwrap();
        }
        BeforeWrite::Rotate => {
            drive(
                &shared.server,
                &[&shared.alice.db],
                shared.alice.client.rotate(&shared.alice.db, &address),
            )
            .unwrap();
        }
        BeforeWrite::Read => {
            let prepared = shared
                .alice
                .db
                .prepare_query(&Query::from("notes"))
                .unwrap();
            let subscription =
                block_on(shared.alice.db.subscribe(&prepared, Default::default())).unwrap();
            shared.watch.use_space(&address);
            shared.turns();
            assert_eq!(
                notes_of(&shared.server, &shared.alice, shared.space).len(),
                1
            );
            drop(subscription);
            shared.watch.release_space(&address);
            shared.turns();
        }
    }
    shared.alice_writes("before");
    shared.turns();
    let before = count(&shared.server, "__e2ee_space_successors");
    shared.bob_removes_his_second_device();
    shared.turns();
    shared.alice_writes("after");
    assert_eq!(count(&shared.server, "__e2ee_space_successors"), before + 1);
}

/// Alice's explicit delivery before her first write still tracks Bob as a holder.
#[test]
fn a_delivery_before_the_first_write_keeps_the_member_watch() {
    within(|| reopen_and_write(BeforeWrite::Deliver));
}

/// Alice's explicit rotation before her first write still tracks Bob as a holder.
#[test]
fn a_rotation_before_the_first_write_keeps_the_member_watch() {
    within(|| reopen_and_write(BeforeWrite::Rotate));
}

/// Closing Alice's subscription before her first write still tracks Bob as a holder.
#[test]
fn a_closed_read_before_the_first_write_keeps_the_member_watch() {
    within(|| reopen_and_write(BeforeWrite::Read));
}

/// Alice writes to a space she wrote to a moment ago, right after her
/// runtime learns that bob was removed from it by someone else, before any
/// pass runs on it: the change she was sent makes the write read the space
/// first, so it rotates rather than seal under the version bob holds.
#[test]
fn a_write_right_after_a_member_leaves_the_space_rotates_first() {
    within(|| {
        let mut shared = SharedSpace::new();
        let address = space_address(&shared.alice, shared.space);
        shared.alice_writes("before");
        shared.turns();
        assert!(!shared.alice.client.needs_rotation(&address).unwrap());
        let before = count(&shared.server, "__e2ee_space_successors");
        let bob = Value::Uuid(uuid::Uuid::parse_str(&account_id(account(0xb0))).unwrap());
        let member = all(&shared.server, Query::from("__e2ee_members"))
            .unwrap()
            .into_iter()
            .find(|row| row.cell("accountId") == Some(bob.clone()))
            .expect("bob's member row")
            .row_uuid();
        block_on(
            shared
                .server
                .delete("__e2ee_members", member, Default::default()),
        )
        .unwrap();
        // Alice's runtime receives the change; no watch sync runs before the write.
        pump(&shared.server, &shared.alice.db);
        shared.alice_writes("after");
        assert_eq!(count(&shared.server, "__e2ee_space_successors"), before + 1);
    });
}

/// Alice stops watching an idle room, then checks its members on first reuse.
/// Bob removes his second device during the idle period.
#[test]
fn a_write_after_the_member_watch_expires_checks_the_room_again() {
    within(|| {
        let mut shared = SharedSpace::new();
        let space = space_address(&shared.alice, shared.space);
        let bob = account_id(account(0xb0));
        assert!(
            shared
                .alice
                .client
                .watched_member_accounts(&space)
                .contains(&("__e2ee_public_account_successors", bob.clone()))
        );
        assert!(
            !shared
                .alice
                .client
                .watched_member_accounts(&space)
                .contains(&("__e2ee_account_keys", bob))
        );
        shared
            .alice
            .client
            .advance_settle_clock(std::time::Duration::from_secs(61));
        shared.turns();
        assert!(
            shared
                .alice
                .client
                .watched_member_accounts(&space)
                .is_empty()
        );
        shared.bob_removes_his_second_device();
        shared.turns();
        let before = count(&shared.server, "__e2ee_space_successors");
        shared.alice_writes("resumed");
        assert_eq!(count(&shared.server, "__e2ee_space_successors"), before + 1);
    });
}

/// After Alice rotates the room, Bob receives a delivery for each new epoch.
/// His cold write and his subscription's pass need only the current delivery.
#[test]
fn cold_writes_and_watch_passes_read_only_the_deliveries_they_need() {
    within(|| {
        let shared = SharedSpace::new();
        let address = space_address(&shared.alice, shared.space);
        let bob = &shared.bob_first;
        let mut watch =
            drive(&shared.server, &[&bob.db], bob.client.watch_spaces(&bob.db)).unwrap();
        let protocol_address = jazz_e2ee::protocol::records::space::SpaceAddress::new(
            &address.scope_id,
            &address.identifier,
        )
        .unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        for account in [None, Some(id.as_str())] {
            let query =
                jazz_e2ee_client::epoch_deliveries_query(&protocol_address, &id, account).unwrap();
            let (_, plan) = jazz::db::block_on(
                shared
                    .server
                    .explain_query_plan(&shared.server.prepare_query(&query).unwrap(), None),
            )
            .unwrap();
            assert!(
                plan.contains("SEARCH") && plan.contains("keyEpochId~") && plan.contains("=?)"),
                "{plan}"
            );
        }
        let mut counts = Vec::new();
        for rotations in [1, 4] {
            for _ in 0..rotations {
                drive(
                    &shared.server,
                    &[&shared.alice.db],
                    shared.alice.client.rotate(&shared.alice.db, &address),
                )
                .unwrap();
            }
            bob.client
                .advance_settle_clock(std::time::Duration::from_secs(61));
            drive(
                &shared.server,
                &[&bob.db],
                bob.client.sync_spaces(&bob.db, &mut watch),
            )
            .unwrap();
            bob.client.take_delivery_reads();
            drive(
                &shared.server,
                &[&bob.db],
                bob.client.write(&bob.db, async || {
                    bob.db
                        .insert("notes", note(shared.space, "resumed"), Default::default())
                        .await
                }),
            )
            .unwrap();
            let write = bob.client.take_delivery_reads();
            watch.use_space(&address);
            let served = drive(
                &shared.server,
                &[&bob.db],
                bob.client.sync_spaces(&bob.db, &mut watch),
            )
            .unwrap();
            assert!(
                served
                    .iter()
                    .any(|(space, state)| space == &address && state.is_ok())
            );
            let pass = bob.client.take_delivery_reads();
            watch.release_space(&address);
            counts.push((write, pass));
        }
        for (write, pass) in &counts {
            assert!(write.0 > 0 && pass.0 > 0, "{counts:?}");
            assert_eq!((write.1, pass.1), (0, 0), "{counts:?}");
        }
        assert_eq!(counts[0], counts[1]);
    });
}

/// Alice evaluates only the current envelopes after each idle period. Past
/// rotations add retained deliveries to the server but no rows to the mark check.
#[test]
fn a_write_checks_the_same_number_of_rows_after_many_rotations() {
    within(|| {
        let mut shared = SharedSpace::new();
        let address = space_address(&shared.alice, shared.space);
        let mut counts = Vec::new();
        for rotations in [1, 5] {
            for _ in 0..rotations {
                drive(
                    &shared.server,
                    &[&shared.alice.db],
                    shared.alice.client.rotate(&shared.alice.db, &address),
                )
                .unwrap();
            }
            shared
                .alice
                .client
                .advance_settle_clock(std::time::Duration::from_secs(61));
            shared.turns();
            shared.alice_writes("resumed");
            let probe = jazz_e2ee_client::successor_probe(
                &jazz_e2ee::protocol::records::space::SpaceAddress::new(
                    &address.scope_id,
                    &address.identifier,
                )
                .unwrap(),
                &uuid::Uuid::new_v4().to_string(),
            )
            .unwrap();
            let (_, plan) = jazz::db::block_on(
                shared
                    .server
                    .explain_query_plan(&shared.server.prepare_query(&probe).unwrap(), None),
            )
            .unwrap();
            assert!(
                plan.contains("SEARCH")
                    && plan.rsplit_once(" AND ").is_some_and(|(_, last)| last
                        .starts_with("predecessor~")
                        && last.ends_with("=?)")),
                "{plan}"
            );
            let prepared = shared.server.prepare_query(&probe).unwrap();
            jazz::db::take_vm_steps_for_test();
            let rows = block_on(shared.server.all(
                &prepared,
                jazz::db::ReadOpts::for_read_tier(jazz::db::ReadTier::LocalOnly),
            ))
            .unwrap();
            let probe_steps = jazz::db::take_vm_steps_for_test();
            assert!(rows.is_empty());
            counts.push((shared.alice.client.write_check_reads().0, probe_steps));
        }
        assert!(counts[0].0 > 0 && counts[0].1 > 0);
        assert_eq!(counts[0], counts[1]);
    });
}

/// Alice's new device acquires her shared room on its first run. Its first
/// write checks recipients even though it opened with no stored history for Bob.
#[test]
fn a_first_session_checks_a_newly_shared_room_before_writing() {
    within(|| {
        let shared = SharedSpace::new();
        let address = space_address(&shared.alice, shared.space);
        shared.bob_removes_his_second_device();
        // The new writer here is Alice's second device. She joins the room
        // on her first run, when Bob's removal has already happened.
        let writer = device(&shared.server, account(0xa1), memory(), 5, &shared.settings);
        writer.status(&shared.server, &[&shared.alice]);
        approve(&shared.server, &shared.alice, &writer);
        assert_eq!(
            writer.status(&shared.server, &[&shared.alice]),
            AccountStatus::Active
        );
        drive(
            &shared.server,
            &[&writer.db],
            writer.client.acquire(&writer.db, &address),
        )
        .unwrap();
        drive(
            &shared.server,
            &[&writer.db],
            writer.client.write(&writer.db, async || {
                writer
                    .db
                    .insert(
                        "notes",
                        note(shared.space, "first write"),
                        Default::default(),
                    )
                    .await
            }),
        )
        .unwrap();
        assert_eq!(count(&shared.server, "__e2ee_space_successors"), 1);
    });
}

/// Alice's second device signs a competing successor of the original root.
/// When it arrives after two rotations, a probe for children of the tip
/// cannot see it. The chain watch makes Alice replay before writing.
#[test]
fn a_competing_successor_of_an_earlier_version_requires_a_fresh_chain_read() {
    within(|| {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let snapshot = dir.path().join("snapshot.sqlite");
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, Some(&live));
        let who = account(0xa1);
        let alice = device(&server, who, memory(), 1, &settings);
        alice.status(&server, &[]);
        let space = space_with_a_note(&server, &alice, &account_id(who));
        let second_store = memory();
        let second = device(&server, who, second_store.clone(), 2, &settings);
        second.status(&server, &[&alice]);
        approve(&server, &alice, &second);
        assert_eq!(second.status(&server, &[&alice]), AccountStatus::Active);
        drop(second);
        copy_database(&live, &snapshot);
        let address = space_address(&alice, space);
        for _ in 0..2 {
            drive(
                &server,
                &[&alice.db],
                alice.client.rotate(&alice.db, &address),
            )
            .unwrap();
        }
        let mut watch = block_on(alice.client.watch_spaces(&alice.db)).unwrap();
        watch.use_space(&address);
        for _ in 0..3 {
            drive(
                &server,
                &[&alice.db],
                alice.client.sync_spaces(&alice.db, &mut watch),
            )
            .unwrap();
            pump(&server, &alice.db);
        }
        drive(
            &server,
            &[&alice.db],
            alice.client.deliver_space(&alice.db, &address),
        )
        .unwrap();
        let before = alice.client.write_check_reads().1;
        let fork = secure_authority_with(&settings, Some(&snapshot));
        let second = device(&fork, who, second_store, 3, &settings);
        assert_eq!(second.status(&fork, &[]), AccountStatus::Active);
        drive(
            &fork,
            &[&second.db],
            second.client.rotate(&second.db, &address),
        )
        .unwrap();
        let table = "__e2ee_space_successors";
        let candidate = all(&fork, Query::from(table)).unwrap().remove(0);
        let cells = candidate
            .cells()
            .filter_map(|(name, value)| value.map(|v| (name.to_owned(), v.clone())))
            .collect();
        block_on(server.upsert(table, candidate.row_uuid(), cells, Default::default())).unwrap();
        pump(&server, &alice.db);
        drive(
            &server,
            &[&alice.db],
            alice.client.write(&alice.db, async || {
                alice
                    .db
                    .insert("notes", note(space, "after the fork"), Default::default())
                    .await
            }),
        )
        .unwrap();
        assert_eq!(alice.client.write_check_reads().1, before + 1);
    });
}

/// A change to Bob's account invalidates only Alice's shared room. Her
/// personal room still accepts a synchronous write with its evaluated key.
#[test]
fn an_unrelated_account_update_does_not_invalidate_another_room() {
    within(|| {
        let mut shared = SharedSpace::new();
        let personal = space_with_a_note(&shared.server, &shared.alice, &account_id(account(0xa1)));
        shared.turns();
        let persist = || {
            block_on(shared.alice.db.insert(
                "notes",
                note(personal, "personal"),
                Default::default(),
            ))
        };
        // Settle the opening subscription snapshots and validate this room.
        drive(
            &shared.server,
            &[&shared.alice.db],
            shared.alice.client.write(&shared.alice.db, async || {
                shared
                    .alice
                    .db
                    .insert("notes", note(personal, "ready"), Default::default())
                    .await
            }),
        )
        .unwrap();
        shared.bob_removes_his_second_device();
        pump(&shared.server, &shared.alice.db);
        persist().unwrap();
    });
}

/// A private-store failure checking Bob's removal is reported with its
/// original reason. Once Alice's storage recovers, the marked room rotates.
#[test]
fn a_rotation_check_reports_storage_failure_even_when_rotation_succeeds() {
    within(|| {
        use jazz_e2ee_client::Report;
        let store = FaultyStore::default();
        let mut shared = SharedSpace::with_store(Rc::new(store.clone()));
        shared
            .alice
            .client
            .advance_settle_clock(std::time::Duration::from_secs(61));
        shared.turns();
        shared.bob_removes_his_second_device();
        let reports = Rc::new(std::cell::RefCell::new(Vec::new()));
        let events = reports.clone();
        let failing = store.failing.clone();
        shared.alice.client.on_report(move |report| {
            if let Report::RotationCheckFailed { reason, .. } = report {
                events.borrow_mut().push(reason.clone());
                failing.set(false);
            }
        });
        store.failing.set(true);
        shared.alice_writes("after the check");
        assert_eq!(
            &*reports.borrow(),
            &["the private store failed: the disk is full"]
        );
        assert_eq!(count(&shared.server, "__e2ee_space_successors"), 1);
    });
}

/// Contract: a space whose member accounts never removed a device or were
/// reset is not read again before a write when only this account's epoch
/// changes without a removal (a recovery root retired): its pass recorded
/// that no removed device can hold its version. A runtime that shares spaces
/// with other accounts otherwise reads a space before its first write to it.
#[test]
fn a_space_whose_members_never_removed_a_device_skips_the_read() {
    within(|| {
        let shared = SharedSpace::new();
        let SharedSpace {
            server,
            settings,
            alice_store,
            alice,
            space,
            ..
        } = shared;
        drop(alice);
        // A new runtime of alice's device, which kept bob's floor: a space it
        // has not read yet is read before its first write.
        let alice = device(&server, account(0xa1), alice_store, 4, &settings);
        assert_eq!(alice.status(&server, &[]), AccountStatus::Active);
        let unread = block_on(
            alice
                .db
                .insert("notes", note(space, "unread"), Default::default()),
        );
        assert!(
            matches!(
                unread.as_ref().map_err(jazz::e2ee::E2eeRefusal::of),
                Err(Some(jazz::e2ee::E2eeRefusal::NeedsKey { .. }))
            ),
            "a space not read in this run is read first: {unread:?}"
        );
        let address = space_address(&alice, space);
        drive(
            &server,
            &[&alice.db],
            alice.client.deliver_space(&alice.db, &address),
        )
        .unwrap();

        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([5; 32]);
        drive(
            &server,
            &[&alice.db],
            alice.client.register_recovery(&alice.db, &secret),
        )
        .unwrap();
        alice.status(&server, &[]);
        let root = alice
            .client
            .with_account(|account| account.state.recovery_roots[0].row.id.clone())
            .unwrap();
        let before = alice
            .client
            .with_account(|account| account.state.epoch_id().to_owned())
            .unwrap();
        drive(
            &server,
            &[&alice.db],
            alice.client.retire_recovery_root(&alice.db, &root),
        )
        .unwrap();
        let after = alice
            .client
            .with_account(|account| account.state.epoch_id().to_owned())
            .unwrap();
        assert_ne!(before, after, "the retirement made a new account epoch");
        // A write no runtime call wraps seals at once: no read was needed.
        block_on(
            alice
                .db
                .insert("notes", note(space, "plain"), Default::default()),
        )
        .unwrap();
        assert_eq!(count(&server, "__e2ee_space_successors"), 0);
    });
}

/// Contract: a write that races the watch update waits for the pass: the
/// update of bob's account has reached alice's database but her runtime
/// has not served it yet when she writes; the write finds it, reads the
/// space and rotates, instead of trusting the evaluation it made before.
#[test]
fn a_write_racing_a_members_device_removal_reads_the_space_first() {
    within(|| {
        let shared = SharedSpace::new();
        shared.bob_removes_his_second_device();
        pump(&shared.server, &shared.alice.db);
        shared.alice_writes("after");
        shared.assert_removed_device_left_out();
    });
}

/// Contract: a device's removal request is completed by another device (the
/// requester never holds the new key), which marks the spaces the requester
/// could open for rotation; the only device's request is refused with
/// `no-other-device`.
#[test]
fn a_removal_request_is_completed_by_another_device() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let alone = drive(
            &server,
            &[&first.db],
            first.client.request_removal(&first.db),
        );
        assert!(
            matches!(
                alone,
                Err(ClientError::Protocol(ProtocolError::Refused(
                    jazz_e2ee::protocol::error::RefusalCode::NoOtherDevice
                )))
            ),
            "{alone:?}"
        );
        let second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        second.status(&server, &[&first]);
        let space = space_with_a_note(&server, &first, &account_id(alice));

        drive(
            &server,
            &[&second.db, &first.db],
            second.client.request_removal(&second.db),
        )
        .unwrap();
        pump(&server, &first.db);
        let done = drive(
            &server,
            &[&first.db, &second.db],
            first.client.complete_removal_requests(&first.db),
        )
        .unwrap();
        assert_eq!(done.len(), 1);
        assert!(done[0].1.is_ok(), "{done:?}");
        assert!(
            first
                .client
                .needs_rotation(&space_address(&first, space))
                .unwrap(),
            "the completed removal marks the space the requester could open"
        );
        pump(&server, &second.db);
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Reenrolled);
    });
}

/// Contract: a wrong approval code approves nothing: the approver refuses
/// it (someone between the devices may have substituted keys), the second
/// device stays unapproved, and the right code then approves.
#[test]
fn a_wrong_approval_code_approves_nothing() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        let both = [&first.db, &second.db];
        let pending = drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();
        let shown = drive(&server, &both, second.client.answer_challenge(&second.db)).unwrap();
        let wrong = if shown == "000000" {
            "000001"
        } else {
            "000000"
        };
        let refused = drive(
            &server,
            &both,
            first.client.approve_device(&first.db, &pending, wrong),
        );
        assert!(
            matches!(refused, Err(ClientError::ApprovalCodeMismatch)),
            "{refused:?}"
        );
        assert_eq!(
            second.status(&server, &[&first]),
            AccountStatus::AwaitingApproval
        );
        drive(
            &server,
            &both,
            first.client.approve_device(&first.db, &pending, &shown),
        )
        .unwrap();
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
        // A replayed proof: approving again is refused by the authority.
        let again = drive(
            &server,
            &both,
            first.client.approve_device(&first.db, &pending, &shown),
        );
        assert!(again.is_err(), "the approval of one challenge is made once");
    });
}

fn answer_once(server: &Db, first: &Device, second: &Device) -> PendingApproval {
    let both = [&first.db, &second.db];
    let pending = drive(
        server,
        &both,
        first.client.challenge_device(&first.db, &second.id()),
    )
    .unwrap();
    drive(server, &both, second.client.answer_challenge(&second.db)).unwrap();
    pending
}

/// A store protected with `key`, holding what `from` holds now: a copy of
/// a device's store taken at this moment (`overwrite` keeps the bytes as
/// they are).
fn store_copy(from: &ProtectedStore, key: u8) -> ProtectedStore {
    let inner = MemoryStore::new();
    for scope in from.inner.scopes() {
        for name in block_on(from.inner.list(&scope)).unwrap() {
            let stored = block_on(from.inner.load(&scope, &name)).unwrap().unwrap();
            inner.overwrite(&scope, &name, stored.bytes);
        }
    }
    ProtectedStore { inner, key }
}

/// Contract: an active device whose proof and challenge rows a server hides
/// refuses to answer and keeps its keys: only a device still waiting for
/// approval enrols again, so a dishonest server cannot make an active device
/// wipe its store.
#[test]
fn an_active_device_whose_proof_is_hidden_refuses_and_is_not_wiped() {
    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let alice = account(0xa1);
        let first_store = Rc::new(MemoryStore::new());
        let second_store = Rc::new(MemoryStore::new());
        let held_before;
        {
            let server = secure_authority_with(&settings, Some(&live));
            let first = device(&server, alice, first_store.clone(), 1, &settings);
            first.status(&server, &[]);
            let second = device(&server, alice, second_store.clone(), 2, &settings);
            second.status(&server, &[&first]);
            approve(&server, &first, &second);
            assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
            held_before = second.id();
            std::mem::forget(first);
            std::mem::forget(second);
        }
        delete_rows(&live, "__e2ee_device_proofs");
        delete_rows(&live, "__e2ee_device_challenges");

        let server = secure_authority_with(&settings, Some(&live));
        let first = device(&server, alice, first_store, 3, &settings);
        first.status(&server, &[]);
        let second = device(&server, alice, second_store, 4, &settings);
        let outcome = drive(
            &server,
            &[&first.db, &second.db],
            second.client.answer_challenge(&second.db),
        );
        assert!(
            matches!(outcome, Err(ClientError::ApprovalAnswered)),
            "{outcome:?}"
        );
        assert_eq!(second.id(), held_before, "not wiped, not enrolled again");
    });
}

/// Contract: a device answers one challenge only. After it answered one,
/// its store says spent: it refuses another challenge, and so does a copy of
/// that store; a copy of the store taken before it answered, restored later
/// with the same protection, sees the server's proof of its own device and
/// refuses; and a copy whose platform protection is bound to the device
/// (here: another key) cannot even be opened, so it answers nothing.
#[test]
fn a_device_answers_one_challenge_only() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let store = ProtectedStore {
            inner: MemoryStore::new(),
            key: 0x5a,
        };
        let second = device(&server, alice, Rc::new(store.clone()), 2, &settings);
        second.status(&server, &[&first]);
        // Taken before the answer; the protection travels with it.
        let before = store_copy(&store, 0x5a);

        answer_once(&server, &first, &second);
        let again = drive(
            &server,
            &[&first.db, &second.db],
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();
        let _ = again;
        let refused = drive(
            &server,
            &[&first.db, &second.db],
            second.client.answer_challenge(&second.db),
        );
        assert!(
            matches!(refused, Err(ClientError::ApprovalAnswered)),
            "{refused:?}"
        );

        // A copy taken after: the document says spent.
        let after = store_copy(&store, 0x5a);
        let copy = device(&server, alice, Rc::new(after), 3, &settings);
        let refused = drive(
            &server,
            &[&first.db, &copy.db],
            copy.client.answer_challenge(&copy.db),
        );
        assert!(
            matches!(refused, Err(ClientError::ApprovalAnswered)),
            "{refused:?}"
        );

        // A restore of the older copy still says unspent, but the server
        // shows the device's earlier proof.
        let restored = device(&server, alice, Rc::new(before), 4, &settings);
        let refused = drive(
            &server,
            &[&first.db, &restored.db],
            restored.client.answer_challenge(&restored.db),
        );
        assert!(
            matches!(refused, Err(ClientError::ApprovalAnswered)),
            "{refused:?}"
        );
        drop(restored);

        // Where the protection is bound to the device, a copy opens nothing.
        let bound = store_copy(&store, 0x99);
        let opened = block_on(E2eeClient::open(jazz_e2ee_client::ClientConfig {
            store: Rc::new(bound),
            declarations: declarations(),
            account_id: account_id(alice),
            registry: jazz_e2ee::registry::Registry::builtin(),
            random: Box::new(jazz_e2ee::OsRandom),
            device_account: None,
        }));
        assert!(
            opened.is_err(),
            "a copy under another protection does not open"
        );
    });
}

/// Contract: a recovery restores access and its root is retired when the
/// recovering device is removed: alice registers the root a recovery secret
/// derives; a new device with only that secret (no other device approves it)
/// is made active and reads; removing that device retires the root in the
/// same rotation, so the secret restores nothing any more.
///
/// ```text
/// first ──register root + delivery──► server
/// new device ──recover (secret)──► active, reads
/// first ──remove it──► root retired; a third device's recovery is refused
/// ```
#[test]
fn a_recovery_restores_access_and_its_root_is_retired_with_the_device() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let space = space_with_a_note(&server, &first, &account_id(alice));
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([7; 32]);
        drive(
            &server,
            &[&first.db],
            first.client.register_recovery(&first.db, &secret),
        )
        .unwrap();
        assert_eq!(
            first
                .client
                .with_account(|account| account.state.recovery_roots.len()),
            Some(0),
            "the verified account predates the root"
        );
        first.status(&server, &[]);
        assert_eq!(
            first
                .client
                .with_account(|account| account.state.recovery_roots.len()),
            Some(1)
        );

        let mut recovered = device(&server, alice, memory(), 2, &settings);
        assert_eq!(
            recovered.status(&server, &[&first]),
            AccountStatus::AwaitingApproval
        );
        let status = drive(
            &server,
            &[&recovered.db, &first.db],
            recovered.client.recover_device(&recovered.db, &secret),
        )
        .unwrap();
        assert_eq!(status, AccountStatus::Active);
        recovered.follow_notes(&server);
        let rows = notes_of(&server, &recovered, space);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));

        first.status(&server, &[&recovered]);
        drive(
            &server,
            &[&first.db, &recovered.db],
            first.client.remove_device(&first.db, &recovered.id()),
        )
        .unwrap();
        assert_eq!(
            first
                .client
                .with_account(|account| account.state.recovery_roots.len()),
            Some(0),
            "the root the removed device held is retired"
        );
        let third = device(&server, alice, memory(), 3, &settings);
        third.status(&server, &[&first]);
        let refused = drive(
            &server,
            &[&third.db, &first.db],
            third.client.recover_device(&third.db, &secret),
        );
        assert!(
            refused.is_err(),
            "the retired root restores nothing: {refused:?}"
        );
    });
}

/// Contract: the host's `recoveryRegister` call honours `automatic`: with a
/// retired root's secret, the start-up registration (`automatic: true`)
/// writes nothing and succeeds, an explicit one (`false`, or absent) is
/// refused with `recovery-root-mismatch`.
///
/// ```text
/// alice registers a secret, then retires its root
/// recoveryRegister { automatic: true }  ──► null, nothing written
/// recoveryRegister { automatic: false } ──► recovery-root-mismatch
/// ```
#[test]
fn the_host_registers_a_retired_root_only_when_automatic() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let client = open_account(memory(), alice, settings.clone(), None);
        let (db, _link) = session(&client, &server, alice, &settings, 1, None);
        let (client, db) = (Rc::new(client), Rc::new(db));
        let runtime = jazz_e2ee_client::host::runtime::Encryption::new(
            jazz_e2ee_client::host::runtime::Built {
                spaces: serde_json::json!({}),
                client: Rc::clone(&client),
                reports: Rc::default(),
            },
            Rc::new(jazz_e2ee_client::host::bridge::JsStore::default()),
            Rc::clone(&db),
        );
        drive(&server, &[&db], client.ensure_account(&db)).unwrap();
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([5; 32]);
        drive(&server, &[&db], client.register_recovery(&db, &secret)).unwrap();
        drive(&server, &[&db], client.ensure_account(&db)).unwrap();
        let root = client
            .with_account(|account| account.state.recovery_roots[0].row.id.clone())
            .unwrap();
        drive(&server, &[&db], client.retire_recovery_root(&db, &root)).unwrap();
        pump(&server, &db);
        let rows = || {
            (
                count(&server, "__e2ee_recovery_roots"),
                count(&server, "__e2ee_recovery_deliveries"),
            )
        };
        let before = rows();
        let register = |automatic: Option<bool>| {
            let text = if automatic == Some(true) {
                secret.format().to_string()
            } else {
                "05".repeat(32)
            };
            let mut args = serde_json::json!({ "secret": text });
            if let Some(automatic) = automatic {
                args["automatic"] = serde_json::json!(automatic);
            }
            drive(
                &server,
                &[&db],
                runtime.call("recoveryRegister".to_owned(), args),
            )
        };

        assert_eq!(register(Some(true)), Ok(serde_json::Value::Null));
        for explicit in [Some(false), None] {
            let refused = register(explicit).unwrap_err();
            assert_eq!(refused, "recovery-root-mismatch: E2EE recovery failed");
        }
        pump(&server, &db);
        assert_eq!(rows(), before, "nothing written");
    });
}

/// lena's device of her device-account account, opened with her identity.
fn lena_device(server: &Db, seed: u8, node: u8) -> (Device, AuthorSubject) {
    let (lena, identity) = device_account(seed);
    let client = open_account(
        memory(),
        lena,
        EncryptionSettings::default(),
        Some(identity),
    );
    (attach(server, client, lena, node), lena)
}

/// Contract: a device account registers the recovery root its account
/// secret derives when it starts (§2.9, "Register"), without the
/// application asking: lena's first device starts, and a new device of hers
/// restores itself with nothing but that secret. Starting again writes
/// nothing; a secret of another identity is refused and writes nothing.
///
/// ```text
/// lena's first device ──start (secret)──► root + delivery
/// lena's first device ──start again──► nothing written
/// lena's new device ──recover (secret)──► active, reads her note
/// lena's first device ──start (mallory's secret)──✗ recovery-root-mismatch
/// ```
#[test]
fn a_device_account_registers_its_recovery_root_at_start() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([0x4f; 32]);
        let (first, lena) = lena_device(&server, 0x4f, 1);
        let status = drive(
            &server,
            &[&first.db],
            first.client.start_device_account(&first.db, &secret),
        )
        .unwrap();
        assert_eq!(status, AccountStatus::Active);
        assert_eq!(
            first
                .client
                .with_account(|account| account.state.recovery_roots.len()),
            Some(1),
            "the verified account lists the root"
        );
        let space = space_with_a_note(&server, &first, &account_id(lena));
        pump(&server, &first.db);
        let rows = || {
            (
                count(&server, "__e2ee_recovery_roots"),
                count(&server, "__e2ee_recovery_deliveries"),
            )
        };
        let before = rows();
        assert_eq!(before, (1, 1));
        drive(
            &server,
            &[&first.db],
            first.client.start_device_account(&first.db, &secret),
        )
        .unwrap();
        pump(&server, &first.db);
        assert_eq!(rows(), before, "a second start writes nothing");

        let mallory = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([0x5f; 32]);
        let refused = drive(
            &server,
            &[&first.db],
            first.client.start_device_account(&first.db, &mallory),
        );
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::Recovery(
                    jazz_e2ee::protocol::error::RecoveryFailure::RootMismatch
                )))
            ),
            "another identity's secret is refused: {refused:?}"
        );
        pump(&server, &first.db);
        assert_eq!(rows(), before, "nothing written for it");

        let (mut second, _) = lena_device(&server, 0x4f, 2);
        assert_eq!(
            drive(
                &server,
                &[&second.db, &first.db],
                second.client.start_device_account(&second.db, &secret),
            )
            .unwrap(),
            AccountStatus::AwaitingApproval,
            "a device awaiting approval registers nothing"
        );
        let status = drive(
            &server,
            &[&second.db, &first.db],
            second.client.recover_device(&second.db, &secret),
        )
        .unwrap();
        assert_eq!(status, AccountStatus::Active);
        second.follow_notes(&server);
        let notes = notes_of(&server, &second, space);
        assert_eq!(notes[0].cell("body"), Some(text(SECRET)));
    });
}

/// Contract: a device account whose secret's root was retired still
/// starts: the start-up registration writes nothing and is not an error
/// (§2.9, "Retiring a derived root"), unlike an explicit registration.
///
/// ```text
/// lena's device ──start (secret)──► root; ──retire it──► server
/// lena's device ──start (secret)──► active, nothing written
/// ```
#[test]
fn a_device_account_starts_after_its_secrets_root_is_retired() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([0x6f; 32]);
        let (first, _) = lena_device(&server, 0x6f, 1);
        drive(
            &server,
            &[&first.db],
            first.client.start_device_account(&first.db, &secret),
        )
        .unwrap();
        let root = first
            .client
            .with_account(|account| account.state.recovery_roots[0].row.id.clone())
            .unwrap();
        drive(
            &server,
            &[&first.db],
            first.client.retire_recovery_root(&first.db, &root),
        )
        .unwrap();
        pump(&server, &first.db);
        let rows = || {
            (
                count(&server, "__e2ee_recovery_roots"),
                count(&server, "__e2ee_recovery_deliveries"),
            )
        };
        let before = rows();
        let status = drive(
            &server,
            &[&first.db],
            first.client.start_device_account(&first.db, &secret),
        )
        .unwrap();
        assert_eq!(status, AccountStatus::Active);
        pump(&server, &first.db);
        assert_eq!(rows(), before, "nothing written");
        assert_eq!(
            first
                .client
                .with_account(|account| account.state.recovery_roots.len()),
            Some(0),
            "the root stays retired"
        );
    });
}

/// Contract: a runtime of an account of the application's own sign-in has
/// no device-account identity to check a secret against: starting it as a
/// device account is refused as unsupported and writes nothing.
#[test]
fn an_app_account_is_not_started_as_a_device_account() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([7; 32]);
        let refused = drive(
            &server,
            &[&first.db],
            first.client.start_device_account(&first.db, &secret),
        );
        assert!(
            matches!(refused, Err(ClientError::Unsupported(_))),
            "{refused:?}"
        );
        pump(&server, &first.db);
        assert_eq!(count(&server, "__e2ee_recovery_roots"), 0);
    });
}

/// Contract: a retired recovery root stays retired. Registering its secret
/// again writes nothing and is refused with `recovery-root-mismatch`, as
/// explicit recovery with it is: a user re-registering an old phrase must
/// not believe they have recovery. (The start-up registration of a device
/// account's own secret, the host runtime's `recoveryRegister` with
/// `automatic`, writes nothing and goes on:
/// `a_device_account_starts_after_its_secrets_root_is_retired`.)
///
/// ```text
/// first ──register root──► server; first ──retire it──► server
/// first ──register the same secret──► refused: recovery-root-mismatch, nothing written
/// new device ──recover (secret)──► refused: recovery-root-mismatch
/// ```
#[test]
fn a_retired_recovery_root_stays_retired() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([5; 32]);
        drive(
            &server,
            &[&first.db],
            first.client.register_recovery(&first.db, &secret),
        )
        .unwrap();
        first.status(&server, &[]);
        let root = first
            .client
            .with_account(|account| account.state.recovery_roots[0].row.id.clone())
            .unwrap();
        drive(
            &server,
            &[&first.db],
            first.client.retire_recovery_root(&first.db, &root),
        )
        .unwrap();
        pump(&server, &first.db);
        let rows = || {
            (
                count(&server, "__e2ee_recovery_roots"),
                count(&server, "__e2ee_recovery_deliveries"),
            )
        };
        let before = rows();

        let again = drive(
            &server,
            &[&first.db],
            first.client.register_recovery(&first.db, &secret),
        );
        assert!(
            matches!(
                again,
                Err(ClientError::Protocol(ProtocolError::Recovery(
                    jazz_e2ee::protocol::error::RecoveryFailure::RootMismatch
                )))
            ),
            "a retired root is not registered again, and the caller is told: {again:?}"
        );
        pump(&server, &first.db);
        assert_eq!(rows(), before, "nothing written");
        assert_eq!(
            first
                .client
                .with_account(|account| account.state.recovery_roots.len()),
            Some(0)
        );

        let second = device(&server, alice, memory(), 2, &settings);
        assert_eq!(
            second.status(&server, &[&first]),
            AccountStatus::AwaitingApproval
        );
        let refused = drive(
            &server,
            &[&second.db, &first.db],
            second.client.recover_device(&second.db, &secret),
        );
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::Recovery(
                    jazz_e2ee::protocol::error::RecoveryFailure::RootMismatch
                )))
            ),
            "{refused:?}"
        );
    });
}

/// Overwrites `column` of the newest row of `table` in the SQLite database
/// at `path` with `value`: a server altering the last device's rows.
fn tamper_newest(path: &std::path::Path, table: &str, column: &str, value: &[u8]) {
    let conn = rusqlite::Connection::open(path).unwrap();
    let stored: String = conn
        .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .find(|name| name == column || name.starts_with(&format!("{column}~")))
        .unwrap();
    let changed = conn
        .execute(
            &format!(
                "UPDATE \"{table}\" SET \"{stored}\" = ?1 WHERE rowid = (SELECT MAX(rowid) FROM \"{table}\")"
            ),
            [value],
        )
        .unwrap();
    assert_eq!(changed, 1);
}

/// Contract: a server that shows the approving device other keys for the
/// requester (the request and keys rows altered together, so they still
/// agree) gets a challenge signed over those keys, and the requester refuses
/// it: `invalid-challenge`; nothing is approved.
#[test]
fn a_server_substituting_the_requesters_keys_is_caught() {
    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let alice = account(0xa1);
        let first_store = Rc::new(MemoryStore::new());
        let second_store = Rc::new(MemoryStore::new());
        let attacker_key = {
            let server = secure_authority_with(&settings, Some(&live));
            let first = device(&server, alice, first_store.clone(), 1, &settings);
            first.status(&server, &[]);
            let second = device(&server, alice, second_store.clone(), 2, &settings);
            second.status(&server, &[&first]);
            first.client.device().unwrap().sealing.public.bytes.clone()
        };
        for table in ["__e2ee_device_requests", "__e2ee_device_keys"] {
            tamper_newest(&live, table, "publicKey", &attacker_key);
        }

        let server = secure_authority_with(&settings, Some(&live));
        let first = device(&server, alice, first_store, 3, &settings);
        first.status(&server, &[]);
        let second = device(&server, alice, second_store, 4, &settings);
        let both = [&first.db, &second.db];
        let _pending = drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();
        let refused = drive(&server, &both, second.client.answer_challenge(&second.db));
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::Refused(
                    jazz_e2ee::protocol::error::RefusalCode::InvalidChallenge
                )))
            ),
            "{refused:?}"
        );
    });
}

/// Contract: an invalid challenge the server shows as the newest of the
/// epoch does not hide a valid one: the requester answers the newest valid
/// challenge, and the approver that made it approves.
#[test]
fn an_invalid_newer_challenge_does_not_hide_a_valid_one() {
    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let alice = account(0xa1);
        let first_store = Rc::new(MemoryStore::new());
        let second_store = Rc::new(MemoryStore::new());
        let pending = {
            let server = secure_authority_with(&settings, Some(&live));
            let first = device(&server, alice, first_store.clone(), 1, &settings);
            first.status(&server, &[]);
            let second = device(&server, alice, second_store.clone(), 2, &settings);
            second.status(&server, &[&first]);
            let both = [&first.db, &second.db];
            let pending = drive(
                &server,
                &both,
                first.client.challenge_device(&first.db, &second.id()),
            )
            .unwrap();
            drive(
                &server,
                &both,
                first.client.challenge_device(&first.db, &second.id()),
            )
            .unwrap();
            pending
        };
        // The newer challenge's signed nonce no longer matches its signature.
        tamper_newest(&live, "__e2ee_device_challenges", "nonce", &[0x5a; 32]);

        let server = secure_authority_with(&settings, Some(&live));
        let first = device(&server, alice, first_store, 3, &settings);
        first.status(&server, &[]);
        let second = device(&server, alice, second_store, 4, &settings);
        let both = [&first.db, &second.db];
        let shown = drive(&server, &both, second.client.answer_challenge(&second.db)).unwrap();
        drive(
            &server,
            &both,
            first.client.approve_device(&first.db, &pending, &shown),
        )
        .unwrap();
    });
}

/// Contract: a challenge of an earlier account epoch is refused, as a
/// replayed one would be: after the account rotates (a third device is
/// removed) the second device's pending challenge names the old epoch, and
/// answering it is `stale-challenge`.
#[test]
fn a_challenge_of_an_earlier_epoch_is_refused() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let third = device(&server, alice, memory(), 3, &settings);
        third.status(&server, &[&first]);
        approve(&server, &first, &third);
        let second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        let both = [&first.db, &second.db, &third.db];
        let _pending = drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();
        drive(
            &server,
            &both,
            first.client.remove_device(&first.db, &third.id()),
        )
        .unwrap();
        let refused = drive(&server, &both, second.client.answer_challenge(&second.db));
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::Refused(
                    jazz_e2ee::protocol::error::RefusalCode::StaleChallenge
                )))
            ) || matches!(refused, Err(ClientError::NoChallenge)),
            "{refused:?}"
        );
    });
}

/// Contract: a possession proof whose wrap names a mechanism this client
/// does not route is an unsupported capability, never an invalid proof: the
/// approver reports `account-key-envelope` and approves nothing.
#[test]
fn a_proof_of_an_unsupported_mechanism_is_unsupported_not_invalid() {
    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let alice = account(0xa1);
        let first_store = Rc::new(MemoryStore::new());
        let second_store = Rc::new(MemoryStore::new());
        let pending = {
            let server = secure_authority_with(&settings, Some(&live));
            let first = device(&server, alice, first_store.clone(), 1, &settings);
            first.status(&server, &[]);
            let second = device(&server, alice, second_store, 2, &settings);
            second.status(&server, &[&first]);
            let pending = answer_once(&server, &first, &second);
            std::mem::forget(first);
            pending
        };
        let unsupported = jazz_e2ee::envelope::encode(
            jazz_e2ee::envelope::Mechanism::new("example.unsupported", 1),
            &[],
        )
        .unwrap();
        tamper_newest(&live, "__e2ee_device_proofs", "proof", &unsupported);

        let server = secure_authority_with(&settings, Some(&live));
        let first = device(&server, alice, first_store, 3, &settings);
        first.status(&server, &[]);
        let code = drive(
            &server,
            &[&first.db],
            first.client.approval_code_of(&first.db, &pending),
        )
        .unwrap()
        .expect("the proof is there");
        let refused = drive(
            &server,
            &[&first.db],
            first.client.approve_device(&first.db, &pending, &code),
        );
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::Unsupported(
                    jazz_e2ee::protocol::error::UnsupportedCapability::AccountKeyEnvelope
                )))
            ),
            "{refused:?}"
        );
    });
}

/// Contract: the approval nonce is marked spent in the private store before
/// the proof reveals it: after a device answered, its stored device document
/// says spent, and before it answered the document holds the nonce.
#[test]
fn the_stored_document_says_spent_after_an_answer() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let store = Rc::new(MemoryStore::new());
        let second = device(&server, alice, store.clone(), 2, &settings);
        second.status(&server, &[&first]);
        let scope = format!(
            r#"["{}","{ENVIRONMENT}","{}"]"#,
            app_id().as_object_id().uuid(),
            account_id(alice)
        );
        let read = || {
            let stored = block_on(jazz_e2ee_client::PrivateStore::load(
                &*store, &scope, "device",
            ))
            .unwrap()
            .unwrap();
            jazz_e2ee::protocol::content::local::decode_device_document(&stored.bytes)
                .unwrap()
                .unwrap()
                .approval
        };
        assert!(matches!(
            read(),
            jazz_e2ee::protocol::content::local::StoredApproval::Unspent { .. }
        ));
        answer_once(&server, &first, &second);
        assert!(matches!(
            read(),
            jazz_e2ee::protocol::content::local::StoredApproval::Spent
        ));
    });
}

/// A store that refuses to save the device document while `fail` is set.
struct FailingSaves {
    inner: MemoryStore,
    fail: Rc<std::cell::Cell<bool>>,
}

type Boxed<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + 'a>>;

impl jazz_e2ee_client::PrivateStore for FailingSaves {
    fn load<'a>(
        &'a self,
        scope: &'a str,
        doc: &'a str,
    ) -> Boxed<'a, Result<Option<jazz_e2ee_client::Stored>, jazz_e2ee_client::StoreError>> {
        jazz_e2ee_client::PrivateStore::load(&self.inner, scope, doc)
    }
    fn open_scope<'a>(
        &'a self,
        scope: &'a str,
    ) -> Boxed<'a, Result<u64, jazz_e2ee_client::StoreError>> {
        jazz_e2ee_client::PrivateStore::open_scope(&self.inner, scope)
    }
    fn save<'a>(
        &'a self,
        scope: &'a str,
        incarnation: u64,
        doc: &'a str,
        bytes: &'a [u8],
        expected: Option<u64>,
    ) -> Boxed<'a, Result<u64, jazz_e2ee_client::StoreError>> {
        if self.fail.get() && doc == "device" {
            return Box::pin(async {
                Err(jazz_e2ee_client::StoreError::Failed("disk full".to_owned()))
            });
        }
        jazz_e2ee_client::PrivateStore::save(&self.inner, scope, incarnation, doc, bytes, expected)
    }
    fn list<'a>(
        &'a self,
        scope: &'a str,
    ) -> Boxed<'a, Result<Vec<String>, jazz_e2ee_client::StoreError>> {
        jazz_e2ee_client::PrivateStore::list(&self.inner, scope)
    }
    fn delete<'a>(&'a self, scope: &'a str) -> Boxed<'a, Result<(), jazz_e2ee_client::StoreError>> {
        jazz_e2ee_client::PrivateStore::delete(&self.inner, scope)
    }
    fn protect<'a>(
        &'a self,
        secret: &'a [u8],
    ) -> Boxed<'a, Result<Vec<u8>, jazz_e2ee_client::StoreError>> {
        jazz_e2ee_client::PrivateStore::protect(&self.inner, secret)
    }
    fn unprotect<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> Boxed<'a, Result<jazz_e2ee::Zeroizing<Vec<u8>>, jazz_e2ee_client::StoreError>> {
        jazz_e2ee_client::PrivateStore::unprotect(&self.inner, blob)
    }
}

/// Contract: the nonce is marked spent, and the store confirms, before the
/// proof is published: when the store cannot save, the answer fails and no
/// proof row reaches the authority (the nonce stays unrevealed); once the
/// store saves again the answer works.
#[test]
fn a_store_that_cannot_save_publishes_no_proof() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let fail = Rc::new(std::cell::Cell::new(false));
        let store = Rc::new(FailingSaves {
            inner: MemoryStore::new(),
            fail: fail.clone(),
        });
        let second = device(&server, alice, store, 2, &settings);
        second.status(&server, &[&first]);
        let both = [&first.db, &second.db];
        drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();

        fail.set(true);
        let refused = drive(&server, &both, second.client.answer_challenge(&second.db));
        assert!(matches!(refused, Err(ClientError::Store(_))), "{refused:?}");
        pump(&server, &second.db);
        assert_eq!(
            count(&server, "__e2ee_device_proofs"),
            0,
            "no proof was published"
        );

        fail.set(false);
        drive(&server, &both, second.client.answer_challenge(&second.db)).unwrap();
        pump(&server, &second.db);
        assert_eq!(count(&server, "__e2ee_device_proofs"), 1);
    });
}

fn delete_rows(path: &std::path::Path, table: &str) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute(&format!("DELETE FROM \"{table}\""), [])
        .unwrap();
}

/// Contract: a device whose stored document says spent, and whose proof the
/// authority never showed (here: the proof rows were removed from the
/// server's database, as a restart between the mark and the publish, or a
/// rollback, would leave it), answers nothing: its next answer wipes it and
/// it enrols again as a new device, which is then approved. The spent nonce
/// was revealed nowhere.
#[test]
fn a_reloaded_device_whose_proof_never_reached_the_authority_enrols_again() {
    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let alice = account(0xa1);
        let first_store = Rc::new(MemoryStore::new());
        let second_store = Rc::new(MemoryStore::new());
        {
            let server = secure_authority_with(&settings, Some(&live));
            let first = device(&server, alice, first_store.clone(), 1, &settings);
            first.status(&server, &[]);
            let second = device(&server, alice, second_store.clone(), 2, &settings);
            second.status(&server, &[&first]);
            answer_once(&server, &first, &second);
            std::mem::forget(first);
            std::mem::forget(second);
        }
        delete_rows(&live, "__e2ee_device_proofs");

        let server = secure_authority_with(&settings, Some(&live));
        let first = device(&server, alice, first_store, 3, &settings);
        first.status(&server, &[]);
        let second = device(&server, alice, second_store, 4, &settings);
        let stranded = second.id();
        let both = [&first.db, &second.db];
        assert_eq!(count(&server, "__e2ee_device_proofs"), 0);
        drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();
        let outcome = drive(&server, &both, second.client.answer_challenge(&second.db));
        assert!(
            matches!(outcome, Err(ClientError::NoChallenge)),
            "{outcome:?}"
        );
        pump(&server, &second.db);
        assert_eq!(count(&server, "__e2ee_device_proofs"), 0);
        assert_ne!(second.id(), stranded, "a new device");

        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
    });
}

/// A random source that fails the next draw it is asked for once armed.
struct FlakyRandom {
    fail_next: Rc<std::cell::Cell<bool>>,
}

impl jazz_e2ee::RandomSource for FlakyRandom {
    fn fill(&mut self, dest: &mut [u8]) -> jazz_e2ee::Result<()> {
        if self.fail_next.replace(false) {
            return Err(jazz_e2ee::Error::RandomUnavailable);
        }
        jazz_e2ee::RandomSource::fill(&mut jazz_e2ee::OsRandom, dest)
    }
}

/// Contract: one removal request that fails does not block the others: of
/// two devices asking to leave, the first rotation fails (the random source
/// refuses once), the second completes, and the call reports each.
#[test]
fn one_failing_removal_request_does_not_block_the_others() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let fail_next = Rc::new(std::cell::Cell::new(false));
        let client = block_on(E2eeClient::open(jazz_e2ee_client::ClientConfig {
            store: memory(),
            declarations: declarations_with(settings.clone()),
            account_id: account_id(alice),
            registry: jazz_e2ee::registry::Registry::builtin(),
            random: Box::new(FlakyRandom {
                fail_next: fail_next.clone(),
            }),
            device_account: None,
        }))
        .unwrap();
        let (db, link) = session(&client, &server, alice, &settings, 1, None);
        let first = Device {
            client,
            db,
            notes: None,
            _link: link,
        };
        first.status(&server, &[]);
        let second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        second.status(&server, &[&first]);
        let third = device(&server, alice, memory(), 3, &settings);
        third.status(&server, &[&first, &second]);
        let all = [&first.db, &second.db, &third.db];
        let pending = drive(
            &server,
            &all,
            first.client.challenge_device(&first.db, &third.id()),
        )
        .unwrap();
        let shown = drive(&server, &all, third.client.answer_challenge(&third.db)).unwrap();
        drive(
            &server,
            &all,
            first.client.approve_device(&first.db, &pending, &shown),
        )
        .unwrap();
        third.status(&server, &[&first, &second]);
        first.status(&server, &[&second, &third]);

        drive(&server, &all, second.client.request_removal(&second.db)).unwrap();
        drive(&server, &all, third.client.request_removal(&third.db)).unwrap();
        pump(&server, &first.db);

        fail_next.set(true);
        let results = drive(
            &server,
            &all,
            first.client.complete_removal_requests(&first.db),
        )
        .unwrap();
        assert_eq!(results.len(), 2, "{results:?}");
        assert_eq!(
            results
                .iter()
                .filter(|(_, outcome)| outcome.is_err())
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|(_, outcome)| outcome.is_ok())
                .count(),
            1
        );
        let removed = results
            .iter()
            .find(|(_, outcome)| outcome.is_ok())
            .unwrap()
            .0
            .clone();
        let failed = results
            .iter()
            .find(|(_, outcome)| outcome.is_err())
            .unwrap()
            .0
            .clone();
        first.status(&server, &[&second, &third]);
        let active = first
            .client
            .with_account(|account| account.state.active.clone())
            .unwrap();
        assert!(
            !active.contains(&removed) && active.contains(&failed),
            "{active:?}"
        );

        // The failed one is tried again at the next call.
        let again = drive(
            &server,
            &all,
            first.client.complete_removal_requests(&first.db),
        )
        .unwrap();
        assert_eq!(again.len(), 1);
        assert!(again[0].1.is_ok(), "{again:?}");
    });
}

/// Contract: a proof kept for the device that was re-enrolled is not
/// published again: the device answered, its proof reached the authority but
/// the call never returned, the device was approved and later removed and
/// enrolled again; the new device answers a new challenge with a new proof.
#[test]
fn a_re_enrolled_device_answers_with_a_new_proof() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        let both = [&first.db, &second.db];
        let pending = drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();
        let old_id = second.id();

        // The proof reaches the authority; the call is dropped before it
        // hears so.
        {
            let mut answer = std::pin::pin!(second.client.answer_challenge(&second.db));
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            let mut reached = false;
            for _ in 0..2_000 {
                if let std::task::Poll::Ready(value) = answer.as_mut().poll(&mut cx) {
                    panic!("the call returned before it could be dropped: {value:?}");
                }
                block_on(second.db.tick()).unwrap();
                block_on(first.db.tick()).unwrap();
                block_on(server.tick()).unwrap();
                if count(&server, "__e2ee_device_proofs") == 1 {
                    reached = true;
                    break;
                }
            }
            assert!(reached, "the proof never reached the authority");
        }
        let code = drive(
            &server,
            &both,
            first.client.approval_code_of(&first.db, &pending),
        )
        .unwrap()
        .expect("the proof is there");
        drive(
            &server,
            &both,
            first.client.approve_device(&first.db, &pending, &code),
        )
        .unwrap();
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
        drive(
            &server,
            &both,
            first.client.remove_device(&first.db, &second.id()),
        )
        .unwrap();
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Reenrolled);
        assert_ne!(second.id(), old_id);

        let pending = drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();
        let shown = drive(&server, &both, second.client.answer_challenge(&second.db)).unwrap();
        let computed = drive(
            &server,
            &both,
            first.client.approval_code_of(&first.db, &pending),
        )
        .unwrap()
        .expect("the new device's proof is there");
        assert_eq!(shown, computed);
        drive(
            &server,
            &both,
            first.client.approve_device(&first.db, &pending, &shown),
        )
        .unwrap();
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
    });
}

/// Contract: registering the recovery root a secret derives is idempotent:
/// the second call, with the root and its delivery for the current epoch
/// there, writes nothing, and after a rotation (a new epoch) it writes the
/// delivery for the new epoch again.
#[test]
fn registering_a_recovery_root_twice_writes_nothing_the_second_time() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        second.status(&server, &[&first]);
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([9; 32]);
        let both = [&first.db, &second.db];
        drive(
            &server,
            &both,
            first.client.register_recovery(&first.db, &secret),
        )
        .unwrap();
        let deliveries = || count(&server, "__e2ee_recovery_deliveries");
        pump(&server, &first.db);
        assert_eq!(
            (count(&server, "__e2ee_recovery_roots"), deliveries()),
            (1, 1)
        );
        drive(
            &server,
            &both,
            first.client.register_recovery(&first.db, &secret),
        )
        .unwrap();
        pump(&server, &first.db);
        assert_eq!(
            (count(&server, "__e2ee_recovery_roots"), deliveries()),
            (1, 1)
        );
        // A rotation makes a new epoch: the next call delivers to it.
        drive(
            &server,
            &both,
            first.client.remove_device(&first.db, &second.id()),
        )
        .unwrap();
        drive(
            &server,
            &[&first.db],
            first.client.register_recovery(&first.db, &secret),
        )
        .unwrap();
        pump(&server, &first.db);
        assert_eq!(count(&server, "__e2ee_recovery_roots"), 1);
        assert!(deliveries() >= 1);
    });
}

/// Contract: the idempotent skip of a recovery registration looks at what
/// opens, not at what is there: a junk delivery row of the root for the
/// current epoch (the server may store anything) neither makes the call
/// write again while a delivery that opens exists, nor is taken for one.
///
/// ```text
/// alice's device ──register──► root + delivery (epoch 1)
/// alice's device ──removes her second device──► epoch 2, delivery for it
/// server ──junk delivery of the root for epoch 2──► (a second row there)
/// alice's device ──register──► nothing written (the real delivery opens)
/// ```
///
/// A root whose only delivery for the epoch is junk cannot be built here: a
/// system table's rows are never updated or deleted, and the rotation
/// delivers to every registered root in its own transaction.
#[test]
fn a_junk_recovery_delivery_beside_a_real_one_changes_nothing() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        second.status(&server, &[&first]);
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([9; 32]);
        let register = || {
            drive(
                &server,
                &[&first.db],
                first.client.register_recovery(&first.db, &secret),
            )
            .unwrap();
            pump(&server, &first.db);
        };
        let deliveries = || count(&server, "__e2ee_recovery_deliveries");
        register();
        assert_eq!(deliveries(), 1);
        drive(
            &server,
            &[&first.db, &second.db],
            first.client.remove_device(&first.db, &second.id()),
        )
        .unwrap();
        pump(&server, &first.db);
        let epoch = first
            .client
            .with_account(|account| account.state.epoch_id().to_owned())
            .unwrap();
        let root = all(&server, Query::from("__e2ee_recovery_roots")).unwrap()[0].row_uuid();
        block_on(server.insert(
            "__e2ee_recovery_deliveries",
            std::collections::BTreeMap::from([
                ("rootId".to_owned(), Value::Uuid(root.0)),
                ("epochId".to_owned(), Value::Uuid(epoch.parse().unwrap())),
                ("envelope".to_owned(), Value::Bytes(vec![0xee; 64])),
            ]),
            Default::default(),
        ))
        .unwrap();
        pump(&server, &first.db);
        assert_eq!(deliveries(), 3);
        register();
        assert_eq!(deliveries(), 3, "the delivery that opens is enough");
    });
}

/// Contract: registering a recovery root is skipped only when a delivery of
/// the root for the current epoch opens with the secret's key. A server that
/// altered the stored delivery (the row is there, its envelope is junk)
/// leaves the registration to be done: the call goes on to write a
/// delivery, and does not fail. The server keeps one delivery per root and
/// epoch, so the altered row stays and recovery stays broken: the call does
/// not read back what it wrote (a known limit).
///
/// ```text
/// alice's device ──register──► root + delivery
/// server (stopped) ──delivery's envelope replaced by junk──► (row still there)
/// alice's device ──register──► writes again, returns ok, the junk row stays
/// ```
#[test]
fn a_recovery_delivery_that_does_not_open_does_not_skip_the_registration() {
    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.sqlite");
        let alice = account(0xa1);
        let store = memory();
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([9; 32]);
        let server = secure_authority_with(&settings, Some(&path));
        let first = device(&server, alice, store.clone(), 1, &settings);
        first.status(&server, &[]);
        drive(
            &server,
            &[&first.db],
            first.client.register_recovery(&first.db, &secret),
        )
        .unwrap();
        pump(&server, &first.db);
        assert_eq!(count(&server, "__e2ee_recovery_deliveries"), 1);
        drop(first);
        // The server alters the stored envelope: the row is there, nothing opens.
        tamper(&path, "__e2ee_recovery_deliveries", "envelope", &[0xee; 64]);
        // The same device, opened afresh, reads what the server now shows.
        let again = device(&server, alice, store, 1, &settings);
        again.status(&server, &[]);
        // The registration proceeds: the stored delivery does not open.
        drive(
            &server,
            &[&again.db],
            again.client.register_recovery(&again.db, &secret),
        )
        .unwrap();
        pump(&server, &again.db);
        assert_eq!(count(&server, "__e2ee_recovery_deliveries"), 1);
    });
}

/// Contract: the device list shows active devices and the ones waiting, with
/// which is this device, the order their requests arrived in and who approved
/// each, so a user can tell the device they approve from others; and a
/// device awaiting approval learns it was challenged without guessing: before
/// the challenge it is not, after it is, and after it answered it is not
/// again; a removed device is no longer listed.
#[test]
fn devices_are_listed_and_a_challenge_is_noticed() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        first.status(&server, &[]);
        let second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        let both = [&first.db, &second.db];
        let listed = drive(&server, &both, first.client.devices(&first.db)).unwrap();
        let status = |id: &str| {
            listed
                .iter()
                .find(|device| device.id == id)
                .map(|device| device.status)
        };
        assert_eq!(
            status(&first.id()),
            Some(jazz_e2ee_client::DeviceStatus::Active)
        );
        assert_eq!(
            status(&second.id()),
            Some(jazz_e2ee_client::DeviceStatus::Waiting)
        );
        let info = |listed: &[jazz_e2ee_client::DeviceInfo], id: &str| {
            listed
                .iter()
                .find(|device| device.id == id)
                .cloned()
                .unwrap()
        };
        let (mine, waiting) = (info(&listed, &first.id()), info(&listed, &second.id()));
        assert!(mine.this_device && !waiting.this_device);
        assert_eq!(
            mine.approved_by,
            Some(jazz_e2ee_client::ApprovedBy::AccountRoot)
        );
        assert_eq!(waiting.approved_by, None);
        assert!(
            waiting.requested_at > mine.requested_at,
            "the later request is told apart by its position"
        );
        assert!(!drive(&server, &both, second.client.challenged(&second.db)).unwrap());
        let pending = drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();
        assert!(drive(&server, &both, second.client.challenged(&second.db)).unwrap());
        let shown = drive(&server, &both, second.client.answer_challenge(&second.db)).unwrap();
        assert!(!drive(&server, &both, second.client.challenged(&second.db)).unwrap());
        drive(
            &server,
            &both,
            first.client.approve_device(&first.db, &pending, &shown),
        )
        .unwrap();
        second.status(&server, &[&first]);
        let listed = drive(&server, &both, first.client.devices(&first.db)).unwrap();
        assert!(
            listed
                .iter()
                .all(|device| device.status == jazz_e2ee_client::DeviceStatus::Active)
        );
        assert_eq!(
            info(&listed, &second.id()).approved_by,
            Some(jazz_e2ee_client::ApprovedBy::Device(first.id()))
        );
        drive(
            &server,
            &both,
            first.client.remove_device(&first.db, &second.id()),
        )
        .unwrap();
        let listed = drive(&server, &[&first.db], first.client.devices(&first.db)).unwrap();
        assert!(listed.iter().all(|device| device.id != second.id()));
    });
}

/// `client` on a new session of `server`.
fn attach(server: &Db, client: E2eeClient, who: AuthorSubject, node: u8) -> Device {
    let (db, link) = session(
        &client,
        server,
        who,
        &EncryptionSettings::default(),
        node,
        None,
    );
    Device {
        client,
        db,
        notes: None,
        _link: link,
    }
}

fn stale_delivery_after_restore(writes: usize) {
    within(move || {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let old = dir.path().join("old.sqlite");
        let settings = EncryptionSettings::default();
        let alice = account(0xa1);
        let bob = account(0xb0);
        let alice_store = memory();
        let deliveries = "__e2ee_space_deliveries";

        let (alice_client, bob_first, removed, space) = {
            let server = secure_authority_with(&settings, Some(&live));
            let alice_device = device(&server, alice, alice_store.clone(), 1, &settings);
            assert_eq!(alice_device.status(&server, &[]), AccountStatus::Active);
            let space = space_with_a_note(&server, &alice_device, &account_id(alice));
            let first = device(&server, bob, memory(), 2, &settings);
            assert_eq!(first.status(&server, &[]), AccountStatus::Active);
            let second = device(&server, bob, memory(), 3, &settings);
            second.status(&server, &[&first]);
            approve(&server, &first, &second);
            assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
            let scope = jazz::db::block_on(alice_device.db.table_identity("projects"))
                .unwrap()
                .unwrap()
                .0;
            let member = std::collections::BTreeMap::from([
                ("scopeId".to_owned(), Value::Uuid(scope)),
                ("identifier".to_owned(), Value::Uuid(space)),
                (
                    "accountId".to_owned(),
                    Value::Uuid(uuid::Uuid::parse_str(&account_id(bob)).unwrap()),
                ),
            ]);
            block_on(
                alice_device
                    .db
                    .insert("__e2ee_members", member, Default::default()),
            )
            .unwrap();
            pump(&server, &alice_device.db);
            assert_eq!(count(&server, deliveries), 0, "bob lacks the key");
            (alice_device.client, first.client, second.id(), space)
        };
        copy_database(&live, &old);

        let alice_client = {
            let server = secure_authority_with(&settings, Some(&live));
            let first = attach(&server, bob_first, bob, 4);
            drive(
                &server,
                &[&first.db],
                first.client.remove_device(&first.db, &removed),
            )
            .unwrap();
            // Alice's own account is not verified again: her own floor stays
            // at a cut the restored copy reaches, so only bob's floor can
            // refuse what it shows.
            let alice_device = attach(&server, alice_client, alice, 5);
            let address = space_address(&alice_device, space);
            let delivered = drive(
                &server,
                &[&alice_device.db],
                alice_device
                    .client
                    .deliver_space(&alice_device.db, &address),
            )
            .unwrap();
            assert_eq!(delivered, 1, "bob is served at his new epoch");
            assert_eq!(count(&server, deliveries), 1);
            alice_device.client
        };
        let floor = block_on(alice_client.floor(&account_id(bob)));
        assert!(floor.is_some(), "alice keeps a floor for bob's account");
        drop(alice_client);

        let restarted = open_account(alice_store.clone(), alice, settings.clone(), None);
        assert_eq!(block_on(restarted.floor(&account_id(bob))), floor);
        let restored = secure_authority_with(&settings, Some(&old));
        let alice_device = attach(&restored, restarted, alice, 6);
        assert_eq!(alice_device.status(&restored, &[]), AccountStatus::Active);
        // Writes after the copy move the restored cut past the saved floor.
        for n in 0..writes {
            project(&alice_device.db, &format!("filler {n}"));
            pump(&restored, &alice_device.db);
        }
        let address = space_address(&alice_device, space);
        let refused = drive(
            &restored,
            &[&alice_device.db],
            alice_device
                .client
                .deliver_space(&alice_device.db, &address),
        );
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::StaleState))
            ),
            "{refused:?}"
        );
        assert_eq!(
            count(&restored, deliveries),
            0,
            "nothing is delivered to the removed device's epoch"
        );
        assert_eq!(block_on(alice_device.client.floor(&account_id(bob))), floor);

        block_on(alice_device.client.wipe()).unwrap();
        let fresh = open_account(alice_store, alice, settings, None);
        assert_eq!(block_on(fresh.floor(&account_id(bob))), None);
        assert_eq!(fresh.with_held(|held| held.spaces().len()), 0);
    });
}

/// Contract: once alice has seen bob remove a device, a server restored
/// from a copy taken before the removal cannot make her deliver a space key
/// to the account epoch the removed device holds: her device kept a floor
/// for bob's account, which survives a restart, so the pass over the space
/// is `stale-state` and writes nothing. The floor is wiped with her held
/// keys.
///
/// ```text
/// alice: space, bob a member without the key; bob: two devices (copy taken)
/// bob ──remove second device (bob's epoch e1 → e2)──► server
/// alice ──pass: delivery to bob at e2──► server        (floor for bob: e2)
/// alice (restarted) ──pass──► restored copy (bob at e1) ──✗ stale-state
/// ```
#[test]
fn a_member_shown_from_before_a_device_removal_gets_no_delivery() {
    within(|| stale_delivery_after_restore(0));
}

/// Contract: as above, but the restored server's cut is LATER than the one
/// alice's floor for bob was taken at (writes after the restore moved it
/// on): a later cut does not excuse bob's earlier account epoch. His
/// floor's epoch is not in the chain the read shows, so the pass is
/// `stale-state` and writes nothing; the floor is kept as it was.
///
/// ```text
/// alice: space, bob a member without the key; bob: two devices (copy taken)
/// bob ──remove second device (bob's epoch e1 → e2)──► server
/// alice ──pass: delivery to bob at e2──► server        (floor for bob: e2)
/// alice (restarted) ──60 writes──► restored copy (bob at e1, cut past the floor's)
/// alice ──pass──► restored copy ──✗ stale-state
/// ```
#[test]
fn old_history_at_a_later_cut_gets_no_delivery() {
    within(|| stale_delivery_after_restore(60));
}
/// Alice keeps the room open between sends. Both writes finish locally even
/// when she loses the connection without the host detecting it.
#[test]
fn an_open_space_stays_ready_to_write_between_messages() {
    within(|| {
        let mut shared = SharedSpace::new();
        let address = space_address(&shared.alice, shared.space);
        shared.watch.use_space(&address);
        shared.turns();
        shared.alice_writes("warm");
        shared.turns();
        let alice = &shared.alice;
        alice.client.take_reports();
        for message in ["still online", "disconnected"] {
            alice
                .client
                .advance_settle_clock(std::time::Duration::from_secs(61));
            // No server pump: preparation that needs a round trip cannot finish.
            let write = alice.client.write(&alice.db, async || {
                alice
                    .db
                    .insert("notes", note(shared.space, message), Default::default())
                    .await
            });
            assert!(
                futures::FutureExt::now_or_never(write)
                    .expect("write waited for the authority")
                    .is_ok()
            );
            if message == "still online" {
                pump(&shared.server, &alice.db);
            }
        }
        assert!(alice.client.take_reports().is_empty());
    });
}
