//! Devices in an account against a real authority with the system tables
//! (`E2EE_PROTOCOL.md` §2.5, §2.8, §2.9): a second device is approved by
//! challenge, response and an approval code; a device is removed and the
//! account rotated; a device is restored with a recovery secret. Every test
//! runs on its own thread with a time limit.

mod common;

use std::cell::Cell;
use std::rc::Rc;

use common::*;
use jazz::db::{Db, ReadSpan, SubscriptionStream, SyncMessage, block_on};
use jazz::e2ee::EncryptionSettings;
use jazz::ids::AuthorSubject;
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee_client::PrivateStore as _;
use jazz_e2ee_client::host::bridge::{JsStore, StoreWake};
use jazz_e2ee_client::host::runtime::{ApprovalWatchFault, Built, Encryption};
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
        host_read(&device.client, &device.db, async || {
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
        host_write(&first.client, &first.db, async || {
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
            second.client.prepare_host_write(&second.db, &address, None),
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
            host_write(&first.client, &first.db, async || {
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
            host_write(&again.client, &again.db, async || {
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
            host_write(&first.client, &first.db, async || {
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
            deliver_for(&alice.client, &alice.db, &address),
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
            host_write(&self.alice.client, &self.alice.db, async || {
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

/// Alice keeps her spaces open while her first device disconnects. Her
/// second device rotates one space and adds Bob to another. The first
/// device sees the new epoch and delivers the other space's key to Bob.
/// A quiet reconnect needs no one-shot reads of the watched spaces.
#[test]
fn reconnect_reads_space_and_membership_changes() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let (alice, bob) = (account(0xa1), account(0xb0));
        let client = open_account(memory(), alice, settings.clone(), None);
        let (db, link, _) = recorded_session(&client, &server, alice, &settings, 1, None);
        let first = Device {
            client,
            db,
            notes: None,
            _link: link,
        };
        assert_eq!(first.status(&server, &[]), AccountStatus::Active);
        let mut second = device(&server, alice, memory(), 2, &settings);
        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
        let bob_device = device(&server, bob, memory(), 3, &settings);
        assert_eq!(bob_device.status(&server, &[]), AccountStatus::Active);

        let spaces: Vec<_> = (0..6)
            .map(|_| space_with_a_note(&server, &first, &account_id(alice)))
            .collect();
        let rotate = space_address(&first, spaces[0]);
        second.follow_notes(&server);
        assert_eq!(notes_of(&server, &second, spaces[0]).len(), 1);

        let client = Rc::new(first.client);
        let db = Rc::new(first.db);
        let runtime = Encryption::new(
            Built {
                spaces: serde_json::json!({}),
                client: client.clone(),
                reports: Rc::default(),
            },
            Rc::new(JsStore::default()),
            db.clone(),
        );
        for space in &spaces {
            runtime.use_space(true, "projects".to_owned(), space.to_string());
        }
        let settle = || {
            for _ in 0..10 {
                pump(&server, &db);
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
            }
        };
        let reconnect = |label| {
            let started = std::time::Instant::now();
            let (link, wire) = link_as(&db, &server, alice);
            runtime.set_online(true);
            settle();
            let elapsed = started.elapsed();
            let wire = wire.borrow();
            let mut remaining = wire.as_slice();
            let (mut live, mut once, mut space_roots, mut unchanged_roots) = (0, 0, 0, 0);
            while !remaining.is_empty() {
                let (message, rest) = postcard::take_from_bytes::<SyncMessage>(remaining).unwrap();
                if let SyncMessage::Read { query, span, .. } = message {
                    match span {
                        ReadSpan::Live => live += 1,
                        ReadSpan::Once { .. } => {
                            once += 1;
                            if query.table == "__e2ee_spaces" {
                                space_roots += 1;
                                unchanged_roots += usize::from(spaces[2..].iter().any(|space| {
                                    query
                                        .filters
                                        .contains(&eq(col("identifier"), lit(Value::Uuid(*space))))
                                }));
                            }
                        }
                    }
                }
                remaining = rest;
            }
            eprintln!(
                "{label}: {live} live, {once} one-shot, {space_roots} space root reads ({unchanged_roots} unchanged), {elapsed:?}"
            );
            (link, space_roots, unchanged_roots)
        };
        settle();
        runtime.set_online(false);
        assert!(db.detach_connection(&first._link));
        let (link, quiet_roots, _) = reconnect("quiet reconnect");

        runtime.set_online(false);
        assert!(db.detach_connection(&link));
        drive(
            &server,
            &[&second.db],
            second.client.rotate(&second.db, &rotate),
        )
        .unwrap();
        let member = std::collections::BTreeMap::from([
            (
                "scopeId".to_owned(),
                Value::Uuid(rotate.scope_id.parse().unwrap()),
            ),
            ("identifier".to_owned(), Value::Uuid(spaces[1])),
            (
                "accountId".to_owned(),
                Value::Uuid(account_id(bob).parse().unwrap()),
            ),
        ]);
        block_on(
            second
                .db
                .insert("__e2ee_members", member, Default::default()),
        )
        .unwrap();
        pump(&server, &second.db);

        let address = jazz_e2ee::protocol::records::space::SpaceAddress::new(
            &rotate.scope_id,
            &rotate.identifier,
        )
        .unwrap();
        let epochs = || client.with_held(|keys| keys.known(&address).unwrap().epochs().len());
        let deliveries = || {
            all(
                &server,
                Query::from("__e2ee_space_deliveries")
                    .filter(eq(col("identifier"), lit(Value::Uuid(spaces[1]))))
                    .filter(eq(
                        col("recipientAccountId"),
                        lit(Value::Uuid(account_id(bob).parse().unwrap())),
                    )),
            )
            .unwrap()
            .len()
        };
        assert_eq!(epochs(), 1);
        assert_eq!(deliveries(), 0);
        let (_link, _, unchanged_roots) = reconnect("reconnect after offline changes");
        assert_eq!(
            (epochs(), deliveries()),
            (2, 1),
            "the first device sees the new space epoch and delivers Bob's key"
        );
        assert_eq!(quiet_roots, 0, "no watched space changed");
        assert_eq!(unchanged_roots, 0, "only the changed spaces need a pass");
        runtime.stop();
    });
}

/// Alice's first device reconnects with no app watches. While it was
/// offline, her other device rotated one space and admitted Bob to both.
/// The reconnect pass delivers the current key of each space to Bob.
///
/// ```text
/// Alice's first device  ──offline────────────────────────► reconnect
/// Alice's other device ──rotate one space──► add Bob to both
/// Bob                  ◄────current keys for both spaces───────┘
/// ```
///
#[test]
fn reconnect_serves_offline_changes_in_unwatched_spaces() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let (alice, bob) = (account(0xa1), account(0xb0));
        let first = device(&server, alice, deferred_memory_store(), 1, &settings);
        assert_eq!(first.status(&server, &[]), AccountStatus::Active);
        let second = device(&server, alice, deferred_memory_store(), 2, &settings);
        second.status(&server, &[&first]);
        approve(&server, &first, &second);
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
        let bob_device = device(&server, bob, deferred_memory_store(), 3, &settings);
        assert_eq!(bob_device.status(&server, &[]), AccountStatus::Active);

        let rotated = space_with_a_note(&server, &first, &account_id(alice));
        let unchanged = space_with_a_note(&server, &first, &account_id(alice));
        let rotate = space_address(&first, rotated);
        // Expire the recent writes too, so no shared watch follows these spaces.
        first
            .client
            .advance_settle_clock(std::time::Duration::from_secs(61));
        let db = Rc::new(first.db);
        let runtime = Encryption::new(
            Built {
                spaces: serde_json::json!({}),
                client: Rc::new(first.client),
                reports: Rc::default(),
            },
            Rc::new(JsStore::default()),
            db.clone(),
        );
        let settle = || {
            for _ in 0..10 {
                pump(&server, &db);
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
            }
        };
        settle();
        runtime.set_online(false);
        assert!(db.detach_connection(&first._link));

        drive(
            &server,
            &[&second.db],
            second.client.rotate(&second.db, &rotate),
        )
        .unwrap();
        let successor = all(&server, Query::from("__e2ee_space_successors")).unwrap();
        let [successor] = successor.as_slice() else {
            panic!("the other device must have committed one rotation");
        };
        let root = all(
            &server,
            Query::from("__e2ee_spaces").filter(eq(col("identifier"), lit(Value::Uuid(unchanged)))),
        )
        .unwrap();
        let bob_id = Value::Uuid(account_id(bob).parse().unwrap());
        for space in [rotated, unchanged] {
            let member = std::collections::BTreeMap::from([
                (
                    "scopeId".to_owned(),
                    Value::Uuid(rotate.scope_id.parse().unwrap()),
                ),
                ("identifier".to_owned(), Value::Uuid(space)),
                ("accountId".to_owned(), bob_id.clone()),
            ]);
            block_on(
                second
                    .db
                    .insert("__e2ee_members", member, Default::default()),
            )
            .unwrap();
        }
        pump(&server, &second.db);
        let deliveries = || {
            all(
                &server,
                Query::from("__e2ee_space_deliveries")
                    .filter(eq(col("recipientAccountId"), lit(bob_id.clone()))),
            )
            .unwrap()
        };
        assert!(deliveries().is_empty());

        let (_link, _) = link_as(&db, &server, alice);
        runtime.set_online(true);
        settle();
        let deliveries = deliveries();
        for (space, epoch) in [
            (rotated, successor.cell("epochId")),
            (unchanged, root[0].cell("epochId")),
        ] {
            assert!(
                deliveries
                    .iter()
                    .any(|row| row.cell("identifier") == Some(Value::Uuid(space))
                        && row.cell("keyEpochId") == epoch),
                "Bob should receive the current key for {space} after the first device reconnects",
            );
        }
        runtime.stop();
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
                deliver_for(&shared.alice.client, &shared.alice.db, &address),
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
                host_write(&bob.client, &bob.db, async || {
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
            host_write(&writer.client, &writer.db, async || {
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
            deliver_for(&alice.client, &alice.db, &address),
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
            host_write(&alice.client, &alice.db, async || {
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
            host_write(&shared.alice.client, &shared.alice.db, async || {
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
        let mut shared = SharedSpace::with_store(store.store());
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
            deliver_for(&alice.client, &alice.db, &address),
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

/// Alice's first device skips the empty sweep after verifying. When her
/// second device requests removal, the watch triggers verification and the
/// sweep still reads the full ledger at its own cut before rotating.
/// The wire trace distinguishes verification from the removal transaction.
///
///```text
/// first ──verify──► idle ◄──watch── authority ◄──request── second
/// first ──verify, read and rotate──► authority
///```
#[test]
fn the_host_only_rereads_the_account_for_a_pending_removal() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let client = open_account(memory(), alice, settings.clone(), None);
        let (db, link, wire) = recorded_session(&client, &server, alice, &settings, 1, None);
        let first = Device {
            notes: None,
            client,
            db,
            _link: link,
        };
        assert_eq!(first.status(&server, &[]), AccountStatus::Active);
        let second = device(&server, alice, memory(), 2, &settings);
        assert_eq!(
            second.status(&server, &[&first]),
            AccountStatus::AwaitingApproval
        );
        approve(&server, &first, &second);
        assert_eq!(second.status(&server, &[&first]), AccountStatus::Active);
        let requester = second.id();
        let client = Rc::new(first.client);
        let db = Rc::new(first.db);
        let runtime = Encryption::new(
            Built {
                spaces: serde_json::json!({}),
                client: client.clone(),
                reports: Rc::default(),
            },
            Rc::new(JsStore::default()),
            db.clone(),
        );

        let take_ledger_tables = || {
            // The test's small messages each fit in one frame.
            let bytes = std::mem::take(&mut *wire.borrow_mut());
            let mut remaining = bytes.as_slice();
            let mut tables = Vec::new();
            while !remaining.is_empty() {
                let (message, rest) = postcard::take_from_bytes::<SyncMessage>(remaining).unwrap();
                if let SyncMessage::Read {
                    query,
                    span: ReadSpan::Once { .. },
                    ..
                } = message
                {
                    tables.push(query.table);
                }
                remaining = rest;
            }
            tables
        };

        wire.borrow_mut().clear();
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
        let verification = take_ledger_tables();
        assert_eq!(verification.len(), 15, "{verification:?}");

        drive(
            &server,
            &[&second.db, &db],
            second.client.request_removal(&second.db),
        )
        .unwrap();
        drive(&server, &[&db], async {
            loop {
                runtime.drive();
                let state = runtime.state();
                assert!(state["error"].is_null(), "{state}");
                if state["stage"] == "idle"
                    && client.with_account(|account| account.state.removed.contains(&requester))
                        == Some(true)
                {
                    break;
                }
                futures::pending!();
            }
        });
        let removal = take_ledger_tables();
        assert_eq!(
            removal.get(..30),
            Some(
                [verification.as_slice(), verification.as_slice()]
                    .concat()
                    .as_slice()
            ),
            "{removal:?}"
        );
        assert_eq!(
            drive(
                &server,
                &[&second.db, &db],
                second.client.ensure_account(&second.db)
            )
            .unwrap(),
            AccountStatus::Reenrolled
        );
        runtime.stop();
    });
}

/// Alice's second device reports a challenge from her first device as
/// soon as the challenge watch delivers it, without a polling interval.
#[test]
fn the_host_reports_a_new_approval_challenge_from_the_watch() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        assert_eq!(first.status(&server, &[]), AccountStatus::Active);
        let second = device(&server, alice, memory(), 2, &settings);
        let requester = second.id();
        let db = Rc::new(second.db);
        let runtime = Encryption::new(
            Built {
                spaces: serde_json::json!({}),
                client: Rc::new(second.client),
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
                if state["status"] == "awaiting-approval" && state["stage"] == "idle" {
                    break;
                }
                futures::pending!();
            }
        });
        let both = [&first.db, db.as_ref()];
        drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &requester),
        )
        .unwrap();
        drive(&server, &both, async {
            loop {
                runtime.drive();
                if runtime
                    .take_reports()
                    .iter()
                    .any(|report| report == r#"{"type":"challenged"}"#)
                {
                    break;
                }
                futures::pending!();
            }
        });
        runtime.stop();
    });
}

/// Alice's second device gets a one-shot wake after its watch fails to
/// open, then receives the first device's challenge on the recovered watch.
#[test]
fn the_host_retries_an_approval_watch_that_failed_to_open() {
    approval_watch_recovery(ApprovalWatchFault::Install);
}

/// Alice's second device gets a one-shot wake after an approval stream
/// closes, then receives the first device's challenge on the new watch.
#[test]
fn the_host_retries_an_approval_watch_that_closed() {
    approval_watch_recovery(ApprovalWatchFault::Closed);
}

fn approval_watch_recovery(fault: ApprovalWatchFault) {
    within(move || {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        assert_eq!(first.status(&server, &[]), AccountStatus::Active);
        let second = device(&server, alice, memory(), 2, &settings);
        let requester = second.id();
        let db = Rc::new(second.db);
        let bridge = Rc::new(JsStore::default());
        let announced = Rc::new(Cell::new(None));
        bridge.set_store_wake(Some(Box::new({
            let announced = announced.clone();
            move |wake| {
                if let StoreWake::Deadline(delay) = wake {
                    announced.set(delay);
                }
            }
        })));
        let runtime = Encryption::new(
            Built {
                spaces: serde_json::json!({}),
                client: Rc::new(second.client),
                reports: Rc::default(),
            },
            bridge.clone(),
            db.clone(),
        );
        runtime.fail_approval_watch_once(fault);
        let wait_for_idle = || {
            drive(&server, &[&db], async {
                loop {
                    runtime.drive();
                    let state = runtime.state();
                    assert!(state["error"].is_null(), "{state}");
                    if state["status"] == "awaiting-approval" && state["stage"] == "idle" {
                        break;
                    }
                    futures::pending!();
                }
            });
        };
        wait_for_idle();
        let delay = announced.get().expect("the host needs a retry wake");
        let deadline = bridge.next_deadline_after(web_time::Instant::now());
        assert!(deadline.is_some());
        wait_for_idle();
        assert_eq!(
            bridge.next_deadline_after(web_time::Instant::now()),
            deadline
        );
        // The host can sleep until its announced deadline, with no polling.
        std::thread::sleep(delay);
        wait_for_idle();
        // A healthy watch arms only its slow re-check, two seconds after it
        // opened.
        assert!(
            announced
                .get()
                .is_some_and(|delay| delay <= std::time::Duration::from_secs(2)),
            "a healthy watch arms only the re-check: {:?}",
            announced.get()
        );
        assert!(runtime.take_reports().is_empty());

        let both = [&first.db, db.as_ref()];
        drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &requester),
        )
        .unwrap();
        drive(&server, &both, async {
            loop {
                runtime.drive();
                assert!(
                    announced
                        .get()
                        .is_some_and(|delay| delay <= std::time::Duration::from_secs(30)),
                    "only the slow re-check may be armed: {:?}",
                    announced.get()
                );
                if runtime
                    .take_reports()
                    .iter()
                    .any(|report| report == r#"{"type":"challenged"}"#)
                {
                    break;
                }
                futures::pending!();
            }
        });
        runtime.stop();
    });
}

/// Alice's second device has an approval watch that stays open but never
/// reports the challenge. The slow re-check still finds the first device's
/// challenge.
#[test]
fn the_host_rechecks_for_a_challenge_when_the_approval_watch_stalls() {
    challenge_found_despite(ApprovalWatchFault::Stalled);
}

/// Alice's second device sees the challenge on its watch, but the
/// authoritative read that follows has not caught up and finds none. The
/// watch reports nothing more, and the re-check finds the challenge.
#[test]
fn the_host_rechecks_after_a_challenge_read_that_ran_early() {
    challenge_found_despite(ApprovalWatchFault::EarlyFalse);
}

fn challenge_found_despite(fault: ApprovalWatchFault) {
    within(move || {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        assert_eq!(first.status(&server, &[]), AccountStatus::Active);
        let second = device(&server, alice, memory(), 2, &settings);
        let requester = second.id();
        let db = Rc::new(second.db);
        let bridge = Rc::new(JsStore::default());
        let announced = Rc::new(Cell::new(None));
        bridge.set_store_wake(Some(Box::new({
            let announced = announced.clone();
            move |wake| {
                if let StoreWake::Deadline(delay) = wake {
                    announced.set(delay);
                }
            }
        })));
        let runtime = Encryption::new(
            Built {
                spaces: serde_json::json!({}),
                client: Rc::new(second.client),
                reports: Rc::default(),
            },
            bridge.clone(),
            db.clone(),
        );
        runtime.fail_approval_watch_once(fault);
        drive(&server, &[&db], async {
            loop {
                runtime.drive();
                let state = runtime.state();
                assert!(state["error"].is_null(), "{state}");
                if state["status"] == "awaiting-approval" && state["stage"] == "idle" {
                    break;
                }
                futures::pending!();
            }
        });
        let both = [&first.db, db.as_ref()];
        drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &requester),
        )
        .unwrap();
        // Drive as a host woken by its deadlines would: the re-check must
        // find the challenge within a few of its first, short waits.
        let started = web_time::Instant::now();
        let mut armed = false;
        loop {
            // One pass of the loop, with the server answering its reads.
            let found = drive(&server, &both, async {
                loop {
                    runtime.drive();
                    if runtime
                        .take_reports()
                        .iter()
                        .any(|report| report == r#"{"type":"challenged"}"#)
                    {
                        return true;
                    }
                    if runtime.state()["stage"] == "idle" {
                        return false;
                    }
                    futures::pending!();
                }
            });
            if found {
                break;
            }
            // The host is told when to wake for the re-check.
            armed |= announced
                .get()
                .is_some_and(|delay| delay <= std::time::Duration::from_secs(30));
            assert!(
                started.elapsed() < std::time::Duration::from_secs(20),
                "the challenge was not found in 20 s (deadline announced: {:?})",
                announced.get()
            );
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        assert!(armed, "the host was given no deadline for the re-check");
        runtime.stop();
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

        let recovered_store = MemoryStore::new();
        let mut recovered = device(
            &server,
            alice,
            Rc::new(recovered_store.clone()),
            2,
            &settings,
        );
        assert_eq!(
            recovered.status(&server, &[&first]),
            AccountStatus::AwaitingApproval
        );
        let other_space = project(&first.db, "previous private space");
        let held = hold_new_space(&recovered.client, &recovered.db, other_space);
        drop(open_account(
            Rc::new(recovered_store.clone()),
            alice,
            settings.clone(),
            None,
        ));
        assert_eq!(stored_verified_depth(&recovered_store, &held), Some(0));
        let status = drive(
            &server,
            &[&recovered.db, &first.db],
            recovered.client.recover_device(&recovered.db, &secret),
        )
        .unwrap();
        assert_eq!(status, AccountStatus::Active);
        assert_eq!(stored_verified_depth(&recovered_store, &held), None);
        let reopened = open_account(
            Rc::new(recovered_store.clone()),
            alice,
            settings.clone(),
            None,
        );
        assert!(reopened.with_held(|keys| keys.space(&held).is_some()));
        assert_eq!(stored_verified_depth(&recovered_store, &held), Some(0));
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

/// Contract: the start-up registration a device account's host runs
/// (`recoveryRegister` with `automatic`) takes only the secret of the
/// identity the device was opened as, like `start_device_account`: another
/// identity's secret is refused with `recovery-root-mismatch` and registers
/// no root, while the device's own key registers once.
///
/// ```text
/// laptop (key K) ──ready──► root of K
/// laptop ──recoveryRegister { secret: mallory's, automatic }──✗ recovery-root-mismatch
/// laptop ──recoveryList──► still only the root of K
/// ```
#[test]
fn a_device_accounts_automatic_registration_refuses_another_identitys_secret() {
    use jazz_e2ee_client::host::{RecoverySecret, bridge::JsStore, runtime};
    use serde_json::json;

    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (lena, _) = lena_device(&server, 0x4f, 1);
        let own_secret = RecoverySecret::from_bytes([0x4f; 32]);
        let client = Rc::new(lena.client);
        let db = Rc::new(lena.db);
        let runtime = runtime::Encryption::new(
            runtime::Built {
                spaces: json!({}),
                client: Rc::clone(&client),
                reports: Rc::default(),
            },
            Rc::new(JsStore::default()),
            Rc::clone(&db),
        );
        assert_eq!(
            drive(
                &server,
                &[&db],
                client.start_device_account(&db, &own_secret)
            )
            .unwrap(),
            AccountStatus::Active
        );
        let roots = || {
            drive(
                &server,
                &[&db],
                runtime.call("recoveryList".to_owned(), json!({})),
            )
            .unwrap()
        };
        let before = roots();
        assert_eq!(before.as_array().map(Vec::len), Some(1), "{before}");

        let mallory = RecoverySecret::from_bytes([0x5f; 32]).format();
        let refusal = drive(
            &server,
            &[&db],
            runtime.call(
                "recoveryRegister".to_owned(),
                json!({ "secret": mallory.as_str(), "automatic": true }),
            ),
        )
        .unwrap_err();
        assert!(refusal.starts_with("recovery-root-mismatch: "), "{refusal}");
        assert_eq!(roots(), before, "no root registered for it");
    });
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

/// Lena retires a second recovery credential but must keep her device account's
/// own root. After the refusal a new device recovers using the account secret.
#[test]
fn a_device_account_can_retire_only_its_other_recovery_roots() {
    within(|| {
        use jazz_e2ee::protocol::derive::RecoverySecret;
        use jazz_e2ee::protocol::error::RefusalCode;
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let secret = RecoverySecret::from_bytes([0x6f; 32]);
        let (first, _) = lena_device(&server, 0x6f, 1);
        drive(
            &server,
            &[&first.db],
            first.client.start_device_account(&first.db, &secret),
        )
        .unwrap();
        let own = first
            .client
            .with_account(|a| a.state.recovery_roots[0].row.id.clone())
            .unwrap();
        let other_secret = RecoverySecret::from_bytes([7; 32]);
        drive(
            &server,
            &[&first.db],
            first.client.register_recovery(&first.db, &other_secret),
        )
        .unwrap();
        first.status(&server, &[]);
        let other = first
            .client
            .with_account(|a| {
                a.state
                    .recovery_roots
                    .iter()
                    .find(|root| root.row.id != own)
                    .unwrap()
                    .row
                    .id
                    .clone()
            })
            .unwrap();
        drive(
            &server,
            &[&first.db],
            first.client.retire_recovery_root(&first.db, &other),
        )
        .unwrap();
        assert!(
            first
                .client
                .with_account(|a| a.state.recovery_root(&other).is_none())
                .unwrap()
        );
        let refused = drive(
            &server,
            &[&first.db],
            first.client.retire_recovery_root(&first.db, &own),
        );
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::Refused(
                    RefusalCode::DeviceAccountRootRequired
                )))
            ),
            "{refused:?}"
        );
        let (second, _) = lena_device(&server, 0x6f, 2);
        assert_eq!(
            second.status(&server, &[&first]),
            AccountStatus::AwaitingApproval
        );
        assert_eq!(
            drive(
                &server,
                &[&second.db],
                second.client.recover_device(&second.db, &secret)
            )
            .unwrap(),
            AccountStatus::Active
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
/// not believe they have recovery.
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
        let store = FaultyStore {
            fail_doc: Some("device"),
            ..FaultyStore::default()
        };
        let second = device(&server, alice, store.store(), 2, &settings);
        second.status(&server, &[&first]);
        let both = [&first.db, &second.db];
        drive(
            &server,
            &both,
            first.client.challenge_device(&first.db, &second.id()),
        )
        .unwrap();

        store.failing.set(true);
        let refused = drive(&server, &both, second.client.answer_challenge(&second.db));
        assert!(matches!(refused, Err(ClientError::Store(_))), "{refused:?}");
        pump(&server, &second.db);
        assert_eq!(
            count(&server, "__e2ee_device_proofs"),
            0,
            "no proof was published"
        );

        store.failing.set(false);
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

/// The built-in key sealing, except that opening a recovery delivery is a
/// provider failure while `fails` is set.
struct RecoveryOpenFails(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl jazz_e2ee::agility::MechanismImpl for RecoveryOpenFails {
    fn mechanism(&self) -> jazz_e2ee::envelope::Mechanism<'static> {
        jazz_e2ee::mechanisms::key_seal::MECHANISM
    }
}

impl jazz_e2ee::agility::KeySealer for RecoveryOpenFails {
    fn derive_key_pair(&self, ikm: &[u8]) -> jazz_e2ee::Result<jazz_e2ee::agility::KeyPair> {
        jazz_e2ee::mechanisms::key_seal::Builtin.derive_key_pair(ikm)
    }

    fn seal(
        &self,
        recipient: &jazz_e2ee::agility::PublicKey,
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn jazz_e2ee::RandomSource,
    ) -> jazz_e2ee::Result<Vec<u8>> {
        jazz_e2ee::mechanisms::key_seal::Builtin.seal(recipient, context, key, rng)
    }

    fn open(
        &self,
        recipient: &jazz_e2ee::agility::KeyPair,
        context: &[u8],
        envelope: &[u8],
    ) -> jazz_e2ee::Result<jazz_e2ee::Zeroizing<[u8; 32]>> {
        let marker = b"__e2ee_recovery_deliveries";
        if self.0.load(std::sync::atomic::Ordering::SeqCst)
            && context.windows(marker.len()).any(|window| window == marker)
        {
            return Err(jazz_e2ee::Error::CryptoFailure);
        }
        jazz_e2ee::mechanisms::key_seal::Builtin.open(recipient, context, envelope)
    }
}

/// Contract: registering a root that is already registered fails with the
/// provider's error when the provider fails while opening the root's
/// delivery, and writes nothing. Only a delivery that is missing or does not
/// open is written again.
///
/// ```text
/// alice's device ──register──► root + delivery
/// provider fails opening recovery deliveries
/// alice's device ──register──► provider failure, still one delivery
/// ```
#[test]
fn a_provider_failure_opening_the_recovery_delivery_fails_the_registration() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let fails = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut registry = jazz_e2ee::registry::Registry::builtin();
        registry
            .replace_key_sealer(std::sync::Arc::new(RecoveryOpenFails(fails.clone())))
            .unwrap();
        let client = block_on(E2eeClient::open(jazz_e2ee_client::ClientConfig {
            store: memory(),
            declarations: declarations_with(settings.clone()),
            account_id: account_id(alice),
            registry,
            random: Box::new(jazz_e2ee::OsRandom),
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
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([9; 32]);
        let register = || {
            drive(
                &server,
                &[&first.db],
                first.client.register_recovery(&first.db, &secret),
            )
        };
        register().unwrap();
        pump(&server, &first.db);
        assert_eq!(count(&server, "__e2ee_recovery_deliveries"), 1);
        fails.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(
            register(),
            Err(ClientError::Protocol(ProtocolError::ProviderFailure))
        ));
        pump(&server, &first.db);
        assert_eq!(count(&server, "__e2ee_recovery_deliveries"), 1);
        fails.store(false, std::sync::atomic::Ordering::SeqCst);
        register().unwrap();
        pump(&server, &first.db);
        assert_eq!(count(&server, "__e2ee_recovery_deliveries"), 1);
    });
}

/// Contract: a root that is already registered gets no new commitment, even
/// when its registration has to write the root's delivery again (here the
/// server lost it).
///
/// ```text
/// alice's device ──register──► root + delivery + commitment
/// server (stopped) ──loses the delivery──► root + commitment
/// alice's device ──register──► a delivery, and no second commitment
/// ```
#[test]
fn rewriting_a_registered_roots_delivery_commits_nothing() {
    within(|| {
        let settings = EncryptionSettings::default();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.sqlite");
        let alice = account(0xa1);
        let store = memory();
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([9; 32]);
        {
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
            assert_eq!(count(&server, "__e2ee_commitments"), 1);
            std::mem::forget(first);
        }
        delete_rows(&path, "__e2ee_recovery_deliveries");

        let server = secure_authority_with(&settings, Some(&path));
        let again = device(&server, alice, store, 2, &settings);
        again.status(&server, &[]);
        assert_eq!(count(&server, "__e2ee_recovery_deliveries"), 0);
        drive(
            &server,
            &[&again.db],
            again.client.register_recovery(&again.db, &secret),
        )
        .unwrap();
        pump(&server, &again.db);
        assert_eq!(count(&server, "__e2ee_recovery_deliveries"), 1);
        assert_eq!(count(&server, "__e2ee_commitments"), 1);
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
                deliver_for(&alice_device.client, &alice_device.db, &address),
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
            deliver_for(&alice_device.client, &alice_device.db, &address),
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
        alice.client.flush_reports();
        let reports = observe_reports(&alice.client);
        for message in ["still online", "disconnected"] {
            alice
                .client
                .advance_settle_clock(std::time::Duration::from_secs(61));
            // No server pump: preparation that needs a round trip cannot finish.
            let write = host_write(&alice.client, &alice.db, async || {
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
        alice.client.flush_reports();
        assert!(reports.borrow().is_empty());
    });
}

const FIRST_LOAD_TABLES: [&str; 26] = [
    "__e2ee_account_identities",
    "__e2ee_device_requests",
    "__e2ee_device_challenges",
    "__e2ee_device_proofs",
    "__e2ee_device_approvals",
    "__e2ee_device_deliveries",
    "__e2ee_account_successors",
    "__e2ee_recovery_deliveries",
    "__e2ee_account_roots",
    "__e2ee_public_device_approvals",
    "__e2ee_public_account_successors",
    "__e2ee_recovery_roots",
    "__e2ee_account_keys",
    "__e2ee_device_removal_requests",
    "__e2ee_device_keys",
    "__e2ee_spaces",
    "__e2ee_space_successors",
    "__e2ee_members",
    "__e2ee_account_roots",
    "__e2ee_public_device_approvals",
    "__e2ee_public_account_successors",
    "__e2ee_recovery_roots",
    "__e2ee_account_keys",
    "__e2ee_device_removal_requests",
    "__e2ee_device_keys",
    "__e2ee_space_deliveries",
];

fn take_first_load_reads(wire: &std::cell::RefCell<Vec<u8>>) -> Vec<(Box<Query>, Option<u64>)> {
    // This fixture's messages each fit in one frame.
    let bytes = std::mem::take(&mut *wire.borrow_mut());
    let mut remaining = bytes.as_slice();
    let mut reads = Vec::new();
    while !remaining.is_empty() {
        let (message, rest) = postcard::take_from_bytes::<SyncMessage>(remaining).unwrap();
        if let SyncMessage::Read {
            query,
            span: ReadSpan::Once { at },
            ..
        } = message
            && query.table.starts_with("__e2ee_")
        {
            reads.push((query, at));
        }
        remaining = rest;
    }
    reads
}

fn approved_reader_test(
    test: impl FnOnce(
        &Db,
        &mut Device,
        uuid::Uuid,
        &std::cell::RefCell<Vec<u8>>,
        &std::cell::RefCell<std::collections::BTreeMap<String, &'static str>>,
    ) + Send
    + 'static,
) {
    within(move || {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let first = device(&server, alice, memory(), 1, &settings);
        assert_eq!(first.status(&server, &[]), AccountStatus::Active);
        let space = space_with_a_note(&server, &first, &account_id(alice));

        let client = open_account(memory(), alice, settings.clone(), None);
        let config = jazz::db::DbConfig::new(
            secure_schema_with(&settings),
            jazz::db::DbIdentity {
                node: jazz::ids::NodeUuid::from_bytes([2; 16]),
                author: alice,
            },
        )
        .with_e2ee(client.hooks());
        let db = block_on(Db::open(config)).unwrap();
        let refusals = Rc::default();
        let (link, wire) = link_as_refusing_reads(&db, &server, alice, Some(Rc::clone(&refusals)));
        pump(&server, &db);
        assert!(block_on(client.prepare(&db)).unwrap());
        let mut second = Device {
            notes: None,
            client,
            db,
            _link: link,
        };
        assert_eq!(
            second.status(&server, &[&first]),
            AccountStatus::AwaitingApproval
        );
        approve(&server, &first, &second);
        wire.borrow_mut().clear();
        test(&server, &mut second, space, &wire, &refusals);
    });
}

fn first_load(
    server: &Db,
    second: &Device,
    space: uuid::Uuid,
    wire: &std::cell::RefCell<Vec<u8>>,
) -> Vec<Vec<(Box<Query>, Option<u64>)>> {
    let mut waves = Vec::new();
    let mut load = std::pin::pin!(async {
        assert_eq!(
            second.client.ensure_account(&second.db).await.unwrap(),
            AccountStatus::Active
        );
        assert_eq!(
            second
                .client
                .acquire(&second.db, &space_address(second, space))
                .await
                .unwrap(),
            jazz_e2ee_client::SpaceKeyState::Held
        );
    });
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..100 {
        if load.as_mut().poll(&mut cx).is_ready() {
            return waves;
        }
        // One round trip: flush the newly issued requests before the
        // authority answers any of them.
        block_on(second.db.tick()).unwrap();
        let reads = take_first_load_reads(wire);
        if !reads.is_empty() {
            waves.push(reads);
        }
        block_on(server.tick()).unwrap();
        block_on(second.db.tick()).unwrap();
    }
    panic!("first load did not finish");
}

/// Alice's approved second device fetches all account and space evidence
/// in eight authority round trips before opening the first note.
#[test]
fn approved_device_first_load_takes_eight_authority_waves() {
    approved_reader_test(|server, second, space, wire, _| {
        let waves = first_load(server, second, space, wire);
        let widths: Vec<_> = waves.iter().map(Vec::len).collect();
        assert_eq!(widths, [1, 13, 1, 1, 2, 6, 1, 1]);
    });
}

/// Alice's approved second device requests the same tables at the same
/// transaction cuts as a serial first load and opens the same plaintext.
/// If the authority refuses two reads in one wave, the earlier read's
/// error wins and the account must still be verified before it can read.
#[test]
fn approved_device_first_load_keeps_its_reads_plaintext_and_refusal() {
    approved_reader_test(|server, second, space, wire, refusals| {
        refusals.borrow_mut().extend([
            ("__e2ee_device_requests".into(), "device requests refused"),
            ("__e2ee_device_proofs".into(), "device proofs refused"),
        ]);
        let error = drive(
            server,
            &[&second.db],
            second.client.ensure_account(&second.db),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ClientError::Database(error) if error.message == "device requests refused"
        ));
        let refused_reads = take_first_load_reads(wire);
        assert_eq!(refused_reads[0].0.table, FIRST_LOAD_TABLES[0]);
        assert_eq!(refused_reads[1].0.table, FIRST_LOAD_TABLES[1]);
        assert!(
            !refused_reads
                .iter()
                .any(|(query, _)| query.table == "__e2ee_device_keys")
        );
        assert!(second.client.with_account(|_| ()).is_none());

        refusals.borrow_mut().clear();
        let reads: Vec<_> = first_load(server, second, space, wire)
            .into_iter()
            .flatten()
            .collect();
        let tables: Vec<_> = reads
            .iter()
            .map(|(query, _)| query.table.as_str())
            .collect();
        assert_eq!(tables, FIRST_LOAD_TABLES);
        let filtered_columns = |read: usize| {
            use jazz::query::{Operand, Predicate};
            reads[read]
                .0
                .filters
                .iter()
                .map(|filter| match filter {
                    Predicate::Eq(Operand::Column(name), _)
                    | Predicate::In(Operand::Column(name), _) => name.as_str(),
                    other => panic!("unexpected ledger filter {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        for read in [0, 1, 2, 3, 4, 5, 7] {
            assert!(filtered_columns(read).is_empty());
        }
        for read in [6].into_iter().chain(8..14).chain(18..24) {
            assert_eq!(filtered_columns(read), ["accountId"]);
        }
        for read in [14, 24] {
            assert_eq!(filtered_columns(read), ["deviceId"]);
        }
        for read in [15, 16] {
            assert_eq!(filtered_columns(read), ["scopeId", "identifier"]);
        }
        assert_eq!(filtered_columns(17), ["scopeId", "identifier", "accountId"]);
        assert_eq!(
            filtered_columns(25),
            ["scopeId", "identifier", "keyEpochId", "recipientAccountId"]
        );
        let read_cut = |range: std::ops::Range<usize>| {
            let mut ranges = reads[range].iter();
            assert_eq!(ranges.next().unwrap().1, None);
            let cut = ranges.next().unwrap().1.expect("first read pinned its cut");
            assert!(ranges.all(|(_, at)| *at == Some(cut)));
        };
        read_cut(0..15);
        read_cut(15..26);
        second.follow_notes(server);
        let rows = notes_of(server, second, space);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    });
}

/// The commitments on `server` for the recovery root `secret` derives for
/// `who` at `generation`, after checking the one there is: written in the
/// root's transaction (the authority accepted both at one position), well
/// formed, holding the SLH-DSA-SHA2-128s key of the root's commitment seed,
/// and signed under the root's published signing key.
fn check_root_commitment(
    server: &Db,
    who: &str,
    secret: &jazz_e2ee::protocol::derive::RecoverySecret,
    generation: u64,
) {
    use jazz_e2ee::primitives::slh_dsa::CommitmentKey;
    use jazz_e2ee::protocol::commitment::MECHANISM;
    use jazz_e2ee::protocol::derive::{RecoveryKeys, recovery_commitment_seed};
    use jazz_e2ee::protocol::domain::AppDomain;
    use jazz_e2ee::protocol::records::KeyColumns;
    use jazz_e2ee::protocol::records::roster::{Commitment, SubjectKind};

    let domain = AppDomain::new(
        &app_id().as_object_id().uuid().to_string(),
        Some(ENVIRONMENT),
    )
    .unwrap()
    .account(who)
    .unwrap();
    let keys = RecoveryKeys::derive(secret, &domain, generation).unwrap();
    let root_id = uuid::Uuid::parse_str(&keys.root_id).unwrap();
    let root = all(server, Query::from("__e2ee_recovery_roots"))
        .unwrap()
        .into_iter()
        .find(|row| row.row_uuid().0 == root_id)
        .expect("the root is registered");
    let mut rows = all(server, Query::from("__e2ee_commitments"))
        .unwrap()
        .into_iter()
        .filter(|row| row.cell("subjectId") == Some(Value::Uuid(root_id)));
    let (Some(row), None) = (rows.next(), rows.next()) else {
        panic!("one commitment for the root");
    };
    assert_eq!(row.seq(), root.seq(), "written in the root's transaction");
    let uuid = |row: &jazz::db::CurrentRow, column| match row.cell(column) {
        Some(Value::Uuid(id)) => id.to_string(),
        other => panic!("{column}: {other:?}"),
    };
    let string = |row: &jazz::db::CurrentRow, column| match row.cell(column) {
        Some(Value::String(text)) => text,
        other => panic!("{column}: {other:?}"),
    };
    let bytes = |row: &jazz::db::CurrentRow, column| match row.cell(column) {
        Some(Value::Bytes(bytes)) => bytes,
        other => panic!("{column}: {other:?}"),
    };
    let number = |row: &jazz::db::CurrentRow, column| match row.cell(column) {
        Some(Value::I64(n)) => u32::try_from(n).unwrap(),
        other => panic!("{column}: {other:?}"),
    };
    let commitment = Commitment {
        id: row.row_uuid().0.to_string(),
        account_id: uuid(&row, "accountId"),
        subject_kind: SubjectKind::parse(&string(&row, "subjectKind")).unwrap(),
        subject_id: uuid(&row, "subjectId"),
        mechanism: string(&row, "mechanism"),
        version: number(&row, "version"),
        commitment: bytes(&row, "commitment"),
        signature: bytes(&row, "signature"),
    };
    commitment.validate().unwrap();
    assert_eq!(commitment.account_id, who);
    assert_eq!(commitment.subject_kind, SubjectKind::RecoveryRoot);
    assert_eq!(
        (commitment.mechanism.as_str(), commitment.version),
        (MECHANISM.id, MECHANISM.version)
    );
    let seed = recovery_commitment_seed(secret, &domain, generation);
    assert_eq!(
        commitment.commitment,
        CommitmentKey::from_seed(&seed).1,
        "the key the secret derives"
    );
    let published = KeyColumns {
        signing_mechanism: string(&root, "signingMechanism"),
        signing_version: number(&root, "signingVersion"),
        signing_public_key: bytes(&root, "signingPublicKey"),
        mechanism: string(&root, "mechanism"),
        version: number(&root, "version"),
        public_key: bytes(&root, "publicKey"),
    };
    assert_eq!(published.signing_public_key, keys.signing.public.bytes);
    assert_eq!(
        jazz_e2ee::protocol::signing::verify(
            &jazz_e2ee::registry::Registry::builtin(),
            &published.signing_key(),
            &commitment.signed_bytes(&domain, generation).unwrap(),
            &commitment.signature,
        ),
        Ok(true),
        "signed by the root's own key"
    );
}

/// Contract: every recovery root alice's app account registers is committed
/// to in the root's own transaction, without anything more from her: the
/// commitment holds the post-quantum key her secret derives and is signed
/// by the root. Registering the same secret again, retiring a root,
/// approving a device and restoring one write no commitment; a new secret
/// (rotation) writes a new one.
///
/// ```text
/// alice's laptop ──register (phrase)──► root + delivery + commitment
/// alice's laptop ──register (phrase) again──► nothing
/// alice's laptop ──register (new phrase), retire old──► root + commitment
/// alice's laptop ──approve phone──► no commitment
/// alice's tablet ──restore (new phrase)──► no commitment
/// ```
#[test]
fn every_recovery_root_an_app_account_registers_is_committed() {
    within(|| {
        use jazz_e2ee::protocol::derive::RecoverySecret;
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa1);
        let laptop = device(&server, alice, memory(), 1, &settings);
        assert_eq!(laptop.status(&server, &[]), AccountStatus::Active);
        let commitments = || count(&server, "__e2ee_commitments");
        assert_eq!(commitments(), 0, "a device's bootstrap commits nothing");

        let phrase = RecoverySecret::from_bytes([0x31; 32]);
        drive(
            &server,
            &[&laptop.db],
            laptop.client.register_recovery(&laptop.db, &phrase),
        )
        .unwrap();
        pump(&server, &laptop.db);
        check_root_commitment(&server, &account_id(alice), &phrase, 0);
        assert_eq!(commitments(), 1);

        drive(
            &server,
            &[&laptop.db],
            laptop.client.register_recovery(&laptop.db, &phrase),
        )
        .unwrap();
        pump(&server, &laptop.db);
        assert_eq!(commitments(), 1, "registering again writes nothing");

        let new_phrase = RecoverySecret::from_bytes([0x32; 32]);
        drive(
            &server,
            &[&laptop.db],
            laptop.client.register_recovery(&laptop.db, &new_phrase),
        )
        .unwrap();
        laptop.status(&server, &[]);
        let old_root = laptop
            .client
            .with_account(|account| {
                account
                    .state
                    .recovery_roots
                    .iter()
                    .map(|root| root.row.id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap();
        let domain = jazz_e2ee::protocol::domain::AppDomain::new(
            &app_id().as_object_id().uuid().to_string(),
            Some(ENVIRONMENT),
        )
        .unwrap()
        .account(&account_id(alice))
        .unwrap();
        let old_root_id = jazz_e2ee::protocol::derive::RecoveryKeys::derive(&phrase, &domain, 0)
            .unwrap()
            .root_id;
        assert!(old_root.contains(&old_root_id));
        drive(
            &server,
            &[&laptop.db],
            laptop.client.retire_recovery_root(&laptop.db, &old_root_id),
        )
        .unwrap();
        pump(&server, &laptop.db);
        check_root_commitment(&server, &account_id(alice), &new_phrase, 0);
        assert_eq!(
            commitments(),
            2,
            "the new root's, and nothing at retirement"
        );

        let phone = device(&server, alice, memory(), 2, &settings);
        assert_eq!(
            phone.status(&server, &[&laptop]),
            AccountStatus::AwaitingApproval
        );
        approve(&server, &laptop, &phone);
        assert_eq!(phone.status(&server, &[&laptop]), AccountStatus::Active);
        pump(&server, &laptop.db);
        assert_eq!(commitments(), 2, "an approval writes none");

        let tablet = device(&server, alice, memory(), 3, &settings);
        assert_eq!(
            tablet.status(&server, &[&laptop]),
            AccountStatus::AwaitingApproval
        );
        let status = drive(
            &server,
            &[&tablet.db, &laptop.db],
            tablet.client.recover_device(&tablet.db, &new_phrase),
        )
        .unwrap();
        assert_eq!(status, AccountStatus::Active);
        pump(&server, &tablet.db);
        assert_eq!(commitments(), 2, "a restore writes none");
    });
}

/// Contract: lena's device account commits to its recovery root at its
/// first start, in the root's transaction, from the account secret it was
/// opened with; starting again, and a new device restoring with the secret,
/// write no further commitment.
///
/// ```text
/// lena's first device ──start (secret)──► root + delivery + commitment
/// lena's first device ──start again──► nothing
/// lena's new device ──recover (secret)──► no commitment
/// ```
#[test]
fn a_device_account_commits_its_recovery_root_at_first_start() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let secret = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([0x7f; 32]);
        let (first, lena) = lena_device(&server, 0x7f, 1);
        let start = || {
            drive(
                &server,
                &[&first.db],
                first.client.start_device_account(&first.db, &secret),
            )
            .unwrap()
        };
        assert_eq!(start(), AccountStatus::Active);
        pump(&server, &first.db);
        check_root_commitment(&server, &account_id(lena), &secret, 0);
        assert_eq!(start(), AccountStatus::Active);
        pump(&server, &first.db);
        assert_eq!(count(&server, "__e2ee_commitments"), 1);

        let (second, _) = lena_device(&server, 0x7f, 2);
        assert_eq!(
            drive(
                &server,
                &[&second.db, &first.db],
                second.client.start_device_account(&second.db, &secret),
            )
            .unwrap(),
            AccountStatus::AwaitingApproval
        );
        let status = drive(
            &server,
            &[&second.db, &first.db],
            second.client.recover_device(&second.db, &secret),
        )
        .unwrap();
        assert_eq!(status, AccountStatus::Active);
        pump(&server, &second.db);
        assert_eq!(
            count(&server, "__e2ee_commitments"),
            1,
            "a restore writes none"
        );
    });
}

/// Contract: after alice's account is reset, registering her phrase again
/// registers the root of the new generation, and commits to it with the key
/// her phrase derives for that generation.
///
/// ```text
/// alice's laptop ──register (phrase)──► root₀ + commitment₀
/// alice's phone ──reset──► generation 1
/// alice's phone ──register (phrase)──► root₁ + commitment₁
/// ```
#[test]
fn a_reset_account_commits_the_root_it_registers_again() {
    within(|| {
        let settings = EncryptionSettings {
            allow_account_reset: true,
            ..Default::default()
        };
        let server = secure_authority_with(&settings, None);
        let alice = account(0xa2);
        let phrase = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([0x33; 32]);
        let laptop = device(&server, alice, memory(), 1, &settings);
        assert_eq!(laptop.status(&server, &[]), AccountStatus::Active);
        drive(
            &server,
            &[&laptop.db],
            laptop.client.register_recovery(&laptop.db, &phrase),
        )
        .unwrap();
        pump(&server, &laptop.db);
        check_root_commitment(&server, &account_id(alice), &phrase, 0);

        let phone = device(&server, alice, memory(), 2, &settings);
        assert_eq!(phone.status(&server, &[]), AccountStatus::AwaitingApproval);
        assert_eq!(
            drive(&server, &[&phone.db], phone.client.reset_account(&phone.db)).unwrap(),
            AccountStatus::Active
        );
        drive(
            &server,
            &[&phone.db],
            phone.client.register_recovery(&phone.db, &phrase),
        )
        .unwrap();
        pump(&server, &phone.db);
        check_root_commitment(&server, &account_id(alice), &phrase, 1);
        assert_eq!(count(&server, "__e2ee_commitments"), 2);
    });
}
