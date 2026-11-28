//! The work a returning device does when the app opens (`E2EE_PROTOCOL.md`
//! §3.4, §3.8): the deliveries already in its store are not changes, so an
//! open with nothing new acquires nothing and runs no pass ahead of the
//! app's reads, whatever the number of spaces it holds; a space a read waits
//! for is served first; and once per session a background pass, one space
//! per turn and never ahead of a read, acquires what a stored delivery
//! brings and delivers to every member who lacks a key.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use common::*;
use jazz::db::{Db, Upstream, block_on};
use jazz::e2ee::{EncryptionSettings, SpaceAddress};
use jazz::ids::AuthorSubject;
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee_client::host::bridge::{JsStore, StoreWake};
use jazz_e2ee_client::host::runtime::{Built, Encryption};
use jazz_e2ee_client::{AccountStatus, E2eeClient, MemoryStore};

const MEMBERS: &str = "__e2ee_members";
const DELIVERIES: &str = "__e2ee_space_deliveries";

/// Runs `test` on its own thread; a hang fails after two minutes.
fn within(test: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        test();
        let _ = done.send(());
    });
    match finished.recv_timeout(std::time::Duration::from_secs(120)) {
        Ok(()) => thread.join().unwrap(),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => thread.join().unwrap(),
        Err(_) => panic!("the test did not finish in two minutes"),
    }
}

/// A device that the app opens again and again: its private store and its
/// database file outlive each session.
struct Returning {
    who: AuthorSubject,
    node: u8,
    store: MemoryStore,
    path: PathBuf,
}

/// One app session of a returning device, with its encryption runtime.
struct Opened {
    client: Rc<E2eeClient>,
    db: Rc<Db>,
    _link: Upstream,
    runtime: Rc<Encryption>,
    /// The bridge store the runtime announces its deadlines through.
    bridge: Rc<JsStore>,
}

impl Returning {
    fn new(dir: &Path, who: AuthorSubject, node: u8) -> Self {
        Self {
            who,
            node,
            store: MemoryStore::new(),
            path: dir.join(format!("device-{node}.db")),
        }
    }

    fn id(&self) -> String {
        account_id(self.who)
    }

    /// Opens the app: the runtime starts and runs until it is idle.
    fn open(&self, server: &Db) -> Opened {
        let session = self.open_quiet(server);
        session.settle(server);
        session
    }

    /// Opens the app without running its runtime yet.
    fn open_quiet(&self, server: &Db) -> Opened {
        let settings = EncryptionSettings::default();
        let client = Rc::new(open_account(
            Rc::new(self.store.clone()),
            self.who,
            settings.clone(),
            None,
        ));
        let (db, link) = session(
            &client,
            server,
            self.who,
            &settings,
            self.node,
            Some(&self.path),
        );
        let status = drive(server, &[&db], client.ensure_account(&db));
        assert_eq!(status.unwrap(), AccountStatus::Active);
        let db = Rc::new(db);
        let bridge = Rc::new(JsStore::default());
        let runtime = Encryption::new(
            Built {
                spaces: serde_json::json!({}),
                client: client.clone(),
                reports: Rc::default(),
            },
            bridge.clone(),
            db.clone(),
        );
        Opened {
            client,
            db,
            _link: link,
            runtime,
            bridge,
        }
    }
}

impl Opened {
    /// Runs the runtime and the sync until both are idle.
    fn settle(&self, server: &Db) {
        for _ in 0..10 {
            pump(server, &self.db);
            drive(server, &[&self.db], async {
                loop {
                    self.runtime.drive();
                    let state = self.runtime.state();
                    assert!(state["error"].is_null(), "{state}");
                    if state["status"] == "active" && state["stage"] == "idle" {
                        break;
                    }
                    futures::pending!();
                }
            });
        }
    }

    /// The acquisitions and passes run since the last call.
    fn work(&self) -> Vec<(&'static str, SpaceAddress)> {
        self.client.take_work_for_test()
    }

    /// The app closes.
    fn close(self) {
        self.runtime.stop();
        drop(self);
    }

    fn address(&self, space: uuid::Uuid) -> SpaceAddress {
        let scope = block_on(self.db.table_identity("projects"))
            .unwrap()
            .unwrap()
            .0;
        SpaceAddress {
            scope_id: scope.to_string(),
            identifier: space.to_string(),
        }
    }

    /// A project with `members`' member rows and one note: the space
    /// exists and this account holds its key.
    fn space_of(&self, server: &Db, members: &[String]) -> uuid::Uuid {
        let space = project(&self.db, "plans");
        pump(server, &self.db);
        for member in members {
            self.add_member(server, space, member);
        }
        self.write_note(server, space);
        space
    }

    fn add_member(&self, server: &Db, space: uuid::Uuid, account: &str) {
        let address = self.address(space);
        let cells = BTreeMap::from([
            (
                "scopeId".to_owned(),
                Value::Uuid(address.scope_id.parse().unwrap()),
            ),
            ("identifier".to_owned(), Value::Uuid(space)),
            (
                "accountId".to_owned(),
                Value::Uuid(uuid::Uuid::parse_str(account).unwrap()),
            ),
        ]);
        block_on(self.db.insert(MEMBERS, cells, Default::default())).unwrap();
        pump(server, &self.db);
    }

    fn write_note(&self, server: &Db, space: uuid::Uuid) {
        drive(
            server,
            &[&self.db],
            host_write(&self.client, &self.db, async || {
                self.db
                    .insert("notes", note(space, SECRET), Default::default())
                    .await
            }),
        )
        .unwrap();
        pump(server, &self.db);
    }

    /// The notes of `space` this device can open, read through the host
    /// read path.
    fn notes(&self, server: &Db, space: uuid::Uuid) -> usize {
        let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
        let prepared = self.db.prepare_query(&query).unwrap();
        let live = block_on(self.db.subscribe(&prepared, Default::default())).unwrap();
        pump(server, &self.db);
        let rows = drive(
            server,
            &[&self.db],
            host_read(&self.client, &self.db, async || {
                self.db.all(&prepared, local()).await
            }),
        )
        .unwrap();
        drop(live);
        rows.iter()
            .filter(|row| row.cell("body") == Some(text(SECRET)))
            .count()
    }
}

/// The deliveries the authority holds to `account` in `space`.
fn deliveries_to(server: &Db, space: uuid::Uuid, account: &str) -> usize {
    all(
        server,
        Query::from(DELIVERIES)
            .filter(eq(col("identifier"), lit(Value::Uuid(space))))
            .filter(eq(
                col("recipientAccountId"),
                lit(Value::Uuid(account.parse().unwrap())),
            )),
    )
    .unwrap()
    .len()
}

fn kinds(work: &[(&'static str, SpaceAddress)]) -> BTreeMap<&'static str, usize> {
    let mut counts = BTreeMap::new();
    for (kind, _) in work {
        *counts.entry(*kind).or_default() += 1;
    }
    counts
}

/// Alice's spaces, each with Bob as a member, delivered to him; Bob's
/// device opened once and acquired them all.
fn bob_holding(server: &Db, dir: &Path, spaces: usize) -> (Returning, Returning, Vec<uuid::Uuid>) {
    let alice = Returning::new(dir, account(0xa1), 1);
    let bob = Returning::new(dir, account(0xb0), 2);
    bob.open(server).close();
    let session = alice.open(server);
    let created: Vec<_> = (0..spaces)
        .map(|_| session.space_of(server, &[alice.id(), bob.id()]))
        .collect();
    session.settle(server);
    for space in &created {
        assert_eq!(deliveries_to(server, *space, &bob.id()), 1);
    }
    session.close();
    let first = bob.open(server);
    let work = first.work();
    assert_eq!(kinds(&work)["acquire"], spaces, "{work:?}");
    first.close();
    (alice, bob, created)
}

/// The work of a session, split into the rest, which the app's reads may
/// wait for, and the spaces the background pass stepped over (each step's
/// own acquisition and pass belong to it).
fn split(
    work: Vec<(&'static str, SpaceAddress)>,
) -> (Vec<(&'static str, SpaceAddress)>, Vec<SpaceAddress>) {
    let mut blocking = Vec::new();
    let mut background = Vec::new();
    let mut step: Option<SpaceAddress> = None;
    for (kind, space) in work {
        if kind == "background" {
            background.push(space.clone());
            step = Some(space);
        } else if step.as_ref() != Some(&space) {
            step = None;
            blocking.push((kind, space));
        }
    }
    (blocking, background)
}

/// Contract: a returning device whose store already holds the deliveries
/// of every space it holds, reopened with nothing new, acquires nothing and
/// runs no pass outside its background pass: the work the app's reads can
/// wait for does not grow with the spaces it holds. The background pass
/// steps once over each held space and acquires nothing.
///
/// ```text
/// alice ──30 spaces, bob a member, delivered──► server
/// bob   ──first open: 30 acquisitions──► reopen ×5: no blocking work,
///                                        30 background steps, no acquisition
/// ```
#[test]
fn reopening_with_nothing_new_does_no_blocking_work_per_held_space() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, bob, spaces) = bob_holding(&server, dir.path(), 30);
        for reopen in 0..5 {
            let session = bob.open(&server);
            let work = session.work();
            assert_eq!(kinds(&work).get("acquire"), None, "reopen {reopen}");
            let (blocking, background) = split(work);
            assert!(blocking.is_empty(), "reopen {reopen}: {blocking:?}");
            assert_eq!(background.len(), 30, "reopen {reopen}");
            session.close();
        }
        let session = bob.open(&server);
        assert_eq!(session.notes(&server, spaces[29]), 1);
        session.close();
    });
}

/// Contract: a space a read waits for is served before every other space
/// whose deliveries arrived at the same time, even one that sorts first.
///
/// ```text
/// bob   ──reads Z (no key yet: key-pending)
/// alice ──delivers S1…S6 and Z at once──► bob
/// bob   ──sync: acquires Z first, then S1…S6
/// ```
#[test]
fn a_space_a_read_waits_for_is_served_first() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let alice = Returning::new(dir.path(), account(0xa1), 1);
        let bob = Returning::new(dir.path(), account(0xb0), 2);
        bob.open(&server).close();
        // Alice's runtime does not run: nothing is delivered until she
        // delivers explicitly.
        let holder = alice.open_quiet(&server);
        let spaces: Vec<_> = (0..7)
            .map(|_| holder.space_of(&server, &[alice.id(), bob.id()]))
            .collect();
        let waited = *spaces.last().unwrap();

        let reader = bob.open_quiet(&server);
        let mut watch = drive(
            &server,
            &[&reader.db],
            reader.client.watch_spaces(&reader.db),
        )
        .unwrap();
        let sync = |watch: &mut jazz_e2ee_client::SpaceWatch| {
            pump(&server, &reader.db);
            drive(
                &server,
                &[&reader.db],
                reader.client.sync_spaces(&reader.db, watch),
            )
            .unwrap()
        };
        sync(&mut watch);
        assert_eq!(reader.notes(&server, waited), 0, "no key yet");
        sync(&mut watch);
        reader.work();

        drive(
            &server,
            &[&holder.db],
            holder.client.deliver_keys(&holder.db),
        )
        .unwrap();
        pump(&server, &holder.db);
        sync(&mut watch);
        let acquired: Vec<_> = reader
            .work()
            .into_iter()
            .filter(|(kind, _)| *kind == "acquire")
            .map(|(_, space)| space)
            .collect();
        assert_eq!(acquired.len(), 7, "{acquired:?}");
        assert_eq!(acquired[0], reader.address(waited), "{acquired:?}");
        assert_eq!(reader.notes(&server, waited), 1);
        reader.close();
        holder.close();
    });
}

impl Opened {
    /// Runs the runtime and the sync, turn by turn, until `done` says so
    /// of the work so far, which it returns.
    fn drive_until(
        &self,
        server: &Db,
        mut done: impl FnMut(&[(&'static str, SpaceAddress)]) -> bool,
    ) -> Vec<(&'static str, SpaceAddress)> {
        let mut work = Vec::new();
        for _ in 0..20_000 {
            self.runtime.drive();
            work.extend(self.work());
            if done(&work) {
                return work;
            }
            block_on(self.db.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        panic!("not done: {work:?} {}", self.runtime.state());
    }

    /// Starts pulling the rows of `table` into this device's store, as an
    /// app query does.
    fn follow(&self, server: &Db, query: Query) -> jazz::db::SubscriptionStream {
        let prepared = self.db.prepare_query(&query).unwrap();
        let stream = block_on(self.db.subscribe(&prepared, Default::default())).unwrap();
        pump(server, &self.db);
        stream
    }
}

/// The member row of `account` in `space`, on the authority.
fn member_row(server: &Db, space: uuid::Uuid, account: &str) -> jazz::ids::RowUuid {
    all(server, Query::from(MEMBERS))
        .unwrap()
        .into_iter()
        .find(|row| {
            row.cell("identifier") == Some(Value::Uuid(space))
                && row.cell("accountId") == Some(Value::Uuid(account.parse().unwrap()))
        })
        .unwrap()
        .row_uuid()
}

/// Contract: the background pass never runs ahead of a read: a read that
/// leaves rows out while the pass runs gets its space acquired at the next
/// turn, before the pass steps over more than one more space.
///
/// ```text
/// bob ──open: background step S1──► app reads Z (no key) ──► acquire Z
///     ──► background steps S2…S30
/// ```
#[test]
fn a_read_is_not_delayed_by_the_background_pass() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, _spaces) = bob_holding(&server, dir.path(), 30);
        // A space bob never opened, with no delivery to him yet.
        let holder = alice.open_quiet(&server);
        let unread = holder.space_of(&server, &[alice.id(), bob.id()]);
        holder.close();

        let session = bob.open_quiet(&server);
        let target = session.address(unread);
        session.drive_until(&server, |work| {
            work.iter().any(|(kind, _)| *kind == "background")
        });
        let _read = session.follow(
            &server,
            Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(unread)))),
        );
        let work = session.drive_until(&server, |work| {
            work.iter()
                .any(|(kind, space)| *kind == "acquire" && *space == target)
        });
        let steps = work
            .iter()
            .filter(|(kind, _)| *kind == "background")
            .count();
        assert!(steps <= 1, "{steps} background steps ran ahead of the read");
        session.settle(&server);
        session.close();
    });
}

/// Contract: a member another member's device added, before that device
/// died without delivering and never came back, gets the key from the next
/// session of any holder: Bob's background pass serves Carol.
///
/// ```text
/// alice ──adds carol to S2──► server, then dies (no runtime turn)
/// bob   ──next session: background pass──► carol served
/// ```
#[test]
fn a_member_whose_adder_died_is_served_by_another_holders_next_session() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let carol = Returning::new(dir.path(), account(0xc0), 3);
        carol.open(&server).close();

        let adder = alice.open_quiet(&server);
        adder.add_member(&server, spaces[1], &carol.id());
        adder.close();
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 0);

        bob.open(&server).close();
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 1);
        let session = carol.open(&server);
        assert_eq!(session.notes(&server, spaces[1]), 1);
        session.close();
    });
}

/// Contract: a member invited before signing up, whom the adder's pass had
/// to skip, gets the key at the adder's next session once signed up.
///
/// ```text
/// alice ──adds carol (no account yet): pass skips her
/// carol ──signs up
/// alice ──next session: background pass──► carol served
/// ```
#[test]
fn a_member_invited_before_signing_up_is_served_at_the_next_session() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let alice = Returning::new(dir.path(), account(0xa1), 1);
        let carol = Returning::new(dir.path(), account(0xc0), 3);
        let session = alice.open(&server);
        let space = session.space_of(&server, &[alice.id(), carol.id()]);
        session.settle(&server);
        session.close();
        assert_eq!(deliveries_to(&server, space, &carol.id()), 0);

        carol.open(&server).close();
        alice.open(&server).close();
        assert_eq!(deliveries_to(&server, space, &carol.id()), 1);
        let session = carol.open(&server);
        assert_eq!(session.notes(&server, space,), 1);
        session.close();
    });
}

/// Contract: a delivery that reached the device's store before its watch
/// started (it synced while the app was closed, or with another query) is
/// not a change of the watch, and the background pass acquires it: the
/// stored delivery is of a version the device does not hold.
///
/// ```text
/// alice ──rotates S1──► delivery to bob
/// bob   ──store syncs the delivery, then the runtime starts
///       ──background step S1: acquire──► holds the new version
/// ```
#[test]
fn a_delivery_stored_before_the_watch_is_acquired_in_the_background() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let session = alice.open(&server);
        let rotated = session.address(spaces[0]);
        drive(
            &server,
            &[&session.db],
            session.client.rotate(&session.db, &rotated),
        )
        .unwrap();
        session.settle(&server);
        session.close();

        let session = bob.open_quiet(&server);
        let _deliveries = session.follow(
            &server,
            Query::from(DELIVERIES).filter(eq(
                col("recipientAccountId"),
                lit(Value::Uuid(bob.id().parse().unwrap())),
            )),
        );
        session.settle(&server);
        let work = session.work();
        let at = work
            .iter()
            .position(|entry| *entry == ("background", rotated.clone()))
            .unwrap();
        assert_eq!(
            work.get(at + 1),
            Some(&("acquire", rotated.clone())),
            "{work:?}"
        );
        let (blocking, _) = split(work);
        assert!(blocking.is_empty(), "{blocking:?}");
        session.close();
    });
}

/// Contract: a pass cut short (the app died during an explicit
/// `deliver_keys`, before Carol's delivery was written) is redone by the
/// next session's background pass.
///
/// ```text
/// alice ──adds carol, deliver_keys starts S2's pass ──✗ (the app dies)
/// alice ──next session: background pass──► carol served
/// ```
#[test]
fn a_pass_cut_short_is_redone_by_the_next_session() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, _bob, spaces) = bob_holding(&server, dir.path(), 3);
        let carol = Returning::new(dir.path(), account(0xc0), 3);
        carol.open(&server).close();

        let session = alice.open_quiet(&server);
        let target = session.address(spaces[1]);
        session.add_member(&server, spaces[1], &carol.id());
        {
            let mut pass = std::pin::pin!(session.client.deliver_keys(&session.db));
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            let mut cut = false;
            for _ in 0..2_000 {
                assert!(
                    pass.as_mut().poll(&mut cx).is_pending(),
                    "the pass finished"
                );
                if session
                    .work()
                    .iter()
                    .any(|entry| *entry == ("pass", target.clone()))
                {
                    cut = true;
                    break;
                }
                block_on(session.db.tick()).unwrap();
                block_on(server.tick()).unwrap();
            }
            assert!(cut);
        }
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 0);
        session.close();

        alice.open(&server).close();
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 1);
    });
}

/// Contract: what makes a member lack a key still delivers it across
/// reopens: alice's rotation reaches Bob, a rotation after carol's removal
/// reaches Bob and not carol, and carol's new device reads with its
/// account's keys.
///
/// ```text
/// alice ──rotate S1──► bob reopens: acquires S1's new version, reads
/// alice ──remove carol from S2, write (rotates)──► bob reads; carol does not
/// carol ──new device, approved──► reads S3
/// ```
#[test]
fn rotations_and_new_devices_still_deliver_across_reopens() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let dir = tempfile::tempdir().unwrap();
        let alice = Returning::new(dir.path(), account(0xa1), 1);
        let bob = Returning::new(dir.path(), account(0xb0), 2);
        let carol = Returning::new(dir.path(), account(0xc0), 3);
        bob.open(&server).close();
        carol.open(&server).close();
        let holder = alice.open(&server);
        let everyone = [alice.id(), bob.id(), carol.id()];
        let spaces: Vec<_> = (0..3)
            .map(|_| holder.space_of(&server, &everyone))
            .collect();
        holder.settle(&server);
        holder.close();
        for member in [&bob, &carol] {
            let session = member.open(&server);
            for space in &spaces {
                assert_eq!(session.notes(&server, *space), 1);
            }
            session.close();
        }

        // A rotation while Bob's device is closed.
        let holder = alice.open(&server);
        let rotated = holder.address(spaces[0]);
        drive(
            &server,
            &[&holder.db],
            holder.client.rotate(&holder.db, &rotated),
        )
        .unwrap();
        holder.write_note(&server, spaces[0]);
        holder.settle(&server);
        // Carol's removal; alice's next write rotates first.
        let row = member_row(&server, spaces[1], &carol.id());
        block_on(holder.db.delete(MEMBERS, row, Default::default())).unwrap();
        pump(&server, &holder.db);
        holder.settle(&server);
        holder.write_note(&server, spaces[1]);
        holder.settle(&server);
        holder.close();

        let session = bob.open(&server);
        let acquired: BTreeSet<_> = session
            .work()
            .into_iter()
            .filter(|(kind, _)| *kind == "acquire")
            .map(|(_, space)| space)
            .collect();
        assert_eq!(
            acquired,
            BTreeSet::from([session.address(spaces[0]), session.address(spaces[1])])
        );
        assert_eq!(session.notes(&server, spaces[0]), 2);
        assert_eq!(session.notes(&server, spaces[1]), 2);
        session.close();
        let session = carol.open(&server);
        assert_eq!(session.notes(&server, spaces[0]), 2);
        assert_eq!(
            session.notes(&server, spaces[1]),
            1,
            "removed before the rotation"
        );
        session.close();

        // Carol's new device, approved by her first.
        let first = carol.open_quiet(&server);
        let client = open_account(
            Rc::new(MemoryStore::new()),
            carol.who,
            settings.clone(),
            None,
        );
        let (db, _link) = common::session(&client, &server, carol.who, &settings, 4, None);
        let both = [&*first.db, &db];
        drive(&server, &both, client.ensure_account(&db)).unwrap();
        let pending = drive(
            &server,
            &both,
            first
                .client
                .challenge_device(&first.db, &client.device().unwrap().id),
        )
        .unwrap();
        let shown = drive(&server, &both, client.answer_challenge(&db)).unwrap();
        drive(
            &server,
            &both,
            first.client.approve_device(&first.db, &pending, &shown),
        )
        .unwrap();
        assert_eq!(
            drive(&server, &both, client.ensure_account(&db)).unwrap(),
            AccountStatus::Active
        );
        first.close();
        let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(spaces[2]))));
        let prepared = db.prepare_query(&query).unwrap();
        let _live = block_on(db.subscribe(&prepared, Default::default())).unwrap();
        pump(&server, &db);
        let rows = drive(
            &server,
            &[&db],
            host_read(&client, &db, async || db.all(&prepared, local()).await),
        )
        .unwrap();
        assert_eq!(
            rows.iter()
                .filter(|row| row.cell("body") == Some(text(SECRET)))
                .count(),
            1
        );
    });
}

/// Contract: the first connection after an open is not a reconnect, so it
/// runs no reconnect pass over every held space, whether the host reports
/// the link offline first (a browser opens before it connects) or connected
/// at once; a connection that comes back after the session was online runs
/// the reconnect pass, even when the notices arrive together.
///
/// ```text
/// bob ──open, offline──► online: background pass only
/// bob ──open; online, offline, online before its first turn──► reconnect
///       pass S1…S3
/// ```
#[test]
fn the_first_connection_after_an_open_is_not_a_reconnect() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let reconnect_passes = |session: &Opened| {
            let (blocking, _) = split(session.work());
            blocking
                .into_iter()
                .filter(|(kind, _)| *kind == "pass")
                .map(|(_, space)| space)
                .collect::<BTreeSet<_>>()
        };

        let session = bob.open_quiet(&server);
        session.runtime.set_online(false);
        session.settle(&server);
        session.runtime.set_online(true);
        session.settle(&server);
        assert_eq!(reconnect_passes(&session), BTreeSet::new());
        session.close();

        // The first connection, a loss and a reconnect, told before the
        // runtime takes its first turn.
        let session = bob.open_quiet(&server);
        session.runtime.set_online(true);
        session.runtime.set_online(false);
        session.runtime.set_online(true);
        session.settle(&server);
        let held: BTreeSet<_> = spaces.iter().map(|space| session.address(*space)).collect();
        assert_eq!(reconnect_passes(&session), held);
        session.close();
    });
}

/// Contract: a read whose space's key cannot be obtained, waiting out its
/// back-off, does not hold back the background pass: in the same session
/// another space's pass still serves the member who lacks its key.
///
/// ```text
/// bob ──reads X: every acquisition fails──► X in back-off, its read kept
///     ──background pass──► carol served in S2
/// ```
#[test]
fn a_read_in_back_off_does_not_hold_back_the_background_pass() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let carol = Returning::new(dir.path(), account(0xc0), 3);
        carol.open(&server).close();
        let adder = alice.open_quiet(&server);
        let stuck = adder.space_of(&server, &[alice.id(), bob.id()]);
        adder.add_member(&server, spaces[1], &carol.id());
        adder.close();

        let session = bob.open_quiet(&server);
        session.client.fail_acquisitions_for_test(u32::MAX);
        let _read = session.follow(
            &server,
            Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(stuck)))),
        );
        // The read's failure is the runtime's last error: run it turn by
        // turn instead of until it is idle and without error.
        let work = session.drive_until(&server, |_| {
            deliveries_to(&server, spaces[1], &carol.id()) == 1
        });
        let stuck = session.address(stuck);
        assert!(
            work.iter()
                .any(|entry| *entry == ("acquire", stuck.clone())),
            "the read's acquisition was tried"
        );
        session.close();
    });
}

/// Alice rotates S1 while Bob's app is closed; Bob's store syncs the
/// delivery before his runtime starts, so only his background pass acquires
/// it. Bob's quiet session, with the delivery stored.
fn bob_with_a_stored_delivery(
    server: &Db,
    dir: &Path,
) -> (
    Returning,
    Returning,
    Vec<uuid::Uuid>,
    Opened,
    jazz::db::SubscriptionStream,
) {
    let (alice, bob, spaces) = bob_holding(server, dir, 3);
    let session = alice.open(server);
    let rotated = session.address(spaces[0]);
    drive(
        server,
        &[&session.db],
        session.client.rotate(&session.db, &rotated),
    )
    .unwrap();
    session.write_note(server, spaces[0]);
    session.settle(server);
    session.close();
    let session = bob.open_quiet(server);
    let deliveries = session.follow(
        server,
        Query::from(DELIVERIES).filter(eq(
            col("recipientAccountId"),
            lit(Value::Uuid(bob.id().parse().unwrap())),
        )),
    );
    (alice, bob, spaces, session, deliveries)
}

/// Contract: a background step that failed is retried in the same session
/// after its wait, with no read asking: one failed acquisition of a stored
/// delivery, and the key is still acquired.
///
/// ```text
/// bob ──background step S1: acquire ✗──► wait ──► acquire ✓
/// ```
#[test]
fn a_failed_background_step_is_retried_in_the_session() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, _bob, spaces, session, _deliveries) =
            bob_with_a_stored_delivery(&server, dir.path());
        let rotated = session.address(spaces[0]);
        session.client.fail_acquisitions_for_test(1);
        let acquisitions = |work: &[(&'static str, SpaceAddress)]| {
            work.iter()
                .filter(|entry| **entry == ("acquire", rotated.clone()))
                .count()
        };
        let mut work = session.drive_until(&server, |work| acquisitions(work) == 1);
        // The step waits for its retry time: the runtime is idle meanwhile.
        for _ in 0..200 {
            session.runtime.drive();
            work.extend(session.work());
            block_on(session.db.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        assert_eq!(acquisitions(&work), 1);
        session
            .client
            .advance_settle_clock(std::time::Duration::from_secs(3));
        let address = jazz_e2ee::protocol::records::space::SpaceAddress::new(
            &rotated.scope_id,
            &rotated.identifier,
        )
        .unwrap();
        let versions = || {
            session.client.with_held(|held| {
                held.roots_of(&address)
                    .iter()
                    .map(|root| root.held().count())
                    .sum::<usize>()
            })
        };
        assert_eq!(versions(), 1);
        let more = session.drive_until(&server, |_| versions() == 2);
        assert_eq!(
            acquisitions(&more),
            1,
            "acquired by the retry, with no read"
        );
        session.close();
    });
}

/// Contract: a session that starts with the link down, whose host's first
/// notice is that it is online, still serves the member who lacks a key in
/// that session: the background steps that failed while the server was out
/// of reach run again at that notice.
///
/// ```text
/// alice ──adds carol to S2, dies
/// bob   ──starts, link down: background steps fail
///       ──link up, first notice "online"──► carol served
/// ```
#[test]
fn a_session_that_starts_unconnected_serves_at_its_first_online_notice() {
    within(|| {
        let settings = EncryptionSettings::default();
        let server = secure_authority_with(&settings, None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let carol = Returning::new(dir.path(), account(0xc0), 3);
        carol.open(&server).close();
        let adder = alice.open_quiet(&server);
        adder.add_member(&server, spaces[1], &carol.id());
        adder.close();

        let session = bob.open_quiet(&server);
        assert!(session.db.detach_connection(&session._link));
        let target = session.address(spaces[1]);
        session.drive_until(&server, |work| {
            work.iter()
                .any(|entry| *entry == ("background", target.clone()))
        });
        for _ in 0..200 {
            session.runtime.drive();
            block_on(session.db.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 0);

        let (_link, _) = link_as(&session.db, &server, bob.who);
        session.runtime.set_online(true);
        session.drive_until(&server, |_| {
            deliveries_to(&server, spaces[1], &carol.id()) == 1
        });
        session.close();
    });
}

/// Contract: a background step is never awaited ahead of a read: while a
/// step waits forever on the server, a read of another space whose delivery
/// is stored is settled at once.
///
/// ```text
/// bob ──background step (never answers)
///     ──app reads S1 (its new key only stored as a delivery)──► acquired
/// ```
#[test]
fn a_slow_background_step_does_not_delay_a_read() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, _bob, spaces, session, _deliveries) =
            bob_with_a_stored_delivery(&server, dir.path());
        let rotated = session.address(spaces[0]);
        session.client.hold_background_steps_for_test(true);
        session.drive_until(&server, |work| {
            work.iter().any(|(kind, _)| *kind == "background")
        });
        let _read = session.follow(
            &server,
            Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(spaces[0])))),
        );
        session.drive_until(&server, |work| {
            work.iter()
                .any(|entry| *entry == ("acquire", rotated.clone()))
        });
        session.close();
    });
}

/// Alice adds Carol to `space` and her app dies before it delivers.
fn carol_added_by_a_dead_adder(
    server: &Db,
    dir: &Path,
    alice: &Returning,
    space: uuid::Uuid,
) -> Returning {
    let carol = Returning::new(dir, account(0xc0), 3);
    carol.open(server).close();
    let adder = alice.open_quiet(server);
    adder.add_member(server, space, &carol.id());
    adder.close();
    assert_eq!(deliveries_to(server, space, &carol.id()), 0);
    carol
}

/// Contract: the background pass counts as started only once its spaces
/// are listed: a listing that failed is tried again after its wait, in the
/// same session.
///
/// ```text
/// bob ──start: listing ✗──► wait ──► listing ✓ ──► carol served
/// ```
#[test]
fn a_failed_listing_of_the_background_pass_is_retried() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let carol = carol_added_by_a_dead_adder(&server, dir.path(), &alice, spaces[1]);
        let session = bob.open_quiet(&server);
        session
            .client
            .advance_settle_clock(std::time::Duration::ZERO);
        session.client.fail_background_lists_for_test(1);
        for _ in 0..300 {
            session.runtime.drive();
            block_on(session.db.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 0);
        session
            .client
            .advance_settle_clock(std::time::Duration::from_secs(3));
        session.drive_until(&server, |_| {
            deliveries_to(&server, spaces[1], &carol.id()) == 1
        });
        session.close();
    });
}

/// Contract: the first connection runs the background steps that failed
/// for want of it: a host that reports the link down as the app opens and
/// up once connected still serves Carol in that session.
///
/// ```text
/// bob ──"offline", start: steps fail offline──► "online": steps run ──► carol served
/// ```
#[test]
fn the_first_connection_runs_the_steps_that_waited_for_it() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let carol = carol_added_by_a_dead_adder(&server, dir.path(), &alice, spaces[1]);
        let session = bob.open_quiet(&server);
        session.runtime.set_online(false);
        session.settle(&server);
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 0);
        session.runtime.set_online(true);
        session.settle(&server);
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 1);
        let (blocking, _) = split(session.work());
        assert!(
            blocking.iter().all(|(kind, _)| *kind != "pass"),
            "no reconnect pass: {blocking:?}"
        );
        session.close();
    });
}

/// Contract: no background step runs while a read is due: a read whose
/// first settle failed is due again at the next turn, and no step starts
/// in between.
///
/// ```text
/// bob ──reads S1: acquire ✗ (due again)──► next turn: acquire ✓ ──► steps
/// ```
#[test]
fn no_background_step_runs_while_a_read_is_due() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, _bob, spaces, session, _deliveries) =
            bob_with_a_stored_delivery(&server, dir.path());
        let rotated = session.address(spaces[0]);
        session.client.fail_acquisitions_for_test(1);
        let _read = session.follow(
            &server,
            Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(spaces[0])))),
        );
        let acquire = ("acquire", rotated.clone());
        let work = session.drive_until(&server, |work| {
            work.iter().filter(|entry| **entry == acquire).count() == 2
        });
        let first = work.iter().position(|entry| *entry == acquire).unwrap();
        let second = work.iter().rposition(|entry| *entry == acquire).unwrap();
        assert!(
            work[first..second]
                .iter()
                .all(|(kind, _)| *kind != "background"),
            "{work:?}"
        );
        session.close();
    });
}

/// Contract: a background step whose acquisition failed still runs its
/// pass with the keys already held: Carol is served before any retry.
///
/// ```text
/// bob ──background step S1: acquire ✗, pass with the held key──► carol served
/// ```
#[test]
fn a_failed_acquisition_does_not_stop_the_steps_pass() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, _bob, spaces, session, _deliveries) =
            bob_with_a_stored_delivery(&server, dir.path());
        let carol = carol_added_by_a_dead_adder(&server, dir.path(), &alice, spaces[0]);
        session
            .client
            .advance_settle_clock(std::time::Duration::ZERO);
        session.client.fail_acquisitions_for_test(1);
        session.drive_until(&server, |_| {
            deliveries_to(&server, spaces[0], &carol.id()) > 0
        });
        session.close();
    });
}

/// Contract: the background pass also covers a space the device holds no
/// key of yet when a delivery to its account is stored: it acquires it with
/// no read.
///
/// ```text
/// alice ──new space Z, delivered to bob──► bob's store syncs it
/// bob   ──background step Z: acquire
/// ```
#[test]
fn a_stored_delivery_of_an_unheld_space_is_acquired_in_the_background() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, _spaces) = bob_holding(&server, dir.path(), 3);
        let holder = alice.open_quiet(&server);
        let unheld = holder.space_of(&server, &[alice.id(), bob.id()]);
        drive(
            &server,
            &[&holder.db],
            holder.client.deliver_keys(&holder.db),
        )
        .unwrap();
        pump(&server, &holder.db);
        holder.close();
        assert_eq!(deliveries_to(&server, unheld, &bob.id()), 1);

        let session = bob.open_quiet(&server);
        let target = session.address(unheld);
        let _deliveries = session.follow(
            &server,
            Query::from(DELIVERIES).filter(eq(
                col("recipientAccountId"),
                lit(Value::Uuid(bob.id().parse().unwrap())),
            )),
        );
        let work = session.drive_until(&server, |work| {
            work.iter()
                .any(|entry| *entry == ("acquire", target.clone()))
        });
        let at = work
            .iter()
            .position(|entry| *entry == ("acquire", target.clone()))
            .unwrap();
        assert_eq!(work[at - 1], ("background", target.clone()), "{work:?}");
        session.close();
    });
}

/// Contract: the deadline the runtime gives its host is the earliest of all
/// its pending work: a read in back-off still wakes the host on time while
/// a background step waits on the server.
///
/// ```text
/// bob ──reads S1: acquire ✗, ✗ (back-off)──► background step (never answers)
///     ──deadline told to the host: the read's retry
/// ```
#[test]
fn a_background_step_does_not_hide_a_reads_retry_deadline() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, _bob, spaces, session, _deliveries) =
            bob_with_a_stored_delivery(&server, dir.path());
        let rotated = session.address(spaces[0]);
        let told = Rc::new(std::cell::RefCell::new(Vec::new()));
        session.bridge.set_store_wake(Some(Box::new({
            let told = told.clone();
            move |wake| {
                if let StoreWake::Deadline(delay) = wake {
                    told.borrow_mut().push(delay);
                }
            }
        })));
        session.client.hold_background_steps_for_test(true);
        session.client.fail_acquisitions_for_test(2);
        let _read = session.follow(
            &server,
            Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(spaces[0])))),
        );
        let acquire = ("acquire", rotated.clone());
        let mut failed = 0;
        session.drive_until(&server, |work| {
            failed = work.iter().filter(|entry| **entry == acquire).count();
            failed == 2 && work.iter().any(|(kind, _)| *kind == "background")
        });
        let last = told.borrow().last().copied().flatten();
        assert!(
            last.is_some_and(|delay| delay <= std::time::Duration::from_secs(1)),
            "the host was told {last:?}, not the read's retry"
        );
        session.close();
    });
}

/// Contract: a connection notice abandons the background step running on
/// the old connection and puts its space back, so the work restarts on the
/// new one: Carol is served after the reconnect.
///
/// ```text
/// bob ──background step stuck on the old connection
///     ──offline, online──► step put back; reconnect pass ──► carol served
/// ```
#[test]
fn a_reconnect_restarts_a_step_left_on_the_old_connection() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let carol = carol_added_by_a_dead_adder(&server, dir.path(), &alice, spaces[1]);
        let session = bob.open_quiet(&server);
        session.runtime.set_online(true);
        session.client.hold_background_steps_for_test(true);
        session.drive_until(&server, |work| {
            work.iter().any(|(kind, _)| *kind == "background")
        });
        for _ in 0..200 {
            session.runtime.drive();
            block_on(session.db.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 0);

        // The connection drops and comes back; the stuck request is lost.
        session.runtime.set_online(false);
        session.runtime.set_online(true);
        session.client.hold_background_steps_for_test(false);
        session.drive_until(&server, |_| {
            deliveries_to(&server, spaces[1], &carol.id()) == 1
        });
        session.close();
    });
}

/// Contract: a step stopped midway, at any of its waits, for a read that
/// became due is put back and completes later in the session.
///
/// ```text
/// bob ──step S2: pass begins ──► app reads S1 (due) ──► step S2 dropped
///     ──read settled ──► step S2 again ──► carol served
/// ```
#[test]
fn a_step_stopped_midway_for_a_read_is_put_back_and_completes() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, _bob, spaces, session, _deliveries) =
            bob_with_a_stored_delivery(&server, dir.path());
        let carol = carol_added_by_a_dead_adder(&server, dir.path(), &alice, spaces[1]);
        // A space Bob is a member of with no delivery to him: its read
        // stays due until settled.
        let holder = alice.open_quiet(&server);
        let keyless = holder.space_of(&server, &[alice.id(), _bob.id()]);
        holder.close();
        let target = session.address(spaces[1]);
        let mut work = session.drive_until(&server, |work| {
            work.iter().any(|entry| *entry == ("pass", target.clone()))
        });
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 0);
        let _read = session.follow(
            &server,
            Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(keyless)))),
        );
        work.extend(session.drive_until(&server, |_| {
            deliveries_to(&server, spaces[1], &carol.id()) == 1
        }));
        let steps = work
            .iter()
            .filter(|entry| **entry == ("background", target.clone()))
            .count();
        assert_eq!(steps, 2, "stopped midway, then run again: {work:?}");
        session.close();
    });
}

/// Contract: a step that keeps failing is retried all session but told
/// `delivery-failed` once per space while its reason stays the same.
///
/// ```text
/// bob ──step S1 ✗ (told) ──► retry ✗ ──► retry ✗ (not told again)
/// ```
#[test]
fn a_step_that_keeps_failing_is_told_once() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, _bob, spaces, session, _deliveries) =
            bob_with_a_stored_delivery(&server, dir.path());
        let rotated = session.address(spaces[0]);
        let heard = observe_reports(&session.client);
        session
            .client
            .advance_settle_clock(std::time::Duration::ZERO);
        session.client.fail_acquisitions_for_test(u32::MAX);
        let attempts = |work: &[(&'static str, SpaceAddress)]| {
            work.iter()
                .filter(|entry| **entry == ("background", rotated.clone()))
                .count()
        };
        let mut seen = 0;
        for _ in 0..3 {
            let work = session.drive_until(&server, |work| attempts(work) == 1);
            seen += attempts(&work);
            // Let the step finish failing before its wait is stepped over.
            for _ in 0..200 {
                session.runtime.drive();
                block_on(session.db.tick()).unwrap();
                block_on(server.tick()).unwrap();
            }
            session
                .client
                .advance_settle_clock(std::time::Duration::from_secs(40));
        }
        assert_eq!(seen, 3);
        let told = heard
            .borrow()
            .iter()
            .filter(|report| {
                matches!(report, jazz_e2ee_client::Report::DeliveryFailed { space, .. }
                    if *space == rotated)
            })
            .count();
        assert_eq!(told, 1);
        session.close();
    });
}

/// Contract: a background step never holds the runtime: while one waits
/// forever on the server, a local call (`deviceId`) and a read are both
/// answered at once.
///
/// ```text
/// bob ──background step (never answers)
///     ──deviceId ──► answered;  reads S1 ──► acquired
/// ```
#[test]
fn a_slow_background_step_does_not_hold_back_calls_or_reads() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, _bob, spaces, session, _deliveries) =
            bob_with_a_stored_delivery(&server, dir.path());
        let rotated = session.address(spaces[0]);
        session.client.hold_background_steps_for_test(true);
        session.drive_until(&server, |work| {
            work.iter().any(|(kind, _)| *kind == "background")
        });
        let mut call = session
            .runtime
            .call("deviceId".to_owned(), serde_json::json!({}));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let mut answer = None;
        for _ in 0..200 {
            if let std::task::Poll::Ready(result) = call.as_mut().poll(&mut cx) {
                answer = Some(result);
                break;
            }
            session.runtime.drive();
            block_on(session.db.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        let device = session.client.device().unwrap().id.clone();
        assert_eq!(answer, Some(Ok(serde_json::json!(device))));
        let _read = session.follow(
            &server,
            Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(spaces[0])))),
        );
        session.drive_until(&server, |work| {
            work.iter()
                .any(|entry| *entry == ("acquire", rotated.clone()))
        });
        session.close();
    });
}

impl Opened {
    /// Runs the runtime and the sync for `turns` turns.
    fn turn(&self, server: &Db, turns: usize) -> Vec<(&'static str, SpaceAddress)> {
        let mut work = Vec::new();
        for _ in 0..turns {
            self.runtime.drive();
            work.extend(self.work());
            block_on(self.db.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        work
    }

    /// Calls `deviceId` and waits for its answer, driving the runtime.
    fn device_id(&self, server: &Db) -> Vec<(&'static str, SpaceAddress)> {
        let mut work = Vec::new();
        let mut call = self
            .runtime
            .call("deviceId".to_owned(), serde_json::json!({}));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        for _ in 0..200 {
            if let std::task::Poll::Ready(answer) = call.as_mut().poll(&mut cx) {
                answer.unwrap();
                return work;
            }
            work.extend(self.turn(server, 1));
        }
        panic!("deviceId was not answered");
    }
}

/// Contract: background work never pre-empts background work: a retry that
/// comes due while another space's step is stalled waits for that step's
/// bound, and the runtime keeps a deadline for both; the retry then runs,
/// and the stalled space completes once its server answers.
///
/// ```text
/// bob ──step S1 ✗ (retry in 2 s) ──► step S2 stalls
///     ──3 s: S1 waits ──► 16 s: S2 timed out, S1 again ──► S2 answers ──► carol served
/// ```
#[test]
fn a_stalled_step_ends_at_its_bound_and_a_due_retry_runs() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, _bob, spaces, session, _deliveries) =
            bob_with_a_stored_delivery(&server, dir.path());
        let carol = carol_added_by_a_dead_adder(&server, dir.path(), &alice, spaces[1]);
        let (retried, stalled) = (session.address(spaces[0]), session.address(spaces[1]));
        let told = Rc::new(std::cell::RefCell::new(Vec::new()));
        session.bridge.set_store_wake(Some(Box::new({
            let told = told.clone();
            move |wake| {
                if let StoreWake::Deadline(delay) = wake {
                    told.borrow_mut().push(delay);
                }
            }
        })));
        session
            .client
            .advance_settle_clock(std::time::Duration::ZERO);
        session.client.fail_acquisitions_for_test(1);
        session
            .client
            .hold_background_step_of_for_test(Some(&stalled));
        let acquisitions = |work: &[(&'static str, SpaceAddress)]| {
            work.iter()
                .filter(|entry| **entry == ("acquire", retried.clone()))
                .count()
        };
        let mut work = session.drive_until(&server, |work| {
            acquisitions(work) == 1
                && work
                    .iter()
                    .any(|entry| *entry == ("background", stalled.clone()))
        });
        work.extend(session.turn(&server, 200));
        assert_eq!(acquisitions(&work), 1);
        let last = told.borrow().last().copied().flatten();
        assert!(
            last.is_some_and(|delay| delay <= std::time::Duration::from_secs(3)),
            "the host was told {last:?}, not the retry"
        );
        session
            .client
            .advance_settle_clock(std::time::Duration::from_secs(3));
        assert_eq!(
            acquisitions(&session.turn(&server, 200)),
            0,
            "no pre-emption"
        );
        session
            .client
            .advance_settle_clock(std::time::Duration::from_secs(13));
        session.drive_until(&server, |work| acquisitions(work) == 1);
        session.client.hold_background_step_of_for_test(None);
        session
            .client
            .advance_settle_clock(std::time::Duration::from_secs(3));
        session.drive_until(&server, |_| {
            deliveries_to(&server, spaces[1], &carol.id()) == 1
        });
        session.close();
    });
}

/// Contract: two background retries that come due together both complete:
/// neither pre-empts the other.
///
/// ```text
/// bob ──steps S1, S2: acquire ✗ ✗ ──► 3 s ──► S1 ✓, S2 ✓
/// ```
#[test]
fn two_due_retries_both_complete() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let session = alice.open(&server);
        for space in &spaces[..2] {
            let address = session.address(*space);
            drive(
                &server,
                &[&session.db],
                session.client.rotate(&session.db, &address),
            )
            .unwrap();
        }
        session.settle(&server);
        session.close();
        let session = bob.open_quiet(&server);
        let _deliveries = session.follow(
            &server,
            Query::from(DELIVERIES).filter(eq(
                col("recipientAccountId"),
                lit(Value::Uuid(bob.id().parse().unwrap())),
            )),
        );
        session
            .client
            .advance_settle_clock(std::time::Duration::ZERO);
        session.client.fail_acquisitions_for_test(2);
        let versions = |space: uuid::Uuid| {
            let address = session.address(space);
            let address = jazz_e2ee::protocol::records::space::SpaceAddress::new(
                &address.scope_id,
                &address.identifier,
            )
            .unwrap();
            session.client.with_held(|held| {
                held.roots_of(&address)
                    .iter()
                    .map(|root| root.held().count())
                    .sum::<usize>()
            })
        };
        session.drive_until(&server, |work| {
            work.iter().filter(|(kind, _)| *kind == "acquire").count() == 2
        });
        session.turn(&server, 200);
        session
            .client
            .advance_settle_clock(std::time::Duration::from_secs(3));
        session.drive_until(&server, |_| {
            versions(spaces[0]) == 2 && versions(spaces[1]) == 2
        });
        session.close();
    });
}

/// Contract: a step stopped again and again goes behind the others: with a
/// slow first space and repeated local calls, the other spaces still
/// complete.
///
/// ```text
/// bob ──step S1 stalls ──► deviceId ×6 ──► S2, S3 run ──► carol served
/// ```
#[test]
fn repeated_calls_do_not_starve_the_other_spaces() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let carol = carol_added_by_a_dead_adder(&server, dir.path(), &alice, spaces[1]);
        let session = bob.open_quiet(&server);
        let slow = session.address(spaces[0]);
        session.client.hold_background_step_of_for_test(Some(&slow));
        let mut work = session.drive_until(&server, |work| {
            work.iter()
                .any(|entry| *entry == ("background", slow.clone()))
        });
        for _ in 0..6 {
            work.extend(session.device_id(&server));
            work.extend(session.turn(&server, 50));
        }
        // The slow space was stopped by the calls and ran again, behind the
        // others, which completed.
        let runs = work
            .iter()
            .filter(|entry| **entry == ("background", slow.clone()))
            .count();
        assert!(runs >= 2, "{work:?}");
        assert_eq!(deliveries_to(&server, spaces[1], &carol.id()), 1);
        session.close();
    });
}

/// The `delivery-failed` reports of `space` a listener heard.
fn delivery_failures(
    heard: &std::cell::RefCell<Vec<jazz_e2ee_client::Report>>,
    space: &SpaceAddress,
) -> Vec<String> {
    heard
        .borrow()
        .iter()
        .filter_map(|report| match report {
            jazz_e2ee_client::Report::DeliveryFailed {
                space: failed,
                reason,
            } if failed == space => Some(reason.clone()),
            _ => None,
        })
        .collect()
}

/// Contract: a step that runs past its bound has failed: it is told
/// `delivery-failed` once (its reason a timeout), however often it times
/// out again.
///
/// ```text
/// bob ──step S1 stalls ──► 16 s: timed out (told) ──► retry stalls ──► 16 s: timed out
/// ```
#[test]
fn a_step_past_its_bound_is_told_failed_once() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let session = bob.open_quiet(&server);
        let heard = observe_reports(&session.client);
        let slow = session.address(spaces[0]);
        session
            .client
            .advance_settle_clock(std::time::Duration::ZERO);
        session.client.hold_background_step_of_for_test(Some(&slow));
        let steps = |work: &[(&'static str, SpaceAddress)]| {
            work.iter()
                .filter(|entry| **entry == ("background", slow.clone()))
                .count()
        };
        for _ in 0..2 {
            session.drive_until(&server, |work| steps(work) == 1);
            session.turn(&server, 50);
            session
                .client
                .advance_settle_clock(std::time::Duration::from_secs(16));
            session.turn(&server, 50);
            // Past the retry's wait.
            session
                .client
                .advance_settle_clock(std::time::Duration::from_secs(3));
        }
        let reasons = delivery_failures(&heard, &slow);
        assert_eq!(reasons.len(), 1, "{reasons:?}");
        assert!(reasons[0].contains("15 s"), "{reasons:?}");
        session.close();
    });
}

/// Contract: a step's bound counts from its first run: being stopped and
/// put back does not renew it.
///
/// ```text
/// bob ──step S1 stalls ──► (5 s, deviceId) ×4 ──► timed out at 20 s
/// ```
#[test]
fn stopping_a_step_does_not_renew_its_bound() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let session = bob.open_quiet(&server);
        let heard = observe_reports(&session.client);
        let slow = session.address(spaces[0]);
        session
            .client
            .advance_settle_clock(std::time::Duration::ZERO);
        session.client.hold_background_step_of_for_test(Some(&slow));
        session.drive_until(&server, |work| {
            work.iter()
                .any(|entry| *entry == ("background", slow.clone()))
        });
        for _ in 0..4 {
            session
                .client
                .advance_settle_clock(std::time::Duration::from_secs(5));
            let _ = session.device_id(&server);
            session.turn(&server, 50);
        }
        let reasons = delivery_failures(&heard, &slow);
        assert_eq!(reasons.len(), 1, "{reasons:?}");
        session.close();
    });
}

/// Contract: at the queue's real limit (10,000 rows left out), the report
/// of an entry the full queue evicts reaches the listeners even when a
/// settlement of thousands of rows follows it.
///
/// ```text
/// bob ──Y: 1 row, X: 10,000 rows queued (Y evicted, reported)
///     ──settle X offline ──► listeners hear Y and X
/// ```
#[test]
fn an_evicted_missing_key_is_reported_at_the_queue_limit() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, _spaces) = bob_holding(&server, dir.path(), 1);
        let holder = alice.open_quiet(&server);
        let keyless = holder.space_of(&server, &[alice.id(), bob.id()]);
        holder.close();
        let session = bob.open_quiet(&server);
        let heard = observe_reports(&session.client);
        let x = session.address(keyless);
        let y = SpaceAddress {
            scope_id: x.scope_id.clone(),
            identifier: uuid::Uuid::new_v4().to_string(),
        };
        let epoch = uuid::Uuid::new_v4().to_string();
        session
            .client
            .queue_missing_for_test("notes", &y, &epoch, 1);
        session
            .client
            .queue_missing_for_test("notes", &x, &epoch, jazz_e2ee_client::MAX_REPORTS);
        // Settled offline: no fetch runs, so nothing is delivered before the
        // settlement's own reports.
        session.client.set_online(false);
        let _ = drive(
            &server,
            &[&session.db],
            session.client.settle_reads(&session.db),
        );
        let missing: BTreeSet<_> = heard
            .borrow()
            .iter()
            .filter_map(|report| match report {
                jazz_e2ee_client::Report::MissingKey { space, .. } => Some(space.clone()),
                _ => None,
            })
            .collect();
        assert!(missing.contains(&y), "the evicted space's report was lost");
        assert!(missing.contains(&x));
        session.close();
    });
}

/// Contract: as above, online, when the settlement's fetch fails.
///
/// ```text
/// bob ──Y: 1 row, X: 10,000 rows queued (Y evicted, reported)
///     ──settle X, its fetch fails ──► listeners hear Y and X
/// ```
#[test]
fn an_evicted_missing_key_is_reported_when_the_settling_fetch_fails() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, _spaces) = bob_holding(&server, dir.path(), 1);
        let holder = alice.open_quiet(&server);
        let keyless = holder.space_of(&server, &[alice.id(), bob.id()]);
        holder.close();
        let session = bob.open_quiet(&server);
        let heard = observe_reports(&session.client);
        let x = session.address(keyless);
        let y = SpaceAddress {
            scope_id: x.scope_id.clone(),
            identifier: uuid::Uuid::new_v4().to_string(),
        };
        let epoch = uuid::Uuid::new_v4().to_string();
        session
            .client
            .queue_missing_for_test("notes", &y, &epoch, 1);
        session
            .client
            .queue_missing_for_test("notes", &x, &epoch, jazz_e2ee_client::MAX_REPORTS);
        // The fetch fails at once: nothing is delivered before the
        // settlement's own reports.
        session.client.fail_acquisitions_for_test(1);
        let _ = drive(
            &server,
            &[&session.db],
            session.client.settle_reads(&session.db),
        );
        let missing: BTreeSet<_> = heard
            .borrow()
            .iter()
            .filter_map(|report| match report {
                jazz_e2ee_client::Report::MissingKey { space, .. } => Some(space.clone()),
                _ => None,
            })
            .collect();
        assert!(missing.contains(&y), "the evicted space's report was lost");
        assert!(missing.contains(&x));
        session.close();
    });
}

/// Contract: every wake of a running step republishes the earliest deadline
/// of all pending work, the step's own bound included: once another
/// space's rotation alarm has fired, the host still has the step's bound.
///
/// ```text
/// bob ──rotation rest of S3 due in 2 s; step S1 stalls
///     ──the 2 s alarm fires ──► the host is told the step's bound (~13 s)
/// ```
#[test]
fn every_wake_republishes_the_next_deadline() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (_alice, bob, spaces) = bob_holding(&server, dir.path(), 3);
        let session = bob.open_quiet(&server);
        let told = Rc::new(std::cell::RefCell::new(Vec::new()));
        session.bridge.set_store_wake(Some(Box::new({
            let told = told.clone();
            move |wake| {
                if let StoreWake::Deadline(delay) = wake {
                    told.borrow_mut().push(delay);
                }
            }
        })));
        let slow = session.address(spaces[0]);
        session
            .client
            .advance_settle_clock(std::time::Duration::ZERO);
        session.client.hold_background_step_of_for_test(Some(&slow));
        session.drive_until(&server, |work| {
            work.iter()
                .any(|entry| *entry == ("background", slow.clone()))
        });
        session
            .client
            .rotation_retry_in_for_test(
                &session.address(spaces[2]),
                std::time::Duration::from_secs(2),
            )
            .unwrap();
        session.turn(&server, 20);
        // The rotation's alarm fires.
        session
            .client
            .advance_settle_clock(std::time::Duration::from_secs(3));
        session.turn(&server, 20);
        let last = told.borrow().last().copied().flatten();
        assert!(
            last.is_some_and(|delay| delay >= std::time::Duration::from_secs(5)),
            "the host was told {last:?}, not the step's bound"
        );
        session.close();
    });
}

/// Contract: a missing key's report is queued once however many rows of it
/// a full queue evicts: with 20,000 rows of the second key, the first key's
/// report is not pushed out, and both keys are reported.
///
/// ```text
/// bob ──Y: 1 row, X: 20,000 rows queued (Y and 10,000 X rows evicted)
///     ──settle X ──► listeners hear Y and X
/// ```
#[test]
fn many_evictions_of_one_key_do_not_push_out_another_keys_report() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let dir = tempfile::tempdir().unwrap();
        let (alice, bob, _spaces) = bob_holding(&server, dir.path(), 1);
        let holder = alice.open_quiet(&server);
        let keyless = holder.space_of(&server, &[alice.id(), bob.id()]);
        holder.close();
        let session = bob.open_quiet(&server);
        let heard = observe_reports(&session.client);
        let x = session.address(keyless);
        let y = SpaceAddress {
            scope_id: x.scope_id.clone(),
            identifier: uuid::Uuid::new_v4().to_string(),
        };
        let epoch = uuid::Uuid::new_v4().to_string();
        session
            .client
            .queue_missing_for_test("notes", &y, &epoch, 1);
        session.client.queue_missing_for_test(
            "notes",
            &x,
            &epoch,
            2 * jazz_e2ee_client::MAX_REPORTS,
        );
        let _ = drive(
            &server,
            &[&session.db],
            session.client.settle_reads(&session.db),
        );
        let missing: BTreeSet<_> = heard
            .borrow()
            .iter()
            .filter_map(|report| match report {
                jazz_e2ee_client::Report::MissingKey { space, .. } => Some(space.clone()),
                _ => None,
            })
            .collect();
        assert!(
            missing.contains(&y),
            "the first key's report was pushed out"
        );
        assert!(missing.contains(&x));
        session.close();
    });
}
