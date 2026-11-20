//! Spaces on a device against a real authority with the system tables
//! (`E2EE_PROTOCOL.md` §3.2, §3.3, §4.2, §4.3, §5.4): a space is created by
//! the first encrypted write, its keys are acquired from the rows the
//! server holds, never built by the test, and what a read cannot open is
//! settled into the reports of §4.3.

mod common;

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
use jazz_e2ee_client::{
    AccountStatus, ClientError, E2eeClient, MemoryStore, PrivateStore, Report, SpaceCreation,
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
    let settings = EncryptionSettings::default();
    let client = open_account(Rc::new(store), who, settings.clone(), None);
    let (db, link) = session(&client, server, who, &settings, node, None);
    Device {
        who,
        client,
        db,
        _link: Some(link),
        live: Default::default(),
    }
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
}

/// A project row and the creator's member row, accepted.
fn new_project(server: &Db, creator: &Device, name: &str) -> uuid::Uuid {
    let space = project(&creator.db, name);
    pump(server, &creator.db);
    add_member(server, creator, space, &creator.id());
    space
}

fn add_member(server: &Db, by: &Device, space: uuid::Uuid, account: &str) {
    let scope = by.db.table_identity("projects").unwrap().unwrap().0;
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

/// A note written through the runtime's write wrapper.
fn write_note(
    server: &Db,
    device: &Device,
    space: uuid::Uuid,
    body: &str,
) -> Result<jazz::db::WriteHandle, ClientError> {
    drive(
        server,
        &[&device.db],
        device.client.write(&device.db, async || {
            device
                .db
                .insert("notes", note(space, body), Default::default())
                .await
        }),
    )
}

/// The notes of `space`, read through the runtime's read wrapper.
fn read_notes(
    server: &Db,
    device: &Device,
    space: uuid::Uuid,
) -> Result<Vec<jazz::db::CurrentRow>, ClientError> {
    let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
    drive(
        server,
        &[&device.db],
        device.client.read(&device.db, async || {
            let prepared = device.db.prepare_query(&query)?;
            device.db.all(&prepared, local()).await
        }),
    )
}

fn space_address(device: &Device, space: uuid::Uuid) -> jazz::e2ee::SpaceAddress {
    let scope = device.db.table_identity("projects").unwrap().unwrap().0;
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
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");

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
        let wire_secret = all(&server, Query::from("notes")).unwrap();
        for row in &wire_secret {
            for column in ["body", "note"] {
                if let Some(Value::Bytes(bytes)) = row.cell(column) {
                    assert!(!contains(&bytes, SECRET.as_bytes()));
                }
            }
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
        let found = drive(
            &server,
            &[&alice.db],
            alice.client.read(&alice.db, async || {
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
    let seen = Rc::new(RefCell::new(Vec::new()));
    let sink = seen.clone();
    device
        .client
        .on_report(move |report| sink.borrow_mut().push(report.clone()));
    seen
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
            drive(
                &server,
                &[&bob.db],
                bob.client.acquire(&bob.db, &space_address(&bob, space))
            )
            .unwrap(),
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
        assert!(first.client.take_reports().is_empty());
        let synced = drive(
            &server,
            &[&first.db],
            first.client.sync_spaces(&first.db, &mut watch),
        )
        .unwrap();
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
    assert!(client.prepare(&db).unwrap());
    db
}

/// Contract: a creation whose outcome is unknown when the device restarts
/// is reconciled by its root, never by staging another: the key stays
/// `unpublished`, `createSpace` reports the creation pending and stages
/// no second root, and once the authority accepts the root the mark is
/// cleared.
///
/// ```text
/// alice ──note + root (not uploaded)──► her store      (restart)
/// alice ──reconcile: root pending──► key unpublished; create_space: Pending
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
            block_on(alice.client.write(&alice.db, async || {
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
        block_on(alice.client.write(&alice.db, async || {
            alice
                .db
                .insert("notes", note(space, "again"), Default::default())
                .await
        }))
        .unwrap();
        alice.client.set_online(true);
        let created = block_on(alice.client.create_space(
            &alice.db,
            "projects",
            jazz::ids::RowUuid(space),
        ))
        .unwrap();
        assert_eq!(created, SpaceCreation::Pending);

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
    assert!(client.prepare(&db).unwrap());
    Device {
        who,
        client,
        db,
        _link: Some(link),
        live: Default::default(),
    }
}

/// Contract: a member who may not update the space row is refused when
/// she creates the space: `space-create-refused`, no root and no key
/// that writes, and, while the client runs, her encrypted writes fail
/// `space-pending` without staging a root. A write the authority refuses
/// as a whole (root and note together) leaves neither. An explicit
/// `createSpace` attempts again, and creates the space once she may
/// update the row.
///
/// ```text
/// bob (member, not owner) ──create_space──► server ✗ refused
/// bob ──write──✗ space-pending (no root staged)
/// alice ──ownerId = bob──► server; bob ──create_space──► Created
/// ```
#[test]
fn a_refused_root_stops_automatic_creation_while_the_client_runs() {
    within(|| {
        let authority = block_on(Db::open(jazz::db::DbConfig::new(
            owned_schema(),
            identity(0),
        )))
        .unwrap();
        authority.set_authority().unwrap();
        let server = authority;
        let alice = owned_device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let bob = owned_device(&server, account(0xb0), MemoryStore::new(), 2);
        bob.activate(&server);
        let owner = |who: &Device| Value::Uuid(uuid::Uuid::parse_str(&who.id()).unwrap());
        let space = block_on(alice.db.insert(
            "projects",
            BTreeMap::from([
                ("name".to_owned(), text("plans")),
                ("ownerId".to_owned(), owner(&alice)),
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

        let refused = drive(
            &server,
            &[&bob.db, &alice.db],
            bob.client
                .create_space(&bob.db, "projects", jazz::ids::RowUuid(space)),
        );
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::Refused(
                    jazz_e2ee::protocol::error::RefusalCode::SpaceCreateRefused
                )))
            ),
            "{refused:?}"
        );
        assert_eq!(count(&server, SPACES), 0);
        assert_eq!(marks(&bob, space), [KeyMark::Refused]);

        let error = match write_note(&server, &bob, space, SECRET) {
            Err(ClientError::Database(error)) => error,
            other => panic!("{other:?}"),
        };
        assert_eq!(error.reason, Some(jazz::db::Reason::SpacePending));
        assert!(all(&bob.db, Query::from(SPACES)).unwrap().is_empty());

        // Her client restarts: the next automatic path may try once, and
        // the authority refuses the root and the note together.
        let store = MemoryStore::new();
        let _ = store;
        let owned = |who: &Device, to: &Device| {
            block_on(who.db.update(
                "projects",
                jazz::ids::RowUuid(space),
                BTreeMap::from([("ownerId".to_owned(), owner(to))]),
                Default::default(),
            ))
            .unwrap();
        };
        owned(&alice, &bob);
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        let created = drive(
            &server,
            &[&bob.db, &alice.db],
            bob.client
                .create_space(&bob.db, "projects", jazz::ids::RowUuid(space)),
        );
        assert_eq!(created.unwrap(), SpaceCreation::Created);
        assert_eq!(count(&server, SPACES), 1);
        let mut now = marks(&bob, space);
        now.sort_by_key(|mark| format!("{mark:?}"));
        assert_eq!(now, [KeyMark::Published, KeyMark::Refused]);
    });
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
        block_on(alice.client.write(&alice.db, async || {
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
        let scope = alice.db.table_identity("projects").unwrap().unwrap().0;
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
                db.table_identity("projects").unwrap().unwrap().0,
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

/// Contract: a device acquires without waiting for a read when the rows
/// it watches change, and the reports that waited for it are withdrawn:
/// bob, a member of a space with no root, is told `space-pending` by a
/// query; when alice creates the space, his watch sees the root and
/// withdraws it (he now waits for a delivery: `key-pending`).
///
/// ```text
/// bob ──eq query──► space-pending     alice ──note + root──► server
/// bob ──sync_spaces (roots watched)──► withdrawn; key-pending
/// ```
#[test]
fn watched_rows_acquire_and_withdraw_reports() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let (alice, bob, space) = two_members(&server);
        let events = reports_of(&bob);
        let query = Query::from("notes")
            .filter(eq(col("projectId"), lit(Value::Uuid(space))))
            .filter(eq(col("body"), lit(text(SECRET))));
        let refused = drive(
            &server,
            &[&bob.db],
            bob.client.read(&bob.db, async || {
                let prepared = bob.db.prepare_query(&query)?;
                bob.db.all(&prepared, local()).await
            }),
        );
        assert!(refused.is_err());
        assert_eq!(codes(&events), [(UnreadableCode::SpacePending, false)]);

        let mut watch = block_on(bob.client.watch_spaces(&bob.db)).unwrap();
        drive(
            &server,
            &[&bob.db],
            bob.client.sync_spaces(&bob.db, &mut watch),
        )
        .unwrap();
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        pump(&server, &bob.db);
        let results = drive(
            &server,
            &[&bob.db, &alice.db],
            bob.client.sync_spaces(&bob.db, &mut watch),
        )
        .unwrap();
        assert!(
            results
                .iter()
                .any(|(_, state)| matches!(state, Ok(SpaceKeyState::KeyPending))),
            "{results:?}"
        );
        assert!(events.borrow().iter().any(|report| matches!(
            report,
            Report::Withdrawn {
                code: UnreadableCode::SpacePending,
                ..
            }
        )));
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
        authority.set_authority().unwrap();
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
        let stopped = match write_note(&server, &bob, space, "third") {
            Err(ClientError::Database(error)) => error,
            other => panic!("{other:?}"),
        };
        assert_eq!(stopped.reason, Some(jazz::db::Reason::SpacePending));
        assert_eq!(count(&server, SPACES), 0);
    });
}

/// A root row of `space` that this client cannot decode: `version` is out
/// of its range (as a newer format's would be, or junk).
fn undecodable_root(server: &Db, by: &Device, space: uuid::Uuid, version: i64) {
    let scope = by.db.table_identity("projects").unwrap().unwrap().0;
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
        let created = drive(
            &server,
            &[&alice.db],
            alice
                .client
                .create_space(&alice.db, "projects", jazz::ids::RowUuid(space)),
        );
        assert!(
            matches!(created, Err(ClientError::Unsupported(_))),
            "{created:?}"
        );
        assert_eq!(count(&server, SPACES), 2);
        assert!(
            codes(&events)
                .iter()
                .all(|(code, _)| *code != UnreadableCode::SpacePending)
        );
    });
}

/// Contract: a read that shows no root of a space whose key this device
/// holds (the server hides the root rows) never gets a second root: an
/// explicit `createSpace` returns the existing space and stages nothing.
///
/// ```text
/// alice (holds K₀) ──create_space──► server has hidden the root ✗ no root staged
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
        let created = drive(
            &hiding,
            &[&alice.db],
            alice
                .client
                .create_space(&alice.db, "projects", jazz::ids::RowUuid(space)),
        )
        .unwrap();
        assert_eq!(created, SpaceCreation::Existing);
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
        block_on(alice.client.write(&alice.db, async || {
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

/// Contract: a write that fails after it staged the space's root leaves no
/// creation: its key is `refused`, and the next write prepares a new root
/// and creates the space.
#[test]
fn a_write_that_fails_after_staging_its_root_abandons_the_creation() {
    within(|| {
        let server = secure_authority_with(&EncryptionSettings::default(), None);
        let alice = device(&server, account(0xa1), MemoryStore::new(), 1);
        alice.activate(&server);
        let space = new_project(&server, &alice, "plans");
        let mut bad = note(space, SECRET);
        bad.insert("title".to_owned(), Value::I32(1));
        let failed = drive(
            &server,
            &[&alice.db],
            alice.client.write(&alice.db, async || {
                alice
                    .db
                    .insert("notes", bad.clone(), Default::default())
                    .await
            }),
        );
        assert!(failed.is_err());
        assert_eq!(marks(&alice, space), [KeyMark::Refused]);
        write_note(&server, &alice, space, SECRET).unwrap();
        pump(&server, &alice.db);
        assert_eq!((count(&server, SPACES), count(&server, "notes")), (1, 1));
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
        let state = drive(
            &server,
            &[&bob.db],
            bob.client.acquire(&bob.db, &space_address(&bob, space)),
        )
        .unwrap();
        assert_eq!(state, SpaceKeyState::KeyPending);
    });
}
