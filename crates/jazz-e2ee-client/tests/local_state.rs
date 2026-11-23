//! One device's private state across restarts (`E2EE_PROTOCOL.md` §2.1,
//! §6): the device is generated once and its secrets are stored only
//! protected; held keys and floors survive a restart and are checked when
//! loaded; a damaged record is skipped and reported, an unknown format is
//! refused and never overwritten, and a wiped store is never recreated.

mod common;

use std::rc::Rc;

use common::*;
use jazz::db::block_on;
use jazz::e2ee::EncryptionSettings;
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee::OsRandom;
use jazz_e2ee::protocol::account::AccountFloor;
use jazz_e2ee::protocol::records::KeyColumns;
use jazz_e2ee::protocol::{Position, ProtocolError};
use jazz_e2ee::registry::Registry;
use jazz_e2ee_client::{ClientConfig, ClientError, E2eeClient, MemoryStore, PrivateStore, Report};

fn scope() -> String {
    format!(
        r#"["{}","{ENVIRONMENT}","{ALICE}"]"#,
        app_id().as_object_id().uuid()
    )
}

fn try_open(
    store: Rc<dyn PrivateStore>,
    declarations: jazz::e2ee::E2eeDeclarations,
) -> Result<E2eeClient, ClientError> {
    block_on(E2eeClient::open(ClientConfig {
        store,
        declarations,
        account_id: ALICE.to_owned(),
        registry: Registry::builtin(),
        random: Box::new(OsRandom),
        device_account: None,
    }))
}

fn skipped(client: &E2eeClient) -> Vec<String> {
    client
        .take_reports()
        .into_iter()
        .filter_map(|report| match report {
            Report::SkippedRecord { document, .. } => Some(document),
            _ => None,
        })
        .collect()
}

/// Contract: alice's device is generated on first use and stored, so every
/// later open is the same device with its unspent approval nonce; its
/// private keys and nonce are written only through the platform's
/// protection, and a copy of the store without that protection does not
/// open.
#[test]
fn the_device_is_generated_once_and_stored_only_protected() {
    let inner = MemoryStore::new();
    let store = |key| -> Rc<dyn PrivateStore> {
        Rc::new(ProtectedStore {
            inner: inner.clone(),
            key,
        })
    };
    let first = open_client(store(0x5a));
    let (id, sealing, signing, nonce) = {
        let device = first.device().unwrap();
        (
            device.id.clone(),
            device.sealing.secret.as_bytes().to_vec(),
            device.signing.secret.as_bytes().to_vec(),
            device.approval_nonce.as_ref().unwrap().0.to_vec(),
        )
    };
    drop(first);
    let again = open_client(store(0x5a));
    assert_eq!(again.device().unwrap().id, id);
    assert_eq!(again.device().unwrap().signing.secret.as_bytes(), signing);
    assert!(again.device().unwrap().approval_nonce.is_some());

    let files = documents(&inner);
    assert_eq!(files.len(), 1, "one device document");
    for secret in [&sealing, &signing, &nonce] {
        assert!(!contains(&files[0], secret), "a secret stored in the clear");
    }
    assert!(contains(&files[0], b"jazz-e2ee-local-devices-v3"));

    let copied = try_open(store(0x33), declarations());
    assert!(copied.is_err(), "a copy without the protection opens");
    assert_eq!(documents(&inner), files, "and it changes nothing");
}

/// The bytes of every document the store holds.
fn documents(store: &MemoryStore) -> Vec<Vec<u8>> {
    let scope = scope();
    block_on(store.list(&scope))
        .unwrap()
        .iter()
        .map(|name| block_on(store.load(&scope, name)).unwrap().unwrap().bytes)
        .collect()
}

/// Contract: a device document of a format this runtime does not read
/// (an older one without the approval nonce's state, say) fails the open
/// and is never overwritten.
#[test]
fn an_unknown_device_document_is_refused_and_left_alone() {
    let store = MemoryStore::new();
    let old = br#"{"devices":[],"format":"jazz-e2ee-local-devices-v2"}"#.to_vec();
    let at = block_on(store.open_scope(&scope())).unwrap();
    block_on(store.save(&scope(), at, "device", &old, None)).unwrap();
    let opened = try_open(Rc::new(store.clone()), declarations());
    assert!(matches!(opened, Err(ClientError::UnrecognisedDocument(_))));
    assert_eq!(
        block_on(store.load(&scope(), "device"))
            .unwrap()
            .unwrap()
            .bytes,
        old
    );
}

/// Contract: alice's held keys and her encrypted rows survive a restart:
/// reopened offline from her files, the runtime holds the same keys, knows
/// the catalogue identities from the database, reads her rows and writes
/// new ones.
///
/// ```text
/// alice ──write──► server     (restart, no server)
///   │                         alice ──open files──► reads, writes
///   └── keys in her private store, rows in her database
/// ```
#[test]
fn held_keys_and_rows_survive_a_restart_offline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let keys = MemoryStore::new();
    let store = || -> Rc<dyn PrivateStore> { Rc::new(keys.clone()) };
    let server = authority();
    let space = {
        let client = open_client(store());
        let (db, _link, _) = online(&client, &server, Some(&path));
        let space = project(&db, "plans");
        hold_new_space(&client, &db, space);
        block_on(db.insert("notes", note(space, SECRET), Default::default())).unwrap();
        pump(&server, &db);
        space
    };

    let client = open_client(store());
    let db = client_db(&client, Some(&path));
    client.set_online(false);
    assert!(client.prepare(&db).unwrap(), "identities are known offline");
    block_on(db.insert("notes", note(space, "offline"), Default::default())).unwrap();
    let mut bodies: Vec<Value> = all(
        &db,
        Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space)))),
    )
    .unwrap()
    .into_iter()
    .map(|row| row.cell("body").unwrap())
    .collect();
    bodies.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(bodies, [text("PLAINTEXT-SECRET"), text("offline")]);
    assert!(client.take_reports().is_empty());
}

/// Contract: a held-key record that fails its load checks is skipped and
/// reported, and the device keeps reading with its other keys; a held-key
/// document of a format this runtime does not read is reported and never
/// overwritten, even when a key of its root is kept again.
#[test]
fn a_damaged_held_key_record_is_skipped_and_reported() {
    let server = authority();
    let store = MemoryStore::new();
    let client = open_client(Rc::new(store.clone()));
    let (db, _link, _) = online(&client, &server, None);
    let (damaged, intact) = (project(&db, "damaged"), project(&db, "intact"));
    let damaged_records = new_root(&client.device().unwrap(), &address(&db, damaged));
    let damaged_root = damaged_records.root.id.clone();
    block_on(client.keep(common::held(&damaged_records))).unwrap();
    hold_new_space(&client, &db, intact);
    let name = format!("held-{damaged_root}");
    let stored = block_on(store.load(&scope(), &name)).unwrap().unwrap();
    let text_form = String::from_utf8(stored.bytes).unwrap();
    assert!(text_form.contains(r#""depth":"0""#));
    store.overwrite(
        &scope(),
        &name,
        text_form
            .replace(r#""depth":"0""#, r#""depth":"1""#)
            .into_bytes(),
    );
    drop((db, client));

    let client = open_client(Rc::new(store.clone()));
    let heard = Rc::new(std::cell::RefCell::new(Vec::new()));
    let sink = heard.clone();
    client.on_report(move |report| {
        if let Report::SkippedRecord { document, .. } = report {
            sink.borrow_mut().push(document.clone());
        }
    });
    client.flush_reports();
    assert_eq!(*heard.borrow(), std::slice::from_ref(&name));
    assert_eq!(skipped(&client), std::slice::from_ref(&name));
    let (db, _link, _) = online(&client, &server, None);
    client.record_fixture_rotation_check(&address(&db, intact));
    block_on(db.insert("notes", note(intact, "fine"), Default::default())).unwrap();
    let refused = block_on(db.insert("notes", note(damaged, "no key"), Default::default()));
    assert!(refused.is_err());

    let unknown = br#"{"format":"jazz-e2ee-held-space-v9"}"#.to_vec();
    store.overwrite(&scope(), &name, unknown.clone());
    drop((db, client));
    let client = open_client(Rc::new(store.clone()));
    assert_eq!(skipped(&client), std::slice::from_ref(&name));
    assert!(matches!(
        block_on(client.keep(common::held(&damaged_records))),
        Err(ClientError::UnrecognisedDocument(_))
    ));
    assert_eq!(
        block_on(store.load(&scope(), &name))
            .unwrap()
            .unwrap()
            .bytes,
        unknown
    );
}

fn floor(through: u64, generation: u64, keys: KeyColumns) -> AccountFloor {
    AccountFloor {
        through: Position(through),
        generation,
        root_id: "10000000-0000-4000-8000-000000000001".into(),
        root_keys: keys,
        epoch_id: "e0000000-0000-4000-8000-000000000000".into(),
        epoch_position: Position(2),
        active_device: None,
    }
}

/// Contract: a floor survives a restart and only moves forward: a floor of
/// an earlier cut, a lower generation or an earlier epoch position is
/// `stale-state` and changes nothing; a floor record that fails its signature check is skipped and
/// reported, never used.
#[test]
fn floors_only_move_forward_and_survive_a_restart() {
    const BOB: &str = "b0b00000-0000-5000-8000-00000000b0b0";
    let store = MemoryStore::new();
    let client = open_client(Rc::new(store.clone()));
    let keys = {
        let device = client.device().unwrap();
        KeyColumns::new(&device.sealing.public, &device.signing.public)
    };
    block_on(client.advance_floor(BOB, &floor(5, 0, keys.clone()))).unwrap();
    let older = block_on(client.advance_floor(BOB, &floor(4, 0, keys.clone())));
    assert!(matches!(
        older,
        Err(ClientError::Protocol(ProtocolError::StaleState))
    ));
    block_on(client.advance_floor(BOB, &floor(9, 1, keys.clone()))).unwrap();
    let lower = block_on(client.advance_floor(BOB, &floor(10, 0, keys.clone())));
    assert!(matches!(
        lower,
        Err(ClientError::Protocol(ProtocolError::StaleState))
    ));
    let earlier_epoch = AccountFloor {
        epoch_position: Position(1),
        ..floor(10, 1, keys.clone())
    };
    let earlier_epoch = block_on(client.advance_floor(BOB, &earlier_epoch));
    assert!(matches!(
        earlier_epoch,
        Err(ClientError::Protocol(ProtocolError::StaleState))
    ));
    drop(client);

    let client = open_client(Rc::new(store.clone()));
    assert_eq!(block_on(client.floor(BOB)), Some(floor(9, 1, keys.clone())));
    assert_eq!(block_on(client.floor(ALICE)), None);
    drop(client);

    let name = format!("floor-{BOB}");
    let stored = String::from_utf8(
        block_on(store.load(&scope(), &name))
            .unwrap()
            .unwrap()
            .bytes,
    )
    .unwrap();
    store.overwrite(
        &scope(),
        &name,
        stored
            .replace(r#""through":"9""#, r#""through":"1""#)
            .into_bytes(),
    );
    let client = open_client(Rc::new(store));
    assert_eq!(skipped(&client), [name]);
    assert_eq!(block_on(client.floor(BOB)), None);
}

/// Contract: a wipe removes alice's device, held keys and floors from the
/// store and closes the runtime: the device's secrets leave memory too
/// (`device()` fails), a later keep, floor or encrypted write fails and
/// writes nothing, so the store is never recreated; the next runtime
/// starts as a new device.
#[test]
fn a_wipe_removes_everything_and_nothing_recreates_it() {
    let server = authority();
    let store = MemoryStore::new();
    let client = open_client(Rc::new(store.clone()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let keys = {
        let device = client.device().unwrap();
        KeyColumns::new(&device.sealing.public, &device.signing.public)
    };
    block_on(client.advance_floor(ALICE, &floor(3, 0, keys.clone()))).unwrap();
    let old_device = client.device().unwrap().id.clone();
    let late = new_space_key(&client.device().unwrap(), &address(&db, space));

    block_on(client.wipe()).unwrap();
    assert!(store.scopes().is_empty());
    // The device's secrets and approval nonce are dropped with the store.
    assert!(matches!(client.device(), Err(ClientError::Closed)));
    assert!(matches!(
        block_on(client.keep(late)),
        Err(ClientError::Closed)
    ));
    assert!(matches!(
        block_on(client.advance_floor(ALICE, &floor(4, 0, keys))),
        Err(ClientError::Closed)
    ));
    // Refused as closed, not merely for want of the cleared key.
    let late = block_on(db.insert("notes", note(space, "late"), Default::default())).unwrap_err();
    assert_eq!(late.code, jazz::db::ErrorCode::Protocol, "{late:?}");
    assert!(
        store.scopes().is_empty(),
        "a late operation recreated the store"
    );

    let fresh = open_client(Rc::new(store));
    assert_ne!(fresh.device().unwrap().id, old_device);
}

/// Contract: two runtimes on one store (two tabs) that keep keys of the
/// same space root do not overwrite each other: the later save meets the
/// first one's revision, reads it, merges and saves both.
#[test]
fn two_writers_of_one_document_merge() {
    let server = authority();
    let store = MemoryStore::new();
    let first = open_client(Rc::new(store.clone()));
    let second = open_client(Rc::new(store.clone()));
    let (db, _link, _) = online(&first, &server, None);
    let space = project(&db, "plans");
    let address = address(&db, space);
    let root = new_root(&first.device().unwrap(), &address);
    let (held, twin) = (common::held(&root), common::held(&root));
    let root = root.root.id.clone();
    block_on(first.keep(held)).unwrap();
    block_on(second.keep(twin)).unwrap();
    let stored = block_on(store.load(&scope(), &format!("held-{root}")))
        .unwrap()
        .unwrap();
    assert_eq!(stored.revision, 2);
    let third = open_client(Rc::new(store));
    assert!(third.with_held(|held| held.space(&root).is_some()));
    assert!(skipped(&third).is_empty());
}

/// Contract: the runtime refuses at open what it cannot honour: mechanism
/// settings that name an unknown role, a mechanism this build lacks, or a
/// selection outside the allow-list, and declarations with an encrypted
/// schema conversion; the explicit built-in settings open.
#[test]
fn unusable_settings_and_declarations_are_refused_at_open() {
    let with = |mechanisms: serde_json::Value| {
        try_open(
            Rc::new(MemoryStore::new()),
            declarations_with(EncryptionSettings {
                allow_account_reset: false,
                mechanisms: Some(mechanisms),
            }),
        )
    };
    let builtin = serde_json::json!({ "id": "jazz.wrap.hpke-x25519", "version": 1 });
    assert!(with(serde_json::json!({ "allowed": { "keySeal": [builtin] }, "selected": { "keySeal": builtin } })).is_ok());
    for refused in [
        serde_json::json!({ "allowed": { "sealing": [builtin] } }),
        serde_json::json!({ "allowed": { "keySeal": [{ "id": "jazz.seal.future", "version": 1 }] } }),
        serde_json::json!({ "selected": { "keySeal": { "id": "jazz.wrap.hpke-x25519", "version": 2 } } }),
        serde_json::json!({ "allowed": { "keySeal": [] } }),
        serde_json::json!({ "preferred": {} }),
    ] {
        assert!(
            matches!(with(refused.clone()), Err(ClientError::Settings(_))),
            "{refused}"
        );
    }
}

/// Contract: a key whose document the store fails to save is not held: it
/// seals no write, so the device never writes rows under a key it would
/// lose on a restart. The save's error is returned.
#[test]
fn a_key_whose_save_fails_is_not_usable() {
    let server = authority();
    let store = FaultyStore::default();
    let client = open_client(Rc::new(store.clone()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    let held = new_space_key(&client.device().unwrap(), &address(&db, space));
    store.failing.set(true);
    assert!(matches!(
        block_on(client.keep(held)),
        Err(ClientError::Store(jazz_e2ee_client::StoreError::Failed(_)))
    ));
    let refused = block_on(db.insert("notes", note(space, SECRET), Default::default()));
    assert!(matches!(
        refused.map_err(|error| jazz::e2ee::E2eeRefusal::of(&error)),
        Err(Some(jazz::e2ee::E2eeRefusal::NeedsKey { .. }))
    ));
}

/// Contract: a wipe that starts while a keep waits on its save wins: the
/// keep fails with the runtime closed instead of panicking or holding the
/// key, and the store ends wiped.
///
/// ```text
/// keep ──save (yields)──┐            ┌── conflict ──✗ closed
/// wipe ──close, clear ──┴── waits ───┴── delete scope
/// ```
#[test]
fn a_wipe_during_a_keep_closes_it() {
    let server = authority();
    let store = FaultyStore::default();
    let client = open_client(Rc::new(store.clone()));
    let (db, _link, _) = online(&client, &server, None);
    let held = new_space_key(
        &client.device().unwrap(),
        &address(&db, project(&db, "plans")),
    );
    store.contended.set(true);
    let (kept, wiped) = block_on(async { futures::join!(client.keep(held), client.wipe()) });
    assert!(matches!(kept, Err(ClientError::Closed)), "{kept:?}");
    wiped.unwrap();
    assert!(store.inner.scopes().is_empty());
}

/// Contract: a stored device whose private key is not the pair of its
/// public key (a damaged or tampered document) fails its local check at
/// open, rather than seal to keys it cannot open with.
#[test]
fn a_device_whose_keys_do_not_match_fails_to_open() {
    let store = MemoryStore::new();
    drop(open_client(Rc::new(store.clone())));
    let other = MemoryStore::new();
    drop(open_client(Rc::new(other.clone())));
    let field = |bytes: &str| {
        let start = bytes.find(r#""privateKey":["#).unwrap();
        let end = start + bytes[start..].find(']').unwrap() + 1;
        bytes[start..end].to_owned()
    };
    let document = |store: &MemoryStore| {
        String::from_utf8(
            block_on(store.load(&scope(), "device"))
                .unwrap()
                .unwrap()
                .bytes,
        )
        .unwrap()
    };
    let mine = document(&store);
    let tampered = mine.replace(&field(&mine), &field(&document(&other)));
    assert_ne!(tampered, mine);
    store.overwrite(&scope(), "device", tampered.into_bytes());
    assert!(try_open(Rc::new(store), declarations()).is_err());
}

/// Contract: when one tab wipes alice's private state, another tab's
/// runtime that opened it earlier never recreates it: its next keep or
/// floor fails with the runtime closed, its device's secrets are dropped,
/// and the store stays wiped.
///
/// ```text
/// tab 1 ──open──┐            tab 2 ──open──┐
///               └── wipe                    └── keep ──✗ closed (store stays empty)
/// ```
#[test]
fn another_runtime_never_recreates_a_wiped_store() {
    let server = authority();
    let store = MemoryStore::new();
    let first = open_client(Rc::new(store.clone()));
    let second = open_client(Rc::new(store.clone()));
    let (db, _link, _) = online(&first, &server, None);
    let held = new_space_key(
        &second.device().unwrap(),
        &address(&db, project(&db, "plans")),
    );
    let keys = {
        let device = second.device().unwrap();
        KeyColumns::new(&device.sealing.public, &device.signing.public)
    };
    block_on(first.wipe()).unwrap();
    assert!(matches!(
        block_on(second.keep(held)),
        Err(ClientError::Closed)
    ));
    assert!(matches!(
        block_on(second.advance_floor(ALICE, &floor(1, 0, keys))),
        Err(ClientError::Closed)
    ));
    assert!(store.scopes().is_empty(), "the wiped store was recreated");
    // Finding the store wiped dropped its device's secrets as well.
    assert!(matches!(second.device(), Err(ClientError::Closed)));
}

/// Contract: a floor record of a format this runtime does not read is
/// reported, and never overwritten: advancing that account's floor fails
/// and leaves the record as it was.
#[test]
fn an_unknown_floor_format_is_never_overwritten() {
    const BOB: &str = "b0b00000-0000-5000-8000-00000000b0b0";
    let store = MemoryStore::new();
    let client = open_client(Rc::new(store.clone()));
    let keys = {
        let device = client.device().unwrap();
        KeyColumns::new(&device.sealing.public, &device.signing.public)
    };
    drop(client);
    let name = format!("floor-{BOB}");
    let newer = br#"{"format":"jazz-e2ee-floor-v9"}"#.to_vec();
    store.overwrite(&scope(), &name, newer.clone());
    let client = open_client(Rc::new(store.clone()));
    assert_eq!(skipped(&client), std::slice::from_ref(&name));
    assert!(matches!(
        block_on(client.advance_floor(BOB, &floor(5, 0, keys))),
        Err(ClientError::UnrecognisedDocument(_))
    ));
    assert_eq!(
        block_on(store.load(&scope(), &name))
            .unwrap()
            .unwrap()
            .bytes,
        newer
    );
}

/// Contract: a runtime that opens while another tab wipes the store does
/// not fail as closed: it opens the store again, as a new device of a new
/// scope, and the store holds that device.
#[test]
fn opening_during_a_wipe_starts_afresh() {
    let store = FaultyStore::default();
    let before = open_client(Rc::new(store.clone()));
    let old_device = before.device().unwrap().id.clone();
    drop(before);
    block_on(store.inner.delete(&scope())).unwrap();
    store.wiped_first.set(true);
    let fresh = open_client(Rc::new(store.clone()));
    assert_ne!(fresh.device().unwrap().id, old_device);
    assert_eq!(store.inner.scopes(), [scope()]);
    let reopened = open_client(Rc::new(store));
    assert_eq!(reopened.device().unwrap().id, fresh.device().unwrap().id);
}
