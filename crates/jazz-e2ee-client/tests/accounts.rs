//! A device in its account against a real authority (`E2EE_PROTOCOL.md`
//! §2.1–§2.7, §2.12, §6.3a): the request, the bootstrap of the first
//! device, the verification of the account at each read with the floor
//! that refuses older evidence, and what a device the account no longer
//! has does.

mod common;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use common::*;
use jazz::db::block_on;
use jazz::e2ee::EncryptionSettings;
use jazz::query::Query;
use jazz::records::Value;
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::key_seal;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::derive::account_sealing_key_pair;
use jazz_e2ee::protocol::domain::AppDomain;
use jazz_e2ee::protocol::error::{RefusalCode, UnsupportedCapability};
use jazz_e2ee::registry::Registry;
use jazz_e2ee_client::{AccountStatus, ClientError, MemoryStore, PrivateStore, Report};

/// Contract: alice's first device publishes its request, bootstraps her
/// account (identity, public root and the first epoch's sealing key, as her
/// session) and holds the account key it then verifies; asking again
/// publishes nothing more. A key it derives from that account seals her
/// notes, which she reads back after a restart without the server.
///
/// ```text
/// alice's device ──request──► server
///                ──identity + root + sealing key──► server
///                ◄──ledger read── verify: active, account key
/// (restart, offline) ──read notes──► plaintext
/// ```
#[test]
fn an_app_account_bootstraps_on_its_first_device() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let settings = EncryptionSettings::default();
    let alice = account(0xa1);
    let server = secure_authority_with(&settings, None);
    let store = MemoryStore::new();
    let space = {
        let client = open_account(Rc::new(store.clone()), alice, settings.clone(), None);
        let (db, _link) = session(&client, &server, alice, &settings, 1, Some(&path));
        let status = drive(&server, &[&db], client.ensure_account(&db)).unwrap();
        assert_eq!(status, AccountStatus::Active);
        for table in [
            "__e2ee_device_requests",
            "__e2ee_device_keys",
            "__e2ee_account_identities",
            "__e2ee_account_roots",
            "__e2ee_account_keys",
        ] {
            assert_eq!(count(&server, table), 1, "{table}");
        }
        let (generation, epochs) = client
            .with_account(|account| {
                (
                    account.state.generation,
                    account.epoch_keys.epoch_ids().count(),
                )
            })
            .unwrap();
        assert_eq!((generation, epochs), (0, 1));
        assert_eq!(
            drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
            AccountStatus::Active
        );
        assert_eq!(count(&server, "__e2ee_account_roots"), 1);

        let space = project(&db, "plans");
        client.record_fixture_rotation_check(&address(&db, space));
        block_on(client.keep(space_key_of_account(&client, &db, space))).unwrap();
        block_on(db.insert("notes", note(space, SECRET), Default::default())).unwrap();
        pump(&server, &db);
        space
    };

    let client = open_account(Rc::new(store), alice, settings.clone(), None);
    let config = jazz::db::DbConfig::new(
        secure_schema_with(&settings),
        jazz::db::DbIdentity {
            node: jazz::ids::NodeUuid::from_bytes([1; 16]),
            author: alice,
        },
    )
    .with_e2ee(client.hooks());
    let offline = block_on(jazz::db::Db::open_at(
        config,
        &path,
        jazz::db::FileOptions::default(),
    ))
    .unwrap();
    assert!(jazz::db::block_on(client.prepare(&offline)).unwrap());
    let rows = all(&offline, Query::from("notes")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell("projectId"), Some(Value::Uuid(space)));
    assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
}

/// Alice's device reports a signed sealing key that does not derive from her
/// account key once, and can still seal and read notes.
#[test]
fn a_conflicting_account_key_is_reported_and_ignored_for_sealing() {
    let settings = EncryptionSettings::default();
    let alice = account(0xa1);
    let server = secure_authority_with(&settings, None);
    let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (db, _link) = session(&client, &server, alice, &settings, 1, None);
    assert_eq!(
        drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
        AccountStatus::Active
    );

    let mut conflict = client
        .with_account(|account| account.public.account_keys[0].row.clone())
        .unwrap();
    let id = uuid::Uuid::new_v4();
    conflict.id = id.to_string();
    let wrong_pair = account_sealing_key_pair(&key_seal::Builtin, &[9; 32]).unwrap();
    assert_ne!(wrong_pair.public.bytes, conflict.public_key);
    conflict.public_key = wrong_pair.public.bytes.clone();
    let domain = AppDomain::new(
        &app_id().as_object_id().uuid().to_string(),
        Some(ENVIRONMENT),
    )
    .unwrap()
    .account(client.account_id())
    .unwrap();
    let registry = Registry::builtin();
    let device = client.device().unwrap().clone();
    let signer = registry
        .signer(device.signing.public.mechanism.as_mechanism())
        .unwrap();
    conflict.signature = signer
        .sign(&device.signing, &conflict.signed_bytes(&domain).unwrap())
        .unwrap();
    let table = "__e2ee_account_keys";
    let original = all(&server, Query::from(table)).unwrap().pop().unwrap();
    let mut cells: BTreeMap<_, _> = original
        .cells()
        .filter_map(|(column, value)| Some((column.to_owned(), value?.clone())))
        .collect();
    cells.insert("publicKey".to_owned(), Value::Bytes(conflict.public_key));
    cells.insert("signature".to_owned(), Value::Bytes(conflict.signature));
    block_on(server.insert(
        table,
        cells,
        jazz::db::InsertOptions {
            row_id: Some(jazz::ids::RowUuid(id)),
            ..Default::default()
        },
    ))
    .unwrap();

    let reports = Rc::new(RefCell::new(Vec::new()));
    let events = reports.clone();
    client.on_report(move |report| {
        if matches!(report, Report::ConflictingAccountKey { .. }) {
            events.borrow_mut().push(report.clone());
        }
    });
    for _ in 0..2 {
        assert_eq!(
            drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
            AccountStatus::Active
        );
        client.flush_reports();
    }
    assert_eq!(
        &*reports.borrow(),
        &[Report::ConflictingAccountKey {
            record_id: conflict.id,
            signer_id: conflict.signer_id,
            epoch_id: conflict.epoch_id,
        }]
    );

    let space = project(&db, "plans");
    pump(&server, &db);
    let scope = block_on(db.table_identity("projects")).unwrap().unwrap().0;
    let member = BTreeMap::from([
        ("scopeId".into(), Value::Uuid(scope)),
        ("identifier".into(), Value::Uuid(space)),
        (
            "accountId".into(),
            Value::Uuid(uuid::Uuid::parse_str(client.account_id()).unwrap()),
        ),
    ]);
    block_on(db.insert("__e2ee_members", member, Default::default())).unwrap();
    pump(&server, &db);
    drive(
        &server,
        &[&db],
        client.write(&db, async || {
            db.insert("notes", note(space, SECRET), Default::default())
                .await
        }),
    )
    .unwrap();
    let rows = all(&db, Query::from("notes")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    pump(&server, &db);
    let root = client.with_held(|keys| keys.roots_of(&address(&db, space))[0].root().clone());
    let context = root.account_envelope_context(&domain).unwrap();
    let row = all(&server, Query::from("__e2ee_spaces"))
        .unwrap()
        .pop()
        .unwrap();
    let Some(Value::Bytes(envelope)) = row.cell("accountEnvelope") else {
        panic!("missing account envelope")
    };
    let derived = client
        .with_account(|a| account_sealing_key_pair(&key_seal::Builtin, a.key.key()).unwrap())
        .unwrap();
    assert!(
        registry
            .open_sealed(&[derived], &context, &envelope)
            .is_ok()
    );
    assert_eq!(
        registry
            .open_sealed(&[wrong_pair], &context, &envelope)
            .unwrap_err(),
        jazz_e2ee::Error::InvalidCiphertext
    );
}

/// Alice reopens an app account with a sealing record signed under a
/// version her client cannot verify. She stays active and hears about
/// the skipped record once across repeated account checks.
#[test]
fn an_unsupported_sealing_key_is_reported_without_failing_account_replay() {
    let settings = EncryptionSettings::default();
    let alice = account(0xa1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server.sqlite");
    let store = Rc::new(MemoryStore::new());
    let client = open_account(store.clone(), alice, settings.clone(), None);
    // This reader starts with the same device, before its first verified floor.
    let first_read = MemoryStore::new();
    let scope = store.scopes().pop().unwrap();
    first_read.overwrite(
        &scope,
        "device",
        block_on(store.load(&scope, "device"))
            .unwrap()
            .unwrap()
            .bytes,
    );
    let record = {
        let server = secure_authority_with(&settings, Some(&path));
        let (db, _link) = session(&client, &server, alice, &settings, 1, None);
        drive(&server, &[&db], client.ensure_account(&db)).unwrap();
        client
            .with_account(|a| a.public.account_keys[0].row.clone())
            .unwrap()
    };
    for table in ["__e2ee_device_keys", "__e2ee_device_requests"] {
        tamper(&path, table, "signingVersion", 2i64);
    }
    let client = open_account(Rc::new(first_read), alice, settings.clone(), None);
    let reports = Rc::new(RefCell::new(Vec::new()));
    let events = reports.clone();
    client.on_report(move |report| events.borrow_mut().push(report.clone()));
    let server = secure_authority_with(&settings, Some(&path));
    let (db, _link) = session(&client, &server, alice, &settings, 2, None);
    for _ in 0..2 {
        assert_eq!(
            drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
            AccountStatus::Active
        );
    }
    assert_eq!(
        &*reports.borrow(),
        &[Report::UnsupportedAccountKey {
            record_id: record.id,
            signer_id: record.signer_id,
            capability: UnsupportedCapability::SigningVersion,
        }]
    );
}

/// Wiping Alice's verified device drops its account and historical epoch
/// keys even while the application retains the client.
#[test]
fn a_wiped_client_drops_its_verified_account_keys() {
    let settings = EncryptionSettings::default();
    let (alice, identity) = device_account(0xa1);
    let server = secure_authority_with(&settings, None);
    let client = open_account(
        Rc::new(MemoryStore::new()),
        alice,
        settings.clone(),
        Some(identity),
    );
    let (db, _link) = session(&client, &server, alice, &settings, 1, None);
    drive(&server, &[&db], client.ensure_account(&db)).unwrap();
    assert_eq!(
        client.with_account(|a| a.epoch_keys.epoch_ids().count()),
        Some(1)
    );
    assert!(client.has_device_account_identity());
    block_on(client.wipe()).unwrap();
    assert!(!client.has_device_account_identity());
    assert!(
        client
            .with_account(|a| a.epoch_keys.epoch_ids().count())
            .is_none()
    );
}

/// Contract: lena's device-account account is bootstrapped with a root her
/// identity signs, which every reader verifies against the key that
/// derives her account ID; her device then holds the account key.
#[test]
fn a_device_account_bootstraps_with_a_signed_root() {
    let settings = EncryptionSettings::default();
    let (lena, identity) = device_account(0x1f);
    let server = secure_authority_with(&settings, None);
    let client = open_account(
        Rc::new(MemoryStore::new()),
        lena,
        settings.clone(),
        Some(identity),
    );
    let (db, _link) = session(&client, &server, lena, &settings, 1, None);
    assert_eq!(
        drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
        AccountStatus::Active
    );
    let roots = all(&server, Query::from("__e2ee_account_roots")).unwrap();
    assert_eq!(
        roots[0].cell("kind"),
        Some(Value::String("device-account".to_owned()))
    );
    assert!(matches!(
        roots[0].cell("deviceAccountSignature"),
        Some(Value::Nullable(Some(_))) | Some(Value::Bytes(_))
    ));
}

/// Contract: a device that holds lena's device-account identity refuses an
/// account whose root that identity did not sign, here one the server
/// wrote itself, with `root-not-mine`; it bootstraps nothing over it.
///
/// ```text
/// server ──root "app" for lena's account──► (stored)
/// lena's device ──verify──✗ root-not-mine
/// ```
#[test]
fn a_root_lena_s_identity_did_not_sign_is_root_not_mine() {
    let settings = EncryptionSettings::default();
    let (lena, identity) = device_account(0x2f);
    let server = secure_authority_with(&settings, None);
    let device = uuid::Uuid::new_v4();
    let epoch = uuid::Uuid::new_v4();
    let keys = |cells: &mut std::collections::BTreeMap<String, Value>| {
        cells.insert("publicKey".to_owned(), Value::Bytes(vec![7; 32]));
        cells.insert("mechanism".to_owned(), text("jazz.wrap.hpke-x25519"));
        cells.insert("version".to_owned(), Value::I64(1));
        cells.insert("signingPublicKey".to_owned(), Value::Bytes(vec![8; 32]));
        cells.insert("signingMechanism".to_owned(), text("jazz.sodium.sign"));
        cells.insert("signingVersion".to_owned(), Value::I64(1));
    };
    let mut key_row =
        std::collections::BTreeMap::from([("deviceId".to_owned(), Value::Uuid(device))]);
    keys(&mut key_row);
    block_on(server.insert("__e2ee_device_keys", key_row, Default::default())).unwrap();
    block_on(server.insert(
        "__e2ee_account_roots",
        std::collections::BTreeMap::from([
            ("ledgerVersion".to_owned(), Value::I64(1)),
            ("generation".to_owned(), Value::I64(0)),
            (
                "accountId".to_owned(),
                Value::Uuid(lena.account_id().unwrap().0),
            ),
            ("deviceId".to_owned(), Value::Uuid(device)),
            ("epochId".to_owned(), Value::Uuid(epoch)),
            ("kind".to_owned(), text("app")),
        ]),
        Default::default(),
    ))
    .unwrap();

    let client = open_account(
        Rc::new(MemoryStore::new()),
        lena,
        settings.clone(),
        Some(identity),
    );
    let (db, _link) = session(&client, &server, lena, &settings, 1, None);
    let refused = drive(&server, &[&db], client.ensure_account(&db));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::RootNotMine))
        ),
        "{refused:?}"
    );
    assert_eq!(count(&server, "__e2ee_account_identities"), 0);
    assert!(client.with_account(|_| ()).is_none());
}

/// Contract: once alice's device has verified her account at a later cut,
/// a server restored from an older copy cannot make it accept the older
/// state: verifying against it is `stale-state`, the floor stays, and the
/// account it verified before is forgotten, so nothing goes on using it.
///
/// ```text
/// alice ──verify (cut c1)──► server      (copy taken)
/// alice ──write, verify (cut c2 > c1)──► server
/// alice ──verify──► restored copy (cut c1) ──✗ stale-state
/// ```
#[test]
fn a_rolled_back_server_is_stale_state() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("server.sqlite");
    let old = dir.path().join("old.sqlite");
    let settings = EncryptionSettings::default();
    let alice = account(0xa2);
    let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    {
        let server = secure_authority_with(&settings, Some(&live));
        let (db, _link) = session(&client, &server, alice, &settings, 1, None);
        drive(&server, &[&db], client.ensure_account(&db)).unwrap();
    }
    copy_database(&live, &old);
    {
        let server = secure_authority_with(&settings, Some(&live));
        let (db, _link) = session(&client, &server, alice, &settings, 2, None);
        project(&db, "a later write");
        pump(&server, &db);
        assert_eq!(
            drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
            AccountStatus::Active
        );
    }
    let floor = block_on(client.floor(&account_id(alice))).unwrap();
    assert!(client.with_account(|_| ()).is_some());
    let restored = secure_authority_with(&settings, Some(&old));
    let (db, _link) = session(&client, &restored, alice, &settings, 3, None);
    let refused = drive(&restored, &[&db], client.ensure_account(&db));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::StaleState))
        ),
        "{refused:?}"
    );
    assert_eq!(block_on(client.floor(&account_id(alice))), Some(floor));
    assert!(
        client.with_account(|_| ()).is_none(),
        "the account verified before is forgotten"
    );
}

/// Contract: alice's active device keeps its verified account when its
/// request to reset the account is refused.
#[test]
fn a_refused_reset_keeps_the_verified_account() {
    let settings = EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let alice = account(0xab);
    let server = secure_authority_with(&settings, None);
    let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (db, _link) = session(&client, &server, alice, &settings, 1, None);
    assert_eq!(
        drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
        AccountStatus::Active
    );

    let refused = drive(&server, &[&db], client.reset_account(&db));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::Refused(
                RefusalCode::ResetDeviceActive
            )))
        ),
        "{refused:?}"
    );
    assert_eq!(
        client.with_account(|account| account.state.generation),
        Some(0),
        "the verified account is still usable after the refusal"
    );
}

/// Alice's waiting device reports which reset records the authority refused.
/// The app can reconcile the original identity, root and sealing key IDs.
#[test]
fn a_failed_reset_reports_the_publication_and_original_record_ids() {
    use jazz_e2ee_client::host::{
        bridge::JsStore,
        runtime::{Built, Encryption},
    };
    use serde_json::{Value as Json, json};

    let server = secure_authority_with(&EncryptionSettings::default(), None);
    let settings = EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let alice = account(0xad);
    let first = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (first_db, _first_link) = session(&first, &server, alice, &settings, 1, None);
    assert_eq!(
        drive(&server, &[&first_db], first.ensure_account(&first_db)).unwrap(),
        AccountStatus::Active
    );

    let second = Rc::new(open_account(
        Rc::new(MemoryStore::new()),
        alice,
        settings.clone(),
        None,
    ));
    let (second_db, _second_link, wire) =
        recorded_session(&second, &server, alice, &settings, 2, None);
    let second_db = Rc::new(second_db);
    assert_eq!(
        drive(&server, &[&second_db], second.ensure_account(&second_db)).unwrap(),
        AccountStatus::AwaitingApproval
    );
    let runtime = Encryption::new(
        Built {
            spaces: json!({}),
            client: second,
            reports: Rc::default(),
        },
        Rc::new(JsStore::default()),
        Rc::clone(&second_db),
    );
    wire.borrow_mut().clear();
    let error = drive(
        &server,
        &[&second_db],
        runtime.call("resetAccount".into(), json!({})),
    )
    .unwrap_err();
    let details: Json =
        serde_json::from_str(error.strip_prefix("publication: ").expect(&error)).unwrap();
    assert_eq!(details["stage"], "account-reset");
    assert_eq!(details["outcome"], "refused");
    assert_eq!(details["code"], "authorization_denied");

    let ids: std::collections::BTreeSet<_> = details["recordIds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| uuid::Uuid::parse_str(id.as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(ids.len(), 3);
    for id in ids {
        assert!(
            contains(&wire.borrow(), id.as_bytes()),
            "record {id} was sent"
        );
    }
    for table in [
        "__e2ee_account_identities",
        "__e2ee_account_roots",
        "__e2ee_account_keys",
    ] {
        assert_eq!(count(&server, table), 1, "nothing published to {table}");
    }
}

/// Contract: after alice's second device resets her account, a reset
/// attempt on the first device discovers that it is no longer active and
/// immediately clears its last verified account.
///
/// ```text
/// first device  ──verify generation 0──► server
/// second device ──reset to generation 1──► server
/// first device  ──reset──✗ device-no-longer-active
/// ```
#[test]
fn a_reset_by_a_device_no_longer_active_clears_the_verified_account() {
    let settings = EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let alice = account(0xac);
    let server = secure_authority_with(&settings, None);
    let first = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (first_db, _first_link) = session(&first, &server, alice, &settings, 1, None);
    assert_eq!(
        drive(&server, &[&first_db], first.ensure_account(&first_db)).unwrap(),
        AccountStatus::Active
    );
    let second = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (second_db, _second_link) = session(&second, &server, alice, &settings, 2, None);
    assert_eq!(
        drive(&server, &[&second_db], second.ensure_account(&second_db)).unwrap(),
        AccountStatus::AwaitingApproval
    );
    assert_eq!(
        drive(&server, &[&second_db], second.reset_account(&second_db)).unwrap(),
        AccountStatus::Active
    );

    assert!(first.with_account(|_| ()).is_some());
    let refused = drive(&server, &[&first_db], first.reset_account(&first_db));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::DeviceNoLongerActive))
        ),
        "{refused:?}"
    );
    assert!(first.with_account(|_| ()).is_none());
}

/// Contract: when alice's account is reset on her second device, her first
/// device, which no longer belongs to it, sees its account rows change,
/// wipes its private state (held keys and floors), and enrolls again as a
/// new device that awaits approval; it never reads with the old state.
///
/// ```text
/// device 1 ──bootstrap, hold a space key──► server
/// device 2 ──request, reset (generation 1)──► server
/// device 1 ──watch: changed ──verify──► device-no-longer-active
///          ──wipe, new device, request──► awaiting approval
/// ```
#[test]
fn a_device_left_out_of_a_reset_enrols_again() {
    let settings = EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let alice = account(0xa3);
    let server = secure_authority_with(&settings, None);
    let first_store = MemoryStore::new();
    let first = open_account(Rc::new(first_store.clone()), alice, settings.clone(), None);
    let (first_db, _first_link) = session(&first, &server, alice, &settings, 1, None);
    assert_eq!(
        drive(&server, &[&first_db], first.ensure_account(&first_db)).unwrap(),
        AccountStatus::Active
    );
    let space = project(&first_db, "plans");
    let root = space_key_of_account(&first, &first_db, space);
    let root_id = root.root_id().to_owned();
    block_on(first.keep(root)).unwrap();
    let mut watch = drive(&server, &[&first_db], first.watch_account(&first_db)).unwrap();
    pump(&server, &first_db);
    assert!(watch.changed(), "the rows already there");
    assert!(!watch.changed());
    let old_device = first.device().unwrap().id.clone();

    let second = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (second_db, _second_link) = session(&second, &server, alice, &settings, 2, None);
    assert_eq!(
        drive(&server, &[&second_db], second.ensure_account(&second_db)).unwrap(),
        AccountStatus::AwaitingApproval
    );
    assert_eq!(
        drive(&server, &[&second_db], second.reset_account(&second_db)).unwrap(),
        AccountStatus::Active
    );
    assert_eq!(
        second.with_account(|account| account.state.generation),
        Some(1)
    );

    pump(&server, &first_db);
    assert!(watch.changed(), "the reset reached the first device");
    assert_eq!(
        drive(&server, &[&first_db], first.ensure_account(&first_db)).unwrap(),
        AccountStatus::Reenrolled
    );
    assert_ne!(first.device().unwrap().id, old_device);
    assert!(first.with_held(|held| held.space(&root_id).is_none()));
    assert!(first.with_account(|_| ()).is_none());
    assert_eq!(
        drive(&server, &[&first_db], first.ensure_account(&first_db)).unwrap(),
        AccountStatus::AwaitingApproval
    );
}

/// Contract: a device left out of a reset whose new device cannot be
/// stored (the store fails after the old state is wiped) closes its
/// runtime: it does not run on as the device the store no longer has.
///
/// ```text
/// device 2 ──request, reset──► server
/// device 1 ──verify──► device-no-longer-active ──wipe──► new device: save ✗
///          ──► the store's error; every later call: closed
/// ```
#[test]
fn a_failed_reenrolment_closes_the_runtime() {
    let settings = EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let alice = account(0xa5);
    let server = secure_authority_with(&settings, None);
    let first_store = FaultyStore::default();
    let first = open_account(first_store.store(), alice, settings.clone(), None);
    let (first_db, _first_link) = session(&first, &server, alice, &settings, 1, None);
    assert_eq!(
        drive(&server, &[&first_db], first.ensure_account(&first_db)).unwrap(),
        AccountStatus::Active
    );
    let second = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (second_db, _second_link) = session(&second, &server, alice, &settings, 2, None);
    assert_eq!(
        drive(&server, &[&second_db], second.ensure_account(&second_db)).unwrap(),
        AccountStatus::AwaitingApproval
    );
    assert_eq!(
        drive(&server, &[&second_db], second.reset_account(&second_db)).unwrap(),
        AccountStatus::Active
    );
    pump(&server, &first_db);

    first_store.failing.set(true);
    let failed = drive(&server, &[&first_db], first.ensure_account(&first_db));
    assert!(
        matches!(
            failed,
            Err(ClientError::Store(jazz_e2ee_client::StoreError::Failed(_)))
        ),
        "{failed:?}"
    );
    first_store.failing.set(false);
    let closed = drive(&server, &[&first_db], first.ensure_account(&first_db));
    assert!(matches!(closed, Err(ClientError::Closed)), "{closed:?}");
    // Its device's secrets are dropped with the store (§6.4).
    assert!(matches!(first.device(), Err(ClientError::Closed)));
    let space = project(&first_db, "plans");
    let held = new_space_key(&second.device().unwrap(), &address(&first_db, space));
    assert!(matches!(
        block_on(first.keep(held)),
        Err(ClientError::Closed)
    ));
}

/// Contract: two first devices of one account that bootstrap at once do
/// not both found it: the authority accepts one bootstrap, the other's
/// read is out of date and conflicts, and that device reads again and
/// awaits approval.
#[test]
fn two_first_devices_found_the_account_once() {
    let settings = EncryptionSettings::default();
    let alice = account(0xa4);
    let server = secure_authority_with(&settings, None);
    let one = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let two = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (one_db, _one_link) = session(&one, &server, alice, &settings, 1, None);
    let (two_db, _two_link) = session(&two, &server, alice, &settings, 2, None);
    let (first, second) = drive(
        &server,
        &[&one_db, &two_db],
        futures::future::join(one.ensure_account(&one_db), two.ensure_account(&two_db)),
    );
    let mut statuses = [first.unwrap(), second.unwrap()];
    statuses.sort_by_key(|status| format!("{status:?}"));
    assert_eq!(
        statuses,
        [AccountStatus::Active, AccountStatus::AwaitingApproval]
    );
    assert_eq!(count(&server, "__e2ee_account_roots"), 1);
    assert_eq!(count(&server, "__e2ee_device_requests"), 2);
}

/// Two of Alice's waiting devices reset concurrently. Each accepted reset
/// advances a distinct generation even when both started at generation zero.
#[test]
fn racing_resets_publish_at_most_one_root_per_generation() {
    let settings = EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let alice = account(0xa1);
    let server = secure_authority_with(&settings, None);
    let devices = [1, 2, 3].map(|node| {
        let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
        let (db, link) = session(&client, &server, alice, &settings, node, None);
        let status = drive(&server, &[&db], client.ensure_account(&db)).unwrap();
        assert_eq!(
            status,
            if node == 1 {
                AccountStatus::Active
            } else {
                AccountStatus::AwaitingApproval
            }
        );
        (client, db, link)
    });
    let [_, (one, db1, _), (two, db2, _)] = &devices;
    let (a, b) = drive(
        &server,
        &[db1, db2],
        futures::future::join(one.reset_account(db1), two.reset_account(db2)),
    );
    assert!(
        matches!(a, Ok(AccountStatus::Active)) || matches!(b, Ok(AccountStatus::Active)),
        "{a:?}, {b:?}"
    );
    let mut generations: Vec<_> = all(&server, Query::from("__e2ee_account_roots"))
        .unwrap()
        .into_iter()
        .map(|row| match row.cell("generation") {
            Some(Value::I64(g)) => g,
            other => panic!("unexpected generation {other:?}"),
        })
        .collect();
    generations.sort();
    assert!(
        generations == [0, 1] || generations == [0, 1, 2],
        "{generations:?}"
    );
}

/// Alice loses the reset acknowledgement after the authority commits.
/// Her live call settles when the device reconnects.
///
/// waiting device → submit reset → authority commits → link lost before receipt
#[test]
fn a_committed_reset_survives_a_lost_acknowledgement() {
    lose_reset_acknowledgement(false);
}

/// After process loss, Alice's retained device replays the committed
/// reset at the authority and obtains its account key for generation one.
#[test]
fn a_restarted_device_recovers_a_reset_whose_acknowledgement_was_lost() {
    lose_reset_acknowledgement(true);
}

fn lose_reset_acknowledgement(restart: bool) {
    use std::future::Future;
    use std::task::{Context, Waker};

    let settings = EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let alice = account(0xa1);
    let server = secure_authority_with(&settings, None);
    let founder = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (first, _link) = session(&founder, &server, alice, &settings, 1, None);
    drive(&server, &[&first], founder.ensure_account(&first)).unwrap();

    let store = Rc::new(MemoryStore::new());
    let client = open_account(store.clone(), alice, settings.clone(), None);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("device.sqlite");
    let (db, link) = session(&client, &server, alice, &settings, 2, Some(&path));
    assert_eq!(
        drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
        AccountStatus::AwaitingApproval
    );

    let mut reset = Box::pin(client.reset_account(&db));
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..2000 {
        assert!(reset.as_mut().poll(&mut cx).is_pending());
        block_on(db.tick()).unwrap();
        block_on(server.tick()).unwrap();
        if count(&server, "__e2ee_account_roots") == 2 {
            break;
        }
    }
    assert_eq!(count(&server, "__e2ee_account_roots"), 2);
    assert!(
        db.pending_upload_count() > 0,
        "the device has not received the reset fate"
    );
    assert!(db.detach_connection(&link));
    if restart {
        drop(reset);
        drop(db);
        drop(client);
        let client = open_account(store, alice, settings.clone(), None);
        let (db, _link) = session(&client, &server, alice, &settings, 2, Some(&path));
        assert_eq!(
            drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
            AccountStatus::Active
        );
        assert_eq!(client.with_account(|a| a.state.generation), Some(1));
    } else {
        let (_link, _) = link_as(&db, &server, alice);
        assert_eq!(
            drive(&server, &[&db], reset).unwrap(),
            AccountStatus::Active
        );
        assert_eq!(client.with_account(|a| a.state.generation), Some(1));
    }
    assert_eq!(count(&server, "__e2ee_account_roots"), 2);
}

/// Inserts, as the authority itself, a root for `who`'s account of `kind`
/// and ledger version `version`, with a key projection for its device.
fn server_root(server: &jazz::db::Db, who: jazz::ids::AuthorSubject, kind: &str, version: i64) {
    let device = uuid::Uuid::new_v4();
    block_on(server.insert(
        "__e2ee_device_keys",
        std::collections::BTreeMap::from([
            ("deviceId".to_owned(), Value::Uuid(device)),
            ("publicKey".to_owned(), Value::Bytes(vec![7; 32])),
            ("mechanism".to_owned(), text("jazz.wrap.hpke-x25519")),
            ("version".to_owned(), Value::I64(1)),
            ("signingPublicKey".to_owned(), Value::Bytes(vec![8; 32])),
            ("signingMechanism".to_owned(), text("jazz.sodium.sign")),
            ("signingVersion".to_owned(), Value::I64(1)),
        ]),
        Default::default(),
    ))
    .unwrap();
    block_on(server.insert(
        "__e2ee_account_roots",
        std::collections::BTreeMap::from([
            ("ledgerVersion".to_owned(), Value::I64(version)),
            ("generation".to_owned(), Value::I64(0)),
            (
                "accountId".to_owned(),
                Value::Uuid(who.account_id().unwrap().0),
            ),
            ("deviceId".to_owned(), Value::Uuid(device)),
            ("epochId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
            ("kind".to_owned(), text(kind)),
        ]),
        Default::default(),
    ))
    .unwrap();
}

/// Contract: a root of a kind this client does not know stays evidence of
/// the account: under another ledger version it is reported as an
/// unsupported capability, under this one it is malformed and never
/// counts; either way the account does not look empty, so the device
/// bootstraps nothing over it.
#[test]
fn a_root_of_an_unknown_kind_is_unsupported_or_malformed_never_absent() {
    let settings = EncryptionSettings::default();
    for (byte, version) in [(0xb1, 2), (0xb2, 1)] {
        let who = account(byte);
        let server = secure_authority_with(&settings, None);
        server_root(&server, who, "passkey", version);
        let client = open_account(Rc::new(MemoryStore::new()), who, settings.clone(), None);
        let (db, _link) = session(&client, &server, who, &settings, 1, None);
        let refused = drive(&server, &[&db], client.ensure_account(&db));
        match version {
            2 => assert!(
                matches!(
                    refused,
                    Err(ClientError::Protocol(ProtocolError::Unsupported(_)))
                ),
                "{refused:?}"
            ),
            _ => assert!(refused.is_err(), "{refused:?}"),
        }
        assert_eq!(count(&server, "__e2ee_account_identities"), 0);
        assert_eq!(count(&server, "__e2ee_account_roots"), 1);
    }
}

/// The authority at `path`, closed again: a copy of an empty server.
fn empty_server_at(path: &std::path::Path, settings: &EncryptionSettings) {
    drop(secure_authority_with(settings, Some(path)));
}

/// Moves the authority's cut forward by `n` positions of its own.
fn advance_cut(server: &jazz::db::Db, n: usize) {
    for i in 0..n {
        block_on(server.insert(
            "projects",
            std::collections::BTreeMap::from([("name".to_owned(), text(&format!("filler {i}")))]),
            Default::default(),
        ))
        .unwrap();
    }
}

fn identity_and_root_counts(server: &jazz::db::Db) -> (usize, usize, usize) {
    (
        count(server, "__e2ee_device_requests"),
        count(server, "__e2ee_account_identities"),
        count(server, "__e2ee_account_roots"),
    )
}

/// Contract: a device that has verified its account, shown the account as
/// empty by a server restored from before the account existed, publishes
/// nothing (no request, no bootstrap, so no second root the identity would
/// sign) and reports `stale-state`; for an app account and a device-account
/// one.
///
/// ```text
/// (copy taken)  device ──bootstrap, verify──► server
/// device ──verify──► restored copy (empty) ──✗ stale-state, nothing written
/// ```
#[test]
fn a_device_with_a_floor_publishes_nothing_to_an_emptied_server() {
    let settings = EncryptionSettings::default();
    let (lena, identity) = device_account(0x3f);
    for (who, identity) in [(account(0xa5), None), (lena, Some(identity))] {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let empty = dir.path().join("empty.sqlite");
        empty_server_at(&live, &settings);
        copy_database(&live, &empty);
        let client = open_account(Rc::new(MemoryStore::new()), who, settings.clone(), identity);
        {
            let server = secure_authority_with(&settings, Some(&live));
            let (db, _link) = session(&client, &server, who, &settings, 1, None);
            assert_eq!(
                drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
                AccountStatus::Active
            );
        }
        let emptied = secure_authority_with(&settings, Some(&empty));
        advance_cut(&emptied, 40);
        let (db, _link) = session(&client, &emptied, who, &settings, 2, None);
        let refused = drive(&emptied, &[&db], client.ensure_account(&db));
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::StaleState))
            ),
            "{refused:?}"
        );
        pump(&emptied, &db);
        assert_eq!(identity_and_root_counts(&emptied), (0, 0, 0));
    }
}

/// Contract: once alice's device has verified her app account, a server
/// that swaps in another valid app root (from a copy taken before the
/// account existed, moved past the cut the device saw) is `root-not-mine`:
/// the floor remembers which root and device keys it verified, and the
/// account it verified before is forgotten.
#[test]
fn a_swapped_app_root_is_root_not_mine() {
    let settings = EncryptionSettings::default();
    let alice = account(0xa7);
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("server.sqlite");
    let empty = dir.path().join("empty.sqlite");
    empty_server_at(&live, &settings);
    copy_database(&live, &empty);
    let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    {
        let server = secure_authority_with(&settings, Some(&live));
        let (db, _link) = session(&client, &server, alice, &settings, 1, None);
        drive(&server, &[&db], client.ensure_account(&db)).unwrap();
    }
    assert!(client.with_account(|_| ()).is_some());
    let forged = secure_authority_with(&settings, Some(&empty));
    server_root(&forged, alice, "app", 1);
    advance_cut(&forged, 40);
    let (db, _link) = session(&client, &forged, alice, &settings, 2, None);
    let refused = drive(&forged, &[&db], client.ensure_account(&db));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::RootNotMine))
        ),
        "{refused:?}"
    );
    assert!(
        client.with_account(|_| ()).is_none(),
        "the account verified before is forgotten"
    );
    assert_eq!(
        identity_and_root_counts(&forged).0,
        0,
        "no request published"
    );
}

/// Contract: after alice's account was reset on her second device, a
/// server that replays the older generation to that device (restored from
/// before the reset) is refused as `stale-state`; the device publishes
/// nothing on it.
#[test]
fn an_older_generation_replayed_after_a_reset_is_stale_state() {
    let settings = EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let alice = account(0xaa);
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("server.sqlite");
    let old = dir.path().join("old.sqlite");
    let first = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let second = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    {
        let server = secure_authority_with(&settings, Some(&live));
        let (one, _l1) = session(&first, &server, alice, &settings, 1, None);
        drive(&server, &[&one], first.ensure_account(&one)).unwrap();
        let (two, _l2) = session(&second, &server, alice, &settings, 2, None);
        drive(&server, &[&two], second.ensure_account(&two)).unwrap();
    }
    copy_database(&live, &old);
    {
        let server = secure_authority_with(&settings, Some(&live));
        let (two, _l2) = session(&second, &server, alice, &settings, 3, None);
        assert_eq!(
            drive(&server, &[&two], second.reset_account(&two)).unwrap(),
            AccountStatus::Active
        );
    }
    let restored = secure_authority_with(&settings, Some(&old));
    advance_cut(&restored, 40);
    let before = identity_and_root_counts(&restored);
    let (two, _l2) = session(&second, &restored, alice, &settings, 4, None);
    let refused = drive(&restored, &[&two], second.ensure_account(&two));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::StaleState))
        ),
        "{refused:?}"
    );
    pump(&restored, &two);
    assert_eq!(identity_and_root_counts(&restored), before);
}
/// Contract: a device awaiting approval that has seen its account at a
/// later cut refuses to reset it on a server restored from an older copy:
/// the reset is `stale-state`, and nothing is published.
#[test]
fn a_reset_on_a_rolled_back_server_is_stale_state() {
    let settings = EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let alice = account(0xa9);
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("server.sqlite");
    let old = dir.path().join("old.sqlite");
    let first = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let second = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    {
        let server = secure_authority_with(&settings, Some(&live));
        let (one, _l1) = session(&first, &server, alice, &settings, 1, None);
        drive(&server, &[&one], first.ensure_account(&one)).unwrap();
        let (two, _l2) = session(&second, &server, alice, &settings, 2, None);
        assert_eq!(
            drive(&server, &[&two], second.ensure_account(&two)).unwrap(),
            AccountStatus::AwaitingApproval
        );
    }
    copy_database(&live, &old);
    {
        let server = secure_authority_with(&settings, Some(&live));
        advance_cut(&server, 5);
        let (two, _l2) = session(&second, &server, alice, &settings, 3, None);
        drive(&server, &[&two], second.ensure_account(&two)).unwrap();
    }
    let restored = secure_authority_with(&settings, Some(&old));
    let (two, _l2) = session(&second, &restored, alice, &settings, 4, None);
    let refused = drive(&restored, &[&two], second.reset_account(&two));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::StaleState))
        ),
        "{refused:?}"
    );
    assert_eq!(count(&restored, "__e2ee_account_identities"), 1);
}

/// The account registry as a test sets it: every account was founded by
/// a device-account identity.
struct AllDeviceAccounts;

impl jazz::db::AccountRegistryView for AllDeviceAccounts {
    fn founded_by_device_account<'a>(
        &'a self,
        _store: &'a jazz::storage::Storage,
        _account: jazz::account_registry::AccountId,
    ) -> futures::future::LocalBoxFuture<'a, Result<Option<bool>, String>> {
        Box::pin(async { Ok(Some(true)) })
    }
}

/// Contract: a publication counts only once the authority accepts it: a
/// bootstrap the authority refuses (here, an app root for an account
/// Jazz's registry says a device-account identity founded) fails with the
/// authority's refusal, and the device holds no account.
#[test]
fn a_refused_bootstrap_fails_with_the_refusal() {
    let settings = EncryptionSettings::default();
    let alice = account(0xa6);
    let server = secure_authority_with(&settings, None);
    server.set_account_registry(Rc::new(AllDeviceAccounts));
    let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (db, _link) = session(&client, &server, alice, &settings, 1, None);
    let refused = drive(&server, &[&db], client.ensure_account(&db));
    let Err(ClientError::Database(error)) = &refused else {
        panic!("{refused:?}");
    };
    assert_eq!(
        error.reason,
        Some(jazz::db::Reason::AuthorizationDenied),
        "{error:?}"
    );
    assert_eq!(count(&server, "__e2ee_account_roots"), 0);
    assert!(client.with_account(|_| ()).is_none());
}

/// Contract: if the server shows alice's device an account key envelope
/// with an unregistered mechanism, the device reports unsupported evidence
/// and clears its last verified account.
#[test]
fn an_unsupported_account_key_envelope_clears_the_verified_account() {
    let settings = EncryptionSettings::default();
    let alice = account(0xad);
    let unsupported = envelope::encode(Mechanism::new("example.unsupported", 1), &[]).unwrap();
    for column in ["envelope", "verification"] {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("server.sqlite");
        let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
        {
            let server = secure_authority_with(&settings, Some(&live));
            let (db, _link) = session(&client, &server, alice, &settings, 1, None);
            assert_eq!(
                drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
                AccountStatus::Active
            );
        }
        assert!(client.with_account(|_| ()).is_some());
        tamper(&live, "__e2ee_account_identities", column, &unsupported);

        let server = secure_authority_with(&settings, Some(&live));
        let (db, _link) = session(&client, &server, alice, &settings, 2, None);
        let refused = drive(&server, &[&db], client.ensure_account(&db));
        assert!(
            matches!(
                refused,
                Err(ClientError::Protocol(ProtocolError::Unsupported(
                    UnsupportedCapability::AccountKeyEnvelope
                )))
            ),
            "{column}: {refused:?}"
        );
        assert!(client.with_account(|_| ()).is_none(), "{column}");
    }
}

/// Contract: alice's device refuses private evidence the server altered:
/// with her identity's envelope replaced, her device obtains no account key
/// and holds no account, whatever the public rows say.
#[test]
fn altered_private_evidence_gives_no_account_key() {
    let settings = EncryptionSettings::default();
    let alice = account(0xa8);
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("server.sqlite");
    let store = MemoryStore::new();
    {
        let client = open_account(Rc::new(store.clone()), alice, settings.clone(), None);
        let server = secure_authority_with(&settings, Some(&live));
        let (db, _link) = session(&client, &server, alice, &settings, 1, None);
        drive(&server, &[&db], client.ensure_account(&db)).unwrap();
    }
    tamper(
        &live,
        "__e2ee_account_identities",
        "envelope",
        &[0x4a, 0x45, 0x32, 0x45, 1, 0],
    );
    // The same device after a restart: its floor passes (the public rows
    // are unchanged), its private evidence does not.
    let client = open_account(Rc::new(store), alice, settings.clone(), None);
    let server = secure_authority_with(&settings, Some(&live));
    let (db, _link) = session(&client, &server, alice, &settings, 2, None);
    let refused = drive(&server, &[&db], client.ensure_account(&db));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::AccountKeyUnavailable))
        ),
        "{refused:?}"
    );
    assert!(client.with_account(|_| ()).is_none());
}

/// Contract: once alice's device has verified her account, a server that
/// keeps her root but stops showing this device's own request gives
/// `stale-state`, and the device publishes nothing: no second request, so
/// the server cannot have it enrol again under its old identity.
///
/// ```text
/// alice's device ──request, bootstrap, verify──► server
/// server ──hides alice's device request (root kept)
/// alice's device ──verify──✗ stale-state, nothing written
/// ```
#[test]
fn a_hidden_own_request_is_stale_state_and_publishes_nothing() {
    let settings = EncryptionSettings::default();
    let alice = account(0xa9);
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("server.sqlite");
    let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    {
        let server = secure_authority_with(&settings, Some(&live));
        let (db, _link) = session(&client, &server, alice, &settings, 1, None);
        assert_eq!(
            drive(&server, &[&db], client.ensure_account(&db)).unwrap(),
            AccountStatus::Active
        );
    }
    let hidden = rusqlite::Connection::open(&live)
        .unwrap()
        .execute("DELETE FROM \"__e2ee_device_requests\"", [])
        .unwrap();
    assert_eq!(hidden, 1, "alice's one request");

    let server = secure_authority_with(&settings, Some(&live));
    assert_eq!(identity_and_root_counts(&server), (0, 1, 1));
    let (db, _link) = session(&client, &server, alice, &settings, 2, None);
    let refused = drive(&server, &[&db], client.ensure_account(&db));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::StaleState))
        ),
        "{refused:?}"
    );
    pump(&server, &db);
    assert_eq!(
        identity_and_root_counts(&server),
        (0, 1, 1),
        "no request published"
    );
}

/// Contract: an account that holds nothing but one row this client cannot
/// decode (here a root with a negative ledger version) is not empty: the
/// device publishes its request but never bootstraps over the row, and
/// reports the account as inconsistent.
///
/// ```text
/// server ──junk root for bea's account──► (stored, undecodable)
/// bea's device ──request──► server
///              ──verify──✗ inconsistent evidence, no identity, no root
/// ```
#[test]
fn one_undecodable_row_stops_a_bootstrap() {
    let settings = EncryptionSettings::default();
    let bea = account(0xaa);
    let server = secure_authority_with(&settings, None);
    server_root(&server, bea, "app", -1);
    let client = open_account(Rc::new(MemoryStore::new()), bea, settings.clone(), None);
    let (db, _link) = session(&client, &server, bea, &settings, 1, None);
    let refused = drive(&server, &[&db], client.ensure_account(&db));
    assert!(
        matches!(
            refused,
            Err(ClientError::Protocol(ProtocolError::InconsistentEvidence))
        ),
        "{refused:?}"
    );
    pump(&server, &db);
    assert_eq!(identity_and_root_counts(&server), (1, 0, 1));
}
