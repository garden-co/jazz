//! A relay serves an encrypted app: its own store holds no keys, a device UI
//! peer attaches with the runtime over a private store the app supplies, and
//! a peer attached without one is keyless like the relay. (The runtime's own
//! flows, which need a server, are tested through the shared host.)
use super::*;
use std::rc::Rc;

use jazz::ids::AuthorSubject;
use jazz::records::{OwnedRecord, RecordDescriptor, ValueType};
use jazz_e2ee_client::MemoryStore;
use jazz_e2ee_client::host::bridge::{JsStore, serve};

use crate::encryption::EncryptedDevice;

const APP: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
    "tables": {
        "projects": { "columns": { "name": "string" } },
        "notes": {
            "columns": { "projectId": "uuid", "title": "string", "body": "bytes" },
            "relations": { "project": { "rel": "projects", "column": "projectId" } },
            "encryption": { "space": "projectId", "scope": "projects",
                "columns": { "body": "string" } }
        }
    } }"#;

fn identity() -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([0x01; 16]),
        author: AuthorSubject::for_test_bytes([0x02; 16]),
    }
}

fn cells(fields: Vec<(&str, ValueType, Value)>) -> Vec<u8> {
    let descriptor =
        RecordDescriptor::new(fields.iter().map(|(name, kind, _)| (*name, kind.clone())));
    let values: Vec<Value> = fields.into_iter().map(|(_, _, value)| value).collect();
    let raw = descriptor.create(&values).expect("the cells are valid");
    jazz::binding_codec::encode_named_cells(&OwnedRecord::new(raw, descriptor))
        .expect("the cells encode")
}

fn device(account_secret: Option<String>) -> EncryptedDevice {
    EncryptedDevice {
        app_file: APP.as_bytes().to_vec(),
        environment: "test".to_owned(),
        app_id: "relay-encryption".to_owned(),
        device_account_secret: account_secret,
        store: Box::new(|| Rc::new(JsStore::served_by(serve(Rc::new(MemoryStore::new()))))),
    }
}

/// Writes `table` through `client` in a transaction and commits it.
fn write(client: &NativeRelayClient, table: &str, cells: Vec<u8>) -> Result<(), RelayError> {
    let ForegroundDbCommandResponse::TransactionOpened { transaction } =
        client.execute(ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        })?
    else {
        panic!("a transaction opens");
    };
    let failed = |response: ForegroundDbCommandResponse| match response {
        ForegroundDbCommandResponse::OperationError { reason }
        | ForegroundDbCommandResponse::CodedOperationError { reason, .. } => {
            Err(RelayError::Drive(reason))
        }
        _ => Ok(()),
    };
    failed(client.execute(ForegroundDbCommandRequest::Insert {
        transaction,
        table: table.to_owned(),
        cells,
        row_id: None,
    })?)?;
    failed(client.execute(ForegroundDbCommandRequest::CommitTransaction { transaction })?)?;
    Ok(())
}

/// Contract: an encrypted app opens on a relay. A device UI peer carries the
/// runtime's hooks, so a plaintext write to an encrypted column is refused
/// until the device holds the space's key, not stored; a peer attached without
/// the runtime is keyless and refuses it by name; plain data goes through
/// both; only the device has an encryption runtime to call.
///
/// ```text
/// device (runtime, own store) ──body ✗ (no key yet)──► relay (keyless store)
/// device ──project (plain) ✓──► relay ──► keyless peer reads it
/// keyless peer ──body ✗ "holds no encryption keys"
/// ```
#[test]
fn a_relay_serves_an_encrypted_app_to_a_device_and_a_keyless_peer() {
    let (mut schema, migrations, declarations) =
        jazz::tools::deployment::artifact::load_client_encrypted(
            APP.as_bytes(),
            jazz::tools::AppId::from_name("relay-encryption"),
            "test",
        )
        .expect("the app loads");
    let declarations = declarations.expect("the app declares encryption");
    // The plain table is open to every peer, as the app's permissions would
    // make it.
    let allow = jazz::tools::PolicyExpr::True;
    schema
        .get_mut(&jazz::tools::TableName::new("projects"))
        .expect("the plain table")
        .policies = jazz::tools::TablePolicies::new()
        .with_select(allow.clone())
        .with_insert(allow.clone())
        .with_update(Some(allow.clone()), allow.clone())
        .with_delete(allow);
    let directory = tempfile::tempdir().unwrap();
    let config = RelayOpenConfig {
        supported_abi: NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1,
            maximum: NATIVE_RELAY_ABI_V1,
        },
        scope: RelayScope {
            app_namespace: "app".to_owned(),
            storage_namespace: "default".to_owned(),
            auth_scope: Some("alice".to_owned()),
        },
        sqlite_path: directory.path().join("relay.sqlite"),
        schema: jazz::schema::JazzSchema::new(&schema).unwrap(),
        migrations,
        identity: identity(),
    };
    // Without the runtime's support the relay still refuses an encrypted app.
    assert!(NativeRelay::spawn(config.clone()).is_err());
    let relay = NativeRelay::spawn_encrypted(config, declarations).expect("an encrypted app opens");

    let device_peer = relay
        .attach_client_encrypted(identity(), BTreeMap::new(), device(None))
        .expect("a device attaches with the runtime");
    let keyless_peer = relay
        .attach_client(identity(), BTreeMap::new())
        .expect("a peer attaches keyless");

    let project = NodeUuid::from_bytes([0x09; 16]).0;
    let note = |body: &str| {
        cells(vec![
            ("projectId", ValueType::Uuid, Value::Uuid(project)),
            ("title", ValueType::String, Value::String("title".into())),
            ("body", ValueType::String, Value::String(body.into())),
        ])
    };
    // The device has no account or key yet (no server here): the write is
    // refused, and nothing of the plaintext is stored.
    let refused = write(&device_peer, "notes", note("PLAINTEXT-NOBODY-MAY-STORE"))
        .expect_err("an encrypted column needs a key");
    let refused = refused.to_string();
    assert!(
        refused.contains("encrypted tables cannot be read or written")
            || refused.contains("pending"),
        "the device's hooks refuse it until it has the keys: {refused}"
    );
    // A peer without the runtime is keyless.
    let refused = write(&keyless_peer, "notes", note("BACKEND-PLAINTEXT"))
        .expect_err("a keyless peer cannot write an encrypted column")
        .to_string();
    assert!(refused.contains("holds no encryption keys"), "{refused}");

    // Plain data goes through, from either peer, and relays to the other.
    write(
        &device_peer,
        "projects",
        cells(vec![(
            "name",
            ValueType::String,
            Value::String("plans".into()),
        )]),
    )
    .expect("a plain table works on a device");
    // A read may suspend on the relay: poll it, driving the relay between.
    let rows = |peer: &NativeRelayClient| {
        let mut response = peer
            .execute(ForegroundDbCommandRequest::All {
                query: postcard::to_allocvec(&jazz::query::Query::from("projects")).unwrap(),
                options_json: "{}".into(),
                transaction: None,
            })
            .expect("a plain read works");
        for _ in 0..400 {
            relay.pump().unwrap();
            if let ForegroundDbCommandResponse::Rows { rows } = &response {
                let count = jazz::binding_codec::decode_rows(rows)
                    .unwrap()
                    .iter()
                    .map(|batch| batch.rows.len())
                    .sum::<usize>();
                if count > 0 {
                    return count;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
            response = match response {
                ForegroundDbCommandResponse::Pending { operation } => peer
                    .execute(ForegroundDbCommandRequest::Poll { operation })
                    .expect("a pending read polls"),
                _ => peer
                    .execute(ForegroundDbCommandRequest::All {
                        query: postcard::to_allocvec(&jazz::query::Query::from("projects"))
                            .unwrap(),
                        options_json: "{}".into(),
                        transaction: None,
                    })
                    .expect("a plain read works"),
            };
        }
        panic!("no plain row reached the peer: {response:?}");
    };
    assert_eq!(
        rows(&keyless_peer),
        1,
        "the plain row relayed to the other peer"
    );

    // Only the device has a runtime.
    let id = device_peer
        .encryption_call("deviceId", serde_json::json!({}))
        .expect("the device's runtime answers");
    assert!(id.is_string());
    assert!(
        keyless_peer
            .encryption_call("deviceId", serde_json::json!({}))
            .is_err()
    );
}

#[test]
fn a_fresh_relay_device_recovers_from_its_account_secret() {
    check_recovery_readiness(true);
}

/// A phone whose account is verified but has no recovery root remains
/// awaiting approval when its relay readiness budget expires.
#[test]
fn a_relay_readiness_deadline_preserves_missing_recovery_evidence() {
    check_recovery_readiness(false);
}

fn check_recovery_readiness(allow_registration: bool) {
    use std::thread;
    use std::time::{Duration, Instant};

    use jazz::account_registry::device_account_id;
    use jazz::binding_host::Binding;
    use jazz::db::{Db, DbConfig, block_on};
    use jazz::tools::deployment::artifact::{load_client_encrypted, system_tables};
    use jazz::tools::identity::derive_user_id;
    use jazz::tools::public_schema::{OperationPolicy, PolicyExpr, TableName};

    let app_id = jazz::tools::AppId::from_name("relay-encryption");
    let (schema, migrations, declarations) =
        load_client_encrypted(APP.as_bytes(), app_id, "test").unwrap();
    let declarations = declarations.unwrap();
    let client_schema = jazz::schema::JazzSchema::new(&schema).unwrap();

    // Deploy's system-table policies govern the authority. The client app
    // loader supplies the table definitions and the declarations.
    let mut schema = schema;
    for (table, policies) in
        system_tables::derive_e2ee_policies(&schema, &declarations.tables, &declarations.settings)
            .unwrap()
    {
        schema.get_mut(&table).unwrap().policies = policies;
    }
    if !allow_registration {
        schema
            .get_mut(&TableName::new(system_tables::RECOVERY_ROOTS))
            .unwrap()
            .policies
            .insert = OperationPolicy::with_check(PolicyExpr::False);
    }
    let server_schema = jazz::schema::JazzSchema::new(&schema).unwrap();
    let authority = DbIdentity {
        node: NodeUuid::from_bytes([0x20; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let server = block_on(Db::open_history_complete(
        DbConfig::new(server_schema, authority)
            .with_migrations(migrations.schemas.clone(), migrations.lenses.clone()),
    ))
    .unwrap();
    jazz::db::block_on(server.set_authority()).unwrap();
    let server = block_on(Binding::new(server, authority, true));

    let seed = [0x42; 32];
    let subject = derive_user_id(&seed).to_string();
    let account = device_account_id(*app_id.uuid(), &subject);
    let author = AuthorSubject::reserved(AuthorSubject::DEVICE_ACCOUNT_ISSUER, &subject)
        .unwrap()
        .with_account(account);
    let identity = |node| DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author,
    };
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn_encrypted(
        RelayOpenConfig {
            supported_abi: NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_V1,
                maximum: NATIVE_RELAY_ABI_V1,
            },
            scope: RelayScope {
                app_namespace: "relay-encryption".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some("alice".to_owned()),
            },
            sqlite_path: directory.path().join("relay.sqlite"),
            schema: client_schema,
            migrations,
            identity: identity(0x31),
        },
        declarations,
    )
    .unwrap();
    let wire = relay.wire().unwrap();
    let link = block_on(server.accept_subscriber(author, Default::default())).unwrap();
    let wait_for_ready = |label: &str, device: &NativeRelayClient, timeout: Duration| {
        thread::scope(|scope| {
            let verify = scope.spawn(|| {
                device.encryption_call_with_timeout("ready", serde_json::json!({}), timeout)
            });
            let mut outbound = 0;
            let mut inbound = 0;
            let deadline = Instant::now() + timeout + Duration::from_secs(1);
            while !verify.is_finished() && Instant::now() < deadline {
                let frames = wire.take_outbound_encoded().unwrap();
                outbound += frames.len();
                link.receive(frames);
                block_on(server.tick()).unwrap();
                for frame in link.take_outbound() {
                    wire.push_inbound_encoded(&frame).unwrap();
                    inbound += 1;
                }
                thread::sleep(Duration::from_millis(2));
            }
            assert!(
                verify.is_finished(),
                "{label} did not finish readiness with {outbound} outbound and {inbound} inbound frames"
            );
            verify.join().unwrap()
        })
    };
    let secret = "42".repeat(32);

    if allow_registration {
        let mut wrong = device(Some(secret.clone()));
        wrong.environment = "a-different-deployment".to_owned();
        let mismatch = relay.attach_client_encrypted(identity(0x32), BTreeMap::new(), wrong);
        assert!(mismatch.is_err_and(|error| error.to_string().contains("do not match")));
    }

    // Attach before carrying any upstream frames. A device waits for the
    // confirmed catalogue before asking its first read, even when its
    // foreground joined the relay before the relay met its authority.
    let laptop = relay
        .attach_client_encrypted(
            identity(0x33),
            BTreeMap::new(),
            device(Some(secret.clone())),
        )
        .unwrap();
    let laptop_status = wait_for_ready("laptop", &laptop, Duration::from_secs(15));
    if allow_registration {
        assert_eq!(laptop_status.unwrap(), "active");
    } else {
        let refused = laptop_status.unwrap_err();
        assert!(
            matches!(refused, RelayError::Drive(ref reason)
                if reason.contains("authorization_denied") && reason.contains(system_tables::RECOVERY_ROOTS)),
            "the authority refused the founder's recovery registration: {refused}"
        );
    }
    let phone = relay
        .attach_client_encrypted(identity(0x34), BTreeMap::new(), device(Some(secret)))
        .unwrap();
    if allow_registration {
        assert_eq!(
            wait_for_ready("phone", &phone, Duration::from_secs(15)).unwrap(),
            "active"
        );
        return;
    }
    // The phone's first pass finds no recovery root. How long that takes
    // depends on the runner, so wait for it with short deadlines: before
    // it, a deadline has no evidence to preserve and times out.
    let first_pass = Instant::now() + Duration::from_secs(60);
    let first = loop {
        match wait_for_ready("phone", &phone, Duration::from_secs(2)) {
            Err(RelayError::Drive(reason))
                if reason.starts_with("timeout:") && Instant::now() < first_pass => {}
            outcome => break outcome,
        }
    };
    assert_eq!(first.unwrap(), "awaiting-approval");
    // The runtime keeps looking for the root on a backoff. Each later
    // deadline, whether it falls inside one of those passes or between
    // them, still answers with the missing evidence.
    for _ in 0..4 {
        assert_eq!(
            wait_for_ready("phone", &phone, Duration::from_millis(1500)).unwrap(),
            "awaiting-approval"
        );
    }
}

/// Readiness spends its own budget while the relay owner is busy with a
/// different operation, even though no store callback is blocking.
#[test]
fn a_queued_relay_readiness_call_uses_its_own_deadline() {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    let app_id = jazz::tools::AppId::from_name("relay-encryption");
    let (schema, migrations, declarations) =
        jazz::tools::deployment::artifact::load_client_encrypted(APP.as_bytes(), app_id, "test")
            .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn_encrypted(
        RelayOpenConfig {
            supported_abi: NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_V1,
                maximum: NATIVE_RELAY_ABI_V1,
            },
            scope: RelayScope {
                app_namespace: "relay-encryption".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some("alice".to_owned()),
            },
            sqlite_path: directory.path().join("relay.sqlite"),
            schema: jazz::schema::JazzSchema::new(&schema).unwrap(),
            migrations,
            identity: identity(),
        },
        declarations.unwrap(),
    )
    .unwrap();
    let client = relay
        .attach_client_encrypted(identity(), BTreeMap::new(), device(None))
        .unwrap();

    let (entered, owner_is_busy) = mpsc::channel();
    let (release, wait_for_release) = mpsc::channel();
    let (finished, ready_result) = mpsc::channel();
    thread::scope(|scope| {
        let blocker = scope.spawn(|| {
            relay.run(move |_| {
                entered.send(()).unwrap();
                wait_for_release
                    .recv_timeout(Duration::from_secs(3))
                    .unwrap();
                Ok(())
            })
        });
        owner_is_busy.recv_timeout(Duration::from_secs(3)).unwrap();
        let waiting = scope.spawn(|| {
            finished
                .send(client.encryption_call_with_timeout(
                    "ready",
                    serde_json::json!({}),
                    Duration::from_millis(20),
                ))
                .unwrap();
        });
        let result = ready_result.recv_timeout(Duration::from_millis(500));
        release.send(()).unwrap();
        blocker.join().unwrap().unwrap();
        waiting.join().unwrap();
        assert!(
            matches!(result, Ok(Err(RelayError::Drive(reason))) if reason.starts_with("timeout:")),
            "readiness must time out while the owner is still busy"
        );
    });
    assert!(
        client
            .encryption_call("deviceId", serde_json::json!({}))
            .unwrap()
            .is_string(),
        "the owner continues processing commands after readiness times out"
    );
}

/// Alice's device waits on readiness, which cannot finish with no server to
/// verify her account against. Meanwhile the relay owner keeps serving her
/// other foreground commands: a waiting encryption call is polled between
/// the owner's jobs instead of holding its thread until the call ends.
///
/// alice ──ready (2 s)──► owner ◄──other job── alice
///                          │ answers the job at once
///                          └── readiness times out at its deadline
#[test]
fn a_waiting_encryption_call_does_not_hold_the_relay_owner() {
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    let app_id = jazz::tools::AppId::from_name("relay-encryption");
    let (schema, migrations, declarations) =
        jazz::tools::deployment::artifact::load_client_encrypted(APP.as_bytes(), app_id, "test")
            .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn_encrypted(
        RelayOpenConfig {
            supported_abi: NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_V1,
                maximum: NATIVE_RELAY_ABI_V1,
            },
            scope: RelayScope {
                app_namespace: "relay-encryption".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some("alice".to_owned()),
            },
            sqlite_path: directory.path().join("relay.sqlite"),
            schema: jazz::schema::JazzSchema::new(&schema).unwrap(),
            migrations,
            identity: identity(),
        },
        declarations.unwrap(),
    )
    .unwrap();
    let alice = relay
        .attach_client_encrypted(identity(), BTreeMap::new(), device(None))
        .unwrap();

    let readiness = Duration::from_secs(2);
    let (finished, ready_result) = mpsc::channel();
    thread::scope(|scope| {
        let started = Instant::now();
        scope.spawn(|| {
            finished
                .send(alice.encryption_call_with_timeout("ready", serde_json::json!({}), readiness))
                .unwrap();
        });
        // Let the readiness call reach the owner and start waiting.
        thread::sleep(Duration::from_millis(100));
        assert!(
            ready_result.try_recv().is_err(),
            "readiness cannot finish without a server"
        );
        let answered = Instant::now();
        alice.pump_foreground().unwrap();
        assert!(
            answered.elapsed() < Duration::from_millis(500),
            "the owner answered another job only after {:?}",
            answered.elapsed()
        );
        assert!(
            started.elapsed() < readiness,
            "the other job was answered before readiness ended"
        );
        let result = ready_result.recv_timeout(readiness * 2).unwrap();
        assert!(
            matches!(result, Err(RelayError::Drive(ref reason)) if reason.starts_with("timeout:")),
            "readiness times out at its own deadline: {result:?}"
        );
    });
}

/// Declarations from another app than the relay's schema are refused before
/// the owner thread starts, rather than leaving encrypted columns unknown to
/// the keyless hooks; the app's own declarations open.
#[test]
fn a_relay_refuses_declarations_from_another_app() {
    const OTHER: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": {
            "projects": { "columns": { "name": "string" } },
            "docs": {
                "columns": { "projectId": "uuid", "text": "bytes" },
                "relations": { "project": { "rel": "projects", "column": "projectId" } },
                "encryption": { "space": "projectId", "scope": "projects",
                    "columns": { "text": "string" } }
            }
        } }"#;
    let app_id = jazz::tools::AppId::from_name("relay-encryption");
    let load = |app: &str| {
        let (schema, migrations, declarations) =
            jazz::tools::deployment::artifact::load_client_encrypted(
                app.as_bytes(),
                app_id,
                "test",
            )
            .unwrap();
        (schema, migrations, declarations.unwrap())
    };
    let (schema, migrations, declarations) = load(APP);
    let (_, _, other) = load(OTHER);
    let directory = tempfile::tempdir().unwrap();
    let config = || RelayOpenConfig {
        supported_abi: NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1,
            maximum: NATIVE_RELAY_ABI_V1,
        },
        scope: RelayScope {
            app_namespace: "app".to_owned(),
            storage_namespace: "default".to_owned(),
            auth_scope: Some("alice".to_owned()),
        },
        sqlite_path: directory.path().join("relay.sqlite"),
        schema: jazz::schema::JazzSchema::new(&schema).unwrap(),
        migrations: migrations.clone(),
        identity: identity(),
    };
    let refused = NativeRelay::spawn_encrypted(config(), other)
        .err()
        .expect("another app's declarations are refused");
    assert!(
        matches!(refused, RelayError::DeclarationsMismatch(ref why) if why.contains("docs")),
        "{refused}"
    );
    NativeRelay::spawn_encrypted(config(), declarations).expect("the app's own declarations open");
}
