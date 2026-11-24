use super::*;
use crate::ForegroundDbCommandRequest as Request;
use crate::ForegroundDbCommandResponse as Response;
use crate::JazzNativeRelayStatus as Status;
use jazz::db::block_on;
use jazz::tools::deployment::artifact::AppDefinition;
use jazz_e2ee_client::MemoryStore;
use jazz_e2ee_client::host::bridge::{Handler, serve};
use serde_json::json;
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

const APP_ID: &str = "07070707-0707-4707-8707-070707070707";
const ALICE_ACCOUNT_ID: &str = "08080808-0808-4808-8808-080808080808";

/// Alice opens an encrypted app through the account-owned C ABI used by RN.
/// Its foreground cannot do database work until her private store has
/// answered. The foreground keeps the same node and reads its encryption
/// settings after setup. The persistent relay can start without these keys.
///
/// Alice ──admit──► keyless relay ──store requests/replies──► Alice ──open──► device
#[test]
fn a_leased_foreground_opens_with_the_apps_async_private_store() {
    let root = tempfile::tempdir().unwrap();
    let root_bytes = root.path().to_str().unwrap().as_bytes();
    let (app, schema) = encrypted_schema();
    let request = alice_request();
    let store = serve(Rc::new(MemoryStore::new()));

    unsafe {
        let host = jazz_native_relay_host_new();
        let lease = jazz_native_relay_host_retain(host, 1);
        let foreground = open_account_foreground(&*lease, root_bytes, &request, &schema);
        let execute = |command| leased_execute(&*lease, foreground, command).unwrap();
        let before = execute(Request::NativeSessionMetadata);
        assert_eq!(
            execute(Request::EncryptionOpen),
            Response::EncryptionOpen { ready: false }
        );
        assert!(matches!(
            execute(Request::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable
            }),
            Response::OperationError { .. },
        ));
        finish_store_open(&execute, &store);
        assert_eq!(execute(Request::NativeSessionMetadata), before);
        let Response::EncryptionResult { json } = execute(Request::EncryptionCall {
            op: "spaces".to_owned(),
            args_json: "{}".to_owned(),
        }) else {
            panic!("the leased foreground must have a device runtime");
        };
        assert!(
            !serde_json::from_str::<serde_json::Value>(&json)
                .unwrap()
                .as_object()
                .unwrap()
                .is_empty()
        );
        let relay = foreground_relay(&*lease, foreground);
        let sibling_lease = jazz_native_relay_host_retain(host, 2);
        let mut sibling_request: serde_json::Value = serde_json::from_slice(&request).unwrap();
        sibling_request["subject"] = json!("bob");
        sibling_request["account_id"] = json!("09090909-0909-0909-0909-090909090909");
        sibling_request["bearer"] = json!("bob-session");
        let sibling = open_account_foreground(
            &*sibling_lease,
            root_bytes,
            &serde_json::to_vec(&sibling_request).unwrap(),
            br#"{"tables":{}}"#,
        );
        let expired =
            readiness_while_sibling_holds_the_host(&*lease, foreground, &*sibling_lease, sibling);
        ready_expires_while_owner_is_busy(&relay, execute);
        readiness_reads_the_authority_again(&relay, execute, &app, &store);
        let Response::Pending { operation: retired } =
            leased_execute(&*sibling_lease, sibling, start_ready(2000)).unwrap()
        else {
            panic!("Bob's foreground should be live before invalidation");
        };
        assert_eq!(
            jazz_native_relay_host_lease_invalidate_foreground_runtime(sibling_lease),
            Status::Ok
        );
        for command in [
            start_ready(20),
            Request::Poll { operation: retired },
            Request::Cancel { operation: retired },
        ] {
            assert_eq!(
                leased_execute(&*sibling_lease, sibling, command),
                Err(Status::InvalidHandle)
            );
        }
        assert_eq!(execute(Request::NativeSessionMetadata), before);
        assert_eq!(execute(Request::Close), Response::Closed { closed: true });
        for command in [
            start_ready(20),
            Request::Poll { operation: expired },
            Request::Cancel { operation: expired },
        ] {
            assert_eq!(
                leased_execute(&*lease, foreground, command),
                Err(Status::InvalidHandle)
            );
        }
        jazz_native_relay_host_lease_free(sibling_lease);
        jazz_native_relay_host_lease_free(lease);
        jazz_native_relay_host_free(host);
    }
}

/// Alice's relay admits her encrypted foreground. A peer attached to the
/// same relay without a device runtime receives the keyless write refusal.
#[test]
fn a_keyless_peer_refuses_an_encrypted_write() {
    use jazz::records::{OwnedRecord, RecordDescriptor, ValueType};

    let root = tempfile::tempdir().unwrap();
    let (_, schema) = encrypted_schema();
    unsafe {
        let host = jazz_native_relay_host_new();
        let lease = jazz_native_relay_host_retain(host, 1);
        let foreground = open_account_foreground(
            &*lease,
            root.path().to_str().unwrap().as_bytes(),
            &alice_request(),
            &schema,
        );
        let relay = foreground_relay(&*lease, foreground);
        let author = AuthorSubject::authenticated("https://issuer.example", "alice")
            .unwrap()
            .with_account(jazz::account_registry::AccountId(
                ALICE_ACCOUNT_ID.parse().unwrap(),
            ));
        let peer = relay
            .attach_client(fresh_client_identity(author).unwrap(), BTreeMap::new())
            .unwrap();
        let Response::TransactionOpened { transaction } = peer
            .execute(Request::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            })
            .unwrap()
        else {
            panic!("the keyless peer could not open a transaction")
        };
        let descriptor =
            RecordDescriptor::new([("name", ValueType::String), ("ownerId", ValueType::Uuid)]);
        let raw = descriptor
            .create(&[
                Value::String("plans".into()),
                Value::Uuid(ALICE_ACCOUNT_ID.parse().unwrap()),
            ])
            .unwrap();
        let response = peer
            .execute(Request::Insert {
                transaction,
                table: "projects".to_owned(),
                row_id: None,
                cells: jazz::binding_codec::encode_named_cells(&OwnedRecord::new(raw, descriptor))
                    .unwrap(),
            })
            .unwrap();
        let response = match response {
            Response::Inserted { .. } => peer
                .execute(Request::CommitTransaction { transaction })
                .unwrap(),
            other => other,
        };
        assert!(
            matches!(&response, Response::CodedOperationError { reason, .. }
            if reason.contains("holds no encryption keys") && reason.contains("projects.name")),
            "expected the keyless write refusal, got {response:?}"
        );
        peer.close().unwrap();
        jazz_native_relay_host_lease_free(lease);
        jazz_native_relay_host_free(host);
    }
}

/// Alice's laptop founds her account, but the authority refuses its recovery root.
/// Her fresh phone has the account secret, but each readiness deadline must
/// report awaiting approval once the phone has verified that the root is absent.
#[test]
fn a_leased_phone_preserves_missing_recovery_evidence_at_its_deadline() {
    use jazz::tools::identity::{
        DEVICE_ACCOUNT_ISSUER, derive_user_id, mint_jazz_self_signed_token,
    };
    let root = tempfile::tempdir().unwrap();
    let root_bytes = root.path().to_str().unwrap().as_bytes();
    let (app, schema) = encrypted_schema();
    let seed = [0x42; 32];
    let subject = derive_user_id(&seed).to_string();
    let account = jazz::account_registry::device_account_id(APP_ID.parse().unwrap(), &subject);
    let author = AuthorSubject::reserved(DEVICE_ACCOUNT_ISSUER, &subject)
        .unwrap()
        .with_account(account);
    let request = json!({
        "registry": format!("https://edge.example/apps/{APP_ID}/accounts"),
        "app_id": APP_ID, "env": "test", "account_id": account.0.to_string(),
        "issuer": DEVICE_ACCOUNT_ISSUER, "subject": subject,
        "jwt": mint_jazz_self_signed_token(&seed, DEVICE_ACCOUNT_ISSUER, APP_ID, 3600).unwrap(),
        "device_account_secret": jazz::identity::DeviceAccountSecret::from_bytes(seed).format().as_str(), "server_url": null,
    });
    unsafe {
        let host = jazz_native_relay_host_new();
        let lease = jazz_native_relay_host_retain(host, 1);
        let open = |request: &serde_json::Value| {
            open_account_foreground(
                &*lease,
                root_bytes,
                &serde_json::to_vec(request).unwrap(),
                &schema,
            )
        };
        let laptop = open(&request);
        let relay = foreground_relay(&*lease, laptop);
        let pump = connect_authority(&relay, &app, author, false);
        let ready = |foreground, store: &Handler, timeout| {
            let execute = |cmd| leased_execute(&*lease, foreground, cmd).unwrap();
            settle(&execute, execute(start_ready(timeout)), || {
                pump();
                answer_store(&execute, store);
            })
        };
        let open_store = |foreground| {
            let store = serve(Rc::new(MemoryStore::new()));
            finish_store_open(
                &|cmd| leased_execute(&*lease, foreground, cmd).unwrap(),
                &store,
            );
            store
        };
        let refused = ready(laptop, &open_store(laptop), 10_000);
        assert!(
            matches!(&refused, Response::OperationError { reason }
            if reason.contains("authorization_denied") && reason.contains("__e2ee_recovery_roots")),
            "the authority must refuse the founder's recovery registration: {refused:?}"
        );
        let phone = open(&request);
        let store = open_store(phone);
        let awaiting = Response::EncryptionResult {
            json: json!("awaiting-approval").to_string(),
        };
        let first_pass = Instant::now() + Duration::from_secs(60);
        loop {
            match ready(phone, &store, 2000) {
                Response::OperationError { reason }
                    if reason.starts_with("timeout:") && Instant::now() < first_pass =>
                {
                    continue;
                }
                result => {
                    assert_eq!(result, awaiting);
                    break;
                }
            }
        }
        for _ in 0..4 {
            let started = Instant::now();
            assert_eq!(ready(phone, &store, 1500), awaiting);
            assert!(
                started.elapsed() >= Duration::from_millis(1500),
                "the phone must wait for recovery until its deadline"
            );
        }
        jazz_native_relay_host_lease_free(lease);
        jazz_native_relay_host_free(host);
    }
}

/// Alice's device account opens its first encrypted foreground: the native
/// host hands her account secret to that foreground's runtime, which wipes
/// it once its recovery at start is done, and keeps no copy of its own for
/// the rest of the session (as `devices-and-recovery.mdx` says).
///
/// Alice ──admit (secret)──► host ──first foreground──► runtime
///                            │
///                            └── holds no secret afterwards
///
/// White-box: what the host holds is not observable through its C ABI, so
/// the test reads the session's owner, and puts the secret there itself
/// rather than minting a device account whose proof the admission checks.
#[test]
fn the_host_keeps_no_account_secret_once_a_foreground_attached() {
    let root = tempfile::tempdir().unwrap();
    let root_bytes = root.path().to_str().unwrap().as_bytes();
    let (_, schema) = encrypted_schema();
    let request = alice_request();

    unsafe {
        let host = jazz_native_relay_host_new();
        let lease = jazz_native_relay_host_retain(host, 1);
        let admitted = admit_account(&*lease, root_bytes, &request, &schema);
        let capability = AdmissionCapability(admitted.as_slice().try_into().unwrap());
        let secret = || {
            (*host).lock().unwrap().account_session_owners[&capability]
                .device_account_secret
                .is_some()
        };
        (*host)
            .lock()
            .unwrap()
            .account_session_owners
            .get_mut(&capability)
            .unwrap()
            .device_account_secret =
            Some(jazz::identity::DeviceAccountSecret::from_bytes([0x42; 32]));
        assert!(secret());

        let foreground = open_admitted_foreground(&*lease, &admitted);
        assert!(!secret(), "the host must not keep the account secret");

        assert_eq!(
            leased_execute(&*lease, foreground, Request::Close).unwrap(),
            Response::Closed { closed: true }
        );
        jazz_native_relay_host_lease_free(lease);
        jazz_native_relay_host_free(host);
    }
}

fn encrypted_schema() -> (AppDefinition, Vec<u8>) {
    let app = jazz::tools::deployment::artifact::load_app(include_bytes!(
        "../../../jazz-codegen/tests/apps/encrypted/jazz.app.json"
    ))
    .unwrap();
    let mut schema = serde_json::to_value(app.current_schema()).unwrap();
    let bindings = json!({
        "schema": {
            "current": app.current_hash(),
            "schemas": [{ "hash": app.current_hash(), "schema": schema }],
            "migrations": [],
            "encryption": app.encryption(),
        },
        "relations": {},
    });
    schema["encryptionBindings"] = json!(bindings.to_string());
    let schema = serde_json::to_vec(&schema).unwrap();
    (app, schema)
}

fn alice_request() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "registry": format!("https://edge.example/apps/{APP_ID}/accounts"),
        "app_id": APP_ID,
        "env": "test",
        "account_id": ALICE_ACCOUNT_ID,
        "issuer": "https://issuer.example",
        "subject": "alice",
        "bearer": "alice-session",
        "server_url": null,
    }))
    .unwrap()
}

fn foreground_relay(lease: &JazzNativeRelayHostLease, foreground: u64) -> NativeRelay {
    let host = lease.lock_foreground(foreground).unwrap();
    host.relays[&host.foregrounds[&foreground].relay]
        .relay
        .clone()
}

fn answer_store(execute: &impl Fn(Request) -> Response, store: &Handler) -> usize {
    let Response::StoreRequests { requests } = execute(Request::TakeStoreRequests) else {
        panic!("the foreground must expose store requests");
    };
    let count = requests.len();
    for request in requests {
        let value: serde_json::Value = serde_json::from_str(&request).unwrap();
        let id = value["id"].as_str().unwrap().to_owned();
        let (ok, payload) = block_on(store(request));
        assert_eq!(
            execute(Request::ReplyStore { id, ok, payload }),
            Response::StoreReply { accepted: true }
        );
    }
    count
}

fn finish_store_open(execute: &impl Fn(Request) -> Response, store: &Handler) {
    let mut answered = 0;
    for _ in 0..32 {
        answered += answer_store(execute, store);
        match execute(Request::EncryptionOpen) {
            Response::EncryptionOpen { ready: true } => {
                assert!(answered > 0, "the runtime used the device's private store");
                return;
            }
            Response::EncryptionOpen { ready: false } => {}
            error => panic!("cannot attach device: {error:?}"),
        }
    }
    panic!("the encrypted foreground did not open");
}

fn connect_authority(
    relay: &NativeRelay,
    app: &AppDefinition,
    author: AuthorSubject,
    allow_registration: bool,
) -> impl Fn() {
    use jazz::binding_host::Binding;
    use jazz::db::{Db, DbConfig};
    use jazz::tools::deployment::artifact::system_tables;

    let mut schema = app.current_schema().clone();
    let declarations = app.encryption();
    let current = app.current_hash().to_string();
    let tables = declarations.entries(&current).unwrap();
    for (table, policies) in
        system_tables::derive_e2ee_policies(&schema, tables, &declarations.settings).unwrap()
    {
        schema.get_mut(&table).unwrap().policies = policies;
    }
    if !allow_registration {
        use jazz::tools::public_schema::{OperationPolicy, PolicyExpr, TableName};
        schema
            .get_mut(&TableName::new(system_tables::RECOVERY_ROOTS))
            .unwrap()
            .policies
            .insert = OperationPolicy::with_check(PolicyExpr::False);
    }
    let root = DbIdentity {
        node: NodeUuid::from_bytes([0x42; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let db = block_on(Db::open_history_complete(DbConfig::new(
        JazzSchema::new(&schema).unwrap(),
        root,
    )))
    .unwrap();
    jazz::db::block_on(db.set_authority()).unwrap();
    let server = block_on(Binding::new(db, root, true));
    let link = block_on(server.accept_subscriber(author, Default::default())).unwrap();
    let wire = relay.wire().unwrap();
    relay.set_remote_link_hint(RemoteLinkHint::Live).unwrap();
    move || {
        link.receive(wire.take_outbound_encoded().unwrap());
        block_on(server.tick()).unwrap();
        for frame in link.take_outbound() {
            wire.push_inbound_encoded(&frame).unwrap();
        }
    }
}

fn open_account_foreground(
    lease: &JazzNativeRelayHostLease,
    root: &[u8],
    request: &[u8],
    schema: &[u8],
) -> u64 {
    let admitted = admit_account(lease, root, request, schema);
    open_admitted_foreground(lease, &admitted)
}

/// Begins the account session and attaches its schema: the admitted
/// scope's capability.
fn admit_account(
    lease: &JazzNativeRelayHostLease,
    root: &[u8],
    request: &[u8],
    schema: &[u8],
) -> Vec<u8> {
    let lease = std::ptr::from_ref(lease).cast_mut();
    let mut out = JazzNativeRelayBytes::EMPTY;
    unsafe {
        assert_eq!(
            jazz_native_relay_host_lease_begin_account_session_json(
                lease,
                request.as_ptr(),
                request.len(),
                root.as_ptr(),
                root.len(),
                &mut out,
            ),
            Status::Ok
        );
        let pending = std::slice::from_raw_parts(out.data, out.len).to_vec();
        jazz_native_relay_bytes_free(&mut out);
        assert_eq!(
            jazz_native_relay_host_lease_attach_account_schema_json(
                lease,
                pending.as_ptr(),
                pending.len(),
                schema.as_ptr(),
                schema.len(),
                &mut out,
            ),
            Status::Ok
        );
        let admitted = std::slice::from_raw_parts(out.data, out.len).to_vec();
        jazz_native_relay_bytes_free(&mut out);
        admitted
    }
}

fn open_admitted_foreground(lease: &JazzNativeRelayHostLease, admitted: &[u8]) -> u64 {
    let lease = std::ptr::from_ref(lease).cast_mut();
    unsafe {
        let mut foreground = 0;
        assert_eq!(
            jazz_native_relay_host_lease_open_attached_foreground(
                lease,
                admitted.as_ptr(),
                admitted.len(),
                &mut foreground,
            ),
            Status::Ok
        );
        foreground
    }
}

fn leased_execute(
    lease: &JazzNativeRelayHostLease,
    foreground: u64,
    command: Request,
) -> Result<Response, Status> {
    let encoded = postcard::to_allocvec(&command).unwrap();
    let mut answer = JazzNativeRelayBytes::EMPTY;
    unsafe {
        let status = jazz_native_relay_host_lease_execute_foreground(
            std::ptr::from_ref(lease).cast_mut(),
            foreground,
            encoded.as_ptr(),
            encoded.len(),
            &mut answer,
        );
        if status != Status::Ok {
            return Err(status);
        }
        let result =
            postcard::from_bytes(std::slice::from_raw_parts(answer.data, answer.len)).unwrap();
        jazz_native_relay_bytes_free(&mut answer);
        Ok(result)
    }
}

fn start_ready(timeout: u64) -> Request {
    Request::EncryptionCall {
        op: "ready".to_owned(),
        args_json: json!({ "timeoutMs": timeout }).to_string(),
    }
}

/// Bob holds the shared lifecycle lock at the same boundary as a blocked
/// SQLite open. Alice's existing and new receipts must still reach their
/// deadline, and Bob cannot use or cancel them with his own lease.
fn readiness_while_sibling_holds_the_host(
    alice: &JazzNativeRelayHostLease,
    foreground: u64,
    bob: &JazzNativeRelayHostLease,
    sibling: u64,
) -> u64 {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    let Response::Pending {
        operation: existing,
    } = leased_execute(alice, foreground, start_ready(20)).unwrap()
    else {
        panic!("Alice's readiness must yield its receipt before host contention");
    };
    let (entered, busy) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let new = thread::scope(|scope| {
        let hold = scope.spawn(move || {
            let _host = bob.lock_foreground(sibling).unwrap();
            entered.send(()).unwrap();
            blocked.recv_timeout(Duration::from_secs(2)).is_ok()
        });
        busy.recv_timeout(Duration::from_secs(2)).unwrap();
        let started = leased_execute(alice, foreground, start_ready(20));
        let new = match started {
            Ok(Response::Pending { operation }) => operation,
            error => panic!("the sibling delayed Alice's readiness submission: {error:?}"),
        };
        thread::sleep(Duration::from_millis(30));
        for operation in [existing, new] {
            assert!(
                matches!(leased_execute(alice, foreground, Request::Poll { operation }),
                Ok(Response::OperationError { reason }) if reason.starts_with("timeout:"))
            );
            assert_eq!(
                leased_execute(alice, foreground, Request::Cancel { operation }),
                Ok(Response::Cancelled { cancelled: false })
            );
        }
        for command in [
            start_ready(20),
            Request::Poll { operation: new },
            Request::Cancel { operation: new },
        ] {
            assert_eq!(
                leased_execute(bob, foreground, command),
                Err(Status::InvalidHandle)
            );
        }
        let _ = release.send(());
        assert!(
            hold.join().unwrap(),
            "readiness waited for the sibling's host lock"
        );
        new
    });
    let mut plain = leased_execute(bob, sibling, start_ready(2000)).unwrap();
    while let Response::Pending { operation } = plain {
        thread::sleep(Duration::from_millis(2));
        plain = leased_execute(bob, sibling, Request::Poll { operation }).unwrap();
    }
    assert_eq!(
        plain,
        Response::OperationError {
            reason: "the app does not declare encryption".to_owned()
        }
    );
    new
}

fn settle(
    execute: &impl Fn(Request) -> Response,
    mut response: Response,
    pump: impl Fn(),
) -> Response {
    let deadline = Instant::now() + Duration::from_secs(15);
    while let Response::Pending { operation } = response {
        assert!(
            Instant::now() < deadline,
            "the foreground operation did not finish"
        );
        pump();
        thread::sleep(Duration::from_millis(2));
        response = execute(Request::Poll { operation });
    }
    response
}

/// After Alice changes her account by registering a recovery root, her next
/// ready cannot succeed on the previous verification while authority frames
/// are held. Once frames flow, the current account is active again.
fn readiness_reads_the_authority_again(
    relay: &NativeRelay,
    execute: impl Fn(Request) -> Response,
    app: &jazz::tools::deployment::artifact::AppDefinition,
    store: &jazz_e2ee_client::host::bridge::Handler,
) {
    let author = AuthorSubject::authenticated("https://issuer.example", "alice")
        .unwrap()
        .with_account(jazz::account_registry::AccountId(
            ALICE_ACCOUNT_ID.parse().unwrap(),
        ));
    let pump = connect_authority(relay, app, author, true);
    let call = |op: &str, args: serde_json::Value, carry: bool| {
        settle(
            &execute,
            execute(Request::EncryptionCall {
                op: op.to_owned(),
                args_json: args.to_string(),
            }),
            || {
                if carry {
                    pump();
                    answer_store(&execute, store);
                }
            },
        )
    };
    let active = Response::EncryptionResult {
        json: "\"active\"".to_owned(),
    };
    assert_eq!(call("ready", json!({ "timeoutMs": 10_000 }), true), active);
    assert_eq!(
        call(
            "recoveryRegister",
            json!({ "secret": "42".repeat(32) }),
            true
        ),
        Response::EncryptionResult {
            json: "null".to_owned()
        }
    );
    assert!(
        matches!(
            call("ready", json!({ "timeoutMs": 150 }), false),
            Response::OperationError { .. }
        ),
        "readiness returned stale account status without contacting the authority again"
    );
    assert_eq!(call("ready", json!({ "timeoutMs": 10_000 }), true), active);
}

/// Alice's readiness start and Poll return while her relay owner's previous
/// command is still running. Once readiness expires, its handle is gone.
fn ready_expires_while_owner_is_busy(relay: &NativeRelay, execute: impl Fn(Request) -> Response) {
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    let (entered, owner_busy) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    thread::scope(|scope| {
        let hold = scope.spawn(move || {
            relay.run(move |_| {
                entered.send(()).unwrap();
                let _ = blocked.recv_timeout(Duration::from_secs(2));
                Ok(())
            })
        });
        owner_busy.recv_timeout(Duration::from_secs(2)).unwrap();
        let start = Instant::now();
        let Response::Pending { operation } = execute(Request::EncryptionCall {
            op: "ready".to_owned(),
            args_json: r#"{"timeoutMs":20}"#.to_owned(),
        }) else {
            panic!("readiness must be admitted without waiting on the owner");
        };
        thread::sleep(Duration::from_millis(30));
        let response = execute(Request::Poll { operation });
        let elapsed = start.elapsed();
        let cancelled = execute(Request::Cancel { operation });
        let _ = release.send(());
        hold.join().unwrap().unwrap();
        assert!(
            elapsed < Duration::from_secs(1),
            "Poll waited for the owner"
        );
        assert!(
            matches!(response, Response::OperationError { reason } if reason.starts_with("timeout:") && reason.contains("relay owner"))
        );
        assert_eq!(cancelled, Response::Cancelled { cancelled: false });
    });
}

/// The private ABI 7 commands and responses append to the existing postcard
/// encoding. The JavaScript wrapper copies these exact discriminants and fields.
#[test]
fn encrypted_foreground_postcard_bytes() {
    assert_eq!(
        postcard::to_allocvec(&Request::EncryptionOpen).unwrap(),
        [42]
    );
    assert_eq!(
        postcard::to_allocvec(&Request::TakeStoreRequests).unwrap(),
        [43]
    );
    assert_eq!(
        postcard::to_allocvec(&Request::ReplyStore {
            id: "2".to_owned(),
            ok: true,
            payload: "null".to_owned()
        })
        .unwrap(),
        b"\x2c\x012\x01\x04null"
    );
    assert_eq!(
        postcard::to_allocvec(&Request::EncryptionCall {
            op: "state".to_owned(),
            args_json: "{}".to_owned()
        })
        .unwrap(),
        b"\x2d\x05state\x02{}"
    );
    assert_eq!(
        postcard::to_allocvec(&Request::TakeEncryptionReports).unwrap(),
        [46]
    );
    assert_eq!(
        postcard::to_allocvec(&Response::EncryptionOpen { ready: false }).unwrap(),
        [33, 0]
    );
    assert_eq!(
        postcard::to_allocvec(&Response::StoreRequests {
            requests: vec!["one".to_owned(), "two".to_owned()]
        })
        .unwrap(),
        b"\x22\x02\x03one\x03two"
    );
    assert_eq!(
        postcard::to_allocvec(&Response::StoreReply { accepted: true }).unwrap(),
        [35, 1]
    );
    assert_eq!(
        postcard::to_allocvec(&Response::EncryptionResult {
            json: "null".to_owned()
        })
        .unwrap(),
        b"\x24\x04null"
    );
    assert_eq!(
        postcard::to_allocvec(&Response::EncryptionReports { reports: vec![] }).unwrap(),
        [37, 0]
    );
}
