//! Native host receipts through the same C ABI that Kotlin, Swift and the
//! React Native bridge use. Row behaviour itself belongs to the Db suites:
//! these prove the relay topology (one SQLite owner per scope, in-memory
//! foregrounds, the upstream socket) and its lifecycle.
use super::*;
use jazz::binding_codec::{decode_rows, decode_subscription_delta_rows};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::Query;
use jazz::records::{OwnedRecord, RecordDescriptor, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_server::{JazzServer, TestJwtIssuer};
use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

fn schema() -> JazzSchema {
    let allow = PolicyExpr::True;
    let policies = TablePolicies::new()
        .with_select(allow.clone())
        .with_insert(allow.clone())
        .with_update(Some(allow.clone()), allow.clone())
        .with_delete(allow);
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
    .unwrap()
}

fn title_cells(title: &str) -> Vec<u8> {
    let descriptor = RecordDescriptor::new([("title", ValueType::String)]);
    let raw = descriptor
        .create(&[Value::String(title.to_owned())])
        .expect("fixture title record is valid");
    jazz::binding_codec::encode_named_cells(&OwnedRecord::new(raw, descriptor))
        .expect("fixture cells encode")
}

fn todos_query() -> Vec<u8> {
    postcard::to_allocvec(&Query::from("todos")).unwrap()
}

/// The `(row id, title)` pairs of a foreground row payload.
fn titles(rows: &[u8]) -> Vec<(RowUuid, String)> {
    decode_rows(rows)
        .expect("foreground rows use the shared binding row codec")
        .into_iter()
        .flat_map(|batch| {
            assert_eq!(batch.table, "todos");
            batch.rows
        })
        .map(|row| {
            let title = match row.cells.get("title") {
                Some(Value::String(title)) => title.clone(),
                Some(Value::Nullable(Some(value))) => match value.as_ref() {
                    Value::String(title) => title.clone(),
                    other => panic!("title cell: {other:?}"),
                },
                other => panic!("title cell: {other:?}"),
            };
            (row.row_id, title)
        })
        .collect()
}

fn take_capability(output: &mut JazzNativeRelayBytes) -> [u8; 32] {
    let bytes = unsafe { std::slice::from_raw_parts(output.data, output.len) };
    assert_eq!(bytes.len(), 32, "admission returns one opaque capability");
    let mut capability = [0; 32];
    capability.copy_from_slice(bytes);
    unsafe { jazz_native_relay_bytes_free(output) };
    capability
}

/// One platform host and its retained runtime lease.
struct Fixture {
    accounts: RefCell<BTreeMap<String, jazz::account_registry::AccountId>>,
    host: *mut JazzNativeRelayHost,
    lease: *mut JazzNativeRelayHostLease,
}

impl Fixture {
    fn new() -> Self {
        let host = jazz_native_relay_host_new();
        assert!(!host.is_null());
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        assert!(!lease.is_null());
        Self {
            accounts: RefCell::new(BTreeMap::new()),
            host,
            lease,
        }
    }

    fn admit(&self, sqlite_path: &std::path::Path, auth_scope: &str, byte: u8) -> [u8; 32] {
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([byte; 16]),
            author: AuthorSubject::for_test_bytes([byte.wrapping_add(1); 16]),
        };
        let request = serde_json::json!({
            "scope": {
                "app_namespace": "native-relay-test",
                "storage_namespace": "default",
                "auth_scope": auth_scope,
            },
            "sqlite_path": sqlite_path.display().to_string(),
            "schema_json": serde_json::to_string(schema().public_schema()).unwrap(),
            "identity": serde_json::to_value(identity).unwrap(),
            "claims": {},
        });
        let request = serde_json::to_vec(&request).unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    self.host,
                    request.as_ptr(),
                    request.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        take_capability(&mut output)
    }

    /// The private account-session handoff Android and iOS use: the
    /// endpoint and bearer cross only the native boundary.
    async fn begin_account_session(
        &self,
        server_url: &str,
        app_id: &str,
        bearer: &str,
        storage_root: &std::path::Path,
    ) -> [u8; 32] {
        let registry = format!(
            "{}/apps/{app_id}/accounts",
            server_url.trim_end_matches('/')
        );
        let (issuer, subject) = jazz::tools::unverified_jwt_scope_subject(bearer).unwrap();
        let key = serde_json::to_string(&(&registry, &issuer, &subject)).unwrap();
        let retained = self.accounts.borrow().get(&key).copied();
        let account = match retained {
            Some(account) => account,
            None => {
                let response = reqwest::Client::new()
                    .post(format!("{registry}/register"))
                    .bearer_auth(bearer)
                    .send()
                    .await
                    .unwrap();
                assert!(response.status().is_success(), "{}", response.status());
                let response: serde_json::Value = response.json().await.unwrap();
                let account = serde_json::from_value(response["account"].clone()).unwrap();
                self.accounts.borrow_mut().insert(key, account);
                account
            }
        };
        let request = serde_json::json!({
            "server_url": server_url, "registry": registry,
            "app_id": app_id, "env": "test", "account_id": account,
            "issuer": issuer, "subject": subject, "jwt": bearer,
            "claims": {},
        });
        let root = storage_root.to_str().unwrap().as_bytes();
        let request = serde_json::to_vec(&request).unwrap();
        let mut setup = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_begin_account_session_json(
                    self.lease,
                    request.as_ptr(),
                    request.len(),
                    root.as_ptr(),
                    root.len(),
                    &mut setup,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let setup = take_capability(&mut setup);
        let schema = serde_json::to_string(schema().public_schema()).unwrap();
        let mut admitted = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_attach_account_schema_json(
                    self.lease,
                    setup.as_ptr(),
                    setup.len(),
                    schema.as_ptr(),
                    schema.len(),
                    &mut admitted,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        take_capability(&mut admitted)
    }

    fn revoke(&self, capability: &[u8; 32]) {
        assert_eq!(
            unsafe {
                jazz_native_relay_host_revoke_scope_capability(
                    self.host,
                    capability.as_ptr(),
                    capability.len(),
                )
            },
            JazzNativeRelayStatus::Ok
        );
    }

    fn try_open(&self, capability: &[u8; 32]) -> (JazzNativeRelayStatus, u64) {
        let mut foreground = 0;
        let status = unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                self.lease,
                capability.as_ptr(),
                capability.len(),
                &mut foreground,
            )
        };
        (status, foreground)
    }

    fn open(&self, capability: &[u8; 32]) -> u64 {
        let (status, foreground) = self.try_open(capability);
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        assert_ne!(foreground, 0);
        foreground
    }

    fn try_execute(
        &self,
        foreground: u64,
        command: &ForegroundDbCommandRequest,
    ) -> Result<ForegroundDbCommandResponse, JazzNativeRelayStatus> {
        let request = postcard::to_allocvec(command).unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        let status = unsafe {
            jazz_native_relay_host_lease_execute_foreground(
                self.lease,
                foreground,
                request.as_ptr(),
                request.len(),
                &mut output,
            )
        };
        if status != JazzNativeRelayStatus::Ok {
            assert!(output.data.is_null(), "a failed command returns no bytes");
            return Err(status);
        }
        let response =
            postcard::from_bytes(unsafe { std::slice::from_raw_parts(output.data, output.len) })
                .expect("one canonical foreground response");
        unsafe { jazz_native_relay_bytes_free(&mut output) };
        Ok(response)
    }

    fn execute(
        &self,
        foreground: u64,
        command: ForegroundDbCommandRequest,
    ) -> ForegroundDbCommandResponse {
        self.try_execute(foreground, &command)
            .unwrap_or_else(|status| panic!("{command:?} failed: {status:?}"))
    }

    fn tick_status(&self, foreground: u64) -> JazzNativeRelayStatus {
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(self.lease, foreground) }
    }

    fn tick(&self, foreground: u64) {
        assert_eq!(self.tick_status(foreground), JazzNativeRelayStatus::Ok);
    }

    fn close(&self, foreground: u64) {
        assert_eq!(
            self.execute(foreground, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
    }

    fn metadata_node(&self, foreground: u64) -> [u8; 16] {
        match self.execute(
            foreground,
            ForegroundDbCommandRequest::NativeSessionMetadata,
        ) {
            ForegroundDbCommandResponse::NativeSessionMetadata { node, .. } => node,
            response => panic!("metadata: {response:?}"),
        }
    }

    fn insert_todo(&self, foreground: u64, row_id: [u8; 16], title: &str) -> [u8; 16] {
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = self.execute(
            foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            },
        ) else {
            panic!("transaction opens");
        };
        assert_eq!(
            self.execute(
                foreground,
                ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "todos".to_owned(),
                    cells: title_cells(title),
                    row_id: Some(row_id),
                },
            ),
            ForegroundDbCommandResponse::Inserted { row_id }
        );
        let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = self.execute(
            foreground,
            ForegroundDbCommandRequest::CommitTransaction { transaction },
        ) else {
            panic!("transaction commits");
        };
        assert_ne!(tx_id, [0; 16]);
        tx_id
    }

    /// Run an operation to completion, ticking between polls.
    fn settle(
        &self,
        foreground: u64,
        command: ForegroundDbCommandRequest,
    ) -> ForegroundDbCommandResponse {
        let mut response = self.execute(foreground, command);
        for _ in 0..400 {
            let ForegroundDbCommandResponse::Pending { operation } = response else {
                return response;
            };
            self.tick(foreground);
            std::thread::sleep(Duration::from_millis(10));
            response = self.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
        }
        panic!("operation did not settle: {response:?}");
    }

    fn rows(&self, foreground: u64) -> Vec<(RowUuid, String)> {
        match self.settle(
            foreground,
            ForegroundDbCommandRequest::All {
                query: todos_query(),
                options_json: "{}".into(),
                transaction: None,
            },
        ) {
            ForegroundDbCommandResponse::Rows { rows } => titles(&rows),
            response => panic!("All: {response:?}"),
        }
    }

    /// Read until `row` is visible: a sibling's write reaches this
    /// foreground through the relay, not synchronously.
    fn wait_for_row(&self, foreground: u64, row: (RowUuid, &str)) {
        for _ in 0..200 {
            let rows = self.rows(foreground);
            if rows
                .iter()
                .any(|(id, title)| *id == row.0 && title == row.1)
            {
                return;
            }
            self.tick(foreground);
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("row {row:?} never reached the foreground");
    }

    async fn wait_for_core_transaction(&self, foreground: u64, tx_id: [u8; 16]) {
        let mut response = self.execute(
            foreground,
            ForegroundDbCommandRequest::WaitForCoreTransaction { tx_id },
        );
        for _ in 0..600 {
            match response {
                ForegroundDbCommandResponse::Pending { operation } => {
                    self.tick(foreground);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    response =
                        self.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
                }
                ForegroundDbCommandResponse::TransactionSettled { tx_id: settled } => {
                    assert_eq!(settled, tx_id);
                    return;
                }
                response => panic!("Core settlement: {response:?}"),
            }
        }
        panic!("the foreground transaction never reached Core");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        unsafe { jazz_native_relay_host_lease_free(self.lease) };
        unsafe { jazz_native_relay_host_free(self.host) };
    }
}

#[test]
fn abi_handshake_accepts_supported_and_rejects_incompatible_ranges() {
    assert_eq!(
        ensure_native_relay_abi_compatible(NativeRelayAbiRange {
            minimum: 0,
            maximum: NATIVE_RELAY_ABI_V1,
        })
        .unwrap(),
        NATIVE_RELAY_ABI_V1
    );
    assert!(matches!(
        ensure_native_relay_abi_compatible(NativeRelayAbiRange {
            minimum: 2,
            maximum: 1,
        }),
        Err(RelayError::InvalidAbiRange { .. })
    ));
    assert!(matches!(
        ensure_native_relay_abi_compatible(NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1 + 1,
            maximum: u16::MAX,
        }),
        Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_V1
    ));
}

#[test]
fn incompatible_open_creates_no_relay_or_sqlite_store() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("must-not-exist.sqlite");
    let registry = NativeRelayRegistry::default();
    let config = RelayOpenConfig {
        supported_abi: NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1 + 1,
            maximum: u16::MAX,
        },
        scope: RelayScope {
            app_namespace: "native-relay-test".to_owned(),
            storage_namespace: "default".to_owned(),
            auth_scope: Some("alice".to_owned()),
        },
        sqlite_path: sqlite_path.clone(),
        schema: schema(),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: AuthorSubject::for_test_bytes([0xa2; 16]),
        },
    };
    assert!(matches!(
        registry.open_admitted(config),
        Err(RelayError::IncompatibleAbi { .. })
    ));
    assert!(!sqlite_path.exists(), "the ABI check runs before SQLite");
}

#[test]
fn sibling_foregrounds_share_one_sqlite_relay() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit(&directory.path().join("alice.sqlite"), "alice", 0x11);
    let a = fixture.open(&capability);
    let b = fixture.open(&capability);
    assert_ne!(
        fixture.metadata_node(a),
        fixture.metadata_node(b),
        "each foreground writes as its own node"
    );

    fixture.insert_todo(a, [0xa1; 16], "from a");
    fixture.wait_for_row(b, (RowUuid::from_bytes([0xa1; 16]), "from a"));
    fixture.insert_todo(b, [0xb1; 16], "from b");
    fixture.wait_for_row(a, (RowUuid::from_bytes([0xb1; 16]), "from b"));

    fixture.close(a);
    assert_eq!(
        fixture.tick_status(a),
        JazzNativeRelayStatus::InvalidHandle,
        "a closed foreground is gone"
    );
    fixture.tick(b);
    fixture.close(b);
}

#[test]
fn process_reopen_rehydrates_the_persisted_row() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let row = RowUuid::from_bytes([0x41; 16]);
    {
        let first = Fixture::new();
        let capability = first.admit(&path, "alice", 0x41);
        let foreground = first.open(&capability);
        first.insert_todo(foreground, [0x41; 16], "survives restart");
        // The commit lands on the relay's SQLite before the foreground
        // reports it settled locally.
        let other = first.open(&capability);
        first.wait_for_row(other, (row, "survives restart"));
        first.close(foreground);
        first.close(other);
    }
    let second = Fixture::new();
    let capability = second.admit(&path, "alice", 0x41);
    let foreground = second.open(&capability);
    second.wait_for_row(foreground, (row, "survives restart"));
}

#[test]
fn scopes_are_isolated_and_revocation_retires_only_its_foregrounds() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let a_capability = fixture.admit(&directory.path().join("a.sqlite"), "scope-a", 0x51);
    let b_capability = fixture.admit(&directory.path().join("b.sqlite"), "scope-b", 0x61);
    assert_eq!(
        fixture.try_open(&[0xa5; 32]),
        (JazzNativeRelayStatus::InvalidHandle, 0),
        "an unadmitted capability opens nothing"
    );
    let a = fixture.open(&a_capability);
    let b = fixture.open(&b_capability);
    fixture.insert_todo(a, [0xa1; 16], "scope-a-only");
    fixture.insert_todo(b, [0xb1; 16], "scope-b-only");
    assert_eq!(
        fixture.rows(a),
        vec![(RowUuid::from_bytes([0xa1; 16]), "scope-a-only".to_owned())]
    );
    assert_eq!(
        fixture.rows(b),
        vec![(RowUuid::from_bytes([0xb1; 16]), "scope-b-only".to_owned())]
    );

    fixture.revoke(&a_capability);
    assert_eq!(fixture.tick_status(a), JazzNativeRelayStatus::InvalidHandle);
    assert_eq!(
        fixture.try_execute(a, &ForegroundDbCommandRequest::Probe),
        Err(JazzNativeRelayStatus::InvalidHandle)
    );
    assert_eq!(
        fixture.try_open(&a_capability).0,
        JazzNativeRelayStatus::InvalidHandle
    );
    fixture.tick(b);
    assert_eq!(fixture.rows(b).len(), 1);
}

#[test]
fn a_relay_file_from_before_sqlite_is_refused_untouched() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE column_families (name TEXT PRIMARY KEY);
                 CREATE TABLE kv (cf INTEGER, key BLOB, value BLOB, PRIMARY KEY (cf, key));",
            )
            .unwrap();
    }
    let before = std::fs::read(&path).unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit(&path, "alice", 0x71);
    for attempt in ["first open", "second open"] {
        assert_eq!(
            fixture.try_open(&capability),
            (JazzNativeRelayStatus::UnsupportedStorageCodecs, 0),
            "{attempt}"
        );
    }
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "the file is not mutated"
    );
}

#[test]
fn every_foreground_open_draws_a_fresh_node() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit(&directory.path().join("fresh.sqlite"), "alice", 0xc1);
    let first = fixture.open(&capability);
    let node = fixture.metadata_node(first);
    fixture.insert_todo(first, [0xd1; 16], "first foreground");
    fixture.close(first);

    // Its counter starts over, so it names its transactions by a node of
    // its own: both writes land.
    let second = fixture.open(&capability);
    assert_ne!(fixture.metadata_node(second), node, "a fresh node");
    fixture.insert_todo(second, [0xd2; 16], "second foreground");
    fixture.wait_for_row(
        second,
        (RowUuid::from_bytes([0xd1; 16]), "first foreground"),
    );
    fixture.wait_for_row(
        second,
        (RowUuid::from_bytes([0xd2; 16]), "second foreground"),
    );
    fixture.close(second);
}

struct WakeRecorder {
    immediate: AtomicUsize,
}

unsafe extern "C" fn record_wake(context: *mut c_void, _foreground: u64, kind: u8, _delay: u64) {
    let recorder = unsafe { &*(context as *const WakeRecorder) };
    if kind == FOREGROUND_WAKE_IMMEDIATE {
        recorder.immediate.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn a_sibling_commit_wakes_and_reaches_a_retained_subscription() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit(&directory.path().join("wake.sqlite"), "alice", 0x21);
    let writer = fixture.open(&capability);
    let reader = fixture.open(&capability);
    let recorder = Box::new(WakeRecorder {
        immediate: AtomicUsize::new(0),
    });
    let context = &*recorder as *const WakeRecorder as *mut c_void;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_set_foreground_wake_callback(
                fixture.lease,
                reader,
                Some(record_wake),
                context,
            )
        },
        JazzNativeRelayStatus::Ok
    );
    let ForegroundDbCommandResponse::Subscribed { subscription } = fixture.execute(
        reader,
        ForegroundDbCommandRequest::Subscribe {
            query: todos_query(),
            options_json: "{}".into(),
        },
    ) else {
        panic!("subscription opens");
    };
    fixture.insert_todo(writer, [0xe1; 16], "seen by a subscription");

    let mut seen = false;
    for _ in 0..300 {
        let woken = recorder.immediate.swap(0, Ordering::AcqRel) > 0;
        if woken {
            fixture.tick(reader);
        }
        let ForegroundDbCommandResponse::SubscriptionEvents { events } = fixture.execute(
            reader,
            ForegroundDbCommandRequest::DrainSubscription { subscription },
        ) else {
            panic!("drain answers events");
        };
        for event in events {
            let (ForegroundSubscriptionEvent::Delta { delta, .. }
            | ForegroundSubscriptionEvent::StructuredDelta { delta, .. }) = event
            else {
                panic!("unexpected event: {event:?}");
            };
            let (added, updated) = decode_subscription_delta_rows(&delta).unwrap();
            seen |= added
                .into_iter()
                .chain(updated)
                .flat_map(|batch| batch.rows)
                .any(|row| row.row_id == RowUuid::from_bytes([0xe1; 16]));
        }
        if seen {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        seen,
        "the sibling's row reaches the subscription through wakes"
    );
    fixture.close(reader);
    fixture.close(writer);
    drop(recorder);
}

/// A real Core authenticates the private-session bearer; the foreground's
/// write reaches Core through the relay's socket worker, and a fresh worker
/// over the same SQLite file reads it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_session_write_reaches_core_and_survives_a_relay_restart() {
    let issuer = TestJwtIssuer::start().await;
    let core = JazzServer::builder()
        .with_schema(schema().public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .expect("start test server");
    let bearer = TestJwtIssuer::jwt_for_user("native-private-alice");
    let storage = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let endpoint = core.base_url();
    let app_id = core.app_id().to_string();
    let admitted = fixture
        .begin_account_session(&endpoint, &app_id, &bearer, storage.path())
        .await;
    let foreground = fixture.open(&admitted);
    jazz_testkit::wait_for(Duration::from_secs(15), "relay socket at Core", || {
        let connected = core.server_state().shutdown.active_websockets() > 0;
        async move { connected.then_some(()) }
    })
    .await;
    assert!(matches!(
        fixture.execute(
            foreground,
            ForegroundDbCommandRequest::NativeConnectionStatus
        ),
        ForegroundDbCommandResponse::NativeConnectionStatus {
            configured: true,
            ..
        }
    ));

    let row = RowUuid::from_bytes([0x3a; 16]);
    let committed = fixture.insert_todo(foreground, [0x3a; 16], "reaches Core");
    fixture
        .wait_for_core_transaction(foreground, committed)
        .await;

    fixture.close(foreground);
    fixture.revoke(&admitted);
    assert_eq!(
        core.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );

    let reopened = fixture
        .begin_account_session(&endpoint, &app_id, &bearer, storage.path())
        .await;
    let reopened_foreground = fixture.open(&reopened);
    fixture.wait_for_row(reopened_foreground, (row, "reaches Core"));
    fixture.revoke(&reopened);
}

/// On a replica a serializable snapshot is pinned by the transaction's
/// first read; later writes outside it stay invisible to it.
#[test]
fn a_serializable_transaction_keeps_its_pinned_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit(&directory.path().join("snapshot.sqlite"), "alice", 0x31);
    let foreground = fixture.open(&capability);
    fixture.insert_todo(foreground, [0x01; 16], "before");
    assert_eq!(fixture.rows(foreground).len(), 1);
    let ForegroundDbCommandResponse::TransactionOpened { transaction } = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        },
    ) else {
        panic!("transaction opens");
    };
    let read = || match fixture.settle(
        foreground,
        ForegroundDbCommandRequest::All {
            query: todos_query(),
            options_json: "{}".into(),
            transaction: Some(transaction),
        },
    ) {
        ForegroundDbCommandResponse::Rows { rows } => titles(&rows),
        response => panic!("transaction read: {response:?}"),
    };
    let before = vec![(RowUuid::from_bytes([0x01; 16]), "before".to_owned())];
    assert_eq!(read(), before);
    fixture.insert_todo(foreground, [0x02; 16], "after");
    assert_eq!(fixture.rows(foreground).len(), 2);
    assert_eq!(read(), before, "the pinned snapshot does not advance");
}

#[test]
fn pending_upload_count_has_pinned_bytes_and_answers() {
    // Appended command and answer: existing ordinals stay fixed.
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::PendingUploadCount).unwrap(),
        vec![38]
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::PendingUploadCount { count: 300 })
            .unwrap(),
        vec![29, 172, 2]
    );

    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit(&directory.path().join("alice.sqlite"), "alice", 0x11);
    let foreground = fixture.open(&capability);
    // Without a server nothing is ever uploaded.
    fixture.insert_todo(foreground, [0xa1; 16], "local only");
    fixture.tick(foreground);
    assert_eq!(
        fixture.execute(foreground, ForegroundDbCommandRequest::PendingUploadCount),
        ForegroundDbCommandResponse::PendingUploadCount { count: 0 }
    );
    fixture.close(foreground);
}

/// Writes made while the app is offline count as pending until Core
/// accepts them after the reconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_upload_count_follows_writes_to_core() {
    let issuer = TestJwtIssuer::start().await;
    let core = JazzServer::builder()
        .with_schema(schema().public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .expect("start test server");
    let bearer = TestJwtIssuer::jwt_for_user("native-pending-alice");
    let storage = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let admitted = fixture
        .begin_account_session(
            &core.base_url(),
            &core.app_id().to_string(),
            &bearer,
            storage.path(),
        )
        .await;
    let foreground = fixture.open(&admitted);
    let count = || match fixture.execute(foreground, ForegroundDbCommandRequest::PendingUploadCount)
    {
        ForegroundDbCommandResponse::PendingUploadCount { count } => count,
        response => panic!("pending upload count: {response:?}"),
    };
    assert_eq!(count(), 0);

    fixture.execute(
        foreground,
        ForegroundDbCommandRequest::DisconnectNativeUpstream,
    );
    fixture.insert_todo(foreground, [0xb1; 16], "offline one");
    fixture.insert_todo(foreground, [0xb2; 16], "offline two");
    fixture.tick(foreground);
    assert_eq!(count(), 2, "both commits wait for the server");

    fixture.execute(
        foreground,
        ForegroundDbCommandRequest::ReconnectNativeUpstream,
    );
    jazz_testkit::wait_for(Duration::from_secs(15), "Core accepts both", || {
        fixture.tick(foreground);
        let uploaded = count() == 0;
        async move { uploaded.then_some(()) }
    })
    .await;

    fixture.close(foreground);
    fixture.revoke(&admitted);
    core.shutdown().await;
}

#[test]
fn slow_query_commands_have_pinned_bytes_and_answer() {
    // Appended commands and answers: existing ordinals stay fixed.
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::TakeSlowQueryReport { app: None })
            .unwrap(),
        vec![36, 0]
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::TakeSlowQueryReport {
            app: Some("a".to_owned())
        })
        .unwrap(),
        vec![36, 1, 1, b'a']
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::SetSlowQueryThresholdMs { ms: 200 })
            .unwrap(),
        vec![37, 200, 1]
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::SlowQueryReport {
            lines_json: "[]".to_owned()
        })
        .unwrap(),
        vec![27, 2, b'[', b']']
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::SlowQueryThresholdSet).unwrap(),
        vec![28]
    );

    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit(&directory.path().join("alice.sqlite"), "alice", 0x11);
    let foreground = fixture.open(&capability);
    assert_eq!(
        fixture.execute(
            foreground,
            ForegroundDbCommandRequest::SetSlowQueryThresholdMs { ms: 3_600_000 }
        ),
        ForegroundDbCommandResponse::SlowQueryThresholdSet
    );
    fixture.insert_todo(foreground, [0xa1; 16], "quick");
    fixture.tick(foreground);
    assert_eq!(
        fixture.execute(
            foreground,
            ForegroundDbCommandRequest::TakeSlowQueryReport {
                app: Some("my-app".to_owned())
            }
        ),
        ForegroundDbCommandResponse::SlowQueryReport {
            lines_json: "[]".to_owned()
        },
        "nothing ran for an hour"
    );
    fixture.close(foreground);
}

/// A relay uploads its clients' writes as its own identity, so it serves
/// clients of that identity only: one of another identity is refused
/// before its database is opened.
#[test]
fn a_relay_refuses_a_client_of_another_identity() {
    let directory = tempfile::tempdir().unwrap();
    let identity = |byte: u8| DbIdentity {
        node: NodeUuid::from_bytes([byte; 16]),
        author: AuthorSubject::for_test_bytes([0x5a; 16]),
    };
    let relay = NativeRelay::spawn(RelayOpenConfig {
        supported_abi: NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1,
            maximum: NATIVE_RELAY_ABI_V1,
        },
        scope: RelayScope {
            app_namespace: "app".to_owned(),
            storage_namespace: "default".to_owned(),
            auth_scope: Some("alice".to_owned()),
        },
        sqlite_path: directory.path().join("alice.sqlite"),
        schema: schema(),
        identity: identity(0x01),
    })
    .unwrap();
    let other = DbIdentity {
        author: AuthorSubject::for_test_bytes([0x5b; 16]),
        ..identity(0x02)
    };
    match relay.attach_client(other, BTreeMap::new()) {
        Err(RelayError::Host(binding_host::HostError::Engine(error))) => {
            assert_eq!(error.reason, Some(jazz::db::Reason::IdentityMismatch));
        }
        Err(error) => panic!("expected an identity mismatch, got {error}"),
        Ok(_) => panic!("the relay served a client of another identity"),
    }
    relay
        .attach_client(identity(0x03), BTreeMap::new())
        .expect("the relay serves its own identity");
}
