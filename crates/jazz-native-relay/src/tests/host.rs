//! Native host receipts through the same C ABI that Kotlin, Swift and the
//! React Native bridge use. Row behaviour itself belongs to the Db suites:
//! these prove the relay topology (one SQLite owner per scope, in-memory
//! foregrounds, the upstream socket) and its lifecycle.
use super::*;
use jazz::binding_codec::{decode_rows, decode_subscription_delta_rows};
use jazz::db::{Db, DbConfig, Migrations, block_on};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::Query;
use jazz::records::{OwnedRecord, RecordDescriptor, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
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
    host: *mut JazzNativeRelayHost,
    lease: *mut JazzNativeRelayHostLease,
}

impl Fixture {
    fn new() -> Self {
        let host = jazz_native_relay_host_new();
        assert!(!host.is_null());
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        assert!(!lease.is_null());
        Self { host, lease }
    }

    fn admit(&self, sqlite_path: &std::path::Path, auth_scope: &str, byte: u8) -> [u8; 32] {
        self.admit_app(sqlite_path, auth_scope, byte, &schema(), None)
    }

    /// The author a scope admitted with `byte` writes and syncs as.
    fn author(byte: u8) -> AuthorSubject {
        AuthorSubject::for_test_bytes([byte.wrapping_add(1); 16])
    }

    /// Admit a scope of the app with `schema` that ships `migrations`
    /// (`ClientMigrations` JSON), attached beside its tables.
    fn admit_app(
        &self,
        sqlite_path: &std::path::Path,
        auth_scope: &str,
        byte: u8,
        schema: &JazzSchema,
        migrations: Option<&serde_json::Value>,
    ) -> [u8; 32] {
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([byte; 16]),
            author: Self::author(byte),
        };
        let request = serde_json::json!({
            "scope": {
                "app_namespace": "native-relay-test",
                "storage_namespace": "default",
                "auth_scope": auth_scope,
            },
            "sqlite_path": sqlite_path.display().to_string(),
            "schema_json": app_json(schema, migrations),
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

    /// Run one relay lifecycle command through the host's C ABI.
    fn relay_command(&self, command: RelayCommandRequest) -> RelayCommandResponse {
        self.try_relay_command(&command)
            .unwrap_or_else(|status| panic!("{command:?} failed: {status:?}"))
    }

    fn try_relay_command(
        &self,
        command: &RelayCommandRequest,
    ) -> Result<RelayCommandResponse, JazzNativeRelayStatus> {
        let request = postcard::to_allocvec(command).unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        let status = unsafe {
            jazz_native_relay_host_execute(self.host, request.as_ptr(), request.len(), &mut output)
        };
        if status != JazzNativeRelayStatus::Ok {
            return Err(status);
        }
        let response =
            postcard::from_bytes(unsafe { std::slice::from_raw_parts(output.data, output.len) })
                .expect("one canonical relay response");
        unsafe { jazz_native_relay_bytes_free(&mut output) };
        Ok(response)
    }

    /// Open the scope's relay as the host that carries its upstream frames.
    fn open_relay(&self, capability: &[u8; 32]) -> u64 {
        match self.relay_command(RelayCommandRequest::Open {
            supported_abi_minimum: NATIVE_RELAY_ABI_V1,
            supported_abi_maximum: NATIVE_RELAY_ABI_V1,
            admitted_scope: AdmissionCapability(*capability),
        }) {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("Open: {response:?}"),
        }
    }

    /// Carry frames both ways between `relay` and `server`'s link to it, and
    /// tick both.
    fn carry(&self, relay: u64, server: &jazz::binding_host::Link) {
        let RelayCommandResponse::Frames { frames } =
            self.relay_command(RelayCommandRequest::ReceiveRelayFrames { relay })
        else {
            panic!("relay frames");
        };
        server.receive(frames);
        block_on(server.binding().tick()).unwrap();
        for frame in server.take_outbound() {
            self.relay_command(RelayCommandRequest::SendRelayFrame { relay, frame });
        }
        self.relay_command(RelayCommandRequest::Pump { relay });
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
        migrations: Default::default(),
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
fn a_clean_handoff_reuses_the_node_above_its_high_water() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit(&directory.path().join("handoff.sqlite"), "alice", 0xc1);
    let first = fixture.open(&capability);
    let node = fixture.metadata_node(first);
    assert_eq!(
        (node[6] >> 4, node[8] >> 6),
        (4, 0b10),
        "the pool mints random version-4 nodes"
    );
    let first_tx = fixture.insert_todo(first, [0xd1; 16], "first lease holder");
    fixture.close(first);

    let second = fixture.open(&capability);
    assert_eq!(
        fixture.metadata_node(second),
        node,
        "the clean lease is reused"
    );
    let second_tx = fixture.insert_todo(second, [0xd2; 16], "second lease holder");
    assert_ne!(
        first_tx, second_tx,
        "the reused node mints fresh transactions"
    );
    fixture.wait_for_row(
        second,
        (RowUuid::from_bytes([0xd1; 16]), "first lease holder"),
    );
    fixture.wait_for_row(
        second,
        (RowUuid::from_bytes([0xd2; 16]), "second lease holder"),
    );

    // A foreground whose runtime disappears without a clean close has an
    // uncertain high water: its node is retired, never reissued.
    let runtime = unsafe { jazz_native_relay_host_retain(fixture.host, 2) };
    let mut held = 0;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                runtime,
                capability.as_ptr(),
                capability.len(),
                &mut held,
            )
        },
        JazzNativeRelayStatus::Ok
    );
    fixture.close(second);
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_invalidate_foreground_runtime(runtime) },
        JazzNativeRelayStatus::Ok
    );
    unsafe { jazz_native_relay_host_lease_free(runtime) };
    let third = fixture.open(&capability);
    assert_eq!(
        fixture.metadata_node(third),
        node,
        "the clean node is reused"
    );
    let fourth = fixture.open(&capability);
    assert_ne!(fixture.metadata_node(fourth), node);
    fixture.close(third);
    fixture.close(fourth);
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
    // A relay with no server decides nothing: the commit stays pending
    // until a server is added and accepts it.
    fixture.insert_todo(foreground, [0xa1; 16], "local only");
    fixture.tick(foreground);
    assert_eq!(
        fixture.execute(foreground, ForegroundDbCommandRequest::PendingUploadCount),
        ForegroundDbCommandResponse::PendingUploadCount { count: 1 }
    );
    fixture.close(foreground);
}

#[test]
fn upload_status_has_pinned_bytes_and_answers() {
    // Appended command and answer: existing ordinals stay fixed.
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::UploadStatus).unwrap(),
        vec![39]
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::UploadStatus {
            status_json: "{}".to_owned()
        })
        .unwrap(),
        vec![30, 2, b'{', b'}']
    );

    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit(&directory.path().join("alice.sqlite"), "alice", 0x11);
    let foreground = fixture.open(&capability);
    fixture.insert_todo(foreground, [0xa1; 16], "local only");
    fixture.tick(foreground);
    let ForegroundDbCommandResponse::UploadStatus { status_json } =
        fixture.execute(foreground, ForegroundDbCommandRequest::UploadStatus)
    else {
        panic!("an upload status");
    };
    let status: serde_json::Value = serde_json::from_str(&status_json).unwrap();
    assert_eq!(status["pending"], 1);
    assert!(status["oldestPendingAt"].as_u64().is_some_and(|at| at > 0));
    assert_eq!(status["rejected"], 0);
    assert!(status["lastRejection"].is_null());
    fixture.close(foreground);
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
        migrations: Default::default(),
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

/// A server of the app in `bundle`, at its head through the lens, holding
/// one row "from the server". Returns it and the head schema.
fn server_with_history(bundle: &serde_json::Value) -> (jazz::binding_host::Binding, JazzSchema) {
    let (schemas, lenses) =
        jazz::tools::deployment::published::client_migrations(bundle.to_string().as_bytes())
            .unwrap();
    let head = JazzSchema::new(&todos_with("title")).unwrap();
    let server_identity = DbIdentity {
        node: NodeUuid::from_bytes([0xee; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let server_db = block_on(Db::open_history_complete(
        DbConfig::new(head.clone(), server_identity).with_migrations(schemas, lenses),
    ))
    .unwrap();
    server_db.set_authority();
    let server = jazz::binding_host::Binding::new(server_db, server_identity, true);
    server
        .insert(
            "todos",
            &title_cells("from the server"),
            &serde_json::Value::Null,
        )
        .unwrap();
    (server, head)
}

/// The schema JSON a session attaches: `schema`'s tables, and beside them
/// the `migrations` the app ships, if any.
fn app_json(schema: &JazzSchema, migrations: Option<&serde_json::Value>) -> String {
    let mut app = serde_json::to_value(schema.public_schema()).unwrap();
    if let Some(bundle) = migrations {
        app["migrations"] = bundle.clone();
    }
    app.to_string()
}

/// `todos` with one text column named `column`, readable and writable by
/// anyone.
fn todos_with(column: &str) -> jazz::tools::Schema {
    let allow = PolicyExpr::True;
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("todos")
                .column(column, ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(allow.clone())
                        .with_insert(allow.clone())
                        .with_update(Some(allow.clone()), allow.clone())
                        .with_delete(allow),
                ),
        )
        .build()
}

/// The `ClientMigrations` bundle of an app that started at `todos(label)`
/// and renamed `label` to `title`, as `jazz-tools` generates it.
fn renamed_label_bundle() -> serde_json::Value {
    let (v1, v2) = (todos_with("label"), todos_with("title"));
    let hash = |schema: &jazz::tools::Schema| jazz::tools::SchemaHash::compute(schema).to_string();
    serde_json::json!({
        "schemas": [
            {"hash": hash(&v1), "schema": v1},
            {"hash": hash(&v2), "schema": v2},
        ],
        "migrations": [{
            "fromHash": hash(&v1),
            "toHash": hash(&v2),
            "forward": [{
                "table": "todos",
                "operations": [{"type": "rename", "column": "label", "value": "title"}],
            }],
        }],
    })
}

/// A relay opens its store, and each foreground's, along the app's schema
/// history, as every other client does, so a foreground reads the rows of a
/// server whose app has more than one version (#4777). Opened from the head
/// schema alone, the relay named each column after the head while the server
/// names it after the version that introduced it, and the foreground read
/// nothing.
///
/// ```text
/// server (v1 + lens → v2) ──frames──► alice's relay (v2 + bundle) ──► foreground
///   seeded "from the server"                                     reads it
/// ```
#[test]
fn a_foreground_reads_a_server_with_schema_history() {
    let bundle = renamed_label_bundle();
    let (server, head) = server_with_history(&bundle);

    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit_app(
        &directory.path().join("alice.sqlite"),
        "alice",
        0x47,
        &head,
        Some(&bundle),
    );
    let relay = fixture.open_relay(&capability);
    let upstream = server
        .accept_subscriber(Fixture::author(0x47), Default::default())
        .unwrap();
    let foreground = fixture.open(&capability);

    for _ in 0..200 {
        let mut response = fixture.execute(
            foreground,
            ForegroundDbCommandRequest::All {
                query: todos_query(),
                options_json: r#"{"tier":"remote"}"#.into(),
                transaction: None,
            },
        );
        while let ForegroundDbCommandResponse::Pending { operation } = response {
            fixture.carry(relay, &upstream);
            fixture.tick(foreground);
            response = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
        }
        let ForegroundDbCommandResponse::Rows { rows } = response else {
            panic!("All: {response:?}");
        };
        let titles = titles(&rows);
        if !titles.is_empty() {
            assert_eq!(
                titles
                    .into_iter()
                    .map(|(_, title)| title)
                    .collect::<Vec<_>>(),
                ["from the server"]
            );
            fixture.close(foreground);
            return;
        }
        fixture.carry(relay, &upstream);
        fixture.tick(foreground);
    }
    panic!("the server's row never reached the foreground");
}

/// A scope's schema history is part of its configuration: admitting the
/// same scope again with another history fails, as another schema does.
#[test]
fn a_scope_readmitted_with_another_history_is_refused() {
    let head = JazzSchema::new(&todos_with("title")).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let fixture = Fixture::new();
    fixture.admit_app(&path, "alice", 0x48, &head, Some(&renamed_label_bundle()));
    let request = serde_json::json!({
        "scope": {
            "app_namespace": "native-relay-test",
            "storage_namespace": "default",
            "auth_scope": "alice",
        },
        "sqlite_path": path.display().to_string(),
        "schema_json": app_json(&head, None),
        "identity": serde_json::to_value(DbIdentity {
            node: NodeUuid::from_bytes([0x48; 16]),
            author: Fixture::author(0x48),
        })
        .unwrap(),
        "claims": {},
    })
    .to_string();
    let mut output = JazzNativeRelayBytes::EMPTY;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_admit_scope_json(
                fixture.host,
                request.as_ptr(),
                request.len(),
                &mut output,
            )
        },
        JazzNativeRelayStatus::LifecycleFailure
    );
    assert!(output.data.is_null());
}

/// A relay attached without the app's migrations stores the head's columns
/// under other names than its server, so its database refuses the server's
/// catalogue. The relay then stops that connection, rather than keeping a
/// link open that never syncs, as an alpha.60 relay did (#4777's symptom):
/// the host can no longer carry frames over it.
///
/// ```text
/// server (v1 + lens → v2) ──catalogue──► relay (v2 alone)
///                                          ✗ refuses: its names differ
///                         ✗──────────────  connection retired
/// ```
#[test]
fn a_relay_attached_without_the_apps_migrations_stops_its_refused_upstream() {
    let (server, head) = server_with_history(&renamed_label_bundle());
    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let capability = fixture.admit_app(
        &directory.path().join("alice.sqlite"),
        "alice",
        0x49,
        &head,
        None,
    );
    let relay = fixture.open_relay(&capability);
    let upstream = server
        .accept_subscriber(Fixture::author(0x49), Default::default())
        .unwrap();
    let foreground = fixture.open(&capability);
    fixture.tick(foreground);
    for _ in 0..50 {
        match fixture.try_relay_command(&RelayCommandRequest::ReceiveRelayFrames { relay }) {
            Ok(RelayCommandResponse::Frames { frames }) => {
                upstream.receive(frames);
                block_on(upstream.binding().tick()).unwrap();
                for frame in upstream.take_outbound() {
                    let _ = fixture
                        .try_relay_command(&RelayCommandRequest::SendRelayFrame { relay, frame });
                }
                fixture.relay_command(RelayCommandRequest::Pump { relay });
                fixture.tick(foreground);
            }
            Ok(response) => panic!("ReceiveRelayFrames: {response:?}"),
            Err(_) => {
                fixture.close(foreground);
                return;
            }
        }
    }
    panic!("the relay kept carrying a connection its database refused");
}

/// #5145: a store an alpha.60 relay created without the app's migrations,
/// holding no unsynced writes, is rebuilt by a relay that now has them. Its
/// server refuses it by name (`store_opened_without_migrations`); the relay
/// reopens a fresh store with the migrations and reconnects, so alice's
/// foreground reads the server's rows again without the app doing anything.
///
/// Through the Rust relay handle rather than the C ABI: the test carries the
/// upstream itself, and must serve the rebuilt store's new connection with a
/// fresh server session, as a socket's reconnect would.
///
/// ```text
/// alpha.60 relay (v2 alone) ──creates──► alice.sqlite (head-only names)
/// relay (v2 + bundle) ──opens alice.sqlite──► server refuses by name
///                     ──rebuilds fresh──► reconnects ──► foreground reads
/// ```
#[test]
fn a_store_opened_without_migrations_is_rebuilt_when_nothing_is_pending() {
    let bundle = renamed_label_bundle();
    let (server, head) = server_with_history(&bundle);
    let (schemas, lenses) =
        jazz::tools::deployment::published::client_migrations(bundle.to_string().as_bytes())
            .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let author = AuthorSubject::for_test_bytes([0x4b; 16]);
    let identity = |byte: u8| DbIdentity {
        node: NodeUuid::from_bytes([byte; 16]),
        author,
    };
    let open = |migrations: Migrations| {
        NativeRelay::spawn(RelayOpenConfig {
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
            schema: head.clone(),
            migrations,
            identity: identity(0x01),
        })
        .unwrap()
    };
    // As alpha.60 did: the head schema alone.
    let older = open(Migrations::default());
    older
        .attach_client(identity(0x02), BTreeMap::new())
        .unwrap();
    older.pump().unwrap();
    drop(older);

    let relay = open(Migrations { schemas, lenses });
    let alice = relay
        .attach_client(identity(0x03), BTreeMap::new())
        .unwrap();
    let mut generation = None;
    let mut upstream = None;
    let mut connections = 0;
    for _ in 0..300 {
        let wire = relay.wire().unwrap();
        let current = relay
            .run(|worker| Ok(worker.upstream_generation()))
            .unwrap();
        if generation != current {
            // A new connection: the server serves it afresh, as on a socket.
            generation = current;
            connections += 1;
            upstream = Some(
                server
                    .accept_subscriber(author, Default::default())
                    .unwrap(),
            );
        }
        let server_link = upstream.as_ref().unwrap();
        if let Ok(frames) = wire.take_outbound_encoded() {
            server_link.receive(frames);
        }
        block_on(server.tick()).unwrap();
        for frame in server_link.take_outbound() {
            let _ = wire.push_inbound_encoded(&frame);
        }
        relay.pump().unwrap();
        let response = alice
            .execute(ForegroundDbCommandRequest::All {
                query: todos_query(),
                options_json: "{}".into(),
                transaction: None,
            })
            .unwrap();
        let response = match response {
            ForegroundDbCommandResponse::Pending { operation } => alice
                .execute(ForegroundDbCommandRequest::Poll { operation })
                .unwrap(),
            response => response,
        };
        if let ForegroundDbCommandResponse::Rows { rows } = response
            && titles(&rows)
                .iter()
                .any(|(_, title)| title == "from the server")
        {
            assert_eq!(
                connections, 2,
                "refused once, then served by the rebuilt store"
            );
            assert!(
                directory.path().join("alice.sqlite.rebuilt-for").exists(),
                "the store remembers the version it was rebuilt for"
            );
            return;
        }
    }
    panic!("the rebuilt store never served the server's row");
}

/// #5145: a store opened without the app's migrations that holds a write
/// the server never confirmed is not rebuilt, which would drop the write.
/// The refusal stays reported by name, so the app can choose to reset it
/// (`deleteClientStorage()`), and the write is still there meanwhile.
#[test]
fn a_store_opened_without_migrations_with_unsynced_writes_is_left_for_the_app() {
    let bundle = renamed_label_bundle();
    let (server, head) = server_with_history(&bundle);
    let (schemas, lenses) =
        jazz::tools::deployment::published::client_migrations(bundle.to_string().as_bytes())
            .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let author = AuthorSubject::for_test_bytes([0x4c; 16]);
    let identity = |byte: u8| DbIdentity {
        node: NodeUuid::from_bytes([byte; 16]),
        author,
    };
    let open = |migrations: Migrations| {
        NativeRelay::spawn(RelayOpenConfig {
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
            schema: head.clone(),
            migrations,
            identity: identity(0x01),
        })
        .unwrap()
    };
    {
        // alpha.60: alice wrote while the relay could never sync it.
        let older = open(Migrations::default());
        let writer = older
            .attach_client(identity(0x02), BTreeMap::new())
            .unwrap();
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = writer
            .execute(ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            })
            .unwrap()
        else {
            panic!("transaction opens");
        };
        writer
            .execute(ForegroundDbCommandRequest::Insert {
                transaction,
                table: "todos".to_owned(),
                cells: title_cells("unsynced"),
                row_id: Some([0x4c; 16]),
            })
            .unwrap();
        writer
            .execute(ForegroundDbCommandRequest::CommitTransaction { transaction })
            .unwrap();
        for _ in 0..5 {
            older.pump().unwrap();
        }
    }

    let relay = open(Migrations { schemas, lenses });
    let _alice = relay
        .attach_client(identity(0x03), BTreeMap::new())
        .unwrap();
    let wire = relay.wire().unwrap();
    let first = relay
        .run(|worker| Ok(worker.upstream_generation()))
        .unwrap();
    let server_link = server
        .accept_subscriber(author, Default::default())
        .unwrap();
    for _ in 0..50 {
        if let Ok(frames) = wire.take_outbound_encoded() {
            server_link.receive(frames);
        }
        block_on(server.tick()).unwrap();
        for frame in server_link.take_outbound() {
            let _ = wire.push_inbound_encoded(&frame);
        }
        relay.pump().unwrap();
    }
    let refusal = relay
        .upstream_refusal()
        .unwrap()
        .expect("the refusal is reported, not acted on");
    assert_eq!(refusal.code, "store_opened_without_migrations");
    assert!(
        relay
            .run(|worker| Ok(worker.upstream_generation()))
            .unwrap()
            == first,
        "the store was not rebuilt"
    );
    let pending = relay
        .run(|worker| Ok(worker.persistent_pending_uploads()))
        .unwrap();
    assert_eq!(pending, 1, "alice's unsynced write is still there");
}

/// `deleteClientStorage()` on React Native (#5145): deleting a scope's
/// store closes its foregrounds and relay, removes its SQLite files and
/// reports the unconfirmed writes it dropped. The admission stays, so the
/// next foreground opens a fresh, empty store for the same account.
#[test]
fn deleting_a_scopes_store_drops_it_and_counts_its_unconfirmed_writes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let fixture = Fixture::new();
    let capability = fixture.admit(&path, "alice", 0x4d);
    let foreground = fixture.open(&capability);
    fixture.insert_todo(foreground, [0x4d; 16], "never synced");
    let sibling = fixture.open(&capability);
    fixture.wait_for_row(sibling, (RowUuid::from_bytes([0x4d; 16]), "never synced"));
    assert!(path.exists());

    assert_eq!(
        fixture.execute(foreground, ForegroundDbCommandRequest::DeleteStore),
        ForegroundDbCommandResponse::StoreDeleted { pending_uploads: 1 },
        "the one write no server confirmed is counted"
    );
    assert!(!path.exists(), "the store's file is gone");
    assert_eq!(
        fixture.tick_status(sibling),
        JazzNativeRelayStatus::InvalidHandle,
        "every foreground of the scope is closed"
    );

    let fresh = fixture.open(&capability);
    assert_eq!(
        fixture.rows(fresh),
        [],
        "the same account opens a fresh store"
    );
    fixture.close(fresh);
}

/// The delete command and its answer keep their ABI 6 ordinals.
#[test]
fn delete_store_command_and_response_have_pinned_bytes() {
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::DeleteStore).unwrap(),
        vec![40]
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::StoreDeleted {
            pending_uploads: 300
        })
        .unwrap(),
        vec![31, 0xac, 0x02]
    );
}

/// #5145 review: a relay whose bundle still disagrees with its server is
/// refused again after its rebuild. It rebuilds once for that schema
/// version, and never again, also in a later process (a marker beside the
/// store remembers it), so a device does not delete and re-sync its store
/// on every launch. The refusal is reported instead.
#[test]
fn a_store_is_rebuilt_once_per_schema_version() {
    let (server, head) = server_with_history(&renamed_label_bundle());
    let directory = tempfile::tempdir().unwrap();
    let author = AuthorSubject::for_test_bytes([0x4e; 16]);
    let identity = |byte: u8| DbIdentity {
        node: NodeUuid::from_bytes([byte; 16]),
        author,
    };
    // Without the app's migrations, as a bundle that disagrees with the
    // server would open it: every store it makes is refused.
    let open = || {
        NativeRelay::spawn(RelayOpenConfig {
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
            schema: head.clone(),
            migrations: Migrations::default(),
            identity: identity(0x01),
        })
        .unwrap()
    };
    // Carry the upstream until it stops changing; returns how many
    // connections the relay made.
    let carry = |relay: &NativeRelay| {
        let mut generation = None;
        let mut upstream = None;
        let mut connections = 0;
        for _ in 0..100 {
            let wire = relay.wire().unwrap();
            let current = relay
                .run(|worker| Ok(worker.upstream_generation()))
                .unwrap();
            if generation != current {
                generation = current;
                connections += 1;
                upstream = Some(
                    server
                        .accept_subscriber(author, Default::default())
                        .unwrap(),
                );
            }
            let link = upstream.as_ref().unwrap();
            if let Ok(frames) = wire.take_outbound_encoded() {
                link.receive(frames);
            }
            block_on(server.tick()).unwrap();
            for frame in link.take_outbound() {
                let _ = wire.push_inbound_encoded(&frame);
            }
            relay.pump().unwrap();
        }
        connections
    };

    let first = open();
    let _alice = first
        .attach_client(identity(0x02), BTreeMap::new())
        .unwrap();
    assert_eq!(carry(&first), 2, "refused, rebuilt once, refused again");
    assert!(first.upstream_refusal().unwrap().is_some());
    drop(first);

    let later = open();
    let _alice = later
        .attach_client(identity(0x03), BTreeMap::new())
        .unwrap();
    assert_eq!(carry(&later), 1, "a later launch does not rebuild again");
    assert_eq!(
        later
            .upstream_refusal()
            .unwrap()
            .map(|refusal| refusal.code)
            .as_deref(),
        Some("store_opened_without_migrations"),
        "the refusal is reported for the app"
    );
}
