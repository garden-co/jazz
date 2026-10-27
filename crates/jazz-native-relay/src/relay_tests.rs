#[path = "wire_test_fixture.rs"]
mod wire_test_fixture;
use wire_test_fixture::*;
#[path = "command_test_fixture.rs"]
mod command_test_fixture;
use command_test_fixture::*;
#[path = "tests/host.rs"]
mod host;
#[path = "tests/initialization.rs"]
mod initialization;
#[path = "tests/legacy_storage.rs"]
mod legacy_storage;
#[path = "tests/publication_codec.rs"]
mod publication_codec;
#[path = "tests/snapshot_capture_codec.rs"]
mod snapshot_capture_codec;

// This is intentionally an internal transport-ownership test: the public
// user-visible behavior of rows/subscriptions belongs to the Db suites.
// Here we prove the native host does not accidentally create one durable
// store per UI runtime or share it across explicit auth scopes.
use super::*;
use crate::*;
use jazz::binding_host::decode_cells as decode_foreground_cells;
use jazz::db::{
    DbConfig, InsertOptions, ReadOpts, SyncMessage, TickUrgency, Transport, WireTransportAdapter,
    block_on, decode_message as decode_sync_message, encode_message as encode_sync_message,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES;
use jazz::query::Query;
use jazz::records::{BorrowedRecord, RecordDescriptor, ValueType};
use jazz::time::TxTime;
use jazz::tools::TransactionId;
use jazz::tools::native_transport_connector::{
    NativeTransportError, NativeTransportRequest, NativeTransportTerminal,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier as CoreDurabilityTier;
use jazz::tx::TxId;
use jazz::wire::{TransportError, WireTransport};
use jazz_server::{JazzServer, TestJwtIssuer};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, mpsc};
use std::task::Poll;
use std::time::Duration;

struct IdleWire;

impl WireTransport for IdleWire {
    fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        None
    }
}

struct ReconnectingTestConnector {
    calls: AtomicUsize,
    bearer_seen: Arc<Mutex<Vec<String>>>,
}

impl NativeTransportConnector for ReconnectingTestConnector {
    fn connect(
        &self,
        request: NativeTransportRequest,
    ) -> jazz::tools::native_transport_connector::NativeTransportFuture {
        self.bearer_seen.lock().unwrap().push(
            request
                .auth
                .jwt_token
                .expect("worker supplies bearer only to connector"),
        );
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            Ok(
                jazz::tools::native_transport_connector::ConnectedNativeTransport {
                    transport: Box::new(IdleWire),
                    protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                    features: jazz::wire::current_wire_features(),
                    terminal: if call == 0 {
                        Box::pin(async {
                            jazz::tools::native_transport_connector::NativeTransportTerminal::PeerClosed(
                            "test close".to_owned(),
                        )
                        })
                    } else {
                        Box::pin(std::future::pending())
                    },
                },
            )
        })
    }
}

struct ClosedPumpWire;

impl WireTransport for ClosedPumpWire {
    fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
        // This is the private WebSocketTransport signal that its outbound
        // pump has already selected a terminal result.
        Err(TransportError::Failed(
            "websocket pump is closed".to_owned(),
        ))
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        None
    }
}

struct ClosedPumpTestConnector {
    calls: AtomicUsize,
    first_terminal: Arc<Mutex<Option<tokio::sync::oneshot::Sender<NativeTransportTerminal>>>>,
}

impl NativeTransportConnector for ClosedPumpTestConnector {
    fn connect(
        &self,
        _request: NativeTransportRequest,
    ) -> jazz::tools::native_transport_connector::NativeTransportFuture {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        let first_terminal = Arc::clone(&self.first_terminal);
        Box::pin(async move {
            let terminal: jazz::tools::native_transport_connector::NativeTransportTerminalFuture =
                if call == 0 {
                    let (sender, receiver) = tokio::sync::oneshot::channel();
                    *first_terminal.lock().unwrap() = Some(sender);
                    Box::pin(async move {
                        receiver
                            .await
                            .unwrap_or(NativeTransportTerminal::OwnerDropped)
                    })
                } else {
                    Box::pin(std::future::pending())
                };
            Ok(
                jazz::tools::native_transport_connector::ConnectedNativeTransport {
                    transport: if call == 0 {
                        Box::new(ClosedPumpWire)
                    } else {
                        Box::new(IdleWire)
                    },
                    protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                    features: jazz::wire::current_wire_features(),
                    terminal,
                },
            )
        })
    }
}

struct PendingTestConnector {
    calls: AtomicUsize,
}

impl NativeTransportConnector for PendingTestConnector {
    fn connect(
        &self,
        _request: NativeTransportRequest,
    ) -> jazz::tools::native_transport_connector::NativeTransportFuture {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async {
            Ok(
                jazz::tools::native_transport_connector::ConnectedNativeTransport {
                    transport: Box::new(IdleWire),
                    protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                    features: jazz::wire::current_wire_features(),
                    terminal: Box::pin(std::future::pending()),
                },
            )
        })
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
            .build(),
    )
    .unwrap()
}

fn permissive_schema() -> JazzSchema {
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

#[test]
fn canceled_private_session_cannot_attach_after_account_handoff() {
    // This tests the private pre-context capability boundary: there is no
    // public database yet through which cancellation could be observed.
    use base64::Engine;
    let root = tempfile::tempdir().unwrap();
    let mut host = NativeRelayHost::default();
    let setup = host
        .begin_private_session(PrivateSessionSetupJson {
            server_url: "https://edge.example".into(),
            app_id: "cancel-account-setup".into(),
            jwt: format!(
                "x.{}.x",
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(br#"{"iss":"https://issuer.example","sub":"alice"}"#)
            ),
            storage_root: root.path().display().to_string(),
        })
        .unwrap();
    assert!(host.revoke_scope(setup).unwrap());
    assert!(!host.revoke_scope(setup).unwrap(), "release is idempotent");
    assert_eq!(
        host.attach_canonical_schema(
            setup,
            &serde_json::to_string(schema().public_schema()).unwrap()
        ),
        Err(JazzNativeRelayStatus::InvalidHandle)
    );
    assert!(host.pending_private_sessions.is_empty());
    assert!(host.admitted_scopes.is_empty());
}

#[test]
fn private_session_setup_partitions_before_credential_free_schema_attachment() {
    use base64::Engine;
    let root = tempfile::tempdir().unwrap();
    let jwt = format!(
        "x.{}.x",
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"iss":"https://issuer.example","sub":"alice"}"#)
    );
    let mut host = NativeRelayHost::default();
    let setup = host
        .begin_private_session(PrivateSessionSetupJson {
            server_url: "HTTPS://EDGE.example:443/sync?ignored=yes".to_owned(),
            app_id: "app-a".to_owned(),
            jwt,
            storage_root: root.path().display().to_string(),
        })
        .unwrap();
    let pending = host.pending_private_sessions.get(&setup).unwrap();
    assert_eq!(pending.scope.app_namespace, "https://edge.example");
    assert_eq!(pending.scope.storage_namespace, "app-a");
    assert_eq!(
        pending.scope.auth_scope.as_deref(),
        Some(r#"["https://issuer.example","alice"]"#)
    );
    assert_eq!(
        host.attach_canonical_schema(setup, "not-json"),
        Err(JazzNativeRelayStatus::LifecycleFailure)
    );
    assert_eq!(
        host.attach_canonical_schema(setup, "{}"),
        Err(JazzNativeRelayStatus::InvalidHandle),
        "a malformed schema consumes the setup capability"
    );
    assert!(host.admitted_scopes.is_empty());
    let socket_setup = host
        .begin_private_session(PrivateSessionSetupJson {
            server_url: "https://edge.example".to_owned(),
            app_id: "app-a".to_owned(),
            jwt: format!(
                "x.{}.x",
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(br#"{"iss":"https://issuer.example","sub":"alice"}"#)
            ),
            storage_root: root.path().display().to_string(),
        })
        .unwrap();
    let admitted = host
        .attach_canonical_schema(
            socket_setup,
            &serde_json::to_string(schema().public_schema()).unwrap(),
        )
        .unwrap();
    assert!(host.admitted_scopes[&admitted].claims.is_empty());
    assert_eq!(
        host.private_socket_sessions[&admitted].bearer,
        "x.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlIiwic3ViIjoiYWxpY2UifQ.x"
    );
    assert!(host.revoke_scope(admitted).unwrap());
    assert!(!host.private_socket_sessions.contains_key(&admitted));
    assert!(validate_private_session_endpoint("http://edge.example").is_err());
    assert!(validate_private_session_endpoint("http://127.0.0.1:9876").is_ok());
    assert!(validate_private_session_endpoint("http://[::1]:9876").is_ok());
    assert!(validate_private_session_endpoint("http://10.0.2.2:9876").is_ok());
    assert!(validate_private_session_endpoint("https://edge.example").is_ok());
    assert!(
        host.begin_private_session(PrivateSessionSetupJson {
            server_url: "ftp://edge.example".to_owned(),
            app_id: "app-a".to_owned(),
            jwt: "x.e30.x".to_owned(),
            storage_root: root.path().display().to_string(),
        })
        .is_err()
    );
}

/// Two foreground roots sharing Alice's admitted scope must attach to one
/// durable relay and one bearer socket worker, rather than racing two
/// independent upstream bridges against the same SQLite store.
#[test]
fn private_scope_owns_one_socket_worker_across_foreground_leases() {
    use base64::Engine;
    let root = tempfile::tempdir().unwrap();
    let jwt = format!(
        "x.{}.x",
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"iss":"https://issuer.example","sub":"alice"}"#)
    );
    let mut host = NativeRelayHost::default();
    let pending = host
        .begin_private_session(PrivateSessionSetupJson {
            server_url: "http://127.0.0.1:9".to_owned(),
            app_id: "one-worker-per-scope".to_owned(),
            jwt,
            storage_root: root.path().display().to_string(),
        })
        .unwrap();
    let admitted = host
        .attach_canonical_schema(
            pending,
            &serde_json::to_string(schema().public_schema()).unwrap(),
        )
        .unwrap();
    let first = host
        .open_foreground(admitted, DIRECT_FOREGROUND_RUNTIME_TOKEN)
        .unwrap();
    let second = host
        .open_foreground(admitted, DIRECT_FOREGROUND_RUNTIME_TOKEN)
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(host.private_scope_workers.len(), 1);
    assert_eq!(host.foregrounds.len(), 2);
    assert!(host.revoke_scope(admitted).unwrap());
    assert!(host.private_scope_workers.is_empty());
    assert!(host.foregrounds.is_empty());
}

/// One scripted connection attempt of the host-owned socket worker.
enum HostSocketStep {
    /// Connect, then lose the established transport non-retryably once
    /// the test releases it.
    ConnectThenTerminal(tokio::sync::oneshot::Receiver<()>),
    /// Hold the attempt until released, then connect and stay connected.
    ConnectWhenReleased(tokio::sync::oneshot::Receiver<()>),
    /// The authenticated handshake rejects a different identity, which
    /// makes the worker give up before installing a semantic connection.
    ForeignIdentity,
}

/// Plays [`HostSocketStep`]s in order; further attempts never complete.
struct ScriptedHostConnector {
    steps: Mutex<std::collections::VecDeque<HostSocketStep>>,
}

impl ScriptedHostConnector {
    fn new(steps: impl IntoIterator<Item = HostSocketStep>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into_iter().collect()),
        })
    }
}

impl NativeTransportConnector for ScriptedHostConnector {
    fn connect(
        &self,
        _request: NativeTransportRequest,
    ) -> jazz::tools::native_transport_connector::NativeTransportFuture {
        let step = self.steps.lock().unwrap().pop_front();
        Box::pin(async move {
            let connected =
                |terminal| jazz::tools::native_transport_connector::ConnectedNativeTransport {
                    transport: Box::new(IdleWire),
                    protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                    features: jazz::wire::current_wire_features(),
                    terminal,
                };
            match step {
                Some(HostSocketStep::ConnectThenTerminal(release)) => {
                    Ok(connected(Box::pin(async move {
                        let _ = release.await;
                        NativeTransportTerminal::Failed(NativeTransportError::Terminal(
                            "websocket peer sent a non-binary wire batch".into(),
                        ))
                    })))
                }
                Some(HostSocketStep::ConnectWhenReleased(release)) => {
                    let _ = release.await;
                    Ok(connected(Box::pin(std::future::pending())))
                }
                Some(HostSocketStep::ForeignIdentity) => Err(NativeTransportError::Terminal(
                    "authenticated handshake rejected a different peer identity".into(),
                )),
                None => std::future::pending().await,
            }
        })
    }
}

/// Admit alice's private scope against a scripted upstream and open one
/// foreground on it.
fn scripted_private_foreground(
    root: &std::path::Path,
    app_id: &str,
    connector: Arc<ScriptedHostConnector>,
) -> (NativeRelayHost, u64) {
    use base64::Engine;
    let jwt = format!(
        "x.{}.x",
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"iss":"https://issuer.example","sub":"alice"}"#)
    );
    let mut host = NativeRelayHost {
        socket_connector: Some(connector),
        ..NativeRelayHost::default()
    };
    let pending = host
        .begin_private_session(PrivateSessionSetupJson {
            server_url: "https://edge.example".to_owned(),
            app_id: app_id.to_owned(),
            jwt,
            storage_root: root.display().to_string(),
        })
        .unwrap();
    let admitted = host
        .attach_canonical_schema(
            pending,
            &serde_json::to_string(schema().public_schema()).unwrap(),
        )
        .unwrap();
    let foreground = host
        .open_foreground(admitted, DIRECT_FOREGROUND_RUNTIME_TOKEN)
        .unwrap();
    (host, foreground)
}

fn wait_for_host(host: &NativeRelayHost, what: &str, done: impl Fn(&NativeRelayHost) -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !done(host) {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting: {what}"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn native_connected(host: &mut NativeRelayHost, foreground: u64) -> bool {
    match host.foreground_connectivity(foreground, None).unwrap() {
        ForegroundDbCommandResponse::NativeConnectionStatus { connected, .. } => connected,
        other => panic!("unexpected connectivity response: {other:?}"),
    }
}

/// Losing an established upstream link that the worker is retrying is
/// connection state, not a foreground failure (#3630), even when the
/// transport classifies the loss as non-retryable. Alice's foreground
/// keeps ticking while the native socket reports disconnected, and the
/// worker's next authenticated connection restores it. (A server that
/// rejects the session itself still fails the tick; see
/// `private_session_invalid_bearer_still_fails_closed`.)
///
/// Host-level rather than through JSI: the regression is the relay host's
/// tick gate, and only a scripted connector can hold the worker inside
/// its retry window deterministically.
///
/// ```text
/// alice ─open─► relay ══socket══► edge   (connected)
///                 │        ✗ terminal transport loss, worker retries
/// alice ─tick─► relay                    (Ok, connected=false)
///                 │ ══socket══► edge     (reconnected)
/// alice ─tick─► relay                    (Ok, connected=true)
/// ```
#[test]
fn retrying_upstream_failure_keeps_foreground_ticking() {
    let root = tempfile::tempdir().unwrap();
    let (fail_first, first_failure) = tokio::sync::oneshot::channel();
    let (reconnect, reconnected) = tokio::sync::oneshot::channel();
    let connector = ScriptedHostConnector::new([
        HostSocketStep::ConnectThenTerminal(first_failure),
        HostSocketStep::ConnectWhenReleased(reconnected),
    ]);
    let (mut host, alice) =
        scripted_private_foreground(root.path(), "retrying-upstream-tick", connector);
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for_host(&host, "first connection", |host| {
        host.private_scope_workers[&scope]
            .connected
            .load(Ordering::Acquire)
    });
    assert_eq!(host.tick_foreground(alice), Ok(()));

    fail_first.send(()).unwrap();
    wait_for_host(&host, "established link lost", |host| {
        !host.private_scope_workers[&scope]
            .connected
            .load(Ordering::Acquire)
    });
    assert!(!native_connected(&mut host, alice));
    assert_eq!(
        host.tick_foreground(alice),
        Ok(()),
        "a retrying upstream failure must not fail a valid foreground tick"
    );
    assert!(host.foregrounds.contains_key(&alice));

    reconnect.send(()).unwrap();
    wait_for_host(&host, "worker reconnects", |host| {
        host.private_scope_workers[&scope]
            .connected
            .load(Ordering::Acquire)
    });
    assert_eq!(host.tick_foreground(alice), Ok(()));
}

/// An upstream whose worker gave up still fails alice's tick, so strict
/// remote work is never parked forever in silence. Explicit reconnect is
/// a single-call recovery: it replaces the stopped worker instead of
/// returning early on it, and the new connection clears the failure.
///
/// ```text
/// alice ─open─► relay ══socket══► edge (as mallory) ✗ worker stops
/// alice ─tick─► relay                  (LifecycleFailure)
/// alice ─reconnect─► relay ══socket══► edge (as alice)
/// alice ─tick─► relay                  (Ok, connected=true)
/// ```
#[test]
fn explicit_reconnect_replaces_a_stopped_upstream_worker() {
    let root = tempfile::tempdir().unwrap();
    let (reconnect, reconnected) = tokio::sync::oneshot::channel();
    reconnect.send(()).unwrap();
    let connector = ScriptedHostConnector::new([
        HostSocketStep::ForeignIdentity,
        HostSocketStep::ConnectWhenReleased(reconnected),
    ]);
    let (mut host, alice) =
        scripted_private_foreground(root.path(), "stopped-upstream-reconnect", connector);
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for_host(&host, "worker gives up", |host| {
        host.private_scope_workers[&scope].terminally_stopped()
    });
    assert_eq!(
        host.tick_foreground(alice),
        Err(JazzNativeRelayStatus::LifecycleFailure)
    );

    host.foreground_connectivity(alice, Some(false)).unwrap();
    wait_for_host(&host, "replacement worker connects", |host| {
        host.private_scope_workers[&scope]
            .connected
            .load(Ordering::Acquire)
    });
    assert!(host.private_scope_terminal_error(&scope).is_none());
    assert_eq!(host.tick_foreground(alice), Ok(()));
}

fn encoded_cells(descriptor: RecordDescriptor, raw: Vec<u8>) -> Vec<u8> {
    jazz::binding_codec::encode_named_cells(&jazz::records::OwnedRecord::new(raw, descriptor))
        .expect("fixture cells encode")
}

fn encoded_title_cells(title: &str) -> Vec<u8> {
    let descriptor = RecordDescriptor::new([("title", ValueType::String)]);
    let raw = descriptor
        .create(&[Value::String(title.to_owned())])
        .expect("fixture title record is valid");
    encoded_cells(descriptor, raw)
}

/// Alice's foreground stage is invisible until exclusive commit; rollback
/// and abort cannot publish it, and a consumed handle cannot attach again.
#[test]
fn foreground_staged_upload_commit_and_rollback_are_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let mut relay_config = config(directory.path().join("staged.sqlite"), Some("staged"));
    relay_config.identity.author = AuthorSubject::for_test_bytes([0x4a; 16]);
    let relay = NativeRelayRegistry::default()
        .open_admitted(relay_config)
        .unwrap();
    let client = relay
        .attach_client(
            fresh_client_identity(AuthorSubject::for_test_bytes([0x4a; 16])).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    let settle = |mut response| {
        for _ in 0..100 {
            let ForegroundDbCommandResponse::Pending { operation } = response else {
                return response;
            };
            relay.pump().unwrap();
            response = client
                .execute(ForegroundDbCommandRequest::Poll { operation })
                .unwrap();
        }
        panic!("foreground stage operation did not settle");
    };
    for (row_id, commit) in [([0x4b; 16], true), ([0x4c; 16], false)] {
        let descriptor = RecordDescriptor::new(std::iter::empty::<(&str, ValueType)>());
        let raw = descriptor.create(&[]).unwrap();
        let response = client
            .execute_mutation_command(ForegroundDbCommandRequest::BeginStreamingMutation {
                mutation: ForegroundMutationKind::Insert,
                table: "todos".into(),
                row_id,
                cells: encoded_cells(descriptor, raw),
                column: "title".into(),
                options_json: "{}".into(),
            })
            .unwrap();
        let ForegroundDbCommandResponse::StreamingMutationOpened { upload } = response else {
            panic!("upload handle")
        };
        assert!(matches!(
            settle(
                client
                    .execute_mutation_command(ForegroundDbCommandRequest::PushStreamingMutation {
                        upload,
                        chunk: b"staged body".to_vec()
                    },)
                    .unwrap()
            ),
            ForegroundDbCommandResponse::StreamingMutationPushed
        ));
        let staged = settle(
            client
                .execute_mutation_command(ForegroundDbCommandRequest::StageStreamingMutation {
                    upload,
                })
                .unwrap(),
        );
        let ForegroundDbCommandResponse::StreamingMutationStaged { staged } = staged else {
            panic!("staged handle")
        };
        let transaction = client
            .begin_foreground_transaction(ForegroundTransactionKind::Serializable)
            .unwrap();
        assert!(matches!(
            settle(
                client
                    .execute_mutation_command(
                        ForegroundDbCommandRequest::AttachStagedStreamingMutation {
                            staged,
                            transaction
                        },
                    )
                    .unwrap()
            ),
            ForegroundDbCommandResponse::StagedStreamingMutationAttached
        ));
        assert!(
            matches!(client.execute(ForegroundDbCommandRequest::AttachStagedStreamingMutation {
            staged, transaction,
        }).unwrap(), ForegroundDbCommandResponse::OperationError { reason } if reason.contains("unknown staged streaming mutation"))
        );
        let rows = client
            .local_current_foreground_row("todos".into(), row_id)
            .unwrap();
        let batches: Vec<DecodedForegroundRowBatch> = postcard::from_bytes(&rows).unwrap();
        assert!(batches.iter().all(|batch| batch.rows.is_empty()));
        if commit {
            let tx = client.commit_foreground_transaction(transaction).unwrap();
            let wait = client
                .execute(ForegroundDbCommandRequest::WaitForTransaction {
                    tx_id: *tx.as_bytes(),
                    tier: "Local".into(),
                    observe_only: false,
                })
                .unwrap();
            assert!(matches!(
                settle(wait),
                ForegroundDbCommandResponse::TransactionSettled { .. }
            ));
            let rows = client
                .local_current_foreground_row("todos".into(), row_id)
                .unwrap();
            let batches: Vec<DecodedForegroundRowBatch> = postcard::from_bytes(&rows).unwrap();
            assert_eq!(batches.len(), 1);
            let batch = &batches[0];
            assert_eq!(batch.table, "todos");
            assert_eq!(batch.rows.len(), 1);
            let row = &batch.rows[0];
            assert_eq!(row.row_id, RowUuid::from_bytes(row_id));
            assert!(!row.deleted);
            let record = BorrowedRecord::new(&row.raw, &batch.descriptor);
            assert_eq!(
                record.get("title").unwrap(),
                Value::String("staged body".into()),
            );
        } else {
            assert!(client.rollback_foreground_transaction(transaction).unwrap());
            relay.pump().unwrap();
            let rows = client
                .local_current_foreground_row("todos".into(), row_id)
                .unwrap();
            let batches: Vec<DecodedForegroundRowBatch> = postcard::from_bytes(&rows).unwrap();
            assert!(batches.iter().all(|batch| batch.rows.is_empty()));
        }
        assert!(matches!(
            settle(
                client
                    .execute_mutation_command(
                        ForegroundDbCommandRequest::AbortStagedStreamingMutation { staged },
                    )
                    .unwrap()
            ),
            ForegroundDbCommandResponse::StagedStreamingMutationAborted { aborted: false }
        ));
    }
    client.close().unwrap();
}

#[derive(serde::Deserialize)]
struct DecodedForegroundRowBatch {
    table: String,
    #[serde(deserialize_with = "publication_codec::descriptor")]
    descriptor: RecordDescriptor,
    rows: Vec<DecodedForegroundRow>,
}

#[derive(serde::Deserialize)]
struct DecodedForegroundRow {
    row_id: RowUuid,
    deleted: bool,
    raw: Vec<u8>,
}

fn assert_exact_todo_rows(rows: &[u8], row_id: RowUuid, title: &str) {
    let batches = postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(rows)
        .expect("foreground row bytes use the shared binding row-batch codec");
    assert_eq!(
        batches.len(),
        1,
        "the query returns one contiguous todo batch"
    );
    let batch = &batches[0];
    assert_eq!(batch.table, "todos");
    assert_eq!(
        batch.rows.len(),
        1,
        "the query returns exactly the seeded row"
    );
    let row = &batch.rows[0];
    assert_eq!(
        row.row_id, row_id,
        "the exact row identity survives relay delivery"
    );
    assert!(!row.deleted, "the persisted row remains live");
    let record = BorrowedRecord::new(&row.raw, &batch.descriptor);
    // SQLite carries physical identity in the row envelope checked above.
    // Decode the logical column by its binding descriptor, not an ordinal
    // from the retired query layout that duplicated the id in its cells.
    assert_eq!(
        record.get("title").unwrap(),
        Value::String(title.to_owned()),
        "the declared logical title is exact"
    );
}

// Independently decode the complete published delta, including removals and
// occurrence/position metadata. The native carrier buffers this exact payload.
#[derive(serde::Deserialize)]
struct DecodedForegroundDelta {
    added: Vec<DecodedForegroundRowBatch>,
    updated: Vec<DecodedForegroundRowBatch>,
    removed: Vec<(String, RowUuid)>,
    _added_keys: Vec<Vec<u8>>,
    _updated_keys: Vec<Vec<u8>>,
    _removed_keys: Vec<Vec<u8>>,
    _added_indices: Vec<u64>,
    _updated_previous_indices: Vec<u64>,
    _updated_indices: Vec<u64>,
    _removed_indices: Vec<u64>,
}

/// Test-only owner for the same C ABI that Kotlin and Swift use.  It is
/// deliberately confined to this `#[cfg(test)]` module: a device fixture
/// needs to prove process close/reopen, but production bindings must not
/// expose a generic host-reset or SQLite-teardown operation to JavaScript.
struct NativeHostAbiFixture {
    accounts: RefCell<BTreeMap<String, jazz::account_registry::AccountId>>,
    host: *mut JazzNativeRelayHost,
    lease: *mut JazzNativeRelayHostLease,
}

impl NativeHostAbiFixture {
    fn new() -> Self {
        let host = jazz_native_relay_host_new();
        assert!(!host.is_null(), "native host allocation succeeds");
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        assert!(!lease.is_null(), "native host lease succeeds");
        Self {
            host,
            lease,
            accounts: RefCell::new(BTreeMap::new()),
        }
    }

    fn admit(
        &self,
        sqlite_path: &std::path::Path,
        auth_scope: &str,
        schema: &JazzSchema,
        byte: u8,
    ) -> [u8; 32] {
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([byte; 16]),
            author: AuthorSubject::for_test_bytes([byte.wrapping_add(1); 16]),
        };
        self.admit_identity(sqlite_path, auth_scope, schema, identity)
    }

    fn admit_identity(
        &self,
        sqlite_path: &std::path::Path,
        auth_scope: &str,
        schema: &JazzSchema,
        identity: DbIdentity,
    ) -> [u8; 32] {
        let request = serde_json::json!({
            "scope": {
                "app_namespace": "native-host-reopen-abi",
                "storage_namespace": "default",
                "auth_scope": auth_scope,
            },
            "sqlite_path": sqlite_path.display().to_string(),
            "schema_json": serde_json::to_string(schema.public_schema()).unwrap(),
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
            JazzNativeRelayStatus::Ok,
            "trusted native admission succeeds"
        );
        let capability = unsafe { std::slice::from_raw_parts(output.data, output.len) };
        assert_eq!(
            capability.len(),
            32,
            "admission returns only one opaque capability"
        );
        let mut bytes = [0; 32];
        bytes.copy_from_slice(capability);
        unsafe { jazz_native_relay_bytes_free(&mut output) };
        bytes
    }

    /// Exercise the private platform session handoff used by Android and
    /// iOS. Unlike `admit`, neither the endpoint nor the bearer crosses
    /// the generic relay command ABI.
    async fn begin_account_session(
        &self,
        server_url: &str,
        app_id: &str,
        bearer: &str,
        storage_root: &std::path::Path,
        schema: &JazzSchema,
    ) -> [u8; 32] {
        let registry = format!(
            "{}/apps/{app_id}/accounts",
            server_url.trim_end_matches('/')
        );
        let (issuer, subject) = jazz::tools::unverified_jwt_scope_subject(bearer).unwrap();
        let key = serde_json::to_string(&(&registry, &issuer, &subject)).unwrap();
        // Retain the enrolled handle across offline reopening; authentication
        // and registration happen before context construction, not inside it.
        let retained = self.accounts.borrow().get(&key).copied();
        let account = if let Some(account) = retained {
            account
        } else {
            let response = reqwest::Client::new()
                .post(format!("{registry}/register"))
                .bearer_auth(bearer)
                .send()
                .await
                .unwrap();
            assert!(
                response.status().is_success(),
                "account registration: {}",
                response.status()
            );
            let response: serde_json::Value = response.json().await.unwrap();
            let account = serde_json::from_value(response["account"].clone()).unwrap();
            self.accounts.borrow_mut().insert(key, account);
            account
        };
        // Match the production React Native account adapter: provider
        // claims accompany the bearer as local advisory policy metadata.
        // Omitting them made this fixture disagree with its signed role
        // claim once the server admitted an exact scope-relay binding.
        use base64::Engine;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(bearer.split('.').nth(1).unwrap())
            .unwrap();
        let mut claims: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&payload).unwrap();
        for registered in ["sub", "exp", "nbf", "iat", "iss", "aud", "jti"] {
            claims.remove(registered);
        }
        let request = serde_json::json!({
            "server_url": server_url, "registry": registry,
            "app_id": app_id, "env": "test", "account_id": account,
            "issuer": issuer, "subject": subject, "jwt": bearer,
            "claims": claims,
        });
        let root = storage_root.to_str().unwrap().as_bytes();
        let request = serde_json::to_vec(&request).expect("private session JSON encodes");
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
            JazzNativeRelayStatus::Ok,
            "private endpoint and ephemeral bearer are accepted only at the native boundary"
        );
        let setup = take_capability(&mut setup);
        let schema = serde_json::to_string(schema.public_schema()).expect("schema JSON encodes");
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
            JazzNativeRelayStatus::Ok,
            "credential-free canonical schema attachment admits the private session"
        );
        take_capability(&mut admitted)
    }

    fn revoke_private_session(&self, capability: &[u8; 32]) {
        assert_eq!(
            unsafe {
                jazz_native_relay_host_revoke_scope_capability(
                    self.host,
                    capability.as_ptr(),
                    capability.len(),
                )
            },
            JazzNativeRelayStatus::Ok,
            "trusted revocation stops the native relay and its socket worker"
        );
    }

    fn open_foreground(&self, capability: &[u8; 32]) -> u64 {
        let (status, foreground) = self.try_open_foreground(capability);
        assert_eq!(
            status,
            JazzNativeRelayStatus::Ok,
            "C ABI opens one admitted foreground"
        );
        assert_ne!(foreground, 0);
        foreground
    }

    fn try_open_foreground(&self, capability: &[u8; 32]) -> (JazzNativeRelayStatus, u64) {
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

    fn execute(
        &self,
        foreground: u64,
        command: ForegroundDbCommandRequest,
    ) -> ForegroundDbCommandResponse {
        let request = postcard::to_allocvec(&command).unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_execute_foreground(
                    self.lease,
                    foreground,
                    request.as_ptr(),
                    request.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::Ok,
            "foreground command succeeds through the native C ABI"
        );
        let response =
            postcard::from_bytes(unsafe { std::slice::from_raw_parts(output.data, output.len) })
                .expect("native C ABI returns one canonical foreground response");
        unsafe { jazz_native_relay_bytes_free(&mut output) };
        response
    }

    fn tick(&self, foreground: u64) {
        assert_eq!(
            self.tick_status(foreground),
            JazzNativeRelayStatus::Ok,
            "native relay tick succeeds"
        );
    }

    fn tick_status(&self, foreground: u64) -> JazzNativeRelayStatus {
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(self.lease, foreground) }
    }

    fn insert_todo(&self, foreground: u64, row_id: [u8; 16], title: &str) -> [u8; 16] {
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = self.execute(
            foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            },
        ) else {
            panic!("foreground transaction must open");
        };
        assert_eq!(
            self.execute(
                foreground,
                ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "todos".to_owned(),
                    cells: encoded_title_cells(title),
                    row_id: Some(row_id),
                },
            ),
            ForegroundDbCommandResponse::Inserted { row_id }
        );
        let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = self.execute(
            foreground,
            ForegroundDbCommandRequest::CommitTransaction { transaction },
        ) else {
            panic!("foreground transaction must commit");
        };
        assert_ne!(tx_id, [0; 16]);
        tx_id
    }

    async fn wait_for_core_transaction(&self, foreground: u64, tx_id: [u8; 16]) {
        let mut operation = match self.execute(
            foreground,
            ForegroundDbCommandRequest::WaitForCoreTransaction { tx_id },
        ) {
            ForegroundDbCommandResponse::Pending { operation } => operation,
            ForegroundDbCommandResponse::TransactionSettled { tx_id: settled } => {
                assert_eq!(settled, tx_id);
                return;
            }
            response => panic!("Core settlement did not start: {response:?}"),
        };
        for _ in 0..600 {
            self.tick(foreground);
            match self.execute(foreground, ForegroundDbCommandRequest::Poll { operation }) {
                ForegroundDbCommandResponse::Pending { operation: pending } => {
                    operation = pending;
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                ForegroundDbCommandResponse::TransactionSettled { tx_id: settled } => {
                    assert_eq!(
                        settled, tx_id,
                        "Core settles the same foreground transaction"
                    );
                    return;
                }
                ForegroundDbCommandResponse::OperationError { reason } => {
                    panic!("Core rejected native foreground transaction: {reason}")
                }
                response => {
                    panic!("Core settlement returned unexpected response: {response:?}")
                }
            }
        }
        panic!("timed out waiting for native foreground transaction to reach Core");
    }

    fn rows_after_sync(&self, foreground: u64) -> Vec<u8> {
        self.rows_after_sync_at_stage(foreground, "foreground read")
    }

    fn rows_after_sync_at_stage(&self, foreground: u64, stage: &str) -> Vec<u8> {
        let _delivery_recording = jazz::delivery_diagnostics::start();
        // Local-first reads are allowed to finish with their current
        // local knowledge. Drive the ordinary relay loop before starting
        // the read, rather than treating an initial empty local snapshot
        // as evidence that replication completed.
        for _ in 0..64 {
            self.tick(foreground);
        }
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();

        let ForegroundDbCommandResponse::Subscribed { .. } = self.execute(
            foreground,
            ForegroundDbCommandRequest::Subscribe {
                query: query.clone(),
                options_json: "{}".into(),
            },
        ) else {
            panic!("foreground subscription preparation must return a handle");
        };

        // Keep the same asynchronous read until it completes. Starting a
        // second All after a Pending reply leaks the first observation and
        // can exhaust the intentionally bounded native operation capacity.
        let mut request = ForegroundDbCommandRequest::All {
            query,
            options_json: "{}".into(),
            transaction: None,
        };
        for _ in 0..120 {
            self.tick(foreground);
            std::thread::sleep(Duration::from_millis(25));
            match self.execute(foreground, request) {
                ForegroundDbCommandResponse::Rows { rows } => return rows,
                ForegroundDbCommandResponse::Pending { operation } => {
                    request = ForegroundDbCommandRequest::Poll { operation };
                }
                response => {
                    panic!("foreground All returned an unexpected response: {response:?}")
                }
            }
        }
        eprintln!("foreground read exhausted bounded native relay ticks at {stage}");
        eprintln!(
            "NATIVE_DELIVERY_FLIGHT_RECORDER\n{}",
            jazz::delivery_diagnostics::snapshot()
        );
        // Inspect only native scheduling state, never row values, session
        // claims, or credentials. A hosted failure must distinguish an
        // opening blocked on its owner from a live read awaiting delivery.
        let client = unsafe { &*self.host }
            .inner
            .lock()
            .unwrap()
            .foreground_client(foreground)
            .unwrap()
            .clone();
        let operation = match request {
            ForegroundDbCommandRequest::Poll { operation } => operation,
            _ => unreachable!("each unfinished read retains its pending operation"),
        };
        let diagnostic = client.relay.run(move |worker| {
            let state = worker.client(client.id)?;
            let upstream_depths = worker.upstream.as_ref().map(|upstream| upstream.wire.queue_depths());
            let scheduling = format!(
                "upstream={} persistent_dirty={} foreground_dirty={} subscriptions={} pending_operations={} completed_operations={} upstream_queues={upstream_depths:?}",
                worker.upstream.is_some(),
                worker.persistent_dirty.get(),
                state.dirty.get(),
                state.subscriptions.len(),
                state.pending.len(),
                state.completed.len(),
            );
            eprintln!("NATIVE_READ_DIAGNOSTIC repoll operation={operation} begin");
            let repoll = match worker.poll_foreground_operation(client.id, operation) {
                Ok(ForegroundOperationPoll::Pending { .. }) => "pending",
                Ok(ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(_))) => {
                    "ready_rows"
                }
                Ok(ForegroundOperationPoll::Ready(_)) => "ready_other",
                Ok(ForegroundOperationPoll::Error { .. }) => "operation_error",
                Err(_) => "relay_error",
            };
            eprintln!("NATIVE_READ_DIAGNOSTIC repoll operation={operation} result={repoll}");
            Ok(format!("{scheduling} diagnostic_repoll={repoll}"))
        });
        panic!(
            "foreground read did not settle after bounded native relay ticks \
             at {stage}: operation={operation}; owner={diagnostic:?}"
        );
    }
}

fn take_capability(output: &mut JazzNativeRelayBytes) -> [u8; 32] {
    let bytes = unsafe { std::slice::from_raw_parts(output.data, output.len) };
    assert_eq!(
        bytes.len(),
        32,
        "native private admission returns one opaque capability"
    );
    let mut capability = [0; 32];
    capability.copy_from_slice(bytes);
    unsafe { jazz_native_relay_bytes_free(output) };
    capability
}

async fn wait_for_persisted_todo(
    fixture: &NativeHostAbiFixture,
    foreground: u64,
    row_id: [u8; 16],
    title: &str,
    stage: &str,
) {
    for _ in 0..120 {
        let rows = fixture.rows_after_sync_at_stage(foreground, stage);
        if !postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&rows)
            .expect("foreground row bytes decode")
            .is_empty()
        {
            assert_exact_todo_rows(&rows, RowUuid::from_bytes(row_id), title);
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {stage} to materialize from the persistent native relay");
}

/// A real Core authenticates a native private-session bearer while
/// Alice writes through a foreground relay. Revoking that admission stops
/// its socket/relay; a fresh worker then reopens the same SQLite partition
/// and reads Alice's row back through the ordinary foreground protocol.
///
/// ```text
/// alice foreground ──peer──► native SQLite relay ──JWT WebSocket──► Core
///       │                         │
///       └──write, close/revoke────┴──new worker/relay──readback──► persisted row
/// ```
///
/// This host-only receipt is deliberately continuously driven from a
/// multithread Tokio harness. It proves topology and lifecycle readiness;
/// an Android emulator/device run remains a separate installed-artifact
/// acceptance receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_session_core_write_survives_worker_and_relay_restart() {
    private_session_restart_receipt(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_session_core_write_survives_offline_relay_restart() {
    private_session_restart_receipt(true).await;
}

/// Advice uses the real negotiated socket authority through the C ABI.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_session_permission_advice_uses_real_socket_authority() {
    let issuer = TestJwtIssuer::start().await;
    let schema = permissive_schema();
    let server = JazzServer::builder()
        .with_schema(schema.public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .expect("start test server");
    let storage = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let bearer = TestJwtIssuer::jwt_for_user("native-advice-alice");
    let admitted = fixture
        .begin_account_session(
            &server.base_url(),
            &server.app_id().to_string(),
            &bearer,
            storage.path(),
            &schema,
        )
        .await;
    let foreground = fixture.open_foreground(&admitted);
    let client = unsafe { &*fixture.host }
        .inner
        .lock()
        .unwrap()
        .foreground_client(foreground)
        .unwrap()
        .clone();
    jazz_testkit::wait_for(Duration::from_secs(5), "real upstream installed", || {
        let installed = client
            .relay
            .run(|worker| Ok(worker.upstream.is_some()))
            .unwrap();
        async move { installed.then_some(()) }
    })
    .await;
    let response = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::PermissionAdvice {
            action: ForegroundPermissionAdviceAction::Insert {
                table: "todos".into(),
                cells: encoded_title_cells("dry run"),
            },
        },
    );
    let ForegroundDbCommandResponse::Pending { operation } = response else {
        panic!("advice pending: {response:?}");
    };
    let advice = jazz_testkit::wait_for(Duration::from_secs(5), "authoritative advice", || {
        fixture.tick(foreground);
        let response = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
        async move {
            match response {
                ForegroundDbCommandResponse::Pending { .. } => None,
                response => Some(response),
            }
        }
    })
    .await;
    assert!(
        matches!(
            advice,
            ForegroundDbCommandResponse::PermissionAdvice {
                advice: ForegroundPermissionAdvice::Allowed
            }
        ),
        "{advice:?}"
    );
    fixture.revoke_private_session(&admitted);
    assert_eq!(
        server.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovered_sibling_cannot_replace_live_serializable_upload() {
    let issuer = TestJwtIssuer::start().await;
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                    ),
            )
            .build(),
    )
    .unwrap();
    let server = JazzServer::builder()
        .with_schema(schema.public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .expect("start test server");
    let storage = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let bearer = TestJwtIssuer::jwt_for_user("native-advice-alice");
    let admitted = fixture
        .begin_account_session(
            &server.base_url(),
            &server.app_id().to_string(),
            &bearer,
            storage.path(),
            &schema,
        )
        .await;
    let foreground = fixture.open_foreground(&admitted);
    let client = unsafe { &*fixture.host }
        .inner
        .lock()
        .unwrap()
        .foreground_client(foreground)
        .unwrap()
        .clone();
    jazz_testkit::wait_for(Duration::from_secs(5), "real upstream installed", || {
        let installed = client
            .relay
            .run(|worker| Ok(worker.upstream.is_some()))
            .unwrap();
        async move { installed.then_some(()) }
    })
    .await;
    let row_id = [0x61; 16];
    let descriptor =
        RecordDescriptor::new([("title", ValueType::String), ("done", ValueType::Bool)]);
    let now_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    };
    let raw = descriptor
        .create(&[Value::String("before".into()), Value::Bool(false)])
        .unwrap();
    let ForegroundDbCommandResponse::MutationCommitted {
        tx_id: inserted, ..
    } = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::DirectMutation {
            mutation: ForegroundMutationKind::Insert,
            table: "todos".into(),
            row_id: Some(row_id),
            cells: encoded_cells(descriptor, raw),
            options_json: format!(r#"{{"updatedAtMs":{}}}"#, now_ms()),
        },
    )
    else {
        panic!("direct insertion queues");
    };
    let mut initial_wait = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::WaitForTransaction {
            tx_id: inserted,
            tier: "global".into(),
            observe_only: false,
        },
    );
    for _ in 0..10000 {
        let ForegroundDbCommandResponse::Pending { operation } = initial_wait else {
            break;
        };
        fixture.tick(foreground);
        tokio::task::yield_now().await;
        initial_wait = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
    }
    assert!(
        matches!(
            initial_wait,
            ForegroundDbCommandResponse::TransactionSettled { .. }
        ),
        "{initial_wait:?}"
    );
    assert_eq!(
        postcard::to_allocvec(&Query::from("todos").limit(1)).unwrap(),
        vec![
            5, 116, 111, 100, 111, 115, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0
        ]
    );
    let mut remote = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::All {
            query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
            options_json: r#"{"tier":"remote"}"#.into(),
            transaction: None,
        },
    );
    for _ in 0..1000 {
        let ForegroundDbCommandResponse::Pending { operation } = remote else {
            break;
        };
        fixture.tick(foreground);
        tokio::time::sleep(Duration::from_millis(1)).await;
        remote = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
    }
    assert!(
        matches!(remote, ForegroundDbCommandResponse::Rows { .. }),
        "{remote:?}"
    );
    let ForegroundDbCommandResponse::TransactionOpened { transaction } = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        },
    ) else {
        panic!("serializable transaction opens");
    };
    assert_eq!(
        fixture.execute(
            foreground,
            ForegroundDbCommandRequest::StageMutation {
                transaction,
                mutation: ForegroundMutationKind::Update,
                table: "todos".into(),
                row_id: Some(row_id),
                cells: encoded_title_cells("accepted"),
                options_json: format!(r#"{{"updatedAtMs":{}}}"#, now_ms()),
            }
        ),
        ForegroundDbCommandResponse::MutationStaged
    );
    let mut read = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::All {
            query: postcard::to_allocvec(&Query::from("todos").limit(1)).unwrap(),
            options_json: r#"{"tier":"local-first"}"#.into(),
            transaction: Some(transaction),
        },
    );
    for _ in 0..200 {
        let ForegroundDbCommandResponse::Pending { operation } = read else {
            break;
        };
        fixture.tick(foreground);
        tokio::time::sleep(Duration::from_millis(1)).await;
        read = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
    }
    assert!(
        matches!(read, ForegroundDbCommandResponse::Rows { .. }),
        "{read:?}"
    );
    // The read hydrated its snapshot online; the commit is made offline.
    fixture.execute(
        foreground,
        ForegroundDbCommandRequest::DisconnectNativeUpstream,
    );
    let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::CommitTransaction { transaction },
    ) else {
        panic!("serializable commit queues");
    };
    let ForegroundDbCommandResponse::Pending { operation } = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::WaitForTransaction {
            tx_id,
            tier: "global".into(),
            observe_only: false,
        },
    ) else {
        panic!("offline acceptance waits");
    };
    for _ in 0..100 {
        fixture.tick(foreground);
        assert!(matches!(
            fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation }),
            ForegroundDbCommandResponse::Pending { .. }
        ));
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // Reopening a sibling recovers pending local history before reconnect.
    // That recovery must not replace the live serializable commit's evidence.
    let _sibling = fixture.open_foreground(&admitted);
    fixture.execute(
        foreground,
        ForegroundDbCommandRequest::ReconnectNativeUpstream,
    );
    let mut settled = ForegroundDbCommandResponse::Pending { operation };
    for _ in 0..1000 {
        fixture.tick(foreground);
        settled = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
        if !matches!(settled, ForegroundDbCommandResponse::Pending { .. }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        settled,
        ForegroundDbCommandResponse::TransactionSettled { tx_id }
    );
    fixture.revoke_private_session(&admitted);
    assert_eq!(
        server.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );
}

/// The C ABI must surface an actual server authentication denial, even though
/// a disconnected peer no longer disables local SQLite work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_session_invalid_bearer_still_fails_closed() {
    let issuer = TestJwtIssuer::start().await;
    let schema = schema();
    let server = JazzServer::builder()
        .with_schema(schema.public_schema().clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .expect("start test server");
    let storage = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let mut bearer = TestJwtIssuer::jwt_for_user("native-private-alice");
    let initial = fixture
        .begin_account_session(
            &server.base_url(),
            &server.app_id().to_string(),
            &bearer,
            storage.path(),
            &schema,
        )
        .await;
    fixture.revoke_private_session(&initial);
    // Retain the enrolled account, then corrupt only its transport proof.
    // The native socket must reject it even though registration succeeded.
    let signature = bearer.rfind('.').unwrap() + 1;
    let replacement = if &bearer[signature..signature + 1] == "A" {
        "B"
    } else {
        "A"
    };
    bearer.replace_range(signature..signature + 1, replacement);
    let admitted = fixture
        .begin_account_session(
            &server.base_url(),
            &server.app_id().to_string(),
            &bearer,
            storage.path(),
            &schema,
        )
        .await;
    let foreground = fixture.open_foreground(&admitted);
    jazz_testkit::wait_for(
        Duration::from_secs(5),
        "invalid signature fails the native foreground closed",
        || {
            let denied = fixture.tick_status(foreground) == JazzNativeRelayStatus::LifecycleFailure;
            async move { denied.then_some(()) }
        },
    )
    .await;
    fixture.revoke_private_session(&admitted);
    assert_eq!(
        server.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );
}

/// A strict native read crosses the local relay and authenticated Core link and must retain
/// its policy binding without exporting a hop-local relay capability.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_session_strict_read_crosses_local_relay_and_core() {
    let issuer = TestJwtIssuer::start().await;
    let schema = permissive_schema();
    let public_schema = schema.public_schema().clone();
    let core = JazzServer::builder()
        .with_schema(public_schema.clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .expect("start test server");
    // Mint this bearer at runtime from the local issuer. No bearer or
    // signing material is checked into the relay/device fixture.
    let bearer = TestJwtIssuer::jwt_for_user("native-private-alice");
    let storage = tempfile::tempdir().expect("private relay storage root");
    let fixture = NativeHostAbiFixture::new();
    let admitted = fixture
        .begin_account_session(
            &core.base_url(),
            &core.app_id().to_string(),
            &bearer,
            storage.path(),
            &schema,
        )
        .await;
    let foreground = fixture.open_foreground(&admitted);

    jazz_testkit::wait_for(
        Duration::from_secs(15),
        "native relay's scoped authenticated Core websocket",
        || {
            let connected = core.server_state().shutdown.active_websockets() > 0;
            async move { connected.then_some(()) }
        },
    )
    .await;

    let row_id = [0x6b; 16];
    let committed = fixture.insert_todo(foreground, row_id, "strict two-hop row");
    fixture
        .wait_for_core_transaction(foreground, committed)
        .await;
    let mut response = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::All {
            query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
            options_json: r#"{"tier":"remote"}"#.into(),
            transaction: None,
        },
    );
    for _ in 0..1000 {
        let ForegroundDbCommandResponse::Pending { operation } = response else {
            break;
        };
        fixture.tick(foreground);
        tokio::time::sleep(Duration::from_millis(1)).await;
        response = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
    }
    let ForegroundDbCommandResponse::Rows { rows } = response else {
        panic!("strict scoped relay read did not settle: {response:?}");
    };
    assert_exact_todo_rows(&rows, RowUuid::from_bytes(row_id), "strict two-hop row");
    fixture.revoke_private_session(&admitted);
    assert_eq!(
        core.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );
}

async fn private_session_restart_receipt(offline: bool) {
    let issuer = TestJwtIssuer::start().await;
    let schema = permissive_schema();
    let public_schema = schema.public_schema().clone();
    let core = JazzServer::builder()
        .with_schema(public_schema.clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .expect("start test server");
    // Mint this bearer at runtime from the local issuer. No bearer or
    // signing material is checked into the relay/device fixture.
    let bearer = TestJwtIssuer::jwt_for_user("native-private-alice");
    let storage = tempfile::tempdir().expect("private relay storage root");
    let fixture = NativeHostAbiFixture::new();
    let admitted = fixture
        .begin_account_session(
            &core.base_url(),
            &core.app_id().to_string(),
            &bearer,
            storage.path(),
            &schema,
        )
        .await;
    let foreground = fixture.open_foreground(&admitted);

    jazz_testkit::wait_for(
        Duration::from_secs(15),
        "native relay's normal bearer-authenticated Core websocket",
        || {
            let connected = core.server_state().shutdown.active_websockets() > 0;
            async move { connected.then_some(()) }
        },
    )
    .await;

    let row_id = [0x3a; 16];
    let committed = fixture.insert_todo(foreground, row_id, "survives native worker restart");
    wait_for_persisted_todo(
        &fixture,
        foreground,
        row_id,
        "survives native worker restart",
        "the initial private foreground write",
    )
    .await;

    fixture
        .wait_for_core_transaction(foreground, committed)
        .await;

    assert_eq!(
        fixture.execute(foreground, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true },
        "the first foreground cleanly hands off before trusted relay revocation"
    );
    fixture.revoke_private_session(&admitted);
    assert_eq!(
        fixture.try_open_foreground(&admitted).0,
        JazzNativeRelayStatus::InvalidHandle,
        "revoked private-session capability cannot restart the old worker"
    );

    let endpoint = core.base_url();
    let app_id = core.app_id().to_string();
    let mut core = Some(core);
    if offline {
        assert_eq!(
            core.take().unwrap().shutdown().await,
            jazz_server::ShutdownPhase::StorageClosed
        );
    }

    let reopened = fixture
        .begin_account_session(&endpoint, &app_id, &bearer, storage.path(), &schema)
        .await;
    let reopened_foreground = fixture.open_foreground(&reopened);
    if !offline {
        jazz_testkit::wait_for(
            Duration::from_secs(15),
            "replacement native worker reconnects through normal Core auth",
            || {
                let connected = core
                    .as_ref()
                    .unwrap()
                    .server_state()
                    .shutdown
                    .active_websockets()
                    > 0;
                async move { connected.then_some(()) }
            },
        )
        .await;
    }
    wait_for_persisted_todo(
        &fixture,
        reopened_foreground,
        row_id,
        "survives native worker restart",
        "the replacement worker/relay readback",
    )
    .await;

    fixture.revoke_private_session(&reopened);
    if !offline {
        assert_eq!(
            core.take().unwrap().shutdown().await,
            jazz_server::ShutdownPhase::StorageClosed
        );
    }
}

impl Drop for NativeHostAbiFixture {
    fn drop(&mut self) {
        // Match platform teardown order: invalidate/release every retained
        // foreground lease before dropping the host itself.
        unsafe { jazz_native_relay_host_lease_free(self.lease) };
        unsafe { jazz_native_relay_host_free(self.host) };
    }
}

/// Models the two stages of the platform wake bridge without a JSI
/// runtime: Rust's owner thread queues a platform task, and the platform
/// later decides whether that queued task may still deliver. This remains
/// an internal lifecycle receipt because raw callback-context lifetime is
/// a native-host concern, not a user-visible Db API.
#[derive(Default)]
struct QueuedNativeWake {
    active: AtomicBool,
    cancelled: AtomicUsize,
    callbacks_after_cancel: AtomicUsize,
    queued: Mutex<Vec<(u64, u8, u64)>>,
    callback_ready: Condvar,
    delivered: AtomicUsize,
}

impl QueuedNativeWake {
    fn active() -> Self {
        Self {
            active: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn queued(&self) -> usize {
        self.queued.lock().unwrap().len()
    }

    fn wait_for_queued(&self, expected: usize) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut queued = self.queued.lock().unwrap();
        while queued.len() < expected {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, timeout) = self.callback_ready.wait_timeout(queued, remaining).unwrap();
            queued = next;
            if timeout.timed_out() {
                return queued.len() >= expected;
            }
        }
        true
    }

    fn assert_no_callback_for(&self, duration: Duration) {
        let queued = self.queued.lock().unwrap();
        let _ = self.callback_ready.wait_timeout(queued, duration).unwrap();
        assert_eq!(self.callbacks_after_cancel.load(Ordering::Acquire), 0);
    }

    /// Run platform turns which were queued before a revoke. Cancellation
    /// must make these harmless instead of delivering a stale result into
    /// a foreground whose owner/context has already been torn down.
    fn deliver_queued(&self) {
        let queued = std::mem::take(&mut *self.queued.lock().unwrap());
        if self.active.load(Ordering::Acquire) {
            self.delivered.fetch_add(queued.len(), Ordering::AcqRel);
        }
    }
}

unsafe extern "C" fn queue_native_wake(
    context: *mut c_void,
    foreground: u64,
    kind: u8,
    delay_ms: u64,
) {
    let wake = unsafe { &*(context as *const QueuedNativeWake) };
    if kind == FOREGROUND_WAKE_CANCELLED {
        wake.active.store(false, Ordering::Release);
        wake.cancelled.fetch_add(1, Ordering::AcqRel);
        return;
    }
    if wake.active.load(Ordering::Acquire) {
        wake.queued
            .lock()
            .unwrap()
            .push((foreground, kind, delay_ms));
    } else {
        wake.callbacks_after_cancel.fetch_add(1, Ordering::AcqRel);
    }
    wake.callback_ready.notify_all();
}

struct SaturatedOwner {
    release: Option<mpsc::Sender<()>>,
    drained: mpsc::Receiver<()>,
}

impl SaturatedOwner {
    fn release_and_wait(mut self) {
        self.release
            .take()
            .expect("owner blocker remains held")
            .send(())
            .expect("owner blocker remains alive");
        self.drained
            .recv()
            .expect("owner drains every admitted saturation job");
    }
}

impl Drop for SaturatedOwner {
    fn drop(&mut self) {
        // A failed assertion must not leave the relay owner blocked and
        // make fixture shutdown hang indefinitely.
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

/// Deterministically occupy the owner and fill every bounded command slot.
/// This is an internal queue-failure seam: the public C ABI can observe
/// backpressure, but cannot safely manufacture a blocked owner thread.
fn saturate_owner(relay: &NativeRelay) -> SaturatedOwner {
    let sender = relay
        .inner
        .commands
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    sender
        .try_send(RelayCommand::Run(Box::new(move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })))
        .expect("test blocker enters an empty queue");
    started_rx.recv().unwrap();
    let (drained_tx, drained_rx) = mpsc::channel();
    let mut drained = Some(drained_tx);
    for index in 0..NATIVE_RELAY_OWNER_COMMAND_MAX {
        let previous = relay.inner.command_slots.fetch_add(1, Ordering::AcqRel);
        assert_eq!(previous, index, "one permit per admitted command");
        let permit = CommandSlot(relay.inner.command_slots.clone());
        let receipt = if index + 1 == NATIVE_RELAY_OWNER_COMMAND_MAX {
            drained.take()
        } else {
            None
        };
        sender
            .try_send(RelayCommand::Run(Box::new(move |_| {
                drop(permit);
                if let Some(done) = receipt {
                    let _ = done.send(());
                }
            })))
            .expect("every normal owner slot is available exactly once");
    }
    // The channel retains one reserved shutdown slot. Normal public commands
    // must be refused by their permit limit without occupying that slot.
    assert!(matches!(
        relay.run(|_| Ok(())),
        Err(RelayError::OwnerQueueFull)
    ));
    SaturatedOwner {
        release: Some(release_tx),
        drained: drained_rx,
    }
}

#[derive(Clone, Copy, Debug)]
enum SaturatedTeardownKind {
    Close,
    Revoke,
    RuntimeInvalidation,
}

#[test]
fn native_c_abi_process_reopen_rehydrates_the_exact_persisted_foreground_row() {
    // This is the close/reopen receipt that an RN device host needs, but
    // it deliberately uses no device-only production API. The test owns a
    // first host process, writes through its foreground C ABI, destroys
    // that host, then starts a wholly fresh host and re-admits the same
    // trusted scope.
    let directory = tempfile::tempdir().unwrap();
    let alice_path = directory.path().join("fixture-user-a.sqlite");
    let schema = permissive_schema();
    let row_id = [0x41; 16];
    let row_uuid = RowUuid::from_bytes(row_id);
    let title = "survives host restart";

    {
        let first_process = NativeHostAbiFixture::new();
        let alice = first_process.admit(&alice_path, "fixture-user-a", &schema, 0x41);
        let foreground = first_process.open_foreground(&alice);
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = first_process.execute(
            foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            },
        ) else {
            panic!("foreground transaction must open");
        };
        assert_eq!(
            first_process.execute(
                foreground,
                ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "todos".to_owned(),
                    cells: encoded_title_cells(title),
                    row_id: Some(row_id),
                },
            ),
            ForegroundDbCommandResponse::Inserted { row_id }
        );
        assert!(matches!(
            first_process.execute(
                foreground,
                ForegroundDbCommandRequest::CommitTransaction { transaction },
            ),
            ForegroundDbCommandResponse::TransactionCommitted { tx_id } if tx_id != [0; 16]
        ));

        // First prove the foreground that committed through the C ABI can
        // materialize its exact local row before we ask the relay to
        // persist it for the next process.
        let rows = first_process.rows_after_sync(foreground);
        assert_exact_todo_rows(&rows, row_uuid, title);
        assert_eq!(
            first_process.execute(foreground, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
    }

    // Dropping `first_process` releases its last lease and host, which in
    // turn closes the owner thread and SQLite. This fresh host has no
    // in-memory registry, capabilities, foregrounds, or queries from the
    // first process.
    let second_process = NativeHostAbiFixture::new();
    let alice = second_process.admit(&alice_path, "fixture-user-a", &schema, 0x41);
    let reopened = second_process.open_foreground(&alice);
    let reopened_rows = second_process.rows_after_sync(reopened);
    assert_exact_todo_rows(&reopened_rows, row_uuid, title);

    // Auth-scope-to-SQLite-path selection remains a trusted *platform*
    // contract, not one the generic C admission ABI can infer or enforce:
    // it intentionally accepts the complete path selected by Kotlin or
    // Swift. Device acceptance must therefore prove scope isolation with
    // the compiled Android/iOS fixture path selectors; this core C-ABI
    // receipt deliberately proves only that an already-selected path
    // survives a complete native-host recreation.
}

#[test]
fn native_c_abi_foregrounds_use_the_exact_admitted_capability_and_scope() {
    // This is intentionally an exact C-ABI receipt rather than a host-map
    // unit test. Admit A before B, then prove B's actual foreground sees
    // B's exact trusted scope. Replacing the capability lookup in
    // `open_foreground` with `.values().next()` makes B attach to A and
    // fails this receipt before revocation can mask that mix-up.
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let schema = permissive_schema();
    let a_capability = fixture.admit(
        &directory.path().join("scope-a.sqlite"),
        "scope-a",
        &schema,
        0x51,
    );
    let b_capability = fixture.admit(
        &directory.path().join("scope-b.sqlite"),
        "scope-b",
        &schema,
        0x61,
    );
    let forged_capability = [0xa5; 32];
    assert_ne!(forged_capability, a_capability);
    assert_ne!(forged_capability, b_capability);

    let (forged_status, forged_foreground) = fixture.try_open_foreground(&forged_capability);
    assert_eq!(forged_status, JazzNativeRelayStatus::InvalidHandle);
    assert_eq!(
        forged_foreground, 0,
        "a valid-length, unadmitted capability cannot produce an opaque handle"
    );

    let a = fixture.open_foreground(&a_capability);
    let b = fixture.open_foreground(&b_capability);
    assert!(matches!(
        fixture.execute(a, ForegroundDbCommandRequest::Probe),
        ForegroundDbCommandResponse::Probe { abi_version } if abi_version == NATIVE_RELAY_ABI_V1
    ));
    assert!(matches!(
        fixture.execute(b, ForegroundDbCommandRequest::Probe),
        ForegroundDbCommandResponse::Probe { abi_version } if abi_version == NATIVE_RELAY_ABI_V1
    ));

    fixture.insert_todo(a, [0xa1; 16], "scope-a-only");
    fixture.insert_todo(b, [0xb1; 16], "scope-b-only");
    assert_exact_todo_rows(
        &fixture.rows_after_sync(a),
        RowUuid::from_bytes([0xa1; 16]),
        "scope-a-only",
    );
    assert_exact_todo_rows(
        &fixture.rows_after_sync(b),
        RowUuid::from_bytes([0xb1; 16]),
        "scope-b-only",
    );

    assert_eq!(
        unsafe {
            jazz_native_relay_host_revoke_scope_capability(
                fixture.host,
                a_capability.as_ptr(),
                a_capability.len(),
            )
        },
        JazzNativeRelayStatus::Ok,
        "trusted native code may revoke exactly A's capability"
    );
    assert_eq!(
        fixture.tick_status(a),
        JazzNativeRelayStatus::InvalidHandle,
        "revocation retires A's already-open foreground"
    );
    // Tick and byte-command execution use separate C entry points. The
    // installed device receipt probes the stale JSI alias, so prove that
    // exact liveness boundary instead of inferring it from tick failure.
    let request = postcard::to_allocvec(&ForegroundDbCommandRequest::Probe).unwrap();
    let mut output = JazzNativeRelayBytes::EMPTY;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_execute_foreground(
                fixture.lease,
                a,
                request.as_ptr(),
                request.len(),
                &mut output,
            )
        },
        JazzNativeRelayStatus::InvalidHandle,
        "a revoked foreground cannot answer the installed JSI byte Probe"
    );
    assert!(output.data.is_null());
    assert_eq!(output.len, 0);
    assert_eq!(
        fixture.tick_status(b),
        JazzNativeRelayStatus::Ok,
        "revoking A cannot disturb B's separately admitted foreground"
    );
    let (reopen_a_status, reopen_a) = fixture.try_open_foreground(&a_capability);
    assert_eq!(reopen_a_status, JazzNativeRelayStatus::InvalidHandle);
    assert_eq!(reopen_a, 0);
    let b_after_a_revoke = fixture.open_foreground(&b_capability);
    assert_eq!(
        fixture.tick_status(b_after_a_revoke),
        JazzNativeRelayStatus::Ok,
        "B remains admissible after A revocation"
    );
}

#[test]
fn c_abi_sibling_commit_wakes_retained_subscription_before_its_delta_drains() {
    // This is deliberately an internal C-ABI receipt. The public Db
    // integration suites establish subscription semantics; only the
    // native host owns opaque foreground handles and raw callback
    // registrations. It mirrors the installed A/B byte-command sequence
    // and proves that B's post-commit callback arrives before B drains
    // the committed delta.
    let directory = tempfile::tempdir().unwrap();
    // Raw callback contexts must outlive the host, including on assertion failure.
    let a_wake = Arc::new(QueuedNativeWake::active());
    let b_wake = Arc::new(QueuedNativeWake::active());
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("same-runtime-wake.sqlite"),
        "same-runtime-wake",
        &permissive_schema(),
        0xc1,
    );
    let a = fixture.open_foreground(&capability);
    let b = fixture.open_foreground(&capability);
    assert_ne!(a, b, "each alias has an independent opaque C-ABI handle");

    for (foreground, wake) in [(a, &a_wake), (b, &b_wake)] {
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_set_foreground_wake_callback(
                    fixture.lease,
                    foreground,
                    Some(queue_native_wake),
                    Arc::as_ptr(wake) as *mut c_void,
                )
            },
            JazzNativeRelayStatus::Ok,
            "each installed alias registers its own native wake callback"
        );
    }

    let ForegroundDbCommandResponse::Subscribed { subscription } = fixture.execute(
        b,
        ForegroundDbCommandRequest::Subscribe {
            query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
            options_json: "{}".into(),
        },
    ) else {
        panic!("B opens the retained subscription");
    };

    let mut initial_reset_seen = false;
    for _ in 0..96 {
        match fixture.execute(
            b,
            ForegroundDbCommandRequest::DrainSubscription { subscription },
        ) {
            ForegroundDbCommandResponse::SubscriptionEvents { events } => {
                initial_reset_seen |= events.iter().any(|event| {
                    matches!(
                        event,
                        ForegroundSubscriptionEvent::Delta { reset: true, .. }
                            | ForegroundSubscriptionEvent::StructuredDelta { reset: true, .. }
                    )
                });
            }
            ForegroundDbCommandResponse::Pending { operation } => {
                fixture.tick(b);
                let response = fixture.execute(b, ForegroundDbCommandRequest::Poll { operation });
                if let ForegroundDbCommandResponse::SubscriptionEvents { events } = response {
                    initial_reset_seen |= events.iter().any(|event| {
                        matches!(
                            event,
                            ForegroundSubscriptionEvent::Delta { reset: true, .. }
                                | ForegroundSubscriptionEvent::StructuredDelta { reset: true, .. }
                        )
                    });
                }
            }
            response => panic!("initial B subscription drain failed: {response:?}"),
        }
        if initial_reset_seen {
            break;
        }
        fixture.tick(b);
    }
    assert!(
        initial_reset_seen,
        "B observes and drains its initial reset before A writes"
    );
    a_wake.queued.lock().unwrap().clear();
    b_wake.queued.lock().unwrap().clear();

    let ForegroundDbCommandResponse::TransactionOpened { transaction } = fixture.execute(
        a,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        },
    ) else {
        panic!("A opens the byte-command transaction");
    };
    let row_id = [0xd1; 16];
    assert_eq!(
        fixture.execute(
            a,
            ForegroundDbCommandRequest::Insert {
                transaction,
                table: "todos".into(),
                cells: encoded_title_cells("wake across opaque aliases"),
                row_id: Some(row_id),
            },
        ),
        ForegroundDbCommandResponse::Inserted { row_id }
    );
    assert!(matches!(
        fixture.execute(
            a,
            ForegroundDbCommandRequest::CommitTransaction { transaction },
        ),
        ForegroundDbCommandResponse::TransactionCommitted { .. }
    ));

    let mut b_woke = false;
    for _ in 0..96 {
        fixture.tick(a);
        fixture.tick(b);
        if b_wake.queued() != 0 {
            b_woke = true;
            break;
        }
    }
    assert!(
        b_woke,
        "A's committed byte transaction schedules B's native callback before B drains"
    );
    assert!(
        b_wake
            .queued
            .lock()
            .unwrap()
            .iter()
            .any(|(foreground, _, _)| *foreground == b),
        "the callback preserves B's exact opaque foreground handle"
    );

    let mut observed_row_delta = false;
    for _ in 0..96 {
        match fixture.execute(
            b,
            ForegroundDbCommandRequest::DrainSubscription { subscription },
        ) {
            ForegroundDbCommandResponse::SubscriptionEvents { events } => {
                observed_row_delta |= events.iter().any(|event| match event {
                    ForegroundSubscriptionEvent::Delta { reset, delta, .. }
                    | ForegroundSubscriptionEvent::StructuredDelta { reset, delta, .. } => {
                        !reset
                            && delta
                                .windows(row_id.len())
                                .any(|candidate| candidate == row_id)
                    }
                    ForegroundSubscriptionEvent::Rejected { .. }
                    | ForegroundSubscriptionEvent::Closed => false,
                });
            }
            ForegroundDbCommandResponse::Pending { operation } => {
                fixture.tick(b);
                let response = fixture.execute(b, ForegroundDbCommandRequest::Poll { operation });
                if let ForegroundDbCommandResponse::SubscriptionEvents { events } = response {
                    observed_row_delta |= events.iter().any(|event| match event {
                        ForegroundSubscriptionEvent::Delta { reset, delta, .. }
                        | ForegroundSubscriptionEvent::StructuredDelta { reset, delta, .. } => {
                            !reset
                                && delta
                                    .windows(row_id.len())
                                    .any(|candidate| candidate == row_id)
                        }
                        ForegroundSubscriptionEvent::Rejected { .. }
                        | ForegroundSubscriptionEvent::Closed => false,
                    });
                }
            }
            response => panic!("post-commit B subscription drain failed: {response:?}"),
        }
        if observed_row_delta {
            break;
        }
        fixture.tick(a);
        fixture.tick(b);
    }
    assert!(
        observed_row_delta,
        "B's retained subscription materializes A's committed row through the ordinary relay"
    );
}

#[test]
fn revocation_clears_queued_owner_wakes_and_retires_only_its_foreground_lease() {
    // Internal native-host lifecycle receipt. A JSI owner turn can already
    // be queued when trusted code revokes its admitted scope. The old
    // `foregrounds.retain(...)` implementation hid A's handle but left
    // its owner client, raw callback context, and active lease alive.
    // This exact C-ABI path proves revocation clears all three while B
    // remains usable.
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let schema = permissive_schema();
    let a_capability = fixture.admit(
        &directory.path().join("revoke-a.sqlite"),
        "revoke-a",
        &schema,
        0x71,
    );
    let b_capability = fixture.admit(
        &directory.path().join("revoke-b.sqlite"),
        "revoke-b",
        &schema,
        0x81,
    );
    let a = fixture.open_foreground(&a_capability);
    let b = fixture.open_foreground(&b_capability);
    let (a_lease, b_lease, a_scope, b_scope) = unsafe {
        let host = (*fixture.host).inner.lock().unwrap();
        (
            host.foregrounds[&a].lease,
            host.foregrounds[&b].lease,
            host.foregrounds[&a].scope.clone(),
            host.foregrounds[&b].scope.clone(),
        )
    };

    // Complete each foreground's initial catalogue exchange before the
    // callback-isolation probe. Otherwise the liveness tick for B below
    // also performs startup and legitimately queues a second owner wake.
    fixture.tick(a);
    fixture.tick(b);

    let a_wake = Arc::new(QueuedNativeWake::active());
    let b_wake = Arc::new(QueuedNativeWake::active());
    for (foreground, wake) in [(a, &a_wake), (b, &b_wake)] {
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_set_foreground_wake_callback(
                    fixture.lease,
                    foreground,
                    Some(queue_native_wake),
                    Arc::as_ptr(wake) as *mut c_void,
                )
            },
            JazzNativeRelayStatus::Ok,
            "the owner installs one native callback before it schedules work"
        );
    }

    // A real initial subscription result queues a platform task. Merely
    // scheduling an empty SQLite tick does not produce a JavaScript event.
    for foreground in [a, b] {
        assert!(matches!(
            fixture.execute(
                foreground,
                ForegroundDbCommandRequest::Subscribe {
                    query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
                    options_json: "{}".into(),
                }
            ),
            ForegroundDbCommandResponse::Subscribed { .. }
        ));
    }
    assert!(a_wake.wait_for_queued(1));
    assert!(b_wake.wait_for_queued(1));
    assert_eq!(a_wake.queued(), 1, "A has one queued platform wake");
    assert_eq!(b_wake.queued(), 1, "B has one queued platform wake");

    assert_eq!(
        unsafe {
            jazz_native_relay_host_revoke_scope_capability(
                fixture.host,
                a_capability.as_ptr(),
                a_capability.len(),
            )
        },
        JazzNativeRelayStatus::Ok
    );
    assert_eq!(
        a_wake.cancelled.load(Ordering::Acquire),
        1,
        "revocation clears A's raw native callback before its context can be dropped"
    );
    assert_eq!(
        b_wake.cancelled.load(Ordering::Acquire),
        0,
        "revoking A never touches B's native callback"
    );
    assert_eq!(fixture.tick_status(a), JazzNativeRelayStatus::InvalidHandle);
    assert_eq!(fixture.tick_status(b), JazzNativeRelayStatus::Ok);
    let mut late_finalizer_closed = true;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_close_attached_foreground(
                fixture.lease,
                a,
                &mut late_finalizer_closed,
            )
        },
        JazzNativeRelayStatus::Ok,
        "a finalizer that races after revocation remains an idempotent no-op"
    );
    assert!(
        !late_finalizer_closed,
        "revocation already owned A's only teardown transition"
    );

    // Deliver the tasks only after revocation. A's stale task is a no-op;
    // B's independent task remains live, which catches both a UAF-prone
    // omitted cancellation and over-broad scope teardown.
    a_wake.deliver_queued();
    b_wake.deliver_queued();
    assert_eq!(a_wake.delivered.load(Ordering::Acquire), 0);
    assert_eq!(b_wake.delivered.load(Ordering::Acquire), 1);

    unsafe {
        let mut host = (*fixture.host).inner.lock().unwrap();
        let pool = host
            .foreground_node_leases
            .get_mut(&a_scope)
            .expect("the revoked scope retains its lease retirement ledger");
        assert_eq!(
            pool.acquire_reusable(),
            None,
            "A's revoked identity was retired, never clean-returned"
        );
        assert_eq!(
            pool.acquire_fresh(a_lease.node),
            Err(crate::node_lease::ForegroundNodeLeaseError::DuplicateNode),
            "the retired A identity cannot be minted again"
        );
        let b_pool = host
            .foreground_node_leases
            .get_mut(&b_scope)
            .expect("the independent live scope retains B's lease pool");
        assert_eq!(
            b_pool.acquire_fresh(b_lease.node),
            Err(crate::node_lease::ForegroundNodeLeaseError::DuplicateNode),
            "B's still-live foreground retains its own exclusive lease"
        );
    }
}

#[test]
fn saturated_owner_cannot_outlive_a_cancelled_raw_wake_context() {
    // Internal native-host lifecycle receipt. Saturating the owner makes
    // both scheduler clearing and owner-local client removal fail with
    // `OwnerQueueFull`. Close, revoke, and runtime invalidation must still
    // make the retained scheduler inert before telling the platform its
    // raw callback context can be freed.
    for teardown in [
        SaturatedTeardownKind::Close,
        SaturatedTeardownKind::Revoke,
        SaturatedTeardownKind::RuntimeInvalidation,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let schema = permissive_schema();
        let path = directory.path().join("shared-saturated-owner.sqlite");

        // Two distinct capabilities intentionally name the exact same
        // trusted scope. They share one owner, while revoking A must not
        // revoke B's independent admission.
        let a_capability = fixture.admit(&path, "shared-owner", &schema, 0x91);
        let b_capability = fixture.admit(&path, "shared-owner", &schema, 0x91);
        assert_ne!(a_capability, b_capability);
        let b_runtime = unsafe { jazz_native_relay_host_retain(fixture.host, 2) };
        assert!(!b_runtime.is_null());
        let a = fixture.open_foreground(&a_capability);
        let mut b = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    b_runtime,
                    b_capability.as_ptr(),
                    b_capability.len(),
                    &mut b,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert_ne!(b, 0);

        let (a_lease, b_lease, scope, a_client) = unsafe {
            let host = (*fixture.host).inner.lock().unwrap();
            (
                host.foregrounds[&a].lease,
                host.foregrounds[&b].lease,
                host.foregrounds[&a].scope.clone(),
                host.foreground_client(a).unwrap().clone(),
            )
        };
        let a_wake = Arc::new(QueuedNativeWake::active());
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_set_foreground_wake_callback(
                    fixture.lease,
                    a,
                    Some(queue_native_wake),
                    Arc::as_ptr(&a_wake) as *mut c_void,
                )
            },
            JazzNativeRelayStatus::Ok
        );

        let saturated = saturate_owner(&a_client.relay);
        match teardown {
            SaturatedTeardownKind::Close => {
                let mut closed = true;
                assert_eq!(
                    unsafe {
                        jazz_native_relay_host_lease_close_attached_foreground(
                            fixture.lease,
                            a,
                            &mut closed,
                        )
                    },
                    JazzNativeRelayStatus::LifecycleFailure
                );
                assert!(!closed);
            }
            SaturatedTeardownKind::Revoke => assert_eq!(
                unsafe {
                    jazz_native_relay_host_revoke_scope_capability(
                        fixture.host,
                        a_capability.as_ptr(),
                        a_capability.len(),
                    )
                },
                JazzNativeRelayStatus::LifecycleFailure
            ),
            SaturatedTeardownKind::RuntimeInvalidation => assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_invalidate_foreground_runtime(fixture.lease)
                },
                JazzNativeRelayStatus::LifecycleFailure
            ),
        }
        assert_eq!(
            a_wake.cancelled.load(Ordering::Acquire),
            1,
            "{teardown:?} inertizes the callback before reporting its forced retirement"
        );

        // A late HostObject finalizer is an idempotent no-op even while
        // the owner queue remains saturated; it must not attempt another
        // scheduler operation or reinterpret the failed teardown as live.
        let mut late_closed = true;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_close_attached_foreground(
                    fixture.lease,
                    a,
                    &mut late_closed,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert!(!late_closed);
        unsafe {
            let mut host = (*fixture.host).inner.lock().unwrap();
            assert!(
                host.foregrounds.contains_key(&b),
                "{teardown:?} preserves B's foreground while its shared owner is saturated"
            );
            assert!(
                host.admitted_scopes
                    .contains_key(&AdmissionCapability(b_capability)),
                "{teardown:?} preserves B's independent admission"
            );
            let pool = host.foreground_node_leases.get_mut(&scope).unwrap();
            assert_eq!(
                pool.acquire_reusable(),
                None,
                "{teardown:?} never clean-returns A while B is live"
            );
            assert_eq!(
                pool.acquire_fresh(a_lease.node),
                Err(crate::node_lease::ForegroundNodeLeaseError::DuplicateNode),
                "{teardown:?} permanently retires A"
            );
            assert_eq!(
                pool.acquire_fresh(b_lease.node),
                Err(crate::node_lease::ForegroundNodeLeaseError::DuplicateNode),
                "{teardown:?} keeps B's lease active"
            );
        }

        saturated.release_and_wait();
        assert_eq!(
            unsafe { jazz_native_relay_host_lease_tick_attached_foreground(b_runtime, b) },
            JazzNativeRelayStatus::Ok,
            "{teardown:?} leaves B usable after shared-owner backpressure clears"
        );
        // The owner still has A's Db because its close command could not
        // enter the full queue. A real scheduler request after the
        // platform has processed cancellation must nevertheless never
        // touch that raw context again.
        a_client
            .with_db(|db| {
                db.schedule_tick_for_test(TickUrgency::Immediate);
                Ok(())
            })
            .expect("orphaned owner client remains available until terminal cleanup");
        assert_eq!(
            a_wake.callbacks_after_cancel.load(Ordering::Acquire),
            0,
            "{teardown:?} leaves only an inert owner scheduler"
        );

        let mut b_closed = false;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_close_attached_foreground(b_runtime, b, &mut b_closed)
            },
            JazzNativeRelayStatus::Ok
        );
        assert!(b_closed);
        unsafe { jazz_native_relay_host_lease_free(b_runtime) };
    }
}

#[test]
fn foreground_cell_and_command_decoders_require_one_canonical_envelope() {
    // Internal ABI-boundary receipt: the JSI adapter only transports
    // bytes, so the Rust decoder owns exact-envelope and duplicate-field
    // rejection. A duplicate must not silently become last-write-wins in
    // the `RowCells` map.
    let host_cells = b"\x01\x01\x05score\x03\x08\x2a\x00\x00\x00\x00\x00\x00\x00";
    assert_eq!(
        decode_foreground_cells(host_cells).unwrap(),
        BTreeMap::from([("score".to_owned(), Value::U64(42))]),
        "RN consumes the same explicit named-cell grammar as NAPI and WASM"
    );
    let duplicate =
        RecordDescriptor::new([("title", ValueType::String), ("title", ValueType::String)]);
    let raw = duplicate
        .create(&[
            Value::String("first".to_owned()),
            Value::String("second".to_owned()),
        ])
        .unwrap();
    let mut cells = encoded_cells(duplicate, raw);
    assert!(decode_foreground_cells(&cells).is_err());

    cells.push(0);
    assert!(decode_foreground_cells(&cells).is_err());

    let canonical = jazz::binding_codec::encode_named_cells(&jazz::records::OwnedRecord::new(
        7_u64.to_le_bytes().to_vec(),
        RecordDescriptor::new([("count", ValueType::U64)]),
    ))
    .unwrap();
    assert_eq!(
        canonical,
        vec![
            1, 1, 5, b'c', b'o', b'u', b'n', b't', 3, 8, 7, 0, 0, 0, 0, 0, 0, 0
        ]
    );
    assert_eq!(
        decode_foreground_cells(&canonical).unwrap().get("count"),
        Some(&Value::U64(7))
    );

    let mut command = postcard::to_allocvec(&ForegroundDbCommandRequest::Probe).unwrap();
    command.push(0);
    assert!(decode_foreground_command(&command).is_err());
    assert!(decode_foreground_command(&[0x80, 0]).is_err());
}

fn config(path: PathBuf, auth_scope: Option<&str>) -> RelayOpenConfig {
    RelayOpenConfig {
        supported_abi: NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1,
            maximum: NATIVE_RELAY_ABI_V1,
        },
        scope: RelayScope {
            app_namespace: "native-relay-test".to_owned(),
            storage_namespace: "default".to_owned(),
            auth_scope: auth_scope.map(str::to_owned),
        },
        sqlite_path: path,
        schema: schema(),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: AuthorSubject::for_test_bytes([0xa2; 16]),
        },
        thread_start_counter: None,
    }
}

#[test]
fn public_relay_open_rejects_a_planted_groove_only_manifest() {
    // This is an internal physical-admission receipt. The public relay
    // opener must not silently adopt a root that was initialized through
    // SQLite's generic Groove-only convenience API.
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relay.sqlite");
    let config = config(path.clone(), Some("alice"));
    legacy_storage::plant(&path, "groove_only");

    let registry = NativeRelayRegistry::default();
    assert!(
        registry.open(config).is_err(),
        "the public relay open must reject a manifest that omits Jazz codecs"
    );
}

#[test]
fn attached_client_identity_preserves_the_admitted_canonical_author() {
    let admitted = AuthorSubject::authenticated("https://issuer.example", "opaque-subject")
        .expect("fixture issuer and subject are valid");

    let client = fresh_client_identity(admitted).expect("OS entropy mints foreground node");

    assert_eq!(client.author, admitted);
    assert_eq!(
        client.author.canonical(),
        r#"["https://issuer.example","opaque-subject"]"#
    );
    assert_ne!(client.author, AuthorSubject::SYSTEM);
}

#[test]
fn foreground_identity_is_fresh_across_fixed_clock_process_restarts() {
    let author = AuthorSubject::for_test_bytes([0x42; 16]);
    // Model the first foreground of two independently restarted native
    // hosts. Both HLCs start at the same fixed physical position; their
    // transaction identities must nevertheless differ by fresh node.
    let before_restart = fresh_client_identity(author).expect("OS entropy is available");
    let after_restart = fresh_client_identity(author).expect("OS entropy is available");
    assert_ne!(before_restart.node, after_restart.node);
    let fixed_time = TxTime::from(1234);
    assert_ne!(
        TxId::new(fixed_time, before_restart.node),
        TxId::new(fixed_time, after_restart.node),
        "same-millisecond first writes after a process restart need distinct transaction ids"
    );

    // Planted old construction: a handle-derived JAZZRN prefix would
    // repeat after every host reset and makes the assertion above unsafe.
    let mut deterministic = [0_u8; 16];
    deterministic[..8].copy_from_slice(b"JAZZRN\0\0");
    deterministic[8..].copy_from_slice(&2_u64.to_be_bytes());
    let old_first_foreground = NodeUuid::from_bytes(deterministic);
    assert_ne!(before_restart.node, old_first_foreground);
    assert_ne!(after_restart.node, old_first_foreground);
}

// Cancellation has no public TypeScript one-shot API. This host-boundary
// test uses the existing test-only owner suspension to make contention
// deterministic, then observes successful cancellation and released pins.
// Internal scheduling receipt: JavaScript cannot deterministically suspend the
// semantic owner between native commands. All data and uploads still use core.
#[test]
fn permission_advice_route_requires_the_admitted_scope_author() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("advice-scope.sqlite"),
        Some("advice"),
    ))
    .unwrap();
    let identity = fresh_client_identity(AuthorSubject::for_test_bytes([0x47; 16])).unwrap();
    let generic = relay.attach_client(identity, BTreeMap::new()).unwrap();
    assert!(matches!(
        generic
            .request_foreground_permission_advice(ForegroundPermissionAdviceAction::Read {
                table: "todos".into(),
                row: [1; 16]
            })
            .unwrap(),
        ForegroundOperationPoll::Ready(ForegroundOperationResult::PermissionAdvice(
            ForegroundPermissionAdvice::Unknown
        ))
    ));
    assert!(matches!(
        relay.attach_foreground_client(
            identity,
            BTreeMap::new(),
            ForegroundNodeLease {
                node: identity.node,
                high_water: 0
            }
        ),
        Err(RelayError::ScopeConfigurationMismatch)
    ));
    generic.close().unwrap();
}

/// The owner command boundary makes a credential change deterministic before
/// any socket reply; public advice delivery must retire only changed claims.
#[test]
fn native_advice_claim_refresh_retires_only_changed_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("advice-refresh.sqlite"),
        Some("advice-refresh"),
    ))
    .unwrap();
    let identity = fresh_client_identity(relay.inner.identity.author).unwrap();
    let claims = BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]);
    let client = relay
        .attach_foreground_client(
            identity,
            claims.clone(),
            ForegroundNodeLease {
                node: identity.node,
                high_water: 0,
            },
        )
        .unwrap();
    let id = client.id;
    relay
        .run(move |worker| {
            worker.begin_socket_upstream();
            let ForegroundOperationPoll::Pending { operation } = worker
                .request_foreground_permission_advice(
                    id,
                    ForegroundPermissionAdviceAction::Read {
                        table: "todos".into(),
                        row: [1; 16],
                    },
                )?
            else {
                panic!("advice waits for authority");
            };
            worker.refresh_claims(identity.author, claims);
            assert!(matches!(
                worker.poll_foreground_operation(id, operation)?,
                ForegroundOperationPoll::Pending { .. }
            ));
            worker.refresh_claims(identity.author, BTreeMap::new());
            assert!(matches!(
                worker.poll_foreground_operation(id, operation)?,
                ForegroundOperationPoll::Ready(ForegroundOperationResult::PermissionAdvice(
                    ForegroundPermissionAdvice::Unknown
                ))
            ));
            Ok(())
        })
        .unwrap();
    client.close().unwrap();
}

/// Alice retires a just-installed socket and replaces it. A stale generation
/// cannot detach the replacement, and her admitted local write still settles.
/// The private owner-turn seam makes installation and retirement atomic with
/// respect to background drives; SQLite installs the connection synchronously.
#[test]
fn socket_install_cancellation_preserves_local_writes() {
    let (_directory, relay, client, _wake) = attached_write_client("socket-owner", 0x47);
    settle_owner(&relay);
    let id = client.id;
    let (generation, wire, committed) = relay
        .run(move |worker| {
            let start = std::time::Instant::now();
            let (generation, wire) = worker.begin_socket_upstream();
            assert!(start.elapsed() < std::time::Duration::from_millis(100));
            assert!(worker.upstream.is_some());
            let (committed, _) = worker.direct_foreground_mutation(
                id,
                ForegroundMutationKind::Insert,
                "todos".into(),
                None,
                encoded_title_cells("accepted before socket retirement"),
                "{}".into(),
            )?;
            assert!(worker.retire_socket_for_test(generation)?);
            assert!(worker.upstream.is_none());
            Ok((generation, wire, committed))
        })
        .unwrap();
    assert!(matches!(
        wire.take_outbound_encoded(),
        Err(RelayError::Closed)
    ));
    let (replacement, fresh) = relay
        .run(|worker| Ok(worker.begin_socket_upstream()))
        .unwrap();
    assert!(replacement > generation);
    assert!(
        !relay
            .run(move |worker| worker.retire_socket_for_test(generation))
            .unwrap()
    );
    assert!(fresh.take_outbound_encoded().is_ok());
    let mut settled = client
        .wait_for_foreground_transaction(*committed.as_bytes(), CoreDurabilityTier::Local)
        .unwrap();
    for _ in 0..100 {
        let ForegroundOperationPoll::Pending { operation } = settled else {
            break;
        };
        relay.pump().unwrap();
        settled = client.poll_foreground_operation(operation).unwrap();
    }
    assert!(matches!(
        settled,
        ForegroundOperationPoll::Ready(ForegroundOperationResult::TransactionSettled(_))
    ));
    client.close().unwrap();
}

/// Alice retires one socket while the owner remains alive. Its frames are
/// refused, a fresh generation can connect, and owner close terminally retires
/// that replacement as well. The internal generation seam pins link lifetime.
#[test]
fn retired_connections_reject_frames_while_live_owner_can_reconnect() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("retired.sqlite"),
        Some("retired"),
    ))
    .unwrap();
    let (old_generation, old) = relay.begin_socket_upstream().unwrap();
    relay.retire_socket_upstream(old_generation).unwrap();
    let message = encode_sync_message(&SyncMessage::Unsubscribe { id: 0 }).unwrap();
    assert!(matches!(
        old.push_inbound_encoded(&message),
        Err(RelayError::Closed)
    ));
    assert!(matches!(
        old.take_outbound_encoded(),
        Err(RelayError::Closed)
    ));
    assert!(relay.is_alive());
    let (new_generation, fresh) = relay.begin_socket_upstream().unwrap();
    assert!(new_generation > old_generation);
    assert!(fresh.take_outbound_encoded().is_ok());
    relay.inner.shutdown().unwrap();
    assert!(matches!(
        fresh.push_inbound_encoded(&message),
        Err(RelayError::Closed)
    ));
    assert!(matches!(
        fresh.take_outbound_encoded(),
        Err(RelayError::Closed)
    ));
    assert!(matches!(
        relay.begin_socket_upstream(),
        Err(RelayError::Closed)
    ));
}

#[test]
fn retired_socket_wire_rejects_stale_advice_and_cannot_detach_successor() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("socket-epoch.sqlite"),
        Some("socket-epoch"),
    ))
    .unwrap();
    let (first, stale) = relay
        .run(|worker| Ok(worker.begin_socket_upstream()))
        .unwrap();
    let (second, fresh) = relay
        .run(|worker| Ok(worker.begin_socket_upstream()))
        .unwrap();
    assert!(second > first);
    assert!(matches!(
        stale.push_inbound(SyncMessage::Advice {
            id: 1,
            advice: jazz::protocol::PermissionAdvice::Allowed,
        }),
        Err(RelayError::Closed)
    ));
    assert!(matches!(
        stale.take_outbound_encoded(),
        Err(RelayError::Closed)
    ));
    assert!(
        !relay
            .run(move |worker| worker.retire_socket_for_test(first))
            .unwrap()
    );
    assert!(fresh.take_outbound_encoded().is_ok());
    assert!(
        relay
            .run(move |worker| worker.retire_socket_for_test(second))
            .unwrap()
    );
    assert!(matches!(
        fresh.take_outbound_encoded(),
        Err(RelayError::Closed)
    ));
}

/// Hold Alice's real owner queue before submitting commands. The synchronous
/// public ABI cannot hold an admitted response, so this fixture uses the same
/// private enqueue path as `run` to model a caller abandoning its receiver.
struct HeldOwner(Option<mpsc::Sender<()>>);
impl Drop for HeldOwner {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}
fn hold_owner(relay: &NativeRelay) -> HeldOwner {
    let (started, ready) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    drop(
        relay
            .enqueue(move |_| {
                let _ = started.send(());
                let _ = wait.recv();
                Ok(())
            })
            .unwrap(),
    );
    ready.recv_timeout(Duration::from_secs(1)).unwrap();
    HeldOwner(Some(release))
}
fn begin_streaming_fixture(client: &NativeRelayClient, row: u8) -> u64 {
    let descriptor = RecordDescriptor::new(std::iter::empty::<(&str, ValueType)>());
    let raw = descriptor.create(&[]).unwrap();
    let response = client
        .execute(ForegroundDbCommandRequest::BeginStreamingMutation {
            mutation: ForegroundMutationKind::Insert,
            table: "todos".into(),
            row_id: [row; 16],
            cells: encoded_cells(descriptor, raw),
            column: "title".into(),
            options_json: "{}".into(),
        })
        .unwrap();
    let ForegroundDbCommandResponse::StreamingMutationOpened { upload } = response else {
        panic!("streaming mutation must open: {response:?}");
    };
    upload
}
fn queue_streaming_fixture(
    client: &NativeRelayClient,
    command: ForegroundDbCommandRequest,
) -> mpsc::Receiver<Result<ForegroundDbCommandResponse, RelayError>> {
    let id = client.id;
    client
        .relay
        .enqueue(move |worker| worker.execute_foreground(id, command))
        .unwrap()
}
fn assert_streaming_released(client: &NativeRelayClient) {
    let id = client.id;
    client
        .relay
        .run(move |worker| {
            let foreground = worker.client(id)?;
            assert!(foreground.uploads.is_empty());
            assert!(foreground.staged.is_empty());
            assert_eq!(
                block_on(foreground.binding.db.pending_upload_count_for_test()).unwrap(),
                0
            );
            Ok(())
        })
        .unwrap();
}
fn streaming_rows(client: &NativeRelayClient) -> Vec<RowUuid> {
    owner_driven_rows(
        client,
        client
            .start_foreground_read(
                postcard::to_allocvec(&Query::from("todos")).unwrap(),
                "{}".into(),
                None,
            )
            .unwrap(),
    )
}

/// Alice closes before her queued push executes. The real owner queue is
/// held to order close -> push; Bob's sibling remains usable and sees no row.
/// Inspecting the enqueue receipt is needed to model an admitted later push.
#[test]
fn streaming_push_waits_for_its_upload_slot_and_close_cancels_it() {
    let (_directory, relay, client, _wake) = attached_write_client("upload-owner", 0x47);
    settle_owner(&relay);
    let sibling = relay
        .attach_client(
            fresh_client_identity(AuthorSubject::for_test_bytes([0x47; 16])).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    let upload = begin_streaming_fixture(&client, 0x49);
    let held = hold_owner(&relay);
    let id = client.id;
    let close = relay.enqueue(move |worker| worker.detach(id)).unwrap();
    let push = queue_streaming_fixture(
        &client,
        ForegroundDbCommandRequest::PushStreamingMutation {
            upload,
            chunk: vec![b'x'; 65536],
        },
    );
    assert!(matches!(push.try_recv(), Err(mpsc::TryRecvError::Empty)));
    drop(held);
    close.recv_timeout(Duration::from_secs(1)).unwrap().unwrap();
    assert!(push.recv_timeout(Duration::from_secs(1)).unwrap().is_err());
    relay
        .pump()
        .expect("relay remains usable after unfinished upload teardown");
    assert!(streaming_rows(&sibling).is_empty());
    sibling.close().unwrap();
}

/// Alice queues a push followed by abort. Both execute once on the owner;
/// a later push cannot resurrect the aborted upload. The queue hold exposes
/// ordering without restoring the old asynchronous per-upload lock.
#[test]
fn streaming_abort_during_pending_push_cannot_resurrect_upload() {
    pending_push_abort_receipt(false);
}

/// Alice drops the receipt for an admitted push, then aborts. Abandoning the
/// receipt cannot lose cleanup or permit a subsequent push to resurrect it.
#[test]
fn cancelled_streaming_push_remains_abortable() {
    pending_push_abort_receipt(true);
}

fn pending_push_abort_receipt(cancel_push: bool) {
    let (_directory, relay, client, _wake) = attached_write_client("upload-owner", 0x47);
    settle_owner(&relay);
    let upload = begin_streaming_fixture(&client, 0x49);
    let held = hold_owner(&relay);
    let push = queue_streaming_fixture(
        &client,
        ForegroundDbCommandRequest::PushStreamingMutation {
            upload,
            chunk: vec![b'x'; 65536],
        },
    );
    let abort = queue_streaming_fixture(
        &client,
        ForegroundDbCommandRequest::AbortStreamingMutation { upload },
    );
    let later_push = queue_streaming_fixture(
        &client,
        ForegroundDbCommandRequest::PushStreamingMutation {
            upload,
            chunk: vec![1],
        },
    );
    assert!(matches!(push.try_recv(), Err(mpsc::TryRecvError::Empty)));
    let push = if cancel_push {
        drop(push);
        None
    } else {
        Some(push)
    };
    drop(held);
    if let Some(push) = push {
        assert_eq!(
            push.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
            ForegroundDbCommandResponse::StreamingMutationPushed
        );
    }
    assert_eq!(
        abort.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
        ForegroundDbCommandResponse::StreamingMutationAborted { aborted: true }
    );
    let later = later_push
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert!(
        matches!(later, ForegroundDbCommandResponse::OperationError { ref reason }
        if reason.contains("unknown streaming mutation")),
        "{later:?}"
    );
    assert_eq!(
        client
            .execute(ForegroundDbCommandRequest::AbortStreamingMutation { upload })
            .unwrap(),
        ForegroundDbCommandResponse::StreamingMutationAborted { aborted: false }
    );
    assert_streaming_released(&client);
    assert!(streaming_rows(&client).is_empty());
    client.close().unwrap();
}

/// Alice abandons queued finish/abort receipts. Every admitted operation
/// still executes and releases its persisted upload lease. Finished rows
/// survive; aborted rows do not. The private receipt seam models observer loss.
#[test]
fn cancelled_upload_results_finish_cleanup_without_pinning_capacity() {
    let (_directory, relay, client, _wake) = attached_write_client("cancel-upload", 0x51);
    settle_owner(&relay);
    let mut expected = Vec::new();
    for finish in [false, true] {
        for round in 0..4 {
            let row = if finish { 60 + round } else { 70 + round };
            let upload = begin_streaming_fixture(&client, row);
            assert_eq!(
                client
                    .execute(ForegroundDbCommandRequest::PushStreamingMutation {
                        upload,
                        chunk: vec![b'x'; 65536],
                    })
                    .unwrap(),
                ForegroundDbCommandResponse::StreamingMutationPushed
            );
            let held = hold_owner(&relay);
            let terminal = queue_streaming_fixture(
                &client,
                if finish {
                    ForegroundDbCommandRequest::FinishStreamingMutation { upload }
                } else {
                    ForegroundDbCommandRequest::AbortStreamingMutation { upload }
                },
            );
            assert!(matches!(
                terminal.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            drop(terminal);
            drop(held);
            // This queued inspection is a barrier after the abandoned response.
            assert_streaming_released(&client);
            if finish {
                expected.push(RowUuid::from_bytes([row; 16]));
            }
        }
    }
    let mut rows = streaming_rows(&client);
    rows.sort();
    expected.sort();
    assert_eq!(rows, expected);
    client.close().unwrap();
}

#[test]
fn mutation_command_rejects_unsupported_target_options_before_admission() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("mutation-options.sqlite"),
        Some("options"),
    ))
    .unwrap();
    let client = relay
        .attach_client(
            fresh_client_identity(AuthorSubject::for_test_bytes([0x52; 16])).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    let id = client.id;
    let tx = client
        .begin_foreground_transaction(ForegroundTransactionKind::Mergeable)
        .unwrap();
    for (mutation, key) in [
        (ForegroundMutationKind::Insert, "head"),
        (ForegroundMutationKind::Restore, "base"),
        (ForegroundMutationKind::Update, "branch"),
        (ForegroundMutationKind::Delete, "branch"),
        (ForegroundMutationKind::Upsert, "branch"),
    ] {
        for value in ["null", "\"draft\""] {
            let options = format!("{{\"{key}\":{value}}}");
            let direct_options = options.clone();
            let error = relay
                .run(move |worker| {
                    worker.direct_foreground_mutation(
                        id,
                        mutation,
                        "todos".into(),
                        Some([0x53; 16]),
                        encoded_title_cells("must not write"),
                        direct_options,
                    )
                })
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&format!("option `{key}` is not supported"))
            );
            let error = client
                .stage_foreground_mutation(
                    tx,
                    mutation,
                    "todos".into(),
                    Some([0x53; 16]),
                    encoded_title_cells("must not write"),
                    options,
                )
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&format!("option `{key}` is not supported"))
            );
        }
    }
    client.close().unwrap();
}

#[test]
fn ordinary_multi_large_scalar_insert_settles_through_native_relay() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(directory.path().join("multi-large.sqlite"), Some("large"));
    config.schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .column("body", ColumnType::Text)
                    .column("payload", ColumnType::Bytea)
                    .column("metadata", ColumnType::Json { schema: None })
                    .column("done", ColumnType::Boolean),
            )
            .build(),
    )
    .unwrap();
    config.identity.author = AuthorSubject::for_test_bytes([0x54; 16]);
    let relay = NativeRelayRegistry::default()
        .open_admitted(config)
        .unwrap();
    let client = relay
        .attach_client(
            fresh_client_identity(AuthorSubject::for_test_bytes([0x54; 16])).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    let id = client.id;
    let (tx_id, _) = relay
        .run(move |worker| {
            let descriptor = RecordDescriptor::new([
                ("body", ValueType::String),
                ("payload", ValueType::Bytes),
                ("metadata", ValueType::String),
                ("done", ValueType::Bool),
            ]);
            let prefix = "a".repeat(70000);
            let json_prefix = prefix.clone();
            let mut bytes = vec![7; 70006];
            bytes[70000..].copy_from_slice(&[0, 1, 2, 3, 4, 5]);
            let raw = descriptor
                .create(&[
                    Value::String(format!("{prefix}A😀BC")),
                    Value::Bytes(bytes),
                    Value::String(format!(
                        "{{\"padding\":\"{json_prefix}\",\"nested\":{{\"answer\":42}}}}"
                    )),
                    Value::Bool(false),
                ])
                .unwrap();
            worker.direct_foreground_mutation(
                id,
                ForegroundMutationKind::Insert,
                "documents".into(),
                None,
                encoded_cells(descriptor, raw),
                "{}".into(),
            )
        })
        .unwrap();
    let mut wait = client
        .wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local)
        .unwrap();
    for _ in 0..100 {
        let ForegroundOperationPoll::Pending { operation } = wait else {
            break;
        };
        relay
            .pump()
            .expect("multi-large local settlement must preserve the underlying relay error");
        wait = client.poll_foreground_operation(operation).unwrap();
    }
    assert!(matches!(
        wait,
        ForegroundOperationPoll::Ready(ForegroundOperationResult::TransactionSettled(_))
    ));
    client.close().unwrap();
}

/// A foreground with a registered platform wake callback, which is how
/// every RN host runs it: its Db tick requests reach the owner as drives.
fn attached_write_client(
    name: &str,
    author: u8,
) -> (
    tempfile::TempDir,
    NativeRelay,
    NativeRelayClient,
    Arc<QueuedNativeWake>,
) {
    let directory = tempfile::tempdir().unwrap();
    let mut relay_config = config(directory.path().join(format!("{name}.sqlite")), Some(name));
    relay_config.identity.author = AuthorSubject::for_test_bytes([author; 16]);
    let relay = NativeRelayRegistry::default()
        .open_admitted(relay_config)
        .unwrap();
    let client = relay
        .attach_client(
            fresh_client_identity(AuthorSubject::for_test_bytes([author; 16])).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    let wake = Arc::new(QueuedNativeWake::active());
    client
        .set_foreground_wake_callback(
            1,
            Some(Arc::new(ForegroundWakeState::new(
                ForegroundWakeRegistration {
                    callback: queue_native_wake,
                    context: Arc::as_ptr(&wake) as usize,
                },
            ))),
        )
        .unwrap();
    (directory, relay, client, wake)
}

/// A real relay/authority connection with the admitted test identity whose authority is driven
/// only by the test. Holding its replies needs no simulated owner lock.
fn attached_transaction_authority(
    name: &str,
    author_byte: u8,
) -> (
    tempfile::TempDir,
    NativeRelay,
    NativeRelayClient,
    Rc<jazz::db::Db>,
) {
    let directory = tempfile::tempdir().unwrap();
    let author = AuthorSubject::for_test_bytes([author_byte; 16]);
    let mut relay_config = config(directory.path().join(format!("{name}.sqlite")), Some(name));
    relay_config.schema = permissive_schema();
    relay_config.identity.author = author;
    let relay = NativeRelayRegistry::default()
        .open_admitted(relay_config)
        .unwrap();
    let client = relay
        .attach_client(fresh_client_identity(author).unwrap(), BTreeMap::new())
        .unwrap();
    let authority = Rc::new(
        block_on(jazz::db::Db::open_history_complete(DbConfig::new(
            permissive_schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xe9; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap(),
    );
    let (_, wire) = relay
        .run(|worker| Ok(worker.begin_socket_upstream()))
        .unwrap();
    authority.accept_subscriber(
        Box::new(WireTransportAdapter::current(AuthorityWire::new(wire))),
        author,
    );
    for _ in 0..16 {
        block_on(authority.tick()).unwrap();
        relay.pump().unwrap();
    }
    (directory, relay, client, authority)
}

/// Complete a native operation through real transport drives. Omitting the
/// authority proves that owner-local reads and receipts need no server reply.
fn complete_transaction_operation(
    relay: &NativeRelay,
    client: &NativeRelayClient,
    mut poll: ForegroundOperationPoll,
    authority: Option<&jazz::db::Db>,
) -> ForegroundOperationResult {
    for _ in 0..100 {
        match poll {
            ForegroundOperationPoll::Ready(result) => return result,
            ForegroundOperationPoll::Pending { operation } => {
                if let Some(authority) = authority {
                    block_on(authority.tick()).unwrap();
                }
                relay.pump().unwrap();
                poll = client.poll_foreground_operation(operation).unwrap();
            }
            ForegroundOperationPoll::Error { code, reason } => {
                panic!("transaction operation failed: {code:?}: {reason}");
            }
        }
    }
    match poll {
        ForegroundOperationPoll::Ready(result) => result,
        ForegroundOperationPoll::Error { code, reason } => {
            panic!("transaction operation failed: {code:?}: {reason}");
        }
        ForegroundOperationPoll::Pending { .. } => {
            panic!("transaction operation did not complete within 100 transport drives");
        }
    }
}

fn admit_title(
    relay: &NativeRelay,
    id: u64,
    title: String,
) -> Result<(TransactionId, RowUuid), RelayError> {
    relay.run(move |worker| {
        worker.direct_foreground_mutation(
            id,
            ForegroundMutationKind::Insert,
            "todos".into(),
            None,
            encoded_title_cells(&title),
            "{}".into(),
        )
    })
}

/// Poll a foreground read without any platform tick or explicit pump:
/// only the owner's own drive turns may make it progress.
fn owner_driven_rows(
    client: &NativeRelayClient,
    mut read: ForegroundOperationPoll,
) -> Vec<RowUuid> {
    for _ in 0..2_000 {
        match read {
            ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(rows)) => {
                let batches: Vec<DecodedForegroundRowBatch> = postcard::from_bytes(&rows).unwrap();
                return batches
                    .into_iter()
                    .flat_map(|batch| batch.rows.into_iter().map(|row| row.row_id))
                    .collect();
            }
            ForegroundOperationPoll::Pending { operation } => {
                std::thread::sleep(std::time::Duration::from_millis(1));
                read = client.poll_foreground_operation(operation).unwrap();
            }
            _ => panic!("foreground read failed"),
        }
    }
    panic!("owner drive turns did not complete the foreground read");
}

/// Emulate the RN JS reactor for one subscription: only a queued platform
/// wake makes JS tick and drain (until an empty batch); nothing polls on a
/// timer. Returns the first delivered frame that contains `row`, or `None`
/// when no further wake arrives within `quiet` (a lost wake).
fn wake_driven_frame_with_row(
    client: &NativeRelayClient,
    wake: &QueuedNativeWake,
    subscription: u64,
    row: RowUuid,
    quiet: Duration,
) -> Option<usize> {
    let mut drains = 0usize;
    let mut pending: Option<u64> = None;
    loop {
        // One JS turn: tick, then drain until an empty batch or a pending op.
        client.pump_foreground().unwrap();
        loop {
            drains += 1;
            let poll = match pending.take() {
                Some(operation) => client.poll_foreground_operation(operation).unwrap(),
                None => client.drain_foreground_subscription(subscription).unwrap(),
            };
            match poll {
                ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(
                    events,
                )) => {
                    if events.is_empty() {
                        break;
                    }
                    if events.iter().any(|event| match event {
                        ForegroundSubscriptionEvent::Delta { delta, .. }
                        | ForegroundSubscriptionEvent::StructuredDelta { delta, .. } => delta
                            .windows(16)
                            .any(|candidate| candidate == row.as_bytes()),
                        _ => false,
                    }) {
                        return Some(drains);
                    }
                }
                ForegroundOperationPoll::Pending { operation } => {
                    // JS retries a pending drain after a setTimeout(0).
                    pending = Some(operation);
                    std::thread::sleep(Duration::from_millis(1));
                }
                _ => panic!("subscription drain failed"),
            }
        }
        // Park like the idle JS thread until the platform delivers a wake.
        let deadline = std::time::Instant::now() + quiet;
        loop {
            if !std::mem::take(&mut *wake.queued.lock().unwrap()).is_empty() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

// Internal receipt (#3273, device typing-composer open stall): a fresh
// foreground opens a subscription and then admits a write in the same JS
// turn. Driven only by platform wakes, the write must reach that
// subscription; a lost wake would park JS forever.
fn attach_with_wake(relay: &NativeRelay) -> (NativeRelayClient, Arc<QueuedNativeWake>) {
    let client = relay
        .attach_client(
            fresh_client_identity(relay.inner.identity.author).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    let wake = Arc::new(QueuedNativeWake::active());
    client
        .set_foreground_wake_callback(
            1,
            Some(Arc::new(ForegroundWakeState::new(
                ForegroundWakeRegistration {
                    callback: queue_native_wake,
                    context: Arc::as_ptr(&wake) as usize,
                },
            ))),
        )
        .unwrap();
    (client, wake)
}

#[test]
fn same_turn_subscribe_then_insert_on_a_used_scope_reaches_subscription_through_wakes() {
    let (_directory, relay, sibling, _sibling_wake) = attached_write_client("wake-used", 0x40);
    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
    admit_title(&relay, sibling.id, "seed".into()).unwrap();
    for round in 0..30u8 {
        // An earlier public client: subscribe, observe a write, close.
        let (earlier, earlier_wake) = attach_with_wake(&relay);
        let subscription = earlier
            .subscribe_foreground_query_with_options(query.clone(), ReadOpts::default())
            .unwrap();
        let (_, row) = admit_title(&relay, earlier.id, format!("earlier-{round}")).unwrap();
        assert!(
            wake_driven_frame_with_row(
                &earlier,
                &earlier_wake,
                subscription,
                row,
                Duration::from_secs(2)
            )
            .is_some(),
            "round {round}: earlier client never observed its write"
        );
        earlier.close().unwrap();
        // The composer client: subscribe, then insert in the same turn.
        let (composer, composer_wake) = attach_with_wake(&relay);
        let subscription = composer
            .subscribe_foreground_query_with_options(query.clone(), ReadOpts::default())
            .unwrap();
        let (_, row) = admit_title(&relay, composer.id, String::new()).unwrap();
        assert!(
            wake_driven_frame_with_row(
                &composer,
                &composer_wake,
                subscription,
                row,
                Duration::from_secs(2)
            )
            .is_some(),
            "round {round}: the same-turn write never reached its subscription through wakes"
        );
        composer.close().unwrap();
    }
    sibling.close().unwrap();
}

#[test]
fn same_turn_subscribe_then_insert_reaches_subscription_through_wakes_alone() {
    for round in 0..40u8 {
        let (_directory, relay, client, wake) =
            attached_write_client(&format!("wake-open-{round}"), 0x70 + (round % 8));
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        let subscription = client
            .subscribe_foreground_query_with_options(query, ReadOpts::default())
            .unwrap();
        let (_tx, row) = admit_title(&relay, client.id, String::new()).unwrap();
        assert!(
            wake_driven_frame_with_row(&client, &wake, subscription, row, Duration::from_secs(2))
                .is_some(),
            "round {round}: the same-turn write never reached its subscription through wakes"
        );
        client.close().unwrap();
    }
}

/// Alice's direct write is applied before the native command returns, and
/// both her next read and the first subscription frame must contain it.
/// The owner-turn inspection is internal because public calls can interleave
/// owner drive turns and cannot distinguish application from later delivery.
#[test]
fn direct_mutations_apply_before_return_and_fence_reads_and_subscriptions() {
    let (_directory, relay, client, _wake) = attached_write_client("fence", 0x61);
    let id = client.id;
    let row_id = relay
        .run(move |worker| {
            let (_, row) = worker.direct_foreground_mutation(
                id,
                ForegroundMutationKind::Insert,
                "todos".into(),
                None,
                encoded_title_cells("fenced"),
                "{}".into(),
            )?;
            // The same owner turn cannot interleave a later drive. SQLite
            // must already expose the applied local write at this boundary.
            let db = &worker.client(id)?.binding.db;
            let query = db
                .prepare_query(&Query::from("todos"))
                .map_err(RelayError::Db)?;
            let rows = block_on(db.all(
                &query,
                ReadOpts::for_read_tier(jazz::db::ReadTier::LocalOnly),
            ))
            .map_err(RelayError::Db)?;
            assert_eq!(
                rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
                vec![row]
            );
            Ok(row)
        })
        .unwrap();
    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
    let read = client
        .start_foreground_read(query.clone(), "{}".into(), None)
        .unwrap();
    let subscription = client
        .subscribe_foreground_query_with_options(query, ReadOpts::default())
        .unwrap();

    assert_eq!(owner_driven_rows(&client, read), vec![row_id]);
    let mut first_frame = None;
    let mut pending = None;
    for _ in 0..2_000 {
        let poll = match pending.take() {
            Some(operation) => client.poll_foreground_operation(operation).unwrap(),
            None => client.drain_foreground_subscription(subscription).unwrap(),
        };
        match poll {
            ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(
                events,
            )) if !events.is_empty() => {
                first_frame = Some(events);
                break;
            }
            ForegroundOperationPoll::Pending { operation } => pending = Some(operation),
            ForegroundOperationPoll::Ready(_) => {}
            _ => panic!("subscription drain failed"),
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let first_frame = first_frame.expect("the owner delivers the first subscription frame");
    assert!(
        first_frame.iter().any(|event| match event {
            ForegroundSubscriptionEvent::Delta { delta, .. }
            | ForegroundSubscriptionEvent::StructuredDelta { delta, .. } => delta
                .windows(16)
                .any(|candidate| candidate == row_id.as_bytes()),
            _ => false,
        }),
        "the first frame of a same-turn subscription contains the write"
    );
    client.close().unwrap();
}

/// Alice's write is refused when the bounded native command queue is full.
/// Releasing the owner preserves every admitted row and allows another write;
/// the refused row must never appear. Blocking the owner is an internal seam
/// because a public caller cannot deterministically fill its command queue.
#[test]
fn direct_mutation_command_queue_rejects_without_admitting_when_full() {
    let (_directory, relay, client, _wake) = attached_write_client("cap-held", 0x62);
    let id = client.id;
    let mut admitted = Vec::new();
    for index in 0..16 {
        admitted.push(
            admit_title(&relay, id, format!("before-full-{index}"))
                .unwrap()
                .1,
        );
    }
    let saturated = saturate_owner(&relay);
    assert!(matches!(
        admit_title(&relay, id, "over the cap".into()),
        Err(RelayError::OwnerQueueFull)
    ));
    saturated.release_and_wait();
    admitted.push(admit_title(&relay, id, "after-drain".into()).unwrap().1);
    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
    let read = client
        .start_foreground_read(query, "{}".into(), None)
        .unwrap();
    let mut rows = owner_driven_rows(&client, read);
    rows.sort();
    admitted.sort();
    assert_eq!(rows, admitted, "only admitted writes are applied");
    client.close().unwrap();
}

/// Alice admits a burst in one owner turn. Every synchronous SQLite mutation
/// must appear exactly once in her next read. The internal owner-turn seam
/// prevents background delivery from hiding lost or deferred applications.
#[test]
fn direct_mutation_burst_loses_nothing_in_one_owner_turn() {
    let (_directory, relay, client, _wake) = attached_write_client("cap-burst", 0x63);
    let id = client.id;
    let burst = 192;
    let mut admitted = relay
        .run(move |worker| {
            let mut admitted = Vec::new();
            for index in 0..burst {
                let (_, row) = worker.direct_foreground_mutation(
                    id,
                    ForegroundMutationKind::Insert,
                    "todos".into(),
                    None,
                    encoded_title_cells(&format!("burst-{index}")),
                    "{}".into(),
                )?;
                admitted.push(row);
            }
            Ok(admitted)
        })
        .unwrap();
    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
    let read = client
        .start_foreground_read(query, "{}".into(), None)
        .unwrap();
    let mut rows = owner_driven_rows(&client, read);
    rows.sort();
    admitted.sort();
    assert_eq!(rows.len(), burst);
    assert_eq!(rows, admitted, "no write is lost or duplicated");
    client.close().unwrap();
}

/// Alice sees her synchronous mutation before the storage owner acknowledges
/// Local durability. The private owner turn prevents a background drive from
/// acknowledging the write before the pending receipt and preview are checked.
#[test]
fn standalone_mutation_is_visible_before_owner_acknowledgement() {
    let (_directory, relay, client, _wake) = attached_write_client("write-owner", 0x48);
    settle_owner(&relay);
    let id = client.id;
    let (tx_id, row_id) = relay
        .run(move |worker| {
            let (tx_id, row_id) = worker.direct_foreground_mutation(
                id,
                ForegroundMutationKind::Insert,
                "todos".into(),
                None,
                encoded_title_cells("local preview"),
                "{}".into(),
            )?;
            assert_eq!(
                worker.foreground_write_state(id, *tx_id.as_bytes())?,
                "{\"fate\":\"Pending\",\"global_time\":null,\"durability\":\"None\"}"
            );
            let rows = worker
                .client(id)?
                .binding
                .local_current_row("todos", row_id.as_bytes())
                .map_err(RelayError::Host)?;
            assert_exact_todo_rows(&rows, row_id, "local preview");
            Ok((tx_id, row_id))
        })
        .unwrap();
    let wait = client
        .wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local)
        .unwrap();
    assert!(matches!(
        complete_transaction_operation(&relay, &client, wait, None),
        ForegroundOperationResult::TransactionSettled(settled) if settled == tx_id
    ));
    let rows = client
        .local_current_foreground_row("todos".into(), *row_id.as_bytes())
        .unwrap();
    assert_exact_todo_rows(&rows, row_id, "local preview");
    let read = client
        .start_foreground_read(
            postcard::to_allocvec(&Query::from("todos")).unwrap(),
            "{}".into(),
            None,
        )
        .unwrap();
    let ForegroundOperationResult::Rows(rows) =
        complete_transaction_operation(&relay, &client, read, None)
    else {
        panic!("acknowledged row must remain readable");
    };
    assert_exact_todo_rows(&rows, row_id, "local preview");
    client.close().unwrap();
}

// Internal receipt: the reported hint is the core read gate's input, which
// no public surface exposes. A plain relay client's own upstream is the
// relay, so it must see the relay's server link, not an always-live peer.
#[test]
fn plain_relay_clients_see_the_relay_server_link() {
    let directory = tempfile::tempdir().unwrap();
    let relay =
        NativeRelay::spawn(config(directory.path().join("link.sqlite"), Some("link"))).unwrap();
    let client = relay
        .attach_client(
            fresh_client_identity(AuthorSubject::for_test_bytes([0x4a; 16])).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    let id = client.id;
    let hint = |relay: &NativeRelay| {
        relay
            .run(move |worker| Ok(worker.client(id)?.binding.db.remote_link_hint_for_test()))
            .unwrap()
    };
    assert_eq!(hint(&relay), Some(RemoteLinkHint::NoServer));

    let generation = relay
        .run(|worker| Ok(worker.begin_socket_upstream().0))
        .unwrap();
    relay.pump().unwrap();
    assert!(matches!(
        hint(&relay),
        Some(RemoteLinkHint::Attempting | RemoteLinkHint::Live)
    ));
    for _ in 0..10 {
        if hint(&relay) == Some(RemoteLinkHint::Live) {
            break;
        }
        relay.pump().unwrap();
    }
    assert_eq!(hint(&relay), Some(RemoteLinkHint::Live));

    assert!(
        relay
            .run(move |worker| worker.retire_socket_for_test(generation))
            .unwrap()
    );
    relay.pump().unwrap();
    assert_eq!(hint(&relay), Some(RemoteLinkHint::NoServer));
    client.close().unwrap();
}

// Internal receipt: see above. A host foreground whose scope has no native
// socket session has no server its reads could wait for.
#[test]
fn host_foreground_without_a_socket_session_reports_no_server() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("no-socket.sqlite"),
        "no-socket",
        &permissive_schema(),
        0xc7,
    );
    let foreground = fixture.open_foreground(&capability);
    fixture.execute(
        foreground,
        ForegroundDbCommandRequest::NativeSessionMetadata,
    );
    let client = unsafe { &*fixture.host }
        .inner
        .lock()
        .unwrap()
        .foreground_client(foreground)
        .unwrap()
        .clone();
    let id = client.id;
    assert_eq!(
        client
            .relay
            .run(move |worker| Ok(worker.client(id)?.binding.db.remote_link_hint_for_test()))
            .unwrap(),
        Some(RemoteLinkHint::NoServer)
    );
}

/// Alice reads a staged prefix, then stages a later value. Mergeable reads
/// recover the owner cache; serializable reads await Bob's authority reply.
/// The native seam controls reply delivery, which JS cannot hold explicitly.
/// stage queued -> admit read -> stage after-read -> finish read -> commit.
#[test]
fn explicit_transactions_preserve_read_order_before_commit() {
    for kind in [
        ForegroundTransactionKind::Mergeable,
        ForegroundTransactionKind::Serializable,
    ] {
        let (_directory, relay, client, authority) =
            attached_transaction_authority("tx-owner", 0x49);
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        let transaction = client.begin_foreground_transaction(kind).unwrap();
        let row = client
            .insert_foreground_transaction(
                transaction,
                "todos".into(),
                encoded_title_cells("first"),
                None,
            )
            .unwrap();
        client
            .stage_foreground_mutation(
                transaction,
                ForegroundMutationKind::Update,
                "todos".into(),
                Some(*row.as_bytes()),
                encoded_title_cells("queued"),
                "{}".into(),
            )
            .unwrap();
        let read = client
            .start_foreground_read(query.clone(), "{}".into(), Some(transaction))
            .unwrap();
        assert!(matches!(read, ForegroundOperationPoll::Pending { .. }));
        client
            .update_foreground_transaction(
                transaction,
                "todos".into(),
                *row.as_bytes(),
                encoded_title_cells("after-read"),
            )
            .unwrap();
        let ForegroundOperationResult::Rows(rows) =
            complete_transaction_operation(&relay, &client, read, Some(&authority))
        else {
            panic!("transaction read did not return rows");
        };
        assert_exact_todo_rows(&rows, row, "queued");
        // SQLite consumes and refuses a commit with an unfinished read.
        // native_failed_commits_release_transaction_capacity covers that
        // boundary through the public C ABI. Here the read must finish first.
        let tx_id = client.commit_foreground_transaction(transaction).unwrap();
        let wait = client
            .wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local)
            .unwrap();
        assert!(matches!(
            complete_transaction_operation(&relay, &client, wait, None),
            ForegroundOperationResult::TransactionSettled(settled) if settled == tx_id
        ));
        let committed = client
            .start_foreground_read(query, "{}".into(), None)
            .unwrap();
        let ForegroundOperationResult::Rows(rows) =
            complete_transaction_operation(&relay, &client, committed, None)
        else {
            panic!("committed row absent");
        };
        assert_exact_todo_rows(&rows, row, "after-read");
        client.close().unwrap();
    }
}

/// Alice cancels a serializable read awaiting Bob's authority. Cancelling
/// releases its real commit fence; commit and rollback remain usable without
/// an authority reply. The native seam holds transport delivery deterministically.
#[test]
fn cancelled_transaction_read_releases_commit_fence_and_rollback_is_bounded() {
    for rollback in [false, true] {
        let (_directory, relay, client, _authority) =
            attached_transaction_authority("tx-cancel", 0x4a);
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        let tx = client
            .begin_foreground_transaction(ForegroundTransactionKind::Serializable)
            .unwrap();
        let row = client
            .insert_foreground_transaction(
                tx,
                "todos".into(),
                encoded_title_cells("cancelled observer"),
                None,
            )
            .unwrap();
        let read = client
            .start_foreground_read(query.clone(), "{}".into(), Some(tx))
            .unwrap();
        let ForegroundOperationPoll::Pending { operation: read } = read else {
            panic!("serializable read must await the withheld authority reply");
        };
        let started = std::time::Instant::now();
        assert!(client.cancel_foreground_operation(read).unwrap());
        let committed = if rollback {
            assert!(client.rollback_foreground_transaction(tx).unwrap());
            None
        } else {
            Some(client.commit_foreground_transaction(tx).unwrap())
        };
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "cancel followed by commit or rollback must not await the authority"
        );
        if let Some(tx_id) = committed {
            let wait = client
                .wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local)
                .unwrap();
            assert!(matches!(
                complete_transaction_operation(&relay, &client, wait, None),
                ForegroundOperationResult::TransactionSettled(settled) if settled == tx_id
            ));
        }
        let read = client
            .start_foreground_read(query, "{}".into(), None)
            .unwrap();
        let ForegroundOperationResult::Rows(rows) =
            complete_transaction_operation(&relay, &client, read, None)
        else {
            panic!("local read did not return rows");
        };
        if rollback {
            assert!(
                postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&rows)
                    .unwrap()
                    .iter()
                    .all(|b| b.rows.is_empty())
            );
        } else {
            assert_exact_todo_rows(&rows, row, "cancelled observer");
        }
        client.close().unwrap();
    }
}

/// Alice closes with an unfinished read, or cancels it and commits first.
/// Her Db allocation must retire despite a retained receipt, stale handles
/// must fail, and only the Local-acknowledged row may reach her sibling.
/// The private weak reference proves allocation release beyond API rejection.
#[test]
fn foreground_close_releases_pending_transactions_and_retained_receipts() {
    for committed in [false, true] {
        let (_directory, relay, client, _authority) =
            attached_transaction_authority("tx-close", 0x4b);
        let id = client.id;
        thread_local! { static CLOSED_DB: RefCell<std::rc::Weak<Db>> = const { RefCell::new(std::rc::Weak::new()) }; }
        relay
            .run(move |worker| {
                CLOSED_DB.with(|weak| {
                    *weak.borrow_mut() = Rc::downgrade(&worker.client(id).unwrap().binding.db)
                });
                Ok(())
            })
            .unwrap();
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        let tx = client
            .begin_foreground_transaction(ForegroundTransactionKind::Serializable)
            .unwrap();
        let row = client
            .insert_foreground_transaction(
                tx,
                "todos".into(),
                encoded_title_cells("closed foreground"),
                None,
            )
            .unwrap();
        let pending = client
            .start_foreground_read(query.clone(), "{}".into(), Some(tx))
            .unwrap();
        let ForegroundOperationPoll::Pending { operation: read } = pending else {
            panic!("serializable read must await the withheld authority reply");
        };
        // SQLite receipts contain value data, not a retained Db/WriteHandle.
        thread_local! { static CLOSED_WRITE: RefCell<Option<jazz::binding_host::Write>> = const { RefCell::new(None) }; }
        let committed_id = if committed {
            assert!(client.cancel_foreground_operation(read).unwrap());
            let tx_id = client.commit_foreground_transaction(tx).unwrap();
            relay
                .run(move |worker| {
                    CLOSED_WRITE.with(|write| {
                        *write.borrow_mut() = worker
                            .client(id)
                            .unwrap()
                            .writes
                            .get(&tx_id)
                            .map(|tracked| tracked.write.clone());
                        assert!(
                            write.borrow().is_some(),
                            "retain the actual binding receipt"
                        );
                    });
                    Ok(())
                })
                .unwrap();
            let wait = client
                .wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local)
                .unwrap();
            assert!(matches!(
                complete_transaction_operation(&relay, &client, wait, None),
                ForegroundOperationResult::TransactionSettled(settled) if settled == tx_id
            ));
            Some(tx_id)
        } else {
            None
        };
        let stale = client.clone();
        let closing = std::time::Instant::now();
        client.close().unwrap();
        assert!(closing.elapsed() < Duration::from_secs(1));
        let mut released = false;
        for _ in 0..100 {
            released = relay
                .run(|_| Ok(CLOSED_DB.with(|weak| weak.borrow().upgrade().is_none())))
                .unwrap();
            if released {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(
            released,
            "closed foreground must release its Db allocation without JS ticks"
        );
        assert!(matches!(
            stale.commit_foreground_transaction(tx),
            Err(RelayError::UnknownClient(closed)) if closed == id
        ));
        assert!(matches!(
            stale.poll_foreground_operation(read),
            Err(RelayError::UnknownClient(closed)) if closed == id
        ));
        if let Some(tx_id) = committed_id {
            assert!(matches!(
                stale.wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local),
                Err(RelayError::UnknownClient(closed)) if closed == id
            ));
            relay
                .run(move |worker| {
                    assert!(matches!(
                        worker.foreground_write_state(id, *tx_id.as_bytes()),
                        Err(RelayError::UnknownClient(closed)) if closed == id
                    ));
                    CLOSED_WRITE.with(|write| {
                        assert_eq!(write.borrow_mut().take().unwrap().tx_id, tx_id);
                    });
                    Ok(())
                })
                .unwrap();
        }
        let sibling = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x4b; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let read = sibling
            .start_foreground_read(query, "{}".into(), None)
            .unwrap();
        let ForegroundOperationResult::Rows(rows) =
            complete_transaction_operation(&relay, &sibling, read, None)
        else {
            panic!("sibling read stalled");
        };
        if committed {
            assert_exact_todo_rows(&rows, row, "closed foreground");
        } else {
            assert!(
                postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&rows)
                    .unwrap()
                    .iter()
                    .all(|b| b.rows.is_empty())
            );
        }
        sibling.close().unwrap();
    }
}

/// Fail the real upstream byte carrier. Socket failure must not substitute
/// for a durable local receipt; writes recover through the durable journal.
fn fail_upstream_fixture(relay: &NativeRelay) {
    struct FailedPeer;
    impl WireTransport for FailedPeer {
        fn send_frame(&mut self, _: Vec<u8>) -> Result<(), TransportError> {
            Err(TransportError::Failed("controlled peer I/O failure".into()))
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            None
        }
    }
    let (_, wire) = relay.begin_socket_upstream().unwrap();
    relay.pump().unwrap();
    assert!(
        wire.queue_depths().1 > 0,
        "the real core supplied an upstream frame"
    );
    assert!(matches!(
        bridge_native_relay_wire_once_classified(
            &wire,
            &mut NativeRelayTransport::current(FailedPeer),
        ),
        Err(NativeRelayWireBridgeError::Relay(_))
    ));
}

/// Alice's locally committed row survives foreground close after an upstream
/// send failure. Bob's same-identity sibling reads the row; the retired
/// foreground's weak Db confirms close releases its in-memory owner.
#[test]
fn closing_owner_finishes_local_commit_despite_peer_io_failure() {
    let (_directory, relay, client, _wake) = attached_write_client("close-local", 0x4e);
    settle_owner(&relay);
    let (_, row) = client
        .direct_foreground_mutation(
            ForegroundMutationKind::Insert,
            "todos".into(),
            None,
            encoded_title_cells("accepted locally"),
            "{}".into(),
        )
        .unwrap();
    fail_upstream_fixture(&relay);
    let id = client.id;
    relay
        .run(move |worker| {
            let weak = Rc::downgrade(&worker.client(id)?.binding.db);
            worker.detach(id)?;
            assert!(
                weak.upgrade().is_none(),
                "peer failure cannot pin the closed foreground"
            );
            Ok(())
        })
        .unwrap();
    let sibling = relay
        .attach_client(
            fresh_client_identity(AuthorSubject::for_test_bytes([0x4e; 16])).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    let read = sibling
        .start_foreground_read(
            postcard::to_allocvec(&Query::from("todos")).unwrap(),
            "{}".into(),
            None,
        )
        .unwrap();
    let ForegroundOperationResult::Rows(rows) =
        complete_transaction_operation(&relay, &sibling, read, None)
    else {
        panic!("sibling must read the locally retained row");
    };
    assert_exact_todo_rows(&rows, row, "accepted locally");
    sibling.close().unwrap();
}

/// Alice shuts down after an upstream I/O failure; reopening the durable
/// private owner recovers her exact locally committed row without a server.
#[test]
fn shutdown_keeps_local_commit_despite_persistent_io_failure() {
    shutdown_failure_receipt(false);
}

/// Alice's command channel disappears after an upstream I/O failure. The
/// owner thread's final drain retains her row for a fresh process to reopen.
#[test]
fn disconnected_owner_keeps_local_commit_despite_persistent_io_failure() {
    shutdown_failure_receipt(true);
}

fn shutdown_failure_receipt(disconnected: bool) {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(
        directory.path().join("shutdown-local.sqlite"),
        Some("shutdown-local"),
    );
    let author = AuthorSubject::for_test_bytes([0x4e; 16]);
    config.identity.author = author;
    let relay = NativeRelayRegistry::default()
        .open_admitted(config.clone())
        .unwrap();
    let client = relay
        .attach_client(fresh_client_identity(author).unwrap(), BTreeMap::new())
        .unwrap();
    settle_owner(&relay);
    let (_, row) = client
        .direct_foreground_mutation(
            ForegroundMutationKind::Insert,
            "todos".into(),
            None,
            encoded_title_cells("accepted before shutdown"),
            "{}".into(),
        )
        .unwrap();
    fail_upstream_fixture(&relay);
    if disconnected {
        // Drop the real last command sender while retaining the join handle.
        relay.inner.commands.lock().unwrap().take();
    }
    relay.inner.shutdown().unwrap();
    assert!(!relay.is_alive());
    assert!(client.take_frames().is_err());
    let reopened = NativeRelayRegistry::default()
        .open_admitted(config)
        .unwrap();
    let foreground = reopened
        .attach_client(fresh_client_identity(author).unwrap(), BTreeMap::new())
        .unwrap();
    let read = foreground
        .start_foreground_read(
            postcard::to_allocvec(&Query::from("todos")).unwrap(),
            "{}".into(),
            None,
        )
        .unwrap();
    let ForegroundOperationResult::Rows(rows) =
        complete_transaction_operation(&reopened, &foreground, read, None)
    else {
        panic!("reopened owner must recover the locally durable write");
    };
    assert_exact_todo_rows(&rows, row, "accepted before shutdown");
    foreground.close().unwrap();
}

/// Alice's foreground close fails once; Bob's sibling must still finish
/// closing. Alice's owner and the shared storage lease remain until retry.
/// The failure seam is the real foreground close boundary, which a public
/// caller cannot deterministically fail without destroying its SQLite store.
#[test]
fn shutdown_close_error_retains_failed_owner_and_finishes_sibling() {
    let (_directory, relay, alice, _wake) = attached_write_client("close-retry", 0x4e);
    let bob = relay
        .attach_client(
            fresh_client_identity(AuthorSubject::for_test_bytes([0x4e; 16])).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    settle_owner(&relay);
    let (_, row) = bob
        .direct_foreground_mutation(
            ForegroundMutationKind::Insert,
            "todos".into(),
            None,
            encoded_title_cells("survives sibling close failure"),
            "{}".into(),
        )
        .unwrap();
    let alice_id = alice.id;
    let bob_id = bob.id;
    relay
        .run(move |worker| {
            let failed = Rc::downgrade(&worker.client(alice_id)?.binding.db);
            let sibling = Rc::downgrade(&worker.client(bob_id)?.binding.db);
            worker.client_mut(alice_id)?.close_error_for_test =
                Some(HostError::Argument("controlled close failure".into()));
            let error = worker.shutdown().unwrap_err();
            assert!(error.to_string().contains("controlled close failure"));
            assert!(failed.upgrade().is_some(), "failed owner remains for retry");
            assert!(worker.clients.contains_key(&alice_id));
            assert!(
                sibling.upgrade().is_none(),
                "one failed close must not retain a healthy sibling"
            );
            assert!(!worker.clients.contains_key(&bob_id));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        streaming_rows(&alice),
        vec![row],
        "the persistent owner was not retired after failure"
    );
    relay
        .inner
        .shutdown()
        .expect("retry completes and releases every owner");
    assert!(!relay.is_alive());
}

/// Alice repeatedly rolls back a transaction after a synchronous schema error.
/// Both the real SQLite batch and its native handle must retire; a subsequent
/// foreground read must contain no staged rows. The owner seam observes the
/// actual batch lifetime because the public API cannot expose retained state.
#[test]
fn rolled_back_staging_failures_retire_open_batches() {
    let (_directory, relay, client, _wake) = attached_write_client("tx-errors", 0x4d);
    settle_owner(&relay);
    let id = client.id;
    relay
        .run(move |worker| {
            for _ in 0..96 {
                let tx = worker
                    .begin_foreground_transaction(id, ForegroundTransactionKind::Mergeable)?;
                let db = worker.client(id)?.binding.db.clone();
                let open: jazz::tools::OpenTransactionId = worker
                    .client(id)?
                    .transaction(tx)
                    .unwrap()
                    .0
                    .parse()
                    .unwrap();
                worker.insert_foreground_transaction(
                    id,
                    tx,
                    "todos".into(),
                    encoded_title_cells("rolled back"),
                    None,
                )?;
                assert!(
                    worker
                        .insert_foreground_transaction(
                            id,
                            tx,
                            "missing_table".into(),
                            encoded_title_cells("rejected"),
                            None
                        )
                        .is_err()
                );
                assert!(db.has_open_transaction(open));
                assert!(worker.rollback_foreground_transaction(id, tx)?);
                assert!(!db.has_open_transaction(open));
                assert!(worker.client(id)?.transaction(tx).is_err());
            }
            assert!(worker.client(id)?.transactions.is_empty());
            Ok(())
        })
        .unwrap();
    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
    let read = client
        .start_foreground_read(query, "{}".into(), None)
        .unwrap();
    assert!(owner_driven_rows(&client, read).is_empty());
    client.close().unwrap();
}

/// Alice starts two Remote reads before Bob's authority can answer. Their
/// shared upstream query survives cancelling either observer and is released
/// when the last observer drops, without another owner drive. This internal
/// turn boundary observes real reference counts before background progress.
#[test]
fn cancelled_read_releases_shared_coverage_without_an_owner_wait() {
    let (_directory, relay, client, _wake) = attached_write_client("coverage-cleanup", 0x45);
    settle_owner(&relay);
    let id = client.id;
    relay
        .run(move |worker| {
            let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
            let mut reads = Vec::new();
            for _ in 0..2 {
                let read = worker.start_foreground_read(
                    id,
                    query.clone(),
                    "{\"tier\":\"remote\"}".into(),
                    None,
                )?;
                let ForegroundOperationPoll::Pending { operation } = read else {
                    panic!("Remote read must wait for the unavailable authority");
                };
                reads.push(operation);
            }
            let db = Rc::clone(&worker.client(id)?.binding.db);
            assert_eq!(db.query_coverage_attachment_counts_for_test(), (1, 2));
            assert!(worker.cancel_foreground_operation(id, reads[0])?);
            assert!(matches!(worker.execute_foreground(id, ForegroundDbCommandRequest::Poll { operation: reads[0] })?,
                ForegroundDbCommandResponse::OperationError { reason } if reason.contains("unknown foreground operation")));
            assert_eq!(db.query_coverage_attachment_counts_for_test(), (1, 1));
            assert!(matches!(
                worker.poll_foreground_operation(id, reads[1])?,
                ForegroundOperationPoll::Pending { .. }
            ));
            assert!(worker.cancel_foreground_operation(id, reads[1])?);
            assert_eq!(db.query_coverage_attachment_counts_for_test(), (0, 0));
            Ok(())
        })
        .unwrap();
    relay
        .pump()
        .expect("cancelled reads must not block the owner");
    assert_eq!(
        client
            .with_db(|db| Ok(db.query_coverage_attachment_counts_for_test()))
            .unwrap(),
        (0, 0)
    );
    client.close().unwrap();
}

/// Wait until the owner stops scheduling its own drive turns.
fn settle_owner(relay: &NativeRelay) -> u64 {
    let mut last = u64::MAX;
    for _ in 0..1_000 {
        let (turns, due) = relay
            .run(|worker| Ok((worker.core_tick_turns, worker.has_drive_work_for_test())))
            .unwrap();
        if turns == last && !due {
            return turns;
        }
        last = turns;
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    panic!("owner drive turns did not reach quiescence");
}

/// Alice's foreground is admitted synchronously. Closing it must abandon
/// staged writes and a real pending Remote read, release its served peer,
/// and leave a sibling able to read a later committed write. The private
/// peer counts observe teardown that opaque C ABI handles cannot expose.
#[test]
fn synchronous_foreground_admission_and_close_retire_unpublished_work() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("admission.sqlite"),
        "admission",
        &permissive_schema(),
        0x63,
    );
    let keeper = fixture.open_foreground(&capability);
    let closed = fixture.open_foreground(&capability);
    let closed_client = unsafe { &*fixture.host }
        .inner
        .lock()
        .unwrap()
        .foreground_client(closed)
        .unwrap()
        .clone();
    let relay = closed_client.relay.clone();
    let id = closed_client.id;
    relay
        .run(move |worker| {
            let client = &worker.clients[&id];
            assert!(
                Rc::ptr_eq(&client.served.binding().db, &worker.persistent.db),
                "open returns after installing the real served peer"
            );
            let mut admitted = Box::pin(client.binding.db.wait_for_storage_owner());
            assert!(matches!(
                admitted
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(worker.persistent.db.open_subscribers(), 2);
            Ok(())
        })
        .unwrap();
    let ForegroundDbCommandResponse::TransactionOpened { transaction } = fixture.execute(
        closed,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        },
    ) else {
        panic!("begin unpublished transaction");
    };
    assert!(matches!(
        fixture.execute(
            closed,
            ForegroundDbCommandRequest::Insert {
                transaction,
                table: "todos".into(),
                cells: encoded_title_cells("abandoned on close"),
                row_id: Some([0x71; 16]),
            }
        ),
        ForegroundDbCommandResponse::Inserted { .. }
    ));
    let ForegroundDbCommandResponse::Pending { operation } = fixture.execute(
        closed,
        ForegroundDbCommandRequest::All {
            query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
            options_json: r#"{"tier":"Remote"}"#.into(),
            transaction: None,
        },
    ) else {
        panic!("Remote read without an authority remains pending");
    };
    assert_eq!(
        fixture.execute(closed, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    assert_eq!(
        fixture.tick_status(closed),
        JazzNativeRelayStatus::InvalidHandle
    );
    assert!(closed_client.poll_foreground_operation(operation).is_err());
    assert!(
        closed_client
            .commit_foreground_transaction(transaction)
            .is_err()
    );
    relay
        .run(move |worker| {
            assert!(!worker.clients.contains_key(&id));
            assert!(
                !worker.closing,
                "closing one foreground keeps its owner live"
            );
            assert_eq!(worker.persistent.db.open_subscribers(), 1);
            Ok(())
        })
        .unwrap();
    assert!(
        postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&fixture.rows_after_sync(keeper))
            .unwrap()
            .iter()
            .all(|batch| batch.rows.is_empty())
    );
    let admitted = fixture.open_foreground(&capability);
    let row = [0x72; 16];
    fixture.insert_todo(admitted, row, "admitted sibling remains usable");
    assert_exact_todo_rows(
        &fixture.rows_after_sync(keeper),
        RowUuid::from_bytes(row),
        "admitted sibling remains usable",
    );
    for foreground in [admitted, keeper] {
        assert_eq!(
            fixture.execute(foreground, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
    }
}

/// Alice revokes a scope while its admitted foreground awaits an authority
/// read and owns an unpublished write. All aliases and work in that scope
/// must retire; Bob's separate admitted scope must remain writable/readable.
/// The private relay handle verifies that the revoked storage owner stops.
#[test]
fn revocation_retires_admitted_pending_work_and_preserves_another_scope() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let schema = permissive_schema();
    let a_capability = fixture.admit(
        &directory.path().join("admission-a.sqlite"),
        "admission-a",
        &schema,
        0x64,
    );
    let b_capability = fixture.admit(
        &directory.path().join("admission-b.sqlite"),
        "admission-b",
        &schema,
        0x65,
    );
    let a = fixture.open_foreground(&a_capability);
    let b = fixture.open_foreground(&b_capability);
    let waiting = fixture.open_foreground(&a_capability);
    let client = unsafe { &*fixture.host }
        .inner
        .lock()
        .unwrap()
        .foreground_client(waiting)
        .unwrap()
        .clone();
    let relay = client.relay.clone();
    let ForegroundDbCommandResponse::TransactionOpened { transaction } = fixture.execute(
        waiting,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        },
    ) else {
        panic!("begin unpublished transaction");
    };
    assert!(matches!(
        fixture.execute(
            waiting,
            ForegroundDbCommandRequest::Insert {
                transaction,
                table: "todos".into(),
                cells: encoded_title_cells("abandoned on revocation"),
                row_id: Some([0x73; 16]),
            }
        ),
        ForegroundDbCommandResponse::Inserted { .. }
    ));
    let ForegroundDbCommandResponse::Pending { operation } = fixture.execute(
        waiting,
        ForegroundDbCommandRequest::All {
            query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
            options_json: r#"{"tier":"Remote"}"#.into(),
            transaction: None,
        },
    ) else {
        panic!("Remote read without an authority remains pending");
    };
    assert_eq!(
        unsafe {
            jazz_native_relay_host_revoke_scope_capability(
                fixture.host,
                a_capability.as_ptr(),
                a_capability.len(),
            )
        },
        JazzNativeRelayStatus::Ok
    );
    assert_eq!(fixture.tick_status(a), JazzNativeRelayStatus::InvalidHandle);
    assert_eq!(
        fixture.tick_status(waiting),
        JazzNativeRelayStatus::InvalidHandle
    );
    assert_eq!(
        fixture.try_open_foreground(&a_capability),
        (JazzNativeRelayStatus::InvalidHandle, 0)
    );
    assert!(matches!(relay.pump(), Err(RelayError::Closed)));
    assert!(client.poll_foreground_operation(operation).is_err());
    assert!(client.commit_foreground_transaction(transaction).is_err());
    let row = [0x74; 16];
    fixture.insert_todo(b, row, "independent scope survives");
    assert_exact_todo_rows(
        &fixture.rows_after_sync(b),
        RowUuid::from_bytes(row),
        "independent scope survives",
    );
}

/// Alice's new foreground fails synchronously at the real catalogue decoder.
/// Failed opens expose no handle and retain no aliases; her existing sibling
/// continues to serve writes and a later open succeeds once the bad capture
/// is removed. Only the fixture mutates the private cache to cause the error.
#[test]
fn synchronous_attachment_failure_is_confined_to_the_opening_foreground() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("admission-error.sqlite"),
        "admission-error",
        &permissive_schema(),
        0x66,
    );
    let keeper = fixture.open_foreground(&capability);
    let relay = unsafe { &*fixture.host }
        .inner
        .lock()
        .unwrap()
        .foreground_client(keeper)
        .unwrap()
        .relay
        .clone();
    let before = {
        let host = unsafe { &*fixture.host }.inner.lock().unwrap();
        (
            host.foregrounds.len(),
            host.clients.len(),
            host.relays.len(),
        )
    };
    let capture = relay
        .run(|worker| {
            Ok(worker
                .catalogue_cache
                .as_ref()
                .unwrap()
                .borrow_mut()
                .capture
                .replace(b"invalid catalogue capture for attachment error".to_vec()))
        })
        .unwrap();
    for _ in 0..4 {
        assert_eq!(
            fixture.try_open_foreground(&capability),
            (JazzNativeRelayStatus::LifecycleFailure, 0)
        );
        let host = unsafe { &*fixture.host }.inner.lock().unwrap();
        assert_eq!(
            (
                host.foregrounds.len(),
                host.clients.len(),
                host.relays.len()
            ),
            before
        );
    }
    fixture.tick(keeper);
    let row = [0x75; 16];
    fixture.insert_todo(keeper, row, "keeper survives failed opens");
    assert_exact_todo_rows(
        &fixture.rows_after_sync(keeper),
        RowUuid::from_bytes(row),
        "keeper survives failed opens",
    );
    relay
        .run(move |worker| {
            worker
                .catalogue_cache
                .as_ref()
                .unwrap()
                .borrow_mut()
                .capture = capture;
            Ok(())
        })
        .unwrap();
    let recovered = fixture.open_foreground(&capability);
    assert_exact_todo_rows(
        &fixture.rows_after_sync(recovered),
        RowUuid::from_bytes(row),
        "keeper survives failed opens",
    );
    for foreground in [recovered, keeper] {
        assert_eq!(
            fixture.execute(foreground, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
    }
}

/// Alice repeatedly starts and cancels Remote reads with no authority.
/// SQLite releases each coverage reference synchronously, without a platform
/// tick. The owner turn seam observes cleanup before background drives can
/// hide a retained reference, and fresh reads prove capacity is reusable.
#[test]
fn cancelled_reads_release_all_coverage_without_platform_ticks() {
    let (_directory, relay, client, _wake) = attached_write_client("cleanup-batch", 0x62);
    settle_owner(&relay);
    let id = client.id;
    relay
        .run(move |worker| {
            let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
            for _ in 0..(NATIVE_RELAY_FOREGROUND_PENDING_MAX + 7) {
                let response = worker.execute_foreground(
                    id,
                    ForegroundDbCommandRequest::All {
                        query: query.clone(),
                        options_json: "{\"tier\":\"remote\"}".into(),
                        transaction: None,
                    },
                )?;
                let ForegroundDbCommandResponse::Pending { operation } = response else {
                    panic!("unavailable remote read must remain pending: {response:?}");
                };
                let foreground = worker.client(id)?;
                assert_eq!(foreground.pending.len(), 1);
                assert_ne!(
                    foreground
                        .binding
                        .db
                        .query_coverage_attachment_counts_for_test(),
                    (0, 0)
                );
                assert_eq!(
                    worker
                        .execute_foreground(id, ForegroundDbCommandRequest::Cancel { operation })?,
                    ForegroundDbCommandResponse::Cancelled { cancelled: true }
                );
                let foreground = worker.client(id)?;
                assert!(foreground.pending.is_empty());
                assert_eq!(
                    foreground
                        .binding
                        .db
                        .query_coverage_attachment_counts_for_test(),
                    (0, 0)
                );
            }
            Ok(())
        })
        .unwrap();
    assert!(
        streaming_rows(&client).is_empty(),
        "cancelled Remote reads cannot stall local reads"
    );
    client.close().unwrap();
}

/// Open a real local subscription, consume its initial owner answer, then
/// leave later row deltas for the callback-lifetime assertions.
fn wake_subscription(client: &NativeRelayClient) -> u64 {
    let subscription = client
        .subscribe_foreground_query_with_options(
            postcard::to_allocvec(&Query::from("todos")).unwrap(),
            ReadOpts::default(),
        )
        .unwrap();
    settle_owner(&client.relay);
    let events = client.drain_foreground_subscription(subscription).unwrap();
    assert!(
        matches!(events, ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(ref events)) if !events.is_empty())
    );
    subscription
}
fn assert_wake_added_once(client: &NativeRelayClient, subscription: u64, row: RowUuid) {
    settle_owner(&client.relay);
    let ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(events)) =
        client.drain_foreground_subscription(subscription).unwrap()
    else {
        panic!("ready subscription must deliver its actual row delta");
    };
    let mut added = 0;
    for event in events {
        if let ForegroundSubscriptionEvent::Delta { delta, .. } = event {
            let delta: DecodedForegroundDelta = postcard::from_bytes(&delta).unwrap();
            added += delta
                .added
                .iter()
                .flat_map(|batch| &batch.rows)
                .filter(|r| r.row_id == row && !r.deleted)
                .count();
        }
    }
    assert_eq!(
        added, 1,
        "one inserted row is delivered once to this sibling"
    );
}

/// Alice and Bob share an owner. Real writes reach both subscriptions; after
/// Alice closes, Bob keeps receiving, and after both close no retained host
/// callback is invoked. The C ABI fixture observes raw callback lifetimes.
#[test]
fn shared_owner_progress_reaches_live_siblings_and_retires_closed_callbacks() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("wake.sqlite"),
        "wake",
        &permissive_schema(),
        0x61,
    );
    let a = fixture.open_foreground(&capability);
    let b = fixture.open_foreground(&capability);
    let writer = fixture.open_foreground(&capability);
    let get_client = |handle| unsafe {
        (*fixture.host)
            .inner
            .lock()
            .unwrap()
            .foreground_client(handle)
            .unwrap()
            .clone()
    };
    let alice = get_client(a);
    let bob = get_client(b);
    let writer = get_client(writer);
    let a_wake = Arc::new(QueuedNativeWake::active());
    let b_wake = Arc::new(QueuedNativeWake::active());
    for (handle, wake) in [(a, &a_wake), (b, &b_wake)] {
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_set_foreground_wake_callback(
                    fixture.lease,
                    handle,
                    Some(queue_native_wake),
                    Arc::as_ptr(wake) as *mut c_void,
                )
            },
            JazzNativeRelayStatus::Ok
        );
    }
    let a_subscription = wake_subscription(&alice);
    let b_subscription = wake_subscription(&bob);
    a_wake.queued.lock().unwrap().clear();
    b_wake.queued.lock().unwrap().clear();
    let write = |row| {
        writer
            .direct_foreground_mutation(
                ForegroundMutationKind::Insert,
                "todos".into(),
                Some([row; 16]),
                encoded_title_cells("shared owner wake"),
                "{}".into(),
            )
            .unwrap()
            .1
    };
    let first = write(0x71);
    assert!(
        a_wake.wait_for_queued(1),
        "Alice is notified without a platform tick"
    );
    assert!(
        b_wake.wait_for_queued(1),
        "Bob is notified without a platform tick"
    );
    assert_wake_added_once(&alice, a_subscription, first);
    assert_wake_added_once(&bob, b_subscription, first);
    assert_eq!(
        fixture.execute(a, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    assert_eq!(a_wake.cancelled.load(Ordering::Acquire), 1);
    b_wake.queued.lock().unwrap().clear();
    let second = write(0x72);
    assert!(
        b_wake.wait_for_queued(1),
        "shared owner progress survives the first reader's close"
    );
    assert_wake_added_once(&bob, b_subscription, second);
    assert_eq!(a_wake.callbacks_after_cancel.load(Ordering::Acquire), 0);
    assert_eq!(
        fixture.execute(b, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    assert_eq!(b_wake.cancelled.load(Ordering::Acquire), 1);
    write(0x73);
    settle_owner(&writer.relay);
    a_wake.assert_no_callback_for(Duration::from_millis(50));
    b_wake.assert_no_callback_for(Duration::from_millis(50));
}

/// Alice replaces a callback before the old registration becomes inert.
/// A real row delta must notify only the replacement and remain readable.
#[test]
fn replacement_wake_before_old_host_state_becomes_inert_uses_current_generation() {
    let (_directory, relay, client, _wake) = attached_write_client("replacement", 0x62);
    let old_receipt = Arc::new(QueuedNativeWake::active());
    let new_receipt = Arc::new(QueuedNativeWake::active());
    let state = |receipt: &Arc<QueuedNativeWake>| {
        Arc::new(ForegroundWakeState::new(ForegroundWakeRegistration {
            callback: queue_native_wake,
            context: Arc::as_ptr(receipt) as usize,
        }))
    };
    let old_state = state(&old_receipt);
    let new_state = state(&new_receipt);
    client
        .set_foreground_wake_callback(1, Some(old_state.clone()))
        .unwrap();
    let subscription = wake_subscription(&client);
    client
        .set_foreground_wake_callback(1, Some(new_state.clone()))
        .unwrap();
    old_receipt.queued.lock().unwrap().clear();
    new_receipt.queued.lock().unwrap().clear();
    let (_, row) = client
        .direct_foreground_mutation(
            ForegroundMutationKind::Insert,
            "todos".into(),
            None,
            encoded_title_cells("current callback"),
            "{}".into(),
        )
        .unwrap();
    assert!(new_receipt.wait_for_queued(1));
    assert_wake_added_once(&client, subscription, row);
    assert_eq!(
        old_receipt.queued(),
        0,
        "replacement does not rely on retiring the old host state"
    );
    old_state.inert();
    new_state.inert();
    client.close().unwrap();
    relay.pump().unwrap();
}

#[test]
fn structured_foreground_read_progresses_without_blocking_the_owner() {
    // This native-boundary receipt needs the actual owner queue: a Rust
    // client executor would keep polling the read itself and hide a host
    // Tick which blocks the only thread able to resume or cancel it.
    let (done_tx, done_rx) = mpsc::channel();
    let receipt = thread::spawn(move || {
        use jazz::query::{ArraySubquery, ArraySubqueryRequirement};
        let directory = tempfile::tempdir().unwrap();
        let allow = PolicyExpr::True;
        let policies = TablePolicies::new()
            .with_select(allow.clone())
            .with_insert(allow.clone())
            .with_update(Some(allow.clone()), allow.clone())
            .with_delete(allow);
        let mut relay_config = config(
            directory.path().join("structured.sqlite"),
            Some("structured"),
        );
        relay_config.schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("groups")
                        .column("title", ColumnType::Text)
                        .policies(policies.clone()),
                )
                .table(
                    TableSchemaBuilder::new("tasks")
                        .column("title", ColumnType::Text)
                        .fk_column("group_id", "groups")
                        .policies(policies.clone()),
                )
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("title", ColumnType::Text)
                        .fk_column("task_id", "tasks")
                        .policies(policies),
                )
                .build(),
        )
        .unwrap();
        relay_config.identity.author = AuthorSubject::for_test_bytes([0x92; 16]);
        let relay = NativeRelayRegistry::default()
            .open_admitted(relay_config)
            .unwrap();
        let client = relay
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0x91; 16]),
                    author: AuthorSubject::for_test_bytes([0x92; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        let group = client
            .with_db(|db| {
                let group = block_on(db.insert(
                    "groups",
                    BTreeMap::from([("title".into(), Value::String("group".into()))]),
                    Default::default(),
                ))
                .map_err(RelayError::Db)?
                .row_uuid();
                let task = block_on(db.insert(
                    "tasks",
                    BTreeMap::from([
                        ("title".into(), Value::String("task".into())),
                        ("group_id".into(), Value::Uuid(group.0)),
                    ]),
                    Default::default(),
                ))
                .map_err(RelayError::Db)?
                .row_uuid();
                block_on(db.insert(
                    "notes",
                    BTreeMap::from([
                        ("title".into(), Value::String("note".into())),
                        ("task_id".into(), Value::Uuid(task.0)),
                    ]),
                    Default::default(),
                ))
                .map_err(RelayError::Db)?;
                Ok(group)
            })
            .unwrap();
        let query = Query::from("groups").array_subquery(
            ArraySubquery::new("tasksViaGroup", "tasks", "group_id", "id")
                .select(["title"])
                .requirement(ArraySubqueryRequirement::AtLeastOne)
                .nested(ArraySubquery::new("notesViaTask", "notes", "task_id", "id")),
        );
        let prepared = postcard::to_allocvec(&query).unwrap();
        let mut response = client
            .start_foreground_read(prepared, "{}".into(), None)
            .unwrap();
        let mut turns = 0;
        let bytes = loop {
            match response {
                ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(bytes)) => {
                    break bytes;
                }
                ForegroundOperationPoll::Pending { operation } => {
                    assert!(
                        turns < 512,
                        "structured read must finish through bounded host turns"
                    );
                    relay.pump().unwrap();
                    response = client.poll_foreground_operation(operation).unwrap();
                    turns += 1;
                }
                _ => panic!("structured read returned an unexpected response"),
            }
        };
        #[derive(serde::Deserialize)]
        struct Snapshot {
            root_count: u64,
            rows: Vec<DecodedForegroundRowBatch>,
        }
        let snapshot: Snapshot = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(snapshot.root_count, 1);
        assert_eq!(snapshot.rows[0].rows[0].row_id, group);
        client.close().unwrap();
        drop(relay);
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("native owner must return from Tick and finish the structured read");
    receipt.join().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn foreground_wakes_leave_alternate_stacks_before_native_callback() {
    // Internal embedding-boundary receipt: row results cannot reveal which
    // native stack invokes a callback. ART derives attachment bounds from
    // pthread metadata, so a scheduler called on stacker's separate segment
    // must defer JNI until the owner operation returns to that stack.
    #[derive(Default)]
    struct Receipt {
        operation_returned: AtomicBool,
        calls: Mutex<Vec<(u8, u64, bool, bool)>>,
    }
    fn on_pthread_stack() -> bool {
        let marker = 0u8;
        unsafe {
            let mut attr = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
            assert_eq!(
                libc::pthread_getattr_np(libc::pthread_self(), attr.as_mut_ptr()),
                0
            );
            let mut attr = attr.assume_init();
            let mut base = std::ptr::null_mut();
            let mut size = 0;
            assert_eq!(libc::pthread_attr_getstack(&attr, &mut base, &mut size), 0);
            assert_eq!(libc::pthread_attr_destroy(&mut attr), 0);
            let address = &marker as *const u8 as usize;
            (base as usize..base as usize + size).contains(&address)
        }
    }
    unsafe extern "C" fn capture(context: *mut c_void, _: u64, kind: u8, delay: u64) {
        let receipt = unsafe { &*(context as *const Receipt) };
        receipt.calls.lock().unwrap().push((
            kind,
            delay,
            on_pthread_stack(),
            receipt.operation_returned.load(Ordering::SeqCst),
        ));
    }
    let (_directory, relay, client, _wake) = attached_write_client("stack", 0x44);
    let receipt = Arc::new(Receipt::default());
    let state = Arc::new(ForegroundWakeState::new(ForegroundWakeRegistration {
        callback: capture,
        context: Arc::as_ptr(&receipt) as usize,
    }));
    client
        .set_foreground_wake_callback(1, Some(state.clone()))
        .unwrap();
    let subscription = wake_subscription(&client);
    receipt.calls.lock().unwrap().clear();
    for (row_byte, fail) in [(0x81, false), (0x82, true)] {
        receipt.operation_returned.store(false, Ordering::SeqCst);
        let done = receipt.clone();
        let result = client.with_db(move |db| {
            assert!(on_pthread_stack());
            stacker::grow(8 * 1024 * 1024, || {
                assert!(
                    !on_pthread_stack(),
                    "control enters a separate stack segment"
                );
                block_on(db.insert(
                    "todos",
                    BTreeMap::from([("title".into(), Value::String("alternate stack".into()))]),
                    InsertOptions {
                        row_id: Some(RowUuid::from_bytes([row_byte; 16])),
                        ..Default::default()
                    },
                ))
                .unwrap();
                assert!(
                    done.calls.lock().unwrap().is_empty(),
                    "JNI callback must wait for the original stack"
                );
            });
            done.operation_returned.store(true, Ordering::SeqCst);
            if fail {
                Err(RelayError::Closed)
            } else {
                Ok(())
            }
        });
        assert_eq!(result.is_err(), fail);
        assert_wake_added_once(&client, subscription, RowUuid::from_bytes([row_byte; 16]));
        assert_eq!(
            *receipt.calls.lock().unwrap(),
            vec![(FOREGROUND_WAKE_IMMEDIATE, 0, true, true)],
            "real row progress flushes on the original stack, even after an operation error"
        );
        receipt.calls.lock().unwrap().clear();
    }
    let cancel = state.clone();
    client
        .with_db(move |db| {
            stacker::grow(8 * 1024 * 1024, || {
                block_on(db.insert(
                    "todos",
                    BTreeMap::from([("title".into(), Value::String("cancelled callback".into()))]),
                    InsertOptions {
                        row_id: Some(RowUuid::from_bytes([0x83; 16])),
                        ..Default::default()
                    },
                ))
                .unwrap();
                cancel.inert();
            });
            Ok(())
        })
        .unwrap();
    assert_wake_added_once(&client, subscription, RowUuid::from_bytes([0x83; 16]));
    assert!(
        receipt.calls.lock().unwrap().is_empty(),
        "cancel before owner delivery suppresses JNI"
    );
    let replacement = Arc::new(Receipt::default());
    replacement.operation_returned.store(true, Ordering::SeqCst);
    let replacement_state = Arc::new(ForegroundWakeState::new(ForegroundWakeRegistration {
        callback: capture,
        context: Arc::as_ptr(&replacement) as usize,
    }));
    client
        .set_foreground_wake_callback(1, Some(replacement_state.clone()))
        .unwrap();
    let stale = state.clone();
    client
        .with_db(move |db| {
            stacker::grow(8 * 1024 * 1024, || {
                stale.wake(1, FOREGROUND_WAKE_IMMEDIATE, 0);
                block_on(db.insert(
                    "todos",
                    BTreeMap::from([(
                        "title".into(),
                        Value::String("replacement callback".into()),
                    )]),
                    InsertOptions {
                        row_id: Some(RowUuid::from_bytes([0x84; 16])),
                        ..Default::default()
                    },
                ))
                .unwrap();
            });
            Ok(())
        })
        .unwrap();
    assert_wake_added_once(&client, subscription, RowUuid::from_bytes([0x84; 16]));
    assert_eq!(
        *replacement.calls.lock().unwrap(),
        vec![(FOREGROUND_WAKE_IMMEDIATE, 0, true, true)]
    );
    assert!(receipt.calls.lock().unwrap().is_empty());
    replacement_state.inert();
    client.close().unwrap();
    relay.pump().unwrap();
}

/// Alice's pending operation must not monopolise the owner thread. A
/// controlled future exposes the scheduling boundary without a full missing
/// chunk topology; Bob's queued work completes before Alice becomes ready.
#[test]
fn pending_foreground_operation_never_blocks_the_owner_tick() {
    let (_directory, relay, client, _wake) = attached_write_client("pending", 0x44);
    settle_owner(&relay);
    let ready = Arc::new(AtomicBool::new(false));
    let pending_ready = Arc::clone(&ready);
    let id = client.id;
    relay
        .run(move |worker| {
            let future: PendingFuture = Box::pin(std::future::poll_fn(move |_| {
                if pending_ready.load(Ordering::SeqCst) {
                    Poll::Ready(Ok(ForegroundDbCommandResponse::Rows { rows: vec![0x2a] }))
                } else {
                    Poll::Pending
                }
            }));
            worker.client_mut(id)?.pending.insert(
                99,
                PendingOperation {
                    future,
                    advice: None,
                },
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(
        client
            .execute(ForegroundDbCommandRequest::Poll { operation: 99 })
            .unwrap(),
        ForegroundDbCommandResponse::Pending { operation: 99 }
    );
    let other = relay
        .enqueue(|worker| {
            worker.drive();
            Ok(42)
        })
        .unwrap();
    assert_eq!(
        other.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
        42,
        "an unrelated owner command completes while the read is pending"
    );
    ready.store(true, Ordering::SeqCst);
    assert_eq!(
        client
            .execute(ForegroundDbCommandRequest::Poll { operation: 99 })
            .unwrap(),
        ForegroundDbCommandResponse::Rows { rows: vec![0x2a] }
    );
    assert!(matches!(
        client
            .execute(ForegroundDbCommandRequest::Poll { operation: 99 })
            .unwrap(),
        ForegroundDbCommandResponse::OperationError { .. }
    ));
    client.close().unwrap();
}

#[test]
fn relation_read_uses_the_canonical_query_byte_contract() {
    let command = ForegroundDbCommandRequest::All {
        query: vec![0, 1, b't', 0],
        options_json: "{}".into(),
        transaction: None,
    };
    let expected = [2, 4, 0, 1, b't', 0, 2, b'{', b'}', 0];
    assert_eq!(postcard::to_allocvec(&command).unwrap(), expected);
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandRequest>(&expected).unwrap(),
        command
    );
}

/// Alice prepares a read and two subscriptions in one native owner turn.
/// SQLite opens the subscriptions synchronously, but delivery waits for the
/// storage owner's real reply. Cancelling one opener cannot stall the other.
/// The private turn boundary prevents a background reply hiding this wait.
#[test]
fn preparation_and_subscription_wait_for_storage_owner_delivery() {
    let (_directory, relay, client, _wake) = attached_write_client("prepare-reply", 0x44);
    settle_owner(&relay);
    let id = client.id;
    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
    let operation = relay.run(move |worker| {
        let read = worker.start_foreground_read(
            id,
            query.clone(),
            "{}".into(),
            None,
        )?;
        let ForegroundOperationPoll::Pending { operation } = read else {
            panic!("read must await the storage owner response");
        };
        let cancelled =
            worker.subscribe_foreground_query_with_options(
                id,
                query.clone(),
                ReadOpts::default(),
            )?;
        assert!(worker.close_foreground_subscription(id, cancelled)?);
        assert_eq!(worker.client(id)?.pending.keys().copied().collect::<Vec<_>>(), vec![operation], "only the independent read remains pending");
        assert!(!worker.client(id)?.subscriptions.contains_key(&cancelled));
        let live =
            worker.subscribe_foreground_query_with_options(
                id,
                query,
                ReadOpts::default(),
            )?;
        assert!(matches!(worker.drain_foreground_subscription(id, live)?,
            ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(events)) if events.is_empty()));
        assert!(worker.client(id)?.subscriptions.contains_key(&live));
        assert_eq!(worker.client(id)?.pending.keys().copied().collect::<Vec<_>>(), vec![operation], "only the independent read remains pending");
        Ok((operation, live))
    }).unwrap();
    let (operation, subscription) = operation;
    let mut read_ready = false;
    let mut subscription_ready = false;
    for _ in 0..32 {
        relay.pump().unwrap();
        let (read, subscribed) = relay
            .run(move |worker| {
                let read = if read_ready {
                    true
                } else {
                    matches!(
                        worker.poll_foreground_operation(id, operation)?,
                        ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(_))
                    )
                };
                let subscribed = subscription_ready || matches!(
                    worker.drain_foreground_subscription(id, subscription)?,
                    ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(events))
                        if !events.is_empty()
                );
                Ok((read, subscribed))
            })
            .unwrap();
        read_ready = read;
        subscription_ready = subscribed;
        if read_ready && subscription_ready {
            break;
        }
    }
    assert!(read_ready, "read completes after the owner replies");
    assert!(
        subscription_ready,
        "the retained subscription delivers its initial owner response"
    );
    client.close().unwrap();
}

#[test]
fn linked_identity_storage_handoff_requires_previous_owner_close() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("account.sqlite");
    let registry = NativeRelayRegistry::default();
    let mut alice = config(path.clone(), Some("alice"));
    let mut bob = config(path, Some("bob"));
    let account =
        jazz::account_registry::AccountId("07070707-0707-0707-0707-070707070707".parse().unwrap());
    alice.identity.author = AuthorSubject::authenticated("https://issuer.example", "alice")
        .unwrap()
        .with_account(account);
    bob.identity.author = AuthorSubject::authenticated("https://issuer.example", "bob")
        .unwrap()
        .with_account(account);
    let old = registry.open(alice.clone()).unwrap();
    assert!(matches!(
        registry.open(bob.clone()),
        Err(RelayError::StorageAlreadyOwned)
    ));
    registry.close(&alice.scope).unwrap();
    assert!(
        old.pump().is_err(),
        "retained old aliases cannot survive handoff"
    );
    let next = registry.open(bob.clone()).unwrap();
    assert!(next.pump().is_ok());
    registry.close(&bob.scope).unwrap();
}

#[test]
fn relay_shares_one_scope_and_forwards_two_ui_client_writes_upstream() {
    let directory = tempfile::tempdir().unwrap();
    let registry = NativeRelayRegistry::default();
    let first = registry
        .open_admitted(config(directory.path().join("alice.sqlite"), Some("alice")))
        .unwrap();
    let same = registry
        .open_admitted(config(directory.path().join("alice.sqlite"), Some("alice")))
        .unwrap();
    let other = registry
        .open_admitted(config(directory.path().join("bob.sqlite"), Some("bob")))
        .unwrap();
    assert!(Arc::ptr_eq(&first.inner, &same.inner));
    assert!(!Arc::ptr_eq(&first.inner, &other.inner));
    assert!(matches!(
        registry.open_admitted(config(directory.path().join("wrong.sqlite"), Some("alice"))),
        Err(RelayError::ScopeConfigurationMismatch)
    ));
    let mut wrong_identity = config(directory.path().join("alice.sqlite"), Some("alice"));
    wrong_identity.identity = DbIdentity {
        node: NodeUuid::from_bytes([0xe1; 16]),
        author: AuthorSubject::for_test_bytes([0xe2; 16]),
    };
    assert!(matches!(
        registry.open_admitted(wrong_identity),
        Err(RelayError::ScopeConfigurationMismatch)
    ));
    let mut wrong_schema = config(directory.path().join("alice.sqlite"), Some("alice"));
    wrong_schema.schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("notes").column("body", ColumnType::Text))
            .build(),
    )
    .unwrap();
    assert!(matches!(
        registry.open_admitted(wrong_schema),
        Err(RelayError::ScopeConfigurationMismatch)
    ));

    let first_client = first
        .attach_client(
            DbIdentity {
                node: NodeUuid::from_bytes([0xb1; 16]),
                author: first.inner.identity.author,
            },
            BTreeMap::new(),
        )
        .unwrap();
    let second_client = same
        .attach_client(
            DbIdentity {
                node: NodeUuid::from_bytes([0xc1; 16]),
                author: same.inner.identity.author,
            },
            BTreeMap::new(),
        )
        .unwrap();
    assert_ne!(first_client.id(), second_client.id());

    let authority = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xe8; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    authority.accept_subscriber(
        Box::new(WireTransportAdapter::current(AuthorityWire::new(
            first.wire().unwrap(),
        ))),
        first.inner.identity.author,
    );
    for _ in 0..16 {
        block_on(authority.tick()).unwrap();
        first.pump().unwrap();
    }
    // Saturate the stateful upstream transport before the relay tries to
    // forward either commit. Core must retain the unsent protocol state on
    // `TransportError::Backpressure`, then retry it after the host drains.
    let filler = SyncMessage::Unsubscribe { id: 0 };
    while first.wire().unwrap().queue_depths().1 < NATIVE_RELAY_QUEUE_MAX_MESSAGES {
        first
            .wire()
            .unwrap()
            .outbound
            .lock()
            .unwrap()
            .push(filler.clone(), "test saturated upstream")
            .unwrap();
    }
    first_client
        .with_db(|db| {
            block_on(db.insert(
                "todos",
                BTreeMap::from([("title".to_owned(), Value::String("native".to_owned()))]),
                InsertOptions {
                    row_id: Some(RowUuid::from_bytes([0xd1; 16])),
                    ..Default::default()
                },
            ))
            .map_err(RelayError::Db)?;
            Ok(())
        })
        .unwrap();
    second_client
        .with_db(|db| {
            block_on(db.insert(
                "todos",
                BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
                InsertOptions {
                    row_id: Some(RowUuid::from_bytes([0xd2; 16])),
                    ..Default::default()
                },
            ))
            .map_err(RelayError::Db)?;
            Ok(())
        })
        .unwrap();

    first.pump().unwrap();
    assert_eq!(
        first.wire().unwrap().queue_depths().1,
        NATIVE_RELAY_QUEUE_MAX_MESSAGES,
        "backpressured stateful messages must remain pending instead of displacing the queue",
    );
    let wire = first.wire().unwrap();
    let mut outbound = Vec::new();
    for _ in 0..100 {
        let drained = wire.take_outbound_encoded().unwrap();
        let empty = drained.is_empty();
        outbound.extend(
            drained
                .iter()
                .map(|bytes| decode_sync_message(bytes).unwrap()),
        );
        first.pump().unwrap();
        if empty && wire.queue_depths().1 == 0 {
            break;
        }
    }
    assert_eq!(wire.queue_depths().1, 0);
    let forwarded_rows = outbound
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Commit { ops, .. } => Some(ops),
            _ => None,
        })
        .flatten()
        .map(|op| {
            let row = serde_json::to_value(op)
                .unwrap()
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap()["row"]
                .clone();
            RowUuid::from_bytes(serde_json::from_value(row).unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(forwarded_rows.len(), 2);
    assert!(forwarded_rows.contains(&RowUuid::from_bytes([0xd1; 16])));
    assert!(forwarded_rows.contains(&RowUuid::from_bytes([0xd2; 16])));
    assert_eq!(
        outbound
            .iter()
            .filter(|message| matches!(message, SyncMessage::Commit { .. }))
            .count(),
        2,
        "each in-memory UI client must reach the shared persistent relay and its upstream"
    );
}

// Native-host topology receipt: inspect the first raw stream event inside
// the relay owner, before the binding codec can hide a provisional opening.
// There is no server: the SQLite owner alone must complete Local loading.
fn assert_native_foreground_initial_owner_snapshot(seed_owner: bool) {
    let directory = tempfile::tempdir().unwrap();
    let registry = NativeRelayRegistry::default();
    let mut relay_config = config(directory.path().join("initial-owner.sqlite"), Some("alice"));
    relay_config.schema = permissive_schema();
    relay_config.identity.author = AuthorSubject::for_test_bytes([0xe3; 16]);
    let relay = registry.open_admitted(relay_config).unwrap();
    let expected = RowUuid::from_bytes([0xe1; 16]);
    if seed_owner {
        relay
            .run(move |worker| {
                block_on(worker.persistent.db.insert(
                    "todos",
                    BTreeMap::from([("title".to_owned(), Value::String("saved".to_owned()))]),
                    InsertOptions {
                        row_id: Some(expected),
                        ..Default::default()
                    },
                ))
                .map_err(RelayError::Db)?;
                block_on(worker.persistent.db.tick()).map_err(RelayError::Db)?;
                Ok(())
            })
            .unwrap();
    }
    let reader = relay
        .attach_client(
            DbIdentity {
                node: NodeUuid::from_bytes([0xe2; 16]),
                author: AuthorSubject::for_test_bytes([0xe3; 16]),
            },
            BTreeMap::new(),
        )
        .unwrap();
    let subscription = reader
        .subscribe_foreground_query_with_options(
            postcard::to_allocvec(&Query::from("todos")).unwrap(),
            ReadOpts::default(),
        )
        .unwrap();
    let reader_id = reader.id();
    let mut first = None;
    for _ in 0..64 {
        // Always inspect before pumping: the original empty opening must
        // fail, even if the owner would deliver the saved row on this turn.
        first = relay
            .run(move |worker| {
                let reader = worker.client_mut(reader_id)?;
                Ok(reader
                    .subscriptions
                    .get_mut(&subscription)
                    .and_then(|stream| stream.buffer.pop_front()))
            })
            .unwrap();
        if first.is_some() {
            break;
        }
        relay.pump().unwrap();
    }
    let Some(ForegroundSubscriptionEvent::Delta {
        reset: true, delta, ..
    }) = first
    else {
        panic!("the native foreground must complete its initial Local snapshot");
    };
    let delta: DecodedForegroundDelta = postcard::from_bytes(&delta).unwrap();
    assert_eq!(
        delta
            .added
            .iter()
            .flat_map(|batch| &batch.rows)
            .map(|row| row.row_id)
            .collect::<Vec<_>>(),
        if seed_owner { vec![expected] } else { vec![] }
    );
    assert!(delta.updated.is_empty());
    assert!(delta.removed.is_empty());
}

#[test]
fn native_foreground_first_snapshot_contains_owner_rows() {
    assert_native_foreground_initial_owner_snapshot(true);
}

#[test]
fn native_foreground_first_snapshot_completes_empty_owner_answer() {
    assert_native_foreground_initial_owner_snapshot(false);
}

#[test]
fn sibling_ui_subscription_observes_another_ui_client_write() {
    // Internal relay-topology receipt: each UI runtime owns an isolated
    // in-memory Db, so a sibling write is observable only if the ordinary
    // client -> persistent relay -> sibling protocol path is complete.
    let directory = tempfile::tempdir().unwrap();
    let registry = NativeRelayRegistry::default();
    let mut relay_config = config(directory.path().join("shared.sqlite"), Some("alice"));
    relay_config.schema = permissive_schema();
    relay_config.identity.author = AuthorSubject::for_test_bytes([0xb2; 16]);
    let relay = registry.open_admitted(relay_config).unwrap();
    let writer = relay
        .attach_client(
            DbIdentity {
                node: NodeUuid::from_bytes([0xb1; 16]),
                author: AuthorSubject::for_test_bytes([0xb2; 16]),
            },
            BTreeMap::new(),
        )
        .unwrap();
    let reader = relay
        .attach_client(
            DbIdentity {
                node: NodeUuid::from_bytes([0xc1; 16]),
                author: AuthorSubject::for_test_bytes([0xb2; 16]),
            },
            BTreeMap::new(),
        )
        .unwrap();

    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
    let subscription = reader
        .subscribe_foreground_query_with_options(query, ReadOpts::default())
        .unwrap();
    let reader_id = reader.id();
    for _ in 0..16 {
        relay.pump().unwrap();
    }
    relay
        .run(move |worker| {
            let reader = worker.client_mut(reader_id)?;
            let stream = reader.subscriptions.get_mut(&subscription).unwrap();
            stream.buffer.clear();
            Ok(())
        })
        .unwrap();

    let expected = RowUuid::from_bytes([0xd1; 16]);
    writer
        .with_db(move |db| {
            block_on(db.insert(
                "todos",
                BTreeMap::from([("title".to_owned(), Value::String("native".to_owned()))]),
                InsertOptions {
                    row_id: Some(expected),
                    ..Default::default()
                },
            ))
            .map_err(RelayError::Db)?;
            Ok(())
        })
        .unwrap();

    let mut observed = false;
    for _ in 0..32 {
        relay.pump().unwrap();
        observed |= relay
            .run(move |worker| {
                let reader = worker.client_mut(reader_id)?;
                let stream = reader.subscriptions.get_mut(&subscription).unwrap();
                let mut observed = false;
                while let Some(event) = stream.buffer.pop_front() {
                    if let ForegroundSubscriptionEvent::Delta { delta, .. } = event {
                        let delta: DecodedForegroundDelta = postcard::from_bytes(&delta).unwrap();
                        observed |= delta
                            .added
                            .iter()
                            .chain(&delta.updated)
                            .flat_map(|batch| &batch.rows)
                            .any(|row| row.row_id == expected);
                    }
                }
                Ok(observed)
            })
            .unwrap();
        if observed {
            break;
        }
    }
    assert!(
        observed,
        "the persistent relay must route a UI write to a sibling UI subscription"
    );
}

#[test]
fn retained_foreground_subscription_wakes_after_its_initial_reset_and_sibling_commit() {
    // This is an internal native-host receipt: public rows cannot establish
    // that the owner scheduled B's platform callback rather than relying on
    // the test's manual relay pumps. It covers the installed JSI sequence
    // where B drains its initial reset, then A commits through a foreground
    // transaction while B's stream remains retained.
    let directory = tempfile::tempdir().unwrap();
    let registry = NativeRelayRegistry::default();
    let mut relay_config = config(
        directory.path().join("wake-after-reset.sqlite"),
        Some("alice"),
    );
    relay_config.schema = permissive_schema();
    relay_config.identity.author = AuthorSubject::for_test_bytes([0xb4; 16]);
    let relay = registry.open_admitted(relay_config).unwrap();
    let writer = relay
        .attach_client(
            DbIdentity {
                node: NodeUuid::from_bytes([0xb3; 16]),
                author: AuthorSubject::for_test_bytes([0xb4; 16]),
            },
            BTreeMap::new(),
        )
        .unwrap();
    let reader = relay
        .attach_client(
            DbIdentity {
                node: NodeUuid::from_bytes([0xc3; 16]),
                author: AuthorSubject::for_test_bytes([0xb4; 16]),
            },
            BTreeMap::new(),
        )
        .unwrap();
    let reader_wake = Arc::new(QueuedNativeWake::active());
    reader
        .set_foreground_wake_callback(
            2,
            Some(Arc::new(ForegroundWakeState::new(
                ForegroundWakeRegistration {
                    callback: queue_native_wake,
                    context: Arc::as_ptr(&reader_wake) as usize,
                },
            ))),
        )
        .unwrap();

    let subscription = reader
        .subscribe_foreground_query_with_options(
            postcard::to_allocvec(&Query::from("todos")).unwrap(),
            ReadOpts::default(),
        )
        .unwrap();
    for _ in 0..16 {
        relay.pump().unwrap();
    }
    let initial = reader.drain_foreground_subscription(subscription).unwrap();
    assert!(
        matches!(
            initial,
            ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(_))
        ),
        "the initial reset drains without retiring the retained subscription"
    );
    reader_wake.queued.lock().unwrap().clear();

    let row = RowUuid::from_bytes([0xd3; 16]);
    let transaction = writer
        .begin_foreground_transaction(ForegroundTransactionKind::Mergeable)
        .unwrap();
    assert_eq!(
        writer
            .insert_foreground_transaction(
                transaction,
                "todos".into(),
                encoded_title_cells("wake after reset"),
                Some(*row.as_bytes()),
            )
            .unwrap(),
        row
    );
    writer.commit_foreground_transaction(transaction).unwrap();

    for _ in 0..32 {
        relay.pump().unwrap();
        if reader_wake.wait_for_queued(1) {
            break;
        }
    }
    assert!(
        reader_wake.wait_for_queued(1),
        "the retained reader subscription must schedule a post-commit native wake"
    );
    let wakes = reader_wake.queued.lock().unwrap();
    assert!(
        wakes.iter().any(|(foreground, _, _)| *foreground == 2),
        "the post-commit wake targets the retained reader foreground"
    );
}

/// Alice's Remote subscription stays pending without an authority. A native
/// wake must drive the owner once without storage writes or network retries;
/// the same receiver settles exactly once when Bob's authority arrives.
/// The internal wake/transport seams make callback coalescing and actual
/// upstream registrations observable without replacing the SQLite runtime.
#[test]
fn native_relay_storage_wake_progresses_unavailable_authority_and_recovers_once() {
    let directory = tempfile::tempdir().unwrap();
    let registry = NativeRelayRegistry::default();
    let mut relay_config = config(
        directory.path().join("authority-liveness.sqlite"),
        Some("alice"),
    );
    relay_config.schema = permissive_schema();
    relay_config.identity.author = AuthorSubject::for_test_bytes([0xd6; 16]);
    let relay = registry.open_admitted(relay_config).unwrap();
    let reader = relay
        .attach_client(
            DbIdentity {
                node: NodeUuid::from_bytes([0xd5; 16]),
                author: AuthorSubject::for_test_bytes([0xd6; 16]),
            },
            BTreeMap::new(),
        )
        .unwrap();
    let reader_wake = Arc::new(QueuedNativeWake::active());
    reader
        .set_foreground_wake_callback(
            2,
            Some(Arc::new(ForegroundWakeState::new(
                ForegroundWakeRegistration {
                    callback: queue_native_wake,
                    context: Arc::as_ptr(&reader_wake) as usize,
                },
            ))),
        )
        .unwrap();

    let subscription = reader
        .subscribe_foreground_query_with_options(
            postcard::to_allocvec(&Query::from("todos")).unwrap(),
            ReadOpts {
                tier: jazz::db::ReadTier::Remote,
                ..ReadOpts::default()
            },
        )
        .unwrap();
    for _ in 0..32 {
        relay.pump().unwrap();
    }
    let initial_events = match reader.drain_foreground_subscription(subscription).unwrap() {
        ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(events)) => {
            events
        }
        _ => panic!("initial Global subscription did not drain"),
    };
    assert!(
        initial_events.is_empty(),
        "an unavailable authority keeps the Global subscription opener pending"
    );
    settle_owner(&relay);
    reader_wake.queued.lock().unwrap().clear();
    relay.wire().unwrap().take_outbound_encoded().unwrap();

    let (baseline_storage, baseline_drive_turns, baseline_coverage) = relay
        .run(|worker| {
            Ok((
                worker.persistent.db.sqlite_total_changes_for_test(),
                worker.core_tick_turns,
                worker
                    .persistent
                    .db
                    .query_coverage_attachment_counts_for_test(),
            ))
        })
        .unwrap();
    let baseline_network_depth = relay.wire().unwrap().queue_depths();

    // Shared Binding coalesces schedule requests into one actual owner tick.
    // No JavaScript wake is needed while the pending query has no new result.
    let (after_wake_turns, coalesced_ticks) = relay
        .run(|worker| {
            let before = worker.core_tick_turns;
            worker.persistent.hooks.schedule_tick();
            worker.persistent.hooks.schedule_tick();
            worker.drive();
            Ok((worker.core_tick_turns, worker.core_tick_turns - before))
        })
        .unwrap();
    assert_eq!(
        coalesced_ticks, 1,
        "two storage schedules request one real owner tick"
    );
    assert!(after_wake_turns > baseline_drive_turns);
    assert_eq!(
        reader_wake.queued(),
        0,
        "a pending query with no new result needs no platform callback"
    );
    assert_eq!(
        relay
            .run(|worker| Ok(worker
                .persistent
                .db
                .query_coverage_attachment_counts_for_test()))
            .unwrap(),
        baseline_coverage,
        "an unavailable authority must not duplicate maintained queries"
    );
    assert_eq!(
        relay
            .run(|worker| { Ok(worker.persistent.db.sqlite_total_changes_for_test()) })
            .unwrap(),
        baseline_storage,
        "a liveness wake does not write relay storage"
    );
    assert_eq!(
        relay.wire().unwrap().queue_depths(),
        baseline_network_depth,
        "an unavailable authority does not emit a network retry"
    );
    reader_wake.queued.lock().unwrap().clear();
    let mut idle_wakes = 0;
    for idle_turn in 0..16 {
        relay.pump().unwrap();
        let queued = reader_wake.queued();
        idle_wakes += queued;
        assert!(
            idle_wakes <= 4,
            "unavailable authority exceeded four idle native wakes by turn {idle_turn}"
        );
        if queued == 0 {
            break;
        }
        reader_wake.deliver_queued();
        reader.pump_foreground().unwrap();
    }
    assert_eq!(
        reader_wake.queued(),
        0,
        "bounded idle pumping must reach native-wake quiescence"
    );
    let parked_turns = settle_owner(&relay);
    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(
        settle_owner(&relay),
        parked_turns,
        "an untimed remote wait must not poll the idle owner"
    );
    let idle_drive_turns = settle_owner(&relay);
    assert!(
        idle_drive_turns <= after_wake_turns + 1,
        "idle owner turns exceeded one residual self-scheduled drive"
    );
    assert_eq!(
        relay
            .run(|worker| Ok(worker
                .persistent
                .db
                .query_coverage_attachment_counts_for_test()))
            .unwrap(),
        baseline_coverage,
        "idle owner turns must not duplicate maintained queries"
    );
    assert_eq!(
        relay
            .run(|worker| { Ok(worker.persistent.db.sqlite_total_changes_for_test()) })
            .unwrap(),
        baseline_storage,
        "idle authority waits must not grow relay storage"
    );
    assert_eq!(
        relay.wire().unwrap().queue_depths(),
        baseline_network_depth,
        "idle authority waits must not grow network queues"
    );
    // The pending Global subscription must not monopolise the owner. An
    // unrelated local query must complete through the same foreground RPC
    // surface while the authority remains unavailable.
    let read_started = std::time::Instant::now();
    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
    let mut read = reader
        .start_foreground_read(query, "{}".into(), None)
        .unwrap();
    loop {
        match read {
            ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(_)) => break,
            ForegroundOperationPoll::Pending { operation } => {
                assert!(
                    read_started.elapsed() < Duration::from_secs(1),
                    "unrelated local read must not block the native owner"
                );
                relay.pump().unwrap();
                read = reader.poll_foreground_operation(operation).unwrap();
            }
            _ => panic!("unrelated local read failed"),
        }
    }
    assert!(
        read_started.elapsed() < Duration::from_secs(1),
        "unrelated local read exceeded its one-second owner deadline"
    );

    let relay_identity = relay.inner.identity;
    let core = Rc::new(
        block_on(jazz::db::Db::open_history_complete(DbConfig::new(
            permissive_schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xe5; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap(),
    );
    let (_generation, socket_wire) = relay
        .run(move |worker| Ok(worker.begin_socket_upstream()))
        .unwrap();
    struct RecordingTransport {
        inner: WireTransportAdapter<AuthorityWire>,
        subscriptions: Rc<std::cell::Cell<usize>>,
    }
    impl Transport for RecordingTransport {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            self.inner.send(message)
        }
        fn try_recv(&mut self) -> Option<SyncMessage> {
            let message = self.inner.try_recv()?;
            if matches!(
                &message,
                SyncMessage::Subscribe { query, .. }
                    | SyncMessage::SubscribeReadablePolicyV1 { query, .. }
                    if query.table == "todos"
            ) {
                self.subscriptions.set(self.subscriptions.get() + 1);
            }
            Some(message)
        }
    }
    let subscriptions = Rc::new(std::cell::Cell::new(0));
    let _core_subscriber = core.accept_subscriber(
        Box::new(RecordingTransport {
            inner: WireTransportAdapter::current(AuthorityWire::new(socket_wire)),
            subscriptions: Rc::clone(&subscriptions),
        }),
        relay_identity.author,
    );

    let mut settled_events = Vec::new();
    for _ in 0..96 {
        block_on(core.tick()).unwrap();
        relay.pump().unwrap();
        if reader_wake.queued() != 0 {
            reader_wake.deliver_queued();
            reader.pump_foreground().unwrap();
        }
        if let ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(
            events,
        )) = reader.drain_foreground_subscription(subscription).unwrap()
        {
            settled_events.extend(events.into_iter().filter(|event| {
                matches!(
                    event,
                    ForegroundSubscriptionEvent::Delta { settled: true, .. }
                        | ForegroundSubscriptionEvent::StructuredDelta { settled: true, .. }
                )
            }));
        }
        if !settled_events.is_empty() {
            break;
        }
    }
    assert_eq!(
        settled_events.len(),
        1,
        "authority readiness must settle the retained receiver once"
    );
    for _ in 0..32 {
        block_on(core.tick()).unwrap();
        relay.pump().unwrap();
        if reader_wake.queued() != 0 {
            reader_wake.deliver_queued();
            reader.pump_foreground().unwrap();
        }
        if let ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(
            events,
        )) = reader.drain_foreground_subscription(subscription).unwrap()
        {
            settled_events.extend(events.into_iter().filter(|event| {
                matches!(
                    event,
                    ForegroundSubscriptionEvent::Delta { settled: true, .. }
                        | ForegroundSubscriptionEvent::StructuredDelta { settled: true, .. }
                )
            }));
        }
    }
    assert_eq!(
        settled_events.len(),
        1,
        "extra authority and owner turns must not duplicate recovery"
    );
    assert_eq!(
        subscriptions.get(),
        1,
        "authority recovery must register the retained todos query exactly once"
    );
    reader.close().unwrap();
}

#[test]
fn abi_handshake_accepts_supported_versions_before_storage_opens() {
    assert_eq!(
        ensure_native_relay_abi_compatible(NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1,
            maximum: NATIVE_RELAY_ABI_V1,
        })
        .unwrap(),
        NATIVE_RELAY_ABI_V1
    );
    assert_eq!(
        NativeRelay::ensure_abi_compatible(NativeRelayAbiRange {
            minimum: 0,
            maximum: NATIVE_RELAY_ABI_V1,
        })
        .unwrap(),
        NATIVE_RELAY_ABI_V1
    );
}

#[test]
fn abi_handshake_rejects_invalid_and_incompatible_ranges() {
    assert!(matches!(
        ensure_native_relay_abi_compatible(NativeRelayAbiRange {
            minimum: 2,
            maximum: 1,
        }),
        Err(RelayError::InvalidAbiRange {
            minimum: 2,
            maximum: 1,
        })
    ));
    assert!(matches!(
        ensure_native_relay_abi_compatible(NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1.saturating_add(1),
            maximum: u16::MAX,
        }),
        Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_V1
    ));
}

#[test]
fn incompatible_open_creates_no_relay_or_sqlite_store() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("must-not-exist.sqlite");
    let mut open = config(sqlite_path.clone(), Some("alice"));
    open.supported_abi = NativeRelayAbiRange {
        minimum: NATIVE_RELAY_ABI_V1.saturating_add(1),
        maximum: u16::MAX,
    };
    let registry = NativeRelayRegistry::default();

    assert!(matches!(
        registry.open(open),
        Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_V1
    ));
    assert!(
        !sqlite_path.exists(),
        "ABI rejection must happen before SQLite creates a database"
    );
    assert!(
        registry.relays.lock().unwrap().is_empty(),
        "ABI rejection must not register a partially-open relay"
    );
}

#[test]
fn direct_incompatible_spawn_creates_no_owner_thread_or_sqlite_store() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("must-not-exist-direct.sqlite");
    let mut open = config(sqlite_path.clone(), Some("alice"));
    open.supported_abi = NativeRelayAbiRange {
        minimum: NATIVE_RELAY_ABI_V1.saturating_add(1),
        maximum: u16::MAX,
    };
    let threads_started = Arc::new(AtomicUsize::new(0));
    open.thread_start_counter = Some(Arc::clone(&threads_started));

    assert!(matches!(
        NativeRelay::spawn(open),
        Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_V1
    ));
    assert_eq!(threads_started.load(Ordering::Relaxed), 0);
    assert!(
        !sqlite_path.exists(),
        "ABI rejection must happen before SQLite creates a database"
    );
}

#[test]
fn encoded_peer_messages_use_the_shared_postcard_contract() {
    let wire = NativeRelayWire::default();
    let message = SyncMessage::Fetch {
        view: jazz::protocol::ReadViewSourceSpec::Current,
        id: 1,
        query: Query::from("todos"),
        binding: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
        at: None,
        include_deleted: false,
        tier: jazz::db::ReadTier::LocalFirst,
        wait: None,
    };
    let bytes = encode_sync_message(&message).unwrap();

    wire.push_inbound_encoded(&bytes).unwrap();
    assert_eq!(
        wire.inbound
            .lock()
            .unwrap()
            .pop()
            .map(|message| encode_sync_message(&message).unwrap()),
        Some(bytes.clone())
    );

    wire.outbound
        .lock()
        .unwrap()
        .push(message, "test outbound")
        .unwrap();
    let encoded = wire.take_outbound_encoded().unwrap();
    assert_eq!(encoded.len(), 1);
    assert_eq!(wire.outbound.lock().unwrap().len(), 0);
    assert_eq!(
        encode_sync_message(&decode_sync_message(&encoded[0]).unwrap()).unwrap(),
        encode_sync_message(&SyncMessage::Fetch {
            view: jazz::protocol::ReadViewSourceSpec::Current,
            id: 1,
            query: Query::from("todos"),
            binding: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
            at: None,
            include_deleted: false,
            tier: jazz::db::ReadTier::LocalFirst,
            wait: None,
        })
        .unwrap()
    );
}

#[test]
fn native_upstream_bridge_uses_the_negotiated_wire_adapter_in_both_directions() {
    let outbound = SyncMessage::Unsubscribe { id: 0x61 };
    let inbound = SyncMessage::Unsubscribe { id: 0x62 };
    let relay_wire = NativeRelayWire::default();
    relay_wire
        .outbound
        .lock()
        .unwrap()
        .push(outbound.clone(), "test relay outbound")
        .unwrap();

    // The peer and relay use SQLite semantic frames after the connector's
    // negotiated handshake. The bridge must cross the byte adapter in both
    // directions without duplicating or dropping a message.
    struct DuplexWire {
        inbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
        outbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
    }
    impl WireTransport for DuplexWire {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            self.outbound.lock().unwrap().push_back(frame);
            Ok(())
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.inbound.lock().unwrap().pop_front()
        }
    }
    let to_relay = Arc::new(Mutex::new(VecDeque::new()));
    let to_peer = Arc::new(Mutex::new(VecDeque::new()));
    let mut peer = WireTransportAdapter::current(DuplexWire {
        inbound: Arc::clone(&to_peer),
        outbound: Arc::clone(&to_relay),
    });
    let mut upstream = NativeRelayTransport::current(DuplexWire {
        inbound: to_relay,
        outbound: to_peer,
    });
    peer.send(inbound.clone()).unwrap();
    assert!(bridge_native_relay_wire_once_classified(&relay_wire, &mut upstream).unwrap());
    assert_eq!(
        relay_wire
            .inbound
            .lock()
            .unwrap()
            .pop()
            .map(|message| encode_sync_message(&message).unwrap()),
        Some(encode_sync_message(&inbound).unwrap())
    );
    assert_eq!(
        peer.try_recv()
            .map(|message| encode_sync_message(&message).unwrap()),
        Some(encode_sync_message(&outbound).unwrap())
    );
    assert_eq!(
        peer.try_recv()
            .map(|message| encode_sync_message(&message).unwrap()),
        None,
        "the bridge emits no extra semantic messages"
    );
}

// Internal bridge seam: the server's error frame and its socket close race,
// and only this seam can deterministically land the error first.
#[test]
fn native_upstream_bridge_leaves_a_server_ended_link_to_the_transport_terminal() {
    assert!(matches!(
        bridge_after_remote_error(
            jazz::wire::WireErrorCode::Backpressure,
            jazz::wire::WireRetry::Later,
        ),
        Err(NativeRelayWireBridgeError::SocketPumpClosed)
    ));
    // Errors that retrying cannot fix still stop the worker.
    for (code, retry) in [
        (
            jazz::wire::WireErrorCode::AuthFailed,
            jazz::wire::WireRetry::AfterAuth,
        ),
        (
            jazz::wire::WireErrorCode::MalformedFrame,
            jazz::wire::WireRetry::Never,
        ),
        (
            jazz::wire::WireErrorCode::Backpressure,
            jazz::wire::WireRetry::Never,
        ),
    ] {
        assert!(matches!(
            bridge_after_remote_error(code, retry),
            Err(NativeRelayWireBridgeError::Relay(_))
        ));
    }
}

fn bridge_after_remote_error(
    code: jazz::wire::WireErrorCode,
    retry: jazz::wire::WireRetry,
) -> Result<bool, NativeRelayWireBridgeError> {
    struct ScriptedWire {
        inbound: VecDeque<Vec<u8>>,
    }
    impl WireTransport for ScriptedWire {
        fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
            Ok(())
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.inbound.pop_front()
        }
    }
    let error = jazz::wire::encode_frame(&jazz::wire::WireFrame::Error(
        jazz::wire::WireError::new(code, retry, "server ended the link"),
    ))
    .unwrap();
    let mut upstream = NativeRelayTransport::current(ScriptedWire {
        inbound: VecDeque::from([error]),
    });
    let relay_wire = NativeRelayWire::default();

    // SQLite classifies the terminal frame as soon as it is read, before
    // a later queued write encounters the same latched adapter failure.
    let first = bridge_native_relay_wire_once_classified(&relay_wire, &mut upstream);
    let remote = upstream.remote_wire_error().expect("latched remote error");
    if remote.asks_reconnect_later() {
        assert!(matches!(
            first,
            Err(NativeRelayWireBridgeError::SocketPumpClosed)
        ));
    } else {
        assert!(matches!(first, Err(NativeRelayWireBridgeError::Relay(_))));
    }

    // A write queued before the close arrives reaches the latched adapter.
    relay_wire
        .outbound
        .lock()
        .unwrap()
        .push(SyncMessage::Unsubscribe { id: 0x63 }, "test relay outbound")
        .unwrap();
    bridge_native_relay_wire_once_classified(&relay_wire, &mut upstream)
}

// Internal bridge seam: a slow native socket is only observable here as
// `TransportError::Backpressure` from the adapter, and the public ClientDb
// path cannot deterministically stall the platform WebSocket pump.
#[test]
fn native_upstream_bridge_retains_drained_messages_under_backpressure() {
    struct GatedWire {
        open: Arc<AtomicBool>,
        inbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
        outbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
    }
    impl WireTransport for GatedWire {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            if !self.open.load(Ordering::Acquire) {
                return Err(TransportError::Backpressure);
            }
            self.outbound.lock().unwrap().push_back(frame);
            Ok(())
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.inbound.lock().unwrap().pop_front()
        }
    }
    struct DuplexWire {
        inbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
        outbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
    }
    impl WireTransport for DuplexWire {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            self.outbound.lock().unwrap().push_back(frame);
            Ok(())
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.inbound.lock().unwrap().pop_front()
        }
    }
    let message = |index: usize| SyncMessage::Unsubscribe { id: index as u64 };

    let open = Arc::new(AtomicBool::new(false));
    let to_relay = Arc::new(Mutex::new(VecDeque::new()));
    let to_peer = Arc::new(Mutex::new(VecDeque::new()));
    let mut upstream = NativeRelayTransport::current(GatedWire {
        open: Arc::clone(&open),
        inbound: Arc::clone(&to_relay),
        outbound: Arc::clone(&to_peer),
    });
    let mut peer = WireTransportAdapter::current(DuplexWire {
        inbound: to_peer,
        outbound: to_relay,
    });
    let relay_wire = NativeRelayWire::default();

    // With the socket stalled, the relay retains its bounded backlog.
    // Keep producing beyond that bound so draining and retry must preserve
    // both the rejected batch and later queued messages.
    let total = 2 * NATIVE_RELAY_QUEUE_MAX_MESSAGES;
    let mut produced = 0;
    let mut received = Vec::new();
    let mut rounds = 0;
    while received.len() < total {
        while produced < total
            && relay_wire
                .outbound
                .lock()
                .unwrap()
                .push(message(produced), "test relay outbound")
                .is_ok()
        {
            produced += 1;
        }
        bridge_native_relay_wire_once_classified(&relay_wire, &mut upstream)
            .expect("backpressure is a transient transport state");
        rounds += 1;
        if rounds == 64 {
            // The stalled socket drains; the bridge must resume in order.
            open.store(true, Ordering::Release);
        }
        // SQLite's carrier sends each semantic frame directly; the next
        // bridge turn retries the retained batch after the socket opens.
        while let Some(message) = peer.try_recv() {
            received.push(message);
        }
        assert!(rounds < 100_000, "bridge made no progress");
    }
    assert_eq!(relay_wire.queue_depths().1, 0);
    assert_eq!(
        received
            .iter()
            .map(|message| encode_sync_message(message).unwrap())
            .collect::<Vec<_>>(),
        (0..total)
            .map(message)
            .map(|message| encode_sync_message(&message).unwrap())
            .collect::<Vec<_>>(),
        "every drained message is delivered exactly once, in order"
    );
}

// Internal queue seam: a host cannot deliberately block the relay owner.
#[test]
fn review_socket_install_recovers_after_owner_queue_saturation() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("socket-capacity.sqlite"),
        Some("socket-capacity"),
    ))
    .unwrap();
    let saturated = saturate_owner(&relay);
    let (events_tx, events_rx) = mpsc::channel();
    let connector = Arc::new(ReconnectingTestConnector {
        calls: AtomicUsize::new(0),
        bearer_seen: Arc::new(Mutex::new(Vec::new())),
    });
    let worker = NativeRelaySocketWorker::start_with_connector(
        relay,
        NativeRelaySocketConfig {
            server_url: "https://edge.example".into(),
            app_id: AppId::from_name("socket-capacity"),
            peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
            auth: AuthConfig {
                jwt_token: Some("edge-validated-bearer".into()),
                ..AuthConfig::default()
            },
            reconnect_delay: std::time::Duration::from_millis(1),
            on_event: Arc::new(move |event| {
                let _ = events_tx.send(event);
            }),
        },
        connector.clone(),
    )
    .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while connector.calls.load(Ordering::Acquire) < 1 {
        if std::time::Instant::now() >= deadline {
            saturated.release_and_wait();
            panic!("connector did not reach installation");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let premature_event = events_rx.recv_timeout(Duration::from_millis(20));
    saturated.release_and_wait();
    assert!(matches!(
        premature_event,
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    let mut connected = false;
    for _ in 0..8 {
        match events_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap()
        {
            NativeRelaySocketEvent::Connected => {
                connected = true;
                break;
            }
            NativeRelaySocketEvent::Stopped => break,
            _ => {}
        }
    }
    worker.cancel();
    assert!(
        connected,
        "transient owner backpressure must not permanently stop socket installation"
    );
}

#[test]
fn socket_install_queue_retry_cancels_before_admission() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("socket-capacity.sqlite"),
        Some("socket-capacity"),
    ))
    .unwrap();
    let saturated = saturate_owner(&relay);
    let (events_tx, events_rx) = mpsc::channel();
    let connector = Arc::new(ReconnectingTestConnector {
        calls: AtomicUsize::new(0),
        bearer_seen: Arc::new(Mutex::new(Vec::new())),
    });
    let worker = NativeRelaySocketWorker::start_with_connector(
        relay.clone(),
        NativeRelaySocketConfig {
            server_url: "https://edge.example".into(),
            app_id: AppId::from_name("socket-capacity"),
            peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
            auth: AuthConfig {
                jwt_token: Some("edge-validated-bearer".into()),
                ..AuthConfig::default()
            },
            reconnect_delay: std::time::Duration::from_millis(1),
            on_event: Arc::new(move |event| {
                let _ = events_tx.send(event);
            }),
        },
        connector.clone(),
    )
    .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while connector.calls.load(Ordering::Acquire) < 1 {
        if std::time::Instant::now() >= deadline {
            saturated.release_and_wait();
            panic!("connector did not reach installation");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    worker.cancel();
    let stopped = events_rx.recv_timeout(Duration::from_secs(1));
    saturated.release_and_wait();
    assert_eq!(stopped.unwrap(), NativeRelaySocketEvent::Stopped);
    assert_eq!(relay.run(|worker| Ok(worker.generation)).unwrap(), 0);
}

#[test]
fn closed_bridge_uses_its_owned_terminal_before_reconnecting() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("closed-bridge.sqlite"),
        Some("closed-bridge"),
    ))
    .unwrap();
    let (events_tx, events_rx) = mpsc::channel();
    let connector = Arc::new(ClosedPumpTestConnector {
        calls: AtomicUsize::new(0),
        first_terminal: Arc::new(Mutex::new(None)),
    });
    let worker = NativeRelaySocketWorker::start_with_connector(
        relay.clone(),
        NativeRelaySocketConfig {
            server_url: "https://edge.example".into(),
            app_id: AppId::from_name("closed-bridge"),
            peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
            auth: AuthConfig {
                jwt_token: Some("edge-validated-bearer".into()),
                ..AuthConfig::default()
            },
            reconnect_delay: std::time::Duration::ZERO,
            on_event: Arc::new(move |event| {
                let _ = events_tx.send(event);
            }),
        },
        connector.clone(),
    )
    .unwrap();
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::Connected
    );
    relay
        .run(|worker| {
            worker
                .upstream
                .as_ref()
                .expect("first socket is installed")
                .wire
                .outbound
                .lock()
                .unwrap()
                .push(SyncMessage::Unsubscribe { id: 0 }, "test closed bridge")
        })
        .unwrap();
    let terminal = connector
        .first_terminal
        .lock()
        .unwrap()
        .take()
        .expect("first connector published its terminal sender");
    terminal
        .send(NativeTransportTerminal::Failed(
            NativeTransportError::Terminal("malformed peer wire batch".into()),
        ))
        .unwrap();
    assert!(matches!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::TerminalError(error)
            if error.contains("malformed peer wire batch")
    ));
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::Reconnecting
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::Connected
    );
    worker.cancel();
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::Stopped
    );
}

#[test]
fn cancellation_does_not_wait_for_a_closed_bridge_terminal() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("closed-bridge-cancel.sqlite"),
        Some("closed-bridge-cancel"),
    ))
    .unwrap();
    let (events_tx, events_rx) = mpsc::channel();
    let connector = Arc::new(ClosedPumpTestConnector {
        calls: AtomicUsize::new(0),
        first_terminal: Arc::new(Mutex::new(None)),
    });
    let worker = NativeRelaySocketWorker::start_with_connector(
        relay.clone(),
        NativeRelaySocketConfig {
            server_url: "https://edge.example".into(),
            app_id: AppId::from_name("closed-bridge-cancel"),
            peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
            auth: AuthConfig {
                jwt_token: Some("edge-validated-bearer".into()),
                ..AuthConfig::default()
            },
            reconnect_delay: std::time::Duration::ZERO,
            on_event: Arc::new(move |event| {
                let _ = events_tx.send(event);
            }),
        },
        connector,
    )
    .unwrap();
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::Connected
    );
    relay
        .run(|worker| {
            worker
                .upstream
                .as_ref()
                .expect("first socket is installed")
                .wire
                .outbound
                .lock()
                .unwrap()
                .push(
                    SyncMessage::Unsubscribe { id: 0 },
                    "test closed bridge cancellation",
                )
        })
        .unwrap();
    worker.cancel();
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::Stopped
    );
}

#[test]
fn owner_close_is_terminal_and_never_resurrects_the_socket() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("owner-close.sqlite"),
        Some("owner-close"),
    ))
    .unwrap();
    let (events_tx, events_rx) = mpsc::channel();
    let connector = Arc::new(PendingTestConnector {
        calls: AtomicUsize::new(0),
    });
    let worker = NativeRelaySocketWorker::start_with_connector(
        relay.clone(),
        NativeRelaySocketConfig {
            server_url: "https://edge.example".into(),
            app_id: AppId::from_name("owner-close"),
            peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
            auth: AuthConfig {
                jwt_token: Some("edge-validated-bearer".into()),
                ..AuthConfig::default()
            },
            reconnect_delay: std::time::Duration::ZERO,
            on_event: Arc::new(move |event| {
                let _ = events_tx.send(event);
            }),
        },
        connector.clone(),
    )
    .unwrap();
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::Connected
    );
    relay.inner.shutdown().unwrap();
    assert!(matches!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::TerminalError(error) if error.contains("native relay is closed")
    ));
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        NativeRelaySocketEvent::Stopped
    );
    assert_eq!(connector.calls.load(Ordering::Acquire), 1);
    drop(worker);
}

#[test]
fn native_socket_worker_reauthenticates_on_reconnect_and_cancels_cleanly() {
    let directory = tempfile::tempdir().unwrap();
    let relay =
        NativeRelay::spawn(config(directory.path().join("relay.sqlite"), Some("alice"))).unwrap();
    let (events_tx, events_rx) = mpsc::channel();
    let bearer_seen = Arc::new(Mutex::new(Vec::new()));
    let connector = Arc::new(ReconnectingTestConnector {
        calls: AtomicUsize::new(0),
        bearer_seen: Arc::clone(&bearer_seen),
    });
    let worker = NativeRelaySocketWorker::start_with_connector(
        relay,
        NativeRelaySocketConfig {
            server_url: "https://edge.example".to_owned(),
            app_id: AppId::from_name("native-relay-socket-test"),
            peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
            auth: AuthConfig {
                jwt_token: Some("edge-validated-bearer".to_owned()),
                ..AuthConfig::default()
            },
            reconnect_delay: std::time::Duration::ZERO,
            on_event: Arc::new(move |event| {
                let _ = events_tx.send(event);
            }),
        },
        connector,
    )
    .unwrap();

    assert_eq!(
        events_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        NativeRelaySocketEvent::Connected
    );
    assert_eq!(
        events_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        NativeRelaySocketEvent::Reconnecting
    );
    assert_eq!(
        events_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        NativeRelaySocketEvent::Connected
    );
    worker.cancel();
    assert_eq!(
        events_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        NativeRelaySocketEvent::Stopped
    );
    drop(worker);
    assert_eq!(
        bearer_seen.lock().unwrap().as_slice(),
        ["edge-validated-bearer", "edge-validated-bearer"],
        "each reconnect sends the bearer to Core again, without exposing it to relay state"
    );
}

/// Alice feeds an encoded server row frame through her foreground boundary.
/// It is consumed by the actual host link and must never be echoed into the
/// opposite direction. The carrier carries versioned bytes end to end.
#[test]
fn client_transport_keeps_the_wire_boundary_opaque_and_directional() {
    let (_directory, relay, client, _wake) = attached_write_client("wire-direction", 0x92);
    settle_owner(&relay);
    let frame = encode_sync_message(&SyncMessage::Rows { images: Vec::new() }).unwrap();
    client.push_frame(frame.clone()).unwrap();
    relay.pump().unwrap();
    assert!(
        client
            .take_frames()
            .unwrap()
            .iter()
            .all(|outbound| outbound != &frame),
        "received rows cannot be echoed as outbound rows"
    );
    assert!(
        streaming_rows(&client).is_empty(),
        "empty frame cannot invent a row"
    );
    client.close().unwrap();
}

#[test]
fn encoded_peer_messages_reject_invalid_bytes() {
    assert!(matches!(
        NativeRelayWire::default().push_inbound_encoded(&[0xff]),
        Err(RelayError::DecodePeerMessage(_))
    ));
}

#[test]
fn encoded_peer_messages_reject_the_exact_logical_message_limit_boundary() {
    let bytes = vec![0; MAX_LOGICAL_MESSAGE_BYTES + 1];
    assert!(matches!(
        NativeRelayWire::default().push_inbound_encoded(&bytes),
        Err(RelayError::PeerMessageTooLarge(message))
            if message.contains(&(MAX_LOGICAL_MESSAGE_BYTES + 1).to_string())
    ));
}

#[test]
fn native_queue_applies_count_backpressure_without_losing_admitted_messages() {
    let message = SyncMessage::Unsubscribe { id: 0 };
    let wire = NativeRelayWire::default();
    for _ in 0..NATIVE_RELAY_QUEUE_MAX_MESSAGES {
        wire.push_inbound(message.clone()).unwrap();
    }
    assert!(matches!(
        wire.push_inbound(message),
        Err(RelayError::QueueCapacityExceeded {
            queued_messages: NATIVE_RELAY_QUEUE_MAX_MESSAGES,
            ..
        })
    ));
    assert_eq!(wire.queue_depths().0, NATIVE_RELAY_QUEUE_MAX_MESSAGES);
}

#[test]
fn native_queue_drains_in_bounded_batches() {
    let wire = NativeRelayWire::default();
    for _ in 0..(NATIVE_RELAY_DRAIN_MAX_MESSAGES + 1) {
        wire.outbound
            .lock()
            .unwrap()
            .push(SyncMessage::Unsubscribe { id: 0 }, "test outbound")
            .unwrap();
    }
    assert_eq!(
        wire.take_outbound_encoded().unwrap().len(),
        NATIVE_RELAY_DRAIN_MAX_MESSAGES
    );
    assert_eq!(wire.queue_depths().1, 1);
}

/// Alice's owner serves more than one batch of foregrounds. It must yield
/// after 64 foregrounds, visit the remaining siblings on the next drive, then
/// become idle. Ready futures count real owner polls; a public timer cannot
/// reliably observe the boundary between two owner turns.
#[test]
fn owner_drive_yields_between_bounded_foreground_batches() {
    let (_directory, relay, _client, _wake) = attached_write_client("bounded-drive", 0x69);
    relay
        .run(|worker| {
            for _ in 1..67 {
                worker.attach(
                    fresh_client_identity(AuthorSubject::for_test_bytes([0x69; 16])).unwrap(),
                    BTreeMap::new(),
                    0,
                )?;
            }
            let polls = Arc::new(AtomicUsize::new(0));
            for foreground in worker.clients.values_mut() {
                let polls = polls.clone();
                foreground.pending.insert(
                    1000,
                    PendingOperation {
                        advice: None,
                        future: Box::pin(std::future::poll_fn(move |_| {
                            polls.fetch_add(1, Ordering::SeqCst);
                            Poll::Ready(Ok(ForegroundDbCommandResponse::Rows { rows: Vec::new() }))
                        })),
                    },
                );
            }
            worker.drive();
            assert_eq!(
                polls.load(Ordering::SeqCst),
                64,
                "one owner drive must bound its foreground work"
            );
            worker.drive();
            assert_eq!(
                polls.load(Ordering::SeqCst),
                67,
                "the next drive must include previously skipped siblings"
            );
            for _ in 0..16 {
                if !worker.redrive {
                    break;
                }
                worker.drive();
            }
            assert!(
                !worker.redrive,
                "bounded batches must reach idle instead of scheduling forever"
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn pump_client_selection_is_bounded_and_round_robins() {
    let clients = (1..=(NATIVE_RELAY_PUMP_MAX_CLIENTS as u64 + 3))
        .map(|id| (id, ()))
        .collect::<BTreeMap<_, _>>();
    let first = bounded_round_robin_ids(&clients, None);
    assert_eq!(first.len(), NATIVE_RELAY_PUMP_MAX_CLIENTS);
    assert_eq!(first[0], 1);
    let second = bounded_round_robin_ids(&clients, first.last().copied());
    assert_eq!(&second[..3], &[65, 66, 67]);
    assert_eq!(second[3], 1);
}

#[test]
fn c_host_lifecycle_open_attach_close_and_bounded_pump_are_handle_safe() {
    let directory = tempfile::tempdir().unwrap();
    let host = jazz_native_relay_host_new();
    assert!(!host.is_null());
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0x71; 16]),
        author: AuthorSubject::for_test_bytes([0x72; 16]),
    };
    let admission = RelayScopeAdmissionRequest {
        scope: RelayScopeRequest {
            app_namespace: "host-receipt".to_owned(),
            storage_namespace: "default".to_owned(),
            auth_scope: Some("opaque-subject".to_owned()),
        },
        sqlite_path: directory.path().join("host.sqlite").display().to_string(),
        schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
        identity,
        claims: BTreeMap::new(),
    };
    let admitted_scope = unsafe { (*host).inner.lock().unwrap().admit_scope(admission) }
        .expect("test admission is valid");
    let open = RelayCommandRequest::Open {
        supported_abi_minimum: NATIVE_RELAY_ABI_V1,
        supported_abi_maximum: NATIVE_RELAY_ABI_V1,
        admitted_scope,
    };
    unsafe fn command(
        host: *mut JazzNativeRelayHost,
        request: RelayCommandRequest,
    ) -> Result<RelayCommandResponse, JazzNativeRelayStatus> {
        let bytes = postcard::to_allocvec(&request).unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        let status = unsafe {
            jazz_native_relay_host_execute(host, bytes.as_ptr(), bytes.len(), &mut output)
        };
        if !matches!(status, JazzNativeRelayStatus::Ok) {
            return Err(status);
        }
        let response =
            postcard::from_bytes(unsafe { std::slice::from_raw_parts(output.data, output.len) })
                .unwrap();
        unsafe { jazz_native_relay_bytes_free(&mut output) };
        Ok(response)
    }
    let relay = match unsafe { command(host, open.clone()) }.unwrap() {
        RelayCommandResponse::Opened { relay } => relay,
        response => panic!("unexpected open response: {response:?}"),
    };
    let alias = match unsafe { command(host, open.clone()) }.unwrap() {
        RelayCommandResponse::Opened { relay } => relay,
        response => panic!("unexpected alias response: {response:?}"),
    };
    assert_ne!(relay, alias);
    unsafe {
        let host = (*host).inner.lock().unwrap();
        assert!(Arc::ptr_eq(
            &host.relays.get(&relay).unwrap().relay.inner,
            &host.relays.get(&alias).unwrap().relay.inner,
        ));
    }
    let client = match unsafe { command(host, RelayCommandRequest::Attach { relay }) }.unwrap() {
        RelayCommandResponse::Attached { client } => client,
        response => panic!("unexpected attach response: {response:?}"),
    };
    assert!(matches!(
        unsafe { command(host, RelayCommandRequest::Pump { relay }) },
        Ok(RelayCommandResponse::Pumped)
    ));
    assert_eq!(
        unsafe { command(host, RelayCommandRequest::CloseRelay { relay }) }.unwrap(),
        RelayCommandResponse::Closed { closed: true }
    );
    assert!(matches!(
        unsafe { command(host, RelayCommandRequest::Pump { relay: alias }) },
        Ok(RelayCommandResponse::Pumped)
    ));
    assert_eq!(
        unsafe { command(host, RelayCommandRequest::CloseClient { client }) }.unwrap(),
        RelayCommandResponse::Closed { closed: false }
    );
    let reopened = match unsafe { command(host, open) }.unwrap() {
        RelayCommandResponse::Opened { relay } => relay,
        response => panic!("unexpected reopen response: {response:?}"),
    };
    unsafe {
        let host = (*host).inner.lock().unwrap();
        assert!(Arc::ptr_eq(
            &host.relays.get(&alias).unwrap().relay.inner,
            &host.relays.get(&reopened).unwrap().relay.inner,
        ));
    }
    assert_eq!(
        unsafe { command(host, RelayCommandRequest::CloseRelay { relay }) }.unwrap(),
        RelayCommandResponse::Closed { closed: false }
    );
    assert!(matches!(
        unsafe { command(host, RelayCommandRequest::Pump { relay }) },
        Err(JazzNativeRelayStatus::InvalidHandle)
    ));
    assert_eq!(
        unsafe { command(host, RelayCommandRequest::CloseRelay { relay: alias }) }.unwrap(),
        RelayCommandResponse::Closed { closed: true }
    );
    assert!(matches!(
        unsafe { command(host, RelayCommandRequest::Pump { relay: reopened }) },
        Ok(RelayCommandResponse::Pumped)
    ));
    assert_eq!(
        unsafe { command(host, RelayCommandRequest::CloseRelay { relay: reopened }) }.unwrap(),
        RelayCommandResponse::Closed { closed: true }
    );
    unsafe { jazz_native_relay_host_free(host) };
}

#[test]
fn c_host_open_rejects_wrapper_abi_before_storage_and_resets_output() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("must-not-open.sqlite");
    let host = jazz_native_relay_host_new();
    let threads_started = Arc::new(AtomicUsize::new(0));
    unsafe {
        (*host).inner.lock().unwrap().thread_start_counter = Some(Arc::clone(&threads_started))
    };
    let admitted_scope = unsafe {
        (*host)
            .inner
            .lock()
            .unwrap()
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "abi-rejection".to_owned(),
                    storage_namespace: "default".to_owned(),
                    auth_scope: Some("opaque-subject".to_owned()),
                },
                sqlite_path: sqlite_path.display().to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0x81; 16]),
                    author: AuthorSubject::for_test_bytes([0x82; 16]),
                },
                claims: BTreeMap::new(),
            })
            .unwrap()
    };
    let request = |minimum, maximum| RelayCommandRequest::Open {
        supported_abi_minimum: minimum,
        supported_abi_maximum: maximum,
        admitted_scope,
    };
    for (request, expected) in [
        (
            request(NATIVE_RELAY_ABI_V1 + 2, NATIVE_RELAY_ABI_V1 + 1),
            JazzNativeRelayStatus::InvalidAbiRange,
        ),
        (
            request(NATIVE_RELAY_ABI_V1 + 1, NATIVE_RELAY_ABI_V1 + 1),
            JazzNativeRelayStatus::IncompatibleAbi,
        ),
    ] {
        let encoded = postcard::to_allocvec(&request).unwrap();
        let mut output = JazzNativeRelayBytes {
            data: std::ptr::dangling_mut(),
            len: 99,
        };
        assert_eq!(
            unsafe {
                jazz_native_relay_host_execute(host, encoded.as_ptr(), encoded.len(), &mut output)
            },
            expected
        );
        assert!(output.data.is_null());
        assert_eq!(output.len, 0);
        assert!(!sqlite_path.exists());
        assert_eq!(threads_started.load(Ordering::Relaxed), 0);
    }
    let mut output = JazzNativeRelayBytes {
        data: std::ptr::dangling_mut(),
        len: 77,
    };
    assert_eq!(
        unsafe {
            jazz_native_relay_host_execute(std::ptr::null_mut(), std::ptr::null(), 1, &mut output)
        },
        JazzNativeRelayStatus::InvalidArgument
    );
    assert!(output.data.is_null());
    assert_eq!(output.len, 0);
    unsafe { jazz_native_relay_host_free(host) };
}

// This is necessarily an internal ABI-boundary test: Kotlin/Swift do not
// run in this Rust target, but the production C entry proves their only
// permitted admission path rejects malformed/credential-bearing config
// before a JavaScript-visible capability exists.
#[test]
fn trusted_json_admission_is_strict_bounded_and_never_echoes_config() {
    let directory = tempfile::tempdir().unwrap();
    let host = jazz_native_relay_host_new();
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0x91; 16]),
        author: AuthorSubject::for_test_bytes([0x92; 16]),
    };
    let request = |claims: BTreeMap<String, Value>| {
        serde_json::json!({
            "scope": {
                "app_namespace": "trusted-host",
                "storage_namespace": "primary",
                "auth_scope": "opaque-validated-subject",
            },
            "sqlite_path": directory.path().join("trusted.sqlite").display().to_string(),
            "schema_json": serde_json::to_string(schema().public_schema()).unwrap(),
            "identity": serde_json::to_value(identity).unwrap(),
            "claims": serde_json::to_value(claims).unwrap(),
        })
    };

    // The platform-owned C ABI is the only production path that can mint
    // a capability. Exercise the JSON forms serde would otherwise map to
    // `Option::None` here, and prove they fail before either registry can
    // change. This is intentionally an ABI-boundary test rather than a
    // Rust-only `RelayScope` unit test.
    for (case, auth_scope) in [
        ("omitted", None),
        ("null", Some(serde_json::Value::Null)),
        ("empty", Some(serde_json::Value::String(String::new()))),
        (
            "whitespace",
            Some(serde_json::Value::String(" \t\n ".to_owned())),
        ),
    ] {
        let mut rejected = request(BTreeMap::new());
        let scope = rejected["scope"]
            .as_object_mut()
            .expect("trusted request has an object scope");
        match auth_scope {
            Some(auth_scope) => {
                scope.insert("auth_scope".to_owned(), auth_scope);
            }
            None => {
                scope.remove("auth_scope");
            }
        }
        let rejected = serde_json::to_vec(&rejected).unwrap();
        let mut output = JazzNativeRelayBytes {
            data: std::ptr::dangling_mut(),
            len: 99,
        };
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    host,
                    rejected.as_ptr(),
                    rejected.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::LifecycleFailure,
            "{case} auth scope must fail closed",
        );
        assert!(output.data.is_null(), "{case} must not return a capability");
        assert_eq!(output.len, 0, "{case} must not return a capability");
        let host_state = unsafe { (*host).inner.lock().unwrap() };
        assert!(
            host_state.admitted_scopes.is_empty(),
            "{case} must not mutate the admission registry"
        );
        assert!(host_state.relays.is_empty(), "{case} must not open a relay");
    }

    let encoded = serde_json::to_vec(&request(BTreeMap::from([(
        "role".to_owned(),
        Value::String("member".to_owned()),
    )])))
    .unwrap();
    let mut output = JazzNativeRelayBytes::EMPTY;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_admit_scope_json(
                host,
                encoded.as_ptr(),
                encoded.len(),
                &mut output,
            )
        },
        JazzNativeRelayStatus::Ok
    );
    assert_eq!(output.len, 32, "only the opaque capability crosses out");
    assert_ne!(
        unsafe { std::slice::from_raw_parts(output.data, output.len) },
        encoded.as_slice(),
        "the trusted config must never be reflected to JavaScript"
    );
    unsafe { jazz_native_relay_bytes_free(&mut output) };

    let mut unknown = request(BTreeMap::new());
    unknown["unexpected"] = serde_json::Value::Bool(true);
    let unknown = serde_json::to_vec(&unknown).unwrap();
    assert_eq!(
        unsafe {
            jazz_native_relay_host_admit_scope_json(
                host,
                unknown.as_ptr(),
                unknown.len(),
                &mut output,
            )
        },
        JazzNativeRelayStatus::InvalidCommand
    );

    let mut bearer = request(BTreeMap::from([(
        "access_token".to_owned(),
        Value::String("never-persist-a-bearer".to_owned()),
    )]));
    // Keep this otherwise-valid request outside the already-admitted
    // scope, so rejection proves the credential filter rather than the
    // immutable-scope configuration check below.
    bearer["scope"]["auth_scope"] = serde_json::Value::String("different-valid-subject".to_owned());
    let bearer = serde_json::to_vec(&bearer).unwrap();
    assert_eq!(
        unsafe {
            jazz_native_relay_host_admit_scope_json(
                host,
                bearer.as_ptr(),
                bearer.len(),
                &mut output,
            )
        },
        JazzNativeRelayStatus::LifecycleFailure
    );

    let oversized = vec![b'x'; NATIVE_RELAY_ADMISSION_MAX_BYTES + 1];
    assert_eq!(
        unsafe {
            jazz_native_relay_host_admit_scope_json(
                host,
                oversized.as_ptr(),
                oversized.len(),
                &mut output,
            )
        },
        JazzNativeRelayStatus::InvalidArgument
    );
    assert!(output.data.is_null());
    assert_eq!(output.len, 0);
    unsafe { jazz_native_relay_host_free(host) };
}

#[test]
fn trusted_admission_rejects_conflicting_scope_before_open() {
    let directory = tempfile::tempdir().unwrap();
    let mut host = NativeRelayHost::default();
    let admission = |sqlite_path: &str,
                     schema_json: String,
                     identity: DbIdentity,
                     claims: BTreeMap<String, Value>| {
        RelayScopeAdmissionRequest {
            scope: RelayScopeRequest {
                app_namespace: "trusted-host".to_owned(),
                storage_namespace: "primary".to_owned(),
                auth_scope: Some("opaque-validated-subject".to_owned()),
            },
            sqlite_path: sqlite_path.to_owned(),
            schema_json,
            identity,
            claims,
        }
    };
    let primary = directory
        .path()
        .join("primary.sqlite")
        .display()
        .to_string();
    let schema_json = serde_json::to_string(schema().public_schema()).unwrap();
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xa1; 16]),
        author: AuthorSubject::for_test_bytes([0xa2; 16]),
    };
    let claims = BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]);
    host.admit_scope(admission(
        &primary,
        schema_json.clone(),
        identity,
        claims.clone(),
    ))
    .unwrap();

    let other_path = directory.path().join("other.sqlite").display().to_string();
    let other_schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("other").column("title", ColumnType::Text))
            .build(),
    )
    .unwrap();
    let other_identity = DbIdentity {
        node: NodeUuid::from_bytes([0xb1; 16]),
        author: AuthorSubject::for_test_bytes([0xb2; 16]),
    };
    for (label, request) in [
        (
            "SQLite path",
            admission(&other_path, schema_json.clone(), identity, claims.clone()),
        ),
        (
            "schema",
            admission(
                &primary,
                serde_json::to_string(other_schema.public_schema()).unwrap(),
                identity,
                claims.clone(),
            ),
        ),
        (
            "durable identity",
            admission(
                &primary,
                schema_json.clone(),
                other_identity,
                claims.clone(),
            ),
        ),
        (
            "validated claims",
            admission(
                &primary,
                schema_json.clone(),
                identity,
                BTreeMap::from([("role".to_owned(), Value::String("admin".to_owned()))]),
            ),
        ),
    ] {
        assert_eq!(
            host.admit_scope(request),
            Err(JazzNativeRelayStatus::LifecycleFailure),
            "a scope cannot mint a second capability with changed trusted {label}"
        );
        assert_eq!(
            host.admitted_scopes.len(),
            1,
            "failed {label} admission must not leave a usable capability"
        );
    }
}

/// Opening a foreground over a relay root written in an older storage
/// format fails with the dedicated `UnsupportedStorageCodecs` C ABI
/// status, which the platform wrappers surface as the JavaScript code
/// `unsupported_storage_codecs`, rather than the generic lifecycle
/// failure. The root is left byte-for-byte unchanged and the refusal is
/// stable across attempts.
///
/// Host-level rather than through JSI: the wrappers only translate this
/// status, and a Rust test cannot host a React Native runtime.
///
/// ```text
/// alice's pre-index-v2 relay root ──admit_scope──► capability
///   ──open_foreground──✗ JazzNativeRelayStatus::UnsupportedStorageCodecs (root unchanged)
/// ```
#[test]
fn foreground_open_over_an_old_storage_format_reports_unsupported_storage_codecs() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    legacy_storage::plant(&path, "pre_index_v2");
    let before = std::fs::read(&path).unwrap();

    let mut host = NativeRelayHost::default();
    let admitted = host
        .admit_scope(RelayScopeAdmissionRequest {
            scope: RelayScopeRequest {
                app_namespace: "old-store".to_owned(),
                storage_namespace: "primary".to_owned(),
                auth_scope: Some("alice".to_owned()),
            },
            sqlite_path: path.display().to_string(),
            schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0xa1; 16]),
                author: AuthorSubject::for_test_bytes([0xa2; 16]),
            },
            claims: BTreeMap::new(),
        })
        .expect("admission does not open storage");

    for attempt in ["first open", "second open"] {
        assert_eq!(
            host.open_foreground(admitted, DIRECT_FOREGROUND_RUNTIME_TOKEN),
            Err(JazzNativeRelayStatus::UnsupportedStorageCodecs),
            "{attempt}"
        );
    }
    assert!(host.foregrounds.is_empty());
    assert_eq!(std::fs::read(&path).unwrap(), before, "root is not mutated");
}

#[test]
fn relay_scope_requires_a_nonempty_authenticated_scope() {
    let directory = tempfile::tempdir().unwrap();
    let mut relay = config(directory.path().join("empty-auth.sqlite"), Some("   "));

    assert!(matches!(relay.validate(), Err(RelayError::InvalidScope(_))));

    relay.scope.auth_scope = None;
    assert!(matches!(relay.validate(), Err(RelayError::InvalidScope(_))));
}

/// Alice closes a foreground after issuing writes, then its replacement
/// fails while installing a malformed host catalogue capture. Retrying must
/// retain the old node's confirmed sequence and never repeat a transaction ID.
/// The private cache seam injects a real attachment error; opens, writes and
/// closes still cross the C ABI, and no failure-only runtime hook is added.
#[test]
fn failed_foreground_attachment_preserves_the_reused_node_sequence() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("failed-handoff.sqlite"),
        "failed-handoff",
        &permissive_schema(),
        0xd3,
    );
    let first = fixture.open_foreground(&capability);
    let keeper = fixture.open_foreground(&capability);
    let (first_node, client) = {
        let host = unsafe { &*fixture.host }.inner.lock().unwrap();
        (
            host.foregrounds[&first].lease.node,
            host.foreground_client(first).unwrap().clone(),
        )
    };
    let mut issued = Vec::new();
    for index in 0..8_u8 {
        issued.push(fixture.insert_todo(first, [index; 16], "before failed open"));
    }
    let last = *issued.last().unwrap();
    let mut wait = fixture.execute(
        first,
        ForegroundDbCommandRequest::WaitForTransaction {
            tx_id: last,
            tier: "local".into(),
            observe_only: false,
        },
    );
    for _ in 0..100 {
        let ForegroundDbCommandResponse::Pending { operation } = wait else {
            break;
        };
        fixture.tick(first);
        wait = fixture.execute(first, ForegroundDbCommandRequest::Poll { operation });
    }
    assert_eq!(
        wait,
        ForegroundDbCommandResponse::TransactionSettled { tx_id: last }
    );
    let floor = client.with_db(|db| Ok(db.seq_high_water())).unwrap();
    assert!(floor > 1);
    let relay = client.relay.clone();
    assert_eq!(
        fixture.execute(first, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    let before = {
        let host = unsafe { &*fixture.host }.inner.lock().unwrap();
        (
            host.foregrounds.len(),
            host.clients.len(),
            host.relays.len(),
        )
    };
    let capture = relay
        .run(|worker| {
            Ok(worker
                .catalogue_cache
                .as_ref()
                .unwrap()
                .borrow_mut()
                .capture
                .replace(b"invalid catalogue capture for failed-open regression".to_vec()))
        })
        .unwrap();
    for _ in 0..4 {
        assert_eq!(
            fixture.try_open_foreground(&capability),
            (JazzNativeRelayStatus::LifecycleFailure, 0),
            "the real catalogue decoder rejects attachment before exposing a handle"
        );
        let host = unsafe { &*fixture.host }.inner.lock().unwrap();
        assert_eq!(
            (
                host.foregrounds.len(),
                host.clients.len(),
                host.relays.len()
            ),
            before,
            "failed opens cannot leak a client or relay alias"
        );
    }
    relay
        .run(move |worker| {
            worker
                .catalogue_cache
                .as_ref()
                .unwrap()
                .borrow_mut()
                .capture = capture;
            Ok(())
        })
        .unwrap();
    fixture.tick(keeper);
    let next = fixture.open_foreground(&capability);
    let next_tx = fixture.insert_todo(next, [0x90; 16], "after failed open");
    assert!(
        !issued.contains(&next_tx),
        "a rejected attachment must not reset a reused node and repeat a transaction ID"
    );
    {
        let host = unsafe { &*fixture.host }.inner.lock().unwrap();
        let lease = host.foregrounds[&next].lease;
        assert_eq!(
            lease.node, first_node,
            "the cleanly returned node is reused"
        );
        assert_eq!(lease.high_water, floor);
    }
    assert_eq!(
        fixture.execute(next, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    assert_eq!(
        fixture.execute(keeper, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
}

#[test]
fn clean_foreground_handoff_does_not_repeat_synchronous_transaction_ids() {
    // The binding C ABI reserves transaction identities synchronously,
    // before taking the node lock. Exercise that actual path across the
    // host's clean node-lease handoff, rather than only reading its HLC.
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("handoff-reservation.sqlite"),
        "handoff-reservation",
        &permissive_schema(),
        0xc1,
    );
    let first = fixture.open_foreground(&capability);
    let first_tx = fixture.insert_todo(first, [0xd1; 16], "first lease holder");
    assert!(matches!(
        fixture.execute(first, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    ));
    let second = fixture.open_foreground(&capability);
    let second_tx = fixture.insert_todo(second, [0xd2; 16], "second lease holder");
    assert_ne!(
        first_tx, second_tx,
        "a cleanly reused foreground node must reserve a fresh public transaction identity"
    );
    assert!(matches!(
        fixture.execute(second, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    ));
}

#[test]
fn clean_foreground_handoff_reuses_one_node_only_after_advancing_its_hlc() {
    // This is intentionally an internal host-lifecycle receipt. The
    // user-visible write protocol is exercised by the Db integration
    // suites; here we need to observe the adapter-owned lease and the
    // runtime-owned HLC exactly at their handoff boundary.
    let directory = tempfile::tempdir().unwrap();
    let mut host = NativeRelayHost::default();
    let capability = host
        .admit_scope(RelayScopeAdmissionRequest {
            scope: RelayScopeRequest {
                app_namespace: "foreground-clean-handoff".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some("opaque-validated-subject".to_owned()),
            },
            sqlite_path: directory
                .path()
                .join("foreground.sqlite")
                .display()
                .to_string(),
            schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0xc1; 16]),
                author: AuthorSubject::for_test_bytes([0xc2; 16]),
            },
            claims: BTreeMap::new(),
        })
        .expect("trusted fixture admission succeeds");

    let first = host.open_foreground(capability, 1).unwrap();
    let first_lease = host.foregrounds[&first].lease;
    let first_client = host.foreground_client(first).unwrap().clone();
    let transaction = first_client
        .begin_foreground_transaction(ForegroundTransactionKind::Mergeable)
        .unwrap();
    first_client
        .insert_foreground_transaction(
            transaction,
            "todos".to_owned(),
            encoded_title_cells("first lease holder"),
            Some([0xc3; 16]),
        )
        .unwrap();
    let first_commit = first_client
        .commit_foreground_transaction(transaction)
        .unwrap();
    let first_high_water = first_client.minted_high_water().unwrap();
    assert!(first_high_water > 0);

    assert!(host.close_foreground(first).unwrap());
    let second = host.open_foreground(capability, 2).unwrap();
    let second_lease = host.foregrounds[&second].lease;
    assert_eq!(second_lease.node, first_lease.node);
    assert_eq!(second_lease.high_water, first_high_water);
    assert!(
        host.foreground_client(second)
            .unwrap()
            .minted_high_water()
            .unwrap()
            >= first_high_water,
        "the reused runtime starts at or above the previous sequence floor"
    );
    let next = host.foreground_client(second).unwrap();
    let (next_commit, _) = next
        .direct_foreground_mutation(
            ForegroundMutationKind::Insert,
            "todos".into(),
            Some([0xc4; 16]),
            encoded_title_cells("next lease holder"),
            "{}".into(),
        )
        .unwrap();
    assert_ne!(next_commit, first_commit);
    assert!(
        next.minted_high_water().unwrap() > first_high_water,
        "the replacement's actual write advances beyond every prior issued sequence"
    );
    assert!(host.close_foreground(second).unwrap());
}

#[test]
fn uncertain_foreground_readout_retires_instead_of_reissuing_its_node() {
    // Internal failure-path receipt: removing the owner-local client
    // models a runtime that disappeared before native close could read its
    // HLC. There is deliberately no best-effort reuse in that case.
    let directory = tempfile::tempdir().unwrap();
    let mut host = NativeRelayHost::default();
    let capability = host
        .admit_scope(RelayScopeAdmissionRequest {
            scope: RelayScopeRequest {
                app_namespace: "foreground-uncertain-handoff".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some("opaque-validated-subject".to_owned()),
            },
            sqlite_path: directory
                .path()
                .join("foreground.sqlite")
                .display()
                .to_string(),
            schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0xd1; 16]),
                author: AuthorSubject::for_test_bytes([0xd2; 16]),
            },
            claims: BTreeMap::new(),
        })
        .expect("trusted fixture admission succeeds");

    let first = host.open_foreground(capability, 1).unwrap();
    let first_lease = host.foregrounds[&first].lease;
    let first_client = host.foregrounds[&first].client;
    drop(host.clients.remove(&first_client));
    assert_eq!(
        host.close_foreground(first),
        Err(JazzNativeRelayStatus::LifecycleFailure)
    );

    let second = host.open_foreground(capability, 2).unwrap();
    assert_ne!(host.foregrounds[&second].lease.node, first_lease.node);
    assert!(host.close_foreground(second).unwrap());
}

/// Alice closes with a Remote read still awaiting an authority. SQLite's
/// synchronous sequence readout cancels that work and permits a clean lease
/// handoff strictly past every issued write. The host seam checks the exact
/// leased node and floor; public commands prove the replacement remains usable.
#[test]
fn pending_remote_read_does_not_obscure_clean_sequence_handoff() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("handoff.sqlite"),
        "pending-read-handoff",
        &permissive_schema(),
        0xc1,
    );
    let first = fixture.open_foreground(&capability);
    let keeper = fixture.open_foreground(&capability);
    let (old_node, client) = {
        let host = unsafe { &*fixture.host }.inner.lock().unwrap();
        (
            host.foregrounds[&first].lease.node,
            host.foreground_client(first).unwrap().clone(),
        )
    };
    let tx_id = fixture.insert_todo(first, [0xc3; 16], "admitted before handoff");
    let mut wait = fixture.execute(
        first,
        ForegroundDbCommandRequest::WaitForTransaction {
            tx_id,
            tier: "local".into(),
            observe_only: false,
        },
    );
    for _ in 0..100 {
        let ForegroundDbCommandResponse::Pending { operation } = wait else {
            break;
        };
        fixture.tick(first);
        wait = fixture.execute(first, ForegroundDbCommandRequest::Poll { operation });
    }
    assert_eq!(
        wait,
        ForegroundDbCommandResponse::TransactionSettled { tx_id }
    );
    let ForegroundDbCommandResponse::Pending { operation } = fixture.execute(
        first,
        ForegroundDbCommandRequest::All {
            query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
            options_json: r#"{"tier":"Remote"}"#.into(),
            transaction: None,
        },
    ) else {
        panic!("Remote read without an authority remains pending");
    };
    let floor = client.with_db(|db| Ok(db.seq_high_water())).unwrap();
    let closing = std::time::Instant::now();
    assert_eq!(
        fixture.execute(first, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    assert!(closing.elapsed() < Duration::from_secs(1));
    assert_eq!(
        fixture.tick_status(first),
        JazzNativeRelayStatus::InvalidHandle
    );
    assert!(client.poll_foreground_operation(operation).is_err());
    let next = fixture.open_foreground(&capability);
    {
        let host = unsafe { &*fixture.host }.inner.lock().unwrap();
        let lease = host.foregrounds[&next].lease;
        assert_eq!(lease.node, old_node);
        assert_eq!(lease.high_water, floor);
        assert!(
            host.foreground_client(next)
                .unwrap()
                .with_db(|db| Ok(db.seq_high_water()))
                .unwrap()
                >= floor
        );
    }
    assert_exact_todo_rows(
        &fixture.rows_after_sync(keeper),
        RowUuid::from_bytes([0xc3; 16]),
        "admitted before handoff",
    );
    let later = fixture.insert_todo(next, [0xc4; 16], "admitted after handoff");
    assert_ne!(later, tx_id);
    assert!(
        unsafe { &*fixture.host }
            .inner
            .lock()
            .unwrap()
            .foreground_client(next)
            .unwrap()
            .with_db(|db| Ok(db.seq_high_water()))
            .unwrap()
            > floor,
        "the replacement's actual write advances past every prior issued sequence"
    );
    let retired_id = client.id();
    assert!(
        client
            .relay
            .run(move |worker| Ok(!worker.clients.contains_key(&retired_id)))
            .unwrap()
    );
    for foreground in [next, keeper] {
        assert_eq!(
            fixture.execute(foreground, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
    }
}

#[test]
fn admitted_scope_capabilities_are_unguessable_and_revocation_closes_all_aliases() {
    let directory = tempfile::tempdir().unwrap();
    let host = jazz_native_relay_host_new();
    let admission = |name: &str, byte: u8| RelayScopeAdmissionRequest {
        scope: RelayScopeRequest {
            app_namespace: "capability-test".to_owned(),
            storage_namespace: "default".to_owned(),
            auth_scope: Some(name.to_owned()),
        },
        sqlite_path: directory
            .path()
            .join(format!("{name}.sqlite"))
            .display()
            .to_string(),
        schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([byte; 16]),
            author: AuthorSubject::for_test_bytes([byte.wrapping_add(1); 16]),
        },
        claims: BTreeMap::from([("sub".to_owned(), Value::String(name.to_owned()))]),
    };
    unsafe fn admit(
        host: *mut JazzNativeRelayHost,
        request: RelayScopeAdmissionRequest,
    ) -> AdmissionCapability {
        unsafe { (*host).inner.lock().unwrap().admit_scope(request) }
            .expect("test admission is valid")
    }
    let alice = unsafe { admit(host, admission("alice", 0xa1)) };
    let bob = unsafe { admit(host, admission("bob", 0xb1)) };
    assert_ne!(alice, bob);
    assert_ne!(alice.0, [0; 32]);
    assert_ne!(bob.0, [0; 32]);

    let open = |admitted_scope| RelayCommandRequest::Open {
        supported_abi_minimum: NATIVE_RELAY_ABI_V1,
        supported_abi_maximum: NATIVE_RELAY_ABI_V1,
        admitted_scope,
    };
    let execute = |request| unsafe { (*host).inner.lock().unwrap().execute(request) };
    let alice_relay = match execute(open(alice)).unwrap() {
        RelayCommandResponse::Opened { relay } => relay,
        response => panic!("unexpected open response: {response:?}"),
    };
    let alice_alias = match execute(open(alice)).unwrap() {
        RelayCommandResponse::Opened { relay } => relay,
        response => panic!("unexpected open response: {response:?}"),
    };
    let alice_client = match execute(RelayCommandRequest::Attach { relay: alice_relay }).unwrap() {
        RelayCommandResponse::Attached { client } => client,
        response => panic!("unexpected attach response: {response:?}"),
    };
    let bob_relay = match execute(open(bob)).unwrap() {
        RelayCommandResponse::Opened { relay } => relay,
        response => panic!("unexpected open response: {response:?}"),
    };

    let mut guessed = alice;
    guessed.0[0] ^= 0x80;
    assert_eq!(
        execute(open(guessed)),
        Err(JazzNativeRelayStatus::InvalidHandle)
    );

    assert!(unsafe { (*host).inner.lock().unwrap().revoke_scope(alice) }.unwrap());

    assert_eq!(
        execute(open(alice)),
        Err(JazzNativeRelayStatus::InvalidHandle)
    );
    assert_eq!(
        execute(RelayCommandRequest::Pump { relay: alice_relay }),
        Err(JazzNativeRelayStatus::InvalidHandle)
    );
    assert_eq!(
        execute(RelayCommandRequest::Pump { relay: alice_alias }),
        Err(JazzNativeRelayStatus::InvalidHandle)
    );
    assert_eq!(
        execute(RelayCommandRequest::CloseClient {
            client: alice_client
        })
        .unwrap(),
        RelayCommandResponse::Closed { closed: false }
    );
    assert!(matches!(
        execute(RelayCommandRequest::Pump { relay: bob_relay }),
        Ok(RelayCommandResponse::Pumped)
    ));
    unsafe { jazz_native_relay_host_free(host) };
}

#[test]
fn attached_foreground_c_abi_opens_ticks_and_closes_only_admitted_aliases() {
    let directory = tempfile::tempdir().unwrap();
    let host = jazz_native_relay_host_new();
    let admission = RelayScopeAdmissionRequest {
        scope: RelayScopeRequest {
            app_namespace: "foreground-c-abi".to_owned(),
            storage_namespace: "default".to_owned(),
            auth_scope: Some("opaque-validated-subject".to_owned()),
        },
        sqlite_path: directory
            .path()
            .join("foreground.sqlite")
            .display()
            .to_string(),
        schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x71; 16]),
            author: AuthorSubject::for_test_bytes([0x72; 16]),
        },
        claims: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
    };
    let capability = unsafe { (*host).inner.lock().unwrap().admit_scope(admission) }
        .expect("trusted fixture admission succeeds");

    let mut foreground = u64::MAX;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_open_attached_foreground(
                host,
                capability.0.as_ptr(),
                capability.0.len() - 1,
                &mut foreground,
            )
        },
        JazzNativeRelayStatus::InvalidArgument
    );
    assert_eq!(foreground, u64::MAX, "invalid calls do not write outputs");

    foreground = 0;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_open_attached_foreground(
                host,
                capability.0.as_ptr(),
                capability.0.len(),
                &mut foreground,
            )
        },
        JazzNativeRelayStatus::Ok
    );
    assert_ne!(foreground, 0, "a real foreground alias was opened");
    assert_eq!(
        unsafe { jazz_native_relay_host_tick_attached_foreground(host, foreground) },
        JazzNativeRelayStatus::Ok,
        "tick reaches the actual memory Db through its owner thread"
    );

    let mut closed = false;
    assert_eq!(
        unsafe { jazz_native_relay_host_close_attached_foreground(host, foreground, &mut closed) },
        JazzNativeRelayStatus::Ok
    );
    assert!(closed, "the first close owns the foreground transition");
    assert_eq!(
        unsafe { jazz_native_relay_host_tick_attached_foreground(host, foreground) },
        JazzNativeRelayStatus::InvalidHandle,
        "a closed JSI alias cannot continue to invoke the owner"
    );
    closed = true;
    assert_eq!(
        unsafe { jazz_native_relay_host_close_attached_foreground(host, foreground, &mut closed) },
        JazzNativeRelayStatus::Ok
    );
    assert!(
        !closed,
        "explicit close and finalization are safely idempotent"
    );

    assert_eq!(
        unsafe {
            jazz_native_relay_host_revoke_scope_capability(
                host,
                capability.0.as_ptr(),
                capability.0.len(),
            )
        },
        JazzNativeRelayStatus::Ok
    );
    let mut reopened = 0;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_open_attached_foreground(
                host,
                capability.0.as_ptr(),
                capability.0.len(),
                &mut reopened,
            )
        },
        JazzNativeRelayStatus::InvalidHandle,
        "revocation closes aliases and prevents all future opens"
    );
    assert_eq!(reopened, 0);
    unsafe { jazz_native_relay_host_free(host) };
}

#[test]
fn retained_runtime_leases_cannot_operate_each_others_foregrounds() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let schema = permissive_schema();
    let a_capability = fixture.admit(
        &directory.path().join("lease-a.sqlite"),
        "lease-a",
        &schema,
        0xa1,
    );
    let b_capability = fixture.admit(
        &directory.path().join("lease-b.sqlite"),
        "lease-b",
        &schema,
        0xb1,
    );
    let a = fixture.open_foreground(&a_capability);
    let b_runtime = unsafe { jazz_native_relay_host_retain(fixture.host, 2) };
    assert!(!b_runtime.is_null());
    let mut b = 0;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                b_runtime,
                b_capability.as_ptr(),
                b_capability.len(),
                &mut b,
            )
        },
        JazzNativeRelayStatus::Ok,
    );

    // All lease-scoped entrypoints reject B when addressed through A's
    // retained runtime token. In particular, a foreign close must not
    // consume B's handle or its node lease.
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(fixture.lease, b) },
        JazzNativeRelayStatus::InvalidHandle,
    );
    let request = postcard::to_allocvec(&ForegroundDbCommandRequest::Probe).unwrap();
    let mut output = JazzNativeRelayBytes::EMPTY;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_execute_foreground(
                fixture.lease,
                b,
                request.as_ptr(),
                request.len(),
                &mut output,
            )
        },
        JazzNativeRelayStatus::InvalidHandle,
    );
    assert!(output.data.is_null() && output.len == 0);
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_set_foreground_wake_callback(
                fixture.lease,
                b,
                None,
                std::ptr::null_mut(),
            )
        },
        JazzNativeRelayStatus::InvalidHandle,
    );
    let mut foreign_closed = true;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_close_attached_foreground(
                fixture.lease,
                b,
                &mut foreign_closed,
            )
        },
        JazzNativeRelayStatus::InvalidHandle,
    );
    assert!(
        !foreign_closed,
        "rejected close cannot report a foreign foreground as closed"
    );

    // Each runtime remains fully capable of operating its own foreground.
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(fixture.lease, a) },
        JazzNativeRelayStatus::Ok,
    );
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(b_runtime, b) },
        JazzNativeRelayStatus::Ok,
    );
    let mut b_closed = false;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_close_attached_foreground(b_runtime, b, &mut b_closed)
        },
        JazzNativeRelayStatus::Ok,
    );
    assert!(b_closed);
    unsafe { jazz_native_relay_host_lease_free(b_runtime) };
}

#[test]
fn retained_foreground_lease_outlives_the_platform_host_wrapper() {
    let directory = tempfile::tempdir().unwrap();
    let host = jazz_native_relay_host_new();
    let capability = unsafe {
        (*host)
            .inner
            .lock()
            .unwrap()
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "foreground-lease-receipt".to_owned(),
                    storage_namespace: "default".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: directory
                    .path()
                    .join("foreground.sqlite")
                    .display()
                    .to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0x81; 16]),
                    author: AuthorSubject::for_test_bytes([0x82; 16]),
                },
                claims: BTreeMap::new(),
            })
            .expect("trusted fixture admission succeeds")
    };
    let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
    assert!(!lease.is_null());
    let mut foreground = 0;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                lease,
                capability.0.as_ptr(),
                capability.0.len(),
                &mut foreground,
            )
        },
        JazzNativeRelayStatus::Ok
    );

    // This is the precise teardown race that the JSI lease closes: platform
    // ownership may disappear while a finalizer still owns its foreground
    // handle. The retained opaque lease keeps Rust state alive and usable.
    unsafe { jazz_native_relay_host_free(host) };
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(lease, foreground) },
        JazzNativeRelayStatus::Ok
    );
    let mut closed = false;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_close_attached_foreground(lease, foreground, &mut closed)
        },
        JazzNativeRelayStatus::Ok
    );
    assert!(closed);
    unsafe { jazz_native_relay_host_lease_free(lease) };
}

#[test]
fn invalidating_one_retained_runtime_retires_only_its_foregrounds() {
    let directory = tempfile::tempdir().unwrap();
    let host = jazz_native_relay_host_new();
    let capability = unsafe {
        (*host)
            .inner
            .lock()
            .unwrap()
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "independent-foreground-leases".to_owned(),
                    storage_namespace: "default".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: directory
                    .path()
                    .join("foreground.sqlite")
                    .display()
                    .to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0xc1; 16]),
                    author: AuthorSubject::for_test_bytes([0xc2; 16]),
                },
                claims: BTreeMap::new(),
            })
            .unwrap()
    };
    let first_lease = unsafe { jazz_native_relay_host_retain(host, 1) };
    let second_lease = unsafe { jazz_native_relay_host_retain(host, 2) };
    let open = |lease| {
        let mut foreground = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    lease,
                    capability.0.as_ptr(),
                    capability.0.len(),
                    &mut foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        foreground
    };
    let first = open(first_lease);
    let second = open(second_lease);
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_invalidate_foreground_runtime(first_lease) },
        JazzNativeRelayStatus::Ok
    );
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(first_lease, first) },
        JazzNativeRelayStatus::InvalidHandle,
        "runtime invalidation retires its foreground before a JS finalizer runs"
    );
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(second_lease, second) },
        JazzNativeRelayStatus::Ok,
        "runtime invalidation cannot revoke a sibling runtime sharing the relay"
    );
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_invalidate_foreground_runtime(second_lease) },
        JazzNativeRelayStatus::Ok
    );
    assert!(unsafe { (*host).inner.lock().unwrap().foregrounds.is_empty() });
    unsafe { jazz_native_relay_host_lease_free(first_lease) };
    unsafe { jazz_native_relay_host_lease_free(second_lease) };
    unsafe { jazz_native_relay_host_free(host) };
}

#[test]
fn attached_foregrounds_are_independent_and_closing_one_keeps_the_other_live() {
    let directory = tempfile::tempdir().unwrap();
    let host = jazz_native_relay_host_new();
    let capability = unsafe {
        (*host)
            .inner
            .lock()
            .unwrap()
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "two-foregrounds".to_owned(),
                    storage_namespace: "default".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: directory
                    .path()
                    .join("foreground.sqlite")
                    .display()
                    .to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0x91; 16]),
                    author: AuthorSubject::for_test_bytes([0x92; 16]),
                },
                claims: BTreeMap::new(),
            })
            .expect("trusted fixture admission succeeds")
    };

    let open = |host: *mut JazzNativeRelayHost| {
        let mut foreground = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_open_attached_foreground(
                    host,
                    capability.0.as_ptr(),
                    capability.0.len(),
                    &mut foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        foreground
    };
    let first = open(host);
    let second = open(host);
    assert_ne!(
        first, second,
        "each JS runtime receives its own opaque foreground handle"
    );
    assert_eq!(
        unsafe { jazz_native_relay_host_tick_attached_foreground(host, first) },
        JazzNativeRelayStatus::Ok
    );
    assert_eq!(
        unsafe { jazz_native_relay_host_tick_attached_foreground(host, second) },
        JazzNativeRelayStatus::Ok
    );

    let mut first_closed = false;
    assert_eq!(
        unsafe { jazz_native_relay_host_close_attached_foreground(host, first, &mut first_closed) },
        JazzNativeRelayStatus::Ok
    );
    assert!(first_closed);
    assert_eq!(
        unsafe { jazz_native_relay_host_tick_attached_foreground(host, first) },
        JazzNativeRelayStatus::InvalidHandle,
        "closing one JS runtime cannot leave its foreground alias usable"
    );
    assert_eq!(
        unsafe { jazz_native_relay_host_tick_attached_foreground(host, second) },
        JazzNativeRelayStatus::Ok,
        "closing one JS runtime cannot tear down its sibling foreground"
    );

    assert_eq!(
        unsafe {
            jazz_native_relay_host_revoke_scope_capability(
                host,
                capability.0.as_ptr(),
                capability.0.len(),
            )
        },
        JazzNativeRelayStatus::Ok
    );
    assert_eq!(
        unsafe { jazz_native_relay_host_tick_attached_foreground(host, second) },
        JazzNativeRelayStatus::InvalidHandle,
        "revocation invalidates every foreground belonging to the capability"
    );
    unsafe { jazz_native_relay_host_free(host) };
}

#[test]
fn foreground_command_c_abi_uses_one_binary_runtime_vocabulary() {
    let directory = tempfile::tempdir().unwrap();
    // Internal ABI test: public row APIs cannot expose native metadata
    // framing or prove the admitted account survives the capability boundary.
    let author = AuthorSubject::for_test_bytes([0xa2; 16]).with_account(
        jazz::account_registry::AccountId(NodeUuid::from_bytes([7; 16]).0),
    );
    let host = jazz_native_relay_host_new();
    let capability = unsafe {
        (*host)
            .inner
            .lock()
            .unwrap()
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "foreground-command-abi".to_owned(),
                    storage_namespace: "default".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: directory
                    .path()
                    .join("foreground.sqlite")
                    .display()
                    .to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0xa1; 16]),
                    author,
                },
                claims: BTreeMap::new(),
            })
            .expect("trusted fixture admission succeeds")
    };
    let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
    let mut foreground = 0;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                lease,
                capability.0.as_ptr(),
                capability.0.len(),
                &mut foreground,
            )
        },
        JazzNativeRelayStatus::Ok
    );

    let execute = |command| {
        let request = postcard::to_allocvec(&command).unwrap();
        let mut response = JazzNativeRelayBytes::EMPTY;
        let status = unsafe {
            jazz_native_relay_host_lease_execute_foreground(
                lease,
                foreground,
                request.as_ptr(),
                request.len(),
                &mut response,
            )
        };
        let bytes = unsafe { std::slice::from_raw_parts(response.data, response.len) }.to_vec();
        unsafe { jazz_native_relay_bytes_free(&mut response) };
        (status, bytes)
    };
    let (status, response) = execute(ForegroundDbCommandRequest::NativeSessionMetadata);
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    let (issuer, user_id) = author.principal_parts();
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
        ForegroundDbCommandResponse::NativeSessionMetadata {
            node: unsafe {
                (*host).inner.lock().unwrap().foregrounds[&foreground]
                    .lease
                    .node
                    .0
                    .into_bytes()
            },
            registry_authority: "foreground-command-abi".into(),
            account_id: Some([7; 16]),
            issuer,
            user_id,
        }
    );
    let (status, response) = execute(ForegroundDbCommandRequest::Probe);
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
        ForegroundDbCommandResponse::Probe {
            abi_version: NATIVE_RELAY_ABI_V1
        }
    );
    let (status, response) = execute(ForegroundDbCommandRequest::Tick);
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
        ForegroundDbCommandResponse::Ticked
    );
    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
    let (status, response) = execute(ForegroundDbCommandRequest::All {
        query: query.clone(),
        options_json: "{}".into(),
        transaction: None,
    });
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    let mut response = postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap();
    let mut turns = 0;
    let rows = loop {
        match response {
            ForegroundDbCommandResponse::Rows { rows } => break rows,
            ForegroundDbCommandResponse::Pending { operation } if turns < 100 => {
                let (status, tick) = execute(ForegroundDbCommandRequest::Tick);
                assert_eq!(status, JazzNativeRelayStatus::Ok);
                assert_eq!(
                    postcard::from_bytes::<ForegroundDbCommandResponse>(&tick).unwrap(),
                    ForegroundDbCommandResponse::Ticked
                );
                let (status, poll) = execute(ForegroundDbCommandRequest::Poll { operation });
                assert_eq!(status, JazzNativeRelayStatus::Ok);
                response = postcard::from_bytes::<ForegroundDbCommandResponse>(&poll).unwrap();
                turns += 1;
            }
            ForegroundDbCommandResponse::Pending { .. } => {
                panic!("all did not finish after {turns} owner turns")
            }
            other => panic!("all returned an unexpected response: {other:?}"),
        }
    };
    assert!(rows.len() <= 2, "empty foreground read must stay bounded");
    let (status, response) = execute(ForegroundDbCommandRequest::Subscribe {
        query,
        options_json: "{}".into(),
    });
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    let ForegroundDbCommandResponse::Subscribed { subscription } =
        postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap()
    else {
        panic!("subscribe must return an opaque subscription handle");
    };
    let (status, response) =
        execute(ForegroundDbCommandRequest::DrainSubscription { subscription });
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    assert!(matches!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
        ForegroundDbCommandResponse::SubscriptionEvents { .. }
    ));
    let (status, response) = execute(ForegroundDbCommandRequest::Unsubscribe { subscription });
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
        ForegroundDbCommandResponse::Unsubscribed { closed: true }
    );
    let (status, response) = execute(ForegroundDbCommandRequest::Cancel { operation: 999 });
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
        ForegroundDbCommandResponse::Cancelled { cancelled: false },
        "an unknown operation cannot cancel a different foreground's work"
    );
    let (status, response) = execute(ForegroundDbCommandRequest::Close);
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    let (status, response) = execute(ForegroundDbCommandRequest::Close);
    assert_eq!(status, JazzNativeRelayStatus::Ok);
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
        ForegroundDbCommandResponse::Closed { closed: false },
        "the byte ABI retains the same idempotent close rule as the JSI convenience method"
    );

    let request = postcard::to_allocvec(&ForegroundDbCommandRequest::Probe).unwrap();
    let mut stale_response = JazzNativeRelayBytes {
        data: usize::MAX as *mut u8,
        len: usize::MAX,
    };
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_execute_foreground(
                lease,
                foreground,
                request.as_ptr(),
                request.len(),
                &mut stale_response,
            )
        },
        JazzNativeRelayStatus::InvalidHandle
    );
    assert!(stale_response.data.is_null() && stale_response.len == 0);
    unsafe { jazz_native_relay_host_lease_free(lease) };
    unsafe { jazz_native_relay_host_free(host) };
}

#[test]
fn foreground_transaction_commands_delegate_to_core_and_retire_with_their_owner() {
    // This is intentionally an internal C-ABI receipt: it proves the
    // byte command family reaches the ordinary foreground Db and its core
    // transaction handles, while public row/query semantics stay covered
    // by the Db integration suites.
    let directory = tempfile::tempdir().unwrap();
    let host = jazz_native_relay_host_new();
    let capability = unsafe {
        (*host)
            .inner
            .lock()
            .unwrap()
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "foreground-transaction-abi".to_owned(),
                    storage_namespace: "default".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: directory
                    .path()
                    .join("foreground.sqlite")
                    .display()
                    .to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0xb1; 16]),
                    author: AuthorSubject::for_test_bytes([0xb2; 16]),
                },
                claims: BTreeMap::new(),
            })
            .expect("trusted fixture admission succeeds")
    };
    let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
    let mut foreground = 0;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                lease,
                capability.0.as_ptr(),
                capability.0.len(),
                &mut foreground,
            )
        },
        JazzNativeRelayStatus::Ok
    );
    let execute = |foreground, command| {
        let request = postcard::to_allocvec(&command).unwrap();
        let mut response = JazzNativeRelayBytes::EMPTY;
        let status = unsafe {
            jazz_native_relay_host_lease_execute_foreground(
                lease,
                foreground,
                request.as_ptr(),
                request.len(),
                &mut response,
            )
        };
        let bytes = if response.data.is_null() {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(response.data, response.len) }.to_vec()
        };
        unsafe { jazz_native_relay_bytes_free(&mut response) };
        (status, bytes)
    };
    let response = |foreground, command| {
        let (status, bytes) = execute(foreground, command);
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        postcard::from_bytes::<ForegroundDbCommandResponse>(&bytes).unwrap()
    };

    let ForegroundDbCommandResponse::TransactionOpened { transaction } = response(
        foreground,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        },
    ) else {
        panic!("begin must return an opaque transaction handle");
    };
    let row_id = [0x71; 16];
    assert_eq!(
        response(
            foreground,
            ForegroundDbCommandRequest::Insert {
                transaction,
                table: "todos".to_owned(),
                cells: encoded_title_cells("mergeable"),
                row_id: Some(row_id),
            },
        ),
        ForegroundDbCommandResponse::Inserted { row_id }
    );
    assert_eq!(
        response(
            foreground,
            ForegroundDbCommandRequest::Update {
                transaction,
                table: "todos".to_owned(),
                row_id,
                patch: encoded_title_cells("updated"),
            },
        ),
        ForegroundDbCommandResponse::MutationStaged
    );
    assert_eq!(
        response(
            foreground,
            ForegroundDbCommandRequest::Upsert {
                transaction,
                table: "todos".to_owned(),
                row_id,
                cells: encoded_title_cells("upserted"),
            },
        ),
        ForegroundDbCommandResponse::MutationStaged
    );
    assert_eq!(
        response(
            foreground,
            ForegroundDbCommandRequest::Delete {
                transaction,
                table: "todos".to_owned(),
                row_id,
            },
        ),
        ForegroundDbCommandResponse::MutationStaged
    );
    let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = response(
        foreground,
        ForegroundDbCommandRequest::CommitTransaction { transaction },
    ) else {
        panic!("commit must return the public committed txId");
    };
    assert_ne!(tx_id, [0; 16]);
    assert!(matches!(
        response(
            foreground,
            ForegroundDbCommandRequest::RollbackTransaction { transaction }
        ),
        ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: false }
    ));

    let ForegroundDbCommandResponse::TransactionOpened { transaction } = response(
        foreground,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        },
    ) else {
        panic!("serializable begin must return a handle");
    };
    assert_eq!(
        response(
            foreground,
            ForegroundDbCommandRequest::Insert {
                transaction,
                table: "todos".to_owned(),
                cells: encoded_title_cells("rolled back"),
                row_id: Some([0x72; 16]),
            },
        ),
        ForegroundDbCommandResponse::Inserted { row_id: [0x72; 16] }
    );
    assert_eq!(
        response(
            foreground,
            ForegroundDbCommandRequest::RollbackTransaction { transaction },
        ),
        ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: true }
    );

    let mut second_foreground = 0;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                lease,
                capability.0.as_ptr(),
                capability.0.len(),
                &mut second_foreground,
            )
        },
        JazzNativeRelayStatus::Ok
    );
    assert!(matches!(
        response(
            second_foreground,
            ForegroundDbCommandRequest::CommitTransaction { transaction }
        ),
        ForegroundDbCommandResponse::OperationError { .. }
    ));

    // Invalid schema/cell input is a logical operation error rather than
    // a lifecycle failure, preserving the core error boundary for the
    // shared adapter. Being a core error, it carries its stable code.
    let ForegroundDbCommandResponse::TransactionOpened { transaction } = response(
        foreground,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        },
    ) else {
        panic!("begin must return a handle");
    };
    let rejected_insert = response(
        foreground,
        ForegroundDbCommandRequest::Insert {
            transaction,
            table: "missing_table".to_owned(),
            cells: encoded_title_cells("nope"),
            row_id: Some([0x73; 16]),
        },
    );
    assert_eq!(
        rejected_insert,
        ForegroundDbCommandResponse::CodedOperationError {
            reason_name: None,
            code: "query".to_owned(),
            reason: "Query: unknown table missing_table".to_owned(),
        }
    );
    assert_eq!(
        response(
            foreground,
            ForegroundDbCommandRequest::RollbackTransaction { transaction },
        ),
        ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: true }
    );

    // Closing a foreground retires all of its mutable core transactions.
    // A fresh alias is allowed later, but cannot retain or complete the
    // closed alias's opaque handle.
    let ForegroundDbCommandResponse::TransactionOpened { transaction } = response(
        second_foreground,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        },
    ) else {
        panic!("second foreground begin must return a handle");
    };
    assert_eq!(
        response(second_foreground, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    let (status, stale_response) = execute(
        second_foreground,
        ForegroundDbCommandRequest::CommitTransaction { transaction },
    );
    assert_eq!(status, JazzNativeRelayStatus::InvalidHandle);
    assert!(stale_response.is_empty());

    unsafe { jazz_native_relay_host_lease_free(lease) };
    unsafe { jazz_native_relay_host_free(host) };
}

#[test]
fn foreground_reads_reject_unknown_tiers() {
    // Internal C-ABI receipt, like the transaction-command test above:
    // React Native reaches read options only through this byte command
    // family, so unknown tiers must fail at deserialization rather than
    // being silently treated as another tier. Migration guidance belongs
    // to the TypeScript public boundary.
    let directory = tempfile::tempdir().unwrap();
    let host = jazz_native_relay_host_new();
    let capability = unsafe {
        (*host)
            .inner
            .lock()
            .unwrap()
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "foreground-unknown-tier".to_owned(),
                    storage_namespace: "default".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: directory
                    .path()
                    .join("foreground.sqlite")
                    .display()
                    .to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0xb3; 16]),
                    author: AuthorSubject::for_test_bytes([0xb4; 16]),
                },
                claims: BTreeMap::new(),
            })
            .expect("trusted fixture admission succeeds")
    };
    let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
    let mut foreground = 0;
    assert_eq!(
        unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                lease,
                capability.0.as_ptr(),
                capability.0.len(),
                &mut foreground,
            )
        },
        JazzNativeRelayStatus::Ok
    );
    let response = |command| {
        let request = postcard::to_allocvec(&command).unwrap();
        let mut response = JazzNativeRelayBytes::EMPTY;
        let status = unsafe {
            jazz_native_relay_host_lease_execute_foreground(
                lease,
                foreground,
                request.as_ptr(),
                request.len(),
                &mut response,
            )
        };
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        let bytes = unsafe { std::slice::from_raw_parts(response.data, response.len) }.to_vec();
        unsafe { jazz_native_relay_bytes_free(&mut response) };
        postcard::from_bytes::<ForegroundDbCommandResponse>(&bytes).unwrap()
    };
    let query = postcard::to_allocvec(&Query::from("todos")).unwrap();

    for tier in [
        "invalid-tier",
        "InvalidTier",
        "remote-if-possible",
        "RemoteIfPossible",
        "local-first-unless-empty",
        "LocalFirstUnlessEmpty",
        "core",
        "Core",
    ] {
        let removed = format!("unknown read tier {tier}");
        let options_json = format!(r#"{{"tier":"{tier}"}}"#);
        assert_eq!(
            response(ForegroundDbCommandRequest::All {
                query: query.clone(),
                options_json: options_json.clone(),
                transaction: None,
            }),
            ForegroundDbCommandResponse::OperationError {
                reason: removed.to_owned()
            },
            "one-shot read with tier {tier}"
        );
        assert_eq!(
            response(ForegroundDbCommandRequest::Subscribe {
                query: query.clone(),
                options_json,
            }),
            ForegroundDbCommandResponse::OperationError {
                reason: removed.to_owned()
            },
            "subscription with tier {tier}"
        );
    }
    // The same command with a supported tier is admitted: it reads or
    // starts a pending read instead of failing its options.
    let local = response(ForegroundDbCommandRequest::All {
        query,
        options_json: r#"{"tier":"local-first"}"#.to_owned(),
        transaction: None,
    });
    assert!(
        matches!(
            local,
            ForegroundDbCommandResponse::Rows { .. } | ForegroundDbCommandResponse::Pending { .. }
        ),
        "{local:?}"
    );

    unsafe { jazz_native_relay_host_lease_free(lease) };
    unsafe { jazz_native_relay_host_free(host) };
}

#[test]
fn foreground_transaction_postcard_layout_matches_the_handwritten_ts_codec() {
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Mergeable,
        })
        .unwrap(),
        vec![9, 0]
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Serializable,
        })
        .unwrap(),
        vec![9, 1]
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::Insert {
            transaction: 3,
            table: "todos".to_owned(),
            cells: vec![1, 2],
            row_id: None,
        })
        .unwrap(),
        vec![10, 3, 5, b't', b'o', b'd', b'o', b's', 2, 1, 2, 0]
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::Update {
            transaction: 3,
            table: "todos".to_owned(),
            row_id: [7; 16],
            patch: vec![9],
        })
        .unwrap(),
        [
            vec![11, 3, 5, b't', b'o', b'd', b'o', b's'],
            vec![7; 16],
            vec![1, 9]
        ]
        .concat()
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::TransactionCommitted {
            tx_id: [4; 16],
        })
        .unwrap(),
        [vec![13], vec![4; 16]].concat()
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::TransactionRolledBack {
            rolled_back: true,
        })
        .unwrap(),
        vec![14, 1]
    );
    // Authoritative settlement has one pinned byte spelling for
    // JNI/Swift/JSI wrappers.
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::WaitForCoreTransaction {
            tx_id: [5; 16],
        })
        .unwrap(),
        [vec![16], vec![5; 16]].concat()
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::TransactionSettled { tx_id: [6; 16] })
            .unwrap(),
        [vec![15], vec![6; 16]].concat()
    );
}

// Public row results cannot detect a changed native event discriminant.
#[test]
fn foreground_structured_delta_v1_byte_contract() {
    let event = ForegroundSubscriptionEvent::StructuredDelta {
        reset: true,
        settled: false,
        tier: "local".into(),
        delta: vec![9],
        terminal_operations_json: "[]".into(),
    };
    let bytes = [
        vec![3, 1, 0, 5],
        b"local".to_vec(),
        vec![1, 9, 2],
        b"[]".to_vec(),
    ]
    .concat();
    assert_eq!(postcard::to_allocvec(&event).unwrap(), bytes);
    assert_eq!(
        postcard::from_bytes::<ForegroundSubscriptionEvent>(&bytes).unwrap(),
        event
    );
}

// Internal byte assertions are necessary: public row results cannot reveal
// a changed enum ordinal or option encoding that breaks installed hosts.
#[test]
fn foreground_extension_v1_byte_contract() {
    let cases = [
        (
            ForegroundDbCommandRequest::All {
                query: vec![1, 128, 2],
                options_json: "{}".into(),
                transaction: None,
            },
            vec![2, 3, 1, 128, 2, 2, 123, 125, 0],
        ),
        (
            ForegroundDbCommandRequest::All {
                query: vec![1],
                options_json: "{}".into(),
                transaction: Some(256),
            },
            vec![2, 1, 1, 2, 123, 125, 1, 128, 2],
        ),
        (
            ForegroundDbCommandRequest::Subscribe {
                query: vec![1],
                options_json: "{}".into(),
            },
            vec![3, 1, 1, 2, 123, 125],
        ),
        (
            ForegroundDbCommandRequest::WaitForTransaction {
                tx_id: [7; 16],
                tier: "core".into(),
                observe_only: false,
            },
            [vec![17], vec![7; 16], vec![4, 99, 111, 114, 101, 0]].concat(),
        ),
        (
            ForegroundDbCommandRequest::WaitForTransaction {
                tx_id: [7; 16],
                tier: "local".into(),
                observe_only: true,
            },
            [vec![17], vec![7; 16], vec![5, 108, 111, 99, 97, 108, 1]].concat(),
        ),
        (
            ForegroundDbCommandRequest::StageMutation {
                transaction: 1,
                mutation: ForegroundMutationKind::Restore,
                table: "t".into(),
                row_id: None,
                cells: vec![],
                options_json: "{}".into(),
            },
            vec![18, 1, 4, 1, 116, 0, 0, 2, 123, 125],
        ),
        (
            ForegroundDbCommandRequest::DisconnectNativeUpstream,
            vec![19],
        ),
        (
            ForegroundDbCommandRequest::ReconnectNativeUpstream,
            vec![20],
        ),
        (ForegroundDbCommandRequest::NativeConnectionStatus, vec![21]),
    ];
    for (command, bytes) in cases {
        assert_eq!(postcard::to_allocvec(&command).unwrap(), bytes);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandRequest>(&bytes).unwrap(),
            command
        );
    }
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::NativeConnectionStatus {
            configured: true,
            explicitly_offline: false,
            connected: true
        })
        .unwrap(),
        vec![16, 1, 0, 1]
    );
    for (ordinal, kind) in [
        ForegroundMutationKind::Insert,
        ForegroundMutationKind::Update,
        ForegroundMutationKind::Upsert,
        ForegroundMutationKind::Delete,
        ForegroundMutationKind::Restore,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(postcard::to_allocvec(&kind).unwrap(), vec![ordinal as u8]);
    }
}

#[test]
fn foreground_coded_operation_error_v1_byte_contract() {
    // Response 25 is appended to V1: a core error's stable code, then the
    // unchanged reason. Uncoded failures keep response 8.
    let coded = ForegroundDbCommandResponse::CodedOperationError {
        reason_name: None,
        code: "not_observed".into(),
        reason: "NotObserved: oops".into(),
    };
    let bytes = [
        vec![25, 12],
        b"not_observed".to_vec(),
        vec![17],
        b"NotObserved: oops".to_vec(),
    ]
    .concat();
    assert_eq!(postcard::to_allocvec(&coded).unwrap(), bytes);
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&bytes).unwrap(),
        coded
    );
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandResponse::OperationError {
            reason: "oops".into()
        })
        .unwrap(),
        vec![8, 4, 111, 111, 112, 115]
    );
}

#[test]
fn foreground_continuation_v1_byte_contract() {
    // Internal byte fixtures pin host/OTA compatibility that row-level
    // database assertions cannot observe.
    let cases = [
        (ForegroundDbCommandRequest::NativeSessionMetadata, vec![22]),
        (
            ForegroundDbCommandRequest::WaitForPendingWrites {
                tier: "core".into(),
            },
            vec![33, 4, 99, 111, 114, 101],
        ),
        (
            ForegroundDbCommandRequest::WriteState { tx_id: [7; 16] },
            [vec![23], vec![7; 16]].concat(),
        ),
        (ForegroundDbCommandRequest::DrainMutationErrors, vec![24]),
        (
            ForegroundDbCommandRequest::BeginStreamingMutation {
                mutation: ForegroundMutationKind::Update,
                table: "t".into(),
                row_id: [7; 16],
                cells: vec![9],
                column: "c".into(),
                options_json: "{}".into(),
            },
            [
                vec![25, 1, 1, 116],
                vec![7; 16],
                vec![1, 9, 1, 99, 2, 123, 125],
            ]
            .concat(),
        ),
        (
            ForegroundDbCommandRequest::PushStreamingMutation {
                upload: 128,
                chunk: vec![9],
            },
            vec![26, 128, 1, 1, 9],
        ),
        (
            ForegroundDbCommandRequest::FinishStreamingMutation { upload: 128 },
            vec![27, 128, 1],
        ),
        (
            ForegroundDbCommandRequest::AbortStreamingMutation { upload: 128 },
            vec![28, 128, 1],
        ),
        (
            ForegroundDbCommandRequest::LocalCurrentRow {
                table: "t".into(),
                row_id: [7; 16],
            },
            [vec![29, 1, 116], vec![7; 16]].concat(),
        ),
        (
            ForegroundDbCommandRequest::UpdateLargeValues {
                table: "t".into(),
                row_id: [7; 16],
                patch: vec![9],
                descriptors_json: "[]".into(),
                updated_at_ms: Some(128),
            },
            [
                vec![30, 1, 116],
                vec![7; 16],
                vec![1, 9, 2, 91, 93, 1, 128, 1],
            ]
            .concat(),
        ),
        (
            ForegroundDbCommandRequest::DirectMutation {
                mutation: ForegroundMutationKind::Insert,
                table: "t".into(),
                row_id: Some([7; 16]),
                cells: vec![9],
                options_json: "{}".into(),
            },
            [vec![31, 0, 1, 116, 1], vec![7; 16], vec![1, 9, 2, 123, 125]].concat(),
        ),
    ];
    for (command, bytes) in cases {
        assert_eq!(postcard::to_allocvec(&command).unwrap(), bytes);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandRequest>(&bytes).unwrap(),
            command
        );
    }
    let responses = [
        (
            ForegroundDbCommandResponse::NativeSessionMetadata {
                node: [9; 16],
                registry_authority: "r".into(),
                account_id: None,
                issuer: "i".into(),
                user_id: "u".into(),
            },
            [vec![17], vec![9; 16], vec![1, 114, 0, 1, 105, 1, 117]].concat(),
        ),
        (
            ForegroundDbCommandResponse::NativeSessionMetadata {
                node: [9; 16],
                registry_authority: "r".into(),
                account_id: Some([7; 16]),
                issuer: "i".into(),
                user_id: "u".into(),
            },
            [
                vec![17],
                vec![9; 16],
                vec![1, 114, 1],
                vec![7; 16],
                vec![1, 105, 1, 117],
            ]
            .concat(),
        ),
        (
            ForegroundDbCommandResponse::WriteState {
                state_json: "{}".into(),
            },
            vec![18, 2, 123, 125],
        ),
        (
            ForegroundDbCommandResponse::MutationErrors {
                events_json: "[]".into(),
            },
            vec![19, 2, 91, 93],
        ),
        (
            ForegroundDbCommandResponse::StreamingMutationOpened { upload: 128 },
            vec![20, 128, 1],
        ),
        (
            ForegroundDbCommandResponse::StreamingMutationPushed,
            vec![21],
        ),
        (
            ForegroundDbCommandResponse::StreamingMutationAborted { aborted: true },
            vec![22, 1],
        ),
        (
            ForegroundDbCommandResponse::MutationCommitted {
                tx_id: [7; 16],
                row_id: [8; 16],
            },
            [vec![23], vec![7; 16], vec![8; 16]].concat(),
        ),
    ];
    for (response, bytes) in responses {
        assert_eq!(postcard::to_allocvec(&response).unwrap(), bytes);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&bytes).unwrap(),
            response
        );
    }
}

/// Alice's staged uploads use command tags appended after main's snapshot
/// command. This boundary fixture pins the Rust bytes independently of JS.
#[test]
fn foreground_staged_upload_v1_byte_contract() {
    for (request, bytes) in [
        (
            ForegroundDbCommandRequest::StageStreamingMutation { upload: 128 },
            vec![35, 128, 1],
        ),
        (
            ForegroundDbCommandRequest::AttachStagedStreamingMutation {
                staged: 128,
                transaction: 300,
            },
            vec![36, 128, 1, 172, 2],
        ),
        (
            ForegroundDbCommandRequest::AbortStagedStreamingMutation { staged: 128 },
            vec![37, 128, 1],
        ),
    ] {
        assert_eq!(postcard::to_allocvec(&request).unwrap(), bytes);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandRequest>(&bytes).unwrap(),
            request
        );
    }
    for (response, bytes) in [
        (
            ForegroundDbCommandResponse::StreamingMutationStaged { staged: 128 },
            vec![26, 128, 1],
        ),
        (
            ForegroundDbCommandResponse::StagedStreamingMutationAttached,
            vec![27],
        ),
        (
            ForegroundDbCommandResponse::StagedStreamingMutationAborted { aborted: true },
            vec![28, 1],
        ),
    ] {
        assert_eq!(postcard::to_allocvec(&response).unwrap(), bytes);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&bytes).unwrap(),
            response
        );
    }
}

#[test]
fn c_host_serializes_concurrent_commands() {
    let host = jazz_native_relay_host_new() as usize;
    let probe = postcard::to_allocvec(&RelayCommandRequest::Probe).unwrap();
    let workers = (0..8)
        .map(|_| {
            let probe = probe.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    let mut output = JazzNativeRelayBytes::EMPTY;
                    assert_eq!(
                        unsafe {
                            jazz_native_relay_host_execute(
                                host as *mut JazzNativeRelayHost,
                                probe.as_ptr(),
                                probe.len(),
                                &mut output,
                            )
                        },
                        JazzNativeRelayStatus::Ok
                    );
                    unsafe { jazz_native_relay_bytes_free(&mut output) };
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    unsafe { jazz_native_relay_host_free(host as *mut JazzNativeRelayHost) };
}

#[test]
fn terminal_owner_aliases_cannot_reopen_or_run_after_panic() {
    let directory = tempfile::tempdir().unwrap();
    let registry = NativeRelayRegistry::default();
    let relay_config = config(directory.path().join("terminal.sqlite"), Some("alice"));
    let stale = registry.open(relay_config.clone()).unwrap();
    assert!(matches!(
        stale.run::<()>(|_| panic!("planted owner failure")),
        Err(RelayError::Closed)
    ));
    assert!(matches!(stale.pump(), Err(RelayError::Closed)));
    let replacement = registry.open(relay_config).unwrap();
    assert!(!Arc::ptr_eq(&stale.inner, &replacement.inner));
    assert!(replacement.pump().is_ok());
}

#[test]
fn closing_a_scope_terminally_retires_held_aliases() {
    let directory = tempfile::tempdir().unwrap();
    let registry = NativeRelayRegistry::default();
    let relay_config = config(directory.path().join("close.sqlite"), Some("alice"));
    let stale = registry.open(relay_config.clone()).unwrap();
    assert!(registry.close(&relay_config.scope).unwrap());
    assert!(matches!(stale.pump(), Err(RelayError::Closed)));
    assert!(registry.open(relay_config).unwrap().pump().is_ok());
}
#[test]
fn native_admitted_scope_owner_encoding_and_generic_reuse_are_separate() {
    let directory = tempfile::tempdir().unwrap();
    let config = config(directory.path().join("scope-mode.sqlite"), Some("alice"));
    assert_eq!(
        config.scope.ledger_owner_encoding(),
        "jazz-native-relay-owner-v1:17:native-relay-test7:default5:alice"
    );
    let registry = NativeRelayRegistry::default();
    let raw = registry.open(config.clone()).unwrap();
    assert!(matches!(
        registry.open_admitted(config.clone()),
        Err(RelayError::ScopeConfigurationMismatch)
    ));
    registry.close(&config.scope).unwrap();
    drop(raw);
    let admitted = registry.open_admitted(config.clone()).unwrap();
    assert!(matches!(
        registry.open(config.clone()),
        Err(RelayError::ScopeConfigurationMismatch)
    ));
    let mut wrong = config.clone();
    wrong.identity.author = AuthorSubject::for_test_bytes([0x99; 16]);
    assert!(matches!(
        registry.open_admitted(wrong),
        Err(RelayError::ScopeConfigurationMismatch)
    ));
    registry.close(&config.scope).unwrap();
    drop(admitted);
}

/// Alice's socket holds a drained batch while her owner produces more frames.
/// The in-flight batch retains its budget until it is sent or restored, so
/// backpressure cannot overfill the native queue or reorder later production.
#[test]
fn socket_batch_reserves_space_while_the_owner_produces() {
    let wire = NativeRelayWire::default();
    let expected = (0..NATIVE_RELAY_QUEUE_MAX_MESSAGES)
        .map(|id| SyncMessage::Unsubscribe { id: id as u64 })
        .collect::<Vec<_>>();
    for message in &expected {
        wire.outbound
            .lock()
            .unwrap()
            .push(message.clone(), "test backlog")
            .unwrap();
    }
    let batch = wire.take_socket_batch().unwrap();
    assert_eq!(batch.len(), NATIVE_RELAY_DRAIN_MAX_MESSAGES);
    assert_eq!(wire.outbound.lock().unwrap().room().0, 0);
    let id = DbIdentity {
        node: NodeUuid::from_bytes([0xa7; 16]),
        author: AuthorSubject::for_test_bytes([0xa8; 16]),
    };
    let producer = Binding::new(
        block_on(Db::open(DbConfig::new(schema(), id))).unwrap(),
        id,
        false,
    );
    let link = producer.connect_upstream();
    block_on(producer.tick()).unwrap();
    let produced = link.outbound_len();
    assert!(produced > 0, "the real binding has frames ready to publish");
    wire.collect_outbound(&link);
    assert_eq!(
        link.outbound_len(),
        produced,
        "held socket frames consume the producer's budget"
    );
    wire.finish_socket_batch(batch);
    let mut sent = wire.take_outbound_encoded().unwrap();
    wire.collect_outbound(&link);
    assert_eq!(link.outbound_len(), 0);
    while wire.queue_depths().1 != 0 {
        sent.extend(wire.take_outbound_encoded().unwrap());
    }
    assert_eq!(sent.len(), expected.len() + produced);
    assert_eq!(
        &sent[..expected.len()],
        expected
            .iter()
            .map(|message| encode_sync_message(message).unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(wire.outbound.lock().unwrap().bytes, 0);
    assert_eq!(wire.outbound.lock().unwrap().in_flight_bytes, 0);
}

/// Alice's local-first read waits for Bob's unavailable authority until its
/// requested deadline. The native owner wakes for the core's actual deadline
/// and delivers the fallback without platform polling or an unrelated write.
#[test]
fn native_read_deadline_wakes_an_idle_owner() {
    let (_directory, relay, client, wake) = attached_write_client("read-deadline", 0x97);
    relay.begin_socket_upstream().unwrap();
    settle_owner(&relay);
    wake.queued.lock().unwrap().clear();
    let started = Instant::now();
    let response = client
        .execute(ForegroundDbCommandRequest::All {
            query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
            options_json: "{\"tier\":\"local-first\",\"first_load_remote_wait_ms\":40}".into(),
            transaction: None,
        })
        .unwrap();
    let ForegroundDbCommandResponse::Pending { operation } = response else {
        panic!("the read must first wait for its owner: {response:?}");
    };
    assert!(
        wake.wait_for_queued(1),
        "the deadline wakes a host with no incoming frames"
    );
    let response = client
        .execute(ForegroundDbCommandRequest::Poll { operation })
        .unwrap();
    let ForegroundDbCommandResponse::Rows { rows } = response else {
        panic!("deadline must release the local fallback: {response:?}");
    };
    assert!(
        // The core carries the remaining wait in whole milliseconds after
        // the owner preparation step, rounding down by less than one ms.
        started.elapsed() >= Duration::from_millis(39),
        "configured live authority gets the requested wait: {:?}",
        started.elapsed()
    );
    assert!(
        postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&rows)
            .unwrap()
            .iter()
            .all(|batch| batch.rows.is_empty())
    );
    client.close().unwrap();
}

/// Bob's authority answered Alice's advice request, but Alice has not polled
/// its completed native response. A claim refresh retires that old answer as
/// Unknown; the buffered result cannot authorise under a different snapshot.
#[test]
fn claim_refresh_retires_completed_unobserved_advice() {
    let (_directory, relay, client, authority) =
        attached_transaction_authority("completed-advice", 0x98);
    let response = client
        .request_foreground_permission_advice(ForegroundPermissionAdviceAction::Read {
            table: "todos".into(),
            row: [0x99; 16],
        })
        .unwrap();
    let ForegroundOperationPoll::Pending { operation } = response else {
        panic!("authority advice starts pending");
    };
    for _ in 0..16 {
        block_on(authority.tick()).unwrap();
        relay.pump().unwrap();
    }
    let id = client.id;
    relay
        .run(move |worker| {
            assert!(matches!(
                worker.client(id)?.completed.get(&operation),
                Some(ForegroundDbCommandResponse::PermissionAdvice {
                    advice: ForegroundPermissionAdvice::Denied
                })
            ));
            worker.refresh_claims(
                AuthorSubject::for_test_bytes([0x98; 16]),
                BTreeMap::from([("role".into(), Value::String("changed".into()))]),
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(
        client
            .execute(ForegroundDbCommandRequest::Poll { operation })
            .unwrap(),
        ForegroundDbCommandResponse::PermissionAdvice {
            advice: ForegroundPermissionAdvice::Unknown
        }
    );
    client.close().unwrap();
}
