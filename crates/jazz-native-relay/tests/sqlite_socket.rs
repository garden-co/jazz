//! Socket admission outcomes through the public native-worker composition seam.
use jazz::db::{DbIdentity, TransportError};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::tools::native_transport_connector::{
    ConnectedNativeTransport, NativeTransportConnector, NativeTransportError,
    NativeTransportFuture, NativeTransportRequest,
};
use jazz::tools::websocket_prelude_auth::AuthConfig;
use jazz::tools::{AppId, ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::wire::WireTransport;
use jazz_native_relay::{
    NATIVE_RELAY_ABI_V1, NativeRelay, NativeRelayAbiRange, NativeRelaySocketConfig,
    NativeRelaySocketEvent, NativeRelaySocketWorker, RelayOpenConfig, RelayScope,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

struct IdleWire;
impl WireTransport for IdleWire {
    fn send_frame(&mut self, _: Vec<u8>) -> Result<(), TransportError> {
        Ok(())
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        None
    }
}
struct Connector {
    first: NativeTransportError,
    attempts: AtomicUsize,
}
impl NativeTransportConnector for Connector {
    fn connect(&self, _: NativeTransportRequest) -> NativeTransportFuture {
        let first = (self.attempts.fetch_add(1, Ordering::AcqRel) == 0).then(|| self.first.clone());
        Box::pin(async move {
            if let Some(error) = first {
                return Err(error);
            }
            Ok(ConnectedNativeTransport {
                transport: Box::new(IdleWire),
                protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                features: jazz::wire::current_wire_features(),
                terminal: Box::pin(std::future::pending()),
            })
        })
    }
}
fn exercise(terminal: bool) {
    let root = tempfile::tempdir().unwrap();
    let alice = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
    let relay = NativeRelay::spawn(RelayOpenConfig {
        supported_abi: NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1,
            maximum: NATIVE_RELAY_ABI_V1,
        },
        scope: RelayScope {
            app_namespace: "socket-admission".into(),
            storage_namespace: "default".into(),
            auth_scope: Some("alice".into()),
        },
        sqlite_path: root.path().join("owner.sqlite"),
        schema: JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("notes").column("text", ColumnType::Text))
                .build(),
        )
        .unwrap(),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([17; 16]),
            author: alice,
        },
    })
    .unwrap();
    let connector = Arc::new(Connector {
        first: if terminal {
            NativeTransportError::Terminal("handshake rejected".into())
        } else {
            NativeTransportError::Retryable("authority unavailable".into())
        },
        attempts: AtomicUsize::new(0),
    });
    let (events, observed) = mpsc::channel();
    let worker = NativeRelaySocketWorker::start_with_connector(
        relay.clone(),
        NativeRelaySocketConfig {
            server_url: "https://edge.example".into(),
            app_id: AppId::from_name("socket-admission"),
            peer_identity: alice,
            auth: AuthConfig {
                jwt_token: Some("test-admitted-bearer".into()),
                ..Default::default()
            },
            reconnect_delay: Duration::from_millis(1),
            on_event: Arc::new(move |event| {
                let _ = events.send(event);
            }),
        },
        connector.clone(),
    )
    .unwrap();
    let next = || observed.recv_timeout(Duration::from_secs(2)).unwrap();
    if terminal {
        assert!(
            matches!(next(), NativeRelaySocketEvent::TerminalError(error)
            if error.contains("handshake rejected"))
        );
        assert_eq!(next(), NativeRelaySocketEvent::Stopped);
        assert_eq!(connector.attempts.load(Ordering::Acquire), 1);
    } else {
        assert_eq!(next(), NativeRelaySocketEvent::Reconnecting);
        assert_eq!(next(), NativeRelaySocketEvent::Connected);
        worker.cancel();
        assert_eq!(next(), NativeRelaySocketEvent::Stopped);
        assert_eq!(connector.attempts.load(Ordering::Acquire), 2);
    }
    // A failed remote admission never destroys Alice's local storage owner.
    relay.pump().unwrap();
}

/// Bob's authority refuses Alice's handshake permanently. The worker reports
/// the failure and stops after one attempt; Alice's local owner remains usable.
#[test]
fn terminal_handshake_rejection_stops_the_native_worker() {
    exercise(true);
}

/// Bob's authority is temporarily unavailable to Alice. The worker retries and
/// connects normally; cancelling that replacement then stops it exactly once.
#[test]
fn retryable_handshake_failure_keeps_the_native_reconnect_path() {
    exercise(false);
}
