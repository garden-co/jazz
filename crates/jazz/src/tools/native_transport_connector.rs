//! Native transport adapter boundary.
//!
//! A socket implementation belongs to a target shell, not to the semantic
//! crate. This contract uses the core's already-buffered
//! [`WireTransport`](crate::wire::WireTransport): an adapter owns DNS, TLS,
//! WebSocket framing and its async pump, while `jazz` owns wire negotiation
//! and peer state. Do not add an adapter dependency back to `jazz` merely to
//! construct a client or an edge upstream connection.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::db::ConnectionSessionContext;
use crate::ids::AuthorSubject;
use crate::protocol::CatalogueSnapshot;
use crate::tools::AppId;
use crate::tools::websocket_prelude_auth::AuthConfig;
use crate::wire::WireTransport;

/// Inputs shared by the public client and an edge server when opening a native
/// peer link. The wake callback is only for newly staged inbound work: waking
/// for outbound sends creates an empty-tick feedback loop in the synchronous
/// database owner.
#[derive(Clone)]
pub struct NativeTransportRequest {
    pub server_url: String,
    pub app_id: AppId,
    pub peer_identity: AuthorSubject,
    pub auth: AuthConfig,
    pub wake: Arc<dyn Fn() + Send + Sync>,
}

impl std::fmt::Debug for NativeTransportRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeTransportRequest")
            .field("server_url", &self.server_url)
            .field("app_id", &self.app_id)
            .field("peer_identity", &self.peer_identity)
            // Authentication can contain JWTs, backend/admin credentials, and
            // an impersonated session. Requests are routinely included in
            // connection diagnostics, so no auth detail is safe to expose.
            .field("auth", &"<redacted>")
            .field("wake", &"inbound-only callback")
            .finish()
    }
}

/// A negotiated byte transport returned by a target-specific connector.
///
/// `session_context` is authenticated during the adapter handshake. Keeping it
/// with the transport prevents an adapter from asserting identity in semantic
/// messages or requiring server-private admission APIs.
pub struct ConnectedNativeTransport {
    pub transport: Box<dyn WireTransport + Send>,
    pub protocol_version: u16,
    pub features: u64,
    pub session_context: Option<ConnectionSessionContext>,
}

/// Future returned by a target-specific native connector.
pub type NativeTransportFuture = Pin<
    Box<
        dyn Future<Output = Result<ConnectedNativeTransport, NativeTransportError>>
            + Send
            + 'static,
    >,
>;
pub type NativeCatalogueBootstrapFuture =
    Pin<Box<dyn Future<Output = Result<CatalogueSnapshot, NativeTransportError>> + Send + 'static>>;

/// Error at the target/socket boundary, before a core peer has been attached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeTransportError(pub String);

impl std::fmt::Display for NativeTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NativeTransportError {}

/// Factory implemented by `jazz-native-transport` (or another native shell).
///
/// The factory is passed at consumer composition points rather than registered
/// globally. CLI/server/NAPI therefore choose an adapter explicitly, and tests
/// can use an in-memory transport without compiling Tokio or TLS into core.
pub trait NativeTransportConnector: Send + Sync {
    /// Validate an edge bootstrap URL using adapter-specific transport rules.
    fn validate_catalogue_bootstrap_url(
        &self,
        _server_url: &str,
        _app_id: AppId,
    ) -> Result<(), NativeTransportError> {
        Ok(())
    }

    fn connect(&self, request: NativeTransportRequest) -> NativeTransportFuture;

    /// Fetch the authenticated, snapshot-only catalogue exchange used before
    /// an edge attaches its ordinary upstream peer.
    fn bootstrap_catalogue(
        &self,
        request: NativeTransportRequest,
    ) -> NativeCatalogueBootstrapFuture;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::TransportError;
    use std::sync::Mutex;

    struct NoopTransport;

    impl WireTransport for NoopTransport {
        fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
            Ok(())
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            None
        }
    }

    struct RecordingConnector(Mutex<Option<NativeTransportRequest>>);

    impl NativeTransportConnector for RecordingConnector {
        fn connect(&self, request: NativeTransportRequest) -> NativeTransportFuture {
            *self.0.lock().expect("record request") = Some(request);
            Box::pin(async {
                Ok(ConnectedNativeTransport {
                    transport: Box::new(NoopTransport),
                    protocol_version: 1,
                    features: 0,
                    session_context: None,
                })
            })
        }

        fn bootstrap_catalogue(
            &self,
            _request: NativeTransportRequest,
        ) -> NativeCatalogueBootstrapFuture {
            Box::pin(async {
                Err(NativeTransportError(
                    "not used in this contract test".to_owned(),
                ))
            })
        }
    }

    #[test]
    // This internal test is necessary because the contract's observable effect
    // is handing composition inputs to an adapter; opening a real socket would
    // test Tokio/TLS rather than this featureless-core boundary.
    fn connector_receives_composition_inputs_without_a_socket_dependency() {
        let connector = RecordingConnector(Mutex::new(None));
        let app_id = AppId::random();
        let peer_identity = AuthorSubject::SYSTEM;
        let connected = futures::executor::block_on(connector.connect(NativeTransportRequest {
            server_url: "https://example.invalid".to_owned(),
            app_id,
            peer_identity,
            auth: AuthConfig::default(),
            wake: Arc::new(|| {}),
        }))
        .expect("connector result");

        assert_eq!(connected.protocol_version, 1);
        let request = connector
            .0
            .lock()
            .expect("recorded request")
            .take()
            .unwrap();
        assert_eq!(request.server_url, "https://example.invalid");
        assert_eq!(request.app_id, app_id);
        assert_eq!(request.peer_identity, peer_identity);
    }

    #[test]
    // This internal test is necessary because redaction is a diagnostic
    // boundary on this core-owned request type, before any public client can
    // observe the socket adapter.
    fn request_debug_redacts_all_authentication_material() {
        let marker = "native-connector-credential-marker-9af1";
        let request = NativeTransportRequest {
            server_url: "https://example.invalid".to_owned(),
            app_id: AppId::random(),
            peer_identity: AuthorSubject::SYSTEM,
            auth: AuthConfig {
                jwt_token: Some(marker.to_owned()),
                backend_secret: Some(marker.to_owned()),
                admin_secret: Some(marker.to_owned()),
                backend_session: Some(serde_json::json!({ "credential": marker })),
            },
            wake: Arc::new(|| {}),
        };

        let rendered = format!("{request:?}");
        assert!(rendered.contains("auth: \"<redacted>\""));
        assert!(!rendered.contains(marker));
    }
}
