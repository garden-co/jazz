//! A WebSocket connection's handshake after its prelude was read and its
//! credential admitted: the peer's `Hello`, the wire features both sides
//! speak, the link capability the server grants, and the server's
//! `Hello`. Each host runs the same steps around its own I/O: the native
//! server on its sockets, a Durable Object on its own.
//!
//! ```text
//! first frame ──negotiate_hello──► Hello{features}  (host: is the runtime ready?)
//!             ──link(admission)──► Link{thin, link_admission, server_hello}
//! ```

use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::wire::{
    WireAuthorityEndpoint, WireError, WireErrorCode, WireFeatures, WireFrame, WireHello,
    WirePeerRole, WireRetry, current_wire_features, encode_frame, negotiate_wire,
};

use crate::portable::admission::{ServerLinkAdmission, WebSocketAdmission, ws_link_admission};
use crate::portable::websocket_codec::decode_ws_hello;

/// Authority endpoints and features negotiated for one connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSessionContext {
    /// This server's endpoint for the connection.
    pub local: WireAuthorityEndpoint,
    /// The peer's endpoint, when it advertised one.
    pub remote: Option<WireAuthorityEndpoint>,
    /// The admitted identity of the link.
    pub link_identity: AuthorSubject,
    /// The negotiated wire features.
    pub negotiated_features: WireFeatures,
}

/// The peer's `Hello` and what both sides speak.
#[derive(Clone, Debug)]
#[must_use]
pub struct NegotiatedHello {
    /// The peer's `Hello`.
    pub remote: WireHello,
    /// The protocol version both speak.
    pub protocol_version: u16,
    /// The wire features both speak.
    pub features: WireFeatures,
}

/// Decode the peer's opening `Hello` (`first`, its first frame batch) and
/// negotiate the wire with it. Thin clients (SPEC 8.14) are served on the
/// same route: a client that advertises thin results is answered with
/// results, not sync. A one-shot catalogue bootstrap is refused.
pub fn negotiate_hello(
    first: &[u8],
    bootstrap_catalogue: bool,
) -> Result<NegotiatedHello, WireError> {
    let remote = decode_ws_hello(first)?;
    let negotiated = negotiate_wire(
        &remote,
        current_wire_features() | jazz::wire::FEATURE_THIN_RESULTS | jazz::wire::FEATURE_HEARTBEAT,
    )?;
    // A downstream browser may be authority-unbound while still accepting the
    // server's authenticated authority in its response Hello.  The server
    // never installs scoped authority semantics without an admitted remote
    // endpoint (below), so this directional capability advertisement does not
    // turn a client self-assertion into authority proof.
    if bootstrap_catalogue {
        return Err(WireError::new(
            WireErrorCode::UnsupportedFeature,
            WireRetry::Never,
            "edge catalogue bootstrap is no longer supported; open an ordinary client session",
        ));
    }
    Ok(NegotiatedHello {
        remote,
        protocol_version: negotiated.protocol_version,
        features: negotiated.features,
    })
}

/// What an admitted connection is linked as.
#[derive(Clone, Debug)]
#[must_use]
pub struct Link {
    /// The connection is a thin client's: one session reading finished
    /// results.
    pub thin: bool,
    /// The capability the server granted the link.
    pub link_admission: ServerLinkAdmission,
    /// For a sync link that negotiated scope views: the endpoints and
    /// features its session is served under.
    pub session_context: Option<ConnectionSessionContext>,
    /// The server's `Hello`, encoded: sent once the session is open.
    pub server_hello: Vec<u8>,
}

/// Link an admitted connection: a fresh server endpoint, the session
/// context, the link capability, and the server's `Hello`.
pub fn link(admission: &WebSocketAdmission, hello: &NegotiatedHello) -> Result<Link, WireError> {
    // Every admitted server link receives a fresh server endpoint. A browser
    // client need not (and must not) self-assert one merely to learn which
    // authority issued its downstream fates.
    let server_endpoint = WireAuthorityEndpoint::fresh(NodeUuid::from_bytes([0x5e; 16]));
    let session_context = if hello.features & jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS != 0 {
        // An authenticated client can request current rows without itself
        // being an authority. Retain our receipt epoch and its admitted identity
        // independently of whether it advertises a remote authority endpoint.
        Some(ConnectionSessionContext {
            local: server_endpoint,
            remote: hello.remote.authority,
            link_identity: admission.identity,
            negotiated_features: hello.features,
        })
    } else {
        None
    };
    let link_admission = ws_link_admission(admission, hello.features, server_endpoint.epoch)?;
    let thin = hello.features & jazz::wire::FEATURE_THIN_RESULTS != 0;
    // A thin connection is one session reading finished results: it holds
    // no replica to relay for tabs, so it is never a client relay. An admin
    // link is served as SYSTEM either way. `session_context` is for sync
    // links alone.
    if thin
        && matches!(
            link_admission,
            ServerLinkAdmission::ScopeIsolatedClientRelay { .. }
        )
    {
        return Err(WireError::new(
            WireErrorCode::UnsupportedFeature,
            WireRetry::Never,
            "a thin connection serves one session: it cannot relay for other tabs",
        ));
    }
    let server_hello = WireFrame::Hello(
        WireHello::current(WirePeerRole::Core, hello.features)
            .with_authority(server_endpoint.node, server_endpoint.epoch),
    );
    let server_hello = encode_frame(&server_hello).map_err(|error| {
        WireError::new(
            WireErrorCode::Internal,
            WireRetry::Never,
            format!("failed to encode websocket server hello: {error}"),
        )
    })?;
    Ok(Link {
        thin,
        link_admission,
        session_context,
        server_hello,
    })
}

// Lower-level tests: the native route's tests exercise these through its
// sockets; a Durable Object host reuses them directly.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::portable::admission::{
        CommitUnitTrust, RequestedWebSocketLink, WebSocketCredential,
    };
    use std::collections::BTreeMap;

    fn hello(features: WireFeatures) -> Vec<u8> {
        let frame = encode_frame(&WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            features,
        )))
        .unwrap();
        crate::portable::websocket_codec::encode_ws_frame_batches(&[frame])
            .unwrap()
            .pop()
            .unwrap()
    }

    fn admission(trust: CommitUnitTrust, link: RequestedWebSocketLink) -> WebSocketAdmission {
        WebSocketAdmission {
            identity: AuthorSubject::authenticated("https://idp.example", "alice").unwrap(),
            claims: BTreeMap::new(),
            trust,
            credential: WebSocketCredential::Backend,
            requested_link: link,
            bundle: None,
            revocation: None,
        }
    }

    /// A thin client's `Hello` negotiates thin results; the server's `Hello`
    /// comes back encoded for it.
    #[test]
    fn a_thin_hello_links_a_thin_connection() {
        let negotiated = negotiate_hello(
            &hello(current_wire_features() | jazz::wire::FEATURE_THIN_RESULTS),
            false,
        )
        .unwrap();
        assert_ne!(negotiated.features & jazz::wire::FEATURE_THIN_RESULTS, 0);
        let link = link(
            &admission(
                CommitUnitTrust::Session,
                RequestedWebSocketLink::OrdinarySession,
            ),
            &negotiated,
        )
        .unwrap();
        assert!(link.thin);
        assert_eq!(link.link_admission, ServerLinkAdmission::OrdinarySession);
        match jazz::wire::decode_frame(&link.server_hello).unwrap() {
            WireFrame::Hello(hello) => assert_eq!(hello.role, WirePeerRole::Core),
            other => panic!("expected the server's Hello, got {other:?}"),
        }
    }

    /// A one-shot catalogue bootstrap is refused, whatever the Hello.
    #[test]
    fn a_catalogue_bootstrap_is_refused() {
        let refused = negotiate_hello(&hello(current_wire_features()), true).unwrap_err();
        assert_eq!(refused.code, WireErrorCode::UnsupportedFeature);
    }

    /// A thin connection serves one session: one that asked to relay for
    /// other tabs is refused.
    #[test]
    fn a_thin_connection_cannot_relay_for_other_tabs() {
        let negotiated = negotiate_hello(
            &hello(
                current_wire_features()
                    | jazz::wire::FEATURE_THIN_RESULTS
                    | jazz::wire::FEATURE_SCOPE_ISOLATED_CLIENT_RELAY,
            ),
            false,
        )
        .unwrap();
        let refused = link(
            &admission(
                CommitUnitTrust::Session,
                RequestedWebSocketLink::ScopeIsolatedClientRelay,
            ),
            &negotiated,
        )
        .unwrap_err();
        assert!(
            refused.message.contains("cannot relay for other tabs"),
            "{refused:?}"
        );
    }
}
