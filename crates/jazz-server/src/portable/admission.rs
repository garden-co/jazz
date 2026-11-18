//! What a WebSocket connection is admitted as: the prelude a peer opens
//! with, the credential class and link it is admitted with, and the
//! server-owned link capability selected after wire negotiation. How the
//! credential is verified stays with each host.

use std::collections::BTreeMap;

use jazz::ids::AuthorSubject;
use jazz::records::Value as CoreValue;
use jazz::wire::{WireError, WireErrorCode, WireRetry};

/// How much the server trusts a link's commits, chosen at admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitUnitTrust {
    /// An end-user session: every read and write is checked against the
    /// table policies for its identity.
    Session,
    /// An authenticated backend. It is served with full access.
    TrustedBackend,
    /// An authenticated administrative or authority link, served with full
    /// access.
    TrustedAuthority,
}

/// Capability selected by the server after authenticating a WebSocket and
/// negotiating its features.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ServerLinkAdmission {
    /// A normal session or backend link.
    #[default]
    OrdinarySession,
    /// Host-verified SYSTEM authority credential permitting scoped query
    /// delegation. On SQLite it is served as a trusted SYSTEM link; the sync
    /// protocol cannot express a delegated scope yet.
    AuthorityQueryDelegate,
    /// A durable relay for exactly the one session authenticated by this
    /// connection. On SQLite it is served as that session.
    ScopeIsolatedClientRelay {
        /// Fresh server-issued capability epoch for this accepted connection.
        admission_epoch: u64,
    },
}

/// The JSON a peer sends first on a new socket, before any wire frame:
/// who it says it is and the credentials it presents.
#[derive(serde::Deserialize)]
#[non_exhaustive]
pub struct WebSocketPrelude {
    pub peer_identity: String,
    pub auth: jazz::tools::websocket_prelude_auth::AuthConfig,
    /// A one-shot authenticated authority-catalogue transfer. This is not a
    /// subscriber session and never admits application frames.
    #[serde(default)]
    pub bootstrap_catalogue: bool,
    /// Requested before any wire frame.  It is only a request: after JWT/cookie
    /// authentication and feature negotiation the server either creates its
    /// own immutable admitted relay capability or rejects the connection.
    #[serde(default)]
    pub requested_link: RequestedWebSocketLink,
    /// The audience a trusted backend verified the client's local-first
    /// proof against; honoured only with that backend's secret (see
    /// [`crate::middleware::auth::DEVICE_ACCOUNT_AUDIENCE_HEADER`]).
    #[serde(default)]
    pub local_first_audience: Option<String>,
    /// The published permissions bundle (hex id) the backend that opened
    /// this socket serves with; honoured only on that backend's word (its
    /// credential on the upgrade, or its secret here), as a client could
    /// otherwise pick an older bundle's laxer policies.
    #[serde(default)]
    pub bundle: Option<String>,
}

/// The only client-selectable *request* at the WebSocket boundary.  This is
/// intentionally separate from `WirePeerRole`: a wire hello says what a peer
/// implements, not what authority it receives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedWebSocketLink {
    #[default]
    OrdinarySession,
    ScopeIsolatedClientRelay,
}

/// Authentication class selected by the prelude, independently of the
/// permission subject carried by each forwarded transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WebSocketCredential {
    Admin,
    Backend,
    Session,
}

/// What a socket was admitted as, once its credential was verified.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WebSocketAdmission {
    pub identity: AuthorSubject,
    pub claims: BTreeMap<String, CoreValue>,
    pub trust: CommitUnitTrust,
    pub credential: WebSocketCredential,
    pub requested_link: RequestedWebSocketLink,
    /// The published permissions bundle the backend pinned this session
    /// to (see [`jazz::catalogue`]).
    pub bundle: Option<jazz::catalogue::BundleId>,
    /// A user session the app's backend stated: `jazz.revokeUser` can end
    /// it from any backend instance (see [`SessionRevocation`]).
    pub revocation: Option<SessionRevocation>,
}

/// What a backend-stated user session is checked against: the user's
/// session revocation mark in the account registry. The session ends once
/// the mark is past the time the backend's `authenticate` started for it
/// (`authenticatedAt` in the stated session, on the backend's clock), so a
/// revocation from one backend instance reaches sessions another holds, and
/// refuses those it admits later on an answer from before the revocation.
#[derive(Clone, Debug)]
pub struct SessionRevocation {
    pub principal: jazz::account_registry::Principal,
    /// Unix milliseconds. A backend that doesn't state it (older than this
    /// check) gets the time the core admitted the session.
    pub authenticated_at_ms: u64,
}

/// Select the server-owned link capability after credential admission and wire
/// feature negotiation. This stays separate from the client prelude because a
/// request alone cannot grant scoped relay authority.
pub fn ws_link_admission(
    admission: &WebSocketAdmission,
    negotiated_features: u64,
    admission_epoch: u64,
) -> Result<ServerLinkAdmission, WireError> {
    match admission.requested_link {
        RequestedWebSocketLink::OrdinarySession
            if admission.credential == WebSocketCredential::Admin
                && admission.identity == AuthorSubject::SYSTEM
                && admission.trust == CommitUnitTrust::TrustedAuthority =>
        {
            Ok(ServerLinkAdmission::AuthorityQueryDelegate)
        }
        RequestedWebSocketLink::OrdinarySession => Ok(ServerLinkAdmission::OrdinarySession),
        RequestedWebSocketLink::ScopeIsolatedClientRelay
            if admission.trust == CommitUnitTrust::Session
                && negotiated_features & jazz::wire::FEATURE_SCOPE_ISOLATED_CLIENT_RELAY != 0 =>
        {
            // This epoch was minted by the server for this accepted socket.
            // Reconnects necessarily get a fresh capability.
            Ok(ServerLinkAdmission::ScopeIsolatedClientRelay { admission_epoch })
        }
        RequestedWebSocketLink::ScopeIsolatedClientRelay => Err(WireError::new(
            WireErrorCode::UnsupportedFeature,
            WireRetry::Never,
            "scope-isolated client relay requires an authenticated session and negotiated relay feature",
        )),
    }
}
