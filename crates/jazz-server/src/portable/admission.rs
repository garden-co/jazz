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

impl CommitUnitTrust {
    /// The identity a link admitted as `identity` is served as: its own for
    /// a user session, under its policies; SYSTEM for a trusted backend or
    /// authority link, served with full access. Every host maps trust
    /// through this one place.
    pub fn served_as(self, identity: AuthorSubject) -> AuthorSubject {
        match self {
            Self::Session => identity,
            Self::TrustedBackend | Self::TrustedAuthority => AuthorSubject::SYSTEM,
        }
    }
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
    /// [`crate::middleware::auth::LOCAL_FIRST_AUDIENCE_HEADER`]).
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

/// Compare two secrets in time independent of where they differ.
fn same_secret(expected: &str, given: &str) -> bool {
    let (a, b) = (expected.as_bytes(), given.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// How a socket's upgrade request vouched for it: the core secret it
/// carried (`X-Jazz-Backend-Credential`) and the access it asked for
/// (`X-Jazz-Backend-Access`: `admin` or `backend`).
pub struct UpgradeCredential<'a> {
    pub credential: &'a str,
    pub access: &'a str,
}

/// Admit a socket to a core that admits only its app's backend, by the
/// core's one `secret` (a Durable Object core): as a native core does with
/// a backend credential and no trusted workload.
///
/// - The upgrade's credential, or the prelude's `admin_secret` /
///   `backend_secret`, must be `secret`; an admin one is an authority link.
/// - A backend's own link is served as the backend; a user session the
///   backend states (`backend_session`) is served under that user's
///   policies, and must be the peer its prelude named.
/// - A local-first guest's own proof (`jwt_token`) is not verified here
///   yet: the backend relays a guest as a stated session.
/// - Such a core keeps no account registry yet, so `jazz.revokeUser` does
///   not end the sessions it serves (a native core watches revocations).
pub fn admit_by_core_secret(
    prelude: &WebSocketPrelude,
    upgrade: Option<UpgradeCredential<'_>>,
    secret: &str,
) -> Result<WebSocketAdmission, String> {
    let peer_identity = crate::portable::session::ws_peer_identity(&prelude.peer_identity)?;
    let bundle = prelude
        .bundle
        .as_deref()
        .map(|text| {
            jazz::catalogue::BundleId::from_hex(text)
                .ok_or_else(|| format!("invalid permissions bundle id: {text}"))
        })
        .transpose()?;
    let auth = &prelude.auth;
    let (admin, backend) = match &upgrade {
        Some(upgrade) if same_secret(secret, upgrade.credential) => match upgrade.access {
            "admin" => (true, true),
            "backend" => (false, true),
            other => return Err(format!("unknown backend access `{other}`")),
        },
        Some(_) => return Err("Invalid backend secret".to_owned()),
        None => (
            auth.admin_secret
                .as_deref()
                .is_some_and(|given| same_secret(secret, given)),
            auth.backend_secret
                .as_deref()
                .is_some_and(|given| same_secret(secret, given)),
        ),
    };
    if admin {
        return Ok(WebSocketAdmission {
            identity: peer_identity,
            claims: BTreeMap::new(),
            trust: CommitUnitTrust::TrustedAuthority,
            credential: WebSocketCredential::Admin,
            requested_link: RequestedWebSocketLink::OrdinarySession,
            bundle,
            revocation: None,
        });
    }
    if !backend {
        return Err(match (&auth.backend_secret, &auth.admin_secret) {
            (Some(_), _) | (_, Some(_)) => "Invalid backend secret",
            _ => {
                "this core admits only its app's backend: connect through the app's createJazz handler"
            }
        }
        .to_owned());
    }
    if auth.jwt_token.is_some() {
        return Err(
            "a local-first proof is not verified by a Durable Object core yet: relay the guest as a backend-stated session"
                .to_owned(),
        );
    }
    let Some(stated) = &auth.backend_session else {
        return Ok(WebSocketAdmission {
            identity: peer_identity,
            claims: BTreeMap::new(),
            trust: CommitUnitTrust::TrustedBackend,
            credential: WebSocketCredential::Backend,
            requested_link: RequestedWebSocketLink::OrdinarySession,
            bundle,
            revocation: None,
        });
    };
    let session: jazz::tools::Session = serde_json::from_value(stated.clone())
        .map_err(|error| format!("Invalid session format: {error}"))?;
    crate::portable::session::ws_validate_session_identity(&session, peer_identity)?;
    Ok(WebSocketAdmission {
        identity: peer_identity,
        claims: crate::portable::session::session_claims(session)?,
        trust: CommitUnitTrust::Session,
        credential: WebSocketCredential::Backend,
        requested_link: prelude.requested_link,
        bundle,
        revocation: None,
    })
}

// Lower-level tests: a Durable Object host admits through this function
// directly (its socket tests run in workerd, outside cargo test).
#[cfg(test)]
mod core_secret_tests {
    use super::*;

    const SECRET: &str = "s3cr3t-0123456789-0123456789-0123456789";

    fn prelude(auth: serde_json::Value, peer: &AuthorSubject) -> WebSocketPrelude {
        serde_json::from_value(serde_json::json!({
            "peer_identity": peer.canonical(),
            "auth": auth,
        }))
        .unwrap()
    }

    fn alice() -> AuthorSubject {
        AuthorSubject::authenticated("https://idp.example", "alice").unwrap()
    }

    fn upgrade<'a>(credential: &'a str, access: &'a str) -> Option<UpgradeCredential<'a>> {
        Some(UpgradeCredential { credential, access })
    }

    /// The core secret on the upgrade admits the backend as a trusted
    /// backend link, or with admin access as an authority link; a wrong
    /// secret of the same length, or none at all, is refused.
    #[test]
    fn the_upgrade_credential_decides_the_link() {
        let system = prelude(serde_json::json!({}), &AuthorSubject::SYSTEM);
        let backend = admit_by_core_secret(&system, upgrade(SECRET, "backend"), SECRET).unwrap();
        assert_eq!(backend.trust, CommitUnitTrust::TrustedBackend);
        assert_eq!(backend.credential, WebSocketCredential::Backend);
        let admin = admit_by_core_secret(&system, upgrade(SECRET, "admin"), SECRET).unwrap();
        assert_eq!(admin.trust, CommitUnitTrust::TrustedAuthority);
        assert_eq!(admin.credential, WebSocketCredential::Admin);

        let wrong = SECRET.replace('s', "x");
        assert_eq!(wrong.len(), SECRET.len());
        let refused = admit_by_core_secret(&system, upgrade(&wrong, "backend"), SECRET);
        assert_eq!(refused.unwrap_err(), "Invalid backend secret");
        assert!(admit_by_core_secret(&system, upgrade(SECRET, "owner"), SECRET).is_err());
        let refused = admit_by_core_secret(&system, None, SECRET).unwrap_err();
        assert!(
            refused.contains("admits only its app's backend"),
            "{refused}"
        );
    }

    /// Without an upgrade credential, the prelude's secrets admit as the
    /// same roles; a wrong one is refused as such.
    #[test]
    fn the_prelude_secrets_decide_the_link_without_an_upgrade_credential() {
        let as_backend = prelude(
            serde_json::json!({ "backend_secret": SECRET }),
            &AuthorSubject::SYSTEM,
        );
        let backend = admit_by_core_secret(&as_backend, None, SECRET).unwrap();
        assert_eq!(backend.trust, CommitUnitTrust::TrustedBackend);
        let as_admin = prelude(
            serde_json::json!({ "admin_secret": SECRET }),
            &AuthorSubject::SYSTEM,
        );
        assert_eq!(
            admit_by_core_secret(&as_admin, None, SECRET).unwrap().trust,
            CommitUnitTrust::TrustedAuthority
        );
        let wrong = prelude(
            serde_json::json!({ "backend_secret": "x".repeat(SECRET.len()) }),
            &AuthorSubject::SYSTEM,
        );
        assert_eq!(
            admit_by_core_secret(&wrong, None, SECRET).unwrap_err(),
            "Invalid backend secret"
        );
    }

    /// A user session the backend states is served as that user, under the
    /// user's policies, and only for the peer the prelude names; a guest's
    /// own local-first proof is not verified here and is refused.
    #[test]
    fn a_backend_stated_session_is_its_user_and_must_be_its_peer() {
        let stated = serde_json::json!({
            "backend_session": { "issuer": "https://idp.example", "user_id": "alice", "claims": { "team": "eng" } }
        });
        let admitted = admit_by_core_secret(
            &prelude(stated.clone(), &alice()),
            upgrade(SECRET, "backend"),
            SECRET,
        )
        .unwrap();
        assert_eq!(admitted.trust, CommitUnitTrust::Session);
        assert_eq!(admitted.trust.served_as(admitted.identity), alice());
        assert!(
            !admitted.claims.is_empty(),
            "the session's policy claims are bound"
        );

        let bob = AuthorSubject::authenticated("https://idp.example", "bob").unwrap();
        let refused =
            admit_by_core_secret(&prelude(stated, &bob), upgrade(SECRET, "backend"), SECRET);
        assert!(refused.unwrap_err().contains("peer_identity must match"));

        let guest = prelude(serde_json::json!({ "jwt_token": "a.b.c" }), &alice());
        let refused = admit_by_core_secret(&guest, upgrade(SECRET, "backend"), SECRET).unwrap_err();
        assert!(
            refused.contains("local-first proof is not verified"),
            "{refused}"
        );
    }
}
