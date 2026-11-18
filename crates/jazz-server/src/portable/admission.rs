//! What a WebSocket connection is admitted as: the prelude a peer opens
//! with, the credential class and link it is admitted with, and the
//! server-owned link capability selected after wire negotiation. How the
//! credential is verified stays with each host.

use std::collections::BTreeMap;

use jazz::ids::AuthorSubject;
use jazz::records::Value as CoreValue;
use jazz::wire::{WireError, WireErrorCode, WireErrorReason, WireRetry};

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

/// Compare two secrets in time independent of where they differ.
pub fn same_secret(expected: &str, given: &str) -> bool {
    let (a, b) = (expected.as_bytes(), given.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// How a socket's upgrade request vouched for it: the core secret it
/// carried (`X-Jazz-Backend-Credential`) and the access it asked for
/// (`X-Jazz-Backend-Access`: `admin` or `backend`; without one, session
/// access: the backend relays a client that proves who it is itself).
pub struct UpgradeCredential<'a> {
    pub credential: &'a str,
    pub access: Option<&'a str>,
}

/// Admits a relayed guest by its own proof: given the proof, the peer it
/// must be and the audience the backend stated, the session to serve it as.
pub type GuestVerifier<'a> =
    dyn FnMut(&str, AuthorSubject, Option<&str>) -> Result<jazz::tools::Session, String> + 'a;

/// Admit a socket to a core that admits only its app's backend, by the
/// core's one `secret` (a Durable Object core): as a native core does with
/// a backend credential and no trusted workload.
///
/// - The upgrade's credential, or the prelude's `admin_secret` /
///   `backend_secret`, must be `secret`; an admin one is an authority link.
/// - A backend's own link is served as the backend; a user session the
///   backend states (`backend_session`) is served under that user's
///   policies, and must be the peer its prelude named.
/// - A local-first guest's own proof (`jwt_token`) comes on session access
///   and is admitted by `guest` (below); on backend access it is refused.
/// - A credential that holds decides the access, and the prelude's secret
///   slots are ignored (as native's workload gate): without a stated
///   access it vouches only that it relays a client, which must prove who
///   it is itself.
/// - A stated session carries its [`SessionRevocation`]: when the backend's
///   `authenticate` started for it (`authenticatedAt`), else `now_ms`, the
///   time the core admits it (Unix ms).
/// - A guest the backend relays (session access, the guest's own proof in
///   `jwt_token`) is admitted by `guest`: given the proof, the peer it must
///   be and the audience the backend stated (`local_first_audience`), the
///   session to serve it as (see `portable::guest::admit_guest`).
/// Why [`admit_by_core_secret`] refused a connection: its message, and the
/// [`WireErrorReason`] a client that reads one is told, when it is known.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreSecretRefusal {
    /// The refusal's message, which older clients classify.
    pub message: String,
    /// Why, when it is a credential's refusal this function decided.
    pub reason: Option<WireErrorReason>,
}

impl CoreSecretRefusal {
    fn because(message: impl Into<String>, reason: WireErrorReason) -> Self {
        Self {
            message: message.into(),
            reason: Some(reason),
        }
    }

    /// The `auth_failed` error the client is told.
    pub fn into_wire(self) -> WireError {
        let mut error = WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, self.message);
        error.reason = self.reason;
        error
    }
}

impl From<String> for CoreSecretRefusal {
    fn from(message: String) -> Self {
        Self {
            message,
            reason: None,
        }
    }
}

impl std::fmt::Display for CoreSecretRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::ops::Deref for CoreSecretRefusal {
    type Target = str;
    fn deref(&self) -> &str {
        &self.message
    }
}

impl PartialEq<&str> for CoreSecretRefusal {
    fn eq(&self, other: &&str) -> bool {
        self.message == *other
    }
}

pub fn admit_by_core_secret(
    prelude: &WebSocketPrelude,
    upgrade: Option<UpgradeCredential<'_>>,
    secret: &str,
    now_ms: u64,
    guest: &mut GuestVerifier<'_>,
) -> Result<WebSocketAdmission, CoreSecretRefusal> {
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
            Some("admin") => (true, true),
            Some("backend") => (false, true),
            None => {
                let Some(token) = auth.jwt_token.as_deref() else {
                    return Err(CoreSecretRefusal::because(
                        "Session required. Provide a backend session or a local-first proof.",
                        WireErrorReason::Missing,
                    ));
                };
                let session = guest(
                    token,
                    peer_identity,
                    prelude.local_first_audience.as_deref(),
                )?;
                return Ok(WebSocketAdmission {
                    identity: peer_identity,
                    claims: crate::portable::session::session_claims(session)?,
                    trust: CommitUnitTrust::Session,
                    credential: WebSocketCredential::Session,
                    requested_link: prelude.requested_link,
                    bundle,
                    revocation: None,
                });
            }
            Some(other) => return Err(format!("unknown backend access `{other}`").into()),
        },
        Some(_) => {
            return Err(CoreSecretRefusal::because(
                "Invalid backend secret",
                WireErrorReason::Invalid,
            ));
        }
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
            (Some(_), _) | (_, Some(_)) => {
                CoreSecretRefusal::because("Invalid backend secret", WireErrorReason::Invalid)
            }
            // A credential the client sent that this core does not take is
            // `invalid`; only a client that sent none is `missing`.
            _ => CoreSecretRefusal::because(
                "this core admits only its app's backend: connect through the app's createJazz handler",
                if auth.jwt_token.is_some() || auth.backend_session.is_some() {
                    WireErrorReason::Invalid
                } else {
                    WireErrorReason::Missing
                },
            ),
        });
    }
    if auth.jwt_token.is_some() {
        return Err(CoreSecretRefusal::because(
            "a guest's local-first proof is admitted on session access: relay it with the backend credential and no X-Jazz-Backend-Access",
            WireErrorReason::Invalid,
        ));
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
    crate::portable::session::ws_validate_session_identity(&session, peer_identity)
        .map_err(|message| CoreSecretRefusal::because(message, WireErrorReason::Invalid))?;
    let revocation = SessionRevocation {
        principal: jazz::account_registry::Principal {
            issuer: session.issuer.clone(),
            subject: session.user_id.clone(),
        },
        authenticated_at_ms: stated
            .get("authenticatedAt")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(now_ms),
    };
    Ok(WebSocketAdmission {
        identity: peer_identity,
        claims: crate::portable::session::session_claims(session)?,
        trust: CommitUnitTrust::Session,
        credential: WebSocketCredential::Backend,
        requested_link: prelude.requested_link,
        bundle,
        revocation: Some(revocation),
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

    const NOW: u64 = 1_700_000_000_000;

    fn upgrade<'a>(credential: &'a str, access: &'a str) -> Option<UpgradeCredential<'a>> {
        Some(UpgradeCredential {
            credential,
            access: Some(access),
        })
    }

    fn admit(
        prelude: &WebSocketPrelude,
        upgrade: Option<UpgradeCredential<'_>>,
    ) -> Result<WebSocketAdmission, CoreSecretRefusal> {
        admit_by_core_secret(prelude, upgrade, SECRET, NOW, &mut |token, _, _| {
            Err(format!("no guest verifier in this test (proof `{token}`)"))
        })
    }

    /// The core secret on the upgrade admits the backend as a trusted
    /// backend link, or with admin access as an authority link; a wrong
    /// secret of the same length, or none at all, is refused.
    #[test]
    fn the_upgrade_credential_decides_the_link() {
        let system = prelude(serde_json::json!({}), &AuthorSubject::SYSTEM);
        let backend = admit(&system, upgrade(SECRET, "backend")).unwrap();
        assert_eq!(backend.trust, CommitUnitTrust::TrustedBackend);
        assert_eq!(backend.credential, WebSocketCredential::Backend);
        let admin = admit(&system, upgrade(SECRET, "admin")).unwrap();
        assert_eq!(admin.trust, CommitUnitTrust::TrustedAuthority);
        assert_eq!(admin.credential, WebSocketCredential::Admin);

        let wrong = SECRET.replace('s', "x");
        assert_eq!(wrong.len(), SECRET.len());
        let refused = admit(&system, upgrade(&wrong, "backend"));
        assert_eq!(refused.unwrap_err(), "Invalid backend secret");
        assert!(admit(&system, upgrade(SECRET, "owner")).is_err());
        let refused = admit(&system, None).unwrap_err();
        assert!(
            refused.contains("admits only its app's backend"),
            "{refused}"
        );
    }

    /// #5927 R3b: a Durable Object core's refusals name their reason as the
    /// native core's do. A credential the client sent that this core does not
    /// take is `invalid`; only a client that sent none is `missing`.
    #[test]
    fn a_refusal_names_why_the_credential_was_refused() {
        let reason = |auth: serde_json::Value, upgrade| {
            admit(&prelude(auth, &alice()), upgrade).unwrap_err().reason
        };
        let wrong = SECRET.replace('s', "x");
        assert_eq!(
            reason(serde_json::json!({}), upgrade(&wrong, "backend")),
            Some(WireErrorReason::Invalid)
        );
        assert_eq!(
            reason(serde_json::json!({ "backend_secret": wrong }), None),
            Some(WireErrorReason::Invalid)
        );
        assert_eq!(
            reason(serde_json::json!({}), None),
            Some(WireErrorReason::Missing)
        );
        assert_eq!(
            reason(serde_json::json!({ "jwt_token": "a.b.c" }), None),
            Some(WireErrorReason::Invalid)
        );
        // Session access with no proof to admit.
        let session_access = Some(UpgradeCredential {
            credential: SECRET,
            access: None,
        });
        assert_eq!(
            reason(serde_json::json!({}), session_access),
            Some(WireErrorReason::Missing)
        );
        // The refusal reaches the client as an `auth_failed` naming it.
        let error = admit(&prelude(serde_json::json!({}), &alice()), None)
            .unwrap_err()
            .into_wire();
        assert_eq!(error.code, WireErrorCode::AuthFailed);
        assert_eq!(error.reason, Some(WireErrorReason::Missing));
    }

    /// Without an upgrade credential, the prelude's secrets admit as the
    /// same roles; a wrong one is refused as such.
    #[test]
    fn the_prelude_secrets_decide_the_link_without_an_upgrade_credential() {
        let as_backend = prelude(
            serde_json::json!({ "backend_secret": SECRET }),
            &AuthorSubject::SYSTEM,
        );
        let backend = admit(&as_backend, None).unwrap();
        assert_eq!(backend.trust, CommitUnitTrust::TrustedBackend);
        let as_admin = prelude(
            serde_json::json!({ "admin_secret": SECRET }),
            &AuthorSubject::SYSTEM,
        );
        assert_eq!(
            admit(&as_admin, None).unwrap().trust,
            CommitUnitTrust::TrustedAuthority
        );
        let wrong = prelude(
            serde_json::json!({ "backend_secret": "x".repeat(SECRET.len()) }),
            &AuthorSubject::SYSTEM,
        );
        assert_eq!(admit(&wrong, None).unwrap_err(), "Invalid backend secret");
    }

    /// A user session the backend states is served as that user, under the
    /// user's policies, and only for the peer the prelude names; a guest's
    /// own local-first proof on backend access is refused (it comes on
    /// session access).
    #[test]
    fn a_backend_stated_session_is_its_user_and_must_be_its_peer() {
        let stated = serde_json::json!({
            "backend_session": { "issuer": "https://idp.example", "user_id": "alice", "claims": { "team": "eng" } }
        });
        let admitted = admit(
            &prelude(stated.clone(), &alice()),
            upgrade(SECRET, "backend"),
        )
        .unwrap();
        assert_eq!(admitted.trust, CommitUnitTrust::Session);
        assert_eq!(admitted.trust.served_as(admitted.identity), alice());
        assert!(
            !admitted.claims.is_empty(),
            "the session's policy claims are bound"
        );

        let bob = AuthorSubject::authenticated("https://idp.example", "bob").unwrap();
        let refused = admit(&prelude(stated, &bob), upgrade(SECRET, "backend"));
        assert!(refused.unwrap_err().contains("peer_identity must match"));

        let guest = prelude(serde_json::json!({ "jwt_token": "a.b.c" }), &alice());
        let refused = admit(&guest, upgrade(SECRET, "backend")).unwrap_err();
        assert!(refused.contains("admitted on session access"), "{refused}");
    }

    /// A stated session carries what a revocation is checked against: its
    /// user, and when the backend's `authenticate` started for it (else
    /// the time the core admits it).
    #[test]
    fn a_backend_stated_session_carries_its_revocation() {
        let stated = |at: Option<u64>| {
            let mut session = serde_json::json!({ "issuer": "https://idp.example", "user_id": "alice", "claims": {} });
            if let Some(at) = at {
                session["authenticatedAt"] = at.into();
            }
            prelude(serde_json::json!({ "backend_session": session }), &alice())
        };
        let admitted = admit(&stated(Some(42)), upgrade(SECRET, "backend")).unwrap();
        let revocation = admitted.revocation.expect("a stated session is revocable");
        assert_eq!(revocation.principal.issuer, "https://idp.example");
        assert_eq!(revocation.principal.subject, "alice");
        assert_eq!(revocation.authenticated_at_ms, 42);
        let unstated = admit(&stated(None), upgrade(SECRET, "backend")).unwrap();
        assert_eq!(unstated.revocation.unwrap().authenticated_at_ms, NOW);
        let backend = prelude(serde_json::json!({}), &AuthorSubject::SYSTEM);
        assert!(
            admit(&backend, upgrade(SECRET, "backend"))
                .unwrap()
                .revocation
                .is_none()
        );
    }

    /// A credential without a stated access vouches only that the backend
    /// relays a client: the prelude's secret slots (a stand-in) are
    /// ignored, and the client must prove who it is itself.
    #[test]
    fn a_credential_without_access_ignores_the_prelude_secrets() {
        let relayed = prelude(
            serde_json::json!({ "backend_secret": "stated-by-backend-credential", "admin_secret": SECRET }),
            &alice(),
        );
        let session_access = Some(UpgradeCredential {
            credential: SECRET,
            access: None,
        });
        let refused = admit(&relayed, session_access).unwrap_err();
        assert!(refused.contains("Session required"), "{refused}");
        let guest = prelude(
            serde_json::json!({ "jwt_token": "a.b.c", "backend_secret": "stated-by-backend-credential" }),
            &alice(),
        );
        let refused = admit(
            &guest,
            Some(UpgradeCredential {
                credential: SECRET,
                access: None,
            }),
        )
        .unwrap_err();
        assert!(refused.contains("no guest verifier"), "{refused}");

        // The verifier's session is served as the guest, by its own proof.
        // (What the verifier answers is its business: any session will do.)
        let guest_session: jazz::tools::Session = serde_json::from_value(serde_json::json!({
            "issuer": "https://idp.example", "user_id": "guest", "claims": {}
        }))
        .unwrap();
        let admitted = admit_by_core_secret(
            &guest,
            Some(UpgradeCredential {
                credential: SECRET,
                access: None,
            }),
            SECRET,
            NOW,
            &mut |token, peer, audience| {
                assert_eq!(token, "a.b.c");
                assert_eq!(peer, alice());
                assert_eq!(audience, None);
                Ok(guest_session.clone())
            },
        )
        .unwrap();
        assert_eq!(admitted.trust, CommitUnitTrust::Session);
        assert_eq!(admitted.credential, WebSocketCredential::Session);
        assert!(admitted.revocation.is_none());
    }
}
