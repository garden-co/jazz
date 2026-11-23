//! Admitting a local-first guest by its own proof: a self-signed token
//! (`jwt_token` in a socket's prelude) that proves the guest holds the key
//! its identity derives from, for one audience, until its `exp`.
//!
//! The steps, in the order every host takes them:
//!
//! 1. [`self_signed_issuer`]: only a Jazz self-signed proof is verified by a
//!    core; any other bearer token is refused ([`external_jwt_refused`]).
//! 2. [`guest_audience`]: the audience the proof must name: the app id, or
//!    the one the app's backend states (on its credential).
//! 3. [`self_signed_session`]: verify the proof at `now`; the session it
//!    proves.
//! 4. [`enrolled_account`]: the account the peer's identity names.
//! 5. [`founding_command`]: a local-first guest's first admission founds
//!    its account (idempotently afterwards).
//! 6. The registry's assignment of the guest ([`principal_of`]) must be
//!    that account ([`bind_account`]); then the session must be the peer
//!    the prelude named
//!    ([`crate::portable::session::ws_validate_session_identity`]).
//!
//! [`admit_guest`] takes them all over a [`Storage`] the host holds (a
//! Durable Object core). A host that reaches its registry another way
//! (native's owner thread) composes the same steps in the same order.

use jazz::account_registry::{AccountCommand, AccountId, Principal};
use jazz::ids::AuthorSubject;
use jazz::storage::Storage;
use jazz::tools::identity;
use jazz::tools::transport_error::UnauthenticatedResponse;
use jazz::tools::{AppId, AuthMode, Session};

use crate::portable::accounts::{self, RegistryError};

/// Why a guest was not admitted.
#[derive(Debug)]
pub enum GuestRefusal {
    /// The proof, or the audience it is checked against, did not
    /// authenticate.
    Unauthenticated(UnauthenticatedResponse),
    /// Authenticated, but not as the peer it names, or not as an enrolled
    /// account.
    Denied(String),
    /// The registry refused the guest, or could not be read or written.
    Registry(RegistryError),
}

impl std::fmt::Display for GuestRefusal {
    /// As a native core words the refusal on the wire, except that an
    /// unavailable registry also says why.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthenticated(error) => f.write_str(
                &serde_json::to_string(error)
                    .unwrap_or_else(|_| "authentication failed".to_owned()),
            ),
            Self::Denied(message) => f.write_str(message),
            Self::Registry(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for GuestRefusal {}

impl From<UnauthenticatedResponse> for GuestRefusal {
    fn from(error: UnauthenticatedResponse) -> Self {
        Self::Unauthenticated(error)
    }
}

impl From<RegistryError> for GuestRefusal {
    fn from(error: RegistryError) -> Self {
        Self::Registry(error)
    }
}

impl From<&str> for GuestRefusal {
    fn from(message: &str) -> Self {
        Self::Denied(message.to_owned())
    }
}

/// The Jazz self-signed issuer a token names (local-first or anonymous),
/// read without verifying it: whether a core verifies the token itself.
pub fn self_signed_issuer(token: &str) -> Option<&'static str> {
    use base64::Engine as _;
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return None;
    }
    let claims_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()?;
    #[derive(serde::Deserialize)]
    struct IssOnly {
        iss: Option<String>,
    }
    let claims = serde_json::from_slice::<IssOnly>(&claims_bytes).ok()?;
    match claims.iss.as_deref() {
        Some(identity::DEVICE_ACCOUNT_ISSUER) => Some(identity::DEVICE_ACCOUNT_ISSUER),
        Some(identity::ANONYMOUS_ISSUER) => Some(identity::ANONYMOUS_ISSUER),
        _ => None,
    }
}

/// The refusal of a bearer token that is not a Jazz self-signed proof: the
/// app's backend verifies its users' tokens and acts for them.
pub fn external_jwt_refused() -> UnauthenticatedResponse {
    UnauthenticatedResponse::disabled(
        "A core verifies no external JWTs: sign users in through your app's backend (createJazz)",
    )
}

/// The audience a guest's proof is verified against: `app_id`, or the
/// audience the app's backend `stated` (a browser client without an app id
/// signs for the endpoint it syncs with, which only that endpoint's backend
/// knows). A stated audience counts only on the backend's credential:
/// `backend_vouched` checks it (only when one is stated), answering native's
/// backend-secret refusal wording on failure.
pub fn guest_audience(
    stated: Option<&str>,
    app_id: AppId,
    backend_vouched: impl FnOnce() -> Result<(), &'static str>,
) -> Result<String, UnauthenticatedResponse> {
    let Some(audience) = stated else {
        return Ok(app_id.to_string());
    };
    let audience = Some(audience.trim())
        .filter(|audience| !audience.is_empty())
        .ok_or_else(|| UnauthenticatedResponse::invalid("Invalid device account audience"))?;
    backend_vouched().map_err(|message| {
        UnauthenticatedResponse::invalid(match message {
            "Invalid backend secret" => message,
            _ => "A device account audience must be vouched for with the backend secret",
        })
    })?;
    Ok(audience.to_owned())
}

/// Verify a self-signed proof of `issuer` (from [`self_signed_issuer`]) for
/// `audience` at `now_seconds` (Unix): the session it proves, before any
/// account is bound to it.
pub fn self_signed_session(
    token: &str,
    issuer: &'static str,
    audience: &str,
    now_seconds: u64,
) -> Result<Session, UnauthenticatedResponse> {
    let verified = identity::verify_jazz_self_signed_proof_at(token, audience, now_seconds)
        .map_err(|message| {
            if message.starts_with("token expired:") {
                UnauthenticatedResponse::expired("JWT has expired")
            } else {
                UnauthenticatedResponse::invalid(message)
            }
        })?;
    let auth_mode = match issuer {
        identity::ANONYMOUS_ISSUER => AuthMode::Anonymous,
        _ => AuthMode::DeviceAccount,
    };
    Ok(Session {
        account_id: None,
        issuer: verified.issuer.to_owned(),
        user_id: verified.user_id,
        // The verified key proves the issuer-scoped subject above; it is
        // transport proof material, not an application policy claim. In
        // particular, fresh anonymous proofs intentionally share the same
        // empty policy binding.
        claims: serde_json::Value::Object(serde_json::Map::new()),
        auth_mode,
    })
}

/// The account a guest's peer identity names: a guest is admitted only as
/// an enrolled account.
pub fn enrolled_account(peer_identity: AuthorSubject) -> Result<AccountId, &'static str> {
    peer_identity
        .account_id()
        .ok_or("public sessions require an enrolled account identity")
}

/// The registry principal a verified session is.
pub fn principal_of(session: &Session) -> Principal {
    Principal {
        issuer: session.issuer.clone(),
        subject: session.user_id.clone(),
    }
}

/// What a guest's admission runs in the registry before it looks the guest
/// up: a local-first guest founds its account in the namespace of the
/// `audience` its proof was verified against (a no-op once founded).
/// Nothing for any other issuer.
pub fn founding_command(principal: &Principal, audience: &str) -> Option<AccountCommand> {
    (principal.issuer == identity::DEVICE_ACCOUNT_ISSUER).then(|| {
        AccountCommand::FoundDeviceAccount {
            principal: principal.clone(),
            app: *accounts::device_account_founding_app(audience).uuid(),
        }
    })
}

/// Bind `account`, the one the peer names, to `session` once the registry
/// `assigned` the session's principal to it.
pub fn bind_account(
    session: &mut Session,
    account: AccountId,
    assigned: AccountId,
) -> Result<(), &'static str> {
    if assigned != account {
        return Err("account does not match authenticated identity assignment");
    }
    session.account_id = Some(account);
    Ok(())
}

/// Admit a guest by its own proof (`token`) as `peer_identity`, over the
/// registry kept in `storage` (opened with [`accounts::open`]), at
/// `now_seconds` (Unix): the session to serve it as, its account bound.
///
/// For a host that admits guests only on the app's backend credential,
/// verified before (a Durable Object core): the audience is the one the
/// backend `stated_audience`, or `app_id`.
pub async fn admit_guest(
    storage: &Storage,
    token: &str,
    peer_identity: AuthorSubject,
    stated_audience: Option<&str>,
    app_id: AppId,
    now_seconds: u64,
) -> Result<Session, GuestRefusal> {
    let issuer = self_signed_issuer(token).ok_or_else(external_jwt_refused)?;
    let audience = guest_audience(stated_audience, app_id, || Ok(()))?;
    let mut session = self_signed_session(token, issuer, &audience, now_seconds)?;
    let account = enrolled_account(peer_identity)?;
    let principal = principal_of(&session);
    if let Some(command) = founding_command(&principal, &audience) {
        accounts::execute(storage, &command).await?;
    }
    let assignment = accounts::login(storage, &principal).await?;
    bind_account(&mut session, account, assignment.account)?;
    crate::portable::session::ws_validate_session_identity(&session, peer_identity)
        .map_err(GuestRefusal::Denied)?;
    Ok(session)
}

#[cfg(test)]
mod tests {
    //! On both store owners #6064's registry tests use: the native SQLite
    //! owner and the emulated Durable Object SQL.

    use super::*;
    use jazz::tools::transport_error::UnauthenticatedCode;

    const NOW: u64 = 1_700_000_000;
    const TTL: u64 = 3600;

    fn app() -> AppId {
        AppId::from_name("guest-admission-test")
    }

    fn stores() -> [(&'static str, Storage); 2] {
        let stores = [
            ("native", Storage::open_in_memory().unwrap()),
            (
                "durable object emulation",
                Storage::managed_emulation_in_memory().unwrap(),
            ),
        ];
        for (_, storage) in &stores {
            jazz::db::block_on(accounts::open(storage)).unwrap();
        }
        stores
    }

    fn seed(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn proof(seed: &[u8; 32], audience: &str) -> String {
        identity::mint_jazz_self_signed_token_at(
            seed,
            identity::DEVICE_ACCOUNT_ISSUER,
            audience,
            TTL,
            NOW,
        )
        .unwrap()
    }

    fn subject(seed: &[u8; 32]) -> String {
        identity::derive_user_id(seed).to_string()
    }

    /// The peer identity a guest's client names: its local-first author,
    /// with the account founded in `audience`'s namespace.
    fn peer(seed: &[u8; 32], audience: &str) -> AuthorSubject {
        let subject = subject(seed);
        AuthorSubject::reserved(identity::DEVICE_ACCOUNT_ISSUER, &subject)
            .unwrap()
            .with_account(jazz::account_registry::device_account_id(
                *accounts::device_account_founding_app(audience).uuid(),
                &subject,
            ))
    }

    fn unauthenticated(refusal: GuestRefusal) -> UnauthenticatedResponse {
        match refusal {
            GuestRefusal::Unauthenticated(error) => error,
            other => panic!("expected an authentication refusal, got {other:?}"),
        }
    }

    fn denied(refusal: GuestRefusal) -> String {
        match refusal {
            GuestRefusal::Denied(message) => message,
            other => panic!("expected a denial, got {other:?}"),
        }
    }

    /// A valid proof founds the guest's account on its first admission and
    /// is served as that account, a local-first session without policy
    /// claims of its own; it is admitted the same way again.
    #[test]
    fn a_valid_proof_is_admitted_and_founds_its_account() {
        for (name, storage) in stores() {
            let app = app();
            let guest = seed(1);
            let peer = peer(&guest, &app.to_string());
            for _ in 0..2 {
                let session = jazz::db::block_on(admit_guest(
                    &storage,
                    &proof(&guest, &app.to_string()),
                    peer,
                    None,
                    app,
                    NOW,
                ))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
                assert_eq!(session.account_id, peer.account_id(), "{name}");
                assert_eq!(session.issuer, identity::DEVICE_ACCOUNT_ISSUER, "{name}");
                assert_eq!(session.user_id, subject(&guest), "{name}");
                assert_eq!(session.auth_mode, AuthMode::DeviceAccount, "{name}");
                assert_eq!(session.author_subject().unwrap(), peer, "{name}");
            }
            let principal = Principal {
                issuer: identity::DEVICE_ACCOUNT_ISSUER.into(),
                subject: subject(&guest),
            };
            assert_eq!(
                Some(
                    jazz::db::block_on(accounts::login(&storage, &principal))
                        .unwrap()
                        .account
                ),
                peer.account_id(),
                "{name}: the founding is durable"
            );
        }
    }

    /// A guest founded before (by `found-local-first`, or an earlier
    /// admission on another core of the app) is admitted as its account.
    #[test]
    fn a_founder_is_admitted() {
        for (name, storage) in stores() {
            let app = app();
            let guest = seed(2);
            let peer = peer(&guest, &app.to_string());
            jazz::db::block_on(accounts::found_device_account(
                &storage,
                serde_json::from_value(serde_json::json!({
                    "identity": { "issuer": identity::DEVICE_ACCOUNT_ISSUER, "subject": subject(&guest) },
                }))
                .unwrap(),
                *app.uuid(),
            ))
            .unwrap();
            let session = jazz::db::block_on(admit_guest(
                &storage,
                &proof(&guest, &app.to_string()),
                peer,
                None,
                app,
                NOW,
            ))
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(session.account_id, peer.account_id(), "{name}");
        }
    }

    /// The audience the app's backend states replaces the app id: the
    /// proof must name it, and it namespaces the founder's account.
    #[test]
    fn a_stated_audience_is_verified_and_namespaces_the_account() {
        for (name, storage) in stores() {
            let endpoint = "https://app.example/jazz";
            let guest = seed(3);
            let peer = peer(&guest, endpoint);
            let session = jazz::db::block_on(admit_guest(
                &storage,
                &proof(&guest, endpoint),
                peer,
                Some(endpoint),
                app(),
                NOW,
            ))
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(session.account_id, peer.account_id(), "{name}");

            let refused = jazz::db::block_on(admit_guest(
                &storage,
                &proof(&guest, endpoint),
                peer,
                Some("  "),
                app(),
                NOW,
            ))
            .unwrap_err();
            assert_eq!(
                unauthenticated(refused).message,
                "Invalid device account audience",
                "{name}"
            );
        }
    }

    /// A proof whose signature does not verify, or that names no Jazz
    /// self-signed issuer, authenticates nothing; nothing is founded.
    #[test]
    fn a_forged_proof_is_refused() {
        for (name, storage) in stores() {
            let app = app();
            let guest = seed(4);
            let peer = peer(&guest, &app.to_string());
            let valid = proof(&guest, &app.to_string());
            let (signed, signature) = valid.rsplit_once('.').unwrap();
            let other_signature = proof(&seed(5), &app.to_string())
                .rsplit_once('.')
                .unwrap()
                .1
                .to_owned();
            assert_ne!(signature, other_signature);
            let forged = format!("{signed}.{other_signature}");
            let refused = jazz::db::block_on(admit_guest(&storage, &forged, peer, None, app, NOW))
                .unwrap_err();
            assert_eq!(
                unauthenticated(refused).code,
                UnauthenticatedCode::Invalid,
                "{name}"
            );

            let refused = jazz::db::block_on(admit_guest(&storage, "a.b.c", peer, None, app, NOW))
                .unwrap_err();
            assert_eq!(
                unauthenticated(refused),
                external_jwt_refused(),
                "{name}: not a self-signed proof"
            );

            let principal = Principal {
                issuer: identity::DEVICE_ACCOUNT_ISSUER.into(),
                subject: subject(&guest),
            };
            assert!(
                jazz::db::block_on(accounts::login(&storage, &principal)).is_err(),
                "{name}: a refused proof founds nothing"
            );
        }
    }

    /// A proof is refused once its `exp` passed, as expired.
    #[test]
    fn an_expired_proof_is_refused() {
        for (name, storage) in stores() {
            let app = app();
            let guest = seed(6);
            let refused = jazz::db::block_on(admit_guest(
                &storage,
                &proof(&guest, &app.to_string()),
                peer(&guest, &app.to_string()),
                None,
                app,
                NOW + TTL + 1,
            ))
            .unwrap_err();
            let refused = unauthenticated(refused);
            assert_eq!(refused.code, UnauthenticatedCode::Expired, "{name}");
            assert_eq!(refused.message, "JWT has expired", "{name}");
        }
    }

    /// A proof signed for another app, or another audience than the one
    /// the backend states, is refused.
    #[test]
    fn a_wrong_audience_or_app_is_refused() {
        for (name, storage) in stores() {
            let app = app();
            let guest = seed(7);
            let other = AppId::from_name("another-app").to_string();
            let refused = jazz::db::block_on(admit_guest(
                &storage,
                &proof(&guest, &other),
                peer(&guest, &other),
                None,
                app,
                NOW,
            ))
            .unwrap_err();
            assert_eq!(
                unauthenticated(refused).code,
                UnauthenticatedCode::Invalid,
                "{name}: another app"
            );

            let refused = jazz::db::block_on(admit_guest(
                &storage,
                &proof(&guest, "https://app.example/jazz"),
                peer(&guest, "https://other.example/jazz"),
                Some("https://other.example/jazz"),
                app,
                NOW,
            ))
            .unwrap_err();
            assert_eq!(
                unauthenticated(refused).code,
                UnauthenticatedCode::Invalid,
                "{name}: another stated audience"
            );
        }
    }

    /// A guest is served only as the account the registry assigned it: a
    /// peer naming another account, or none, or another user, is refused;
    /// so is a guest already founded in another namespace.
    #[test]
    fn an_account_mismatch_is_refused() {
        for (name, storage) in stores() {
            let app = app();
            let audience = app.to_string();
            let guest = seed(8);
            let token = proof(&guest, &audience);

            let elsewhere = peer(&guest, "another-namespace");
            let refused =
                jazz::db::block_on(admit_guest(&storage, &token, elsewhere, None, app, NOW))
                    .unwrap_err();
            assert_eq!(
                denied(refused),
                "account does not match authenticated identity assignment",
                "{name}"
            );

            let someone_else = peer(&seed(9), &audience);
            let refused =
                jazz::db::block_on(admit_guest(&storage, &token, someone_else, None, app, NOW))
                    .unwrap_err();
            assert_eq!(
                denied(refused),
                "account does not match authenticated identity assignment",
                "{name}: another user's account"
            );

            let unenrolled =
                AuthorSubject::reserved(identity::DEVICE_ACCOUNT_ISSUER, &subject(&guest)).unwrap();
            let refused =
                jazz::db::block_on(admit_guest(&storage, &token, unenrolled, None, app, NOW))
                    .unwrap_err();
            assert_eq!(
                denied(refused),
                "public sessions require an enrolled account identity",
                "{name}"
            );

            // Founded under the app id above; a proof for another audience
            // cannot found it again elsewhere.
            let endpoint = "https://app.example/jazz";
            let refused = jazz::db::block_on(admit_guest(
                &storage,
                &proof(&guest, endpoint),
                peer(&guest, endpoint),
                Some(endpoint),
                app,
                NOW,
            ))
            .unwrap_err();
            assert!(
                matches!(
                    refused,
                    GuestRefusal::Registry(RegistryError::Decision(
                        jazz::account_registry::AccountError::AlreadyAssigned
                    ))
                ),
                "{name}: {refused:?}"
            );
        }
    }

    /// An anonymous proof verifies, but founds no account: it is not
    /// admitted as one.
    #[test]
    fn an_anonymous_proof_is_not_an_account() {
        for (name, storage) in stores() {
            let app = app();
            let guest = seed(10);
            let token = identity::mint_jazz_self_signed_token_at(
                &guest,
                identity::ANONYMOUS_ISSUER,
                &app.to_string(),
                TTL,
                NOW,
            )
            .unwrap();
            let peer = AuthorSubject::reserved(identity::ANONYMOUS_ISSUER, &subject(&guest))
                .unwrap()
                .with_account(jazz::account_registry::AccountId(uuid::Uuid::from_u128(1)));
            let refused = jazz::db::block_on(admit_guest(&storage, &token, peer, None, app, NOW))
                .unwrap_err();
            assert!(
                matches!(
                    refused,
                    GuestRefusal::Registry(RegistryError::Decision(
                        jazz::account_registry::AccountError::NotAssigned
                    ))
                ),
                "{name}: {refused:?}"
            );
        }
    }

    /// A stated audience counts only on the backend's word, refused in
    /// native's wording otherwise; without one the app id is the audience
    /// and the backend is not asked.
    #[test]
    fn a_stated_audience_needs_the_backend() {
        let app = app();
        assert_eq!(
            guest_audience(None, app, || panic!("not asked")).unwrap(),
            app.to_string()
        );
        assert_eq!(
            guest_audience(Some(" https://app.example "), app, || Ok(())).unwrap(),
            "https://app.example"
        );
        assert_eq!(
            guest_audience(Some("https://app.example"), app, || Err(
                "Invalid backend secret"
            ))
            .unwrap_err()
            .message,
            "Invalid backend secret"
        );
        assert_eq!(
            guest_audience(Some("https://app.example"), app, || Err(
                "Backend secret required"
            ))
            .unwrap_err()
            .message,
            "A device account audience must be vouched for with the backend secret"
        );
    }
}
