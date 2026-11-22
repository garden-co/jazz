//! Account control endpoints, for the app's backend: it vouches for the
//! identities it names with its admin secret.
use crate::portable::accounts::{
    self, AccountResponse, FoundDeviceAccountRequest, IdentityRequest, LinkRequest, RegistryError,
    RevokeRequest, RevokeSessionsRequest, RevokeSessionsResponse, RouteFailure, registry_failure,
};
use crate::server::ServerState;
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use jazz::account_registry::{AccountCommand, AccountId, Assignment, Principal};
use jazz::tools::transport_error::{UnauthenticatedCode, UnauthenticatedResponse};
use std::sync::Arc;
use uuid::Uuid;

type Failure = (StatusCode, &'static str);

/// Validate backend service authority without creating or impersonating an account.
/// The surrounding app-id gate binds admission to this configured application.
pub(super) async fn admit_backend(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<StatusCode, Failure> {
    crate::middleware::auth::validate_backend_secret(
        headers
            .get("x-jazz-backend-secret")
            .and_then(|value| value.to_str().ok()),
        &state.auth_config,
    )?;
    Ok(StatusCode::NO_CONTENT)
}

/// The app's backend, vouching for the identities it names: its admin
/// secret, or the backend secret a backend client holds (a backend client may
/// already act as any user on its sockets, so naming the user's account here
/// grants it nothing more). Linking and revoking stay admin-only.
fn validate_vouching_backend(headers: &HeaderMap, state: &ServerState) -> Result<(), Failure> {
    match super::http::admin_secret_header(headers) {
        Some(admin) => {
            crate::middleware::auth::validate_admin_secret(Some(admin), &state.auth_config)
        }
        None => crate::middleware::auth::validate_backend_secret(
            headers
                .get("x-jazz-backend-secret")
                .and_then(|value| value.to_str().ok()),
            &state.auth_config,
        ),
    }
}

fn owner(state: &ServerState) -> Result<&crate::server::accounts::AccountRegistryOwner, Failure> {
    state.accounts.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "account operation requires the core authority",
    ))
}

/// A refusal decided in [`crate::portable::accounts`], as axum answers it.
fn fail(failure: RouteFailure) -> Failure {
    (
        StatusCode::from_u16(failure.status).expect("an account route status"),
        failure.body,
    )
}

fn failure(error: RegistryError) -> Failure {
    fail(registry_failure(error))
}

/// Read-only administrative lookup;
/// service authority can resolve an assignment but cannot create or link one.
pub(super) async fn resolve_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<IdentityRequest>,
) -> Result<Json<AccountResponse>, Failure> {
    validate_vouching_backend(&headers, &state)?;
    let assignment = owner(&state)?
        .login(request.identity.clone())
        .await
        .map_err(failure)?;
    Ok(response(request.identity, assignment))
}

/// The app's backend vouches for this identity (`createJazz` in front of a
/// remote core): its own auth verified the user, so the core resolves the
/// assignment or creates the account, as `login-or-register` would for that
/// user's own credential.
pub(super) async fn login_or_register_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<IdentityRequest>,
) -> Result<Json<AccountResponse>, Failure> {
    validate_vouching_backend(&headers, &state)?;
    accounts::require_external_identity(&request.identity).map_err(fail)?;
    let assignment = owner(&state)?
        .login_or_register(request.identity.clone())
        .await
        .map_err(failure)?;
    Ok(response(request.identity, assignment))
}

/// The rule that an encrypting app serves only device accounts founded
/// in its own namespace (E2EE protocol R5, R6): the registry keeps no record
/// of which namespace founded an account, so an account founded for another
/// audience could not be told from the app's own, and no account record is
/// accepted for it. Founding one is refused at the first point the server
/// sees it, here, and the registry view refuses its records
/// (`founded_for_another_audience`) for any that exist already.
async fn refuse_foreign_founding(state: &ServerState, app: uuid::Uuid) -> Result<(), &'static str> {
    if app == *state.app_id.uuid() {
        return Ok(());
    }
    let encrypts = match state.runtime() {
        Some(runtime) => runtime.encrypts().await.unwrap_or(true),
        None => false,
    };
    if encrypts {
        Err("an app that encrypts columns serves device accounts founded for its own app id only")
    } else {
        Ok(())
    }
}

/// The app's backend vouches for a local-first guest
/// ([`FoundDeviceAccountRequest`]).
pub(super) async fn found_device_account_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<FoundDeviceAccountRequest>,
) -> Result<Json<AccountResponse>, Failure> {
    validate_vouching_backend(&headers, &state)?;
    accounts::require_device_account_identity(&request.identity).map_err(fail)?;
    let registry = owner(&state)?;
    let app =
        accounts::founding_app(request.audience.as_deref(), *state.app_id.uuid()).map_err(fail)?;
    refuse_foreign_founding(&state, app)
        .await
        .map_err(|reason| {
            fail(crate::portable::accounts::RouteFailure {
                status: 400,
                body: reason,
            })
        })?;
    let result = registry
        .execute(AccountCommand::FoundDeviceAccount {
            principal: request.identity.clone(),
            app,
        })
        .await
        .map_err(failure)?;
    Ok(response(request.identity, accounts::assignment_of(result)))
}

/// The app's backend vouches for both identities: link `to` to the account
/// `from` uses, in the same request-and-accept steps the two users' own
/// credentials would take.
pub(super) async fn link_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<LinkRequest>,
) -> Result<Json<AccountResponse>, Failure> {
    crate::middleware::auth::validate_admin_secret(
        super::http::admin_secret_header(&headers),
        &state.auth_config,
    )?;
    accounts::require_external_identity(&request.to).map_err(fail)?;
    let registry = owner(&state)?;
    // `from` names an account that already exists (a user the backend signed
    // in, or a device account founded from its own key proof): linking
    // never creates it.
    registry
        .login(request.from.clone())
        .await
        .map_err(failure)?;
    let now = state.auth_config.clock.now_seconds();
    let steps = accounts::link_steps(&request, now, Uuid::new_v4).map_err(fail)?;
    registry.execute(steps.request).await.map_err(failure)?;
    let result = registry.execute(steps.accept).await.map_err(failure)?;
    Ok(response(request.to, accounts::assignment_of(result)))
}

/// The app's backend vouches for `approver`, an identity of the account, and
/// revokes `target`, another of its identities, as the approver's own
/// credential would (`createJazz` revokes a device account's key once it
/// links to a signed-in user, when the app asks for that). The assignment
/// stays: the target can no longer sign in, and is never reassigned.
pub(super) async fn revoke_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<RevokeRequest>,
) -> Result<StatusCode, Failure> {
    crate::middleware::auth::validate_admin_secret(
        super::http::admin_secret_header(&headers),
        &state.auth_config,
    )?;
    owner(&state)?
        .execute(AccountCommand::Revoke {
            approver: request.approver,
            target: request.target,
        })
        .await
        .map_err(failure)?;
    Ok(StatusCode::NO_CONTENT)
}

/// The message a revoked session closes with. `createJazz` reads its prefix
/// to end the browser's session as refused by `jazz.revokeUser`.
pub(super) const SESSION_REVOKED: &str =
    "session revoked: jazz.revokeUser ended this user's sessions";

/// `jazz.revokeUser`: end the identity's sessions authenticated before
/// `revokedBefore`, on every backend instance. Each live session of the
/// user closes as revoked, and the core refuses those the backend states
/// later with an older `authenticatedAt`. The assignment is untouched: the
/// user signs in again as before.
pub(super) async fn revoke_sessions_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<RevokeSessionsRequest>,
) -> Result<Json<RevokeSessionsResponse>, Failure> {
    crate::middleware::auth::validate_admin_secret(
        super::http::admin_secret_header(&headers),
        &state.auth_config,
    )?;
    let revoked_before = owner(&state)?
        .revoke_sessions(request.identity.clone(), request.revoked_before)
        .await
        .map_err(failure)?;
    Ok(Json(RevokeSessionsResponse {
        identity: request.identity,
        revoked_before,
    }))
}

/// The app's backend signing key (see
/// [`crate::portable::backend_signing_key`]), for the app's trusted backend
/// only: the admin secret, or a backend credential stating admin access.
/// Derived from a configured admin secret; otherwise the key kept in the
/// store, so it outlives this process either way.
pub(super) async fn backend_signing_key_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<impl axum::response::IntoResponse, Failure> {
    use crate::portable::backend_signing_key as signing;
    crate::middleware::auth::validate_admin_secret(
        super::http::admin_secret_header(&headers),
        &state.auth_config,
    )?;
    let key = match &state.backend_signing_secret {
        Some(secret) => signing::derived(secret, &state.app_id.to_string()),
        None => owner(&state)?
            .backend_signing_key()
            .await
            .map_err(failure)?,
    };
    Ok((
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (axum::http::header::CACHE_CONTROL, "no-store"),
        ],
        signing::body(&key),
    ))
}

/// Authentication denial and temporary registry availability are different wire outcomes.
#[derive(Debug)]
pub(in crate::server) enum AdmissionError {
    Denied(String),
    /// The user's sessions were revoked after this one was authenticated:
    /// the backend asks its `authenticate` again.
    Revoked,
    /// A denial whose reason is known: the client is told it as a
    /// [`jazz::wire::WireErrorReason`] when it reads one, and by `message`
    /// otherwise.
    Refused {
        message: String,
        reason: jazz::wire::WireErrorReason,
    },
    Unavailable,
    AuthenticationUnavailable(String),
}
impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Denied(message) | Self::Refused { message, .. } => f.write_str(message),
            Self::Unavailable => f.write_str("account registry unavailable"),
            Self::Revoked => f.write_str(SESSION_REVOKED),
            Self::AuthenticationUnavailable(message) => f.write_str(message),
        }
    }
}
impl From<String> for AdmissionError {
    fn from(message: String) -> Self {
        Self::Denied(message)
    }
}
impl From<&str> for AdmissionError {
    fn from(message: &str) -> Self {
        Self::Denied(message.into())
    }
}
impl From<UnauthenticatedResponse> for AdmissionError {
    fn from(error: UnauthenticatedResponse) -> Self {
        match error.code {
            UnauthenticatedCode::Unavailable => Self::AuthenticationUnavailable(error.message),
            code => Self::Refused {
                // Clients that read no reason classify this text.
                message: serde_json::to_string(&error)
                    .unwrap_or_else(|_| "authentication failed".to_owned()),
                reason: match code {
                    UnauthenticatedCode::Expired => jazz::wire::WireErrorReason::Expired,
                    UnauthenticatedCode::Missing => jazz::wire::WireErrorReason::Missing,
                    UnauthenticatedCode::Disabled => jazz::wire::WireErrorReason::Disabled,
                    UnauthenticatedCode::Invalid | UnauthenticatedCode::Unavailable => {
                        jazz::wire::WireErrorReason::Invalid
                    }
                },
            },
        }
    }
}
impl From<RegistryError> for AdmissionError {
    fn from(error: RegistryError) -> Self {
        match error {
            RegistryError::Unavailable(_) => Self::Unavailable,
            RegistryError::Decision(error) => Self::Denied(error.to_string()),
        }
    }
}
impl AdmissionError {
    /// A denial naming why.
    pub(super) fn refused(message: impl Into<String>, reason: jazz::wire::WireErrorReason) -> Self {
        Self::Refused {
            message: message.into(),
            reason,
        }
    }

    pub(super) fn into_wire(self) -> jazz::wire::WireError {
        use jazz::wire::{WireError, WireErrorCode, WireRetry};
        match self {
            Self::Unavailable => {
                WireError::new(WireErrorCode::NotReady, WireRetry::Later, self.to_string())
            }
            Self::Denied(message) => {
                WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, message)
            }
            Self::Revoked => WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterAuth,
                SESSION_REVOKED,
            )
            .with_reason(jazz::wire::WireErrorReason::Revoked),
            Self::Refused { message, reason } => {
                WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, message)
                    .with_reason(reason)
            }
            Self::AuthenticationUnavailable(message) => {
                WireError::new(WireErrorCode::NotReady, WireRetry::Later, message)
            }
        }
    }
}

pub(super) async fn resolve_assignment(
    state: &ServerState,
    principal: Principal,
) -> Result<AccountId, AdmissionError> {
    if let Some(registry) = &state.accounts {
        return registry
            .login(principal)
            .await
            .map(|value| value.account)
            .map_err(AdmissionError::from);
    }
    Err(AdmissionError::Unavailable)
}

/// Offline local founding becomes durable on the first authenticated
/// connection: what [`crate::portable::guest::founding_command`] runs for a
/// local-first guest, in the namespace of the audience its proof was
/// verified against (see
/// [`crate::middleware::auth::DEVICE_ACCOUNT_AUDIENCE_HEADER`]).
pub(in crate::server) async fn admit_local_founder(
    state: &ServerState,
    principal: &Principal,
    headers: &HeaderMap,
) -> Result<(), AdmissionError> {
    let audience = crate::middleware::auth::device_account_audience(
        headers,
        state.app_id,
        &state.auth_config,
    )?;
    let Some(command) = crate::portable::guest::founding_command(principal, &audience) else {
        return Ok(());
    };
    refuse_foreign_founding(
        state,
        *crate::portable::accounts::device_account_founding_app(&audience).uuid(),
    )
    .await?;
    if let Some(registry) = &state.accounts {
        registry
            .execute(command)
            .await
            .map_err(AdmissionError::from)?;
        return Ok(());
    }
    Err(AdmissionError::Unavailable)
}
fn response(identity: Principal, assignment: Assignment) -> Json<AccountResponse> {
    Json(AccountResponse::new(identity, assignment))
}

#[cfg(all(test, feature = "embedded-server"))]
mod tests {
    use super::*;
    use crate::server::testing::JazzServer;

    /// Concurrent first-device requests and a lost-response retry must resolve
    /// one immutable assignment at Core.
    #[tokio::test]
    async fn login_or_register_is_atomic_and_revocation_is_permanent() {
        let core = JazzServer::start().await.expect("start test server");
        let client = reqwest::Client::new();
        let admin = format!("{}/apps/{}/admin/accounts", core.base_url(), core.app_id());
        let identity = serde_json::json!({ "issuer": "jazz:app:test", "subject": "new-account" });
        let url = format!("{admin}/login-or-register");
        let login = |client: reqwest::Client,
                     url: String,
                     secret: String,
                     identity: serde_json::Value| async move {
            client
                .post(url)
                .header("x-jazz-admin-secret", secret)
                .json(&serde_json::json!({ "identity": identity }))
                .send()
                .await
                .unwrap()
        };
        let mut requests = Vec::new();
        for _ in 0..16 {
            let request = login(
                client.clone(),
                url.clone(),
                core.admin_secret().to_owned(),
                identity.clone(),
            );
            requests.push(tokio::spawn(async move {
                let response = request.await;
                assert_eq!(response.status(), StatusCode::OK);
                response.json::<AccountResponse>().await.unwrap().account
            }));
        }
        let mut account = None;
        for request in requests {
            let resolved = request.await.unwrap();
            assert_eq!(*account.get_or_insert(resolved), resolved);
        }
        // The backend may have lost any preceding response: no request ID is
        // necessary to recover the immutable identity assignment.
        let retry = login(
            client.clone(),
            url.clone(),
            core.admin_secret().to_owned(),
            identity.clone(),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::OK);
        assert_eq!(
            retry.json::<AccountResponse>().await.unwrap().account,
            account.unwrap()
        );
        let revoked = client
            .post(format!("{admin}/revoke"))
            .header("x-jazz-admin-secret", core.admin_secret())
            .json(&serde_json::json!({ "approver": identity, "target": identity }))
            .send()
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
        for operation in ["login-or-register", "resolve"] {
            let denied = login(
                client.clone(),
                format!("{admin}/{operation}"),
                core.admin_secret().to_owned(),
                identity.clone(),
            )
            .await;
            assert_eq!(denied.status(), StatusCode::FORBIDDEN, "{operation}");
        }
        core.shutdown().await;
    }

    #[tokio::test]
    async fn a_backend_client_names_accounts_but_cannot_link_or_revoke() {
        let core = JazzServer::start().await.expect("start test server");
        let client = reqwest::Client::new();
        let admin = format!("{}/apps/{}/admin/accounts", core.base_url(), core.app_id());
        let identity = serde_json::json!({ "issuer": "jazz:app:test", "subject": "user" });
        let other = serde_json::json!({ "issuer": "jazz:app:test", "subject": "other" });
        let post = |operation: &str, secret: &str, body: serde_json::Value| {
            client
                .post(format!("{admin}/{operation}"))
                .header("x-jazz-backend-secret", secret)
                .json(&body)
                .send()
        };
        let resolve = post(
            "resolve",
            core.backend_secret(),
            serde_json::json!({ "identity": identity }),
        )
        .await
        .unwrap();
        assert_eq!(resolve.status(), StatusCode::NOT_FOUND);
        let registered = post(
            "login-or-register",
            core.backend_secret(),
            serde_json::json!({ "identity": identity }),
        )
        .await
        .unwrap();
        assert_eq!(registered.status(), StatusCode::OK);
        let account = registered.json::<AccountResponse>().await.unwrap().account;
        let resolved = post(
            "resolve",
            core.backend_secret(),
            serde_json::json!({ "identity": identity }),
        )
        .await
        .unwrap();
        assert_eq!(
            resolved.json::<AccountResponse>().await.unwrap().account,
            account
        );
        let wrong = post(
            "login-or-register",
            "not-the-secret",
            serde_json::json!({ "identity": identity }),
        )
        .await
        .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
        let link = post(
            "link",
            core.backend_secret(),
            serde_json::json!({ "from": identity, "to": other }),
        )
        .await
        .unwrap();
        assert_eq!(link.status(), StatusCode::UNAUTHORIZED);
        let revoke = post(
            "revoke",
            core.backend_secret(),
            serde_json::json!({ "approver": identity, "target": identity }),
        )
        .await
        .unwrap();
        assert_eq!(revoke.status(), StatusCode::UNAUTHORIZED);
        core.shutdown().await;
    }

    #[tokio::test]
    async fn admin_vouched_login_or_register_and_link_share_one_account() {
        let core = JazzServer::start().await.expect("start test server");
        let client = reqwest::Client::new();
        let admin = format!("{}/apps/{}/admin/accounts", core.base_url(), core.app_id());
        let identity =
            |subject: &str| serde_json::json!({ "issuer": "jazz:app:test", "subject": subject });
        let denied = client
            .post(format!("{admin}/login-or-register"))
            .json(&serde_json::json!({ "identity": identity("guest") }))
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let guest = client
            .post(format!("{admin}/login-or-register"))
            .header("x-jazz-admin-secret", core.admin_secret())
            .json(&serde_json::json!({ "identity": identity("guest") }))
            .send()
            .await
            .unwrap();
        assert_eq!(guest.status(), StatusCode::OK);
        let guest = guest.json::<AccountResponse>().await.unwrap().account;
        let linked = client
            .post(format!("{admin}/link"))
            .header("x-jazz-admin-secret", core.admin_secret())
            .json(&serde_json::json!({ "from": identity("guest"), "to": identity("alice") }))
            .send()
            .await
            .unwrap();
        assert_eq!(linked.status(), StatusCode::OK);
        assert_eq!(
            linked.json::<AccountResponse>().await.unwrap().account,
            guest
        );
        let alice = client
            .post(format!("{admin}/login-or-register"))
            .header("x-jazz-admin-secret", core.admin_secret())
            .json(&serde_json::json!({ "identity": identity("alice") }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            alice.json::<AccountResponse>().await.unwrap().account,
            guest
        );
        let link = |from: &str, to: &str| {
            client
                .post(format!("{admin}/link"))
                .header("x-jazz-admin-secret", core.admin_secret())
                .json(&serde_json::json!({ "from": identity(from), "to": identity(to) }))
                .send()
        };
        // Linking never creates `from`.
        assert_eq!(
            link("nobody", "carol").await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        // `to` already has an account (bob's own): no merge.
        let bob = client
            .post(format!("{admin}/login-or-register"))
            .header("x-jazz-admin-secret", core.admin_secret())
            .json(&serde_json::json!({ "identity": identity("bob") }))
            .send()
            .await
            .unwrap();
        assert_eq!(bob.status(), StatusCode::OK);
        assert_eq!(
            link("guest", "bob").await.unwrap().status(),
            StatusCode::CONFLICT
        );
        core.shutdown().await;
    }

    /// A guest linked on the backend's word: the backend founds the guest
    /// and links it with the nonce of the challenge the guest signed, and a
    /// second link with that nonce is refused, whoever it names.
    #[tokio::test]
    async fn admin_link_nonce_is_single_use() {
        let core = JazzServer::start().await.expect("start test server");
        let client = reqwest::Client::new();
        let admin = format!("{}/apps/{}/admin/accounts", core.base_url(), core.app_id());
        let user =
            |subject: &str| serde_json::json!({ "issuer": "jazz:app:test", "subject": subject });
        let guest = serde_json::json!({
            "issuer": jazz::tools::identity::DEVICE_ACCOUNT_ISSUER,
            "subject": "6f9619ff-8b86-5011-b42d-00c04fc964ff",
        });
        let post = |path: &str, body: serde_json::Value, secret: Option<&str>| {
            let mut request = client.post(format!("{admin}/{path}")).json(&body);
            if let Some(secret) = secret {
                request = request.header("x-jazz-admin-secret", secret);
            }
            request.send()
        };
        let secret = Some(core.admin_secret());

        // Founding needs the admin secret, and only founds device accounts.
        let denied = post(
            "found-local-first",
            serde_json::json!({ "identity": guest }),
            None,
        );
        assert_eq!(denied.await.unwrap().status(), StatusCode::UNAUTHORIZED);
        let external = post(
            "found-local-first",
            serde_json::json!({ "identity": user("alice") }),
            secret,
        );
        assert_eq!(external.await.unwrap().status(), StatusCode::BAD_REQUEST);
        let founded = post(
            "found-local-first",
            serde_json::json!({ "identity": guest }),
            secret,
        )
        .await
        .unwrap();
        assert_eq!(founded.status(), StatusCode::OK);
        let guest_account = founded.json::<AccountResponse>().await.unwrap().account;
        // Founding is deterministic and repeatable.
        let again = post(
            "found-local-first",
            serde_json::json!({ "identity": guest }),
            secret,
        )
        .await
        .unwrap();
        assert_eq!(
            again.json::<AccountResponse>().await.unwrap().account,
            guest_account
        );

        let nonce = Uuid::new_v4();
        let linked = post(
            "link",
            serde_json::json!({ "from": guest, "to": user("alice"), "nonce": nonce }),
            secret,
        )
        .await
        .unwrap();
        assert_eq!(linked.status(), StatusCode::OK);
        assert_eq!(
            linked.json::<AccountResponse>().await.unwrap().account,
            guest_account
        );
        // The same nonce again, for another user: refused, nothing linked.
        let replayed = post(
            "link",
            serde_json::json!({ "from": guest, "to": user("mallory"), "nonce": nonce }),
            secret,
        )
        .await
        .unwrap();
        assert_eq!(replayed.status(), StatusCode::BAD_REQUEST);
        let mallory = post(
            "resolve",
            serde_json::json!({ "identity": user("mallory") }),
            secret,
        )
        .await
        .unwrap();
        assert_eq!(mallory.status(), StatusCode::NOT_FOUND);

        // The backend may retire the guest's key once it linked: the user
        // signs in, the guest no longer does, and founding can't revive it.
        let unvouched = post(
            "revoke",
            serde_json::json!({ "approver": user("alice"), "target": guest }),
            None,
        );
        assert_eq!(unvouched.await.unwrap().status(), StatusCode::UNAUTHORIZED);
        let revoked = post(
            "revoke",
            serde_json::json!({ "approver": user("alice"), "target": guest }),
            secret,
        )
        .await
        .unwrap();
        assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
        let guest_login = post("resolve", serde_json::json!({ "identity": guest }), secret)
            .await
            .unwrap();
        assert_eq!(guest_login.status(), StatusCode::FORBIDDEN);
        let refounded = post(
            "found-local-first",
            serde_json::json!({ "identity": guest }),
            secret,
        )
        .await
        .unwrap();
        assert_eq!(refounded.status(), StatusCode::FORBIDDEN);
        let alice = post(
            "resolve",
            serde_json::json!({ "identity": user("alice") }),
            secret,
        )
        .await
        .unwrap();
        assert_eq!(alice.status(), StatusCode::OK);
        core.shutdown().await;
    }

    /// A link nonce stays used across a core restart.
    #[tokio::test]
    async fn admin_link_nonce_stays_used_after_a_restart() {
        let dir = tempfile::tempdir().expect("data dir");
        let app_id = jazz::tools::AppId::random();
        let start = || {
            JazzServer::builder()
                .with_app_id(app_id)
                .with_data_dir(dir.path().to_path_buf())
                .with_persistent_storage()
                .start()
        };
        let user =
            |subject: &str| serde_json::json!({ "issuer": "jazz:app:test", "subject": subject });
        let guest = serde_json::json!({
            "issuer": jazz::tools::identity::DEVICE_ACCOUNT_ISSUER,
            "subject": "6f9619ff-8b86-5011-b42d-00c04fc964ff",
        });
        let nonce = Uuid::new_v4();
        let client = reqwest::Client::new();
        let post = |core: &JazzServer, path: &str, body: serde_json::Value| {
            client
                .post(format!(
                    "{}/apps/{}/admin/accounts/{path}",
                    core.base_url(),
                    core.app_id()
                ))
                .header("x-jazz-admin-secret", core.admin_secret())
                .json(&body)
                .send()
        };

        let core = start().await.expect("start test server");
        let founded = post(
            &core,
            "found-local-first",
            serde_json::json!({ "identity": guest }),
        );
        assert_eq!(founded.await.unwrap().status(), StatusCode::OK);
        let linked = post(
            &core,
            "link",
            serde_json::json!({ "from": guest, "to": user("alice"), "nonce": nonce }),
        );
        assert_eq!(linked.await.unwrap().status(), StatusCode::OK);
        core.shutdown().await;

        let core = start().await.expect("restart test server");
        let replayed = post(
            &core,
            "link",
            serde_json::json!({ "from": guest, "to": user("mallory"), "nonce": nonce }),
        )
        .await
        .unwrap();
        assert_eq!(replayed.status(), StatusCode::BAD_REQUEST);
        let mallory = post(
            &core,
            "resolve",
            serde_json::json!({ "identity": user("mallory") }),
        );
        assert_eq!(mallory.await.unwrap().status(), StatusCode::NOT_FOUND);
        core.shutdown().await;
    }
}
