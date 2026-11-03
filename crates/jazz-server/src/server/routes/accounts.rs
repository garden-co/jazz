//! Account control endpoints, for the app's backend: it vouches for the
//! identities it names with its admin secret.
use crate::server::ServerState;
use crate::server::account_store::RegistryError;
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use jazz::account_registry::{
    AccountCommand, AccountCommandResult, AccountId, Assignment, Principal,
};
use jazz::tools::transport_error::{UnauthenticatedCode, UnauthenticatedResponse};
use serde::{Deserialize, Serialize};
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
fn failure(error: RegistryError) -> Failure {
    use jazz::account_registry::AccountError;
    match error {
        RegistryError::Decision(AccountError::NotAssigned) => {
            (StatusCode::NOT_FOUND, "identity_not_assigned")
        }
        RegistryError::Decision(AccountError::AlreadyAssigned) => {
            (StatusCode::CONFLICT, "identity_already_assigned")
        }
        RegistryError::Decision(AccountError::NotAuthorized) => {
            (StatusCode::FORBIDDEN, "identity_not_authorized")
        }
        RegistryError::Decision(_) => (StatusCode::BAD_REQUEST, "invalid_link_intent"),
        RegistryError::Unavailable(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "account_registry_unavailable",
        ),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IdentityRequest {
    identity: Principal,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AccountResponse {
    account: Uuid,
    identity: Principal,
    generation: u64,
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
    jazz::ids::AuthorSubject::authenticated(&request.identity.issuer, &request.identity.subject)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid_external_identity"))?;
    let assignment = owner(&state)?
        .login_or_register(request.identity.clone())
        .await
        .map_err(failure)?;
    Ok(response(request.identity, assignment))
}

/// The app's backend vouches for a local-first guest: it checked the
/// guest's proof of its key itself (`createJazz` links a guest only on a
/// fresh, single-use challenge the guest signed). The core founds the
/// guest's account deterministically, as `found-local-first` would for the
/// guest's own token.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdminFoundLocalFirst {
    identity: Principal,
    /// The namespace the guest's account was founded in, when it is not
    /// this app: the endpoint an app-less client's proofs are for.
    #[serde(default)]
    audience: Option<String>,
}

pub(super) async fn found_local_first_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<AdminFoundLocalFirst>,
) -> Result<Json<AccountResponse>, Failure> {
    validate_vouching_backend(&headers, &state)?;
    if request.identity.issuer != jazz::tools::identity::LOCAL_FIRST_ISSUER {
        return Err((StatusCode::BAD_REQUEST, "local_first_identity_required"));
    }
    let result = owner(&state)?
        .execute(AccountCommand::FoundLocalFirst {
            principal: request.identity.clone(),
            app: match request.audience.as_deref().map(str::trim) {
                Some("") => return Err((StatusCode::BAD_REQUEST, "invalid_local_first_audience")),
                Some(audience) => {
                    *crate::middleware::auth::local_first_founding_app(audience).uuid()
                }
                None => *state.app_id.uuid(),
            },
        })
        .await
        .map_err(failure)?;
    let AccountCommandResult::Assignment(assignment) = result else {
        unreachable!("founding result")
    };
    Ok(response(request.identity, assignment))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdminLink {
    from: Principal,
    to: Principal,
    /// The link's single-use intent id, when the backend has one (the nonce
    /// of the challenge a guest signed): a second link with the same nonce
    /// is refused, whoever it names.
    #[serde(default)]
    nonce: Option<Uuid>,
}

/// The app's backend vouches for both identities: link `to` to the account
/// `from` uses, in the same request-and-accept steps the two users' own
/// credentials would take.
pub(super) async fn link_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<AdminLink>,
) -> Result<Json<AccountResponse>, Failure> {
    crate::middleware::auth::validate_admin_secret(
        super::http::admin_secret_header(&headers),
        &state.auth_config,
    )?;
    jazz::ids::AuthorSubject::authenticated(&request.to.issuer, &request.to.subject)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid_external_identity"))?;
    let registry = owner(&state)?;
    // `from` names an account that already exists (a user the backend signed
    // in, or a local-first guest founded from its own key proof): linking
    // never creates it.
    registry
        .login(request.from.clone())
        .await
        .map_err(failure)?;
    let now = state.auth_config.clock.now_seconds();
    let expires_at = now
        .checked_add(300)
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "invalid_clock"))?;
    let nonce = request.nonce.unwrap_or_else(Uuid::new_v4);
    registry
        .execute(AccountCommand::RequestLink {
            approver: request.from,
            candidate: request.to.clone(),
            nonce,
            now,
            expires_at,
        })
        .await
        .map_err(failure)?;
    let result = registry
        .execute(AccountCommand::AcceptLink {
            candidate: request.to.clone(),
            nonce,
            now,
        })
        .await
        .map_err(failure)?;
    let AccountCommandResult::Assignment(assignment) = result else {
        unreachable!("accept result")
    };
    Ok(response(request.to, assignment))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdminRevoke {
    approver: Principal,
    target: Principal,
}

/// The app's backend vouches for `approver`, an identity of the account, and
/// revokes `target`, another of its identities, as the approver's own
/// credential would (`createJazz` revokes a local-first guest's key once it
/// links to a signed-in user, when the app asks for that). The assignment
/// stays: the target can no longer sign in, and is never reassigned.
pub(super) async fn revoke_for_admin(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<AdminRevoke>,
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

/// Authentication denial and temporary registry availability are different wire outcomes.
#[derive(Debug)]
pub(super) enum AdmissionError {
    Denied(String),
    Unavailable,
    AuthenticationUnavailable(String),
}
impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Denied(message) => f.write_str(message),
            Self::Unavailable => f.write_str("account registry unavailable"),
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
            _ => Self::Denied(
                serde_json::to_string(&error)
                    .unwrap_or_else(|_| "authentication failed".to_owned()),
            ),
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
    pub(super) fn into_wire(self) -> jazz::wire::WireError {
        use jazz::wire::{WireError, WireErrorCode, WireRetry};
        match self {
            Self::Unavailable => {
                WireError::new(WireErrorCode::NotReady, WireRetry::Later, self.to_string())
            }
            Self::Denied(message) => {
                WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, message)
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

/// Offline local founding becomes durable on the first authenticated connection.
/// The namespace a local-first founder's account id derives from: the
/// audience its proof was verified against (see
/// [`crate::middleware::auth::LOCAL_FIRST_AUDIENCE_HEADER`]).
fn founding_app(state: &ServerState, headers: &HeaderMap) -> Result<Uuid, UnauthenticatedResponse> {
    let audience =
        crate::middleware::auth::local_first_audience(headers, state.app_id, &state.auth_config)?;
    Ok(*crate::middleware::auth::local_first_founding_app(&audience).uuid())
}

pub(super) async fn admit_local_founder(
    state: &ServerState,
    principal: &Principal,
    headers: &HeaderMap,
) -> Result<(), AdmissionError> {
    let app = founding_app(state, headers)?;
    if let Some(registry) = &state.accounts {
        registry
            .execute(AccountCommand::FoundLocalFirst {
                principal: principal.clone(),
                app,
            })
            .await
            .map_err(AdmissionError::from)?;
        return Ok(());
    }
    Err(AdmissionError::Unavailable)
}
fn response(identity: Principal, assignment: Assignment) -> Json<AccountResponse> {
    Json(AccountResponse {
        account: assignment.account.0,
        identity,
        generation: assignment.generation,
    })
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
            "issuer": jazz::tools::identity::LOCAL_FIRST_ISSUER,
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

        // Founding needs the admin secret, and only founds local-first guests.
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
            "issuer": jazz::tools::identity::LOCAL_FIRST_ISSUER,
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
