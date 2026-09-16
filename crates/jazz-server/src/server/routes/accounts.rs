//! Account control endpoints: JWTs authenticate requests, not custom intent claims.
use crate::server::ServerState;
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use jazz::account_registry::storage::RegistryError;
use jazz::account_registry::{
    AccountCommand, AccountCommandResult, AccountId, Assignment, Principal,
};
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

/// Edges preserve end-user bearer proof; they never substitute service authority.
pub(super) async fn forward_if_edge(
    State(state): State<Arc<ServerState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if state.accounts.is_some() {
        return next.run(request).await;
    }
    match forward_account_request(&state, request).await {
        Ok(response) => response,
        Err(failure) => failure.into_response(),
    }
}

async fn forward_account_request(
    state: &ServerState,
    request: axum::extract::Request,
) -> Result<axum::response::Response, Failure> {
    let base = state.upstream_http_url.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "account_registry_unavailable",
    ))?;
    let authorization = request
        .headers()
        .get("authorization")
        .filter(|_| !request.headers().contains_key("x-jazz-session"))
        .cloned()
        .ok_or((StatusCode::UNAUTHORIZED, "account_bearer_required"))?;
    let path = request.uri().path();
    let operation = [
        "links/request",
        "links/accept",
        "found-local-first",
        "register",
        "login-or-register",
        "login",
        "revoke",
    ]
    .into_iter()
    .find(|operation| path.ends_with(&format!("/{operation}")))
    .ok_or((StatusCode::NOT_FOUND, "unknown_account_operation"))?;
    let url = format!(
        "{}/apps/{}/accounts/{operation}",
        base.trim_end_matches('/'),
        state.app_id
    );
    let body = axum::body::to_bytes(request.into_body(), 64 * 1024)
        .await
        .map_err(|_| (StatusCode::PAYLOAD_TOO_LARGE, "account_request_too_large"))?;
    let mut upstream = state
        .http_client
        .post(url)
        .header("authorization", authorization)
        .header("content-type", "application/json")
        .timeout(std::time::Duration::from_secs(10))
        .body(body)
        .send()
        .await
        .map_err(|_| (StatusCode::BAD_GATEWAY, "account_registry_unavailable"))?;
    let status = upstream.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = upstream
        .chunk()
        .await
        .map_err(|_| (StatusCode::BAD_GATEWAY, "account_registry_unavailable"))?
    {
        if bytes.len().saturating_add(chunk.len()) > 64 * 1024 {
            return Err((StatusCode::BAD_GATEWAY, "invalid_account_response"));
        }
        bytes.extend_from_slice(&chunk);
    }
    axum::response::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(bytes))
        .map_err(|_| (StatusCode::BAD_GATEWAY, "invalid_account_response"))
}

async fn authenticate(state: &ServerState, headers: &HeaderMap) -> Result<Principal, Failure> {
    // Explicit bearer auth only: ambient cookies cannot authorize an account
    // mutation, and backend impersonation is not proof of the user's intent.
    if !headers.contains_key("authorization") || headers.contains_key("x-jazz-session") {
        return Err((
            StatusCode::UNAUTHORIZED,
            "account operation requires bearer authentication",
        ));
    }
    let session = crate::middleware::auth::extract_session(
        headers,
        state.app_id,
        &state.auth_config,
        state.jwt_verifier.as_deref(),
    )
    .await
    .map_err(|_| (StatusCode::UNAUTHORIZED, "invalid account credential"))?
    .ok_or((StatusCode::UNAUTHORIZED, "missing account credential"))?;
    if session.issuer == jazz::tools::identity::ANONYMOUS_ISSUER {
        return Err((
            StatusCode::UNAUTHORIZED,
            "read-only anonymous identities cannot own accounts",
        ));
    }
    Ok(Principal {
        issuer: session.issuer,
        subject: session.user_id,
    })
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AccountResponse {
    account: Uuid,
    identity: Principal,
    generation: u64,
}

/// Read-only service lookup. An edge must authenticate the user independently;
/// service authority can resolve an assignment but cannot create or link one.
pub(super) async fn resolve_for_edge(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<RequestLink>,
) -> Result<Json<AccountResponse>, Failure> {
    crate::middleware::auth::validate_admin_secret(
        headers
            .get("x-jazz-admin-secret")
            .and_then(|value| value.to_str().ok()),
        &state.auth_config,
    )?;
    let assignment = owner(&state)?
        .login(request.identity.clone())
        .await
        .map_err(failure)?;
    Ok(response(request.identity, assignment))
}

/// Authentication denial and temporary registry availability are different wire outcomes.
#[derive(Debug)]
pub(super) enum AdmissionError {
    Denied(String),
    Unavailable,
}
impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Denied(message) => f.write_str(message),
            Self::Unavailable => f.write_str("account registry unavailable"),
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
        }
    }
}

async fn read_upstream_assignment(
    request: reqwest::RequestBuilder,
    principal: &Principal,
) -> Result<AccountId, AdmissionError> {
    let mut response = request
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|_| AdmissionError::Unavailable)?;
    if response.status().is_server_error()
        || response.status() == StatusCode::TOO_MANY_REQUESTS
        || response.status() == StatusCode::REQUEST_TIMEOUT
    {
        return Err(AdmissionError::Unavailable);
    }
    if !response.status().is_success() {
        return Err("account identity not admitted by core".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| AdmissionError::Unavailable)?
    {
        if bytes.len().saturating_add(chunk.len()) > 64 * 1024 {
            return Err("invalid account registry response".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let assignment: AccountResponse =
        serde_json::from_slice(&bytes).map_err(|_| "invalid account registry response")?;
    if assignment.identity != *principal {
        return Err("account registry returned another identity".into());
    }
    Ok(AccountId(assignment.account))
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
    let base = state
        .upstream_http_url
        .as_deref()
        .ok_or(AdmissionError::Unavailable)?;
    let secret = state
        .auth_config
        .admin_secret
        .as_deref()
        .ok_or("edge registry authority unavailable")?;
    let request = state
        .http_client
        .post(format!(
            "{}/apps/{}/admin/accounts/resolve",
            base.trim_end_matches('/'),
            state.app_id,
        ))
        .header("x-jazz-admin-secret", secret)
        .json(&serde_json::json!({ "identity": principal }));
    read_upstream_assignment(request, &principal).await
}

/// Offline local founding becomes durable on the first authenticated connection.
/// An edge forwards the actual founder's proof, never its own service secret.
pub(super) async fn admit_local_founder(
    state: &ServerState,
    principal: &Principal,
    headers: &HeaderMap,
) -> Result<(), AdmissionError> {
    if let Some(registry) = &state.accounts {
        registry
            .execute(AccountCommand::FoundLocalFirst {
                principal: principal.clone(),
                app: *state.app_id.uuid(),
            })
            .await
            .map_err(AdmissionError::from)?;
        return Ok(());
    }
    let base = state
        .upstream_http_url
        .as_deref()
        .ok_or(AdmissionError::Unavailable)?;
    let proof = headers
        .get("authorization")
        .ok_or("local founding requires bearer proof")?;
    if headers.contains_key("x-jazz-session") {
        return Err("local founding requires the founder's own proof".into());
    }
    let request = state
        .http_client
        .post(format!(
            "{}/apps/{}/accounts/found-local-first",
            base.trim_end_matches('/'),
            state.app_id,
        ))
        .header("authorization", proof);
    read_upstream_assignment(request, principal).await?;
    Ok(())
}
fn response(identity: Principal, assignment: Assignment) -> Json<AccountResponse> {
    Json(AccountResponse {
        account: assignment.account.0,
        identity,
        generation: assignment.generation,
    })
}

pub(super) async fn register(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<Json<AccountResponse>, Failure> {
    let principal = authenticate(&state, &headers).await?;
    if principal.issuer == jazz::tools::identity::LOCAL_FIRST_ISSUER {
        return Err((StatusCode::BAD_REQUEST, "use_local_first_founding"));
    }
    let result = owner(&state)?
        .execute(AccountCommand::Register {
            principal: principal.clone(),
            account: AccountId(Uuid::new_v4()),
        })
        .await
        .map_err(failure)?;
    let AccountCommandResult::Assignment(assignment) = result else {
        unreachable!("register result")
    };
    Ok(response(principal, assignment))
}

/// Resolve or create an external account in one ordered core decision.
pub(super) async fn login_or_register(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<Json<AccountResponse>, Failure> {
    let principal = authenticate(&state, &headers).await?;
    if principal.issuer == jazz::tools::identity::LOCAL_FIRST_ISSUER {
        return Err((StatusCode::BAD_REQUEST, "use_local_first_founding"));
    }
    let assignment = owner(&state)?
        .login_or_register(principal.clone())
        .await
        .map_err(failure)?;
    Ok(response(principal, assignment))
}

pub(super) async fn login(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<Json<AccountResponse>, Failure> {
    let principal = authenticate(&state, &headers).await?;
    let assignment = owner(&state)?
        .login(principal.clone())
        .await
        .map_err(failure)?;
    Ok(response(principal, assignment))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RequestLink {
    identity: Principal,
}
#[derive(Serialize)]
pub(super) struct LinkResponse {
    nonce: Uuid,
    expires_at: u64,
}

pub(super) async fn request_link(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<RequestLink>,
) -> Result<Json<LinkResponse>, Failure> {
    let approver = authenticate(&state, &headers).await?;
    jazz::ids::AuthorSubject::authenticated(&request.identity.issuer, &request.identity.subject)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid_external_identity"))?;
    let now = state.auth_config.clock.now_seconds();
    let expires_at = now
        .checked_add(300)
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "invalid_clock"))?;
    let nonce = Uuid::new_v4();
    owner(&state)?
        .execute(AccountCommand::RequestLink {
            approver,
            candidate: request.identity,
            nonce,
            now,
            expires_at,
        })
        .await
        .map_err(failure)?;
    Ok(Json(LinkResponse { nonce, expires_at }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AcceptLink {
    nonce: Uuid,
}

pub(super) async fn accept_link(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<AcceptLink>,
) -> Result<Json<AccountResponse>, Failure> {
    let candidate = authenticate(&state, &headers).await?;
    let result = owner(&state)?
        .execute(AccountCommand::AcceptLink {
            candidate: candidate.clone(),
            nonce: request.nonce,
            now: state.auth_config.clock.now_seconds(),
        })
        .await
        .map_err(failure)?;
    let AccountCommandResult::Assignment(assignment) = result else {
        unreachable!("accept result")
    };
    Ok(response(candidate, assignment))
}

/// Register the deterministic binding of a verified local-first founder.
/// The client does not choose an account ID and cannot claim another founder.
pub(super) async fn found_local_first(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<Json<AccountResponse>, Failure> {
    let principal = authenticate(&state, &headers).await?;
    if principal.issuer != jazz::tools::identity::LOCAL_FIRST_ISSUER {
        return Err((StatusCode::BAD_REQUEST, "local_first_proof_required"));
    }
    let result = owner(&state)?
        .execute(AccountCommand::FoundLocalFirst {
            principal: principal.clone(),
            app: *state.app_id.uuid(),
        })
        .await
        .map_err(failure)?;
    let AccountCommandResult::Assignment(assignment) = result else {
        unreachable!("founding result")
    };
    Ok(response(principal, assignment))
}

pub(super) async fn revoke(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<RequestLink>,
) -> Result<StatusCode, Failure> {
    let approver = authenticate(&state, &headers).await?;
    owner(&state)?
        .execute(AccountCommand::Revoke {
            approver,
            target: request.identity,
        })
        .await
        .map_err(failure)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(all(test, feature = "embedded-server"))]
mod tests {
    use super::*;
    use crate::server::testing::{JazzServer, TestJwtIssuer};

    /// Concurrent first-device requests and a lost-response retry must resolve
    /// one immutable assignment, including when routed through an edge.
    #[tokio::test]
    async fn login_or_register_is_atomic_and_revocation_is_permanent() {
        let core = JazzServer::start().await.expect("start test server");
        let edge = JazzServer::builder()
            .with_app_id(core.app_id())
            .with_upstream_url(core.base_url())
            .start()
            .await
            .expect("start test server");
        let client = reqwest::Client::new();
        let base = format!("{}/apps/{}/accounts", edge.base_url(), edge.app_id());
        let token = TestJwtIssuer::jwt_for_user("new-account");
        let url = format!("{base}/login-or-register");
        let mut requests = Vec::new();
        for _ in 0..16 {
            let client = client.clone();
            let token = token.clone();
            let url = url.clone();
            requests.push(tokio::spawn(async move {
                let response = client.post(url).bearer_auth(token).send().await.unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                response.json::<AccountResponse>().await.unwrap().account
            }));
        }
        let mut account = None;
        for request in requests {
            let resolved = request.await.unwrap();
            assert_eq!(*account.get_or_insert(resolved), resolved);
        }
        // The client may have lost any preceding response: no client-generated
        // request ID is necessary to recover the immutable identity assignment.
        let retry = client.post(&url).bearer_auth(&token).send().await.unwrap();
        assert_eq!(retry.status(), StatusCode::OK);
        assert_eq!(
            retry.json::<AccountResponse>().await.unwrap().account,
            account.unwrap()
        );
        let strict = client
            .post(format!("{base}/register"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(strict.status(), StatusCode::CONFLICT);
        let invalid = client
            .post(&url)
            .bearer_auth("invalid-token")
            .send()
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
        let revoked = client.post(format!("{base}/revoke")).bearer_auth(&token)
            .json(&serde_json::json!({"identity": {"issuer": "urn:jazz:test", "subject": "new-account"}}))
            .send().await.unwrap();
        assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
        for operation in ["login-or-register", "login"] {
            let denied = client
                .post(format!("{base}/{operation}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap();
            assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        }
        edge.shutdown().await;
        core.shutdown().await;
    }

    /// Exercise public HTTP enrollment through an actual edge/core topology.
    /// Service lookup is deliberately read-only and rejects user credentials.
    #[tokio::test]
    async fn edge_forwards_identity_proof_and_core_resolves_revocation() {
        let core = JazzServer::start().await.expect("start test server");
        let edge = JazzServer::builder()
            .with_app_id(core.app_id())
            .with_upstream_url(core.base_url())
            .start()
            .await
            .expect("start test server");
        let client = reqwest::Client::new();
        let base = format!("{}/apps/{}/accounts", edge.base_url(), edge.app_id());
        let alice = TestJwtIssuer::jwt_for_user("alice");
        let bob = TestJwtIssuer::jwt_for_user("bob");
        let identity = serde_json::json!({"issuer": "urn:jazz:test", "subject": "bob"});
        let resolve = format!(
            "{}/apps/{}/admin/accounts/resolve",
            core.base_url(),
            core.app_id()
        );
        let denied = client
            .post(&resolve)
            .bearer_auth(&alice)
            .json(&serde_json::json!({"identity": identity}))
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let registered = client
            .post(format!("{base}/register"))
            .bearer_auth(&alice)
            .send()
            .await
            .unwrap();
        assert_eq!(registered.status(), StatusCode::OK);
        let account: AccountResponse = registered.json().await.unwrap();
        let requested = client
            .post(format!("{base}/links/request"))
            .bearer_auth(&alice)
            .json(&serde_json::json!({"identity": identity}))
            .send()
            .await
            .unwrap();
        assert_eq!(requested.status(), StatusCode::OK);
        let intent: serde_json::Value = requested.json().await.unwrap();
        let accepted = client
            .post(format!("{base}/links/accept"))
            .bearer_auth(&bob)
            .json(&serde_json::json!({"nonce": intent["nonce"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let linked: AccountResponse = accepted.json().await.unwrap();
        assert_eq!(linked.account, account.account);
        let active = client
            .post(&resolve)
            .header("x-jazz-admin-secret", core.admin_secret())
            .json(&serde_json::json!({"identity": identity}))
            .send()
            .await
            .unwrap();
        assert_eq!(active.status(), StatusCode::OK);
        let resolved: AccountResponse = active.json().await.unwrap();
        assert_eq!(resolved.account, account.account);
        let revoked = client
            .post(format!("{base}/revoke"))
            .bearer_auth(&alice)
            .json(&serde_json::json!({"identity": identity}))
            .send()
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
        let inactive = client
            .post(&resolve)
            .header("x-jazz-admin-secret", core.admin_secret())
            .json(&serde_json::json!({"identity": identity}))
            .send()
            .await
            .unwrap();
        assert_eq!(inactive.status(), StatusCode::FORBIDDEN);
        let login = client
            .post(format!("{base}/login"))
            .bearer_auth(&bob)
            .send()
            .await
            .unwrap();
        assert_eq!(login.status(), StatusCode::FORBIDDEN);
        edge.shutdown().await;
        core.shutdown().await;
    }

    /// Alice registers, approves Bob, and Bob accepts using ordinary JWTs.
    /// Mallory cannot accept Bob's nonce. Revocation survives nonce replay.
    /// alice -> request(bob) -> bob accepts -> alice revokes -> replay denied
    #[tokio::test]
    async fn ordinary_jwts_link_only_the_target_and_replay_cannot_undo_revocation() {
        let server = JazzServer::start().await.expect("start test server");
        let client = reqwest::Client::new();
        let base = format!("{}/apps/{}/accounts", server.base_url(), server.app_id());
        let alice = TestJwtIssuer::jwt_for_user("alice");
        let bob = TestJwtIssuer::jwt_for_user("bob");
        let mallory = TestJwtIssuer::jwt_for_user("mallory");
        let unassigned = client
            .post(format!("{base}/login"))
            .bearer_auth(&bob)
            .send()
            .await
            .unwrap();
        assert_eq!(unassigned.status(), StatusCode::NOT_FOUND);
        let registered = client
            .post(format!("{base}/register"))
            .bearer_auth(&alice)
            .send()
            .await
            .unwrap();
        assert_eq!(registered.status(), StatusCode::OK);
        let account: serde_json::Value = registered.json().await.unwrap();
        let identity = serde_json::json!({ "issuer": "urn:jazz:test", "subject": "bob" });
        let requested = client
            .post(format!("{base}/links/request"))
            .bearer_auth(&alice)
            .json(&serde_json::json!({ "identity": identity }))
            .send()
            .await
            .unwrap();
        assert_eq!(requested.status(), StatusCode::OK);
        let intent: serde_json::Value = requested.json().await.unwrap();
        let accept_body = serde_json::json!({ "nonce": intent["nonce"] });
        let stolen = client
            .post(format!("{base}/links/accept"))
            .bearer_auth(&mallory)
            .json(&accept_body)
            .send()
            .await
            .unwrap();
        assert_eq!(stolen.status(), StatusCode::BAD_REQUEST);
        let accepted = client
            .post(format!("{base}/links/accept"))
            .bearer_auth(&bob)
            .json(&accept_body)
            .send()
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let linked: serde_json::Value = accepted.json().await.unwrap();
        assert_eq!(linked["account"], account["account"]);
        let revoked = client
            .post(format!("{base}/revoke"))
            .bearer_auth(&alice)
            .json(&serde_json::json!({ "identity": identity }))
            .send()
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
        let replay = client
            .post(format!("{base}/links/accept"))
            .bearer_auth(&bob)
            .json(&accept_body)
            .send()
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::FORBIDDEN);
        let reregister = client
            .post(format!("{base}/register"))
            .bearer_auth(&bob)
            .send()
            .await
            .unwrap();
        assert_eq!(reregister.status(), StatusCode::CONFLICT);
        server.shutdown().await;
    }
}
