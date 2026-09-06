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

#[derive(Serialize)]
pub(super) struct AccountResponse {
    account: Uuid,
    identity: Principal,
    generation: u64,
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

    /// Alice registers, approves Bob, and Bob accepts using ordinary JWTs.
    /// Mallory cannot accept Bob's nonce. Revocation survives nonce replay.
    /// alice -> request(bob) -> bob accepts -> alice revokes -> replay denied
    #[tokio::test]
    async fn ordinary_jwts_link_only_the_target_and_replay_cannot_undo_revocation() {
        let server = JazzServer::start().await;
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
