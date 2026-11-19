//! What a backend does before it connects: publish the app's permissions
//! bundle and resolve the account of the user it acts as.

use std::time::Duration;

use jazz::account_registry::AccountId;
use jazz::tools::AppId;
use jazz::tools::deployment::artifact::LoadedApp;
use serde::Deserialize;
use serde_json::json;

use crate::config::Backend;
use crate::error::{Error, Result};

const TIMEOUT: Duration = Duration::from_secs(30);

fn http() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(TIMEOUT)
        .build()
        .map_err(|error| Error::Server(error.to_string()))
}

fn app_url(server: &str, app_id: AppId, path: &str) -> String {
    format!("{}/apps/{app_id}/{path}", server.trim_end_matches('/'))
}

async fn refusal(what: &str, response: reqwest::Response) -> Error {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let detail = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|body| body.get("error").and_then(|e| e.as_str()).map(String::from))
        .unwrap_or(body);
    Error::Server(format!("{what} failed ({status}): {detail}"))
}

/// Publishes the app's schema versions, migrations and permissions as a
/// bundle, as `createJazz` does on start: nothing becomes current, and
/// publishing a bundle already published changes nothing. Returns its id.
pub(crate) async fn publish(
    server: &str,
    app_id: AppId,
    backend: &Backend,
    app: &LoadedApp,
) -> Result<String> {
    let mut body = json!({
        "targetSchemaHash": app.current_hash().to_string(),
        "schemas": app
            .schemas()
            .iter()
            .map(|(hash, schema)| json!({ "hash": hash.to_string(), "schema": schema }))
            .collect::<Vec<_>>(),
        "migrations": app.migrations(),
        "permissions": app.permissions(),
        "allowDroppingHistory": backend.allow_dropping_history,
    });
    // An app that declares encryption sends its declarations, and the
    // decrypt conversions its deployer confirms.
    if !app.encryption().is_empty() {
        body["encryption"] = serde_json::to_value(app.encryption())
            .map_err(|error| Error::Server(format!("encoding the app's encryption: {error}")))?;
    }
    if !backend.confirm_decrypt.is_empty() {
        body["confirmDecrypt"] = json!(backend.confirm_decrypt);
    }
    let response = http()?
        .post(app_url(server, app_id, "admin/bundles"))
        .header("X-Jazz-Admin-Secret", &backend.admin_secret)
        .json(&body)
        .send()
        .await
        .map_err(|error| Error::Server(format!("publishing the app's bundle: {error}")))?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
    {
        return Err(Error::Server(
            "the sync server predates per-deployment permissions bundles; upgrade it before this backend"
                .into(),
        ));
    }
    if !status.is_success() {
        return Err(refusal("publishing the app's bundle", response).await);
    }
    #[derive(Deserialize)]
    struct Published {
        bundle: Option<String>,
    }
    let published: Published = response
        .json()
        .await
        .map_err(|error| Error::Server(format!("publishing the app's bundle: {error}")))?;
    published.bundle.ok_or_else(|| {
        Error::Server("the core did not publish a permissions bundle; upgrade it".into())
    })
}

/// The account of the user the backend acts as: the backend vouches for
/// them with its secret, and the core resolves (or creates) their account.
pub(crate) async fn enrol(
    server: &str,
    app_id: AppId,
    backend: &Backend,
    issuer: &str,
) -> Result<AccountId> {
    let response = http()?
        .post(app_url(server, app_id, "admin/accounts/login-or-register"))
        .header("X-Jazz-Backend-Secret", &backend.backend_secret)
        .json(&json!({ "identity": { "issuer": issuer, "subject": backend.user_id } }))
        .send()
        .await
        .map_err(|error| Error::Server(format!("admitting the backend's user: {error}")))?;
    if !response.status().is_success() {
        return Err(refusal("admitting the backend's user", response).await);
    }
    #[derive(Deserialize)]
    struct Admission {
        account: AccountId,
    }
    let admitted: Admission = response
        .json()
        .await
        .map_err(|error| Error::Server(format!("admitting the backend's user: {error}")))?;
    if admitted.account.is_system() {
        return Err(Error::Server(
            "admitting the backend's user returned the system account".into(),
        ));
    }
    Ok(admitted.account)
}

/// A user the app's backend signed in: what `createJazz`'s
/// `backend-sign-in` answers.
#[derive(Debug, Deserialize)]
pub(crate) struct SignedIn {
    pub account: AccountId,
    pub identity: SignedInIdentity,
    #[serde(default)]
    pub claims: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SignedInIdentity {
    pub issuer: String,
    pub subject: String,
}

/// Signs the bearer's user in at the app's Jazz endpoint (`createJazz`'s
/// handler, e.g. `https://app.example/jazz`), as a browser does with its
/// cookies: the backend's `authenticate` names the user, and the core
/// resolves (or creates) their account. `None`: the bearer names no one.
pub(crate) async fn sign_in(
    endpoint: &str,
    app_id: AppId,
    bearer: &str,
) -> Result<Option<SignedIn>> {
    let response = http()?
        .post(app_url(endpoint, app_id, "accounts/backend-sign-in"))
        .bearer_auth(bearer)
        .send()
        .await
        .map_err(|error| Error::Server(format!("signing in: {error}")))?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Ok(None);
    }
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(Error::Server(
            "the server has no backend sign-in: Config::server must be the app's Jazz \
             endpoint (createJazz's handler, e.g. https://app.example/jazz), not the core"
                .into(),
        ));
    }
    if !response.status().is_success() {
        return Err(refusal("signing in", response).await);
    }
    let signed_in: SignedIn = response
        .json()
        .await
        .map_err(|error| Error::Server(format!("signing in: {error}")))?;
    if signed_in.account.is_system() {
        return Err(Error::Server(
            "signing in returned the system account".into(),
        ));
    }
    Ok(Some(signed_in))
}

/// Links the device account `key` to the user the app's backend names from
/// `bearer`, as TypeScript's `linkWithBackend`: the device signs a challenge
/// the backend issued for that user (never its sync token), so a proof links
/// once, and only to them. The backend founds the device's account and links
/// the user to it; it answers as `backend-sign-in` does.
pub(crate) async fn link(
    endpoint: &str,
    app_id: AppId,
    bearer: &str,
    key: &crate::auth::DeviceKey,
) -> Result<SignedIn> {
    let response = http()?
        .post(app_url(endpoint, app_id, "accounts/backend-link/challenge"))
        .bearer_auth(bearer)
        .send()
        .await
        .map_err(|error| Error::Server(format!("linking the device account: {error}")))?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(Error::SignedOut);
    }
    if !response.status().is_success() {
        return Err(refusal("asking for a link challenge", response).await);
    }
    #[derive(Deserialize)]
    struct Challenge {
        challenge: String,
    }
    let Challenge { challenge } = response
        .json()
        .await
        .map_err(|error| Error::Server(format!("asking for a link challenge: {error}")))?;
    let proof = key.sign_challenge(&challenge)?;
    let response = http()?
        .post(app_url(endpoint, app_id, "accounts/backend-link"))
        .bearer_auth(bearer)
        .json(&json!({ "challenge": challenge, "proof": proof }))
        .send()
        .await
        .map_err(|error| Error::Server(format!("linking the device account: {error}")))?;
    match response.status() {
        // No one signed in any more; any other 401 refuses the device's
        // proof (a retired key, another app's guest), for good.
        reqwest::StatusCode::UNAUTHORIZED => {
            let body = response.text().await.unwrap_or_default();
            if body.trim() == "signed_out" || body.trim().is_empty() {
                return Err(Error::SignedOut);
            }
            return Err(Error::Refused(format!(
                "the backend refused the device account's proof: {}",
                body.trim()
            )));
        }
        reqwest::StatusCode::CONFLICT => {
            return Err(Error::Refused(
                "the backend refused the link: its user already has another account".into(),
            ));
        }
        status if !status.is_success() => {
            return Err(refusal("linking the device account", response).await);
        }
        _ => {}
    }
    response
        .json()
        .await
        .map_err(|error| Error::Server(format!("linking the device account: {error}")))
}

/// Who the app's backend says the bearer names (`backend-user`), without
/// assigning an account: `None` for no one; a user with `account: None`
/// has none yet (a guest on this device may link to them).
#[derive(Debug, Deserialize)]
pub(crate) struct BackendUser {
    pub account: Option<AccountId>,
    pub identity: SignedInIdentity,
}

pub(crate) async fn backend_user(
    endpoint: &str,
    app_id: AppId,
    bearer: &str,
) -> Result<Option<BackendUser>> {
    let response = http()?
        .post(app_url(endpoint, app_id, "accounts/backend-user"))
        .bearer_auth(bearer)
        .send()
        .await
        .map_err(|error| Error::Server(format!("asking who is signed in: {error}")))?;
    // Older backends answer no one with a 401.
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(refusal("asking who is signed in", response).await);
    }
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|error| Error::Server(format!("asking who is signed in: {error}")))?;
    if body.get("identity").is_none() {
        return Ok(None);
    }
    serde_json::from_value(body)
        .map(Some)
        .map_err(|error| Error::Server(format!("asking who is signed in: {error}")))
}
