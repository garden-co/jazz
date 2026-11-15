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
    let body = json!({
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
