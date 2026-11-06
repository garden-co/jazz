//! Server access for `jazz-tools alpha59-alpha60`, which imports an alpha.59
//! server data dir. It drives the same code `deploy()` and the account
//! routes run, without HTTP.
//!
//! Only for the alpha59-alpha60 importer; it goes when the importer does.
#![allow(missing_docs)]

use std::sync::Arc;

use jazz::account_registry::AccountCommand;

use super::ServerState;
use super::deployment::{DeployRequest, deploy};

/// One deployment, as a `DeployRequest` in JSON; it publishes a bundle.
pub async fn migrate_import_deploy(
    state: &Arc<ServerState>,
    request: serde_json::Value,
) -> Result<bool, String> {
    let request: DeployRequest =
        serde_json::from_value(request).map_err(|error| format!("deploy request: {error}"))?;
    deploy(state.clone(), request)
        .await
        .map(|response| response.changed)
        .map_err(|error| format!("deploy refused ({}): {}", error.status, error.body))
}

/// Replay one account command, in serde's JSON shape of `AccountCommand`.
pub async fn migrate_import_account(state: &ServerState, command_json: &str) -> Result<(), String> {
    let command: AccountCommand =
        serde_json::from_str(command_json).map_err(|error| format!("account command: {error}"))?;
    state
        .accounts
        .as_ref()
        .ok_or("the server has no account registry")?
        .execute(command)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// What this server answers when `principal_json` logs in:
/// `{"Ok": assignment}` or `{"Err": reason}`, as the exporter records it.
pub async fn migrate_import_login(
    state: &ServerState,
    principal_json: &str,
) -> Result<serde_json::Value, String> {
    let principal = serde_json::from_str(principal_json).map_err(|error| error.to_string())?;
    let accounts = state
        .accounts
        .as_ref()
        .ok_or("the server has no account registry")?;
    Ok(match accounts.login(principal).await {
        Ok(assignment) => serde_json::json!({ "Ok": assignment }),
        Err(error) => serde_json::json!({ "Err": error.to_string() }),
    })
}

/// The permissions bundle published first: `{"schema_hash", "bundle",
/// "permissions"}`. That is the import's one deployment; the app's own
/// deployments, such as a backend's first start with its permissions, come
/// after it.
pub fn migrate_import_first_permissions(state: &ServerState) -> Result<serde_json::Value, String> {
    let Some(bundle) = state.catalogue_store.published_bundles().into_iter().next() else {
        return Ok(serde_json::Value::Null);
    };
    let permissions: std::collections::BTreeMap<_, _> = bundle.permissions.into_iter().collect();
    Ok(serde_json::json!({
        "schema_hash": bundle.schema.to_string(),
        "bundle": bundle.id.to_hex(),
        "permissions": permissions,
    }))
}
