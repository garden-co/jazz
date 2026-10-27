//! Import hook for the one-time alpha.59 -> SQLite migration.
//!
//! `dev/migrate-alpha59/prepare.sh` copies this file into a SQLite-engine
//! checkout as `jazz_server::server::migrate_import`. It drives the same
//! code the admin `POST /deploy` route and the account routes run, without
//! HTTP. Nothing in released Jazz contains it.
#![allow(missing_docs)]

use std::sync::Arc;

use jazz::account_registry::AccountCommand;

use super::ServerState;
use super::deployment::{DeployRequest, deploy};

/// One deployment, as the JSON body of `POST /apps/{app}/admin/deploy`.
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

/// The active permissions head: `{"schema_hash", "version", "permissions"}`.
pub fn migrate_import_active_permissions(state: &ServerState) -> Result<serde_json::Value, String> {
    let Some(active) = state
        .catalogue
        .active_schema(&state.catalogue_store)
        .map_err(|error| error.to_string())?
    else {
        return Ok(serde_json::Value::Null);
    };
    let permissions: std::collections::BTreeMap<_, _> = active.permissions.into_iter().collect();
    Ok(serde_json::json!({
        "schema_hash": active.summary.schema_hash.to_string(),
        "version": active.summary.version,
        "permissions": permissions,
    }))
}
