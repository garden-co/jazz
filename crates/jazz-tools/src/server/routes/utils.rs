//! Shared parsing/validation helpers used by both HTTP and WebSocket handlers.

//! HTTP routes for the Jazz server.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::object::ObjectId;
use crate::query_manager::types::{SchemaHash, TableName, TablePolicies};
use crate::schema_manager::AppId;
use crate::server::ServerState;

use super::http::PermissionsHeadView;

pub(super) fn parse_schema_hash_param(hash_text: &str) -> Result<SchemaHash, String> {
    let decoded_hash_bytes = hex::decode(hash_text)
        .map_err(|_| "invalid schema hash: expected hex string".to_string())?;
    if decoded_hash_bytes.len() != 32 {
        return Err("invalid schema hash: expected 64 hex chars".to_string());
    }

    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&decoded_hash_bytes);
    Ok(SchemaHash::from_bytes(hash_bytes))
}

pub(super) fn connection_schema_diagnostics_from_handshake(
    state: &Arc<ServerState>,
    handshake: &crate::transport_manager::AuthHandshake,
) -> Result<
    Option<crate::sync_manager::ConnectionSchemaDiagnostics>,
    crate::runtime_tokio::RuntimeError,
> {
    let Some(client_schema_hash) = handshake.declared_schema_hash() else {
        return Ok(None);
    };

    let diagnostics = state
        .runtime
        .with_schema_manager(|sm| sm.connection_schema_diagnostics(client_schema_hash))?;
    Ok(diagnostics.has_issues().then_some(diagnostics))
}

pub(super) fn parse_object_id_param(object_id_text: &str) -> Result<ObjectId, String> {
    let uuid = Uuid::parse_str(object_id_text)
        .map_err(|_| "invalid object id: expected UUID".to_string())?;
    Ok(ObjectId::from_uuid(uuid))
}

pub(super) fn parse_app_id_param(app_id_text: &str) -> Result<AppId, String> {
    let trimmed = app_id_text.trim();
    if trimmed.is_empty() {
        return Err("invalid appId: expected UUID or app identifier".to_string());
    }

    if let Ok(app_id) = AppId::from_string(trimmed) {
        return Ok(app_id);
    }

    if trimmed
        .chars()
        .all(|char| char.is_ascii_alphanumeric() || matches!(char, '-' | '_' | '.'))
    {
        return Ok(AppId::from_name(trimmed));
    }

    Err("invalid appId: expected UUID or app identifier".to_string())
}

pub(super) fn permissions_head_view(
    head: crate::schema_manager::manager::PermissionsHeadSummary,
) -> PermissionsHeadView {
    PermissionsHeadView {
        schema_hash: head.schema_hash.to_string(),
        version: head.version,
        parent_bundle_object_id: head
            .parent_bundle_object_id
            .map(|object_id| object_id.to_string()),
        bundle_object_id: head.bundle_object_id.to_string(),
    }
}

pub(super) fn permissions_map_view(
    permissions: std::collections::HashMap<TableName, TablePolicies>,
) -> std::collections::HashMap<String, TablePolicies> {
    permissions
        .into_iter()
        .map(|(table_name, policies)| (table_name.to_string(), policies))
        .collect()
}

pub(super) fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
