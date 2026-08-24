//! Compatibility reader for the former file-backed admin schema catalogue.
//!
//! The production HTTP API persists its catalogue through [`ServerCatalogue`],
//! but the standalone CLI can still start from a previously written
//! `admin-schemas.json`. Keep that migration input separate from HTTP serving
//! so it cannot grow a second server implementation again.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

use jazz::schema::JazzSchema;
use jazz::tools::public_schema::Schema;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredAdminSchema {
    object_id: String,
    schema: Value,
    permissions: Option<Value>,
}

/// Load the latest schema for an app from the legacy `admin-schemas.json`
/// compatibility file.
///
/// New servers persist catalogue data in their configured storage backend.
/// This loader only preserves CLI startup for data directories produced by the
/// retired loopback HTTP bridge.
pub fn load_latest_admin_schema_for_app(
    data_dir: impl AsRef<Path>,
    app_id: &str,
) -> io::Result<Option<JazzSchema>> {
    let schema_store_path = data_dir.as_ref().join("admin-schemas.json");
    let schemas = load_admin_schema_store(&schema_store_path)?;
    let Some(schema) = schemas.get(app_id).and_then(|schemas| schemas.last()) else {
        return Ok(None);
    };
    if matches!(schema.permissions.as_ref(), Some(value) if !value.is_null()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "stored admin schema {} has unsupported permissions",
                schema.object_id
            ),
        ));
    }
    compile_canonical_admin_schema(&schema.schema)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn load_admin_schema_store(path: &Path) -> io::Result<HashMap<String, Vec<StoredAdminSchema>>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(error) => Err(error),
    }
}

fn compile_canonical_admin_schema(schema: &Value) -> Result<JazzSchema, String> {
    let source = serde_json::from_value::<Schema>(schema.clone())
        .map_err(|error| format!("invalid canonical public schema: {error}"))?;
    JazzSchema::new(&source).map_err(|error| format!("public schema compilation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn loads_the_latest_legacy_schema_for_the_requested_app() {
        let data_dir = tempfile::tempdir().expect("temporary data directory");
        std::fs::write(
            data_dir.path().join("admin-schemas.json"),
            serde_json::to_vec(&json!({
                "app-a": [{
                    "objectId": "schema:app-a:old",
                    "schema": { "tables": {} },
                    "permissions": null
                }, {
                    "objectId": "schema:app-a:new",
                    "schema": { "tables": {} },
                    "permissions": null
                }]
            }))
            .expect("encode legacy catalogue"),
        )
        .expect("write legacy catalogue");

        assert!(
            load_latest_admin_schema_for_app(data_dir.path(), "app-a")
                .expect("read legacy catalogue")
                .is_some()
        );
        assert!(
            load_latest_admin_schema_for_app(data_dir.path(), "missing")
                .expect("read legacy catalogue")
                .is_none()
        );
    }
}
