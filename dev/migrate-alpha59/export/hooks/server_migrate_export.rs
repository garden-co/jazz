//! Read-only export hook for the one-time alpha.59 -> SQLite migration.
//!
//! `dev/migrate-alpha59/prepare.sh` copies this file into an alpha.59
//! checkout as `jazz_server::server::migrate_export`, so it can read the
//! server's private admin catalogue with alpha.59's own decoders. It never
//! writes. Nothing in released Jazz contains it.
#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use jazz::groove::storage::StorageFactory;
use jazz::tools::public_schema::{ColumnType, TableSchema, Value};
use jazz::tools::schema_lens::LensOp;
use serde::Serialize;

use super::catalogue_payload_codec::{
    decode_lens_transform, decode_permissions_bundle, decode_permissions_head, decode_schema,
};
use super::catalogue_storage::{CatalogueKvStorage, CatalogueStorage};

/// One admin catalogue entry, payload decoded to JSON of public types.
#[derive(Clone, Debug)]
pub struct ExportedAdminEntry {
    pub object_id: [u8; 16],
    pub metadata: BTreeMap<String, String>,
    pub object_type: String,
    pub payload_json: String,
}

/// A lens transform as JSON. Same shape as the public `LensTransform`.
#[derive(Serialize)]
struct LensTransformJson<'a> {
    ops: Vec<LensOpJson<'a>>,
    draft_ops: &'a [usize],
}

#[derive(Serialize)]
enum LensOpJson<'a> {
    RenameTable {
        old_name: &'a str,
        new_name: &'a str,
    },
    AddColumn {
        table: &'a str,
        column: &'a str,
        column_type: &'a ColumnType,
        default: &'a Value,
    },
    RemoveColumn {
        table: &'a str,
        column: &'a str,
        column_type: &'a ColumnType,
        default: &'a Value,
    },
    RenameColumn {
        table: &'a str,
        old_name: &'a str,
        new_name: &'a str,
    },
    AddTable {
        table: &'a str,
        schema: &'a TableSchema,
    },
    RemoveTable {
        table: &'a str,
        schema: &'a TableSchema,
    },
}

fn lens_op(op: &LensOp) -> LensOpJson<'_> {
    match op {
        LensOp::RenameTable { old_name, new_name } => {
            LensOpJson::RenameTable { old_name, new_name }
        }
        LensOp::AddColumn {
            table,
            column,
            column_type,
            default,
        } => LensOpJson::AddColumn {
            table,
            column,
            column_type,
            default,
        },
        LensOp::RemoveColumn {
            table,
            column,
            column_type,
            default,
        } => LensOpJson::RemoveColumn {
            table,
            column,
            column_type,
            default,
        },
        LensOp::RenameColumn {
            table,
            old_name,
            new_name,
        } => LensOpJson::RenameColumn {
            table,
            old_name,
            new_name,
        },
        LensOp::AddTable { table, schema } => LensOpJson::AddTable { table, schema },
        LensOp::RemoveTable { table, schema } => LensOpJson::RemoveTable { table, schema },
    }
}

#[derive(Serialize)]
struct PermissionsBundleJson<'a, P: Serialize> {
    schema_hash: String,
    version: u64,
    parent_bundle_object_id: Option<String>,
    permissions: &'a P,
}

#[derive(Serialize)]
struct PermissionsHeadJson {
    schema_hash: String,
    version: u64,
    parent_bundle_object_id: Option<String>,
    bundle_object_id: String,
}

/// Every entry of the admin catalogue root at `path` (a private copy).
pub fn migrate_export_admin_catalogue(
    factory: Arc<dyn StorageFactory>,
    path: PathBuf,
) -> Result<Vec<ExportedAdminEntry>, String> {
    let storage = CatalogueKvStorage::open(factory, path).map_err(|error| error.to_string())?;
    let entries = storage
        .scan_catalogue_entries()
        .map_err(|error| error.to_string())?;
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let object_type = entry.object_type().unwrap_or("unknown").to_owned();
        let context = |error: &dyn std::fmt::Display| {
            format!(
                "catalogue entry {} ({object_type}): {error}",
                entry.object_id
            )
        };
        let payload_json = match object_type.as_str() {
            "catalogue_schema" => {
                let schema = decode_schema(&entry.content).map_err(|e| context(&e))?;
                serde_json::to_string(&schema)
            }
            "catalogue_lens" => {
                let transform = decode_lens_transform(&entry.content).map_err(|e| context(&e))?;
                serde_json::to_string(&LensTransformJson {
                    ops: transform.ops.iter().map(lens_op).collect(),
                    draft_ops: &transform.draft_ops,
                })
            }
            "catalogue_permissions_bundle" => {
                let (schema_hash, version, parent, permissions) =
                    decode_permissions_bundle(&entry.content).map_err(|e| context(&e))?;
                let permissions: BTreeMap<_, _> = permissions.into_iter().collect();
                serde_json::to_string(&PermissionsBundleJson {
                    schema_hash: schema_hash.to_string(),
                    version,
                    parent_bundle_object_id: parent.map(|id| id.to_string()),
                    permissions: &permissions,
                })
            }
            "catalogue_permissions_head" => {
                let (schema_hash, version, parent, bundle) =
                    decode_permissions_head(&entry.content).map_err(|e| context(&e))?;
                serde_json::to_string(&PermissionsHeadJson {
                    schema_hash: schema_hash.to_string(),
                    version,
                    parent_bundle_object_id: parent.map(|id| id.to_string()),
                    bundle_object_id: bundle.to_string(),
                })
            }
            // alpha.59 itself ignores kinds it does not know; the migration
            // must not, or it would silently drop state.
            other => return Err(context(&format!("unknown catalogue entry type {other:?}"))),
        }
        .map_err(|e| context(&e))?;
        out.push(ExportedAdminEntry {
            object_id: *entry.object_id.uuid().as_bytes(),
            metadata: entry.metadata.into_iter().collect(),
            object_type,
            payload_json,
        });
    }
    storage.close().map_err(|error| error.to_string())?;
    Ok(out)
}
