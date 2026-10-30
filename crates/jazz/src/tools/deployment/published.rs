//! Migrations as the CLI publishes them (`POST .../admin/deploy`) and as an
//! app ships them to its clients: per-table lens edits between two schema
//! hashes, lowered against the resolved source and target schemas.

use serde::{Deserialize, Serialize};

use crate::protocol::MigrationLens;
use crate::schema::JazzSchema;
use crate::tools::public_schema::{ColumnType, Schema, SchemaHash, TableName, Value};
use crate::tools::schema_lens::{Lens, LensOp, LensTransform, runtime::compile_lens};

/// A schema hash as 64 hex characters.
pub fn parse_schema_hash(hash_text: &str) -> Result<SchemaHash, String> {
    let decoded_hash_bytes = hex::decode(hash_text)
        .map_err(|_| "invalid schema hash: expected hex string".to_string())?;
    if decoded_hash_bytes.len() != 32 {
        return Err("invalid schema hash: expected 64 hex chars".to_string());
    }
    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&decoded_hash_bytes);
    Ok(SchemaHash::from_bytes(hash_bytes))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishMigrationRequest {
    pub from_hash: String,
    pub to_hash: String,
    pub forward: Vec<PublishTableLens>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishTableLens {
    pub table: String,
    #[serde(default)]
    pub added: bool,
    #[serde(default)]
    pub removed: bool,
    pub renamed_from: Option<String>,
    pub operations: Vec<PublishLensOp>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum PublishLensOp {
    Introduce {
        column: String,
        column_type: ColumnType,
        value: Value,
    },
    Drop {
        column: String,
        column_type: ColumnType,
        value: Value,
    },
    Rename {
        column: String,
        value: String,
    },
}

/// Lower a deployment migration against its resolved source and target schemas.
pub fn lower_migration(
    request: PublishMigrationRequest,
    source_schema: &Schema,
    target_schema: &Schema,
) -> Result<Lens, String> {
    let source_hash = parse_schema_hash(&request.from_hash)?;
    let target_hash = parse_schema_hash(&request.to_hash)?;
    let tables = request.forward;
    let mut forward = LensTransform::new();
    for table_lens in tables {
        let table_name = table_lens.table;
        if table_lens.added && table_lens.removed {
            return Err(format!(
                "table {} cannot be both added and removed",
                table_name
            ));
        }
        if (table_lens.added || table_lens.removed) && table_lens.renamed_from.is_some() {
            return Err(format!(
                "table {} cannot combine added/removed markers with renamedFrom",
                table_name
            ));
        }
        if (table_lens.added || table_lens.removed) && !table_lens.operations.is_empty() {
            return Err(format!(
                "table {} cannot combine added/removed markers with column operations",
                table_name
            ));
        }
        if table_lens.added {
            let target_table_name = TableName::from(table_name.clone());
            let schema = match target_schema.get(&target_table_name) {
                Some(schema) => schema.clone(),
                None => {
                    return Err(format!(
                        "createTables references unknown target table {}",
                        table_name
                    ));
                }
            };
            forward.push(
                LensOp::AddTable {
                    table: table_name.clone(),
                    schema,
                },
                false,
            );
        }
        if table_lens.removed {
            let source_table_name = TableName::from(table_name.clone());
            let schema = match source_schema.get(&source_table_name) {
                Some(schema) => schema.clone(),
                None => {
                    return Err(format!(
                        "dropTables references unknown source table {}",
                        table_name
                    ));
                }
            };
            forward.push(
                LensOp::RemoveTable {
                    table: table_name.clone(),
                    schema,
                },
                false,
            );
        }
        if let Some(renamed_from) = table_lens.renamed_from {
            forward.push(
                LensOp::RenameTable {
                    old_name: renamed_from,
                    new_name: table_name.clone(),
                },
                false,
            );
        }
        for operation in table_lens.operations {
            let op = match operation {
                PublishLensOp::Introduce {
                    column,
                    column_type,
                    value,
                } => LensOp::AddColumn {
                    table: table_name.clone(),
                    column,
                    column_type,
                    default: value,
                },
                PublishLensOp::Drop {
                    column,
                    column_type,
                    value,
                } => LensOp::RemoveColumn {
                    table: table_name.clone(),
                    column,
                    column_type,
                    default: value,
                },
                PublishLensOp::Rename { column, value } => LensOp::RenameColumn {
                    table: table_name.clone(),
                    old_name: column,
                    new_name: value,
                },
            };
            forward.push(op, false);
        }
    }

    Ok(Lens::new(source_hash, target_hash, forward))
}

/// A schema in a [`ClientMigrations`] bundle, with its hash.
#[derive(Debug, Serialize, Deserialize)]
pub struct PublishedSchema {
    pub hash: String,
    pub schema: Schema,
}

/// The app's schema versions and the migrations between them, as it ships
/// them to its clients (the `schemas` and `migrations` of a deployment).
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientMigrations {
    pub schemas: Vec<PublishedSchema>,
    pub migrations: Vec<PublishMigrationRequest>,
}

/// The runtime schemas and lenses a [`ClientMigrations`] bundle (JSON)
/// holds, lowered as the authority lowers a deployment, so a client opens
/// along the lenses its authority admitted (SPEC 10.7).
pub fn client_migrations(json: &[u8]) -> Result<(Vec<JazzSchema>, Vec<MigrationLens>), String> {
    let bundle: ClientMigrations =
        serde_json::from_slice(json).map_err(|error| format!("decode migrations: {error}"))?;
    let mut by_hash = std::collections::HashMap::new();
    for entry in &bundle.schemas {
        let claimed = parse_schema_hash(&entry.hash)?;
        let actual = SchemaHash::compute(&entry.schema);
        if claimed != actual {
            return Err(format!(
                "migrations: schema {} does not match its content hash {actual}",
                entry.hash
            ));
        }
        by_hash.insert(actual, &entry.schema);
    }
    let mut lenses = Vec::new();
    for migration in bundle.migrations {
        let (from, to) = (
            parse_schema_hash(&migration.from_hash)?,
            parse_schema_hash(&migration.to_hash)?,
        );
        let (Some(source), Some(target)) = (by_hash.get(&from), by_hash.get(&to)) else {
            return Err(format!(
                "migrations: {} -> {} names a schema the bundle lacks",
                migration.from_hash, migration.to_hash
            ));
        };
        let lens = lower_migration(migration, source, target)?;
        lenses.push(compile_lens(&lens, source, target)?);
    }
    let schemas = bundle
        .schemas
        .iter()
        .map(|entry| JazzSchema::new(&entry.schema).map_err(|error| format!("migrations: {error}")))
        .collect::<Result<_, _>>()?;
    Ok((schemas, lenses))
}

/// The bundle in `migrations` (JSON, see [`client_migrations`]) lowered for
/// a store; none ships nothing.
pub fn lowered_client_migrations(
    migrations: Option<&[u8]>,
) -> Result<crate::db::Migrations, String> {
    match migrations {
        None => Ok(crate::db::Migrations::default()),
        Some(json) => {
            let (schemas, lenses) = client_migrations(json)?;
            Ok(crate::db::Migrations { schemas, lenses })
        }
    }
}

/// The bundle in a `migrations/index.ts` module `jazz-tools validate` and
/// `deploy` generate (`export const migrations: ClientMigrations = <JSON>;`),
/// lowered for a store, so a Rust app ships what its TypeScript clients do.
pub fn client_migrations_module(source: &str) -> Result<crate::db::Migrations, String> {
    let json = source
        .split_once("export const migrations: ClientMigrations =")
        .map(|(_, rest)| rest.trim().trim_end_matches(';').trim_end())
        .ok_or("migrations module: no `export const migrations: ClientMigrations =`")?;
    lowered_client_migrations(Some(json.as_bytes())).map_err(|error| {
        format!("{error} (was the module reformatted? regenerate it with `jazz-tools validate`)")
    })
}

/// `config` shipping the bundle in `migrations` (JSON, see
/// [`client_migrations`]), as a binding's open configuration carries it.
pub fn with_client_migrations(
    config: crate::db::DbConfig,
    migrations: Option<&[u8]>,
) -> Result<crate::db::DbConfig, String> {
    match migrations {
        None => Ok(config),
        Some(json) => {
            let (schemas, lenses) = client_migrations(json)?;
            Ok(config.with_migrations(schemas, lenses))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::LensOp as CoreLensOp;
    use crate::tools::public_schema::{SchemaBuilder, TableSchema};

    fn todos(title: &str) -> Schema {
        SchemaBuilder::new()
            .table(TableSchema::builder("todos").column(title, ColumnType::Text))
            .build()
    }

    fn bundle(v1: &Schema, v2: &Schema, v1_hash: String) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schemas": [
                {"hash": v1_hash, "schema": v1},
                {"hash": SchemaHash::compute(v2).to_string(), "schema": v2},
            ],
            "migrations": [{
                "fromHash": SchemaHash::compute(v1).to_string(),
                "toHash": SchemaHash::compute(v2).to_string(),
                "forward": [{
                    "table": "todos",
                    "operations": [{"type": "rename", "column": "title", "value": "name"}],
                }],
            }],
        }))
        .unwrap()
    }

    #[test]
    fn a_generated_module_lowers_like_its_bundle() {
        let (v1, v2) = (todos("title"), todos("name"));
        let json = bundle(&v1, &v2, SchemaHash::compute(&v1).to_string());
        let module = format!(
            "// Generated by jazz-tools\nimport type {{ ClientMigrations }} from \"jazz-tools\";\n\n\
             export const migrations: ClientMigrations = {};\n",
            String::from_utf8(json.clone()).unwrap()
        );
        let lowered = client_migrations_module(&module).unwrap();
        let (schemas, lenses) = client_migrations(&json).unwrap();
        assert_eq!(lowered.schemas, schemas);
        assert_eq!(lowered.lenses, lenses);
        let empty = client_migrations_module(
            "export const migrations: ClientMigrations = {\"schemas\":[],\"migrations\":[]};",
        )
        .unwrap();
        assert!(empty.schemas.is_empty() && empty.lenses.is_empty());
    }

    #[test]
    fn a_bundle_lowers_to_the_runtime_lenses_the_authority_admits() {
        let (v1, v2) = (todos("title"), todos("name"));
        let (schemas, lenses) =
            client_migrations(&bundle(&v1, &v2, SchemaHash::compute(&v1).to_string())).unwrap();
        assert_eq!(
            schemas.iter().map(|s| s.version_id()).collect::<Vec<_>>(),
            [
                JazzSchema::new(&v1).unwrap().version_id(),
                JazzSchema::new(&v2).unwrap().version_id()
            ]
        );
        let [lens] = lenses.as_slice() else {
            panic!("{lenses:?}")
        };
        assert_eq!(lens.source, schemas[0].version_id());
        assert_eq!(lens.target, schemas[1].version_id());
        assert!(
            lens.table_lenses[0].ops.iter().any(|op| matches!(
                op,
                CoreLensOp::RenameColumn { from, to } if from == "title" && to == "name"
            )),
            "{lens:?}"
        );
    }

    #[test]
    fn a_schema_whose_hash_is_wrong_is_refused() {
        let (v1, v2) = (todos("title"), todos("name"));
        let error =
            client_migrations(&bundle(&v1, &v2, SchemaHash::compute(&v2).to_string())).unwrap_err();
        assert!(error.contains("content hash"), "{error}");
    }
}
