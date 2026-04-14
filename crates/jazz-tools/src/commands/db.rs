use std::collections::HashMap;
use std::fs;
use std::io::{self, Read};
use std::path::Path;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use clap::{Args, Subcommand};
use jazz_tools::binding_support::parse_query_input;
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::encoding::decode_row;
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::types::{
    ColumnDescriptor, ColumnType, RowDescriptor, Schema, SchemaHash, TableName, Value,
};
use jazz_tools::runtime_core::SubscriptionDelta;
use jazz_tools::runtime_tokio::TokioRuntime;
use jazz_tools::schema_manager::{AppId, SchemaManager, rehydrate_schema_manager_from_catalogue};
use jazz_tools::server::DynStorage;
#[cfg(feature = "rocksdb")]
use jazz_tools::storage::RocksDBStorage;
#[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
use jazz_tools::storage::SqliteStorage;
use jazz_tools::sync_manager::{DurabilityTier, SyncManager};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use uuid::Uuid;

const STORAGE_CACHE_SIZE_BYTES: usize = 64 * 1024 * 1024;

#[derive(clap::Args)]
pub struct DbCommand {
    #[command(subcommand)]
    action: DbAction,
}

#[derive(Subcommand)]
enum DbAction {
    /// Show the JSON contract for query and mutation inputs
    Syntax,
    /// List tables in the selected schema
    ListTables {
        #[command(flatten)]
        args: DbContextArgs,
    },
    /// Describe one table from the selected schema
    DescribeTable {
        table: String,
        #[command(flatten)]
        args: DbContextArgs,
    },
    /// Execute a canonical Jazz query JSON payload
    Query {
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Insert a row using Jazz Value JSON for column inputs
    Insert {
        table: String,
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Update a row by object id using Jazz Value JSON for changed columns
    Update {
        table: String,
        object_id: String,
        #[command(flatten)]
        args: DbContextArgs,
        #[command(flatten)]
        input: JsonInputArgs,
    },
    /// Delete a row by object id
    Delete {
        object_id: String,
        #[command(flatten)]
        args: DbContextArgs,
    },
}

#[derive(Args, Clone)]
pub(crate) struct DbContextArgs {
    /// Application id for the local Jazz store
    #[arg(long)]
    pub(crate) app_id: String,

    /// Local Jazz storage path. Accepts a storage directory, or for SQLite-only
    /// builds a database file path.
    #[arg(long, default_value = "./data")]
    pub(crate) data_dir: String,

    /// Environment namespace used for composed branch names
    #[arg(long, default_value = "dev")]
    pub(crate) env: String,

    /// User-facing branch name for the selected schema
    #[arg(long, default_value = "main")]
    pub(crate) user_branch: String,

    /// Full schema hash or unique prefix. Required only when multiple schemas exist.
    #[arg(long)]
    pub(crate) schema_hash: Option<String>,

    /// Reserved for future schema bootstrap support. The current CLI expects a
    /// persisted schema catalogue in storage.
    #[arg(long)]
    pub(crate) schema_dir: Option<String>,
}

#[derive(Args, Clone)]
pub(crate) struct JsonInputArgs {
    /// Inline JSON payload
    #[arg(long, conflicts_with = "input_file")]
    json: Option<String>,

    /// JSON file path, or '-' to read stdin
    #[arg(long = "json-file", conflicts_with = "json")]
    input_file: Option<String>,
}

pub(crate) struct OpenDb {
    pub(crate) runtime: TokioRuntime<DynStorage>,
    pub(crate) schema: Schema,
    pub(crate) schema_hash: SchemaHash,
    pub(crate) catalogue_state_hash: String,
    pub(crate) app_id: AppId,
    pub(crate) data_dir: String,
    pub(crate) env: String,
    pub(crate) user_branch: String,
}

pub(crate) struct QuerySnapshot {
    pub(crate) descriptor: RowDescriptor,
    pub(crate) rows: Vec<(ObjectId, Vec<Value>)>,
}

pub fn run(command: DbCommand) -> Result<(), String> {
    match command.action {
        DbAction::Syntax => {
            print_json(&syntax_json())?;
        }
        DbAction::ListTables { args } => {
            let db = open_db(&args)?;
            let output = list_tables_json(&db);
            db.close()?;
            print_json(&output)?;
        }
        DbAction::DescribeTable { table, args } => {
            let db = open_db(&args)?;
            let output = describe_table_json(&db, &table)?;
            db.close()?;
            print_json(&output)?;
        }
        DbAction::Query { args, input } => {
            let db = open_db(&args)?;
            let query = parse_query_input(&input.read_required("query")?)
                .map_err(|err| format!("invalid query JSON: {err}"))?;
            let snapshot = execute_query_snapshot(&db.runtime, query)?;
            let output = query_result_json(&db, snapshot);
            db.close()?;
            print_json(&output)?;
        }
        DbAction::Insert { table, args, input } => {
            let db = open_db(&args)?;
            let values = parse_value_map(&input.read_required("insert values")?)?;
            let (object_id, row_values) = db
                .runtime
                .insert(&table, values, None)
                .map_err(|err| format!("insert failed: {err}"))?;
            let descriptor = table_descriptor(&db.schema, &table)?.clone();
            let output = mutation_result_json(
                &db,
                "insert",
                Some(&table),
                Some(row_json(object_id, &row_values, &descriptor)),
                Some(object_id),
            );
            db.close()?;
            print_json(&output)?;
        }
        DbAction::Update {
            table,
            object_id,
            args,
            input,
        } => {
            let db = open_db(&args)?;
            let object_id = parse_object_id(&object_id)?;
            let values = parse_value_map(&input.read_required("update values")?)?;
            db.runtime
                .update(object_id, values.into_iter().collect(), None)
                .map_err(|err| format!("update failed: {err}"))?;
            let output = mutation_result_json(&db, "update", Some(&table), None, Some(object_id));
            db.close()?;
            print_json(&output)?;
        }
        DbAction::Delete { object_id, args } => {
            let db = open_db(&args)?;
            let object_id = parse_object_id(&object_id)?;
            db.runtime
                .delete(object_id, None)
                .map_err(|err| format!("delete failed: {err}"))?;
            let output = mutation_result_json(&db, "delete", None, None, Some(object_id));
            db.close()?;
            print_json(&output)?;
        }
    }

    Ok(())
}

impl JsonInputArgs {
    pub(crate) fn read_required(&self, label: &str) -> Result<String, String> {
        match (&self.json, &self.input_file) {
            (Some(json), None) => Ok(json.clone()),
            (None, Some(path)) if path == "-" => {
                let mut buffer = String::new();
                io::stdin()
                    .read_to_string(&mut buffer)
                    .map_err(|err| format!("failed to read {label} from stdin: {err}"))?;
                if buffer.trim().is_empty() {
                    return Err(format!("{label} JSON from stdin was empty"));
                }
                Ok(buffer)
            }
            (None, Some(path)) => fs::read_to_string(path)
                .map_err(|err| format!("failed to read {label} JSON from '{path}': {err}")),
            (None, None) => Err(format!(
                "missing {label} JSON. Pass --json '<payload>' or --json-file <path>"
            )),
            (Some(_), Some(_)) => unreachable!("clap enforces conflicts"),
        }
    }
}

impl OpenDb {
    pub(crate) fn metadata_json(&self) -> JsonValue {
        json!({
            "appId": self.app_id.to_string(),
            "storagePath": self.data_dir,
            "env": self.env,
            "userBranch": self.user_branch,
            "schemaHash": self.schema_hash,
            "catalogueStateHash": self.catalogue_state_hash,
        })
    }

    pub(crate) fn close(self) -> Result<(), String> {
        self.runtime
            .with_storage(|storage| {
                storage.flush();
                storage.close()
            })
            .map_err(|err| format!("failed to access storage for close: {err}"))?
            .map_err(|err| format!("failed to close storage: {err}"))
    }
}

pub(crate) fn open_db(args: &DbContextArgs) -> Result<OpenDb, String> {
    let app_id = AppId::from_string(&args.app_id).unwrap_or_else(|_| AppId::from_name(&args.app_id));
    let storage = open_storage(&args.data_dir)?;

    let mut inspection_manager = SchemaManager::new_server(server_sync_manager(), app_id, &args.env);
    rehydrate_schema_manager_from_catalogue(&mut inspection_manager, storage.as_ref(), app_id)?;

    if inspection_manager.known_schema_hashes().is_empty() {
        let schema_dir_note = args
            .schema_dir
            .as_deref()
            .map(|schema_dir| format!(" (schema bootstrap via --schema-dir is not implemented in the current CLI: {schema_dir})"))
            .unwrap_or_default();
        return Err(format!(
            "no persisted schema catalogue was found in this Jazz store{schema_dir_note}"
        ));
    }

    let schema_hash = resolve_schema_hash(&inspection_manager, args.schema_hash.as_deref())?;
    let schema = inspection_manager
        .get_known_schema(&schema_hash)
        .cloned()
        .ok_or_else(|| format!("schema {} was selected but could not be loaded", schema_hash))?;

    let mut schema_manager = SchemaManager::new(
        server_sync_manager(),
        schema.clone(),
        app_id,
        &args.env,
        &args.user_branch,
    )
    .map_err(|err| format!("failed to initialize schema manager: {err}"))?;
    rehydrate_schema_manager_from_catalogue(&mut schema_manager, storage.as_ref(), app_id)?;

    let runtime = TokioRuntime::new(schema_manager, storage, |_| {});
    let catalogue_state_hash = runtime
        .catalogue_state_hash()
        .map_err(|err| format!("failed to compute catalogue state hash: {err}"))?;

    Ok(OpenDb {
        runtime,
        schema,
        schema_hash,
        catalogue_state_hash,
        app_id,
        data_dir: args.data_dir.clone(),
        env: args.env.clone(),
        user_branch: args.user_branch.clone(),
    })
}

#[cfg(test)]
pub(crate) fn persist_schema_catalogue_for_test(
    args: &DbContextArgs,
    schema: Schema,
) -> Result<(), String> {
    let app_id = AppId::from_string(&args.app_id).unwrap_or_else(|_| AppId::from_name(&args.app_id));
    let storage = open_storage(&args.data_dir)?;
    let schema_manager =
        SchemaManager::new(server_sync_manager(), schema, app_id, &args.env, &args.user_branch)
            .map_err(|err| format!("failed to initialize schema manager: {err}"))?;
    let runtime = TokioRuntime::new(schema_manager, storage, |_| {});
    runtime
        .persist_schema()
        .map_err(|err| format!("failed to persist schema: {err}"))?;
    runtime
        .with_storage(|storage| {
            storage.flush();
            storage.close()
        })
        .map_err(|err| format!("failed to access storage for close: {err}"))?
        .map_err(|err| format!("failed to close storage: {err}"))
}

fn open_storage(data_dir: &str) -> Result<DynStorage, String> {
    let requested_path = Path::new(data_dir);
    #[cfg(feature = "rocksdb")]
    {
        let db_path = if requested_path.exists()
            && requested_path.is_dir()
            && (requested_path.join("CURRENT").exists() || requested_path.join("LOCK").exists())
        {
            requested_path.to_path_buf()
        } else if requested_path.join("jazz.rocksdb").exists() {
            requested_path.join("jazz.rocksdb")
        } else {
            std::fs::create_dir_all(data_dir)
                .map_err(|err| format!("failed to create data dir '{data_dir}': {err}"))?;
            requested_path.join("jazz.rocksdb")
        };

        let storage = RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES)
            .map_err(|err| format!("failed to open storage '{}': {err:?}", db_path.display()))?;
        return Ok(Box::new(storage));
    }

    #[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
    {
        let db_path = if requested_path.exists() && requested_path.is_file() {
            requested_path.to_path_buf()
        } else if requested_path.join("jazz.sqlite").exists() {
            requested_path.join("jazz.sqlite")
        } else if requested_path.extension().is_some() && !requested_path.exists() {
            if let Some(parent) = requested_path.parent() {
                std::fs::create_dir_all(parent).map_err(|err| {
                    format!(
                        "failed to create parent dir '{}' for storage file: {err}",
                        parent.display()
                    )
                })?;
            }
            requested_path.to_path_buf()
        } else {
            std::fs::create_dir_all(data_dir)
                .map_err(|err| format!("failed to create data dir '{data_dir}': {err}"))?;
            requested_path.join("jazz.sqlite")
        };

        let storage = SqliteStorage::open(&db_path)
            .map_err(|err| format!("failed to open storage '{}': {err:?}", db_path.display()))?;
        return Ok(Box::new(storage));
    }

    #[allow(unreachable_code)]
    Err("no persistent storage backend is enabled for this build".to_string())
}

fn resolve_schema_hash(
    schema_manager: &SchemaManager,
    requested: Option<&str>,
) -> Result<SchemaHash, String> {
    let mut known_hashes = schema_manager.known_schema_hashes();
    known_hashes.sort_by_key(|hash| hash.to_string());

    if known_hashes.is_empty() {
        return Err(
            "no persisted schema catalogue was found in this Jazz store. Persist a schema first."
                .to_string(),
        );
    }

    match requested {
        Some(requested) => {
            let matches: Vec<SchemaHash> = known_hashes
                .iter()
                .copied()
                .filter(|hash| {
                    let full = hash.to_string();
                    full == requested || full.starts_with(requested)
                })
                .collect();

            match matches.as_slice() {
                [schema_hash] => Ok(*schema_hash),
                [] => Err(format!(
                    "schema hash '{requested}' did not match any known schema. Known hashes: {}",
                    known_hash_list(&known_hashes)
                )),
                _ => Err(format!(
                    "schema hash prefix '{requested}' is ambiguous. Known hashes: {}",
                    known_hash_list(&known_hashes)
                )),
            }
        }
        None if known_hashes.len() == 1 => Ok(known_hashes[0]),
        None => Err(format!(
            "multiple schema hashes were found in this Jazz store. Pass --schema-hash. Known hashes: {}",
            known_hash_list(&known_hashes)
        )),
    }
}

fn known_hash_list(hashes: &[SchemaHash]) -> String {
    hashes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn server_sync_manager() -> SyncManager {
    SyncManager::new()
        .with_durability_tiers([DurabilityTier::EdgeServer, DurabilityTier::GlobalServer])
}

fn parse_value_map(input: &str) -> Result<HashMap<String, Value>, String> {
    serde_json::from_str(input)
        .map_err(|err| format!("invalid Jazz Value JSON object: {err}"))
}

fn parse_object_id(raw: &str) -> Result<ObjectId, String> {
    let uuid = Uuid::parse_str(raw).map_err(|err| format!("invalid object id '{raw}': {err}"))?;
    Ok(ObjectId::from_uuid(uuid))
}

pub(crate) fn execute_query_snapshot(
    runtime: &TokioRuntime<DynStorage>,
    query: Query,
) -> Result<QuerySnapshot, String> {
    let snapshot: Arc<Mutex<Option<Result<QuerySnapshot, String>>>> = Arc::new(Mutex::new(None));
    let snapshot_ref = Arc::clone(&snapshot);

    let handle = runtime
        .subscribe(
            query,
            move |delta: SubscriptionDelta| {
                let result = delta
                    .ordered_delta
                    .added
                    .iter()
                    .map(|change| {
                        decode_row(&delta.descriptor, &change.row.data)
                            .map(|values| (change.row.id, values))
                            .map_err(|err| {
                                format!(
                                    "failed to decode query row {}: {err}",
                                    change.row.id
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|rows| QuerySnapshot {
                        descriptor: delta.descriptor.clone(),
                        rows,
                    });

                let mut guard = snapshot_ref.lock().expect("query snapshot mutex poisoned");
                if guard.is_none() {
                    *guard = Some(result);
                }
            },
            None,
        )
        .map_err(|err| format!("query subscription failed: {err}"))?;

    let result = snapshot
        .lock()
        .map_err(|_| "query snapshot mutex poisoned".to_string())?
        .take()
        .ok_or_else(|| "query did not deliver an initial snapshot".to_string())?;

    runtime
        .unsubscribe(handle)
        .map_err(|err| format!("failed to unsubscribe query snapshot: {err}"))?;

    result
}

fn list_tables_json(db: &OpenDb) -> JsonValue {
    let mut tables: Vec<_> = db.schema.iter().collect();
    tables.sort_by_key(|(name, _)| name.as_str());

    let items: Vec<JsonValue> = tables
        .into_iter()
        .map(|(name, table)| {
            json!({
                "name": name,
                "columnCount": table.columns.columns.len(),
                "hasPolicies": table.policies != Default::default(),
            })
        })
        .collect();

    json!({
        "database": db.metadata_json(),
        "tables": items,
    })
}

fn describe_table_json(db: &OpenDb, table: &str) -> Result<JsonValue, String> {
    let table_schema = db
        .schema
        .get(&TableName::new(table))
        .ok_or_else(|| format!("table '{table}' was not found in schema {}", db.schema_hash))?;

    Ok(json!({
        "database": db.metadata_json(),
        "table": table,
        "columns": columns_json(&table_schema.columns),
        "policies": table_schema.policies,
    }))
}

fn query_result_json(db: &OpenDb, snapshot: QuerySnapshot) -> JsonValue {
    json!({
        "database": db.metadata_json(),
        "columns": columns_json(&snapshot.descriptor),
        "rowCount": snapshot.rows.len(),
        "rows": snapshot.rows.into_iter().map(|(id, values)| row_json(id, &values, &snapshot.descriptor)).collect::<Vec<_>>(),
    })
}

fn mutation_result_json(
    db: &OpenDb,
    action: &str,
    table: Option<&str>,
    row: Option<JsonValue>,
    object_id: Option<ObjectId>,
) -> JsonValue {
    json!({
        "database": db.metadata_json(),
        "action": action,
        "table": table,
        "ok": true,
        "objectId": object_id.map(|id| id.to_string()),
        "row": row,
    })
}

fn table_descriptor<'a>(schema: &'a Schema, table: &str) -> Result<&'a RowDescriptor, String> {
    schema
        .get(&TableName::new(table))
        .map(|table_schema| &table_schema.columns)
        .ok_or_else(|| format!("table '{table}' was not found in the selected schema"))
}

fn columns_json(descriptor: &RowDescriptor) -> Vec<JsonValue> {
    let output_keys = output_keys(descriptor);
    descriptor
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| column_json(column, index, &output_keys[index]))
        .collect()
}

fn column_json(column: &ColumnDescriptor, index: usize, output_key: &str) -> JsonValue {
    json!({
        "index": index,
        "name": column.name,
        "outputKey": output_key,
        "type": column.column_type,
        "nullable": column.nullable,
        "references": column.references,
        "default": column.default,
    })
}

fn output_keys(descriptor: &RowDescriptor) -> Vec<String> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    descriptor
        .columns
        .iter()
        .map(|column| {
            let base = column.name.as_str().to_string();
            let count = seen.entry(base.clone()).or_insert(0);
            *count += 1;
            if *count == 1 {
                base
            } else {
                format!("{base}__{count}")
            }
        })
        .collect()
}

pub(crate) fn row_json(object_id: ObjectId, values: &[Value], descriptor: &RowDescriptor) -> JsonValue {
    let output_keys = output_keys(descriptor);
    let cells: Vec<JsonValue> = values
        .iter()
        .zip(&descriptor.columns)
        .map(|(value, column)| value_json(value, Some(&column.column_type)))
        .collect();

    let mut record = JsonMap::new();
    for (key, cell) in output_keys.into_iter().zip(cells.iter().cloned()) {
        record.insert(key, cell);
    }

    json!({
        "id": object_id.to_string(),
        "cells": cells,
        "record": record,
    })
}

pub(crate) fn value_json(value: &Value, column_type: Option<&ColumnType>) -> JsonValue {
    match (value, column_type) {
        (Value::Integer(v), _) => json!(v),
        (Value::BigInt(v), _) => json!(v),
        (Value::Double(v), _) => json!(v),
        (Value::Boolean(v), _) => json!(v),
        (Value::Text(v), Some(ColumnType::Json { .. })) => {
            serde_json::from_str(v).unwrap_or_else(|_| json!(v))
        }
        (Value::Text(v), _) => json!(v),
        (Value::Timestamp(v), _) => json!(v),
        (Value::Uuid(v), _) => json!(v.to_string()),
        (Value::Bytea(bytes), _) => json!(base64::engine::general_purpose::STANDARD.encode(bytes)),
        (Value::Array(items), Some(ColumnType::Array { element })) => JsonValue::Array(
            items.iter()
                .map(|value| value_json(value, Some(element.as_ref())))
                .collect(),
        ),
        (Value::Array(items), _) => {
            JsonValue::Array(items.iter().map(|value| value_json(value, None)).collect())
        }
        (Value::Row { id, values }, Some(ColumnType::Row { columns })) => {
            let nested_cells: Vec<JsonValue> = values
                .iter()
                .zip(&columns.columns)
                .map(|(value, column)| value_json(value, Some(&column.column_type)))
                .collect();
            let mut record = JsonMap::new();
            for (key, cell) in output_keys(columns).into_iter().zip(nested_cells.iter().cloned()) {
                record.insert(key, cell);
            }
            json!({
                "id": id.map(|row_id| row_id.to_string()),
                "cells": nested_cells,
                "record": record,
            })
        }
        (Value::Row { id, values }, _) => json!({
            "id": id.map(|row_id| row_id.to_string()),
            "values": values.iter().map(|value| value_json(value, None)).collect::<Vec<_>>(),
        }),
        (Value::Null, _) => JsonValue::Null,
    }
}

fn syntax_json() -> JsonValue {
    json!({
        "overview": {
            "purpose": "Agent-friendly local Jazz database CLI with schema discovery, queries, and basic mutations.",
            "notes": [
                "All command outputs are JSON.",
                "The current CLI expects a persisted schema catalogue in storage.",
                "Query inputs use canonical Jazz query JSON.",
                "Insert and update inputs use Jazz Value JSON so column types are explicit.",
                "Result rows are returned with both ordered cells and a record object keyed by outputKey.",
                "When duplicate column names appear, later keys are suffixed as __2, __3, and so on."
            ]
        },
        "commands": {
            "listTables": "jazz-tools db list-tables --app-id <uuid> --data-dir <dir>",
            "describeTable": "jazz-tools db describe-table <table> --app-id <uuid> --data-dir <dir>",
            "query": "jazz-tools db query --app-id <uuid> --data-dir <dir> --json '<query-json>'",
            "insert": "jazz-tools db insert <table> --app-id <uuid> --data-dir <dir> --json '<values-json>'",
            "update": "jazz-tools db update <table> <object-id> --app-id <uuid> --data-dir <dir> --json '<values-json>'",
            "delete": "jazz-tools db delete <object-id> --app-id <uuid> --data-dir <dir>"
        },
        "examples": {
            "queryJson": {
                "table": "agent_runs",
                "branches": ["main"],
                "relation_ir": {
                    "Filter": {
                        "input": { "TableScan": { "table": "agent_runs" } },
                        "predicate": {
                            "Cmp": {
                                "left": { "column": "status" },
                                "op": "Eq",
                                "right": { "Literal": { "type": "Text", "value": "running" } }
                            }
                        }
                    }
                }
            },
            "insertValuesJson": {
                "name": { "type": "Text", "value": "review" },
                "enabled": { "type": "Boolean", "value": true }
            },
            "updateValuesJson": {
                "status": { "type": "Text", "value": "completed" },
                "finished_at": { "type": "Timestamp", "value": 1743072000000000u64 }
            }
        }
    })
}

pub(crate) fn print_json(value: &JsonValue) -> Result<(), String> {
    let rendered = serde_json::to_string_pretty(value)
        .map_err(|err| format!("failed to serialize JSON output: {err}"))?;
    println!("{rendered}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz_tools::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};

    #[test]
    fn duplicate_output_keys_are_disambiguated() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);

        assert_eq!(output_keys(&descriptor), vec!["id", "id__2", "name"]);
    }

    #[tokio::test]
    async fn open_db_lists_tables_and_queries_rows() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let data_dir = tempdir.path().to_string_lossy().to_string();
        let app_id = AppId::from_name("db-command-test");
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let storage = open_storage(&data_dir).expect("open storage");
        let schema_manager = SchemaManager::new(
            server_sync_manager(),
            schema.clone(),
            app_id,
            "dev",
            "main",
        )
        .expect("create schema manager");
        let runtime = TokioRuntime::new(schema_manager, storage, |_| {});

        runtime.persist_schema().expect("persist schema");
        let row_id = ObjectId::new();
        runtime
            .insert(
                "users",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(row_id)),
                    ("name".to_string(), Value::Text("Alice".to_string())),
                ]),
                None,
            )
            .expect("insert row");
        runtime
            .with_storage(|storage| {
                storage.flush();
                storage.close()
            })
            .expect("access storage")
            .expect("close storage");

        let args = DbContextArgs {
            app_id: app_id.to_string(),
            data_dir,
            env: "dev".to_string(),
            user_branch: "main".to_string(),
            schema_hash: None,
            schema_dir: None,
        };
        let db = open_db(&args).expect("open db command context");

        let tables = list_tables_json(&db);
        assert_eq!(tables["tables"][0]["name"], json!("users"));

        let query = Query::new("users");
        let snapshot = execute_query_snapshot(&db.runtime, query).expect("query snapshot");
        let result = query_result_json(&db, snapshot);
        assert_eq!(result["rowCount"], json!(1));
        assert_eq!(result["rows"][0]["record"]["name"], json!("Alice"));

        db.close().expect("close db command context");
    }
}
