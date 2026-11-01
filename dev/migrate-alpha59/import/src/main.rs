//! `sqlite-import`: build a SQLite-engine server data dir from an alpha.59
//! dump. See ../README.md.
//!
//! Everything goes through the new server's own code: the admin catalogue
//! is replayed as one deployment per permissions bundle (what
//! `POST /admin/deploy` runs), the account journal command by command, and
//! every stored row version through the public Db API, attributed to its
//! original author and timestamp. Nothing is written into the SQLite files
//! directly.

mod verify;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use jazz::account_registry::AccountId;
use jazz::db::{
    Db, DeleteOptions, InsertOptions, RestoreOptions, UpdateOptions, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::AppId;
use jazz_server::server::migrate_import::{migrate_import_account, migrate_import_deploy};
use jazz_server::{ServerBuilder, ServerState, StorageBackend};
use migrate_alpha59_dump as dump;
use serde_json::{Value as Json, json};

/// Row versions written per owner-thread job.
const BATCH: usize = 2_000;

struct Args {
    dump: PathBuf,
    data_dir: PathBuf,
    app_id: AppId,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let (mut dump, mut data_dir, mut app_id) = (None, None, None);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--dump" => dump = Some(PathBuf::from(value()?)),
            "--data-dir" => data_dir = Some(PathBuf::from(value()?)),
            "--app-id" => {
                app_id = Some(AppId::from_string(&value()?).map_err(|error| error.to_string())?)
            }
            _ => return Err(format!("unknown argument {arg}")),
        }
    }
    Ok(Args {
        dump: dump.ok_or("--dump is required")?,
        data_dir: data_dir.ok_or("--data-dir (a new, empty dir) is required")?,
        app_id: app_id.ok_or("--app-id is required")?,
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1).peekable();
    let verifying = args.peek().is_some_and(|arg| arg == "verify");
    if verifying {
        args.next();
    }
    let args = match parse_args(args) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("sqlite-import: {error}");
            eprintln!(
                "usage: sqlite-import [verify] --dump <dump> --data-dir <dir> --app-id <uuid>"
            );
            return ExitCode::from(2);
        }
    };
    let result = if verifying {
        verify::verify(&args).await
    } else {
        run(&args).await
    };
    match result {
        Ok(summary) => {
            println!("{summary}");
            if summary["ok"] == Json::Bool(false) {
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("sqlite-import: {error}");
            ExitCode::FAILURE
        }
    }
}

/// The catalogue and accounts, which a dump carries before any row.
#[derive(Default)]
struct Head {
    runtime_schemas: Vec<dump::Schema>,
    admin: Vec<dump::AdminEntry>,
    accounts: Vec<dump::Account>,
}

async fn run(args: &Args) -> Result<Json, String> {
    if args.data_dir.exists()
        && std::fs::read_dir(&args.data_dir)
            .map_err(err)?
            .next()
            .is_some()
    {
        return Err(format!("{} is not empty", args.data_dir.display()));
    }
    let file = std::fs::File::open(&args.dump).map_err(err)?;
    let (mut reader, header) = dump::Reader::open(std::io::BufReader::new(file)).map_err(err)?;
    if header.format != dump::DUMP_FORMAT {
        return Err(format!(
            "dump format {} is not {}",
            header.format,
            dump::DUMP_FORMAT
        ));
    }

    let mut head = Head::default();
    let mut first_version = None;
    while let Some(record) = reader.next_record().map_err(err)? {
        match record {
            dump::Record::Schema(schema) => head.runtime_schemas.push(schema),
            dump::Record::AdminEntry(entry) => head.admin.push(entry),
            dump::Record::Account(account) => head.accounts.push(account),
            dump::Record::Lens(_)
            | dump::Record::ActiveSchema { .. }
            | dump::Record::AccountLogin { .. } => {}
            dump::Record::Version(version) => {
                first_version = Some(version);
                break;
            }
            other => return Err(format!("unexpected record {other:?}")),
        }
    }

    let built = ServerBuilder::new(args.app_id)
        .with_storage(StorageBackend::Persistent {
            path: args.data_dir.clone(),
        })
        .build()
        .await?;
    let state = built.state.clone();
    let deployments = replay_admin_catalogue(&state, &head.admin).await?;
    for account in &head.accounts {
        migrate_import_account(&state, &account.command_json)
            .await
            .map_err(|error| format!("account command {}: {error}", account.revision))?;
    }

    let mut rows = RowImport::default();
    if let Some(first) = first_version {
        let runtime = state
            .runtime()
            .ok_or("the dump has rows but its catalogue never activated a schema")?;
        let views = match_schemas(
            &head.runtime_schemas,
            runtime.migrate_import_schemas().await?,
        )?;
        let mut batch = vec![first];
        while let Some(record) = reader.next_record().map_err(err)? {
            match record {
                dump::Record::Version(version) => batch.push(version),
                other => return Err(format!("unexpected record after rows: {other:?}")),
            }
            if batch.len() == BATCH {
                rows.write(&runtime, &views, std::mem::take(&mut batch))
                    .await?;
            }
        }
        rows.write(&runtime, &views, batch).await?;
        runtime
            .migrate_import_with_db(|db| {
                for _ in 0..4 {
                    block_on(db.tick()).map_err(err)?;
                }
                Ok(())
            })
            .await?;
    }
    let footer = reader.footer().clone();

    let phase = built.shutdown().await;
    Ok(json!({
        "data_dir": args.data_dir,
        "deployments": deployments,
        "account_commands": head.accounts.len(),
        "versions_in_dump": footer.versions,
        "versions_written": rows.written,
        "versions_skipped_rejected": rows.rejected,
        "shutdown": format!("{phase:?}"),
    }))
}

// ---- admin catalogue -------------------------------------------------------

/// Replay the permissions history as deployments, oldest first. Each one
/// carries every schema and migration, and activates one bundle's schema
/// with its permissions, as `jazz-tools deploy` would have.
async fn replay_admin_catalogue(
    state: &Arc<ServerState>,
    entries: &[dump::AdminEntry],
) -> Result<usize, String> {
    let mut schemas = Vec::new();
    let mut migrations = Vec::new();
    let mut bundles: HashMap<String, Json> = HashMap::new();
    let mut head: Option<Json> = None;
    for entry in entries {
        let payload: Json = serde_json::from_str(&entry.payload_json).map_err(err)?;
        let meta = |key: &str| {
            entry
                .metadata
                .get(key)
                .cloned()
                .ok_or(format!("{} entry is missing {key}", entry.object_type))
        };
        match entry.object_type.as_str() {
            "catalogue_schema" => {
                schemas.push(json!({ "hash": meta("schema_hash")?, "schema": payload }));
            }
            "catalogue_lens" => {
                if payload["draft_ops"]
                    .as_array()
                    .is_some_and(|ops| !ops.is_empty())
                {
                    // alpha.59 never put a draft lens on its migration graph.
                    continue;
                }
                migrations.push(migration_request(
                    meta("source_hash")?,
                    meta("target_hash")?,
                    &payload,
                )?);
            }
            "catalogue_permissions_bundle" => {
                bundles.insert(uuid_string(entry.object_id), payload);
            }
            "catalogue_permissions_head" => {
                let newer = head.as_ref().is_none_or(|current| {
                    current["version"].as_u64() < payload["version"].as_u64()
                });
                if newer {
                    head = Some(payload);
                }
            }
            other => return Err(format!("unknown admin catalogue entry type {other}")),
        }
    }
    let Some(head) = head else {
        if schemas.is_empty() {
            return Ok(0);
        }
        return Err("schemas were published but never activated: not supported yet".to_owned());
    };
    // The head's ancestry, oldest first. A bundle off that chain lost a
    // compare-and-set race and was never active.
    let mut chain = Vec::new();
    let mut next = head["bundle_object_id"].as_str().map(str::to_owned);
    while let Some(id) = next {
        let bundle = bundles
            .get(&id)
            .ok_or(format!("permissions bundle {id} is missing"))?;
        next = bundle["parent_bundle_object_id"]
            .as_str()
            .map(str::to_owned);
        chain.push(bundle.clone());
    }
    chain.reverse();
    for bundle in &chain {
        migrate_import_deploy(
            state,
            json!({
                "targetSchemaHash": bundle["schema_hash"],
                "schemas": schemas,
                "migrations": migrations,
                "permissions": bundle["permissions"],
            }),
        )
        .await
        .map_err(|error| format!("permissions bundle v{}: {error}", bundle["version"]))?;
    }
    Ok(chain.len())
}

/// alpha.59's admin `LensTransform` back into the `POST /admin/migrations`
/// request that produced it: ops grouped by the table they name.
fn migration_request(from: String, to: String, transform: &Json) -> Result<Json, String> {
    let mut tables: Vec<(String, Json)> = Vec::new();
    fn entry<'a>(tables: &'a mut Vec<(String, Json)>, name: &str) -> &'a mut Json {
        let index = match tables.iter().position(|(table, _)| table == name) {
            Some(index) => index,
            None => {
                tables.push((
                    name.to_owned(),
                    json!({ "table": name, "added": false, "removed": false, "renamedFrom": null, "operations": [] }),
                ));
                tables.len() - 1
            }
        };
        &mut tables[index].1
    }
    let mut ops = Vec::new();
    for op in transform["ops"].as_array().into_iter().flatten() {
        let (kind, body) = op
            .as_object()
            .and_then(|op| op.iter().next())
            .ok_or(format!("unreadable lens op {op}"))?;
        ops.push((kind.clone(), body.clone()));
    }
    for (kind, body) in ops {
        let name = |key: &str| body[key].as_str().unwrap_or_default().to_owned();
        match kind.as_str() {
            "RenameTable" => {
                entry(&mut tables, &name("new_name"))["renamedFrom"] = json!(name("old_name"));
            }
            "AddTable" => {
                entry(&mut tables, &name("table"))["added"] = json!(true);
            }
            "RemoveTable" => {
                entry(&mut tables, &name("table"))["removed"] = json!(true);
            }
            "AddColumn" | "RemoveColumn" => {
                entry(&mut tables, &name("table"))["operations"]
                    .as_array_mut()
                    .expect("operations")
                    .push(json!({
                        "type": if kind == "AddColumn" { "introduce" } else { "drop" },
                        "column": body["column"],
                        "column_type": body["column_type"],
                        "value": body["default"],
                    }));
            }
            "RenameColumn" => {
                entry(&mut tables, &name("table"))["operations"]
                    .as_array_mut()
                    .expect("operations")
                    .push(json!({ "type": "rename", "column": body["old_name"], "value": body["new_name"] }));
            }
            other => return Err(format!("admin lens op {other} is not supported yet")),
        }
    }
    Ok(json!({
        "fromHash": from,
        "toHash": to,
        "forward": tables.into_iter().map(|(_, table)| table).collect::<Vec<_>>(),
    }))
}

// ---- rows --------------------------------------------------------------------

/// alpha.59 runtime schema id -> the SQLite runtime's schema with the same
/// tables and columns. Policies are left out of the comparison: the
/// deployments above re-attached them.
fn match_schemas(
    old: &[dump::Schema],
    new: Vec<JazzSchema>,
) -> Result<HashMap<[u8; 16], JazzSchema>, String> {
    fn structural(mut schema: Json) -> Json {
        if let Some(tables) = schema.get_mut("tables").and_then(Json::as_object_mut) {
            for table in tables.values_mut() {
                if let Some(table) = table.as_object_mut() {
                    table.remove("policies");
                }
            }
        }
        schema
    }
    let new: Vec<(Json, JazzSchema)> = new
        .into_iter()
        .map(|schema| {
            let json = serde_json::to_value(schema.public_schema()).map_err(err)?;
            Ok((structural(json), schema))
        })
        .collect::<Result<_, String>>()?;
    let mut out = HashMap::new();
    for schema in old {
        let json = structural(serde_json::from_str(&schema.public_schema_json).map_err(err)?);
        let found = new
            .iter()
            .find(|(candidate, _)| *candidate == json)
            .ok_or(format!(
                "alpha.59 schema {} has no counterpart in the SQLite runtime catalogue",
                uuid_string(schema.id)
            ))?;
        out.insert(schema.id, found.1.clone());
    }
    Ok(out)
}

enum Op {
    Insert,
    Update,
    Delete,
    Restore,
}

struct Write {
    op: Op,
    schema: JazzSchema,
    table: String,
    row: RowUuid,
    author: AuthorSubject,
    at_ms: u64,
    cells: BTreeMap<String, Value>,
}

#[derive(Default)]
struct RowImport {
    seen: HashSet<[u8; 16]>,
    written: u64,
    rejected: u64,
}

impl RowImport {
    async fn write(
        &mut self,
        runtime: &jazz_server::runtime::ServerRuntimeHandle,
        views: &HashMap<[u8; 16], JazzSchema>,
        versions: Vec<dump::Version>,
    ) -> Result<(), String> {
        let mut writes = Vec::with_capacity(versions.len());
        for version in versions {
            if version.fate.starts_with("rejected") {
                self.rejected += 1;
                continue;
            }
            if !version.branch.is_empty() {
                return Err(format!(
                    "row {} of {} is on a branch: branched rows are not supported yet",
                    uuid_string(version.row),
                    version.table
                ));
            }
            let op = match version.deletion {
                Some(dump::Deletion::Deleted) => Op::Delete,
                Some(dump::Deletion::Restored) => Op::Restore,
                None if self.seen.insert(version.row) => Op::Insert,
                None => Op::Update,
            };
            let mut cells = BTreeMap::new();
            for (name, value) in version.cells {
                cells.insert(name, convert_value(value));
            }
            writes.push(Write {
                op,
                schema: views
                    .get(&version.schema)
                    .ok_or(format!(
                        "version under unknown schema {}",
                        uuid_string(version.schema)
                    ))?
                    .clone(),
                table: version.table,
                row: RowUuid::from_bytes(version.row),
                author: convert_author(&version.updated_by)?,
                at_ms: dump::Version::millis(version.updated_at),
                cells,
            });
        }
        self.written += writes.len() as u64;
        runtime
            .migrate_import_with_db(move |db| {
                let mut views: HashMap<jazz::ids::SchemaVersionId, Db> = HashMap::new();
                for write in writes {
                    let id = write.schema.version_id();
                    if !views.contains_key(&id) {
                        let view =
                            block_on(db.register_schema_view(write.schema.clone())).map_err(err)?;
                        views.insert(id, view);
                    }
                    let view = &views[&id];
                    let identity = WriteIdentity::Attribution(write.author);
                    let at = Some(write.at_ms);
                    let (table, row) = (write.table.as_str(), write.row);
                    let result = match write.op {
                        Op::Insert => block_on(view.insert(
                            table,
                            write.cells,
                            InsertOptions {
                                row_id: Some(row),
                                identity,
                                updated_at_ms: at,
                                ..Default::default()
                            },
                        )),
                        Op::Update => block_on(view.update(
                            table,
                            row,
                            write.cells,
                            UpdateOptions {
                                identity,
                                updated_at_ms: at,
                                ..Default::default()
                            },
                        )),
                        Op::Delete => block_on(view.delete(
                            table,
                            row,
                            DeleteOptions {
                                identity,
                                updated_at_ms: at,
                                ..Default::default()
                            },
                        )),
                        Op::Restore => block_on(view.restore(
                            table,
                            row,
                            None,
                            RestoreOptions {
                                identity,
                                updated_at_ms: at,
                                ..Default::default()
                            },
                        )),
                    };
                    result.map_err(|error| format!("{table} row {row:?}: {error}"))?;
                }
                Ok(())
            })
            .await
    }
}

fn convert_author(author: &dump::Author) -> Result<AuthorSubject, String> {
    Ok(match author {
        dump::Author::System => AuthorSubject::SYSTEM,
        dump::Author::SystemAt { subject, .. } => AuthorSubject::system_at(NodeUuid::from_bytes(
            *uuid::Uuid::parse_str(subject).map_err(err)?.as_bytes(),
        )),
        dump::Author::Authenticated {
            account,
            issuer,
            subject,
        } => {
            let author = AuthorSubject::authenticated(issuer, subject).map_err(err)?;
            match account {
                Some(account) => author.with_account(AccountId(uuid::Uuid::from_bytes(*account))),
                None => author,
            }
        }
    })
}

fn convert_value(value: dump::Value) -> Value {
    let many = |values: Vec<dump::Value>| values.into_iter().map(convert_value).collect();
    match value {
        dump::Value::U8(v) => Value::U8(v),
        dump::Value::U16(v) => Value::U16(v),
        dump::Value::U32(v) => Value::U32(v),
        dump::Value::U64(v) => Value::U64(v),
        dump::Value::F64(v) => Value::F64(v),
        dump::Value::Bool(v) => Value::Bool(v),
        dump::Value::String(v) => Value::String(v),
        dump::Value::Bytes(v) => Value::Bytes(v.into_vec()),
        dump::Value::Uuid(v) => Value::Uuid(uuid::Uuid::from_bytes(v)),
        dump::Value::EnumTag(v) => Value::EnumTag(v),
        dump::Value::Tuple(values) => Value::Tuple(many(values)),
        dump::Value::Array(values) => Value::Array(many(values)),
        dump::Value::Nullable(None) => Value::Nullable(None),
        dump::Value::Nullable(Some(inner)) => {
            Value::Nullable(Some(Box::new(convert_value(*inner))))
        }
        dump::Value::I64(v) => Value::I64(v),
        dump::Value::I32(v) => Value::I32(v),
    }
}

fn uuid_string(bytes: [u8; 16]) -> String {
    uuid::Uuid::from_bytes(bytes).to_string()
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
