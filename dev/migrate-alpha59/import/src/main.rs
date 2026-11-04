//! `sqlite-import`: build a SQLite-engine server data dir from an alpha.59
//! dump. See ../README.md.
//!
//! Everything goes through the new server's own code: the admin catalogue
//! is replayed as one deployment per permissions bundle (what
//! `POST /admin/deploy` runs), the account journal command by command, and
//! every stored row version through the public Db API, attributed to its
//! original author and timestamp. Nothing is written into the SQLite files
//! directly.

mod active;
mod links;
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
    /// Link each external identity's subject as a `createJazz` user id.
    link_backend_identities: bool,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let (mut dump, mut data_dir, mut app_id) = (None, None, None);
    let mut link_backend_identities = true;
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--dump" => dump = Some(PathBuf::from(value()?)),
            "--data-dir" => data_dir = Some(PathBuf::from(value()?)),
            "--app-id" => {
                app_id = Some(AppId::from_string(&value()?).map_err(|error| error.to_string())?)
            }
            "--no-backend-identity-links" => link_backend_identities = false,
            _ => return Err(format!("unknown argument {arg}")),
        }
    }
    Ok(Args {
        dump: dump.ok_or("--dump is required")?,
        data_dir: data_dir.ok_or("--data-dir (a new, empty dir) is required")?,
        app_id: app_id.ok_or("--app-id is required")?,
        link_backend_identities,
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
                "usage: sqlite-import [verify] --dump <dump> --data-dir <dir> --app-id <uuid> \
                 [--no-backend-identity-links]"
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
    runtime_lenses: Vec<dump::Lens>,
    active_schema: Option<[u8; 16]>,
    admin: Vec<dump::AdminEntry>,
    accounts: Vec<dump::Account>,
    /// Every principal the account journal names.
    principals: Vec<String>,
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
            dump::Record::Lens(lens) => head.runtime_lenses.push(lens),
            dump::Record::ActiveSchema { schema } => head.active_schema = Some(schema),
            dump::Record::AccountLogin { principal_json, .. } => {
                head.principals.push(principal_json)
            }
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
    let mut linked = 0;
    if args.link_backend_identities {
        let admitted = links::admitted(&state, &head.principals).await?;
        let planned = links::plan(&admitted)?;
        linked = links::apply(&state, &args.app_id.to_string(), &planned).await?;
    }

    let mut rows = RowImport::default();
    if let Some(first) = first_version {
        let runtime = state
            .runtime()
            .ok_or("the dump has rows but its catalogue never activated a schema")?;
        let to_head = active::ToHead::new(
            head.active_schema
                .ok_or("the dump has rows but no active schema")?,
            &head.runtime_schemas,
            std::mem::take(&mut head.runtime_lenses),
        )?;
        let active: Vec<_> = head
            .runtime_schemas
            .iter()
            .filter(|schema| schema.id == to_head.head)
            .cloned()
            .collect();
        let views = match_schemas(&active, runtime.migrate_import_schemas().await?)?;
        rows.to_head = Some(to_head);
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
        "backend_identities_linked": linked,
        "versions_in_dump": footer.versions,
        "versions_written": rows.written,
        "versions_skipped_rejected": rows.rejected,
        "versions_dropped_with_their_table": rows.table_gone,
        "shutdown": format!("{phase:?}"),
    }))
}

// ---- admin catalogue -------------------------------------------------------

/// Deploy the active schema with its permissions, as one fresh
/// `jazz-tools deploy` would. Old schemas, migrations and permission
/// revisions are left behind: the SQLite store starts from the active schema
/// alone, and every row is brought to it (see [`active`]).
async fn replay_admin_catalogue(
    state: &Arc<ServerState>,
    entries: &[dump::AdminEntry],
) -> Result<usize, String> {
    let Some(deployment) = active_deployment(entries)? else {
        return Ok(0);
    };
    migrate_import_deploy(state, deployment)
        .await
        .map_err(|error| format!("active schema: {error}"))?;
    Ok(1)
}

/// The deploy request for the admin catalogue's active schema, or `None`
/// when nothing was ever published.
pub(crate) fn active_deployment(entries: &[dump::AdminEntry]) -> Result<Option<Json>, String> {
    let payload = |entry: &dump::AdminEntry| -> Result<Json, String> {
        serde_json::from_str(&entry.payload_json).map_err(err)
    };
    for entry in entries {
        if !matches!(
            entry.object_type.as_str(),
            "catalogue_schema"
                | "catalogue_lens"
                | "catalogue_permissions_bundle"
                | "catalogue_permissions_head"
        ) {
            return Err(format!(
                "unknown admin catalogue entry type {}",
                entry.object_type
            ));
        }
    }
    let mut head: Option<Json> = None;
    for entry in entries
        .iter()
        .filter(|e| e.object_type == "catalogue_permissions_head")
    {
        let candidate = payload(entry)?;
        if head
            .as_ref()
            .is_none_or(|current| current["version"].as_u64() < candidate["version"].as_u64())
        {
            head = Some(candidate);
        }
    }
    let Some(head) = head else {
        if entries.iter().any(|e| e.object_type == "catalogue_schema") {
            return Err("schemas were published but never activated: not supported yet".to_owned());
        }
        return Ok(None);
    };
    let bundle = entries
        .iter()
        .find(|entry| {
            entry.object_type == "catalogue_permissions_bundle"
                && Some(uuid_string(entry.object_id).as_str()) == head["bundle_object_id"].as_str()
        })
        .ok_or("the active permissions bundle is missing from the dump")?;
    let bundle = payload(bundle)?;
    let schema = entries
        .iter()
        .find(|entry| {
            entry.object_type == "catalogue_schema"
                && entry.metadata.get("schema_hash").map(String::as_str)
                    == bundle["schema_hash"].as_str()
        })
        .ok_or("the active schema is missing from the dump")?;
    let schema = payload(schema)?;
    let hash = schema_hash(&schema)?;
    Ok(Some(json!({
        "targetSchemaHash": hash,
        "schemas": [{ "hash": hash, "schema": schema }],
        "migrations": [],
        "permissions": bundle["permissions"],
    })))
}

/// A schema's hash as the SQLite engine computes it. alpha.59's catalogue
/// keys schemas by its own hash function, which need not be the same.
pub(crate) fn schema_hash(schema: &Json) -> Result<String, String> {
    let schema: jazz::tools::public_schema::Schema =
        serde_json::from_value(schema.clone()).map_err(|error| format!("schema: {error}"))?;
    Ok(jazz::tools::public_schema::SchemaHash::compute(&schema).to_string())
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
    to_head: Option<active::ToHead>,
    seen: HashSet<[u8; 16]>,
    written: u64,
    rejected: u64,
    table_gone: u64,
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
            let to_head = self.to_head.as_ref().ok_or("rows before the catalogue")?;
            let version = match to_head.translate(version)? {
                active::Translated::Kept(version) => version,
                active::Translated::TableGone => {
                    self.table_gone += 1;
                    continue;
                }
            };
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
