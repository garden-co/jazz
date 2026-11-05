//! `jazz-tools migrate-alpha59`: build a new server data dir from a dump of
//! an alpha.59 server data dir, and verify it. The dump comes from
//! `jazz-alpha59-export`, a separate binary built against alpha.59 (see
//! `dev/migrate-alpha59/`).
//!
//! Everything goes through the new server's own code: the active schema and
//! its permissions as one deployment, the account journal command by
//! command, and every stored row version through the public Db API,
//! attributed to its original author and timestamp. Nothing is written into
//! the SQLite files directly.
//!
//! Self-hosters moving off alpha.59 need it; it stays until they no longer
//! do.

mod active;
mod links;
mod replay;
mod verify;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use jazz::account_registry::AccountId;
use jazz::db::{
    Db, DeleteOptions, InsertOptions, RestoreOptions, UpdateOptions, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::AppId;
use jazz_alpha59_dump as dump;
use jazz_server::server::migrate_alpha59::{migrate_import_account, migrate_import_deploy};
use jazz_server::{ServerBuilder, ServerState, StorageBackend};
use serde_json::{Value as Json, json};

/// Row versions written per owner-thread job.
const BATCH: usize = 2_000;

struct Args {
    dump: PathBuf,
    data_dir: PathBuf,
    app_id: AppId,
    /// `--app-id` exactly as given, kept in `<data-dir>/app-id` so that
    /// `sqlite({ path })` runs the imported app without naming its id.
    app_id_text: String,
    /// Link each external identity's subject as a `createJazz` user id.
    link_backend_identities: bool,
    /// Import versions whose transaction carried user metadata, without it:
    /// the SQLite engine has no transaction metadata.
    drop_transaction_metadata: bool,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let (mut dump, mut data_dir, mut app_id) = (None, None, None::<(AppId, String)>);
    let mut link_backend_identities = true;
    let mut drop_transaction_metadata = false;
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--dump" => dump = Some(PathBuf::from(value()?)),
            "--data-dir" => data_dir = Some(PathBuf::from(value()?)),
            "--app-id" => {
                let text = value()?;
                let id = AppId::from_string(&text).map_err(|error| error.to_string())?;
                app_id = Some((id, text));
            }
            "--no-backend-identity-links" => link_backend_identities = false,
            "--drop-transaction-metadata" => drop_transaction_metadata = true,
            _ => return Err(format!("unknown argument {arg}")),
        }
    }
    Ok(Args {
        dump: dump.ok_or("--dump is required")?,
        data_dir: data_dir.ok_or("--data-dir (a new, empty dir) is required")?,
        app_id: app_id.as_ref().ok_or("--app-id is required")?.0,
        app_id_text: app_id.ok_or("--app-id is required")?.1,
        link_backend_identities,
        drop_transaction_metadata,
    })
}

const USAGE: &str = "usage: jazz-tools migrate-alpha59 (import | verify) --dump <dump> \
    --data-dir <dir> --app-id <app id> [--no-backend-identity-links] [--drop-transaction-metadata]";

/// Run `jazz-tools migrate-alpha59 <args>`; the process exit code.
pub async fn main(args: Vec<String>) -> i32 {
    let mut args = args.into_iter();
    let verifying = match args.next().as_deref() {
        Some("import") => false,
        Some("verify") => true,
        _ => {
            eprintln!("{USAGE}");
            return 2;
        }
    };
    let args = match parse_args(args) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("jazz-tools migrate-alpha59: {error}");
            eprintln!("{USAGE}");
            return 2;
        }
    };
    let result = if verifying {
        verify::verify(&args).await
    } else {
        import(&args).await
    };
    match result {
        Ok(summary) => {
            println!("{summary}");
            if summary["ok"] == Json::Bool(false) {
                return 1;
            }
            0
        }
        Err(error) => {
            eprintln!("jazz-tools migrate-alpha59: {error}");
            1
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
    /// What alpha.59 reads as each row's current state, keyed once the
    /// active schema is known.
    currents: Vec<dump::Current>,
}

/// A row: the active-schema table it lands in, and its id. A row id names a
/// row within one table, so the same id can occur in several.
pub(crate) type RowKey = (String, [u8; 16]);

/// alpha.59's current state of each row, by the active-schema row it lands
/// in. Rows whose table the active schema dropped are left out.
pub(crate) fn keyed_currents(
    to_head: &active::ToHead,
    currents: Vec<dump::Current>,
) -> Result<HashMap<RowKey, dump::Current>, String> {
    let mut keyed: HashMap<RowKey, dump::Current> = HashMap::new();
    for current in currents {
        let Some(table) = to_head.head_table(current.schema, &current.table)? else {
            continue;
        };
        match keyed.entry((table, current.row)) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(current);
            }
            std::collections::hash_map::Entry::Occupied(entry) => {
                let held = entry.get();
                if (&held.content, &held.deletion, &held.created)
                    != (&current.content, &current.deletion, &current.created)
                {
                    return Err(format!(
                        "{} row {} has two current states in alpha.59, under two schemas: \
                         not supported yet",
                        entry.key().0,
                        uuid_string(current.row)
                    ));
                }
            }
        }
    }
    Ok(keyed)
}

/// Import into a new, empty data dir; a failed import removes what it built,
/// so a half-built dir is never left to boot. A dir that existed beforehand
/// (a mount point, say) is kept, emptied.
async fn import(args: &Args) -> Result<Json, String> {
    let existed = args.data_dir.exists();
    if existed
        && std::fs::read_dir(&args.data_dir)
            .map_err(err)?
            .next()
            .is_some()
    {
        return Err(format!("{} is not empty", args.data_dir.display()));
    }
    check_dump(args)?;
    let result = run(args).await;
    if result.is_err()
        && args.data_dir.exists()
        && let Err(error) = remove_built(&args.data_dir, existed)
    {
        eprintln!(
            "jazz-tools migrate-alpha59: could not remove the half-built {}: {error}",
            args.data_dir.display()
        );
    }
    result
}

fn remove_built(dir: &std::path::Path, keep_dir: bool) -> std::io::Result<()> {
    if !keep_dir {
        return std::fs::remove_dir_all(dir);
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Read the whole dump once, so its digest and counts are checked before
/// anything is built from it.
fn check_dump(args: &Args) -> Result<(), String> {
    let file = std::fs::File::open(&args.dump).map_err(err)?;
    let (mut reader, _) = dump::Reader::open(std::io::BufReader::new(file)).map_err(err)?;
    while reader.next_record().map_err(err)?.is_some() {}
    Ok(())
}

async fn run(args: &Args) -> Result<Json, String> {
    let file = std::fs::File::open(&args.dump).map_err(err)?;
    // The reader refuses another dump format, naming the exporter to use.
    let (mut reader, _) = dump::Reader::open(std::io::BufReader::new(file)).map_err(err)?;

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
            // For verify.
            dump::Record::Current(current) => head.currents.push(current),
            dump::Record::Version(version) => {
                first_version = Some(version);
                break;
            }
            other => return Err(format!("unexpected {} record", other.kind())),
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
        let planned = links::plan(&links::identities(&head.accounts, &head.principals)?)?;
        linked = links::apply(&state, &planned).await?;
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
        let views: HashMap<[u8; 16], Arc<JazzSchema>> =
            match_schemas(&active, runtime.migrate_import_schemas().await?)?
                .into_iter()
                .map(|(id, schema)| (id, Arc::new(schema)))
                .collect();
        let currents = keyed_currents(&to_head, std::mem::take(&mut head.currents))?;
        rows.creations = currents
            .iter()
            .filter_map(|(key, current)| Some((key.clone(), current.created.clone()?)))
            .collect();
        rows.to_head = Some(to_head);
        rows.drop_transaction_metadata = args.drop_transaction_metadata;
        let mut batch = vec![first];
        while let Some(record) = reader.next_record().map_err(err)? {
            match record {
                dump::Record::Version(version) => batch.push(version),
                other => return Err(format!("unexpected {} record after rows", other.kind())),
            }
            if batch.len() == BATCH {
                rows.write(&runtime, &views, std::mem::take(&mut batch))
                    .await?;
            }
        }
        rows.write(&runtime, &views, batch).await?;
        rows.flush(&runtime).await?;
        rows.check_against(&currents)?;
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
    // `sqlite({ path })` keeps its app id here: the store runs as the
    // alpha.59 app, so its guests' and users' accounts stay theirs, without
    // the app naming the id.
    std::fs::write(
        args.data_dir.join("app-id"),
        format!("{}\n", args.app_id_text),
    )
    .map_err(err)?;
    Ok(json!({
        "data_dir": args.data_dir,
        "deployments": deployments,
        "account_commands": head.accounts.len(),
        "backend_identities_linked": linked,
        "versions_in_dump": footer.versions,
        "versions_written": rows.written,
        "versions_skipped_rejected": rows.rejected,
        "versions_dropped_with_their_table": rows.table_gone,
        "deletion_events_coalesced": rows.rows.coalesced,
        "inserts_with_their_own_creation": rows.created_apart,
        "versions_settled_after_a_newer_one": rows.rows.superseded,
        "versions_written_while_deleted": rows.rows.written_while_deleted,
        "restores_bringing_content_written_while_deleted": rows.rows.restores_with_hidden,
        "deleted_rows_dropping_content_written_while_deleted": rows.rows.rows_dropping_hidden,
        "transaction_metadata_dropped": rows.metadata_dropped,
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
    schema: Arc<JazzSchema>,
    table: String,
    row: RowUuid,
    author: AuthorSubject,
    at_ms: u64,
    cells: BTreeMap<String, Value>,
}

#[derive(Default)]
struct RowImport {
    to_head: Option<active::ToHead>,
    drop_transaction_metadata: bool,
    seen: HashSet<RowKey>,
    /// Per row, the last content and the last deletion transaction written.
    last: HashMap<RowKey, (Option<dump::TxRef>, Option<dump::TxRef>)>,
    /// Deletion events and content written under a deletion, held until
    /// the store can take them (see [`replay`]).
    rows: replay::RowReplay<Write, Write>,
    /// Per row, its creation as alpha.59 reads it.
    creations: HashMap<RowKey, (dump::Author, u64)>,
    written: u64,
    rejected: u64,
    table_gone: u64,
    metadata_dropped: u64,
    created_apart: u64,
}

impl RowImport {
    /// The replay leaves each row with the content and deletion that
    /// alpha.59 itself reads as current; refuse the import otherwise.
    fn check_against(&self, currents: &HashMap<RowKey, dump::Current>) -> Result<(), String> {
        let mut differing = 0;
        let mut first = None;
        for (key, current) in currents {
            let written = self.last.get(key).cloned().unwrap_or_default();
            if written != (current.content, current.deletion) {
                differing += 1;
                first.get_or_insert(key);
            }
        }
        for key in self.last.keys() {
            if !currents.contains_key(key) {
                differing += 1;
                first.get_or_insert(key);
            }
        }
        match first {
            None => Ok(()),
            Some((table, row)) => Err(format!(
                "{differing} rows end in another state than alpha.59 reads for them (first: {table} row {})",
                uuid_string(*row)
            )),
        }
    }

    /// Write every deletion event still held.
    async fn flush(
        &mut self,
        runtime: &jazz_server::runtime::ServerRuntimeHandle,
    ) -> Result<(), String> {
        let mut writes = self.plan_flush();
        while !writes.is_empty() {
            let rest = writes.split_off(writes.len().min(BATCH));
            self.apply(runtime, writes).await?;
            writes = rest;
        }
        Ok(())
    }

    fn plan_flush(&mut self) -> Vec<Write> {
        let mut released = Vec::new();
        self.rows.finish(&mut released);
        released.into_iter().map(Self::event_write).collect()
    }

    /// The write for a held deletion event the store takes.
    fn event_write(released: replay::Released<Write, Write>) -> Write {
        let mut write = released.event;
        if let Some(content) = released.content {
            write.schema = content.schema;
            write.cells = content.cells;
        }
        write
    }

    async fn write(
        &mut self,
        runtime: &jazz_server::runtime::ServerRuntimeHandle,
        views: &HashMap<[u8; 16], Arc<JazzSchema>>,
        versions: Vec<dump::Version>,
    ) -> Result<(), String> {
        let writes = self.plan(views, versions)?;
        self.apply(runtime, writes).await
    }

    /// The writes that replay `versions`, in order.
    fn plan(
        &mut self,
        views: &HashMap<[u8; 16], Arc<JazzSchema>>,
        versions: Vec<dump::Version>,
    ) -> Result<Vec<Write>, String> {
        let mut writes = Vec::with_capacity(versions.len());
        for version in versions {
            if version.fate.starts_with("rejected") {
                self.rejected += 1;
                continue;
            }
            if version.fate != "accepted" {
                return Err(format!(
                    "row {} of {} has a {} version: only settled versions are supported",
                    uuid_string(version.row),
                    version.table,
                    version.fate
                ));
            }
            if version.user_metadata_json.is_some() {
                if !self.drop_transaction_metadata {
                    return Err(format!(
                        "row {} of {} was written with transaction metadata, which the SQLite \
                         engine cannot store; pass --drop-transaction-metadata to import without it",
                        uuid_string(version.row),
                        version.table
                    ));
                }
                self.metadata_dropped += 1;
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
            let key: RowKey = (version.table.clone(), version.row);
            let tx = dump::TxRef {
                node: version.tx_node,
                time: version.tx_time,
            };
            if !self.rows.wins(&key, &version) {
                continue;
            }
            let last = self.last.entry(key.clone()).or_default();
            if version.deletion.is_none() {
                last.0 = Some(tx);
            } else {
                last.1 = Some(tx);
            }
            let schema = views
                .get(&version.schema)
                .ok_or(format!(
                    "version under unknown schema {}",
                    uuid_string(version.schema)
                ))?
                .clone();
            let write = |op, author: &dump::Author, at: u64, cells| -> Result<Write, String> {
                Ok(Write {
                    op,
                    schema: schema.clone(),
                    table: version.table.clone(),
                    row: RowUuid::from_bytes(version.row),
                    author: convert_author(author)?,
                    at_ms: dump::Version::millis(at),
                    cells,
                })
            };
            let mut released = Vec::new();
            match version.deletion {
                Some(deletion) => {
                    if !self.seen.contains(&key) {
                        return Err(format!(
                            "row {} of {} has a deletion event before any content: not \
                             supported yet",
                            uuid_string(version.row),
                            version.table
                        ));
                    }
                    let (op, deleting) = match deletion {
                        dump::Deletion::Deleted => (Op::Delete, true),
                        dump::Deletion::Restored => (Op::Restore, false),
                    };
                    let event =
                        write(op, &version.updated_by, version.updated_at, BTreeMap::new())?;
                    self.rows.event(&key, deleting, event, &mut released);
                    writes.extend(released.into_iter().map(Self::event_write));
                }
                None => {
                    let cells = full_cells(&schema, &version.table, version.cells.clone())?;
                    let content =
                        write(Op::Update, &version.updated_by, version.updated_at, cells)?;
                    let content = self.rows.content(&key, content, &mut released);
                    writes.extend(released.into_iter().map(Self::event_write));
                    let Some(Write { cells, .. }) = content else {
                        continue;
                    };
                    if !self.seen.contains(&key) {
                        // alpha.59 reads a row's creation off its content
                        // winner, which takes it from whichever version won
                        // when it was committed: not always the first one in
                        // commit order (two clients inserting one row, say).
                        // The insert carries that creation; an update then
                        // carries this version's own authorship.
                        let (created_by, created_at) = self
                            .creations
                            .get(&key)
                            .cloned()
                            .unwrap_or((version.created_by.clone(), version.created_at));
                        self.seen.insert(key);
                        let created = (&created_by, dump::Version::millis(created_at));
                        let updated = (
                            &version.updated_by,
                            dump::Version::millis(version.updated_at),
                        );
                        writes.push(write(Op::Insert, &created_by, created_at, cells.clone())?);
                        if created != updated {
                            self.created_apart += 1;
                            writes.push(write(
                                Op::Update,
                                &version.updated_by,
                                version.updated_at,
                                cells,
                            )?);
                        }
                    } else {
                        writes.push(write(
                            Op::Update,
                            &version.updated_by,
                            version.updated_at,
                            cells,
                        )?);
                    }
                }
            }
        }
        Ok(writes)
    }

    async fn apply(
        &mut self,
        runtime: &jazz_server::runtime::ServerRuntimeHandle,
        writes: Vec<Write>,
    ) -> Result<(), String> {
        self.written += writes.len() as u64;
        runtime
            .migrate_import_with_db(move |db| {
                // alpha.59 keeps a variant a later schema removed in rows
                // written before; the store keeps it too.
                db.keep_undeclared_variants_for_import();
                let mut views: HashMap<jazz::ids::SchemaVersionId, Db> = HashMap::new();
                for write in writes {
                    let id = write.schema.version_id();
                    let view = match views.entry(id) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::hash_map::Entry::Vacant(entry) => entry.insert(
                            block_on(db.register_schema_view((*write.schema).clone()))
                                .map_err(err)?,
                        ),
                    };
                    let view = &*view;
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
                            (!write.cells.is_empty()).then_some(write.cells),
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

/// Every column of `table`, as alpha.59 stored them: an absent cell is a
/// null, written as one so an update clears it rather than keeping the old
/// value.
fn full_cells(
    schema: &JazzSchema,
    table: &str,
    cells: BTreeMap<String, dump::Value>,
) -> Result<BTreeMap<String, Value>, String> {
    let columns = &schema
        .tables()
        .iter()
        .find(|candidate| candidate.name == table)
        .ok_or(format!("table {table} is not in the active schema"))?
        .columns;
    let mut out = BTreeMap::new();
    for (name, value) in cells {
        out.insert(name, convert_value(value));
    }
    for column in columns {
        if out.contains_key(&column.name) {
            continue;
        }
        if !matches!(
            column.column_type,
            jazz::column_schema::ColumnType::Nullable(_)
        ) {
            return Err(format!(
                "{table} column {} has no value and is not nullable",
                column.name
            ));
        }
        out.insert(column.name.clone(), Value::Nullable(None));
    }
    Ok(out)
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
            // Local-first, static-bearer and anonymous principals live in
            // Jazz-owned issuer namespaces.
            let author = match issuer.as_str() {
                AuthorSubject::LOCAL_FIRST_ISSUER
                | AuthorSubject::STATIC_BEARER_ISSUER
                | AuthorSubject::ANONYMOUS_ISSUER => AuthorSubject::reserved(issuer, subject),
                _ => AuthorSubject::authenticated(issuer, subject),
            }
            .map_err(err)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    const HEAD: [u8; 16] = [7; 16];
    const ROW: [u8; 16] = [1; 16];

    fn author(subject: &str) -> dump::Author {
        dump::Author::Authenticated {
            account: Some([0xa1; 16]),
            issuer: "https://issuer.example".to_owned(),
            subject: subject.to_owned(),
        }
    }

    fn version(n: u64, by: &str, deletion: Option<dump::Deletion>) -> dump::Version {
        dump::Version {
            table: "notes".to_owned(),
            schema: HEAD,
            branch: Vec::new(),
            row: ROW,
            tx_node: [9; 16],
            tx_time: n << 18,
            parents: Vec::new(),
            global_time: Some(n),
            fate: "accepted".to_owned(),
            made_by: author(by),
            created_by: author("creator"),
            created_at: 1 << 18,
            updated_by: author(by),
            updated_at: n << 18,
            deletion,
            cells: [("body".to_owned(), dump::Value::String(format!("v{n}")))].into(),
            user_metadata_json: None,
        }
    }

    fn public() -> jazz::tools::public_schema::Schema {
        use jazz::tools::public_schema::{ColumnType, SchemaBuilder, TableSchema};
        SchemaBuilder::new()
            .table(TableSchema::builder("notes").column("body", ColumnType::Text))
            .build()
    }

    fn schema() -> dump::Schema {
        dump::Schema {
            id: HEAD,
            public_schema_json: serde_json::to_string(&public()).unwrap(),
        }
    }

    fn views() -> HashMap<[u8; 16], Arc<JazzSchema>> {
        HashMap::from([(HEAD, Arc::new(JazzSchema::new(&public()).unwrap()))])
    }

    fn replay(versions: Vec<dump::Version>) -> (RowImport, Vec<(String, String, u64)>) {
        let mut rows = RowImport {
            to_head: Some(active::ToHead::new(HEAD, &[schema()], Vec::new()).unwrap()),
            ..Default::default()
        };
        rows.creations
            .insert(("notes".to_owned(), ROW), (author("creator"), 1 << 18));
        let views = views();
        let mut writes = rows.plan(&views, versions).unwrap();
        writes.extend(rows.plan_flush());
        let summary = writes
            .iter()
            .map(|write| {
                let op = match write.op {
                    Op::Insert => "insert",
                    Op::Update => "update",
                    Op::Delete => "delete",
                    Op::Restore => "restore",
                };
                let (_, subject) = write.author.principal_parts();
                (op.to_owned(), subject.to_owned(), write.at_ms)
            })
            .collect();
        (rows, summary)
    }

    fn op(op: &str, by: &str, at: u64) -> (String, String, u64) {
        (op.to_owned(), by.to_owned(), at)
    }

    #[test]
    fn repeated_deletes_write_the_last_one_only() {
        let deleted = Some(dump::Deletion::Deleted);
        let (rows, writes) = replay(vec![
            version(2, "a", None),
            version(3, "a", deleted),
            version(4, "b", deleted),
        ]);
        // The insert carries alpha.59's creation, then the first version's
        // own update; the second delete replaces the first.
        assert_eq!(
            writes,
            [
                op("insert", "creator", 1),
                op("update", "a", 2),
                op("delete", "b", 4)
            ]
        );
        assert_eq!(rows.rows.coalesced, 1);
    }

    #[test]
    fn a_version_settled_after_a_newer_one_changes_nothing() {
        // Written at 2 and 3, settled 3 first: alpha.59 keeps 3.
        let mut older = version(2, "a", None);
        older.global_time = Some(9);
        let (rows, writes) = replay(vec![version(3, "b", None), older]);
        assert_eq!(writes, [op("insert", "creator", 1), op("update", "b", 3)]);
        assert_eq!(rows.rows.superseded, 1);
    }

    #[test]
    fn the_later_node_wins_a_tie_in_time() {
        let mut low = version(3, "a", None);
        low.tx_node = [1; 16];
        let mut high = version(3, "b", None);
        high.tx_node = [2; 16];
        let (rows, writes) = replay(vec![version(1, "creator", None), high, low]);
        assert_eq!(writes, [op("insert", "creator", 1), op("update", "b", 3)]);
        assert_eq!(rows.rows.superseded, 1);
    }

    #[test]
    fn a_version_written_on_its_winner_wins_whatever_its_time() {
        let first = version(5, "creator", None);
        let mut child = version(4, "a", None);
        child.parents = vec![dump::TxRef {
            node: first.tx_node,
            time: first.tx_time,
        }];
        let (rows, writes) = replay(vec![first, child]);
        assert_eq!(
            writes,
            [
                op("insert", "creator", 1),
                op("update", "creator", 5),
                op("update", "a", 4)
            ]
        );
        assert_eq!(rows.rows.superseded, 0);
    }

    #[test]
    fn a_delete_settled_after_a_newer_one_changes_nothing() {
        let deleted = Some(dump::Deletion::Deleted);
        let mut older = version(3, "a", deleted);
        older.global_time = Some(9);
        let (rows, writes) = replay(vec![
            version(1, "creator", None),
            version(4, "b", deleted),
            older,
        ]);
        assert_eq!(writes, [op("insert", "creator", 1), op("delete", "b", 4)]);
        assert_eq!(rows.rows.superseded, 1);
    }

    #[test]
    fn delete_restore_delete_writes_all_three() {
        let (deleted, restored) = (
            Some(dump::Deletion::Deleted),
            Some(dump::Deletion::Restored),
        );
        let (_, writes) = replay(vec![
            version(1, "creator", None),
            version(2, "a", deleted),
            version(3, "b", restored),
            version(4, "c", deleted),
        ]);
        assert_eq!(
            writes,
            [
                op("insert", "creator", 1),
                op("delete", "a", 2),
                op("restore", "b", 3),
                op("delete", "c", 4)
            ]
        );
    }

    #[test]
    fn content_written_while_deleted_goes_with_the_restore() {
        let deleted = Some(dump::Deletion::Deleted);
        let restored = Some(dump::Deletion::Restored);
        let versions = vec![
            version(1, "creator", None),
            version(2, "a", deleted),
            version(3, "b", None),
            version(4, "c", None),
            version(5, "d", restored),
        ];
        let (rows, writes) = replay(versions.clone());
        assert_eq!(
            writes,
            [
                op("insert", "creator", 1),
                op("delete", "a", 2),
                op("restore", "d", 5)
            ]
        );
        assert_eq!(
            (
                rows.rows.restores_with_hidden,
                rows.rows.rows_dropping_hidden
            ),
            (1, 0)
        );
        // The restore carries the last content written under the deletion.
        let mut rows = replay(vec![]).0;
        let views = views();
        let mut planned = rows.plan(&views, versions).unwrap();
        planned.extend(rows.plan_flush());
        let restore = planned.last().expect("a restore");
        assert!(matches!(restore.op, Op::Restore));
        assert_eq!(
            restore.cells.get("body"),
            Some(&convert_value(dump::Value::String("v4".to_owned())))
        );
    }

    #[test]
    fn delete_content_delete_restore_brings_the_content_back() {
        let deleted = Some(dump::Deletion::Deleted);
        let versions = vec![
            version(1, "creator", None),
            version(2, "a", deleted),
            version(3, "b", None),
            version(4, "c", deleted),
            version(5, "d", Some(dump::Deletion::Restored)),
        ];
        let (rows, writes) = replay(versions.clone());
        assert_eq!(
            writes,
            [
                op("insert", "creator", 1),
                op("delete", "c", 4),
                op("restore", "d", 5)
            ]
        );
        assert_eq!(rows.rows.coalesced, 1);
        let mut rows = replay(vec![]).0;
        let views = views();
        let mut planned = rows.plan(&views, versions).unwrap();
        planned.extend(rows.plan_flush());
        assert_eq!(
            planned.last().and_then(|restore| restore.cells.get("body")),
            Some(&convert_value(dump::Value::String("v3".to_owned())))
        );
    }

    #[test]
    fn a_deletion_before_any_content_is_refused() {
        let mut rows = replay(vec![]).0;
        let views = views();
        let error = rows
            .plan(&views, vec![version(1, "a", Some(dump::Deletion::Deleted))])
            .err()
            .expect("refused");
        assert!(
            error.contains("deletion event before any content"),
            "{error}"
        );
    }

    #[test]
    fn content_written_under_a_lasting_deletion_is_dropped() {
        let deleted = Some(dump::Deletion::Deleted);
        let (rows, writes) = replay(vec![
            version(1, "creator", None),
            version(2, "a", deleted),
            version(3, "b", None),
        ]);
        assert_eq!(writes, [op("insert", "creator", 1), op("delete", "a", 2)]);
        assert_eq!(
            (
                rows.rows.restores_with_hidden,
                rows.rows.rows_dropping_hidden
            ),
            (0, 1)
        );
    }

    #[test]
    fn two_current_states_for_one_row_are_refused() {
        let to_head = active::ToHead::new(HEAD, &[schema()], Vec::new()).unwrap();
        let current = |time| dump::Current {
            schema: HEAD,
            table: "notes".to_owned(),
            row: ROW,
            content: Some(dump::TxRef {
                node: [9; 16],
                time,
            }),
            deletion: None,
            created: None,
        };
        assert!(keyed_currents(&to_head, vec![current(1), current(1)]).is_ok());
        let error = keyed_currents(&to_head, vec![current(1), current(2)]).unwrap_err();
        assert!(error.contains("two current states"), "{error}");
        let created = |subject| dump::Current {
            created: Some((author(subject), 3)),
            ..current(1)
        };
        assert!(keyed_currents(&to_head, vec![created("user1"), created("user1")]).is_ok());
        let error = keyed_currents(&to_head, vec![created("user1"), created("user2")]).unwrap_err();
        assert!(error.contains("two current states"), "{error}");
    }

    #[test]
    fn jazz_owned_issuers_become_reserved_authors() {
        for issuer in [
            AuthorSubject::LOCAL_FIRST_ISSUER,
            AuthorSubject::STATIC_BEARER_ISSUER,
            AuthorSubject::ANONYMOUS_ISSUER,
        ] {
            let author = convert_author(&dump::Author::Authenticated {
                account: Some([0xa1; 16]),
                issuer: issuer.to_owned(),
                subject: "device".to_owned(),
            })
            .unwrap_or_else(|error| panic!("{issuer}: {error}"));
            assert_eq!(
                author.principal_parts(),
                (issuer.to_owned(), "device".to_owned())
            );
        }
        assert!(
            convert_author(&dump::Author::Authenticated {
                account: None,
                issuer: AuthorSubject::SYSTEM_ISSUER.to_owned(),
                subject: "x".to_owned(),
            })
            .is_err()
        );
    }

    #[test]
    fn a_delete_and_restore_both_write_and_a_restore_of_a_live_row_does_not() {
        let (deleted, restored) = (
            Some(dump::Deletion::Deleted),
            Some(dump::Deletion::Restored),
        );
        let (rows, writes) = replay(vec![
            version(1, "creator", None),
            version(2, "a", deleted),
            version(3, "b", restored),
            version(4, "c", restored),
            version(5, "c", None),
        ]);
        assert_eq!(
            writes,
            [
                op("insert", "creator", 1),
                op("delete", "a", 2),
                op("restore", "c", 4),
                op("update", "c", 5)
            ]
        );
        assert_eq!(rows.rows.coalesced, 1);
    }
}
