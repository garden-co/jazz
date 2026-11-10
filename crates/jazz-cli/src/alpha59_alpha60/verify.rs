//! `jazz-tools alpha59-alpha60 verify`: compare a migrated data dir with the dump it was
//! built from, through the new server's own read paths.
//!
//! - the bundle the one deployment published: schema and policies;
//! - every principal's login answer, against what alpha.59 itself answered;
//! - every backend identity link;
//! - every row, against what alpha.59 itself reads as its current state
//!   (the dump's settled content and deletion winners): deleted or not,
//!   every cell in the active schema, and its creation; and no row alpha.59
//!   lacks. Where the store differs from alpha.59 by design (its last write,
//!   and content written under a deletion that lasts), the import's replay
//!   rules ([`super::replay`]) say what it holds, and the replay must end on
//!   alpha.59's winners.
//!
//! It prints counts and at most 20 mismatches, naming rows and columns but
//! never cell values or authors.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use jazz::account_registry::SYSTEM_ACCOUNT_ID;
use jazz::db::{ReadOpts, block_on};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::protocol::ReadViewSpec;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz_alpha59_alpha60_dump as dump;
use jazz_server::server::alpha59_alpha60::{
    migrate_import_login, migrate_import_published_permissions,
};
use jazz_server::{BlobStore, ServerBuilder, ServerState, StorageBackend};
use serde_json::{Value as Json, json};

use super::active::{ToHead, Translated};
use super::replay::{Released, RowReplay};
use super::{
    Args, RowKey, active_deployment, convert_author, convert_value, err, keyed_currents,
    match_schemas, uuid_string,
};

const MAX_REPORTED: usize = 20;

/// A write's author and time.
type Stamp = (dump::Author, u64);

/// What the import writes for each row, by its own replay rules
/// ([`super::replay`]): which content the store holds, and who wrote the
/// row last, when. Only alpha.59's current state says what the content and
/// deletion are; this says where the store differs from it by design.
#[derive(Default)]
struct Predicted {
    rows: RowReplay<Stamp, (dump::TxRef, Stamp)>,
    /// Per row, the content transaction the store holds.
    stored: HashMap<RowKey, dump::TxRef>,
    last_write: HashMap<RowKey, Stamp>,
    /// Per row that stays deleted, the content dropped under the deletion.
    dropped: HashMap<RowKey, dump::TxRef>,
}

impl Predicted {
    fn version(&mut self, key: &RowKey, version: &dump::Version) {
        if version.fate != "accepted" || !self.rows.wins(key, version) {
            return;
        }
        let stamp = (version.updated_by.clone(), version.updated_at);
        let mut out = Vec::new();
        match version.deletion {
            Some(event) => {
                self.rows
                    .event(key, event == dump::Deletion::Deleted, stamp, &mut out);
                self.take(out);
            }
            None => {
                let tx = dump::TxRef {
                    node: version.tx_node,
                    time: version.tx_time,
                };
                let content = self.rows.content(key, (tx, stamp), &mut out);
                self.take(out);
                if let Some((tx, stamp)) = content {
                    self.stored.insert(key.clone(), tx);
                    self.last_write.insert(key.clone(), stamp);
                }
            }
        }
    }

    fn take(&mut self, out: Vec<Released<Stamp, (dump::TxRef, Stamp)>>) {
        for released in out {
            if let Some((tx, _)) = released.content {
                self.stored.insert(released.key.clone(), tx);
            }
            self.last_write.insert(released.key, released.event);
        }
    }

    fn finish(&mut self) {
        let mut out = Vec::new();
        let dropped = self.rows.finish(&mut out);
        self.take(out);
        self.dropped = dropped
            .into_iter()
            .map(|(key, (tx, _))| (key, tx))
            .collect();
    }
}

/// A row as alpha.59 reads it.
struct Expected {
    table: String,
    schema: [u8; 16],
    deleted: bool,
    cells: BTreeMap<String, dump::Value>,
    created_by: dump::Author,
    created_at: u64,
    updated_by: dump::Author,
    updated_at: u64,
}

#[derive(Default)]
struct Report {
    checked: BTreeMap<&'static str, u64>,
    mismatches: u64,
    details: Vec<String>,
    /// Mismatches per check and table (and field, where one is named).
    counts: BTreeMap<&'static str, BTreeMap<String, u64>>,
}

impl Report {
    fn check(&mut self, what: &'static str, ok: bool, detail: impl FnOnce() -> String) {
        *self.checked.entry(what).or_default() += 1;
        if !ok {
            self.mismatches += 1;
            if self.details.len() < MAX_REPORTED {
                self.details.push(format!("{what}: {}", detail()));
            }
        }
    }

    /// Count a mismatch of `what` under `place` (a table, or `table.field`).
    fn count(&mut self, what: &'static str, place: String) {
        *self
            .counts
            .entry(what)
            .or_default()
            .entry(place)
            .or_default() += 1;
    }
}

/// The import keeps `--app-id` beside the data exactly as given; verify
/// checks against the same id.
fn check_app_id(data_dir: &std::path::Path, app_id: &str) -> Result<(), String> {
    let path = data_dir.join("app-id");
    let kept =
        std::fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let kept = kept.trim_end_matches('\n');
    if kept != app_id {
        return Err(format!(
            "--app-id {app_id} is not the id {} was imported with ({kept})",
            data_dir.display()
        ));
    }
    Ok(())
}

/// Where the data dir keeps its large values: in `jazz.sqlite` as the
/// import writes them, or in files once a backend's first start moved them
/// there. A remote store would need its credentials, so verify reads it
/// only before that first start.
fn blob_store(data_dir: &std::path::Path) -> Result<BlobStore, String> {
    let marker = data_dir.join("blob-store");
    let recorded = match std::fs::read_to_string(&marker) {
        Ok(recorded) => recorded.trim_end_matches('\n').to_owned(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BlobStore::Sqlite),
        Err(error) => return Err(format!("{}: {error}", marker.display())),
    };
    match recorded.as_str() {
        "sqlite" => Ok(BlobStore::Sqlite),
        "files" => Ok(BlobStore::Files { dir: None }),
        other => match other.strip_prefix("files ") {
            Some(dir) => Ok(BlobStore::Files {
                dir: Some(data_dir.join(dir)),
            }),
            None => Err(format!(
                "{} keeps its large values in {other}: verify reads them only from the data dir, so run it before the backend's first start",
                data_dir.display()
            )),
        },
    }
}

pub async fn verify(args: &Args) -> Result<Json, String> {
    let file = std::fs::File::open(&args.dump)
        .map_err(|error| format!("{}: {error}", args.dump.display()))?;
    let (mut reader, _) = dump::Reader::open(std::io::BufReader::new(file)).map_err(err)?;
    let mut runtime_schemas = Vec::new();
    let mut runtime_lenses = Vec::new();
    let mut active_schema = None;
    let mut to_head: Option<ToHead> = None;
    let mut admin = Vec::new();
    let mut logins = Vec::new();
    let mut accounts = Vec::new();
    let mut raw_currents = Vec::new();
    let mut currents: HashMap<RowKey, dump::Current> = HashMap::new();
    // Per row: its content winner and its deletion winner, as alpha.59
    // reads them.
    let mut content: HashMap<RowKey, dump::Version> = HashMap::new();
    let mut deletion: HashMap<RowKey, dump::Version> = HashMap::new();
    let mut predicted = Predicted::default();
    while let Some(record) = reader.next_record().map_err(err)? {
        match record {
            dump::Record::Schema(schema) => runtime_schemas.push(schema),
            dump::Record::Lens(lens) => runtime_lenses.push(lens),
            dump::Record::ActiveSchema { schema } => active_schema = Some(schema),
            dump::Record::AdminEntry(entry) => admin.push(entry),
            dump::Record::Account(account) => accounts.push(account),
            dump::Record::AccountLogin {
                principal_json,
                login_json,
            } => logins.push((principal_json, login_json)),
            dump::Record::Current(current) => raw_currents.push(current),
            dump::Record::Version(version) => {
                if to_head.is_none() {
                    let built = ToHead::new(
                        active_schema.ok_or("the dump has rows but no active schema")?,
                        &runtime_schemas,
                        std::mem::take(&mut runtime_lenses),
                    )?;
                    currents = keyed_currents(&built, std::mem::take(&mut raw_currents))?;
                    to_head = Some(built);
                }
                let to_head = to_head.as_ref().expect("set above");
                let tx = dump::TxRef {
                    node: version.tx_node,
                    time: version.tx_time,
                };
                // Its table is not in the active schema.
                let Some(table) = to_head.head_table(version.schema, &version.table)? else {
                    continue;
                };
                let key: RowKey = (table, version.row);
                predicted.version(&key, &version);
                let Some(current) = currents.get(&key) else {
                    continue;
                };
                let winner = if version.deletion.is_none() {
                    current.content
                } else {
                    current.deletion
                };
                if winner != Some(tx) {
                    continue;
                }
                let version = translated(to_head, &key, version)?;
                let winners = if version.deletion.is_none() {
                    &mut content
                } else {
                    &mut deletion
                };
                winners.insert(key, version);
            }
            _ => {}
        }
    }
    predicted.finish();
    // A row that stays deleted over content written under its deletion: the
    // store holds the content from before it. Read those versions back.
    let mut wanted: HashMap<RowKey, dump::TxRef> = HashMap::new();
    for (key, current) in &currents {
        if let (Some(stored), Some(winner)) = (predicted.stored.get(key), current.content)
            && *stored != winner
        {
            wanted.insert(key.clone(), *stored);
        }
    }
    let mut stored_versions: HashMap<RowKey, dump::Version> = HashMap::new();
    if !wanted.is_empty() {
        let to_head = to_head.as_ref().expect("rows imply a head");
        let file = std::fs::File::open(&args.dump)
            .map_err(|error| format!("{}: {error}", args.dump.display()))?;
        let (mut reader, _) = dump::Reader::open(std::io::BufReader::new(file)).map_err(err)?;
        while let Some(record) = reader.next_record().map_err(err)? {
            let dump::Record::Version(version) = record else {
                continue;
            };
            if version.deletion.is_some() {
                continue;
            }
            let Some(table) = to_head.head_table(version.schema, &version.table)? else {
                continue;
            };
            let key: RowKey = (table, version.row);
            let tx = dump::TxRef {
                node: version.tx_node,
                time: version.tx_time,
            };
            if wanted.get(&key) == Some(&tx) {
                stored_versions.insert(key.clone(), translated(to_head, &key, version)?);
            }
        }
    }
    let mut report = Report::default();
    let mut rows: HashMap<RowKey, Expected> = HashMap::new();
    for (key, current) in &currents {
        let label = || format!("{} row {}", key.0, uuid_string(key.1));
        // Every version of the row was rejected: alpha.59 has no such row.
        if current.content.is_none() && current.deletion.is_none() {
            continue;
        }
        if current.content.is_none() {
            report.check("alpha.59 current state", false, || {
                format!(
                    "{}: alpha.59 reads a deletion but no content for it",
                    label()
                )
            });
            continue;
        }
        let Some(content) = content.remove(key) else {
            report.check("alpha.59 current state", false, || {
                format!("{}: its current content version is missing", label())
            });
            continue;
        };
        let deletion = deletion.remove(key);
        if current.deletion.is_some() && deletion.is_none() {
            report.check("alpha.59 current state", false, || {
                format!("{}: its current deletion event is missing", label())
            });
            continue;
        }
        report.check("alpha.59 current state", true, label);
        // The import's replay ends on alpha.59's content winner, held by the
        // store or dropped under a lasting deletion.
        let winner = current.content;
        let replayed = predicted.stored.get(key).copied() == winner
            || predicted.dropped.get(key).copied() == winner;
        report.check("replayed content", replayed, || {
            format!("{}: the replay ends on another content version", label())
        });
        let Some((updated_by, updated_at)) = predicted.last_write.remove(key) else {
            report.check("replayed content", false, || {
                format!("{}: the replay writes nothing", label())
            });
            continue;
        };
        let stored = match stored_versions.remove(key) {
            Some(stored) => stored,
            None if predicted.stored.get(key).copied() == winner => content.clone(),
            None => {
                report.check("replayed content", false, || {
                    format!("{}: the content the store holds is missing", label())
                });
                continue;
            }
        };
        rows.insert(
            key.clone(),
            Expected {
                table: stored.table,
                schema: stored.schema,
                deleted: deletion
                    .is_some_and(|event| event.deletion == Some(dump::Deletion::Deleted)),
                cells: stored.cells,
                // The insert carries alpha.59's creation: the content winner's.
                created_by: content.created_by,
                created_at: content.created_at,
                updated_by,
                updated_at,
            },
        );
    }

    check_app_id(&args.data_dir, &args.app_id_text)?;
    // A running backend holds the dir's lock; verify reads a dir at rest.
    let _lock = jazz_server::lock_data_dir(&args.data_dir).map_err(|error| {
        format!(
            "data dir '{}' {error}: stop the backend before verifying",
            args.data_dir.display()
        )
    })?;
    let built = ServerBuilder::new(args.app_id)
        .with_storage(StorageBackend::Persistent {
            path: args.data_dir.clone(),
        })
        .with_blob_store(blob_store(&args.data_dir)?)
        .build()
        .await?;
    let state = built.state.clone();
    verify_permissions(&state, &admin, &mut report)?;
    for (index, (principal, expected)) in logins.iter().enumerate() {
        let actual = migrate_import_login(&state, principal).await?;
        let expected: Json = serde_json::from_str(expected).map_err(err)?;
        let outcome = |login: &Json| {
            if login.get("Ok").is_some() {
                "admitted"
            } else {
                "refused"
            }
        };
        report.check("account logins", actual == expected, || {
            format!(
                "principal #{index} (in dump order): alpha.59 {}, SQLite {}",
                outcome(&expected),
                outcome(&actual)
            )
        });
    }
    if args.link_backend_identities {
        let principals: Vec<String> = logins.iter().map(|(p, _)| p.clone()).collect();
        let planned = super::links::plan(&super::links::identities(&accounts, &principals)?)?;
        let issuer = super::links::BACKEND_ISSUER;
        for (index, link) in planned.iter().enumerate() {
            let candidate = json!({ "issuer": issuer, "subject": link.subject }).to_string();
            let login = migrate_import_login(&state, &candidate).await?;
            let ok = login.get("Ok").is_some_and(|assignment| {
                serde_json::to_string(&assignment["account"]).ok().as_ref() == Some(&link.account)
            });
            report.check("backend identity links", ok, || {
                format!("link #{index}: the {issuer} identity does not open its account")
            });
        }
    }
    if !rows.is_empty() {
        let runtime = state
            .runtime()
            .ok_or("the migrated server has no runtime")?;
        let head = to_head.as_ref().expect("rows imply a head").head;
        let active: Vec<_> = runtime_schemas
            .iter()
            .filter(|schema| schema.id == head)
            .cloned()
            .collect();
        let views = match_schemas(&active, runtime.migrate_import_schemas().await?)?;
        verify_rows(&runtime, &views, rows, &mut report).await?;
    }
    built.shutdown().await;
    Ok(json!({
        "ok": report.mismatches == 0,
        "checked": report.checked,
        "mismatches": report.mismatches,
        "mismatch_counts": report.counts,
        "first_mismatches": report.details,
    }))
}

fn verify_permissions(
    state: &Arc<ServerState>,
    admin: &[dump::AdminEntry],
    report: &mut Report,
) -> Result<(), String> {
    let published = migrate_import_published_permissions(state)?;
    // The one deployment the import makes publishes this bundle; the app may
    // have published its own beside it since, so any published bundle that
    // matches counts, whatever the clocks that ordered them.
    let Some((deployment, _)) = active_deployment(admin)? else {
        report.check("published bundle", published.is_empty(), || {
            format!(
                "{} bundles, though alpha.59 published none",
                published.len()
            )
        });
        return Ok(());
    };
    let expected = json!({
        "schema_hash": deployment["targetSchemaHash"],
        "permissions": deployment["permissions"],
    });
    let found = published.iter().any(|bundle| {
        bundle["schema_hash"] == expected["schema_hash"]
            && bundle["permissions"] == expected["permissions"]
    });
    report.check("published bundle", found, || {
        let hashes: Vec<&str> = published
            .iter()
            .filter_map(|bundle| bundle["schema_hash"].as_str())
            .collect();
        format!(
            "no published bundle has alpha.59's active permissions for schema {} \
             (published schemas: {})",
            expected["schema_hash"],
            hashes.join(", ")
        )
    });
    Ok(())
}

/// What the new server holds for one row.
struct Actual {
    deleted: bool,
    cells: Vec<(String, Option<Value>)>,
    provenance: Option<(u64, String, u64, String)>,
}

async fn verify_rows(
    runtime: &jazz_server::runtime::ServerRuntimeHandle,
    views: &HashMap<[u8; 16], JazzSchema>,
    rows: HashMap<RowKey, Expected>,
    report: &mut Report,
) -> Result<(), String> {
    let mut groups: BTreeMap<([u8; 16], String), Vec<[u8; 16]>> = BTreeMap::new();
    for ((_, row), expected) in &rows {
        groups
            .entry((expected.schema, expected.table.clone()))
            .or_default()
            .push(*row);
    }
    for ((schema, table), ids) in groups {
        let view_schema = views
            .get(&schema)
            .ok_or(format!("rows under unknown schema {}", uuid_string(schema)))?
            .clone();
        let job_table = table.clone();
        let job_ids = ids.clone();
        let actual: HashMap<[u8; 16], Actual> = runtime
            .migrate_import_with_db(move |db| {
                let view = block_on(db.register_schema_view(view_schema)).map_err(err)?;
                let prepared = view
                    .prepare_query(&view.table(job_table.clone()))
                    .map_err(err)?;
                let opts = ReadOpts {
                    read_view: ReadViewSpec {
                        include_deleted: true,
                        ..Default::default()
                    },
                    ..Default::default()
                };
                let mut found: HashMap<[u8; 16], Actual> = HashMap::new();
                for current in block_on(view.all(&prepared, opts)).map_err(err)? {
                    found.insert(
                        *current.row_uuid().0.as_bytes(),
                        Actual {
                            deleted: current.is_deleted(),
                            cells: current
                                .cells()
                                .map(|(name, value)| (name.to_owned(), value.cloned()))
                                .collect(),
                            provenance: None,
                        },
                    );
                }
                for id in job_ids {
                    if let Some(actual) = found.get_mut(&id) {
                        actual.provenance = jazz::db::block_on(
                            view.row_provenance(&job_table, RowUuid::from_bytes(id)),
                        )
                        .map_err(err)?;
                    }
                }
                Ok(found)
            })
            .await?;
        let wanted: std::collections::HashSet<_> = ids.iter().copied().collect();
        let extra = actual.keys().filter(|id| !wanted.contains(*id)).count();
        report.check("no extra rows", extra == 0, || {
            format!("{table}: {extra} rows alpha.59 does not have")
        });
        if extra > 0 {
            *report
                .counts
                .entry("no extra rows")
                .or_default()
                .entry(table.clone())
                .or_default() += extra as u64;
        }
        for id in ids {
            let expected = &rows[&(table.clone(), id)];
            let label = || format!("{table} row {}", uuid_string(id));
            let Some(actual) = actual.get(&id) else {
                report.check("rows present", false, label);
                report.count("rows present", table.clone());
                continue;
            };
            report.check("rows present", true, label);
            report.check(
                "row deleted flags",
                actual.deleted == expected.deleted,
                || {
                    format!(
                        "{}: alpha.59 {}, SQLite {}",
                        label(),
                        expected.deleted,
                        actual.deleted
                    )
                },
            );
            if actual.deleted != expected.deleted {
                report.count("row deleted flags", table.clone());
            }
            let mut differing = Vec::new();
            for (name, value) in &actual.cells {
                let want = expected.cells.get(name).cloned().map(convert_value);
                let same = match (&want, value) {
                    (Some(want), Some(value)) => want == value,
                    (None, None) | (None, Some(Value::Nullable(None))) => true,
                    (Some(Value::Nullable(None)), None) => true,
                    _ => false,
                };
                if !same {
                    differing.push(name.clone());
                }
            }
            for name in expected.cells.keys() {
                if !actual.cells.iter().any(|(actual, _)| actual == name) {
                    differing.push(format!("{name} (missing)"));
                }
            }
            for name in &differing {
                report.count("row cells", format!("{table}.{name}"));
            }
            report.check("row cells", differing.is_empty(), || {
                format!("{}: columns {}", label(), differing.join(", "))
            });
            let want = (
                dump::Version::millis(expected.created_at),
                canonical(&convert_author(&expected.created_by)?),
                dump::Version::millis(expected.updated_at),
                canonical(&convert_author(&expected.updated_by)?),
            );
            let differing: Vec<&str> = match &actual.provenance {
                None => vec!["all"],
                Some(actual) => [
                    ("created_at", actual.0 == want.0),
                    ("created_by", actual.1 == want.1),
                    ("updated_at", actual.2 == want.2),
                    ("updated_by", actual.3 == want.3),
                ]
                .iter()
                .filter(|(_, same)| !same)
                .map(|(field, _)| *field)
                .collect(),
            };
            for field in &differing {
                report.count("row provenance", format!("{table}.{field}"));
            }
            let differing = differing.join(", ");
            report.check("row provenance", differing.is_empty(), || {
                format!("{}: {differing}", label())
            });
        }
    }
    Ok(())
}

/// `version` brought to the active schema, whose table holds `key`.
fn translated(
    to_head: &ToHead,
    key: &RowKey,
    version: dump::Version,
) -> Result<dump::Version, String> {
    match to_head.translate(version)? {
        Translated::Kept(version) => Ok(version),
        Translated::TableGone => Err(format!(
            "{} row {}: its table is in the active schema, but its version translates to none",
            key.0,
            uuid_string(key.1)
        )),
    }
}

/// An author as `Db::row_provenance` spells it.
fn canonical(author: &AuthorSubject) -> String {
    let account = author.account_id().unwrap_or(SYSTEM_ACCOUNT_ID).0;
    let (issuer, subject) = author.principal_parts();
    if account == SYSTEM_ACCOUNT_ID.0 && issuer != AuthorSubject::SYSTEM_ISSUER {
        serde_json::to_string(&(issuer, subject))
    } else {
        serde_json::to_string(&(account.to_string(), issuer, subject))
    }
    .expect("author parts have a JSON spelling")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_refuses_another_app_id_than_the_import_kept() {
        let dir = tempfile::tempdir().unwrap();
        let id = "0a590000-0000-4000-8000-000000000059";
        std::fs::write(dir.path().join("app-id"), format!("{id}\n")).unwrap();
        check_app_id(dir.path(), id).unwrap();
        let refused = check_app_id(dir.path(), &id.to_uppercase()).unwrap_err();
        assert!(refused.contains("is not the id"), "{refused}");
    }

    #[test]
    fn verify_reads_large_values_where_the_backend_moved_them() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(blob_store(dir.path()), Ok(BlobStore::Sqlite)));
        let marker = dir.path().join("blob-store");
        std::fs::write(&marker, "files\n").unwrap();
        assert!(matches!(
            blob_store(dir.path()),
            Ok(BlobStore::Files { dir: None })
        ));
        std::fs::write(&marker, "files values\n").unwrap();
        assert!(matches!(
            blob_store(dir.path()),
            Ok(BlobStore::Files { dir: Some(at) }) if at == dir.path().join("values")
        ));
        std::fs::write(&marker, "s3 bucket/prefix\n").unwrap();
        let refused = blob_store(dir.path()).err().unwrap();
        assert!(
            refused.contains("before the backend's first start"),
            "{refused}"
        );
    }

    fn key() -> RowKey {
        ("notes".to_owned(), [1; 16])
    }

    fn author(subject: &str) -> dump::Author {
        dump::Author::Authenticated {
            account: None,
            issuer: "https://issuer.example".to_owned(),
            subject: subject.to_owned(),
        }
    }

    fn version(n: u64, by: &str, deletion: Option<dump::Deletion>) -> dump::Version {
        dump::Version {
            table: "notes".to_owned(),
            schema: [7; 16],
            branch: Vec::new(),
            row: [1; 16],
            tx_node: [9; 16],
            tx_time: n,
            parents: Vec::new(),
            global_time: Some(n),
            fate: "accepted".to_owned(),
            made_by: author(by),
            created_by: author("creator"),
            created_at: 1,
            updated_by: author(by),
            updated_at: n,
            deletion,
            cells: BTreeMap::new(),
            user_metadata_json: None,
        }
    }

    fn tx(n: u64) -> dump::TxRef {
        dump::TxRef {
            node: [9; 16],
            time: n,
        }
    }

    fn predict(versions: &[dump::Version]) -> Predicted {
        let mut predicted = Predicted::default();
        for version in versions {
            predicted.version(&key(), version);
        }
        predicted.finish();
        predicted
    }

    #[test]
    fn a_restore_of_a_live_row_keeps_the_rows_update() {
        let predicted = predict(&[
            version(1, "a", None),
            version(2, "b", Some(dump::Deletion::Restored)),
        ]);
        assert_eq!(predicted.last_write[&key()], (author("a"), 1));
    }

    #[test]
    fn content_under_a_lasting_deletion_leaves_the_delete_last() {
        let predicted = predict(&[
            version(1, "a", None),
            version(2, "b", Some(dump::Deletion::Deleted)),
            version(3, "c", None),
        ]);
        // The store keeps the content from before the delete, and the
        // delete as its last write, though alpha.59's content winner is 3.
        assert_eq!(predicted.stored[&key()], tx(1));
        assert_eq!(predicted.dropped[&key()], tx(3));
        assert_eq!(predicted.last_write[&key()], (author("b"), 2));
    }

    #[test]
    fn content_written_while_deleted_comes_back_with_the_restore() {
        let deleted = Some(dump::Deletion::Deleted);
        let predicted = predict(&[
            version(1, "a", None),
            version(2, "b", deleted),
            version(3, "c", None),
            version(4, "d", deleted),
            version(5, "e", Some(dump::Deletion::Restored)),
        ]);
        assert_eq!(predicted.stored[&key()], tx(3));
        assert!(predicted.dropped.is_empty());
        assert_eq!(predicted.last_write[&key()], (author("e"), 5));
    }

    #[test]
    fn a_version_settled_after_a_newer_one_writes_nothing() {
        let predicted = predict(&[version(2, "a", None), version(1, "b", None)]);
        assert_eq!(predicted.stored[&key()], tx(2));
        assert_eq!(predicted.last_write[&key()], (author("a"), 2));
    }
}
