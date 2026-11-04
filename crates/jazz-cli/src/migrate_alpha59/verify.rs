//! `jazz-tools migrate-alpha59 verify`: compare a migrated data dir with the dump it was
//! built from, through the new server's own read paths.
//!
//! - the bundle the one deployment published: schema and policies;
//! - every principal's login answer, against what alpha.59 itself answered;
//! - every backend identity link;
//! - every row, against what alpha.59 itself reads as its current state
//!   (the dump's settled content and deletion winners, not this tool's own
//!   replay): deleted or not, every cell in the active schema, and its
//!   created/updated author and timestamp; and no row alpha.59 lacks.
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
use jazz_alpha59_dump as dump;
use jazz_server::server::migrate_alpha59::{
    migrate_import_active_permissions, migrate_import_login,
};
use jazz_server::{ServerBuilder, ServerState, StorageBackend};
use serde_json::{Value as Json, json};

use super::active::{ToHead, Translated};
use super::{
    Args, active_deployment, convert_author, convert_value, err, match_schemas, uuid_string,
};

const MAX_REPORTED: usize = 20;

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
}

pub async fn verify(args: &Args) -> Result<Json, String> {
    let file = std::fs::File::open(&args.dump).map_err(err)?;
    let (mut reader, _) = dump::Reader::open(std::io::BufReader::new(file)).map_err(err)?;
    let mut runtime_schemas = Vec::new();
    let mut runtime_lenses = Vec::new();
    let mut active_schema = None;
    let mut to_head: Option<ToHead> = None;
    let mut admin = Vec::new();
    let mut logins = Vec::new();
    let mut accounts = Vec::new();
    let mut currents: HashMap<[u8; 16], dump::Current> = HashMap::new();
    // Per row: its content winner and its deletion winner, each with its
    // position in commit order.
    let mut content: HashMap<[u8; 16], (u64, dump::Version)> = HashMap::new();
    let mut deletion: HashMap<[u8; 16], (u64, dump::Version)> = HashMap::new();
    let mut gone: std::collections::HashSet<[u8; 16]> = Default::default();
    let mut position = 0u64;
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
            dump::Record::Current(current) => {
                currents.insert(current.row, current);
            }
            dump::Record::Version(version) => {
                position += 1;
                let tx = dump::TxRef {
                    node: version.tx_node,
                    time: version.tx_time,
                };
                let Some(current) = currents.get(&version.row) else {
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
                if to_head.is_none() {
                    to_head = Some(ToHead::new(
                        active_schema.ok_or("the dump has rows but no active schema")?,
                        &runtime_schemas,
                        std::mem::take(&mut runtime_lenses),
                    )?);
                }
                let to_head = to_head.as_ref().expect("set above");
                let version = match to_head.translate(version)? {
                    Translated::Kept(version) => version,
                    Translated::TableGone => {
                        gone.insert(current.row);
                        continue;
                    }
                };
                let winners = if version.deletion.is_none() {
                    &mut content
                } else {
                    &mut deletion
                };
                winners.insert(version.row, (position, version));
            }
            _ => {}
        }
    }
    let mut report = Report::default();
    let mut rows: HashMap<[u8; 16], Expected> = HashMap::new();
    for (row, current) in &currents {
        if gone.contains(row) {
            continue;
        }
        let label = || format!("row {}", uuid_string(*row));
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
        let Some((content_at, content)) = content.remove(row) else {
            report.check("alpha.59 current state", false, || {
                format!("{}: its current content version is missing", label())
            });
            continue;
        };
        let deletion = deletion.remove(row);
        if current.deletion.is_some() && deletion.is_none() {
            report.check("alpha.59 current state", false, || {
                format!("{}: its current deletion event is missing", label())
            });
            continue;
        }
        report.check("alpha.59 current state", true, label);
        let (updated_by, updated_at) = match &deletion {
            Some((at, event)) if *at > content_at => (event.updated_by.clone(), event.updated_at),
            _ => (content.updated_by.clone(), content.updated_at),
        };
        rows.insert(
            *row,
            Expected {
                table: content.table,
                schema: content.schema,
                deleted: deletion
                    .is_some_and(|(_, event)| event.deletion == Some(dump::Deletion::Deleted)),
                cells: content.cells,
                created_by: content.created_by,
                created_at: content.created_at,
                updated_by,
                updated_at,
            },
        );
    }

    let built = ServerBuilder::new(args.app_id)
        .with_storage(StorageBackend::Persistent {
            path: args.data_dir.clone(),
        })
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
        "first_mismatches": report.details,
    }))
}

fn verify_permissions(
    state: &Arc<ServerState>,
    admin: &[dump::AdminEntry],
    report: &mut Report,
) -> Result<(), String> {
    // The one deployment the import makes publishes this bundle.
    let expected = match active_deployment(admin)? {
        None => Json::Null,
        Some(deployment) => json!({
            "schema_hash": deployment["targetSchemaHash"],
            "permissions": deployment["permissions"],
        }),
    };
    let mut actual = migrate_import_active_permissions(state)?;
    if let Some(actual) = actual.as_object_mut() {
        actual.remove("bundle");
    }
    report.check("published bundle", actual == expected, || {
        format!(
            "alpha.59 schema {}, SQLite schema {}",
            expected["schema_hash"], actual["schema_hash"]
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
    rows: HashMap<[u8; 16], Expected>,
    report: &mut Report,
) -> Result<(), String> {
    let mut groups: BTreeMap<([u8; 16], String), Vec<[u8; 16]>> = BTreeMap::new();
    for (row, expected) in &rows {
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
                        actual.provenance = view
                            .row_provenance(&job_table, RowUuid::from_bytes(id))
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
        for id in ids {
            let expected = &rows[&id];
            let label = || format!("{table} row {}", uuid_string(id));
            let Some(actual) = actual.get(&id) else {
                report.check("rows present", false, label);
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
            report.check("row cells", differing.is_empty(), || {
                format!("{}: columns {}", label(), differing.join(", "))
            });
            let want = (
                dump::Version::millis(expected.created_at),
                canonical(&convert_author(&expected.created_by)?),
                dump::Version::millis(expected.updated_at),
                canonical(&convert_author(&expected.updated_by)?),
            );
            let differing = match &actual.provenance {
                None => "all".to_owned(),
                Some(actual) => [
                    ("created_at", actual.0 == want.0),
                    ("created_by", actual.1 == want.1),
                    ("updated_at", actual.2 == want.2),
                    ("updated_by", actual.3 == want.3),
                ]
                .iter()
                .filter(|(_, same)| !same)
                .map(|(field, _)| *field)
                .collect::<Vec<_>>()
                .join(", "),
            };
            report.check("row provenance", differing.is_empty(), || {
                format!("{}: {differing}", label())
            });
        }
    }
    Ok(())
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
