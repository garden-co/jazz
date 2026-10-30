//! `sqlite-import verify`: compare a migrated data dir with the dump it was
//! built from, through the new server's own read paths.
//!
//! - the active permissions head: schema, version and policies;
//! - every principal's login answer, against what alpha.59 itself answered;
//! - every row: deleted or not, every cell (read through the schema version
//!   its last version was written under), and its created/updated author
//!   and timestamp.
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
use jazz_server::server::migrate_import::{
    migrate_import_active_permissions, migrate_import_login,
};
use jazz_server::{ServerBuilder, ServerState, StorageBackend};
use migrate_alpha59_dump as dump;
use serde_json::{Value as Json, json};

use crate::{Args, convert_author, convert_value, err, match_schemas, uuid_string};

const MAX_REPORTED: usize = 20;

/// A row as its last stored version left it.
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
    let mut admin = Vec::new();
    let mut logins = Vec::new();
    let mut rows: HashMap<[u8; 16], Expected> = HashMap::new();
    while let Some(record) = reader.next_record().map_err(err)? {
        match record {
            dump::Record::Schema(schema) => runtime_schemas.push(schema),
            dump::Record::AdminEntry(entry) => admin.push(entry),
            dump::Record::AccountLogin {
                principal_json,
                login_json,
            } => logins.push((principal_json, login_json)),
            dump::Record::Version(version) if !version.fate.starts_with("rejected") => {
                let previous = rows.remove(&version.row);
                let deleted = version.deletion == Some(dump::Deletion::Deleted);
                let cells = match (version.deletion, previous) {
                    (None, _) => version.cells,
                    // A delete or restore keeps the content it covers.
                    (Some(_), Some(previous)) => previous.cells,
                    (Some(_), None) => version.cells,
                };
                rows.insert(
                    version.row,
                    Expected {
                        table: version.table,
                        schema: version.schema,
                        deleted,
                        cells,
                        created_by: version.created_by,
                        created_at: version.created_at,
                        updated_by: version.updated_by,
                        updated_at: version.updated_at,
                    },
                );
            }
            _ => {}
        }
    }

    let built = ServerBuilder::new(args.app_id)
        .with_storage(StorageBackend::Persistent {
            path: args.data_dir.clone(),
        })
        .build()
        .await?;
    let state = built.state.clone();
    let mut report = Report::default();
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
    if !rows.is_empty() {
        let runtime = state
            .runtime()
            .ok_or("the migrated server has no runtime")?;
        let views = match_schemas(&runtime_schemas, runtime.migrate_import_schemas().await?)?;
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
    let payload = |entry: &dump::AdminEntry| serde_json::from_str::<Json>(&entry.payload_json);
    let head = admin
        .iter()
        .filter(|entry| entry.object_type == "catalogue_permissions_head")
        .map(payload)
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?
        .into_iter()
        .max_by_key(|head| head["version"].as_u64());
    let expected = match head {
        None => Json::Null,
        Some(head) => {
            let bundle = admin
                .iter()
                .find(|entry| {
                    entry.object_type == "catalogue_permissions_bundle"
                        && Some(uuid_string(entry.object_id).as_str())
                            == head["bundle_object_id"].as_str()
                })
                .ok_or("the head's permissions bundle is missing from the dump")?;
            let bundle = payload(bundle).map_err(err)?;
            json!({
                "schema_hash": bundle["schema_hash"],
                "version": bundle["version"],
                "permissions": bundle["permissions"],
            })
        }
    };
    let actual = migrate_import_active_permissions(state)?;
    report.check("permissions head", actual == expected, || {
        format!(
            "alpha.59 schema {} v{}, SQLite schema {} v{}",
            expected["schema_hash"], expected["version"], actual["schema_hash"], actual["version"]
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
