//! `a59-export`: read one alpha.59 server data dir and write a dump.
//!
//! It opens a private copy of the data dir (never the original: opening a
//! RocksDB root can run recovery, which writes) and reads it with alpha.59's
//! own decoders. See ../README.md.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use jazz::groove::records::Value;
use jazz::groove::storage::{OrderedKvStorage, StorageFactory};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::node::NodeState;
use jazz::node::migrate_export::ExportedVersion;
use jazz::schema::JazzSchema;
use jazz::tx::{DeletionEvent, Fate};
use migrate_alpha59_dump as dump;

/// The node id every alpha.59 server shell runs as.
const SERVER_NODE: [u8; 16] = [0x5e; 16];
/// The RocksDB roots of an alpha.59 server data dir.
const ROOTS: [&str; 3] = [
    "server-shell.rocksdb",
    "catalogue.rocksdb",
    "accounts.rocksdb",
];

struct Args {
    data_dir: PathBuf,
    out: PathBuf,
    work: PathBuf,
    /// Schemas, lenses, admin catalogue and accounts only: no rows. For the
    /// fleet census, which must not copy customer data anywhere.
    catalogue_only: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut data_dir = None;
    let mut out = None;
    let mut work = None;
    let mut catalogue_only = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(value()?)),
            "--out" => out = Some(PathBuf::from(value()?)),
            "--work" => work = Some(PathBuf::from(value()?)),
            "--catalogue-only" => catalogue_only = true,
            _ => return Err(format!("unknown argument {arg}")),
        }
    }
    Ok(Args {
        data_dir: data_dir.ok_or("--data-dir is required")?,
        out: out.ok_or("--out is required")?,
        work: work.ok_or("--work (an empty scratch dir for the copy) is required")?,
        catalogue_only,
    })
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("a59-export: {error}");
            eprintln!(
                "usage: a59-export --data-dir <alpha.59 data dir> --work <empty dir> --out <dump> [--catalogue-only]"
            );
            return ExitCode::from(2);
        }
    };
    match run(&args) {
        Ok(footer) => {
            println!(
                "wrote {} ({} records, {} versions, digest {})",
                args.out.display(),
                footer.records,
                footer.versions,
                hex(&footer.digest)
            );
            for (table, count) in &footer.tables {
                println!("  {table}: {count} versions");
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("a59-export: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<dump::Footer, String> {
    let shell = args.data_dir.join("server-shell.rocksdb");
    if !shell.is_dir() {
        return Err(format!(
            "{} is not an alpha.59 data dir",
            args.data_dir.display()
        ));
    }
    if args.work.exists() && std::fs::read_dir(&args.work).map_err(err)?.next().is_some() {
        return Err(format!("{} is not empty", args.work.display()));
    }
    // Opening a RocksDB root can run recovery, which writes: only ever open
    // private copies.
    for root in ROOTS {
        let from = args.data_dir.join(root);
        if from.is_dir() {
            copy_dir(&from, &args.work.join(root))?;
        }
    }

    let file = std::fs::File::create(&args.out).map_err(err)?;
    let mut writer = dump::Writer::new(
        std::io::BufWriter::new(file),
        dump::Header {
            format: dump::DUMP_FORMAT,
            source: "jazz v2.0.0-alpha.59".to_owned(),
            node: SERVER_NODE,
        },
    )
    .map_err(err)?;

    // Every catalogue and account record comes before the first row
    // version, so the importer can stream rows.
    let catalogue = args.work.join("catalogue.rocksdb");
    let accounts = args.work.join("accounts.rocksdb");
    jazz::db::block_on(async {
        let mut node = open_shell(&args.work.join("server-shell.rocksdb")).await?;
        export_runtime_catalogue(&node, &mut writer)?;
        if catalogue.is_dir() {
            export_admin_catalogue(catalogue, &mut writer)?;
        }
        if accounts.is_dir() {
            export_accounts(&accounts, &mut writer).await?;
        }
        if !args.catalogue_only {
            export_versions(&mut node, &mut writer).await?;
        }
        Ok::<_, String>(())
    })?;

    let (_, footer) = writer.finish().map_err(err)?;
    Ok(footer)
}

type Node = NodeState<jazz::groove::storage::BoxedStorage>;

async fn open_shell(path: &Path) -> Result<Node, String> {
    let requested = JazzSchema::empty();
    let storage = jazz_storage_rocksdb::RocksDbStorageFactory
        .open(
            path.to_path_buf(),
            requested.column_families(),
            jazz::storage_codec_profile::epoch_1_storage_codec_profile().map_err(err)?,
        )
        .await
        .map_err(err)?;
    let (storage, schema) = NodeState::select_durable_reopen_schema(storage, requested)
        .await
        .map_err(err)?;
    NodeState::new_history_complete(NodeUuid::from_bytes(SERVER_NODE), schema, storage)
        .await
        .map_err(err)
}

fn export_runtime_catalogue<W: std::io::Write>(
    node: &Node,
    writer: &mut dump::Writer<W>,
) -> Result<(), String> {
    let catalogue = node.migrate_export_catalogue();
    for schema in &catalogue.schemas {
        writer
            .write(&dump::Record::Schema(dump::Schema {
                id: *schema.id.0.as_bytes(),
                public_schema_json: serde_json::to_string(schema.schema.public_schema())
                    .map_err(err)?,
            }))
            .map_err(err)?;
    }
    for lens in &catalogue.lenses {
        let mut tables = Vec::new();
        for table in &lens.table_lenses {
            tables.push(dump::TableLens {
                source_table: table.source_table.clone(),
                target_table: table.target_table.clone(),
                ops_json: table
                    .ops
                    .iter()
                    .map(serde_json::to_string)
                    .collect::<Result<_, _>>()
                    .map_err(err)?,
            });
        }
        writer
            .write(&dump::Record::Lens(dump::Lens {
                id: *lens.id.0.as_bytes(),
                source: *lens.source.0.as_bytes(),
                target: *lens.target.0.as_bytes(),
                tables,
            }))
            .map_err(err)?;
    }
    writer
        .write(&dump::Record::ActiveSchema {
            schema: *catalogue.active_schema.0.as_bytes(),
        })
        .map_err(err)?;
    Ok(())
}

async fn export_versions<W: std::io::Write>(
    node: &mut Node,
    writer: &mut dump::Writer<W>,
) -> Result<(), String> {
    let mut versions = node.migrate_export_versions().await.map_err(err)?;
    // Commit order: the authority's global order, then the HLC for anything
    // it never ordered (which `fate` reports).
    versions.sort_by_key(|version| {
        (
            version.global_time.map(|time| time.0).unwrap_or(u64::MAX),
            version.tx.time.0,
            *version.tx.node.as_bytes(),
            version.table.clone(),
            *version.row.0.as_bytes(),
            version.deletion.is_some(),
        )
    });
    for version in versions {
        writer
            .write(&dump::Record::Version(convert_version(version)?))
            .map_err(err)?;
    }
    Ok(())
}

fn export_admin_catalogue<W: std::io::Write>(
    path: PathBuf,
    writer: &mut dump::Writer<W>,
) -> Result<(), String> {
    let factory: std::sync::Arc<dyn StorageFactory> =
        std::sync::Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory);
    let mut entries =
        jazz_server::server::migrate_export::migrate_export_admin_catalogue(factory, path)?;
    entries.sort_by_key(|entry| entry.object_id);
    for entry in entries {
        writer
            .write(&dump::Record::AdminEntry(dump::AdminEntry {
                object_id: entry.object_id,
                metadata: entry.metadata,
                object_type: entry.object_type,
                payload_json: entry.payload_json,
            }))
            .map_err(err)?;
    }
    Ok(())
}

async fn export_accounts<W: std::io::Write>(
    path: &Path,
    writer: &mut dump::Writer<W>,
) -> Result<(), String> {
    // Exactly how the alpha.59 server opens its account root.
    let profile = jazz::storage_codec_profile::epoch_1_storage_codec_profile()
        .and_then(|profile| profile.with_additional_codecs(["jazz.account-journal.v1"]))
        .map_err(err)?;
    let storage = jazz_storage_rocksdb::RocksDbStorageFactory
        .open(path.to_path_buf(), vec!["default".into()], profile)
        .await
        .map_err(err)?;
    let (commands, decoded) =
        jazz::account_registry::migrate_export::migrate_export_account_commands(&storage).await?;
    for (revision, command_json) in commands.into_iter().enumerate() {
        writer
            .write(&dump::Record::Account(dump::Account {
                revision: revision as u64,
                command_json,
            }))
            .map_err(err)?;
    }
    for (principal_json, login_json) in
        jazz::account_registry::migrate_export::migrate_export_account_logins(&decoded)?
    {
        writer
            .write(&dump::Record::AccountLogin {
                principal_json,
                login_json,
            })
            .map_err(err)?;
    }
    storage.close().await.map_err(err)?;
    Ok(())
}

fn convert_version(version: ExportedVersion) -> Result<dump::Version, String> {
    let mut cells = BTreeMap::new();
    for (name, value) in version.cells {
        cells.insert(name, convert_value(&value)?);
    }
    Ok(dump::Version {
        table: version.table,
        schema: *version.schema.0.as_bytes(),
        branch: version
            .branch
            .values
            .iter()
            .map(|(name, value)| (name.clone(), value.0.clone().into()))
            .collect(),
        row: *version.row.0.as_bytes(),
        tx_node: *version.tx.node.as_bytes(),
        tx_time: version.tx.time.0,
        global_time: version.global_time.map(|time| time.0),
        fate: match &version.fate {
            Fate::Accepted => "accepted".to_owned(),
            Fate::Pending => "pending".to_owned(),
            Fate::Rejected(reason) => format!("rejected:{reason:?}"),
        },
        made_by: convert_author(&version.made_by),
        created_by: convert_author(&version.created_by),
        created_at: version.created_at.0,
        updated_by: convert_author(&version.updated_by),
        updated_at: version.updated_at.0,
        deletion: version.deletion.map(|deletion| match deletion {
            DeletionEvent::Deleted => dump::Deletion::Deleted,
            DeletionEvent::Restored => dump::Deletion::Restored,
        }),
        cells,
        user_metadata_json: version.user_metadata_json,
    })
}

fn convert_author(author: &AuthorSubject) -> dump::Author {
    let (issuer, subject) = author.principal_parts();
    let account = author.account_id().map(|account| *account.0.as_bytes());
    match author {
        AuthorSubject::System => dump::Author::System,
        AuthorSubject::SystemAt(_) => dump::Author::SystemAt {
            account,
            issuer,
            subject,
        },
        AuthorSubject::Authenticated(_) => dump::Author::Authenticated {
            account,
            issuer,
            subject,
        },
    }
}

fn convert_value(value: &Value) -> Result<dump::Value, String> {
    let many = |values: &[Value]| {
        values
            .iter()
            .map(convert_value)
            .collect::<Result<Vec<_>, _>>()
    };
    Ok(match value {
        Value::U8(v) => dump::Value::U8(*v),
        Value::U16(v) => dump::Value::U16(*v),
        Value::U32(v) => dump::Value::U32(*v),
        Value::U64(v) => dump::Value::U64(*v),
        Value::F64(v) => dump::Value::F64(*v),
        Value::Bool(v) => dump::Value::Bool(*v),
        Value::String(v) => dump::Value::String(v.clone()),
        Value::Bytes(v) => dump::Value::Bytes(v.clone().into()),
        Value::Uuid(v) => dump::Value::Uuid(*v.as_bytes()),
        Value::EnumTag(v) => dump::Value::EnumTag(*v),
        Value::Tuple(values) => dump::Value::Tuple(many(values)?),
        Value::Array(values) => dump::Value::Array(many(values)?),
        Value::Nullable(None) => dump::Value::Nullable(None),
        Value::Nullable(Some(inner)) => {
            dump::Value::Nullable(Some(Box::new(convert_value(inner)?)))
        }
        Value::I64(v) => dump::Value::I64(*v),
        Value::I32(v) => dump::Value::I32(*v),
        Value::Large(_) => return Err("a large value was not read back inline".to_owned()),
        Value::Record(_) | Value::Enum(_) => {
            return Err(format!(
                "nested record cells are not supported yet: {value:?}"
            ));
        }
    })
}

fn copy_dir(from: &Path, to: &Path) -> Result<(), String> {
    std::fs::create_dir_all(to).map_err(err)?;
    for entry in std::fs::read_dir(from).map_err(err)? {
        let entry = entry.map_err(err)?;
        let target = to.join(entry.file_name());
        if entry.file_type().map_err(err)?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target).map_err(err)?;
        }
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
