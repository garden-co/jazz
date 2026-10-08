//! Per-row durable storage bytes on RocksDB.
//!
//! Measurement harness, not a regression gate: it is ignored by default and only
//! prints a table. Run it with
//!
//! ```text
//! cargo test -p jazz --test integration --features testing \
//!     storage_per_row_bytes -- --ignored --nocapture
//! ```
//!
//! Workload (see `per-row-bytes.md`): one project, then 1000 todos each
//! inserted in its own transaction and then updated once with `done = true`.
//! Core is a history-complete `Db`; the writer is a client `Db` subscribed to
//! both tables at `Global`; the reader connects after every write settled and
//! subscribes to both tables. Every store is RocksDB. After close, every
//! column family is read raw and each entry is attributed to its logical
//! table (class-CF framing `u32be len | name | key`) or, in `indices`, to its
//! `table\0index\0` prefix. Bytes are key plus value.

// Shared with jazz-testkit by path so Jazz needs no testkit dev-dependency.
#[path = "../../jazz-testkit/src/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, UpdateOptions};
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_storage_rocksdb::RocksDbStorage;

mod common;

use common::{allow_all_policies, compile_schema};

const TODOS: usize = 1000;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies())
                    .index_only(Vec::<String>::new()),
            )
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("due_at", ColumnType::Timestamp)
                    .fk_column("project_id", "projects")
                    .policies(allow_all_policies())
                    .index_only(Vec::<String>::new()),
            )
            .build(),
    )
}

fn open(path: &std::path::Path, node: u8, history_complete: bool) -> Db {
    let schema = schema();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(path, &refs).expect("open RocksDB");
    let config = DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: if history_complete {
                AuthorSubject::SYSTEM
            } else {
                AuthorSubject::for_test_bytes([node; 16])
            },
        },
    );
    if history_complete {
        block_on(Db::open_history_complete(config)).expect("open core")
    } else {
        block_on(Db::open(config)).expect("open client")
    }
}

/// UUIDv7-shaped ids: a timestamp prefix and pseudo-random low bits, so key
/// escaping reflects real ids rather than zero-padded fixtures.
fn row_id(index: usize) -> RowUuid {
    let mut state = (index as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    state ^= state >> 31;
    state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state ^= state >> 29;
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&(0x0199_0000_0000u64 + index as u64).to_be_bytes()[2..]);
    bytes[6..].copy_from_slice(&[
        0x70 | (state as u8 & 0x0f),
        (state >> 8) as u8,
        0x80 | ((state >> 16) as u8 & 0x3f),
        (state >> 24) as u8,
        (state >> 32) as u8,
        (state >> 40) as u8,
        (state >> 48) as u8,
        (state >> 56) as u8,
        state as u8 ^ 0x5a,
        (state >> 12) as u8,
    ]);
    RowUuid::from_bytes(bytes)
}

fn settle(dbs: &[&Db]) {
    for _ in 0..400 {
        for db in dbs {
            block_on(db.tick()).expect("tick");
        }
    }
}

#[derive(Default)]
struct Group {
    entries: usize,
    key_bytes: usize,
    value_bytes: usize,
}

fn split_logical(cf: &str, key: &[u8]) -> (String, Vec<u8>) {
    if key.len() >= 4 {
        let len = u32::from_be_bytes(key[..4].try_into().unwrap()) as usize;
        if len > 0
            && key.len() >= 4 + len
            && key[4..4 + len]
                .iter()
                .all(|byte| byte.is_ascii_graphic() && *byte != b'\0')
        {
            return (
                String::from_utf8(key[4..4 + len].to_vec()).unwrap(),
                key[4 + len..].to_vec(),
            );
        }
    }
    (cf.to_owned(), key.to_vec())
}

/// Groove's durable index registry: `\0groove-index-id\0` | u16be table
/// length | table | index -> u32be id | definition.
const INDEX_REGISTRY: &[u8] = b"\0groove-index-id\0";

fn index_registry_entry(key: &[u8], value: &[u8]) -> Option<(u32, String)> {
    let rest = key.strip_prefix(INDEX_REGISTRY)?;
    let table_len = usize::from(u16::from_be_bytes([rest[0], rest[1]]));
    let table = String::from_utf8_lossy(&rest[2..2 + table_len]);
    let index = String::from_utf8_lossy(&rest[2 + table_len..]);
    let id = u32::from_be_bytes(value[..4].try_into().ok()?);
    Some((id, format!("index {table}.{index}")))
}

/// Group an `indices` entry by its LEB128 index id (metadata keys start 00).
fn index_name(key: &[u8], names: &BTreeMap<u32, String>) -> Option<String> {
    if key.first() == Some(&0) {
        return Some("indices metadata".to_owned());
    }
    let (mut id, mut shift) = (0_u32, 0);
    for byte in key {
        id |= u32::from(byte & 0x7f) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
    }
    names.get(&id).cloned()
}

fn logical_entry(family: &str, key: &[u8]) -> (String, Vec<u8>) {
    // The class layout stores the `indices` class without a logical frame.
    if family == "__groove_class_indices" {
        return ("indices".to_owned(), key.to_vec());
    }
    split_logical(family, key)
}

fn measure(path: &std::path::Path) -> BTreeMap<String, Group> {
    let options = rocksdb::Options::default();
    let families = rocksdb::DB::list_cf(&options, path).expect("list column families");
    let db = rocksdb::DB::open_cf_for_read_only(&options, path, &families, false)
        .expect("open RocksDB read-only");
    let mut names = BTreeMap::new();
    for family in &families {
        let handle = db.cf_handle(family).expect("column family handle");
        for entry in db.iterator_cf(&handle, rocksdb::IteratorMode::Start) {
            let (key, value) = entry.expect("raw entry");
            let (logical, rest) = logical_entry(family, &key);
            if logical == "indices"
                && let Some((id, name)) = index_registry_entry(&rest, &value)
            {
                names.insert(id, name);
            }
        }
    }
    let mut groups = BTreeMap::<String, Group>::new();
    for family in &families {
        let handle = db.cf_handle(family).expect("column family handle");
        for entry in db.iterator_cf(&handle, rocksdb::IteratorMode::Start) {
            let (key, value) = entry.expect("raw entry");
            let (logical, rest) = logical_entry(family, &key);
            let group = if logical == "indices" {
                index_name(&rest, &names).unwrap_or(logical)
            } else {
                logical
            };
            let group = groups.entry(group).or_default();
            group.entries += 1;
            group.key_bytes += key.len();
            group.value_bytes += value.len();
        }
    }
    groups
}

fn report(label: &str, groups: &BTreeMap<String, Group>) {
    println!("\n## {label} (bytes per row, N = {TODOS})\n");
    println!("| logical table | entries | key/e | val/e | raw / row |");
    println!("|---|---:|---:|---:|---:|");
    let mut total = 0usize;
    for (name, group) in groups {
        let bytes = group.key_bytes + group.value_bytes;
        // Fixed per-store metadata (catalogue, schemas, markers, nodes) is
        // listed but does not scale with rows.
        total += bytes;
        println!(
            "| `{name}` | {} | {} | {} | {:.0} |",
            group.entries,
            group.key_bytes / group.entries.max(1),
            group.value_bytes / group.entries.max(1),
            bytes as f64 / TODOS as f64
        );
    }
    println!(
        "| **total** | | | | **{:.0}** |",
        total as f64 / TODOS as f64
    );
}

#[test]
#[ignore = "#1787: manual storage measurement receipt; run with --ignored --nocapture"]
fn storage_per_row_bytes() {
    let root = tempfile::tempdir().expect("temporary directory");
    let core_path = root.path().join("core");
    let writer_path = root.path().join("writer");
    let reader_path = root.path().join("reader");
    {
        let core = open(&core_path, 0x51, true);
        let writer = open(&writer_path, 0x61, false);
        let (client_transport, server_transport) = duplex();
        let _upstream = block_on(writer.connect_upstream(client_transport));
        let _subscriber =
            core.accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0x61; 16]));
        let global = ReadOpts {
            tier: jazz::db::ReadTier::Remote,
            ..ReadOpts::default()
        };
        let projects = writer.prepare_query(&Query::from("projects")).unwrap();
        let todos = writer.prepare_query(&Query::from("todos")).unwrap();
        let _projects_sub = block_on(writer.subscribe(&projects, global.clone())).unwrap();
        let _todos_sub = block_on(writer.subscribe(&todos, global.clone())).unwrap();
        settle(&[&writer, &core]);

        let project = RowUuid::from_bytes([0x0a; 16]);
        block_on(writer.insert(
            "projects",
            BTreeMap::from([("name".to_owned(), Value::String("Home".to_owned()))]),
            InsertOptions {
                row_id: Some(project),
                ..Default::default()
            },
        ))
        .expect("insert project");
        for index in 0..TODOS {
            block_on(writer.insert(
                "todos",
                BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String(format!("Buy groceries #{index:05}")),
                    ),
                    ("done".to_owned(), Value::Bool(false)),
                    (
                        "due_at".to_owned(),
                        Value::U64(1_790_000_000_000 + index as u64),
                    ),
                    ("project_id".to_owned(), Value::Uuid(project.0)),
                ]),
                InsertOptions {
                    row_id: Some(row_id(index)),
                    ..Default::default()
                },
            ))
            .expect("insert todo");
            if index % 50 == 0 {
                settle(&[&writer, &core]);
            }
        }
        settle(&[&writer, &core]);
        assert_eq!(
            block_on(writer.all(&todos, global.clone())).unwrap().len(),
            TODOS,
            "every insert settled"
        );
        for index in 0..TODOS {
            block_on(writer.update(
                "todos",
                row_id(index),
                BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
                UpdateOptions::default(),
            ))
            .expect("update todo");
            if index % 50 == 0 {
                settle(&[&writer, &core]);
            }
        }
        settle(&[&writer, &core]);

        let reader = open(&reader_path, 0x71, false);
        let (client_transport, server_transport) = duplex();
        let _reader_upstream = block_on(reader.connect_upstream(client_transport));
        let _reader_subscriber = core.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
        let reader_projects = reader.prepare_query(&Query::from("projects")).unwrap();
        let reader_todos = reader.prepare_query(&Query::from("todos")).unwrap();
        let _rp = block_on(reader.subscribe(&reader_projects, global.clone())).unwrap();
        let _rt = block_on(reader.subscribe(&reader_todos, global.clone())).unwrap();
        settle(&[&reader, &core, &writer]);

        let schema = schema();
        let todos_table = schema
            .tables
            .iter()
            .find(|table| table.name == "todos")
            .unwrap()
            .clone();
        let rows = block_on(reader.all(&reader_todos, global)).expect("reader rows");
        assert_eq!(rows.len(), TODOS, "reader sees every todo");
        assert!(
            rows.iter()
                .all(|row| row.cell(&todos_table, "done") == Some(Value::Bool(true))),
            "reader sees every update"
        );
    }
    report("Core (history complete)", &measure(&core_path));
    report("Writer client", &measure(&writer_path));
    report("Reader client", &measure(&reader_path));
}
