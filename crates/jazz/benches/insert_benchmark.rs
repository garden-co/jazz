//! Insert throughput benchmark for permissioned core operations.
//!
//! Measures inserts/second with public `jazz::db::Db<MemoryStorage>` APIs.
//!
//! Variants:
//! - Insert into an owned folder (direct owner write policy)
//! - Insert into a team-access folder (direct folder-access policy)
//! - Batch insert into owned folders

#![allow(clippy::single_element_loop)]

use std::collections::BTreeMap;

mod schema_fixture;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{CmpOp, PolicyValue};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

type BenchDb = Db<MemoryStorage>;

fn author() -> AuthorSubject {
    AuthorSubject::for_test_uuid(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"))
}
fn other_author() -> AuthorSubject {
    AuthorSubject::for_test_uuid(uuid::uuid!("00000000-0000-0000-0000-0000000000b2"))
}

fn public_schema_convert() -> JazzSchema {
    let folder_owner = schema_fixture::session_user_id_column("owner");
    let folder_access = PolicyExpr::Exists {
        table: "folder_access".to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            PolicyExpr::Cmp {
                column: "folder".to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec![
                    "__jazz_outer_row".to_owned(),
                    "folder".to_owned(),
                ]),
            },
            schema_fixture::session_user_id_column("user"),
        ])),
    };
    schema_fixture::compile(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("folders")
                    .column("name", ColumnType::Text)
                    .column("owner", ColumnType::Uuid)
                    .column("created_at", ColumnType::Timestamp)
                    .policies(schema_fixture::all_operations(folder_owner)),
            )
            .table(
                TableSchemaBuilder::new("folder_access")
                    .fk_column("folder", "folders")
                    .column("user", ColumnType::Uuid)
                    .column("role", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("documents")
                    .fk_column("folder", "folders")
                    .column("title", ColumnType::Text)
                    .column("content", ColumnType::Text)
                    .column("author", ColumnType::Uuid)
                    .column("created_at", ColumnType::Timestamp)
                    .policies(schema_fixture::write_operations(folder_access)),
            ),
    )
}

fn open_db(seed: u64) -> BenchDb {
    let schema = public_schema_convert();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    // Open the public core database path directly. This benchmark should
    // not route inserts through legacy runtime/schema/sync manager layers.
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([seed as u8; 16]),
                author: author(),
            },
        )
        .with_id_source(SeededRowIdSource::new(seed)),
    ))
    .expect("open core insert benchmark db")
}

fn row_uuid(index: usize) -> RowUuid {
    RowUuid::from_bytes([(index % 251 + 1) as u8; 16])
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn folder_cells(index: usize, owner: AuthorSubject) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("name".to_owned(), Value::String(format!("Folder {index}"))),
        ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
        ("created_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn access_cells(folder: RowUuid, user: AuthorSubject, role: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(folder.0)),
        ("user".to_owned(), Value::Uuid(user.test_uuid())),
        ("role".to_owned(), Value::String(role.to_owned())),
    ])
}

fn document_cells(
    folder: RowUuid,
    title: String,
    content: &'static str,
    author: AuthorSubject,
    created_at: u64,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(folder.0)),
        ("title".to_owned(), Value::String(title)),
        ("content".to_owned(), Value::String(content.to_owned())),
        ("author".to_owned(), Value::Uuid(author.test_uuid())),
        ("created_at".to_owned(), Value::U64(created_at)),
    ])
}

struct BenchmarkData {
    owned_folders: Vec<RowUuid>,
    team_folders: Vec<RowUuid>,
}

fn wait_local(write: jazz::db::WriteHandle<MemoryStorage>) -> RowUuid {
    block_on(write.wait(DurabilityTier::Local)).expect("write should be local");
    write.row_uuid()
}

fn seed_data(db: &BenchDb, scale: usize) -> BenchmarkData {
    let num_folders = (scale / 10).max(100);
    let owned_folder_count = (num_folders / 10).max(1);
    let team_folder_count = (num_folders / 10).max(1);

    let mut owned_folders = Vec::with_capacity(owned_folder_count);
    let mut team_folders = Vec::with_capacity(team_folder_count);

    for index in 0..num_folders {
        let folder = row_uuid(index);
        let is_owned = index < owned_folder_count;
        let is_team_accessible =
            index >= owned_folder_count && index < owned_folder_count + team_folder_count;
        let owner = if is_owned { author() } else { other_author() };

        let write = db
            .insert(
                "folders",
                folder_cells(index, owner),
                jazz::db::InsertOptions {
                    row_id: Some(folder),
                    ..Default::default()
                },
            )
            .expect("seed folder");
        wait_local(write);

        if is_owned || is_team_accessible {
            let role = if is_owned { "owner" } else { "member" };
            let write = db
                .insert(
                    "folder_access",
                    access_cells(folder, author(), role),
                    Default::default(),
                )
                .expect("seed folder access");
            wait_local(write);
        }

        if is_owned {
            owned_folders.push(folder);
        } else if is_team_accessible {
            team_folders.push(folder);
        }
    }

    for index in 0..scale {
        let folder = if index.is_multiple_of(2) {
            owned_folders[(index / 2) % owned_folders.len()]
        } else {
            team_folders[(index / 2) % team_folders.len()]
        };
        let author = if index.is_multiple_of(2) {
            author()
        } else {
            other_author()
        };
        let write = db
            .insert(
                "documents",
                document_cells(
                    folder,
                    format!("Document {index}"),
                    "Seed document content",
                    author,
                    index as u64,
                ),
                Default::default(),
            )
            .expect("seed document");
        wait_local(write);
    }

    BenchmarkData {
        owned_folders,
        team_folders,
    }
}

fn insert_own_folder(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/own_folder");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            let db = open_db(1);
            let data = seed_data(&db, scale);
            let folder = data.owned_folders[0];
            let mut doc_counter = 0u64;

            b.iter(|| {
                doc_counter += 1;
                let write = db
                    .insert(
                        "documents",
                        document_cells(
                            folder,
                            format!("Bench Doc {doc_counter}"),
                            "Benchmark content",
                            author(),
                            current_timestamp(),
                        ),
                        Default::default(),
                    )
                    .expect("own-folder insert should succeed");
                wait_local(write)
            });
        });
    }

    group.finish();
}

fn insert_team_folder(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/team_folder");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            let db = open_db(2);
            let data = seed_data(&db, scale);
            let folder = data.team_folders[0];
            let mut doc_counter = 0u64;

            b.iter(|| {
                doc_counter += 1;
                let write = db
                    .insert(
                        "documents",
                        document_cells(
                            folder,
                            format!("Team Doc {doc_counter}"),
                            "Team benchmark content",
                            other_author(),
                            current_timestamp(),
                        ),
                        Default::default(),
                    )
                    .expect("team-folder insert should succeed via folder access");
                wait_local(write)
            });
        });
    }

    group.finish();
}

fn insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/batch");

    for scale in [1_000usize] {
        let batch_size = 100;
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("documents_x100", scale),
            &scale,
            |b, &scale| {
                let db = open_db(3);
                let data = seed_data(&db, scale);
                let folders = data
                    .owned_folders
                    .iter()
                    .cycle()
                    .take(batch_size)
                    .copied()
                    .collect::<Vec<_>>();
                let mut batch_counter = 0u64;

                b.iter(|| {
                    batch_counter += 1;
                    let timestamp = current_timestamp();

                    for (index, folder) in folders.iter().copied().enumerate() {
                        let write = db
                            .insert(
                                "documents",
                                document_cells(
                                    folder,
                                    format!("Batch {batch_counter} Doc {index}"),
                                    "Batch content",
                                    author(),
                                    timestamp + index as u64,
                                ),
                                Default::default(),
                            )
                            .expect("batch insert should succeed");
                        wait_local(write);
                    }
                });
            },
        );
    }

    group.finish();
}

fn guarded_benches(c: &mut Criterion) {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    insert_own_folder(c);
    insert_team_folder(c);
    insert_batch(c);
}

criterion_group!(benches, guarded_benches);
criterion_main!(benches);
mod support;

use support::BenchFutureExt as _;
