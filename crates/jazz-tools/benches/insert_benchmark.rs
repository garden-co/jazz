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

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource, block_on};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{Query, claim, col, eq};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

type BenchDb = Db<MemoryStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));
const OTHER_AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000b2"));

fn schema_convert() -> JazzSchema {
    let folder_owner_policy =
        Policy::shape(Query::from("folders").filter(eq(col("owner"), claim("sub"))));
    let folder_access_policy = Policy::shape(Query::from("documents").join_via_column(
        "folder_access",
        "folder",
        "folder",
        [eq(col("user"), claim("sub"))],
    ));

    JazzSchema::new([
        TableSchema::new(
            "folders",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("created_at", ColumnType::U64),
            ],
        )
        .with_read_policy(folder_owner_policy.clone())
        .with_write_policy(folder_owner_policy),
        TableSchema::new(
            "folder_access",
            [
                ColumnSchema::new("folder", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
            ],
        )
        .with_reference("folder", "folders")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "documents",
            [
                ColumnSchema::new("folder", ColumnType::Uuid),
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("content", ColumnType::String),
                ColumnSchema::new("author", ColumnType::Uuid),
                ColumnSchema::new("created_at", ColumnType::U64),
            ],
        )
        .with_reference("folder", "folders")
        .with_read_policy(Policy::public())
        .with_write_policy(folder_access_policy),
    ])
}

fn open_db(seed: u64) -> BenchDb {
    let schema = schema_convert();
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
                author: AUTHOR,
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

fn folder_cells(index: usize, owner: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("name".to_owned(), Value::String(format!("Folder {index}"))),
        ("owner".to_owned(), Value::Uuid(owner.0)),
        ("created_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn access_cells(folder: RowUuid, user: AuthorId, role: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(folder.0)),
        ("user".to_owned(), Value::Uuid(user.0)),
        ("role".to_owned(), Value::String(role.to_owned())),
    ])
}

fn document_cells(
    folder: RowUuid,
    title: String,
    content: &'static str,
    author: AuthorId,
    created_at: u64,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(folder.0)),
        ("title".to_owned(), Value::String(title)),
        ("content".to_owned(), Value::String(content.to_owned())),
        ("author".to_owned(), Value::Uuid(author.0)),
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
        let owner = if is_owned { AUTHOR } else { OTHER_AUTHOR };

        let write = db
            .insert_with_id("folders", folder, folder_cells(index, owner))
            .expect("seed folder");
        wait_local(write);

        if is_owned || is_team_accessible {
            let role = if is_owned { "owner" } else { "member" };
            let write = db
                .insert("folder_access", access_cells(folder, AUTHOR, role))
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
        let folder = if index % 2 == 0 {
            owned_folders[(index / 2) % owned_folders.len()]
        } else {
            team_folders[(index / 2) % team_folders.len()]
        };
        let author = if index % 2 == 0 { AUTHOR } else { OTHER_AUTHOR };
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
                            AUTHOR,
                            current_timestamp(),
                        ),
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
                            OTHER_AUTHOR,
                            current_timestamp(),
                        ),
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
                                    AUTHOR,
                                    timestamp + index as u64,
                                ),
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

criterion_group!(benches, insert_own_folder, insert_team_folder, insert_batch);
criterion_main!(benches);
