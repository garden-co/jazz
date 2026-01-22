//! Insert throughput benchmark for permissioned operations.
//!
//! Measures inserts/second with policy evaluation at different scales.
//!
//! Variants:
//! - Insert into own folder (simple WITH CHECK)
//! - Insert into team folder (INHERITS chain evaluation)

mod common;

use common::{create_query_manager, create_session, current_timestamp, setup_data};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use groove::query_manager::types::Value;

const USER_ID: &str = "benchmark_user";

fn insert_own_folder(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/own_folder");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            // Setup: create data at scale
            let mut qm = create_query_manager();
            let data = setup_data(&mut qm, scale, USER_ID);
            let session = create_session(USER_ID);

            // Pick a folder owned by the user
            let folder_id = data.owned_folders[0];
            let mut doc_counter = 0u64;

            b.iter(|| {
                doc_counter += 1;
                let timestamp = current_timestamp();

                // Insert with session - exercises WITH CHECK policy (INHERITS chain)
                let result = qm.insert_with_session(
                    "documents",
                    &[
                        Value::Uuid(folder_id),
                        Value::Text(format!("Bench Doc {}", doc_counter)),
                        Value::Text("Benchmark content".to_string()),
                        Value::Text(USER_ID.to_string()),
                        Value::Timestamp(timestamp),
                    ],
                    Some(&session),
                );
                qm.process();

                result.expect("insert should succeed")
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
            // Setup: create data at scale
            // For this benchmark, we modify setup to give user access to some "other" folders
            // by making them owner of more teams
            let mut qm = create_query_manager();
            let data = setup_data(&mut qm, scale, USER_ID);
            let session = create_session(USER_ID);

            // Use a folder from owned teams (which exercises INHERITS chain)
            let folder_id = data.owned_folders[data.owned_folders.len() / 2];
            let mut doc_counter = 0u64;

            b.iter(|| {
                doc_counter += 1;
                let timestamp = current_timestamp();

                // Insert authored by a different user but in accessible folder
                // This exercises the INHERITS SELECT VIA folder_id chain
                let result = qm.insert_with_session(
                    "documents",
                    &[
                        Value::Uuid(folder_id),
                        Value::Text(format!("Team Doc {}", doc_counter)),
                        Value::Text("Team benchmark content".to_string()),
                        Value::Text("other_author".to_string()), // Different author
                        Value::Timestamp(timestamp),
                    ],
                    Some(&session),
                );
                qm.process();

                result.expect("insert should succeed via INHERITS")
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
                let mut qm = create_query_manager();
                let data = setup_data(&mut qm, scale, USER_ID);
                let session = create_session(USER_ID);

                let folder_ids: Vec<_> = data
                    .owned_folders
                    .iter()
                    .cycle()
                    .take(batch_size)
                    .copied()
                    .collect();
                let mut batch_counter = 0u64;

                b.iter(|| {
                    batch_counter += 1;
                    let timestamp = current_timestamp();

                    // Insert batch of documents
                    for (i, &folder_id) in folder_ids.iter().enumerate() {
                        let result = qm.insert_with_session(
                            "documents",
                            &[
                                Value::Uuid(folder_id),
                                Value::Text(format!("Batch {} Doc {}", batch_counter, i)),
                                Value::Text("Batch content".to_string()),
                                Value::Text(USER_ID.to_string()),
                                Value::Timestamp(timestamp + i as u64),
                            ],
                            Some(&session),
                        );
                        result.expect("batch insert should succeed");
                    }
                    qm.process();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, insert_own_folder, insert_team_folder, insert_batch);
criterion_main!(benches);
