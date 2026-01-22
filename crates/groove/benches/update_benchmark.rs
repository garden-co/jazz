//! Update throughput benchmark for permissioned operations.
//!
//! Measures updates/second with USING + WITH CHECK policy evaluation.
//!
//! Variants:
//! - Update own documents (simple USING check)
//! - Update team documents (INHERITS evaluation)

mod common;

use common::{create_query_manager, create_session, current_timestamp, setup_data};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use groove::query_manager::types::Value;

const USER_ID: &str = "benchmark_user";

fn update_own_documents(c: &mut Criterion) {
    let mut group = c.benchmark_group("update/own_documents");

    for scale in [10_000usize, 100_000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            // Setup
            let mut qm = create_query_manager();
            let data = setup_data(&mut qm, scale, USER_ID);
            let session = create_session(USER_ID);

            // We'll cycle through owned documents
            let mut doc_idx = 0;
            let mut update_counter = 0u64;

            b.iter(|| {
                update_counter += 1;
                let doc_id = data.owned_documents[doc_idx % data.owned_documents.len()];
                doc_idx += 1;

                // Get the folder_id from an owned folder (need to maintain FK)
                let folder_id = data.owned_folders[doc_idx % data.owned_folders.len()];
                let timestamp = current_timestamp();

                // Update with session - exercises USING (author_id check) + WITH CHECK
                let result = qm.update_with_session(
                    doc_id,
                    &[
                        Value::Uuid(folder_id),
                        Value::Text(format!("Updated Title {}", update_counter)),
                        Value::Text("Updated content".to_string()),
                        Value::Text(USER_ID.to_string()),
                        Value::Timestamp(timestamp),
                    ],
                    Some(&session),
                );
                qm.process();

                result.expect("update own document should succeed")
            });
        });
    }

    group.finish();
}

fn update_team_documents(c: &mut Criterion) {
    let mut group = c.benchmark_group("update/team_documents");

    for scale in [10_000usize, 100_000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            // Setup: Create data where user has team access to documents authored by others
            let mut qm = create_query_manager();
            let data = setup_data(&mut qm, scale, USER_ID);
            let session = create_session(USER_ID);

            // For team documents, we need docs in folders the user has access to
            // but authored by others. Our setup_data creates these in owned_folders
            // Let's insert some team documents for this benchmark
            let folder_id = data.owned_folders[0];
            let mut team_doc_ids = Vec::new();
            for i in 0..100 {
                let handle = qm
                    .insert(
                        "documents",
                        &[
                            Value::Uuid(folder_id),
                            Value::Text(format!("Team Doc {}", i)),
                            Value::Text("Team content".to_string()),
                            Value::Text("other_author".to_string()),
                            Value::Timestamp(current_timestamp() + i as u64),
                        ],
                    )
                    .expect("setup team doc");
                qm.process();
                team_doc_ids.push(handle.row_id);
            }

            let mut doc_idx = 0;
            let mut update_counter = 0u64;

            b.iter(|| {
                update_counter += 1;
                let doc_id = team_doc_ids[doc_idx % team_doc_ids.len()];
                doc_idx += 1;

                let timestamp = current_timestamp();

                // Update with session - exercises INHERITS for both USING and WITH CHECK
                // The USING policy passes via INHERITS (folder access), not author_id
                let result = qm.update_with_session(
                    doc_id,
                    &[
                        Value::Uuid(folder_id), // Keep in same folder
                        Value::Text(format!("Team Updated {}", update_counter)),
                        Value::Text("Team updated content".to_string()),
                        Value::Text("other_author".to_string()), // Keep original author
                        Value::Timestamp(timestamp),
                    ],
                    Some(&session),
                );
                qm.process();

                result.expect("update team document should succeed via INHERITS")
            });
        });
    }

    group.finish();
}

fn update_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("update/batch");

    for scale in [10_000usize, 100_000] {
        let batch_size = 100;
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("documents_x100", scale),
            &scale,
            |b, &scale| {
                let mut qm = create_query_manager();
                let data = setup_data(&mut qm, scale, USER_ID);
                let session = create_session(USER_ID);

                // Use subset of owned documents for batch updates
                let doc_ids: Vec<_> = data
                    .owned_documents
                    .iter()
                    .cycle()
                    .take(batch_size)
                    .copied()
                    .collect();
                let folder_id = data.owned_folders[0];
                let mut batch_counter = 0u64;

                b.iter(|| {
                    batch_counter += 1;
                    let timestamp = current_timestamp();

                    // Update batch of documents
                    for (i, &doc_id) in doc_ids.iter().enumerate() {
                        let result = qm.update_with_session(
                            doc_id,
                            &[
                                Value::Uuid(folder_id),
                                Value::Text(format!("Batch {} Update {}", batch_counter, i)),
                                Value::Text("Batch updated content".to_string()),
                                Value::Text(USER_ID.to_string()),
                                Value::Timestamp(timestamp + i as u64),
                            ],
                            Some(&session),
                        );
                        result.expect("batch update should succeed");
                    }
                    qm.process();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    update_own_documents,
    update_team_documents,
    update_batch
);
criterion_main!(benches);
