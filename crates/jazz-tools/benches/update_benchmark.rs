//! Update throughput benchmark for permissioned operations.
//!
//! Measures updates/second with USING + WITH CHECK policy evaluation.
//!
//! Variants:
//! - Update own documents (simple USING check)

mod common;

use common::{create_runtime, create_session, current_timestamp, setup_data};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz::query_manager::types::Value;

const USER_ID: &str = "benchmark_user";

fn update_own_documents(c: &mut Criterion) {
    let mut group = c.benchmark_group("update/own_documents");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            // Setup
            let mut core = create_runtime();
            let data = setup_data(&mut core, scale, USER_ID);
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
                let result = core.update(
                    doc_id,
                    vec![
                        ("folder_id".to_string(), Value::Uuid(folder_id)),
                        (
                            "title".to_string(),
                            Value::Text(format!("Updated Title {}", update_counter)),
                        ),
                        (
                            "content".to_string(),
                            Value::Text("Updated content".to_string()),
                        ),
                        ("author_id".to_string(), Value::Text(USER_ID.to_string())),
                        ("created_at".to_string(), Value::Timestamp(timestamp)),
                    ],
                    Some(&session),
                );
                core.immediate_tick();

                result.expect("update own document should succeed")
            });
        });
    }

    group.finish();
}

fn update_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("update/batch");

    for scale in [1_000usize] {
        let batch_size = 100;
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("documents_x100", scale),
            &scale,
            |b, &scale| {
                let mut core = create_runtime();
                let data = setup_data(&mut core, scale, USER_ID);
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
                        let result = core.update(
                            doc_id,
                            vec![
                                ("folder_id".to_string(), Value::Uuid(folder_id)),
                                (
                                    "title".to_string(),
                                    Value::Text(format!("Batch {} Update {}", batch_counter, i)),
                                ),
                                (
                                    "content".to_string(),
                                    Value::Text("Batch updated content".to_string()),
                                ),
                                ("author_id".to_string(), Value::Text(USER_ID.to_string())),
                                (
                                    "created_at".to_string(),
                                    Value::Timestamp(timestamp + i as u64),
                                ),
                            ],
                            Some(&session),
                        );
                        result.expect("batch update should succeed");
                    }
                    core.immediate_tick();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, update_own_documents, update_batch);
criterion_main!(benches);
