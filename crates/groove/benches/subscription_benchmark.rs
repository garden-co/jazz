//! Subscription latency benchmarks for permissioned operations.
//!
//! Measures:
//! - Single subscription: time from insert to update appearing
//! - Fan-out: time to notify 100 subscriptions
//! - Complex query: overhead of ORDER BY + LIMIT
//! - Cold start: time to receive initial result set

mod common;

use common::{create_query_manager, create_session, current_timestamp, setup_data};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use groove::query_manager::types::Value;

const USER_ID: &str = "benchmark_user";

/// Measure latency from insert to subscription update.
fn single_subscription_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/single_latency");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            // Setup
            let mut qm = create_query_manager();
            let data = setup_data(&mut qm, scale, USER_ID);
            let session = create_session(USER_ID);

            // Subscribe to documents with policy filtering
            let query = qm.query("documents").build();
            let _sub_id = qm
                .subscribe_with_session(query, Some(session.clone()))
                .expect("subscribe");
            qm.process();
            qm.take_updates(); // Clear initial updates

            let folder_id = data.owned_folders[0];
            let mut doc_counter = 0u64;

            b.iter(|| {
                doc_counter += 1;
                let timestamp = current_timestamp();

                // Insert a document
                let _handle = qm
                    .insert_with_session(
                        "documents",
                        &[
                            Value::Uuid(folder_id),
                            Value::Text(format!("Sub Doc {}", doc_counter)),
                            Value::Text("Subscription test".to_string()),
                            Value::Text(USER_ID.to_string()),
                            Value::Timestamp(timestamp),
                        ],
                        Some(&session),
                    )
                    .expect("insert");

                // Process and wait for update
                qm.process();
                let updates = qm.take_updates();

                // Should have exactly one update with one added row
                assert!(!updates.is_empty(), "should receive subscription update");
            });
        });
    }

    group.finish();
}

/// Measure fan-out latency: time to notify multiple subscriptions.
fn fanout_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/fanout");

    for scale in [1_000usize] {
        let num_subscriptions = 100;
        group.throughput(Throughput::Elements(num_subscriptions));
        group.bench_with_input(
            BenchmarkId::new("subscriptions_x100", scale),
            &scale,
            |b, &scale| {
                // Setup
                let mut qm = create_query_manager();
                let data = setup_data(&mut qm, scale, USER_ID);

                // Create 100 subscriptions with different sessions (same user for simplicity)
                let mut sub_ids = Vec::with_capacity(num_subscriptions as usize);
                for _ in 0..num_subscriptions {
                    // For fan-out, all sessions should see the same data
                    // We'll use the base user ID since they own the folders
                    let session = create_session(USER_ID);
                    let query = qm.query("documents").build();
                    let sub_id = qm
                        .subscribe_with_session(query, Some(session))
                        .expect("subscribe");
                    sub_ids.push(sub_id);
                }
                qm.process();
                qm.take_updates(); // Clear initial

                let session = create_session(USER_ID);
                let folder_id = data.owned_folders[0];
                let mut doc_counter = 0u64;

                b.iter(|| {
                    doc_counter += 1;
                    let timestamp = current_timestamp();

                    // Insert a document that all subscriptions can see
                    let _handle = qm
                        .insert_with_session(
                            "documents",
                            &[
                                Value::Uuid(folder_id),
                                Value::Text(format!("Fanout Doc {}", doc_counter)),
                                Value::Text("Fanout test".to_string()),
                                Value::Text(USER_ID.to_string()),
                                Value::Timestamp(timestamp),
                            ],
                            Some(&session),
                        )
                        .expect("insert");

                    // Process - should update all 100 subscriptions
                    qm.process();
                    let updates = qm.take_updates();

                    // Should have updates for all subscriptions
                    assert!(
                        updates.len() >= num_subscriptions as usize,
                        "should notify all {} subscriptions, got {}",
                        num_subscriptions,
                        updates.len()
                    );
                });
            },
        );
    }

    group.finish();
}

/// Measure complex query latency: ORDER BY + LIMIT.
fn complex_query_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/complex_query");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("order_limit", scale),
            &scale,
            |b, &scale| {
                // Setup
                let mut qm = create_query_manager();
                let data = setup_data(&mut qm, scale, USER_ID);
                let session = create_session(USER_ID);

                // Subscribe with ORDER BY created_at DESC LIMIT 50
                let query = qm
                    .query("documents")
                    .order_by_desc("created_at")
                    .limit(50)
                    .build();
                let _sub_id = qm
                    .subscribe_with_session(query, Some(session.clone()))
                    .expect("subscribe");
                qm.process();
                qm.take_updates(); // Clear initial

                let folder_id = data.owned_folders[0];
                let mut doc_counter = 0u64;

                b.iter(|| {
                    doc_counter += 1;
                    // Use a timestamp that ensures this doc appears in top 50
                    let timestamp = current_timestamp() + 1_000_000_000; // Far future

                    // Insert a document
                    let _handle = qm
                        .insert_with_session(
                            "documents",
                            &[
                                Value::Uuid(folder_id),
                                Value::Text(format!("Complex Doc {}", doc_counter)),
                                Value::Text("Complex query test".to_string()),
                                Value::Text(USER_ID.to_string()),
                                Value::Timestamp(timestamp),
                            ],
                            Some(&session),
                        )
                        .expect("insert");

                    // Process
                    qm.process();
                    let updates = qm.take_updates();

                    // Should have update (new doc in top 50)
                    assert!(!updates.is_empty(), "should receive update for top-50 doc");
                });
            },
        );
    }

    group.finish();
}

/// Measure cold start: time to get initial result set.
fn cold_start_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/cold_start");

    for scale in [1_000usize] {
        group.bench_with_input(
            BenchmarkId::new("initial_load", scale),
            &scale,
            |b, &scale| {
                // Setup data once (reused across iterations)
                let mut qm = create_query_manager();
                let _data = setup_data(&mut qm, scale, USER_ID);

                b.iter(|| {
                    let session = create_session(USER_ID);

                    // Fresh subscription
                    let query = qm.query("documents").build();
                    let sub_id = qm
                        .subscribe_with_session(query, Some(session))
                        .expect("subscribe");

                    // Process to receive initial results
                    qm.process();
                    let updates = qm.take_updates();

                    // Should have initial result set
                    assert!(
                        updates.iter().any(|u| u.subscription_id == sub_id),
                        "should receive initial results"
                    );

                    // Clean up subscription
                    qm.unsubscribe(sub_id);
                });
            },
        );
    }

    group.finish();
}

/// Measure filtered subscription: only see subset of documents.
fn filtered_subscription_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/filtered");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("author_filter", scale),
            &scale,
            |b, &scale| {
                // Setup
                let mut qm = create_query_manager();
                let data = setup_data(&mut qm, scale, USER_ID);
                let session = create_session(USER_ID);

                // Subscribe only to user's own documents
                let query = qm
                    .query("documents")
                    .filter_eq("author_id", Value::Text(USER_ID.to_string()))
                    .build();
                let _sub_id = qm
                    .subscribe_with_session(query, Some(session.clone()))
                    .expect("subscribe");
                qm.process();
                qm.take_updates(); // Clear initial

                let folder_id = data.owned_folders[0];
                let mut doc_counter = 0u64;

                b.iter(|| {
                    doc_counter += 1;
                    let timestamp = current_timestamp();

                    // Insert a document authored by user (should appear in subscription)
                    let _handle = qm
                        .insert_with_session(
                            "documents",
                            &[
                                Value::Uuid(folder_id),
                                Value::Text(format!("Filtered Doc {}", doc_counter)),
                                Value::Text("Filtered test".to_string()),
                                Value::Text(USER_ID.to_string()),
                                Value::Timestamp(timestamp),
                            ],
                            Some(&session),
                        )
                        .expect("insert");

                    qm.process();
                    let updates = qm.take_updates();

                    assert!(!updates.is_empty(), "should receive filtered update");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    single_subscription_latency,
    fanout_latency,
    complex_query_latency,
    cold_start_latency,
    filtered_subscription_latency
);
criterion_main!(benches);
