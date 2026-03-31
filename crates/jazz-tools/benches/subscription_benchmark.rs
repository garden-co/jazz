//! Subscription latency benchmarks for permissioned operations.
//!
//! Measures:
//! - Single subscription: time from insert to update appearing
//! - Fan-out: time to notify 100 subscriptions
//! - Cold start: time to receive initial result set

#![allow(clippy::single_element_loop)]

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::{create_runtime, create_session, current_timestamp, setup_data};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz_tools::query_manager::query::{Query, QueryBuilder};
use jazz_tools::query_manager::session::WriteContext;
use jazz_tools::query_manager::types::Value;

const USER_ID: &str = "benchmark_user";

fn row<const N: usize>(pairs: [(&str, Value); N]) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

/// Measure latency from insert to subscription update.
fn single_subscription_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/single_latency");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            // Setup
            let mut core = create_runtime();
            let data = setup_data(&mut core, scale, USER_ID);
            let session = create_session(USER_ID);
            let write_context = WriteContext::from_session(session.clone());

            // Track updates via callback
            let update_count = Arc::new(AtomicUsize::new(0));
            let update_count_clone = update_count.clone();

            // Subscribe to documents with policy filtering
            let query = Query::new("documents");
            let _sub_handle = core
                .subscribe(
                    query,
                    move |_delta| {
                        update_count_clone.fetch_add(1, Ordering::SeqCst);
                    },
                    Some(session.clone()),
                )
                .expect("subscribe");
            core.immediate_tick();
            core.batched_tick();
            update_count.store(0, Ordering::SeqCst); // Clear initial update count

            let folder_id = data.owned_folders[0];
            let mut doc_counter = 0u64;

            b.iter(|| {
                doc_counter += 1;
                let timestamp = current_timestamp();

                // Insert a document
                let _id = core
                    .insert(
                        "documents",
                        row([
                            ("folder_id", Value::Uuid(folder_id)),
                            ("title", Value::Text(format!("Sub Doc {}", doc_counter))),
                            ("content", Value::Text("Subscription test".to_string())),
                            ("author_id", Value::Text(USER_ID.to_string())),
                            ("created_at", Value::Timestamp(timestamp)),
                        ]),
                        Some(&write_context),
                    )
                    .expect("insert");

                // Process and wait for update
                core.immediate_tick();
                let count = update_count.load(Ordering::SeqCst);

                // Should have received an update
                assert!(count > 0, "should receive subscription update");
                update_count.store(0, Ordering::SeqCst); // Reset for next iteration
            });
        });
    }

    group.finish();
}

/// Measure fan-out latency: time to notify multiple subscriptions.
fn fanout_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/fanout");

    for scale in [1_000usize] {
        let num_subscriptions = 100usize;
        group.throughput(Throughput::Elements(num_subscriptions as u64));
        group.bench_with_input(
            BenchmarkId::new("subscriptions_x100", scale),
            &scale,
            |b, &scale| {
                // Setup
                let mut core = create_runtime();
                let data = setup_data(&mut core, scale, USER_ID);

                // Track updates via shared counter
                let update_count = Arc::new(AtomicUsize::new(0));

                // Create 100 subscriptions
                for _ in 0..num_subscriptions {
                    let session = create_session(USER_ID);
                    let query = Query::new("documents");
                    let count_clone = update_count.clone();
                    let _handle = core
                        .subscribe(
                            query,
                            move |_delta| {
                                count_clone.fetch_add(1, Ordering::SeqCst);
                            },
                            Some(session),
                        )
                        .expect("subscribe");
                }
                core.immediate_tick();
                core.batched_tick();
                update_count.store(0, Ordering::SeqCst); // Clear initial

                let session = create_session(USER_ID);
                let write_context = WriteContext::from_session(session.clone());
                let folder_id = data.owned_folders[0];
                let mut doc_counter = 0u64;

                b.iter(|| {
                    doc_counter += 1;
                    let timestamp = current_timestamp();

                    // Insert a document that all subscriptions can see
                    let _id = core
                        .insert(
                            "documents",
                            row([
                                ("folder_id", Value::Uuid(folder_id)),
                                ("title", Value::Text(format!("Fanout Doc {}", doc_counter))),
                                ("content", Value::Text("Fanout test".to_string())),
                                ("author_id", Value::Text(USER_ID.to_string())),
                                ("created_at", Value::Timestamp(timestamp)),
                            ]),
                            Some(&write_context),
                        )
                        .expect("insert");

                    // Process - should update all subscriptions
                    core.immediate_tick();
                    let count = update_count.load(Ordering::SeqCst);

                    // Should have updates for all subscriptions
                    assert!(
                        count >= num_subscriptions,
                        "should notify all {} subscriptions, got {}",
                        num_subscriptions,
                        count
                    );
                    update_count.store(0, Ordering::SeqCst);
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
                let mut core = create_runtime();
                let _data = setup_data(&mut core, scale, USER_ID);

                b.iter(|| {
                    let session = create_session(USER_ID);
                    let got_update = Arc::new(AtomicBool::new(false));
                    let got_update_clone = got_update.clone();

                    // Fresh subscription
                    let query = Query::new("documents");
                    let handle = core
                        .subscribe(
                            query,
                            move |_delta| {
                                got_update_clone.store(true, Ordering::SeqCst);
                            },
                            Some(session),
                        )
                        .expect("subscribe");

                    // Process to receive initial results
                    core.immediate_tick();

                    // Should have initial result set
                    assert!(
                        got_update.load(Ordering::SeqCst),
                        "should receive initial results"
                    );

                    // Clean up subscription
                    core.unsubscribe(handle);
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
                let mut core = create_runtime();
                let data = setup_data(&mut core, scale, USER_ID);
                let session = create_session(USER_ID);
                let write_context = WriteContext::from_session(session.clone());

                let update_count = Arc::new(AtomicUsize::new(0));
                let update_count_clone = update_count.clone();

                // Subscribe only to user's own documents
                let query = QueryBuilder::new("documents")
                    .filter_eq("author_id", Value::Text(USER_ID.to_string()))
                    .build();
                let _handle = core
                    .subscribe(
                        query,
                        move |_delta| {
                            update_count_clone.fetch_add(1, Ordering::SeqCst);
                        },
                        Some(session.clone()),
                    )
                    .expect("subscribe");
                core.immediate_tick();
                core.batched_tick();
                update_count.store(0, Ordering::SeqCst); // Clear initial

                let folder_id = data.owned_folders[0];
                let mut doc_counter = 0u64;

                b.iter(|| {
                    doc_counter += 1;
                    let timestamp = current_timestamp();

                    // Insert a document authored by user (should appear in subscription)
                    let _id = core
                        .insert(
                            "documents",
                            row([
                                ("folder_id", Value::Uuid(folder_id)),
                                (
                                    "title",
                                    Value::Text(format!("Filtered Doc {}", doc_counter)),
                                ),
                                ("content", Value::Text("Filtered test".to_string())),
                                ("author_id", Value::Text(USER_ID.to_string())),
                                ("created_at", Value::Timestamp(timestamp)),
                            ]),
                            Some(&write_context),
                        )
                        .expect("insert");

                    core.immediate_tick();
                    let count = update_count.load(Ordering::SeqCst);

                    assert!(count > 0, "should receive filtered update");
                    update_count.store(0, Ordering::SeqCst);
                });
            },
        );
    }

    group.finish();
}

/// Measure batch insert latency with subscription (exercises delta fast path).
fn batch_insert_subscription_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/batch_insert");

    for scale in [1_000usize, 10_000usize] {
        let batch_size = 100usize;
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("documents_x100", scale),
            &scale,
            |b, &scale| {
                // Setup
                let mut core = create_runtime();
                let data = setup_data(&mut core, scale, USER_ID);
                let session = create_session(USER_ID);
                let write_context = WriteContext::from_session(session.clone());

                let update_count = Arc::new(AtomicUsize::new(0));
                let update_count_clone = update_count.clone();

                // Subscribe to documents
                let query = Query::new("documents");
                let _handle = core
                    .subscribe(
                        query,
                        move |_delta| {
                            update_count_clone.fetch_add(1, Ordering::SeqCst);
                        },
                        Some(session.clone()),
                    )
                    .expect("subscribe");
                core.immediate_tick();
                core.batched_tick();
                update_count.store(0, Ordering::SeqCst); // Clear initial updates

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

                    // Insert batch of documents WITHOUT processing between each
                    for (i, &folder_id) in folder_ids.iter().enumerate() {
                        let _id = core
                            .insert(
                                "documents",
                                row([
                                    ("folder_id", Value::Uuid(folder_id)),
                                    (
                                        "title",
                                        Value::Text(format!("Batch {} Doc {}", batch_counter, i)),
                                    ),
                                    (
                                        "content",
                                        Value::Text("Batch subscription test".to_string()),
                                    ),
                                    ("author_id", Value::Text(USER_ID.to_string())),
                                    ("created_at", Value::Timestamp(timestamp + i as u64)),
                                ]),
                                Some(&write_context),
                            )
                            .expect("insert");
                    }

                    // Single immediate_tick - subscription receives batch update
                    core.immediate_tick();
                    let count = update_count.load(Ordering::SeqCst);

                    assert!(count > 0, "should receive batch update");
                    update_count.store(0, Ordering::SeqCst);
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
    cold_start_latency,
    filtered_subscription_latency,
    batch_insert_subscription_latency
);
criterion_main!(benches);
