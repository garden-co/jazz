//! Memory overhead benchmark for permissioned operations.
//!
//! Measures memory usage as a multiple of plaintext data size.
//!
//! Run with: cargo bench --bench memory_benchmark
//!
//! This benchmark outputs memory statistics rather than timing.

mod common;

use common::{
    MemoryBreakdown, ObjectManagerMemory, QueryManagerMemory, TrackingAllocator,
    create_query_manager, create_session, current_timestamp, document_plaintext_size, format_bytes,
    get_stats, reset_stats, setup_data,
};
use groove::query_manager::types::Value;

// Install tracking allocator globally
#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

const USER_ID: &str = "benchmark_user";

fn main() {
    println!("=== Memory Overhead Benchmark ===\n");

    for scale in [1_000usize] {
        run_memory_benchmark(scale);
        println!();
    }
}

fn run_memory_benchmark(scale: usize) {
    println!("--- Scale: {} documents ---", scale);

    // Reset stats before setup
    reset_stats();

    // Create query manager and populate data
    let mut qm = create_query_manager();
    let data = setup_data(&mut qm, scale, USER_ID);
    let session = create_session(USER_ID);

    let after_setup = get_stats();

    // Calculate plaintext data size
    // Our setup creates documents with titles like "Document N" and content like "Content of document N"
    // Average title: ~12 chars, average content: ~25 chars
    let avg_title = 12;
    let avg_content = 25;
    let doc_data_size =
        scale * document_plaintext_size(&"D".repeat(avg_title), &"C".repeat(avg_content), USER_ID);

    // Also account for teams and folders
    let num_teams = scale / 100;
    let num_folders = scale / 10;
    let team_data_size = num_teams * (8 + 20); // name + owner_id
    let folder_data_size = num_folders * (16 + 10 + 8); // team_id + name + timestamp
    let total_plaintext = doc_data_size + team_data_size + folder_data_size;

    println!("Setup complete:");
    println!(
        "  Teams: {}, Folders: {}, Documents: {}",
        num_teams, num_folders, scale
    );
    println!(
        "  Estimated plaintext data: {}",
        format_bytes(total_plaintext)
    );
    println!("  Memory used: {}", format_bytes(after_setup.current()));
    println!("  Peak memory: {}", format_bytes(after_setup.peak));
    println!(
        "  Overhead multiple: {:.1}x (current), {:.1}x (peak)",
        after_setup.current() as f64 / total_plaintext as f64,
        after_setup.peak as f64 / total_plaintext as f64
    );

    // Print memory breakdown
    let breakdown = compute_memory_breakdown(&qm);
    breakdown.print();

    // Now measure incremental insert overhead
    let folder_id = data.owned_folders[0];
    let insert_title = "Benchmark Insert Title";
    let insert_content = "Benchmark insert content for measuring incremental overhead";
    let insert_plaintext_size = document_plaintext_size(insert_title, insert_content, USER_ID);

    // Insert 100 documents and measure incremental overhead
    let before_inserts = get_stats();
    let num_inserts = 100;

    for i in 0..num_inserts {
        let timestamp = current_timestamp() + i as u64;
        let _handle = qm
            .insert_with_session(
                "documents",
                &[
                    Value::Uuid(folder_id),
                    Value::Text(format!("{} {}", insert_title, i)),
                    Value::Text(insert_content.to_string()),
                    Value::Text(USER_ID.to_string()),
                    Value::Timestamp(timestamp),
                ],
                Some(&session),
            )
            .expect("insert");
        qm.process();
        qm.drain_storage_noop();
    }

    let after_inserts = get_stats();
    let insert_memory = after_inserts.current() - before_inserts.current();
    let insert_plaintext = num_inserts * insert_plaintext_size;

    println!("\nIncremental inserts ({} documents):", num_inserts);
    println!("  Plaintext added: {}", format_bytes(insert_plaintext));
    println!("  Memory added: {}", format_bytes(insert_memory));
    println!(
        "  Incremental overhead: {:.1}x",
        insert_memory as f64 / insert_plaintext as f64
    );

    // Measure subscription memory overhead
    let before_sub = get_stats();

    let query = qm.query("documents").build();
    let _sub_id = qm
        .subscribe_with_session(query, Some(session.clone()))
        .expect("subscribe");
    qm.process();
    qm.drain_storage_noop();
    let _ = qm.take_updates();

    let after_sub = get_stats();
    let sub_memory = after_sub.current() - before_sub.current();

    println!(
        "\nSubscription overhead (all {} docs):",
        scale + num_inserts
    );
    println!("  Memory for subscription: {}", format_bytes(sub_memory));
    println!(
        "  Per-document subscription cost: {} bytes",
        sub_memory / (scale + num_inserts)
    );

    // Final summary
    println!("\nFinal state:");
    let final_stats = get_stats();
    let final_plaintext = total_plaintext + insert_plaintext;
    println!("  Total plaintext: {}", format_bytes(final_plaintext));
    println!("  Total memory: {}", format_bytes(final_stats.current()));
    println!("  Peak memory: {}", format_bytes(final_stats.peak));
    println!(
        "  Overall multiple: {:.1}x (current), {:.1}x (peak)",
        final_stats.current() as f64 / final_plaintext as f64,
        final_stats.peak as f64 / final_plaintext as f64
    );

    // Final breakdown
    println!("\nFinal memory breakdown:");
    let final_breakdown = compute_memory_breakdown(&qm);
    final_breakdown.print();
}

/// Compute memory breakdown from QueryManager.
fn compute_memory_breakdown(qm: &groove::query_manager::QueryManager) -> MemoryBreakdown {
    // Get ObjectManager memory breakdown via SyncManager
    let (row_objects, index_objects, blobs, subscriptions, outbox_inbox, om_total) =
        qm.sync_manager().object_manager.memory_size();

    let object_manager = ObjectManagerMemory {
        row_objects,
        index_objects,
        blobs,
        subscriptions,
        outbox_inbox,
        total: om_total,
    };

    // Get QueryManager memory breakdown
    let (indices, qm_subscriptions, policy_checks, qm_total) = qm.memory_size();

    let query_manager = QueryManagerMemory {
        indices,
        subscriptions: qm_subscriptions,
        policy_checks,
        total: qm_total,
    };

    let total = om_total + qm_total;

    MemoryBreakdown {
        object_manager,
        query_manager,
        total,
    }
}
