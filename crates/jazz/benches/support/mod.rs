#![allow(dead_code)]

use std::collections::BTreeMap;
use std::process::Command;

use jazz::groove::db::{
    CommitMetrics, StorageReadBucket, StorageReadMetrics, StorageWriteBucket, StorageWriteMetrics,
};
use jazz::groove::ivm::{RuntimeStats, TickMetrics};
use jazz::groove::storage::OrderedKvStorage;
use jazz::node::{LargeValueMetrics, NodeState, QueryEngineReadMetrics, SyncMetrics};
use jazz::tx::DurabilityTier;
use serde_json::{Map, Value, json};

pub fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

pub fn csv_usizes(name: &str, default: &str) -> Vec<usize> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|value| {
            value
                .trim()
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("invalid {name} value: {value}"))
        })
        .collect()
}

pub fn emit_json_line(scenario: &str, mut fields: Map<String, Value>) {
    fields.insert("scenario".to_owned(), json!(scenario));
    insert_process_metadata(&mut fields);
    let line = serde_json::to_string(&Value::Object(fields)).expect("json line");
    println!("{line}");
}

pub fn phase_fields(phase: &str, wall_us: u128) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("phase".to_owned(), json!(phase));
    fields.insert("wall_us".to_owned(), json!(wall_us));
    fields
}

pub fn insert_node_metrics<S>(fields: &mut Map<String, Value>, prefix: &str, node: &NodeState<S>)
where
    S: OrderedKvStorage,
{
    insert_storage_read_metrics(
        fields,
        &format!("{prefix}_storage_read"),
        &node.storage_read_metrics(),
    );
    if let Some(metrics) = node.last_commit_metrics() {
        insert_commit_metrics(fields, &format!("{prefix}_last_commit"), metrics);
    }
    if let Some(metrics) = node.last_tick_metrics() {
        insert_tick_metrics(fields, &format!("{prefix}_last_tick"), metrics);
    }
    insert_sync_metrics(fields, &format!("{prefix}_sync"), node.sync_metrics());
    insert_query_engine_read_metrics(
        fields,
        &format!("{prefix}_query_engine_read"),
        node.query_engine_read_metrics(),
    );
    insert_large_value_metrics(
        fields,
        &format!("{prefix}_large_value"),
        node.large_value_metrics(),
    );
}

pub fn reset_phase_counters<S>(nodes: &mut [&mut NodeState<S>])
where
    S: OrderedKvStorage,
{
    for node in nodes {
        node.reset_storage_read_metrics();
        node.reset_query_engine_read_metrics();
        node.reset_large_value_metrics();
    }
}

pub fn insert_durability_tier(fields: &mut Map<String, Value>, tier: DurabilityTier) {
    fields.insert(
        "durability_tier".to_owned(),
        json!(match tier {
            DurabilityTier::None => "None",
            DurabilityTier::Local => "Local",
            DurabilityTier::Edge => "Edge",
            DurabilityTier::Global => "Global",
        }),
    );
}

fn insert_storage_read_metrics(
    fields: &mut Map<String, Value>,
    prefix: &str,
    metrics: &StorageReadMetrics,
) {
    insert_read_bucket(fields, prefix, "total", metrics.total);
    insert_read_bucket(fields, prefix, "history_rows", metrics.history_rows);
    insert_read_bucket(fields, prefix, "history_indexes", metrics.history_indexes);
    insert_read_bucket(
        fields,
        prefix,
        "global_current_rows",
        metrics.global_current_rows,
    );
    insert_read_bucket(
        fields,
        prefix,
        "global_current_indexes",
        metrics.global_current_indexes,
    );
    insert_read_bucket(
        fields,
        prefix,
        "register_global_current_rows",
        metrics.register_global_current_rows,
    );
    insert_read_bucket(
        fields,
        prefix,
        "global_changes_rows",
        metrics.global_changes_rows,
    );
    insert_read_bucket(
        fields,
        prefix,
        "global_changes_indexes",
        metrics.global_changes_indexes,
    );
    insert_read_bucket(
        fields,
        prefix,
        "transactions_rows",
        metrics.transactions_rows,
    );
    insert_read_bucket(
        fields,
        prefix,
        "transactions_indexes",
        metrics.transactions_indexes,
    );
    insert_read_bucket(fields, prefix, "other", metrics.other);
}

fn insert_read_bucket(
    fields: &mut Map<String, Value>,
    prefix: &str,
    name: &str,
    bucket: StorageReadBucket,
) {
    fields.insert(format!("{prefix}_{name}_reads"), json!(bucket.reads));
    fields.insert(format!("{prefix}_{name}_ranges"), json!(bucket.ranges));
}

fn insert_commit_metrics(fields: &mut Map<String, Value>, prefix: &str, metrics: &CommitMetrics) {
    fields.insert(
        format!("{prefix}_storage_write_us"),
        json!(metrics.storage_write_time.as_micros()),
    );
    fields.insert(
        format!("{prefix}_ivm_tick_us"),
        json!(metrics.ivm_tick_time.as_micros()),
    );
    fields.insert(
        format!("{prefix}_storage_write_count"),
        json!(metrics.storage_write_count),
    );
    fields.insert(
        format!("{prefix}_storage_write_bytes"),
        json!(metrics.storage_write_bytes),
    );
    insert_storage_write_metrics(
        fields,
        &format!("{prefix}_storage_write"),
        &metrics.storage_writes,
    );
    insert_tick_metrics(fields, &format!("{prefix}_tick"), &metrics.tick);
}

fn insert_storage_write_metrics(
    fields: &mut Map<String, Value>,
    prefix: &str,
    metrics: &StorageWriteMetrics,
) {
    insert_write_bucket(fields, prefix, "total", metrics.total);
    insert_write_bucket(fields, prefix, "history_rows", metrics.history_rows);
    insert_write_bucket(fields, prefix, "history_indexes", metrics.history_indexes);
    insert_write_bucket(
        fields,
        prefix,
        "global_current_rows",
        metrics.global_current_rows,
    );
    insert_write_bucket(
        fields,
        prefix,
        "global_current_indexes",
        metrics.global_current_indexes,
    );
    insert_write_bucket(
        fields,
        prefix,
        "register_global_current_rows",
        metrics.register_global_current_rows,
    );
    insert_write_bucket(
        fields,
        prefix,
        "global_changes_rows",
        metrics.global_changes_rows,
    );
    insert_write_bucket(
        fields,
        prefix,
        "global_changes_indexes",
        metrics.global_changes_indexes,
    );
    insert_write_bucket(
        fields,
        prefix,
        "transactions_rows",
        metrics.transactions_rows,
    );
    insert_write_bucket(
        fields,
        prefix,
        "transactions_indexes",
        metrics.transactions_indexes,
    );
    insert_write_bucket(fields, prefix, "other", metrics.other);
}

fn insert_write_bucket(
    fields: &mut Map<String, Value>,
    prefix: &str,
    name: &str,
    bucket: StorageWriteBucket,
) {
    fields.insert(format!("{prefix}_{name}_count"), json!(bucket.count));
    fields.insert(format!("{prefix}_{name}_bytes"), json!(bucket.bytes));
}

fn insert_tick_metrics(fields: &mut Map<String, Value>, prefix: &str, metrics: &TickMetrics) {
    fields.insert(format!("{prefix}_tick"), json!(metrics.tick));
    fields.insert(
        format!("{prefix}_table_delta_records"),
        json!(metrics.table_delta_records),
    );
    fields.insert(
        format!("{prefix}_records_processed"),
        json!(metrics.records_processed),
    );
    fields.insert(
        format!("{prefix}_recursive_recomputes"),
        json!(metrics.recursive_recomputes),
    );
    fields.insert(
        format!("{prefix}_notifications_sent"),
        json!(metrics.notifications_sent),
    );
    fields.insert(
        format!("{prefix}_notification_records"),
        json!(metrics.notification_records),
    );
    fields.insert(
        format!("{prefix}_notification_encoded_bytes"),
        json!(metrics.notification_encoded_bytes),
    );
    insert_runtime_stats(fields, &format!("{prefix}_runtime"), &metrics.runtime_stats);
}

fn insert_runtime_stats(fields: &mut Map<String, Value>, prefix: &str, stats: &RuntimeStats) {
    fields.insert(format!("{prefix}_graph_nodes"), json!(stats.graph_nodes));
    fields.insert(
        format!("{prefix}_active_subscriptions"),
        json!(stats.active_subscriptions),
    );
    fields.insert(
        format!("{prefix}_active_prepared_shapes"),
        json!(stats.active_prepared_shapes),
    );
    fields.insert(
        format!("{prefix}_active_shape_params"),
        json!(stats.active_shape_params),
    );
    fields.insert(
        format!("{prefix}_arrangement_count"),
        json!(stats.arrangement_count),
    );
    fields.insert(
        format!("{prefix}_logical_nodes_requested"),
        json!(stats.logical_nodes_requested),
    );
    fields.insert(
        format!("{prefix}_deduped_graph_nodes"),
        json!(stats.deduped_graph_nodes),
    );
}

fn insert_sync_metrics(fields: &mut Map<String, Value>, prefix: &str, metrics: &SyncMetrics) {
    fields.insert(
        format!("{prefix}_parked_orphans"),
        json!(metrics.parked_orphans),
    );
    fields.insert(
        format!("{prefix}_parked_orphans_resolved"),
        json!(metrics.parked_orphans_resolved),
    );
    fields.insert(
        format!("{prefix}_parked_catalogue_orphans"),
        json!(metrics.parked_catalogue_orphans),
    );
    fields.insert(
        format!("{prefix}_parked_catalogue_orphans_resolved"),
        json!(metrics.parked_catalogue_orphans_resolved),
    );
    fields.insert(
        format!("{prefix}_parked_catalogue_shapes"),
        json!(metrics.parked_catalogue_shapes),
    );
    fields.insert(
        format!("{prefix}_parked_catalogue_shapes_resolved"),
        json!(metrics.parked_catalogue_shapes_resolved),
    );
}

fn insert_query_engine_read_metrics(
    fields: &mut Map<String, Value>,
    prefix: &str,
    metrics: &QueryEngineReadMetrics,
) {
    fields.insert(
        format!("{prefix}_policy_authorization_graphs"),
        json!(metrics.policy_authorization_graphs),
    );
    fields.insert(
        format!("{prefix}_policy_authorized_source_joins"),
        json!(metrics.policy_authorized_source_joins),
    );
}

fn insert_large_value_metrics(
    fields: &mut Map<String, Value>,
    prefix: &str,
    metrics: &LargeValueMetrics,
) {
    fields.insert(
        format!("{prefix}_materializations"),
        json!(metrics.materializations),
    );
    fields.insert(
        format!("{prefix}_total_replayed_ops"),
        json!(metrics.total_replayed_ops),
    );
    fields.insert(
        format!("{prefix}_last_replayed_ops"),
        json!(metrics.last_replayed_ops),
    );
    fields.insert(
        format!("{prefix}_last_replayed_versions"),
        json!(metrics.last_replayed_versions),
    );
    fields.insert(
        format!("{prefix}_checkpoint_hits"),
        json!(metrics.checkpoint_hits),
    );
    fields.insert(
        format!("{prefix}_checkpoint_writes"),
        json!(metrics.checkpoint_writes),
    );
}

fn insert_process_metadata(fields: &mut Map<String, Value>) {
    fields.insert(
        "git_sha".to_owned(),
        json!(git_output(["rev-parse", "HEAD"])),
    );
    fields.insert("git_dirty".to_owned(), json!(git_dirty()));
    fields.insert("hostname".to_owned(), json!(hostname()));
    fields.insert("knobs".to_owned(), json!(knob_env()));
}

fn git_dirty() -> bool {
    !git_output(["status", "--porcelain"]).is_empty()
}

fn git_output<const N: usize>(args: [&str; N]) -> String {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_owned())
        .unwrap_or_default()
}

fn hostname() -> String {
    Command::new("hostname")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_owned())
        .unwrap_or_default()
}

fn knob_env() -> BTreeMap<String, String> {
    std::env::vars()
        .filter(|(key, _)| key.starts_with("JAZZ_") || key.starts_with("GROOVE_"))
        .collect()
}
