use mini_jazz_sqlite::sync::{decode_bundle, encode_bundle, merge_bundles, Bundle};
use mini_jazz_sqlite::{
    persisted_text_ops, reset_runtime_write_phase_stats, take_runtime_write_phase_stats,
    ApplyBundleProfile, HistoryBlockManifest, HistoryCompactionPolicy, QueryExportProfile, Result,
    RowDiff, RowsSubscription, Runtime, RuntimeWritePhaseStats, SchemaDef, Storage, Value,
};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::tempdir;

const OWNER: &str = "alice";
type BenchResult<T> = std::result::Result<T, Box<dyn Error>>;

fn main() -> BenchResult<()> {
    let config = Config::from_env();
    if let Ok(kind) = env::var("MINI_JAZZ_PERF_ONLY_DEEP_HISTORY") {
        if let Some(report) = run_deep_history_group_probe(&kind)? {
            println!("{}", serde_json::to_string_pretty(&report)?);
            return Ok(());
        }
        let report = run_deep_history_probe(&kind)?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    if let Some(repeat) = env_optional_usize("MINI_JAZZ_PERF_REPEAT_PRIMARY") {
        let report = run_primary_repeat(&config, repeat.max(1))?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    if let Some(repeat) = env_optional_usize("MINI_JAZZ_PERF_REPEAT_DASHBOARD_SCALING") {
        let report = run_dashboard_query_scaling_repeat(repeat.max(1))?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    if env_bool("MINI_JAZZ_PERF_ONLY_RECURSIVE_TREE", false) {
        let report = RecursiveTreeOnlyReport {
            recursive_tree_subscription_probe: run_recursive_tree_subscription_probe()?,
            recursive_tree_topology_probe: run_recursive_tree_topology_probe()?,
            process_rss_end_bytes: process_rss_bytes(),
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    let process_rss_start_bytes = process_rss_bytes();
    let report = BenchmarkReport {
        process_rss_start_bytes,
        primary: run_core_only_scoped_page(&config)?,
        tx_granularity_probe: run_tx_granularity_probe()?,
        recursive_policy_probe: run_recursive_policy_probe()?,
        multi_tab_fanout_probe: run_multi_tab_fanout_probe()?,
        many_user_page_probe: run_many_user_page_probe()?,
        user_id_footprint_probe: run_user_id_footprint_probe()?,
        user_id_interning_projection_probe: run_user_id_interning_projection_probe()?,
        permissioned_dashboard_probe: run_permissioned_dashboard_probe()?,
        dashboard_query_scaling_probe: run_dashboard_query_scaling_probe()?,
        recursive_tree_subscription_probe: run_recursive_tree_subscription_probe()?,
        recursive_tree_topology_probe: run_recursive_tree_topology_probe()?,
        recursive_closure_layout_probe: run_recursive_closure_layout_probe()?,
        cold_reopen_profile_probe: run_cold_reopen_profile_probe()?,
        project_board_probe: run_project_board_probe()?,
        current_projection_tradeoff_probe: run_current_projection_tradeoff_probe()?,
        mixed_mutation_refresh_probe: run_mixed_mutation_refresh_probe()?,
        wide_schema_apply_probe: run_wide_schema_apply_probe()?,
        storage_topology_probe: run_storage_topology_probe()?,
        multi_query_refresh_probe: run_multi_query_refresh_probe()?,
        subscription_storm_probe: run_subscription_storm_probe()?,
        apply_profile_probe: run_apply_profile_probe()?,
        branch_overlay_probe: run_branch_overlay_probe()?,
        pinned_branch_snapshot_probe: run_pinned_branch_snapshot_probe()?,
        branch_fan_in_probe: run_branch_fan_in_probe()?,
        export_profile_probe: run_export_profile_probe()?,
        process_rss_end_bytes: process_rss_bytes(),
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[derive(Serialize)]
struct PrimaryRepeatReport {
    repeat: usize,
    samples: Vec<PrimaryRepeatSample>,
    median: PrimaryRepeatSample,
}

#[derive(Clone, Serialize)]
struct PrimaryRepeatSample {
    seed_ms: f64,
    api_to_first_result_ms: f64,
    refresh_ms: f64,
    core_database_bytes: i64,
    core_total_file_bytes: i64,
    bundle_bytes: usize,
}

fn run_primary_repeat(config: &Config, repeat: usize) -> BenchResult<PrimaryRepeatReport> {
    let mut samples = Vec::new();
    for _ in 0..repeat {
        let report = run_core_only_scoped_page(config)?;
        samples.push(PrimaryRepeatSample {
            seed_ms: report.seed_ms,
            api_to_first_result_ms: report.api_to_first_result_ms,
            refresh_ms: report.refresh_after_new_top_rows.api_to_updated_result_ms,
            core_database_bytes: report.core_database_bytes,
            core_total_file_bytes: report.core_total_file_bytes,
            bundle_bytes: report.bundle_bytes,
        });
    }
    let median = PrimaryRepeatSample {
        seed_ms: median_f64(samples.iter().map(|sample| sample.seed_ms).collect()),
        api_to_first_result_ms: median_f64(
            samples
                .iter()
                .map(|sample| sample.api_to_first_result_ms)
                .collect(),
        ),
        refresh_ms: median_f64(samples.iter().map(|sample| sample.refresh_ms).collect()),
        core_database_bytes: median_i64(
            samples
                .iter()
                .map(|sample| sample.core_database_bytes)
                .collect(),
        ),
        core_total_file_bytes: median_i64(
            samples
                .iter()
                .map(|sample| sample.core_total_file_bytes)
                .collect(),
        ),
        bundle_bytes: median_usize(samples.iter().map(|sample| sample.bundle_bytes).collect()),
    };
    Ok(PrimaryRepeatReport {
        repeat,
        samples,
        median,
    })
}

#[derive(Debug)]
struct Config {
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    seed_batch_size: usize,
    refresh_new_top_rows: usize,
    durable_intermediaries: bool,
}

impl Config {
    fn from_env() -> Self {
        Self {
            total_rows: env_usize("MINI_JAZZ_PERF_TOTAL_ROWS", 100_000),
            target_owner_rows: env_usize("MINI_JAZZ_PERF_TARGET_OWNER_ROWS", 10_000),
            page_size: env_usize("MINI_JAZZ_PERF_PAGE_SIZE", 50),
            seed_batch_size: env_usize("MINI_JAZZ_PERF_SEED_BATCH_SIZE", 100),
            refresh_new_top_rows: env_usize("MINI_JAZZ_PERF_REFRESH_NEW_TOP_ROWS", 50),
            durable_intermediaries: env_bool("MINI_JAZZ_PERF_DURABLE_INTERMEDIARIES", true),
        }
    }
}

#[derive(Serialize)]
struct BenchmarkReport {
    process_rss_start_bytes: Option<i64>,
    primary: ScenarioReport,
    tx_granularity_probe: TxGranularityProbe,
    recursive_policy_probe: RecursivePolicyProbe,
    multi_tab_fanout_probe: MultiTabFanoutProbe,
    many_user_page_probe: ManyUserPageProbe,
    user_id_footprint_probe: UserIdFootprintProbe,
    user_id_interning_projection_probe: UserIdInterningProjectionProbe,
    permissioned_dashboard_probe: PermissionedDashboardProbe,
    dashboard_query_scaling_probe: DashboardQueryScalingProbe,
    recursive_tree_subscription_probe: RecursiveTreeSubscriptionProbe,
    recursive_tree_topology_probe: RecursiveTreeTopologyProbe,
    recursive_closure_layout_probe: RecursiveClosureLayoutProbe,
    cold_reopen_profile_probe: ColdReopenProfileProbe,
    project_board_probe: ProjectBoardProbe,
    current_projection_tradeoff_probe: CurrentProjectionTradeoffProbe,
    mixed_mutation_refresh_probe: MixedMutationRefreshProbe,
    wide_schema_apply_probe: WideSchemaApplyProbe,
    storage_topology_probe: StorageTopologyProbe,
    multi_query_refresh_probe: MultiQueryRefreshProbe,
    subscription_storm_probe: SubscriptionStormProbe,
    apply_profile_probe: ApplyProfileProbe,
    branch_overlay_probe: BranchOverlayProbe,
    pinned_branch_snapshot_probe: PinnedBranchSnapshotProbe,
    branch_fan_in_probe: BranchFanInProbe,
    export_profile_probe: ExportProfileProbe,
    process_rss_end_bytes: Option<i64>,
}

#[derive(Serialize)]
#[serde(tag = "scenario")]
enum DeepHistoryReport {
    AppendStream(DeepHistoryCaseReport),
    AutomergePaper(DeepHistoryCaseReport),
    CanvasPositions(DeepHistoryCaseReport),
    AppendStreamHistoryBlocks(DeepHistoryCaseReport),
    AutomergePaperHistoryBlocks(DeepHistoryCaseReport),
    CanvasPositionsHistoryBlocks(DeepHistoryCaseReport),
    AppendStreamBlockOps(DeepHistoryCaseReport),
    AutomergePaperBlockOps(DeepHistoryCaseReport),
}

#[derive(Serialize)]
struct DeepHistoryCaseReport {
    target_updates: usize,
    completed_updates: usize,
    stopped_early: bool,
    stop_reason: Option<String>,
    elapsed_write_ms: f64,
    average_write_ms: f64,
    total_loop_ms: f64,
    write_only_ms: f64,
    average_write_only_ms: f64,
    sampled_receive_total_ms: f64,
    live_receive_sample_count: usize,
    live_receive_average_ms: Option<f64>,
    live_receive_p50_ms: Option<f64>,
    live_receive_p95_ms: Option<f64>,
    live_receive_p99_ms: Option<f64>,
    live_receive_last_ms: Option<f64>,
    cold_load_ms: f64,
    current_read_ms: f64,
    historical_read_total_ms: f64,
    historical_read_count: usize,
    historical_read_average_ms: Option<f64>,
    transaction_info_total_ms: f64,
    transaction_info_count: usize,
    transaction_info_average_ms: Option<f64>,
    final_payload_bytes: usize,
    extrapolated_final_payload_bytes_for_target: Option<usize>,
    reference_gzip_bytes: Option<usize>,
    bundle_bytes: usize,
    native_sync_bytes: usize,
    native_export_ms: f64,
    native_import_ms: f64,
    block_native_export_ms: Option<f64>,
    block_native_import_ms: Option<f64>,
    block_native_blocks: Option<usize>,
    block_native_payload_bytes: Option<usize>,
    database_bytes: i64,
    live_database_bytes: i64,
    freelist_bytes: i64,
    total_file_bytes: i64,
    history_rows: i64,
    current_rows: i64,
    database_to_final_payload_ratio: Option<f64>,
    live_database_to_final_payload_ratio: Option<f64>,
    total_file_to_final_payload_ratio: Option<f64>,
    database_to_extrapolated_final_payload_ratio: Option<f64>,
    live_database_to_extrapolated_final_payload_ratio: Option<f64>,
    total_file_to_extrapolated_final_payload_ratio: Option<f64>,
    database_to_reference_gzip_ratio: Option<f64>,
    live_database_to_reference_gzip_ratio: Option<f64>,
    bundle_to_reference_gzip_ratio: Option<f64>,
    extrapolated_write_ms_for_target: Option<f64>,
    extrapolated_database_bytes_for_target: Option<i64>,
    phase_ms: DeepHistoryPhaseReport,
    notes: Vec<String>,
}

#[derive(Default, Serialize)]
struct DeepHistoryPhaseReport {
    sidecar_edit: f64,
    sidecar_commit: f64,
    jazz_write: f64,
    live_export: f64,
    live_encode_decode: f64,
    live_apply: f64,
    live_sidecar_apply: f64,
    live_listener_and_verify: f64,
    compaction: f64,
    reclaim: f64,
    native_export: f64,
    native_import_apply: f64,
    final_export: f64,
    final_encode_decode: f64,
    cold_apply: f64,
    cold_sidecar_apply: f64,
    cold_read_and_verify: f64,
    current_read: f64,
    historical_reads: f64,
    transaction_info_reads: f64,
    live_apply_profile: ApplyBundleProfile,
    cold_apply_profile: ApplyBundleProfile,
    jazz_write_detail: RuntimeWritePhaseStats,
    jazz_write_sqlite_transactions: usize,
    jazz_write_updates_per_transaction_avg: Option<f64>,
    jazz_write_updates_per_transaction_max: usize,
}

impl DeepHistoryPhaseReport {
    fn add_elapsed(slot: &mut f64, started: Instant) {
        *slot += ms(started.elapsed());
    }
}

fn add_write_detail(total: &mut RuntimeWritePhaseStats, sample: RuntimeWritePhaseStats) {
    total.update_prelookup_ms += sample.update_prelookup_ms;
    total.begin_transaction_ms += sample.begin_transaction_ms;
    total.next_epoch_ms += sample.next_epoch_ms;
    total.tx_create_ms += sample.tx_create_ms;
    total.upsert_probe_ms += sample.upsert_probe_ms;
    total.validate_fields_ms += sample.validate_fields_ms;
    total.row_lookup_ms += sample.row_lookup_ms;
    total.effective_values_ms += sample.effective_values_ms;
    total.tx_tuple_ms += sample.tx_tuple_ms;
    total.policy_ms += sample.policy_ms;
    total.sql_value_encode_ms += sample.sql_value_encode_ms;
    total.creation_metadata_ms += sample.creation_metadata_ms;
    total.history_insert_ms += sample.history_insert_ms;
    total.record_tx_write_ms += sample.record_tx_write_ms;
    total.current_upsert_ms += sample.current_upsert_ms;
    total.reject_cleanup_ms += sample.reject_cleanup_ms;
    total.commit_ms += sample.commit_ms;
}

fn empty_apply_profile() -> ApplyBundleProfile {
    ApplyBundleProfile::default()
}

fn add_apply_profile(total: &mut ApplyBundleProfile, sample: ApplyBundleProfile) {
    total.total_ms += sample.total_ms;
    total.validation_ms += sample.validation_ms;
    total.begin_tx_ms += sample.begin_tx_ms;
    total.branches_ms += sample.branches_ms;
    total.txs_ms += sample.txs_ms;
    total.reads_ms += sample.reads_ms;
    total.rejected_cleanup_ms += sample.rejected_cleanup_ms;
    total.query_reads_ms += sample.query_reads_ms;
    total.history_ms += sample.history_ms;
    total.query_scope_repair_ms += sample.query_scope_repair_ms;
    total.commit_ms += sample.commit_ms;
    total.revalidate_awaiting_ms += sample.revalidate_awaiting_ms;
    total.branch_rows += sample.branch_rows;
    total.tx_rows += sample.tx_rows;
    total.read_rows += sample.read_rows;
    total.query_read_rows += sample.query_read_rows;
    total.history_rows += sample.history_rows;
}

#[derive(Serialize)]
struct ScenarioReport {
    scenario_id: &'static str,
    profile_id: String,
    topology: &'static str,
    cache_mode: &'static str,
    seed_rows_by_table: BTreeMap<&'static str, usize>,
    seed_batch_size: usize,
    visible_rows_returned: usize,
    history_rows_synced: usize,
    transaction_rows_synced: usize,
    observed_facts_synced: usize,
    bundle_bytes: usize,
    approx_raw_json_payload_bytes: usize,
    core_database_to_raw_payload_ratio: f64,
    core_database_bytes: i64,
    core_total_file_bytes: i64,
    core_table_page_bytes: BTreeMap<String, i64>,
    edge_database_bytes: i64,
    edge_total_file_bytes: i64,
    edge_table_page_bytes: BTreeMap<String, i64>,
    worker_database_bytes: i64,
    worker_total_file_bytes: i64,
    worker_table_page_bytes: BTreeMap<String, i64>,
    tab_database_bytes: i64,
    tab_total_file_bytes: i64,
    tab_table_page_bytes: BTreeMap<String, i64>,
    seed_ms: f64,
    core_query_ms: f64,
    export_ms: f64,
    core_to_edge_apply_ms: f64,
    edge_export_ms: f64,
    edge_to_worker_apply_ms: f64,
    worker_export_ms: f64,
    worker_to_tab_apply_ms: f64,
    tab_query_ms: f64,
    api_to_first_result_ms: f64,
    edge_warm_worker_cold: WarmBootReport,
    worker_warm_tab_cold: WarmBootReport,
    refresh_after_new_top_rows: RefreshReport,
}

#[derive(Serialize)]
struct TxGranularityProbe {
    batched_100: TxGranularityCase,
    one_write_per_row: TxGranularityCase,
}

#[derive(Serialize)]
struct TxGranularityCase {
    total_rows: usize,
    target_owner_rows: usize,
    seed_batch_size: usize,
    seed_ms: f64,
    export_ms: f64,
    bundle_bytes: usize,
    history_rows_synced: usize,
    transaction_rows_synced: usize,
    core_database_bytes: i64,
}

#[derive(Serialize)]
struct RecursivePolicyProbe {
    total_rows: usize,
    target_owner_rows: usize,
    policy_depth: usize,
    visible_rows_returned: usize,
    history_rows_synced: usize,
    transaction_rows_synced: usize,
    bundle_bytes: usize,
    core_database_bytes: i64,
    seed_ms: f64,
    core_query_ms: f64,
    export_ms: f64,
}

#[derive(Serialize)]
struct MultiTabFanoutProbe {
    total_rows: usize,
    target_owner_rows: usize,
    tab_count: usize,
    worker_boot_ms: f64,
    worker_export_ms: f64,
    bundle_bytes: usize,
    history_rows_synced: usize,
    transaction_rows_synced: usize,
    total_tab_apply_ms: f64,
    average_tab_apply_ms: f64,
    total_tab_query_ms: f64,
    average_tab_query_ms: f64,
}

#[derive(Serialize)]
struct ManyUserPageProbe {
    user_count: usize,
    total_rows: usize,
    rows_per_user: usize,
    sampled_users: usize,
    page_size: usize,
    seed_ms: f64,
    total_export_ms: f64,
    average_export_ms: f64,
    total_bundle_bytes: usize,
    average_bundle_bytes: f64,
    total_history_rows_synced: usize,
    average_history_rows_synced: f64,
    total_transaction_rows_synced: usize,
    average_transaction_rows_synced: f64,
    core_database_bytes: i64,
}

#[derive(Serialize)]
struct UserIdFootprintProbe {
    short_user_ids: UserIdFootprintCase,
    long_user_ids: UserIdFootprintCase,
    additional_bytes_per_row_for_long_ids: f64,
}

#[derive(Serialize)]
struct UserIdFootprintCase {
    user_count: usize,
    rows_per_user: usize,
    representative_user_id_bytes: usize,
    seed_ms: f64,
    core_database_bytes: i64,
    current_page_bytes: i64,
    history_page_bytes: i64,
    tx_page_bytes: i64,
}

#[derive(Serialize)]
struct UserIdInterningProjectionProbe {
    text_system_users: UserIdInterningProjectionCase,
    interned_system_users: UserIdInterningProjectionCase,
    saved_bytes_per_row: f64,
}

#[derive(Serialize)]
struct UserIdInterningProjectionCase {
    user_count: usize,
    rows_per_user: usize,
    representative_user_id_bytes: usize,
    database_bytes: i64,
    seed_ms: f64,
    materialize_page_ms: f64,
}

#[derive(Serialize)]
struct PermissionedDashboardProbe {
    total_rows: usize,
    target_owner_rows: usize,
    query_count: usize,
    page_size: usize,
    seed_ms: f64,
    core_export_ms: f64,
    merged_bundle_bytes: usize,
    merged_history_rows: usize,
    merged_transaction_rows: usize,
    edge_apply_ms: f64,
    worker_apply_ms: f64,
    tab_apply_ms: f64,
    subscribe_ms: f64,
    refresh_core_export_ms: f64,
    refresh_edge_apply_ms: f64,
    refresh_edge_export_ms: f64,
    refresh_worker_apply_ms: f64,
    refresh_worker_export_ms: f64,
    refresh_tab_apply_ms: f64,
    refresh_bundle_bytes: usize,
    refresh_history_rows: usize,
    refresh_transaction_rows: usize,
    subscription_poll_ms: f64,
    subscription_added: usize,
    subscription_updated: usize,
    subscription_removed: usize,
    core_database_bytes: i64,
    tab_database_bytes: i64,
}

#[derive(Serialize)]
struct DashboardQueryScalingProbe {
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    cases: Vec<DashboardQueryScalingCase>,
}

#[derive(Clone, Serialize)]
struct DashboardQueryScalingCase {
    query_count: usize,
    initial_export_ms: f64,
    initial_bundle_bytes: usize,
    initial_history_rows: usize,
    initial_transaction_rows: usize,
    tab_apply_ms: f64,
    tab_apply_profile: ApplyBundleProfile,
    refresh_export_ms: f64,
    refresh_bundle_count: usize,
    refresh_bundle_bytes: usize,
    refresh_history_rows: usize,
    refresh_apply_ms: f64,
    refresh_apply_profile: ApplyBundleProfile,
}

#[derive(Serialize)]
struct DashboardQueryScalingRepeatReport {
    repeat: usize,
    samples: Vec<DashboardQueryScalingProbe>,
    median: DashboardQueryScalingProbe,
}

#[derive(Serialize)]
struct RecursiveTreeOnlyReport {
    recursive_tree_subscription_probe: RecursiveTreeSubscriptionProbe,
    recursive_tree_topology_probe: RecursiveTreeTopologyProbe,
    process_rss_end_bytes: Option<i64>,
}

#[derive(Serialize)]
struct RecursiveTreeSubscriptionProbe {
    node_count: usize,
    branch_factor: usize,
    root_id: String,
    rss_start_bytes: Option<i64>,
    rss_after_seed_bytes: Option<i64>,
    rss_after_initial_apply_bytes: Option<i64>,
    rss_after_refresh_bytes: Option<i64>,
    rss_after_noop_refresh_bytes: Option<i64>,
    seed_ms: f64,
    initial_read_ms: f64,
    initial_admin_read_ms: f64,
    initial_rows_read: usize,
    initial_export_ms: f64,
    initial_bundle_bytes: usize,
    initial_history_rows: usize,
    initial_apply_ms: f64,
    initial_apply_profile: ApplyBundleProfile,
    subscribe_ms: f64,
    refresh_read_ms: f64,
    refresh_rows_read: usize,
    refresh_export_ms: f64,
    refresh_bundle_bytes: usize,
    refresh_history_rows: usize,
    refresh_apply_ms: f64,
    refresh_apply_profile: ApplyBundleProfile,
    subscription_poll_ms: f64,
    noop_refresh_export_ms: f64,
    noop_refresh_history_rows: usize,
    noop_refresh_apply_ms: f64,
    noop_refresh_apply_profile: ApplyBundleProfile,
    noop_subscription_poll_ms: f64,
    noop_subscription_added: usize,
    noop_subscription_updated: usize,
    noop_subscription_removed: usize,
    repeated_noop_refresh_count: usize,
    repeated_noop_total_export_ms: f64,
    repeated_noop_total_apply_ms: f64,
    repeated_noop_total_poll_ms: f64,
    repeated_noop_total_history_rows: usize,
    repeated_noop_total_diffs: usize,
    rss_after_repeated_noop_refreshes_bytes: Option<i64>,
    subscription_added: usize,
    subscription_updated: usize,
    subscription_removed: usize,
    visible_rows_after_refresh: usize,
    core_database_bytes: i64,
    tab_database_bytes: i64,
}

#[derive(Serialize)]
struct RecursiveTreeTopologyProbe {
    node_count: usize,
    branch_factor: usize,
    root_id: String,
    rss_start_bytes: Option<i64>,
    rss_after_seed_bytes: Option<i64>,
    rss_after_initial_flow_bytes: Option<i64>,
    rss_after_refresh_flow_bytes: Option<i64>,
    initial_core_export_ms: f64,
    initial_edge_apply_ms: f64,
    initial_edge_export_ms: f64,
    initial_worker_apply_ms: f64,
    initial_worker_export_ms: f64,
    initial_tab_apply_ms: f64,
    refresh_core_export_ms: f64,
    refresh_edge_apply_ms: f64,
    refresh_edge_export_ms: f64,
    refresh_worker_apply_ms: f64,
    refresh_worker_export_ms: f64,
    refresh_tab_apply_ms: f64,
    subscription_poll_ms: f64,
    subscription_added: usize,
    subscription_updated: usize,
    subscription_removed: usize,
    tab_visible_rows_after_refresh: usize,
    initial_core_bundle_bytes: usize,
    initial_core_bundle_gzip_bytes: Option<usize>,
    refresh_core_bundle_bytes: usize,
    refresh_core_bundle_gzip_bytes: Option<usize>,
    core_database_bytes: i64,
    edge_database_bytes: i64,
    worker_database_bytes: i64,
    tab_database_bytes: i64,
}

#[derive(Serialize)]
struct RecursiveClosureLayoutProbe {
    node_count: usize,
    branch_factor: usize,
    edge_only_database_bytes: i64,
    closure_database_bytes: i64,
    closure_rows: usize,
    seed_edges_ms: f64,
    seed_closure_ms: f64,
    recursive_cte_ms: f64,
    closure_query_ms: f64,
    recursive_rows: usize,
    closure_rows_returned: usize,
}

#[derive(Serialize)]
struct ColdReopenProfileProbe {
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    seed_ms: f64,
    cold_export_total_ms: f64,
    warm_export_total_ms: f64,
    cold_export_read_rows_ms: f64,
    warm_export_read_rows_ms: f64,
    cold_export_history_rows: usize,
    bundle_bytes: usize,
    cold_worker_apply_ms: f64,
    cold_worker_apply_history_ms: f64,
    warm_worker_query_ms: f64,
    reopened_worker_query_ms: f64,
    reopened_worker_observed_reads: usize,
    core_database_bytes: i64,
    worker_database_bytes: i64,
}

#[derive(Serialize)]
struct ProjectBoardProbe {
    user_count: usize,
    project_count: usize,
    task_count: usize,
    comment_count: usize,
    sampled_users: usize,
    page_size: usize,
    seed_ms: f64,
    my_tasks_export_ms: f64,
    merged_bundle_bytes: usize,
    merged_history_rows: usize,
    merged_transaction_rows: usize,
    tab_apply_ms: f64,
    tab_apply_profile: ApplyBundleProfile,
    tab_query_ms: f64,
    visible_rows_returned: usize,
    core_database_bytes: i64,
    tab_database_bytes: i64,
}

#[derive(Serialize)]
struct CurrentProjectionTradeoffProbe {
    current_projection: CurrentProjectionTradeoffCase,
    history_only: CurrentProjectionTradeoffCase,
    deep_versions_history_only: CurrentProjectionTradeoffCase,
    saved_bytes_without_current: i64,
    history_only_query_slowdown: f64,
}

#[derive(Serialize)]
struct CurrentProjectionTradeoffCase {
    row_count: usize,
    update_count: usize,
    database_bytes: i64,
    seed_ms: f64,
    query_ms: f64,
    rows_returned: usize,
}

#[derive(Serialize)]
struct MixedMutationRefreshProbe {
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    top_inserts: usize,
    current_page_updates: usize,
    current_page_deletes: usize,
    off_page_owner_updates: usize,
    unrelated_owner_updates: usize,
    visible_rows_returned: usize,
    history_rows_synced: usize,
    transaction_rows_synced: usize,
    observed_facts_synced: usize,
    bundle_bytes: usize,
    export_ms: f64,
    apply_ms: f64,
    tab_query_ms: f64,
    subscription_poll_ms: f64,
    subscription_added: usize,
    subscription_updated: usize,
    subscription_removed: usize,
}

#[derive(Serialize)]
struct WideSchemaApplyProbe {
    total_tables: usize,
    synced_tables: usize,
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    visible_rows_returned: usize,
    history_rows_synced: usize,
    transaction_rows_synced: usize,
    bundle_bytes: usize,
    apply_ms: f64,
    query_ms: f64,
    tab_database_bytes: i64,
}

#[derive(Serialize)]
struct StorageTopologyProbe {
    all_memory_intermediaries: StorageTopologyCase,
    durable_intermediaries: StorageTopologyCase,
}

#[derive(Serialize)]
struct StorageTopologyCase {
    durable_intermediaries: bool,
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    bundle_bytes: usize,
    core_export_ms: f64,
    edge_apply_ms: f64,
    edge_export_ms: f64,
    worker_apply_ms: f64,
    worker_export_ms: f64,
    tab_apply_ms: f64,
    tab_query_ms: f64,
    api_to_first_result_ms: f64,
    edge_database_bytes: i64,
    worker_database_bytes: i64,
}

#[derive(Serialize)]
struct MultiQueryRefreshProbe {
    query_count: usize,
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    inserted_per_query: usize,
    refresh_bundle_count: usize,
    refresh_bundle_bytes: usize,
    equivalent_merged_bundle_bytes: usize,
    refresh_export_ms: f64,
    refresh_apply_ms: f64,
    equivalent_merged_apply_ms: f64,
    refresh_history_rows: usize,
    equivalent_merged_history_rows: usize,
    refresh_transaction_rows: usize,
    equivalent_merged_transaction_rows: usize,
    refresh_observed_facts: usize,
}

#[derive(Serialize)]
struct SubscriptionStormProbe {
    subscription_count: usize,
    total_rows: usize,
    page_size: usize,
    inserted_per_subscription: usize,
    merged_bundle_bytes: usize,
    apply_ms: f64,
    total_poll_ms: f64,
    average_poll_ms: f64,
    total_added: usize,
    total_updated: usize,
    total_removed: usize,
}

#[derive(Serialize)]
struct ApplyProfileProbe {
    subscription_count: usize,
    total_rows: usize,
    page_size: usize,
    inserted_per_subscription: usize,
    bundle_bytes: usize,
    profile: ApplyBundleProfile,
}

#[derive(Serialize)]
struct BranchOverlayProbe {
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    branch_overlay_updates: usize,
    main_visible_rows: usize,
    branch_visible_rows: usize,
    main_query_ms: f64,
    branch_query_ms: f64,
    branch_export_ms: f64,
    branch_bundle_bytes: usize,
    branch_history_rows: usize,
    branch_transaction_rows: usize,
    export_profile: QueryExportProfile,
}

#[derive(Serialize)]
struct PinnedBranchSnapshotProbe {
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    post_base_updates: usize,
    overlay_updates: usize,
    branch_visible_rows: usize,
    branch_query_ms: f64,
    branch_export_ms: f64,
    branch_bundle_bytes: usize,
    branch_history_rows: usize,
    branch_transaction_rows: usize,
    export_profile: QueryExportProfile,
}

#[derive(Serialize)]
struct BranchFanInProbe {
    total_rows: usize,
    branch_count: usize,
    source_count: usize,
    page_size: usize,
    create_source_branches_ms: f64,
    create_fan_in_branch_ms: f64,
    branch_query_ms: f64,
    branch_export_ms: f64,
    visible_rows_returned: usize,
    bundle_bytes: usize,
    history_rows: usize,
    transaction_rows: usize,
    core_database_bytes: i64,
}

#[derive(Serialize)]
struct ExportProfileProbe {
    total_rows: usize,
    target_owner_rows: usize,
    page_size: usize,
    bundle_bytes: usize,
    profile: QueryExportProfile,
}

#[derive(Serialize)]
struct WarmBootReport {
    visible_rows_returned: usize,
    history_rows_synced: usize,
    transaction_rows_synced: usize,
    observed_facts_synced: usize,
    bundle_bytes: usize,
    export_ms: f64,
    first_apply_ms: f64,
    second_apply_ms: Option<f64>,
    query_ms: f64,
    api_to_first_result_ms: f64,
}

#[derive(Serialize)]
struct RefreshReport {
    inserted_rows: usize,
    visible_rows_returned: usize,
    history_rows_synced: usize,
    transaction_rows_synced: usize,
    observed_facts_synced: usize,
    bundle_bytes: usize,
    export_ms: f64,
    core_to_edge_apply_ms: f64,
    edge_export_ms: f64,
    edge_to_worker_apply_ms: f64,
    worker_export_ms: f64,
    worker_to_tab_apply_ms: f64,
    tab_query_ms: f64,
    tab_subscription_poll_ms: f64,
    tab_subscription_added: usize,
    tab_subscription_updated: usize,
    tab_subscription_moved: usize,
    tab_subscription_removed: usize,
    api_to_updated_result_ms: f64,
}

fn run_core_only_scoped_page(config: &Config) -> BenchResult<ScenarioReport> {
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut edge = Runtime::open_trusted_with_schema(
        storage_for(config, &dir, "edge.sqlite"),
        "edge",
        schema.clone(),
    )?;
    let mut worker = Runtime::open_with_schema(
        storage_for(config, &dir, "worker.sqlite"),
        "worker",
        OWNER,
        schema.clone(),
    )?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema.clone())?;

    let seed_started = Instant::now();
    seed_documents(
        &mut core,
        config.total_rows,
        config.target_owner_rows,
        config.seed_batch_size,
    )?;
    let seed_elapsed = seed_started.elapsed();

    let export_started = Instant::now();
    let core_bundle = export_top_owner_page(&mut core, config.page_size)?;
    let export_elapsed = export_started.elapsed();

    let core_bundle_summary = BundleSummary::from(&core_bundle)?;

    let edge_apply_elapsed = timed(|| edge.apply_bundle(&core_bundle))?;
    let edge_export_started = Instant::now();
    let edge_bundle = export_top_owner_page(&mut edge, config.page_size)?;
    let edge_export_elapsed = edge_export_started.elapsed();
    let worker_apply_elapsed = timed(|| worker.apply_bundle(&edge_bundle))?;
    let worker_export_started = Instant::now();
    let worker_bundle = export_top_owner_page(&mut worker, config.page_size)?;
    let worker_export_elapsed = worker_export_started.elapsed();
    let tab_apply_elapsed = timed(|| tab.apply_bundle(&worker_bundle))?;

    let query_started = Instant::now();
    let rows = tab.read_rows_where_eq_top_field_desc(
        "documents",
        "owner_id",
        json!(OWNER),
        "updated_at",
        config.page_size,
    )?;
    let tab_query_elapsed = query_started.elapsed();
    let core_query_started = Instant::now();
    let core_rows = core.run_as_user(OWNER, |core| {
        core.read_rows_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!(OWNER),
            "updated_at",
            config.page_size,
        )
    })?;
    let core_query_elapsed = core_query_started.elapsed();
    assert_eq!(core_rows.len(), config.page_size);
    let edge_warm_worker_cold = run_edge_warm_worker_cold(config, &dir, &schema, &mut edge)?;
    let worker_warm_tab_cold = run_worker_warm_tab_cold(config, &mut worker)?;
    let mut tab_subscription = tab.subscribe_rows_where_eq_top_field_desc(
        "documents",
        "owner_id",
        json!(OWNER),
        "updated_at",
        config.page_size,
    )?;
    let refresh_after_new_top_rows = run_refresh_after_new_top_rows(
        config,
        &mut core,
        &mut edge,
        &mut worker,
        &mut tab,
        &mut tab_subscription,
    )?;

    let mut seed_rows_by_table = BTreeMap::new();
    seed_rows_by_table.insert("orgs", 100);
    seed_rows_by_table.insert("documents", config.total_rows);
    let approx_raw_json_payload_bytes = approx_raw_json_payload_bytes(config)?;
    let core_stats = core.storage_stats()?;
    let edge_stats = edge.storage_stats()?;
    let worker_stats = worker.storage_stats()?;
    let tab_stats = tab.storage_stats()?;
    let core_database_bytes = core_stats.database_bytes;

    Ok(ScenarioReport {
        scenario_id: "C1_CORE_ONLY_SCOPED_PAGE_UPDATED_AT",
        profile_id: format!(
            "rows_{}_owner_{}_page_{}",
            config.total_rows, config.target_owner_rows, config.page_size
        ),
        topology: "tab_memory__worker__edge__core",
        cache_mode: if config.durable_intermediaries {
            "core_only_cold_with_durable_intermediaries"
        } else {
            "core_only_cold_all_memory_except_core"
        },
        seed_rows_by_table,
        seed_batch_size: config.seed_batch_size,
        visible_rows_returned: rows.len(),
        history_rows_synced: core_bundle.history.len(),
        transaction_rows_synced: core_bundle.txs.len(),
        observed_facts_synced: core_bundle.query_reads.len(),
        bundle_bytes: core_bundle_summary.bytes,
        approx_raw_json_payload_bytes,
        core_database_to_raw_payload_ratio: core_database_bytes as f64
            / approx_raw_json_payload_bytes as f64,
        core_database_bytes,
        core_total_file_bytes: core_stats.total_file_bytes,
        core_table_page_bytes: core_stats.table_page_bytes,
        edge_database_bytes: edge_stats.database_bytes,
        edge_total_file_bytes: edge_stats.total_file_bytes,
        edge_table_page_bytes: edge_stats.table_page_bytes,
        worker_database_bytes: worker_stats.database_bytes,
        worker_total_file_bytes: worker_stats.total_file_bytes,
        worker_table_page_bytes: worker_stats.table_page_bytes,
        tab_database_bytes: tab_stats.database_bytes,
        tab_total_file_bytes: tab_stats.total_file_bytes,
        tab_table_page_bytes: tab_stats.table_page_bytes,
        seed_ms: ms(seed_elapsed),
        core_query_ms: ms(core_query_elapsed),
        export_ms: ms(export_elapsed),
        core_to_edge_apply_ms: ms(edge_apply_elapsed),
        edge_export_ms: ms(edge_export_elapsed),
        edge_to_worker_apply_ms: ms(worker_apply_elapsed),
        worker_export_ms: ms(worker_export_elapsed),
        worker_to_tab_apply_ms: ms(tab_apply_elapsed),
        tab_query_ms: ms(tab_query_elapsed),
        api_to_first_result_ms: ms(export_elapsed
            + edge_apply_elapsed
            + edge_export_elapsed
            + worker_apply_elapsed
            + worker_export_elapsed
            + tab_apply_elapsed
            + tab_query_elapsed),
        edge_warm_worker_cold,
        worker_warm_tab_cold,
        refresh_after_new_top_rows,
    })
}

fn run_tx_granularity_probe() -> BenchResult<TxGranularityProbe> {
    Ok(TxGranularityProbe {
        batched_100: run_tx_granularity_case(100)?,
        one_write_per_row: run_tx_granularity_case(1)?,
    })
}

fn run_tx_granularity_case(seed_batch_size: usize) -> BenchResult<TxGranularityCase> {
    let config = Config {
        total_rows: 5_000,
        target_owner_rows: 500,
        page_size: 50,
        seed_batch_size,
        refresh_new_top_rows: 0,
        durable_intermediaries: true,
    };
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema,
    )?;

    let seed_started = Instant::now();
    seed_documents(
        &mut core,
        config.total_rows,
        config.target_owner_rows,
        config.seed_batch_size,
    )?;
    let seed_elapsed = seed_started.elapsed();

    let export_started = Instant::now();
    let bundle = export_top_owner_page(&mut core, config.page_size)?;
    let export_elapsed = export_started.elapsed();
    let summary = BundleSummary::from(&bundle)?;

    Ok(TxGranularityCase {
        total_rows: config.total_rows,
        target_owner_rows: config.target_owner_rows,
        seed_batch_size,
        seed_ms: ms(seed_elapsed),
        export_ms: ms(export_elapsed),
        bundle_bytes: summary.bytes,
        history_rows_synced: bundle.history.len(),
        transaction_rows_synced: bundle.txs.len(),
        core_database_bytes: core.storage_stats()?.database_bytes,
    })
}

fn run_recursive_policy_probe() -> BenchResult<RecursivePolicyProbe> {
    let total_rows = 20_000;
    let target_owner_rows = 2_000;
    let page_size = 50;
    let dir = tempdir()?;
    let schema = recursive_policy_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema,
    )?;

    let seed_started = Instant::now();
    seed_recursive_policy_graph(&mut core, total_rows, target_owner_rows, 100)?;
    let seed_elapsed = seed_started.elapsed();

    let query_started = Instant::now();
    let rows = core.run_as_user(OWNER, |core| {
        core.read_rows_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!(OWNER),
            "updated_at",
            page_size,
        )
    })?;
    let query_elapsed = query_started.elapsed();

    let export_started = Instant::now();
    let bundle = export_top_owner_page(&mut core, page_size)?;
    let export_elapsed = export_started.elapsed();
    let summary = BundleSummary::from(&bundle)?;

    Ok(RecursivePolicyProbe {
        total_rows,
        target_owner_rows,
        policy_depth: 3,
        visible_rows_returned: rows.len(),
        history_rows_synced: bundle.history.len(),
        transaction_rows_synced: bundle.txs.len(),
        bundle_bytes: summary.bytes,
        core_database_bytes: core.storage_stats()?.database_bytes,
        seed_ms: ms(seed_elapsed),
        core_query_ms: ms(query_elapsed),
        export_ms: ms(export_elapsed),
    })
}

fn run_multi_tab_fanout_probe() -> BenchResult<MultiTabFanoutProbe> {
    let total_rows = 20_000;
    let target_owner_rows = 2_000;
    let page_size = 50;
    let tab_count = 8;
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut worker = Runtime::open_with_schema(
        Storage::File(dir.path().join("worker.sqlite")),
        "worker",
        OWNER,
        schema.clone(),
    )?;
    seed_documents(&mut core, total_rows, target_owner_rows, 100)?;

    let worker_boot_started = Instant::now();
    let core_bundle = export_top_owner_page(&mut core, page_size)?;
    worker.apply_bundle(&core_bundle)?;
    let worker_boot_elapsed = worker_boot_started.elapsed();

    let worker_export_started = Instant::now();
    let worker_bundle = export_top_owner_page(&mut worker, page_size)?;
    let worker_export_elapsed = worker_export_started.elapsed();
    let summary = BundleSummary::from(&worker_bundle)?;

    let mut total_apply = Duration::ZERO;
    let mut total_query = Duration::ZERO;
    for tab_index in 0..tab_count {
        let mut tab = Runtime::open_with_schema(
            Storage::Memory,
            &format!("tab-{tab_index}"),
            OWNER,
            schema.clone(),
        )?;
        total_apply += timed(|| tab.apply_bundle(&worker_bundle))?;
        let query_started = Instant::now();
        let rows = read_top_owner_page(&tab, page_size)?;
        total_query += query_started.elapsed();
        assert_eq!(rows.len(), page_size);
    }

    Ok(MultiTabFanoutProbe {
        total_rows,
        target_owner_rows,
        tab_count,
        worker_boot_ms: ms(worker_boot_elapsed),
        worker_export_ms: ms(worker_export_elapsed),
        bundle_bytes: summary.bytes,
        history_rows_synced: worker_bundle.history.len(),
        transaction_rows_synced: worker_bundle.txs.len(),
        total_tab_apply_ms: ms(total_apply),
        average_tab_apply_ms: ms(total_apply) / tab_count as f64,
        total_tab_query_ms: ms(total_query),
        average_tab_query_ms: ms(total_query) / tab_count as f64,
    })
}

fn run_many_user_page_probe() -> BenchResult<ManyUserPageProbe> {
    let user_count = 100;
    let rows_per_user = 500;
    let total_rows = user_count * rows_per_user;
    let sampled_users = 20;
    let page_size = 20;
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema,
    )?;

    let seed_started = Instant::now();
    seed_many_user_documents(&mut core, user_count, rows_per_user, 100)?;
    let seed_elapsed = seed_started.elapsed();

    let export_started = Instant::now();
    let mut total_bundle_bytes = 0;
    let mut total_history_rows_synced = 0;
    let mut total_transaction_rows_synced = 0;
    for user_index in 0..sampled_users {
        let user = format!("user-{user_index}");
        let bundle = export_top_owner_page_for(&mut core, &user, &user, page_size)?;
        let summary = BundleSummary::from(&bundle)?;
        total_bundle_bytes += summary.bytes;
        total_history_rows_synced += bundle.history.len();
        total_transaction_rows_synced += bundle.txs.len();
    }
    let export_elapsed = export_started.elapsed();

    Ok(ManyUserPageProbe {
        user_count,
        total_rows,
        rows_per_user,
        sampled_users,
        page_size,
        seed_ms: ms(seed_elapsed),
        total_export_ms: ms(export_elapsed),
        average_export_ms: ms(export_elapsed) / sampled_users as f64,
        total_bundle_bytes,
        average_bundle_bytes: total_bundle_bytes as f64 / sampled_users as f64,
        total_history_rows_synced,
        average_history_rows_synced: total_history_rows_synced as f64 / sampled_users as f64,
        total_transaction_rows_synced,
        average_transaction_rows_synced: total_transaction_rows_synced as f64
            / sampled_users as f64,
        core_database_bytes: core.storage_stats()?.database_bytes,
    })
}

fn run_user_id_footprint_probe() -> BenchResult<UserIdFootprintProbe> {
    let user_count = 100;
    let rows_per_user = 200;
    let short_user_ids = run_user_id_footprint_case(user_count, rows_per_user, false)?;
    let long_user_ids = run_user_id_footprint_case(user_count, rows_per_user, true)?;
    let total_rows = user_count * rows_per_user;
    Ok(UserIdFootprintProbe {
        additional_bytes_per_row_for_long_ids: (long_user_ids.core_database_bytes
            - short_user_ids.core_database_bytes)
            as f64
            / total_rows as f64,
        short_user_ids,
        long_user_ids,
    })
}

fn run_user_id_footprint_case(
    user_count: usize,
    rows_per_user: usize,
    long_user_ids: bool,
) -> BenchResult<UserIdFootprintCase> {
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema,
    )?;
    let representative_user = synthetic_user_id(0, long_user_ids);

    let seed_started = Instant::now();
    seed_many_user_documents_with_id_shape(
        &mut core,
        user_count,
        rows_per_user,
        100,
        long_user_ids,
    )?;
    let seed_elapsed = seed_started.elapsed();

    let stats = core.storage_stats()?;
    Ok(UserIdFootprintCase {
        user_count,
        rows_per_user,
        representative_user_id_bytes: representative_user.len(),
        seed_ms: ms(seed_elapsed),
        core_database_bytes: stats.database_bytes,
        current_page_bytes: *stats
            .table_page_bytes
            .get("documents__schema_v1_current")
            .unwrap_or(&0),
        history_page_bytes: *stats
            .table_page_bytes
            .get("documents__schema_v1_history")
            .unwrap_or(&0),
        tx_page_bytes: *stats.table_page_bytes.get("jazz_tx").unwrap_or(&0),
    })
}

fn run_user_id_interning_projection_probe() -> BenchResult<UserIdInterningProjectionProbe> {
    let user_count = 100;
    let rows_per_user = 500;
    let text_system_users =
        run_user_id_interning_projection_case(user_count, rows_per_user, false)?;
    let interned_system_users =
        run_user_id_interning_projection_case(user_count, rows_per_user, true)?;
    let total_rows = user_count * rows_per_user;
    Ok(UserIdInterningProjectionProbe {
        saved_bytes_per_row: (text_system_users.database_bytes
            - interned_system_users.database_bytes) as f64
            / total_rows as f64,
        text_system_users,
        interned_system_users,
    })
}

fn run_user_id_interning_projection_case(
    user_count: usize,
    rows_per_user: usize,
    interned: bool,
) -> BenchResult<UserIdInterningProjectionCase> {
    let dir = tempdir()?;
    let path = dir.path().join(if interned {
        "interned.sqlite"
    } else {
        "text.sqlite"
    });
    let mut conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "OFF")?;
    conn.pragma_update(None, "synchronous", "OFF")?;

    if interned {
        conn.execute_batch(
            "CREATE TABLE jazz_user (
               user_num INTEGER PRIMARY KEY,
               user_id TEXT NOT NULL UNIQUE
             );
             CREATE TABLE documents (
               row_num INTEGER PRIMARY KEY,
               owner_id TEXT NOT NULL,
               updated_at TEXT NOT NULL,
               title TEXT NOT NULL,
               j_created_by_num INTEGER NOT NULL,
               j_updated_by_num INTEGER NOT NULL
             );
             CREATE INDEX documents_owner_updated
               ON documents(owner_id, updated_at DESC, row_num);",
        )?;
    } else {
        conn.execute_batch(
            "CREATE TABLE documents (
               row_num INTEGER PRIMARY KEY,
               owner_id TEXT NOT NULL,
               updated_at TEXT NOT NULL,
               title TEXT NOT NULL,
               j_created_by TEXT NOT NULL,
               j_updated_by TEXT NOT NULL
             );
             CREATE INDEX documents_owner_updated
               ON documents(owner_id, updated_at DESC, row_num);",
        )?;
    }

    let seed_started = Instant::now();
    let tx = conn.transaction()?;
    if interned {
        {
            let mut insert_user =
                tx.prepare("INSERT INTO jazz_user (user_num, user_id) VALUES (?, ?)")?;
            for user_index in 0..user_count {
                insert_user.execute(params![
                    user_index as i64 + 1,
                    synthetic_user_id(user_index, true)
                ])?;
            }
        }
        {
            let mut insert_doc = tx.prepare(
                "INSERT INTO documents
                   (row_num, owner_id, updated_at, title, j_created_by_num, j_updated_by_num)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )?;
            for row_index in 0..(user_count * rows_per_user) {
                let user_index = row_index / rows_per_user;
                insert_doc.execute(params![
                    row_index as i64 + 1,
                    synthetic_user_id(user_index, true),
                    format!("{:020}", row_index),
                    format!("Projected document {row_index}"),
                    user_index as i64 + 1,
                    user_index as i64 + 1
                ])?;
            }
        }
    } else {
        let mut insert_doc = tx.prepare(
            "INSERT INTO documents
               (row_num, owner_id, updated_at, title, j_created_by, j_updated_by)
             VALUES (?, ?, ?, ?, ?, ?)",
        )?;
        for row_index in 0..(user_count * rows_per_user) {
            let user_index = row_index / rows_per_user;
            let user_id = synthetic_user_id(user_index, true);
            insert_doc.execute(params![
                row_index as i64 + 1,
                user_id,
                format!("{:020}", row_index),
                format!("Projected document {row_index}"),
                user_id,
                synthetic_user_id(user_index, true)
            ])?;
        }
    }
    tx.commit()?;
    let seed_elapsed = seed_started.elapsed();

    let owner = synthetic_user_id(0, true);
    let materialize_started = Instant::now();
    if interned {
        let mut stmt = conn.prepare(
            "SELECT d.row_num, d.owner_id, d.updated_at, d.title, created.user_id, updated.user_id
             FROM documents d
             JOIN jazz_user created ON created.user_num = d.j_created_by_num
             JOIN jazz_user updated ON updated.user_num = d.j_updated_by_num
             WHERE d.owner_id = ?
             ORDER BY d.updated_at DESC, d.row_num
             LIMIT 50",
        )?;
        let rows = stmt
            .query_map(params![owner], |row| row.get::<_, String>(4))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(rows.len(), 50);
    } else {
        let mut stmt = conn.prepare(
            "SELECT row_num, owner_id, updated_at, title, j_created_by, j_updated_by
             FROM documents
             WHERE owner_id = ?
             ORDER BY updated_at DESC, row_num
             LIMIT 50",
        )?;
        let rows = stmt
            .query_map(params![owner], |row| row.get::<_, String>(4))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(rows.len(), 50);
    }
    let materialize_elapsed = materialize_started.elapsed();

    Ok(UserIdInterningProjectionCase {
        user_count,
        rows_per_user,
        representative_user_id_bytes: synthetic_user_id(0, true).len(),
        database_bytes: sqlite_database_bytes(&conn)?,
        seed_ms: ms(seed_elapsed),
        materialize_page_ms: ms(materialize_elapsed),
    })
}

fn run_permissioned_dashboard_probe() -> BenchResult<PermissionedDashboardProbe> {
    let total_rows = env_usize("MINI_JAZZ_PERF_DASHBOARD_TOTAL_ROWS", 50_000);
    let target_owner_rows = env_usize("MINI_JAZZ_PERF_DASHBOARD_TARGET_OWNER_ROWS", 5_000);
    let query_count = env_usize("MINI_JAZZ_PERF_DASHBOARD_QUERY_COUNT", 24);
    let page_size = env_usize("MINI_JAZZ_PERF_DASHBOARD_PAGE_SIZE", 20);
    let dir = tempdir()?;
    let schema = recursive_policy_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut edge = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("edge.sqlite")),
        "edge",
        schema.clone(),
    )?;
    let mut worker = Runtime::open_with_schema(
        Storage::File(dir.path().join("worker.sqlite")),
        "worker",
        OWNER,
        schema.clone(),
    )?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema)?;

    let seed_started = Instant::now();
    seed_recursive_policy_graph(&mut core, total_rows, target_owner_rows, 100)?;
    let seed_elapsed = seed_started.elapsed();

    let owners = dashboard_owner_filters(query_count);
    let export_started = Instant::now();
    let merged_bundle = core.run_as_user(OWNER, |core| {
        core.export_many_query_where_eq_top_field_desc(
            "documents",
            "owner_id",
            owners.iter().map(|owner| json!(owner)).collect(),
            "updated_at",
            page_size,
        )
    })?;
    let core_export_elapsed = export_started.elapsed();
    let merged_summary = BundleSummary::from(&merged_bundle)?;
    let edge_apply_elapsed = timed(|| edge.apply_bundle(&merged_bundle))?;
    let edge_bundle = edge.run_as_user(OWNER, |edge| {
        edge.export_many_query_where_eq_top_field_desc(
            "documents",
            "owner_id",
            owners.iter().map(|owner| json!(owner)).collect(),
            "updated_at",
            page_size,
        )
    })?;
    let worker_apply_elapsed = timed(|| worker.apply_bundle(&edge_bundle))?;
    let worker_bundle = worker.export_many_query_where_eq_top_field_desc(
        "documents",
        "owner_id",
        owners.iter().map(|owner| json!(owner)).collect(),
        "updated_at",
        page_size,
    )?;
    let tab_apply_elapsed = timed(|| tab.apply_bundle(&worker_bundle))?;

    let subscribe_started = Instant::now();
    let mut subscriptions = owners
        .iter()
        .map(|owner| {
            tab.subscribe_rows_where_eq_top_field_desc(
                "documents",
                "owner_id",
                json!(owner),
                "updated_at",
                page_size,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let subscribe_elapsed = subscribe_started.elapsed();

    insert_new_top_recursive_documents_for_owners(&mut core, total_rows, &owners, 3)?;
    let refresh_started = Instant::now();
    let edge_reads = edge.observed_query_reads()?;
    let core_refresh_bundles =
        core.run_as_user(OWNER, |core| core.export_query_read_refreshes(&edge_reads))?;
    let core_refresh_elapsed = refresh_started.elapsed();
    let core_refresh_merged = merge_bundles(&core_refresh_bundles)?;
    let refresh_summary = BundleSummary::from(&core_refresh_merged)?;
    let edge_refresh_apply_elapsed = timed(|| edge.apply_bundle(&core_refresh_merged))?;

    let edge_refresh_export_started = Instant::now();
    let worker_reads = worker.observed_query_reads()?;
    let edge_refresh_bundles = edge.run_as_user(OWNER, |edge| {
        edge.export_query_read_refreshes(&worker_reads)
    })?;
    let edge_refresh_export_elapsed = edge_refresh_export_started.elapsed();
    let edge_refresh_merged = merge_bundles(&edge_refresh_bundles)?;
    let worker_refresh_apply_elapsed = timed(|| worker.apply_bundle(&edge_refresh_merged))?;

    let worker_refresh_export_started = Instant::now();
    let worker_refresh_bundles =
        worker.export_query_read_refreshes(&tab.observed_query_reads()?)?;
    let worker_refresh_export_elapsed = worker_refresh_export_started.elapsed();
    let worker_refresh_merged = merge_bundles(&worker_refresh_bundles)?;
    let tab_refresh_apply_elapsed = timed(|| tab.apply_bundle(&worker_refresh_merged))?;
    let poll_started = Instant::now();
    let mut total_counts = DiffCounts {
        added: 0,
        updated: 0,
        moved: 0,
        removed: 0,
    };
    for subscription in &mut subscriptions {
        let counts = DiffCounts::from(&tab.poll_subscription(subscription)?);
        total_counts.added += counts.added;
        total_counts.updated += counts.updated;
        total_counts.moved += counts.moved;
        total_counts.removed += counts.removed;
    }
    let poll_elapsed = poll_started.elapsed();

    Ok(PermissionedDashboardProbe {
        total_rows,
        target_owner_rows,
        query_count,
        page_size,
        seed_ms: ms(seed_elapsed),
        core_export_ms: ms(core_export_elapsed),
        merged_bundle_bytes: merged_summary.bytes,
        merged_history_rows: merged_bundle.history.len(),
        merged_transaction_rows: merged_bundle.txs.len(),
        edge_apply_ms: ms(edge_apply_elapsed),
        worker_apply_ms: ms(worker_apply_elapsed),
        tab_apply_ms: ms(tab_apply_elapsed),
        subscribe_ms: ms(subscribe_elapsed),
        refresh_core_export_ms: ms(core_refresh_elapsed),
        refresh_edge_apply_ms: ms(edge_refresh_apply_elapsed),
        refresh_edge_export_ms: ms(edge_refresh_export_elapsed),
        refresh_worker_apply_ms: ms(worker_refresh_apply_elapsed),
        refresh_worker_export_ms: ms(worker_refresh_export_elapsed),
        refresh_tab_apply_ms: ms(tab_refresh_apply_elapsed),
        refresh_bundle_bytes: refresh_summary.bytes,
        refresh_history_rows: core_refresh_merged.history.len(),
        refresh_transaction_rows: core_refresh_merged.txs.len(),
        subscription_poll_ms: ms(poll_elapsed),
        subscription_added: total_counts.added,
        subscription_updated: total_counts.updated,
        subscription_removed: total_counts.removed,
        core_database_bytes: core.storage_stats()?.database_bytes,
        tab_database_bytes: tab.storage_stats()?.database_bytes,
    })
}

fn run_dashboard_query_scaling_probe() -> BenchResult<DashboardQueryScalingProbe> {
    let total_rows = env_usize("MINI_JAZZ_PERF_DASHBOARD_TOTAL_ROWS", 50_000);
    let target_owner_rows = env_usize("MINI_JAZZ_PERF_DASHBOARD_TARGET_OWNER_ROWS", 5_000);
    let page_size = env_usize("MINI_JAZZ_PERF_DASHBOARD_PAGE_SIZE", 20);
    let schema = recursive_policy_schema();

    let mut cases = Vec::new();
    for (case_index, query_count) in [1, 4, 12, 24, 48].into_iter().enumerate() {
        let dir = tempdir()?;
        let mut core = Runtime::open_trusted_with_schema(
            Storage::File(dir.path().join("core.sqlite")),
            &format!("scaling-core-{query_count}"),
            schema.clone(),
        )?;
        seed_recursive_policy_graph(&mut core, total_rows, target_owner_rows, 100)?;
        let owners = dashboard_owner_filters(query_count);
        let mut tab = Runtime::open_with_schema(
            Storage::Memory,
            &format!("scaling-tab-{query_count}"),
            OWNER,
            schema.clone(),
        )?;

        let initial_started = Instant::now();
        let initial_bundle = core.run_as_user(OWNER, |core| {
            core.export_many_query_where_eq_top_field_desc(
                "documents",
                "owner_id",
                owners.iter().map(|owner| json!(owner)).collect(),
                "updated_at",
                page_size,
            )
        })?;
        let initial_elapsed = initial_started.elapsed();
        let initial_summary = BundleSummary::from(&initial_bundle)?;
        let tab_apply_profile = tab.profile_apply_bundle(&initial_bundle)?;

        insert_new_top_recursive_documents_for_owners(
            &mut core,
            total_rows + case_index * 10_000,
            &owners,
            1,
        )?;
        let refresh_started = Instant::now();
        let refresh_bundles = core.run_as_user(OWNER, |core| {
            core.export_query_read_refreshes(&tab.observed_query_reads()?)
        })?;
        let refresh_elapsed = refresh_started.elapsed();
        let refresh_summary = BundleBatchSummary::from(&refresh_bundles)?;
        let refresh_bundle_count = refresh_bundles.len();
        let refresh_apply_profile = profile_apply_bundles(&mut tab, refresh_bundles)?;

        cases.push(DashboardQueryScalingCase {
            query_count,
            initial_export_ms: ms(initial_elapsed),
            initial_bundle_bytes: initial_summary.bytes,
            initial_history_rows: initial_bundle.history.len(),
            initial_transaction_rows: initial_bundle.txs.len(),
            tab_apply_ms: tab_apply_profile.total_ms,
            tab_apply_profile,
            refresh_export_ms: ms(refresh_elapsed),
            refresh_bundle_count,
            refresh_bundle_bytes: refresh_summary.bytes,
            refresh_history_rows: refresh_summary.history_rows,
            refresh_apply_ms: refresh_apply_profile.total_ms,
            refresh_apply_profile,
        });
    }

    Ok(DashboardQueryScalingProbe {
        total_rows,
        target_owner_rows,
        page_size,
        cases,
    })
}

fn run_dashboard_query_scaling_repeat(
    repeat: usize,
) -> BenchResult<DashboardQueryScalingRepeatReport> {
    let mut samples = Vec::new();
    for _ in 0..repeat {
        samples.push(run_dashboard_query_scaling_probe()?);
    }
    let first = samples
        .first()
        .ok_or_else(|| "dashboard scaling repeat needs at least one sample".to_owned())?;
    let mut median_cases = Vec::new();
    for case_index in 0..first.cases.len() {
        let cases = samples
            .iter()
            .map(|sample| sample.cases[case_index].clone())
            .collect::<Vec<_>>();
        median_cases.push(DashboardQueryScalingCase {
            query_count: cases[0].query_count,
            initial_export_ms: median_f64(
                cases.iter().map(|case| case.initial_export_ms).collect(),
            ),
            initial_bundle_bytes: median_usize(
                cases.iter().map(|case| case.initial_bundle_bytes).collect(),
            ),
            initial_history_rows: median_usize(
                cases.iter().map(|case| case.initial_history_rows).collect(),
            ),
            initial_transaction_rows: median_usize(
                cases
                    .iter()
                    .map(|case| case.initial_transaction_rows)
                    .collect(),
            ),
            tab_apply_ms: median_f64(cases.iter().map(|case| case.tab_apply_ms).collect()),
            tab_apply_profile: median_apply_profile(
                cases
                    .iter()
                    .map(|case| case.tab_apply_profile.clone())
                    .collect(),
            ),
            refresh_export_ms: median_f64(
                cases.iter().map(|case| case.refresh_export_ms).collect(),
            ),
            refresh_bundle_count: median_usize(
                cases.iter().map(|case| case.refresh_bundle_count).collect(),
            ),
            refresh_bundle_bytes: median_usize(
                cases.iter().map(|case| case.refresh_bundle_bytes).collect(),
            ),
            refresh_history_rows: median_usize(
                cases.iter().map(|case| case.refresh_history_rows).collect(),
            ),
            refresh_apply_ms: median_f64(cases.iter().map(|case| case.refresh_apply_ms).collect()),
            refresh_apply_profile: median_apply_profile(
                cases
                    .iter()
                    .map(|case| case.refresh_apply_profile.clone())
                    .collect(),
            ),
        });
    }

    Ok(DashboardQueryScalingRepeatReport {
        repeat,
        median: DashboardQueryScalingProbe {
            total_rows: first.total_rows,
            target_owner_rows: first.target_owner_rows,
            page_size: first.page_size,
            cases: median_cases,
        },
        samples,
    })
}

fn run_recursive_tree_subscription_probe() -> BenchResult<RecursiveTreeSubscriptionProbe> {
    let node_count = env_usize("MINI_JAZZ_PERF_RECURSIVE_TREE_NODES", 2_000);
    let branch_factor = env_usize("MINI_JAZZ_PERF_RECURSIVE_TREE_BRANCH_FACTOR", 5).max(1);
    let repeated_noop_refresh_count =
        env_usize("MINI_JAZZ_PERF_RECURSIVE_REPEATED_NOOP_REFRESHES", 3);
    let root_id =
        env::var("MINI_JAZZ_PERF_RECURSIVE_TREE_ROOT_ID").unwrap_or_else(|_| "folder-0".to_owned());
    let rss_start_bytes = process_rss_bytes();
    let dir = tempdir()?;
    let schema = folder_tree_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema)?;

    let seed_started = Instant::now();
    core.run_as_user(OWNER, |core| {
        seed_folder_tree(core, node_count, branch_factor)
    })?;
    let seed_elapsed = seed_started.elapsed();
    let rss_after_seed_bytes = process_rss_bytes();

    let initial_read_started = Instant::now();
    let initial_rows = core.run_as_user(OWNER, |core| {
        core.read_recursive_refs("folders", &root_id, "parent")
    })?;
    let initial_read_elapsed = initial_read_started.elapsed();
    let initial_admin_read_started = Instant::now();
    core.run_attributing_to_user(OWNER, |core| {
        core.read_recursive_refs("folders", &root_id, "parent")
    })?;
    let initial_admin_read_elapsed = initial_admin_read_started.elapsed();

    let export_started = Instant::now();
    let initial_bundle = core.run_as_user(OWNER, |core| {
        core.export_recursive_refs("folders", &root_id, "parent")
    })?;
    let export_elapsed = export_started.elapsed();
    let initial_summary = BundleSummary::from(&initial_bundle)?;
    let initial_apply_profile = tab.profile_apply_bundle(&initial_bundle)?;
    let rss_after_initial_apply_bytes = process_rss_bytes();

    let subscribe_started = Instant::now();
    let mut subscription = tab.subscribe_observed_query(&tab.observed_query_reads()?[0])?;
    let subscribe_elapsed = subscribe_started.elapsed();

    core.run_as_user(OWNER, |core| {
        mutate_folder_tree(core, node_count, branch_factor)
    })?;
    let refresh_read_started = Instant::now();
    let refresh_rows = core.run_as_user(OWNER, |core| {
        core.read_recursive_refs("folders", &root_id, "parent")
    })?;
    let refresh_read_elapsed = refresh_read_started.elapsed();
    let refresh_started = Instant::now();
    let refresh_bundles = core.run_as_user(OWNER, |core| {
        core.export_query_read_refreshes(&tab.observed_query_reads()?)
    })?;
    let refresh_elapsed = refresh_started.elapsed();
    let refresh_merged = merge_bundles(&refresh_bundles)?;
    let refresh_summary = BundleSummary::from(&refresh_merged)?;
    let refresh_apply_profile = tab.profile_apply_bundle(&refresh_merged)?;
    let rss_after_refresh_bytes = process_rss_bytes();

    let poll_started = Instant::now();
    let diff_counts = DiffCounts::from(&tab.poll_subscription(&mut subscription)?);
    let poll_elapsed = poll_started.elapsed();
    let noop_refresh_started = Instant::now();
    let noop_refresh_bundles = core.run_as_user(OWNER, |core| {
        core.export_query_read_refreshes(&tab.observed_query_reads()?)
    })?;
    let noop_refresh_elapsed = noop_refresh_started.elapsed();
    let noop_refresh_merged = merge_bundles(&noop_refresh_bundles)?;
    let noop_refresh_history_rows = noop_refresh_merged.history.len();
    let noop_refresh_apply_profile = tab.profile_apply_bundle(&noop_refresh_merged)?;
    let rss_after_noop_refresh_bytes = process_rss_bytes();
    let noop_poll_started = Instant::now();
    let noop_diff_counts = DiffCounts::from(&tab.poll_subscription(&mut subscription)?);
    let noop_poll_elapsed = noop_poll_started.elapsed();
    let mut repeated_noop_total_export_ms = 0.0;
    let mut repeated_noop_total_apply_ms = 0.0;
    let mut repeated_noop_total_poll_ms = 0.0;
    let mut repeated_noop_total_history_rows = 0;
    let mut repeated_noop_total_diffs = 0;
    for _ in 0..repeated_noop_refresh_count {
        let repeated_export_started = Instant::now();
        let repeated_bundles = core.run_as_user(OWNER, |core| {
            core.export_query_read_refreshes(&tab.observed_query_reads()?)
        })?;
        repeated_noop_total_export_ms += ms(repeated_export_started.elapsed());
        let repeated_merged = merge_bundles(&repeated_bundles)?;
        repeated_noop_total_history_rows += repeated_merged.history.len();
        let repeated_apply_started = Instant::now();
        tab.apply_bundle(&repeated_merged)?;
        repeated_noop_total_apply_ms += ms(repeated_apply_started.elapsed());
        let repeated_poll_started = Instant::now();
        repeated_noop_total_diffs += tab.poll_subscription(&mut subscription)?.len();
        repeated_noop_total_poll_ms += ms(repeated_poll_started.elapsed());
    }
    let rss_after_repeated_noop_refreshes_bytes = process_rss_bytes();
    let visible_rows_after_refresh = tab
        .read_recursive_refs("folders", &root_id, "parent")?
        .len();

    Ok(RecursiveTreeSubscriptionProbe {
        node_count,
        branch_factor,
        root_id,
        rss_start_bytes,
        rss_after_seed_bytes,
        rss_after_initial_apply_bytes,
        rss_after_refresh_bytes,
        rss_after_noop_refresh_bytes,
        seed_ms: ms(seed_elapsed),
        initial_read_ms: ms(initial_read_elapsed),
        initial_admin_read_ms: ms(initial_admin_read_elapsed),
        initial_rows_read: initial_rows.len(),
        initial_export_ms: ms(export_elapsed),
        initial_bundle_bytes: initial_summary.bytes,
        initial_history_rows: initial_bundle.history.len(),
        initial_apply_ms: initial_apply_profile.total_ms,
        initial_apply_profile,
        subscribe_ms: ms(subscribe_elapsed),
        refresh_read_ms: ms(refresh_read_elapsed),
        refresh_rows_read: refresh_rows.len(),
        refresh_export_ms: ms(refresh_elapsed),
        refresh_bundle_bytes: refresh_summary.bytes,
        refresh_history_rows: refresh_merged.history.len(),
        refresh_apply_ms: refresh_apply_profile.total_ms,
        refresh_apply_profile,
        subscription_poll_ms: ms(poll_elapsed),
        noop_refresh_export_ms: ms(noop_refresh_elapsed),
        noop_refresh_history_rows,
        noop_refresh_apply_ms: noop_refresh_apply_profile.total_ms,
        noop_refresh_apply_profile,
        noop_subscription_poll_ms: ms(noop_poll_elapsed),
        noop_subscription_added: noop_diff_counts.added,
        noop_subscription_updated: noop_diff_counts.updated,
        noop_subscription_removed: noop_diff_counts.removed,
        repeated_noop_refresh_count,
        repeated_noop_total_export_ms,
        repeated_noop_total_apply_ms,
        repeated_noop_total_poll_ms,
        repeated_noop_total_history_rows,
        repeated_noop_total_diffs,
        rss_after_repeated_noop_refreshes_bytes,
        subscription_added: diff_counts.added,
        subscription_updated: diff_counts.updated,
        subscription_removed: diff_counts.removed,
        visible_rows_after_refresh,
        core_database_bytes: core.storage_stats()?.database_bytes,
        tab_database_bytes: tab.storage_stats()?.database_bytes,
    })
}

fn run_recursive_tree_topology_probe() -> BenchResult<RecursiveTreeTopologyProbe> {
    let node_count = env_usize("MINI_JAZZ_PERF_RECURSIVE_TREE_NODES", 2_000);
    let branch_factor = env_usize("MINI_JAZZ_PERF_RECURSIVE_TREE_BRANCH_FACTOR", 5).max(1);
    let root_id =
        env::var("MINI_JAZZ_PERF_RECURSIVE_TREE_ROOT_ID").unwrap_or_else(|_| "folder-0".to_owned());
    let rss_start_bytes = process_rss_bytes();
    let dir = tempdir()?;
    let schema = folder_tree_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut edge = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("edge.sqlite")),
        "edge",
        schema.clone(),
    )?;
    let mut worker = Runtime::open_trusted_with_schema(Storage::Memory, "worker", schema.clone())?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema)?;

    core.run_as_user(OWNER, |core| {
        seed_folder_tree(core, node_count, branch_factor)
    })?;
    let rss_after_seed_bytes = process_rss_bytes();

    let initial_core_export_started = Instant::now();
    let initial_core_bundle = core.run_as_user(OWNER, |core| {
        core.export_recursive_refs("folders", &root_id, "parent")
    })?;
    let initial_core_export_elapsed = initial_core_export_started.elapsed();
    let initial_core_summary = BundleSummary::from(&initial_core_bundle)?;
    let initial_core_bundle_gzip_bytes = gzip_json_bytes(&initial_core_bundle)?;
    let initial_edge_apply = edge.profile_apply_bundle(&initial_core_bundle)?;

    let initial_edge_export_started = Instant::now();
    let initial_edge_bundles =
        edge.run_as_user(OWNER, |edge| edge.export_observed_query_refreshes())?;
    let initial_edge_export_elapsed = initial_edge_export_started.elapsed();
    let initial_edge_bundle = merge_bundles(&initial_edge_bundles)?;
    let initial_worker_apply = worker.profile_apply_bundle(&initial_edge_bundle)?;

    let initial_worker_export_started = Instant::now();
    let initial_worker_bundles =
        worker.run_as_user(OWNER, |worker| worker.export_observed_query_refreshes())?;
    let initial_worker_export_elapsed = initial_worker_export_started.elapsed();
    let initial_worker_bundle = merge_bundles(&initial_worker_bundles)?;
    let initial_tab_apply = tab.profile_apply_bundle(&initial_worker_bundle)?;
    let mut subscription = tab.subscribe_observed_query(&tab.observed_query_reads()?[0])?;
    let rss_after_initial_flow_bytes = process_rss_bytes();

    core.run_as_user(OWNER, |core| {
        mutate_folder_tree(core, node_count, branch_factor)
    })?;

    let refresh_core_export_started = Instant::now();
    let refresh_core_bundles = core.run_as_user(OWNER, |core| {
        core.export_query_read_refreshes(&edge.observed_query_reads()?)
    })?;
    let refresh_core_export_elapsed = refresh_core_export_started.elapsed();
    let refresh_core_bundle = merge_bundles(&refresh_core_bundles)?;
    let refresh_core_summary = BundleSummary::from(&refresh_core_bundle)?;
    let refresh_core_bundle_gzip_bytes = gzip_json_bytes(&refresh_core_bundle)?;
    let refresh_edge_apply = edge.profile_apply_bundle(&refresh_core_bundle)?;

    let refresh_edge_export_started = Instant::now();
    let refresh_edge_bundles = edge.run_as_user(OWNER, |edge| {
        edge.export_query_read_refreshes(&worker.observed_query_reads()?)
    })?;
    let refresh_edge_export_elapsed = refresh_edge_export_started.elapsed();
    let refresh_edge_bundle = merge_bundles(&refresh_edge_bundles)?;
    let refresh_worker_apply = worker.profile_apply_bundle(&refresh_edge_bundle)?;

    let refresh_worker_export_started = Instant::now();
    let refresh_worker_bundles = worker.run_as_user(OWNER, |worker| {
        worker.export_query_read_refreshes(&tab.observed_query_reads()?)
    })?;
    let refresh_worker_export_elapsed = refresh_worker_export_started.elapsed();
    let refresh_worker_bundle = merge_bundles(&refresh_worker_bundles)?;
    let refresh_tab_apply = tab.profile_apply_bundle(&refresh_worker_bundle)?;
    let rss_after_refresh_flow_bytes = process_rss_bytes();

    let poll_started = Instant::now();
    let diff_counts = DiffCounts::from(&tab.poll_subscription(&mut subscription)?);
    let poll_elapsed = poll_started.elapsed();
    let tab_visible_rows_after_refresh = tab
        .read_recursive_refs("folders", &root_id, "parent")?
        .len();

    Ok(RecursiveTreeTopologyProbe {
        node_count,
        branch_factor,
        root_id,
        rss_start_bytes,
        rss_after_seed_bytes,
        rss_after_initial_flow_bytes,
        rss_after_refresh_flow_bytes,
        initial_core_export_ms: ms(initial_core_export_elapsed),
        initial_edge_apply_ms: initial_edge_apply.total_ms,
        initial_edge_export_ms: ms(initial_edge_export_elapsed),
        initial_worker_apply_ms: initial_worker_apply.total_ms,
        initial_worker_export_ms: ms(initial_worker_export_elapsed),
        initial_tab_apply_ms: initial_tab_apply.total_ms,
        refresh_core_export_ms: ms(refresh_core_export_elapsed),
        refresh_edge_apply_ms: refresh_edge_apply.total_ms,
        refresh_edge_export_ms: ms(refresh_edge_export_elapsed),
        refresh_worker_apply_ms: refresh_worker_apply.total_ms,
        refresh_worker_export_ms: ms(refresh_worker_export_elapsed),
        refresh_tab_apply_ms: refresh_tab_apply.total_ms,
        subscription_poll_ms: ms(poll_elapsed),
        subscription_added: diff_counts.added,
        subscription_updated: diff_counts.updated,
        subscription_removed: diff_counts.removed,
        tab_visible_rows_after_refresh,
        initial_core_bundle_bytes: initial_core_summary.bytes,
        initial_core_bundle_gzip_bytes,
        refresh_core_bundle_bytes: refresh_core_summary.bytes,
        refresh_core_bundle_gzip_bytes,
        core_database_bytes: core.storage_stats()?.database_bytes,
        edge_database_bytes: edge.storage_stats()?.database_bytes,
        worker_database_bytes: worker.storage_stats()?.database_bytes,
        tab_database_bytes: tab.storage_stats()?.database_bytes,
    })
}

fn run_recursive_closure_layout_probe() -> BenchResult<RecursiveClosureLayoutProbe> {
    let node_count = env_usize("MINI_JAZZ_PERF_RECURSIVE_TREE_NODES", 2_000);
    let branch_factor = env_usize("MINI_JAZZ_PERF_RECURSIVE_TREE_BRANCH_FACTOR", 5).max(1);

    let edge_conn = Connection::open_in_memory()?;
    edge_conn.execute_batch(
        "CREATE TABLE folder_current (
           row_num INTEGER PRIMARY KEY,
           parent_num INTEGER,
           name TEXT NOT NULL
         );
         CREATE INDEX folder_parent_idx ON folder_current(parent_num, row_num);",
    )?;
    let seed_edges_started = Instant::now();
    seed_raw_folder_edges(&edge_conn, node_count, branch_factor)?;
    let seed_edges_elapsed = seed_edges_started.elapsed();
    let edge_only_database_bytes = sqlite_database_bytes(&edge_conn)?;
    let recursive_started = Instant::now();
    let recursive_rows = query_raw_recursive_cte(&edge_conn)?;
    let recursive_elapsed = recursive_started.elapsed();

    let closure_conn = Connection::open_in_memory()?;
    closure_conn.execute_batch(
        "CREATE TABLE folder_current (
           row_num INTEGER PRIMARY KEY,
           parent_num INTEGER,
           name TEXT NOT NULL
         );
         CREATE INDEX folder_parent_idx ON folder_current(parent_num, row_num);
         CREATE TABLE folder_closure (
           ancestor_num INTEGER NOT NULL,
           descendant_num INTEGER NOT NULL,
           depth INTEGER NOT NULL,
           PRIMARY KEY (ancestor_num, descendant_num)
         ) WITHOUT ROWID;
         CREATE INDEX folder_closure_descendant_idx
           ON folder_closure(descendant_num, ancestor_num);",
    )?;
    seed_raw_folder_edges(&closure_conn, node_count, branch_factor)?;
    let seed_closure_started = Instant::now();
    let closure_rows = seed_raw_folder_closure(&closure_conn, node_count, branch_factor)?;
    let seed_closure_elapsed = seed_closure_started.elapsed();
    let closure_database_bytes = sqlite_database_bytes(&closure_conn)?;
    let closure_started = Instant::now();
    let closure_rows_returned = query_raw_closure(&closure_conn)?;
    let closure_elapsed = closure_started.elapsed();

    Ok(RecursiveClosureLayoutProbe {
        node_count,
        branch_factor,
        edge_only_database_bytes,
        closure_database_bytes,
        closure_rows,
        seed_edges_ms: ms(seed_edges_elapsed),
        seed_closure_ms: ms(seed_closure_elapsed),
        recursive_cte_ms: ms(recursive_elapsed),
        closure_query_ms: ms(closure_elapsed),
        recursive_rows,
        closure_rows_returned,
    })
}

fn run_cold_reopen_profile_probe() -> BenchResult<ColdReopenProfileProbe> {
    let total_rows = env_usize("MINI_JAZZ_PERF_COLD_TOTAL_ROWS", 50_000);
    let target_owner_rows = env_usize("MINI_JAZZ_PERF_COLD_TARGET_OWNER_ROWS", 5_000);
    let page_size = env_usize("MINI_JAZZ_PERF_COLD_PAGE_SIZE", 50);
    let dir = tempdir()?;
    let core_path = dir.path().join("cold-core.sqlite");
    let worker_path = dir.path().join("cold-worker.sqlite");
    let schema = documents_schema();

    let seed_elapsed = {
        let mut core = Runtime::open_trusted_with_schema(
            Storage::File(core_path.clone()),
            "core",
            schema.clone(),
        )?;
        let seed_started = Instant::now();
        seed_documents(&mut core, total_rows, target_owner_rows, 100)?;
        seed_started.elapsed()
    };

    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(core_path.clone()),
        "core",
        schema.clone(),
    )?;
    let (bundle, cold_export_profile) = core.run_as_user(OWNER, |core| {
        core.profile_export_query_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!(OWNER),
            "updated_at",
            page_size,
        )
    })?;
    let (_, warm_export_profile) = core.run_as_user(OWNER, |core| {
        core.profile_export_query_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!(OWNER),
            "updated_at",
            page_size,
        )
    })?;
    let bundle_summary = BundleSummary::from(&bundle)?;
    let core_database_bytes = core.storage_stats()?.database_bytes;

    let mut worker = Runtime::open_with_schema(
        Storage::File(worker_path.clone()),
        "worker",
        OWNER,
        schema.clone(),
    )?;
    let cold_worker_apply = worker.profile_apply_bundle(&bundle)?;
    let warm_query_started = Instant::now();
    let warm_rows = read_top_owner_page(&worker, page_size)?;
    let warm_query_elapsed = warm_query_started.elapsed();
    assert_eq!(warm_rows.len(), page_size);
    drop(worker);

    let reopened = Runtime::open_with_schema(Storage::File(worker_path), "worker", OWNER, schema)?;
    let reopened_query_started = Instant::now();
    let reopened_rows = read_top_owner_page(&reopened, page_size)?;
    let reopened_query_elapsed = reopened_query_started.elapsed();
    assert_eq!(reopened_rows.len(), page_size);
    let reopened_worker_observed_reads = reopened.observed_query_reads()?.len();
    let worker_database_bytes = reopened.storage_stats()?.database_bytes;

    Ok(ColdReopenProfileProbe {
        total_rows,
        target_owner_rows,
        page_size,
        seed_ms: ms(seed_elapsed),
        cold_export_total_ms: cold_export_profile.total_ms,
        warm_export_total_ms: warm_export_profile.total_ms,
        cold_export_read_rows_ms: cold_export_profile.read_rows_ms,
        warm_export_read_rows_ms: warm_export_profile.read_rows_ms,
        cold_export_history_rows: bundle.history.len(),
        bundle_bytes: bundle_summary.bytes,
        cold_worker_apply_ms: cold_worker_apply.total_ms,
        cold_worker_apply_history_ms: cold_worker_apply.history_ms,
        warm_worker_query_ms: ms(warm_query_elapsed),
        reopened_worker_query_ms: ms(reopened_query_elapsed),
        reopened_worker_observed_reads,
        core_database_bytes,
        worker_database_bytes,
    })
}

fn run_project_board_probe() -> BenchResult<ProjectBoardProbe> {
    let user_count = 50;
    let project_count = 100;
    let task_count = 20_000;
    let comments_per_task_sample = 2;
    let sampled_users = 10;
    let page_size = 40;
    let dir = tempdir()?;
    let schema = project_board_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema)?;

    let seed_started = Instant::now();
    seed_project_board(
        &mut core,
        user_count,
        project_count,
        task_count,
        comments_per_task_sample,
    )?;
    let seed_elapsed = seed_started.elapsed();

    let users = (0..sampled_users)
        .map(|index| format!("member-{index}"))
        .collect::<Vec<_>>();
    let export_started = Instant::now();
    let merged_bundle = core.run_as_user(OWNER, |core| {
        core.export_many_query_where_eq_top_field_desc_with_ref_include(
            "tasks",
            "assignee",
            users.iter().map(|user| json!(user)).collect(),
            "updated_at",
            page_size,
            "project",
        )
    })?;
    let export_elapsed = export_started.elapsed();
    let merged_summary = BundleSummary::from(&merged_bundle)?;
    let tab_apply_profile = tab.profile_apply_bundle(&merged_bundle)?;
    let query_started = Instant::now();
    let mut visible_rows = 0;
    for user in &users {
        visible_rows += tab
            .read_rows_where_eq_top_field_desc(
                "tasks",
                "assignee",
                json!(user),
                "updated_at",
                page_size,
            )?
            .len();
    }
    let query_elapsed = query_started.elapsed();

    Ok(ProjectBoardProbe {
        user_count,
        project_count,
        task_count,
        comment_count: task_count.min(1_000) * comments_per_task_sample,
        sampled_users,
        page_size,
        seed_ms: ms(seed_elapsed),
        my_tasks_export_ms: ms(export_elapsed),
        merged_bundle_bytes: merged_summary.bytes,
        merged_history_rows: merged_bundle.history.len(),
        merged_transaction_rows: merged_bundle.txs.len(),
        tab_apply_ms: tab_apply_profile.total_ms,
        tab_apply_profile,
        tab_query_ms: ms(query_elapsed),
        visible_rows_returned: visible_rows,
        core_database_bytes: core.storage_stats()?.database_bytes,
        tab_database_bytes: tab.storage_stats()?.database_bytes,
    })
}

fn run_current_projection_tradeoff_probe() -> BenchResult<CurrentProjectionTradeoffProbe> {
    let row_count = 100_000;
    let update_count = 10_000;
    let current_projection = run_current_projection_tradeoff_case(row_count, update_count, true)?;
    let history_only = run_current_projection_tradeoff_case(row_count, update_count, false)?;
    let deep_versions_history_only = run_current_projection_tradeoff_case(20_000, 100_000, false)?;
    Ok(CurrentProjectionTradeoffProbe {
        saved_bytes_without_current: current_projection.database_bytes
            - history_only.database_bytes,
        history_only_query_slowdown: history_only.query_ms / current_projection.query_ms.max(0.001),
        current_projection,
        history_only,
        deep_versions_history_only,
    })
}

fn run_current_projection_tradeoff_case(
    row_count: usize,
    update_count: usize,
    with_current: bool,
) -> BenchResult<CurrentProjectionTradeoffCase> {
    let dir = tempdir()?;
    let path = dir.path().join(if with_current {
        "current.sqlite"
    } else {
        "history-only.sqlite"
    });
    let mut conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "OFF")?;
    conn.pragma_update(None, "synchronous", "OFF")?;
    conn.execute_batch(
        "CREATE TABLE docs_history (
           row_num INTEGER NOT NULL,
           tx_num INTEGER NOT NULL,
           owner_id TEXT NOT NULL,
           updated_at TEXT NOT NULL,
           title TEXT NOT NULL,
           PRIMARY KEY (row_num, tx_num)
         ) WITHOUT ROWID;
         CREATE INDEX docs_history_owner_updated
           ON docs_history(owner_id, updated_at DESC, row_num, tx_num);
         CREATE INDEX docs_history_latest
           ON docs_history(row_num, tx_num DESC);",
    )?;
    if with_current {
        conn.execute_batch(
            "CREATE TABLE docs_current (
               row_num INTEGER PRIMARY KEY,
               tx_num INTEGER NOT NULL,
               owner_id TEXT NOT NULL,
               updated_at TEXT NOT NULL,
               title TEXT NOT NULL
             );
             CREATE INDEX docs_current_owner_updated
               ON docs_current(owner_id, updated_at DESC, row_num);",
        )?;
    }

    let seed_started = Instant::now();
    let tx = conn.transaction()?;
    {
        let mut insert_history = tx.prepare(
            "INSERT INTO docs_history (row_num, tx_num, owner_id, updated_at, title)
             VALUES (?, ?, ?, ?, ?)",
        )?;
        for row_index in 0..row_count {
            insert_history.execute(params![
                row_index as i64 + 1,
                1_i64,
                if row_index < row_count / 10 {
                    OWNER.to_owned()
                } else {
                    format!("user-{}", row_index % 10_000)
                },
                format!("{row_index:020}"),
                format!("Document {row_index}")
            ])?;
        }
        for row_index in 0..update_count {
            let updated_row_index = row_index % row_count;
            insert_history.execute(params![
                updated_row_index as i64 + 1,
                row_index as i64 + 2,
                OWNER,
                format!("{:020}", row_count + row_index),
                format!("Updated document {updated_row_index} version {row_index}")
            ])?;
        }
    }
    if with_current {
        tx.execute(
            "INSERT INTO docs_current (row_num, tx_num, owner_id, updated_at, title)
             SELECT row_num, tx_num, owner_id, updated_at, title
             FROM docs_history h
             WHERE NOT EXISTS (
               SELECT 1 FROM docs_history newer
               WHERE newer.row_num = h.row_num
                 AND newer.tx_num > h.tx_num
             )",
            [],
        )?;
    }
    tx.commit()?;
    let seed_elapsed = seed_started.elapsed();

    let query_started = Instant::now();
    let rows = if with_current {
        let mut stmt = conn.prepare(
            "SELECT row_num
             FROM docs_current
             WHERE owner_id = ?
             ORDER BY updated_at DESC, row_num
             LIMIT 50",
        )?;
        let rows = stmt
            .query_map(params![OWNER], |row| row.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    } else {
        let mut stmt = conn.prepare(
            "SELECT h.row_num
             FROM docs_history h
             WHERE h.owner_id = ?
               AND NOT EXISTS (
                 SELECT 1 FROM docs_history newer
                 WHERE newer.row_num = h.row_num
                   AND newer.tx_num > h.tx_num
               )
             ORDER BY h.updated_at DESC, h.row_num
             LIMIT 50",
        )?;
        let rows = stmt
            .query_map(params![OWNER], |row| row.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    let query_elapsed = query_started.elapsed();

    Ok(CurrentProjectionTradeoffCase {
        row_count,
        update_count,
        database_bytes: sqlite_database_bytes(&conn)?,
        seed_ms: ms(seed_elapsed),
        query_ms: ms(query_elapsed),
        rows_returned: rows.len(),
    })
}

fn run_mixed_mutation_refresh_probe() -> BenchResult<MixedMutationRefreshProbe> {
    let config = Config {
        total_rows: 20_000,
        target_owner_rows: 2_000,
        page_size: 50,
        seed_batch_size: 100,
        refresh_new_top_rows: 0,
        durable_intermediaries: true,
    };
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema.clone())?;

    seed_documents(
        &mut core,
        config.total_rows,
        config.target_owner_rows,
        config.seed_batch_size,
    )?;
    let initial_bundle = export_top_owner_page(&mut core, config.page_size)?;
    tab.apply_bundle(&initial_bundle)?;
    let mut subscription = tab.subscribe_rows_where_eq_top_field_desc(
        "documents",
        "owner_id",
        json!(OWNER),
        "updated_at",
        config.page_size,
    )?;

    let top_inserts = 25;
    let current_page_updates = 10;
    let current_page_deletes = 5;
    let off_page_owner_updates = 100;
    let unrelated_owner_updates = 100;
    let mutation_mix = MixedMutationConfig {
        total_rows: config.total_rows,
        target_owner_rows: config.target_owner_rows,
        top_inserts,
        current_page_updates,
        current_page_deletes,
        off_page_owner_updates,
        unrelated_owner_updates,
    };
    apply_mixed_mutations(&mut core, mutation_mix)?;

    let export_started = Instant::now();
    let bundles = core.run_as_user(OWNER, |core| {
        core.export_query_read_refreshes(&tab.observed_query_reads()?)
    })?;
    let export_elapsed = export_started.elapsed();
    let summary = BundleBatchSummary::from(&bundles)?;
    let apply_elapsed = timed_apply_bundles(&mut tab, bundles)?;

    let query_started = Instant::now();
    let rows = read_top_owner_page(&tab, config.page_size)?;
    let query_elapsed = query_started.elapsed();
    let poll_started = Instant::now();
    let diffs = tab.poll_subscription(&mut subscription)?;
    let poll_elapsed = poll_started.elapsed();
    let diff_counts = DiffCounts::from(&diffs);

    Ok(MixedMutationRefreshProbe {
        total_rows: config.total_rows,
        target_owner_rows: config.target_owner_rows,
        page_size: config.page_size,
        top_inserts,
        current_page_updates,
        current_page_deletes,
        off_page_owner_updates,
        unrelated_owner_updates,
        visible_rows_returned: rows.len(),
        history_rows_synced: summary.history_rows,
        transaction_rows_synced: summary.transaction_rows,
        observed_facts_synced: summary.observed_facts,
        bundle_bytes: summary.bytes,
        export_ms: ms(export_elapsed),
        apply_ms: ms(apply_elapsed),
        tab_query_ms: ms(query_elapsed),
        subscription_poll_ms: ms(poll_elapsed),
        subscription_added: diff_counts.added,
        subscription_updated: diff_counts.updated,
        subscription_removed: diff_counts.removed,
    })
}

fn run_wide_schema_apply_probe() -> BenchResult<WideSchemaApplyProbe> {
    let total_rows = 20_000;
    let target_owner_rows = 2_000;
    let page_size = 50;
    let filler_table_count = 40;
    let dir = tempdir()?;
    let schema = wide_documents_schema(filler_table_count);
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema.clone())?;

    seed_documents(&mut core, total_rows, target_owner_rows, 100)?;
    let bundle = export_top_owner_page(&mut core, page_size)?;
    let summary = BundleSummary::from(&bundle)?;
    let apply_elapsed = timed(|| tab.apply_bundle(&bundle))?;
    let query_started = Instant::now();
    let rows = read_top_owner_page(&tab, page_size)?;
    let query_elapsed = query_started.elapsed();

    Ok(WideSchemaApplyProbe {
        total_tables: filler_table_count + 2,
        synced_tables: 2,
        total_rows,
        target_owner_rows,
        page_size,
        visible_rows_returned: rows.len(),
        history_rows_synced: bundle.history.len(),
        transaction_rows_synced: bundle.txs.len(),
        bundle_bytes: summary.bytes,
        apply_ms: ms(apply_elapsed),
        query_ms: ms(query_elapsed),
        tab_database_bytes: tab.storage_stats()?.database_bytes,
    })
}

fn run_storage_topology_probe() -> BenchResult<StorageTopologyProbe> {
    Ok(StorageTopologyProbe {
        all_memory_intermediaries: run_storage_topology_case(false)?,
        durable_intermediaries: run_storage_topology_case(true)?,
    })
}

fn run_storage_topology_case(durable_intermediaries: bool) -> BenchResult<StorageTopologyCase> {
    let config = Config {
        total_rows: 20_000,
        target_owner_rows: 2_000,
        page_size: 50,
        seed_batch_size: 100,
        refresh_new_top_rows: 0,
        durable_intermediaries,
    };
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut edge = Runtime::open_trusted_with_schema(
        storage_for(&config, &dir, "edge-storage-probe.sqlite"),
        "edge",
        schema.clone(),
    )?;
    let mut worker = Runtime::open_with_schema(
        storage_for(&config, &dir, "worker-storage-probe.sqlite"),
        "worker",
        OWNER,
        schema.clone(),
    )?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema.clone())?;

    seed_documents(
        &mut core,
        config.total_rows,
        config.target_owner_rows,
        config.seed_batch_size,
    )?;

    let core_export_started = Instant::now();
    let core_bundle = export_top_owner_page(&mut core, config.page_size)?;
    let core_export_elapsed = core_export_started.elapsed();
    let bundle_summary = BundleSummary::from(&core_bundle)?;
    let edge_apply_elapsed = timed(|| edge.apply_bundle(&core_bundle))?;
    let edge_export_started = Instant::now();
    let edge_bundle = export_top_owner_page(&mut edge, config.page_size)?;
    let edge_export_elapsed = edge_export_started.elapsed();
    let worker_apply_elapsed = timed(|| worker.apply_bundle(&edge_bundle))?;
    let worker_export_started = Instant::now();
    let worker_bundle = export_top_owner_page(&mut worker, config.page_size)?;
    let worker_export_elapsed = worker_export_started.elapsed();
    let tab_apply_elapsed = timed(|| tab.apply_bundle(&worker_bundle))?;
    let query_started = Instant::now();
    let _rows = read_top_owner_page(&tab, config.page_size)?;
    let query_elapsed = query_started.elapsed();

    Ok(StorageTopologyCase {
        durable_intermediaries,
        total_rows: config.total_rows,
        target_owner_rows: config.target_owner_rows,
        page_size: config.page_size,
        bundle_bytes: bundle_summary.bytes,
        core_export_ms: ms(core_export_elapsed),
        edge_apply_ms: ms(edge_apply_elapsed),
        edge_export_ms: ms(edge_export_elapsed),
        worker_apply_ms: ms(worker_apply_elapsed),
        worker_export_ms: ms(worker_export_elapsed),
        tab_apply_ms: ms(tab_apply_elapsed),
        tab_query_ms: ms(query_elapsed),
        api_to_first_result_ms: ms(core_export_elapsed
            + edge_apply_elapsed
            + edge_export_elapsed
            + worker_apply_elapsed
            + worker_export_elapsed
            + tab_apply_elapsed
            + query_elapsed),
        edge_database_bytes: edge.storage_stats()?.database_bytes,
        worker_database_bytes: worker.storage_stats()?.database_bytes,
    })
}

fn run_multi_query_refresh_probe() -> BenchResult<MultiQueryRefreshProbe> {
    let total_rows = 20_000;
    let target_owner_rows = 2_000;
    let page_size = 20;
    let inserted_per_query = 10;
    let owners = ["alice", "user-2000", "user-2001", "user-2002"];
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut separate_tab =
        Runtime::open_with_schema(Storage::Memory, "separate-tab", OWNER, schema.clone())?;
    let mut merged_tab =
        Runtime::open_with_schema(Storage::Memory, "merged-tab", OWNER, schema.clone())?;

    seed_documents(&mut core, total_rows, target_owner_rows, 100)?;
    for owner in owners {
        let bundle = export_top_owner_page_for(&mut core, OWNER, owner, page_size)?;
        separate_tab.apply_bundle(&bundle)?;
        merged_tab.apply_bundle(&bundle)?;
    }
    insert_new_top_documents_for_owners(
        &mut core,
        total_rows,
        target_owner_rows,
        &owners,
        inserted_per_query,
    )?;

    let refresh_export_started = Instant::now();
    let refresh_bundles = core.run_as_user(OWNER, |core| {
        core.export_query_read_refreshes(&separate_tab.observed_query_reads()?)
    })?;
    let refresh_export_ms = ms(refresh_export_started.elapsed());
    let separate_summary = BundleBatchSummary::from(&refresh_bundles)?;
    let merged_bundle = merge_bundles(&refresh_bundles)?;
    let merged_summary = BundleSummary::from(&merged_bundle)?;

    let separate_apply_elapsed = timed_apply_bundles(&mut separate_tab, refresh_bundles.clone())?;
    let merged_apply_elapsed = timed(|| merged_tab.apply_bundle(&merged_bundle))?;

    Ok(MultiQueryRefreshProbe {
        query_count: owners.len(),
        total_rows,
        target_owner_rows,
        page_size,
        inserted_per_query,
        refresh_bundle_count: refresh_bundles.len(),
        refresh_bundle_bytes: separate_summary.bytes,
        equivalent_merged_bundle_bytes: merged_summary.bytes,
        refresh_export_ms,
        refresh_apply_ms: ms(separate_apply_elapsed),
        equivalent_merged_apply_ms: ms(merged_apply_elapsed),
        refresh_history_rows: separate_summary.history_rows,
        equivalent_merged_history_rows: merged_bundle.history.len(),
        refresh_transaction_rows: separate_summary.transaction_rows,
        equivalent_merged_transaction_rows: merged_bundle.txs.len(),
        refresh_observed_facts: merged_bundle.query_reads.len(),
    })
}

fn run_subscription_storm_probe() -> BenchResult<SubscriptionStormProbe> {
    let owner_count = 50;
    let rows_per_owner = 200;
    let total_rows = owner_count * rows_per_owner;
    let subscription_count = 20;
    let page_size = 10;
    let inserted_per_subscription = 5;
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema.clone())?;

    seed_shared_readable_owner_documents(&mut core, owner_count, rows_per_owner, 100)?;
    let owners = (0..subscription_count)
        .map(|index| format!("user-{index}"))
        .collect::<Vec<_>>();
    let mut subscriptions = Vec::new();
    for owner in &owners {
        let bundle = export_top_owner_page_for(&mut core, OWNER, owner, page_size)?;
        tab.apply_bundle(&bundle)?;
        subscriptions.push(tab.subscribe_rows_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!(owner),
            "updated_at",
            page_size,
        )?);
    }
    insert_new_top_documents_for_shared_readable_owners(
        &mut core,
        total_rows,
        &owners,
        inserted_per_subscription,
    )?;
    let refresh_bundles = core.run_as_user(OWNER, |core| {
        core.export_query_read_refreshes(&tab.observed_query_reads()?)
    })?;
    let merged_bundle = merge_bundles(&refresh_bundles)?;
    let merged_summary = BundleSummary::from(&merged_bundle)?;
    let apply_elapsed = timed(|| tab.apply_bundle(&merged_bundle))?;

    let poll_started = Instant::now();
    let mut total_counts = DiffCounts {
        added: 0,
        updated: 0,
        moved: 0,
        removed: 0,
    };
    for subscription in &mut subscriptions {
        let counts = DiffCounts::from(&tab.poll_subscription(subscription)?);
        total_counts.added += counts.added;
        total_counts.updated += counts.updated;
        total_counts.moved += counts.moved;
        total_counts.removed += counts.removed;
    }
    let poll_elapsed = poll_started.elapsed();

    Ok(SubscriptionStormProbe {
        subscription_count,
        total_rows,
        page_size,
        inserted_per_subscription,
        merged_bundle_bytes: merged_summary.bytes,
        apply_ms: ms(apply_elapsed),
        total_poll_ms: ms(poll_elapsed),
        average_poll_ms: ms(poll_elapsed) / subscription_count as f64,
        total_added: total_counts.added,
        total_updated: total_counts.updated,
        total_removed: total_counts.removed,
    })
}

fn run_apply_profile_probe() -> BenchResult<ApplyProfileProbe> {
    let owner_count = 50;
    let rows_per_owner = 200;
    let total_rows = owner_count * rows_per_owner;
    let subscription_count = 20;
    let page_size = 10;
    let inserted_per_subscription = 5;
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema.clone(),
    )?;
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema.clone())?;

    seed_shared_readable_owner_documents(&mut core, owner_count, rows_per_owner, 100)?;
    let owners = (0..subscription_count)
        .map(|index| format!("user-{index}"))
        .collect::<Vec<_>>();
    for owner in &owners {
        let bundle = export_top_owner_page_for(&mut core, OWNER, owner, page_size)?;
        tab.apply_bundle(&bundle)?;
        tab.subscribe_rows_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!(owner),
            "updated_at",
            page_size,
        )?;
    }
    insert_new_top_documents_for_shared_readable_owners(
        &mut core,
        total_rows,
        &owners,
        inserted_per_subscription,
    )?;
    let refresh_bundles = core.run_as_user(OWNER, |core| {
        core.export_query_read_refreshes(&tab.observed_query_reads()?)
    })?;
    let merged_bundle = merge_bundles(&refresh_bundles)?;
    let merged_summary = BundleSummary::from(&merged_bundle)?;
    let profile = tab.profile_apply_bundle(&merged_bundle)?;

    Ok(ApplyProfileProbe {
        subscription_count,
        total_rows,
        page_size,
        inserted_per_subscription,
        bundle_bytes: merged_summary.bytes,
        profile,
    })
}

fn run_branch_overlay_probe() -> BenchResult<BranchOverlayProbe> {
    let total_rows = 20_000;
    let target_owner_rows = 2_000;
    let page_size = 50;
    let branch_overlay_updates = 100;
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut runtime = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema,
    )?;

    seed_documents(&mut runtime, total_rows, target_owner_rows, 100)?;
    let main_query_started = Instant::now();
    let main_rows = read_top_owner_page(&runtime, page_size)?;
    let main_query_elapsed = main_query_started.elapsed();

    runtime.create_branch_from_branches("draft", &["main"])?;
    runtime.checkout_branch("draft")?;
    let mut tx = runtime.transaction();
    for index in 0..branch_overlay_updates {
        let row_index = target_owner_rows - 1 - index;
        tx = tx.update_row(
            "documents",
            &format!("doc-{row_index}"),
            BTreeMap::from([
                ("title".to_owned(), json!(format!("Draft update {index}"))),
                (
                    "updated_at".to_owned(),
                    json!(format!("{:020}", total_rows + index)),
                ),
            ]),
        );
    }
    tx.commit()?;

    let branch_query_started = Instant::now();
    let branch_rows = read_top_owner_page(&runtime, page_size)?;
    let branch_query_elapsed = branch_query_started.elapsed();
    let branch_export_started = Instant::now();
    let (branch_bundle, export_profile) = runtime.profile_export_query_where_eq_top_field_desc(
        "documents",
        "owner_id",
        json!(OWNER),
        "updated_at",
        page_size,
    )?;
    let branch_export_elapsed = branch_export_started.elapsed();
    let branch_summary = BundleSummary::from(&branch_bundle)?;

    Ok(BranchOverlayProbe {
        total_rows,
        target_owner_rows,
        page_size,
        branch_overlay_updates,
        main_visible_rows: main_rows.len(),
        branch_visible_rows: branch_rows.len(),
        main_query_ms: ms(main_query_elapsed),
        branch_query_ms: ms(branch_query_elapsed),
        branch_export_ms: ms(branch_export_elapsed),
        branch_bundle_bytes: branch_summary.bytes,
        branch_history_rows: branch_bundle.history.len(),
        branch_transaction_rows: branch_bundle.txs.len(),
        export_profile,
    })
}

fn run_pinned_branch_snapshot_probe() -> BenchResult<PinnedBranchSnapshotProbe> {
    let total_rows = 10_000;
    let target_owner_rows = 1_000;
    let page_size = 50;
    let post_base_updates = 100;
    let overlay_updates = 50;
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut runtime = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema,
    )?;

    seed_orgs(&mut runtime)?;
    let mut base_tx = runtime.transaction().exclusive_at_global(1);
    for row_index in 0..total_rows {
        base_tx = base_tx.insert_row(
            "documents",
            &format!("doc-{row_index}"),
            document_values(row_index, target_owner_rows),
        );
    }
    base_tx.commit()?;
    runtime.create_branch("pinned", Some(1))?;
    for index in 0..post_base_updates {
        runtime
            .transaction()
            .insert_row(
                "documents",
                &format!("doc-post-base-{index}"),
                BTreeMap::from([
                    ("owner_id".to_owned(), json!(OWNER)),
                    ("org".to_owned(), json!("org-0")),
                    (
                        "title".to_owned(),
                        json!(format!("Post base insert {index}")),
                    ),
                    (
                        "updated_at".to_owned(),
                        json!(format!("{:020}", total_rows + index)),
                    ),
                ]),
            )
            .commit()?;
    }

    runtime.checkout_branch("pinned")?;
    let mut tx = runtime.transaction();
    for index in 0..overlay_updates {
        let row_index = target_owner_rows - 1 - index;
        tx = tx.update_row(
            "documents",
            &format!("doc-{row_index}"),
            BTreeMap::from([
                ("title".to_owned(), json!(format!("Pinned draft {index}"))),
                (
                    "updated_at".to_owned(),
                    json!(format!("{:020}", total_rows + post_base_updates + index)),
                ),
            ]),
        );
    }
    tx.commit()?;

    let branch_query_started = Instant::now();
    let branch_rows = read_top_owner_page(&runtime, page_size)?;
    let branch_query_elapsed = branch_query_started.elapsed();
    let branch_export_started = Instant::now();
    let (branch_bundle, export_profile) = runtime.profile_export_query_where_eq_top_field_desc(
        "documents",
        "owner_id",
        json!(OWNER),
        "updated_at",
        page_size,
    )?;
    let branch_export_elapsed = branch_export_started.elapsed();
    let branch_summary = BundleSummary::from(&branch_bundle)?;

    Ok(PinnedBranchSnapshotProbe {
        total_rows,
        target_owner_rows,
        page_size,
        post_base_updates,
        overlay_updates,
        branch_visible_rows: branch_rows.len(),
        branch_query_ms: ms(branch_query_elapsed),
        branch_export_ms: ms(branch_export_elapsed),
        branch_bundle_bytes: branch_summary.bytes,
        branch_history_rows: branch_bundle.history.len(),
        branch_transaction_rows: branch_bundle.txs.len(),
        export_profile,
    })
}

fn run_branch_fan_in_probe() -> BenchResult<BranchFanInProbe> {
    let total_rows = 5_000;
    let target_owner_rows = 500;
    let branch_count = 100;
    let source_count = 20;
    let page_size = 50;
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut runtime = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema,
    )?;

    seed_documents(&mut runtime, total_rows, target_owner_rows, 100)?;
    let source_started = Instant::now();
    for branch_index in 0..branch_count {
        let branch_id = format!("source-{branch_index}");
        runtime.create_branch_from_branches(&branch_id, &["main"])?;
        runtime.checkout_branch(&branch_id)?;
        runtime.update_row(
            "documents",
            &format!("doc-{}", branch_index % target_owner_rows),
            BTreeMap::from([
                (
                    "title".to_owned(),
                    json!(format!("Source branch update {branch_index}")),
                ),
                (
                    "updated_at".to_owned(),
                    json!(format!("{:020}", total_rows + branch_index)),
                ),
            ]),
        )?;
    }
    let source_elapsed = source_started.elapsed();
    let source_ids = (0..source_count)
        .map(|index| format!("source-{index}"))
        .collect::<Vec<_>>();
    let source_refs = source_ids.iter().map(String::as_str).collect::<Vec<_>>();
    let fan_in_started = Instant::now();
    runtime.create_branch_from_branches("fan-in", &source_refs)?;
    runtime.checkout_branch("fan-in")?;
    let fan_in_elapsed = fan_in_started.elapsed();

    let query_started = Instant::now();
    let rows = read_top_owner_page(&runtime, page_size)?;
    let query_elapsed = query_started.elapsed();
    let export_started = Instant::now();
    let bundle = export_top_owner_page(&mut runtime, page_size)?;
    let export_elapsed = export_started.elapsed();
    let summary = BundleSummary::from(&bundle)?;

    Ok(BranchFanInProbe {
        total_rows,
        branch_count,
        source_count,
        page_size,
        create_source_branches_ms: ms(source_elapsed),
        create_fan_in_branch_ms: ms(fan_in_elapsed),
        branch_query_ms: ms(query_elapsed),
        branch_export_ms: ms(export_elapsed),
        visible_rows_returned: rows.len(),
        bundle_bytes: summary.bytes,
        history_rows: bundle.history.len(),
        transaction_rows: bundle.txs.len(),
        core_database_bytes: runtime.storage_stats()?.database_bytes,
    })
}

fn run_export_profile_probe() -> BenchResult<ExportProfileProbe> {
    let total_rows = 100_000;
    let target_owner_rows = 10_000;
    let page_size = 50;
    let dir = tempdir()?;
    let schema = documents_schema();
    let mut core = Runtime::open_trusted_with_schema(
        Storage::File(dir.path().join("core.sqlite")),
        "core",
        schema,
    )?;
    seed_documents(&mut core, total_rows, target_owner_rows, 100)?;
    let (bundle, profile) = core.run_as_user(OWNER, |core| {
        core.profile_export_query_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!(OWNER),
            "updated_at",
            page_size,
        )
    })?;
    let summary = BundleSummary::from(&bundle)?;
    Ok(ExportProfileProbe {
        total_rows,
        target_owner_rows,
        page_size,
        bundle_bytes: summary.bytes,
        profile,
    })
}

fn run_edge_warm_worker_cold(
    config: &Config,
    dir: &tempfile::TempDir,
    schema: &SchemaDef,
    edge: &mut Runtime,
) -> BenchResult<WarmBootReport> {
    let mut worker = Runtime::open_with_schema(
        storage_for(config, dir, "edge-warm-worker.sqlite"),
        "edge-warm-worker",
        OWNER,
        schema.clone(),
    )?;
    let mut tab =
        Runtime::open_with_schema(Storage::Memory, "edge-warm-tab", OWNER, schema.clone())?;

    let export_started = Instant::now();
    let bundle = export_top_owner_page(edge, config.page_size)?;
    let export_elapsed = export_started.elapsed();
    let summary = BundleSummary::from(&bundle)?;
    let worker_apply_elapsed = timed(|| worker.apply_bundle(&bundle))?;
    let worker_bundle = export_top_owner_page(&mut worker, config.page_size)?;
    let tab_apply_elapsed = timed(|| tab.apply_bundle(&worker_bundle))?;
    let query_started = Instant::now();
    let rows = read_top_owner_page(&tab, config.page_size)?;
    let query_elapsed = query_started.elapsed();

    Ok(WarmBootReport {
        visible_rows_returned: rows.len(),
        history_rows_synced: bundle.history.len(),
        transaction_rows_synced: bundle.txs.len(),
        observed_facts_synced: bundle.query_reads.len(),
        bundle_bytes: summary.bytes,
        export_ms: ms(export_elapsed),
        first_apply_ms: ms(worker_apply_elapsed),
        second_apply_ms: Some(ms(tab_apply_elapsed)),
        query_ms: ms(query_elapsed),
        api_to_first_result_ms: ms(export_elapsed
            + worker_apply_elapsed
            + tab_apply_elapsed
            + query_elapsed),
    })
}

fn run_worker_warm_tab_cold(config: &Config, worker: &mut Runtime) -> BenchResult<WarmBootReport> {
    let mut tab = Runtime::open_with_schema(
        Storage::Memory,
        "worker-warm-tab",
        OWNER,
        documents_schema(),
    )?;

    let export_started = Instant::now();
    let bundle = export_top_owner_page(worker, config.page_size)?;
    let export_elapsed = export_started.elapsed();
    let summary = BundleSummary::from(&bundle)?;
    let tab_apply_elapsed = timed(|| tab.apply_bundle(&bundle))?;
    let query_started = Instant::now();
    let rows = read_top_owner_page(&tab, config.page_size)?;
    let query_elapsed = query_started.elapsed();

    Ok(WarmBootReport {
        visible_rows_returned: rows.len(),
        history_rows_synced: bundle.history.len(),
        transaction_rows_synced: bundle.txs.len(),
        observed_facts_synced: bundle.query_reads.len(),
        bundle_bytes: summary.bytes,
        export_ms: ms(export_elapsed),
        first_apply_ms: ms(tab_apply_elapsed),
        second_apply_ms: None,
        query_ms: ms(query_elapsed),
        api_to_first_result_ms: ms(export_elapsed + tab_apply_elapsed + query_elapsed),
    })
}

fn run_refresh_after_new_top_rows(
    config: &Config,
    core: &mut Runtime,
    edge: &mut Runtime,
    worker: &mut Runtime,
    tab: &mut Runtime,
    tab_subscription: &mut RowsSubscription,
) -> BenchResult<RefreshReport> {
    insert_new_top_documents(
        core,
        config.total_rows,
        config.target_owner_rows,
        config.refresh_new_top_rows,
    )?;

    let export_started = Instant::now();
    let edge_reads = edge.observed_query_reads()?;
    let core_bundles =
        core.run_as_user(OWNER, |core| core.export_query_read_refreshes(&edge_reads))?;
    let export_elapsed = export_started.elapsed();
    let core_summary = BundleBatchSummary::from(&core_bundles)?;

    let edge_apply_elapsed = timed_apply_bundles(edge, core_bundles)?;
    let worker_reads = worker.observed_query_reads()?;
    let edge_export_started = Instant::now();
    let edge_bundles = edge.run_as_user(OWNER, |edge| {
        edge.export_query_read_refreshes(&worker_reads)
    })?;
    let edge_export_elapsed = edge_export_started.elapsed();
    let worker_apply_elapsed = timed_apply_bundles(worker, edge_bundles)?;
    let worker_export_started = Instant::now();
    let worker_bundles = worker.export_query_read_refreshes(&tab.observed_query_reads()?)?;
    let worker_export_elapsed = worker_export_started.elapsed();
    let tab_apply_elapsed = timed_apply_bundles(tab, worker_bundles)?;

    let query_started = Instant::now();
    let rows = tab.read_rows_where_eq_top_field_desc(
        "documents",
        "owner_id",
        json!(OWNER),
        "updated_at",
        config.page_size,
    )?;
    let tab_query_elapsed = query_started.elapsed();
    let subscription_poll_started = Instant::now();
    let diffs = tab.poll_subscription(tab_subscription)?;
    let subscription_poll_elapsed = subscription_poll_started.elapsed();
    let diff_counts = DiffCounts::from(&diffs);

    Ok(RefreshReport {
        inserted_rows: config.refresh_new_top_rows,
        visible_rows_returned: rows.len(),
        history_rows_synced: core_summary.history_rows,
        transaction_rows_synced: core_summary.transaction_rows,
        observed_facts_synced: core_summary.observed_facts,
        bundle_bytes: core_summary.bytes,
        export_ms: ms(export_elapsed),
        core_to_edge_apply_ms: ms(edge_apply_elapsed),
        edge_export_ms: ms(edge_export_elapsed),
        edge_to_worker_apply_ms: ms(worker_apply_elapsed),
        worker_export_ms: ms(worker_export_elapsed),
        worker_to_tab_apply_ms: ms(tab_apply_elapsed),
        tab_query_ms: ms(tab_query_elapsed),
        tab_subscription_poll_ms: ms(subscription_poll_elapsed),
        tab_subscription_added: diff_counts.added,
        tab_subscription_updated: diff_counts.updated,
        tab_subscription_moved: diff_counts.moved,
        tab_subscription_removed: diff_counts.removed,
        api_to_updated_result_ms: ms(export_elapsed
            + edge_apply_elapsed
            + edge_export_elapsed
            + worker_apply_elapsed
            + worker_export_elapsed
            + tab_apply_elapsed
            + tab_query_elapsed
            + subscription_poll_elapsed),
    })
}

fn documents_schema() -> SchemaDef {
    SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_user();
        })
        .table("documents", |table| {
            table.text("owner_id");
            table.ref_("org", "orgs");
            table.text("updated_at");
            table.text("title");
            table.index("owner_updated", ["owner_id", "updated_at"]);
            table.read_if_ref_readable("org");
        })
}

fn wide_documents_schema(filler_table_count: usize) -> SchemaDef {
    let mut schema = documents_schema();
    for table_index in 0..filler_table_count {
        schema = schema.table(&format!("filler_{table_index}"), |table| {
            table.text("owner_id");
            table.text("status");
            table.text("updated_at");
            table.index("owner_updated", ["owner_id", "updated_at"]);
            table.read_if_created_by_user();
        });
    }
    schema
}

fn recursive_policy_schema() -> SchemaDef {
    SchemaDef::new()
        .table("teams", |table| {
            table.text("name");
            table.read_if_created_by_user();
        })
        .table("projects", |table| {
            table.text("name");
            table.ref_("team", "teams");
            table.read_if_ref_readable("team");
        })
        .table("documents", |table| {
            table.text("owner_id");
            table.ref_("project", "projects");
            table.text("updated_at");
            table.text("title");
            table.index("owner_updated", ["owner_id", "updated_at"]);
            table.read_if_ref_readable("project");
        })
}

fn folder_tree_schema() -> SchemaDef {
    SchemaDef::new().table("folders", |table| {
        table.text("name");
        table.optional_ref("parent", "folders");
        table.index("parent_name", ["parent", "name"]);
        table.read_if_created_by_user();
    })
}

fn project_board_schema() -> SchemaDef {
    SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_user();
        })
        .table("members", |table| {
            table.text("name");
            table.read_if_created_by_user();
        })
        .table("projects", |table| {
            table.text("name");
            table.ref_("org", "orgs");
            table.read_if_ref_readable("org");
        })
        .table("tasks", |table| {
            table.text("title");
            table.text("status");
            table.text("updated_at");
            table.ref_("project", "projects");
            table.ref_("assignee", "members");
            table.index("assignee_updated", ["assignee", "updated_at"]);
            table.index("project_status", ["project", "status"]);
            table.read_if_ref_readable("project");
        })
        .table("comments", |table| {
            table.text("body");
            table.text("created_at");
            table.ref_("task", "tasks");
            table.read_if_ref_readable("task");
        })
}

struct DiffCounts {
    added: usize,
    updated: usize,
    moved: usize,
    removed: usize,
}

impl DiffCounts {
    fn from(diffs: &[RowDiff]) -> Self {
        let mut counts = Self {
            added: 0,
            updated: 0,
            moved: 0,
            removed: 0,
        };
        for diff in diffs {
            match diff {
                RowDiff::Added(_) => counts.added += 1,
                RowDiff::Updated { .. } => counts.updated += 1,
                RowDiff::Moved { .. } => counts.moved += 1,
                RowDiff::Removed(_) => counts.removed += 1,
            }
        }
        counts
    }
}

fn seed_documents(
    runtime: &mut Runtime,
    total_rows: usize,
    target_owner_rows: usize,
    seed_batch_size: usize,
) -> Result<()> {
    seed_orgs(runtime)?;
    let seed_batch_size = seed_batch_size.max(1);
    for chunk_start in (0..total_rows).step_by(seed_batch_size) {
        let chunk_end = (chunk_start + seed_batch_size).min(total_rows);
        let mut tx = runtime.transaction();
        for row_index in chunk_start..chunk_end {
            tx = tx.insert_row(
                "documents",
                &format!("doc-{row_index}"),
                document_values(row_index, target_owner_rows),
            );
        }
        tx.commit()?;
    }
    Ok(())
}

fn seed_orgs(runtime: &mut Runtime) -> Result<()> {
    runtime.run_attributing_to_user(OWNER, |runtime| {
        let mut tx = runtime.transaction();
        for org_index in 0..100 {
            tx = tx.insert_row(
                "orgs",
                &format!("org-{org_index}"),
                BTreeMap::from([(
                    "name".to_owned(),
                    json!(format!("Organization {org_index}")),
                )]),
            );
        }
        tx.commit().map(|_| ())
    })
}

fn seed_recursive_policy_graph(
    runtime: &mut Runtime,
    total_rows: usize,
    target_owner_rows: usize,
    seed_batch_size: usize,
) -> Result<()> {
    runtime.run_attributing_to_user(OWNER, |runtime| {
        let mut tx = runtime.transaction();
        for team_index in 0..10 {
            tx = tx.insert_row(
                "teams",
                &format!("team-{team_index}"),
                BTreeMap::from([("name".to_owned(), json!(format!("Team {team_index}")))]),
            );
        }
        for project_index in 0..100 {
            tx = tx.insert_row(
                "projects",
                &format!("project-{project_index}"),
                BTreeMap::from([
                    ("name".to_owned(), json!(format!("Project {project_index}"))),
                    (
                        "team".to_owned(),
                        json!(format!("team-{}", project_index % 10)),
                    ),
                ]),
            );
        }
        tx.commit().map(|_| ())
    })?;

    let seed_batch_size = seed_batch_size.max(1);
    for chunk_start in (0..total_rows).step_by(seed_batch_size) {
        let chunk_end = (chunk_start + seed_batch_size).min(total_rows);
        let mut tx = runtime.transaction();
        for row_index in chunk_start..chunk_end {
            tx = tx.insert_row(
                "documents",
                &format!("recursive-doc-{row_index}"),
                recursive_document_values(row_index, target_owner_rows),
            );
        }
        tx.commit()?;
    }
    Ok(())
}

fn seed_project_board(
    runtime: &mut Runtime,
    user_count: usize,
    project_count: usize,
    task_count: usize,
    comments_per_task_sample: usize,
) -> Result<()> {
    runtime.run_attributing_to_user(OWNER, |runtime| {
        let mut tx = runtime.transaction();
        tx = tx.insert_row(
            "orgs",
            "org-main",
            BTreeMap::from([("name".to_owned(), json!("Main organization"))]),
        );
        for user_index in 0..user_count {
            tx = tx.insert_row(
                "members",
                &format!("member-{user_index}"),
                BTreeMap::from([("name".to_owned(), json!(format!("Member {user_index}")))]),
            );
        }
        for project_index in 0..project_count {
            tx = tx.insert_row(
                "projects",
                &format!("project-{project_index}"),
                BTreeMap::from([
                    ("name".to_owned(), json!(format!("Project {project_index}"))),
                    ("org".to_owned(), json!("org-main")),
                ]),
            );
        }
        tx.commit().map(|_| ())
    })?;

    for chunk_start in (0..task_count).step_by(100) {
        let chunk_end = (chunk_start + 100).min(task_count);
        let mut tx = runtime.transaction();
        for task_index in chunk_start..chunk_end {
            tx = tx.insert_row(
                "tasks",
                &format!("task-{task_index}"),
                BTreeMap::from([
                    ("title".to_owned(), json!(format!("Task {task_index}"))),
                    (
                        "status".to_owned(),
                        json!(if task_index % 3 == 0 { "done" } else { "open" }),
                    ),
                    (
                        "updated_at".to_owned(),
                        json!(format!("{:020}", task_index)),
                    ),
                    (
                        "project".to_owned(),
                        json!(format!("project-{}", task_index % project_count)),
                    ),
                    (
                        "assignee".to_owned(),
                        json!(format!("member-{}", task_index % user_count)),
                    ),
                ]),
            );
        }
        tx.commit()?;
    }

    let comment_task_count = task_count.min(1_000);
    for chunk_start in (0..comment_task_count).step_by(100) {
        let chunk_end = (chunk_start + 100).min(comment_task_count);
        let mut tx = runtime.transaction();
        for task_index in chunk_start..chunk_end {
            for comment_index in 0..comments_per_task_sample {
                tx = tx.insert_row(
                    "comments",
                    &format!("comment-{task_index}-{comment_index}"),
                    BTreeMap::from([
                        (
                            "body".to_owned(),
                            json!(format!("Comment {comment_index} on task {task_index}")),
                        ),
                        (
                            "created_at".to_owned(),
                            json!(format!("{task_index:020}-{comment_index:02}")),
                        ),
                        ("task".to_owned(), json!(format!("task-{task_index}"))),
                    ]),
                );
            }
        }
        tx.commit()?;
    }
    Ok(())
}

fn seed_many_user_documents(
    runtime: &mut Runtime,
    user_count: usize,
    rows_per_user: usize,
    seed_batch_size: usize,
) -> Result<()> {
    seed_many_user_documents_with_id_shape(
        runtime,
        user_count,
        rows_per_user,
        seed_batch_size,
        false,
    )
}

fn seed_many_user_documents_with_id_shape(
    runtime: &mut Runtime,
    user_count: usize,
    rows_per_user: usize,
    seed_batch_size: usize,
    long_user_ids: bool,
) -> Result<()> {
    for user_index in 0..user_count {
        let user = synthetic_user_id(user_index, long_user_ids);
        runtime.run_attributing_to_user(&user, |runtime| {
            runtime
                .transaction()
                .insert_row(
                    "orgs",
                    &format!("org-{user_index}"),
                    BTreeMap::from([(
                        "name".to_owned(),
                        json!(format!("Organization {user_index}")),
                    )]),
                )
                .commit()
                .map(|_| ())
        })?;
    }

    let total_rows = user_count * rows_per_user;
    let seed_batch_size = seed_batch_size.max(1);
    for chunk_start in (0..total_rows).step_by(seed_batch_size) {
        let chunk_end = (chunk_start + seed_batch_size).min(total_rows);
        let mut tx = runtime.transaction();
        for row_index in chunk_start..chunk_end {
            let user_index = row_index / rows_per_user;
            let owner_id = synthetic_user_id(user_index, long_user_ids);
            tx = tx.insert_row(
                "documents",
                &format!("many-user-doc-{row_index}"),
                BTreeMap::from([
                    ("owner_id".to_owned(), json!(owner_id)),
                    ("org".to_owned(), json!(format!("org-{user_index}"))),
                    ("updated_at".to_owned(), json!(format!("{:020}", row_index))),
                    (
                        "title".to_owned(),
                        json!(format!("Many-user document {row_index}")),
                    ),
                ]),
            );
        }
        tx.commit()?;
    }
    Ok(())
}

fn synthetic_user_id(user_index: usize, long_user_ids: bool) -> String {
    if long_user_ids {
        format!("acct_01JAZZSQLITEPERF_{user_index:08}_tenant_01JAZZSQLITEPERF_LONG_STABLE_USER_ID")
    } else {
        format!("user-{user_index}")
    }
}

fn seed_shared_readable_owner_documents(
    runtime: &mut Runtime,
    owner_count: usize,
    rows_per_owner: usize,
    seed_batch_size: usize,
) -> Result<()> {
    seed_orgs(runtime)?;
    let total_rows = owner_count * rows_per_owner;
    let seed_batch_size = seed_batch_size.max(1);
    for chunk_start in (0..total_rows).step_by(seed_batch_size) {
        let chunk_end = (chunk_start + seed_batch_size).min(total_rows);
        let mut tx = runtime.transaction();
        for row_index in chunk_start..chunk_end {
            let owner_index = row_index / rows_per_owner;
            tx = tx.insert_row(
                "documents",
                &format!("shared-readable-doc-{row_index}"),
                BTreeMap::from([
                    ("owner_id".to_owned(), json!(format!("user-{owner_index}"))),
                    ("org".to_owned(), json!(format!("org-{}", row_index % 100))),
                    ("updated_at".to_owned(), json!(format!("{:020}", row_index))),
                    (
                        "title".to_owned(),
                        json!(format!("Shared readable document {row_index}")),
                    ),
                ]),
            );
        }
        tx.commit()?;
    }
    Ok(())
}

fn export_top_owner_page(runtime: &mut Runtime, page_size: usize) -> Result<Bundle> {
    export_top_owner_page_for(runtime, OWNER, OWNER, page_size)
}

fn export_top_owner_page_for(
    runtime: &mut Runtime,
    user: &str,
    owner_id: &str,
    page_size: usize,
) -> Result<Bundle> {
    if runtime.is_trusted() {
        runtime.run_as_user(user, |runtime| {
            runtime.export_query_where_eq_top_field_desc(
                "documents",
                "owner_id",
                json!(owner_id),
                "updated_at",
                page_size,
            )
        })
    } else {
        runtime.export_query_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!(owner_id),
            "updated_at",
            page_size,
        )
    }
}

fn read_top_owner_page(
    runtime: &Runtime,
    page_size: usize,
) -> Result<Vec<mini_jazz_sqlite::RowView>> {
    runtime.read_rows_where_eq_top_field_desc(
        "documents",
        "owner_id",
        json!(OWNER),
        "updated_at",
        page_size,
    )
}

fn insert_new_top_documents(
    runtime: &mut Runtime,
    total_rows: usize,
    target_owner_rows: usize,
    count: usize,
) -> Result<()> {
    let mut tx = runtime.transaction();
    for index in 0..count {
        let row_index = total_rows + index;
        let mut values = document_values(row_index, target_owner_rows);
        values.insert("owner_id".to_owned(), json!(OWNER));
        tx = tx.insert_row("documents", &format!("doc-refresh-new-{index}"), values);
    }
    tx.commit()?;
    Ok(())
}

fn insert_new_top_documents_for_owners(
    runtime: &mut Runtime,
    total_rows: usize,
    target_owner_rows: usize,
    owners: &[&str],
    count_per_owner: usize,
) -> Result<()> {
    let mut tx = runtime.transaction();
    for (owner_index, owner) in owners.iter().enumerate() {
        for index in 0..count_per_owner {
            let row_index = total_rows + owner_index * count_per_owner + index;
            let mut values = document_values(row_index, target_owner_rows);
            values.insert("owner_id".to_owned(), json!(owner));
            values.insert("org".to_owned(), json!(format!("org-{}", row_index % 100)));
            tx = tx.insert_row(
                "documents",
                &format!("doc-multi-query-new-{owner_index}-{index}"),
                values,
            );
        }
    }
    tx.commit()?;
    Ok(())
}

fn insert_new_top_documents_for_shared_readable_owners(
    runtime: &mut Runtime,
    total_rows: usize,
    owners: &[String],
    count_per_owner: usize,
) -> Result<()> {
    let mut tx = runtime.transaction();
    for (owner_index, owner) in owners.iter().enumerate() {
        for index in 0..count_per_owner {
            let row_index = total_rows + owner_index * count_per_owner + index;
            tx = tx.insert_row(
                "documents",
                &format!("doc-shared-readable-new-{owner_index}-{index}"),
                BTreeMap::from([
                    ("owner_id".to_owned(), json!(owner)),
                    ("org".to_owned(), json!(format!("org-{}", row_index % 100))),
                    ("updated_at".to_owned(), json!(format!("{:020}", row_index))),
                    (
                        "title".to_owned(),
                        json!(format!(
                            "Shared readable new document {owner_index}-{index}"
                        )),
                    ),
                ]),
            );
        }
    }
    tx.commit()?;
    Ok(())
}

fn dashboard_owner_filters(query_count: usize) -> Vec<String> {
    let mut owners = Vec::with_capacity(query_count);
    owners.push(OWNER.to_owned());
    for index in 1..query_count {
        owners.push(format!("user-{}", 5_000 + index));
    }
    owners
}

fn insert_new_top_recursive_documents_for_owners(
    runtime: &mut Runtime,
    total_rows: usize,
    owners: &[String],
    count_per_owner: usize,
) -> Result<()> {
    let mut tx = runtime.transaction();
    for (owner_index, owner) in owners.iter().enumerate() {
        for index in 0..count_per_owner {
            let row_index = total_rows + owner_index * count_per_owner + index;
            let mut values = recursive_document_values(row_index, 0);
            values.insert("owner_id".to_owned(), json!(owner));
            tx = tx.insert_row(
                "documents",
                &format!("recursive-dashboard-new-{total_rows}-{owner_index}-{index}"),
                values,
            );
        }
    }
    tx.commit()?;
    Ok(())
}

fn seed_folder_tree(runtime: &mut Runtime, node_count: usize, branch_factor: usize) -> Result<()> {
    runtime.insert_row(
        "folders",
        "folder-0",
        BTreeMap::from([
            ("name".to_owned(), json!("Folder 000000")),
            ("parent".to_owned(), json!(null)),
        ]),
    )?;
    let mut tx = runtime.transaction();
    for index in 1..node_count {
        let parent = format!("folder-{}", (index - 1) / branch_factor);
        tx = tx.insert_row(
            "folders",
            &format!("folder-{index}"),
            BTreeMap::from([
                ("name".to_owned(), json!(format!("Folder {index:06}"))),
                ("parent".to_owned(), json!(parent)),
            ]),
        );
    }
    tx.commit()?;
    Ok(())
}

fn mutate_folder_tree(
    runtime: &mut Runtime,
    node_count: usize,
    branch_factor: usize,
) -> Result<()> {
    let mut tx = runtime.transaction();
    for index in 0..25 {
        let id = node_count + index;
        let parent = format!("folder-{}", index % branch_factor);
        tx = tx.insert_row(
            "folders",
            &format!("folder-new-{index}"),
            BTreeMap::from([
                ("name".to_owned(), json!(format!("New folder {id:06}"))),
                ("parent".to_owned(), json!(parent)),
            ]),
        );
    }
    for index in 0..25 {
        let row_index = 1 + index;
        tx = tx.update_row(
            "folders",
            &format!("folder-{row_index}"),
            BTreeMap::from([("name".to_owned(), json!(format!("Renamed folder {index}")))]),
        );
    }
    for index in 0..10 {
        let row_index = node_count.saturating_sub(1 + index);
        tx = tx.delete_row("folders", &format!("folder-{row_index}"));
    }
    for index in 0..10 {
        let row_index = node_count.saturating_sub(11 + index);
        tx = tx.update_row(
            "folders",
            &format!("folder-{row_index}"),
            BTreeMap::from([("parent".to_owned(), json!("folder-1"))]),
        );
    }
    tx.commit()?;
    Ok(())
}

fn seed_raw_folder_edges(
    conn: &Connection,
    node_count: usize,
    branch_factor: usize,
) -> rusqlite::Result<()> {
    let mut stmt =
        conn.prepare("INSERT INTO folder_current (row_num, parent_num, name) VALUES (?, ?, ?)")?;
    for index in 0..node_count {
        let parent = if index == 0 {
            None
        } else {
            Some(((index - 1) / branch_factor) as i64)
        };
        stmt.execute(params![index as i64, parent, format!("Folder {index:06}")])?;
    }
    Ok(())
}

fn seed_raw_folder_closure(
    conn: &Connection,
    node_count: usize,
    branch_factor: usize,
) -> rusqlite::Result<usize> {
    let mut rows = 0;
    let mut stmt = conn.prepare(
        "INSERT INTO folder_closure (ancestor_num, descendant_num, depth)
         VALUES (?, ?, ?)",
    )?;
    for descendant in 0..node_count {
        stmt.execute(params![descendant as i64, descendant as i64, 0_i64])?;
        rows += 1;
        let mut ancestor = descendant;
        let mut depth = 1_i64;
        while ancestor > 0 {
            ancestor = (ancestor - 1) / branch_factor;
            stmt.execute(params![ancestor as i64, descendant as i64, depth])?;
            rows += 1;
            depth += 1;
        }
    }
    Ok(rows)
}

fn query_raw_recursive_cte(conn: &Connection) -> rusqlite::Result<usize> {
    conn.query_row(
        "WITH RECURSIVE subtree(row_num) AS (
           SELECT row_num FROM folder_current WHERE row_num = 0
           UNION
           SELECT child.row_num
           FROM folder_current child
           JOIN subtree ON child.parent_num = subtree.row_num
         )
         SELECT COUNT(*) FROM subtree",
        [],
        |row| row.get(0),
    )
}

fn query_raw_closure(conn: &Connection) -> rusqlite::Result<usize> {
    conn.query_row(
        "SELECT COUNT(*)
         FROM folder_closure closure
         JOIN folder_current current ON current.row_num = closure.descendant_num
         WHERE closure.ancestor_num = 0",
        [],
        |row| row.get(0),
    )
}

struct MixedMutationConfig {
    total_rows: usize,
    target_owner_rows: usize,
    top_inserts: usize,
    current_page_updates: usize,
    current_page_deletes: usize,
    off_page_owner_updates: usize,
    unrelated_owner_updates: usize,
}

fn apply_mixed_mutations(runtime: &mut Runtime, config: MixedMutationConfig) -> Result<()> {
    let mut tx = runtime.transaction();
    for index in 0..config.top_inserts {
        let row_index = config.total_rows + index;
        let mut values = document_values(row_index, config.target_owner_rows);
        values.insert("owner_id".to_owned(), json!(OWNER));
        tx = tx.insert_row("documents", &format!("doc-mixed-new-top-{index}"), values);
    }
    for index in 0..config.current_page_updates {
        let row_index = config.target_owner_rows.saturating_sub(1 + index);
        tx = tx.update_row(
            "documents",
            &format!("doc-{row_index}"),
            BTreeMap::from([(
                "title".to_owned(),
                json!(format!("Current page updated {index}")),
            )]),
        );
    }
    for index in 0..config.current_page_deletes {
        let row_index = config
            .target_owner_rows
            .saturating_sub(1 + config.current_page_updates + index);
        tx = tx.delete_row("documents", &format!("doc-{row_index}"));
    }
    for index in 0..config.off_page_owner_updates {
        let row_index = index.min(config.target_owner_rows.saturating_sub(1));
        tx = tx.update_row(
            "documents",
            &format!("doc-{row_index}"),
            BTreeMap::from([(
                "title".to_owned(),
                json!(format!("Off-page owner updated {index}")),
            )]),
        );
    }
    for index in 0..config.unrelated_owner_updates {
        let row_index = config.target_owner_rows + index;
        tx = tx.update_row(
            "documents",
            &format!("doc-{row_index}"),
            BTreeMap::from([(
                "title".to_owned(),
                json!(format!("Unrelated owner updated {index}")),
            )]),
        );
    }
    tx.commit()?;
    Ok(())
}

fn document_values(
    row_index: usize,
    target_owner_rows: usize,
) -> BTreeMap<String, serde_json::Value> {
    let is_target_owner = row_index < target_owner_rows;
    let owner_id = if is_target_owner {
        OWNER.to_owned()
    } else {
        format!("user-{}", row_index % 10_000)
    };
    BTreeMap::from([
        ("owner_id".to_owned(), json!(owner_id)),
        ("org".to_owned(), json!(format!("org-{}", row_index % 100))),
        ("updated_at".to_owned(), json!(format!("{:020}", row_index))),
        ("title".to_owned(), json!(format!("Document {row_index}"))),
    ])
}

fn recursive_document_values(
    row_index: usize,
    target_owner_rows: usize,
) -> BTreeMap<String, serde_json::Value> {
    let is_target_owner = row_index < target_owner_rows;
    let owner_id = if is_target_owner {
        OWNER.to_owned()
    } else {
        format!("user-{}", row_index % 10_000)
    };
    BTreeMap::from([
        ("owner_id".to_owned(), json!(owner_id)),
        (
            "project".to_owned(),
            json!(format!("project-{}", row_index % 100)),
        ),
        ("updated_at".to_owned(), json!(format!("{:020}", row_index))),
        (
            "title".to_owned(),
            json!(format!("Recursive document {row_index}")),
        ),
    ])
}

fn approx_raw_json_payload_bytes(config: &Config) -> BenchResult<usize> {
    let mut total = 0;
    for org_index in 0..100 {
        total += serde_json::to_vec(&BTreeMap::from([(
            "name".to_owned(),
            json!(format!("Organization {org_index}")),
        )]))?
        .len();
    }
    for row_index in 0..config.total_rows {
        total += serde_json::to_vec(&document_values(row_index, config.target_owner_rows))?.len();
    }
    Ok(total)
}

fn storage_for(config: &Config, dir: &tempfile::TempDir, file_name: &str) -> Storage {
    if config.durable_intermediaries {
        Storage::File(dir.path().join(file_name))
    } else {
        Storage::Memory
    }
}

struct BundleSummary {
    bytes: usize,
}

impl BundleSummary {
    fn from(bundle: &Bundle) -> BenchResult<Self> {
        Ok(Self {
            bytes: encode_bundle(bundle)?.len(),
        })
    }
}

fn encode_native_bundle(bundle: &Bundle) -> BenchResult<Vec<u8>> {
    Ok(encode_bundle(bundle)?)
}

fn decode_native_bundle(bytes: &[u8]) -> BenchResult<Bundle> {
    Ok(decode_bundle(bytes)?)
}

fn max_local_epoch_for_node(bundle: &Bundle, node_id: &str) -> i64 {
    bundle
        .txs
        .iter()
        .filter(|tx| tx.node_id == node_id)
        .map(|tx| tx.local_epoch)
        .max()
        .unwrap_or(0)
}

fn gzip_json_bytes(bundle: &Bundle) -> BenchResult<Option<usize>> {
    let mut child = match Command::new("gzip")
        .arg("-c")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return Ok(None),
    };
    let payload = encode_bundle(bundle)?;
    let mut stdin = child.stdin.take().ok_or("gzip stdin was not piped")?;
    stdin.write_all(&payload)?;
    drop(stdin);
    let output = child.wait_with_output()?;
    if output.status.success() {
        Ok(Some(output.stdout.len()))
    } else {
        Ok(None)
    }
}

struct BundleBatchSummary {
    bytes: usize,
    history_rows: usize,
    transaction_rows: usize,
    observed_facts: usize,
}

impl BundleBatchSummary {
    fn from(bundles: &[Bundle]) -> BenchResult<Self> {
        let mut bytes = 0;
        let mut history_rows = 0;
        let mut transaction_rows = 0;
        let mut observed_facts = 0;
        for bundle in bundles {
            bytes += encode_bundle(bundle)?.len();
            history_rows += bundle.history.len();
            transaction_rows += bundle.txs.len();
            observed_facts += bundle.query_reads.len();
        }
        Ok(Self {
            bytes,
            history_rows,
            transaction_rows,
            observed_facts,
        })
    }
}

fn timed(f: impl FnOnce() -> Result<()>) -> Result<Duration> {
    let started = Instant::now();
    f()?;
    Ok(started.elapsed())
}

fn timed_apply_bundles(runtime: &mut Runtime, bundles: Vec<Bundle>) -> Result<Duration> {
    timed(|| {
        for bundle in bundles {
            runtime.apply_bundle(&bundle)?;
        }
        Ok(())
    })
}

fn run_deep_history_probe(kind: &str) -> BenchResult<DeepHistoryReport> {
    match kind {
        "append" | "append-stream" => Ok(DeepHistoryReport::AppendStream(run_append_stream_probe()?)),
        "append-history-blocks" | "append-stream-history-blocks" => Ok(
            DeepHistoryReport::AppendStreamHistoryBlocks(run_append_stream_history_blocks_probe()?),
        ),
        "append-text-ops" | "append-oplog" => Ok(
            DeepHistoryReport::AppendStreamBlockOps(run_append_text_ops_probe()?),
        ),
        "automerge" | "automerge-paper" => {
            Ok(DeepHistoryReport::AutomergePaper(run_automerge_paper_probe()?))
        }
        "automerge-history-blocks" | "automerge-paper-history-blocks" => Ok(
            DeepHistoryReport::AutomergePaperHistoryBlocks(
                run_automerge_paper_history_blocks_probe()?,
            ),
        ),
        "automerge-text-ops" | "automerge-oplog" => Ok(
            DeepHistoryReport::AutomergePaperBlockOps(run_automerge_text_ops_probe()?),
        ),
        "canvas" | "canvas-positions" => {
            Ok(DeepHistoryReport::CanvasPositions(run_canvas_positions_probe()?))
        }
        "canvas-history-blocks" | "canvas-positions-history-blocks" => Ok(
            DeepHistoryReport::CanvasPositionsHistoryBlocks(run_canvas_positions_history_blocks_probe()?),
        ),
        other => Err(format!(
            "unknown MINI_JAZZ_PERF_ONLY_DEEP_HISTORY={other}; expected all, all-history-blocks, all-block-ops, append, append-history-blocks, append-text-ops, automerge-paper, automerge-history-blocks, automerge-text-ops, canvas, or canvas-history-blocks"
        )
        .into()),
    }
}

fn run_all_deep_history_probes() -> BenchResult<Vec<DeepHistoryReport>> {
    Ok(vec![
        DeepHistoryReport::AppendStream(run_append_stream_probe()?),
        DeepHistoryReport::AutomergePaper(run_automerge_paper_probe()?),
        DeepHistoryReport::CanvasPositions(run_canvas_positions_probe()?),
    ])
}

fn run_all_deep_history_block_probes() -> BenchResult<Vec<DeepHistoryReport>> {
    Ok(vec![
        DeepHistoryReport::AppendStreamHistoryBlocks(run_append_stream_history_blocks_probe()?),
        DeepHistoryReport::AutomergePaperHistoryBlocks(run_automerge_paper_history_blocks_probe()?),
        DeepHistoryReport::CanvasPositionsHistoryBlocks(
            run_canvas_positions_history_blocks_probe()?
        ),
    ])
}

fn run_all_deep_history_block_ops_probes() -> BenchResult<Vec<DeepHistoryReport>> {
    Ok(vec![
        DeepHistoryReport::AppendStreamBlockOps(run_append_text_ops_probe()?),
        DeepHistoryReport::AutomergePaperBlockOps(run_automerge_text_ops_probe()?),
        DeepHistoryReport::CanvasPositionsHistoryBlocks(
            run_canvas_positions_history_blocks_probe()?
        ),
    ])
}

fn run_deep_history_group_probe(kind: &str) -> BenchResult<Option<Vec<DeepHistoryReport>>> {
    match kind {
        "all" | "all-baseline" | "all-base" => Ok(Some(run_all_deep_history_probes()?)),
        "all-history-blocks" | "all-blocks" | "all-block" => {
            Ok(Some(run_all_deep_history_block_probes()?))
        }
        "all-block-ops" | "all-ops" => Ok(Some(run_all_deep_history_block_ops_probes()?)),
        _ => Ok(None),
    }
}

fn run_append_stream_probe() -> BenchResult<DeepHistoryCaseReport> {
    let target_updates = env_usize("MINI_JAZZ_DEEP_HISTORY_APPEND_TOKENS", 100_000);
    let max_seconds = env_usize("MINI_JAZZ_DEEP_HISTORY_MAX_SECONDS", 120) as u64;
    let sample_every =
        deep_history_sample_every("MINI_JAZZ_DEEP_HISTORY_APPEND_SAMPLE_EVERY", 1_000);
    let token = env::var("MINI_JAZZ_DEEP_HISTORY_APPEND_TOKEN").unwrap_or_else(|_| " token".into());
    let mut state = String::new();
    run_naive_deep_history_case(DeepHistoryCaseInput {
        target_updates,
        max_seconds,
        sample_every,
        schema: text_document_schema(),
        table: "documents",
        row_id: "stream",
        field: "body",
        initial_value: String::new(),
        reference_gzip_bytes: None,
        next_value: Box::new(move |_| {
            state.push_str(&token);
            Ok(state.clone())
        }),
        final_reference_json: None,
        compare_to_final_payload: true,
        compact_hot_tail: None,
        compact_max_rows_per_block: None,
        reclaim_after_compact: false,
        write_batch_size: deep_history_write_batch_size(),
        notes: vec!["Naive baseline: one full-row text update per token-like append.".to_owned()],
    })
}

fn run_append_stream_history_blocks_probe() -> BenchResult<DeepHistoryCaseReport> {
    let target_updates = env_usize("MINI_JAZZ_DEEP_HISTORY_APPEND_TOKENS", 2_225);
    let max_seconds = env_usize("MINI_JAZZ_DEEP_HISTORY_MAX_SECONDS", 120) as u64;
    let sample_every = deep_history_sample_every("MINI_JAZZ_DEEP_HISTORY_APPEND_SAMPLE_EVERY", 445);
    let hot_tail = env_usize("MINI_JAZZ_DEEP_HISTORY_COMPACT_HOT_TAIL", sample_every);
    let max_rows_per_block = deep_history_max_rows_per_block();
    let reclaim_after_compact = env_bool("MINI_JAZZ_DEEP_HISTORY_RECLAIM_AFTER_COMPACT", false);
    let token = env::var("MINI_JAZZ_DEEP_HISTORY_APPEND_TOKEN").unwrap_or_else(|_| " token".into());
    let mut state = String::new();
    run_naive_deep_history_case(DeepHistoryCaseInput {
        target_updates,
        max_seconds,
        sample_every,
        schema: text_document_schema(),
        table: "documents",
        row_id: "stream",
        field: "body",
        initial_value: String::new(),
        reference_gzip_bytes: None,
        next_value: Box::new(move |_| {
            state.push_str(&token);
            Ok(state.clone())
        }),
        final_reference_json: None,
        compare_to_final_payload: true,
        compact_hot_tail: Some(hot_tail),
        compact_max_rows_per_block: max_rows_per_block,
        reclaim_after_compact,
        write_batch_size: deep_history_write_batch_size(),
        notes: vec![
            "History blocks: naive full-row writes, then seal old accepted history into lz4 blocks."
                .to_owned(),
        ],
    })
}

fn run_automerge_paper_probe() -> BenchResult<DeepHistoryCaseReport> {
    let trace_path = automerge_trace_path()?;
    let trace_bytes = std::fs::metadata(&trace_path)?.len() as usize;
    let trace_json = gzip_decode_to_string(&trace_path)?;
    let trace: AutomergeTrace = serde_json::from_str(&trace_json)?;
    let target_updates = env_optional_usize("MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_UPDATES")
        .unwrap_or(trace.txns.len())
        .min(trace.txns.len());
    let max_seconds = env_usize("MINI_JAZZ_DEEP_HISTORY_MAX_SECONDS", 120) as u64;
    let sample_every =
        deep_history_sample_every("MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_SAMPLE_EVERY", 1_000);
    let mut state = trace.start_content;
    let txns = trace.txns;
    let available_txns = txns.len();
    run_naive_deep_history_case(DeepHistoryCaseInput {
        target_updates,
        max_seconds,
        sample_every,
        schema: text_document_schema(),
        table: "documents",
        row_id: "automerge-paper",
        field: "body",
        initial_value: state.clone(),
        reference_gzip_bytes: Some(trace_bytes),
        next_value: Box::new(move |index| {
            apply_automerge_patches(&mut state, &txns[index].patches)?;
            Ok(state.clone())
        }),
        final_reference_json: None,
        compare_to_final_payload: true,
        compact_hot_tail: None,
        compact_max_rows_per_block: None,
        reclaim_after_compact: false,
        write_batch_size: deep_history_write_batch_size(),
        notes: vec![
            format!("Source trace transactions available: {available_txns}"),
            "Naive baseline: apply trace patch locally, then write whole document body.".to_owned(),
        ],
    })
}

fn run_automerge_paper_history_blocks_probe() -> BenchResult<DeepHistoryCaseReport> {
    let trace_path = automerge_trace_path()?;
    let trace_bytes = std::fs::metadata(&trace_path)?.len() as usize;
    let trace_json = gzip_decode_to_string(&trace_path)?;
    let trace: AutomergeTrace = serde_json::from_str(&trace_json)?;
    let target_updates = env_optional_usize("MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_UPDATES")
        .unwrap_or(2_900)
        .min(trace.txns.len());
    let max_seconds = env_usize("MINI_JAZZ_DEEP_HISTORY_MAX_SECONDS", 120) as u64;
    let sample_every =
        deep_history_sample_every("MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_SAMPLE_EVERY", 580);
    let hot_tail = env_usize("MINI_JAZZ_DEEP_HISTORY_COMPACT_HOT_TAIL", sample_every);
    let max_rows_per_block = deep_history_max_rows_per_block();
    let reclaim_after_compact = env_bool("MINI_JAZZ_DEEP_HISTORY_RECLAIM_AFTER_COMPACT", false);
    let mut state = trace.start_content;
    let txns = trace.txns;
    let available_txns = txns.len();
    run_naive_deep_history_case(DeepHistoryCaseInput {
        target_updates,
        max_seconds,
        sample_every,
        schema: text_document_schema(),
        table: "documents",
        row_id: "automerge-paper",
        field: "body",
        initial_value: state.clone(),
        reference_gzip_bytes: Some(trace_bytes),
        next_value: Box::new(move |index| {
            apply_automerge_patches(&mut state, &txns[index].patches)?;
            Ok(state.clone())
        }),
        final_reference_json: None,
        compare_to_final_payload: true,
        compact_hot_tail: Some(hot_tail),
        compact_max_rows_per_block: max_rows_per_block,
        reclaim_after_compact,
        write_batch_size: deep_history_write_batch_size(),
        notes: vec![
            format!("Source trace transactions available: {available_txns}"),
            "History blocks: naive full-row writes, then seal old accepted history into lz4 blocks."
                .to_owned(),
        ],
    })
}

fn run_append_text_ops_probe() -> BenchResult<DeepHistoryCaseReport> {
    let target_updates = env_usize("MINI_JAZZ_DEEP_HISTORY_APPEND_TOKENS", 2_225);
    let sample_every = deep_history_sample_every("MINI_JAZZ_DEEP_HISTORY_APPEND_SAMPLE_EVERY", 445);
    let hot_tail = env_usize("MINI_JAZZ_DEEP_HISTORY_COMPACT_HOT_TAIL", sample_every);
    let snapshot_every = env_usize("MINI_JAZZ_DEEP_HISTORY_TEXT_OP_SNAPSHOT_EVERY", 256);
    let token = env::var("MINI_JAZZ_DEEP_HISTORY_APPEND_TOKEN").unwrap_or_else(|_| " token".into());
    let mut state_len = 0usize;
    run_text_ops_case(TextOpsCaseInput {
        target_updates,
        sample_every,
        compact_hot_tail: Some(hot_tail),
        reference_gzip_bytes: None,
        snapshot_every,
        next_edit: Box::new(move |conn, root, _index, snapshot_every| {
            state_len += token.len();
            Ok((
                persisted_text_ops::append(conn, root, &token, snapshot_every)?,
                state_len,
            ))
        }),
        notes: vec![
            format!(
                "Text op-log sidecar: current roots are latest op ids; snapshots are content-addressed fixed chunks every {snapshot_every} ops."
            ),
        ],
    })
}

fn run_automerge_text_ops_probe() -> BenchResult<DeepHistoryCaseReport> {
    let trace_path = automerge_trace_path()?;
    let trace_bytes = std::fs::metadata(&trace_path)?.len() as usize;
    let trace_json = gzip_decode_to_string(&trace_path)?;
    let trace: AutomergeTrace = serde_json::from_str(&trace_json)?;
    let target_updates = env_optional_usize("MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_UPDATES")
        .unwrap_or(2_900)
        .min(trace.txns.len());
    let sample_every =
        deep_history_sample_every("MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_SAMPLE_EVERY", 580);
    let hot_tail = env_usize("MINI_JAZZ_DEEP_HISTORY_COMPACT_HOT_TAIL", sample_every);
    let snapshot_every = env_usize("MINI_JAZZ_DEEP_HISTORY_TEXT_OP_SNAPSHOT_EVERY", 256);
    let mut materialized = trace.start_content;
    let txns = trace.txns;
    let available_txns = txns.len();
    run_text_ops_case(TextOpsCaseInput {
        target_updates,
        sample_every,
        compact_hot_tail: Some(hot_tail),
        reference_gzip_bytes: Some(trace_bytes),
        snapshot_every,
        next_edit: Box::new(move |conn, mut root, index, snapshot_every| {
            for (pos, del, ins) in &txns[index].patches {
                let start = byte_index_for_char(&materialized, *pos)?;
                let end = byte_index_for_char(&materialized, pos + del)?;
                root = persisted_text_ops::replace_range(
                    conn,
                    root,
                    start,
                    end - start,
                    ins,
                    snapshot_every,
                )?;
                materialized.replace_range(start..end, ins);
            }
            Ok((root, materialized.len()))
        }),
        notes: vec![
            format!("Source trace transactions available: {available_txns}"),
            format!(
                "Text op-log sidecar: current roots are latest op ids; snapshots are content-addressed fixed chunks every {snapshot_every} ops."
            ),
        ],
    })
}

fn run_canvas_positions_probe() -> BenchResult<DeepHistoryCaseReport> {
    let target_updates = env_usize("MINI_JAZZ_DEEP_HISTORY_CANVAS_FRAMES", 60 * 60 * 60);
    let max_seconds = env_usize("MINI_JAZZ_DEEP_HISTORY_MAX_SECONDS", 120) as u64;
    let sample_every =
        deep_history_sample_every("MINI_JAZZ_DEEP_HISTORY_CANVAS_SAMPLE_EVERY", 1_000);
    let mut all_positions = Vec::with_capacity(target_updates);
    for frame in 0..target_updates {
        all_positions.push(canvas_position_json(frame));
    }
    let reference_json = serde_json::to_string(&all_positions)?;
    let reference_gzip_bytes = Some(gzip_bytes(reference_json.as_bytes())?);
    run_naive_deep_history_case(DeepHistoryCaseInput {
        target_updates,
        max_seconds,
        sample_every,
        schema: text_document_schema(),
        table: "documents",
        row_id: "canvas-object",
        field: "body",
        initial_value: canvas_position_json(0),
        reference_gzip_bytes,
        next_value: Box::new(move |index| Ok(all_positions[index].clone())),
        final_reference_json: Some(reference_json),
        compare_to_final_payload: false,
        compact_hot_tail: None,
        compact_max_rows_per_block: None,
        reclaim_after_compact: false,
        write_batch_size: deep_history_write_batch_size(),
        notes: vec![
            "Naive baseline: one full-row position JSON text update per 60 FPS frame.".to_owned(),
            "Final-payload ratios are not meaningful for presence-like coordinates and are emitted as null.".to_owned(),
        ],
    })
}

fn run_canvas_positions_history_blocks_probe() -> BenchResult<DeepHistoryCaseReport> {
    let target_updates = env_usize("MINI_JAZZ_DEEP_HISTORY_CANVAS_FRAMES", 3_900);
    let max_seconds = env_usize("MINI_JAZZ_DEEP_HISTORY_MAX_SECONDS", 120) as u64;
    let sample_every = deep_history_sample_every("MINI_JAZZ_DEEP_HISTORY_CANVAS_SAMPLE_EVERY", 780);
    let hot_tail = env_usize("MINI_JAZZ_DEEP_HISTORY_COMPACT_HOT_TAIL", sample_every);
    let max_rows_per_block = deep_history_max_rows_per_block();
    let reclaim_after_compact = env_bool("MINI_JAZZ_DEEP_HISTORY_RECLAIM_AFTER_COMPACT", false);
    let mut all_positions = Vec::with_capacity(target_updates);
    for frame in 0..target_updates {
        all_positions.push(canvas_position_json(frame));
    }
    let reference_json = serde_json::to_string(&all_positions)?;
    let reference_gzip_bytes = Some(gzip_bytes(reference_json.as_bytes())?);
    run_naive_deep_history_case(DeepHistoryCaseInput {
        target_updates,
        max_seconds,
        sample_every,
        schema: text_document_schema(),
        table: "documents",
        row_id: "canvas-object",
        field: "body",
        initial_value: canvas_position_json(0),
        reference_gzip_bytes,
        next_value: Box::new(move |index| Ok(all_positions[index].clone())),
        final_reference_json: Some(reference_json),
        compare_to_final_payload: false,
        compact_hot_tail: Some(hot_tail),
        compact_max_rows_per_block: max_rows_per_block,
        reclaim_after_compact,
        write_batch_size: deep_history_write_batch_size(),
        notes: vec![
            "History blocks: naive full-row writes, then seal old accepted history into lz4 blocks."
                .to_owned(),
            "Final-payload ratios are not meaningful for presence-like coordinates and are emitted as null.".to_owned(),
        ],
    })
}

struct DeepHistoryCaseInput {
    target_updates: usize,
    max_seconds: u64,
    sample_every: usize,
    schema: SchemaDef,
    table: &'static str,
    row_id: &'static str,
    field: &'static str,
    initial_value: String,
    reference_gzip_bytes: Option<usize>,
    next_value: Box<dyn FnMut(usize) -> BenchResult<String>>,
    final_reference_json: Option<String>,
    compare_to_final_payload: bool,
    compact_hot_tail: Option<usize>,
    compact_max_rows_per_block: Option<usize>,
    reclaim_after_compact: bool,
    write_batch_size: Option<usize>,
    notes: Vec<String>,
}

struct TextOpsCaseInput {
    target_updates: usize,
    sample_every: usize,
    compact_hot_tail: Option<usize>,
    reference_gzip_bytes: Option<usize>,
    snapshot_every: usize,
    next_edit: Box<TextOpsEditFn>,
    notes: Vec<String>,
}

type TextOpsEditFn = dyn FnMut(
    &Connection,
    persisted_text_ops::TextRoot,
    usize,
    usize,
) -> BenchResult<(persisted_text_ops::TextRoot, usize)>;

struct PendingTextOpWrite {
    root: persisted_text_ops::TextRoot,
    op_bytes: Vec<u8>,
}

fn flush_text_op_writes(
    writer: &mut Runtime,
    pending_writes: &mut Vec<PendingTextOpWrite>,
) -> BenchResult<()> {
    if pending_writes.is_empty() {
        return Ok(());
    }
    let updates = pending_writes
        .drain(..)
        .map(|write| {
            (
                "doc".to_owned(),
                map1(
                    "body_root",
                    json!(write.root.unwrap_or_default().to_string()),
                )
                .into_iter()
                .chain(map1("body_op", json!(bytes_to_hex(&write.op_bytes))).into_iter())
                .collect::<BTreeMap<String, serde_json::Value>>(),
            )
        })
        .collect::<Vec<_>>();
    writer.update_rows_batched("documents", updates)?;
    Ok(())
}

fn run_text_ops_case(mut input: TextOpsCaseInput) -> BenchResult<DeepHistoryCaseReport> {
    let tmp = tempdir()?;
    let writer_db_path = tmp.path().join("writer.sqlite");
    let writer_ops_path = tmp.path().join("writer-text-ops.sqlite");
    let receiver_ops_path = tmp.path().join("receiver-text-ops.sqlite");
    let cold_ops_path = tmp.path().join("cold-text-ops.sqlite");
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("body_root");
        table.bytes("body_op");
    });
    let mut writer = Runtime::open_with_schema(
        Storage::File(writer_db_path.clone()),
        "writer-node",
        "alice",
        schema.clone(),
    )?;
    let mut receiver =
        Runtime::open_with_schema(Storage::Memory, "receiver-node", "bob", schema.clone())?;
    let mut writer_ops = Connection::open(&writer_ops_path)?;
    let mut receiver_ops = Connection::open(&receiver_ops_path)?;
    persisted_text_ops::install(&writer_ops)?;
    persisted_text_ops::install(&receiver_ops)?;
    let mut receiver_inline_watermark = 0i64;

    let mut root = None;
    let (initial_root, mut final_payload_bytes) =
        (input.next_edit)(&writer_ops, root, 0, input.snapshot_every)?;
    root = initial_root;
    let initial_op_bytes = persisted_text_ops::inline_op_bytes(&writer_ops, root)?;
    writer.insert_row(
        "documents",
        "doc",
        map2(
            "body_root",
            json!(root.unwrap_or_default().to_string()),
            "body_op",
            json!(bytes_to_hex(&initial_op_bytes)),
        ),
    )?;
    let initial_bundle = writer.export_table_history("documents")?;
    let initial_wire = encode_native_bundle(&initial_bundle)?;
    let initial_bundle = decode_native_bundle(&initial_wire)?;
    let mut live_sync_writer_epoch = max_local_epoch_for_node(&initial_bundle, "writer-node");
    receiver.apply_bundle(&initial_bundle)?;
    apply_inline_text_ops_from_bundle(
        &initial_bundle,
        &mut receiver_ops,
        &mut receiver_inline_watermark,
        input.snapshot_every,
    )?;
    let mut subscription = receiver.subscribe_rows("documents")?;

    let started = Instant::now();
    let mut receive_samples = Vec::new();
    let mut write_only_ms = 0.0;
    let mut sampled_receive_total_ms = 0.0;
    let mut phase_ms = DeepHistoryPhaseReport::default();
    let mut live_apply_profile = empty_apply_profile();
    let mut cold_apply_profile = empty_apply_profile();
    let mut jazz_write_detail = RuntimeWritePhaseStats::default();
    let mut jazz_write_sqlite_transactions = 0usize;
    let mut jazz_write_updates_per_transaction_total = 0usize;
    let mut jazz_write_updates_per_transaction_max = 0usize;
    let mut self_sample = SelfSample::from_env("text-ops-write")?;
    let batch_window = deep_history_sqlite_tx_batch_window();
    input.notes.push(format!(
        "SQLite transaction batching enabled for Jazz root writes and text op sidecar edits: flush window {} ms.",
        batch_window.as_millis()
    ));
    let mut pending_writes = Vec::new();
    let mut index = 1usize;
    if self_sample.is_enabled() {
        self_sample.start_after_first_sample()?;
    }
    while index < input.target_updates {
        let write_started = Instant::now();
        let sidecar_tx = writer_ops.transaction()?;
        let batch_started = Instant::now();
        let mut should_sample = false;
        loop {
            let edit_started = Instant::now();
            let (next_root, payload_bytes) =
                (input.next_edit)(&sidecar_tx, root, index, input.snapshot_every)?;
            root = next_root;
            final_payload_bytes = payload_bytes;
            let op_bytes = persisted_text_ops::inline_op_bytes(&sidecar_tx, root)?;
            DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.sidecar_edit, edit_started);
            pending_writes.push(PendingTextOpWrite { root, op_bytes });
            should_sample = should_sample
                || (!self_sample.is_enabled() && index == 1)
                || (index + 1).is_multiple_of(input.sample_every)
                || index + 1 == input.target_updates;
            index += 1;
            if index >= input.target_updates
                || should_sample
                || batch_started.elapsed() >= batch_window
            {
                break;
            }
        }
        let sidecar_commit_started = Instant::now();
        sidecar_tx.commit()?;
        DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.sidecar_commit, sidecar_commit_started);
        let jazz_write_started = Instant::now();
        reset_runtime_write_phase_stats();
        let batch_updates = pending_writes.len();
        flush_text_op_writes(&mut writer, &mut pending_writes)?;
        jazz_write_sqlite_transactions += 1;
        jazz_write_updates_per_transaction_total += batch_updates;
        jazz_write_updates_per_transaction_max =
            jazz_write_updates_per_transaction_max.max(batch_updates);
        add_write_detail(&mut jazz_write_detail, take_runtime_write_phase_stats());
        DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.jazz_write, jazz_write_started);
        write_only_ms += ms(write_started.elapsed());

        if should_sample {
            let receive_started = Instant::now();
            let export_started = Instant::now();
            let bundle = if env_bool("MINI_JAZZ_DEEP_HISTORY_INCREMENTAL_LIVE_FILTER", false) {
                writer.export_table_history_since_node_epoch(
                    "documents",
                    "writer-node",
                    live_sync_writer_epoch,
                )?
            } else {
                writer.export_table_history("documents")?
            };
            live_sync_writer_epoch =
                live_sync_writer_epoch.max(max_local_epoch_for_node(&bundle, "writer-node"));
            DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.live_export, export_started);
            let encode_decode_started = Instant::now();
            let wire = encode_native_bundle(&bundle)?;
            let bundle = decode_native_bundle(&wire)?;
            DeepHistoryPhaseReport::add_elapsed(
                &mut phase_ms.live_encode_decode,
                encode_decode_started,
            );
            let apply_started = Instant::now();
            let profile = receiver.profile_apply_bundle(&bundle)?;
            add_apply_profile(&mut live_apply_profile, profile);
            DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.live_apply, apply_started);
            let sidecar_apply_started = Instant::now();
            apply_inline_text_ops_from_bundle(
                &bundle,
                &mut receiver_ops,
                &mut receiver_inline_watermark,
                input.snapshot_every,
            )?;
            DeepHistoryPhaseReport::add_elapsed(
                &mut phase_ms.live_sidecar_apply,
                sidecar_apply_started,
            );
            let listener_started = Instant::now();
            let diffs = receiver.poll_subscription(&mut subscription)?;
            if diffs.is_empty() {
                return Err("text op listener did not observe sampled edit".into());
            }
            let rows = receiver.read_rows("documents")?;
            let observed_root = row_root_id(&rows, "body_root")?;
            let observed = persisted_text_ops::materialize(&receiver_ops, Some(observed_root))?;
            if observed.len() != final_payload_bytes {
                return Err("text op listener materialized unexpected length".into());
            }
            DeepHistoryPhaseReport::add_elapsed(
                &mut phase_ms.live_listener_and_verify,
                listener_started,
            );
            let receive_ms = ms(receive_started.elapsed());
            sampled_receive_total_ms += receive_ms;
            receive_samples.push(receive_ms);
        }
        if !self_sample.is_enabled() {
            self_sample.start_after_first_sample()?;
        }
    }
    self_sample.finish()?;
    let total_loop_ms = ms(started.elapsed());

    let mut block_native_export_ms = None;
    let mut block_native_import_ms = None;
    let mut block_native_blocks = None;
    let mut block_native_payload_bytes = None;
    let mut native_sync_bytes = None;
    let mut native_export_ms = None;
    let mut native_import_ms = None;
    if let Some(hot_tail) = input.compact_hot_tail {
        let compaction_started = Instant::now();
        let compaction = writer.compact_table_accepted_history("documents", hot_tail, hot_tail)?;
        DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.compaction, compaction_started);
        input.notes.push(format!(
            "Root history block compaction: hot tail {}, sealed rows {}, blocks {}, sealed tx rows {}, uncompressed bytes {}, compressed bytes {}.",
            hot_tail,
            compaction.sealed_history_rows,
            compaction.history_blocks,
            compaction.sealed_transactions,
            compaction.uncompressed_bytes,
            compaction.compressed_bytes,
        ));
        persisted_text_ops::snapshot(&writer_ops, root)?;
        let block_export_started = Instant::now();
        let delta = writer.export_table_history_delta("documents", &[])?;
        let delta_wire = encode_native_bundle(&delta.bundle)?;
        let delta_bundle = decode_native_bundle(&delta_wire)?;
        block_native_export_ms = Some(ms(block_export_started.elapsed()));
        phase_ms.native_export += block_native_export_ms.unwrap_or(0.0);
        block_native_blocks = Some(delta.blocks.len());
        block_native_payload_bytes =
            Some(delta.blocks.iter().map(|block| block.payload.len()).sum());
        native_export_ms = block_native_export_ms;
        let mut block_peer =
            Runtime::open_with_schema(Storage::Memory, "block-peer-node", "bob", schema.clone())?;
        let block_peer_ops_path = tmp.path().join("block-peer-text-ops.sqlite");
        let mut block_peer_ops = Connection::open(&block_peer_ops_path)?;
        persisted_text_ops::install(&block_peer_ops)?;
        let mut block_watermark = 0i64;
        let block_import_started = Instant::now();
        block_peer.apply_history_delta(&delta_bundle, &delta.blocks)?;
        let block_history = block_peer.export_table_history("documents")?;
        apply_inline_text_ops_from_bundle(
            &block_history,
            &mut block_peer_ops,
            &mut block_watermark,
            input.snapshot_every,
        )?;
        let block_rows = block_peer.read_rows("documents")?;
        let block_root = row_root_id(&block_rows, "body_root")?;
        let block_text = persisted_text_ops::materialize(&block_peer_ops, Some(block_root))?;
        if block_text.len() != final_payload_bytes {
            return Err("text op block import materialized unexpected length".into());
        }
        block_native_import_ms = Some(ms(block_import_started.elapsed()));
        phase_ms.native_import_apply += block_native_import_ms.unwrap_or(0.0);
        native_import_ms = block_native_import_ms;
        native_sync_bytes = Some(delta_wire.len() + block_native_payload_bytes.unwrap_or(0));
    }

    let current_started = Instant::now();
    let current_rows = writer.read_rows("documents")?;
    let current_root = row_root_id(&current_rows, "body_root")?;
    let current = persisted_text_ops::materialize(&writer_ops, Some(current_root))?;
    let current_read_ms = ms(current_started.elapsed());
    phase_ms.current_read += current_read_ms;
    if current.len() != final_payload_bytes {
        return Err("text op current read materialized unexpected length".into());
    }

    let final_bundle_export_started = Instant::now();
    let final_bundle = writer.export_table_history("documents")?;
    DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.final_export, final_bundle_export_started);
    let final_encode_decode_started = Instant::now();
    let final_wire = encode_native_bundle(&final_bundle)?;
    let final_bundle = decode_native_bundle(&final_wire)?;
    DeepHistoryPhaseReport::add_elapsed(
        &mut phase_ms.final_encode_decode,
        final_encode_decode_started,
    );
    let final_bundle_export_ms = phase_ms.final_export + phase_ms.final_encode_decode;
    let final_bundle_bytes = final_wire.len();
    let sidecar_bundle_bytes = 0;
    let mut cold = Runtime::open_with_schema(Storage::Memory, "cold-node", "bob", schema)?;
    let mut cold_ops = Connection::open(&cold_ops_path)?;
    persisted_text_ops::install(&cold_ops)?;
    let mut cold_watermark = 0i64;
    let cold_started = Instant::now();
    let cold_apply_started = Instant::now();
    let profile = cold.profile_apply_bundle(&final_bundle)?;
    add_apply_profile(&mut cold_apply_profile, profile);
    DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.cold_apply, cold_apply_started);
    let cold_sidecar_started = Instant::now();
    apply_inline_text_ops_from_bundle(
        &final_bundle,
        &mut cold_ops,
        &mut cold_watermark,
        input.snapshot_every,
    )?;
    DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.cold_sidecar_apply, cold_sidecar_started);
    let cold_verify_started = Instant::now();
    let cold_rows = cold.read_rows("documents")?;
    let cold_root = row_root_id(&cold_rows, "body_root")?;
    let cold_text = persisted_text_ops::materialize(&cold_ops, Some(cold_root))?;
    DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.cold_read_and_verify, cold_verify_started);
    let cold_load_ms = ms(cold_started.elapsed());
    if cold_text.len() != final_payload_bytes {
        return Err("text op cold load materialized unexpected length".into());
    }

    let historical_epochs = historical_read_epochs_for_case(
        &final_bundle,
        &writer.history_block_manifests("documents")?,
        "doc",
    );
    let historical_started = Instant::now();
    let mut historical_read_count = 0;
    for epoch in &historical_epochs {
        if let Some(row) =
            writer.read_row_at_node_epoch("documents", "doc", "writer-node", *epoch)?
        {
            let historical_root = row
                .values
                .get("body_root")
                .and_then(Value::as_str)
                .ok_or("historical text op row missing body_root")?;
            let historical_text = persisted_text_ops::materialize(
                &writer_ops,
                Some(historical_root.parse::<i64>()?),
            )?;
            if historical_text.len() <= final_payload_bytes {
                historical_read_count += 1;
            }
        }
    }
    let historical_read_total_ms = ms(historical_started.elapsed());
    phase_ms.historical_reads += historical_read_total_ms;
    let tx_info_ids = sampled_tx_ids_from_bundle(&final_bundle);
    let tx_info_started = Instant::now();
    for tx_id in &tx_info_ids {
        writer.transaction_info(tx_id)?;
    }
    let transaction_info_total_ms = ms(tx_info_started.elapsed());
    phase_ms.transaction_info_reads += transaction_info_total_ms;
    let transaction_info_count = tx_info_ids.len();
    let writer_stats = writer.storage_stats()?;
    let sidecar_database_bytes = persisted_text_ops::database_bytes(&writer_ops)?;
    let database_bytes = writer_stats.database_bytes;
    let live_database_bytes = writer_stats.live_database_bytes;
    let total_file_bytes = writer_stats.total_file_bytes;
    let mut notes = input.notes;
    notes.push(format!(
        "Text op local materialization cache bytes excluded from storage totals: database {}. Inline op payloads are stored in Jazz history/current rows.",
        sidecar_database_bytes
    ));

    Ok(DeepHistoryCaseReport {
        target_updates: input.target_updates,
        completed_updates: input.target_updates,
        stopped_early: false,
        stop_reason: None,
        elapsed_write_ms: total_loop_ms,
        average_write_ms: total_loop_ms / input.target_updates as f64,
        total_loop_ms,
        write_only_ms,
        average_write_only_ms: write_only_ms / input.target_updates as f64,
        sampled_receive_total_ms,
        live_receive_sample_count: receive_samples.len(),
        live_receive_average_ms: average(&receive_samples),
        live_receive_p50_ms: percentile(receive_samples.clone(), 0.50),
        live_receive_p95_ms: percentile(receive_samples.clone(), 0.95),
        live_receive_p99_ms: percentile(receive_samples.clone(), 0.99),
        live_receive_last_ms: receive_samples.last().copied(),
        cold_load_ms,
        current_read_ms,
        historical_read_total_ms,
        historical_read_count,
        historical_read_average_ms: if historical_read_count == 0 {
            None
        } else {
            Some(historical_read_total_ms / historical_read_count as f64)
        },
        transaction_info_total_ms,
        transaction_info_count,
        transaction_info_average_ms: if transaction_info_count == 0 {
            None
        } else {
            Some(transaction_info_total_ms / transaction_info_count as f64)
        },
        final_payload_bytes,
        extrapolated_final_payload_bytes_for_target: Some(final_payload_bytes),
        reference_gzip_bytes: input.reference_gzip_bytes,
        bundle_bytes: final_bundle_bytes + sidecar_bundle_bytes,
        native_sync_bytes: native_sync_bytes.unwrap_or(final_bundle_bytes + sidecar_bundle_bytes),
        native_export_ms: native_export_ms.unwrap_or(final_bundle_export_ms),
        native_import_ms: native_import_ms.unwrap_or(cold_load_ms),
        block_native_export_ms,
        block_native_import_ms,
        block_native_blocks,
        block_native_payload_bytes,
        database_bytes,
        live_database_bytes,
        freelist_bytes: writer_stats.freelist_bytes,
        total_file_bytes,
        history_rows: writer_stats.history_rows,
        current_rows: writer_stats.current_rows,
        database_to_final_payload_ratio: ratio_i64_usize(database_bytes, final_payload_bytes),
        live_database_to_final_payload_ratio: ratio_i64_usize(
            live_database_bytes,
            final_payload_bytes,
        ),
        total_file_to_final_payload_ratio: ratio_i64_usize(total_file_bytes, final_payload_bytes),
        database_to_extrapolated_final_payload_ratio: ratio_i64_usize(
            database_bytes,
            final_payload_bytes,
        ),
        live_database_to_extrapolated_final_payload_ratio: ratio_i64_usize(
            live_database_bytes,
            final_payload_bytes,
        ),
        total_file_to_extrapolated_final_payload_ratio: ratio_i64_usize(
            total_file_bytes,
            final_payload_bytes,
        ),
        database_to_reference_gzip_ratio: input
            .reference_gzip_bytes
            .and_then(|bytes| ratio_i64_usize(database_bytes, bytes)),
        live_database_to_reference_gzip_ratio: input
            .reference_gzip_bytes
            .and_then(|bytes| ratio_i64_usize(live_database_bytes, bytes)),
        bundle_to_reference_gzip_ratio: input
            .reference_gzip_bytes
            .and_then(|bytes| ratio_usize(final_bundle_bytes + sidecar_bundle_bytes, bytes)),
        extrapolated_write_ms_for_target: None,
        extrapolated_database_bytes_for_target: None,
        phase_ms: DeepHistoryPhaseReport {
            jazz_write_detail,
            live_apply_profile,
            cold_apply_profile,
            jazz_write_sqlite_transactions,
            jazz_write_updates_per_transaction_avg: if jazz_write_sqlite_transactions == 0 {
                None
            } else {
                Some(
                    jazz_write_updates_per_transaction_total as f64
                        / jazz_write_sqlite_transactions as f64,
                )
            },
            jazz_write_updates_per_transaction_max,
            ..phase_ms
        },
        notes,
    })
}

fn apply_inline_text_ops_from_bundle(
    bundle: &Bundle,
    target: &mut Connection,
    watermark: &mut i64,
    snapshot_every: usize,
) -> BenchResult<()> {
    let mut ops = bundle
        .history
        .iter()
        .filter(|record| record.table == "documents" && record.row_id == "doc")
        .filter_map(|record| {
            let root = record
                .values
                .get("body_root")
                .and_then(Value::as_str)?
                .parse::<i64>()
                .ok()?;
            let op = record.values.get("body_op")?;
            let op_bytes = if let Some(bytes) = op.as_bytes() {
                bytes.to_vec()
            } else {
                hex_to_bytes(op.as_str()?).ok()?
            };
            Some((root, op_bytes))
        })
        .collect::<Vec<_>>();
    ops.sort_by_key(|(root, _)| *root);
    let tx = target.transaction()?;
    for (root, op_bytes) in ops {
        if root <= *watermark {
            continue;
        }
        persisted_text_ops::apply_inline_op(&tx, &op_bytes, snapshot_every)?;
        *watermark = root;
    }
    tx.commit()?;
    Ok(())
}

fn run_naive_deep_history_case(
    mut input: DeepHistoryCaseInput,
) -> BenchResult<DeepHistoryCaseReport> {
    let tmp = tempdir()?;
    let db_path = tmp.path().join("writer.sqlite");
    let mut writer = Runtime::open_with_schema(
        Storage::File(db_path.clone()),
        "writer-node",
        "alice",
        input.schema.clone(),
    )?;
    let mut receiver = Runtime::open_with_schema(
        Storage::Memory,
        "receiver-node",
        "bob",
        input.schema.clone(),
    )?;
    let initial_tx = writer.insert_row(
        input.table,
        input.row_id,
        map1(input.field, json!(input.initial_value)),
    )?;
    let mut written_tx_ids = vec![initial_tx];
    let initial_bundle = writer.export_table_history(input.table)?;
    let mut live_sync_writer_epoch = max_local_epoch_for_node(&initial_bundle, "writer-node");
    receiver.apply_bundle(&initial_bundle)?;
    let mut subscription = receiver.subscribe_rows(input.table)?;

    let started = Instant::now();
    let deadline = started + Duration::from_secs(input.max_seconds);
    let mut receive_samples = Vec::new();
    let mut write_only_ms = 0.0;
    let mut sampled_receive_total_ms = 0.0;
    let mut phase_ms = DeepHistoryPhaseReport::default();
    let mut live_apply_profile = empty_apply_profile();
    let mut cold_apply_profile = empty_apply_profile();
    let mut jazz_write_detail = RuntimeWritePhaseStats::default();
    let mut jazz_write_sqlite_transactions = 0usize;
    let mut jazz_write_updates_per_transaction_total = 0usize;
    let mut jazz_write_updates_per_transaction_max = 0usize;
    let mut self_sample = SelfSample::from_env(input.table)?;
    let mut completed_updates = 0;
    let mut last_value = String::new();
    let mut stopped_early = false;
    let mut stop_reason = None;
    let mut notes = input.notes;
    let write_batch_size = input.write_batch_size.unwrap_or(1).max(1);
    let batch_window = deep_history_sqlite_tx_batch_window();
    let mut batch_started_at: Option<Instant> = None;
    let mut pending_updates = Vec::new();
    if write_batch_size > 1 {
        notes.push(format!(
            "SQLite write batching enabled: up to {write_batch_size} logical Jazz txs per SQLite commit."
        ));
    } else {
        notes.push(format!(
            "SQLite transaction batching enabled for row writes: flush window {} ms.",
            batch_window.as_millis()
        ));
    }

    if self_sample.is_enabled() {
        self_sample.start_after_first_sample()?;
    }
    for index in 0..input.target_updates {
        if Instant::now() >= deadline {
            if !pending_updates.is_empty() {
                let write_started = Instant::now();
                let jazz_write_started = Instant::now();
                reset_runtime_write_phase_stats();
                let batch_updates = pending_updates.len();
                let tx_ids = writer
                    .update_rows_batched(input.table, std::mem::take(&mut pending_updates))?;
                jazz_write_sqlite_transactions += 1;
                jazz_write_updates_per_transaction_total += batch_updates;
                jazz_write_updates_per_transaction_max =
                    jazz_write_updates_per_transaction_max.max(batch_updates);
                add_write_detail(&mut jazz_write_detail, take_runtime_write_phase_stats());
                DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.jazz_write, jazz_write_started);
                write_only_ms += ms(write_started.elapsed());
                written_tx_ids.extend(tx_ids);
            }
            stopped_early = true;
            stop_reason = Some(format!("stopped after {} seconds", input.max_seconds));
            break;
        }
        let value = (input.next_value)(index)?;
        last_value = value.clone();
        completed_updates += 1;
        if pending_updates.is_empty() {
            batch_started_at = Some(Instant::now());
        }
        pending_updates.push((input.row_id.to_owned(), map1(input.field, json!(value))));
        let time_window_elapsed = batch_started_at
            .map(|started| started.elapsed() >= batch_window)
            .unwrap_or(false);
        let size_window_elapsed = write_batch_size > 1 && pending_updates.len() >= write_batch_size;
        let should_flush = size_window_elapsed
            || time_window_elapsed
            || (!self_sample.is_enabled() && index == 0)
            || (index + 1) % input.sample_every == 0
            || index + 1 == input.target_updates;
        if should_flush {
            let write_started = Instant::now();
            let jazz_write_started = Instant::now();
            reset_runtime_write_phase_stats();
            let batch_updates = pending_updates.len();
            let tx_ids =
                writer.update_rows_batched(input.table, std::mem::take(&mut pending_updates))?;
            jazz_write_sqlite_transactions += 1;
            jazz_write_updates_per_transaction_total += batch_updates;
            jazz_write_updates_per_transaction_max =
                jazz_write_updates_per_transaction_max.max(batch_updates);
            add_write_detail(&mut jazz_write_detail, take_runtime_write_phase_stats());
            DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.jazz_write, jazz_write_started);
            write_only_ms += ms(write_started.elapsed());
            written_tx_ids.extend(tx_ids);
            batch_started_at = None;
        }

        if (!self_sample.is_enabled() && index == 0)
            || (index + 1) % input.sample_every == 0
            || index + 1 == input.target_updates
        {
            if !pending_updates.is_empty() {
                let write_started = Instant::now();
                let jazz_write_started = Instant::now();
                reset_runtime_write_phase_stats();
                let batch_updates = pending_updates.len();
                let tx_ids = writer
                    .update_rows_batched(input.table, std::mem::take(&mut pending_updates))?;
                jazz_write_sqlite_transactions += 1;
                jazz_write_updates_per_transaction_total += batch_updates;
                jazz_write_updates_per_transaction_max =
                    jazz_write_updates_per_transaction_max.max(batch_updates);
                add_write_detail(&mut jazz_write_detail, take_runtime_write_phase_stats());
                DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.jazz_write, jazz_write_started);
                write_only_ms += ms(write_started.elapsed());
                written_tx_ids.extend(tx_ids);
                batch_started_at = None;
            }
            let receive_started = Instant::now();
            let export_started = Instant::now();
            let bundle = if env_bool("MINI_JAZZ_DEEP_HISTORY_INCREMENTAL_LIVE_FILTER", false) {
                writer.export_table_history_since_node_epoch(
                    input.table,
                    "writer-node",
                    live_sync_writer_epoch,
                )?
            } else {
                writer.export_table_history(input.table)?
            };
            live_sync_writer_epoch =
                live_sync_writer_epoch.max(max_local_epoch_for_node(&bundle, "writer-node"));
            DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.live_export, export_started);
            let apply_started = Instant::now();
            let profile = receiver.profile_apply_bundle(&bundle)?;
            add_apply_profile(&mut live_apply_profile, profile);
            DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.live_apply, apply_started);
            let listener_started = Instant::now();
            let diffs = receiver.poll_subscription(&mut subscription)?;
            if diffs.is_empty() {
                return Err("live listener did not observe sampled edit".into());
            }
            DeepHistoryPhaseReport::add_elapsed(
                &mut phase_ms.live_listener_and_verify,
                listener_started,
            );
            let receive_ms = ms(receive_started.elapsed());
            sampled_receive_total_ms += receive_ms;
            receive_samples.push(receive_ms);
        }
        if !self_sample.is_enabled() {
            self_sample.start_after_first_sample()?;
        }
    }
    self_sample.finish()?;
    if !pending_updates.is_empty() {
        let write_started = Instant::now();
        let jazz_write_started = Instant::now();
        reset_runtime_write_phase_stats();
        let batch_updates = pending_updates.len();
        let tx_ids =
            writer.update_rows_batched(input.table, std::mem::take(&mut pending_updates))?;
        jazz_write_sqlite_transactions += 1;
        jazz_write_updates_per_transaction_total += batch_updates;
        jazz_write_updates_per_transaction_max =
            jazz_write_updates_per_transaction_max.max(batch_updates);
        add_write_detail(&mut jazz_write_detail, take_runtime_write_phase_stats());
        DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.jazz_write, jazz_write_started);
        write_only_ms += ms(write_started.elapsed());
        written_tx_ids.extend(tx_ids);
    }

    let total_loop_ms = ms(started.elapsed());
    let mut block_native_export_ms = None;
    let mut block_native_import_ms = None;
    let mut block_native_blocks = None;
    let mut block_native_payload_bytes = None;
    let mut native_sync_bytes = None;
    let mut native_export_ms = None;
    let mut native_import_ms = None;
    if let Some(hot_tail) = input.compact_hot_tail {
        let compaction_started = Instant::now();
        let compaction = if let Some(max_rows_per_block) = input.compact_max_rows_per_block {
            writer.compact_history_with_policy(
                HistoryCompactionPolicy::accepted_only(hot_tail, hot_tail)
                    .with_max_rows_per_block(max_rows_per_block),
            )?
        } else {
            writer.compact_table_accepted_history(input.table, hot_tail, hot_tail)?
        };
        let compaction_ms = ms(compaction_started.elapsed());
        phase_ms.compaction += compaction_ms;
        if let Some(max_rows_per_block) = input.compact_max_rows_per_block {
            notes.push(format!(
                "History block row cap: at most {max_rows_per_block} sealed history rows per block."
            ));
        }
        notes.push(format!(
            "Table history block compaction: hot tail {}, sealed rows {}, blocks {}, sealed tx rows {}, uncompressed bytes {}, compressed bytes {}, compaction {:.2} ms.",
            hot_tail,
            compaction.sealed_history_rows,
            compaction.history_blocks,
            compaction.sealed_transactions,
            compaction.uncompressed_bytes,
            compaction.compressed_bytes,
            compaction_ms
        ));
        if input.reclaim_after_compact {
            let reclaim_started = Instant::now();
            writer.reclaim_storage()?;
            let reclaim_ms = ms(reclaim_started.elapsed());
            phase_ms.reclaim += reclaim_ms;
            notes.push(format!(
                "SQLite storage reclaim after compaction: {:.2} ms.",
                reclaim_ms
            ));
        }
        let block_export_started = Instant::now();
        let delta = writer.export_table_history_delta(input.table, &[])?;
        block_native_export_ms = Some(ms(block_export_started.elapsed()));
        phase_ms.native_export += block_native_export_ms.unwrap_or(0.0);
        let delta_bundle_summary = BundleSummary::from(&delta.bundle)?;
        block_native_blocks = Some(delta.blocks.len());
        block_native_payload_bytes = Some(
            delta
                .blocks
                .iter()
                .map(|block| block.payload.len())
                .sum::<usize>(),
        );
        notes.push(format!(
            "Block-native table delta: open bundle bytes {}, block payload bytes {}.",
            delta_bundle_summary.bytes,
            block_native_payload_bytes.unwrap_or(0)
        ));
        native_sync_bytes =
            Some(delta_bundle_summary.bytes + block_native_payload_bytes.unwrap_or(0));
        native_export_ms = block_native_export_ms;
        let mut block_peer = Runtime::open_with_schema(
            Storage::Memory,
            "block-peer-node",
            "bob",
            input.schema.clone(),
        )?;
        let block_import_started = Instant::now();
        block_peer.apply_history_delta(&delta.bundle, &delta.blocks)?;
        let block_rows = block_peer.read_rows(input.table)?;
        if block_rows.len() != 1 {
            return Err(format!(
                "expected one block-native imported row, got {}",
                block_rows.len()
            )
            .into());
        }
        block_native_import_ms = Some(ms(block_import_started.elapsed()));
        phase_ms.native_import_apply += block_native_import_ms.unwrap_or(0.0);
        native_import_ms = block_native_import_ms;
    }
    let bundle_export_started = Instant::now();
    let bundle = writer.export_table_history(input.table)?;
    let bundle_export_ms = ms(bundle_export_started.elapsed());
    phase_ms.final_export += bundle_export_ms;
    let bundle_summary = BundleSummary::from(&bundle)?;
    let cold_schema = input.schema.clone();
    let mut cold = Runtime::open_with_schema(Storage::Memory, "cold-node", "bob", cold_schema)?;
    let cold_started = Instant::now();
    let cold_apply_started = Instant::now();
    let profile = cold.profile_apply_bundle(&bundle)?;
    add_apply_profile(&mut cold_apply_profile, profile);
    DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.cold_apply, cold_apply_started);
    let cold_verify_started = Instant::now();
    let cold_rows = cold.read_rows(input.table)?;
    DeepHistoryPhaseReport::add_elapsed(&mut phase_ms.cold_read_and_verify, cold_verify_started);
    let cold_load_ms = ms(cold_started.elapsed());
    if cold_rows.len() != 1 {
        return Err(format!("expected one cold-loaded row, got {}", cold_rows.len()).into());
    }
    let current_started = Instant::now();
    let rows = writer.read_rows(input.table)?;
    let current_read_ms = ms(current_started.elapsed());
    phase_ms.current_read += current_read_ms;
    let final_payload = rows
        .first()
        .and_then(|row| row.values.get(input.field))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or(last_value);
    let final_payload_bytes = final_payload.len();
    let historical_epochs = historical_read_epochs_for_case(
        &bundle,
        &writer.history_block_manifests(input.table)?,
        input.row_id,
    );
    let historical_started = Instant::now();
    let mut historical_read_count = 0;
    for epoch in &historical_epochs {
        if writer
            .read_row_at_node_epoch(input.table, input.row_id, "writer-node", *epoch)?
            .is_some()
        {
            historical_read_count += 1;
        }
    }
    let historical_read_total_ms = ms(historical_started.elapsed());
    phase_ms.historical_reads += historical_read_total_ms;
    let tx_info_ids = sampled_tx_ids(&written_tx_ids);
    let tx_info_started = Instant::now();
    for tx_id in &tx_info_ids {
        writer.transaction_info(tx_id)?;
    }
    let transaction_info_total_ms = ms(tx_info_started.elapsed());
    phase_ms.transaction_info_reads += transaction_info_total_ms;
    let transaction_info_count = tx_info_ids.len();
    let stats = writer.storage_stats()?;
    drop(writer);
    let total_file_bytes = stats.total_file_bytes;
    let extrapolated_final_payload_bytes_for_target =
        if input.compare_to_final_payload && stopped_early && completed_updates > 0 {
            Some(
                (final_payload_bytes as f64 * input.target_updates as f64
                    / completed_updates as f64)
                    .round() as usize,
            )
        } else if input.compare_to_final_payload {
            Some(final_payload_bytes)
        } else {
            None
        };
    let extrapolated_write_ms_for_target = if stopped_early && completed_updates > 0 {
        Some(write_only_ms * input.target_updates as f64 / completed_updates as f64)
    } else {
        None
    };
    let extrapolated_database_bytes_for_target = if stopped_early && completed_updates > 0 {
        Some(
            (stats.database_bytes as f64 * input.target_updates as f64 / completed_updates as f64)
                as i64,
        )
    } else {
        None
    };
    if let Some(reference_json) = input.final_reference_json {
        notes.push(format!(
            "Reference uncompressed JSON bytes: {}",
            reference_json.len()
        ));
    }
    Ok(DeepHistoryCaseReport {
        target_updates: input.target_updates,
        completed_updates,
        stopped_early,
        stop_reason,
        elapsed_write_ms: total_loop_ms,
        average_write_ms: if completed_updates == 0 {
            0.0
        } else {
            total_loop_ms / completed_updates as f64
        },
        total_loop_ms,
        write_only_ms,
        average_write_only_ms: if completed_updates == 0 {
            0.0
        } else {
            write_only_ms / completed_updates as f64
        },
        sampled_receive_total_ms,
        live_receive_sample_count: receive_samples.len(),
        live_receive_average_ms: average(&receive_samples),
        live_receive_p50_ms: percentile(receive_samples.clone(), 0.50),
        live_receive_p95_ms: percentile(receive_samples.clone(), 0.95),
        live_receive_p99_ms: percentile(receive_samples.clone(), 0.99),
        live_receive_last_ms: receive_samples.last().copied(),
        cold_load_ms,
        current_read_ms,
        historical_read_total_ms,
        historical_read_count,
        historical_read_average_ms: if historical_read_count == 0 {
            None
        } else {
            Some(historical_read_total_ms / historical_read_count as f64)
        },
        transaction_info_total_ms,
        transaction_info_count,
        transaction_info_average_ms: if transaction_info_count == 0 {
            None
        } else {
            Some(transaction_info_total_ms / transaction_info_count as f64)
        },
        final_payload_bytes,
        extrapolated_final_payload_bytes_for_target,
        reference_gzip_bytes: input.reference_gzip_bytes,
        bundle_bytes: bundle_summary.bytes,
        native_sync_bytes: native_sync_bytes.unwrap_or(bundle_summary.bytes),
        native_export_ms: native_export_ms.unwrap_or(bundle_export_ms),
        native_import_ms: native_import_ms.unwrap_or(cold_load_ms),
        block_native_export_ms,
        block_native_import_ms,
        block_native_blocks,
        block_native_payload_bytes,
        database_bytes: stats.database_bytes,
        live_database_bytes: stats.live_database_bytes,
        freelist_bytes: stats.freelist_bytes,
        total_file_bytes,
        history_rows: stats.history_rows,
        current_rows: stats.current_rows,
        database_to_final_payload_ratio: if input.compare_to_final_payload {
            ratio_i64_usize(stats.database_bytes, final_payload_bytes)
        } else {
            None
        },
        live_database_to_final_payload_ratio: if input.compare_to_final_payload {
            ratio_i64_usize(stats.live_database_bytes, final_payload_bytes)
        } else {
            None
        },
        total_file_to_final_payload_ratio: if input.compare_to_final_payload {
            ratio_i64_usize(total_file_bytes, final_payload_bytes)
        } else {
            None
        },
        database_to_extrapolated_final_payload_ratio: extrapolated_final_payload_bytes_for_target
            .and_then(|bytes| ratio_i64_usize(stats.database_bytes, bytes)),
        live_database_to_extrapolated_final_payload_ratio:
            extrapolated_final_payload_bytes_for_target
                .and_then(|bytes| ratio_i64_usize(stats.live_database_bytes, bytes)),
        total_file_to_extrapolated_final_payload_ratio: extrapolated_final_payload_bytes_for_target
            .and_then(|bytes| ratio_i64_usize(total_file_bytes, bytes)),
        database_to_reference_gzip_ratio: input
            .reference_gzip_bytes
            .and_then(|bytes| ratio_i64_usize(stats.database_bytes, bytes)),
        live_database_to_reference_gzip_ratio: input
            .reference_gzip_bytes
            .and_then(|bytes| ratio_i64_usize(stats.live_database_bytes, bytes)),
        bundle_to_reference_gzip_ratio: input
            .reference_gzip_bytes
            .and_then(|bytes| ratio_usize(bundle_summary.bytes, bytes)),
        extrapolated_write_ms_for_target,
        extrapolated_database_bytes_for_target,
        phase_ms: DeepHistoryPhaseReport {
            jazz_write_detail,
            live_apply_profile,
            cold_apply_profile,
            jazz_write_sqlite_transactions,
            jazz_write_updates_per_transaction_avg: if jazz_write_sqlite_transactions == 0 {
                None
            } else {
                Some(
                    jazz_write_updates_per_transaction_total as f64
                        / jazz_write_sqlite_transactions as f64,
                )
            },
            jazz_write_updates_per_transaction_max,
            ..phase_ms
        },
        notes,
    })
}

#[derive(Deserialize)]
struct AutomergeTrace {
    #[serde(rename = "startContent")]
    start_content: String,
    txns: Vec<AutomergeTxn>,
}

#[derive(Deserialize)]
struct AutomergeTxn {
    patches: Vec<(usize, usize, String)>,
}

fn apply_automerge_patches(
    state: &mut String,
    patches: &[(usize, usize, String)],
) -> BenchResult<()> {
    for (pos, del, ins) in patches {
        let start = byte_index_for_char(state, *pos)?;
        let end = byte_index_for_char(state, pos + del)?;
        state.replace_range(start..end, ins);
    }
    Ok(())
}

fn byte_index_for_char(value: &str, char_index: usize) -> BenchResult<usize> {
    if char_index == value.chars().count() {
        return Ok(value.len());
    }
    value
        .char_indices()
        .nth(char_index)
        .map(|(idx, _)| idx)
        .ok_or_else(|| format!("edit position {char_index} out of bounds").into())
}

fn automerge_trace_path() -> BenchResult<std::path::PathBuf> {
    if let Ok(path) = env::var("MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_TRACE") {
        return Ok(path.into());
    }
    let path = std::env::temp_dir().join("mini-jazz-automerge-paper.json.gz");
    if path.exists() {
        return Ok(path);
    }
    let status = Command::new("curl")
        .args([
            "-L",
            "-sS",
            "--fail",
            "https://raw.githubusercontent.com/josephg/diamond-types/master/benchmark_data/automerge-paper.json.gz",
            "-o",
        ])
        .arg(&path)
        .status()?;
    if !status.success() {
        return Err("failed to download automerge paper trace".into());
    }
    Ok(path)
}

fn gzip_decode_to_string(path: &std::path::Path) -> BenchResult<String> {
    let output = Command::new("gzip").arg("-dc").arg(path).output()?;
    if !output.status.success() {
        return Err(format!("gzip -dc failed for {}", path.display()).into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn gzip_bytes(input: &[u8]) -> BenchResult<usize> {
    let mut child = Command::new("gzip")
        .arg("-c")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    child.stdin.as_mut().unwrap().write_all(input)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err("gzip -c failed".into());
    }
    Ok(output.stdout.len())
}

fn canvas_position_json(frame: usize) -> String {
    let t = frame as f64 / 60.0;
    let x = 500.0 + (t * 1.7).sin() * 320.0 + t * 0.05;
    let y = 300.0 + (t * 2.3).cos() * 180.0;
    serde_json::json!({ "x": x, "y": y }).to_string()
}

fn row_root_id(rows: &[mini_jazz_sqlite::RowView], field: &str) -> BenchResult<i64> {
    rows.first()
        .and_then(|row| row.values.get(field))
        .and_then(Value::as_str)
        .ok_or_else(|| "missing text op root field".into())
        .and_then(|value| value.parse::<i64>().map_err(Into::into))
}

fn text_document_schema() -> SchemaDef {
    SchemaDef::new().table("documents", |table| {
        table.text("body");
    })
}

fn map1(key: &str, value: serde_json::Value) -> BTreeMap<String, serde_json::Value> {
    let mut map = BTreeMap::new();
    map.insert(key.to_owned(), value);
    map
}

fn map2(
    key1: &str,
    value1: serde_json::Value,
    key2: &str,
    value2: serde_json::Value,
) -> BTreeMap<String, serde_json::Value> {
    let mut map = BTreeMap::new();
    map.insert(key1.to_owned(), value1);
    map.insert(key2.to_owned(), value2);
    map
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_to_bytes(value: &str) -> BenchResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err("hex bytes value has odd length".into());
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let raw = value.as_bytes();
    for idx in (0..raw.len()).step_by(2) {
        let high = hex_nibble(raw[idx])?;
        let low = hex_nibble(raw[idx + 1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> BenchResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("invalid hex bytes value".into()),
    }
}

fn average(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn historical_read_epochs_for_case(
    bundle: &Bundle,
    block_manifests: &[HistoryBlockManifest],
    row_id: &str,
) -> Vec<i64> {
    let tx_epochs = bundle
        .txs
        .iter()
        .map(|tx| (tx.tx_id.as_str(), tx.local_epoch))
        .collect::<BTreeMap<_, _>>();
    let mut row_epochs = bundle
        .history
        .iter()
        .filter(|record| record.row_id == row_id)
        .filter_map(|record| tx_epochs.get(record.tx_id.as_str()).copied())
        .collect::<Vec<_>>();
    for manifest in block_manifests
        .iter()
        .filter(|manifest| manifest.row_id == row_id && manifest.kind == "accepted")
    {
        row_epochs.push(manifest.min_global_epoch);
        row_epochs.push(manifest.max_global_epoch);
    }
    if row_epochs.is_empty() {
        return Vec::new();
    }
    let first = *row_epochs.iter().min().unwrap_or(&1);
    let latest = *row_epochs.iter().max().unwrap_or(&first);
    let mut epochs = vec![first, ((first + latest) / 2).max(first), latest];
    epochs.sort_unstable();
    epochs.dedup();
    epochs
}

fn sampled_tx_ids(tx_ids: &[String]) -> Vec<String> {
    if tx_ids.is_empty() {
        return Vec::new();
    }
    let mut indexes = vec![0, tx_ids.len() / 2, tx_ids.len() - 1];
    indexes.sort_unstable();
    indexes.dedup();
    indexes
        .into_iter()
        .filter_map(|index| tx_ids.get(index).cloned())
        .collect()
}

fn sampled_tx_ids_from_bundle(bundle: &Bundle) -> Vec<String> {
    let tx_ids = bundle
        .txs
        .iter()
        .map(|tx| tx.tx_id.clone())
        .collect::<Vec<_>>();
    sampled_tx_ids(&tx_ids)
}

fn percentile(mut values: Vec<f64>, percentile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let idx = ((values.len() - 1) as f64 * percentile).round() as usize;
    values.get(idx).copied()
}

fn ratio_i64_usize(numerator: i64, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

fn ratio_usize(numerator: usize, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

fn profile_apply_bundles(
    runtime: &mut Runtime,
    bundles: Vec<Bundle>,
) -> Result<ApplyBundleProfile> {
    let mut aggregate = ApplyBundleProfile {
        total_ms: 0.0,
        validation_ms: 0.0,
        begin_tx_ms: 0.0,
        branches_ms: 0.0,
        txs_ms: 0.0,
        reads_ms: 0.0,
        rejected_cleanup_ms: 0.0,
        query_reads_ms: 0.0,
        history_ms: 0.0,
        query_scope_repair_ms: 0.0,
        commit_ms: 0.0,
        revalidate_awaiting_ms: 0.0,
        branch_rows: 0,
        tx_rows: 0,
        read_rows: 0,
        query_read_rows: 0,
        history_rows: 0,
    };
    for bundle in bundles {
        let profile = runtime.profile_apply_bundle(&bundle)?;
        aggregate.total_ms += profile.total_ms;
        aggregate.validation_ms += profile.validation_ms;
        aggregate.begin_tx_ms += profile.begin_tx_ms;
        aggregate.branches_ms += profile.branches_ms;
        aggregate.txs_ms += profile.txs_ms;
        aggregate.reads_ms += profile.reads_ms;
        aggregate.rejected_cleanup_ms += profile.rejected_cleanup_ms;
        aggregate.query_reads_ms += profile.query_reads_ms;
        aggregate.history_ms += profile.history_ms;
        aggregate.query_scope_repair_ms += profile.query_scope_repair_ms;
        aggregate.commit_ms += profile.commit_ms;
        aggregate.revalidate_awaiting_ms += profile.revalidate_awaiting_ms;
        aggregate.branch_rows += profile.branch_rows;
        aggregate.tx_rows += profile.tx_rows;
        aggregate.read_rows += profile.read_rows;
        aggregate.query_read_rows += profile.query_read_rows;
        aggregate.history_rows += profile.history_rows;
    }
    Ok(aggregate)
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn sqlite_database_bytes(conn: &Connection) -> Result<i64> {
    let page_count: i64 = conn.pragma_query_value(None, "page_count", |row| row.get(0))?;
    let page_size: i64 = conn.pragma_query_value(None, "page_size", |row| row.get(0))?;
    Ok(page_count * page_size)
}

fn process_rss_bytes() -> Option<i64> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let rss_kib = String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .parse::<i64>()
        .ok()?;
    Some(rss_kib * 1024)
}

fn env_usize(name: &str, default: usize) -> usize {
    env_optional_usize(name).unwrap_or(default)
}

fn env_optional_usize(name: &str) -> Option<usize> {
    env::var(name).ok().and_then(|value| value.parse().ok())
}

fn deep_history_sample_every(specific_name: &str, default: usize) -> usize {
    env_optional_usize(specific_name)
        .or_else(|| env_optional_usize("MINI_JAZZ_DEEP_HISTORY_SAMPLE_EVERY"))
        .unwrap_or(default)
        .max(1)
}

fn deep_history_write_batch_size() -> Option<usize> {
    env_optional_usize("MINI_JAZZ_DEEP_HISTORY_WRITE_BATCH_SIZE").filter(|size| *size > 1)
}

fn deep_history_sqlite_tx_batch_window() -> Duration {
    Duration::from_millis(env_usize("MINI_JAZZ_DEEP_HISTORY_SQLITE_TX_BATCH_MS", 10) as u64)
}

fn deep_history_max_rows_per_block() -> Option<usize> {
    env_optional_usize("MINI_JAZZ_DEEP_HISTORY_MAX_ROWS_PER_BLOCK").filter(|size| *size > 0)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| match value.as_str() {
            "1" | "true" | "TRUE" | "yes" => Some(true),
            "0" | "false" | "FALSE" | "no" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

struct SelfSample {
    output: Option<String>,
    label: String,
    seconds: String,
    interval_ms: usize,
    warmup_ms: usize,
    child: Option<Child>,
}

impl SelfSample {
    fn from_env(label: impl Into<String>) -> BenchResult<Self> {
        Ok(Self {
            output: env::var("MINI_JAZZ_PERF_SELF_SAMPLE_WRITE").ok(),
            label: label.into(),
            seconds: env::var("MINI_JAZZ_PERF_SELF_SAMPLE_SECONDS")
                .unwrap_or_else(|_| "5".to_owned()),
            interval_ms: env_usize("MINI_JAZZ_PERF_SELF_SAMPLE_INTERVAL_MS", 1).max(1),
            warmup_ms: env_usize("MINI_JAZZ_PERF_SELF_SAMPLE_WARMUP_MS", 0),
            child: None,
        })
    }

    fn is_enabled(&self) -> bool {
        self.output.is_some() || self.child.is_some()
    }

    fn start_after_first_sample(&mut self) -> BenchResult<()> {
        let Some(output) = self.output.take() else {
            return Ok(());
        };
        let pid = std::process::id().to_string();
        eprintln!(
            "MINI_JAZZ_PERF_SELF_SAMPLE_START label={} pid={} seconds={} interval_ms={} output={}",
            self.label, pid, self.seconds, self.interval_ms, output
        );
        let child = Command::new("sample")
            .arg(pid)
            .arg(&self.seconds)
            .arg(self.interval_ms.to_string())
            .arg("-mayDie")
            .arg("-file")
            .arg(&output)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        self.child = Some(child);
        if self.warmup_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.warmup_ms as u64));
        }
        Ok(())
    }

    fn finish(&mut self) -> BenchResult<()> {
        if let Some(mut child) = self.child.take() {
            let status = child.wait()?;
            eprintln!(
                "MINI_JAZZ_PERF_SELF_SAMPLE_DONE label={} status={}",
                self.label, status
            );
        }
        Ok(())
    }
}

fn median_f64(mut values: Vec<f64>) -> f64 {
    values.sort_by(|left, right| left.total_cmp(right));
    values[values.len() / 2]
}

fn median_i64(mut values: Vec<i64>) -> i64 {
    values.sort();
    values[values.len() / 2]
}

fn median_usize(mut values: Vec<usize>) -> usize {
    values.sort();
    values[values.len() / 2]
}

fn median_apply_profile(profiles: Vec<ApplyBundleProfile>) -> ApplyBundleProfile {
    ApplyBundleProfile {
        total_ms: median_f64(profiles.iter().map(|profile| profile.total_ms).collect()),
        validation_ms: median_f64(
            profiles
                .iter()
                .map(|profile| profile.validation_ms)
                .collect(),
        ),
        begin_tx_ms: median_f64(profiles.iter().map(|profile| profile.begin_tx_ms).collect()),
        branches_ms: median_f64(profiles.iter().map(|profile| profile.branches_ms).collect()),
        txs_ms: median_f64(profiles.iter().map(|profile| profile.txs_ms).collect()),
        reads_ms: median_f64(profiles.iter().map(|profile| profile.reads_ms).collect()),
        rejected_cleanup_ms: median_f64(
            profiles
                .iter()
                .map(|profile| profile.rejected_cleanup_ms)
                .collect(),
        ),
        query_reads_ms: median_f64(
            profiles
                .iter()
                .map(|profile| profile.query_reads_ms)
                .collect(),
        ),
        history_ms: median_f64(profiles.iter().map(|profile| profile.history_ms).collect()),
        query_scope_repair_ms: median_f64(
            profiles
                .iter()
                .map(|profile| profile.query_scope_repair_ms)
                .collect(),
        ),
        commit_ms: median_f64(profiles.iter().map(|profile| profile.commit_ms).collect()),
        revalidate_awaiting_ms: median_f64(
            profiles
                .iter()
                .map(|profile| profile.revalidate_awaiting_ms)
                .collect(),
        ),
        branch_rows: median_usize(profiles.iter().map(|profile| profile.branch_rows).collect()),
        tx_rows: median_usize(profiles.iter().map(|profile| profile.tx_rows).collect()),
        read_rows: median_usize(profiles.iter().map(|profile| profile.read_rows).collect()),
        query_read_rows: median_usize(
            profiles
                .iter()
                .map(|profile| profile.query_read_rows)
                .collect(),
        ),
        history_rows: median_usize(
            profiles
                .iter()
                .map(|profile| profile.history_rows)
                .collect(),
        ),
    }
}
