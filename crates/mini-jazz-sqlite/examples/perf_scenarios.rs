use mini_jazz_sqlite::sync::Bundle;
use mini_jazz_sqlite::{Result, RowDiff, RowsSubscription, Runtime, SchemaDef, Storage};
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::time::{Duration, Instant};
use tempfile::tempdir;

const OWNER: &str = "alice";
type BenchResult<T> = std::result::Result<T, Box<dyn Error>>;

fn main() -> BenchResult<()> {
    let config = Config::from_env();
    let report = run_core_only_scoped_page(&config)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
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
    edge_database_bytes: i64,
    worker_database_bytes: i64,
    tab_database_bytes: i64,
    seed_ms: f64,
    core_query_ms: f64,
    export_ms: f64,
    core_to_edge_apply_ms: f64,
    edge_to_worker_apply_ms: f64,
    worker_to_tab_apply_ms: f64,
    tab_query_ms: f64,
    api_to_first_result_ms: f64,
    refresh_after_new_top_rows: RefreshReport,
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
    edge_to_worker_apply_ms: f64,
    worker_to_tab_apply_ms: f64,
    tab_query_ms: f64,
    tab_subscription_poll_ms: f64,
    tab_subscription_added: usize,
    tab_subscription_updated: usize,
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
    let mut tab = Runtime::open_with_schema(Storage::Memory, "tab", OWNER, schema)?;

    let seed_started = Instant::now();
    seed_documents(
        &mut core,
        config.total_rows,
        config.target_owner_rows,
        config.seed_batch_size,
    )?;
    let seed_elapsed = seed_started.elapsed();

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

    let export_started = Instant::now();
    let core_bundle = export_top_owner_page(&mut core, config.page_size)?;
    let export_elapsed = export_started.elapsed();

    let core_bundle_summary = BundleSummary::from(&core_bundle)?;

    let edge_apply_elapsed = timed(|| edge.apply_bundle(&core_bundle))?;
    let edge_bundle = export_top_owner_page(&mut edge, config.page_size)?;
    let worker_apply_elapsed = timed(|| worker.apply_bundle(&edge_bundle))?;
    let worker_bundle = export_top_owner_page(&mut worker, config.page_size)?;
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
    let core_database_bytes = core.storage_stats()?.database_bytes;

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
        edge_database_bytes: edge.storage_stats()?.database_bytes,
        worker_database_bytes: worker.storage_stats()?.database_bytes,
        tab_database_bytes: tab.storage_stats()?.database_bytes,
        seed_ms: ms(seed_elapsed),
        core_query_ms: ms(core_query_elapsed),
        export_ms: ms(export_elapsed),
        core_to_edge_apply_ms: ms(edge_apply_elapsed),
        edge_to_worker_apply_ms: ms(worker_apply_elapsed),
        worker_to_tab_apply_ms: ms(tab_apply_elapsed),
        tab_query_ms: ms(tab_query_elapsed),
        api_to_first_result_ms: ms(export_elapsed
            + edge_apply_elapsed
            + worker_apply_elapsed
            + tab_apply_elapsed
            + tab_query_elapsed),
        refresh_after_new_top_rows,
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
    let edge_bundles = edge.run_as_user(OWNER, |edge| {
        edge.export_query_read_refreshes(&worker_reads)
    })?;
    let worker_apply_elapsed = timed_apply_bundles(worker, edge_bundles)?;
    let worker_bundles = worker.export_query_read_refreshes(&tab.observed_query_reads()?)?;
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
        edge_to_worker_apply_ms: ms(worker_apply_elapsed),
        worker_to_tab_apply_ms: ms(tab_apply_elapsed),
        tab_query_ms: ms(tab_query_elapsed),
        tab_subscription_poll_ms: ms(subscription_poll_elapsed),
        tab_subscription_added: diff_counts.added,
        tab_subscription_updated: diff_counts.updated,
        tab_subscription_removed: diff_counts.removed,
        api_to_updated_result_ms: ms(export_elapsed
            + edge_apply_elapsed
            + worker_apply_elapsed
            + tab_apply_elapsed
            + tab_query_elapsed),
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

struct DiffCounts {
    added: usize,
    updated: usize,
    removed: usize,
}

impl DiffCounts {
    fn from(diffs: &[RowDiff]) -> Self {
        let mut counts = Self {
            added: 0,
            updated: 0,
            removed: 0,
        };
        for diff in diffs {
            match diff {
                RowDiff::Added(_) => counts.added += 1,
                RowDiff::Updated { .. } => counts.updated += 1,
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

fn export_top_owner_page(runtime: &mut Runtime, page_size: usize) -> Result<Bundle> {
    if runtime.is_trusted() {
        runtime.run_as_user(OWNER, |runtime| {
            runtime.export_query_where_eq_top_field_desc(
                "documents",
                "owner_id",
                json!(OWNER),
                "updated_at",
                page_size,
            )
        })
    } else {
        runtime.export_query_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!(OWNER),
            "updated_at",
            page_size,
        )
    }
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
            bytes: serde_json::to_vec(bundle)?.len(),
        })
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
            bytes += serde_json::to_vec(bundle)?.len();
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

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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
