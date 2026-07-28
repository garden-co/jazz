//! Multi-tenant SaaS permission and active-subscription benchmark.
//!
//! This harness uses only the public `jazz::db::Db` query/write surface. It
//! keeps row-count, customer fan-out, and people fan-out independently
//! configurable so their costs can be attributed instead of collapsed into one
//! maximum-size number.
//!
//! Example:
//!
//! ```text
//! JAZZ_SAAS_DOCUMENTS=10000 \
//! JAZZ_SAAS_ORGANIZATIONS=100 \
//! JAZZ_SAAS_TEAMS=1001 \
//! JAZZ_SAAS_HOT_DOCUMENTS=5000 \
//! JAZZ_SAAS_TEAM_MEMBERS_PER_TEAM=5 \
//! JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS=100 \
//! cargo bench --profile perf -p jazz-tools --features saas-permission-bench \
//!   --bench saas_permission_fanout --quiet
//! ```

mod saas_fanout_fixture;
mod saas_fanout_oracle;
mod saas_permission_support;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionStream, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::query::{OrderDirection, Query, col, eq, in_list, lit, param};
use jazz::tx::DurabilityTier;
use saas_fanout_fixture::{
    AccessPath, Config, DocumentSpec, DocumentStatus as FixtureDocumentStatus, Fixture, Profile,
    SeedReport, SubscriberPlan,
};
use saas_fanout_oracle::{
    DocumentMetadata, DocumentStatus, ExpectedPage, ObservedPage, PageTransition,
    PerTeamTop100Oracle, StreamAuditTarget, drain_streams, take_initial_reset,
};
use serde::Serialize;
use serde_json::json;

type BenchDb = Db<MemoryStorage>;

const SYSTEM_NODE: NodeUuid = NodeUuid(uuid::uuid!("52000000-0000-0000-0000-000000000001"));
const SYSTEM_AUTHOR: AuthorId = AuthorId(uuid::uuid!("52000000-0000-0000-0000-000000000002"));

fn main() {
    match run() {
        Ok(output) => println!(
            "{}",
            serde_json::to_string_pretty(&output).expect("serialize benchmark output")
        ),
        Err(error) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "benchmark": "saas_permission_fanout",
                    "completed": false,
                    "ok": false,
                    "error": error,
                }))
                .expect("serialize benchmark error")
            );
            std::process::exit(1);
        }
    }
}

#[derive(Debug, Serialize)]
struct BenchmarkOutput {
    benchmark: &'static str,
    completed: bool,
    ok: bool,
    config: Config,
    distribution: saas_fanout_fixture::DocumentDistribution,
    open_ms: f64,
    seeding: SeedReport,
    oracle_build_ms: f64,
    permission_mix: BTreeMap<&'static str, usize>,
    hydration: HydrationReport,
    writes: Vec<WritePhaseReport>,
    one_shot_canary: OneShotCanaryReport,
    runtime_after_hydration: RuntimeReport,
    runtime_after_writes: RuntimeReport,
    runtime_after_one_shot: RuntimeReport,
    local_subscription_footprint: LocalSubscriptionFootprintReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_churn: Option<SubscriptionChurnReport>,
    attribution: Vec<&'static str>,
    tooling_friction: &'static str,
}

#[derive(Debug, Serialize)]
struct HydrationReport {
    subscriptions: usize,
    unique_teams: usize,
    total_ms: f64,
    prepare_us: LatencyStats,
    subscribe_us: LatencyStats,
    first_subscribe_us: u64,
    last_subscribe_us: u64,
    initial_rows: usize,
    initial_reset_audit_ms: f64,
    post_bind_quiescence_audit_ms: f64,
    post_bind_events: usize,
    exact_initial_membership: bool,
}

#[derive(Clone, Debug, Serialize)]
struct LatencyStats {
    samples: usize,
    total_us: u64,
    mean_us: f64,
    min_us: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

#[derive(Clone, Debug)]
struct CommitSample {
    wall_us: u64,
    storage_us: u64,
    initial_commit_ivm_tick_us: u64,
    jazz_residual_us: u64,
    initial_commit_groove_notifications: usize,
    initial_commit_groove_notification_records: usize,
    initial_commit_groove_notification_bytes: usize,
}

#[derive(Debug, Serialize)]
struct WritePhaseReport {
    name: &'static str,
    transactions: usize,
    rows: usize,
    commit_wall_us: LatencyStats,
    groove_storage_us: LatencyStats,
    groove_initial_commit_ivm_tick_us: LatencyStats,
    jazz_facade_and_subscription_refresh_residual_us: LatencyStats,
    groove_initial_commit_notifications: usize,
    groove_initial_commit_notification_records: usize,
    groove_initial_commit_notification_bytes: usize,
    stream_drain_ms: f64,
    oracle_validation_ms: f64,
    streams_scanned: usize,
    expected_notified_streams: usize,
    actual_notified_streams: usize,
    events: usize,
    added_rows: usize,
    updated_rows: usize,
    removed_rows: usize,
    resets: usize,
    exact_oracle_match: bool,
}

#[derive(Debug, Serialize)]
struct RuntimeReport {
    graph_nodes: usize,
    active_subscriptions: usize,
    active_prepared_shapes: usize,
    active_shape_params: usize,
    arrangement_count: usize,
    arrangement_rows: usize,
    arrangement_encoded_bytes: usize,
    eval_memo_entries: usize,
    eval_memo_bytes: usize,
    hydration_memo_entries: usize,
    hydration_memo_hits: u64,
    hydration_memo_computes: u64,
    logical_nodes_requested: u64,
    deduped_graph_nodes: usize,
    dedupe_ratio: f64,
}

#[derive(Debug, Serialize)]
struct LocalSubscriptionFootprintReport {
    subscriptions: usize,
    root_rows: usize,
    snapshot_bytes: usize,
    reset_frame_bytes: usize,
    validation_tuple_estimate_bytes: usize,
    approximate_private_maintained_heap_bytes: usize,
    approximate_private_control_state_bytes: usize,
    approximate_private_total_heap_bytes: usize,
    mean_approximate_private_total_heap_bytes_per_subscription: f64,
}

#[derive(Debug, Serialize)]
struct SubscriptionChurnReport {
    dropped_subscriptions: usize,
    remaining_subscriptions: usize,
    query_irrelevant_organization_index: usize,
    reaping_document_team_index: usize,
    before_drop: SubscriptionLifecycleCounts,
    immediately_after_drop: SubscriptionLifecycleCounts,
    query_irrelevant_organization_update: WritePhaseReport,
    after_query_irrelevant_update: SubscriptionLifecycleCounts,
    unbound_team_document_write: WritePhaseReport,
    after_unbound_team_document_write: SubscriptionLifecycleCounts,
    stale_outputs_created_by_drop: usize,
    stale_outputs_remaining_after_irrelevant_update: usize,
    stale_outputs_reaped_by_irrelevant_update: usize,
    stale_outputs_reaped_by_document_write: usize,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct SubscriptionLifecycleCounts {
    jazz_live_subscriptions: usize,
    groove_active_subscriptions: usize,
    groove_active_prepared_shapes: usize,
    groove_active_shape_params: usize,
    groove_graph_nodes: usize,
    stale_groove_subscriptions: usize,
}

#[derive(Debug, Serialize)]
struct OneShotCanaryReport {
    checked: usize,
    passed: usize,
    failed: usize,
    ordered_exact: bool,
    failures: Vec<String>,
}

struct ActiveSubscription {
    label: String,
    plan: SubscriberPlan,
    stream: SubscriptionStream,
    observed: ObservedPage,
    expected: ExpectedPage,
    full_document_access: bool,
}

fn run() -> Result<BenchmarkOutput, String> {
    let config = Config::from_env()?;
    let fixture = Fixture::build(config.clone())?;
    let distribution = fixture.distribution().clone();

    let open_started = Instant::now();
    let db = open_db(fixture.schema())?;
    let open_ms = millis(open_started.elapsed());

    eprintln!(
        "seeding profile={:?}, documents={}, teams={}, active_subscriptions={}",
        config.profile, config.documents, config.teams, config.active_subscriptions
    );
    let seeding = fixture.seed_local(&db)?;

    let oracle_started = Instant::now();
    let mut oracle = build_oracle(&fixture);
    let oracle_build_ms = millis(oracle_started.elapsed());

    let query = document_list_query();
    let read_opts = local_read_opts();
    let (mut subscriptions, hydration) = open_subscriptions(&db, &fixture, &query, &read_opts)?;
    let runtime_after_hydration = runtime_report(&db);
    let local_subscription_footprint = local_subscription_footprint_report(&db);
    let team_subscribers = subscribers_by_team(&subscriptions);

    let mut writes = Vec::new();
    let mut next_document_index = fixture.next_document_index();

    let target_team = subscriptions
        .first()
        .ok_or_else(|| "fixture generated no subscriptions".to_owned())?
        .plan
        .team_index;

    if config.matching_writes > 0 {
        let mut expected_events = empty_expected_events(subscriptions.len());
        let mut samples = Vec::with_capacity(config.matching_writes);
        for _ in 0..config.matching_writes {
            let document =
                next_private_list_document(&fixture, &mut next_document_index, target_team, None);
            samples.push(commit_documents(&db, &[document])?);
            apply_single_document(
                document,
                &mut oracle,
                &mut subscriptions,
                &team_subscribers,
                &mut expected_events,
            );
        }
        writes.push(audit_write_phase(
            "matching_team_separate_transactions",
            config.matching_writes,
            config.matching_writes,
            samples,
            expected_events,
            &mut subscriptions,
        )?);
    }

    if let Some(unbound_team) = first_unbound_team(config.teams, &team_subscribers)
        && config.unrelated_writes > 0
    {
        let expected_events = empty_expected_events(subscriptions.len());
        let mut samples = Vec::with_capacity(config.unrelated_writes);
        for _ in 0..config.unrelated_writes {
            let document =
                next_private_list_document(&fixture, &mut next_document_index, unbound_team, None);
            samples.push(commit_documents(&db, &[document])?);
        }
        writes.push(audit_write_phase(
            "unsubscribed_team_separate_transactions",
            config.unrelated_writes,
            config.unrelated_writes,
            samples,
            expected_events,
            &mut subscriptions,
        )?);
    }

    if config.batched_write_rows > 0 {
        let documents = (0..config.batched_write_rows)
            .map(|_| {
                next_private_list_document(&fixture, &mut next_document_index, target_team, None)
            })
            .collect::<Vec<_>>();
        let sample = commit_documents(&db, &documents)?;
        let mut expected_events = empty_expected_events(subscriptions.len());
        apply_document_batch(
            &documents,
            &mut oracle,
            &mut subscriptions,
            &team_subscribers,
            &mut expected_events,
        );
        writes.push(audit_write_phase(
            "matching_team_single_batch",
            1,
            documents.len(),
            vec![sample],
            expected_events,
            &mut subscriptions,
        )?);
    }

    let spread_teams = team_subscribers
        .keys()
        .copied()
        .take(config.batched_write_rows.min(100))
        .collect::<Vec<_>>();
    if spread_teams.len() > 1 {
        let documents = spread_teams
            .iter()
            .map(|team_index| {
                next_private_list_document(&fixture, &mut next_document_index, *team_index, None)
            })
            .collect::<Vec<_>>();
        let sample = commit_documents(&db, &documents)?;
        let mut expected_events = empty_expected_events(subscriptions.len());
        apply_document_batch(
            &documents,
            &mut oracle,
            &mut subscriptions,
            &team_subscribers,
            &mut expected_events,
        );
        writes.push(audit_write_phase(
            "one_transaction_spread_across_subscribed_teams",
            1,
            documents.len(),
            vec![sample],
            expected_events,
            &mut subscriptions,
        )?);
    }

    let boundary_document =
        next_private_list_document(&fixture, &mut next_document_index, target_team, Some(0));
    let boundary_sample = commit_documents(&db, &[boundary_document])?;
    let mut boundary_events = empty_expected_events(subscriptions.len());
    apply_single_document(
        boundary_document,
        &mut oracle,
        &mut subscriptions,
        &team_subscribers,
        &mut boundary_events,
    );
    writes.push(audit_write_phase(
        "matching_team_below_top100_boundary",
        1,
        1,
        vec![boundary_sample],
        boundary_events,
        &mut subscriptions,
    )?);

    let runtime_after_writes = runtime_report(&db);
    let one_shot_canary = validate_one_shot_samples(&db, &query, &read_opts, &subscriptions);
    let runtime_after_one_shot = runtime_report(&db);
    let ok = one_shot_canary.ordered_exact;
    let permission_mix = permission_mix(&subscriptions);
    let subscription_churn = measure_subscription_churn(
        &db,
        &fixture,
        &mut subscriptions,
        &mut next_document_index,
        config.drop_subscriptions,
    )?;

    Ok(BenchmarkOutput {
        benchmark: "saas_permission_fanout",
        completed: true,
        ok,
        config,
        distribution,
        open_ms,
        seeding,
        oracle_build_ms,
        permission_mix,
        hydration,
        writes,
        one_shot_canary,
        runtime_after_hydration,
        runtime_after_writes,
        runtime_after_one_shot,
        local_subscription_footprint,
        subscription_churn,
        attribution: vec![
            "commit_wall includes Groove storage + Groove IVM + Jazz policy/finalization/subscription refresh",
            "jazz_facade_and_subscription_refresh_residual is wall minus Groove storage and the initial commit IVM tick; repeated empty Groove ticks during subscription refresh are included in this residual",
            "Groove notification counters cover the initial commit tick only",
            "stream draining and oracle validation are timed separately and excluded from commit latency",
            "subscription reset membership and later add/remove deltas are checked against a fixture-derived oracle; one-shot reads separately check ordering",
        ],
        tooling_friction: "A direct timer around Db::refresh_subscriptions would separate snapshot cloning from other Jazz facade work.",
    })
}

fn open_db(schema: jazz::schema::JazzSchema) -> Result<BenchDb, String> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let config = DbConfig::new(
        schema,
        MemoryStorage::new(&refs),
        DbIdentity {
            node: SYSTEM_NODE,
            author: SYSTEM_AUTHOR,
        },
    )
    .with_id_source(SeededRowIdSource::new(0x5aa6));
    block_on(Db::open(config)).map_err(|error| format!("open benchmark db: {error}"))
}

fn local_read_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn document_list_query() -> Query {
    Query::from(saas_permission_support::DOCUMENTS)
        .filter(eq(col("team"), param("team")))
        .filter(eq(col("archived"), lit(false)))
        .filter(in_list(col("status"), [lit("active"), lit("draft")]))
        .order_by("updated_at", OrderDirection::Desc)
        .order_by("id", OrderDirection::Desc)
        .limit(saas_fanout_oracle::TOP_PAGE_SIZE)
}

fn build_oracle(fixture: &Fixture) -> PerTeamTop100Oracle {
    let teams = fixture
        .subscribers()
        .iter()
        .map(|subscriber| subscriber.team_index)
        .collect::<BTreeSet<_>>();
    let mut oracle = PerTeamTop100Oracle::new();
    for team_index in teams {
        for local_index in 0..fixture.distribution().count(team_index) {
            let document = fixture
                .document(team_index, local_index)
                .expect("fixture document index is valid");
            oracle.upsert(oracle_document(document));
        }
    }
    oracle
}

fn open_subscriptions(
    db: &BenchDb,
    fixture: &Fixture,
    query: &Query,
    read_opts: &ReadOpts,
) -> Result<(Vec<ActiveSubscription>, HydrationReport), String> {
    let started = Instant::now();
    let mut prepare_us = Vec::with_capacity(fixture.subscribers().len());
    let mut subscribe_us = Vec::with_capacity(fixture.subscribers().len());
    let mut initial_rows = 0;
    let mut initial_audit_us = 0_u64;
    let mut active = Vec::with_capacity(fixture.subscribers().len());
    let mut shape_id = None;

    for plan in fixture.subscribers().iter().cloned() {
        db.set_identity_claims(plan.identity, plan.claims.clone());
        let prepare_started = Instant::now();
        let prepared = db
            .prepare_query_bound(
                query,
                BTreeMap::from([("team".to_owned(), Value::Uuid(plan.team.0))]),
            )
            .map_err(|error| format!("prepare subscription {}: {error}", plan.index))?;
        prepare_us.push(duration_micros(prepare_started.elapsed()));
        match shape_id {
            Some(expected) if expected != prepared.shape().shape_id() => {
                return Err("fan-out subscriptions did not share one query shape".to_owned());
            }
            None => shape_id = Some(prepared.shape().shape_id()),
            _ => {}
        }

        let subscribe_started = Instant::now();
        let mut stream =
            block_on(db.subscribe_for_identity(&prepared, read_opts.clone(), plan.identity))
                .map_err(|error| format!("subscribe {}: {error}", plan.index))?;
        subscribe_us.push(duration_micros(subscribe_started.elapsed()));

        let audit_started = Instant::now();
        let label = format!("subscription-{}", plan.index);
        let initial = take_initial_reset(&label, &mut stream, plan.expected_page())?;
        initial_audit_us += duration_micros(audit_started.elapsed());
        initial_rows += initial.observed.len();
        let expected = ExpectedPage::new(plan.expected_page().iter().copied())?;
        active.push(ActiveSubscription {
            label,
            full_document_access: has_full_document_access(fixture.config().profile, &plan),
            plan,
            stream,
            observed: initial.observed,
            expected,
        });
    }

    let post_bind_started = Instant::now();
    let post_bind = drain_all(&mut active)?;
    let post_bind_audit_ms = millis(post_bind_started.elapsed());
    post_bind.assert_quiet("post-bind quiescence")?;

    let total_ms = millis(started.elapsed());
    let first_subscribe_us = subscribe_us.first().copied().unwrap_or_default();
    let last_subscribe_us = subscribe_us.last().copied().unwrap_or_default();
    Ok((
        active,
        HydrationReport {
            subscriptions: fixture.subscribers().len(),
            unique_teams: fixture
                .subscribers()
                .iter()
                .map(|subscriber| subscriber.team_index)
                .collect::<BTreeSet<_>>()
                .len(),
            total_ms,
            prepare_us: latency_stats(&prepare_us),
            subscribe_us: latency_stats(&subscribe_us),
            first_subscribe_us,
            last_subscribe_us,
            initial_rows,
            initial_reset_audit_ms: initial_audit_us as f64 / 1_000.0,
            post_bind_quiescence_audit_ms: post_bind_audit_ms,
            post_bind_events: post_bind.total_events,
            exact_initial_membership: true,
        },
    ))
}

fn has_full_document_access(profile: Profile, plan: &SubscriberPlan) -> bool {
    match profile {
        Profile::Baseline => true,
        Profile::RealWorld => matches!(
            plan.access_path,
            AccessPath::TeamMember | AccessPath::OrganizationAdmin | AccessPath::AdminClaim
        ),
    }
}

fn subscribers_by_team(subscriptions: &[ActiveSubscription]) -> BTreeMap<usize, Vec<usize>> {
    let mut by_team = BTreeMap::<usize, Vec<usize>>::new();
    for (index, subscription) in subscriptions.iter().enumerate() {
        by_team
            .entry(subscription.plan.team_index)
            .or_default()
            .push(index);
    }
    by_team
}

fn first_unbound_team(teams: usize, subscribers: &BTreeMap<usize, Vec<usize>>) -> Option<usize> {
    (0..teams).find(|team| !subscribers.contains_key(team))
}

fn next_private_list_document(
    fixture: &Fixture,
    next_index: &mut u64,
    team_index: usize,
    updated_at: Option<u64>,
) -> DocumentSpec {
    loop {
        let mut document = fixture.synthetic_document(*next_index, team_index);
        *next_index += 1;
        if document.appears_in_list() && !document.public {
            if let Some(updated_at) = updated_at {
                document.updated_at = updated_at;
            }
            return document;
        }
    }
}

fn commit_documents(db: &BenchDb, documents: &[DocumentSpec]) -> Result<CommitSample, String> {
    let mut tx = db.mergeable_tx();
    for document in documents {
        tx.insert_with_id(
            saas_permission_support::DOCUMENTS,
            document.row,
            document.cells(),
        )
        .map_err(|error| format!("stage document {}: {error}", document.index))?;
    }
    let started = Instant::now();
    tx.commit()
        .map_err(|error| format!("commit {} documents: {error}", documents.len()))?;
    commit_sample_since(db, started)
}

fn commit_organization_update(
    db: &BenchDb,
    organization_index: usize,
) -> Result<CommitSample, String> {
    let mut tx = db.mergeable_tx();
    tx.update(
        saas_permission_support::ORGANIZATIONS,
        saas_permission_support::organization_row(organization_index as u64),
        BTreeMap::from([("suspended".to_owned(), Value::Bool(true))]),
    )
    .map_err(|error| format!("stage query-irrelevant organization update: {error}"))?;
    let started = Instant::now();
    tx.commit()
        .map_err(|error| format!("commit query-irrelevant organization update: {error}"))?;
    commit_sample_since(db, started)
}

fn commit_sample_since(db: &BenchDb, started: Instant) -> Result<CommitSample, String> {
    let wall_us = duration_micros(started.elapsed());
    let metrics = db
        .last_commit_metrics_for_test()
        .ok_or_else(|| "missing Groove commit metrics".to_owned())?;
    let storage_us = duration_micros(metrics.storage_write_time);
    let ivm_tick_us = duration_micros(metrics.ivm_tick_time);
    Ok(CommitSample {
        wall_us,
        storage_us,
        initial_commit_ivm_tick_us: ivm_tick_us,
        jazz_residual_us: wall_us.saturating_sub(storage_us.saturating_add(ivm_tick_us)),
        initial_commit_groove_notifications: metrics.tick.notifications_sent,
        initial_commit_groove_notification_records: metrics.tick.notification_records,
        initial_commit_groove_notification_bytes: metrics.tick.notification_encoded_bytes,
    })
}

fn measure_subscription_churn(
    db: &BenchDb,
    fixture: &Fixture,
    subscriptions: &mut Vec<ActiveSubscription>,
    next_document_index: &mut u64,
    drop_subscriptions: usize,
) -> Result<Option<SubscriptionChurnReport>, String> {
    if drop_subscriptions == 0 {
        return Ok(None);
    }
    if drop_subscriptions >= subscriptions.len() {
        return Err(format!(
            "subscription churn must leave one live stream ({} requested drops, {} streams)",
            drop_subscriptions,
            subscriptions.len()
        ));
    }

    let subscribed_organizations = subscriptions
        .iter()
        .map(|subscription| subscription.plan.organization_index)
        .collect::<BTreeSet<_>>();
    let query_irrelevant_organization_index = (0..fixture.config().organizations)
        .find(|organization| !subscribed_organizations.contains(organization))
        .ok_or_else(|| {
            "subscription churn needs one organization unused by every benchmark subscriber"
                .to_owned()
        })?;
    let _pre_churn_drain = drain_all(subscriptions)?;
    for subscription in subscriptions.iter() {
        subscription
            .observed
            .assert_matches(&subscription.label, &subscription.expected)?;
    }
    let before_drop = subscription_lifecycle_counts(db);
    let remaining_subscriptions = subscriptions.len() - drop_subscriptions;
    let reaping_document_team_index = subscriptions[remaining_subscriptions].plan.team_index;
    subscriptions.truncate(remaining_subscriptions);
    let immediately_after_drop = subscription_lifecycle_counts(db);
    if immediately_after_drop.jazz_live_subscriptions != remaining_subscriptions {
        return Err(format!(
            "subscription churn retained {} live Jazz receipts after dropping to {remaining_subscriptions}",
            immediately_after_drop.jazz_live_subscriptions
        ));
    }

    let organization_sample = commit_organization_update(db, query_irrelevant_organization_index)?;
    let query_irrelevant_organization_update = audit_membership_quiet_write_phase(
        "churn_query_irrelevant_organization_update",
        1,
        1,
        vec![organization_sample],
        empty_expected_events(subscriptions.len()),
        subscriptions,
    )?;
    let after_query_irrelevant_update = subscription_lifecycle_counts(db);

    if subscriptions
        .iter()
        .any(|subscription| subscription.plan.team_index == reaping_document_team_index)
    {
        return Err(
            "subscription churn selected a dropped team that is still bound by a survivor"
                .to_owned(),
        );
    }
    let document = next_private_list_document(
        fixture,
        next_document_index,
        reaping_document_team_index,
        None,
    );
    let document_sample = commit_documents(db, &[document])?;
    let unbound_team_document_write = audit_membership_quiet_write_phase(
        "churn_unbound_team_document_write",
        1,
        1,
        vec![document_sample],
        empty_expected_events(subscriptions.len()),
        subscriptions,
    )?;
    let after_unbound_team_document_write = subscription_lifecycle_counts(db);

    Ok(Some(SubscriptionChurnReport {
        dropped_subscriptions: drop_subscriptions,
        remaining_subscriptions,
        query_irrelevant_organization_index,
        reaping_document_team_index,
        before_drop,
        immediately_after_drop,
        query_irrelevant_organization_update,
        after_query_irrelevant_update,
        unbound_team_document_write,
        after_unbound_team_document_write,
        stale_outputs_created_by_drop: immediately_after_drop
            .stale_groove_subscriptions
            .saturating_sub(before_drop.stale_groove_subscriptions),
        stale_outputs_remaining_after_irrelevant_update: after_query_irrelevant_update
            .stale_groove_subscriptions,
        stale_outputs_reaped_by_irrelevant_update: immediately_after_drop
            .stale_groove_subscriptions
            .saturating_sub(after_query_irrelevant_update.stale_groove_subscriptions),
        stale_outputs_reaped_by_document_write: after_query_irrelevant_update
            .stale_groove_subscriptions
            .saturating_sub(after_unbound_team_document_write.stale_groove_subscriptions),
    }))
}

fn subscription_lifecycle_counts(db: &BenchDb) -> SubscriptionLifecycleCounts {
    let jazz_live_subscriptions = db.maintained_subscription_size_receipts_for_test().len();
    let runtime = db.runtime_stats_for_test();
    SubscriptionLifecycleCounts {
        jazz_live_subscriptions,
        groove_active_subscriptions: runtime.active_subscriptions,
        groove_active_prepared_shapes: runtime.active_prepared_shapes,
        groove_active_shape_params: runtime.active_shape_params,
        groove_graph_nodes: runtime.graph_nodes,
        stale_groove_subscriptions: runtime
            .active_subscriptions
            .saturating_sub(jazz_live_subscriptions),
    }
}

fn empty_expected_events(subscriptions: usize) -> Vec<Vec<PageTransition>> {
    (0..subscriptions).map(|_| Vec::new()).collect()
}

fn apply_single_document(
    document: DocumentSpec,
    oracle: &mut PerTeamTop100Oracle,
    subscriptions: &mut [ActiveSubscription],
    team_subscribers: &BTreeMap<usize, Vec<usize>>,
    expected_events: &mut [Vec<PageTransition>],
) {
    let affected = team_subscribers
        .get(&document.team_index)
        .cloned()
        .unwrap_or_default();
    let before = affected
        .iter()
        .map(|index| (*index, subscriptions[*index].expected.clone()))
        .collect::<Vec<_>>();
    oracle.upsert(oracle_document(document));
    for (index, before) in before {
        let after = expected_after_write(&subscriptions[index], oracle);
        if after != before {
            expected_events[index].push(PageTransition::between(before, after.clone()));
            subscriptions[index].expected = after;
        }
    }
}

fn apply_document_batch(
    documents: &[DocumentSpec],
    oracle: &mut PerTeamTop100Oracle,
    subscriptions: &mut [ActiveSubscription],
    team_subscribers: &BTreeMap<usize, Vec<usize>>,
    expected_events: &mut [Vec<PageTransition>],
) {
    let affected = documents
        .iter()
        .filter_map(|document| team_subscribers.get(&document.team_index))
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>();
    let before = affected
        .iter()
        .map(|index| (*index, subscriptions[*index].expected.clone()))
        .collect::<Vec<_>>();
    for document in documents {
        oracle.upsert(oracle_document(*document));
    }
    for (index, before) in before {
        let after = expected_after_write(&subscriptions[index], oracle);
        if after != before {
            expected_events[index].push(PageTransition::between(before, after.clone()));
            subscriptions[index].expected = after;
        }
    }
}

fn expected_after_write(
    subscription: &ActiveSubscription,
    oracle: &PerTeamTop100Oracle,
) -> ExpectedPage {
    if subscription.full_document_access {
        oracle.page(subscription.plan.team)
    } else {
        subscription.expected.clone()
    }
}

fn oracle_document(document: DocumentSpec) -> DocumentMetadata {
    DocumentMetadata::new(
        document.row,
        document.team,
        document.updated_at,
        match document.status {
            FixtureDocumentStatus::Active => DocumentStatus::Active,
            FixtureDocumentStatus::Draft => DocumentStatus::Draft,
            FixtureDocumentStatus::Closed => DocumentStatus::Other,
        },
        document.archived,
    )
}

fn audit_write_phase(
    name: &'static str,
    transactions: usize,
    rows: usize,
    samples: Vec<CommitSample>,
    expected_events: Vec<Vec<PageTransition>>,
    subscriptions: &mut [ActiveSubscription],
) -> Result<WritePhaseReport, String> {
    audit_write_phase_inner(
        name,
        transactions,
        rows,
        samples,
        expected_events,
        subscriptions,
        false,
    )
}

fn audit_membership_quiet_write_phase(
    name: &'static str,
    transactions: usize,
    rows: usize,
    samples: Vec<CommitSample>,
    expected_events: Vec<Vec<PageTransition>>,
    subscriptions: &mut [ActiveSubscription],
) -> Result<WritePhaseReport, String> {
    audit_write_phase_inner(
        name,
        transactions,
        rows,
        samples,
        expected_events,
        subscriptions,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn audit_write_phase_inner(
    name: &'static str,
    transactions: usize,
    rows: usize,
    samples: Vec<CommitSample>,
    expected_events: Vec<Vec<PageTransition>>,
    subscriptions: &mut [ActiveSubscription],
    allow_empty_delta_events: bool,
) -> Result<WritePhaseReport, String> {
    let drain_started = Instant::now();
    let audit = drain_all(subscriptions)?;
    let stream_drain_ms = millis(drain_started.elapsed());

    let validation_started = Instant::now();
    let mut expected_notified_streams = 0;
    for (index, (subscription, transitions)) in
        subscriptions.iter().zip(expected_events.iter()).enumerate()
    {
        if transitions.is_empty() {
            if let Some(events) = audit.receipt_for(index)
                && (!allow_empty_delta_events
                    || events.deltas.iter().any(|delta| {
                        delta.reset
                            || !delta.added.is_empty()
                            || !delta.updated.is_empty()
                            || !delta.removed.is_empty()
                    }))
            {
                return Err(format!(
                    "{name}: quiet {} emitted an event: {:?}",
                    subscription.label, events
                ));
            }
        } else {
            expected_notified_streams += 1;
            let events = audit
                .receipt_for(index)
                .ok_or_else(|| format!("{name}: {} emitted no event", subscription.label))?;
            if events.deltas.len() != transitions.len() {
                return Err(format!(
                    "{name}: {} emitted {} events, expected {}",
                    subscription.label,
                    events.deltas.len(),
                    transitions.len()
                ));
            }
            for (event_index, (delta, transition)) in
                events.deltas.iter().zip(transitions).enumerate()
            {
                if delta.reset {
                    return Err(format!(
                        "{name}: {} event {event_index} was an unexpected reset",
                        subscription.label
                    ));
                }
                if !delta.updated.is_empty()
                    || delta.added_set() != transition.added
                    || delta.removed_set() != transition.removed
                {
                    return Err(format!(
                        "{name}: {} event {event_index} mismatch: delta={delta:?}, expected added={:?}, removed={:?}",
                        subscription.label, transition.added, transition.removed
                    ));
                }
            }
        }
        subscription
            .observed
            .assert_matches(&subscription.label, &subscription.expected)?;
    }
    let oracle_validation_ms = millis(validation_started.elapsed());

    let wall = samples
        .iter()
        .map(|sample| sample.wall_us)
        .collect::<Vec<_>>();
    let storage = samples
        .iter()
        .map(|sample| sample.storage_us)
        .collect::<Vec<_>>();
    let ivm = samples
        .iter()
        .map(|sample| sample.initial_commit_ivm_tick_us)
        .collect::<Vec<_>>();
    let residual = samples
        .iter()
        .map(|sample| sample.jazz_residual_us)
        .collect::<Vec<_>>();
    let (events, added_rows, updated_rows, removed_rows, resets) = audit
        .notified
        .iter()
        .flat_map(|receipt| &receipt.events.deltas)
        .fold(
            (0, 0, 0, 0, 0),
            |(events, added, updated, removed, resets), delta| {
                (
                    events + 1,
                    added + delta.added.len(),
                    updated + delta.updated.len(),
                    removed + delta.removed.len(),
                    resets + usize::from(delta.reset),
                )
            },
        );

    Ok(WritePhaseReport {
        name,
        transactions,
        rows,
        commit_wall_us: latency_stats(&wall),
        groove_storage_us: latency_stats(&storage),
        groove_initial_commit_ivm_tick_us: latency_stats(&ivm),
        jazz_facade_and_subscription_refresh_residual_us: latency_stats(&residual),
        groove_initial_commit_notifications: samples
            .iter()
            .map(|sample| sample.initial_commit_groove_notifications)
            .sum(),
        groove_initial_commit_notification_records: samples
            .iter()
            .map(|sample| sample.initial_commit_groove_notification_records)
            .sum(),
        groove_initial_commit_notification_bytes: samples
            .iter()
            .map(|sample| sample.initial_commit_groove_notification_bytes)
            .sum(),
        stream_drain_ms,
        oracle_validation_ms,
        streams_scanned: audit.streams_scanned,
        expected_notified_streams,
        actual_notified_streams: audit.notified_streams(),
        events,
        added_rows,
        updated_rows,
        removed_rows,
        resets,
        exact_oracle_match: true,
    })
}

fn drain_all(
    subscriptions: &mut [ActiveSubscription],
) -> Result<saas_fanout_oracle::StreamDrainReceipt, String> {
    drain_streams(subscriptions.iter_mut().map(|subscription| {
        StreamAuditTarget::new(
            &subscription.label,
            &mut subscription.stream,
            &mut subscription.observed,
        )
    }))
}

fn validate_one_shot_samples(
    db: &BenchDb,
    query: &Query,
    read_opts: &ReadOpts,
    subscriptions: &[ActiveSubscription],
) -> OneShotCanaryReport {
    if subscriptions.is_empty() {
        return OneShotCanaryReport {
            checked: 0,
            passed: 0,
            failed: 0,
            ordered_exact: true,
            failures: Vec::new(),
        };
    }
    let mut indices = BTreeSet::from([
        0,
        subscriptions.len() / 2,
        subscriptions.len().saturating_sub(1),
    ]);
    if subscriptions.len() > 10 {
        indices.insert(10);
    }
    for access_path in [
        AccessPath::TeamMember,
        AccessPath::OrganizationAdmin,
        AccessPath::DirectAcl,
        AccessPath::Public,
        AccessPath::AdminClaim,
    ] {
        if let Some((index, _)) = subscriptions
            .iter()
            .enumerate()
            .find(|(_, subscription)| subscription.plan.access_path == access_path)
        {
            indices.insert(index);
        }
    }
    let mut failures = Vec::new();
    for index in &indices {
        let subscription = &subscriptions[*index];
        let result = db
            .prepare_query_bound(
                query,
                BTreeMap::from([("team".to_owned(), Value::Uuid(subscription.plan.team.0))]),
            )
            .map_err(|error| format!("prepare: {error}"))
            .and_then(|prepared| {
                block_on(db.all_for_identity(
                    &prepared,
                    read_opts.clone(),
                    subscription.plan.identity,
                ))
                .map_err(|error| format!("read: {error}"))
            });
        match result {
            Ok(rows) => {
                let actual = rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
                if actual != subscription.expected.rows() {
                    failures.push(format!(
                        "sample {index} ({:?}) returned {} rows, expected {} in exact order",
                        subscription.plan.access_path,
                        actual.len(),
                        subscription.expected.len()
                    ));
                }
            }
            Err(error) => failures.push(format!(
                "sample {index} ({:?}) failed: {error}",
                subscription.plan.access_path
            )),
        }
    }
    OneShotCanaryReport {
        checked: indices.len(),
        passed: indices.len() - failures.len(),
        failed: failures.len(),
        ordered_exact: failures.is_empty(),
        failures,
    }
}

fn runtime_report(db: &BenchDb) -> RuntimeReport {
    let stats = db.runtime_stats_for_test();
    RuntimeReport {
        graph_nodes: stats.graph_nodes,
        active_subscriptions: stats.active_subscriptions,
        active_prepared_shapes: stats.active_prepared_shapes,
        active_shape_params: stats.active_shape_params,
        arrangement_count: stats.arrangement_count,
        arrangement_rows: stats.arrangement_rows,
        arrangement_encoded_bytes: stats.arrangement_encoded_bytes,
        eval_memo_entries: stats.eval_memo_entries,
        eval_memo_bytes: stats.eval_memo_bytes,
        hydration_memo_entries: stats.hydration_memo_entries,
        hydration_memo_hits: stats.hydration_memo_hits,
        hydration_memo_computes: stats.hydration_memo_computes,
        logical_nodes_requested: stats.logical_nodes_requested,
        deduped_graph_nodes: stats.deduped_graph_nodes,
        dedupe_ratio: stats.dedupe_ratio(),
    }
}

fn local_subscription_footprint_report(db: &BenchDb) -> LocalSubscriptionFootprintReport {
    let receipts = db.maintained_subscription_size_receipts_for_test();
    let root_rows = receipts.iter().map(|receipt| receipt.root_rows).sum();
    let snapshot_bytes = receipts.iter().map(|receipt| receipt.snapshot_bytes).sum();
    let reset_frame_bytes = receipts
        .iter()
        .map(|receipt| receipt.reset_frame_bytes)
        .sum();
    let validation_tuple_estimate_bytes = receipts
        .iter()
        .map(|receipt| receipt.validation_tuple_estimate_bytes)
        .sum();
    let maintained_heap_bytes = receipts
        .iter()
        .map(|receipt| receipt.footprint.maintained_heap_bytes)
        .sum();
    let control_state_bytes = receipts
        .iter()
        .map(|receipt| receipt.footprint.control_state_bytes)
        .sum();
    let total_heap_bytes = receipts
        .iter()
        .map(|receipt| receipt.footprint.total_heap_bytes)
        .sum();
    LocalSubscriptionFootprintReport {
        subscriptions: receipts.len(),
        root_rows,
        snapshot_bytes,
        reset_frame_bytes,
        validation_tuple_estimate_bytes,
        approximate_private_maintained_heap_bytes: maintained_heap_bytes,
        approximate_private_control_state_bytes: control_state_bytes,
        approximate_private_total_heap_bytes: total_heap_bytes,
        mean_approximate_private_total_heap_bytes_per_subscription: if receipts.is_empty() {
            0.0
        } else {
            total_heap_bytes as f64 / receipts.len() as f64
        },
    }
}

fn permission_mix(subscriptions: &[ActiveSubscription]) -> BTreeMap<&'static str, usize> {
    let mut mix = BTreeMap::new();
    for subscription in subscriptions {
        *mix.entry(access_path_label(subscription.plan.access_path))
            .or_default() += 1;
    }
    mix
}

fn access_path_label(access_path: AccessPath) -> &'static str {
    match access_path {
        AccessPath::TeamMember => "team_member",
        AccessPath::OrganizationAdmin => "organization_admin",
        AccessPath::DirectAcl => "direct_acl",
        AccessPath::Public => "public",
        AccessPath::AdminClaim => "admin_claim",
    }
}

fn latency_stats(samples: &[u64]) -> LatencyStats {
    if samples.is_empty() {
        return LatencyStats {
            samples: 0,
            total_us: 0,
            mean_us: 0.0,
            min_us: 0,
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            max_us: 0,
        };
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let total_us = sorted.iter().copied().sum();
    LatencyStats {
        samples: sorted.len(),
        total_us,
        mean_us: total_us as f64 / sorted.len() as f64,
        min_us: sorted[0],
        p50_us: percentile(&sorted, 0.50),
        p95_us: percentile(&sorted, 0.95),
        p99_us: percentile(&sorted, 0.99),
        max_us: *sorted.last().expect("non-empty samples"),
    }
}

fn percentile(sorted: &[u64], percentile: f64) -> u64 {
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index]
}

fn duration_micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn millis(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
