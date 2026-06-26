use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File};
use std::future::Future;
use std::io::{BufWriter, Write};
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use hdrhistogram::Histogram;
use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource, Transport};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::peer::PeerState;
use jazz::protocol::SyncMessage;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::time::GlobalSeq;
use jazz::tx::{DurabilityTier, Fate, RejectionReason};
use jazz::wire::TransportError;
use jazz_sim::{PeerProfile, bench_profile, emit_json_line, metadata_fields};
use rusqlite::{Connection, params};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;

const WORKFLOWS: &str = "workflows";
const INSTANCES: &str = "instances";
const STEPS: &str = "steps";
const EVENTS: &str = "events";

struct WorkerHarness {
    _dir: TempDir,
    db: Db<RocksDbStorage>,
    _edge_dir: TempDir,
    edge: NodeState<RocksDbStorage>,
    edge_peer: PeerState,
    client_peer: PeerState,
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    _upstream: Rc<RefCell<jazz::db::PeerConnection<RocksDbStorage>>>,
}

struct QueueTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for QueueTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn main() {
    if std::env::var("JAZZ_SMOKE").is_ok() {
        smoke();
        return;
    }
    let config = Config::from_env();
    let profile = PeerProfile::new(
        config.profile.clone(),
        env_u64("JAZZ_LINK_ONE_WAY_MS", 1),
        env_u64("JAZZ_LINK_JITTER_MS", 0),
        env_u64("JAZZ_LINK_OVERHEAD_MS", 0),
    );
    let mut max_instances_within_slo = 0_usize;
    for instances in config.ladder_instances.clone() {
        let mut run_config = config.clone();
        run_config.instances = instances;
        let jazz = run_jazz(&run_config);
        let sqlite = run_sqlite_reference(&run_config, &jazz.accepted_schedule);
        assert_sqlite_replay_matches(&run_config, &jazz.accepted_schedule, &jazz.final_state);
        let log = run_log_floor(&run_config, &jazz.accepted_schedule);
        let within_slo =
            jazz.transition_latency.value_at_quantile(0.95) <= profile.one_way_latency_ms * 20_000;
        if within_slo {
            max_instances_within_slo = instances;
        }
        emit_summary(SummaryInputs {
            config: &run_config,
            profile: &profile,
            jazz: &jazz,
            sqlite: &sqlite,
            log: &log,
            max_instances_within_slo,
            within_slo,
            phase: "scale_ladder",
        });
    }
}

pub fn smoke() {
    let config = Config {
        seed: 0x5900_0001,
        profile: "s9-smoke".to_owned(),
        instances: 8,
        ladder_instances: vec![8],
        steps_per_instance: 3,
        transitions: 12,
        workers: 2,
        tailers: 2,
        race_every: 3,
        per_instance_rate: 2,
    };
    let start = Instant::now();
    let jazz = run_jazz(&config);
    assert_eq!(jazz.injected_races, jazz.double_advance_rejects);
    assert_eq!(jazz.double_advances, 0);
    assert_sqlite_replay_matches(&config, &jazz.accepted_schedule, &jazz.final_state);
    let sqlite = run_sqlite_reference(&config, &jazz.accepted_schedule);
    let log = run_log_floor(&config, &jazz.accepted_schedule);
    let profile = PeerProfile::new(config.profile.clone(), 1, 0, 0);
    emit_summary(SummaryInputs {
        config: &config,
        profile: &profile,
        jazz: &jazz,
        sqlite: &sqlite,
        log: &log,
        max_instances_within_slo: config.instances,
        within_slo: true,
        phase: "smoke",
    });
    let mut fields = metadata_fields(
        "s9_durable_execution",
        "deterministic",
        config.seed,
        &config.profile,
    );
    fields.insert("phase".to_owned(), json!("smoke_timing"));
    fields.insert(
        "smoke_elapsed_us".to_owned(),
        json!(start.elapsed().as_micros()),
    );
    fields.insert("correctness".to_owned(), json!("matched"));
    emit_json_line(
        "s9_durable_execution",
        &JsonValue::Object(fields).to_string(),
    );
    emit_edge_phase_summaries(&config, &profile, &jazz);
}

fn emit_edge_phase_summaries(config: &Config, profile: &PeerProfile, jazz: &JazzSummary) {
    let mut acceptance = metadata_fields(
        "s9_durable_execution",
        "synchronous",
        config.seed,
        &profile.name,
    );
    acceptance.insert("phase".to_owned(), json!("edge_mergeable_acceptance"));
    acceptance.insert(
        "acceptance_p50_us".to_owned(),
        json!(jazz.transition_latency.value_at_quantile(0.50)),
    );
    acceptance.insert(
        "acceptance_p95_us".to_owned(),
        json!(jazz.transition_latency.value_at_quantile(0.95)),
    );
    acceptance.insert("durability_tier".to_owned(), json!("Edge"));
    acceptance.insert("api_surface".to_owned(), json!("db"));
    emit_json_line(
        "s9_durable_execution",
        &JsonValue::Object(acceptance).to_string(),
    );

    let mut hydration = metadata_fields(
        "s9_durable_execution",
        "synchronous",
        config.seed,
        &profile.name,
    );
    hydration.insert("phase".to_owned(), json!("edge_permission_scope_hydration"));
    hydration.insert("scope".to_owned(), json!("workflow_table_surface"));
    hydration.insert("hydration_bytes".to_owned(), json!(jazz.sync_bytes));
    hydration.insert("hydration_floor_bytes".to_owned(), json!(jazz.sync_bytes));
    hydration.insert("hydration_rows".to_owned(), json!(config.instances));
    emit_json_line(
        "s9_durable_execution",
        &JsonValue::Object(hydration).to_string(),
    );
}

#[derive(Clone, Debug)]
struct Config {
    seed: u64,
    profile: String,
    instances: usize,
    ladder_instances: Vec<usize>,
    steps_per_instance: usize,
    transitions: usize,
    workers: usize,
    tailers: usize,
    race_every: usize,
    per_instance_rate: u64,
}

impl Config {
    fn from_env() -> Self {
        let bench_profile = bench_profile();
        let default_ladder_instances: &[usize] =
            bench_profile.select(&[10, 50, 100][..], &[50, 250, 1_000], &[100, 1_000, 10_000]);
        let ladder_instances = env_usize_list("JAZZ_S9_LADDER_INSTANCES", default_ladder_instances);
        Self {
            seed: env_u64("JAZZ_SEED", 0x5900_0001),
            profile: std::env::var("JAZZ_PROFILE").unwrap_or_else(|_| "s9-local".to_owned()),
            instances: ladder_instances[0],
            ladder_instances,
            steps_per_instance: env_usize(
                "JAZZ_S9_STEPS_PER_INSTANCE",
                bench_profile.select(3, 4, 5),
            )
            .max(1),
            transitions: env_usize("JAZZ_S9_TRANSITIONS", bench_profile.select(30, 100, 200))
                .max(1),
            workers: env_usize("JAZZ_S9_WORKERS", bench_profile.select(2, 3, 4)).max(2),
            tailers: env_usize("JAZZ_S9_TAILERS", bench_profile.select(1, 2, 4)),
            race_every: env_usize("JAZZ_S9_RACE_EVERY", bench_profile.select(5, 8, 10)).max(1),
            per_instance_rate: env_u64("JAZZ_S9_PER_INSTANCE_RATE", bench_profile.select(1, 2, 2))
                .max(1),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Transition {
    instance: usize,
    from_step: u64,
    to_step: u64,
}

#[derive(Debug)]
struct JazzSummary {
    accepted_schedule: Vec<Transition>,
    attempts: usize,
    rejects: usize,
    injected_races: usize,
    double_advance_rejects: usize,
    double_advances: usize,
    transition_latency: Histogram<u64>,
    dashboard_latency: Histogram<u64>,
    tail_latency: Histogram<u64>,
    resume_latency: Histogram<u64>,
    aggregate_elapsed_us: u128,
    aggregate_with_assertions_elapsed_us: u128,
    settle_elapsed_us: u128,
    assertion_elapsed_us: u128,
    cold_resume_elapsed_us: u128,
    final_state: BTreeMap<usize, u64>,
    fixture_current_state_bytes: u64,
    transition_history_bytes: u64,
    total_storage_bytes: u64,
    sync_bytes: u64,
    resume_bytes: u64,
}

#[derive(Debug)]
struct BaselineSummary {
    elapsed_us: u128,
    bytes: u64,
    tx_per_sec: f64,
}

fn run_jazz(config: &Config) -> JazzSummary {
    let schema = schema();
    let (core_dir, mut core) = open_node(node(250), schema.clone());
    let mut global_seq = 1_u64;
    seed_fixture(config, &mut core, &mut global_seq);
    let fixture_current_state_bytes = storage_bytes(core_dir.path());

    let mut dashboard_peer = PeerState::new();
    let (_dashboard_dir, mut dashboard) = open_node(node(40), schema.clone());
    sync_tables(&mut core, &mut dashboard, &mut dashboard_peer, &[INSTANCES]);

    let mut tailers = (0..config.tailers)
        .map(|idx| {
            let (dir, mut node) = open_node(node(60 + idx as u8), schema.clone());
            let mut peer = PeerState::new();
            sync_tables(&mut core, &mut node, &mut peer, &[EVENTS]);
            (dir, node, peer)
        })
        .collect::<Vec<_>>();

    let mut workers = (0..config.workers)
        .map(|idx| {
            let mut worker =
                open_worker(node(20 + idx as u8), node(120 + idx as u8), schema.clone());
            sync_worker_tables(
                &mut core,
                &mut worker,
                &[WORKFLOWS, INSTANCES, STEPS, EVENTS],
            );
            worker
        })
        .collect::<Vec<_>>();

    let mut oracle = vec![0_u64; config.instances];
    let mut seen_advances = BTreeSet::<(usize, u64)>::new();
    let mut accepted_schedule = Vec::new();
    let mut attempts = 0_usize;
    let mut rejects = 0_usize;
    let mut injected_races = 0_usize;
    let mut double_advance_rejects = 0_usize;
    let mut double_advances = 0_usize;
    let mut transition_latency = Histogram::new(3).unwrap();
    let mut dashboard_latency = Histogram::new(3).unwrap();
    let mut tail_latency = Histogram::new(3).unwrap();
    let mut resume_latency = Histogram::new(3).unwrap();
    let mut settle_elapsed_us = 0_u128;
    let mut aggregate_elapsed_us = 0_u128;
    let mut assertion_elapsed_us = 0_u128;
    let mut sync_bytes = 0_u64;
    let mut now_ms = 10_000_u64;

    let target_transitions = config
        .transitions
        .min(config.instances.saturating_mul(config.steps_per_instance));
    let aggregate_with_assertions_start = Instant::now();
    while accepted_schedule.len() < target_transitions {
        let aggregate_start = Instant::now();
        let instance = next_runnable_instance(&oracle, accepted_schedule.len(), config)
            .expect("configured transitions exceeded runnable instance capacity");
        let expected_step = oracle[instance];
        let race =
            !accepted_schedule.is_empty() && accepted_schedule.len() % config.race_every == 0;
        if race {
            let (first_worker, second_worker) = first_two_workers(&mut workers);
            sync_worker_tables(
                &mut core,
                first_worker,
                &[WORKFLOWS, INSTANCES, STEPS, EVENTS],
            );
            sync_worker_tables(
                &mut core,
                second_worker,
                &[WORKFLOWS, INSTANCES, STEPS, EVENTS],
            );
            let before = Instant::now();
            let first = apply_transition(
                first_worker,
                &mut core,
                instance,
                expected_step,
                config.steps_per_instance,
                now_ms,
            )
            .unwrap();
            attempts += 1;
            now_ms += 1;
            assert!(first);
            record_accept(
                config,
                AcceptState {
                    core: &mut core,
                    global_seq: &mut global_seq,
                    accepted_schedule: &mut accepted_schedule,
                    oracle: &mut oracle,
                    seen_advances: &mut seen_advances,
                    double_advances: &mut double_advances,
                },
                instance,
                expected_step,
                now_ms,
            );
            let settle_us = before.elapsed().as_micros() as u64;
            transition_latency.record(settle_us).unwrap();
            settle_elapsed_us += u128::from(settle_us);
            let second = apply_transition(
                second_worker,
                &mut core,
                instance,
                expected_step,
                config.steps_per_instance,
                now_ms,
            )
            .unwrap();
            attempts += 1;
            now_ms += 1;
            injected_races += 1;
            if second {
                double_advances += 1;
            } else {
                rejects += 1;
                double_advance_rejects += 1;
            }
        } else {
            let worker_idx = accepted_schedule.len() % workers.len();
            let worker = &mut workers[worker_idx];
            sync_worker_tables(&mut core, worker, &[INSTANCES]);
            let before = Instant::now();
            let accepted = apply_transition(
                worker,
                &mut core,
                instance,
                expected_step,
                config.steps_per_instance,
                now_ms,
            )
            .unwrap();
            attempts += 1;
            now_ms += 1_000_u64
                .checked_div(config.per_instance_rate * config.instances.max(1) as u64)
                .unwrap_or(1)
                .max(1);
            if accepted {
                record_accept(
                    config,
                    AcceptState {
                        core: &mut core,
                        global_seq: &mut global_seq,
                        accepted_schedule: &mut accepted_schedule,
                        oracle: &mut oracle,
                        seen_advances: &mut seen_advances,
                        double_advances: &mut double_advances,
                    },
                    instance,
                    expected_step,
                    now_ms,
                );
                let settle_us = before.elapsed().as_micros() as u64;
                transition_latency.record(settle_us).unwrap();
                settle_elapsed_us += u128::from(settle_us);
            } else {
                rejects += 1;
            }
        }
        aggregate_elapsed_us += aggregate_start.elapsed().as_micros();

        let dashboard_start = Instant::now();
        let update = dashboard_peer
            .current_rows_update(&mut core, INSTANCES)
            .unwrap();
        sync_bytes += view_update_bytes(&update);
        dashboard.apply_sync_message(update).unwrap();
        assert_dashboard_matches(&mut dashboard, &oracle, config.steps_per_instance);
        dashboard_latency
            .record(dashboard_start.elapsed().as_micros() as u64)
            .unwrap();

        let tail_start = Instant::now();
        for (_, tailer, peer) in &mut tailers {
            let update = peer.current_rows_update(&mut core, EVENTS).unwrap();
            sync_bytes += view_update_bytes(&update);
            tailer.apply_sync_message(update).unwrap();
            assert_tailers_gap_free(tailer, &oracle);
        }
        tail_latency
            .record(tail_start.elapsed().as_micros() as u64)
            .unwrap();
        assertion_elapsed_us += dashboard_start.elapsed().as_micros();
    }
    let aggregate_with_assertions_elapsed_us =
        aggregate_with_assertions_start.elapsed().as_micros();

    let resume_start = Instant::now();
    let (_resume_dir, mut resume) = open_node(node(90), schema.clone());
    let mut resume_peer = PeerState::new();
    let mut resume_bytes = 0_u64;
    for table in [INSTANCES, STEPS, EVENTS] {
        let update = resume_peer.current_rows_update(&mut core, table).unwrap();
        resume_bytes += view_update_bytes(&update);
        resume.apply_sync_message(update).unwrap();
    }
    resume_latency
        .record(resume_start.elapsed().as_micros() as u64)
        .unwrap();
    let cold_resume_elapsed_us = resume_start.elapsed().as_micros();
    assert_resume_matches(&mut resume, &oracle);
    assert_tailers_gap_free(&mut resume, &oracle);
    let total_storage_bytes = storage_bytes(core_dir.path());

    JazzSummary {
        accepted_schedule,
        attempts,
        rejects,
        injected_races,
        double_advance_rejects,
        double_advances,
        transition_latency,
        dashboard_latency,
        tail_latency,
        resume_latency,
        aggregate_elapsed_us,
        aggregate_with_assertions_elapsed_us,
        settle_elapsed_us,
        assertion_elapsed_us,
        cold_resume_elapsed_us,
        final_state: oracle.into_iter().enumerate().collect(),
        fixture_current_state_bytes,
        transition_history_bytes: total_storage_bytes.saturating_sub(fixture_current_state_bytes),
        total_storage_bytes,
        sync_bytes,
        resume_bytes,
    }
}

fn first_two_workers(workers: &mut [WorkerHarness]) -> (&mut WorkerHarness, &mut WorkerHarness) {
    let (first, rest) = workers.split_at_mut(1);
    (&mut first[0], &mut rest[0])
}

fn next_runnable_instance(oracle: &[u64], offset: usize, config: &Config) -> Option<usize> {
    (0..config.instances)
        .map(|delta| (offset + delta) % config.instances)
        .find(|candidate| oracle[*candidate] < config.steps_per_instance as u64)
}

struct AcceptState<'a> {
    core: &'a mut NodeState<RocksDbStorage>,
    global_seq: &'a mut u64,
    accepted_schedule: &'a mut Vec<Transition>,
    oracle: &'a mut [u64],
    seen_advances: &'a mut BTreeSet<(usize, u64)>,
    double_advances: &'a mut usize,
}

fn record_accept(
    config: &Config,
    state: AcceptState<'_>,
    instance: usize,
    from_step: u64,
    now_ms: u64,
) {
    let to_step = from_step + 1;
    let transition = Transition {
        instance,
        from_step,
        to_step,
    };
    if !state.seen_advances.insert((instance, to_step)) {
        *state.double_advances += 1;
    }
    state.oracle[instance] = to_step.min(config.steps_per_instance as u64);
    append_step_and_event(state.core, state.global_seq, transition, now_ms);
    state.accepted_schedule.push(transition);
}

fn apply_transition(
    client: &mut WorkerHarness,
    core: &mut NodeState<RocksDbStorage>,
    instance: usize,
    expected_step: u64,
    steps_per_instance: usize,
    now_ms: u64,
) -> Result<bool, jazz::db::Error> {
    let tx = client.db.exclusive_tx()?;
    let row = instance_row(instance);
    let cells = tx.read(INSTANCES, row)?.expect("instance");
    let workflow = uuid_cell(&cells, "workflow");
    let current_step = u64_cell(&cells, "currentStep");
    let next_step = current_step + 1;
    let state = if next_step >= steps_per_instance as u64 {
        "completed"
    } else {
        "running"
    };
    tx.insert_with_id(
        INSTANCES,
        row,
        cells_map([
            ("workflow", Value::Uuid(workflow.0)),
            ("state", Value::String(state.to_owned())),
            ("currentStep", Value::U64(next_step)),
            ("wakeAt", Value::U64(0)),
        ]),
    )?;
    let _tx_id = tx.commit()?;
    client.db.tick()?;
    let unit = client
        .outbound
        .borrow_mut()
        .pop_front()
        .expect("db exclusive transition should upload a commit unit");
    let SyncMessage::CommitUnit { tx, versions } = unit.clone() else {
        unreachable!();
    };
    client.edge.ingest_relay_commit_unit(tx, versions).unwrap();
    let _ = now_ms;
    let updates = core.apply_sync_message(unit).unwrap();
    let mut accepted = false;
    let mut rejection = None;
    for update in updates {
        if let SyncMessage::FateUpdate { fate, .. } = &update {
            match fate {
                Fate::Accepted => accepted = true,
                Fate::Rejected(reason) => rejection = Some(reason.clone()),
                Fate::Pending => {}
            }
        }
        client.edge.apply_sync_message(update.clone()).unwrap();
        client.inbound.borrow_mut().push_back(update);
        client.db.tick()?;
    }
    if matches!(
        rejection,
        Some(RejectionReason::ClientClockTooFarAhead | RejectionReason::CausalityViolation)
    ) {
        panic!("unexpected transition rejection: {rejection:?}");
    }
    if accepted {
        assert_eq!(current_step, expected_step);
    }
    Ok(accepted)
}

fn append_step_and_event(
    core: &mut NodeState<RocksDbStorage>,
    global_seq: &mut u64,
    transition: Transition,
    now_ms: u64,
) {
    let instance = instance_row(transition.instance);
    let step_tx = core
        .commit_mergeable(
            MergeableCommit::new(
                STEPS,
                step_row(transition.instance, transition.to_step),
                now_ms,
            )
            .cells(cells_map([
                ("instance", Value::Uuid(instance.0)),
                ("seq", Value::U64(transition.to_step)),
                ("kind", Value::String("activity".to_owned())),
                (
                    "input",
                    Value::String(format!("{{\"from\":{}}}", transition.from_step)),
                ),
                (
                    "output",
                    Value::String(format!("{{\"to\":{}}}", transition.to_step)),
                ),
                ("status", Value::String("completed".to_owned())),
            ])),
        )
        .unwrap();
    accept_global(core, step_tx, global_seq);
    let event_tx = core
        .commit_mergeable(
            MergeableCommit::new(
                EVENTS,
                event_row(transition.instance, transition.to_step),
                now_ms,
            )
            .cells(cells_map([
                ("instance", Value::Uuid(instance.0)),
                ("seq", Value::U64(transition.to_step)),
                (
                    "payload",
                    Value::Bytes(
                        format!("{}:{}", transition.instance, transition.to_step).into_bytes(),
                    ),
                ),
            ])),
        )
        .unwrap();
    accept_global(core, event_tx, global_seq);
}

fn seed_fixture(config: &Config, core: &mut NodeState<RocksDbStorage>, global_seq: &mut u64) {
    let tx = core
        .commit_mergeable(
            MergeableCommit::new(WORKFLOWS, workflow_row(), 1).cells(cells_map([
                ("name", Value::String("workflow-main".to_owned())),
                (
                    "definition",
                    Value::String(format!("{{\"steps\":{}}}", config.steps_per_instance)),
                ),
            ])),
        )
        .unwrap();
    accept_global(core, tx, global_seq);
    for instance in 0..config.instances {
        let tx = core
            .commit_mergeable(
                MergeableCommit::new(INSTANCES, instance_row(instance), 2).cells(cells_map([
                    ("workflow", Value::Uuid(workflow_row().0)),
                    ("state", Value::String("running".to_owned())),
                    ("currentStep", Value::U64(0)),
                    ("wakeAt", Value::U64(0)),
                ])),
            )
            .unwrap();
        accept_global(core, tx, global_seq);
    }
}

fn run_sqlite_reference(config: &Config, schedule: &[Transition]) -> BaselineSummary {
    let conn = sqlite_fixture(config);
    let start = Instant::now();
    for transition in schedule {
        let changed = conn
            .execute(
                "UPDATE instances SET current_step = ?1, state = ?2 WHERE id = ?3 AND current_step = ?4",
                params![
                    transition.to_step as i64,
                    if transition.to_step >= config.steps_per_instance as u64 { "completed" } else { "running" },
                    transition.instance as i64,
                    transition.from_step as i64,
                ],
            )
            .unwrap();
        assert_eq!(changed, 1);
    }
    let elapsed_us = start.elapsed().as_micros();
    BaselineSummary {
        elapsed_us,
        bytes: 0,
        tx_per_sec: schedule.len() as f64 / (elapsed_us as f64 / 1_000_000.0).max(0.000_001),
    }
}

fn assert_sqlite_replay_matches(
    config: &Config,
    schedule: &[Transition],
    jazz: &BTreeMap<usize, u64>,
) {
    let conn = sqlite_fixture(config);
    for transition in schedule {
        conn.execute(
            "UPDATE instances SET current_step = ?1, state = ?2 WHERE id = ?3 AND current_step = ?4",
            params![
                transition.to_step as i64,
                if transition.to_step >= config.steps_per_instance as u64 { "completed" } else { "running" },
                transition.instance as i64,
                transition.from_step as i64,
            ],
        )
        .unwrap();
    }
    for instance in 0..config.instances {
        let step: i64 = conn
            .query_row(
                "SELECT current_step FROM instances WHERE id = ?1",
                [instance as i64],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(Some(&(step as u64)), jazz.get(&instance));
    }
}

fn sqlite_fixture(config: &Config) -> Connection {
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open(dir.path().join("s9.sqlite")).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    conn.execute_batch(
        "
        CREATE TABLE instances(id INTEGER PRIMARY KEY, current_step INTEGER NOT NULL, state TEXT NOT NULL);
        ",
    )
    .unwrap();
    for instance in 0..config.instances {
        conn.execute(
            "INSERT INTO instances VALUES (?1, 0, 'running')",
            [instance as i64],
        )
        .unwrap();
    }
    conn
}

fn run_log_floor(_config: &Config, schedule: &[Transition]) -> BaselineSummary {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.log");
    let mut writer = BufWriter::new(File::create(&path).unwrap());
    let start = Instant::now();
    for transition in schedule {
        let payload = format!("{}:{}", transition.instance, transition.to_step);
        writer
            .write_all(&(payload.len() as u32).to_le_bytes())
            .unwrap();
        writer.write_all(payload.as_bytes()).unwrap();
    }
    writer.flush().unwrap();
    writer.get_ref().sync_all().unwrap();
    let elapsed_us = start.elapsed().as_micros();
    BaselineSummary {
        elapsed_us,
        bytes: fs::metadata(path).unwrap().len(),
        tx_per_sec: schedule.len() as f64 / (elapsed_us as f64 / 1_000_000.0).max(0.000_001),
    }
}

struct SummaryInputs<'a> {
    config: &'a Config,
    profile: &'a PeerProfile,
    jazz: &'a JazzSummary,
    sqlite: &'a BaselineSummary,
    log: &'a BaselineSummary,
    max_instances_within_slo: usize,
    within_slo: bool,
    phase: &'a str,
}

fn emit_summary(input: SummaryInputs<'_>) {
    let SummaryInputs {
        config,
        profile,
        jazz,
        sqlite,
        log,
        max_instances_within_slo,
        within_slo,
        phase,
    } = input;
    let steps = jazz.accepted_schedule.len().max(1) as f64;
    let mut fields = metadata_fields(
        "s9_durable_execution",
        "synchronous",
        config.seed,
        &profile.name,
    );
    fields.insert("phase".to_owned(), json!(phase));
    fields.insert("instances".to_owned(), json!(config.instances));
    fields.insert(
        "steps_per_instance".to_owned(),
        json!(config.steps_per_instance),
    );
    fields.insert(
        "committed_transitions".to_owned(),
        json!(jazz.accepted_schedule.len()),
    );
    fields.insert("attempts".to_owned(), json!(jazz.attempts));
    fields.insert("rejects".to_owned(), json!(jazz.rejects));
    fields.insert("injected_races".to_owned(), json!(jazz.injected_races));
    fields.insert(
        "double_advance_rejects".to_owned(),
        json!(jazz.double_advance_rejects),
    );
    fields.insert("double_advances".to_owned(), json!(jazz.double_advances));
    fields.insert(
        "transition_settle_p50_us".to_owned(),
        json!(jazz.transition_latency.value_at_quantile(0.5)),
    );
    fields.insert(
        "transition_settle_p95_us".to_owned(),
        json!(jazz.transition_latency.value_at_quantile(0.95)),
    );
    fields.insert(
        "dashboard_p95_us".to_owned(),
        json!(jazz.dashboard_latency.value_at_quantile(0.95)),
    );
    fields.insert(
        "tail_p95_us".to_owned(),
        json!(jazz.tail_latency.value_at_quantile(0.95)),
    );
    fields.insert(
        "resume_p95_us".to_owned(),
        json!(jazz.resume_latency.value_at_quantile(0.95)),
    );
    fields.insert(
        "link_rtt_floor_us".to_owned(),
        json!(profile.one_way_latency_ms * 2_000),
    );
    fields.insert(
        "max_concurrent_instances_within_slo".to_owned(),
        json!(max_instances_within_slo),
    );
    fields.insert("within_slo".to_owned(), json!(within_slo));
    fields.insert(
        "aggregate_transitions_per_sec".to_owned(),
        json!(steps / (jazz.aggregate_elapsed_us as f64 / 1_000_000.0).max(0.000_001)),
    );
    fields.insert(
        "aggregate_elapsed_us".to_owned(),
        json!(jazz.aggregate_elapsed_us),
    );
    fields.insert(
        "aggregate_with_assertions_transitions_per_sec".to_owned(),
        json!(
            steps / (jazz.aggregate_with_assertions_elapsed_us as f64 / 1_000_000.0).max(0.000_001)
        ),
    );
    fields.insert(
        "aggregate_with_assertions_elapsed_us".to_owned(),
        json!(jazz.aggregate_with_assertions_elapsed_us),
    );
    fields.insert(
        "settle_transitions_per_sec".to_owned(),
        json!(steps / (jazz.settle_elapsed_us as f64 / 1_000_000.0).max(0.000_001)),
    );
    fields.insert(
        "assertion_elapsed_us".to_owned(),
        json!(jazz.assertion_elapsed_us),
    );
    fields.insert(
        "cold_resume_elapsed_us".to_owned(),
        json!(jazz.cold_resume_elapsed_us),
    );
    fields.insert(
        "cold_resume_transitions_per_sec".to_owned(),
        json!(steps / (jazz.cold_resume_elapsed_us as f64 / 1_000_000.0).max(0.000_001)),
    );
    fields.insert("sqlite_tx_per_sec".to_owned(), json!(sqlite.tx_per_sec));
    fields.insert("sqlite_elapsed_us".to_owned(), json!(sqlite.elapsed_us));
    fields.insert("log_floor_elapsed_us".to_owned(), json!(log.elapsed_us));
    fields.insert(
        "log_floor_bytes_per_step".to_owned(),
        json!(log.bytes as f64 / steps),
    );
    fields.insert(
        "fixture_current_state_bytes".to_owned(),
        json!(jazz.fixture_current_state_bytes),
    );
    fields.insert(
        "transition_history_bytes".to_owned(),
        json!(jazz.transition_history_bytes),
    );
    fields.insert(
        "transition_history_bytes_per_step".to_owned(),
        json!(jazz.transition_history_bytes as f64 / steps),
    );
    fields.insert(
        "total_storage_bytes".to_owned(),
        json!(jazz.total_storage_bytes),
    );
    fields.insert(
        "total_storage_bytes_per_step_including_fixture".to_owned(),
        json!(jazz.total_storage_bytes as f64 / steps),
    );
    fields.insert("sync_bytes".to_owned(), json!(jazz.sync_bytes));
    fields.insert("resume_bytes".to_owned(), json!(jazz.resume_bytes));
    fields.insert("same_schedule_replay".to_owned(), json!("matched"));
    fields.insert(
        "correctness".to_owned(),
        json!("gap_free_monotone_no_double_advance_resume_exact"),
    );
    emit_json_line(
        "s9_durable_execution",
        &JsonValue::Object(fields).to_string(),
    );
}

fn assert_dashboard_matches(
    node: &mut NodeState<RocksDbStorage>,
    oracle: &[u64],
    steps_per_instance: usize,
) {
    let schema = schema();
    let table = table_schema(&schema, INSTANCES);
    let running = node
        .current_rows(INSTANCES, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .filter(|row| row.cell(table, "state") == Some(Value::String("running".to_owned())))
        .count();
    let expected = oracle
        .iter()
        .filter(|step| **step < steps_per_instance as u64)
        .count();
    assert_eq!(running, expected);
}

fn assert_tailers_gap_free(node: &mut NodeState<RocksDbStorage>, oracle: &[u64]) {
    let schema = schema();
    let table = table_schema(&schema, EVENTS);
    let mut seen = BTreeMap::<usize, BTreeSet<u64>>::new();
    for row in node.current_rows(EVENTS, DurabilityTier::Local).unwrap() {
        let instance = instance_idx(RowUuid(match row.cell(table, "instance").unwrap() {
            Value::Uuid(uuid) => uuid,
            other => panic!("expected uuid, got {other:?}"),
        }));
        let seq = value_u64(row.cell(table, "seq").unwrap());
        seen.entry(instance).or_default().insert(seq);
    }
    for (instance, expected_step) in oracle.iter().copied().enumerate() {
        let actual = seen.get(&instance).cloned().unwrap_or_default();
        assert_eq!(actual.len() as u64, expected_step);
        for seq in 1..=expected_step {
            assert!(actual.contains(&seq), "missing event {instance}:{seq}");
        }
    }
}

fn assert_resume_matches(node: &mut NodeState<RocksDbStorage>, oracle: &[u64]) {
    let schema = schema();
    let table = table_schema(&schema, INSTANCES);
    let rows = node.current_rows(INSTANCES, DurabilityTier::Local).unwrap();
    for row in rows {
        let instance = instance_idx(row.row_uuid());
        assert_eq!(
            value_u64(row.cell(table, "currentStep").unwrap()),
            oracle[instance]
        );
    }
}

fn sync_tables(
    core: &mut NodeState<RocksDbStorage>,
    node: &mut NodeState<RocksDbStorage>,
    peer: &mut PeerState,
    tables: &[&str],
) {
    for table in tables {
        let update = peer.current_rows_update(core, table).unwrap();
        node.apply_sync_message(update).unwrap();
    }
}

fn sync_worker_tables(
    core: &mut NodeState<RocksDbStorage>,
    worker: &mut WorkerHarness,
    tables: &[&str],
) {
    for table in tables {
        let update = worker.edge_peer.current_rows_update(core, table).unwrap();
        worker.edge.apply_sync_message(update).unwrap();
        let update = worker
            .client_peer
            .current_rows_update(&mut worker.edge, table)
            .unwrap();
        worker.inbound.borrow_mut().push_back(update);
        worker.db.tick().unwrap();
    }
}

fn open_worker(node_uuid: NodeUuid, edge_uuid: NodeUuid, schema: JazzSchema) -> WorkerHarness {
    let (dir, db) = open_db(
        node_uuid,
        schema.clone(),
        AuthorId::from_bytes([node_uuid.as_bytes()[0]; 16]),
    );
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let inbound = Rc::new(RefCell::new(VecDeque::new()));
    let upstream = db.connect_upstream(Box::new(QueueTransport {
        outbound: Rc::clone(&outbound),
        inbound: Rc::clone(&inbound),
    }));
    let (edge_dir, edge) = open_node(edge_uuid, schema);
    WorkerHarness {
        _dir: dir,
        db,
        _edge_dir: edge_dir,
        edge,
        edge_peer: PeerState::new(),
        client_peer: PeerState::new(),
        outbound,
        inbound,
        _upstream: upstream,
    }
}

fn accept_global(core: &mut NodeState<RocksDbStorage>, tx: jazz::tx::TxId, global_seq: &mut u64) {
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalSeq(*global_seq)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    *global_seq += 1;
}

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            WORKFLOWS,
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("definition", ColumnType::String),
            ],
        ),
        TableSchema::new(
            INSTANCES,
            [
                ColumnSchema::new("workflow", ColumnType::Uuid),
                ColumnSchema::new("state", ColumnType::String),
                ColumnSchema::new("currentStep", ColumnType::U64),
                ColumnSchema::new("wakeAt", ColumnType::U64),
            ],
        )
        .with_reference("workflow", WORKFLOWS),
        TableSchema::new(
            STEPS,
            [
                ColumnSchema::new("instance", ColumnType::Uuid),
                ColumnSchema::new("seq", ColumnType::U64),
                ColumnSchema::new("kind", ColumnType::String),
                ColumnSchema::new("input", ColumnType::String),
                ColumnSchema::new("output", ColumnType::String),
                ColumnSchema::new("status", ColumnType::String),
            ],
        )
        .with_reference("instance", INSTANCES),
        TableSchema::new(
            EVENTS,
            [
                ColumnSchema::new("instance", ColumnType::Uuid),
                ColumnSchema::new("seq", ColumnType::U64),
                ColumnSchema::new("payload", ColumnType::Bytes),
            ],
        )
        .with_reference("instance", INSTANCES),
    ])
}

fn open_node(node_uuid: NodeUuid, schema: JazzSchema) -> (TempDir, NodeState<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let node = NodeState::new(node_uuid, schema, storage).unwrap();
    (dir, node)
}

fn open_db(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    author: AuthorId,
) -> (TempDir, Db<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(u64::from_le_bytes(
            node_uuid.as_bytes()[..8]
                .try_into()
                .expect("node seed bytes"),
        )))),
        large_value_checkpoint_op_interval: 1024,
    }))
    .unwrap();
    (dir, db)
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn storage_bytes(path: &std::path::Path) -> u64 {
    fs::read_dir(path)
        .into_iter()
        .flat_map(|entries| entries.flatten())
        .map(|entry| {
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                storage_bytes(&entry.path())
            } else {
                meta.len()
            }
        })
        .sum()
}

fn view_update_bytes(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory,
            result_row_adds,
            result_row_removes,
            ..
        } => {
            version_bundles
                .iter()
                .flat_map(|bundle| bundle.versions.iter())
                .map(|version| version.record().raw().len() as u64 + 64)
                .sum::<u64>()
                + (peer_payload_inventory.complete_tx_payloads.len() as u64 * 24)
                + ((result_row_adds.len() + result_row_removes.len()) as u64 * 64)
        }
        _ => 0,
    }
}

fn table_schema<'a>(schema: &'a JazzSchema, table: &str) -> &'a TableSchema {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .unwrap()
}

fn cells_map<const N: usize>(items: [(&str, Value); N]) -> BTreeMap<String, Value> {
    items.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

fn u64_cell(cells: &BTreeMap<String, Value>, key: &str) -> u64 {
    value_u64(cells.get(key).unwrap().clone())
}

fn uuid_cell(cells: &BTreeMap<String, Value>, key: &str) -> RowUuid {
    match cells.get(key).unwrap() {
        Value::Uuid(uuid) => RowUuid(*uuid),
        other => panic!("expected uuid cell {key}, got {other:?}"),
    }
}

fn value_u64(value: Value) -> u64 {
    match value {
        Value::U64(value) => value,
        other => panic!("expected u64, got {other:?}"),
    }
}

fn workflow_row() -> RowUuid {
    row(1, 0)
}

fn instance_row(instance: usize) -> RowUuid {
    row(2, instance as u64)
}

fn step_row(instance: usize, seq: u64) -> RowUuid {
    row(3, (instance as u64) << 32 | seq)
}

fn event_row(instance: usize, seq: u64) -> RowUuid {
    row(4, (instance as u64) << 32 | seq)
}

fn instance_idx(row: RowUuid) -> usize {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&row.as_bytes()[8..]);
    u64::from_be_bytes(bytes) as usize
}

fn row(tag: u8, value: u64) -> RowUuid {
    let mut bytes = [tag; 16];
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .filter(|value| *value > 0)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}
