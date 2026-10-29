//! S9 durable execution: workflow instances advanced step by step by
//! workers that coordinate only through serializable transactions.
//!
//! Each transition reads an instance's `currentStep`, advances it, and
//! records the step and an event in the same transaction. Every few
//! transitions two workers race on the same instance from the same read;
//! the authority must reject the second. A dashboard follows running
//! instances, tailers follow the event log, a worker crashes and resumes
//! mid-run, and a late tailer rehydrates the whole log.
//!
//! Gates: injected races equal double-advance rejects with zero double
//! advances; the dashboard and tailers match the oracle after every
//! transition and every tailer's log is gap-free (`INV-BENCH-8`); the
//! accepted schedule replayed on plain SQLite ends in the same state
//! (`INV-BENCH-7`).

use std::collections::BTreeMap;
use std::io::Write;
use std::time::Instant;

use jazz::db::{Db, ReadOpts};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_sim::oracle::{Live, cell};
use jazz_sim::support::{allow_all, cells, compile, insert, int, open, row, text, user, uuid};
use jazz_sim::{
    Latencies, PauseMode, PeerProfile, Sim, SimulatorTransportCodec, bench_profile, emit_summary,
    env_u64, env_usize,
};
use serde_json::{Value as JsonValue, json};

const WORKFLOWS: &str = "workflows";
const INSTANCES: &str = "instances";
const STEPS: &str = "steps";
const EVENTS: &str = "events";

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let profile = bench_profile();
    let config = Config {
        seed: env_u64("JAZZ_SEED", 0x5900_0001),
        profile: format!("{profile:?}").to_lowercase(),
        instances: env_usize("JAZZ_S9_INSTANCES", profile.select(10, 50, 200)).max(1),
        steps_per_instance: env_usize("JAZZ_S9_STEPS_PER_INSTANCE", profile.select(3, 4, 5)).max(1),
        transitions: env_usize("JAZZ_S9_TRANSITIONS", profile.select(30, 100, 400)).max(1),
        workers: env_usize("JAZZ_S9_WORKERS", profile.select(2, 3, 4)).max(2),
        tailers: env_usize("JAZZ_S9_TAILERS", profile.select(1, 2, 4)),
        race_every: env_usize("JAZZ_S9_RACE_EVERY", profile.select(5, 8, 10)).max(2),
        link: PeerProfile::new(
            "regional",
            env_u64("JAZZ_LINK_ONE_WAY_MS", 20),
            env_u64("JAZZ_LINK_JITTER_MS", 5),
            0,
        ),
    };
    run(&config, true);
}

#[allow(dead_code)]
pub fn smoke() {
    run(
        &Config {
            seed: 0x5900_0001,
            profile: "s9-smoke".to_owned(),
            instances: 8,
            steps_per_instance: 3,
            transitions: 12,
            workers: 2,
            tailers: 2,
            race_every: 3,
            link: PeerProfile::new("s9-smoke", 2, 1, 0),
        },
        false,
    );
}

struct Config {
    seed: u64,
    profile: String,
    instances: usize,
    steps_per_instance: usize,
    transitions: usize,
    workers: usize,
    tailers: usize,
    race_every: usize,
    link: PeerProfile,
}

fn schema() -> JazzSchema {
    let table = TableSchemaBuilder::new;
    compile(
        &SchemaBuilder::new()
            .table(
                table(WORKFLOWS)
                    .column("name", ColumnType::Text)
                    .column("definition", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                table(INSTANCES)
                    .fk_column("workflow", WORKFLOWS)
                    .column("state", ColumnType::Text)
                    .column("currentStep", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .table(
                table(STEPS)
                    .fk_column("instance", INSTANCES)
                    .column("seq", ColumnType::BigInt)
                    .column("output", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                table(EVENTS)
                    .fk_column("instance", INSTANCES)
                    .column("seq", ColumnType::BigInt)
                    .column("payload", ColumnType::Text)
                    .policies(allow_all()),
            )
            .build(),
    )
}

fn workflow() -> RowUuid {
    row(0x90, 0)
}

fn instance(n: usize) -> RowUuid {
    row(0x91, n as u64)
}

/// Steps and events are keyed by (instance, seq), so a double advance
/// would show as two transactions writing one row, never as a silent gap.
fn step_row(n: usize, seq: u64) -> RowUuid {
    row(0x92, ((n as u64) << 16) | seq)
}

fn event_row(n: usize, seq: u64) -> RowUuid {
    row(0x93, ((n as u64) << 16) | seq)
}

fn state_for(step: u64, steps_per_instance: usize) -> &'static str {
    if step >= steps_per_instance as u64 {
        "completed"
    } else {
        "running"
    }
}

fn get_int(row: &jazz::db::CurrentRow, column: &str) -> i64 {
    match cell(row, column) {
        Some(Value::I64(v)) => v,
        other => panic!("{column}: {other:?}"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Transition {
    instance: usize,
    from: u64,
}

/// One worker's attempt to advance `instance`: read its current step in a
/// serializable transaction, then stage the advance, the step and the event.
struct Attempt<'a> {
    sim: &'a Sim,
    db: &'a Db,
    tx: jazz::db::SerializableTx<'a>,
    transition: Transition,
}

impl<'a> Attempt<'a> {
    fn stage(sim: &'a Sim, db: &'a Db, n: usize, steps_per_instance: usize) -> Self {
        let tx = sim.drive(db.serializable_tx()).expect("begin serializable");
        let prepared = db
            .prepare_query(&Query::from(INSTANCES).filter(eq(col("id"), lit(uuid(instance(n))))))
            .expect("prepare");
        let rows = sim
            .drive(tx.all_prepared(&prepared))
            .expect("serializable read");
        assert_eq!(rows.len(), 1, "instance {n}");
        let from = get_int(&rows[0], "currentStep") as u64;
        let to = from + 1;
        sim.drive(tx.update(
            INSTANCES,
            instance(n),
            cells([
                ("currentStep", int(to as i64)),
                ("state", text(state_for(to, steps_per_instance))),
            ]),
            Default::default(),
        ))
        .unwrap_or_else(|error| panic!("stage advance: {error}"));
        for (table, id, payload_column, payload) in [
            (STEPS, step_row(n, to), "output", format!("output-{n}-{to}")),
            (
                EVENTS,
                event_row(n, to),
                "payload",
                format!("event-{n}-{to}"),
            ),
        ] {
            sim.drive(tx.insert(
                table,
                cells([
                    ("instance", uuid(instance(n))),
                    ("seq", int(to as i64)),
                    (payload_column, text(payload)),
                ]),
                jazz::db::InsertOptions {
                    row_id: Some(id),
                    ..Default::default()
                },
            ))
            .unwrap_or_else(|error| panic!("stage {table}: {error}"));
        }
        Self {
            sim,
            db,
            tx,
            transition: Transition { instance: n, from },
        }
    }

    /// Commit and wait for the authority. True if accepted.
    fn commit(self) -> bool {
        let outcome = match self.sim.drive(self.tx.commit()) {
            Ok(receipt) => self
                .sim
                .drive(
                    self.db
                        .wait_for_transaction(receipt.0, DurabilityTier::Global),
                )
                .map(|_| ()),
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => true,
            Err(error) if error.is_conflict() => false,
            Err(error) => panic!("unexpected commit failure: {error}"),
        }
    }
}

/// The dashboard: running instances and their current step.
fn assert_dashboard(dashboard: &Live, oracle: &[u64], steps_per_instance: usize) {
    let expected: BTreeMap<RowUuid, i64> = oracle
        .iter()
        .enumerate()
        .filter(|(_, step)| **step < steps_per_instance as u64)
        .map(|(n, step)| (instance(n), *step as i64))
        .collect();
    let actual: BTreeMap<RowUuid, i64> = dashboard
        .rows
        .iter()
        .map(|(id, cells)| match cells.get("currentStep") {
            Some(Value::I64(step)) => (*id, *step),
            other => panic!("currentStep: {other:?}"),
        })
        .collect();
    assert_eq!(
        actual, expected,
        "the dashboard shows exactly the running instances"
    );
}

/// A tailer holds every event the oracle has accepted, with no gaps, and
/// never lost one it had seen.
fn assert_tailer(tailer: &Live, oracle: &[u64]) {
    let expected: Vec<RowUuid> = oracle
        .iter()
        .enumerate()
        .flat_map(|(n, step)| (1..=*step).map(move |seq| event_row(n, seq)))
        .collect();
    let mut actual: Vec<RowUuid> = tailer.ids().into_iter().collect();
    actual.sort();
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    assert_eq!(
        actual, expected_sorted,
        "a tailer's event log is complete and gap-free"
    );
    assert_eq!(tailer.removed, 0, "an event log is prefix-monotone");
}

fn next_runnable(oracle: &[u64], offset: usize, steps_per_instance: usize) -> usize {
    (0..oracle.len())
        .map(|i| (offset + i) % oracle.len())
        .find(|n| oracle[*n] < steps_per_instance as u64)
        .expect("transitions exceed the instances' step capacity")
}

fn run(config: &Config, emit: bool) {
    assert!(
        config.transitions <= config.instances * config.steps_per_instance,
        "the run needs enough steps to advance"
    );
    let mut sim = Sim::new(config.seed, SimulatorTransportCodec::WireFrames);
    let core = sim.add("core", open(schema(), 0, AuthorSubject::SYSTEM));
    insert(
        core,
        WORKFLOWS,
        workflow(),
        cells([
            ("name", text("order-fulfilment")),
            ("definition", text("charge;pack;ship;notify;close")),
        ]),
    );
    for n in 0..config.instances {
        insert(
            core,
            INSTANCES,
            instance(n),
            cells([
                ("workflow", uuid(workflow())),
                ("state", text("running")),
                ("currentStep", int(0)),
            ]),
        );
    }

    let workers: Vec<String> = (0..config.workers).map(|w| format!("worker-{w}")).collect();
    for (w, name) in workers.iter().enumerate() {
        sim.add(
            name.clone(),
            open(schema(), 1 + w as u32, user(1 + w as u32)),
        );
        sim.connect(name, "core", user(1 + w as u32), config.link.clone());
    }
    sim.add("dashboard", open(schema(), 100, user(100)));
    sim.connect("dashboard", "core", user(100), config.link.clone());
    let running = Query::from(INSTANCES).filter(eq(col("state"), lit(text("running"))));
    let mut dashboard = Live::subscribe(sim.db("dashboard"), &running, ReadOpts::default());
    let mut tailers: Vec<Live> = (0..config.tailers)
        .map(|t| {
            let name = format!("tailer-{t}");
            sim.add(
                name.clone(),
                open(schema(), 200 + t as u32, user(200 + t as u32)),
            );
            sim.connect(&name, "core", user(200 + t as u32), config.link.clone());
            Live::subscribe(sim.db(&name), &Query::from(EVENTS), ReadOpts::default())
        })
        .collect();
    sim.settle();
    sim.reset_stats();

    let mut oracle = vec![0_u64; config.instances];
    let mut schedule: Vec<Transition> = Vec::new();
    let (
        mut attempts,
        mut rejects,
        mut injected_races,
        mut double_advance_rejects,
        mut double_advances,
    ) = (0, 0, 0, 0, 0);
    let mut transition_us = Latencies::default();
    let mut dashboard_ms = Latencies::default();
    let mut tail_ms = Latencies::default();
    let crash_at = config.transitions / 2;
    let started = Instant::now();
    while schedule.len() < config.transitions {
        let n = next_runnable(&oracle, schedule.len(), config.steps_per_instance);
        if schedule.len() == crash_at {
            // A worker crashes between transitions; its peers keep going
            // and it resumes from whatever the authority holds.
            sim.pause(&workers[0], PauseMode::Disconnect);
        }
        if schedule.len() == crash_at + config.workers {
            sim.resume(&workers[0]);
            sim.settle();
        }
        let crashed =
            |w: usize| w == 0 && (crash_at..crash_at + config.workers).contains(&schedule.len());
        let race =
            !schedule.is_empty() && schedule.len().is_multiple_of(config.race_every) && !crashed(0);
        let attempt_started = Instant::now();
        let before_ms = sim.now_ms();
        if race {
            // Both workers read the same step before either commits.
            let first = Attempt::stage(&sim, sim.db(&workers[0]), n, config.steps_per_instance);
            let second = Attempt::stage(&sim, sim.db(&workers[1]), n, config.steps_per_instance);
            assert_eq!(
                first.transition, second.transition,
                "the racers read the same step"
            );
            let transition = first.transition;
            attempts += 2;
            injected_races += 1;
            assert!(first.commit(), "the first racer commits");
            if second.commit() {
                double_advances += 1;
            } else {
                rejects += 1;
                double_advance_rejects += 1;
            }
            schedule.push(transition);
        } else {
            let w = (1..=config.workers)
                .map(|i| (schedule.len() + i) % config.workers)
                .find(|w| !crashed(*w))
                .expect("a live worker");
            let attempt = Attempt::stage(&sim, sim.db(&workers[w]), n, config.steps_per_instance);
            let transition = attempt.transition;
            attempts += 1;
            if attempt.commit() {
                schedule.push(transition);
            } else {
                rejects += 1;
                continue;
            }
        }
        transition_us.record_duration_us(attempt_started.elapsed());
        let transition = *schedule.last().expect("accepted");
        assert_eq!(
            transition.from, oracle[n],
            "a transition advances from the accepted step"
        );
        oracle[n] += 1;

        // Dashboard and tailers follow; time is virtual.
        let committed_ms = sim.now_ms();
        sim.settle();
        dashboard.poll();
        assert_dashboard(&dashboard, &oracle, config.steps_per_instance);
        dashboard_ms.record(sim.now_ms().saturating_sub(committed_ms));
        for tailer in &mut tailers {
            tailer.poll();
            assert_tailer(tailer, &oracle);
        }
        tail_ms.record(sim.now_ms().saturating_sub(before_ms));
    }
    let run_us = started.elapsed().as_micros() as u64;
    let (run_up, run_down) = sim.total_stats();

    assert_eq!(
        injected_races, double_advance_rejects,
        "every injected race is rejected"
    );
    assert_eq!(
        double_advances, 0,
        "no instance advances twice from one step"
    );
    assert!(injected_races > 0, "the run injects races");

    // A late tailer rehydrates the whole log.
    sim.add("late-tailer", open(schema(), 300, user(300)));
    sim.connect("late-tailer", "core", user(300), config.link.clone());
    let resume_started = sim.now_ms();
    let mut late = Live::subscribe(
        sim.db("late-tailer"),
        &Query::from(EVENTS),
        ReadOpts::default(),
    );
    sim.settle();
    late.poll();
    assert_tailer(&late, &oracle);
    let resume_ms = sim.now_ms() - resume_started;

    // The authority's end state equals plain SQLite replaying the schedule.
    let jazz_steps: Vec<u64> = (0..config.instances)
        .map(|n| {
            let rows = jazz_sim::oracle::read(
                sim.db("core"),
                &Query::from(INSTANCES).filter(eq(col("id"), lit(uuid(instance(n))))),
                ReadOpts::default(),
            );
            match rows
                .values()
                .next()
                .and_then(|cells| cells.get("currentStep"))
            {
                Some(Value::I64(step)) => *step as u64,
                other => panic!("currentStep: {other:?}"),
            }
        })
        .collect();
    assert_eq!(jazz_steps, oracle, "the authority holds the oracle's steps");
    let (sqlite_steps, sqlite_us) = sqlite_replay(config, &schedule);
    assert_eq!(
        sqlite_steps, jazz_steps,
        "SQLite replay of the accepted schedule matches"
    );
    for (n, steps) in oracle.iter().enumerate() {
        let events = jazz_sim::oracle::read(
            sim.db("core"),
            &Query::from(EVENTS).filter(eq(col("instance"), lit(uuid(instance(n))))),
            ReadOpts::default(),
        );
        assert_eq!(
            events.len() as u64,
            *steps,
            "one event per accepted step of instance {n}"
        );
    }

    if emit {
        let (log_bytes, log_us) = log_floor(&schedule);
        let mut fields = sim.json_fields();
        fields.insert("phase".to_owned(), json!("summary"));
        fields.insert("instances".to_owned(), json!(config.instances));
        fields.insert("transitions".to_owned(), json!(schedule.len()));
        fields.insert("workers".to_owned(), json!(config.workers));
        fields.insert("tailers".to_owned(), json!(config.tailers));
        fields.insert("attempts".to_owned(), json!(attempts));
        fields.insert("rejects".to_owned(), json!(rejects));
        fields.insert("injected_races".to_owned(), json!(injected_races));
        fields.insert(
            "double_advance_rejects".to_owned(),
            json!(double_advance_rejects),
        );
        fields.insert("double_advances".to_owned(), json!(double_advances));
        fields.insert("run_us".to_owned(), json!(run_us));
        fields.insert(
            "transitions_per_sec".to_owned(),
            json!(schedule.len() as f64 / (run_us as f64 / 1e6).max(1e-6)),
        );
        fields.insert("run_bytes".to_owned(), json!(run_up.bytes + run_down.bytes));
        fields.insert("resume_ms".to_owned(), json!(resume_ms));
        fields.insert("sqlite_replay_us".to_owned(), json!(sqlite_us));
        fields.insert("log_floor_bytes".to_owned(), json!(log_bytes));
        fields.insert("log_floor_us".to_owned(), json!(log_us));
        fields.extend(transition_us.json_fields("transition_us"));
        fields.extend(dashboard_ms.json_fields("dashboard_ms"));
        fields.extend(tail_ms.json_fields("tail_ms"));
        emit_summary(
            "s9_durable_execution",
            "deterministic",
            config.seed,
            &config.profile,
            JsonValue::Object(fields),
        );
    }
}

/// Plain SQLite: a compare-and-set on the instance per transition.
fn sqlite_replay(config: &Config, schedule: &[Transition]) -> (Vec<u64>, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = rusqlite::Connection::open(dir.path().join("s9.sqlite")).expect("sqlite");
    db.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;
         CREATE TABLE instances (id INTEGER PRIMARY KEY, current_step INTEGER NOT NULL, state TEXT NOT NULL);
         CREATE TABLE events (instance INTEGER, seq INTEGER, payload TEXT, PRIMARY KEY (instance, seq));",
    )
    .expect("schema");
    for n in 0..config.instances {
        db.execute(
            "INSERT INTO instances VALUES (?1, 0, 'running')",
            [n as i64],
        )
        .expect("seed");
    }
    let started = Instant::now();
    for transition in schedule {
        let to = transition.from + 1;
        let tx = db.unchecked_transaction().expect("transaction");
        let changed = tx
            .execute(
                "UPDATE instances SET current_step = ?1, state = ?2 WHERE id = ?3 AND current_step = ?4",
                rusqlite::params![
                    to as i64,
                    state_for(to, config.steps_per_instance),
                    transition.instance as i64,
                    transition.from as i64
                ],
            )
            .expect("advance");
        assert_eq!(
            changed, 1,
            "the SQLite compare-and-set accepts the jazz schedule"
        );
        tx.execute(
            "INSERT INTO events VALUES (?1, ?2, ?3)",
            rusqlite::params![
                transition.instance as i64,
                to as i64,
                format!("event-{}-{to}", transition.instance)
            ],
        )
        .expect("event");
        tx.commit().expect("commit");
    }
    let elapsed = started.elapsed().as_micros() as u64;
    let mut statement = db
        .prepare("SELECT current_step FROM instances ORDER BY id")
        .expect("prepare");
    let steps = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .expect("query")
        .map(|step| step.expect("step") as u64)
        .collect();
    (steps, elapsed)
}

/// The floor: an append-only event log file.
fn log_floor(schedule: &[Transition]) -> (u64, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.log");
    let started = Instant::now();
    let mut file = std::io::BufWriter::new(std::fs::File::create(&path).expect("log file"));
    for transition in schedule {
        let payload = format!("event-{}-{}", transition.instance, transition.from + 1);
        file.write_all(&(payload.len() as u32).to_le_bytes())
            .expect("length");
        file.write_all(payload.as_bytes()).expect("payload");
    }
    file.flush().expect("flush");
    file.get_ref().sync_all().expect("sync");
    (
        std::fs::metadata(&path).expect("size").len(),
        started.elapsed().as_micros() as u64,
    )
}
