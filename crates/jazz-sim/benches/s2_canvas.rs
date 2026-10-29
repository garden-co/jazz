//! S2 realtime canvas: active clients drag shapes at a steady rate, passive
//! clients watch, and a spy who was never invited watches too.
//!
//! Gates: every invited replica converges on the authority, which holds
//! exactly the moves the model predicts; the spy receives zero shape rows
//! (`INV-BENCH-6`); historical cuts at 25/50/75% of the run read exactly
//! the prefix of moves accepted by then; a client cut off mid-run
//! re-converges after reconnecting; concurrent drags of one shape converge
//! on the authority's last arrival everywhere.

use std::collections::BTreeMap;
use std::time::Instant;

use jazz::db::{ReadOpts, block_on};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::protocol::{ReadViewSourceSpec, SnapshotRef};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, Operation, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use jazz_sim::distributions::Lcg;
use jazz_sim::oracle::{self, Live, Rows};
use jazz_sim::support::{
    allow_all, cells, compile, exists, insert, int, is_me, open, outer_eq, read_gated, row, settle,
    subject, text, user, uuid,
};
use jazz_sim::{
    Latencies, PauseMode, PeerProfile, Sim, SimulatorTransportCodec, bench_profile, emit_summary,
    env_u64,
};
use serde_json::{Map, Value as JsonValue, json};

const CANVASES: &str = "canvases";
const INVITES: &str = "invites";
const SHAPES: &str = "shapes";

/// One animation frame: coalesced drags send at most one move per shape
/// per frame.
const FRAME_MS: u64 = 50;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let profile = bench_profile();
    let config = Config {
        seed: env_u64("JAZZ_SEED", 0x5200_cafe),
        profile: format!("{profile:?}").to_lowercase(),
        shapes: profile.select(16, 64, 256),
        active: profile.select(2, 4, 8),
        passive: profile.select(2, 8, 16),
        rate_per_sec: profile.select(10, 30, 60),
        duration_secs: profile.select(2, 5, 10),
        codec: SimulatorTransportCodec::from_env("JAZZ_S2_TRANSPORT_CODEC"),
        link: PeerProfile::new(
            "regional",
            env_u64("JAZZ_LINK_ONE_WAY_MS", 20),
            env_u64("JAZZ_LINK_JITTER_MS", 5),
            0,
        ),
    };
    for coalesced in [false, true] {
        run(&config, coalesced, true);
    }
    failure(&config, true);
}

#[allow(dead_code)]
pub fn smoke() {
    let config = Config {
        seed: 0x5200_cafe,
        profile: "s2-smoke".to_owned(),
        shapes: 4,
        active: 2,
        passive: 1,
        rate_per_sec: 4,
        duration_secs: 1,
        codec: SimulatorTransportCodec::WireFrames,
        link: PeerProfile::new("s2-smoke", 2, 1, 0),
    };
    for coalesced in [false, true] {
        run(&config, coalesced, false);
        db_surface(&config, coalesced);
    }
    failure(&config, false);
}

#[derive(Clone)]
struct Config {
    seed: u64,
    profile: String,
    shapes: usize,
    active: usize,
    passive: usize,
    rate_per_sec: u64,
    duration_secs: u64,
    codec: SimulatorTransportCodec,
    link: PeerProfile,
}

impl Config {
    /// Moves each active client makes.
    fn moves_per_active(&self) -> usize {
        (self.rate_per_sec * self.duration_secs) as usize
    }
}

fn schema() -> JazzSchema {
    let table = TableSchemaBuilder::new;
    compile(
        &SchemaBuilder::new()
            .table(
                table(CANVASES)
                    .column("name", ColumnType::Text)
                    .policies(read_gated(exists(
                        INVITES,
                        vec![outer_eq("canvas", "id"), is_me("member")],
                    ))),
            )
            .table(
                table(INVITES)
                    .fk_column("canvas", CANVASES)
                    .column("member", ColumnType::Text)
                    .policies(read_gated(is_me("member"))),
            )
            .table(
                table(SHAPES)
                    .fk_column("canvas", CANVASES)
                    .column("x", ColumnType::BigInt)
                    .column("y", ColumnType::BigInt)
                    .column("color", ColumnType::Text)
                    .policies(read_gated(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "canvas".to_owned(),
                        max_depth: None,
                    })),
            )
            .table(
                table("unrelated")
                    .column("note", ColumnType::Text)
                    .policies(allow_all()),
            )
            .build(),
    )
}

fn canvas() -> RowUuid {
    row(0x20, 0)
}

fn shape(n: usize) -> RowUuid {
    row(0x22, n as u64)
}

fn position(x: i64, y: i64) -> BTreeMap<String, Value> {
    cells([("x", int(x)), ("y", int(y))])
}

fn shape_cells(x: i64, y: i64) -> BTreeMap<String, Value> {
    cells([
        ("canvas", uuid(canvas())),
        ("x", int(x)),
        ("y", int(y)),
        ("color", text("teal")),
    ])
}

/// Everyone on one canvas: the authority, `active` + `passive` invited
/// clients, and a spy.
struct Room {
    sim: Sim,
    invited: Vec<String>,
    active: Vec<String>,
    spy: Live,
    watchers: BTreeMap<String, Live>,
}

fn room(config: &Config, seed: u64) -> Room {
    let mut sim = Sim::new(seed, config.codec);
    let core = sim.add("core", open(schema(), 0, AuthorSubject::SYSTEM));
    insert(core, CANVASES, canvas(), cells([("name", text("board"))]));
    let members = config.active + config.passive;
    for m in 0..members {
        insert(
            core,
            INVITES,
            row(0x21, m as u64),
            cells([
                ("canvas", uuid(canvas())),
                ("member", subject(user(m as u32))),
            ]),
        );
    }
    for n in 0..config.shapes {
        insert(core, SHAPES, shape(n), shape_cells(0, 0));
    }
    let mut invited = Vec::new();
    let mut watchers = BTreeMap::new();
    for m in 0..members {
        let name = if m < config.active {
            format!("active-{m}")
        } else {
            format!("passive-{m}")
        };
        sim.add(name.clone(), open(schema(), 10 + m as u32, user(m as u32)));
        sim.connect(&name, "core", user(m as u32), config.link.clone());
        watchers.insert(
            name.clone(),
            Live::subscribe(sim.db(&name), &Query::from(SHAPES), ReadOpts::default()),
        );
        invited.push(name);
    }
    let spy_user = user(9_999);
    sim.add("spy", open(schema(), 9_999, spy_user));
    sim.connect("spy", "core", spy_user, config.link.clone());
    sim.record_downstream("spy");
    let spy = Live::subscribe(sim.db("spy"), &Query::from(SHAPES), ReadOpts::default());
    sim.settle();
    let active = invited[..config.active].to_vec();
    Room {
        sim,
        invited,
        active,
        spy,
        watchers,
    }
}

impl Room {
    fn core_shapes(&self) -> Rows {
        oracle::read(
            self.sim.db("core"),
            &Query::from(SHAPES),
            ReadOpts::default(),
        )
    }

    /// Every invited replica holds exactly the authority's shapes; the spy
    /// holds none and was never shipped one.
    fn assert_converged(&mut self, phase: &str) {
        let core = self.core_shapes();
        for (name, live) in &mut self.watchers {
            live.poll();
            assert_eq!(
                live.rows, core,
                "{phase}: {name} converged on the authority"
            );
        }
        for name in &self.invited {
            assert_eq!(
                oracle::read(self.sim.db(name), &Query::from(SHAPES), ReadOpts::default()),
                core,
                "{phase}: {name}'s store matches the authority"
            );
        }
        self.spy.poll();
        assert!(self.spy.rows.is_empty(), "{phase}: the spy sees no shapes");
        assert_eq!(
            self.spy.seen.len(),
            0,
            "{phase}: the spy got no shape deltas"
        );
        for message in self.sim.downstream_log("spy") {
            if let jazz::db::SyncMessage::Rows { images } = message {
                assert!(
                    images.iter().all(|image| image.deleted
                        || ![SHAPES, CANVASES, INVITES].contains(&image.table.as_str())),
                    "{phase}: the spy was shipped a canvas row: {images:?}"
                );
            }
        }
    }
}

struct Summary {
    moves: usize,
    commits: usize,
    latencies: Latencies,
    wall_us: u64,
}

/// Each active client drags the shapes it owns (shape n belongs to active
/// n % active), so the final positions do not depend on arrival order.
/// Each shape's position, by row.
type Positions = BTreeMap<RowUuid, (i64, i64)>;

fn run(config: &Config, coalesced: bool, emit: bool) -> Summary {
    let mut room = room(config, config.seed);
    let mut rng = Lcg::new(config.seed ^ u64::from(coalesced));
    let mut model: BTreeMap<RowUuid, (i64, i64)> =
        (0..config.shapes).map(|n| (shape(n), (0, 0))).collect();
    let interval_ms = 1_000 / config.rate_per_sec.max(1);
    let moves = config.moves_per_active();
    let mut commits = 0;
    let mut cuts: Vec<(SnapshotRef, Positions)> = Vec::new();
    let mut latencies = Latencies::default();
    let wall = Instant::now();
    // Moves staged but not yet sent, per (active, shape), for coalescing.
    let mut staged: BTreeMap<(usize, RowUuid), (i64, i64)> = BTreeMap::new();
    let mut last_flush = room.sim.now_ms();
    for step in 0..moves {
        for a in 0..config.active {
            let owned: Vec<usize> = (0..config.shapes)
                .filter(|n| n % config.active == a)
                .collect();
            if owned.is_empty() {
                continue;
            }
            let id = shape(owned[rng.usize(owned.len())]);
            let to = (rng.usize(1_000) as i64, rng.usize(1_000) as i64);
            staged.insert((a, id), to);
        }
        let flush = !coalesced || room.sim.now_ms() - last_flush >= FRAME_MS || step + 1 == moves;
        if flush {
            last_flush = room.sim.now_ms();
            for ((a, id), (x, y)) in std::mem::take(&mut staged) {
                let name = room.active[a].clone();
                let sent_at = room.sim.now_ms();
                let handle = block_on(room.sim.db(&name).update(
                    SHAPES,
                    id,
                    position(x, y),
                    Default::default(),
                ))
                .expect("drag");
                model.insert(id, (x, y));
                commits += 1;
                // Latency: until every passive watcher shows the move.
                settle(&room.sim, room.sim.db(&name), &handle);
                room.sim.settle();
                let expected = shape_cells(x, y);
                for (watcher, live) in &mut room.watchers {
                    live.poll();
                    assert_eq!(
                        live.rows.get(&id),
                        Some(&expected),
                        "{watcher} shows the move"
                    );
                }
                latencies.record(room.sim.now_ms() - sent_at);
            }
        }
        // Record historical cuts at 25/50/75% of the run.
        for quarter in 1..=3 {
            if step + 1 == moves * quarter / 4 && staged.is_empty() {
                let snapshot = room
                    .sim
                    .drive(room.sim.db("core").capture_snapshot())
                    .expect("snapshot");
                cuts.push((snapshot, model.clone()));
            }
        }
        room.sim.advance(interval_ms);
    }
    room.sim.settle();
    let wall_us = wall.elapsed().as_micros() as u64;
    room.assert_converged("live");
    let expected: Rows = model
        .iter()
        .map(|(id, (x, y))| (*id, shape_cells(*x, *y)))
        .collect();
    assert_eq!(
        room.core_shapes(),
        expected,
        "the authority holds every accepted move"
    );
    if coalesced {
        assert!(
            commits <= config.active * moves,
            "coalescing never adds commits"
        );
    } else {
        assert_eq!(commits, config.active * moves);
    }

    // Historical loads: each cut reads exactly the prefix accepted by then.
    let mut historical = Vec::new();
    for (index, (snapshot, at_cut)) in cuts.into_iter().enumerate() {
        let opts = ReadOpts {
            read_view: jazz::protocol::ReadViewSpec {
                source: ReadViewSourceSpec::Snapshot { snapshot },
                ..Default::default()
            },
            ..Default::default()
        };
        let start = Instant::now();
        let read = oracle::read(room.sim.db("core"), &Query::from(SHAPES), opts);
        let expected: Rows = at_cut
            .iter()
            .map(|(id, (x, y))| (*id, shape_cells(*x, *y)))
            .collect();
        assert_eq!(
            read,
            expected,
            "historical cut {} reads its prefix",
            index + 1
        );
        historical
            .push(json!({"cut": (index + 1) * 25, "read_us": start.elapsed().as_micros() as u64}));
    }

    if !coalesced {
        assert_eq!(historical.len(), 3, "three historical cuts were read");
    }

    // Concurrent drags of one shape: everyone converges on the authority's
    // last arrival.
    let contested = shape(0);
    let handles: Vec<_> = room
        .active
        .iter()
        .enumerate()
        .map(|(a, name)| {
            let handle = block_on(room.sim.db(name).update(
                SHAPES,
                contested,
                position(-1 - a as i64, -1),
                Default::default(),
            ))
            .expect("contested drag");
            (name.clone(), handle)
        })
        .collect();
    for (name, handle) in &handles {
        settle(&room.sim, room.sim.db(name), handle);
    }
    room.sim.settle();
    room.assert_converged("contested");
    let winner = room.core_shapes()[&contested].clone();
    assert!(
        (0..config.active).any(|a| winner == shape_cells(-1 - a as i64, -1)),
        "the contested shape holds one of the drags: {winner:?}"
    );

    let summary = Summary {
        moves: config.active * moves,
        commits,
        latencies,
        wall_us,
    };
    if emit {
        let mut fields = room.sim.json_fields();
        fields.insert("phase".to_owned(), json!("live"));
        fields.insert("coalesced".to_owned(), json!(coalesced));
        fields.insert("moves".to_owned(), json!(summary.moves));
        fields.insert("commits".to_owned(), json!(summary.commits));
        fields.insert("wall_us".to_owned(), json!(summary.wall_us));
        fields.insert("historical".to_owned(), json!(historical));
        fields.extend(summary.latencies.json_fields("visible_virtual_ms"));
        emit_summary(
            "s2_canvas",
            "deterministic",
            config.seed,
            &config.profile,
            JsonValue::Object(fields),
        );
    }
    summary
}

/// An active client is cut off mid-run and keeps dragging offline; after
/// reconnecting, every replica converges and the offline moves landed.
fn failure(config: &Config, emit: bool) {
    let mut room = room(config, config.seed ^ 0x5200_fa11);
    let name = room.active[0].clone();
    let mut model = BTreeMap::new();
    let start = Instant::now();
    room.sim.pause(&name, PauseMode::Disconnect);
    for n in (0..config.shapes).step_by(config.active.max(1)) {
        let to = (n as i64 * 7 + 1, -(n as i64) - 1);
        block_on(room.sim.db(&name).update(
            SHAPES,
            shape(n),
            position(to.0, to.1),
            Default::default(),
        ))
        .expect("offline drag");
        model.insert(shape(n), to);
    }
    room.sim.advance(1_000);
    let core = room.core_shapes();
    for (id, (x, y)) in &model {
        assert_ne!(
            core[id],
            shape_cells(*x, *y),
            "offline moves wait for the link"
        );
    }
    let reconnect_at = room.sim.now_ms();
    room.sim.resume(&name);
    room.sim
        .drive(
            room.sim
                .db(&name)
                .wait_for_pending_writes(jazz::tx::DurabilityTier::Global),
        )
        .expect("offline moves settle");
    room.sim.settle();
    room.assert_converged("failure recovery");
    let core = room.core_shapes();
    for (id, (x, y)) in &model {
        assert_eq!(core[id], shape_cells(*x, *y), "offline move landed");
    }
    if emit {
        let mut fields = Map::new();
        fields.insert("phase".to_owned(), json!("failure"));
        fields.insert("offline_moves".to_owned(), json!(model.len()));
        fields.insert(
            "recovery_virtual_ms".to_owned(),
            json!(room.sim.now_ms() - reconnect_at),
        );
        fields.insert(
            "wall_us".to_owned(),
            json!(start.elapsed().as_micros() as u64),
        );
        emit_summary(
            "s2_canvas",
            "deterministic",
            config.seed,
            &config.profile,
            JsonValue::Object(fields),
        );
    }
}

/// The canvas on one local `Db`: subscriptions follow every applied move.
fn db_surface(config: &Config, coalesced: bool) {
    let db = open(schema(), 0, AuthorSubject::SYSTEM);
    insert(&db, CANVASES, canvas(), cells([("name", text("board"))]));
    for n in 0..config.shapes {
        insert(&db, SHAPES, shape(n), shape_cells(0, 0));
    }
    let mut live = Live::subscribe(&db, &Query::from(SHAPES), ReadOpts::default());
    live.poll();
    assert_eq!(live.rows.len(), config.shapes);
    let mut rng = Lcg::new(config.seed);
    let mut model: BTreeMap<RowUuid, (i64, i64)> = BTreeMap::new();
    let moves = config.active * config.moves_per_active();
    let mut applied = 0;
    let batch = if coalesced { 4 } else { 1 };
    for chunk in (0..moves).collect::<Vec<_>>().chunks(batch) {
        let tx = block_on(db.mergeable_tx()).expect("open transaction");
        for _ in chunk {
            let id = shape(rng.usize(config.shapes));
            let to = (rng.usize(100) as i64, rng.usize(100) as i64);
            block_on(tx.update(SHAPES, id, position(to.0, to.1), Default::default()))
                .expect("move");
            model.insert(id, to);
            applied += 1;
        }
        block_on(tx.commit()).expect("commit moves");
        live.poll();
    }
    assert_eq!(applied, moves);
    for (id, (x, y)) in &model {
        assert_eq!(live.rows[id], shape_cells(*x, *y));
    }
    assert_eq!(
        live.rows,
        oracle::read(&db, &Query::from(SHAPES), ReadOpts::default())
    );
}
