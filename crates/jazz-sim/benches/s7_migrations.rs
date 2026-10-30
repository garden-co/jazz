//! S7 migration lenses: mixed-version clients against an authority that
//! has moved along a schema chain.
//!
//! v1 `todos(title)` -> v2 renames `title` to `name` -> v3 adds `priority`
//! (default 0) -> v4 renames the table to `tasks` and copies `name` into
//! `search_name`. Clients on v1 and v4 write live; a v1 client that was
//! offline since before the chain lands its queued write after the
//! authority reached v4.
//!
//! Gates: every version reads the same rows under its own names, at the
//! authority and on each client; the offline write is translated; the copy
//! holds the seeded titles and the titles of rows v1 inserts, and stays a
//! column of its own: a write of the title leaves it.

use std::collections::BTreeMap;
use std::time::Instant;

use jazz::db::{Db, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_sim::oracle::{self, Live, Rows};
use jazz_sim::support::{
    allow_all, cells, compile, config, insert, int, open, row, settle, text, user,
};
use jazz_sim::{
    Latencies, PauseMode, PeerProfile, Sim, SimulatorTransportCodec, emit_summary, env_u64,
    env_usize,
};
use serde_json::{Map, Value as JsonValue, json};

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let seed = env_u64("JAZZ_SEED", 0x5700_0001);
    let rows = env_usize("JAZZ_S7_ROWS", 1_000);
    run(seed, rows, true);
}

#[allow(dead_code)]
pub fn smoke() {
    run(0x5700_0001, 8, false);
}

fn todos(table: &str, title: &str, priority: bool, copy: bool) -> JazzSchema {
    let mut builder = TableSchemaBuilder::new(table).column(title, ColumnType::Text);
    if priority {
        builder = builder.column_with_default(
            "priority",
            ColumnType::BigInt,
            jazz::tools::Value::BigInt(0),
        );
    }
    if copy {
        builder = builder.column("search_name", ColumnType::Text);
    }
    compile(
        &SchemaBuilder::new()
            .table(builder.policies(allow_all()))
            .build(),
    )
}

/// The chain: (schema, table name, title column, has priority and copy).
fn chain() -> Vec<(JazzSchema, &'static str, &'static str, bool, bool)> {
    vec![
        (
            todos("todos", "title", false, false),
            "todos",
            "title",
            false,
            false,
        ),
        (
            todos("todos", "name", false, false),
            "todos",
            "name",
            false,
            false,
        ),
        (
            todos("todos", "name", true, false),
            "todos",
            "name",
            true,
            false,
        ),
        (
            todos("tasks", "name", true, true),
            "tasks",
            "name",
            true,
            true,
        ),
    ]
}

fn lens(
    from: &JazzSchema,
    to: &JazzSchema,
    source: &str,
    target: &str,
    ops: Vec<LensOp>,
) -> MigrationLens {
    MigrationLens::new(
        from.version_id(),
        to.version_id(),
        vec![TableLens {
            source_table: source.into(),
            target_table: target.into(),
            ops,
        }],
    )
    .expect("lens")
}

fn lenses(schemas: &[JazzSchema]) -> Vec<MigrationLens> {
    vec![
        lens(
            &schemas[0],
            &schemas[1],
            "todos",
            "todos",
            vec![LensOp::RenameColumn {
                from: "title".into(),
                to: "name".into(),
            }],
        ),
        lens(
            &schemas[1],
            &schemas[2],
            "todos",
            "todos",
            vec![LensOp::AddColumn {
                column: "priority".into(),
                default: Value::I64(0),
            }],
        ),
        lens(
            &schemas[2],
            &schemas[3],
            "todos",
            "tasks",
            vec![LensOp::CopyColumn {
                from: "name".into(),
                to: "search_name".into(),
            }],
        ),
    ]
}

/// What a version reads for each row: its title column, and priority and
/// the copy where it has them.
fn expected(
    version: &(JazzSchema, &'static str, &'static str, bool, bool),
    titles: &BTreeMap<RowUuid, String>,
    searches: &BTreeMap<RowUuid, String>,
) -> Rows {
    titles
        .iter()
        .map(|(id, title)| {
            let mut values = cells([(version.2, text(title.clone()))]);
            if version.3 {
                values.insert("priority".to_owned(), int(0));
            }
            if version.4 {
                values.insert("search_name".to_owned(), text(searches[id].clone()));
            }
            (*id, values)
        })
        .collect()
}

fn run(seed: u64, rows: usize, emit: bool) {
    let versions = chain();
    let schemas: Vec<JazzSchema> = versions.iter().map(|v| v.0.clone()).collect();
    let mut sim = Sim::new(seed, SimulatorTransportCodec::WireFrames);
    let core = block_on(Db::open_history_complete(config(
        schemas[0].clone(),
        0,
        AuthorSubject::SYSTEM,
    )))
    .expect("open authority");
    sim.add("core", core);
    let link = PeerProfile::new("s7", 2, 1, 0);
    let mut titles: BTreeMap<RowUuid, String> = BTreeMap::new();
    // What v4's copy holds: the title a row was seeded or inserted with.
    let mut searches: BTreeMap<RowUuid, String> = BTreeMap::new();

    // An offline v1 client queues a write before the authority migrates.
    sim.add("offline-v1", open(schemas[0].clone(), 5, user(5)));
    sim.connect("offline-v1", "core", user(5), link.clone());
    sim.pause("offline-v1", PauseMode::Disconnect);
    let queued = row(0x70, 90);
    insert(
        sim.db("offline-v1"),
        "todos",
        queued,
        cells([("title", text("offline-v1"))]),
    );
    titles.insert(queued, "offline-v1".to_owned());
    searches.insert(queued, "offline-v1".to_owned());

    // Seed rows on v1, then move the authority along the chain.
    for n in 0..rows {
        insert(
            sim.db("core"),
            "todos",
            row(0x71, n as u64),
            cells([("title", text(format!("seed-{n}")))]),
        );
        titles.insert(row(0x71, n as u64), format!("seed-{n}"));
        searches.insert(row(0x71, n as u64), format!("seed-{n}"));
    }
    let migrate = Instant::now();
    for (step, lens) in lenses(&schemas).into_iter().enumerate() {
        block_on(
            sim.db("core")
                .activate_schema_with_lenses(schemas[step + 1].clone(), &[lens]),
        )
        .unwrap_or_else(|error| panic!("activate v{}: {error}", step + 2));
    }
    let migrate_us = migrate.elapsed().as_micros() as u64;

    // Live clients on the oldest and the newest version, each shipping the
    // app's versions and lenses as an app does (SPEC 10.7).
    for (name, n, version) in [("writer-v1", 1, 0), ("writer-v4", 4, 3)] {
        let config = jazz_sim::support::config(schemas[version].clone(), n, user(n))
            .with_migrations(schemas.clone(), lenses(&schemas));
        sim.add(name, block_on(Db::open(config)).expect("open scenario db"));
        sim.connect(name, "core", user(n), link.clone());
    }
    let mut v1_live = Live::subscribe(
        sim.db("writer-v1"),
        &Query::from("todos"),
        ReadOpts::default(),
    );
    let mut v4_live = Live::subscribe(
        sim.db("writer-v4"),
        &Query::from("tasks"),
        ReadOpts::default(),
    );
    sim.settle();

    let v1_row = row(0x70, 1);
    let handle = insert(
        sim.db("writer-v1"),
        "todos",
        v1_row,
        cells([("title", text("live-v1"))]),
    );
    settle(&sim, sim.db("writer-v1"), &handle);
    titles.insert(v1_row, "live-v1".to_owned());
    searches.insert(v1_row, "live-v1".to_owned());
    let v4_row = row(0x70, 4);
    let handle = insert(
        sim.db("writer-v4"),
        "tasks",
        v4_row,
        cells([
            ("name", text("live-v4")),
            ("priority", int(0)),
            ("search_name", text("live-v4")),
        ]),
    );
    settle(&sim, sim.db("writer-v4"), &handle);
    titles.insert(v4_row, "live-v4".to_owned());
    searches.insert(v4_row, "live-v4".to_owned());
    // Each writer edits the other's row under its own names.
    sim.settle();
    v1_live.poll();
    v4_live.poll();
    let handle = block_on(sim.db("writer-v1").update(
        "todos",
        v4_row,
        cells([("title", text("v4-edited-by-v1"))]),
        Default::default(),
    ))
    .expect("v1 edits a v4 row");
    settle(&sim, sim.db("writer-v1"), &handle);
    titles.insert(v4_row, "v4-edited-by-v1".to_owned());
    let handle = block_on(sim.db("writer-v4").update(
        "tasks",
        v1_row,
        cells([("name", text("v1-edited-by-v4"))]),
        Default::default(),
    ))
    .expect("v4 edits a v1 row");
    settle(&sim, sim.db("writer-v4"), &handle);
    titles.insert(v1_row, "v1-edited-by-v4".to_owned());

    // The offline client comes back: its v1 write lands in v4 storage.
    sim.resume("offline-v1");
    sim.drive(
        sim.db("offline-v1")
            .wait_for_pending_writes(jazz::tx::DurabilityTier::Global),
    )
    .expect("the queued v1 write settles after the migration");
    let mut offline_live = Live::subscribe(
        sim.db("offline-v1"),
        &Query::from("todos"),
        ReadOpts::default(),
    );
    sim.settle();

    // Every version reads the same rows under its own names at the authority.
    let mut lens_tax = Map::new();
    for (index, version) in versions.iter().enumerate() {
        let view =
            block_on(sim.db("core").register_schema_view(version.0.clone())).expect("schema view");
        let mut read_latency = Latencies::default();
        let mut read = Rows::new();
        for _ in 0..if emit { 20 } else { 1 } {
            let start = Instant::now();
            read = oracle::read(&view, &Query::from(version.1), ReadOpts::default());
            read_latency.record_duration_us(start.elapsed());
        }
        assert_eq!(
            read,
            expected(version, &titles, &searches),
            "the authority reads v{} correctly",
            index + 1
        );
        lens_tax.insert(
            format!("v{}", index + 1),
            JsonValue::Object(read_latency.json_fields("read_us")),
        );
    }
    // And each client, from its own replica.
    v1_live.poll();
    v4_live.poll();
    offline_live.poll();
    assert_eq!(
        v1_live.rows,
        expected(&versions[0], &titles, &searches),
        "the v1 writer converged"
    );
    assert_eq!(
        offline_live.rows,
        expected(&versions[0], &titles, &searches),
        "the late v1 client converged"
    );
    assert_eq!(
        v4_live.rows,
        expected(&versions[3], &titles, &searches),
        "the v4 writer converged"
    );

    if emit {
        let mut fields = sim.json_fields();
        fields.insert("phase".to_owned(), json!("late_offline_client"));
        fields.insert("rows".to_owned(), json!(titles.len()));
        fields.insert("migrate_us".to_owned(), json!(migrate_us));
        fields.insert("lens_tax".to_owned(), JsonValue::Object(lens_tax));
        emit_summary(
            "s7_migrations",
            "deterministic",
            seed,
            "s7",
            JsonValue::Object(fields),
        );
    }
}
