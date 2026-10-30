//! S0 micro primitives on jazz-db: HLC minting, linear-history ordering of
//! concurrent writes, deletion and restore, authority ingest, commit units
//! and serializable read-set validation. Each probe asserts its outcome on
//! every iteration; `main` also reports per-iteration latencies.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

use jazz::db::{Db, ReadOpts, block_on};
use jazz::ids::AuthorSubject;
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::time::TxTime;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_sim::oracle::{self, Live};
use jazz_sim::support::{
    allow_all, cells, compile, insert, int, open, row, settle, text, user, uuid,
};
use jazz_sim::{
    Latencies, PeerProfile, Sim, SimulatorTransportCodec, emit_summary, env_u64, env_usize,
};
use serde_json::{Map, Value as JsonValue, json};

const ITEMS: &str = "items";

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let config = Config {
        seed: env_u64("JAZZ_SEED", 0x006d_6963_726f),
        iterations: env_usize("JAZZ_MICRO_ITERS", 100).max(1),
    };
    run(&config, true);
}

#[allow(dead_code)]
pub(crate) fn correctness_smoke() {
    run(
        &Config {
            seed: 0x006d_6963_726f,
            iterations: 2,
        },
        false,
    );
}

type Probe = fn(&Config) -> Latencies;

struct Config {
    seed: u64,
    iterations: usize,
}

fn run(config: &Config, emit: bool) {
    let probes: [(&str, Probe); 7] = [
        ("hlc_mint", hlc_mint),
        ("linear_order", linear_order),
        ("deletion_restore", deletion_restore),
        ("ingest", ingest),
        ("commit_unit", commit_unit),
        ("read_set_validation", read_set_validation),
        ("history_versions", history_versions),
    ];
    for (primitive, probe) in probes {
        let latencies = probe(config);
        if emit {
            let mut fields = Map::new();
            fields.insert("primitive".to_owned(), json!(primitive));
            fields.extend(latencies.json_fields("latency_us"));
            emit_summary(
                "micro",
                "deterministic",
                config.seed,
                "micro",
                JsonValue::Object(fields),
            );
        }
    }
}

fn schema() -> JazzSchema {
    compile(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(ITEMS)
                    .column("label", ColumnType::Text)
                    .column("n", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .build(),
    )
}

/// An authority and `clients` clients on instant links.
fn net(config: &Config, clients: u32) -> Sim {
    let mut sim = Sim::new(config.seed, SimulatorTransportCodec::WireFrames);
    sim.add("core", open(schema(), 0, AuthorSubject::SYSTEM));
    for c in 1..=clients {
        let name = format!("c{c}");
        sim.add(name.clone(), open(schema(), c, user(c)));
        sim.connect(&name, "core", user(c), PeerProfile::new("micro", 1, 0, 0));
    }
    sim
}

fn item(label: &str, n: i64) -> BTreeMap<String, Value> {
    cells([("label", text(label)), ("n", int(n))])
}

/// Subscribe each named client to every item: a client updates only rows
/// it holds.
fn watch_all(sim: &Sim, clients: &[&str]) -> Vec<Live> {
    let live = clients
        .iter()
        .map(|client| Live::subscribe(sim.db(client), &Query::from(ITEMS), ReadOpts::default()))
        .collect();
    sim.settle();
    live
}

fn all_items(db: &Db) -> oracle::Rows {
    oracle::read(db, &Query::from(ITEMS), ReadOpts::default())
}

/// TxTime minting is strictly monotone, also when the clock stalls or
/// steps back.
fn hlc_mint(config: &Config) -> Latencies {
    let mut latencies = Latencies::default();
    let mut register = TxTime::default();
    for idx in 0..config.iterations * 1_000 {
        // The physical clock stalls for 128 mints, then steps back once.
        let now = 1_000 + (idx as u64 / 128) - u64::from(idx % 512 == 511);
        let start = Instant::now();
        let next = TxTime::tick(register, now).expect("clock stays below the HLC horizon");
        latencies.record(start.elapsed().as_nanos() as u64);
        assert!(next > register, "HLC mint must be strictly monotone");
        register = black_box(next);
    }
    latencies
}

/// Two clients write one row concurrently. Linear history: the authority
/// orders the commits by arrival, the later one wins, and every replica
/// converges on it.
fn linear_order(config: &Config) -> Latencies {
    let mut latencies = Latencies::default();
    for iteration in 0..config.iterations {
        let sim = net(config, 2);
        let _watching = watch_all(&sim, &["c1", "c2"]);
        let id = row(1, iteration as u64);
        let created = insert(sim.db("c1"), ITEMS, id, item("created", 0));
        settle(&sim, sim.db("c1"), &created);
        sim.settle();
        let start = Instant::now();
        let first =
            block_on(
                sim.db("c1")
                    .update(ITEMS, id, item("from-c1", 1), Default::default()),
            )
            .expect("c1 update");
        let second =
            block_on(
                sim.db("c2")
                    .update(ITEMS, id, item("from-c2", 2), Default::default()),
            )
            .expect("c2 update");
        // c1's tick uploads first, so its commit arrives first.
        settle(&sim, sim.db("c1"), &first);
        settle(&sim, sim.db("c2"), &second);
        sim.settle();
        latencies.record_duration_us(start.elapsed());
        let expected = BTreeMap::from([(id, item("from-c2", 2))]);
        for node in ["core", "c1", "c2"] {
            assert_eq!(
                all_items(sim.db(node)),
                expected,
                "{node} converges on the later write"
            );
        }
    }
    latencies
}

/// A deleted row leaves reads and comes back with its content on restore,
/// on every replica.
fn deletion_restore(config: &Config) -> Latencies {
    let mut latencies = Latencies::default();
    for iteration in 0..config.iterations {
        let sim = net(config, 2);
        let watcher = Live::subscribe(sim.db("c2"), &Query::from(ITEMS), ReadOpts::default());
        let id = row(2, iteration as u64);
        let created = insert(sim.db("c1"), ITEMS, id, item("doomed", 7));
        settle(&sim, sim.db("c1"), &created);
        let start = Instant::now();
        let deleted = block_on(sim.db("c1").delete(ITEMS, id, Default::default())).expect("delete");
        settle(&sim, sim.db("c1"), &deleted);
        sim.settle();
        for node in ["core", "c1", "c2"] {
            assert!(
                all_items(sim.db(node)).is_empty(),
                "{node} no longer reads the row"
            );
        }
        let with_deleted = ReadOpts {
            read_view: jazz::protocol::ReadViewSpec {
                include_deleted: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            oracle::ids(sim.db("core"), &Query::from(ITEMS), with_deleted),
            [id].into()
        );
        let restored =
            block_on(sim.db("c2").restore(ITEMS, id, None, Default::default())).expect("restore");
        settle(&sim, sim.db("c2"), &restored);
        sim.settle();
        latencies.record_duration_us(start.elapsed());
        for node in ["core", "c1", "c2"] {
            assert_eq!(
                all_items(sim.db(node)),
                BTreeMap::from([(id, item("doomed", 7))]),
                "{node} reads the restored row"
            );
        }
        let mut watcher = watcher;
        watcher.poll();
        assert_eq!(watcher.ids(), [id].into());
        assert!(watcher.removed >= 1, "the deletion reached the subscriber");
    }
    latencies
}

/// The authority ingests a burst of client commits and accepts them all.
fn ingest(config: &Config) -> Latencies {
    let mut latencies = Latencies::default();
    let sim = net(config, 1);
    let burst = 64;
    for iteration in 0..config.iterations {
        let handles: Vec<_> = (0..burst)
            .map(|n| {
                let id = row(3, (iteration * burst + n) as u64);
                insert(sim.db("c1"), ITEMS, id, item("burst", n as i64))
            })
            .collect();
        let start = Instant::now();
        for handle in &handles {
            settle(&sim, sim.db("c1"), handle);
        }
        latencies.record_duration_us(start.elapsed());
        assert_eq!(all_items(sim.db("core")).len(), (iteration + 1) * burst);
    }
    latencies
}

/// A mergeable transaction lands as one unit: another client's
/// subscription sees all of its rows in one delta, never a prefix.
fn commit_unit(config: &Config) -> Latencies {
    let mut latencies = Latencies::default();
    let sim = net(config, 2);
    let mut watcher = Live::subscribe(sim.db("c2"), &Query::from(ITEMS), ReadOpts::default());
    sim.settle();
    watcher.poll();
    for iteration in 0..config.iterations {
        let start = Instant::now();
        let tx = block_on(sim.db("c1").mergeable_tx()).expect("open transaction");
        for n in 0..3 {
            block_on(tx.insert(
                ITEMS,
                item("unit", n),
                jazz::db::InsertOptions {
                    row_id: Some(row(4, (iteration * 3) as u64 + n as u64)),
                    ..Default::default()
                },
            ))
            .expect("insert in unit");
        }
        let receipt = block_on(tx.commit()).expect("commit unit");
        sim.drive(
            sim.db("c1")
                .wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global),
        )
        .expect("unit settles");
        let before = watcher.rows.len();
        // Step until the subscriber changes, then check it changed by the
        // whole unit at once.
        while !watcher.poll() {
            assert!(sim.step(), "the unit must reach the subscriber");
        }
        latencies.record_duration_us(start.elapsed());
        assert_eq!(
            watcher.rows.len(),
            before + 3,
            "all three rows arrive together"
        );
    }
    latencies
}

/// A serializable transaction commits when its reads still hold at the
/// authority and conflicts when another client changed a row it read.
fn read_set_validation(config: &Config) -> Latencies {
    let mut latencies = Latencies::default();
    for iteration in 0..config.iterations {
        let sim = net(config, 2);
        let id = row(5, iteration as u64);
        let created = insert(sim.db("c1"), ITEMS, id, item("guard", 0));
        settle(&sim, sim.db("c1"), &created);
        sim.settle();
        let guard = Query::from(ITEMS).filter(eq(col("label"), lit(text("guard"))));
        for conflicting in [false, true] {
            let start = Instant::now();
            let db = sim.db("c2");
            let tx = sim.drive(db.serializable_tx()).expect("begin serializable");
            let prepared = db.prepare_query(&guard).expect("prepare guard read");
            let seen = sim.drive(tx.all_prepared(&prepared)).expect("guard read");
            assert_eq!(seen.len(), 1);
            sim.drive(tx.insert(ITEMS, item("derived", 1), Default::default()))
                .expect("derived insert");
            if conflicting {
                let bump =
                    block_on(
                        sim.db("c1")
                            .update(ITEMS, id, item("guard", 9), Default::default()),
                    )
                    .expect("concurrent update");
                settle(&sim, sim.db("c1"), &bump);
            }
            let outcome = match sim.drive(tx.commit()) {
                Ok(receipt) => {
                    sim.drive(db.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global))
                }
                Err(error) => Err(error),
            };
            latencies.record_duration_us(start.elapsed());
            match (conflicting, outcome) {
                (false, Ok(_)) => {}
                (true, Err(error)) => assert!(error.is_conflict(), "{error}"),
                (conflicting, outcome) => {
                    panic!("conflicting={conflicting}: unexpected outcome {outcome:?}")
                }
            }
        }
        sim.settle();
        let derived = Query::from(ITEMS).filter(eq(col("label"), lit(text("derived"))));
        assert_eq!(
            oracle::read(sim.db("core"), &derived, ReadOpts::default()).len(),
            1,
            "only the non-conflicting transaction's write landed"
        );
    }
    latencies
}

/// The authority keeps every accepted version, in arrival order.
fn history_versions(config: &Config) -> Latencies {
    let mut latencies = Latencies::default();
    let sim = net(config, 2);
    let _watching = watch_all(&sim, &["c1", "c2"]);
    for iteration in 0..config.iterations {
        let id = row(6, iteration as u64);
        let created = insert(sim.db("c1"), ITEMS, id, item("v", 0));
        settle(&sim, sim.db("c1"), &created);
        for version in 1..=4 {
            let writer = if version % 2 == 0 { "c1" } else { "c2" };
            sim.settle();
            let handle =
                block_on(
                    sim.db(writer)
                        .update(ITEMS, id, item("v", version), Default::default()),
                )
                .expect("update");
            settle(&sim, sim.db(writer), &handle);
        }
        let start = Instant::now();
        let history = Query::from(format!("history.{ITEMS}"))
            .filter(eq(col("id"), lit(uuid(id))))
            .order_by("$acceptedAt", OrderDirection::Asc);
        let prepared = sim
            .db("core")
            .prepare_query(&history)
            .expect("prepare history");
        let versions =
            block_on(sim.db("core").all(&prepared, ReadOpts::default())).expect("history read");
        latencies.record_duration_us(start.elapsed());
        let ns: Vec<_> = versions.iter().map(|v| oracle::cell(v, "n")).collect();
        assert_eq!(ns, (0..=4).map(|n| Some(int(n))).collect::<Vec<_>>());
    }
    latencies
}
