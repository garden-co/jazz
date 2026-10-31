//! S4 serializable order processing (TPC-C-derived).
//!
//! Terminals run new-order, payment and delivery transactions as
//! serializable transactions against their warehouse; stock-level runs as
//! a read. Contention rounds open several transactions on the same rows
//! before any commits, so the authority must reject the stale ones.
//!
//! Gate (`INV-BENCH-7`): the accepted schedule, replayed against plain
//! SQLite, produces the same final state as the authority. Settlement and
//! propagation-inclusive runs accept the same schedule.

use std::collections::BTreeMap;
use std::time::Instant;

use jazz::db::{Db, ReadOpts};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_sim::distributions::Lcg;
use jazz_sim::oracle::{Live, cell};
use jazz_sim::support::{allow_all, cells, compile, insert, int, open, row, user, uuid};
use jazz_sim::{
    Latencies, PeerProfile, Sim, SimulatorTransportCodec, bench_profile, emit_summary, env_u64,
};
use serde_json::{Value as JsonValue, json};

const WAREHOUSES: &str = "warehouses";
const DISTRICTS: &str = "districts";
const CUSTOMERS: &str = "customers";
const ITEMS: &str = "items";
const STOCK: &str = "stock";
const ORDERS: &str = "orders";
const ORDER_LINES: &str = "orderLines";

const INITIAL_STOCK: i64 = 50;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let profile = bench_profile();
    let config = Config {
        seed: env_u64("JAZZ_SEED", 0x5400_0001),
        profile: format!("{profile:?}").to_lowercase(),
        warehouses: profile.select(1, 2, 4),
        districts_per_warehouse: 10,
        customers_per_district: profile.select(5, 10, 30),
        items: profile.select(20, 100, 500),
        terminals: profile.select(2, 4, 8),
        commits: profile.select(40, 200, 1_000),
        order_lines: 5,
        link: PeerProfile::new(
            "regional",
            env_u64("JAZZ_LINK_ONE_WAY_MS", 20),
            env_u64("JAZZ_LINK_JITTER_MS", 5),
            0,
        ),
    };
    run_all(&config, true);
}

#[allow(dead_code)]
pub fn smoke() {
    run_all(
        &Config {
            seed: 0x5400_0001,
            profile: "s4-smoke".to_owned(),
            warehouses: 1,
            districts_per_warehouse: 3,
            customers_per_district: 2,
            items: 5,
            terminals: 2,
            commits: 8,
            order_lines: 2,
            link: PeerProfile::new("s4-smoke", 2, 1, 0),
        },
        false,
    );
}

#[derive(Clone)]
struct Config {
    seed: u64,
    profile: String,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    items: usize,
    terminals: usize,
    commits: usize,
    order_lines: usize,
    link: PeerProfile,
}

fn run_all(config: &Config, emit: bool) {
    let settlement = run(config, Mode::Settlement, emit);
    let propagation = run(config, Mode::PropagationInclusive, emit);
    assert_eq!(
        settlement.schedule, propagation.schedule,
        "settlement and propagation-inclusive runs accept the same workload"
    );
    for level in [2, 3, config.terminals.max(2)] {
        run_contention(config, level, false, emit);
    }
    run_contention(config, config.terminals.max(2), true, emit);
}

fn schema() -> JazzSchema {
    let table = TableSchemaBuilder::new;
    compile(
        &SchemaBuilder::new()
            .table(
                table(WAREHOUSES)
                    .column("ytd", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .table(
                table(DISTRICTS)
                    .fk_column("warehouse", WAREHOUSES)
                    .column("nextOrder", ColumnType::BigInt)
                    .column("ytd", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .table(
                table(CUSTOMERS)
                    .fk_column("district", DISTRICTS)
                    .column("balance", ColumnType::BigInt)
                    .column("deliveries", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .table(
                table(ITEMS)
                    .column("price", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .table(
                table(STOCK)
                    .fk_column("warehouse", WAREHOUSES)
                    .fk_column("item", ITEMS)
                    .column("quantity", ColumnType::BigInt)
                    .column("orders", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .table(
                table(ORDERS)
                    .fk_column("district", DISTRICTS)
                    .fk_column("customer", CUSTOMERS)
                    .column("number", ColumnType::BigInt)
                    .column("carrier", ColumnType::BigInt)
                    .column("amount", ColumnType::BigInt)
                    .column("lines", ColumnType::BigInt)
                    .composite_index(["district", "carrier", "number"])
                    .policies(allow_all()),
            )
            .table(
                table(ORDER_LINES)
                    .fk_column("order", ORDERS)
                    .fk_column("item", ITEMS)
                    .column("quantity", ColumnType::BigInt)
                    .column("amount", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .build(),
    )
}

fn warehouse(w: usize) -> RowUuid {
    row(0x40, w as u64)
}
fn district(w: usize, d: usize) -> RowUuid {
    row(0x41, (w * 100 + d) as u64)
}
fn customer(w: usize, d: usize, c: usize) -> RowUuid {
    row(0x42, (w * 100_000 + d * 1_000 + c) as u64)
}
fn item(i: usize) -> RowUuid {
    row(0x43, i as u64)
}
fn stock(w: usize, i: usize) -> RowUuid {
    row(0x44, (w * 100_000 + i) as u64)
}
fn price(i: usize) -> i64 {
    100 + (i as i64 * 37) % 900
}

/// One accepted logical operation, replayable on SQLite.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Op {
    NewOrder {
        w: usize,
        d: usize,
        c: usize,
        lines: Vec<(usize, i64)>,
        order: RowUuid,
    },
    Payment {
        w: usize,
        d: usize,
        c: usize,
        amount: i64,
    },
    Delivery {
        w: usize,
        d: usize,
        carrier: i64,
    },
}

/// Stock after taking `quantity`: TPC-C refills by 91 below 10.
fn restock(on_hand: i64, quantity: i64) -> i64 {
    if on_hand - quantity >= 10 {
        on_hand - quantity
    } else {
        on_hand - quantity + 91
    }
}

fn seed_rows(core: &Db, config: &Config) {
    for w in 0..config.warehouses {
        insert(core, WAREHOUSES, warehouse(w), cells([("ytd", int(0))]));
        for d in 0..config.districts_per_warehouse {
            insert(
                core,
                DISTRICTS,
                district(w, d),
                cells([
                    ("warehouse", uuid(warehouse(w))),
                    ("nextOrder", int(1)),
                    ("ytd", int(0)),
                ]),
            );
            for c in 0..config.customers_per_district {
                insert(
                    core,
                    CUSTOMERS,
                    customer(w, d, c),
                    cells([
                        ("district", uuid(district(w, d))),
                        ("balance", int(0)),
                        ("deliveries", int(0)),
                    ]),
                );
            }
        }
        for i in 0..config.items {
            insert(
                core,
                STOCK,
                stock(w, i),
                cells([
                    ("warehouse", uuid(warehouse(w))),
                    ("item", uuid(item(i))),
                    ("quantity", int(INITIAL_STOCK)),
                    ("orders", int(0)),
                ]),
            );
        }
    }
    for i in 0..config.items {
        insert(core, ITEMS, item(i), cells([("price", int(price(i)))]));
    }
}

fn get_int(row: &jazz::db::CurrentRow, column: &str) -> i64 {
    match cell(row, column) {
        Some(Value::I64(v)) => v,
        other => panic!("{column}: {other:?}"),
    }
}

/// A serializable transaction run on a terminal: its reads, then its writes.
/// `Err` is a conflict; anything else is a bug.
struct Txn<'a> {
    sim: &'a Sim,
    db: &'a Db,
    tx: jazz::db::SerializableTx<'a>,
}

impl<'a> Txn<'a> {
    fn begin(sim: &'a Sim, db: &'a Db) -> Self {
        Self {
            sim,
            db,
            tx: sim.drive(db.serializable_tx()).expect("begin serializable"),
        }
    }

    fn read(&self, query: &Query) -> Vec<jazz::db::CurrentRow> {
        let prepared = self.db.prepare_query(query).expect("prepare");
        self.sim
            .drive(self.tx.all_prepared(&prepared))
            .expect("serializable read")
    }

    fn one(&self, table: &str, id: RowUuid) -> jazz::db::CurrentRow {
        let mut rows = self.read(&Query::from(table).filter(eq(col("id"), lit(uuid(id)))));
        assert_eq!(rows.len(), 1, "{table} {id:?}");
        rows.pop().expect("one row")
    }

    fn update(&self, table: &str, id: RowUuid, values: BTreeMap<String, Value>) {
        self.sim
            .drive(self.tx.update(table, id, values, Default::default()))
            .unwrap_or_else(|error| panic!("update {table}: {error}"));
    }

    fn insert(&self, table: &str, id: RowUuid, values: BTreeMap<String, Value>) {
        self.sim
            .drive(self.tx.insert(
                table,
                values,
                jazz::db::InsertOptions {
                    row_id: Some(id),
                    ..Default::default()
                },
            ))
            .unwrap_or_else(|error| panic!("insert {table}: {error}"));
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

/// Stage `op`'s reads and writes in `txn`. A delivery with nothing to
/// deliver resolves to `None` and writes nothing.
fn stage(txn: &Txn<'_>, op: &Op) -> Option<Op> {
    match op {
        Op::NewOrder {
            w,
            d,
            c,
            lines,
            order,
        } => {
            let district_row = txn.one(DISTRICTS, district(*w, *d));
            let number = get_int(&district_row, "nextOrder");
            txn.update(
                DISTRICTS,
                district(*w, *d),
                cells([("nextOrder", int(number + 1))]),
            );
            let mut amount = 0;
            for (n, (i, quantity)) in lines.iter().enumerate() {
                let item_row = txn.one(ITEMS, item(*i));
                let line_amount = get_int(&item_row, "price") * quantity;
                amount += line_amount;
                let stock_row = txn.one(STOCK, stock(*w, *i));
                txn.update(
                    STOCK,
                    stock(*w, *i),
                    cells([
                        (
                            "quantity",
                            int(restock(get_int(&stock_row, "quantity"), *quantity)),
                        ),
                        ("orders", int(get_int(&stock_row, "orders") + 1)),
                    ]),
                );
                txn.insert(
                    ORDER_LINES,
                    row(
                        0x46,
                        (order.0.as_u128() as u64 & 0xffff_ffff) << 8 | n as u64,
                    ),
                    cells([
                        ("order", uuid(*order)),
                        ("item", uuid(item(*i))),
                        ("quantity", int(*quantity)),
                        ("amount", int(line_amount)),
                    ]),
                );
            }
            txn.insert(
                ORDERS,
                *order,
                cells([
                    ("district", uuid(district(*w, *d))),
                    ("customer", uuid(customer(*w, *d, *c))),
                    ("number", int(number)),
                    ("carrier", int(0)),
                    ("amount", int(amount)),
                    ("lines", int(lines.len() as i64)),
                ]),
            );
            Some(op.clone())
        }
        Op::Payment { w, d, c, amount } => {
            let wh = txn.one(WAREHOUSES, warehouse(*w));
            txn.update(
                WAREHOUSES,
                warehouse(*w),
                cells([("ytd", int(get_int(&wh, "ytd") + amount))]),
            );
            let di = txn.one(DISTRICTS, district(*w, *d));
            txn.update(
                DISTRICTS,
                district(*w, *d),
                cells([("ytd", int(get_int(&di, "ytd") + amount))]),
            );
            let cu = txn.one(CUSTOMERS, customer(*w, *d, *c));
            txn.update(
                CUSTOMERS,
                customer(*w, *d, *c),
                cells([("balance", int(get_int(&cu, "balance") - amount))]),
            );
            Some(op.clone())
        }
        Op::Delivery { w, d, carrier } => {
            let oldest = txn.read(
                &Query::from(ORDERS)
                    .filter(eq(col("district"), lit(uuid(district(*w, *d)))))
                    .filter(eq(col("carrier"), lit(int(0))))
                    .order_by("number", OrderDirection::Asc)
                    .limit(1),
            );
            let order = oldest.first()?;
            txn.update(
                ORDERS,
                order.row_uuid(),
                cells([("carrier", int(*carrier))]),
            );
            let Some(Value::Uuid(customer_id)) = cell(order, "customer") else {
                panic!("order customer");
            };
            let customer_id = RowUuid(customer_id);
            let cu = txn.one(CUSTOMERS, customer_id);
            txn.update(
                CUSTOMERS,
                customer_id,
                cells([
                    (
                        "balance",
                        int(get_int(&cu, "balance") + get_int(order, "amount")),
                    ),
                    ("deliveries", int(get_int(&cu, "deliveries") + 1)),
                ]),
            );
            Some(op.clone())
        }
    }
}

/// Stock-level: a read-only check of low stock in a district's warehouse.
fn stock_level(sim: &Sim, db: &Db, w: usize, threshold: i64) -> usize {
    let query = Query::from(STOCK).filter(eq(col("warehouse"), lit(uuid(warehouse(w)))));
    let prepared = db.prepare_query(&query).expect("prepare");
    sim.drive(db.all(
        &prepared,
        ReadOpts::for_read_tier(jazz::db::ReadTier::Remote),
    ))
    .expect("stock level read")
    .iter()
    .filter(|row| get_int(row, "quantity") < threshold)
    .count()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// A commit is done when the authority accepted it.
    Settlement,
    /// A commit is done when a dashboard terminal shows it.
    PropagationInclusive,
}

struct Run {
    schedule: Vec<Op>,
}

struct Bench {
    sim: Sim,
    terminals: Vec<String>,
    next_order: u64,
}

fn bench(config: &Config, seed: u64, terminals: usize) -> Bench {
    let mut sim = Sim::new(seed, SimulatorTransportCodec::WireFrames);
    let core = sim.add("core", open(schema(), 0, AuthorSubject::SYSTEM));
    seed_rows(core, config);
    let names: Vec<String> = (0..terminals).map(|t| format!("terminal-{t}")).collect();
    for (t, name) in names.iter().enumerate() {
        sim.add(name.clone(), open(schema(), 1 + t as u32, user(t as u32)));
        sim.connect(name, "core", user(t as u32), config.link.clone());
    }
    Bench {
        sim,
        terminals: names,
        next_order: 0,
    }
}

impl Bench {
    fn order_id(&mut self) -> RowUuid {
        self.next_order += 1;
        row(0x45, self.next_order)
    }

    fn random_op(&mut self, config: &Config, rng: &mut Lcg) -> Op {
        let w = rng.usize(config.warehouses);
        let d = rng.usize(config.districts_per_warehouse);
        let c = rng.usize(config.customers_per_district);
        match rng.usize(100) {
            0..60 => {
                let lines = (0..config.order_lines)
                    .map(|_| (rng.usize(config.items), 1 + rng.usize(5) as i64))
                    .collect();
                Op::NewOrder {
                    w,
                    d,
                    c,
                    lines,
                    order: self.order_id(),
                }
            }
            60..90 => Op::Payment {
                w,
                d,
                c,
                amount: 1 + rng.usize(5_000) as i64,
            },
            _ => Op::Delivery {
                w,
                d,
                carrier: 1 + rng.usize(10) as i64,
            },
        }
    }
}

fn run(config: &Config, mode: Mode, emit: bool) -> Run {
    let mut bench = bench(config, config.seed, config.terminals);
    let mut rng = Lcg::new(config.seed);
    let mut schedule = Vec::new();
    let mut latency = Latencies::default();
    let mut dashboard = (mode == Mode::PropagationInclusive).then(|| {
        Live::subscribe(
            bench.sim.db(&bench.terminals[0]),
            &Query::from(ORDERS),
            ReadOpts::default(),
        )
    });
    bench.sim.settle();
    let wall = Instant::now();
    let mut low_stock_checks = 0;
    for n in 0..config.commits {
        let op = bench.random_op(config, &mut rng);
        let terminal = bench.terminals[n % bench.terminals.len()].clone();
        let at = bench.sim.now_ms();
        let db = bench.sim.db(&terminal);
        let txn = Txn::begin(&bench.sim, db);
        let staged = stage(&txn, &op);
        assert!(txn.commit(), "an uncontended transaction commits: {op:?}");
        if let Some(op) = staged {
            schedule.push(op);
        }
        if let (Some(dashboard), Some(Op::NewOrder { order, .. })) =
            (&mut dashboard, schedule.last())
        {
            // The last step's tick may be the one that delivers it, so the
            // dashboard is polled once more after the network goes idle.
            let mut idle = false;
            loop {
                dashboard.poll();
                if dashboard.rows.contains_key(order) {
                    break;
                }
                assert!(!idle, "the order must reach the dashboard");
                idle = !bench.sim.step();
            }
        }
        latency.record(bench.sim.now_ms() - at);
        if n % 10 == 9 {
            low_stock_checks += stock_level(&bench.sim, bench.sim.db(&terminal), 0, 20);
        }
    }
    bench.sim.settle();
    let wall_us = wall.elapsed().as_micros() as u64;
    assert_replay_matches(config, &bench.sim, &schedule);
    if emit {
        let mut fields = bench.sim.json_fields();
        fields.insert(
            "phase".to_owned(),
            json!(match mode {
                Mode::Settlement => "throughput_settlement",
                Mode::PropagationInclusive => "throughput_propagation_inclusive",
            }),
        );
        fields.insert("accepted".to_owned(), json!(schedule.len()));
        fields.insert("wall_us".to_owned(), json!(wall_us));
        fields.insert(
            "accepted_per_sec".to_owned(),
            json!(schedule.len() as f64 / (wall_us.max(1) as f64 / 1e6)),
        );
        fields.insert("low_stock_rows_seen".to_owned(), json!(low_stock_checks));
        fields.extend(latency.json_fields("commit_virtual_ms"));
        emit_summary(
            "s4_order_processing",
            "deterministic",
            config.seed,
            &config.profile,
            JsonValue::Object(fields),
        );
    }
    Run { schedule }
}

/// `level` terminals each open a transaction on the same district (or,
/// with `hot_items`, order the same item from different districts) before
/// any of them commits: exactly the first commit of each round is accepted.
fn run_contention(config: &Config, level: usize, hot_items: bool, emit: bool) -> Run {
    let mut bench = bench(config, config.seed ^ level as u64, level);
    bench.sim.settle();
    let mut schedule = Vec::new();
    let mut rejected = 0;
    let rounds = (config.commits / level).max(2);
    for round in 0..rounds {
        let ops: Vec<Op> = (0..level)
            .map(|t| {
                let d = if hot_items {
                    t % config.districts_per_warehouse
                } else {
                    round % config.districts_per_warehouse
                };
                Op::NewOrder {
                    w: 0,
                    d,
                    c: t % config.customers_per_district,
                    lines: vec![(if hot_items { 0 } else { t % config.items }, 1)],
                    order: bench.order_id(),
                }
            })
            .collect();
        let txns: Vec<_> = bench
            .terminals
            .iter()
            .zip(&ops)
            .map(|(terminal, op)| {
                let txn = Txn::begin(&bench.sim, bench.sim.db(terminal));
                let staged = stage(&txn, op);
                (txn, staged)
            })
            .collect();
        let mut accepted = 0;
        for (txn, staged) in txns {
            if txn.commit() {
                accepted += 1;
                schedule.extend(staged);
            } else {
                rejected += 1;
            }
        }
        // Every transaction of the round read the same contended row, so
        // only the first commit can be serializable.
        assert_eq!(
            accepted, 1,
            "round {round}: exactly one of {level} contenders commits"
        );
    }
    bench.sim.settle();
    assert_eq!(rejected, rounds * (level - 1));
    assert_replay_matches(config, &bench.sim, &schedule);
    if emit {
        let mut fields = bench.sim.json_fields();
        fields.insert(
            "phase".to_owned(),
            json!(if hot_items {
                "contention_hot_items"
            } else {
                "contention"
            }),
        );
        fields.insert("contenders".to_owned(), json!(level));
        fields.insert("accepted".to_owned(), json!(schedule.len()));
        fields.insert("rejected".to_owned(), json!(rejected));
        emit_summary(
            "s4_order_processing",
            "deterministic",
            config.seed,
            &config.profile,
            JsonValue::Object(fields),
        );
    }
    Run { schedule }
}

/// The logical end state: every number that a transaction can change.
type State = BTreeMap<String, Vec<i64>>;

fn jazz_state(sim: &Sim) -> State {
    let core = sim.db("core");
    let mut state = State::new();
    let read = |table: &str, columns: &[&str]| -> Vec<Vec<i64>> {
        let prepared = core.prepare_query(&Query::from(table)).expect("prepare");
        let mut rows: Vec<Vec<i64>> = jazz::db::block_on(core.all(&prepared, ReadOpts::default()))
            .expect("read")
            .iter()
            .map(|row| columns.iter().map(|c| get_int(row, c)).collect())
            .collect();
        rows.sort();
        rows
    };
    for (key, rows) in [
        ("warehouses", read(WAREHOUSES, &["ytd"])),
        ("districts", read(DISTRICTS, &["nextOrder", "ytd"])),
        ("customers", read(CUSTOMERS, &["balance", "deliveries"])),
        ("stock", read(STOCK, &["quantity", "orders"])),
        (
            "orders",
            read(ORDERS, &["number", "carrier", "amount", "lines"]),
        ),
        ("lines", read(ORDER_LINES, &["quantity", "amount"])),
    ] {
        state.insert(key.to_owned(), rows.into_iter().flatten().collect());
    }
    state
}

/// Replay `schedule` on plain SQLite with the same transaction logic.
fn sqlite_state(config: &Config, schedule: &[Op]) -> State {
    let db = rusqlite::Connection::open_in_memory().expect("sqlite");
    db.execute_batch(
        "CREATE TABLE warehouses (w INTEGER PRIMARY KEY, ytd INTEGER);
         CREATE TABLE districts (w INTEGER, d INTEGER, next INTEGER, ytd INTEGER, PRIMARY KEY (w, d));
         CREATE TABLE customers (w INTEGER, d INTEGER, c INTEGER, balance INTEGER, deliveries INTEGER, PRIMARY KEY (w, d, c));
         CREATE TABLE stock (w INTEGER, i INTEGER, quantity INTEGER, orders INTEGER, PRIMARY KEY (w, i));
         CREATE TABLE orders (w INTEGER, d INTEGER, c INTEGER, number INTEGER, carrier INTEGER, amount INTEGER, lines INTEGER);
         CREATE TABLE lines (quantity INTEGER, amount INTEGER);",
    )
    .expect("schema");
    for w in 0..config.warehouses {
        db.execute("INSERT INTO warehouses VALUES (?1, 0)", [w as i64])
            .unwrap();
        for d in 0..config.districts_per_warehouse {
            db.execute(
                "INSERT INTO districts VALUES (?1, ?2, 1, 0)",
                [w as i64, d as i64],
            )
            .unwrap();
            for c in 0..config.customers_per_district {
                db.execute(
                    "INSERT INTO customers VALUES (?1, ?2, ?3, 0, 0)",
                    [w as i64, d as i64, c as i64],
                )
                .unwrap();
            }
        }
        for i in 0..config.items {
            db.execute(
                "INSERT INTO stock VALUES (?1, ?2, ?3, 0)",
                [w as i64, i as i64, INITIAL_STOCK],
            )
            .unwrap();
        }
    }
    let one = |sql: &str, params: &[i64]| -> i64 {
        db.query_row(sql, rusqlite::params_from_iter(params), |row| row.get(0))
            .expect(sql)
    };
    for op in schedule {
        let tx = db.unchecked_transaction().expect("transaction");
        match op {
            Op::NewOrder { w, d, c, lines, .. } => {
                let (w, d, c) = (*w as i64, *d as i64, *c as i64);
                let number = one(
                    "SELECT next FROM districts WHERE w = ?1 AND d = ?2",
                    &[w, d],
                );
                tx.execute(
                    "UPDATE districts SET next = next + 1 WHERE w = ?1 AND d = ?2",
                    [w, d],
                )
                .unwrap();
                let mut amount = 0;
                for (i, quantity) in lines {
                    let line = price(*i) * quantity;
                    amount += line;
                    let on_hand = one(
                        "SELECT quantity FROM stock WHERE w = ?1 AND i = ?2",
                        &[w, *i as i64],
                    );
                    tx.execute(
                        "UPDATE stock SET quantity = ?3, orders = orders + 1 WHERE w = ?1 AND i = ?2",
                        [w, *i as i64, restock(on_hand, *quantity)],
                    )
                    .unwrap();
                    tx.execute("INSERT INTO lines VALUES (?1, ?2)", [*quantity, line])
                        .unwrap();
                }
                tx.execute(
                    "INSERT INTO orders VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)",
                    [w, d, c, number, amount, lines.len() as i64],
                )
                .unwrap();
            }
            Op::Payment { w, d, c, amount } => {
                let (w, d, c) = (*w as i64, *d as i64, *c as i64);
                tx.execute(
                    "UPDATE warehouses SET ytd = ytd + ?2 WHERE w = ?1",
                    [w, *amount],
                )
                .unwrap();
                tx.execute(
                    "UPDATE districts SET ytd = ytd + ?3 WHERE w = ?1 AND d = ?2",
                    [w, d, *amount],
                )
                .unwrap();
                tx.execute(
                    "UPDATE customers SET balance = balance - ?4 WHERE w = ?1 AND d = ?2 AND c = ?3",
                    [w, d, c, *amount],
                )
                .unwrap();
            }
            Op::Delivery { w, d, carrier } => {
                let (w, d) = (*w as i64, *d as i64);
                let (number, c, amount): (i64, i64, i64) = tx
                    .query_row(
                        "SELECT number, c, amount FROM orders WHERE w = ?1 AND d = ?2 AND carrier = 0 ORDER BY number LIMIT 1",
                        [w, d],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .expect("a delivery in the schedule had an order to deliver");
                tx.execute(
                    "UPDATE orders SET carrier = ?4 WHERE w = ?1 AND d = ?2 AND number = ?3",
                    [w, d, number, *carrier],
                )
                .unwrap();
                tx.execute(
                    "UPDATE customers SET balance = balance + ?4, deliveries = deliveries + 1 WHERE w = ?1 AND d = ?2 AND c = ?3",
                    [w, d, c, amount],
                )
                .unwrap();
            }
        }
        tx.commit().expect("commit");
    }
    let rows = |sql: &str, width: usize| -> Vec<i64> {
        let mut statement = db.prepare(sql).expect(sql);
        let mut rows: Vec<Vec<i64>> = statement
            .query_map([], |row| (0..width).map(|i| row.get(i)).collect())
            .expect(sql)
            .map(|row| row.expect("row"))
            .collect();
        rows.sort();
        rows.into_iter().flatten().collect()
    };
    State::from([
        (
            "warehouses".to_owned(),
            rows("SELECT ytd FROM warehouses", 1),
        ),
        (
            "districts".to_owned(),
            rows("SELECT next, ytd FROM districts", 2),
        ),
        (
            "customers".to_owned(),
            rows("SELECT balance, deliveries FROM customers", 2),
        ),
        (
            "stock".to_owned(),
            rows("SELECT quantity, orders FROM stock", 2),
        ),
        (
            "orders".to_owned(),
            rows("SELECT number, carrier, amount, lines FROM orders", 4),
        ),
        (
            "lines".to_owned(),
            rows("SELECT quantity, amount FROM lines", 2),
        ),
    ])
}

fn assert_replay_matches(config: &Config, sim: &Sim, schedule: &[Op]) {
    assert!(!schedule.is_empty(), "the run accepted transactions");
    assert_eq!(
        jazz_state(sim),
        sqlite_state(config, schedule),
        "the accepted schedule replayed on SQLite reaches the authority's state"
    );
}
