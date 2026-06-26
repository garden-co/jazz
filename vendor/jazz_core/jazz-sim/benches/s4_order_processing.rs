use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
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
use jazz::tx::{DurabilityTier, Fate};
use jazz::wire::TransportError;
use jazz_sim::{PeerProfile, bench_profile, emit_json_line, metadata_fields};
use rusqlite::{Connection, params};
use serde_json::{Value as JsonValue, json};

const WAREHOUSES: &str = "warehouses";
const DISTRICTS: &str = "districts";
const CUSTOMERS: &str = "customers";
const ITEMS: &str = "items";
const STOCK: &str = "stock";
const ORDERS: &str = "orders";
const ORDER_LINES: &str = "orderLines";
const PAYMENTS: &str = "payments";

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
    let throughput = run_jazz(
        &config,
        config.throughput_commits,
        RunMode::ThroughputPropagationInclusive,
        PeerRefreshMode::AfterAccept,
    );
    let throughput_settlement = replay_jazz(
        &config,
        &throughput.accepted_schedule,
        RunMode::ThroughputSettlement,
        PeerRefreshMode::SuppressAfterAccept,
    );
    assert_eq!(
        throughput_settlement.accepted_schedule, throughput.accepted_schedule,
        "settlement and propagation-inclusive throughput runs must use the same accepted workload"
    );
    let throughput_sqlite = run_sqlite_reference(&config, &throughput.accepted_schedule);
    assert_sqlite_replay_matches(
        &config,
        &throughput_settlement.accepted_schedule,
        &throughput_settlement.final_totals,
    );
    assert_sqlite_replay_matches(
        &config,
        &throughput.accepted_schedule,
        &throughput.final_totals,
    );
    emit_summary(
        &config,
        &profile,
        &throughput_settlement,
        Some(&throughput_sqlite),
        RunMode::ThroughputSettlement,
    );
    emit_summary(
        &config,
        &profile,
        &throughput,
        Some(&throughput_sqlite),
        RunMode::ThroughputPropagationInclusive,
    );

    let slo = run_jazz(
        &config,
        config.slo_commits,
        RunMode::Slo,
        PeerRefreshMode::AfterAccept,
    );
    assert_sqlite_replay_matches(&config, &slo.accepted_schedule, &slo.final_totals);
    emit_summary(&config, &profile, &slo, None, RunMode::Slo);

    for level in [
        ContentionLevel::Low,
        ContentionLevel::Medium,
        ContentionLevel::High,
    ] {
        let contention = run_jazz_contention(&config, level);
        let mut contention_config = config.clone();
        contention_config.clients = level.attempts_per_round();
        assert_sqlite_replay_matches(
            &contention_config,
            &contention.accepted_schedule,
            &contention.final_totals,
        );
        emit_summary(
            &contention_config,
            &profile,
            &contention,
            None,
            RunMode::Contention(level),
        );
    }

    let hot_items = run_jazz_hot_item_contention(&config);
    assert_sqlite_replay_matches(
        &config,
        &hot_items.accepted_schedule,
        &hot_items.final_totals,
    );
    emit_summary(
        &config,
        &profile,
        &hot_items,
        None,
        RunMode::HotItemContention,
    );

    let mut max_sustained_warehouses = 0_usize;
    for warehouses in [1_usize, 2, 4, 8] {
        let mut scale_config = config.clone();
        scale_config.warehouses = warehouses;
        scale_config.clients = warehouses.max(1);
        let scale = run_jazz(
            &scale_config,
            scale_config.slo_commits,
            RunMode::ScaleOut,
            PeerRefreshMode::AfterAccept,
        );
        let achieved_rate =
            scale.accepted_schedule.len() as f64 / (scale.elapsed_us as f64 / 1_000_000.0);
        let required_rate = (scale_config.per_warehouse_rate * warehouses as u64) as f64;
        let latency_with_link_us =
            scale.latency.value_at_quantile(0.95) + profile.one_way_latency_ms * 2_000;
        let within_slo = latency_with_link_us <= profile.one_way_latency_ms.saturating_mul(20_000);
        let sustained_rate_met = achieved_rate >= required_rate;
        if within_slo && sustained_rate_met {
            max_sustained_warehouses = warehouses;
        }
        emit_summary(
            &scale_config,
            &profile,
            &scale,
            None,
            RunMode::ScaleOutLadder {
                max_sustained_warehouses,
                within_slo,
                sustained_rate_met,
                required_rate,
                achieved_rate,
                latency_with_link_us,
            },
        );
    }
}

pub fn smoke() {
    let config = Config {
        seed: 0x5400_0001,
        profile: "s4-smoke".to_owned(),
        warehouses: 1,
        districts_per_warehouse: 10,
        customers_per_district: 2,
        items: 5,
        clients: 1,
        throughput_commits: 5,
        slo_commits: 2,
        order_lines: 2,
        new_order_pct: 70,
        delivery_pct: 5,
        stock_level_pct: 5,
        per_warehouse_rate: 40,
    };
    let throughput = run_jazz(
        &config,
        config.throughput_commits,
        RunMode::ThroughputPropagationInclusive,
        PeerRefreshMode::AfterAccept,
    );
    assert_sqlite_replay_matches(
        &config,
        &throughput.accepted_schedule,
        &throughput.final_totals,
    );
    let slo = run_jazz(
        &config,
        config.slo_commits,
        RunMode::Slo,
        PeerRefreshMode::AfterAccept,
    );
    assert_sqlite_replay_matches(&config, &slo.accepted_schedule, &slo.final_totals);
    for level in [
        ContentionLevel::Low,
        ContentionLevel::Medium,
        ContentionLevel::High,
    ] {
        let contention = run_jazz_contention(&config, level);
        let mut contention_config = config.clone();
        contention_config.clients = level.attempts_per_round();
        assert_sqlite_replay_matches(
            &contention_config,
            &contention.accepted_schedule,
            &contention.final_totals,
        );
    }
    let hot_items = run_jazz_hot_item_contention(&config);
    assert_sqlite_replay_matches(
        &config,
        &hot_items.accepted_schedule,
        &hot_items.final_totals,
    );
}

#[derive(Clone, Debug)]
struct Config {
    seed: u64,
    profile: String,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    items: usize,
    clients: usize,
    throughput_commits: usize,
    slo_commits: usize,
    order_lines: usize,
    new_order_pct: u64,
    delivery_pct: u64,
    stock_level_pct: u64,
    per_warehouse_rate: u64,
}

impl Config {
    fn from_env() -> Self {
        let bench_profile = bench_profile();
        let warehouses = env_usize("JAZZ_S4_WAREHOUSES", bench_profile.select(1, 2, 2)).max(1);
        Self {
            seed: env_u64("JAZZ_SEED", 0x5400_0001),
            profile: std::env::var("JAZZ_PROFILE").unwrap_or_else(|_| "s4-local".to_owned()),
            warehouses,
            districts_per_warehouse: bench_profile.select(2, 5, 10),
            customers_per_district: env_usize(
                "JAZZ_S4_CUSTOMERS_PER_DISTRICT",
                bench_profile.select(2, 4, 5),
            )
            .max(1),
            items: env_usize("JAZZ_S4_ITEMS", bench_profile.select(10, 15, 20)).max(1),
            clients: env_usize("JAZZ_S4_CLIENTS", warehouses).max(1),
            throughput_commits: env_usize(
                "JAZZ_S4_THROUGHPUT_COMMITS",
                bench_profile.select(50, 200, 500),
            )
            .max(1),
            slo_commits: env_usize("JAZZ_S4_SLO_COMMITS", bench_profile.select(10, 25, 50)).max(1),
            order_lines: env_usize("JAZZ_S4_ORDER_LINES", 3).max(1),
            new_order_pct: env_u64("JAZZ_S4_NEW_ORDER_PCT", 70).min(100),
            delivery_pct: env_u64("JAZZ_S4_DELIVERY_PCT", 5).min(100),
            stock_level_pct: env_u64("JAZZ_S4_STOCK_LEVEL_PCT", 5).min(100),
            per_warehouse_rate: env_u64(
                "JAZZ_S4_PER_WAREHOUSE_RATE",
                bench_profile.select(10, 20, 40),
            ),
        }
    }

    fn customers(&self) -> usize {
        self.warehouses * self.districts_per_warehouse * self.customers_per_district
    }

    fn stock_rows(&self) -> usize {
        self.warehouses * self.items
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RunMode {
    ThroughputSettlement,
    ThroughputPropagationInclusive,
    Slo,
    Contention(ContentionLevel),
    HotItemContention,
    ScaleOut,
    ScaleOutLadder {
        max_sustained_warehouses: usize,
        within_slo: bool,
        sustained_rate_met: bool,
        required_rate: f64,
        achieved_rate: f64,
        latency_with_link_us: u64,
    },
}

impl RunMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::ThroughputSettlement => "throughput_settlement",
            Self::ThroughputPropagationInclusive => "throughput_propagation_inclusive",
            Self::Slo => "slo",
            Self::Contention(_) => "contention",
            Self::HotItemContention => "contention_hot_items",
            Self::ScaleOut => "scale_out",
            Self::ScaleOutLadder { .. } => "scale_out_ladder",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerRefreshMode {
    AfterAccept,
    SuppressAfterAccept,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ContentionLevel {
    Low,
    Medium,
    High,
}

impl ContentionLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }

    fn attempts_per_round(self) -> usize {
        match self {
            Self::Low => 2,
            Self::Medium => 4,
            Self::High => 8,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Op {
    NewOrder {
        warehouse: usize,
        district: usize,
        customer: usize,
        items: Vec<(usize, u64)>,
    },
    Payment {
        warehouse: usize,
        district: usize,
        customer: usize,
        amount: f64,
    },
    Delivery {
        warehouse: usize,
        district: usize,
    },
    StockLevel {
        warehouse: usize,
        district: usize,
        threshold: u64,
    },
}

#[derive(Debug)]
struct JazzSummary {
    accepted_schedule: Vec<Op>,
    attempts: usize,
    rejects: usize,
    retries: usize,
    latency: Histogram<u64>,
    edge_acceptance: Histogram<u64>,
    edge_hydration_bytes: u64,
    edge_hydration_rows: usize,
    settle_elapsed_us: u128,
    propagation: Histogram<u64>,
    elapsed_us: u128,
    final_totals: Totals,
}

#[derive(Clone, Debug, PartialEq)]
struct Totals {
    warehouse_ytd: Vec<f64>,
    district_ytd: Vec<f64>,
    district_next_order: Vec<u64>,
    customer_balance_sum_cents: i64,
    stock_quantity_sum: u64,
    orders: u64,
    delivered_orders: u64,
    delivered_order_lines: u64,
    delivered_max_order_by_district: Vec<u64>,
    order_lines: u64,
    payments: u64,
}

#[derive(Debug)]
struct SqliteSummary {
    elapsed_us: u128,
    tx_per_sec: f64,
}

struct ClientHarness {
    _dir: tempfile::TempDir,
    db: Db<RocksDbStorage>,
    _edge_dir: tempfile::TempDir,
    edge: NodeState<RocksDbStorage>,
    edge_peer: PeerState,
    client_peer: PeerState,
    hydration_bytes: u64,
    hydration_rows: usize,
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

fn run_jazz(
    config: &Config,
    committed_target: usize,
    mode: RunMode,
    peer_refresh: PeerRefreshMode,
) -> JazzSummary {
    run_jazz_workload(config, committed_target, None, mode, peer_refresh)
}

fn replay_jazz(
    config: &Config,
    schedule: &[Op],
    mode: RunMode,
    peer_refresh: PeerRefreshMode,
) -> JazzSummary {
    run_jazz_workload(config, schedule.len(), Some(schedule), mode, peer_refresh)
}

fn run_jazz_workload(
    config: &Config,
    committed_target: usize,
    replay_schedule: Option<&[Op]>,
    mode: RunMode,
    peer_refresh: PeerRefreshMode,
) -> JazzSummary {
    let schema = schema();
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    seed_jazz_fixture(config, &mut core);
    let mut clients = open_clients(config.clients, 20, &schema, &mut core);
    let mut rng = Lcg::new(config.seed);
    let mut accepted_schedule = Vec::new();
    let mut attempts = 0;
    let mut rejects = 0;
    let mut retries = 0;
    let mut latency = Histogram::new(3).unwrap();
    let mut edge_acceptance = Histogram::new(3).unwrap();
    let mut settle_elapsed_us = 0_u128;
    let mut propagation = Histogram::new(3).unwrap();
    let start = Instant::now();
    let mut now_ms = 10_000;
    while accepted_schedule.len() < committed_target {
        let client_idx = accepted_schedule.len() % config.clients;
        let op = if let Some(schedule) = replay_schedule {
            if peer_refresh == PeerRefreshMode::SuppressAfterAccept {
                refresh_clients(&mut core, &mut clients);
            }
            schedule[accepted_schedule.len()].clone()
        } else {
            let warehouse = client_idx % config.warehouses;
            next_op(config, &mut rng, warehouse)
        };
        attempts += 1;
        let submit = Instant::now();
        let result = apply_jazz_op(
            &mut clients[client_idx],
            &mut core,
            &op,
            now_ms,
            &mut edge_acceptance,
        );
        now_ms += match mode {
            RunMode::ThroughputSettlement | RunMode::ThroughputPropagationInclusive => 1,
            RunMode::Slo | RunMode::ScaleOut | RunMode::ScaleOutLadder { .. } => 1_000_u64
                .checked_div(config.per_warehouse_rate.max(1))
                .unwrap_or(1)
                .max(1),
            RunMode::Contention(_) | RunMode::HotItemContention => 1,
        };
        match result {
            Ok(true) => {
                let settle_us = submit.elapsed().as_micros() as u64;
                latency.record(settle_us).unwrap();
                settle_elapsed_us += u128::from(settle_us);
                accepted_schedule.push(op);
                if peer_refresh == PeerRefreshMode::AfterAccept {
                    let propagation_start = Instant::now();
                    refresh_clients(&mut core, &mut clients);
                    propagation
                        .record(propagation_start.elapsed().as_micros() as u64)
                        .unwrap();
                }
            }
            Ok(false) => {
                rejects += 1;
                retries += 1;
            }
            Err(err) => panic!("s4 jazz op failed: {err:?}"),
        }
        if attempts > committed_target * 20 {
            panic!("too many retries in s4 run");
        }
    }
    let elapsed_us =
        if replay_schedule.is_some() && peer_refresh == PeerRefreshMode::SuppressAfterAccept {
            settle_elapsed_us
        } else {
            start.elapsed().as_micros()
        };
    let final_totals = jazz_totals(config, &schema, &mut core);
    JazzSummary {
        accepted_schedule,
        attempts,
        rejects,
        retries,
        latency,
        edge_acceptance,
        edge_hydration_bytes: clients.iter().map(|client| client.hydration_bytes).sum(),
        edge_hydration_rows: clients.iter().map(|client| client.hydration_rows).sum(),
        settle_elapsed_us,
        propagation,
        elapsed_us,
        final_totals,
    }
}

fn run_jazz_hot_item_contention(config: &Config) -> JazzSummary {
    let schema = schema();
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    seed_jazz_fixture(config, &mut core);
    let mut clients = open_clients(config.clients.max(2), 70, &schema, &mut core);
    let mut accepted_schedule = Vec::new();
    let mut attempts = 0;
    let rejects = 0;
    let retries = 0;
    let mut latency = Histogram::new(3).unwrap();
    let mut edge_acceptance = Histogram::new(3).unwrap();
    let mut settle_elapsed_us = 0_u128;
    let mut propagation = Histogram::new(3).unwrap();
    let start = Instant::now();
    let mut now_ms = 80_000;
    while accepted_schedule.len() < config.slo_commits {
        let client_idx = accepted_schedule.len() % clients.len();
        let op = Op::NewOrder {
            warehouse: 0,
            district: client_idx % config.districts_per_warehouse,
            customer: client_idx % config.customers_per_district,
            items: vec![(0, 1)],
        };
        attempts += 1;
        let submit = Instant::now();
        match apply_jazz_op(
            &mut clients[client_idx],
            &mut core,
            &op,
            now_ms,
            &mut edge_acceptance,
        ) {
            Ok(true) => {
                let settle_us = submit.elapsed().as_micros() as u64;
                latency.record(settle_us).unwrap();
                settle_elapsed_us += u128::from(settle_us);
                accepted_schedule.push(op);
                let propagation_start = Instant::now();
                refresh_clients(&mut core, &mut clients);
                propagation
                    .record(propagation_start.elapsed().as_micros() as u64)
                    .unwrap();
            }
            Ok(false) => panic!("hot-item contention smoke unexpectedly rejected"),
            Err(err) => panic!("s4 hot-item contention op failed: {err:?}"),
        }
        now_ms += 1;
    }
    let elapsed_us = start.elapsed().as_micros();
    let final_totals = jazz_totals(config, &schema, &mut core);
    JazzSummary {
        accepted_schedule,
        attempts,
        rejects,
        retries,
        latency,
        edge_acceptance,
        edge_hydration_bytes: clients.iter().map(|client| client.hydration_bytes).sum(),
        edge_hydration_rows: clients.iter().map(|client| client.hydration_rows).sum(),
        settle_elapsed_us,
        propagation,
        elapsed_us,
        final_totals,
    }
}

fn run_jazz_contention(config: &Config, level: ContentionLevel) -> JazzSummary {
    let mut hot = config.clone();
    hot.clients = level.attempts_per_round();
    let schema = schema();
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    seed_jazz_fixture(&hot, &mut core);
    let mut clients = open_clients(hot.clients, 40, &schema, &mut core);
    let mut accepted_schedule = Vec::new();
    let mut attempts = 0;
    let mut rejects = 0;
    let mut retries = 0;
    let mut latency = Histogram::new(3).unwrap();
    let mut edge_acceptance = Histogram::new(3).unwrap();
    let mut settle_elapsed_us = 0_u128;
    let mut propagation = Histogram::new(3).unwrap();
    let start = Instant::now();
    let mut now_ms = 50_000;
    while accepted_schedule.len() < config.slo_commits {
        let mut round_accepted = false;
        for client_idx in 0..hot.clients {
            attempts += 1;
            let op = Op::Payment {
                warehouse: 0,
                district: 0,
                customer: client_idx % hot.customers_per_district,
                amount: 1.0 + attempts as f64 / 100.0,
            };
            let submit = Instant::now();
            match apply_jazz_op(
                &mut clients[client_idx],
                &mut core,
                &op,
                now_ms,
                &mut edge_acceptance,
            ) {
                Ok(true) => {
                    let settle_us = submit.elapsed().as_micros() as u64;
                    latency.record(settle_us).unwrap();
                    settle_elapsed_us += u128::from(settle_us);
                    if !round_accepted {
                        accepted_schedule.push(op);
                        round_accepted = true;
                    }
                }
                Ok(false) => {
                    rejects += 1;
                    retries += 1;
                }
                Err(err) => panic!("s4 contention op failed: {err:?}"),
            }
            now_ms += 1;
        }
        let propagation_start = Instant::now();
        refresh_clients(&mut core, &mut clients);
        if round_accepted {
            propagation
                .record(propagation_start.elapsed().as_micros() as u64)
                .unwrap();
        }
        if attempts > config.slo_commits * hot.clients * 20 {
            panic!("too many retries in s4 contention smoke");
        }
    }
    let elapsed_us = start.elapsed().as_micros();
    let final_totals = jazz_totals(&hot, &schema, &mut core);
    JazzSummary {
        accepted_schedule,
        attempts,
        rejects,
        retries,
        latency,
        edge_acceptance,
        edge_hydration_bytes: clients.iter().map(|client| client.hydration_bytes).sum(),
        edge_hydration_rows: clients.iter().map(|client| client.hydration_rows).sum(),
        settle_elapsed_us,
        propagation,
        elapsed_us,
        final_totals,
    }
}

fn apply_jazz_op(
    client: &mut ClientHarness,
    core: &mut NodeState<RocksDbStorage>,
    op: &Op,
    now_ms: u64,
    edge_acceptance: &mut Histogram<u64>,
) -> Result<bool, jazz::db::Error> {
    let tx = client.db.exclusive_tx()?;
    match op {
        Op::NewOrder {
            warehouse,
            district,
            customer,
            items,
        } => {
            let w = warehouse_row(*warehouse);
            let d = district_row(*warehouse, *district);
            let c = customer_row(*warehouse, *district, *customer);
            tx.read(WAREHOUSES, w)?;
            let district_cells = tx.read(DISTRICTS, d)?.expect("district");
            tx.read(CUSTOMERS, c)?;
            let order_number = u64_cell(&district_cells, "nextOrderNumber");
            tx.insert_with_id(
                DISTRICTS,
                d,
                cells([
                    ("warehouse", Value::Uuid(w.0)),
                    ("districtNo", Value::U64(*district as u64)),
                    ("nextOrderNumber", Value::U64(order_number + 1)),
                    ("ytd", Value::F64(f64_cell(&district_cells, "ytd"))),
                ]),
            )?;
            let order = order_row(*warehouse, *district, order_number);
            tx.insert_with_id(
                ORDERS,
                order,
                cells([
                    ("warehouse", Value::Uuid(w.0)),
                    ("district", Value::Uuid(d.0)),
                    ("customer", Value::Uuid(c.0)),
                    ("orderNumber", Value::U64(order_number)),
                    ("lineCount", Value::U64(items.len() as u64)),
                    ("delivered", Value::Bool(false)),
                ]),
            )?;
            for (line_idx, (item, quantity)) in items.iter().enumerate() {
                let item_row = item_row(*item);
                let stock = stock_row(*warehouse, *item);
                let item_cells = tx.read(ITEMS, item_row)?.expect("item");
                let stock_cells = tx.read(STOCK, stock)?.expect("stock");
                let price = f64_cell(&item_cells, "price");
                let old_qty = u64_cell(&stock_cells, "quantity");
                let new_qty = old_qty.saturating_sub(*quantity);
                tx.insert_with_id(
                    STOCK,
                    stock,
                    cells([
                        ("warehouse", Value::Uuid(w.0)),
                        ("item", Value::Uuid(item_row.0)),
                        ("quantity", Value::U64(new_qty)),
                        ("ytd", Value::U64(u64_cell(&stock_cells, "ytd") + quantity)),
                    ]),
                )?;
                tx.insert_with_id(
                    ORDER_LINES,
                    order_line_row(*warehouse, *district, order_number, line_idx),
                    cells([
                        ("order", Value::Uuid(order.0)),
                        ("item", Value::Uuid(item_row.0)),
                        ("stock", Value::Uuid(stock.0)),
                        ("quantity", Value::U64(*quantity)),
                        ("amount", Value::F64(price * *quantity as f64)),
                        ("delivered", Value::Bool(false)),
                    ]),
                )?;
            }
        }
        Op::Payment {
            warehouse,
            district,
            customer,
            amount,
        } => {
            let w = warehouse_row(*warehouse);
            let d = district_row(*warehouse, *district);
            let c = customer_row(*warehouse, *district, *customer);
            let warehouse_cells = tx.read(WAREHOUSES, w)?.expect("warehouse");
            let district_cells = tx.read(DISTRICTS, d)?.expect("district");
            let customer_cells = tx.read(CUSTOMERS, c)?.expect("customer");
            tx.insert_with_id(
                WAREHOUSES,
                w,
                cells([
                    ("name", Value::String(format!("w-{warehouse}"))),
                    (
                        "ytd",
                        Value::F64(f64_cell(&warehouse_cells, "ytd") + amount),
                    ),
                ]),
            )?;
            tx.insert_with_id(
                DISTRICTS,
                d,
                cells([
                    ("warehouse", Value::Uuid(w.0)),
                    ("districtNo", Value::U64(*district as u64)),
                    (
                        "nextOrderNumber",
                        Value::U64(u64_cell(&district_cells, "nextOrderNumber")),
                    ),
                    ("ytd", Value::F64(f64_cell(&district_cells, "ytd") + amount)),
                ]),
            )?;
            tx.insert_with_id(
                CUSTOMERS,
                c,
                cells([
                    ("warehouse", Value::Uuid(w.0)),
                    ("district", Value::Uuid(d.0)),
                    ("customerNo", Value::U64(*customer as u64)),
                    (
                        "balance",
                        Value::F64(f64_cell(&customer_cells, "balance") - amount),
                    ),
                    (
                        "paymentCount",
                        Value::U64(u64_cell(&customer_cells, "paymentCount") + 1),
                    ),
                ]),
            )?;
            tx.insert_with_id(
                PAYMENTS,
                payment_row(*warehouse, *district, *customer, now_ms),
                cells([
                    ("warehouse", Value::Uuid(w.0)),
                    ("district", Value::Uuid(d.0)),
                    ("customer", Value::Uuid(c.0)),
                    ("amount", Value::F64(*amount)),
                ]),
            )?;
        }
        Op::Delivery {
            warehouse,
            district,
        } => {
            let oldest = tx
                .all(ORDERS)?
                .into_iter()
                .filter_map(|row| {
                    let schema = schema();
                    let table = table_schema(&schema, ORDERS);
                    if row.cell(table, "district")?
                        != Value::Uuid(district_row(*warehouse, *district).0)
                        || row.cell(table, "delivered")? != Value::Bool(false)
                    {
                        return None;
                    }
                    let order_no = row.cell(table, "orderNumber")?;
                    Some((value_u64(order_no), row.row_uuid()))
                })
                .min_by_key(|(order_no, _)| *order_no);
            if let Some((order_number, order)) = oldest {
                let order_cells = tx.read(ORDERS, order)?.expect("order");
                let customer = match order_cells.get("customer").unwrap() {
                    Value::Uuid(value) => RowUuid(*value),
                    other => panic!("expected customer uuid, got {other:?}"),
                };
                tx.insert_with_id(
                    ORDERS,
                    order,
                    cells([
                        ("warehouse", Value::Uuid(warehouse_row(*warehouse).0)),
                        (
                            "district",
                            Value::Uuid(district_row(*warehouse, *district).0),
                        ),
                        ("customer", Value::Uuid(customer.0)),
                        ("orderNumber", Value::U64(order_number)),
                        ("lineCount", Value::U64(u64_cell(&order_cells, "lineCount"))),
                        ("delivered", Value::Bool(true)),
                    ]),
                )?;
                let mut total = 0.0;
                for line_idx in 0..u64_cell(&order_cells, "lineCount") as usize {
                    let line = order_line_row(*warehouse, *district, order_number, line_idx);
                    let line_cells = tx.read(ORDER_LINES, line)?.expect("line");
                    total += f64_cell(&line_cells, "amount");
                    tx.insert_with_id(
                        ORDER_LINES,
                        line,
                        cells([
                            ("order", Value::Uuid(order.0)),
                            ("item", line_cells.get("item").unwrap().clone()),
                            ("stock", line_cells.get("stock").unwrap().clone()),
                            ("quantity", line_cells.get("quantity").unwrap().clone()),
                            ("amount", line_cells.get("amount").unwrap().clone()),
                            ("delivered", Value::Bool(true)),
                        ]),
                    )?;
                }
                let customer_cells = tx.read(CUSTOMERS, customer)?.expect("customer");
                tx.insert_with_id(
                    CUSTOMERS,
                    customer,
                    cells([
                        ("warehouse", Value::Uuid(warehouse_row(*warehouse).0)),
                        (
                            "district",
                            Value::Uuid(district_row(*warehouse, *district).0),
                        ),
                        (
                            "customerNo",
                            customer_cells.get("customerNo").unwrap().clone(),
                        ),
                        (
                            "balance",
                            Value::F64(f64_cell(&customer_cells, "balance") + total),
                        ),
                        (
                            "paymentCount",
                            customer_cells.get("paymentCount").unwrap().clone(),
                        ),
                    ]),
                )?;
            }
        }
        Op::StockLevel {
            warehouse,
            district,
            threshold,
        } => {
            let recent_start = tx
                .read(DISTRICTS, district_row(*warehouse, *district))?
                .map(|cells| u64_cell(&cells, "nextOrderNumber").saturating_sub(20))
                .unwrap_or(0);
            for order_no in recent_start..recent_start + 20 {
                let order = order_row(*warehouse, *district, order_no);
                if let Some(order_cells) = tx.read(ORDERS, order)? {
                    for line_idx in 0..u64_cell(&order_cells, "lineCount") as usize {
                        let line = order_line_row(*warehouse, *district, order_no, line_idx);
                        if let Some(line_cells) = tx.read(ORDER_LINES, line)? {
                            let stock = match line_cells.get("stock").unwrap() {
                                Value::Uuid(value) => RowUuid(*value),
                                other => panic!("expected stock uuid, got {other:?}"),
                            };
                            if let Some(stock_cells) = tx.read(STOCK, stock)?
                                && u64_cell(&stock_cells, "quantity") < *threshold
                            {
                                // The benchmark reports validation pressure; the count is client-side.
                            }
                        }
                    }
                }
            }
        }
    }
    let _tx_id = tx.commit()?;
    client.db.tick()?;
    let unit = client
        .outbound
        .borrow_mut()
        .pop_front()
        .expect("db exclusive commit should upload a commit unit");
    if let SyncMessage::CommitUnit { tx, versions } = unit.clone() {
        let edge_start = Instant::now();
        let _ = now_ms;
        client.edge.ingest_relay_commit_unit(tx, versions)?;
        edge_acceptance
            .record(edge_start.elapsed().as_micros() as u64)
            .unwrap();
    } else {
        unreachable!();
    }
    let updates = core.apply_sync_message(unit)?;
    let mut accepted = false;
    for update in updates {
        if let SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        } = update
        {
            accepted = true;
        }
        client.edge.apply_sync_message(update.clone())?;
        client.inbound.borrow_mut().push_back(update);
        client.db.tick()?;
    }
    Ok(accepted)
}

fn open_clients(
    count: usize,
    base_node: u8,
    schema: &JazzSchema,
    core: &mut NodeState<RocksDbStorage>,
) -> Vec<ClientHarness> {
    (0..count)
        .map(|idx| {
            let byte = base_node + idx as u8;
            let (dir, db) = open_db(node(byte), schema.clone(), AuthorId::from_bytes([byte; 16]));
            let outbound = Rc::new(RefCell::new(VecDeque::new()));
            let inbound = Rc::new(RefCell::new(VecDeque::new()));
            let upstream = db.connect_upstream(Box::new(QueueTransport {
                outbound: Rc::clone(&outbound),
                inbound: Rc::clone(&inbound),
            }));
            let (edge_dir, edge) = open_node(node(base_node + 100 + idx as u8), schema.clone());
            let mut client = ClientHarness {
                _dir: dir,
                db,
                _edge_dir: edge_dir,
                edge,
                edge_peer: PeerState::new(),
                client_peer: PeerState::new(),
                hydration_bytes: 0,
                hydration_rows: 0,
                outbound,
                inbound,
                _upstream: upstream,
            };
            refresh_client(core, &mut client);
            client
        })
        .collect()
}

fn refresh_clients(core: &mut NodeState<RocksDbStorage>, clients: &mut [ClientHarness]) {
    for client in clients {
        refresh_client(core, client);
    }
}

fn refresh_client(core: &mut NodeState<RocksDbStorage>, client: &mut ClientHarness) {
    for table in [
        WAREHOUSES,
        DISTRICTS,
        CUSTOMERS,
        ITEMS,
        STOCK,
        ORDERS,
        ORDER_LINES,
        PAYMENTS,
    ] {
        let update = client.edge_peer.current_rows_update(core, table).unwrap();
        client.hydration_bytes += view_update_bytes(&update);
        client.hydration_rows += result_row_count(&update);
        client.edge.apply_sync_message(update).unwrap();
        let update = client
            .client_peer
            .current_rows_update(&mut client.edge, table)
            .unwrap();
        client.inbound.borrow_mut().push_back(update);
        client.db.tick().unwrap();
    }
}

fn seed_jazz_fixture(config: &Config, core: &mut NodeState<RocksDbStorage>) {
    let mut global = 1;
    for w in 0..config.warehouses {
        accept_merge(
            core,
            WAREHOUSES,
            warehouse_row(w),
            cells([
                ("name", Value::String(format!("w-{w}"))),
                ("ytd", Value::F64(0.0)),
            ]),
            &mut global,
        );
        for d in 0..config.districts_per_warehouse {
            accept_merge(
                core,
                DISTRICTS,
                district_row(w, d),
                cells([
                    ("warehouse", Value::Uuid(warehouse_row(w).0)),
                    ("districtNo", Value::U64(d as u64)),
                    ("nextOrderNumber", Value::U64(1)),
                    ("ytd", Value::F64(0.0)),
                ]),
                &mut global,
            );
            for c in 0..config.customers_per_district {
                accept_merge(
                    core,
                    CUSTOMERS,
                    customer_row(w, d, c),
                    cells([
                        ("warehouse", Value::Uuid(warehouse_row(w).0)),
                        ("district", Value::Uuid(district_row(w, d).0)),
                        ("customerNo", Value::U64(c as u64)),
                        ("balance", Value::F64(0.0)),
                        ("paymentCount", Value::U64(0)),
                    ]),
                    &mut global,
                );
            }
        }
    }
    for item in 0..config.items {
        accept_merge(
            core,
            ITEMS,
            item_row(item),
            cells([
                ("name", Value::String(format!("item-{item}"))),
                ("price", Value::F64(1.0 + (item % 100) as f64 / 10.0)),
            ]),
            &mut global,
        );
        for w in 0..config.warehouses {
            accept_merge(
                core,
                STOCK,
                stock_row(w, item),
                cells([
                    ("warehouse", Value::Uuid(warehouse_row(w).0)),
                    ("item", Value::Uuid(item_row(item).0)),
                    ("quantity", Value::U64(100)),
                    ("ytd", Value::U64(0)),
                ]),
                &mut global,
            );
        }
    }
}

fn accept_merge(
    core: &mut NodeState<RocksDbStorage>,
    table: &str,
    row: RowUuid,
    values: BTreeMap<String, Value>,
    global: &mut u64,
) {
    let tx = core
        .commit_mergeable(MergeableCommit::new(table, row, *global + 1).cells(values))
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalSeq(*global)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    *global += 1;
}

fn run_sqlite_reference(config: &Config, schedule: &[Op]) -> SqliteSummary {
    let conn = sqlite_fixture(config);
    let start = Instant::now();
    for op in schedule {
        apply_sqlite_op(&conn, op);
    }
    let elapsed_us = start.elapsed().as_micros();
    SqliteSummary {
        elapsed_us,
        tx_per_sec: schedule.len() as f64 / (elapsed_us as f64 / 1_000_000.0).max(0.000_001),
    }
}

fn assert_sqlite_replay_matches(config: &Config, schedule: &[Op], jazz: &Totals) {
    let conn = sqlite_fixture(config);
    for op in schedule {
        apply_sqlite_op(&conn, op);
    }
    let sqlite = sqlite_totals(config, &conn);
    assert_eq!(&sqlite, jazz);
}

fn sqlite_fixture(config: &Config) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    conn.execute_batch(
        "
        CREATE TABLE warehouses(id INTEGER PRIMARY KEY, ytd REAL NOT NULL);
        CREATE TABLE districts(id INTEGER PRIMARY KEY, warehouse INTEGER NOT NULL, next_order INTEGER NOT NULL, ytd REAL NOT NULL);
        CREATE TABLE customers(id INTEGER PRIMARY KEY, warehouse INTEGER NOT NULL, district INTEGER NOT NULL, balance REAL NOT NULL, payment_count INTEGER NOT NULL);
        CREATE TABLE items(id INTEGER PRIMARY KEY, price REAL NOT NULL);
        CREATE TABLE stock(id INTEGER PRIMARY KEY, warehouse INTEGER NOT NULL, item INTEGER NOT NULL, quantity INTEGER NOT NULL, ytd INTEGER NOT NULL);
        CREATE TABLE orders(id INTEGER PRIMARY KEY, warehouse INTEGER NOT NULL, district INTEGER NOT NULL, customer INTEGER NOT NULL, order_number INTEGER NOT NULL, line_count INTEGER NOT NULL, delivered INTEGER NOT NULL);
        CREATE TABLE order_lines(id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL, item INTEGER NOT NULL, stock_id INTEGER NOT NULL, quantity INTEGER NOT NULL, amount REAL NOT NULL, delivered INTEGER NOT NULL);
        CREATE TABLE payments(id INTEGER PRIMARY KEY, warehouse INTEGER NOT NULL, district INTEGER NOT NULL, customer INTEGER NOT NULL, amount REAL NOT NULL);
        ",
    )
    .unwrap();
    for w in 0..config.warehouses {
        conn.execute("INSERT INTO warehouses VALUES (?1, 0.0)", [w as i64])
            .unwrap();
        for d in 0..config.districts_per_warehouse {
            let district_id = district_idx_configless(w, d) as i64;
            conn.execute(
                "INSERT INTO districts VALUES (?1, ?2, 1, 0.0)",
                params![district_id, w as i64],
            )
            .unwrap();
            for c in 0..config.customers_per_district {
                conn.execute(
                    "INSERT INTO customers VALUES (?1, ?2, ?3, 0.0, 0)",
                    params![
                        customer_idx_configless(w, d, c) as i64,
                        w as i64,
                        district_id
                    ],
                )
                .unwrap();
            }
        }
    }
    for item in 0..config.items {
        conn.execute(
            "INSERT INTO items VALUES (?1, ?2)",
            params![item as i64, 1.0 + (item % 100) as f64 / 10.0],
        )
        .unwrap();
        for w in 0..config.warehouses {
            conn.execute(
                "INSERT INTO stock VALUES (?1, ?2, ?3, 100, 0)",
                params![stock_idx_configless(w, item) as i64, w as i64, item as i64],
            )
            .unwrap();
        }
    }
    conn
}

fn apply_sqlite_op(conn: &Connection, op: &Op) {
    let tx = conn.unchecked_transaction().unwrap();
    match op {
        Op::NewOrder {
            warehouse,
            district,
            customer,
            items,
        } => {
            let district_id = district_idx_configless(*warehouse, *district);
            let order_number: u64 = tx
                .query_row(
                    "SELECT next_order FROM districts WHERE id=?1",
                    [district_id as i64],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap();
            tx.execute(
                "UPDATE districts SET next_order=?1 WHERE id=?2",
                params![order_number + 1, district_id as i64],
            )
            .unwrap();
            let order_id = order_idx_configless(*warehouse, *district, order_number);
            tx.execute(
                "INSERT INTO orders VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
                params![
                    order_id as i64,
                    *warehouse as i64,
                    district_id as i64,
                    customer_idx_configless(*warehouse, *district, *customer) as i64,
                    order_number,
                    items.len() as u64
                ],
            )
            .unwrap();
            for (line_idx, (item, quantity)) in items.iter().enumerate() {
                let stock_id = stock_idx_configless(*warehouse, *item);
                let price: f64 = tx
                    .query_row(
                        "SELECT price FROM items WHERE id=?1",
                        [*item as i64],
                        |row| row.get(0),
                    )
                    .unwrap();
                tx.execute(
                    "UPDATE stock SET quantity=max(quantity-?1, 0), ytd=ytd+?1 WHERE id=?2",
                    params![*quantity, stock_id as i64],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO order_lines VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
                    params![
                        order_line_idx_configless(*warehouse, *district, order_number, line_idx)
                            as i64,
                        order_id as i64,
                        *item as i64,
                        stock_id as i64,
                        *quantity,
                        price * *quantity as f64
                    ],
                )
                .unwrap();
            }
        }
        Op::Payment {
            warehouse,
            district,
            customer,
            amount,
        } => {
            let district_id = district_idx_configless(*warehouse, *district);
            let customer_id = customer_idx_configless(*warehouse, *district, *customer);
            tx.execute(
                "UPDATE warehouses SET ytd=ytd+?1 WHERE id=?2",
                params![*amount, *warehouse as i64],
            )
            .unwrap();
            tx.execute(
                "UPDATE districts SET ytd=ytd+?1 WHERE id=?2",
                params![*amount, district_id as i64],
            )
            .unwrap();
            tx.execute(
                "UPDATE customers SET balance=balance-?1, payment_count=payment_count+1 WHERE id=?2",
                params![*amount, customer_id as i64],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO payments VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    payment_idx_configless(*warehouse, *district, *customer, cents(*amount)) as i64,
                    *warehouse as i64,
                    district_id as i64,
                    customer_id as i64,
                    *amount
                ],
            )
            .unwrap();
        }
        Op::Delivery {
            warehouse,
            district,
        } => {
            let district_id = district_idx_configless(*warehouse, *district);
            let selected = tx
                .query_row(
                    "SELECT id, customer, order_number, line_count FROM orders WHERE warehouse=?1 AND district=?2 AND delivered=0 ORDER BY order_number LIMIT 1",
                    params![*warehouse as i64, district_id as i64],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, u64>(2)?,
                            row.get::<_, u64>(3)?,
                        ))
                    },
                )
                .ok();
            if let Some((order_id, customer_id, order_number, line_count)) = selected {
                tx.execute("UPDATE orders SET delivered=1 WHERE id=?1", [order_id])
                    .unwrap();
                let mut total = 0.0;
                for line_idx in 0..line_count as usize {
                    let line_id =
                        order_line_idx_configless(*warehouse, *district, order_number, line_idx);
                    total += tx
                        .query_row(
                            "SELECT amount FROM order_lines WHERE id=?1",
                            [line_id as i64],
                            |row| row.get::<_, f64>(0),
                        )
                        .unwrap_or(0.0);
                    tx.execute(
                        "UPDATE order_lines SET delivered=1 WHERE id=?1",
                        [line_id as i64],
                    )
                    .unwrap();
                }
                tx.execute(
                    "UPDATE customers SET balance=balance+?1 WHERE id=?2",
                    params![total, customer_id],
                )
                .unwrap();
            }
        }
        Op::StockLevel {
            warehouse,
            district,
            threshold,
        } => {
            let district_id = district_idx_configless(*warehouse, *district);
            let next_order: u64 = tx
                .query_row(
                    "SELECT next_order FROM districts WHERE id=?1",
                    [district_id as i64],
                    |row| row.get(0),
                )
                .unwrap();
            let _low_stock: u64 = tx
                .query_row(
                    "SELECT COUNT(DISTINCT s.id) FROM orders o JOIN order_lines ol ON ol.order_id=o.id JOIN stock s ON s.id=ol.stock_id WHERE o.district=?1 AND o.order_number>=?2 AND s.quantity<?3",
                    params![district_id as i64, next_order.saturating_sub(20), *threshold],
                    |row| row.get(0),
                )
                .unwrap();
        }
    }
    tx.commit().unwrap();
}

fn jazz_totals(
    config: &Config,
    schema: &JazzSchema,
    core: &mut NodeState<RocksDbStorage>,
) -> Totals {
    let warehouse_ytd = (0..config.warehouses)
        .map(|w| row_f64(core, WAREHOUSES, warehouse_row(w), "ytd"))
        .collect();
    let district_ytd = (0..config.warehouses)
        .flat_map(|w| (0..config.districts_per_warehouse).map(move |d| (w, d)))
        .map(|(w, d)| row_f64(core, DISTRICTS, district_row(w, d), "ytd"))
        .collect();
    let district_next_order = (0..config.warehouses)
        .flat_map(|w| (0..config.districts_per_warehouse).map(move |d| (w, d)))
        .map(|(w, d)| row_u64(core, DISTRICTS, district_row(w, d), "nextOrderNumber"))
        .collect();
    let customer_balance_sum_cents = core
        .current_rows(CUSTOMERS, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| {
            cents(value_f64(
                row.cell(table_schema(schema, CUSTOMERS), "balance")
                    .unwrap(),
            ))
        })
        .sum();
    let stock_quantity_sum = core
        .current_rows(STOCK, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| value_u64(row.cell(table_schema(schema, STOCK), "quantity").unwrap()))
        .sum();
    let order_rows = core.current_rows(ORDERS, DurabilityTier::Global).unwrap();
    let order_line_rows = core
        .current_rows(ORDER_LINES, DurabilityTier::Global)
        .unwrap();
    let delivered_max_order_by_district = (0..config.warehouses)
        .flat_map(|w| (0..config.districts_per_warehouse).map(move |d| (w, d)))
        .map(|(w, d)| {
            order_rows
                .iter()
                .filter_map(|row| {
                    let table = table_schema(schema, ORDERS);
                    if row.cell(table, "warehouse").unwrap() == Value::Uuid(warehouse_row(w).0)
                        && row.cell(table, "district").unwrap() == Value::Uuid(district_row(w, d).0)
                        && row.cell(table, "delivered").unwrap() == Value::Bool(true)
                    {
                        Some(value_u64(row.cell(table, "orderNumber").unwrap()))
                    } else {
                        None
                    }
                })
                .max()
                .unwrap_or(0)
        })
        .collect();
    Totals {
        warehouse_ytd,
        district_ytd,
        district_next_order,
        customer_balance_sum_cents,
        stock_quantity_sum,
        orders: order_rows.len() as u64,
        delivered_orders: order_rows
            .iter()
            .filter(|row| {
                row.cell(table_schema(schema, ORDERS), "delivered").unwrap() == Value::Bool(true)
            })
            .count() as u64,
        delivered_order_lines: order_line_rows
            .iter()
            .filter(|row| {
                row.cell(table_schema(schema, ORDER_LINES), "delivered")
                    .unwrap()
                    == Value::Bool(true)
            })
            .count() as u64,
        delivered_max_order_by_district,
        order_lines: order_line_rows.len() as u64,
        payments: core
            .current_rows(PAYMENTS, DurabilityTier::Global)
            .unwrap()
            .len() as u64,
    }
}

fn sqlite_totals(config: &Config, conn: &Connection) -> Totals {
    Totals {
        warehouse_ytd: (0..config.warehouses)
            .map(|w| query_f64(conn, "SELECT ytd FROM warehouses WHERE id=?1", w as i64))
            .collect(),
        district_ytd: (0..config.warehouses)
            .flat_map(|w| (0..config.districts_per_warehouse).map(move |d| (w, d)))
            .map(|(w, d)| {
                query_f64(
                    conn,
                    "SELECT ytd FROM districts WHERE id=?1",
                    district_idx_configless(w, d) as i64,
                )
            })
            .collect(),
        district_next_order: (0..config.warehouses)
            .flat_map(|w| (0..config.districts_per_warehouse).map(move |d| (w, d)))
            .map(|(w, d)| {
                conn.query_row(
                    "SELECT next_order FROM districts WHERE id=?1",
                    [district_idx(config, w, d) as i64],
                    |row| row.get(0),
                )
                .unwrap()
            })
            .collect(),
        customer_balance_sum_cents: cents(query_f64(conn, "SELECT SUM(balance) FROM customers", 0)),
        stock_quantity_sum: conn
            .query_row("SELECT SUM(quantity) FROM stock", [], |row| row.get(0))
            .unwrap(),
        delivered_orders: count_where(conn, "orders", "delivered=1"),
        delivered_order_lines: count_where(conn, "order_lines", "delivered=1"),
        delivered_max_order_by_district: (0..config.warehouses)
            .flat_map(|w| (0..config.districts_per_warehouse).map(move |d| (w, d)))
            .map(|(w, d)| {
                conn.query_row(
                    "SELECT COALESCE(MAX(order_number), 0) FROM orders WHERE warehouse=?1 AND district=?2 AND delivered=1",
                    params![w as i64, district_idx_configless(w, d) as i64],
                    |row| row.get(0),
                )
                .unwrap()
            })
            .collect(),
        orders: count(conn, "orders"),
        order_lines: count(conn, "order_lines"),
        payments: count(conn, "payments"),
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            WAREHOUSES,
            [col("name", ColumnType::String), col("ytd", ColumnType::F64)],
        ),
        TableSchema::new(
            DISTRICTS,
            [
                col("warehouse", ColumnType::Uuid),
                col("districtNo", ColumnType::U64),
                col("nextOrderNumber", ColumnType::U64),
                col("ytd", ColumnType::F64),
            ],
        )
        .with_reference("warehouse", WAREHOUSES),
        TableSchema::new(
            CUSTOMERS,
            [
                col("warehouse", ColumnType::Uuid),
                col("district", ColumnType::Uuid),
                col("customerNo", ColumnType::U64),
                col("balance", ColumnType::F64),
                col("paymentCount", ColumnType::U64),
            ],
        )
        .with_reference("warehouse", WAREHOUSES)
        .with_reference("district", DISTRICTS),
        TableSchema::new(
            ITEMS,
            [
                col("name", ColumnType::String),
                col("price", ColumnType::F64),
            ],
        ),
        TableSchema::new(
            STOCK,
            [
                col("warehouse", ColumnType::Uuid),
                col("item", ColumnType::Uuid),
                col("quantity", ColumnType::U64),
                col("ytd", ColumnType::U64),
            ],
        )
        .with_reference("warehouse", WAREHOUSES)
        .with_reference("item", ITEMS),
        TableSchema::new(
            ORDERS,
            [
                col("warehouse", ColumnType::Uuid),
                col("district", ColumnType::Uuid),
                col("customer", ColumnType::Uuid),
                col("orderNumber", ColumnType::U64),
                col("lineCount", ColumnType::U64),
                col("delivered", ColumnType::Bool),
            ],
        )
        .with_reference("warehouse", WAREHOUSES)
        .with_reference("district", DISTRICTS)
        .with_reference("customer", CUSTOMERS),
        TableSchema::new(
            ORDER_LINES,
            [
                col("order", ColumnType::Uuid),
                col("item", ColumnType::Uuid),
                col("stock", ColumnType::Uuid),
                col("quantity", ColumnType::U64),
                col("amount", ColumnType::F64),
                col("delivered", ColumnType::Bool),
            ],
        )
        .with_reference("order", ORDERS)
        .with_reference("item", ITEMS)
        .with_reference("stock", STOCK),
        TableSchema::new(
            PAYMENTS,
            [
                col("warehouse", ColumnType::Uuid),
                col("district", ColumnType::Uuid),
                col("customer", ColumnType::Uuid),
                col("amount", ColumnType::F64),
            ],
        )
        .with_reference("warehouse", WAREHOUSES)
        .with_reference("district", DISTRICTS)
        .with_reference("customer", CUSTOMERS),
    ])
}

fn emit_summary(
    config: &Config,
    profile: &PeerProfile,
    jazz: &JazzSummary,
    sqlite: Option<&SqliteSummary>,
    mode: RunMode,
) {
    let mut fields = metadata_fields(
        "s4_order_processing",
        "synchronous",
        config.seed,
        &profile.name,
    );
    fields.insert("phase".to_owned(), json!(mode.as_str()));
    fields.insert("warehouses".to_owned(), json!(config.warehouses));
    fields.insert(
        "districts_per_warehouse".to_owned(),
        json!(config.districts_per_warehouse),
    );
    fields.insert(
        "customers_per_district".to_owned(),
        json!(config.customers_per_district),
    );
    fields.insert("items".to_owned(), json!(config.items));
    fields.insert("stock_rows".to_owned(), json!(config.stock_rows()));
    fields.insert("customers".to_owned(), json!(config.customers()));
    fields.insert("clients".to_owned(), json!(config.clients));
    fields.insert("committed".to_owned(), json!(jazz.accepted_schedule.len()));
    fields.insert("attempts".to_owned(), json!(jazz.attempts));
    fields.insert("rejects".to_owned(), json!(jazz.rejects));
    fields.insert("retries".to_owned(), json!(jazz.retries));
    fields.insert(
        "abort_retry_rate".to_owned(),
        json!(jazz.retries as f64 / jazz.attempts as f64),
    );
    let committed = jazz.accepted_schedule.len() as f64;
    let jazz_wall_tx_per_sec = committed / (jazz.elapsed_us as f64 / 1_000_000.0);
    let jazz_settle_tx_per_sec =
        committed / (jazz.settle_elapsed_us as f64 / 1_000_000.0).max(0.000_001);
    fields.insert(
        "jazz_settle_tx_per_sec".to_owned(),
        json!(jazz_settle_tx_per_sec),
    );
    fields.insert(
        "jazz_wall_tx_per_sec".to_owned(),
        json!(jazz_wall_tx_per_sec),
    );
    fields.insert(
        "jazz_exclusive_tx_per_sec".to_owned(),
        json!(jazz_wall_tx_per_sec),
    );
    fields.insert(
        "deprecated_alias".to_owned(),
        json!("jazz_exclusive_tx_per_sec=jazz_wall_tx_per_sec"),
    );
    fields.insert(
        "delivered_orders".to_owned(),
        json!(jazz.final_totals.delivered_orders),
    );
    fields.insert(
        "delivered_order_lines".to_owned(),
        json!(jazz.final_totals.delivered_order_lines),
    );
    fields.insert(
        "settle_p50_us".to_owned(),
        json!(jazz.latency.value_at_quantile(0.5)),
    );
    fields.insert(
        "settle_p95_us".to_owned(),
        json!(jazz.latency.value_at_quantile(0.95)),
    );
    fields.insert(
        "propagation_p50_us".to_owned(),
        json!(jazz.propagation.value_at_quantile(0.5)),
    );
    fields.insert(
        "propagation_p95_us".to_owned(),
        json!(jazz.propagation.value_at_quantile(0.95)),
    );
    fields.insert(
        "link_rtt_floor_us".to_owned(),
        json!(profile.one_way_latency_ms * 2_000),
    );
    if let Some(sqlite) = sqlite {
        fields.insert("sqlite_tx_per_sec".to_owned(), json!(sqlite.tx_per_sec));
        fields.insert("sqlite_elapsed_us".to_owned(), json!(sqlite.elapsed_us));
        if jazz.accepted_schedule.len() >= 200 {
            fields.insert(
                "jazz_sqlite_ratio".to_owned(),
                json!(jazz_wall_tx_per_sec / sqlite.tx_per_sec),
            );
        } else {
            fields.insert("jazz_sqlite_ratio".to_owned(), JsonValue::Null);
            fields.insert(
                "ratio_omitted_reason".to_owned(),
                json!("minimum sample is 200 committed transactions"),
            );
        }
    }
    fields.insert("same_schedule_replay".to_owned(), json!("matched"));
    match mode {
        RunMode::Contention(level) => {
            fields.insert("contention_level".to_owned(), json!(level.as_str()));
            fields.insert(
                "hot_attempts_per_round".to_owned(),
                json!(level.attempts_per_round()),
            );
            fields.insert(
                "schedule_note".to_owned(),
                json!(
                    "deterministic lock-step stale-client construction over hot payment counters; abort curve is by construction, not organic traffic"
                ),
            );
        }
        RunMode::HotItemContention => {
            fields.insert(
                "schedule_note".to_owned(),
                json!("hot-item NewOrder contention smoke; overlapping stock row across clients with refresh between accepted transactions"),
            );
            fields.insert("hot_item".to_owned(), json!(0));
        }
        RunMode::ScaleOutLadder {
            max_sustained_warehouses,
            within_slo,
            sustained_rate_met,
            required_rate,
            achieved_rate,
            latency_with_link_us,
        } => {
            fields.insert(
                "max_sustained_warehouses_within_slo".to_owned(),
                json!(max_sustained_warehouses),
            );
            fields.insert("within_slo".to_owned(), json!(within_slo));
            fields.insert("sustained_rate_met".to_owned(), json!(sustained_rate_met));
            fields.insert("required_tx_per_sec".to_owned(), json!(required_rate));
            fields.insert("achieved_tx_per_sec".to_owned(), json!(achieved_rate));
            fields.insert(
                "settle_p95_with_link_us".to_owned(),
                json!(latency_with_link_us),
            );
            fields.insert(
                "slo_p95_settle_us".to_owned(),
                json!(profile.one_way_latency_ms * 20_000),
            );
        }
        RunMode::ThroughputSettlement => {
            fields.insert("throughput_line".to_owned(), json!("settlement throughput"));
            fields.insert(
                "measurement_includes".to_owned(),
                json!("engine commit/accept path"),
            );
            fields.insert(
                "measurement_excludes".to_owned(),
                json!("per-accepted-commit peer current_rows_update fan-out"),
            );
        }
        RunMode::ThroughputPropagationInclusive => {
            fields.insert(
                "throughput_line".to_owned(),
                json!("propagation-inclusive throughput"),
            );
            fields.insert(
                "measurement_includes".to_owned(),
                json!("engine commit/accept path plus per-accepted-commit peer current_rows_update fan-out"),
            );
            fields.insert("measurement_excludes".to_owned(), JsonValue::Null);
        }
        RunMode::Slo | RunMode::ScaleOut => {}
    }
    let line = JsonValue::Object(fields).to_string();
    emit_json_line("s4_order_processing", &line);
    emit_edge_phase_summaries(config, profile, jazz);
}

fn emit_edge_phase_summaries(config: &Config, profile: &PeerProfile, jazz: &JazzSummary) {
    let mut acceptance = metadata_fields(
        "s4_order_processing",
        "synchronous",
        config.seed,
        &profile.name,
    );
    acceptance.insert("phase".to_owned(), json!("edge_mergeable_acceptance"));
    acceptance.insert(
        "acceptance_p50_us".to_owned(),
        json!(jazz.edge_acceptance.value_at_quantile(0.50)),
    );
    acceptance.insert(
        "acceptance_p95_us".to_owned(),
        json!(jazz.edge_acceptance.value_at_quantile(0.95)),
    );
    acceptance.insert("durability_tier".to_owned(), json!("Edge"));
    acceptance.insert("clients".to_owned(), json!(config.clients));
    emit_json_line(
        "s4_order_processing",
        &JsonValue::Object(acceptance).to_string(),
    );

    let mut hydration = metadata_fields(
        "s4_order_processing",
        "synchronous",
        config.seed,
        &profile.name,
    );
    hydration.insert("phase".to_owned(), json!("edge_permission_scope_hydration"));
    hydration.insert("scope".to_owned(), json!("order_processing_table_surface"));
    hydration.insert(
        "hydration_bytes".to_owned(),
        json!(jazz.edge_hydration_bytes),
    );
    hydration.insert(
        "hydration_floor_bytes".to_owned(),
        json!(jazz.edge_hydration_bytes),
    );
    hydration.insert("hydration_rows".to_owned(), json!(jazz.edge_hydration_rows));
    emit_json_line(
        "s4_order_processing",
        &JsonValue::Object(hydration).to_string(),
    );
}

fn next_op(config: &Config, rng: &mut Lcg, warehouse: usize) -> Op {
    let district = rng.choose(config.districts_per_warehouse);
    let customer = rng.choose(config.customers_per_district);
    let roll = rng.next_u64() % 100;
    if roll < config.new_order_pct {
        let mut items = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..config.order_lines {
            let mut item = rng.choose(config.items);
            while !seen.insert(item) {
                item = (item + 1) % config.items;
            }
            items.push((item, 1 + (rng.next_u64() % 5)));
        }
        Op::NewOrder {
            warehouse,
            district,
            customer,
            items,
        }
    } else if roll < config.new_order_pct + config.delivery_pct {
        Op::Delivery {
            warehouse,
            district,
        }
    } else if roll < config.new_order_pct + config.delivery_pct + config.stock_level_pct {
        Op::StockLevel {
            warehouse,
            district,
            threshold: 50,
        }
    } else {
        Op::Payment {
            warehouse,
            district,
            customer,
            amount: 1.0 + (rng.next_u64() % 5_000) as f64 / 100.0,
        }
    }
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let node = NodeState::new(node_uuid, schema, storage).unwrap();
    (dir, node)
}

fn open_db(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    author: AuthorId,
) -> (tempfile::TempDir, Db<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(u64::from(
            node_uuid.as_bytes()[0],
        )))),
        large_value_checkpoint_op_interval: 1024,
    }))
    .unwrap();
    (dir, db)
}

fn cells<const N: usize>(items: [(&str, Value); N]) -> BTreeMap<String, Value> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

fn col(name: &str, ty: ColumnType) -> ColumnSchema {
    ColumnSchema::new(name, ty)
}

fn row(tag: u8, value: u64) -> RowUuid {
    let mut bytes = [tag; 16];
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn warehouse_row(w: usize) -> RowUuid {
    row(1, w as u64)
}

fn district_row(w: usize, d: usize) -> RowUuid {
    row(2, district_idx_configless(w, d) as u64)
}

fn customer_row(w: usize, d: usize, c: usize) -> RowUuid {
    row(3, customer_idx_configless(w, d, c) as u64)
}

fn item_row(i: usize) -> RowUuid {
    row(4, i as u64)
}

fn stock_row(w: usize, item: usize) -> RowUuid {
    row(5, stock_idx_configless(w, item) as u64)
}

fn order_row(w: usize, d: usize, order_number: u64) -> RowUuid {
    row(6, order_idx_configless(w, d, order_number) as u64)
}

fn order_line_row(w: usize, d: usize, order_number: u64, line: usize) -> RowUuid {
    row(
        7,
        order_line_idx_configless(w, d, order_number, line) as u64,
    )
}

fn payment_row(w: usize, d: usize, c: usize, now_ms: u64) -> RowUuid {
    row(8, payment_idx_configless(w, d, c, now_ms as i64) as u64)
}

fn district_idx(config: &Config, w: usize, d: usize) -> usize {
    w * config.districts_per_warehouse + d
}

fn district_idx_configless(w: usize, d: usize) -> usize {
    w * 10 + d
}

fn customer_idx_configless(w: usize, d: usize, c: usize) -> usize {
    district_idx_configless(w, d) * 100_000 + c
}

fn stock_idx_configless(w: usize, item: usize) -> usize {
    w * 1_000_000 + item
}

fn order_idx_configless(w: usize, d: usize, order_number: u64) -> usize {
    district_idx_configless(w, d) * 1_000_000 + order_number as usize
}

fn order_line_idx_configless(w: usize, d: usize, order_number: u64, line: usize) -> usize {
    order_idx_configless(w, d, order_number) * 16 + line
}

fn payment_idx_configless(w: usize, d: usize, c: usize, suffix: i64) -> usize {
    customer_idx_configless(w, d, c) * 100_000 + suffix.unsigned_abs() as usize
}

fn value_u64(value: Value) -> u64 {
    match value {
        Value::U64(value) => value,
        other => panic!("expected u64, got {other:?}"),
    }
}

fn value_f64(value: Value) -> f64 {
    match value {
        Value::F64(value) => value,
        other => panic!("expected f64, got {other:?}"),
    }
}

fn u64_cell(cells: &BTreeMap<String, Value>, name: &str) -> u64 {
    value_u64(cells.get(name).unwrap().clone())
}

fn f64_cell(cells: &BTreeMap<String, Value>, name: &str) -> f64 {
    value_f64(cells.get(name).unwrap().clone())
}

fn row_u64(core: &mut NodeState<RocksDbStorage>, table: &str, row: RowUuid, column: &str) -> u64 {
    let schema = schema();
    let table_schema = table_schema(&schema, table);
    let row = core
        .current_rows(table, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.row_uuid() == row)
        .unwrap();
    value_u64(row.cell(table_schema, column).unwrap())
}

fn row_f64(core: &mut NodeState<RocksDbStorage>, table: &str, row: RowUuid, column: &str) -> f64 {
    let schema = schema();
    let table_schema = table_schema(&schema, table);
    let row = core
        .current_rows(table, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.row_uuid() == row)
        .unwrap();
    value_f64(row.cell(table_schema, column).unwrap())
}

fn table_schema<'a>(schema: &'a JazzSchema, table: &str) -> &'a TableSchema {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .expect("known table")
}

fn view_update_bytes(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::ViewUpdate {
            version_bundles,
            complete_tx_refs,
            result_row_adds,
            result_row_removes,
            ..
        } => {
            version_bundles
                .iter()
                .flat_map(|bundle| bundle.versions.iter())
                .map(|version| version.record().raw().len() as u64 + 64)
                .sum::<u64>()
                + (complete_tx_refs.len() as u64 * 24)
                + ((result_row_adds.len() + result_row_removes.len()) as u64 * 64)
        }
        _ => 0,
    }
}

fn result_row_count(update: &SyncMessage) -> usize {
    match update {
        SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } => result_row_adds.len() + result_row_removes.len(),
        _ => 0,
    }
}

fn cents(value: f64) -> i64 {
    (value * 100.0).round() as i64
}

fn query_f64(conn: &Connection, sql: &str, param: i64) -> f64 {
    if sql.contains("?1") {
        conn.query_row(sql, [param], |row| row.get(0)).unwrap()
    } else {
        conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }
}

fn count(conn: &Connection, table: &str) -> u64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap()
}

fn count_where(conn: &Connection, table: &str, predicate: &str) -> u64 {
    conn.query_row(
        &format!("SELECT COUNT(*) FROM {table} WHERE {predicate}"),
        [],
        |row| row.get(0),
    )
    .unwrap()
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

#[derive(Clone, Debug)]
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    fn choose(&mut self, len: usize) -> usize {
        (self.next_u64() as usize) % len
    }
}
