//! Geo-sharding spike (private exploration, not for merge).
//!
//! Benchmark B2: ordered pagination across N shard authorities. Each shard
//! is an ordinary `Db` holding its own rows; the "router" here is plain
//! test code that issues the per-shard reads a real edge router would send,
//! and counts what would cross the wire and how many round trips it takes
//! under a geo latency matrix.
//!
//! Run: `cargo test -p jazz --test geo_shard_spike -- --ignored --nocapture`

mod common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{OrderDirection, Query, col, gte, lit, lt};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

// ---- geo model --------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Region {
    UsEast,
    EuWest,
    ApSoutheast,
}

const REGIONS: [Region; 3] = [Region::UsEast, Region::EuWest, Region::ApSoutheast];

/// Approximate public round-trip times in ms, used only as model inputs.
fn rtt(a: Region, b: Region) -> f64 {
    use Region::*;
    match (a, b) {
        _ if a == b => 2.0,
        (UsEast, EuWest) | (EuWest, UsEast) => 75.0,
        (EuWest, ApSoutheast) | (ApSoutheast, EuWest) => 160.0,
        (UsEast, ApSoutheast) | (ApSoutheast, UsEast) => 220.0,
        _ => unreachable!(),
    }
}

/// Bytes a row image of this table costs on the wire (id, score, payload,
/// framing), and what a bare sort key costs (score + id).
const ROW_BYTES: usize = 16 + 8 + PAYLOAD_LEN + 24;
const KEY_BYTES: usize = 16 + 8;
const PAYLOAD_LEN: usize = 120;

// ---- shards -----------------------------------------------------------------

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("score", ColumnType::BigInt)
                    .column("payload", ColumnType::Text)
                    .index_only(["score"])
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

struct Shard {
    db: Db,
    region: Region,
}

fn open_shard(i: usize) -> Shard {
    let db = block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa0 + i as u8; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    Shard {
        db,
        region: REGIONS[i % REGIONS.len()],
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

#[derive(Clone, Copy, Debug)]
enum Distribution {
    /// Every row lands on a random shard.
    Uniform,
    /// A feed ordered by time where one region writes 80% of the newest
    /// fifth of the rows.
    HotRecent,
}

/// Rows with distinct scores (so the order is total without the id
/// tie-break), spread over the shards.
fn populate(shards: &[Shard], rows: usize, dist: Distribution, seed: u64) {
    let mut rng = Rng(seed);
    let payload = "x".repeat(PAYLOAD_LEN);
    for score in 0..rows {
        let shard = match dist {
            Distribution::Uniform => rng.below(shards.len()),
            Distribution::HotRecent => {
                if score * 5 >= rows * 4 && rng.unit() < 0.8 {
                    0
                } else {
                    rng.below(shards.len())
                }
            }
        };
        let values = BTreeMap::from([
            ("score".to_owned(), Value::I64(score as i64)),
            ("payload".to_owned(), Value::String(payload.clone())),
        ]);
        block_on(shards[shard].db.insert("items", values, Default::default())).unwrap();
    }
}

/// Scores of a shard's rows with `lo <= score < hi`, highest first, at most
/// `limit` of them.
fn shard_page(shard: &Shard, lo: Option<i64>, hi: Option<i64>, limit: Option<usize>) -> Vec<i64> {
    let mut query = Query::from("items");
    if let Some(hi) = hi {
        query = query.filter(lt(col("score"), lit(Value::I64(hi))));
    }
    if let Some(lo) = lo {
        query = query.filter(gte(col("score"), lit(Value::I64(lo))));
    }
    let mut query = query.order_by("score", OrderDirection::Desc);
    if let Some(limit) = limit {
        query = query.limit(limit);
    }
    let prepared = shard.db.prepare_query(&query).unwrap();
    block_on(shard.db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|row| match row.cells().find(|(name, _)| *name == "score") {
            Some((_, Some(Value::I64(score)))) => *score,
            other => panic!("score cell: {other:?}"),
        })
        .collect()
}

// ---- router strategies ------------------------------------------------------

#[derive(Default, Debug, Clone)]
struct PageCost {
    rows: usize,
    keys: usize,
    rounds: usize,
    latency_ms: f64,
}

impl PageCost {
    fn bytes(&self) -> usize {
        self.rows * ROW_BYTES + self.keys * KEY_BYTES
    }
    fn round(&mut self, router: Region, shards: &[&Shard]) {
        self.rounds += 1;
        self.latency_ms += shards
            .iter()
            .map(|s| rtt(router, s.region))
            .fold(0.0, f64::max);
    }
}

/// One strategy's view of a paged read: page `n+1` starts below `cursor`.
trait Pager {
    fn name(&self) -> &'static str;
    fn page(
        &mut self,
        shards: &[Shard],
        router: Region,
        cursor: Option<i64>,
        k: usize,
    ) -> (Vec<i64>, PageCost);
}

/// Every shard returns its top k; the router keeps k.
struct Naive;

impl Pager for Naive {
    fn name(&self) -> &'static str {
        "naive top-k per shard"
    }
    fn page(
        &mut self,
        shards: &[Shard],
        router: Region,
        cursor: Option<i64>,
        k: usize,
    ) -> (Vec<i64>, PageCost) {
        let mut cost = PageCost::default();
        let mut all = Vec::new();
        for shard in shards {
            let rows = shard_page(shard, None, cursor, Some(k));
            cost.rows += rows.len();
            all.extend(rows);
        }
        cost.round(router, &shards.iter().collect::<Vec<_>>());
        all.sort_unstable_by(|a, b| b.cmp(a));
        all.truncate(k);
        (all, cost)
    }
}

/// Round 1: every shard sends its top-k keys only. Round 2: every shard
/// sends exactly its rows at or above the k-th key. Zero rows overfetched.
struct KeysFirst;

impl Pager for KeysFirst {
    fn name(&self) -> &'static str {
        "keys first (q = 0)"
    }
    fn page(
        &mut self,
        shards: &[Shard],
        router: Region,
        cursor: Option<i64>,
        k: usize,
    ) -> (Vec<i64>, PageCost) {
        let mut cost = PageCost::default();
        let mut keys = Vec::new();
        for shard in shards {
            let page = shard_page(shard, None, cursor, Some(k));
            cost.keys += page.len();
            keys.extend(page);
        }
        cost.round(router, &shards.iter().collect::<Vec<_>>());
        keys.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&bound) = keys.get(k.saturating_sub(1)).or(keys.last()) else {
            return (Vec::new(), cost);
        };
        let mut rows = Vec::new();
        let mut asked = Vec::new();
        for shard in shards {
            let page = shard_page(shard, Some(bound), cursor, None);
            if !page.is_empty() {
                asked.push(shard);
            }
            cost.rows += page.len();
            rows.extend(page);
        }
        cost.round(router, &asked);
        rows.sort_unstable_by(|a, b| b.cmp(a));
        (rows, cost)
    }
}

/// Each shard sends its top q_i rows plus the key of its next row; q_i
/// comes from the shard's share of the previous page (k/N on the first).
/// A shard whose next key could still be in the page is asked for more,
/// bounded by the current k-th key, until no shard can contribute.
struct AdaptiveQuota {
    /// Extra speculation on top of the expected share, as a fraction of it.
    slack: f64,
    shares: Option<Vec<f64>>,
}

impl Pager for AdaptiveQuota {
    fn name(&self) -> &'static str {
        if self.slack < 1.5 {
            "adaptive quota, 1 sd"
        } else {
            "adaptive quota, 2 sd"
        }
    }
    fn page(
        &mut self,
        shards: &[Shard],
        router: Region,
        cursor: Option<i64>,
        k: usize,
    ) -> (Vec<i64>, PageCost) {
        let n = shards.len();
        let shares = self
            .shares
            .clone()
            .unwrap_or_else(|| vec![1.0 / n as f64; n]);
        let mut cost = PageCost::default();
        // Per shard: rows received (descending), the key of its next row,
        // and how far down it has been read.
        let mut received: Vec<Vec<i64>> = vec![Vec::new(); n];
        let mut next: Vec<Option<i64>> = vec![None; n];
        let mut asking: Vec<usize> = (0..n).collect();
        let mut quota: Vec<usize> = shares
            .iter()
            .map(|share| {
                // Expected share plus `slack` standard deviations of the
                // count's sampling noise (roughly Poisson).
                // Blend the observed share with the uniform prior so a
                // shard that contributed nothing last page is still asked.
                let mean = (0.5 * share + 0.5 / n as f64) * k as f64;
                (mean + self.slack * mean.sqrt()).ceil() as usize
            })
            .collect();
        loop {
            let mut round = Vec::new();
            for &i in &asking {
                let hi = received[i].last().copied().or(cursor);
                let q = quota[i];
                // q rows plus the next row's key (q = 0 asks for the key only).
                let mut page = shard_page(&shards[i], None, hi, Some(q + 1));
                next[i] = if page.len() > q { page.pop() } else { None };
                if next[i].is_some() {
                    cost.keys += 1;
                }
                cost.rows += page.len();
                received[i].extend(page);
                round.push(&shards[i]);
            }
            cost.round(router, &round);
            let mut merged: Vec<i64> = received.iter().flatten().copied().collect();
            merged.sort_unstable_by(|a, b| b.cmp(a));
            let kth = if merged.len() >= k {
                Some(merged[k - 1])
            } else {
                None
            };
            // A shard still owes rows if its next key would rank in the page.
            asking = (0..n)
                .filter(|&i| match (next[i], kth) {
                    (None, _) => false,
                    (Some(_), None) => true,
                    (Some(key), Some(kth)) => key > kth,
                })
                .collect();
            if asking.is_empty() {
                merged.truncate(k);
                let total = merged.len().max(1) as f64;
                let mut counts = vec![0usize; n];
                for (i, rows) in received.iter().enumerate() {
                    counts[i] = rows.iter().filter(|s| merged.contains(s)).count();
                }
                self.shares = Some(counts.iter().map(|&c| c as f64 / total).collect());
                return (merged, cost);
            }
            for &i in &asking {
                // Double what this shard is asked for each extra round.
                quota[i] = (quota[i] * 2).max(1);
            }
        }
    }
}

fn run_pages(
    pager: &mut dyn Pager,
    shards: &[Shard],
    router: Region,
    k: usize,
    pages: usize,
) -> (Vec<Vec<i64>>, Vec<PageCost>) {
    let mut cursor = None;
    let mut out = Vec::new();
    let mut costs = Vec::new();
    for _ in 0..pages {
        let (page, cost) = pager.page(shards, router, cursor, k);
        cursor = page.last().copied();
        out.push(page);
        costs.push(cost);
    }
    (out, costs)
}

#[test]
#[ignore = "benchmark; run explicitly"]
fn b2_pagination_overfill() {
    const ROWS: usize = 4_000;
    const PAGES: usize = 5;
    let router = Region::EuWest;
    println!(
        "| shards | dist | k | strategy | rows/page (p1, later) | keys/page (p1, later) | KB/page (p1, later) | rounds (p1, later) | model ms (p1, later) |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for &n in &[1usize, 2, 4, 8, 16] {
        for dist in [Distribution::Uniform, Distribution::HotRecent] {
            let shards: Vec<Shard> = (0..n).map(open_shard).collect();
            populate(&shards, ROWS, dist, 0x5eed + n as u64);
            for &k in &[20usize, 100] {
                let mut reference = None;
                let pagers: Vec<Box<dyn Pager>> = vec![
                    Box::new(Naive),
                    Box::new(KeysFirst),
                    Box::new(AdaptiveQuota {
                        slack: 1.0,
                        shares: None,
                    }),
                    Box::new(AdaptiveQuota {
                        slack: 2.0,
                        shares: None,
                    }),
                ];
                for mut pager in pagers {
                    let (pages, costs) = run_pages(pager.as_mut(), &shards, router, k, PAGES);
                    // Every strategy returns the same pages: the global order.
                    let expected: Vec<Vec<i64>> = (0..PAGES)
                        .map(|p| {
                            ((ROWS - (p + 1) * k)..(ROWS - p * k))
                                .rev()
                                .map(|s| s as i64)
                                .collect()
                        })
                        .collect();
                    assert_eq!(pages, expected, "{} n={n} k={k} {dist:?}", pager.name());
                    reference.get_or_insert(pages);
                    let later = &costs[1..];
                    let avg = |f: &dyn Fn(&PageCost) -> f64| {
                        later.iter().map(f).sum::<f64>() / later.len() as f64
                    };
                    println!(
                        "| {n} | {dist:?} | {k} | {} | {}, {:.1} | {}, {:.1} | {:.1}, {:.1} | {}, {:.1} | {:.0}, {:.0} |",
                        pager.name(),
                        costs[0].rows,
                        avg(&|c| c.rows as f64),
                        costs[0].keys,
                        avg(&|c| c.keys as f64),
                        costs[0].bytes() as f64 / 1024.0,
                        avg(&|c| c.bytes() as f64 / 1024.0),
                        costs[0].rounds,
                        avg(&|c| c.rounds as f64),
                        costs[0].latency_ms,
                        avg(&|c| c.latency_ms),
                    );
                }
            }
        }
    }
}

// ---- B2b: live windows ------------------------------------------------------
//
// A pure model of what crosses the wire while a `limit k` subscription over
// N shards stays maintained. No Db here: the per-shard SQL is the same range
// read as above, and this measures the protocol, not SQLite.

use std::collections::BTreeSet;

#[derive(Default, Debug)]
struct LiveCost {
    rows: usize,
    keys: usize,
    control: usize,
    /// Rows the client holds for the window, summed over checkpoints.
    held: usize,
    checkpoints: usize,
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    /// New rows land at random positions; deletes hit random rows.
    Random,
    /// A feed: new rows are the newest (top of the order); deletes hit
    /// random rows, a third of them inside the visible window.
    Feed,
}

/// Each shard keeps its own top-k window: every change to a shard's local
/// top-k ships a row, and the client holds N windows.
fn live_naive(shards: &mut [BTreeSet<i64>], k: usize, ops: &[(usize, i64, bool)]) -> LiveCost {
    let mut cost = LiveCost::default();
    let top = |s: &BTreeSet<i64>| s.iter().rev().nth(k - 1).copied();
    cost.rows += shards.iter().map(|s| s.len().min(k)).sum::<usize>();
    for &(i, key, insert) in ops {
        let floor = top(&shards[i]);
        if insert {
            shards[i].insert(key);
            if floor.is_none_or(|f| key > f) {
                cost.rows += 1;
            }
        } else {
            shards[i].remove(&key);
            if floor.is_none_or(|f| key >= f) {
                // The leaving row is unlisted; the next row slides in.
                cost.control += 1;
                if shards[i].len() >= k {
                    cost.rows += 1;
                }
            }
        }
        cost.held += shards.iter().map(|s| s.len().min(k)).sum::<usize>();
        cost.checkpoints += 1;
    }
    cost
}

/// The bound protocol: shard i streams its rows with key >= B and keeps
/// the router told of its sentinel (its first key below B). The router
/// lets the window hold k..=k+slack rows and moves B only past that.
fn live_bound(
    shards: &mut [BTreeSet<i64>],
    k: usize,
    slack: usize,
    ops: &[(usize, i64, bool)],
) -> LiveCost {
    let mut cost = LiveCost::default();
    let n = shards.len();
    // Initial load: keys-first, then exactly k rows.
    let mut all: Vec<i64> = shards.iter().flatten().copied().collect();
    all.sort_unstable_by(|a, b| b.cmp(a));
    let mut bound = all[k - 1];
    cost.keys += shards.iter().map(|s| s.len().min(k)).sum::<usize>();
    cost.rows += k;
    let sentinel = |s: &BTreeSet<i64>, bound: i64| s.range(..bound).next_back().copied();
    let mut sentinels: Vec<Option<i64>> = shards.iter().map(|s| sentinel(s, bound)).collect();
    cost.keys += n;
    let in_window = |shards: &[BTreeSet<i64>], bound: i64| -> usize {
        shards.iter().map(|s| s.range(bound..).count()).sum()
    };
    for &(i, key, insert) in ops {
        if insert {
            shards[i].insert(key);
            if key >= bound {
                cost.rows += 1;
            }
        } else {
            shards[i].remove(&key);
            if key >= bound {
                cost.control += 1; // the shard unlists it
            }
        }
        // Keep this shard's sentinel current.
        let now = sentinel(&shards[i], bound);
        if now != sentinels[i] {
            sentinels[i] = now;
            cost.keys += 1;
        }
        let mut count = in_window(shards, bound);
        if count > k + slack {
            // Tighten to the k-th key: shards drop the rows below it.
            let mut window: Vec<i64> = shards
                .iter()
                .flat_map(|s| s.range(bound..))
                .copied()
                .collect();
            window.sort_unstable_by(|a, b| b.cmp(a));
            let new_bound = window[k - 1];
            for (j, s) in shards.iter().enumerate() {
                if s.range(bound..new_bound).next().is_some() {
                    cost.control += 1; // new bound to shard j
                    sentinels[j] = sentinel(s, new_bound);
                    cost.keys += 1;
                }
            }
            bound = new_bound;
            count = k;
        }
        while count < k {
            // Loosen to the best sentinel: that shard ships one row.
            let Some((j, key)) = sentinels
                .iter()
                .enumerate()
                .filter_map(|(j, s)| s.map(|s| (j, s)))
                .max_by_key(|&(_, s)| s)
            else {
                break;
            };
            cost.control += 1;
            cost.rows += 1;
            bound = key;
            for (m, s) in shards.iter().enumerate() {
                let now = sentinel(s, bound);
                if now != sentinels[m] {
                    sentinels[m] = now;
                    if m == j {
                        cost.keys += 1;
                    }
                }
            }
            count += 1;
        }
        // The window the client derives is the global top k.
        let mut expect: Vec<i64> = shards.iter().flatten().copied().collect();
        expect.sort_unstable_by(|a, b| b.cmp(a));
        expect.truncate(k);
        let mut got: Vec<i64> = shards
            .iter()
            .flat_map(|s| s.range(bound..))
            .copied()
            .collect();
        got.sort_unstable_by(|a, b| b.cmp(a));
        got.truncate(k);
        assert_eq!(got, expect);
        cost.held += in_window(shards, bound);
        cost.checkpoints += 1;
    }
    cost
}

fn live_ops(
    n: usize,
    rows: usize,
    ops: usize,
    workload: Workload,
    seed: u64,
) -> (Vec<BTreeSet<i64>>, Vec<(usize, i64, bool)>) {
    let mut rng = Rng(seed);
    let mut shards = vec![BTreeSet::new(); n];
    let mut next_key = 0i64;
    let mut placed: Vec<(usize, i64)> = Vec::new();
    for _ in 0..rows {
        let key = (rng.next() >> 20) as i64;
        let i = rng.below(n);
        shards[i].insert(key);
        placed.push((i, key));
        next_key = next_key.max(key);
    }
    let mut out = Vec::new();
    let mut live = shards.clone();
    for _ in 0..ops {
        if rng.unit() < 0.5 || placed.len() < 2 {
            let i = rng.below(n);
            let key = match workload {
                Workload::Random => (rng.next() >> 20) as i64,
                Workload::Feed => {
                    next_key += 1 + rng.below(4) as i64;
                    next_key
                }
            };
            if live[i].insert(key) {
                placed.push((i, key));
                out.push((i, key, true));
            }
        } else {
            let idx = match workload {
                // A third of feed deletes hit one of the newest 20 rows.
                Workload::Feed if rng.below(3) == 0 => {
                    placed.len() - 1 - rng.below(20.min(placed.len()))
                }
                _ => rng.below(placed.len()),
            };
            let (i, key) = placed.swap_remove(idx);
            live[i].remove(&key);
            out.push((i, key, false));
        }
    }
    (shards, out)
}

#[test]
#[ignore = "benchmark; run explicitly"]
fn b2b_live_windows() {
    const ROWS: usize = 20_000;
    const OPS: usize = 2_000;
    println!(
        "| shards | workload | k | strategy | rows shipped | keys | control msgs | avg rows held |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- | --- |");
    for &n in &[2usize, 4, 8, 16] {
        for workload in [Workload::Random, Workload::Feed] {
            for &k in &[20usize, 100] {
                let (shards, ops) = live_ops(n, ROWS, OPS, workload, 0xfeed + n as u64);
                let naive = live_naive(&mut shards.clone(), k, &ops);
                let row = |name: &str, c: &LiveCost| {
                    println!(
                        "| {n} | {workload:?} | {k} | {name} | {} | {} | {} | {:.0} |",
                        c.rows,
                        c.keys,
                        c.control,
                        c.held as f64 / c.checkpoints.max(1) as f64
                    )
                };
                row("naive per-shard top-k", &naive);
                for slack in [0usize, k / 10] {
                    let bound = live_bound(&mut shards.clone(), k, slack, &ops);
                    row(&format!("bound, slack {slack}"), &bound);
                }
            }
        }
    }
}

// ---- B3/B4: cross-shard commits (discrete-event model) ----------------------
//
// Shards hold versioned keys; clients in three regions run transactions
// through their region's router. Times are virtual microseconds from the
// RTT matrix above (one-way = RTT / 2), plus a fixed apply cost per shard
// step. The model measures the protocol: round trips, aborts under
// contention, and how long writers stall behind prepared transactions.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

const APPLY_US: u64 = 200;

fn one_way(a: Region, b: Region) -> u64 {
    (rtt(a, b) * 500.0) as u64
}

#[derive(Clone, Debug)]
struct Tx {
    id: usize,
    region: Region,
    serializable: bool,
    /// (shard, key, version seen at read time)
    reads: Vec<(usize, u32, u64)>,
    writes: Vec<(usize, u32)>,
    start: u64,
}

impl Tx {
    fn shards(&self) -> Vec<usize> {
        let mut s: Vec<usize> = self
            .reads
            .iter()
            .map(|r| r.0)
            .chain(self.writes.iter().map(|w| w.0))
            .collect();
        s.sort_unstable();
        s.dedup();
        s
    }
}

#[derive(Debug)]
enum Ev {
    /// A participant receives its prepare (or, single-shard, the commit).
    Prepare { tx: usize, shard: usize },
    /// The router has a vote.
    Vote { tx: usize, yes: bool },
    /// A participant learns the decision.
    Decide {
        tx: usize,
        shard: usize,
        commit: bool,
    },
    /// A mergeable write reaches its shard.
    Mergeable { tx: usize, shard: usize },
}

#[derive(Default)]
struct ShardState {
    versions: HashMap<u32, u64>,
    /// key -> prepared tx holding a guard on it
    guards: HashMap<u32, usize>,
    /// Mergeable writes waiting for a guard: (tx, arrival time)
    waiting: Vec<(usize, u64)>,
}

#[derive(Default, Debug)]
struct CommitStats {
    latencies: Vec<u64>,
    aborts: usize,
    commits: usize,
    stalls: Vec<u64>,
    mergeable_latencies: Vec<u64>,
    cross_latencies: Vec<u64>,
}

fn pct(v: &mut [u64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p) as usize] as f64 / 1000.0
}

struct Workload2 {
    shards: usize,
    keys_per_shard: u32,
    hot_keys: u32,
    hot_share: f64,
    cross_share: f64,
    serializable_share: f64,
    txs: usize,
    /// Mean gap between transaction starts, µs.
    gap_us: u64,
    /// Time between a transaction's reads and its commit, µs.
    think_us: u64,
}

fn run_commits(w: &Workload2, seed: u64) -> CommitStats {
    let mut rng = Rng(seed);
    let shard_region = |s: usize| REGIONS[s % REGIONS.len()];
    let mut shards: Vec<ShardState> = (0..w.shards).map(|_| ShardState::default()).collect();
    let mut stats = CommitStats::default();
    let mut events: BinaryHeap<Reverse<(u64, usize, usize)>> = BinaryHeap::new();
    let mut payloads: Vec<Ev> = Vec::new();
    let push = |events: &mut BinaryHeap<Reverse<(u64, usize, usize)>>,
                payloads: &mut Vec<Ev>,
                t: u64,
                ev: Ev| {
        payloads.push(ev);
        events.push(Reverse((t, payloads.len() - 1, 0)));
    };
    let mut txs: Vec<Tx> = Vec::new();
    let mut votes: HashMap<usize, (usize, bool)> = HashMap::new();
    let pick_key = |rng: &mut Rng| -> u32 {
        if rng.unit() < w.hot_share {
            rng.below(w.hot_keys as usize) as u32
        } else {
            w.hot_keys + rng.below((w.keys_per_shard - w.hot_keys) as usize) as u32
        }
    };
    // Generate transactions up front; reads see versions at read time, so
    // record the read time and resolve versions when the reads "happen".
    let mut t = 0u64;
    let mut planned = Vec::new();
    for id in 0..w.txs {
        t += 1 + rng.below((2 * w.gap_us) as usize) as u64;
        let region = REGIONS[rng.below(REGIONS.len())];
        // The client's home shard is one in its region.
        let home_candidates: Vec<usize> = (0..w.shards)
            .filter(|&s| shard_region(s) == region)
            .collect();
        let home = home_candidates[rng.below(home_candidates.len())];
        let cross = rng.unit() < w.cross_share && w.shards > 1;
        let other = loop {
            let s = rng.below(w.shards);
            if s != home || w.shards == 1 {
                break s;
            }
        };
        let serializable = rng.unit() < w.serializable_share;
        let mut reads = Vec::new();
        let mut writes = Vec::new();
        for (i, shard) in [home, other].into_iter().enumerate() {
            if i == 1 && !cross {
                break;
            }
            let k = pick_key(&mut rng);
            if serializable {
                reads.push((shard, k, 0));
            }
            writes.push((shard, k));
        }
        planned.push(Tx {
            id,
            region,
            serializable,
            reads,
            writes,
            start: t,
        });
    }
    // Reads happen at `start`; the commit leaves after `think_us`. Resolve
    // reads lazily by processing starts in time order alongside events.
    let mut next_start = 0usize;
    loop {
        let next_event_t = events.peek().map(|Reverse((t, _, _))| *t);
        let start_t = planned.get(next_start).map(|tx: &Tx| tx.start);
        let take_start = match (start_t, next_event_t) {
            (Some(s), Some(e)) => s <= e,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_start {
            let mut tx = planned[next_start].clone();
            next_start += 1;
            // Reads: the client's replica is assumed fresh at read time
            // (a cold read would add a round trip; it does not change
            // the commit protocol).
            for r in &mut tx.reads {
                r.2 = *shards[r.0].versions.get(&r.1).unwrap_or(&0);
            }
            let commit_at = tx.start + w.think_us;
            let parts = tx.shards();
            for &s in &parts {
                let arrive = commit_at + one_way(tx.region, shard_region(s));
                if tx.serializable {
                    push(
                        &mut events,
                        &mut payloads,
                        arrive,
                        Ev::Prepare {
                            tx: tx.id,
                            shard: s,
                        },
                    );
                } else {
                    push(
                        &mut events,
                        &mut payloads,
                        arrive,
                        Ev::Mergeable {
                            tx: tx.id,
                            shard: s,
                        },
                    );
                }
            }
            votes.insert(tx.id, (parts.len(), true));
            txs.push(tx);
            continue;
        }
        let Reverse((now, idx, _)) = events.pop().unwrap();
        let ev = std::mem::replace(
            &mut payloads[idx],
            Ev::Vote {
                tx: usize::MAX,
                yes: true,
            },
        );
        match ev {
            Ev::Prepare { tx, shard } => {
                let t = &txs[tx];
                let st = &mut shards[shard];
                let reads_ok = t
                    .reads
                    .iter()
                    .filter(|r| r.0 == shard)
                    .all(|r| st.versions.get(&r.1).copied().unwrap_or(0) == r.2);
                let keys: Vec<u32> = t
                    .reads
                    .iter()
                    .filter(|r| r.0 == shard)
                    .map(|r| r.1)
                    .chain(t.writes.iter().filter(|w| w.0 == shard).map(|w| w.1))
                    .collect();
                let guarded = keys
                    .iter()
                    .any(|k| st.guards.get(k).is_some_and(|&o| o != tx));
                let yes = reads_ok && !guarded;
                let single = t.shards().len() == 1;
                if yes {
                    if single {
                        // One shard validates and applies at once.
                        for w in t.writes.iter().filter(|w| w.0 == shard) {
                            *st.versions.entry(w.1).or_default() += 1;
                        }
                    } else {
                        for k in keys {
                            st.guards.insert(k, tx);
                        }
                    }
                }
                let back = now + APPLY_US + one_way(shard_region(shard), t.region);
                push(&mut events, &mut payloads, back, Ev::Vote { tx, yes });
            }
            Ev::Vote { tx, yes } => {
                if tx == usize::MAX {
                    continue;
                }
                let entry = votes.get_mut(&tx).unwrap();
                entry.0 -= 1;
                entry.1 &= yes;
                if entry.0 == 0 {
                    let commit = entry.1;
                    let t = &txs[tx];
                    let commit_at = t.start + w.think_us;
                    if !t.serializable {
                        stats.mergeable_latencies.push(now - commit_at);
                    } else if commit {
                        stats.commits += 1;
                        if t.shards().len() > 1 {
                            stats.cross_latencies.push(now - commit_at);
                        } else {
                            stats.latencies.push(now - commit_at);
                        }
                    } else {
                        stats.aborts += 1;
                    }
                    if t.shards().len() > 1 {
                        for s in t.shards() {
                            // The router tells participants; the
                            // coordinator's record is written in parallel.
                            let at = now + one_way(t.region, shard_region(s));
                            push(
                                &mut events,
                                &mut payloads,
                                at,
                                Ev::Decide {
                                    tx,
                                    shard: s,
                                    commit,
                                },
                            );
                        }
                    }
                }
            }
            Ev::Decide { tx, shard, commit } => {
                let t = &txs[tx];
                let st = &mut shards[shard];
                if commit {
                    for w in t.writes.iter().filter(|w| w.0 == shard) {
                        *st.versions.entry(w.1).or_default() += 1;
                    }
                }
                st.guards.retain(|_, holder| *holder != tx);
                // Release mergeable writes that waited on this guard.
                let waiting = std::mem::take(&mut st.waiting);
                for (m, arrived) in waiting {
                    stats.stalls.push(now - arrived);
                    push(
                        &mut events,
                        &mut payloads,
                        now,
                        Ev::Mergeable { tx: m, shard },
                    );
                    let _ = arrived;
                }
            }
            Ev::Mergeable { tx, shard } => {
                let t = &txs[tx];
                let st = &mut shards[shard];
                let blocked = t
                    .writes
                    .iter()
                    .filter(|w| w.0 == shard)
                    .any(|w| st.guards.contains_key(&w.1));
                if blocked {
                    st.waiting.push((tx, now));
                    continue;
                }
                for w in t.writes.iter().filter(|w| w.0 == shard) {
                    *st.versions.entry(w.1).or_default() += 1;
                }
                let back = now + APPLY_US + one_way(shard_region(shard), t.region);
                push(&mut events, &mut payloads, back, Ev::Vote { tx, yes: true });
            }
        }
    }
    stats
}

#[test]
#[ignore = "benchmark; run explicitly"]
fn b3_b4_cross_shard_commits() {
    println!(
        "| shards | cross-shard share | hot share | mergeable p50/p99 ms | serializable 1-shard p50/p99 ms | serializable cross-shard p50/p99 ms | abort % | writer stalls (count, p50 ms, p99 ms) |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- | --- |");
    for &shards in &[3usize, 9] {
        for &cross in &[0.0, 0.1, 0.5] {
            for &hot in &[0.0, 0.5, 0.9] {
                let w = Workload2 {
                    shards,
                    keys_per_shard: 10_000,
                    hot_keys: 10,
                    hot_share: hot,
                    cross_share: cross,
                    serializable_share: 0.3,
                    txs: 20_000,
                    gap_us: 1_000,
                    think_us: 50_000,
                };
                let mut s = run_commits(&w, 0xbeef + shards as u64);
                let total = (s.commits + s.aborts).max(1);
                let stalls = s.stalls.len();
                println!(
                    "| {shards} | {cross} | {hot} | {:.0}/{:.0} | {:.0}/{:.0} | {:.0}/{:.0} | {:.1} | {stalls}, {:.0}, {:.0} |",
                    pct(&mut s.mergeable_latencies, 0.5),
                    pct(&mut s.mergeable_latencies, 0.99),
                    pct(&mut s.latencies, 0.5),
                    pct(&mut s.latencies, 0.99),
                    pct(&mut s.cross_latencies, 0.5),
                    pct(&mut s.cross_latencies, 0.99),
                    100.0 * s.aborts as f64 / total as f64,
                    pct(&mut s.stalls, 0.5),
                    pct(&mut s.stalls, 0.99),
                );
            }
        }
    }
}

// ---- E5: what geo-sharding buys under local-first ---------------------------
//
// Own writes are visible at once everywhere (local-first), so the latencies
// that placement changes are: a collaborator seeing a write, a serializable
// commit, and loading a room the device has not cached. Compared: one core
// region, one core plus regional relays (read caches), and geo shards that
// home each room in its members' majority region.

#[derive(Clone, Copy, Debug)]
enum Placement {
    Core,
    CoreRelays,
    GeoMajority,
    GeoCreator,
}

/// Hop processing at a server or relay, ms.
const HOP_MS: f64 = 1.0;
/// Share of cold room loads a regional relay answers from cache.
const RELAY_HIT: f64 = 0.9;

fn region_weighted(rng: &mut Rng, weights: [f64; 3]) -> Region {
    let x = rng.unit();
    if x < weights[0] {
        REGIONS[0]
    } else if x < weights[0] + weights[1] {
        REGIONS[1]
    } else {
        REGIONS[2]
    }
}

struct GeoOutcome {
    visible: Vec<f64>,
    serializable: Vec<f64>,
    cold: Vec<f64>,
    cross_pairs: f64,
}

fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}

/// Share of events that feel local (under 20 ms), in percent.
fn local(v: &[f64]) -> f64 {
    v.iter().filter(|x| **x < 20.0).count() as f64 * 100.0 / v.len() as f64
}

fn run_geo(
    placement: Placement,
    weights: [f64; 3],
    locality: f64,
    room_size: usize,
    seed: u64,
) -> GeoOutcome {
    let mut rng = Rng(seed);
    let rooms = 4_000;
    let mut out = GeoOutcome {
        visible: Vec::new(),
        serializable: Vec::new(),
        cold: Vec::new(),
        cross_pairs: 0.0,
    };
    let mut homes = Vec::new();
    for _ in 0..rooms {
        let community = region_weighted(&mut rng, weights);
        let members: Vec<Region> = (0..room_size)
            .map(|_| {
                if rng.unit() < locality {
                    community
                } else {
                    region_weighted(&mut rng, weights)
                }
            })
            .collect();
        let home = match placement {
            Placement::Core | Placement::CoreRelays => Region::UsEast,
            Placement::GeoCreator => members[0],
            Placement::GeoMajority => *REGIONS
                .iter()
                .max_by_key(|r| {
                    members.iter().filter(|m| m == r).count() * 4 + usize::from(**r == members[0])
                })
                .unwrap(),
        };
        homes.push(home);
        for (w, &writer) in members.iter().enumerate() {
            for (r, &reader) in members.iter().enumerate() {
                if w == r {
                    continue;
                }
                let relay = matches!(placement, Placement::CoreRelays);
                let extra = if relay {
                    2.0 * (rtt(writer, writer) / 2.0 + HOP_MS)
                } else {
                    0.0
                };
                out.visible
                    .push(rtt(writer, home) / 2.0 + rtt(home, reader) / 2.0 + HOP_MS + extra);
            }
            out.serializable.push(rtt(writer, home) + HOP_MS);
            let cold = if matches!(placement, Placement::CoreRelays) && rng.unit() < RELAY_HIT {
                rtt(writer, writer) + HOP_MS
            } else {
                rtt(writer, home) + HOP_MS
            };
            out.cold.push(cold);
        }
    }
    // A user's transaction spanning two random rooms of theirs: how often
    // their homes differ (rooms of a user share its community region with
    // probability `locality`).
    let mut cross = 0;
    let pairs = 20_000;
    for _ in 0..pairs {
        let a = rng.below(rooms);
        let b = rng.below(rooms);
        // Rooms of one user: same community with probability `locality`.
        let same_community = rng.unit() < locality;
        let differ = if same_community {
            false
        } else {
            homes[a] != homes[b]
        };
        cross += usize::from(differ);
    }
    out.cross_pairs = cross as f64 / pairs as f64;
    out
}

#[test]
#[ignore = "benchmark; run explicitly"]
fn e5_geo_value() {
    let scenarios: [(&str, [f64; 3], f64); 5] = [
        ("single-region app (all US)", [1.0, 0.0, 0.0], 1.0),
        ("regional communities (90% local)", [0.5, 0.3, 0.2], 0.9),
        ("mostly regional (70% local)", [0.5, 0.3, 0.2], 0.7),
        ("mixed (40% local)", [0.5, 0.3, 0.2], 0.4),
        ("fully global rooms", [0.5, 0.3, 0.2], 0.0),
    ];
    for room_size in [2, 8] {
        println!("\nroom size {room_size}");
        println!(
            "{:<34} {:<12} {:>20} {:>20} {:>20} {:>8}",
            "scenario",
            "placement",
            "visible mean/<20ms",
            "serial. mean/<20ms",
            "cold mean/<20ms",
            "x-shard"
        );
        for (name, weights, locality) in scenarios {
            for placement in [
                Placement::Core,
                Placement::CoreRelays,
                Placement::GeoCreator,
                Placement::GeoMajority,
            ] {
                let o = run_geo(placement, weights, locality, room_size, 7);
                println!(
                    "{:<34} {:<12} {:>8.0} ms /{:>5.0}% {:>8.0} ms /{:>5.0}% {:>8.0} ms /{:>5.0}% {:>7.0}%",
                    name,
                    format!("{placement:?}"),
                    mean(&o.visible),
                    local(&o.visible),
                    mean(&o.serializable),
                    local(&o.serializable),
                    mean(&o.cold),
                    local(&o.cold),
                    o.cross_pairs * 100.0
                );
            }
        }
    }
}
