use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const REPORT_INTERVAL: Duration = Duration::from_secs(5);
const TOP_N: usize = 12;

#[derive(Default)]
struct Bucket {
    total: u64,
    keys: HashMap<String, u64>,
}

#[derive(Default)]
struct DurationBucket {
    total: u64,
    total_micros: u128,
    keys: HashMap<String, (u64, u128)>,
}

impl Bucket {
    fn increment(&mut self, key: String) {
        self.total += 1;
        *self.keys.entry(key).or_insert(0) += 1;
    }
}

impl DurationBucket {
    fn observe(&mut self, key: String, duration: Duration) {
        let micros = duration.as_micros();
        self.total += 1;
        self.total_micros += micros;
        let entry = self.keys.entry(key).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += micros;
    }
}

struct PolicyCounters {
    started_at: Instant,
    last_report: Instant,
    buckets: HashMap<&'static str, Bucket>,
    duration_buckets: HashMap<&'static str, DurationBucket>,
}

impl PolicyCounters {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            started_at: now,
            last_report: now,
            buckets: HashMap::new(),
            duration_buckets: HashMap::new(),
        }
    }
}

static COUNTERS: OnceLock<Mutex<PolicyCounters>> = OnceLock::new();

fn enabled() -> bool {
    std::env::var_os("JAZZ_POLICY_COUNTERS").is_some()
}

pub(crate) fn increment(bucket: &'static str, key: impl Into<String>) {
    if !enabled() {
        return;
    }

    let counters = COUNTERS.get_or_init(|| Mutex::new(PolicyCounters::new()));
    let Ok(mut counters) = counters.lock() else {
        return;
    };

    counters
        .buckets
        .entry(bucket)
        .or_default()
        .increment(key.into());

    if counters.last_report.elapsed() >= REPORT_INTERVAL {
        counters.last_report = Instant::now();
        print_report(&counters);
    }
}

pub(crate) fn observe_duration(bucket: &'static str, key: impl Into<String>, duration: Duration) {
    if !enabled() {
        return;
    }

    let counters = COUNTERS.get_or_init(|| Mutex::new(PolicyCounters::new()));
    let Ok(mut counters) = counters.lock() else {
        return;
    };

    counters
        .duration_buckets
        .entry(bucket)
        .or_default()
        .observe(key.into(), duration);

    if counters.last_report.elapsed() >= REPORT_INTERVAL {
        counters.last_report = Instant::now();
        print_report(&counters);
    }
}

fn print_report(counters: &PolicyCounters) {
    eprintln!(
        "[jazz-policy-counters] elapsed={:.1}s",
        counters.started_at.elapsed().as_secs_f64()
    );

    let mut bucket_names = counters.buckets.keys().copied().collect::<Vec<_>>();
    bucket_names.sort_unstable();

    for bucket_name in bucket_names {
        let Some(bucket) = counters.buckets.get(bucket_name) else {
            continue;
        };
        eprintln!(
            "[jazz-policy-counters] bucket={} total={} unique={} repeated={}",
            bucket_name,
            bucket.total,
            bucket.keys.len(),
            bucket.total.saturating_sub(bucket.keys.len() as u64)
        );

        let mut top = bucket.keys.iter().collect::<Vec<_>>();
        top.sort_by(|(left_key, left_count), (right_key, right_count)| {
            right_count
                .cmp(left_count)
                .then_with(|| left_key.cmp(right_key))
        });

        for (key, count) in top.into_iter().take(TOP_N) {
            eprintln!(
                "[jazz-policy-counters] bucket={} count={} key={}",
                bucket_name, count, key
            );
        }
    }

    let mut duration_bucket_names = counters
        .duration_buckets
        .keys()
        .copied()
        .collect::<Vec<_>>();
    duration_bucket_names.sort_unstable();

    for bucket_name in duration_bucket_names {
        let Some(bucket) = counters.duration_buckets.get(bucket_name) else {
            continue;
        };
        eprintln!(
            "[jazz-policy-counters] duration_bucket={} total={} unique={} total_ms={:.1}",
            bucket_name,
            bucket.total,
            bucket.keys.len(),
            bucket.total_micros as f64 / 1000.0
        );

        let mut top = bucket.keys.iter().collect::<Vec<_>>();
        top.sort_by(
            |(left_key, (_, left_micros)), (right_key, (_, right_micros))| {
                right_micros
                    .cmp(left_micros)
                    .then_with(|| left_key.cmp(right_key))
            },
        );

        for (key, (count, micros)) in top.into_iter().take(TOP_N) {
            eprintln!(
                "[jazz-policy-counters] duration_bucket={} count={} total_ms={:.1} avg_ms={:.1} key={}",
                bucket_name,
                count,
                *micros as f64 / 1000.0,
                *micros as f64 / (*count as f64 * 1000.0),
                key
            );
        }
    }
}
