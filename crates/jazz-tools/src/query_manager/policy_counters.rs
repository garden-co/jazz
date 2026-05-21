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

impl Bucket {
    fn increment(&mut self, key: String) {
        self.total += 1;
        *self.keys.entry(key).or_insert(0) += 1;
    }
}

struct PolicyCounters {
    started_at: Instant,
    last_report: Instant,
    buckets: HashMap<&'static str, Bucket>,
}

impl PolicyCounters {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            started_at: now,
            last_report: now,
            buckets: HashMap::new(),
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
}
