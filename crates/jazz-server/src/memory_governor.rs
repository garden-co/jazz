//! Keeps the server under its memory limit by spilling query state to disk.
//!
//! The kernel kills a container that crosses its memory limit, failing every
//! client at once. Instead, this loop watches the process's anonymous memory
//! and, once it passes a high-water mark, asks the runtime to move query join
//! state to scratch storage until usage is back under a low-water mark.
//! Queries keep returning the same results, reading spilled state back from
//! disk, so the server trades speed for memory rather than dying.

use std::sync::Arc;
use std::time::Duration;

use crate::server::ServerState;

/// Share of the memory limit above which the governor sheds memory.
const HIGH_WATER_PERCENT: u64 = 70;
/// Share of the memory limit the governor sheds down to.
const LOW_WATER_PERCENT: u64 = 60;
const CHECK_INTERVAL: Duration = Duration::from_secs(2);

/// Memory the governor holds the process to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryWatermarks {
    pub high_bytes: u64,
    pub low_bytes: u64,
}

impl MemoryWatermarks {
    pub fn for_limit(limit_bytes: u64) -> Self {
        Self {
            high_bytes: limit_bytes / 100 * HIGH_WATER_PERCENT,
            low_bytes: limit_bytes / 100 * LOW_WATER_PERCENT,
        }
    }

    /// Bytes to shed at `used_bytes`, or `None` while under the high-water
    /// mark.
    pub fn excess(&self, used_bytes: u64) -> Option<u64> {
        (used_bytes > self.high_bytes).then(|| used_bytes - self.low_bytes)
    }
}

/// Watch memory until the server shuts down, spilling query state whenever
/// it crosses the high-water mark.
pub async fn run(state: Arc<ServerState>, limit_bytes: u64) {
    let watermarks = MemoryWatermarks::for_limit(limit_bytes);
    let mut interval = tokio::time::interval(CHECK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let Some(used) = anonymous_memory_bytes() else {
            tracing::warn!("memory governor cannot read process memory; stopping");
            return;
        };
        let Some(excess) = watermarks.excess(used) else {
            continue;
        };
        let Some(runtime) = state.runtime() else {
            continue;
        };
        let excess = usize::try_from(excess).unwrap_or(usize::MAX);
        match runtime.shed_arrangement_memory(excess).await {
            Ok(report) if report.spilled_arrangements > 0 => tracing::info!(
                used_mib = used / MIB,
                spilled_arrangements = report.spilled_arrangements,
                resident_before_mib = report.resident_bytes_before as u64 / MIB,
                resident_after_mib = report.resident_bytes_after as u64 / MIB,
                "memory above high-water mark; spilled query state to disk"
            ),
            Ok(_) => tracing::debug!(
                used_mib = used / MIB,
                "memory above high-water mark; no query state left to spill"
            ),
            Err(error) => tracing::warn!(%error, "spilling query state failed"),
        }
    }
}

const MIB: u64 = 1024 * 1024;

/// Anonymous resident memory of this process: heap and other memory the
/// kernel cannot reclaim by dropping file pages. Page cache counts toward a
/// cgroup's usage too, but the kernel reclaims it before it kills anything.
fn anonymous_memory_bytes() -> Option<u64> {
    parse_rss_anon(&std::fs::read_to_string("/proc/self/status").ok()?)
}

fn parse_rss_anon(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("RssAnon:"))?;
    let kib = line
        .trim_start_matches("RssAnon:")
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(kib * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sheds_down_to_the_low_water_mark_once_past_the_high_one() {
        let watermarks = MemoryWatermarks::for_limit(1000 * MIB);
        assert_eq!(watermarks.excess(600 * MIB), None);
        assert_eq!(watermarks.excess(700 * MIB), None);
        assert_eq!(watermarks.excess(750 * MIB), Some(150 * MIB));
    }

    #[test]
    fn reads_anonymous_rss_from_proc_status() {
        let status =
            "Name:\tjazz\nVmRSS:\t  204800 kB\nRssAnon:\t  102400 kB\nRssFile:\t 1024 kB\n";
        assert_eq!(parse_rss_anon(status), Some(100 * MIB));
        assert_eq!(parse_rss_anon("Name:\tjazz\n"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reads_this_process_memory() {
        assert!(anonymous_memory_bytes().is_some_and(|bytes| bytes > 0));
    }
}
