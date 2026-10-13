//! Keeps the server under its memory limit by spilling query state to disk.
//!
//! The kernel kills a container that crosses its memory limit, failing every
//! client at once. Instead, this loop watches the process's anonymous memory
//! and, once it passes a high-water mark, asks the runtime to move query join
//! state to scratch storage until usage is back under a low-water mark.
//! Queries keep returning the same results, reading spilled state back from
//! disk, so the server trades speed for memory rather than dying.

use std::path::{Path, PathBuf};
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

/// Longest wait between sheds that free nothing, so memory that is not
/// query state costs little scratch I/O.
const MAX_BACKOFF: Duration = Duration::from_secs(64);
/// A shed returning less than this did not help; back off before the next.
const USEFUL_SHED_BYTES: usize = 1024 * 1024;

/// Where and how much query state may spill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpillConfig {
    /// A fresh directory is created inside this one for the scratch store.
    pub parent_dir: PathBuf,
    /// Scratch bytes the server may hold at once.
    pub max_bytes: u64,
}

/// Watch memory until the server shuts down, spilling query state whenever
/// it crosses the high-water mark.
pub async fn run(state: Arc<ServerState>, limit_bytes: u64, spill: SpillConfig) {
    let watermarks = MemoryWatermarks::for_limit(limit_bytes);
    let mut delay = CHECK_INTERVAL;
    let mut spill_enabled = false;
    loop {
        tokio::time::sleep(delay).await;
        let Some(runtime) = state.runtime() else {
            continue;
        };
        if !spill_enabled {
            match enable_spill(&state, &runtime, &spill).await {
                Ok(()) => spill_enabled = true,
                Err(error) => {
                    tracing::warn!(%error, "cannot spill query state to disk; keeping it in memory");
                    return;
                }
            }
        }
        let Some(used) = anonymous_memory_bytes() else {
            tracing::warn!("memory governor cannot read process memory; stopping");
            return;
        };
        let Some(excess) = watermarks.excess(used) else {
            delay = CHECK_INTERVAL;
            continue;
        };
        let excess = usize::try_from(excess).unwrap_or(usize::MAX);
        let freed = match runtime.shed_arrangement_memory(excess).await {
            Ok(report) => {
                let freed = report
                    .resident_bytes_before
                    .saturating_sub(report.resident_bytes_after);
                if report.kept_at_disk_cap > 0 {
                    tracing::warn!(
                        kept_arrangements = report.kept_at_disk_cap,
                        max_spill_mib = spill.max_bytes / MIB,
                        "spill disk cap reached; keeping query state in memory"
                    );
                }
                tracing::info!(
                    used_mib = used / MIB,
                    spilled_arrangements = report.spilled_arrangements,
                    resident_before_mib = report.resident_bytes_before as u64 / MIB,
                    resident_after_mib = report.resident_bytes_after as u64 / MIB,
                    "memory above high-water mark; shed query state"
                );
                freed
            }
            Err(error) => {
                tracing::warn!(%error, "spilling query state failed");
                0
            }
        };
        delay = next_delay(delay, freed);
    }
}

/// The wait before the next check after a shed that freed `freed` bytes:
/// back to normal when it helped, doubling up to [`MAX_BACKOFF`] when it
/// did not, so pressure that query state does not cause stops costing
/// scratch I/O every few seconds.
fn next_delay(current: Duration, freed: usize) -> Duration {
    if freed >= USEFUL_SHED_BYTES {
        CHECK_INTERVAL
    } else {
        (current * 2).min(MAX_BACKOFF)
    }
}

pub(crate) async fn enable_spill(
    state: &ServerState,
    runtime: &jazz::serving::ServerRuntimeHandle,
    spill: &SpillConfig,
) -> Result<(), String> {
    let factory = state
        .storage_factory
        .clone()
        .ok_or_else(|| "server has no storage factory".to_owned())?;
    let dir = fresh_spill_dir(&spill.parent_dir)?;
    let max_bytes = usize::try_from(spill.max_bytes).unwrap_or(usize::MAX);
    if runtime
        .enable_arrangement_spill(factory, dir.clone(), max_bytes)
        .await?
    {
        tracing::info!(
            dir = %dir.display(),
            max_mib = spill.max_bytes / MIB,
            "query state spills to disk under memory pressure"
        );
        Ok(())
    } else {
        let _ = std::fs::remove_dir(&dir);
        Err("storage backend has no scratch storage".to_owned())
    }
}

/// Create a new, empty directory for this process's scratch store, so it
/// never reuses (and later deletes) a directory something else owns.
fn fresh_spill_dir(parent: &Path) -> Result<PathBuf, String> {
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("creating {}: {error}", parent.display()))?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let dir = parent.join(format!("jazz-spill-{}-{nanos}", std::process::id()));
    std::fs::create_dir(&dir).map_err(|error| format!("creating {}: {error}", dir.display()))?;
    Ok(dir)
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
    fn backs_off_while_shedding_frees_nothing() {
        let mut delay = CHECK_INTERVAL;
        for expected in [4, 8, 16, 32, 64, 64] {
            delay = next_delay(delay, 0);
            assert_eq!(delay, Duration::from_secs(expected));
        }
        assert_eq!(next_delay(delay, 8 * 1024 * 1024), CHECK_INTERVAL);
    }

    #[test]
    fn spill_dirs_are_fresh_and_inside_the_parent() {
        let parent = tempfile::tempdir().unwrap();
        let first = fresh_spill_dir(parent.path()).unwrap();
        let second = fresh_spill_dir(parent.path()).unwrap();
        assert_ne!(first, second);
        assert_eq!(first.parent(), Some(parent.path()));
        assert!(std::fs::read_dir(&first).unwrap().next().is_none());
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
