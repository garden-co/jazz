//! Resource limits the server process sizes itself against.
//!
//! The server runs inside a container with a hard memory limit; crossing it
//! gets the process OOM-killed rather than any request failed. The storage
//! cache is therefore sized from that limit instead of from constants, so the
//! same binary fits a 512 MiB pod and a 16 GiB host.

use std::path::Path;

/// Share of the process memory limit SQLite (its page caches, mostly) may
/// use while memory is plentiful.
const CACHE_CEILING_SHARE_PERCENT: u64 = 40;
/// Resident set size the cache budget governor steers toward, as a share of
/// the process memory limit.
const RSS_TARGET_SHARE_PERCENT: u64 = 80;

/// Limits the server process applies to itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerResourceLimits {
    /// Live WebSocket connections admitted before upgrades get `503`.
    pub max_websocket_connections: usize,
    /// Memory the process may use, or `None` when nothing limits it.
    pub memory_limit_bytes: Option<u64>,
    /// Accepted for older launch configurations; the SQLite store always
    /// reads stored tables by key.
    pub stored_arrangements: bool,
    /// The storage cache budget while memory is plentiful, instead of a
    /// share of the memory limit.
    pub query_cache_budget_bytes: Option<u64>,
}

impl Default for ServerResourceLimits {
    fn default() -> Self {
        Self {
            max_websocket_connections: crate::server::DEFAULT_MAX_WEBSOCKET_CONNECTIONS,
            memory_limit_bytes: None,
            stored_arrangements: true,
            query_cache_budget_bytes: None,
        }
    }
}

/// Steers the process-wide SQLite memory budget (see [`crate::storage_cache`])
/// from the process's resident set, trading memory for storage reads
/// continuously:
///
/// - Over the RSS target, each step lowers the budget by an eighth, never
///   below a floor, so queries read more pages from disk.
/// - Once SQLite has released memory down to a lowered budget, the
///   governor checks that RSS fell. If it did not, the memory is held
///   elsewhere and lowering further would only cost reads: the budget holds,
///   and grows back slowly while RSS stays flat, until RSS grows again.
/// - Under the target, the budget grows back a tenth of the ceiling per
///   step, within the headroom.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheBudgetGovernor {
    target_rss_bytes: u64,
    ceiling_bytes: usize,
    floor_bytes: usize,
    budget: usize,
    /// The RSS at the last lowering, until eviction catches up with it.
    lowered_at_rss: Option<u64>,
    /// The RSS at which lowering stopped moving RSS.
    unresponsive_at_rss: Option<u64>,
    /// Steps spent waiting for SQLite to release memory down to a lowered
    /// budget.
    waited_steps: u32,
}

/// Steps the governor waits for SQLite to release memory down to a lowered
/// budget before judging whether lowering helped.
const CACHE_EVICTION_WAIT_STEPS: u32 = 10;

/// The floor of the budget, as a fraction of its ceiling: the hot buckets
/// every query reads stay resident under any pressure.
const CACHE_FLOOR_DIVISOR: usize = 8;
/// The most one step lowers the budget by, as a fraction of the budget.
const CACHE_STEP_DOWN_DIVISOR: usize = 8;
/// What one step grows the budget by, as a fraction of the ceiling: faster
/// under the target, slower while probing whether RSS stays flat over it.
const CACHE_STEP_UP_DIVISOR: usize = 10;
const CACHE_PROBE_UP_DIVISOR: usize = 50;

impl ServerResourceLimits {
    /// The governor for this process's limit, or `None` when nothing limits
    /// memory and the budget stays where it was configured.
    pub fn cache_budget_governor(&self) -> Option<CacheBudgetGovernor> {
        let limit = self.memory_limit_bytes?;
        let ceiling = self.query_cache_ceiling_bytes()?;
        Some(CacheBudgetGovernor {
            target_rss_bytes: limit / 100 * RSS_TARGET_SHARE_PERCENT,
            ceiling_bytes: ceiling,
            floor_bytes: ceiling / CACHE_FLOOR_DIVISOR,
            budget: ceiling,
            lowered_at_rss: None,
            unresponsive_at_rss: None,
            waited_steps: 0,
        })
    }

    /// The query cache budget while memory is plentiful: the configured one,
    /// or a share of the memory limit; `None` is unbounded.
    pub fn query_cache_ceiling_bytes(&self) -> Option<usize> {
        let to_usize = |bytes: u64| usize::try_from(bytes).unwrap_or(usize::MAX);
        self.query_cache_budget_bytes
            .or_else(|| {
                self.memory_limit_bytes
                    .map(|limit| limit / 100 * CACHE_CEILING_SHARE_PERCENT)
            })
            .map(to_usize)
    }
}

impl CacheBudgetGovernor {
    /// The budget while memory is plentiful.
    pub fn ceiling_bytes(&self) -> usize {
        self.ceiling_bytes
    }

    /// The budget never goes below this.
    pub fn floor_bytes(&self) -> usize {
        self.floor_bytes
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget
    }

    /// Take one step given the process's resident set and SQLite's
    /// resident bytes; returns the budget to set.
    pub fn step(&mut self, rss_bytes: u64, resident_bytes: usize) -> usize {
        let to_usize = |bytes: u64| usize::try_from(bytes).unwrap_or(usize::MAX);
        // RSS moves by allocator noise between steps; changes within it are
        // flat.
        let noise = self.target_rss_bytes / 200;
        if rss_bytes <= self.target_rss_bytes {
            self.lowered_at_rss = None;
            self.unresponsive_at_rss = None;
            let headroom = to_usize(self.target_rss_bytes - rss_bytes);
            self.budget = self
                .budget
                .saturating_add(self.ceiling_bytes / CACHE_STEP_UP_DIVISOR)
                .min(resident_bytes.saturating_add(headroom))
                .clamp(self.budget, self.ceiling_bytes);
            return self.budget;
        }
        if let Some(at) = self.unresponsive_at_rss {
            if rss_bytes > at + noise {
                // Memory grew since lowering stopped helping: try again.
                self.unresponsive_at_rss = None;
            } else {
                // Lowering does not move RSS; hand reads back their cache
                // while RSS stays flat.
                self.budget = self
                    .budget
                    .saturating_add(self.ceiling_bytes / CACHE_PROBE_UP_DIVISOR)
                    .min(self.ceiling_bytes);
                return self.budget;
            }
        }
        if let Some(at) = self.lowered_at_rss {
            if resident_bytes > self.budget && self.waited_steps < CACHE_EVICTION_WAIT_STEPS {
                // Arrangements evict at their next install; judge after, or
                // after a while if an idle runtime never installs.
                self.waited_steps += 1;
                return self.budget;
            }
            if rss_bytes + noise >= at {
                self.lowered_at_rss = None;
                self.unresponsive_at_rss = Some(rss_bytes);
                return self.budget;
            }
        }
        // Lowering a budget SQLite does not fill frees nothing, so
        // start from what they hold.
        let from = self.budget.min(resident_bytes);
        let lowered = from
            .saturating_sub(from / CACHE_STEP_DOWN_DIVISOR)
            .max(self.floor_bytes);
        if lowered < self.budget {
            self.budget = lowered;
            self.lowered_at_rss = Some(rss_bytes);
            self.waited_steps = 0;
        } else {
            // At the floor and still over target: the rest of the memory is
            // held elsewhere, so treat it as lowering that did not help.
            self.unresponsive_at_rss = Some(rss_bytes);
        }
        self.budget
    }
}

/// This process's resident set size, where the platform reports it.
pub fn process_rss_bytes() -> Option<u64> {
    parse_vm_rss(&std::fs::read_to_string("/proc/self/status").ok()?)
}

fn parse_vm_rss(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line["VmRSS:".len()..]
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(kib * 1024)
}

/// The memory limit of the cgroup this process runs in, if it has one.
pub fn detect_memory_limit_bytes() -> Option<u64> {
    cgroup_memory_limit_bytes(Path::new("/sys/fs/cgroup"))
}

fn cgroup_memory_limit_bytes(root: &Path) -> Option<u64> {
    // cgroup v2 writes "max" for an unlimited group.
    if let Ok(raw) = std::fs::read_to_string(root.join("memory.max")) {
        return parse_limit(&raw);
    }
    // cgroup v1 reports an unlimited group as a huge page-rounded value.
    let raw = std::fs::read_to_string(root.join("memory/memory.limit_in_bytes")).ok()?;
    parse_limit(&raw).filter(|limit| *limit < (1 << 62))
}

fn parse_limit(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if raw == "max" {
        return None;
    }
    raw.parse::<u64>().ok().filter(|limit| *limit > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn reads_cgroup_v2_limit() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("memory.max"), "1073741824\n").unwrap();
        assert_eq!(cgroup_memory_limit_bytes(root.path()), Some(1024 * MIB));

        std::fs::write(root.path().join("memory.max"), "max\n").unwrap();
        assert_eq!(cgroup_memory_limit_bytes(root.path()), None);
    }

    #[test]
    fn reads_cgroup_v1_limit_and_treats_huge_values_as_unlimited() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("memory")).unwrap();
        let file = root.path().join("memory/memory.limit_in_bytes");
        std::fs::write(&file, "536870912\n").unwrap();
        assert_eq!(cgroup_memory_limit_bytes(root.path()), Some(512 * MIB));

        std::fs::write(&file, "9223372036854771712\n").unwrap();
        assert_eq!(cgroup_memory_limit_bytes(root.path()), None);
    }

    #[test]
    fn missing_cgroup_files_mean_no_limit() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(cgroup_memory_limit_bytes(root.path()), None);
    }

    #[test]
    fn reads_resident_set_from_proc_status() {
        let status = "Name:\tjazz\nVmPeak:\t  900 kB\nVmRSS:\t  2048 kB\nThreads:\t4\n";
        assert_eq!(parse_vm_rss(status), Some(2048 * 1024));
        assert_eq!(parse_vm_rss("Name:\tjazz\n"), None);
    }

    fn governor(limit_mib: u64) -> CacheBudgetGovernor {
        ServerResourceLimits {
            memory_limit_bytes: Some(limit_mib * MIB),
            ..ServerResourceLimits::default()
        }
        .cache_budget_governor()
        .unwrap()
    }

    #[test]
    fn cache_budget_ceiling_comes_from_the_limit_or_configuration() {
        let limits = |limit: Option<u64>, budget: Option<u64>| ServerResourceLimits {
            memory_limit_bytes: limit,
            query_cache_budget_bytes: budget,
            ..ServerResourceLimits::default()
        };
        assert_eq!(limits(None, None).cache_budget_governor(), None);
        assert_eq!(limits(None, None).query_cache_ceiling_bytes(), None);
        assert_eq!(
            limits(None, Some(64 * MIB)).query_cache_ceiling_bytes(),
            Some(64 * MIB as usize)
        );
        assert_eq!(limits(None, Some(64 * MIB)).cache_budget_governor(), None);
        let governor = governor(1000);
        assert_eq!(governor.ceiling_bytes() as u64, 1000 * MIB / 100 * 40);
        assert_eq!(governor.floor_bytes(), governor.ceiling_bytes() / 8);
        let configured = limits(Some(1000 * MIB), Some(64 * MIB))
            .cache_budget_governor()
            .unwrap();
        assert_eq!(configured.ceiling_bytes() as u64, 64 * MIB);
    }

    #[test]
    fn cache_budget_steps_down_smoothly_to_a_floor() {
        let mut governor = governor(1000);
        let ceiling = governor.ceiling_bytes();
        let target = 1000 * MIB / 100 * 80;
        // Plenty of room: the budget stays at its ceiling.
        assert_eq!(governor.step(100 * MIB, ceiling), ceiling);
        // Far over the target, each step lowers by an eighth as long as RSS
        // follows the evictions (here 10 MiB per step), and never below the
        // floor.
        let mut rss = target + 500 * MIB;
        let mut budgets = vec![ceiling];
        while budgets.last() != Some(&governor.floor_bytes()) {
            let budget = governor.step(rss, *budgets.last().unwrap());
            assert!(budget >= *budgets.last().unwrap() / 8 * 7, "{budgets:?}");
            budgets.push(budget);
            rss -= 10 * MIB;
            assert!(budgets.len() < 100, "{budgets:?}");
        }
        assert!(budgets.len() > 10, "{budgets:?}");
        assert_eq!(
            governor.step(rss, governor.floor_bytes()),
            governor.floor_bytes()
        );
    }

    #[test]
    fn cache_budget_stops_lowering_when_rss_does_not_follow() {
        let mut governor = governor(1000);
        let ceiling = governor.ceiling_bytes();
        let rss = 1000 * MIB / 100 * 85;
        // The first step lowers; SQLite releases memory down to
        // it, but RSS stays where it was.
        let lowered = governor.step(rss, ceiling);
        assert!(lowered < ceiling);
        // Before SQLite releases memory, the governor waits.
        assert_eq!(governor.step(rss, ceiling), lowered);
        // Evicted, RSS unmoved: the budget holds rather than collapsing.
        assert_eq!(governor.step(rss, lowered), lowered);
        // RSS stays flat: the budget grows back slowly towards the ceiling.
        let grown = governor.step(rss, lowered);
        assert!(grown > lowered && grown < ceiling);
        let mut budget = grown;
        for _ in 0..100 {
            budget = governor.step(rss, budget);
        }
        assert_eq!(budget, ceiling);
        // RSS grows again: lowering is tried again.
        assert!(governor.step(rss + 50 * MIB, ceiling) < ceiling);
    }

    #[test]
    fn cache_budget_does_not_stay_at_the_floor_when_rss_plateaus() {
        let mut governor = governor(1000);
        let ceiling = governor.ceiling_bytes();
        let target = 1000 * MIB / 100 * 80;
        // RSS climbs past the target and keeps following evictions down to
        // the floor, then plateaus above the target.
        let mut rss = target + 500 * MIB;
        let mut budget = ceiling;
        while budget > governor.floor_bytes() {
            budget = governor.step(rss, budget);
            rss -= 10 * MIB;
        }
        let plateau = rss;
        assert!(plateau > target);
        // At the floor, lowering can no longer help: the budget grows back
        // while RSS stays flat, rather than staying at the floor for good.
        assert_eq!(governor.step(plateau, budget), governor.floor_bytes());
        for _ in 0..200 {
            budget = governor.step(plateau, budget);
        }
        assert_eq!(budget, ceiling);
    }

    #[test]
    fn cache_budget_judges_lowering_when_eviction_never_catches_up() {
        // An idle runtime that never installs keeps its buckets resident.
        let mut governor = governor(1000);
        let ceiling = governor.ceiling_bytes();
        let rss = 1000 * MIB / 100 * 85;
        let lowered = governor.step(rss, ceiling);
        assert!(lowered < ceiling);
        for _ in 0..CACHE_EVICTION_WAIT_STEPS {
            assert_eq!(governor.step(rss, ceiling), lowered);
        }
        // RSS did not move: the budget holds, then grows back.
        assert_eq!(governor.step(rss, ceiling), lowered);
        assert!(governor.step(rss, ceiling) > lowered);
    }

    #[test]
    fn cache_budget_grows_back_within_the_headroom() {
        let mut governor = governor(1000);
        let ceiling = governor.ceiling_bytes();
        let target = 1000 * MIB / 100 * 80;
        let mut rss = target + 500 * MIB;
        let mut budget = ceiling;
        // Stay above the floor, where flat RSS would start probing back up.
        for _ in 0..12 {
            budget = governor.step(rss, budget);
            rss -= 10 * MIB;
        }
        let low = budget;
        assert!(low > governor.floor_bytes());
        assert!(low < ceiling / 2);
        // Back under the target it grows by at most a tenth of the ceiling
        // per step, and no further than the headroom.
        let mib = MIB as usize;
        assert_eq!(governor.step(target - 5 * MIB, low), low + 5 * mib);
        let next = governor.step(target - 500 * MIB, low + 5 * mib);
        assert_eq!(next, low + 5 * mib + ceiling / 10);
        for _ in 0..20 {
            budget = governor.step(target - 500 * MIB, budget);
        }
        assert_eq!(budget, ceiling);
    }
}
