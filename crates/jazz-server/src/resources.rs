//! Resource limits the server process sizes itself against.
//!
//! The server runs inside a container with a hard memory limit; crossing it
//! gets the process OOM-killed rather than any request failed. Fixed-size
//! consumers such as the storage cache are therefore sized from that limit
//! instead of from constants, so the same binary fits a 512 MiB pod and a
//! 16 GiB host.

use std::path::Path;

/// Share of the process memory limit given to the RocksDB block cache, which
/// also carries memtable reservations and index/filter blocks.
const ROCKSDB_CACHE_SHARE_PERCENT: u64 = 15;
const ROCKSDB_CACHE_MIN_BYTES: u64 = 32 * 1024 * 1024;
const ROCKSDB_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Share of the process memory limit the resident buckets of stored
/// arrangements may use while memory is plentiful.
const ARRANGEMENT_CEILING_SHARE_PERCENT: u64 = 25;
/// Resident set size the arrangement budget governor steers toward, as a
/// share of the process memory limit.
const RSS_TARGET_SHARE_PERCENT: u64 = 80;

/// Limits the server process applies to itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerResourceLimits {
    /// Live WebSocket connections admitted before upgrades get `503`.
    pub max_websocket_connections: usize,
    /// Memory the process may use, or `None` when nothing limits it.
    pub memory_limit_bytes: Option<u64>,
}

impl Default for ServerResourceLimits {
    fn default() -> Self {
        Self {
            max_websocket_connections: crate::server::DEFAULT_MAX_WEBSOCKET_CONNECTIONS,
            memory_limit_bytes: None,
        }
    }
}

impl ServerResourceLimits {
    /// The RocksDB budget for this process: a fixed share of the memory
    /// limit, clamped so small pods keep a usable cache and large hosts do
    /// not grow it past the measured default.
    pub fn rocksdb_memory_budget(&self) -> jazz_storage_rocksdb::RocksDbMemoryBudget {
        let default = jazz_storage_rocksdb::RocksDbMemoryBudget::default();
        let Some(limit) = self.memory_limit_bytes else {
            return default;
        };
        let block_cache_bytes = (limit / 100 * ROCKSDB_CACHE_SHARE_PERCENT)
            .clamp(ROCKSDB_CACHE_MIN_BYTES, ROCKSDB_CACHE_MAX_BYTES);
        let block_cache_bytes = usize::try_from(block_cache_bytes).unwrap_or(usize::MAX);
        jazz_storage_rocksdb::RocksDbMemoryBudget {
            block_cache_bytes,
            // Memtables are charged to the cache; keep them from crowding out
            // data blocks in a small one.
            write_buffer_bytes: default.write_buffer_bytes.min(block_cache_bytes / 4),
        }
    }
}

/// Steers the process-wide budget of stored arrangement buckets (see
/// `groove::ivm::set_process_arrangement_budget`) from the process's
/// resident set: over the target, the budget drops by the overshoot, so
/// queries read more buckets from storage; under it, the budget grows back
/// towards its ceiling a step at a time. The trade between memory and
/// storage reads is continuous, with no switch between modes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArrangementBudgetGovernor {
    target_rss_bytes: u64,
    ceiling_bytes: usize,
}

impl ServerResourceLimits {
    /// The governor for this process's limit, or `None` when nothing limits
    /// memory and arrangements keep every bucket they read.
    pub fn arrangement_budget_governor(&self) -> Option<ArrangementBudgetGovernor> {
        let limit = self.memory_limit_bytes?;
        Some(ArrangementBudgetGovernor {
            target_rss_bytes: limit / 100 * RSS_TARGET_SHARE_PERCENT,
            ceiling_bytes: usize::try_from(limit / 100 * ARRANGEMENT_CEILING_SHARE_PERCENT)
                .unwrap_or(usize::MAX),
        })
    }
}

impl ArrangementBudgetGovernor {
    /// The budget while memory is plentiful.
    pub fn ceiling_bytes(&self) -> usize {
        self.ceiling_bytes
    }

    /// The budget to set next, given the process's resident set, the
    /// arrangements' resident bytes, and the current budget.
    pub fn next_budget(&self, rss_bytes: u64, resident_bytes: usize, current: usize) -> usize {
        let to_usize = |bytes: u64| usize::try_from(bytes).unwrap_or(usize::MAX);
        if rss_bytes > self.target_rss_bytes {
            // Evicting the overshoot from the cache brings the process back
            // to its target, as far as the cache can.
            let overshoot = to_usize(rss_bytes - self.target_rss_bytes);
            return resident_bytes.saturating_sub(overshoot).min(current);
        }
        // Grow back a tenth of the ceiling per step, and no further than the
        // headroom, so reclaimed memory does not refill the cache at once.
        let headroom = to_usize(self.target_rss_bytes - rss_bytes);
        current
            .saturating_add(self.ceiling_bytes / 10)
            .min(resident_bytes.saturating_add(headroom))
            .max(current)
            .min(self.ceiling_bytes)
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

    #[test]
    fn arrangement_budget_follows_memory_pressure_smoothly() {
        let limits = |limit: Option<u64>| ServerResourceLimits {
            memory_limit_bytes: limit,
            ..ServerResourceLimits::default()
        };
        assert_eq!(limits(None).arrangement_budget_governor(), None);
        let governor = limits(Some(1000 * MIB))
            .arrangement_budget_governor()
            .unwrap();
        let ceiling = governor.ceiling_bytes();
        assert_eq!(ceiling as u64, 1000 * MIB / 100 * 25);
        let target = 1000 * MIB / 100 * 80;
        let mib = MIB as usize;

        // Plenty of room: the budget stays at its ceiling.
        assert_eq!(governor.next_budget(100 * MIB, 50 * mib, ceiling), ceiling);
        // Over the target, the budget drops by the overshoot below what the
        // arrangements hold, and to nothing when they cannot cover it.
        assert_eq!(
            governor.next_budget(target + 30 * MIB, 200 * mib, ceiling),
            170 * mib
        );
        assert_eq!(
            governor.next_budget(target + 300 * MIB, 200 * mib, ceiling),
            0
        );
        // A higher resident set never raises a lowered budget.
        assert_eq!(
            governor.next_budget(target + 10 * MIB, 200 * mib, 100 * mib),
            100 * mib
        );
        // Back under the target it grows a tenth of the ceiling per step,
        // within the headroom.
        assert_eq!(governor.next_budget(target - 500 * MIB, 0, 0), ceiling / 10);
        assert_eq!(
            governor.next_budget(target - 5 * MIB, 0, 10 * mib),
            10 * mib
        );
        assert_eq!(
            governor.next_budget(target - 20 * MIB, 0, 10 * mib),
            20 * mib
        );
    }

    #[test]
    fn rocksdb_budget_scales_with_the_memory_limit() {
        let budget = |limit: Option<u64>| {
            ServerResourceLimits {
                memory_limit_bytes: limit,
                ..ServerResourceLimits::default()
            }
            .rocksdb_memory_budget()
        };

        assert_eq!(
            budget(None),
            jazz_storage_rocksdb::RocksDbMemoryBudget::default()
        );
        let one_gib = budget(Some(1024 * MIB));
        assert_eq!(one_gib.block_cache_bytes as u64, 1024 * MIB / 100 * 15);
        assert_eq!(one_gib.write_buffer_bytes as u64, 16 * MIB);

        let small = budget(Some(128 * MIB));
        assert_eq!(small.block_cache_bytes as u64, 32 * MIB);
        assert_eq!(small.write_buffer_bytes as u64, 8 * MIB);

        assert_eq!(
            budget(Some(64 * 1024 * MIB)).block_cache_bytes as u64,
            256 * MIB
        );
    }
}
