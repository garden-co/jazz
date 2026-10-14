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
