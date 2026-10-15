//! One memory budget for what the runtime keeps only to avoid recomputing:
//! the resident buckets of stored arrangements and the retained hydration
//! memo.
//!
//! A stored arrangement keeps the buckets it read resident, as a cache over
//! storage (see `stored_arrangement`), and the hydration memo keeps node
//! results for later hydrations to reuse. The budget bounds their estimated
//! bytes together: after each install, a runtime over budget evicts the
//! least recently read buckets and memo entries, which later reads fetch
//! from storage or recompute. Lowering the budget therefore trades memory
//! for work smoothly, with no switch between a held and a stored plan.
//!
//! The budget is process-wide, shared by every runtime in the process, so
//! that one figure (set from the process memory limit, and lowered under
//! memory pressure) bounds them all. A runtime can override it with a budget
//! of its own.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::*;

/// Orders reads of resident buckets across every stored arrangement.
static RESIDENCY_CLOCK: AtomicU64 = AtomicU64::new(1);
/// The process-wide budget in bytes; `usize::MAX` is unbounded.
static PROCESS_BUDGET: AtomicUsize = AtomicUsize::new(usize::MAX);
/// Resident bytes every runtime in the process reported at its last install.
static PROCESS_RESIDENT: AtomicUsize = AtomicUsize::new(0);

pub(super) fn residency_tick() -> u64 {
    RESIDENCY_CLOCK.fetch_add(1, Ordering::Relaxed)
}

/// Bound the estimated bytes the resident buckets of stored arrangements
/// keep, across every runtime in the process; `None` is unbounded (the
/// default). Runtimes evict down to it at their next install.
pub fn set_process_cache_budget(budget: Option<usize>) {
    PROCESS_BUDGET.store(budget.unwrap_or(usize::MAX), Ordering::Relaxed);
}

pub fn process_cache_budget() -> Option<usize> {
    match PROCESS_BUDGET.load(Ordering::Relaxed) {
        usize::MAX => None,
        budget => Some(budget),
    }
}

/// Estimated resident bytes of stored arrangements across the process, as
/// of each runtime's last install.
pub fn process_cache_resident_bytes() -> usize {
    PROCESS_RESIDENT.load(Ordering::Relaxed)
}

/// A runtime's share of `PROCESS_RESIDENT`, withdrawn when it is dropped.
#[derive(Debug, Default)]
pub(super) struct ResidentShare(usize);

impl ResidentShare {
    fn report(&mut self, bytes: usize) {
        if bytes >= self.0 {
            PROCESS_RESIDENT.fetch_add(bytes - self.0, Ordering::Relaxed);
        } else {
            PROCESS_RESIDENT.fetch_sub(self.0 - bytes, Ordering::Relaxed);
        }
        self.0 = bytes;
    }
}

/// A cloned runtime reports its own share at its first install.
impl Clone for ResidentShare {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl Drop for ResidentShare {
    fn drop(&mut self) {
        self.report(0);
    }
}

impl IvmRuntime {
    /// Give this runtime a budget of its own instead of the process-wide
    /// one; `None` follows the process budget again. Takes effect at once.
    pub fn set_cache_memory_budget(&mut self, budget: Option<usize>) {
        self.cache_budget = budget;
        self.enforce_cache_budget();
    }

    pub fn cache_memory_budget(&self) -> Option<usize> {
        self.cache_budget.or_else(process_cache_budget)
    }

    /// Whether reads must record the buckets they use: only a finite budget
    /// evicts by them.
    pub(super) fn arrangement_use_tracked(&self) -> bool {
        self.cache_memory_budget().is_some()
    }

    /// Evict the least recently read resident buckets and memo entries
    /// until this runtime fits its own budget, or the process fits the
    /// process budget. Eviction stops some way under the budget so the next
    /// install does not evict again at once.
    pub(super) fn enforce_cache_budget(&mut self) {
        self.arrangement_use_tracking
            .store(self.arrangement_use_tracked(), Ordering::Relaxed);
        let resident: usize = self
            .arrangement_states
            .values()
            .map(|state| state.value().resident_bytes())
            .sum::<usize>()
            + self.eval_memo.values().map(memo_entry_bytes).sum::<usize>();
        // Bytes to free, and the process total they come out of.
        let excess = match (self.cache_budget, process_cache_budget()) {
            (Some(budget), _) => excess_over(resident, budget),
            (None, Some(budget)) => {
                let others = process_cache_resident_bytes().saturating_sub(self.resident_share.0);
                excess_over(others.saturating_add(resident), budget)
                    .map(|excess| excess.min(resident))
            }
            (None, None) => None,
        };
        let Some(excess) = excess else {
            self.resident_share.report(resident);
            return;
        };
        let buckets = self
            .arrangement_states
            .iter()
            .filter(|(_, state)| state.value().resident_bytes() > 0)
            .flat_map(|(arrangement, state)| {
                state
                    .value()
                    .resident_entries()
                    .map(move |(last_used, key, bytes)| {
                        (last_used, Cached::Bucket(arrangement, key), bytes)
                    })
            });
        let memos = self
            .eval_memo
            .iter()
            .map(|(key, entry)| (entry.last_used, Cached::Memo(key), memo_entry_bytes(entry)));
        let mut entries = buckets.chain(memos).collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(last_used, ..)| *last_used);
        let mut evict_buckets: HashMap<ArrangementKey, Vec<join::JoinKey>> = HashMap::default();
        let mut evict_memos = Vec::new();
        let mut freed = 0;
        for (_, cached, bytes) in entries {
            if freed >= excess {
                break;
            }
            freed += bytes;
            match cached {
                Cached::Bucket(arrangement, key) => {
                    evict_buckets
                        .entry(arrangement.clone())
                        .or_default()
                        .push(key);
                }
                Cached::Memo(key) => evict_memos.push(key.clone()),
            }
        }
        for (arrangement, keys) in evict_buckets {
            if let Some(state) = self.arrangement_states.get_mut(&arrangement) {
                state.value_mut().evict(&keys);
            }
        }
        for key in evict_memos {
            self.remove_retained_eval_memo(&key);
        }
        self.resident_share.report(resident.saturating_sub(freed));
    }
}

/// Something the budget can evict.
enum Cached<'a> {
    Bucket(&'a ArrangementKey, join::JoinKey),
    Memo(&'a EvalMemoKey),
}

/// Estimated bytes a retained memo entry holds: its encoded records, their
/// handles, and the entry itself.
fn memo_entry_bytes(entry: &EvalMemoEntry) -> usize {
    const ENTRY_OVERHEAD: usize = 128;
    entry.payload_bytes
        + entry.records.deltas.len() * std::mem::size_of::<RecordDelta>()
        + ENTRY_OVERHEAD
}

/// Bytes to free so `resident` lands a tenth under `budget`, or `None`
/// within budget.
fn excess_over(resident: usize, budget: usize) -> Option<usize> {
    (resident > budget).then(|| resident - (budget - budget / 10))
}
