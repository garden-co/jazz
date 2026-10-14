//! One memory budget for the resident buckets of stored arrangements.
//!
//! A stored arrangement keeps the buckets it read resident, as a cache over
//! storage (see `stored_arrangement`). The budget bounds those buckets'
//! estimated bytes: after each install, a runtime over budget evicts its
//! least recently read buckets, which later reads fetch from storage again.
//! Lowering the budget therefore trades memory for storage reads smoothly,
//! with no switch between a held and a stored plan.
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
static PROCESS_RESIDENT: AtomicUsize = AtomicUsize::new(usize::MAX);

pub(super) fn residency_tick() -> u64 {
    RESIDENCY_CLOCK.fetch_add(1, Ordering::Relaxed)
}

/// Bound the estimated bytes the resident buckets of stored arrangements
/// keep, across every runtime in the process; `None` is unbounded (the
/// default). Runtimes evict down to it at their next install.
pub fn set_process_arrangement_budget(budget: Option<usize>) {
    PROCESS_BUDGET.store(budget.unwrap_or(usize::MAX), Ordering::Relaxed);
}

pub fn process_arrangement_budget() -> Option<usize> {
    match PROCESS_BUDGET.load(Ordering::Relaxed) {
        usize::MAX => None,
        budget => Some(budget),
    }
}

/// Estimated resident bytes of stored arrangements across the process, as
/// of each runtime's last install.
pub fn process_arrangement_resident_bytes() -> usize {
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
    pub fn set_arrangement_memory_budget(&mut self, budget: Option<usize>) {
        self.arrangement_budget = budget;
        self.enforce_arrangement_budget();
    }

    pub fn arrangement_memory_budget(&self) -> Option<usize> {
        self.arrangement_budget.or_else(process_arrangement_budget)
    }

    /// Whether reads must record the buckets they use: only a finite budget
    /// evicts by them.
    pub(super) fn arrangement_use_tracked(&self) -> bool {
        self.arrangement_memory_budget().is_some()
    }

    /// Evict the least recently read resident buckets until this runtime
    /// fits its own budget, or the process fits the process budget. Eviction
    /// stops some way under the budget so the next install does not evict
    /// again at once.
    pub(super) fn enforce_arrangement_budget(&mut self) {
        self.arrangement_use_tracking
            .store(self.arrangement_use_tracked(), Ordering::Relaxed);
        let resident: usize = self
            .arrangement_states
            .values()
            .map(|state| state.value().resident_bytes())
            .sum();
        // Bytes to free, and the process total they come out of.
        let excess = match (self.arrangement_budget, process_arrangement_budget()) {
            (Some(budget), _) => excess_over(resident, budget),
            (None, Some(budget)) => {
                let others =
                    process_arrangement_resident_bytes().saturating_sub(self.resident_share.0);
                excess_over(others + resident, budget).map(|excess| excess.min(resident))
            }
            (None, None) => None,
        };
        let Some(excess) = excess else {
            self.resident_share.report(resident);
            return;
        };
        let mut entries = self
            .arrangement_states
            .iter()
            .filter(|(_, state)| state.value().resident_bytes() > 0)
            .flat_map(|(arrangement, state)| {
                state
                    .value()
                    .resident_entries()
                    .map(move |(last_used, key, bytes)| (last_used, arrangement, key, bytes))
            })
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(last_used, ..)| *last_used);
        let mut evict: HashMap<ArrangementKey, Vec<join::JoinKey>> = HashMap::default();
        let mut freed = 0;
        for (_, arrangement, key, bytes) in entries {
            if freed >= excess {
                break;
            }
            freed += bytes;
            evict.entry(arrangement.clone()).or_default().push(key);
        }
        for (arrangement, keys) in evict {
            if let Some(state) = self.arrangement_states.get_mut(&arrangement) {
                state.value_mut().evict(&keys);
            }
        }
        self.resident_share.report(resident.saturating_sub(freed));
    }
}

/// Bytes to free so `resident` lands a tenth under `budget`, or `None`
/// within budget.
fn excess_over(resident: usize, budget: usize) -> Option<usize> {
    (resident > budget).then(|| resident - (budget - budget / 10))
}
