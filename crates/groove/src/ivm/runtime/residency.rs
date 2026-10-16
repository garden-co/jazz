//! One memory budget for the resident buckets of stored arrangements.
//!
//! A stored arrangement keeps the buckets it read resident, as a cache over
//! storage (see `stored_arrangement`). The budget bounds their estimated
//! bytes: after each install, a runtime over budget evicts the least
//! recently read buckets, which later reads fetch from storage again.
//! Lowering the budget therefore trades memory for work smoothly, with no
//! switch between a held and a stored plan.
//!
//! The retained hydration memo stays outside the budget. It holds the
//! results every hydration of a shared graph reads, and evicting them costs
//! a full recomputation of the shared path per subscribe (measured 15x
//! slower at a 32 MB budget) without lowering RSS.
//!
//! The budget is process-wide, shared by every runtime in the process, so
//! that one figure (set from the process memory limit, and lowered under
//! memory pressure) bounds them all; each runtime keeps a share of it in
//! proportion to what it holds. A runtime can override it with a budget of
//! its own.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use super::*;

/// Whether runtimes created from now on read arrangements of stored tables
/// from storage by key. Off by default: a server turns it on for its
/// process, while clients keep holding their arrangements.
static STORED_ARRANGEMENTS: AtomicBool = AtomicBool::new(false);
/// Buckets evicted across the process since it started.
static EVICTED_BUCKETS: AtomicU64 = AtomicU64::new(0);

/// Have runtimes created from now on read arrangements of stored tables
/// from storage by key (see `stored_arrangement`); runtimes already running
/// keep their setting.
pub fn set_process_stored_arrangements(enabled: bool) {
    STORED_ARRANGEMENTS.store(enabled, Ordering::Relaxed);
}

pub fn process_stored_arrangements() -> bool {
    STORED_ARRANGEMENTS.load(Ordering::Relaxed)
}

/// Resident buckets evicted across the process since it started; each is
/// read from storage again by its next read.
pub fn process_cache_evicted_buckets() -> u64 {
    EVICTED_BUCKETS.load(Ordering::Relaxed)
}

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

    /// Evict the least recently read resident buckets until this runtime fits its own budget, or the process fits the
    /// process budget. Eviction stops some way under the budget so the next
    /// install does not evict again at once.
    pub(super) fn enforce_cache_budget(&mut self) {
        self.arrangement_use_tracking
            .store(self.arrangement_use_tracked(), Ordering::Relaxed);
        let resident: usize = self
            .arrangement_states
            .values()
            .map(|state| state.value().resident_bytes())
            .sum::<usize>();
        // Bytes to free, and the process total they come out of.
        let excess = match (self.cache_budget, process_cache_budget()) {
            (Some(budget), _) => excess_over(resident, budget),
            (None, Some(budget)) => {
                // Over the process budget, each runtime keeps its share of it
                // in proportion to what it holds, so a busy runtime does not
                // evict everything to pay for idle ones, which shrink to
                // their own share at their next install.
                let others = process_cache_resident_bytes().saturating_sub(self.resident_share.0);
                let total = others.saturating_add(resident);
                (total > budget)
                    .then(|| fair_share(budget, resident, total))
                    .and_then(|share| excess_over(resident, share))
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
                    .map(move |(last_used, key, bytes)| (last_used, arrangement, key, bytes))
            });
        let mut entries = buckets.collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(last_used, ..)| *last_used);
        let mut evict: HashMap<ArrangementKey, Vec<join::JoinKey>> = HashMap::default();
        let mut freed = 0;
        for (_, arrangement, key, bytes) in entries {
            if freed >= excess {
                break;
            }
            freed += bytes;
            evict
                .entry(arrangement.clone())
                .or_default()
                .push(key.clone());
        }
        for (arrangement, keys) in evict {
            if let Some(state) = self.arrangement_states.get_mut(&arrangement) {
                EVICTED_BUCKETS.fetch_add(keys.len() as u64, Ordering::Relaxed);
                state.value_mut().evict(&keys);
            }
        }
        self.resident_share.report(resident.saturating_sub(freed));
    }
}

/// The part of `budget` a runtime holding `resident` of `total` bytes keeps.
fn fair_share(budget: usize, resident: usize, total: usize) -> usize {
    let share = budget as u128 * resident as u128 / total.max(1) as u128;
    usize::try_from(share).unwrap_or(usize::MAX)
}

/// Bytes to free so `resident` lands a tenth under `budget`, or `None`
/// within budget.
fn excess_over(resident: usize, budget: usize) -> Option<usize> {
    (resident > budget).then(|| resident - (budget - budget / 10))
}

#[cfg(test)]
mod tests {
    use super::{excess_over, fair_share};

    #[test]
    fn a_runtime_keeps_its_proportional_share_of_the_process_budget() {
        // A busy runtime holding 20 of 220 bytes keeps 20/220 of a 110 byte
        // budget, not nothing to pay for an idle runtime's 200.
        assert_eq!(fair_share(110, 20, 220), 10);
        assert_eq!(excess_over(20, fair_share(110, 20, 220)), Some(11));
        assert_eq!(fair_share(110, 200, 220), 100);
        assert_eq!(fair_share(0, 20, 220), 0);
        assert_eq!(fair_share(usize::MAX - 1, 1, 1), usize::MAX - 1);
    }
}
