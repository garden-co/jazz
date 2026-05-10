//! [`DurabilityTracker`] — owns the per-batch ack-watcher map and the set of
//! rejected replayable batch ids that bindings still need to surface.
//!
//! Extracted from `RuntimeCore` so the orchestrator no longer carries the
//! bookkeeping for who-is-waiting-on-which-tier; it only delegates the
//! primitive operations (`register_watcher`, `record_ack`,
//! `record_rejection`, `drain_rejected`, `forget_batch`) defined here.

use std::collections::{BTreeSet, HashMap};

use futures::channel::oneshot;

use crate::row_histories::BatchId;
use crate::sync_manager::{DurabilityTier, RowBatchKey};

use super::{PersistedWriteAck, PersistedWriteRejection};

#[derive(Default)]
pub(crate) struct DurabilityTracker {
    /// Watchers waiting for a row+batch to be confirmed at a requested tier.
    ack_watchers: HashMap<RowBatchKey, Vec<(DurabilityTier, oneshot::Sender<PersistedWriteAck>)>>,
    /// Rejected replayable batch ids surfaced once to bindings on next drain.
    rejected_batch_ids: BTreeSet<BatchId>,
}

impl DurabilityTracker {
    /// Construct a tracker preloaded with rejection ids recovered from
    /// persisted local batch records on startup.
    pub(crate) fn with_initial_rejections(rejected: BTreeSet<BatchId>) -> Self {
        Self {
            ack_watchers: HashMap::new(),
            rejected_batch_ids: rejected,
        }
    }

    /// Register a watcher that resolves when `key` reaches `tier` (or higher).
    pub(crate) fn register_watcher(
        &mut self,
        key: RowBatchKey,
        tier: DurabilityTier,
        sender: oneshot::Sender<PersistedWriteAck>,
    ) {
        self.ack_watchers
            .entry(key)
            .or_default()
            .push((tier, sender));
    }

    /// Resolve every watcher on `key` whose requested tier is satisfied by
    /// `acked_tier`; keep the rest registered.
    pub(crate) fn record_ack(&mut self, key: RowBatchKey, acked_tier: DurabilityTier) {
        let Some(watchers) = self.ack_watchers.remove(&key) else {
            return;
        };
        let mut remaining = Vec::new();
        for (requested_tier, sender) in watchers {
            if acked_tier >= requested_tier {
                tracing::debug!(?key, ?acked_tier, ?requested_tier, "ack watcher resolved");
                let _ = sender.send(Ok(()));
            } else {
                remaining.push((requested_tier, sender));
            }
        }
        if !remaining.is_empty() {
            self.ack_watchers.insert(key, remaining);
        }
    }

    /// Resolve every watcher on `batch_id` whose requested tier is satisfied by
    /// `acked_tier`; keep the rest registered.
    pub(crate) fn record_batch_ack(&mut self, batch_id: BatchId, acked_tier: DurabilityTier) {
        let affected_keys: Vec<_> = self
            .ack_watchers
            .keys()
            .copied()
            .filter(|key| key.batch_id == batch_id)
            .collect();
        for key in affected_keys {
            self.record_ack(key, acked_tier);
        }
    }

    /// Mark `batch_id` rejected and notify every watcher whose key references
    /// that batch with the rejection details.
    pub(crate) fn record_rejection(&mut self, batch_id: BatchId, code: &str, reason: &str) {
        self.rejected_batch_ids.insert(batch_id);

        let rejection = PersistedWriteRejection {
            batch_id,
            code: code.to_string(),
            reason: reason.to_string(),
        };

        let affected_keys: Vec<_> = self
            .ack_watchers
            .keys()
            .copied()
            .filter(|key| key.batch_id == batch_id)
            .collect();
        for key in affected_keys {
            if let Some(watchers) = self.ack_watchers.remove(&key) {
                for (_requested_tier, sender) in watchers {
                    let _ = sender.send(Err(rejection.clone()));
                }
            }
        }
    }

    /// Drain every rejection id pending surface; subsequent calls return an
    /// empty vec until a new rejection is recorded.
    pub(crate) fn drain_rejected(&mut self) -> Vec<BatchId> {
        std::mem::take(&mut self.rejected_batch_ids)
            .into_iter()
            .collect()
    }

    /// Forget a batch entirely — drops any remaining watchers for it and
    /// removes it from the rejected set. Used when an explicit ack flushes
    /// the local batch record.
    pub(crate) fn forget_batch(&mut self, batch_id: BatchId) {
        self.ack_watchers.retain(|key, _| key.batch_id != batch_id);
        self.rejected_batch_ids.remove(&batch_id);
    }
}
