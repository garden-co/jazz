//! [`DurabilityTracker`] — owns durability waiters and pending mutation error
//! events that bindings still need to surface.
//!
//! Extracted from `RuntimeCore` so the orchestrator no longer carries the
//! bookkeeping for who-is-waiting-on-which-tier; it only delegates the
//! primitive operations (`register_batch_watcher`, `record_batch_ack`,
//! `record_rejection`, `drain_mutation_error_events`, `forget_batch`) defined here.

use std::collections::{BTreeMap, HashMap};

use futures::channel::oneshot;

use crate::row_histories::BatchId;
use crate::sync_manager::DurabilityTier;

use super::{MutationErrorEvent, PersistedWriteAck, PersistedWriteRejection};

#[derive(Default)]
pub(crate) struct DurabilityTracker {
    /// Watchers waiting for a batch to settle at a requested tier.
    batch_watchers: HashMap<BatchId, Vec<(DurabilityTier, oneshot::Sender<PersistedWriteAck>)>>,
    /// Rejected replayable batch events surfaced once to bindings on next drain.
    pending_mutation_error_events: BTreeMap<BatchId, MutationErrorEvent>,
}

impl DurabilityTracker {
    /// Construct a tracker preloaded with mutation errors recovered from
    /// persisted local batch records on startup.
    pub(crate) fn with_initial_mutation_error_events(
        events: BTreeMap<BatchId, MutationErrorEvent>,
    ) -> Self {
        Self {
            batch_watchers: HashMap::new(),
            pending_mutation_error_events: events,
        }
    }

    /// Register a watcher that resolves when `batch_id` reaches `tier` (or higher).
    pub(crate) fn register_batch_watcher(
        &mut self,
        batch_id: BatchId,
        tier: DurabilityTier,
        sender: oneshot::Sender<PersistedWriteAck>,
    ) {
        self.batch_watchers
            .entry(batch_id)
            .or_default()
            .push((tier, sender));
    }

    /// Resolve every watcher on `batch_id` whose requested tier is satisfied by
    /// `acked_tier`; keep the rest registered.
    pub(crate) fn record_batch_ack(&mut self, batch_id: BatchId, acked_tier: DurabilityTier) {
        let Some(watchers) = self.batch_watchers.remove(&batch_id) else {
            return;
        };
        let mut remaining = Vec::new();
        for (requested_tier, sender) in watchers {
            if acked_tier >= requested_tier {
                tracing::debug!(
                    ?batch_id,
                    ?acked_tier,
                    ?requested_tier,
                    "batch watcher resolved"
                );
                let _ = sender.send(Ok(()));
            } else {
                remaining.push((requested_tier, sender));
            }
        }
        if !remaining.is_empty() {
            self.batch_watchers.insert(batch_id, remaining);
        }
    }

    /// Mark `batch_id` rejected and notify every watcher waiting on that batch
    /// with the rejection details. Returns true when at least one live watcher
    /// accepted the rejection.
    pub(crate) fn record_rejection(&mut self, batch_id: BatchId, code: &str, reason: &str) -> bool {
        let rejection = PersistedWriteRejection {
            batch_id,
            code: code.to_string(),
            reason: reason.to_string(),
        };

        let mut handled_by_waiter = false;
        if let Some(watchers) = self.batch_watchers.remove(&batch_id) {
            for (_requested_tier, sender) in watchers {
                if sender.send(Err(rejection.clone())).is_ok() {
                    handled_by_waiter = true;
                }
            }
        }

        handled_by_waiter
    }

    /// Queue a mutation error event for bindings to deliver. Repeated
    /// rejection fates for the same batch coalesce to one pending event.
    pub(crate) fn queue_mutation_error_event(&mut self, event: MutationErrorEvent) {
        self.pending_mutation_error_events
            .entry(event.batch.batch_id)
            .or_insert(event);
    }

    /// Drop a queued-but-undelivered mutation error event for `batch_id`.
    pub(crate) fn take_mutation_error_event(
        &mut self,
        batch_id: BatchId,
    ) -> Option<MutationErrorEvent> {
        self.pending_mutation_error_events.remove(&batch_id)
    }

    /// Drain every mutation error event pending surface; subsequent calls
    /// return an empty vec until a new rejection is recorded.
    pub(crate) fn drain_mutation_error_events(&mut self) -> Vec<MutationErrorEvent> {
        std::mem::take(&mut self.pending_mutation_error_events)
            .into_values()
            .collect()
    }

    /// Forget a batch entirely — drops any remaining watchers for it and
    /// removes it from the rejected set. Used when an explicit ack flushes
    /// the local batch record.
    pub(crate) fn forget_batch(&mut self, batch_id: BatchId) {
        self.batch_watchers.remove(&batch_id);
        self.pending_mutation_error_events.remove(&batch_id);
    }
}
