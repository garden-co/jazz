use super::*;
use crate::row_histories::{RowState, patch_row_version_state};
use crate::storage::metadata_from_row_locator;

impl<S: Storage, Sch: Scheduler, Sy: SyncSender> RuntimeCore<S, Sch, Sy> {
    fn batch_id_for_row_version_ack(
        &self,
        row_version_key: crate::sync_manager::RowVersionKey,
    ) -> Option<crate::row_histories::BatchId> {
        let row_locator = match self.storage.load_row_locator(row_version_key.row_id) {
            Ok(Some(row_locator)) => row_locator,
            Ok(None) => return None,
            Err(error) => {
                tracing::warn!(
                    row_id = %row_version_key.row_id,
                    ?row_version_key.version_id,
                    %error,
                    "failed to load row locator for durability ack"
                );
                return None;
            }
        };

        match self.storage.load_history_row_version(
            row_locator.table.as_str(),
            row_version_key.row_id,
            row_version_key.version_id,
        ) {
            Ok(Some(row)) => Some(row.batch_id),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(
                    table = row_locator.table.as_str(),
                    row_id = %row_version_key.row_id,
                    ?row_version_key.version_id,
                    %error,
                    "failed to load row history for durability ack"
                );
                None
            }
        }
    }

    fn apply_received_batch_settlement(&mut self, settlement: crate::batch_fate::BatchSettlement) {
        let batch_id = settlement.batch_id();
        if let Ok(Some(mut record)) = self.storage.load_local_batch_record(batch_id) {
            record.apply_settlement(settlement.clone());
            if let Err(error) = self.storage.upsert_local_batch_record(&record) {
                tracing::warn!(
                    ?batch_id,
                    %error,
                    "failed to persist local batch settlement"
                );
            }
        }

        if matches!(
            settlement,
            crate::batch_fate::BatchSettlement::Rejected { .. }
        ) {
            self.mark_local_batch_rows_rejected(batch_id);
        } else if matches!(
            settlement,
            crate::batch_fate::BatchSettlement::Missing { .. }
        ) {
            self.retransmit_local_batch_to_servers(batch_id);
        }

        if let Some(acked_tier) = settlement.confirmed_tier()
            && let Some(watchers) = self.ack_watchers.remove(&batch_id)
        {
            let mut remaining = Vec::new();
            for (requested_tier, sender) in watchers {
                if acked_tier >= requested_tier {
                    tracing::debug!(
                        ?batch_id,
                        ?acked_tier,
                        ?requested_tier,
                        "batch settlement resolved ack watcher"
                    );
                    let _ = sender.send(());
                } else {
                    remaining.push((requested_tier, sender));
                }
            }
            if !remaining.is_empty() {
                self.ack_watchers.insert(batch_id, remaining);
            }
        }
    }

    fn mark_local_batch_rows_rejected(&mut self, batch_id: crate::row_histories::BatchId) {
        let Ok(row_locators) = self.storage.scan_row_locators() else {
            return;
        };
        let mut cleared_overlays = Vec::new();

        for (row_id, row_locator) in row_locators {
            let Ok(history_rows) = self
                .storage
                .scan_history_row_versions(row_locator.table.as_str(), row_id)
            else {
                continue;
            };

            for row in history_rows {
                if row.batch_id != batch_id || !matches!(row.state, RowState::StagingPending) {
                    continue;
                }

                let branch_name = crate::object::BranchName::new(&row.branch);
                let _ = patch_row_version_state(
                    &mut self.storage,
                    row_id,
                    &branch_name,
                    row.version_id(),
                    Some(RowState::Rejected),
                    None,
                );
                cleared_overlays.push((
                    row_locator.table.to_string(),
                    row.branch.to_string(),
                    row_id,
                    row.data.to_vec(),
                ));
            }
        }

        let query_manager = self.schema_manager.query_manager_mut();
        for (table, branch, row_id, row_data) in cleared_overlays {
            query_manager.retract_local_pending_transaction_row(
                &mut self.storage,
                &table,
                &branch,
                row_id,
                &row_data,
            );
        }
    }

    fn retransmit_local_batch_to_servers(&mut self, batch_id: crate::row_histories::BatchId) {
        let Ok(row_locators) = self.storage.scan_row_locators() else {
            return;
        };

        let mut rows_to_retransmit = Vec::new();
        for (row_id, row_locator) in row_locators {
            let Ok(history_rows) = self
                .storage
                .scan_history_row_versions(row_locator.table.as_str(), row_id)
            else {
                continue;
            };

            for row in history_rows {
                if row.batch_id == batch_id {
                    rows_to_retransmit.push((row_id, metadata_from_row_locator(&row_locator), row));
                }
            }
        }

        let sync_manager = self.schema_manager.query_manager_mut().sync_manager_mut();
        for (row_id, metadata, row) in rows_to_retransmit {
            sync_manager.force_row_version_to_servers(row_id, metadata, row);
        }
    }

    // =========================================================================
    // Tick Methods
    // =========================================================================

    /// Synchronous tick - processes managers, fulfills completed queries.
    ///
    /// Schedules batched_tick if there are outbound messages.
    ///
    /// Call this after any mutation operation (insert, update, delete, etc.)
    /// to process the change and schedule any required I/O.
    pub fn immediate_tick(&mut self) -> TickOutput {
        let _span = trace_span!("immediate_tick", tier = self.tier_label).entered();

        // 1. Process logical updates (sync, subscriptions)
        self.schema_manager.process(&mut self.storage);

        // 2. Second process() handles deferred query subscriptions that couldn't
        //    compile on first pass (schema wasn't available yet, e.g. catalogue
        //    was just processed and made the schema available).
        self.schema_manager.process(&mut self.storage);

        // 2b. Release QuerySettled notifications whose upstream stream watermark
        // has definitely been applied.
        let ready_query_settled = {
            let pending = self
                .schema_manager
                .query_manager_mut()
                .sync_manager_mut()
                .take_pending_query_settled();
            let mut ready = Vec::new();
            let mut blocked = Vec::new();

            for pending_settled in pending {
                let is_ready = pending_settled.server_id.is_none_or(|server_id| {
                    self.last_applied_server_seq
                        .get(&server_id)
                        .copied()
                        .unwrap_or(0)
                        >= pending_settled.through_seq
                });
                if is_ready {
                    ready.push(pending_settled);
                } else {
                    blocked.push(pending_settled);
                }
            }

            if !blocked.is_empty() {
                self.schema_manager
                    .query_manager_mut()
                    .sync_manager_mut()
                    .requeue_pending_query_settled(blocked);
            }

            ready
        };

        if !ready_query_settled.is_empty() {
            {
                let query_manager = self.schema_manager.query_manager_mut();
                for pending_settled in ready_query_settled {
                    query_manager
                        .apply_query_settled(pending_settled.query_id, pending_settled.tier);
                }
            }
            self.schema_manager.process(&mut self.storage);
        }

        // 2c. Apply replayable batch settlements before collecting subscription
        // updates so settlement-driven visibility changes land in the same tick.
        let received_batch_settlements = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_pending_batch_settlements();
        if !received_batch_settlements.is_empty() {
            for settlement in received_batch_settlements {
                self.apply_received_batch_settlement(settlement);
            }
            self.schema_manager.process(&mut self.storage);
        }

        // 3. Collect subscription updates
        let subscription_updates = self.schema_manager.query_manager_mut().take_updates();
        let subscription_failures = self
            .schema_manager
            .query_manager_mut()
            .take_failed_subscriptions();

        // Track one-shot queries that completed this tick
        let mut completed_one_shots: Vec<SubscriptionHandle> = Vec::new();
        let mut failed_one_shots: Vec<SubscriptionHandle> = Vec::new();
        let mut callbacks_fired: u64 = 0;

        // 3. Call subscription callbacks AND handle one-shot queries
        for update in &subscription_updates {
            if let Some(&handle) = self.subscription_reverse.get(&update.subscription_id) {
                // Check if this is a one-shot query
                if let Some(pending) = self.pending_one_shot_queries.get_mut(&handle) {
                    // First callback = graph settled, fulfill the future
                    if let Some(sender) = pending.sender.take() {
                        // Decode rows using the query's output descriptor
                        let results: Vec<(ObjectId, Vec<Value>)> = update
                            .ordered_delta
                            .added
                            .iter()
                            .filter_map(|row| {
                                decode_row(&update.descriptor, &row.row.data)
                                    .ok()
                                    .map(|values| (row.row.id, values))
                            })
                            .collect();
                        let _ = sender.send(Ok(results));
                    }
                    // Mark for cleanup (unsubscribe happens after loop)
                    completed_one_shots.push(handle);
                } else if let Some(state) = self.subscriptions.get(&handle) {
                    // Regular subscription - call callback
                    let delta = SubscriptionDelta {
                        handle,
                        ordered_delta: update.ordered_delta.clone(),
                        descriptor: update.descriptor.clone(),
                    };
                    (state.callback)(delta);
                    callbacks_fired += 1;
                }
            }
        }
        tracing::debug!(callbacks_fired, "subscription callbacks fired this tick");

        for failure in &subscription_failures {
            if let Some(&handle) = self.subscription_reverse.get(&failure.subscription_id) {
                if let Some(pending) = self.pending_one_shot_queries.get_mut(&handle) {
                    if let Some(sender) = pending.sender.take() {
                        let _ = sender.send(Err(RuntimeError::QueryError(format!(
                            "query subscription {} failed during schema recompile: {}",
                            failure.subscription_id.0, failure.reason
                        ))));
                    }
                    failed_one_shots.push(handle);
                } else if self.subscriptions.remove(&handle).is_some() {
                    self.subscription_reverse.remove(&failure.subscription_id);
                    tracing::error!(
                        handle = handle.0,
                        sub_id = failure.subscription_id.0,
                        error = %failure.reason,
                        "subscription failed during schema recompile and was dropped"
                    );
                }
            } else {
                tracing::error!(
                    sub_id = failure.subscription_id.0,
                    error = %failure.reason,
                    "subscription failed during schema recompile and was dropped"
                );
            }
        }

        // 2b. Cleanup completed one-shot queries
        for handle in completed_one_shots {
            if let Some(pending) = self.pending_one_shot_queries.remove(&handle) {
                // Unsubscribe from the underlying subscription
                self.schema_manager
                    .query_manager_mut()
                    .unsubscribe_with_sync(pending.subscription_id);
                self.subscription_reverse.remove(&pending.subscription_id);
            }
        }

        // 2c. Cleanup failed one-shot queries.
        // The underlying subscriptions were already removed by QueryManager.
        for handle in failed_one_shots {
            if let Some(pending) = self.pending_one_shot_queries.remove(&handle) {
                self.subscription_reverse.remove(&pending.subscription_id);
            }
        }

        // 3b. Process received row-version persistence acks — resolve matching watchers
        let received_acks = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_received_row_version_acks();
        for (row_version_key, acked_tier) in received_acks {
            let Some(batch_id) = self.batch_id_for_row_version_ack(row_version_key) else {
                continue;
            };
            if let Some(watchers) = self.ack_watchers.remove(&batch_id) {
                let mut remaining = Vec::new();
                for (requested_tier, sender) in watchers {
                    if acked_tier >= requested_tier {
                        tracing::debug!(
                            ?batch_id,
                            ?acked_tier,
                            ?requested_tier,
                            "ack watcher resolved"
                        );
                        let _ = sender.send(());
                    } else {
                        remaining.push((requested_tier, sender));
                    }
                }
                if !remaining.is_empty() {
                    self.ack_watchers.insert(batch_id, remaining);
                }
            }
        }

        // 4. Schedule batched_tick if outbound messages exist
        if self.has_outbound() {
            self.scheduler.schedule_batched_tick();
        }

        TickOutput {
            subscription_updates,
        }
    }

    /// Batched tick - handles all I/O, then processes parked messages.
    ///
    /// Called by the platform when the scheduled tick fires. This:
    /// 1. Sends all outgoing sync messages via SyncSender
    /// 2. Processes parked sync messages
    ///
    /// Each step is followed by an immediate_tick to process results.
    pub fn batched_tick(&mut self) {
        let _span = debug_span!("batched_tick", tier = self.tier_label).entered();

        // 1. Send all outgoing sync messages
        let outbox = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        if !outbox.is_empty() {
            debug!(count = outbox.len(), "flushing outbox");
        }
        for msg in outbox {
            self.sync_sender.send_sync_message(msg);
        }

        // 2. Process parked sync messages
        self.handle_sync_messages();

        // 3. Flush any new outbox entries generated by processing.
        // The scheduler's debounce prevents immediate_tick() from scheduling
        // another batched_tick while we're inside one, so we must flush here.
        let outbox = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        if !outbox.is_empty() {
            debug!(count = outbox.len(), "flushing post-process outbox");
        }
        for msg in outbox {
            self.sync_sender.send_sync_message(msg);
        }

        // Flush the storage durability barrier so writes survive a hard kill (tab close, crash).
        if self.storage_write_pending_flush {
            let _span = tracing::debug_span!("flush_wal").entered();
            self.storage.flush_wal();
            self.clear_storage_write_pending_flush();
        }
    }

    /// Apply parked sync messages and tick.
    fn handle_sync_messages(&mut self) {
        let messages = std::mem::take(&mut self.parked_sync_messages);
        let mut applied_messages = 0usize;

        if !messages.is_empty() {
            debug!(
                count = messages.len(),
                "processing parked unsequenced sync messages"
            );
        }
        for msg in messages {
            if msg.payload.writes_storage() {
                self.mark_storage_write_pending_flush();
            }
            self.push_sync_inbox(msg);
            applied_messages += 1;
        }

        let server_ids: Vec<ServerId> = self
            .parked_sync_messages_by_server_seq
            .keys()
            .copied()
            .collect();
        for server_id in server_ids {
            let mut next_expected = *self.next_expected_server_seq.get(&server_id).unwrap_or(&1);
            let mut ready_messages = Vec::new();
            let mut remove_buffer = false;
            if let Some(buffered) = self.parked_sync_messages_by_server_seq.get_mut(&server_id) {
                while let Some(msg) = buffered.remove(&next_expected) {
                    ready_messages.push((next_expected, msg));
                    next_expected += 1;
                }

                if buffered.is_empty() {
                    remove_buffer = true;
                }
            }
            let mut last_applied = self
                .last_applied_server_seq
                .get(&server_id)
                .copied()
                .unwrap_or(next_expected.saturating_sub(1));
            for (sequence, msg) in ready_messages {
                if msg.payload.writes_storage() {
                    self.mark_storage_write_pending_flush();
                }
                self.push_sync_inbox(msg);
                applied_messages += 1;
                last_applied = sequence;
            }
            self.next_expected_server_seq
                .insert(server_id, next_expected);
            self.last_applied_server_seq.insert(server_id, last_applied);
            if remove_buffer {
                self.parked_sync_messages_by_server_seq.remove(&server_id);
            }
        }

        if applied_messages > 0 {
            debug!(count = applied_messages, "applied parked sync messages");
            self.immediate_tick();
        }
    }

    /// Check if there are outbound messages requiring a batched_tick.
    pub fn has_outbound(&self) -> bool {
        !self
            .schema_manager
            .query_manager()
            .sync_manager()
            .outbox()
            .is_empty()
    }

    /// Park a sync message for processing in next batched_tick.
    pub fn park_sync_message(&mut self, message: InboxEntry) {
        trace!(source = ?message.source, payload = message.payload.variant_name(), "parking sync message");
        self.parked_sync_messages.push(message);
        self.scheduler.schedule_batched_tick();
    }

    /// Park a sequenced sync message for in-order processing in next batched_tick.
    pub fn park_sync_message_with_sequence(&mut self, message: InboxEntry, sequence: u64) {
        match message.source {
            crate::sync_manager::Source::Server(server_id) => {
                let next_expected = self
                    .next_expected_server_seq
                    .entry(server_id)
                    .or_insert(sequence);
                if sequence < *next_expected {
                    trace!(
                        ?server_id,
                        sequence,
                        next_expected = *next_expected,
                        "dropping already-applied sequenced sync message"
                    );
                    return;
                }

                self.parked_sync_messages_by_server_seq
                    .entry(server_id)
                    .or_default()
                    .insert(sequence, message);
                self.scheduler.schedule_batched_tick();
            }
            _ => self.park_sync_message(message),
        }
    }

    /// Set the next expected sequenced message for a server stream.
    pub fn set_next_expected_server_sequence(&mut self, server_id: ServerId, next_sequence: u64) {
        let next_sequence = next_sequence.max(1);
        self.next_expected_server_seq
            .insert(server_id, next_sequence);
        self.last_applied_server_seq
            .insert(server_id, next_sequence.saturating_sub(1));
        if let Some(buffered) = self.parked_sync_messages_by_server_seq.get_mut(&server_id) {
            buffered.retain(|seq, _| *seq >= next_sequence);
        }
    }
}
