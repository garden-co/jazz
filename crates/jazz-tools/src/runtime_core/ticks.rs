use super::*;

impl<S: Storage, Sch: Scheduler, Sy: SyncSender> RuntimeCore<S, Sch, Sy> {
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

        // 3. Process received persistence acks — resolve matching watchers
        let received_acks = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_received_acks();
        for (commit_id, acked_tier) in received_acks {
            if let Err(error) = self.advance_mutation_ack(commit_id, acked_tier) {
                tracing::warn!(?commit_id, ?acked_tier, error = %error, "failed to advance mutation ack");
            }
            if let Some(watchers) = self.ack_watchers.remove(&commit_id) {
                let mut remaining = Vec::new();
                for (requested_tier, sender) in watchers {
                    if acked_tier >= requested_tier {
                        tracing::debug!(
                            ?commit_id,
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
                    self.ack_watchers.insert(commit_id, remaining);
                }
            }
        }

        // 4. Process received mutation outcomes.
        let received_mutation_outcomes = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_received_mutation_outcomes();
        let had_mutation_outcomes = !received_mutation_outcomes.is_empty();
        for outcome in received_mutation_outcomes {
            if let Err(error) = self.handle_received_mutation_outcome(outcome) {
                tracing::warn!(error = %error, "failed to process mutation outcome");
            }
        }

        if had_mutation_outcomes {
            self.schema_manager.process(&mut self.storage);
            self.schema_manager.process(&mut self.storage);
        }

        // 5. Collect subscription updates after mutation outcomes have had a chance
        // to roll back local state and invalidate queries.
        let subscription_updates = self.schema_manager.query_manager_mut().take_updates();
        let subscription_failures = self
            .schema_manager
            .query_manager_mut()
            .take_failed_subscriptions();

        // Track one-shot queries that completed this tick
        let mut completed_one_shots: Vec<SubscriptionHandle> = Vec::new();
        let mut failed_one_shots: Vec<SubscriptionHandle> = Vec::new();
        let mut callbacks_fired: u64 = 0;

        // 6. Call subscription callbacks AND handle one-shot queries.
        for update in &subscription_updates {
            if let Some(&handle) = self.subscription_reverse.get(&update.subscription_id) {
                if let Some(pending) = self.pending_one_shot_queries.get_mut(&handle) {
                    if let Some(sender) = pending.sender.take() {
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
                    completed_one_shots.push(handle);
                } else if let Some(state) = self.subscriptions.get(&handle) {
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

        for handle in completed_one_shots {
            if let Some(pending) = self.pending_one_shot_queries.remove(&handle) {
                self.schema_manager
                    .query_manager_mut()
                    .unsubscribe_with_sync(pending.subscription_id);
                self.subscription_reverse.remove(&pending.subscription_id);
            }
        }

        for handle in failed_one_shots {
            if let Some(pending) = self.pending_one_shot_queries.remove(&handle) {
                self.subscription_reverse.remove(&pending.subscription_id);
            }
        }

        // 7. Schedule batched_tick if outbound messages exist
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
        {
            let _span = tracing::debug_span!("flush_wal").entered();
            self.storage.flush_wal();
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
                    ready_messages.push(msg);
                    next_expected += 1;
                }

                if buffered.is_empty() {
                    remove_buffer = true;
                }
            }
            for msg in ready_messages {
                self.push_sync_inbox(msg);
                applied_messages += 1;
            }
            self.next_expected_server_seq
                .insert(server_id, next_expected);
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
        if let Some(buffered) = self.parked_sync_messages_by_server_seq.get_mut(&server_id) {
            buffered.retain(|seq, _| *seq >= next_sequence);
        }
    }
}
