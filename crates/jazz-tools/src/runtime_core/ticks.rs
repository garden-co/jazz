use super::*;

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
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

        // 3b. Process received persistence acks — resolve matching watchers
        let received_acks = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_received_row_version_acks();
        for (row_version_key, acked_tier) in received_acks {
            if let Some(watchers) = self.ack_watchers.remove(&row_version_key) {
                let mut remaining = Vec::new();
                for (requested_tier, sender) in watchers {
                    if acked_tier >= requested_tier {
                        tracing::debug!(
                            ?row_version_key,
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
                    self.ack_watchers.insert(row_version_key, remaining);
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
    /// 1. Drains inbound transport events (Connected / Sync / Disconnected)
    /// 2. Sends all outgoing sync messages via transport
    /// 3. Processes parked sync messages
    /// 4. Flushes any new outbox entries generated by processing
    /// 5. Flushes the storage WAL barrier if writes are pending
    pub fn batched_tick(&mut self) {
        let _span = debug_span!("batched_tick", tier = self.tier_label).entered();

        // 1. Drain inbound transport events.
        // Collect into a Vec first to avoid holding &mut self.transport while
        // calling &mut self methods (remove_server, add_server_with_catalogue_state_hash, etc.).
        {
            let events: Vec<crate::transport_manager::TransportInbound> = {
                let mut buf = Vec::new();
                if let Some(ref mut transport) = self.transport {
                    while let Some(event) = transport.try_recv_inbound() {
                        buf.push(event);
                    }
                }
                buf
            };
            if !events.is_empty() {
                debug!(count = events.len(), "draining inbound transport events");
            }
            for event in events {
                let server_id = match &self.transport {
                    Some(h) => h.server_id,
                    None => break,
                };
                match event {
                    crate::transport_manager::TransportInbound::Connected {
                        catalogue_state_hash,
                        next_sync_seq,
                    } => {
                        self.remove_server(server_id);
                        self.add_server_with_catalogue_state_hash(
                            server_id,
                            catalogue_state_hash.as_deref(),
                        );
                        if let Some(seq) = next_sync_seq {
                            self.set_next_expected_server_sequence(server_id, seq);
                        }
                    }
                    crate::transport_manager::TransportInbound::Sync { entry, sequence } => {
                        if let Some(seq) = sequence {
                            self.park_sync_message_with_sequence(*entry, seq);
                        } else {
                            self.park_sync_message(*entry);
                        }
                    }
                    crate::transport_manager::TransportInbound::Disconnected => {
                        self.remove_server(server_id);
                    }
                }
            }
        }

        // 2. Send all outgoing sync messages — only when a sink is present.
        if self.has_outbox_sink() {
            let outbox = self
                .schema_manager
                .query_manager_mut()
                .sync_manager_mut()
                .take_outbox();
            if !outbox.is_empty() {
                debug!(count = outbox.len(), "flushing outbox");
            }
            self.flush_outbox(outbox);
        }

        // 3. Process parked sync messages
        self.handle_sync_messages();

        // 4. Flush any new outbox entries generated by processing.
        // The scheduler's debounce prevents immediate_tick() from scheduling
        // another batched_tick while we're inside one, so we must flush here.
        if self.has_outbox_sink() {
            let outbox = self
                .schema_manager
                .query_manager_mut()
                .sync_manager_mut()
                .take_outbox();
            if !outbox.is_empty() {
                debug!(count = outbox.len(), "flushing post-process outbox");
            }
            self.flush_outbox(outbox);
        }

        // 5. Flush the storage durability barrier so writes survive a hard kill (tab close, crash).
        if self.storage_write_pending_flush {
            let _span = tracing::debug_span!("flush_wal").entered();
            self.storage.flush_wal();
            self.clear_storage_write_pending_flush();
        }
    }

    /// Drain a batch of outbox entries through the live delivery path:
    /// transport handle when set, otherwise the fallback `sync_sender`.
    fn flush_outbox(&self, outbox: Vec<crate::sync_manager::types::OutboxEntry>) {
        if outbox.is_empty() {
            return;
        }
        if let Some(ref h) = self.transport {
            for msg in outbox {
                h.send_outbox(msg);
            }
            return;
        }
        if let Some(ref sender) = self.sync_sender {
            for msg in outbox {
                sender.send_sync_message(msg);
            }
        }
    }

    /// True when there is a live sink (transport handle or fallback sync_sender)
    /// to receive outbox entries. When false, batched_tick skips the outbox
    /// drain so tests can inspect `take_outbox()` directly.
    fn has_outbox_sink(&self) -> bool {
        self.transport.is_some() || self.sync_sender.is_some()
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
