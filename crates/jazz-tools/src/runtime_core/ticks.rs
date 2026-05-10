use super::*;
use crate::batch_fate::{LocalBatchMember, SealedBatchMember, SealedBatchSubmission};
use crate::row_histories::{RowState, patch_row_batch_state};
use crate::storage::metadata_from_row_locator;
use crate::sync_manager::{RowMetadata, SyncPayload};

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    fn local_batch_row_was_insert(
        &self,
        table: &str,
        row: &crate::row_histories::StoredRowBatch,
    ) -> bool {
        if !row.parents.is_empty() {
            return false;
        }

        let Ok(history_rows) = self.storage.scan_history_row_batches(table, row.row_id) else {
            return true;
        };
        !history_rows.iter().any(|candidate| {
            candidate.branch == row.branch
                && candidate.batch_id != row.batch_id
                && !matches!(candidate.state, RowState::Rejected)
        })
    }

    pub(crate) fn local_batch_rows(
        &self,
        batch_id: crate::row_histories::BatchId,
    ) -> Vec<(
        LocalBatchMember,
        crate::storage::RowLocator,
        crate::row_histories::StoredRowBatch,
    )> {
        let mut rows = Vec::new();
        if let Ok(Some(submission)) = self.storage.load_sealed_batch_submission(batch_id) {
            for sealed_member in submission.members {
                let Ok(Some(row_locator)) = self.storage.load_row_locator(sealed_member.object_id)
                else {
                    continue;
                };
                let Some(row) = self
                    .storage
                    .scan_history_row_batches(row_locator.table.as_str(), sealed_member.object_id)
                    .ok()
                    .and_then(|rows| {
                        rows.into_iter().find(|row| {
                            row.batch_id == batch_id
                                && row.branch.as_str() == submission.target_branch_name.as_str()
                                && row.content_digest() == sealed_member.row_digest
                        })
                    })
                else {
                    continue;
                };
                let Ok(schema_hash) = self.local_batch_member_schema_hash(
                    submission.target_branch_name,
                    sealed_member.object_id,
                    batch_id,
                ) else {
                    continue;
                };
                let member = LocalBatchMember {
                    object_id: sealed_member.object_id,
                    table_name: row_locator.table.to_string(),
                    branch_name: submission.target_branch_name,
                    schema_hash,
                    row_digest: sealed_member.row_digest,
                };
                rows.push((member, row_locator, row));
            }
        }

        if rows.is_empty()
            && let Some(record) = self.local_batch_record_cache.get(&batch_id)
        {
            for member in record.members.clone() {
                let row_locator = self
                    .storage
                    .load_row_locator(member.object_id)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| crate::storage::RowLocator {
                        table: member.table_name.clone().into(),
                        origin_schema_hash: None,
                    });
                let Ok(Some(row)) = self.storage.load_history_row_batch_for_schema_hash(
                    member.table_name.as_str(),
                    member.schema_hash,
                    member.branch_name.as_str(),
                    member.object_id,
                    batch_id,
                ) else {
                    let Some(row) = self
                        .storage
                        .scan_history_row_batches(member.table_name.as_str(), member.object_id)
                        .ok()
                        .and_then(|rows| {
                            rows.into_iter().find(|row| {
                                row.batch_id == batch_id
                                    && row.branch.as_str() == member.branch_name.as_str()
                            })
                        })
                    else {
                        continue;
                    };
                    rows.push((member, row_locator, row));
                    continue;
                };
                rows.push((member, row_locator, row));
            }
        }
        if rows.is_empty()
            && let Ok(Some(record)) = self.storage.load_local_batch_record(batch_id)
        {
            for member in record.members {
                let row_locator = self
                    .storage
                    .load_row_locator(member.object_id)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| crate::storage::RowLocator {
                        table: member.table_name.clone().into(),
                        origin_schema_hash: None,
                    });
                let Ok(Some(row)) = self.storage.load_history_row_batch_for_schema_hash(
                    member.table_name.as_str(),
                    member.schema_hash,
                    member.branch_name.as_str(),
                    member.object_id,
                    batch_id,
                ) else {
                    continue;
                };
                rows.push((member, row_locator, row));
            }
        }
        if rows.is_empty()
            && let Ok(row_locators) = self.storage.scan_row_locators()
        {
            for (object_id, row_locator) in row_locators {
                let Ok(history_rows) = self
                    .storage
                    .scan_history_row_batches(row_locator.table.as_str(), object_id)
                else {
                    continue;
                };
                for row in history_rows
                    .into_iter()
                    .filter(|row| row.batch_id == batch_id)
                {
                    let branch_name = BranchName::new(row.branch.as_str());
                    let Ok(schema_hash) =
                        self.local_batch_member_schema_hash(branch_name, object_id, batch_id)
                    else {
                        continue;
                    };
                    let member = LocalBatchMember {
                        object_id,
                        table_name: row_locator.table.to_string(),
                        branch_name,
                        schema_hash,
                        row_digest: row.content_digest(),
                    };
                    rows.push((member, row_locator.clone(), row));
                }
            }
        }

        rows.sort_by(
            |(left_member, left_locator, left_row), (right_member, right_locator, right_row)| {
                left_member
                    .object_id
                    .uuid()
                    .as_bytes()
                    .cmp(right_member.object_id.uuid().as_bytes())
                    .then_with(|| {
                        left_locator
                            .table
                            .as_str()
                            .cmp(right_locator.table.as_str())
                    })
                    .then_with(|| left_row.branch.as_str().cmp(right_row.branch.as_str()))
                    .then_with(|| {
                        left_member
                            .schema_hash
                            .as_bytes()
                            .cmp(right_member.schema_hash.as_bytes())
                    })
                    .then_with(|| left_row.batch_id.0.cmp(&right_row.batch_id.0))
            },
        );
        rows
    }

    pub(crate) fn direct_sealed_submission_from_local_batch_rows(
        batch_id: crate::row_histories::BatchId,
        rows: &[(
            LocalBatchMember,
            crate::storage::RowLocator,
            crate::row_histories::StoredRowBatch,
        )],
    ) -> Option<SealedBatchSubmission> {
        let first_branch = rows.first()?.0.branch_name;
        if rows
            .iter()
            .any(|(member, _, _)| member.branch_name != first_branch)
        {
            return None;
        }
        Some(SealedBatchSubmission::new(
            batch_id,
            first_branch,
            rows.iter()
                .map(|(member, _, _)| SealedBatchMember {
                    object_id: member.object_id,
                    row_digest: member.row_digest,
                })
                .collect(),
            Vec::new(),
        ))
    }

    fn apply_received_batch_fate(&mut self, fate: crate::batch_fate::BatchFate) {
        let batch_id = fate.batch_id();
        if let Err(error) = self.storage.upsert_authoritative_batch_fate(&fate) {
            tracing::warn!(
                ?batch_id,
                %error,
                "failed to persist batch fate"
            );
        }

        if let crate::batch_fate::BatchFate::Rejected { code, reason, .. } = &fate {
            self.mark_local_batch_rows_rejected(batch_id);
            let acknowledged = self
                .is_rejected_batch_acknowledged(batch_id)
                .unwrap_or(false);
            if !acknowledged {
                self.durability.record_rejection(batch_id, code, reason);
            }
        } else if matches!(fate, crate::batch_fate::BatchFate::Missing { .. }) {
            self.retransmit_local_batch_to_servers(batch_id);
        } else if matches!(
            fate,
            crate::batch_fate::BatchFate::AcceptedTransaction { .. }
        ) {
            self.schema_manager
                .query_manager_mut()
                .mark_subscriptions_visibility_recompute_for_batch(batch_id);
        }

        if let Some(acked_tier) = fate.confirmed_tier() {
            self.schema_manager
                .query_manager_mut()
                .mark_subscriptions_visibility_recompute_for_tier(acked_tier);
            for (member, row_locator, _) in self.local_batch_rows(batch_id) {
                self.schema_manager
                    .query_manager_mut()
                    .mark_local_row_updated_in_subscriptions(
                        row_locator.table.as_str(),
                        member.object_id,
                    );
            }
            self.durability.record_batch_ack(batch_id, acked_tier);
        }
    }

    pub(crate) fn mark_local_batch_rows_rejected(
        &mut self,
        batch_id: crate::row_histories::BatchId,
    ) {
        let mut cleared_rows = Vec::new();
        let mut batch_patch_succeeded_by_table = std::collections::HashMap::new();

        for (member, row_locator, row) in self.local_batch_rows(batch_id) {
            let was_visible = matches!(row.state, RowState::VisibleDirect)
                || (matches!(row.state, RowState::Rejected)
                    && self
                        .storage
                        .load_visible_region_row(
                            row_locator.table.as_str(),
                            row.branch.as_str(),
                            member.object_id,
                        )
                        .ok()
                        .flatten()
                        .is_some_and(|visible_row| visible_row.batch_id() == row.batch_id()));

            if !was_visible && !matches!(row.state, RowState::StagingPending | RowState::Superseded)
            {
                continue;
            }

            cleared_rows.push((
                row_locator.table.to_string(),
                member.schema_hash,
                row.branch.to_string(),
                member.object_id,
                row.batch_id(),
                row.data.to_vec(),
                was_visible,
                row.delete_kind.is_some(),
                self.local_batch_row_was_insert(row_locator.table.as_str(), &row),
            ));
        }

        for (table, _, _, _, _, _, was_visible, _, _) in &cleared_rows {
            if *was_visible {
                continue;
            }
            batch_patch_succeeded_by_table
                .entry(table.clone())
                .or_insert_with(|| {
                    self.storage
                        .patch_row_region_rows_by_batch(
                            table,
                            batch_id,
                            Some(RowState::Rejected),
                            None,
                        )
                        .is_ok()
                });
        }

        let query_manager = self.schema_manager.query_manager_mut();
        for (
            table,
            schema_hash,
            branch,
            row_id,
            member_batch_id,
            row_data,
            was_visible,
            was_delete,
            was_insert,
        ) in cleared_rows
        {
            if was_visible {
                let branch_name = crate::object::BranchName::new(&branch);
                let _ = self.storage.patch_row_region_rows_by_batch(
                    &table,
                    member_batch_id,
                    Some(RowState::Rejected),
                    None,
                );
                let _ = patch_row_batch_state(
                    &mut self.storage,
                    row_id,
                    &branch_name,
                    member_batch_id,
                    Some(RowState::Rejected),
                    None,
                );
                let _ = self.storage.patch_exact_row_batch_for_schema_hash(
                    &table,
                    schema_hash,
                    &branch,
                    row_id,
                    member_batch_id,
                    Some(RowState::Rejected),
                    None,
                );
            } else if !batch_patch_succeeded_by_table
                .get(&table)
                .copied()
                .unwrap_or(false)
            {
                let _ = self.storage.patch_exact_row_batch_for_schema_hash(
                    &table,
                    schema_hash,
                    &branch,
                    row_id,
                    member_batch_id,
                    Some(RowState::Rejected),
                    None,
                );
            }
            if was_visible {
                if was_delete {
                    query_manager.restore_local_rejected_delete_row(
                        &mut self.storage,
                        &table,
                        &branch,
                        row_id,
                        &row_data,
                    );
                } else if !was_insert {
                    query_manager.clear_local_pending_row_overlay(&table, row_id);
                } else {
                    let _ = self
                        .storage
                        .delete_visible_region_row(&table, &branch, row_id);
                    query_manager.retract_local_rejected_row(
                        &mut self.storage,
                        &table,
                        &branch,
                        row_id,
                        &row_data,
                        true,
                    );
                }
            } else {
                query_manager.retract_local_pending_transaction_row(
                    &mut self.storage,
                    &table,
                    &branch,
                    row_id,
                    &row_data,
                );
            }
        }
    }

    pub fn retransmit_local_batch_to_servers(&mut self, batch_id: crate::row_histories::BatchId) {
        let sealed_submission = self
            .storage
            .load_sealed_batch_submission(batch_id)
            .ok()
            .flatten();

        let local_rows = self.local_batch_rows(batch_id);
        let rows_to_retransmit = local_rows
            .iter()
            .map(|(member, row_locator, row)| {
                (
                    member.object_id,
                    metadata_from_row_locator(row_locator),
                    row.clone(),
                )
            })
            .collect::<Vec<_>>();
        let sealed_submission = sealed_submission.or_else(|| {
            Self::direct_sealed_submission_from_local_batch_rows(batch_id, &local_rows)
        });

        let sync_manager = self.schema_manager.query_manager_mut().sync_manager_mut();
        for (row_id, metadata, row) in rows_to_retransmit {
            sync_manager.force_row_batch_to_servers(row_id, metadata, row);
        }
        if let Some(submission) = sealed_submission {
            sync_manager.seal_batch_to_servers(submission);
        }
    }

    // =========================================================================
    // Tick Methods
    // =========================================================================

    /// Synchronous tick - processes managers, fulfills completed queries.
    ///
    /// Schedules batched_tick if there are outbound messages or storage writes
    /// waiting on the WAL flush barrier.
    ///
    /// Call this after any mutation operation (insert, update, delete, etc.)
    /// to process the change and schedule any required I/O.
    pub fn immediate_tick(&mut self) -> TickOutput {
        let _span = trace_span!("immediate_tick", tier = self.tier_label).entered();

        let recovered_sealed_batches = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .recover_completed_sealed_batches_with_storage(&mut self.storage);
        if recovered_sealed_batches {
            self.mark_storage_write_pending_flush();
        }

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
                    tracing::trace!(
                        server_id = ?pending_settled.server_id,
                        query_id = pending_settled.query_id.0,
                        tier = ?pending_settled.tier,
                        through_seq = pending_settled.through_seq,
                        "jazz trace query settled ready for query manager"
                    );
                    ready.push(pending_settled);
                } else {
                    tracing::trace!(
                        server_id = ?pending_settled.server_id,
                        query_id = pending_settled.query_id.0,
                        tier = ?pending_settled.tier,
                        through_seq = pending_settled.through_seq,
                        last_applied = pending_settled.server_id.and_then(|server_id| {
                            self.last_applied_server_seq.get(&server_id).copied()
                        }),
                        "jazz trace query settled blocked on stream sequence"
                    );
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
                    if let Some(server_id) = pending_settled.server_id {
                        query_manager
                            .sync_manager_mut()
                            .relay_query_settled_to_origins(
                                server_id,
                                pending_settled.query_id,
                                pending_settled.tier,
                            );
                    }
                    query_manager
                        .apply_query_settled(pending_settled.query_id, pending_settled.tier);
                }
            }
            self.schema_manager.process(&mut self.storage);
        }

        // 2c. Apply replayable batch fates before collecting subscription
        // updates so fate-driven visibility changes land in the same tick.
        let received_batch_fates = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_pending_batch_fates();
        if !received_batch_fates.is_empty() {
            for fate in received_batch_fates {
                self.apply_received_batch_fate(fate);
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

        // 4. Schedule batched_tick if outbound messages exist or a WAL flush
        // barrier is pending.
        if self.has_outbound() || self.storage_write_pending_flush {
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

        self.handle_transport_messages();

        if !self.has_outbound()
            && self
                .schema_manager
                .query_manager()
                .sync_manager()
                .has_pending_query_subscriptions()
        {
            self.immediate_tick();
        }

        // 1. Send all outgoing sync messages
        self.flush_runtime_outbox("flushing outbox");
        if self
            .schema_manager
            .query_manager()
            .sync_manager()
            .has_pending_query_subscriptions()
        {
            self.scheduler.schedule_batched_tick();
            return;
        }

        // 2. Process parked sync messages
        self.handle_sync_messages();

        // 3. Flush any new outbox entries generated by processing.
        // The scheduler's debounce prevents immediate_tick() from scheduling
        // another batched_tick while we're inside one, so we must flush here.
        self.flush_runtime_outbox("flushing post-process outbox");
        if self
            .schema_manager
            .query_manager()
            .sync_manager()
            .has_pending_query_subscriptions()
        {
            self.scheduler.schedule_batched_tick();
            return;
        }

        // Flush the storage durability barrier so writes survive a hard kill (tab close, crash).
        if self.storage_write_pending_flush {
            let _span = tracing::debug_span!("flush_wal").entered();
            self.storage.flush_wal();
            self.clear_storage_write_pending_flush();
        }
    }

    fn flush_runtime_outbox(&mut self, log_message: &str) {
        let outbox = self
            .schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        if !outbox.is_empty() {
            debug!(count = outbox.len(), "{log_message}");
        }

        let mut unsent = Vec::new();
        for msg in outbox {
            if let Some((ref tracer, ref name)) = self.sync_tracer {
                tracer.record_outgoing(name, &msg.destination, &msg.payload);
            }
            let handled_by_transport = self
                .transport
                .as_ref()
                .is_some_and(|handle| matches!(msg.destination, crate::sync_manager::Destination::Server(server_id) if server_id == handle.server_id));
            if handled_by_transport {
                if let Some(handle) = self.transport.as_ref() {
                    handle.send_outbox(msg);
                }
            } else if let Some(sync_sender) = self.sync_sender.as_ref() {
                sync_sender.send_sync_message(msg);
            } else {
                unsent.push(msg);
            }
        }

        if !unsent.is_empty() {
            self.schema_manager
                .query_manager_mut()
                .sync_manager_mut()
                .prepend_outbox(unsent);
        }
    }

    fn handle_transport_messages(&mut self) {
        let Some(server_id) = self.transport.as_ref().map(|handle| handle.server_id) else {
            return;
        };

        let mut inbound = Vec::new();
        if let Some(handle) = self.transport.as_mut() {
            while let Some(message) = handle.try_recv_inbound() {
                inbound.push(message);
            }
        }

        for message in inbound {
            match message {
                crate::transport_manager::TransportInbound::Connected {
                    catalogue_state_hash,
                    next_sync_seq,
                } => {
                    if let Some(next_sync_seq) = next_sync_seq {
                        self.set_next_expected_server_sequence(server_id, next_sync_seq);
                    }
                    self.add_server_with_catalogue_state_hash(
                        server_id,
                        catalogue_state_hash.as_deref(),
                    );
                }
                crate::transport_manager::TransportInbound::Sync { entry, sequence } => {
                    if let Some(sequence) = sequence {
                        self.park_sync_message_with_sequence(*entry, sequence);
                    } else {
                        self.park_sync_message(*entry);
                    }
                }
                crate::transport_manager::TransportInbound::SyncBatch { entries } => {
                    self.park_sync_message_batch(entries);
                }
                crate::transport_manager::TransportInbound::Disconnected => {
                    self.remove_server(server_id);
                }
                crate::transport_manager::TransportInbound::ConnectFailed { reason } => {
                    tracing::warn!(%server_id, %reason, "transport connect failed");
                    self.schema_manager
                        .query_manager_mut()
                        .sync_manager_mut()
                        .remove_pending_server(server_id);
                }
                crate::transport_manager::TransportInbound::AuthFailure { reason } => {
                    tracing::warn!(%server_id, %reason, "transport auth failure");
                    self.remove_server(server_id);
                    if let Some(callback) = self.auth_failure_callback.as_ref() {
                        callback(reason);
                    }
                }
            }
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
        let mut applied_since_last_tick = applied_messages > 0;
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
                applied_since_last_tick = true;
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
            if applied_since_last_tick {
                self.immediate_tick();
            }
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
        if let Some((ref tracer, ref name)) = self.sync_tracer {
            tracer.record_incoming(&message.source, name, &message.payload);
        }
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

                if let Some((ref tracer, ref name)) = self.sync_tracer {
                    tracer.record_incoming(&message.source, name, &message.payload);
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

    fn park_sync_message_batch(
        &mut self,
        entries: Vec<crate::transport_manager::SequencedInboxEntry>,
    ) {
        let mut parked_any = false;
        for crate::transport_manager::SequencedInboxEntry { entry, sequence } in entries {
            if let Some(sequence) = sequence {
                match entry.source {
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
                            continue;
                        }

                        if let Some((ref tracer, ref name)) = self.sync_tracer {
                            tracer.record_incoming(&entry.source, name, &entry.payload);
                        }

                        self.parked_sync_messages_by_server_seq
                            .entry(server_id)
                            .or_default()
                            .insert(sequence, entry);
                        parked_any = true;
                    }
                    _ => {
                        self.parked_sync_messages.push(entry);
                        parked_any = true;
                    }
                }
            } else {
                if let Some((ref tracer, ref name)) = self.sync_tracer {
                    tracer.record_incoming(&entry.source, name, &entry.payload);
                }
                self.parked_sync_messages.push(entry);
                parked_any = true;
            }
        }

        if parked_any {
            self.scheduler.schedule_batched_tick();
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

    /// Test seam: directly dispatch a `TransportInbound` event as if it arrived
    /// from `server_id`, exercising the same match arm as `batched_tick`.
    #[cfg(test)]
    #[cfg(feature = "transport-websocket")]
    pub(crate) fn handle_transport_inbound_for_test(
        &mut self,
        server_id: ServerId,
        event: crate::transport_manager::TransportInbound,
    ) {
        let mut released_server_hold = false;
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
            crate::transport_manager::TransportInbound::SyncBatch { entries } => {
                self.park_sync_message_batch(entries);
            }
            crate::transport_manager::TransportInbound::Disconnected => {
                self.remove_server(server_id);
                released_server_hold = true;
            }
            crate::transport_manager::TransportInbound::ConnectFailed { reason } => {
                debug!(%reason, "transport connect failed; releasing pending-server hold");
                self.schema_manager
                    .query_manager_mut()
                    .sync_manager_mut()
                    .remove_pending_server(server_id);
                released_server_hold = true;
            }
            crate::transport_manager::TransportInbound::AuthFailure { reason } => {
                self.remove_server(server_id);
                released_server_hold = true;
                if let Some(ref cb) = self.auth_failure_callback {
                    cb(reason);
                }
            }
        }

        if released_server_hold {
            self.immediate_tick();
        }
    }

    pub fn local_batch_replay_payloads(
        &self,
        batch_id: crate::row_histories::BatchId,
    ) -> Vec<SyncPayload> {
        let local_rows = self.local_batch_rows(batch_id);
        let mut payloads = local_rows
            .iter()
            .map(|(member, row_locator, row)| SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: member.object_id,
                    metadata: metadata_from_row_locator(row_locator),
                }),
                row: row.clone(),
            })
            .collect::<Vec<_>>();

        if let Some(submission) =
            Self::direct_sealed_submission_from_local_batch_rows(batch_id, &local_rows)
        {
            payloads.push(SyncPayload::SealBatch { submission });
        }

        payloads
    }
}
