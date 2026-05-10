use super::*;
use crate::batch_fate::{BatchFate, SealedBatchSubmission};
use crate::metadata::MetadataKey;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::policy::Operation;
use crate::row_histories::{
    RowState, RowVisibilityChange, StoredRowBatch, apply_row_batch, patch_row_batch_state,
};
use crate::storage::{Storage, metadata_from_row_locator};
use std::collections::{HashMap, HashSet};

struct AppliedRowBatch {
    metadata: HashMap<String, String>,
    row: StoredRowBatch,
    visibility_change: Option<RowVisibilityChange>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SealedBatchMode {
    Direct,
    Transactional,
}

impl SyncManager {
    fn queue_batch_durability_ack_to_server(
        &mut self,
        server_id: ServerId,
        tier: DurabilityTier,
        row: &StoredRowBatch,
        _branch_name: BranchName,
    ) {
        if !row.state.is_visible() {
            return;
        }

        let is_transactional = matches!(row.state, RowState::VisibleTransactional);

        for entry in self.outbox.iter_mut().rev() {
            if entry.destination != Destination::Server(server_id) {
                continue;
            }
            let SyncPayload::BatchFate { fate } = &mut entry.payload else {
                continue;
            };

            match fate {
                BatchFate::DurableDirect {
                    batch_id,
                    confirmed_tier,
                } if !is_transactional && *batch_id == row.batch_id && *confirmed_tier == tier => {
                    return;
                }
                BatchFate::AcceptedTransaction {
                    batch_id,
                    confirmed_tier,
                } if is_transactional && *batch_id == row.batch_id && *confirmed_tier == tier => {
                    return;
                }
                _ => {}
            }
        }

        let fate = if is_transactional {
            BatchFate::AcceptedTransaction {
                batch_id: row.batch_id,
                confirmed_tier: tier,
            }
        } else {
            BatchFate::DurableDirect {
                batch_id: row.batch_id,
                confirmed_tier: tier,
            }
        };

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::BatchFate { fate },
        });
    }

    fn retain_client_batch_fate<H: Storage>(&mut self, storage: &mut H, fate: &BatchFate) {
        let _ = storage.upsert_authoritative_batch_fate(fate);
    }

    fn validate_sealed_batch_submission(
        &self,
        submission: &SealedBatchSubmission,
    ) -> Result<BranchName, BatchFate> {
        if submission.members.is_empty() {
            return Err(BatchFate::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: "sealed batch must declare at least one member".to_string(),
            });
        }

        if submission.batch_digest
            != SealedBatchSubmission::compute_batch_digest(&submission.members)
        {
            return Err(BatchFate::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: "sealed batch digest does not match declared members".to_string(),
            });
        }

        Ok(submission.target_branch_name)
    }

    fn frontier_conflict_fate(&self, batch_id: crate::row_histories::BatchId) -> BatchFate {
        BatchFate::Rejected {
            batch_id,
            code: "transaction_conflict".to_string(),
            reason: "family-visible frontier changed since batch was sealed".to_string(),
        }
    }

    fn validate_batch_rows_target_branch(
        &self,
        submission: &SealedBatchSubmission,
        batch_rows: &[(String, StoredRowBatch)],
    ) -> Result<(), BatchFate> {
        if batch_rows.iter().any(|(_, row)| {
            row.batch_id == submission.batch_id
                && row.branch.as_str() != submission.target_branch_name.as_str()
        }) {
            return Err(BatchFate::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: "sealed batch rows must belong to the declared target branch".to_string(),
            });
        }

        Ok(())
    }

    fn infer_sealed_batch_mode(
        &self,
        submission: &SealedBatchSubmission,
        batch_rows: &[(String, StoredRowBatch)],
    ) -> Result<Option<SealedBatchMode>, BatchFate> {
        let mut mode = None;
        for (_, row) in batch_rows {
            let row_mode = match row.state {
                RowState::VisibleDirect => SealedBatchMode::Direct,
                RowState::StagingPending => SealedBatchMode::Transactional,
                _ => {
                    return Err(BatchFate::Rejected {
                        batch_id: submission.batch_id,
                        code: "invalid_batch_submission".to_string(),
                        reason: "sealed batch rows must be visible direct or staging pending"
                            .to_string(),
                    });
                }
            };

            match mode {
                Some(existing) if existing != row_mode => {
                    return Err(BatchFate::Rejected {
                        batch_id: submission.batch_id,
                        code: "invalid_batch_submission".to_string(),
                        reason: "sealed batch mixes direct and transactional rows".to_string(),
                    });
                }
                Some(_) => {}
                None => mode = Some(row_mode),
            }
        }

        Ok(mode)
    }

    fn validate_captured_frontier<H: Storage>(
        &self,
        storage: &H,
        submission: &SealedBatchSubmission,
    ) -> Result<(), BatchFate> {
        let current_frontier = storage
            .capture_family_visible_frontier(submission.target_branch_name)
            .map_err(|error| BatchFate::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: format!("failed to capture family-visible frontier: {error}"),
            })?;
        if current_frontier != submission.captured_frontier {
            return Err(self.frontier_conflict_fate(submission.batch_id));
        }

        Ok(())
    }

    fn persist_authoritative_batch_fate<H: Storage>(
        &self,
        storage: &mut H,
        fate: &BatchFate,
    ) -> Result<(), crate::storage::StorageError> {
        storage
            .upsert_authoritative_batch_fate(fate)
            .map_err(|error| {
                tracing::trace!(
                    batch_id = ?fate.batch_id(),
                    %error,
                    "failed to persist authoritative batch fate"
                );
                error
            })
    }

    fn persist_sealed_batch_submission<H: Storage>(
        &self,
        storage: &mut H,
        submission: &SealedBatchSubmission,
    ) -> Result<(), crate::storage::StorageError> {
        storage
            .upsert_sealed_batch_submission(submission)
            .map_err(|error| {
                tracing::trace!(
                    batch_id = ?submission.batch_id,
                    %error,
                    "failed to persist sealed batch submission"
                );
                error
            })
    }

    fn ensure_object_metadata<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
    ) {
        let existing_row_locator = storage.load_row_locator(object_id).ok().flatten();
        if existing_row_locator.is_none()
            && let Some(row_locator) = crate::storage::row_locator_from_metadata(&metadata)
        {
            let _ = storage.put_row_locator(object_id, Some(&row_locator));
        }
    }

    fn row_metadata_from_payload<H: Storage>(
        &self,
        storage: &H,
        row: &StoredRowBatch,
        metadata: Option<&RowMetadata>,
    ) -> Option<HashMap<String, String>> {
        if let Some(metadata) = metadata {
            return Some(metadata.metadata.clone());
        }

        storage
            .load_row_locator(row.row_id)
            .ok()
            .flatten()
            .map(|locator| metadata_from_row_locator(&locator))
    }

    fn matches_replayed_row_batch(existing: &StoredRowBatch, incoming: &StoredRowBatch) -> bool {
        existing.row_id == incoming.row_id
            && existing.batch_id == incoming.batch_id
            && existing.branch == incoming.branch
            && existing.parents == incoming.parents
            && existing.updated_at == incoming.updated_at
            && existing.created_by == incoming.created_by
            && existing.created_at == incoming.created_at
            && existing.updated_by == incoming.updated_by
            && existing.state == incoming.state
            && existing.delete_kind == incoming.delete_kind
            && existing.is_deleted == incoming.is_deleted
            && existing.data == incoming.data
            && existing.metadata == incoming.metadata
    }

    fn pre_batch_visible_row<H: Storage>(
        &self,
        storage: &H,
        table: &str,
        row: &StoredRowBatch,
    ) -> Option<StoredRowBatch> {
        if row.parents.is_empty() {
            return None;
        }

        let context =
            crate::storage::resolve_history_row_write_context(storage, table, row).ok()?;
        let history_rows = storage.scan_history_row_batches(table, row.row_id).ok()?;
        let visible_rows = history_rows
            .into_iter()
            .filter(|candidate| {
                candidate.branch.as_str() == row.branch.as_str()
                    && candidate.batch_id != row.batch_id
                    && candidate.state.is_visible()
            })
            .collect::<Vec<_>>();
        let visible_rows_by_batch = visible_rows
            .iter()
            .cloned()
            .map(|candidate| (candidate.batch_id(), candidate))
            .collect::<HashMap<_, _>>();

        let mut included_batch_ids = HashSet::new();
        let mut frontier = row.parents.iter().copied().collect::<Vec<_>>();
        while let Some(batch_id) = frontier.pop() {
            if !included_batch_ids.insert(batch_id) {
                continue;
            }
            if let Some(parent_row) = visible_rows_by_batch.get(&batch_id) {
                frontier.extend(parent_row.parents.iter().copied());
            }
        }

        let pre_batch_rows = visible_rows
            .into_iter()
            .filter(|candidate| included_batch_ids.contains(&candidate.batch_id()))
            .collect::<Vec<_>>();
        crate::row_histories::visible_row_preview_from_history_rows(
            context.user_descriptor.as_ref(),
            &pre_batch_rows,
            None,
        )
        .ok()
        .flatten()
    }

    fn apply_row_updated<H: Storage>(
        &mut self,
        storage: &mut H,
        metadata: Option<RowMetadata>,
        mut row: StoredRowBatch,
        record_local_fate: bool,
    ) -> Option<AppliedRowBatch> {
        let authoritative_tier = match (row.confirmed_tier, self.max_local_durability_tier()) {
            (Some(incoming), Some(local)) => Some(incoming.max(local)),
            (Some(incoming), None) => Some(incoming),
            (None, Some(local)) => Some(local),
            (None, None) => None,
        };
        row.confirmed_tier = None;

        let metadata = self.row_metadata_from_payload(storage, &row, metadata.as_ref())?;
        self.ensure_object_metadata(storage, row.row_id, metadata.clone());
        let branch_name = BranchName::new(&row.branch);
        let visibility_change =
            match apply_row_batch(storage, row.row_id, &branch_name, row.clone(), &[]) {
                Ok(applied) => applied.visibility_change,
                Err(err) => {
                    tracing::warn!(
                        row_id = %row.row_id,
                        %branch_name,
                        ?err,
                        "failed to apply synced row batch"
                    );
                    return None;
                }
            };
        if record_local_fate
            && let Some(confirmed_tier) = authoritative_tier
            && row.state.is_visible()
        {
            let fate = match row.state {
                RowState::VisibleDirect => BatchFate::DurableDirect {
                    batch_id: row.batch_id,
                    confirmed_tier,
                },
                RowState::VisibleTransactional => BatchFate::AcceptedTransaction {
                    batch_id: row.batch_id,
                    confirmed_tier,
                },
                RowState::StagingPending | RowState::Superseded | RowState::Rejected => {
                    unreachable!("row.state.is_visible() guarded non-visible states")
                }
            };
            if self
                .persist_authoritative_batch_fate(storage, &fate)
                .is_ok()
            {
                self.pending_batch_fates.push(fate.clone());
            }
        }

        Some(AppliedRowBatch {
            metadata,
            row,
            visibility_change,
        })
    }

    pub(super) fn respond_to_batch_fate_request<H: Storage>(
        &mut self,
        storage: &H,
        destination: Destination,
        mut batch_ids: Vec<crate::row_histories::BatchId>,
    ) {
        batch_ids.sort();
        batch_ids.dedup();
        for batch_id in batch_ids {
            let fate = self
                .load_batch_fate_by_batch_id_from_storage(storage, batch_id)
                .unwrap_or(BatchFate::Missing { batch_id });
            match destination {
                Destination::Client(client_id) => {
                    self.queue_batch_fate_to_client_unfiltered(client_id, fate);
                }
                Destination::Server(_) => {
                    self.outbox.push(OutboxEntry {
                        destination: destination.clone(),
                        payload: SyncPayload::BatchFate { fate },
                    });
                }
            }
        }
    }

    pub(super) fn batch_fate_for_client(
        &self,
        client_id: ClientId,
        fate: &BatchFate,
    ) -> Option<BatchFate> {
        self.clients.get(&client_id)?;
        match fate {
            BatchFate::DurableDirect { batch_id, .. }
            | BatchFate::AcceptedTransaction { batch_id, .. }
            | BatchFate::Rejected { batch_id, .. } => self
                .row_batch_interest
                .iter()
                .any(|(key, clients)| key.batch_id == *batch_id && clients.contains(&client_id))
                .then(|| fate.clone()),
            BatchFate::Missing { .. } => Some(fate.clone()),
        }
    }

    pub(super) fn interested_clients_for_batch_fate<H: Storage>(
        &self,
        _storage: &H,
        fate: &BatchFate,
    ) -> HashSet<ClientId> {
        match fate {
            BatchFate::DurableDirect { batch_id, .. }
            | BatchFate::AcceptedTransaction { batch_id, .. }
            | BatchFate::Rejected { batch_id, .. } => {
                let mut interested = HashSet::new();
                for (key, clients) in &self.row_batch_interest {
                    if key.batch_id == *batch_id {
                        interested.extend(clients.iter().copied());
                    }
                }
                interested
            }
            BatchFate::Missing { .. } => HashSet::new(),
        }
    }

    fn transactional_batch_rows<H: Storage>(
        &self,
        storage: &H,
        batch_id: crate::row_histories::BatchId,
        object_ids: &[ObjectId],
    ) -> Vec<(String, StoredRowBatch)> {
        let mut rows = Vec::new();
        for row_id in object_ids {
            let Ok(Some(row_locator)) = storage.load_row_locator(*row_id) else {
                continue;
            };
            let Ok(history_rows) =
                storage.scan_history_row_batches(row_locator.table.as_str(), *row_id)
            else {
                continue;
            };

            for row in history_rows {
                if row.batch_id == batch_id {
                    rows.push((row_locator.table.to_string(), row));
                }
            }
        }

        rows.sort_by(|(_, left), (_, right)| {
            left.row_id
                .uuid()
                .as_bytes()
                .cmp(right.row_id.uuid().as_bytes())
                .then_with(|| left.branch.as_str().cmp(right.branch.as_str()))
                .then_with(|| left.batch_id.0.cmp(&right.batch_id.0))
        });
        rows
    }

    fn known_transactional_batch_rows_for_fate<H: Storage>(
        &self,
        storage: &H,
        batch_id: crate::row_histories::BatchId,
    ) -> Vec<(String, StoredRowBatch)> {
        let mut object_ids = HashSet::new();
        for scope in self.remote_query_scopes.values() {
            object_ids.extend(scope.iter().map(|(object_id, _)| *object_id));
        }
        for key in self.row_batch_interest.keys() {
            if key.batch_id == batch_id {
                object_ids.insert(key.row_id);
            }
        }
        if let Ok(Some(submission)) = storage.load_sealed_batch_submission(batch_id) {
            object_ids.extend(submission.members.iter().map(|member| member.object_id));
        }
        self.transactional_batch_rows(
            storage,
            batch_id,
            &object_ids.into_iter().collect::<Vec<_>>(),
        )
    }

    fn apply_transactional_batch_fate_to_rows<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: Option<ClientId>,
        fate: &BatchFate,
        batch_rows: &[(String, StoredRowBatch)],
    ) {
        let server_ids: Vec<_> = self.servers.keys().copied().collect();
        match fate {
            BatchFate::AcceptedTransaction { confirmed_tier, .. } => {
                for (_table, row) in batch_rows {
                    let row_id = row.row_id;
                    let branch_name = BranchName::new(&row.branch);
                    let accepted_row = row.accepted_transaction_output(*confirmed_tier);
                    let applied =
                        apply_row_batch(storage, row_id, &branch_name, accepted_row.clone(), &[])
                            .ok();

                    let metadata = storage
                        .load_row_locator(row_id)
                        .ok()
                        .flatten()
                        .map(|locator| metadata_from_row_locator(&locator));

                    if let Some(metadata) = metadata {
                        for server_id in &server_ids {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Server(*server_id),
                                payload: SyncPayload::RowBatchNeeded {
                                    metadata: Some(RowMetadata {
                                        id: row_id,
                                        metadata: metadata.clone(),
                                    }),
                                    row: accepted_row.clone(),
                                },
                            });
                        }
                    }

                    if let Some(applied) = applied
                        && let Some(update) = applied.visibility_change
                    {
                        self.pending_row_visibility_changes.push(update);
                        if let Some(client_id) = origin_client_id {
                            self.forward_update_to_clients_except_with_storage(
                                storage,
                                row_id,
                                branch_name,
                                client_id,
                            );
                        } else {
                            self.forward_update_to_clients_with_storage(
                                storage,
                                row_id,
                                branch_name,
                            );
                        }
                    }
                }

                for server_id in &server_ids {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Server(*server_id),
                        payload: SyncPayload::BatchFate { fate: fate.clone() },
                    });
                }
            }
            BatchFate::Rejected { .. } => {
                for (_, row) in batch_rows {
                    let row_id = row.row_id;
                    let branch_name = BranchName::new(&row.branch);
                    let row_batch_id = row.batch_id();

                    let visibility_change = patch_row_batch_state(
                        storage,
                        row_id,
                        &branch_name,
                        row_batch_id,
                        Some(RowState::Rejected),
                        None,
                    )
                    .ok()
                    .flatten();

                    if let Some(update) = visibility_change {
                        self.pending_row_visibility_changes.push(update);
                        if let Some(client_id) = origin_client_id {
                            self.forward_update_to_clients_except_with_storage(
                                storage,
                                row_id,
                                branch_name,
                                client_id,
                            );
                        } else {
                            self.forward_update_to_clients_with_storage(
                                storage,
                                row_id,
                                branch_name,
                            );
                        }
                    }
                }
            }
            BatchFate::DurableDirect { .. } | BatchFate::Missing { .. } => return,
        }

        if let Some(client_id) = origin_client_id {
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::BatchFate { fate: fate.clone() },
            });
        }
    }

    fn apply_authoritative_transaction_fate_for_row<H: Storage>(
        &mut self,
        storage: &mut H,
        row: &StoredRowBatch,
    ) {
        let fate = match storage.load_authoritative_batch_fate(row.batch_id) {
            Ok(Some(fate @ BatchFate::AcceptedTransaction { .. })) => fate,
            Ok(Some(_)) | Ok(None) => return,
            Err(error) => {
                tracing::warn!(
                    batch_id = ?row.batch_id,
                    %error,
                    "failed to load authoritative batch fate for received row"
                );
                return;
            }
        };

        let rows = vec![(
            storage
                .load_row_locator(row.row_id)
                .ok()
                .flatten()
                .map(|locator| locator.table.to_string())
                .unwrap_or_default(),
            row.clone(),
        )];
        self.apply_transactional_batch_fate_to_rows(storage, None, &fate, &rows);
    }

    fn reject_sealed_transactional_batch<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: Option<ClientId>,
        fate: BatchFate,
        batch_rows: &[(String, StoredRowBatch)],
    ) {
        if self
            .persist_authoritative_batch_fate(storage, &fate)
            .is_err()
        {
            return;
        }
        self.pending_batch_fates.push(fate.clone());
        if let Err(error) = storage.delete_sealed_batch_submission(fate.batch_id()) {
            tracing::warn!(
                batch_id = ?fate.batch_id(),
                %error,
                "failed to delete rejected sealed batch submission"
            );
        }
        self.apply_transactional_batch_fate_to_rows(storage, origin_client_id, &fate, batch_rows);
    }

    fn settle_sealed_batch<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: Option<ClientId>,
        submission: SealedBatchSubmission,
        batch_rows: Vec<(String, StoredRowBatch)>,
        mode: SealedBatchMode,
    ) {
        let batch_id = submission.batch_id;
        let declared_rows: Vec<_> = submission
            .members
            .iter()
            .filter_map(|member| {
                batch_rows
                    .iter()
                    .find(|(_, row)| {
                        row.row_id == member.object_id
                            && row.branch.as_str() == submission.target_branch_name.as_str()
                            && row.content_digest() == member.row_digest
                    })
                    .cloned()
            })
            .collect();
        let fate = match storage.load_authoritative_batch_fate(batch_id) {
            Ok(Some(BatchFate::DurableDirect { confirmed_tier, .. }))
                if mode == SealedBatchMode::Direct =>
            {
                let confirmed_tier = self
                    .my_tiers
                    .iter()
                    .copied()
                    .max()
                    .map(|authority_tier| authority_tier.max(confirmed_tier))
                    .unwrap_or(confirmed_tier);
                let fate = BatchFate::DurableDirect {
                    batch_id,
                    confirmed_tier,
                };
                if self
                    .persist_authoritative_batch_fate(storage, &fate)
                    .is_err()
                {
                    return;
                }
                fate
            }
            Ok(Some(existing_fate)) => existing_fate,
            Ok(None) => {
                if batch_rows.is_empty() {
                    BatchFate::Missing { batch_id }
                } else {
                    let Some(confirmed_tier) = self.my_tiers.iter().copied().max() else {
                        return;
                    };
                    let fate = match mode {
                        SealedBatchMode::Direct => BatchFate::DurableDirect {
                            batch_id,
                            confirmed_tier,
                        },
                        SealedBatchMode::Transactional => BatchFate::AcceptedTransaction {
                            batch_id,
                            confirmed_tier,
                        },
                    };
                    if self
                        .persist_authoritative_batch_fate(storage, &fate)
                        .is_err()
                    {
                        return;
                    }
                    fate
                }
            }
            Err(error) => {
                tracing::warn!(?batch_id, %error, "failed to load authoritative batch fate");
                return;
            }
        };

        if !matches!(fate, BatchFate::Missing { .. }) {
            self.pending_batch_fates.push(fate.clone());
            if let Err(error) = storage.delete_sealed_batch_submission(batch_id) {
                tracing::warn!(?batch_id, %error, "failed to delete sealed batch submission");
            }
        }
        if matches!(fate, BatchFate::DurableDirect { .. }) {
            let mut interested_clients = self.interested_clients_for_batch_fate(storage, &fate);
            if let Some(client_id) = origin_client_id {
                self.queue_batch_fate_to_client_unfiltered(client_id, fate.clone());
                interested_clients.remove(&client_id);
            }
            for client_id in interested_clients {
                self.queue_batch_fate_to_client(client_id, fate.clone());
            }
            return;
        }
        let rows_to_patch: &[(String, StoredRowBatch)] = match fate {
            BatchFate::AcceptedTransaction { .. } => &declared_rows,
            BatchFate::Rejected { .. } => &batch_rows,
            BatchFate::DurableDirect { .. } | BatchFate::Missing { .. } => &[],
        };
        self.apply_transactional_batch_fate_to_rows(
            storage,
            origin_client_id,
            &fate,
            rows_to_patch,
        );
    }

    pub(super) fn try_accept_completed_sealed_batch_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        batch_id: crate::row_histories::BatchId,
    ) {
        let submission = match storage.load_sealed_batch_submission(batch_id) {
            Ok(Some(submission)) => submission,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(?batch_id, %error, "failed to load sealed batch submission");
                return;
            }
        };
        match storage.load_authoritative_batch_fate(batch_id) {
            Ok(Some(fate)) => {
                let highest_authority_tier = self.my_tiers.iter().copied().max();
                if matches!(
                    fate,
                    BatchFate::DurableDirect { confirmed_tier, .. }
                        if highest_authority_tier
                            .is_some_and(|authority_tier| confirmed_tier < authority_tier)
                ) {
                    // Continue into seal validation so this authority can promote a
                    // previously local direct fate to its own durability tier.
                } else {
                    let should_prune_submission = matches!(fate, BatchFate::Rejected { .. })
                        || fate
                            .confirmed_tier()
                            .is_some_and(|tier| tier >= DurabilityTier::GlobalServer);
                    let prune_result = if should_prune_submission {
                        storage.delete_sealed_batch_submission(batch_id)
                    } else {
                        Ok(())
                    };
                    if let Err(error) = prune_result {
                        tracing::warn!(
                            ?batch_id,
                            %error,
                            "failed to delete sealed batch submission"
                        );
                    }
                    self.queue_batch_fate_to_client_unfiltered(client_id, fate);
                    return;
                }
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(?batch_id, %error, "failed to load authoritative batch fate");
                return;
            }
        }

        let batch_rows = self.transactional_batch_rows(
            storage,
            batch_id,
            &submission
                .members
                .iter()
                .map(|member| member.object_id)
                .collect::<Vec<_>>(),
        );
        if let Err(rejection) = self.validate_sealed_batch_submission(&submission) {
            self.reject_sealed_transactional_batch(
                storage,
                Some(client_id),
                rejection,
                &batch_rows,
            );
            return;
        }
        if let Err(rejection) = self.validate_batch_rows_target_branch(&submission, &batch_rows) {
            self.reject_sealed_transactional_batch(
                storage,
                Some(client_id),
                rejection,
                &batch_rows,
            );
            return;
        }
        if !submission.members.iter().all(|member| {
            batch_rows.iter().any(|(_, row)| {
                row.row_id == member.object_id
                    && row.branch.as_str() == submission.target_branch_name.as_str()
                    && row.content_digest() == member.row_digest
            })
        }) {
            return;
        }
        let mode = match self.infer_sealed_batch_mode(&submission, &batch_rows) {
            Ok(Some(mode)) => mode,
            Ok(None) => return,
            Err(rejection) => {
                self.reject_sealed_transactional_batch(
                    storage,
                    Some(client_id),
                    rejection,
                    &batch_rows,
                );
                return;
            }
        };
        if mode == SealedBatchMode::Transactional
            && let Err(rejection) = self.validate_captured_frontier(storage, &submission)
        {
            self.reject_sealed_transactional_batch(
                storage,
                Some(client_id),
                rejection,
                &batch_rows,
            );
            return;
        }

        self.settle_sealed_batch(storage, Some(client_id), submission, batch_rows, mode);
    }

    pub(crate) fn recover_completed_sealed_batches_with_storage<H: Storage>(
        &mut self,
        storage: &mut H,
    ) -> bool {
        if self.my_tiers.is_empty() {
            return false;
        }

        let submissions = match storage.scan_sealed_batch_submissions() {
            Ok(submissions) => submissions,
            Err(error) => {
                tracing::warn!(%error, "failed to scan sealed batch submissions for recovery");
                return false;
            }
        };

        let mut recovered_any = false;
        for submission in submissions {
            let batch_rows = self.transactional_batch_rows(
                storage,
                submission.batch_id,
                &submission
                    .members
                    .iter()
                    .map(|member| member.object_id)
                    .collect::<Vec<_>>(),
            );
            if let Err(rejection) = self.validate_sealed_batch_submission(&submission) {
                self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                recovered_any = true;
                continue;
            }
            if let Err(rejection) = self.validate_batch_rows_target_branch(&submission, &batch_rows)
            {
                self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                recovered_any = true;
                continue;
            }
            if !submission.members.iter().all(|member| {
                batch_rows.iter().any(|(_, row)| {
                    row.row_id == member.object_id
                        && row.branch.as_str() == submission.target_branch_name.as_str()
                        && row.content_digest() == member.row_digest
                })
            }) {
                continue;
            }
            let mode = match self.infer_sealed_batch_mode(&submission, &batch_rows) {
                Ok(Some(mode)) => mode,
                Ok(None) => continue,
                Err(rejection) => {
                    self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                    recovered_any = true;
                    continue;
                }
            };
            if mode == SealedBatchMode::Transactional
                && let Err(rejection) = self.validate_captured_frontier(storage, &submission)
            {
                self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                recovered_any = true;
                continue;
            }

            self.settle_sealed_batch(storage, None, submission, batch_rows, mode);
            recovered_any = true;
        }

        recovered_any
    }

    /// Process a single inbox entry.
    pub(super) fn process_inbox_entry<H: Storage>(&mut self, storage: &mut H, entry: InboxEntry) {
        tracing::trace!(source = ?entry.source, payload = entry.payload.variant_name(), "processing inbox entry");
        match entry.source {
            Source::Server(server_id) => {
                self.process_from_server(storage, server_id, entry.payload)
            }
            Source::Client(client_id) => {
                self.process_from_client(storage, client_id, entry.payload)
            }
        }
    }

    /// Process a payload from a server.
    pub(super) fn process_from_server<H: Storage>(
        &mut self,
        storage: &mut H,
        server_id: ServerId,
        payload: SyncPayload,
    ) {
        let _span = tracing::debug_span!("process_from_server", %server_id, payload = payload.variant_name()).entered();
        match payload {
            SyncPayload::CatalogueEntryUpdated { entry } => {
                tracing::debug!(
                    object_id = %entry.object_id,
                    object_type = ?entry.object_type(),
                    "server→CatalogueEntryUpdated"
                );
                if self.persist_catalogue_entry(storage, entry.clone()) {
                    self.pending_catalogue_updates.push(entry.clone());
                    self.forward_catalogue_entry_to_clients(entry, None);
                }
            }
            SyncPayload::RowBatchCreated { metadata, row }
            | SyncPayload::RowBatchNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                let incoming_confirmed_tier = row.confirmed_tier;
                tracing::debug!(
                    %object_id,
                    %branch_name,
                    "server→row-batch payload"
                );
                if let Some(applied) = self.apply_row_updated(storage, metadata, row.clone(), true)
                {
                    self.apply_authoritative_transaction_fate_for_row(storage, &applied.row);

                    let local_tiers = self.my_tiers.iter().copied().collect::<Vec<_>>();
                    for tier in local_tiers {
                        if incoming_confirmed_tier.is_some_and(|confirmed| confirmed >= tier) {
                            continue;
                        }
                        self.queue_batch_durability_ack_to_server(
                            server_id,
                            tier,
                            &applied.row,
                            branch_name,
                        );
                    }

                    if let Some(update) = applied.visibility_change {
                        self.pending_row_visibility_changes.push(update);
                        self.forward_update_to_clients_with_storage(
                            storage,
                            object_id,
                            branch_name,
                        );
                    }
                }
            }
            SyncPayload::BatchFate { fate } => {
                if self
                    .persist_authoritative_batch_fate(storage, &fate)
                    .is_err()
                {
                    return;
                }
                self.pending_batch_fates.push(fate.clone());
                if let BatchFate::AcceptedTransaction { batch_id, .. } = fate {
                    let rows = self.known_transactional_batch_rows_for_fate(storage, batch_id);
                    self.apply_transactional_batch_fate_to_rows(storage, None, &fate, &rows);
                }
                let interested = self.interested_clients_for_batch_fate(storage, &fate);
                for cid in interested {
                    if let Some(fate) = self.batch_fate_for_client(cid, &fate) {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::BatchFate { fate },
                        });
                    }
                }
            }
            SyncPayload::BatchFateNeeded { batch_ids } => {
                self.respond_to_batch_fate_request(
                    storage,
                    Destination::Server(server_id),
                    batch_ids,
                );
            }
            SyncPayload::QuerySettled {
                query_id,
                tier,
                scope,
                through_seq,
            } => {
                let scope_set: HashSet<(ObjectId, BranchName)> = scope.iter().copied().collect();
                let scope_changed = self
                    .remote_query_scopes
                    .get(&(server_id, query_id))
                    .is_none_or(|previous_scope| previous_scope != &scope_set);
                let tier_changed = self
                    .remote_query_scope_tiers
                    .get(&(server_id, query_id))
                    .is_none_or(|previous_tier| *previous_tier != tier);
                self.remote_query_scopes
                    .insert((server_id, query_id), scope_set);
                self.remote_query_scope_tiers
                    .insert((server_id, query_id), tier);
                if scope_changed || tier_changed {
                    self.remote_query_scope_dirty.insert(query_id);
                }

                tracing::debug!(?query_id, "server→QuerySettled");
                // Queue for local QueryManager to process
                self.pending_query_settled.push(PendingQuerySettled {
                    server_id: Some(server_id),
                    query_id,
                    tier,
                    through_seq,
                });

                // RuntimeCore relays this to interested clients once the
                // upstream stream watermark proves the scope's rows are local.
            }
            SyncPayload::SchemaWarning(warning) => {
                super::log_schema_warning(&warning, Some("server"), None);

                if let Some(clients) = self.query_origin.get(&warning.query_id) {
                    for &cid in clients {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::SchemaWarning(warning.clone()),
                        });
                    }
                }
            }
            SyncPayload::ConnectionSchemaDiagnostics(diagnostics) => {
                super::log_connection_schema_diagnostics(&diagnostics, Some("server"));
            }
            SyncPayload::Error(err) => match err {
                SyncError::QuerySubscriptionRejected {
                    query_id,
                    code,
                    reason,
                } => {
                    tracing::warn!(
                        ?server_id,
                        query_id = query_id.0,
                        code = %code,
                        error = %reason,
                        "server rejected query subscription"
                    );
                    self.pending_query_rejections.push(PendingQueryRejection {
                        query_id,
                        code: code.clone(),
                        reason: reason.clone(),
                    });
                }
                _ => {
                    tracing::warn!(?server_id, error = ?err, "error from server");
                }
            },
            // Servers shouldn't send these to us
            SyncPayload::QuerySubscription { .. }
            | SyncPayload::QueryUnsubscription { .. }
            | SyncPayload::SealBatch { .. } => {}
        }
    }

    /// Process a payload from a client.
    pub(super) fn process_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
    ) {
        let _span = tracing::debug_span!("process_from_client", %client_id, payload = payload.variant_name()).entered();
        let Some(client) = self.clients.get(&client_id) else {
            tracing::warn!(%client_id, "message from unknown client, ignoring");
            return;
        };
        tracing::trace!(%client_id, role = ?client.role, payload = payload.variant_name(), "client→payload");

        match &payload {
            SyncPayload::CatalogueEntryUpdated { entry } => {
                let object_id = entry.object_id;
                let branch_name = BranchName::new("main");
                match client.role {
                    ClientRole::Peer | ClientRole::Admin => {
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::Backend => {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                object_id,
                                branch_name,
                            }),
                        });
                    }
                    ClientRole::User => {
                        let Some(_session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        if self.allow_unprivileged_schema_catalogue_writes
                            && entry.is_structural_schema_catalogue()
                        {
                            self.apply_payload_from_client(storage, client_id, payload, false);
                            return;
                        }
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                object_id,
                                branch_name,
                            }),
                        });
                    }
                }
            }
            SyncPayload::RowBatchCreated { metadata, row }
            | SyncPayload::RowBatchNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                match client.role {
                    ClientRole::Peer | ClientRole::Admin => {
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::Backend => {
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::User => {
                        let Some(session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        if payload.is_catalogue() {
                            if self.allow_unprivileged_schema_catalogue_writes
                                && payload.is_structural_schema_catalogue()
                            {
                                self.apply_payload_from_client(storage, client_id, payload, false);
                                return;
                            }
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }

                        let payload_metadata = metadata
                            .as_ref()
                            .map(|meta| meta.metadata.clone())
                            .unwrap_or_default();
                        let (stored_metadata, existing_history_row, pre_batch_visible_row) = self
                            .row_metadata_from_payload(storage, row, metadata.as_ref())
                            .and_then(|stored_metadata| {
                                let table =
                                    stored_metadata.get(MetadataKey::Table.as_str())?.clone();
                                let existing_history_row = storage
                                    .load_history_row_batch(
                                        &table,
                                        &row.branch,
                                        row.row_id,
                                        row.batch_id,
                                    )
                                    .ok()
                                    .flatten();
                                let pre_batch_visible_row =
                                    self.pre_batch_visible_row(storage, &table, row);
                                Some((stored_metadata, existing_history_row, pre_batch_visible_row))
                            })
                            .unwrap_or_else(|| (HashMap::new(), None, None));

                        // Idempotent replay short-circuit: reconnect replays row
                        // history, so only an exact stored row-batch match counts as
                        // a true no-op. Same-batch corrections must still flow
                        // through permission evaluation.
                        if let Some(existing_history_row) = existing_history_row.as_ref()
                            && Self::matches_replayed_row_batch(existing_history_row, row)
                        {
                            self.row_batch_interest
                                .entry(RowBatchKey::from_row(row))
                                .or_default()
                                .insert(client_id);
                            self.try_accept_completed_sealed_batch_from_client(
                                storage,
                                client_id,
                                row.batch_id,
                            );
                            self.pending_client_batch_fates
                                .entry(client_id)
                                .or_default()
                                .insert(row.batch_id);
                            return;
                        }

                        let old_content = pre_batch_visible_row
                            .as_ref()
                            .map(|previous| previous.data.clone());
                        let metadata = if old_content.is_none() && stored_metadata.is_empty() {
                            payload_metadata
                        } else {
                            stored_metadata
                        };
                        let new_content = (!row.is_deleted).then_some(row.data.clone());
                        let operation = if row.is_deleted {
                            Operation::Delete
                        } else if old_content.is_some() || !row.parents.is_empty() {
                            Operation::Update
                        } else {
                            Operation::Insert
                        };
                        self.queue_for_permission_check(
                            client_id,
                            payload,
                            session.clone(),
                            metadata,
                            old_content.map(|content| content.to_vec()),
                            new_content.map(|content| content.to_vec()),
                            operation,
                        );
                    }
                }
            }
            SyncPayload::SealBatch { .. } => {
                self.apply_payload_from_client(storage, client_id, payload, false);
            }
            // Handle query subscription with full Query struct
            // Queue for QueryManager to process (SyncManager doesn't know about QueryGraph)
            SyncPayload::QuerySubscription {
                query_id,
                query,
                session,
                required_tier,
                propagation,
                policy_context_tables,
            } => {
                // Build effective session: identity (user_id) comes from the
                // server-established session (set during the SSE auth handshake) and
                // cannot be overridden by the payload. However, ephemeral per-subscription
                // claims supplied in the payload — such as a join_code for invite flows —
                // are merged in when the user_id matches, so that policy conditions like
                // `claims.join_code` evaluate correctly for this subscription.
                let effective_session = match (&client.session, session) {
                    (Some(client_session), Some(payload_session)) => {
                        if client_session.user_id != payload_session.user_id {
                            tracing::warn!(
                                %client_id,
                                "QuerySubscription payload session user_id does not match client session; ignoring payload session"
                            );
                            Some(client_session.clone())
                        } else {
                            // Same user: merge claims. Payload provides ephemeral claims
                            // (e.g. join_code); client session claims take precedence so
                            // auth-established values cannot be spoofed.
                            let merged_claims = if let (
                                serde_json::Value::Object(client_map),
                                serde_json::Value::Object(payload_map),
                            ) =
                                (&client_session.claims, &payload_session.claims)
                            {
                                let mut merged = payload_map.clone();
                                merged.extend(client_map.clone());
                                serde_json::Value::Object(merged)
                            } else {
                                client_session.claims.clone()
                            };
                            Some(Session {
                                user_id: client_session.user_id.clone(),
                                claims: merged_claims,
                                auth_mode: client_session.auth_mode,
                            })
                        }
                    }
                    (Some(client_session), None) => Some(client_session.clone()),
                    (None, payload_session) => payload_session.clone(),
                };
                // Track origin for QuerySettled relay
                self.query_origin
                    .entry(*query_id)
                    .or_default()
                    .insert(client_id);
                tracing::trace!(
                    %client_id,
                    query_id = query_id.0,
                    table = %query.table,
                    ?propagation,
                    "jazz trace received query subscription from client"
                );
                self.pending_query_subscriptions
                    .push(PendingQuerySubscription {
                        client_id,
                        query_id: *query_id,
                        query: query.as_ref().clone(),
                        session: effective_session,
                        required_tier: *required_tier,
                        propagation: *propagation,
                        policy_context_tables: policy_context_tables.clone(),
                    });
            }
            // Handle query unsubscription
            // Queue for QueryManager to process (remove server-side QueryGraph, forward upstream)
            SyncPayload::QueryUnsubscription { query_id } => {
                tracing::trace!(
                    %client_id,
                    query_id = query_id.0,
                    "jazz trace received query unsubscription from client"
                );
                // Clean up query origin
                if let Some(clients) = self.query_origin.get_mut(query_id) {
                    clients.remove(&client_id);
                    if clients.is_empty() {
                        self.query_origin.remove(query_id);
                    }
                }
                self.pending_query_unsubscriptions
                    .push(PendingQueryUnsubscription {
                        client_id,
                        query_id: *query_id,
                    });
            }
            SyncPayload::BatchFate { fate } => {
                self.retain_client_batch_fate(storage, fate);
                self.pending_batch_fates.push(fate.clone());
            }
            SyncPayload::BatchFateNeeded { batch_ids } => {
                self.respond_to_batch_fate_request(
                    storage,
                    Destination::Client(client_id),
                    batch_ids.clone(),
                );
            }
            SyncPayload::QuerySettled {
                query_id,
                tier,
                scope: _,
                through_seq,
            } => {
                // Client relaying a QuerySettled from downstream
                self.pending_query_settled.push(PendingQuerySettled {
                    server_id: None,
                    query_id: *query_id,
                    tier: *tier,
                    through_seq: *through_seq,
                });
            }
            SyncPayload::SchemaWarning(warning) => {
                tracing::warn!(
                    %client_id,
                    query_id = warning.query_id.0,
                    "client attempted to send SchemaWarning payload; ignoring"
                );
            }
            SyncPayload::ConnectionSchemaDiagnostics(_) => {
                tracing::warn!(
                    %client_id,
                    "client attempted to send ConnectionSchemaDiagnostics payload; ignoring"
                );
            }
            // Clients shouldn't send these
            SyncPayload::Error(_) => {}
        }
    }

    /// Apply a payload from a client (either directly or after approval).
    pub(super) fn apply_payload_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
        _was_pending: bool,
    ) {
        match payload {
            SyncPayload::CatalogueEntryUpdated { entry } => {
                if self.persist_catalogue_entry(storage, entry.clone()) {
                    self.pending_catalogue_updates.push(entry.clone());
                    self.forward_catalogue_entry_to_servers(entry.clone());
                    self.forward_catalogue_entry_to_clients(entry, Some(client_id));
                }
            }
            SyncPayload::RowBatchCreated { metadata, row }
            | SyncPayload::RowBatchNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                let batch_id = row.batch_id;
                self.row_batch_interest
                    .entry(RowBatchKey::new(object_id, branch_name, batch_id))
                    .or_default()
                    .insert(client_id);

                if let Some(applied) = self.apply_row_updated(storage, metadata, row.clone(), false)
                {
                    self.forward_row_batch_to_servers(
                        storage,
                        object_id,
                        applied.metadata.clone(),
                        row,
                    );
                    if !matches!(
                        applied.row.state,
                        RowState::StagingPending | RowState::Superseded
                    ) && let Some(update) = applied.visibility_change
                    {
                        self.pending_row_visibility_changes.push(update);
                        self.forward_update_to_clients_except_with_storage(
                            storage,
                            object_id,
                            branch_name,
                            client_id,
                        );
                    }
                }
                self.try_accept_completed_sealed_batch_from_client(storage, client_id, batch_id);
            }
            SyncPayload::SealBatch { submission } => {
                if submission.members.is_empty() {
                    tracing::warn!(batch_id = ?submission.batch_id, "ignoring SealBatch with no declared members");
                    return;
                }
                match storage.load_authoritative_batch_fate(submission.batch_id) {
                    Ok(Some(fate @ BatchFate::Rejected { .. }))
                    | Ok(Some(fate @ BatchFate::AcceptedTransaction { .. }))
                    | Ok(Some(fate @ BatchFate::Missing { .. })) => {
                        self.queue_batch_fate_to_client(client_id, fate);
                        return;
                    }
                    Ok(Some(BatchFate::DurableDirect { .. })) => {}
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(
                            batch_id = ?submission.batch_id,
                            %error,
                            "failed to load authoritative batch fate"
                        );
                        return;
                    }
                }
                if let Err(rejection) = self.validate_sealed_batch_submission(&submission) {
                    let batch_rows = self.transactional_batch_rows(
                        storage,
                        submission.batch_id,
                        &submission
                            .members
                            .iter()
                            .map(|member| member.object_id)
                            .collect::<Vec<_>>(),
                    );
                    self.reject_sealed_transactional_batch(
                        storage,
                        Some(client_id),
                        rejection,
                        &batch_rows,
                    );
                    return;
                }
                if let Err(error) = self.persist_sealed_batch_submission(storage, &submission) {
                    tracing::warn!(
                        batch_id = ?submission.batch_id,
                        %error,
                        "failed to persist sealed batch submission"
                    );
                    return;
                }
                self.seal_batch_to_servers(submission.clone());
                self.try_accept_completed_sealed_batch_from_client(
                    storage,
                    client_id,
                    submission.batch_id,
                );
            }
            SyncPayload::BatchFate { fate } => {
                self.retain_client_batch_fate(storage, &fate);
                self.pending_batch_fates.push(fate.clone());
            }
            SyncPayload::BatchFateNeeded { batch_ids } => {
                self.respond_to_batch_fate_request(
                    storage,
                    Destination::Client(client_id),
                    batch_ids,
                );
            }
            _ => {}
        }
    }
}
