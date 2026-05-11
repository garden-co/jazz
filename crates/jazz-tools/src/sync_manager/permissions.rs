use super::*;
use crate::batch_fate::BatchFate;
use crate::query_manager::policy::Operation;
use crate::query_manager::session::Session;
use crate::row_histories::{
    BatchId, RowState, RowVisibilityChange, StoredRowBatch, VisibleRowEntry, patch_row_batch_state,
};
use crate::storage::Storage;
use std::collections::{HashMap, HashSet};

impl SyncManager {
    /// Take all pending permission checks for policy evaluation.
    ///
    /// Called by QueryManager to get writes that need permission evaluation.
    pub fn take_pending_permission_checks(&mut self) -> Vec<PendingPermissionCheck> {
        std::mem::take(&mut self.pending_permission_checks)
    }

    /// Re-queue permission checks that could not be evaluated yet.
    pub fn requeue_pending_permission_checks(&mut self, checks: Vec<PendingPermissionCheck>) {
        self.pending_permission_checks.extend(checks);
    }

    /// Approve a pending permission check, applying the payload.
    ///
    /// This takes the full PendingPermissionCheck since it was already taken
    /// from the queue by take_pending_permission_checks().
    pub fn approve_permission_check<H: Storage>(
        &mut self,
        storage: &mut H,
        check: PendingPermissionCheck,
    ) {
        let batch_id = match &check.payload {
            SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } => {
                Some(row.batch_id)
            }
            _ => None,
        };
        if let Some(batch_id) = batch_id
            && let Some(fate) = self.load_rejected_batch_fate(storage, batch_id)
        {
            self.queue_batch_fate_to_client(check.client_id, fate);
            return;
        }
        self.apply_payload_from_client(storage, check.client_id, check.payload, true);
        if let Some(batch_id) = batch_id {
            self.try_accept_completed_sealed_batch_from_client(storage, check.client_id, batch_id);
        }
    }

    /// Reject a pending permission check, sending error back to client.
    ///
    /// This takes the full PendingPermissionCheck since it was already taken
    /// from the queue by take_pending_permission_checks().
    pub fn reject_permission_check<H: Storage>(
        &mut self,
        storage: &mut H,
        check: PendingPermissionCheck,
        reason: String,
    ) {
        self.reject_permission_check_with_code(
            storage,
            check,
            "permission_denied".to_string(),
            reason,
        );
    }

    pub fn reject_permission_check_with_code<H: Storage>(
        &mut self,
        storage: &mut H,
        check: PendingPermissionCheck,
        code: String,
        reason: String,
    ) {
        if let SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } =
            &check.payload
            && matches!(
                row.state,
                RowState::StagingPending | RowState::VisibleDirect
            )
        {
            let fate = BatchFate::Rejected {
                batch_id: row.batch_id,
                code: code.clone(),
                reason: reason.clone(),
            };
            self.reject_permission_batch(storage, check.client_id, fate, row.clone());
            return;
        }

        let Some(object_id) = check.payload.object_id() else {
            return;
        };
        let Some(branch_name) = check.payload.branch_name() else {
            return;
        };

        self.outbox.push(OutboxEntry {
            destination: Destination::Client(check.client_id),
            payload: SyncPayload::Error(SyncError::PermissionDenied {
                object_id,
                branch_name,
                code,
                reason,
            }),
        });
    }

    /// Queue a payload for permission checking.
    ///
    /// Called internally when a client write needs policy evaluation.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn queue_for_permission_check(
        &mut self,
        client_id: ClientId,
        payload: SyncPayload,
        session: Session,
        metadata: HashMap<String, String>,
        old_content: Option<Vec<u8>>,
        new_content: Option<Vec<u8>>,
        operation: Operation,
    ) -> PendingUpdateId {
        let id = PendingUpdateId(self.next_pending_id);
        self.next_pending_id += 1;
        self.pending_permission_checks.push(PendingPermissionCheck {
            id,
            client_id,
            payload,
            session,
            schema_wait_started_at: None,
            metadata,
            old_content,
            new_content,
            operation,
        });
        id
    }

    fn load_rejected_batch_fate<H: Storage>(
        &self,
        storage: &H,
        batch_id: BatchId,
    ) -> Option<BatchFate> {
        match storage.load_authoritative_batch_fate(batch_id) {
            Ok(Some(fate @ BatchFate::Rejected { .. })) => Some(fate),
            _ => None,
        }
    }

    fn local_batch_rows<H: Storage>(
        &self,
        storage: &H,
        batch_id: BatchId,
        fallback_row: StoredRowBatch,
    ) -> Vec<StoredRowBatch> {
        let mut row_ids = HashSet::new();
        for key in self.row_batch_interest.keys() {
            if key.batch_id == batch_id {
                row_ids.insert(key.row_id);
            }
        }
        for pending in &self.pending_permission_checks {
            if let SyncPayload::RowBatchCreated { row, .. }
            | SyncPayload::RowBatchNeeded { row, .. } = &pending.payload
                && row.batch_id == batch_id
            {
                row_ids.insert(row.row_id);
            }
        }

        let mut rows = row_ids
            .into_iter()
            .filter_map(|row_id| {
                let table = storage.load_row_locator(row_id).ok().flatten()?.table;
                storage
                    .scan_history_row_batches(table.as_str(), row_id)
                    .ok()?
                    .into_iter()
                    .find(|row| row.batch_id == batch_id)
            })
            .collect::<Vec<_>>();

        rows.extend(
            storage
                .load_sealed_batch_submission(batch_id)
                .ok()
                .flatten()
                .map(|submission| {
                    submission
                        .members
                        .into_iter()
                        .filter_map(|member| {
                            let table = storage
                                .load_row_locator(member.object_id)
                                .ok()
                                .flatten()?
                                .table;
                            storage
                                .scan_history_row_batches(table.as_str(), member.object_id)
                                .ok()?
                                .into_iter()
                                .find(|row| {
                                    row.batch_id == batch_id
                                        && row.branch.as_str()
                                            == submission.target_branch_name.as_str()
                                        && row.content_digest() == member.row_digest
                                })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        );

        if !rows.iter().any(|row| {
            row.row_id == fallback_row.row_id
                && row.branch == fallback_row.branch
                && row.batch_id == fallback_row.batch_id
        }) {
            rows.push(fallback_row);
        }
        rows.sort_by(|left, right| {
            left.row_id
                .uuid()
                .as_bytes()
                .cmp(right.row_id.uuid().as_bytes())
                .then_with(|| left.branch.as_str().cmp(right.branch.as_str()))
                .then_with(|| left.batch_id.0.cmp(&right.batch_id.0))
        });
        rows.dedup_by(|left, right| {
            left.row_id == right.row_id
                && left.branch == right.branch
                && left.batch_id == right.batch_id
        });
        rows
    }

    fn reject_permission_batch<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: ClientId,
        fate: BatchFate,
        fallback_row: StoredRowBatch,
    ) {
        let batch_id = fate.batch_id();
        if let Err(error) = storage.upsert_authoritative_batch_fate(&fate) {
            tracing::warn!(
                ?batch_id,
                %error,
                "failed to persist rejected batch fate"
            );
            return;
        }

        let _ = storage.delete_sealed_batch_submission(batch_id);
        self.pending_permission_checks.retain(|pending| {
            !matches!(
                &pending.payload,
                SyncPayload::RowBatchCreated { row, .. }
                    | SyncPayload::RowBatchNeeded { row, .. }
                    if row.batch_id == batch_id
            )
        });
        self.pending_batch_fates.push(fate.clone());

        for row in self.local_batch_rows(storage, batch_id, fallback_row) {
            let branch_name = BranchName::new(&row.branch);
            let was_visible_delete = row.state.is_visible() && row.delete_kind.is_some();
            let visibility_change = patch_row_batch_state(
                storage,
                row.row_id,
                &branch_name,
                row.batch_id,
                Some(RowState::Rejected),
                None,
            )
            .ok()
            .flatten();

            if let Some(update) = visibility_change {
                self.pending_row_visibility_changes.push(update);
                self.forward_update_to_clients_except_with_storage(
                    storage,
                    row.row_id,
                    branch_name,
                    origin_client_id,
                );
            }
            if was_visible_delete
                && let Some(update) =
                    self.restore_permission_rejected_delete_row(storage, row.clone())
            {
                let restored_branch_name = BranchName::new(&update.row.branch);
                self.pending_row_visibility_changes.push(update.clone());
                self.forward_update_to_clients_except_with_storage(
                    storage,
                    update.object_id,
                    restored_branch_name,
                    origin_client_id,
                );
            }
        }

        let mut clients = self.interested_clients_for_batch_fate(storage, &fate);
        self.queue_batch_fate_to_client_unfiltered(origin_client_id, fate.clone());
        clients.remove(&origin_client_id);
        for client_id in clients {
            self.queue_batch_fate_to_client(client_id, fate.clone());
        }
    }

    fn restore_permission_rejected_delete_row<H: Storage>(
        &self,
        storage: &mut H,
        rejected_delete_row: StoredRowBatch,
    ) -> Option<RowVisibilityChange> {
        let row_locator = storage
            .load_row_locator(rejected_delete_row.row_id)
            .ok()
            .flatten()?;
        let table = row_locator.table.to_string();
        let mut restored_row = storage
            .scan_history_row_batches(&table, rejected_delete_row.row_id)
            .ok()?
            .into_iter()
            .filter(|row| {
                row.branch == rejected_delete_row.branch
                    && !matches!(row.state, RowState::Rejected)
                    && row.delete_kind.is_none()
            })
            .max_by_key(|row| row.updated_at)?;

        restored_row.state = RowState::VisibleDirect;
        let visible_entry = VisibleRowEntry::new(restored_row.clone());
        if let Err(error) = storage.apply_row_mutation(
            &table,
            std::slice::from_ref(&restored_row),
            std::slice::from_ref(&visible_entry),
            &[],
        ) {
            tracing::warn!(
                table,
                branch = restored_row.branch.as_str(),
                object_id = %restored_row.row_id,
                %error,
                "failed to restore permission-rejected delete visible row"
            );
            return None;
        }

        Some(RowVisibilityChange {
            object_id: restored_row.row_id,
            row_locator,
            row: restored_row,
            previous_row: Some(rejected_delete_row),
            is_new_object: false,
        })
    }
}
