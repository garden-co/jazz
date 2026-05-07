use super::*;
use crate::batch_fate::BatchFate;
use crate::query_manager::policy::Operation;
use crate::query_manager::session::Session;
use crate::row_histories::{BatchId, RowState, StoredRowBatch, patch_row_batch_state};
use crate::storage::Storage;
use std::collections::HashMap;

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
        let mut rows = storage
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
                                    && row.branch.as_str() == submission.target_branch_name.as_str()
                                    && row.content_digest() == member.row_digest
                            })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        if !rows.iter().any(|row| {
            row.row_id == fallback_row.row_id
                && row.branch == fallback_row.branch
                && row.batch_id == fallback_row.batch_id
        }) {
            rows.push(fallback_row);
        }
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
        }

        let mut clients = self.interested_clients_for_batch_fate(storage, &fate);
        self.queue_batch_fate_to_client_unfiltered(origin_client_id, fate.clone());
        clients.remove(&origin_client_id);
        for client_id in clients {
            self.queue_batch_fate_to_client(client_id, fate.clone());
        }
    }
}
