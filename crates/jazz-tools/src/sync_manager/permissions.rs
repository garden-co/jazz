use super::*;
use crate::batch_fate::BatchSettlement;
use crate::query_manager::policy::Operation;
use crate::query_manager::session::Session;
use crate::row_histories::RowState;
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
        self.apply_payload_from_client(storage, check.client_id, check.payload, true);
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
        if let SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } =
            &check.payload
            && matches!(row.state, RowState::StagingPending)
        {
            let settlement = BatchSettlement::Rejected {
                batch_id: row.batch_id,
                code: "permission_denied".to_string(),
                reason: reason.clone(),
            };
            if let Err(error) = storage.upsert_authoritative_batch_settlement(&settlement) {
                tracing::warn!(
                    batch_id = ?row.batch_id,
                    %error,
                    "failed to persist rejected transactional batch settlement"
                );
            }
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(check.client_id),
                payload: SyncPayload::RowBatchStateChanged {
                    row_id: row.row_id,
                    branch_name: crate::object::BranchName::new(&row.branch),
                    batch_id: row.batch_id,
                    state: Some(RowState::Rejected),
                    confirmed_tier: None,
                },
            });
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(check.client_id),
                payload: SyncPayload::BatchSettlement { settlement },
            });
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
}
