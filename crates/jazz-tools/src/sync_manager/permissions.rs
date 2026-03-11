use super::*;
use crate::query_manager::policy::Operation;
use crate::query_manager::session::Session;
use crate::storage::Storage;
use std::collections::HashMap;

impl SyncManager {
    /// Take all pending permission checks for policy evaluation.
    ///
    /// Called by QueryManager to get writes that need permission evaluation.
    pub fn take_pending_permission_checks(&mut self) -> Vec<PendingPermissionCheck> {
        std::mem::take(&mut self.pending_permission_checks)
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
    pub fn reject_permission_check(&mut self, check: PendingPermissionCheck, reason: String) {
        let Some(operation) = MutationOperation::from_write_operation(check.operation) else {
            return;
        };
        let payload = &check.payload;
        let Some(mut rejection) = self.build_mutation_rejection_from_payload(
            payload,
            MutationRejectCode::PermissionDenied,
            reason,
        ) else {
            return;
        };
        rejection.operation = operation;

        self.outbox.push(OutboxEntry {
            destination: Destination::Client(check.client_id),
            payload: SyncPayload::MutationOutcome(MutationOutcome::Rejected(rejection)),
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
            metadata,
            old_content,
            new_content,
            operation,
        });
        id
    }
}
