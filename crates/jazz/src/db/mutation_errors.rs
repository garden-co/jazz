//! One reporting queue for local validation failures and authority rejections.
//!
//! Application waits own first refusal while alive. Consuming a failure removes
//! its fallback event; cancelling a wait releases it for callback delivery.
//! Write-state observers (including internal persistence waits) do not own errors.

use super::*;

pub(super) fn mutation_error_event(rejected: crate::tx::RejectedTransaction) -> MutationErrorEvent {
    let tx_id = rejected.tx_id();
    mutation_error_event_for(tx_id, rejected.kind(), &rejected.reason())
}

pub(super) fn mutation_error_event_for(
    tx_id: TxId,
    kind: TxKind,
    rejection: &RejectionReason,
) -> MutationErrorEvent {
    let transaction_id = TransactionId::from_committed_tx(tx_id);
    let (code, reason) = mutation_error_details(rejection);
    mutation_error_event_with_details(transaction_id, kind, code, reason)
}

pub(super) fn queued_mutation_error_event(
    tx_id: TxId,
    kind: TxKind,
    error: &Error,
) -> MutationErrorEvent {
    let code = match error.code {
        ErrorCode::Schema => "schema",
        ErrorCode::Query => "query",
        ErrorCode::WriteRejected => "write_rejected",
        ErrorCode::TransactionConflict => "transaction_conflict",
        ErrorCode::Storage => "storage",
        ErrorCode::Protocol => "protocol",
        ErrorCode::Backpressure => "backpressure",
        ErrorCode::NotObserved => "not_observed",
        ErrorCode::HistoricalReadRequiresServer => "historical_read_requires_server",
    };
    mutation_error_event_with_details(
        TransactionId::from_committed_tx(tx_id),
        kind,
        code.to_owned(),
        error.message.clone(),
    )
}

fn mutation_error_event_with_details(
    transaction_id: TransactionId,
    kind: TxKind,
    code: String,
    reason: String,
) -> MutationErrorEvent {
    MutationErrorEvent {
        code: code.clone(),
        reason: reason.clone(),
        transaction: LocalTransactionRecord {
            transaction_id,
            kind: kind.into(),
            sealed: true,
            latest_settlement: TransactionFate::Rejected {
                transaction_id,
                code,
                reason,
            },
        },
    }
}

fn mutation_error_details(reason: &RejectionReason) -> (String, String) {
    match reason {
        RejectionReason::ClientClockTooFarAhead => (
            "client_clock_too_far_ahead".to_owned(),
            "Client clock is too far ahead".to_owned(),
        ),
        RejectionReason::AuthorizationDenied => (
            "permission_denied".to_owned(),
            "Write rejected by server authorization".to_owned(),
        ),
        RejectionReason::ExclusiveConflict => (
            "exclusive_conflict".to_owned(),
            "Exclusive transaction conflicted with another write".to_owned(),
        ),
        RejectionReason::CausalityViolation => (
            "causality_violation".to_owned(),
            "Transaction violated causal ordering".to_owned(),
        ),
        RejectionReason::Cascade { root } => (
            "cascade_rejected".to_owned(),
            format!("Transaction was rejected because ancestor {root:?} was rejected"),
        ),
        RejectionReason::MalformedCommit(reason) => (
            "write_rejected".to_owned(),
            format!("Malformed transaction: {reason}"),
        ),
    }
}

pub(super) fn queue_mutation_error(
    errors: &SharedMutationErrors,
    scheduler: &SharedTickScheduler,
    tx_id: TxId,
    event: MutationErrorEvent,
) {
    let mut state = errors.borrow_mut();
    state.pending.entry(tx_id).or_insert(event);
    let has_callback = state.callback.is_some();
    drop(state);
    if has_callback {
        schedule_tick_in(scheduler, TickUrgency::Immediate);
    }
}

pub(super) struct MutationErrorWait {
    errors: SharedMutationErrors,
    scheduler: SharedTickScheduler,
    tx_id: TxId,
}

impl MutationErrorWait {
    pub(super) fn new(
        errors: &SharedMutationErrors,
        scheduler: &SharedTickScheduler,
        tx_id: TxId,
    ) -> Self {
        *errors
            .borrow_mut()
            .application_waiters
            .entry(tx_id)
            .or_default() += 1;
        Self {
            errors: Rc::clone(errors),
            scheduler: Rc::clone(scheduler),
            tx_id,
        }
    }
}

impl Drop for MutationErrorWait {
    fn drop(&mut self) {
        let mut state = self.errors.borrow_mut();
        let count = state
            .application_waiters
            .get_mut(&self.tx_id)
            .expect("registered application waiter");
        *count -= 1;
        let released = *count == 0;
        if released {
            state.application_waiters.remove(&self.tx_id);
        }
        let needs_delivery =
            released && state.callback.is_some() && state.pending.contains_key(&self.tx_id);
        drop(state);
        if needs_delivery {
            schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
        }
    }
}

pub(super) fn take_pending_mutation_error_delivery(
    errors: &SharedMutationErrors,
) -> Option<(MutationErrorCallback, BTreeMap<TxId, MutationErrorEvent>)> {
    let mut state = errors.borrow_mut();
    let callback = state.callback.clone()?;
    let pending = std::mem::take(&mut state.pending);
    let (claimed, unclaimed) = pending
        .into_iter()
        .partition(|(tx_id, _)| state.application_waiters.contains_key(tx_id));
    state.pending = claimed;
    Some((callback, unclaimed))
}
