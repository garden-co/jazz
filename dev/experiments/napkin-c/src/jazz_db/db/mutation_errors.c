/* mutation_errors.c: crates/jazz/layers/db/src/db/mutation_errors.rs
 * One reporting queue for local validation failures and authority rejections. */
#include "mutation_errors.h"

static void mutation_error_event_with_details(jazz_db_MutationErrorEvent *out, jazz_model_TransactionId transaction_id, u32 kind,
                                              jz_str code, jz_str reason) {
    out->code = jz_str_clone(&code);
    out->reason = jz_str_clone(&reason);
    out->transaction.transaction_id = transaction_id;
    out->transaction.kind = jazz_db__db__TransactionKind__from_tx_kind(kind);
    out->transaction.sealed = true;
    out->transaction.latest_settlement.transaction_id = transaction_id;
    out->transaction.latest_settlement.code = code;
    out->transaction.latest_settlement.reason = reason;
}

void MERR_FN(mutation_error_event)(jazz_db_MutationErrorEvent *out, jazz_model_RejectedTransaction *rejected) {
    jazz_model_TxId tx_id = jazz_model__tx__RejectedTransaction__tx_id(rejected);
    jazz_model_RejectionReason reason;
    jazz_model__tx__RejectedTransaction__reason(rejected, &reason);
    MERR_FN(mutation_error_event_for)(out, tx_id, jazz_model__tx__RejectedTransaction__kind(rejected), &reason);
    jazz_model__tx__RejectionReason__drop(&reason);
    jazz_model__tx__RejectedTransaction__drop(rejected);
}

void MERR_FN(mutation_error_event_for)(jazz_db_MutationErrorEvent *out, jazz_model_TxId tx_id, u32 kind, const jazz_model_RejectionReason *rejection) {
    jazz_model_TransactionId transaction_id;
    jazz_model__model__public_schema__TransactionId__from_committed_tx(&transaction_id, tx_id);
    jz_str code, reason;
    MERR_FN(mutation_error_details)(rejection, &code, &reason);
    mutation_error_event_with_details(out, transaction_id, kind, code, reason);
}

void MERR_FN(queued_mutation_error_event)(jazz_db_MutationErrorEvent *out, jazz_model_TxId tx_id, u32 kind, const jz_err *error) {
    const char *code = jazz_node__node__api_error__ErrorCode__as_str(error->code);
    jazz_model_TransactionId transaction_id;
    jazz_model__model__public_schema__TransactionId__from_committed_tx(&transaction_id, tx_id);
    jz_str message = error->msg ? jz_str_clone(error->msg) : jz_str_from("");
    mutation_error_event_with_details(out, transaction_id, kind, jz_str_from(code), message);
}

static bool str_ends_with(const jz_str *s, const char *suffix) {
    u32 n = (u32)strlen(suffix);
    return s->len >= n && memcmp(s->ptr + s->len - n, suffix, n) == 0;
}

void MERR_FN(mutation_error_details)(const jazz_model_RejectionReason *reason, jz_str *code, jz_str *message) {
    switch (reason->tag) {
    case JAZZ_REJ_CLIENT_CLOCK_TOO_FAR_AHEAD:
        *code = jz_str_from("client_clock_too_far_ahead");
        *message = jz_str_from("Client clock is too far ahead");
        return;
    case JAZZ_REJ_AUTHORIZATION_DENIED:
        *code = jz_str_from("permission_denied");
        *message = jz_str_from("Write rejected by server authorization");
        return;
    case JAZZ_REJ_EXCLUSIVE_CONFLICT:
        *code = jz_str_from("exclusive_conflict");
        *message = jz_str_from("Exclusive transaction conflicted with another write");
        return;
    case JAZZ_REJ_CAUSALITY_VIOLATION:
        *code = jz_str_from("causality_violation");
        *message = jz_str_from("Transaction violated causal ordering");
        return;
    case JAZZ_REJ_CASCADE: {
        jazz_model_TransactionId root;
        jazz_model__model__public_schema__TransactionId__from_committed_tx(&root, reason->root);
        jz_str id = {0};
        jazz_model__model__public_schema__TransactionId__to_string(&root, &id);
        *code = jz_str_from("cascade_rejected");
        *message = jz_format("Transaction was rejected because ancestor transaction %.*s was rejected", (int)id.len, id.ptr);
        jz_str_drop(&id);
        return;
    }
    default:
        /* MalformedCommit: "... is not supported yet" is reported verbatim. */
        *code = jz_str_from("write_rejected");
        if (str_ends_with(&reason->detail, "is not supported yet")) *message = jz_str_clone(&reason->detail);
        else *message = jz_format("Malformed transaction: %.*s", (int)reason->detail.len, reason->detail.ptr);
        return;
    }
}

void MERR_FN(queue_mutation_error)(jazz_db_SharedMutationErrors errors, jazz_db_SharedTickScheduler scheduler, jazz_model_TxId tx_id,
                                   jazz_db_MutationErrorEvent event) {
    bool inserted;
    jazz_db_MutationErrorEvent *slot = jz_btree_entry(&errors->pending, &tx_id, &inserted);
    if (inserted) *slot = event;
    else jazz_db__db__MutationErrorEvent__drop(&event);
    bool has_callback = errors->has_callback;
    if (has_callback) jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_IMMEDIATE);
}

void jazz_db__db__mutation_errors__MutationErrorWait__new(jazz_db_MutationErrorWait *out, jazz_db_SharedMutationErrors errors,
                                                         jazz_db_SharedTickScheduler scheduler, jazz_model_TxId tx_id) {
    bool inserted;
    usize *count = jz_btree_entry(&errors->application_waiters, &tx_id, &inserted);
    if (inserted) *count = 0;
    *count += 1;
    out->errors = jz_rc_clone(errors);
    out->scheduler = jz_rc_clone(scheduler);
    out->tx_id = tx_id;
}

void jazz_db__db__mutation_errors__MutationErrorWait__Drop__drop(jazz_db_MutationErrorWait *self) {
    jazz_db_MutationErrorState *state = self->errors;
    usize *count = jz_btree_get(&state->application_waiters, &self->tx_id);
    if (!count) jz_panic("registered application waiter");
    *count -= 1;
    bool released = *count == 0;
    if (released) jz_btree_remove(&state->application_waiters, &self->tx_id, NULL, NULL);
    bool needs_delivery = released && state->has_callback && jz_btree_contains(&state->pending, &self->tx_id);
    if (needs_delivery) jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
    jz_rc_release(self->errors);
    jz_rc_release(self->scheduler);
}

bool MERR_FN(take_pending_mutation_error_delivery)(jazz_db_SharedMutationErrors errors, jazz_db_MutationErrorCallback *callback, jz_btree *unclaimed) {
    jazz_db_MutationErrorState *state = errors;
    if (!state->has_callback) return false;
    *callback = jazz_db__db__MutationErrorCallback__clone(&state->callback);
    jz_btree pending = state->pending;
    jz_btree_init(&state->pending, &jazz_db_mutation_error_pending_desc);
    jz_btree claimed;
    jz_btree_init(&claimed, &jazz_db_mutation_error_pending_desc);
    jz_btree_init(unclaimed, &jazz_db_mutation_error_pending_desc);
    jazz_model_TxId tx_id;
    jazz_db_MutationErrorEvent event;
    while (jz_btree_pop_first(&pending, &tx_id, &event)) {
        if (jz_btree_contains(&state->application_waiters, &tx_id)) jz_btree_insert(&claimed, &tx_id, &event, NULL);
        else jz_btree_insert(unclaimed, &tx_id, &event, NULL);
    }
    jz_btree_drop(&pending);
    jz_btree_drop(&state->pending);
    state->pending = claimed;
    return true;
}
