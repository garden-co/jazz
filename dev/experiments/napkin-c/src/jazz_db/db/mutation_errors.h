/* mutation_errors.h: crates/jazz/layers/db/src/db/mutation_errors.rs */
#ifndef JAZZ_DB_DB_MUTATION_ERRORS_H
#define JAZZ_DB_DB_MUTATION_ERRORS_H
#include "../db.h"

#define MERR_FN(n) jazz_db__db__mutation_errors__##n
void MERR_FN(mutation_error_event)(jazz_db_MutationErrorEvent *out, jazz_model_RejectedTransaction *rejected);
void MERR_FN(mutation_error_event_for)(jazz_db_MutationErrorEvent *out, jazz_model_TxId tx_id, u32 kind, const jazz_model_RejectionReason *rejection);
void MERR_FN(queued_mutation_error_event)(jazz_db_MutationErrorEvent *out, jazz_model_TxId tx_id, u32 kind, const jz_err *error);
void MERR_FN(mutation_error_details)(const jazz_model_RejectionReason *reason, jz_str *code, jz_str *message);
void MERR_FN(queue_mutation_error)(jazz_db_SharedMutationErrors errors, jazz_db_SharedTickScheduler scheduler, jazz_model_TxId tx_id, jazz_db_MutationErrorEvent event);

typedef struct jazz_db_MutationErrorWait { jazz_db_SharedMutationErrors errors; jazz_db_SharedTickScheduler scheduler; jazz_model_TxId tx_id; } jazz_db_MutationErrorWait;
void jazz_db__db__mutation_errors__MutationErrorWait__new(jazz_db_MutationErrorWait *out, jazz_db_SharedMutationErrors errors, jazz_db_SharedTickScheduler scheduler, jazz_model_TxId tx_id);
void jazz_db__db__mutation_errors__MutationErrorWait__Drop__drop(jazz_db_MutationErrorWait *self);

typedef struct jazz_db_TxMutationErrorEvent { jazz_model_TxId tx_id; jazz_db_MutationErrorEvent event; } jazz_db_TxMutationErrorEvent;
/* Some((callback, unclaimed)) -> true */
bool MERR_FN(take_pending_mutation_error_delivery)(jazz_db_SharedMutationErrors errors, jazz_db_MutationErrorCallback *callback, jz_btree *unclaimed);
#endif
