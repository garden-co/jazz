/* read_barrier.h: crates/jazz/layers/db/src/db/read_barrier.rs */
#ifndef JAZZ_DB_DB_READ_BARRIER_H
#define JAZZ_DB_DB_READ_BARRIER_H
#include "channel_endpoint.h"

typedef struct jazz_db_ReadBarrierSender { u64 commit_units_sent; u64 barrier_sent; } jazz_db_ReadBarrierSender;

typedef struct jazz_db_HeldRequest {
    jazz_protocol_SyncMessage message;
    jazz_protocol_BufferLease lease; /* Option: rc == NULL */
    bool receipts_validated;
} jazz_db_HeldRequest;
void jazz_db__db__read_barrier__HeldRequest__drop(void *p);

typedef struct jazz_db_ReadBarrierReceiver {
    u64 received;
    jz_btree unanswered;        /* BTreeSet<u64> */
    jz_btree unanswered_by_tx;  /* BTreeMap<TxId, VecDeque<u64>> */
    jz_btree answered;          /* BTreeSet<TxId> */
    jz_deque held;              /* VecDeque<HeldRequest> */
    jz_deque released;          /* VecDeque<HeldRequest> */
} jazz_db_ReadBarrierReceiver;

void jazz_db__db__read_barrier__ReadBarrierSender__commit_unit_sent(jazz_db_ReadBarrierSender *s);
bool jazz_db__db__read_barrier__ReadBarrierSender__pending_barrier(const jazz_db_ReadBarrierSender *s, jazz_protocol_SyncMessage *out);
void jazz_db__db__read_barrier__ReadBarrierSender__barrier_accepted(jazz_db_ReadBarrierSender *s);

void jazz_db__db__read_barrier__ReadBarrierReceiver__default(jazz_db_ReadBarrierReceiver *r);
void jazz_db__db__read_barrier__ReadBarrierReceiver__drop(jazz_db_ReadBarrierReceiver *r);
void jazz_db__db__read_barrier__ReadBarrierReceiver__commit_unit_received(jazz_db_ReadBarrierReceiver *r, jazz_model_TxId tx_id);
bool jazz_db__db__read_barrier__ReadBarrierReceiver__answer_sent(jazz_db_ReadBarrierReceiver *r, const jazz_protocol_SyncMessage *message);
/* admit: returns true and moves request to *out when it may be served now */
bool jazz_db__db__read_barrier__ReadBarrierReceiver__admit(jazz_db_ReadBarrierReceiver *r, jazz_db_HeldRequest *request, jazz_db_HeldRequest *out);
bool jazz_db__db__read_barrier__ReadBarrierReceiver__pop_released(jazz_db_ReadBarrierReceiver *r, jazz_db_HeldRequest *out);
bool jazz_db__db__read_barrier__ReadBarrierReceiver__is_holding(const jazz_db_ReadBarrierReceiver *r);
bool jazz_db__db__read_barrier__ReadBarrierReceiver__has_released(const jazz_db_ReadBarrierReceiver *r);
#endif
