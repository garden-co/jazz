/* read_barrier.c: crates/jazz/layers/db/src/db/read_barrier.rs
 * Read-your-writes across independently credited channels (SPEC 8 §8.2.1). */
#include "read_barrier.h"

static void ordinal_queue_drop(void *p) { jz_deque_drop((jz_deque *)p, NULL); }
static const jz_btree_desc u64_set_desc = { sizeof(u64), 0, jz_cmp_u64, NULL, NULL };
static const jz_btree_desc tx_set_desc = { sizeof(jazz_model_TxId), 0, jazz_model__tx__TxId__cmp, NULL, NULL };
static const jz_btree_desc tx_ordinals_desc = { sizeof(jazz_model_TxId), sizeof(jz_deque), jazz_model__tx__TxId__cmp, NULL, ordinal_queue_drop };

void jazz_db__db__read_barrier__HeldRequest__drop(void *p) {
    jazz_db_HeldRequest *h = p;
    jazz_protocol__protocol__SyncMessage__drop(&h->message);
    if (h->lease.rc) jazz_protocol__wire__channel_credit__BufferLease__drop(&h->lease);
}

/* ---- ReadBarrierSender ---- */
void jazz_db__db__read_barrier__ReadBarrierSender__commit_unit_sent(jazz_db_ReadBarrierSender *s) {
    s->commit_units_sent += 1;
}

bool jazz_db__db__read_barrier__ReadBarrierSender__pending_barrier(const jazz_db_ReadBarrierSender *s, jazz_protocol_SyncMessage *out) {
    if (!(s->commit_units_sent > s->barrier_sent)) return false;
    memset(out, 0, sizeof *out);
    out->tag = JAZZ_SM_READ_BARRIER;
    out->v.read_barrier.writes_through = s->commit_units_sent;
    return true;
}

void jazz_db__db__read_barrier__ReadBarrierSender__barrier_accepted(jazz_db_ReadBarrierSender *s) {
    s->barrier_sent = s->commit_units_sent;
}

/* ---- ReadBarrierReceiver ---- */
void jazz_db__db__read_barrier__ReadBarrierReceiver__default(jazz_db_ReadBarrierReceiver *r) {
    r->received = 0;
    jz_btree_init(&r->unanswered, &u64_set_desc);
    jz_btree_init(&r->unanswered_by_tx, &tx_ordinals_desc);
    jz_btree_init(&r->answered, &tx_set_desc);
    jz_deque_init(&r->held, sizeof(jazz_db_HeldRequest));
    jz_deque_init(&r->released, sizeof(jazz_db_HeldRequest));
}

void jazz_db__db__read_barrier__ReadBarrierReceiver__drop(jazz_db_ReadBarrierReceiver *r) {
    jz_btree_drop(&r->unanswered);
    jz_btree_drop(&r->unanswered_by_tx);
    jz_btree_drop(&r->answered);
    jz_deque_drop(&r->held, jazz_db__db__read_barrier__HeldRequest__drop);
    jz_deque_drop(&r->released, jazz_db__db__read_barrier__HeldRequest__drop);
}

/* Number a commit unit as it arrives on this link. */
void jazz_db__db__read_barrier__ReadBarrierReceiver__commit_unit_received(jazz_db_ReadBarrierReceiver *r, jazz_model_TxId tx_id) {
    r->received += 1;
    if (jz_btree_contains(&r->answered, &tx_id)) return;
    jz_btree_insert(&r->unanswered, &r->received, NULL, NULL);
    bool inserted;
    jz_deque *ordinals = jz_btree_entry(&r->unanswered_by_tx, &tx_id, &inserted);
    if (inserted) jz_deque_init(ordinals, sizeof(u64));
    jz_deque_push_back(ordinals, &r->received);
}

static bool jazz_db__db__read_barrier__ReadBarrierReceiver__satisfied(const jazz_db_ReadBarrierReceiver *r, u64 writes_through) {
    if (r->received < writes_through) return false;
    void *oldest, *unused;
    if (!jz_btree_first(&r->unanswered, &oldest, &unused)) return true;
    return *(const u64 *)oldest > writes_through;
}

/* Move requests behind now-satisfied barriers to the released queue. */
static bool jazz_db__db__read_barrier__ReadBarrierReceiver__release_satisfied(jazz_db_ReadBarrierReceiver *r) {
    bool released_any = false;
    while (r->held.len > 0) {
        jazz_db_HeldRequest *front = jz_deque_at(&r->held, 0);
        if (front->message.tag != JAZZ_SM_READ_BARRIER) break;
        if (!jazz_db__db__read_barrier__ReadBarrierReceiver__satisfied(r, front->message.v.read_barrier.writes_through)) break;
        jazz_db_HeldRequest barrier;
        jz_deque_pop_front(&r->held, &barrier);
        jazz_db__db__read_barrier__HeldRequest__drop(&barrier);
        while (r->held.len > 0) {
            jazz_db_HeldRequest *next = jz_deque_at(&r->held, 0);
            if (next->message.tag == JAZZ_SM_READ_BARRIER) break;
            jazz_db_HeldRequest moved;
            if (!jz_deque_pop_front(&r->held, &moved)) jz_panic("front exists");
            jz_deque_push_back(&r->released, &moved);
            released_any = true;
        }
    }
    return released_any;
}

/* Record an answer this link sent downstream: the first FateUpdate for a write.
 * Returns whether held requests may now be released. */
bool jazz_db__db__read_barrier__ReadBarrierReceiver__answer_sent(jazz_db_ReadBarrierReceiver *r, const jazz_protocol_SyncMessage *message) {
    if (message->tag != JAZZ_SM_FATE_UPDATE) return false;
    jazz_model_TxId tx_id = message->v.fate_update.tx_id;
    bool settled = message->v.fate_update.fate.tag == JAZZ_FATE_REJECTED ||
                   (message->v.fate_update.has_durability && message->v.fate_update.durability == JAZZ_DUR_GLOBAL);
    if (settled) jz_btree_remove(&r->answered, &tx_id, NULL, NULL);
    else jz_btree_insert(&r->answered, &tx_id, NULL, NULL);
    jz_deque *ordinals = jz_btree_get(&r->unanswered_by_tx, &tx_id);
    if (!ordinals) return false;
    u64 ordinal;
    if (!jz_deque_pop_front(ordinals, &ordinal)) jz_panic("no empty ordinal queues are kept");
    if (ordinals->len == 0) jz_btree_remove(&r->unanswered_by_tx, &tx_id, NULL, NULL);
    jz_btree_remove(&r->unanswered, &ordinal, NULL, NULL);
    return jazz_db__db__read_barrier__ReadBarrierReceiver__release_satisfied(r);
}

/* Whether this inbound request must wait. Satisfied barriers are consumed here. */
bool jazz_db__db__read_barrier__ReadBarrierReceiver__admit(jazz_db_ReadBarrierReceiver *r, jazz_db_HeldRequest *request, jazz_db_HeldRequest *out) {
    bool barrier;
    if (jazz_db__db__channel_endpoint__message_class(&request->message, &barrier) != JAZZ_CC_REQUESTS) {
        *out = *request;
        return true;
    }
    if (r->held.len > 0) {
        jz_deque_push_back(&r->held, request);
        return false;
    }
    if (request->message.tag == JAZZ_SM_READ_BARRIER) {
        if (jazz_db__db__read_barrier__ReadBarrierReceiver__satisfied(r, request->message.v.read_barrier.writes_through)) {
            jazz_db__db__read_barrier__HeldRequest__drop(request);
            return false;
        }
        jz_deque_push_back(&r->held, request);
        return false;
    }
    *out = *request;
    return true;
}

bool jazz_db__db__read_barrier__ReadBarrierReceiver__pop_released(jazz_db_ReadBarrierReceiver *r, jazz_db_HeldRequest *out) {
    return jz_deque_pop_front(&r->released, out);
}

bool jazz_db__db__read_barrier__ReadBarrierReceiver__is_holding(const jazz_db_ReadBarrierReceiver *r) { return r->held.len != 0; }
bool jazz_db__db__read_barrier__ReadBarrierReceiver__has_released(const jazz_db_ReadBarrierReceiver *r) { return r->released.len != 0; }
