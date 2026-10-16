/* routed_messages.c: port of crates/jazz/layers/db/src/db/routed_messages.rs */
#include "routed_messages.h"

/* channel_endpoint::message_class (defined in channel_endpoint.c) */
u32 jazz_db__db__channel_endpoint__message_class(const jazz_protocol_SyncMessage *message, bool *barrier);

typedef struct { u16 slot; u64 n; } Dep;
typedef struct jazz_db_RoutedEnvelope {
    u8 version;
    u64 epoch;
    u64 ordinal;
    bool has_predecessors;
    JZ_VEC(Dep) predecessors;
    jz_slice payload;
} jazz_db_RoutedEnvelope;

void jazz_db__db__routed_messages__ReceivedSyncMessage__unleased(jazz_db_ReceivedSyncMessage *out, jazz_protocol_SyncMessage message) {
    out->message = message;
    out->lease.rc = NULL;
    out->receipts_validated = false;
}

void jazz_db__db__routed_messages__ReceivedSyncMessage__drop(void *p) {
    jazz_db_ReceivedSyncMessage *m = p;
    jazz_protocol__protocol__SyncMessage__drop(&m->message);
    if (m->lease.rc) jazz_protocol__wire__channel_credit__BufferLease__drop(&m->lease);
}

static void pending_drop(void *p) {
    jazz_db_RoutedPending *x = p;
    jz_free(x->predecessors.ptr);
    jazz_protocol__protocol__SyncMessage__drop(&x->message);
    jazz_protocol__wire__channel_credit__BufferLease__drop(&x->lease);
}

static jz_err jazz_db__db__routed_messages__decode_envelope(jz_slice bytes, jazz_db_RoutedEnvelope *env) {
    jz_pc_r r = { bytes.ptr, bytes.ptr + bytes.len, 0 };
    memset(env, 0, sizeof *env);
    jz_err e = jz_pc_r_u8(&r, &env->version);
    if (!JZ_IS_ERR(e)) e = jz_pc_r_varint(&r, &env->epoch);
    if (!JZ_IS_ERR(e)) e = jz_pc_r_varint(&r, &env->ordinal);
    if (!JZ_IS_ERR(e)) e = jz_pc_r_option(&r, &env->has_predecessors);
    if (!JZ_IS_ERR(e) && env->has_predecessors) {
        u32 n;
        e = jz_pc_r_seq(&r, &n);
        for (u32 i = 0; !JZ_IS_ERR(e) && i < n; i++) {
            Dep d;
            e = jz_pc_r_u16(&r, &d.slot);
            if (!JZ_IS_ERR(e)) e = jz_pc_r_varint(&r, &d.n);
            if (!JZ_IS_ERR(e)) JZ_VEC_PUSH(env->predecessors, d);
        }
    }
    if (!JZ_IS_ERR(e)) e = jz_pc_r_bytes_ref(&r, &env->payload);
    if (JZ_IS_ERR(e)) {
        jz_free(env->predecessors.ptr);
        jz_err out = jz_err_context(e, "invalid routed envelope");
        return out;
    }
    if (r.p != r.end) {
        jz_free(env->predecessors.ptr);
        return jz_err_new(JAZZ_DB_STRING_ERROR, "trailing bytes after routed envelope");
    }
    return JZ_OK;
}

u32 jazz_db__db__routed_messages__max_routed_payload_bytes(void) {
    return JAZZ_PROTOCOL_MAX_LOGICAL_MESSAGE_BYTES - JAZZ_DB_ROUTED_MAX_ENVELOPE_OVERHEAD;
}

void jazz_db__db__routed_messages__RoutedMessages__default(jazz_db_RoutedMessages *r) {
    memset(r, 0, sizeof *r);
    jz_deque_init(&r->pending, sizeof(jazz_db_RoutedPending));
    jz_deque_init(&r->ready, sizeof(jazz_db_ReceivedSyncMessage));
}

void jazz_db__db__routed_messages__RoutedMessages__drop(jazz_db_RoutedMessages *r) {
    jz_deque_drop(&r->pending, pending_drop);
    jz_deque_drop(&r->ready, jazz_db__db__routed_messages__ReceivedSyncMessage__drop);
}

jz_err jazz_db__db__routed_messages__RoutedMessages__prepare(const jazz_db_RoutedMessages *r, u16 channel, bool barrier, jz_bytes payload, jz_bytes *out) {
    jz_err e = JZ_OK;
    if (channel >= JAZZ_WIRE_MAX_CHANNELS) { e = jz_err_new(JAZZ_DB_STRING_ERROR, "invalid logical stream ID"); goto done; }
    if (payload.len > jazz_db__db__routed_messages__max_routed_payload_bytes()) {
        e = jz_err_new(JAZZ_DB_STRING_ERROR, "semantic message exceeds routed payload limit");
        goto done;
    }
    if (r->sent[channel] == UINT64_MAX) { e = jz_err_new(JAZZ_DB_STRING_ERROR, "logical ordinal exhausted"); goto done; }
    u64 ordinal = r->sent[channel] + 1;
    if (barrier && r->sent_epoch == UINT64_MAX) { e = jz_err_new(JAZZ_DB_STRING_ERROR, "logical epoch exhausted"); goto done; }
    memset(out, 0, sizeof *out);
    jz_pc_w w = { out };
    jz_pc_u8(&w, JAZZ_DB_ROUTED_ENVELOPE_VERSION);
    jz_pc_varint(&w, r->sent_epoch);
    jz_pc_varint(&w, ordinal);
    jz_pc_option(&w, barrier);
    if (barrier) {
        u32 count = 0;
        for (u32 slot = 0; slot < JAZZ_WIRE_MAX_CHANNELS; slot++) if (r->sent[slot] != 0) count++;
        jz_pc_seq(&w, count);
        for (u32 slot = 0; slot < JAZZ_WIRE_MAX_CHANNELS; slot++) {
            if (r->sent[slot] == 0) continue;
            jz_pc_varint(&w, slot);
            jz_pc_varint(&w, r->sent[slot]);
        }
    }
    jz_pc_bytes(&w, payload.ptr, payload.len);
done:
    jz_bytes_drop(&payload);
    return e;
}

void jazz_db__db__routed_messages__RoutedMessages__accepted(jazz_db_RoutedMessages *r, u16 channel, bool barrier) {
    r->sent[channel] += 1;
    if (barrier) r->sent_epoch += 1;
}

static jz_err malformed(const char *message) {
    jazz_protocol_WireError err;
    jazz_protocol__wire__WireError__new(&err, JAZZ_WEC_MALFORMED_FRAME, JAZZ_WR_NEVER, jz_str_from(message));
    return jazz_protocol__wire__WireError__into_err(err);
}

static jz_err jazz_db__db__routed_messages__RoutedMessages__receive_inner(jazz_db_RoutedMessages *r, u16 channel, u32 class_, jz_bytes bytes, jazz_protocol_BufferLease lease, const jazz_protocol_WireInboundContext *context) {
    jazz_db_RoutedEnvelope envelope;
    jz_err e = jazz_db__db__routed_messages__decode_envelope(jz_bytes_slice(&bytes), &envelope);
    if (JZ_IS_ERR(e)) {
        jazz_protocol_WireError err;
        jz_str msg = e.msg ? jz_str_clone(e.msg) : jz_str_from("invalid routed envelope");
        jz_err_drop(&e);
        jazz_protocol__wire__WireError__new(&err, JAZZ_WEC_MALFORMED_FRAME, JAZZ_WR_NEVER, msg);
        e = jazz_protocol__wire__WireError__into_err(err);
        goto fail_lease;
    }
    u32 slot = channel;
    if (r->received[slot] == UINT64_MAX) { e = malformed("logical ordinal exhausted"); goto fail; }
    u64 diff = envelope.epoch >= r->receive_epoch ? envelope.epoch - r->receive_epoch : 0;
    if (envelope.version != JAZZ_DB_ROUTED_ENVELOPE_VERSION || envelope.epoch < r->receive_epoch ||
        diff > JAZZ_WIRE_MAX_CHANNEL_QUEUED_MESSAGES || envelope.ordinal != r->received[slot] + 1) {
        e = malformed("invalid routed epoch or stream ordinal");
        goto fail;
    }
    if (envelope.has_predecessors) {
        bool bad = envelope.predecessors.len > JAZZ_WIRE_MAX_CHANNELS;
        for (u32 i = 0; !bad && i + 1 < envelope.predecessors.len; i++)
            if (envelope.predecessors.ptr[i].slot >= envelope.predecessors.ptr[i + 1].slot) bad = true;
        for (u32 i = 0; !bad && i < envelope.predecessors.len; i++)
            if (envelope.predecessors.ptr[i].slot >= JAZZ_WIRE_MAX_CHANNELS || envelope.predecessors.ptr[i].n == 0) bad = true;
        if (bad) { e = malformed("invalid barrier dependency vector"); goto fail; }
    }
    if (r->pending.len + r->ready.len >= JAZZ_WIRE_MAX_CHANNEL_QUEUED_MESSAGES + JAZZ_WIRE_CONTROL_RESERVE_MESSAGES) {
        e = malformed("routed message queue count exceeded");
        goto fail;
    }
    jazz_protocol_SyncMessage message;
    e = jazz_protocol__wire__WireInboundContext__decode_semantic_payload(context, envelope.payload, &message);
    if (JZ_IS_ERR(e)) goto fail;
    bool barrier;
    u32 expected_class = jazz_db__db__channel_endpoint__message_class(&message, &barrier);
    if (class_ != expected_class || barrier != envelope.has_predecessors) {
        jazz_protocol__protocol__SyncMessage__drop(&message);
        e = malformed("message routing metadata disagrees with semantics");
        goto fail;
    }
    r->received[slot] = envelope.ordinal;
    jazz_db_RoutedPending pending;
    pending.channel = channel;
    pending.epoch = envelope.epoch;
    pending.ordinal = envelope.ordinal;
    pending.has_predecessors = envelope.has_predecessors;
    pending.predecessors.ptr = (void *)envelope.predecessors.ptr;
    pending.predecessors.len = envelope.predecessors.len;
    pending.predecessors.cap = envelope.predecessors.cap;
    pending.message = message;
    pending.lease = lease;
    pending.receipts_validated = jazz_protocol__wire__WireInboundContext__validates_receipts(context);
    jz_deque_push_back(&r->pending, &pending);
    jz_bytes_drop(&bytes);
    for (;;) {
        u32 index = UINT32_MAX;
        for (u32 i = 0; i < r->pending.len; i++) {
            jazz_db_RoutedPending *p = jz_deque_at(&r->pending, i);
            if (p->epoch != r->receive_epoch || p->ordinal != r->admitted[p->channel] + 1) continue;
            bool ok = true;
            if (p->has_predecessors)
                for (u32 j = 0; j < p->predecessors.len; j++)
                    if (r->admitted[p->predecessors.ptr[j].slot] < p->predecessors.ptr[j].n) { ok = false; break; }
            if (ok) { index = i; break; }
        }
        if (index == UINT32_MAX) break;
        /* VecDeque::remove(index) */
        jazz_db_RoutedPending taken = *(jazz_db_RoutedPending *)jz_deque_at(&r->pending, index);
        for (u32 i = index; i + 1 < r->pending.len; i++)
            memcpy(jz_deque_at(&r->pending, i), jz_deque_at(&r->pending, i + 1), sizeof(jazz_db_RoutedPending));
        jazz_db_RoutedPending dummy;
        jz_deque_pop_back(&r->pending, &dummy);
        r->admitted[taken.channel] = taken.ordinal;
        if (taken.has_predecessors) {
            if (r->receive_epoch == UINT64_MAX) {
                pending_drop(&taken);
                return malformed("logical epoch exhausted");
            }
            r->receive_epoch += 1;
        }
        jz_free(taken.predecessors.ptr);
        jazz_db_ReceivedSyncMessage ready = { taken.message, taken.lease, taken.receipts_validated };
        jz_deque_push_back(&r->ready, &ready);
    }
    return JZ_OK;
fail:
    jz_free(envelope.predecessors.ptr);
fail_lease:
    jz_bytes_drop(&bytes);
    jazz_protocol__wire__channel_credit__BufferLease__drop(&lease);
    return e;
}

jz_err jazz_db__db__routed_messages__RoutedMessages__receive(jazz_db_RoutedMessages *r, u16 channel, u32 class_, jz_bytes bytes, jazz_protocol_BufferLease lease, const jazz_protocol_WireInboundContext *context) {
    return jazz_db__db__routed_messages__RoutedMessages__receive_inner(r, channel, class_, bytes, lease, context);
}

bool jazz_db__db__routed_messages__RoutedMessages__pop(jazz_db_RoutedMessages *r, jazz_db_ReceivedSyncMessage *out) {
    return jz_deque_pop_front(&r->ready, out);
}
