/* channel_endpoint.c: port of crates/jazz/layers/db/src/db/channel_endpoint.rs */
#include "channel_endpoint.h"

static jz_err transport_failed(jz_str msg) {
    jz_err e = jz_err_fmt(JAZZ_TE_FAILED, "%.*s", (int)msg.len, msg.ptr);
    jz_str_drop(&msg);
    return e;
}

static void shared_aux_drop(void *p) {
    jazz_db_SharedAuxiliaryEndpoint *s = p;
    jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__drop(&s->inner);
}

jazz_db_SharedAuxiliaryEndpoint *jazz_db__db__channel_endpoint__SharedAuxiliaryEndpoint__new(jazz_db_AuxiliaryChannelEndpoint endpoint) {
    jazz_db_SharedAuxiliaryEndpoint *s = jz_rc_new(sizeof *s, shared_aux_drop);
    s->poisoned = false;
    s->inner = endpoint;
    return s;
}

jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__new(jazz_protocol_WireInboundContext *context, jazz_db_ChannelEndpoint *out) {
    memset(out, 0, sizeof *out);
    jazz_db__db__routed_messages__RoutedMessages__default(&out->routing);
    JZ_TRY(jazz_protocol__wire__stream_backend__OrderedChannelBackend__new(jazz_protocol__wire__WireInboundContext__clone(context), &out->raw));
    out->context = context;
    out->has_last_wire_error = false;
    return JZ_OK;
}

void jazz_db__db__channel_endpoint__ChannelEndpoint__drop(jazz_db_ChannelEndpoint *e) {
    jazz_db__db__routed_messages__RoutedMessages__drop(&e->routing);
    jazz_protocol__wire__stream_backend__OrderedChannelBackend__drop(e->raw);
    jazz_protocol__wire__WireInboundContext__drop(e->context);
    if (e->has_last_wire_error) jazz_protocol__wire__WireError__drop(&e->last_wire_error);
}

void jazz_db__db__channel_endpoint__ChannelEndpoint__set_trusted_encoder(jazz_db_ChannelEndpoint *e, bool trusted) {
    jazz_protocol__wire__WireInboundContext__set_trusted_encoder(e->context, trusted);
}

bool jazz_db__db__channel_endpoint__ChannelEndpoint__last_wire_error(const jazz_db_ChannelEndpoint *e, jazz_protocol_WireError *out) {
    if (!e->has_last_wire_error) return false;
    jazz_protocol__wire__WireError__clone(out, &e->last_wire_error);
    return true;
}

bool jazz_db__db__channel_endpoint__ChannelEndpoint__incomplete_receive_timeout_ms(const jazz_db_ChannelEndpoint *e, u64 *out) {
    return jazz_protocol__wire__stream_backend__OrderedChannelBackend__incomplete_receive_timeout_ms(e->raw, out);
}

jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__expire(jazz_db_ChannelEndpoint *e) {
    return jazz_protocol__wire__stream_backend__OrderedChannelBackend__expire(e->raw);
}

jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__enqueue(jazz_db_ChannelEndpoint *e, u16 channel, u64 generation, u32 class_, const jazz_protocol_SyncMessage *message, bool barrier) {
    jz_bytes bytes;
    jz_err err = jazz_protocol__wire__encode_sync_message_for_features(message, jazz_protocol__wire__WireInboundContext__negotiated_features(e->context), &bytes);
    if (JZ_IS_ERR(err)) {
        jz_str msg = jz_format("cannot encode channel message: %.*s", err.msg ? (int)err.msg->len : 0, err.msg ? err.msg->ptr : "");
        jz_err_drop(&err);
        return transport_failed(msg);
    }
    if (class_ == JAZZ_CC_AUXILIARY)
        return jazz_protocol__wire__stream_backend__OrderedChannelBackend__enqueue(e->raw, channel, generation, class_, bytes);
    jz_bytes routed;
    err = jazz_db__db__routed_messages__RoutedMessages__prepare(&e->routing, channel, barrier, bytes, &routed);
    if (JZ_IS_ERR(err)) { err.code = JAZZ_TE_FAILED; return err; }
    JZ_TRY(jazz_protocol__wire__stream_backend__OrderedChannelBackend__enqueue(e->raw, channel, generation, class_, routed));
    jazz_db__db__routed_messages__RoutedMessages__accepted(&e->routing, channel, barrier);
    return JZ_OK;
}

jazz_protocol_SharedChannelCredits *jazz_db__db__channel_endpoint__ChannelEndpoint__channel_credits(const jazz_db_ChannelEndpoint *e) {
    return jazz_protocol__wire__stream_backend__OrderedChannelBackend__channel_credits(e->raw);
}
bool jazz_db__db__channel_endpoint__ChannelEndpoint__is_idle(const jazz_db_ChannelEndpoint *e, u16 channel) {
    return jazz_protocol__wire__stream_backend__OrderedChannelBackend__is_idle(e->raw, channel);
}
jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__next_idle_generation(const jazz_db_ChannelEndpoint *e, u16 channel, u64 *out) {
    return jazz_protocol__wire__stream_backend__OrderedChannelBackend__next_idle_generation(e->raw, channel, out);
}
bool jazz_db__db__channel_endpoint__ChannelEndpoint__has_pending(const jazz_db_ChannelEndpoint *e) {
    return jazz_protocol__wire__stream_backend__OrderedChannelBackend__has_pending(e->raw);
}
jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__peek_outbound(jazz_db_ChannelEndpoint *e, bool *has, jz_bytes *out) {
    return jazz_protocol__wire__stream_backend__OrderedChannelBackend__peek_outbound(e->raw, has, out);
}
jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__accept_outbound(jazz_db_ChannelEndpoint *e, bool *completed) {
    return jazz_protocol__wire__stream_backend__OrderedChannelBackend__accept_outbound(e->raw, completed);
}
bool jazz_db__db__channel_endpoint__ChannelEndpoint__pop(jazz_db_ChannelEndpoint *e, jazz_db_ReceivedSyncMessage *out) {
    return jazz_db__db__routed_messages__RoutedMessages__pop(&e->routing, out);
}

static void set_last_wire_error(jazz_db_ChannelEndpoint *e, jz_err err) {
    if (e->has_last_wire_error) jazz_protocol__wire__WireError__drop(&e->last_wire_error);
    jazz_protocol_WireError *we = jazz_protocol__wire__WireError__from_err(err);
    jazz_protocol__wire__WireError__clone(&e->last_wire_error, we);
    e->has_last_wire_error = true;
}

jz_err jazz_db__db__channel_endpoint__ChannelEndpoint__receive(jazz_db_ChannelEndpoint *e, jazz_protocol_WireChannelEnvelope frame, u32 encoded_len, bool *has, jazz_db_ReceivedSyncMessage *out) {
    bool got = false;
    jazz_protocol_ReceivedChannelPayload received;
    JZ_TRY(jazz_protocol__wire__stream_backend__OrderedChannelBackend__receive(e->raw, frame, encoded_len, &got, &received));
    if (got) {
        if (received.class_ == JAZZ_CC_AUXILIARY) {
            jazz_protocol_SyncMessage message;
            jz_err err = jazz_protocol__wire__WireInboundContext__decode_semantic_payload(e->context, jz_bytes_slice(&received.payload), &message);
            jz_bytes_drop(&received.payload);
            if (JZ_IS_ERR(err)) {
                set_last_wire_error(e, err);
                jz_str dbg = {0};
                jazz_protocol__wire__WireError__debug(&dbg, &e->last_wire_error);
                jz_err out_err = jz_err_fmt(JAZZ_DB_STRING_ERROR, "invalid auxiliary payload: %.*s", (int)dbg.len, dbg.ptr);
                jz_str_drop(&dbg);
                jz_err_drop(&err);
                jazz_protocol__wire__channel_credit__BufferLease__drop(&received.lease);
                return out_err;
            }
            bool barrier;
            if (jazz_db__db__channel_endpoint__message_class(&message, &barrier) != JAZZ_CC_AUXILIARY) {
                jazz_protocol__protocol__SyncMessage__drop(&message);
                jazz_protocol__wire__channel_credit__BufferLease__drop(&received.lease);
                return jz_err_new(JAZZ_DB_STRING_ERROR, "message is not auxiliary");
            }
            out->message = message;
            out->lease = received.lease;
            out->receipts_validated = jazz_protocol__wire__WireInboundContext__validates_receipts(e->context);
            *has = true;
            return JZ_OK;
        }
        jz_err err = jazz_db__db__routed_messages__RoutedMessages__receive(&e->routing, received.channel, received.class_, received.payload, received.lease, e->context);
        if (JZ_IS_ERR(err)) {
            set_last_wire_error(e, err);
            jz_str dbg = {0};
            jazz_protocol__wire__WireError__debug(&dbg, &e->last_wire_error);
            jz_err out_err = jz_err_fmt(JAZZ_DB_STRING_ERROR, "invalid routed payload: %.*s", (int)dbg.len, dbg.ptr);
            jz_str_drop(&dbg);
            jz_err_drop(&err);
            return out_err;
        }
    }
    *has = jazz_db__db__routed_messages__RoutedMessages__pop(&e->routing, out);
    return JZ_OK;
}

u32 jazz_db__db__channel_endpoint__message_class(const jazz_protocol_SyncMessage *message, bool *barrier) {
    switch (message->tag) {
    case JAZZ_SM_CHUNK_REQUEST_BATCH:
    case JAZZ_SM_CHUNK_RESPONSE_BATCH:
        *barrier = false; return JAZZ_CC_AUXILIARY;
    case JAZZ_SM_SESSION_CLAIMS:
    case JAZZ_SM_PUBLISH_SCHEMA:
    case JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS:
    case JAZZ_SM_PUBLISH_LENS:
    case JAZZ_SM_CATALOGUE_ACK:
    case JAZZ_SM_CATALOGUE_SNAPSHOT:
        *barrier = true; return JAZZ_CC_CONTROL;
    case JAZZ_SM_FATE_UPDATE:
        *barrier = true; return JAZZ_CC_WRITES;
    case JAZZ_SM_COMMIT_UNIT:
        *barrier = false; return JAZZ_CC_WRITES;
    case JAZZ_SM_REGISTER_SHAPE:
    case JAZZ_SM_SUBSCRIBE:
    case JAZZ_SM_UNSUBSCRIBE:
    case JAZZ_SM_AUTHORIZATION_SCOPE_INTENT:
    case JAZZ_SM_CURRENT_ROWS_REQUEST:
    case JAZZ_SM_CURRENT_ROWS_CANCEL:
    case JAZZ_SM_READ_BARRIER:
        *barrier = false; return JAZZ_CC_REQUESTS;
    case JAZZ_SM_CHUNK_UPLOAD_START:
    case JAZZ_SM_CHUNK_UPLOAD_NODES:
    case JAZZ_SM_CHUNK_UPLOAD_RESULT:
        *barrier = false; return JAZZ_CC_LARGE_VALUE;
    default:
        *barrier = false; return JAZZ_CC_DELIVERY;
    }
}

/* ---- AuxiliaryChannelEndpoint ---- */
jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__new(jazz_protocol_WireInboundContext *context, jazz_db_AuxiliaryChannelEndpoint *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_db__db__channel_endpoint__ChannelEndpoint__new(context, &out->endpoint));
    out->has_message = false;
    out->pump_owned = false;
    out->has_waker = false;
    return JZ_OK;
}

void jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__drop(jazz_db_AuxiliaryChannelEndpoint *e) {
    jazz_db__db__channel_endpoint__ChannelEndpoint__drop(&e->endpoint);
    if (e->has_message) jazz_protocol__protocol__SyncMessage__drop(&e->message);
}

void jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_trusted_encoder(jazz_db_AuxiliaryChannelEndpoint *e, bool trusted) {
    jazz_db__db__channel_endpoint__ChannelEndpoint__set_trusted_encoder(&e->endpoint, trusted);
}
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__last_wire_error(const jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_WireError *out) {
    return jazz_db__db__channel_endpoint__ChannelEndpoint__last_wire_error(&e->endpoint, out);
}
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__incomplete_receive_timeout_ms(const jazz_db_AuxiliaryChannelEndpoint *e, u64 *out) {
    return jazz_db__db__channel_endpoint__ChannelEndpoint__incomplete_receive_timeout_ms(&e->endpoint, out);
}
jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__expire_incomplete_receive(jazz_db_AuxiliaryChannelEndpoint *e) {
    return jazz_db__db__channel_endpoint__ChannelEndpoint__expire(&e->endpoint);
}
void jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_channel_credits(jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_SharedChannelCredits *credits) {
    jazz_protocol__wire__stream_backend__OrderedChannelBackend__set_channel_credits(e->endpoint.raw, credits);
}
jazz_protocol_SharedChannelCredits *jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__channel_credits(const jazz_db_AuxiliaryChannelEndpoint *e) {
    return jazz_db__db__channel_endpoint__ChannelEndpoint__channel_credits(&e->endpoint);
}
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__outbound_is_ready(const jazz_db_AuxiliaryChannelEndpoint *e) {
    if (!jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__has_pending_outbound(e)) return false;
    jazz_protocol_SharedChannelCredits *credits = jazz_db__db__channel_endpoint__ChannelEndpoint__channel_credits(&e->endpoint);
    bool ok = !credits->poisoned && jazz_protocol__wire__channel_credit__ChannelCredits__can_send(credits->inner, JAZZ_CC_AUXILIARY);
    jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
    return ok;
}
void jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_pump_owned(jazz_db_AuxiliaryChannelEndpoint *e) { e->pump_owned = true; }
bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__pump_owned(const jazz_db_AuxiliaryChannelEndpoint *e) { return e->pump_owned; }

jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__enqueue(jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_SyncMessage message) {
    jazz_protocol_SyncMessage rejected;
    jz_err err = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__try_enqueue(e, message, &rejected);
    if (JZ_IS_ERR(err)) jazz_protocol__protocol__SyncMessage__drop(&rejected);
    return err;
}

jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__try_enqueue(jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_SyncMessage message, jazz_protocol_SyncMessage *rejected) {
    bool barrier;
    if (jazz_db__db__channel_endpoint__message_class(&message, &barrier) != JAZZ_CC_AUXILIARY) {
        *rejected = message;
        return jz_err_new(JAZZ_TE_FAILED, "canonical message attempted auxiliary bypass");
    }
    if (e->has_message) {
        *rejected = message;
        return jz_err_new(JAZZ_TE_BACKPRESSURE, NULL);
    }
    jz_err err = jazz_db__db__channel_endpoint__ChannelEndpoint__enqueue(&e->endpoint, JAZZ_WIRE_AUXILIARY_CHANNEL, 0, JAZZ_CC_AUXILIARY, &message, false);
    if (JZ_IS_ERR(err)) { *rejected = message; return err; }
    e->message = message;
    e->has_message = true;
    if (e->has_waker) {
        e->has_waker = false;
        jz_waker_wake(&e->waker);
    }
    return JZ_OK;
}

jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__peek_outbound(jazz_db_AuxiliaryChannelEndpoint *e, bool *has, jz_bytes *out) {
    return jazz_db__db__channel_endpoint__ChannelEndpoint__peek_outbound(&e->endpoint, has, out);
}

jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__accept_outbound(jazz_db_AuxiliaryChannelEndpoint *e, bool *has, jazz_protocol_SyncMessage *out) {
    bool completed;
    JZ_TRY(jazz_db__db__channel_endpoint__ChannelEndpoint__accept_outbound(&e->endpoint, &completed));
    *has = false;
    if (completed && e->has_message) {
        *out = e->message;
        e->has_message = false;
        *has = true;
    }
    return JZ_OK;
}

bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__has_pending_outbound(const jazz_db_AuxiliaryChannelEndpoint *e) {
    return jazz_db__db__channel_endpoint__ChannelEndpoint__has_pending(&e->endpoint);
}

bool jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__poll_outbound_ready(jazz_db_AuxiliaryChannelEndpoint *e, const jz_waker *cx) {
    if (jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__outbound_is_ready(e)) return true;
    e->waker = *cx;
    e->has_waker = true;
    jazz_protocol_SharedChannelCredits *credits = jazz_db__db__channel_endpoint__ChannelEndpoint__channel_credits(&e->endpoint);
    if (!credits->poisoned) jazz_protocol__wire__channel_credit__ChannelCredits__register_waker(credits->inner, cx);
    jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
    return false;
}

jz_err jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__receive(jazz_db_AuxiliaryChannelEndpoint *e, jazz_protocol_WireChannelEnvelope frame, u32 encoded_len, bool *has, jazz_protocol_SyncMessage *out) {
    if (frame.extent.channel != JAZZ_WIRE_AUXILIARY_CHANNEL || frame.extent.class_ != JAZZ_CC_AUXILIARY) {
        jazz_protocol__wire__WireChannelEnvelope__drop(&frame);
        return jz_err_new(JAZZ_DB_STRING_ERROR, "non-auxiliary frame attempted auxiliary bypass");
    }
    jazz_db_ReceivedSyncMessage received;
    JZ_TRY(jazz_db__db__channel_endpoint__ChannelEndpoint__receive(&e->endpoint, frame, encoded_len, has, &received));
    if (*has) {
        *out = received.message;
        if (received.lease.rc) jazz_protocol__wire__channel_credit__BufferLease__drop(&received.lease);
    }
    return JZ_OK;
}
