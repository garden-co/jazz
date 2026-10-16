/* wire_transport.c: crates/jazz/layers/db/src/db/wire_transport.rs
 * Wire-frame adaptation, bounded-message fragmentation, and reassembly. */
#include "wire_transport.h"

#define WTA(name) jazz_db__db__wire_transport__WireTransportAdapter__##name

/* ---- TransportError helpers (jz_err with JAZZ_TE_* code) ---- */
static jz_err te_failed(jz_err e) {
    /* map_err(TransportError::Failed): keep the String, retag */
    e.code = JAZZ_TE_FAILED;
    return e;
}
static jz_err te_failed_str(const char *msg) { return jz_err_new(JAZZ_TE_FAILED, msg); }
static jz_err te_clone(const jz_err *e) {
    if (e->msg) return jz_err_fmt(e->code, "%.*s", (int)e->msg->len, e->msg->ptr);
    return jz_err_new(e->code, NULL);
}
static void te_debug(jz_str *out, const jz_err *e) {
    switch (e->code) {
    case JAZZ_TE_BACKPRESSURE: jz_str_push_cstr(out, "Backpressure"); break;
    case JAZZ_TE_CLOSED: jz_str_push_cstr(out, "Closed"); break;
    default:
        jz_str_push_cstr(out, "Failed(\"");
        if (e->msg) jz_str_push(out, e->msg->ptr, e->msg->len);
        jz_str_push_cstr(out, "\")");
        break;
    }
}
static void set_terminal(jazz_db_WireTransportAdapter *self, const jz_err *error) {
    if (self->terminal_error.code) jz_err_drop(&self->terminal_error);
    self->terminal_error = te_clone(error);
}
static void set_last_wire_error(jazz_db_WireTransportAdapter *self, jazz_protocol_WireError error) {
    if (self->has_last_wire_error) jazz_protocol__wire__WireError__drop(&self->last_wire_error);
    self->last_wire_error = error;
    self->has_last_wire_error = true;
}

/* ---- btree descs ---- */
static int route_key_cmp(const void *a, const void *b, void *ctx) { (void)ctx; return jz_bytes_cmp(a, b); }
static void route_key_drop(void *k) { jz_bytes_drop(k); }
typedef struct { u16 slot; u64 generation; } RouteVal;
static const jz_btree_desc routes_desc = { sizeof(jz_bytes), sizeof(RouteVal), route_key_cmp, route_key_drop, NULL };
static void inbound_parts_drop(void *v) {
    jazz_db_InboundViewParts *p = v;
    jz_vec_drop(&p->parts, sizeof(jazz_protocol_ViewUpdatePayload), jazz_protocol__protocol__ViewUpdatePayload__drop);
}
static const jz_btree_desc inbound_parts_desc = { sizeof(jazz_protocol_SubscriptionKey), sizeof(jazz_db_InboundViewParts),
                                                  jazz_protocol__protocol__SubscriptionKey__cmp, NULL, inbound_parts_drop };

/* ---- free helpers ---- */

/* Whether `view` must be sent as bounded parts. */
static bool jazz_db__db__wire_transport__exceeds_routed_payload_limit(const jazz_protocol_ViewUpdatePayload *view) {
    bool ok;
    u32 bytes = jazz_protocol__protocol__ViewUpdatePayload__serialized_size(view, &ok);
    if (!ok) return true;
    /* Measure the payload as a message: the variant tag is one byte. */
    u64 with_tag = (u64)bytes + 1;
    return with_tag > jazz_db__db__routed_messages__max_routed_payload_bytes();
}

static jz_bytes jazz_db__db__wire_transport__delivery_route_key(const jazz_protocol_SyncMessage *message) {
    u8 tag;
    jz_bytes bytes = {0};
    jz_pc_w w = { &bytes };
    switch (message->tag) {
    /* Every part of one update shares its subscription's stream. */
    case JAZZ_SM_VIEW_UPDATE:
    case JAZZ_SM_VIEW_UPDATE_PART:
        tag = 0;
        jazz_protocol__protocol__SubscriptionKey__serialize(&message->v.view_update.subscription, &w);
        break;
    case JAZZ_SM_SUBSCRIBE_REJECTED:
        tag = 0;
        jazz_protocol__protocol__SubscriptionKey__serialize(&message->v.subscribe_rejected.subscription, &w);
        break;
    case JAZZ_SM_AUTHORIZATION_SCOPE_VIEW:
    case JAZZ_SM_AUTHORIZATION_SCOPE_AGGREGATE_RECEIPT:
    case JAZZ_SM_AUTHORIZATION_SCOPE_UNAVAILABLE:
    case JAZZ_SM_AUTHORIZATION_SCOPE_DECISION:
        tag = 1;
        jz_bytes_push(&bytes, message->v.scope.request_id.b, 16);
        break;
    case JAZZ_SM_CURRENT_ROWS_RECEIPT:
        tag = 2;
        jz_bytes_push(&bytes, message->v.current_rows_receipt.request_id.b, 16);
        break;
    case JAZZ_SM_CHUNK_UPLOAD_START:
    case JAZZ_SM_CHUNK_UPLOAD_NODES:
    case JAZZ_SM_CHUNK_UPLOAD_RESULT:
        tag = 3;
        jz_bytes_push(&bytes, message->v.chunk_upload.object_hash, 32);
        break;
    default:
        tag = 4;
        break;
    }
    jz_bytes key = {0};
    jz_bytes_push_u8(&key, tag);
    jz_bytes_push(&key, bytes.ptr, bytes.len);
    jz_bytes_drop(&bytes);
    return key;
}

/* ---- construction ---- */

void WTA(current)(jazz_protocol_WireTransportDyn inner, jazz_db_WireTransportAdapter *out) {
    WTA(new)(inner, JAZZ_WIRE_PROTOCOL_VERSION, jazz_protocol__wire__current_wire_features(), NULL, out);
}

void WTA(new)(jazz_protocol_WireTransportDyn inner, u16 protocol_version, jazz_protocol_WireFeatures features,
              jazz_protocol_WireSession *session, jazz_db_WireTransportAdapter *out) {
    WTA(new_with_session_context)(inner, protocol_version, features, session, NULL, out);
}

void WTA(new_with_session_context)(jazz_protocol_WireTransportDyn inner, u16 protocol_version, jazz_protocol_WireFeatures features,
                                   jazz_protocol_WireSession *session, const jazz_db_ConnectionSessionContext *session_context,
                                   jazz_db_WireTransportAdapter *out) {
    WTA(new_with_session_context_and_delegated_sessions)(inner, protocol_version, features, session, session_context, false, out);
}

void WTA(new_with_session_context_and_delegated_sessions)(jazz_protocol_WireTransportDyn inner, u16 protocol_version,
                                                         jazz_protocol_WireFeatures features, jazz_protocol_WireSession *session,
                                                         const jazz_db_ConnectionSessionContext *session_context,
                                                         bool permits_delegated_sessions, jazz_db_WireTransportAdapter *out) {
    memset(out, 0, sizeof *out);
    jazz_protocol_WireInboundContext *context = jazz_protocol__wire__WireInboundContext__new(protocol_version, features, session);
    jz_err e = jazz_db__db__channel_endpoint__ChannelEndpoint__new(jazz_protocol__wire__WireInboundContext__clone(context), &out->endpoint);
    if (JZ_IS_ERR(e)) jz_panic("valid channel context");
    jazz_db_AuxiliaryChannelEndpoint auxiliary;
    e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__new(jazz_protocol__wire__WireInboundContext__clone(context), &auxiliary);
    if (JZ_IS_ERR(e)) jz_panic("valid auxiliary context");
    jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_channel_credits(
        &auxiliary, jazz_db__db__channel_endpoint__ChannelEndpoint__channel_credits(&out->endpoint));
    out->inner = inner;
    out->inbound_context = context;
    if (session_context) { out->has_session_context = true; out->session_context = *session_context; }
    out->permits_delegated_sessions = permits_delegated_sessions;
    out->auxiliary = jazz_db__db__channel_endpoint__SharedAuxiliaryEndpoint__new(auxiliary);
    jz_btree_init(&out->routes, &routes_desc);
    out->canonical_turns = 0;
    out->terminal_error = JZ_OK;
    out->has_last_wire_error = false;
    out->received_wire_error = false;
    jz_deque_init(&out->outbound_view_parts, sizeof(jazz_protocol_SyncMessage));
    jz_btree_init(&out->inbound_view_parts, &inbound_parts_desc);
}

static void adapter_drop_except_inner(jazz_db_WireTransportAdapter *self) {
    jazz_protocol__wire__WireInboundContext__drop(self->inbound_context);
    jazz_db__db__channel_endpoint__ChannelEndpoint__drop(&self->endpoint);
    jz_rc_release(self->auxiliary);
    jz_btree_drop(&self->routes);
    if (self->terminal_error.code) jz_err_drop(&self->terminal_error);
    if (self->has_last_wire_error) jazz_protocol__wire__WireError__drop(&self->last_wire_error);
    jz_deque_drop(&self->outbound_view_parts, jazz_protocol__protocol__SyncMessage__drop);
    jz_btree_drop(&self->inbound_view_parts);
}

/* Return the underlying byte transport. */
jazz_protocol_WireTransportDyn WTA(into_inner)(jazz_db_WireTransportAdapter *self) {
    jazz_protocol_WireTransportDyn inner = self->inner;
    adapter_drop_except_inner(self);
    return inner;
}

void WTA(drop)(void *p) {
    jazz_db_WireTransportAdapter *self = p;
    adapter_drop_except_inner(self);
    if (self->inner.vt && self->inner.vt->drop) self->inner.vt->drop(self->inner.self);
}

/* ---- routing ---- */

static jz_err WTA(route)(jazz_db_WireTransportAdapter *self, const jazz_protocol_SyncMessage *message,
                         u16 *slot_out, u64 *generation_out, u32 *class_out, bool *barrier_out) {
    bool barrier;
    u32 class_ = jazz_db__db__channel_endpoint__message_class(message, &barrier);
    i32 fixed = -1;
    switch (class_) {
    case JAZZ_CC_CONTROL: fixed = 0; break;
    case JAZZ_CC_REQUESTS: fixed = 1; break;
    case JAZZ_CC_WRITES: fixed = 2; break;
    case JAZZ_CC_PROGRESS: fixed = JAZZ_WIRE_PROGRESS_CHANNEL; break;
    default: break;
    }
    *class_out = class_;
    *barrier_out = barrier;
    if (fixed >= 0) {
        *slot_out = (u16)fixed;
        *generation_out = 0;
        return JZ_OK;
    }
    jz_bytes key = jazz_db__db__wire_transport__delivery_route_key(message);
    RouteVal *existing = jz_btree_get(&self->routes, &key);
    jz_bytes_drop(&key);
    if (existing) {
        *slot_out = existing->slot;
        *generation_out = existing->generation;
        return JZ_OK;
    }
    i32 found = -1;
    for (u16 slot = 3; slot < JAZZ_WIRE_PROGRESS_CHANNEL; ++slot) {
        if (jazz_db__db__channel_endpoint__ChannelEndpoint__is_idle(&self->endpoint, slot)) { found = slot; break; }
    }
    if (found < 0) return jz_err_new(JAZZ_TE_BACKPRESSURE, NULL);
    u64 generation;
    jz_err e = jazz_db__db__channel_endpoint__ChannelEndpoint__next_idle_generation(&self->endpoint, (u16)found, &generation);
    if (JZ_IS_ERR(e)) return te_failed(e);
    *slot_out = (u16)found;
    *generation_out = generation;
    return JZ_OK;
}

typedef struct { u16 slot; } RetainSlotCtx;
static bool retain_other_slots(const void *k, void *v, void *ctx) {
    (void)k;
    return ((RouteVal *)v)->slot != ((RetainSlotCtx *)ctx)->slot;
}

/* Route and enqueue one canonical message. `*admitted = false` is backpressure. */
static jz_err WTA(enqueue_canonical)(jazz_db_WireTransportAdapter *self, const jazz_protocol_SyncMessage *message, bool *admitted) {
    u16 slot; u64 generation; u32 class_; bool barrier;
    jz_err e = WTA(route)(self, message, &slot, &generation, &class_, &barrier);
    if (e.code == JAZZ_TE_BACKPRESSURE) { jz_err_drop(&e); *admitted = false; return JZ_OK; }
    if (JZ_IS_ERR(e)) return e;
    e = jazz_db__db__channel_endpoint__ChannelEndpoint__enqueue(&self->endpoint, slot, generation, class_, message, barrier);
    if (e.code == JAZZ_TE_BACKPRESSURE) { jz_err_drop(&e); *admitted = false; return JZ_OK; }
    if (JZ_IS_ERR(e)) return e;
    if (slot >= 3 && slot < JAZZ_WIRE_PROGRESS_CHANNEL) {
        RetainSlotCtx ctx = { slot };
        jz_btree_retain(&self->routes, retain_other_slots, &ctx);
        jz_bytes key = jazz_db__db__wire_transport__delivery_route_key(message);
        RouteVal val = { slot, generation };
        if (jz_btree_insert(&self->routes, &key, &val, NULL)) jz_bytes_drop(&key);
    }
    *admitted = true;
    return JZ_OK;
}

/* Admit as many remaining parts of the accepted oversized update as channel credit allows. */
static jz_err WTA(enqueue_outbound_view_parts)(jazz_db_WireTransportAdapter *self) {
    jazz_protocol_SyncMessage part;
    while (jz_deque_pop_front(&self->outbound_view_parts, &part)) {
        bool admitted;
        jz_err e = WTA(enqueue_canonical)(self, &part, &admitted);
        if (JZ_IS_ERR(e)) {
            jazz_protocol__protocol__SyncMessage__drop(&part);
            jz_deque_drop(&self->outbound_view_parts, jazz_protocol__protocol__SyncMessage__drop);
            jz_deque_init(&self->outbound_view_parts, sizeof(jazz_protocol_SyncMessage));
            set_terminal(self, &e);
            return e;
        }
        if (!admitted) {
            jz_deque_push_front(&self->outbound_view_parts, &part);
            break;
        }
        jazz_protocol__protocol__SyncMessage__drop(&part);
    }
    return JZ_OK;
}

static jz_err WTA(fail_view_update_assembly)(jazz_db_WireTransportAdapter *self, const char *reason) {
    jz_btree_clear(&self->inbound_view_parts);
    jazz_protocol_WireError error;
    jazz_protocol__wire__WireError__new(&error, JAZZ_WEC_MALFORMED_FRAME, JAZZ_WR_NEVER, jz_str_from(reason));
    set_last_wire_error(self, error);
    return te_failed_str(reason);
}

/* Hold a received ViewUpdatePart until its final ViewUpdate, and yield that update whole. */
static jz_err WTA(assemble_view_update)(jazz_db_WireTransportAdapter *self, jazz_db_ReceivedSyncMessage received,
                                        bool *has, jazz_db_ReceivedSyncMessage *out) {
    *has = false;
    if (received.message.tag == JAZZ_SM_VIEW_UPDATE_PART) {
        /* The part's lease is released here: it is buffered semantically. */
        if (received.lease.rc) jazz_protocol__wire__channel_credit__BufferLease__drop(&received.lease);
        jazz_protocol_ViewUpdatePayload part = received.message.v.view_update;
        u32 open = self->inbound_view_parts.len;
        bool inserted;
        jazz_db_InboundViewParts *entry = jz_btree_entry(&self->inbound_view_parts, &part.subscription, &inserted);
        if (inserted) { memset(&entry->parts, 0, sizeof entry->parts); entry->receipts_validated = true; }
        if (entry->parts.len == 0 && open >= JAZZ_WIRE_MAX_CHANNELS) {
            jazz_protocol__protocol__ViewUpdatePayload__drop(&part);
            return WTA(fail_view_update_assembly)(self, "more open view-update part sequences than delivery streams");
        }
        JZ_VEC_PUSH(entry->parts, part);
        entry->receipts_validated &= received.receipts_validated;
        return JZ_OK;
    }
    if (received.message.tag == JAZZ_SM_VIEW_UPDATE) {
        jazz_protocol_ViewUpdatePayload last = received.message.v.view_update;
        jazz_db_InboundViewParts buffered;
        if (!jz_btree_remove(&self->inbound_view_parts, &last.subscription, NULL, &buffered)) {
            *out = received;
            *has = true;
            return JZ_OK;
        }
        jazz_protocol_ViewUpdatePayload merged;
        jz_err e = jazz_db__db__view_update_parts__merge_view_update_parts(buffered.parts, last, &merged);
        if (JZ_IS_ERR(e)) {
            jz_str reason = jz_str_clone(e.msg);
            jz_str_push_char(&reason, '\0');
            jz_err_drop(&e);
            if (received.lease.rc) jazz_protocol__wire__channel_credit__BufferLease__drop(&received.lease);
            jz_err fail = WTA(fail_view_update_assembly)(self, reason.ptr);
            jz_str_drop(&reason);
            return fail;
        }
        memset(&out->message, 0, sizeof out->message);
        out->message.tag = JAZZ_SM_VIEW_UPDATE;
        out->message.v.view_update = merged;
        out->lease = received.lease;
        out->receipts_validated = received.receipts_validated && buffered.receipts_validated;
        *has = true;
        return JZ_OK;
    }
    *out = received;
    *has = true;
    return JZ_OK;
}

#define AUX_LOCK(var, msg)                                                             \
    jazz_db_AuxiliaryChannelEndpoint *var = jazz_db_aux_lock(self->auxiliary);        \
    if (!var) return te_failed_str(msg)

/* Send one frame; backpressure stops the turn, other errors are terminal. */
typedef enum { SEND_OK, SEND_BACKPRESSURED, SEND_FAILED } SendResult;
static SendResult send_frame(jazz_db_WireTransportAdapter *self, jz_bytes frame, jz_err *err) {
    jz_err e = self->inner.vt->send_frame(self->inner.self, frame);
    if (!JZ_IS_ERR(e)) return SEND_OK;
    if (e.code == JAZZ_TE_BACKPRESSURE) { jz_err_drop(&e); return SEND_BACKPRESSURED; }
    set_terminal(self, &e);
    *err = e;
    return SEND_FAILED;
}

static jz_err WTA(flush_endpoints)(jazz_db_WireTransportAdapter *self, usize turns, jazz_db_WireFlushStatus *status) {
    jz_err e = jazz_db__db__channel_endpoint__ChannelEndpoint__expire(&self->endpoint);
    if (JZ_IS_ERR(e)) return te_failed(e);
    {
        AUX_LOCK(aux, "auxiliary mutex poisoned");
        e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__expire_incomplete_receive(aux);
        if (JZ_IS_ERR(e)) return te_failed(e);
    }
    for (usize turn = 0; turn < turns; ++turn) {
        bool pump_owned;
        {
            AUX_LOCK(aux, "auxiliary mutex poisoned");
            pump_owned = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__pump_owned(aux);
        }
        if (!pump_owned) {
            jazz_protocol_SharedChannelCredits *credits = jazz_db__db__channel_endpoint__ChannelEndpoint__channel_credits(&self->endpoint);
            if (credits->poisoned) {
                jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
                return te_failed_str("credit mutex poisoned");
            }
            bool has_grant;
            jz_bytes grant;
            e = jazz_protocol__wire__channel_credit__ChannelCredits__peek_grant(credits->inner, &has_grant, &grant);
            if (JZ_IS_ERR(e)) { jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits); return te_failed(e); }
            if (has_grant) {
                jz_err se;
                SendResult r = send_frame(self, grant, &se);
                if (r == SEND_OK) {
                    e = jazz_protocol__wire__channel_credit__ChannelCredits__accept_grant(credits->inner);
                    jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
                    if (JZ_IS_ERR(e)) return te_failed(e);
                    continue;
                }
                jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
                if (r == SEND_BACKPRESSURED) { *status = JAZZ_WFS_BACKPRESSURED; return JZ_OK; }
                return se;
            }
            jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
        }
        if (!pump_owned && self->canonical_turns >= 4) {
            AUX_LOCK(aux, "auxiliary mutex poisoned");
            if (jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__outbound_is_ready(aux)) {
                bool has_frame;
                jz_bytes frame;
                e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__peek_outbound(aux, &has_frame, &frame);
                if (JZ_IS_ERR(e)) return te_failed(e);
                if (has_frame) {
                    jz_err se;
                    SendResult r = send_frame(self, frame, &se);
                    if (r == SEND_OK) {
                        bool had;
                        jazz_protocol_SyncMessage accepted;
                        e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__accept_outbound(aux, &had, &accepted);
                        if (JZ_IS_ERR(e)) return te_failed(e);
                        if (had) jazz_protocol__protocol__SyncMessage__drop(&accepted);
                        self->canonical_turns = 0;
                        continue;
                    }
                    if (r == SEND_BACKPRESSURED) { *status = JAZZ_WFS_BACKPRESSURED; return JZ_OK; }
                    return se;
                }
            }
        }
        bool has_canonical;
        jz_bytes canonical;
        e = jazz_db__db__channel_endpoint__ChannelEndpoint__peek_outbound(&self->endpoint, &has_canonical, &canonical);
        if (JZ_IS_ERR(e)) return te_failed(e);
        if (has_canonical) {
            jz_err se;
            SendResult r = send_frame(self, canonical, &se);
            if (r == SEND_OK) {
                bool completed;
                e = jazz_db__db__channel_endpoint__ChannelEndpoint__accept_outbound(&self->endpoint, &completed);
                if (JZ_IS_ERR(e)) return te_failed(e);
                if (self->canonical_turns < 255) self->canonical_turns += 1;
            } else if (r == SEND_BACKPRESSURED) {
                *status = JAZZ_WFS_BACKPRESSURED;
                return JZ_OK;
            } else {
                return se;
            }
        } else {
            AUX_LOCK(aux, "auxiliary channel mutex poisoned");
            if (jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__pump_owned(aux)) {
                *status = jazz_db__db__channel_endpoint__ChannelEndpoint__has_pending(&self->endpoint) ? JAZZ_WFS_BACKPRESSURED : JAZZ_WFS_IDLE;
                return JZ_OK;
            }
            bool has_frame;
            jz_bytes frame;
            e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__peek_outbound(aux, &has_frame, &frame);
            if (JZ_IS_ERR(e)) return te_failed(e);
            if (!has_frame) {
                *status = (jazz_db__db__channel_endpoint__ChannelEndpoint__has_pending(&self->endpoint) ||
                           jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__has_pending_outbound(aux))
                              ? JAZZ_WFS_BACKPRESSURED
                              : JAZZ_WFS_IDLE;
                return JZ_OK;
            }
            jz_err se;
            SendResult r = send_frame(self, frame, &se);
            if (r == SEND_OK) {
                bool had;
                jazz_protocol_SyncMessage accepted;
                e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__accept_outbound(aux, &had, &accepted);
                if (JZ_IS_ERR(e)) return te_failed(e);
                if (had) jazz_protocol__protocol__SyncMessage__drop(&accepted);
                self->canonical_turns = 0;
            } else if (r == SEND_BACKPRESSURED) {
                *status = JAZZ_WFS_BACKPRESSURED;
                return JZ_OK;
            } else {
                return se;
            }
        }
    }
    AUX_LOCK(aux, "auxiliary channel mutex poisoned");
    *status = (jazz_db__db__channel_endpoint__ChannelEndpoint__has_pending(&self->endpoint) ||
               (!jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__pump_owned(aux) &&
                jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__has_pending_outbound(aux)))
                  ? JAZZ_WFS_MORE_READY
                  : JAZZ_WFS_IDLE;
    return JZ_OK;
}

static jz_err WTA(flush_turn)(jazz_db_WireTransportAdapter *self, usize turns, jazz_db_WireFlushStatus *out) {
    if (self->terminal_error.code) return te_clone(&self->terminal_error);
    JZ_TRY(WTA(enqueue_outbound_view_parts)(self));
    jazz_db_WireFlushStatus status;
    JZ_TRY(WTA(flush_endpoints)(self, turns, &status));
    if (self->outbound_view_parts.len == 0) { *out = status; return JZ_OK; }
    /* A part may have become admissible since the first attempt. */
    JZ_TRY(WTA(enqueue_outbound_view_parts)(self));
    if (status == JAZZ_WFS_IDLE && jazz_db__db__channel_endpoint__ChannelEndpoint__has_pending(&self->endpoint)) *out = JAZZ_WFS_MORE_READY;
    else if (status == JAZZ_WFS_IDLE && self->outbound_view_parts.len != 0) *out = JAZZ_WFS_BACKPRESSURED;
    else *out = status;
    return JZ_OK;
}

/* Best-effort terminal diagnostics. */
static void WTA(send_wire_error)(jazz_db_WireTransportAdapter *self, const jazz_protocol_WireError *error) {
    jazz_protocol_WireFrame frame;
    memset(&frame, 0, sizeof frame);
    frame.tag = JAZZ_WF_ERROR;
    jazz_protocol__wire__WireError__clone(&frame.error, error);
    jz_bytes bytes = {0};
    jz_err e = jazz_protocol__wire__encode_frame(&frame, &bytes);
    jazz_protocol__wire__WireFrame__drop(&frame);
    if (!JZ_IS_ERR(e)) {
        jz_err se = self->inner.vt->send_frame(self->inner.self, bytes);
        if (JZ_IS_ERR(se)) jz_err_drop(&se);
    } else {
        jz_err_drop(&e);
    }
}

static jz_err WTA(receive_routed)(jazz_db_WireTransportAdapter *self, bool *has, jazz_db_ReceivedSyncMessage *out) {
    *has = false;
    if (self->terminal_error.code) return te_clone(&self->terminal_error);
    jazz_db_WireFlushStatus ignored;
    /* Outbound backpressure does not block independent inbound progress. */
    JZ_TRY(WTA(flush_turn)(self, 1, &ignored));
    if (jazz_db__db__channel_endpoint__ChannelEndpoint__pop(&self->endpoint, out)) { *has = true; return JZ_OK; }
    jz_bytes bytes;
    while (self->inner.vt->try_recv_frame(self->inner.self, &bytes)) {
        u32 n = bytes.len;
        jz_err e = jazz_protocol__protocol_limits__validate_wire_frame_len(n);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&bytes); return te_failed(e); }
        jazz_protocol_WireFrame frame;
        e = jazz_protocol__wire__WireInboundContext__decode_frame(self->inbound_context, jz_bytes_slice(&bytes), &frame);
        jz_bytes_drop(&bytes);
        if (JZ_IS_ERR(e)) {
            jazz_protocol_WireError *we = jazz_protocol__wire__WireError__from_err(e);
            jz_err f = jz_err_fmt(JAZZ_TE_FAILED, "%.*s", (int)we->message.len, we->message.ptr);
            jazz_protocol__wire__WireError__drop(we);
            jz_free(we);
            return f;
        }
        if (frame.tag == JAZZ_WF_CHANNEL) {
            e = jazz_protocol__wire__WireInboundContext__validate_channel_metadata(self->inbound_context, &frame.channel);
            if (JZ_IS_ERR(e)) {
                jazz_protocol_WireError *we = jazz_protocol__wire__WireError__from_err(e);
                jz_err f = jz_err_fmt(JAZZ_TE_FAILED, "%.*s", (int)we->message.len, we->message.ptr);
                set_last_wire_error(self, *we);
                jz_free(we);
                jazz_protocol__wire__WireFrame__drop(&frame);
                return f;
            }
        }
        bool got = false;
        jazz_db_ReceivedSyncMessage message;
        switch (frame.tag) {
        case JAZZ_WF_CHANNEL:
            if (frame.channel.extent.channel == JAZZ_WIRE_AUXILIARY_CHANNEL) {
                AUX_LOCK(aux, "auxiliary channel mutex poisoned");
                jazz_protocol_SyncMessage m;
                e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__receive(aux, frame.channel, n, &got, &m);
                if (JZ_IS_ERR(e)) return te_failed(e);
                if (got) jazz_db__db__routed_messages__ReceivedSyncMessage__unleased(&message, m);
            } else {
                e = jazz_db__db__channel_endpoint__ChannelEndpoint__receive(&self->endpoint, frame.channel, n, &got, &message);
                if (JZ_IS_ERR(e)) return te_failed(e);
            }
            break;
        case JAZZ_WF_CHANNEL_CREDIT: {
            jazz_protocol_SharedChannelCredits *credits = jazz_db__db__channel_endpoint__ChannelEndpoint__channel_credits(&self->endpoint);
            if (credits->poisoned) {
                jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
                return te_failed_str("credit mutex poisoned");
            }
            e = jazz_protocol__wire__channel_credit__ChannelCredits__receive_credit(credits->inner, frame.credit);
            jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
            if (JZ_IS_ERR(e)) return te_failed(e);
            break;
        }
        case JAZZ_WF_ERROR: {
            self->received_wire_error = true;
            jz_str dbg = {0};
            jazz_protocol__wire__WireError__debug(&dbg, &frame.error);
            set_last_wire_error(self, frame.error);
            jz_err f = jz_err_fmt(JAZZ_TE_FAILED, "remote wire error: %.*s", (int)dbg.len, dbg.ptr);
            jz_str_drop(&dbg);
            return f;
        }
        default:
            jazz_protocol__wire__WireFrame__drop(&frame);
            return te_failed_str("live wire-v3 transport requires ordered channel frames");
        }
        if (got) {
            JZ_TRY(WTA(flush_turn)(self, 1, &ignored));
            *out = message;
            *has = true;
            return JZ_OK;
        }
    }
    JZ_TRY(WTA(flush_turn)(self, 1, &ignored));
    return JZ_OK;
}

static jz_err WTA(receive)(jazz_db_WireTransportAdapter *self, bool *has, jazz_db_ReceivedSyncMessage *out) {
    /* A buffered part yields nothing on its own; keep draining. */
    for (;;) {
        bool got;
        jazz_db_ReceivedSyncMessage received;
        JZ_TRY(WTA(receive_routed)(self, &got, &received));
        if (!got) break;
        JZ_TRY(WTA(assemble_view_update)(self, received, has, out));
        if (*has) return JZ_OK;
    }
    *has = false;
    return JZ_OK;
}

/* ---- public ---- */

const jazz_protocol_WireError *WTA(remote_wire_error)(const jazz_db_WireTransportAdapter *self) {
    if (self->received_wire_error && self->has_last_wire_error) return &self->last_wire_error;
    return NULL;
}

/* Offer one logical message, handing it back if it was not admitted. */
jz_err WTA(offer)(jazz_db_WireTransportAdapter *self, jazz_protocol_SyncMessage message, jazz_db_WireSendOutcome *out) {
    if (self->terminal_error.code) {
        jazz_protocol__protocol__SyncMessage__drop(&message);
        return te_clone(&self->terminal_error);
    }
    jz_err fe = jazz_protocol__wire__ensure_sync_message_features(&message, jazz_protocol__wire__WireInboundContext__negotiated_features(self->inbound_context));
    if (JZ_IS_ERR(fe)) {
        jazz_protocol_WireError *error = jazz_protocol__wire__WireError__from_err(fe);
        WTA(send_wire_error)(self, error);
        jazz_protocol__wire__WireError__drop(error);
        jz_free(error);
        jazz_protocol__protocol__SyncMessage__drop(&message);
        out->tag = JAZZ_WSO_ACCEPTED;
        return JZ_OK;
    }
    bool barrier;
    if (jazz_db__db__channel_endpoint__message_class(&message, &barrier) == JAZZ_CC_AUXILIARY) {
        jazz_db_AuxiliaryChannelEndpoint *aux = jazz_db_aux_lock(self->auxiliary);
        if (!aux) {
            jazz_protocol__protocol__SyncMessage__drop(&message);
            return te_failed_str("auxiliary channel mutex poisoned");
        }
        jazz_protocol_SyncMessage rejected;
        jz_err e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__try_enqueue(aux, message, &rejected);
        if (JZ_IS_ERR(e)) {
            if (e.code == JAZZ_TE_BACKPRESSURE) {
                jz_err_drop(&e);
                out->tag = JAZZ_WSO_REJECTED;
                out->rejected = rejected;
                return JZ_OK;
            }
            jazz_protocol__protocol__SyncMessage__drop(&rejected);
            return e;
        }
    } else {
        /* The parts of an accepted oversized update go out contiguously. */
        jz_err e = WTA(enqueue_outbound_view_parts)(self);
        if (JZ_IS_ERR(e)) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
        if (self->outbound_view_parts.len != 0) {
            out->tag = JAZZ_WSO_REJECTED;
            out->rejected = message;
            return JZ_OK;
        }
        if (message.tag == JAZZ_SM_VIEW_UPDATE && jazz_db__db__wire_transport__exceeds_routed_payload_limit(&message.v.view_update)) {
            usize limit = jazz_db__db__routed_messages__max_routed_payload_bytes();
            jazz_db_SyncMessageVec parts;
            jazz_db_OversizedViewUpdate oversized;
            if (!jazz_db__db__view_update_parts__split_view_update(message.v.view_update, limit, &parts, &oversized)) {
                jz_str s = {0};
                jazz_db__db__view_update_parts__OversizedViewUpdate__Display__fmt(&oversized, &s);
                jz_err f = jz_err_fmt(JAZZ_TE_FAILED, "%.*s", (int)s.len, s.ptr);
                jz_str_drop(&s);
                return f;
            }
            jz_deque_drop(&self->outbound_view_parts, jazz_protocol__protocol__SyncMessage__drop);
            jz_deque_init(&self->outbound_view_parts, sizeof(jazz_protocol_SyncMessage));
            for (u32 i = 0; i < parts.len; ++i) jz_deque_push_back(&self->outbound_view_parts, &parts.ptr[i]);
            jz_free(parts.ptr);
            JZ_TRY(WTA(enqueue_outbound_view_parts)(self));
        } else {
            bool admitted;
            e = WTA(enqueue_canonical)(self, &message, &admitted);
            if (JZ_IS_ERR(e)) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
            if (!admitted) {
                out->tag = JAZZ_WSO_REJECTED;
                out->rejected = message;
                return JZ_OK;
            }
            jazz_protocol__protocol__SyncMessage__drop(&message);
        }
    }
    /* Semantic ownership is now accepted. */
    jazz_db_WireFlushStatus status;
    jz_err result = WTA(flush_turn)(self, 1, &status);
    if (JZ_IS_ERR(result)) {
        set_terminal(self, &result);
        return result;
    }
    out->tag = JAZZ_WSO_ACCEPTED;
    return JZ_OK;
}

/* ---- impl Transport for WireTransportAdapter ---- */

static jz_err jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_recv_owned_result(void *p, bool *has, struct jazz_db_ReceivedSyncMessage *out) {
    jazz_db_WireTransportAdapter *self = p;
    bool was_terminal = self->terminal_error.code != 0;
    jz_err result = WTA(receive)(self, has, out);
    if (JZ_IS_ERR(result)) {
        if (!was_terminal) {
            jazz_protocol_WireError wire_error;
            if (self->has_last_wire_error) {
                jazz_protocol__wire__WireError__clone(&wire_error, &self->last_wire_error);
            } else if (jazz_db__db__channel_endpoint__ChannelEndpoint__last_wire_error(&self->endpoint, &wire_error)) {
            } else {
                jazz_db_AuxiliaryChannelEndpoint *aux = jazz_db_aux_lock(self->auxiliary);
                if (!(aux && jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__last_wire_error(aux, &wire_error))) {
                    jz_str dbg = {0};
                    te_debug(&dbg, &result);
                    jazz_protocol__wire__WireError__new(&wire_error, JAZZ_WEC_MALFORMED_FRAME, JAZZ_WR_NEVER, dbg);
                }
            }
            if (!self->received_wire_error) WTA(send_wire_error)(self, &wire_error);
            set_last_wire_error(self, wire_error);
        }
        set_terminal(self, &result);
    }
    return result;
}

static jz_err jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_recv_result(void *p, bool *has, jazz_protocol_SyncMessage *out) {
    jazz_db_ReceivedSyncMessage received;
    JZ_TRY(jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_recv_owned_result(p, has, &received));
    if (*has) {
        *out = received.message;
        if (received.lease.rc) jazz_protocol__wire__channel_credit__BufferLease__drop(&received.lease);
    }
    return JZ_OK;
}

/* Strict receive for bootstrap and live channels. */
jz_err WTA(try_recv_strict)(jazz_db_WireTransportAdapter *self, bool *has, jazz_protocol_SyncMessage *out) {
    jz_err error = jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_recv_result(self, has, out);
    if (!JZ_IS_ERR(error)) return JZ_OK;
    jazz_protocol_WireError we;
    if (self->has_last_wire_error) {
        jazz_protocol__wire__WireError__clone(&we, &self->last_wire_error);
    } else {
        jz_str dbg = {0};
        te_debug(&dbg, &error);
        jazz_protocol__wire__WireError__new(&we, JAZZ_WEC_INTERNAL, JAZZ_WR_NEVER, dbg);
    }
    jz_err_drop(&error);
    return jazz_protocol__wire__WireError__into_err(we);
}

static jz_err jazz_db__db__wire_transport__WireTransportAdapter__Transport__send(void *p, jazz_protocol_SyncMessage message) {
    jazz_db_WireSendOutcome outcome;
    JZ_TRY(WTA(offer)(p, message, &outcome));
    if (outcome.tag == JAZZ_WSO_REJECTED) {
        jazz_protocol__protocol__SyncMessage__drop(&outcome.rejected);
        return jz_err_new(JAZZ_TE_BACKPRESSURE, NULL);
    }
    return JZ_OK;
}

static jz_err jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_send(void *p, jazz_protocol_SyncMessage message, bool *returned, jazz_protocol_SyncMessage *back) {
    jazz_db_WireSendOutcome outcome;
    JZ_TRY(WTA(offer)(p, message, &outcome));
    *returned = outcome.tag == JAZZ_WSO_REJECTED;
    if (*returned) *back = outcome.rejected;
    return JZ_OK;
}

static bool jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_recv(void *p, jazz_protocol_SyncMessage *out) {
    bool has = false;
    jz_err e = jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_recv_result(p, &has, out);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    return has;
}

static jz_err jazz_db__db__wire_transport__WireTransportAdapter__Transport__poll_flush(void *p, u32 *status) {
    jazz_db_WireTransportAdapter *self = p;
    jazz_db_WireFlushStatus s;
    jz_err result = WTA(flush_turn)(self, 8, &s);
    if (JZ_IS_ERR(result)) { set_terminal(self, &result); return result; }
    *status = s;
    return JZ_OK;
}

static bool jazz_db__db__wire_transport__WireTransportAdapter__Transport__has_terminal_failure(const void *p) {
    return ((const jazz_db_WireTransportAdapter *)p)->terminal_error.code != 0;
}

static bool jazz_db__db__wire_transport__WireTransportAdapter__Transport__remote_wire_error(const void *p, jazz_protocol_WireError *out) {
    const jazz_protocol_WireError *e = WTA(remote_wire_error)(p);
    if (!e) return false;
    jazz_protocol__wire__WireError__clone(out, e);
    return true;
}

static bool jazz_db__db__wire_transport__WireTransportAdapter__Transport__incomplete_receive_timeout_ms(const void *p, u64 *out) {
    const jazz_db_WireTransportAdapter *self = p;
    u64 aux_ms = 0, ep_ms = 0;
    bool has_aux = false;
    jazz_db_AuxiliaryChannelEndpoint *aux = jazz_db_aux_lock(self->auxiliary);
    if (aux) has_aux = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__incomplete_receive_timeout_ms(aux, &aux_ms);
    bool has_ep = jazz_db__db__channel_endpoint__ChannelEndpoint__incomplete_receive_timeout_ms(&self->endpoint, &ep_ms);
    if (has_ep && has_aux) { *out = ep_ms < aux_ms ? ep_ms : aux_ms; return true; }
    if (has_ep) { *out = ep_ms; return true; }
    if (has_aux) { *out = aux_ms; return true; }
    return false;
}

static struct jazz_db_SharedAuxiliaryEndpoint *jazz_db__db__wire_transport__WireTransportAdapter__Transport__shared_auxiliary_endpoint(const void *p) {
    return jz_rc_clone(((const jazz_db_WireTransportAdapter *)p)->auxiliary);
}

static void jazz_db__db__wire_transport__WireTransportAdapter__Transport__set_trusted_encoder(void *p, bool trusted) {
    jazz_db_WireTransportAdapter *self = p;
    jazz_protocol__wire__WireInboundContext__set_trusted_encoder(self->inbound_context, trusted);
    jazz_db__db__channel_endpoint__ChannelEndpoint__set_trusted_encoder(&self->endpoint, trusted);
    jazz_db_AuxiliaryChannelEndpoint *aux = jazz_db_aux_lock(self->auxiliary);
    if (!aux) jz_panic("auxiliary mutex poisoned");
    jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_trusted_encoder(aux, trusted);
}

static jazz_protocol_WireInboundContext *jazz_db__db__wire_transport__WireTransportAdapter__Transport__wire_inbound_context(const void *p) {
    return jazz_protocol__wire__WireInboundContext__clone(((const jazz_db_WireTransportAdapter *)p)->inbound_context);
}

static bool jazz_db__db__wire_transport__WireTransportAdapter__Transport__connection_session_context(const void *p, jazz_db_ConnectionSessionContext *out) {
    const jazz_db_WireTransportAdapter *self = p;
    if (!self->has_session_context) return false;
    *out = self->session_context;
    return true;
}

static bool jazz_db__db__wire_transport__WireTransportAdapter__Transport__permits_delegated_sessions(const void *p) {
    return ((const jazz_db_WireTransportAdapter *)p)->permits_delegated_sessions;
}

static void adapter_box_drop(void *p) {
    WTA(drop)(p);
    jz_free(p);
}

static const jazz_db_TransportVt wire_transport_adapter_vt = {
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__send,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_send,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_recv,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_recv_result,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__try_recv_owned_result,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__poll_flush,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__has_terminal_failure,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__remote_wire_error,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__incomplete_receive_timeout_ms,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__shared_auxiliary_endpoint,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__set_trusted_encoder,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__wire_inbound_context,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__connection_session_context,
    jazz_db__db__wire_transport__WireTransportAdapter__Transport__permits_delegated_sessions,
    adapter_box_drop,
};

jazz_db_Transport jazz_db__db__wire_transport__WireTransportAdapter__into_transport(jazz_db_WireTransportAdapter *boxed) {
    jazz_db_Transport t = { boxed, &wire_transport_adapter_vt };
    return t;
}
