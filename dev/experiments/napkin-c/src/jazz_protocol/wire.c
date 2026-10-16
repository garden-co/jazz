/* wire.c: jazz_protocol::wire (crates/jazz/layers/protocol/src/wire.rs) */
#include "jz_rt.h"
#include "jazz_protocol/protocol.h"
#include "jazz_protocol/wire.h"

#define TRYF(expr) do { if ((e = (expr)).code) goto fail; } while (0)

/* ---------------- WireError ---------------- */
jz_err jazz_protocol__wire__WireError__new(u32 code, u32 retry, jz_str message) {
    jz_err e = jz_err_fmt(JAZZ_WIRE_ERR | (code << 4) | retry, "%.*s", (int)message.len, message.ptr);
    jz_str_drop(&message);
    return e;
}
static jz_err wire_err(u32 code, u32 retry, const char *msg) {
    return jz_err_new(JAZZ_WIRE_ERR | (code << 4) | retry, msg);
}
void jazz_protocol__wire__WireError__from_err(const jz_err *e, jazz_protocol_WireError *out) {
    out->code = (e->code >> 4) & 0xf;
    out->retry = e->code & 0xf;
    out->message = e->msg ? jz_str_clone(e->msg) : jz_str_from("");
}
bool jazz_protocol__wire__WireError__asks_reconnect_later(const jazz_protocol_WireError *e) {
    return e->retry == JAZZ_RETRY_LATER && (e->code == JAZZ_WEC_BACKPRESSURE || e->code == JAZZ_WEC_NOT_READY);
}
static const char *const WIRE_ERROR_CODE_NAMES[] = {
    "UnsupportedProtocolVersion", "UnsupportedFeature", "MalformedFrame", "AuthFailed",
    "Backpressure", "Internal", "NotReady",
};
static const char *const WIRE_RETRY_NAMES[] = { "Never", "AfterAuth", "AfterResume", "Later" };
void jazz_protocol__wire__WireErrorCode__Debug__fmt(jz_str *out, u32 code) {
    jz_str_push_cstr(out, code < 7 ? WIRE_ERROR_CODE_NAMES[code] : "?");
}
void jazz_protocol__wire__WireRetry__Debug__fmt(jz_str *out, u32 retry) {
    jz_str_push_cstr(out, retry < 4 ? WIRE_RETRY_NAMES[retry] : "?");
}
static void debug_str(jz_str *out, const char *p, u32 n) {
    jz_str_push_char(out, '"');
    for (u32 i = 0; i < n; i++) {
        char c = p[i];
        switch (c) {
        case '"': jz_str_push_cstr(out, "\\\""); break;
        case '\\': jz_str_push_cstr(out, "\\\\"); break;
        case '\n': jz_str_push_cstr(out, "\\n"); break;
        case '\r': jz_str_push_cstr(out, "\\r"); break;
        case '\t': jz_str_push_cstr(out, "\\t"); break;
        default:
            if ((unsigned char)c < 0x20) jz_str_appendf(out, "\\u{%x}", (unsigned)c);
            else jz_str_push_char(out, c);
        }
    }
    jz_str_push_char(out, '"');
}
void jazz_protocol__wire__WireError__Debug__fmt(jz_str *out, const jazz_protocol_WireError *e) {
    jz_str_push_cstr(out, "WireError { code: ");
    jazz_protocol__wire__WireErrorCode__Debug__fmt(out, e->code);
    jz_str_push_cstr(out, ", retry: ");
    jazz_protocol__wire__WireRetry__Debug__fmt(out, e->retry);
    jz_str_push_cstr(out, ", message: ");
    debug_str(out, e->message.ptr, e->message.len);
    jz_str_push_cstr(out, " }");
}
void jazz_protocol__wire__TransportError__Debug__fmt(jz_str *out, const jz_err *e) {
    if (e->code == JAZZ_TRANSPORT_BACKPRESSURE) { jz_str_push_cstr(out, "Backpressure"); return; }
    jz_str_push_cstr(out, "Failed(");
    if (e->msg) debug_str(out, e->msg->ptr, e->msg->len);
    else debug_str(out, "", 0);
    jz_str_push_char(out, ')');
}

void jazz_protocol__wire__postcard_error_display(jz_str *out, const jz_err *e) {
    const char *m;
    switch (e->code) {
    case JZ_PC_EOF: case JAZZ_PROTO_ERR_PC_UNEXPECTED_END: m = "Hit the end of buffer, expected more data"; break;
    case JZ_PC_BAD_VARINT: m = "Found a varint that didn't terminate. Is the usize too big for this platform?"; break;
    case JZ_PC_BAD_VARIANT: m = "Found an enum discriminant that was > u32::max_value()"; break;
    case JZ_PC_BAD_UTF8: m = "Tried to parse invalid utf-8"; break;
    case JZ_PC_BAD_BOOL: m = "Found a bool that wasn't 0 or 1"; break;
    case JAZZ_PROTO_ERR_PC_BAD_OPTION: m = "Found an Option discriminant that wasn't 0 or 1"; break;
    case JAZZ_TYPES_ERR_PC_BAD_ENCODING: case JZ_PC_TRAILING: m = "The original data was not well encoded"; break;
    case JAZZ_PROTO_ERR_PC_SERIALIZE_BUFFER_FULL: m = "The serialize buffer is full"; break;
    case JZ_PC_DEPTH: m = "Serde Deserialization Error"; break;
    default:
        if (e->msg) { jz_str_push(out, e->msg->ptr, e->msg->len); return; }
        m = "Serde Deserialization Error";
    }
    jz_str_push_cstr(out, m);
}
static jz_err wire_err_with_pc(u32 code, u32 retry, const char *prefix, jz_err inner) {
    jz_str s = jz_str_from(prefix);
    jazz_protocol__wire__postcard_error_display(&s, &inner);
    jz_err_drop(&inner);
    return jazz_protocol__wire__WireError__new(code, retry, s);
}

/* ---------------- WireCompression / Hello / Authority ---------------- */
u32 jazz_protocol__wire__WireCompression__from_features(jazz_protocol_WireFeatures f) {
    if (f & JAZZ_FEATURE_PAYLOAD_LZ4) return JAZZ_WC_LZ4;
    if (f & JAZZ_FEATURE_PAYLOAD_ZSTD) return JAZZ_WC_ZSTD;
    return JAZZ_WC_NONE;
}
jazz_protocol_WireFeatures jazz_protocol__wire__WireCompression__feature(u32 c) {
    switch (c) {
    case JAZZ_WC_LZ4: return JAZZ_FEATURE_PAYLOAD_LZ4;
    case JAZZ_WC_ZSTD: return JAZZ_FEATURE_PAYLOAD_ZSTD;
    default: return JAZZ_FEATURE_NONE;
    }
}
jazz_protocol_WireAuthorityEndpoint jazz_protocol__wire__WireAuthorityEndpoint__fresh(jazz_types_NodeUuid node) {
    for (;;) {
        u64 epoch;
        jz_random_bytes((u8 *)&epoch, sizeof epoch);
        if (epoch != 0) {
            jazz_protocol_WireAuthorityEndpoint ep = { node, epoch };
            return ep;
        }
    }
}
void jazz_protocol__wire__WireHello__current(jazz_protocol_WireHello *out, u32 role, jazz_protocol_WireFeatures features) {
    memset(out, 0, sizeof *out);
    out->min_protocol_version = JAZZ_WIRE_PROTOCOL_VERSION;
    out->max_protocol_version = JAZZ_WIRE_PROTOCOL_VERSION;
    out->features = features;
    out->role = role;
    out->has_authority = false;
}
void jazz_protocol__wire__WireHello__with_authority(jazz_protocol_WireHello *h, jazz_types_NodeUuid node, u64 epoch) {
    h->has_authority = true;
    h->authority.node = node;
    h->authority.epoch = epoch;
}

/* ---------------- WireSession ---------------- */
void jazz_protocol__wire__WireSession__Clone__clone(jazz_protocol_WireSession *dst, const jazz_protocol_WireSession *src) {
    dst->session_id = jz_str_clone(&src->session_id);
    dst->epoch = src->epoch;
    dst->has_identity = src->has_identity;
    dst->identity = src->identity;
}
void jazz_protocol__wire__WireSession__drop(jazz_protocol_WireSession *s) { jz_str_drop(&s->session_id); }
static bool wire_session_eq_identity(const jazz_protocol_WireSession *a, const jazz_protocol_WireSession *b) {
    if (a->has_identity != b->has_identity) return false;
    return !a->has_identity || jazz_types_AuthorSubject_eq(&a->identity, &b->identity);
}
static void blake3_hex12(const u8 *p, usize n, jz_str *out) {
    u8 h[32];
    jz_blake3(p, n, h);
    jz_str hex = {0};
    jz_hex_encode(h, 32, &hex);
    jz_str_push(out, hex.ptr, 12);
    jz_str_drop(&hex);
}
void jazz_protocol__wire__WireSession__Debug__fmt(jz_str *out, const jazz_protocol_WireSession *s) {
    jz_str_appendf(out, "WireSession { session_id_len: %u, session_id_fingerprint: \"", s->session_id.len);
    blake3_hex12((const u8 *)s->session_id.ptr, s->session_id.len, out);
    jz_str_appendf(out, "\", epoch: %llu, identity_fingerprint: ", (unsigned long long)s->epoch);
    if (s->has_identity) {
        jz_slice c = jazz_types__ids__AuthorSubject__canonical(&s->identity);
        jz_str_push_cstr(out, "Some(\"");
        blake3_hex12(c.ptr, c.len, out);
        jz_str_push_cstr(out, "\")");
    } else {
        jz_str_push_cstr(out, "None");
    }
    jz_str_push_cstr(out, " }");
}
static void wire_session_serialize(jz_pc_w *w, const jazz_protocol_WireSession *s) {
    jz_pc_str(w, &s->session_id);
    jz_pc_varint(w, s->epoch);
    jz_pc_option(w, s->has_identity);
    if (s->has_identity) jazz_types__ids__AuthorSubject__Serialize__serialize(w, &s->identity);
}
static jz_err wire_session_deserialize(jz_pc_r *r, jazz_protocol_WireSession *out) {
    memset(out, 0, sizeof *out);
    jz_err e;
    TRYF(jz_pc_r_str(r, &out->session_id));
    TRYF(jz_pc_r_varint(r, &out->epoch));
    TRYF(jz_pc_r_option(r, &out->has_identity));
    if (out->has_identity) TRYF(jazz_types__ids__AuthorSubject__Deserialize__deserialize(r, &out->identity));
    return e;
fail:
    jz_str_drop(&out->session_id);
    return e;
}
static void opt_session_serialize(jz_pc_w *w, bool has, const jazz_protocol_WireSession *s) {
    jz_pc_option(w, has);
    if (has) wire_session_serialize(w, s);
}
static jz_err opt_session_deserialize(jz_pc_r *r, bool *has, jazz_protocol_WireSession *s) {
    JZ_TRY(jz_pc_r_option(r, has));
    if (*has) JZ_TRY(wire_session_deserialize(r, s));
    return (jz_err){0};
}

/* ---------------- WireInboundContext ---------------- */
void jazz_protocol__wire__WireInboundContext__new(jazz_protocol_WireInboundContext *out, u16 version, jazz_protocol_WireFeatures features, const jazz_protocol_WireSession *session_moved) {
    memset(out, 0, sizeof *out);
    out->expected_protocol_version = version;
    out->negotiated_features = features;
    if (session_moved) { out->has_expected_session = true; out->expected_session = *session_moved; }
    out->trusted_encoder = false;
}
void jazz_protocol__wire__WireInboundContext__Clone__clone(jazz_protocol_WireInboundContext *dst, const jazz_protocol_WireInboundContext *src) {
    *dst = *src;
    if (src->has_expected_session) jazz_protocol__wire__WireSession__Clone__clone(&dst->expected_session, &src->expected_session);
}
void jazz_protocol__wire__WireInboundContext__drop(jazz_protocol_WireInboundContext *c) {
    if (c->has_expected_session) jazz_protocol__wire__WireSession__drop(&c->expected_session);
    c->has_expected_session = false;
}
void jazz_protocol__wire__WireInboundContext__set_trusted_encoder(jazz_protocol_WireInboundContext *c, bool trusted) { c->trusted_encoder = trusted; }
bool jazz_protocol__wire__WireInboundContext__validates_receipts(const jazz_protocol_WireInboundContext *c) { return !c->trusted_encoder; }

jz_err jazz_protocol__wire__WireInboundContext__decode_semantic_payload(const jazz_protocol_WireInboundContext *c, jz_slice bytes, jazz_protocol_SyncMessage *out) {
    if (c->trusted_encoder) {
        jz_err e = jazz_protocol__wire__decode_sync_message_trusted(bytes, out);
        if (e.code) return wire_err_with_pc(JAZZ_WEC_MALFORMED_FRAME, JAZZ_RETRY_NEVER, "", e);
        e = jazz_protocol__wire__ensure_sync_message_features(out, c->negotiated_features);
        if (e.code) { jazz_protocol__protocol__SyncMessage__drop(out); return e; }
        return e;
    }
    return jazz_protocol__wire__decode_sync_message_for_features(bytes, c->negotiated_features, out);
}
jz_err jazz_protocol__wire__WireInboundContext__validate_channel_metadata(const jazz_protocol_WireInboundContext *c, const jazz_protocol_WireChannelEnvelope *f) {
    return jazz_protocol__wire__WireInboundContext__validate_metadata(c, f->protocol_version, f->features, f->has_session ? &f->session : NULL);
}
static jz_err wire_frame_decode_v(jz_pc_r *r, void *out) { return jazz_protocol__wire__WireFrame__Deserialize__deserialize(r, out); }
static void wire_frame_encode_v(jz_pc_w *w, const void *v) { jazz_protocol__wire__WireFrame__Serialize__serialize(w, v); }
static void wire_frame_drop_v(void *v) { jazz_protocol__wire__WireFrame__drop(v); }
static const jazz_types_PcCodec WIRE_FRAME_CODEC = { wire_frame_decode_v, wire_frame_encode_v, wire_frame_drop_v };

jz_err jazz_protocol__wire__WireInboundContext__decode_frame(const jazz_protocol_WireInboundContext *c, jz_slice bytes, jazz_protocol_WireFrame *out) {
    if (c->trusted_encoder) {
        /* postcard::from_bytes: trailing bytes are ignored */
        jz_pc_r r = { bytes.ptr, bytes.ptr + bytes.len, 0 };
        return jazz_protocol__wire__WireFrame__Deserialize__deserialize(&r, out);
    }
    return jazz_protocol__wire__decode_frame(bytes, out);
}
jz_err jazz_protocol__wire__WireInboundContext__validate_metadata(const jazz_protocol_WireInboundContext *c, u16 protocol_version, jazz_protocol_WireFeatures features, const jazz_protocol_WireSession *session) {
    if (protocol_version != c->expected_protocol_version)
        return jazz_protocol__wire__WireError__new(JAZZ_WEC_UNSUPPORTED_PROTOCOL_VERSION, JAZZ_RETRY_AFTER_RESUME,
            jz_format("wire message protocol version %u does not match negotiated %u", (unsigned)protocol_version, (unsigned)c->expected_protocol_version));
    jazz_protocol_WireFeatures unnegotiated = features & ~c->negotiated_features;
    if (unnegotiated != 0)
        return jazz_protocol__wire__WireError__new(JAZZ_WEC_UNSUPPORTED_FEATURE, JAZZ_RETRY_AFTER_RESUME,
            jz_format("wire message declares unnegotiated features 0x%llx", (unsigned long long)unnegotiated));
    if (!c->has_expected_session) return (jz_err){0};
    const jazz_protocol_WireSession *expected = &c->expected_session;
    if (!session) return wire_err(JAZZ_WEC_AUTH_FAILED, JAZZ_RETRY_AFTER_AUTH, "missing wire session metadata");
    if (!jz_str_eq(&session->session_id, &expected->session_id))
        return wire_err(JAZZ_WEC_AUTH_FAILED, JAZZ_RETRY_AFTER_RESUME, "wire session id does not match this connection");
    if (!wire_session_eq_identity(session, expected))
        return wire_err(JAZZ_WEC_AUTH_FAILED, JAZZ_RETRY_AFTER_AUTH, "wire session identity does not match this connection");
    if (session->epoch < expected->epoch)
        return wire_err(JAZZ_WEC_AUTH_FAILED, JAZZ_RETRY_AFTER_RESUME, "stale wire session epoch");
    if (session->epoch != expected->epoch)
        return wire_err(JAZZ_WEC_AUTH_FAILED, JAZZ_RETRY_AFTER_RESUME, "wire session epoch does not match this connection");
    return (jz_err){0};
}

/* ---------------- WireFrame serde ---------------- */
void jazz_protocol__wire__WireChannelEnvelope__drop(jazz_protocol_WireChannelEnvelope *e) {
    if (e->has_session) jazz_protocol__wire__WireSession__drop(&e->session);
    e->has_session = false;
    jz_bytes_drop(&e->extent.payload);
}
void jazz_protocol__wire__WireFrame__drop(jazz_protocol_WireFrame *f) {
    switch (f->tag) {
    case JAZZ_WF_HELLO: break;
    case JAZZ_WF_ERROR: jz_str_drop(&f->u.error.message); break;
    case JAZZ_WF_CHANNEL: jazz_protocol__wire__WireChannelEnvelope__drop(&f->u.channel); break;
    case JAZZ_WF_CHANNEL_CREDIT:
        if (f->u.credit.has_session) jazz_protocol__wire__WireSession__drop(&f->u.credit.session);
        f->u.credit.has_session = false;
        break;
    }
}
static void credit_kind_serialize(jz_pc_w *w, const jazz_protocol_WireCreditKind *k) {
    jz_pc_variant(w, k->tag);
    if (k->tag == JAZZ_CREDIT_MESSAGES) { jz_pc_varint(w, k->count); jz_pc_bool(w, k->bulk); }
}
static jz_err credit_kind_deserialize(jz_pc_r *r, jazz_protocol_WireCreditKind *k) {
    memset(k, 0, sizeof *k);
    JZ_TRY(jz_pc_r_variant(r, &k->tag, 2));
    if (k->tag == JAZZ_CREDIT_MESSAGES) {
        JZ_TRY(jz_pc_r_u32(r, &k->count));
        JZ_TRY(jz_pc_r_bool(r, &k->bulk));
    }
    return (jz_err){0};
}
void jazz_protocol__wire__WireFrame__Serialize__serialize(jz_pc_w *w, const jazz_protocol_WireFrame *f) {
    jz_pc_variant(w, f->tag);
    switch (f->tag) {
    case JAZZ_WF_HELLO: {
        const jazz_protocol_WireHello *h = &f->u.hello;
        jz_pc_varint(w, h->min_protocol_version);
        jz_pc_varint(w, h->max_protocol_version);
        jz_pc_varint(w, h->features);
        jz_pc_variant(w, h->role);
        jz_pc_option(w, h->has_authority);
        if (h->has_authority) { jz_pc_uuid(w, &h->authority.node); jz_pc_varint(w, h->authority.epoch); }
        break;
    }
    case JAZZ_WF_ERROR:
        jz_pc_variant(w, f->u.error.code);
        jz_pc_variant(w, f->u.error.retry);
        jz_pc_str(w, &f->u.error.message);
        break;
    case JAZZ_WF_CHANNEL: {
        const jazz_protocol_WireChannelEnvelope *c = &f->u.channel;
        jz_pc_varint(w, c->protocol_version);
        jz_pc_varint(w, c->features);
        opt_session_serialize(w, c->has_session, &c->session);
        jazz_protocol__wire__channels__ChannelFrame__Serialize__serialize(w, &c->extent);
        break;
    }
    case JAZZ_WF_CHANNEL_CREDIT: {
        const jazz_protocol_WireChannelCredit *c = &f->u.credit;
        jz_pc_varint(w, c->protocol_version);
        jz_pc_varint(w, c->features);
        opt_session_serialize(w, c->has_session, &c->session);
        jz_pc_variant(w, c->class_);
        jz_pc_varint(w, c->sequence);
        jz_pc_varint(w, c->consumed_bytes);
        credit_kind_serialize(w, &c->kind);
        break;
    }
    }
}
jz_err jazz_protocol__wire__WireFrame__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_WireFrame *out) {
    memset(out, 0, sizeof *out);
    jz_err e;
    TRYF(jz_pc_r_variant(r, &out->tag, 4));
    switch (out->tag) {
    case JAZZ_WF_HELLO: {
        jazz_protocol_WireHello *h = &out->u.hello;
        TRYF(jz_pc_r_u16(r, &h->min_protocol_version));
        TRYF(jz_pc_r_u16(r, &h->max_protocol_version));
        TRYF(jz_pc_r_varint(r, &h->features));
        TRYF(jz_pc_r_variant(r, &h->role, 3));
        TRYF(jz_pc_r_option(r, &h->has_authority));
        if (h->has_authority) {
            TRYF(jz_pc_r_uuid(r, &h->authority.node));
            TRYF(jz_pc_r_varint(r, &h->authority.epoch));
        }
        break;
    }
    case JAZZ_WF_ERROR:
        TRYF(jz_pc_r_variant(r, &out->u.error.code, 7));
        TRYF(jz_pc_r_variant(r, &out->u.error.retry, 4));
        TRYF(jz_pc_r_str(r, &out->u.error.message));
        break;
    case JAZZ_WF_CHANNEL: {
        jazz_protocol_WireChannelEnvelope *c = &out->u.channel;
        TRYF(jz_pc_r_u16(r, &c->protocol_version));
        TRYF(jz_pc_r_varint(r, &c->features));
        TRYF(opt_session_deserialize(r, &c->has_session, &c->session));
        TRYF(jazz_protocol__wire__channels__ChannelFrame__Deserialize__deserialize(r, &c->extent));
        break;
    }
    case JAZZ_WF_CHANNEL_CREDIT: {
        jazz_protocol_WireChannelCredit *c = &out->u.credit;
        TRYF(jz_pc_r_u16(r, &c->protocol_version));
        TRYF(jz_pc_r_varint(r, &c->features));
        TRYF(opt_session_deserialize(r, &c->has_session, &c->session));
        TRYF(jz_pc_r_variant(r, &c->class_, 7));
        TRYF(jz_pc_r_varint(r, &c->sequence));
        TRYF(jz_pc_r_varint(r, &c->consumed_bytes));
        TRYF(credit_kind_deserialize(r, &c->kind));
        break;
    }
    }
    return e;
fail:
    jazz_protocol__wire__WireFrame__drop(out);
    return e;
}

/* ---------------- frame codec ---------------- */
jz_err jazz_protocol__wire__encode_frame(const jazz_protocol_WireFrame *f, jz_bytes *out) {
    *out = (jz_bytes){0};
    jz_pc_w w = { out };
    jazz_protocol__wire__WireFrame__Serialize__serialize(&w, f);
    return (jz_err){0};
}
bool jazz_protocol__wire__is_channel_credit_frame(jz_slice bytes) { return bytes.len > 0 && bytes.ptr[0] == 3; }
jz_err jazz_protocol__wire__decode_frame(jz_slice bytes, jazz_protocol_WireFrame *out) {
    jz_err e = jazz_protocol__protocol_limits__validate_wire_frame_len(bytes.len);
    if (e.code) { jz_err_drop(&e); return jz_err_new(JAZZ_PROTO_ERR_PC_UNEXPECTED_END, NULL); }
    return jazz_types__postcard_exact__decode_postcard_exact(&WIRE_FRAME_CODEC, bytes, out);
}

/* ---------------- sync message codec ---------------- */
jz_err jazz_protocol__wire__encode_sync_message(const jazz_protocol_SyncMessage *m, jz_bytes *out) {
    /* Our encoder owns correctness; do not decode/revalidate its input rows. */
    *out = (jz_bytes){0};
    if (m->tag == JAZZ_SM_VIEW_UPDATE) {
        /* A view update can carry a whole snapshot: size the buffer exactly first. */
        usize len;
        JZ_TRY(jazz_protocol__wire__encoded_sync_message_len(m, &len));
        jz_vec_reserve(out, (u32)len, 1);
    }
    jz_pc_w w = { out };
    jz_err e = jazz_protocol__protocol__SyncMessage__Serialize__serialize(&w, m);
    if (e.code) jz_bytes_drop(out);
    return e;
}
jz_err jazz_protocol__wire__encoded_sync_message_len(const jazz_protocol_SyncMessage *m, usize *out) {
    /* postcard::ser_flavors::Size: the C port measures a scratch encoding. */
    jz_bytes scratch = {0};
    jz_pc_w w = { &scratch };
    jz_err e = jazz_protocol__protocol__SyncMessage__Serialize__serialize(&w, m);
    *out = scratch.len;
    jz_bytes_drop(&scratch);
    return e;
}
jz_err jazz_protocol__wire__encode_sync_message_for_features(const jazz_protocol_SyncMessage *m, jazz_protocol_WireFeatures f, jz_bytes *out) {
    JZ_TRY(jazz_protocol__wire__ensure_sync_message_features(m, f));
    jz_err e = jazz_protocol__wire__encode_sync_message(m, out);
    if (e.code) return wire_err_with_pc(JAZZ_WEC_MALFORMED_FRAME, JAZZ_RETRY_NEVER, "failed to encode sync message payload: ", e);
    return e;
}
jz_err jazz_protocol__wire__decode_sync_message(jz_slice bytes, jazz_protocol_SyncMessage *out) {
    jz_err e = jazz_protocol__protocol_limits__validate_logical_message_len(bytes.len);
    if (e.code) { jz_err_drop(&e); return jz_err_new(JAZZ_PROTO_ERR_PC_UNEXPECTED_END, NULL); }
    /* decode_postcard_exact already rejects any alternate postcard representation. */
    JZ_TRY(jazz_types__postcard_exact__decode_postcard_exact(&jazz_protocol__protocol__SYNC_MESSAGE_CODEC, bytes, out));
    e = jazz_protocol__protocol__SyncMessage__validate_wire_contract(out);
    if (e.code) {
        jz_err_drop(&e);
        jazz_protocol__protocol__SyncMessage__drop(out);
        return jz_err_new(JAZZ_PROTO_ERR_PC_BAD_OPTION, NULL);
    }
    return e;
}
jz_err jazz_protocol__wire__decode_sync_message_trusted(jz_slice bytes, jazz_protocol_SyncMessage *out) {
    jz_pc_r r = { bytes.ptr, bytes.ptr + bytes.len, 0 };
    return jazz_protocol__protocol__SyncMessage__Deserialize__deserialize(&r, out);
}

static jz_err take_canonical_postcard_usize(jz_slice *bytes, usize *out) {
    jz_pc_r r = { bytes->ptr, bytes->ptr + bytes->len, 0 };
    u64 v;
    JZ_TRY(jz_pc_r_varint(&r, &v));
    if (v > 0xffffffffull) return jz_err_new(JZ_PC_BAD_VARINT, NULL);
    u32 consumed = (u32)(r.p - bytes->ptr);
    /* encodes_exactly(&value, &source[..consumed]) */
    jz_bytes again = {0};
    jz_pc_w w = { &again };
    jz_pc_varint(&w, v);
    bool exact = again.len == consumed && memcmp(again.ptr, bytes->ptr, consumed) == 0;
    jz_bytes_drop(&again);
    if (!exact) return jz_err_new(JAZZ_TYPES_ERR_PC_BAD_ENCODING, NULL);
    bytes->ptr += consumed;
    bytes->len -= consumed;
    *out = (usize)v;
    return (jz_err){0};
}
static void bytes_drop_v(void *p) { jz_bytes_drop(p); }
jz_err jazz_protocol__wire__decode_websocket_frame_batch(jz_slice bytes, jazz_protocol_BytesVec *out) {
    *out = (jazz_protocol_BytesVec){0};
    jz_err e = jazz_protocol__protocol_limits__validate_wire_frame_len(bytes.len);
    if (e.code) { jz_err_drop(&e); return jz_err_new(JAZZ_PROTO_ERR_PC_UNEXPECTED_END, NULL); }
    jz_slice remaining = bytes;
    usize count;
    JZ_TRY(take_canonical_postcard_usize(&remaining, &count));
    if (count == 0 || count > JAZZ_MAX_WIRE_BATCH_FRAMES) return jz_err_new(JAZZ_TYPES_ERR_PC_BAD_ENCODING, NULL);
    jz_vec_reserve(out, (u32)count, sizeof(jz_bytes));
    for (usize i = 0; i < count; i++) {
        usize frame_len;
        TRYF(take_canonical_postcard_usize(&remaining, &frame_len));
        e = jazz_protocol__protocol_limits__validate_wire_frame_len(frame_len);
        if (e.code) { jz_err_drop(&e); e = jz_err_new(JAZZ_PROTO_ERR_PC_UNEXPECTED_END, NULL); goto fail; }
        if (frame_len > remaining.len) { e = jz_err_new(JAZZ_PROTO_ERR_PC_UNEXPECTED_END, NULL); goto fail; }
        jz_bytes frame = jz_bytes_from(remaining.ptr, (u32)frame_len);
        jz_vec_push(out, &frame, sizeof frame);
        remaining.ptr += frame_len;
        remaining.len -= (u32)frame_len;
    }
    if (remaining.len == 0) return (jz_err){0};
    e = jz_err_new(JAZZ_TYPES_ERR_PC_BAD_ENCODING, NULL);
fail:
    jz_vec_drop(out, sizeof(jz_bytes), bytes_drop_v);
    return e;
}
jz_err jazz_protocol__wire__encode_websocket_frame_batch(const jz_bytes *frames, u32 n, jz_bytes *out) {
    *out = (jz_bytes){0};
    bool bad = n == 0 || n > JAZZ_MAX_WIRE_BATCH_FRAMES;
    for (u32 i = 0; !bad && i < n; i++) {
        jz_err e = jazz_protocol__protocol_limits__validate_wire_frame_len(frames[i].len);
        if (e.code) { jz_err_drop(&e); bad = true; }
    }
    if (bad) return jz_err_new(JAZZ_PROTO_ERR_PC_SERIALIZE_BUFFER_FULL, NULL);
    jz_pc_w w = { out };
    jz_pc_seq(&w, n);
    for (u32 i = 0; i < n; i++) jz_pc_bytes(&w, frames[i].ptr, frames[i].len);
    jz_err e = jazz_protocol__protocol_limits__validate_wire_frame_len(out->len);
    if (e.code) {
        jz_err_drop(&e);
        jz_bytes_drop(out);
        return jz_err_new(JAZZ_PROTO_ERR_PC_SERIALIZE_BUFFER_FULL, NULL);
    }
    return e;
}
jz_err jazz_protocol__wire__decode_sync_message_for_features(jz_slice bytes, jazz_protocol_WireFeatures f, jazz_protocol_SyncMessage *out) {
    jz_err e = jazz_protocol__wire__decode_sync_message(bytes, out);
    if (e.code) return wire_err_with_pc(JAZZ_WEC_MALFORMED_FRAME, JAZZ_RETRY_NEVER, "failed to decode sync message payload: ", e);
    e = jazz_protocol__wire__ensure_sync_message_features(out, f);
    if (e.code) jazz_protocol__protocol__SyncMessage__drop(out);
    return e;
}
jz_err jazz_protocol__wire__decode_sync_message_for_receive(jz_slice bytes, jazz_protocol_SyncMessage *out) {
    return jazz_protocol__wire__decode_sync_message(bytes, out);
}
jz_err jazz_protocol__wire__ensure_sync_message_features(const jazz_protocol_SyncMessage *m, jazz_protocol_WireFeatures f) {
    u64 required = jazz_protocol__protocol__SyncMessage__required_wire_features(m);
    u64 missing = required & ~f;
    if (missing == 0) return (jz_err){0};
    return jazz_protocol__wire__WireError__new(JAZZ_WEC_UNSUPPORTED_FEATURE, JAZZ_RETRY_AFTER_RESUME,
        jz_format("sync message requires unnegotiated features 0x%llx", (unsigned long long)missing));
}

/* ---------------- features ---------------- */
static jazz_protocol_WireFeatures default_transport_compression_features(void) {
    /* wasm32 + transport-compression-ruzstd */
    return JAZZ_FEATURE_PAYLOAD_ZSTD;
}
jazz_protocol_WireFeatures jazz_protocol__wire__runtime_transport_compression_features(void) {
    /* std::env::var("JAZZ_TRANSPORT_COMPRESSION") is always Err on wasm32-unknown-unknown. */
    return default_transport_compression_features();
}
jazz_protocol_WireFeatures jazz_protocol__wire__current_wire_features(void) {
    return JAZZ_FEATURE_AUTHORIZATION_SCOPE_VIEWS | JAZZ_FEATURE_AUXILIARY_CHUNKS | JAZZ_FEATURE_SCOPE_ISOLATED_CLIENT_RELAY
         | jazz_protocol__wire__runtime_transport_compression_features();
}

/* ---------------- hello negotiation ---------------- */
static jz_err check_wire_protocol_range(u16 min, u16 max) {
    if (min == JAZZ_WIRE_PROTOCOL_VERSION && max == JAZZ_WIRE_PROTOCOL_VERSION) return (jz_err){0};
    return jazz_protocol__wire__WireError__new(JAZZ_WEC_UNSUPPORTED_PROTOCOL_VERSION, JAZZ_RETRY_NEVER,
        jz_format("unsupported wire protocol advertisement: remote %u..=%u, expected %u..=%u",
                  (unsigned)min, (unsigned)max, JAZZ_WIRE_PROTOCOL_VERSION, JAZZ_WIRE_PROTOCOL_VERSION));
}
bool jazz_protocol__wire__wire_hello_version_range(jz_slice bytes, u16 *min, u16 *max) {
    jz_pc_r r = { bytes.ptr, bytes.ptr + bytes.len, 0 };
    u32 tag;
    jz_err e = jz_pc_r_u32(&r, &tag);
    if (e.code) { jz_err_drop(&e); return false; }
    if (tag != 0) return false;
    e = jz_pc_r_u16(&r, min);
    if (!e.code) e = jz_pc_r_u16(&r, max);
    if (e.code) { jz_err_drop(&e); return false; }
    return true;
}
jz_err jazz_protocol__wire__decode_hello_frame(jz_slice bytes, jazz_protocol_WireHello *out) {
    u16 min, max;
    if (jazz_protocol__wire__wire_hello_version_range(bytes, &min, &max)) JZ_TRY(check_wire_protocol_range(min, max));
    jazz_protocol_WireFrame f;
    jz_err e = jazz_protocol__wire__decode_frame(bytes, &f);
    if (e.code) return wire_err_with_pc(JAZZ_WEC_MALFORMED_FRAME, JAZZ_RETRY_NEVER, "malformed WireFrame::Hello: ", e);
    if (f.tag == JAZZ_WF_HELLO) { *out = f.u.hello; return e; }
    jazz_protocol__wire__WireFrame__drop(&f);
    return wire_err(JAZZ_WEC_MALFORMED_FRAME, JAZZ_RETRY_NEVER, "expected the first wire frame to be WireFrame::Hello");
}
jz_err jazz_protocol__wire__negotiate_wire(const jazz_protocol_WireHello *remote, jazz_protocol_WireFeatures local, jazz_protocol_WireNegotiated *out) {
    JZ_TRY(check_wire_protocol_range(remote->min_protocol_version, remote->max_protocol_version));
    out->protocol_version = JAZZ_WIRE_PROTOCOL_VERSION;
    out->features = remote->features & local;
    return (jz_err){0};
}
