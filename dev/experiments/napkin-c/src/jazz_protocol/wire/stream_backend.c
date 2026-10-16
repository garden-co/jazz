/* stream_backend.c: jazz_protocol::wire::stream_backend (crates/jazz/layers/protocol/src/wire/stream_backend.rs)
 * Instant -> jz_now_ms(); Arc<Mutex<ChannelCredits>> -> rc'd ChannelCredits (single-threaded: no poisoning). */
#include "jz_rt.h"
#include "jazz_protocol/protocol.h"
#include "jazz_protocol/wire.h"

static jz_err str_err(const char *m) { return jz_err_new(JAZZ_WIRE_STR_ERR, m); }
static jz_str err_string(const jz_err *e) { return e->msg ? jz_str_clone(e->msg) : jz_str_from(""); }

static jz_err codec(u64 features, bool *some, jazz_compression_Codec *out) {
    switch (features & (JAZZ_FEATURE_PAYLOAD_LZ4 | JAZZ_FEATURE_PAYLOAD_ZSTD)) {
    case 0: *some = false; return (jz_err){0};
    case JAZZ_FEATURE_PAYLOAD_LZ4: *some = true; *out = JAZZ_CODEC_LZ4; return (jz_err){0};
    case JAZZ_FEATURE_PAYLOAD_ZSTD: *some = true; *out = JAZZ_CODEC_ZSTD; return (jz_err){0};
    default: return str_err("channel declares multiple compression codecs");
    }
}

static int u16_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    u16 x = *(const u16 *)a, y = *(const u16 *)b;
    return x < y ? -1 : x > y;
}
static void encoder_slot_drop(void *p) {
    jazz_protocol_ChannelEncoderSlot *s = p;
    if (s->has_encoder) jazz_compression__stream__StreamEncoder__drop(&s->encoder);
    s->has_encoder = false;
}
static void inbound_channel_drop(void *p) {
    jazz_protocol_InboundChannel *s = p;
    if (s->has_decoder) jazz_compression__stream__StreamDecoder__drop(&s->decoder);
    s->has_decoder = false;
    jz_bytes_drop(&s->payload);
    if (s->lease) jazz_protocol__wire__channel_credit__BufferLease__drop(s->lease);
    s->lease = NULL;
}
static const jz_btree_desc DESC_U16_ENCODER = { sizeof(u16), sizeof(jazz_protocol_ChannelEncoderSlot), u16_cmp, NULL, encoder_slot_drop };
static const jz_btree_desc DESC_U16_INBOUND = { sizeof(u16), sizeof(jazz_protocol_InboundChannel), u16_cmp, NULL, inbound_channel_drop };

static usize encoded_extent_capacity(usize chunk_len) {
    usize n = chunk_len + chunk_len / 10 + 64;
    return n < JAZZ_MAX_CHANNEL_FRAME_PAYLOAD ? n : JAZZ_MAX_CHANNEL_FRAME_PAYLOAD;
}
static void bytes_resize_zeroed(jz_bytes *b, u32 len) {
    if (len > b->len) {
        jz_vec_reserve(b, len - b->len, 1);
        memset(b->ptr + b->len, 0, len - b->len);
    }
    b->len = len;
}
static bool grow_encoded_extent(jz_bytes *output) {
    if (output->len >= JAZZ_MAX_CHANNEL_FRAME_PAYLOAD) return false;
    usize grown = (usize)output->len * 2;
    if (grown > JAZZ_MAX_CHANNEL_FRAME_PAYLOAD) grown = JAZZ_MAX_CHANNEL_FRAME_PAYLOAD;
    bytes_resize_zeroed(output, (u32)grown);
    return true;
}

jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__new(const jazz_protocol_WireInboundContext *ctx_moved, jazz_protocol_OrderedChannelBackend *out) {
    memset(out, 0, sizeof *out);
    u64 features = ctx_moved->negotiated_features;
    /* A decode-only browser explicitly emits uncompressed channel bytes; it never substitutes
     * independent per-message frames for a stream. cfg!(feature = "transport-compression-zstd") is false. */
    u32 compression = jazz_protocol__wire__WireCompression__from_features(features);
    u64 selected = compression == JAZZ_WC_ZSTD ? 0 : jazz_protocol__wire__WireCompression__feature(compression);
    out->outbound_features = (features & ~(JAZZ_FEATURE_PAYLOAD_LZ4 | JAZZ_FEATURE_PAYLOAD_ZSTD)) | selected;
    jazz_protocol_WireInboundContext cloned;
    jazz_protocol__wire__WireInboundContext__Clone__clone(&cloned, ctx_moved);
    out->credits = jazz_protocol__wire__channel_credit__ChannelCredits__new_shared(&cloned);
    out->context = *ctx_moved;
    jazz_protocol__wire__channels__ChannelScheduler__default(&out->scheduler);
    jz_btree_init(&out->encoders, &DESC_U16_ENCODER);
    jz_btree_init(&out->inbound, &DESC_U16_INBOUND);
    out->reserved = 0;
    out->has_pending = false;
    out->pending_last = false;
    out->pending_class = JAZZ_CC_CONTROL;
    out->has_failed = false;
    out->idle_timeout_ms = JAZZ_MAX_FRAGMENT_REASSEMBLY_IDLE_MS;
    out->age_timeout_ms = JAZZ_MAX_FRAGMENT_REASSEMBLY_AGE_MS;
    return (jz_err){0};
}

bool jazz_protocol__wire__stream_backend__OrderedChannelBackend__incomplete_receive_timeout_ms(const jazz_protocol_OrderedChannelBackend *b, u64 *out) {
    bool any = false;
    u64 best = UINT64_MAX, now = jz_now_ms();
    u16 *id; jazz_protocol_InboundChannel *state;
    JZ_BTREE_FOR(&b->inbound, id, state) {
        (void)id;
        for (u32 i = 0; i < 2; i++) {
            bool has = i == 0 ? state->has_started : state->has_progressed;
            if (!has) continue;
            u64 at = i == 0 ? state->started_ms : state->progressed_ms;
            u64 limit = i == 0 ? b->age_timeout_ms : b->idle_timeout_ms;
            u64 elapsed = now >= at ? now - at : 0;
            u64 remaining = limit > elapsed ? limit - elapsed : 0; /* saturating_sub, div_ceil(1ms) */
            if (!any || remaining < best) best = remaining;
            any = true;
        }
    }
    if (any) *out = best;
    return any;
}

static void set_failed(jazz_protocol_OrderedChannelBackend *b, const jz_err *e) {
    if (b->has_failed) jz_str_drop(&b->failed);
    b->has_failed = true;
    b->failed = err_string(e);
}
static jz_err failed_err(const jazz_protocol_OrderedChannelBackend *b, u32 code) {
    return jz_err_fmt(code, "%.*s", (int)b->failed.len, b->failed.ptr);
}

jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__expire(jazz_protocol_OrderedChannelBackend *b) {
    bool expired = false;
    u64 now = jz_now_ms();
    u16 *id; jazz_protocol_InboundChannel *state;
    JZ_BTREE_FOR(&b->inbound, id, state) {
        (void)id;
        if ((state->has_started && now - state->started_ms >= b->age_timeout_ms)
            || (state->has_progressed && now - state->progressed_ms >= b->idle_timeout_ms)) {
            expired = true;
            break;
        }
    }
    if (expired) {
        jz_btree_clear(&b->inbound);
        b->reserved = 0;
        jz_err e = str_err("incomplete channel message expired; reconnect required");
        set_failed(b, &e);
        return e;
    }
    return (jz_err){0};
}

static bool str_contains(const jz_str *s, const char *needle) {
    u32 n = (u32)strlen(needle);
    if (n > s->len) return false;
    for (u32 i = 0; i + n <= s->len; i++)
        if (memcmp(s->ptr + i, needle, n) == 0) return true;
    return false;
}
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__enqueue(jazz_protocol_OrderedChannelBackend *b, u16 channel, u64 generation, u32 class_, jz_bytes payload) {
    jz_err e = jazz_protocol__wire__stream_backend__OrderedChannelBackend__expire(b);
    if (e.code) { jz_bytes_drop(&payload); e.code = JAZZ_TRANSPORT_FAILED; return e; }
    if (b->has_failed) { jz_bytes_drop(&payload); return failed_err(b, JAZZ_TRANSPORT_FAILED); }
    jazz_protocol_ChannelCredits *credits = b->credits;
    usize size = payload.len;
    if (!jazz_protocol__wire__channel_credit__ChannelCredits__can_reserve_message(credits, class_, size)) {
        jz_bytes_drop(&payload);
        return jz_err_new(JAZZ_TRANSPORT_BACKPRESSURE, NULL);
    }
    e = jazz_protocol__wire__channels__ChannelScheduler__enqueue(&b->scheduler, channel, generation, class_, payload);
    if (e.code) {
        if (e.msg && str_contains(e.msg, "backpressure")) { jz_err_drop(&e); return jz_err_new(JAZZ_TRANSPORT_BACKPRESSURE, NULL); }
        e.code = JAZZ_TRANSPORT_FAILED;
        return e;
    }
    jazz_protocol__wire__channel_credit__ChannelCredits__reserve_message(credits, class_, size);
    const jazz_protocol_ChannelEncoderSlot *slot = jz_btree_get(&b->encoders, &channel);
    if (slot && slot->generation != generation) jz_btree_remove(&b->encoders, &channel, NULL, NULL);
    return (jz_err){0};
}
void jazz_protocol__wire__stream_backend__OrderedChannelBackend__set_channel_credits(jazz_protocol_OrderedChannelBackend *b, jazz_protocol_SharedChannelCredits c) {
    jz_rc_release(b->credits);
    b->credits = c;
}
jazz_protocol_SharedChannelCredits jazz_protocol__wire__stream_backend__OrderedChannelBackend__channel_credits(const jazz_protocol_OrderedChannelBackend *b) {
    return jz_rc_clone(b->credits);
}
bool jazz_protocol__wire__stream_backend__OrderedChannelBackend__is_idle(const jazz_protocol_OrderedChannelBackend *b, u16 channel) {
    return jazz_protocol__wire__channels__ChannelScheduler__is_idle(&b->scheduler, channel);
}
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__next_idle_generation(const jazz_protocol_OrderedChannelBackend *b, u16 channel, u64 *out) {
    return jazz_protocol__wire__channels__ChannelScheduler__next_idle_generation(&b->scheduler, channel, out);
}
bool jazz_protocol__wire__stream_backend__OrderedChannelBackend__has_pending(const jazz_protocol_OrderedChannelBackend *b) {
    return jazz_protocol__wire__channels__ChannelScheduler__queued_messages(&b->scheduler) != 0;
}

static bool credits_can_send(u32 class_, void *ctx) {
    return jazz_protocol__wire__channel_credit__ChannelCredits__can_send(ctx, class_);
}
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__peek_outbound(jazz_protocol_OrderedChannelBackend *b, bool *some, jz_bytes *out) {
    *some = false;
    JZ_TRY(jazz_protocol__wire__stream_backend__OrderedChannelBackend__expire(b));
    if (b->has_failed) return failed_err(b, JAZZ_WIRE_STR_ERR);
    if (b->has_pending) { *some = true; *out = jz_bytes_clone(&b->pending); return (jz_err){0}; }
    jazz_protocol_ScheduledChunk chunk;
    if (!jazz_protocol__wire__channels__ChannelScheduler__next_chunk_where(&b->scheduler, credits_can_send, b->credits, &chunk))
        return (jz_err){0};
    bool has_codec;
    jazz_compression_Codec c;
    JZ_TRY(codec(b->outbound_features, &has_codec, &c));
    if (!jz_btree_contains(&b->encoders, &chunk.channel)) {
        jazz_protocol_ChannelEncoderSlot slot = { chunk.generation, false, {0} };
        if (has_codec) {
            JZ_TRY(jazz_compression__stream__StreamEncoder__new(c, &slot.encoder));
            slot.has_encoder = true;
        }
        jz_btree_insert(&b->encoders, &chunk.channel, &slot, NULL);
    }
    jazz_protocol_ChannelEncoderSlot *slot = jz_btree_get(&b->encoders, &chunk.channel);
    if (!slot) jz_panic("called `Option::unwrap()` on a `None` value");
    if (slot->generation != chunk.generation) return str_err("outbound codec generation mismatch");
    jz_bytes payload = {0};
    jz_err e;
    if (slot->has_encoder) {
        /* Size the extent for this chunk rather than the largest possible one, and grow only up to
         * the unchanged physical bound when the encoder stalls. */
        bytes_resize_zeroed(&payload, (u32)encoded_extent_capacity(chunk.bytes.len));
        u32 consumed = 0, written = 0;
        jazz_compression_Progress progress;
        while (consumed < chunk.bytes.len) {
            jz_slice rest = { chunk.bytes.ptr + consumed, chunk.bytes.len - consumed };
            e = jazz_compression__stream__StreamEncoder__encode(&slot->encoder, rest, payload.ptr + written, payload.len - written, &progress);
            if (e.code) goto fail;
            if (progress.consumed == 0 && progress.written == 0) {
                if (!grow_encoded_extent(&payload)) { e = str_err("channel encoder exceeded physical extent bound"); goto fail; }
                continue;
            }
            consumed += progress.consumed;
            written += progress.written;
        }
        for (;;) {
            e = jazz_compression__stream__StreamEncoder__flush(&slot->encoder, payload.ptr + written, payload.len - written, &progress);
            if (e.code) goto fail;
            written += progress.written;
            if (progress.finished) break;
            if (progress.written == 0 && !grow_encoded_extent(&payload)) { e = str_err("channel flush exceeded physical extent bound"); goto fail; }
        }
        payload.len = written; /* truncate */
    } else {
        payload = jz_bytes_from(chunk.bytes.ptr, chunk.bytes.len);
    }
    jazz_protocol_WireFrame f;
    memset(&f, 0, sizeof f);
    f.tag = JAZZ_WF_CHANNEL;
    jazz_protocol_ChannelFrame *extent = &f.u.channel.extent;
    extent->channel = chunk.channel;
    extent->generation = chunk.generation;
    extent->sequence = chunk.sequence;
    extent->class_ = chunk.class_;
    extent->first = chunk.first;
    extent->last = chunk.last;
    extent->message_len = chunk.message_len;
    extent->decoded_len = chunk.bytes.len;
    extent->payload = payload;
    e = jazz_protocol__wire__channels__ChannelFrame__validate(extent);
    if (e.code) { jazz_protocol__wire__WireFrame__drop(&f); return e; }
    b->pending_last = extent->last;
    b->pending_class = extent->class_;
    f.u.channel.protocol_version = b->context.expected_protocol_version;
    f.u.channel.features = b->outbound_features;
    f.u.channel.has_session = b->context.has_expected_session;
    if (f.u.channel.has_session) jazz_protocol__wire__WireSession__Clone__clone(&f.u.channel.session, &b->context.expected_session);
    jz_bytes frame;
    e = jazz_protocol__wire__encode_frame(&f, &frame);
    jazz_protocol__wire__WireFrame__drop(&f);
    if (e.code) {
        jz_str s = {0};
        jazz_protocol__wire__postcard_error_display(&s, &e);
        jz_err_drop(&e);
        e = jz_err_fmt(JAZZ_WIRE_STR_ERR, "%.*s", (int)s.len, s.ptr);
        jz_str_drop(&s);
        return e;
    }
    b->has_pending = true;
    b->pending = jz_bytes_clone(&frame);
    *some = true;
    *out = frame;
    return (jz_err){0};
fail:
    jz_bytes_drop(&payload);
    return e;
}

jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__accept_outbound(jazz_protocol_OrderedChannelBackend *b, bool *last) {
    if (!b->has_pending) return str_err("no encoded channel frame to accept");
    JZ_TRY(jazz_protocol__wire__channel_credit__ChannelCredits__charge(b->credits, b->pending_class, b->pending.len));
    JZ_TRY(jazz_protocol__wire__channels__ChannelScheduler__accepted(&b->scheduler));
    jz_bytes_drop(&b->pending);
    b->has_pending = false;
    *last = b->pending_last;
    return (jz_err){0};
}

static jz_err receive_inner(jazz_protocol_OrderedChannelBackend *b, jazz_protocol_WireChannelEnvelope *frame, bool *some, jazz_protocol_ReceivedBytes *out) {
    jz_err e = jazz_protocol__wire__WireInboundContext__validate_channel_metadata(&b->context, frame);
    if (e.code) {
        /* map_err(|error| format!("{error:?}")) */
        jazz_protocol_WireError we;
        jazz_protocol__wire__WireError__from_err(&e, &we);
        jz_err_drop(&e);
        jz_str s = {0};
        jazz_protocol__wire__WireError__Debug__fmt(&s, &we);
        jz_str_drop(&we.message);
        e = jz_err_fmt(JAZZ_WIRE_STR_ERR, "%.*s", (int)s.len, s.ptr);
        jz_str_drop(&s);
        return e;
    }
    const jazz_protocol_ChannelFrame *extent = &frame->extent;
    JZ_TRY(jazz_protocol__wire__channels__ChannelFrame__validate(extent));
    u64 active_features = frame->features & (JAZZ_FEATURE_PAYLOAD_LZ4 | JAZZ_FEATURE_PAYLOAD_ZSTD);
    bool has_codec;
    jazz_compression_Codec c;
    JZ_TRY(codec(active_features, &has_codec, &c));
    bool reset;
    const jazz_protocol_InboundChannel *existing = jz_btree_get(&b->inbound, &extent->channel);
    if (!existing) {
        if (extent->generation != 0) return str_err("new channel must start at generation zero");
        reset = true;
    } else if (existing->generation != extent->generation) {
        if (existing->generation == UINT64_MAX || existing->generation + 1 != extent->generation || existing->message_len != 0)
            return str_err("stale or incomplete channel generation reset");
        reset = true;
    } else {
        reset = false;
    }
    if (reset) {
        if (!extent->first || extent->sequence != 0) return str_err("new channel generation must start at sequence zero");
        if (b->inbound.len == JAZZ_MAX_CHANNELS && !jz_btree_contains(&b->inbound, &extent->channel))
            return str_err("too many channel contexts");
        jazz_protocol_InboundChannel fresh;
        memset(&fresh, 0, sizeof fresh);
        fresh.generation = extent->generation;
        fresh.sequence = 0;
        fresh.class_ = extent->class_;
        fresh.active_features = active_features;
        if (has_codec) {
            JZ_TRY(jazz_compression__stream__StreamDecoder__new(c, &fresh.decoder));
            fresh.has_decoder = true;
        }
        jz_btree_insert(&b->inbound, &extent->channel, &fresh, NULL);
    }
    jazz_protocol_InboundChannel *state = jz_btree_get(&b->inbound, &extent->channel);
    if (!state) jz_panic("called `Option::unwrap()` on a `None` value");
    if (state->sequence != extent->sequence || state->class_ != extent->class_ || state->active_features != active_features)
        return str_err("channel sequence, class or compression changed");
    if (extent->first) {
        if (state->message_len != 0) return str_err("channel starts message before predecessor completes");
        usize size = extent->message_len;
        if ((u64)b->reserved + size > (u64)JAZZ_MAX_CHANNEL_BUFFER_BYTES)
            return str_err("channel declared messages exceed aggregate budget");
        jazz_protocol_BufferLease lease;
        JZ_TRY(jazz_protocol__wire__channel_credit__ChannelCredits__receive_message(b->credits, extent->class_, size, &lease));
        if (state->lease) jazz_protocol__wire__channel_credit__BufferLease__drop(state->lease);
        state->lease = lease;
        /* Reserve the declared length once; the spare byte is the decoder's overrun probe.
         * (try_reserve_exact's "channel reassembly allocation failed" cannot surface: jz_vec_reserve aborts.) */
        jz_vec_reserve(&state->payload, (u32)size + 1, 1);
        b->reserved += size;
        state->message_len = size;
        state->has_started = true;
        state->started_ms = jz_now_ms();
        state->encoded_len = 0;
    } else if (state->message_len == 0) {
        return str_err("channel continuation has no message");
    }
    usize enc = state->encoded_len + extent->payload.len;
    if (enc < state->encoded_len) return str_err("channel encoded length overflow");
    state->encoded_len = enc;
    if ((u64)state->encoded_len > (u64)JAZZ_MAX_ENCODED_MESSAGE_BYTES) return str_err("channel encoded message exceeds size limit");
    usize expected = extent->decoded_len;
    if (state->payload.len + expected > state->message_len) return str_err("channel exceeds declared logical length");
    if (state->has_decoder) {
        /* Decode straight into the reassembly buffer; one spare byte detects a codec overrun. */
        u32 base = state->payload.len;
        jz_vec_reserve(&state->payload, (u32)expected + 1, 1);
        bytes_resize_zeroed(&state->payload, base + (u32)expected + 1);
        u8 *output = state->payload.ptr + base;
        u32 output_len = (u32)expected + 1;
        u32 consumed = 0, written = 0;
        jz_err derr = {0};
        for (;;) {
            jazz_compression_Progress progress;
            jz_slice rest = { extent->payload.ptr + consumed, extent->payload.len - consumed };
            derr = jazz_compression__stream__StreamDecoder__decode(&state->decoder, rest, output + written, output_len - written, &progress);
            if (derr.code) break;
            consumed += progress.consumed;
            written += progress.written;
            if (written > expected || progress.finished) {
                derr = str_err("channel codec ended or exceeded declared decoded extent");
                break;
            }
            if (progress.consumed == 0 && progress.written == 0) break;
        }
        if (!derr.code && (consumed != extent->payload.len || written != expected))
            derr = str_err("channel flush did not produce exact declared decoded extent");
        if (derr.code) {
            state->payload.len = base;
            return derr;
        }
        state->payload.len = base + written;
    } else {
        if (extent->payload.len != expected) return str_err("uncompressed channel extent size mismatch");
        jz_vec_reserve(&state->payload, extent->payload.len, 1);
        jz_bytes_push(&state->payload, extent->payload.ptr, extent->payload.len);
    }
    state->has_progressed = true;
    state->progressed_ms = jz_now_ms();
    if (state->sequence == UINT64_MAX) return str_err("channel sequence exhausted");
    state->sequence += 1;
    if (!extent->last) {
        if (state->payload.len == state->message_len) return str_err("channel omitted final extent flag");
        *some = false;
        return (jz_err){0};
    }
    if (state->payload.len != state->message_len) return str_err("channel completed before declared logical length");
    b->reserved -= state->message_len;
    state->message_len = 0;
    state->has_started = false;
    state->has_progressed = false;
    state->encoded_len = 0;
    out->channel = extent->channel;
    out->class_ = state->class_;
    out->payload = state->payload; /* mem::take */
    state->payload = (jz_bytes){0};
    if (!state->lease) jz_panic("active receive owns buffer reservation");
    out->lease = state->lease;
    state->lease = NULL;
    *some = true;
    return (jz_err){0};
}

jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__receive(jazz_protocol_OrderedChannelBackend *b, jazz_protocol_WireChannelEnvelope frame, usize encoded_len, bool *some, jazz_protocol_ReceivedBytes *out) {
    *some = false;
    jz_err e = jazz_protocol__wire__stream_backend__OrderedChannelBackend__expire(b);
    if (!e.code && b->has_failed) e = failed_err(b, JAZZ_WIRE_STR_ERR);
    if (!e.code) e = jazz_protocol__wire__channel_credit__ChannelCredits__consumed(b->credits, frame.extent.class_, encoded_len);
    if (e.code) { jazz_protocol__wire__WireChannelEnvelope__drop(&frame); return e; }
    e = receive_inner(b, &frame, some, out);
    jazz_protocol__wire__WireChannelEnvelope__drop(&frame);
    if (e.code) {
        jz_btree_clear(&b->inbound);
        b->reserved = 0;
        set_failed(b, &e);
    }
    return e;
}

void jazz_protocol__wire__stream_backend__OrderedChannelBackend__Drop__drop(jazz_protocol_OrderedChannelBackend *b) {
    jazz_protocol__wire__channel_credit__ChannelCredits__close(b->credits);
    jazz_protocol__wire__WireInboundContext__drop(&b->context);
    jz_rc_release(b->credits);
    jazz_protocol__wire__channels__ChannelScheduler__drop(&b->scheduler);
    jz_btree_drop(&b->encoders);
    jz_btree_drop(&b->inbound);
    if (b->has_pending) jz_bytes_drop(&b->pending);
    if (b->has_failed) jz_str_drop(&b->failed);
}
