/* channels.c: jazz_protocol::wire::channels (crates/jazz/layers/protocol/src/wire/channels.rs) */
#include "jz_rt.h"
#include "jazz_protocol/protocol.h"
#include "jazz_protocol/wire.h"

static jz_err str_err(const char *m) { return jz_err_new(JAZZ_WIRE_STR_ERR, m); }

/* ---------------- ChannelFrame ---------------- */
jz_err jazz_protocol__wire__channels__ChannelFrame__validate(const jazz_protocol_ChannelFrame *f) {
    if (f->channel >= JAZZ_MAX_CHANNELS) return str_err("channel slot exceeds connection limit");
    if ((f->channel == JAZZ_AUXILIARY_CHANNEL) != (f->class_ == JAZZ_CC_AUXILIARY))
        return str_err("auxiliary class must use reserved auxiliary channel");
    if ((f->channel == JAZZ_PROGRESS_CHANNEL) != (f->class_ == JAZZ_CC_PROGRESS))
        return str_err("progress class must use reserved progress channel");
    if ((f->channel == JAZZ_CONTROL_CHANNEL) != (f->class_ == JAZZ_CC_CONTROL))
        return str_err("control class must use the reserved channel");
    if ((usize)f->decoded_len > JAZZ_CHANNEL_CHUNK_BYTES || f->payload.len > JAZZ_MAX_CHANNEL_FRAME_PAYLOAD)
        return str_err("channel extent exceeds chunk limit");
    if (f->first) {
        if (f->message_len == 0 || (usize)f->message_len > JAZZ_MAX_LOGICAL_MESSAGE_BYTES)
            return str_err("channel message exceeds logical size limit");
        if (f->decoded_len > f->message_len) return str_err("channel extent exceeds declared message size");
    } else if (f->message_len != 0) {
        return str_err("continuation repeats logical size");
    }
    if (f->decoded_len == 0 || f->payload.len == 0) return str_err("empty channel extent");
    return (jz_err){0};
}
void jazz_protocol__wire__channels__ChannelFrame__Serialize__serialize(jz_pc_w *w, const jazz_protocol_ChannelFrame *f) {
    jz_pc_varint(w, f->channel);
    jz_pc_varint(w, f->generation);
    jz_pc_varint(w, f->sequence);
    jz_pc_variant(w, f->class_);
    jz_pc_bool(w, f->first);
    jz_pc_bool(w, f->last);
    jz_pc_varint(w, f->message_len);
    jz_pc_varint(w, f->decoded_len);
    jz_pc_bytes(w, f->payload.ptr, f->payload.len); /* serde_bytes */
}
jz_err jazz_protocol__wire__channels__ChannelFrame__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_ChannelFrame *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_u16(r, &out->channel));
    JZ_TRY(jz_pc_r_varint(r, &out->generation));
    JZ_TRY(jz_pc_r_varint(r, &out->sequence));
    JZ_TRY(jz_pc_r_variant(r, &out->class_, 7));
    JZ_TRY(jz_pc_r_bool(r, &out->first));
    JZ_TRY(jz_pc_r_bool(r, &out->last));
    JZ_TRY(jz_pc_r_u32(r, &out->message_len));
    JZ_TRY(jz_pc_r_u32(r, &out->decoded_len));
    JZ_TRY(jz_pc_r_bytes(r, &out->payload));
    return (jz_err){0};
}
static jz_err to_string_err(jz_err inner) {
    /* map_err(|error| error.to_string()) */
    jz_str s = {0};
    jazz_protocol__wire__postcard_error_display(&s, &inner);
    jz_err_drop(&inner);
    jz_err e = jz_err_fmt(JAZZ_WIRE_STR_ERR, "%.*s", (int)s.len, s.ptr);
    jz_str_drop(&s);
    return e;
}
jz_err jazz_protocol__wire__channels__ChannelFrame__encode(const jazz_protocol_ChannelFrame *f, jz_bytes *out) {
    JZ_TRY(jazz_protocol__wire__channels__ChannelFrame__validate(f));
    *out = (jz_bytes){0};
    jz_pc_w w = { out };
    jazz_protocol__wire__channels__ChannelFrame__Serialize__serialize(&w, f);
    return (jz_err){0};
}
jz_err jazz_protocol__wire__channels__ChannelFrame__decode(jz_slice bytes, jazz_protocol_ChannelFrame *out) {
    if (bytes.len > JAZZ_CHANNEL_HEADER_BYTES + JAZZ_MAX_CHANNEL_FRAME_PAYLOAD) return str_err("channel frame exceeds size limit");
    jz_pc_r r = { bytes.ptr, bytes.ptr + bytes.len, 0 };
    jz_err e = jazz_protocol__wire__channels__ChannelFrame__Deserialize__deserialize(&r, out);
    if (e.code) { jz_bytes_drop(&out->payload); return to_string_err(e); }
    if (r.p != r.end) { jz_bytes_drop(&out->payload); return str_err("trailing bytes after channel frame"); }
    e = jazz_protocol__wire__channels__ChannelFrame__validate(out);
    if (e.code) jz_bytes_drop(&out->payload);
    return e;
}

/* ---------------- ChannelScheduler ---------------- */
static void channel_message_drop(void *p) { jazz_protocol_ChannelMessage *m = p; jz_free(m->bytes); m->bytes = NULL; }
static void outbound_channel_drop(void *p) { jazz_protocol_OutboundChannel *c = p; jz_deque_drop(&c->messages, channel_message_drop); }
static int u16_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    u16 x = *(const u16 *)a, y = *(const u16 *)b;
    return x < y ? -1 : x > y;
}
static const jz_btree_desc DESC_U16_OUTBOUND = { sizeof(u16), sizeof(jazz_protocol_OutboundChannel), u16_cmp, NULL, outbound_channel_drop };

void jazz_protocol__wire__channels__ChannelScheduler__default(jazz_protocol_ChannelScheduler *s) {
    memset(s, 0, sizeof *s);
    jz_btree_init(&s->channels, &DESC_U16_OUTBOUND);
}
void jazz_protocol__wire__channels__ChannelScheduler__drop(jazz_protocol_ChannelScheduler *s) { jz_btree_drop(&s->channels); }

jz_err jazz_protocol__wire__channels__ChannelScheduler__enqueue(jazz_protocol_ChannelScheduler *s, u16 channel, u64 generation, u32 class_, jz_bytes payload) {
    if (channel >= JAZZ_MAX_CHANNELS
        || (channel == JAZZ_CONTROL_CHANNEL) != (class_ == JAZZ_CC_CONTROL)
        || (channel == JAZZ_AUXILIARY_CHANNEL) != (class_ == JAZZ_CC_AUXILIARY)
        || (channel == JAZZ_PROGRESS_CHANNEL) != (class_ == JAZZ_CC_PROGRESS)) {
        jz_bytes_drop(&payload);
        return str_err("invalid channel slot or class");
    }
    usize len = payload.len;
    if (len == 0 || len > JAZZ_MAX_LOGICAL_MESSAGE_BYTES) { jz_bytes_drop(&payload); return str_err("invalid logical message size"); }
    if (class_ == JAZZ_CC_PROGRESS) {
        if (s->progress_messages >= JAZZ_CONTROL_RESERVE_MESSAGES || s->progress_bytes + len > JAZZ_MAX_LOGICAL_MESSAGE_BYTES) {
            jz_bytes_drop(&payload);
            return str_err("progress channel queue backpressure");
        }
    } else if (s->messages - s->progress_messages >= JAZZ_MAX_CHANNEL_QUEUED_MESSAGES
               || (class_ != JAZZ_CC_CONTROL && s->data_messages >= JAZZ_MAX_CHANNEL_QUEUED_MESSAGES - JAZZ_CONTROL_RESERVE_MESSAGES)
               || (u64)(s->bytes - s->progress_bytes + len) > (u64)JAZZ_MAX_CHANNEL_BUFFER_BYTES - JAZZ_MAX_LOGICAL_MESSAGE_BYTES
               || (class_ != JAZZ_CC_CONTROL && (u64)s->data_bytes + len > (u64)JAZZ_MAX_LOGICAL_MESSAGE_BYTES + JAZZ_INTERACTIVE_RESERVE_BYTES)
               || (len > JAZZ_CHANNEL_CHUNK_BYTES && s->bulk_bytes + len > JAZZ_MAX_LOGICAL_MESSAGE_BYTES)) {
        jz_bytes_drop(&payload);
        return str_err("channel queue backpressure");
    }
    jazz_protocol_OutboundChannel *existing = jz_btree_get(&s->channels, &channel);
    if (existing) {
        if ((existing->generation != generation || existing->class_ != class_)
            && !(jazz_protocol__wire__channels__ChannelScheduler__is_idle(s, channel)
                 && existing->generation != UINT64_MAX && existing->generation + 1 == generation)) {
            jz_bytes_drop(&payload);
            return str_err("channel generation or class changed without reset");
        }
        if (existing->bytes + len > JAZZ_MAX_LOGICAL_MESSAGE_BYTES) { jz_bytes_drop(&payload); return str_err("channel queue backpressure"); }
    }
    bool inserted;
    jazz_protocol_OutboundChannel *state = jz_btree_entry(&s->channels, &channel, &inserted);
    if (inserted) {
        state->class_ = class_;
        state->generation = generation;
        state->sequence = 0;
        state->bytes = 0;
        jz_deque_init(&state->messages, sizeof(jazz_protocol_ChannelMessage));
    }
    if (state->generation != generation) {
        state->generation = generation;
        state->sequence = 0;
        state->class_ = class_;
    }
    state->bytes += len;
    jazz_protocol_ChannelMessage m = { payload.ptr, payload.len, 0 }; /* into_boxed_slice */
    jz_deque_push_back(&state->messages, &m);
    s->bytes += len;
    if (class_ == JAZZ_CC_PROGRESS) {
        s->progress_bytes += len;
        s->progress_messages += 1;
    } else if (len > JAZZ_CHANNEL_CHUNK_BYTES) {
        s->bulk_bytes += len;
    }
    if (class_ != JAZZ_CC_CONTROL && class_ != JAZZ_CC_PROGRESS) {
        s->data_bytes += len;
        s->data_messages += 1;
    }
    s->messages += 1;
    return (jz_err){0};
}

/* Weight classes, then round-robin within each class: a newly admitted request waits at most
 * one finite 20-frame class round, independent of how many large transfers are already active. */
static const u8 ROUND[20] = {
    JAZZ_CC_CONTROL, JAZZ_CC_CONTROL, JAZZ_CC_CONTROL, JAZZ_CC_CONTROL,
    JAZZ_CC_CONTROL, JAZZ_CC_CONTROL, JAZZ_CC_CONTROL, JAZZ_CC_CONTROL,
    JAZZ_CC_REQUESTS, JAZZ_CC_REQUESTS, JAZZ_CC_REQUESTS, JAZZ_CC_REQUESTS,
    JAZZ_CC_DELIVERY, JAZZ_CC_DELIVERY, JAZZ_CC_WRITES, JAZZ_CC_WRITES,
    JAZZ_CC_LARGE_VALUE, JAZZ_CC_AUXILIARY, JAZZ_CC_PROGRESS, JAZZ_CC_PROGRESS,
};
static bool always_credit(u32 class_, void *ctx) { (void)class_; (void)ctx; return true; }
bool jazz_protocol__wire__channels__ChannelScheduler__next_chunk(jazz_protocol_ChannelScheduler *s, jazz_protocol_ScheduledChunk *out) {
    return jazz_protocol__wire__channels__ChannelScheduler__next_chunk_where(s, always_credit, NULL, out);
}
bool jazz_protocol__wire__channels__ChannelScheduler__next_chunk_where(jazz_protocol_ChannelScheduler *s, jazz_protocol_HasCreditFn has_credit, void *ctx, jazz_protocol_ScheduledChunk *out) {
    if (!s->has_selected) {
        for (u32 n = 0; n < 20; n++) {
            u32 class_ = ROUND[s->round_cursor];
            s->round_cursor = (s->round_cursor + 1) % 20;
            u16 after = s->class_cursor[class_];
            bool found = false, found_first = false;
            u16 candidate = 0, first = 0;
            u16 *id; jazz_protocol_OutboundChannel *state;
            JZ_BTREE_FOR(&s->channels, id, state) {
                if (state->class_ != class_ || !has_credit(state->class_, ctx) || state->messages.len == 0) continue;
                if (!found_first) { found_first = true; first = *id; }
                if (*id > after) { found = true; candidate = *id; break; }
            }
            if (!found && found_first) { found = true; candidate = first; }
            if (found) {
                s->class_cursor[class_] = candidate;
                s->has_selected = true;
                s->selected = candidate;
                break;
            }
        }
    }
    if (!s->has_selected) return false;
    u16 channel = s->selected;
    const jazz_protocol_OutboundChannel *state = jz_btree_get(&s->channels, &channel);
    if (!state || state->messages.len == 0) return false;
    const jazz_protocol_ChannelMessage *message = jz_deque_at(&state->messages, 0);
    usize end = message->offset + JAZZ_CHANNEL_CHUNK_BYTES;
    if (end > message->len) end = message->len;
    out->channel = channel;
    out->generation = state->generation;
    out->sequence = state->sequence;
    out->class_ = state->class_;
    out->first = message->offset == 0;
    out->last = end == message->len;
    out->message_len = message->offset == 0 ? message->len : 0;
    out->bytes = (jz_slice){ message->bytes + message->offset, (u32)(end - message->offset) };
    return true;
}
jz_err jazz_protocol__wire__channels__ChannelScheduler__accepted(jazz_protocol_ChannelScheduler *s) {
    if (!s->has_selected) return str_err("no selected channel extent");
    u16 channel = s->selected;
    jazz_protocol_OutboundChannel *state = jz_btree_get(&s->channels, &channel);
    if (!state) jz_panic("called `Option::unwrap()` on a `None` value");
    if (state->sequence == UINT64_MAX) return str_err("channel sequence exhausted");
    u64 next_sequence = state->sequence + 1;
    jazz_protocol_ChannelMessage *message = jz_deque_at(&state->messages, 0);
    if (!message) jz_panic("called `Option::unwrap()` on a `None` value");
    usize len = message->len - message->offset;
    if (len > JAZZ_CHANNEL_CHUNK_BYTES) len = JAZZ_CHANNEL_CHUNK_BYTES;
    message->offset += (u32)len;
    state->sequence = next_sequence;
    /* Retain accounting for the complete allocation until it is dropped. */
    if (message->offset == message->len) {
        usize full = message->len;
        jazz_protocol_ChannelMessage popped;
        jz_deque_pop_front(&state->messages, &popped);
        channel_message_drop(&popped);
        state->bytes -= full;
        s->bytes -= full;
        if (state->class_ == JAZZ_CC_PROGRESS) {
            s->progress_bytes -= full;
            s->progress_messages -= 1;
        } else if (full > JAZZ_CHANNEL_CHUNK_BYTES) {
            s->bulk_bytes -= full;
        }
        if (state->class_ != JAZZ_CC_CONTROL && state->class_ != JAZZ_CC_PROGRESS) {
            s->data_bytes -= full;
            s->data_messages -= 1;
        }
        s->messages -= 1;
    }
    s->has_selected = false;
    return (jz_err){0};
}
jz_err jazz_protocol__wire__channels__ChannelScheduler__next_idle_generation(const jazz_protocol_ChannelScheduler *s, u16 channel, u64 *out) {
    if (!jazz_protocol__wire__channels__ChannelScheduler__is_idle(s, channel))
        return str_err("channel still owns queued or selected messages");
    const jazz_protocol_OutboundChannel *state = jz_btree_get(&s->channels, &channel);
    if (!state) { *out = 0; return (jz_err){0}; }
    if (state->generation == UINT64_MAX) return str_err("channel generation exhausted");
    *out = state->generation + 1;
    return (jz_err){0};
}
bool jazz_protocol__wire__channels__ChannelScheduler__is_idle(const jazz_protocol_ChannelScheduler *s, u16 channel) {
    if (s->has_selected && s->selected == channel) return false;
    const jazz_protocol_OutboundChannel *state = jz_btree_get(&s->channels, &channel);
    return !state || state->messages.len == 0;
}
usize jazz_protocol__wire__channels__ChannelScheduler__queued_bytes(const jazz_protocol_ChannelScheduler *s) { return s->bytes; }
usize jazz_protocol__wire__channels__ChannelScheduler__queued_messages(const jazz_protocol_ChannelScheduler *s) { return s->messages; }
