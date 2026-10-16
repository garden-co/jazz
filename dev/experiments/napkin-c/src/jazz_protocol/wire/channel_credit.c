/* channel_credit.c: jazz_protocol::wire::channel_credit (crates/jazz/layers/protocol/src/wire/channel_credit.rs) */
#include "jz_rt.h"
#include "jazz_protocol/protocol.h"
#include "jazz_protocol/wire.h"

static jz_err str_err(const char *m) { return jz_err_new(JAZZ_WIRE_STR_ERR, m); }

usize jazz_protocol__wire__channel_credit__channel_frame_credit_cost(usize encoded_len) {
    return encoded_len > JAZZ_CHANNEL_FRAME_CREDIT_FLOOR ? encoded_len : JAZZ_CHANNEL_FRAME_CREDIT_FLOOR;
}
static const usize WINDOWS[6] = { 256u * 1024u, 512u * 1024u, 1024u * 1024u, 4u * 1024u * 1024u, 1024u * 1024u, 1024u * 1024u };

static usize bucket(u32 class_) {
    switch (class_) {
    case JAZZ_CC_CONTROL: return 0;
    case JAZZ_CC_REQUESTS: return 1;
    case JAZZ_CC_DELIVERY: return 2;
    case JAZZ_CC_WRITES: case JAZZ_CC_LARGE_VALUE: return 3;
    case JAZZ_CC_AUXILIARY: return 4;
    default: return 5;
    }
}
static u32 bucket_class(usize index) {
    /* Keep the mapping scalar (see Rust comment about ARM64 codegen). */
    switch (index) {
    case 0: return JAZZ_CC_CONTROL;
    case 1: return JAZZ_CC_REQUESTS;
    case 2: return JAZZ_CC_DELIVERY;
    case 3: return JAZZ_CC_WRITES;
    case 4: return JAZZ_CC_AUXILIARY;
    case 5: return JAZZ_CC_PROGRESS;
    default: jz_panic("internal error: entered unreachable code: physical credit bucket out of range");
    }
}
static usize buffer_bucket(u32 class_, usize bytes) {
    if (class_ == JAZZ_CC_AUXILIARY) return 3;
    if (class_ == JAZZ_CC_PROGRESS) return 5;
    if (class_ == JAZZ_CC_CONTROL) return bytes > JAZZ_CHANNEL_CHUNK_BYTES ? 4 : 0;
    return bytes > JAZZ_CHANNEL_CHUNK_BYTES ? 2 : 1;
}
static usize grant_buffer_bucket(u32 class_, bool bulk) {
    return buffer_bucket(class_, bulk ? JAZZ_CHANNEL_CHUNK_BYTES + 1 : 1);
}
static void take_and_wake(jazz_protocol_ChannelCredits *c) {
    if (c->has_waker) {
        c->has_waker = false;
        jz_waker_wake(&c->waker);
    }
}

/* ---------------- BufferLease ---------------- */
static void buffer_lease_inner_drop(void *p) {
    jazz_protocol_BufferLeaseInner *l = p;
    jazz_protocol_ChannelCredits *state = l->owner ? l->owner->target : NULL; /* Weak::upgrade */
    if (state) {
        state->received_buffers[l->bucket].bytes -= l->bytes;
        state->received_buffers[l->bucket].count -= 1;
        if (!state->closed) {
            state->buffer_grants[l->bucket].bytes += l->bytes;
            state->buffer_grants[l->bucket].count += 1;
            take_and_wake(state);
        }
    }
    if (l->owner) jz_rc_release(l->owner);
}
jazz_protocol_BufferLease jazz_protocol__wire__channel_credit__BufferLease__clone(jazz_protocol_BufferLease l) { return jz_rc_clone(l); }
void jazz_protocol__wire__channel_credit__BufferLease__drop(jazz_protocol_BufferLease l) { if (l) jz_rc_release(l); }
void jazz_protocol__wire__channel_credit__BufferLease__Debug__fmt(jz_str *out, jazz_protocol_BufferLease l) {
    jz_str_appendf(out, "BufferLease { bytes: %zu }", (size_t)l->bytes);
}

/* ---------------- ChannelCredits ---------------- */
void jazz_protocol__wire__channel_credit__ChannelCredits__new(jazz_protocol_ChannelCredits *out, const jazz_protocol_WireInboundContext *ctx) {
    memset(out, 0, sizeof *out);
    out->context = *ctx; /* moved */
}
static void channel_credits_drop(void *p) {
    jazz_protocol_ChannelCredits *c = p;
    if (c->weak) { c->weak->target = NULL; jz_rc_release(c->weak); c->weak = NULL; }
    jazz_protocol__wire__WireInboundContext__drop(&c->context);
    if (c->has_pending) jz_bytes_drop(&c->pending_frame);
    c->has_pending = false;
}
jazz_protocol_SharedChannelCredits jazz_protocol__wire__channel_credit__ChannelCredits__new_shared(const jazz_protocol_WireInboundContext *ctx) {
    jazz_protocol_ChannelCredits *c = jz_rc_new(sizeof *c, channel_credits_drop);
    jazz_protocol__wire__channel_credit__ChannelCredits__new(c, ctx);
    c->weak = jz_rc_new(sizeof(jazz_protocol_CreditsWeakCell), NULL);
    c->weak->target = c;
    return c;
}
static bool buffer_can_add(const jazz_protocol_BufferCost costs[6], usize index, usize bytes) {
    if (index == 3 || index == 5) {
        usize n = costs[index].bytes + bytes;
        bool fits = n >= bytes && n <= JAZZ_MAX_LOGICAL_MESSAGE_BYTES;
        return fits && costs[index].count < (index == 5 ? JAZZ_CONTROL_RESERVE_MESSAGES : JAZZ_MAX_CHANNEL_QUEUED_MESSAGES);
    }
    static const u8 canonical[4] = { 0, 1, 2, 4 };
    usize total_bytes = 0, total_count = 0;
    for (u32 i = 0; i < 4; i++) { total_bytes += costs[canonical[i]].bytes; total_count += costs[canonical[i]].count; }
    bool control = index == 0 || index == 4;
    usize n = total_bytes + bytes;
    return n >= bytes
        && (u64)n <= (u64)JAZZ_MAX_LOGICAL_MESSAGE_BYTES + JAZZ_INTERACTIVE_RESERVE_BYTES + JAZZ_CONTROL_RESERVE_BYTES
        && total_count < JAZZ_MAX_CHANNEL_QUEUED_MESSAGES
        && (control
            || ((u64)costs[1].bytes + costs[2].bytes + bytes <= (u64)JAZZ_MAX_LOGICAL_MESSAGE_BYTES + JAZZ_INTERACTIVE_RESERVE_BYTES
                && costs[1].count + costs[2].count < JAZZ_MAX_CHANNEL_QUEUED_MESSAGES - JAZZ_CONTROL_RESERVE_MESSAGES))
        && ((index != 2 && index != 4) || (u64)costs[2].bytes + costs[4].bytes + bytes <= JAZZ_MAX_LOGICAL_MESSAGE_BYTES);
}
bool jazz_protocol__wire__channel_credit__ChannelCredits__can_reserve_message(const jazz_protocol_ChannelCredits *c, u32 class_, usize bytes) {
    return !c->closed && buffer_can_add(c->sent_buffers, buffer_bucket(class_, bytes), bytes);
}
void jazz_protocol__wire__channel_credit__ChannelCredits__reserve_message(jazz_protocol_ChannelCredits *c, u32 class_, usize bytes) {
    JZ_ASSERT(jazz_protocol__wire__channel_credit__ChannelCredits__can_reserve_message(c, class_, bytes),
              "assertion failed: self.can_reserve_message(class, bytes)");
    jazz_protocol_BufferCost *cost = &c->sent_buffers[buffer_bucket(class_, bytes)];
    cost->bytes += bytes;
    cost->count += 1;
}
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__receive_message(jazz_protocol_SharedChannelCredits owner, u32 class_, usize bytes, jazz_protocol_BufferLease *out) {
    /* owner.lock(): single-threaded port, poisoning cannot happen */
    jazz_protocol_ChannelCredits *state = owner;
    usize index = buffer_bucket(class_, bytes);
    if (state->closed || !buffer_can_add(state->received_buffers, index, bytes))
        return str_err("decoded message receive window exceeded");
    state->received_buffers[index].bytes += bytes;
    state->received_buffers[index].count += 1;
    if (!state->weak) {
        state->weak = jz_rc_new(sizeof(jazz_protocol_CreditsWeakCell), NULL);
        state->weak->target = state;
    }
    jazz_protocol_BufferLeaseInner *l = jz_rc_new(sizeof *l, buffer_lease_inner_drop);
    l->owner = jz_rc_clone(state->weak); /* Arc::downgrade */
    l->bucket = (u32)index;
    l->bytes = bytes;
    *out = l;
    return (jz_err){0};
}
void jazz_protocol__wire__channel_credit__ChannelCredits__close(jazz_protocol_ChannelCredits *c) {
    c->closed = true;
    if (c->has_pending) jz_bytes_drop(&c->pending_frame);
    c->has_pending = false;
    memset(c->pending_grants, 0, sizeof c->pending_grants);
    memset(c->buffer_grants, 0, sizeof c->buffer_grants);
    take_and_wake(c);
}
bool jazz_protocol__wire__channel_credit__ChannelCredits__can_send(const jazz_protocol_ChannelCredits *c, u32 class_) {
    usize index = bucket(class_);
    return WINDOWS[index] - c->outstanding[index] >= JAZZ_MAX_CHANNEL_FRAME_PAYLOAD + 256;
}
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__charge(jazz_protocol_ChannelCredits *c, u32 class_, usize encoded_len) {
    usize index = bucket(class_);
    usize cost = jazz_protocol__wire__channel_credit__channel_frame_credit_cost(encoded_len);
    usize next = c->outstanding[index] + cost;
    if (next < cost) return str_err("channel credit arithmetic overflow");
    if (next > WINDOWS[index]) return str_err("channel sender exceeded receive credit");
    c->outstanding[index] = next;
    return (jz_err){0};
}
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__consumed(jazz_protocol_ChannelCredits *c, u32 class_, usize encoded_len) {
    usize index = bucket(class_);
    usize cost = jazz_protocol__wire__channel_credit__channel_frame_credit_cost(encoded_len);
    usize next = c->pending_grants[index] + cost;
    if (next < cost) return str_err("channel grant arithmetic overflow");
    if (next > WINDOWS[index]) return str_err("channel receiver exceeded ungranted window");
    c->pending_grants[index] = next;
    take_and_wake(c);
    return (jz_err){0};
}
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__receive_credit(jazz_protocol_ChannelCredits *c, jazz_protocol_WireChannelCredit grant) {
    jz_err e = jazz_protocol__wire__WireInboundContext__validate_metadata(&c->context, grant.protocol_version, grant.features,
                                                                          grant.has_session ? &grant.session : NULL);
    if (grant.has_session) jazz_protocol__wire__WireSession__drop(&grant.session);
    if (e.code) {
        jazz_protocol_WireError we;
        jazz_protocol__wire__WireError__from_err(&e, &we);
        jz_err_drop(&e);
        jz_str dbg = jz_str_from("invalid credit context: ");
        jazz_protocol__wire__WireError__Debug__fmt(&dbg, &we);
        jz_str_drop(&we.message);
        jz_err out = jz_err_fmt(JAZZ_WIRE_STR_ERR, "%.*s", (int)dbg.len, dbg.ptr);
        jz_str_drop(&dbg);
        return out;
    }
    if (grant.sequence != c->next_received) return str_err("channel credit sequence mismatch");
    usize index = bucket(grant.class_);
    if (grant.consumed_bytes > (u64)SIZE_MAX) return str_err("channel credit does not fit receiver");
    usize amount = (usize)grant.consumed_bytes;
    if (amount == 0) return str_err("empty channel credit");
    if (grant.kind.tag == JAZZ_CREDIT_FRAMES) {
        if (amount > c->outstanding[index]) return str_err("channel credit exceeds outstanding balance");
    } else {
        usize bi = grant_buffer_bucket(grant.class_, grant.kind.bulk);
        const jazz_protocol_BufferCost *cost = &c->sent_buffers[bi];
        if (grant.kind.count == 0 || (usize)grant.kind.count > cost->count || amount > cost->bytes)
            return str_err("message credit exceeds outstanding balance");
    }
    if (c->next_received == UINT64_MAX) return str_err("channel credit sequence exhausted");
    u64 successor = c->next_received + 1;
    if (grant.kind.tag == JAZZ_CREDIT_FRAMES) {
        c->outstanding[index] -= amount;
    } else {
        usize bi = grant_buffer_bucket(grant.class_, grant.kind.bulk);
        c->sent_buffers[bi].bytes -= amount;
        c->sent_buffers[bi].count -= grant.kind.count;
    }
    c->next_received = successor;
    take_and_wake(c);
    return (jz_err){0};
}
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__peek_grant(jazz_protocol_ChannelCredits *c, bool *some, jz_bytes *out) {
    if (c->has_pending) { *some = true; *out = jz_bytes_clone(&c->pending_frame); return (jz_err){0}; }
    usize index = 6, amount = 0;
    u32 class_ = 0;
    jazz_protocol_WireCreditKind kind = {0};
    for (usize i = 0; i < 6; i++) if (c->buffer_grants[i].count != 0) { index = i; break; }
    if (index < 6) {
        jazz_protocol_BufferCost cost = c->buffer_grants[index];
        amount = cost.bytes;
        switch (index) {
        case 0: case 4: class_ = JAZZ_CC_CONTROL; break;
        case 1: class_ = JAZZ_CC_REQUESTS; break;
        case 2: class_ = JAZZ_CC_WRITES; break;
        case 3: class_ = JAZZ_CC_AUXILIARY; break;
        case 5: class_ = JAZZ_CC_PROGRESS; break;
        default: jz_panic("internal error: entered unreachable code: message credit bucket out of range");
        }
        kind.tag = JAZZ_CREDIT_MESSAGES;
        kind.count = (u32)cost.count;
        kind.bulk = index == 2 || index == 4;
    } else {
        for (usize i = 0; i < 6; i++) if (c->pending_grants[i] != 0) { index = i; break; }
        if (index == 6) { *some = false; return (jz_err){0}; }
        amount = c->pending_grants[index];
        class_ = bucket_class(index);
        kind.tag = JAZZ_CREDIT_FRAMES;
    }
    jazz_protocol_WireFrame f;
    memset(&f, 0, sizeof f);
    f.tag = JAZZ_WF_CHANNEL_CREDIT;
    f.u.credit.protocol_version = c->context.expected_protocol_version;
    f.u.credit.features = c->context.negotiated_features;
    f.u.credit.has_session = c->context.has_expected_session;
    if (f.u.credit.has_session) jazz_protocol__wire__WireSession__Clone__clone(&f.u.credit.session, &c->context.expected_session);
    f.u.credit.class_ = class_;
    f.u.credit.sequence = c->next_sent;
    f.u.credit.consumed_bytes = amount;
    f.u.credit.kind = kind;
    jz_bytes frame;
    jz_err e = jazz_protocol__wire__encode_frame(&f, &frame);
    jazz_protocol__wire__WireFrame__drop(&f);
    if (e.code) {
        jz_str s = {0};
        jazz_protocol__wire__postcard_error_display(&s, &e);
        jz_err_drop(&e);
        jz_err out_e = jz_err_fmt(JAZZ_WIRE_STR_ERR, "%.*s", (int)s.len, s.ptr);
        jz_str_drop(&s);
        return out_e;
    }
    c->has_pending = true;
    c->pending_frame = jz_bytes_clone(&frame);
    c->pending_index = (u32)index;
    c->pending_amount = amount;
    c->pending_kind = kind;
    *some = true;
    *out = frame;
    return (jz_err){0};
}
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__accept_grant(jazz_protocol_ChannelCredits *c) {
    if (c->next_sent == UINT64_MAX) return str_err("channel credit sequence exhausted");
    u64 next = c->next_sent + 1;
    if (!c->has_pending) return str_err("no channel credit grant pending");
    c->has_pending = false;
    jz_bytes_drop(&c->pending_frame);
    usize index = c->pending_index, amount = c->pending_amount;
    if (c->pending_kind.tag == JAZZ_CREDIT_FRAMES) {
        if (c->pending_grants[index] < amount) return str_err("channel grant accounting underflow");
        c->pending_grants[index] -= amount;
    } else {
        c->buffer_grants[index].bytes -= amount;
        c->buffer_grants[index].count -= c->pending_kind.count;
    }
    c->next_sent = next;
    return (jz_err){0};
}
static bool grant_ready(const jazz_protocol_ChannelCredits *c) {
    if (c->has_pending) return true;
    for (u32 i = 0; i < 6; i++) if (c->pending_grants[i] != 0) return true;
    for (u32 i = 0; i < 6; i++) if (c->buffer_grants[i].count != 0) return true;
    return false;
}
bool jazz_protocol__wire__channel_credit__ChannelCredits__has_pending_grant(const jazz_protocol_ChannelCredits *c) { return grant_ready(c); }
bool jazz_protocol__wire__channel_credit__ChannelCredits__poll_grant_ready(jazz_protocol_ChannelCredits *c, const jz_waker *cx) {
    if (grant_ready(c)) return true; /* Poll::Ready(()) */
    c->has_waker = true;
    c->waker = *cx;
    return false;
}
void jazz_protocol__wire__channel_credit__ChannelCredits__register_waker(jazz_protocol_ChannelCredits *c, const jz_waker *cx) {
    c->has_waker = true;
    c->waker = *cx;
}
