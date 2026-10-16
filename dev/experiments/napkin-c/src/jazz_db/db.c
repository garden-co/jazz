/* db.c: crates/jazz/layers/db/src/db.rs
 * High-level thread-affine database facade. */
#include "db.h"
#include "db/mutation_errors.h"

/* ======================================================================
 * Peer chunk routing (db.rs:98-1447)
 * ====================================================================== */

static int u64_cmp(const void *a, const void *b, void *ctx) { return jz_cmp_u64(a, b, ctx); }
static const jz_btree_desc u64_set_desc = { sizeof(u64), 0, u64_cmp, NULL, NULL };

static void pending_chunk_demand_drop(void *p);
static void request_vec_drop(void *p) { jz_vec_drop(p, sizeof(jazz_protocol_ChunkResponseEntry), jazz_protocol__protocol__ChunkResponseEntry__drop); }
static void request_vec_deque_drop(void *p) { jz_deque_drop(p, request_vec_drop); }
static void trace_deque_drop(void *p) { jz_deque_drop(p, jazz_db__db__PeerIoTraceEntry__drop); }

static const jz_btree_desc pending_by_chunk_desc = { sizeof(groove_ChunkRequest), sizeof(jazz_db_PendingChunkDemand),
                                                     groove__chunks__ChunkRequest__cmp, groove__chunks__ChunkRequest__drop, pending_chunk_demand_drop };
static const jz_btree_desc chunk_by_upstream_desc = { sizeof(u64), sizeof(groove_ChunkRequest), u64_cmp, NULL, groove__chunks__ChunkRequest__drop };
static const jz_btree_desc relay_responses_desc = { sizeof(u64), sizeof(jazz_db_ChunkResponseEntryVec), u64_cmp, NULL, request_vec_drop };
static const jz_btree_desc inflight_relay_desc = { sizeof(u64), sizeof(jz_deque), u64_cmp, NULL, request_vec_deque_drop };
static const jz_btree_desc wakers_desc = { sizeof(u64), sizeof(jz_waker), u64_cmp, NULL, NULL };
static const jz_btree_desc trace_desc = { sizeof(u64), sizeof(jz_deque), u64_cmp, NULL, trace_deque_drop };

void jazz_db__db__PeerIoTraceEntry__drop(void *p) {
    jazz_db_PeerIoTraceEntry *e = p;
    jz_str_drop(&e->object_hash);
    jz_str_drop(&e->locator_fingerprint);
}
void jazz_db__db__PeerIoTraceEntry__clone(void *dst, const void *src) {
    jazz_db_PeerIoTraceEntry *d = dst;
    const jazz_db_PeerIoTraceEntry *s = src;
    *d = *s;
    d->object_hash = jz_str_clone(&s->object_hash);
    d->locator_fingerprint = jz_str_clone(&s->locator_fingerprint);
}

static void chunk_demand_waiter_drop(void *p) {
    jazz_db_ChunkDemandWaiter *w = p;
    if (w->tag == JAZZ_CDW_LOCAL && w->sender) jz_chan_close(w->sender); /* Sender drop closes the oneshot */
}
static void pending_chunk_demand_drop(void *p) {
    jazz_db_PendingChunkDemand *d = p;
    jz_vec_drop(&d->waiters, sizeof(jazz_db_ChunkDemandWaiter), chunk_demand_waiter_drop);
}

static jz_str jazz_db__db__short_fingerprint(const u8 *bytes, u32 n) {
    u8 hash[32];
    jz_blake3(bytes, n, hash);
    jz_str hex = {0};
    jz_hex_encode(hash, 32, &hex);
    jz_str out = {0};
    jz_str_push(&out, hex.ptr, 12);
    jz_str_drop(&hex);
    return out;
}

static const char *jazz_db__db__response_kind(const jazz_protocol_ChunkResponse *response) {
    switch (response->tag) {
    case JAZZ_CHUNK_FOUND: return "found";
    case JAZZ_CHUNK_UNAVAILABLE: return "unavailable";
    default: return "retryable";
    }
}

static const char *jazz_db__db__storage_error_kind(const groove_ChunkStorageError *error) {
    switch (error->tag) {
    case GROOVE_CSE_UNAVAILABLE: return "unavailable";
    case GROOVE_CSE_LOCATOR_CONFLICT: return "locator-conflict";
    case GROOVE_CSE_INTEGRITY: return "integrity";
    default: return "backend";
    }
}

static const char *jazz_db__db__PeerIoPumpRole__as_str(u32 role) {
    return role == JAZZ_PIO_UPSTREAM ? "upstream" : "subscriber";
}

static void chunk_demand_state_drop(void *p) {
    jazz_db_ChunkDemandState *s = p;
    jz_btree_drop(&s->pending_by_chunk);
    jz_btree_drop(&s->chunk_by_upstream_id);
    jz_deque_drop(&s->outbound, jazz_protocol__protocol__ChunkRequestEntry__drop);
    jz_btree_drop(&s->relay_responses);
    jz_btree_drop(&s->inflight_relay_responses);
    jz_btree_drop(&s->outbound_wakers);
    jz_btree_drop(&s->disconnected_connections);
    jz_btree_drop(&s->upstream_connections);
    jz_btree_drop(&s->trace_by_connection);
    jz_btree_drop(&s->traced_connections);
}

void jazz_db__db__PeerChunkResolver__default(jazz_db_PeerChunkResolver *out) {
    jazz_db_ChunkDemandState *s = jz_rc_new(sizeof *s, chunk_demand_state_drop);
    memset(s, 0, sizeof *s);
    jz_btree_init(&s->pending_by_chunk, &pending_by_chunk_desc);
    jz_btree_init(&s->chunk_by_upstream_id, &chunk_by_upstream_desc);
    jz_deque_init(&s->outbound, sizeof(jazz_protocol_ChunkRequestEntry));
    jz_btree_init(&s->relay_responses, &relay_responses_desc);
    jz_btree_init(&s->inflight_relay_responses, &inflight_relay_desc);
    jz_btree_init(&s->outbound_wakers, &wakers_desc);
    jz_btree_init(&s->disconnected_connections, &u64_set_desc);
    jz_btree_init(&s->upstream_connections, &u64_set_desc);
    jz_btree_init(&s->trace_by_connection, &trace_desc);
    jz_btree_init(&s->traced_connections, &u64_set_desc);
    out->state = s;
}
jazz_db_PeerChunkResolver jazz_db__db__PeerChunkResolver__clone(const jazz_db_PeerChunkResolver *r) {
    jazz_db_PeerChunkResolver c = { jz_rc_clone(r->state) };
    return c;
}
void jazz_db__db__PeerChunkResolver__drop(jazz_db_PeerChunkResolver *r) { jz_rc_release(r->state); }

static void jazz_db__db__PeerChunkResolver__record_request(const jazz_db_PeerChunkResolver *self, u64 connection, u32 role,
                                                          const char *event, const jazz_protocol_ChunkRequestEntry *request,
                                                          const jazz_protocol_ChunkResponse *response,
                                                          const groove_ChunkStorageError *storage_error) {
    jazz_db_ChunkDemandState *state = self->state;
    if (!jz_btree_contains(&state->traced_connections, &connection)) return;
    bool inserted;
    jz_deque *trace = jz_btree_entry(&state->trace_by_connection, &connection, &inserted);
    if (inserted) jz_deque_init(trace, sizeof(jazz_db_PeerIoTraceEntry));
    if (trace->len >= JAZZ_DB_MAX_CHUNK_RELAY_TRACE_EVENTS_PER_CONNECTION) {
        jazz_db_PeerIoTraceEntry old;
        if (jz_deque_pop_front(trace, &old)) jazz_db__db__PeerIoTraceEntry__drop(&old);
    }
    jazz_db_PeerIoTraceEntry entry;
    entry.event = event;
    entry.role = jazz_db__db__PeerIoPumpRole__as_str(role);
    entry.connection = connection;
    entry.request_id = request->request_id;
    entry.remaining_hops = request->remaining_hops;
    entry.object_hash = jazz_db__db__short_fingerprint(request->expected_hash, 32);
    entry.locator_fingerprint = jazz_db__db__short_fingerprint(request->locator.ptr, request->locator.len);
    entry.response = response ? jazz_db__db__response_kind(response) : NULL;
    entry.storage_error = storage_error ? jazz_db__db__storage_error_kind(storage_error) : NULL;
    jz_deque_push_back(trace, &entry);
}

static jazz_db_PeerIoTraceVec jazz_db__db__PeerChunkResolver__take_trace(const jazz_db_PeerChunkResolver *self, u64 connection) {
    jazz_db_PeerIoTraceVec out = {0};
    jz_deque trace;
    if (!jz_btree_remove(&self->state->trace_by_connection, &connection, NULL, &trace)) return out;
    jazz_db_PeerIoTraceEntry e;
    while (jz_deque_pop_front(&trace, &e)) JZ_VEC_PUSH(out, e);
    jz_deque_drop(&trace, NULL);
    return out;
}

static void jazz_db__db__PeerChunkResolver__set_trace_enabled(const jazz_db_PeerChunkResolver *self, u64 connection, bool enabled) {
    jazz_db_ChunkDemandState *state = self->state;
    if (enabled) {
        jz_btree_insert(&state->traced_connections, &connection, NULL, NULL);
    } else {
        jz_btree_remove(&state->traced_connections, &connection, NULL, NULL);
        jz_btree_remove(&state->trace_by_connection, &connection, NULL, NULL);
    }
}

bool jazz_db__db__PeerChunkResolver__has_pending_local_demand(const jazz_db_PeerChunkResolver *r) {
    void *k, *v;
    JZ_BTREE_FOR(&r->state->pending_by_chunk, k, v) {
        jazz_db_PendingChunkDemand *pending = v;
        JZ_VEC_FOR(jazz_db_ChunkDemandWaiter, w, pending->waiters) {
            if (w->tag == JAZZ_CDW_LOCAL) return true;
        }
    }
    return false;
}

u64 jazz_db__db__PeerChunkResolver__completion_generation(const jazz_db_PeerChunkResolver *r) {
    return r->state->completion_generation;
}

static void jazz_db__db__PeerChunkResolver__wake_connection(jazz_db_ChunkDemandState *state, u64 connection) {
    jz_waker waker;
    if (jz_btree_remove(&state->outbound_wakers, &connection, NULL, &waker)) jz_waker_wake(&waker);
}

/* Put every demand absent from the outbound queue back on the active upstream. */
static void jazz_db__db__PeerChunkResolver__requeue_pending_demands(jazz_db_ChunkDemandState *state) {
    jz_btree queued;
    jz_btree_init(&queued, &u64_set_desc);
    for (u32 i = 0; i < state->outbound.len; ++i) {
        jazz_protocol_ChunkRequestEntry *entry = jz_deque_at(&state->outbound, i);
        jz_btree_insert(&queued, &entry->request_id, NULL, NULL);
    }
    jazz_db_ChunkRequestEntryVec retries = {0};
    void *k, *v;
    JZ_BTREE_FOR(&state->pending_by_chunk, k, v) {
        const groove_ChunkRequest *request = k;
        jazz_db_PendingChunkDemand *pending = v;
        if (jz_btree_contains(&queued, &pending->upstream_id)) continue;
        jazz_protocol_ChunkRequestEntry entry;
        entry.request_id = pending->upstream_id;
        entry.locator = jz_bytes_clone(&request->locator);
        memcpy(entry.expected_hash, request->object_hash, 32);
        entry.remaining_hops = pending->remaining_hops;
        JZ_VEC_PUSH(retries, entry);
    }
    jz_btree_drop(&queued);
    JZ_VEC_FOR(jazz_protocol_ChunkRequestEntry, e, retries) jz_deque_push_back(&state->outbound, e);
    jz_free(retries.ptr);
}

static void jazz_db__db__PeerChunkResolver__register_connection(const jazz_db_PeerChunkResolver *self, u64 connection, bool upstream) {
    jazz_db_ChunkDemandState *state = self->state;
    jz_btree_remove(&state->disconnected_connections, &connection, NULL, NULL);
    if (upstream) {
        jz_btree_insert(&state->upstream_connections, &connection, NULL, NULL);
        if (!state->has_upstream_connection) {
            state->has_upstream_connection = true;
            state->upstream_connection = connection;
            /* Requeue in-flight demand when a replacement registers. */
            jazz_db__db__PeerChunkResolver__requeue_pending_demands(state);
            jazz_db__db__PeerChunkResolver__wake_connection(state, connection);
        }
    }
}

static bool jazz_db__db__PeerChunkResolver__reserve_relay_obligation(jazz_db_ChunkDemandState *state) {
    if (state->relay_chunk_obligations >= JAZZ_DB_MAX_RELAY_CHUNK_OBLIGATIONS) return false;
    state->relay_chunk_obligations += 1;
    return true;
}

static bool jazz_db__db__PeerChunkResolver__queue_relay_response(jazz_db_ChunkDemandState *state, u64 connection,
                                                                jazz_protocol_ChunkResponseEntry response, bool reservation_transferred) {
    if (!reservation_transferred && !jazz_db__db__PeerChunkResolver__reserve_relay_obligation(state)) {
        jazz_protocol__protocol__ChunkResponseEntry__drop(&response);
        return false;
    }
    bool inserted;
    jazz_db_ChunkResponseEntryVec *queue = jz_btree_entry(&state->relay_responses, &connection, &inserted);
    if (inserted) memset(queue, 0, sizeof *queue);
    JZ_VEC_PUSH(*queue, response);
    jazz_db__db__PeerChunkResolver__wake_connection(state, connection);
    return true;
}

static void jazz_db__db__PeerChunkResolver__enqueue_relay_responses(const jazz_db_PeerChunkResolver *self, u64 connection,
                                                                   jazz_db_ChunkResponseEntryVec responses) {
    u32 i = 0;
    for (; i < responses.len; ++i) {
        if (!jazz_db__db__PeerChunkResolver__queue_relay_response(self->state, connection, responses.ptr[i], false)) { ++i; break; }
    }
    for (; i < responses.len; ++i) jazz_protocol__protocol__ChunkResponseEntry__drop(&responses.ptr[i]);
    jz_free(responses.ptr);
}

/* Ok -> true. Err(waiter) -> false and the waiter is handed back. */
static bool jazz_db__db__PeerChunkResolver__enqueue(const jazz_db_PeerChunkResolver *self, groove_ChunkRequest request,
                                                   u8 remaining_hops, jazz_db_ChunkDemandWaiter waiter, jazz_db_ChunkDemandWaiter *back) {
    jazz_db_ChunkDemandState *state = self->state;
    bool relay_waiter = waiter.tag == JAZZ_CDW_RELAY;
    if (relay_waiter && !jazz_db__db__PeerChunkResolver__reserve_relay_obligation(state)) {
        groove__chunks__ChunkRequest__drop(&request);
        *back = waiter;
        return false;
    }
    jazz_db_PendingChunkDemand *pending = jz_btree_get(&state->pending_by_chunk, &request);
    if (pending) {
        JZ_VEC_PUSH(pending->waiters, waiter);
        groove__chunks__ChunkRequest__drop(&request);
        return true;
    }
    if (state->pending_by_chunk.len >= JAZZ_DB_MAX_PENDING_CHUNK_DEMANDS) {
        state->relay_chunk_obligations -= relay_waiter ? 1 : 0;
        groove__chunks__ChunkRequest__drop(&request);
        *back = waiter;
        return false;
    }
    u64 next = state->next_request_id + 1;
    state->next_request_id = next > 1 ? next : 1;
    u64 upstream_id = state->next_request_id;
    jazz_protocol_ChunkRequestEntry entry;
    entry.request_id = upstream_id;
    entry.locator = jz_bytes_clone(&request.locator);
    memcpy(entry.expected_hash, request.object_hash, 32);
    entry.remaining_hops = remaining_hops;
    jz_deque_push_back(&state->outbound, &entry);
    groove_ChunkRequest request_clone;
    groove__chunks__ChunkRequest__clone(&request_clone, &request);
    jz_btree_insert(&state->chunk_by_upstream_id, &upstream_id, &request_clone, NULL);
    jazz_db_PendingChunkDemand demand;
    demand.upstream_id = upstream_id;
    demand.remaining_hops = remaining_hops;
    memset(&demand.waiters, 0, sizeof demand.waiters);
    JZ_VEC_PUSH(demand.waiters, waiter);
    jz_btree_insert(&state->pending_by_chunk, &request, &demand, NULL);
    if (state->has_upstream_connection) jazz_db__db__PeerChunkResolver__wake_connection(state, state->upstream_connection);
    return true;
}

static void jazz_db__db__PeerChunkResolver__enqueue_relay(const jazz_db_PeerChunkResolver *self, u64 connection, jazz_protocol_ChunkRequestEntry request) {
    if (request.remaining_hops == 0) {
        jazz_protocol_ChunkResponseEntry response;
        memset(&response, 0, sizeof response);
        response.request_id = request.request_id;
        response.result.tag = JAZZ_CHUNK_UNAVAILABLE;
        jazz_db__db__PeerChunkResolver__queue_relay_response(self->state, connection, response, false);
        jazz_protocol__protocol__ChunkRequestEntry__drop(&request);
        return;
    }
    groove_ChunkRequest chunk;
    memcpy(chunk.object_hash, request.expected_hash, 32);
    chunk.locator = request.locator;
    jazz_db_ChunkDemandWaiter waiter;
    memset(&waiter, 0, sizeof waiter);
    waiter.tag = JAZZ_CDW_RELAY;
    waiter.connection = connection;
    waiter.request_id = request.request_id;
    jazz_db_ChunkDemandWaiter back;
    if (!jazz_db__db__PeerChunkResolver__enqueue(self, chunk, request.remaining_hops - 1, waiter, &back)) {
        if (back.tag == JAZZ_CDW_RELAY) {
            jazz_protocol_ChunkResponseEntry response;
            memset(&response, 0, sizeof response);
            response.request_id = back.request_id;
            response.result.tag = JAZZ_CHUNK_RETRYABLE;
            response.result.retry_after_ms = 25;
            jazz_db__db__PeerChunkResolver__queue_relay_response(self->state, back.connection, response, false);
        }
    }
}

static jazz_db_ChunkRequestEntryVec jazz_db__db__PeerChunkResolver__take_outbound(const jazz_db_PeerChunkResolver *self, usize limit) {
    jazz_db_ChunkDemandState *state = self->state;
    /* The wire decoder rejects batches above this cardinality. */
    usize count = limit;
    if (count > JAZZ_PROTOCOL_MAX_CHUNK_REQUEST_BATCH_ENTRIES) count = JAZZ_PROTOCOL_MAX_CHUNK_REQUEST_BATCH_ENTRIES;
    if (count > state->outbound.len) count = state->outbound.len;
    jazz_db_ChunkRequestEntryVec out = {0};
    for (usize i = 0; i < count; ++i) {
        jazz_protocol_ChunkRequestEntry e;
        jz_deque_pop_front(&state->outbound, &e);
        JZ_VEC_PUSH(out, e);
    }
    return out;
}

static jazz_db_ChunkResponseEntryVec clone_responses(const jazz_db_ChunkResponseEntryVec *v) {
    jazz_db_ChunkResponseEntryVec out = {0};
    jz_vec_clone(&out, v, sizeof(jazz_protocol_ChunkResponseEntry), jazz_protocol__protocol__ChunkResponseEntry__clone);
    return out;
}

static bool responses_eq(const jazz_db_ChunkResponseEntryVec *a, const jazz_protocol_ChunkResponseEntry *b, u32 blen) {
    if (a->len != blen) return false;
    for (u32 i = 0; i < blen; ++i)
        if (!jazz_protocol__protocol__ChunkResponseEntry__eq(&a->ptr[i], &b[i])) return false;
    return true;
}

static jazz_db_ChunkResponseEntryVec jazz_db__db__PeerChunkResolver__take_relay_responses(const jazz_db_PeerChunkResolver *self, u64 connection, usize limit) {
    jazz_db_ChunkDemandState *state = self->state;
    jazz_db_ChunkResponseEntryVec responses = {0};
    jazz_db_ChunkResponseEntryVec *queued = jz_btree_get(&state->relay_responses, &connection);
    if (!queued) return responses;
    usize count = limit < queued->len ? limit : queued->len;
    jz_vec_extend(&responses, queued->ptr, (u32)count, sizeof(jazz_protocol_ChunkResponseEntry));
    memmove(queued->ptr, queued->ptr + count, (queued->len - count) * sizeof(jazz_protocol_ChunkResponseEntry));
    queued->len -= (u32)count;
    bool exhausted = queued->len == 0;
    if (exhausted) jz_btree_remove(&state->relay_responses, &connection, NULL, NULL);
    bool inserted;
    jz_deque *inflight = jz_btree_entry(&state->inflight_relay_responses, &connection, &inserted);
    if (inserted) jz_deque_init(inflight, sizeof(jazz_db_ChunkResponseEntryVec));
    jazz_db_ChunkResponseEntryVec copy = clone_responses(&responses);
    jz_deque_push_back(inflight, &copy);
    return responses;
}

static u32 inflight_position(jz_deque *inflight, const jazz_protocol_ChunkResponseEntry *responses, u32 n, const char *missing) {
    for (u32 i = 0; i < inflight->len; ++i) {
        if (responses_eq(jz_deque_at(inflight, i), responses, n)) return i;
    }
    jz_panic(missing);
    return 0;
}

static void deque_remove_at(jz_deque *q, u32 position, void *out) {
    /* VecDeque::remove(position) */
    jz_deque tmp;
    jz_deque_init(&tmp, q->elem);
    u8 *buf = jz_alloc(q->elem);
    u32 len = q->len;
    for (u32 i = 0; i < len; ++i) {
        jz_deque_pop_front(q, buf);
        if (i == position) memcpy(out, buf, q->elem);
        else jz_deque_push_back(&tmp, buf);
    }
    jz_free(buf);
    jz_deque_drop(q, NULL);
    *q = tmp;
}

static void jazz_db__db__PeerChunkResolver__restore_relay_responses(const jazz_db_PeerChunkResolver *self, u64 connection,
                                                                   jazz_db_ChunkResponseEntryVec responses) {
    jazz_db_ChunkDemandState *state = self->state;
    jz_deque *inflight = jz_btree_get(&state->inflight_relay_responses, &connection);
    if (!inflight) jz_panic("restored relay response batch was handed out");
    u32 position = inflight_position(inflight, responses.ptr, responses.len, "restored relay response batch is still in flight");
    jazz_db_ChunkResponseEntryVec removed;
    deque_remove_at(inflight, position, &removed);
    request_vec_drop(&removed);
    if (inflight->len == 0) jz_btree_remove(&state->inflight_relay_responses, &connection, NULL, NULL);
    bool inserted;
    jazz_db_ChunkResponseEntryVec *queue = jz_btree_entry(&state->relay_responses, &connection, &inserted);
    if (inserted) memset(queue, 0, sizeof *queue);
    /* splice(0..0, responses) */
    for (u32 i = 0; i < responses.len; ++i)
        jz_vec_insert(queue, i, &responses.ptr[i], sizeof(jazz_protocol_ChunkResponseEntry));
    jz_free(responses.ptr);
}

static void jazz_db__db__PeerChunkResolver__acknowledge_relay_response_send(const jazz_db_PeerChunkResolver *self, u64 connection,
                                                                           const jazz_protocol_ChunkResponseEntry *responses, u32 n) {
    jazz_db_ChunkDemandState *state = self->state;
    jz_deque *inflight = jz_btree_get(&state->inflight_relay_responses, &connection);
    if (!inflight) jz_panic("acknowledged relay response batch was handed out");
    u32 position = inflight_position(inflight, responses, n, "acknowledged relay response batch is still in flight");
    if (position >= inflight->len) jz_panic("located relay response batch remains removable");
    jazz_db_ChunkResponseEntryVec released;
    deque_remove_at(inflight, position, &released);
    if (inflight->len == 0) jz_btree_remove(&state->inflight_relay_responses, &connection, NULL, NULL);
    if (state->relay_chunk_obligations < released.len) jz_panic("acknowledged relay responses are accounted");
    state->relay_chunk_obligations -= released.len;
    request_vec_drop(&released);
}

static bool jazz_db__db__PeerChunkResolver__is_active_upstream(const jazz_db_PeerChunkResolver *self, u64 connection) {
    return self->state->has_upstream_connection && self->state->upstream_connection == connection;
}

static void jazz_db__db__PeerChunkResolver__complete(const jazz_db_PeerChunkResolver *self, jazz_protocol_ChunkResponseEntry response) {
    jazz_db_ChunkDemandState *state = self->state;
    groove_ChunkRequest request;
    if (!jz_btree_remove(&state->chunk_by_upstream_id, &response.request_id, NULL, &request)) {
        jazz_protocol__protocol__ChunkResponseEntry__drop(&response);
        return;
    }
    jazz_db_PendingChunkDemand pending;
    bool found = jz_btree_remove(&state->pending_by_chunk, &request, NULL, &pending);
    groove__chunks__ChunkRequest__drop(&request);
    if (!found) {
        jazz_protocol__protocol__ChunkResponseEntry__drop(&response);
        return;
    }
    state->completion_generation += 1;
    JZ_VEC_FOR(jazz_db_ChunkDemandWaiter, waiter, pending.waiters) {
        if (waiter->tag == JAZZ_CDW_LOCAL) {
            groove_ChunkResult result;
            memset(&result, 0, sizeof result);
            switch (response.result.tag) {
            case JAZZ_CHUNK_FOUND:
                result.ok = true;
                result.bytes = jz_bytes_clone(&response.result.found);
                break;
            case JAZZ_CHUNK_UNAVAILABLE:
                result.err.tag = GROOVE_CHUNK_ERR_UNAVAILABLE;
                break;
            default:
                result.err.tag = GROOVE_CHUNK_ERR_RETRYABLE;
                result.err.retry_after_ms = response.result.retry_after_ms;
                break;
            }
            jz_chan_send(waiter->sender, &result);
            jz_chan_close(waiter->sender);
        } else {
            jazz_protocol_ChunkResponseEntry relayed;
            relayed.request_id = waiter->request_id;
            jazz_protocol__protocol__ChunkResponse__clone(&relayed.result, &response.result);
            jazz_db__db__PeerChunkResolver__queue_relay_response(state, waiter->connection, relayed, true);
        }
    }
    jz_free(pending.waiters.ptr);
    jazz_protocol__protocol__ChunkResponseEntry__drop(&response);
}

typedef struct { u64 id; } RetainIdCtx;
static bool keep_other_request(void *item, void *ctx) {
    return ((jazz_protocol_ChunkRequestEntry *)item)->request_id != ((RetainIdCtx *)ctx)->id;
}
static void deque_retain_requests(jz_deque *q, u64 upstream_id) {
    jz_deque kept;
    jz_deque_init(&kept, q->elem);
    jazz_protocol_ChunkRequestEntry e;
    RetainIdCtx ctx = { upstream_id };
    while (jz_deque_pop_front(q, &e)) {
        if (keep_other_request(&e, &ctx)) jz_deque_push_back(&kept, &e);
        else jazz_protocol__protocol__ChunkRequestEntry__drop(&e);
    }
    jz_deque_drop(q, NULL);
    *q = kept;
}

typedef JZ_VEC(groove_ChunkRequest) ChunkRequestVec;
static ChunkRequestVec pending_request_keys(jazz_db_ChunkDemandState *state) {
    ChunkRequestVec requests = {0};
    void *k, *v;
    JZ_BTREE_FOR(&state->pending_by_chunk, k, v) {
        groove_ChunkRequest c;
        groove__chunks__ChunkRequest__clone(&c, k);
        JZ_VEC_PUSH(requests, c);
    }
    return requests;
}

/* Fail every local chunk read that still waits on an upstream. */
void jazz_db__db__PeerChunkResolver__fail_local_demand_for_close(const jazz_db_PeerChunkResolver *self) {
    jazz_db_ChunkDemandState *state = self->state;
    ChunkRequestVec requests = pending_request_keys(state);
    bool failed = false;
    JZ_VEC_FOR(groove_ChunkRequest, request, requests) {
        jazz_db_PendingChunkDemand *pending = jz_btree_get(&state->pending_by_chunk, request);
        if (!pending) continue;
        jazz_db_ChunkDemandWaiterVec waiters = pending->waiters;
        memset(&pending->waiters, 0, sizeof pending->waiters);
        JZ_VEC_FOR(jazz_db_ChunkDemandWaiter, waiter, waiters) {
            if (waiter->tag == JAZZ_CDW_LOCAL) {
                failed = true;
                groove_ChunkResult result;
                memset(&result, 0, sizeof result);
                result.err.tag = GROOVE_CHUNK_ERR_UNAVAILABLE;
                jz_chan_send(waiter->sender, &result);
                jz_chan_close(waiter->sender);
            } else {
                JZ_VEC_PUSH(pending->waiters, *waiter);
            }
        }
        jz_free(waiters.ptr);
        if (pending->waiters.len == 0) {
            u64 upstream_id = pending->upstream_id;
            jz_btree_remove(&state->pending_by_chunk, request, NULL, NULL);
            jz_btree_remove(&state->chunk_by_upstream_id, &upstream_id, NULL, NULL);
            deque_retain_requests(&state->outbound, upstream_id);
        }
    }
    jz_vec_drop(&requests, sizeof(groove_ChunkRequest), groove__chunks__ChunkRequest__drop);
    if (failed) state->completion_generation += 1;
}

static void jazz_db__db__PeerChunkResolver__cancel_local(const jazz_db_PeerChunkResolver *self, const groove_ChunkRequest *request, u64 waiter_id) {
    jazz_db_ChunkDemandState *state = self->state;
    jazz_db_PendingChunkDemand *pending = jz_btree_get(&state->pending_by_chunk, request);
    if (!pending) return;
    u32 w = 0;
    for (u32 r = 0; r < pending->waiters.len; ++r) {
        jazz_db_ChunkDemandWaiter *waiter = &pending->waiters.ptr[r];
        if (waiter->tag == JAZZ_CDW_LOCAL && waiter->waiter_id == waiter_id) { chunk_demand_waiter_drop(waiter); continue; }
        pending->waiters.ptr[w++] = *waiter;
    }
    pending->waiters.len = w;
    if (pending->waiters.len == 0) {
        u64 upstream_id = pending->upstream_id;
        jz_btree_remove(&state->pending_by_chunk, request, NULL, NULL);
        jz_btree_remove(&state->chunk_by_upstream_id, &upstream_id, NULL, NULL);
        deque_retain_requests(&state->outbound, upstream_id);
    }
}

static void jazz_db__db__PeerChunkResolver__disconnect(const jazz_db_PeerChunkResolver *self, u64 connection, bool upstream) {
    jazz_db_ChunkDemandState *state = self->state;
    jz_btree_insert(&state->disconnected_connections, &connection, NULL, NULL);
    jz_btree_remove(&state->trace_by_connection, &connection, NULL, NULL);
    jz_btree_remove(&state->traced_connections, &connection, NULL, NULL);
    jz_waker disconnected_waker;
    bool has_waker = jz_btree_remove(&state->outbound_wakers, &connection, NULL, &disconnected_waker);
    usize queued_responses = 0;
    jazz_db_ChunkResponseEntryVec queued;
    if (jz_btree_remove(&state->relay_responses, &connection, NULL, &queued)) {
        queued_responses = queued.len;
        request_vec_drop(&queued);
    }
    usize inflight_responses = 0;
    jz_deque batches;
    if (jz_btree_remove(&state->inflight_relay_responses, &connection, NULL, &batches)) {
        for (u32 i = 0; i < batches.len; ++i) inflight_responses += ((jazz_db_ChunkResponseEntryVec *)jz_deque_at(&batches, i))->len;
        request_vec_deque_drop(&batches);
    }
    if (state->relay_chunk_obligations < queued_responses + inflight_responses) jz_panic("disconnected relay responses are accounted");
    state->relay_chunk_obligations -= queued_responses + inflight_responses;
    if (upstream) {
        jz_btree_remove(&state->upstream_connections, &connection, NULL, NULL);
        if (state->has_upstream_connection && state->upstream_connection == connection) {
            void *first, *unused;
            state->has_upstream_connection = jz_btree_first(&state->upstream_connections, &first, &unused);
            if (state->has_upstream_connection) {
                state->upstream_connection = *(u64 *)first;
                u64 successor = state->upstream_connection;
                jazz_db__db__PeerChunkResolver__requeue_pending_demands(state);
                jazz_db__db__PeerChunkResolver__wake_connection(state, successor);
            }
        }
        if (has_waker) jz_waker_wake(&disconnected_waker);
        return;
    }
    ChunkRequestVec requests = pending_request_keys(state);
    JZ_VEC_FOR(groove_ChunkRequest, request, requests) {
        jazz_db_PendingChunkDemand *pending = jz_btree_get(&state->pending_by_chunk, request);
        if (!pending) continue;
        usize removed_relay_waiters = 0;
        u32 w = 0;
        for (u32 r = 0; r < pending->waiters.len; ++r) {
            jazz_db_ChunkDemandWaiter *waiter = &pending->waiters.ptr[r];
            bool remove = waiter->tag == JAZZ_CDW_RELAY && waiter->connection == connection;
            removed_relay_waiters += remove ? 1 : 0;
            if (!remove) pending->waiters.ptr[w++] = *waiter;
        }
        pending->waiters.len = w;
        bool emptied = pending->waiters.len == 0;
        u64 upstream_id = pending->upstream_id;
        if (state->relay_chunk_obligations < removed_relay_waiters) jz_panic("disconnected relay waiters are accounted");
        state->relay_chunk_obligations -= removed_relay_waiters;
        if (emptied) {
            jz_btree_remove(&state->pending_by_chunk, request, NULL, NULL);
            jz_btree_remove(&state->chunk_by_upstream_id, &upstream_id, NULL, NULL);
            deque_retain_requests(&state->outbound, upstream_id);
        }
    }
    jz_vec_drop(&requests, sizeof(groove_ChunkRequest), groove__chunks__ChunkRequest__drop);
    if (has_waker) jz_waker_wake(&disconnected_waker);
}

/* ---- ChunkResolutionFuture ---- */
typedef struct jazz_db_ChunkResolutionFuture {
    jazz_db_PeerChunkResolver resolver;
    groove_ChunkRequest request;
    u64 waiter_id;
    jz_chan *receiver;
    bool completed;
} jazz_db_ChunkResolutionFuture;

static bool jazz_db__db__ChunkResolutionFuture__Future__poll(void *p, const jz_waker *cx, groove_ChunkResult *out) {
    (void)cx;
    jazz_db_ChunkResolutionFuture *self = p;
    if (jz_chan_try_recv(self->receiver, out)) {
        self->completed = true;
        return true;
    }
    if (jz_chan_is_closed(self->receiver)) {
        /* sender dropped: unwrap_or(Err(Unavailable)) */
        self->completed = true;
        memset(out, 0, sizeof *out);
        out->err.tag = GROOVE_CHUNK_ERR_UNAVAILABLE;
        return true;
    }
    return false;
}

static void jazz_db__db__ChunkResolutionFuture__Drop__drop(void *p) {
    jazz_db_ChunkResolutionFuture *self = p;
    if (!self->completed) jazz_db__db__PeerChunkResolver__cancel_local(&self->resolver, &self->request, self->waiter_id);
    jz_chan_release(self->receiver);
    groove__chunks__ChunkRequest__drop(&self->request);
    jazz_db__db__PeerChunkResolver__drop(&self->resolver);
    jz_free(self);
}

static const groove_ChunkFutureVt chunk_resolution_future_vt = {
    jazz_db__db__ChunkResolutionFuture__Future__poll,
    jazz_db__db__ChunkResolutionFuture__Drop__drop,
};

groove_ChunkFuture jazz_db__db__PeerChunkResolver__MissingChunkResolver__resolve(const void *p, groove_ChunkRequest request) {
    const jazz_db_PeerChunkResolver *self = p;
    jz_chan *channel = jz_chan_new(sizeof(groove_ChunkResult));
    jazz_db_ChunkDemandState *state = self->state;
    u64 next = state->next_waiter_id + 1;
    state->next_waiter_id = next > 1 ? next : 1;
    u64 waiter_id = state->next_waiter_id;
    jazz_db_ChunkDemandWaiter waiter;
    memset(&waiter, 0, sizeof waiter);
    waiter.tag = JAZZ_CDW_LOCAL;
    waiter.waiter_id = waiter_id;
    waiter.sender = channel; /* oneshot pair: sender closes, receiver releases */
    groove_ChunkRequest request_clone;
    groove__chunks__ChunkRequest__clone(&request_clone, &request);
    jazz_db_ChunkDemandWaiter back;
    if (!jazz_db__db__PeerChunkResolver__enqueue(self, request_clone, JAZZ_DB_DEFAULT_CHUNK_FORWARD_HOPS, waiter, &back)) {
        if (back.tag == JAZZ_CDW_LOCAL) {
            groove_ChunkResult result;
            memset(&result, 0, sizeof result);
            result.err.tag = GROOVE_CHUNK_ERR_BACKEND;
            result.err.backend = jz_str_from("chunk request backpressure");
            jz_chan_send(back.sender, &result);
            jz_chan_close(back.sender);
        }
    }
    jazz_db_ChunkResolutionFuture *future = JZ_NEW(jazz_db_ChunkResolutionFuture);
    future->resolver = jazz_db__db__PeerChunkResolver__clone(self);
    future->request = request;
    future->waiter_id = waiter_id;
    future->receiver = channel;
    future->completed = false;
    groove_ChunkFuture f = { future, &chunk_resolution_future_vt };
    return f;
}

static void peer_chunk_resolver_box_drop(void *p) {
    jazz_db__db__PeerChunkResolver__drop(p);
    jz_free(p);
}
const groove_MissingChunkResolverVt jazz_db_peer_chunk_resolver_vt = {
    jazz_db__db__PeerChunkResolver__MissingChunkResolver__resolve,
    peer_chunk_resolver_box_drop,
};

/* ---- PeerIoPump ---- */

static void rc_message_cell_drop(void *p) {
    jazz_db_RcMessageCell *c = p;
    if (c->has) jazz_protocol__protocol__SyncMessage__drop(&c->m);
}

void PIO_FN(new)(jazz_db_PeerChunkResolver resolver, groove_LocalChunkReader local_chunks, u64 connection, u32 role,
                 jazz_protocol_WireInboundContext *wire_inbound_context, jazz_db_PeerIoPump *out) {
    jazz_db__db__PeerChunkResolver__register_connection(&resolver, connection, role == JAZZ_PIO_UPSTREAM);
    jazz_db_SharedAuxiliaryEndpoint *auxiliary_endpoint = NULL;
    if (wire_inbound_context) {
        jazz_db_AuxiliaryChannelEndpoint endpoint;
        jz_err e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__new(
            jazz_protocol__wire__WireInboundContext__clone(wire_inbound_context), &endpoint);
        if (JZ_IS_ERR(e)) jz_panic("paired transport has a valid auxiliary channel context");
        jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_pump_owned(&endpoint);
        auxiliary_endpoint = jazz_db__db__channel_endpoint__SharedAuxiliaryEndpoint__new(endpoint);
    }
    out->resolver = resolver;
    out->local_chunks = local_chunks;
    out->connection = connection;
    out->role = role;
    out->wire_inbound_context = wire_inbound_context;
    out->auxiliary_endpoint = auxiliary_endpoint;
    out->wire_reservation_active = jz_rc_new(sizeof(jazz_db_RcBoolCell), NULL);
    out->wire_reservation_active->v = false;
    out->wire_outbound_obligation = jz_rc_new(sizeof(jazz_db_RcMessageCell), rc_message_cell_drop);
    out->wire_outbound_obligation->has = false;
    out->canonical_credit_progress = jz_rc_new(sizeof(jazz_db_RcBoolCell), NULL);
    out->canonical_credit_progress->v = false;
}

jazz_db_PeerIoPump PIO_FN(clone)(const jazz_db_PeerIoPump *p) {
    jazz_db_PeerIoPump c = *p;
    c.resolver = jazz_db__db__PeerChunkResolver__clone(&p->resolver);
    c.local_chunks = groove__chunks__LocalChunkReader__clone(&p->local_chunks);
    if (p->wire_inbound_context) c.wire_inbound_context = jz_rc_clone(p->wire_inbound_context);
    if (p->auxiliary_endpoint) c.auxiliary_endpoint = jz_rc_clone(p->auxiliary_endpoint);
    c.wire_reservation_active = jz_rc_clone(p->wire_reservation_active);
    c.wire_outbound_obligation = jz_rc_clone(p->wire_outbound_obligation);
    c.canonical_credit_progress = jz_rc_clone(p->canonical_credit_progress);
    return c;
}

void PIO_FN(drop)(jazz_db_PeerIoPump *p) {
    jazz_db__db__PeerChunkResolver__drop(&p->resolver);
    groove__chunks__LocalChunkReader__drop(&p->local_chunks);
    if (p->wire_inbound_context) jz_rc_release(p->wire_inbound_context);
    if (p->auxiliary_endpoint) jz_rc_release(p->auxiliary_endpoint);
    jz_rc_release(p->wire_reservation_active);
    jz_rc_release(p->wire_outbound_obligation);
    jz_rc_release(p->canonical_credit_progress);
}

void PIO_FN(with_shared_auxiliary_endpoint)(jazz_db_PeerIoPump *self, jazz_db_SharedAuxiliaryEndpoint *endpoint) {
    if (endpoint) {
        if (self->auxiliary_endpoint) jz_rc_release(self->auxiliary_endpoint);
        self->auxiliary_endpoint = endpoint;
    }
}

void PIO_FN(claim_binding_output)(const jazz_db_PeerIoPump *self) {
    if (self->auxiliary_endpoint) {
        jazz_db_AuxiliaryChannelEndpoint *e = jazz_db_aux_lock(self->auxiliary_endpoint);
        if (!e) jz_panic("auxiliary endpoint lock poisoned");
        jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__set_pump_owned(e);
    }
}

/* Remaining delay for the independent auxiliary receive deadline. */
bool PIO_FN(incomplete_receive_timeout_ms)(const jazz_db_PeerIoPump *self, u64 *out) {
    if (!self->auxiliary_endpoint) return false;
    jazz_db_AuxiliaryChannelEndpoint *e = jazz_db_aux_lock(self->auxiliary_endpoint);
    if (!e) return false;
    return jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__incomplete_receive_timeout_ms(e, out);
}

/* Service an auxiliary receive deadline without entering the semantic node. */
jz_err PIO_FN(expire_incomplete_receive)(const jazz_db_PeerIoPump *self) {
    if (!self->auxiliary_endpoint) return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary endpoint unavailable");
    jazz_db_AuxiliaryChannelEndpoint *e = jazz_db_aux_lock(self->auxiliary_endpoint);
    if (!e) return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary endpoint lock poisoned");
    return jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__expire_incomplete_receive(e);
}

static jz_err PIO_FN(wire_inbound_context)(const jazz_db_PeerIoPump *self, jazz_protocol_WireInboundContext **out) {
    if (!self->wire_inbound_context)
        return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary wire framing requires a paired wire transport adapter");
    *out = self->wire_inbound_context;
    return JZ_OK;
}

/* Route an auxiliary message received by the binding. */
bool PIO_FN(route_incoming)(const jazz_db_PeerIoPump *self, jazz_protocol_SyncMessage message, jazz_protocol_SyncMessage *back) {
    if (self->role == JAZZ_PIO_UPSTREAM && message.tag == JAZZ_SM_CHUNK_RESPONSE_BATCH) {
        /* A disconnected or superseded upstream can still have a late frame. */
        if (!jazz_db__db__PeerChunkResolver__is_active_upstream(&self->resolver, self->connection)) {
            jazz_protocol__protocol__SyncMessage__drop(&message);
            return true;
        }
        for (u32 i = 0; i < message.v.chunk_responses.len; ++i) {
            jazz_protocol_ChunkResponseEntry response = message.v.chunk_responses.ptr[i];
            jazz_db_ChunkDemandState *state = self->resolver.state;
            groove_ChunkRequest *request = jz_btree_get(&state->chunk_by_upstream_id, &response.request_id);
            if (request) {
                jazz_db_PendingChunkDemand *pending = jz_btree_get(&state->pending_by_chunk, request);
                if (pending) {
                    jazz_protocol_ChunkRequestEntry entry;
                    entry.request_id = response.request_id;
                    entry.locator = jz_bytes_clone(&request->locator);
                    memcpy(entry.expected_hash, request->object_hash, 32);
                    entry.remaining_hops = pending->remaining_hops;
                    jazz_db__db__PeerChunkResolver__record_request(&self->resolver, self->connection, self->role,
                                                                   "inbound-response", &entry, &response.result, NULL);
                    jazz_protocol__protocol__ChunkRequestEntry__drop(&entry);
                }
            }
            jazz_db__db__PeerChunkResolver__complete(&self->resolver, response);
        }
        jz_free(message.v.chunk_responses.ptr);
        return true;
    }
    if (self->role == JAZZ_PIO_SUBSCRIBER && message.tag == JAZZ_SM_CHUNK_REQUEST_BATCH) {
        jazz_db_ChunkResponseEntryVec responses = {0};
        u32 i = 0;
        for (; i < message.v.chunk_requests.len; ++i) {
            jazz_protocol_ChunkRequestEntry request = message.v.chunk_requests.ptr[i];
            if (PIO_FN(is_disconnected)(self)) break;
            jazz_db__db__PeerChunkResolver__record_request(&self->resolver, self->connection, self->role, "inbound-request", &request, NULL, NULL);
            jz_bytes bytes;
            groove_ChunkStorageError error;
            bool ok = groove__chunks__LocalChunkReader__get(&self->local_chunks, jz_bytes_clone(&request.locator), request.expected_hash, &bytes, &error);
            if (PIO_FN(is_disconnected)(self)) {
                if (ok) jz_bytes_drop(&bytes); else jz_str_drop(&error.backend);
                break;
            }
            if (ok) {
                jazz_protocol_ChunkResponse found_marker;
                memset(&found_marker, 0, sizeof found_marker);
                found_marker.tag = JAZZ_CHUNK_FOUND;
                jazz_db__db__PeerChunkResolver__record_request(&self->resolver, self->connection, self->role, "local-response", &request, &found_marker, NULL);
                jazz_protocol_ChunkResponseEntry r;
                memset(&r, 0, sizeof r);
                r.request_id = request.request_id;
                r.result.tag = JAZZ_CHUNK_FOUND;
                r.result.found = bytes;
                JZ_VEC_PUSH(responses, r);
                jazz_protocol__protocol__ChunkRequestEntry__drop(&request);
            } else if (error.tag == GROOVE_CSE_UNAVAILABLE) {
                jazz_db__db__PeerChunkResolver__record_request(&self->resolver, self->connection, self->role, "relay-request", &request, NULL, NULL);
                jazz_db__db__PeerChunkResolver__enqueue_relay(&self->resolver, self->connection, request);
            } else {
                jazz_protocol_ChunkResponse unavailable;
                memset(&unavailable, 0, sizeof unavailable);
                unavailable.tag = JAZZ_CHUNK_UNAVAILABLE;
                jazz_db__db__PeerChunkResolver__record_request(&self->resolver, self->connection, self->role, "local-response", &request, &unavailable, &error);
                jazz_protocol_ChunkResponseEntry r;
                memset(&r, 0, sizeof r);
                r.request_id = request.request_id;
                r.result.tag = JAZZ_CHUNK_UNAVAILABLE;
                JZ_VEC_PUSH(responses, r);
                jz_str_drop(&error.backend);
                jazz_protocol__protocol__ChunkRequestEntry__drop(&request);
            }
        }
        for (u32 j = i; j < message.v.chunk_requests.len; ++j) jazz_protocol__protocol__ChunkRequestEntry__drop(&message.v.chunk_requests.ptr[j]);
        jz_free(message.v.chunk_requests.ptr);
        if (responses.len != 0 && !PIO_FN(is_disconnected)(self)) {
            jazz_db__db__PeerChunkResolver__enqueue_relay_responses(&self->resolver, self->connection, responses);
        } else {
            request_vec_drop(&responses);
        }
        return true;
    }
    if (message.tag == JAZZ_SM_CHUNK_REQUEST_BATCH || message.tag == JAZZ_SM_CHUNK_RESPONSE_BATCH) {
        jazz_protocol__protocol__SyncMessage__drop(&message);
        return true;
    }
    *back = message;
    return false;
}

/* Decode and route one payload from the dedicated auxiliary protocol channel. */
jz_err PIO_FN(route_incoming_payload)(const jazz_db_PeerIoPump *self, jz_bytes payload, bool *has, jz_bytes *out) {
    jazz_protocol_SyncMessage message;
    jz_err e = self->role == JAZZ_PIO_UPSTREAM
                   ? jazz_protocol__wire__decode_sync_message_trusted(jz_bytes_slice(&payload), &message)
                   : jazz_protocol__wire__decode_sync_message(jz_bytes_slice(&payload), &message);
    if (JZ_IS_ERR(e)) {
        jazz_protocol_WireError *we = jazz_protocol__wire__WireError__from_err(e);
        jz_err f = jz_err_fmt(JAZZ_DB_STRING_ERROR, "malformed auxiliary chunk payload: %.*s", (int)we->message.len, we->message.ptr);
        jazz_protocol__wire__WireError__drop(we);
        jz_free(we);
        jz_bytes_drop(&payload);
        return f;
    }
    jazz_protocol_SyncMessage back;
    if (PIO_FN(route_incoming)(self, message, &back)) {
        jz_bytes_drop(&payload);
        *has = false;
    } else {
        jazz_protocol__protocol__SyncMessage__drop(&back);
        *has = true;
        *out = payload;
    }
    return JZ_OK;
}

static jz_err PIO_FN(channel_credits)(const jazz_db_PeerIoPump *self, jazz_protocol_SharedChannelCredits **out) {
    if (!self->auxiliary_endpoint) return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary channel endpoint is unavailable");
    jazz_db_AuxiliaryChannelEndpoint *e = jazz_db_aux_lock(self->auxiliary_endpoint);
    if (!e) return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary endpoint lock poisoned");
    *out = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__channel_credits(e);
    return JZ_OK;
}

/* Demultiplex one complete wire frame without taking the Jazz node lock. */
jz_err PIO_FN(route_incoming_wire_frame)(const jazz_db_PeerIoPump *self, jz_bytes frame, bool *has, jz_bytes *out) {
    *has = false;
    if (PIO_FN(is_disconnected)(self)) {
        jz_bytes_drop(&frame);
        return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary connection is disconnected");
    }
    jazz_protocol_WireInboundContext *context;
    jz_err e = PIO_FN(wire_inbound_context)(self, &context);
    if (JZ_IS_ERR(e)) { jz_bytes_drop(&frame); return e; }
    jazz_protocol_WireFrame decoded;
    e = jazz_protocol__wire__WireInboundContext__decode_frame(context, jz_bytes_slice(&frame), &decoded);
    if (JZ_IS_ERR(e)) {
        jazz_protocol_WireError *we = jazz_protocol__wire__WireError__from_err(e);
        jz_err f = jz_err_fmt(JAZZ_DB_STRING_ERROR, "malformed auxiliary wire frame: %.*s", (int)we->message.len, we->message.ptr);
        jazz_protocol__wire__WireError__drop(we);
        jz_free(we);
        jz_bytes_drop(&frame);
        return f;
    }
    if (decoded.tag == JAZZ_WF_CHANNEL_CREDIT) {
        jz_bytes_drop(&frame);
        jazz_protocol_SharedChannelCredits *credits;
        JZ_TRY(PIO_FN(channel_credits)(self, &credits));
        if (credits->poisoned) {
            jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
            return jz_err_new(JAZZ_DB_STRING_ERROR, "channel credit lock poisoned");
        }
        e = jazz_protocol__wire__channel_credit__ChannelCredits__receive_credit(credits->inner, decoded.credit);
        jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
        JZ_TRY(e);
        self->canonical_credit_progress->v = true;
        return JZ_OK;
    }
    if (decoded.tag != JAZZ_WF_CHANNEL) {
        jazz_protocol__wire__WireFrame__drop(&decoded);
        *has = true;
        *out = frame;
        return JZ_OK;
    }
    if (decoded.channel.extent.channel != JAZZ_WIRE_AUXILIARY_CHANNEL) {
        jazz_protocol__wire__WireFrame__drop(&decoded);
        *has = true;
        *out = frame;
        return JZ_OK;
    }
    u32 frame_len = frame.len;
    jz_bytes_drop(&frame);
    if (!self->auxiliary_endpoint) {
        jazz_protocol__wire__WireFrame__drop(&decoded);
        return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary channel endpoint is unavailable");
    }
    jazz_db_AuxiliaryChannelEndpoint *endpoint = jazz_db_aux_lock(self->auxiliary_endpoint);
    if (!endpoint) {
        jazz_protocol__wire__WireFrame__drop(&decoded);
        return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary endpoint lock poisoned");
    }
    bool got;
    jazz_protocol_SyncMessage message;
    JZ_TRY(jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__receive(endpoint, decoded.channel, frame_len, &got, &message));
    if (got) {
        jazz_protocol_SyncMessage back;
        if (!PIO_FN(route_incoming)(self, message, &back)) {
            jazz_protocol__protocol__SyncMessage__drop(&back);
            return jz_err_new(JAZZ_DB_STRING_ERROR, "canonical message on reserved auxiliary channel");
        }
    }
    return JZ_OK;
}

jz_bytes jazz_db__db__ReservedOutboundWireFrame__take_frame(jazz_db_ReservedOutboundWireFrame *r) {
    return jz_bytes_clone(&r->frame);
}

void jazz_db__db__ReservedOutboundWireFrame__drop(jazz_db_ReservedOutboundWireFrame *r) {
    r->pump.wire_reservation_active->v = false;
    PIO_FN(drop)(&r->pump);
    jz_bytes_drop(&r->frame);
}

void jazz_db__db__ReservedOutboundWireFrame__commit(jazz_db_ReservedOutboundWireFrame *r) {
    if (r->credit_grant) {
        jazz_protocol_SharedChannelCredits *credits;
        jz_err e = PIO_FN(channel_credits)(&r->pump, &credits);
        if (JZ_IS_ERR(e)) jz_panic("reserved credit has endpoint");
        if (credits->poisoned) jz_panic("channel credit lock poisoned");
        e = jazz_protocol__wire__channel_credit__ChannelCredits__accept_grant(credits->inner);
        if (JZ_IS_ERR(e)) jz_panic("reserved credit remains pending until handoff");
        jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
        jazz_db__db__ReservedOutboundWireFrame__drop(r);
        return;
    }
    if (!r->pump.auxiliary_endpoint) jz_panic("reserved frame has an auxiliary endpoint");
    jazz_db_AuxiliaryChannelEndpoint *endpoint = jazz_db_aux_lock(r->pump.auxiliary_endpoint);
    if (!endpoint) jz_panic("auxiliary endpoint lock poisoned");
    bool completed;
    jazz_protocol_SyncMessage done;
    jz_err e = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__accept_outbound(endpoint, &completed, &done);
    if (JZ_IS_ERR(e)) jz_panic("reserved frame remains pending until handoff");
    if (completed) {
        jazz_protocol__protocol__SyncMessage__drop(&done);
        jazz_db_RcMessageCell *cell = r->pump.wire_outbound_obligation;
        if (cell->has) {
            cell->has = false;
            jazz_protocol_SyncMessage message = cell->m;
            PIO_FN(acknowledge_outbound)(&r->pump, &message);
            jazz_protocol__protocol__SyncMessage__drop(&message);
        }
    }
    jazz_db__db__ReservedOutboundWireFrame__drop(r);
}

static void PIO_FN(restore_outbound)(const jazz_db_PeerIoPump *self, jazz_protocol_SyncMessage message);

/* Reserve one complete auxiliary wire frame for a binding-owned send. */
jz_err PIO_FN(reserve_outbound_wire_frame)(const jazz_db_PeerIoPump *self, bool *has, jazz_db_ReservedOutboundWireFrame *out) {
    *has = false;
    if (PIO_FN(is_disconnected)(self) || self->wire_reservation_active->v) return JZ_OK;
    jazz_protocol_SharedChannelCredits *credits;
    JZ_TRY(PIO_FN(channel_credits)(self, &credits));
    if (credits->poisoned) {
        jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
        return jz_err_new(JAZZ_DB_STRING_ERROR, "channel credit lock poisoned");
    }
    bool has_grant;
    jz_bytes grant;
    jz_err e = jazz_protocol__wire__channel_credit__ChannelCredits__peek_grant(credits->inner, &has_grant, &grant);
    jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
    JZ_TRY(e);
    if (has_grant) {
        self->wire_reservation_active->v = true;
        out->pump = PIO_FN(clone)(self);
        out->frame = grant;
        out->credit_grant = true;
        *has = true;
        return JZ_OK;
    }
    if (!self->auxiliary_endpoint)
        return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary wire framing requires a paired wire transport adapter");
    jazz_db_AuxiliaryChannelEndpoint *endpoint = jazz_db_aux_lock(self->auxiliary_endpoint);
    if (!endpoint) return jz_err_new(JAZZ_DB_STRING_ERROR, "auxiliary endpoint lock poisoned");
    if (!jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__has_pending_outbound(endpoint)) {
        jazz_protocol_SyncMessage message;
        if (!PIO_FN(take_outbound)(self, 1, &message)) return JZ_OK;
        jazz_protocol_SyncMessage copy;
        jazz_protocol__protocol__SyncMessage__clone(&copy, &message);
        jz_err error = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__enqueue(endpoint, copy);
        if (JZ_IS_ERR(error)) {
            PIO_FN(restore_outbound)(self, message);
            jz_str dbg = {0};
            jz_str_push_cstr(&dbg, error.code == JAZZ_TE_BACKPRESSURE ? "Backpressure" : error.code == JAZZ_TE_CLOSED ? "Closed" : "Failed");
            jz_err f = jz_err_fmt(JAZZ_DB_STRING_ERROR, "cannot enqueue auxiliary channel message: %.*s", (int)dbg.len, dbg.ptr);
            jz_str_drop(&dbg);
            jz_err_drop(&error);
            return f;
        }
        jazz_db_RcMessageCell *cell = self->wire_outbound_obligation;
        if (cell->has) jazz_protocol__protocol__SyncMessage__drop(&cell->m);
        cell->has = true;
        cell->m = message;
    }
    bool has_frame;
    jz_bytes frame;
    JZ_TRY(jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__peek_outbound(endpoint, &has_frame, &frame));
    if (!has_frame) return JZ_OK;
    self->wire_reservation_active->v = true;
    out->pump = PIO_FN(clone)(self);
    out->frame = frame;
    out->credit_grant = false;
    *has = true;
    return JZ_OK;
}

/* Encode one bounded auxiliary batch as an ordinary complete wire frame. */
jz_err PIO_FN(take_outbound_wire_frame)(const jazz_db_PeerIoPump *self, bool *has, jz_bytes *out) {
    jazz_db_ReservedOutboundWireFrame reservation;
    JZ_TRY(PIO_FN(reserve_outbound_wire_frame)(self, has, &reservation));
    if (!*has) return JZ_OK;
    *out = jazz_db__db__ReservedOutboundWireFrame__take_frame(&reservation);
    jazz_db__db__ReservedOutboundWireFrame__commit(&reservation);
    return JZ_OK;
}

/* Drain a bounded prefix while retaining exact encoded bytes for any frame that does not fit. */
jz_err PIO_FN(take_outbound_wire_frames)(const jazz_db_PeerIoPump *self, usize max_frames, usize max_bytes, jazz_db_BytesVec *out) {
    memset(out, 0, sizeof *out);
    usize total_bytes = 0;
    while (out->len < max_frames && total_bytes < max_bytes) {
        bool has;
        jazz_db_ReservedOutboundWireFrame reservation;
        jz_err e = PIO_FN(reserve_outbound_wire_frame)(self, &has, &reservation);
        if (JZ_IS_ERR(e)) { jz_vec_drop(out, sizeof(jz_bytes), (void (*)(void *))jz_bytes_drop); return e; }
        if (!has) break;
        if (reservation.frame.len > max_bytes - total_bytes) {
            if (out->len == 0) {
                jz_err f = jz_err_fmt(JAZZ_DB_STRING_ERROR, "auxiliary wire frame exceeds bounded drain budget: frame=%u budget=%zu",
                                      reservation.frame.len, (size_t)max_bytes);
                jazz_db__db__ReservedOutboundWireFrame__drop(&reservation);
                return f;
            }
            jazz_db__db__ReservedOutboundWireFrame__drop(&reservation);
            break;
        }
        jz_bytes frame = jazz_db__db__ReservedOutboundWireFrame__take_frame(&reservation);
        total_bytes += frame.len;
        jazz_db__db__ReservedOutboundWireFrame__commit(&reservation);
        JZ_VEC_PUSH(*out, frame);
    }
    return JZ_OK;
}

/* Drain one bounded auxiliary batch for immediate transmission. */
bool PIO_FN(take_outbound)(const jazz_db_PeerIoPump *self, usize limit, jazz_protocol_SyncMessage *out) {
    memset(out, 0, sizeof *out);
    if (self->role == JAZZ_PIO_UPSTREAM) {
        jazz_db_ChunkRequestEntryVec requests = jazz_db__db__PeerChunkResolver__take_outbound(&self->resolver, limit);
        JZ_VEC_FOR(jazz_protocol_ChunkRequestEntry, request, requests)
            jazz_db__db__PeerChunkResolver__record_request(&self->resolver, self->connection, self->role, "outbound-request", request, NULL, NULL);
        if (requests.len == 0) { jz_free(requests.ptr); return false; }
        out->tag = JAZZ_SM_CHUNK_REQUEST_BATCH;
        out->v.chunk_requests.ptr = requests.ptr; out->v.chunk_requests.len = requests.len; out->v.chunk_requests.cap = requests.cap;
        return true;
    }
    jazz_db_ChunkResponseEntryVec responses = jazz_db__db__PeerChunkResolver__take_relay_responses(&self->resolver, self->connection, limit);
    if (responses.len == 0) { jz_free(responses.ptr); return false; }
    out->tag = JAZZ_SM_CHUNK_RESPONSE_BATCH;
    out->v.chunk_responses.ptr = responses.ptr; out->v.chunk_responses.len = responses.len; out->v.chunk_responses.cap = responses.cap;
    return true;
}

/* Hand one bounded auxiliary batch to an in-process transport. */
jz_err PIO_FN(send_outbound)(const jazz_db_PeerIoPump *self, jazz_db_Transport *transport, usize limit, bool *sent) {
    jazz_protocol_SyncMessage message;
    if (!PIO_FN(take_outbound)(self, limit, &message)) { *sent = false; return JZ_OK; }
    jazz_protocol_SyncMessage copy;
    jazz_protocol__protocol__SyncMessage__clone(&copy, &message);
    jz_err error = transport->vt->send(transport->self, copy);
    if (JZ_IS_ERR(error)) {
        PIO_FN(restore_outbound)(self, message);
        return error;
    }
    PIO_FN(acknowledge_outbound)(self, &message);
    jazz_protocol__protocol__SyncMessage__drop(&message);
    *sent = true;
    return JZ_OK;
}

static void PIO_FN(restore_outbound)(const jazz_db_PeerIoPump *self, jazz_protocol_SyncMessage message) {
    if (self->role == JAZZ_PIO_UPSTREAM && message.tag == JAZZ_SM_CHUNK_REQUEST_BATCH) {
        jazz_db_ChunkDemandState *state = self->resolver.state;
        for (u32 i = message.v.chunk_requests.len; i-- > 0;) jz_deque_push_front(&state->outbound, &message.v.chunk_requests.ptr[i]);
        jz_free(message.v.chunk_requests.ptr);
        return;
    }
    if (self->role == JAZZ_PIO_SUBSCRIBER && message.tag == JAZZ_SM_CHUNK_RESPONSE_BATCH) {
        jazz_db_ChunkResponseEntryVec responses;
        responses.ptr = message.v.chunk_responses.ptr; responses.len = message.v.chunk_responses.len; responses.cap = message.v.chunk_responses.cap;
        jazz_db__db__PeerChunkResolver__restore_relay_responses(&self->resolver, self->connection, responses);
        return;
    }
    jazz_protocol__protocol__SyncMessage__drop(&message);
}

/* Confirm that an auxiliary batch has been handed to its binding. */
void PIO_FN(acknowledge_outbound)(const jazz_db_PeerIoPump *self, const jazz_protocol_SyncMessage *message) {
    if (self->role == JAZZ_PIO_SUBSCRIBER && message->tag == JAZZ_SM_CHUNK_RESPONSE_BATCH) {
        jazz_db__db__PeerChunkResolver__acknowledge_relay_response_send(&self->resolver, self->connection,
                                                                       message->v.chunk_responses.ptr, message->v.chunk_responses.len);
    }
}

/* Encode one bounded auxiliary payload for a binding-owned socket channel. */
jz_err PIO_FN(take_outbound_payload)(const jazz_db_PeerIoPump *self, usize limit, bool *has, jz_bytes *out) {
    jazz_protocol_SyncMessage message;
    *has = false;
    if (!PIO_FN(take_outbound)(self, limit, &message)) return JZ_OK;
    jz_bytes payload = {0};
    jz_err e = jazz_protocol__wire__encode_sync_message(&message, &payload);
    if (!JZ_IS_ERR(e)) {
        PIO_FN(acknowledge_outbound)(self, &message);
        jazz_protocol__protocol__SyncMessage__drop(&message);
        *has = true;
        *out = payload;
        return JZ_OK;
    }
    PIO_FN(restore_outbound)(self, message);
    jazz_protocol_WireError *we = jazz_protocol__wire__WireError__from_err(e);
    jz_err f = jz_err_fmt(JAZZ_DB_STRING_ERROR, "cannot encode auxiliary chunk payload: %.*s", (int)we->message.len, we->message.ptr);
    jazz_protocol__wire__WireError__drop(we);
    jz_free(we);
    return f;
}

jazz_db_PeerIoOutboundReady PIO_FN(outbound_ready)(const jazz_db_PeerIoPump *self) {
    jazz_db_PeerIoOutboundReady r = { PIO_FN(clone)(self) };
    return r;
}

static bool PIO_FN(has_outbound)(const jazz_db_PeerIoPump *self);

bool PIO_FN(outbound_is_ready)(const jazz_db_PeerIoPump *self) { return PIO_FN(has_outbound)(self); }

/* Detach this link's hop-local routing state. */
void PIO_FN(disconnect)(const jazz_db_PeerIoPump *self) {
    if (self->auxiliary_endpoint) {
        jazz_db_AuxiliaryChannelEndpoint *endpoint = jazz_db_aux_lock(self->auxiliary_endpoint);
        if (endpoint) {
            jazz_protocol_SharedChannelCredits *credits = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__channel_credits(endpoint);
            if (!credits->poisoned) jazz_protocol__wire__channel_credit__ChannelCredits__close(credits->inner);
            jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
        }
    }
    jazz_db__db__PeerChunkResolver__disconnect(&self->resolver, self->connection, self->role == JAZZ_PIO_UPSTREAM);
}

static bool PIO_FN(has_outbound)(const jazz_db_PeerIoPump *self) {
    if (PIO_FN(is_disconnected)(self)) return false;
    if (self->auxiliary_endpoint) {
        jazz_db_AuxiliaryChannelEndpoint *endpoint = jazz_db_aux_lock(self->auxiliary_endpoint);
        if (!endpoint) jz_panic("auxiliary endpoint lock poisoned");
        jazz_protocol_SharedChannelCredits *credits = jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__channel_credits(endpoint);
        if (credits->poisoned) jz_panic("channel credit lock poisoned");
        bool pending_grant = jazz_protocol__wire__channel_credit__ChannelCredits__has_pending_grant(credits->inner);
        bool can_send = jazz_protocol__wire__channel_credit__ChannelCredits__can_send(credits->inner, JAZZ_CC_AUXILIARY);
        jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
        if (pending_grant) return true;
        if (!can_send) return false;
        if (jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__outbound_is_ready(endpoint)) return true;
    }
    jazz_db_ChunkDemandState *state = self->resolver.state;
    if (self->role == JAZZ_PIO_UPSTREAM) return state->outbound.len != 0;
    jazz_db_ChunkResponseEntryVec *responses = jz_btree_get(&state->relay_responses, &self->connection);
    return responses && responses->len != 0;
}

bool PIO_FN(is_disconnected)(const jazz_db_PeerIoPump *self) {
    return jz_btree_contains(&self->resolver.state->disconnected_connections, &self->connection);
}

jazz_db_PeerIoTraceVec PIO_FN(take_trace)(const jazz_db_PeerIoPump *self) {
    return jazz_db__db__PeerChunkResolver__take_trace(&self->resolver, self->connection);
}

void PIO_FN(set_trace_enabled)(const jazz_db_PeerIoPump *self, bool enabled) {
    jazz_db__db__PeerChunkResolver__set_trace_enabled(&self->resolver, self->connection, enabled);
}

/* Future completed when a PeerIoPump has outbound auxiliary traffic. */
bool jazz_db__db__PeerIoOutboundReady__poll(jazz_db_PeerIoOutboundReady *f, const jz_waker *cx) {
    jazz_protocol_SharedChannelCredits *credits;
    jz_err e = PIO_FN(channel_credits)(&f->pump, &credits);
    if (!JZ_IS_ERR(e)) {
        if (credits->poisoned) jz_panic("channel credit lock poisoned");
        bool ready = jazz_protocol__wire__channel_credit__ChannelCredits__poll_grant_ready(credits->inner, cx);
        jazz_protocol__wire__channel_credit__SharedChannelCredits__release(credits);
        if (ready) return true;
    } else {
        jz_err_drop(&e);
    }
    if (f->pump.auxiliary_endpoint) {
        jazz_db_AuxiliaryChannelEndpoint *endpoint = jazz_db_aux_lock(f->pump.auxiliary_endpoint);
        if (!endpoint) jz_panic("auxiliary endpoint lock poisoned");
        if (jazz_db__db__channel_endpoint__AuxiliaryChannelEndpoint__poll_outbound_ready(endpoint, cx)) return true;
    }
    if (PIO_FN(has_outbound)(&f->pump) || PIO_FN(is_disconnected)(&f->pump)) return true;
    jz_btree_insert(&f->pump.resolver.state->outbound_wakers, &f->pump.connection, cx, NULL);
    return false;
}

/* ======================================================================
 * Facade state (db.rs:1448-3460)
 * ====================================================================== */

static int txid_cmp(const void *a, const void *b, void *ctx) { return jazz_model__tx__TxId__cmp(a, b, ctx); }
static u64 txid_hash(const void *a) { return jazz_model__tx__TxId__hash(a); }
static bool txid_eq(const void *a, const void *b) { return jazz_model__tx__TxId__eq(a, b); }
const jz_hmap_desc jazz_db_txid_set_desc = { sizeof(jazz_model_TxId), 0, txid_hash, txid_eq, NULL, NULL };
static const jz_btree_desc txid_set_desc = { sizeof(jazz_model_TxId), 0, txid_cmp, NULL, NULL };
static void sync_message_drop(void *p) { jazz_protocol__protocol__SyncMessage__drop(p); }
static const jz_btree_desc txid_message_desc = { sizeof(jazz_model_TxId), sizeof(jazz_protocol_SyncMessage), txid_cmp, NULL, sync_message_drop };

/* ---- SharedNodeState / LocalMutexBorrow ---- */
jazz_node_NodeState *jazz_db__db__SharedNodeState__lock(jazz_db_SharedNodeState *s) {
    s->locked = true;
    return s->state;
}
void jazz_db__db__SharedNodeState__unlock(jazz_db_SharedNodeState *s) { s->locked = false; }

jazz_node_NodeState *jazz_db__db__LocalMutexBorrow__borrow(jazz_db_SharedNodeState *s, const char *caller) {
    if (s->locked) {
        jz_str msg = jz_format("synchronous node operation at %s reentered a suspended operation", caller);
        jz_str_push_char(&msg, '\0');
        jz_panic(msg.ptr);
    }
    return s->state;
}

/* ---- PendingOpenSchema ---- */
static void shared_result_cell_drop(void *p) {
    jazz_db_SharedResultCell *c = p;
    if (c->ready && c->result.code) jz_err_drop(&c->result);
}

void jazz_db__db__PendingOpenSchema__new(jazz_db_PendingOpenSchema *out, jazz_types_SchemaVersionId schema) {
    jazz_db_SharedResultCell *cell = jz_rc_new(sizeof *cell, shared_result_cell_drop);
    cell->ready = false;
    cell->result = JZ_OK;
    cell->sender_dropped = false;
    out->has_connection_epoch = false;
    out->connection_epoch = 0;
    out->authoritative_catalogue_received = false;
    out->schema = schema;
    out->has_result = false;
    out->result = JZ_OK;
    out->sender = jz_rc_clone(cell);
    out->wait = cell;
}

void jazz_db__db__PendingOpenSchema__drop(jazz_db_PendingOpenSchema *p) {
    if (p->has_result && p->result.code) jz_err_drop(&p->result);
    if (p->sender) { p->sender->sender_dropped = true; jz_rc_release(p->sender); }
    jz_rc_release(p->wait);
}

static jz_err err_clone(const jz_err *e) {
    if (!e->code) return JZ_OK;
    if (e->msg) return jz_err_fmt(e->code, "%.*s", (int)e->msg->len, e->msg->ptr);
    return jz_err_new(e->code, NULL);
}

bool jazz_db__db__PendingOpenSchema__poll_wait(jazz_db_SharedResultCell *wait, jz_err *out) {
    if (wait->ready) { *out = err_clone(&wait->result); return true; }
    if (wait->sender_dropped) {
        *out = jz_err_new(JAZZ_EC_PROTOCOL, "database closed before schema admission");
        return true;
    }
    return false;
}

void jazz_db__db__finish_open_schema_admission(jazz_db_OpenSchemaAdmission state, jz_err result) {
    if (!state->has) { jz_err_drop(&result); return; }
    jazz_db_PendingOpenSchema *pending = &state->pending;
    if (pending->has_result && pending->result.code == 0) { jz_err_drop(&result); return; }
    if (pending->has_result && pending->result.code) jz_err_drop(&pending->result);
    pending->has_result = true;
    pending->result = err_clone(&result);
    jazz_db_SharedResultCell *sender = pending->sender;
    pending->sender = NULL;
    if (sender) {
        sender->ready = true;
        sender->result = result;
        jz_rc_release(sender);
    } else {
        jz_err_drop(&result);
    }
}

void jazz_db__db__finish_open_schema_connection(jazz_db_OpenSchemaAdmission state, u64 connection_epoch, jz_err result) {
    bool owns_attempt = state->has && state->pending.has_connection_epoch && state->pending.connection_epoch == connection_epoch;
    if (owns_attempt) jazz_db__db__finish_open_schema_admission(state, result);
    else jz_err_drop(&result);
}

void jazz_db__db__begin_open_schema_connection(jazz_db_OpenSchemaAdmission state, u64 connection_epoch) {
    bool replace = state->has && state->pending.has_connection_epoch &&
                   !(state->pending.has_result && state->pending.result.code == 0);
    if (replace) {
        jazz_db__db__finish_open_schema_admission(state, jz_err_new(JAZZ_EC_PROTOCOL, "upstream replaced before schema admission"));
    }
    if (state->has) {
        jazz_db_PendingOpenSchema *pending = &state->pending;
        if (pending->has_result && pending->result.code == 0) return;
        if (replace) {
            jazz_types_SchemaVersionId schema = pending->schema;
            jazz_db__db__PendingOpenSchema__drop(pending);
            jazz_db__db__PendingOpenSchema__new(pending, schema);
        }
        pending->has_connection_epoch = true;
        pending->connection_epoch = connection_epoch;
    }
}

jz_err jazz_db__db__pending_open_schema_error(void) {
    return jz_err_new(JAZZ_EC_SCHEMA, "opened schema is awaiting published catalogue admission; connect to the authority");
}

/* ---- MutationErrorEvent ---- */
static void transaction_fate_clone(jazz_db_TransactionFate *d, const jazz_db_TransactionFate *s) {
    d->transaction_id = s->transaction_id;
    d->code = jz_str_clone(&s->code);
    d->reason = jz_str_clone(&s->reason);
}

void jazz_db__db__MutationErrorEvent__clone(void *dst, const void *src) {
    jazz_db_MutationErrorEvent *d = dst;
    const jazz_db_MutationErrorEvent *s = src;
    d->code = jz_str_clone(&s->code);
    d->reason = jz_str_clone(&s->reason);
    d->transaction.transaction_id = s->transaction.transaction_id;
    d->transaction.kind = s->transaction.kind;
    d->transaction.sealed = s->transaction.sealed;
    transaction_fate_clone(&d->transaction.latest_settlement, &s->transaction.latest_settlement);
}

void jazz_db__db__MutationErrorEvent__drop(void *p) {
    jazz_db_MutationErrorEvent *e = p;
    jz_str_drop(&e->code);
    jz_str_drop(&e->reason);
    jz_str_drop(&e->transaction.latest_settlement.code);
    jz_str_drop(&e->transaction.latest_settlement.reason);
}

bool jazz_db__db__MutationErrorEvent__eq(const jazz_db_MutationErrorEvent *a, const jazz_db_MutationErrorEvent *b) {
    return jz_str_eq(&a->code, &b->code) && jz_str_eq(&a->reason, &b->reason) &&
           memcmp(&a->transaction.transaction_id, &b->transaction.transaction_id, sizeof a->transaction.transaction_id) == 0 &&
           a->transaction.kind == b->transaction.kind && a->transaction.sealed == b->transaction.sealed &&
           memcmp(&a->transaction.latest_settlement.transaction_id, &b->transaction.latest_settlement.transaction_id,
                  sizeof a->transaction.latest_settlement.transaction_id) == 0 &&
           jz_str_eq(&a->transaction.latest_settlement.code, &b->transaction.latest_settlement.code) &&
           jz_str_eq(&a->transaction.latest_settlement.reason, &b->transaction.latest_settlement.reason);
}

static void json_transaction_id(jz_json_w *w, const jazz_model_TransactionId *id) {
    jz_str s = {0};
    jazz_model__model__public_schema__TransactionId__to_string(id, &s);
    jz_jw_str(w, (&s)->ptr, (&s)->len);
    jz_str_drop(&s);
}

/* #[serde(rename_all = "camelCase")] */
void jazz_db__db__MutationErrorEvent__Serialize__serialize(const jazz_db_MutationErrorEvent *e, jz_json_w *w) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "code"); jz_jw_str(w, (&e->code)->ptr, (&e->code)->len);
    jz_jw_key(w, "reason"); jz_jw_str(w, (&e->reason)->ptr, (&e->reason)->len);
    jz_jw_key(w, "transaction");
    jz_jw_begin_obj(w);
    jz_jw_key(w, "transactionId"); json_transaction_id(w, &e->transaction.transaction_id);
    jz_jw_key(w, "kind"); { const char *_k = e->transaction.kind == JAZZ_TXK_MERGEABLE ? "mergeable" : "exclusive"; jz_jw_str(w, _k, (u32)strlen(_k)); }
    jz_jw_key(w, "sealed"); jz_jw_bool(w, e->transaction.sealed);
    jz_jw_key(w, "latestSettlement");
    jz_jw_begin_obj(w);
    jz_jw_key(w, "kind"); { const char *_k = "rejected"; jz_jw_str(w, _k, (u32)strlen(_k)); }
    jz_jw_key(w, "transactionId"); json_transaction_id(w, &e->transaction.latest_settlement.transaction_id);
    jz_jw_key(w, "code"); jz_jw_str(w, (&e->transaction.latest_settlement.code)->ptr, (&e->transaction.latest_settlement.code)->len);
    jz_jw_key(w, "reason"); jz_jw_str(w, (&e->transaction.latest_settlement.reason)->ptr, (&e->transaction.latest_settlement.reason)->len);
    jz_jw_end_obj(w);
    jz_jw_end_obj(w);
    jz_jw_end_obj(w);
}

u32 jazz_db__db__TransactionKind__from_tx_kind(u32 kind) {
    return kind == JAZZ_TXKIND_MERGEABLE ? JAZZ_TXK_MERGEABLE : JAZZ_TXK_EXCLUSIVE;
}

jazz_db_MutationErrorCallback jazz_db__db__MutationErrorCallback__clone(const jazz_db_MutationErrorCallback *c) {
    jazz_db_MutationErrorCallback out = { jz_rc_clone(c->self), c->call };
    return out;
}
void jazz_db__db__MutationErrorCallback__drop(jazz_db_MutationErrorCallback *c) { jz_rc_release(c->self); }

/* ---- sync_autopsy (feature disabled) ---- */
void jazz_db__db__sync_autopsy__enable(void) {}
void jazz_db__db__sync_autopsy__clear(void) {}
jz_str jazz_db__db__sync_autopsy__dump(void) { jz_str s = {0}; return s; }

/* ---- SchemaViewId ---- */
const u8 *jazz_db__db__SchemaViewId__as_bytes(const jazz_db_SchemaViewId *id) { return id->b; }

jazz_db_SchemaViewId jazz_db__db__SchemaViewId__for_schema(const jazz_model_JazzSchema *schema) {
    jz_bytes bytes = {0};
    jz_err e = jazz_model__schema__JazzSchema__public_schema_json(schema, &bytes);
    if (JZ_IS_ERR(e)) jz_panic("public schema always serializes");
    jazz_db_SchemaViewId id;
    jazz_third_party__blake3__derive_key("jazz typed schema view id v1", bytes.ptr, bytes.len, id.b);
    jz_bytes_drop(&bytes);
    return id;
}

/* ---- WriteClock / UploadRetryClock ---- */
void jazz_db__db__WriteClock__default(jazz_db_WriteClock *c) { c->next_counter_ms = 1; c->wall = false; }

u64 jazz_db__db__WriteClock__next_now_ms(jazz_db_WriteClock *c) {
    if (c->wall) {
        u64 now = jz_now_ms();
        return now < JAZZ_TYPES_HLC_MAX_PHYSICAL_MS ? now : JAZZ_TYPES_HLC_MAX_PHYSICAL_MS;
    }
    u64 next = c->next_counter_ms;
    c->next_counter_ms = next + 1;
    return next;
}

typedef struct jazz_db_MonotonicUploadRetryClock { u64 started; } jazz_db_MonotonicUploadRetryClock;
static u64 jazz_db__db__MonotonicUploadRetryClock__UploadRetryClock__now_ms(const void *p) {
    const jazz_db_MonotonicUploadRetryClock *self = p;
    u64 now = jz_now_ms();
    return now >= self->started ? now - self->started : 0;
}
static const jazz_db_UploadRetryClockVt monotonic_upload_retry_clock_vt = {
    jazz_db__db__MonotonicUploadRetryClock__UploadRetryClock__now_ms, NULL,
};
jazz_db_UploadRetryClock jazz_db__db__MonotonicUploadRetryClock__new(void) {
    jazz_db_MonotonicUploadRetryClock *c = jz_rc_new(sizeof *c, NULL);
    c->started = jz_now_ms();
    jazz_db_UploadRetryClock out = { c, &monotonic_upload_retry_clock_vt };
    return out;
}

int jazz_db__db__UpstreamUploadDestination__cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jazz_db_UpstreamUploadDestination *x = a, *y = b;
    int c = memcmp(x->remote_node, y->remote_node, 16);
    if (c) return c;
    return jazz_types__ids__AuthorSubject__cmp(&x->link_identity, &y->link_identity);
}

/* ---- local fate routes ---- */
static void pending_fates_drop(void *p) {
    jazz_db_PendingDownstreamFatesCell *c = p;
    jz_vec_drop(&c->v, sizeof(jazz_protocol_SyncMessage), sync_message_drop);
}
jazz_db_PendingDownstreamFates jazz_db__db__PendingDownstreamFates__new(void) {
    jazz_db_PendingDownstreamFatesCell *c = jz_rc_new(sizeof *c, pending_fates_drop);
    memset(&c->v, 0, sizeof c->v);
    return c;
}

static void local_fate_route_drop(void *p) {
    jazz_db_LocalFateRoute *r = p;
    if (r->has_replay_unit) jazz_protocol__protocol__SyncMessage__drop(&r->replay_unit);
    if (r->has_held_fate) jazz_protocol__protocol__SyncMessage__drop(&r->held_fate);
}
static void local_fate_route_vec_drop(void *p) { jz_vec_drop(p, sizeof(jazz_db_LocalFateRoute), local_fate_route_drop); }
static const jz_btree_desc local_fate_routes_desc = { sizeof(jazz_model_TxId), sizeof(jazz_db_LocalFateRouteVec), txid_cmp, NULL, local_fate_route_vec_drop };
static void local_fate_routes_drop(void *p) { jz_btree_drop(&((jazz_db_LocalFateRoutesCell *)p)->map); }
jazz_db_LocalFateRoutes jazz_db__db__LocalFateRoutes__new(void) {
    jazz_db_LocalFateRoutesCell *c = jz_rc_new(sizeof *c, local_fate_routes_drop);
    jz_btree_init(&c->map, &local_fate_routes_desc);
    return c;
}

/* Weak::upgrade */
static bool queue_alive(const jazz_db_PendingDownstreamFatesCell *queue) { return queue && jz_rc_strong(queue) > 0; }

static bool author_opt_eq(bool ha, const jazz_types_AuthorSubject *a, bool hb, const jazz_types_AuthorSubject *b) {
    if (ha != hb) return false;
    return !ha || jazz_types_AuthorSubject_eq(a, b);
}

static bool retain_live_routes(const void *k, void *v, void *ctx) {
    (void)k; (void)ctx;
    jazz_db_LocalFateRouteVec *pending = v;
    u32 w = 0;
    for (u32 r = 0; r < pending->len; ++r) {
        jazz_db_LocalFateRoute *candidate = &pending->ptr[r];
        if (!candidate->replay_ready || queue_alive(candidate->queue)) pending->ptr[w++] = *candidate;
        else local_fate_route_drop(candidate);
    }
    pending->len = w;
    return pending->len != 0;
}

static void jazz_db__db__register_local_fate_route_state(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id, jazz_db_PendingDownstreamFates queue,
                                                        bool local_acknowledged, bool replay_ready, bool has_replay_author,
                                                        jazz_types_AuthorSubject replay_author, const jazz_protocol_SyncMessage *replay_unit) {
    jz_btree_retain(&routes->map, retain_live_routes, NULL);

    jazz_db_LocalFateRouteVec *pending = jz_btree_get(&routes->map, &tx_id);
    if (pending) {
        JZ_VEC_FOR(jazz_db_LocalFateRoute, candidate, *pending) {
            if (!(queue_alive(candidate->queue) && candidate->queue == queue)) continue;
            candidate->local_acknowledged |= local_acknowledged;
            if (replay_ready) {
                candidate->replay_ready = true;
                if (candidate->has_replay_unit) jazz_protocol__protocol__SyncMessage__drop(&candidate->replay_unit);
                candidate->has_replay_unit = false;
            } else {
                if (has_replay_author) { candidate->has_replay_author = true; candidate->replay_author = replay_author; }
                if (replay_unit) {
                    if (candidate->has_replay_unit) jazz_protocol__protocol__SyncMessage__drop(&candidate->replay_unit);
                    jazz_protocol__protocol__SyncMessage__clone(&candidate->replay_unit, replay_unit);
                    candidate->has_replay_unit = true;
                }
            }
            return;
        }
    }

    if (!replay_ready && pending) {
        JZ_VEC_FOR(jazz_db_LocalFateRoute, candidate, *pending) {
            if (!(!candidate->replay_ready && !queue_alive(candidate->queue) &&
                  author_opt_eq(candidate->has_replay_author, &candidate->replay_author, has_replay_author, &replay_author)))
                continue;
            candidate->queue = queue;
            candidate->local_acknowledged = local_acknowledged;
            if (!candidate->has_replay_unit && replay_unit) {
                jazz_protocol__protocol__SyncMessage__clone(&candidate->replay_unit, replay_unit);
                candidate->has_replay_unit = true;
            }
            return;
        }
    }

    bool has_held_fate = false;
    jazz_protocol_SyncMessage held_fate;
    if (!replay_ready && pending) {
        JZ_VEC_FOR(jazz_db_LocalFateRoute, candidate, *pending) {
            bool live_same_author = !candidate->replay_ready &&
                                    author_opt_eq(candidate->has_replay_author, &candidate->replay_author, has_replay_author, &replay_author) &&
                                    queue_alive(candidate->queue);
            if (live_same_author && candidate->has_held_fate && jazz_db__db__local_fate_is_terminal(&candidate->held_fate)) {
                jazz_protocol__protocol__SyncMessage__clone(&held_fate, &candidate->held_fate);
                has_held_fate = true;
                break;
            }
        }
    }
    bool inserted;
    jazz_db_LocalFateRouteVec *entry = jz_btree_entry(&routes->map, &tx_id, &inserted);
    if (inserted) memset(entry, 0, sizeof *entry);
    jazz_db_LocalFateRoute route;
    memset(&route, 0, sizeof route);
    route.queue = queue;
    route.local_acknowledged = local_acknowledged;
    route.replay_ready = replay_ready;
    route.has_replay_author = has_replay_author;
    route.replay_author = replay_author;
    if (replay_unit) { jazz_protocol__protocol__SyncMessage__clone(&route.replay_unit, replay_unit); route.has_replay_unit = true; }
    route.has_held_fate = has_held_fate;
    if (has_held_fate) route.held_fate = held_fate;
    JZ_VEC_PUSH(*entry, route);
}

static void jazz_db__db__register_local_fate_route_with_acknowledgement(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id,
                                                                       jazz_db_PendingDownstreamFates queue, bool local_acknowledged) {
    jazz_types_AuthorSubject none;
    memset(&none, 0, sizeof none);
    jazz_db__db__register_local_fate_route_state(routes, tx_id, queue, local_acknowledged, true, false, none, NULL);
}

void jazz_db__db__register_local_fate_route(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id, jazz_db_PendingDownstreamFates queue) {
    jazz_db__db__register_local_fate_route_with_acknowledgement(routes, tx_id, queue, false);
}

void jazz_db__db__register_local_fate_observer(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id, jazz_db_PendingDownstreamFates queue) {
    jazz_db__db__register_local_fate_route_with_acknowledgement(routes, tx_id, queue, true);
}

void jazz_db__db__register_local_replay_route(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id, jazz_db_PendingDownstreamFates queue,
                                             jazz_types_AuthorSubject author, const jazz_protocol_SyncMessage *replay_unit) {
    jazz_db__db__register_local_fate_route_state(routes, tx_id, queue, false, false, true, author, replay_unit);
}

typedef struct { const jz_btree *durable; } AckCtx;
static bool queue_acknowledgements_retain(const void *k, void *v, void *ctx) {
    const jazz_model_TxId *tx_id = k;
    jazz_db_LocalFateRouteVec *pending = v;
    bool locally_durable = jz_btree_contains(((AckCtx *)ctx)->durable, tx_id);
    u32 w = 0;
    for (u32 r = 0; r < pending->len; ++r) {
        jazz_db_LocalFateRoute *route = &pending->ptr[r];
        if (!queue_alive(route->queue)) {
            if (!route->replay_ready) pending->ptr[w++] = *route;
            else local_fate_route_drop(route);
            continue;
        }
        if (route->replay_ready && locally_durable && !route->local_acknowledged) {
            jazz_protocol_SyncMessage ack;
            memset(&ack, 0, sizeof ack);
            ack.tag = JAZZ_SM_FATE_UPDATE;
            ack.v.fate_update.tx_id = *tx_id;
            ack.v.fate_update.fate.tag = JAZZ_FATE_PENDING;
            ack.v.fate_update.has_global_time = false;
            ack.v.fate_update.has_durability = true;
            ack.v.fate_update.durability = JAZZ_DUR_LOCAL;
            JZ_VEC_PUSH(route->queue->v, ack);
            route->local_acknowledged = true;
        }
        pending->ptr[w++] = *route;
    }
    pending->len = w;
    return pending->len != 0;
}

void jazz_db__db__queue_local_acknowledgements(jazz_db_LocalFateRoutes routes, jazz_db_SharedNodeState *node) {
    /* Acknowledged routes remain registered for later global/rejection fates. */
    JZ_VEC(jazz_model_TxId) tx_ids = {0};
    void *k, *v;
    JZ_BTREE_FOR(&routes->map, k, v) {
        jazz_db_LocalFateRouteVec *pending = v;
        bool any = false;
        JZ_VEC_FOR(jazz_db_LocalFateRoute, route, *pending) {
            if (route->replay_ready && !route->local_acknowledged && queue_alive(route->queue)) { any = true; break; }
        }
        if (any) JZ_VEC_PUSH(tx_ids, *(jazz_model_TxId *)k);
    }
    jz_btree durable;
    jz_btree_init(&durable, &txid_set_desc);
    jazz_node_NodeState *state = jazz_db__db__SharedNodeState__lock(node);
    JZ_VEC_FOR(jazz_model_TxId, tx_id, tx_ids) {
        jazz_model_Fate fate; bool has_gt; u64 gt; u32 durability;
        if (jazz_node__node__NodeState__transaction_state(state, *tx_id, &fate, &has_gt, &gt, &durability) && durability >= JAZZ_DUR_LOCAL)
            jz_btree_insert(&durable, tx_id, NULL, NULL);
    }
    jazz_db__db__SharedNodeState__unlock(node);
    jz_free(tx_ids.ptr);
    AckCtx ctx = { &durable };
    jz_btree_retain(&routes->map, queue_acknowledgements_retain, &ctx);
    jz_btree_drop(&durable);
}

bool jazz_db__db__local_fate_is_terminal(const jazz_protocol_SyncMessage *fate) {
    return fate->tag == JAZZ_SM_FATE_UPDATE &&
           (fate->v.fate_update.fate.tag == JAZZ_FATE_REJECTED ||
            (fate->v.fate_update.has_durability && fate->v.fate_update.durability == JAZZ_DUR_GLOBAL));
}

static bool release_replay_retain(const void *k, void *v, void *ctx) {
    (void)k; (void)ctx;
    jazz_db_LocalFateRouteVec *pending = v;
    u32 w = 0;
    for (u32 r = 0; r < pending->len; ++r) {
        jazz_db_LocalFateRoute *route = &pending->ptr[r];
        if (!route->replay_ready) { pending->ptr[w++] = *route; continue; }
        if (!queue_alive(route->queue)) { local_fate_route_drop(route); continue; }
        if (!route->has_held_fate) { pending->ptr[w++] = *route; continue; }
        jazz_protocol_SyncMessage fate = route->held_fate;
        route->has_held_fate = false;
        bool terminal = jazz_db__db__local_fate_is_terminal(&fate);
        JZ_VEC_PUSH(route->queue->v, fate);
        if (!terminal) pending->ptr[w++] = *route;
        else local_fate_route_drop(route);
    }
    pending->len = w;
    return pending->len != 0;
}

void jazz_db__db__release_local_replay_fates(jazz_db_LocalFateRoutes routes) {
    jz_btree_retain(&routes->map, release_replay_retain, NULL);
}

void jazz_db__db__route_local_fate(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id, const jazz_protocol_SyncMessage *fate) {
    bool terminal = jazz_db__db__local_fate_is_terminal(fate);
    jazz_db_LocalFateRouteVec *pending = jz_btree_get(&routes->map, &tx_id);
    if (!pending) return;
    u32 w = 0;
    for (u32 r = 0; r < pending->len; ++r) {
        jazz_db_LocalFateRoute *candidate = &pending->ptr[r];
        if (!candidate->replay_ready) {
            /* Never replace a terminal fate with a later non-terminal progress update. */
            bool replace = (candidate->has_held_fate && jazz_db__db__local_fate_is_terminal(&candidate->held_fate)) ? terminal : true;
            if (replace) {
                if (candidate->has_held_fate) jazz_protocol__protocol__SyncMessage__drop(&candidate->held_fate);
                jazz_protocol__protocol__SyncMessage__clone(&candidate->held_fate, fate);
                candidate->has_held_fate = true;
            }
            pending->ptr[w++] = *candidate;
            continue;
        }
        if (!queue_alive(candidate->queue)) { local_fate_route_drop(candidate); continue; }
        jazz_protocol_SyncMessage copy;
        jazz_protocol__protocol__SyncMessage__clone(&copy, fate);
        JZ_VEC_PUSH(candidate->queue->v, copy);
        if (!terminal) pending->ptr[w++] = *candidate;
        else local_fate_route_drop(candidate);
    }
    pending->len = w;
    if (pending->len == 0) jz_btree_remove(&routes->map, &tx_id, NULL, NULL);
}

/* ---- local replay planning ---- */
typedef enum { JAZZ_LRS_VISITING, JAZZ_LRS_COMPLETE, JAZZ_LRS_BLOCKED } jazz_db_LocalReplayStatus;
static const jz_btree_desc replay_status_desc = { sizeof(jazz_model_TxId), sizeof(u32), txid_cmp, NULL, NULL };
static void txid_vec_drop(void *p) { jz_vec_drop(p, sizeof(jazz_model_TxId), NULL); }
static const jz_btree_desc parents_desc = { sizeof(jazz_model_TxId), sizeof(jz_vec), txid_cmp, NULL, txid_vec_drop };

static bool jazz_db__db__local_replay_unit_is_complete(const jazz_protocol_SyncMessage *unit) {
    if (unit->tag != JAZZ_SM_COMMIT_UNIT) return false;
    return unit->v.commit_unit.tx.n_total_writes == unit->v.commit_unit.versions.len;
}

typedef enum { JAZZ_LRF_ENTER, JAZZ_LRF_EXIT } jazz_db_LocalReplayFrameTag;
typedef struct { u32 tag; jazz_model_TxId tx_id; } jazz_db_LocalReplayFrame;
typedef struct { jazz_model_TxId tx_id; jazz_protocol_SyncMessage unit; } TxUnit;
typedef JZ_VEC(TxUnit) TxUnitVec;
static void tx_unit_drop(void *p) { jazz_protocol__protocol__SyncMessage__drop(&((TxUnit *)p)->unit); }

static void clone_message_map(jz_btree *dst, const jz_btree *src) {
    jz_btree_clone(dst, src, NULL, jazz_protocol__protocol__SyncMessage__clone);
}

static jz_err jazz_db__db__plan_local_replay_commit_units(jazz_node_NodeState *node, const jz_btree *roots, const jz_btree *retained_replay_units,
                                                        jz_btree *statuses_out, jz_btree *units_out, TxUnitVec *complete_out) {
    jz_btree statuses, units, overrides, parents_by_tx;
    jz_btree_init(&statuses, &replay_status_desc);
    jz_btree_init(&units, &txid_message_desc);
    clone_message_map(&overrides, retained_replay_units);
    jz_btree_init(&parents_by_tx, &parents_desc);
    TxUnitVec complete_units = {0};

    void *rk, *rv;
    JZ_BTREE_FOR(roots, rk, rv) {
        JZ_VEC(jazz_db_LocalReplayFrame) frames = {0};
        jazz_db_LocalReplayFrame enter = { JAZZ_LRF_ENTER, *(jazz_model_TxId *)rk };
        JZ_VEC_PUSH(frames, enter);
        while (frames.len > 0) {
            jazz_db_LocalReplayFrame frame = frames.ptr[--frames.len];
            jazz_model_TxId tx_id = frame.tx_id;
            if (frame.tag == JAZZ_LRF_ENTER) {
                u32 *status = jz_btree_get(&statuses, &tx_id);
                if (status && (*status == JAZZ_LRS_COMPLETE || *status == JAZZ_LRS_BLOCKED)) continue;
                if (status && *status == JAZZ_LRS_VISITING) {
                    /* A causal parent must not point back into the active stack. */
                    u32 blocked = JAZZ_LRS_BLOCKED;
                    jz_btree_insert(&statuses, &tx_id, &blocked, NULL);
                    continue;
                }
                jazz_protocol_SyncMessage unit;
                if (!jz_btree_remove(&overrides, &tx_id, NULL, &unit)) {
                    jz_err e = jazz_node__node__NodeState__commit_unit_for(node, tx_id, &unit);
                    if (e.code == JAZZ_NODE_ERR_MISSING_TRANSACTION) {
                        jz_err_drop(&e);
                        u32 blocked = JAZZ_LRS_BLOCKED;
                        jz_btree_insert(&statuses, &tx_id, &blocked, NULL);
                        continue;
                    }
                    if (JZ_IS_ERR(e)) {
                        jz_free(frames.ptr);
                        jz_btree_drop(&statuses); jz_btree_drop(&units); jz_btree_drop(&overrides); jz_btree_drop(&parents_by_tx);
                        jz_vec_drop(&complete_units, sizeof(TxUnit), tx_unit_drop);
                        return e;
                    }
                }
                if (unit.tag != JAZZ_SM_COMMIT_UNIT) jz_panic("commit_unit_for always returns a commit unit");
                u64 n_total_writes = unit.v.commit_unit.tx.n_total_writes;
                u32 version_count = unit.v.commit_unit.versions.len;
                /* Linear history: replay order is transaction order. */
                jz_vec parents = {0};
                jz_btree_insert(&units, &tx_id, &unit, NULL);
                if (n_total_writes != version_count) {
                    u32 blocked = JAZZ_LRS_BLOCKED;
                    jz_btree_insert(&statuses, &tx_id, &blocked, NULL);
                    continue;
                }
                jz_btree_insert(&parents_by_tx, &tx_id, &parents, NULL);
                u32 visiting = JAZZ_LRS_VISITING;
                jz_btree_insert(&statuses, &tx_id, &visiting, NULL);
                jazz_db_LocalReplayFrame exit_frame = { JAZZ_LRF_EXIT, tx_id };
                JZ_VEC_PUSH(frames, exit_frame);
                /* parents.into_iter().rev(): none under linear history */
            } else {
                u32 *status = jz_btree_get(&statuses, &tx_id);
                if (!(status && *status == JAZZ_LRS_VISITING)) continue;
                bool complete = true;
                jz_vec *parents = jz_btree_get(&parents_by_tx, &tx_id);
                if (parents) {
                    for (u32 i = 0; i < parents->len; ++i) {
                        u32 *ps = jz_btree_get(&statuses, &((jazz_model_TxId *)parents->ptr)[i]);
                        if (!(ps && *ps == JAZZ_LRS_COMPLETE)) { complete = false; break; }
                    }
                }
                if (complete) {
                    u32 done = JAZZ_LRS_COMPLETE;
                    jz_btree_insert(&statuses, &tx_id, &done, NULL);
                    TxUnit tu;
                    tu.tx_id = tx_id;
                    if (!jz_btree_remove(&units, &tx_id, NULL, &tu.unit)) jz_panic("visiting replay node retains its unit");
                    JZ_VEC_PUSH(complete_units, tu);
                } else {
                    u32 blocked = JAZZ_LRS_BLOCKED;
                    jz_btree_insert(&statuses, &tx_id, &blocked, NULL);
                }
            }
        }
        jz_free(frames.ptr);
    }
    jz_btree_drop(&overrides);
    jz_btree_drop(&parents_by_tx);
    *statuses_out = statuses;
    *units_out = units;
    *complete_out = complete_units;
    return JZ_OK;
}

/* The pending units recovery uploads, keyed by transaction. */
static void jazz_db__db__local_replay_upload_order(const jz_btree *pending, const TxUnitVec *complete, const jz_btree *roots,
                                                  const jz_btree *statuses, const jz_btree *blocked, const jz_btree *retained, jz_btree *uploads) {
    jz_btree_init(uploads, &txid_message_desc);
    JZ_VEC_FOR(TxUnit, tu, *complete) {
        if (jz_btree_contains(pending, &tu->tx_id) && !jz_btree_contains(uploads, &tu->tx_id)) {
            jazz_protocol_SyncMessage c;
            jazz_protocol__protocol__SyncMessage__clone(&c, &tu->unit);
            jz_btree_insert(uploads, &tu->tx_id, &c, NULL);
        }
    }
    void *k, *v;
    JZ_BTREE_FOR(roots, k, v) {
        const jazz_model_TxId *tx_id = k;
        u32 *status = jz_btree_get(statuses, tx_id);
        if (!jz_btree_contains(pending, tx_id) || (status && *status == JAZZ_LRS_COMPLETE)) continue;
        const jazz_protocol_SyncMessage *unit = jz_btree_get(blocked, tx_id);
        if (!unit) unit = jz_btree_get(retained, tx_id);
        if (unit && !jz_btree_contains(uploads, tx_id)) {
            jazz_protocol_SyncMessage c;
            jazz_protocol__protocol__SyncMessage__clone(&c, unit);
            jz_btree_insert(uploads, tx_id, &c, NULL);
        }
    }
}

jz_err jazz_db__db__restore_local_subscriber_replay(jazz_db_SharedNodeState *node, jazz_db_Outbox outbox, jazz_db_LocalFateRoutes routes,
                                                    jazz_types_AuthorSubject author, jazz_db_PendingDownstreamFates downstream_fates) {
    jz_btree retained_replay_roots, retained_replay_units;
    jz_btree_init(&retained_replay_roots, &txid_set_desc);
    jz_btree_init(&retained_replay_units, &txid_message_desc);
    {
        void *k, *v;
        JZ_BTREE_FOR(&routes->map, k, v) {
            const jazz_model_TxId *tx_id = k;
            jazz_db_LocalFateRouteVec *list = v;
            bool has_matching_route = false;
            JZ_VEC_FOR(jazz_db_LocalFateRoute, route, *list) {
                if (route->replay_ready || !(route->has_replay_author && jazz_types_AuthorSubject_eq(&route->replay_author, &author))) continue;
                has_matching_route = true;
                if (route->has_replay_unit && jazz_db__db__local_replay_unit_is_complete(&route->replay_unit) &&
                    !jz_btree_contains(&retained_replay_units, tx_id)) {
                    jazz_protocol_SyncMessage c;
                    jazz_protocol__protocol__SyncMessage__clone(&c, &route->replay_unit);
                    jz_btree_insert(&retained_replay_units, tx_id, &c, NULL);
                }
            }
            if (has_matching_route) jz_btree_insert(&retained_replay_roots, tx_id, NULL, NULL);
        }
    }
    jazz_node_NodeState *node_state = jazz_db__db__SharedNodeState__lock(node);
    JZ_VEC(jazz_model_TxId) pending = {0};
    jz_err e = jazz_node__node__NodeState__pending_transaction_ids_for_author(node_state, author, (void *)&pending);
    if (JZ_IS_ERR(e)) {
        jazz_db__db__SharedNodeState__unlock(node);
        jz_btree_drop(&retained_replay_roots); jz_btree_drop(&retained_replay_units);
        return jazz_node__node__api_error__Error__from_node_error(e);
    }
    jz_sort(pending.ptr, pending.len, sizeof(jazz_model_TxId), txid_cmp, NULL);
    jz_btree pending_set;
    jz_btree_init(&pending_set, &txid_set_desc);
    JZ_VEC_FOR(jazz_model_TxId, t, pending) jz_btree_insert(&pending_set, t, NULL, NULL); /* sort + dedup */
    jz_free(pending.ptr);
    jz_btree roots;
    jz_btree_clone(&roots, &pending_set, NULL, NULL);
    {
        void *k, *v;
        JZ_BTREE_FOR(&retained_replay_roots, k, v) jz_btree_insert(&roots, k, NULL, NULL);
    }
    jz_btree statuses, blocked_units;
    TxUnitVec replay_units;
    e = jazz_db__db__plan_local_replay_commit_units(node_state, &roots, &retained_replay_units, &statuses, &blocked_units, &replay_units);
    jazz_db__db__SharedNodeState__unlock(node);
    if (JZ_IS_ERR(e)) {
        jz_btree_drop(&retained_replay_roots); jz_btree_drop(&retained_replay_units);
        jz_btree_drop(&pending_set); jz_btree_drop(&roots);
        return jazz_node__node__api_error__Error__from_node_error(e);
    }

    jz_hmap outbox_units;
    jz_hmap_init(&outbox_units, &jazz_db_txid_set_desc);
    for (u32 i = 0; i < OUTBOX_FN(len)(outbox); ++i) {
        const jazz_db_PendingUpload *p = OUTBOX_FN(at)(outbox, i);
        if (p->has_unit) jz_hmap_insert(&outbox_units, &p->tx_id, NULL, NULL);
    }
    jz_btree uploads;
    jazz_db__db__local_replay_upload_order(&pending_set, &replay_units, &roots, &statuses, &blocked_units, &retained_replay_units, &uploads);
    JZ_VEC_FOR(TxUnit, tu, replay_units) {
        /* Send accepted causal ancestors before each pending unit. */
        JZ_VEC_PUSH(downstream_fates->v, tu->unit);
    }
    jz_free(replay_units.ptr);
    {
        void *k, *v;
        JZ_BTREE_FOR(&uploads, k, v) {
            const jazz_model_TxId *tx_id = k;
            bool was_new = !jz_hmap_get(&outbox_units, tx_id);
            if (was_new) {
                jz_hmap_insert(&outbox_units, tx_id, NULL, NULL);
                /* Never replace an exact authored unit with its redacted history replay. */
                jazz_db__db__queue_pending_upload_in(outbox, *tx_id, v);
            }
        }
    }
    jz_btree_drop(&uploads);
    jz_hmap_drop(&outbox_units);

    {
        void *k, *v;
        JZ_BTREE_FOR(&roots, k, v) {
            u32 *status = jz_btree_get(&statuses, k);
            if (!(status && *status == JAZZ_LRS_COMPLETE)) continue;
            /* Re-admit through the blocked state first. */
            jazz_db__db__register_local_replay_route(routes, *(jazz_model_TxId *)k, downstream_fates, author, NULL);
            jazz_db__db__register_local_fate_route(routes, *(jazz_model_TxId *)k, downstream_fates);
        }
    }
    {
        void *k, *v;
        JZ_BTREE_FOR(&roots, k, v) {
            u32 *status = jz_btree_get(&statuses, k);
            if (status && *status == JAZZ_LRS_COMPLETE) continue;
            /* Keep the exact pending root for a later repair-ready reconnect. */
            const jazz_protocol_SyncMessage *unit = jz_btree_get(&blocked_units, k);
            if (!unit) unit = jz_btree_get(&retained_replay_units, k);
            jazz_db__db__register_local_replay_route(routes, *(jazz_model_TxId *)k, downstream_fates, author, unit);
        }
    }
    jazz_db__db__queue_local_acknowledgements(routes, node);
    jazz_db__db__release_local_replay_fates(routes);
    jz_btree_drop(&retained_replay_roots); jz_btree_drop(&retained_replay_units);
    jz_btree_drop(&pending_set); jz_btree_drop(&roots);
    jz_btree_drop(&statuses); jz_btree_drop(&blocked_units);
    return JZ_OK;
}

/* ---- schema views ---- */
groove_Value jazz_db__db__default_cell_for_column_type(const groove_ColumnType *column_type, const groove_Value *default_) {
    groove_Value out;
    if (column_type->tag == GROOVE_COLTYPE_NULLABLE && default_->tag != GROOVE_V_NULLABLE) {
        out.tag = GROOVE_V_NULLABLE;
        out.v.nullable = jz_alloc(sizeof(groove_Value));
        groove__records__Value__clone(out.v.nullable, default_);
        return out;
    }
    groove__records__Value__clone(&out, default_);
    return out;
}

static jz_err jazz_db__db__schema_view_column_default(const jazz_model_ColumnSchema *column, groove_Value *out) {
    if (column->has_default) { groove__records__Value__clone(out, &column->default_); return JZ_OK; }
    if (column->column_type.tag == GROOVE_COLTYPE_NULLABLE) {
        out->tag = GROOVE_V_NULLABLE;
        out->v.nullable = NULL;
        return JZ_OK;
    }
    return jz_err_fmt(JAZZ_EC_SCHEMA, "schema view column %.*s requires a migration default", (int)column->name.len, column->name.ptr);
}

static void table_lens_drop(void *p) {
    jazz_protocol_TableLens *l = p;
    jz_str_drop(&l->source_table);
    jz_str_drop(&l->target_table);
    for (u32 i = 0; i < l->ops.len; ++i) { jz_str_drop(&l->ops.ptr[i].column); groove__records__Value__drop(&l->ops.ptr[i].default_); }
    jz_free(l->ops.ptr);
}

static const jazz_model_TableSchema *find_table(const jazz_model_JazzSchema *schema, const jz_str *name) {
    JZ_VEC_FOR(jazz_model_TableSchema, t, schema->tables) if (jz_str_eq(&t->name, name)) return t;
    return NULL;
}
static const jazz_model_ColumnSchema *find_column(const jazz_model_TableSchema *table, const jz_str *name) {
    JZ_VEC_FOR(jazz_model_ColumnSchema, c, table->columns) if (jz_str_eq(&c->name, name)) return c;
    return NULL;
}

jz_err jazz_db__db__direct_schema_view_lens(const jazz_model_JazzSchema *source, const jazz_model_JazzSchema *target,
                                            jazz_protocol_MigrationLens **lens, jazz_db_StrVec *new_tables, jazz_db_StrVec *dropped_tables) {
    jazz_types_SchemaVersionId source_id = jazz_model__schema__JazzSchema__version_id(source);
    jazz_types_SchemaVersionId target_id = jazz_model__schema__JazzSchema__version_id(target);
    JZ_VEC(jazz_protocol_TableLens) table_lenses = {0};
    jz_err err = JZ_OK;
    JZ_VEC_FOR(jazz_model_TableSchema, source_table, source->tables) {
        const jazz_model_TableSchema *target_table = find_table(target, &source_table->name);
        if (!target_table) continue;
        const char *diff = NULL;
        if (!jazz_model__schema__TableSchema__references_eq(source_table, target_table)) diff = "schema view changes references on %.*s without an explicit lens";
        else if (!jazz_model__schema__TableSchema__merge_strategies_eq(source_table, target_table)) diff = "schema view changes merge strategies on %.*s without an explicit lens";
        else if (!jazz_model__schema__TableSchema__indexed_columns_eq(source_table, target_table)) diff = "schema view changes indices on %.*s without explicit index admission";
        else if (!jazz_model__schema__TableSchema__composite_indexes_eq(source_table, target_table)) diff = "schema view changes composite indexes on %.*s without explicit index admission";
        if (diff) { err = jz_err_fmt(JAZZ_EC_SCHEMA, diff, (int)target_table->name.len, target_table->name.ptr); goto fail; }
        JZ_VEC(jazz_protocol_LensOp) ops = {0};
        JZ_VEC_FOR(jazz_model_ColumnSchema, target_column, target_table->columns) {
            const jazz_model_ColumnSchema *source_column = find_column(source_table, &target_column->name);
            if (source_column && groove__schema__ColumnType__eq(&source_column->column_type, &target_column->column_type)) continue;
            if (source_column) {
                err = jz_err_fmt(JAZZ_EC_SCHEMA, "schema view changes type of %.*s.%.*s without an explicit lens",
                                 (int)target_table->name.len, target_table->name.ptr, (int)target_column->name.len, target_column->name.ptr);
                jazz_protocol_TableLens tmp = { {0}, {0}, {ops.ptr, ops.len, ops.cap} };
                table_lens_drop(&tmp);
                goto fail;
            }
            jazz_protocol_LensOp op;
            op.tag = JAZZ_LENS_ADD_COLUMN;
            err = jazz_db__db__schema_view_column_default(target_column, &op.default_);
            if (JZ_IS_ERR(err)) { jazz_protocol_TableLens tmp = { {0}, {0}, {ops.ptr, ops.len, ops.cap} }; table_lens_drop(&tmp); goto fail; }
            op.column = jz_str_clone(&target_column->name);
            JZ_VEC_PUSH(ops, op);
        }
        JZ_VEC_FOR(jazz_model_ColumnSchema, source_column, source_table->columns) {
            if (find_column(target_table, &source_column->name)) continue;
            jazz_protocol_LensOp op;
            op.tag = JAZZ_LENS_DROP_COLUMN;
            err = jazz_db__db__schema_view_column_default(source_column, &op.default_);
            if (JZ_IS_ERR(err)) { jazz_protocol_TableLens tmp = { {0}, {0}, {ops.ptr, ops.len, ops.cap} }; table_lens_drop(&tmp); goto fail; }
            op.column = jz_str_clone(&source_column->name);
            JZ_VEC_PUSH(ops, op);
        }
        jazz_protocol_TableLens tl;
        tl.source_table = jz_str_clone(&source_table->name);
        tl.target_table = jz_str_clone(&target_table->name);
        tl.ops.ptr = ops.ptr; tl.ops.len = ops.len; tl.ops.cap = ops.cap;
        JZ_VEC_PUSH(table_lenses, tl);
    }
    memset(new_tables, 0, sizeof *new_tables);
    memset(dropped_tables, 0, sizeof *dropped_tables);
    JZ_VEC_FOR(jazz_model_TableSchema, t, target->tables) if (!find_table(source, &t->name)) JZ_VEC_PUSH(*new_tables, jz_str_clone(&t->name));
    JZ_VEC_FOR(jazz_model_TableSchema, t, source->tables) if (!find_table(target, &t->name)) JZ_VEC_PUSH(*dropped_tables, jz_str_clone(&t->name));
    err = jazz_protocol__protocol__MigrationLens__new(source_id, target_id, table_lenses.ptr, table_lenses.len, lens);
    if (JZ_IS_ERR(err)) jz_panic("valid migration lens");
    jz_free(table_lenses.ptr);
    return JZ_OK;
fail:
    jz_vec_drop(&table_lenses, sizeof(jazz_protocol_TableLens), table_lens_drop);
    return err;
}

/* ---- mutation error state ---- */
static void mutation_error_event_drop(void *p) { jazz_db__db__MutationErrorEvent__drop(p); }
const jz_btree_desc jazz_db_mutation_error_pending_desc = { sizeof(jazz_model_TxId), sizeof(jazz_db_MutationErrorEvent), txid_cmp, NULL, mutation_error_event_drop };
static const jz_btree_desc txid_usize_desc = { sizeof(jazz_model_TxId), sizeof(usize), txid_cmp, NULL, NULL };
static void mutation_error_state_drop(void *p) {
    jazz_db_MutationErrorState *s = p;
    if (s->has_callback) jazz_db__db__MutationErrorCallback__drop(&s->callback);
    jz_btree_drop(&s->pending);
    jz_btree_drop(&s->application_waiters);
}
jazz_db_SharedMutationErrors jazz_db__db__SharedMutationErrors__new(void) {
    jazz_db_MutationErrorState *s = jz_rc_new(sizeof *s, mutation_error_state_drop);
    s->has_callback = false;
    jz_btree_init(&s->pending, &jazz_db_mutation_error_pending_desc);
    jz_btree_init(&s->application_waiters, &txid_usize_desc);
    return s;
}

jazz_db_WriteWaitOptions jazz_db__db__WriteWaitOptions__from_tier(u32 tier) {
    jazz_db_WriteWaitOptions o = { tier, false };
    return o;
}

bool jazz_db__db__StreamingValueUploadCleanupTicket__result(const jazz_db_StreamingValueUploadCleanupTicket *t, jz_err *out) {
    if (!t->result->ready) return false;
    *out = err_clone(&t->result->result);
    return true;
}

/* ---- UploadOutbox ---- */
static void pending_upload_drop(void *p) {
    jazz_db_PendingUpload *u = p;
    if (u->has_unit) jazz_protocol__protocol__SyncMessage__drop(&u->unit);
}
static void outbox_drop(void *p) {
    jazz_db_UploadOutbox *o = p;
    jz_deque_drop(&o->entries, pending_upload_drop);
    jz_hmap_drop(&o->tx_ids);
    jz_hmap_drop(&o->on_wire);
    jz_deque_drop(&o->upload_failures, NULL);
}
jazz_db_Outbox jazz_db__db__Outbox__new(void) {
    jazz_db_UploadOutbox *o = jz_rc_new(sizeof *o, outbox_drop);
    jz_deque_init(&o->entries, sizeof(jazz_db_PendingUpload));
    jz_hmap_init(&o->tx_ids, &jazz_db_txid_set_desc);
    o->declared_root = false;
    o->upstream_attached = false;
    jz_hmap_init(&o->on_wire, &jazz_db_txid_set_desc);
    jz_deque_init(&o->upload_failures, sizeof(jazz_db_UploadFailure));
    o->upload_progress = 0;
    return o;
}

/* Whether a subscriber upload this node settled terminally has nobody above it. */
bool OUTBOX_FN(settles_uploads_locally)(const jazz_db_UploadOutbox *o) { return o->declared_root && !o->upstream_attached; }
void OUTBOX_FN(mark_upstream_attached)(jazz_db_UploadOutbox *o) { o->upstream_attached = true; }

bool OUTBOX_FN(push)(jazz_db_UploadOutbox *o, jazz_db_PendingUpload pending) {
    if (jz_hmap_get(&o->tx_ids, &pending.tx_id)) { pending_upload_drop(&pending); return false; }
    jz_hmap_insert(&o->tx_ids, &pending.tx_id, NULL, NULL);
    jz_deque_push_back(&o->entries, &pending);
    return true;
}

u32 OUTBOX_FN(len)(const jazz_db_UploadOutbox *o) { return o->entries.len; }
const jazz_db_PendingUpload *OUTBOX_FN(at)(const jazz_db_UploadOutbox *o, u32 i) { return jz_deque_at(&o->entries, i); }
bool OUTBOX_FN(contains)(const jazz_db_UploadOutbox *o, jazz_model_TxId tx_id) { return jz_hmap_get(&o->tx_ids, &tx_id) != NULL; }

static bool on_wire_keep(const void *k, void *v, void *ctx) { (void)v; return jz_hmap_get((const jz_hmap *)ctx, k) != NULL; }

void OUTBOX_FN(retain)(jazz_db_UploadOutbox *o, bool (*keep)(const jazz_db_PendingUpload *p, void *ctx), void *ctx) {
    jz_deque kept;
    jz_deque_init(&kept, sizeof(jazz_db_PendingUpload));
    jazz_db_PendingUpload p;
    while (jz_deque_pop_front(&o->entries, &p)) {
        if (keep(&p, ctx)) jz_deque_push_back(&kept, &p);
        else pending_upload_drop(&p);
    }
    jz_deque_drop(&o->entries, NULL);
    o->entries = kept;
    jz_hmap_clear(&o->tx_ids);
    for (u32 i = 0; i < o->entries.len; ++i) jz_hmap_insert(&o->tx_ids, &((jazz_db_PendingUpload *)jz_deque_at(&o->entries, i))->tx_id, NULL, NULL);
    jz_hmap_retain(&o->on_wire, on_wire_keep, &o->tx_ids);
}

/* Record that an upstream link has sent this queued commit. */
void OUTBOX_FN(mark_on_wire)(jazz_db_UploadOutbox *o, jazz_model_TxId tx_id) {
    if (jz_hmap_get(&o->tx_ids, &tx_id)) jz_hmap_insert(&o->on_wire, &tx_id, NULL, NULL);
    OUTBOX_FN(note_upload_progress)(o);
}
void OUTBOX_FN(note_upload_progress)(jazz_db_UploadOutbox *o) { o->upload_progress += 1; }
u64 OUTBOX_FN(upload_progress)(const jazz_db_UploadOutbox *o) { return o->upload_progress; }
void OUTBOX_FN(forget_on_wire)(jazz_db_UploadOutbox *o) { jz_hmap_clear(&o->on_wire); }
bool OUTBOX_FN(awaits_wire)(const jazz_db_UploadOutbox *o, jazz_model_TxId tx_id) {
    return jz_hmap_get(&o->tx_ids, &tx_id) && !jz_hmap_get(&o->on_wire, &tx_id);
}
void OUTBOX_FN(mark_upload_failed)(jazz_db_UploadOutbox *o, jazz_model_TxId tx_id, const char *reason) {
    if (o->upload_failures.len >= JAZZ_DB_RECENT_UPLOAD_FAILURES) {
        jazz_db_UploadFailure old;
        jz_deque_pop_front(&o->upload_failures, &old);
    }
    jazz_db_UploadFailure f = { tx_id, reason };
    jz_deque_push_back(&o->upload_failures, &f);
}
const char *OUTBOX_FN(upload_failure)(const jazz_db_UploadOutbox *o, jazz_model_TxId tx_id) {
    for (u32 i = 0; i < o->upload_failures.len; ++i) {
        jazz_db_UploadFailure *f = jz_deque_at(&o->upload_failures, i);
        if (jazz_model__tx__TxId__eq(&f->tx_id, &tx_id)) return f->reason;
    }
    return NULL;
}

static bool not_released(const jazz_db_PendingUpload *p, void *ctx) { return jz_hmap_get((const jz_hmap *)ctx, &p->tx_id) == NULL; }

void OUTBOX_FN(remove_released)(jazz_db_UploadOutbox *o, jz_hmap *released, jz_hmap *completed) {
    /* Ordinary single-transaction uploads retain their prefix fast path. */
    jz_hmap_init(completed, &jazz_db_txid_set_desc);
    {
        void *k, *v;
        JZ_HMAP_FOR(released, k, v) jz_hmap_insert(completed, k, NULL, NULL);
    }
    while (o->entries.len > 0) {
        jazz_db_PendingUpload *front = jz_deque_at(&o->entries, 0);
        if (!jz_hmap_remove(released, &front->tx_id, NULL, NULL)) break;
        jazz_db_PendingUpload pending;
        if (!jz_deque_pop_front(&o->entries, &pending)) jz_panic("released outbox front remains present");
        jz_hmap_remove(&o->tx_ids, &pending.tx_id, NULL, NULL);
        jz_hmap_remove(&o->on_wire, &pending.tx_id, NULL, NULL);
        pending_upload_drop(&pending);
    }
    if (released->len == 0) return;
    OUTBOX_FN(retain)(o, not_released, released);
}

/* Queue an upload, letting an exact canonical unit supersede a recovery marker. */
bool jazz_db__db__queue_pending_upload_in(jazz_db_Outbox outbox, jazz_model_TxId tx_id, const jazz_protocol_SyncMessage *unit) {
    if (jz_hmap_get(&outbox->tx_ids, &tx_id)) {
        for (u32 i = 0; i < outbox->entries.len; ++i) {
            jazz_db_PendingUpload *pending = jz_deque_at(&outbox->entries, i);
            if (!jazz_model__tx__TxId__eq(&pending->tx_id, &tx_id)) continue;
            if (!unit) return false;
            if (pending->has_unit && jazz_protocol__protocol__SyncMessage__eq(&pending->unit, unit)) return false;
            /* The canonical payload must win even when both entries have a body. */
            if (pending->has_unit) jazz_protocol__protocol__SyncMessage__drop(&pending->unit);
            jazz_protocol__protocol__SyncMessage__clone(&pending->unit, unit);
            pending->has_unit = true;
            return true;
        }
    }
    jazz_db_PendingUpload p;
    memset(&p, 0, sizeof p);
    p.tx_id = tx_id;
    if (unit) { jazz_protocol__protocol__SyncMessage__clone(&p.unit, unit); p.has_unit = true; }
    OUTBOX_FN(push)(outbox, p);
    return true;
}

/* Whether a transaction has reached the requested application-visible wait boundary. */
bool jazz_db__db__transaction_satisfies_wait(const jazz_model_Fate *fate, bool has_global_time, u32 durability, u32 tier) {
    return durability >= tier && (tier <= JAZZ_DUR_LOCAL || fate->tag == JAZZ_FATE_ACCEPTED) &&
           (tier < JAZZ_DUR_GLOBAL || has_global_time);
}

/* ---- QueryAttachment ---- */
jazz_protocol_SubscriptionKey jazz_db__db__QueryAttachment__subscription(const jazz_db_QueryAttachment *a) {
    if (a->subscriptions.len == 0) jz_panic("index out of bounds: the len is 0 but the index is 0");
    return a->subscriptions.ptr[0];
}
void jazz_db__db__QueryAttachment__drop(jazz_db_QueryAttachment *a) {
    jz_free(a->subscriptions.ptr);
    jz_free(a->required_after.ptr);
    jz_free(a->registrations.ptr);
    jz_free(a->refreshes.ptr);
}
void jazz_db__db__QueryAttachment__clone(jazz_db_QueryAttachment *dst, const jazz_db_QueryAttachment *src) {
    *dst = *src;
    memset(&dst->subscriptions, 0, sizeof dst->subscriptions);
    memset(&dst->required_after, 0, sizeof dst->required_after);
    memset(&dst->registrations, 0, sizeof dst->registrations);
    memset(&dst->refreshes, 0, sizeof dst->refreshes);
    jz_vec_extend(&dst->subscriptions, src->subscriptions.ptr, src->subscriptions.len, sizeof *src->subscriptions.ptr);
    jz_vec_extend(&dst->required_after, src->required_after.ptr, src->required_after.len, sizeof *src->required_after.ptr);
    jz_vec_extend(&dst->registrations, src->registrations.ptr, src->registrations.len, sizeof *src->registrations.ptr);
    jz_vec_extend(&dst->refreshes, src->refreshes.ptr, src->refreshes.len, sizeof *src->refreshes.ptr);
}

/* ---- WriteStateChange ---- */
bool jazz_db__db__WriteStateChange__poll(jazz_db_WriteStateChange *f, const jz_waker *cx) {
    (void)cx;
    u8 unit;
    return jz_chan_try_recv(f->receiver, &unit) || jz_chan_is_closed(f->receiver);
}

void jazz_db__db__WriteStateChange__drop(jazz_db_WriteStateChange *f) {
    jazz_db_WriteStateWaiterVec *tx_waiters = jz_btree_get(&f->waiters->map, &f->tx_id);
    if (tx_waiters) {
        u32 w = 0;
        for (u32 r = 0; r < tx_waiters->len; ++r) {
            if (tx_waiters->ptr[r].id != f->waiter_id) tx_waiters->ptr[w++] = tx_waiters->ptr[r];
            else jz_chan_close(tx_waiters->ptr[r].notify);
        }
        tx_waiters->len = w;
        if (w == 0) jz_btree_remove(&f->waiters->map, &f->tx_id, NULL, NULL);
    }
    jz_chan_release(f->receiver);
    jz_rc_release(f->waiters);
}

/* ---- PermissionAdviceFuture ---- */
bool jazz_db__db__PermissionAdviceFuture__poll(jazz_db_PermissionAdviceFuture *f, const jz_waker *cx, u32 *out) {
    (void)cx;
    if (jz_chan_try_recv(f->receiver, out)) return true;
    if (jz_chan_is_closed(f->receiver)) { *out = JAZZ_ADVICE_UNKNOWN; return true; }
    return false;
}
jazz_protocol_PermissionAdviceRequestId jazz_db__db__PermissionAdviceFuture__request_id(const jazz_db_PermissionAdviceFuture *f) { return f->request_id; }
void jazz_db__db__PermissionAdviceFuture__drop(jazz_db_PermissionAdviceFuture *f) {
    jz_chan *sender;
    if (jz_btree_remove(&f->waiters->map, &f->request_id, NULL, &sender)) jz_chan_close(sender);
    jz_chan_release(f->receiver);
    jz_rc_release(f->waiters);
}

/* ---- ReadOpts ---- */
void jazz_db__db__ReadOpts__default(jazz_db_ReadOpts *out) {
    out->tier = JAZZ_DB_READ_LOCAL_FIRST;
    out->include_deleted = false;
    jazz_protocol__protocol__ReadViewSpec__default(&out->read_view);
    out->first_load.tag = JAZZ_FL_DELIVER; out->first_load.timeout_ms = 0;
}
void jazz_db__db__ReadOpts__for_read_tier(jazz_db_ReadOpts *out, u32 tier) {
    jazz_db__db__ReadOpts__default(out);
    out->tier = tier;
}
void jazz_db__db__ReadOpts__branch_view(jazz_db_ReadOpts *self, jazz_model_BranchSelector head, const jazz_protocol_BranchViewBase *base) {
    jazz_protocol__protocol__ReadViewSpec__drop(&self->read_view);
    jazz_protocol_BranchViewBase b;
    memset(&b, 0, sizeof b);
    if (base) b = *base;
    jazz_protocol__protocol__ReadViewSpec__branch_view(&self->read_view, head, base != NULL, b);
}

/* ---- error constructors ---- */
jz_err jazz_db__db__transaction_abandoned(jazz_model_OpenTransactionId open_tx_id) {
    jz_str id = {0};
    jazz_model__model__transaction__OpenTransactionId__display(&id, &open_tx_id);
    jz_err e = jz_err_fmt(JAZZ_EC_PROTOCOL, "transaction handle was abandoned: %.*s", (int)id.len, id.ptr);
    jz_str_drop(&id);
    return e;
}

static void uuid_str(const jz_uuid *u, char out[37]) { jz_uuid_to_string(u, out); }

jz_err jazz_db__db__row_already_deleted(jazz_types_RowUuid row) {
    char s[37];
    uuid_str(&row, s);
    return jz_err_fmt(JAZZ_EC_WRITE_REJECTED, "row already deleted: %s", s);
}

jz_err jazz_db__db__row_already_exists(jz_slice table, jazz_types_RowUuid row) {
    char s[37];
    uuid_str(&row, s);
    return jz_err_fmt(JAZZ_EC_WRITE_REJECTED, "row already exists in table %.*s: %s", (int)table.len, (const char *)table.ptr, s);
}

jz_err jazz_db__db__read_for_write_denied(const char *operation, jz_slice table) {
    return jz_err_fmt(JAZZ_EC_WRITE_REJECTED,
                      "read policy denied %s on table %.*s: the operation requires read permission on the target row",
                      operation, (int)table.len, (const char *)table.ptr);
}

/* An UPDATE on a row this replica does not hold. */
jz_err jazz_db__db__update_target_not_loaded(const char *operation, jz_slice table, jazz_types_RowUuid row) {
    char s[37];
    uuid_str(&row, s);
    return jz_err_fmt(JAZZ_EC_NOT_OBSERVED,
                      "%s on table %.*s needs the current row, but row %s is not loaded locally; read or subscribe to the row before updating it",
                      operation, (int)table.len, (const char *)table.ptr, s);
}

/* ======================= db.rs part 3: admission, write API, tx ops (db.rs:3726-4940) ======================= */
#define EMSG(e) (int)((e).msg ? (e).msg->len : 0), ((e).msg ? (e).msg->ptr : "")
static jz_err recode_err(jz_err e, u32 code) { e.code = code; return e; }
static jz_err wrap_err(u32 code, const char *prefix, jz_err e) {
    jz_err out = jz_err_fmt(code, "%s: %.*s", prefix, EMSG(e));
    jz_err_drop(&e);
    return out;
}

/* RowCells / claims map desc: String -> Value */
static void str_key_drop(void *p) { jz_str_drop(p); }
static void str_key_clone(void *d, const void *s) { *(jz_str *)d = jz_str_clone(s); }
const jz_btree_desc jazz_db_str_value_desc = { sizeof(jz_str), sizeof(groove_Value), jz_cmp_str, str_key_drop, groove__records__Value__drop };
void jazz_db__db__RowCells__clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, str_key_clone, groove__records__Value__clone); }
jazz_db_PolicyBinding jazz_db__db__PolicyBinding__clone(const jazz_db_PolicyBinding *b) {
    jazz_db_PolicyBinding out;
    out.author = b->author;
    jazz_db__db__RowCells__clone(&out.claims, &b->claims);
    return out;
}
void jazz_db__db__PolicyBinding__drop(jazz_db_PolicyBinding *b) { jz_btree_drop(&b->claims); }

/* Messages whose semantics assert downstream authority state must never be
 * accepted from a subscriber transport. */
bool jazz_db__db__subscriber_inbound_message_is_authority_only(const jazz_protocol_SyncMessage *message, jazz_node_CommitUnitIngestContext ingest, const jazz_node_PeerState *peer) {
    switch (message->tag) {
    case JAZZ_SM_FATE_UPDATE: case JAZZ_SM_SUBSCRIBE_REJECTED: case JAZZ_SM_CATALOGUE_ACK: case JAZZ_SM_VIEW_UPDATE:
    case JAZZ_SM_CATALOGUE_SNAPSHOT: case JAZZ_SM_AUTHORIZATION_SCOPE_VIEW: case JAZZ_SM_AUTHORIZATION_SCOPE_AGGREGATE_RECEIPT:
    case JAZZ_SM_AUTHORIZATION_SCOPE_UNAVAILABLE: case JAZZ_SM_AUTHORIZATION_SCOPE_DECISION:
        return true;
    default: break;
    }
    return message->tag == JAZZ_SM_SESSION_CLAIMS
        && (jazz_node__peer__PeerState__rejects_raw_session_claims(peer)
            || (!jazz_node__node__CommitUnitTrust__is_trusted(ingest.trust)
                && !jazz_db__db__delegated_session_capability(ingest, jazz_node__peer__PeerState__role(peer))));
}

/* Only the host-admitted core-facing relay may carry another session's binding. */
bool jazz_db__db__delegated_session_capability(jazz_node_CommitUnitIngestContext ingest, jazz_node_PeerRole role) {
    return ingest.trust == JAZZ_CUT_RELAY && role.tag == JAZZ_PEER_ROLE_RELAY;
}

/* Select the immutable session snapshot permitted for one request. */
bool jazz_db__db__admitted_request_policy_binding(jazz_node_CommitUnitIngestContext ingest, const jazz_node_PeerState *peer,
                                                  const jazz_db_PolicyBinding *direct, const jazz_protocol_DelegatedSessionBinding *delegated,
                                                  jazz_db_PolicyBinding *out) {
    jazz_node_PeerRole role = jazz_node__peer__PeerState__role(peer);
    if (!delegated) {
        if (role.tag == JAZZ_PEER_ROLE_RELAY) return false;
        if (!direct) return false;
        *out = jazz_db__db__PolicyBinding__clone(direct);
        return true;
    }
    if (jazz_db__db__delegated_session_capability(ingest, role)) {
        if (!jazz_node__peer__PeerState__admits_relay_binding(peer, &delegated->identity, &delegated->claims)) return false;
        out->author = delegated->identity;
        jazz_db__db__RowCells__clone(&out->claims, &delegated->claims);
        return true;
    }
    if (ingest.trust == JAZZ_CUT_TRUSTED_BACKEND && role.tag == JAZZ_PEER_ROLE_CLIENT_LINK) {
        out->author = delegated->identity;
        jazz_db__db__RowCells__clone(&out->claims, &delegated->claims);
        return true;
    }
    if (jazz_node__peer__PeerState__authority_query_delegate(peer) && ingest.trust == JAZZ_CUT_TRUSTED_AUTHORITY
        && jazz_types_AuthorSubject_eq(&ingest.identity, &jazz_types__ids__AuthorSubject__SYSTEM)
        && role.tag == JAZZ_PEER_ROLE_CLIENT_LINK) {
        out->author = delegated->identity;
        jazz_db__db__RowCells__clone(&out->claims, &delegated->claims);
        return true;
    }
    return false;
}

bool jazz_db__db__subscriber_permission_subject(jazz_node_CommitUnitIngestContext ingest, jazz_types_AuthorSubject *out) {
    switch (ingest.trust) {
    case JAZZ_CUT_SESSION: *out = ingest.identity; return true;
    case JAZZ_CUT_RELAY: return false;
    default: *out = jazz_types__ids__AuthorSubject__SYSTEM; return true;
    }
}

/* ---- write targets / options ---- */
jazz_model_BranchSelector jazz_db__db__ExactWriteTarget__branch(const jazz_db_ExactWriteTarget *t) {
    jazz_model_BranchSelector out;
    if (t->tag == JAZZ_EWT_ROOT) jazz_model__model__branch__BranchSelector__default(&out);
    else jazz_model__model__branch__BranchSelector__clone(&out, &t->branch);
    return out;
}

jz_err jazz_db__db__ensure_transaction_identity(jazz_db_WriteIdentity identity) {
    if (identity.tag == JAZZ_WI_DATABASE) return JZ_OK;
    return jz_err_new(JAZZ_EC_SCHEMA, "transaction identity is selected when the transaction is opened");
}

jz_err jazz_db__db__ensure_exclusive_target(const jazz_db_ExactWriteTarget *target) {
    if (target->tag != JAZZ_EWT_ROOT)
        return jz_err_new(JAZZ_EC_SCHEMA, "exclusive transactions do not support branch writes");
    return JZ_OK;
}

jz_err jazz_db__db__ensure_exclusive_view_target(const jazz_db_WriteTarget *target) {
    if (target->tag != JAZZ_WT_ROOT)
        return jz_err_new(JAZZ_EC_SCHEMA, "exclusive transactions do not support branch writes");
    return JZ_OK;
}

static void write_target_drop(jazz_db_WriteTarget *t) {
    if (t->tag == JAZZ_WT_BRANCH_VIEW) {
        jazz_model__model__branch__BranchSelector__drop(&t->head);
        if (t->has_base) jazz_protocol__protocol__BranchViewBase__drop(&t->base);
    }
}
static void exact_target_drop(jazz_db_ExactWriteTarget *t) {
    if (t->tag == JAZZ_EWT_BRANCH) jazz_model__model__branch__BranchSelector__drop(&t->branch);
}

jazz_types_RowUuid jazz_db__db__Db__next_row_id(const jazz_db_Db *db) {
    jazz_db_RowIdSource *src = &db->row_id_source->source; /* borrow_mut() */
    return src->vt->next_row_id(src->self);
}

/* ---- MergeableTxOps defaults (one C copy shared by MergeableTx and MergeableTxRef) ---- */
jz_err MTX_FN(insert)(const jazz_db_TxHandle *t, jz_slice table, jazz_db_RowCells cells, jazz_db_InsertOptions options, jazz_types_RowUuid *out) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&cells); exact_target_drop(&options.target); return e; }
    bool known_fresh_row = !options.has_row_id && t->db->row_id_source_guarantees_fresh;
    jazz_types_RowUuid row = options.has_row_id ? options.row_id : jazz_db__db__Db__next_row_id(t->db);
    if (options.target.tag == JAZZ_EWT_ROOT) {
        e = DBTX_FN(stage_mergeable_insert)(t->db, t->tx_id, table, row, cells, options.has_updated_at_ms, options.updated_at_ms, known_fresh_row);
    } else {
        e = DBTX_FN(stage_mergeable_insert_in_branch)(t->db, t->tx_id, table, options.target.branch, row, cells,
                                                     options.has_updated_at_ms, options.updated_at_ms, known_fresh_row);
    }
    if (JZ_IS_ERR(e)) return e;
    *out = row;
    return JZ_OK;
}

jz_err MTX_FN(update)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells patch, jazz_db_UpdateOptions options) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&patch); write_target_drop(&options.target); return e; }
    if (options.target.tag == JAZZ_WT_ROOT)
        return DBTX_FN(stage_mergeable_update)(t->db, t->tx_id, table, row, patch, options.has_updated_at_ms, options.updated_at_ms);
    return DBTX_FN(stage_mergeable_update_in_branch_view)(t->db, t->tx_id, table, options.target.head, options.target.has_base, options.target.base,
                                                         row, patch, options.has_updated_at_ms, options.updated_at_ms);
}

jz_err MTX_FN(upsert)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, jazz_db_UpsertOptions options) {
    return DBTX_FN(stage_mergeable_upsert)(t->db, t->tx_id, table, row, cells, options, false);
}

jz_err MTX_FN(delete)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_DeleteOptions options) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (JZ_IS_ERR(e)) { write_target_drop(&options.target); return e; }
    if (options.target.tag == JAZZ_WT_ROOT)
        return DBTX_FN(stage_mergeable_delete)(t->db, t->tx_id, table, row, options.has_updated_at_ms, options.updated_at_ms);
    return DBTX_FN(stage_mergeable_delete_in_branch_view)(t->db, t->tx_id, table, options.target.head, options.target.has_base, options.target.base,
                                                         row, options.has_updated_at_ms, options.updated_at_ms);
}

jz_err MTX_FN(restore)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, bool has_cells, jazz_db_RowCells cells, jazz_db_RestoreOptions options) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (JZ_IS_ERR(e)) { if (has_cells) jz_btree_drop(&cells); exact_target_drop(&options.target); return e; }
    if (!has_cells) {
        exact_target_drop(&options.target);
        return jz_err_new(JAZZ_EC_SCHEMA, "transaction restores currently require replacement cells");
    }
    if (options.target.tag == JAZZ_EWT_ROOT)
        return DBTX_FN(stage_mergeable_restore)(t->db, t->tx_id, table, row, cells, options.has_updated_at_ms, options.updated_at_ms);
    return DBTX_FN(stage_mergeable_restore_in_branch)(t->db, t->tx_id, table, options.target.branch, row, cells,
                                                     options.has_updated_at_ms, options.updated_at_ms);
}

/* Stage an atomic move of one object branch-local row between exact branch keys. */
jz_err MTX_FN(move_between_branches)(const jazz_db_TxHandle *t, jz_slice table, jazz_model_BranchSelector source, jazz_model_BranchSelector target, jazz_types_RowUuid row) {
    if (jazz_model__model__branch__BranchSelector__eq(&source, &target)) {
        jazz_model__model__branch__BranchSelector__drop(&source);
        jazz_model__model__branch__BranchSelector__drop(&target);
        return jz_err_new(JAZZ_EC_SCHEMA, "branch move requires distinct source and target selectors");
    }
    bool has = false;
    jz_btree cells;
    jz_btree_init(&cells, &jazz_db_str_value_desc);
    jazz_node_NodeState *ns = jazz_db__db__SharedNodeState__lock(t->db->node->node);
    jz_err e = jazz_node__node__NodeState__visible_current_cells_in_branch(ns, table, &source, row, &has, &cells);
    jazz_db__db__SharedNodeState__unlock(t->db->node->node);
    if (JZ_IS_ERR(e) || !has) {
        jazz_model__model__branch__BranchSelector__drop(&source);
        jazz_model__model__branch__BranchSelector__drop(&target);
        if (JZ_IS_ERR(e)) return e;
        char s[37];
        jz_uuid_to_string(&row, s);
        return jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "source branch-local row is not visible: %s", s);
    }
    const jazz_model_TableSchema *table_schema = NULL;
    JZ_VEC_FOR(const jazz_model_TableSchema, candidate, t->db->schema.tables) {
        jz_slice name = jz_str_slice(&candidate->name);
        if (name.len == table.len && memcmp(name.ptr, table.ptr, table.len) == 0) { table_schema = candidate; break; }
    }
    if (!table_schema) {
        jz_btree_drop(&cells);
        jazz_model__model__branch__BranchSelector__drop(&source);
        jazz_model__model__branch__BranchSelector__drop(&target);
        return jz_err_fmt(JAZZ_EC_SCHEMA, "unknown table %.*s", (int)table.len, (const char *)table.ptr);
    }
    jz_btree selector, target_cells;
    e = jazz_model__schema__JazzSchema__project_branch_selector(&t->db->schema, table_schema, &target, &selector, &target_cells);
    if (JZ_IS_ERR(e)) {
        jz_btree_drop(&cells);
        jazz_model__model__branch__BranchSelector__drop(&source);
        jazz_model__model__branch__BranchSelector__drop(&target);
        return recode_err(e, JAZZ_EC_SCHEMA);
    }
    jz_btree_drop(&selector);
    jz_btree_append(&cells, &target_cells); /* cells.extend(target_cells) */
    jz_btree_drop(&target_cells);
    jazz_db_RestoreOptions restore = {0};
    restore.target.tag = JAZZ_EWT_BRANCH;
    restore.target.branch = target;
    e = MTX_FN(restore)(t, table, row, true, cells, restore);
    if (JZ_IS_ERR(e)) { jazz_model__model__branch__BranchSelector__drop(&source); return e; }
    jazz_db_DeleteOptions del = {0};
    del.target.tag = JAZZ_WT_BRANCH_VIEW;
    del.target.head = source;
    del.target.has_base = false;
    return MTX_FN(delete)(t, table, row, del);
}

jz_err MTX_FN(read)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_db_RowCells *out) {
    return DBTX_FN(transaction_read)(t->db, t->tx_id, table, row, has, out);
}
jz_err MTX_FN(all_prepared_with_opts)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_CurrentRowVec *out) {
    return DBTX_FN(transaction_all)(t->db, t->tx_id, prepared, opts, out);
}
jz_err MTX_FN(all_prepared)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_db_CurrentRowVec *out) {
    jazz_db_ReadOpts opts;
    jazz_db__db__ReadOpts__default(&opts);
    return MTX_FN(all_prepared_with_opts)(t, prepared, opts, out);
}
jz_err MTX_FN(all_prepared_for_identity_with_opts)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, jazz_db_CurrentRowVec *out) {
    return DBTX_FN(transaction_all_for_identity)(t->db, t->tx_id, prepared, author, opts, out);
}
jz_err MTX_FN(all_prepared_for_identity)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_CurrentRowVec *out) {
    jazz_db_ReadOpts opts;
    jazz_db__db__ReadOpts__default(&opts);
    return MTX_FN(all_prepared_for_identity_with_opts)(t, prepared, author, opts, out);
}
jz_err MTX_FN(relation_snapshot_prepared_with_opts)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out) {
    return DBTX_FN(transaction_relation_snapshot)(t->db, t->tx_id, prepared, opts, out);
}
jz_err MTX_FN(relation_snapshot_prepared_for_identity_with_opts)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out) {
    return DBTX_FN(transaction_relation_snapshot_for_identity)(t->db, t->tx_id, prepared, author, opts, out);
}

/* Commit all staged writes as one mergeable transaction. */
jz_err jazz_db__db__MergeableTx__commit(jazz_db_TxHandle *t, jazz_model_TxId *out) {
    jz_err result = DBTX_FN(commit_mergeable_handle)(t->db, t->tx_id, out);
    if (!JZ_IS_ERR(result)) t->committed = true;
    jazz_db__db__MergeableTx__Drop__drop(t); /* `mut self` is consumed */
    return result;
}
void jazz_db__db__MergeableTx__Drop__drop(jazz_db_TxHandle *t) {
    if (t->committed) return;
    t->committed = true; /* drop runs once */
    jazz_db__db__node_runtime__Node__abandon_or_enqueue_transaction(t->db->node, t->tx_id);
}

/* ---- ExclusiveTxOps defaults ---- */
jz_err XTX_FN(read)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_db_RowCells *out) {
    return DBTX_FN(transaction_read)(t->db, t->tx_id, table, row, has, out);
}
jz_err XTX_FN(all)(const jazz_db_TxHandle *t, jz_slice table, jazz_db_CurrentRowVec *out) {
    return DBTX_FN(transaction_current_rows)(t->db, t->tx_id, table, out);
}
jz_err XTX_FN(all_prepared_with_opts)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_CurrentRowVec *out) {
    return DBTX_FN(transaction_all)(t->db, t->tx_id, prepared, opts, out);
}
jz_err XTX_FN(all_prepared)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_db_CurrentRowVec *out) {
    jazz_db_ReadOpts opts;
    jazz_db__db__ReadOpts__default(&opts);
    return XTX_FN(all_prepared_with_opts)(t, prepared, opts, out);
}
jz_err XTX_FN(all_prepared_for_identity_with_opts)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, jazz_db_CurrentRowVec *out) {
    return DBTX_FN(transaction_all_for_identity)(t->db, t->tx_id, prepared, author, opts, out);
}
jz_err XTX_FN(all_prepared_for_identity)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_CurrentRowVec *out) {
    jazz_db_ReadOpts opts;
    jazz_db__db__ReadOpts__default(&opts);
    return XTX_FN(all_prepared_for_identity_with_opts)(t, prepared, author, opts, out);
}
jz_err XTX_FN(relation_snapshot_prepared_with_opts)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out) {
    return DBTX_FN(transaction_relation_snapshot)(t->db, t->tx_id, prepared, opts, out);
}
jz_err XTX_FN(relation_snapshot_prepared_for_identity_with_opts)(const jazz_db_TxHandle *t, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out) {
    return DBTX_FN(transaction_relation_snapshot_for_identity)(t->db, t->tx_id, prepared, author, opts, out);
}

jz_err XTX_FN(insert)(const jazz_db_TxHandle *t, jz_slice table, jazz_db_RowCells cells, jazz_db_InsertOptions options, jazz_types_RowUuid *out) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_exclusive_target(&options.target);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&cells); exact_target_drop(&options.target); return e; }
    jazz_types_RowUuid row = options.has_row_id ? options.row_id : jazz_db__db__Db__next_row_id(t->db);
    e = DBTX_FN(stage_exclusive_insert)(t->db, t->tx_id, table, row, cells, options.has_updated_at_ms, options.updated_at_ms, options.has_row_id);
    if (JZ_IS_ERR(e)) return e;
    *out = row;
    return JZ_OK;
}
jz_err XTX_FN(update)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells patch, jazz_db_UpdateOptions options) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_exclusive_view_target(&options.target);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&patch); write_target_drop(&options.target); return e; }
    return DBTX_FN(stage_exclusive_update)(t->db, t->tx_id, table, row, patch, options.has_updated_at_ms, options.updated_at_ms);
}
jz_err XTX_FN(upsert)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, jazz_db_UpsertOptions options) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_exclusive_view_target(&options.target);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&cells); write_target_drop(&options.target); return e; }
    return DBTX_FN(stage_exclusive_upsert)(t->db, t->tx_id, table, row, cells, options.has_updated_at_ms, options.updated_at_ms, false);
}
jz_err XTX_FN(delete)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_DeleteOptions options) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_exclusive_view_target(&options.target);
    if (JZ_IS_ERR(e)) { write_target_drop(&options.target); return e; }
    return DBTX_FN(stage_exclusive_delete)(t->db, t->tx_id, table, row, options.has_updated_at_ms, options.updated_at_ms);
}
jz_err XTX_FN(restore)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, bool has_cells, jazz_db_RowCells cells, jazz_db_RestoreOptions options) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_exclusive_target(&options.target);
    if (JZ_IS_ERR(e)) { if (has_cells) jz_btree_drop(&cells); exact_target_drop(&options.target); return e; }
    if (!has_cells) return jz_err_new(JAZZ_EC_SCHEMA, "exclusive transaction restores require replacement cells");
    return DBTX_FN(stage_exclusive_restore)(t->db, t->tx_id, table, row, cells, options.has_updated_at_ms, options.updated_at_ms);
}

jz_err jazz_db__db__ExclusiveTx__commit(jazz_db_TxHandle *t, jazz_model_TxId *out) {
    jz_err result = DBTX_FN(commit_exclusive_handle)(t->db, t->tx_id, out);
    if (!JZ_IS_ERR(result)) t->committed = true;
    jazz_db__db__ExclusiveTx__Drop__drop(t);
    return result;
}
void jazz_db__db__ExclusiveTx__Drop__drop(jazz_db_TxHandle *t) {
    if (t->committed) return;
    t->committed = true;
    jazz_db__db__node_runtime__Node__abandon_or_enqueue_transaction(t->db->node, t->tx_id);
}

/* ---- WriteHandle ---- */
jazz_types_RowUuid jazz_db__db__WriteHandle__row_uuid(const jazz_db_WriteHandle *w) { return w->row_uuid; }
jazz_model_TxId jazz_db__db__WriteHandle__mergeable_tx_id(const jazz_db_WriteHandle *w) { return w->tx_id; }

/* The message keeps the core diagnostic and ends with the readable reason. */
static jz_err jazz_db__db__write_rejected(const jz_str *transaction_id_debug, const jazz_model_RejectionReason *reason) {
    jz_str code = {0}, readable = {0}, reason_dbg = {0};
    jazz_db__db__mutation_errors__mutation_error_details(reason, &code, &readable);
    jazz_model__tx__RejectionReason__debug(&reason_dbg, reason);
    jz_err e = jz_err_fmt(JAZZ_EC_WRITE_REJECTED, "transaction %.*s was rejected: %.*s (reason: %.*s)",
                          (int)transaction_id_debug->len, transaction_id_debug->ptr, (int)reason_dbg.len, reason_dbg.ptr,
                          (int)readable.len, readable.ptr);
    jz_str_drop(&code);
    jz_str_drop(&readable);
    jz_str_drop(&reason_dbg);
    return e;
}
jz_err jazz_db__db__write_rejected_tx(jazz_model_TxId tx_id, const jazz_model_RejectionReason *reason) {
    jz_str dbg = {0};
    jazz_model__tx__TxId__debug(&dbg, &tx_id);
    jz_err e = jazz_db__db__write_rejected(&dbg, reason);
    jz_str_drop(&dbg);
    return e;
}

/* Wait until this write has reached the requested tier. */
jz_err jazz_db__db__WriteHandle__wait(const jazz_db_WriteHandle *w, u32 tier, jazz_model_TxId *out) {
    bool reservation_is_pending = w->queued_status && w->queued_status->tag == JAZZ_QMS_PENDING;
    if (tier <= w->local_tier && !reservation_is_pending) { *out = w->tx_id; return JZ_OK; }
    jazz_db_WriteState state;
    JZ_TRY(jazz_db__db__WriteHandle__write_state(w, &state));
    jz_err result = JZ_OK;
    if (state.fate.tag == JAZZ_FATE_REJECTED) {
        result = jazz_db__db__write_rejected_tx(w->tx_id, &state.fate.rejected);
    } else if (state.fate.tag == JAZZ_FATE_PENDING && tier >= JAZZ_DUR_GLOBAL) {
        jz_str t = {0};
        jazz_model__tx__DurabilityTier__debug(&t, tier);
        result = jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "write has not been accepted at requested tier %.*s", (int)t.len, t.ptr);
        jz_str_drop(&t);
    } else if (!jazz_db__db__transaction_satisfies_wait(&state.fate, state.has_global_time, state.durability, tier)) {
        jz_str t = {0};
        jazz_model__tx__DurabilityTier__debug(&t, tier);
        result = jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "write has not reached requested tier %.*s", (int)t.len, t.ptr);
        jz_str_drop(&t);
    } else {
        *out = w->tx_id;
    }
    jazz_model__tx__Fate__drop(&state.fate);
    return result;
}

/* Return the locally observed fate and durability for this write. */
jz_err jazz_db__db__WriteHandle__write_state(const jazz_db_WriteHandle *w, jazz_db_WriteState *out) {
    if (w->queued_status) {
        switch (w->queued_status->tag) {
        case JAZZ_QMS_PENDING:
            memset(out, 0, sizeof *out);
            out->fate.tag = JAZZ_FATE_PENDING;
            out->has_global_time = false;
            out->durability = JAZZ_DUR_NONE;
            return JZ_OK;
        case JAZZ_QMS_FAILED: return err_clone(&w->queued_status->error);
        case JAZZ_QMS_PUBLISHED: break;
        }
    }
    if (!w->node || jz_rc_strong(w->node) == 0)
        return jz_err_new(JAZZ_EC_NOT_OBSERVED, "database handle was dropped");
    jazz_model_TxId resolved_tx_id = (w->queued_alias && w->queued_alias->has) ? w->queued_alias->tx_id : w->tx_id;
    jazz_node_NodeState *ns = jazz_db__db__SharedNodeState__lock(w->node);
    memset(out, 0, sizeof *out);
    u64 gt = 0;
    bool known = jazz_node__node__NodeState__transaction_state(ns, resolved_tx_id, &out->fate, &out->has_global_time, &gt, &out->durability);
    jazz_db__db__SharedNodeState__unlock(w->node);
    if (!known) {
        jz_str dbg = {0};
        jazz_model__tx__TxId__debug(&dbg, &w->tx_id);
        jz_err e = jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "transaction %.*s is not known locally", (int)dbg.len, dbg.ptr);
        jz_str_drop(&dbg);
        return e;
    }
    out->global_time = gt;
    return JZ_OK;
}

/* ---- materialize_result_tree ---- */
static int slice_cmp_desc(const void *a, const void *b, void *ctx) { (void)ctx; return jz_slice_cmp(*(const jz_slice *)a, *(const jz_slice *)b); }
static const jz_btree_desc array_by_name_desc = { sizeof(jz_slice), sizeof(const jazz_model_ArraySubquery *), slice_cmp_desc, NULL, NULL };
typedef JZ_VEC(groove_Value) ValueVec;
typedef JZ_VEC(groove_DescriptorField) FieldVec;
static void value_drop(void *p) { groove__records__Value__drop(p); }
static void descriptor_field_drop(void *p) {
    groove_DescriptorField *f = p;
    if (f->has_name) jz_str_drop(&f->name);
    groove__records__values__ValueType__drop(&f->value_type);
}

static jz_err jazz_db__db__materialize_result_tree__node_from_record(jz_slice table, groove_OwnedRecord record,
                                                                    const jazz_model_ArraySubquery *arrays, u32 n_arrays, jazz_db_ResultNode *out) {
    const groove_RecordDescriptor *descriptor = groove__records__OwnedRecord__descriptor(&record);
    ValueVec values = {0};
    jz_err e = groove__records__OwnedRecord__to_values(&record, (void *)&values);
    if (JZ_IS_ERR(e)) { groove__records__OwnedRecord__drop(&record); return wrap_err(JAZZ_EC_PROTOCOL, "invalid Groove terminal record", e); }
    jz_btree array_by_name;
    jz_btree_init(&array_by_name, &array_by_name_desc);
    for (u32 i = 0; i < n_arrays; i++) {
        jz_slice k = jz_str_slice(&arrays[i].column_name);
        const jazz_model_ArraySubquery *a = &arrays[i];
        jz_btree_insert(&array_by_name, &k, &a, NULL);
    }
    FieldVec scalar_fields = {0};
    ValueVec scalar_values = {0};
    jz_btree relations;
    jz_btree_init(&relations, &jazz_db__result_tree__relations_desc);
    u32 nfields = 0;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(descriptor, &nfields);
    u32 count = nfields < values.len ? nfields : values.len;
    u32 i = 0;
    for (; i < count; i++) {
        const groove_DescriptorField *field = &fields[i];
        groove_Value value = values.ptr[i];
        if (!field->has_name) {
            groove__records__Value__drop(&value);
            e = jz_err_new(JAZZ_EC_PROTOCOL, "Groove terminal emitted an unnamed field");
            goto fail;
        }
        jz_slice name = jz_str_slice(&field->name);
        const jazz_model_ArraySubquery **array = jz_btree_get(&array_by_name, &name);
        if (!array) {
            groove_DescriptorField f = {0};
            f.has_name = true;
            f.name = jz_str_clone(&field->name);
            groove__records__values__ValueType__clone(&f.value_type, &field->value_type);
            JZ_VEC_PUSH(scalar_fields, f);
            JZ_VEC_PUSH(scalar_values, value);
            continue;
        }
        if (value.tag != GROOVE_V_ARRAY) {
            groove__records__Value__drop(&value);
            e = jz_err_fmt(JAZZ_EC_PROTOCOL, "Groove terminal relation %.*s was not an array", (int)name.len, (const char *)name.ptr);
            goto fail;
        }
        jazz_db_ResultNodeVec children = {0};
        for (u32 c = 0; c < value.v.array.len; c++) {
            groove_Value *child = &value.v.array.ptr[c];
            if (child->tag != GROOVE_V_RECORD) {
                for (u32 r = c; r < value.v.array.len; r++) groove__records__Value__drop(&value.v.array.ptr[r]);
                jz_free(value.v.array.ptr);
                jz_vec_drop(&children, sizeof(jazz_db_ResultNode), jazz_db__result_tree__ResultNode__drop);
                e = jz_err_fmt(JAZZ_EC_PROTOCOL, "Groove terminal relation %.*s contained a non-record", (int)name.len, (const char *)name.ptr);
                goto fail;
            }
            jazz_db_ResultNode node;
            e = jazz_db__db__materialize_result_tree__node_from_record(jz_str_slice(&(*array)->table), child->v.record,
                                                                       (*array)->nested_arrays.ptr, (*array)->nested_arrays.len, &node);
            if (JZ_IS_ERR(e)) {
                for (u32 r = c + 1; r < value.v.array.len; r++) groove__records__Value__drop(&value.v.array.ptr[r]);
                jz_free(value.v.array.ptr);
                jz_vec_drop(&children, sizeof(jazz_db_ResultNode), jazz_db__result_tree__ResultNode__drop);
                goto fail;
            }
            JZ_VEC_PUSH(children, node);
        }
        jz_free(value.v.array.ptr);
        jz_str key = jz_str_clone(&field->name);
        jazz_db_ResultRelation rel = { JAZZ_DB_REL_ARRAY, children };
        jz_btree_insert(&relations, &key, &rel, NULL);
    }
    {
        const groove_RecordDescriptor *scalar_descriptor = groove__records__RecordDescriptor__new(scalar_fields.ptr, scalar_fields.len);
        jz_bytes raw = {0};
        e = groove__records__RecordDescriptor__create(scalar_descriptor, scalar_values.ptr, scalar_values.len, &raw);
        jz_vec_drop(&scalar_values, sizeof(groove_Value), value_drop);
        jz_free(scalar_fields.ptr); /* moved into the descriptor */
        jz_btree_drop(&array_by_name);
        jz_free(values.ptr);
        groove__records__OwnedRecord__drop(&record);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&relations); return wrap_err(JAZZ_EC_PROTOCOL, "invalid scalar projection from Groove terminal", e); }
        groove_OwnedRecord owned;
        groove__records__OwnedRecord__new(&owned, raw, scalar_descriptor);
        jazz_node__node__CurrentRow__new(&out->row, table, owned);
        jazz_types_RowUuid row_uuid = jazz_node__node__CurrentRow__row_uuid(&out->row);
        jazz_types__object__OutputOccurrenceId__single_source(&out->occurrence, jazz_types__object__ObjectId__from_uuid(&row_uuid));
        out->relations = relations;
        return JZ_OK;
    }
fail:
    for (u32 r = i + 1; r < values.len; r++) groove__records__Value__drop(&values.ptr[r]);
    jz_free(values.ptr);
    jz_vec_drop(&scalar_values, sizeof(groove_Value), value_drop);
    jz_vec_drop(&scalar_fields, sizeof(groove_DescriptorField), descriptor_field_drop);
    jz_btree_drop(&relations);
    jz_btree_drop(&array_by_name);
    groove__records__OwnedRecord__drop(&record);
    return e;
}

/* Decode Groove's recursively assembled terminal value into a typed result tree. */
jz_err jazz_db__db__materialize_result_tree(const jazz_model_Query *query, jazz_node_RelationSnapshot snapshot, jazz_db_ResultTree *out) {
    if (snapshot.edges.len != 0) {
        jazz_node__node__RelationSnapshot__drop(&snapshot);
        return jz_err_new(JAZZ_EC_PROTOCOL, "structured result unexpectedly contained relation-fact edges");
    }
    u32 n_arrays = 0;
    const jazz_model_ArraySubquery *arrays = jazz_model__query__Query__array_subqueries(query, &n_arrays);
    jazz_db_ResultNodeVec roots = {0};
    jz_err e = JZ_OK;
    u32 take = snapshot.root_count < snapshot.rows.len ? snapshot.root_count : snapshot.rows.len;
    for (u32 i = 0; i < take; i++) {
        const jazz_node_CurrentRow *row = &snapshot.rows.ptr[i];
        const groove_RecordDescriptor *descriptor;
        jz_slice raw;
        jazz_node__node__CurrentRow__encoded_record(row, &descriptor, &raw);
        groove_OwnedRecord record;
        groove__records__OwnedRecord__new(&record, jz_bytes_from(raw.ptr, raw.len), descriptor);
        jazz_db_ResultNode node;
        e = jazz_db__db__materialize_result_tree__node_from_record(jazz_node__node__CurrentRow__table(row), record, arrays, n_arrays, &node);
        if (JZ_IS_ERR(e)) break;
        JZ_VEC_PUSH(roots, node);
    }
    jazz_node__node__RelationSnapshot__drop(&snapshot);
    if (JZ_IS_ERR(e)) { jz_vec_drop(&roots, sizeof(jazz_db_ResultNode), jazz_db__result_tree__ResultNode__drop); return e; }
    out->roots = roots;
    return JZ_OK;
}

/* ======================= db.rs part 3b: reconciliation, publication, streams (db.rs:4940-5970) ======================= */
void jazz_db__db__LocalFuture__drop(jazz_db_LocalFuture *f) { if (f->drop) f->drop(f->self); f->self = NULL; f->drop = NULL; }
static void noop_wake(void *ctx) { (void)ctx; }
static const jz_waker noop_waker = { noop_wake, NULL };
void jazz_db__db__LocalFuture__block_on(jazz_db_LocalFuture *f, void *out) {
    while (!f->poll(f->self, &noop_waker, out)) jz_task_run_ready();
}

/* ---- ScalarReconciliation ---- */
void jazz_db__db__ScalarReconciliation__default(jazz_db_ScalarReconciliation *out) {
    memset(out, 0, sizeof *out);
    jz_deque_init(&out->pending, sizeof(jazz_types_RowUuid));
}
void jazz_db__db__ScalarReconciliation__drop(jazz_db_ScalarReconciliation *s) {
    jz_deque_drop(&s->pending, NULL);
    if (s->has_active) {
        jz_vec_drop(&s->active.rows, sizeof(jazz_types_RowUuid), NULL);
        jazz_db__db__LocalFuture__drop(&s->active.future);
        s->has_active = false;
    }
}

/* ---- deletion reconciliation ---- */
bool jazz_db__db__DeletionDiscoverySpec__same_discovery(const jazz_db_DeletionDiscoverySpec *a, const jazz_db_DeletionDiscoverySpec *b) {
    return jazz_model__query__ValidatedQuery__eq(a->shape, b->shape)
        && jazz_model__query__Binding__eq(a->binding, b->binding)
        && jazz_types_AuthorSubject_eq(&a->author, &b->author)
        && jazz_protocol__protocol__PolicyBindingKey__eq(&a->context, &b->context);
}
void jazz_db__db__DeletionDiscoverySpec__drop(jazz_db_DeletionDiscoverySpec *s) {
    jazz_model__query__ValidatedQuery__drop(s->shape);
    jazz_model__query__Binding__drop(s->binding);
    jz_btree_drop(&s->claims);
    jazz_protocol__protocol__PolicyBindingKey__drop(&s->context);
    jz_btree_drop(&s->authoritative);
}
static void spec_box_drop(jazz_db_DeletionDiscoverySpec *s) { if (s) { jazz_db__db__DeletionDiscoverySpec__drop(s); jz_free(s); } }

bool jazz_db__db__DeletionReconciliation__waiting_since(const jazz_db_DeletionReconciliation *d, u64 *out) {
    switch (d->tag) {
    case JAZZ_DR_DEFERRED: case JAZZ_DR_DISCOVERING: case JAZZ_DR_PENDING: case JAZZ_DR_PROBING:
        *out = d->since;
        return true;
    default: return false;
    }
}

int jazz_db__db__TableRow__cmp(const void *a, const void *b, void *ctx) {
    const jazz_db_TableRow *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table);
    return c ? c : jz_uuid_cmp(&x->row, &y->row);
}
void jazz_db__db__TableRow__drop(void *p) { jz_str_drop(&((jazz_db_TableRow *)p)->table); }

void jazz_db__db__DeletionReconciliation__drop(void *p) {
    jazz_db_DeletionReconciliation *d = p;
    switch (d->tag) {
    case JAZZ_DR_DEFERRED: spec_box_drop(d->spec); break;
    case JAZZ_DR_DISCOVERING:
        jazz_node__node__LocalMaintainedViewSubscription__drop(d->maintained);
        spec_box_drop(d->spec);
        break;
    case JAZZ_DR_PENDING:
        jz_vec_drop(&d->rows, sizeof(jazz_protocol_CurrentRowCoordinate), jazz_protocol__protocol__CurrentRowCoordinate__drop);
        jazz_protocol__protocol__PolicyBindingKey__drop(&d->context);
        break;
    case JAZZ_DR_PROBING: jz_vec_drop(&d->probing, sizeof(jazz_db_TableRow), jazz_db__db__TableRow__drop); break;
    default: break;
    }
    d->tag = JAZZ_DR_DISABLED;
}

static void held_row_check_drop(void *p) {
    jazz_db_HeldRowCheck *c = p;
    jazz_protocol__protocol__CurrentRowCoordinate__drop(&c->coordinate);
    jazz_protocol__protocol__PolicyBindingKey__drop(&c->context);
}
static const jz_btree_desc held_rows_desc = { sizeof(jazz_db_TableRow), sizeof(jazz_db_HeldRowCheck), jazz_db__db__TableRow__cmp, jazz_db__db__TableRow__drop, held_row_check_drop };

void jazz_db__db__HeldRowChecks__default(jazz_db_HeldRowChecks *out) {
    memset(out, 0, sizeof *out);
    jz_btree_init(&out->rows, &held_rows_desc);
}

void jazz_db__db__HeldRowChecks__enqueue(jazz_db_HeldRowChecks *self, jazz_db_CoordinateVec rows,
                                         const jazz_protocol_PolicyBindingKey *context, jazz_db_TableRowVec *out) {
    jazz_db_TableRowVec keys = {0};
    jz_vec_reserve(&keys, rows.len, sizeof(jazz_db_TableRow));
    for (u32 i = 0; i < rows.len; i++) {
        jazz_protocol_CurrentRowCoordinate coordinate = rows.ptr[i];
        jazz_db_TableRow key = { jz_str_clone(&coordinate.table), coordinate.row };
        jazz_db_HeldRowCheck *existing = jz_btree_get(&self->rows, &key);
        bool replace = !existing || !jazz_protocol__protocol__CurrentRowCoordinate__eq(&existing->coordinate, &coordinate);
        if (replace) {
            jazz_db_HeldRowCheck check = {0};
            check.coordinate = coordinate;
            jazz_protocol__protocol__PolicyBindingKey__clone(&check.context, context);
            check.attempts = 0;
            check.has_retry_at = false;
            jazz_db_TableRow k2 = { jz_str_clone(&key.table), key.row };
            jz_btree_insert(&self->rows, &k2, &check, NULL);
        } else {
            jazz_protocol__protocol__CurrentRowCoordinate__drop(&coordinate);
        }
        JZ_VEC_PUSH(keys, key);
    }
    jz_free(rows.ptr);
    *out = keys;
}

/* Hand a stream's unfinished discovery to the runtime. Returns true (and the
 * redundant reconciliation) when a same-query discovery superseded it. */
bool jazz_db__db__HeldRowChecks__adopt_discovery(jazz_db_HeldRowChecks *self, jazz_db_DeletionReconciliation reconciliation, jazz_db_DeletionReconciliation *redundant) {
    if (reconciliation.tag != JAZZ_DR_DEFERRED && reconciliation.tag != JAZZ_DR_DISCOVERING) {
        jazz_db__db__DeletionReconciliation__drop(&reconciliation);
        return false;
    }
    const jazz_db_DeletionDiscoverySpec *spec = reconciliation.spec;
    JZ_VEC_FOR(jazz_db_DeletionReconciliation, existing, self->discoveries) {
        if ((existing->tag == JAZZ_DR_DEFERRED || existing->tag == JAZZ_DR_DISCOVERING)
            && jazz_db__db__DeletionDiscoverySpec__same_discovery(existing->spec, spec)) {
            jz_btree_drop(&existing->spec->authoritative);
            jz_btree_clone(&existing->spec->authoritative, &spec->authoritative, NULL, NULL);
            *redundant = reconciliation;
            return true;
        }
    }
    JZ_VEC_PUSH(self->discoveries, reconciliation);
    return false;
}

u64 jazz_db__db__HeldRowChecks__retry_delay(u32 attempts) {
    u32 shift = attempts < 7 ? attempts : 7;
    u64 ms = 250ull << shift;
    return ms < 30000 ? ms : 30000;
}

/* ---- SubscriptionEvent ---- */
static void output_rows_clone(jazz_db_OutputRowVec *d, const jazz_db_OutputRowVec *s) { jz_vec_clone(d, s, sizeof(jazz_db_SubscriptionOutputRow), jazz_db__db__SubscriptionOutputRow__clone); }
static void output_rows_drop(jazz_db_OutputRowVec *v) { jz_vec_drop(v, sizeof(jazz_db_SubscriptionOutputRow), jazz_db__db__SubscriptionOutputRow__drop); }
static void removed_rows_drop(jazz_db_RemovedRowVec *v) { jz_vec_drop(v, sizeof(jazz_db_RemovedRow), jazz_db__db__RemovedRow__drop); }
static void occurrence_vec_drop(jazz_db_OccurrenceVec *v) { jz_vec_drop(v, sizeof(jazz_types_OutputOccurrenceId), jazz_types__object__OutputOccurrenceId__drop); }
static void occurrence_vec_clone(jazz_db_OccurrenceVec *d, const jazz_db_OccurrenceVec *s) { jz_vec_clone(d, s, sizeof(jazz_types_OutputOccurrenceId), jazz_types__object__OutputOccurrenceId__clone); }
static void terminal_ops_drop(jazz_db_TerminalOperationVec *v) { jz_vec_drop(v, sizeof(groove_TerminalOperation), groove__ivm__runtime__terminal__TerminalOperation__drop); }

void jazz_db__db__SubscriptionEvent__drop(void *p) {
    jazz_db_SubscriptionEvent *e = p;
    if (e->tag == JAZZ_SE_DELTA) {
        output_rows_drop(&e->added);
        output_rows_drop(&e->updated);
        removed_rows_drop(&e->removed);
        terminal_ops_drop(&e->terminal_operations);
    } else if (e->tag == JAZZ_SE_REJECTED) {
        jazz_protocol__protocol__SubscribeRejectReason__drop(&e->reason);
    }
    e->tag = JAZZ_SE_CLOSED;
}
void jazz_db__db__SubscriptionEvent__clone(void *dst, const void *src) {
    jazz_db_SubscriptionEvent *d = dst;
    const jazz_db_SubscriptionEvent *s = src;
    *d = *s;
    if (s->tag == JAZZ_SE_DELTA) {
        output_rows_clone(&d->added, &s->added);
        output_rows_clone(&d->updated, &s->updated);
        jz_vec_clone(&d->removed, &s->removed, sizeof(jazz_db_RemovedRow), jazz_db__db__RemovedRow__clone);
        jz_vec_clone(&d->terminal_operations, &s->terminal_operations, sizeof(groove_TerminalOperation), groove__ivm__runtime__terminal__TerminalOperation__clone);
    } else if (s->tag == JAZZ_SE_REJECTED) {
        jazz_protocol__protocol__SubscribeRejectReason__clone(&d->reason, &s->reason);
    }
}
jazz_db_SubscriptionEvent jazz_db__db__SubscriptionEvent__delta(bool reset, u32 tier, bool settled) {
    jazz_db_SubscriptionEvent e;
    memset(&e, 0, sizeof e);
    e.tag = JAZZ_SE_DELTA;
    e.reset = reset;
    e.publishable = true;
    e.settled = settled;
    e.tier = tier;
    return e;
}

/* RemovedRow::from_result_key */
void jazz_db__db__RemovedRow__from_result_key(jazz_db_RemovedRow *out, jz_str table, jazz_types_RowUuid row_uuid, jazz_types_ResultKey key) {
    out->table = table;
    out->row_uuid = row_uuid;
    jazz_types__object__OutputOccurrenceId__clone(&out->occurrence_id, &key.occ); /* key.as_occurrence().clone() */
    jazz_types__object__OutputOccurrenceId__drop(&key.occ);
    out->index = 0;
}

/* ---- RelationSnapshotIndex ---- */
static void terminal_record_drop(void *p) { jazz_node__node__terminal_record__TerminalRecordState__drop(*(jazz_node_TerminalRecordState **)p); }
static void terminal_record_clone(void *d, const void *s) { *(jazz_node_TerminalRecordState **)d = jazz_node__node__terminal_record__TerminalRecordState__clone(*(jazz_node_TerminalRecordState *const *)s); }
static const jz_btree_desc terminal_records_desc = { sizeof(jazz_types_OutputOccurrenceId), sizeof(jazz_node_TerminalRecordState *),
                                                     jazz_types__object__OutputOccurrenceId__cmp, jazz_types__object__OutputOccurrenceId__drop, terminal_record_drop };
const jz_btree_desc jazz_db_occurrence_set_desc = { sizeof(jazz_types_OutputOccurrenceId), 0, jazz_types__object__OutputOccurrenceId__cmp, jazz_types__object__OutputOccurrenceId__drop, NULL };

void jazz_db__db__RelationSnapshotIndex__default(jazz_db_RelationSnapshotIndex *out) {
    jazz_db__positional_order__PositionalOrder__default(&out->roots, &jazz_db__positional_order__occurrence_key);
    jz_btree_init(&out->terminal_records, &terminal_records_desc);
}
void jazz_db__db__RelationSnapshotIndex__clone(jazz_db_RelationSnapshotIndex *dst, const jazz_db_RelationSnapshotIndex *src) {
    jazz_db__positional_order__PositionalOrder__clone(&dst->roots, &src->roots);
    jz_btree_clone(&dst->terminal_records, &src->terminal_records, jazz_types__object__OutputOccurrenceId__clone, terminal_record_clone);
}
void jazz_db__db__RelationSnapshotIndex__drop(jazz_db_RelationSnapshotIndex *i) {
    jazz_db__positional_order__PositionalOrder__drop(&i->roots);
    jz_btree_drop(&i->terminal_records);
}
/* Index roots in sequence order. A repeated identity keeps its earlier
 * position occupied but anonymous. Occurrences are moved in. */
void jazz_db__db__RelationSnapshotIndex__with_root_occurrences(jazz_db_RelationSnapshotIndex *out, jazz_types_OutputOccurrenceId *occurrences, u32 n) {
    jazz_db__db__RelationSnapshotIndex__default(out);
    for (u32 i = 0; i < n; i++) {
        u32 ignored;
        jazz_db__positional_order__PositionalOrder__anonymize(&out->roots, &occurrences[i], &ignored);
        u32 position = jazz_db__positional_order__PositionalOrder__len(&out->roots);
        jazz_db__positional_order__PositionalOrder__insert(&out->roots, position, &occurrences[i]);
    }
}
void jazz_db__db__RelationSnapshotIndex__from_snapshot(jazz_db_RelationSnapshotIndex *out, const jazz_node_RelationSnapshot *snapshot) {
    jazz_db_OccurrenceVec occ = {0};
    u32 take = snapshot->root_count < snapshot->rows.len ? snapshot->root_count : snapshot->rows.len;
    for (u32 i = 0; i < take; i++) {
        jazz_types_OutputOccurrenceId o;
        jazz_db__db__subscription_row_occurrence_id(&snapshot->rows.ptr[i], &o);
        JZ_VEC_PUSH(occ, o);
    }
    jazz_db__db__RelationSnapshotIndex__with_root_occurrences(out, occ.ptr, occ.len);
    jz_free(occ.ptr);
}

/* ---- SubscriptionPublicationSnapshot ---- */
void jazz_db__db__SubscriptionPublicationSnapshot__drop(jazz_db_SubscriptionPublicationSnapshot *s) {
    jazz_node__node__RelationSnapshot__drop(&s->snapshot);
    occurrence_vec_drop(&s->occurrences);
}
static jz_err jazz_db__db__SubscriptionPublicationSnapshot__capture(const jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index,
                                                                   jazz_db_SubscriptionPublicationSnapshot *out) {
    JZ_TRY(jazz_db__db__materialized_subscription_snapshot(snapshot, index, &out->snapshot));
    jz_err e = jazz_db__db__snapshot_root_occurrences(snapshot, index, &out->occurrences);
    if (JZ_IS_ERR(e)) { jazz_node__node__RelationSnapshot__drop(&out->snapshot); return e; }
    return JZ_OK;
}

/* ---- SubscriptionSender ---- */
jazz_db_SubscriptionSender jazz_db__db__SubscriptionSender__clone(const jazz_db_SubscriptionSender *s) {
    jazz_db_SubscriptionSender out = *s;
    out.publication = jz_rc_clone(s->publication);
    /* UnboundedSender clone: the channel is shared by reference here */
    return out;
}
void jazz_db__db__SubscriptionSender__drop(jazz_db_SubscriptionSender *s) {
    jz_rc_release(s->publication);
    s->publication = NULL;
}

/* Validate only changed roots. */
jz_err jazz_db__db__SubscriptionSender__materialized(const jazz_db_SubscriptionSender *self, jazz_node_NodeState *node, const jazz_model_ValidatedQuery *shape,
                                                    const jazz_db_SubscriptionEvent *event, bool *out) {
    jazz_db_SubscriptionPublication *publication = self->publication;
    if (event->tag == JAZZ_SE_DELTA) {
        if (event->reset) jz_btree_clear(&publication->unresolved);
        JZ_VEC_FOR(const jazz_db_RemovedRow, row, event->removed) jz_btree_remove(&publication->unresolved, &row->occurrence_id, NULL, NULL);
        for (int pass = 0; pass < 2; pass++) {
            const jazz_db_OutputRowVec *rows = pass == 0 ? &event->added : &event->updated;
            JZ_VEC_FOR(const jazz_db_SubscriptionOutputRow, row, *rows) {
                jazz_node_RelationSnapshot changed = {0};
                changed.root_count = 1;
                jazz_node_CurrentRow r;
                jazz_node__node__CurrentRow__clone(&r, &row->row);
                JZ_VEC_PUSH(changed.rows, r);
                bool has_cells = false;
                jz_err e = jazz_node__node__NodeState__relation_snapshot_has_materialized_required_cells(node, shape, &changed, &has_cells);
                jazz_node__node__RelationSnapshot__drop(&changed);
                if (JZ_IS_ERR(e)) return e;
                if (has_cells) {
                    jz_btree_remove(&publication->unresolved, &row->occurrence_id, NULL, NULL);
                } else {
                    jazz_types_OutputOccurrenceId k;
                    jazz_types__object__OutputOccurrenceId__clone(&k, &row->occurrence_id);
                    jz_btree_insert(&publication->unresolved, &k, NULL, NULL);
                }
            }
        }
    }
    *out = publication->unresolved.len == 0;
    return JZ_OK;
}

jz_err jazz_db__db__SubscriptionSender__checkpoint(const jazz_db_SubscriptionSender *self, u32 tier, bool settled, const jazz_node_RelationSnapshot *snapshot,
                                                  const jazz_db_RelationSnapshotIndex *index, bool *has, jazz_db_SubscriptionPublicationSnapshot *out) {
    const jazz_db_SubscriptionPublication *publication = self->publication;
    if (tier >= JAZZ_DUR_GLOBAL && !settled && publication->opened && !publication->has_deferred) {
        JZ_TRY(jazz_db__db__SubscriptionPublicationSnapshot__capture(snapshot, index, out));
        *has = true;
        return JZ_OK;
    }
    *has = false;
    return JZ_OK;
}

static bool channel_send(jz_chan *chan, jazz_db_SubscriptionEvent *event) {
    if (!chan || jz_chan_is_closed(chan)) { jazz_db__db__SubscriptionEvent__drop(event); return false; }
    jz_chan_send(chan, event);
    return true;
}

jz_err jazz_db__db__SubscriptionSender__publish(const jazz_db_SubscriptionSender *self, jazz_db_SubscriptionEvent event, bool has_before,
                                               jazz_db_SubscriptionPublicationSnapshot before, const jazz_node_RelationSnapshot *snapshot,
                                               const jazz_db_RelationSnapshotIndex *index, bool materialized, bool *out) {
#define DROP_BEFORE() do { if (has_before) { jazz_db__db__SubscriptionPublicationSnapshot__drop(&before); has_before = false; } } while (0)
    if (event.tag != JAZZ_SE_DELTA) {
        DROP_BEFORE();
        *out = jazz_db__db__SubscriptionSender__unbounded_send(self, event);
        return JZ_OK;
    }
    bool reset = event.reset, publishable = event.publishable, settled = event.settled;
    u32 tier = event.tier;
    if (!publishable && !event.reset && event.added.len == 0 && event.updated.len == 0 && event.removed.len == 0 && event.terminal_operations.len == 0) {
        DROP_BEFORE();
        jazz_db__db__SubscriptionEvent__drop(&event);
        *out = false;
        return JZ_OK;
    }
    if (settled && !materialized) {
        DROP_BEFORE();
        jazz_db__db__SubscriptionEvent__drop(&event);
        return jz_err_new(JAZZ_EC_PROTOCOL, "settled subscription retained unresolved required cells");
    }
    jazz_db_SubscriptionPublication *publication = self->publication;
    if (!publishable || !materialized || (self->requested_tier >= JAZZ_DUR_GLOBAL && !settled)) {
        /* A strict remote window withholds its unsettled opening here. */
        if (publishable && materialized && !publication->opened && publication->has_opening_gate)
            publication->opening_gate.withheld = true;
        if (publication->opened && !publication->has_deferred && self->requested_tier >= JAZZ_DUR_GLOBAL) {
            if (!has_before) {
                jazz_db__db__SubscriptionEvent__drop(&event);
                return jz_err_new(JAZZ_EC_PROTOCOL, "withheld subscription change has no publication baseline");
            }
            publication->deferred = before;
            publication->has_deferred = true;
            has_before = false;
        }
        publication->reset |= reset || self->requested_tier < JAZZ_DUR_GLOBAL;
        DROP_BEFORE();
        jazz_db__db__SubscriptionEvent__drop(&event);
        *out = false;
        return JZ_OK;
    }
    DROP_BEFORE();
    if (publication->has_opening_gate) {
        if (!publication->opened && jazz_db__db__first_load__OpeningGate__awaits_answer(&publication->opening_gate, settled)) {
            publication->opening_gate.withheld = true;
            jazz_db__db__SubscriptionEvent__drop(&event);
            *out = false;
            return JZ_OK;
        }
        publication->has_opening_gate = false;
    }
    if (!publication->opened || publication->reset || (reset && publication->has_deferred)) {
        jazz_db_SubscriptionPublicationSnapshot current;
        jz_err e = jazz_db__db__SubscriptionPublicationSnapshot__capture(snapshot, index, &current);
        if (JZ_IS_ERR(e)) { jazz_db__db__SubscriptionEvent__drop(&event); return e; }
        jazz_db_OutputRowVec added = {0};
        e = jazz_db__db__subscription_outputs_with_occurrence_sidecar(&current.snapshot, current.occurrences.ptr, current.occurrences.len, &added);
        jazz_db__db__SubscriptionPublicationSnapshot__drop(&current);
        jazz_db__db__SubscriptionEvent__drop(&event);
        if (JZ_IS_ERR(e)) return e;
        event = jazz_db__db__SubscriptionEvent__delta(true, tier, settled);
        event.added = added;
    } else if (publication->has_deferred) {
        jazz_db_SubscriptionPublicationSnapshot current;
        jz_err e = jazz_db__db__SubscriptionPublicationSnapshot__capture(snapshot, index, &current);
        if (JZ_IS_ERR(e)) { jazz_db__db__SubscriptionEvent__drop(&event); return e; }
        jazz_db_SubscriptionEvent next;
        const jazz_db_SubscriptionPublicationSnapshot *previous = &publication->deferred;
        e = jazz_db__db__subscription_terminal_delta_event(tier, settled, &previous->snapshot, previous->occurrences.ptr, previous->occurrences.len,
                                                           &current.snapshot, current.occurrences.ptr, current.occurrences.len, &next);
        jazz_db__db__SubscriptionPublicationSnapshot__drop(&current);
        jazz_db__db__SubscriptionEvent__drop(&event);
        if (JZ_IS_ERR(e)) return e;
        event = next;
    }
    publication->opened = true;
    if (publication->has_deferred) { jazz_db__db__SubscriptionPublicationSnapshot__drop(&publication->deferred); publication->has_deferred = false; }
    publication->reset = false;
    *out = channel_send(self->sender, &event);
    return JZ_OK;
#undef DROP_BEFORE
}

bool jazz_db__db__SubscriptionSender__unbounded_send(const jazz_db_SubscriptionSender *self, jazz_db_SubscriptionEvent event) {
    bool terminal = event.tag == JAZZ_SE_CLOSED
        || (event.tag == JAZZ_SE_REJECTED && event.reason.tag != JAZZ_SRR_SHAPE_REGISTRATION_PENDING_CATALOGUE_ADMISSION);
    jazz_db_SubscriptionPublication *publication = self->publication;
    if (terminal) {
        if (publication->has_deferred) { jazz_db__db__SubscriptionPublicationSnapshot__drop(&publication->deferred); publication->has_deferred = false; }
        /* A terminal event ends any first-load wait. */
        publication->has_opening_gate = false;
    } else if (event.tag == JAZZ_SE_DELTA) {
        /* Receipt-only transitions bypass `publish`; before a gated stream has opened there is no published view. */
        if (publication->has_opening_gate && !publication->opened) {
            jazz_db__db__SubscriptionEvent__drop(&event);
            return true;
        }
    }
    return channel_send(self->sender, &event);
}

/* ---- CleanupGuard ---- */
void jazz_db__db__CleanupGuard__new(jazz_db_CleanupGuard *out, void (*call)(void *ctx), void *ctx) { out->has = true; out->call = call; out->ctx = ctx; }
void jazz_db__db__CleanupGuard__take(jazz_db_CleanupGuard *g, void (**call)(void *ctx), void **ctx) {
    if (!g->has) jz_panic("cleanup guard must only be disarmed once");
    g->has = false;
    *call = g->call;
    *ctx = g->ctx;
}
void jazz_db__db__CleanupGuard__Drop__drop(jazz_db_CleanupGuard *g) {
    if (g->has) { g->has = false; g->call(g->ctx); }
}

/* ---- SubscriptionStream ---- */
typedef struct FinalizationCompletion { bool has_drain; jazz_db_LocalFuture drain; jz_chan *receiver; } FinalizationCompletion;
static bool finalization_completion_poll(void *p, const jz_waker *cx, void *out) {
    FinalizationCompletion *f = p;
    jz_err *result = out;
    if (f->has_drain) {
        jz_err drained = JZ_OK;
        if (!f->drain.poll(f->drain.self, cx, &drained)) return false;
        jazz_db__db__LocalFuture__drop(&f->drain);
        f->has_drain = false;
        if (JZ_IS_ERR(drained)) { *result = drained; return true; }
    }
    u8 ack;
    if (jz_chan_try_recv(f->receiver, &ack)) { *result = JZ_OK; return true; }
    if (jz_chan_is_closed(f->receiver)) {
        *result = jz_err_new(JAZZ_EC_PROTOCOL, "subscription finalization acknowledgement was dropped");
        return true;
    }
    return false;
}
static void finalization_completion_drop(void *p) {
    FinalizationCompletion *f = p;
    if (f->has_drain) jazz_db__db__LocalFuture__drop(&f->drain);
    jz_chan_release(f->receiver);
    jz_free(f);
}

static jz_err jazz_db__db__SubscriptionStream__close_own(jazz_db_SubscriptionStream *s) {
    if (s->finalization_tag == JAZZ_SF_NONE) {
        if (!s->has_cleanup) return JZ_OK;
        jazz_db_SubscriptionCleanup cleanup = s->cleanup;
        s->has_cleanup = false;
        /* `close` is a terminal stream operation. Do this before awaiting. */
        s->terminated = true;
        jazz_db_SubscriptionPublication *publication = s->state->sender.publication;
        if (publication->has_deferred) { jazz_db__db__SubscriptionPublicationSnapshot__drop(&publication->deferred); publication->has_deferred = false; }
        jz_chan_close(s->receiver);
        jz_chan *ack = jz_chan_new(sizeof(u8)); /* oneshot::channel() */
        FinalizationCompletion *f = JZ_NEW(FinalizationCompletion);
        f->receiver = ack;
        f->has_drain = cleanup.call(cleanup.ctx, ack, &f->drain);
        s->finalization_tag = JAZZ_SF_PENDING;
        s->finalization_pending = (jazz_db_LocalFuture){ f, finalization_completion_poll, finalization_completion_drop };
    }
    if (s->finalization_tag == JAZZ_SF_FAILED) {
        jz_err e = jz_err_new(s->failed_code, "");
        jz_str_drop(e.msg);
        *e.msg = jz_str_clone(&s->failed_message);
        return e;
    }
    if (s->finalization_tag != JAZZ_SF_PENDING) jz_panic("close completion must exist after finalization starts");
    jz_err result = JZ_OK;
    jazz_db__db__LocalFuture__block_on(&s->finalization_pending, &result);
    jazz_db__db__LocalFuture__drop(&s->finalization_pending);
    if (!JZ_IS_ERR(result)) {
        s->finalization_tag = JAZZ_SF_NONE;
        return JZ_OK;
    }
    s->finalization_tag = JAZZ_SF_FAILED;
    s->failed_code = result.code;
    s->failed_message = result.msg ? jz_str_clone(result.msg) : jz_str_from("");
    return result;
}

/* Queue cancellation, drive finalization and wait until retired. */
jz_err jazz_db__db__SubscriptionStream__close(jazz_db_SubscriptionStream *s) {
    JZ_TRY(jazz_db__db__SubscriptionStream__close_own(s));
    if (s->window_fallback) JZ_TRY(jazz_db__db__SubscriptionStream__close(s->window_fallback));
    return JZ_OK;
}

bool jazz_db__db__SubscriptionStream__is_remote_window(const jazz_db_SubscriptionStream *s) {
    return s->state->sender.publication->remote_window;
}

static void stream_box_drop(jazz_db_SubscriptionStream *s) {
    if (!s) return;
    jazz_db__db__SubscriptionStream__Drop__drop(s);
    jz_free(s);
}

static void jazz_db__db__SubscriptionStream__sync_window_fallback(jazz_db_SubscriptionStream *s) {
    if (s->serving_fallback || !s->window_fallback) return;
    const jazz_db_SubscriptionPublication *publication = s->state->sender.publication;
    bool fell_back = publication->window_fell_back;
    bool decided = publication->opened || !publication->has_opening_gate;
    if (fell_back) {
        s->serving_fallback = true;
    } else if (decided) {
        stream_box_drop(s->window_fallback);
        s->window_fallback = NULL;
    }
}

/* Poll the receiver: Ready(Some) -> 1, Ready(None) -> 0, Pending -> -1 */
static int receiver_poll(jz_chan *receiver, jazz_db_SubscriptionEvent *out) {
    if (jz_chan_try_recv(receiver, out)) return 1;
    return jz_chan_is_closed(receiver) ? 0 : -1;
}

static bool jazz_db__db__SubscriptionStream__poll_window_fallback(jazz_db_SubscriptionStream *s, const jz_waker *cx, bool *has, jazz_db_SubscriptionEvent *out) {
    for (;;) {
        jazz_db_SubscriptionEvent event;
        int r = receiver_poll(s->receiver, &event);
        if (r <= 0) break;
        if (event.tag == JAZZ_SE_DELTA && event.reset && event.publishable) {
            s->serving_fallback = false;
            stream_box_drop(s->window_fallback);
            s->window_fallback = NULL;
            *has = true;
            *out = event;
            return true;
        }
        if (event.tag == JAZZ_SE_DELTA) { jazz_db__db__SubscriptionEvent__drop(&event); continue; }
        if (event.tag == JAZZ_SE_REJECTED) { *has = true; *out = event; return true; }
        /* A closed window leaves the fallback serving. */
        break;
    }
    if (s->window_fallback) return jazz_db__db__SubscriptionStream__Stream__poll_next(s->window_fallback, cx, has, out);
    *has = false;
    return true;
}

/* Return the receiver-local maintained snapshot after a settled event. */
jz_err jazz_db__db__SubscriptionStream__settled_receiver_local_snapshot(const jazz_db_SubscriptionStream *s, jazz_node_RelationSnapshot *out) {
    const jazz_db_SubscriptionState *state = s->state;
    if (!state->settled)
        return jz_err_new(JAZZ_EC_PROTOCOL, "remote one-shot attempted to materialize before subscription settlement");
    if (state->snapshot_source != JAZZ_SSS_LOCAL_MAINTAINED)
        return jz_err_new(JAZZ_EC_PROTOCOL, "remote one-shot attempted to materialize a non-local maintained snapshot");
    return jazz_db__db__materialized_subscription_snapshot(&state->snapshot, &state->snapshot_index, out);
}

/* Return the next queued materialized subscription event without waiting. */
bool jazz_db__db__SubscriptionStream__try_next_event(jazz_db_SubscriptionStream *s, jazz_db_SubscriptionEvent *out) {
    if (s->terminated) return false;
    for (;;) {
        jazz_db__db__SubscriptionStream__sync_window_fallback(s);
        if (s->serving_fallback) {
            bool has = false;
            if (!jazz_db__db__SubscriptionStream__poll_window_fallback(s, &noop_waker, &has, out)) return false;
            return has;
        }
        jazz_db_SubscriptionEvent event;
        if (!jz_chan_try_recv(s->receiver, &event)) return false;
        if (jazz_db__db__subscription_event_is_publishable(&event)) { *out = event; return true; }
        jazz_db__db__SubscriptionEvent__drop(&event);
    }
}

bool jazz_db__db__subscription_event_is_publishable(const jazz_db_SubscriptionEvent *e) {
    return !(e->tag == JAZZ_SE_DELTA && !e->publishable);
}

bool jazz_db__db__SubscriptionStream__Stream__poll_next(jazz_db_SubscriptionStream *s, const jz_waker *cx, bool *has, jazz_db_SubscriptionEvent *out) {
    if (s->terminated) { *has = false; return true; }
    for (;;) {
        jazz_db__db__SubscriptionStream__sync_window_fallback(s);
        if (s->serving_fallback) return jazz_db__db__SubscriptionStream__poll_window_fallback(s, cx, has, out);
        jazz_db_SubscriptionEvent event;
        int r = receiver_poll(s->receiver, &event);
        if (r == 1) {
            if (jazz_db__db__subscription_event_is_publishable(&event)) { *has = true; *out = event; return true; }
            jazz_db__db__SubscriptionEvent__drop(&event);
            continue;
        }
        if (r == 0) { *has = false; return true; }
        return false;
    }
}

/* Await the next materialized subscription event. */
bool jazz_db__db__SubscriptionStream__next_event(jazz_db_SubscriptionStream *s, jazz_db_SubscriptionEvent *out) {
    bool has = false;
    while (!jazz_db__db__SubscriptionStream__Stream__poll_next(s, &noop_waker, &has, out)) jz_task_run_ready();
    return has;
}

static void subscription_state_rc_drop(void *p);
void jazz_db__db__SubscriptionStream__Drop__drop(jazz_db_SubscriptionStream *s) {
    if (s->has_cleanup) {
        s->has_cleanup = false;
        jazz_db_LocalFuture f;
        if (s->cleanup.call(s->cleanup.ctx, NULL, &f)) jazz_db__db__LocalFuture__drop(&f);
    }
    if (s->finalization_tag == JAZZ_SF_PENDING) jazz_db__db__LocalFuture__drop(&s->finalization_pending);
    if (s->finalization_tag == JAZZ_SF_FAILED) jz_str_drop(&s->failed_message);
    s->finalization_tag = JAZZ_SF_NONE;
    jz_chan_release(s->receiver);
    s->receiver = NULL;
    jz_rc_release(s->state);
    s->state = NULL;
    stream_box_drop(s->window_fallback);
    s->window_fallback = NULL;
}

/* ---- PreparedQuery ---- */
void jazz_db__db__PreparedQuery__clone(jazz_db_PreparedQuery *dst, const jazz_db_PreparedQuery *src) {
    *dst = *src;
    if (src->has_request_identity_claims) dst->request_identity_claims = jazz_db__db__PolicyBinding__clone(&src->request_identity_claims);
    dst->shape = jazz_model__query__ValidatedQuery__clone(src->shape);
    dst->binding = jazz_model__query__Binding__clone(src->binding);
    dst->local_plan = src->local_plan ? jazz_node__node__PreparedQueryPlanHandle__clone(src->local_plan) : NULL;
    dst->global_plan = src->global_plan ? jazz_node__node__PreparedQueryPlanHandle__clone(src->global_plan) : NULL;
}
void jazz_db__db__PreparedQuery__drop(jazz_db_PreparedQuery *q) {
    if (q->has_request_identity_claims) jazz_db__db__PolicyBinding__drop(&q->request_identity_claims);
    jazz_model__query__ValidatedQuery__drop(q->shape);
    jazz_model__query__Binding__drop(q->binding);
    if (q->local_plan) jazz_node__node__PreparedQueryPlanHandle__drop(q->local_plan);
    if (q->global_plan) jazz_node__node__PreparedQueryPlanHandle__drop(q->global_plan);
}
bool jazz_db__db__PreparedQuery__request_identity(const jazz_db_PreparedQuery *q, jazz_types_AuthorSubject *out) {
    if (!q->has_request_identity_claims) return false;
    *out = q->request_identity_claims.author;
    return true;
}
/* Capture the admitted claims for this trusted-serving request. */
void jazz_db__db__PreparedQuery__with_identity_claims(jazz_db_PreparedQuery *self, jazz_types_AuthorSubject author, jz_btree claims) {
    if (self->has_request_identity_claims) jazz_db__db__PolicyBinding__drop(&self->request_identity_claims);
    self->has_request_identity_claims = true;
    self->request_identity_claims.author = author;
    self->request_identity_claims.claims = claims;
    /* Plans prepared before admission may embed SYSTEM or another scope. */
    if (self->local_plan) { jazz_node__node__PreparedQueryPlanHandle__drop(self->local_plan); self->local_plan = NULL; }
    if (self->global_plan) { jazz_node__node__PreparedQueryPlanHandle__drop(self->global_plan); self->global_plan = NULL; }
}
jz_err jazz_db__db__PreparedQuery__request_policy_binding(const jazz_db_PreparedQuery *q, jazz_types_AuthorSubject author, bool *has, jazz_db_PolicyBinding *out) {
    if (q->has_request_identity_claims && !jazz_types_AuthorSubject_eq(&q->request_identity_claims.author, &author))
        return jz_err_new(JAZZ_EC_PROTOCOL, "prepared request identity does not match read identity");
    *has = q->has_request_identity_claims;
    if (*has && out) *out = jazz_db__db__PolicyBinding__clone(&q->request_identity_claims);
    return JZ_OK;
}
jz_err jazz_db__db__PreparedQuery__scoped_node(const jazz_db_PreparedQuery *q, jazz_node_NodeState *node, jazz_types_AuthorSubject author, jazz_node_ActiveSessionClaimsScope **out) {
    bool has = false;
    JZ_TRY(jazz_db__db__PreparedQuery__request_policy_binding(q, author, &has, NULL));
    jz_btree claims;
    jz_btree *claims_opt = NULL;
    if (q->has_request_identity_claims) { jazz_db__db__RowCells__clone(&claims, &q->request_identity_claims.claims); claims_opt = &claims; }
    *out = jazz_node__node__NodeState__scoped_optional_session_claims(node, author, claims_opt);
    return JZ_OK;
}
const jazz_model_ValidatedQuery *jazz_db__db__PreparedQuery__shape(const jazz_db_PreparedQuery *q) { return q->shape; }
const jazz_model_Binding *jazz_db__db__PreparedQuery__binding(const jazz_db_PreparedQuery *q) { return q->binding; }
jazz_node_PreparedQueryPlanHandle *jazz_db__db__PreparedQuery__plan_for_tier(const jazz_db_PreparedQuery *q, u32 tier, u64 groove_runtime_token) {
    if (q->groove_runtime_token != groove_runtime_token) return NULL;
    switch (tier) {
    case JAZZ_DUR_LOCAL: return q->local_plan;
    case JAZZ_DUR_GLOBAL: return q->global_plan;
    default: return NULL;
    }
}
bool jazz_db__db__should_install_prepared_plan(const jazz_model_ValidatedQuery *shape) {
    const jazz_model_Query *q = jazz_model__query__ValidatedQuery__query(shape);
    return jazz_model__query__Query__joins_len(q) != 0 || jazz_model__query__Query__reachable_len(q) != 0;
}

/* ======================= db.rs part 3c: subscription snapshot algebra (db.rs:5970-7166) ======================= */
static void handle_vec_drop(jazz_db_UpstreamCoverageHandleVec *v) { jz_vec_drop(v, sizeof(jazz_db_UpstreamCoverageHandle), jazz_db__db__UpstreamCoverageHandle__drop); }
void jazz_db__db__UpstreamCoverageHandle__clone(void *dst, const void *src) {
    jazz_db_UpstreamCoverageHandle *d = dst;
    const jazz_db_UpstreamCoverageHandle *s = src;
    jazz_protocol__protocol__CoverageKey__clone(&d->coverage, &s->coverage);
    d->subscription = s->subscription;
}
void jazz_db__db__UpstreamCoverageHandle__drop(void *p) { jazz_protocol__protocol__CoverageKey__drop(&((jazz_db_UpstreamCoverageHandle *)p)->coverage); }

void jazz_db__db__PendingUpstreamSubscription__clone(void *dst, const void *src) {
    jazz_db_PendingUpstreamSubscription *d = dst;
    const jazz_db_PendingUpstreamSubscription *s = src;
    *d = *s;
    d->shape = jazz_model__query__ValidatedQuery__clone(s->shape);
    d->binding = jazz_model__query__Binding__clone(s->binding);
    jazz_protocol__protocol__RegisterShapeOptions__clone(&d->opts, &s->opts);
    if (s->has_policy_binding) d->policy_binding = jazz_db__db__PolicyBinding__clone(&s->policy_binding);
}
void jazz_db__db__PendingUpstreamSubscription__drop(void *p) {
    jazz_db_PendingUpstreamSubscription *s = p;
    jazz_model__query__ValidatedQuery__drop(s->shape);
    jazz_model__query__Binding__drop(s->binding);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&s->opts);
    if (s->has_policy_binding) jazz_db__db__PolicyBinding__drop(&s->policy_binding);
}
void jazz_db__db__PendingUpstreamCommand__drop(void *p) {
    jazz_db_PendingUpstreamCommand *c = p;
    if (c->tag == JAZZ_PUC_SUBSCRIBE) jazz_db__db__PendingUpstreamSubscription__drop(&c->subscribe);
    else if (c->tag == JAZZ_PUC_AUTHORIZATION_SCOPE_INTENT) {
        jazz_protocol__protocol__PermissionAdviceAction__drop(&c->action);
        if (c->has_session_claim_binding) jazz_db__db__PolicyBinding__drop(&c->session_claim_binding);
        if (c->has_delegated_session) jazz_protocol__protocol__DelegatedSessionBinding__drop(&c->delegated_session);
    }
}

/* SubscriptionState drop (RefCell payload) */
void jazz_db__db__SubscriptionState__drop(jazz_db_SubscriptionState *s) {
    jz_rc_release(s->closed);
    jazz_db__db__DeletionReconciliation__drop(&s->deletion_reconciliation);
    jazz_db__db__ScalarReconciliation__drop(&s->scalar_reconciliation);
    jazz_model__query__ValidatedQuery__drop(s->kind.shape);
    jazz_model__query__Binding__drop(s->kind.binding);
    if (s->kind.maintained_subscription) jazz_node__node__LocalMaintainedViewSubscription__drop(s->kind.maintained_subscription);
    jz_rc_release(s->local_subscription_cleanup);
    handle_vec_drop(&s->upstream_subscription_handles);
    if (s->has_request_identity_claims) jazz_db__db__PolicyBinding__drop(&s->request_identity_claims);
    jazz_protocol__protocol__ReadViewSpec__drop(&s->read_view);
    jazz_node__node__RelationSnapshot__drop(&s->snapshot);
    jazz_db__db__RelationSnapshotIndex__drop(&s->snapshot_index);
    handle_vec_drop(&s->authority_witness);
    jazz_db__db__SubscriptionSender__drop(&s->sender);
}
static void subscription_state_rc_drop(void *p) { jazz_db__db__SubscriptionState__drop(p); }

jazz_db_SubscriptionEvent jazz_db__db__subscription_delta_event(u32 tier, bool settled, const jazz_node_RelationSnapshot *previous, const jazz_node_RelationSnapshot *current, bool terminal_rows) {
    return jazz_db__db__subscription_delta_event_with_reset(tier, settled, previous, current, false, terminal_rows);
}

/* Retire authority settlement without disturbing the receiver-local snapshot. */
void jazz_db__db__demote_authority_receipt_subscriptions(jazz_db_SubscriptionList subscriptions, const jz_btree *publishing_subscriptions) {
    jazz_db_WeakStateVec retained = {0};
    JZ_VEC_FOR(jazz_db_SubscriptionState *, weak, subscriptions->v) {
        jazz_db_SubscriptionState *state = jazz_db_weak_state_upgrade(*weak);
        if (!state) continue;
        if (state->propagates_upstream) {
            state->requires_authority_receipt = true;
            if (state->settled) {
                state->settled = false;
                bool frame_will_publish = false;
                JZ_VEC_FOR(const jazz_db_UpstreamCoverageHandle, handle, state->upstream_subscription_handles) {
                    if (jz_btree_contains(publishing_subscriptions, &handle->subscription)) { frame_will_publish = true; break; }
                }
                if (!frame_will_publish && state->read_tier < JAZZ_DUR_GLOBAL) {
                    jazz_db_SubscriptionEvent event = jazz_db__db__subscription_delta_event(state->read_tier, false, &state->snapshot, &state->snapshot, state->terminal_rows);
                    (void)jazz_db__db__SubscriptionSender__unbounded_send(&state->sender, event);
                }
            }
        }
        JZ_VEC_PUSH(retained, state); /* Rc::downgrade */
        jz_rc_release(state);
    }
    jz_free(subscriptions->v.ptr);
    subscriptions->v = *(__typeof__(subscriptions->v) *)&retained;
}

static int occ_cmp(const void *a, const void *b) { return jazz_types__object__OutputOccurrenceId__cmp(a, b, NULL); }
typedef struct IndexedRow { u32 index; const jazz_node_CurrentRow *row; } IndexedRow;
static const jz_btree_desc occ_index_desc = { sizeof(jazz_types_OutputOccurrenceId), sizeof(u32), jazz_types__object__OutputOccurrenceId__cmp, jazz_types__object__OutputOccurrenceId__drop, NULL };
static const jz_btree_desc occ_indexed_row_desc = { sizeof(jazz_types_OutputOccurrenceId), sizeof(IndexedRow), jazz_types__object__OutputOccurrenceId__cmp, jazz_types__object__OutputOccurrenceId__drop, NULL };

static void push_output_row(jazz_db_OutputRowVec *v, const jazz_types_OutputOccurrenceId *occ, const jazz_node_CurrentRow *row, bool has_prev, u32 prev, u32 index) {
    jazz_db_SubscriptionOutputRow r;
    jazz_types__object__OutputOccurrenceId__clone(&r.occurrence_id, occ);
    jazz_node__node__CurrentRow__clone(&r.row, row);
    r.has_previous_index = has_prev;
    r.previous_index = prev;
    r.index = index;
    JZ_VEC_PUSH(*v, r);
}
static void push_removed_row(jazz_db_RemovedRowVec *v, const jazz_node_CurrentRow *row, const jazz_types_OutputOccurrenceId *occ, u32 index) {
    jazz_db_RemovedRow r;
    r.table = jz_str_from_slice(jazz_node__node__CurrentRow__table(row));
    r.row_uuid = jazz_node__node__CurrentRow__row_uuid(row);
    jazz_types__object__OutputOccurrenceId__clone(&r.occurrence_id, occ);
    r.index = index;
    JZ_VEC_PUSH(*v, r);
}

/* Publishes an ordered terminal as explicit root placements. */
jz_err jazz_db__db__subscription_terminal_delta_event(u32 tier, bool settled, const jazz_node_RelationSnapshot *previous, const jazz_types_OutputOccurrenceId *previous_occurrences, u32 n_prev,
                                                     const jazz_node_RelationSnapshot *current, const jazz_types_OutputOccurrenceId *current_occurrences, u32 n_cur, jazz_db_SubscriptionEvent *out) {
    const jazz_node_CurrentRow *previous_roots = previous->rows.ptr;
    const jazz_node_CurrentRow *current_roots = current->rows.ptr;
    u32 prev_len = previous->root_count, cur_len = current->root_count;
    if (prev_len > previous->rows.len || cur_len > current->rows.len) jz_panic("range end index out of range for slice");
    if (prev_len != n_prev || cur_len != n_cur)
        return jz_err_new(JAZZ_EC_PROTOCOL, "maintained terminal occurrence sidecar length does not match root rows");
    u32 common_prefix = 0;
    while (common_prefix < prev_len && common_prefix < cur_len
           && jazz_types__object__OutputOccurrenceId__eq(&previous_occurrences[common_prefix], &current_occurrences[common_prefix]))
        common_prefix++;

    jazz_db_SubscriptionEvent event = jazz_db__db__SubscriptionEvent__delta(false, tier, settled);
    for (u32 index = 0; index < common_prefix; index++) {
        if (!jazz_node__node__CurrentRow__subscription_equivalent(&previous_roots[index], &current_roots[index]))
            push_output_row(&event.updated, &current_occurrences[index], &current_roots[index], true, index, index);
    }
    jz_btree previous_suffix_positions;
    jz_btree_init(&previous_suffix_positions, &occ_index_desc);
    for (u32 offset = 0; common_prefix + offset < n_prev; offset++) {
        jazz_types_OutputOccurrenceId k;
        jazz_types__object__OutputOccurrenceId__clone(&k, &previous_occurrences[common_prefix + offset]);
        u32 pos = common_prefix + offset;
        jz_btree_insert(&previous_suffix_positions, &k, &pos, NULL);
    }
    jz_btree current_suffix;
    jz_btree_init(&current_suffix, &jazz_db_occurrence_set_desc);
    for (u32 i = common_prefix; i < n_cur; i++) {
        jazz_types_OutputOccurrenceId k;
        jazz_types__object__OutputOccurrenceId__clone(&k, &current_occurrences[i]);
        jz_btree_insert(&current_suffix, &k, NULL, NULL);
    }
    for (u32 i = common_prefix; i < prev_len; i++) {
        if (!jz_btree_contains(&current_suffix, &previous_occurrences[i]))
            push_removed_row(&event.removed, &previous_roots[i], &previous_occurrences[i], i);
    }
    for (u32 index = common_prefix; index < cur_len; index++) {
        const u32 *previous_index = jz_btree_get(&previous_suffix_positions, &current_occurrences[index]);
        if (previous_index) push_output_row(&event.updated, &current_occurrences[index], &current_roots[index], true, *previous_index, index);
        else push_output_row(&event.added, &current_occurrences[index], &current_roots[index], false, 0, index);
    }
    jz_btree_drop(&previous_suffix_positions);
    jz_btree_drop(&current_suffix);
    *out = event;
    return JZ_OK;
}

jazz_db_SubscriptionEvent jazz_db__db__subscription_delta_event_with_reset(u32 tier, bool settled, const jazz_node_RelationSnapshot *previous, const jazz_node_RelationSnapshot *current, bool reset, bool terminal_rows) {
    (void)terminal_rows;
    /* A reset is a complete ordered snapshot. */
    if (reset) {
        jazz_db_SubscriptionEvent e = jazz_db__db__SubscriptionEvent__delta(true, tier, settled);
        for (u32 index = 0; index < current->rows.len; index++) {
            jazz_node_CurrentRow row;
            jazz_node__node__CurrentRow__clone(&row, &current->rows.ptr[index]);
            jazz_db_SubscriptionOutputRow out;
            jazz_db__db__subscription_output_row(row, false, 0, index, &out);
            JZ_VEC_PUSH(e.added, out);
        }
        return e;
    }
    jz_btree previous_by_id, current_by_id;
    jz_btree_init(&previous_by_id, &occ_indexed_row_desc);
    jz_btree_init(&current_by_id, &occ_indexed_row_desc);
    for (u32 index = 0; index < previous->rows.len; index++) {
        jazz_types_OutputOccurrenceId k;
        jazz_db__db__subscription_row_occurrence_id(&previous->rows.ptr[index], &k);
        IndexedRow v = { index, &previous->rows.ptr[index] };
        jz_btree_insert(&previous_by_id, &k, &v, NULL);
    }
    for (u32 index = 0; index < current->rows.len; index++) {
        jazz_types_OutputOccurrenceId k;
        jazz_db__db__subscription_row_occurrence_id(&current->rows.ptr[index], &k);
        IndexedRow v = { index, &current->rows.ptr[index] };
        jz_btree_insert(&current_by_id, &k, &v, NULL);
    }
    jazz_db_SubscriptionEvent e = jazz_db__db__SubscriptionEvent__delta(reset, tier, settled);
    jazz_types_OutputOccurrenceId *key;
    IndexedRow *entry;
    JZ_BTREE_FOR(&current_by_id, key, entry) {
        const IndexedRow *prev = jz_btree_get(&previous_by_id, key);
        if (!prev) {
            jazz_node_CurrentRow row;
            jazz_node__node__CurrentRow__clone(&row, entry->row);
            jazz_db_SubscriptionOutputRow out;
            jazz_db__db__subscription_output_row(row, false, 0, entry->index, &out);
            JZ_VEC_PUSH(e.added, out);
        } else if (!jazz_node__node__CurrentRow__subscription_equivalent(prev->row, entry->row) || prev->index != entry->index) {
            jazz_node_CurrentRow row;
            jazz_node__node__CurrentRow__clone(&row, entry->row);
            jazz_db_SubscriptionOutputRow out;
            jazz_db__db__subscription_output_row(row, true, prev->index, entry->index, &out);
            JZ_VEC_PUSH(e.updated, out);
        }
    }
    jazz_types_OutputOccurrenceId *pkey;
    IndexedRow *pentry;
    JZ_BTREE_FOR(&previous_by_id, pkey, pentry) {
        if (!jz_btree_contains(&current_by_id, pkey)) push_removed_row(&e.removed, pentry->row, pkey, pentry->index);
    }
    jz_btree_drop(&previous_by_id);
    jz_btree_drop(&current_by_id);
    return e;
}

static void rows_remove(jazz_node_RelationSnapshot *s, u32 index, jazz_node_CurrentRow *out) {
    jazz_node_CurrentRow tmp;
    jz_vec_remove(&s->rows, index, out ? out : &tmp, sizeof(jazz_node_CurrentRow));
    if (!out) jazz_node__node__CurrentRow__drop(&tmp);
}
static void rows_insert(jazz_node_RelationSnapshot *s, u32 index, jazz_node_CurrentRow row) { jz_vec_insert(&s->rows, index, &row, sizeof row); }

static jz_err jazz_db__db__apply_maintained_membership_update_to_snapshot(jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *snapshot_index,
                                                                         void *update_added_p, jazz_db_OccurrenceVec *update_removed,
                                                                         u32 tier, bool settled, jazz_db_SubscriptionEvent *out);
static jz_err jazz_db__db__apply_terminal_operations_to_subscription_snapshot(jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *snapshot_index,
                                                                             jazz_db_TerminalOperationVec operations, const jz_btree *occurrence_overrides,
                                                                             const jazz_node_TerminalRootLayout *layout, jz_slice table, u32 tier, bool settled,
                                                                             jazz_db_SubscriptionEvent *out);
static void occurrence_row_drop(void *p) {
    jazz_db_OccurrenceRow *r = p;
    jazz_types__object__OutputOccurrenceId__drop(&r->occurrence);
    jazz_node__node__CurrentRow__drop(&r->row);
}
static void bytes_key_drop(void *p) { jz_bytes_drop(p); }
static const jz_btree_desc occurrence_overrides_desc = { sizeof(jz_bytes), sizeof(jazz_types_OutputOccurrenceId), jz_cmp_bytes, bytes_key_drop, jazz_types__object__OutputOccurrenceId__drop };

jz_err jazz_db__db__apply_maintained_update_to_snapshot(jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *snapshot_index, jazz_node_LocalMaintainedViewSubscriptionUpdate update,
                                                       jz_slice table, u32 tier, bool settled, const jazz_node_TerminalRootLayout *terminal_layout, jazz_db_SubscriptionEvent *out) {
    if (jazz_types__debug_env__covered_input_trace()) {
        jz_str update_kind = {0};
        switch (update.tag) {
        case JAZZ_LMU_STRUCTURED: update_kind = jz_format("structured:%u", update.terminal_operations.len); break;
        case JAZZ_LMU_FLAT: update_kind = jz_format("flat:add=%u remove=%u", update.added.len, update.removed.len); break;
        default: update_kind = jz_format("ordered-window:roots=%u occurrences=%u", update.snapshot.root_count, update.occurrence_ids.len); break;
        }
        jz_trace_event(0, "stderr", "JAZZ_COVERED_INPUT_TRACE stage=apply_maintained_snapshot roots=%u update=%.*s",
                       snapshot->root_count, (int)update_kind.len, update_kind.ptr);
        jz_str_drop(&update_kind);
    }
    switch (update.tag) {
    case JAZZ_LMU_ORDERED_WINDOW: {
        /* Validate the entire replacement facade before touching the published snapshot. */
        jazz_db_RelationSnapshotIndex current_index;
        jz_err e = jazz_db__db__relation_snapshot_index_with_root_occurrences(&update.snapshot, update.occurrence_ids.ptr, update.occurrence_ids.len, &current_index);
        if (JZ_IS_ERR(e)) goto drop_window;
        jazz_db_OccurrenceVec previous_occurrences = {0};
        e = jazz_db__db__snapshot_root_occurrences(snapshot, snapshot_index, &previous_occurrences);
        if (JZ_IS_ERR(e)) { jazz_db__db__RelationSnapshotIndex__drop(&current_index); goto drop_window; }
        e = jazz_db__db__subscription_terminal_delta_event(tier, settled, snapshot, previous_occurrences.ptr, previous_occurrences.len,
                                                           &update.snapshot, update.occurrence_ids.ptr, update.occurrence_ids.len, out);
        occurrence_vec_drop(&previous_occurrences);
        if (JZ_IS_ERR(e)) { jazz_db__db__RelationSnapshotIndex__drop(&current_index); goto drop_window; }
        jazz_node__node__RelationSnapshot__drop(snapshot);
        *snapshot = update.snapshot;
        jazz_db__db__RelationSnapshotIndex__drop(snapshot_index);
        *snapshot_index = current_index;
        occurrence_vec_drop(&update.occurrence_ids);
        return JZ_OK;
    drop_window:
        jazz_node__node__RelationSnapshot__drop(&update.snapshot);
        occurrence_vec_drop(&update.occurrence_ids);
        return e;
    }
    case JAZZ_LMU_FLAT: {
        jazz_db_SubscriptionEvent event;
        jz_err e = jazz_db__db__apply_maintained_membership_update_to_snapshot(snapshot, snapshot_index, (void *)&update.added, &update.removed, tier, settled, &event);
        if (JZ_IS_ERR(e)) return e;
        for (int pass = 0; pass < 2; pass++) {
            jazz_db_OutputRowVec *rows = pass == 0 ? &event.added : &event.updated;
            JZ_VEC_FOR(jazz_db_SubscriptionOutputRow, output, *rows) {
                u32 index;
                if (!jazz_db__positional_order__PositionalOrder__position(&snapshot_index->roots, &output->occurrence_id, &index)) continue;
                jazz_node__node__CurrentRow__drop(&output->row);
                jazz_node__node__CurrentRow__clone(&output->row, &snapshot->rows.ptr[index]);
                output->index = index;
            }
        }
        *out = event;
        return JZ_OK;
    }
    default: break;
    }
    /* Structured */
    if (update.terminal_operations.len == 0) {
        terminal_ops_drop(&update.terminal_operations);
        *out = jazz_db__db__SubscriptionEvent__delta(false, tier, settled);
        return JZ_OK;
    }
    if (!terminal_layout) {
        terminal_ops_drop(&update.terminal_operations);
        return jz_err_new(JAZZ_EC_PROTOCOL, "structured terminal operation arrived without a prepared root layout");
    }
    const jazz_node_TerminalRootLayout *layout = terminal_layout;
    jz_btree occurrence_overrides;
    jz_btree_init(&occurrence_overrides, &occurrence_overrides_desc);
    JZ_VEC_FOR(const groove_TerminalOperation, operation, update.terminal_operations) {
        if (operation->path.len != 0) continue;
        jazz_types_OutputOccurrenceId probe;
        jz_err e = jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(jz_bytes_slice(&operation->root_key), layout->root_union_arm, &probe);
        if (!JZ_IS_ERR(e)) { jazz_types__object__OutputOccurrenceId__drop(&probe); continue; }
        jz_err_drop(&e);
        bool root_bytes_ok = operation->root_key.len >= 17 && operation->root_key.ptr[0] == 10;
        /* Only a key hiding internal join identities reaches this scan. */
        jazz_db_OccurrenceVec candidates = {0};
        jazz_types_OutputOccurrenceId *candidate;
        u32 *slot;
        JZ_BTREE_FOR(&snapshot_index->roots.slots, candidate, slot) {
            jz_bytes cb = jazz_types__object__OutputOccurrenceId__canonical_bytes(candidate);
            bool matches = root_bytes_ok ? (cb.len >= 16 && memcmp(cb.ptr, operation->root_key.ptr + 1, 16) == 0) : cb.len < 16;
            jz_bytes_drop(&cb);
            if (matches) {
                jazz_types_OutputOccurrenceId c;
                jazz_types__object__OutputOccurrenceId__clone(&c, candidate);
                JZ_VEC_PUSH(candidates, c);
            }
        }
        if (candidates.len == 1) {
            jz_bytes k = jz_bytes_clone(&operation->root_key);
            jz_btree_insert(&occurrence_overrides, &k, &candidates.ptr[0], NULL);
            jz_free(candidates.ptr);
        } else if (candidates.len == 0) {
            jz_free(candidates.ptr);
            e = jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(jz_bytes_slice(&operation->root_key), layout->root_union_arm, &probe);
            if (JZ_IS_ERR(e)) { jz_btree_drop(&occurrence_overrides); terminal_ops_drop(&update.terminal_operations); return e; }
            jazz_types__object__OutputOccurrenceId__drop(&probe);
        } else {
            occurrence_vec_drop(&candidates);
            jz_btree_drop(&occurrence_overrides);
            terminal_ops_drop(&update.terminal_operations);
            return jz_err_new(JAZZ_EC_PROTOCOL, "terminal root key is ambiguous after hiding internal join identities");
        }
    }
    jz_err e = jazz_db__db__apply_terminal_operations_to_subscription_snapshot(snapshot, snapshot_index, update.terminal_operations, &occurrence_overrides, layout, table, tier, settled, out);
    jz_btree_drop(&occurrence_overrides);
    return e;
}

typedef JZ_VEC(jazz_db_OccurrenceRow) OccurrenceRowVec;
typedef struct OccPos { jazz_types_OutputOccurrenceId occ; u32 position; } OccPos;
static int occpos_cmp(const void *a, const void *b, void *ctx) { (void)ctx; u32 x = ((const OccPos *)a)->position, y = ((const OccPos *)b)->position; return x < y ? -1 : x > y; }
static int u32_cmp_plain(const void *a, const void *b, void *ctx) { return jz_cmp_u32(a, b, ctx); }

static jz_err jazz_db__db__apply_maintained_membership_update_to_snapshot(jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *snapshot_index,
                                                                         void *update_added_p, jazz_db_OccurrenceVec *update_removed,
                                                                         u32 tier, bool settled, jazz_db_SubscriptionEvent *out) {
    OccurrenceRowVec *update_added = (OccurrenceRowVec *)update_added_p;
    if (snapshot->rows.len == 0 && snapshot->edges.len == 0 && snapshot->root_count == 0 && update_removed->len == 0) {
        snapshot->root_count = update_added->len;
        jz_vec_reserve(&snapshot->rows, update_added->len, sizeof(jazz_node_CurrentRow));
        jazz_db_OccurrenceVec occ = {0};
        JZ_VEC_FOR(const jazz_db_OccurrenceRow, a, *update_added) {
            jazz_node_CurrentRow r;
            jazz_node__node__CurrentRow__clone(&r, &a->row);
            JZ_VEC_PUSH(snapshot->rows, r);
            jazz_types_OutputOccurrenceId o;
            jazz_types__object__OutputOccurrenceId__clone(&o, &a->occurrence);
            JZ_VEC_PUSH(occ, o);
        }
        /* Later duplicates replace earlier ones, as in a keyed map. */
        jazz_db_RelationSnapshotIndex fresh;
        jazz_db__db__RelationSnapshotIndex__with_root_occurrences(&fresh, occ.ptr, occ.len);
        jz_free(occ.ptr);
        jazz_db__positional_order__PositionalOrder__drop(&snapshot_index->roots);
        snapshot_index->roots = fresh.roots;
        jz_btree_drop(&fresh.terminal_records);
        jazz_db_SubscriptionEvent e = jazz_db__db__SubscriptionEvent__delta(false, tier, settled);
        for (u32 index = 0; index < update_added->len; index++) {
            jazz_db_SubscriptionOutputRow r;
            r.occurrence_id = update_added->ptr[index].occurrence;
            r.row = update_added->ptr[index].row;
            r.has_previous_index = false;
            r.previous_index = 0;
            r.index = index;
            JZ_VEC_PUSH(e.added, r);
        }
        jz_free(update_added->ptr);
        occurrence_vec_drop(update_removed);
        *out = e;
        return JZ_OK;
    }

    jazz_db_SubscriptionEvent e = jazz_db__db__SubscriptionEvent__delta(false, tier, settled);
    JZ_VEC_FOR(const jazz_db_OccurrenceRow, a, *update_added) {
        u32 position;
        if (jazz_db__positional_order__PositionalOrder__position(&snapshot_index->roots, &a->occurrence, &position)) {
            bool equivalent = jazz_node__node__CurrentRow__subscription_equivalent(&snapshot->rows.ptr[position], &a->row);
            if (jazz_types__debug_env__covered_input_trace()) {
                jz_str k = {0}, o = {0}, n = {0};
                jazz_types__object__OutputOccurrenceId__debug(&k, &a->occurrence);
                jazz_node__node__CurrentRow__debug(&o, &snapshot->rows.ptr[position]);
                jazz_node__node__CurrentRow__debug(&n, &a->row);
                jz_trace_event(0, "stderr", "JAZZ_COVERED_INPUT_TRACE stage=flat_snapshot_replace occurrence=%.*s position=%u equivalent=%s old=%.*s new=%.*s",
                               (int)k.len, k.ptr, position, equivalent ? "true" : "false", (int)o.len, o.ptr, (int)n.len, n.ptr);
                jz_str_drop(&k); jz_str_drop(&o); jz_str_drop(&n);
            }
            if (!equivalent) {
                jazz_node__node__CurrentRow__drop(&snapshot->rows.ptr[position]);
                jazz_node__node__CurrentRow__clone(&snapshot->rows.ptr[position], &a->row);
                push_output_row(&e.updated, &a->occurrence, &a->row, true, position, position);
            }
        } else {
            u32 index = snapshot->root_count;
            jazz_node_CurrentRow r;
            jazz_node__node__CurrentRow__clone(&r, &a->row);
            rows_insert(snapshot, index, r);
            jazz_types_OutputOccurrenceId k;
            jazz_types__object__OutputOccurrenceId__clone(&k, &a->occurrence);
            jazz_db__positional_order__PositionalOrder__insert(&snapshot_index->roots, index, &k);
            snapshot->root_count += 1;
            push_output_row(&e.added, &a->occurrence, &a->row, false, 0, index);
        }
    }

    if (update_removed->len != 0) {
        jz_btree replaced, requested_removals;
        jz_btree_init(&replaced, &jazz_db_occurrence_set_desc);
        jz_btree_init(&requested_removals, &jazz_db_occurrence_set_desc);
        JZ_VEC_FOR(const jazz_db_OccurrenceRow, a, *update_added) {
            jazz_types_OutputOccurrenceId k;
            jazz_types__object__OutputOccurrenceId__clone(&k, &a->occurrence);
            jz_btree_insert(&replaced, &k, NULL, NULL);
        }
        JZ_VEC_FOR(const jazz_types_OutputOccurrenceId, r, *update_removed) {
            jazz_types_OutputOccurrenceId k;
            jazz_types__object__OutputOccurrenceId__clone(&k, r);
            jz_btree_insert(&requested_removals, &k, NULL, NULL);
        }
        JZ_VEC(OccPos) removals = {0};
        jazz_types_OutputOccurrenceId *occurrence;
        void *unit;
        JZ_BTREE_FOR(&requested_removals, occurrence, unit) {
            if (jz_btree_contains(&replaced, occurrence)) continue;
            u32 position;
            if (!jazz_db__positional_order__PositionalOrder__position(&snapshot_index->roots, occurrence, &position)) continue;
            OccPos op;
            jazz_types__object__OutputOccurrenceId__clone(&op.occ, occurrence);
            op.position = position;
            JZ_VEC_PUSH(removals, op);
        }
        jz_sort(removals.ptr, removals.len, sizeof(OccPos), occpos_cmp, NULL);
        if (removals.len != 0) {
            /* A removed index is the position in the complete pre-frame result. */
            JZ_VEC(u32) removed_positions = {0};
            JZ_VEC_FOR(const OccPos, op, removals) JZ_VEC_PUSH(removed_positions, op->position);
            JZ_VEC_FOR(const OccPos, op, removals) push_removed_row(&e.removed, &snapshot->rows.ptr[op->position], &op->occ, op->position);
            u32 root_count_before_removals = snapshot->root_count;
            u32 w = 0;
            for (u32 position = 0; position < snapshot->rows.len; position++) {
                u32 idx;
                bool keep = position >= root_count_before_removals
                    || !jz_bsearch(removed_positions.ptr, removed_positions.len, sizeof(u32), &position, u32_cmp_plain, NULL, &idx);
                if (keep) snapshot->rows.ptr[w++] = snapshot->rows.ptr[position];
                else jazz_node__node__CurrentRow__drop(&snapshot->rows.ptr[position]);
            }
            snapshot->rows.len = w;
            snapshot->root_count -= removed_positions.len;
            JZ_VEC_FOR(const OccPos, op, removals) {
                u32 ignored;
                jazz_db__positional_order__PositionalOrder__remove(&snapshot_index->roots, &op->occ, &ignored);
            }
            jz_free(removed_positions.ptr);
        }
        JZ_VEC_FOR(OccPos, op, removals) jazz_types__object__OutputOccurrenceId__drop(&op->occ);
        jz_free(removals.ptr);
        jz_btree_drop(&replaced);
        jz_btree_drop(&requested_removals);
    }
    jz_vec_drop(update_added, sizeof(jazz_db_OccurrenceRow), occurrence_row_drop);
    occurrence_vec_drop(update_removed);
    *out = e;
    return JZ_OK;
}

static jz_err jazz_db__db__materialize_subscription_terminal_record(jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index, const jazz_types_OutputOccurrenceId *occurrence);
static jz_err jazz_db__db__apply_descendant_terminal_operations_to_snapshot(jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *snapshot_index,
                                                                           const jz_btree *removed_roots, const groove_TerminalOperation *operations, u32 n, bool root_union_arm);
static jz_err jazz_db__db__terminal_subscription_output_row(jz_slice table, jazz_types_OutputOccurrenceId occurrence_id, jz_slice raw, const jazz_node_TerminalRootLayout *layout,
                                                           bool has_previous_index, u32 previous_index, u32 index, jazz_db_SubscriptionOutputRow *out);

typedef struct AddressedOperation { bool has_occurrence; jazz_types_OutputOccurrenceId occurrence; groove_TerminalOperation operation; } AddressedOperation;
typedef struct BeforeRow { u32 index; jazz_node_CurrentRow row; } BeforeRow;
static void before_row_drop(void *p) { jazz_node__node__CurrentRow__drop(&((BeforeRow *)p)->row); }
static const jz_btree_desc before_desc = { sizeof(jazz_types_OutputOccurrenceId), sizeof(BeforeRow), jazz_types__object__OutputOccurrenceId__cmp, jazz_types__object__OutputOccurrenceId__drop, before_row_drop };
static void addressed_drop(void *p) {
    AddressedOperation *a = p;
    if (a->has_occurrence) jazz_types__object__OutputOccurrenceId__drop(&a->occurrence);
    groove__ivm__runtime__terminal__TerminalOperation__drop(&a->operation);
}

/* Applies only top-level structured-terminal edits to the producer-owned snapshot. */
static jz_err jazz_db__db__apply_terminal_operations_to_subscription_snapshot(jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *snapshot_index,
                                                                             jazz_db_TerminalOperationVec operations, const jz_btree *occurrence_overrides,
                                                                             const jazz_node_TerminalRootLayout *layout, jz_slice table, u32 tier, bool settled,
                                                                             jazz_db_SubscriptionEvent *out) {
    JZ_VEC(AddressedOperation) addressed = {0};
    jz_err e = JZ_OK;
    u32 consumed = 0;
    for (; consumed < operations.len; consumed++) {
        groove_TerminalOperation operation = operations.ptr[consumed];
        if (!groove__records__RecordDescriptor__eq(operation.root_descriptor, layout->root_descriptor)) {
            groove__ivm__runtime__terminal__TerminalOperation__drop(&operation);
            e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal operation descriptor disagrees with its prepared root layout");
            break;
        }
        AddressedOperation a = {0};
        a.operation = operation;
        if (operation.path.len == 0) {
            if (!jz_bytes_eq(&operation.edit.key, &operation.root_key)) {
                groove__ivm__runtime__terminal__TerminalOperation__drop(&operation);
                e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal root edit key does not match its addressed root key");
                break;
            }
            const jazz_types_OutputOccurrenceId *ov = occurrence_overrides ? jz_btree_get(occurrence_overrides, &operation.root_key) : NULL;
            if (ov) {
                jazz_types__object__OutputOccurrenceId__clone(&a.occurrence, ov);
            } else {
                e = jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(jz_bytes_slice(&operation.root_key), layout->root_union_arm, &a.occurrence);
                if (JZ_IS_ERR(e)) { groove__ivm__runtime__terminal__TerminalOperation__drop(&operation); break; }
            }
            a.has_occurrence = true;
        }
        JZ_VEC_PUSH(addressed, a);
    }
    if (JZ_IS_ERR(e)) {
        for (u32 r = consumed + 1; r < operations.len; r++) groove__ivm__runtime__terminal__TerminalOperation__drop(&operations.ptr[r]);
        jz_free(operations.ptr);
        jz_vec_drop(&addressed, sizeof(AddressedOperation), addressed_drop);
        return e;
    }
    jz_free(operations.ptr);

    /* Every root must be addressable before positional edits are applied. */
    if (jazz_db__positional_order__PositionalOrder__len(&snapshot_index->roots) != snapshot->root_count
        || jazz_db__positional_order__PositionalOrder__anonymous_len(&snapshot_index->roots) != 0) {
        jz_vec_drop(&addressed, sizeof(AddressedOperation), addressed_drop);
        return jz_err_new(JAZZ_EC_PROTOCOL, "maintained snapshot root is missing an occurrence identity");
    }
    jz_btree affected, before, removed_roots;
    jz_btree_init(&affected, &jazz_db_occurrence_set_desc);
    jz_btree_init(&before, &before_desc);
    jz_btree_init(&removed_roots, &jazz_db_occurrence_set_desc);
    jazz_db_TerminalOperationVec descendant_operations = {0};
    JZ_VEC_FOR(const AddressedOperation, a, addressed) {
        if (!a->has_occurrence) continue;
        jazz_types_OutputOccurrenceId k;
        jazz_types__object__OutputOccurrenceId__clone(&k, &a->occurrence);
        jz_btree_insert(&affected, &k, NULL, NULL);
    }
    jazz_types_OutputOccurrenceId *occ;
    void *unit;
    JZ_BTREE_FOR(&affected, occ, unit) {
        e = jazz_db__db__materialize_subscription_terminal_record(snapshot, snapshot_index, occ);
        if (JZ_IS_ERR(e)) goto fail;
    }
    JZ_BTREE_FOR(&affected, occ, unit) {
        u32 index;
        if (!jazz_db__positional_order__PositionalOrder__position(&snapshot_index->roots, occ, &index)) continue;
        jazz_types_OutputOccurrenceId k;
        jazz_types__object__OutputOccurrenceId__clone(&k, occ);
        BeforeRow b;
        b.index = index;
        jazz_node__node__CurrentRow__clone(&b.row, &snapshot->rows.ptr[index]);
        jz_btree_insert(&before, &k, &b, NULL);
    }

    /* Apply Groove's positional edits directly to the root order. */
    u32 ai = 0;
    for (; ai < addressed.len; ai++) {
        AddressedOperation *a = &addressed.ptr[ai];
        groove_TerminalOperation *operation = &a->operation;
        if (!a->has_occurrence) {
            e = jazz_db__db__apply_descendant_terminal_operations_to_snapshot(snapshot, snapshot_index, &removed_roots, operation, 1, layout->root_union_arm);
            if (JZ_IS_ERR(e)) goto fail_ops;
            JZ_VEC_PUSH(descendant_operations, *operation);
            memset(operation, 0, sizeof *operation);
            continue;
        }
        jazz_types_OutputOccurrenceId *occurrence = &a->occurrence;
        if (operation->edit.tag == GROOVE_TE_UPDATE) {
            /* A root which has not received child edits still holds its collections in the snapshot row. */
            if (!jz_btree_contains(&snapshot_index->terminal_records, occurrence)) {
                u32 position;
                if (!jazz_db__positional_order__PositionalOrder__position(&snapshot_index->roots, occurrence, &position)) {
                    e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal root update addressed a missing result");
                    goto fail_ops;
                }
                const groove_RecordDescriptor *descriptor;
                jz_slice raw;
                jazz_node__node__CurrentRow__encoded_record(&snapshot->rows.ptr[position], &descriptor, &raw);
                groove_OwnedRecord record;
                groove__records__OwnedRecord__new(&record, jz_bytes_from(raw.ptr, raw.len), descriptor);
                jazz_node_TerminalRecordState *state;
                e = jazz_node__node__terminal_record__TerminalRecordState__new(record, &state);
                if (JZ_IS_ERR(e)) goto fail_ops;
                jazz_types_OutputOccurrenceId k;
                jazz_types__object__OutputOccurrenceId__clone(&k, occurrence);
                jz_btree_insert(&snapshot_index->terminal_records, &k, &state, NULL);
            }
            jazz_node_TerminalRecordState **state = jz_btree_get(&snapshot_index->terminal_records, occurrence);
            if (!state) jz_panic("initialized above");
            groove_OwnedRecord record;
            groove__records__OwnedRecord__new(&record, jz_bytes_clone(&operation->edit.value), operation->root_descriptor);
            e = jazz_node__node__terminal_record__TerminalRecordState__update_record(*state, record);
            if (JZ_IS_ERR(e)) goto fail_ops;
        } else if (operation->edit.tag != GROOVE_TE_MOVE) {
            jz_btree_remove(&snapshot_index->terminal_records, occurrence, NULL, NULL);
        }
        switch (operation->edit.tag) {
        case GROOVE_TE_INSERT: {
            jz_btree_remove(&removed_roots, occurrence, NULL, NULL);
            u32 existing;
            if (jazz_db__positional_order__PositionalOrder__remove(&snapshot_index->roots, occurrence, &existing)) {
                rows_remove(snapshot, existing, NULL);
                snapshot->root_count -= 1;
            }
            u32 index = operation->edit.index < snapshot->root_count ? operation->edit.index : snapshot->root_count;
            jazz_types_OutputOccurrenceId occ_clone;
            jazz_types__object__OutputOccurrenceId__clone(&occ_clone, occurrence);
            jazz_db_SubscriptionOutputRow row;
            e = jazz_db__db__terminal_subscription_output_row(table, occ_clone, jz_bytes_slice(&operation->edit.value), layout, false, 0, index, &row);
            if (JZ_IS_ERR(e)) goto fail_ops;
            jazz_types__object__OutputOccurrenceId__drop(&row.occurrence_id);
            jazz_db__positional_order__PositionalOrder__insert(&snapshot_index->roots, index, occurrence);
            a->has_occurrence = false; /* moved into the index */
            rows_insert(snapshot, index, row.row);
            snapshot->root_count += 1;
            break;
        }
        case GROOVE_TE_UPDATE: {
            u32 index;
            if (!jazz_db__positional_order__PositionalOrder__position(&snapshot_index->roots, occurrence, &index)) {
                e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal root update addressed a missing result");
                goto fail_ops;
            }
            jazz_types_OutputOccurrenceId occ_clone;
            jazz_types__object__OutputOccurrenceId__clone(&occ_clone, occurrence);
            jazz_db_SubscriptionOutputRow row;
            e = jazz_db__db__terminal_subscription_output_row(table, occ_clone, jz_bytes_slice(&operation->edit.value), layout, true, index, index, &row);
            if (JZ_IS_ERR(e)) goto fail_ops;
            jazz_types__object__OutputOccurrenceId__drop(&row.occurrence_id);
            jazz_node__node__CurrentRow__drop(&snapshot->rows.ptr[index]);
            snapshot->rows.ptr[index] = row.row;
            break;
        }
        case GROOVE_TE_REMOVE: {
            jazz_types_OutputOccurrenceId k;
            jazz_types__object__OutputOccurrenceId__clone(&k, occurrence);
            jz_btree_insert(&removed_roots, &k, NULL, NULL);
            u32 index;
            if (!jazz_db__positional_order__PositionalOrder__remove(&snapshot_index->roots, occurrence, &index)) {
                e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal root removal addressed a missing result");
                goto fail_ops;
            }
            rows_remove(snapshot, index, NULL);
            snapshot->root_count -= 1;
            break;
        }
        default: { /* Move */
            u32 previous_index;
            if (!jazz_db__positional_order__PositionalOrder__remove(&snapshot_index->roots, occurrence, &previous_index)) {
                e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal root move addressed a missing result");
                goto fail_ops;
            }
            jazz_node_CurrentRow row;
            rows_remove(snapshot, previous_index, &row);
            u32 max = snapshot->root_count > 0 ? snapshot->root_count - 1 : 0;
            u32 index = operation->edit.index < max ? operation->edit.index : max;
            jazz_db__positional_order__PositionalOrder__insert(&snapshot_index->roots, index, occurrence);
            a->has_occurrence = false;
            rows_insert(snapshot, index, row);
            break;
        }
        }
    }

    /* Roots changed in this drain are published as complete final rows below. */
    {
        u32 w = 0;
        for (u32 i = 0; i < descendant_operations.len; i++) {
            groove_TerminalOperation *operation = &descendant_operations.ptr[i];
            jazz_types_OutputOccurrenceId o;
            jz_err oe = jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(jz_bytes_slice(&operation->root_key), layout->root_union_arm, &o);
            bool drop_it = false;
            if (!JZ_IS_ERR(oe)) { drop_it = jz_btree_contains(&affected, &o); jazz_types__object__OutputOccurrenceId__drop(&o); }
            else jz_err_drop(&oe);
            if (drop_it) groove__ivm__runtime__terminal__TerminalOperation__drop(operation);
            else descendant_operations.ptr[w++] = *operation;
        }
        descendant_operations.len = w;
    }

    JZ_BTREE_FOR(&affected, occ, unit) {
        e = jazz_db__db__materialize_subscription_terminal_record(snapshot, snapshot_index, occ);
        if (JZ_IS_ERR(e)) goto fail;
    }

    {
        jazz_db_SubscriptionEvent event = jazz_db__db__SubscriptionEvent__delta(false, tier, settled);
        JZ_BTREE_FOR(&affected, occ, unit) {
            const BeforeRow *previous = jz_btree_get(&before, occ);
            u32 index;
            bool has_current = jazz_db__positional_order__PositionalOrder__position(&snapshot_index->roots, occ, &index);
            if (!previous && has_current) {
                push_output_row(&event.added, occ, &snapshot->rows.ptr[index], false, 0, index);
            } else if (previous && !has_current) {
                push_removed_row(&event.removed, &previous->row, occ, previous->index);
            } else if (previous && has_current
                       && (!jazz_node__node__CurrentRow__eq(&previous->row, &snapshot->rows.ptr[index]) || previous->index != index)) {
                push_output_row(&event.updated, occ, &snapshot->rows.ptr[index], true, previous->index, index);
            }
        }
        event.terminal_operations = descendant_operations;
        *out = event;
    }
    jz_vec_drop(&addressed, sizeof(AddressedOperation), addressed_drop);
    jz_btree_drop(&affected);
    jz_btree_drop(&before);
    jz_btree_drop(&removed_roots);
    return JZ_OK;
fail_ops:
fail:
    terminal_ops_drop(&descendant_operations);
    jz_vec_drop(&addressed, sizeof(AddressedOperation), addressed_drop);
    jz_btree_drop(&affected);
    jz_btree_drop(&before);
    jz_btree_drop(&removed_roots);
    return e;
}

/* Fold descendant edits into the private receiver snapshot. */
static jz_err jazz_db__db__apply_descendant_terminal_operations_to_snapshot(jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *snapshot_index,
                                                                           const jz_btree *removed_roots, const groove_TerminalOperation *operations, u32 n, bool root_union_arm) {
    for (u32 i = 0; i < n; i++) {
        const groove_TerminalOperation *operation = &operations[i];
        if (operation->path.len == 0) continue;
        jazz_types_OutputOccurrenceId occurrence;
        JZ_TRY(jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(jz_bytes_slice(&operation->root_key), root_union_arm, &occurrence));
        u32 root_index;
        if (!jazz_db__positional_order__PositionalOrder__position(&snapshot_index->roots, &occurrence, &root_index)) {
            /* A collector can emit the child retractions belonging to a root it already retracted. */
            bool removed = jz_btree_contains(removed_roots, &occurrence);
            jazz_types__object__OutputOccurrenceId__drop(&occurrence);
            if (removed) continue;
            return jz_err_new(JAZZ_EC_PROTOCOL, "terminal descendant operation addressed a missing root");
        }
        if (root_index >= snapshot->rows.len) {
            jazz_types__object__OutputOccurrenceId__drop(&occurrence);
            return jz_err_new(JAZZ_EC_PROTOCOL, "terminal root position is outside the receiver snapshot");
        }
        jazz_node_CurrentRow *root = &snapshot->rows.ptr[root_index];
        if (!jz_btree_contains(&snapshot_index->terminal_records, &occurrence)) {
            const groove_RecordDescriptor *descriptor;
            jz_slice raw;
            jazz_node__node__CurrentRow__encoded_record(root, &descriptor, &raw);
            groove_OwnedRecord record;
            groove__records__OwnedRecord__new(&record, jz_bytes_from(raw.ptr, raw.len), descriptor);
            jazz_node_TerminalRecordState *state;
            jz_err e = jazz_node__node__terminal_record__TerminalRecordState__new(record, &state);
            if (JZ_IS_ERR(e)) { jazz_types__object__OutputOccurrenceId__drop(&occurrence); return e; }
            jazz_types_OutputOccurrenceId k;
            jazz_types__object__OutputOccurrenceId__clone(&k, &occurrence);
            jz_btree_insert(&snapshot_index->terminal_records, &k, &state, NULL);
        }
        jazz_node_TerminalRecordState **state = jz_btree_get(&snapshot_index->terminal_records, &occurrence);
        jazz_types__object__OutputOccurrenceId__drop(&occurrence);
        if (!state) jz_panic("initialized above");
        JZ_TRY(jazz_node__node__terminal_record__TerminalRecordState__apply(*state, operation));
    }
    return JZ_OK;
}

/* Materialize the retained terminal state only at a full-snapshot boundary. */
jz_err jazz_db__db__materialized_subscription_snapshot(const jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index, jazz_node_RelationSnapshot *out) {
    jazz_node__node__RelationSnapshot__clone(out, snapshot);
    jazz_types_OutputOccurrenceId *occurrence;
    jazz_node_TerminalRecordState **rec;
    JZ_BTREE_FOR(&index->terminal_records, occurrence, rec) {
        jz_err e = jazz_db__db__materialize_subscription_terminal_record(out, index, occurrence);
        if (JZ_IS_ERR(e)) { jazz_node__node__RelationSnapshot__drop(out); return e; }
    }
    return JZ_OK;
}

static jz_err jazz_db__db__materialize_subscription_terminal_record(jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index, const jazz_types_OutputOccurrenceId *occurrence) {
    jazz_node_TerminalRecordState *const *record = jz_btree_get(&index->terminal_records, occurrence);
    if (!record) return JZ_OK;
    u32 position;
    if (!jazz_db__positional_order__PositionalOrder__position(&index->roots, occurrence, &position))
        return jz_err_new(JAZZ_EC_PROTOCOL, "retained terminal record has no root occurrence");
    if (position >= snapshot->rows.len)
        return jz_err_new(JAZZ_EC_PROTOCOL, "retained terminal root position is outside snapshot");
    jazz_node_CurrentRow *root = &snapshot->rows.ptr[position];
    jz_slice table = jazz_node__node__CurrentRow__table(root);
    void *publication_fields = jazz_node__node__CurrentRow__publication_fields_clone(root);
    groove_OwnedRecord owned;
    jz_err e = jazz_node__node__terminal_record__TerminalRecordState__record(*record, &owned);
    if (JZ_IS_ERR(e)) return e;
    jazz_node_CurrentRow replacement;
    jazz_node__node__CurrentRow__new_with_publication_fields(&replacement, table, owned, publication_fields);
    jazz_node__node__CurrentRow__drop(root);
    *root = replacement;
    return JZ_OK;
}

static jz_err jazz_db__db__terminal_subscription_output_row(jz_slice table, jazz_types_OutputOccurrenceId occurrence_id, jz_slice raw, const jazz_node_TerminalRootLayout *layout,
                                                           bool has_previous_index, u32 previous_index, u32 index, jazz_db_SubscriptionOutputRow *out) {
    jz_err e = JZ_OK;
    u32 nfields = 0;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(layout->root_descriptor, &nfields);
    if (layout->root_key_slot >= nfields) { e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal root layout key slot is out of bounds"); goto fail; }
    const groove_DescriptorField *root_field = &fields[layout->root_key_slot];
    if (!root_field->has_name || !jz_str_eq(&root_field->name, &layout->root_key_field_name)) {
        e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal root layout key slot does not match its descriptor");
        goto fail;
    }
    if (layout->root_key_slot != 0) { e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal root layout cannot be represented as a current row"); goto fail; }
    {
        static const jz_btree_desc slot_set_desc = { sizeof(u32), 0, u32_cmp_plain, NULL, NULL };
        jz_btree occupied_slots;
        jz_btree_init(&occupied_slots, &slot_set_desc);
        u32 key_slot = layout->root_key_slot;
        jz_btree_insert(&occupied_slots, &key_slot, NULL, NULL);
        JZ_VEC_FOR(const jazz_node_TerminalRootPublicField, field, layout->public_fields) {
            if (field->slot >= nfields) {
                e = jz_err_fmt(JAZZ_EC_PROTOCOL, "terminal root layout field %.*s is out of bounds", (int)field->name.len, field->name.ptr);
                jz_btree_drop(&occupied_slots);
                goto fail;
            }
            const groove_DescriptorField *descriptor_field = &fields[field->slot];
            u32 slot = field->slot;
            if (!descriptor_field->has_name || !jz_str_eq(&descriptor_field->name, &field->descriptor_field_name)
                || jz_btree_insert(&occupied_slots, &slot, NULL, NULL)) {
                e = jz_err_fmt(JAZZ_EC_PROTOCOL, "terminal root layout field %.*s does not match its descriptor", (int)field->name.len, field->name.ptr);
                jz_btree_drop(&occupied_slots);
                goto fail;
            }
        }
        jz_btree_drop(&occupied_slots);
    }
    {
        ValueVec values = {0};
        e = groove__records__BorrowedRecord__to_values(raw, layout->root_descriptor, (void *)&values);
        if (JZ_IS_ERR(e)) { e = wrap_err(JAZZ_EC_PROTOCOL, "invalid terminal root payload", e); goto fail; }
        jz_vec_drop(&values, sizeof(groove_Value), value_drop);
    }
    jazz_types_RowUuid row_uuid;
    e = groove__records__BorrowedRecord__get_uuid(raw, layout->root_descriptor, layout->root_key_slot, &row_uuid);
    if (JZ_IS_ERR(e)) { e = wrap_err(JAZZ_EC_PROTOCOL, "invalid terminal root key", e); goto fail; }
    {
        jz_bytes cb = jazz_types__object__OutputOccurrenceId__canonical_bytes(&occurrence_id);
        bool matches = cb.len >= 16 && memcmp(cb.ptr, row_uuid.b, 16) == 0;
        jz_bytes_drop(&cb);
        if (!matches) { e = jz_err_new(JAZZ_EC_PROTOCOL, "terminal root payload key does not match its addressed result"); goto fail; }
    }
    out->occurrence_id = occurrence_id;
    groove_OwnedRecord record;
    groove__records__OwnedRecord__new(&record, jz_bytes_from(raw.ptr, raw.len), layout->root_descriptor);
    jazz_node__node__CurrentRow__new_with_publication_fields(&out->row, table, record, jazz_node__node__terminal_root__terminal_root_publication_fields(layout));
    out->has_previous_index = has_previous_index;
    out->previous_index = previous_index;
    out->index = index;
    return JZ_OK;
fail:
    jazz_types__object__OutputOccurrenceId__drop(&occurrence_id);
    return e;
}

jz_err jazz_db__db__snapshot_root_occurrences(const jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index, jazz_db_OccurrenceVec *out) {
    u32 len = jazz_db__positional_order__PositionalOrder__len(&index->roots);
    if (len > snapshot->root_count)
        return jz_err_new(JAZZ_EC_PROTOCOL, "maintained root occurrence index exceeds snapshot roots");
    if (len != snapshot->root_count || jazz_db__positional_order__PositionalOrder__anonymous_len(&index->roots) != 0)
        return jz_err_new(JAZZ_EC_PROTOCOL, "maintained snapshot root is missing an occurrence identity");
    jazz_db_OccurrenceVec v = {0};
    jazz_db_PositionalOrderIter it;
    jazz_db__positional_order__PositionalOrder__iter(&index->roots, &it);
    const void *key;
    while (jazz_db__positional_order__Iter__next(&it, &key)) {
        if (!key) continue; /* flatten() */
        jazz_types_OutputOccurrenceId o;
        jazz_types__object__OutputOccurrenceId__clone(&o, key);
        JZ_VEC_PUSH(v, o);
    }
    jazz_db__positional_order__Iter__drop(&it);
    *out = v;
    return JZ_OK;
}

jz_err jazz_db__db__relation_snapshot_index_with_root_occurrences(const jazz_node_RelationSnapshot *snapshot, const jazz_types_OutputOccurrenceId *occurrences, u32 n, jazz_db_RelationSnapshotIndex *out) {
    if (snapshot->root_count != n)
        return jz_err_new(JAZZ_EC_PROTOCOL, "maintained terminal occurrence sidecar length does not match root rows");
    jazz_db_OccurrenceVec keys = {0};
    jz_vec_reserve(&keys, n, sizeof(jazz_types_OutputOccurrenceId));
    for (u32 i = 0; i < n; i++) {
        jazz_types_OutputOccurrenceId o;
        jazz_types__object__OutputOccurrenceId__clone(&o, &occurrences[i]);
        JZ_VEC_PUSH(keys, o);
    }
    jazz_types_OutputOccurrenceId dup;
    bool ok = jazz_db__positional_order__PositionalOrder__from_ordered(&out->roots, &jazz_db__positional_order__occurrence_key, keys.ptr, keys.len, &dup);
    jz_free(keys.ptr);
    if (!ok) {
        jazz_types__object__OutputOccurrenceId__drop(&dup);
        return jz_err_new(JAZZ_EC_PROTOCOL, "maintained terminal occurrence sidecar contains duplicate identity");
    }
    jz_btree_init(&out->terminal_records, &terminal_records_desc);
    return JZ_OK;
}

/* Flat tuple identity is carried by the maintained terminal sidecar, not the public row. */
jz_err jazz_db__db__maintained_snapshot_index_or_row_index(jazz_node_NodeState *node, const jazz_db_SubscriptionKind *kind, const jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *out) {
    if (!kind->maintained_subscription) { jazz_db__db__RelationSnapshotIndex__from_snapshot(out, snapshot); return JZ_OK; }
    if (!jazz_model__query__Query__has_flat_join(jazz_model__query__ValidatedQuery__query(kind->shape))) {
        jazz_db__db__RelationSnapshotIndex__from_snapshot(out, snapshot);
        return JZ_OK;
    }
    jazz_node_RelationSnapshot materialized;
    jazz_db_OccurrenceVec root_occurrence_ids = {0};
    JZ_TRY(jazz_node__node__NodeState__materialize_local_maintained_relation_snapshot_with_occurrences(node, kind->maintained_subscription, &materialized, &root_occurrence_ids));
    jz_err e = JZ_OK;
    if (materialized.root_count == snapshot->root_count)
        e = jazz_db__db__relation_snapshot_index_with_root_occurrences(snapshot, root_occurrence_ids.ptr, root_occurrence_ids.len, out);
    else
        jazz_db__db__RelationSnapshotIndex__from_snapshot(out, snapshot);
    jazz_node__node__RelationSnapshot__drop(&materialized);
    occurrence_vec_drop(&root_occurrence_ids);
    return e;
}

static u32 delta_slack(u32 len) { u32 s = len / 8; return s > 64 ? s : 64; }
void jazz_db__db__reserve_relation_snapshot_delta_slack(jazz_node_RelationSnapshot *snapshot) {
    jz_vec_reserve(&snapshot->rows, delta_slack(snapshot->rows.len), sizeof(jazz_node_CurrentRow));
    jz_vec_reserve(&snapshot->edges, delta_slack(snapshot->edges.len), sizeof(jazz_node_RelationEdge));
}
void jazz_db__db__relation_snapshot_with_delta_slack(const jazz_node_RelationSnapshot *snapshot, jazz_node_RelationSnapshot *out) {
    jazz_node__node__RelationSnapshot__clone(out, snapshot);
    jazz_db__db__reserve_relation_snapshot_delta_slack(out);
}

bool jazz_db__db__subscription_is_settled(const jazz_node_NodeState *node, jazz_db_ActiveAuthorityViewReceipts receipts, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
                                          u32 tier, jazz_protocol_ReadViewSpec read_view, bool requires_authority_receipt, const jazz_protocol_AuthorityResultKey *authority_result_key) {
    if (tier <= JAZZ_DUR_LOCAL) { jazz_protocol__protocol__ReadViewSpec__drop(&read_view); return true; }
    jazz_protocol_RegisterShapeOptions opts;
    jazz_protocol__protocol__RegisterShapeOptions__default(&opts);
    opts.tier = tier;
    jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view);
    opts.read_view = read_view;
    jazz_protocol_BindingViewKey binding_view_key;
    binding_view_key.shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
    binding_view_key.binding_id = jazz_model__query__Binding__binding_id(binding);
    binding_view_key.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
    /* Callers without a registered usage-site key may inspect only the explicit unscoped receipt. */
    jazz_protocol_AuthorityResultKey fallback_unscoped;
    jazz_protocol__protocol__AuthorityResultKey__unscoped(&fallback_unscoped, binding_view_key);
    const jazz_protocol_AuthorityResultKey *key = authority_result_key ? authority_result_key : &fallback_unscoped;
    return jazz_node__node__NodeState__has_settled_authority_result(node, key)
        && !jazz_node__node__NodeState__opening_pending_for_authority_result(node, key)
        && (!requires_authority_receipt || (receipts->has && jz_btree_contains(&receipts->binding_views, &binding_view_key)));
}

void jazz_db__db__subscription_row_occurrence_id(const jazz_node_CurrentRow *row, jazz_types_OutputOccurrenceId *out) {
    jazz_types_RowUuid root_uuid = jazz_node__node__CurrentRow__row_uuid(row);
    jazz_types_ObjectId root = jazz_types__object__ObjectId__from_uuid(&root_uuid);
    JZ_VEC(jazz_types_ObjectId) joined = {0};
    for (u32 position = 1;; position++) {
        jz_str name = jz_format("__flat_join_row_%u", position);
        groove_Value value;
        bool found = jazz_node__node__CurrentRow__raw_field(row, jz_str_slice(&name), &value);
        jz_str_drop(&name);
        if (!found) break;
        if (value.tag != GROOVE_V_UUID) { groove__records__Value__drop(&value); break; }
        jazz_types_ObjectId id = jazz_types__object__ObjectId__from_uuid(&value.v.uuid);
        JZ_VEC_PUSH(joined, id);
    }
    jazz_types__object__OutputOccurrenceId__new(out, root, joined.ptr, joined.len);
    jz_free(joined.ptr);
}

jz_err jazz_db__db__subscription_outputs_with_occurrence_sidecar(const jazz_node_RelationSnapshot *snapshot, const jazz_types_OutputOccurrenceId *occurrence_ids, u32 n, jazz_db_OutputRowVec *out) {
    if (n != snapshot->root_count)
        return jz_err_new(JAZZ_EC_PROTOCOL, "maintained root occurrence sidecar length does not match root rows");
    jz_btree unique;
    jz_btree_init(&unique, &jazz_db_occurrence_set_desc);
    jazz_db_OutputRowVec rows = {0};
    jz_err e = JZ_OK;
    u32 take = snapshot->root_count < snapshot->rows.len ? snapshot->root_count : snapshot->rows.len;
    for (u32 index = 0; index < take; index++) {
        const jazz_node_CurrentRow *row = &snapshot->rows.ptr[index];
        const jazz_types_OutputOccurrenceId *occurrence_id = &occurrence_ids[index];
        jz_bytes bytes = jazz_types__object__OutputOccurrenceId__canonical_bytes(occurrence_id);
        jazz_types_RowUuid uuid = jazz_node__node__CurrentRow__row_uuid(row);
        bool matches = bytes.len >= 16 && memcmp(bytes.ptr, uuid.b, 16) == 0;
        jz_bytes_drop(&bytes);
        if (!matches) { e = jz_err_new(JAZZ_EC_PROTOCOL, "maintained root occurrence sidecar root does not match row"); break; }
        jazz_types_OutputOccurrenceId k;
        jazz_types__object__OutputOccurrenceId__clone(&k, occurrence_id);
        if (jz_btree_insert(&unique, &k, NULL, NULL)) { e = jz_err_new(JAZZ_EC_PROTOCOL, "maintained root occurrence sidecar contains duplicate identity"); break; }
        push_output_row(&rows, occurrence_id, row, false, 0, index);
    }
    jz_btree_drop(&unique);
    if (JZ_IS_ERR(e)) { output_rows_drop(&rows); return e; }
    *out = rows;
    return JZ_OK;
}

void jazz_db__db__reset_removed_roots(const jazz_node_RelationSnapshot *previous, const jazz_db_RelationSnapshotIndex *previous_index, const jazz_types_OutputOccurrenceId *current_occurrences, u32 n, jazz_db_RemovedRowVec *out) {
    jz_btree current;
    jz_btree_init(&current, &jazz_db_occurrence_set_desc);
    for (u32 i = 0; i < n; i++) {
        jazz_types_OutputOccurrenceId k;
        jazz_types__object__OutputOccurrenceId__clone(&k, &current_occurrences[i]);
        jz_btree_insert(&current, &k, NULL, NULL);
    }
    jazz_db_RemovedRowVec removed = {0};
    /* Keyed (not positional) order. */
    jazz_types_OutputOccurrenceId *occurrence_id;
    u32 *slot;
    JZ_BTREE_FOR(&previous_index->roots.slots, occurrence_id, slot) {
        if (jz_btree_contains(&current, occurrence_id)) continue;
        u32 position;
        if (!jazz_db__positional_order__PositionalOrder__position(&previous_index->roots, occurrence_id, &position))
            jz_panic("indexed root occurrence has a position");
        push_removed_row(&removed, &previous->rows.ptr[position], occurrence_id, position);
    }
    jz_btree_drop(&current);
    *out = removed;
}

void jazz_db__db__subscription_output_row(jazz_node_CurrentRow row, bool has_previous_index, u32 previous_index, u32 index, jazz_db_SubscriptionOutputRow *out) {
    jazz_db__db__subscription_row_occurrence_id(&row, &out->occurrence_id);
    out->row = row;
    out->has_previous_index = has_previous_index;
    out->previous_index = previous_index;
    out->index = index;
}

static void jazz_db__db__subscription_row_key(const jazz_node_CurrentRow *row, jazz_types_OutputOccurrenceId *out) {
    jazz_db__db__subscription_row_occurrence_id(row, out);
}

/* ======================= db.rs:3509-3726 ======================= */

jazz_model_DurabilityTier jazz_db__db__read_coverage_tier(const jazz_db_ReadOpts *opts) {
    switch (opts->tier) {
    case JAZZ_DB_READ_LOCAL_FIRST:
    case JAZZ_DB_READ_LOCAL_ONLY: return JAZZ_DUR_LOCAL;
    default: return JAZZ_DUR_GLOBAL;
    }
}

void jazz_db__db__upstream_register_shape_options(jazz_model_DurabilityTier tier, jazz_protocol_ReadViewSpec read_view, jazz_model_DurabilityTier upstream_durability_floor, jazz_protocol_RegisterShapeOptions *out) {
    jazz_protocol__protocol__RegisterShapeOptions__default(out);
    jazz_protocol__protocol__ReadViewSpec__drop(&out->read_view);
    out->tier = jazz_db__db__remote_subscription_tier(tier, upstream_durability_floor);
    out->read_view = read_view;
}

jazz_model_DurabilityTier jazz_db__db__remote_subscription_tier(jazz_model_DurabilityTier tier, jazz_model_DurabilityTier upstream_durability_floor) {
    return tier > upstream_durability_floor ? tier : upstream_durability_floor;
}

jz_err jazz_db__db__ensure_default_read_view(const jazz_db_ReadOpts *opts) {
    if (jazz_protocol__protocol__ReadViewSpec__is_default(&opts->read_view)) return JZ_OK;
    return jz_err_new(JAZZ_EC_QUERY, "non-default read_view is not supported yet; reads currently execute against the current/default view");
}

jz_err jazz_db__db__ensure_supported_read_view(const jazz_db_ReadOpts *opts) {
    u32 t = opts->read_view.source_tag;
    if (t == JAZZ_RVS_CURRENT || t == JAZZ_RVS_BRANCH_VIEW || t == JAZZ_RVS_SNAPSHOT) return JZ_OK;
    return jz_err_new(JAZZ_EC_QUERY, "this read_view combination is not supported yet");
}

jz_err jazz_db__db__ensure_supported_subscription_read_opts(const jazz_db_ReadOpts *opts) {
    if (opts->include_deleted)
        return jz_err_new(JAZZ_EC_QUERY, "live subscriptions do not support include_deleted yet");
    return jazz_db__db__ensure_supported_read_view(opts);
}

jz_err jazz_db__db__ensure_supported_register_shape_read_view(const jazz_protocol_RegisterShapeOptions *opts) {
    jazz_db_ReadOpts read_opts;
    jazz_db__db__ReadOpts__default(&read_opts);
    jazz_protocol__protocol__ReadViewSpec__drop(&read_opts.read_view);
    jazz_protocol__protocol__ReadViewSpec__clone(&read_opts.read_view, &opts->read_view);
    jz_err e = jazz_db__db__ensure_supported_read_view(&read_opts);
    jazz_protocol__protocol__ReadViewSpec__drop(&read_opts.read_view);
    return e;
}

jz_err jazz_db__db__ensure_supported_register_shape_options(const jazz_protocol_RegisterShapeOptions *opts, bool local_receiver, jazz_node_PeerRole peer_role, bool delegated_session_capability) {
    JZ_TRY(jazz_db__db__ensure_supported_register_shape_read_view(opts));
    if (opts->binding_source == JAZZ_BINDING_SOURCE_RELAY_AUTHORITY_SESSION && !delegated_session_capability)
        return jz_err_new(JAZZ_EC_QUERY, "relay authority-session bindings require a live server-admitted scope-isolated relay capability");
    bool supported;
    if (local_receiver || peer_role.tag == JAZZ_PEER_ROLE_RELAY) supported = opts->tier >= JAZZ_DUR_LOCAL;
    else supported = opts->tier == JAZZ_DUR_GLOBAL;
    if (!supported) {
        const char *message;
        if (local_receiver) message = "Local sync subscription serving requires at least local-tier registration";
        else if (peer_role.tag == JAZZ_PEER_ROLE_RELAY) message = "relay sync subscription serving requires at least local-tier registration";
        else message = "sync subscription serving supports only global-tier registration";
        return jz_err_new(JAZZ_EC_QUERY, message);
    }
    return JZ_OK;
}

jz_err jazz_db__db__validate_shape_ast_for_registration(jazz_node_NodeState *node, jazz_model_ShapeId shape_id, const jazz_model_ShapeAst *ast, bool *has, jazz_model_ValidatedQuery *out) {
    return jazz_node__node__NodeState__validate_shape_ast_for_registration(node, shape_id, ast, has, out);
}

void jazz_db__db__unsupported_shape_capability_rejection_message(jazz_protocol_SubscriptionKey subscription, jz_str detail, jazz_protocol_SyncMessage *out) {
    jazz_protocol_SubscribeRejectReason reason = {0};
    reason.tag = JAZZ_SRR_UNSUPPORTED_SHAPE_CAPABILITY;
    reason.detail = detail;
    jazz_db__db__subscription_rejection_message(subscription, reason, out);
}

void jazz_db__db__server_subscription_failure_rejection_message(jazz_protocol_SubscriptionKey subscription, const jz_err *error, jazz_protocol_SyncMessage *out) {
    /* Keep the complete error on the serving process only. Subscription keys
     * provide a correlation handle without disclosing schema, policy, or
     * storage details to the peer. */
    char shape[37], binding[37], read_view[37];
    jz_uuid_to_string(&subscription.shape_id.u, shape);
    jz_uuid_to_string(&subscription.binding_id.u, binding);
    jz_uuid_to_string(&subscription.read_view.id, read_view);
    jz_str line = jz_format("jazz subscription rejected: shape=%s binding=%s read_view=%s server_error=%.*s",
                            shape, binding, read_view,
                            error->msg ? (int)error->msg->len : 0, error->msg ? error->msg->ptr : "");
    jazz_std__eprintln(jz_str_slice(&line));
    jz_str_drop(&line);
    jazz_protocol_SubscribeRejectReason reason = {0};
    reason.tag = JAZZ_SRR_SERVER_FAILURE;
    reason.code = jazz_db__db__server_failure_code(error);
    jazz_db__db__subscription_rejection_message(subscription, reason, out);
}

void jazz_db__db__subscription_rejection_message(jazz_protocol_SubscriptionKey subscription, jazz_protocol_SubscribeRejectReason reason, jazz_protocol_SyncMessage *out) {
    memset(out, 0, sizeof *out);
    out->tag = JAZZ_SM_SUBSCRIBE_REJECTED;
    out->v.subscribe_rejected.subscription = subscription;
    out->v.subscribe_rejected.reason = reason;
}

u32 jazz_db__db__server_failure_code(const jz_err *error) {
    switch (error->code) {
    case JAZZ_NODE_ERR_TABLE_NOT_FOUND: return JAZZ_SSFC_TABLE_NOT_FOUND;
    case JAZZ_NODE_ERR_QUERY:
        if (jazz_node__node__Error__query_is_unknown_table(error)) return JAZZ_SSFC_TABLE_NOT_FOUND;
        return JAZZ_SSFC_QUERY_VALIDATION;
    case JAZZ_NODE_ERR_QUERY_LOWERING: return JAZZ_SSFC_QUERY_LOWERING;
    case JAZZ_NODE_ERR_AUTHORIZATION_DENIED: return JAZZ_SSFC_POLICY_EVALUATION;
    case JAZZ_NODE_ERR_INVALID_STORED_VALUE:
    case JAZZ_NODE_ERR_INVALID_CATALOGUE_UPDATE: return JAZZ_SSFC_SCHEMA_RESOLUTION;
    default: return JAZZ_SSFC_INTERNAL;
    }
}

bool jazz_db__db__is_server_shape_validation_failure(const jz_err *error) {
    return error->code == JAZZ_NODE_ERR_TABLE_NOT_FOUND || error->code == JAZZ_NODE_ERR_QUERY;
}

jazz_protocol_SubscriptionKey jazz_db__db__register_shape_rejection_subscription(jazz_model_ShapeId shape_id, jazz_protocol_ReadViewKey read_view) {
    jazz_protocol_SubscriptionKey key;
    memset(&key, 0, sizeof key);
    key.shape_id = shape_id;
    /* binding_id: BindingId(Uuid::nil()) */
    key.read_view = read_view;
    return key;
}

void jazz_db__db__coverage_key(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, jazz_protocol_CoverageKey *out) {
    memset(out, 0, sizeof *out);
    out->shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
    out->binding_id = jazz_model__query__Binding__binding_id(binding);
    out->opts = opts;
    out->has_policy_binding = false;
}

void jazz_db__db__request_coverage_key(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, const jazz_db_PolicyBinding *policy_binding, jazz_protocol_CoverageKey *out) {
    jazz_db__db__coverage_key(shape, binding, opts, out);
    if (policy_binding) {
        jz_btree claims;
        jazz_db__db__RowCells__clone(&claims, &policy_binding->claims);
        jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(&out->policy_binding, policy_binding->author, claims);
        out->has_policy_binding = true;
    }
}

bool jazz_db__db__subscriber_permissions_ready(bool permissions_ready, jazz_node_CommitUnitTrust trust) {
    return jazz_node__node__CommitUnitTrust__is_trusted(trust) || permissions_ready;
}

bool jazz_db__db__SubscriptionStream__compiled_authorization_mode(const jazz_db_SubscriptionStream *s, u32 *out) {
    const jazz_db_SubscriptionState *state = s->state; /* _state.borrow() */
    if (!state->kind.maintained_subscription) return false;
    return jazz_node__node__LocalMaintainedViewSubscription__compiled_authorization_mode(state->kind.maintained_subscription, out);
}

/* ---- remaining db.rs items ---- */
bool jazz_db__db__SubscriberShapeRegistration__owns_node_shape(const jazz_db_SubscriberShapeRegistration *r) {
    return r->tag == JAZZ_SSR_REGISTERED || r->tag == JAZZ_SSR_PENDING_CATALOGUE_ADMISSION;
}

/* TickScheduler default methods */
bool jazz_db__db__TickScheduler__query_runtime_waker_default(const void *self, jz_waker *out) { return false; }
bool jazz_db__db__TickScheduler__drops_pending_ticks_default(const void *self) { return false; }

/* Reserve local transaction-clock positions through `high_water` before a
 * trusted native foreground host reuses a node identity. */
jz_err jazz_db__db__Db__reserve_minted_tx_time_after(const jazz_db_Db *db, jazz_types_TxTime high_water) {
    jazz_node_NodeState *ns = jazz_db__db__SharedNodeState__lock(db->node->node);
    jz_err e = jazz_node__node__NodeState__reserve_tx_time_after(ns, high_water);
    jazz_db__db__SharedNodeState__unlock(db->node->node);
    if (JZ_IS_ERR(e)) return jazz_node__node__api_error__Error__from_node_error(e);
    return JZ_OK;
}

/* Return the HLC high-water mark for locally minted transactions. */
jazz_types_TxTime jazz_db__db__Db__minted_tx_time_high_water(const jazz_db_Db *db) {
    jazz_node_NodeState *ns = jazz_db__db__SharedNodeState__lock(db->node->node);
    jazz_types_TxTime t = jazz_node__node__NodeState__tx_time_high_water(ns);
    jazz_db__db__SharedNodeState__unlock(db->node->node);
    return t;
}

const jazz_node_CurrentRow *jazz_db__db__SubscriptionOutputRow__Deref__deref(const jazz_db_SubscriptionOutputRow *r) { return &r->row; }

jz_err jazz_db__db__materialize_subscription_terminal_records(jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index) {
    jazz_types_OutputOccurrenceId *occurrence;
    void *state;
    JZ_BTREE_FOR(&index->terminal_records, occurrence, state) {
        JZ_TRY(jazz_db__db__materialize_subscription_terminal_record(snapshot, index, occurrence));
    }
    return JZ_OK;
}
