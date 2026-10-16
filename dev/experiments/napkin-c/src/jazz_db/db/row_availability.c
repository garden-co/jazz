/* row_availability.c: crates/jazz/layers/db/src/db/row_availability.rs
 * Bounded, ephemeral known-row routing. */
#include "row_availability.h"

static int request_id_cmp(const void *a, const void *b, void *ctx) { (void)ctx; return memcmp(a, b, 16); }
static int u64c(const void *a, const void *b, void *ctx) { return jz_cmp_u64(a, b, ctx); }
static void route_drop(void *p) {
    jazz_db_CurrentRowsRoute *r = p;
    jazz_protocol__protocol__CurrentRowsRequest__drop(&r->request);
    jazz_protocol__protocol__PolicyBindingKey__drop(&r->context);
    if (r->sender) jz_chan_close(r->sender); /* dropping the oneshot sender */
}
static void receipt_deque_drop(void *p) { jz_deque_drop(p, jazz_protocol__protocol__CurrentRowsReceipt__drop); }
static int floor_key_cmp(const void *a, const void *b, void *ctx) {
    const jazz_db_FloorKey *x = a, *y = b;
    int c = jz_uuid_cmp(&x->core, &y->core);
    if (c) return c;
    if (x->epoch != y->epoch) return x->epoch < y->epoch ? -1 : 1;
    return jazz_protocol__protocol__PolicyBindingKey__cmp(&x->context, &y->context, ctx);
}
static void floor_key_drop(void *p) { jazz_protocol__protocol__PolicyBindingKey__drop(&((jazz_db_FloorKey *)p)->context); }
static void floor_drop(void *p) { jz_btree_drop(&((jazz_db_CurrentRowsFloor *)p)->protected_requests); }
static const jz_btree_desc routes_desc = { 16, sizeof(jazz_db_CurrentRowsRoute), request_id_cmp, NULL, route_drop };
static const jz_btree_desc responses_desc = { sizeof(u64), sizeof(jz_deque), u64c, NULL, receipt_deque_drop };
static const jz_btree_desc progress_desc = { sizeof(u64), sizeof(u64), u64c, NULL, NULL };
static const jz_btree_desc floors_desc = { sizeof(jazz_db_FloorKey), sizeof(jazz_db_CurrentRowsFloor), floor_key_cmp, floor_key_drop, floor_drop };
static const jz_btree_desc request_id_set_desc = { 16, 0, request_id_cmp, NULL, NULL };

void CRR_FN(default)(jazz_db_CurrentRowsRouter *out) {
    jz_btree_init(&out->routes, &routes_desc);
    jz_btree_init(&out->responses, &responses_desc);
    jz_deque_init(&out->cancels, sizeof(jazz_db_RouteCancel));
    jz_btree_init(&out->progress, &progress_desc);
    jz_btree_init(&out->floors, &floors_desc);
}

bool CRR_FN(admit)(jazz_db_CurrentRowsRouter *self, jazz_db_CurrentRowsRoute route) {
    usize queued = 0;
    u64 *k;
    jz_deque *q;
    JZ_BTREE_FOR(&self->responses, k, q) queued += q->len;
    if (self->routes.len + self->cancels.len + queued >= JAZZ_DB_CURRENT_ROWS_MAX_PENDING) {
        route_drop(&route);
        return false;
    }
    jazz_protocol_PermissionAdviceRequestId id = route.request.request_id;
    jz_btree_insert(&self->routes, &id, &route, NULL);
    return true;
}

void jazz_db__db__row_availability__CurrentRowsResult__drop(jazz_db_CurrentRowsResult *r) {
    if (r->tag == JAZZ_CRR_APPLIED) jazz_protocol__protocol__CurrentRowsReceipt__drop(&r->applied);
    r->tag = JAZZ_CRR_UNKNOWN;
}

void CRR_FN(finish)(jazz_db_CurrentRowsRouter *self, jazz_protocol_PermissionAdviceRequestId id, bool has_receipt, jazz_protocol_CurrentRowsReceipt receipt) {
    jazz_db_CurrentRowsRoute route;
    if (!jz_btree_remove(&self->routes, &id, NULL, &route)) {
        if (has_receipt) jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt);
        return;
    }
    if (route.has_downstream) {
        jazz_protocol_CurrentRowsReceipt r;
        if (has_receipt) r = receipt;
        else {
            jazz_protocol_PolicyBindingKey ctx;
            jazz_protocol__protocol__PolicyBindingKey__clone(&ctx, &route.context);
            jazz_db__db__row_availability__unknown_receipt(&route.request, ctx, &r);
        }
        r.request_id = route.downstream_id;
        bool inserted;
        jz_deque *queue = jz_btree_entry(&self->responses, &route.downstream_epoch, &inserted);
        if (inserted) jz_deque_init(queue, sizeof(jazz_protocol_CurrentRowsReceipt));
        jz_deque_push_back(queue, &r);
    } else if (route.sender) {
        jazz_db_CurrentRowsResult result;
        memset(&result, 0, sizeof result);
        if (has_receipt) { result.tag = JAZZ_CRR_APPLIED; result.applied = receipt; }
        else result.tag = JAZZ_CRR_UNKNOWN;
        if (jz_chan_is_closed(route.sender)) jazz_db__db__row_availability__CurrentRowsResult__drop(&result);
        else jz_chan_send(route.sender, &result);
        jz_chan_close(route.sender);
        route.sender = NULL;
    } else if (has_receipt) {
        jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt);
    }
    route_drop(&route);
}

typedef JZ_VEC(jazz_protocol_PermissionAdviceRequestId) RequestIdVec;

void CRR_FN(cancel_downstream)(jazz_db_CurrentRowsRouter *self, u64 epoch, jazz_protocol_PermissionAdviceRequestId id) {
    RequestIdVec ids = {0};
    jazz_protocol_PermissionAdviceRequestId *key;
    jazz_db_CurrentRowsRoute *route;
    JZ_BTREE_FOR(&self->routes, key, route) {
        if (route->has_downstream && route->downstream_epoch == epoch && memcmp(route->downstream_id.b, id.b, 16) == 0) JZ_VEC_PUSH(ids, *key);
    }
    JZ_VEC_FOR(const jazz_protocol_PermissionAdviceRequestId, k, ids) {
        jazz_db_CurrentRowsRoute removed;
        if (jz_btree_remove(&self->routes, k, NULL, &removed)) {
            if (removed.has_upstream) {
                jazz_db_RouteCancel c = { removed.upstream, *k };
                jz_deque_push_back(&self->cancels, &c);
            }
            route_drop(&removed);
        }
    }
    jz_free(ids.ptr);
}

void CRR_FN(disconnect)(jazz_db_CurrentRowsRouter *self, u64 epoch) {
    RequestIdVec ids = {0};
    jazz_protocol_PermissionAdviceRequestId *key;
    jazz_db_CurrentRowsRoute *route;
    JZ_BTREE_FOR(&self->routes, key, route) {
        if ((route->has_upstream && route->upstream.connection_id == epoch) || (route->has_downstream && route->downstream_epoch == epoch))
            JZ_VEC_PUSH(ids, *key);
    }
    JZ_VEC_FOR(const jazz_protocol_PermissionAdviceRequestId, id, ids) {
        jazz_db_CurrentRowsRoute *r = jz_btree_get(&self->routes, id);
        if (r && r->has_downstream && r->downstream_epoch == epoch) {
            jazz_db_CurrentRowsRoute removed;
            if (jz_btree_remove(&self->routes, id, NULL, &removed)) {
                if (removed.has_upstream) {
                    jazz_db_RouteCancel c = { removed.upstream, *id };
                    jz_deque_push_back(&self->cancels, &c);
                }
                route_drop(&removed);
            }
        } else {
            jazz_protocol_CurrentRowsReceipt none;
            memset(&none, 0, sizeof none);
            CRR_FN(finish)(self, *id, false, none);
        }
    }
    jz_free(ids.ptr);
    jz_btree_remove(&self->responses, &epoch, NULL, NULL);
    jz_btree_remove(&self->progress, &epoch, NULL, NULL);
    /* cancels.retain(|(upstream, _)| upstream.connection_id != epoch) */
    jz_deque kept;
    jz_deque_init(&kept, sizeof(jazz_db_RouteCancel));
    jazz_db_RouteCancel c;
    while (jz_deque_pop_front(&self->cancels, &c)) if (c.upstream.connection_id != epoch) jz_deque_push_back(&kept, &c);
    jz_deque_drop(&self->cancels, NULL);
    self->cancels = kept;
    /* Retain monotonic floors. */
}

bool jazz_db__db__row_availability__valid_request(const jazz_protocol_CurrentRowsRequest *request) {
    if (request->rows.len == 0 || request->rows.len > JAZZ_PROTOCOL_MAX_CURRENT_ROWS) return false;
    JZ_VEC_FOR(const jazz_protocol_CurrentRowCoordinate, row, request->rows)
        if (row->table.len == 0 || row->table.len > 256) return false;
    for (u32 index = 0; index < request->rows.len; index++)
        for (u32 j = 0; j < index; j++)
            if (jazz_protocol__protocol__CurrentRowCoordinate__eq(&request->rows.ptr[j], &request->rows.ptr[index])) return false;
    return true;
}

void jazz_db__db__row_availability__unknown_receipt(const jazz_protocol_CurrentRowsRequest *request, jazz_protocol_PolicyBindingKey context, jazz_protocol_CurrentRowsReceipt *out) {
    memset(out, 0, sizeof *out);
    out->request_id = request->request_id;
    jz_vec_clone(&out->rows, &request->rows, sizeof(jazz_protocol_CurrentRowCoordinate), jazz_protocol__protocol__CurrentRowCoordinate__clone);
    for (u32 i = 0; i < request->rows.len; i++) { u32 u = JAZZ_CRO_UNKNOWN; JZ_VEC_PUSH(out->outcomes, u); }
    out->context = context;
    memset(&out->core, 0, sizeof out->core); /* NodeUuid(Uuid::nil()) */
    out->core_epoch = 0;
    out->claims_revision = 0;
    out->policy_epoch = 0;
    out->settled_through = 0;
    out->authorization_progress = 0;
}

/* Internal pilot hook. */
jazz_db_LocalFuture jazz_db__db__row_availability__Node__request_current_rows(const jazz_db_Node *self, jazz_db_CoordinateVec rows, jazz_protocol_PolicyBindingKey context) {
    jazz_db_HeldAuthorityCell *held = self->admitted_upstream_authority;
    return jazz_db__db__row_availability__request_current_rows_for_owner(jz_rc_clone(self->current_rows), jz_rc_clone(self->scheduler),
                                                                         held && held->has, rows, context);
}

/* CurrentRowsGuard: cancels the route when the future completes or is dropped. */
typedef struct CurrentRowsGuard { jazz_protocol_PermissionAdviceRequestId id; jazz_db_SharedCurrentRows router; jazz_db_SharedTickScheduler scheduler; } CurrentRowsGuard;
static void jazz_db__db__row_availability__CurrentRowsGuard__Drop__drop(CurrentRowsGuard *g) {
    jazz_db_CurrentRowsRoute route;
    if (jz_btree_remove(&g->router->routes, &g->id, NULL, &route)) {
        if (route.has_upstream) {
            jazz_db_RouteCancel c = { route.upstream, g->id };
            jz_deque_push_back(&g->router->cancels, &c);
        }
        route_drop(&route);
    }
    jazz_db__db__peer_connection__schedule_tick_in(g->scheduler, JAZZ_TICK_IMMEDIATE);
    jz_rc_release(g->router);
    jz_rc_release(g->scheduler);
}

typedef struct CurrentRowsFuture { jz_chan *receiver; CurrentRowsGuard guard; bool guard_live; } CurrentRowsFuture;
static bool current_rows_future_poll(void *p, const jz_waker *cx, void *out) {
    (void)cx;
    CurrentRowsFuture *f = p;
    jazz_db_CurrentRowsResult *result = out;
    if (jz_chan_try_recv(f->receiver, result)) {
    } else if (jz_chan_is_closed(f->receiver)) {
        memset(result, 0, sizeof *result);
        result->tag = JAZZ_CRR_UNKNOWN; /* receiver.await.unwrap_or(Unknown) */
    } else {
        return false;
    }
    if (f->guard_live) { jazz_db__db__row_availability__CurrentRowsGuard__Drop__drop(&f->guard); f->guard_live = false; }
    return true;
}
static void current_rows_future_drop(void *p) {
    CurrentRowsFuture *f = p;
    if (f->guard_live) jazz_db__db__row_availability__CurrentRowsGuard__Drop__drop(&f->guard);
    jz_chan_release(f->receiver);
    jz_free(f);
}

/* Owned requester shared by local operations and relay repair owners. */
jazz_db_LocalFuture jazz_db__db__row_availability__request_current_rows_for_owner(jazz_db_SharedCurrentRows router, jazz_db_SharedTickScheduler scheduler, bool has_upstream,
                                                                                  jazz_db_CoordinateVec rows, jazz_protocol_PolicyBindingKey context) {
    jz_uuid u = jz_uuid_v4();
    jazz_protocol_PermissionAdviceRequestId id;
    memcpy(id.b, u.b, 16);
    jz_chan *channel = jz_chan_new(sizeof(jazz_db_CurrentRowsResult)); /* oneshot::channel() */
    jazz_protocol_CurrentRowsRequest request;
    memset(&request, 0, sizeof request);
    request.request_id = id;
    request.rows.ptr = rows.ptr; request.rows.len = rows.len; request.rows.cap = rows.cap;
    request.has_delegated_session = false;
    if (jazz_db__db__row_availability__valid_request(&request) && has_upstream) {
        jazz_db_CurrentRowsRoute route;
        memset(&route, 0, sizeof route);
        route.request = request;
        route.context = context;
        route.has_upstream = false;
        route.has_downstream = false;
        route.sender = channel;
        (void)CRR_FN(admit)(router, route);
    } else {
        jazz_protocol__protocol__CurrentRowsRequest__drop(&request);
        jazz_protocol__protocol__PolicyBindingKey__drop(&context);
        jz_chan_close(channel); /* the unused sender is dropped */
    }
    jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_IMMEDIATE);
    CurrentRowsFuture *f = JZ_NEW(CurrentRowsFuture);
    f->receiver = channel;
    f->guard.id = id;
    f->guard.router = jz_rc_clone(router);
    f->guard.scheduler = jz_rc_clone(scheduler);
    f->guard_live = true;
    jz_rc_release(router);
    jz_rc_release(scheduler);
    return (jazz_db_LocalFuture){ f, current_rows_future_poll, current_rows_future_drop };
}

/* ---- impl PeerConnection ---- */
static jz_err jazz_db__db__row_availability__PeerConnection__send_current_rows_message(jazz_db_PeerConnection *self, jazz_protocol_SyncMessage message, bool *out) {
    jz_err error = self->transport.vt->send(self->transport.self, message);
    if (!JZ_IS_ERR(error)) { *out = true; return JZ_OK; }
    if (jazz_db__db__peer_connection__handle_transport_backpressure(self->node, self->scheduler, &error)) {
        jz_err_drop(&error);
        *out = false;
        return JZ_OK;
    }
    return jazz_db__db__peer_connection__transport_error(error);
}

static jz_err jazz_db__db__row_availability__PeerConnection__send_current_rows(jazz_db_PeerConnection *self, jazz_protocol_SyncMessage message, bool *out) {
    /* Requests on an upstream link go after the read barrier naming the writes sent before them. */
    if (self->link.tag == JAZZ_LINK_UPSTREAM) {
        jazz_protocol_SyncMessage barrier;
        if (jazz_db__db__read_barrier__ReadBarrierSender__pending_barrier(&self->link.v.upstream.read_barrier, &barrier)) {
            bool sent = false;
            jz_err e = jazz_db__db__row_availability__PeerConnection__send_current_rows_message(self, barrier, &sent);
            if (JZ_IS_ERR(e)) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
            if (!sent) { jazz_protocol__protocol__SyncMessage__drop(&message); *out = false; return JZ_OK; }
            if (self->link.tag != JAZZ_LINK_UPSTREAM) jz_panic("link kind is fixed for a connection");
            jazz_db__db__read_barrier__ReadBarrierSender__barrier_accepted(&self->link.v.upstream.read_barrier);
        }
    }
    return jazz_db__db__row_availability__PeerConnection__send_current_rows_message(self, message, out);
}

jz_err jazz_db__db__row_availability__PeerConnection__pump_current_rows(jazz_db_PeerConnection *self) {
    jazz_db_CurrentRowsRouter *router = self->current_rows;
    if (self->link.tag == JAZZ_LINK_SUBSCRIBER) {
        for (;;) {
            jz_deque *queue = jz_btree_get(&router->responses, &self->connection_epoch);
            if (!queue || queue->len == 0) break;
            jazz_protocol_CurrentRowsReceipt response;
            jazz_protocol__protocol__CurrentRowsReceipt__clone(&response, jz_deque_at(queue, 0));
            jazz_protocol_SyncMessage m;
            memset(&m, 0, sizeof m);
            m.tag = JAZZ_SM_CURRENT_ROWS_RECEIPT;
            m.v.current_rows_receipt = response;
            jazz_db__db__peer_connection__queue_sync_context_control(&self->pending_control_responses, m);
            queue = jz_btree_get(&router->responses, &self->connection_epoch);
            if (!queue) jz_panic("called `Option::unwrap()` on a `None` value");
            jazz_protocol_CurrentRowsReceipt popped;
            if (jz_deque_pop_front(queue, &popped)) jazz_protocol__protocol__CurrentRowsReceipt__drop(&popped);
        }
        return JZ_OK;
    }
    if (self->link.tag != JAZZ_LINK_UPSTREAM) return JZ_OK;
    if (!self->link.v.upstream.has_expected_scope_authority) return JZ_OK;
    jazz_protocol_AuthorityContext expected = self->link.v.upstream.expected_scope_authority;
    if (!(self->admitted_upstream_authority->has
          && jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(self->admitted_upstream_authority->ctx, expected)))
        return JZ_OK;
    RequestIdVec stale = {0};
    jazz_protocol_PermissionAdviceRequestId *key;
    jazz_db_CurrentRowsRoute *route;
    JZ_BTREE_FOR(&router->routes, key, route) {
        if (route->has_upstream && !jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(route->upstream, expected)) JZ_VEC_PUSH(stale, *key);
    }
    jazz_protocol_CurrentRowsReceipt none;
    memset(&none, 0, sizeof none);
    JZ_VEC_FOR(const jazz_protocol_PermissionAdviceRequestId, id, stale) CRR_FN(finish)(router, *id, false, none);
    jz_free(stale.ptr);
    RequestIdVec ids = {0};
    JZ_BTREE_FOR(&router->routes, key, route) if (!route->has_upstream) JZ_VEC_PUSH(ids, *key);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < ids.len; i++) {
        jazz_protocol_PermissionAdviceRequestId id = ids.ptr[i];
        jazz_db_CurrentRowsRoute *r = jz_btree_get(&router->routes, &id);
        if (!r) jz_panic("called `Option::unwrap()` on a `None` value");
        jazz_protocol_CurrentRowsRequest request;
        jazz_protocol__protocol__CurrentRowsRequest__clone(&request, &r->request);
        jazz_node_NodeState *ns = jazz_db__db__LocalMutexBorrow__borrow(self->node, "pump_current_rows");
        const jazz_node_ClientRelayScope *scope = jazz_node__node__NodeState__client_relay_scope(ns);
        bool scoped_session = scope && jazz_node__node__relay_scope__ClientRelayScope__admits_session(scope, r->context.identity);
        if (self->transport.vt->permits_delegated_sessions(self->transport.self) || scoped_session) {
            request.has_delegated_session = true;
            request.delegated_session.identity = r->context.identity;
            jazz_db__db__RowCells__clone(&request.delegated_session.claims, jazz_protocol__protocol__PolicyBindingKey__claims(&r->context));
        } else if (r->has_downstream || !jazz_types_AuthorSubject_eq(&r->context.identity, &expected.link)) {
            jazz_protocol__protocol__CurrentRowsRequest__drop(&request);
            CRR_FN(finish)(router, id, false, none);
            continue;
        }
        jazz_protocol_SyncMessage m;
        memset(&m, 0, sizeof m);
        m.tag = JAZZ_SM_CURRENT_ROWS_REQUEST;
        m.v.current_rows_request = request;
        bool sent = false;
        e = jazz_db__db__row_availability__PeerConnection__send_current_rows(self, m, &sent);
        if (JZ_IS_ERR(e) || !sent) { jz_free(ids.ptr); return e; }
        jazz_db_CurrentRowsRoute *after = jz_btree_get(&router->routes, &id);
        if (after) { after->has_upstream = true; after->upstream = expected; }
    }
    jz_free(ids.ptr);
    for (;;) {
        bool found = false;
        u32 index = 0;
        for (u32 i = 0; i < router->cancels.len; i++) {
            const jazz_db_RouteCancel *c = jz_deque_at(&router->cancels, i);
            if (jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(c->upstream, expected)) { found = true; index = i; break; }
        }
        if (!found) break;
        jazz_protocol_PermissionAdviceRequestId id = ((const jazz_db_RouteCancel *)jz_deque_at(&router->cancels, index))->id;
        jazz_protocol_SyncMessage m;
        memset(&m, 0, sizeof m);
        m.tag = JAZZ_SM_CURRENT_ROWS_CANCEL;
        m.v.current_rows_cancel.request_id = id;
        bool sent = false;
        JZ_TRY(jazz_db__db__row_availability__PeerConnection__send_current_rows(self, m, &sent));
        if (!sent) return JZ_OK;
        /* cancels.remove(index) */
        jz_deque kept;
        jz_deque_init(&kept, sizeof(jazz_db_RouteCancel));
        jazz_db_RouteCancel c;
        u32 at = 0;
        while (jz_deque_pop_front(&router->cancels, &c)) { if (at++ != index) jz_deque_push_back(&kept, &c); }
        jz_deque_drop(&router->cancels, NULL);
        router->cancels = kept;
    }
    return JZ_OK;
}

typedef struct FloorRetainCtx { const jz_btree *live; } FloorRetainCtx;
static bool floor_retain(const void *k, void *v, void *ctx) {
    (void)k;
    const jazz_db_CurrentRowsFloor *floor = v;
    const jz_btree *live = ((FloorRetainCtx *)ctx)->live;
    jazz_protocol_PermissionAdviceRequestId *request;
    void *unit;
    JZ_BTREE_FOR(&floor->protected_requests, request, unit) if (jz_btree_contains(live, request)) return true;
    return false;
}

static void finish_none(jazz_db_SharedCurrentRows router, jazz_protocol_PermissionAdviceRequestId id) {
    jazz_protocol_CurrentRowsReceipt none;
    memset(&none, 0, sizeof none);
    CRR_FN(finish)(router, id, false, none);
}

static bool coordinates_eq(const jazz_protocol_CurrentRowCoordinate *a, u32 na, const jazz_protocol_CurrentRowCoordinate *b, u32 nb) {
    if (na != nb) return false;
    for (u32 i = 0; i < na; i++) if (!jazz_protocol__protocol__CurrentRowCoordinate__eq(&a[i], &b[i])) return false;
    return true;
}

jz_err jazz_db__db__row_availability__receive_current_rows(jazz_db_SharedNodeState *node, jazz_db_SharedCurrentRows router, const jazz_protocol_AuthorityContext *selected,
                                                          const jazz_protocol_AuthorityContext *expected, bool eligible, jazz_protocol_CurrentRowsReceipt receipt) {
    if (!expected) { jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt); return JZ_OK; }
    jazz_protocol_PermissionAdviceRequestId id = receipt.request_id;
    bool valid = false;
    {
        const jazz_db_CurrentRowsRoute *route = jz_btree_get(&router->routes, &id);
        valid = route && eligible
            && selected && jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(*selected, *expected)
            && route->has_upstream && jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(route->upstream, *expected)
            && coordinates_eq(receipt.rows.ptr, receipt.rows.len, route->request.rows.ptr, route->request.rows.len)
            && jazz_protocol__protocol__PolicyBindingKey__eq(&receipt.context, &route->context)
            && receipt.outcomes.len == receipt.rows.len;
    }
    if (!valid) { jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt); return JZ_OK; }
    bool all_unknown = true;
    JZ_VEC_FOR(const u32, outcome, receipt.outcomes) if (*outcome != JAZZ_CRO_UNKNOWN) { all_unknown = false; break; }
    if (all_unknown) {
        if (receipt.version_carriers.len == 0) finish_none(router, id);
        jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt);
        return JZ_OK;
    }
    static const jz_uuid nil = { { 0 } };
    if (jz_uuid_eq(&receipt.core, &nil) || receipt.core_epoch == 0 || receipt.authorization_progress == 0) {
        /* Unusable evidence answers nothing: release the route now so its owner retries. */
        finish_none(router, id);
        jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt);
        return JZ_OK;
    }
    jazz_db_FloorKey key;
    key.core = receipt.core;
    key.epoch = receipt.core_epoch;
    jazz_protocol__protocol__PolicyBindingKey__clone(&key.context, &receipt.context);
    const jazz_db_CurrentRowsFloor *floor = jz_btree_get(&router->floors, &key);
    if (floor && (receipt.settled_through < floor->cut || receipt.authorization_progress < floor->progress || receipt.policy_epoch < floor->policy)) {
        finish_none(router, id);
        floor_key_drop(&key);
        jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt);
        return JZ_OK;
    }
    if (router->floors.len >= JAZZ_DB_CURRENT_ROWS_MAX_PENDING && !jz_btree_contains(&router->floors, &key)) {
        jz_btree live;
        jz_btree_init(&live, &request_id_set_desc);
        jazz_protocol_PermissionAdviceRequestId *rk;
        jazz_db_CurrentRowsRoute *rv;
        JZ_BTREE_FOR(&router->routes, rk, rv) jz_btree_insert(&live, rk, NULL, NULL);
        FloorRetainCtx ctx = { &live };
        jz_btree_retain(&router->floors, floor_retain, &ctx);
        jz_btree_drop(&live);
        if (router->floors.len >= JAZZ_DB_CURRENT_ROWS_MAX_PENDING) {
            finish_none(router, id);
            floor_key_drop(&key);
            jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt);
            return JZ_OK;
        }
    }
    {
        jazz_node_NodeState *ns = jazz_db__db__SharedNodeState__lock(node);
        bool mismatch = false;
        JZ_VEC_FOR(const jazz_protocol_CurrentRowCoordinate, row, receipt.rows) {
            jazz_protocol_CurrentRowCoordinate current;
            jz_err ce = jazz_node__node__NodeState__current_row_coordinate(ns, jz_str_slice(&row->table), row->row, &current);
            if (JZ_IS_ERR(ce)) { jz_err_drop(&ce); mismatch = true; break; }
            bool same = jazz_protocol__protocol__CurrentRowCoordinate__eq(&current, row);
            jazz_protocol__protocol__CurrentRowCoordinate__drop(&current);
            if (!same) { mismatch = true; break; }
        }
        if (mismatch
            || jazz_node__node__NodeState__active_catalogue_seq(ns) > receipt.policy_epoch
            || jazz_node__node__NodeState__committed_global_time(ns) > receipt.settled_through) {
            jazz_db__db__SharedNodeState__unlock(node);
            finish_none(router, id);
            floor_key_drop(&key);
            jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt);
            return JZ_OK;
        }
        jz_err e = jazz_node__node__NodeState__ingest_current_rows_receipt(ns, &receipt);
        jazz_db__db__SharedNodeState__unlock(node);
        if (JZ_IS_ERR(e)) {
            floor_key_drop(&key);
            jazz_protocol__protocol__CurrentRowsReceipt__drop(&receipt);
            return jazz_node__node__api_error__Error__from_node_error(e);
        }
    }
    /* At most one floor per outstanding route budget. */
    jazz_db_CurrentRowsFloor fresh;
    fresh.cut = receipt.settled_through;
    fresh.progress = receipt.authorization_progress;
    fresh.policy = receipt.policy_epoch;
    jz_btree_init(&fresh.protected_requests, &request_id_set_desc);
    jazz_protocol_PermissionAdviceRequestId *rk;
    jazz_db_CurrentRowsRoute *rv;
    JZ_BTREE_FOR(&router->routes, rk, rv) {
        if (memcmp(rk->b, id.b, 16) != 0 && jazz_protocol__protocol__PolicyBindingKey__eq(&rv->context, &receipt.context)
            && rv->has_upstream && jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(rv->upstream, *expected))
            jz_btree_insert(&fresh.protected_requests, rk, NULL, NULL);
    }
    jz_btree_insert(&router->floors, &key, &fresh, NULL);
    CRR_FN(finish)(router, id, true, receipt);
    return JZ_OK;
}
