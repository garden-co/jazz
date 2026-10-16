/* first_load.c: crates/jazz/layers/db/src/db/first_load.rs
 * The local-first opening gate and the host remote-link hint. */
#include "first_load.h"

/* FirstLoad::requested_wait */
bool jazz_db__db__first_load__FirstLoad__requested_wait(jazz_db_FirstLoad f, u64 *out_ms) {
    if (f.tag == JAZZ_FL_DELIVER || f.timeout_ms == 0) return false;
    *out_ms = f.timeout_ms;
    return true;
}

static bool slice_is(jz_slice v, const char *s) { usize n = strlen(s); return v.len == n && memcmp(v.ptr, s, n) == 0; }

/* Parse the host binding spelling. */
bool jazz_db__db__first_load__RemoteLinkHint__from_host_str(jz_slice value, u32 *out) {
    if (slice_is(value, "none") || slice_is(value, "None") || slice_is(value, "no-server") || slice_is(value, "NoServer")) { *out = JAZZ_RLH_NO_SERVER; return true; }
    if (slice_is(value, "attempting") || slice_is(value, "Attempting") || slice_is(value, "connecting") || slice_is(value, "Connecting")) { *out = JAZZ_RLH_ATTEMPTING; return true; }
    if (slice_is(value, "live") || slice_is(value, "Live") || slice_is(value, "connected") || slice_is(value, "Connected")) { *out = JAZZ_RLH_LIVE; return true; }
    if (slice_is(value, "failed") || slice_is(value, "Failed") || slice_is(value, "unavailable") || slice_is(value, "Unavailable")) { *out = JAZZ_RLH_FAILED; return true; }
    return false;
}

static void tracker_drop(void *p) {
    jazz_db_RemoteLinkTracker *t = p;
    jz_free(t->deadlines.ptr);
    jz_rc_release(t->scheduler);
    jz_free(t->gated.ptr);
    jz_free(t->witnessed.ptr);
    jz_free(t->wakers.ptr);
}

jazz_db_RemoteLinkTracker *RLT_FN(new)(jazz_db_SharedTickScheduler scheduler) {
    jazz_db_RemoteLinkTracker *t = jz_rc_new(sizeof *t, tracker_drop);
    memset(t, 0, sizeof *t);
    t->scheduler = scheduler;
    return t;
}

static jazz_db_RemoteReach RLT_FN(reach)(const jazz_db_RemoteLinkTracker *self) {
    jazz_db_RemoteReach r = { JAZZ_RR_UNREACHABLE, 0 };
    if (self->has_hint) {
        switch (self->hint) {
        case JAZZ_RLH_LIVE: r.tag = JAZZ_RR_LIVE; break;
        case JAZZ_RLH_ATTEMPTING: r.tag = JAZZ_RR_ATTEMPTING; r.since = self->hint_since; break;
        default: r.tag = JAZZ_RR_UNREACHABLE; break;
        }
        return r;
    }
    if (self->live_upstreams > 0) r.tag = JAZZ_RR_LIVE;
    return r;
}

/* Whether a wait armed at `epoch` may continue. */
static bool RLT_FN(may_wait)(const jazz_db_RemoteLinkTracker *self, u64 epoch, bool has_deadline, u64 deadline) {
    if (epoch != self->loss_epoch) return false;
    if (has_deadline && !(jz_now_ms() < deadline)) return false;
    jazz_db_RemoteReach reach = RLT_FN(reach)(self);
    switch (reach.tag) {
    case JAZZ_RR_LIVE: return true;
    case JAZZ_RR_ATTEMPTING: return jz_now_ms() < reach.since + JAZZ_DB_REMOTE_LINK_ATTEMPT_WINDOW_MS;
    default: return false;
    }
}

static void schedule_after(jazz_db_SharedTickScheduler scheduler, u64 ms) {
    if (scheduler && scheduler->has) scheduler->scheduler.vt->schedule_tick_after(scheduler->scheduler.self, ms);
}

static void RLT_FN(schedule_attempt_expiry)(const jazz_db_RemoteLinkTracker *self, u64 since) {
    u64 end = since + JAZZ_DB_REMOTE_LINK_ATTEMPT_WINDOW_MS;
    u64 now = jz_now_ms();
    u64 remaining = end > now ? end - now : 0;
    /* Round up so the tick observes an elapsed window. */
    schedule_after(self->scheduler, remaining + 1);
}

/* Arm a gate if the remote could answer now. */
bool RLT_FN(arm)(jazz_db_RemoteLinkTracker *self, u64 timeout_ms, u64 *epoch, u64 *deadline) {
    u64 e = self->loss_epoch;
    u64 d = jz_now_ms() + timeout_ms;
    if (!RLT_FN(may_wait)(self, e, true, d)) return false;
    jazz_db_RemoteReach reach = RLT_FN(reach)(self);
    if (reach.tag == JAZZ_RR_ATTEMPTING) RLT_FN(schedule_attempt_expiry)(self, reach.since);
    JZ_VEC_PUSH(self->deadlines, d);
    /* Round up so the tick observes an elapsed deadline. */
    schedule_after(self->scheduler, timeout_ms + 1);
    *epoch = e;
    *deadline = d;
    return true;
}

/* Arm a wait bounded only by the remote's reachability. */
bool RLT_FN(arm_until_loss)(jazz_db_RemoteLinkTracker *self, u64 *epoch) {
    u64 e = self->loss_epoch;
    if (!RLT_FN(may_wait)(self, e, false, 0)) return false;
    jazz_db_RemoteReach reach = RLT_FN(reach)(self);
    if (reach.tag == JAZZ_RR_ATTEMPTING) RLT_FN(schedule_attempt_expiry)(self, reach.since);
    *epoch = e;
    return true;
}

static void RLT_FN(notify)(jazz_db_RemoteLinkTracker *self);

void RLT_FN(set_hint)(jazz_db_RemoteLinkTracker *self, u32 hint) {
    bool was_live = RLT_FN(reach)(self).tag == JAZZ_RR_LIVE;
    self->has_hint = true;
    self->hint = hint;
    self->hint_since = hint == JAZZ_RLH_ATTEMPTING ? jz_now_ms() : 0;
    if (was_live && RLT_FN(reach)(self).tag != JAZZ_RR_LIVE) self->loss_epoch += 1; /* wrapping_add */
    if (hint == JAZZ_RLH_ATTEMPTING) {
        self->attempt_expiry_observed = false;
        RLT_FN(schedule_attempt_expiry)(self, self->hint_since);
    }
    RLT_FN(notify)(self);
}

static bool RLT_FN(reported_hint)(const jazz_db_RemoteLinkTracker *self, u32 *out) {
    if (!self->has_hint) return false;
    *out = self->hint;
    return true;
}

void RLT_FN(upstream_attached)(jazz_db_RemoteLinkTracker *self) { self->live_upstreams += 1; }

/* Any own-upstream detach is a link loss for every armed gate. */
void RLT_FN(upstream_detached)(jazz_db_RemoteLinkTracker *self) {
    if (self->live_upstreams > 0) self->live_upstreams -= 1;
    self->loss_epoch += 1;
    RLT_FN(notify)(self);
}

/* Observe an elapsed attempt window or server-wait deadline on the host tick. */
void RLT_FN(on_tick)(jazz_db_RemoteLinkTracker *self) {
    u64 now = jz_now_ms();
    u32 before = self->deadlines.len, w = 0;
    bool has_next = false;
    u64 next_deadline = 0;
    for (u32 i = 0; i < self->deadlines.len; i++) {
        u64 d = self->deadlines.ptr[i];
        if (now < d) {
            self->deadlines.ptr[w++] = d;
            if (!has_next || d < next_deadline) { next_deadline = d; has_next = true; }
        }
    }
    self->deadlines.len = w;
    bool elapsed = w != before;
    jazz_db_RemoteReach reach = RLT_FN(reach)(self);
    if (reach.tag == JAZZ_RR_ATTEMPTING && !self->attempt_expiry_observed) {
        u64 expiry = reach.since + JAZZ_DB_REMOTE_LINK_ATTEMPT_WINDOW_MS;
        if (now >= expiry) {
            self->attempt_expiry_observed = true;
            elapsed = true;
        } else {
            next_deadline = has_next ? (next_deadline < expiry ? next_deadline : expiry) : expiry;
            has_next = true;
        }
    }
    if (elapsed) RLT_FN(notify)(self);
    /* Hosts retain only their earliest timer: re-arm our next obligation. */
    if (has_next && self->scheduler && self->scheduler->has) {
        u64 t = jz_now_ms();
        u64 remaining = next_deadline > t ? next_deadline - t : 0;
        schedule_after(self->scheduler, remaining + 1);
    }
}

/* Register a stream whose opening gate is armed. */
void RLT_FN(register)(jazz_db_RemoteLinkTracker *self, jazz_db_SubscriptionState *state) {
    JZ_VEC_PUSH(self->gated, state); /* Rc::downgrade */
    RLT_FN(notify)(self);
}

/* Release every held opening whose remote can no longer answer, and wake one-shot reads. */
static void RLT_FN(notify)(jazz_db_RemoteLinkTracker *self) {
    jazz_db_WeakStateVec gated = self->gated;
    memset(&self->gated, 0, sizeof self->gated);
    jazz_db_WeakStateVec retained = {0};
    jz_vec_reserve(&retained, gated.len, sizeof(jazz_db_SubscriptionState *));
    bool busy = false;
    for (u32 i = 0; i < gated.len; i++) {
        jazz_db_SubscriptionState *weak = gated.ptr[i];
        jazz_db_SubscriptionState *state = jazz_db_weak_state_upgrade(weak);
        if (!state) continue;
        if (state->borrow_flag < 0) {
            /* Mid-refresh; re-evaluate on the next owner turn. */
            busy = true;
            JZ_VEC_PUSH(retained, weak);
            jz_rc_release(state);
            continue;
        }
        jazz_db_OpeningGate gate;
        if (!jazz_db__db__first_load__SubscriptionSender__opening_gate(&state->sender, &gate)) { jz_rc_release(state); continue; }
        if (RLT_FN(may_wait)(self, gate.epoch, true, gate.deadline)) {
            JZ_VEC_PUSH(retained, weak);
        } else {
            jazz_db__db__first_load__SubscriptionState__release_opening_gate(state);
            /* The next owner turn retires the released witness coverage. */
            busy |= state->authority_witness.len != 0;
        }
        jz_rc_release(state);
    }
    jz_free(gated.ptr);
    jz_vec_extend(&self->gated, retained.ptr, retained.len, sizeof(jazz_db_SubscriptionState *));
    jz_free(retained.ptr);
    if (busy) jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_AFTER_CURRENT_TURN);
    jazz_db_WakerVec wakers = self->wakers;
    memset(&self->wakers, 0, sizeof self->wakers);
    JZ_VEC_FOR(const jz_waker, waker, wakers) jz_waker_wake(waker);
    jz_free(wakers.ptr);
}

bool RLT_FN(has_witnesses)(const jazz_db_RemoteLinkTracker *self) { return self->witnessed.len != 0; }

void RLT_FN(register_witness)(jazz_db_RemoteLinkTracker *self, jazz_db_SubscriptionState *state) {
    JZ_VEC_PUSH(self->witnessed, state);
}

static void jazz_db__db__first_load__SubscriptionSender__answer_witness(const jazz_db_SubscriptionSender *s) {
    if (s->publication->has_opening_gate) s->publication->opening_gate.witness_answered = true;
}

/* Resolve the authority witnesses at the end of an owner turn. */
void RLT_FN(resolve_witnesses)(jazz_db_RemoteLinkTracker *self, bool (*answered)(void *ctx, const jazz_db_UpstreamCoverageHandleVec *handles), void *ctx,
                               jazz_db_UpstreamCoverageHandleVec *retired_out) {
    jazz_db_WeakStateVec witnessed = self->witnessed;
    memset(&self->witnessed, 0, sizeof self->witnessed);
    jazz_db_WeakStateVec retained = {0};
    jz_vec_reserve(&retained, witnessed.len, sizeof(jazz_db_SubscriptionState *));
    jazz_db_UpstreamCoverageHandleVec retired = {0};
    for (u32 i = 0; i < witnessed.len; i++) {
        jazz_db_SubscriptionState *weak = witnessed.ptr[i];
        jazz_db_SubscriptionState *state = jazz_db_weak_state_upgrade(weak);
        if (!state) continue;
        if (state->borrow_flag != 0) { JZ_VEC_PUSH(retained, weak); jz_rc_release(state); continue; }
        if (state->closed->v) { jz_rc_release(state); continue; } /* Stream finalization retires the witness. */
        jazz_db_OpeningGate gate;
        if (jazz_db__db__first_load__SubscriptionSender__opening_gate(&state->sender, &gate) && !gate.witness_answered
            && answered(ctx, &state->authority_witness)) {
            jazz_db__db__first_load__SubscriptionSender__answer_witness(&state->sender);
            if (gate.withheld) jazz_db__db__first_load__SubscriptionState__release_opening_gate(state);
        }
        if (jazz_db__db__first_load__SubscriptionSender__opening_gate(&state->sender, &gate)) {
            JZ_VEC_PUSH(retained, weak);
        } else {
            jz_vec_extend(&retired, state->authority_witness.ptr, state->authority_witness.len, sizeof(jazz_db_UpstreamCoverageHandle));
            state->authority_witness.len = 0; /* append() moves */
        }
        jz_rc_release(state);
    }
    jz_free(witnessed.ptr);
    jz_vec_extend(&self->witnessed, retained.ptr, retained.len, sizeof(jazz_db_SubscriptionState *));
    jz_free(retained.ptr);
    *retired_out = retired;
}

static bool waker_will_wake(const jz_waker *a, const jz_waker *b) { return a->wake == b->wake && a->ctx == b->ctx; }
static void RLT_FN(register_waker)(jazz_db_RemoteLinkTracker *self, const jz_waker *waker) {
    JZ_VEC_FOR(const jz_waker, existing, self->wakers) if (waker_will_wake(existing, waker)) return;
    JZ_VEC_PUSH(self->wakers, *waker);
}

/* RemoteAnswerLoss future: resolves once the remote can no longer answer. */
typedef struct jazz_db_RemoteAnswerLoss { jazz_db_RemoteLinkTracker *tracker; u64 epoch; bool has_deadline; u64 deadline; } jazz_db_RemoteAnswerLoss;
static bool jazz_db__db__first_load__RemoteAnswerLoss__Future__poll(jazz_db_RemoteAnswerLoss *self, const jz_waker *cx) {
    if (!RLT_FN(may_wait)(self->tracker, self->epoch, self->has_deadline, self->deadline)) return true;
    RLT_FN(register_waker)(self->tracker, cx);
    return false;
}

/* ---- OpeningGate ---- */
void jazz_db__db__first_load__OpeningGate__armed(jazz_db_OpeningGate *out, u64 epoch, u64 deadline, u32 route) {
    out->epoch = epoch;
    out->deadline = deadline;
    out->route = route;
    out->withheld = false;
    out->witnessed = false;
    out->witness_answered = false;
}
/* Still waiting for the answer that may release a withheld opening. */
bool jazz_db__db__first_load__OpeningGate__awaits_answer(const jazz_db_OpeningGate *g, bool settled) {
    if (g->witnessed) return !g->witness_answered;
    return !settled;
}

bool jazz_db__db__first_load__SubscriptionSender__opening_gate(const jazz_db_SubscriptionSender *s, jazz_db_OpeningGate *out) {
    if (!s->publication->has_opening_gate) return false;
    *out = s->publication->opening_gate;
    return true;
}

/* Deliver a subscription rejection; a withheld local-first opening is published first. */
bool jazz_db__db__first_load__SubscriptionState__send_rejection(const jazz_db_SubscriptionState *state, jazz_db_SubscriptionEvent event) {
    jazz_db_OpeningGate gate;
    if (jazz_db__db__first_load__SubscriptionSender__opening_gate(&state->sender, &gate) && gate.route == JAZZ_OR_LOCAL_FIRST)
        jazz_db__db__first_load__SubscriptionState__release_opening_gate(state);
    return jazz_db__db__SubscriptionSender__unbounded_send(&state->sender, event);
}

static void raw_send(jz_chan *chan, jazz_db_SubscriptionEvent *event) {
    if (!chan || jz_chan_is_closed(chan)) { jazz_db__db__SubscriptionEvent__drop(event); return; }
    jz_chan_send(chan, event);
}

/* Release an armed opening gate from outside the publication path. */
void jazz_db__db__first_load__SubscriptionState__release_opening_gate(const jazz_db_SubscriptionState *state) {
    jazz_db_SubscriptionPublication *publication = state->sender.publication;
    if (!publication->has_opening_gate) return;
    jazz_db_OpeningGate gate = publication->opening_gate;
    publication->has_opening_gate = false;
    if (gate.route == JAZZ_OR_REMOTE_WINDOW) {
        if (!publication->opened) {
            publication->window_fell_back = true;
            /* Wake the consumer so the stream switches to its fallback. */
            jazz_db_SubscriptionEvent wake = jazz_db__db__SubscriptionEvent__delta(false, state->read_tier, false);
            wake.publishable = false;
            raw_send(state->sender.sender, &wake);
        }
        return;
    }
    /* Without a withheld opening, ordinary local-first publication delivers the first result. */
    if (publication->opened || !gate.withheld || publication->unresolved.len != 0) return;
    jazz_node_RelationSnapshot snapshot;
    if (JZ_IS_ERR(jazz_db__db__materialized_subscription_snapshot(&state->snapshot, &state->snapshot_index, &snapshot))) return;
    jazz_db_OccurrenceVec occurrences = {0};
    jz_err e = jazz_db__db__snapshot_root_occurrences(&state->snapshot, &state->snapshot_index, &occurrences);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); jazz_node__node__RelationSnapshot__drop(&snapshot); return; }
    jazz_db_OutputRowVec added = {0};
    e = jazz_db__db__subscription_outputs_with_occurrence_sidecar(&snapshot, occurrences.ptr, occurrences.len, &added);
    jazz_node__node__RelationSnapshot__drop(&snapshot);
    jz_vec_drop(&occurrences, sizeof(jazz_types_OutputOccurrenceId), jazz_types__object__OutputOccurrenceId__drop);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return; }
    /* Keep any pending canonical-reset request. */
    publication->opened = true;
    if (publication->has_deferred) { jazz_db__db__SubscriptionPublicationSnapshot__drop(&publication->deferred); publication->has_deferred = false; }
    jazz_db_SubscriptionEvent event = jazz_db__db__SubscriptionEvent__delta(true, state->read_tier, state->settled);
    event.added = added;
    raw_send(state->sender.sender, &event);
}

/* ---- impl Db ---- */
void jazz_db__db__first_load__Db__set_remote_link_hint(const jazz_db_Db *db, u32 hint) { RLT_FN(set_hint)(db->node->remote_link, hint); }
bool jazz_db__db__first_load__Db__remote_link_hint_for_test(const jazz_db_Db *db, u32 *out) { return RLT_FN(reported_hint)(db->node->remote_link, out); }

/* Resolve a FirstLoad::WaitForRemote subscription request. Returns the coverage tier. */
u32 jazz_db__db__first_load__Db__resolve_first_load(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, const jazz_db_ReadOpts *opts,
                                                   u32 authorization_mode, u32 coverage_tier, bool *has_gate, jazz_db_OpeningGate *gate) {
    *has_gate = false;
    u64 timeout;
    if (!jazz_db__db__first_load__FirstLoad__requested_wait(opts->first_load, &timeout)) return coverage_tier;
    if (authorization_mode != JAZZ_QAM_CLIENT_LOCAL || opts->tier == JAZZ_DB_READ_LOCAL_ONLY || coverage_tier != JAZZ_DUR_LOCAL) return coverage_tier;
    u64 epoch, deadline;
    if (!RLT_FN(arm)(db->node->remote_link, timeout, &epoch, &deadline)) return coverage_tier;
    *has_gate = true;
    if (jazz_model__query__Query__has_offset(jazz_model__query__ValidatedQuery__query(jazz_db__db__PreparedQuery__shape(prepared)))) {
        jazz_db__db__first_load__OpeningGate__armed(gate, epoch, deadline, JAZZ_OR_REMOTE_WINDOW);
        return JAZZ_DUR_GLOBAL;
    }
    jazz_db__db__first_load__OpeningGate__armed(gate, epoch, deadline, JAZZ_OR_LOCAL_FIRST);
    return coverage_tier;
}

void jazz_db__db__first_load__Db__register_opening_gate(const jazz_db_Db *db, jazz_db_SubscriptionState *state) {
    if (state->authority_witness.len != 0) RLT_FN(register_witness)(db->node->remote_link, state);
    RLT_FN(register)(db->node->remote_link, state);
}

/* Poll `remote` until it completes, the remote can no longer answer, or the deadline passes. */
bool jazz_db__db__first_load__Db__race_remote_answer(const jazz_db_Db *db, u64 epoch, bool has_deadline, u64 deadline, jazz_db_LocalFuture remote, void *out) {
    jazz_db_RemoteAnswerLoss loss = { jz_rc_clone(db->node->remote_link), epoch, has_deadline, deadline };
    static const jz_waker noop = { 0 };
    bool result = false;
    for (;;) {
        if (remote.poll(remote.self, &noop, out)) { result = true; break; }
        if (jazz_db__db__first_load__RemoteAnswerLoss__Future__poll(&loss, &noop)) { result = false; break; }
        jz_task_run_ready();
    }
    jazz_db__db__LocalFuture__drop(&remote);
    jz_rc_release(loss.tracker);
    return result;
}

/* One-shot FirstLoad::WaitForRemote read: local first with a server-wait timeout. */
jz_err jazz_db__db__first_load__Db__read_local_first_within(const jazz_db_Db *db, u64 timeout_ms,
                                                           jazz_db_LocalFuture (*local)(void *ctx), jazz_db_LocalFuture (*remote)(void *ctx), void *ctx) {
    u64 epoch, deadline;
    if (timeout_ms != 0 && RLT_FN(arm)(db->node->remote_link, timeout_ms, &epoch, &deadline)) {
        jz_err result = JZ_OK;
        if (jazz_db__db__first_load__Db__race_remote_answer(db, epoch, true, deadline, remote(ctx), &result)) {
            if (!JZ_IS_ERR(result)) return JZ_OK;
            jz_err_drop(&result);
        }
    }
    jazz_db_LocalFuture f = local(ctx);
    jz_err result = JZ_OK;
    jazz_db__db__LocalFuture__block_on(&f, &result);
    jazz_db__db__LocalFuture__drop(&f);
    return result;
}
