/* node_runtime.c: port of crates/jazz/layers/db/src/db/node_runtime.rs
 *
 * Node-owned orchestration for upstream and subscriber connections.
 *
 * The node runtime owns shared connection state, scheduling, pending uploads,
 * subscription refresh, write-state notification, and connection lifecycle.
 *
 * Async functions are synchronous: `x.lock().await` is jazz_db_node_lock /
 * jazz_db_node_unlock, boxed futures are jazz_db_LocalFuture state machines
 * polled to completion by block_on() (which runs ready tasks between polls). */
#include "node_runtime.h"
#include "peer_connection.h"

/* Clone of a String map key (BTreeMap<String, Value> claims). */
static void str_key_clone(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }

#define NODE_FN(name) jazz_db__db__node_runtime__Node__##name
#define NS_FN(name) jazz_node__node__NodeState__##name
#define PC_FN(name) jazz_db__db__peer_connection__PeerConnection__##name
#define QUEUE_ERR(code, msg) jz_err_new((code), (msg))

/* Await a boxed local future synchronously. */
static void block_on(jazz_db_LocalFuture *f, const jz_waker *w, void *out) {
    static const jz_waker noop = {0};
    for (;;) {
        if (jazz_db_future_poll(f, w ? w : &noop, out)) return;
        jz_task_run_ready();
    }
}

/* ---------------- QueuedMutationLease ----------------
 * Retain a FIFO owner operation while `Db::close` polls it. Dropping the
 * close future drops the lease, rather than the accepted operation. */
typedef struct QueuedMutationLease {
    jz_deque *queue;
    u32 *active_leases;
    bool has_operation;
    jazz_db_QueuedMutationOperation operation;
} QueuedMutationLease;

static void QueuedMutationLease_new(QueuedMutationLease *self, jz_deque *queue, u32 *active_leases, jazz_db_QueuedMutationOperation operation) {
    if (*active_leases == UINT32_MAX) jz_panic("queued mutation lease count overflow");
    *active_leases += 1;
    self->queue = queue;
    self->active_leases = active_leases;
    self->has_operation = true;
    self->operation = operation;
}
static jazz_db_QueuedMutationOperation *QueuedMutationLease_operation(QueuedMutationLease *self) {
    if (!self->has_operation) jz_panic("queued mutation lease must retain its operation");
    return &self->operation;
}
static jazz_db_QueuedMutationOperation QueuedMutationLease_take(QueuedMutationLease *self) {
    if (!self->has_operation) jz_panic("queued mutation lease must retain its operation");
    self->has_operation = false;
    return self->operation;
}
static void QueuedMutationLease_drop(QueuedMutationLease *self) {
    if (self->has_operation) {
        self->has_operation = false;
        jz_deque_push_front(self->queue, &self->operation);
    }
    if (*self->active_leases == 0) jz_panic("queued mutation lease count underflow");
    *self->active_leases -= 1;
}

/* ---------------- QueryRuntimeWake ---------------- */
static void QueryRuntimeWake_wake_by_ref(void *ctx) {
    jazz_db_QueryRuntimeWake *self = (jazz_db_QueryRuntimeWake *)ctx;
    *self->pending = true; /* Ordering::Release */
    jz_waker_wake(&self->scheduler);
}

bool jazz_db__db__node_runtime__make_query_runtime_waker(jazz_db_SharedTickScheduler *scheduler, bool *pending, jazz_db_WakerCache *cache, jz_waker *out) {
    if (cache->has) { *out = cache->waker; return true; }
    if (!scheduler->has || !scheduler->s.vt->query_runtime_waker) return false;
    jz_waker scheduler_waker;
    if (!scheduler->s.vt->query_runtime_waker(scheduler->s.self, &scheduler_waker)) return false;
    jazz_db_QueryRuntimeWake *wake = (jazz_db_QueryRuntimeWake *)jz_rc_new(sizeof(jazz_db_QueryRuntimeWake), NULL);
    wake->pending = pending;
    wake->scheduler = scheduler_waker;
    jz_waker waker = { QueryRuntimeWake_wake_by_ref, wake };
    cache->has = true;
    cache->waker = waker;
    *out = waker;
    return true;
}

/* ---------------- Node::new ---------------- */
static void collect_pending_mutation_errors(jazz_node_NodeState *node, jz_btree *pending) {
    JZ_VEC(jazz_model_TxId) rejected = {0};
    NS_FN(rejected_transactions)(node, (void *)&rejected);
    for (u32 i = 0; i < rejected.len; ++i) {
        jazz_model_TxId tx_id = rejected.ptr[i];
        const void *r = NS_FN(rejected_transaction)(node, tx_id);
        if (r) {
            jazz_db_MutationErrorEvent *ev = jazz_db__mutation_errors__mutation_error_event(r);
            jz_btree_insert(pending, &tx_id, &ev, NULL);
        }
    }
    jz_vec_drop(&rejected, sizeof(jazz_model_TxId), NULL);
}

#define RC_BTREE(desc) ({ jz_btree *_t = (jz_btree *)jz_rc_new(sizeof(jz_btree), (void (*)(void *))jz_btree_drop); jz_btree_init(_t, (desc)); _t; })

extern const jz_btree_desc jazz_db_large_value_upload_queues_desc;
extern const jz_btree_desc jazz_db_coverage_refresh_generations_desc;
extern const jz_btree_desc jazz_db_admitted_upstream_authorities_desc;

/// Wrap a node for serving subscriber links.
jazz_db_Node *NODE_FN(new)(jazz_node_NodeState *node) {
    /* History completeness is a structural property of the opened node,
     * not evaluator state. Cache it so connection attachment never needs
     * to synchronously borrow storage-owning state during evaluation. */
    bool receives_commits_as_local = !NS_FN(is_history_complete)(node);
    jazz_db_PeerChunkResolver *chunk_resolver = jazz_db__db__PeerChunkResolver__default();
    void *local_chunk_reader = NS_FN(local_chunk_reader_handle)(node);
    jazz_types_TxTime *tx_time_reservation_clock = (jazz_types_TxTime *)NS_FN(tx_time_reservation_clock)(node);
    jazz_types_NodeUuid node_uuid = NS_FN(node_uuid)(node);
    NS_FN(set_missing_chunk_resolver)(node, jazz_db__db__PeerChunkResolver__clone(chunk_resolver));
    bool *detach_covered_chunk_waits = NS_FN(detach_covered_chunk_waits_handle)(node);

    jazz_db_Node *self = (jazz_db_Node *)jz_rc_new(sizeof(jazz_db_Node), NULL); /* Rc<Node> */
    memset(self, 0, sizeof(*self));
    jazz_db_MutationErrorState *errors = (jazz_db_MutationErrorState *)jz_rc_new(sizeof(*errors), NULL);
    memset(errors, 0, sizeof(*errors));
    jz_btree_init(&errors->pending, &jazz_db_mutation_error_event_desc);
    jz_btree_init(&errors->application_waiters, &jazz_db_txid_set_desc);
    collect_pending_mutation_errors(node, &errors->pending);

    jazz_db_SharedTickScheduler *scheduler = (jazz_db_SharedTickScheduler *)jz_rc_new(sizeof(*scheduler), NULL);
    scheduler->has = false;

    jazz_db_NodeMutex *m = (jazz_db_NodeMutex *)jz_rc_new(sizeof(*m), NULL);
    m->locked = 0;
    m->state = node;
    self->node = m;
    self->has_owner_release_wait = false;
    self->mutation_owner_lifecycle = JAZZ_OWNER_OPEN;
    self->close_owner = 0;
    self->tx_time_reservation_clock = tx_time_reservation_clock;
    self->node_uuid = node_uuid;
    self->receives_commits_as_local = receives_commits_as_local;
    self->subscriptions = (jazz_db_SubscriptionList *)jz_rc_new(sizeof(jazz_db_SubscriptionList), NULL);
    memset(self->subscriptions, 0, sizeof(*self->subscriptions));
    self->outbox = jazz_db__db__UploadOutbox__default();
    self->pending_local_publications = (jz_deque *)jz_rc_new(sizeof(jz_deque), NULL);
    jz_deque_init(self->pending_local_publications, sizeof(jazz_db_PendingLocalPublication));
    jz_deque_init(&self->queued_mutations, sizeof(jazz_db_QueuedMutationOperation));
    self->queued_mutation_active_leases = 0;
    jz_btree_init(&self->queued_mutation_failures, &jazz_db_txid_err_desc);
    jz_btree_init(&self->deferred_rejection_discards, &jazz_db_txid_set_desc);
    jz_btree_init(&self->queued_open_transaction_failures, &jazz_db_open_tx_err_desc);
    jz_btree_init(&self->reserved_mutations, &jazz_db_txid_set_desc);
    self->pending_transaction_abandonments = RC_BTREE(&jazz_db_open_tx_set_desc);
    self->transaction_abandonments_closed = false;
    self->transaction_abandonment_shutdown_pending = false;
    self->upstream_subscriptions = (jazz_db_PendingUpstreamCommands *)jz_rc_new(sizeof(jazz_db_PendingUpstreamCommands), NULL);
    memset(self->upstream_subscriptions, 0, sizeof(*self->upstream_subscriptions));
    self->pending_subscription_finalizations = (jz_deque *)jz_rc_new(sizeof(jz_deque), NULL);
    jz_deque_init(self->pending_subscription_finalizations, sizeof(jazz_db_PendingSubscriptionFinalization));
    self->subscription_finalizations_closed = false;
    self->subscription_runtime_retired = false;
    self->latest_coverage_subscriptions = RC_BTREE(&jazz_db_coverage_subscription_desc);
    self->upstream_coverage_refcounts = RC_BTREE(&jazz_db_coverage_count_desc);
    self->awaiting_initial_authority_coverage = RC_BTREE(&jazz_db_coverage_set_desc);
    self->active_authority_view_receipts = (jazz_db_ActiveAuthorityViewReceipts *)jz_rc_new(sizeof(jazz_db_ActiveAuthorityViewReceipts), NULL);
    memset(self->active_authority_view_receipts, 0, sizeof(jazz_db_ActiveAuthorityViewReceipts));
    self->coverage_refresh_generations = RC_BTREE(&jazz_db_coverage_count_desc);
    self->query_coverage_registrations = RC_BTREE(&jazz_db_query_coverage_registrations_desc);
    self->upstream_subscription_owners = RC_BTREE(&jazz_db_upstream_owners_desc);
    self->relay_upstream_subscription_owners = RC_BTREE(&jazz_db_relay_owner_desc);
    self->pending_relay_subscription_rejections = RC_BTREE(&jazz_db_relay_rejections_desc);
    self->scheduler = (jazz_db_SharedTickScheduler *)jz_rc_clone(scheduler);
    self->remote_link = jazz_db__first_load__RemoteLinkTracker__new(scheduler);
    self->query_runtime_wake_pending = (bool *)jz_rc_new(sizeof(bool), NULL);
    *self->query_runtime_wake_pending = false;
    self->query_runtime_waker = (jazz_db_WakerCache *)jz_rc_new(sizeof(jazz_db_WakerCache), NULL);
    self->query_runtime_waker->has = false;
    self->upload_retry_clock = (jazz_db_UploadRetryClock *)jz_rc_new(sizeof(jazz_db_UploadRetryClock), NULL);
    *self->upload_retry_clock = jazz_db__db__MonotonicUploadRetryClock__new();
    self->detached_large_value_uploads = RC_BTREE(&jazz_db_upload_destination_queues_desc);
    self->large_value_upload_retry_deadlines = RC_BTREE(&jazz_db_txid_u64_desc);
    self->write_state_waiters = RC_BTREE(&jazz_db_write_state_waiters_desc);
    self->open_schema_admission = (jazz_db_OpenSchemaAdmission *)jz_rc_new(sizeof(jazz_db_OpenSchemaAdmission), NULL);
    self->open_schema_admission->has = false;
    self->mutation_errors = errors;
    self->browser_relay_recovered_tx_ids = RC_BTREE(&jazz_db_txid_set_desc);
    self->next_write_state_waiter_id = 1;
    self->next_subscription_nonce = 1;
    self->permission_advice_waiters = RC_BTREE(&jazz_db_permission_waiters_desc);
    self->current_rows = jazz_db__row_availability__CurrentRowsRouter__default();
    jazz_db__db__HeldRowChecks__default(&self->held_row_checks);
    self->local_fate_routes = RC_BTREE(&jazz_db_local_fate_routes_desc);
    self->admitted_upstream_authorities = (jazz_db_AuthorityContextVec *)jz_rc_new(sizeof(jazz_db_AuthorityContextVec), NULL);
    memset(self->admitted_upstream_authorities, 0, sizeof(jazz_db_AuthorityContextVec));
    self->admitted_upstream_authority = (jazz_db_HeldAuthority *)jz_rc_new(sizeof(jazz_db_HeldAuthority), NULL);
    self->admitted_upstream_authority->has = false;
    self->subscriber_dirty_epoch = (u64 *)jz_rc_new(sizeof(u64), NULL);
    *self->subscriber_dirty_epoch = 0;
    self->upstream_durability_floor = JAZZ_DUR_GLOBAL;
    self->defer_local_persistence = false;
    self->chunk_resolver = chunk_resolver;
    self->detach_covered_chunk_waits = detach_covered_chunk_waits;
    self->local_chunk_reader = local_chunk_reader;
    self->observed_chunk_completion_generation = 0;
    self->local_subscription_dirty_generation = 0;
    self->observed_local_subscription_dirty_generation = 0;
    jz_rc_release(scheduler);
    return self;
}

/// Reserve a definitive local transaction identity without borrowing the
/// async storage-owning node. The shared high-water mirror is advanced by
/// every ordinary mint and remote observation as well.
jz_err NODE_FN(reserve_transaction_id)(const jazz_db_Node *self, u64 now_ms, jazz_model_TxId *out) {
    JZ_TRY(NODE_FN(ensure_mutation_admission_open)(self));
    jazz_types_TxTime made_at;
    jz_err e = jazz_types__time__TxTime__tick(*self->tx_time_reservation_clock, now_ms, &made_at);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    *self->tx_time_reservation_clock = made_at;
    jazz_model__tx__TxId__new(out, made_at, self->node_uuid);
    return JZ_OK;
}

void NODE_FN(begin_mutation_shutdown)(jazz_db_Node *self) {
    jazz_db__db__finish_open_schema_admission(self->open_schema_admission,
        jz_err_new(JAZZ_EC_PROTOCOL, "database closed before schema admission"));
    self->mutation_owner_lifecycle = JAZZ_OWNER_CLOSING;
    /* Wait observers may be parked on a state-change channel even when
     * there is no queued mutation left to wake them. Closing is itself a
     * terminal observation boundary, so wake every waiter to let it
     * either observe its requested tier or report that the runtime closed
     * first. */
    jz_btree waiters = *self->write_state_waiters;
    jz_btree_init(self->write_state_waiters, waiters.d);
    jazz_model_TxId *k; jazz_db_WriteStateWaiterVec *v;
    JZ_BTREE_FOR(&waiters, k, v) {
        for (u32 i = 0; i < v->len; ++i) {
            jazz_db_Oneshot *sender = v->ptr[i].notify;
            (void)jazz_db__oneshot__send(sender, NULL);
            jazz_db__oneshot__drop_sender(sender);
            v->ptr[i].notify = NULL;
        }
    }
    jz_btree_drop(&waiters);
}

/// Serialize shutdown drains while keeping admission closure outside the
/// await. Cancellation releases ownership so another close caller can
/// resume the same idempotent drain and finalization sequence.
void NODE_FN(lock_close_owner)(jazz_db_Node *self) { self->close_owner++; }
void NODE_FN(unlock_close_owner)(jazz_db_Node *self) { if (self->close_owner) self->close_owner--; }

jz_err NODE_FN(ensure_mutation_admission_open)(const jazz_db_Node *self) {
    if (self->mutation_owner_lifecycle == JAZZ_OWNER_OPEN) return JZ_OK;
    return jz_err_new(JAZZ_EC_WRITE_REJECTED, "database mutation owner is closing");
}

jz_err NODE_FN(ensure_subscription_finalization_open)(const jazz_db_Node *self) {
    if (self->subscription_finalizations_closed)
        return jz_err_new(JAZZ_EC_PROTOCOL, "database subscription admission is closed");
    return JZ_OK;
}

static jazz_db_QueuedMutationStatus *new_pending_status(void) {
    jazz_db_QueuedMutationStatus *status = (jazz_db_QueuedMutationStatus *)jz_rc_new(sizeof(*status), jazz_db__db__QueuedMutationStatus__drop);
    status->tag = JAZZ_QMS_PENDING;
    status->error = JZ_OK;
    return status;
}

static void push_operation(jazz_db_Node *self, bool has_tx, jazz_model_TxId tx_id, jazz_model_TxKind kind,
                           bool has_open, jazz_model_OpenTransactionId open_tx_id, jazz_db_QueuedMutationFuture future,
                           jazz_db_QueuedMutationStatus *status, const jazz_db_QueuedMutationCompletion *completion) {
    jazz_db_QueuedMutationOperation op;
    memset(&op, 0, sizeof(op));
    op.has_transaction = has_tx;
    op.transaction_tx_id = tx_id;
    op.transaction_kind = (u8)kind;
    op.has_open_tx_id = has_open;
    op.open_tx_id = open_tx_id;
    op.future = future;
    op.status = status;
    op.has_completion = completion != NULL;
    if (completion) op.completion = *completion;
    jz_deque_push_back(&self->queued_mutations, &op);
}

jazz_db_QueuedMutationStatus *NODE_FN(enqueue_mutation)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_QueuedMutationFuture future) {
    jazz_db_QueuedMutationStatus *status = new_pending_status();
    jz_btree_insert(&self->reserved_mutations, &tx_id, NULL, NULL);
    jazz_model_OpenTransactionId none = {0};
    push_operation(self, true, tx_id, JAZZ_TX_MERGEABLE, false, none, future, (jazz_db_QueuedMutationStatus *)jz_rc_clone(status), NULL);
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    return status;
}

/// Queue a write whose transaction identity `tx_id` was reserved just
/// now. The future reports its own outcome; the queue only runs it in its
/// place, so it takes no status, error event or completion.
void NODE_FN(enqueue_reserved_write)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_QueuedMutationFuture future) {
    jz_btree_insert(&self->reserved_mutations, &tx_id, NULL, NULL);
    jazz_model_OpenTransactionId none = {0};
    push_operation(self, true, tx_id, JAZZ_TX_MERGEABLE, false, none, future, NULL, NULL);
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

jz_err NODE_FN(enqueue_transaction_operation)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, jazz_db_QueuedMutationFuture future) {
    jz_err e = NODE_FN(ensure_mutation_admission_open)(self);
    if (e.code) { jazz_db_future_drop(&future); return e; }
    jazz_model_TxId none = {0};
    push_operation(self, false, none, JAZZ_TX_MERGEABLE, true, open_tx_id, future, NULL, NULL);
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    return JZ_OK;
}

/* ---- enqueue_read_after_mutations ----
 * The read future writes `jz_err` followed by `out_size` bytes of T into its
 * output buffer; the result is forwarded on a oneshot carrying the same layout. */
typedef struct ReadSender { jazz_db_Oneshot *sender; /* Rc<RefCell<Option<Sender>>> */ } ReadSender;
typedef struct ReadAfterMutations {
    jazz_db_LocalFuture read;
    ReadSender *sender;
    u32 out_size;
    u8 *buf;
} ReadAfterMutations;

static bool ReadAfterMutations_poll(void *p, const jz_waker *cx, void *out) {
    ReadAfterMutations *self = (ReadAfterMutations *)p;
    /* Cancelling an observation releases its read fence, not any staging
     * or commit already admitted after that read. */
    bool cancelled = self->sender->sender == NULL || jazz_db__oneshot__is_canceled(self->sender->sender);
    if (!cancelled) {
        if (!self->buf) self->buf = (u8 *)jz_zalloc(sizeof(jz_err) + self->out_size);
        if (!jazz_db_future_poll(&self->read, cx, self->buf)) return false;
        if (self->sender->sender) {
            jazz_db_Oneshot *sender = self->sender->sender;
            self->sender->sender = NULL;
            (void)jazz_db__oneshot__send(sender, self->buf);
            jazz_db__oneshot__drop_sender(sender);
        }
    }
    *(jz_err *)out = JZ_OK;
    return true;
}
static void ReadAfterMutations_drop(void *p) {
    ReadAfterMutations *self = (ReadAfterMutations *)p;
    jazz_db_future_drop(&self->read);
    jz_rc_release(self->sender);
    jz_free(self->buf);
    jz_free(self);
}
typedef struct ReadCompletion { ReadSender *sender; u32 out_size; } ReadCompletion;
static void ReadCompletion_call(void *p, jz_err outcome) {
    ReadCompletion *self = (ReadCompletion *)p;
    if (outcome.code && self->sender->sender) {
        jazz_db_Oneshot *sender = self->sender->sender;
        self->sender->sender = NULL;
        u8 *buf = (u8 *)jz_zalloc(sizeof(jz_err) + self->out_size);
        *(jz_err *)buf = outcome;
        (void)jazz_db__oneshot__send(sender, buf);
        jazz_db__oneshot__drop_sender(sender);
        jz_free(buf);
    } else {
        jz_err_drop(&outcome);
    }
}
static void ReadCompletion_drop(void *p) {
    ReadCompletion *self = (ReadCompletion *)p;
    jz_rc_release(self->sender);
    jz_free(self);
}
static void ReadSender_drop(void *p) {
    ReadSender *s = (ReadSender *)p;
    if (s->sender) jazz_db__oneshot__drop_sender(s->sender);
}

static jazz_db_Oneshot *enqueue_read_after_mutations(jazz_db_Node *self, bool has_open, jazz_model_OpenTransactionId open_tx_id, jazz_db_LocalFuture read, u32 out_size) {
    jazz_db_Oneshot *receiver = jazz_db__oneshot__channel((u32)sizeof(jz_err) + out_size);
    receiver->refs++; /* sender half */
    ReadSender *sender = (ReadSender *)jz_rc_new(sizeof(ReadSender), ReadSender_drop);
    sender->sender = receiver;
    ReadAfterMutations *fut = JZ_NEW(ReadAfterMutations);
    fut->read = read;
    fut->sender = (ReadSender *)jz_rc_clone(sender);
    fut->out_size = out_size;
    ReadCompletion *completion_ctx = JZ_NEW(ReadCompletion);
    completion_ctx->sender = (ReadSender *)jz_rc_clone(sender);
    completion_ctx->out_size = out_size;
    jz_rc_release(sender);
    jazz_db_LocalFuture future = { ReadAfterMutations_poll, ReadAfterMutations_drop, fut };
    jazz_db_QueuedMutationCompletion completion = { ReadCompletion_call, completion_ctx, ReadCompletion_drop };
    jazz_model_TxId none = {0};
    push_operation(self, false, none, JAZZ_TX_MERGEABLE, has_open, open_tx_id, future, NULL, &completion);
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    return receiver;
}

/// Put a transaction-local read behind every already-admitted operation
/// for that transaction. The receiver owns the eventual result, while the
/// owner queue retains the cold storage future and its wake route.
jazz_db_Oneshot *NODE_FN(enqueue_transaction_read)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, jazz_db_LocalFuture read, u32 out_size) {
    return enqueue_read_after_mutations(self, true, open_tx_id, read, out_size);
}

u32 NODE_FN(queued_mutation_count)(const jazz_db_Node *self) { return self->queued_mutations.len; }

static bool ready_ok_poll(void *self, const jz_waker *cx, void *out) { (void)self; (void)cx; *(jz_err *)out = JZ_OK; return true; }

jazz_db_Oneshot *NODE_FN(queued_mutation_barrier)(jazz_db_Node *self) {
    if (NODE_FN(owner_queue_is_quiescent)(self)) {
        jazz_db_Oneshot *receiver = jazz_db__oneshot__channel(sizeof(jz_err));
        jz_err ok = JZ_OK;
        (void)jazz_db__oneshot__send(receiver, &ok);
        return receiver;
    }
    jazz_db_LocalFuture ready = { ready_ok_poll, NULL, (void *)1 };
    jazz_model_OpenTransactionId none = {0};
    return enqueue_read_after_mutations(self, false, none, ready, 0);
}

jazz_db_QueuedMutationStatus *NODE_FN(enqueue_transaction_commit)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, jazz_model_TxId tx_id, jazz_model_TxKind kind, jazz_db_QueuedMutationFuture future) {
    jazz_db_QueuedMutationStatus *status = new_pending_status();
    jz_btree_insert(&self->reserved_mutations, &tx_id, NULL, NULL);
    push_operation(self, true, tx_id, kind, true, open_tx_id, future, (jazz_db_QueuedMutationStatus *)jz_rc_clone(status), NULL);
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    return status;
}

void NODE_FN(enqueue_transaction_cleanup)(jazz_db_Node *self, jazz_db_QueuedMutationFuture future) {
    NODE_FN(enqueue_transaction_cleanup_with_completion)(self, future, NULL);
}

/// Order a non-transactional owner operation behind every operation
/// already admitted. Like cleanup, it bypasses mutation admission.
void NODE_FN(enqueue_owner_operation)(jazz_db_Node *self, jazz_db_QueuedMutationFuture future) {
    NODE_FN(enqueue_transaction_cleanup_with_completion)(self, future, NULL);
}

/// Whether no admitted owner operation is queued or being drained.
bool NODE_FN(owner_queue_is_quiescent)(const jazz_db_Node *self) {
    return self->queued_mutations.len == 0 && self->queued_mutation_active_leases == 0;
}

void NODE_FN(enqueue_transaction_cleanup_with_completion)(jazz_db_Node *self, jazz_db_QueuedMutationFuture future, const jazz_db_QueuedMutationCompletion *completion) {
    jazz_model_TxId none = {0};
    jazz_model_OpenTransactionId no_open = {0};
    push_operation(self, false, none, JAZZ_TX_MERGEABLE, false, no_open, future, NULL, completion);
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

/// Whether owner-free queue waiting can coexist with semantic maintenance.
bool NODE_FN(owner_is_available)(const jazz_db_Node *self) {
    /* Drop the probe guard before maintenance can acquire the owner. */
    if (jazz_db_node_try_lock(self->node)) { jazz_db_node_unlock(self->node); return true; }
    return false;
}

/* Box::pin(async move { drop(node.lock().await) }) */
static bool OwnerReleaseWait_poll(void *p, const jz_waker *cx, void *out) {
    jazz_db_NodeMutex *node = (jazz_db_NodeMutex *)p;
    (void)cx; (void)out;
    if (jazz_db_node_try_lock(node)) { jazz_db_node_unlock(node); return true; }
    return false;
}
static void OwnerReleaseWait_drop(void *p) { jz_rc_release(p); }

/// Yield without losing the host wake needed after an external read finishes.
bool NODE_FN(owner_is_available_or_wake_when_released)(jazz_db_Node *self) {
    if (NODE_FN(owner_is_available)(self)) {
        if (self->has_owner_release_wait) { jazz_db_future_drop(&self->owner_release_wait); self->has_owner_release_wait = false; }
        return true;
    }
    jz_waker waker;
    if (!NODE_FN(query_runtime_waker)(self, &waker)) return false;
    /* Retain the lock future: dropping a pending mutex waiter cancels its wake. */
    if (!self->has_owner_release_wait) {
        jazz_db_LocalFuture f = { OwnerReleaseWait_poll, OwnerReleaseWait_drop, jz_rc_clone(self->node) };
        self->owner_release_wait = f;
        self->has_owner_release_wait = true;
    }
    if (jazz_db_future_poll(&self->owner_release_wait, &waker, NULL)) {
        jazz_db_future_drop(&self->owner_release_wait);
        self->has_owner_release_wait = false;
        return true;
    }
    return false;
}

bool NODE_FN(queued_transaction_error)(const jazz_db_Node *self, jazz_model_OpenTransactionId id, jz_err *out) {
    jz_err *e = (jz_err *)jz_btree_get(&self->queued_open_transaction_failures, &id);
    if (!e) return false;
    *out = jazz_db_err_clone(e);
    return true;
}

void NODE_FN(retire_queued_transaction_error)(jazz_db_Node *self, jazz_model_OpenTransactionId id) {
    jz_btree_remove(&self->queued_open_transaction_failures, &id, NULL, NULL);
}

static void finish_queued_mutation(jazz_db_Node *self, jazz_db_QueuedMutationOperation operation, jz_err result);

static bool open_tx_eq_opt(const jazz_db_QueuedMutationOperation *a, const jazz_db_QueuedMutationOperation *b) {
    if (a->has_open_tx_id != b->has_open_tx_id) return false;
    return !a->has_open_tx_id || a->open_tx_id.v == b->open_tx_id.v;
}

/// Poll the next eligible owner operation, retaining pending read fences.
bool NODE_FN(poll_queued_mutation_once)(jazz_db_Node *self) {
    if (self->queued_mutations.len == 0) return false;
    static const jz_waker noop = {0};
    jz_waker owned_waker;
    bool has_owned = NODE_FN(query_runtime_waker)(self, &owned_waker);
    const jz_waker *waker = has_owned ? &owned_waker : &noop;
    JZ_VEC(jazz_db_QueuedMutationOperation) deferred_reads = {0};
    bool has_active = false;
    jazz_db_QueuedMutationOperation active_operation;
    bool pending;
    for (;;) {
        jazz_db_QueuedMutationOperation operation;
        if (!jz_deque_pop_front(&self->queued_mutations, &operation)) { pending = deferred_reads.len != 0; break; }
        /* ponytail: scan pending readers; index by transaction if wide
         * concurrent read batches make this quadratic path significant. */
        bool fenced = false;
        for (u32 i = 0; i < deferred_reads.len; ++i)
            if (open_tx_eq_opt(&deferred_reads.ptr[i], &operation)) { fenced = true; break; }
        if (fenced) {
            /* Reads in one transaction retain their invocation order. A
             * later mutation also fences every operation behind it. */
            if (operation.has_transaction || !operation.has_completion) {
                jz_deque_push_front(&self->queued_mutations, &operation);
                pending = true;
                break;
            }
            jz_vec_push(&deferred_reads, &operation, sizeof(operation));
            continue;
        }
        jz_err poisoned = JZ_OK;
        bool has_poisoned = operation.has_open_tx_id && NODE_FN(queued_transaction_error)(self, operation.open_tx_id, &poisoned);
        bool ready;
        jz_err result = JZ_OK;
        if (has_poisoned) { ready = true; result = poisoned; }
        else ready = jazz_db_future_poll(&operation.future, waker, &result);
        if (!ready) {
            if (!operation.has_transaction && operation.has_open_tx_id && operation.has_completion && NODE_FN(owner_is_available)(self)) {
                /* Remote coverage must not stop unrelated local work.
                 * No queue scan or allocation is needed for ready reads. */
                jz_vec_push(&deferred_reads, &operation, sizeof(operation));
                continue;
            }
            /* Once started, this operation may own the lock needed by
             * the deferred reads. It must resume before those readers. */
            active_operation = operation;
            has_active = true;
            pending = true;
            break;
        }
        finish_queued_mutation(self, operation, result);
        pending = deferred_reads.len != 0;
        break;
    }
    for (u32 i = deferred_reads.len; i-- > 0;)
        jz_deque_push_front(&self->queued_mutations, &deferred_reads.ptr[i]);
    jz_vec_drop(&deferred_reads, sizeof(jazz_db_QueuedMutationOperation), NULL);
    if (has_active) jz_deque_push_front(&self->queued_mutations, &active_operation);
    return pending;
}

/* cleanup future: abandon_tx after a failed transaction commit */
typedef struct AbandonCleanup { jazz_db_NodeMutex *node; jazz_model_OpenTransactionId open_tx_id; } AbandonCleanup;
static bool AbandonCleanup_poll(void *p, const jz_waker *cx, void *out) {
    AbandonCleanup *self = (AbandonCleanup *)p;
    (void)cx;
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    jz_err e = NS_FN(abandon_tx)(node, self->open_tx_id);
    jazz_db_node_unlock(self->node);
    if (e.code == 0 || e.code == JAZZ_NODE_ERR_MISSING_OPEN_BATCH) { jz_err_drop(&e); *(jz_err *)out = JZ_OK; }
    else *(jz_err *)out = jazz_node__node__api_error__Error__From__from(e);
    return true;
}
static void AbandonCleanup_drop(void *p) { AbandonCleanup *self = (AbandonCleanup *)p; jz_rc_release(self->node); jz_free(self); }

static void finish_queued_mutation(jazz_db_Node *self, jazz_db_QueuedMutationOperation operation, jz_err result) {
    if (operation.has_completion) {
        operation.has_completion = false;
        operation.completion.call(operation.completion.ctx, jazz_db_err_clone(&result));
        if (operation.completion.drop) operation.completion.drop(operation.completion.ctx);
    }
    if (operation.has_transaction)
        jz_btree_remove(&self->reserved_mutations, &operation.transaction_tx_id, NULL, NULL);
    if (result.code && operation.has_open_tx_id) {
        bool inserted;
        jz_err *slot = (jz_err *)jz_btree_entry(&self->queued_open_transaction_failures, &operation.open_tx_id, &inserted);
        if (inserted) *slot = jazz_db_err_clone(&result);
    }
    if (operation.has_transaction && operation.status) {
        jazz_model_TxId tx_id = operation.transaction_tx_id;
        bool terminal_failed = result.code != 0;
        jazz_db_QueuedMutationStatus *status = operation.status;
        if (!result.code) {
            status->tag = JAZZ_QMS_PUBLISHED;
        } else {
            jz_err_drop(&status->error);
            status->tag = JAZZ_QMS_FAILED;
            status->error = jazz_db_err_clone(&result);
            /* Validation can fail before any durable transaction exists.
             * Retain a live fallback event as well as the wait result; do
             * not manufacture a persisted rejection for an unpublished write. */
            jazz_db__mutation_errors__queue_mutation_error(self->mutation_errors, self->scheduler, tx_id,
                jazz_db__mutation_errors__queued_mutation_error_event(tx_id, (jazz_model_TxKind)operation.transaction_kind, &result));
            jz_err moved = result;
            result = JZ_OK;
            jz_btree_insert(&self->queued_mutation_failures, &tx_id, &moved, NULL);
        }
        jazz_db__db__node_runtime__notify_write_state_waiters(self->write_state_waiters, tx_id);
        if (terminal_failed && operation.has_open_tx_id) {
            AbandonCleanup *c = JZ_NEW(AbandonCleanup);
            c->node = (jazz_db_NodeMutex *)jz_rc_clone(self->node);
            c->open_tx_id = operation.open_tx_id;
            jazz_db_LocalFuture f = { AbandonCleanup_poll, AbandonCleanup_drop, c };
            NODE_FN(enqueue_transaction_cleanup)(self, f);
        }
    }
    if (operation.status) jz_rc_release(operation.status);
    if (operation.has_transaction && operation.has_open_tx_id)
        jz_btree_remove(&self->queued_open_transaction_failures, &operation.open_tx_id, NULL, NULL);
    jazz_db_future_drop(&operation.future);
    jz_err_drop(&result);
    if (self->queued_mutations.len != 0) NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

void NODE_FN(drain_queued_mutations)(jazz_db_Node *self) {
    for (;;) {
        jazz_db_QueuedMutationOperation operation;
        if (!jz_deque_pop_front(&self->queued_mutations, &operation)) return;
        QueuedMutationLease lease;
        QueuedMutationLease_new(&lease, &self->queued_mutations, &self->queued_mutation_active_leases, operation);
        jazz_db_QueuedMutationOperation *op = QueuedMutationLease_operation(&lease);
        jz_err result = JZ_OK;
        bool poisoned = op->has_open_tx_id && NODE_FN(queued_transaction_error)(self, op->open_tx_id, &result);
        if (!poisoned) block_on(&op->future, NULL, &result);
        finish_queued_mutation(self, QueuedMutationLease_take(&lease), result);
        QueuedMutationLease_drop(&lease);
    }
}

void NODE_FN(upstream_register_shape_options)(const jazz_db_Node *self, jazz_model_DurabilityTier tier, jazz_protocol_ReadViewSpec *read_view, jazz_protocol_RegisterShapeOptions *out) {
    jazz_db__db__upstream_register_shape_options(out, tier, read_view, (jazz_model_DurabilityTier)self->upstream_durability_floor);
}

/// Ordinary `Db::open` nodes are Local receivers. Only the structurally
/// separate history-complete path acts as the Core fate authority.
bool NODE_FN(receives_commits_as_local)(const jazz_db_Node *self) { return self->receives_commits_as_local; }

/// Borrow the served node.
jazz_db_NodeMutex *NODE_FN(node)(const jazz_db_Node *self) { return (jazz_db_NodeMutex *)jz_rc_clone(self->node); }

/* ---------------- configuration ---------------- */

/// Configure Jazz-owned ingress and expiry policy for unpublished large
/// values. Groove persists timestamps and performs eviction, but does not
/// choose these product limits.
void NODE_FN(set_large_value_staging_policy)(jazz_db_Node *self, const void *policy) {
    NS_FN(set_large_value_staging_policy)(jazz_db__db__LocalMutexBorrow__borrow(self->node), policy);
    jazz_db_node_unlock(self->node);
}

/// Run one host-driven staging-expiry maintenance pass.
///
/// Browser, NAPI, and server hosts call this from their own timer cadence;
/// it is idempotent and does not make Groove own an executor or clock.
jz_err NODE_FN(evict_expired_staged_large_values)(jazz_db_Node *self, u32 *out) {
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node);
    jz_err e = NS_FN(evict_expired_staged_large_values)(node, out);
    jazz_db_node_unlock(self->node);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    return JZ_OK;
}

void NODE_FN(set_non_durable_client)(jazz_db_Node *self) {
    NS_FN(set_non_durable_client)(jazz_db__db__LocalMutexBorrow__borrow(self->node));
    jazz_db_node_unlock(self->node);
    self->upstream_durability_floor = JAZZ_DUR_LOCAL;
}

jz_err NODE_FN(configure_scope_isolated_client_relay)(jazz_db_Node *self, void *scope) {
    jz_err e = NS_FN(configure_scope_isolated_client_relay)(jazz_db__db__LocalMutexBorrow__borrow(self->node), scope);
    jazz_db_node_unlock(self->node);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    return JZ_OK;
}

void NODE_FN(set_deferred_local_persistence)(jazz_db_Node *self, bool deferred) { self->defer_local_persistence = deferred; }

/// Change whether subscriber links may serve their registered views.
/// Publishing a permissions head always rehydrates every live view, so a
/// tighter head retracts rows without requiring a reconnect.
jz_err NODE_FN(set_permissions_ready)(jazz_db_Node *self, bool ready) {
    NS_FN(set_permissions_ready)(jazz_db__db__LocalMutexBorrow__borrow(self->node), ready);
    jazz_db_node_unlock(self->node);
    if (ready) {
        for (u32 i = 0; i < self->connections.len; ++i) {
            jazz_db_PeerMutex *m = self->connections.ptr[i];
            jazz_db_PeerConnection *c = jazz_db_peer_try_lock(m);
            if (!c) jz_panic("connection is already borrowed");
            jz_err e = PC_FN(rehydrate_subscriber_views)(c);
            jazz_db_peer_unlock(m);
            JZ_TRY(e);
        }
    }
    return JZ_OK;
}

/* ---------------- uploads / local publications ---------------- */

void NODE_FN(queue_pending_upload)(jazz_db_Node *self, jazz_model_TxId tx_id, bool has_unit, jazz_protocol_SyncMessage unit) {
    if (jazz_db__db__queue_pending_upload_in(self->outbox, tx_id, has_unit, unit)) {
        NODE_FN(mark_subscriber_connections_dirty)(self);
        NODE_FN(schedule_tick)(self, JAZZ_TICK_DEFERRED);
    }
}

typedef struct LocalSettlement { jazz_node_PublishedTransaction *publication; jazz_db_NodeMutex *node; } LocalSettlement;
static bool LocalSettlement_poll(void *p, const jz_waker *cx, void *out) {
    LocalSettlement *self = (LocalSettlement *)p;
    jazz_db_TxIdResult *r = (jazz_db_TxIdResult *)out;
    (void)cx;
    jazz_model_TxId tx_id = jazz_node__node__PublishedTransaction__tx_id(self->publication);
    jz_err persistence = jazz_node__node__PublishedTransaction__persist(self->publication);
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    jz_err e = NS_FN(settle_published_transaction)(node, tx_id, persistence);
    jazz_db_node_unlock(self->node);
    if (e.code) { r->err = jazz_node__node__api_error__Error__From__from(e); return true; }
    r->err = JZ_OK;
    r->tx_id = tx_id;
    return true;
}
static void LocalSettlement_drop(void *p) {
    LocalSettlement *self = (LocalSettlement *)p;
    jz_rc_release(self->publication);
    jz_rc_release(self->node);
    jz_free(self);
}

void NODE_FN(queue_local_publication)(jazz_db_Node *self, jazz_node_PublishedTransaction *published, bool has_unit, jazz_protocol_SyncMessage upload_unit) {
    /* `published` is an Rc payload owned by the caller and moved in */
    LocalSettlement *s = JZ_NEW(LocalSettlement);
    s->publication = (jazz_node_PublishedTransaction *)jz_rc_clone(published);
    s->node = (jazz_db_NodeMutex *)jz_rc_clone(self->node);
    jazz_db_PendingLocalPublication pending;
    memset(&pending, 0, sizeof(pending));
    pending.published = published;
    pending.has_upload_unit = has_unit;
    if (has_unit) pending.upload_unit = upload_unit;
    jazz_db_LocalFuture settlement = { LocalSettlement_poll, LocalSettlement_drop, s };
    pending.settlement = settlement;
    jz_deque_push_back(self->pending_local_publications, &pending);
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

/// Whether a resident publication still needs its ordered durability turn.
/// This check is deliberately synchronous so a host's one bounded tick
/// does not spend its sole poll acquiring the publication-settler mutex
/// when there is no publication to advance.
bool NODE_FN(has_pending_local_publications)(const jazz_db_Node *self) { return self->pending_local_publications->len != 0; }

/// Local commits still settling before they join the upload outbox, with
/// the storage tables each one changed.
void NODE_FN(pending_local_publication_tables)(const jazz_db_Node *self, jazz_db_TxTablesVec *out) {
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < self->pending_local_publications->len; ++i) {
        jazz_db_PendingLocalPublication *pending = (jazz_db_PendingLocalPublication *)jz_deque_at(self->pending_local_publications, i);
        jazz_db_TxTables entry;
        memset(&entry, 0, sizeof(entry));
        entry.tx_id = jazz_node__node__PublishedTransaction__tx_id(pending->published);
        const JZ_VEC(jz_str) *tables = (const void *)jazz_node__node__PublishedTransaction__changed_tables(pending->published);
        for (u32 j = 0; j < tables->len; ++j) {
            jz_str t = jz_str_clone(&tables->ptr[j]);
            jz_vec_push(&entry.tables, &t, sizeof(jz_str));
        }
        jz_vec_push(out, &entry, sizeof(entry));
    }
}

bool NODE_FN(is_pending_local_publication)(const jazz_db_Node *self, jazz_model_TxId tx_id) {
    for (u32 i = 0; i < self->pending_local_publications->len; ++i) {
        jazz_db_PendingLocalPublication *pending = (jazz_db_PendingLocalPublication *)jz_deque_at(self->pending_local_publications, i);
        if (jazz_model_TxId_eq(jazz_node__node__PublishedTransaction__tx_id(pending->published), tx_id)) return true;
    }
    return false;
}

/* Poll<Result<(), Error>>: returns true when ready, with the result in *result. */
static bool poll_local_publication_settlement(jazz_db_Node *self, const jz_waker *cx, jz_err *result) {
    for (;;) {
        jazz_db_PendingLocalPublication pending;
        if (!jz_deque_pop_front(self->pending_local_publications, &pending)) { *result = JZ_OK; return true; }
        jazz_db_TxIdResult r;
        if (!jazz_db_future_poll(&pending.settlement, cx, &r)) {
            jz_deque_push_front(self->pending_local_publications, &pending);
            return false;
        }
        jazz_db_future_drop(&pending.settlement);
        if (r.err.code) {
            jz_rc_release(pending.published);
            if (pending.has_upload_unit) jazz_protocol__protocol__SyncMessage__drop(&pending.upload_unit);
            *result = r.err;
            return true;
        }
        JZ_ASSERT(jazz_model_TxId_eq(jazz_node__node__PublishedTransaction__tx_id(pending.published), r.tx_id),
                  "assertion `left == right` failed");
        /* Persistence has now advanced durability from None to
         * Local. Wake waits registered before this receipt; merely
         * polling their futures cannot advance a pending state-change channel. */
        jazz_db__db__node_runtime__notify_write_state_waiters(self->write_state_waiters, r.tx_id);
        NODE_FN(queue_pending_upload)(self, r.tx_id, pending.has_upload_unit, pending.upload_unit);
        jz_rc_release(pending.published);
    }
}

/// Advance ordered local durability without keeping the host tick pending.
/// A retained mutation may own the node lock needed to finish settlement;
/// the tick must still poll that mutation. The runtime-owned waker requests
/// another owner turn when this retained settlement can make progress.
jz_err NODE_FN(poll_local_publication_settlement_once)(jazz_db_Node *self, bool *pending) {
    static const jz_waker noop = {0};
    jz_waker owned;
    bool has = NODE_FN(query_runtime_waker)(self, &owned);
    jz_err result;
    if (!poll_local_publication_settlement(self, has ? &owned : &noop, &result)) { *pending = true; return JZ_OK; }
    JZ_TRY(result);
    *pending = false;
    return JZ_OK;
}

/// Chunk staging must not retain the node while waiting for a lifecycle
/// guard owned by an earlier local publication: settling that publication
/// also needs the node. Recheck under the acquired guard so another writer
/// cannot publish between draining the queue and starting staging.
/// On success the node stays locked; the caller unlocks `self->node`.
jz_err NODE_FN(lock_for_large_value_staging)(jazz_db_Node *self, jazz_node_NodeState **out) {
    for (;;) {
        jazz_node_NodeState *node = jazz_db_node_lock(self->node);
        if (!NODE_FN(has_pending_local_publications)(self)) { *out = node; return JZ_OK; }
        jazz_db_node_unlock(self->node);
        JZ_TRY(NODE_FN(settle_local_publications)(self));
    }
}

jz_err NODE_FN(settle_local_publications)(jazz_db_Node *self) {
    static const jz_waker noop = {0};
    jz_err result;
    while (!poll_local_publication_settlement(self, &noop, &result)) jz_task_run_ready();
    return result;
}

static void queue_restored_uploads(jazz_db_Node *self, JZ_VEC(jazz_model_TxId) *pending, bool dedup) {
    jz_hmap restored;
    jz_hmap_init(&restored, &jazz_db_txid_hset_desc);
    jazz_protocol_SyncMessage none;
    memset(&none, 0, sizeof(none));
    for (u32 i = 0; i < pending->len; ++i) {
        jazz_model_TxId tx_id = pending->ptr[i];
        if (dedup) {
            bool inserted;
            jz_hmap_entry(&restored, &tx_id, &inserted);
            if (!inserted) continue;
        }
        NODE_FN(queue_pending_upload)(self, tx_id, false, none);
    }
    jz_hmap_drop(&restored);
    jz_vec_drop(pending, sizeof(jazz_model_TxId), NULL);
}

/// Restore locally originated, unsettled durable writes into the
/// process-local upload queue after reopening client storage.
jz_err NODE_FN(restore_pending_uploads)(jazz_db_Node *self, jazz_db_DbIdentity identity) {
    JZ_VEC(jazz_model_TxId) pending = {0};
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    jz_err e = NS_FN(pending_transaction_ids_for)(node, identity.node, identity.author, (void *)&pending);
    jazz_db_node_unlock(self->node);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    queue_restored_uploads(self, (void *)&pending, true);
    return JZ_OK;
}

jz_err NODE_FN(restore_backend_pending_uploads)(jazz_db_Node *self, jazz_types_NodeUuid node_id) {
    JZ_VEC(jazz_model_TxId) pending = {0};
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    jz_err e = NS_FN(synchronizing_transaction_ids_for_node)(node, node_id, (void *)&pending);
    jazz_db_node_unlock(self->node);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    queue_restored_uploads(self, (void *)&pending, false);
    return JZ_OK;
}

jz_err NODE_FN(restore_browser_relay_pending_uploads)(jazz_db_Node *self, jazz_types_AuthorSubject author) {
    JZ_VEC(jazz_model_TxId) pending = {0};
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    jz_err e = NS_FN(pending_transaction_ids_for_author)(node, author, (void *)&pending);
    jazz_db_node_unlock(self->node);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    for (u32 i = 0; i < pending.len; ++i)
        jz_btree_insert(self->browser_relay_recovered_tx_ids, &pending.ptr[i], NULL, NULL);
    queue_restored_uploads(self, (void *)&pending, false);
    return JZ_OK;
}

static jz_err restore_local_subscriber(jazz_db_Node *self, jazz_types_AuthorSubject author, jazz_db_SyncMessageVec *downstream_fates) {
    return jazz_db__db__restore_local_subscriber_replay(self->node, self->outbox, self->local_fate_routes, author, downstream_fates);
}

void NODE_FN(mark_subscriber_connections_dirty)(jazz_db_Node *self) {
    u64 next = *self->subscriber_dirty_epoch + 1; /* wrapping_add */
    *self->subscriber_dirty_epoch = next;
    for (u32 i = 0; i < self->connections.len; ++i) {
        /* A suspended peer observes the shared epoch on its next tick.
         * Local publication must not synchronously reenter that owner. */
        jazz_db_PeerMutex *m = self->connections.ptr[i];
        jazz_db_PeerConnection *connection = jazz_db_peer_try_lock(m);
        if (!connection) continue;
        if (connection->link.tag == JAZZ_LINK_SUBSCRIBER) {
            connection->link.v.subscriber.serve_dirty = true;
            connection->observed_subscriber_dirty_epoch = next;
        }
        jazz_db_peer_unlock(m);
    }
}

void NODE_FN(mark_subscriber_connections_dirty_after_query_runtime_wake)(jazz_db_Node *self) {
    bool was = *self->query_runtime_wake_pending; /* swap(false, AcqRel) */
    *self->query_runtime_wake_pending = false;
    if (was) {
        self->local_subscription_dirty_generation += 1;
        NODE_FN(mark_subscriber_connections_dirty)(self);
    }
}

jazz_protocol_SubscriptionKey NODE_FN(next_subscription_key)(jazz_db_Node *self, const jazz_model_ValidatedQuery *shape, jazz_protocol_ReadViewKey read_view) {
    u64 nonce = self->next_subscription_nonce;
    self->next_subscription_nonce = nonce == UINT64_MAX ? nonce : nonce + 1;
    u8 be[8];
    for (int i = 0; i < 8; ++i) be[i] = (u8)(nonce >> (56 - 8 * i));
    jazz_protocol_SubscriptionKey key;
    memset(&key, 0, sizeof(key));
    key.shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
    key.binding_id.u = jz_uuid_v5(&jazz_model__query__QUERY_NAMESPACE, be, 8);
    key.read_view = read_view;
    return key;
}

void NODE_FN(set_scheduler)(jazz_db_Node *self, const jazz_db_TickScheduler *scheduler) {
    *self->detach_covered_chunk_waits = scheduler && scheduler->vt->drops_pending_ticks && scheduler->vt->drops_pending_ticks(scheduler->self);
    if (scheduler) { self->scheduler->has = true; self->scheduler->s = *scheduler; }
    else self->scheduler->has = false;
    self->query_runtime_waker->has = false;
}

void NODE_FN(schedule_tick)(const jazz_db_Node *self, jazz_db_TickUrgency urgency) {
    jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, urgency);
}

/// Obtain the host-owned waker that survives this short owner turn.
///
/// It is passed only into Groove's non-blocking query-progress poll. A
/// later cold-storage completion therefore asks the host for one new tick
/// instead of making this runtime poll while the storage is still cold.
bool NODE_FN(query_runtime_waker)(const jazz_db_Node *self, jz_waker *out) {
    return jazz_db__db__node_runtime__make_query_runtime_waker(self->scheduler, self->query_runtime_wake_pending, self->query_runtime_waker, out);
}

/* ---------------- transaction admission / abandonment ---------------- */

static jz_err ensure_transaction_admission_open(const jazz_db_Node *self) {
    if (self->transaction_abandonments_closed)
        return jz_err_new(JAZZ_EC_PROTOCOL, "database transaction admission is closed");
    return JZ_OK;
}

static jz_err abandon_transaction_for_maintenance(jazz_node_NodeState *node, jazz_model_OpenTransactionId open_tx_id) {
    jz_err e = NS_FN(abandon_tx)(node, open_tx_id);
    if (e.code == 0) return JZ_OK;
    if (e.code == JAZZ_NODE_ERR_MISSING_OPEN_BATCH) { jz_err_drop(&e); return JZ_OK; }
    return jazz_node__node__api_error__Error__From__from(e);
}

static jz_err reject_tombstoned_transaction_in(jazz_db_Node *self, jazz_node_NodeState *node, jazz_model_OpenTransactionId open_tx_id) {
    if (!jz_btree_contains(self->pending_transaction_abandonments, &open_tx_id)) return JZ_OK;
    jz_err retirement = abandon_transaction_for_maintenance(node, open_tx_id);
    if (!retirement.code) NODE_FN(clear_transaction_abandonment)(self, open_tx_id);
    JZ_TRY(retirement);
    return jazz_db__db__transaction_abandoned(open_tx_id);
}

static jz_err drain_transaction_abandonments_in(jazz_db_Node *self, jazz_node_NodeState *node, u32 *out) {
    jz_btree abandonments = *self->pending_transaction_abandonments;
    jz_btree_init(self->pending_transaction_abandonments, abandonments.d);
    u32 count = abandonments.len;
    jz_err first_error = JZ_OK;
    jazz_model_OpenTransactionId *id; void *unused;
    JZ_BTREE_FOR(&abandonments, id, unused) {
        jz_err e = abandon_transaction_for_maintenance(node, *id);
        if (e.code) {
            jz_btree_insert(self->pending_transaction_abandonments, id, NULL, NULL);
            if (!first_error.code) first_error = e; else jz_err_drop(&e);
        }
    }
    jz_btree_drop(&abandonments);
    if (first_error.code) return first_error;
    if (out) *out = count;
    return JZ_OK;
}

static jz_err finish_transaction_abandonment_shutdown_in(jazz_db_Node *self, jazz_node_NodeState *node, u32 *out) {
    jz_err drain_result = drain_transaction_abandonments_in(self, node, out);
    /* Shutdown must not retire every open transaction until the FIFO owner
     * has reached quiescence. A queued begin/stage/commit was admitted
     * before close closed the gate, but may not have acquired the node
     * lock yet. Sweeping it from an intervening ordinary tick would turn
     * that accepted operation into a tombstoned transaction. Keep the
     * shutdown sweep pending until its own queue has completely drained;
     * a later owner turn or close will run this same idempotent sweep. */
    if (self->transaction_abandonment_shutdown_pending && NODE_FN(owner_queue_is_quiescent)(self)) {
        self->transaction_abandonment_shutdown_pending = false;
        NS_FN(abandon_all_open_transactions)(node);
    }
    return drain_result;
}

/// Lock the node for opening a transaction while the database still owns
/// transaction admission.
///
/// `owner_operation_admitted` is true only for an operation already
/// retained by the binding owner before shutdown. Those operations must
/// drain in FIFO order before the final sweep. Every direct caller checks
/// the gate both before and after waiting for the node mutex.
///
/// The tombstone check closes the corresponding race with an
/// already-linearized handle abandonment. On success the node stays locked.
jz_err NODE_FN(lock_for_transaction_open)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, bool owner_operation_admitted, jazz_node_NodeState **out) {
    if (!owner_operation_admitted) JZ_TRY(ensure_transaction_admission_open(self));
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    if (!owner_operation_admitted) {
        jz_err error = ensure_transaction_admission_open(self);
        if (error.code) {
            jz_err e = finish_transaction_abandonment_shutdown_in(self, node, NULL);
            jazz_db_node_unlock(self->node);
            if (e.code) { jz_err_drop(&error); return e; }
            return error;
        }
    }
    jz_err e = reject_tombstoned_transaction_in(self, node, open_tx_id);
    if (e.code) { jazz_db_node_unlock(self->node); return e; }
    *out = node;
    return JZ_OK;
}

/// Lock the node for an operation on an existing transaction.
///
/// A handle drop records its tombstone without waiting for this mutex.
/// Therefore even an operation already queued ahead of the maintenance tick
/// observes and retires the tombstone before it can stage, read, or commit.
/// Binding-owner operations accepted before shutdown may finish draining;
/// direct callers observe the closed gate after acquiring ownership.
jz_err NODE_FN(lock_for_transaction_operation)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, bool owner_operation_admitted, jazz_node_NodeState **out) {
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    jz_err e = reject_tombstoned_transaction_in(self, node, open_tx_id);
    if (e.code) { jazz_db_node_unlock(self->node); return e; }
    if (!owner_operation_admitted) {
        jz_err error = ensure_transaction_admission_open(self);
        if (error.code) {
            jz_err e2 = finish_transaction_abandonment_shutdown_in(self, node, NULL);
            jazz_db_node_unlock(self->node);
            if (e2.code) { jz_err_drop(&error); return e2; }
            return error;
        }
    }
    *out = node;
    return JZ_OK;
}

bool NODE_FN(mark_transaction_abandoned)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id) {
    return !jz_btree_insert(self->pending_transaction_abandonments, &open_tx_id, NULL, NULL);
}

void NODE_FN(clear_transaction_abandonment)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id) {
    jz_btree_remove(self->pending_transaction_abandonments, &open_tx_id, NULL, NULL);
}

/// Abandon an RAII-owned transaction immediately when the node is
/// uncontended, otherwise leave its deduplicated tombstone for the next
/// transaction operation or asynchronous owner turn.
///
/// This is synchronous and never waits for the node mutex.
void NODE_FN(abandon_or_enqueue_transaction)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id) {
    if (self->transaction_abandonments_closed) {
        /* Shutdown owns every still-open transaction after closing this
         * gate, so a late Drop has no separate maintenance to admit. */
        return;
    }
    if (!NODE_FN(mark_transaction_abandoned)(self, open_tx_id)) return;
    jazz_node_NodeState *node = jazz_db_node_try_lock(self->node);
    if (node) {
        jz_err e = abandon_transaction_for_maintenance(node, open_tx_id);
        if (!e.code) {
            NODE_FN(clear_transaction_abandonment)(self, open_tx_id);
            if (NS_FN(has_released_large_values)(node)) NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
        } else {
            jz_err_drop(&e);
            NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
        }
        jazz_db_node_unlock(self->node);
        return;
    }
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

static jz_err drain_transaction_abandonments(jazz_db_Node *self, u32 *out) {
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    jz_err drained = finish_transaction_abandonment_shutdown_in(self, node, out);
    /* Groove defers eviction while a local publication is resident; a
     * later tick picks the released roots up once it has settled. */
    if (NS_FN(has_released_large_values)(node) && !NODE_FN(has_pending_local_publications)(self))
        NS_FN(evict_released_large_values)(node);
    jazz_db_node_unlock(self->node);
    return drained;
}

/// Close transaction admission and transfer the final open-transaction
/// sweep to the node tick owner before the close future can suspend.
///
/// If the caller cancels `Db::close` while it is waiting for the node
/// mutex, the scheduled tick still drains tombstones and terminalizes every
/// open transaction. Repeated close attempts and tick passes are benign.
void NODE_FN(begin_transaction_abandonment_shutdown)(jazz_db_Node *self) {
    bool was = self->transaction_abandonments_closed;
    self->transaction_abandonments_closed = true;
    if (was) return;
    self->transaction_abandonment_shutdown_pending = true;
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

jz_err NODE_FN(finish_transaction_abandonment_shutdown)(jazz_db_Node *self, u32 *out) {
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    jz_err drained = finish_transaction_abandonment_shutdown_in(self, node, out);
    jazz_db_node_unlock(self->node);
    /* No later owner turn runs after close, so release now what the
     * retired transactions staged. */
    NODE_FN(evict_released_large_values)(self);
    return drained;
}

/// Best-effort eviction of Groove roots staged by abandoned transactions,
/// after settling earlier local publications so Groove does not defer it.
/// Whatever this misses is reclaimed by the staging TTL.
void NODE_FN(evict_released_large_values)(jazz_db_Node *self) {
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    bool has = NS_FN(has_released_large_values)(node);
    jazz_db_node_unlock(self->node);
    if (!has) return;
    jz_err e = NODE_FN(lock_for_large_value_staging)(self, &node);
    if (!e.code) {
        NS_FN(evict_released_large_values)(node);
        jazz_db_node_unlock(self->node);
    } else {
        jz_err_drop(&e);
    }
}

/* ---------------- subscription finalization ---------------- */

static void release_redundant_discovery(jazz_node_NodeState *owner, bool has_redundant, jazz_db_DeletionReconciliation *redundant);

/// Enqueue a stream-finalization command without touching the async node
/// mutex. This is the only operation a stream's `Drop` implementation may
/// perform. A closed node has already retired its runtime, so later
/// commands are safely invalidated and acknowledged immediately.
void NODE_FN(enqueue_subscription_finalization)(jazz_db_Node *self, jazz_db_PendingSubscriptionFinalization command) {
    if (self->subscription_finalizations_closed && self->subscription_runtime_retired) {
        /* Terminal retirement already drained every maintained view
         * and connection, so a late finalizer owns no resident work. */
        if (command.acknowledgement) {
            jazz_db_Oneshot *ack = command.acknowledgement;
            command.acknowledgement = NULL;
            (void)jazz_db__oneshot__send(ack, NULL);
            jazz_db__oneshot__drop_sender(ack);
        }
        jazz_db__db__PendingSubscriptionFinalization__drop(&command);
        return;
    }
    jz_deque_push_back(self->pending_subscription_finalizations, &command);
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

typedef struct RetainCoverageCtx { const jazz_db_UpstreamCoverageHandle *handle; } RetainCoverageCtx;
static bool retain_other_coverage(const void *k, void *v, void *ctx) {
    const RetainCoverageCtx *c = (const RetainCoverageCtx *)ctx;
    const jazz_db_CoverageKey *coverage = (const jazz_db_CoverageKey *)k;
    const jazz_protocol_SubscriptionKey *subscription = (const jazz_protocol_SubscriptionKey *)v;
    return !jazz_db__db__CoverageKey__eq(coverage, &c->handle->coverage) && !jazz_protocol_SubscriptionKey_eq(subscription, &c->handle->subscription);
}

/// Drain queued stream cleanup under the ordinary async node owner. There
/// is no await after the queue is taken, so cancellation while waiting for
/// the mutex leaves the command queued for a later owner turn.
jz_err NODE_FN(drain_subscription_finalizations)(jazz_db_Node *self, u32 *out) {
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    jz_deque commands = *self->pending_subscription_finalizations;
    jz_deque_init(self->pending_subscription_finalizations, sizeof(jazz_db_PendingSubscriptionFinalization));
    u32 drained = 0;
    bool changed_upstream = false;
    jazz_db_PendingSubscriptionFinalization command;
    while (jz_deque_pop_front(&commands, &command)) {
        jazz_db_OpeningLocal local = {0};
        jazz_db_UpstreamCoverageHandleVec upstream = {0};
        jazz_db_SubscriptionState *owner = NULL; /* Weak */
        if (command.state) {
            jazz_db_SubscriptionState *state = command.state;
            command.state = NULL;
            owner = jazz_db_rc_downgrade(state);
            jazz_db__db__ScalarReconciliation__drop(&state->scalar_reconciliation);
            jazz_db__db__ScalarReconciliation__default(&state->scalar_reconciliation);
            /* A closing stream stops withholding, never reconciling: an
             * unfinished discovery and unprobed rows move to the runtime
             * (INV-SYNC-48). Rows already in `Probing` are runtime-owned. */
            jazz_db_DeletionReconciliation reconciliation = state->deletion_reconciliation;
            memset(&state->deletion_reconciliation, 0, sizeof(state->deletion_reconciliation));
            state->deletion_reconciliation.tag = JAZZ_DR_DISABLED;
            if (reconciliation.tag == JAZZ_DR_PENDING) {
                JZ_VEC(jazz_db_HeldRowKey) keys = {0};
                jazz_db__db__HeldRowChecks__enqueue(&self->held_row_checks, &reconciliation.rows, &reconciliation.context, &keys);
                jz_vec_drop(&keys, sizeof(jazz_db_HeldRowKey), jazz_db__db__HeldRowKey__drop);
                memset(&reconciliation.rows, 0, sizeof(reconciliation.rows));
                jazz_db__db__DeletionReconciliation__drop(&reconciliation);
                NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
            } else if (reconciliation.tag == JAZZ_DR_DEFERRED || reconciliation.tag == JAZZ_DR_DISCOVERING) {
                jazz_db_DeletionReconciliation redundant;
                bool has_redundant = jazz_db__db__HeldRowChecks__adopt_discovery(&self->held_row_checks, reconciliation, &redundant);
                release_redundant_discovery(node, has_redundant, &redundant);
                NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
            } else {
                jazz_db__db__DeletionReconciliation__drop(&reconciliation);
            }
            upstream = state->upstream_subscription_handles;
            memset(&state->upstream_subscription_handles, 0, sizeof(upstream));
            jz_vec_extend(&upstream, state->authority_witness.ptr, state->authority_witness.len, sizeof(jazz_db_UpstreamCoverageHandle));
            state->authority_witness.len = 0;
            if (state->local_subscription_cleanup && state->local_subscription_cleanup->has) {
                local.has = true;
                local.token = state->local_subscription_cleanup->token;
                local.id = state->local_subscription_cleanup->id;
                state->local_subscription_cleanup->has = false;
            }
            jz_rc_release(state);
        } else {
            local = command.opening_local;
            command.opening_local.has = false;
            upstream = command.opening_upstream;
            memset(&command.opening_upstream, 0, sizeof(upstream));
        }
        if (local.has && NS_FN(groove_runtime_token)(node) == local.token)
            NS_FN(unsubscribe_groove_subscription)(node, local.id);
        for (u32 i = 0; i < upstream.len; ++i) {
            jazz_db_UpstreamCoverageHandle *handle = &upstream.ptr[i];
            jazz_db__db__node_runtime__unregister_upstream_subscription_owner(self->upstream_subscription_owners, handle->subscription, owner);
            u64 *count = (u64 *)jz_btree_get(self->upstream_coverage_refcounts, &handle->coverage);
            if (!count) continue;
            if (*count) *count -= 1;
            if (*count > 0) continue;
            jz_btree_remove(self->upstream_coverage_refcounts, &handle->coverage, NULL, NULL);
            jz_btree_remove(self->awaiting_initial_authority_coverage, &handle->coverage, NULL, NULL);
            NS_FN(apply_unsubscribe)(node, handle->subscription);
            jz_btree_remove(self->coverage_refresh_generations, &handle->coverage, NULL, NULL);
            if (self->active_authority_view_receipts->has)
                jz_btree_remove(&self->active_authority_view_receipts->subscriptions, &handle->subscription, NULL, NULL);
            RetainCoverageCtx ctx = { handle };
            jz_btree_retain(self->latest_coverage_subscriptions, retain_other_coverage, &ctx);
            jazz_db_PendingUpstreamCommand cmd;
            memset(&cmd, 0, sizeof(cmd));
            cmd.tag = JAZZ_PUC_UNSUBSCRIBE;
            cmd.unsubscribe = handle->subscription;
            jz_vec_push(self->upstream_subscriptions, &cmd, sizeof(cmd));
            changed_upstream = true;
        }
        jz_vec_drop(&upstream, sizeof(jazz_db_UpstreamCoverageHandle), jazz_db__db__UpstreamCoverageHandle__drop);
        if (owner) jazz_db_weak_release(owner);
        if (command.acknowledgement) {
            jazz_db_Oneshot *ack = command.acknowledgement;
            command.acknowledgement = NULL;
            (void)jazz_db__oneshot__send(ack, NULL);
            jazz_db__oneshot__drop_sender(ack);
        }
        jazz_db__db__PendingSubscriptionFinalization__drop(&command);
        drained += 1;
    }
    jz_deque_drop(&commands, NULL);
    jazz_db_node_unlock(self->node);
    if (changed_upstream) NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    *out = drained;
    return JZ_OK;
}

/// Atomically close the admission gate and enqueue every live stream for
/// retirement.  `Db::close` calls this before its first await, so a drop
/// racing storage shutdown cannot land between a drain and the durable
/// close. Late finalizers may be acknowledged only after this method has
/// made their state part of the terminal retirement set.
void NODE_FN(begin_subscription_finalization_shutdown)(jazz_db_Node *self) {
    bool was = self->subscription_finalizations_closed;
    self->subscription_finalizations_closed = true;
    if (was) return;
    for (u32 i = 0; i < self->subscriptions->len; ++i) {
        jazz_db_SubscriptionState *state = jazz_db_weak_upgrade(self->subscriptions->ptr[i]);
        if (!state) continue;
        *state->closed = true;
        jazz_db_PendingSubscriptionFinalization pending;
        memset(&pending, 0, sizeof(pending));
        pending.state = state;
        jz_deque_push_back(self->pending_subscription_finalizations, &pending);
    }
    /* Once admission closes, an abandoned close future must not be the
     * sole owner capable of retiring the captured streams. */
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

static void drop_peer_mutex_ptr(void *p) { jz_rc_release(*(void **)p); }
static void drop_weak_state(void *p) { jazz_db_weak_release(*(jazz_db_SubscriptionState **)p); }

/// Release all connection and subscription bookkeeping after its backing
/// storage has closed.  No later finalizer can leave a live local Groove
/// view or upstream ownership behind: the retirement pass drained them
/// before close, and this removes the now-unusable runtime shell.
void NODE_FN(retire_subscription_runtime_after_close)(jazz_db_Node *self) {
    self->subscription_runtime_retired = true;
    jz_vec_truncate(self->subscriptions, 0, sizeof(jazz_db_SubscriptionState *), drop_weak_state);
    jz_vec_truncate(&self->connections, 0, sizeof(jazz_db_PeerMutex *), drop_peer_mutex_ptr);
    jz_vec_truncate(&self->pending_detaches, 0, sizeof(jazz_db_PeerMutex *), drop_peer_mutex_ptr);
    jz_vec_truncate(self->upstream_subscriptions, 0, sizeof(jazz_db_PendingUpstreamCommand), jazz_db__db__PendingUpstreamCommand__drop);
    jz_deque_drop(self->pending_subscription_finalizations, jazz_db__db__PendingSubscriptionFinalization__drop);
    jz_deque_init(self->pending_subscription_finalizations, sizeof(jazz_db_PendingSubscriptionFinalization));
    jz_btree_clear(self->latest_coverage_subscriptions);
    jz_btree_clear(self->upstream_coverage_refcounts);
    jz_btree_clear(self->awaiting_initial_authority_coverage);
    jz_btree_clear(self->query_coverage_registrations);
    jz_btree_clear(self->upstream_subscription_owners);
    jz_btree_clear(self->relay_upstream_subscription_owners);
    jz_btree_clear(self->pending_relay_subscription_rejections);
}

/* ---------------- mutation errors ---------------- */

void NODE_FN(set_mutation_error_callback)(jazz_db_Node *self, const jazz_db_MutationErrorCallback *callback) {
    jazz_db_MutationErrorState *state = self->mutation_errors;
    state->has_callback = callback != NULL;
    if (callback) state->callback = *callback;
    bool should_schedule = state->has_callback && state->pending.len != 0;
    if (should_schedule) NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

static void consume_mutation_error(jazz_db_Node *self, jazz_model_TxId tx_id) {
    jz_btree_remove(&self->mutation_errors->pending, &tx_id, NULL, NULL);
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node);
    bool retained = NS_FN(rejected_transaction)(node, tx_id) != NULL;
    jazz_db_node_unlock(self->node);
    if (retained) {
        jz_btree_insert(&self->deferred_rejection_discards, &tx_id, NULL, NULL);
        NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    }
}

/// Finish rejection acknowledgement after all wait observers woken by one
/// owner turn have had a chance to inspect the shared terminal state.
jz_err NODE_FN(flush_deferred_rejection_discards)(jazz_db_Node *self) {
    jz_btree tx_ids = self->deferred_rejection_discards;
    jz_btree_init(&self->deferred_rejection_discards, tx_ids.d);
    jz_err first_error = JZ_OK;
    jazz_model_TxId *tx_id; void *unused;
    JZ_BTREE_FOR(&tx_ids, tx_id, unused) {
        jazz_node_NodeState *node = jazz_db_node_lock(self->node);
        jz_err e = NS_FN(discard_rejection)(node, *tx_id);
        jazz_db_node_unlock(self->node);
        if (e.code) {
            jz_btree_insert(&self->deferred_rejection_discards, tx_id, NULL, NULL);
            if (!first_error.code) first_error = e; else jz_err_drop(&e);
        }
    }
    jz_btree_drop(&tx_ids);
    if (first_error.code) {
        /* A failed atomic Groove commit poisons this opened database. Do
         * not convert its retained acknowledgement into an immediate tick
         * loop: the caller must observe the error and reopen before this
         * runtime can make durable progress again. Other errors are also
         * retained, but have no generic retry-safety contract here, so a
         * host may explicitly service a later owner turn without us
         * spinning on an unknown storage failure. */
        jz_str shown = {0};
        jazz_node__node__Error__display(&shown, &first_error);
        if (first_error.code == JAZZ_NODE_ERR_GROOVE_DATABASE_POISONED)
            JZ_TRACE(1, "jazz_db::db::node_runtime", "error=%.*s deferred mutation-error acknowledgement requires reopening the database", (int)shown.len, shown.ptr);
        else
            JZ_TRACE(2, "jazz_db::db::node_runtime", "error=%.*s deferred mutation-error acknowledgement retained for an explicit later retry", (int)shown.len, shown.ptr);
        jz_str_drop(&shown);
        return jazz_node__node__api_error__Error__From__from(first_error);
    }
    return JZ_OK;
}

/* ---------------- write waits ---------------- */

static jz_err not_observed_tx(const char *fmt_prefix, jazz_model_TxId tx_id, const char *suffix) {
    jz_str dbg = {0};
    jazz_model__tx__TxId__debug(&dbg, &tx_id);
    jz_err e = jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "%s%.*s%s", fmt_prefix, (int)dbg.len, dbg.ptr, suffix);
    jz_str_drop(&dbg);
    return e;
}

static bool fate_outcome(jazz_model_TxId public_tx_id, jazz_model_Fate *fate, bool has_gt, jazz_types_GlobalTime gt,
                         jazz_model_DurabilityTier durability, jazz_model_DurabilityTier tier, jz_err *err, jazz_model_TxId *out) {
    bool satisfied = jazz_db__db__transaction_satisfies_wait(fate, has_gt, gt, durability, tier);
    if (fate->tag == JAZZ_FATE_REJECTED) {
        *err = jazz_db__db__write_rejected(public_tx_id, &fate->rejected);
        jazz_model__tx__Fate__drop(fate);
        return true;
    }
    jazz_model__tx__Fate__drop(fate);
    if (satisfied) { *err = JZ_OK; *out = public_tx_id; return true; }
    return false;
}

bool NODE_FN(transaction_wait_outcome)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_model_DurabilityTier tier, jz_err *err, jazz_model_TxId *out) {
    jz_err *error = (jz_err *)jz_btree_get(&self->queued_mutation_failures, &tx_id);
    if (error) { *err = jazz_db_err_clone(error); return true; }
    if (jz_btree_contains(&self->reserved_mutations, &tx_id)) return false;
    jazz_model_Fate fate; bool has_gt; jazz_types_GlobalTime gt; jazz_model_DurabilityTier durability;
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    bool known = NS_FN(transaction_state)(node, tx_id, &fate, &has_gt, &gt, &durability);
    jazz_db_node_unlock(self->node);
    if (!known) { *err = not_observed_tx("transaction ", tx_id, " is not known locally"); return true; }
    return fate_outcome(tx_id, &fate, has_gt, gt, durability, tier, err, out);
}

/// Read a transaction's wait predicate without claiming its rejection.
///
/// A queued empty update may use an existing transaction as its bounded
/// completion target. That public request must observe the target's state,
/// but it is not an additional owner of the target's mutation-error
/// delivery: the target's own waiter or mutation-error callback retains
/// that responsibility.
static bool completion_target_wait_outcome(jazz_db_Node *self, jazz_model_TxId public_tx_id, jazz_model_TxId target_tx_id, jazz_model_DurabilityTier tier, jz_err *err, jazz_model_TxId *out) {
    jazz_model_Fate fate; bool has_gt; jazz_types_GlobalTime gt; jazz_model_DurabilityTier durability;
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    bool known = NS_FN(transaction_state)(node, target_tx_id, &fate, &has_gt, &gt, &durability);
    jazz_db_node_unlock(self->node);
    if (!known) { *err = not_observed_tx("completion target for transaction ", public_tx_id, " is not known locally"); return true; }
    return fate_outcome(public_tx_id, &fate, has_gt, gt, durability, tier, err, out);
}

bool NODE_FN(queued_mutation_write_state)(const jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteState *out, jz_err *err) {
    jz_err *error = (jz_err *)jz_btree_get(&self->queued_mutation_failures, &tx_id);
    if (error) { *err = jazz_db_err_clone(error); return true; }
    if (!jz_btree_contains(&self->reserved_mutations, &tx_id)) return false;
    *err = JZ_OK;
    memset(out, 0, sizeof(*out));
    out->fate.tag = JAZZ_FATE_PENDING;
    out->has_global_time = false;
    out->durability = JAZZ_DUR_NONE;
    return true;
}

bool NODE_FN(take_queued_mutation_failure)(jazz_db_Node *self, jazz_model_TxId tx_id, jz_err *out) {
    if (!jz_btree_remove(&self->queued_mutation_failures, &tx_id, NULL, out)) return false;
    /* A binding returning the failure synchronously has already reported it. */
    consume_mutation_error(self, tx_id);
    return true;
}

/* The observe_write / wait_for_completion_target / observe_transaction loops
 * as one poll state machine. Each poll re-runs the loop until it would await
 * a retained WriteStateChange that has not fired yet. */
typedef struct WriteWait {
    jazz_db_Node *node;
    jazz_model_TxId tx_id;
    jazz_db_QueuedMutationAlias *alias; /* NULL = None */
    jazz_db_WriteWaitOptions options;
    bool has_error_wait;
    jazz_db_MutationErrorWait error_wait;
    bool has_target;
    jazz_model_TxId target_tx_id;
    bool waiting;
    jazz_db_WriteStateChange state_change;
} WriteWait;

static bool closing_without_reservation(jazz_db_Node *self, jazz_model_TxId tx_id) {
    return self->mutation_owner_lifecycle == JAZZ_OWNER_CLOSING && !jz_btree_contains(&self->reserved_mutations, &tx_id);
}
static jz_err closed_before(jazz_model_TxId tx_id, jazz_model_DurabilityTier tier) {
    jz_str dbg = {0}, t = {0};
    jazz_model__tx__TxId__debug(&dbg, &tx_id);
    jazz_model__tx__DurabilityTier__debug(&t, tier);
    jz_err e = jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "database closed before transaction %.*s reached %.*s", (int)dbg.len, dbg.ptr, (int)t.len, t.ptr);
    jz_str_drop(&dbg); jz_str_drop(&t);
    return e;
}

static void WriteWait_drop_change(WriteWait *w) {
    if (w->waiting) { jazz_db__db__WriteStateChange__drop(&w->state_change); w->waiting = false; }
}

/* Returns true when the observation finished with (*err, *out). */
static bool WriteWait_poll_observe(WriteWait *w, jz_err *err, jazz_model_TxId *out) {
    jazz_db_Node *self = w->node;
    jazz_model_DurabilityTier tier = (jazz_model_DurabilityTier)w->options.tier;
    for (;;) {
        if (w->waiting) {
            /* state_change.await */
            if (!jazz_db__oneshot__try_recv(w->state_change.receiver, NULL) && !jazz_db__oneshot__is_canceled(w->state_change.receiver))
                return false;
            WriteWait_drop_change(w);
        }
        if (w->has_target) {
            /* wait_for_completion_target */
            if (completion_target_wait_outcome(self, w->tx_id, w->target_tx_id, tier, err, out)) return true;
            w->state_change = NODE_FN(register_write_state_waiter)(self, w->target_tx_id);
            w->waiting = true;
            if (completion_target_wait_outcome(self, w->tx_id, w->target_tx_id, tier, err, out)) { WriteWait_drop_change(w); return true; }
            continue;
        }
        if (w->alias) {
            if (w->alias->has) { w->has_target = true; w->target_tx_id = w->alias->tx_id; continue; }
            if (NODE_FN(transaction_wait_outcome)(self, w->tx_id, tier, err, out)) return true;
            if (closing_without_reservation(self, w->tx_id)) { *err = closed_before(w->tx_id, tier); return true; }
            w->state_change = NODE_FN(register_write_state_waiter)(self, w->tx_id);
            w->waiting = true;
            if (w->alias->has) { WriteWait_drop_change(w); w->has_target = true; w->target_tx_id = w->alias->tx_id; continue; }
            if (NODE_FN(transaction_wait_outcome)(self, w->tx_id, tier, err, out)) { WriteWait_drop_change(w); return true; }
            continue;
        }
        /* observe_transaction */
        if (NODE_FN(transaction_wait_outcome)(self, w->tx_id, tier, err, out)) return true;
        if (closing_without_reservation(self, w->tx_id)) { *err = closed_before(w->tx_id, tier); return true; }
        w->state_change = NODE_FN(register_write_state_waiter)(self, w->tx_id);
        w->waiting = true;
        if (NODE_FN(transaction_wait_outcome)(self, w->tx_id, tier, err, out)) { WriteWait_drop_change(w); return true; }
        if (closing_without_reservation(self, w->tx_id)) { WriteWait_drop_change(w); *err = closed_before(w->tx_id, tier); return true; }
    }
}

/* wait_for_write: observe, then consume the mutation error unless observe-only. */
static bool WriteWait_poll(WriteWait *w, jz_err *err, jazz_model_TxId *out) {
    if (!WriteWait_poll_observe(w, err, out)) return false;
    if (err->code && !w->options.observe_only) consume_mutation_error(w->node, w->tx_id);
    return true;
}

static void WriteWait_init(WriteWait *w, jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_QueuedMutationAlias *alias, jazz_db_WriteWaitOptions options);

static void WriteWait_release(WriteWait *w) {
    WriteWait_drop_change(w);
    if (w->has_error_wait) { jazz_db__mutation_errors__MutationErrorWait__drop(&w->error_wait); w->has_error_wait = false; }
    if (w->alias) { jz_rc_release(w->alias); w->alias = NULL; }
}

/* transaction_wait_observers entry: Box::pin(async move { callback(node.wait_for_write(..).await) }) */
typedef struct WriteWaitObserver { WriteWait wait; jazz_db_TxIdCallback callback; jazz_db_Node *node_rc; } WriteWaitObserver;
static bool WriteWaitObserver_poll(void *p, const jz_waker *cx, void *out) {
    WriteWaitObserver *o = (WriteWaitObserver *)p;
    (void)cx; (void)out;
    jz_err err = JZ_OK;
    jazz_model_TxId tx = {0};
    if (!WriteWait_poll(&o->wait, &err, &tx)) return false;
    jazz_db_TxIdCallback cb = o->callback;
    o->callback.call = NULL;
    cb.call(cb.ctx, err, tx);
    if (cb.drop) cb.drop(cb.ctx);
    return true;
}
static void WriteWaitObserver_drop(void *p) {
    WriteWaitObserver *o = (WriteWaitObserver *)p;
    WriteWait_release(&o->wait);
    if (o->callback.call && o->callback.drop) o->callback.drop(o->callback.ctx);
    jz_rc_release(o->node_rc);
    jz_free(o);
}

static bool register_mutation_error_wait(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteWaitOptions options, jazz_db_MutationErrorWait *out) {
    if (options.observe_only) return false;
    jazz_db__mutation_errors__MutationErrorWait__new(out, self->mutation_errors, self->scheduler, tx_id);
    return true;
}

static void WriteWait_init(WriteWait *w, jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_QueuedMutationAlias *alias, jazz_db_WriteWaitOptions options) {
    memset(w, 0, sizeof(*w));
    w->node = self;
    w->tx_id = tx_id;
    w->alias = alias ? (jazz_db_QueuedMutationAlias *)jz_rc_clone(alias) : NULL;
    w->options = options;
    w->has_error_wait = register_mutation_error_wait(self, tx_id, options, &w->error_wait);
}

void NODE_FN(wait_for_transaction_with)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteWaitOptions options, jazz_db_TxIdCallback callback) {
    NODE_FN(wait_for_write_with)(self, tx_id, NULL, options, callback);
}

/// Callback wait for a binding-owned write handle. A queued empty update
/// reserves a request id before it can asynchronously discover that the
/// update is a no-op; its handle-local alias then points at the existing
/// transaction whose state satisfies this wait. The alias is retained by
/// the caller, never by this runtime.
void NODE_FN(wait_for_write_with)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_QueuedMutationAlias *alias, jazz_db_WriteWaitOptions options, jazz_db_TxIdCallback callback) {
    if (self->mutation_owner_lifecycle == JAZZ_OWNER_CLOSING) {
        jazz_model_TxId none = {0};
        callback.call(callback.ctx, not_observed_tx("database is closed; transaction ", tx_id, " cannot be observed"), none);
        if (callback.drop) callback.drop(callback.ctx);
        return;
    }
    WriteWaitObserver *o = JZ_NEW(WriteWaitObserver);
    o->node_rc = (jazz_db_Node *)jz_rc_clone(self);
    WriteWait_init(&o->wait, self, tx_id, alias, options);
    o->callback = callback;
    jazz_db_TransactionWaitObserver observer = { WriteWaitObserver_poll, WriteWaitObserver_drop, o };
    jz_vec_push(&self->transaction_wait_observers, &observer, sizeof(observer));
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

jz_err NODE_FN(wait_for_transaction)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteWaitOptions options, jazz_model_TxId *out) {
    WriteWait w;
    WriteWait_init(&w, self, tx_id, NULL, options);
    jz_err err = JZ_OK;
    while (!WriteWait_poll(&w, &err, out)) jz_task_run_ready();
    WriteWait_release(&w);
    return err;
}

static bool poll_observer_pending(void *item, void *ctx) {
    jazz_db_TransactionWaitObserver *observer = (jazz_db_TransactionWaitObserver *)item;
    const jz_waker *w = (const jz_waker *)ctx;
    if (jazz_db_future_poll(observer, w, NULL)) { jazz_db_future_drop(observer); return false; }
    return true;
}

/* observers.retain_mut(poll == Pending); then splice the survivors before any new ones */
static bool poll_observers_once(jazz_db_Node *self, const jz_waker *waker) {
    jazz_db_TransactionWaitObserverVec observers = self->transaction_wait_observers;
    memset(&self->transaction_wait_observers, 0, sizeof(observers));
    jz_vec_retain(&observers, sizeof(jazz_db_TransactionWaitObserver), poll_observer_pending, (void *)waker, NULL);
    bool empty = observers.len == 0;
    jazz_db_TransactionWaitObserverVec added = self->transaction_wait_observers;
    jz_vec_extend(&observers, added.ptr, added.len, sizeof(jazz_db_TransactionWaitObserver));
    jz_vec_drop(&added, sizeof(jazz_db_TransactionWaitObserver), NULL);
    self->transaction_wait_observers = observers;
    return empty;
}

void NODE_FN(poll_transaction_wait_observers)(jazz_db_Node *self) {
    static const jz_waker noop = {0};
    jz_waker owned;
    bool has = NODE_FN(query_runtime_waker)(self, &owned);
    (void)poll_observers_once(self, has ? &owned : &noop);
}

void NODE_FN(drain_transaction_wait_observers)(jazz_db_Node *self) {
    static const jz_waker noop = {0};
    while (!poll_observers_once(self, &noop)) jz_task_run_ready();
}

/// Deliver only at the start of the outer Db turn, before queued mutations
/// can produce new failures. All origins get the same following-turn
/// opportunity for an application wait to claim the error.
void NODE_FN(deliver_pending_mutation_errors)(jazz_db_Node *self) {
    jazz_db_MutationErrorDelivery delivery;
    if (!jazz_db__mutation_errors__take_pending_mutation_error_delivery(self->mutation_errors, &delivery)) return;
    for (u32 i = 0; i < delivery.events.len; ++i) {
        consume_mutation_error(self, delivery.events.ptr[i].tx);
        delivery.callback.call(delivery.callback.ctx, delivery.events.ptr[i].event);
    }
    jz_vec_drop(&delivery.events, sizeof(*delivery.events.ptr), NULL);
}

/* ---------------- permission advice ---------------- */

static jazz_db_PermissionAdviceFuture push_scope_intent(jazz_db_Node *self, jazz_protocol_PermissionAdviceAction *action, bool delegated, jazz_protocol_DelegatedSessionBinding session) {
    jazz_protocol_PermissionAdviceRequestId request_id;
    jz_uuid u = jz_uuid_v4();
    memcpy(request_id.b, u.b, 16);
    jazz_db_Oneshot *channel = jazz_db__oneshot__channel(sizeof(u8));
    channel->refs++; /* sender half stored in the waiters map */
    jz_btree_insert(self->permission_advice_waiters, &request_id, &channel, NULL);
    jazz_db_PendingUpstreamCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.tag = JAZZ_PUC_AUTHORIZATION_SCOPE_INTENT;
    cmd.intent.request_id = request_id;
    cmd.intent.action = action;
    if (delegated) {
        cmd.intent.has_session_claim_binding = true;
        cmd.intent.session_claim_binding.identity = session.identity;
        jz_btree_clone(&cmd.intent.session_claim_binding.claims, &session.claims, str_key_clone, groove__records__Value__clone);
        cmd.intent.has_delegated_session = true;
        cmd.intent.delegated_session = session;
    }
    jz_vec_push(self->upstream_subscriptions, &cmd, sizeof(cmd));
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    jazz_db_PermissionAdviceFuture f = { (jz_btree *)jz_rc_clone(self->permission_advice_waiters), request_id, channel };
    return f;
}

jazz_db_PermissionAdviceFuture NODE_FN(request_permission_advice)(jazz_db_Node *self, jazz_protocol_PermissionAdviceAction *action) {
    jazz_protocol_DelegatedSessionBinding none;
    memset(&none, 0, sizeof(none));
    return push_scope_intent(self, action, false, none);
}

jazz_db_PermissionAdviceFuture NODE_FN(request_permission_advice_with_delegated_session)(jazz_db_Node *self, jazz_protocol_PermissionAdviceAction *action, jazz_protocol_DelegatedSessionBinding session) {
    return push_scope_intent(self, action, true, session);
}

static bool lease_request_has_waiters(const void *k, void *v, void *ctx) {
    (void)k; (void)ctx;
    return ((jazz_db_AuthorizationScopeLeaseRequest *)v)->waiters.len != 0;
}

void NODE_FN(cancel_permission_advice_request)(jazz_db_Node *self, jazz_protocol_PermissionAdviceRequestId request_id) {
    jazz_db_Oneshot *sender = NULL;
    if (jz_btree_remove(self->permission_advice_waiters, &request_id, NULL, &sender) && sender)
        jazz_db__oneshot__drop_sender(sender);
    for (u32 i = 0; i < self->connections.len; ++i) {
        jazz_db_PeerMutex *m = self->connections.ptr[i];
        jazz_db_PeerConnection *connection = jazz_db_peer_try_lock(m);
        if (!connection) jz_panic("connection is already borrowed");
        if (connection->link.tag == JAZZ_LINK_UPSTREAM) {
            jazz_db_AuthorizationScopeLeaseManager *scope_lease_manager = &connection->link.v.upstream.scope_lease_manager;
            bool empty = false;
            void *k; jazz_db_AuthorizationScopeLeaseRequest *request;
            JZ_BTREE_FOR(&scope_lease_manager->requests, k, request) {
                if (jz_btree_remove(&request->waiters, &request_id, NULL, NULL)) empty |= request->waiters.len == 0;
            }
            if (empty) jz_btree_retain(&scope_lease_manager->requests, lease_request_has_waiters, NULL);
        }
        jazz_db_peer_unlock(m);
    }
}

jazz_db_WriteStateChange NODE_FN(register_write_state_waiter)(jazz_db_Node *self, jazz_model_TxId tx_id) {
    u64 waiter_id = self->next_write_state_waiter_id;
    u64 next = waiter_id + 1;
    self->next_write_state_waiter_id = next > 1 ? next : 1;
    jazz_db_Oneshot *channel = jazz_db__oneshot__channel(0);
    channel->refs++; /* sender half */
    bool inserted;
    jazz_db_WriteStateWaiterVec *waiters = (jazz_db_WriteStateWaiterVec *)jz_btree_entry(self->write_state_waiters, &tx_id, &inserted);
    jazz_db_WriteStateWaiter waiter = { waiter_id, channel };
    jz_vec_push(waiters, &waiter, sizeof(waiter));
    jazz_db_WriteStateChange change = { (jz_btree *)jz_rc_clone(self->write_state_waiters), tx_id, waiter_id, channel };
    return change;
}

jz_err NODE_FN(refresh_subscriptions)(jazz_db_Node *self, u32 *out) {
    return NODE_FN(refresh_subscriptions_with_tables)(self, NULL, out);
}

jz_err NODE_FN(refresh_subscriptions_with_tables)(jazz_db_Node *self, const jz_hmap *changed_tables, u32 *out) {
    jz_waker progress_waker;
    bool has = NODE_FN(query_runtime_waker)(self, &progress_waker);
    return jazz_db__db__node_runtime__refresh_subscriptions_in_with_tables(self->node, self->subscriptions, self->active_authority_view_receipts, changed_tables, has ? &progress_waker : NULL, out);
}

/// Retire authority settlement synchronously without re-evaluating data.
///
/// Connection replacement changes Jazz status, not the Groove terminal
/// baseline. Keeping this path non-suspending ensures consumers observe
/// the demotion before the replacement transport can publish anything.
static void demote_authority_receipt_subscriptions(jazz_db_Node *self) {
    jz_btree empty;
    jz_btree_init(&empty, &jazz_db_subscription_key_set_desc);
    jazz_db__db__demote_authority_receipt_subscriptions(self->subscriptions, &empty);
}

/* ---------------- connection attachment ---------------- */

static u64 random_connection_epoch(void) {
    /* uuid::Uuid::new_v4().as_u128() as u64: the low 64 bits */
    jz_uuid u = jz_uuid_v4();
    u64 v = 0;
    for (int i = 8; i < 16; ++i) v = (v << 8) | u.b[i];
    return v;
}

static void PeerMutex_drop(void *p) {
    jazz_db_PeerMutex *m = (jazz_db_PeerMutex *)p;
    if (m->conn) { PC_FN(drop)(m->conn); jz_free(m->conn); m->conn = NULL; }
}

static jazz_db_PeerMutex *new_peer_mutex(jazz_db_PeerConnection *conn) {
    jazz_db_PeerMutex *m = (jazz_db_PeerMutex *)jz_rc_new(sizeof(jazz_db_PeerMutex), PeerMutex_drop);
    m->locked = 0;
    m->conn = conn;
    return m;
}

/* acquire_peer_inventory: lock every registered peer in registry order */
static void acquire_peer_inventory(const jazz_db_PeerMutexVec *peers) {
    for (u32 i = 0; i < peers->len; ++i) (void)jazz_db_peer_lock(peers->ptr[i]);
}
static void release_peer_inventory(const jazz_db_PeerMutexVec *peers, const jazz_db_PeerMutex *except) {
    for (u32 i = 0; i < peers->len; ++i) if (peers->ptr[i] != except) jazz_db_peer_unlock(peers->ptr[i]);
}
static void clone_peer_inventory(const jazz_db_Node *self, jazz_db_PeerMutexVec *out) {
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < self->connections.len; ++i) {
        jazz_db_PeerMutex *p = (jazz_db_PeerMutex *)jz_rc_clone(self->connections.ptr[i]);
        jz_vec_push(out, &p, sizeof(p));
    }
}
static void drop_peer_inventory(jazz_db_PeerMutexVec *peers) {
    jz_vec_drop(peers, sizeof(jazz_db_PeerMutex *), drop_peer_mutex_ptr);
}

static bool peer_inventory_matches(const jazz_db_Node *self, const jazz_db_PeerMutexVec *peers) {
    if (self->connections.len != peers->len) return false;
    for (u32 i = 0; i < peers->len; ++i) if (self->connections.ptr[i] != peers->ptr[i]) return false;
    return true;
}

/* Fill the fields shared by upstream and subscriber connections. */
static void init_shared_connection(jazz_db_Node *self, jazz_db_PeerConnection *c, jazz_db_Transport transport, u64 connection_epoch) {
    memset(c, 0, sizeof(*c));
    c->transport = transport;
    jz_deque_init(&c->staged_inbound, sizeof(jazz_db_StagedInboundMessage));
    c->node = (jazz_db_NodeMutex *)jz_rc_clone(self->node);
    c->subscriptions = (jazz_db_SubscriptionList *)jz_rc_clone(self->subscriptions);
    c->upstream_subscription_owners = (jz_btree *)jz_rc_clone(self->upstream_subscription_owners);
    c->relay_upstream_subscription_owners = (jz_btree *)jz_rc_clone(self->relay_upstream_subscription_owners);
    c->pending_relay_subscription_rejections = (jz_btree *)jz_rc_clone(self->pending_relay_subscription_rejections);
    c->latest_coverage_subscriptions = (jz_btree *)jz_rc_clone(self->latest_coverage_subscriptions);
    c->awaiting_initial_authority_coverage = (jz_btree *)jz_rc_clone(self->awaiting_initial_authority_coverage);
    c->query_coverage_registrations = (jz_btree *)jz_rc_clone(self->query_coverage_registrations);
    c->active_authority_view_receipts = (jazz_db_ActiveAuthorityViewReceipts *)jz_rc_clone(self->active_authority_view_receipts);
    c->coverage_refresh_generations = (jz_btree *)jz_rc_clone(self->coverage_refresh_generations);
    c->scheduler = (jazz_db_SharedTickScheduler *)jz_rc_clone(self->scheduler);
    c->query_runtime_wake_pending = (bool *)jz_rc_clone(self->query_runtime_wake_pending);
    c->query_runtime_waker = (jazz_db_WakerCache *)jz_rc_clone(self->query_runtime_waker);
    c->upload_retry_clock = (jazz_db_UploadRetryClock *)jz_rc_clone(self->upload_retry_clock);
    c->has_upstream_upload_destination = false;
    c->large_value_upload_retry_deadlines = (jz_btree *)jz_rc_clone(self->large_value_upload_retry_deadlines);
    c->write_state_waiters = (jz_btree *)jz_rc_clone(self->write_state_waiters);
    c->open_schema_admission = (jazz_db_OpenSchemaAdmission *)jz_rc_clone(self->open_schema_admission);
    c->permission_advice_waiters = (jz_btree *)jz_rc_clone(self->permission_advice_waiters);
    c->current_rows = self->current_rows; /* Rc<RefCell<CurrentRowsRouter>> shared */
    c->local_fate_routes = (jz_btree *)jz_rc_clone(self->local_fate_routes);
    c->admitted_upstream_authority = (jazz_db_HeldAuthority *)jz_rc_clone(self->admitted_upstream_authority);
    c->mutation_errors = (jazz_db_MutationErrorState *)jz_rc_clone(self->mutation_errors);
    c->browser_relay_recovered_tx_ids = (jz_btree *)jz_rc_clone(self->browser_relay_recovered_tx_ids);
    c->subscriber_dirty_epoch = (u64 *)jz_rc_clone(self->subscriber_dirty_epoch);
    c->observed_subscriber_dirty_epoch = *self->subscriber_dirty_epoch;
    c->local_replay_epoch = *self->subscriber_dirty_epoch;
    c->observed_session_claim_revision = 0;
    c->inbound_authority_receipt_quarantine = false;
    c->connection_epoch = connection_epoch;
    c->has_startup_error = false;
    c->pending_chunk_response = NULL;
    jz_deque_init(&c->pending_control_responses, sizeof(jazz_db_PendingSubscriberControlResponse));
    c->has_last_resume_bytes = false;
}

static jazz_db_SyncMessageVec *new_downstream_fates(void) {
    jazz_db_SyncMessageVec *v = (jazz_db_SyncMessageVec *)jz_rc_new(sizeof(jazz_db_SyncMessageVec), NULL);
    memset(v, 0, sizeof(*v));
    return v;
}

static bool owner_list_has_state(const jazz_db_WeakStateVec *owners, const jazz_db_SubscriptionState *state_rc) {
    for (u32 i = 0; i < owners->len; ++i) {
        jazz_db_SubscriptionState *owner = jazz_db_weak_upgrade(owners->ptr[i]);
        if (!owner) continue;
        bool same = owner == state_rc;
        jz_rc_release(owner);
        if (same) return true;
    }
    return false;
}

static void push_subscribe_command(jazz_db_PendingUpstreamCommands *pending, jazz_db_PendingUpstreamSubscription subscription) {
    jazz_db_PendingUpstreamCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.tag = JAZZ_PUC_SUBSCRIBE;
    cmd.subscribe = subscription;
    jz_vec_push(pending, &cmd, sizeof(cmd));
}

static jz_err connect_upstream_inner(jazz_db_Node *self, jazz_db_Transport transport, jazz_db_PeerMutex **out) {
    jazz_db__db__node_runtime__Transport__set_trusted_encoder(&transport, true);
    jazz_db__db__UploadOutbox__mark_upstream_attached(self->outbox);
    for (;;) {
        /* Connection installation mutates runtime metadata synchronously, but
         * first needs a coherent view of storage-owning node state. Evaluation
         * may temporarily own that state across an async hydration boundary,
         * so wait for it instead of using the synchronous borrow escape hatch. */
        jazz_db_PeerMutexVec peers;
        clone_peer_inventory(self, &peers);
        acquire_peer_inventory(&peers);
        jazz_node_NodeState *node = jazz_db_node_lock(self->node);
        if (!peer_inventory_matches(self, &peers)) {
            jazz_db_node_unlock(self->node);
            release_peer_inventory(&peers, NULL);
            drop_peer_inventory(&peers);
            continue;
        }
        jazz_db_ConnectionSessionContext session_context;
        bool has_session_context = jazz_db__db__node_runtime__Transport__connection_session_context(&transport, &session_context);
        bool local_receiver = !NS_FN(is_history_complete)(node);
        jazz_types_GlobalTime confirmation_floor = NS_FN(committed_global_time)(node);
        jazz_db_node_unlock(self->node);
        void *wire_inbound_context = jazz_db__db__node_runtime__Transport__wire_inbound_context(&transport);
        void *shared_auxiliary_endpoint = jazz_db__db__node_runtime__Transport__shared_auxiliary_endpoint(&transport);
        bool has_destination = has_session_context && session_context.has_remote;
        jazz_db_UpstreamUploadDestination upstream_upload_destination;
        memset(&upstream_upload_destination, 0, sizeof(upstream_upload_destination));
        if (has_destination) {
            memcpy(upstream_upload_destination.remote_node, session_context.remote.node.b, 16);
            upstream_upload_destination.link_identity = session_context.link_identity;
        }
        jz_btree transferred_large_value_uploads;
        jz_btree_init(&transferred_large_value_uploads, &jazz_db_large_value_upload_queues_desc);
        if (has_destination) {
            jz_btree removed;
            if (jz_btree_remove(self->detached_large_value_uploads, &upstream_upload_destination, NULL, &removed))
                transferred_large_value_uploads = removed;
        }
        u64 connection_epoch = has_session_context ? session_context.local.epoch : random_connection_epoch();
        jazz_db__db__begin_open_schema_connection(self->open_schema_admission, connection_epoch);
        /* Durable settled-view state remains available for known-state
         * payload repair, but a new upstream (including a server switch) owns
         * no settlement receipts until it sends a fresh ViewUpdate. */
        jazz_db__db__AuthorityViewReceipts__set(self->active_authority_view_receipts, connection_epoch, confirmation_floor);
        /* A replacement link invalidates the prior link's receipt before the
         * new transport can deliver anything. Publish that demotion now rather
         * than letting cached rows remain settled until the next tick. */
        demote_authority_receipt_subscriptions(self);
        bool has_expected = false;
        jazz_protocol_AuthorityContext expected_scope_authority;
        memset(&expected_scope_authority, 0, sizeof(expected_scope_authority));
        if (has_session_context && (session_context.negotiated_features & JAZZ_FEATURE_AUTHORIZATION_SCOPE_VIEWS) != 0 && session_context.has_remote) {
            has_expected = true;
            memcpy(expected_scope_authority.authority, session_context.remote.node.b, 16);
            expected_scope_authority.link = session_context.link_identity;
            expected_scope_authority.connection_id = connection_epoch;
            expected_scope_authority.connection_epoch = session_context.remote.epoch;
        }
        /* Keep every admitted link eligible, but bind each downstream route
         * to one stable selected owner. A newly connected parallel upstream
         * must not silently replace the owner (or settle its parked writes). */
        if (has_expected) {
            jazz_db_AuthorityContextVec *eligible = self->admitted_upstream_authorities;
            bool contains = false;
            for (u32 i = 0; i < eligible->len; ++i)
                if (jazz_protocol_AuthorityContext_eq(&eligible->ptr[i], &expected_scope_authority)) { contains = true; break; }
            if (!contains) jz_vec_push(eligible, &expected_scope_authority, sizeof(expected_scope_authority));
            if (!self->admitted_upstream_authority->has) {
                self->admitted_upstream_authority->has = true;
                self->admitted_upstream_authority->ctx = expected_scope_authority;
            }
        }
        /* Carry queued and already-registered subscriptions upstream immediately. */
        jazz_db_PendingUpstreamCommands pending = *self->upstream_subscriptions;
        memset(self->upstream_subscriptions, 0, sizeof(pending));
        jz_btree pending_subscriptions;
        jz_btree_init(&pending_subscriptions, &jazz_db_subscription_key_set_desc);
        for (u32 i = 0; i < pending.len; ++i)
            if (pending.ptr[i].tag == JAZZ_PUC_SUBSCRIBE)
                jz_btree_insert(&pending_subscriptions, &pending.ptr[i].subscribe.subscription, NULL, NULL);
        {
            void *k; jazz_db_QueryCoverageRegistration *registration;
            JZ_BTREE_FOR(self->query_coverage_registrations, k, registration) {
                if (!jz_btree_insert(&pending_subscriptions, &registration->subscription.subscription, NULL, NULL)) {
                    jazz_db_PendingUpstreamSubscription copy;
                    jazz_db__db__PendingUpstreamSubscription__clone(&copy, &registration->subscription);
                    push_subscribe_command(&pending, copy);
                }
            }
        }
        for (u32 i = 0; i < self->subscriptions->len; ++i) {
            jazz_db_SubscriptionState *state_rc = jazz_db_weak_upgrade(self->subscriptions->ptr[i]);
            if (!state_rc) continue;
            jazz_db_SubscriptionState *state = state_rc;
            if (!state->propagates_upstream) { jz_rc_release(state_rc); continue; }
            jazz_protocol_RegisterShapeOptions opts;
            NODE_FN(upstream_register_shape_options)(self, (jazz_model_DurabilityTier)state->read_tier, jazz_protocol__protocol__ReadViewSpec__clone(state->read_view), &opts);
            jazz_protocol_RegisterShapeOptions opts_for_key;
            jazz_protocol__protocol__RegisterShapeOptions__clone(&opts_for_key, &opts);
            jazz_db_CoverageKey coverage;
            jazz_db__db__request_coverage_key(&coverage, state->shape, state->binding, opts_for_key, state->has_request_identity_claims ? &state->request_identity_claims : NULL);
            bool found = false;
            jazz_protocol_SubscriptionKey subscription;
            {
                jazz_protocol_SubscriptionKey *sk; jazz_db_WeakStateVec *owners;
                JZ_BTREE_FOR(self->upstream_subscription_owners, sk, owners) {
                    if (owner_list_has_state(owners, state_rc)) { subscription = *sk; found = true; break; }
                }
            }
            if (!found) subscription = NODE_FN(next_subscription_key)(self, state->shape, jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts));
            jz_btree_insert(self->latest_coverage_subscriptions, &coverage, &subscription, NULL);
            if (!jz_btree_insert(&pending_subscriptions, &subscription, NULL, NULL)) {
                jazz_db_PendingUpstreamSubscription sub;
                memset(&sub, 0, sizeof(sub));
                sub.subscription = subscription;
                sub.shape = jazz_model__query__ValidatedQuery__clone(state->shape);
                sub.binding = jazz_model__query__Binding__clone(state->binding);
                sub.opts = opts;
                sub.identity = state->author;
                sub.has_policy_binding = state->has_request_identity_claims;
                if (state->has_request_identity_claims) jazz_db__db__PolicyBinding__clone(&sub.policy_binding, &state->request_identity_claims);
                push_subscribe_command(&pending, sub);
            } else {
                jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
            }
            jz_rc_release(state_rc);
        }
        /* Relay-owned coverage is not represented by the public subscription
         * list above: it is retained by the downstream connection that it
         * serves. Replacing an upstream transport drops that transport's wire
         * subscriptions, while the downstream browser is still connected and
         * therefore will not send a fresh Subscribe. Replay every live relay
         * owner onto the successor authority, using its stable usage-site key.
         *
         * The owner map is the lifecycle authority here. A rejected coverage
         * group can briefly remain on its downstream link while its rejection
         * waits to be delivered; replaying that orphan would resurrect a
         * subscription that is already being retired. */
        JZ_VEC(jazz_db_PendingUpstreamSubscription) relay_subscriptions = {0};
        for (u32 i = 0; i < self->connections.len; ++i) {
            jazz_db_PeerConnection *connection = self->connections.ptr[i]->conn; /* guarded by the inventory */
            if (!self->connections.ptr[i]->locked) jz_panic("stable upstream peer inventory");
            if (connection->link.tag != JAZZ_LINK_SUBSCRIBER) continue;
            jazz_db_SubscriberConnectionState *subscriber = &connection->link.v.subscriber;
            jazz_db_CoverageKey *coverage; jazz_db_CoverageGroup *group;
            JZ_BTREE_FOR(&subscriber->coverage_groups, coverage, group) {
                jazz_db_RelayOwnerKey key;
                memset(&key, 0, sizeof(key));
                key.subscription = group->upstream_subscription;
                key.downstream_connection_epoch = connection->connection_epoch;
                key.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&coverage->opts);
                jazz_db_RelayUpstreamSubscriptionOwner *owner = (jazz_db_RelayUpstreamSubscriptionOwner *)jz_btree_get(self->relay_upstream_subscription_owners, &key);
                if (!owner) continue;
                if (!(owner->downstream_connection_epoch == connection->connection_epoch && jazz_db__db__CoverageKey__eq(&owner->coverage, coverage))) continue;
                jazz_db_PendingUpstreamSubscription sub;
                memset(&sub, 0, sizeof(sub));
                sub.subscription = group->upstream_subscription;
                sub.shape = jazz_model__query__ValidatedQuery__clone(group->shape);
                sub.binding = jazz_model__query__Binding__clone(group->binding);
                jazz_protocol__protocol__RegisterShapeOptions__clone(&sub.opts, &group->upstream_opts);
                /* This usage is forwarded under the subscription's
                 * topology-admitted policy binding, never the relay
                 * transport. A relay has no link permission subject to
                 * fall back to. */
                sub.identity = group->policy_binding.identity;
                sub.has_policy_binding = true;
                jazz_db__db__PolicyBinding__clone(&sub.policy_binding, &group->policy_binding);
                jz_vec_push(&relay_subscriptions, &sub, sizeof(sub));
            }
        }
        for (u32 i = 0; i < relay_subscriptions.len; ++i) {
            if (!jz_btree_insert(&pending_subscriptions, &relay_subscriptions.ptr[i].subscription, NULL, NULL))
                push_subscribe_command(&pending, relay_subscriptions.ptr[i]);
            else
                jazz_db__db__PendingUpstreamSubscription__drop(&relay_subscriptions.ptr[i]);
        }
        jz_vec_drop(&relay_subscriptions, sizeof(jazz_db_PendingUpstreamSubscription), NULL);
        jz_btree_drop(&pending_subscriptions);

        jazz_db_PeerConnection *c = JZ_NEW(jazz_db_PeerConnection);
        init_shared_connection(self, c, transport, connection_epoch);
        c->has_upstream_upload_destination = has_destination;
        c->upstream_upload_destination = upstream_upload_destination;
        c->downstream_fates = new_downstream_fates();
        c->link.tag = JAZZ_LINK_UPSTREAM;
        jazz_db_UpstreamConnectionState *up = &c->link.v.upstream;
        up->local_receiver = local_receiver;
        up->pending = pending;
        up->upstream_subscriptions = (jazz_db_PendingUpstreamCommands *)jz_rc_clone(self->upstream_subscriptions);
        jz_btree_init(&up->announced_shapes, &jazz_db_shape_registration_key_set_desc);
        jz_btree_init(&up->sent_subscriptions, &jazz_db_sent_subscriptions_desc);
        jz_btree_init(&up->awaiting_support_snapshots, &jazz_db_subscription_global_time_desc);
        jz_btree_init(&up->sent_session_claim_revisions, &jazz_db_author_u64_desc);
        up->outbox = (jazz_db_UploadOutbox *)jz_rc_clone(self->outbox);
        jz_btree_init(&up->uploaded, &jazz_db_txid_set_desc);
        up->large_value_uploads = transferred_large_value_uploads;
        jz_btree_init(&up->awaiting_large_value_uploads, &jazz_db_txid_large_value_ref_desc);
        jz_btree_init(&up->failed_large_value_uploads, &jazz_db_txid_set_desc);
        jz_btree_init(&up->missing_body_resends, &jazz_db_subscription_key_set_desc);
        up->has_expected_scope_authority = has_expected;
        up->expected_scope_authority = expected_scope_authority;
        jazz_db__db__AuthorizationScopeLeaseManager__default(&up->scope_lease_manager);
        memset(&up->read_barrier, 0, sizeof(up->read_barrier));
        c->auxiliary_pump = jazz_db__db__PeerIoPump__new(jazz_db__db__PeerChunkResolver__clone(self->chunk_resolver),
            jazz_node__node__NodeState__local_chunk_reader_clone(self->local_chunk_reader), connection_epoch, JAZZ_PUMP_UPSTREAM, wire_inbound_context);
        jazz_db__db__PeerIoPump__with_shared_auxiliary_endpoint(&c->auxiliary_pump, shared_auxiliary_endpoint);
        jazz_db_PeerMutex *connection = new_peer_mutex(c);

        release_peer_inventory(&peers, NULL);
        drop_peer_inventory(&peers);
        jazz_db_PeerMutex *pushed = (jazz_db_PeerMutex *)jz_rc_clone(connection);
        jz_vec_push(&self->connections, &pushed, sizeof(pushed));
        jazz_db__first_load__RemoteLinkTracker__upstream_attached(self->remote_link);
        NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
        *out = connection;
        return JZ_OK;
    }
}

/// Attach an upstream after acquiring a stable runtime and peer inventory.
jz_err NODE_FN(connect_upstream)(jazz_db_Node *self, jazz_db_Transport transport, jazz_db_PeerMutex **out) {
    jz_err e = connect_upstream_inner(self, transport, out);
    if (e.code) jz_panic("upstream attachment is infallible");
    return JZ_OK;
}

/// Fallible binding-facing form of upstream attachment.
jz_err NODE_FN(try_connect_upstream)(jazz_db_Node *self, jazz_db_Transport transport, jazz_db_PeerMutex **out) {
    return connect_upstream_inner(self, transport, out);
}

/* subscriber_startup -> (PendingDownstreamFates, Option<Error>) */
static jazz_db_SyncMessageVec *subscriber_startup(jazz_db_Node *self, jazz_types_AuthorSubject identity, jazz_node_CommitUnitTrust trust, jz_err *startup_error) {
    jazz_db_SyncMessageVec *downstream_fates = new_downstream_fates();
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    void *scope = NS_FN(client_relay_scope)(node);
    bool mismatch = scope && trust == JAZZ_TRUST_SESSION && !jazz_node__node__ClientRelayScope__admits_session(scope, identity);
    jazz_db_node_unlock(self->node);
    jz_err scope_mismatch = mismatch
        ? jz_err_new(JAZZ_EC_PROTOCOL, "foreground session is outside this scope-isolated relay ownership scope")
        : JZ_OK;
    if (!scope_mismatch.code && NODE_FN(receives_commits_as_local)(self))
        *startup_error = restore_local_subscriber(self, identity, downstream_fates);
    else
        *startup_error = scope_mismatch;
    return downstream_fates;
}

static jazz_db_PeerMutex *accept_subscriber_with_peer_and_startup(
    jazz_db_Node *self, jazz_db_Transport transport, jazz_types_AuthorSubject identity, jazz_node_CommitUnitTrust trust,
    jz_btree claims, jazz_db_ResumeCursor *cursor /* NULL = None */, jazz_node_PeerState *peer,
    jazz_db_SyncMessageVec *downstream_fates, jz_err startup_error) {
    bool local_receiver = NODE_FN(receives_commits_as_local)(self);
    jazz_node_CommitUnitIngestContext ingest_context;
    jz_btree session_claims;
    u64 session_claim_revision;
    if (cursor) {
        if (!jazz_types_AuthorSubject_eq(cursor->ingest_context.identity, identity))
            jz_panic("assertion `left == right` failed: a resume cursor may only be used by its authenticated identity");
        /* Catalogue receipt is per physical connection. Retain the
         * cursor's subscription state, but force the new connection's
         * initial metadata even when the authority is unchanged. */
        jazz_node__peer__PeerState__reset_catalogue_snapshot_announcement(cursor->peer);
        jazz_node__peer__PeerState__drop(peer);
        jz_btree_drop(&claims);
        peer = cursor->peer;
        ingest_context = cursor->ingest_context;
        session_claims = cursor->session_claims;
        session_claim_revision = cursor->session_claim_revision;
    } else {
        memset(&ingest_context, 0, sizeof(ingest_context));
        ingest_context.identity = identity;
        ingest_context.trust = (u8)trust;
        ingest_context.admitted_write_authorization = false;
        ingest_context.version_receipts_validated = false;
        session_claims = claims;
        session_claim_revision = 0;
    }
    jazz_db_ConnectionSessionContext ctx;
    u64 connection_epoch = jazz_db__db__node_runtime__Transport__connection_session_context(&transport, &ctx) ? ctx.local.epoch : random_connection_epoch();
    jazz_db__db__node_runtime__Transport__set_trusted_encoder(&transport, jazz_node__node__CommitUnitTrust__is_trusted((jazz_node_CommitUnitTrust)ingest_context.trust));
    void *wire_inbound_context = jazz_db__db__node_runtime__Transport__wire_inbound_context(&transport);
    void *shared_auxiliary_endpoint = jazz_db__db__node_runtime__Transport__shared_auxiliary_endpoint(&transport);
    jazz_db_PeerConnection *c = JZ_NEW(jazz_db_PeerConnection);
    init_shared_connection(self, c, transport, connection_epoch);
    c->downstream_fates = downstream_fates;
    c->observed_session_claim_revision = session_claim_revision;
    c->has_startup_error = startup_error.code != 0;
    c->startup_error = startup_error;
    c->link.tag = JAZZ_LINK_SUBSCRIBER;
    jazz_db_SubscriberConnectionState *sub = &c->link.v.subscriber;
    sub->peer = peer;
    sub->ingest_context = ingest_context;
    sub->session_claims = session_claims;
    sub->session_claim_revision = session_claim_revision;
    sub->local_receiver = local_receiver;
    sub->outbox = (jazz_db_UploadOutbox *)jz_rc_clone(self->outbox);
    sub->upstream_subscriptions = (jazz_db_PendingUpstreamCommands *)jz_rc_clone(self->upstream_subscriptions);
    jz_btree_init(&sub->served, &jazz_db_served_desc);
    jz_btree_init(&sub->coverage_groups, &jazz_db_coverage_groups_desc);
    jz_btree_init(&sub->shape_registrations, &jazz_db_shape_registrations_desc);
    jz_deque_init(&sub->deferred_subscribe_rejections, sizeof(jazz_db_PendingSubscriberControlResponse));
    jz_btree_init(&sub->pending_catalogue_subscriptions, &jazz_db_pending_catalogue_subscriptions_desc);
    jz_btree_init(&sub->authority_scope_hydrations, &jazz_db_scope_hydrations_desc);
    sub->authority_scope_hydration_count = 0;
    sub->serve_dirty = true;
    jazz_db__read_barrier__ReadBarrierReceiver__default(&sub->read_barrier);
    c->auxiliary_pump = jazz_db__db__PeerIoPump__new(jazz_db__db__PeerChunkResolver__clone(self->chunk_resolver),
        jazz_node__node__NodeState__local_chunk_reader_clone(self->local_chunk_reader), connection_epoch, JAZZ_PUMP_SUBSCRIBER, wire_inbound_context);
    jazz_db__db__PeerIoPump__with_shared_auxiliary_endpoint(&c->auxiliary_pump, shared_auxiliary_endpoint);
    jazz_db_PeerMutex *connection = new_peer_mutex(c);
    jazz_db_PeerMutex *pushed = (jazz_db_PeerMutex *)jz_rc_clone(connection);
    jz_vec_push(&self->connections, &pushed, sizeof(pushed));
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    return connection;
}

/// Admit a normal session subscriber without waiting on its semantic owner.
jz_err NODE_FN(accept_subscriber_with_claims_async)(jazz_db_Node *self, jazz_db_Transport transport, jazz_types_AuthorSubject identity, jz_btree claims, jazz_node_CommitUnitTrust trust, jazz_db_PeerMutex **out) {
    (void)trust; /* the async form always admits CommitUnitTrust::Session */
    jz_err startup_error = JZ_OK;
    jazz_db_SyncMessageVec *downstream_fates = subscriber_startup(self, identity, JAZZ_TRUST_SESSION, &startup_error);
    if (startup_error.code) {
        jz_rc_release(downstream_fates);
        jz_btree_drop(&claims);
        if (transport.vt->drop) transport.vt->drop(transport.self);
        return startup_error;
    }
    *out = accept_subscriber_with_peer_and_startup(self, transport, identity, JAZZ_TRUST_SESSION, claims, NULL,
        jazz_node__peer__PeerState__client_link(identity), downstream_fates, JZ_OK);
    return JZ_OK;
}

/* ---------------- detach ---------------- */

/* with_detach_peer: run on a sibling either through the stable inventory
 * guards or, without them, through a synchronous borrow. */
static jazz_db_PeerConnection *detach_peer_enter(jazz_db_PeerMutex *peer, bool has_guards) {
    if (has_guards) {
        if (!peer->locked) jz_panic("stable detach peer inventory");
        return peer->conn;
    }
    jazz_db_PeerConnection *c = jazz_db_peer_try_lock(peer);
    if (!c) jz_panic("connection is already borrowed");
    return c;
}
static void detach_peer_leave(jazz_db_PeerMutex *peer, bool has_guards) { if (!has_guards) jazz_db_peer_unlock(peer); }

typedef struct ReconnectAdvice {
    jazz_protocol_PermissionAdviceRequestId request_id;
    jazz_protocol_PermissionAdviceAction *action;
    bool has_session_claim_binding;
    jazz_db_PolicyBinding session_claim_binding;
    bool has_delegated_session;
    jazz_protocol_DelegatedSessionBinding delegated_session;
} ReconnectAdvice;

static void current_claims_for(const jazz_node_SessionClaimsRevVec *current, jazz_types_AuthorSubject identity, jz_btree *out) {
    for (u32 i = 0; i < current->len; ++i) {
        if (jazz_types_AuthorSubject_eq(current->ptr[i].identity, identity)) {
            jz_btree_clone(out, &current->ptr[i].claims, str_key_clone, groove__records__Value__clone);
            return;
        }
    }
    jz_btree_init(out, &jazz_db_str_value_desc);
}
static bool current_claims_equal(const jazz_node_SessionClaimsRevVec *current, const jazz_db_PolicyBinding *binding) {
    jz_btree c;
    current_claims_for(current, binding->identity, &c);
    bool eq = jz_btree_eq(&c, &binding->claims, groove__records__Value__eq);
    jz_btree_drop(&c);
    return eq;
}

static bool detach_connection_with_guards(jazz_db_Node *self, jazz_db_PeerMutex *connection, jazz_db_PeerConnection *connection_ref,
                                          jazz_node_NodeState *node, bool has_sibling_guards) {
    bool registered = false;
    for (u32 i = 0; i < self->connections.len; ++i) if (self->connections.ptr[i] == connection) { registered = true; break; }
    if (!registered) {
        jazz_db_node_unlock(self->node);
        jazz_db_peer_unlock(connection);
        return false;
    }
    u64 connection_epoch = connection_ref->connection_epoch;
    if (connection_ref->link.tag == JAZZ_LINK_UPSTREAM) {
        jazz_db__db__finish_open_schema_connection(self->open_schema_admission, connection_epoch,
            jz_err_new(JAZZ_EC_PROTOCOL, "upstream disconnected before schema admission"));
    }
    jz_deque_drop(&connection_ref->staged_inbound, jazz_db__db__StagedInboundMessage__drop);
    jz_deque_init(&connection_ref->staged_inbound, sizeof(jazz_db_StagedInboundMessage));
    if (connection_ref->link.tag == JAZZ_LINK_UPSTREAM) jz_btree_clear(&connection_ref->link.v.upstream.missing_body_resends);
    jazz_db__row_availability__CurrentRowsRouter__disconnect(self->current_rows, connection_epoch);
    bool has_destination = connection_ref->has_upstream_upload_destination;
    jazz_db_UpstreamUploadDestination upstream_upload_destination = connection_ref->upstream_upload_destination;
    JZ_VEC(ReconnectAdvice) reconnect_permission_advice = {0};
    JZ_VEC(jazz_protocol_PermissionAdviceRequestId) terminal_permission_advice = {0};
    jazz_node_SessionClaimsRevVec current_session_claims = {0};
    NS_FN(session_claims_with_revisions)(node, &current_session_claims);

    bool has_authority = false;
    jazz_protocol_AuthorityContext authority;
    memset(&authority, 0, sizeof(authority));
    bool has_upstream_epoch = false;
    u64 upstream_epoch = 0;
    bool has_transferable = false;
    jz_btree transferable_uploads;
    jazz_db_KeyedRelayOwnerVec retired_relay_subscriptions = {0};

    if (connection_ref->link.tag == JAZZ_LINK_UPSTREAM) {
        jazz_db_UpstreamConnectionState *up = &connection_ref->link.v.upstream;
        /* Permission-advice futures outlive one transport. Rebuild
         * their link-local scope bookkeeping on the successor,
         * one command per live waiter so identical actions can
         * coalesce under its fresh authority context and wire id. */
        jz_btree *live_waiters = self->permission_advice_waiters;
        jz_btree queued;
        jz_btree_init(&queued, &jazz_db_permission_request_id_set_desc);
        /* The lease manager becomes authoritative as soon as it
         * captures a session binding, even if the matching
         * command is still retained after a backpressured send.
         * Visit it first so a command's initial `None` cannot
         * erase the request-owned snapshot during detach. */
        void *rk; jazz_db_AuthorizationScopeLeaseRequest *request;
        JZ_BTREE_FOR(&up->scope_lease_manager.requests, rk, request) {
            jazz_protocol_PermissionAdviceRequestId *request_id; void *unused;
            JZ_BTREE_FOR(&request->waiters, request_id, unused) {
                if (!jz_btree_contains(live_waiters, request_id)) continue;
                if (jz_btree_insert(&queued, request_id, NULL, NULL)) continue;
                if (request->has_delegated_session || current_claims_equal(&current_session_claims, &request->session_claim_binding)) {
                    ReconnectAdvice r;
                    memset(&r, 0, sizeof(r));
                    r.request_id = *request_id;
                    r.action = jazz_protocol__protocol__PermissionAdviceAction__clone(request->action);
                    r.has_session_claim_binding = true;
                    jazz_db__db__PolicyBinding__clone(&r.session_claim_binding, &request->session_claim_binding);
                    r.has_delegated_session = request->has_delegated_session;
                    if (r.has_delegated_session) jazz_protocol__protocol__DelegatedSessionBinding__clone(&r.delegated_session, &request->delegated_session);
                    jz_vec_push(&reconnect_permission_advice, &r, sizeof(r));
                } else {
                    /* A successor can only prove its currently
                     * admitted session. Never send a B-scoped
                     * hydration to settle this A-owned request. */
                    jz_vec_push(&terminal_permission_advice, request_id, sizeof(*request_id));
                }
            }
        }
        for (u32 i = 0; i < up->pending.len; ++i) {
            jazz_db_PendingUpstreamCommand *command = &up->pending.ptr[i];
            if (command->tag != JAZZ_PUC_AUTHORIZATION_SCOPE_INTENT) continue;
            jazz_protocol_PermissionAdviceRequestId *request_id = &command->intent.request_id;
            if (!jz_btree_contains(live_waiters, request_id)) continue;
            if (jz_btree_insert(&queued, request_id, NULL, NULL)) continue;
            if (command->intent.has_delegated_session || !command->intent.has_session_claim_binding
                || current_claims_equal(&current_session_claims, &command->intent.session_claim_binding)) {
                ReconnectAdvice r;
                memset(&r, 0, sizeof(r));
                r.request_id = *request_id;
                r.action = jazz_protocol__protocol__PermissionAdviceAction__clone(command->intent.action);
                r.has_session_claim_binding = command->intent.has_session_claim_binding;
                if (r.has_session_claim_binding) jazz_db__db__PolicyBinding__clone(&r.session_claim_binding, &command->intent.session_claim_binding);
                r.has_delegated_session = command->intent.has_delegated_session;
                if (r.has_delegated_session) jazz_protocol__protocol__DelegatedSessionBinding__clone(&r.delegated_session, &command->intent.delegated_session);
                jz_vec_push(&reconnect_permission_advice, &r, sizeof(r));
            } else {
                jz_vec_push(&terminal_permission_advice, request_id, sizeof(*request_id));
            }
        }
        jz_btree_drop(&queued);
        has_authority = up->has_expected_scope_authority;
        authority = up->expected_scope_authority;
        has_upstream_epoch = true;
        upstream_epoch = connection_epoch;
        has_transferable = true;
        jazz_db__db__peer_connection__take_reconnectable_large_value_uploads(&up->large_value_uploads, &up->awaiting_large_value_uploads, &transferable_uploads);
    } else {
        jazz_db_SubscriberConnectionState *sub = &connection_ref->link.v.subscriber;
        jazz_db__db__node_runtime__retire_relay_upstream_subscriptions_for_connection(self->relay_upstream_subscription_owners, connection_epoch, &retired_relay_subscriptions);
        /* A detached subscriber cannot later send a normal
         * Unsubscribe. Retire its concrete served usage sites and the
         * one maintained receiver per coverage group now; groups on
         * every other downstream connection remain untouched. */
        jz_btree groups = sub->coverage_groups;
        jz_btree_init(&sub->coverage_groups, groups.d);
        jazz_db_CoverageKey *coverage; jazz_db_CoverageGroup *group;
        JZ_BTREE_FOR(&groups, coverage, group) {
            jazz_protocol_SubscriptionKey *subscription; void *unused;
            JZ_BTREE_FOR(&group->subscribers, subscription, unused) {
                NS_FN(apply_unsubscribe)(node, *subscription);
                jz_btree_remove(&sub->served, subscription, NULL, NULL);
            }
            jazz_node__peer__PeerState__forget_subscription_with_node(sub->peer, node, jazz_db__db__peer_connection__coverage_group_subscription_key(coverage));
        }
        jz_btree_drop(&groups);
        NS_FN(release_shapes_for_peer)(node, connection_epoch);
        jz_btree_clear(&sub->shape_registrations);
        jz_btree_clear(&sub->authority_scope_hydrations);
    }
    jazz_node__node__SessionClaimsRevVec__drop(&current_session_claims);
    jazz_db_node_unlock(self->node);
    /* The auxiliary lane is independent of semantic ticks. Retire it for
     * both upstream and subscriber links before releasing this connection
     * so an in-flight local lookup cannot recreate relay state afterward. */
    jazz_db__db__PeerIoPump__disconnect(&connection_ref->auxiliary_pump);
    jazz_db_peer_unlock(connection);
    /* A rejection can be queued by the upstream turn immediately before
     * this abrupt detach. Its downstream transport is gone, so retaining
     * that queue entry would be an unbounded stale-epoch leak. */
    jz_btree_remove(self->pending_relay_subscription_rejections, &connection_epoch, NULL, NULL);
    for (u32 i = 0; i < self->connections.len;) {
        if (self->connections.ptr[i] == connection) {
            jazz_db_PeerMutex *removed;
            jz_vec_remove(&self->connections, i, &removed, sizeof(removed));
            jz_rc_release(removed);
        } else ++i;
    }
    bool detached = true;
    if (has_upstream_epoch) {
        /* Releases openings that were waiting on this link. */
        jazz_db__first_load__RemoteLinkTracker__upstream_detached(self->remote_link);
        /* Commits that link sent are resent by the next one; until then a
         * Global read must not treat them as ahead of its open. */
        jazz_db__db__UploadOutbox__forget_on_wire(self->outbox);
    }
    for (u32 i = 0; i < terminal_permission_advice.len; ++i) {
        jazz_db_Oneshot *waiter = NULL;
        if (jz_btree_remove(self->permission_advice_waiters, &terminal_permission_advice.ptr[i], NULL, &waiter) && waiter) {
            u8 advice = JAZZ_ADVICE_UNKNOWN;
            (void)jazz_db__oneshot__send(waiter, &advice);
            jazz_db__oneshot__drop_sender(waiter);
        }
    }
    jz_vec_drop(&terminal_permission_advice, sizeof(jazz_protocol_PermissionAdviceRequestId), NULL);
    if (retired_relay_subscriptions.len != 0) {
        for (u32 i = 0; i < retired_relay_subscriptions.len; ++i) {
            jazz_db_PendingUpstreamCommand cmd;
            memset(&cmd, 0, sizeof(cmd));
            cmd.tag = JAZZ_PUC_UNSUBSCRIBE;
            cmd.unsubscribe = retired_relay_subscriptions.ptr[i].subscription;
            jz_vec_push(self->upstream_subscriptions, &cmd, sizeof(cmd));
            jazz_db__db__RelayUpstreamSubscriptionOwner__drop(&retired_relay_subscriptions.ptr[i].owner);
        }
        NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    }
    jz_vec_drop(&retired_relay_subscriptions, sizeof(jazz_db_KeyedRelayOwner), NULL);
    if (reconnect_permission_advice.len != 0) {
        for (u32 i = 0; i < reconnect_permission_advice.len; ++i) {
            ReconnectAdvice *r = &reconnect_permission_advice.ptr[i];
            jazz_db_PendingUpstreamCommand cmd;
            memset(&cmd, 0, sizeof(cmd));
            cmd.tag = JAZZ_PUC_AUTHORIZATION_SCOPE_INTENT;
            cmd.intent.request_id = r->request_id;
            cmd.intent.action = r->action;
            cmd.intent.has_session_claim_binding = r->has_session_claim_binding;
            cmd.intent.session_claim_binding = r->session_claim_binding;
            cmd.intent.has_delegated_session = r->has_delegated_session;
            cmd.intent.delegated_session = r->delegated_session;
            jz_vec_push(self->upstream_subscriptions, &cmd, sizeof(cmd));
        }
        NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    }
    jz_vec_drop(&reconnect_permission_advice, sizeof(ReconnectAdvice), NULL);
    if (has_transferable) {
        if (has_destination && transferable_uploads.len != 0) {
            bool inserted;
            jz_btree *destination_uploads = (jz_btree *)jz_btree_entry(self->detached_large_value_uploads, &upstream_upload_destination, &inserted);
            if (inserted) jz_btree_init(destination_uploads, &jazz_db_large_value_upload_queues_desc);
            jazz_db__db__peer_connection__merge_reconnectable_large_value_uploads(destination_uploads, transferable_uploads);
        } else {
            jz_btree_drop(&transferable_uploads);
        }
    }
    if (detached && has_upstream_epoch && self->active_authority_view_receipts->has
        && self->active_authority_view_receipts->connection_epoch == upstream_epoch) {
        /* A parallel upstream that survived the selected link is a new
         * active authority epoch for settlement purposes. Its older
         * receipt was retired by the switch, so it must confirm the view
         * again before cached rows can become settled.
         * Retire B before staging A's queued frames: otherwise applying a
         * row-changing A update during the handoff could briefly publish
         * it under B's now-dead receipt. */
        jz_btree retired_subscriptions;
        jz_btree_clone(&retired_subscriptions, &self->active_authority_view_receipts->subscriptions, NULL, NULL);
        {
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            jazz_protocol_SubscriptionKey *subscription; void *unused;
            JZ_BTREE_FOR(&retired_subscriptions, subscription, unused) {
                jazz_protocol_AuthorityResultKey key;
                jz_err e = NS_FN(authority_result_key_for_subscription)(n, *subscription, &key);
                if (!e.code) {
                    NS_FN(invalidate_authority_result_settlement)(n, &key);
                    jazz_protocol__protocol__AuthorityResultKey__drop(&key);
                } else jz_err_drop(&e);
            }
            jazz_db_node_unlock(self->node);
        }
        jz_btree_drop(&retired_subscriptions);
        jazz_db__db__AuthorityViewReceipts__clear(self->active_authority_view_receipts);
        jazz_db_PeerMutex *fallback_connection = NULL;
        for (u32 i = self->connections.len; i-- > 0;) {
            jazz_db_PeerMutex *candidate = self->connections.ptr[i];
            jazz_db_PeerConnection *peer = detach_peer_enter(candidate, has_sibling_guards);
            bool upstream = peer->link.tag == JAZZ_LINK_UPSTREAM;
            detach_peer_leave(candidate, has_sibling_guards);
            if (upstream) { fallback_connection = candidate; break; }
        }
        if (fallback_connection) {
            jazz_db_PeerConnection *peer = detach_peer_enter(fallback_connection, has_sibling_guards);
            PC_FN(stage_inbound_without_authority_receipt)(peer);
            u64 epoch = peer->connection_epoch;
            detach_peer_leave(fallback_connection, has_sibling_guards);
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            jazz_types_GlobalTime floor = NS_FN(committed_global_time)(n);
            jazz_db_node_unlock(self->node);
            jazz_db__db__AuthorityViewReceipts__set(self->active_authority_view_receipts, epoch, floor);
        }
        /* Cached rows remain readable as stale/local state, but their
         * settled receipt died with this authority connection. */
        demote_authority_receipt_subscriptions(self);
    }
    if (detached && has_authority) {
        jazz_db_AuthorityContextVec *eligible = self->admitted_upstream_authorities;
        for (u32 i = 0; i < eligible->len;) {
            if (jazz_protocol_AuthorityContext_eq(&eligible->ptr[i], &authority)) jz_vec_remove(eligible, i, NULL, sizeof(authority));
            else ++i;
        }
        if (self->admitted_upstream_authority->has && jazz_protocol_AuthorityContext_eq(&self->admitted_upstream_authority->ctx, &authority)) {
            self->admitted_upstream_authority->has = eligible->len != 0;
            if (eligible->len) self->admitted_upstream_authority->ctx = eligible->ptr[0];
            NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
        }
    }
    return detached;
}

/// Detach with every owner taken by `try_lock`, or `None` when a suspended
/// operation still owns the node or a registered peer. Returns false for None.
static bool try_detach_connection_now(jazz_db_Node *self, jazz_db_PeerMutex *connection, bool *out) {
    jazz_db_PeerMutexVec peers;
    clone_peer_inventory(self, &peers);
    bool any = false;
    for (u32 i = 0; i < peers.len; ++i) if (peers.ptr[i] == connection) { any = true; break; }
    if (!any) { drop_peer_inventory(&peers); *out = false; return true; }
    u32 locked = 0;
    for (; locked < peers.len; ++locked) if (!jazz_db_peer_try_lock(peers.ptr[locked])) break;
    if (locked < peers.len) {
        for (u32 i = 0; i < locked; ++i) jazz_db_peer_unlock(peers.ptr[i]);
        drop_peer_inventory(&peers);
        return false;
    }
    jazz_node_NodeState *node = jazz_db_node_try_lock(self->node);
    if (!node) {
        release_peer_inventory(&peers, NULL);
        drop_peer_inventory(&peers);
        return false;
    }
    /* guards.remove(target).expect("registered detach target") */
    jazz_db_PeerConnection *target = connection->conn;
    *out = detach_connection_with_guards(self, connection, target, node, true);
    release_peer_inventory(&peers, connection);
    drop_peer_inventory(&peers);
    return true;
}

/// Detach a previously attached peer connection from this node.
///
/// A host can close a transport while a suspended operation (a tick or a
/// read awaiting storage) still owns that peer, a sibling peer or the node.
/// Such a detach is completed by the next tick instead of re-entering the
/// suspended owner; the connection is reported as detached either way.
bool NODE_FN(detach_connection)(jazz_db_Node *self, jazz_db_PeerMutex *connection) {
    for (u32 i = 0; i < self->pending_detaches.len; ++i)
        if (self->pending_detaches.ptr[i] == connection) return false;
    bool detached;
    if (try_detach_connection_now(self, connection, &detached)) return detached;
    jazz_db_PeerMutex *pushed = (jazz_db_PeerMutex *)jz_rc_clone(connection);
    jz_vec_push(&self->pending_detaches, &pushed, sizeof(pushed));
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    return true;
}

/// Complete queued synchronous detaches without awaiting an owner.
///
/// A poll-once host (NAPI) drops a tick that returns pending, so awaiting
/// the owner here would lose the queue. A detach whose owner is still held
/// stays queued and retries on a later tick.
static void drain_pending_detaches(jazz_db_Node *self) {
    if (self->pending_detaches.len == 0) return;
    jazz_db_PeerMutexVec pending = {0};
    for (u32 i = 0; i < self->pending_detaches.len; ++i) {
        jazz_db_PeerMutex *p = (jazz_db_PeerMutex *)jz_rc_clone(self->pending_detaches.ptr[i]);
        jz_vec_push(&pending, &p, sizeof(p));
    }
    jazz_db_PeerMutexVec finished = {0};
    for (u32 i = 0; i < pending.len; ++i) {
        bool detached;
        if (try_detach_connection_now(self, pending.ptr[i], &detached)) jz_vec_push(&finished, &pending.ptr[i], sizeof(jazz_db_PeerMutex *));
    }
    for (u32 i = 0; i < self->pending_detaches.len;) {
        bool done = false;
        for (u32 j = 0; j < finished.len; ++j) if (finished.ptr[j] == self->pending_detaches.ptr[i]) { done = true; break; }
        if (done) {
            jazz_db_PeerMutex *removed;
            jz_vec_remove(&self->pending_detaches, i, &removed, sizeof(removed));
            jz_rc_release(removed);
        } else ++i;
    }
    jz_vec_drop(&finished, sizeof(jazz_db_PeerMutex *), NULL);
    drop_peer_inventory(&pending);
    if (self->pending_detaches.len != 0) NODE_FN(schedule_tick)(self, JAZZ_TICK_DEFERRED);
}

/// Await a stable peer inventory before detach mutation.
jz_err NODE_FN(detach_connection_async)(jazz_db_Node *self, jazz_db_PeerMutex *connection, bool *out) {
    for (;;) {
        jazz_db_PeerMutexVec peers;
        clone_peer_inventory(self, &peers);
        bool any = false;
        for (u32 i = 0; i < peers.len; ++i) if (peers.ptr[i] == connection) { any = true; break; }
        if (!any) { drop_peer_inventory(&peers); *out = false; return JZ_OK; }
        /* Every concurrent detach uses registry order, never target-first.
         * No node guard is held while waiting for a peer owner. */
        acquire_peer_inventory(&peers);
        jazz_node_NodeState *node = jazz_db_node_lock(self->node);
        if (!peer_inventory_matches(self, &peers)) {
            jazz_db_node_unlock(self->node);
            release_peer_inventory(&peers, NULL);
            drop_peer_inventory(&peers);
            continue;
        }
        *out = detach_connection_with_guards(self, connection, connection->conn, node, true);
        release_peer_inventory(&peers, connection);
        drop_peer_inventory(&peers);
        return JZ_OK;
    }
}

/* ---------------- tick ---------------- */

static jz_err advance_deletion_reconciliations(jazz_db_Node *self);
static jz_err reconcile_scalar_query_inputs(jazz_db_Node *self);
static void release_outbox_uploads(jazz_db_Node *self, jz_hmap released_tx_ids);
static void resolve_authority_witnesses(jazz_db_Node *self);

/* Box::pin(connection.tick()).await with subscriber terminal-failure retirement.
 * Returns 0 = ticked, 1 = retired, 2 = error (in *err). */
static int tick_connection(jazz_db_PeerMutex *handle, jazz_db_PeerMutexVec *retired, jz_hmap *released_outbox_tx_ids, jazz_db_DbTickStats *next, jz_err *err) {
    jazz_db_PeerConnection *connection = jazz_db_peer_lock(handle);
    /* `PeerConnection::tick` contains the subscriber admission state
     * machine. Keep that future off the enclosing Db tick frame so a
     * normal host/test thread cannot accumulate it across connection
     * passes. */
    jz_err e = PC_FN(tick)(connection, next);
    if (e.code) {
        if (connection->link.tag == JAZZ_LINK_SUBSCRIBER && jazz_db__db__node_runtime__Transport__has_terminal_failure(&connection->transport)) {
            jz_err_drop(&e);
            jazz_db__db__PeerIoPump__disconnect(&connection->auxiliary_pump);
            jazz_db_peer_unlock(handle);
            jazz_db_PeerMutex *r = (jazz_db_PeerMutex *)jz_rc_clone(handle);
            jz_vec_push(retired, &r, sizeof(r));
            return 1;
        }
        jazz_db_peer_unlock(handle);
        *err = e;
        return 2;
    }
    JZ_VEC(jazz_model_TxId) released = {0};
    PC_FN(take_released_outbox_tx_ids)(connection, (void *)&released);
    for (u32 i = 0; i < released.len; ++i) jz_hmap_insert(released_outbox_tx_ids, &released.ptr[i], NULL, NULL);
    jz_vec_drop(&released, sizeof(jazz_model_TxId), NULL);
    jazz_db_peer_unlock(handle);
    return 0;
}

static jz_err detach_retired(jazz_db_Node *self, jazz_db_PeerMutexVec *connections, jazz_db_PeerMutexVec *retired) {
    for (u32 i = 0; i < retired->len; ++i) {
        bool detached;
        JZ_TRY(NODE_FN(detach_connection_async)(self, retired->ptr[i], &detached));
    }
    for (u32 i = 0; i < connections->len;) {
        bool failed = false;
        for (u32 j = 0; j < retired->len; ++j) if (retired->ptr[j] == connections->ptr[i]) { failed = true; break; }
        if (failed) { jazz_db_PeerMutex *removed; jz_vec_remove(connections, i, &removed, sizeof(removed)); jz_rc_release(removed); }
        else ++i;
    }
    jz_vec_truncate(retired, 0, sizeof(jazz_db_PeerMutex *), drop_peer_mutex_ptr);
    return JZ_OK;
}

/// Service every accepted subscriber connection once.
jz_err NODE_FN(tick)(jazz_db_Node *self, jazz_db_DbTickStats *out) {
    /* Storage futures wake on arbitrary threads, but subscriber links remain
     * thread-affine. Consume the cross-thread marker only at this owner
     * boundary, before any connection tick can observe stale readiness. */
    NODE_FN(mark_subscriber_connections_dirty_after_query_runtime_wake)(self);
    jazz_db__first_load__RemoteLinkTracker__on_tick(self->remote_link);
    drain_pending_detaches(self);
    u32 count;
    JZ_TRY(drain_transaction_abandonments(self, &count));
    JZ_TRY(NODE_FN(drain_subscription_finalizations)(self, &count));
    jazz_db_DbTickStats stats = {0, 0};
    jz_waker progress_waker;
    bool has_waker = NODE_FN(query_runtime_waker)(self, &progress_waker);
    u64 chunk_completion_generation = jazz_db__db__PeerChunkResolver__completion_generation(self->chunk_resolver);
    /* Another query can finish a local subscription's first terminal batch
     * before this turn. Groove is then idle, but the queued terminal still
     * needs to be folded into the application stream. Preserve its wake as
     * local delivery work as well as downstream peer publication work. */
    u64 local_dirty_generation = self->local_subscription_dirty_generation;
    bool refresh = local_dirty_generation != self->observed_local_subscription_dirty_generation
        || jazz_db__db__PeerChunkResolver__has_pending_local_demand(self->chunk_resolver)
        || chunk_completion_generation != self->observed_chunk_completion_generation;
    if (!refresh) {
        jazz_node_NodeState *node = jazz_db_node_lock(self->node);
        refresh = NS_FN(has_pending_query_runtime)(node);
        jazz_db_node_unlock(self->node);
    }
    if (refresh) {
        u32 events;
        JZ_TRY(jazz_db__db__node_runtime__refresh_subscriptions_in(self->node, self->subscriptions, self->active_authority_view_receipts, has_waker ? &progress_waker : NULL, &events));
        stats.subscription_events += events;
        /* Acknowledge only completed delivery. Cancellation/error retains
         * this generation, and wakes consumed during refresh remain dirty. */
        self->observed_local_subscription_dirty_generation = local_dirty_generation;
        self->observed_chunk_completion_generation = chunk_completion_generation;
    }
    bool remote_sync_applied = false;
    jz_hmap released_outbox_tx_ids;
    jz_hmap_init(&released_outbox_tx_ids, &jazz_db_txid_hset_desc);
    /* A later connection can mutate Core state after an earlier subscriber
     * has already had its turn in this pass. Remember that generation so
     * every subscriber is served on a fresh owner turn below. In
     * particular, do not recursively re-enter a just-admitted subscriber:
     * its initial view can suspend on cold storage, while later inbound
     * commit frames and their local fates must still get a turn. */
    u64 subscriber_dirty_epoch_before = *self->subscriber_dirty_epoch;
    jazz_db_PeerMutexVec connections;
    clone_peer_inventory(self, &connections);
    jazz_db_PeerMutexVec retired = {0};
    jz_err err = JZ_OK;
    for (u32 i = 0; i < connections.len; ++i) {
        jazz_db_DbTickStats next = {0, 0};
        int r = tick_connection(connections.ptr[i], &retired, &released_outbox_tx_ids, &next, &err);
        if (r == 2) goto fail;
        if (r == 1) continue;
        stats.subscription_events += next.subscription_events;
        stats.remote_sync_applied += next.remote_sync_applied;
        remote_sync_applied |= next.remote_sync_applied > 0;
    }
    err = detach_retired(self, &connections, &retired);
    if (err.code) goto fail;
    {
        bool subscriber_state_changed = *self->subscriber_dirty_epoch != subscriber_dirty_epoch_before;
        if (remote_sync_applied || subscriber_state_changed) {
            /* A binding with a host scheduler can service this newly dirty
             * subscriber on the next owner turn.  A bare `Db` intentionally
             * permits manual driving without installing such a scheduler,
             * though: dropping the wake in that mode would leave the
             * subscriber dirty until unrelated inbound traffic happened to
             * arrive. Preserve the former bounded second serve pass there. */
            if (self->scheduler->has) {
                for (u32 i = 0; i < connections.len; ++i) {
                    jazz_db_PeerConnection *c = jazz_db_peer_lock(connections.ptr[i]);
                    (void)PC_FN(mark_subscriber_dirty)(c);
                    jazz_db_peer_unlock(connections.ptr[i]);
                }
                NODE_FN(schedule_tick)(self, JAZZ_TICK_AFTER_CURRENT_TURN);
            } else {
                for (u32 i = 0; i < connections.len; ++i) {
                    jazz_db_PeerConnection *c = jazz_db_peer_lock(connections.ptr[i]);
                    bool should_tick = PC_FN(mark_subscriber_dirty)(c) || subscriber_state_changed;
                    jazz_db_peer_unlock(connections.ptr[i]);
                    if (should_tick) {
                        jazz_db_DbTickStats next = {0, 0};
                        int r = tick_connection(connections.ptr[i], &retired, &released_outbox_tx_ids, &next, &err);
                        if (r == 2) goto fail;
                        if (r == 1) continue;
                        stats.subscription_events += next.subscription_events;
                        stats.remote_sync_applied += next.remote_sync_applied;
                    }
                }
            }
        }
    }
    err = detach_retired(self, &connections, &retired);
    if (err.code) goto fail;
    drop_peer_inventory(&connections);
    jz_vec_drop(&retired, sizeof(jazz_db_PeerMutex *), drop_peer_mutex_ptr);
    err = advance_deletion_reconciliations(self);
    if (err.code) { jz_hmap_drop(&released_outbox_tx_ids); return err; }
    err = reconcile_scalar_query_inputs(self);
    if (err.code) { jz_hmap_drop(&released_outbox_tx_ids); return err; }
    if (released_outbox_tx_ids.len != 0) release_outbox_uploads(self, released_outbox_tx_ids);
    else jz_hmap_drop(&released_outbox_tx_ids);
    resolve_authority_witnesses(self);
    *out = stats;
    return JZ_OK;
fail:
    drop_peer_inventory(&connections);
    jz_vec_drop(&retired, sizeof(jazz_db_PeerMutex *), drop_peer_mutex_ptr);
    jz_hmap_drop(&released_outbox_tx_ids);
    return err;
}

static bool witness_handles_settled(void *ctx, const jazz_db_UpstreamCoverageHandleVec *handles) {
    jazz_node_NodeState *owner = (jazz_node_NodeState *)ctx;
    if (handles->len == 0) return false;
    for (u32 i = 0; i < handles->len; ++i) {
        jazz_protocol_AuthorityResultKey key;
        jz_err e = NS_FN(authority_result_key_for_subscription)(owner, handles->ptr[i].subscription, &key);
        if (e.code) { jz_err_drop(&e); return false; }
        bool ok = NS_FN(has_settled_authority_result)(owner, &key) && !NS_FN(opening_pending_for_authority_result)(owner, &key);
        jazz_protocol__protocol__AuthorityResultKey__drop(&key);
        if (!ok) return false;
    }
    return true;
}

/// Settle first-load authority witnesses after this turn's
/// inputs were folded into every stream, and retire the witness coverage
/// of every gate that has released.
static void resolve_authority_witnesses(jazz_db_Node *self) {
    if (!jazz_db__first_load__RemoteLinkTracker__has_witnesses(self->remote_link)) return;
    jazz_db_UpstreamCoverageHandleVec retired = {0};
    jazz_node_NodeState *owner = jazz_db_node_lock(self->node);
    jazz_db__first_load__RemoteLinkTracker__resolve_witnesses(self->remote_link, witness_handles_settled, owner, &retired);
    jazz_db_node_unlock(self->node);
    if (retired.len != 0) {
        jazz_db_PendingSubscriptionFinalization command;
        memset(&command, 0, sizeof(command));
        command.opening_upstream = retired;
        NODE_FN(enqueue_subscription_finalization)(self, command);
    } else {
        jz_vec_drop(&retired, sizeof(jazz_db_UpstreamCoverageHandle), jazz_db__db__UpstreamCoverageHandle__drop);
    }
}

static jz_err advance_scalar_reconciliation(jazz_db_Node *self, const jazz_db_PendingUpstreamSubscription *request, u64 revision,
                                            jazz_db_ScalarReconciliation *reconciliation, jazz_db_SubscriptionState *owner /* NULL = None */);

/// Both public local-first queries and relay-owned upstream scopes use
/// ordinary policy-bound point queries to refresh extra scalar inputs.
static jz_err reconcile_scalar_query_inputs(jazz_db_Node *self) {
    jazz_db_SubscriptionList live = {0};
    for (u32 i = 0; i < self->subscriptions->len; ++i) {
        jazz_db_SubscriptionState *w = self->subscriptions->ptr[i];
        if (w) ((jz_rc_hdr *)w - 1)->weak++;
        jz_vec_push(&live, &w, sizeof(w));
    }
    jz_err err = JZ_OK;
    for (u32 i = 0; i < live.len; ++i) {
        jazz_db_SubscriptionState *owner = jazz_db_weak_upgrade(live.ptr[i]);
        if (!owner) continue;
        jazz_db_SubscriptionState *state = owner;
        if (*state->closed || !state->scalar_reconciliation_enabled || state->upstream_subscription_handles.len == 0) { jz_rc_release(owner); continue; }
        jazz_db_UpstreamCoverageHandle *handle = &state->upstream_subscription_handles.ptr[0];
        jazz_db_PendingUpstreamSubscription request;
        memset(&request, 0, sizeof(request));
        request.subscription = handle->subscription;
        request.shape = jazz_model__query__ValidatedQuery__clone(state->shape);
        request.binding = jazz_model__query__Binding__clone(state->binding);
        jazz_protocol__protocol__RegisterShapeOptions__clone(&request.opts, &handle->coverage.opts);
        request.identity = state->author;
        request.has_policy_binding = true;
        if (state->has_request_identity_claims) {
            jazz_db__db__PolicyBinding__clone(&request.policy_binding, &state->request_identity_claims);
        } else {
            request.policy_binding.identity = state->author;
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            NS_FN(session_claims_for)(n, state->author, &request.policy_binding.claims);
            jazz_db_node_unlock(self->node);
        }
        u64 revision = state->scalar_authority_revision;
        jazz_db_ScalarReconciliation reconciliation = state->scalar_reconciliation;
        jazz_db__db__ScalarReconciliation__default(&state->scalar_reconciliation);
        err = advance_scalar_reconciliation(self, &request, revision, &reconciliation, owner);
        jazz_db__db__PendingUpstreamSubscription__drop(&request);
        if (err.code) { jazz_db__db__ScalarReconciliation__drop(&reconciliation); jz_rc_release(owner); goto done; }
        if (!*state->closed) {
            jazz_db__db__ScalarReconciliation__drop(&state->scalar_reconciliation);
            state->scalar_reconciliation = reconciliation;
        } else {
            jazz_db__db__ScalarReconciliation__drop(&reconciliation);
        }
        jz_rc_release(owner);
    }
    {
        JZ_VEC(jazz_db_RelayOwnerKey) owners = {0};
        jazz_db_RelayOwnerKey *k; void *v;
        JZ_BTREE_FOR(self->relay_upstream_subscription_owners, k, v) jz_vec_push(&owners, k, sizeof(*k));
        for (u32 i = 0; i < owners.len; ++i) {
            jazz_db_RelayUpstreamSubscriptionOwner *owner = (jazz_db_RelayUpstreamSubscriptionOwner *)jz_btree_get(self->relay_upstream_subscription_owners, &owners.ptr[i]);
            if (!owner) continue;
            jazz_db_PendingUpstreamSubscription request;
            jazz_db__db__PendingUpstreamSubscription__clone(&request, &owner->request);
            u64 revision = owner->scalar_authority_revision;
            jazz_db_ScalarReconciliation reconciliation = owner->scalar_reconciliation;
            jazz_db__db__ScalarReconciliation__default(&owner->scalar_reconciliation);
            err = advance_scalar_reconciliation(self, &request, revision, &reconciliation, NULL);
            jazz_db__db__PendingUpstreamSubscription__drop(&request);
            if (err.code) { jazz_db__db__ScalarReconciliation__drop(&reconciliation); jz_vec_drop(&owners, sizeof(jazz_db_RelayOwnerKey), NULL); goto done; }
            owner = (jazz_db_RelayUpstreamSubscriptionOwner *)jz_btree_get(self->relay_upstream_subscription_owners, &owners.ptr[i]);
            if (owner) {
                jazz_db__db__ScalarReconciliation__drop(&owner->scalar_reconciliation);
                owner->scalar_reconciliation = reconciliation;
            } else {
                jazz_db__db__ScalarReconciliation__drop(&reconciliation);
            }
        }
        jz_vec_drop(&owners, sizeof(jazz_db_RelayOwnerKey), NULL);
    }
done:
    jz_vec_drop(&live, sizeof(jazz_db_SubscriptionState *), drop_weak_state);
    return err;
}

/// Re-run local subscription refresh on the next owner turn.
static void mark_local_subscriptions_dirty(jazz_db_Node *self) {
    *self->subscriber_dirty_epoch += 1;
    self->local_subscription_dirty_generation += 1;
    NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
}

/* ---------------- deletion reconciliation ---------------- */

static void scheduler_schedule_tick_after(jazz_db_SharedTickScheduler *scheduler, u64 delay_ms) {
    if (scheduler->has && scheduler->s.vt->schedule_tick_after) scheduler->s.vt->schedule_tick_after(scheduler->s.self, delay_ms);
}

static bool held_rows_all_answered(const jazz_db_HeldRowChecks *checks, const jazz_db_DeletionReconciliation *r) {
    for (u32 i = 0; i < r->probing_rows.len; ++i)
        if (jz_btree_contains(&checks->rows, &r->probing_rows.ptr[i])) return false;
    return true;
}

static void release_probing_streams(jazz_db_Node *self, bool routable);
static jz_err advance_runtime_deletion_discoveries(jazz_db_Node *self);
static void advance_held_row_checks(jazz_db_Node *self);

/// Drive deletion reconciliation (see [`DeletionReconciliation`] and
/// [`HeldRowChecks`]). Streams hand their omitted rows to the runtime and
/// withhold first settlement for at most `DELETION_DISCOVERY_LIMIT`;
/// past that bound an unfinished discovery moves to the runtime too.
/// Nothing is abandoned on a timer: every held row stays checked until
/// the authority answers it (INV-SYNC-48). Access loss is never applied
/// as local redaction (INV-SYNC-14).
static jz_err advance_deletion_reconciliations(jazz_db_Node *self) {
    jazz_db_SubscriptionList live = {0};
    for (u32 i = 0; i < self->subscriptions->len; ++i) {
        jazz_db_SubscriptionState *w = self->subscriptions->ptr[i];
        if (w) ((jz_rc_hdr *)w - 1)->weak++;
        jz_vec_push(&live, &w, sizeof(w));
    }
    for (u32 i = 0; i < live.len; ++i) {
        jazz_db_SubscriptionState *owner = jazz_db_weak_upgrade(live.ptr[i]);
        if (!owner) continue;
        jazz_db_Instant since;
        if (*owner->closed || !jazz_db__db__DeletionReconciliation__waiting_since(&owner->deletion_reconciliation, &since)) { jz_rc_release(owner); continue; }
        jazz_db_Instant now = jazz_db_instant_now();
        u64 elapsed = now > since ? now - since : 0;
        bool expired = elapsed >= JAZZ_DELETION_DISCOVERY_LIMIT_MS;
        jazz_db_DeletionReconciliation reconciliation = owner->deletion_reconciliation;
        memset(&owner->deletion_reconciliation, 0, sizeof(reconciliation));
        owner->deletion_reconciliation.tag = JAZZ_DR_DISABLED;
        jazz_db_DeletionReconciliation next;
        memset(&next, 0, sizeof(next));
        if (reconciliation.tag == JAZZ_DR_PENDING) {
            next.tag = JAZZ_DR_PROBING;
            next.since = reconciliation.since;
            jazz_db__db__HeldRowChecks__enqueue(&self->held_row_checks, &reconciliation.rows, &reconciliation.context, &next.probing_rows);
            memset(&reconciliation.rows, 0, sizeof(reconciliation.rows));
            jazz_db__db__DeletionReconciliation__drop(&reconciliation);
        } else if (reconciliation.tag == JAZZ_DR_PROBING && (expired || held_rows_all_answered(&self->held_row_checks, &reconciliation))) {
            /* Resolved, or settlement stops waiting; unresolved rows
             * keep being checked by the runtime. */
            jazz_db__db__DeletionReconciliation__drop(&reconciliation);
            next.tag = JAZZ_DR_DONE;
        } else if ((reconciliation.tag == JAZZ_DR_DEFERRED || reconciliation.tag == JAZZ_DR_DISCOVERING) && expired) {
            jazz_db_DeletionReconciliation redundant;
            bool has_redundant = jazz_db__db__HeldRowChecks__adopt_discovery(&self->held_row_checks, reconciliation, &redundant);
            if (has_redundant) {
                jazz_node_NodeState *node = jazz_db_node_lock(self->node);
                release_redundant_discovery(node, true, &redundant);
                jazz_db_node_unlock(self->node);
            }
            next.tag = JAZZ_DR_DONE;
        } else {
            next = reconciliation;
        }
        bool done = next.tag == JAZZ_DR_DONE;
        bool deferred = next.tag == JAZZ_DR_DEFERRED;
        bool discovering = deferred || next.tag == JAZZ_DR_DISCOVERING;
        owner->deletion_reconciliation = next;
        jz_rc_release(owner);
        if (done) {
            /* Ingested images changed local storage, or the bound passed;
             * the withheld stream now refreshes and settles. */
            mark_local_subscriptions_dirty(self);
            continue;
        }
        /* Discovery progresses in subscription refresh; re-arm it on a
         * short cadence (not a hot loop) within the bound. A deferred
         * stream waits for its own receiver graph, which a busy runtime
         * is still advancing; an opened discovery graph always drains. */
        if (discovering) {
            bool bump = !deferred;
            if (!bump) {
                jazz_node_NodeState *node = jazz_db_node_lock(self->node);
                bump = !NS_FN(has_pending_query_runtime)(node);
                jazz_db_node_unlock(self->node);
            }
            if (bump) self->local_subscription_dirty_generation += 1;
        }
        if (self->scheduler->has) {
            u64 remaining = JAZZ_DELETION_DISCOVERY_LIMIT_MS > elapsed ? JAZZ_DELETION_DISCOVERY_LIMIT_MS - elapsed : 0;
            if (remaining < 1) remaining = 1;
            scheduler_schedule_tick_after(self->scheduler, discovering ? (remaining < 50 ? remaining : 50) : remaining);
        }
    }
    jz_vec_drop(&live, sizeof(jazz_db_SubscriptionState *), drop_weak_state);
    JZ_TRY(advance_runtime_deletion_discoveries(self));
    u32 held_before = self->held_row_checks.rows.len;
    advance_held_row_checks(self);
    /* Release waiting streams in this same turn, not one turn later (the
     * pass above ran before this turn's answers were polled): those whose
     * held rows are all answered, and, while no upstream authority is
     * admitted, every probing stream, since no probe can be routed at all.
     * Released rows stay in `HeldRowChecks` and are probed once an
     * upstream is admitted (INV-SYNC-48). */
    bool routable = self->admitted_upstream_authority->has;
    u32 held_after = self->held_row_checks.rows.len;
    if (held_after < held_before || (!routable && held_after > 0)) release_probing_streams(self, routable);
    return JZ_OK;
}

/// Mark `Probing` streams as `Done` so they refresh and settle on the next
/// owner turn: every one when `routable` is false, otherwise those whose
/// held rows the authority has all answered.
static void release_probing_streams(jazz_db_Node *self, bool routable) {
    bool released = false;
    for (u32 i = 0; i < self->subscriptions->len; ++i) {
        jazz_db_SubscriptionState *owner = jazz_db_weak_upgrade(self->subscriptions->ptr[i]);
        if (!owner) continue;
        if (!*owner->closed && owner->deletion_reconciliation.tag == JAZZ_DR_PROBING) {
            bool resolved = !routable || held_rows_all_answered(&self->held_row_checks, &owner->deletion_reconciliation);
            if (resolved) {
                jazz_db__db__DeletionReconciliation__drop(&owner->deletion_reconciliation);
                memset(&owner->deletion_reconciliation, 0, sizeof(owner->deletion_reconciliation));
                owner->deletion_reconciliation.tag = JAZZ_DR_DONE;
                released = true;
            }
        }
        jz_rc_release(owner);
    }
    if (released) mark_local_subscriptions_dirty(self);
}

/* DiscoveryStep: Cold(maintained) / Snapshot(snapshot) */
typedef struct DiscoveryStep { bool cold; jazz_node_LocalMaintainedViewSubscription *maintained; jazz_node_RelationSnapshot snapshot; } DiscoveryStep;

static jz_err drain_deletion_discovery(jazz_node_NodeState *owner, jazz_node_LocalMaintainedViewSubscription *maintained, const jz_waker *progress_waker, DiscoveryStep *out);

/// Open the Local-tier discovery graph of `spec`'s query. With `drive`, a
/// cold opening is driven to completion in this turn.
static jz_err open_deletion_discovery(jazz_node_NodeState *owner, const jazz_db_DeletionDiscoverySpec *spec, const jz_waker *progress_waker, bool drive, DiscoveryStep *out) {
    jazz_node_LocalMaintainedViewSubscription *maintained = NULL;
    jazz_node_RelationSnapshot snapshot;
    memset(&snapshot, 0, sizeof(snapshot));
    {
        jz_btree claims;
        jz_btree_clone(&claims, &spec->claims, str_key_clone, groove__records__Value__clone);
        jazz_node_NodeState *scoped = NS_FN(scoped_optional_session_claims)(owner, spec->author, &claims);
        jazz_protocol_ReadViewSpec *view = jazz_protocol__protocol__ReadViewSpec__default();
        jz_err e = NS_FN(open_maintained_view_subscription_in_authorization_mode_with_waker)(
            scoped, spec->shape, spec->binding, spec->author, JAZZ_DUR_LOCAL, view, NULL,
            JAZZ_QAM_CLIENT_LOCAL, false, progress_waker, &maintained, &snapshot);
        jazz_protocol__protocol__ReadViewSpec__drop(view);
        NS_FN(end_scoped_claims)(owner);
        jz_btree_drop(&claims);
        if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    }
    if (jazz_node__node__LocalMaintainedViewSubscription__initial_snapshot_received(maintained)) {
        NS_FN(unsubscribe_groove_subscription)(owner, jazz_node__node__LocalMaintainedViewSubscription__subscription_id(maintained));
        jazz_node__node__LocalMaintainedViewSubscription__drop(maintained);
        out->cold = false;
        out->snapshot = snapshot;
        return JZ_OK;
    }
    jazz_node__node__RelationSnapshot__drop(&snapshot);
    if (!drive) { out->cold = true; out->maintained = maintained; return JZ_OK; }
    /* A cold opening needs only local storage work here: the stream graph
     * and the runtime are idle, so no chunk demand is outstanding. */
    jz_err e = NS_FN(drive_query_runtime)(owner);
    if (e.code) { jazz_node__node__LocalMaintainedViewSubscription__drop(maintained); return jazz_node__node__api_error__Error__From__from(e); }
    return drain_deletion_discovery(owner, maintained, progress_waker, out);
}

/// Drain a cold discovery graph without blocking on other query work.
static jz_err drain_deletion_discovery(jazz_node_NodeState *owner, jazz_node_LocalMaintainedViewSubscription *maintained, const jz_waker *progress_waker, DiscoveryStep *out) {
    jz_err drained = NS_FN(drive_ready_query_runtime_with_waker)(owner, progress_waker);
    if (!drained.code) {
        jz_btree empty;
        jz_btree_init(&empty, &jazz_db_str_set_desc);
        bool has_update = false, suppressed = false;
        jazz_node_LocalMaintainedUpdate update;
        memset(&update, 0, sizeof(update));
        drained = NS_FN(drain_local_maintained_view_subscription_preserving_rows_with_waker)(owner, maintained, NULL, &empty, progress_waker, &has_update, &update, &suppressed);
        jz_btree_drop(&empty);
    }
    if (drained.code) {
        NS_FN(unsubscribe_groove_subscription)(owner, jazz_node__node__LocalMaintainedViewSubscription__subscription_id(maintained));
        jazz_node__node__LocalMaintainedViewSubscription__drop(maintained);
        return jazz_node__node__api_error__Error__From__from(drained);
    }
    if (!jazz_node__node__LocalMaintainedViewSubscription__initial_snapshot_received(maintained)) {
        out->cold = true;
        out->maintained = maintained;
        return JZ_OK;
    }
    jazz_node_MaterializedSnapshot materialized;
    jz_err e = NS_FN(materialize_local_maintained_relation_snapshot_with_occurrences)(owner, maintained, &materialized);
    NS_FN(unsubscribe_groove_subscription)(owner, jazz_node__node__LocalMaintainedViewSubscription__subscription_id(maintained));
    jazz_node__node__LocalMaintainedViewSubscription__drop(maintained);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    out->cold = false;
    out->snapshot = materialized.snapshot;
    jz_vec_drop(&materialized.root_occurrence_ids, 1, NULL);
    return JZ_OK;
}

typedef JZ_VEC(jazz_protocol_CurrentRowCoordinate) CoordinateVec;

/// Held rows of a discovery snapshot that the authority answer omitted and
/// whose current local winner is an accepted Global transaction.
static jz_err deletion_candidates(jazz_node_NodeState *owner, const jazz_node_RelationSnapshot *snapshot, const jazz_db_DeletionDiscoverySpec *spec, CoordinateVec *out) {
    const jz_str *table = &jazz_model__query__ValidatedQuery__query(spec->shape)->table;
    memset(out, 0, sizeof(*out));
    u32 n = snapshot->root_count < snapshot->rows.len ? snapshot->root_count : snapshot->rows.len;
    for (u32 i = 0; i < n; ++i) {
        jazz_types_RowUuid row = jazz_node__node__CurrentRow__row_uuid(&snapshot->rows.ptr[i]);
        if (jz_btree_contains(&spec->authoritative, &row)) continue;
        bool has; jazz_model_TxId tx;
        jz_err e = NS_FN(local_content_winner_tx_id)(owner, table, row, &has, &tx);
        if (e.code) { jz_vec_drop(out, sizeof(jazz_protocol_CurrentRowCoordinate), jazz_protocol__protocol__CurrentRowCoordinate__drop); return jazz_node__node__api_error__Error__From__from(e); }
        if (!has) continue;
        jazz_node_TransactionRecord record;
        bool accepted_global = NS_FN(transaction_record)(owner, tx, &record)
            && record.fate.tag == JAZZ_FATE_ACCEPTED && record.durability >= JAZZ_DUR_GLOBAL;
        if (!accepted_global) continue;
        jazz_protocol_CurrentRowCoordinate coordinate;
        jz_err ce = NS_FN(current_row_coordinate)(owner, table, row, &coordinate);
        if (ce.code) { jz_err_drop(&ce); continue; }
        jz_vec_push(out, &coordinate, sizeof(coordinate));
    }
    return JZ_OK;
}

/// Release the discovery graph of a discovery superseded by an equivalent
/// runtime-owned one.
static void release_redundant_discovery(jazz_node_NodeState *owner, bool has_redundant, jazz_db_DeletionReconciliation *redundant) {
    if (!has_redundant) return;
    if (redundant->tag == JAZZ_DR_DISCOVERING && NS_FN(groove_runtime_token)(owner) == redundant->runtime_token)
        NS_FN(unsubscribe_groove_subscription)(owner, jazz_node__node__LocalMaintainedViewSubscription__subscription_id(redundant->maintained));
    jazz_db__db__DeletionReconciliation__drop(redundant);
}

typedef struct FoundRows { CoordinateVec rows; jazz_protocol_PolicyBindingKey context; } FoundRows;

/// Finish discoveries whose stream closed or stopped waiting. These no
/// longer depend on the stream's receiver graph; a cold discovery graph
/// is drained without blocking on other query work.
static jz_err advance_runtime_deletion_discoveries(jazz_db_Node *self) {
    if (self->held_row_checks.discoveries.len == 0) return JZ_OK;
    JZ_VEC(jazz_db_DeletionReconciliation) discoveries = { self->held_row_checks.discoveries.ptr, self->held_row_checks.discoveries.len, self->held_row_checks.discoveries.cap };
    memset(&self->held_row_checks.discoveries, 0, sizeof(self->held_row_checks.discoveries));
    jz_waker waker;
    bool has_waker = NODE_FN(query_runtime_waker)(self, &waker);
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    u64 runtime_token = NS_FN(groove_runtime_token)(node);
    JZ_VEC(jazz_db_DeletionReconciliation) remaining = {0};
    JZ_VEC(FoundRows) found = {0};
    jz_err failure = JZ_OK;
    for (u32 i = 0; i < discoveries.len; ++i) {
        jazz_db_DeletionReconciliation discovery = discoveries.ptr[i];
        if (failure.code) { jz_vec_push(&remaining, &discovery, sizeof(discovery)); continue; }
        jazz_node_LocalMaintainedViewSubscription *maintained = NULL;
        jazz_db_DeletionDiscoverySpec *spec;
        if (discovery.tag == JAZZ_DR_DISCOVERING && discovery.runtime_token == runtime_token) {
            maintained = discovery.maintained;
            spec = discovery.spec;
        } else if (discovery.tag == JAZZ_DR_DISCOVERING || discovery.tag == JAZZ_DR_DEFERRED) {
            /* A runtime rebuild retired the old graph with its IDs. */
            if (discovery.maintained) jazz_node__node__LocalMaintainedViewSubscription__drop(discovery.maintained);
            spec = discovery.spec;
        } else {
            jazz_db__db__DeletionReconciliation__drop(&discovery);
            continue;
        }
        jazz_db_Instant since = jazz_db_instant_now();
        DiscoveryStep step;
        memset(&step, 0, sizeof(step));
        jz_err result = maintained
            ? drain_deletion_discovery(node, maintained, has_waker ? &waker : NULL, &step)
            : open_deletion_discovery(node, spec, has_waker ? &waker : NULL, false, &step);
        jazz_db_DeletionReconciliation next;
        memset(&next, 0, sizeof(next));
        if (!result.code && step.cold) {
            next.tag = JAZZ_DR_DISCOVERING;
            next.maintained = step.maintained;
            next.runtime_token = runtime_token;
            next.since = since;
            next.spec = spec;
            jz_vec_push(&remaining, &next, sizeof(next));
        } else if (!result.code) {
            FoundRows f;
            jz_err ce = deletion_candidates(node, &step.snapshot, spec, &f.rows);
            jazz_node__node__RelationSnapshot__drop(&step.snapshot);
            if (!ce.code) {
                jazz_protocol__protocol__PolicyBindingKey__clone(&f.context, &spec->context);
                jz_vec_push(&found, &f, sizeof(f));
                jazz_db__db__DeletionDiscoverySpec__drop(spec);
                jz_free(spec);
            } else {
                next.tag = JAZZ_DR_DEFERRED;
                next.since = since;
                next.spec = spec;
                jz_vec_push(&remaining, &next, sizeof(next));
                failure = ce;
            }
        } else {
            next.tag = JAZZ_DR_DEFERRED;
            next.since = since;
            next.spec = spec;
            jz_vec_push(&remaining, &next, sizeof(next));
            failure = result;
        }
    }
    jz_vec_drop(&discoveries, sizeof(jazz_db_DeletionReconciliation), NULL);
    jazz_db_node_unlock(self->node);
    jazz_db_HeldRowChecks *checks = &self->held_row_checks;
    for (u32 i = 0; i < found.len; ++i) {
        JZ_VEC(jazz_db_HeldRowKey) keys = {0};
        jazz_db__db__HeldRowChecks__enqueue(checks, &found.ptr[i].rows, &found.ptr[i].context, &keys);
        jz_vec_drop(&keys, sizeof(jazz_db_HeldRowKey), jazz_db__db__HeldRowKey__drop);
        jazz_protocol__protocol__PolicyBindingKey__drop(&found.ptr[i].context);
    }
    jz_vec_drop(&found, sizeof(FoundRows), NULL);
    bool pending = remaining.len != 0;
    jz_vec_extend(&remaining, checks->discoveries.ptr, checks->discoveries.len, sizeof(jazz_db_DeletionReconciliation));
    jz_vec_drop(&checks->discoveries, sizeof(jazz_db_DeletionReconciliation), NULL);
    checks->discoveries.ptr = remaining.ptr;
    checks->discoveries.len = remaining.len;
    checks->discoveries.cap = remaining.cap;
    if (pending) scheduler_schedule_tick_after(self->scheduler, 100);
    return failure;
}

typedef struct HeldEntry { jazz_db_HeldRowKey key; jazz_protocol_CurrentRowCoordinate coordinate; } HeldEntry;

/// Probe held rows in `MAX_CURRENT_ROWS` batches through the ordinary
/// current-rows path; the router ingests each receipt's carriers
/// (deleted images included) before exposing it. A row leaves the set
/// only when the authority answers it (`Readable`, including a deletion,
/// or `CurrentUnavailable`, which is access loss and is not applied) or
/// when its local coordinate changes. Unknown answers, dropped receipts,
/// timeouts and disconnects are retried with backoff.
static void advance_held_row_checks(jazz_db_Node *self) {
    bool has_upstream = self->admitted_upstream_authority->has;
    u64 upstream = has_upstream ? self->admitted_upstream_authority->ctx.connection_id : 0;
    jazz_db_Instant now = jazz_db_instant_now();
    jazz_db_HeldRowChecks *checks = &self->held_row_checks;
    if (checks->rows.len == 0) {
        if (checks->has_active) {
            jazz_db_future_drop(&checks->active.future);
            jz_vec_drop(&checks->active.rows, sizeof(jazz_db_HeldRowKey), jazz_db__db__HeldRowKey__drop);
            checks->has_active = false;
        }
        checks->has_upstream_connection = has_upstream;
        checks->upstream_connection = upstream;
        return;
    }
    if (has_upstream && (!checks->has_upstream_connection || checks->upstream_connection != upstream)) {
        /* A new upstream link: retry every held row immediately. */
        void *k; jazz_db_HeldRowCheck *check;
        JZ_BTREE_FOR(&checks->rows, k, check) { check->attempts = 0; check->has_retry_at = false; }
    }
    checks->has_upstream_connection = has_upstream;
    checks->upstream_connection = upstream;
    bool changed = false;
    if (checks->has_active) {
        static const jz_waker noop = {0};
        jz_waker waker;
        bool has_waker = NODE_FN(query_runtime_waker)(self, &waker);
        jazz_db_CurrentRowsResult result;
        bool has_result = jazz_db_future_poll(&checks->active.future, has_waker ? &waker : &noop, &result);
        if (!has_result && now >= checks->active.deadline) {
            has_result = true;
            result.tag = JAZZ_CRR_UNKNOWN;
            result.receipt = NULL;
        }
        if (has_result) {
            jazz_db_HeldRowProbe probe = checks->active;
            checks->has_active = false;
            jazz_db_future_drop(&probe.future);
            jz_btree answered;
            jz_btree_init(&answered, &jazz_db_held_row_key_set_desc);
            if (result.tag == JAZZ_CRR_APPLIED && result.receipt) {
                jazz_protocol_CurrentRowsReceipt *receipt = result.receipt;
                u32 n = receipt->rows.len < receipt->outcomes.len ? receipt->rows.len : receipt->outcomes.len;
                for (u32 i = 0; i < n; ++i) {
                    if (receipt->outcomes.ptr[i] == JAZZ_CRO_UNKNOWN) continue;
                    jazz_db_HeldRowKey key;
                    key.table = jz_str_clone(&receipt->rows.ptr[i].table);
                    key.row = receipt->rows.ptr[i].row;
                    if (jz_btree_insert(&answered, &key, NULL, NULL)) jz_str_drop(&key.table);
                }
            }
            jazz_db__row_availability__CurrentRowsResult__drop(&result);
            changed = answered.len != 0;
            for (u32 i = 0; i < probe.rows.len; ++i) {
                jazz_db_HeldRowKey *key = &probe.rows.ptr[i];
                if (jz_btree_contains(&answered, key)) {
                    jz_btree_remove(&checks->rows, key, NULL, NULL);
                } else {
                    jazz_db_HeldRowCheck *check = (jazz_db_HeldRowCheck *)jz_btree_get(&checks->rows, key);
                    if (check) {
                        check->has_retry_at = true;
                        check->retry_at = now + jazz_db__db__HeldRowChecks__retry_delay(check->attempts);
                        if (check->attempts != UINT32_MAX) check->attempts += 1;
                    }
                }
            }
            jz_btree_drop(&answered);
            jz_vec_drop(&probe.rows, sizeof(jazz_db_HeldRowKey), jazz_db__db__HeldRowKey__drop);
        }
    }
    /* Rows to re-validate before dispatching the next batch. */
    if (!checks->has_active && has_upstream) {
        JZ_VEC(HeldEntry) held = {0};
        {
            jazz_db_HeldRowKey *key; jazz_db_HeldRowCheck *check;
            JZ_BTREE_FOR(&checks->rows, key, check) {
                HeldEntry e;
                jazz_db__db__HeldRowKey__clone(&e.key, key);
                jazz_protocol__protocol__CurrentRowCoordinate__clone(&e.coordinate, &check->coordinate);
                jz_vec_push(&held, &e, sizeof(e));
            }
        }
        /* Cancel rows whose local version changed (or was deleted) since
         * discovery: later reads reconcile the new version. */
        JZ_VEC(jazz_db_HeldRowKey) changed_rows = {0};
        {
            jazz_node_NodeState *node = jazz_db_node_lock(self->node);
            for (u32 i = 0; i < held.len; ++i) {
                jazz_protocol_CurrentRowCoordinate current;
                jz_err e = NS_FN(current_row_coordinate)(node, &held.ptr[i].coordinate.table, held.ptr[i].coordinate.row, &current);
                bool same = !e.code && jazz_protocol__protocol__CurrentRowCoordinate__eq(&current, &held.ptr[i].coordinate);
                if (!e.code) jazz_protocol__protocol__CurrentRowCoordinate__drop(&current); else jz_err_drop(&e);
                if (!same) { jazz_db_HeldRowKey k; jazz_db__db__HeldRowKey__clone(&k, &held.ptr[i].key); jz_vec_push(&changed_rows, &k, sizeof(k)); }
                jazz_db__db__HeldRowKey__drop(&held.ptr[i].key);
                jazz_protocol__protocol__CurrentRowCoordinate__drop(&held.ptr[i].coordinate);
            }
            jazz_db_node_unlock(self->node);
        }
        jz_vec_drop(&held, sizeof(HeldEntry), NULL);
        for (u32 i = 0; i < changed_rows.len; ++i) jz_btree_remove(&checks->rows, &changed_rows.ptr[i], NULL, NULL);
        jz_vec_drop(&changed_rows, sizeof(jazz_db_HeldRowKey), jazz_db__db__HeldRowKey__drop);
        /* ready rows: retry_at is none or due; batch the first context */
        const jazz_protocol_PolicyBindingKey *first_context = NULL;
        JZ_VEC(jazz_db_HeldRowKey) keys = {0};
        CoordinateVec batch = {0};
        jazz_db_HeldRowKey *key; jazz_db_HeldRowCheck *check;
        JZ_BTREE_FOR(&checks->rows, key, check) {
            if (check->has_retry_at && check->retry_at > now) continue;
            if (!first_context) first_context = &check->context;
            if (!jazz_protocol__protocol__PolicyBindingKey__eq(&check->context, first_context)) continue;
            if (keys.len >= JAZZ_PROTOCOL_MAX_CURRENT_ROWS) break;
            jazz_db_HeldRowKey k; jazz_db__db__HeldRowKey__clone(&k, key);
            jz_vec_push(&keys, &k, sizeof(k));
            jazz_protocol_CurrentRowCoordinate c; jazz_protocol__protocol__CurrentRowCoordinate__clone(&c, &check->coordinate);
            jz_vec_push(&batch, &c, sizeof(c));
        }
        if (first_context) {
            jazz_protocol_PolicyBindingKey context;
            jazz_protocol__protocol__PolicyBindingKey__clone(&context, first_context);
            checks->active.rows.ptr = keys.ptr; checks->active.rows.len = keys.len; checks->active.rows.cap = keys.cap;
            checks->active.deadline = now + 10000;
            checks->active.future = jazz_db__db__row_availability__Node__request_current_rows(self, &batch, context);
            checks->has_active = true;
            scheduler_schedule_tick_after(self->scheduler, 10000);
            NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
        } else {
            jz_vec_drop(&keys, sizeof(jazz_db_HeldRowKey), NULL);
            jz_vec_drop(&batch, sizeof(jazz_protocol_CurrentRowCoordinate), NULL);
        }
    }
    /* Wake for the earliest backoff; with no upstream, a reconnect's own
     * turn resumes the checks instead of a timer spinning. */
    bool has_next = false;
    jazz_db_Instant next_retry = 0;
    {
        void *k; jazz_db_HeldRowCheck *check;
        JZ_BTREE_FOR(&checks->rows, k, check) {
            if (!check->has_retry_at || check->retry_at <= now) continue;
            if (!has_next || check->retry_at < next_retry) { next_retry = check->retry_at; has_next = true; }
        }
    }
    if (!checks->has_active && has_upstream && has_next && self->scheduler->has) {
        u64 delay = next_retry - now;
        if (delay < 1) delay = 1;
        scheduler_schedule_tick_after(self->scheduler, delay);
    }
    if (changed) {
        /* Deletions applied after settlement emit the ordinary local
         * change, so live subscriptions update. */
        mark_local_subscriptions_dirty(self);
    }
}

/* ---------------- scalar reconciliation ---------------- */

static void scalar_reset(jazz_db_ScalarReconciliation *state) {
    jazz_db__db__ScalarReconciliation__drop(state);
    jazz_db__db__ScalarReconciliation__default(state);
}

static void request_policy_binding_or(const jazz_db_PendingUpstreamSubscription *request, jazz_db_PolicyBinding *out) {
    if (request->has_policy_binding) jazz_db__db__PolicyBinding__clone(out, &request->policy_binding);
    else { out->identity = request->identity; jz_btree_init(&out->claims, &jazz_db_str_value_desc); }
}

static bool accepted_global_winner(jazz_node_NodeState *node, const jz_str *table, jazz_types_RowUuid row, jz_err *err) {
    bool has; jazz_model_TxId tx;
    jz_err e = NS_FN(local_content_winner_tx_id)(node, table, row, &has, &tx);
    if (e.code) { *err = jazz_node__node__api_error__Error__From__from(e); return false; }
    if (!has) return false;
    jazz_node_TransactionRecord record;
    return NS_FN(transaction_record)(node, tx, &record) && record.fate.tag == JAZZ_FATE_ACCEPTED && record.durability >= JAZZ_DUR_GLOBAL;
}

static jz_err advance_scalar_reconciliation(jazz_db_Node *self, const jazz_db_PendingUpstreamSubscription *request, u64 revision,
                                            jazz_db_ScalarReconciliation *state, jazz_db_SubscriptionState *local_owner) {
    if (!jazz_protocol__protocol__ReadViewSpec__is_default(request->opts.read_view)
        || !jazz_node__node__simple_scalar_exit_query(jazz_model__query__ValidatedQuery__query(request->shape)))
        return JZ_OK;
    jazz_db_ActiveAuthorityViewReceipts *receipts = self->active_authority_view_receipts;
    if (!(receipts->has && jz_btree_contains(&receipts->subscriptions, &request->subscription))) {
        scalar_reset(state);
        return JZ_OK;
    }
    jazz_db_ScalarGeneration generation = { receipts->connection_epoch, request->subscription, revision };
    /* A new source receipt cannot starve the current bounded batch. It
     * invalidates queued candidates only after the active ordinary probe
     * completes. An authority connection replacement cancels immediately. */
    if (state->has_generation && (state->generation.epoch != generation.epoch || !jazz_protocol_SubscriptionKey_eq(&state->generation.subscription, &generation.subscription)))
        scalar_reset(state);
    if (state->has_active) {
        static const jz_waker noop = {0};
        jz_waker waker;
        bool has_waker = NODE_FN(query_runtime_waker)(self, &waker);
        jazz_db_CurrentRowsResult result;
        if (!jazz_db_future_poll(&state->active.future, has_waker ? &waker : &noop, &result)) {
            if (jazz_db_instant_now() >= state->active.deadline) { result.tag = JAZZ_CRR_UNKNOWN; result.receipt = NULL; }
            else return JZ_OK;
        }
        JZ_VEC(jazz_types_RowUuid) retry_rows = {0};
        if (result.tag == JAZZ_CRR_UNKNOWN) {
            jz_vec_extend(&retry_rows, state->active.rows.ptr, state->active.rows.len, sizeof(jazz_types_RowUuid));
        } else {
            jazz_protocol_CurrentRowsReceipt *receipt = result.receipt;
            u32 n = receipt->rows.len < receipt->outcomes.len ? receipt->rows.len : receipt->outcomes.len;
            for (u32 i = 0; i < n; ++i)
                if (receipt->outcomes.ptr[i] == JAZZ_CRO_UNKNOWN) jz_vec_push(&retry_rows, &receipt->rows.ptr[i].row, sizeof(jazz_types_RowUuid));
        }
        jazz_db_future_drop(&state->active.future);
        jz_vec_drop(&state->active.rows, sizeof(jazz_types_RowUuid), NULL);
        state->has_active = false;
        if (result.tag == JAZZ_CRR_APPLIED) {
            jazz_protocol_CurrentRowsReceipt *receipt = result.receipt;
            /* The router has correlated this receipt and ingested native
             * carriers. This live original owner additionally gates marker
             * application; raw wire claims are not the derived local key. */
            jazz_db_PolicyBinding pb;
            request_policy_binding_or(request, &pb);
            jazz_protocol_PolicyBindingKey expected;
            jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(&expected, pb.identity, pb.claims);
            bool context_matches = jazz_protocol__protocol__PolicyBindingKey__eq(&receipt->context, &expected);
            jazz_protocol__protocol__PolicyBindingKey__drop(&expected);
            if (!context_matches) {
                jazz_db__row_availability__CurrentRowsResult__drop(&result);
                jz_vec_drop(&retry_rows, sizeof(jazz_types_RowUuid), NULL);
                scalar_reset(state);
                return JZ_OK;
            }
            jazz_node_NodeState *owner = jazz_db_node_lock(self->node);
            bool stale = false;
            if (local_owner) {
                bool any = false;
                for (u32 i = 0; i < local_owner->upstream_subscription_handles.len; ++i)
                    if (jazz_protocol_SubscriptionKey_eq(&local_owner->upstream_subscription_handles.ptr[i].subscription, &request->subscription)) { any = true; break; }
                stale = *local_owner->closed || !any;
            } else {
                bool any = false;
                void *k; jazz_db_RelayUpstreamSubscriptionOwner *relay;
                JZ_BTREE_FOR(self->relay_upstream_subscription_owners, k, relay) {
                    bool same_binding = relay->request.has_policy_binding == request->has_policy_binding
                        && (!request->has_policy_binding || jazz_db__db__PolicyBinding__eq(&relay->request.policy_binding, &request->policy_binding));
                    if (jazz_protocol_SubscriptionKey_eq(&relay->request.subscription, &request->subscription) && same_binding) { any = true; break; }
                }
                stale = !any;
            }
            if (!stale) stale = !(receipts->has && receipts->connection_epoch == generation.epoch && jz_btree_contains(&receipts->subscriptions, &request->subscription));
            if (stale) {
                jazz_db_node_unlock(self->node);
                jazz_db__row_availability__CurrentRowsResult__drop(&result);
                jz_vec_drop(&retry_rows, sizeof(jazz_types_RowUuid), NULL);
                scalar_reset(state);
                return JZ_OK;
            }
            jz_btree claims;
            jz_btree_clone(&claims, jazz_protocol__protocol__PolicyBindingKey__claims(&receipt->context), str_key_clone, groove__records__Value__clone);
            jazz_node_NodeState *node = NS_FN(scoped_active_session_claims)(owner, receipt->context.identity, claims);
            jazz_node_LocalReadPolicyBinding *scope = NS_FN(local_read_policy_binding)(node, receipt->context.identity);
            if (scope) {
                JZ_VEC(jazz_node_RowAvailabilityOutcome) outcomes = {0};
                u32 n = receipt->rows.len < receipt->outcomes.len ? receipt->rows.len : receipt->outcomes.len;
                for (u32 i = 0; i < n; ++i) {
                    u8 status;
                    if (receipt->outcomes.ptr[i] == JAZZ_CRO_READABLE) status = JAZZ_LRA_READABLE;
                    /* Availability constrains settled input. The normal
                     * pending source remains visible without postponing
                     * this receipt or scheduling edit-specific retries. */
                    else if (receipt->outcomes.ptr[i] == JAZZ_CRO_CURRENT_UNAVAILABLE) status = JAZZ_LRA_CURRENT_UNAVAILABLE;
                    else continue;
                    jazz_node_RowAvailabilityOutcome o = { receipt->rows.ptr[i].physical_table, receipt->rows.ptr[i].row, status };
                    jz_vec_push(&outcomes, &o, sizeof(o));
                }
                if (outcomes.len != 0) {
                    jz_err e = NS_FN(activate_local_availability_authority)(node, scope, receipt->core, receipt->core_epoch);
                    bool changed = false;
                    if (!e.code) {
                        jazz_node_LocalAvailabilityWatermark wm;
                        memcpy(wm.core, receipt->core, 16);
                        wm.core_epoch = receipt->core_epoch;
                        wm.claims_revision = receipt->claims_revision;
                        wm.policy_epoch = receipt->policy_epoch;
                        wm.settled_through = receipt->settled_through;
                        wm.authorization_progress = receipt->authorization_progress;
                        e = NS_FN(apply_verified_local_row_availability)(node, scope, wm, outcomes.ptr, outcomes.len, &changed);
                    }
                    jz_vec_drop(&outcomes, sizeof(jazz_node_RowAvailabilityOutcome), NULL);
                    if (e.code) {
                        NS_FN(end_scoped_claims)(owner);
                        jazz_db_node_unlock(self->node);
                        jazz_db__row_availability__CurrentRowsResult__drop(&result);
                        jz_vec_drop(&retry_rows, sizeof(jazz_types_RowUuid), NULL);
                        return jazz_node__node__api_error__Error__From__from(e);
                    }
                    if (changed) {
                        NS_FN(end_scoped_claims)(owner);
                        jazz_db_node_unlock(self->node);
                        owner = NULL;
                        *self->subscriber_dirty_epoch += 1;
                        self->local_subscription_dirty_generation += 1;
                        NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
                    }
                } else {
                    jz_vec_drop(&outcomes, sizeof(jazz_node_RowAvailabilityOutcome), NULL);
                }
            }
            if (owner) { NS_FN(end_scoped_claims)(owner); jazz_db_node_unlock(self->node); }
        }
        jazz_db__row_availability__CurrentRowsResult__drop(&result);
        if (retry_rows.len == 0) {
            if (state->pending.len == 0) state->retry_delay_ms = 0;
        } else {
            for (u32 i = 0; i < retry_rows.len; ++i) jz_deque_push_back(&state->pending, &retry_rows.ptr[i]);
            state->retry_delay_ms = state->retry_delay_ms == 0 ? 100 : (state->retry_delay_ms * 2 < 2000 ? state->retry_delay_ms * 2 : 2000);
            state->has_retry_at = true;
            state->retry_at = jazz_db_instant_now() + state->retry_delay_ms;
            scheduler_schedule_tick_after(self->scheduler, state->retry_delay_ms);
        }
        jz_vec_drop(&retry_rows, sizeof(jazz_types_RowUuid), NULL);
    }
    if (state->has_retry_at) {
        if (jazz_db_instant_now() < state->retry_at) return JZ_OK;
        state->has_retry_at = false;
    }
    bool same_generation = state->has_generation && state->generation.epoch == generation.epoch
        && jazz_protocol_SubscriptionKey_eq(&state->generation.subscription, &generation.subscription) && state->generation.revision == generation.revision;
    if (!same_generation && state->pending.len == 0) {
        jazz_node_NodeState *owner = jazz_db_node_lock(self->node);
        const jz_str *table = &jazz_model__query__ValidatedQuery__query(request->shape)->table;
        jazz_protocol_AuthorityResultKey key;
        jz_err e = NS_FN(authority_result_key_for_subscription)(owner, request->subscription, &key);
        if (e.code) { jazz_db_node_unlock(self->node); return jazz_node__node__api_error__Error__From__from(e); }
        u64 closure_generation;
        if (!NS_FN(has_settled_authority_result)(owner, &key) || NS_FN(opening_pending_for_authority_result)(owner, &key)
            || NS_FN(publication_deferred_for_authority_result)(owner, &key) || !NS_FN(authority_source_closure_generation)(owner, &key, &closure_generation)) {
            jazz_protocol__protocol__AuthorityResultKey__drop(&key);
            jazz_db_node_unlock(self->node);
            return JZ_OK;
        }
        jz_btree authoritative;
        jz_btree_init(&authoritative, &jazz_db_row_uuid_set_desc);
        NS_FN(scalar_authority_input_rows)(owner, &key, table, &authoritative);
        jazz_protocol__protocol__AuthorityResultKey__drop(&key);
        jz_btree claims;
        const jz_btree *claims_ptr = NULL;
        if (request->has_policy_binding) { jz_btree_clone(&claims, &request->policy_binding.claims, str_key_clone, groove__records__Value__clone); claims_ptr = &claims; }
        jazz_node_NodeState *node = NS_FN(scoped_optional_session_claims)(owner, request->identity, claims_ptr);
        /* Public local-first output is the candidate inventory, even when
         * cached policy proofs no longer admit those retained inputs. */
        JZ_VEC(jazz_types_RowUuid) local = {0};
        if (local_owner) {
            u32 n = local_owner->snapshot.root_count < local_owner->snapshot.rows.len ? local_owner->snapshot.root_count : local_owner->snapshot.rows.len;
            for (u32 i = 0; i < n; ++i) { jazz_types_RowUuid r = jazz_node__node__CurrentRow__row_uuid(&local_owner->snapshot.rows.ptr[i]); jz_vec_push(&local, &r, sizeof(r)); }
        } else {
            /* This is a local candidate inventory, not a serving answer.
             * Re-evaluating cached permission rules here can both omit
             * stale rows needing revalidation and wait on query work that
             * this same owner pass must drive. Core authorizes the later
             * CurrentRows request; discovery uses ordinary client-local reads. */
            jazz_node_CurrentRowVec rows = {0};
            jz_err qe = NS_FN(query_rows_for_client)(node, request->shape, request->binding, JAZZ_DUR_LOCAL, request->identity, &rows);
            if (qe.code) {
                NS_FN(end_scoped_claims)(owner);
                jazz_db_node_unlock(self->node);
                if (claims_ptr) jz_btree_drop(&claims);
                jz_btree_drop(&authoritative);
                return jazz_node__node__api_error__Error__From__from(qe);
            }
            for (u32 i = 0; i < rows.len; ++i) { jazz_types_RowUuid r = jazz_node__node__CurrentRow__row_uuid(&rows.ptr[i]); jz_vec_push(&local, &r, sizeof(r)); }
            jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), jazz_node__node__CurrentRow__drop);
        }
        jz_btree candidates;
        jz_btree_init(&candidates, &jazz_db_row_uuid_set_desc);
        for (u32 i = 0; i < local.len; ++i)
            if (!jz_btree_contains(&authoritative, &local.ptr[i])) jz_btree_insert(&candidates, &local.ptr[i], NULL, NULL);
        jz_vec_drop(&local, sizeof(jazz_types_RowUuid), NULL);
        jazz_node_LocalReadPolicyBinding *scope = NS_FN(local_read_policy_binding)(node, request->identity);
        if (scope) {
            u64 table_id;
            jz_err te = NS_FN(local_availability_table_id)(node, jazz_model__query__ValidatedQuery__schema_version(request->shape), table, &table_id);
            if (!te.code) {
                /* A fresh inclusion must revalidate a hidden row as well;
                 * otherwise the local exclusion would prevent readmission. */
                jazz_types_RowUuid *row; void *unused;
                JZ_BTREE_FOR(&authoritative, row, unused)
                    if (NS_FN(is_local_row_unavailable)(node, scope, table_id, *row)) jz_btree_insert(&candidates, row, NULL, NULL);
            } else jz_err_drop(&te);
        }
        NS_FN(end_scoped_claims)(owner);
        jazz_db_node_unlock(self->node);
        if (claims_ptr) jz_btree_drop(&claims);
        jz_btree_drop(&authoritative);
        jz_deque_drop(&state->pending, NULL);
        jz_deque_init(&state->pending, sizeof(jazz_types_RowUuid));
        jazz_types_RowUuid *row; void *unused;
        JZ_BTREE_FOR(&candidates, row, unused) jz_deque_push_back(&state->pending, row);
        jz_btree_drop(&candidates);
        state->has_generation = true;
        state->generation = generation;
    }
    u32 count = state->pending.len < 64 ? state->pending.len : 64;
    if (count == 0) return JZ_OK;
    JZ_VEC(jazz_types_RowUuid) batch = {0};
    for (u32 i = 0; i < count; ++i) { jazz_types_RowUuid r; jz_deque_pop_front(&state->pending, &r); jz_vec_push(&batch, &r, sizeof(r)); }
    jazz_node_NodeState *node = jazz_db_node_lock(self->node);
    const jz_str *table = &jazz_model__query__ValidatedQuery__query(request->shape)->table;
    CoordinateVec candidates = {0};
    for (u32 i = 0; i < batch.len; ++i) {
        jz_err err = JZ_OK;
        bool ok = accepted_global_winner(node, table, batch.ptr[i], &err);
        if (err.code) {
            jazz_db_node_unlock(self->node);
            jz_vec_drop(&batch, sizeof(jazz_types_RowUuid), NULL);
            jz_vec_drop(&candidates, sizeof(jazz_protocol_CurrentRowCoordinate), jazz_protocol__protocol__CurrentRowCoordinate__drop);
            return err;
        }
        if (!ok) continue;
        jazz_protocol_CurrentRowCoordinate coordinate;
        jz_err ce = NS_FN(current_row_coordinate)(node, table, batch.ptr[i], &coordinate);
        if (!ce.code) jz_vec_push(&candidates, &coordinate, sizeof(coordinate)); else jz_err_drop(&ce);
    }
    jazz_db_node_unlock(self->node);
    jz_vec_drop(&batch, sizeof(jazz_types_RowUuid), NULL);
    if (candidates.len != 0) {
        jazz_db_PolicyBinding pb;
        request_policy_binding_or(request, &pb);
        jazz_protocol_PolicyBindingKey context;
        memset(&context, 0, sizeof(context));
        context.identity = pb.identity;
        jazz_protocol__protocol__CanonicalPolicyClaims__new(&context.canonical_claims, pb.claims);
        state->has_active = true;
        state->active.deadline = jazz_db_instant_now() + 5000;
        memset(&state->active.rows, 0, sizeof(state->active.rows));
        for (u32 i = 0; i < candidates.len; ++i) jz_vec_push(&state->active.rows, &candidates.ptr[i].row, sizeof(jazz_types_RowUuid));
        state->active.future = jazz_db__db__row_availability__Node__request_current_rows(self, &candidates, context);
        scheduler_schedule_tick_after(self->scheduler, 5000);
        NODE_FN(schedule_tick)(self, JAZZ_TICK_IMMEDIATE);
    } else {
        jz_vec_drop(&candidates, sizeof(jazz_protocol_CurrentRowCoordinate), NULL);
        if (state->pending.len != 0) NODE_FN(schedule_tick)(self, JAZZ_TICK_AFTER_CURRENT_TURN);
    }
    return JZ_OK;
}

static bool keep_unreleased_deadline(const void *k, void *v, void *ctx) { (void)v; return !jz_hmap_get((const jz_hmap *)ctx, k); }
static bool keep_nonempty_uploads(const void *k, void *v, void *ctx) {
    (void)k;
    jz_btree *uploads = (jz_btree *)v;
    jz_btree_retain(uploads, keep_unreleased_deadline, ctx);
    return uploads->len != 0;
}

static void release_outbox_uploads(jazz_db_Node *self, jz_hmap released_tx_ids) {
    jz_hmap remaining = released_tx_ids; /* released_tx_ids.clone() moved into `remaining` */
    jz_hmap released;
    jz_hmap_init(&released, &jazz_db_txid_hset_desc);
    jazz_db__db__UploadOutbox__remove_released(self->outbox, &remaining, &released);
    for (u32 i = 0; i < self->connections.len; ++i) {
        jazz_db_PeerConnection *c = jazz_db_peer_try_lock(self->connections.ptr[i]);
        if (!c) jz_panic("connection is already borrowed");
        PC_FN(forget_released_outbox_tx_ids)(c, &released);
        jazz_db_peer_unlock(self->connections.ptr[i]);
    }
    jz_btree_retain(self->large_value_upload_retry_deadlines, keep_unreleased_deadline, &released);
    jz_btree_retain(self->detached_large_value_uploads, keep_nonempty_uploads, &released);
    jz_hmap_drop(&released);
    jz_hmap_drop(&remaining);
}

/* ---------------- subscription refresh helpers ---------------- */

/// Temporarily owns a maintained Groove handle while subscription refresh does
/// asynchronous work. Restoring on drop keeps every `?`/`continue` exit safe
/// without lending the subscription's `RefCell` contents across an await.
typedef struct DetachedSubscriptionRefresh {
    jazz_db_SubscriptionState *state;
    jazz_node_LocalMaintainedViewSubscription *maintained;
    jazz_node_RelationSnapshot snapshot;
    jazz_node_RelationSnapshotIndex snapshot_index;
    u8 snapshot_source;
    bool settled;
    jazz_db_SubscriptionSender *sender;
    jazz_db_LocalCleanupCell *local_subscription_cleanup;
} DetachedSubscriptionRefresh;

static void DetachedSubscriptionRefresh_new(DetachedSubscriptionRefresh *self, jazz_db_SubscriptionState *state) {
    self->state = (jazz_db_SubscriptionState *)jz_rc_clone(state);
    self->maintained = state->maintained_subscription;
    state->maintained_subscription = NULL;
    self->snapshot = state->snapshot;
    memset(&state->snapshot, 0, sizeof(state->snapshot));
    self->snapshot_index = state->snapshot_index;
    memset(&state->snapshot_index, 0, sizeof(state->snapshot_index));
    self->snapshot_source = state->snapshot_source;
    self->settled = state->settled;
    self->sender = jazz_db__db__SubscriptionSender__clone(state->sender);
    self->local_subscription_cleanup = (jazz_db_LocalCleanupCell *)jz_rc_clone(state->local_subscription_cleanup);
}

static void DetachedSubscriptionRefresh_drop(DetachedSubscriptionRefresh *self) {
    jazz_db_SubscriptionState *state_ref = self->state;
    JZ_ASSERT(state_ref->maintained_subscription == NULL, "assertion failed: maintained_subscription.is_none()");
    state_ref->maintained_subscription = self->maintained;
    self->maintained = NULL;
    jazz_node__node__RelationSnapshot__drop(&state_ref->snapshot);
    state_ref->snapshot = self->snapshot;
    memset(&self->snapshot, 0, sizeof(self->snapshot));
    jazz_node__node__RelationSnapshotIndex__drop(&state_ref->snapshot_index);
    state_ref->snapshot_index = self->snapshot_index;
    memset(&self->snapshot_index, 0, sizeof(self->snapshot_index));
    state_ref->snapshot_source = self->snapshot_source;
    state_ref->settled = self->settled;
    jazz_db__db__SubscriptionSender__drop(self->sender);
    jz_rc_release(self->local_subscription_cleanup);
    jz_rc_release(self->state);
}

/// Resolve a public stream's authority lifecycle from its own registered wire
/// usage site. A canonical binding view is not sufficient here: two delegated
/// sessions may legitimately share it while receiving independent resets.
static bool authority_result_key_for_stream(jazz_node_NodeState *node, const jazz_db_UpstreamCoverageHandleVec *handles, jazz_protocol_BindingViewKey binding_view, jazz_protocol_AuthorityResultKey *out) {
    bool has = false;
    for (u32 i = 0; i < handles->len; ++i) {
        jazz_protocol_AuthorityResultKey key;
        jz_err e = NS_FN(authority_result_key_for_subscription)(node, handles->ptr[i].subscription, &key);
        if (e.code) { jz_err_drop(&e); continue; }
        if (memcmp(&key.binding_view, &binding_view, sizeof(binding_view)) != 0) { jazz_protocol__protocol__AuthorityResultKey__drop(&key); continue; }
        if (!has) { *out = key; has = true; continue; }
        bool same = jazz_protocol__protocol__AuthorityResultKey__eq(&key, out);
        jazz_protocol__protocol__AuthorityResultKey__drop(&key);
        if (!same) { jazz_protocol__protocol__AuthorityResultKey__drop(out); return false; }
    }
    return has;
}

static bool subscription_needs_targeted_refresh(const jazz_db_SubscriptionState *state, const jz_hmap *changed_logical_tables) {
    if (!state->maintained_subscription) return true;
    return jazz_node__node__LocalMaintainedViewSubscription__uses_logical_tables(state->maintained_subscription, changed_logical_tables);
}

/// The answer of [`check_deletion_reconciliation`] when it needs no node
/// work: a stream that is not reconciling, has finished, is waiting on a
/// probe, or has no upstream answer to compare against. Checked before the
/// (large) async check is built, since every refresh of every live stream
/// asks and almost all of them are in one of these states.
/// Returns 0 = Some(false), 1 = Some(true), 2 = None.
static int deletion_reconciliation_known(const jazz_db_SubscriptionState *state) {
    switch (state->deletion_reconciliation.tag) {
    case JAZZ_DR_DISABLED: case JAZZ_DR_DONE: return 0;
    case JAZZ_DR_PENDING: case JAZZ_DR_PROBING: return 1;
    default: return state->upstream_subscription_handles.len == 0 ? 0 : 2;
    }
}

static void restore_reconciliation(jazz_db_SubscriptionState *state, jazz_db_DeletionReconciliation r) {
    jazz_db__db__DeletionReconciliation__drop(&state->deletion_reconciliation);
    state->deletion_reconciliation = r;
}

/// Compare a fresh strict stream's first settled authority answer with the
/// rows this client holds live for the same query, and return whether the
/// stream must withhold publication for a probe of the difference.
///
/// Local candidates come from an ordinary Local-tier maintained graph of the
/// same query, never a blocking one-shot evaluation: this runs inside the
/// sync turn that must itself send any large-value chunk requests. A cold
/// discovery graph withholds settlement and is drained on later turns. Only
/// rows whose current local winner is an accepted Global transaction are
/// candidates: pending local writes are legitimately absent from the
/// authority's answer. The comparison runs once per stream; agreeing views
/// with a warm discovery graph settle in this same refresh, as before.
///
/// Discovery waits only for this stream's own receiver graph, never for a
/// globally idle query runtime: other streams' continuous work must not
/// starve it (INV-SYNC-48). While other work is pending the discovery graph
/// is opened without driving the runtime, and drained on later turns.
static jz_err check_deletion_reconciliation(jazz_db_NodeMutex *node, jazz_db_SubscriptionState *state,
                                            const jazz_node_LocalMaintainedViewSubscription *stream_graph,
                                            const jz_waker *progress_waker, bool *out) {
    int known = deletion_reconciliation_known(state);
    if (known != 2) { *out = known == 1; return JZ_OK; }
    if (state->upstream_subscription_handles.len == 0) jz_panic("checked by deletion_reconciliation_known");
    jazz_protocol_SubscriptionKey subscription = state->upstream_subscription_handles.ptr[0].subscription;
    jazz_db_DeletionReconciliation previous = state->deletion_reconciliation;
    memset(&state->deletion_reconciliation, 0, sizeof(previous));
    state->deletion_reconciliation.tag = JAZZ_DR_UNCHECKED;
    jazz_model_ValidatedQuery *shape = jazz_model__query__ValidatedQuery__clone(state->shape);
    jazz_model_Binding *binding = jazz_model__query__Binding__clone(state->binding);
    jazz_types_AuthorSubject author = state->author;
    bool has_request_claims = state->has_request_identity_claims;
    jazz_db_PolicyBinding request_claims;
    if (has_request_claims) jazz_db__db__PolicyBinding__clone(&request_claims, &state->request_identity_claims);

    jazz_node_NodeState *owner = jazz_db_node_lock(node);
    u64 runtime_token = NS_FN(groove_runtime_token)(owner);
    if (previous.tag == JAZZ_DR_DISCOVERING && previous.runtime_token != runtime_token) {
        /* A runtime rebuild retired the old discovery graph with its IDs. */
        if (previous.maintained) jazz_node__node__LocalMaintainedViewSubscription__drop(previous.maintained);
        previous.maintained = NULL;
        previous.tag = JAZZ_DR_DEFERRED;
    }
    jz_err err = JZ_OK;
    jazz_protocol_AuthorityResultKey key;
    bool settled = false;
    {
        jz_err e = NS_FN(authority_result_key_for_subscription)(owner, subscription, &key);
        if (!e.code) {
            u64 g;
            settled = NS_FN(has_settled_authority_result)(owner, &key) && !NS_FN(opening_pending_for_authority_result)(owner, &key)
                && !NS_FN(publication_deferred_for_authority_result)(owner, &key) && NS_FN(authority_source_closure_generation)(owner, &key, &g);
            if (!settled) jazz_protocol__protocol__AuthorityResultKey__drop(&key);
        } else jz_err_drop(&e);
    }
    if (!settled) {
        /* Nothing can claim settlement yet; compare once an answer settles. */
        jazz_db_node_unlock(node);
        bool withheld = previous.tag != JAZZ_DR_UNCHECKED;
        restore_reconciliation(state, previous);
        *out = withheld;
        goto cleanup;
    }
    const jz_str *table = &jazz_model__query__ValidatedQuery__query(shape)->table;
    jz_btree authoritative;
    jz_btree_init(&authoritative, &jazz_db_row_uuid_set_desc);
    NS_FN(scalar_authority_input_rows)(owner, &key, table, &authoritative);
    jazz_db_Instant since;
    jazz_db_DeletionDiscoverySpec *spec;
    jazz_node_LocalMaintainedViewSubscription *maintained = NULL;
    if (previous.tag == JAZZ_DR_DISCOVERING) {
        maintained = previous.maintained; since = previous.since; spec = previous.spec;
    } else if (previous.tag == JAZZ_DR_DEFERRED) {
        since = previous.since; spec = previous.spec;
    } else {
        jazz_db__db__DeletionReconciliation__drop(&previous);
        jazz_db_PolicyBinding pb;
        if (has_request_claims) jazz_db__db__PolicyBinding__clone(&pb, &request_claims);
        else { pb.identity = author; NS_FN(session_claims_for)(owner, author, &pb.claims); }
        since = jazz_db_instant_now();
        spec = JZ_NEW(jazz_db_DeletionDiscoverySpec);
        spec->shape = jazz_model__query__ValidatedQuery__clone(shape);
        spec->binding = jazz_model__query__Binding__clone(binding);
        spec->author = author;
        spec->context.identity = pb.identity;
        jz_btree canon;
        jz_btree_clone(&canon, &pb.claims, str_key_clone, groove__records__Value__clone);
        jazz_protocol__protocol__CanonicalPolicyClaims__new(&spec->context.canonical_claims, canon);
        spec->claims = pb.claims;
        jz_btree_init(&spec->authoritative, &jazz_db_row_uuid_set_desc);
    }
    /* The freshest settled answer decides which held rows are omitted. */
    jz_btree_drop(&spec->authoritative);
    spec->authoritative = authoritative;
    DiscoveryStep step;
    memset(&step, 0, sizeof(step));
    if (maintained) {
        err = drain_deletion_discovery(owner, maintained, progress_waker, &step);
    } else {
        /* The stream's own receiver graph must have installed and fully
         * evaluated this closure (large-value chunks included) before a
         * discovery graph reads the same rows from local storage. */
        u64 generation;
        bool stream_graph_ready = stream_graph
            && jazz_node__node__LocalMaintainedViewSubscription__has_covered_input_sources(stream_graph)
            && NS_FN(authority_source_closure_generation)(owner, &key, &generation)
            && jazz_node__node__LocalMaintainedViewSubscription__has_installed_covered_closure(stream_graph, &key, generation)
            && !NS_FN(subscription_has_pending_query_runtime)(owner, jazz_node__node__LocalMaintainedViewSubscription__subscription_id(stream_graph));
        if (!stream_graph_ready) {
            jazz_db_node_unlock(node);
            jazz_db_DeletionReconciliation r;
            memset(&r, 0, sizeof(r));
            r.tag = JAZZ_DR_DEFERRED; r.since = since; r.spec = spec;
            restore_reconciliation(state, r);
            jazz_protocol__protocol__AuthorityResultKey__drop(&key);
            *out = true;
            goto cleanup;
        }
        /* Finish a cold opening in this turn only when nothing else is
         * queued (so agreeing views need not wait for another turn);
         * otherwise leave it to drain on later turns rather than queue
         * behind work, such as chunk hydration, only this turn advances. */
        bool drive = !NS_FN(has_pending_query_runtime)(owner);
        err = open_deletion_discovery(owner, spec, progress_waker, drive, &step);
    }
    jazz_protocol__protocol__AuthorityResultKey__drop(&key);
    if (err.code) {
        jazz_db_node_unlock(node);
        jazz_db__db__DeletionDiscoverySpec__drop(spec);
        jz_free(spec);
        goto cleanup;
    }
    if (step.cold) {
        jazz_db_node_unlock(node);
        jazz_db_DeletionReconciliation r;
        memset(&r, 0, sizeof(r));
        r.tag = JAZZ_DR_DISCOVERING; r.maintained = step.maintained; r.runtime_token = runtime_token; r.since = since; r.spec = spec;
        restore_reconciliation(state, r);
        *out = true;
        goto cleanup;
    }
    {
        CoordinateVec rows;
        err = deletion_candidates(owner, &step.snapshot, spec, &rows);
        jazz_node__node__RelationSnapshot__drop(&step.snapshot);
        jazz_db_node_unlock(node);
        if (err.code) { jazz_db__db__DeletionDiscoverySpec__drop(spec); jz_free(spec); goto cleanup; }
        jazz_db_DeletionReconciliation r;
        memset(&r, 0, sizeof(r));
        if (rows.len == 0) {
            jz_vec_drop(&rows, sizeof(jazz_protocol_CurrentRowCoordinate), NULL);
            r.tag = JAZZ_DR_DONE;
            restore_reconciliation(state, r);
            *out = false;
        } else {
            r.tag = JAZZ_DR_PENDING;
            r.rows.ptr = rows.ptr; r.rows.len = rows.len; r.rows.cap = rows.cap;
            r.context = spec->context;
            memset(&spec->context, 0, sizeof(spec->context));
            r.since = since;
            restore_reconciliation(state, r);
            *out = true;
        }
        jazz_db__db__DeletionDiscoverySpec__drop(spec);
        jz_free(spec);
    }
cleanup:
    jazz_model__query__ValidatedQuery__drop(shape);
    jazz_model__query__Binding__drop(binding);
    if (has_request_claims) jazz_db__db__PolicyBinding__drop(&request_claims);
    return err;
}

/* ---------------- refresh_subscriptions_in_with_tables ---------------- */

typedef struct RefreshEnv {
    jazz_db_NodeMutex *node;
    jazz_db_ActiveAuthorityViewReceipts *receipts;
    const jz_waker *waker;
    jz_btree pending_resets;    /* AuthorityResultKey -> u64 */
    jz_btree reconciled_resets; /* AuthorityResultKey -> u64 */
    jazz_db_SubscriptionList retained;
    u32 changed;
} RefreshEnv;

typedef struct RefreshSub {
    jazz_db_SubscriptionState *state;
    jazz_model_DurabilityTier read_tier;
    bool pending_overlay;
    bool has_remote_read_tier;
    jazz_model_DurabilityTier remote_read_tier;
    bool requires_authority_receipt;
    jazz_protocol_ReadViewSpec *read_view;
    u8 previous_source;
    bool previous_settled;
    jazz_types_AuthorSubject author;
    u8 authorization_mode;
    bool terminal_rows;
    jazz_db_UpstreamCoverageHandleVec handles;
    bool awaiting_initial_owner_result;
    bool deletion_withheld;
    bool has_request_claims;
    jz_btree request_claims;
    u64 groove_runtime_token;
} RefreshSub;

enum { R_FALLTHROUGH = 0, R_RETAIN = 1, R_DROP = 2, R_PUSHED = 3, R_ERR = 4 };

static void refresh_retain(RefreshEnv *env, jazz_db_SubscriptionState *state) {
    jazz_db_SubscriptionState *w = jazz_db_rc_downgrade(state);
    jz_vec_push(&env->retained, &w, sizeof(w));
}

static void covered_trace(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void covered_trace(const char *fmt, ...) {
    (void)fmt;
    /* eprintln!("JAZZ_COVERED_INPUT_TRACE ...") when the debug env flag is set */
}
#define COVERED_TRACE(...) do { if (jazz_types__debug_env__covered_input_trace()) jz_trace_event(0, "JAZZ_COVERED_INPUT_TRACE", __VA_ARGS__); } while (0)

static jazz_protocol_BindingViewKey binding_view_for(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view) {
    jazz_protocol_RegisterShapeOptions opts;
    jazz_protocol__protocol__RegisterShapeOptions__default(&opts);
    opts.tier = tier;
    jazz_protocol__protocol__ReadViewSpec__drop(opts.read_view);
    opts.read_view = jazz_protocol__protocol__ReadViewSpec__clone(read_view);
    jazz_protocol_BindingViewKey key;
    memset(&key, 0, sizeof(key));
    key.shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
    key.binding_id = jazz_model__query__Binding__binding_id(binding);
    key.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
    return key;
}

static bool stream_key(RefreshEnv *env, const RefreshSub *sub, jazz_protocol_BindingViewKey view, jazz_protocol_AuthorityResultKey *out) {
    jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(env->node);
    bool has = authority_result_key_for_stream(n, &sub->handles, view, out);
    jazz_db_node_unlock(env->node);
    return has;
}

static bool refresh_is_settled(RefreshEnv *env, const RefreshSub *sub, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
                               jazz_model_DurabilityTier settled_tier, bool has_settled_key, const jazz_protocol_AuthorityResultKey *settled_key) {
    if (sub->deletion_withheld) return false;
    jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(env->node);
    bool settled = jazz_db__db__subscription_is_settled(n, env->receipts, shape, binding, settled_tier, sub->read_view,
                                                        sub->requires_authority_receipt, has_settled_key ? settled_key : NULL);
    jazz_db_node_unlock(env->node);
    return settled;
}

/* sender.materialized(&node.borrow(), shape, &event) then sender.publish(..) */
static jz_err refresh_publish(RefreshEnv *env, jazz_db_SubscriptionSender *sender, const jazz_model_ValidatedQuery *shape, jazz_db_SubscriptionEvent *event,
                              jazz_db_PublicationCheckpoint before, const jazz_node_RelationSnapshot *snapshot, const jazz_node_RelationSnapshotIndex *index, bool *delivered_out) {
    void *materialized = NULL;
    jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(env->node);
    jz_err e = jazz_db__db__SubscriptionSender__materialized(sender, n, shape, event, &materialized);
    jazz_db_node_unlock(env->node);
    if (e.code) { jazz_db__db__SubscriptionEvent__drop(event); return e; }
    bool delivered = false;
    JZ_TRY(jazz_db__db__SubscriptionSender__publish(sender, event, before, snapshot, index, materialized, &delivered));
    if (delivered) env->changed += 1;
    if (delivered_out) *delivered_out = delivered;
    return JZ_OK;
}

static bool pending_reset_for(RefreshEnv *env, bool has_delivered, const jazz_protocol_AuthorityResultKey *delivered,
                              bool has_settled, const jazz_protocol_AuthorityResultKey *settled, jazz_protocol_AuthorityResultKey *out) {
    if (has_delivered && jz_btree_contains(&env->pending_resets, delivered)) { jazz_protocol__protocol__AuthorityResultKey__clone(out, delivered); return true; }
    if (has_settled && jz_btree_contains(&env->pending_resets, settled)) { jazz_protocol__protocol__AuthorityResultKey__clone(out, settled); return true; }
    return false;
}

static void note_reconciled_reset(RefreshEnv *env, const jazz_protocol_AuthorityResultKey *key) {
    u64 *generation = (u64 *)jz_btree_get(&env->pending_resets, key);
    if (!generation) return;
    jazz_protocol_AuthorityResultKey k;
    jazz_protocol__protocol__AuthorityResultKey__clone(&k, key);
    u64 g = *generation;
    jazz_protocol_AuthorityResultKey old;
    if (jz_btree_insert(&env->reconciled_resets, &k, &g, NULL)) { (void)old; }
}

/* Set reset/added/updated/removed of a Delta event for a full reset publication. */
static jz_err set_reset_outputs(jazz_db_SubscriptionEvent *event, const jazz_node_RelationSnapshot *snapshot, const jazz_db_OutputOccurrenceVec *roots,
                                const jazz_node_RelationSnapshot *previous, const jazz_node_RelationSnapshotIndex *previous_index, bool set_reset, bool settled_publishable, bool settled) {
    jazz_db_DeltaFields d;
    if (!jazz_db__db__SubscriptionEvent__as_delta(event, &d)) return JZ_OK;
    if (set_reset) *d.reset = true;
    void *added = NULL;
    JZ_TRY(jazz_db__db__subscription_outputs_with_occurrence_sidecar(snapshot, roots, &added));
    jazz_db__db__OutputVec__assign(d.added, added);
    jazz_db__db__OutputVec__clear(d.updated);
    jazz_db__db__OutputVec__assign(d.removed, jazz_db__db__reset_removed_roots(previous, previous_index, roots));
    if (settled_publishable) *d.publishable = settled || !jazz_db__db__OutputVec__is_empty(d.added) || !jazz_db__db__OutputVec__is_empty(d.removed);
    return JZ_OK;
}

static jz_err decoded_terminal_records_or_default(const jazz_node_LocalMaintainedViewSubscription *m, jz_btree *out) {
    if (!m) { memset(out, 0, sizeof(*out)); return JZ_OK; }
    jz_err e = jazz_node__node__LocalMaintainedViewSubscription__decoded_terminal_records(m, out);
    if (e.code) return jazz_node__node__api_error__Error__From__from(e);
    return JZ_OK;
}

/* The runtime-token mismatch branch: reopen the maintained graph and publish
 * a reset from the previously published facade. */
static int refresh_reopen_runtime(RefreshEnv *env, RefreshSub *sub, jz_err *err) {
    jazz_db_SubscriptionState *state = sub->state;
    jazz_db_NodeMutex *node = env->node;
    COVERED_TRACE("stage=reopen_runtime stale=%llu current=%llu", (unsigned long long)state->groove_runtime_token, (unsigned long long)sub->groove_runtime_token);
    jazz_model_ValidatedQuery *shape = jazz_model__query__ValidatedQuery__clone(state->shape);
    jazz_model_Binding *binding = jazz_model__query__Binding__clone(state->binding);
    bool has_stale = state->maintained_subscription != NULL;
    groove_SubscriptionId stale_subscription_id = {0};
    if (has_stale) stale_subscription_id = jazz_node__node__LocalMaintainedViewSubscription__subscription_id(state->maintained_subscription);
    /* Jazz plan invalidation can leave the Groove runtime alive.
     * Runtime-scoped IDs also make this safe after a full rebuild.
     * Retire the old maintained handle before installing its replacement so
     * two descriptor generations cannot consume the next physical delta. */
    if (has_stale) {
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        NS_FN(unsubscribe_groove_subscription)(n, stale_subscription_id);
        jazz_db_node_unlock(node);
    }
    int code = R_ERR;
    jazz_model_ValidatedQuery *link_shape = NULL;
    jazz_model_Binding *link_binding = NULL;
    {
        jazz_node_NodeState *owner = jazz_db_node_lock(node);
        jazz_node_NodeState *scoped = NS_FN(scoped_optional_session_claims)(owner, sub->author, sub->has_request_claims ? &sub->request_claims : NULL);
        /* Reopening installs its own maintained graph, just like the
         * initial opener; no unused AppRows graph needs to be retained. */
        jz_err e = NS_FN(query_binding_for_link)(scoped, shape, binding, &link_shape, &link_binding);
        NS_FN(end_scoped_claims)(owner);
        jazz_db_node_unlock(node);
        jazz_model__query__ValidatedQuery__drop(shape);
        jazz_model__query__Binding__drop(binding);
        if (e.code) { *err = jazz_node__node__api_error__Error__From__from(e); return R_ERR; }
    }
    shape = link_shape;
    binding = link_binding;
    jazz_node_RelationSnapshot previous_snapshot;
    jazz_node_RelationSnapshotIndex previous_snapshot_index;
    *err = jazz_db__db__materialized_subscription_snapshot(&state->snapshot, &state->snapshot_index, &previous_snapshot);
    if (err->code) goto out_shape;
    jazz_node__node__RelationSnapshotIndex__clone(&previous_snapshot_index, &state->snapshot_index);
    jazz_node_LocalMaintainedViewSubscription *maintained = NULL;
    jazz_node_RelationSnapshot snapshot;
    memset(&snapshot, 0, sizeof(snapshot));
    {
        jazz_node_NodeState *owner = jazz_db_node_lock(node);
        jazz_node_NodeState *scoped = NS_FN(scoped_optional_session_claims)(owner, sub->author, sub->has_request_claims ? &sub->request_claims : NULL);
        jz_err e = NS_FN(open_maintained_view_subscription_in_authorization_mode_with_waker)(scoped, shape, binding, sub->author, sub->read_tier,
            sub->read_view, NULL, sub->authorization_mode, sub->pending_overlay, env->waker, &maintained, &snapshot);
        NS_FN(end_scoped_claims)(owner);
        jazz_db_node_unlock(node);
        if (e.code) { *err = jazz_node__node__api_error__Error__From__from(e); goto out_prev; }
    }
    bool replacement_is_cold = !jazz_node__node__LocalMaintainedViewSubscription__initial_snapshot_received(maintained);
    if (*state->closed) {
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        NS_FN(unsubscribe_groove_subscription)(n, jazz_node__node__LocalMaintainedViewSubscription__subscription_id(maintained));
        jazz_db_node_unlock(node);
        jazz_node__node__LocalMaintainedViewSubscription__drop(maintained);
        jazz_node__node__RelationSnapshot__drop(&snapshot);
        code = R_DROP;
        goto out_prev;
    }
    jazz_protocol_BindingViewKey delivered_binding_view = binding_view_for(shape, binding, sub->read_tier, sub->read_view);
    /* From this point, cleanup must target the replacement runtime
     * subscription even if a later fallible refresh step aborts. */
    {
        groove_SubscriptionId subscription_id = jazz_node__node__LocalMaintainedViewSubscription__subscription_id(maintained);
        if (state->maintained_subscription) jazz_node__node__LocalMaintainedViewSubscription__drop(state->maintained_subscription);
        state->maintained_subscription = maintained;
        state->local_subscription_cleanup->has = true;
        state->local_subscription_cleanup->token = sub->groove_runtime_token;
        state->local_subscription_cleanup->id = subscription_id;
        state->pending_initial_local_snapshot = replacement_is_cold;
        state->pending_initial_owner_result = false;
        /* Own the replacement before yielding its cold initial batch;
         * otherwise the next owner turn would retire and reopen it. */
        if (replacement_is_cold) state->groove_runtime_token = sub->groove_runtime_token;
    }
    if (replacement_is_cold) {
        /* Opening a new maintained graph is intentionally nonblocking:
         * an empty cold materialization is pending local IVM work, not
         * a replacement result. The first terminal batch below emits
         * one reset from the previously published facade. */
        jazz_node__node__RelationSnapshot__drop(&snapshot);
        code = R_RETAIN;
        goto out_prev;
    }
    jazz_node_RelationSnapshotIndex snapshot_index;
    {
        if (!state->maintained_subscription) jz_panic("replacement maintained subscription installed");
        *err = jazz_db__db__relation_snapshot_index_with_root_occurrences(&snapshot, jazz_node__node__LocalMaintainedViewSubscription__root_occurrence_ids(state->maintained_subscription), &snapshot_index);
        if (err->code) { jazz_node__node__RelationSnapshot__drop(&snapshot); goto out_prev; }
        jz_btree_drop(&snapshot_index.terminal_records);
        *err = decoded_terminal_records_or_default(state->maintained_subscription, &snapshot_index.terminal_records);
        if (err->code) goto out_snap;
    }
    jazz_model_DurabilityTier settled_tier = sub->has_remote_read_tier ? sub->remote_read_tier : sub->read_tier;
    jazz_protocol_BindingViewKey settled_binding_view = binding_view_for(shape, binding, settled_tier, sub->read_view);
    jazz_protocol_AuthorityResultKey delivered_key, settled_key, reset_key;
    bool has_delivered = stream_key(env, sub, delivered_binding_view, &delivered_key);
    bool has_settled = stream_key(env, sub, settled_binding_view, &settled_key);
    bool has_reset = pending_reset_for(env, has_delivered, &delivered_key, has_settled, &settled_key, &reset_key);
    jz_btree local_overlay_row_keys;
    jz_btree_init(&local_overlay_row_keys, &jazz_db_row_uuid_set_desc);
    if (has_reset) {
        /* A client receiver only ever advances from the exact
         * CoveredInput closure into its own maintained terminal. A
         * receipt with no compiler-owned source registry remains
         * pending; authority result members and payloads are not an
         * alternate result representation. */
        jazz_node_LocalMaintainedViewSubscription *m = state->maintained_subscription;
        if (!m) jz_panic("replacement maintained subscription installed");
        state->maintained_subscription = NULL;
        jazz_node_NodeState *node_ref = jazz_db_node_lock(node);
        /* A stream may be cancelled while this refresh waits for the
         * node owner. Its queued finalizer can retire the exact local
         * subscription before this drain acquires that owner. */
        if (*state->closed) {
            jazz_db_node_unlock(node);
            state->maintained_subscription = m;
            code = R_DROP;
            goto out_keys;
        }
        bool has_update = false, suppressed = false;
        jazz_node_LocalMaintainedUpdate update;
        memset(&update, 0, sizeof(update));
        jz_err drained = NS_FN(drain_local_maintained_view_subscription_preserving_rows_with_waker)(node_ref, m, &reset_key, &local_overlay_row_keys, env->waker, &has_update, &update, &suppressed);
        jazz_db_node_unlock(node);
        state->maintained_subscription = m;
        if (drained.code) { *err = jazz_node__node__api_error__Error__From__from(drained); goto out_keys; }
        if (has_update) {
            const void *terminal_layout = state->maintained_subscription ? jazz_node__node__LocalMaintainedViewSubscription__terminal_root_layout(state->maintained_subscription) : NULL;
            jazz_db_SubscriptionEvent *ignored = NULL;
            const jz_str *table = &jazz_model__query__ValidatedQuery__query(shape)->table;
            *err = jazz_db__db__apply_maintained_update_to_snapshot(&snapshot, &snapshot_index, update, jz_str_slice(table), sub->read_tier, sub->previous_settled, terminal_layout, &ignored);
            if (err->code) goto out_keys;
            if (ignored) jazz_db__db__SubscriptionEvent__drop(ignored);
            *err = jazz_db__db__materialize_subscription_terminal_records(&snapshot, &snapshot_index);
            if (err->code) goto out_keys;
            note_reconciled_reset(env, &reset_key);
        }
    }
    {
        /* Preserve the graph's complete tuple identities and their current
         * positions, including any terminal edits drained during this reset.
         * Public root UUIDs alone cannot distinguish flat-join occurrences.
         * Anonymous (collapsed row-derived) positions carry no identity. */
        jazz_db_OutputOccurrenceVec root_occurrence_ids;
        jazz_node__node__RelationSnapshotIndex__flatten_roots(&snapshot_index, &root_occurrence_ids);
        bool settled = refresh_is_settled(env, sub, shape, binding, settled_tier, has_settled, &settled_key);
        jazz_db_SubscriptionEvent *event = jazz_db__db__subscription_delta_event_with_reset(sub->read_tier, settled, &previous_snapshot, &snapshot, false, sub->terminal_rows);
        *err = set_reset_outputs(event, &snapshot, &root_occurrence_ids, &previous_snapshot, &previous_snapshot_index, true, true, settled);
        if (err->code) { jazz_db__db__SubscriptionEvent__drop(event); jz_vec_drop(&root_occurrence_ids, 1, NULL); goto out_keys; }
        jazz_db_PublicationCheckpoint publication_before;
        *err = jazz_db__db__SubscriptionSender__checkpoint(state->sender, sub->read_tier, settled, &state->snapshot, &state->snapshot_index, &publication_before);
        if (err->code) { jazz_db__db__SubscriptionEvent__drop(event); jz_vec_drop(&root_occurrence_ids, 1, NULL); goto out_keys; }
        state->groove_runtime_token = sub->groove_runtime_token;
        jazz_node__node__RelationSnapshot__drop(&state->snapshot);
        jazz_db__db__relation_snapshot_with_delta_slack(&state->snapshot, &snapshot);
        jazz_node__node__RelationSnapshotIndex__drop(&state->snapshot_index);
        jazz_node__node__RelationSnapshotIndex__with_root_occurrences(&state->snapshot_index, root_occurrence_ids);
        jz_btree_drop(&state->snapshot_index.terminal_records);
        *err = decoded_terminal_records_or_default(state->maintained_subscription, &state->snapshot_index.terminal_records);
        if (err->code) { jazz_db__db__SubscriptionEvent__drop(event); goto out_keys; }
        state->snapshot_source = JAZZ_SNAP_LOCAL_MAINTAINED;
        state->settled = settled;
        /* A strict remote opening is intentionally absent until its
         * exact covered closure has reached the receiver-local terminal.
         * Do not enqueue that provisional frame merely for the stream
         * facade to discard later: raw/poll consumers must not observe a
         * stale empty opening before the authoritative reset. */
        *err = refresh_publish(env, state->sender, shape, event, publication_before, &state->snapshot, &state->snapshot_index, NULL);
        if (err->code) goto out_keys;
        refresh_retain(env, state);
        code = R_PUSHED;
    }
out_keys:
    jz_btree_drop(&local_overlay_row_keys);
    if (has_reset) jazz_protocol__protocol__AuthorityResultKey__drop(&reset_key);
    if (has_delivered) jazz_protocol__protocol__AuthorityResultKey__drop(&delivered_key);
    if (has_settled) jazz_protocol__protocol__AuthorityResultKey__drop(&settled_key);
out_snap:
    jazz_node__node__RelationSnapshot__drop(&snapshot);
    jazz_node__node__RelationSnapshotIndex__drop(&snapshot_index);
out_prev:
    jazz_node__node__RelationSnapshot__drop(&previous_snapshot);
    jazz_node__node__RelationSnapshotIndex__drop(&previous_snapshot_index);
out_shape:
    jazz_model__query__ValidatedQuery__drop(shape);
    jazz_model__query__Binding__drop(binding);
    return code;
}

/* Outputs of the detached refresh block that fall through to the common
 * compare-and-publish tail. */
typedef struct DetachedOut {
    jazz_node_RelationSnapshot snapshot;
    u8 snapshot_source;
    bool settled;
    jazz_model_DurabilityTier snapshot_tier;
    bool force_reset_event;
} DetachedOut;

static bool refresh_settled_now(RefreshEnv *env, const RefreshSub *sub, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
                                jazz_model_DurabilityTier settled_tier, bool has_settled, const jazz_protocol_AuthorityResultKey *settled_key) {
    return refresh_is_settled(env, sub, shape, binding, settled_tier, has_settled, settled_key);
}

/* The body of `let (..) = { let mut refresh = DetachedSubscriptionRefresh::new(&state); .. }`.
 * The caller owns `refresh` and restores it on every exit. */
static int refresh_detached(RefreshEnv *env, RefreshSub *sub, DetachedSubscriptionRefresh *refresh, bool initial_local_snapshot_pending,
                            DetachedOut *out, jz_err *err) {
    jazz_db_SubscriptionState *state = sub->state;
    jazz_db_NodeMutex *node = env->node;
    jazz_model_ValidatedQuery *shape = jazz_model__query__ValidatedQuery__clone(state->shape);
    jazz_model_Binding *binding = jazz_model__query__Binding__clone(state->binding);
    int code = R_ERR;
    bool has_maintained_subscription = refresh->maintained != NULL;
    jazz_model_DurabilityTier settled_tier = sub->has_remote_read_tier ? sub->remote_read_tier : sub->read_tier;
    jazz_protocol_BindingViewKey settled_binding_view = binding_view_for(shape, binding, settled_tier, sub->read_view);
    jazz_protocol_BindingViewKey delivered_binding_view = binding_view_for(shape, binding, sub->read_tier, sub->read_view);
    jazz_protocol_AuthorityResultKey delivered_key, settled_key, reset_key;
    bool has_delivered = stream_key(env, sub, delivered_binding_view, &delivered_key);
    bool has_settled = stream_key(env, sub, settled_binding_view, &settled_key);
    bool has_remote_settled_tier = false;
    if (sub->has_remote_read_tier && has_settled) {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        has_remote_settled_tier = NS_FN(has_settled_authority_result)(n, &settled_key);
        jazz_db_node_unlock(node);
    }
    bool has_reset = pending_reset_for(env, has_delivered, &delivered_key, has_settled, &settled_key, &reset_key);
    /* A propagation receipt is not a replacement input frontier for
     * local-first. Observation tier, not the caller's authorization host,
     * decides whether the remote closure is this stream's input frontier. */
    bool authority_scoped = sub->read_tier >= JAZZ_DUR_GLOBAL;
    bool authoritative_reset_pending = authority_scoped && has_reset;
    /* Optimistic local writes are represented as eligible local input
     * records in the receiver graph. */
    jz_btree local_overlay_row_keys;
    jz_btree_init(&local_overlay_row_keys, &jazz_db_row_uuid_set_desc);
    if (has_settled) {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        bool deferred = NS_FN(publication_deferred_for_authority_result)(n, &settled_key);
        jazz_db_node_unlock(node);
        if (deferred) { code = R_RETAIN; goto out; }
    }
    jazz_model_DurabilityTier snapshot_tier = has_remote_settled_tier ? sub->remote_read_tier : sub->read_tier;
    bool authoritative_reset = authoritative_reset_pending;
    COVERED_TRACE("stage=refresh terminal_rows=%d covered=%d reset_pending=%d authoritative_reset=%d delivered=%d settled=%d",
                  sub->terminal_rows, refresh->maintained && jazz_node__node__LocalMaintainedViewSubscription__has_covered_input_sources(refresh->maintained),
                  authoritative_reset_pending, authoritative_reset, has_delivered, has_settled);
    if (authoritative_reset && !initial_local_snapshot_pending && sub->terminal_rows
        && (!refresh->maintained || !jazz_node__node__LocalMaintainedViewSubscription__has_covered_input_sources(refresh->maintained))) {
        /* A client receiver may only reset after it has installed the
         * exact scoped covered-input closure. */
        if (sub->authorization_mode == JAZZ_QAM_CLIENT_LOCAL && sub->has_remote_read_tier) {
            code = R_RETAIN; goto out;
        }
        if (!refresh->maintained) {
            *err = jz_err_new(JAZZ_EC_PROTOCOL, "structured subscription lost its Groove terminal");
            goto out;
        }
        groove_SubscriptionId stale_subscription_id = jazz_node__node__LocalMaintainedViewSubscription__subscription_id(refresh->maintained);
        /* Re-open the Groove terminal at an authoritative boundary so the
         * reset is a fresh complete value and subsequent FIFO patches are
         * relative to exactly that value. */
        jazz_node_LocalMaintainedViewSubscription *replacement = NULL;
        jazz_node_RelationSnapshot snapshot;
        memset(&snapshot, 0, sizeof(snapshot));
        {
            jazz_node_NodeState *n = jazz_db_node_lock(node);
            jz_err e = NS_FN(open_maintained_view_subscription_in_authorization_mode_with_waker)(n, shape, binding, sub->author, sub->read_tier,
                sub->read_view, NULL, sub->authorization_mode, sub->pending_overlay, env->waker, &replacement, &snapshot);
            jazz_db_node_unlock(node);
            if (e.code) { *err = jazz_node__node__api_error__Error__From__from(e); goto out; }
        }
        groove_SubscriptionId replacement_subscription_id = jazz_node__node__LocalMaintainedViewSubscription__subscription_id(replacement);
        {
            jazz_node_NodeState *n = jazz_db_node_lock(node);
            NS_FN(unsubscribe_groove_subscription)(n, stale_subscription_id);
            jazz_db_node_unlock(node);
        }
        jazz_node__node__LocalMaintainedViewSubscription__drop(refresh->maintained);
        refresh->maintained = replacement;
        refresh->local_subscription_cleanup->has = true;
        refresh->local_subscription_cleanup->token = sub->groove_runtime_token;
        refresh->local_subscription_cleanup->id = replacement_subscription_id;
        if (!jazz_node__node__LocalMaintainedViewSubscription__initial_snapshot_received(refresh->maintained)) {
            state->pending_initial_local_snapshot = true;
            jazz_node__node__RelationSnapshot__drop(&snapshot);
            code = R_RETAIN; goto out;
        }
        out->settled = refresh_settled_now(env, sub, shape, binding, settled_tier, has_settled, &settled_key);
        out->snapshot = snapshot;
        out->snapshot_source = JAZZ_SNAP_LOCAL_MAINTAINED;
        out->snapshot_tier = snapshot_tier;
        out->force_reset_event = true;
        code = R_FALLTHROUGH;
        goto out;
    }
    bool has_update = false, suppressed_authoritative_change = false;
    jazz_node_LocalMaintainedUpdate maintained_update;
    memset(&maintained_update, 0, sizeof(maintained_update));
    if (refresh->maintained) {
        jazz_node_LocalMaintainedViewSubscription *maintained = refresh->maintained;
        jazz_node_NodeState *node_ref = jazz_db_node_lock(node);
        /* Finalization may have run while this refresh waited for the
         * owner. Do not drain the detached receiver after its stream
         * synchronously marked itself closed. */
        if (*state->closed) { jazz_db_node_unlock(node); code = R_DROP; goto out; }
        /* Only a scope-bound source consumes authority inputs. */
        bool has_authoritative_key = sub->authorization_mode == JAZZ_QAM_CLIENT_LOCAL
            && jazz_node__node__LocalMaintainedViewSubscription__has_covered_input_sources(maintained) && has_settled;
        COVERED_TRACE("stage=runtime_drain sources=%d authority=%d", jazz_node__node__LocalMaintainedViewSubscription__has_covered_input_sources(maintained), has_authoritative_key);
        bool covered_authority = has_authoritative_key;
        jz_err e = NS_FN(drain_local_maintained_view_subscription_preserving_rows_with_waker)(node_ref, maintained, has_authoritative_key ? &settled_key : NULL,
            &local_overlay_row_keys, env->waker, &has_update, &maintained_update, &suppressed_authoritative_change);
        if (e.code == 0) {
            /* The receiver's evaluation is still waiting (for example on
             * large-value chunks) and nothing was drained. Publishing now
             * would report an incomplete authority state (#3349). */
            if (!has_update && covered_authority && NS_FN(covered_receiver_evaluation_pending)(node_ref, maintained)) {
                jazz_db_node_unlock(node);
                code = R_RETAIN; goto out;
            }
        } else if (e.code == JAZZ_NODE_ERR_MISSING_TRANSACTION) {
            jz_err_drop(&e);
            NS_FN(record_authoritative_reset_missing_payload_fallback)(node_ref);
            jazz_db_node_unlock(node);
            code = R_RETAIN; goto out;
        } else {
            jazz_db_node_unlock(node);
            *err = jazz_node__node__api_error__Error__From__from(e);
            goto out;
        }
        jazz_db_node_unlock(node);
    }
    if (sub->deletion_withheld) {
        *err = check_deletion_reconciliation(node, state, refresh->maintained, env->waker, &sub->deletion_withheld);
        if (err->code) goto out_update;
    }
    /* A reset claims an authority successor, but it is publishable only
     * after the receiver has installed that exact closure and drained the
     * same local graph. */
    if (authoritative_reset && sub->authorization_mode == JAZZ_QAM_CLIENT_LOCAL && sub->has_remote_read_tier && has_reset) {
        bool closure_installed = false;
        if (refresh->maintained && jazz_node__node__LocalMaintainedViewSubscription__has_covered_input_sources(refresh->maintained)) {
            u64 generation = 0;
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
            bool has_gen = NS_FN(authority_source_closure_generation)(n, &reset_key, &generation);
            jazz_db_node_unlock(node);
            closure_installed = has_gen && jazz_node__node__LocalMaintainedViewSubscription__has_installed_covered_closure(refresh->maintained, &reset_key, generation);
        }
        if (!closure_installed) {
            /* Missing source descriptors and incomplete coverage are both
             * pending protocol state. */
            code = R_RETAIN; goto out_update;
        }
    }
    if (has_reset) note_reconciled_reset(env, &reset_key);
    if (initial_local_snapshot_pending) {
        bool replacement_ready = false;
        if (refresh->maintained && jazz_node__node__LocalMaintainedViewSubscription__initial_snapshot_received(refresh->maintained)) {
            if (!sub->awaiting_initial_owner_result) replacement_ready = true;
            else {
                jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
                replacement_ready = !NS_FN(subscription_has_pending_query_runtime)(n, jazz_node__node__LocalMaintainedViewSubscription__subscription_id(refresh->maintained));
                jazz_db_node_unlock(node);
            }
        }
        if (!replacement_ready) { code = R_RETAIN; goto out_update; }
        jazz_node_RelationSnapshot previous;
        *err = jazz_db__db__materialized_subscription_snapshot(&refresh->snapshot, &refresh->snapshot_index, &previous);
        if (err->code) goto out_update;
        jazz_node_RelationSnapshotIndex previous_snapshot_index;
        jazz_node__node__RelationSnapshotIndex__clone(&previous_snapshot_index, &refresh->snapshot_index);
        jazz_node_LocalMaintainedViewSubscription *maintained = refresh->maintained;
        if (!maintained) jz_panic("pending initial snapshot kept its maintained subscription");
        refresh->maintained = NULL;
        jazz_node_MaterializedSnapshot materialized;
        jz_err e;
        {
            jazz_node_NodeState *n = jazz_db_node_lock(node);
            e = NS_FN(materialize_local_maintained_relation_snapshot_with_occurrences)(n, maintained, &materialized);
            jazz_db_node_unlock(node);
        }
        refresh->maintained = maintained;
        int icode = R_ERR;
        if (e.code) { *err = jazz_node__node__api_error__Error__From__from(e); goto init_prev; }
        jazz_db_PublicationCheckpoint publication_before;
        *err = jazz_db__db__SubscriptionSender__checkpoint(refresh->sender, sub->read_tier, false, &refresh->snapshot, &refresh->snapshot_index, &publication_before);
        if (err->code) goto init_mat;
        jazz_node_RelationSnapshot *replacement = &materialized.snapshot;
        jazz_node__node__RelationSnapshot__drop(&refresh->snapshot);
        jazz_db__db__relation_snapshot_with_delta_slack(&refresh->snapshot, replacement);
        jazz_node__node__RelationSnapshotIndex__drop(&refresh->snapshot_index);
        memset(&refresh->snapshot_index, 0, sizeof(refresh->snapshot_index));
        *err = jazz_db__db__relation_snapshot_index_with_root_occurrences(&refresh->snapshot, &materialized.root_occurrence_ids, &refresh->snapshot_index);
        if (err->code) goto init_mat;
        if (!refresh->maintained) jz_panic("pending initial snapshot restored maintained subscription");
        jz_btree_drop(&refresh->snapshot_index.terminal_records);
        *err = decoded_terminal_records_or_default(refresh->maintained, &refresh->snapshot_index.terminal_records);
        if (err->code) goto init_mat;
        bool settled = refresh_settled_now(env, sub, shape, binding, settled_tier, has_settled, &settled_key);
        refresh->snapshot_source = JAZZ_SNAP_LOCAL_MAINTAINED;
        refresh->settled = settled;
        state->pending_initial_local_snapshot = false;
        state->pending_initial_owner_result = false;
        jazz_db_SubscriptionEvent *event = jazz_db__db__subscription_delta_event_with_reset(snapshot_tier, settled, &previous, replacement, true, sub->terminal_rows);
        *err = set_reset_outputs(event, replacement, &materialized.root_occurrence_ids, &previous, &previous_snapshot_index, false, false, settled);
        if (err->code) { jazz_db__db__SubscriptionEvent__drop(event); goto init_mat; }
        refresh_retain(env, state);
        *err = refresh_publish(env, refresh->sender, shape, event, publication_before, &refresh->snapshot, &refresh->snapshot_index, NULL);
        icode = err->code ? R_ERR : R_PUSHED;
    init_mat:
        jazz_node__node__RelationSnapshot__drop(&materialized.snapshot);
        jz_vec_drop(&materialized.root_occurrence_ids, 1, NULL);
    init_prev:
        jazz_node__node__RelationSnapshot__drop(&previous);
        jazz_node__node__RelationSnapshotIndex__drop(&previous_snapshot_index);
        code = icode;
        goto out_update;
    }
    if (has_update) {
        if (maintained_update.tag == JAZZ_LMU_STRUCTURED) {
            if (maintained_update.terminal_operations.len != 0) {
                bool settled = refresh_settled_now(env, sub, shape, binding, settled_tier, has_settled, &settled_key);
                const void *terminal_layout = refresh->maintained ? jazz_node__node__LocalMaintainedViewSubscription__terminal_root_layout(refresh->maintained) : NULL;
                if (!terminal_layout) {
                    *err = jz_err_new(JAZZ_EC_PROTOCOL, "terminal operation arrived without a prepared root layout");
                    goto out_update;
                }
                u32 terminal_operation_count = maintained_update.terminal_operations.len;
                COVERED_TRACE("stage=terminal_ops count=%u reset=%d", terminal_operation_count, authoritative_reset);
                bool has_previous = false;
                jazz_node_RelationSnapshot previous_snapshot;
                if (authoritative_reset) {
                    *err = jazz_db__db__materialized_subscription_snapshot(&refresh->snapshot, &refresh->snapshot_index, &previous_snapshot);
                    if (err->code) goto out_update;
                    has_previous = true;
                }
                jazz_db_PublicationCheckpoint publication_before;
                *err = jazz_db__db__SubscriptionSender__checkpoint(refresh->sender, sub->read_tier, settled, &refresh->snapshot, &refresh->snapshot_index, &publication_before);
                if (err->code) { if (has_previous) jazz_node__node__RelationSnapshot__drop(&previous_snapshot); goto out_update; }
                jazz_db_SubscriptionEvent *event = NULL;
                *err = jazz_db__db__apply_terminal_operations_to_subscription_snapshot(&refresh->snapshot, &refresh->snapshot_index, &maintained_update.terminal_operations,
                    NULL, terminal_layout, jz_str_slice(&jazz_model__query__ValidatedQuery__query(shape)->table), snapshot_tier, settled, &event);
                has_update = false; /* terminal operations consumed */
                if (err->code) { if (has_previous) jazz_node__node__RelationSnapshot__drop(&previous_snapshot); goto out; }
                /* A newly received authority closure is a reset boundary.
                 * Its complete value comes from the same receiver-local
                 * terminal reducer just advanced above. */
                if (authoritative_reset) {
                    *err = jazz_db__db__materialize_subscription_terminal_records(&refresh->snapshot, &refresh->snapshot_index);
                    if (err->code) { jazz_db__db__SubscriptionEvent__drop(event); jazz_node__node__RelationSnapshot__drop(&previous_snapshot); goto out; }
                    if (!has_previous) jz_panic("reset snapshot captured above");
                    jazz_db__db__SubscriptionEvent__drop(event);
                    event = jazz_db__db__subscription_delta_event_with_reset(snapshot_tier, settled, &previous_snapshot, &refresh->snapshot, true, sub->terminal_rows);
                    jazz_node__node__RelationSnapshot__drop(&previous_snapshot);
                }
                COVERED_TRACE("stage=publish_terminal ops=%u roots=%u", terminal_operation_count, refresh->snapshot.root_count);
                refresh->settled = settled;
                refresh_retain(env, state);
                *err = refresh_publish(env, refresh->sender, shape, event, publication_before, &refresh->snapshot, &refresh->snapshot_index, NULL);
                code = err->code ? R_ERR : R_PUSHED;
                goto out;
            }
            /* No terminal delta means the receiver-local collector state
             * did not change. The common reset publication below handles an
             * unchanged but newly complete closure. */
        } else {
            /* Flat / OrderedWindow */
            jazz_db_PublicationCheckpoint publication_before;
            *err = jazz_db__db__SubscriptionSender__checkpoint(refresh->sender, sub->read_tier, false, &refresh->snapshot, &refresh->snapshot_index, &publication_before);
            if (err->code) goto out_update;
            jazz_node_RelationSnapshot previous_snapshot;
            *err = jazz_db__db__materialized_subscription_snapshot(&refresh->snapshot, &refresh->snapshot_index, &previous_snapshot);
            if (err->code) goto out_update;
            jazz_db_SubscriptionEvent *event = NULL;
            *err = jazz_db__db__apply_maintained_update_to_snapshot(&refresh->snapshot, &refresh->snapshot_index, maintained_update,
                jz_str_slice(&jazz_model__query__ValidatedQuery__query(shape)->table), snapshot_tier, sub->previous_settled, NULL, &event);
            has_update = false; /* consumed */
            if (err->code) { jazz_node__node__RelationSnapshot__drop(&previous_snapshot); goto out; }
            refresh->snapshot_source = JAZZ_SNAP_LOCAL_MAINTAINED;
            bool settled = refresh_settled_now(env, sub, shape, binding, settled_tier, has_settled, &settled_key);
            if (settled) {
                jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
                bool has_cells = false;
                jz_err e = NS_FN(relation_snapshot_has_materialized_required_cells)(n, shape, &refresh->snapshot, &has_cells);
                jazz_db_node_unlock(node);
                if (e.code) {
                    *err = jazz_node__node__api_error__Error__From__from(e);
                    jazz_db__db__SubscriptionEvent__drop(event);
                    jazz_node__node__RelationSnapshot__drop(&previous_snapshot);
                    goto out;
                }
                settled = has_cells;
            }
            if (authoritative_reset) {
                jazz_db__db__SubscriptionEvent__drop(event);
                event = jazz_db__db__subscription_delta_event_with_reset(snapshot_tier, settled, &previous_snapshot, &refresh->snapshot, true, sub->terminal_rows);
            }
            jazz_node__node__RelationSnapshot__drop(&previous_snapshot);
            jazz_db_DeltaFields d;
            if (jazz_db__db__SubscriptionEvent__as_delta(event, &d)) {
                *d.publishable = sub->previous_settled != settled || *d.reset || !jazz_db__db__OutputVec__is_empty(d.added)
                    || !jazz_db__db__OutputVec__is_empty(d.updated) || !jazz_db__db__OutputVec__is_empty(d.removed);
                *d.settled = settled;
            }
            refresh->settled = settled;
            refresh_retain(env, state);
            *err = refresh_publish(env, refresh->sender, shape, event, publication_before, &refresh->snapshot, &refresh->snapshot_index, NULL);
            code = err->code ? R_ERR : R_PUSHED;
            goto out;
        }
    }
    if (authoritative_reset && refresh->maintained && jazz_node__node__LocalMaintainedViewSubscription__has_covered_input_sources(refresh->maintained)) {
        jazz_db_PublicationCheckpoint publication_before;
        *err = jazz_db__db__SubscriptionSender__checkpoint(refresh->sender, sub->read_tier, false, &refresh->snapshot, &refresh->snapshot_index, &publication_before);
        if (err->code) goto out_update;
        /* Local-first may already have published the exact same collector
         * state from its provisional local input; a no-op source handoff
         * is not a user-visible reset. */
        if (refresh->settled) {
            refresh->snapshot_source = JAZZ_SNAP_LOCAL_MAINTAINED;
            code = R_RETAIN; goto out_update;
        }
        /* Aggregate fact terminals have no structural app-row collector:
         * refresh the facade from the local terminal state. */
        if (jazz_model__query__ValidatedQuery__query(shape)->aggregate) {
            if (!refresh->maintained) jz_panic("covered reset retained its maintained graph");
            jazz_node_MaterializedSnapshot materialized;
            jazz_node_NodeState *n = jazz_db_node_lock(node);
            jz_err e = NS_FN(materialize_local_maintained_relation_snapshot_with_occurrences)(n, refresh->maintained, &materialized);
            jazz_db_node_unlock(node);
            if (e.code) { *err = jazz_node__node__api_error__Error__From__from(e); goto out_update; }
            jazz_node__node__RelationSnapshot__drop(&refresh->snapshot);
            refresh->snapshot = materialized.snapshot;
            jz_vec_drop(&materialized.root_occurrence_ids, 1, NULL);
        }
        bool settled = refresh_settled_now(env, sub, shape, binding, settled_tier, has_settled, &settled_key);
        /* A complete closure can be observably unchanged. It is still the
         * exact authority boundary for this receiver and must publish a
         * reset from the collector state. */
        jazz_node_RelationSnapshot snapshot;
        *err = jazz_db__db__materialized_subscription_snapshot(&refresh->snapshot, &refresh->snapshot_index, &snapshot);
        if (err->code) goto out_update;
        jazz_db_SubscriptionEvent *event = jazz_db__db__subscription_delta_event_with_reset(snapshot_tier, settled, &snapshot, &snapshot, true, sub->terminal_rows);
        COVERED_TRACE("stage=publish_covered_reset roots=%u settled=%d", snapshot.root_count, settled);
        jazz_node__node__RelationSnapshot__drop(&snapshot);
        refresh->snapshot_source = JAZZ_SNAP_LOCAL_MAINTAINED;
        refresh->settled = settled;
        refresh_retain(env, state);
        bool delivered = false;
        *err = refresh_publish(env, refresh->sender, shape, event, publication_before, &refresh->snapshot, &refresh->snapshot_index, &delivered);
        if (err->code) goto out_update;
        COVERED_TRACE("stage=covered_reset_delivery delivered=%d", delivered);
        code = R_PUSHED;
        goto out_update;
    }
    {
        bool preserve_local_overlay = suppressed_authoritative_change;
        if (sub->terminal_rows || preserve_local_overlay || has_remote_settled_tier || has_maintained_subscription) {
            /* A remote receipt has already been staged through the local
             * collector above. Never rebuild a relation facade from
             * authority output or a second storage read here. */
            *err = jazz_db__db__materialized_subscription_snapshot(&refresh->snapshot, &refresh->snapshot_index, &out->snapshot);
            if (err->code) goto out_update;
            if (sub->terminal_rows || (!preserve_local_overlay && has_remote_settled_tier)) out->snapshot_source = JAZZ_SNAP_LOCAL_MAINTAINED;
            else out->snapshot_source = sub->previous_source;
        } else {
            jazz_node_NodeState *n = jazz_db_node_lock(node);
            jz_err e = NS_FN(subscription_snapshot_in_authorization_mode)(n, shape, binding, sub->read_tier, sub->author, sub->read_view, sub->authorization_mode, &out->snapshot);
            jazz_db_node_unlock(node);
            if (e.code) { *err = jazz_node__node__api_error__Error__From__from(e); goto out_update; }
            out->snapshot_source = JAZZ_SNAP_LINK_SNAPSHOT;
        }
        out->settled = refresh_settled_now(env, sub, shape, binding, settled_tier, has_settled, &settled_key);
        out->snapshot_tier = snapshot_tier;
        out->force_reset_event = preserve_local_overlay;
        code = R_FALLTHROUGH;
    }
out_update:
    if (has_update) jazz_node__node__LocalMaintainedUpdate__drop(&maintained_update);
out:
    jz_btree_drop(&local_overlay_row_keys);
    if (has_reset) jazz_protocol__protocol__AuthorityResultKey__drop(&reset_key);
    if (has_delivered) jazz_protocol__protocol__AuthorityResultKey__drop(&delivered_key);
    if (has_settled) jazz_protocol__protocol__AuthorityResultKey__drop(&settled_key);
    jazz_model__query__ValidatedQuery__drop(shape);
    jazz_model__query__Binding__drop(binding);
    return code;
}

/* One iteration of the refresh loop for a live subscription. */

static int refresh_one(RefreshEnv *env, jazz_db_SubscriptionState *state, const jz_hmap *changed_logical_tables, jz_err *err) {
    jazz_db_NodeMutex *node = env->node;
    /* Finalization flips this synchronously, before awaiting the node
     * mutex. Never rehydrate a stream whose cleanup is merely waiting for
     * this tick to acquire that mutex. */
    if (*state->closed) return R_DROP;
    if (changed_logical_tables && !subscription_needs_targeted_refresh(state, changed_logical_tables)) {
        /* Targeted refresh skips evaluation, not subscription liveness. */
        return R_RETAIN;
    }
    RefreshSub sub;
    memset(&sub, 0, sizeof(sub));
    sub.state = state;
    sub.read_tier = state->read_tier;
    sub.pending_overlay = state->pending_overlay;
    sub.has_remote_read_tier = state->has_remote_read_tier;
    sub.remote_read_tier = state->remote_read_tier;
    sub.requires_authority_receipt = state->requires_authority_receipt;
    sub.read_view = jazz_protocol__protocol__ReadViewSpec__clone(state->read_view);
    sub.previous_source = state->snapshot_source;
    sub.previous_settled = state->settled;
    sub.author = state->author;
    sub.authorization_mode = state->authorization_mode;
    sub.terminal_rows = state->terminal_rows;
    jz_vec_clone(&sub.handles, &state->upstream_subscription_handles, sizeof(jazz_db_UpstreamCoverageHandle), NULL);
    int code = R_ERR;
    sub.awaiting_initial_owner_result = state->pending_initial_owner_result;
    if (sub.awaiting_initial_owner_result) {
        jazz_node_NodeState *owner = jazz_db_node_lock(node);
        bool ready = sub.handles.len != 0;
        for (u32 i = 0; ready && i < sub.handles.len; ++i) {
            jazz_protocol_AuthorityResultKey key;
            jz_err e = NS_FN(authority_result_key_for_subscription)(owner, sub.handles.ptr[i].subscription, &key);
            if (e.code) { jz_err_drop(&e); ready = false; break; }
            ready = NS_FN(has_settled_authority_result)(owner, &key) && !NS_FN(opening_pending_for_authority_result)(owner, &key);
            jazz_protocol__protocol__AuthorityResultKey__drop(&key);
        }
        jazz_db_node_unlock(node);
        if (!ready) { code = R_RETAIN; goto out; }
    }
    /* A settled strict answer may omit rows this client still holds live.
     * Until those rows are discovered and probed, the stream keeps draining
     * its graph but must not report settlement. */
    switch (deletion_reconciliation_known(state)) {
    case 0: sub.deletion_withheld = false; break;
    case 1: sub.deletion_withheld = true; break;
    default:
        *err = check_deletion_reconciliation(node, state, NULL, env->waker, &sub.deletion_withheld);
        if (err->code) goto out;
    }
    if (state->has_request_identity_claims) {
        sub.has_request_claims = true;
        jz_btree_clone(&sub.request_claims, &state->request_identity_claims.claims, str_key_clone, groove__records__Value__clone);
    }
    {
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        sub.groove_runtime_token = NS_FN(groove_runtime_token)(n);
        jazz_db_node_unlock(node);
    }
    /* Owner input arrival advances the existing storage-backed graph.
     * Recompile only after real runtime/plan invalidation. */
    if (state->groove_runtime_token != sub.groove_runtime_token) {
        code = refresh_reopen_runtime(env, &sub, err);
        goto out;
    }
    bool initial_local_snapshot_pending = state->pending_initial_local_snapshot;
    DetachedOut d;
    memset(&d, 0, sizeof(d));
    {
        DetachedSubscriptionRefresh refresh;
        DetachedSubscriptionRefresh_new(&refresh, state);
        code = refresh_detached(env, &sub, &refresh, initial_local_snapshot_pending, &d, err);
        DetachedSubscriptionRefresh_drop(&refresh);
    }
    if (code != R_FALLTHROUGH) goto out;
    jazz_node_RelationSnapshot previous;
    *err = jazz_db__db__materialized_subscription_snapshot(&state->snapshot, &state->snapshot_index, &previous);
    if (err->code) { jazz_node__node__RelationSnapshot__drop(&d.snapshot); code = R_ERR; goto out; }
    u8 previous_source = state->snapshot_source;
    bool has_maintained_flat_tuple_subscription = state->maintained_subscription && jazz_model__query__ValidatedQuery__query(state->shape)->flat_join;
    /* A link snapshot is a relation facade without the terminal's flat
     * tuple occurrence sidecar. Once a maintained terminal owns this
     * stream, replace it from the terminal. */
    if (!d.force_reset_event && has_maintained_flat_tuple_subscription && previous_source == JAZZ_SNAP_LOCAL_MAINTAINED && d.snapshot_source == JAZZ_SNAP_LINK_SNAPSHOT) {
        jazz_node_LocalMaintainedViewSubscription *maintained = state->maintained_subscription;
        if (!maintained) jz_panic("checked maintained subscription above");
        state->maintained_subscription = NULL;
        jazz_node_MaterializedSnapshot materialized;
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        jz_err e = NS_FN(materialize_local_maintained_relation_snapshot_with_occurrences)(n, maintained, &materialized);
        jazz_db_node_unlock(node);
        state->maintained_subscription = maintained;
        if (e.code) { *err = jazz_node__node__api_error__Error__From__from(e); code = R_ERR; goto out_prev; }
        jazz_node__node__RelationSnapshot__drop(&d.snapshot);
        d.snapshot = materialized.snapshot;
        jz_vec_drop(&materialized.root_occurrence_ids, 1, NULL);
        d.snapshot_source = JAZZ_SNAP_LOCAL_MAINTAINED;
    }
    if (d.force_reset_event || !jazz_node__node__RelationSnapshot__eq(&d.snapshot, &previous) || d.settled != sub.previous_settled) {
        jazz_db_PublicationCheckpoint publication_before;
        *err = jazz_db__db__SubscriptionSender__checkpoint(state->sender, sub.read_tier, d.settled, &state->snapshot, &state->snapshot_index, &publication_before);
        if (err->code) { code = R_ERR; goto out_prev; }
        jazz_db_SubscriptionEvent *event = d.force_reset_event
            ? jazz_db__db__subscription_delta_event_with_reset(d.snapshot_tier, d.settled, &previous, &d.snapshot, true, sub.terminal_rows)
            : jazz_db__db__subscription_delta_event(d.snapshot_tier, d.settled, &previous, &d.snapshot, sub.terminal_rows);
        jazz_node__node__RelationSnapshot__drop(&state->snapshot);
        jazz_db__db__relation_snapshot_with_delta_slack(&state->snapshot, &d.snapshot);
        jazz_node_RelationSnapshotIndex index;
        {
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
            *err = jazz_db__db__maintained_snapshot_index_or_row_index(n, state, &state->snapshot, &index);
            jazz_db_node_unlock(node);
        }
        if (err->code) { jazz_db__db__SubscriptionEvent__drop(event); code = R_ERR; goto out_prev; }
        jazz_node__node__RelationSnapshotIndex__drop(&state->snapshot_index);
        state->snapshot_index = index;
        jz_btree_drop(&state->snapshot_index.terminal_records);
        *err = decoded_terminal_records_or_default(state->maintained_subscription, &state->snapshot_index.terminal_records);
        if (err->code) { jazz_db__db__SubscriptionEvent__drop(event); code = R_ERR; goto out_prev; }
        state->snapshot_source = d.snapshot_source;
        state->settled = d.settled;
        *err = refresh_publish(env, state->sender, state->shape, event, publication_before, &state->snapshot, &state->snapshot_index, NULL);
        if (err->code) { code = R_ERR; goto out_prev; }
    }
    code = R_RETAIN;
out_prev:
    jazz_node__node__RelationSnapshot__drop(&previous);
    jazz_node__node__RelationSnapshot__drop(&d.snapshot);
out:
    jazz_protocol__protocol__ReadViewSpec__drop(sub.read_view);
    jz_vec_drop(&sub.handles, sizeof(jazz_db_UpstreamCoverageHandle), NULL);
    if (sub.has_request_claims) jz_btree_drop(&sub.request_claims);
    return code;
}

jz_err jazz_db__db__node_runtime__refresh_subscriptions_in_with_tables(jazz_db_NodeMutex *node, jazz_db_SubscriptionList *subscriptions, jazz_db_ActiveAuthorityViewReceipts *receipts,
                                                                       const jz_hmap *changed_tables, const jz_waker *progress_waker, u32 *out) {
    RefreshEnv env;
    memset(&env, 0, sizeof(env));
    env.node = node;
    env.receipts = receipts;
    env.waker = progress_waker;
    {
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        NS_FN(snapshot_pending_authoritative_resets)(n, &env.pending_resets);
        jazz_db_node_unlock(node);
    }
    jz_btree_init(&env.reconciled_resets, env.pending_resets.d);
    jz_err err = JZ_OK;
    {
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        jz_err e = NS_FN(drive_ready_query_runtime_with_waker)(n, progress_waker);
        jazz_db_node_unlock(node);
        if (e.code) { err = jazz_node__node__api_error__Error__From__from(e); goto fail; }
    }
    bool has_changed_logical = false;
    jz_hmap changed_logical_tables;
    memset(&changed_logical_tables, 0, sizeof(changed_logical_tables));
    if (changed_tables) {
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        NS_FN(logical_table_names_for_storage_tables)(n, changed_tables, &changed_logical_tables);
        jazz_db_node_unlock(node);
        if (changed_logical_tables.len != 0) has_changed_logical = true;
        else jz_hmap_drop(&changed_logical_tables);
    }
    /* Clone of the live weak list. */
    jazz_db_SubscriptionList live;
    jz_vec_clone(&live, subscriptions, sizeof(jazz_db_SubscriptionState *), NULL);
    for (u32 i = 0; i < live.len; ++i) jazz_db_rc_downgrade(live.ptr[i]);
    for (u32 i = 0; i < live.len; ++i) {
        jazz_db_SubscriptionState *state = jazz_db_weak_upgrade(live.ptr[i]);
        if (!state) continue;
        int code = refresh_one(&env, state, has_changed_logical ? &changed_logical_tables : NULL, &err);
        if (code == R_RETAIN) refresh_retain(&env, state);
        jz_rc_release(state);
        if (code == R_ERR) {
            for (u32 j = 0; j < live.len; ++j) jazz_db_weak_release(live.ptr[j]);
            jz_vec_drop(&live, sizeof(jazz_db_SubscriptionState *), NULL);
            if (has_changed_logical) jz_hmap_drop(&changed_logical_tables);
            goto fail;
        }
    }
    for (u32 j = 0; j < live.len; ++j) jazz_db_weak_release(live.ptr[j]);
    jz_vec_drop(&live, sizeof(jazz_db_SubscriptionState *), NULL);
    if (has_changed_logical) jz_hmap_drop(&changed_logical_tables);
    /* *subscriptions.borrow_mut() = retained; */
    for (u32 j = 0; j < subscriptions->len; ++j) jazz_db_weak_release(subscriptions->ptr[j]);
    jz_vec_drop(subscriptions, sizeof(jazz_db_SubscriptionState *), NULL);
    *subscriptions = env.retained;
    memset(&env.retained, 0, sizeof(env.retained));
    {
        jazz_protocol_AuthorityResultKey *key;
        u64 *generation;
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        JZ_BTREE_FOR(&env.reconciled_resets, key, generation) NS_FN(acknowledge_authoritative_reset)(n, key, *generation);
        jazz_db_node_unlock(node);
    }
    *out = env.changed;
    jz_btree_drop(&env.pending_resets);
    jz_btree_drop(&env.reconciled_resets);
    return JZ_OK;
fail:
    for (u32 j = 0; j < env.retained.len; ++j) jazz_db_weak_release(env.retained.ptr[j]);
    jz_vec_drop(&env.retained, sizeof(jazz_db_SubscriptionState *), NULL);
    jz_btree_drop(&env.pending_resets);
    jz_btree_drop(&env.reconciled_resets);
    return err;
}

jz_err jazz_db__db__node_runtime__refresh_subscriptions_in(jazz_db_NodeMutex *node, jazz_db_SubscriptionList *subscriptions, jazz_db_ActiveAuthorityViewReceipts *receipts, const jz_waker *progress_waker, u32 *out) {
    return jazz_db__db__node_runtime__refresh_subscriptions_in_with_tables(node, subscriptions, receipts, NULL, progress_waker, out);
}

/* ---------------- upstream subscription owners ---------------- */

/// Wake observers after any transaction-state transition, whether produced by
/// a queued mutation, local persistence, or an upstream acknowledgement.
void jazz_db__db__node_runtime__notify_write_state_waiters(jz_btree *waiters, jazz_model_TxId tx_id) {
    jazz_db_WriteStateWaiterVec removed;
    if (!jz_btree_remove(waiters, &tx_id, NULL, &removed)) return;
    for (u32 i = 0; i < removed.len; ++i) {
        jazz_db_Oneshot *sender = removed.ptr[i].notify;
        u8 unit = 0;
        (void)jazz_db__oneshot__send(sender, &unit);
        jazz_db__oneshot__drop_sender(sender);
    }
    jz_vec_drop(&removed, sizeof(jazz_db_WriteStateWaiter), NULL);
}

void jazz_db__db__node_runtime__register_upstream_subscription_owner(jz_btree *owners, const jazz_db_UpstreamCoverageHandle *handles, u32 n, jazz_db_SubscriptionState *state) {
    for (u32 i = 0; i < n; ++i) {
        jazz_db_WeakStateVec *entries = (jazz_db_WeakStateVec *)jz_btree_entry(owners, &handles[i].subscription, NULL);
        jazz_db_SubscriptionState *weak = jazz_db_rc_downgrade(state);
        jz_vec_push(entries, &weak, sizeof(weak));
    }
}

typedef struct { jazz_db_SubscriptionState *state; } UnregCtx;
static bool keep_other_live_owner(void *item, void *ctx) {
    jazz_db_SubscriptionState *entry = *(jazz_db_SubscriptionState **)item;
    return entry != ((UnregCtx *)ctx)->state && jazz_db_weak_strong_count(entry) > 0;
}
static bool keep_live_owner(void *item, void *ctx) { (void)ctx; return jazz_db_weak_strong_count(*(jazz_db_SubscriptionState **)item) > 0; }
static void weak_state_drop(void *item) { jazz_db_weak_release(*(jazz_db_SubscriptionState **)item); }

void jazz_db__db__node_runtime__unregister_upstream_subscription_owner(jz_btree *owners, jazz_protocol_SubscriptionKey subscription, jazz_db_SubscriptionState *state) {
    jazz_db_WeakStateVec *entries = (jazz_db_WeakStateVec *)jz_btree_get(owners, &subscription);
    if (!entries) return;
    UnregCtx ctx = { state };
    jz_vec_retain(entries, sizeof(jazz_db_SubscriptionState *), keep_other_live_owner, &ctx, weak_state_drop);
    if (entries->len == 0) jz_btree_remove(owners, &subscription, NULL, NULL);
}

static bool relay_owners_have_subscription(const jz_btree *owners, const jazz_protocol_SubscriptionKey *subscription) {
    jazz_db_RelayOwnerKey *k;
    jazz_db_RelayUpstreamSubscriptionOwner *v;
    JZ_BTREE_FOR(owners, k, v) {
        (void)v;
        if (jazz_protocol_SubscriptionKey_eq(&k->subscription, subscription)) return true;
    }
    return false;
}

/// Release the matching connection pin. Return an owner only when this was
/// the final pin and the caller must retire the upstream stream itself.
bool jazz_db__db__node_runtime__retire_relay_upstream_subscription(jz_btree *owners, jazz_protocol_SubscriptionKey subscription, u64 downstream_connection_epoch,
                                                                   const jazz_db_CoverageKey *coverage, jazz_db_RelayUpstreamSubscriptionOwner *out) {
    jazz_db_RelayOwnerKey key;
    memset(&key, 0, sizeof(key));
    key.subscription = subscription;
    key.downstream_connection_epoch = downstream_connection_epoch;
    key.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&coverage->opts);
    const jazz_db_RelayUpstreamSubscriptionOwner *owner = (const jazz_db_RelayUpstreamSubscriptionOwner *)jz_btree_get(owners, &key);
    if (!owner) return false;
    if (owner->downstream_connection_epoch != downstream_connection_epoch || !jazz_db__db__CoverageKey__eq(&owner->coverage, coverage)) return false;
    jazz_db_RelayUpstreamSubscriptionOwner removed;
    if (!jz_btree_remove(owners, &key, NULL, &removed)) return false;
    /* The departing connection releases its pin, not another connection's
     * shared stream. Only the last pin authorizes wire/local retirement. */
    if (relay_owners_have_subscription(owners, &subscription)) {
        jazz_db__db__RelayUpstreamSubscriptionOwner__drop(&removed);
        return false;
    }
    *out = removed;
    return true;
}

/// Retire a relay owner after its authority has terminally rejected the wire
/// subscription. A RegisterShape rejection uses a nil binding marker and
/// retires every pin for that exact shape/read-view registration.
void jazz_db__db__node_runtime__take_relay_upstream_subscription_owner(jz_btree *owners, jazz_protocol_SubscriptionKey subscription, jazz_db_KeyedRelayOwnerVec *out) {
    static const jz_uuid nil = {{0}};
    JZ_VEC(jazz_db_RelayOwnerKey) keys = {0};
    {
        jazz_db_RelayOwnerKey *k;
        jazz_db_RelayUpstreamSubscriptionOwner *v;
        JZ_BTREE_FOR(owners, k, v) {
            (void)v;
            const jazz_protocol_SubscriptionKey *candidate = &k->subscription;
            if (jazz_protocol_SubscriptionKey_eq(candidate, &subscription)
                || (jz_uuid_eq(&subscription.binding_id.u, &nil)
                    && jz_uuid_eq(&candidate->shape_id.u, &subscription.shape_id.u)
                    && memcmp(&candidate->read_view, &subscription.read_view, sizeof(subscription.read_view)) == 0))
                jz_vec_push(&keys, k, sizeof(*k));
        }
    }
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < keys.len; ++i) {
        jazz_db_KeyedRelayOwner item;
        if (jz_btree_remove(owners, &keys.ptr[i], NULL, &item.owner)) {
            item.subscription = keys.ptr[i].subscription;
            jz_vec_push(out, &item, sizeof(item));
        }
    }
    jz_vec_drop(&keys, sizeof(jazz_db_RelayOwnerKey), NULL);
}

/// Take every propagated upstream owner for a disconnected downstream link.
/// Taking the records before enqueuing wire retirement makes repeated detach
/// calls and late rejections harmless no-ops.
void jazz_db__db__node_runtime__retire_relay_upstream_subscriptions_for_connection(jz_btree *owners, u64 downstream_connection_epoch, jazz_db_KeyedRelayOwnerVec *out) {
    JZ_VEC(jazz_db_RelayOwnerKey) keys = {0};
    {
        jazz_db_RelayOwnerKey *k;
        jazz_db_RelayUpstreamSubscriptionOwner *v;
        JZ_BTREE_FOR(owners, k, v)
            if (v->downstream_connection_epoch == downstream_connection_epoch) jz_vec_push(&keys, k, sizeof(*k));
    }
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < keys.len; ++i) {
        jazz_db_KeyedRelayOwner item;
        if (!jz_btree_remove(owners, &keys.ptr[i], NULL, &item.owner)) continue;
        if (relay_owners_have_subscription(owners, &keys.ptr[i].subscription)) {
            jazz_db__db__RelayUpstreamSubscriptionOwner__drop(&item.owner);
            continue;
        }
        item.subscription = keys.ptr[i].subscription;
        jz_vec_push(out, &item, sizeof(item));
    }
    jz_vec_drop(&keys, sizeof(jazz_db_RelayOwnerKey), NULL);
}

bool jazz_db__db__node_runtime__register_shape_rejection_matches(jazz_protocol_SubscriptionKey subscription, const jazz_model_ValidatedQuery *shape, const jazz_protocol_RegisterShapeOptions *opts) {
    jazz_model_ShapeId shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
    jazz_protocol_ReadViewKey read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(opts);
    return jz_uuid_eq(&shape_id.u, &subscription.shape_id.u) && memcmp(&read_view, &subscription.read_view, sizeof(read_view)) == 0;
}

u32 jazz_db__db__node_runtime__route_upstream_subscription_rejection(jazz_db_SubscriptionList *subscriptions, jz_btree *owners, jazz_protocol_SubscriptionKey subscription,
                                                                    const jazz_protocol_SubscribeRejectReason *reason) {
    static const jz_uuid nil = {{0}};
    u32 delivered = 0;
    jazz_db_WeakStateVec *entries = (jazz_db_WeakStateVec *)jz_btree_get(owners, &subscription);
    if (entries) {
        jz_vec_retain(entries, sizeof(jazz_db_SubscriptionState *), keep_live_owner, NULL, weak_state_drop);
        for (u32 i = 0; i < entries->len; ++i) {
            jazz_db_SubscriptionState *state = jazz_db_weak_upgrade(entries->ptr[i]);
            if (!state) continue;
            jazz_protocol_SubscribeRejectReason r;
            jazz_protocol__protocol__SubscribeRejectReason__clone(&r, reason);
            jz_err e = jazz_db__first_load__SubscriptionState__send_rejection(state, r);
            if (e.code == 0) delivered += 1; else jz_err_drop(&e);
            jz_rc_release(state);
        }
        if (delivered > 0) return delivered;
    }
    for (u32 i = 0; i < subscriptions->len; ++i) {
        jazz_db_SubscriptionState *state_ref = jazz_db_weak_upgrade(subscriptions->ptr[i]);
        if (!state_ref) continue;
        if (!state_ref->propagates_upstream) { jz_rc_release(state_ref); continue; }
        jazz_protocol_RegisterShapeOptions opts;
        jazz_protocol__protocol__RegisterShapeOptions__default(&opts);
        opts.tier = state_ref->has_remote_read_tier ? state_ref->remote_read_tier : state_ref->read_tier;
        jazz_protocol__protocol__ReadViewSpec__drop(opts.read_view);
        opts.read_view = jazz_protocol__protocol__ReadViewSpec__clone(state_ref->read_view);
        bool matches = jazz_db__db__node_runtime__register_shape_rejection_matches(subscription, state_ref->shape, &opts);
        jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
        if (matches && !jz_uuid_eq(&subscription.binding_id.u, &nil)) {
            jazz_model_BindingId b = jazz_model__query__Binding__binding_id(state_ref->binding);
            if (!jz_uuid_eq(&b.u, &subscription.binding_id.u)) matches = false;
        }
        if (matches) {
            jazz_protocol_SubscribeRejectReason r;
            jazz_protocol__protocol__SubscribeRejectReason__clone(&r, reason);
            jz_err e = jazz_db__first_load__SubscriptionState__send_rejection(state_ref, r);
            if (e.code == 0) delivered += 1; else jz_err_drop(&e);
        }
        jz_rc_release(state_ref);
    }
    return delivered;
}

/* ---------------- Transport (dyn trait with default methods) ---------------- */
/* A NULL vtable slot selects the trait's default method body. */

jz_err jazz_db__db__node_runtime__Transport__send(jazz_db_Transport *t, jazz_protocol_SyncMessage m, jazz_db_TransportError *err) {
    return t->vt->send(t->self, m, err);
}

/// Like `send`, but backpressure hands the unsent message back. The default
/// copies the message before sending.
jz_err jazz_db__db__node_runtime__Transport__try_send(jazz_db_Transport *t, jazz_protocol_SyncMessage m, bool *returned, jazz_protocol_SyncMessage *back, jazz_db_TransportError *err) {
    if (t->vt->try_send) return t->vt->try_send(t->self, m, returned, back, err);
    jazz_protocol_SyncMessage retained;
    jazz_protocol__protocol__SyncMessage__clone(&retained, &m);
    jz_err e = t->vt->send(t->self, m, err);
    if (e.code == 0) { jazz_protocol__protocol__SyncMessage__drop(&retained); *returned = false; return JZ_OK; }
    if (e.code == JAZZ_DB_TRANSPORT_ERR && err->tag == JAZZ_TRANSPORT_BACKPRESSURE) {
        jz_err_drop(&e);
        jazz_protocol__wire__TransportError__drop(err);
        *returned = true;
        *back = retained;
        return JZ_OK;
    }
    jazz_protocol__protocol__SyncMessage__drop(&retained);
    return e;
}

bool jazz_db__db__node_runtime__Transport__try_recv(jazz_db_Transport *t, jazz_protocol_SyncMessage *out) {
    return t->vt->try_recv(t->self, out);
}

jz_err jazz_db__db__node_runtime__Transport__try_recv_result(jazz_db_Transport *t, bool *has, jazz_protocol_SyncMessage *out, jazz_db_TransportError *err) {
    if (t->vt->try_recv_result) return t->vt->try_recv_result(t->self, has, out, err);
    *has = t->vt->try_recv(t->self, out);
    return JZ_OK;
}

jz_err jazz_db__db__node_runtime__Transport__try_recv_owned_result(jazz_db_Transport *t, bool *has, jazz_db_ReceivedSyncMessage *out, jazz_db_TransportError *err) {
    if (t->vt->try_recv_owned_result) return t->vt->try_recv_owned_result(t->self, has, out, err);
    jazz_protocol_SyncMessage message;
    JZ_TRY(jazz_db__db__node_runtime__Transport__try_recv_result(t, has, &message, err));
    if (*has) jazz_db__db__routed_messages__ReceivedSyncMessage__unleased(out, message);
    return JZ_OK;
}

jz_err jazz_db__db__node_runtime__Transport__poll_flush(jazz_db_Transport *t, jazz_db_WireFlushStatus *out, jazz_db_TransportError *err) {
    if (t->vt->poll_flush) return t->vt->poll_flush(t->self, out, err);
    *out = JAZZ_FLUSH_IDLE;
    return JZ_OK;
}

bool jazz_db__db__node_runtime__Transport__has_terminal_failure(const jazz_db_Transport *t) {
    return t->vt->has_terminal_failure ? t->vt->has_terminal_failure(t->self) : false;
}

jazz_db_WireError *jazz_db__db__node_runtime__Transport__remote_wire_error(const jazz_db_Transport *t) {
    return t->vt->remote_wire_error ? t->vt->remote_wire_error(t->self) : NULL;
}

bool jazz_db__db__node_runtime__Transport__incomplete_receive_timeout_ms(const jazz_db_Transport *t, u64 *out) {
    return t->vt->incomplete_receive_timeout_ms ? t->vt->incomplete_receive_timeout_ms(t->self, out) : false;
}

void *jazz_db__db__node_runtime__Transport__shared_auxiliary_endpoint(const jazz_db_Transport *t) {
    return t->vt->shared_auxiliary_endpoint ? t->vt->shared_auxiliary_endpoint(t->self) : NULL;
}

void jazz_db__db__node_runtime__Transport__set_trusted_encoder(jazz_db_Transport *t, bool trusted) {
    if (t->vt->set_trusted_encoder) t->vt->set_trusted_encoder(t->self, trusted);
}

void *jazz_db__db__node_runtime__Transport__wire_inbound_context(const jazz_db_Transport *t) {
    return t->vt->wire_inbound_context ? t->vt->wire_inbound_context(t->self) : NULL;
}

bool jazz_db__db__node_runtime__Transport__connection_session_context(const jazz_db_Transport *t, jazz_db_ConnectionSessionContext *out) {
    return t->vt->connection_session_context ? t->vt->connection_session_context(t->self, out) : false;
}

bool jazz_db__db__node_runtime__Transport__permits_delegated_sessions(const jazz_db_Transport *t) {
    return t->vt->permits_delegated_sessions ? t->vt->permits_delegated_sessions(t->self) : false;
}
