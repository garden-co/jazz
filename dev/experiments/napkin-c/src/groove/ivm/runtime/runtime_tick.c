/* Port of crates/groove/src/ivm/runtime/runtime_tick.rs:
 * tick orchestration, hydration, memo eviction, and durable-node evaluation.
 *
 * Async model: every Rust `Poll<T>` becomes a u32 (0 = Ready, GROOVE_POLL_PENDING)
 * with the value in an out-param. `async fn` that only drives such polls run
 * them to completion synchronously (one poll; a remaining Pending is reported
 * as Pending to the caller). The persist flush future completes synchronously. */
#include "runtime_tick.h"
#include "record_projection.h"

#define RT(name) groove__ivm__runtime__runtime_tick__##name
#define SB(name) groove__ivm__runtime__subscriptions__##name
#define ES(name) groove__ivm__runtime__evaluation_session__##name
#define TEV(name) groove__ivm__runtime__evaluator__TickEvaluator__##name
#define MEMO(name) groove__ivm__runtime__evaluation_memo__EvaluationMemo__##name

/* mod.rs */
#define EVAL_MEMO_MAX_ENTRIES ((usize)8192)
#define EVAL_MEMO_MAX_BYTES ((usize)128 * 1024 * 1024)
/* Hydration can discover a large resident graph in one poll. Keep every owner
 * turn bounded so a browser worker returns to transport ingress between
 * CPU-only graph slices. */
#define MAX_HYDRATION_RUNNABLE_NODES_PER_POLL ((usize)32)

/* ---------------------------------------------------------------------- */
/* small shared helpers                                                   */
/* ---------------------------------------------------------------------- */
static void nset_init(jz_hmap *s) { jz_hmap_init(s, &groove_ivm_rt_a_hdesc_nodeid_set); }
static bool nset_insert(jz_hmap *s, groove_NodeId n) { return !jz_hmap_insert(s, &n, NULL, NULL); }
static bool nset_contains(const jz_hmap *s, groove_NodeId n) { return jz_hmap_get(s, &n) != NULL; }
static void nset_clone(jz_hmap *dst, const jz_hmap *src) {
    nset_init(dst);
    groove_NodeId *k; void *v;
    JZ_HMAP_FOR(src, k, v) nset_insert(dst, *k);
}
static void nset_clone_v(void *dst, const void *src) { nset_clone(dst, src); }
static void nset_drop_v(void *s) { jz_hmap_drop(s); }
static jz_hmap *nset_arc_new(jz_hmap set) {
    jz_hmap *p = jz_rc_new(sizeof(jz_hmap), nset_drop_v);
    *p = set;
    return p;
}
static jz_hmap *nset_make_mut(jz_hmap **rc) { return jz_rc_make_mut((void **)rc, sizeof(jz_hmap), nset_clone_v); }
static void nset_to_vec(const jz_hmap *s, groove_NodeIdVec *out) {
    groove_NodeId *k; void *v;
    JZ_HMAP_FOR(s, k, v) jz_vec_push(out, k, sizeof *k);
}
static void nodeid_vec_drop_v(void *v) { jz_vec_drop(v, sizeof(groove_NodeId), NULL); }
static int nodeid_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    u64 x = ((const groove_NodeId *)a)->v, y = ((const groove_NodeId *)b)->v;
    return x < y ? -1 : x > y;
}

/* HashMap clone through the map's own descriptor */
static void hmap_clone(jz_hmap *dst, const jz_hmap *src, void (*kclone)(void *, const void *), void (*vclone)(void *, const void *)) {
    jz_hmap_init(dst, src->d);
    u16 ks = src->d->key_size, vs = src->d->val_size;
    void *kb = __builtin_alloca(ks + 8), *vb = __builtin_alloca(vs + 8);
    void *k, *v;
    JZ_HMAP_FOR(src, k, v) {
        if (kclone) kclone(kb, k); else memcpy(kb, k, ks);
        if (vs) { if (vclone) vclone(vb, v); else memcpy(vb, v, vs); }
        jz_hmap_insert(dst, kb, vs ? vb : NULL, NULL);
    }
}

/* Release a map whose entries were moved out shallowly (no key/value drop). */
static void hmap_forget(jz_hmap *m) {
    const jz_hmap_desc *d = m->d;
    jz_hmap_desc nodrop = *d; nodrop.key_drop = NULL; nodrop.val_drop = NULL;
    m->d = &nodrop;
    jz_hmap_drop(m);
    jz_hmap_init(m, d);
}
/* dst.extend(std::mem::take(src)) */
static void hmap_move_into(jz_hmap *dst, jz_hmap *src) {
    void *k, *v;
    JZ_HMAP_FOR(src, k, v) jz_hmap_insert(dst, k, v, NULL);
    hmap_forget(src);
}
static void keyset_clone_v(void *dst, const void *src) { hmap_clone(dst, src, NULL, NULL); }
static void str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone(src); }
static void binding_key_clone_v(void *dst, const void *src) { groove__ivm__op_types__BindingSourceKey__clone(dst, src); }

typedef struct NodeUsize { groove_NodeId node; usize n; } NodeUsize;
static const jz_hmap_desc hdesc_node_usize = { sizeof(groove_NodeId), sizeof(usize), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, NULL };
static void arc_deltas_release_v(void *p) { groove_RecordDeltas *d = *(groove_RecordDeltas **)p; if (d) jz_rc_release(d); }
static void record_deltas_drop_v(void *p) { SB(RecordDeltas__drop)(p); }
static const jz_hmap_desc hdesc_node_arc_deltas = { sizeof(groove_NodeId), sizeof(groove_RecordDeltas *), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, arc_deltas_release_v };
static const jz_hmap_desc hdesc_node_deltas = { sizeof(groove_NodeId), sizeof(groove_RecordDeltas), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, record_deltas_drop_v };

static void err_rc_drop(void *p) { jz_err_drop((jz_err *)p); }
static jz_err *err_rc_new(jz_err e) { jz_err *p = jz_rc_new(sizeof(jz_err), err_rc_drop); *p = e; return p; }
static jz_err err_rc_clone_value(const jz_err *e) {
    jz_err out = { e->code, NULL };
    if (e->msg) { out.msg = jz_alloc(sizeof(jz_str)); *out.msg = jz_str_clone(e->msg); }
    return out;
}

static inline void wake(const jz_waker *cx) { if (cx && cx->wake) jz_waker_wake(cx); }

/* Arc<[NodeId]> */
static void nodeid_list_drop(void *p) { groove_NodeIdList *l = p; jz_free(l->items); }
static groove_NodeIdList *nodeid_list_from(groove_NodeIdVec v) {
    groove_NodeIdList *l = jz_rc_new(sizeof *l, nodeid_list_drop);
    l->len = v.len; l->items = v.ptr;
    return l;
}
static void nodeid_list_release_v(void *p) { jz_rc_release(*(void **)p); }
static const jz_hmap_desc hdesc_pipelines = { sizeof(groove_NodeId), sizeof(groove_NodeIdList *), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, nodeid_list_release_v };

static usize layout_slot(const groove_ExecutionLayout *l, groove_NodeId node) {
    const usize *s = jz_hmap_get(&l->slots, &node);
    if (!s) jz_panic("layout slot");
    return *s;
}
static const usize *layout_slot_get(const groove_ExecutionLayout *l, groove_NodeId node) { return jz_hmap_get(&l->slots, &node); }
static groove_FrameInputs layout_inputs(const groove_ExecutionLayout *l, usize slot) {
    u32 len = 0;
    const u32 *p = groove__ivm__execution_layout__ExecutionLayout__inputs(l, (u32)slot, &len);
    groove_FrameInputs f = { (const usize *)p, len };
    return f;
}
static const u32 *layout_dependents(const groove_ExecutionLayout *l, usize slot, u32 *len) {
    return groove__ivm__execution_layout__ExecutionLayout__dependents(l, (u32)slot, len);
}

/* BTreeMap<EvaluationRequestKey, Vec<NodeId>>: comparator owned by evaluation_session.rs */
static jz_btree_desc REQ_DEPS_DESC;
static const jz_btree_desc *req_deps_desc(void) {
    if (!REQ_DEPS_DESC.key_size) {
        REQ_DEPS_DESC.key_size = sizeof(groove_EvaluationRequestKey);
        REQ_DEPS_DESC.val_size = sizeof(groove_NodeIdVec);
        REQ_DEPS_DESC.cmp = groove_ivm_rt_REQUEST_KEY_SET.cmp;
        REQ_DEPS_DESC.key_drop = groove_ivm_rt_REQUEST_KEY_SET.key_drop;
        REQ_DEPS_DESC.val_drop = nodeid_vec_drop_v;
    }
    return &REQ_DEPS_DESC;
}

static usize memo_payload_bytes(const groove_EvaluationMemo *m) {
    usize sum = 0;
    groove_EvaluationMemoIter it; MEMO(iter)(m, &it);
    const groove_EvalMemoKey *k; groove_EvalMemoEntry *e;
    while (groove__ivm__runtime__evaluation_memo__EvaluationMemoIter__next(&it, &k, &e)) sum += e->payload_bytes;
    return sum;
}

/* ---------------------------------------------------------------------- */
/* PendingSubscriptionOutput                                              */
/* ---------------------------------------------------------------------- */
typedef struct MaterializedOutput { groove_RootIndirectValues representation; groove_RecordDeltas *records; } MaterializedOutput;
typedef struct PendingSubscriptionOutput {
    groove_RecordDeltas *physical; /* Arc */
    JZ_VEC(MaterializedOutput) materialized;
} PendingSubscriptionOutput;

static void materialized_output_drop(void *p) {
    MaterializedOutput *m = p;
    SB(RootIndirectValues__drop)(&m->representation);
    jz_rc_release(m->records);
}
static void pending_output_drop_v(void *p) {
    PendingSubscriptionOutput *o = p;
    jz_rc_release(o->physical);
    jz_vec_drop(&o->materialized, sizeof(MaterializedOutput), materialized_output_drop);
}
static const jz_hmap_desc hdesc_pending_outputs = { sizeof(groove_NodeId), sizeof(PendingSubscriptionOutput), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, pending_output_drop_v };

static PendingSubscriptionOutput RT(PendingSubscriptionOutput__new)(groove_RecordDeltas *physical) {
    PendingSubscriptionOutput o; memset(&o, 0, sizeof o);
    o.physical = physical;
    return o;
}

static groove_RecordDeltas *RT(PendingSubscriptionOutput__materialized_as)(const PendingSubscriptionOutput *self, const groove_RootIndirectValues *representation) {
    for (u32 i = 0; i < self->materialized.len; i++) {
        const MaterializedOutput *m = &self->materialized.ptr[i];
        if (SB(RootIndirectValues__eq)(&m->representation, representation)) return jz_rc_clone(m->records);
    }
    return NULL;
}

static void RT(PendingSubscriptionOutput__record_materialized)(PendingSubscriptionOutput *self, const groove_RootIndirectValues *representation, groove_RecordDeltas *records) {
    groove_RecordDeltas *existing = RT(PendingSubscriptionOutput__materialized_as)(self, representation);
    if (existing) { jz_rc_release(existing); jz_rc_release(records); return; }
    MaterializedOutput m;
    SB(RootIndirectValues__clone)(&m.representation, representation);
    m.records = records;
    jz_vec_push(&self->materialized, &m, sizeof m);
}

/* ---------------------------------------------------------------------- */
/* Notifications / resident publication                                   */
/* ---------------------------------------------------------------------- */
static void notification_drop(void *p) { SB(QueuedMultisinkDeltas__drop)(&((groove_SubscriptionNotification *)p)->queued); }
void RT(NotificationVec__drop)(void *v) { jz_vec_drop(v, sizeof(groove_SubscriptionNotification), notification_drop); }
static void notification_vec_rc_drop(void *p) { RT(NotificationVec__drop)(p); }

void RT(PendingResidentPublication__clone)(groove_PendingResidentPublication *dst, const groove_PendingResidentPublication *src) {
    *dst = *src;
    jz_rc_clone(dst->publication);
    jz_rc_clone(dst->notifications);
    jz_rc_clone(dst->completed);
    if (dst->chunk_provider) jz_rc_clone(dst->chunk_provider);
}
void RT(PendingResidentPublication__drop)(groove_PendingResidentPublication *p) {
    jz_rc_release(p->publication);
    jz_rc_release(p->notifications);
    jz_rc_release(p->completed);
    if (p->chunk_provider) jz_rc_release(p->chunk_provider);
}

groove_OwnedWriteOperationVec RT(ResidentTick__take_durable_writes)(const groove_ResidentTick *t) {
    return groove__storage__StagedWriteState__into_operations(t->durable_writes);
}
void RT(ResidentTick__drop)(groove_ResidentTick *t) {
    groove__ivm__runtime__state__TickMetrics__drop(&t->metrics);
    RT(PendingResidentPublication__drop)(&t->publication);
    jz_rc_release(t->durable_writes);
}

/* ---------------------------------------------------------------------- */
/* EvaluationFailure                                                      */
/* ---------------------------------------------------------------------- */
enum { EFK_SCOPED, EFK_FATAL };
typedef struct EvaluationFailure {
    u32 kind;
    jz_hmap affected_nodes;
    jz_err *error; /* Arc<IvmRuntimeError>; NULL = no failure (Ok) */
} EvaluationFailure;

static void RT(EvaluationFailure__drop)(EvaluationFailure *f) {
    if (!f->error) return;
    jz_hmap_drop(&f->affected_nodes);
    jz_rc_release(f->error);
    f->error = NULL;
}

/* Arc::try_unwrap(self.error).expect("unshared evaluation failure") */
static jz_err RT(EvaluationFailure__into_error)(EvaluationFailure f) {
    if (jz_rc_strong(f.error) != 1) jz_panic("unshared evaluation failure");
    jz_err e = *f.error;
    f.error->code = 0; f.error->msg = NULL;
    jz_rc_release(f.error);
    jz_hmap_drop(&f.affected_nodes);
    return e;
}

/* impl From<IvmRuntimeError> for EvaluationFailure */
static EvaluationFailure RT(EvaluationFailure__From__from)(jz_err error) {
    EvaluationFailure f;
    f.kind = EFK_FATAL;
    nset_init(&f.affected_nodes);
    f.error = err_rc_new(error);
    return f;
}

/* ---------------------------------------------------------------------- */
/* EvaluationWorkQueue                                                    */
/* ---------------------------------------------------------------------- */
enum { EE_WAITING, EE_FUSED, EE_RUNNABLE, EE_COMPLETE };
typedef struct EvaluationEntry { u32 tag; usize waiting; } EvaluationEntry;
static inline bool entry_is(EvaluationEntry e, u32 tag) { return e.tag == tag; }
static inline EvaluationEntry entry_waiting(usize n) { EvaluationEntry e = { EE_WAITING, n }; return e; }
static inline EvaluationEntry entry_of(u32 tag) { EvaluationEntry e = { tag, 0 }; return e; }
typedef JZ_VEC(EvaluationEntry) EntryVec;

typedef struct EvaluationWorkQueue {
    jz_hmap unary_batches;       /* NodeId -> PendingUnaryBatch */
    jz_hmap pipelines;           /* NodeId -> Arc<[NodeId]> */
    jz_hmap pipeline_batches;    /* NodeId -> PendingPipeline */
    groove_UsizeVec task_slots;
    groove_ExecutionLayout *layout; /* Arc */
    EntryVec entries;
    jz_btree request_dependents; /* EvaluationRequestKey -> Vec<NodeId> */
    jz_deque runnable;           /* VecDeque<NodeId> */
    groove_NodeIdVec completed_events;
    groove_UsizeVec temporal_waiting;
    /* Nodes whose completion an earlier frame of this evaluation already
     * published (#3306). */
    jz_hmap released_by_earlier_frame;
} EvaluationWorkQueue;

static void RT(EvaluationWorkQueue__drop)(EvaluationWorkQueue *q) {
    jz_hmap_drop(&q->unary_batches);
    jz_hmap_drop(&q->pipelines);
    jz_hmap_drop(&q->pipeline_batches);
    jz_vec_drop(&q->task_slots, sizeof(usize), NULL);
    if (q->layout) jz_rc_release(q->layout);
    jz_vec_drop(&q->entries, sizeof(EvaluationEntry), NULL);
    jz_btree_drop(&q->request_dependents);
    jz_deque_drop(&q->runnable, NULL);
    jz_vec_drop(&q->completed_events, sizeof(groove_NodeId), NULL);
    jz_vec_drop(&q->temporal_waiting, sizeof(usize), NULL);
    jz_hmap_drop(&q->released_by_earlier_frame);
}

static void RT(EvaluationWorkQueue__make_slot_runnable)(EvaluationWorkQueue *q, usize slot) {
    EvaluationEntry e = q->entries.ptr[slot];
    if (entry_is(e, EE_RUNNABLE) || entry_is(e, EE_COMPLETE)) return;
    q->entries.ptr[slot] = entry_of(EE_RUNNABLE);
    jz_deque_push_back(&q->runnable, &q->layout->nodes.ptr[slot]);
}

static jz_err RT(EvaluationWorkQueue__discover_frame)(const groove_IvmGraph *graph, const jz_hmap *node_meta, const groove_NodeId *roots, u32 nroots,
                                                      bool hydrate_sources, EvaluationWorkQueue *out) {
    groove_ExecutionLayout *layout = NULL;
    groove_NodeId missing;
    if (!groove__ivm__graph__IvmGraph__execution_layout(graph, roots, nroots, &layout, &missing)) return groove_err_node_not_found(missing);
    EvaluationWorkQueue q; memset(&q, 0, sizeof q);
    jz_hmap_init(&q.unary_batches, &groove_ivm_rt_a_hdesc_pending_unary);
    jz_hmap_init(&q.pipelines, &hdesc_pipelines);
    jz_hmap_init(&q.pipeline_batches, &groove_ivm_rt_PENDING_PIPELINE_MAP);
    u32 n = layout->nodes.len;
    for (u32 i = 0; i < n; i++) { usize s = i; jz_vec_push(&q.task_slots, &s, sizeof s); }
    for (u32 i = 0; i < layout->input_counts.len; i++) {
        EvaluationEntry e = entry_waiting(layout->input_counts.ptr[i]);
        jz_vec_push(&q.entries, &e, sizeof e);
    }
    for (u32 i = 0; i < n; i++) { usize z = 0; jz_vec_push(&q.temporal_waiting, &z, sizeof z); }
    q.layout = layout;
    jz_btree_init(&q.request_dependents, req_deps_desc());
    jz_deque_init(&q.runnable, sizeof(groove_NodeId));
    nset_init(&q.released_by_earlier_frame);

    /* Contract physical tasks, not graph identity. A globally shared or
     * explicitly retained intermediate remains independently executable. */
    groove_OptU32 *predecessors = jz_zalloc(sizeof(groove_OptU32) * (n ? n : 1));
    for (u32 i = 0; i < layout->pipeline_predecessors.len; i++) {
        groove_OptU32 p = layout->pipeline_predecessors.ptr[i];
        if (p.some) {
            groove_NodeId id = layout->nodes.ptr[p.v];
            const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, id);
            if (!node) jz_panic("layout node");
            const groove_NodeRuntimeMeta *meta = jz_hmap_get(node_meta, &id);
            bool keep = !groove__ivm__graph__GraphNode__is_durable(node) && node->children.len == 1 && (!meta || meta->retainers.len == 0);
            if (!keep) p.some = false;
        }
        predecessors[i] = p;
    }
    bool *interior = jz_zalloc(n ? n : 1);
    for (u32 i = 0; i < n; i++) if (predecessors[i].some) interior[predecessors[i].v] = true;
    for (u32 tail = 0; tail < n; tail++) {
        if (interior[tail] || !predecessors[tail].some) continue;
        groove_NodeIdVec members = {0};
        jz_vec_push(&members, &layout->nodes.ptr[tail], sizeof(groove_NodeId));
        u32 cursor = tail;
        while (predecessors[cursor].some) {
            u32 previous = predecessors[cursor].v;
            q.task_slots.ptr[previous] = tail;
            q.entries.ptr[previous] = entry_of(EE_FUSED);
            jz_vec_push(&members, &layout->nodes.ptr[previous], sizeof(groove_NodeId));
            cursor = previous;
        }
        for (u32 a = 0, b = members.len ? members.len - 1 : 0; a < b; a++, b--) {
            groove_NodeId t = members.ptr[a]; members.ptr[a] = members.ptr[b]; members.ptr[b] = t;
        }
        groove_NodeIdList *list = nodeid_list_from(members);
        jz_hmap_insert(&q.pipelines, &layout->nodes.ptr[tail], &list, NULL);
    }
    jz_free(predecessors);
    jz_free(interior);

    if (hydrate_sources) {
        for (u32 i = 0; i < layout->source_slots.len; i++) {
            usize slot = layout->source_slots.ptr[i];
            groove_NodeId node_id = layout->nodes.ptr[slot];
            const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, node_id);
            if (!node) { RT(EvaluationWorkQueue__drop)(&q); return groove_err_node_not_found(node_id); }
            bool some = false; groove_StorageRequestKey request; memset(&request, 0, sizeof request);
            jz_err e;
            switch (node->descriptor.op.tag) {
            case GROOVE_OP_TABLE_SOURCE: e = groove__ivm__runtime__operator_updates__NodeState__table_source_request(&node->descriptor.op.v.table_source, &some, &request); break;
            case GROOVE_OP_INDEX_SOURCE: e = groove__ivm__runtime__operator_updates__NodeState__index_source_request(&node->descriptor.op.v.index_source, &some, &request); break;
            default: jz_panic("layout source slot is not a storage source");
            }
            if (JZ_IS_ERR(e)) { RT(EvaluationWorkQueue__drop)(&q); return e; }
            if (some) {
                groove_EvaluationRequestKey key; memset(&key, 0, sizeof key);
                key.tag = GROOVE_ERK_STORAGE; key.storage = request;
                groove_NodeIdVec *deps = jz_btree_entry(&q.request_dependents, &key, NULL);
                jz_vec_push(deps, &node_id, sizeof node_id);
                q.entries.ptr[slot] = entry_waiting(1);
            }
        }
    }
    for (u32 slot = 0; slot < q.entries.len; slot++)
        if (entry_is(q.entries.ptr[slot], EE_WAITING) && q.entries.ptr[slot].waiting == 0) RT(EvaluationWorkQueue__make_slot_runnable)(&q, slot);
    *out = q;
    return JZ_OK;
}

static jz_err RT(EvaluationWorkQueue__discover)(const groove_IvmGraph *graph, const jz_hmap *node_meta, const groove_NodeId *roots, u32 nroots,
                                                bool hydrate_sources, jz_hmap *relevant, EvaluationWorkQueue *out) {
    JZ_TRY(RT(EvaluationWorkQueue__discover_frame)(graph, node_meta, roots, nroots, hydrate_sources, out));
    nset_init(relevant);
    for (u32 i = 0; i < out->layout->nodes.len; i++) nset_insert(relevant, out->layout->nodes.ptr[i]);
    return JZ_OK;
}

/* Dispatch with direct compiled input slots. */
static jz_err RT(EvaluationWorkQueue__poll_node)(EvaluationWorkQueue *q, groove_TickEvaluator *evaluator, groove_NodeId node, groove_RecordDeltas **out) {
    usize slot = layout_slot(q->layout, node);
    groove_NodeIdList **pipeline = jz_hmap_get(&q->pipelines, &node);
    if (pipeline) {
        usize head = layout_slot(q->layout, (*pipeline)->items[0]);
        jz_err err = JZ_OK;
        bool ready = groove__ivm__runtime__pipeline__TickEvaluator__poll_pipeline(evaluator, (*pipeline)->items, (*pipeline)->len,
            &q->pipeline_batches, layout_inputs(q->layout, head), &err, out);
        if (!ready) return groove_poll_pending();
        return err;
    }
    return TEV(poll_ready_node)(evaluator, node, &q->unary_batches, layout_inputs(q->layout, slot), out);
}

static void RT(EvaluationWorkQueue__slot_dependency_ready)(EvaluationWorkQueue *q, usize slot) {
    slot = q->task_slots.ptr[slot];
    EvaluationEntry *e = &q->entries.ptr[slot];
    if (!entry_is(*e, EE_WAITING)) return;
    if (e->waiting) e->waiting--;
    if (e->waiting == 0) RT(EvaluationWorkQueue__make_slot_runnable)(q, slot);
}

static void RT(EvaluationWorkQueue__dependency_ready)(EvaluationWorkQueue *q, groove_NodeId node) {
    const usize *slot = layout_slot_get(q->layout, node);
    if (slot) RT(EvaluationWorkQueue__slot_dependency_ready)(q, *slot);
}

/* `requests` are the keys of a ready map (EvaluationRequestKey -> output). */
static void RT(EvaluationWorkQueue__requests_ready)(EvaluationWorkQueue *q, const jz_btree *ready) {
    void *k, *v;
    JZ_BTREE_FOR(ready, k, v) {
        groove_EvaluationRequestKey removed_key; groove_NodeIdVec nodes;
        if (jz_btree_remove(&q->request_dependents, k, &removed_key, &nodes)) {
            q->request_dependents.d->key_drop(&removed_key);
            for (u32 i = 0; i < nodes.len; i++) RT(EvaluationWorkQueue__dependency_ready)(q, nodes.ptr[i]);
            jz_vec_drop(&nodes, sizeof(groove_NodeId), NULL);
        }
    }
}

typedef struct ResidentCtx { const jz_hmap *resident; groove_NodeIdVec *ready; } ResidentCtx;
static bool resident_keep(const void *k, void *v, void *ctx) {
    (void)k;
    ResidentCtx *c = ctx;
    groove_NodeIdVec *deps = v;
    u32 w = 0;
    for (u32 i = 0; i < deps->len; i++) {
        if (nset_contains(c->resident, deps->ptr[i])) jz_vec_push(c->ready, &deps->ptr[i], sizeof(groove_NodeId));
        else deps->ptr[w++] = deps->ptr[i];
    }
    deps->len = w;
    return w != 0;
}
static void RT(EvaluationWorkQueue__storage_already_resident)(EvaluationWorkQueue *q, const jz_hmap *resident_nodes) {
    groove_NodeIdVec ready = {0};
    ResidentCtx c = { resident_nodes, &ready };
    jz_btree_retain(&q->request_dependents, resident_keep, &c);
    for (u32 i = 0; i < ready.len; i++) RT(EvaluationWorkQueue__dependency_ready)(q, ready.ptr[i]);
    jz_vec_drop(&ready, sizeof(groove_NodeId), NULL);
}

/* consumes `requests` (BTreeSet<EvaluationRequestKey>) */
static void RT(EvaluationWorkQueue__wait_for_requests)(EvaluationWorkQueue *q, groove_NodeId node, jz_btree *requests) {
    q->entries.ptr[layout_slot(q->layout, node)] = entry_waiting(requests->len);
    groove_EvaluationRequestKey request;
    while (jz_btree_pop_first(requests, &request, NULL)) {
        groove_NodeIdVec *dependents = jz_btree_entry(&q->request_dependents, &request, NULL);
        bool found = false;
        for (u32 i = 0; i < dependents->len; i++) if (dependents->ptr[i].v == node.v) { found = true; break; }
        if (!found) jz_vec_push(dependents, &node, sizeof node);
    }
    jz_btree_drop(requests);
}

static void RT(EvaluationWorkQueue__complete)(EvaluationWorkQueue *q, groove_NodeId node) {
    usize slot = layout_slot(q->layout, node);
    /* Publish completion only after the entire task succeeds. In particular,
     * temporal waiters must not observe an interior prefix. */
    groove_NodeIdList **members = jz_hmap_get(&q->pipelines, &node);
    if (members) {
        for (u32 i = 0; i < (*members)->len; i++) {
            groove_NodeId member = (*members)->items[i];
            q->entries.ptr[layout_slot(q->layout, member)] = entry_of(EE_COMPLETE);
            if (!nset_contains(&q->released_by_earlier_frame, member)) jz_vec_push(&q->completed_events, &member, sizeof member);
        }
    } else if (!nset_contains(&q->released_by_earlier_frame, node)) {
        jz_vec_push(&q->completed_events, &node, sizeof node);
    }
    q->entries.ptr[slot] = entry_of(EE_COMPLETE);
    u32 nd = 0;
    const u32 *deps = layout_dependents(q->layout, slot, &nd);
    for (u32 i = 0; i < nd; i++) RT(EvaluationWorkQueue__slot_dependency_ready)(q, deps[i]);
}

/* Retain a runnable node after its private step deliberately yields. */
static void RT(EvaluationWorkQueue__requeue_yielded)(EvaluationWorkQueue *q, groove_NodeId node) {
    q->entries.ptr[layout_slot(q->layout, node)] = entry_of(EE_RUNNABLE);
    jz_deque_push_front(&q->runnable, &node);
}

static bool RT(EvaluationWorkQueue__has_resident_continuation)(const EvaluationWorkQueue *q) { return q->runnable.len != 0; }

static bool RT(EvaluationWorkQueue__is_root)(const EvaluationWorkQueue *q, groove_NodeId node) {
    u32 idx;
    return jz_bsearch(q->layout->roots.ptr, q->layout->roots.len, sizeof(groove_NodeId), &node, nodeid_cmp, NULL, &idx);
}

static bool RT(EvaluationWorkQueue__is_complete)(const EvaluationWorkQueue *q, groove_NodeId node) {
    const usize *slot = layout_slot_get(q->layout, node);
    return slot && entry_is(q->entries.ptr[*slot], EE_COMPLETE);
}

static bool RT(EvaluationWorkQueue__roots_complete)(const EvaluationWorkQueue *q) {
    for (u32 i = 0; i < q->layout->roots.len; i++)
        if (!RT(EvaluationWorkQueue__is_complete)(q, q->layout->roots.ptr[i])) return false;
    return true;
}

static void RT(EvaluationWorkQueue__incomplete_nodes)(const EvaluationWorkQueue *q, groove_NodeIdVec *out) {
    for (u32 slot = 0; slot < q->entries.len; slot++)
        if (!entry_is(q->entries.ptr[slot], EE_COMPLETE)) jz_vec_push(out, &q->layout->nodes.ptr[slot], sizeof(groove_NodeId));
}

/* A hydration's entire graph slice remains a temporal barrier until install. */
static void RT(EvaluationWorkQueue__registered_nodes)(const EvaluationWorkQueue *q, groove_NodeIdVec *out) {
    jz_vec_extend(out, q->layout->nodes.ptr, q->layout->nodes.len, sizeof(groove_NodeId));
}

static bool RT(EvaluationWorkQueue__overlaps)(const EvaluationWorkQueue *q, const jz_hmap *nodes) {
    for (u32 i = 0; i < q->layout->nodes.len; i++) if (nset_contains(nodes, q->layout->nodes.ptr[i])) return true;
    return false;
}

static void RT(EvaluationWorkQueue__downstream_closure)(const EvaluationWorkQueue *q, const groove_NodeId *roots, u32 n, jz_hmap *affected) {
    nset_init(affected);
    jz_deque pending; jz_deque_init(&pending, sizeof(groove_NodeId));
    for (u32 i = 0; i < n; i++) jz_deque_push_back(&pending, &roots[i]);
    groove_NodeId node;
    while (jz_deque_pop_front(&pending, &node)) {
        if (!nset_insert(affected, node)) continue;
        const usize *slot = layout_slot_get(q->layout, node);
        if (slot) {
            u32 nd = 0;
            const u32 *deps = layout_dependents(q->layout, *slot, &nd);
            for (u32 i = 0; i < nd; i++) jz_deque_push_back(&pending, &q->layout->nodes.ptr[deps[i]]);
        }
    }
    jz_deque_drop(&pending, NULL);
}

static EvaluationFailure RT(EvaluationWorkQueue__failure_for_request)(const EvaluationWorkQueue *q, const groove_EvaluationRequestKey *request, groove_EvaluationRequestFailure failure) {
    EvaluationFailure f;
    f.kind = failure.publication_metadata_durability ? EFK_FATAL : EFK_SCOPED;
    const groove_NodeIdVec *deps = jz_btree_get(&q->request_dependents, request);
    RT(EvaluationWorkQueue__downstream_closure)(q, deps ? deps->ptr : NULL, deps ? deps->len : 0, &f.affected_nodes);
    f.error = err_rc_new(failure.error);
    return f;
}

static EvaluationFailure RT(EvaluationWorkQueue__failure_for_node)(const EvaluationWorkQueue *q, groove_NodeId node, jz_err error) {
    EvaluationFailure f;
    f.kind = EFK_SCOPED;
    RT(EvaluationWorkQueue__downstream_closure)(q, &node, 1, &f.affected_nodes);
    f.error = err_rc_new(error);
    return f;
}

static bool keep_not_in_set_k(const void *k, void *v, void *ctx) { (void)v; return !nset_contains(ctx, *(const groove_NodeId *)k); }
static bool deps_not_all_in_set(const void *k, void *v, void *ctx) {
    (void)k;
    const groove_NodeIdVec *deps = v;
    for (u32 i = 0; i < deps->len; i++) if (!nset_contains(ctx, deps->ptr[i])) return true;
    return false;
}
static void deque_retain_nodes(jz_deque *q, bool (*keep)(groove_NodeId, void *), void *ctx) {
    u32 n = q->len;
    groove_NodeId node;
    for (u32 i = 0; i < n; i++) {
        jz_deque_pop_front(q, &node);
        if (keep(node, ctx)) jz_deque_push_back(q, &node);
    }
}
static bool node_not_in_set(groove_NodeId n, void *ctx) { return !nset_contains(ctx, n); }

static void RT(EvaluationWorkQueue__abandon)(EvaluationWorkQueue *q, const jz_hmap *nodes) {
    jz_hmap_retain(&q->unary_batches, keep_not_in_set_k, (void *)nodes);
    jz_hmap_retain(&q->pipeline_batches, keep_not_in_set_k, (void *)nodes);
    deque_retain_nodes(&q->runnable, node_not_in_set, (void *)nodes);
    jz_btree_retain(&q->request_dependents, deps_not_all_in_set, (void *)nodes);
    groove_NodeId *node; void *v;
    JZ_HMAP_FOR(nodes, node, v) {
        const usize *slot = layout_slot_get(q->layout, *node);
        if (!slot) continue;
        groove_NodeIdList **members = jz_hmap_get(&q->pipelines, node);
        if (members)
            for (u32 i = 0; i < (*members)->len; i++) q->entries.ptr[layout_slot(q->layout, (*members)->items[i])] = entry_of(EE_COMPLETE);
        q->entries.ptr[*slot] = entry_of(EE_COMPLETE);
        q->temporal_waiting.ptr[*slot] = 0;
    }
}

typedef struct NodeEq { groove_NodeId node; } NodeEq;
static bool node_ne(groove_NodeId n, void *ctx) { return n.v != ((NodeEq *)ctx)->node.v; }

static void RT(EvaluationWorkQueue__add_temporal_blockers)(EvaluationWorkQueue *q, const jz_hmap *blockers /* NodeId -> usize */) {
    for (u32 slot = 0; slot < q->entries.len; slot++) {
        groove_NodeId node = q->layout->nodes.ptr[slot];
        const usize *countp = jz_hmap_get(blockers, &node);
        usize count = countp ? *countp : 0;
        if (count == 0) continue;
        q->temporal_waiting.ptr[slot] = count;
        usize task_slot = q->task_slots.ptr[slot];
        EvaluationEntry e = q->entries.ptr[task_slot];
        switch (e.tag) {
        case EE_RUNNABLE: {
            NodeEq ctx = { q->layout->nodes.ptr[task_slot] };
            deque_retain_nodes(&q->runnable, node_ne, &ctx);
            q->entries.ptr[task_slot] = entry_waiting(count);
            break;
        }
        case EE_WAITING: q->entries.ptr[task_slot] = entry_waiting(e.waiting + count); break;
        case EE_COMPLETE: break;
        case EE_FUSED: jz_panic("canonical task slot");
        }
    }
}

/* Whether `node` still waits on a predecessor and has not been evaluated. */
static bool RT(EvaluationWorkQueue__is_temporally_waiting)(const EvaluationWorkQueue *q, groove_NodeId node) {
    const usize *slot = layout_slot_get(q->layout, node);
    return slot && q->temporal_waiting.ptr[*slot] > 0 && !entry_is(q->entries.ptr[q->task_slots.ptr[*slot]], EE_COMPLETE);
}

static void RT(EvaluationWorkQueue__temporal_ready)(EvaluationWorkQueue *q, groove_NodeId node) {
    const usize *slot = layout_slot_get(q->layout, node);
    if (!slot) return;
    if (q->temporal_waiting.ptr[*slot] == 0) return;
    q->temporal_waiting.ptr[*slot] -= 1;
    RT(EvaluationWorkQueue__slot_dependency_ready)(q, *slot);
}

static groove_NodeIdVec RT(EvaluationWorkQueue__drain_completed_events)(EvaluationWorkQueue *q) {
    groove_NodeIdVec out = q->completed_events;
    memset(&q->completed_events, 0, sizeof q->completed_events);
    return out;
}

static void RT(EvaluationWorkQueue__discard_unregistered_completions)(EvaluationWorkQueue *q) {
    q->completed_events.len = 0;
    jz_hmap_clear(&q->released_by_earlier_frame);
}

/* Nodes this frame has completed (or abandoned). */
static void RT(EvaluationWorkQueue__complete_nodes)(const EvaluationWorkQueue *q, groove_NodeIdVec *out) {
    for (u32 slot = 0; slot < q->entries.len; slot++)
        if (entry_is(q->entries.ptr[slot], EE_COMPLETE)) jz_vec_push(out, &q->layout->nodes.ptr[slot], sizeof(groove_NodeId));
}

typedef struct CompleteCtx { const EvaluationWorkQueue *q; } CompleteCtx;
static bool node_not_complete(groove_NodeId n, void *ctx) {
    const EvaluationWorkQueue *q = ctx;
    return !entry_is(q->entries.ptr[layout_slot(q->layout, n)], EE_COMPLETE);
}

/* Seed a later frame of the same evaluation with the earlier frame's
 * completed nodes (#3306). Consumes `released`. */
static void RT(EvaluationWorkQueue__carry_earlier_frame)(EvaluationWorkQueue *q, const jz_hmap *carried, jz_hmap released) {
    groove_UsizeVec tails = {0};
    for (usize slot = 0; slot < q->entries.len; slot++) {
        if (q->task_slots.ptr[slot] != slot) continue;
        groove_NodeIdList **members = jz_hmap_get(&q->pipelines, &q->layout->nodes.ptr[slot]);
        bool all;
        if (members) {
            all = true;
            for (u32 i = 0; i < (*members)->len; i++) if (!nset_contains(carried, (*members)->items[i])) { all = false; break; }
        } else {
            all = nset_contains(carried, q->layout->nodes.ptr[slot]);
        }
        if (all) jz_vec_push(&tails, &slot, sizeof slot);
    }
    /* Mark every carried task complete before releasing dependents, so a
     * carried dependent is never made runnable by a carried input. */
    for (u32 t = 0; t < tails.len; t++) {
        usize slot = tails.ptr[t];
        groove_NodeIdList **members = jz_hmap_get(&q->pipelines, &q->layout->nodes.ptr[slot]);
        if (members)
            for (u32 i = 0; i < (*members)->len; i++) q->entries.ptr[layout_slot(q->layout, (*members)->items[i])] = entry_of(EE_COMPLETE);
        q->entries.ptr[slot] = entry_of(EE_COMPLETE);
    }
    deque_retain_nodes(&q->runnable, node_not_complete, q);
    for (u32 t = 0; t < tails.len; t++) {
        u32 nd = 0;
        const u32 *deps = layout_dependents(q->layout, tails.ptr[t], &nd);
        for (u32 i = 0; i < nd; i++) RT(EvaluationWorkQueue__slot_dependency_ready)(q, deps[i]);
    }
    jz_vec_drop(&tails, sizeof(usize), NULL);
    jz_hmap_drop(&q->released_by_earlier_frame);
    q->released_by_earlier_frame = released;
}

/* ---------------------------------------------------------------------- */
/* Evaluation sessions and pending state                                  */
/* ---------------------------------------------------------------------- */
typedef struct EvaluationSession {
    jz_hmap relevant_nodes;            /* HashSet<NodeId> */
    jz_hmap roots;                     /* HashSet<NodeId> */
    jz_hmap outputs;                   /* NodeId -> RecordDeltas */
    jz_hmap pending_outputs;           /* NodeId -> Arc<RecordDeltas> */
    jz_hmap operator_states;           /* OperatorStateKey -> OperatorState */
    jz_hmap arrangement_states;        /* ArrangementKey -> AsOf<ArrangementState, SubTick> */
    jz_hmap arrangement_keys_by_input; /* NodeId -> HashSet<ArrangementKey> */
    groove_EvaluationMemo *eval_memo;
    usize eval_memo_bytes;
    u64 memo_use_clock;
    jz_hmap node_meta;                 /* NodeId -> NodeRuntimeMeta */
    /* Collector operations produced while hydrating the exact initial snapshot. */
    jz_hmap terminal_deltas;           /* NodeId -> TerminalDeltas */
    jz_hmap binding_frontiers;         /* BindingSourceKey -> u64 */
    groove_OwnedStorage storage;
    groove_EvaluationRequests requests;
    groove_EvaluationInputs evaluation_inputs;
    EvaluationWorkQueue work_queue;
    groove_RootIndirectValues root_indirect_values;
    /* Roots hydrated only to order another output's terminal. */
    jz_hmap ordering_only_roots;
    /* Nodes that stay owned by the live runtime rather than this session. */
    jz_hmap borrowed;
} EvaluationSession;

static void RT(EvaluationSession__drop)(EvaluationSession *s) {
    jz_hmap_drop(&s->relevant_nodes);
    jz_hmap_drop(&s->roots);
    jz_hmap_drop(&s->outputs);
    jz_hmap_drop(&s->pending_outputs);
    jz_hmap_drop(&s->operator_states);
    jz_hmap_drop(&s->arrangement_states);
    jz_hmap_drop(&s->arrangement_keys_by_input);
    if (s->eval_memo) MEMO(drop)(s->eval_memo);
    jz_hmap_drop(&s->node_meta);
    jz_hmap_drop(&s->terminal_deltas);
    jz_hmap_drop(&s->binding_frontiers);
    groove__storage__OwnedStorage__drop(&s->storage);
    groove_EvaluationRequests_drop(&s->requests);
    groove_EvaluationInputs_drop(&s->evaluation_inputs);
    RT(EvaluationWorkQueue__drop)(&s->work_queue);
    SB(RootIndirectValues__drop)(&s->root_indirect_values);
    jz_hmap_drop(&s->ordering_only_roots);
    jz_hmap_drop(&s->borrowed);
}

typedef struct IncrementalEvaluation {
    groove_TableDeltaVec table_deltas;
    groove_BindingDeltaVec binding_deltas;
    groove_BindingSnapshots *binding_snapshots; /* Arc */
    jz_hmap table_frontiers;   /* String -> u64 */
    jz_hmap binding_frontiers; /* BindingSourceKey -> u64 */
    u64 current_tick;
    groove_TickMetrics metrics;
    groove_OwnedStorage storage;
    groove_EvaluationRequests requests;
    bool has_evaluation_inputs; groove_EvaluationInputs evaluation_inputs;
    EvaluationWorkQueue work_queue;
    /* Graph slice whose state is staged by this evaluation. */
    jz_hmap *relevant_nodes; /* Arc<HashSet<NodeId>> */
    jz_hmap published_subscriptions;  /* HashSet<SubscriptionId> */
    jz_hmap affected_subscriptions;   /* HashSet<SubscriptionId> */
    /* Subscriptions whose hydration this write set aside (#3901). */
    jz_hmap restarted_subscriptions;  /* HashSet<SubscriptionId> */
    jz_hmap *affected_nodes; /* Arc<HashSet<NodeId>> */
    jz_hmap pending_subscription_outputs; /* NodeId -> PendingSubscriptionOutput */
    jz_hmap terminal_deltas;          /* NodeId -> TerminalDeltas */
    jz_hmap root_ordering_windows;    /* NodeId -> RootOrderingWindows */
    groove_OptPublicationId notification_publication;
    bool defer_notifications_until_durable;
    bool has_pending_resident_publication; groove_PendingResidentPublication pending_resident_publication;
    jz_hmap operator_states;
    jz_hmap arrangement_states;
    jz_hmap arrangement_keys_by_input;
    groove_EvaluationMemo *eval_memo;
    usize eval_memo_bytes;
    u64 memo_use_clock;
    jz_hmap node_meta;
    usize pending_binding_retractions;
    groove_NotificationVec pending_notifications;
    /* Derived writes use the same sparse overlay as their evaluation reads. */
    groove_StagedWriteState *durable_writes; /* Rc<RefCell<..>> */
    bool persist_flush; /* Option<PersistFlush>: the flush completes synchronously in this port */
    /* No independent root remains after a scoped failure. */
    bool discarded;
    /* Affected shared terminals whose route barriers await this tick's deltas (#3288). */
    groove_NodeIdVec routed_terminals;
    /* Completed nodes whose state this evaluation already wrote back (#3815). */
    jz_hmap early_installed;
    /* Completed nodes registered as temporal barriers instead of written back. */
    jz_hmap held_barriers;
} IncrementalEvaluation;

static void RT(IncrementalEvaluation__drop)(IncrementalEvaluation *e) {
    jz_vec_drop(&e->table_deltas, sizeof(groove_TableDelta), SB(TableDelta__drop));
    jz_vec_drop(&e->binding_deltas, sizeof(groove_BindingDelta), SB(BindingDelta__drop));
    jz_rc_release(e->binding_snapshots);
    jz_hmap_drop(&e->table_frontiers);
    jz_hmap_drop(&e->binding_frontiers);
    groove__ivm__runtime__state__TickMetrics__drop(&e->metrics);
    groove__storage__OwnedStorage__drop(&e->storage);
    groove_EvaluationRequests_drop(&e->requests);
    if (e->has_evaluation_inputs) groove_EvaluationInputs_drop(&e->evaluation_inputs);
    RT(EvaluationWorkQueue__drop)(&e->work_queue);
    jz_rc_release(e->relevant_nodes);
    jz_hmap_drop(&e->published_subscriptions);
    jz_hmap_drop(&e->affected_subscriptions);
    jz_hmap_drop(&e->restarted_subscriptions);
    jz_rc_release(e->affected_nodes);
    jz_hmap_drop(&e->pending_subscription_outputs);
    jz_hmap_drop(&e->terminal_deltas);
    jz_hmap_drop(&e->root_ordering_windows);
    if (e->has_pending_resident_publication) RT(PendingResidentPublication__drop)(&e->pending_resident_publication);
    jz_hmap_drop(&e->operator_states);
    jz_hmap_drop(&e->arrangement_states);
    jz_hmap_drop(&e->arrangement_keys_by_input);
    if (e->eval_memo) MEMO(drop)(e->eval_memo);
    jz_hmap_drop(&e->node_meta);
    RT(NotificationVec__drop)(&e->pending_notifications);
    jz_rc_release(e->durable_writes);
    jz_vec_drop(&e->routed_terminals, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&e->early_installed);
    jz_hmap_drop(&e->held_barriers);
}

typedef struct PendingSubscriptionHydration {
    groove_SubscriptionId subscription_id;
    jz_btree outputs; /* String -> CompiledNode */
    groove_InitialCell *initial; /* Arc<Mutex<Option<MultisinkDeltas>>> */
    EvaluationSession session;
    groove_BindingSnapshots *binding_snapshots; /* Arc */
    /* The prepared shape whose binding frontier this hydration advanced. */
    bool has_binding_frontier_advance; jz_str binding_frontier_advance;
    bool hydrate_arrangements;
    groove_SubscriptionLifetime lifetime;
    groove_TickMetrics metrics;
    bool session_consumed; /* the session was installed (moved into the runtime) */
} PendingSubscriptionHydration;

static void RT(PendingSubscriptionHydration__drop)(PendingSubscriptionHydration *h) {
    jz_btree_drop(&h->outputs);
    if (h->initial) jz_rc_release(h->initial);
    if (!h->session_consumed) RT(EvaluationSession__drop)(&h->session);
    if (h->binding_snapshots) jz_rc_release(h->binding_snapshots);
    if (h->has_binding_frontier_advance) jz_str_drop(&h->binding_frontier_advance);
    groove__ivm__runtime__state__TickMetrics__drop(&h->metrics);
}

enum { PE_INCREMENTAL, PE_SUBSCRIPTION_HYDRATION };
typedef struct PendingEvaluation {
    u32 tag;
    IncrementalEvaluation *incremental;         /* boxed */
    PendingSubscriptionHydration *hydration;    /* boxed */
} PendingEvaluation;

static void RT(PendingEvaluation__drop)(PendingEvaluation *e) {
    if (e->tag == PE_INCREMENTAL) { RT(IncrementalEvaluation__drop)(e->incremental); jz_free(e->incremental); }
    else { RT(PendingSubscriptionHydration__drop)(e->hydration); jz_free(e->hydration); }
}
static void pending_evaluation_drop_v(void *p) { RT(PendingEvaluation__drop)(p); }

static EvaluationWorkQueue *RT(PendingEvaluation__work_queue_mut)(PendingEvaluation *e) {
    return e->tag == PE_INCREMENTAL ? &e->incremental->work_queue : &e->hydration->session.work_queue;
}
static const EvaluationWorkQueue *RT(PendingEvaluation__work_queue)(const PendingEvaluation *e) {
    return e->tag == PE_INCREMENTAL ? &e->incremental->work_queue : &e->hydration->session.work_queue;
}
static bool RT(PendingEvaluation__has_resident_continuation)(const PendingEvaluation *e) {
    return RT(EvaluationWorkQueue__has_resident_continuation)(RT(PendingEvaluation__work_queue)(e));
}

static void RT(IncrementalEvaluation__restage_released_node)(IncrementalEvaluation *self, const groove_IvmRuntime *runtime, groove_NodeId node);

/* Release this evaluation's temporal wait on `node`. */
static void RT(PendingEvaluation__temporal_ready)(PendingEvaluation *self, const groove_IvmRuntime *runtime, groove_NodeId node) {
    if (self->tag == PE_INCREMENTAL && RT(EvaluationWorkQueue__is_temporally_waiting)(&self->incremental->work_queue, node))
        RT(IncrementalEvaluation__restage_released_node)(self->incremental, runtime, node);
    RT(EvaluationWorkQueue__temporal_ready)(RT(PendingEvaluation__work_queue_mut)(self), node);
}

static const jz_btree_desc desc_u64_pending_eval = { sizeof(u64), sizeof(PendingEvaluation), jz_cmp_u64, NULL, pending_evaluation_drop_v };
static void deque_u64_drop_v(void *p) { jz_deque_drop(p, NULL); }
static const jz_hmap_desc hdesc_waiters = { sizeof(groove_NodeId), sizeof(jz_deque), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, deque_u64_drop_v };

struct groove_PendingIncrementalState {
    jz_btree evaluations;     /* u64 -> PendingEvaluation */
    jz_deque order;           /* VecDeque<u64> */
    jz_hmap waiters_by_node;  /* NodeId -> VecDeque<u64> */
    u64 next_id;
};
typedef struct groove_PendingIncrementalState PendingIncrementalState;

static void RT(PendingIncrementalState__default)(PendingIncrementalState *s) {
    jz_btree_init(&s->evaluations, &desc_u64_pending_eval);
    jz_deque_init(&s->order, sizeof(u64));
    jz_hmap_init(&s->waiters_by_node, &hdesc_waiters);
    s->next_id = 0;
}
static void RT(PendingIncrementalState__drop)(PendingIncrementalState *s) {
    jz_btree_drop(&s->evaluations);
    jz_deque_drop(&s->order, NULL);
    jz_hmap_drop(&s->waiters_by_node);
}

static u64 hash_u64_v(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool eq_u64_v(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static const jz_hmap_desc hdesc_id_set = { sizeof(u64), 0, hash_u64_v, eq_u64_v, NULL, NULL };
static bool idset_insert(jz_hmap *s, u64 id) { return !jz_hmap_insert(s, &id, NULL, NULL); }
static bool idset_contains(const jz_hmap *s, u64 id) { return jz_hmap_get(s, &id) != NULL; }

/* Select each hydration whose snapshot overlaps `nodes`, plus every evaluation
 * which is temporally ahead of it on any registered node. Repeat to closure. */
static void RT(PendingIncrementalState__hydration_admission_evaluations)(const PendingIncrementalState *self, const jz_hmap *nodes, jz_hmap *selected) {
    jz_hmap_init(selected, &hdesc_id_set);
    u64 *id; PendingEvaluation *evaluation;
    JZ_BTREE_FOR(&self->evaluations, id, evaluation) {
        if (evaluation->tag == PE_SUBSCRIPTION_HYDRATION && RT(EvaluationWorkQueue__overlaps)(RT(PendingEvaluation__work_queue)(evaluation), nodes))
            idset_insert(selected, *id);
    }
    for (;;) {
        bool changed = false;
        groove_NodeId *node; jz_deque *waiters;
        JZ_HMAP_FOR(&self->waiters_by_node, node, waiters) {
            i64 last_selected = -1;
            for (i64 i = (i64)waiters->len - 1; i >= 0; i--)
                if (idset_contains(selected, *(u64 *)jz_deque_at(waiters, (u32)i))) { last_selected = i; break; }
            if (last_selected < 0) continue;
            for (i64 i = 0; i <= last_selected; i++) changed |= idset_insert(selected, *(u64 *)jz_deque_at(waiters, (u32)i));
        }
        if (!changed) return;
    }
}

/* Release evaluations queued behind `evaluation_id` at completed graph nodes. */
static void RT(PendingIncrementalState__release_temporal_successors)(PendingIncrementalState *self, const groove_IvmRuntime *runtime, u64 evaluation_id,
                                                                    const groove_NodeId *nodes, u32 n) {
    (void)evaluation_id;
    for (u32 i = 0; i < n; i++) {
        groove_NodeId node = nodes[i];
        jz_deque *waiters = jz_hmap_get(&self->waiters_by_node, &node);
        if (!waiters) continue;
        u64 front;
        jz_deque_pop_front(waiters, &front);
        bool has_successor = waiters->len != 0;
        u64 successor = has_successor ? *(u64 *)jz_deque_at(waiters, 0) : 0;
        if (waiters->len == 0) jz_hmap_remove(&self->waiters_by_node, &node, NULL, NULL);
        if (has_successor) {
            PendingEvaluation *later = jz_btree_get(&self->evaluations, &successor);
            if (later) RT(PendingEvaluation__temporal_ready)(later, runtime, node);
        }
    }
}

PendingIncrementalState *RT(PendingIncrementalEvaluation__default)(void) {
    PendingIncrementalState *s = JZ_NEW(PendingIncrementalState);
    RT(PendingIncrementalState__default)(s);
    return s;
}
void RT(PendingIncrementalEvaluation__drop)(PendingIncrementalState *p) { if (p) { RT(PendingIncrementalState__drop)(p); jz_free(p); } }
bool RT(PendingIncrementalEvaluation__is_pending)(const PendingIncrementalState *p) { return p && p->order.len != 0; }
static bool RT(PendingIncrementalEvaluation__has_resident_continuation)(const PendingIncrementalState *p) {
    if (!p) return false;
    u64 *id; PendingEvaluation *e;
    JZ_BTREE_FOR(&p->evaluations, id, e) if (RT(PendingEvaluation__has_resident_continuation)(e)) return true;
    return false;
}
/* Nodes referenced by queued continuations remain graph-live until the
 * continuation completes or is cancelled. */
void RT(PendingIncrementalEvaluation__registered_nodes)(const PendingIncrementalState *p, jz_hmap *out) {
    nset_init(out);
    if (!p) return;
    u64 *id; PendingEvaluation *e;
    JZ_BTREE_FOR(&p->evaluations, id, e) {
        groove_NodeIdVec nodes = {0};
        RT(EvaluationWorkQueue__registered_nodes)(RT(PendingEvaluation__work_queue)(e), &nodes);
        for (u32 i = 0; i < nodes.len; i++) nset_insert(out, nodes.ptr[i]);
        jz_vec_drop(&nodes, sizeof(groove_NodeId), NULL);
    }
}

static PendingIncrementalState *pending_state(groove_IvmRuntime *rt) {
    if (!rt->pending_incremental) rt->pending_incremental = RT(PendingIncrementalEvaluation__default)();
    return rt->pending_incremental;
}


/* ---------------------------------------------------------------------- */
/* free functions used by the evaluations                                 */
/* ---------------------------------------------------------------------- */
static groove_ScopeId scope_root(void) { return groove__ivm__runtime__state__ScopeId__root(); }

/* The component a written-back arrangement belongs to: its recursive owner
 * for a child scope, else its own `Arrange` node. */
static groove_NodeId RT(arrangement_owner)(const groove_ArrangementKey *key) {
    groove_NodeId owner;
    if (groove__ivm__runtime__state__ScopeId__outermost(key->scope, &owner)) return owner;
    return key->input;
}

static void retainer_clone_v(void *dst, const void *src) {
    groove_Retainer *d = dst; const groove_Retainer *s = src;
    d->tag = s->tag; d->key = jz_str_clone(&s->key);
}

/* Fold graph-lifecycle state into an evaluation's `node_meta` snapshot just
 * before it replaces the live entries. */
static void RT(carry_live_node_lifecycle)(jz_hmap *snapshot, const groove_IvmRuntime *runtime, const jz_hmap *nodes) {
    groove_NodeId *node; void *unused;
    JZ_HMAP_FOR(nodes, node, unused) {
        groove_NodeRuntimeMeta *meta = jz_hmap_get(snapshot, node);
        const groove_NodeRuntimeMeta *live = jz_hmap_get(&runtime->node_meta, node);
        if (meta && live) {
            jz_hmap_drop(&meta->retainers);
            hmap_clone(&meta->retainers, &live->retainers, retainer_clone_v, NULL);
            if (live->input_generation > meta->input_generation) meta->input_generation = live->input_generation;
        } else if (!meta && live) {
            groove_NodeRuntimeMeta copy;
            groove_NodeRuntimeMeta_clone(&copy, live);
            jz_hmap_insert(snapshot, node, &copy, NULL);
        } else if (meta && !live && !groove__ivm__graph__IvmGraph__node(runtime->graph, *node)) {
            jz_hmap_remove(snapshot, node, NULL, NULL);
        }
    }
}

/* Fold an operator state's uncommitted overlays into its base. */
static void RT(commit_operator_state)(groove_OperatorState *state) {
    switch (state->tag) {
    case GROOVE_OS_RECURSIVE: groove__ivm__runtime__recursion__RecursiveState__commit_staged_positive(state->v.recursive.value); break;
    case GROOVE_OS_TOP_BY: groove__ivm__runtime__evaluator__TopByIncrementalState__commit_overlays(&state->v.top_by.value); break;
    case GROOVE_OS_ARG_BY: groove__ivm__runtime__evaluator__SparseGroups__commit_overlay(&state->v.arg_by.value); break;
    case GROOVE_OS_SEMI_JOIN: groove__ivm__runtime__join__SemiJoinState__commit_published_overlay(state->v.semi_join); break;
    case GROOVE_OS_ANTI_JOIN: groove__ivm__runtime__join__AntiJoinState__commit_published_overlay(state->v.anti_join); break;
    case GROOVE_OS_COLLECT_BY:
        groove__ivm__runtime__evaluator__SparseGroups__commit_overlay(&groove__ivm__runtime__evaluator__CollectByIncrementalState__deref_mut(&state->v.collect_by)->groups);
        break;
    case GROOVE_OS_STATELESS: case GROOVE_OS_JOIN: case GROOVE_OS_STREAMING_CHECKSUM: case GROOVE_OS_LOOKUP: break;
    }
}

static void RT(bump_input_frontiers_staged)(const groove_IvmGraph *graph, const groove_TableDelta *table_deltas, u32 ntd,
                                           const groove_BindingDelta *binding_deltas, u32 nbd, jz_hmap *table_frontiers,
                                           jz_hmap *binding_frontiers, jz_hmap *node_meta) {
    JZ_VEC(jz_str) changed_tables = {0};
    for (u32 i = 0; i < ntd; i++) {
        const groove_TableDelta *delta = &table_deltas[i];
        if (delta->deltas.len == 0) continue;
        jz_str key = jz_str_clone(&delta->table);
        u64 *frontier = jz_hmap_entry(table_frontiers, &key, NULL);
        *frontier += 1;
        jz_str borrowed = { delta->table.ptr, delta->table.len, 0 };
        jz_vec_push(&changed_tables, &borrowed, sizeof borrowed);
    }
    groove_BindingSourceKeyVec changed_bindings = {0};
    for (u32 i = 0; i < nbd; i++) {
        const groove_BindingDelta *delta = &binding_deltas[i];
        if (delta->deltas.len == 0 && !delta->initializes_snapshot) continue;
        groove_BindingSourceKey key; groove__ivm__op_types__BindingSourceKey__clone(&key, &delta->key);
        u64 *frontier = jz_hmap_entry(binding_frontiers, &key, NULL);
        *frontier += 1;
        jz_vec_push(&changed_bindings, &delta->key, sizeof delta->key); /* shallow borrow */
    }
    if (changed_tables.len == 0 && changed_bindings.len == 0) {
        jz_vec_drop(&changed_tables, sizeof(jz_str), NULL);
        jz_vec_drop(&changed_bindings, sizeof(groove_BindingSourceKey), NULL);
        return;
    }
    jz_hmap *affected = groove__ivm__graph__IvmGraph__affected_nodes(graph, changed_tables.ptr, changed_tables.len, changed_bindings.ptr, changed_bindings.len);
    groove_NodeId *node; void *unused;
    JZ_HMAP_FOR(affected, node, unused) {
        groove_NodeRuntimeMeta *meta = groove_ivm_rt_a_node_meta_entry(node_meta, *node);
        meta->input_generation = meta->input_generation + 1;
    }
    jz_rc_release(affected);
    jz_vec_drop(&changed_tables, sizeof(jz_str), NULL);
    jz_vec_drop(&changed_bindings, sizeof(groove_BindingSourceKey), NULL);
}

/* Route barriers reached by this tick's shared-terminal deltas (#3288). Any
 * terminal whose delta cannot be read or keyed conservatively touches all of
 * its barriers, which is exactly the unrouted activation. */
static void touch_all_barriers(const groove_RouteTable *table, jz_hmap *touched) {
    groove_NodeIdVec barriers = {0};
    groove__ivm__routes__RouteTable__barriers(table, &barriers);
    for (u32 i = 0; i < barriers.len; i++) nset_insert(touched, barriers.ptr[i]);
    jz_vec_drop(&barriers, sizeof(groove_NodeId), NULL);
}

static void RT(touched_route_barriers)(groove_TickEvaluator *evaluator, const groove_IvmGraph *graph, const groove_NodeId *routed, u32 n, jz_hmap *touched) {
    nset_init(touched);
    const groove_RouteIndex *routes = groove__ivm__graph__IvmGraph__routes(graph);
    for (u32 i = 0; i < n; i++) {
        groove_NodeId terminal = routed[i];
        const groove_RouteTable *table = groove__ivm__routes__RouteIndex__table(routes, terminal);
        if (!table) continue;
        groove_RecordDeltas *records = NULL;
        jz_err e = TEV(update_node)(evaluator, terminal, &records);
        if (e.code) { jz_err_drop(&e); records = NULL; }
        /* Only the route fields key a barrier. */
        if (records) {
            groove_RecordDeltas *materialized = NULL;
            e = TEV(materialize_indirect_field_indices)(evaluator, records, table->field_indices.ptr, table->field_indices.len, &materialized);
            jz_rc_release(records);
            if (e.code) { jz_err_drop(&e); materialized = NULL; }
            records = materialized;
        }
        if (!records) { touch_all_barriers(table, touched); continue; }
        /* Route fields are indices into the terminal's compiled output. */
        const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, terminal);
        bool layout_matches = node && groove_NodeOutput_records(node->descriptor.output) == records->descriptor;
        if (!layout_matches) { touch_all_barriers(table, touched); jz_rc_release(records); continue; }
        for (u32 d = 0; d < records->deltas.len; d++) {
            groove_BorrowedRecord record = groove_RecordDelta_borrowed(&records->deltas.ptr[d], records->descriptor);
            jz_bytes key = {0};
            if (groove__ivm__routes__RouteTable__key_of_record(table, &record, &key)) {
                const jz_hmap *barriers = jz_hmap_get(&table->by_key, &key);
                if (barriers) {
                    groove_NodeId *b; void *unused;
                    JZ_HMAP_FOR(barriers, b, unused) nset_insert(touched, *b);
                }
                jz_bytes_drop(&key);
            } else {
                touch_all_barriers(table, touched);
                break;
            }
        }
        jz_rc_release(records);
    }
}

/* ---------------------------------------------------------------------- */
/* IncrementalEvaluation                                                  */
/* ---------------------------------------------------------------------- */
static void RT(IncrementalEvaluation__evaluator)(IncrementalEvaluation *self, const groove_IvmRuntime *runtime, groove_TickEvaluator *ev) {
    memset(ev, 0, sizeof *ev);
    ev->schema = &runtime->schema;
    ev->graph = runtime->graph;
    ev->variant_projections = &runtime->variant_projections;
    ev->table_deltas = self->table_deltas.ptr; ev->n_table_deltas = self->table_deltas.len;
    ev->binding_deltas = self->binding_deltas.ptr; ev->n_binding_deltas = self->binding_deltas.len;
    ev->binding_snapshots = self->binding_snapshots;
    ev->current_tick = self->current_tick;
    ev->operator_states = &self->operator_states;
    ev->arrangement_states = &self->arrangement_states;
    ev->arrangement_keys_by_input = &self->arrangement_keys_by_input;
    ev->eval_memo = self->eval_memo;
    ev->eval_memo_bytes = &self->eval_memo_bytes;
    ev->table_frontiers = &self->table_frontiers;
    ev->binding_frontiers = &self->binding_frontiers;
    ev->memo_use_clock = &self->memo_use_clock;
    ev->node_meta = &self->node_meta;
    ev->storage = self->storage.storage;
    ev->evaluation_inputs = self->has_evaluation_inputs ? &self->evaluation_inputs : NULL;
    groove__ivm__runtime__state__EvalContext__root(&ev->context);
    ev->metrics = &self->metrics;
    /* std::mem::take(&mut self.terminal_deltas) / root_ordering_windows */
    ev->terminal_deltas = self->terminal_deltas;
    ev->root_ordering_windows = self->root_ordering_windows;
    jz_hmap_init(&self->terminal_deltas, &groove_ivm_rt_a_hdesc_terminal_deltas);
    jz_hmap_init(&self->root_ordering_windows, &groove_ivm_rt_a_hdesc_root_windows);
}

/* self.terminal_deltas = take(evaluator.terminal_deltas); same for windows */
static void RT(IncrementalEvaluation__take_back)(IncrementalEvaluation *self, groove_TickEvaluator *ev) {
    jz_hmap_drop(&self->terminal_deltas);
    jz_hmap_drop(&self->root_ordering_windows);
    self->terminal_deltas = ev->terminal_deltas;
    self->root_ordering_windows = ev->root_ordering_windows;
    TEV(init_maps)(ev);
}

static void stage_runtime_node_state(jz_hmap *operator_states, jz_hmap *arrangement_states, jz_hmap *arrangement_keys_by_input,
                                     const groove_IvmRuntime *runtime, groove_NodeId node) {
    groove_OperatorStateKey key = { scope_root(), node };
    const groove_OperatorState *state = jz_hmap_get(&runtime->operator_states, &key);
    if (state) {
        groove_OperatorState copy; groove__ivm__runtime__evaluator__OperatorState__clone(&copy, state);
        jz_hmap_insert(operator_states, &key, &copy, NULL);
    }
    const jz_hmap *keys = jz_hmap_get(&runtime->arrangement_keys_by_input, &node);
    if (keys) {
        groove_ArrangementKey *akey; void *unused;
        JZ_HMAP_FOR(keys, akey, unused) {
            const groove_AsOfArrangement *astate = jz_hmap_get(&runtime->arrangement_states, akey);
            if (!astate) continue;
            groove_AsOfArrangement copy; groove__ivm__runtime__join__AsOfArrangement__clone(&copy, astate);
            jz_hmap_insert(arrangement_states, akey, &copy, NULL);
            bool inserted = false;
            jz_hmap *set = jz_hmap_entry(arrangement_keys_by_input, &node, &inserted);
            if (inserted) jz_hmap_init(set, &groove_ivm_rt_ARRANGEMENT_KEY_SET);
            jz_hmap_insert(set, akey, NULL, NULL);
        }
    }
}

/* The first frame staged state only for the nodes its activation plan
 * reached; barriers activated now must bring their live state in first. */
static jz_err RT(IncrementalEvaluation__stage_newly_relevant_state)(IncrementalEvaluation *self, const groove_IvmRuntime *runtime, const jz_hmap *closure) {
    groove_NodeIdVec roots = {0};
    nset_to_vec(closure, &roots);
    groove_ExecutionLayout *layout = NULL; groove_NodeId missing;
    bool ok = groove__ivm__graph__IvmGraph__execution_layout(runtime->graph, roots.ptr, roots.len, &layout, &missing);
    jz_vec_drop(&roots, sizeof(groove_NodeId), NULL);
    if (!ok) return groove_err_node_not_found(missing);
    jz_hmap *relevant = nset_make_mut(&self->relevant_nodes);
    for (u32 i = 0; i < layout->nodes.len; i++) {
        groove_NodeId node = layout->nodes.ptr[i];
        if (!nset_insert(relevant, node)) continue;
        stage_runtime_node_state(&self->operator_states, &self->arrangement_states, &self->arrangement_keys_by_input, runtime, node);
        const groove_NodeRuntimeMeta *meta = jz_hmap_get(&runtime->node_meta, &node);
        if (meta && !jz_hmap_get(&self->node_meta, &node)) {
            groove_NodeRuntimeMeta copy; groove_NodeRuntimeMeta_clone(&copy, meta);
            jz_hmap_insert(&self->node_meta, &node, &copy, NULL);
        }
    }
    jz_rc_release(layout);
    return JZ_OK;
}

static jz_err RT(EvaluationWorkQueue__discover_frame)(const groove_IvmGraph *graph, const jz_hmap *node_meta, const groove_NodeId *roots, u32 nroots,
                                                      bool hydrate_sources, EvaluationWorkQueue *out);

static bool retainer_is_not_hydration(const jz_hmap *retainers) {
    groove_Retainer *r; void *unused;
    JZ_HMAP_FOR(retainers, r, unused) if (r->tag != GROOVE_RET_HYDRATION) return true;
    return false;
}

/* Second frame of a routed tick (#3288): activate exactly the route barriers
 * the shared terminals' deltas reached, with their downstream closure. Consumes `touched`. */
static jz_err RT(IncrementalEvaluation__activate_route_barriers)(IncrementalEvaluation *self, const groove_IvmRuntime *runtime, jz_hmap touched) {
    groove_NodeIdVec touched_vec = {0};
    nset_to_vec(&touched, &touched_vec);
    jz_hmap_drop(&touched);
    jz_hmap closure;
    groove__ivm__graph__IvmGraph__downstream_through_routes(runtime->graph, touched_vec.ptr, touched_vec.len, &closure);
    jz_vec_drop(&touched_vec, sizeof(groove_NodeId), NULL);
    jz_err e = RT(IncrementalEvaluation__stage_newly_relevant_state)(self, runtime, &closure);
    if (e.code) { jz_hmap_drop(&closure); return e; }
    groove_NodeIdVec roots = {0};
    jz_vec_extend(&roots, self->work_queue.layout->roots.ptr, self->work_queue.layout->roots.len, sizeof(groove_NodeId));
    groove_NodeId *node; void *unused;
    JZ_HMAP_FOR(&closure, node, unused) {
        groove_NodeRuntimeMeta meta;
        const groove_NodeRuntimeMeta *staged = jz_hmap_get(&self->node_meta, node);
        if (!staged) staged = jz_hmap_get(&runtime->node_meta, node);
        if (staged) groove_NodeRuntimeMeta_clone(&meta, staged);
        else groove__ivm__runtime__operator_updates__NodeRuntimeMeta__default(&meta);
        meta.input_generation = meta.input_generation + 1;
        if (retainer_is_not_hydration(&meta.retainers)) jz_vec_push(&roots, node, sizeof *node);
        jz_hmap_insert(&self->node_meta, node, &meta, NULL);
        const jz_hmap *subs = jz_hmap_get(&runtime->subscriptions_by_output_node, node);
        if (!subs) continue;
        groove_SubscriptionId *sid; void *u2;
        JZ_HMAP_FOR(subs, sid, u2) {
            if (jz_hmap_get(&self->restarted_subscriptions, sid)) continue;
            if (!jz_hmap_insert(&self->affected_subscriptions, sid, NULL, NULL)) self->metrics.subscriptions_considered += 1;
            const groove_MultisinkSubscriptionState *state = jz_hmap_get(&runtime->multisink_subscriptions, sid);
            if (!state) continue;
            jz_str *sink; groove_CompiledNode *output;
            JZ_BTREE_FOR(&state->outputs, sink, output)
                if (output->node.v == node->v && output->has_root_ordering_node) jz_vec_push(&roots, &output->root_ordering_node, sizeof(groove_NodeId));
        }
    }
    jz_hmap *affected = nset_make_mut(&self->affected_nodes);
    jz_hmap *relevant = nset_make_mut(&self->relevant_nodes);
    JZ_HMAP_FOR(&closure, node, unused) { nset_insert(affected, *node); nset_insert(relevant, *node); }
    jz_sort_unstable(roots.ptr, roots.len, sizeof(groove_NodeId), nodeid_cmp, NULL);
    u32 w = 0;
    for (u32 i = 0; i < roots.len; i++) if (w == 0 || roots.ptr[w - 1].v != roots.ptr[i].v) roots.ptr[w++] = roots.ptr[i];
    roots.len = w;
    EvaluationWorkQueue queue;
    e = RT(EvaluationWorkQueue__discover_frame)(runtime->graph, &runtime->node_meta, roots.ptr, roots.len, false, &queue);
    jz_vec_drop(&roots, sizeof(groove_NodeId), NULL);
    if (e.code) { jz_hmap_drop(&closure); return e; }
    /* The first frame's completions were (or will be) released to temporal
     * successors exactly once (#3306). Only nodes the barriers reach are re-evaluated. */
    groove_NodeIdVec complete = {0};
    RT(EvaluationWorkQueue__complete_nodes)(&self->work_queue, &complete);
    jz_hmap released, carried;
    nset_init(&released); nset_init(&carried);
    for (u32 i = 0; i < complete.len; i++) {
        nset_insert(&released, complete.ptr[i]);
        if (!nset_contains(&closure, complete.ptr[i])) nset_insert(&carried, complete.ptr[i]);
    }
    jz_vec_drop(&complete, sizeof(groove_NodeId), NULL);
    RT(EvaluationWorkQueue__carry_earlier_frame)(&queue, &carried, released);
    jz_hmap_drop(&carried);
    jz_vec_drop(&queue.completed_events, sizeof(groove_NodeId), NULL);
    queue.completed_events = RT(EvaluationWorkQueue__drain_completed_events)(&self->work_queue);
    MEMO(set_layout)(self->eval_memo, jz_rc_clone(queue.layout));
    RT(EvaluationWorkQueue__drop)(&self->work_queue);
    self->work_queue = queue;
    jz_hmap_drop(&closure);
    return JZ_OK;
}

/* A started direct flush has no rollback path. The flush runs to its outcome
 * synchronously here, so the PersistFlushAttempt drop guard never fires. */
static u32 RT(IncrementalEvaluation__poll_storage_flush)(IncrementalEvaluation *self, bool *indeterminate, const groove_OwnedStorage *extraction_storage,
                                                        const jz_waker *cx, jz_err *result) {
    (void)cx;
    *result = JZ_OK;
    if (!self->persist_flush && !groove__storage__StagedWriteState__is_empty(self->durable_writes)) {
        groove_OwnedWriteOperationVec operations = groove__storage__StagedWriteState__into_operations(self->durable_writes);
        const groove_OrderedKvStorage *storage = extraction_storage ? extraction_storage->storage : self->storage.storage;
        self->persist_flush = true;
        bool completed = false;
        jz_err error = JZ_OK;
        switch (groove__storage__OrderedKvStorage__write_many_outcome(storage, operations, &error)) {
        case GROOVE_WMO_COMMITTED: break;
        case GROOVE_WMO_UNCOMMITTED: *result = error; break;
        case GROOVE_WMO_POSSIBLY_COMMITTED: *indeterminate = true; *result = error; break;
        }
        completed = true;
        if (!completed) *indeterminate = true;
        self->persist_flush = false;
    }
    return 0;
}

/* Publish base-table frontiers monotonically when this evaluation parks. */
static void RT(IncrementalEvaluation__install_input_frontiers)(const IncrementalEvaluation *self, groove_IvmRuntime *runtime) {
    jz_str *table; u64 *frontier;
    JZ_HMAP_FOR(&self->table_frontiers, table, frontier) {
        jz_str key = jz_str_clone(table);
        bool inserted = false;
        u64 *live = jz_hmap_entry(&runtime->table_frontiers, &key, &inserted);
        if (inserted || *live < *frontier) *live = *frontier;
    }
    groove_BindingSourceKey *binding;
    JZ_HMAP_FOR(&self->binding_frontiers, binding, frontier) {
        groove_BindingSourceKey key; groove__ivm__op_types__BindingSourceKey__clone(&key, binding);
        bool inserted = false;
        u64 *live = jz_hmap_entry(&runtime->binding_frontiers, &key, &inserted);
        if (inserted || *live < *frontier) *live = *frontier;
    }
    groove_NodeId *node; groove_NodeRuntimeMeta *staged;
    JZ_HMAP_FOR(&self->node_meta, node, staged) {
        groove_NodeRuntimeMeta *live = jz_hmap_get(&runtime->node_meta, node);
        if (live && staged->input_generation > live->input_generation) live->input_generation = staged->input_generation;
    }
}

static bool op_key_not_in(const void *k, void *v, void *ctx) { (void)v; return !nset_contains(ctx, ((const groove_OperatorStateKey *)k)->node); }
static bool arr_key_not_in(const void *k, void *v, void *ctx) { (void)v; return !nset_contains(ctx, ((const groove_ArrangementKey *)k)->input); }
static bool memo_key_not_in(const groove_EvalMemoKey *k, groove_EvalMemoEntry *e, void *ctx) { (void)e; return !nset_contains(ctx, k->node); }
static bool nodeset_key_not_in(const void *k, void *v, void *ctx) { (void)v; return !nset_contains(ctx, *(const groove_NodeId *)k); }

static void RT(IncrementalEvaluation__abandon)(IncrementalEvaluation *self, const jz_hmap *nodes) {
    /* An empty set is the explicit cancellation path. */
    bool all_roots = true;
    for (u32 i = 0; i < self->work_queue.layout->roots.len; i++)
        if (!nset_contains(nodes, self->work_queue.layout->roots.ptr[i])) { all_roots = false; break; }
    self->discarded = nodes->len == 0 || all_roots;
    RT(EvaluationWorkQueue__abandon)(&self->work_queue, nodes);
    jz_hmap_retain(&self->operator_states, op_key_not_in, (void *)nodes);
    jz_hmap_retain(&self->arrangement_states, arr_key_not_in, (void *)nodes);
    jz_hmap_retain(&self->arrangement_keys_by_input, nodeset_key_not_in, (void *)nodes);
    MEMO(retain)(self->eval_memo, memo_key_not_in, (void *)nodes);
    self->eval_memo_bytes = memo_payload_bytes(self->eval_memo);
    jz_hmap_retain(&self->node_meta, nodeset_key_not_in, (void *)nodes);
    jz_hmap_retain(nset_make_mut(&self->relevant_nodes), nodeset_key_not_in, (void *)nodes);
    jz_hmap_retain(nset_make_mut(&self->affected_nodes), nodeset_key_not_in, (void *)nodes);
    jz_hmap_retain(&self->terminal_deltas, nodeset_key_not_in, (void *)nodes);
    jz_hmap_retain(&self->root_ordering_windows, nodeset_key_not_in, (void *)nodes);
    jz_hmap_retain(&self->pending_subscription_outputs, nodeset_key_not_in, (void *)nodes);
}

/* Whether a completed node's state may be handed to later evaluations while
 * this one stays parked. */
static bool RT(IncrementalEvaluation__writes_back)(const IncrementalEvaluation *self, const groove_IvmRuntime *runtime, groove_NodeId node) {
    if (self->routed_terminals.len != 0) return false;
    const groove_GraphNode *graph_node = groove__ivm__graph__IvmGraph__node(runtime->graph, node);
    return !(graph_node && graph_node->descriptor.op.tag == GROOVE_OP_RECURSIVE);
}

static void RT(IncrementalEvaluation__install_completed_nodes)(IncrementalEvaluation *self, groove_IvmRuntime *runtime, const groove_NodeId *nodes, u32 n);

/* Hand completed `nodes` to later evaluations while this one stays parked
 * (#3815). Returns the written-back nodes; the others are held. Consumes `nodes`. */
static groove_NodeIdVec RT(IncrementalEvaluation__write_back_or_hold)(IncrementalEvaluation *self, groove_IvmRuntime *runtime, groove_NodeIdVec nodes) {
    groove_NodeIdVec written = {0};
    for (u32 i = 0; i < nodes.len; i++) {
        if (RT(IncrementalEvaluation__writes_back)(self, runtime, nodes.ptr[i])) jz_vec_push(&written, &nodes.ptr[i], sizeof(groove_NodeId));
        else nset_insert(&self->held_barriers, nodes.ptr[i]);
    }
    jz_vec_drop(&nodes, sizeof(groove_NodeId), NULL);
    RT(IncrementalEvaluation__install_completed_nodes)(self, runtime, written.ptr, written.len);
    return written;
}

/* Write the state of completed `nodes` back to `runtime` while the rest of this
 * evaluation stays parked (#3815). */
static void RT(IncrementalEvaluation__install_completed_nodes)(IncrementalEvaluation *self, groove_IvmRuntime *runtime, const groove_NodeId *nodes, u32 n) {
    if (self->discarded) return;
    jz_hmap installed; nset_init(&installed);
    groove_ScopeId root = scope_root();
    for (u32 i = 0; i < n; i++) {
        groove_NodeId node = nodes[i];
        if (!nset_insert(&self->early_installed, node)) continue;
        nset_insert(&installed, node);
        groove_OperatorStateKey key = { root, node };
        const groove_OperatorState *state = jz_hmap_get(&self->operator_states, &key);
        if (state) {
            groove_OperatorState copy; groove__ivm__runtime__evaluator__OperatorState__clone(&copy, state);
            jz_hmap_insert(&runtime->operator_states, &key, &copy, NULL);
        }
        const jz_hmap *keys = jz_hmap_get(&self->arrangement_keys_by_input, &node);
        if (keys) {
            jz_hmap live_keys; jz_hmap_init(&live_keys, &groove_ivm_rt_ARRANGEMENT_KEY_SET);
            groove_ArrangementKey *akey; void *unused;
            JZ_HMAP_FOR(keys, akey, unused) {
                if (akey->scope != root) continue;
                const groove_AsOfArrangement *astate = jz_hmap_get(&self->arrangement_states, akey);
                if (!astate) continue;
                groove_AsOfArrangement copy; groove__ivm__runtime__join__AsOfArrangement__clone(&copy, astate);
                /* The producer's transition belongs to this tick. */
                groove__ivm__runtime__join__ArrangementState__release_changes(copy.value);
                jz_hmap_insert(&runtime->arrangement_states, akey, &copy, NULL);
                jz_hmap_insert(&live_keys, akey, NULL, NULL);
            }
            if (live_keys.len) {
                bool inserted = false;
                jz_hmap *set = jz_hmap_entry(&runtime->arrangement_keys_by_input, &node, &inserted);
                if (inserted) jz_hmap_init(set, &groove_ivm_rt_ARRANGEMENT_KEY_SET);
                JZ_HMAP_FOR(&live_keys, akey, unused) jz_hmap_insert(set, akey, NULL, NULL);
            }
            jz_hmap_drop(&live_keys);
        }
    }
    if (installed.len == 0) { jz_hmap_drop(&installed); return; }
    jz_hmap node_meta; jz_hmap_init(&node_meta, self->node_meta.d);
    groove_NodeId *node; void *unused;
    JZ_HMAP_FOR(&installed, node, unused) {
        const groove_NodeRuntimeMeta *meta = jz_hmap_get(&self->node_meta, node);
        if (!meta) continue;
        groove_NodeRuntimeMeta copy; groove_NodeRuntimeMeta_clone(&copy, meta);
        jz_hmap_insert(&node_meta, node, &copy, NULL);
    }
    RT(carry_live_node_lifecycle)(&node_meta, runtime, &installed);
    hmap_move_into(&runtime->node_meta, &node_meta);
    jz_hmap_drop(&node_meta);
    jz_hmap_drop(&installed);
}

typedef struct OwnedByCtx { groove_NodeId node; } OwnedByCtx;
static bool arr_not_owned(const void *k, void *v, void *ctx) { (void)v; return RT(arrangement_owner)(k).v != ((OwnedByCtx *)ctx)->node.v; }
static bool memo_not_owned(const groove_EvalMemoKey *k, groove_EvalMemoEntry *e, void *ctx) {
    (void)e;
    groove_NodeId node = ((OwnedByCtx *)ctx)->node, outer;
    if (k->node.v == node.v) return false;
    return !(groove__ivm__runtime__state__ScopeId__outermost(k->scope, &outer) && outer.v == node.v);
}

/* Replace the state staged for `node` with the live runtime's, once the
 * predecessor this evaluation waited on has written it back (#3815). */
static void RT(IncrementalEvaluation__restage_released_node)(IncrementalEvaluation *self, const groove_IvmRuntime *runtime, groove_NodeId node) {
    groove_OperatorStateKey key = { scope_root(), node };
    const groove_OperatorState *state = jz_hmap_get(&runtime->operator_states, &key);
    if (state) {
        groove_OperatorState copy; groove__ivm__runtime__evaluator__OperatorState__clone(&copy, state);
        jz_hmap_insert(&self->operator_states, &key, &copy, NULL);
    } else {
        jz_hmap_remove(&self->operator_states, &key, NULL, NULL);
    }
    OwnedByCtx ctx = { node };
    jz_hmap_retain(&self->arrangement_states, arr_not_owned, &ctx);
    groove_NodeId *input; jz_hmap *keys;
    JZ_HMAP_FOR(&self->arrangement_keys_by_input, input, keys) jz_hmap_retain(keys, arr_not_owned, &ctx);
    /* Child-scope arrangements are keyed by their body inputs, so find them by owner. */
    groove_ArrangementKey *akey; groove_AsOfArrangement *astate;
    JZ_HMAP_FOR(&runtime->arrangement_states, akey, astate) {
        if (RT(arrangement_owner)(akey).v != node.v) continue;
        groove_AsOfArrangement copy; groove__ivm__runtime__join__AsOfArrangement__clone(&copy, astate);
        jz_hmap_insert(&self->arrangement_states, akey, &copy, NULL);
        bool inserted = false;
        jz_hmap *set = jz_hmap_entry(&self->arrangement_keys_by_input, &akey->input, &inserted);
        if (inserted) jz_hmap_init(set, &groove_ivm_rt_ARRANGEMENT_KEY_SET);
        jz_hmap_insert(set, akey, NULL, NULL);
    }
    jz_hmap single; nset_init(&single); nset_insert(&single, node);
    RT(carry_live_node_lifecycle)(&self->node_meta, runtime, &single);
    jz_hmap_drop(&single);
    MEMO(retain)(self->eval_memo, memo_not_owned, &ctx);
    self->eval_memo_bytes = memo_payload_bytes(self->eval_memo);
}

static bool op_key_is_root(const void *k, void *v, void *ctx) { (void)v; (void)ctx; return ((const groove_OperatorStateKey *)k)->scope == scope_root(); }
static bool arr_owner_not_in(const void *k, void *v, void *ctx) { (void)v; return !nset_contains(ctx, RT(arrangement_owner)(k)); }
static bool arr_owner_in(const void *k, void *v, void *ctx) { (void)v; return nset_contains(ctx, RT(arrangement_owner)(k)); }

static void RT(IncrementalEvaluation__install)(IncrementalEvaluation *self, groove_IvmRuntime *runtime) {
    if (self->discarded) return;
    /* Root ordering snapshots share TopBy group state. Release them before folding overlays. */
    jz_hmap_clear(&self->root_ordering_windows);
    /* Installed operator state is root-scoped; recursive child scopes are scratch. */
    jz_hmap_retain(&self->operator_states, op_key_is_root, NULL);
    /* Nodes written back while this evaluation was parked may since have been
     * advanced by a later evaluation (#3815). */
    jz_hmap early_installed = self->early_installed;
    nset_init(&self->early_installed);
    if (early_installed.len) {
        jz_hmap_retain(&self->operator_states, op_key_not_in, &early_installed);
        jz_hmap_retain(&self->arrangement_states, arr_owner_not_in, &early_installed);
        groove_NodeId *input; jz_hmap *keys;
        JZ_HMAP_FOR(&self->arrangement_keys_by_input, input, keys) jz_hmap_retain(keys, arr_owner_not_in, &early_installed);
        jz_hmap_retain(&self->node_meta, nodeset_key_not_in, &early_installed);
    }
    /* Drop the committed entries before folding staged COW state. */
    groove_OperatorStateKey *okey; groove_OperatorState *ostate;
    JZ_HMAP_FOR(&self->operator_states, okey, ostate) {
        jz_hmap_remove(&runtime->operator_states, okey, NULL, NULL);
        RT(commit_operator_state)(ostate);
    }
    hmap_move_into(&runtime->operator_states, &self->operator_states);

    groove_ArrangementKey *akey; groove_AsOfArrangement *astate;
    JZ_HMAP_FOR(&self->arrangement_states, akey, astate) {
        jz_hmap_remove(&runtime->arrangement_states, akey, NULL, NULL);
        groove__ivm__runtime__join__ArrangementState__commit_overlay(astate->value);
    }
    hmap_move_into(&runtime->arrangement_states, &self->arrangement_states);
    /* Each input's key set is replaced, except for the keys of nodes written back earlier. */
    groove_NodeId *input; jz_hmap *keys;
    JZ_HMAP_FOR(&self->arrangement_keys_by_input, input, keys) {
        jz_hmap *live = jz_hmap_get(&runtime->arrangement_keys_by_input, input);
        if (live && early_installed.len) {
            jz_hmap_retain(live, arr_owner_in, &early_installed);
            groove_ArrangementKey *k; void *unused;
            JZ_HMAP_FOR(keys, k, unused) jz_hmap_insert(live, k, NULL, NULL);
        } else {
            jz_hmap copy; hmap_clone(&copy, keys, NULL, NULL);
            jz_hmap_insert(&runtime->arrangement_keys_by_input, input, &copy, NULL);
        }
    }
    jz_hmap_clear(&self->arrangement_keys_by_input);
    jz_hmap_drop(&self->early_installed);
    self->early_installed = early_installed;

    /* Tick deltas belong to the frame, not the retained hydration cache. */
    groove_EvaluationMemoIter it; MEMO(iter)(self->eval_memo, &it);
    const groove_EvalMemoKey *mkey; groove_EvalMemoEntry *entry;
    while (groove__ivm__runtime__evaluation_memo__EvaluationMemoIter__next(&it, &mkey, &entry)) {
        if (mkey->has_tick_epoch) continue;
        groove_EvalMemoEntry copy = *entry;
        if (copy.records) jz_rc_clone(copy.records);
        groove__ivm__runtime__IvmRuntime__insert_retained_eval_memo(runtime, *mkey, copy);
    }
    MEMO(drop)(self->eval_memo);
    self->eval_memo = MEMO(default)();
    if (self->memo_use_clock > runtime->memo_use_clock) runtime->memo_use_clock = self->memo_use_clock;
    RT(carry_live_node_lifecycle)(&self->node_meta, runtime, self->relevant_nodes);
    hmap_move_into(&runtime->node_meta, &self->node_meta);
    RT(IncrementalEvaluation__install_input_frontiers)(self, runtime);
    if (self->current_tick > runtime->current_tick) runtime->current_tick = self->current_tick;
    /* runtime.pending_binding_retractions.drain(..self.pending_binding_retractions) */
    groove_BindingDeltaVec *pending = &runtime->pending_binding_retractions;
    usize drain = self->pending_binding_retractions < pending->len ? self->pending_binding_retractions : pending->len;
    for (usize i = 0; i < drain; i++) SB(BindingDelta__drop)(&pending->ptr[i]);
    memmove(pending->ptr, pending->ptr + drain, (pending->len - drain) * sizeof(groove_BindingDelta));
    pending->len -= (u32)drain;
}


/* Registers every missing input of `evaluator` with `requests`. Returns false
 * when nothing is missing. Sets *registered when any request was new. */
static bool register_missing_requests(groove_TickEvaluator *evaluator, groove_EvaluationRequests *requests, const groove_OrderedKvStorage *storage,
                                      const void *chunks, const groove_DatabaseSchema *schema, bool *registered, jz_btree *out_requests) {
    if (evaluator->evaluation_inputs) ES(EvaluationInputs__take_missing)(evaluator->evaluation_inputs, out_requests);
    else jz_btree_init(out_requests, &groove_ivm_rt_REQUEST_KEY_SET);
    if (out_requests->len == 0) { jz_btree_drop(out_requests); return false; }
    void *k, *v;
    JZ_BTREE_FOR(out_requests, k, v) {
        groove_EvaluationRequestKey key; ES(EvaluationRequestKey__clone)(&key, k);
        bool r = ES(EvaluationRequests__request)(requests, key, storage, chunks, schema);
        if (registered) *registered |= r;
    }
    return true;
}

static const void *IncrementalEvaluation_chunks(const IncrementalEvaluation *self, const groove_IvmRuntime *runtime) {
    if (self->has_pending_resident_publication && self->pending_resident_publication.chunk_provider) return self->pending_resident_publication.chunk_provider;
    return runtime->chunk_provider;
}

typedef struct PreparedOutput { const jz_str *sink; const groove_CompiledNode *output; groove_RecordDeltas *physical; groove_RecordDeltas *records; } PreparedOutput;
typedef JZ_VEC(PreparedOutput) PreparedOutputVec;
static void prepared_output_drop(void *p) { PreparedOutput *o = p; jz_rc_release(o->physical); jz_rc_release(o->records); }

enum { PUB_DONE, PUB_FAIL, PUB_BLOCKED };

/* Build one subscription's sinks and terminal sinks from its prepared outputs. */
static jz_err publish_prepared_outputs(IncrementalEvaluation *self, groove_TickEvaluator *evaluator, PreparedOutputVec *prepared,
                                       jz_hmap *terminal_consumers, groove_MultisinkDeltas *out) {
    SB(MultisinkDeltas__default)(out);
    for (u32 i = 0; i < prepared->len; i++) {
        PreparedOutput *p = &prepared->ptr[i];
        const groove_CompiledNode *output = p->output;
        if (p->records->deltas.len && !groove__records__RecordDescriptor__registry_compatible_with(p->records->descriptor, output->output))
            return groove_err_output_mismatch();
        bool structured = false, public_root = false;
        JZ_TRY(TEV(output_is_structured_collect_by)(evaluator, output->node, &structured));
        JZ_TRY(TEV(output_has_public_root)(evaluator, output->node, &public_root));
        bool terminal_owned = output->has_root_ordering_node || structured;
        bool has_identity = false;
        groove_RootIdentity identity; memset(&identity, 0, sizeof identity);
        if (output->has_root_ordering_node && !structured)
            JZ_TRY(groove__ivm__runtime__record_projection__root_identity_fields(evaluator->graph, output->node, output->root_ordering_node, &has_identity, &identity));
        groove_RecordDeltas records; SB(RecordDeltas__clone)(&records, p->records);
        /* Only the groups this output's own deltas reach take part in its root
         * ordering; the group fields lead the identity. */
        jz_btree identity_groups; jz_btree_init(&identity_groups, &groove_ivm_rt_a_desc_bytes_set);
        jz_err e = JZ_OK;
        if (has_identity) {
            for (u32 d = 0; d < p->physical->deltas.len && !e.code; d++) {
                const groove_RecordDelta *delta = &p->physical->deltas.ptr[d];
                jz_bytes part = {0};
                e = groove__ivm__runtime__windows__encoded_identity_key_part(p->physical->descriptor, delta->record.ptr, delta->record.len,
                                                                            identity.fields.ptr, (u32)identity.group_len, &part);
                if (!e.code) jz_btree_insert(&identity_groups, &part, NULL, NULL);
            }
        }
        if (!e.code && terminal_owned) {
            bool has_terminal = false;
            groove_TerminalDeltas terminal; memset(&terminal, 0, sizeof terminal);
            if (structured) {
                bool has_node = false; groove_NodeId tnode;
                e = TEV(terminal_delta_node_for_output)(evaluator, output->node, &has_node, &tnode);
                if (!e.code) {
                    if (has_node) {
                        usize *remaining = jz_hmap_get(terminal_consumers, &tnode);
                        if (!remaining) jz_panic("terminal consumer counted before publication");
                        *remaining -= 1;
                        has_terminal = TEV(terminal_deltas_for_consumer)(evaluator, tnode, *remaining == 0, &terminal);
                    } else if (!public_root && records.deltas.len) {
                        e = groove__ivm__runtime__terminal__terminal_deltas_from_record_deltas(&records, &terminal);
                        has_terminal = !e.code;
                    } else if (output->has_root_ordering_node) {
                        has_terminal = true;
                    }
                }
            } else if (records.deltas.len) {
                e = has_identity
                    ? groove__ivm__runtime__terminal__terminal_deltas_keyed_by_identity(&records, p->physical, identity.fields.ptr, identity.fields.len, &terminal)
                    : groove__ivm__runtime__terminal__terminal_deltas_from_record_deltas(&records, &terminal);
                has_terminal = !e.code;
            } else if (output->has_root_ordering_node) {
                has_terminal = true;
            }
            if (!e.code && has_terminal) {
                /* A CollectBy root terminal already owns exact occurrence keys
                 * and positional edits. */
                if (!structured && output->has_root_ordering_node)
                    e = TEV(apply_root_ordering)(evaluator, output->root_ordering_node, output->output,
                                                 has_identity ? &identity : NULL, has_identity ? &identity_groups : NULL, &terminal);
                if (!e.code && !groove__ivm__runtime__terminal__TerminalDeltas__is_empty(&terminal)) {
                    jz_str sink = jz_str_clone(p->sink);
                    jz_btree_insert(&out->terminal_sinks, &sink, &terminal, NULL);
                } else {
                    groove__ivm__runtime__terminal__TerminalDeltas__drop(&terminal);
                }
            }
        }
        jz_btree_drop(&identity_groups);
        if (has_identity) groove__ivm__runtime__record_projection__RootIdentity__drop(&identity);
        if (e.code) { SB(RecordDeltas__drop)(&records); return e; }
        if (records.deltas.len) {
            jz_str sink = jz_str_clone(p->sink);
            jz_btree_insert(&out->sinks, &sink, &records, NULL);
        } else {
            SB(RecordDeltas__drop)(&records);
        }
    }
    return JZ_OK;
}

static bool awaits_route_barriers(bool routes_pending, const groove_MultisinkSubscriptionState *s) {
    return routes_pending && s->target.tag == GROOVE_MST_ROUTED_SHAPE && s->target.route_barriers.len != 0;
}

/* Publish one affected subscription. PUB_BLOCKED: requests were registered and
 * the caller must poll them; PUB_FAIL: *err is set. */
static u32 publish_subscription(IncrementalEvaluation *self, groove_IvmRuntime *runtime, groove_TickEvaluator *evaluator,
                                groove_SubscriptionId subscription_id, const groove_MultisinkSubscriptionState *subscription,
                                jz_hmap *terminal_consumers, jz_err *err) {
    PreparedOutputVec prepared = {0};
    jz_str *sink; groove_CompiledNode *output;
    JZ_BTREE_FOR(&subscription->outputs, sink, output) {
        if (!nset_contains(self->affected_nodes, output->node)) continue;
        groove_RecordDeltas *physical = NULL, *materialized = NULL;
        PendingSubscriptionOutput *pending = jz_hmap_get(&self->pending_subscription_outputs, &output->node);
        if (pending) {
            physical = jz_rc_clone(pending->physical);
            materialized = RT(PendingSubscriptionOutput__materialized_as)(pending, &subscription->root_indirect_values);
        } else {
            jz_err e = TEV(update_node)(evaluator, output->node, &physical);
            if (e.code == GROOVE_POLL_PENDING) { jz_err_drop(&e); e = groove_err_evaluation_blocked(); }
            if (e.code) { jz_vec_drop(&prepared, sizeof(PreparedOutput), prepared_output_drop); *err = e; return PUB_FAIL; }
            PendingSubscriptionOutput p = RT(PendingSubscriptionOutput__new)(jz_rc_clone(physical));
            jz_hmap_insert(&self->pending_subscription_outputs, &output->node, &p, NULL);
        }
        /* Each subscription publishes in the representation its initial snapshot used. */
        jz_err e = JZ_OK;
        if (!materialized) {
            groove_UsizeVec fields = {0};
            if (!SB(RootIndirectValues__materialized_field_indices)(&subscription->root_indirect_values, physical->descriptor, &fields))
                e = TEV(materialize_indirect_input)(evaluator, physical, &materialized);
            else
                e = TEV(materialize_indirect_field_indices)(evaluator, physical, fields.ptr, fields.len, &materialized);
            jz_vec_drop(&fields, sizeof(usize), NULL);
            if (!e.code) {
                pending = jz_hmap_get(&self->pending_subscription_outputs, &output->node);
                if (pending) RT(PendingSubscriptionOutput__record_materialized)(pending, &subscription->root_indirect_values, jz_rc_clone(materialized));
            }
        }
        if (e.code == GROOVE_IVM_ERR_EVALUATION_BLOCKED) {
            jz_err_drop(&e);
            jz_rc_release(physical);
            jz_vec_drop(&prepared, sizeof(PreparedOutput), prepared_output_drop);
            jz_btree requests;
            if (!register_missing_requests(evaluator, &self->requests, self->storage.storage, IncrementalEvaluation_chunks(self, runtime),
                                           &runtime->schema, NULL, &requests)) {
                *err = groove_err_evaluation_blocked();
                return PUB_FAIL;
            }
            jz_btree_drop(&requests);
            return PUB_BLOCKED;
        }
        if (e.code) { jz_rc_release(physical); jz_vec_drop(&prepared, sizeof(PreparedOutput), prepared_output_drop); *err = e; return PUB_FAIL; }
        PreparedOutput po = { sink, output, physical, materialized };
        jz_vec_push(&prepared, &po, sizeof po);
    }

    groove_MultisinkDeltas records;
    jz_err e = publish_prepared_outputs(self, evaluator, &prepared, terminal_consumers, &records);
    jz_vec_drop(&prepared, sizeof(PreparedOutput), prepared_output_drop);
    if (e.code) { SB(MultisinkDeltas__drop)(&records); *err = e; return PUB_FAIL; }
    if (!SB(MultisinkDeltas__is_empty)(&records)) {
        evaluator->metrics->notifications_sent += 1;
        evaluator->metrics->notification_records += SB(multisink_deltas_record_count)(&records);
        evaluator->metrics->notification_encoded_bytes += SB(multisink_deltas_encoded_bytes)(&records);
    }
    groove_QueuedMultisinkDeltas queued = SB(QueuedMultisinkDeltas__new)(records);
    groove_OptPublicationId publication = self->notification_publication;
    if (self->has_pending_resident_publication && self->pending_resident_publication.publication->some)
        publication = *self->pending_resident_publication.publication;
    queued.has_publication = publication.some;
    queued.publication = publication.id;
    if (!SB(MultisinkDeltas__is_empty)(&queued.deltas)) {
        groove_SubscriptionNotification n = { subscription_id, queued };
        if (self->has_pending_resident_publication && !queued.has_publication) {
            /* A resident publication may have an independent cold branch still
             * parked; hand completed notifications to the resident receipt now. */
            jz_vec_push(self->pending_resident_publication.notifications, &n, sizeof n);
        } else {
            jz_vec_push(&self->pending_notifications, &n, sizeof n);
        }
    } else {
        SB(QueuedMultisinkDeltas__drop)(&queued);
    }
    jz_hmap_insert(&self->published_subscriptions, &subscription_id, NULL, NULL);
    return PUB_DONE;
}

static void RT(IvmRuntime__evict_eval_memo)(groove_IvmRuntime *self);

static u32 RT(IncrementalEvaluation__poll)(IncrementalEvaluation *self, groove_IvmRuntime *runtime, const jz_waker *cx, EvaluationFailure *fail) {
    fail->error = NULL;
    if (self->discarded) return 0;
    u32 ready = ES(EvaluationRequests__poll)(&self->requests);
    if (ready == 0) ES(EvaluationRequests__poll_eager_retry)(&self->requests);
    jz_btree ready_map;
    groove_EvaluationRequestKey failed_key; groove_EvaluationRequestFailure failure;
    if (!ES(EvaluationRequests__drain_ready)(&self->requests, &ready_map, &failed_key, &failure)) {
        *fail = RT(EvaluationWorkQueue__failure_for_request)(&self->work_queue, &failed_key, failure);
        groove_ivm_rt_REQUEST_KEY_SET.key_drop(&failed_key);
        return 0;
    }
    RT(EvaluationWorkQueue__requests_ready)(&self->work_queue, &ready_map);
    if (self->has_evaluation_inputs) ES(EvaluationInputs__install)(&self->evaluation_inputs, &ready_map);
    else jz_btree_drop(&ready_map);

    JZ_VEC(groove_SubscriptionId) dropped_subscriptions = {0};
    groove_TickEvaluator evaluator;
    RT(IncrementalEvaluation__evaluator)(self, runtime, &evaluator);
    const void *chunks = IncrementalEvaluation_chunks(self, runtime);

    bool registered_requests = false;
    groove_NodeId node;
    while (jz_deque_pop_front(&self->work_queue.runnable, &node)) {
        groove_RecordDeltas *result = NULL;
        jz_err e = RT(EvaluationWorkQueue__poll_node)(&self->work_queue, &evaluator, node, &result);
        if (e.code == 0) {
            if (result) jz_rc_release(result);
            RT(EvaluationWorkQueue__complete)(&self->work_queue, node);
        } else if (e.code == GROOVE_IVM_ERR_EVALUATION_BLOCKED) {
            jz_err_drop(&e);
            jz_btree requests;
            if (!register_missing_requests(&evaluator, &self->requests, self->storage.storage, chunks, &runtime->schema, &registered_requests, &requests)) {
                *fail = RT(EvaluationWorkQueue__failure_for_node)(&self->work_queue, node, groove_err_evaluation_blocked());
                TEV(drop)(&evaluator);
                return 0;
            }
            RT(EvaluationWorkQueue__wait_for_requests)(&self->work_queue, node, &requests);
        } else if (e.code == GROOVE_POLL_PENDING) {
            jz_err_drop(&e);
            RT(EvaluationWorkQueue__requeue_yielded)(&self->work_queue, node);
            RT(IncrementalEvaluation__take_back)(self, &evaluator);
            TEV(drop)(&evaluator);
            wake(cx);
            return GROOVE_POLL_PENDING;
        } else {
            *fail = RT(EvaluationWorkQueue__failure_for_node)(&self->work_queue, node, e);
            TEV(drop)(&evaluator);
            return 0;
        }
    }
    u32 immediately_ready = registered_requests ? ES(EvaluationRequests__poll)(&self->requests) : 0;
    if (registered_requests && immediately_ready == 0) {
        /* Retry only requests whose backend attests a yielded read is still executor-local. */
        immediately_ready = ES(EvaluationRequests__poll_eager_retry)(&self->requests);
    }
    if (immediately_ready > 0) {
        /* Resident requests completed synchronously; resume within this poll. */
        RT(IncrementalEvaluation__take_back)(self, &evaluator);
        TEV(drop)(&evaluator);
        return RT(IncrementalEvaluation__poll)(self, runtime, cx, fail);
    }
    if (self->routed_terminals.len && RT(EvaluationWorkQueue__roots_complete)(&self->work_queue)) {
        groove_NodeIdVec routed = self->routed_terminals;
        memset(&self->routed_terminals, 0, sizeof self->routed_terminals);
        jz_hmap touched;
        RT(touched_route_barriers)(&evaluator, runtime->graph, routed.ptr, routed.len, &touched);
        jz_vec_drop(&routed, sizeof(groove_NodeId), NULL);
        /* A barrier that reaches durable state was already activated and evaluated in the first frame. */
        jz_hmap_retain(&touched, nodeset_key_not_in, self->affected_nodes);
        if (touched.len) {
            RT(IncrementalEvaluation__take_back)(self, &evaluator);
            TEV(drop)(&evaluator);
            jz_err e = RT(IncrementalEvaluation__activate_route_barriers)(self, runtime, touched);
            if (e.code) { *fail = RT(EvaluationFailure__From__from)(e); return 0; }
            return RT(IncrementalEvaluation__poll)(self, runtime, cx, fail);
        }
        jz_hmap_drop(&touched);
    }

    /* Until phase B has activated this tick's route barriers, a routed
     * subscription's barrier-gated sinks have not produced their deltas (#3288). */
    bool routes_pending = self->routed_terminals.len != 0;
    jz_hmap terminal_consumers; jz_hmap_init(&terminal_consumers, &hdesc_node_usize);
    groove_SubscriptionId *sid; void *unused;
    JZ_HMAP_FOR(&self->affected_subscriptions, sid, unused) {
        const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&runtime->multisink_subscriptions, sid);
        if (!subscription) continue;
        if (subscription->failed || jz_hmap_get(&self->published_subscriptions, sid) || awaits_route_barriers(routes_pending, subscription)) continue;
        jz_str *sink; groove_CompiledNode *output;
        JZ_BTREE_FOR(&subscription->outputs, sink, output) {
            if (!nset_contains(self->affected_nodes, output->node)) continue;
            bool has = false; groove_NodeId tnode;
            jz_err e = TEV(terminal_delta_node_for_output)(&evaluator, output->node, &has, &tnode);
            if (e.code) { jz_hmap_drop(&terminal_consumers); TEV(drop)(&evaluator); *fail = RT(EvaluationFailure__From__from)(e); return 0; }
            if (has) { usize *count = jz_hmap_entry(&terminal_consumers, &tnode, NULL); *count += 1; }
        }
    }

    JZ_HMAP_FOR(&self->affected_subscriptions, sid, unused) {
        const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&runtime->multisink_subscriptions, sid);
        if (!subscription) continue;
        if (subscription->failed || jz_hmap_get(&self->published_subscriptions, sid) || awaits_route_barriers(routes_pending, subscription)) continue;
        bool incomplete = false;
        jz_str *sink; groove_CompiledNode *output;
        JZ_BTREE_FOR(&subscription->outputs, sink, output) {
            if (!nset_contains(self->affected_nodes, output->node)) continue;
            if (!RT(EvaluationWorkQueue__is_complete)(&self->work_queue, output->node)
                || (output->has_root_ordering_node && !RT(EvaluationWorkQueue__is_complete)(&self->work_queue, output->root_ordering_node))) {
                incomplete = true;
                break;
            }
        }
        if (incomplete) continue;
        jz_err e = JZ_OK;
        switch (publish_subscription(self, runtime, &evaluator, *sid, subscription, &terminal_consumers, &e)) {
        case PUB_DONE: break;
        case PUB_FAIL:
            jz_hmap_drop(&terminal_consumers);
            RT(IncrementalEvaluation__take_back)(self, &evaluator);
            TEV(drop)(&evaluator);
            *fail = RT(EvaluationFailure__From__from)(e);
            return 0;
        case PUB_BLOCKED: {
            jz_hmap_drop(&terminal_consumers);
            RT(IncrementalEvaluation__take_back)(self, &evaluator);
            TEV(drop)(&evaluator);
            u32 r = ES(EvaluationRequests__poll)(&self->requests);
            /* Retry only a backend that explicitly attests its reads are executor-local. */
            if (r == 0) r = ES(EvaluationRequests__poll_eager_retry)(&self->requests);
            if (r > 0) return RT(IncrementalEvaluation__poll)(self, runtime, cx, fail);
            return GROOVE_POLL_PENDING;
        }
        }
    }
    jz_hmap_drop(&terminal_consumers);
    RT(IncrementalEvaluation__take_back)(self, &evaluator);

    if (ES(EvaluationRequests__has_pending)(&self->requests) || !RT(EvaluationWorkQueue__roots_complete)(&self->work_queue)) {
        TEV(drop)(&evaluator);
        return GROOVE_POLL_PENDING;
    }
    TEV(drop)(&evaluator);
    jz_err flush = JZ_OK;
    if (RT(IncrementalEvaluation__poll_storage_flush)(self, runtime->persistence_indeterminate, NULL, cx, &flush) != 0) return GROOVE_POLL_PENDING;
    if (flush.code) { *fail = RT(EvaluationFailure__From__from)(flush); return 0; }
    RT(IncrementalEvaluation__install)(self, runtime);

    groove_NotificationVec notifications = self->pending_notifications;
    memset(&self->pending_notifications, 0, sizeof self->pending_notifications);
    for (u32 i = 0; i < notifications.len; i++) {
        groove_SubscriptionNotification n = notifications.ptr[i];
        if (self->defer_notifications_until_durable && n.queued.has_publication
            && !jz_btree_contains(&runtime->durable_notification_publications, &n.queued.publication)) {
            if (!n.queued.has_publication) jz_panic("checked publication");
            groove_NotificationVec *deferred = jz_btree_entry(&runtime->deferred_notifications, &n.queued.publication, NULL);
            jz_vec_push(deferred, &n, sizeof n);
        } else if (self->has_pending_resident_publication && !n.queued.has_publication) {
            jz_vec_push(self->pending_resident_publication.notifications, &n, sizeof n);
        } else {
            const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&runtime->multisink_subscriptions, &n.id);
            if (subscription) {
                groove_QueuedMultisinkDeltas returned;
                if (!SB(SubscriptionSender__send)(&subscription->sender, n.queued, &returned)) {
                    SB(QueuedMultisinkDeltas__drop)(&returned);
                    jz_vec_push(&dropped_subscriptions, &n.id, sizeof n.id);
                }
            } else {
                SB(QueuedMultisinkDeltas__drop)(&n.queued);
            }
        }
    }
    jz_vec_drop(&notifications, sizeof(groove_SubscriptionNotification), NULL);
    for (u32 i = 0; i < dropped_subscriptions.len; i++) SUBS(unsubscribe)(runtime, dropped_subscriptions.ptr[i]);
    jz_vec_drop(&dropped_subscriptions, sizeof(groove_SubscriptionId), NULL);
    /* debug_assert!(affected_recursive_nodes_are_current(..)) is debug-only. */
    RT(IvmRuntime__evict_eval_memo)(runtime);
    groove_OptPublicationId publication = self->notification_publication;
    if (self->has_pending_resident_publication && self->pending_resident_publication.publication->some)
        publication = *self->pending_resident_publication.publication;
    if (self->has_pending_resident_publication && !self->pending_resident_publication.publication->some) {
        *self->pending_resident_publication.completed = true;
    } else if (self->defer_notifications_until_durable && publication.some
               && !jz_btree_remove(&runtime->durable_notification_publications, &publication.id, NULL, NULL)) {
        jz_btree_insert(&runtime->completed_deferred_publications, &publication.id, NULL, NULL);
    }
    if (runtime->collect_tick_runtime_stats) groove__ivm__runtime__graph_lifecycle__IvmRuntime__stats(runtime, &self->metrics.runtime_stats);
    else groove__ivm__runtime__graph_lifecycle__IvmRuntime__cheap_stats(runtime, &self->metrics.runtime_stats);
    return 0;
}


/* ---------------------------------------------------------------------- */
/* EvaluationSession                                                      */
/* ---------------------------------------------------------------------- */
static jz_err RT(EvaluationSession__hydration)(const groove_IvmRuntime *runtime, const groove_NodeId *roots, u32 nroots, groove_OwnedStorage storage, EvaluationSession *out) {
    EvaluationSession s; memset(&s, 0, sizeof s);
    jz_err e = RT(EvaluationWorkQueue__discover)(runtime->graph, &runtime->node_meta, roots, nroots, true, &s.relevant_nodes, &s.work_queue);
    if (e.code) { groove__storage__OwnedStorage__drop(&storage); return e; }
    /* Installed operator state is root-scoped. Probe by reachable node instead
     * of scanning state owned by unrelated graphs. */
    jz_hmap_init(&s.operator_states, runtime->operator_states.d);
    jz_hmap_init(&s.arrangement_states, runtime->arrangement_states.d);
    jz_hmap_init(&s.arrangement_keys_by_input, runtime->arrangement_keys_by_input.d);
    groove_NodeId *node; void *unused;
    JZ_HMAP_FOR(&s.relevant_nodes, node, unused)
        stage_runtime_node_state(&s.operator_states, &s.arrangement_states, &s.arrangement_keys_by_input, runtime, *node);
    s.eval_memo = MEMO(for_layout)(jz_rc_clone(s.work_queue.layout));
    JZ_HMAP_FOR(&s.relevant_nodes, node, unused) {
        const jz_hmap *keys = jz_hmap_get(&runtime->eval_memo_keys_by_node, node);
        if (!keys) continue;
        groove_EvalMemoKey *key; void *u2;
        JZ_HMAP_FOR(keys, key, u2) {
            groove_EvalMemoEntry *entry = MEMO(get)(runtime->eval_memo, key);
            if (!entry) continue;
            groove_EvalMemoEntry copy = *entry;
            if (copy.records) jz_rc_clone(copy.records);
            groove_EvalMemoEntry previous;
            if (MEMO(insert)(s.eval_memo, key, copy, &previous) && previous.records) jz_rc_release(previous.records);
        }
    }
    s.eval_memo_bytes = memo_payload_bytes(s.eval_memo);
    jz_hmap_init(&s.node_meta, runtime->node_meta.d);
    JZ_HMAP_FOR(&s.relevant_nodes, node, unused) {
        const groove_NodeRuntimeMeta *meta = jz_hmap_get(&runtime->node_meta, node);
        if (!meta) continue;
        groove_NodeRuntimeMeta copy; groove_NodeRuntimeMeta_clone(&copy, meta);
        jz_hmap_insert(&s.node_meta, node, &copy, NULL);
    }
    /* Recursive hydration rebuilds internal arrangements from complete source
     * snapshots, so a leaf memo alone cannot satisfy those inputs. */
    jz_deque pending_recursive_inputs; jz_deque_init(&pending_recursive_inputs, sizeof(groove_NodeId));
    JZ_HMAP_FOR(&s.relevant_nodes, node, unused) {
        const groove_GraphNode *graph_node = groove__ivm__graph__IvmGraph__node(runtime->graph, *node);
        if (!graph_node) continue;
        if (graph_node->descriptor.op.tag == GROOVE_OP_RECURSIVE)
            for (u32 i = 0; i < graph_node->descriptor.inputs.len; i++) jz_deque_push_back(&pending_recursive_inputs, &graph_node->descriptor.inputs.ptr[i]);
    }
    jz_hmap recursive_inputs; nset_init(&recursive_inputs);
    groove_NodeId rn;
    while (jz_deque_pop_front(&pending_recursive_inputs, &rn)) {
        if (!nset_insert(&recursive_inputs, rn)) continue;
        const groove_GraphNode *graph_node = groove__ivm__graph__IvmGraph__node(runtime->graph, rn);
        if (graph_node)
            for (u32 i = 0; i < graph_node->descriptor.inputs.len; i++) jz_deque_push_back(&pending_recursive_inputs, &graph_node->descriptor.inputs.ptr[i]);
    }
    jz_deque_drop(&pending_recursive_inputs, NULL);
    jz_hmap resident_source_nodes; nset_init(&resident_source_nodes);
    groove_NodeRuntimeMeta *meta;
    JZ_HMAP_FOR(&s.node_meta, node, meta) {
        if (nset_contains(&recursive_inputs, *node)) continue;
        if (!meta->input_signature) continue;
        groove_EvalMemoKey key; memset(&key, 0, sizeof key);
        key.scope = scope_root(); key.node = *node; key.input_signature_hash = meta->input_signature->hash;
        key.has_tick_epoch = false; key.sub_tick = 0; key.context_digest = 0;
        const groove_EvalMemoEntry *entry = MEMO(get)(s.eval_memo, &key);
        if (entry && entry->input_watermark == meta->input_generation) nset_insert(&resident_source_nodes, *node);
    }
    jz_hmap_drop(&recursive_inputs);
    RT(EvaluationWorkQueue__storage_already_resident)(&s.work_queue, &resident_source_nodes);
    jz_hmap_drop(&resident_source_nodes);
    ES(EvaluationRequests__new)(&s.requests);
    void *k, *v;
    JZ_BTREE_FOR(&s.work_queue.request_dependents, k, v) {
        groove_EvaluationRequestKey key; ES(EvaluationRequestKey__clone)(&key, k);
        ES(EvaluationRequests__request)(&s.requests, key, storage.storage, runtime->chunk_provider, &runtime->schema);
    }
    nset_init(&s.roots);
    for (u32 i = 0; i < nroots; i++) nset_insert(&s.roots, roots[i]);
    jz_hmap_init(&s.outputs, &hdesc_node_deltas);
    jz_hmap_init(&s.pending_outputs, &hdesc_node_arc_deltas);
    s.memo_use_clock = runtime->memo_use_clock;
    jz_hmap_init(&s.terminal_deltas, &groove_ivm_rt_a_hdesc_terminal_deltas);
    hmap_clone(&s.binding_frontiers, &runtime->binding_frontiers, binding_key_clone_v, NULL);
    s.storage = storage;
    ES(EvaluationInputs__default)(&s.evaluation_inputs);
    s.root_indirect_values.tag = GROOVE_RIV_MATERIALIZE; s.root_indirect_values.physical = NULL;
    nset_init(&s.ordering_only_roots);
    nset_init(&s.borrowed);
    *out = s;
    return JZ_OK;
}

static void RT(EvaluationSession__advance_binding_input)(EvaluationSession *self, const groove_IvmGraph *graph, const jz_str *shape) {
    groove_BindingSourceKey key;
    groove__ivm__op_types__BindingSourceKey__prepared(shape->ptr, shape->len, &key);
    groove_BindingSourceKey entry_key; groove__ivm__op_types__BindingSourceKey__clone(&entry_key, &key);
    u64 *frontier = jz_hmap_entry(&self->binding_frontiers, &entry_key, NULL);
    *frontier += 1;
    groove_NodeIdVec affected = {0};
    groove__ivm__graph__IvmGraph__affected_nodes_through_routes(graph, NULL, 0, &key, 1, &affected);
    for (u32 i = 0; i < affected.len; i++) {
        if (!nset_contains(&self->relevant_nodes, affected.ptr[i])) continue;
        groove_NodeRuntimeMeta *meta = groove_ivm_rt_a_node_meta_entry(&self->node_meta, affected.ptr[i]);
        meta->input_generation = meta->input_generation + 1;
    }
    jz_vec_drop(&affected, sizeof(groove_NodeId), NULL);
    groove__ivm__op_types__BindingSourceKey__drop(&key);
}

static void RT(EvaluationSession__evaluator)(EvaluationSession *self, const groove_IvmRuntime *runtime, const groove_BindingSnapshots *binding_snapshots,
                                            bool hydrate_arrangements, groove_TickMetrics *metrics, groove_TickEvaluator *ev) {
    memset(ev, 0, sizeof *ev);
    ev->schema = &runtime->schema;
    ev->graph = runtime->graph;
    ev->variant_projections = &runtime->variant_projections;
    ev->binding_snapshots = binding_snapshots;
    ev->current_tick = runtime->current_tick;
    ev->operator_states = &self->operator_states;
    ev->arrangement_states = &self->arrangement_states;
    ev->arrangement_keys_by_input = &self->arrangement_keys_by_input;
    ev->eval_memo = self->eval_memo;
    ev->eval_memo_bytes = &self->eval_memo_bytes;
    ev->table_frontiers = &runtime->table_frontiers;
    ev->binding_frontiers = &self->binding_frontiers;
    ev->memo_use_clock = &self->memo_use_clock;
    ev->node_meta = &self->node_meta;
    ev->storage = self->storage.storage;
    ev->evaluation_inputs = &self->evaluation_inputs;
    if (hydrate_arrangements) groove__ivm__runtime__state__EvalContext__root_subscription_snapshot(&ev->context);
    else groove__ivm__runtime__state__EvalContext__root_snapshot(&ev->context);
    ev->metrics = metrics;
    ev->terminal_deltas = self->terminal_deltas;
    jz_hmap_init(&self->terminal_deltas, &groove_ivm_rt_a_hdesc_terminal_deltas);
    jz_hmap_init(&ev->root_ordering_windows, &groove_ivm_rt_a_hdesc_root_windows);
}

/* Registers `requests`, polls them once, and reports whether any completed. */
static void session_register(EvaluationSession *self, const groove_IvmRuntime *runtime, const jz_btree *requests, bool *registered) {
    void *k, *v;
    JZ_BTREE_FOR(requests, k, v) {
        groove_EvaluationRequestKey key; ES(EvaluationRequestKey__clone)(&key, k);
        bool r = ES(EvaluationRequests__request)(&self->requests, key, self->storage.storage, runtime->chunk_provider, &runtime->schema);
        if (registered) *registered |= r;
    }
}

static u32 RT(EvaluationSession__poll)(EvaluationSession *self, const groove_IvmRuntime *runtime, const groove_BindingSnapshots *binding_snapshots,
                                      bool hydrate_arrangements, groove_TickMetrics *metrics, const jz_waker *cx, jz_err *result) {
    *result = JZ_OK;
    usize remaining_runnable_nodes = MAX_HYDRATION_RUNNABLE_NODES_PER_POLL;
owner_turn:
    for (;;) {
        u32 ready = ES(EvaluationRequests__poll)(&self->requests);
        if (ready == 0) ES(EvaluationRequests__poll_eager_retry)(&self->requests);
        jz_btree ready_map;
        groove_EvaluationRequestKey failed_key; groove_EvaluationRequestFailure failure;
        if (!ES(EvaluationRequests__drain_ready)(&self->requests, &ready_map, &failed_key, &failure)) {
            groove_ivm_rt_REQUEST_KEY_SET.key_drop(&failed_key);
            *result = failure.error;
            return 0;
        }
        RT(EvaluationWorkQueue__requests_ready)(&self->work_queue, &ready_map);
        ES(EvaluationInputs__install)(&self->evaluation_inputs, &ready_map);

        groove_NodeId node;
        while (jz_deque_pop_front(&self->work_queue.runnable, &node)) {
            if (remaining_runnable_nodes == 0) {
                RT(EvaluationWorkQueue__requeue_yielded)(&self->work_queue, node);
                /* A resident CPU budget yield: arrange exactly one fresh owner turn. */
                wake(cx);
                return GROOVE_POLL_PENDING;
            }
            remaining_runnable_nodes -= 1;
            groove_RecordDeltas *records = NULL;
            jz_err e = JZ_OK;
            groove_RecordDeltas **pending = jz_hmap_get(&self->pending_outputs, &node);
            if (pending) {
                records = *pending;
                *pending = NULL;
                jz_hmap_remove(&self->pending_outputs, &node, NULL, NULL);
            } else {
                groove_TickEvaluator evaluator;
                RT(EvaluationSession__evaluator)(self, runtime, binding_snapshots, hydrate_arrangements, metrics, &evaluator);
                e = RT(EvaluationWorkQueue__poll_node)(&self->work_queue, &evaluator, node, &records);
                jz_hmap_drop(&self->terminal_deltas);
                self->terminal_deltas = evaluator.terminal_deltas;
                TEV(init_maps)(&evaluator);
                TEV(drop)(&evaluator);
                if (e.code == GROOVE_POLL_PENDING) {
                    /* A future which cooperatively yielded has not registered a
                     * storage request: requeue it for a fresh owner turn. */
                    jz_err_drop(&e);
                    RT(EvaluationWorkQueue__requeue_yielded)(&self->work_queue, node);
                    wake(cx);
                    return GROOVE_POLL_PENDING;
                }
            }
            if (e.code == 0) {
                if (RT(EvaluationWorkQueue__is_root)(&self->work_queue, node)) {
                    groove_UsizeVec fields = {0};
                    bool has_fields;
                    if (nset_contains(&self->ordering_only_roots, node)) has_fields = true;
                    else has_fields = SB(RootIndirectValues__materialized_field_indices)(&self->root_indirect_values, records->descriptor, &fields);
                    static const usize no_fields[1] = { 0 };
                    groove_RecordDeltaVec materialized = {0};
                    jz_vec_reserve(&materialized, records->deltas.len, sizeof(groove_RecordDelta));
                    bool blocked = false;
                    for (u32 i = 0; i < records->deltas.len; i++) {
                        const groove_RecordDelta *delta = &records->deltas.ptr[i];
                        bool owned = false; jz_bytes bytes = {0};
                        jz_err me = groove__large_values__materialize_record_borrowed_attempt(records->descriptor, delta->record.ptr, delta->record.len,
                            has_fields ? (fields.ptr ? fields.ptr : no_fields) : NULL, fields.len, &self->evaluation_inputs, &owned, &bytes);
                        if (me.code == 0) {
                            groove_RecordDelta d = { owned ? bytes : jz_bytes_clone(&delta->record), delta->weight };
                            jz_vec_push(&materialized, &d, sizeof d);
                        } else if (me.code == GROOVE_IVM_ERR_EVALUATION_BLOCKED) {
                            jz_err_drop(&me);
                            blocked = true;
                        } else {
                            jz_vec_drop(&fields, sizeof(usize), NULL);
                            jz_vec_drop(&materialized, sizeof(groove_RecordDelta), SB(RecordDelta__drop));
                            jz_rc_release(records);
                            *result = me;
                            return 0;
                        }
                    }
                    jz_vec_drop(&fields, sizeof(usize), NULL);
                    if (blocked) {
                        jz_vec_drop(&materialized, sizeof(groove_RecordDelta), SB(RecordDelta__drop));
                        jz_btree requests;
                        ES(EvaluationInputs__take_missing)(&self->evaluation_inputs, &requests);
                        if (requests.len == 0) {
                            jz_btree_drop(&requests);
                            jz_rc_release(records);
                            *result = groove_err_evaluation_blocked();
                            return 0;
                        }
                        session_register(self, runtime, &requests, NULL);
                        jz_hmap_insert(&self->pending_outputs, &node, &records, NULL);
                        RT(EvaluationWorkQueue__wait_for_requests)(&self->work_queue, node, &requests);
                        /* Every newly retained future must be polled once before
                         * returning Pending so it can install the caller's waker. */
                        u32 r = ES(EvaluationRequests__poll)(&self->requests);
                        if (r > 0 || (r == 0 && ES(EvaluationRequests__poll_eager_retry)(&self->requests) > 0)) goto owner_turn;
                        /* Cold storage owns the continuation now. */
                        if (ES(EvaluationRequests__has_pending)(&self->requests)) return GROOVE_POLL_PENDING;
                        continue;
                    }
                    groove_RecordDeltas output = { records->descriptor, materialized };
                    jz_hmap_insert(&self->outputs, &node, &output, NULL);
                }
                if (records) jz_rc_release(records);
                RT(EvaluationWorkQueue__complete)(&self->work_queue, node);
            } else if (e.code == GROOVE_IVM_ERR_EVALUATION_BLOCKED) {
                jz_err_drop(&e);
                jz_btree requests;
                ES(EvaluationInputs__take_missing)(&self->evaluation_inputs, &requests);
                if (requests.len == 0) { jz_btree_drop(&requests); *result = groove_err_evaluation_blocked(); return 0; }
                bool registered = false;
                session_register(self, runtime, &requests, &registered);
                RT(EvaluationWorkQueue__wait_for_requests)(&self->work_queue, node, &requests);
                if (registered || ES(EvaluationRequests__has_pending)(&self->requests)) {
                    u32 r = ES(EvaluationRequests__poll)(&self->requests);
                    if (r > 0 || (r == 0 && ES(EvaluationRequests__poll_eager_retry)(&self->requests) > 0)) goto owner_turn;
                }
                /* A request was polled with this owner's durable waker; yield to it. */
                if (ES(EvaluationRequests__has_pending)(&self->requests)) return GROOVE_POLL_PENDING;
            } else {
                *result = e;
                return 0;
            }
        }

        if (self->outputs.len == self->roots.len) return 0;
        if (ES(EvaluationRequests__has_pending)(&self->requests)) return GROOVE_POLL_PENDING;
        /* Earlier evaluations own the continuation until their shared nodes are
         * installed and the temporal barriers are released. */
        if (self->work_queue.temporal_waiting.len != 0) return GROOVE_POLL_PENDING;
        *result = groove_err_evaluation_blocked();
        return 0;
    }
}

static bool op_key_node_not_in(const void *k, void *v, void *ctx) { (void)v; return !nset_contains(ctx, ((const groove_OperatorStateKey *)k)->node); }
static bool arr_key_not_in_keyset(const void *k, void *v, void *ctx) { (void)v; return jz_hmap_get(ctx, k) == NULL; }

/* Consumes the session. */
static void RT(EvaluationSession__install)(EvaluationSession *self, groove_IvmRuntime *runtime) {
    if (self->borrowed.len) {
        jz_hmap borrowed = self->borrowed;
        nset_init(&self->borrowed);
        jz_hmap_retain(&self->relevant_nodes, nodeset_key_not_in, &borrowed);
        jz_hmap_retain(&self->operator_states, op_key_node_not_in, &borrowed);
        jz_hmap borrowed_keys; jz_hmap_init(&borrowed_keys, &groove_ivm_rt_ARRANGEMENT_KEY_SET);
        groove_NodeId *node; jz_hmap *keys;
        JZ_HMAP_FOR(&self->arrangement_keys_by_input, node, keys) {
            if (!nset_contains(&borrowed, *node)) continue;
            groove_ArrangementKey *k; void *u;
            JZ_HMAP_FOR(keys, k, u) jz_hmap_insert(&borrowed_keys, k, NULL, NULL);
        }
        jz_hmap_retain(&self->arrangement_keys_by_input, nodeset_key_not_in, &borrowed);
        jz_hmap_retain(&self->arrangement_states, arr_key_not_in_keyset, &borrowed_keys);
        MEMO(retain)(self->eval_memo, memo_key_not_in, &borrowed);
        jz_hmap_retain(&self->node_meta, nodeset_key_not_in, &borrowed);
        jz_hmap_drop(&borrowed_keys);
        jz_hmap_drop(&borrowed);
    }
    groove_ScopeId root = scope_root();
    groove_NodeId *node; void *unused;
    JZ_HMAP_FOR(&self->relevant_nodes, node, unused) {
        groove_OperatorStateKey key = { root, *node };
        jz_hmap_remove(&runtime->operator_states, &key, NULL, NULL);
    }
    /* Session hydration also establishes long-lived collector state. Fold its
     * initially populated sparse groups now. */
    groove_OperatorStateKey *okey; groove_OperatorState *state;
    JZ_HMAP_FOR(&self->operator_states, okey, state) {
        if (state->tag == GROOVE_OS_TOP_BY) groove__ivm__runtime__evaluator__TopByIncrementalState__commit_overlays(&state->v.top_by.value);
        if (state->tag == GROOVE_OS_ARG_BY) groove__ivm__runtime__evaluator__SparseGroups__commit_overlay(&state->v.arg_by.value);
        if (state->tag == GROOVE_OS_SEMI_JOIN) groove__ivm__runtime__join__SemiJoinState__commit_published_overlay(state->v.semi_join);
        if (state->tag == GROOVE_OS_ANTI_JOIN) groove__ivm__runtime__join__AntiJoinState__commit_published_overlay(state->v.anti_join);
        if (state->tag == GROOVE_OS_COLLECT_BY)
            groove__ivm__runtime__evaluator__SparseGroups__commit_overlay(&groove__ivm__runtime__evaluator__CollectByIncrementalState__deref_mut(&state->v.collect_by)->groups);
    }
    jz_hmap_retain(&self->operator_states, op_key_is_root, NULL);
    hmap_move_into(&runtime->operator_states, &self->operator_states);
    JZ_HMAP_FOR(&self->relevant_nodes, node, unused) {
        const jz_hmap *keys = jz_hmap_get(&runtime->arrangement_keys_by_input, node);
        if (!keys) continue;
        groove_ArrangementKey *k; void *u;
        JZ_HMAP_FOR(keys, k, u) jz_hmap_remove(&runtime->arrangement_states, k, NULL, NULL);
    }
    /* Hydration-created arrangements are the immutable bases for the next staged tick. */
    groove_ArrangementKey *akey; groove_AsOfArrangement *astate;
    JZ_HMAP_FOR(&self->arrangement_states, akey, astate) groove__ivm__runtime__join__ArrangementState__commit_overlay(astate->value);
    hmap_move_into(&runtime->arrangement_states, &self->arrangement_states);
    JZ_HMAP_FOR(&self->relevant_nodes, node, unused) jz_hmap_remove(&runtime->arrangement_keys_by_input, node, NULL, NULL);
    hmap_move_into(&runtime->arrangement_keys_by_input, &self->arrangement_keys_by_input);
    groove__ivm__runtime__IvmRuntime__remove_retained_eval_memos_for_nodes(runtime, &self->relevant_nodes);
    groove_EvaluationMemoIter it; MEMO(iter)(self->eval_memo, &it);
    const groove_EvalMemoKey *mkey; groove_EvalMemoEntry *entry;
    while (groove__ivm__runtime__evaluation_memo__EvaluationMemoIter__next(&it, &mkey, &entry)) {
        if (mkey->has_tick_epoch) continue;
        groove_EvalMemoEntry copy = *entry;
        if (copy.records) jz_rc_clone(copy.records);
        groove__ivm__runtime__IvmRuntime__insert_retained_eval_memo(runtime, *mkey, copy);
    }
    if (self->memo_use_clock > runtime->memo_use_clock) runtime->memo_use_clock = self->memo_use_clock;
    RT(carry_live_node_lifecycle)(&self->node_meta, runtime, &self->relevant_nodes);
    JZ_HMAP_FOR(&self->relevant_nodes, node, unused) jz_hmap_remove(&runtime->node_meta, node, NULL, NULL);
    hmap_move_into(&runtime->node_meta, &self->node_meta);
    RT(EvaluationSession__drop)(self);
}


/* ---------------------------------------------------------------------- */
/* IvmRuntime                                                             */
/* ---------------------------------------------------------------------- */
static groove_OwnedStorage owned_storage(groove_OrderedKvStorage *storage) { return groove__storage__OwnedStorage__new(jz_rc_clone(storage)); }

/* `.await` of a real suspension point: storage and chunk futures are assumed
 * to complete synchronously, so the spawned-task queue is drained between polls. */
static void await_turn(void) { jz_task_run_ready(); }

static void tick_metrics_clone(groove_TickMetrics *dst, const groove_TickMetrics *src) {
    *dst = *src;
    nset_clone(&dst->hydration_memo_computed_nodes, &src->hydration_memo_computed_nodes);
}

static void deque_retain_u64_ne(jz_deque *q, u64 id) {
    u32 n = q->len; u64 v;
    for (u32 i = 0; i < n; i++) { jz_deque_pop_front(q, &v); if (v != id) jz_deque_push_back(q, &v); }
}

static jz_err RT(IvmRuntime__output_depends_on_aggregate)(const groove_IvmRuntime *self, groove_NodeId output_node, bool *out) {
    jz_hmap ancestors; nset_init(&ancestors);
    groove__ivm__graph__IvmGraph__mark_ancestors(self->graph, output_node, &ancestors);
    *out = false;
    groove_NodeId *ancestor; void *unused;
    JZ_HMAP_FOR(&ancestors, ancestor, unused) {
        const groove_GraphNode *graph_node = groove__ivm__graph__IvmGraph__node(self->graph, *ancestor);
        if (!graph_node) { groove_NodeId a = *ancestor; jz_hmap_drop(&ancestors); return groove_err_node_not_found(a); }
        u32 tag = graph_node->descriptor.op.tag;
        if (tag == GROOVE_OP_AGGREGATE || tag == GROOVE_OP_ARG_MIN_BY || tag == GROOVE_OP_ARG_MAX_BY) { *out = true; break; }
    }
    jz_hmap_drop(&ancestors);
    return JZ_OK;
}

static jz_err roots_depend_on_aggregate(const groove_IvmRuntime *self, const groove_NodeId *roots, u32 n, bool *out) {
    *out = false;
    for (u32 i = 0; i < n && !*out; i++) JZ_TRY(RT(IvmRuntime__output_depends_on_aggregate)(self, roots[i], out));
    return JZ_OK;
}

static void outputs_roots(const jz_btree *outputs, groove_NodeIdVec *roots) {
    jz_hmap seen; nset_init(&seen);
    jz_str *sink; groove_CompiledNode *output;
    JZ_BTREE_FOR(outputs, sink, output) {
        if (output->has_root_ordering_node && nset_insert(&seen, output->root_ordering_node)) jz_vec_push(roots, &output->root_ordering_node, sizeof(groove_NodeId));
        if (nset_insert(&seen, output->node)) jz_vec_push(roots, &output->node, sizeof(groove_NodeId));
    }
    jz_hmap_drop(&seen);
}

static void temporal_blockers_of(const PendingIncrementalState *pending, jz_hmap *out) {
    jz_hmap_init(out, &hdesc_node_usize);
    groove_NodeId *node; jz_deque *waiters;
    JZ_HMAP_FOR(&pending->waiters_by_node, node, waiters) { usize one = 1; jz_hmap_insert(out, node, &one, NULL); }
}

static void register_waiters(PendingIncrementalState *pending, const groove_NodeId *nodes, u32 n, u64 evaluation_id) {
    for (u32 i = 0; i < n; i++) {
        bool inserted = false;
        jz_deque *waiters = jz_hmap_entry(&pending->waiters_by_node, &nodes[i], &inserted);
        if (inserted) jz_deque_init(waiters, sizeof(u64));
        jz_deque_push_back(waiters, &evaluation_id);
    }
}

jz_err RTK(enqueue_subscription_hydration)(groove_IvmRuntime *self, groove_SubscriptionId subscription_id, jz_btree outputs,
    groove_OwnedStorage storage, groove_BindingSnapshots *binding_snapshots, const jz_str *binding_frontier_advance, groove_InitialCell *initial,
    groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root_indirect_values, jz_hmap borrowed, u64 *out) {
    groove_NodeIdVec roots = {0};
    outputs_roots(&outputs, &roots);
    bool hydrate_arrangements = false;
    jz_err e = JZ_OK;
    if (lifetime == GROOVE_SL_RETAINED) e = roots_depend_on_aggregate(self, roots.ptr, roots.len, &hydrate_arrangements);
    EvaluationSession session;
    if (!e.code) {
        /* A collector's ordering root carries every source field, but only its
         * row identity orders the terminal snapshot (#3830). */
        e = RT(EvaluationSession__hydration)(self, roots.ptr, roots.len, storage, &session);
    } else {
        groove__storage__OwnedStorage__drop(&storage);
    }
    jz_vec_drop(&roots, sizeof(groove_NodeId), NULL);
    if (e.code) {
        jz_btree_drop(&outputs);
        if (binding_snapshots) jz_rc_release(binding_snapshots);
        jz_rc_release(initial);
        SB(RootIndirectValues__drop)(&root_indirect_values);
        jz_hmap_drop(&borrowed);
        return e;
    }
    jz_str *sink; groove_CompiledNode *output;
    JZ_BTREE_FOR(&outputs, sink, output) {
        if (!output->has_root_ordering_node) continue;
        bool is_output = false;
        jz_str *s2; groove_CompiledNode *o2;
        JZ_BTREE_FOR(&outputs, s2, o2) if (o2->node.v == output->root_ordering_node.v) { is_output = true; break; }
        if (!is_output) nset_insert(&session.ordering_only_roots, output->root_ordering_node);
    }
    SB(RootIndirectValues__drop)(&session.root_indirect_values);
    session.root_indirect_values = root_indirect_values;
    if (borrowed.len) {
        /* The attach tick advanced every shared node; the subscription's own
         * nodes may hold a memo that predates later writes. Never reuse it. */
        groove_NodeIdVec own = {0};
        groove_NodeId *node; void *unused;
        JZ_HMAP_FOR(&session.relevant_nodes, node, unused) if (!nset_contains(&borrowed, *node)) jz_vec_push(&own, node, sizeof *node);
        for (u32 i = 0; i < own.len; i++) {
            groove_NodeRuntimeMeta *meta = groove_ivm_rt_a_node_meta_entry(&session.node_meta, own.ptr[i]);
            meta->input_generation = meta->input_generation + 1;
        }
        jz_vec_drop(&own, sizeof(groove_NodeId), NULL);
    }
    jz_hmap_drop(&session.borrowed);
    session.borrowed = borrowed;
    if (binding_frontier_advance) RT(EvaluationSession__advance_binding_input)(&session, self->graph, binding_frontier_advance);
    PendingIncrementalState *pending = pending_state(self);
    jz_hmap temporal_blockers;
    temporal_blockers_of(pending, &temporal_blockers);
    RT(EvaluationWorkQueue__add_temporal_blockers)(&session.work_queue, &temporal_blockers);
    jz_hmap_drop(&temporal_blockers);
    PendingSubscriptionHydration *hydration = JZ_NEW(PendingSubscriptionHydration);
    hydration->subscription_id = subscription_id;
    hydration->outputs = outputs;
    hydration->initial = initial;
    hydration->session = session;
    hydration->binding_snapshots = binding_snapshots ? binding_snapshots : SUBS(binding_snapshot_deltas)(self);
    hydration->has_binding_frontier_advance = binding_frontier_advance != NULL;
    if (binding_frontier_advance) hydration->binding_frontier_advance = jz_str_clone(binding_frontier_advance);
    hydration->hydrate_arrangements = hydrate_arrangements;
    hydration->lifetime = lifetime;
    groove__ivm__runtime__state__TickMetrics__default(&hydration->metrics);
    PendingEvaluation evaluation = { PE_SUBSCRIPTION_HYDRATION, NULL, hydration };
    u64 evaluation_id = pending->next_id;
    pending->next_id = pending->next_id == UINT64_MAX ? UINT64_MAX : pending->next_id + 1;
    groove_NodeIdVec incomplete = {0};
    RT(EvaluationWorkQueue__incomplete_nodes)(RT(PendingEvaluation__work_queue)(&evaluation), &incomplete);
    register_waiters(pending, incomplete.ptr, incomplete.len, evaluation_id);
    jz_vec_drop(&incomplete, sizeof(groove_NodeId), NULL);
    jz_btree_insert(&pending->evaluations, &evaluation_id, &evaluation, NULL);
    jz_deque_push_back(&pending->order, &evaluation_id);
    *out = evaluation_id;
    return JZ_OK;
}

typedef struct AffectedCtx { const jz_hmap *affected; } AffectedCtx;
static bool owned_by_affected(groove_ScopeId scope, const jz_hmap *affected) {
    groove_NodeId node;
    return groove__ivm__runtime__state__ScopeId__outermost(scope, &node) && nset_contains(affected, node);
}
static bool op_state_unaffected(const void *k, void *v, void *ctx) {
    (void)v;
    const groove_OperatorStateKey *key = k;
    return !nset_contains(ctx, key->node) && !owned_by_affected(key->scope, ctx);
}

static bool outputs_touch(const jz_btree *outputs, const jz_hmap *nodes) {
    jz_str *sink; groove_CompiledNode *output;
    JZ_BTREE_FOR(outputs, sink, output) {
        if (nset_contains(nodes, output->node)) return true;
        if (output->has_root_ordering_node && nset_contains(nodes, output->root_ordering_node)) return true;
    }
    return false;
}

static void RT(IvmRuntime__fail_evaluation_nodes)(groove_IvmRuntime *self, const EvaluationFailure *failure) {
    const jz_hmap *affected = &failure->affected_nodes;
    /* A recursive node also owns the state of its child scopes. */
    jz_hmap_retain(&self->operator_states, op_state_unaffected, (void *)affected);
    groove__ivm__runtime__IvmRuntime__remove_retained_eval_memos_for_nodes(self, affected);
    JZ_VEC(groove_EvalMemoKey) scoped_memos = {0};
    groove_NodeId *node; jz_hmap *keys;
    JZ_HMAP_FOR(&self->eval_memo_keys_by_node, node, keys) {
        groove_EvalMemoKey *key; void *unused;
        JZ_HMAP_FOR(keys, key, unused) if (owned_by_affected(key->scope, affected)) jz_vec_push(&scoped_memos, key, sizeof *key);
    }
    for (u32 i = 0; i < scoped_memos.len; i++) groove__ivm__runtime__IvmRuntime__remove_retained_eval_memo(self, &scoped_memos.ptr[i]);
    jz_vec_drop(&scoped_memos, sizeof(groove_EvalMemoKey), NULL);
    void *unused;
    JZ_HMAP_FOR(affected, node, unused) {
        jz_hmap removed;
        if (jz_hmap_remove(&self->arrangement_keys_by_input, node, NULL, &removed)) {
            groove_ArrangementKey *key; void *u2;
            JZ_HMAP_FOR(&removed, key, u2) jz_hmap_remove(&self->arrangement_states, key, NULL, NULL);
            jz_hmap_drop(&removed);
        }
        groove_NodeRuntimeMeta *meta = jz_hmap_get(&self->node_meta, node);
        if (meta) {
            if (meta->input_signature) jz_rc_release(meta->input_signature);
            meta->input_signature = NULL;
            if (meta->input_generation != UINT64_MAX) meta->input_generation += 1;
        }
    }
    JZ_VEC(groove_ArrangementKey) scoped = {0};
    groove_ArrangementKey *akey; void *astate;
    JZ_HMAP_FOR(&self->arrangement_states, akey, astate) if (owned_by_affected(akey->scope, affected)) jz_vec_push(&scoped, akey, sizeof *akey);
    for (u32 i = 0; i < scoped.len; i++) {
        groove_ArrangementKey key = scoped.ptr[i];
        jz_hmap_remove(&self->arrangement_states, &key, NULL, NULL);
        jz_hmap *set = jz_hmap_get(&self->arrangement_keys_by_input, &key.input);
        if (set) {
            jz_hmap_remove(set, &key, NULL, NULL);
            if (set->len == 0) jz_hmap_remove(&self->arrangement_keys_by_input, &key.input, NULL, NULL);
        }
    }
    jz_vec_drop(&scoped, sizeof(groove_ArrangementKey), NULL);
    groove_SubscriptionId *sid; groove_MultisinkSubscriptionState *subscription;
    JZ_HMAP_FOR(&self->multisink_subscriptions, sid, subscription) {
        if (subscription->failed || !outputs_touch(&subscription->outputs, affected)) continue;
        subscription->failed = true;
        SB(SubscriptionSender__fail)(&subscription->sender, SB(SubscriptionError__new)(jz_rc_clone(failure->error)));
    }
}

/* consumes `error` (rc) */
static void RT(IvmRuntime__fail_all_subscriptions)(groove_IvmRuntime *self, jz_err *error) {
    groove_SubscriptionId *sid; groove_MultisinkSubscriptionState *subscription;
    JZ_HMAP_FOR(&self->multisink_subscriptions, sid, subscription) {
        if (subscription->failed) continue;
        subscription->failed = true;
        SB(SubscriptionSender__fail)(&subscription->sender, SB(SubscriptionError__new)(jz_rc_clone(error)));
    }
    jz_rc_release(error);
}

static jz_err RT(IvmRuntime__tick_durable_nodes)(const groove_IvmRuntime *self, const groove_TableDelta *table_deltas, u32 ntd,
    const groove_BindingSnapshots *binding_snapshots, const groove_NodeId *durable_nodes, u32 ndurable, u64 current_tick,
    const groove_OrderedKvStorage *storage, jz_hmap *operator_states, jz_hmap *arrangement_states, jz_hmap *arrangement_keys_by_input,
    groove_EvaluationMemo *eval_memo, usize *eval_memo_bytes, u64 *memo_use_clock, jz_hmap *node_meta,
    const jz_hmap *table_frontiers, const jz_hmap *binding_frontiers, groove_StagedWriteState *durable_writes) {
    groove_OrderedKvStorage *durable_overlay = groove__storage__StagedWriteOverlay__new(storage, durable_writes);
    groove_TickMetrics metrics; groove__ivm__runtime__state__TickMetrics__default(&metrics);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < ndurable && !e.code; i++) {
        groove_NodeId node = durable_nodes[i];
        const groove_GraphNode *graph_node = groove__ivm__graph__IvmGraph__node(self->graph, node);
        if (!graph_node) { e = groove_err_node_not_found(node); break; }
        if (graph_node->descriptor.op.tag != GROOVE_OP_PERSIST) { e = groove_err_unsupported_operator(); break; }
        const groove_PersistOp *persist = &graph_node->descriptor.op.v.persist;
        if (graph_node->descriptor.inputs.len != 1) { e = groove_err_arity(node); break; }
        groove_NodeId input_node = graph_node->descriptor.inputs.ptr[0];
        groove_TickEvaluator evaluator; memset(&evaluator, 0, sizeof evaluator);
        evaluator.schema = &self->schema;
        evaluator.graph = self->graph;
        evaluator.variant_projections = &self->variant_projections;
        evaluator.table_deltas = table_deltas; evaluator.n_table_deltas = ntd;
        evaluator.binding_snapshots = binding_snapshots;
        evaluator.current_tick = current_tick;
        evaluator.operator_states = operator_states;
        evaluator.arrangement_states = arrangement_states;
        evaluator.arrangement_keys_by_input = arrangement_keys_by_input;
        evaluator.eval_memo = eval_memo;
        evaluator.eval_memo_bytes = eval_memo_bytes;
        evaluator.table_frontiers = table_frontiers;
        evaluator.binding_frontiers = binding_frontiers;
        evaluator.memo_use_clock = memo_use_clock;
        evaluator.node_meta = node_meta;
        evaluator.storage = durable_overlay;
        evaluator.evaluation_inputs = NULL;
        groove__ivm__runtime__state__EvalContext__root(&evaluator.context);
        evaluator.metrics = &metrics;
        TEV(init_maps)(&evaluator);
        groove_RecordDeltas *records = NULL;
        for (;;) {
            e = TEV(update_node)(&evaluator, input_node, &records);
            if (e.code != GROOVE_POLL_PENDING) break;
            jz_err_drop(&e);
            await_turn();
        }
        TEV(drop)(&evaluator);
        if (e.code) break;
        groove_RecordDeltas input; SB(RecordDeltas__clone)(&input, records);
        jz_rc_release(records);
        e = groove__ivm__runtime__persist__apply_persist_delta(durable_overlay, &persist->storage, persist->key_fields.ptr, persist->key_fields.len, persist->unique, &input);
        SB(RecordDeltas__drop)(&input);
    }
    groove__ivm__runtime__state__TickMetrics__drop(&metrics);
    groove__storage__StagedWriteOverlay__drop(durable_overlay);
    return e;
}

typedef JZ_VEC(jz_str) StrRefVec;
static void str_set_unique_push(StrRefVec *v, const jz_str *s) {
    for (u32 i = 0; i < v->len; i++) if (jz_str_eq(&v->ptr[i], s)) return;
    jz_str borrowed = { s->ptr, s->len, 0 };
    jz_vec_push(v, &borrowed, sizeof borrowed);
}
static void binding_set_unique_push(groove_BindingSourceKeyVec *v, const groove_BindingSourceKey *k) {
    for (u32 i = 0; i < v->len; i++) if (groove__ivm__op_types__BindingSourceKey__eq(&v->ptr[i], k)) return;
    jz_vec_push(v, k, sizeof *k); /* shallow borrow */
}

static jz_err RT(IvmRuntime__begin_tick_with_params_and_notification_policy)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas,
    groove_BindingDeltaVec binding_deltas, groove_OwnedStorage storage, groove_OptPublicationId notification_publication,
    bool defer_notifications_until_durable, bool has_publication, groove_PendingResidentPublication pending_resident_publication, IncrementalEvaluation *out) {
    usize pending_binding_retractions = self->pending_binding_retractions.len;
    if (pending_binding_retractions != 0) {
        groove_BindingDeltaVec pending = {0};
        jz_vec_clone(&pending, &self->pending_binding_retractions, sizeof(groove_BindingDelta), SB(BindingDelta__clone));
        jz_vec_extend(&pending, binding_deltas.ptr, binding_deltas.len, sizeof(groove_BindingDelta));
        jz_vec_drop(&binding_deltas, sizeof(groove_BindingDelta), NULL);
        binding_deltas = pending;
    }
    StrRefVec changed_tables = {0};
    for (u32 i = 0; i < table_deltas.len; i++) str_set_unique_push(&changed_tables, &table_deltas.ptr[i].table);
    groove_BindingSourceKeyVec changed_bindings = {0};
    for (u32 i = 0; i < binding_deltas.len; i++) binding_set_unique_push(&changed_bindings, &binding_deltas.ptr[i].key);
    groove_ActivationPlan *activation = NULL; groove_NodeId missing;
    bool ok = groove__ivm__graph__IvmGraph__activation_plan(self->graph, changed_tables.ptr, changed_tables.len, changed_bindings.ptr, changed_bindings.len, &activation, &missing);
    jz_vec_drop(&changed_tables, sizeof(jz_str), NULL);
    jz_vec_drop(&changed_bindings, sizeof(groove_BindingSourceKey), NULL);
    jz_err e = JZ_OK;
    if (!ok) {
        e = groove_err_node_not_found(missing);
        jz_vec_drop(&table_deltas, sizeof(groove_TableDelta), SB(TableDelta__drop));
        jz_vec_drop(&binding_deltas, sizeof(groove_BindingDelta), SB(BindingDelta__drop));
        groove__storage__OwnedStorage__drop(&storage);
        if (has_publication) RT(PendingResidentPublication__drop)(&pending_resident_publication);
        return e;
    }
    IncrementalEvaluation ev; memset(&ev, 0, sizeof ev);
    ev.affected_nodes = jz_rc_clone(activation->affected);
    /* Capture only the graph slice reached from changed inputs. */
    ev.relevant_nodes = jz_rc_clone(activation->relevant);
    jz_hmap_init(&ev.operator_states, self->operator_states.d);
    jz_hmap_init(&ev.arrangement_states, self->arrangement_states.d);
    jz_hmap_init(&ev.arrangement_keys_by_input, self->arrangement_keys_by_input.d);
    groove_NodeId *node; void *unused;
    JZ_HMAP_FOR(ev.relevant_nodes, node, unused)
        stage_runtime_node_state(&ev.operator_states, &ev.arrangement_states, &ev.arrangement_keys_by_input, self, *node);
    /* Tick memo entries are disposable. */
    ev.eval_memo = MEMO(default)();
    ev.eval_memo_bytes = 0;
    ev.memo_use_clock = self->memo_use_clock;
    jz_hmap_init(&ev.node_meta, self->node_meta.d);
    JZ_HMAP_FOR(ev.relevant_nodes, node, unused) {
        const groove_NodeRuntimeMeta *meta = jz_hmap_get(&self->node_meta, node);
        if (!meta) continue;
        groove_NodeRuntimeMeta copy; groove_NodeRuntimeMeta_clone(&copy, meta);
        jz_hmap_insert(&ev.node_meta, node, &copy, NULL);
    }
    jz_hmap_init(&ev.table_frontiers, self->table_frontiers.d);
    jz_hmap_init(&ev.binding_frontiers, self->binding_frontiers.d);
    for (u32 i = 0; i < activation->tables.len; i++) {
        const u64 *frontier = jz_hmap_get(&self->table_frontiers, &activation->tables.ptr[i]);
        if (!frontier) continue;
        jz_str key = jz_str_clone(&activation->tables.ptr[i]);
        jz_hmap_insert(&ev.table_frontiers, &key, frontier, NULL);
    }
    for (u32 i = 0; i < activation->bindings.len; i++) {
        const u64 *frontier = jz_hmap_get(&self->binding_frontiers, &activation->bindings.ptr[i]);
        if (!frontier) continue;
        groove_BindingSourceKey key; groove__ivm__op_types__BindingSourceKey__clone(&key, &activation->bindings.ptr[i]);
        jz_hmap_insert(&ev.binding_frontiers, &key, frontier, NULL);
    }
    ev.current_tick = self->current_tick + 1;
    ev.durable_writes = groove__storage__StagedWriteState__default();
    /* Durable nodes run under `&self`, so the binding set cannot change before
     * the incremental evaluation below reuses this snapshot. */
    ev.binding_snapshots = SUBS(binding_snapshot_deltas)(self);
    usize table_delta_records = 0;
    for (u32 i = 0; i < table_deltas.len; i++) table_delta_records += table_deltas.ptr[i].deltas.len;
    ev.table_deltas = table_deltas;
    ev.binding_deltas = binding_deltas;
    ev.storage = storage;
    ES(EvaluationRequests__new)(&ev.requests);
    ev.has_evaluation_inputs = true;
    ES(EvaluationInputs__default)(&ev.evaluation_inputs);
    jz_hmap_init(&ev.published_subscriptions, &groove_ivm_rt_a_hdesc_subscription_id_set);
    jz_hmap_init(&ev.affected_subscriptions, &groove_ivm_rt_a_hdesc_subscription_id_set);
    jz_hmap_init(&ev.restarted_subscriptions, &groove_ivm_rt_a_hdesc_subscription_id_set);
    jz_hmap_init(&ev.pending_subscription_outputs, &hdesc_pending_outputs);
    jz_hmap_init(&ev.terminal_deltas, &groove_ivm_rt_a_hdesc_terminal_deltas);
    jz_hmap_init(&ev.root_ordering_windows, &groove_ivm_rt_a_hdesc_root_windows);
    nset_init(&ev.early_installed);
    nset_init(&ev.held_barriers);
    groove__ivm__runtime__state__TickMetrics__default(&ev.metrics);
    ev.notification_publication = notification_publication;
    ev.defer_notifications_until_durable = defer_notifications_until_durable;
    ev.has_pending_resident_publication = has_publication;
    if (has_publication) ev.pending_resident_publication = pending_resident_publication;
    ev.pending_binding_retractions = pending_binding_retractions;
    /* The work queue is discovered below; give the drop path a valid empty one. */
    jz_hmap_init(&ev.work_queue.unary_batches, &groove_ivm_rt_a_hdesc_pending_unary);
    jz_hmap_init(&ev.work_queue.pipelines, &hdesc_pipelines);
    jz_hmap_init(&ev.work_queue.pipeline_batches, &groove_ivm_rt_PENDING_PIPELINE_MAP);
    jz_btree_init(&ev.work_queue.request_dependents, req_deps_desc());
    jz_deque_init(&ev.work_queue.runnable, sizeof(groove_NodeId));
    nset_init(&ev.work_queue.released_by_earlier_frame);

    e = RT(IvmRuntime__tick_durable_nodes)(self, ev.table_deltas.ptr, ev.table_deltas.len, ev.binding_snapshots, activation->durable.ptr, activation->durable.len,
        ev.current_tick, ev.storage.storage, &ev.operator_states, &ev.arrangement_states, &ev.arrangement_keys_by_input, ev.eval_memo, &ev.eval_memo_bytes,
        &ev.memo_use_clock, &ev.node_meta, &ev.table_frontiers, &ev.binding_frontiers, ev.durable_writes);
    if (e.code) goto fail;
    RT(bump_input_frontiers_staged)(self->graph, ev.table_deltas.ptr, ev.table_deltas.len, ev.binding_deltas.ptr, ev.binding_deltas.len,
                                    &ev.table_frontiers, &ev.binding_frontiers, &ev.node_meta);
    JZ_HMAP_FOR(ev.affected_nodes, node, unused) {
        const jz_hmap *subs = jz_hmap_get(&self->subscriptions_by_output_node, node);
        if (!subs) continue;
        groove_SubscriptionId *sid; void *u2;
        JZ_HMAP_FOR(subs, sid, u2) jz_hmap_insert(&ev.affected_subscriptions, sid, NULL, NULL);
    }
    ev.metrics.tick = ev.current_tick;
    ev.metrics.table_delta_records = table_delta_records;
    ev.metrics.subscriptions_considered = ev.affected_subscriptions.len;
    /* Only plain outputs that carry the TopBy's row identity consume the
     * generic before/after windows. */
    if (self->plain_output_root_positions) {
        groove_SubscriptionId *sid; void *u2;
        JZ_HMAP_FOR(&ev.affected_subscriptions, sid, u2) {
            const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&self->multisink_subscriptions, sid);
            if (!subscription) continue;
            jz_str *sink; groove_CompiledNode *output;
            JZ_BTREE_FOR(&subscription->outputs, sink, output) {
                if (!nset_contains(ev.affected_nodes, output->node) || !output->has_root_ordering_node) continue;
                bool consumes = false;
                e = groove__ivm__runtime__record_projection__output_consumes_root_positions(self->graph, output->node, output->root_ordering_node, &consumes);
                if (e.code) goto fail;
                if (consumes) {
                    bool inserted = false;
                    groove_RootOrderingWindows *w = jz_hmap_entry(&ev.root_ordering_windows, &output->root_ordering_node, &inserted);
                    if (inserted) groove__ivm__runtime__evaluator__RootOrderingWindows__default(w);
                }
            }
        }
        /* A routed TopBy runs before its barriers are known to be touched. */
        const groove_RouteIndex *routes = groove__ivm__graph__IvmGraph__routes(self->graph);
        for (u32 i = 0; i < activation->routed.len; i++) {
            const groove_RouteTable *table = groove__ivm__routes__RouteIndex__table(routes, activation->routed.ptr[i]);
            if (!table) continue;
            groove_NodeId *rn; void *u3;
            JZ_HMAP_FOR(&table->root_ordering_nodes, rn, u3) {
                bool inserted = false;
                groove_RootOrderingWindows *w = jz_hmap_entry(&ev.root_ordering_windows, rn, &inserted);
                if (inserted) groove__ivm__runtime__evaluator__RootOrderingWindows__default(w);
            }
        }
    }
    groove_NodeIdVec active_roots = {0};
    {
        groove_SubscriptionId *sid; void *u2;
        JZ_HMAP_FOR(&ev.affected_subscriptions, sid, u2) {
            const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&self->multisink_subscriptions, sid);
            if (!subscription) continue;
            jz_str *sink; groove_CompiledNode *output;
            JZ_BTREE_FOR(&subscription->outputs, sink, output) {
                if (!nset_contains(ev.affected_nodes, output->node)) continue;
                jz_vec_push(&active_roots, &output->node, sizeof(groove_NodeId));
                if (output->has_root_ordering_node) jz_vec_push(&active_roots, &output->root_ordering_node, sizeof(groove_NodeId));
            }
        }
    }
    for (u32 i = 0; i < activation->ephemeral.len; i++) {
        const groove_NodeRuntimeMeta *meta = jz_hmap_get(&self->node_meta, &activation->ephemeral.ptr[i]);
        if (meta && retainer_is_not_hydration(&meta->retainers)) jz_vec_push(&active_roots, &activation->ephemeral.ptr[i], sizeof(groove_NodeId));
    }
    {
        EvaluationWorkQueue queue;
        e = RT(EvaluationWorkQueue__discover_frame)(self->graph, &self->node_meta, active_roots.ptr, active_roots.len, false, &queue);
        jz_vec_drop(&active_roots, sizeof(groove_NodeId), NULL);
        if (e.code) goto fail;
        RT(EvaluationWorkQueue__drop)(&ev.work_queue);
        ev.work_queue = queue;
    }
    MEMO(set_layout)(ev.eval_memo, jz_rc_clone(ev.work_queue.layout));
    jz_vec_clone(&ev.routed_terminals, &activation->routed, sizeof(groove_NodeId), NULL);
    jz_rc_release(activation);
    *out = ev;
    return JZ_OK;
fail:
    jz_rc_release(activation);
    RT(IncrementalEvaluation__drop)(&ev);
    return e;
}

static jz_err RT(IvmRuntime__begin_tick_with_params)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_BindingDeltaVec binding_deltas,
    groove_OwnedStorage storage, groove_OptPublicationId notification_publication, IncrementalEvaluation *out) {
    groove_PendingResidentPublication none; memset(&none, 0, sizeof none);
    return RT(IvmRuntime__begin_tick_with_params_and_notification_policy)(self, table_deltas, binding_deltas, storage, notification_publication, false, false, none, out);
}

void RTK(bump_input_frontiers)(groove_IvmRuntime *self, const groove_TableDelta *td, u32 ntd, const groove_BindingDelta *bd, u32 nbd) {
    RT(bump_input_frontiers_staged)(self->graph, td, ntd, bd, nbd, &self->table_frontiers, &self->binding_frontiers, &self->node_meta);
}

typedef struct MemoUse { groove_EvalMemoKey key; u64 last_used; } MemoUse;
static int memo_use_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    u64 x = ((const MemoUse *)a)->last_used, y = ((const MemoUse *)b)->last_used;
    return x < y ? -1 : x > y;
}

static void RT(IvmRuntime__evict_eval_memo)(groove_IvmRuntime *self) {
    if (MEMO(len)(self->eval_memo) <= EVAL_MEMO_MAX_ENTRIES && self->eval_memo_bytes <= EVAL_MEMO_MAX_BYTES) return;
    JZ_VEC(MemoUse) entries = {0};
    groove_EvaluationMemoIter it; MEMO(iter)(self->eval_memo, &it);
    const groove_EvalMemoKey *key; groove_EvalMemoEntry *entry;
    while (groove__ivm__runtime__evaluation_memo__EvaluationMemoIter__next(&it, &key, &entry)) {
        MemoUse u = { *key, entry->last_used };
        jz_vec_push(&entries, &u, sizeof u);
    }
    jz_sort_unstable(entries.ptr, entries.len, sizeof(MemoUse), memo_use_cmp, NULL);
    for (u32 i = 0; i < entries.len; i++) {
        if (MEMO(len)(self->eval_memo) <= EVAL_MEMO_MAX_ENTRIES && self->eval_memo_bytes <= EVAL_MEMO_MAX_BYTES) break;
        groove__ivm__runtime__IvmRuntime__remove_retained_eval_memo(self, &entries.ptr[i].key);
    }
    jz_vec_drop(&entries, sizeof(MemoUse), NULL);
}

/* ---- owner wakers ---- */
typedef struct OwnerFanoutWake { jz_waker caller; jz_waker owner; } OwnerFanoutWake;
static void owner_fanout_wake(void *ctx) {
    OwnerFanoutWake *w = ctx;
    wake(&w->caller);
    wake(&w->owner);
}
static bool will_wake(const jz_waker *a, const jz_waker *b) {
    jz_waker noop = { NULL, NULL };
    if (!a) a = &noop;
    if (!b) b = &noop;
    return a->wake == b->wake && a->ctx == b->ctx;
}

/* Remember the durable wake bridge of the runtime owner. */
void RTK(retain_owner_progress_waker)(groove_IvmRuntime *self, const jz_waker *owner) {
    if (self->has_owner_progress_waker && will_wake(&self->owner_progress_waker, owner)) return;
    jz_waker noop = { NULL, NULL };
    self->owner_progress_waker = owner ? *owner : noop;
    self->has_owner_progress_waker = true;
    self->has_owner_fanout_waker = false;
}

/* The waker a poll by `caller` must install on suspended work. False when no
 * owner is known or the caller is the owner. The fanout context is shared by
 * every waker copy and is intentionally never freed (Arc<Wake> in Rust). */
static bool RT(IvmRuntime__owner_fanout_waker_for)(groove_IvmRuntime *self, const jz_waker *caller, jz_waker *out) {
    if (!self->has_owner_progress_waker) return false;
    if (will_wake(&self->owner_progress_waker, caller)) return false;
    if (self->has_owner_fanout_waker && will_wake(&self->owner_fanout_caller, caller)) { *out = self->owner_fanout_waker; return true; }
    OwnerFanoutWake *fan = JZ_NEW(OwnerFanoutWake);
    jz_waker noop = { NULL, NULL };
    fan->caller = caller ? *caller : noop;
    fan->owner = self->owner_progress_waker;
    jz_waker fanout = { owner_fanout_wake, fan };
    self->owner_fanout_caller = fan->caller;
    self->owner_fanout_waker = fanout;
    self->has_owner_fanout_waker = true;
    *out = fanout;
    return true;
}

static u32 RT(IvmRuntime__finish_pending_incremental_poll)(groove_IvmRuntime *self, PendingIncrementalState *slot, PendingIncrementalState state,
                                                           u32 status, jz_err result, jz_err *out) {
    RT(PendingIncrementalState__drop)(slot);
    *slot = state;
    self->pending_incremental_polling = false;
    /* A lifecycle operation may have released retainers while this owner turn
     * held the queue locally. Retry only requested collection. */
    if (self->ephemeral_graph_gc_pending) groove__ivm__runtime__graph_lifecycle__IvmRuntime__collect_unretained_ephemeral_nodes(self);
    *out = result;
    return status;
}

static void release_failed_waiters(groove_IvmRuntime *self, PendingIncrementalState *state, u64 evaluation_id, const groove_NodeIdVec *nodes) {
    for (u32 i = 0; i < nodes->len; i++) {
        groove_NodeId node = nodes->ptr[i];
        jz_deque *waiters = jz_hmap_get(&state->waiters_by_node, &node);
        if (!waiters) continue;
        deque_retain_u64_ne(waiters, evaluation_id);
        bool has_successor = waiters->len != 0;
        u64 successor = has_successor ? *(u64 *)jz_deque_at(waiters, 0) : 0;
        if (waiters->len == 0) jz_hmap_remove(&state->waiters_by_node, &node, NULL, NULL);
        if (has_successor) {
            PendingEvaluation *later = jz_btree_get(&state->evaluations, &successor);
            if (later) RT(PendingEvaluation__temporal_ready)(later, self, node);
        }
    }
}

static jz_err RT(subscription_snapshot_from_hydrated)(const groove_IvmGraph *graph, const jz_btree *outputs, const jz_hmap *hydrated,
                                                     const jz_hmap *terminal_deltas, groove_MultisinkDeltas *out);

static u32 RT(IvmRuntime__poll_incremental)(groove_IvmRuntime *self, const jz_waker *cx, bool resident_only, const jz_hmap *selected_evaluations, jz_err *result) {
    /* debug_assert!(!self.pending_incremental_polling) is debug-only. */
    self->pending_incremental_polling = true;
    /* Suspended storage and chunk futures retain only the waker of their latest
     * poll; install the owner's bridge so an older cold hydration is resumed. */
    jz_waker fanout;
    const jz_waker *owner_cx = RT(IvmRuntime__owner_fanout_waker_for)(self, cx, &fanout) ? &fanout : cx;
    PendingIncrementalState *slot = pending_state(self);
    PendingIncrementalState state = *slot;
    RT(PendingIncrementalState__default)(slot);
    if (state.order.len == 0) return RT(IvmRuntime__finish_pending_incremental_poll)(self, slot, state, 0, JZ_OK, result);
    jz_deque retained_order; jz_deque_init(&retained_order, sizeof(u64));
    u64 evaluation_id;
    while (jz_deque_pop_front(&state.order, &evaluation_id)) {
        PendingEvaluation evaluation;
        u64 removed_key;
        if (!jz_btree_remove(&state.evaluations, &evaluation_id, &removed_key, &evaluation)) jz_panic("pending evaluation order references a live session");
        if ((selected_evaluations && !idset_contains(selected_evaluations, evaluation_id))
            || (resident_only && !RT(PendingEvaluation__has_resident_continuation)(&evaluation))) {
            jz_btree_insert(&state.evaluations, &evaluation_id, &evaluation, NULL);
            jz_deque_push_back(&retained_order, &evaluation_id);
            continue;
        }
        /* Queued evaluations run under whichever caller polls the queue. */
        EvaluationFailure failure; memset(&failure, 0, sizeof failure);
        u32 status;
        if (evaluation.tag == PE_INCREMENTAL) {
            status = RT(IncrementalEvaluation__poll)(evaluation.incremental, self, owner_cx, &failure);
        } else {
            PendingSubscriptionHydration *hydration = evaluation.hydration;
            jz_err herr = JZ_OK;
            status = RT(EvaluationSession__poll)(&hydration->session, self, hydration->binding_snapshots, hydration->hydrate_arrangements,
                                                 &hydration->metrics, owner_cx, &herr);
            if (status == 0 && herr.code) {
                failure.kind = EFK_SCOPED;
                groove_NodeIdVec incomplete = {0};
                RT(EvaluationWorkQueue__incomplete_nodes)(&hydration->session.work_queue, &incomplete);
                nset_init(&failure.affected_nodes);
                for (u32 i = 0; i < incomplete.len; i++) nset_insert(&failure.affected_nodes, incomplete.ptr[i]);
                jz_vec_drop(&incomplete, sizeof(groove_NodeId), NULL);
                failure.error = err_rc_new(herr);
            }
        }
        bool failed = status == 0 && failure.error != NULL;
        /* A hydration session owns a private snapshot of all reachable state;
         * treat the whole session as one temporal barrier. */
        if (evaluation.tag == PE_INCREMENTAL) {
            IncrementalEvaluation *incremental = evaluation.incremental;
            groove_NodeIdVec completed = RT(EvaluationWorkQueue__drain_completed_events)(&incremental->work_queue);
            if (status == 0 && !failed) {
                /* Installed: every node, held ones included, is live. */
                nset_to_vec(&incremental->held_barriers, &completed);
                jz_hmap_clear(&incremental->held_barriers);
                RT(PendingIncrementalState__release_temporal_successors)(&state, self, evaluation_id, completed.ptr, completed.len);
                jz_vec_drop(&completed, sizeof(groove_NodeId), NULL);
            } else if (status != 0) {
                groove_NodeIdVec released = RT(IncrementalEvaluation__write_back_or_hold)(incremental, self, completed);
                RT(PendingIncrementalState__release_temporal_successors)(&state, self, evaluation_id, released.ptr, released.len);
                jz_vec_drop(&released, sizeof(groove_NodeId), NULL);
            } else {
                /* A failed evaluation writes nothing more back. */
                for (u32 i = 0; i < completed.len; i++) nset_insert(&incremental->held_barriers, completed.ptr[i]);
                jz_vec_drop(&completed, sizeof(groove_NodeId), NULL);
            }
        }
        if (status == 0 && !failed) {
            if (evaluation.tag == PE_SUBSCRIPTION_HYDRATION) {
                PendingSubscriptionHydration *hydration = evaluation.hydration;
                groove_MultisinkDeltas snapshot;
                jz_err se = RT(subscription_snapshot_from_hydrated)(self->graph, &hydration->outputs, &hydration->session.outputs,
                                                                    &hydration->session.terminal_deltas, &snapshot);
                groove_NodeIdVec completed = {0};
                RT(EvaluationWorkQueue__registered_nodes)(&hydration->session.work_queue, &completed);
                /* First-result evaluation borrows shared inputs, but its temporary
                 * state is not a maintenance proof. */
                if (hydration->lifetime == GROOVE_SL_RETAINED) {
                    RT(EvaluationSession__install)(&hydration->session, self);
                    hydration->session_consumed = true;
                }
                RT(PendingIncrementalState__release_temporal_successors)(&state, self, evaluation_id, completed.ptr, completed.len);
                jz_vec_drop(&completed, sizeof(groove_NodeId), NULL);
                groove__ivm__runtime__graph_lifecycle__IvmRuntime__record_hydration_memo_metrics(self, &hydration->metrics);
                RT(IvmRuntime__evict_eval_memo)(self);
                if (!se.code) {
                    groove_InitialCell *initial = hydration->initial;
                    if (initial->some) SB(MultisinkDeltas__drop)(&initial->value);
                    initial->some = true;
                    initial->value = snapshot;
                    if (hydration->lifetime == GROOVE_SL_FIRST_RESULT) SUBS(unsubscribe)(self, hydration->subscription_id);
                } else {
                    const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&self->multisink_subscriptions, &hydration->subscription_id);
                    if (subscription) SB(SubscriptionSender__fail)(&subscription->sender, SB(SubscriptionError__new)(err_rc_new(se)));
                    else jz_err_drop(&se);
                    SUBS(unsubscribe)(self, hydration->subscription_id);
                }
            }
            RT(PendingEvaluation__drop)(&evaluation);
        } else if (failed) {
            if (evaluation.tag == PE_SUBSCRIPTION_HYDRATION) {
                const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&self->multisink_subscriptions, &evaluation.hydration->subscription_id);
                if (subscription) SB(SubscriptionSender__fail)(&subscription->sender, SB(SubscriptionError__new)(jz_rc_clone(failure.error)));
                SUBS(unsubscribe)(self, evaluation.hydration->subscription_id);
            }
            if (failure.kind == EFK_FATAL) {
                if (failure.error->code != GROOVE_IVM_ERR_CHUNK) jz_panic("trusted install failures originate from chunk requests");
                RT(IvmRuntime__fail_all_subscriptions)(self, err_rc_new(err_rc_clone_value(failure.error)));
                jz_deque_drop(&state.order, NULL);
                state.order = retained_order;
                RT(PendingEvaluation__drop)(&evaluation);
                return RT(IvmRuntime__finish_pending_incremental_poll)(self, slot, state, 0, RT(EvaluationFailure__into_error)(failure), result);
            }
            /* Held nodes were never installed; reset them too. */
            if (evaluation.tag == PE_INCREMENTAL) {
                groove_NodeId *n; void *u;
                JZ_HMAP_FOR(&evaluation.incremental->held_barriers, n, u) nset_insert(&failure.affected_nodes, *n);
            }
            /* A failed first-result session has not published mutable state. */
            if (!(evaluation.tag == PE_SUBSCRIPTION_HYDRATION && evaluation.hydration->lifetime == GROOVE_SL_FIRST_RESULT))
                RT(IvmRuntime__fail_evaluation_nodes)(self, &failure);
            groove_NodeIdVec released_nodes = {0};
            if (evaluation.tag == PE_SUBSCRIPTION_HYDRATION) {
                RT(EvaluationWorkQueue__registered_nodes)(&evaluation.hydration->session.work_queue, &released_nodes);
            } else {
                RT(EvaluationWorkQueue__incomplete_nodes)(&evaluation.incremental->work_queue, &released_nodes);
                nset_to_vec(&evaluation.incremental->held_barriers, &released_nodes);
                jz_hmap_clear(&evaluation.incremental->held_barriers);
            }
            release_failed_waiters(self, &state, evaluation_id, &released_nodes);
            jz_vec_drop(&released_nodes, sizeof(groove_NodeId), NULL);
            RT(EvaluationFailure__drop)(&failure);
            RT(PendingEvaluation__drop)(&evaluation);
        } else {
            jz_btree_insert(&state.evaluations, &evaluation_id, &evaluation, NULL);
            jz_deque_push_back(&retained_order, &evaluation_id);
            /* A resident slice can discover cold storage; leave it parked. */
            if (resident_only) continue;
            /* One owner turn advances at most one suspended evaluation. */
            u64 id;
            while (jz_deque_pop_front(&state.order, &id)) jz_deque_push_back(&retained_order, &id);
            jz_deque_drop(&state.order, NULL);
            state.order = retained_order;
            return RT(IvmRuntime__finish_pending_incremental_poll)(self, slot, state, GROOVE_POLL_PENDING, JZ_OK, result);
        }
    }
    bool done = retained_order.len == 0;
    jz_deque_drop(&state.order, NULL);
    state.order = retained_order;
    return RT(IvmRuntime__finish_pending_incremental_poll)(self, slot, state, done ? 0 : GROOVE_POLL_PENDING, JZ_OK, result);
}

u32 RTK(poll_pending_incremental)(groove_IvmRuntime *self, const jz_waker *cx, jz_err *result) {
    return RT(IvmRuntime__poll_incremental)(self, cx, false, NULL, result);
}

/* Poll only evaluations that have retained an explicit in-memory continuation. */
u32 RTK(poll_resident_incremental)(groove_IvmRuntime *self, const jz_waker *cx, jz_err *result) {
    return RT(IvmRuntime__poll_incremental)(self, cx, true, NULL, result);
}

/* Finish already-prepared durable writes in temporal order. */
u32 RTK(poll_storage_extraction)(groove_IvmRuntime *self, const groove_OwnedStorage *storage, const jz_waker *cx, jz_err *result) {
    *result = JZ_OK;
    PendingIncrementalState *pending = pending_state(self);
    for (u32 i = 0; i < pending->order.len; i++) {
        u64 id = *(u64 *)jz_deque_at(&pending->order, i);
        PendingEvaluation *evaluation = jz_btree_get(&pending->evaluations, &id);
        if (!evaluation || evaluation->tag != PE_INCREMENTAL) continue;
        jz_err r = JZ_OK;
        if (RT(IncrementalEvaluation__poll_storage_flush)(evaluation->incremental, self->persistence_indeterminate, storage, cx, &r) != 0) return GROOVE_POLL_PENDING;
        if (r.code) { *result = r; return 0; }
    }
    return 0;
}

bool RTK(has_pending_storage_writes)(const groove_IvmRuntime *self) {
    if (!self->pending_incremental) return false;
    u64 *id; PendingEvaluation *evaluation;
    JZ_BTREE_FOR(&self->pending_incremental->evaluations, id, evaluation) {
        if (evaluation->tag == PE_INCREMENTAL
            && (evaluation->incremental->persist_flush || !groove__storage__StagedWriteState__is_empty(evaluation->incremental->durable_writes)))
            return true;
    }
    return false;
}

bool RTK(has_pending_incremental)(const groove_IvmRuntime *self) { return RT(PendingIncrementalEvaluation__is_pending)(self->pending_incremental); }

/* Whether admitted evaluation work (including notifications deferred until
 * durable) can still reach this subscription. */
bool RTK(subscription_has_pending_evaluation)(const groove_IvmRuntime *self, groove_SubscriptionId id) {
    if (self->pending_incremental_polling) return true;
    if (self->pending_incremental) {
        u64 *eid; PendingEvaluation *evaluation;
        JZ_BTREE_FOR(&self->pending_incremental->evaluations, eid, evaluation) {
            if (evaluation->tag == PE_SUBSCRIPTION_HYDRATION) {
                if (groove_ivm_rt_a_eq_subscription_id(&evaluation->hydration->subscription_id, &id)) return true;
            } else if (jz_hmap_get(&evaluation->incremental->affected_subscriptions, &id)) {
                /* Even computed output may still be buffered until the evaluation commits. */
                return true;
            }
        }
    }
    groove_PublicationId *publication; groove_NotificationVec *notifications;
    JZ_BTREE_FOR(&self->deferred_notifications, publication, notifications)
        for (u32 i = 0; i < notifications->len; i++)
            if (groove_ivm_rt_a_eq_subscription_id(&notifications->ptr[i].id, &id)) return true;
    return false;
}

/* Whether an admitted evaluation can still change this receiver's terminal. */
bool RTK(subscription_has_pending_progress)(const groove_IvmRuntime *self, groove_SubscriptionId id) {
    const groove_MultisinkSubscriptionState *s = jz_hmap_get(&self->multisink_subscriptions, &id);
    return !s || s->failed || RTK(subscription_has_pending_evaluation)(self, id);
}

bool RTK(has_resident_continuation)(const groove_IvmRuntime *self) {
    return RT(PendingIncrementalEvaluation__has_resident_continuation)(self->pending_incremental);
}

typedef JZ_VEC(PendingSubscriptionHydration *) HydrationVec;

/* Remove the uninstalled hydrations `evaluation_ids` from the queue. Any
 * successor waiting behind one is released as a completed predecessor would
 * release it, but the hydration's state is never installed. */
static HydrationVec RT(IvmRuntime__remove_pending_hydrations)(groove_IvmRuntime *self, const u64 *evaluation_ids, u32 n) {
    PendingIncrementalState *slot = pending_state(self);
    PendingIncrementalState state = *slot;
    RT(PendingIncrementalState__default)(slot);
    HydrationVec removed = {0};
    for (u32 i = 0; i < n; i++) {
        u64 evaluation_id = evaluation_ids[i], removed_key;
        PendingEvaluation evaluation;
        if (!jz_btree_remove(&state.evaluations, &evaluation_id, &removed_key, &evaluation)) continue;
        deque_retain_u64_ne(&state.order, evaluation_id);
        groove_NodeIdVec nodes = {0};
        RT(EvaluationWorkQueue__registered_nodes)(RT(PendingEvaluation__work_queue)(&evaluation), &nodes);
        for (u32 j = 0; j < nodes.len; j++) {
            groove_NodeId node = nodes.ptr[j];
            jz_deque *waiters = jz_hmap_get(&state.waiters_by_node, &node);
            if (!waiters) continue;
            bool was_front = waiters->len && *(u64 *)jz_deque_at(waiters, 0) == evaluation_id;
            deque_retain_u64_ne(waiters, evaluation_id);
            bool has_successor = waiters->len != 0;
            u64 successor = has_successor ? *(u64 *)jz_deque_at(waiters, 0) : 0;
            if (waiters->len == 0) jz_hmap_remove(&state.waiters_by_node, &node, NULL, NULL);
            if (was_front && has_successor) {
                PendingEvaluation *later = jz_btree_get(&state.evaluations, &successor);
                if (later) RT(PendingEvaluation__temporal_ready)(later, self, node);
            }
        }
        jz_vec_drop(&nodes, sizeof(groove_NodeId), NULL);
        if (evaluation.tag == PE_SUBSCRIPTION_HYDRATION) jz_vec_push(&removed, &evaluation.hydration, sizeof(PendingSubscriptionHydration *));
        else RT(PendingEvaluation__drop)(&evaluation);
    }
    RT(PendingIncrementalState__drop)(slot);
    *slot = state;
    return removed;
}

/* Drop an uninstalled hydration when its subscription is cancelled. */
void RTK(cancel_pending_subscription_hydration)(groove_IvmRuntime *self, groove_SubscriptionId subscription_id) {
    JZ_VEC(u64) cancelled = {0};
    PendingIncrementalState *pending = pending_state(self);
    u64 *id; PendingEvaluation *evaluation;
    JZ_BTREE_FOR(&pending->evaluations, id, evaluation)
        if (evaluation->tag == PE_SUBSCRIPTION_HYDRATION && groove_ivm_rt_a_eq_subscription_id(&evaluation->hydration->subscription_id, &subscription_id))
            jz_vec_push(&cancelled, id, sizeof *id);
    HydrationVec removed = RT(IvmRuntime__remove_pending_hydrations)(self, cancelled.ptr, cancelled.len);
    for (u32 i = 0; i < removed.len; i++) { RT(PendingSubscriptionHydration__drop)(removed.ptr[i]); jz_free(removed.ptr[i]); }
    jz_vec_drop(&removed, sizeof(PendingSubscriptionHydration *), NULL);
    jz_vec_drop(&cancelled, sizeof(u64), NULL);
}

/* Set aside hydrations parked on a large-value chunk fetch so that an
 * overlapping local write can begin (#3901). */
static HydrationVec RT(IvmRuntime__abandon_pending_hydrations)(groove_IvmRuntime *self, const u64 *evaluation_ids, u32 n) {
    HydrationVec abandoned = RT(IvmRuntime__remove_pending_hydrations)(self, evaluation_ids, n);
    for (u32 i = 0; i < abandoned.len; i++)
        if (abandoned.ptr[i]->lifetime == GROOVE_SL_RETAINED)
            SUBS(unindex_subscription_outputs)(self, abandoned.ptr[i]->subscription_id, &abandoned.ptr[i]->outputs);
    return abandoned;
}

/* Restart each hydration abandoned by a local write from the snapshot after
 * that write, read through the write's own `storage`. Consumes both. */
static void RT(IvmRuntime__restart_abandoned_hydrations)(groove_IvmRuntime *self, HydrationVec abandoned, groove_OwnedStorage storage) {
    for (u32 i = 0; i < abandoned.len; i++) {
        PendingSubscriptionHydration *hydration = abandoned.ptr[i];
        if (!jz_hmap_get(&self->multisink_subscriptions, &hydration->subscription_id)) {
            RT(PendingSubscriptionHydration__drop)(hydration); jz_free(hydration);
            continue;
        }
        if (hydration->lifetime == GROOVE_SL_RETAINED) SUBS(index_subscription_outputs)(self, hydration->subscription_id, &hydration->outputs);
        groove_SubscriptionId subscription_id = hydration->subscription_id;
        jz_btree outputs = hydration->outputs;
        jz_btree_init(&hydration->outputs, outputs.d);
        groove_BindingSnapshots *snapshots = hydration->binding_snapshots; hydration->binding_snapshots = NULL;
        groove_InitialCell *initial = hydration->initial; hydration->initial = NULL;
        groove_RootIndirectValues root = hydration->session.root_indirect_values;
        hydration->session.root_indirect_values.tag = GROOVE_RIV_MATERIALIZE; hydration->session.root_indirect_values.physical = NULL;
        jz_hmap borrowed = hydration->session.borrowed;
        nset_init(&hydration->session.borrowed);
        u64 evaluation_id = 0;
        jz_err e = RTK(enqueue_subscription_hydration)(self, subscription_id, outputs, groove__storage__OwnedStorage__clone(&storage), snapshots,
            hydration->has_binding_frontier_advance ? &hydration->binding_frontier_advance : NULL, initial, hydration->lifetime, root, borrowed, &evaluation_id);
        if (e.code) {
            const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&self->multisink_subscriptions, &subscription_id);
            if (subscription) SB(SubscriptionSender__fail)(&subscription->sender, SB(SubscriptionError__new)(err_rc_new(e)));
            else jz_err_drop(&e);
            SUBS(unsubscribe)(self, subscription_id);
            RT(PendingSubscriptionHydration__drop)(hydration); jz_free(hydration);
            continue;
        }
        PendingIncrementalState *pending = pending_state(self);
        PendingEvaluation *restarted = jz_btree_get(&pending->evaluations, &evaluation_id);
        if (!restarted || restarted->tag != PE_SUBSCRIPTION_HYDRATION) jz_panic("a restarted hydration is queued under its id");
        ES(EvaluationRequests__hand_off_chunks)(&hydration->session.requests, &restarted->hydration->session.requests);
        ES(EvaluationInputs__hand_off_chunks)(&hydration->session.evaluation_inputs, &restarted->hydration->session.evaluation_inputs);
        RT(PendingSubscriptionHydration__drop)(hydration); jz_free(hydration);
    }
    jz_vec_drop(&abandoned, sizeof(PendingSubscriptionHydration *), NULL);
    groove__storage__OwnedStorage__drop(&storage);
}

jz_err RTK(drive_pending_incremental)(groove_IvmRuntime *self) {
    jz_err result = JZ_OK;
    while (RTK(poll_pending_incremental)(self, NULL, &result) != 0) await_turn();
    return result;
}

/* ---- public tick entry points ---- */
jz_err RTK(tick)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_OrderedKvStorage *storage, groove_TickMetrics *out) {
    groove_BindingDeltaVec none = {0};
    groove_OptPublicationId no_publication = { false, {0} };
    return RTK(tick_with_params)(self, table_deltas, none, owned_storage(storage), no_publication, out);
}

jz_err RTK(flush_pending_binding_retractions)(groove_IvmRuntime *self, groove_OrderedKvStorage *storage) {
    if (self->pending_binding_retractions.len != 0) {
        /* Unsubscribe may queue routed binding retractions for the next
         * runtime tick. Snapshot hydration also needs a binding snapshot, so
         * it must first bring queued retractions into arranged state. */
        groove_TableDeltaVec no_tables = {0};
        groove_BindingDeltaVec no_bindings = {0};
        groove_OptPublicationId no_publication = { false, {0} };
        groove_TickMetrics metrics;
        JZ_TRY(RTK(tick_with_params)(self, no_tables, no_bindings, owned_storage(storage), no_publication, &metrics));
        groove__ivm__runtime__state__TickMetrics__drop(&metrics);
    }
    return JZ_OK;
}

static jz_err RT(IvmRuntime__tick_admitted)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_BindingDeltaVec binding_deltas,
    groove_OwnedStorage storage, bool defer_notifications_until_durable, bool has_publication, groove_PendingResidentPublication publication,
    u32 detach_on, jz_hmap restarted_subscriptions, groove_TickMetrics *metrics_out, groove_StagedWriteState **durable_out);

/* Shared body of the resident and detaching-cold ticks: admit the write past
 * overlapping hydrations, then run it, detaching cold work. */
static jz_err RT(IvmRuntime__tick_detaching_cold)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_BindingDeltaVec binding_deltas,
    groove_OwnedStorage storage, bool defer_notifications_until_durable, bool has_publication, groove_PendingResidentPublication publication,
    u32 detach_on, groove_TickMetrics *metrics_out, groove_StagedWriteState **durable_out) {
    StrRefVec changed_tables = {0};
    for (u32 i = 0; i < table_deltas.len; i++) str_set_unique_push(&changed_tables, &table_deltas.ptr[i].table);
    /* Beginning the tick folds queued binding retractions into it, so
     * hydration admission must cover their graph slice too. */
    groove_BindingSourceKeyVec changed_bindings = {0};
    for (u32 i = 0; i < binding_deltas.len; i++) binding_set_unique_push(&changed_bindings, &binding_deltas.ptr[i].key);
    if (binding_deltas.len != 0)
        for (u32 i = 0; i < self->pending_binding_retractions.len; i++)
            binding_set_unique_push(&changed_bindings, &self->pending_binding_retractions.ptr[i].key);
    groove_ActivationPlan *activation = NULL; groove_NodeId missing;
    bool ok = groove__ivm__graph__IvmGraph__activation_plan(self->graph, changed_tables.ptr, changed_tables.len, changed_bindings.ptr, changed_bindings.len, &activation, &missing);
    jz_vec_drop(&changed_tables, sizeof(jz_str), NULL);
    jz_vec_drop(&changed_bindings, sizeof(groove_BindingSourceKey), NULL);
    if (!ok) {
        jz_vec_drop(&table_deltas, sizeof(groove_TableDelta), SB(TableDelta__drop));
        jz_vec_drop(&binding_deltas, sizeof(groove_BindingDelta), SB(BindingDelta__drop));
        groove__storage__OwnedStorage__drop(&storage);
        if (has_publication) RT(PendingResidentPublication__drop)(&publication);
        return groove_err_node_not_found(missing);
    }
    jz_hmap *affected_nodes = jz_rc_clone(activation->affected);
    jz_rc_release(activation);
    /* Hydration evaluates an isolated snapshot and installs it atomically. Do
     * not begin a resident tick which overlaps its graph slice. The exception
     * is a hydration parked on a large-value chunk fetch: a table-only write
     * sets it aside and restarts it once the write has begun. A binding tick
     * keeps waiting: the hydration's binding snapshot would be stale. */
    bool abandon_parked_hydrations = binding_deltas.len == 0;
    HydrationVec abandoned = {0};
    jz_err admitted = JZ_OK;
    for (;;) { /* poll_fn(..).await */
        bool resident_only = false;
        u32 status = 0;
        for (;;) {
            jz_hmap selected;
            RT(PendingIncrementalState__hydration_admission_evaluations)(pending_state(self), affected_nodes, &selected);
            if (selected.len == 0) { jz_hmap_drop(&selected); status = 0; break; }
            jz_err polled = JZ_OK;
            u32 ps = RT(IvmRuntime__poll_incremental)(self, NULL, resident_only, &selected, &polled);
            jz_hmap_drop(&selected);
            if (ps == 0 && polled.code) { admitted = polled; status = 0; break; }
            jz_err_drop(&polled);
            const PendingIncrementalState *pending = pending_state(self);
            jz_hmap remaining;
            RT(PendingIncrementalState__hydration_admission_evaluations)(pending, affected_nodes, &remaining);
            if (remaining.len == 0) { jz_hmap_drop(&remaining); status = 0; break; }
            bool any_resident = false;
            u64 *id; void *unused;
            JZ_HMAP_FOR(&remaining, id, unused) {
                const PendingEvaluation *ev = jz_btree_get(&pending->evaluations, id);
                if (ev && RT(PendingEvaluation__has_resident_continuation)(ev)) { any_resident = true; break; }
            }
            if (!any_resident) {
                if (!abandon_parked_hydrations) { jz_hmap_drop(&remaining); status = GROOVE_POLL_PENDING; break; }
                JZ_VEC(u64) parked = {0};
                JZ_HMAP_FOR(&remaining, id, unused) {
                    const PendingEvaluation *ev = jz_btree_get(&pending->evaluations, id);
                    if (ev && ev->tag == PE_SUBSCRIPTION_HYDRATION && ES(EvaluationRequests__has_pending_chunk)(&ev->hydration->session.requests))
                        jz_vec_push(&parked, id, sizeof *id);
                }
                jz_hmap_drop(&remaining);
                if (parked.len == 0) { jz_vec_drop(&parked, sizeof(u64), NULL); status = GROOVE_POLL_PENDING; break; }
                /* Local writes never wait on chunk fetches (#3901). */
                HydrationVec set_aside = RT(IvmRuntime__abandon_pending_hydrations)(self, parked.ptr, parked.len);
                jz_vec_extend(&abandoned, set_aside.ptr, set_aside.len, sizeof(PendingSubscriptionHydration *));
                jz_vec_drop(&set_aside, sizeof(PendingSubscriptionHydration *), NULL);
                jz_vec_drop(&parked, sizeof(u64), NULL);
                resident_only = false;
                continue;
            }
            jz_hmap_drop(&remaining);
            /* The direct write owns CPU-only continuations needed to finish an
             * overlapping hydration. Never re-poll cold requests here. */
            resident_only = true;
        }
        if (status == 0) break;
        await_turn();
    }
    jz_rc_release(affected_nodes);
    groove_OwnedStorage restart_storage = groove__storage__OwnedStorage__clone(&storage);
    jz_hmap restarted_subscriptions; jz_hmap_init(&restarted_subscriptions, &groove_ivm_rt_a_hdesc_subscription_id_set);
    for (u32 i = 0; i < abandoned.len; i++) jz_hmap_insert(&restarted_subscriptions, &abandoned.ptr[i]->subscription_id, NULL, NULL);
    jz_err ticked;
    if (!admitted.code) {
        ticked = RT(IvmRuntime__tick_admitted)(self, table_deltas, binding_deltas, storage, defer_notifications_until_durable,
                                               has_publication, publication, detach_on, restarted_subscriptions, metrics_out, durable_out);
    } else {
        ticked = admitted;
        jz_hmap_drop(&restarted_subscriptions);
        jz_vec_drop(&table_deltas, sizeof(groove_TableDelta), SB(TableDelta__drop));
        jz_vec_drop(&binding_deltas, sizeof(groove_BindingDelta), SB(BindingDelta__drop));
        groove__storage__OwnedStorage__drop(&storage);
        if (has_publication) RT(PendingResidentPublication__drop)(&publication);
    }
    /* Restart on every outcome: an abandoned subscription still awaits its
     * first result. */
    RT(IvmRuntime__restart_abandoned_hydrations)(self, abandoned, restart_storage);
    return ticked;
}

/* The part of tick_detaching_cold after hydration admission. */
static jz_err RT(IvmRuntime__tick_admitted)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_BindingDeltaVec binding_deltas,
    groove_OwnedStorage storage, bool defer_notifications_until_durable, bool has_publication, groove_PendingResidentPublication publication,
    u32 detach_on, jz_hmap restarted_subscriptions, groove_TickMetrics *metrics_out, groove_StagedWriteState **durable_out) {
    jz_hmap temporal_blockers;
    temporal_blockers_of(pending_state(self), &temporal_blockers);
    groove_OptPublicationId no_publication = { false, {0} };
    IncrementalEvaluation evaluation;
    jz_err e = RT(IvmRuntime__begin_tick_with_params_and_notification_policy)(self, table_deltas, binding_deltas, storage, no_publication,
        defer_notifications_until_durable, has_publication, publication, &evaluation);
    if (e.code) { jz_hmap_drop(&temporal_blockers); jz_hmap_drop(&restarted_subscriptions); return e; }
    RT(EvaluationWorkQueue__add_temporal_blockers)(&evaluation.work_queue, &temporal_blockers);
    jz_hmap_drop(&temporal_blockers);
    jz_hmap_drop(&evaluation.restarted_subscriptions);
    evaluation.restarted_subscriptions = restarted_subscriptions;
    EvaluationFailure failure;
    u32 status;
    for (;;) {
        memset(&failure, 0, sizeof failure);
        status = RT(IncrementalEvaluation__poll)(&evaluation, self, NULL, &failure);
        if (status == 0 && failure.error && failure.kind == EFK_SCOPED) {
            RT(IvmRuntime__fail_evaluation_nodes)(self, &failure);
            RT(IncrementalEvaluation__abandon)(&evaluation, &failure.affected_nodes);
            RT(EvaluationFailure__drop)(&failure);
            continue;
        }
        if (status != 0 && evaluation.work_queue.runnable.len != 0) {
            /* A resumable operator deliberately yielded while it remains
             * runnable: keep this applying future alive. */
            await_turn();
            continue;
        }
        if (status != 0 && detach_on == GROOVE_DETACH_CHUNK_REQUEST && !ES(EvaluationRequests__has_pending_chunk)(&evaluation.requests)) {
            /* Storage completes without this caller's turn; await it inline.
             * Only a chunk fetch is worth detaching. */
            await_turn();
            continue;
        }
        break;
    }
    tick_metrics_clone(metrics_out, &evaluation.metrics);
    *durable_out = jz_rc_clone(evaluation.durable_writes);
    if (status == 0) {
        if (failure.error) {
            groove__ivm__runtime__state__TickMetrics__drop(metrics_out);
            jz_rc_release(*durable_out); *durable_out = NULL;
            RT(IncrementalEvaluation__drop)(&evaluation);
            return RT(EvaluationFailure__into_error)(failure);
        }
        RT(IncrementalEvaluation__drop)(&evaluation);
        return JZ_OK;
    }
    RT(EvaluationWorkQueue__discard_unregistered_completions)(&evaluation.work_queue);
    RT(IncrementalEvaluation__install_input_frontiers)(&evaluation, self);
    /* Nothing waits on a node this evaluation completed before it parked, so
     * write the finished state back now (#3815). */
    groove_NodeIdVec completed = {0};
    RT(EvaluationWorkQueue__complete_nodes)(&evaluation.work_queue, &completed);
    groove_NodeIdVec written = RT(IncrementalEvaluation__write_back_or_hold)(&evaluation, self, completed);
    jz_vec_drop(&written, sizeof(groove_NodeId), NULL);
    /* A later evaluation restages state stamped at this tick, so it must
     * begin at a later tick. */
    if (evaluation.current_tick > self->current_tick) self->current_tick = evaluation.current_tick;
    PendingIncrementalState *pending = pending_state(self);
    u64 evaluation_id = pending->next_id;
    pending->next_id = pending->next_id == UINT64_MAX ? UINT64_MAX : pending->next_id + 1;
    groove_NodeIdVec registered = {0};
    RT(EvaluationWorkQueue__incomplete_nodes)(&evaluation.work_queue, &registered);
    nset_to_vec(&evaluation.held_barriers, &registered);
    register_waiters(pending, registered.ptr, registered.len, evaluation_id);
    jz_vec_drop(&registered, sizeof(groove_NodeId), NULL);
    PendingEvaluation queued; memset(&queued, 0, sizeof queued);
    queued.tag = PE_INCREMENTAL;
    queued.incremental = JZ_NEW(IncrementalEvaluation);
    *queued.incremental = evaluation;
    jz_btree_insert(&pending->evaluations, &evaluation_id, &queued, NULL);
    jz_deque_push_back(&pending->order, &evaluation_id);
    return JZ_OK;
}

static void opt_publication_drop(void *p) { (void)p; }
static void bool_cell_drop(void *p) { (void)p; }

jz_err RTK(tick_resident_staged)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_OwnedStorage storage,
    bool defer_notifications_until_durable, bool has_install, void *install_observer, void *install_failures, groove_ResidentTick *out) {
    groove_PendingResidentPublication publication;
    publication.publication = jz_rc_new(sizeof(groove_OptPublicationId), opt_publication_drop);
    publication.publication->some = false;
    publication.notifications = jz_rc_new(sizeof(groove_NotificationVec), notification_vec_rc_drop);
    memset(publication.notifications, 0, sizeof *publication.notifications);
    publication.completed = jz_rc_new(sizeof(bool), bool_cell_drop);
    *publication.completed = false;
    publication.defer_notifications_until_durable = defer_notifications_until_durable;
    publication.chunk_provider = has_install
        ? groove__chunks__OwnedChunkProvider__with_install_observer(self->chunk_provider, install_observer, install_failures) : NULL;
    groove_PendingResidentPublication shared;
    RT(PendingResidentPublication__clone)(&shared, &publication);
    groove_BindingDeltaVec none = {0};
    groove_TickMetrics metrics; groove_StagedWriteState *durable_writes = NULL;
    jz_err e = RT(IvmRuntime__tick_detaching_cold)(self, table_deltas, none, storage, defer_notifications_until_durable, true, shared,
                                                   GROOVE_DETACH_ANY_REQUEST, &metrics, &durable_writes);
    if (e.code) { RT(PendingResidentPublication__drop)(&publication); return e; }
    out->metrics = metrics;
    out->durable_writes = durable_writes;
    out->publication = publication;
    return JZ_OK;
}

/* Drive one tick of runtime-owned input changes without waiting for remote
 * chunks; chunk-blocked work is retained as pending incremental progress. */
jz_err RTK(tick_bindings_detaching_cold)(groove_IvmRuntime *self, groove_BindingDeltaVec binding_deltas, groove_OwnedStorage storage, groove_TickMetrics *out) {
    if (*self->persistence_indeterminate) {
        jz_vec_drop(&binding_deltas, sizeof(groove_BindingDelta), SB(BindingDelta__drop));
        groove__storage__OwnedStorage__drop(&storage);
        return jz_err_new(GROOVE_IVM_ERR_PERSISTENCE_OUTCOME_INDETERMINATE, "runtime persistence outcome is indeterminate; reopen before continuing");
    }
    groove_TableDeltaVec none = {0};
    groove_PendingResidentPublication no_publication; memset(&no_publication, 0, sizeof no_publication);
    groove_StagedWriteState *durable_writes = NULL;
    JZ_TRY(RT(IvmRuntime__tick_detaching_cold)(self, none, binding_deltas, storage, false, false, no_publication,
                                               GROOVE_DETACH_CHUNK_REQUEST, out, &durable_writes));
    jz_rc_release(durable_writes);
    return JZ_OK;
}

/* Consumes `notifications`. */
static void RT(IvmRuntime__send_deferred_notifications)(groove_IvmRuntime *self, groove_NotificationVec notifications) {
    JZ_VEC(groove_SubscriptionId) dropped = {0};
    for (u32 i = 0; i < notifications.len; i++) {
        groove_SubscriptionNotification n = notifications.ptr[i];
        const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&self->multisink_subscriptions, &n.id);
        if (!subscription) { SB(QueuedMultisinkDeltas__drop)(&n.queued); continue; }
        groove_QueuedMultisinkDeltas returned;
        if (!SB(SubscriptionSender__send)(&subscription->sender, n.queued, &returned)) {
            SB(QueuedMultisinkDeltas__drop)(&returned);
            jz_vec_push(&dropped, &n.id, sizeof n.id);
        }
    }
    jz_vec_drop(&notifications, sizeof(groove_SubscriptionNotification), NULL);
    for (u32 i = 0; i < dropped.len; i++) SUBS(unsubscribe)(self, dropped.ptr[i]);
    jz_vec_drop(&dropped, sizeof(groove_SubscriptionId), NULL);
}

void RTK(assign_resident_publication)(groove_IvmRuntime *self, groove_ResidentTick tick, groove_PublicationId publication, groove_TickMetrics *out) {
    groove_OptPublicationId previous = *tick.publication.publication;
    tick.publication.publication->some = true;
    tick.publication.publication->id = publication;
    if (previous.some) jz_panic("assertion `left == right` failed: resident publication already assigned");
    groove_NotificationVec notifications = *tick.publication.notifications;
    memset(tick.publication.notifications, 0, sizeof *tick.publication.notifications);
    for (u32 i = 0; i < notifications.len; i++) {
        notifications.ptr[i].queued.has_publication = true;
        notifications.ptr[i].queued.publication = publication;
    }
    if (tick.publication.defer_notifications_until_durable) {
        groove_NotificationVec *deferred = jz_btree_entry(&self->deferred_notifications, &publication, NULL);
        jz_vec_extend(deferred, notifications.ptr, notifications.len, sizeof(groove_SubscriptionNotification));
        jz_vec_drop(&notifications, sizeof(groove_SubscriptionNotification), NULL);
        if (*tick.publication.completed) jz_btree_insert(&self->completed_deferred_publications, &publication, NULL, NULL);
    } else {
        RT(IvmRuntime__send_deferred_notifications)(self, notifications);
    }
    *out = tick.metrics;
    RT(PendingResidentPublication__drop)(&tick.publication);
    jz_rc_release(tick.durable_writes);
}

void RTK(settle_deferred_notifications)(groove_IvmRuntime *self, groove_PublicationId publication) {
    groove_PublicationId removed_key;
    groove_NotificationVec notifications;
    if (jz_btree_remove(&self->completed_deferred_publications, &publication, NULL, NULL)) {
        if (jz_btree_remove(&self->deferred_notifications, &publication, &removed_key, &notifications))
            RT(IvmRuntime__send_deferred_notifications)(self, notifications);
        return;
    }
    jz_btree_insert(&self->durable_notification_publications, &publication, NULL, NULL);
    if (!jz_btree_remove(&self->deferred_notifications, &publication, &removed_key, &notifications)) return;
    RT(IvmRuntime__send_deferred_notifications)(self, notifications);
}

void RTK(discard_deferred_notifications)(groove_IvmRuntime *self, groove_PublicationId publication) {
    jz_btree_remove(&self->deferred_notifications, &publication, NULL, NULL);
    jz_btree_remove(&self->completed_deferred_publications, &publication, NULL, NULL);
    jz_btree_remove(&self->durable_notification_publications, &publication, NULL, NULL);
}

jz_err RTK(tick_with_params)(groove_IvmRuntime *self, groove_TableDeltaVec table_deltas, groove_BindingDeltaVec binding_deltas,
    groove_OwnedStorage storage, groove_OptPublicationId notification_publication, groove_TickMetrics *out) {
    jz_err e = JZ_OK;
    if (*self->persistence_indeterminate)
        e = jz_err_new(GROOVE_IVM_ERR_PERSISTENCE_OUTCOME_INDETERMINATE, "runtime persistence outcome is indeterminate; reopen before continuing");
    if (!e.code) e = RTK(drive_pending_incremental)(self);
    if (e.code) {
        jz_vec_drop(&table_deltas, sizeof(groove_TableDelta), SB(TableDelta__drop));
        jz_vec_drop(&binding_deltas, sizeof(groove_BindingDelta), SB(BindingDelta__drop));
        groove__storage__OwnedStorage__drop(&storage);
        return e;
    }
    IncrementalEvaluation evaluation;
    JZ_TRY(RT(IvmRuntime__begin_tick_with_params)(self, table_deltas, binding_deltas, storage, notification_publication, &evaluation));
    EvaluationFailure failure; memset(&failure, 0, sizeof failure);
    while (RT(IncrementalEvaluation__poll)(&evaluation, self, NULL, &failure) != 0) await_turn();
    if (failure.error) {
        RT(IncrementalEvaluation__drop)(&evaluation);
        return RT(EvaluationFailure__into_error)(failure);
    }
    *out = evaluation.metrics;
    groove__ivm__runtime__state__TickMetrics__default(&evaluation.metrics);
    RT(IncrementalEvaluation__drop)(&evaluation);
    return JZ_OK;
}

/* ---- hydration snapshots ---- */
/* Consumes `owned_storage`, `binding_snapshots` (nullable rc) and `root_indirect_values`. */
static jz_err RT(IvmRuntime__hydration_roots_owned)(groove_IvmRuntime *self, const groove_NodeId *roots, u32 nroots, groove_OwnedStorage owned_storage,
    u32 mode, groove_BindingSnapshots *binding_snapshots, const jz_str *binding_frontier_advance, groove_RootIndirectValues root_indirect_values, jz_hmap *out) {
    if (!binding_snapshots) binding_snapshots = SUBS(binding_snapshot_deltas)(self);
    groove_TickMetrics metrics; groove__ivm__runtime__state__TickMetrics__default(&metrics);
    jz_err e = JZ_OK;
    bool hydrate_arrangements = false;
    if (nroots == 0) e = groove_err_unsupported_operator();
    if (!e.code && mode == GROOVE_HYDRATION_MODE_SUBSCRIPTION) e = roots_depend_on_aggregate(self, roots, nroots, &hydrate_arrangements);
    if (e.code) {
        groove__storage__OwnedStorage__drop(&owned_storage);
        SB(RootIndirectValues__drop)(&root_indirect_values);
        jz_rc_release(binding_snapshots);
        groove__ivm__runtime__state__TickMetrics__drop(&metrics);
        return e;
    }
    EvaluationSession session;
    e = RT(EvaluationSession__hydration)(self, roots, nroots, owned_storage, &session);
    if (e.code) {
        SB(RootIndirectValues__drop)(&root_indirect_values);
        jz_rc_release(binding_snapshots);
        groove__ivm__runtime__state__TickMetrics__drop(&metrics);
        return e;
    }
    SB(RootIndirectValues__drop)(&session.root_indirect_values);
    session.root_indirect_values = root_indirect_values;
    if (binding_frontier_advance) RT(EvaluationSession__advance_binding_input)(&session, self->graph, binding_frontier_advance);
    jz_err result = JZ_OK;
    while (RT(EvaluationSession__poll)(&session, self, binding_snapshots, hydrate_arrangements, &metrics, NULL, &result) != 0) await_turn();
    jz_rc_release(binding_snapshots);
    if (result.code) {
        RT(EvaluationSession__drop)(&session);
        groove__ivm__runtime__state__TickMetrics__drop(&metrics);
        return result;
    }
    *out = session.outputs;
    jz_hmap_init(&session.outputs, out->d);
    RT(EvaluationSession__install)(&session, self);
    groove__ivm__runtime__graph_lifecycle__IvmRuntime__record_hydration_memo_metrics(self, &metrics);
    groove__ivm__runtime__state__TickMetrics__drop(&metrics);
    RT(IvmRuntime__evict_eval_memo)(self);
    return JZ_OK;
}

jz_err RTK(hydration_snapshot_with_root_values)(groove_IvmRuntime *self, groove_NodeId output_node, groove_OrderedKvStorage *storage, u32 mode,
    groove_RootIndirectValues root_indirect_values, groove_RecordDeltas *out) {
    jz_hmap hydrated;
    JZ_TRY(RT(IvmRuntime__hydration_roots_owned)(self, &output_node, 1, owned_storage(storage), mode, NULL, NULL, root_indirect_values, &hydrated));
    groove_NodeId removed_key;
    bool found = jz_hmap_remove(&hydrated, &output_node, &removed_key, out);
    jz_hmap_drop(&hydrated);
    if (!found) return groove_err_node_not_found(output_node);
    return JZ_OK;
}

jz_err RTK(hydration_snapshot)(groove_IvmRuntime *self, groove_NodeId output_node, groove_OrderedKvStorage *storage, u32 mode, groove_RecordDeltas *out) {
    groove_RootIndirectValues materialize; memset(&materialize, 0, sizeof materialize);
    materialize.tag = GROOVE_RIV_MATERIALIZE;
    return RTK(hydration_snapshot_with_root_values)(self, output_node, storage, mode, materialize, out);
}

static jz_err RT(IvmRuntime__hydration_snapshots_with_binding_snapshots)(groove_IvmRuntime *self, const jz_btree *outputs, groove_OrderedKvStorage *storage,
    u32 mode, groove_BindingSnapshots *binding_snapshots, const jz_str *binding_frontier_advance, groove_MultisinkDeltas *out) {
    groove_NodeIdVec roots = {0};
    outputs_roots(outputs, &roots);
    groove_RootIndirectValues materialize; memset(&materialize, 0, sizeof materialize);
    materialize.tag = GROOVE_RIV_MATERIALIZE;
    jz_hmap hydrated;
    jz_err e = RT(IvmRuntime__hydration_roots_owned)(self, roots.ptr, roots.len, owned_storage(storage), mode, binding_snapshots,
                                                     binding_frontier_advance, materialize, &hydrated);
    jz_vec_drop(&roots, sizeof(groove_NodeId), NULL);
    if (e.code) return e;
    jz_hmap no_terminals; jz_hmap_init(&no_terminals, &groove_ivm_rt_a_hdesc_terminal_deltas);
    e = RT(subscription_snapshot_from_hydrated)(self->graph, outputs, &hydrated, &no_terminals, out);
    jz_hmap_drop(&no_terminals);
    jz_hmap_drop(&hydrated);
    return e;
}

jz_err RTK(hydration_snapshots)(groove_IvmRuntime *self, const jz_btree *outputs, groove_OrderedKvStorage *storage, u32 mode, groove_MultisinkDeltas *out) {
    return RT(IvmRuntime__hydration_snapshots_with_binding_snapshots)(self, outputs, storage, mode, NULL, NULL, out);
}

/* Locate the public collector that belongs to one output and return its
 * hydration seed operations. `Collect` and `Root` both own an opaque
 * terminal key and must seed an opening through the same terminal
 * representation as updates. */
static jz_err RT(terminal_delta_for_hydrated_output)(const groove_IvmGraph *graph, groove_NodeId output, const jz_hmap *terminal_deltas,
                                                    bool *has_public_collector_out, bool *has_terminal, groove_TerminalDeltas *terminal_out) {
    groove_NodeIdVec pending = {0};
    jz_vec_push(&pending, &output, sizeof output);
    jz_hmap seen; nset_init(&seen);
    const groove_TerminalDeltas *fallback = NULL;
    bool has_public_collector = false;
    *has_terminal = false;
    jz_err e = JZ_OK;
    while (pending.len) {
        groove_NodeId node = pending.ptr[--pending.len];
        if (!nset_insert(&seen, node)) continue;
        const groove_GraphNode *graph_node = groove__ivm__graph__IvmGraph__node(graph, node);
        if (!graph_node) { e = groove_err_node_not_found(node); goto done; }
        bool is_public_collector = graph_node->descriptor.op.tag == GROOVE_OP_COLLECT_BY
            && (graph_node->descriptor.op.v.collect_by->mode == GROOVE_CBM_COLLECT || graph_node->descriptor.op.v.collect_by->mode == GROOVE_CBM_ROOT);
        has_public_collector |= is_public_collector;
        const groove_TerminalDeltas *terminal = jz_hmap_get(terminal_deltas, &node);
        if (terminal) {
            if (is_public_collector) {
                *has_public_collector_out = true;
                *has_terminal = true;
                groove__ivm__runtime__terminal__TerminalDeltas__clone(terminal_out, terminal);
                goto done;
            }
            if (!fallback) fallback = terminal;
        }
        jz_vec_extend(&pending, graph_node->descriptor.inputs.ptr, graph_node->descriptor.inputs.len, sizeof(groove_NodeId));
    }
    *has_public_collector_out = has_public_collector;
    if (!has_public_collector && fallback) {
        *has_terminal = true;
        groove__ivm__runtime__terminal__TerminalDeltas__clone(terminal_out, fallback);
    }
done:
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&seen);
    return e;
}

static jz_err RT(subscription_snapshot_from_hydrated)(const groove_IvmGraph *graph, const jz_btree *outputs, const jz_hmap *hydrated,
                                                     const jz_hmap *terminal_deltas, groove_MultisinkDeltas *out) {
    groove_MultisinkDeltas result; SB(MultisinkDeltas__default)(&result);
    jz_err e = JZ_OK;
    jz_str *sink; groove_CompiledNode *output;
    JZ_BTREE_FOR(outputs, sink, output) {
        bool has_ordering = false; groove_RecordDeltas ordering;
        if (output->has_root_ordering_node) {
            const groove_RecordDeltas *found = jz_hmap_get(hydrated, &output->root_ordering_node);
            if (!found) { e = groove_err_node_not_found(output->root_ordering_node); goto fail; }
            SB(RecordDeltas__clone)(&ordering, found);
            has_ordering = true;
        }
        const groove_RecordDeltas *found = jz_hmap_get(hydrated, &output->node);
        if (!found) { if (has_ordering) SB(RecordDeltas__drop)(&ordering); e = groove_err_node_not_found(output->node); goto fail; }
        groove_RecordDeltas records; SB(RecordDeltas__clone)(&records, found);
        if (!groove__records__RecordDescriptor__registry_compatible_with(records.descriptor, output->output)) {
            if (has_ordering) SB(RecordDeltas__drop)(&ordering);
            SB(RecordDeltas__drop)(&records);
            e = groove_err_output_mismatch(); goto fail;
        }
        if (has_ordering) {
            e = groove__ivm__runtime__terminal__order_terminal_snapshot(&records, &ordering);
            SB(RecordDeltas__drop)(&ordering);
            if (e.code) { SB(RecordDeltas__drop)(&records); goto fail; }
        }
        bool has_public_collector = false, has_terminal = false; groove_TerminalDeltas terminal;
        e = RT(terminal_delta_for_hydrated_output)(graph, output->node, terminal_deltas, &has_public_collector, &has_terminal, &terminal);
        if (e.code) { SB(RecordDeltas__drop)(&records); goto fail; }
        /* A hydrated root collector has no before-image, but it still owns the
         * exact terminal key later incremental edits address. */
        if (!has_terminal && has_public_collector) {
            e = groove__ivm__runtime__terminal__terminal_deltas_from_record_deltas(&records, &terminal);
            if (e.code) { SB(RecordDeltas__drop)(&records); goto fail; }
            has_terminal = true;
        }
        if (has_terminal) {
            if (!groove__ivm__runtime__terminal__TerminalDeltas__is_empty(&terminal)) {
                jz_str key; groove_ivm_rt_a_clone_str(&key, sink);
                jz_btree_insert(&result.terminal_sinks, &key, &terminal, NULL);
            } else {
                groove__ivm__runtime__terminal__TerminalDeltas__drop(&terminal);
            }
        }
        jz_str key; groove_ivm_rt_a_clone_str(&key, sink);
        jz_btree_insert(&result.sinks, &key, &records, NULL);
    }
    *out = result;
    return JZ_OK;
fail:
    SB(MultisinkDeltas__drop)(&result);
    return e;
}

