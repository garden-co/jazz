/* groove::ivm::runtime::recursion: recursive operator state and scoped
 * fixpoint evaluation. Async evaluation is synchronous here: storage reads
 * complete inline, and the EvaluationBlocked/yield protocol is preserved. */
#include "mod.h"

#define REC(name) groove__ivm__runtime__recursion__##name
#define RS(name) groove__ivm__runtime__recursion__RecursiveState__##name
#define NS(name) groove__ivm__runtime__operator_updates__NodeState__##name
#define CONSOLIDATE groove__ivm__runtime__key_encoding__consolidate_deltas

const jz_hmap_desc groove_ivm_rt_BYTES_I64_HMAP = { sizeof(jz_bytes), sizeof(i64), groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, NULL };
static const jz_hmap_desc BYTES_HSET = { sizeof(jz_bytes), 0, groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, NULL };
static void deltas_drop_v(void *d) { groove_RecordDeltas_drop(d); }
static const jz_hmap_desc NODE_DELTAS_MAP = { sizeof(groove_NodeId), sizeof(groove_RecordDeltas), groove_NodeId_hash, groove_NodeId_eq, NULL, deltas_drop_v };

#define MAX_HYDRATION_TRAVERSAL_NODES_PER_POLL 32

static void record_delta_vec_drop(groove_RecordDeltaVec *v) { jz_vec_drop(v, sizeof(groove_RecordDelta), groove_RecordDelta_drop); }
static groove_RecordDeltas empty_deltas(groove_RecordDescriptor d) { groove_RecordDeltas r = { d, {0} }; return r; }

/* HashMap<Bytes, i64> clone (derive(Clone)). */
static void bytes_i64_hmap_clone(jz_hmap *dst, const jz_hmap *src) {
    jz_hmap_init(dst, &groove_ivm_rt_BYTES_I64_HMAP);
    jz_bytes *k; i64 *v;
    JZ_HMAP_FOR(src, k, v) { jz_bytes kc = jz_bytes_clone(k); jz_hmap_insert(dst, &kc, v, NULL); }
}
static void bytes_i64_hmap_clone_v(void *dst, const void *src) { bytes_i64_hmap_clone(dst, src); }
static void *rc_bytes_i64_new(void) {
    jz_hmap *m = jz_rc_new(sizeof(jz_hmap), groove_ivm_rt_hmap_drop_v);
    jz_hmap_init(m, &groove_ivm_rt_BYTES_I64_HMAP);
    return m;
}
static void delta_vec_clone_v(void *dst, const void *src) { jz_vec_clone(dst, src, sizeof(groove_RecordDelta), groove_RecordDelta_clone); }
static void delta_vec_drop_v(void *v) { record_delta_vec_drop(v); }
static void *rc_delta_vec_new(void) {
    groove_RecordDeltaVec *v = jz_rc_new(sizeof(groove_RecordDeltaVec), delta_vec_drop_v);
    memset(v, 0, sizeof *v);
    return v;
}

/* ---------- traversal / pending hydration ---------- */
typedef enum { Frame_Visit, Frame_Evaluate } FrameTag;
typedef struct Frame { u8 tag; groove_NodeId node; } Frame;
typedef struct HydrationTraversal {
    groove_NodeId root;
    groove_EvalContext context;
    JZ_VEC(Frame) discovery;
    jz_hmap discovered, visiting; /* HashSet<NodeId> */
    groove_NodeIdVec order;
    usize next_evaluation;
    jz_hmap memo; /* NodeId -> RecordDeltas */
} HydrationTraversal;

typedef enum { Phase_Seed, Phase_Step, Phase_StepWitness, Phase_ReadyForArrangementHydration } PhaseTag;
typedef struct PendingHydrationPhase { u8 tag; groove_RecordDeltas frontier; groove_RecordDeltas step_delta; usize sub_tick; } PendingHydrationPhase;

typedef struct PendingHydrationRecompute {
    u64 input_generation;
    jz_hmap accumulated;
    jz_hmap step_witness_accumulated;
    PendingHydrationPhase phase;
    HydrationTraversal *traversal; /* Option */
} PendingHydrationRecompute;

struct groove_RecursiveState {
    jz_hmap *accumulated;              /* Rc<HashMap<Bytes, i64>> */
    jz_hmap *staged_positive;          /* Rc<HashMap<Bytes, i64>> */
    jz_hmap *step_witness_accumulated; /* Rc<HashMap<Bytes, i64>> */
    groove_RecordDeltaVec *step_witness_tick_deltas; /* Rc<Vec<RecordDelta>> */
    bool step_arrangements_hydrated;
    bool has_hydrated_input_generation; u64 hydrated_input_generation;
    PendingHydrationRecompute *pending_hydration; /* Option<Box<..>> */
};

static void phase_drop(PendingHydrationPhase *p) {
    if (p->tag == Phase_Step || p->tag == Phase_StepWitness) groove_RecordDeltas_drop(&p->frontier);
    if (p->tag == Phase_StepWitness) groove_RecordDeltas_drop(&p->step_delta);
    p->tag = Phase_Seed;
}
static void phase_clone(PendingHydrationPhase *dst, const PendingHydrationPhase *src) {
    *dst = *src;
    if (src->tag == Phase_Step || src->tag == Phase_StepWitness) groove_RecordDeltas_clone(&dst->frontier, &src->frontier);
    if (src->tag == Phase_StepWitness) groove_RecordDeltas_clone(&dst->step_delta, &src->step_delta);
}

static HydrationTraversal *traversal_new(groove_NodeId root, groove_EvalContext context) {
    HydrationTraversal *t = calloc(1, sizeof *t);
    t->root = root;
    t->context = context;
    Frame f = { Frame_Visit, root };
    JZ_VEC_PUSH(t->discovery, f);
    jz_hmap_init(&t->discovered, &groove_ivm_rt_NODE_SET);
    jz_hmap_init(&t->visiting, &groove_ivm_rt_NODE_SET);
    jz_hmap_init(&t->memo, &NODE_DELTAS_MAP);
    return t;
}
static void traversal_drop(HydrationTraversal *t) {
    if (!t) return;
    groove_EvalContext_drop(&t->context);
    jz_vec_drop(&t->discovery, sizeof(Frame), NULL);
    jz_hmap_drop(&t->discovered);
    jz_hmap_drop(&t->visiting);
    jz_vec_drop(&t->order, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&t->memo);
    free(t);
}
static void node_set_clone(jz_hmap *dst, const jz_hmap *src) {
    jz_hmap_init(dst, &groove_ivm_rt_NODE_SET);
    groove_NodeId *k; void *v;
    JZ_HMAP_FOR(src, k, v) jz_hmap_insert(dst, k, NULL, NULL);
}
static HydrationTraversal *traversal_clone(const HydrationTraversal *s) {
    if (!s) return NULL;
    HydrationTraversal *t = calloc(1, sizeof *t);
    t->root = s->root;
    groove_EvalContext_clone(&t->context, &s->context);
    jz_vec_clone(&t->discovery, &s->discovery, sizeof(Frame), NULL);
    node_set_clone(&t->discovered, &s->discovered);
    node_set_clone(&t->visiting, &s->visiting);
    jz_vec_clone(&t->order, &s->order, sizeof(groove_NodeId), NULL);
    t->next_evaluation = s->next_evaluation;
    jz_hmap_init(&t->memo, &NODE_DELTAS_MAP);
    groove_NodeId *k; groove_RecordDeltas *v;
    JZ_HMAP_FOR(&s->memo, k, v) { groove_RecordDeltas c; groove_RecordDeltas_clone(&c, v); jz_hmap_insert(&t->memo, k, &c, NULL); }
    return t;
}

static void pending_drop(PendingHydrationRecompute *p) {
    if (!p) return;
    jz_hmap_drop(&p->accumulated);
    jz_hmap_drop(&p->step_witness_accumulated);
    phase_drop(&p->phase);
    traversal_drop(p->traversal);
    free(p);
}
static PendingHydrationRecompute *pending_clone(const PendingHydrationRecompute *s) {
    if (!s) return NULL;
    PendingHydrationRecompute *p = calloc(1, sizeof *p);
    p->input_generation = s->input_generation;
    bytes_i64_hmap_clone(&p->accumulated, &s->accumulated);
    bytes_i64_hmap_clone(&p->step_witness_accumulated, &s->step_witness_accumulated);
    phase_clone(&p->phase, &s->phase);
    p->traversal = traversal_clone(s->traversal);
    return p;
}

/* ---------- RecursiveState ---------- */
groove_RecursiveState *RS(default)(void) {
    groove_RecursiveState *s = calloc(1, sizeof *s);
    s->accumulated = rc_bytes_i64_new();
    s->staged_positive = rc_bytes_i64_new();
    s->step_witness_accumulated = rc_bytes_i64_new();
    s->step_witness_tick_deltas = rc_delta_vec_new();
    return s;
}
groove_RecursiveState *RS(clone)(const groove_RecursiveState *src) {
    groove_RecursiveState *s = calloc(1, sizeof *s);
    *s = *src;
    s->accumulated = jz_rc_clone(src->accumulated);
    s->staged_positive = jz_rc_clone(src->staged_positive);
    s->step_witness_accumulated = jz_rc_clone(src->step_witness_accumulated);
    s->step_witness_tick_deltas = jz_rc_clone(src->step_witness_tick_deltas);
    s->pending_hydration = pending_clone(src->pending_hydration);
    return s;
}
void RS(drop)(groove_RecursiveState *s) {
    if (!s) return;
    jz_rc_release(s->accumulated);
    jz_rc_release(s->staged_positive);
    jz_rc_release(s->step_witness_accumulated);
    jz_rc_release(s->step_witness_tick_deltas);
    pending_drop(s->pending_hydration);
    free(s);
}
bool RS(is_empty)(const groove_RecursiveState *s) { return s->accumulated->len == 0; }
bool RS(step_arrangements_hydrated)(const groove_RecursiveState *s) { return s->step_arrangements_hydrated; }
usize RS(accumulated_row_count)(const groove_RecursiveState *s) {
    usize n = 0; jz_bytes *k; i64 *w;
    JZ_HMAP_FOR(s->accumulated, k, w) if (*w != 0) n++;
    return n;
}
usize RS(accumulated_encoded_bytes)(const groove_RecursiveState *s) {
    usize n = 0; jz_bytes *k; i64 *w;
    JZ_HMAP_FOR(s->accumulated, k, w) n += k->len;
    return n;
}
void RS(mark_step_arrangements_hydrated)(groove_RecursiveState *s) { s->step_arrangements_hydrated = true; }
bool RS(hydrated_input_generation)(const groove_RecursiveState *s, u64 *out) {
    if (s->has_hydrated_input_generation) *out = s->hydrated_input_generation;
    return s->has_hydrated_input_generation;
}
void RS(mark_hydrated_input_generation)(groove_RecursiveState *s, u64 g) { s->has_hydrated_input_generation = true; s->hydrated_input_generation = g; }
bool RS(has_pending_hydration)(const groove_RecursiveState *s) { return s->pending_hydration != NULL; }
bool RS(has_pending_hydration_for)(const groove_RecursiveState *s, u64 g) {
    return s->pending_hydration && s->pending_hydration->input_generation == g;
}

static void begin_hydration_recompute(groove_RecursiveState *s, u64 input_generation) {
    pending_drop(s->pending_hydration);
    PendingHydrationRecompute *p = calloc(1, sizeof *p);
    p->input_generation = input_generation;
    jz_hmap_init(&p->accumulated, &groove_ivm_rt_BYTES_I64_HMAP);
    jz_hmap_init(&p->step_witness_accumulated, &groove_ivm_rt_BYTES_I64_HMAP);
    p->phase.tag = Phase_Seed;
    p->traversal = NULL;
    s->pending_hydration = p;
}
static PendingHydrationRecompute *pending_hydration_mut(groove_RecursiveState *s) {
    if (!s->pending_hydration) jz_panic("hydration recompute was initialized");
    return s->pending_hydration;
}

/* positive weights as RecordDelta (filter_map(weight > 0)) */
static void positive_deltas(const jz_hmap *m, groove_RecordDeltaVec *out) {
    jz_bytes *k; i64 *w;
    JZ_HMAP_FOR(m, k, w) if (*w > 0) { groove_RecordDelta d = { jz_bytes_clone(k), *w }; JZ_VEC_PUSH(*out, d); }
}

void RS(pending_hydration_accumulated_deltas)(const groove_RecursiveState *s, groove_RecordDescriptor d, groove_RecordDeltas *out) {
    if (!s->pending_hydration) jz_panic("hydration recompute was initialized");
    *out = empty_deltas(d);
    positive_deltas(&s->pending_hydration->accumulated, &out->deltas);
}

void RS(finish_hydration_recompute)(groove_RecursiveState *s, jz_hmap *next, jz_hmap *step_witness) {
    PendingHydrationRecompute *p = s->pending_hydration;
    if (!p) jz_panic("hydration recompute was initialized");
    s->pending_hydration = NULL;
    *next = p->accumulated;
    *step_witness = p->step_witness_accumulated;
    phase_drop(&p->phase);
    traversal_drop(p->traversal);
    free(p);
}

void RS(accumulated_deltas)(const groove_RecursiveState *s, groove_RecordDeltaVec *out) {
    memset(out, 0, sizeof *out);
    positive_deltas(s->accumulated, out);
}
void RS(step_witness_deltas)(const groove_RecursiveState *s, groove_RecordDeltaVec *out) {
    jz_vec_clone(out, s->step_witness_tick_deltas, sizeof(groove_RecordDelta), groove_RecordDelta_clone);
}
void RS(step_witness_accumulated_deltas)(const groove_RecursiveState *s, groove_RecordDeltaVec *out) {
    memset(out, 0, sizeof *out);
    jz_bytes *k; i64 *w;
    JZ_HMAP_FOR(s->step_witness_accumulated, k, w) if (*w != 0) { groove_RecordDelta d = { jz_bytes_clone(k), *w }; JZ_VEC_PUSH(*out, d); }
}

static void append_step_witness(groove_RecursiveState *s, groove_RecordDeltaVec deltas) {
    CONSOLIDATE(&deltas);
    if (deltas.len == 0) { record_delta_vec_drop(&deltas); return; }
    jz_hmap *accumulated = jz_rc_make_mut((void **)&s->step_witness_accumulated, sizeof(jz_hmap), bytes_i64_hmap_clone_v);
    groove_RecordDeltaVec *tick = jz_rc_make_mut((void **)&s->step_witness_tick_deltas, sizeof(groove_RecordDeltaVec), delta_vec_clone_v);
    JZ_VEC_FOR(groove_RecordDelta, delta, deltas) {
        bool inserted;
        i64 *w = jz_hmap_get(accumulated, &delta->record);
        if (!w) { jz_bytes k = jz_bytes_clone(&delta->record); w = jz_hmap_entry(accumulated, &k, &inserted); if (!inserted) jz_bytes_drop(&k); *w = 0; }
        *w += delta->weight;
        if (*w == 0) jz_hmap_remove(accumulated, &delta->record, NULL, NULL);
        jz_vec_push(tick, delta, sizeof(groove_RecordDelta));
    }
    jz_vec_drop(&deltas, sizeof(groove_RecordDelta), NULL); /* moved into tick */
    CONSOLIDATE(tick);
}

void RS(replace_step_witness_with)(groove_RecursiveState *s, jz_hmap next) {
    groove_RecordDeltaVec deltas = {0};
    jz_bytes *record; i64 *old_weight;
    JZ_HMAP_FOR(s->step_witness_accumulated, record, old_weight) {
        i64 *nw = jz_hmap_get(&next, record);
        i64 next_weight = nw ? *nw : 0;
        if (next_weight != *old_weight) { groove_RecordDelta d = { jz_bytes_clone(record), next_weight - *old_weight }; JZ_VEC_PUSH(deltas, d); }
    }
    i64 *next_weight;
    JZ_HMAP_FOR(&next, record, next_weight) {
        if (!jz_hmap_get(s->step_witness_accumulated, record) && *next_weight != 0) {
            groove_RecordDelta d = { jz_bytes_clone(record), *next_weight }; JZ_VEC_PUSH(deltas, d);
        }
    }
    jz_rc_release(s->step_witness_accumulated);
    jz_hmap *m = jz_rc_new(sizeof(jz_hmap), groove_ivm_rt_hmap_drop_v);
    *m = next;
    s->step_witness_accumulated = m;
    jz_rc_release(s->step_witness_tick_deltas);
    groove_RecordDeltaVec *t = rc_delta_vec_new();
    CONSOLIDATE(&deltas);
    *t = deltas;
    s->step_witness_tick_deltas = t;
}

static void begin_step_witness_tick(groove_RecursiveState *s) {
    jz_rc_release(s->step_witness_tick_deltas);
    s->step_witness_tick_deltas = rc_delta_vec_new();
}

static jz_err reject_non_positive_frontier_deltas(const groove_RecordDeltaVec *deltas) {
    JZ_VEC_FOR(groove_RecordDelta, d, *deltas) if (d->weight <= 0) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedNonMonotoneRecursion);
    return JZ_OK;
}

static jz_err accept_positive_staged(groove_RecursiveState *s, groove_RecordDeltaVec deltas, groove_RecordDeltaVec *out) {
    jz_err e = reject_non_positive_frontier_deltas(&deltas);
    if (JZ_IS_ERR(e)) { record_delta_vec_drop(&deltas); return e; }
    groove_RecordDeltaVec accepted = {0};
    jz_hmap *staged = jz_rc_make_mut((void **)&s->staged_positive, sizeof(jz_hmap), bytes_i64_hmap_clone_v);
    CONSOLIDATE(&deltas);
    for (u32 i = 0; i < deltas.len; i++) {
        groove_RecordDelta *delta = &deltas.ptr[i];
        if (delta->weight <= 0) {
            record_delta_vec_drop(&deltas); record_delta_vec_drop(&accepted);
            return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedNonMonotoneRecursion);
        }
        if (jz_hmap_get(s->accumulated, &delta->record) || jz_hmap_get(staged, &delta->record)) continue;
        jz_bytes k = jz_bytes_clone(&delta->record); i64 one = 1;
        jz_hmap_insert(staged, &k, &one, NULL);
        groove_RecordDelta a = { delta->record, 1 };
        delta->record = (jz_bytes){0};
        JZ_VEC_PUSH(accepted, a);
    }
    record_delta_vec_drop(&deltas);
    CONSOLIDATE(&accepted);
    *out = accepted;
    return JZ_OK;
}

void RS(commit_staged_positive)(groove_RecursiveState *s) {
    if (s->staged_positive->len == 0) return;
    jz_hmap *staged = s->staged_positive;
    s->staged_positive = rc_bytes_i64_new();
    jz_hmap owned;
    /* Rc::try_unwrap(..).unwrap_or_else(clone) */
    if (jz_rc_strong(staged) == 1) { owned = *staged; jz_hmap_init(staged, &groove_ivm_rt_BYTES_I64_HMAP); }
    else bytes_i64_hmap_clone(&owned, staged);
    jz_rc_release(staged);
    jz_hmap *acc = jz_rc_make_mut((void **)&s->accumulated, sizeof(jz_hmap), bytes_i64_hmap_clone_v);
    jz_bytes *k; i64 *v;
    JZ_HMAP_FOR(&owned, k, v) {
        jz_bytes kc = jz_bytes_clone(k);
        if (jz_hmap_get(acc, &kc)) { i64 *w = jz_hmap_get(acc, &kc); *w = *v; jz_bytes_drop(&kc); }
        else jz_hmap_insert(acc, &kc, v, NULL);
    }
    jz_hmap_drop(&owned);
}

void RS(replace_with)(groove_RecursiveState *s, jz_hmap next, groove_RecordDeltaVec *out) {
    groove_RecordDeltaVec deltas = {0};
    jz_bytes *record; i64 *old_weight;
    JZ_HMAP_FOR(s->accumulated, record, old_weight) {
        i64 *nw = jz_hmap_get(&next, record);
        i64 delta = (nw ? *nw : 0) - *old_weight;
        if (delta != 0) { groove_RecordDelta d = { jz_bytes_clone(record), delta }; JZ_VEC_PUSH(deltas, d); }
    }
    i64 *next_weight;
    JZ_HMAP_FOR(&next, record, next_weight) {
        if (jz_hmap_get(s->accumulated, record) || *next_weight == 0) continue;
        groove_RecordDelta d = { jz_bytes_clone(record), *next_weight }; JZ_VEC_PUSH(deltas, d);
    }
    jz_rc_release(s->accumulated);
    jz_hmap *m = jz_rc_new(sizeof(jz_hmap), groove_ivm_rt_hmap_drop_v);
    *m = next;
    s->accumulated = m;
    s->step_arrangements_hydrated = false;
    pending_drop(s->pending_hydration);
    s->pending_hydration = NULL;
    jz_rc_release(s->staged_positive);
    s->staged_positive = rc_bytes_i64_new();
    CONSOLIDATE(&deltas);
    if (out) *out = deltas; else record_delta_vec_drop(&deltas);
}

static jz_err accept_positive_into_set(jz_hmap *multiset, groove_RecordDeltaVec deltas, groove_RecordDeltaVec *out) {
    /* Recompute must match the incremental regime: recursive SELECTs are
     * monotone set fixed points, not UNION ALL bag fixed points. */
    jz_err e = reject_non_positive_frontier_deltas(&deltas);
    if (JZ_IS_ERR(e)) { record_delta_vec_drop(&deltas); return e; }
    groove_RecordDeltaVec accepted = {0};
    CONSOLIDATE(&deltas);
    for (u32 i = 0; i < deltas.len; i++) {
        groove_RecordDelta *delta = &deltas.ptr[i];
        if (delta->weight <= 0) {
            record_delta_vec_drop(&deltas); record_delta_vec_drop(&accepted);
            return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedNonMonotoneRecursion);
        }
        if (jz_hmap_get(multiset, &delta->record)) continue;
        jz_bytes k = jz_bytes_clone(&delta->record); i64 one = 1;
        jz_hmap_insert(multiset, &k, &one, NULL);
        groove_RecordDelta a = { delta->record, 1 };
        delta->record = (jz_bytes){0};
        JZ_VEC_PUSH(accepted, a);
    }
    record_delta_vec_drop(&deltas);
    CONSOLIDATE(&accepted);
    *out = accepted;
    return JZ_OK;
}

/* ---------- graph walks ---------- */
static const groove_GraphNode *graph_node_of(const groove_IvmGraph *g, groove_NodeId node, jz_err *e) {
    const groove_GraphNode *n = groove__ivm__graph__IvmGraph__node(g, node);
    if (!n) *e = GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, node);
    return n;
}

/* Walk an IVM graph's inputs without consuming the stack; each node once. */
typedef jz_err (*visit_fn)(groove_NodeId node, const groove_GraphNode *gn, void *ctx);
static jz_err walk_input_graph(const groove_IvmGraph *graph, groove_NodeId root, visit_fn visit, void *ctx) {
    groove_NodeIdVec pending = {0};
    JZ_VEC_PUSH(pending, root);
    jz_hmap seen; jz_hmap_init(&seen, &groove_ivm_rt_NODE_SET);
    jz_err e = JZ_OK;
    while (pending.len > 0) {
        groove_NodeId node = pending.ptr[--pending.len];
        bool inserted; jz_hmap_entry(&seen, &node, &inserted);
        if (!inserted) continue;
        const groove_GraphNode *gn = graph_node_of(graph, node, &e);
        if (!gn) break;
        e = visit(node, gn, ctx);
        if (JZ_IS_ERR(e)) break;
        /* Reverse push retains the recursive walk's left-to-right input order. */
        for (u32 i = gn->descriptor.inputs.len; i > 0; i--) JZ_VEC_PUSH(pending, gn->descriptor.inputs.ptr[i - 1]);
    }
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&seen);
    return e;
}

static void str_desc_entry(jz_hmap *tables, const jz_str *table, groove_RecordDescriptor d) {
    if (jz_hmap_get(tables, table)) return;
    jz_str k = jz_str_clone(table);
    jz_hmap_insert(tables, &k, &d, NULL);
}
static jz_err visit_table_source_names(groove_NodeId node, const groove_GraphNode *gn, void *ctx) {
    (void)node;
    const groove_OpType *op = &gn->descriptor.operator_;
    if (op->tag == groove_Op_TableSource) str_desc_entry(ctx, &op->v.table_source.table, gn->descriptor.output.records);
    else if (op->tag == groove_Op_IndexSource) str_desc_entry(ctx, &op->v.index_source.table, gn->descriptor.output.records);
    return JZ_OK;
}
static jz_err collect_table_source_names(const groove_IvmGraph *g, groove_NodeId node, jz_hmap *tables) {
    return walk_input_graph(g, node, visit_table_source_names, tables);
}

static u64 binding_key_hash_v(const void *k) { return groove_BindingSourceKey_hash(k); }
static bool binding_key_eq_v(const void *a, const void *b) { return groove_BindingSourceKey_eq(a, b); }
static void binding_key_drop_v(void *k) { groove_BindingSourceKey_drop(k); }
static const jz_hmap_desc BINDING_DESC_MAP = { sizeof(groove_BindingSourceKey), sizeof(groove_RecordDescriptor), binding_key_hash_v, binding_key_eq_v, binding_key_drop_v, NULL };

static jz_err visit_binding_sources(groove_NodeId node, const groove_GraphNode *gn, void *ctx) {
    (void)node;
    const groove_OpType *op = &gn->descriptor.operator_;
    if (op->tag == groove_Op_BindingSource && !jz_hmap_get(ctx, &op->v.binding_source.key)) {
        groove_BindingSourceKey k; groove_BindingSourceKey_clone(&k, &op->v.binding_source.key);
        groove_RecordDescriptor d = gn->descriptor.output.records;
        jz_hmap_insert(ctx, &k, &d, NULL);
    }
    return JZ_OK;
}
static jz_err collect_binding_sources(const groove_IvmGraph *g, groove_NodeId node, jz_hmap *shapes) {
    return walk_input_graph(g, node, visit_binding_sources, shapes);
}

typedef struct AntiJoinCtx { const groove_IvmGraph *graph; jz_hmap *tables; } AntiJoinCtx;
static jz_err visit_anti_join(groove_NodeId node, const groove_GraphNode *gn, void *ctx) {
    AntiJoinCtx *c = ctx;
    if (gn->descriptor.operator_.tag == groove_Op_AntiJoin) {
        if (gn->descriptor.inputs.len < 2) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphInputMissing, node);
        return collect_table_source_names(c->graph, gn->descriptor.inputs.ptr[1], c->tables);
    }
    return JZ_OK;
}
static jz_err collect_anti_join_right_table_sources(const groove_IvmGraph *g, groove_NodeId node, jz_hmap *tables) {
    AntiJoinCtx c = { g, tables };
    return walk_input_graph(g, node, visit_anti_join, &c);
}

/* TableSnapshotSource { table, scan }: HashSet becomes a deduplicated Vec. */
typedef struct TableSnapshotSource { jz_str table; bool has_scan; groove_StaticScanSpec scan; } TableSnapshotSource;
typedef JZ_VEC(TableSnapshotSource) TableSnapshotSourceVec;
static bool literal_vec_eq(const groove_LiteralVec *a, const groove_LiteralVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove_LiteralValue_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static bool scan_spec_eq(const groove_StaticScanSpec *a, const groove_StaticScanSpec *b) {
    return a->tag == b->tag && a->max_items == b->max_items && literal_vec_eq(&a->a, &b->a) && literal_vec_eq(&a->b, &b->b);
}
static bool snapshot_source_eq(const TableSnapshotSource *a, const TableSnapshotSource *b) {
    return jz_str_eq(&a->table, &b->table) && a->has_scan == b->has_scan && (!a->has_scan || scan_spec_eq(&a->scan, &b->scan));
}
static void literal_vec_drop(groove_LiteralVec *v) { jz_vec_drop(v, sizeof(groove_LiteralValue), (void (*)(void *))groove_LiteralValue_drop); }
static void snapshot_source_drop(void *p) {
    TableSnapshotSource *s = p;
    jz_str_drop(&s->table);
    if (s->has_scan) { literal_vec_drop(&s->scan.a); literal_vec_drop(&s->scan.b); }
}
static jz_err visit_table_sources(groove_NodeId node, const groove_GraphNode *gn, void *ctx) {
    (void)node;
    TableSnapshotSourceVec *tables = ctx;
    if (gn->descriptor.operator_.tag != groove_Op_TableSource) return JZ_OK;
    const groove_TableSourceOp *t = &gn->descriptor.operator_.v.table_source;
    TableSnapshotSource probe = { t->table, t->has_scan, t->scan };
    JZ_VEC_FOR(TableSnapshotSource, s, *tables) if (snapshot_source_eq(s, &probe)) return JZ_OK;
    TableSnapshotSource src = { jz_str_clone(&t->table), t->has_scan, {0} };
    if (t->has_scan) groove_StaticScanSpec_clone(&src.scan, &t->scan);
    JZ_VEC_PUSH(*tables, src);
    return JZ_OK;
}
static jz_err collect_table_sources(const groove_IvmGraph *g, groove_NodeId node, TableSnapshotSourceVec *tables) {
    return walk_input_graph(g, node, visit_table_sources, tables);
}
static void snapshot_sources_drop(TableSnapshotSourceVec *v) { jz_vec_drop(v, sizeof(TableSnapshotSource), snapshot_source_drop); }

static groove_TableSourceOp snapshot_table_source_op(const TableSnapshotSource *s) {
    groove_TableSourceOp op; memset(&op, 0, sizeof op);
    op.table = s->table; op.has_scan = s->has_scan; op.scan = s->scan; op.has_vp = false; /* variant_projection: None */
    return op;
}

jz_err REC(recursive_read_tables)(const groove_IvmGraph *g, groove_NodeId seed, groove_NodeId step, const groove_NodeId *step_witness, groove_StrVec *out) {
    jz_hmap tables; jz_hmap_init(&tables, &groove_ivm_rt_STR_DESC_MAP);
    jz_err e = collect_table_source_names(g, seed, &tables);
    if (!JZ_IS_ERR(e)) e = collect_table_source_names(g, step, &tables);
    if (!JZ_IS_ERR(e) && step_witness) e = collect_table_source_names(g, *step_witness, &tables);
    if (JZ_IS_ERR(e)) { jz_hmap_drop(&tables); return e; }
    groove_StrVec names = {0};
    jz_str *k; groove_RecordDescriptor *v;
    JZ_HMAP_FOR(&tables, k, v) { jz_str c = jz_str_clone(k); JZ_VEC_PUSH(names, c); }
    jz_hmap_drop(&tables);
    jz_sort(names.ptr, names.len, sizeof(jz_str), jz_cmp_str, NULL);
    *out = names;
    return JZ_OK;
}

static jz_err contains_frontier(const groove_IvmGraph *graph, groove_NodeId root, const groove_FrontierName *binding, bool *out) {
    groove_NodeIdVec pending = {0}; JZ_VEC_PUSH(pending, root);
    jz_hmap seen; jz_hmap_init(&seen, &groove_ivm_rt_NODE_SET);
    jz_err e = JZ_OK; *out = false;
    while (pending.len > 0) {
        groove_NodeId node = pending.ptr[--pending.len];
        bool inserted; jz_hmap_entry(&seen, &node, &inserted);
        if (!inserted) continue;
        const groove_GraphNode *gn = graph_node_of(graph, node, &e);
        if (!gn) break;
        if (gn->descriptor.operator_.tag == groove_Op_FrontierSource &&
            jz_str_eq(&gn->descriptor.operator_.v.frontier_source.binding.name, &binding->name)) { *out = true; break; }
        jz_vec_extend(&pending, gn->descriptor.inputs.ptr, gn->descriptor.inputs.len, sizeof(groove_NodeId));
    }
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&seen);
    return e;
}

static jz_err contains_changed_table(const groove_IvmGraph *graph, groove_NodeId root, const groove_TableDelta *td, u32 ntd, bool *out) {
    jz_hmap changed; jz_hmap_init(&changed, &groove_ivm_rt_STR_DESC_MAP);
    for (u32 i = 0; i < ntd; i++) if (td[i].deltas.len > 0) str_desc_entry(&changed, &td[i].table, td[i].descriptor);
    groove_NodeIdVec pending = {0}; JZ_VEC_PUSH(pending, root);
    jz_hmap seen; jz_hmap_init(&seen, &groove_ivm_rt_NODE_SET);
    jz_err e = JZ_OK; *out = false;
    while (pending.len > 0) {
        groove_NodeId node = pending.ptr[--pending.len];
        bool inserted; jz_hmap_entry(&seen, &node, &inserted);
        if (!inserted) continue;
        const groove_GraphNode *gn = graph_node_of(graph, node, &e);
        if (!gn) break;
        if (gn->descriptor.operator_.tag == groove_Op_TableSource && jz_hmap_get(&changed, &gn->descriptor.operator_.v.table_source.table)) { *out = true; break; }
        jz_vec_extend(&pending, gn->descriptor.inputs.ptr, gn->descriptor.inputs.len, sizeof(groove_NodeId));
    }
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&seen);
    jz_hmap_drop(&changed);
    return e;
}

static jz_err collect_changed_join_keys(const groove_IvmGraph *graph, groove_NodeId root, const groove_TableDelta *td, u32 ntd,
                                        const groove_StrVec *fields, groove_ValueComparison comparison, jz_hmap *keys) {
    groove_NodeIdVec pending = {0}; JZ_VEC_PUSH(pending, root);
    jz_hmap seen; jz_hmap_init(&seen, &groove_ivm_rt_NODE_SET);
    jz_err e = JZ_OK;
    while (pending.len > 0) {
        groove_NodeId node = pending.ptr[--pending.len];
        bool inserted; jz_hmap_entry(&seen, &node, &inserted);
        if (!inserted) continue;
        const groove_GraphNode *gn = graph_node_of(graph, node, &e);
        if (!gn) break;
        if (gn->descriptor.operator_.tag == groove_Op_TableSource) {
            const jz_str *table = &gn->descriptor.operator_.v.table_source.table;
            for (u32 i = 0; i < ntd && !JZ_IS_ERR(e); i++) {
                if (!jz_str_eq(&td[i].table, table) || td[i].deltas.len == 0) continue;
                groove_BytesVec touched = {0};
                e = groove__ivm__runtime__join__touched_join_keys(td[i].descriptor, fields->ptr, fields->len, td[i].deltas.ptr, td[i].deltas.len, comparison, &touched);
                if (JZ_IS_ERR(e)) break;
                JZ_VEC_FOR(jz_bytes, k, touched) { if (!jz_hmap_insert(keys, k, NULL, NULL)) {} else jz_bytes_drop(k); }
                jz_vec_drop(&touched, sizeof(jz_bytes), NULL);
            }
            if (JZ_IS_ERR(e)) break;
        }
        jz_vec_extend(&pending, gn->descriptor.inputs.ptr, gn->descriptor.inputs.len, sizeof(groove_NodeId));
    }
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&seen);
    return e;
}

static void str_vec_drop(groove_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), groove_ivm_rt_str_drop_v); }

static jz_err affected_recursive_frontier(const groove_GraphRuntimeView *runtime, const groove_RecursiveOp *recursive,
                                          const groove_RecordDeltaVec *accumulated, groove_NodeId step, groove_RecordDeltaVec *out) {
    jz_hmap selected; jz_hmap_init(&selected, &BYTES_HSET);
    groove_NodeIdVec pending = {0}; JZ_VEC_PUSH(pending, step);
    jz_hmap seen; jz_hmap_init(&seen, &groove_ivm_rt_NODE_SET);
    jz_err e = JZ_OK;
    while (pending.len > 0) {
        groove_NodeId node = pending.ptr[--pending.len];
        bool inserted; jz_hmap_entry(&seen, &node, &inserted);
        if (!inserted) continue;
        const groove_GraphNode *gn = graph_node_of(runtime->graph, node, &e);
        if (!gn) break;
        if (gn->descriptor.operator_.tag != groove_Op_Join) {
            jz_vec_extend(&pending, gn->descriptor.inputs.ptr, gn->descriptor.inputs.len, sizeof(groove_NodeId));
            continue;
        }
        const groove_JoinOp *join = &gn->descriptor.operator_.v.join;
        if (gn->descriptor.inputs.len != 2) { e = GROOVE_IVM_ERR_N(GROOVE_IVM_GraphInputArityMismatch, node); break; }
        groove_NodeId left = gn->descriptor.inputs.ptr[0], right = gn->descriptor.inputs.ptr[1];
        bool left_frontier, right_frontier;
        e = contains_frontier(runtime->graph, left, &recursive->frontier, &left_frontier); if (JZ_IS_ERR(e)) break;
        e = contains_frontier(runtime->graph, right, &recursive->frontier, &right_frontier); if (JZ_IS_ERR(e)) break;
        if (left_frontier != right_frontier) {
            groove_NodeId changed_input; groove_StrVec changed_fields = {0}, frontier_fields = {0};
            groove_RecordDescriptor frontier_desc;
            bool right_changed = false, left_changed = false;
            if (left_frontier) { e = contains_changed_table(runtime->graph, right, runtime->table_deltas, runtime->n_table_deltas, &right_changed); if (JZ_IS_ERR(e)) break; }
            if (left_frontier && right_changed) {
                changed_input = right;
                groove__ivm__runtime__evaluator__plan_expr_names(join->right_key.ptr, join->right_key.len, &changed_fields);
                groove__ivm__runtime__evaluator__plan_expr_names(join->left_key.ptr, join->left_key.len, &frontier_fields);
                frontier_desc = join->left_descriptor;
            } else {
                if (right_frontier) { e = contains_changed_table(runtime->graph, left, runtime->table_deltas, runtime->n_table_deltas, &left_changed); if (JZ_IS_ERR(e)) break; }
                if (right_frontier && left_changed) {
                    changed_input = left;
                    groove__ivm__runtime__evaluator__plan_expr_names(join->left_key.ptr, join->left_key.len, &changed_fields);
                    groove__ivm__runtime__evaluator__plan_expr_names(join->right_key.ptr, join->right_key.len, &frontier_fields);
                    frontier_desc = join->right_descriptor;
                } else {
                    JZ_VEC_PUSH(pending, left); JZ_VEC_PUSH(pending, right);
                    continue;
                }
            }
            jz_hmap touched; jz_hmap_init(&touched, &BYTES_HSET);
            /* Raw table keys are a valid selective frontier only when they
             * still have the join descriptor's fields. */
            e = collect_changed_join_keys(runtime->graph, changed_input, runtime->table_deltas, runtime->n_table_deltas, &changed_fields, join->comparison, &touched);
            str_vec_drop(&changed_fields);
            if (JZ_IS_ERR(e)) {
                jz_hmap_drop(&touched); str_vec_drop(&frontier_fields);
                if (e.code == GROOVE_IVM_GraphFieldNotFound) {
                    jz_err_drop(&e); e = JZ_OK;
                    JZ_VEC_PUSH(pending, left); JZ_VEC_PUSH(pending, right);
                    continue;
                }
                break;
            }
            JZ_VEC_FOR(groove_RecordDelta, delta, *accumulated) {
                groove_BytesVec keys = {0};
                e = groove__ivm__runtime__join__join_keys(frontier_desc, jz_bytes_slice(&delta->record), frontier_fields.ptr, frontier_fields.len, join->comparison, &keys);
                if (JZ_IS_ERR(e)) break;
                JZ_VEC_FOR(jz_bytes, key, keys) {
                    if (jz_hmap_get(&touched, key)) {
                        if (!jz_hmap_get(&selected, &delta->record)) { jz_bytes c = jz_bytes_clone(&delta->record); jz_hmap_insert(&selected, &c, NULL, NULL); }
                        break;
                    }
                }
                jz_vec_drop(&keys, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
            }
            jz_hmap_drop(&touched); str_vec_drop(&frontier_fields);
            if (JZ_IS_ERR(e)) break;
        }
        JZ_VEC_PUSH(pending, left); JZ_VEC_PUSH(pending, right);
    }
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&seen);
    if (JZ_IS_ERR(e)) { jz_hmap_drop(&selected); return e; }
    groove_RecordDeltaVec r = {0};
    jz_bytes *k; void *v;
    JZ_HMAP_FOR(&selected, k, v) { groove_RecordDelta d = { jz_bytes_clone(k), 1 }; JZ_VEC_PUSH(r, d); }
    jz_hmap_drop(&selected);
    *out = r;
    return JZ_OK;
}

static bool has_table_delta_for_cached_tables(const groove_GraphRuntimeView *runtime, const groove_RecursiveOp *recursive) {
    for (u32 i = 0; i < runtime->n_table_deltas; i++)
        JZ_VEC_FOR(jz_str, t, recursive->read_tables) if (jz_str_eq(t, &runtime->table_deltas[i].table)) return true;
    return false;
}

static jz_err has_recompute_table_delta(const groove_IvmGraph *graph, const groove_TableDelta *td, u32 ntd, groove_RecursiveNodes nodes, bool *out) {
    jz_hmap tables; jz_hmap_init(&tables, &groove_ivm_rt_STR_DESC_MAP);
    jz_hmap anti; jz_hmap_init(&anti, &groove_ivm_rt_STR_DESC_MAP);
    jz_err e = collect_table_source_names(graph, nodes.seed, &tables);
    if (!JZ_IS_ERR(e)) e = collect_table_source_names(graph, nodes.step, &tables);
    if (!JZ_IS_ERR(e) && nodes.has_step_witness) e = collect_table_source_names(graph, nodes.step_witness, &tables);
    if (!JZ_IS_ERR(e)) e = collect_anti_join_right_table_sources(graph, nodes.seed, &anti);
    if (!JZ_IS_ERR(e)) e = collect_anti_join_right_table_sources(graph, nodes.step, &anti);
    if (!JZ_IS_ERR(e) && nodes.has_step_witness) e = collect_anti_join_right_table_sources(graph, nodes.step_witness, &anti);
    *out = false;
    if (!JZ_IS_ERR(e)) {
        for (u32 i = 0; i < ntd && !*out; i++) {
            if (!jz_hmap_get(&tables, &td[i].table)) continue;
            if (jz_hmap_get(&anti, &td[i].table)) { *out = true; break; }
            JZ_VEC_FOR(groove_RecordDelta, d, td[i].deltas) if (d->weight <= 0) { *out = true; break; }
        }
    }
    jz_hmap_drop(&tables); jz_hmap_drop(&anti);
    return e;
}

static jz_err has_recompute_binding_delta(const groove_IvmGraph *graph, const groove_BindingDelta *bd, u32 nbd, groove_RecursiveNodes nodes, bool *out) {
    jz_hmap shapes; jz_hmap_init(&shapes, &BINDING_DESC_MAP);
    jz_err e = collect_binding_sources(graph, nodes.seed, &shapes);
    if (!JZ_IS_ERR(e)) e = collect_binding_sources(graph, nodes.step, &shapes);
    if (!JZ_IS_ERR(e) && nodes.has_step_witness) e = collect_binding_sources(graph, nodes.step_witness, &shapes);
    *out = false;
    if (!JZ_IS_ERR(e)) {
        for (u32 i = 0; i < nbd && !*out; i++) {
            if (!jz_hmap_get(&shapes, &bd[i].key)) continue;
            JZ_VEC_FOR(groove_RecordDelta, d, bd[i].deltas) if (d->weight <= 0) { *out = true; break; }
        }
    }
    jz_hmap_drop(&shapes);
    return e;
}

jz_err REC(snapshot_requirement)(const groove_IvmGraph *g, groove_NodeId node, groove_RecursiveNodes nodes, const groove_TableDelta *td, u32 ntd,
                                 const groove_BindingDelta *bd, u32 nbd, const groove_RecursiveState *state, bool *some, groove_NodeId *out) {
    bool has_bindings = nbd > 0;
    bool requires_recompute;
    JZ_TRY(has_recompute_table_delta(g, td, ntd, nodes, &requires_recompute));
    if (!requires_recompute) JZ_TRY(has_recompute_binding_delta(g, bd, nbd, nodes, &requires_recompute));
    if (!requires_recompute) requires_recompute = (!has_bindings && (!state || RS(is_empty)(state))) || !state || !state->step_arrangements_hydrated;
    if (requires_recompute) { *some = true; *out = node; }       /* the owning recursive node's complete input closure */
    else if (has_bindings) { *some = true; *out = nodes.seed; }
    else *some = false;
    return JZ_OK;
}

jz_err REC(require_snapshot_inputs)(const groove_IvmGraph *g, groove_EvaluationInputs *in, groove_NodeId root) {
    TableSnapshotSourceVec tables = {0};
    jz_err e = collect_table_sources(g, root, &tables);
    if (JZ_IS_ERR(e)) { snapshot_sources_drop(&tables); return e; }
    bool blocked = false;
    JZ_VEC_FOR(TableSnapshotSource, source, tables) {
        groove_TableSourceOp op = snapshot_table_source_op(source);
        bool some; groove_StorageRequestKey request;
        e = NS(table_source_request)(&op, &some, &request);
        if (JZ_IS_ERR(e)) break;
        if (some) {
            const groove_KvPairVec *rows;
            e = groove__ivm__runtime__evaluation_session__EvaluationInputs__rows(in, request, &rows);
            if (JZ_IS_ERR(e)) {
                if (e.code == GROOVE_IVM_EvaluationBlocked) { jz_err_drop(&e); e = JZ_OK; blocked = true; }
                else break;
            }
        }
    }
    snapshot_sources_drop(&tables);
    if (JZ_IS_ERR(e)) return e;
    return blocked ? GROOVE_IVM_ERR(GROOVE_IVM_EvaluationBlocked) : JZ_OK;
}

/* HashMap<(u32, RecordDescriptor), Vec<RecordDelta>> grouping shared by the
 * three snapshot readers (each repeats it inline in Rust). */
typedef struct VariantKey { u32 tag; groove_RecordDescriptor descriptor; } VariantKey;
static u64 variant_key_hash(const void *k) { return jz_hash_bytes(k, sizeof(VariantKey)); }
static bool variant_key_eq(const void *a, const void *b) { const VariantKey *x = a, *y = b; return x->tag == y->tag && x->descriptor == y->descriptor; }
static void delta_vec_drop_vv(void *v) { record_delta_vec_drop(v); }
static const jz_hmap_desc VARIANT_DELTAS_MAP = { sizeof(VariantKey), sizeof(groove_RecordDeltaVec), variant_key_hash, variant_key_eq, NULL, delta_vec_drop_vv };

static jz_err group_stored_by_variant(const groove_TableSchema *table_schema, const jz_str *table, const jz_slice *stored, u32 n, groove_TableDeltaVec *output) {
    jz_hmap by_variant; jz_hmap_init(&by_variant, &VARIANT_DELTAS_MAP);
    for (u32 i = 0; i < n; i++) {
        u32 variant_tag; jz_slice payload;
        jz_err e = groove__records__split_variant_record(stored[i], &variant_tag, &payload);
        if (JZ_IS_ERR(e)) { jz_hmap_drop(&by_variant); return e; }
        VariantKey key = { variant_tag, NULL };
        if (!groove__schema__TableSchema__record_schema_for_variant(table_schema, variant_tag, &key.descriptor)) {
            jz_hmap_drop(&by_variant);
            return groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_UnknownTableVariant, table, "", 0, variant_tag);
        }
        bool inserted;
        groove_RecordDeltaVec *v = jz_hmap_entry(&by_variant, &key, &inserted);
        if (inserted) memset(v, 0, sizeof *v);
        groove_RecordDelta d = { jz_bytes_from(payload.ptr, payload.len), 1 };
        JZ_VEC_PUSH(*v, d);
    }
    VariantKey *k; groove_RecordDeltaVec *v;
    JZ_HMAP_FOR(&by_variant, k, v) {
        groove_TableDelta td = { jz_str_clone(table), k->tag, k->descriptor, *v };
        memset(v, 0, sizeof *v);
        JZ_VEC_PUSH(*output, td);
    }
    jz_hmap_drop(&by_variant);
    return JZ_OK;
}

static jz_err scan_record_store(groove_RecordStore store, groove_ScanBounds bounds, groove_KvPairVec *out) {
    groove_ScanRequest req; memset(&req, 0, sizeof req);
    req.bounds = bounds;
    req.direction = groove_ScanDirection_Forward;
    jz_err e = store.vt->scan(store.self, &req, out);
    groove_ScanRequest_drop(&req);
    return e;
}

static void table_delta_drop(void *p) { groove_TableDelta *t = p; jz_str_drop(&t->table); record_delta_vec_drop(&t->deltas); }
static void table_delta_vec_drop(groove_TableDeltaVec *v) { jz_vec_drop(v, sizeof(groove_TableDelta), table_delta_drop); }

jz_err REC(snapshot_table_deltas)(const groove_DatabaseSchema *schema, const groove_IvmGraph *graph, const groove_Storage *storage, groove_NodeId root, groove_TableDeltaVec *out) {
    TableSnapshotSourceVec tables = {0};
    jz_err e = collect_table_sources(graph, root, &tables);
    groove_TableDeltaVec output = {0};
    JZ_VEC_FOR(TableSnapshotSource, source, tables) {
        if (JZ_IS_ERR(e)) break;
        const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(schema, source->table.ptr, source->table.len);
        if (!table_schema) { e = GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &source->table); break; }
        groove_RecordDescriptor storage_descriptor = groove__schema__TableSchema__record_schema(table_schema);
        groove_RecordStore store = groove__ivm__runtime__subscriptions__record_store_for_table(storage, table_schema, &storage_descriptor);
        groove_KvPairVec rows = {0};
        if (!source->has_scan) {
            groove_ScanBounds b = { groove_ScanBounds_Prefix, {0}, {0} };
            e = scan_record_store(store, b, &rows);
        } else {
            groove_StaticScanBounds sb;
            e = groove__ivm__runtime__record_projection__scan_bounds(&source->scan, &sb);
            if (JZ_IS_ERR(e)) break;
            if (sb.tag == groove_StaticScanBounds_Prefix) {
                groove_ScanBounds b = { groove_ScanBounds_Prefix, sb.a, {0} };
                e = scan_record_store(store, b, &rows);
            } else if (jz_bytes_cmp(&sb.a, &sb.b) < 0) {
                groove_ScanBounds b = { groove_ScanBounds_Range, sb.a, sb.b };
                e = scan_record_store(store, b, &rows);
            } else {
                groove__ivm__runtime__record_projection__StaticScanBounds__drop(&sb);
            }
        }
        if (JZ_IS_ERR(e)) { groove_KvPairVec_drop(&rows); break; }
        JZ_VEC(jz_slice) stored = {0};
        JZ_VEC_FOR(groove_KvPair, kv, rows) { jz_slice s = jz_bytes_slice(&kv->value); JZ_VEC_PUSH(stored, s); }
        e = group_stored_by_variant(table_schema, &source->table, stored.ptr, stored.len, &output);
        jz_vec_drop(&stored, sizeof(jz_slice), NULL);
        groove_KvPairVec_drop(&rows);
    }
    snapshot_sources_drop(&tables);
    if (JZ_IS_ERR(e)) { table_delta_vec_drop(&output); return e; }
    *out = output;
    return JZ_OK;
}

static jz_err snapshot_table_deltas_from_inputs(const groove_DatabaseSchema *schema, const groove_IvmGraph *graph, groove_EvaluationInputs *inputs, groove_NodeId root, groove_TableDeltaVec *out) {
    TableSnapshotSourceVec tables = {0};
    jz_err e = collect_table_sources(graph, root, &tables);
    if (!JZ_IS_ERR(e)) e = REC(require_snapshot_inputs)(graph, inputs, root);
    groove_TableDeltaVec output = {0};
    JZ_VEC_FOR(TableSnapshotSource, source, tables) {
        if (JZ_IS_ERR(e)) break;
        groove_TableSourceOp op = snapshot_table_source_op(source);
        bool some; groove_StorageRequestKey request;
        e = NS(table_source_request)(&op, &some, &request);
        if (JZ_IS_ERR(e)) break;
        JZ_VEC(jz_slice) stored = {0};
        if (some) {
            const groove_KvPairVec *rows;
            e = groove__ivm__runtime__evaluation_session__EvaluationInputs__rows(inputs, request, &rows);
            if (JZ_IS_ERR(e)) break;
            JZ_VEC_FOR(groove_KvPair, kv, *rows) { jz_slice s = jz_bytes_slice(&kv->value); JZ_VEC_PUSH(stored, s); }
        }
        const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(schema, source->table.ptr, source->table.len);
        if (!table_schema) { jz_vec_drop(&stored, sizeof(jz_slice), NULL); e = GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &source->table); break; }
        e = group_stored_by_variant(table_schema, &source->table, stored.ptr, stored.len, &output);
        jz_vec_drop(&stored, sizeof(jz_slice), NULL);
    }
    snapshot_sources_drop(&tables);
    if (JZ_IS_ERR(e)) { table_delta_vec_drop(&output); return e; }
    *out = output;
    return JZ_OK;
}

/* Recursive full-snapshot evaluation may traverse ArgBy inside a seed or
 * step; it uses the maintained operator's shared comparator. */
typedef struct Winner { jz_bytes comparison_key; jz_bytes record; } Winner;
static void winner_drop(void *p) { Winner *w = p; jz_bytes_drop(&w->comparison_key); jz_bytes_drop(&w->record); }
static int bytes_cmp_v(const void *a, const void *b, void *ctx) { (void)ctx; return jz_bytes_cmp(a, b); }
static const jz_btree_desc WINNERS_MAP = { sizeof(jz_bytes), sizeof(Winner), bytes_cmp_v, groove_ivm_rt_bytes_drop_v, winner_drop };

jz_err REC(hydrated_arg_by_winners)(const groove_RecordDeltas *input, groove_RecordDescriptor output_desc, const usize *g, u32 ng, const usize *c, u32 nc, u32 direction, groove_RecordDeltas *out) {
    jz_btree winners; jz_btree_init(&winners, &WINNERS_MAP);
    JZ_VEC_FOR(groove_RecordDelta, delta, input->deltas) {
        if (delta->weight <= 0) continue;
        jz_bytes group_key, comparison_key;
        jz_err e = groove__ivm__runtime__windows__encoded_record_key_part(output_desc, delta->record.ptr, delta->record.len, g, ng, &group_key);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&winners); return e; }
        e = groove__ivm__runtime__windows__encoded_record_key_part(output_desc, delta->record.ptr, delta->record.len, c, nc, &comparison_key);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&group_key); jz_btree_drop(&winners); return e; }
        Winner *winner = jz_btree_get(&winners, &group_key);
        if (!winner) {
            Winner w = { comparison_key, jz_bytes_clone(&delta->record) };
            jz_btree_insert(&winners, &group_key, &w, NULL);
        } else {
            jz_bytes_drop(&group_key);
            bool replaces = groove__ivm__runtime__windows__arg_by_candidate_replaces(jz_bytes_slice(&comparison_key), jz_bytes_slice(&delta->record),
                                                                                    jz_bytes_slice(&winner->comparison_key), jz_bytes_slice(&winner->record), direction);
            if (replaces) { winner_drop(winner); winner->comparison_key = comparison_key; winner->record = jz_bytes_clone(&delta->record); }
            else jz_bytes_drop(&comparison_key);
        }
    }
    *out = empty_deltas(output_desc);
    jz_bytes k; Winner w;
    while (jz_btree_pop_first(&winners, &k, &w)) {
        groove_RecordDelta d = { w.record, 1 };
        JZ_VEC_PUSH(out->deltas, d);
        jz_bytes_drop(&k); jz_bytes_drop(&w.comparison_key);
    }
    jz_btree_drop(&winners);
    return JZ_OK;
}

/* ---------- HydrationEvaluator: full-snapshot evaluator ---------- */
typedef struct HydrationEvaluator {
    const groove_DatabaseSchema *schema;
    const groove_IvmGraph *graph;
    const jz_hmap *variant_projections;
    const groove_TableDelta *table_deltas; u32 n_table_deltas; bool has_table_deltas;
    groove_EvaluationInputs *evaluation_inputs;
    const groove_Storage *storage;
    const jz_hmap *binding_snapshots;
    const groove_EvalContext *context;
    jz_hmap *memo; /* NodeId -> RecordDeltas */
} HydrationEvaluator;

static jz_err eval_node(HydrationEvaluator *ev, groove_NodeId node, groove_RecordDeltas *out);

static jz_err eval_unary_input(HydrationEvaluator *ev, const groove_GraphNode *gn, groove_NodeId node, groove_RecordDeltas *out) {
    if (gn->descriptor.inputs.len == 0) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphInputMissing, node);
    return eval_node(ev, gn->descriptor.inputs.ptr[0], out);
}

static jz_err eval_table_source(HydrationEvaluator *ev, const groove_TableSourceOp *table, groove_RecordDescriptor output_desc, groove_RecordDeltas *out) {
    const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(ev->schema, table->table.ptr, table->table.len);
    if (!table_schema) return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &table->table);
    groove_RecordDescriptor storage_descriptor = groove__schema__TableSchema__record_schema(table_schema);
    groove_RecordStore store = groove__ivm__runtime__subscriptions__record_store_for_table(ev->storage, table_schema, &storage_descriptor);
    groove_KvPairVec rows = {0};
    groove_ScanBounds b = { groove_ScanBounds_Prefix, {0}, {0} };
    JZ_TRY(scan_record_store(store, b, &rows));
    JZ_VEC(jz_slice) stored = {0};
    JZ_VEC_FOR(groove_KvPair, kv, rows) { jz_slice s = jz_bytes_slice(&kv->value); JZ_VEC_PUSH(stored, s); }
    groove_TableDeltaVec table_deltas = {0};
    jz_err e = group_stored_by_variant(table_schema, &table->table, stored.ptr, stored.len, &table_deltas);
    jz_vec_drop(&stored, sizeof(jz_slice), NULL);
    groove_KvPairVec_drop(&rows);
    if (!JZ_IS_ERR(e)) e = NS(update_table_source)(table, ev->schema, ev->variant_projections, output_desc, table_deltas.ptr, table_deltas.len, out);
    table_delta_vec_drop(&table_deltas);
    return e;
}

typedef JZ_VEC(const groove_RecordDelta *) DeltaRefVec;
static void delta_ref_vec_drop(void *v) { jz_vec_drop(v, sizeof(const groove_RecordDelta *), NULL); }
static const jz_btree_desc JOIN_KEY_REFS_MAP = { sizeof(jz_bytes), sizeof(DeltaRefVec), bytes_cmp_v, groove_ivm_rt_bytes_drop_v, delta_ref_vec_drop };

static jz_err eval_join_snapshot(const groove_JoinOp *join, groove_RecordDescriptor output_desc, const groove_RecordDeltas *left, const groove_RecordDeltas *right, groove_RecordDeltas *out) {
    groove_StrVec left_on = {0}, right_on = {0};
    groove__ivm__runtime__evaluator__plan_expr_names(join->left_key.ptr, join->left_key.len, &left_on);
    groove__ivm__runtime__evaluator__plan_expr_names(join->right_key.ptr, join->right_key.len, &right_on);
    jz_btree right_by_key; jz_btree_init(&right_by_key, &JOIN_KEY_REFS_MAP);
    jz_err e = JZ_OK;
    groove_RecordDeltaVec deltas = {0};
    JZ_VEC_FOR(groove_RecordDelta, rd, right->deltas) {
        groove_BytesVec keys = {0};
        e = groove__ivm__runtime__join__join_keys(join->right_descriptor, jz_bytes_slice(&rd->record), right_on.ptr, right_on.len, join->comparison, &keys);
        if (JZ_IS_ERR(e)) goto done;
        JZ_VEC_FOR(jz_bytes, key, keys) {
            bool inserted;
            DeltaRefVec *v = jz_btree_entry(&right_by_key, key, &inserted);
            if (!inserted) jz_bytes_drop(key);
            const groove_RecordDelta *p = rd;
            JZ_VEC_PUSH(*v, p);
        }
        jz_vec_drop(&keys, sizeof(jz_bytes), NULL);
    }
    JZ_VEC_FOR(groove_RecordDelta, ld, left->deltas) {
        groove_BytesVec keys = {0};
        e = groove__ivm__runtime__join__join_keys(join->left_descriptor, jz_bytes_slice(&ld->record), left_on.ptr, left_on.len, join->comparison, &keys);
        if (JZ_IS_ERR(e)) goto done;
        JZ_VEC_FOR(jz_bytes, key, keys) {
            DeltaRefVec *matches = jz_btree_get(&right_by_key, key);
            if (!matches) continue;
            for (u32 i = 0; i < matches->len && !JZ_IS_ERR(e); i++) {
                const groove_RecordDelta *rd = matches->ptr[i];
                groove_RecordDelta d;
                e = groove__ivm__runtime__join__create_join_record(join->left_descriptor, jz_bytes_slice(&ld->record), join->right_descriptor, jz_bytes_slice(&rd->record), output_desc, &d.record);
                if (JZ_IS_ERR(e)) break;
                d.weight = ld->weight * rd->weight;
                JZ_VEC_PUSH(deltas, d);
            }
            if (JZ_IS_ERR(e)) break;
        }
        jz_vec_drop(&keys, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
        if (JZ_IS_ERR(e)) goto done;
    }
done:
    jz_btree_drop(&right_by_key);
    str_vec_drop(&left_on); str_vec_drop(&right_on);
    if (JZ_IS_ERR(e)) { record_delta_vec_drop(&deltas); return e; }
    out->descriptor = output_desc; out->deltas = deltas;
    return JZ_OK;
}

static jz_err eval_threshold(HydrationEvaluator *ev, const groove_GraphNode *gn, groove_NodeId node, groove_RecordDescriptor output_desc, bool semi, groove_RecordDeltas *out) {
    const groove_JoinOp *join = &gn->descriptor.operator_.v.join;
    if (gn->descriptor.inputs.len != 2) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphInputArityMismatch, node);
    groove_RecordDeltas left, right;
    JZ_TRY(eval_node(ev, gn->descriptor.inputs.ptr[0], &left));
    jz_err e = eval_node(ev, gn->descriptor.inputs.ptr[1], &right);
    if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(&left); return e; }
    groove_StrVec lk = {0}, rk = {0};
    groove__ivm__runtime__evaluator__plan_expr_names(join->left_key.ptr, join->left_key.len, &lk);
    groove__ivm__runtime__evaluator__plan_expr_names(join->right_key.ptr, join->right_key.len, &rk);
    groove_RecordDeltaVec deltas = {0};
    e = groove__ivm__runtime__join__threshold_snapshot(&left, &right, lk.ptr, lk.len, rk.ptr, rk.len, join->comparison, semi, &deltas);
    str_vec_drop(&lk); str_vec_drop(&rk);
    groove_RecordDeltas_drop(&left); groove_RecordDeltas_drop(&right);
    if (JZ_IS_ERR(e)) return e;
    out->descriptor = output_desc; out->deltas = deltas;
    return JZ_OK;
}

#define UNARY(call) do { groove_RecordDeltas input; JZ_TRY(eval_unary_input(ev, gn, node, &input)); \
    e = (call); groove_RecordDeltas_drop(&input); } while (0)

static jz_err eval_node_uncached(HydrationEvaluator *ev, groove_NodeId node, groove_RecordDeltas *out) {
    jz_err e = JZ_OK;
    const groove_GraphNode *gn = graph_node_of(ev->graph, node, &e);
    if (!gn) return e;
    groove_RecordDescriptor output_desc = gn->descriptor.output.records;
    const groove_OpType *op = &gn->descriptor.operator_;
    switch (op->tag) {
    case groove_Op_TableSource:
        if (ev->evaluation_inputs)
            return NS(update_table_source_from_inputs)(&op->v.table_source, ev->schema, ev->variant_projections, output_desc, ev->evaluation_inputs, out);
        if (ev->has_table_deltas)
            return NS(update_table_source)(&op->v.table_source, ev->schema, ev->variant_projections, output_desc, ev->table_deltas, ev->n_table_deltas, out);
        return eval_table_source(ev, &op->v.table_source, output_desc, out);
    case groove_Op_IndexSource:
        if (ev->evaluation_inputs)
            return NS(update_index_source_from_inputs)(&op->v.index_source, ev->schema, ev->variant_projections, output_desc, ev->evaluation_inputs, out);
        return NS(update_index_source)(&op->v.index_source, ev->schema, ev->variant_projections, output_desc, NULL, 0, ev->storage, groove_EvalMode_Hydrate, out);
    case groove_Op_InlineRecords:
        *out = empty_deltas(output_desc);
        JZ_VEC_FOR(jz_bytes, r, op->v.inline_records.records) { groove_RecordDelta d = { jz_bytes_clone(r), 1 }; JZ_VEC_PUSH(out->deltas, d); }
        return JZ_OK;
    case groove_Op_FrontierSource: {
        const groove_RecordDeltas *b = jz_hmap_get(&ev->context->bindings, &op->v.frontier_source.binding);
        groove_RecordDeltas deltas;
        if (b) groove_RecordDeltas_clone(&deltas, b); else deltas = empty_deltas(output_desc);
        if (deltas.descriptor != output_desc) { groove_RecordDeltas_drop(&deltas); return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch); }
        *out = deltas;
        return JZ_OK;
    }
    case groove_Op_BindingSource: {
        const groove_RecordDeltas *d = ev->binding_snapshots ? jz_hmap_get(ev->binding_snapshots, &op->v.binding_source.key) : NULL;
        if (d) return groove__ivm__runtime__key_encoding__project_binding_source_deltas(d, output_desc, out);
        groove_RecordDeltas empty = empty_deltas(output_desc);
        return groove__ivm__runtime__key_encoding__project_binding_source_deltas(&empty, output_desc, out);
    }
    case groove_Op_Arrange:
        return eval_unary_input(ev, gn, node, out);
    /* A lookup's storage probes are driven by evaluation sessions. */
    case groove_Op_Lookup:
        return GROOVE_IVM_ERR_C(GROOVE_IVM_UnsupportedLookup, "a lookup cannot run in a recursive step");
    case groove_Op_Filter:
        UNARY(NS(update_filter)(&op->v.filter, output_desc, &input, out));
        return e;
    case groove_Op_MapProject: {
        groove_RecordDeltas input;
        JZ_TRY(eval_unary_input(ev, gn, node, &input));
        groove_PreparedProjection *fields = NULL;
        e = groove__ivm__runtime__record_projection__raw_projection_fields(&op->v.map_project, input.descriptor, output_desc, &fields);
        if (!JZ_IS_ERR(e)) e = NS(update_map_project)(&op->v.map_project, output_desc, &input, fields, false, out);
        if (fields) jz_rc_release(fields);
        groove_RecordDeltas_drop(&input);
        return e;
    }
    case groove_Op_UnwrapNullable:
        UNARY(NS(update_unwrap_nullable)(&op->v.unwrap_nullable, output_desc, &input, out));
        return e;
    case groove_Op_Unnest:
        UNARY(NS(update_unnest)(&op->v.unnest, output_desc, &input, out));
        return e;
    case groove_Op_VariantProject:
        UNARY(NS(update_variant_project)(&op->v.variant_project, output_desc, &input, out));
        return e;
    case groove_Op_ArgMaxBy: case groove_Op_ArgMinBy: {
        const groove_ArgByOp *a = &op->v.arg_by;
        UNARY(REC(hydrated_arg_by_winners)(&input, output_desc, (const usize *)a->group_field_indices.ptr, a->group_field_indices.len,
                                            (const usize *)a->comparison_field_indices.ptr, a->comparison_field_indices.len,
                                            op->tag == groove_Op_ArgMaxBy ? GROOVE_ARGBY_MAX : GROOVE_ARGBY_MIN, out));
        return e;
    }
    case groove_Op_Union: {
        u32 n = gn->descriptor.inputs.len;
        groove_RecordDeltas **inputs = calloc(n ? n : 1, sizeof *inputs);
        u32 got = 0;
        for (; got < n; got++) {
            groove_RecordDeltas *arc = jz_rc_new(sizeof(groove_RecordDeltas), deltas_drop_v);
            e = eval_node(ev, gn->descriptor.inputs.ptr[got], arc);
            if (JZ_IS_ERR(e)) { *arc = empty_deltas(output_desc); jz_rc_release(arc); break; }
            inputs[got] = arc;
        }
        if (JZ_IS_ERR(e)) { for (u32 i = 0; i < got; i++) jz_rc_release(inputs[i]); free(inputs); return e; }
        e = NS(update_union)(output_desc, inputs, n, out); /* consumes the Arcs */
        free(inputs);
        return e;
    }
    case groove_Op_IndexBy:
        UNARY(NS(update_index_by)(&op->v.index_by, output_desc, &input, out));
        return e;
    case groove_Op_Join: {
        if (gn->descriptor.inputs.len != 2) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphInputArityMismatch, node);
        groove_RecordDeltas left, right;
        JZ_TRY(eval_node(ev, gn->descriptor.inputs.ptr[0], &left));
        e = eval_node(ev, gn->descriptor.inputs.ptr[1], &right);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(&left); return e; }
        e = eval_join_snapshot(&op->v.join, output_desc, &left, &right, out);
        groove_RecordDeltas_drop(&left); groove_RecordDeltas_drop(&right);
        return e;
    }
    case groove_Op_TopBy: case groove_Op_CollectBy:
        return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    case groove_Op_AntiJoin:
        return eval_threshold(ev, gn, node, output_desc, false, out);
    /* A nested recursive graph needs its own persisted frontier state. */
    case groove_Op_Recursive:
        return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedNestedRecursion);
    case groove_Op_RecursiveStepWitness:
        return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    case groove_Op_Persist: case groove_Op_StreamingChecksum: case groove_Op_Distinct: case groove_Op_Negate:
        return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    /* A semi-join is positive relational algebra: evaluate with fresh
     * replace-mode arrangements like AntiJoin. */
    case groove_Op_SemiJoin:
        return eval_threshold(ev, gn, node, output_desc, true, out);
    case groove_Op_Aggregate:
        return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    default:
        JZ_UNREACHABLE();
    }
}

static jz_err eval_node(HydrationEvaluator *ev, groove_NodeId node, groove_RecordDeltas *out) {
    const groove_RecordDeltas *records = jz_hmap_get(ev->memo, &node);
    if (records) { groove_RecordDeltas_clone(out, records); return JZ_OK; }
    return eval_node_uncached(ev, node, out);
}

/* ---------- HydrationTraversal::poll ---------- */
typedef enum { THP_Yield, THP_Ready } TraversalProgressTag;

static jz_err traversal_poll(HydrationTraversal *t, groove_HydrationRecomputeContext *context, u32 *progress, groove_RecordDeltas *ready) {
    usize remaining = MAX_HYDRATION_TRAVERSAL_NODES_PER_POLL;
    while (remaining > 0) {
        if (t->discovery.len > 0) {
            Frame frame = t->discovery.ptr[--t->discovery.len];
            remaining -= 1;
            if (frame.tag == Frame_Visit) {
                bool inserted; jz_hmap_entry(&t->discovered, &frame.node, &inserted);
                if (!inserted) {
                    if (jz_hmap_get(&t->visiting, &frame.node)) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphCycle, frame.node);
                    continue;
                }
                jz_hmap_insert(&t->visiting, &frame.node, NULL, NULL);
                jz_err e = JZ_OK;
                const groove_GraphNode *gn = graph_node_of(context->graph, frame.node, &e);
                if (!gn) return e;
                Frame ev = { Frame_Evaluate, frame.node };
                JZ_VEC_PUSH(t->discovery, ev);
                for (u32 i = gn->descriptor.inputs.len; i > 0; i--) {
                    Frame v = { Frame_Visit, gn->descriptor.inputs.ptr[i - 1] };
                    JZ_VEC_PUSH(t->discovery, v);
                }
            } else {
                /* Keep the removal outside any assertion: a node that stays
                 * marked as visiting turns later shared inputs into a false
                 * dependency cycle. */
                jz_hmap_remove(&t->visiting, &frame.node, NULL, NULL);
                JZ_VEC_PUSH(t->order, frame.node);
            }
            continue;
        }
        if (t->next_evaluation >= t->order.len) {
            const groove_RecordDeltas *root = jz_hmap_get(&t->memo, &t->root);
            if (!root) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, t->root);
            groove_RecordDeltas_clone(ready, root);
            *progress = THP_Ready;
            return JZ_OK;
        }
        groove_NodeId node = t->order.ptr[t->next_evaluation];
        remaining -= 1;
        HydrationEvaluator evaluator = {
            context->schema, context->graph, context->variant_projections,
            context->table_deltas, context->n_table_deltas, context->has_table_deltas,
            context->inputs, context->storage, context->binding_snapshots,
            &t->context, &t->memo,
        };
        groove_RecordDeltas result;
        JZ_TRY(eval_node(&evaluator, node, &result));
        groove_RecordDeltas old;
        if (jz_hmap_insert(&t->memo, &node, &result, &old)) groove_RecordDeltas_drop(&old);
        t->next_evaluation += 1;
    }
    *progress = THP_Yield;
    return JZ_OK;
}

/* Poll the retained traversal; keep it across EvaluationBlocked and yields. */
static jz_err poll_retained(groove_RecursiveState *s, HydrationTraversal *traversal, groove_HydrationRecomputeContext *context, u32 *progress, groove_RecordDeltas *ready) {
    jz_err e = traversal_poll(traversal, context, progress, ready);
    if (JZ_IS_ERR(e)) {
        /* The session owns request registration, but this operator owns
         * traversal continuation: keep the exact postorder stack/memo. */
        if (e.code == GROOVE_IVM_EvaluationBlocked) pending_hydration_mut(s)->traversal = traversal;
        else traversal_drop(traversal);
        return e;
    }
    if (*progress == THP_Yield) pending_hydration_mut(s)->traversal = traversal;
    else traversal_drop(traversal);
    return JZ_OK;
}

static void set_phase(groove_RecursiveState *s, PendingHydrationPhase phase) {
    PendingHydrationRecompute *p = pending_hydration_mut(s);
    phase_drop(&p->phase);
    p->phase = phase;
}

jz_err REC(resume_inputs_hydration_recompute)(groove_RecursiveState *s, groove_HydrationRecomputeContext context, groove_NodeId node, const groove_RecursiveOp *recursive,
                                              groove_RecordDescriptor output_desc, groove_RecursiveNodes nodes, u32 *progress_out) {
    if (!s->pending_hydration || s->pending_hydration->input_generation != context.input_generation)
        begin_hydration_recompute(s, context.input_generation);
    PendingHydrationPhase phase;
    phase_clone(&phase, &pending_hydration_mut(s)->phase);
    jz_err e = JZ_OK;
    u32 progress; groove_RecordDeltas ready;
    switch (phase.tag) {
    case Phase_Seed: {
        HydrationTraversal *traversal = pending_hydration_mut(s)->traversal;
        pending_hydration_mut(s)->traversal = NULL;
        if (!traversal) { groove_EvalContext c; groove__ivm__runtime__state__EvalContext__root(&c); traversal = traversal_new(nodes.seed, c); }
        JZ_TRY(poll_retained(s, traversal, &context, &progress, &ready));
        if (progress == THP_Yield) { *progress_out = GROOVE_HRP_YIELD; return JZ_OK; }
        if (ready.descriptor != output_desc) { groove_RecordDeltas_drop(&ready); return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch); }
        groove_RecordDeltaVec accepted;
        JZ_TRY(accept_positive_into_set(&pending_hydration_mut(s)->accumulated, ready.deltas, &accepted));
        PendingHydrationPhase next; memset(&next, 0, sizeof next);
        if (accepted.len == 0) { record_delta_vec_drop(&accepted); next.tag = Phase_ReadyForArrangementHydration; }
        else { next.tag = Phase_Step; next.frontier.descriptor = output_desc; next.frontier.deltas = accepted; next.sub_tick = 1; }
        set_phase(s, next);
        *progress_out = GROOVE_HRP_YIELD;
        return JZ_OK;
    }
    case Phase_Step: {
        usize sub_tick = phase.sub_tick;
        if (sub_tick > recursive->max_iters) {
            phase_drop(&phase);
            if (recursive->truncate_at_max_iters) {
                PendingHydrationPhase next = { Phase_ReadyForArrangementHydration };
                set_phase(s, next);
                *progress_out = GROOVE_HRP_READY_FOR_ARRANGEMENT_HYDRATION;
                return JZ_OK;
            }
            return groove__ivm__runtime__IvmRuntimeError__recursive_limit(node, recursive->max_iters);
        }
        HydrationTraversal *traversal = pending_hydration_mut(s)->traversal;
        pending_hydration_mut(s)->traversal = NULL;
        if (!traversal) {
            groove_RecordDeltas f; groove_RecordDeltas_clone(&f, &phase.frontier);
            groove_FrontierName b = { jz_str_clone(&recursive->frontier.name) };
            groove_EvalContext c; groove__ivm__runtime__state__EvalContext__with_binding(context.scope, (u64)sub_tick, b, f, &c);
            traversal = traversal_new(nodes.step, c);
        }
        e = poll_retained(s, traversal, &context, &progress, &ready);
        if (JZ_IS_ERR(e)) { phase_drop(&phase); return e; }
        if (progress == THP_Yield) { phase_drop(&phase); *progress_out = GROOVE_HRP_YIELD; return JZ_OK; }
        if (ready.descriptor != output_desc) { phase_drop(&phase); groove_RecordDeltas_drop(&ready); return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch); }
        groove_RecordDeltaVec accepted;
        e = accept_positive_into_set(&pending_hydration_mut(s)->accumulated, ready.deltas, &accepted);
        if (JZ_IS_ERR(e)) { phase_drop(&phase); return e; }
        PendingHydrationPhase next; memset(&next, 0, sizeof next);
        if (nodes.has_step_witness) {
            next.tag = Phase_StepWitness; next.frontier = phase.frontier;
            next.step_delta.descriptor = output_desc; next.step_delta.deltas = accepted; next.sub_tick = sub_tick;
        } else if (accepted.len == 0) {
            phase_drop(&phase); record_delta_vec_drop(&accepted);
            next.tag = Phase_ReadyForArrangementHydration;
        } else {
            phase_drop(&phase);
            next.tag = Phase_Step; next.frontier.descriptor = output_desc; next.frontier.deltas = accepted; next.sub_tick = sub_tick + 1;
        }
        set_phase(s, next);
        *progress_out = GROOVE_HRP_YIELD;
        return JZ_OK;
    }
    case Phase_StepWitness: {
        if (!nodes.has_step_witness) jz_panic("witness phase requires a witness graph");
        usize sub_tick = phase.sub_tick;
        HydrationTraversal *traversal = pending_hydration_mut(s)->traversal;
        pending_hydration_mut(s)->traversal = NULL;
        if (!traversal) {
            groove_FrontierName b = { jz_str_clone(&recursive->frontier.name) };
            groove_EvalContext c; groove__ivm__runtime__state__EvalContext__with_binding(context.scope, (u64)sub_tick, b, phase.frontier, &c);
            phase.frontier = empty_deltas(output_desc);
            traversal = traversal_new(nodes.step_witness, c);
        }
        e = poll_retained(s, traversal, &context, &progress, &ready);
        if (JZ_IS_ERR(e)) { phase_drop(&phase); return e; }
        if (progress == THP_Yield) { phase_drop(&phase); *progress_out = GROOVE_HRP_YIELD; return JZ_OK; }
        PendingHydrationRecompute *pending = pending_hydration_mut(s);
        CONSOLIDATE(&ready.deltas);
        JZ_VEC_FOR(groove_RecordDelta, delta, ready.deltas) {
            bool inserted;
            i64 *w = jz_hmap_entry(&pending->step_witness_accumulated, &delta->record, &inserted);
            if (inserted) { *w = 0; delta->record = (jz_bytes){0}; }
            *w += delta->weight;
        }
        groove_RecordDeltas_drop(&ready);
        jz_bytes *k; i64 *w;
        groove_BytesVec zero = {0};
        JZ_HMAP_FOR(&pending->step_witness_accumulated, k, w) if (*w == 0) { jz_bytes c = jz_bytes_clone(k); JZ_VEC_PUSH(zero, c); }
        JZ_VEC_FOR(jz_bytes, z, zero) jz_hmap_remove(&pending->step_witness_accumulated, z, NULL, NULL);
        jz_vec_drop(&zero, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
        PendingHydrationPhase next; memset(&next, 0, sizeof next);
        groove_RecordDeltas_drop(&phase.frontier);
        if (phase.step_delta.deltas.len == 0) { groove_RecordDeltas_drop(&phase.step_delta); next.tag = Phase_ReadyForArrangementHydration; }
        else { next.tag = Phase_Step; next.frontier = phase.step_delta; next.sub_tick = sub_tick + 1; }
        set_phase(s, next);
        *progress_out = GROOVE_HRP_YIELD;
        return JZ_OK;
    }
    case Phase_ReadyForArrangementHydration:
    default:
        *progress_out = GROOVE_HRP_READY_FOR_ARRANGEMENT_HYDRATION;
        return JZ_OK;
    }
}

/* ---------- live recursive tick ---------- */
jz_err REC(hydrate_recursive_arrangements)(groove_GraphRuntimeView *runtime, const groove_RecursiveOp *recursive, groove_NodeId step, groove_RecordDeltas accumulated) {
    /* Evaluate the step once against snapshot table deltas and the full
     * accumulated relation; the result only prepares shared arrangements. */
    groove_TableDeltaVec full_table_deltas = {0};
    if (!runtime->evaluation_inputs) {
        jz_err e = REC(snapshot_table_deltas)(runtime->schema, runtime->graph, runtime->storage, step, &full_table_deltas);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(&accumulated); return e; }
    }
    groove_FrontierName b = { jz_str_clone(&recursive->frontier.name) };
    groove_RecordDeltas discarded;
    jz_err e = groove__ivm__runtime__evaluator__GraphRuntimeView__eval_with_binding_and_table_deltas(runtime, full_table_deltas.ptr, full_table_deltas.len, 0, b, accumulated, step, &discarded);
    table_delta_vec_drop(&full_table_deltas);
    if (JZ_IS_ERR(e)) return e;
    groove_RecordDeltas_drop(&discarded);
    groove__ivm__runtime__evaluator__GraphRuntimeView__clear_operator_state_for_scope(runtime);
    return JZ_OK;
}

static groove_FrontierName frontier_clone(const groove_RecursiveOp *r) { groove_FrontierName b = { jz_str_clone(&r->frontier.name) }; return b; }

jz_err REC(recursive_delta)(groove_RecursiveState *s, groove_GraphRuntimeView *runtime, groove_NodeId node, const groove_RecursiveOp *recursive,
                            groove_RecordDescriptor output_desc, groove_RecursiveNodes nodes, u32 *progress_out, groove_RecordDeltaVec *out) {
    begin_step_witness_tick(s);
    bool recompute_table, recompute_binding;
    JZ_TRY(has_recompute_table_delta(runtime->graph, runtime->table_deltas, runtime->n_table_deltas, nodes, &recompute_table));
    bool has_table_delta = has_table_delta_for_cached_tables(runtime, recursive);
    JZ_TRY(has_recompute_binding_delta(runtime->graph, runtime->binding_deltas, runtime->n_binding_deltas, nodes, &recompute_binding));
    bool has_binding_deltas = runtime->n_binding_deltas > 0;
    if (recompute_table || recompute_binding || (!has_binding_deltas && RS(is_empty)(s)) ||
        !s->step_arrangements_hydrated || (recursive->truncate_at_max_iters && (has_table_delta || has_binding_deltas))) {
        /* Retractions are handled by full recompute + diff until DRed or
         * DBSP-style nested negative deltas exist. (JAZZ_CLOSURE_TRACE
         * diagnostics read the environment, which is always unset on wasm.) */
        if (!RS(has_pending_hydration_for)(s, runtime->current_tick)) runtime->metrics->recursive_recomputes += 1;
        groove_HydrationRecomputeContext ctx = {
            runtime->schema, runtime->graph, runtime->variant_projections, runtime->evaluation_inputs,
            NULL, 0, false, runtime->storage, runtime->binding_snapshots, runtime->scope,
            /* The live tick's inputs are immutable while it is driven. */
            runtime->current_tick,
        };
        u32 progress;
        JZ_TRY(REC(resume_inputs_hydration_recompute)(s, ctx, node, recursive, output_desc, nodes, &progress));
        if (progress == GROOVE_HRP_YIELD) { *progress_out = GROOVE_RDP_YIELD; return JZ_OK; }
        jz_hmap next, next_step_witness;
        RS(finish_hydration_recompute)(s, &next, &next_step_witness);
        groove_RecordDeltas accumulated = empty_deltas(output_desc);
        positive_deltas(&next, &accumulated.deltas);
        groove_RecordDeltaVec emitted;
        RS(replace_with)(s, next, &emitted);
        RS(replace_step_witness_with)(s, next_step_witness);
        jz_err e = REC(hydrate_recursive_arrangements)(runtime, recursive, nodes.step, accumulated);
        if (!JZ_IS_ERR(e) && nodes.has_step_witness) {
            groove_RecordDeltas acc = empty_deltas(output_desc);
            RS(accumulated_deltas)(s, &acc.deltas);
            e = REC(hydrate_recursive_arrangements)(runtime, recursive, nodes.step_witness, acc);
        }
        if (JZ_IS_ERR(e)) { record_delta_vec_drop(&emitted); return e; }
        s->step_arrangements_hydrated = true;
        *progress_out = GROOVE_RDP_READY;
        *out = emitted;
        return JZ_OK;
    }

    groove_RecordDeltaVec emitted = {0};
    groove_RecordDeltas seed_delta;
    jz_err e;
    if (has_binding_deltas) {
        /* A new binding can make existing seed-table rows visible without a
         * table delta: evaluate the seed over a current snapshot. */
        groove_TableDeltaVec full = {0};
        if (runtime->evaluation_inputs) e = snapshot_table_deltas_from_inputs(runtime->schema, runtime->graph, runtime->evaluation_inputs, nodes.seed, &full);
        else e = REC(snapshot_table_deltas)(runtime->schema, runtime->graph, runtime->storage, nodes.seed, &full);
        if (JZ_IS_ERR(e)) return e;
        e = groove__ivm__runtime__evaluator__GraphRuntimeView__eval_with_binding_and_table_deltas(runtime, full.ptr, full.len, 0, frontier_clone(recursive), empty_deltas(output_desc), nodes.seed, &seed_delta);
        table_delta_vec_drop(&full);
        if (JZ_IS_ERR(e)) return e;
    } else {
        JZ_TRY(groove__ivm__runtime__evaluator__GraphRuntimeView__eval_root(runtime, nodes.seed, &seed_delta));
    }
    if (seed_delta.descriptor != output_desc) { groove_RecordDeltas_drop(&seed_delta); return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch); }
    groove_RecordDeltaVec seed_frontier;
    JZ_TRY(accept_positive_staged(s, seed_delta.deltas, &seed_frontier));
    JZ_VEC_FOR(groove_RecordDelta, d, seed_frontier) { groove_RecordDelta c; groove_RecordDelta_clone(&c, d); JZ_VEC_PUSH(emitted, c); }

    groove_RecordDeltas frontier = empty_deltas(output_desc);
    if (has_table_delta) {
        /* A changed table can extend paths ending at the changed row's join
         * key: select only those already-known paths. */
        groove_RecordDeltaVec acc, affected;
        RS(accumulated_deltas)(s, &acc);
        e = affected_recursive_frontier(runtime, recursive, &acc, nodes.step, &affected);
        record_delta_vec_drop(&acc);
        if (JZ_IS_ERR(e)) { record_delta_vec_drop(&seed_frontier); record_delta_vec_drop(&emitted); return e; }
        jz_vec_extend(&seed_frontier, affected.ptr, affected.len, sizeof(groove_RecordDelta));
        jz_vec_drop(&affected, sizeof(groove_RecordDelta), NULL);
        CONSOLIDATE(&seed_frontier);
    }
    frontier.deltas = seed_frontier;
    usize sub_tick = 1;
    bool must_run_step = true;
    for (;;) {
        if (sub_tick > recursive->max_iters) {
            if (recursive->truncate_at_max_iters) break;
            groove_RecordDeltas_drop(&frontier); record_delta_vec_drop(&emitted);
            return groove__ivm__runtime__IvmRuntimeError__recursive_limit(node, recursive->max_iters);
        }
        CONSOLIDATE(&frontier.deltas);
        if (frontier.deltas.len == 0 && !must_run_step) break;
        must_run_step = false;
        groove_RecordDeltas step_frontier; groove_RecordDeltas_clone(&step_frontier, &frontier);
        groove_RecordDeltas step_delta;
        e = groove__ivm__runtime__evaluator__GraphRuntimeView__eval_with_binding(runtime, (u64)sub_tick, frontier_clone(recursive), frontier, nodes.step, &step_delta);
        frontier = empty_deltas(output_desc);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(&step_frontier); record_delta_vec_drop(&emitted); return e; }
        if (step_delta.descriptor != output_desc) e = GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
        if (!JZ_IS_ERR(e) && nodes.has_step_witness) {
            groove_RecordDeltas witness_delta;
            e = groove__ivm__runtime__evaluator__GraphRuntimeView__eval_with_binding(runtime, (u64)sub_tick, frontier_clone(recursive), step_frontier, nodes.step_witness, &witness_delta);
            step_frontier = empty_deltas(output_desc);
            if (!JZ_IS_ERR(e)) {
                if (!recursive->has_step_witness_output || witness_delta.descriptor != recursive->step_witness_output) {
                    e = GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
                    groove_RecordDeltas_drop(&witness_delta);
                } else append_step_witness(s, witness_delta.deltas);
            }
        }
        groove_RecordDeltas_drop(&step_frontier);
        if (JZ_IS_ERR(e)) { groove_RecordDeltas_drop(&step_delta); record_delta_vec_drop(&emitted); return e; }
        groove_RecordDeltaVec accepted;
        e = accept_positive_staged(s, step_delta.deltas, &accepted);
        if (JZ_IS_ERR(e)) { record_delta_vec_drop(&emitted); return e; }
        if (accepted.len == 0) { record_delta_vec_drop(&accepted); break; }
        JZ_VEC_FOR(groove_RecordDelta, d, accepted) { groove_RecordDelta c; groove_RecordDelta_clone(&c, d); JZ_VEC_PUSH(emitted, c); }
        frontier.deltas = accepted;
        sub_tick += 1;
    }
    groove_RecordDeltas_drop(&frontier);
    CONSOLIDATE(&emitted);
    *progress_out = GROOVE_RDP_READY;
    *out = emitted;
    return JZ_OK;
}
