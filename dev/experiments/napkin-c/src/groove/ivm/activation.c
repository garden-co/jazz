/* activation.c: port of crates/groove/src/ivm/activation.rs.
 * Source-keyed tick programs. Only immutable topology is retained. */
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "graph.h"

static void nodeset_arc_drop(void *p) { jz_hmap_drop((jz_hmap *)p); }
jz_hmap *groove__ivm__nodeset_arc_new(jz_hmap set) {
    jz_hmap *arc = jz_rc_new(sizeof(jz_hmap), nodeset_arc_drop);
    *arc = set;
    return arc;
}

static void ActivationPlan__drop(void *p) {
    groove_ActivationPlan *plan = p;
    jz_vec_drop(&plan->sources, sizeof(u64), NULL);
    jz_rc_release(plan->affected);
    jz_rc_release(plan->relevant);
    jz_rc_release(plan->layout);
    jz_vec_drop(&plan->durable, sizeof(u64), NULL);
    jz_vec_drop(&plan->ephemeral, sizeof(u64), NULL);
    groove__ivm__strvec_drop(&plan->tables);
    jz_vec_drop(&plan->bindings, sizeof(groove_BindingSourceKey), groove__ivm__op_types__BindingSourceKey__drop_v);
    jz_vec_drop(&plan->routed, sizeof(u64), NULL);
}

static bool contains_id(const groove_NodeIdVec *v, u64 id) {
    for (u32 i = 0; i < v->len; i++) if (v->ptr[i] == id) return true;
    return false;
}

/* fn reaches_durable(graph, barrier) -> Result<bool, NodeId> */
static bool reaches_durable(const groove_IvmGraph *graph, groove_NodeId barrier, bool *out, groove_NodeId *err) {
    jz_hmap seen;
    jz_hmap_init(&seen, &groove__ivm__NODESET_DESC);
    groove_NodeIdVec pending = {0};
    JZ_VEC_PUSH(pending, barrier);
    bool ok = true;
    *out = false;
    while (pending.len) {
        u64 id = pending.ptr[--pending.len];
        if (jz_hmap_insert(&seen, &id, NULL, NULL)) continue;
        const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, id);
        if (!node) { *err = id; ok = false; break; }
        if (groove__ivm__graph__GraphNode__is_durable(node)) { *out = true; break; }
        u64 *child; void *unused;
        JZ_HMAP_FOR(&node->children, child, unused) JZ_VEC_PUSH(pending, *child);
    }
    jz_vec_drop(&pending, sizeof(u64), NULL);
    jz_hmap_drop(&seen);
    return ok;
}

static u64 str_hash_v(const void *k) { return jz_str_hash((const jz_str *)k); }
static bool str_eq_v(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }
static const jz_hmap_desc STRSET_HDESC = { sizeof(jz_str), 0, str_hash_v, str_eq_v, groove__ivm__jz_str_drop_v, NULL };
static const jz_hmap_desc BSKSET_HDESC = { sizeof(groove_BindingSourceKey), 0, groove__ivm__op_types__BindingSourceKey__hash_v,
                                           groove__ivm__op_types__BindingSourceKey__eq_v, groove__ivm__op_types__BindingSourceKey__drop_v, NULL };
static int nodeid_cmp(const void *a, const void *b, void *ctx) { return jz_cmp_u64(a, b, ctx); }
static int nodeid_cmp_raw(const void *a, const void *b) { return jz_cmp_u64(a, b, NULL); }

static bool ActivationPlan__compile(const groove_IvmGraph *graph, groove_NodeIdVec sources, groove_ActivationPlan **out, groove_NodeId *err) {
    jz_hmap affected;
    jz_hmap_init(&affected, &groove__ivm__NODESET_DESC);
    groove_NodeIdVec pending = {0};
    jz_vec_extend(&pending, sources.ptr, sources.len, sizeof(u64));
    const groove_RouteIndex *routes = groove__ivm__graph__IvmGraph__routes(graph);
    while (pending.len) {
        u64 id = pending.ptr[--pending.len];
        /* A route barrier is reached only through its shared terminal; the
         * tick activates it when that terminal's delta carries its key. */
        if (groove__ivm__routes__RouteIndex__is_barrier(routes, id) && !contains_id(&sources, id)) {
            bool durable;
            if (!reaches_durable(graph, id, &durable, err)) goto fail_pending;
            if (!durable) continue;
        }
        if (!jz_hmap_insert(&affected, &id, NULL, NULL)) {
            const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, id);
            if (!node) { *err = id; goto fail_pending; }
            u64 *child; void *unused;
            JZ_HMAP_FOR(&node->children, child, unused) JZ_VEC_PUSH(pending, *child);
        }
    }
    jz_vec_drop(&pending, sizeof(u64), NULL);

    groove_NodeIdVec routed = {0};
    groove_NodeIdVec affected_ids = {0};
    {
        u64 *id; void *unused;
        JZ_HMAP_FOR(&affected, id, unused) {
            JZ_VEC_PUSH(affected_ids, *id);
            if (groove__ivm__routes__RouteIndex__has_table(routes, *id)) JZ_VEC_PUSH(routed, *id);
        }
    }
    jz_sort_unstable(routed.ptr, routed.len, sizeof(u64), nodeid_cmp, NULL);
    groove_ExecutionLayout *layout;
    bool ok = groove__ivm__graph__IvmGraph__execution_layout(graph, affected_ids.ptr, affected_ids.len, &layout, err);
    jz_vec_drop(&affected_ids, sizeof(u64), NULL);
    if (!ok) { jz_vec_drop(&routed, sizeof(u64), NULL); goto fail; }
    jz_hmap relevant_set;
    jz_hmap_init(&relevant_set, &groove__ivm__NODESET_DESC);
    for (u32 i = 0; i < layout->nodes.len; i++) jz_hmap_insert(&relevant_set, &layout->nodes.ptr[i], NULL, NULL);
    groove_NodeIdVec durable = {0}, ephemeral = {0};
    jz_hmap tables, bindings;
    jz_hmap_init(&tables, &STRSET_HDESC);
    jz_hmap_init(&bindings, &BSKSET_HDESC);
    for (u32 i = 0; i < layout->nodes.len; i++) {
        u64 id = layout->nodes.ptr[i];
        const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, id);
        if (!node) {
            *err = id;
            jz_vec_drop(&durable, 8, NULL); jz_vec_drop(&ephemeral, 8, NULL); jz_vec_drop(&routed, 8, NULL);
            jz_hmap_drop(&tables); jz_hmap_drop(&bindings); jz_hmap_drop(&relevant_set);
            jz_rc_release(layout);
            goto fail;
        }
        if (jz_hmap_get(&affected, &id)) {
            if (groove__ivm__graph__GraphNode__is_durable(node)) JZ_VEC_PUSH(durable, id);
            else JZ_VEC_PUSH(ephemeral, id);
        }
        const groove_OpType *op = &node->descriptor.operator_;
        switch (op->tag) {
        case GROOVE_OP_TABLE_SOURCE: {
            jz_str t = jz_str_clone(&op->u.table_source.table);
            jz_hmap_insert(&tables, &t, NULL, NULL);
            break;
        }
        case GROOVE_OP_LOOKUP: {
            jz_str t = jz_str_clone(&op->u.lookup.source.table);
            jz_hmap_insert(&tables, &t, NULL, NULL);
            break;
        }
        case GROOVE_OP_BINDING_SOURCE: {
            groove_BindingSourceKey k;
            groove__ivm__op_types__BindingSourceKey__Clone__clone(&k, &op->u.binding_source.key);
            jz_hmap_insert(&bindings, &k, NULL, NULL);
            break;
        }
        default: break;
        }
    }
    groove_ActivationPlan *plan = jz_rc_new(sizeof(groove_ActivationPlan), ActivationPlan__drop);
    plan->sources = sources;
    plan->affected = groove__ivm__nodeset_arc_new(affected);
    plan->relevant = groove__ivm__nodeset_arc_new(relevant_set);
    plan->layout = layout;
    plan->durable = durable;
    plan->ephemeral = ephemeral;
    memset(&plan->tables, 0, sizeof plan->tables);
    memset(&plan->bindings, 0, sizeof plan->bindings);
    {
        jz_str *t; void *unused;
        JZ_HMAP_FOR(&tables, t, unused) JZ_VEC_PUSH(plan->tables, jz_str_clone(t));
        groove_BindingSourceKey *k;
        JZ_HMAP_FOR(&bindings, k, unused) {
            groove_BindingSourceKey copy;
            groove__ivm__op_types__BindingSourceKey__Clone__clone(&copy, k);
            JZ_VEC_PUSH(plan->bindings, copy);
        }
    }
    jz_hmap_drop(&tables);
    jz_hmap_drop(&bindings);
    plan->routed = routed;
    *out = plan;
    return true;
fail_pending:
    jz_vec_drop(&pending, sizeof(u64), NULL);
fail:
    jz_hmap_drop(&affected);
    jz_vec_drop(&sources, sizeof(u64), NULL);
    return false;
}

static u32 ActivationPlan__weight(const groove_ActivationPlan *p) {
    return p->sources.len + p->affected->len + p->relevant->len + groove__ivm__execution_layout__ExecutionLayout__weight(p->layout) +
           p->durable.len + p->ephemeral.len + p->tables.len + p->bindings.len + p->routed.len;
}

/* ---------------- ActivationCache ---------------- */
static void plan_release_v(void *p) { jz_rc_release(*(groove_ActivationPlan **)p); }
void groove__ivm__activation__ActivationCache__Default__default(groove_ActivationCache *self) {
    jz_deque_init(&self->plans, sizeof(groove_ActivationPlan *));
    self->weight = 0;
    self->builds = 0;
    self->hits = 0;
}
void groove__ivm__activation__ActivationCache__drop(groove_ActivationCache *self) {
    jz_deque_drop(&self->plans, plan_release_v);
}

bool groove__ivm__activation__ActivationCache__get(groove_ActivationCache *cache, const groove_IvmGraph *graph,
                                                   groove_NodeIdVec sources, groove_ActivationPlan **out, groove_NodeId *err) {
    jz_sort_unstable(sources.ptr, sources.len, sizeof(u64), nodeid_cmp, NULL);
    jz_vec_dedup(&sources, sizeof(u64), nodeid_cmp_raw, NULL);
    /* self.0.lock().expect("activation cache poisoned"): single-threaded, never poisoned. */
    for (u32 index = 0; index < cache->plans.len; index++) {
        groove_ActivationPlan *plan = *(groove_ActivationPlan **)jz_deque_at(&cache->plans, index);
        if (plan->sources.len == sources.len && (sources.len == 0 || memcmp(plan->sources.ptr, sources.ptr, sources.len * 8) == 0)) {
            for (u32 j = index; j + 1 < cache->plans.len; j++)
                *(groove_ActivationPlan **)jz_deque_at(&cache->plans, j) = *(groove_ActivationPlan **)jz_deque_at(&cache->plans, j + 1);
            *(groove_ActivationPlan **)jz_deque_at(&cache->plans, cache->plans.len - 1) = plan;
            cache->hits += 1;
            jz_vec_drop(&sources, sizeof(u64), NULL);
            *out = jz_rc_clone(plan);
            return true;
        }
    }
    groove_ActivationPlan *plan;
    if (!ActivationPlan__compile(graph, sources, &plan, err)) return false;
    cache->builds += 1;
    /* A repeated activation over roughly a thousand routed subscriptions
     * exceeds the former 65K-weight cap; retain a bounded plan instead. */
    const u32 MAX_PLANS = 16;
    const u32 MAX_WEIGHT = 1000000;
    u32 weight = ActivationPlan__weight(plan);
    if (weight <= MAX_WEIGHT) {
        while (cache->plans.len >= MAX_PLANS || cache->weight + weight > MAX_WEIGHT) {
            groove_ActivationPlan *evicted;
            if (!jz_deque_pop_front(&cache->plans, &evicted)) jz_unwrap_failed();
            cache->weight -= ActivationPlan__weight(evicted);
            jz_rc_release(evicted);
        }
        cache->weight += weight;
        groove_ActivationPlan *kept = jz_rc_clone(plan);
        jz_deque_push_back(&cache->plans, &kept);
    }
    *out = plan;
    return true;
}

typedef bool (*plan_keep_fn)(const groove_ActivationPlan *plan, const void *ctx);
static void ActivationCache__retain(groove_ActivationCache *cache, plan_keep_fn keep, const void *ctx) {
    jz_deque kept;
    jz_deque_init(&kept, sizeof(groove_ActivationPlan *));
    groove_ActivationPlan *plan;
    while (jz_deque_pop_front(&cache->plans, &plan)) {
        if (keep(plan, ctx)) jz_deque_push_back(&kept, &plan);
        else jz_rc_release(plan);
    }
    jz_deque_drop(&cache->plans, NULL);
    cache->plans = kept;
    cache->weight = 0;
    for (u32 i = 0; i < cache->plans.len; i++)
        cache->weight += ActivationPlan__weight(*(groove_ActivationPlan **)jz_deque_at(&cache->plans, i));
}
typedef struct { const groove_NodeId *ids; u32 n; } id_slice;
static bool keep_unaffected(const groove_ActivationPlan *plan, const void *ctx) {
    const id_slice *s = ctx;
    for (u32 i = 0; i < s->n; i++) if (jz_hmap_get(plan->affected, &s->ids[i])) return false;
    return true;
}
static bool keep_irrelevant(const groove_ActivationPlan *plan, const void *ctx) {
    return jz_hmap_get(plan->relevant, ctx) == NULL;
}
static bool keep_none(const groove_ActivationPlan *plan, const void *ctx) { return false; }

void groove__ivm__activation__ActivationCache__added(groove_ActivationCache *self, const groove_NodeId *inputs, u32 n) {
    id_slice s = { inputs, n };
    ActivationCache__retain(self, keep_unaffected, &s);
}
void groove__ivm__activation__ActivationCache__removed(groove_ActivationCache *self, groove_NodeId node) {
    ActivationCache__retain(self, keep_irrelevant, &node);
}
void groove__ivm__activation__ActivationCache__clear(groove_ActivationCache *self) {
    ActivationCache__retain(self, keep_none, NULL);
}
