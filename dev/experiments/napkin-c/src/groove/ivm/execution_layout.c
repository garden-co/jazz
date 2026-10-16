/* execution_layout.c: port of crates/groove/src/ivm/execution_layout.rs.
 * Reusable graph wiring, deliberately independent of evaluation state. */
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "graph.h"

static u64 nodeid_hash_v(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool nodeid_eq_v(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static const jz_hmap_desc SLOTS_DESC = { sizeof(u64), sizeof(u32), nodeid_hash_v, nodeid_eq_v, NULL, NULL };

static void ExecutionLayout__drop(void *p) {
    groove_ExecutionLayout *l = p;
    jz_vec_drop(&l->nodes, sizeof(u64), NULL);
    jz_hmap_drop(&l->slots);
    jz_vec_drop(&l->roots, sizeof(u64), NULL);
    jz_vec_drop(&l->input_counts, sizeof(u32), NULL);
    jz_vec_drop(&l->input_offsets, sizeof(u32), NULL);
    jz_vec_drop(&l->input_slots, sizeof(u32), NULL);
    jz_vec_drop(&l->source_slots, sizeof(u32), NULL);
    jz_vec_drop(&l->pipeline_predecessors, sizeof(groove_OptU32), NULL);
    jz_vec_drop(&l->dependent_offsets, sizeof(u32), NULL);
    jz_vec_drop(&l->dependent_slots, sizeof(u32), NULL);
}
static u32 slot_of(const jz_hmap *slots, u64 id) { return *(const u32 *)jz_hmap_get(slots, &id); }
static void zeroed_u32s(groove_UsizeVec *v, u32 n) {
    memset(v, 0, sizeof *v);
    jz_vec_reserve(v, n, sizeof(u32));
    memset(v->ptr, 0, n * sizeof(u32));
    v->len = n;
}

static bool ExecutionLayout__compile(const groove_IvmGraph *graph, groove_NodeIdVec roots, groove_ExecutionLayout **out, groove_NodeId *err) {
    groove_ExecutionLayout *l = jz_rc_new(sizeof(groove_ExecutionLayout), ExecutionLayout__drop);
    memset(l, 0, sizeof *l);
    jz_hmap_init(&l->slots, &SLOTS_DESC);
    l->roots = roots;
    jz_deque pending;
    jz_deque_init(&pending, sizeof(u64));
    for (u32 i = 0; i < roots.len; i++) jz_deque_push_back(&pending, &roots.ptr[i]);
    u64 id;
    while (jz_deque_pop_front(&pending, &id)) {
        if (jz_hmap_get(&l->slots, &id)) continue;
        const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, id);
        if (!node) { *err = id; goto fail; }
        u32 slot = l->nodes.len;
        jz_hmap_insert(&l->slots, &id, &slot, NULL);
        JZ_VEC_PUSH(l->nodes, id);
        JZ_VEC_PUSH(l->input_counts, node->descriptor.inputs.len);
        if (node->descriptor.operator_.tag == GROOVE_OP_TABLE_SOURCE || node->descriptor.operator_.tag == GROOVE_OP_INDEX_SOURCE)
            JZ_VEC_PUSH(l->source_slots, slot);
        for (u32 i = 0; i < node->descriptor.inputs.len; i++) jz_deque_push_back(&pending, &node->descriptor.inputs.ptr[i]);
    }

    /* Compressed adjacency: one allocation for every reverse edge, with
     * repeated inputs preserved (a self-join has two dependency edges). */
    u32 n = l->nodes.len;
    zeroed_u32s(&l->dependent_offsets, n + 1);
    jz_vec_reserve(&l->input_offsets, n + 1, sizeof(u32));
    for (u32 s = 0; s < n; s++) {
        JZ_VEC_PUSH(l->input_offsets, l->input_slots.len);
        const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, l->nodes.ptr[s]);
        if (!node) { *err = l->nodes.ptr[s]; goto fail; }
        for (u32 i = 0; i < node->descriptor.inputs.len; i++) {
            u32 input_slot = slot_of(&l->slots, node->descriptor.inputs.ptr[i]);
            JZ_VEC_PUSH(l->input_slots, input_slot);
            l->dependent_offsets.ptr[input_slot + 1] += 1;
        }
    }
    JZ_VEC_PUSH(l->input_offsets, l->input_slots.len);
    for (u32 s = 0; s < n; s++) l->dependent_offsets.ptr[s + 1] += l->dependent_offsets.ptr[s];
    groove_UsizeVec positions;
    groove__ivm__usizevec_clone(&positions, &l->dependent_offsets);
    if (l->dependent_offsets.len == 0) jz_unwrap_failed();
    zeroed_u32s(&l->dependent_slots, l->dependent_offsets.ptr[l->dependent_offsets.len - 1]);
    for (u32 s = 0; s < n; s++) {
        const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, l->nodes.ptr[s]);
        if (!node) { jz_vec_drop(&positions, sizeof(u32), NULL); *err = l->nodes.ptr[s]; goto fail; }
        for (u32 i = 0; i < node->descriptor.inputs.len; i++) {
            u32 *position = &positions.ptr[slot_of(&l->slots, node->descriptor.inputs.ptr[i])];
            l->dependent_slots.ptr[*position] = s;
            *position += 1;
        }
    }
    jz_vec_drop(&positions, sizeof(u32), NULL);

    JZ_VEC(bool) unary = {0};
    jz_vec_reserve(&unary, n, sizeof(bool));
    for (u32 s = 0; s < n; s++) JZ_VEC_PUSH(unary, groove__ivm__runtime__pipeline__supports_node(graph, l->nodes.ptr[s]));
    jz_vec_reserve(&l->pipeline_predecessors, n, sizeof(groove_OptU32));
    for (u32 s = 0; s < n; s++) {
        groove_OptU32 pred = { false, 0 };
        if (unary.ptr[s]) {
            const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(graph, l->nodes.ptr[s]);
            if (node && node->descriptor.inputs.len) {
                u64 input = node->descriptor.inputs.ptr[0];
                u32 predecessor = slot_of(&l->slots, input);
                u32 at;
                bool is_root = jz_bsearch(l->roots.ptr, l->roots.len, sizeof(u64), &input, jz_cmp_u64, NULL, &at);
                if (unary.ptr[predecessor] && !is_root &&
                    l->dependent_offsets.ptr[predecessor + 1] - l->dependent_offsets.ptr[predecessor] == 1) {
                    pred.some = true;
                    pred.v = predecessor;
                }
            }
        }
        JZ_VEC_PUSH(l->pipeline_predecessors, pred);
    }
    jz_vec_drop(&unary, sizeof(bool), NULL);
    jz_deque_drop(&pending, NULL);
    *out = l;
    return true;
fail:
    jz_deque_drop(&pending, NULL);
    jz_rc_release(l);
    return false;
}

const u32 *groove__ivm__execution_layout__ExecutionLayout__dependents(const groove_ExecutionLayout *self, u32 slot, u32 *len) {
    u32 lo = self->dependent_offsets.ptr[slot], hi = self->dependent_offsets.ptr[slot + 1];
    *len = hi - lo;
    return self->dependent_slots.ptr + lo;
}
const u32 *groove__ivm__execution_layout__ExecutionLayout__inputs(const groove_ExecutionLayout *self, u32 slot, u32 *len) {
    u32 lo = self->input_offsets.ptr[slot], hi = self->input_offsets.ptr[slot + 1];
    *len = hi - lo;
    return self->input_slots.ptr + lo;
}
u32 groove__ivm__execution_layout__ExecutionLayout__weight(const groove_ExecutionLayout *self) {
    return self->nodes.len + self->dependent_slots.len + self->input_slots.len;
}

/* ---------------- ExecutionLayoutCache ---------------- */
static void layout_release_v(void *p) { jz_rc_release(*(groove_ExecutionLayout **)p); }
void groove__ivm__execution_layout__ExecutionLayoutCache__Default__default(groove_ExecutionLayoutCache *self) {
    jz_deque_init(&self->layouts, sizeof(groove_ExecutionLayout *));
    self->weight = 0;
    self->builds = 0;
    self->hits = 0;
}
void groove__ivm__execution_layout__ExecutionLayoutCache__drop(groove_ExecutionLayoutCache *self) {
    jz_deque_drop(&self->layouts, layout_release_v);
}
static int nodeid_cmp(const void *a, const void *b, void *ctx) { return jz_cmp_u64(a, b, ctx); }
static int nodeid_cmp_raw(const void *a, const void *b) { return jz_cmp_u64(a, b, NULL); }

bool groove__ivm__execution_layout__ExecutionLayoutCache__get(groove_ExecutionLayoutCache *cache, const groove_IvmGraph *graph,
                                                              const groove_NodeId *roots_in, u32 n_roots,
                                                              groove_ExecutionLayout **out, groove_NodeId *err) {
    groove_NodeIdVec roots = {0};
    jz_vec_extend(&roots, roots_in, n_roots, sizeof(groove_NodeId));
    jz_sort_unstable(roots.ptr, roots.len, sizeof(groove_NodeId), nodeid_cmp, NULL);
    jz_vec_dedup(&roots, sizeof(groove_NodeId), nodeid_cmp_raw, NULL);
    /* self.0.lock().expect("execution layout cache poisoned"): single-threaded, never poisoned. */
    for (u32 index = 0; index < cache->layouts.len; index++) {
        groove_ExecutionLayout *layout = *(groove_ExecutionLayout **)jz_deque_at(&cache->layouts, index);
        if (layout->roots.len == roots.len && (roots.len == 0 || memcmp(layout->roots.ptr, roots.ptr, roots.len * 8) == 0)) {
            /* cache.layouts.remove(index).unwrap(); push_back(clone) */
            for (u32 j = index; j + 1 < cache->layouts.len; j++)
                *(groove_ExecutionLayout **)jz_deque_at(&cache->layouts, j) = *(groove_ExecutionLayout **)jz_deque_at(&cache->layouts, j + 1);
            *(groove_ExecutionLayout **)jz_deque_at(&cache->layouts, cache->layouts.len - 1) = layout;
            cache->hits += 1;
            jz_vec_drop(&roots, sizeof(groove_NodeId), NULL);
            *out = jz_rc_clone(layout);
            return true;
        }
    }
    groove_ExecutionLayout *layout;
    if (!ExecutionLayout__compile(graph, roots, &layout, err)) return false;
    cache->builds += 1;
    /* Bound both entry count and topology size; giant one-off plans are
     * usable but not retained by the cache. Frames can outlive eviction. */
    const u32 MAX_LAYOUTS = 16;
    const u32 MAX_WEIGHT = 65536;
    u32 weight = groove__ivm__execution_layout__ExecutionLayout__weight(layout);
    if (weight <= MAX_WEIGHT) {
        while (cache->layouts.len >= MAX_LAYOUTS || cache->weight + weight > MAX_WEIGHT) {
            groove_ExecutionLayout *evicted;
            if (!jz_deque_pop_front(&cache->layouts, &evicted)) jz_unwrap_failed();
            cache->weight -= groove__ivm__execution_layout__ExecutionLayout__weight(evicted);
            jz_rc_release(evicted);
        }
        cache->weight += weight;
        groove_ExecutionLayout *kept = jz_rc_clone(layout);
        jz_deque_push_back(&cache->layouts, &kept);
    }
    *out = layout;
    return true;
}

void groove__ivm__execution_layout__ExecutionLayoutCache__invalidate(groove_ExecutionLayoutCache *cache, bool has_node, groove_NodeId node) {
    /* layouts.retain(|layout| node.is_some_and(|node| !layout.slots.contains_key(&node))) */
    jz_deque kept;
    jz_deque_init(&kept, sizeof(groove_ExecutionLayout *));
    groove_ExecutionLayout *layout;
    while (jz_deque_pop_front(&cache->layouts, &layout)) {
        if (has_node && !jz_hmap_get(&layout->slots, &node)) jz_deque_push_back(&kept, &layout);
        else jz_rc_release(layout);
    }
    jz_deque_drop(&cache->layouts, NULL);
    cache->layouts = kept;
    cache->weight = 0;
    for (u32 i = 0; i < cache->layouts.len; i++)
        cache->weight += groove__ivm__execution_layout__ExecutionLayout__weight(*(groove_ExecutionLayout **)jz_deque_at(&cache->layouts, i));
}

void groove__ivm__execution_layout__ExecutionLayoutCache__counters(const groove_ExecutionLayoutCache *cache, u64 *builds, u64 *hits) {
    *builds = cache->builds;
    *hits = cache->hits;
}
