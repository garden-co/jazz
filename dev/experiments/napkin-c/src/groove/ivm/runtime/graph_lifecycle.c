/* groove::ivm::runtime::graph_lifecycle: graph retention, runtime statistics,
 * garbage collection, and node initialization. */
#include "mod.h"

#define GL(name) groove__ivm__runtime__graph_lifecycle__IvmRuntime__##name
#define EGI(name) groove__ivm__runtime__graph_lifecycle__EphemeralGraphInstall__##name

/* Owns unretained graph additions until an operation promotes them by adding
 * retainers. Cancellation or failure eagerly collects only ephemeral nodes. */
groove_EphemeralGraphInstall EGI(new)(groove_IvmRuntime *runtime) { groove_EphemeralGraphInstall g = { runtime, false }; return g; }
groove_IvmRuntime *EGI(runtime)(groove_EphemeralGraphInstall *g) { return g->runtime; }
/* Promote this provisional installation after its roots have retainers. */
void EGI(commit)(groove_EphemeralGraphInstall *g) { g->committed = true; }
void groove_EphemeralGraphInstall_drop(groove_EphemeralGraphInstall *g) {
    if (g->committed) return;
    GL(collect_unretained_ephemeral_nodes)(g->runtime);
}

/* Cache diagnostics count attempted work, not installed semantic state. */
groove_ExecutionLayoutStats GL(execution_layout_stats)(const groove_IvmRuntime *self) {
    groove_ExecutionLayoutStats s;
    groove__ivm__graph__IvmGraph__execution_layout_counters(self->graph, &s.builds, &s.hits);
    return s;
}

static bool is_durable(const groove_GraphNode *n) { return n->durability.tag == groove_NodeDurability_Durable; }
static bool has_retainers(const groove_IvmRuntime *self, groove_NodeId id) {
    const groove_NodeRuntimeMeta *meta = jz_hmap_get(&self->node_meta, &id);
    return meta && meta->retainers.len > 0;
}

void GL(retained_node_ids)(const groove_IvmRuntime *self, jz_hmap *retained) {
    jz_hmap_init(retained, &groove_ivm_rt_NODE_SET);
    groove_NodeIdVec roots = {0};
    groove_NodeId *id; groove_GraphNode *node;
    JZ_HMAP_FOR(groove__ivm__graph__IvmGraph__nodes(self->graph), id, node)
        if (is_durable(node) || has_retainers(self, node->id)) JZ_VEC_PUSH(roots, node->id);
    JZ_VEC_FOR(groove_NodeId, root, roots) groove__ivm__graph__IvmGraph__mark_ancestors(self->graph, *root, retained);
    jz_vec_drop(&roots, sizeof(groove_NodeId), NULL);
    /* A queued evaluation has already discovered every node it may resume
     * through: its graph slice must remain live. */
    jz_hmap queued;
    groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__registered_nodes(&self->pending_incremental, &queued);
    void *unused;
    JZ_HMAP_FOR(&queued, id, unused) groove__ivm__graph__IvmGraph__mark_ancestors(self->graph, *id, retained);
    jz_hmap_drop(&queued);
}

groove_RuntimeStats GL(stats)(const groove_IvmRuntime *self) {
    groove_RuntimeStats stats = GL(cheap_stats)(self);
    groove_ArrangementKey *ak; groove_AsOfArrangement *arrangement;
    JZ_HMAP_FOR(&self->arrangement_states, ak, arrangement) {
        stats.arrangement_rows += groove__ivm__runtime__join__ArrangementState__row_count(arrangement->value);
        stats.arrangement_encoded_bytes += groove__ivm__runtime__join__ArrangementState__encoded_bytes(arrangement->value);
    }
    groove_OperatorStateKey *ok; groove_OperatorState *state;
    JZ_HMAP_FOR(&self->operator_states, ok, state) {
        if (state->tag != groove_OperatorState_Recursive) continue;
        stats.recursive_state_count += 1;
        stats.recursive_accumulated_rows += groove__ivm__runtime__recursion__RecursiveState__accumulated_row_count(state->v.recursive.value);
        stats.recursive_accumulated_encoded_bytes += groove__ivm__runtime__recursion__RecursiveState__accumulated_encoded_bytes(state->v.recursive.value);
    }
    return stats;
}

/* eval_memo_bytes is maintained at every memo mutation (the debug recount is
 * compiled out of release builds). */
static usize tracked_eval_memo_bytes(const groove_IvmRuntime *self) { return self->eval_memo_bytes; }

groove_RuntimeStats GL(cheap_stats)(const groove_IvmRuntime *self) {
    groove_RuntimeStats s; memset(&s, 0, sizeof s);
    u32 graph_nodes = groove__ivm__graph__IvmGraph__nodes(self->graph)->len;
    s.graph_nodes = graph_nodes;
    s.active_subscriptions = self->multisink_subscriptions.len;
    s.active_prepared_shapes = self->prepared_shapes.len;
    groove_BindingSourceKey *k; groove_BindingSourceState *source;
    JZ_HMAP_FOR(&self->binding_sources.states, k, source) s.active_shape_params += source->refcounts.len;
    s.arrangement_count = self->arrangement_states.len;
    u32 memo_len = groove__ivm__runtime__evaluation_memo__EvaluationMemo__len(&self->eval_memo);
    s.eval_memo_entries = memo_len;
    s.hydration_memo_entries = memo_len - groove__ivm__runtime__evaluation_memo__EvaluationMemo__tick_entries(&self->eval_memo);
    s.eval_memo_bytes = (u32)tracked_eval_memo_bytes(self);
    s.hydration_memo_hits = self->hydration_memo_hits;
    s.hydration_memo_computes = self->hydration_memo_computes;
    s.hydration_memo_distinct_computed_nodes = self->hydration_memo_computed_nodes.len;
    s.logical_nodes_requested = self->logical_nodes_requested;
    s.deduped_graph_nodes = graph_nodes;
    return s;
}

void GL(record_hydration_memo_metrics)(groove_IvmRuntime *self, const groove_TickMetrics *metrics) {
    self->hydration_memo_hits += metrics->hydration_memo_hits;
    self->hydration_memo_computes += metrics->hydration_memo_computes;
    groove_NodeId *id; void *unused;
    JZ_HMAP_FOR(&metrics->hydration_memo_computed_nodes, id, unused) jz_hmap_insert(&self->hydration_memo_computed_nodes, id, NULL, NULL);
}

bool GL(add_retainer)(groove_IvmRuntime *self, groove_NodeId id, groove_Retainer retainer) {
    if (!groove__ivm__graph__IvmGraph__node(self->graph, id)) { groove_Retainer_drop(&retainer); return false; }
    groove_NodeRuntimeMeta *meta = groove_ivm_rt_node_meta_entry(&self->node_meta, id);
    meta->last_used_tick = self->current_tick;
    if (jz_hmap_get(&meta->retainers, &retainer)) { groove_Retainer_drop(&retainer); return false; }
    jz_hmap_insert(&meta->retainers, &retainer, NULL, NULL);
    return true;
}

bool GL(remove_multisink_retainers)(groove_IvmRuntime *self, groove_SubscriptionId subscription_id, const jz_btree *outputs) {
    bool removed = false;
    jz_str *sink; groove_CompiledNode *output;
    JZ_BTREE_FOR(outputs, sink, output) {
        groove_Retainer r; r.tag = groove_Retainer_Subscription;
        groove__ivm__runtime__subscriptions__SubscriptionId__retainer_key(subscription_id, &r.name);
        removed |= GL(remove_retainer)(self, output->node, &r);
        groove_Retainer_drop(&r);
        r.tag = groove_Retainer_Hydration;
        groove__ivm__runtime__subscriptions__SubscriptionId__retainer_key(subscription_id, &r.name);
        removed |= GL(remove_retainer)(self, output->node, &r);
        groove_Retainer_drop(&r);
    }
    GL(collect_unretained_ephemeral_nodes)(self);
    return removed;
}

bool GL(remove_retainer)(groove_IvmRuntime *self, groove_NodeId id, const groove_Retainer *retainer) {
    /* Conservatively a GC candidate even when nothing was removed. */
    jz_hmap_insert(&self->gc_candidates, &id, NULL, NULL);
    groove_NodeRuntimeMeta *meta = jz_hmap_get(&self->node_meta, &id);
    if (!meta) return false;
    return jz_hmap_remove(&meta->retainers, retainer, NULL, NULL);
}

static bool is_retained_gc_root(const groove_IvmRuntime *self, const groove_GraphNode *node) {
    return is_durable(node) || has_retainers(self, node->id);
}

enum { Retention_None, Retention_Queued, Retention_Retained };
static const jz_hmap_desc NODE_U8_MAP = { sizeof(groove_NodeId), sizeof(u8), groove_NodeId_hash, groove_NodeId_eq, NULL, NULL };
typedef struct StackItem { groove_NodeId id; bool children_done; } StackItem;
static bool node_live(const void *k, void *v, void *ctx) { (void)v; return groove__ivm__graph__IvmGraph__node(ctx, *(const groove_NodeId *)k) != NULL; }

/* Remove every node no durable, retained or queued node reaches. Only
 * candidates and their ancestors can have become unreachable since the
 * previous sweep. Returns the removed nodes and whether some candidate was
 * only kept alive by queued evaluation work. */
static bool gc_ephemeral_nodes(groove_IvmRuntime *self, groove_NodeIdVec *removable) {
    jz_hmap candidates = self->gc_candidates;
    jz_hmap_init(&self->gc_candidates, &groove_ivm_rt_NODE_SET);
    groove_NodeIdVec added = {0};
    groove__ivm__graph__IvmGraph__take_added_nodes(self->graph, &added);
    JZ_VEC_FOR(groove_NodeId, a, added) jz_hmap_insert(&candidates, a, NULL, NULL);
    jz_vec_drop(&added, sizeof(groove_NodeId), NULL);
    jz_hmap closure; jz_hmap_init(&closure, &groove_ivm_rt_NODE_SET);
    groove_NodeId *cid; void *unused;
    JZ_HMAP_FOR(&candidates, cid, unused) groove__ivm__graph__IvmGraph__mark_ancestors(self->graph, *cid, &closure);
    jz_hmap_retain(&closure, node_live, self->graph);
    memset(removable, 0, sizeof *removable);
    if (closure.len == 0) { jz_hmap_drop(&closure); jz_hmap_drop(&candidates); return false; }
    jz_hmap queued;
    if (groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__is_pending(&self->pending_incremental))
        groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__registered_nodes(&self->pending_incremental, &queued);
    else jz_hmap_init(&queued, &groove_ivm_rt_NODE_SET);

    /* Decide consumers before their inputs: an iterative post-order over
     * children, restricted to the closure (the graph is acyclic). */
    jz_hmap kept; jz_hmap_init(&kept, &NODE_U8_MAP);
    bool blocked_by_queue = false;
    JZ_VEC(StackItem) stack = {0};
    groove_NodeId *start;
    JZ_HMAP_FOR(&closure, start, unused) {
        if (jz_hmap_get(&kept, start)) continue;
        stack.len = 0;
        StackItem s0 = { *start, false };
        JZ_VEC_PUSH(stack, s0);
        while (stack.len > 0) {
            StackItem item = stack.ptr[--stack.len];
            if (jz_hmap_get(&kept, &item.id)) continue;
            const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(self->graph, item.id);
            if (!node) jz_panic("closure holds live nodes");
            groove_NodeId *child; void *u2;
            if (!item.children_done) {
                StackItem again = { item.id, true };
                JZ_VEC_PUSH(stack, again);
                JZ_HMAP_FOR(&node->children, child, u2)
                    if (jz_hmap_get(&closure, child) && !jz_hmap_get(&kept, child)) { StackItem c = { *child, false }; JZ_VEC_PUSH(stack, c); }
                continue;
            }
            u8 retention = Retention_None;
            bool retained = is_retained_gc_root(self, node);
            if (!retained) JZ_HMAP_FOR(&node->children, child, u2) {
                const u8 *k = jz_hmap_get(&kept, child);
                if (!jz_hmap_get(&closure, child) || (k && *k == Retention_Retained)) { retained = true; break; }
            }
            if (retained) retention = Retention_Retained;
            else {
                bool q = jz_hmap_get(&queued, &item.id) != NULL;
                if (!q) JZ_HMAP_FOR(&node->children, child, u2) { const u8 *k = jz_hmap_get(&kept, child); if (k && *k == Retention_Queued) { q = true; break; } }
                if (q) retention = Retention_Queued;
            }
            blocked_by_queue |= retention == Retention_Queued;
            jz_hmap_insert(&kept, &item.id, &retention, NULL);
        }
    }
    jz_vec_drop(&stack, sizeof(StackItem), NULL);
    groove_NodeId *kid; u8 *retention;
    JZ_HMAP_FOR(&kept, kid, retention) if (*retention == Retention_None) JZ_VEC_PUSH(*removable, *kid);
    JZ_VEC_FOR(groove_NodeId, rid, *removable) groove__ivm__graph__IvmGraph__remove_node(self->graph, *rid);
    if (blocked_by_queue) {
        /* Queued work releases its nodes without a lifecycle event, so
         * revisit these candidates on the next sweep. */
        jz_hmap_retain(&candidates, node_live, self->graph);
        JZ_HMAP_FOR(&candidates, cid, unused) jz_hmap_insert(&self->gc_candidates, cid, NULL, NULL);
    }
    jz_hmap_drop(&kept); jz_hmap_drop(&queued); jz_hmap_drop(&closure); jz_hmap_drop(&candidates);
    return blocked_by_queue;
}

static bool keep_operator_state(const void *k, void *v, void *ctx) { (void)v; return !jz_hmap_get(ctx, &((const groove_OperatorStateKey *)k)->node); }
static bool keep_arrangement_state(const void *k, void *v, void *ctx) { (void)v; return !jz_hmap_get(ctx, &((const groove_ArrangementKey *)k)->input); }

/* Reclaim unretained nodes only while all pending evaluator queues are
 * visible through pending_incremental. (The debug full-reachability check is
 * compiled out of release builds.) */
void GL(collect_unretained_ephemeral_nodes)(groove_IvmRuntime *self) {
    self->ephemeral_graph_gc_pending = true;
    if (self->pending_incremental_polling) return;
    groove_NodeIdVec removed;
    bool blocked_by_queue = gc_ephemeral_nodes(self, &removed);
    if (removed.len > 0) {
        jz_hmap set; jz_hmap_init(&set, &groove_ivm_rt_NODE_SET);
        JZ_VEC_FOR(groove_NodeId, id, removed) jz_hmap_insert(&set, id, NULL, NULL);
        /* One pass per state map, not one full scan per expired node. */
        jz_hmap_retain(&self->operator_states, keep_operator_state, &set);
        jz_hmap_retain(&self->arrangement_states, keep_arrangement_state, &set);
        groove__ivm__runtime__IvmRuntime__remove_retained_eval_memos_for_nodes(self, &set);
        groove_NodeId *id; void *unused;
        JZ_HMAP_FOR(&set, id, unused) {
            jz_hmap_remove(&self->arrangement_keys_by_input, id, NULL, NULL);
            jz_hmap_remove(&self->node_meta, id, NULL, NULL);
        }
        jz_hmap_drop(&set);
    }
    jz_vec_drop(&removed, sizeof(groove_NodeId), NULL);
    if (!blocked_by_queue) self->ephemeral_graph_gc_pending = false;
}

bool GL(affected_recursive_nodes_are_current)(const groove_IvmRuntime *self, const jz_hmap *affected, u64 current_tick) {
    groove_NodeId *node; void *unused;
    JZ_HMAP_FOR(affected, node, unused) {
        groove_OperatorStateKey key = { groove__ivm__runtime__state__ScopeId__root(), *node };
        const groove_OperatorState *state = jz_hmap_get(&self->operator_states, &key);
        if (!state || state->tag != groove_OperatorState_Recursive) continue;
        if (!(state->v.recursive.as_of.has && state->v.recursive.as_of.tick == current_tick)) return false;
    }
    return true;
}

void GL(initialize_node_runtime)(groove_IvmRuntime *self, groove_NodeId node) {
    groove_ivm_rt_node_meta_entry(&self->node_meta, node);
    const groove_GraphNode *graph_node = groove__ivm__graph__IvmGraph__node(self->graph, node);
    if (!graph_node) return;
    groove_OperatorState operator_state;
    groove__ivm__runtime__evaluator__operator_state_for(&graph_node->descriptor.operator_, &operator_state);
    if (operator_state.tag == groove_OperatorState_Stateless) return;
    groove_OperatorStateKey key = { groove__ivm__runtime__state__ScopeId__root(), node };
    if (jz_hmap_get(&self->operator_states, &key)) { groove_OperatorState_drop(&operator_state); return; }
    jz_hmap_insert(&self->operator_states, &key, &operator_state, NULL);
}
