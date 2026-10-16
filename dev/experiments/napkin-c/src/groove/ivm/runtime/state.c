/* groove::ivm::runtime::state: shared state and evaluation context. */
#include "mod.h"

/* ---------- shared container helpers ---------- */
u64 groove_ivm_rt_str_hash_v(const void *k) { return jz_str_hash((const jz_str *)k); }
bool groove_ivm_rt_str_eq_v(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }
void groove_ivm_rt_str_drop_v(void *s) { jz_str_drop((jz_str *)s); }
void groove_ivm_rt_bytes_drop_v(void *b) { jz_bytes_drop((jz_bytes *)b); }
u64 groove_ivm_rt_bytes_hash_v(const void *k) { const jz_bytes *b = k; return jz_hash_bytes(b->ptr, b->len); }
bool groove_ivm_rt_bytes_eq_v(const void *a, const void *b) { return jz_bytes_eq((const jz_bytes *)a, (const jz_bytes *)b); }
void groove_ivm_rt_hmap_drop_v(void *m) { jz_hmap_drop((jz_hmap *)m); }
static void deltas_drop_v(void *d) { groove_RecordDeltas_drop((groove_RecordDeltas *)d); }
static u64 frontier_hash_v(const void *k) { return jz_str_hash(&((const groove_FrontierName *)k)->name); }
static bool frontier_eq_v(const void *a, const void *b) { return jz_str_eq(&((const groove_FrontierName *)a)->name, &((const groove_FrontierName *)b)->name); }
static void frontier_drop_v(void *k) { jz_str_drop(&((groove_FrontierName *)k)->name); }
static int frontier_cmp(const void *a, const void *b, void *ctx) { (void)ctx; return jz_str_cmp(&((const groove_FrontierName *)a)->name, &((const groove_FrontierName *)b)->name); }
static u64 binding_hash_v(const void *k) { return groove_BindingSourceKey_hash((const groove_BindingSourceKey *)k); }
static bool binding_eq_v(const void *a, const void *b) { return groove_BindingSourceKey_eq(a, b); }
static void binding_drop_v(void *k) { groove_BindingSourceKey_drop((groove_BindingSourceKey *)k); }
static int binding_cmp(const void *a, const void *b, void *ctx) { (void)ctx; return groove_BindingSourceKey_cmp(a, b); }

const jz_hmap_desc groove_ivm_rt_NODE_SET = { sizeof(groove_NodeId), 0, groove_NodeId_hash, groove_NodeId_eq, NULL, NULL };
const jz_hmap_desc groove_ivm_rt_NODE_U64_MAP = { sizeof(groove_NodeId), sizeof(u64), groove_NodeId_hash, groove_NodeId_eq, NULL, NULL };
const jz_hmap_desc groove_ivm_rt_STR_U64_MAP = { sizeof(jz_str), sizeof(u64), groove_ivm_rt_str_hash_v, groove_ivm_rt_str_eq_v, groove_ivm_rt_str_drop_v, NULL };
const jz_hmap_desc groove_ivm_rt_STR_DESC_MAP = { sizeof(jz_str), sizeof(groove_RecordDescriptor), groove_ivm_rt_str_hash_v, groove_ivm_rt_str_eq_v, groove_ivm_rt_str_drop_v, NULL };
const jz_hmap_desc groove_ivm_rt_FRONTIER_DELTAS_MAP = { sizeof(groove_FrontierName), sizeof(groove_RecordDeltas), frontier_hash_v, frontier_eq_v, frontier_drop_v, deltas_drop_v };
const jz_hmap_desc groove_ivm_rt_FRONTIER_U64_MAP = { sizeof(groove_FrontierName), sizeof(u64), frontier_hash_v, frontier_eq_v, frontier_drop_v, NULL };
const jz_hmap_desc groove_ivm_rt_BINDING_U64_MAP = { sizeof(groove_BindingSourceKey), sizeof(u64), binding_hash_v, binding_eq_v, binding_drop_v, NULL };
const jz_btree_desc groove_ivm_rt_STR_SET = { sizeof(jz_str), 0, jz_cmp_str, groove_ivm_rt_str_drop_v, NULL };
const jz_btree_desc groove_ivm_rt_BINDING_SET = { sizeof(groove_BindingSourceKey), 0, binding_cmp, binding_drop_v, NULL };
const jz_btree_desc groove_ivm_rt_FRONTIER_SET = { sizeof(groove_FrontierName), 0, frontier_cmp, frontier_drop_v, NULL };
const jz_btree_desc groove_ivm_rt_BYTES_SET = { sizeof(jz_bytes), 0, jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, NULL };
const jz_btree_desc groove_ivm_rt_BYTES_I64_MAP = { sizeof(jz_bytes), sizeof(i64), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, NULL };

/* ---------- RuntimeStats / TickMetrics ---------- */
f64 groove__ivm__runtime__state__RuntimeStats__dedupe_ratio(const groove_RuntimeStats *s) {
    if (s->logical_nodes_requested == 0) return 1.0;
    return (f64)s->deduped_graph_nodes / (f64)s->logical_nodes_requested;
}

void groove_TickMetrics_drop(groove_TickMetrics *m) { jz_hmap_drop(&m->hydration_memo_computed_nodes); }

void groove_TickMetrics_clone(groove_TickMetrics *dst, const groove_TickMetrics *src) {
    *dst = *src;
    jz_hmap_init(&dst->hydration_memo_computed_nodes, &groove_ivm_rt_NODE_SET);
    groove_NodeId *k; void *v;
    JZ_HMAP_FOR(&src->hydration_memo_computed_nodes, k, v) jz_hmap_insert(&dst->hydration_memo_computed_nodes, k, NULL, NULL);
}

/* ---------- ScopePath / ScopeId ---------- */
static jz_interner *scope_pool;
static u64 scope_path_hash(const void *p) {
    const groove_ScopePath *s = p;
    u64 h = jz_hash_u64(s->nodes.len);
    for (u32 i = 0; i < s->nodes.len; i++) h = jz_hash_combine(h, s->nodes.ptr[i]);
    return h;
}
static bool scope_path_eq(const void *a, const void *b) {
    const groove_ScopePath *x = a, *y = b;
    return x->nodes.len == y->nodes.len && memcmp(x->nodes.ptr, y->nodes.ptr, x->nodes.len * sizeof(groove_NodeId)) == 0;
}

static groove_ScopeId scope_intern(groove_ScopePath path) {
    groove_ScopeId id = jz_intern(&scope_pool, &path, sizeof(path), scope_path_hash, scope_path_eq);
    if (id->nodes.ptr != path.nodes.ptr) jz_vec_drop(&path.nodes, sizeof(groove_NodeId), NULL);
    return id;
}

groove_ScopeId groove__ivm__runtime__state__ScopeId__root(void) {
    static groove_ScopeId ROOT;
    if (!ROOT) {
        groove_ScopePath root = {{0}};
        ROOT = scope_intern(root);
    }
    return ROOT;
}

groove_ScopeId groove__ivm__runtime__state__ScopeId__child(groove_ScopeId self, groove_NodeId recursive_node) {
    groove_ScopePath scope = {{0}};
    jz_vec_clone(&scope.nodes, &self->nodes, sizeof(groove_NodeId), NULL);
    JZ_VEC_PUSH(scope.nodes, recursive_node);
    return scope_intern(scope);
}

bool groove__ivm__runtime__state__ScopeId__outermost(groove_ScopeId self, groove_NodeId *out) {
    if (self->nodes.len == 0) return false;
    *out = self->nodes.ptr[0];
    return true;
}

/* OperatorStateKey and ArrangementKey share one layout: (ScopeId, NodeId). */
u64 groove_ScopedKey_hash(const void *k) {
    const groove_OperatorStateKey *key = k;
    return jz_hash_combine(jz_hash_u64((u64)(uintptr_t)key->scope), key->node);
}
bool groove_ScopedKey_eq(const void *a, const void *b) {
    const groove_OperatorStateKey *x = a, *y = b;
    return x->scope == y->scope && x->node == y->node;
}

/* ---------- SubTick / Tick / AsOf ---------- */
int groove_SubTick_cmp(groove_SubTick a, groove_SubTick b) {
    if (a.tick != b.tick) return a.tick < b.tick ? -1 : 1;
    if (a.sub_tick != b.sub_tick) return a.sub_tick < b.sub_tick ? -1 : 1;
    return 0;
}

static jz_str stamp_debug(u8 kind, groove_SubTick at) {
    if (kind == groove_AsOfKind_Tick) return jz_format("Tick(%llu)", (unsigned long long)at.tick);
    return jz_format("SubTick { tick: %llu, sub_tick: %llu }", (unsigned long long)at.tick, (unsigned long long)at.sub_tick);
}

jz_err groove__ivm__runtime__state__AsOf__value_at(const groove_AsOfStamp *self, groove_SubTick expected) {
    if (self->has && groove_SubTick_cmp(self->at, expected) == 0) return JZ_OK;
    jz_str exp = stamp_debug(self->kind, expected);
    jz_str actual = {0};
    if (self->has) actual = stamp_debug(self->kind, self->at);
    return groove__ivm__runtime__IvmRuntimeError__stale(exp, self->has, actual);
}

jz_err groove__ivm__runtime__state__AsOf__mark_forward_as_of(groove_AsOfStamp *self, groove_SubTick next) {
    if (self->has && groove_SubTick_cmp(self->at, next) > 0) {
        if (!self->has) jz_panic("checked above");
        return groove__ivm__runtime__IvmRuntimeError__out_of_order(stamp_debug(self->kind, self->at), stamp_debug(self->kind, next));
    }
    self->has = true;
    self->at = next;
    return JZ_OK;
}

void groove__ivm__runtime__state__AsOf__replace_as_of_at_least(groove_AsOfStamp *self, groove_SubTick next) {
    if (!self->has || groove_SubTick_cmp(self->at, next) <= 0) {
        self->has = true;
        self->at = next;
    }
}

/* ---------- EvalMemoKey / EvalMemoEntry ---------- */
u64 groove_EvalMemoKey_hash(const void *k) {
    const groove_EvalMemoKey *key = k;
    u64 h = jz_hash_u64((u64)(uintptr_t)key->scope);
    h = jz_hash_combine(h, key->node);
    h = jz_hash_combine(h, key->input_signature_hash);
    h = jz_hash_combine(h, key->has_tick_epoch);
    if (key->has_tick_epoch) h = jz_hash_combine(h, key->tick_epoch);
    h = jz_hash_combine(h, key->sub_tick);
    return jz_hash_combine(h, key->context_digest);
}
bool groove_EvalMemoKey_eq(const void *a, const void *b) {
    const groove_EvalMemoKey *x = a, *y = b;
    return x->scope == y->scope && x->node == y->node && x->input_signature_hash == y->input_signature_hash &&
           x->has_tick_epoch == y->has_tick_epoch && (!x->has_tick_epoch || x->tick_epoch == y->tick_epoch) &&
           x->sub_tick == y->sub_tick && x->context_digest == y->context_digest;
}

void groove_EvalMemoEntry_drop(void *e) { groove_EvalMemoEntry *entry = e; if (entry->records) jz_rc_release(entry->records); }
void groove_EvalMemoEntry_clone(void *dst, const void *src) {
    groove_EvalMemoEntry *d = dst; const groove_EvalMemoEntry *s = src;
    *d = *s;
    d->records = jz_rc_clone(s->records);
}

groove_EvalMemoEntry groove__ivm__runtime__state__EvalMemoEntry__new(groove_RecordDeltas *records, u64 input_watermark, u32 payload_bytes, u64 last_used) {
    groove_EvalMemoEntry e = { records, input_watermark, payload_bytes, last_used };
    return e;
}

/* ---------- NodeInputSignature ---------- */
void groove__ivm__runtime__state__NodeInputSignature__from_sets(jz_btree *tables, jz_btree *bindings, jz_btree *frontier_bindings, groove_NodeInputSignature *out) {
    memset(out, 0, sizeof(*out));
    out->tables = jz_alloc((tables->len ? tables->len : 1) * sizeof(jz_str));
    out->bindings = jz_alloc((bindings->len ? bindings->len : 1) * sizeof(groove_BindingSourceKey));
    out->frontier_bindings = jz_alloc((frontier_bindings->len ? frontier_bindings->len : 1) * sizeof(groove_FrontierName));
    /* into_iter(): move the sorted keys out */
    while (jz_btree_pop_first(tables, &out->tables[out->tables_len], NULL)) out->tables_len++;
    while (jz_btree_pop_first(bindings, &out->bindings[out->bindings_len], NULL)) out->bindings_len++;
    while (jz_btree_pop_first(frontier_bindings, &out->frontier_bindings[out->frontier_bindings_len], NULL)) out->frontier_bindings_len++;
    /* DefaultHasher over (tables, bindings, frontier_bindings): each slice hashes its length then its items */
    u64 h = jz_hash_u64(out->tables_len);
    for (u32 i = 0; i < out->tables_len; i++) h = jz_hash_combine(h, jz_str_hash(&out->tables[i]));
    h = jz_hash_combine(h, out->bindings_len);
    for (u32 i = 0; i < out->bindings_len; i++) h = jz_hash_combine(h, groove_BindingSourceKey_hash(&out->bindings[i]));
    h = jz_hash_combine(h, out->frontier_bindings_len);
    for (u32 i = 0; i < out->frontier_bindings_len; i++) h = jz_hash_combine(h, jz_str_hash(&out->frontier_bindings[i].name));
    out->hash = h;
}

void groove_NodeInputSignature_drop(groove_NodeInputSignature *s) {
    for (u32 i = 0; i < s->tables_len; i++) jz_str_drop(&s->tables[i]);
    for (u32 i = 0; i < s->bindings_len; i++) groove_BindingSourceKey_drop(&s->bindings[i]);
    for (u32 i = 0; i < s->frontier_bindings_len; i++) jz_str_drop(&s->frontier_bindings[i].name);
    jz_free(s->tables); jz_free(s->bindings); jz_free(s->frontier_bindings);
}

/* ---------- EvalContext ---------- */
static void eval_context_default(groove_EvalContext *out) {
    out->scope = groove__ivm__runtime__state__ScopeId__root();
    out->sub_tick = 0;
    jz_hmap_init(&out->bindings, &groove_ivm_rt_FRONTIER_DELTAS_MAP);
    jz_hmap_init(&out->binding_digests, &groove_ivm_rt_FRONTIER_U64_MAP);
    out->arrangement_update_mode = groove_ArrangementUpdateMode_Accumulate;
    out->eval_mode = groove_EvalMode_Tick;
    out->hydrate_arrangements = false;
}

void groove__ivm__runtime__state__EvalContext__root(groove_EvalContext *out) { eval_context_default(out); }

void groove__ivm__runtime__state__EvalContext__root_snapshot(groove_EvalContext *out) {
    eval_context_default(out);
    out->arrangement_update_mode = groove_ArrangementUpdateMode_Replace;
    out->eval_mode = groove_EvalMode_Hydrate;
}

void groove__ivm__runtime__state__EvalContext__root_subscription_snapshot(groove_EvalContext *out) {
    groove__ivm__runtime__state__EvalContext__root_snapshot(out);
    out->hydrate_arrangements = true;
}

void groove__ivm__runtime__state__EvalContext__with_binding(groove_ScopeId scope, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_EvalContext *out) {
    groove__ivm__runtime__state__EvalContext__with_binding_and_arrangement_mode(scope, sub_tick, binding, deltas, groove_ArrangementUpdateMode_Accumulate, out);
}

void groove__ivm__runtime__state__EvalContext__with_binding_and_arrangement_mode(groove_ScopeId scope, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_ArrangementUpdateMode mode, groove_EvalContext *out) {
    u64 digest = groove__ivm__runtime__evaluator__record_deltas_digest(&deltas);
    eval_context_default(out);
    out->scope = scope;
    out->sub_tick = sub_tick;
    groove_FrontierName binding2 = { jz_str_clone(&binding.name) };
    jz_hmap_insert(&out->bindings, &binding2, &deltas, NULL);
    jz_hmap_insert(&out->binding_digests, &binding, &digest, NULL);
    out->arrangement_update_mode = mode;
}

void groove_EvalContext_drop(groove_EvalContext *c) {
    jz_hmap_drop(&c->bindings);
    jz_hmap_drop(&c->binding_digests);
}

void groove_EvalContext_clone(groove_EvalContext *dst, const groove_EvalContext *src) {
    *dst = *src;
    jz_hmap_init(&dst->bindings, &groove_ivm_rt_FRONTIER_DELTAS_MAP);
    jz_hmap_init(&dst->binding_digests, &groove_ivm_rt_FRONTIER_U64_MAP);
    groove_FrontierName *k; void *v;
    JZ_HMAP_FOR(&src->bindings, k, v) {
        groove_FrontierName nk = { jz_str_clone(&k->name) };
        groove_RecordDeltas nv; groove_RecordDeltas_clone(&nv, v);
        jz_hmap_insert(&dst->bindings, &nk, &nv, NULL);
    }
    groove_FrontierName *k2; u64 *v2;
    JZ_HMAP_FOR(&src->binding_digests, k2, v2) {
        groove_FrontierName nk = { jz_str_clone(&k2->name) };
        jz_hmap_insert(&dst->binding_digests, &nk, v2, NULL);
    }
}
