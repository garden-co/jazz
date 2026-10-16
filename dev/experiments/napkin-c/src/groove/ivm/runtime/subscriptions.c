/* groove::ivm::runtime::subscriptions — prepared shapes, routed bindings and
 * subscription delivery state (port of subscriptions.rs). */
#include "jz_rt.h"
#include "runtime_tick.h"
#include "record_projection.h"

#define SB(name) groove__ivm__runtime__subscriptions__##name

/* ======================================================================
 * shared descriptors / helpers of this unit
 * ==================================================================== */
u64 groove_ivm_rt_a_hash_nodeid(const void *k) { return jz_hash_u64(((const groove_NodeId *)k)->v); }
bool groove_ivm_rt_a_eq_nodeid(const void *a, const void *b) { return ((const groove_NodeId *)a)->v == ((const groove_NodeId *)b)->v; }
u64 groove_ivm_rt_a_hash_bytes(const void *k) { const jz_bytes *b = k; return jz_hash_bytes(b->ptr, b->len); }
bool groove_ivm_rt_a_eq_bytes(const void *a, const void *b) {
    const jz_bytes *x = a, *y = b;
    return x->len == y->len && (x->len == 0 || memcmp(x->ptr, y->ptr, x->len) == 0);
}
void groove_ivm_rt_a_drop_bytes(void *p) { jz_bytes *b = p; jz_free(b->ptr); b->ptr = NULL; b->len = b->cap = 0; }
void groove_ivm_rt_a_drop_str(void *p) { jz_str_drop((jz_str *)p); }
void groove_ivm_rt_a_clone_bytes(void *dst, const void *src) {
    const jz_bytes *s = src; jz_bytes *d = dst;
    d->ptr = s->len ? jz_memdup(s->ptr, s->len) : NULL; d->len = d->cap = s->len;
}
void groove_ivm_rt_a_clone_str(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }

const jz_btree_desc groove_ivm_rt_a_desc_str_set = { sizeof(jz_str), 0, jz_cmp_str, groove_ivm_rt_a_drop_str, NULL };
const jz_btree_desc groove_ivm_rt_a_desc_bytes_set = { sizeof(jz_bytes), 0, jz_cmp_bytes, groove_ivm_rt_a_drop_bytes, NULL };
const jz_hmap_desc groove_ivm_rt_a_hdesc_nodeid_set = { sizeof(groove_NodeId), 0, groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, NULL };
const jz_hmap_desc groove_ivm_rt_a_hdesc_bytes_set = { sizeof(jz_bytes), 0, groove_ivm_rt_a_hash_bytes, groove_ivm_rt_a_eq_bytes, groove_ivm_rt_a_drop_bytes, NULL };

u64 groove_ivm_rt_a_hash_subscription_id(const void *k) {
    const groove_SubscriptionId *s = k;
    return jz_hash_combine(jz_hash_u64(s->runtime_namespace), s->sequence);
}
bool groove_ivm_rt_a_eq_subscription_id(const void *a, const void *b) {
    const groove_SubscriptionId *x = a, *y = b;
    return x->runtime_namespace == y->runtime_namespace && x->sequence == y->sequence;
}
static u64 hash_shape_id(const void *k) { return jz_hash_u64(((const groove_PreparedShapeId *)k)->v); }
static bool eq_shape_id(const void *a, const void *b) { return ((const groove_PreparedShapeId *)a)->v == ((const groove_PreparedShapeId *)b)->v; }
static u64 hash_binding_source_key(const void *k) { return groove_hash_BindingSourceKey((const groove_BindingSourceKey *)k, 0); }
static bool eq_binding_source_key(const void *a, const void *b) { return groove__ivm__op_types__BindingSourceKey__eq(a, b); }
static void drop_binding_source_key(void *k) { groove__ivm__op_types__BindingSourceKey__drop(k); }
static u64 hash_binding_key(const void *k) { return groove_ivm_rt_a_hash_bytes(&((const groove_BindingKey *)k)->bytes); }
static bool eq_binding_key(const void *a, const void *b) { return groove_ivm_rt_a_eq_bytes(&((const groove_BindingKey *)a)->bytes, &((const groove_BindingKey *)b)->bytes); }
static void drop_binding_key(void *k) { groove_ivm_rt_a_drop_bytes(&((groove_BindingKey *)k)->bytes); }
static void hmap_set_drop(void *m) { jz_hmap_drop((jz_hmap *)m); }

const jz_hmap_desc groove_ivm_rt_a_hdesc_multisink_subscriptions = { sizeof(groove_SubscriptionId), sizeof(groove_MultisinkSubscriptionState), groove_ivm_rt_a_hash_subscription_id, groove_ivm_rt_a_eq_subscription_id, NULL, SB(MultisinkSubscriptionState__drop) };
const jz_hmap_desc groove_ivm_rt_a_hdesc_subscription_id_set = { sizeof(groove_SubscriptionId), 0, groove_ivm_rt_a_hash_subscription_id, groove_ivm_rt_a_eq_subscription_id, NULL, NULL };
static const jz_hmap_desc hdesc_nodeid_subscription_set = { sizeof(groove_NodeId), sizeof(jz_hmap), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, hmap_set_drop };
const jz_hmap_desc groove_ivm_rt_a_hdesc_prepared_shapes = { sizeof(groove_PreparedShapeId), sizeof(groove_RoutedMultisinkShapeState), hash_shape_id, eq_shape_id, NULL, SB(RoutedMultisinkShapeState__drop) };
const jz_hmap_desc groove_ivm_rt_a_hdesc_binding_refcounts = { sizeof(groove_BindingKey), sizeof(usize), hash_binding_key, eq_binding_key, drop_binding_key, NULL };
static void rc_release_slot(void *p) { jz_rc_release(*(void **)p); }
const jz_hmap_desc groove_ivm_rt_a_hdesc_binding_snapshots = { sizeof(groove_BindingSourceKey), sizeof(groove_RecordDeltas *), hash_binding_source_key, eq_binding_source_key, drop_binding_source_key, rc_release_slot };
static const jz_hmap_desc hdesc_binding_states = { sizeof(groove_BindingSourceKey), sizeof(groove_BindingSourceState), hash_binding_source_key, eq_binding_source_key, drop_binding_source_key, SB(BindingSourceState__drop) };
static const jz_hmap_desc hdesc_binding_source_key_set = { sizeof(groove_BindingSourceKey), 0, hash_binding_source_key, eq_binding_source_key, drop_binding_source_key, NULL };
static u64 hash_ptr(const void *k) { return jz_hash_u64((u64)(usize)*(const void *const *)k); }
static bool eq_ptr(const void *a, const void *b) { return *(const void *const *)a == *(const void *const *)b; }
static const jz_hmap_desc hdesc_ptr_u64 = { sizeof(void *), sizeof(u64), hash_ptr, eq_ptr, NULL, NULL };
static const jz_hmap_desc hdesc_ptr_desc = { sizeof(void *), sizeof(groove_RecordDescriptor), hash_ptr, eq_ptr, NULL, NULL };

static jz_str str_borrow(const char *p, u32 n) { jz_str s = { (char *)p, n, 0 }; return s; }

/* SubTick / Tick Debug */
void groove_ivm_rt_a_subtick_debug(jz_str *out, groove_SubTick t, bool is_tick) {
    if (is_tick) jz_str_appendf(out, "Tick(%llu)", (unsigned long long)t.tick);
    else jz_str_appendf(out, "SubTick { tick: Tick(%llu), sub_tick: %llu }", (unsigned long long)t.tick, (unsigned long long)t.sub_tick);
}
jz_err groove_ivm_rt_a_err_out_of_order(groove_SubTick current, groove_SubTick next, bool is_tick) {
    jz_str c = {0}, n = {0};
    groove_ivm_rt_a_subtick_debug(&c, current, is_tick);
    groove_ivm_rt_a_subtick_debug(&n, next, is_tick);
    jz_err e = jz_err_fmt(GROOVE_IVM_ERR_OUT_OF_ORDER_RUNTIME_STATE, "runtime state advanced out of order: current=%.*s, next=%.*s", (int)c.len, c.ptr, (int)n.len, n.ptr);
    jz_str_drop(&c); jz_str_drop(&n);
    return e;
}

/* ======================================================================
 * RecordDelta / RecordDeltas / TableDelta / BindingDelta
 * ==================================================================== */
void SB(RecordDelta__clone)(void *dst, const void *src) {
    const groove_RecordDelta *s = src; groove_RecordDelta *d = dst;
    groove_ivm_rt_a_clone_bytes(&d->record, &s->record); d->weight = s->weight;
}
void SB(RecordDelta__drop)(void *d) { groove_ivm_rt_a_drop_bytes(&((groove_RecordDelta *)d)->record); }
bool SB(RecordDelta__eq)(const groove_RecordDelta *a, const groove_RecordDelta *b) {
    return a->weight == b->weight && groove_ivm_rt_a_eq_bytes(&a->record, &b->record);
}
void SB(RecordDeltas__clone)(groove_RecordDeltas *dst, const groove_RecordDeltas *src) {
    dst->descriptor = src->descriptor;
    memset(&dst->deltas, 0, sizeof dst->deltas);
    jz_vec_clone(&dst->deltas, &src->deltas, sizeof(groove_RecordDelta), SB(RecordDelta__clone));
}
void SB(RecordDeltas__drop)(groove_RecordDeltas *d) { jz_vec_drop(&d->deltas, sizeof(groove_RecordDelta), SB(RecordDelta__drop)); }
bool SB(RecordDeltas__eq)(const groove_RecordDeltas *a, const groove_RecordDeltas *b) {
    if (a->descriptor != b->descriptor || a->deltas.len != b->deltas.len) return false;
    for (u32 i = 0; i < a->deltas.len; i++)
        if (!SB(RecordDelta__eq)(&a->deltas.ptr[i], &b->deltas.ptr[i])) return false;
    return true;
}
bool SB(RecordDeltas__is_empty)(const groove_RecordDeltas *d) { return d->deltas.len == 0; }

/* Vec<(Vec<Value>, i64)> */
typedef struct groove_ValuesWeight { groove_ValueVec values; i64 weight; } groove_ValuesWeight;
static void values_weight_drop(void *p) { groove__records__values__ValueVec__drop(&((groove_ValuesWeight *)p)->values); }
jz_err SB(RecordDeltas__to_values)(const groove_RecordDeltas *d, void *out_v) {
    JZ_VEC(groove_ValuesWeight) *out = out_v;
    JZ_VEC_FOR(groove_RecordDelta, delta, d->deltas) {
        groove_BorrowedRecord record = groove_RecordDelta_borrowed(delta, d->descriptor);
        groove_ValuesWeight vw = { {0}, delta->weight };
        jz_err e = groove__records__BorrowedRecord__to_values(&record, &vw.values);
        if (JZ_IS_ERR(e)) { jz_vec_drop(out, sizeof(groove_ValuesWeight), values_weight_drop); memset(out, 0, sizeof *out); return e; }
        jz_vec_push(out, &vw, sizeof vw);
    }
    return JZ_OK;
}
void SB(RecordDeltas__arc_drop)(void *rc) { SB(RecordDeltas__drop)((groove_RecordDeltas *)rc); }
groove_RecordDeltas *SB(RecordDeltas__arc_new)(groove_RecordDeltas value) {
    groove_RecordDeltas *p = jz_rc_new(sizeof(groove_RecordDeltas), SB(RecordDeltas__arc_drop));
    *p = value;
    return p;
}
static void record_deltas_clone_v(void *dst, const void *src) { SB(RecordDeltas__clone)(dst, src); }
usize SB(record_deltas_encoded_bytes)(const groove_RecordDeltas *d) {
    usize total = 0;
    JZ_VEC_FOR(groove_RecordDelta, delta, d->deltas) total += delta->record.len;
    return total;
}

void SB(TableDelta__clone)(void *dst, const void *src) {
    const groove_TableDelta *s = src; groove_TableDelta *d = dst;
    d->table = jz_str_clone(&s->table); d->variant_tag = s->variant_tag; d->descriptor = s->descriptor;
    memset(&d->deltas, 0, sizeof d->deltas);
    jz_vec_clone(&d->deltas, &s->deltas, sizeof(groove_RecordDelta), SB(RecordDelta__clone));
}
void SB(TableDelta__drop)(void *p) {
    groove_TableDelta *d = p; jz_str_drop(&d->table);
    jz_vec_drop(&d->deltas, sizeof(groove_RecordDelta), SB(RecordDelta__drop));
}
void SB(BindingDelta__clone)(void *dst, const void *src) {
    const groove_BindingDelta *s = src; groove_BindingDelta *d = dst;
    groove__ivm__op_types__BindingSourceKey__clone(&d->key, &s->key);
    d->descriptor = s->descriptor; d->initializes_snapshot = s->initializes_snapshot;
    memset(&d->deltas, 0, sizeof d->deltas);
    jz_vec_clone(&d->deltas, &s->deltas, sizeof(groove_RecordDelta), SB(RecordDelta__clone));
}
void SB(BindingDelta__drop)(void *p) {
    groove_BindingDelta *d = p; groove__ivm__op_types__BindingSourceKey__drop(&d->key);
    jz_vec_drop(&d->deltas, sizeof(groove_RecordDelta), SB(RecordDelta__drop));
}
static void binding_delta_vec_drop(groove_BindingDeltaVec *v) { jz_vec_drop(v, sizeof(groove_BindingDelta), SB(BindingDelta__drop)); memset(v, 0, sizeof *v); }

/* ======================================================================
 * SubscriptionLifetime / RootIndirectValues / ids / errors
 * ==================================================================== */
void SB(SubscriptionId__retainer_key)(groove_SubscriptionId id, jz_str *out) {
    *out = jz_format("%llu", (unsigned long long)id.sequence);
}
void SB(PreparedShapeId__retainer_key)(groove_PreparedShapeId id, jz_str *out) {
    *out = jz_format("%llu", (unsigned long long)id.v);
}
void SB(PublicationId__debug)(groove_PublicationId id, jz_str *out) {
    jz_str_appendf(out, "PublicationId(%llu)", (unsigned long long)id.v);
}
groove_Retainer SB(SubscriptionLifetime__retainer)(groove_SubscriptionLifetime self, groove_SubscriptionId id) {
    groove_Retainer r;
    r.tag = self == GROOVE_SL_FIRST_RESULT ? GROOVE_RET_HYDRATION : GROOVE_RET_SUBSCRIPTION;
    SB(SubscriptionId__retainer_key)(id, &r.key);
    return r;
}
static groove_Retainer prepared_shape_retainer(groove_PreparedShapeId id) {
    groove_Retainer r; r.tag = GROOVE_RET_PREPARED_SHAPE;
    SB(PreparedShapeId__retainer_key)(id, &r.key);
    return r;
}

void SB(RootIndirectValues__clone)(groove_RootIndirectValues *dst, const groove_RootIndirectValues *src) {
    dst->tag = src->tag;
    dst->physical = src->physical ? jz_rc_clone(src->physical) : NULL;
}
void SB(RootIndirectValues__drop)(groove_RootIndirectValues *r) { if (r->physical) jz_rc_release(r->physical); r->physical = NULL; }
static bool str_unit_eq(const void *a, const void *b) { (void)a; (void)b; return true; }
bool SB(RootIndirectValues__eq)(const groove_RootIndirectValues *a, const groove_RootIndirectValues *b) {
    if (a->tag != b->tag) return false;
    if (a->tag != GROOVE_RIV_PHYSICAL_FIELDS) return true;
    return a->physical == b->physical || jz_btree_eq(a->physical, b->physical, str_unit_eq);
}
/* Top-level field indices that must be materialized, or `None` (false) for all. */
bool SB(RootIndirectValues__materialized_field_indices)(const groove_RootIndirectValues *self, groove_RecordDescriptor d, groove_UsizeVec *out) {
    switch (self->tag) {
    case GROOVE_RIV_MATERIALIZE: return false;
    case GROOVE_RIV_PHYSICAL: memset(out, 0, sizeof *out); return true;
    default: {
        memset(out, 0, sizeof *out);
        for (u32 i = 0; i < d->fields.len; i++) {
            const groove_DescriptorField *field = &d->fields.ptr[i];
            /* Match the storage name only: a public name can collide with another column's storage name. */
            if (field->has_name && jz_btree_contains(self->physical, &field->name)) continue;
            usize index = i; jz_vec_push(out, &index, sizeof index);
        }
        return true;
    }
    }
}

static void err_rc_drop(void *p) { jz_err_drop((jz_err *)p); }
groove_SubscriptionError SB(SubscriptionError__new)(jz_err *rc_error) {
    groove_SubscriptionError e = { GROOVE_SE_EVALUATION, rc_error };
    return e;
}
jz_err *groove_ivm_rt_a_arc_error(jz_err e) {
    jz_err *p = jz_rc_new(sizeof(jz_err), err_rc_drop);
    *p = e;
    return p;
}
const jz_err *SB(SubscriptionError__source_error)(const groove_SubscriptionError *e) {
    return e->tag == GROOVE_SE_EVALUATION ? e->error : NULL;
}
void SB(SubscriptionError__fmt)(const groove_SubscriptionError *e, jz_str *out) {
    if (e->tag == GROOVE_SE_EVALUATION) {
        jz_str_push_cstr(out, "subscription evaluation failed: ");
        if (e->error->msg) jz_str_push(out, e->error->msg->ptr, e->error->msg->len);
    } else {
        jz_str_push_cstr(out, "subscription ended");
    }
}
void SB(SubscriptionError__debug)(const groove_SubscriptionError *e, jz_str *out) {
    if (e->tag == GROOVE_SE_EVALUATION) {
        jz_str_push_cstr(out, "Evaluation(");
        if (e->error->msg) jz_str_push(out, e->error->msg->ptr, e->error->msg->len);
        jz_str_push_char(out, ')');
    } else {
        jz_str_push_cstr(out, "Ended");
    }
}
void SB(SubscriptionError__clone)(groove_SubscriptionError *dst, const groove_SubscriptionError *src) {
    dst->tag = src->tag; dst->error = src->error ? jz_rc_clone(src->error) : NULL;
}
void SB(SubscriptionError__drop)(groove_SubscriptionError *e) { if (e->error) jz_rc_release(e->error); e->error = NULL; }

/* ======================================================================
 * RoutedMultisinkTerminal
 * ==================================================================== */
void SB(RoutedMultisinkTerminal__new)(jz_str sink, groove_GraphBuilder graph, groove_StrVec route_fields, groove_StrVec public_fields, groove_RoutedMultisinkTerminal *out) {
    out->sink = sink; out->graph = graph; out->route_fields = route_fields; out->public_fields = public_fields;
    memset(&out->route_value_indices, 0, sizeof out->route_value_indices);
    for (usize i = 0; i < route_fields.len; i++) jz_vec_push(&out->route_value_indices, &i, sizeof i);
}
/* Select non-prefix binding values for the terminal's route predicates. */
void SB(RoutedMultisinkTerminal__with_route_value_indices)(groove_RoutedMultisinkTerminal *self, groove_UsizeVec indices) {
    jz_vec_drop(&self->route_value_indices, sizeof(usize), NULL);
    self->route_value_indices = indices;
}
static void str_vec_clone(groove_StrVec *dst, const groove_StrVec *src) {
    memset(dst, 0, sizeof *dst);
    jz_vec_clone(dst, src, sizeof(jz_str), groove_ivm_rt_a_clone_str);
}
static void str_vec_drop(groove_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), groove_ivm_rt_a_drop_str); memset(v, 0, sizeof *v); }
static bool str_vec_eq(const groove_StrVec *a, const groove_StrVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!jz_str_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static bool str_vec_contains(const groove_StrVec *v, const jz_str *s) {
    for (u32 i = 0; i < v->len; i++) if (jz_str_eq(&v->ptr[i], s)) return true;
    return false;
}
static u64 str_vec_hash(const groove_StrVec *v, u64 h) {
    h = jz_hash_combine(h, v->len);
    for (u32 i = 0; i < v->len; i++) h = jz_hash_combine(h, jz_str_hash(&v->ptr[i]));
    return h;
}
static void usize_vec_clone(groove_UsizeVec *dst, const groove_UsizeVec *src) {
    memset(dst, 0, sizeof *dst);
    jz_vec_clone(dst, src, sizeof(usize), NULL);
}
void SB(RoutedMultisinkTerminal__clone)(void *dst_v, const void *src_v) {
    groove_RoutedMultisinkTerminal *d = dst_v; const groove_RoutedMultisinkTerminal *s = src_v;
    d->sink = jz_str_clone(&s->sink);
    groove__ivm__graph__GraphBuilder__clone(&d->graph, &s->graph);
    str_vec_clone(&d->route_fields, &s->route_fields);
    usize_vec_clone(&d->route_value_indices, &s->route_value_indices);
    str_vec_clone(&d->public_fields, &s->public_fields);
}
void SB(RoutedMultisinkTerminal__drop)(void *p) {
    groove_RoutedMultisinkTerminal *t = p;
    jz_str_drop(&t->sink);
    groove__ivm__graph__GraphBuilder__drop(&t->graph);
    str_vec_drop(&t->route_fields);
    jz_vec_drop(&t->route_value_indices, sizeof(usize), NULL);
    str_vec_drop(&t->public_fields);
}
bool SB(RoutedMultisinkTerminal__eq)(const groove_RoutedMultisinkTerminal *a, const groove_RoutedMultisinkTerminal *b) {
    return jz_str_eq(&a->sink, &b->sink) && groove__ivm__graph__GraphBuilder__eq(&a->graph, &b->graph)
        && str_vec_eq(&a->route_fields, &b->route_fields)
        && a->route_value_indices.len == b->route_value_indices.len
        && (a->route_value_indices.len == 0 || memcmp(a->route_value_indices.ptr, b->route_value_indices.ptr, a->route_value_indices.len * sizeof(usize)) == 0)
        && str_vec_eq(&a->public_fields, &b->public_fields);
}
u64 SB(RoutedMultisinkTerminal__hash)(const groove_RoutedMultisinkTerminal *t, u64 h) {
    h = jz_hash_combine(h, jz_str_hash(&t->sink));
    h = groove__ivm__graph__GraphBuilder__hash(&t->graph, h);
    h = str_vec_hash(&t->route_fields, h);
    h = jz_hash_combine(h, jz_hash_bytes(t->route_value_indices.ptr, t->route_value_indices.len * sizeof(usize)));
    return str_vec_hash(&t->public_fields, h);
}

/* ======================================================================
 * MultisinkDeltas / QueuedMultisinkDeltas
 * ==================================================================== */
static void record_deltas_val_drop(void *p) { SB(RecordDeltas__drop)(p); }
static void terminal_deltas_val_drop(void *p) { groove__ivm__runtime__terminal__TerminalDeltas__drop(p); }
static void terminal_deltas_clone_v(void *d, const void *s) { groove__ivm__runtime__terminal__TerminalDeltas__clone(d, s); }
const jz_btree_desc groove_ivm_rt_a_desc_sink_record_deltas = { sizeof(jz_str), sizeof(groove_RecordDeltas), jz_cmp_str, groove_ivm_rt_a_drop_str, record_deltas_val_drop };
const jz_btree_desc groove_ivm_rt_a_desc_sink_terminal_deltas = { sizeof(jz_str), sizeof(groove_TerminalDeltas), jz_cmp_str, groove_ivm_rt_a_drop_str, terminal_deltas_val_drop };
const jz_btree_desc groove_ivm_rt_a_desc_sink_compiled_node = { sizeof(jz_str), sizeof(groove_CompiledNode), jz_cmp_str, groove_ivm_rt_a_drop_str, NULL };
static void terminal_state_drop(void *p) { SB(RoutedMultisinkTerminal__drop)(&((groove_RoutedMultisinkTerminalState *)p)->terminal); }
static void terminal_state_clone(void *d, const void *s) {
    groove_RoutedMultisinkTerminalState *dst = d; const groove_RoutedMultisinkTerminalState *src = s;
    SB(RoutedMultisinkTerminal__clone)(&dst->terminal, &src->terminal); dst->output = src->output;
}
const jz_btree_desc groove_ivm_rt_a_desc_sink_terminal_state = { sizeof(jz_str), sizeof(groove_RoutedMultisinkTerminalState), jz_cmp_str, groove_ivm_rt_a_drop_str, terminal_state_drop };

void SB(MultisinkDeltas__default)(groove_MultisinkDeltas *d) {
    jz_btree_init(&d->sinks, &groove_ivm_rt_a_desc_sink_record_deltas);
    jz_btree_init(&d->terminal_sinks, &groove_ivm_rt_a_desc_sink_terminal_deltas);
}
void SB(MultisinkDeltas__clone)(groove_MultisinkDeltas *dst, const groove_MultisinkDeltas *src) {
    jz_btree_clone(&dst->sinks, &src->sinks, groove_ivm_rt_a_clone_str, record_deltas_clone_v);
    jz_btree_clone(&dst->terminal_sinks, &src->terminal_sinks, groove_ivm_rt_a_clone_str, terminal_deltas_clone_v);
}
void SB(MultisinkDeltas__drop)(groove_MultisinkDeltas *d) { jz_btree_drop(&d->sinks); jz_btree_drop(&d->terminal_sinks); }
bool SB(MultisinkDeltas__is_empty)(const groove_MultisinkDeltas *d) {
    jz_str *k; groove_RecordDeltas *v;
    JZ_BTREE_FOR(&d->sinks, k, v) if (!SB(RecordDeltas__is_empty)(v)) return false;
    groove_TerminalDeltas *t;
    JZ_BTREE_FOR(&d->terminal_sinks, k, t) if (t->operations.len != 0) return false;
    return true;
}
const groove_RecordDeltas *SB(MultisinkDeltas__get)(const groove_MultisinkDeltas *d, const char *sink, u32 len) {
    jz_str key = str_borrow(sink, len);
    return jz_btree_get(&d->sinks, &key);
}
usize SB(multisink_deltas_record_count)(const groove_MultisinkDeltas *d) {
    usize total = 0; jz_str *k; groove_RecordDeltas *v;
    JZ_BTREE_FOR(&d->sinks, k, v) total += v->deltas.len;
    return total;
}
usize SB(multisink_deltas_encoded_bytes)(const groove_MultisinkDeltas *d) {
    usize total = 0; jz_str *k; groove_RecordDeltas *v;
    JZ_BTREE_FOR(&d->sinks, k, v) total += SB(record_deltas_encoded_bytes)(v);
    return total;
}
groove_QueuedMultisinkDeltas SB(QueuedMultisinkDeltas__new)(groove_MultisinkDeltas deltas) {
    groove_QueuedMultisinkDeltas q = { deltas, false, {0} };
    return q;
}
void SB(QueuedMultisinkDeltas__drop)(groove_QueuedMultisinkDeltas *q) { SB(MultisinkDeltas__drop)(&q->deltas); }
void SB(QueuedMultisinkDeltas__clone)(groove_QueuedMultisinkDeltas *dst, const groove_QueuedMultisinkDeltas *src) {
    SB(MultisinkDeltas__clone)(&dst->deltas, &src->deltas);
    dst->has_publication = src->has_publication; dst->publication = src->publication;
}

/* ======================================================================
 * channel cells, SubscriptionSender, MultisinkSubscription
 * ==================================================================== */
static void sender_cell_drop(void *p) { groove_SenderCell *c = p; if (c->some) jz_chan_release(c->chan); c->some = false; }
static void waiter_cell_drop(void *p) { (void)p; }
static void initial_cell_drop(void *p) { groove_InitialCell *c = p; if (c->some) SB(MultisinkDeltas__drop)(&c->value); c->some = false; }
static void liveness_cell_drop(void *p) { (void)p; }
groove_InitialCell *groove_ivm_rt_a_initial_cell_new(void) {
    groove_InitialCell *c = jz_rc_new(sizeof(groove_InitialCell), initial_cell_drop);
    c->some = false;
    return c;
}

/* mpsc::channel() + SubscriptionSender + liveness, as subscribe_staged and bind build them */
static void new_subscription_channel(groove_SubscriptionSender *sender, jz_chan **receiver, groove_WaiterCell **waiter, groove_LivenessCell **liveness) {
    jz_chan *chan = jz_chan_new(sizeof(groove_SubscriptionItem));
    groove_SenderCell *cell = jz_rc_new(sizeof(groove_SenderCell), sender_cell_drop);
    cell->some = true; cell->chan = chan;
    groove_WaiterCell *w = jz_rc_new(sizeof(groove_WaiterCell), waiter_cell_drop);
    w->some = false;
    sender->sender = cell;
    sender->waiter = jz_rc_clone(w);
    *receiver = chan;
    *waiter = w;
    groove_LivenessCell *l = jz_rc_new(sizeof(groove_LivenessCell), liveness_cell_drop);
    l->alive = true;
    *liveness = l;
}

static void wake_waiter(groove_WaiterCell *w) {
    if (w->some) {
        jz_waker waker = w->waker;
        w->some = false;
        jz_waker_wake(&waker);
    }
}

bool SB(SubscriptionSender__send)(const groove_SubscriptionSender *self, groove_QueuedMultisinkDeltas queued, groove_QueuedMultisinkDeltas *returned) {
    groove_SenderCell *sender = self->sender;
    if (!sender->some || jz_chan_is_closed(sender->chan)) {
        /* SendError(queued): the update is handed back to the caller */
        *returned = queued;
        return false;
    }
    groove_SubscriptionItem item; memset(&item, 0, sizeof item);
    item.ok = true; item.queued = queued;
    jz_chan_send(sender->chan, &item);
    wake_waiter(self->waiter);
    return true;
}
void SB(SubscriptionSender__fail)(const groove_SubscriptionSender *self, groove_SubscriptionError error) {
    groove_SenderCell *sender = self->sender;
    if (sender->some) {
        jz_chan *chan = sender->chan;
        sender->some = false;
        groove_SubscriptionItem item; memset(&item, 0, sizeof item);
        item.ok = false; item.error = error;
        if (!jz_chan_is_closed(chan)) jz_chan_send(chan, &item);
        else SB(SubscriptionError__drop)(&item.error);
        jz_chan_release(chan);
    } else {
        SB(SubscriptionError__drop)(&error);
    }
    wake_waiter(self->waiter);
}
void SB(SubscriptionSender__clone)(groove_SubscriptionSender *dst, const groove_SubscriptionSender *src) {
    dst->sender = jz_rc_clone(src->sender); dst->waiter = jz_rc_clone(src->waiter);
}
void SB(SubscriptionSender__drop)(groove_SubscriptionSender *s) { jz_rc_release(s->sender); jz_rc_release(s->waiter); }

void SB(MultisinkSubscription__drop)(groove_MultisinkSubscription *s) {
    jz_rc_release(s->initial);
    groove_SubscriptionItem item;
    while (jz_chan_try_recv(s->receiver, &item)) {
        if (item.ok) SB(QueuedMultisinkDeltas__drop)(&item.queued); else SB(SubscriptionError__drop)(&item.error);
    }
    jz_chan_release(s->receiver);
    jz_rc_release(s->waiter);
    /* dropping the last Arc<()> makes every Weak fail to upgrade */
    s->receiver_liveness->alive = false;
    jz_rc_release(s->receiver_liveness);
}
groove_SubscriptionId SB(MultisinkSubscription__id)(const groove_MultisinkSubscription *s) { return s->id; }

/* Take the complete multisink value captured when this terminal session opened. */
bool SB(MultisinkSubscription__take_initial)(const groove_MultisinkSubscription *s, groove_MultisinkDeltas *out) {
    if (!s->initial->some) return false;
    *out = s->initial->value;
    s->initial->some = false;
    return true;
}
static u32 receiver_try_recv(jz_chan *chan, groove_SubscriptionItem *item) {
    if (jz_chan_try_recv(chan, item)) return GROOVE_TRY_RECV_OK;
    return jz_chan_is_closed(chan) ? GROOVE_TRY_RECV_DISCONNECTED : GROOVE_TRY_RECV_EMPTY;
}
u32 SB(MultisinkSubscription__try_recv)(const groove_MultisinkSubscription *s, groove_MultisinkDeltas *out) {
    if (SB(MultisinkSubscription__take_initial)(s, out)) return GROOVE_TRY_RECV_OK;
    groove_SubscriptionItem item;
    u32 r = receiver_try_recv(s->receiver, &item);
    if (r != GROOVE_TRY_RECV_OK) return r;
    if (item.ok) { *out = item.queued.deltas; return GROOVE_TRY_RECV_OK; }
    SB(SubscriptionError__drop)(&item.error);
    return GROOVE_TRY_RECV_DISCONNECTED;
}
u32 SB(MultisinkSubscription__try_recv_with_publication)(const groove_MultisinkSubscription *s, groove_PublicationUpdate *out) {
    if (SB(MultisinkSubscription__take_initial)(s, &out->deltas)) { out->has_publication = false; return GROOVE_TRY_RECV_OK; }
    groove_SubscriptionItem item;
    u32 r = receiver_try_recv(s->receiver, &item);
    if (r != GROOVE_TRY_RECV_OK) return r;
    if (item.ok) {
        out->has_publication = item.queued.has_publication; out->publication = item.queued.publication;
        out->deltas = item.queued.deltas;
        return GROOVE_TRY_RECV_OK;
    }
    SB(SubscriptionError__drop)(&item.error);
    return GROOVE_TRY_RECV_DISCONNECTED;
}
static bool event_from_receiver(const groove_MultisinkSubscription *s, groove_SubscriptionEvent *out) {
    groove_SubscriptionItem item;
    switch (receiver_try_recv(s->receiver, &item)) {
    case GROOVE_TRY_RECV_OK:
        if (item.ok) {
            out->tag = GROOVE_SEV_UPDATE;
            out->update.has_publication = item.queued.has_publication; out->update.publication = item.queued.publication;
            out->update.deltas = item.queued.deltas;
        } else {
            out->tag = GROOVE_SEV_ERROR; out->error = item.error;
        }
        return true;
    case GROOVE_TRY_RECV_DISCONNECTED:
        out->tag = GROOVE_SEV_ERROR; out->error.tag = GROOVE_SE_ENDED; out->error.error = NULL;
        return true;
    default:
        return false;
    }
}
u32 SB(MultisinkSubscription__poll_next_event)(const groove_MultisinkSubscription *s, const jz_waker *cx, groove_SubscriptionEvent *out) {
    if (SB(MultisinkSubscription__take_initial)(s, &out->update.deltas)) {
        out->tag = GROOVE_SEV_UPDATE; out->update.has_publication = false;
        return 0;
    }
    if (event_from_receiver(s, out)) return 0;
    s->waiter->some = true; s->waiter->waker = *cx;
    if (event_from_receiver(s, out)) return 0;
    return GROOVE_POLL_PENDING;
}
u32 SB(MultisinkSubscription__poll_next_with_publication)(const groove_MultisinkSubscription *s, const jz_waker *cx, bool *ok, groove_PublicationUpdate *out) {
    groove_SubscriptionEvent event;
    u32 p = SB(MultisinkSubscription__poll_next_event)(s, cx, &event);
    if (p) return p;
    if (event.tag == GROOVE_SEV_UPDATE) { *ok = true; *out = event.update; }
    else { *ok = false; SB(SubscriptionError__drop)(&event.error); }
    return 0;
}

/* ======================================================================
 * impl PredicateExpr (matchers live in this module)
 * ==================================================================== */
static jz_err resolved_record_value(groove_PredicateRecord record, const jz_str *field, groove_Value *out) {
    return record.vt->value(record.self, field, out);
}
static bool ord_is_eq(int o) { return o == 0; }
static bool ord_is_ne(int o) { return o != 0; }
static bool ord_is_gt(int o) { return o > 0; }
static bool ord_is_ge(int o) { return o >= 0; }
static bool ord_is_lt(int o) { return o < 0; }
static bool ord_is_le(int o) { return o <= 0; }

bool SB(PredicateExpr__supports_indirect_literal_attempt)(const groove_PredicateExpr *p) {
    switch (p->tag) {
    case GROOVE_PRED_EQ: case GROOVE_PRED_NEQ: case GROOVE_PRED_GT: case GROOVE_PRED_GT_EQ: case GROOVE_PRED_LT: case GROOVE_PRED_LT_EQ: return true;
    default: return false;
    }
}
static void value_unwrap_nullable(groove_Value *v) {
    if (v->tag == GROOVE_V_NULLABLE && v->v.nullable) {
        groove_Value *inner = v->v.nullable;
        *v = *inner;
        jz_free(inner);
    }
}
jz_err SB(PredicateExpr__matches_indirect_literal_attempt)(const groove_PredicateExpr *p, groove_BorrowedRecord record, groove_EvaluationInputs *inputs, i8 *out) {
    groove_OrdPred predicate;
    switch (p->tag) {
    case GROOVE_PRED_EQ: predicate = ord_is_eq; break;
    case GROOVE_PRED_NEQ: predicate = ord_is_ne; break;
    case GROOVE_PRED_GT: predicate = ord_is_gt; break;
    case GROOVE_PRED_GT_EQ: predicate = ord_is_ge; break;
    case GROOVE_PRED_LT: predicate = ord_is_lt; break;
    case GROOVE_PRED_LT_EQ: predicate = ord_is_le; break;
    default: *out = -1; return JZ_OK;
    }
    groove_PredicateRecord pr = { &groove__ivm__runtime__key_encoding__BorrowedRecord__PredicateRecord__VT, &record };
    groove_Value actual;
    JZ_TRY(resolved_record_value(pr, &p->field, &actual));
    value_unwrap_nullable(&actual);
    if (actual.tag != GROOVE_V_LARGE) { groove__records__values__Value__drop(&actual); *out = -1; return JZ_OK; }
    groove_Value literal;
    groove__ivm__op_types__LiteralValue__to_value(&p->value, &literal);
    value_unwrap_nullable(&literal);
    u32 kind = groove__large_values__LargeValueRef__kind(actual.v.large);
    const u8 *inline_ptr; u32 inline_len;
    if (kind == GROOVE_LVK_BYTES && literal.tag == GROOVE_V_BYTES) {
        inline_ptr = literal.v.bytes.ptr; inline_len = literal.v.bytes.len;
    } else if ((kind == GROOVE_LVK_STRING || kind == GROOVE_LVK_JSON) && literal.tag == GROOVE_V_STRING) {
        inline_ptr = (const u8 *)literal.v.s.ptr; inline_len = literal.v.s.len;
    } else {
        groove__records__values__Value__drop(&literal); groove__records__values__Value__drop(&actual);
        *out = 0; return JZ_OK;
    }
    int ordering = 0;
    jz_err e = groove__large_values__compare_inline_attempt(actual.v.large, inline_ptr, inline_len, inputs, &ordering);
    groove__records__values__Value__drop(&literal); groove__records__values__Value__drop(&actual);
    if (JZ_IS_ERR(e)) return e;
    *out = predicate(ordering) ? 1 : 0;
    return JZ_OK;
}

static void str_set_insert(jz_btree *set, const jz_str *s) {
    if (!jz_btree_contains(set, s)) { jz_str c = jz_str_clone(s); jz_btree_insert(set, &c, NULL, NULL); }
}
void SB(PredicateExpr__referenced_fields)(const groove_PredicateExpr *p, jz_btree *output) {
    switch (p->tag) {
    case GROOVE_PRED_TEMPLATE_ARGUMENT:
        for (u32 i = 0; i < p->fields.len; i++) str_set_insert(output, &p->fields.ptr[i]);
        break;
    case GROOVE_PRED_EQ: case GROOVE_PRED_NEQ: case GROOVE_PRED_CONTAINS: case GROOVE_PRED_GT: case GROOVE_PRED_GT_EQ:
    case GROOVE_PRED_LT: case GROOVE_PRED_LT_EQ: case GROOVE_PRED_IS_NULL: case GROOVE_PRED_IS_NOT_NULL: case GROOVE_PRED_ENUM_MATCH:
        str_set_insert(output, &p->field);
        break;
    case GROOVE_PRED_EQ_FIELD: case GROOVE_PRED_NEQ_FIELD: case GROOVE_PRED_CONTAINS_FIELD:
        str_set_insert(output, &p->field);
        str_set_insert(output, &p->other_field);
        break;
    case GROOVE_PRED_AND: case GROOVE_PRED_OR:
        for (u32 i = 0; i < p->list.len; i++) SB(PredicateExpr__referenced_fields)(&p->list.ptr[i], output);
        break;
    }
}

jz_err SB(PredicateExpr__matches)(const groove_PredicateExpr *p, groove_PredicateRecord record, u32 comparison, bool *out) {
    switch (p->tag) {
    case GROOVE_PRED_TEMPLATE_ARGUMENT: return groove_err_unsupported_operator();
    case GROOVE_PRED_EQ: return groove__ivm__runtime__key_encoding__compare_record_field(record, &p->field, &p->value, ord_is_eq, comparison, out);
    case GROOVE_PRED_NEQ: return groove__ivm__runtime__key_encoding__compare_record_field(record, &p->field, &p->value, ord_is_ne, comparison, out);
    case GROOVE_PRED_CONTAINS: return groove__ivm__runtime__key_encoding__contains_record_field(record, &p->field, &p->value, comparison, out);
    case GROOVE_PRED_EQ_FIELD: return groove__ivm__runtime__key_encoding__compare_record_fields(record, &p->field, &p->other_field, ord_is_eq, comparison, out);
    case GROOVE_PRED_CONTAINS_FIELD: return groove__ivm__runtime__key_encoding__contains_record_field_value(record, &p->field, &p->other_field, comparison, out);
    case GROOVE_PRED_NEQ_FIELD: return groove__ivm__runtime__key_encoding__compare_record_fields(record, &p->field, &p->other_field, ord_is_ne, comparison, out);
    case GROOVE_PRED_GT: return groove__ivm__runtime__key_encoding__compare_record_field(record, &p->field, &p->value, ord_is_gt, comparison, out);
    case GROOVE_PRED_GT_EQ: return groove__ivm__runtime__key_encoding__compare_record_field(record, &p->field, &p->value, ord_is_ge, comparison, out);
    case GROOVE_PRED_LT: return groove__ivm__runtime__key_encoding__compare_record_field(record, &p->field, &p->value, ord_is_lt, comparison, out);
    case GROOVE_PRED_LT_EQ: return groove__ivm__runtime__key_encoding__compare_record_field(record, &p->field, &p->value, ord_is_le, comparison, out);
    case GROOVE_PRED_IS_NULL:
    case GROOVE_PRED_IS_NOT_NULL: {
        groove_Value v;
        JZ_TRY(resolved_record_value(record, &p->field, &v));
        bool is_null = groove__ivm__runtime__key_encoding__is_sql_null_value(&v);
        groove__records__values__Value__drop(&v);
        *out = p->tag == GROOVE_PRED_IS_NULL ? is_null : !is_null;
        return JZ_OK;
    }
    case GROOVE_PRED_ENUM_MATCH: {
        groove_Value v;
        JZ_TRY(resolved_record_value(record, &p->field, &v));
        value_unwrap_nullable(&v);
        jz_err e = JZ_OK;
        if (v.tag == GROOVE_V_ENUM && v.v.enm.tag == p->case_tag) {
            groove_BorrowedRecord payload = { v.v.enm.record.raw.ptr, v.v.enm.record.raw.len, v.v.enm.record.descriptor };
            groove_PredicateRecord pr = { &groove__ivm__runtime__key_encoding__BorrowedRecord__PredicateRecord__VT, &payload };
            e = SB(PredicateExpr__matches)(p->payload, pr, comparison, out);
        } else {
            /* Wrong arms and NULL never match (fail-closed). */
            *out = false;
        }
        groove__records__values__Value__drop(&v);
        return e;
    }
    case GROOVE_PRED_AND:
    case GROOVE_PRED_OR: {
        bool acc = p->tag == GROOVE_PRED_AND;
        for (u32 i = 0; i < p->list.len; i++) {
            bool m;
            JZ_TRY(SB(PredicateExpr__matches)(&p->list.ptr[i], record, comparison, &m));
            acc = p->tag == GROOVE_PRED_AND ? (acc && m) : (acc || m);
        }
        *out = acc;
        return JZ_OK;
    }
    }
    return groove_err_unsupported_operator();
}
jz_err SB(PredicateExpr__matches_borrowed)(const groove_PredicateExpr *p, groove_BorrowedRecord record, u32 comparison, bool *out) {
    groove_PredicateRecord pr = { &groove__ivm__runtime__key_encoding__BorrowedRecord__PredicateRecord__VT, &record };
    return SB(PredicateExpr__matches)(p, pr, comparison, out);
}

/* ======================================================================
 * subscription / shape state: clone + drop
 * ==================================================================== */
static void target_clone(groove_MultisinkSubscriptionTarget *d, const groove_MultisinkSubscriptionTarget *s) {
    d->tag = s->tag; d->shape_id = s->shape_id;
    groove_ivm_rt_a_clone_bytes(&d->binding_key.bytes, &s->binding_key.bytes);
    memset(&d->route_barriers, 0, sizeof d->route_barriers);
    jz_vec_clone(&d->route_barriers, &s->route_barriers, sizeof(groove_NodeId), NULL);
}
static void target_drop(groove_MultisinkSubscriptionTarget *t) {
    groove_ivm_rt_a_drop_bytes(&t->binding_key.bytes);
    jz_vec_drop(&t->route_barriers, sizeof(groove_NodeId), NULL);
}
void SB(MultisinkSubscriptionState__drop)(void *p) {
    groove_MultisinkSubscriptionState *s = p;
    SB(SubscriptionSender__drop)(&s->sender);
    jz_rc_release(s->receiver_liveness);
    jz_btree_drop(&s->outputs);
    target_drop(&s->target);
    SB(RootIndirectValues__drop)(&s->root_indirect_values);
}
void SB(MultisinkSubscriptionState__clone)(void *dst, const void *src) {
    groove_MultisinkSubscriptionState *d = dst; const groove_MultisinkSubscriptionState *s = src;
    SB(SubscriptionSender__clone)(&d->sender, &s->sender);
    d->receiver_liveness = jz_rc_clone(s->receiver_liveness);
    jz_btree_clone(&d->outputs, &s->outputs, groove_ivm_rt_a_clone_str, NULL);
    target_clone(&d->target, &s->target);
    d->failed = s->failed;
    SB(RootIndirectValues__clone)(&d->root_indirect_values, &s->root_indirect_values);
}

void SB(AutoDirectFamilyKey__clone)(void *dst, const void *src) {
    groove_AutoDirectFamilyKey *d = dst; const groove_AutoDirectFamilyKey *s = src;
    groove__ivm__graph__GraphBuilder__clone(&d->graph, &s->graph);
    d->binding_descriptor = s->binding_descriptor;
    d->binding_field = jz_str_clone(&s->binding_field);
    str_vec_clone(&d->public_fields, &s->public_fields);
}
void SB(AutoDirectFamilyKey__drop)(void *p) {
    groove_AutoDirectFamilyKey *k = p;
    groove__ivm__graph__GraphBuilder__drop(&k->graph);
    jz_str_drop(&k->binding_field);
    str_vec_drop(&k->public_fields);
}
bool SB(AutoDirectFamilyKey__eq)(const void *pa, const void *pb) {
    const groove_AutoDirectFamilyKey *a = pa, *b = pb;
    return a->binding_descriptor == b->binding_descriptor
        && jz_str_eq(&a->binding_field, &b->binding_field)
        && str_vec_eq(&a->public_fields, &b->public_fields)
        && SB(graph_builders_equal)(&a->graph, &b->graph);
}
u64 SB(AutoDirectFamilyKey__hash)(const void *p) {
    const groove_AutoDirectFamilyKey *k = p;
    u64 h = jz_hash_u64(SB(graph_builder_fingerprint)(&k->graph));
    h = jz_hash_combine(h, groove__records__RecordDescriptor__hash(k->binding_descriptor));
    h = jz_hash_combine(h, jz_str_hash(&k->binding_field));
    return str_vec_hash(&k->public_fields, h);
}
void SB(AutoDirectFamilyKey__debug)(const groove_AutoDirectFamilyKey *k, jz_str *out) {
    jz_str_appendf(out, "AutoDirectFamilyKey { graph_fingerprint: %llu, binding_descriptor: ", (unsigned long long)SB(graph_builder_fingerprint)(&k->graph));
    groove__records__RecordDescriptor__debug(out, k->binding_descriptor);
    jz_str_appendf(out, ", binding_field: \"%.*s\", public_fields: [", (int)k->binding_field.len, k->binding_field.ptr);
    for (u32 i = 0; i < k->public_fields.len; i++)
        jz_str_appendf(out, "%s\"%.*s\"", i ? ", " : "", (int)k->public_fields.ptr[i].len, k->public_fields.ptr[i].ptr);
    jz_str_push_cstr(out, "] }");
}

void SB(SharedShapeKey__clone)(void *dst, const void *src) {
    groove_SharedShapeKey *d = dst; const groove_SharedShapeKey *s = src;
    d->shape = jz_str_clone(&s->shape);
    memset(&d->terminals, 0, sizeof d->terminals);
    jz_vec_clone(&d->terminals, &s->terminals, sizeof(groove_RoutedMultisinkTerminal), SB(RoutedMultisinkTerminal__clone));
}
void SB(SharedShapeKey__drop)(void *p) {
    groove_SharedShapeKey *k = p;
    jz_str_drop(&k->shape);
    jz_vec_drop(&k->terminals, sizeof(groove_RoutedMultisinkTerminal), SB(RoutedMultisinkTerminal__drop));
}
bool SB(SharedShapeKey__eq)(const void *pa, const void *pb) {
    const groove_SharedShapeKey *a = pa, *b = pb;
    if (!jz_str_eq(&a->shape, &b->shape) || a->terminals.len != b->terminals.len) return false;
    for (u32 i = 0; i < a->terminals.len; i++)
        if (!SB(RoutedMultisinkTerminal__eq)(&a->terminals.ptr[i], &b->terminals.ptr[i])) return false;
    return true;
}
u64 SB(SharedShapeKey__hash)(const void *p) {
    const groove_SharedShapeKey *k = p;
    u64 h = jz_str_hash(&k->shape);
    h = jz_hash_combine(h, k->terminals.len);
    for (u32 i = 0; i < k->terminals.len; i++) h = SB(RoutedMultisinkTerminal__hash)(&k->terminals.ptr[i], h);
    return h;
}

void SB(RoutedMultisinkShapeState__clone)(void *dst, const void *src) {
    groove_RoutedMultisinkShapeState *d = dst; const groove_RoutedMultisinkShapeState *s = src;
    d->shape = jz_str_clone(&s->shape);
    d->binding_descriptor = s->binding_descriptor;
    jz_btree_clone(&d->terminals, &s->terminals, groove_ivm_rt_a_clone_str, terminal_state_clone);
    d->has_auto_family_key = s->has_auto_family_key;
    if (s->has_auto_family_key) SB(AutoDirectFamilyKey__clone)(&d->auto_family_key, &s->auto_family_key);
    d->has_shared_key = s->has_shared_key;
    if (s->has_shared_key) SB(SharedShapeKey__clone)(&d->shared_key, &s->shared_key);
    d->retire_when_unbound = s->retire_when_unbound;
}
void SB(RoutedMultisinkShapeState__drop)(void *p) {
    groove_RoutedMultisinkShapeState *s = p;
    jz_str_drop(&s->shape);
    jz_btree_drop(&s->terminals);
    if (s->has_auto_family_key) SB(AutoDirectFamilyKey__drop)(&s->auto_family_key);
    if (s->has_shared_key) SB(SharedShapeKey__drop)(&s->shared_key);
}
void SB(LiveAttach__drop)(groove_LiveAttach *l) {
    groove__ivm__op_types__BindingSourceKey__drop(&l->binding_source);
    groove_ivm_rt_a_drop_bytes(&l->binding_key.bytes);
    jz_hmap_drop(&l->borrowed);
}
void SB(BindingSourceState__drop)(void *p) { jz_hmap_drop(&((groove_BindingSourceState *)p)->refcounts); }
static void input_source_delta_drop(groove_InputSourceDelta *d) {
    jz_vec_drop(&d->adds, sizeof(jz_bytes), groove_ivm_rt_a_drop_bytes);
    jz_vec_drop(&d->removes, sizeof(jz_bytes), groove_ivm_rt_a_drop_bytes);
}

/* ======================================================================
 * graph_builder_fingerprint / graph_builders_equal(_with)
 * ==================================================================== */
static u64 h_str(u64 h, const jz_str *s) { return jz_hash_combine(h, jz_str_hash(s)); }
static u64 h_opt_str(u64 h, bool some, const jz_str *s) { h = jz_hash_combine(h, some); return some ? h_str(h, s) : h; }
static u64 h_desc(u64 h, groove_RecordDescriptor d) { return jz_hash_combine(h, groove__records__RecordDescriptor__hash(d)); }
static u64 h_opt_scan(u64 h, bool some, const groove_StaticScanSpec *s) {
    h = jz_hash_combine(h, some);
    return some ? groove__ivm__op_types__StaticScanSpec__hash(s, h) : h;
}
static u64 h_field_refs(u64 h, const groove_FieldRef *f, u32 n) {
    h = jz_hash_combine(h, n);
    for (u32 i = 0; i < n; i++) h = groove__ivm__graph__FieldRef__hash(&f[i], h);
    return h;
}
static u64 h_child(const jz_hmap *hashes, const groove_GraphBuilder *child) {
    const u64 *v = jz_hmap_get(hashes, &child);
    if (!v) jz_panic("postorder visits graph children before their parent");
    return *v;
}

/* Bounded structural fingerprint for auto-direct family lookup. Hash-map
 * collisions are resolved by graph_builders_equal. */
u64 SB(graph_builder_fingerprint)(const groove_GraphBuilder *graph) {
    jz_hmap hashes; jz_hmap_init(&hashes, &hdesc_ptr_u64);
    groove_GraphBuilderRefVec order = {0};
    groove__ivm__graph__GraphBuilder__postorder(graph, &order);
    for (u32 oi = 0; oi < order.len; oi++) {
        const groove_GraphBuilder *node = order.ptr[oi];
        if (jz_hmap_get(&hashes, &node)) continue;
        u64 h = jz_hash_u64(node->tag);
        switch (node->tag) {
        case GROOVE_GB_TYPED_TEMPLATE:
            h = groove__ivm__template__TypedGraphTemplate__hash(node->program, h);
            h = jz_hash_combine(h, node->predicates.len);
            for (u32 i = 0; i < node->predicates.len; i++) h = groove__ivm__op_types__PredicateExpr__hash(&node->predicates.ptr[i], h);
            h = groove__ivm__template__TemplateScalars__hash(node->scalars, node->scalars_len, h);
            for (u32 i = 0; i < node->inputs.len; i++) h = jz_hash_combine(h, h_child(&hashes, node->inputs.ptr[i]));
            break;
        case GROOVE_GB_TEMPLATE_INPUT:
            h = jz_hash_combine(h, node->slot);
            h = h_desc(h, node->output);
            h = jz_hash_combine(h, node->input != NULL);
            if (node->input) h = jz_hash_combine(h, h_child(&hashes, node->input));
            break;
        case GROOVE_GB_TABLE:
            h = h_str(h, &node->table);
            h = h_opt_scan(h, node->has_scan, &node->scan);
            h = h_opt_str(h, node->has_variant_projection, &node->variant_projection);
            break;
        case GROOVE_GB_INLINE_RECORDS:
            h = h_desc(h, node->output);
            h = jz_hash_combine(h, node->records.len);
            for (u32 i = 0; i < node->records.len; i++) h = jz_hash_combine(h, jz_hash_bytes(node->records.ptr[i].ptr, node->records.ptr[i].len));
            break;
        case GROOVE_GB_INPUT_SOURCE:
            h = jz_hash_combine(jz_hash_combine(h, node->id.runtime_namespace), node->id.local);
            h = h_desc(h, node->output);
            break;
        case GROOVE_GB_INDEX:
            h = h_str(h, &node->table);
            h = h_str(h, &node->index);
            h = h_opt_scan(h, node->has_scan, &node->scan);
            h = groove__ivm__graph__IndexIntersections__hash(&node->intersections, h);
            h = groove__ivm__graph__IndexCandidateFilter__hash(node->candidate_filter, h);
            h = h_opt_str(h, node->has_variant_projection, &node->variant_projection);
            break;
        case GROOVE_GB_FRONTIER_SOURCE:
            h = groove_hash_FrontierName(&node->binding, h);
            h = h_desc(h, node->output);
            break;
        case GROOVE_GB_BINDING_SOURCE:
            h = h_str(h, &node->shape);
            h = h_desc(h, node->output);
            break;
        case GROOVE_GB_RECURSIVE:
            h = jz_hash_combine(h, h_child(&hashes, node->seed));
            h = jz_hash_combine(h, h_child(&hashes, node->step));
            h = jz_hash_combine(h, node->step_witness != NULL);
            if (node->step_witness) h = jz_hash_combine(h, h_child(&hashes, node->step_witness));
            h = groove_hash_FrontierName(&node->frontier, h);
            h = jz_hash_combine(h, node->max_iters);
            h = jz_hash_combine(h, node->truncate_at_max_iters);
            break;
        case GROOVE_GB_RECURSIVE_STEP_WITNESS:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            break;
        case GROOVE_GB_LOOKUP:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = h_field_refs(h, node->keys_on.ptr, node->keys_on.len);
            h = h_str(h, &node->table);
            h = h_opt_scan(h, node->has_scan, &node->scan);
            h = h_opt_str(h, node->has_variant_projection, &node->variant_projection);
            break;
        case GROOVE_GB_FILTER:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = groove__ivm__op_types__PredicateExpr__hash(&node->predicate, h);
            h = jz_hash_combine(h, node->comparison);
            break;
        case GROOVE_GB_UNWRAP_NULLABLE:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = groove__ivm__graph__FieldRef__hash(&node->field, h);
            break;
        case GROOVE_GB_UNNEST:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = groove__ivm__graph__FieldRef__hash(&node->field, h);
            h = h_str(h, &node->element_field);
            break;
        case GROOVE_GB_VARIANT_PROJECT:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = groove__ivm__graph__FieldRef__hash(&node->field, h);
            h = h_str(h, &node->case_name);
            break;
        case GROOVE_GB_PROJECT:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = jz_hash_combine(h, node->fields.len);
            for (u32 i = 0; i < node->fields.len; i++) h = groove__ivm__graph__ProjectField__hash(&node->fields.ptr[i], h);
            break;
        case GROOVE_GB_STREAMING_CHECKSUM:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = groove__ivm__graph__FieldRef__hash(&node->field, h);
            h = h_str(h, &node->output_field);
            h = jz_hash_combine(h, node->window_bytes);
            h = jz_hash_combine(h, node->max_bytes_per_turn);
            break;
        case GROOVE_GB_UNION:
            for (u32 i = 0; i < node->inputs.len; i++) h = jz_hash_combine(h, h_child(&hashes, node->inputs.ptr[i]));
            break;
        case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
            h = jz_hash_combine(h, h_child(&hashes, node->left));
            h = jz_hash_combine(h, h_child(&hashes, node->right));
            h = h_field_refs(h, node->left_on.ptr, node->left_on.len);
            h = h_field_refs(h, node->right_on.ptr, node->right_on.len);
            h = jz_hash_combine(h, node->comparison);
            break;
        case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = h_field_refs(h, node->group_cols.ptr, node->group_cols.len);
            h = h_field_refs(h, node->order_cols.ptr, node->order_cols.len);
            break;
        case GROOVE_GB_TOP_BY:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = h_field_refs(h, node->group_cols.ptr, node->group_cols.len);
            h = jz_hash_combine(h, node->top_order_cols.len);
            for (u32 i = 0; i < node->top_order_cols.len; i++) h = groove__ivm__graph__TopByOrder__hash(&node->top_order_cols.ptr[i], h);
            h = h_field_refs(h, node->tie_cols_ref.ptr, node->tie_cols_ref.len);
            h = jz_hash_combine(h, node->offset);
            h = jz_hash_combine(jz_hash_combine(h, node->limit.tag), node->limit.n);
            break;
        case GROOVE_GB_COLLECT_BY:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = groove__ivm__graph__CollectByBuilder__hash(node->collect, h);
            break;
        case GROOVE_GB_AGGREGATE:
            h = jz_hash_combine(h, h_child(&hashes, node->input));
            h = h_field_refs(h, node->group_cols.ptr, node->group_cols.len);
            h = jz_hash_combine(h, node->aggregates.len);
            for (u32 i = 0; i < node->aggregates.len; i++) h = groove__ivm__graph__AggregateExpr__hash(&node->aggregates.ptr[i], h);
            break;
        }
        jz_hmap_insert(&hashes, &node, &h, NULL);
    }
    u64 result = h_child(&hashes, graph);
    jz_vec_drop(&order, sizeof(const groove_GraphBuilder *), NULL);
    jz_hmap_drop(&hashes);
    return result;
}

static bool eq_field_refs(const groove_FieldRef *a, u32 na, const groove_FieldRef *b, u32 nb) {
    if (na != nb) return false;
    for (u32 i = 0; i < na; i++) if (!groove__ivm__graph__FieldRef__eq(&a[i], &b[i])) return false;
    return true;
}
static bool eq_opt_str(bool sa, const jz_str *a, bool sb, const jz_str *b) { return sa == sb && (!sa || jz_str_eq(a, b)); }
static bool eq_opt_scan(bool sa, const groove_StaticScanSpec *a, bool sb, const groove_StaticScanSpec *b) {
    return sa == sb && (!sa || groove__ivm__op_types__StaticScanSpec__eq(a, b));
}
static bool eq_predicates(const groove_GraphBuilder *a, const groove_GraphBuilder *b) {
    if (a->predicates.len != b->predicates.len) return false;
    for (u32 i = 0; i < a->predicates.len; i++)
        if (!groove__ivm__op_types__PredicateExpr__eq(&a->predicates.ptr[i], &b->predicates.ptr[i])) return false;
    return true;
}
static bool eq_records(const groove_GraphBuilder *a, const groove_GraphBuilder *b) {
    if (a->records.len != b->records.len) return false;
    for (u32 i = 0; i < a->records.len; i++) if (!groove_ivm_rt_a_eq_bytes(&a->records.ptr[i], &b->records.ptr[i])) return false;
    return true;
}
static bool eq_project_fields(const groove_GraphBuilder *a, const groove_GraphBuilder *b) {
    if (a->fields.len != b->fields.len) return false;
    for (u32 i = 0; i < a->fields.len; i++) if (!groove__ivm__graph__ProjectField__eq(&a->fields.ptr[i], &b->fields.ptr[i])) return false;
    return true;
}
static bool eq_top_order(const groove_GraphBuilder *a, const groove_GraphBuilder *b) {
    if (a->top_order_cols.len != b->top_order_cols.len) return false;
    for (u32 i = 0; i < a->top_order_cols.len; i++) if (!groove__ivm__graph__TopByOrder__eq(&a->top_order_cols.ptr[i], &b->top_order_cols.ptr[i])) return false;
    return true;
}
static bool eq_aggregates(const groove_GraphBuilder *a, const groove_GraphBuilder *b) {
    if (a->aggregates.len != b->aggregates.len) return false;
    for (u32 i = 0; i < a->aggregates.len; i++) if (!groove__ivm__graph__AggregateExpr__eq(&a->aggregates.ptr[i], &b->aggregates.ptr[i])) return false;
    return true;
}

typedef struct GbPair { const groove_GraphBuilder *a, *b; } GbPair;
typedef JZ_VEC(GbPair) GbPairVec;
static void push_pair(GbPairVec *v, const groove_GraphBuilder *a, const groove_GraphBuilder *b) { GbPair p = { a, b }; jz_vec_push(v, &p, sizeof p); }

/* Structural equality with a caller hook consulted before each pair. */
bool SB(graph_builders_equal_with)(const groove_GraphBuilder *left, const groove_GraphBuilder *right, groove_GraphBuilderEqHook hook) {
    GbPairVec pending = {0};
    push_pair(&pending, left, right);
    bool equal = true;
    while (pending.len) {
        GbPair p = pending.ptr[--pending.len];
        const groove_GraphBuilder *l = p.a, *r = p.b;
        if (hook.call) {
            i8 decided = hook.call(hook.ctx, l, r);
            if (decided >= 0) {
                if (!decided) { equal = false; break; }
                continue;
            }
        }
        if (l->tag != r->tag) { equal = false; break; }
        bool ok = true;
        switch (l->tag) {
        case GROOVE_GB_TYPED_TEMPLATE:
            ok = groove__ivm__template__TypedGraphTemplate__eq(l->program, r->program) && l->inputs.len == r->inputs.len
                && eq_predicates(l, r) && groove__ivm__template__TemplateScalars__eq(l->scalars, l->scalars_len, r->scalars, r->scalars_len);
            if (ok) for (u32 i = 0; i < l->inputs.len; i++) push_pair(&pending, l->inputs.ptr[i], r->inputs.ptr[i]);
            break;
        case GROOVE_GB_TEMPLATE_INPUT:
            ok = l->slot == r->slot && l->output == r->output;
            if (ok) {
                if (l->input && r->input) push_pair(&pending, l->input, r->input);
                else if (l->input || r->input) ok = false;
            }
            break;
        case GROOVE_GB_TABLE:
            ok = jz_str_eq(&l->table, &r->table) && eq_opt_scan(l->has_scan, &l->scan, r->has_scan, &r->scan)
                && eq_opt_str(l->has_variant_projection, &l->variant_projection, r->has_variant_projection, &r->variant_projection);
            break;
        case GROOVE_GB_INLINE_RECORDS:
            ok = l->output == r->output && eq_records(l, r);
            break;
        case GROOVE_GB_INPUT_SOURCE:
            ok = l->id.runtime_namespace == r->id.runtime_namespace && l->id.local == r->id.local && l->output == r->output;
            break;
        case GROOVE_GB_INDEX:
            ok = jz_str_eq(&l->table, &r->table) && jz_str_eq(&l->index, &r->index)
                && eq_opt_scan(l->has_scan, &l->scan, r->has_scan, &r->scan)
                && groove__ivm__graph__IndexIntersections__eq(&l->intersections, &r->intersections)
                && groove__ivm__graph__IndexCandidateFilter__eq(l->candidate_filter, r->candidate_filter)
                && eq_opt_str(l->has_variant_projection, &l->variant_projection, r->has_variant_projection, &r->variant_projection);
            break;
        case GROOVE_GB_FRONTIER_SOURCE:
            ok = jz_str_eq(&l->binding.name, &r->binding.name) && l->output == r->output;
            break;
        case GROOVE_GB_BINDING_SOURCE:
            ok = jz_str_eq(&l->shape, &r->shape) && l->output == r->output;
            break;
        case GROOVE_GB_RECURSIVE:
            ok = jz_str_eq(&l->frontier.name, &r->frontier.name) && l->max_iters == r->max_iters
                && l->truncate_at_max_iters == r->truncate_at_max_iters && (l->step_witness != NULL) == (r->step_witness != NULL);
            if (ok) {
                push_pair(&pending, l->seed, r->seed);
                push_pair(&pending, l->step, r->step);
                if (l->step_witness && r->step_witness) push_pair(&pending, l->step_witness, r->step_witness);
            }
            break;
        case GROOVE_GB_RECURSIVE_STEP_WITNESS:
            push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_LOOKUP:
            ok = eq_field_refs(l->keys_on.ptr, l->keys_on.len, r->keys_on.ptr, r->keys_on.len) && jz_str_eq(&l->table, &r->table)
                && eq_opt_scan(l->has_scan, &l->scan, r->has_scan, &r->scan)
                && eq_opt_str(l->has_variant_projection, &l->variant_projection, r->has_variant_projection, &r->variant_projection);
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_FILTER:
            ok = groove__ivm__op_types__PredicateExpr__eq(&l->predicate, &r->predicate) && l->comparison == r->comparison;
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_UNWRAP_NULLABLE:
            ok = groove__ivm__graph__FieldRef__eq(&l->field, &r->field);
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_UNNEST:
            ok = groove__ivm__graph__FieldRef__eq(&l->field, &r->field) && jz_str_eq(&l->element_field, &r->element_field);
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_VARIANT_PROJECT:
            ok = groove__ivm__graph__FieldRef__eq(&l->field, &r->field) && jz_str_eq(&l->case_name, &r->case_name);
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_PROJECT:
            ok = eq_project_fields(l, r);
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_STREAMING_CHECKSUM:
            ok = groove__ivm__graph__FieldRef__eq(&l->field, &r->field) && jz_str_eq(&l->output_field, &r->output_field)
                && l->window_bytes == r->window_bytes && l->max_bytes_per_turn == r->max_bytes_per_turn;
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_UNION:
            ok = l->inputs.len == r->inputs.len;
            if (ok) for (u32 i = 0; i < l->inputs.len; i++) push_pair(&pending, l->inputs.ptr[i], r->inputs.ptr[i]);
            break;
        case GROOVE_GB_JOIN: case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
            ok = eq_field_refs(l->left_on.ptr, l->left_on.len, r->left_on.ptr, r->left_on.len)
                && eq_field_refs(l->right_on.ptr, l->right_on.len, r->right_on.ptr, r->right_on.len) && l->comparison == r->comparison;
            if (ok) { push_pair(&pending, l->left, r->left); push_pair(&pending, l->right, r->right); }
            break;
        case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY:
            ok = eq_field_refs(l->group_cols.ptr, l->group_cols.len, r->group_cols.ptr, r->group_cols.len)
                && eq_field_refs(l->order_cols.ptr, l->order_cols.len, r->order_cols.ptr, r->order_cols.len);
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_TOP_BY:
            ok = eq_field_refs(l->group_cols.ptr, l->group_cols.len, r->group_cols.ptr, r->group_cols.len) && eq_top_order(l, r)
                && eq_field_refs(l->tie_cols_ref.ptr, l->tie_cols_ref.len, r->tie_cols_ref.ptr, r->tie_cols_ref.len)
                && l->offset == r->offset && groove_TopByLimit_eq(l->limit, r->limit);
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_COLLECT_BY:
            ok = groove__ivm__graph__CollectByBuilder__eq(l->collect, r->collect);
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        case GROOVE_GB_AGGREGATE:
            ok = eq_field_refs(l->group_cols.ptr, l->group_cols.len, r->group_cols.ptr, r->group_cols.len) && eq_aggregates(l, r);
            if (ok) push_pair(&pending, l->input, r->input);
            break;
        default:
            ok = false;
        }
        if (!ok) { equal = false; break; }
    }
    jz_vec_drop(&pending, sizeof(GbPair), NULL);
    return equal;
}
/* Exact, nonrecursive equality check paired with the bounded family hash. */
bool SB(graph_builders_equal)(const groove_GraphBuilder *a, const groove_GraphBuilder *b) {
    groove_GraphBuilderEqHook none = { NULL, NULL };
    return SB(graph_builders_equal_with)(a, b, none);
}

/* ======================================================================
 * BindingSources
 * ==================================================================== */
static void snapshot_map_drop(void *p) { jz_hmap_drop((jz_hmap *)p); }
static void snapshot_map_clone(void *dst, const void *src) {
    jz_hmap *d = dst; const jz_hmap *s = src;
    jz_hmap_init(d, &groove_ivm_rt_a_hdesc_binding_snapshots);
    groove_BindingSourceKey *k; groove_RecordDeltas **v;
    JZ_HMAP_FOR(s, k, v) {
        groove_BindingSourceKey kc; groove__ivm__op_types__BindingSourceKey__clone(&kc, k);
        groove_RecordDeltas *vc = jz_rc_clone(*v);
        jz_hmap_insert(d, &kc, &vc, NULL);
    }
}
void SB(BindingSources__default)(groove_BindingSources *b) {
    jz_hmap_init(&b->states, &hdesc_binding_states);
    b->snapshot = jz_rc_new(sizeof(jz_hmap), snapshot_map_drop);
    jz_hmap_init(b->snapshot, &groove_ivm_rt_a_hdesc_binding_snapshots);
    jz_hmap_init(&b->dirty, &hdesc_binding_source_key_set);
}
static void mark_dirty(groove_BindingSources *b, const groove_BindingSourceKey *key) {
    if (jz_hmap_get(&b->dirty, key)) return;
    groove_BindingSourceKey c; groove__ivm__op_types__BindingSourceKey__clone(&c, key);
    jz_hmap_insert(&b->dirty, &c, NULL, NULL);
}
const groove_BindingSourceState *SB(BindingSources__get)(const groove_BindingSources *b, const groove_BindingSourceKey *key) {
    return jz_hmap_get(&b->states, key);
}
bool SB(BindingSources__contains_key)(const groove_BindingSources *b, const groove_BindingSourceKey *key) {
    return jz_hmap_get(&b->states, key) != NULL;
}
groove_BindingSourceState *SB(BindingSources__get_mut)(groove_BindingSources *b, const groove_BindingSourceKey *key) {
    groove_BindingSourceState *state = jz_hmap_get(&b->states, key);
    if (!state) return NULL;
    mark_dirty(b, key);
    return state;
}
groove_BindingSourceState *SB(BindingSources__entry)(groove_BindingSources *b, groove_BindingSourceKey key, bool *inserted) {
    mark_dirty(b, &key);
    groove_BindingSourceState *slot = jz_hmap_entry(&b->states, &key, inserted);
    if (!*inserted) groove__ivm__op_types__BindingSourceKey__drop(&key);
    return slot;
}
bool SB(BindingSources__remove)(groove_BindingSources *b, const groove_BindingSourceKey *key, groove_BindingSourceState *out) {
    groove_BindingSourceKey removed_key; groove_BindingSourceState removed;
    if (!jz_hmap_remove(&b->states, key, &removed_key, &removed)) return false;
    mark_dirty(b, &removed_key);
    groove__ivm__op_types__BindingSourceKey__drop(&removed_key);
    if (out) *out = removed; else SB(BindingSourceState__drop)(&removed);
    return true;
}
static groove_RecordDeltas source_snapshot(const groove_BindingSourceState *source) {
    groove_RecordDeltas r = groove_RecordDeltas_empty(source->descriptor);
    groove_BindingKey *binding; usize *count;
    JZ_HMAP_FOR(&source->refcounts, binding, count) {
        groove_RecordDelta d; groove_ivm_rt_a_clone_bytes(&d.record, &binding->bytes); d.weight = 1;
        jz_vec_push(&r.deltas, &d, sizeof d);
    }
    return r;
}
/* Current bindings of every source, as positive-weight record deltas. */
groove_BindingSnapshots *SB(BindingSources__snapshot)(groove_BindingSources *b) {
    if (b->dirty.len) {
        jz_hmap *snapshot = jz_rc_make_mut((void **)&b->snapshot, sizeof(jz_hmap), snapshot_map_clone);
        groove_BindingSourceKey *key; void *unit;
        JZ_HMAP_FOR(&b->dirty, key, unit) {
            const groove_BindingSourceState *source = jz_hmap_get(&b->states, key);
            if (source) {
                groove_BindingSourceKey kc; groove__ivm__op_types__BindingSourceKey__clone(&kc, key);
                groove_RecordDeltas *arc = SB(RecordDeltas__arc_new)(source_snapshot(source));
                groove_RecordDeltas *old = NULL;
                if (jz_hmap_insert(snapshot, &kc, &arc, &old)) { groove__ivm__op_types__BindingSourceKey__drop(&kc); jz_rc_release(old); }
            } else {
                groove_BindingSourceKey ok; groove_RecordDeltas *ov;
                if (jz_hmap_remove(snapshot, key, &ok, &ov)) { groove__ivm__op_types__BindingSourceKey__drop(&ok); jz_rc_release(ov); }
            }
        }
        jz_hmap_clear(&b->dirty);
    }
    return jz_rc_clone(b->snapshot);
}

/* ======================================================================
 * free functions
 * ==================================================================== */
static jz_err err_field_not_found_str(const jz_str *s) { return groove_err_field_not_found(s->ptr, s->len); }

static jz_err descriptor_field_names(groove_RecordDescriptor d, groove_StrVec *out) {
    memset(out, 0, sizeof *out);
    for (u32 i = 0; i < d->fields.len; i++) {
        if (!d->fields.ptr[i].has_name) { str_vec_drop(out); return groove_err_field_not_found("<unnamed>", 9); }
        jz_str c = jz_str_clone(&d->fields.ptr[i].name);
        jz_vec_push(out, &c, sizeof c);
    }
    return JZ_OK;
}

static jz_err validate_public_output_fields(groove_RecordDescriptor source, groove_RecordDescriptor public_output) {
    for (u32 fi = 0; fi < public_output->fields.len; fi++) {
        const groove_DescriptorField *field = &public_output->fields.ptr[fi];
        u32 name_len = 0;
        const char *name = groove__records__DescriptorField__logical_name(field, &name_len);
        if (!name && field->has_name) { name = field->name.ptr; name_len = field->name.len; }
        if (!name) return groove_err_field_not_found("<unnamed>", 9);
        usize index = 0; bool found = false;
        if (field->has_identity) found = groove__records__RecordDescriptor__field_index_by_identity(source, &field->identity, &index);
        if (!found) found = groove__records__RecordDescriptor__field_index(source, name, name_len, &index);
        if (!found && field->has_name) {
            for (u32 c = 0; c < source->fields.len; c++) {
                const groove_DescriptorField *candidate = &source->fields.ptr[c];
                if (candidate->has_name && jz_str_eq(&candidate->name, &field->name)) { index = c; found = true; break; }
            }
        }
        if (!found) return groove_err_field_not_found(name, name_len);
        if (index >= source->fields.len) return groove_err_field_oob(index);
        if (!groove__records__ValueType__registry_compatible_with(&source->fields.ptr[index].value_type, &field->value_type))
            return groove_err_output_mismatch();
    }
    return JZ_OK;
}

static jz_err validate_public_output_for_shape(const groove_RoutedMultisinkShapeState *shape, const jz_str *sink, groove_RecordDescriptor public_output) {
    const groove_RoutedMultisinkTerminalState *terminal = jz_btree_get(&shape->terminals, sink);
    if (!terminal) return jz_err_fmt(GROOVE_IVM_ERR_DUPLICATE_MULTISINK_SINK, "multisink sink already exists: %.*s", (int)sink->len, sink->ptr);
    return validate_public_output_fields(terminal->output.output, public_output);
}

static void route_predicate(const jz_str *field, const groove_Value *value, groove_PredicateExpr *out) {
    if (value->tag == GROOVE_V_NULLABLE && value->v.nullable == NULL) {
        groove__ivm__op_types__PredicateExpr__is_null(jz_str_clone(field), out);
    } else {
        groove__ivm__op_types__PredicateExpr__eq_literal(jz_str_clone(field), groove__ivm__op_types__LiteralValue__from_value_ref(value), out);
    }
}

typedef struct RouteFieldCtx { const groove_StrVec *route_fields; } RouteFieldCtx;
static bool is_route(const groove_FieldRef *field, const groove_StrVec *route_fields) {
    return field->tag == GROOVE_FR_NAME && str_vec_contains(route_fields, &field->name);
}
static bool keep_non_route_field(void *item, void *ctx) { return !is_route(item, ((RouteFieldCtx *)ctx)->route_fields); }
static bool keep_non_route_order(void *item, void *ctx) { return !is_route(&((groove_TopByOrder *)item)->field, ((RouteFieldCtx *)ctx)->route_fields); }
static void field_ref_drop_v(void *p) { groove__ivm__graph__FieldRef__drop(p); }
static void top_order_drop_v(void *p) { groove__ivm__graph__FieldRef__drop(&((groove_TopByOrder *)p)->field); }

void SB(strip_bound_route_fields_from_slot)(groove_CollectBySlotBuilder *slot, const groove_StrVec *route_fields) {
    RouteFieldCtx ctx = { route_fields };
    jz_vec_retain(&slot->group_cols, sizeof(groove_FieldRef), keep_non_route_field, &ctx, field_ref_drop_v);
    jz_vec_retain(&slot->owner_key_cols, sizeof(groove_FieldRef), keep_non_route_field, &ctx, field_ref_drop_v);
    jz_vec_retain(&slot->tie_cols, sizeof(groove_FieldRef), keep_non_route_field, &ctx, field_ref_drop_v);
    jz_vec_retain(&slot->order_cols, sizeof(groove_TopByOrder), keep_non_route_order, &ctx, top_order_drop_v);
    for (u32 i = 0; i < slot->slots.len; i++) SB(strip_bound_route_fields_from_slot)(&slot->slots.ptr[i], route_fields);
}
/* Route fields select the input; they are not part of a public collector's identity. */
static void strip_bound_route_fields_from_collect(groove_CollectByBuilder *collect, const groove_StrVec *route_fields) {
    RouteFieldCtx ctx = { route_fields };
    jz_vec_retain(&collect->group_cols, sizeof(groove_FieldRef), keep_non_route_field, &ctx, field_ref_drop_v);
    jz_vec_retain(&collect->tie_cols, sizeof(groove_FieldRef), keep_non_route_field, &ctx, field_ref_drop_v);
    jz_vec_retain(&collect->order_cols, sizeof(groove_TopByOrder), keep_non_route_order, &ctx, top_order_drop_v);
    for (u32 i = 0; i < collect->slots.len; i++) SB(strip_bound_route_fields_from_slot)(&collect->slots.ptr[i], route_fields);
}

typedef struct PublicFieldCtx { const groove_StrVec *public_fields; } PublicFieldCtx;
static bool keep_public_collect_field(void *item, void *ctx) {
    return str_vec_contains(((PublicFieldCtx *)ctx)->public_fields, &((groove_CollectByField *)item)->output_name);
}
static void collect_field_drop_v(void *p) { groove_CollectByField *f = p; groove__ivm__graph__FieldRef__drop(&f->field); jz_str_drop(&f->output_name); }

static jz_err bound_routed_multisink_graph(const groove_RoutedMultisinkTerminal *terminal, const groove_Value *binding_values, groove_RecordDescriptor output, groove_GraphBuilder *out) {
    JZ_VEC(groove_PredicateExpr) predicates = {0};
    u32 n = terminal->route_fields.len < terminal->route_value_indices.len ? terminal->route_fields.len : terminal->route_value_indices.len;
    for (u32 i = 0; i < n; i++) {
        groove_PredicateExpr p;
        route_predicate(&terminal->route_fields.ptr[i], &binding_values[terminal->route_value_indices.ptr[i]], &p);
        jz_vec_push(&predicates, &p, sizeof p);
    }
    bool has_predicate = predicates.len > 0;
    groove_PredicateExpr predicate; memset(&predicate, 0, sizeof predicate);
    if (predicates.len == 1) {
        predicate = predicates.ptr[0];
        jz_free(predicates.ptr);
    } else if (predicates.len > 1) {
        groove_PredicateExpr and_; memset(&and_, 0, sizeof and_);
        and_.tag = GROOVE_PRED_AND;
        and_.list.ptr = predicates.ptr; and_.list.len = predicates.len; and_.list.cap = predicates.cap;
        groove__ivm__op_types__PredicateExpr__canonicalize(and_, &predicate);
    }
    if (terminal->graph.tag == GROOVE_GB_COLLECT_BY) {
        /* CollectBy is terminal-only: route its flat input before rendering and
         * remove hidden route columns from the collector's projection and keys. */
        groove_CollectByBuilder *collect = groove__ivm__graph__CollectByBuilder__box_clone(terminal->graph.collect);
        PublicFieldCtx pctx = { &terminal->public_fields };
        jz_vec_retain(&collect->parent_fields, sizeof(groove_CollectByField), keep_public_collect_field, &pctx, collect_field_drop_v);
        jz_vec_retain(&collect->tuple_fields, sizeof(groove_CollectByField), keep_public_collect_field, &pctx, collect_field_drop_v);
        strip_bound_route_fields_from_collect(collect, &terminal->route_fields);
        groove_GraphBuilder input;
        groove__ivm__graph__GraphBuilder__clone(&input, terminal->graph.input);
        if (has_predicate) {
            groove_GraphBuilder filtered;
            groove__ivm__graph__GraphBuilder__filter(input, predicate, &filtered);
            input = filtered;
        }
        memset(out, 0, sizeof *out);
        out->tag = GROOVE_GB_COLLECT_BY;
        out->input = groove__ivm__graph__GraphBuilder__arc_new(input);
        out->collect = collect;
        return JZ_OK;
    }
    groove_GraphBuilder graph;
    groove__ivm__graph__GraphBuilder__clone(&graph, &terminal->graph);
    if (has_predicate) {
        groove_GraphBuilder filtered;
        groove__ivm__graph__GraphBuilder__filter(graph, predicate, &filtered);
        graph = filtered;
    }
    JZ_VEC(groove_ProjectField) fields = {0};
    for (u32 i = 0; i < terminal->public_fields.len; i++) {
        const jz_str *name = &terminal->public_fields.ptr[i];
        groove_ProjectField field; memset(&field, 0, sizeof field);
        field.expression.tag = GROOVE_PX_FIELD;
        groove__ivm__graph__FieldRef__stored_name(jz_str_clone(name), &field.expression.source);
        usize index = 0;
        jz_err e = groove__ivm__runtime__record_projection__resolve_field_ref(output, &field.expression.source, &index);
        if (JZ_IS_ERR(e)) {
            groove__ivm__graph__ProjectField__drop(&field);
            jz_vec_drop(&fields, sizeof(groove_ProjectField), (void (*)(void *))groove__ivm__graph__ProjectField__drop);
            groove__ivm__graph__GraphBuilder__drop(&graph);
            return e;
        }
        const groove_DescriptorField *df = &output->fields.ptr[index];
        field.output_name = jz_str_clone(name);
        if (df->has_identity) groove__records__FieldIdentity__clone(&field.output_identity, &df->identity);
        else { field.output_identity.tag = 0 /* FieldIdentity::Name */; field.output_identity.name = jz_str_clone(name); }
        jz_vec_push(&fields, &field, sizeof field);
    }
    groove__ivm__graph__GraphBuilder__project_fields(graph, fields.ptr, fields.len, out);
    return JZ_OK;
}

usize SB(count_builder_nodes)(const groove_GraphBuilder *g) {
    groove_GraphBuilderRefVec order = {0};
    groove__ivm__graph__GraphBuilder__postorder(g, &order);
    usize n = order.len;
    jz_vec_drop(&order, sizeof(const groove_GraphBuilder *), NULL);
    return n;
}
bool SB(builder_contains_binding_source)(const groove_GraphBuilder *g) {
    groove_GraphBuilderRefVec order = {0};
    groove__ivm__graph__GraphBuilder__postorder(g, &order);
    bool found = false;
    for (u32 i = 0; i < order.len && !found; i++) found = order.ptr[i]->tag == GROOVE_GB_BINDING_SOURCE;
    jz_vec_drop(&order, sizeof(const groove_GraphBuilder *), NULL);
    return found;
}

/* ======================================================================
 * impl IvmRuntime (subscriptions.rs)
 * ==================================================================== */
#define DEFAULT_SINK "__default"
enum { INPUT_SOURCE_TICK_COMPLETE, INPUT_SOURCE_TICK_DETACH_COLD };

static jz_err err_prepared_shape_not_found(groove_PreparedShapeId id) {
    return jz_err_fmt(GROOVE_IVM_ERR_PREPARED_SHAPE_NOT_FOUND, "subscription shape not found: PreparedShapeId(%llu)", (unsigned long long)id.v);
}
static jz_err err_binding_source_descriptor_mismatch(const char *p, u32 n) {
    return jz_err_fmt(GROOVE_IVM_ERR_BINDING_SOURCE_DESCRIPTOR_MISMATCH, "binding source descriptor mismatch: %.*s", (int)n, p);
}
static jz_err err_binding_source_not_found(const jz_str *s) {
    return jz_err_fmt(GROOVE_IVM_ERR_BINDING_SOURCE_NOT_FOUND, "binding source not found: %.*s", (int)s->len, s->ptr);
}
static jz_err err_foreign_input_source(void) { return jz_err_new(GROOVE_IVM_ERR_FOREIGN_INPUT_SOURCE, "input source belongs to a different IVM runtime"); }
static jz_err err_input_source_retired(void) { return jz_err_new(GROOVE_IVM_ERR_INPUT_SOURCE_RETIRED, "input source has been retired"); }
static jz_err err_empty_multisink(void) { return jz_err_new(GROOVE_IVM_ERR_EMPTY_MULTISINK_SUBSCRIPTION, "multisink subscription must have at least one sink"); }
static jz_err err_duplicate_sink(const jz_str *s) { return jz_err_fmt(GROOVE_IVM_ERR_DUPLICATE_MULTISINK_SINK, "multisink sink already exists: %.*s", (int)s->len, s->ptr); }
static jz_err err_route_arity(const jz_str *sink, usize expected, usize actual) {
    return jz_err_fmt(GROOVE_IVM_ERR_ROUTED_MULTISINK_ROUTE_ARITY_MISMATCH, "routed multisink sink %.*s has %zu route fields, expected %zu", (int)sink->len, sink->ptr, actual, expected);
}
static jz_err err_input_source_mismatch(groove_InputSourceId id) {
    jz_str name = {0};
    groove__ivm__op_types__InputSourceId__diagnostic_name(id, &name);
    jz_err e = err_binding_source_descriptor_mismatch(name.ptr, name.len);
    jz_str_drop(&name);
    return e;
}

static groove_BindingSourceKey prepared_key(const jz_str *shape) {
    groove_BindingSourceKey k; groove__ivm__op_types__BindingSourceKey__prepared(shape->ptr, shape->len, &k);
    return k;
}
static bool any_subscription_targets(const groove_IvmRuntime *self, groove_PreparedShapeId shape_id) {
    groove_SubscriptionId *id; groove_MultisinkSubscriptionState *sub;
    JZ_HMAP_FOR(&self->multisink_subscriptions, id, sub)
        if (sub->target.tag == GROOVE_MST_ROUTED_SHAPE && sub->target.shape_id.v == shape_id.v) return true;
    return false;
}
static void ephemeral_install_abort(groove_IvmRuntime *self) {
    /* Drop for EphemeralGraphInstall without commit */
    groove__ivm__runtime__graph_lifecycle__IvmRuntime__collect_unretained_ephemeral_nodes(self);
}
static void empty_table_deltas(groove_TableDeltaVec *v) { memset(v, 0, sizeof *v); }
static groove_OwnedStorage owned_storage(groove_OrderedKvStorage *storage) {
    return groove__storage__OwnedStorage__new(jz_rc_clone(storage));
}

static jz_err remove_binding_ref_for_source(groove_IvmRuntime *self, groove_BindingSourceKey source_key, const groove_BindingKey *binding, bool *some, groove_BindingDelta *out);
static bool remove_binding_metadata(groove_IvmRuntime *self, const groove_BindingSourceKey *source_key, const groove_BindingKey *binding, groove_RecordDescriptor *descriptor, bool *last_reference);
static void remove_unreferenced_binding_owned_shape(groove_IvmRuntime *self, groove_PreparedShapeId shape_id);
static void remove_unreferenced_auto_family(groove_IvmRuntime *self, groove_PreparedShapeId shape_id);
static jz_err binding_source_shape_name(const groove_IvmRuntime *self, groove_PreparedShapeId shape_id, jz_str *out);
static jz_err add_binding_ref(groove_IvmRuntime *self, groove_PreparedShapeId shape_id, groove_BindingKey binding, groove_BindingDelta *out);
static jz_err table_source_output(const groove_IvmRuntime *self, const jz_str *table, bool has_vp, const jz_str *variant_projection, groove_RecordDescriptor *out);

/* ---------------- runtime-owned input sources ---------------- */
typedef struct CanonInput { groove_RecordDescriptor descriptor; jz_btree adds, removes; } CanonInput;
static int cmp_input_source_id(const void *pa, const void *pb, void *ctx) {
    (void)ctx; const groove_InputSourceId *a = pa, *b = pb;
    if (a->runtime_namespace != b->runtime_namespace) return a->runtime_namespace < b->runtime_namespace ? -1 : 1;
    return a->local < b->local ? -1 : a->local > b->local;
}
static void canon_input_drop(void *p) { CanonInput *c = p; jz_btree_drop(&c->adds); jz_btree_drop(&c->removes); }
static const jz_btree_desc desc_canon_input = { sizeof(groove_InputSourceId), sizeof(CanonInput), cmp_input_source_id, NULL, canon_input_drop };
typedef struct CanonReplacement { groove_RecordDescriptor descriptor; jz_btree records; } CanonReplacement;
static void canon_replacement_drop(void *p) { jz_btree_drop(&((CanonReplacement *)p)->records); }
static const jz_btree_desc desc_canon_replacement = { sizeof(groove_InputSourceId), sizeof(CanonReplacement), cmp_input_source_id, NULL, canon_replacement_drop };
static const jz_btree_desc desc_input_source_id_set = { sizeof(groove_InputSourceId), 0, cmp_input_source_id, NULL, NULL };

static void bytes_set_insert_owned(jz_btree *set, jz_bytes b) {
    if (jz_btree_contains(set, &b)) groove_ivm_rt_a_drop_bytes(&b);
    else jz_btree_insert(set, &b, NULL, NULL);
}
static jz_err validate_encoded_record(const jz_bytes *record, groove_RecordDescriptor descriptor) {
    groove_BorrowedRecord borrowed = { record->ptr, record->len, descriptor };
    groove_ValueVec values = {0};
    jz_err e = groove__records__BorrowedRecord__to_values(&borrowed, &values);
    groove__records__values__ValueVec__drop(&values);
    return e;
}
static jz_err validate_record_set(const jz_btree *set, groove_RecordDescriptor descriptor) {
    jz_bytes *record; void *unit;
    JZ_BTREE_FOR(set, record, unit) JZ_TRY(validate_encoded_record(record, descriptor));
    return JZ_OK;
}
static bool binding_refcount_contains(const groove_BindingSourceState *source, const jz_bytes *record) {
    groove_BindingKey key = { *record };
    return jz_hmap_get(&source->refcounts, &key) != NULL;
}
static void push_record_delta(groove_RecordDeltaVec *v, const jz_bytes *record, i64 weight) {
    groove_RecordDelta d; groove_ivm_rt_a_clone_bytes(&d.record, record); d.weight = weight;
    jz_vec_push(v, &d, sizeof d);
}

/* Every descriptor and active source is checked before the first source mutates. */
static jz_err preflight_input_source(const groove_IvmRuntime *self, groove_InputSourceId id, groove_RecordDescriptor descriptor, bool check_retired_first) {
    if (!groove__ivm__op_types__InputSourceId__belongs_to(id, self->input_source_runtime_namespace)) return err_foreign_input_source();
    groove_BindingSourceKey key; groove__ivm__op_types__InputSourceId__binding_key(id, &key);
    const groove_BindingSourceState *source = SB(BindingSources__get)(&self->binding_sources, &key);
    groove__ivm__op_types__BindingSourceKey__drop(&key);
    if (!source) {
        if (groove__ivm__op_types__InputSourceId__was_allocated_by(id, self->input_source_runtime_namespace, self->next_input_source_id))
            return err_input_source_retired();
        return err_foreign_input_source();
    }
    (void)check_retired_first;
    if (source->descriptor != descriptor) return err_input_source_mismatch(id);
    return JZ_OK;
}

static jz_err tick_input_sources(groove_IvmRuntime *self, groove_BindingDeltaVec deltas, groove_OrderedKvStorage *storage, u32 mode, groove_TickMetrics *out) {
    groove_OwnedStorage owned = owned_storage(storage);
    if (mode == INPUT_SOURCE_TICK_COMPLETE) {
        groove_TableDeltaVec none; empty_table_deltas(&none);
        groove_OptPublicationId no_publication = { false, {0} };
        return RTK(tick_with_params)(self, none, deltas, owned, no_publication, out);
    }
    return RTK(tick_bindings_detaching_cold)(self, deltas, owned, out);
}

static jz_err apply_input_source_deltas_in_mode(groove_IvmRuntime *self, groove_InputSourceDelta *input_deltas, u32 n, groove_OrderedKvStorage *storage, u32 mode, groove_TickMetrics *out) {
    jz_btree canonical; jz_btree_init(&canonical, &desc_canon_input);
    jz_err e = JZ_OK;
    u32 i = 0;
    for (; i < n; i++) {
        groove_InputSourceDelta *delta = &input_deltas[i];
        jz_btree adds, removes;
        jz_btree_init(&adds, &groove_ivm_rt_a_desc_bytes_set);
        jz_btree_init(&removes, &groove_ivm_rt_a_desc_bytes_set);
        JZ_VEC_FOR(jz_bytes, r, delta->adds) bytes_set_insert_owned(&adds, *r);
        JZ_VEC_FOR(jz_bytes, r, delta->removes) bytes_set_insert_owned(&removes, *r);
        jz_free(delta->adds.ptr); jz_free(delta->removes.ptr);
        e = validate_record_set(&adds, delta->descriptor);
        if (!JZ_IS_ERR(e)) e = validate_record_set(&removes, delta->descriptor);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&adds); jz_btree_drop(&removes); i++; goto fail; }
        bool inserted;
        CanonInput *slot = jz_btree_entry(&canonical, &delta->id, &inserted);
        if (inserted) {
            slot->descriptor = delta->descriptor; slot->adds = adds; slot->removes = removes;
        } else {
            if (slot->descriptor != delta->descriptor) {
                e = err_input_source_mismatch(delta->id);
                jz_btree_drop(&adds); jz_btree_drop(&removes); i++; goto fail;
            }
            jz_btree_append(&slot->adds, &adds); jz_btree_append(&slot->removes, &removes);
            jz_btree_drop(&adds); jz_btree_drop(&removes);
        }
    }
    {
        groove_InputSourceId *id; CanonInput *ci;
        JZ_BTREE_FOR(&canonical, id, ci) {
            e = preflight_input_source(self, *id, ci->descriptor, false);
            if (JZ_IS_ERR(e)) goto fail;
        }
    }
    groove_BindingDeltaVec deltas = {0};
    groove_InputSourceId id; CanonInput ci;
    while (jz_btree_pop_first(&canonical, &id, &ci)) {
        groove_BindingSourceKey key; groove__ivm__op_types__InputSourceId__binding_key(id, &key);
        groove_BindingSourceState *source = SB(BindingSources__get_mut)(&self->binding_sources, &key);
        if (!source) jz_panic("preflight retained every active input source");
        /* cancelled = adds ∩ removes; removals and additions exclude it */
        groove_RecordDeltaVec records = {0};
        JZ_VEC(jz_bytes) removals = {0}, additions = {0};
        jz_bytes *record; void *unit;
        JZ_BTREE_FOR(&ci.removes, record, unit) {
            if (jz_btree_contains(&ci.adds, record)) continue;
            if (binding_refcount_contains(source, record)) { jz_vec_push(&removals, record, sizeof *record); push_record_delta(&records, record, -1); }
        }
        JZ_BTREE_FOR(&ci.adds, record, unit) {
            if (jz_btree_contains(&ci.removes, record)) continue;
            if (!binding_refcount_contains(source, record)) { jz_vec_push(&additions, record, sizeof *record); push_record_delta(&records, record, 1); }
        }
        if (records.len == 0) {
            groove__ivm__op_types__BindingSourceKey__drop(&key);
            jz_vec_drop(&removals, sizeof(jz_bytes), NULL); jz_vec_drop(&additions, sizeof(jz_bytes), NULL);
            canon_input_drop(&ci);
            continue;
        }
        JZ_VEC_FOR(jz_bytes, r, removals) {
            groove_BindingKey k = { *r }, ok; usize ov;
            if (jz_hmap_remove(&source->refcounts, &k, &ok, &ov)) drop_binding_key(&ok);
        }
        JZ_VEC_FOR(jz_bytes, r, additions) {
            groove_BindingKey k; groove_ivm_rt_a_clone_bytes(&k.bytes, r);
            usize one = 1;
            jz_hmap_insert(&source->refcounts, &k, &one, NULL);
        }
        jz_vec_drop(&removals, sizeof(jz_bytes), NULL); jz_vec_drop(&additions, sizeof(jz_bytes), NULL);
        canon_input_drop(&ci);
        groove_BindingDelta bd = { key, ci.descriptor, records, false };
        jz_vec_push(&deltas, &bd, sizeof bd);
    }
    jz_btree_drop(&canonical);
    if (deltas.len == 0) {
        jz_vec_drop(&deltas, sizeof(groove_BindingDelta), NULL);
        groove__ivm__runtime__state__TickMetrics__default(out);
        return JZ_OK;
    }
    return tick_input_sources(self, deltas, storage, mode, out);
fail:
    for (; i < n; i++) input_source_delta_drop(&input_deltas[i]);
    jz_btree_drop(&canonical);
    return e;
}
/* Atomically apply record-level changes to runtime-owned inputs. */
jz_err SUBS(apply_input_source_deltas)(groove_IvmRuntime *self, groove_InputSourceDelta *deltas, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out) {
    return apply_input_source_deltas_in_mode(self, deltas, n, storage, INPUT_SOURCE_TICK_COMPLETE, out);
}
/* Like the blocking form, but evaluation waiting on cold storage is retained as pending progress. */
jz_err SUBS(apply_input_source_deltas_detaching_cold)(groove_IvmRuntime *self, groove_InputSourceDelta *deltas, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out) {
    return apply_input_source_deltas_in_mode(self, deltas, n, storage, INPUT_SOURCE_TICK_DETACH_COLD, out);
}

static jz_err replace_input_sources_in_mode(groove_IvmRuntime *self, groove_InputSourceReplacement *replacements, u32 n, groove_OrderedKvStorage *storage, u32 mode, groove_TickMetrics *out) {
    jz_btree canonical; jz_btree_init(&canonical, &desc_canon_replacement);
    jz_err e = JZ_OK;
    u32 i = 0;
    for (; i < n; i++) {
        groove_InputSourceReplacement *replacement = &replacements[i];
        jz_btree records; jz_btree_init(&records, &groove_ivm_rt_a_desc_bytes_set);
        JZ_VEC_FOR(jz_bytes, r, replacement->records) bytes_set_insert_owned(&records, *r);
        jz_free(replacement->records.ptr);
        e = validate_record_set(&records, replacement->descriptor);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&records); i++; goto fail; }
        bool inserted;
        CanonReplacement *slot = jz_btree_entry(&canonical, &replacement->id, &inserted);
        if (inserted) {
            slot->descriptor = replacement->descriptor; slot->records = records;
        } else {
            if (slot->descriptor != replacement->descriptor) {
                e = err_input_source_mismatch(replacement->id);
                jz_btree_drop(&records); i++; goto fail;
            }
            jz_btree_append(&slot->records, &records);
            jz_btree_drop(&records);
        }
    }
    /* Do every failure-prone compatibility check before the first source changes. */
    {
        groove_InputSourceId *id; CanonReplacement *cr;
        JZ_BTREE_FOR(&canonical, id, cr) {
            if (!groove__ivm__op_types__InputSourceId__belongs_to(*id, self->input_source_runtime_namespace)) { e = err_foreign_input_source(); goto fail; }
            groove_BindingSourceKey key; groove__ivm__op_types__InputSourceId__binding_key(*id, &key);
            bool present = SB(BindingSources__contains_key)(&self->binding_sources, &key);
            const groove_BindingSourceState *source = SB(BindingSources__get)(&self->binding_sources, &key);
            groove__ivm__op_types__BindingSourceKey__drop(&key);
            if (!present) {
                e = groove__ivm__op_types__InputSourceId__was_allocated_by(*id, self->input_source_runtime_namespace, self->next_input_source_id)
                    ? err_input_source_retired() : err_foreign_input_source();
                goto fail;
            }
            if (!source) { e = err_input_source_retired(); goto fail; }
            if (source->descriptor != cr->descriptor) { e = err_input_source_mismatch(*id); goto fail; }
        }
    }
    groove_BindingDeltaVec deltas = {0};
    groove_InputSourceId id; CanonReplacement cr;
    while (jz_btree_pop_first(&canonical, &id, &cr)) {
        groove_BindingSourceKey key; groove__ivm__op_types__InputSourceId__binding_key(id, &key);
        groove_BindingSourceState *source = SB(BindingSources__get_mut)(&self->binding_sources, &key);
        if (!source) jz_panic("preflight retained every active input source");
        jz_btree previous; jz_btree_init(&previous, &groove_ivm_rt_a_desc_bytes_set);
        groove_BindingKey *bk; usize *count;
        JZ_HMAP_FOR(&source->refcounts, bk, count) {
            if (*count > 0) { jz_bytes c; groove_ivm_rt_a_clone_bytes(&c, &bk->bytes); jz_btree_insert(&previous, &c, NULL, NULL); }
        }
        bool changed = !jz_btree_eq(&previous, &cr.records, str_unit_eq);
        bool initializes_snapshot = !source->initialized;
        if (!changed && !initializes_snapshot) {
            jz_btree_drop(&previous); canon_replacement_drop(&cr); groove__ivm__op_types__BindingSourceKey__drop(&key);
            continue;
        }
        source->initialized = true;
        groove_RecordDeltaVec records = {0};
        jz_bytes *record; void *unit;
        JZ_BTREE_FOR(&previous, record, unit) if (!jz_btree_contains(&cr.records, record)) push_record_delta(&records, record, -1);
        JZ_BTREE_FOR(&cr.records, record, unit) if (!jz_btree_contains(&previous, record)) push_record_delta(&records, record, 1);
        jz_hmap_clear(&source->refcounts);
        jz_bytes r;
        while (jz_btree_pop_first(&cr.records, &r, NULL)) {
            groove_BindingKey k = { r }; usize one = 1;
            jz_hmap_insert(&source->refcounts, &k, &one, NULL);
        }
        jz_btree_drop(&previous); canon_replacement_drop(&cr);
        groove_BindingDelta bd = { key, cr.descriptor, records, initializes_snapshot };
        jz_vec_push(&deltas, &bd, sizeof bd);
    }
    jz_btree_drop(&canonical);
    if (deltas.len == 0) {
        jz_vec_drop(&deltas, sizeof(groove_BindingDelta), NULL);
        groove__ivm__runtime__state__TickMetrics__default(out);
        return JZ_OK;
    }
    return tick_input_sources(self, deltas, storage, mode, out);
fail:
    for (; i < n; i++) jz_vec_drop(&replacements[i].records, sizeof(jz_bytes), groove_ivm_rt_a_drop_bytes);
    jz_btree_drop(&canonical);
    return e;
}
/* Atomically replace the complete record multisets of runtime-owned inputs and drive one tick. */
jz_err SUBS(replace_input_sources)(groove_IvmRuntime *self, groove_InputSourceReplacement *r, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out) {
    return replace_input_sources_in_mode(self, r, n, storage, INPUT_SOURCE_TICK_COMPLETE, out);
}
jz_err SUBS(replace_input_sources_detaching_cold)(groove_IvmRuntime *self, groove_InputSourceReplacement *r, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out) {
    return replace_input_sources_in_mode(self, r, n, storage, INPUT_SOURCE_TICK_DETACH_COLD, out);
}

/* Retire runtime-owned input sources permanently: retract through one tick, then remove. */
jz_err SUBS(retire_input_sources)(groove_IvmRuntime *self, const groove_InputSourceId *in_ids, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out) {
    jz_btree ids; jz_btree_init(&ids, &desc_input_source_id_set);
    for (u32 i = 0; i < n; i++) if (!jz_btree_contains(&ids, &in_ids[i])) jz_btree_insert(&ids, &in_ids[i], NULL, NULL);
    groove_InputSourceId *id; void *unit;
    JZ_BTREE_FOR(&ids, id, unit) {
        if (!groove__ivm__op_types__InputSourceId__belongs_to(*id, self->input_source_runtime_namespace)) { jz_btree_drop(&ids); return err_foreign_input_source(); }
        groove_BindingSourceKey key; groove__ivm__op_types__InputSourceId__binding_key(*id, &key);
        bool present = SB(BindingSources__contains_key)(&self->binding_sources, &key);
        groove__ivm__op_types__BindingSourceKey__drop(&key);
        if (!present) { jz_btree_drop(&ids); return err_input_source_retired(); }
    }
    groove_BindingDeltaVec deltas = {0};
    JZ_BTREE_FOR(&ids, id, unit) {
        groove_BindingSourceKey key; groove__ivm__op_types__InputSourceId__binding_key(*id, &key);
        groove_BindingSourceState *source = SB(BindingSources__get_mut)(&self->binding_sources, &key);
        if (!source) jz_panic("retirement preflight retained every active input source");
        groove_RecordDeltaVec records = {0};
        groove_BindingKey *bk; usize *count;
        JZ_HMAP_FOR(&source->refcounts, bk, count) push_record_delta(&records, &bk->bytes, -1);
        if (records.len) {
            groove_BindingDelta bd = { key, source->descriptor, records, false };
            jz_vec_push(&deltas, &bd, sizeof bd);
        } else {
            groove__ivm__op_types__BindingSourceKey__drop(&key);
        }
        jz_hmap_clear(&source->refcounts);
    }
    jz_err e;
    if (deltas.len == 0) {
        jz_vec_drop(&deltas, sizeof(groove_BindingDelta), NULL);
        e = RTK(drive_pending_incremental)(self);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&ids); return e; }
        groove__ivm__runtime__state__TickMetrics__default(out);
    } else {
        groove_TableDeltaVec none; empty_table_deltas(&none);
        groove_OptPublicationId no_publication = { false, {0} };
        e = RTK(tick_with_params)(self, none, deltas, owned_storage(storage), no_publication, out);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&ids); return e; }
    }
    JZ_BTREE_FOR(&ids, id, unit) {
        groove_BindingSourceKey key; groove__ivm__op_types__InputSourceId__binding_key(*id, &key);
        SB(BindingSources__remove)(&self->binding_sources, &key, NULL);
        groove_BindingSourceKey ok; u64 ov;
        if (jz_hmap_remove(&self->binding_frontiers, &key, &ok, &ov)) groove__ivm__op_types__BindingSourceKey__drop(&ok);
        groove__ivm__op_types__BindingSourceKey__drop(&key);
    }
    jz_btree_drop(&ids);
    return JZ_OK;
}

/* ---------------- direct subscriptions ---------------- */
/* Publish all subscription work that can complete from resident inputs before
 * returning control to the caller. Cold inputs remain queued for the owner. */
static jz_err poll_ready_subscription_work_now_with_waker(groove_IvmRuntime *self, const jz_waker *progress_waker) {
    if (progress_waker) RTK(retain_owner_progress_waker)(self, progress_waker);
    static const jz_waker noop = { NULL, NULL };
    const jz_waker *cx = progress_waker ? progress_waker : &noop;
    jz_err result = JZ_OK;
    if (RTK(poll_pending_incremental)(self, cx, &result) == 0) return result;
    if (progress_waker) return JZ_OK;
    /* A direct opening owns every explicitly retained CPU slice: scan the
     * per-evaluation continuation state and leave storage requests untouched. */
    while (RTK(has_resident_continuation)(self)) {
        if (RTK(poll_resident_incremental)(self, cx, &result) == 0) return result;
    }
    return JZ_OK;
}

static void sink_graphs_drop(groove_SinkGraph *sinks, u32 from, u32 n) {
    for (u32 i = from; i < n; i++) { jz_str_drop(&sinks[i].sink); groove__ivm__graph__GraphBuilder__drop(&sinks[i].graph); }
}
static void outputs_init(jz_btree *outputs) { jz_btree_init(outputs, &groove_ivm_rt_a_desc_sink_compiled_node); }
static void add_output_retainers(groove_IvmRuntime *self, const jz_btree *outputs, groove_Retainer (*make)(void *ctx), void *ctx) {
    jz_str *sink; groove_CompiledNode *output;
    JZ_BTREE_FOR(outputs, sink, output) groove__ivm__runtime__graph_lifecycle__IvmRuntime__add_retainer(self, output->node, make(ctx));
}
typedef struct LifetimeRetainerCtx { groove_SubscriptionLifetime lifetime; groove_SubscriptionId id; } LifetimeRetainerCtx;
static groove_Retainer lifetime_retainer(void *ctx) { LifetimeRetainerCtx *c = ctx; return SB(SubscriptionLifetime__retainer)(c->lifetime, c->id); }
static groove_Retainer shape_retainer(void *ctx) { return prepared_shape_retainer(*(groove_PreparedShapeId *)ctx); }

static void subscription_handles_drop(groove_InitialCell *initial, jz_chan *receiver, groove_WaiterCell *waiter, groove_LivenessCell *liveness) {
    groove_MultisinkSubscription s = { {0, 0}, initial, receiver, waiter, liveness };
    SB(MultisinkSubscription__drop)(&s);
}

static jz_err subscribe_staged(groove_IvmRuntime *self, groove_SinkGraph *sinks, u32 n, groove_OrderedKvStorage *storage, groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root_indirect_values, groove_MultisinkSubscription *out) {
    jz_err e = JZ_OK;
    if (n == 0) { SB(RootIndirectValues__drop)(&root_indirect_values); return err_empty_multisink(); }
    for (u32 i = 0; i < n; i++)
        for (u32 j = 0; j < i; j++)
            if (jz_str_eq(&sinks[i].sink, &sinks[j].sink)) { e = err_duplicate_sink(&sinks[i].sink); goto fail_sinks; }
    for (u32 i = 0; i < n; i++)
        if (SB(builder_contains_binding_source)(&sinks[i].graph)) {
            e = jz_err_fmt(GROOVE_IVM_ERR_MULTISINK_SINK_REQUIRES_PREPARE, "multisink sink requires prepare because it contains a binding source: %.*s", (int)sinks[i].sink.len, sinks[i].sink.ptr);
            goto fail_sinks;
        }
    groove_SubscriptionId subscription_id = SUBS(next_subscription_id)(self);
    groove_SubscriptionSender sender; jz_chan *receiver; groove_WaiterCell *waiter; groove_LivenessCell *receiver_liveness;
    new_subscription_channel(&sender, &receiver, &waiter, &receiver_liveness);
    /* Compilation may add nodes to the shared hash-consed graph; the ephemeral
     * guard collects only unretained additions if hydration fails. */
    jz_btree outputs; outputs_init(&outputs);
    {
        u64 requested = 0;
        for (u32 i = 0; i < n; i++) requested += SB(count_builder_nodes)(&sinks[i].graph);
        self->logical_nodes_requested += requested;
        for (u32 i = 0; i < n; i++) {
            groove_CompiledNode compiled;
            e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_graph(self, &sinks[i].graph, &compiled);
            if (JZ_IS_ERR(e)) {
                sink_graphs_drop(sinks, i, n);
                jz_btree_drop(&outputs);
                ephemeral_install_abort(self);
                SB(SubscriptionSender__drop)(&sender);
                subscription_handles_drop(groove_ivm_rt_a_initial_cell_new(), receiver, waiter, receiver_liveness);
                SB(RootIndirectValues__drop)(&root_indirect_values);
                return e;
            }
            groove__ivm__graph__GraphBuilder__drop(&sinks[i].graph);
            jz_btree_insert(&outputs, &sinks[i].sink, &compiled, NULL);
        }
        LifetimeRetainerCtx ctx = { lifetime, subscription_id };
        add_output_retainers(self, &outputs, lifetime_retainer, &ctx);
        /* install.commit() */
    }
    groove_MultisinkSubscriptionState state;
    state.sender = sender;
    state.receiver_liveness = jz_rc_clone(receiver_liveness);
    jz_btree_clone(&state.outputs, &outputs, groove_ivm_rt_a_clone_str, NULL);
    memset(&state.target, 0, sizeof state.target);
    state.target.tag = GROOVE_MST_DIRECT;
    state.failed = false;
    SB(RootIndirectValues__clone)(&state.root_indirect_values, &root_indirect_values);
    jz_hmap_insert(&self->multisink_subscriptions, &subscription_id, &state, NULL);
    if (lifetime == GROOVE_SL_RETAINED) SUBS(index_subscription_outputs)(self, subscription_id, &outputs);
    groove_InitialCell *initial = groove_ivm_rt_a_initial_cell_new();
    jz_hmap no_borrowed; jz_hmap_init(&no_borrowed, &groove_ivm_rt_a_hdesc_nodeid_set);
    u64 evaluation;
    e = RTK(enqueue_subscription_hydration)(self, subscription_id, outputs, owned_storage(storage), NULL, NULL,
        jz_rc_clone(initial), lifetime, root_indirect_values, no_borrowed, &evaluation);
    if (JZ_IS_ERR(e)) { subscription_handles_drop(initial, receiver, waiter, receiver_liveness); return e; }
    out->id = subscription_id; out->initial = initial; out->receiver = receiver; out->waiter = waiter; out->receiver_liveness = receiver_liveness;
    return JZ_OK;
fail_sinks:
    sink_graphs_drop(sinks, 0, n);
    SB(RootIndirectValues__drop)(&root_indirect_values);
    return e;
}

jz_err SUBS(subscribe_with_lifetime)(groove_IvmRuntime *self, groove_SinkGraph *sinks, u32 n, groove_OrderedKvStorage *storage, groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root, const jz_waker *progress_waker, groove_MultisinkSubscription *out) {
    JZ_TRY(subscribe_staged(self, sinks, n, storage, lifetime, root, out));
    jz_err e = poll_ready_subscription_work_now_with_waker(self, progress_waker);
    if (JZ_IS_ERR(e)) { SB(MultisinkSubscription__drop)(out); return e; }
    return JZ_OK;
}
/* Internal owner-loop counterpart to subscribe. */
jz_err SUBS(subscribe_with_waker)(groove_IvmRuntime *self, groove_SinkGraph *sinks, u32 n, groove_OrderedKvStorage *storage, const jz_waker *progress_waker, groove_MultisinkSubscription *out) {
    groove_RootIndirectValues materialize = { GROOVE_RIV_MATERIALIZE, NULL };
    return SUBS(subscribe_with_lifetime)(self, sinks, n, storage, GROOVE_SL_RETAINED, materialize, progress_waker, out);
}
jz_err SUBS(subscribe)(groove_IvmRuntime *self, groove_SinkGraph *sinks, u32 n, groove_OrderedKvStorage *storage, groove_MultisinkSubscription *out) {
    return SUBS(subscribe_with_waker)(self, sinks, n, storage, NULL, out);
}

/* ---------------- prepared shapes ---------------- */
static void terminals_drop(groove_RoutedMultisinkTerminal *t, u32 from, u32 n) {
    for (u32 i = from; i < n; i++) SB(RoutedMultisinkTerminal__drop)(&t[i]);
}
jz_err SUBS(prepare)(groove_IvmRuntime *self, groove_RoutedMultisinkTerminal *terminals, u32 n, jz_str shape, groove_RecordDescriptor binding_descriptor, groove_OrderedKvStorage *storage, groove_PreparedShape *out) {
    jz_err e = RTK(flush_pending_binding_retractions)(self, storage);
    if (JZ_IS_ERR(e)) goto fail_all;
    if (n == 0) { e = err_empty_multisink(); goto fail_all; }
    usize binding_len = binding_descriptor->fields.len;
    for (u32 i = 0; i < n; i++) {
        const groove_RoutedMultisinkTerminal *terminal = &terminals[i];
        for (u32 j = 0; j < i; j++) if (jz_str_eq(&terminals[j].sink, &terminal->sink)) { e = err_duplicate_sink(&terminal->sink); goto fail_all; }
        if (terminal->route_fields.len > binding_len) { e = err_route_arity(&terminal->sink, binding_len, terminal->route_fields.len); goto fail_all; }
        if (terminal->route_value_indices.len != terminal->route_fields.len) {
            e = err_route_arity(&terminal->sink, terminal->route_fields.len, terminal->route_value_indices.len); goto fail_all;
        }
        for (u32 k = 0; k < terminal->route_value_indices.len; k++)
            if (terminal->route_value_indices.ptr[k] >= binding_len) { e = groove_err_field_oob(terminal->route_value_indices.ptr[k]); goto fail_all; }
        groove_RecordDescriptor output;
        e = SUBS(infer_builder_output)(self, &terminal->graph, &output);
        if (JZ_IS_ERR(e)) goto fail_all;
        for (u32 k = 0; k < terminal->route_fields.len; k++) {
            usize idx;
            if (!groove__records__RecordDescriptor__field_index(output, terminal->route_fields.ptr[k].ptr, terminal->route_fields.ptr[k].len, &idx)) {
                e = err_field_not_found_str(&terminal->route_fields.ptr[k]); goto fail_all;
            }
        }
        for (u32 k = 0; k < terminal->public_fields.len; k++) {
            groove_FieldRef f; groove__ivm__graph__FieldRef__stored_name(jz_str_clone(&terminal->public_fields.ptr[k]), &f);
            usize idx;
            e = groove__ivm__runtime__record_projection__resolve_field_ref(output, &f, &idx);
            groove__ivm__graph__FieldRef__drop(&f);
            if (JZ_IS_ERR(e)) goto fail_all;
        }
    }
    {
        u64 requested = 0;
        for (u32 i = 0; i < n; i++) requested += SB(count_builder_nodes)(&terminals[i].graph);
        self->logical_nodes_requested += requested;
    }
    groove_PreparedShapeId shape_id = SUBS(next_shape_id)(self);
    bool inserted_source;
    {
        bool inserted;
        groove_BindingSourceState *slot = SB(BindingSources__entry)(&self->binding_sources, prepared_key(&shape), &inserted);
        if (!inserted && slot->descriptor != binding_descriptor) {
            e = err_binding_source_descriptor_mismatch(shape.ptr, shape.len);
            goto fail_all;
        }
        if (inserted) {
            slot->descriptor = binding_descriptor;
            jz_hmap_init(&slot->refcounts, &groove_ivm_rt_a_hdesc_binding_refcounts);
            slot->initialized = true;
        }
        inserted_source = inserted;
    }
    jz_btree terminal_states; jz_btree_init(&terminal_states, &groove_ivm_rt_a_desc_sink_terminal_state);
    for (u32 i = 0; i < n; i++) {
        groove_CompiledNode output;
        e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_graph(self, &terminals[i].graph, &output);
        if (JZ_IS_ERR(e)) {
            if (inserted_source) {
                groove_BindingSourceKey key = prepared_key(&shape);
                SB(BindingSources__remove)(&self->binding_sources, &key, NULL);
                groove__ivm__op_types__BindingSourceKey__drop(&key);
            }
            terminals_drop(terminals, i, n);
            jz_btree_drop(&terminal_states);
            jz_str_drop(&shape);
            ephemeral_install_abort(self);
            return e;
        }
        jz_str sink = jz_str_clone(&terminals[i].sink);
        groove_RoutedMultisinkTerminalState state = { terminals[i], output };
        groove_RoutedMultisinkTerminalState old;
        if (jz_btree_insert(&terminal_states, &sink, &state, &old)) { jz_str_drop(&sink); terminal_state_drop(&old); }
    }
    /* Publish retainers only after every terminal compiles. */
    {
        jz_str *sink; groove_RoutedMultisinkTerminalState *terminal;
        JZ_BTREE_FOR(&terminal_states, sink, terminal)
            groove__ivm__runtime__graph_lifecycle__IvmRuntime__add_retainer(self, terminal->output.node, prepared_shape_retainer(shape_id));
    }
    groove_RoutedMultisinkShapeState shape_state; memset(&shape_state, 0, sizeof shape_state);
    shape_state.shape = shape;
    shape_state.binding_descriptor = binding_descriptor;
    shape_state.terminals = terminal_states;
    jz_hmap_insert(&self->prepared_shapes, &shape_id, &shape_state, NULL);
    /* install.commit() */
    out->id = shape_id;
    return JZ_OK;
fail_all:
    terminals_drop(terminals, 0, n);
    jz_str_drop(&shape);
    return e;
}

static int cmp_terminal_sink(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_str_cmp(&((const groove_RoutedMultisinkTerminal *)a)->sink, &((const groove_RoutedMultisinkTerminal *)b)->sink);
}
/* Like prepare, but callers preparing identical terminals for a live shared
 * shape of the same binding source get that shape back. */
jz_err SUBS(prepare_shared)(groove_IvmRuntime *self, groove_RoutedMultisinkTerminal *terminals, u32 n, jz_str shape, groove_RecordDescriptor binding_descriptor, groove_OrderedKvStorage *storage, groove_PreparedShape *out) {
    jz_sort(terminals, n, sizeof(groove_RoutedMultisinkTerminal), cmp_terminal_sink, NULL);
    groove_SharedShapeKey key; key.shape = jz_str_clone(&shape);
    memset(&key.terminals, 0, sizeof key.terminals);
    for (u32 i = 0; i < n; i++) {
        groove_RoutedMultisinkTerminal c; SB(RoutedMultisinkTerminal__clone)(&c, &terminals[i]);
        jz_vec_push(&key.terminals, &c, sizeof c);
    }
    /* Prune first so a shape whose last receiver was dropped retires now. */
    usize pruned;
    jz_err e = SUBS(prune_dropped_subscriptions_with_storage)(self, storage, &pruned);
    if (JZ_IS_ERR(e)) goto fail;
    const groove_PreparedShapeId *existing = jz_hmap_get(&self->shared_prepared_shapes, &key);
    if (existing) {
        groove_PreparedShapeId shape_id = *existing;
        const groove_RoutedMultisinkShapeState *state = jz_hmap_get(&self->prepared_shapes, &shape_id);
        if (state && state->binding_descriptor == binding_descriptor) {
            e = RTK(flush_pending_binding_retractions)(self, storage);
            if (JZ_IS_ERR(e)) goto fail;
            SB(SharedShapeKey__drop)(&key); terminals_drop(terminals, 0, n); jz_str_drop(&shape);
            out->id = shape_id;
            return JZ_OK;
        }
    }
    groove_PreparedShape prepared;
    e = SUBS(prepare)(self, terminals, n, shape, binding_descriptor, storage, &prepared);
    if (JZ_IS_ERR(e)) { SB(SharedShapeKey__drop)(&key); return e; }
    groove_RoutedMultisinkShapeState *state = jz_hmap_get(&self->prepared_shapes, &prepared.id);
    if (state) {
        if (state->has_shared_key) SB(SharedShapeKey__drop)(&state->shared_key);
        SB(SharedShapeKey__clone)(&state->shared_key, &key);
        state->has_shared_key = true;
        state->retire_when_unbound = true;
    }
    groove_PreparedShapeId old;
    if (jz_hmap_insert(&self->shared_prepared_shapes, &key, &prepared.id, &old)) SB(SharedShapeKey__drop)(&key);
    *out = prepared;
    return JZ_OK;
fail:
    SB(SharedShapeKey__drop)(&key); terminals_drop(terminals, 0, n); jz_str_drop(&shape);
    return e;
}

/* Retire a shared prepared shape that no retained binding holds. */
void SUBS(release_shared_prepared_shape)(groove_IvmRuntime *self, groove_PreparedShapeId shape_id) {
    remove_unreferenced_binding_owned_shape(self, shape_id);
}
/* Transfer a successful binding's shape to its retained bindings. */
void SUBS(transfer_prepared_shape_to_bindings)(groove_IvmRuntime *self, groove_PreparedShapeId shape_id) {
    groove_RoutedMultisinkShapeState *shape = jz_hmap_get(&self->prepared_shapes, &shape_id);
    if (shape) shape->retire_when_unbound = true;
    remove_unreferenced_binding_owned_shape(self, shape_id);
}

/* ---------------- binding a prepared shape ---------------- */
enum { PPO_FIELDS, PPO_ONE_SINK };
typedef struct PreparedPublicOutput { u32 tag; jz_btree fields; /* String -> Vec<String> */ groove_RecordDescriptor output; } PreparedPublicOutput;
static void str_vec_val_drop(void *p) { str_vec_drop((groove_StrVec *)p); }
static const jz_btree_desc desc_sink_public_fields = { sizeof(jz_str), sizeof(groove_StrVec), jz_cmp_str, groove_ivm_rt_a_drop_str, str_vec_val_drop };
static PreparedPublicOutput public_fields_output(void) {
    PreparedPublicOutput p; p.tag = PPO_FIELDS; jz_btree_init(&p.fields, &desc_sink_public_fields); p.output = NULL;
    return p;
}

static bool register_route_barrier(groove_IvmRuntime *self, const groove_RoutedMultisinkTerminal *terminal, const groove_CompiledNode *shared, const groove_CompiledNode *bound, const groove_Value *binding_values, u32 nvalues, groove_NodeId *out);
bool SB(IvmRuntime__cancel_pending_binding_retraction)(groove_IvmRuntime *self, const jz_str *shape, const groove_BindingKey *binding);
jz_err SB(IvmRuntime__provisional_binding_delta)(const groove_IvmRuntime *self, groove_PreparedShapeId shape_id, const groove_BindingKey *binding, groove_BindingDelta *out);

static jz_err bind_shape_with_public_fields_staged_inner(groove_IvmRuntime *self, groove_PreparedShapeId shape_id, const groove_Value *binding_values, u32 nvalues,
    PreparedPublicOutput public_output, groove_OrderedKvStorage *storage, groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root_indirect_values,
    groove_LiveAttach *live, groove_MultisinkSubscription *out) {
    jz_err e = JZ_OK;
    const groove_RoutedMultisinkShapeState *found = jz_hmap_get(&self->prepared_shapes, &shape_id);
    jz_btree public_fields;
    if (!found) { e = err_prepared_shape_not_found(shape_id); jz_btree_drop(&public_output.fields); goto fail_root; }
    if (public_output.tag == PPO_FIELDS) {
        public_fields = public_output.fields;
    } else {
        jz_btree_drop(&public_output.fields);
        jz_str default_sink = str_borrow(DEFAULT_SINK, sizeof DEFAULT_SINK - 1);
        e = validate_public_output_for_shape(found, &default_sink, public_output.output);
        if (JZ_IS_ERR(e)) goto fail_root;
        groove_StrVec names;
        e = descriptor_field_names(public_output.output, &names);
        if (JZ_IS_ERR(e)) goto fail_root;
        jz_btree_init(&public_fields, &desc_sink_public_fields);
        jz_str key = jz_str_from(DEFAULT_SINK);
        jz_btree_insert(&public_fields, &key, &names, NULL);
    }
    groove_RoutedMultisinkShapeState shape;
    SB(RoutedMultisinkShapeState__clone)(&shape, found);
    jz_bytes binding_record = {0};
    e = groove__records__RecordDescriptor__create(shape.binding_descriptor, binding_values, nvalues, &binding_record);
    if (JZ_IS_ERR(e)) { SB(RoutedMultisinkShapeState__drop)(&shape); jz_btree_drop(&public_fields); goto fail_root; }
    groove_BindingKey binding_key = { binding_record };
    if (live) JZ_ASSERT(groove_ivm_rt_a_eq_bytes(&live->binding_key.bytes, &binding_key.bytes), "live attach binding key mismatch");
    groove_SubscriptionId subscription_id = SUBS(next_subscription_id)(self);

    jz_btree outputs; outputs_init(&outputs);
    groove_NodeIdVec route_barriers = {0};
    groove_BindingSnapshots *binding_snapshots = NULL;
    bool binding_added = false;
    {
        u64 requested = 0;
        jz_str *sink; groove_RoutedMultisinkTerminalState *prepared_terminal;
        JZ_BTREE_FOR(&shape.terminals, sink, prepared_terminal) requested += SB(count_builder_nodes)(&prepared_terminal->terminal.graph) + 2;
        self->logical_nodes_requested += requested;
        JZ_BTREE_FOR(&shape.terminals, sink, prepared_terminal) {
            groove_RoutedMultisinkTerminal terminal;
            SB(RoutedMultisinkTerminal__clone)(&terminal, &prepared_terminal->terminal);
            const groove_StrVec *fields = jz_btree_get(&public_fields, sink);
            if (fields) { str_vec_drop(&terminal.public_fields); str_vec_clone(&terminal.public_fields, fields); }
            groove_GraphBuilder graph;
            e = bound_routed_multisink_graph(&terminal, binding_values, prepared_terminal->output.output, &graph);
            groove_CompiledNode output;
            if (!JZ_IS_ERR(e)) {
                e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_graph(self, &graph, &output);
                groove__ivm__graph__GraphBuilder__drop(&graph);
            }
            if (JZ_IS_ERR(e)) { SB(RoutedMultisinkTerminal__drop)(&terminal); goto fail_install; }
            groove_NodeId barrier;
            if (lifetime == GROOVE_SL_RETAINED && register_route_barrier(self, &terminal, &prepared_terminal->output, &output, binding_values, nvalues, &barrier))
                jz_vec_push(&route_barriers, &barrier, sizeof barrier);
            SB(RoutedMultisinkTerminal__drop)(&terminal);
            jz_str sink_name = jz_str_clone(sink);
            jz_btree_insert(&outputs, &sink_name, &output, NULL);
        }
        jz_str binding_shape;
        e = binding_source_shape_name(self, shape_id, &binding_shape);
        if (JZ_IS_ERR(e)) goto fail_install;
        LifetimeRetainerCtx ctx = { lifetime, subscription_id };
        if (live) {
            /* The attach tick already made the shared nodes current for this
             * binding: hydrate only what this binding reads. */
            binding_snapshots = SUBS(binding_snapshot_deltas)(self);
            groove_BindingSourceKey source_key = prepared_key(&binding_shape);
            groove_RecordDeltas **current = jz_hmap_get(binding_snapshots, &source_key);
            if (current) {
                groove_RecordDeltas only = groove_RecordDeltas_empty((*current)->descriptor);
                JZ_VEC_FOR(groove_RecordDelta, delta, (*current)->deltas)
                    if (groove_ivm_rt_a_eq_bytes(&delta->record, &binding_key.bytes)) {
                        groove_RecordDelta c; SB(RecordDelta__clone)(&c, delta); jz_vec_push(&only.deltas, &c, sizeof c);
                    }
                groove_RecordDeltas *only_arc = SB(RecordDeltas__arc_new)(only);
                jz_hmap *snapshots = jz_rc_make_mut((void **)&binding_snapshots, sizeof(jz_hmap), snapshot_map_clone);
                groove_RecordDeltas *old;
                if (jz_hmap_insert(snapshots, &source_key, &only_arc, &old)) { groove__ivm__op_types__BindingSourceKey__drop(&source_key); jz_rc_release(old); }
            } else {
                groove__ivm__op_types__BindingSourceKey__drop(&source_key);
            }
            add_output_retainers(self, &outputs, lifetime_retainer, &ctx);
            binding_added = false;
        } else {
            bool cancelled_retraction = lifetime == GROOVE_SL_RETAINED && SB(IvmRuntime__cancel_pending_binding_retraction)(self, &binding_shape, &binding_key);
            groove_BindingDelta binding_delta;
            e = SB(IvmRuntime__provisional_binding_delta)(self, shape_id, &binding_key, &binding_delta);
            if (JZ_IS_ERR(e)) { jz_str_drop(&binding_shape); goto fail_install; }
            /* A queued retraction still changes the source's binding set before this hydration is current. */
            binding_added = binding_delta.deltas.len != 0;
            for (u32 i = 0; !binding_added && i < self->pending_binding_retractions.len; i++)
                binding_added = groove__ivm__op_types__BindingSourceKey__eq(&self->pending_binding_retractions.ptr[i].key, &binding_delta.key);
            binding_snapshots = SUBS(binding_snapshot_deltas)(self);
            jz_hmap *snapshots = jz_rc_make_mut((void **)&binding_snapshots, sizeof(jz_hmap), snapshot_map_clone);
            bool inserted;
            groove_BindingSourceKey entry_key; groove__ivm__op_types__BindingSourceKey__clone(&entry_key, &binding_delta.key);
            groove_RecordDeltas **slot = jz_hmap_entry(snapshots, &entry_key, &inserted);
            if (inserted) *slot = SB(RecordDeltas__arc_new)(groove_RecordDeltas_empty(binding_delta.descriptor));
            else groove__ivm__op_types__BindingSourceKey__drop(&entry_key);
            groove_RecordDeltas *snapshot = jz_rc_make_mut((void **)slot, sizeof(groove_RecordDeltas), record_deltas_clone_v);
            JZ_VEC_FOR(groove_RecordDelta, delta, binding_delta.deltas) {
                if (delta->weight <= 0) continue;
                bool present = false;
                for (u32 i = 0; !present && i < snapshot->deltas.len; i++) present = groove_ivm_rt_a_eq_bytes(&snapshot->deltas.ptr[i].record, &delta->record);
                if (!present) { groove_RecordDelta c; SB(RecordDelta__clone)(&c, delta); jz_vec_push(&snapshot->deltas, &c, sizeof c); }
            }
            if (lifetime == GROOVE_SL_RETAINED) {
                groove_BindingKey key_clone; groove_ivm_rt_a_clone_bytes(&key_clone.bytes, &binding_key.bytes);
                groove_BindingDelta installed_delta;
                e = add_binding_ref(self, shape_id, key_clone, &installed_delta);
                if (JZ_IS_ERR(e)) { SB(BindingDelta__drop)(&binding_delta); jz_str_drop(&binding_shape); goto fail_install; }
                if (!cancelled_retraction) RTK(bump_input_frontiers)(self, NULL, 0, &installed_delta, 1);
                SB(BindingDelta__drop)(&installed_delta);
            }
            SB(BindingDelta__drop)(&binding_delta);
            add_output_retainers(self, &outputs, lifetime_retainer, &ctx);
        }
        jz_str_drop(&binding_shape);
        /* install.commit() */
    }
    groove_SubscriptionSender sender; jz_chan *receiver; groove_WaiterCell *waiter; groove_LivenessCell *receiver_liveness;
    new_subscription_channel(&sender, &receiver, &waiter, &receiver_liveness);
    groove_MultisinkSubscriptionState state;
    state.sender = sender;
    state.receiver_liveness = jz_rc_clone(receiver_liveness);
    jz_btree_clone(&state.outputs, &outputs, groove_ivm_rt_a_clone_str, NULL);
    memset(&state.target, 0, sizeof state.target);
    if (lifetime == GROOVE_SL_RETAINED) {
        state.target.tag = GROOVE_MST_ROUTED_SHAPE;
        state.target.shape_id = shape_id;
        groove_ivm_rt_a_clone_bytes(&state.target.binding_key.bytes, &binding_key.bytes);
        state.target.route_barriers = route_barriers;
    } else {
        state.target.tag = GROOVE_MST_DIRECT;
        jz_vec_drop(&route_barriers, sizeof(groove_NodeId), NULL);
    }
    state.failed = false;
    SB(RootIndirectValues__clone)(&state.root_indirect_values, &root_indirect_values);
    jz_hmap_insert(&self->multisink_subscriptions, &subscription_id, &state, NULL);
    if (lifetime == GROOVE_SL_RETAINED) SUBS(index_subscription_outputs)(self, subscription_id, &outputs);
    groove_InitialCell *initial = groove_ivm_rt_a_initial_cell_new();
    /* Only a change to the source's binding set changes what its readers produce (#3797). */
    const jz_str *binding_frontier_advance = NULL;
    jz_hmap borrowed;
    if (live) {
        borrowed = live->borrowed;
        jz_hmap_init(&live->borrowed, &groove_ivm_rt_a_hdesc_nodeid_set);
    } else {
        jz_hmap_init(&borrowed, &groove_ivm_rt_a_hdesc_nodeid_set);
        if (binding_added) binding_frontier_advance = &shape.shape;
    }
    u64 evaluation;
    e = RTK(enqueue_subscription_hydration)(self, subscription_id, outputs, owned_storage(storage), binding_snapshots, binding_frontier_advance,
        jz_rc_clone(initial), lifetime, root_indirect_values, borrowed, &evaluation);
    groove_ivm_rt_a_drop_bytes(&binding_key.bytes);
    SB(RoutedMultisinkShapeState__drop)(&shape);
    jz_btree_drop(&public_fields);
    if (JZ_IS_ERR(e)) { subscription_handles_drop(initial, receiver, waiter, receiver_liveness); return e; }
    out->id = subscription_id; out->initial = initial; out->receiver = receiver; out->waiter = waiter; out->receiver_liveness = receiver_liveness;
    return JZ_OK;
fail_install:
    ephemeral_install_abort(self);
    if (binding_snapshots) jz_rc_release(binding_snapshots);
    jz_btree_drop(&outputs);
    jz_vec_drop(&route_barriers, sizeof(groove_NodeId), NULL);
    groove_ivm_rt_a_drop_bytes(&binding_key.bytes);
    SB(RoutedMultisinkShapeState__drop)(&shape);
    jz_btree_drop(&public_fields);
fail_root:
    SB(RootIndirectValues__drop)(&root_indirect_values);
    return e;
}

static jz_err bind_shape_with_public_fields_staged(groove_IvmRuntime *self, groove_PreparedShapeId shape_id, const groove_Value *values, u32 n,
    PreparedPublicOutput public_output, groove_OrderedKvStorage *storage, groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root,
    groove_LiveAttach *live, groove_MultisinkSubscription *out) {
    jz_err result = bind_shape_with_public_fields_staged_inner(self, shape_id, values, n, public_output, storage, lifetime, root, live, out);
    if (JZ_IS_ERR(result) && live) {
        /* The attach tick already admitted this binding into the shared graph.
         * Retract it on the next tick, as an unsubscribe would. */
        bool some; groove_BindingDelta delta;
        groove_BindingSourceKey source = live->binding_source;
        memset(&live->binding_source, 0, sizeof live->binding_source);
        remove_binding_ref_for_source(self, source, &live->binding_key, &some, &delta);
        if (some) {
            if (delta.deltas.len) jz_vec_push(&self->pending_binding_retractions, &delta, sizeof delta);
            else SB(BindingDelta__drop)(&delta);
        }
    }
    if (live) SB(LiveAttach__drop)(live);
    return result;
}

static jz_err bind_shape_with_public_fields(groove_IvmRuntime *self, groove_PreparedShapeId shape_id, const groove_Value *values, u32 n, jz_btree public_fields, groove_OrderedKvStorage *storage, const jz_waker *progress_waker, groove_MultisinkSubscription *out) {
    PreparedPublicOutput po = { PPO_FIELDS, public_fields, NULL };
    groove_RootIndirectValues materialize = { GROOVE_RIV_MATERIALIZE, NULL };
    JZ_TRY(bind_shape_with_public_fields_staged(self, shape_id, values, n, po, storage, GROOVE_SL_RETAINED, materialize, NULL, out));
    jz_err e = poll_ready_subscription_work_now_with_waker(self, progress_waker);
    if (JZ_IS_ERR(e)) { SB(MultisinkSubscription__drop)(out); return e; }
    return JZ_OK;
}
jz_err SUBS(bind_shape)(groove_IvmRuntime *self, groove_PreparedShapeId id, const groove_Value *values, u32 n, groove_OrderedKvStorage *storage, groove_MultisinkSubscription *out) {
    PreparedPublicOutput po = public_fields_output();
    return bind_shape_with_public_fields(self, id, values, n, po.fields, storage, NULL, out);
}
jz_err SUBS(bind_shape_with_lifetime)(groove_IvmRuntime *self, groove_PreparedShapeId id, const groove_Value *values, u32 n, groove_OrderedKvStorage *storage, groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root, const jz_waker *progress_waker, groove_LiveAttach *live, groove_MultisinkSubscription *out) {
    JZ_TRY(bind_shape_with_public_fields_staged(self, id, values, n, public_fields_output(), storage, lifetime, root, live, out));
    jz_err e = poll_ready_subscription_work_now_with_waker(self, progress_waker);
    if (JZ_IS_ERR(e)) { SB(MultisinkSubscription__drop)(out); return e; }
    return JZ_OK;
}

/* ---------------- route barriers ---------------- */
/* The route filter of a flat binding: `bound` must be exactly Project(Filter(shared)). */
static bool flat_route_filter(const groove_IvmRuntime *self, groove_NodeId shared, groove_NodeId bound, groove_NodeId *out) {
    const groove_GraphNode *project = groove__ivm__graph__IvmGraph__node(self->graph, bound);
    if (!project || project->descriptor.op.tag != GROOVE_OP_MAP_PROJECT) return false;
    if (project->descriptor.inputs.len != 1) return false;
    groove_NodeId barrier = project->descriptor.inputs.ptr[0];
    const groove_GraphNode *filter = groove__ivm__graph__IvmGraph__node(self->graph, barrier);
    if (!filter) return false;
    if (filter->descriptor.op.tag != GROOVE_OP_FILTER || filter->descriptor.inputs.len != 1 || filter->descriptor.inputs.ptr[0].v != shared.v) return false;
    *out = barrier;
    return true;
}
/* The route filter of a collector binding (#3308): CollectBy(Arrange(Filter(input))). */
static bool collector_route_filter(const groove_IvmRuntime *self, groove_NodeId bound, groove_NodeId *barrier, groove_NodeId *shared, groove_RecordDescriptor *shared_output) {
    const groove_GraphNode *collector = groove__ivm__graph__IvmGraph__node(self->graph, bound);
    if (!collector || collector->descriptor.op.tag != GROOVE_OP_COLLECT_BY) return false;
    const groove_GraphNode *current = collector;
    for (;;) {
        if (current->descriptor.inputs.len != 1) return false;
        current = groove__ivm__graph__IvmGraph__node(self->graph, current->descriptor.inputs.ptr[0]);
        if (!current) return false;
        if (current->descriptor.op.tag == GROOVE_OP_ARRANGE) continue;
        if (current->descriptor.op.tag == GROOVE_OP_FILTER) break;
        return false;
    }
    if (current->descriptor.inputs.len != 1) return false;
    groove_NodeId shared_id = current->descriptor.inputs.ptr[0];
    const groove_GraphNode *shared_node = groove__ivm__graph__IvmGraph__node(self->graph, shared_id);
    if (!shared_node) return false;
    *shared_output = groove_NodeOutput_records(shared_node->descriptor.output);
    *barrier = current->id;
    *shared = shared_id;
    return true;
}
/* Register the binding's route filter as a barrier: a shared-node write activates
 * this binding only when the delta carries its route key. */
static void value_type_drop_v(void *p) { groove__records__values__ValueType__drop(p); }
static void value_drop_v(void *p) { groove__records__values__Value__drop(p); }
static bool register_route_barrier(groove_IvmRuntime *self, const groove_RoutedMultisinkTerminal *terminal, const groove_CompiledNode *shared, const groove_CompiledNode *bound, const groove_Value *binding_values, u32 nvalues, groove_NodeId *out) {
    if (terminal->route_fields.len == 0) return false;
    groove_NodeId barrier, shared_node; groove_RecordDescriptor shared_output;
    if (terminal->graph.tag == GROOVE_GB_COLLECT_BY) {
        if (!collector_route_filter(self, bound->node, &barrier, &shared_node, &shared_output)) return false;
    } else {
        if (!flat_route_filter(self, shared->node, bound->node, &barrier)) return false;
        shared_node = shared->node; shared_output = shared->output;
    }
    groove_UsizeVec field_indices = {0};
    groove_ValueTypeVec field_types = {0};
    groove_ValueVec values = {0};
    bool ok = true;
    u32 n = terminal->route_fields.len < terminal->route_value_indices.len ? terminal->route_fields.len : terminal->route_value_indices.len;
    for (u32 i = 0; ok && i < n; i++) {
        usize index;
        if (!groove__records__RecordDescriptor__field_index(shared_output, terminal->route_fields.ptr[i].ptr, terminal->route_fields.ptr[i].len, &index)) { ok = false; break; }
        const groove_ValueType *vt = &shared_output->fields.ptr[index].value_type;
        if (!groove__ivm__routes__is_routable_type(vt)) { ok = false; break; }
        usize value_index = terminal->route_value_indices.ptr[i];
        if (value_index >= nvalues) { ok = false; break; }
        jz_vec_push(&field_indices, &index, sizeof index);
        groove_ValueType t; groove__records__values__ValueType__clone(&t, vt); jz_vec_push(&field_types, &t, sizeof t);
        groove_Value v; groove__records__values__Value__clone(&v, &binding_values[value_index]); jz_vec_push(&values, &v, sizeof v);
    }
    jz_bytes key = {0};
    if (ok) ok = groove__ivm__routes__encode_route_key(values.ptr, values.len, field_types.ptr, field_types.len, &key);
    jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
    /* One table keys one shared node's delta by one field list. */
    if (ok) {
        const groove_RouteTable *table = groove__ivm__routes__RouteIndex__table(groove__ivm__graph__IvmGraph__routes(self->graph), shared_node);
        if (table && (table->field_indices.len != field_indices.len
                || (field_indices.len && memcmp(table->field_indices.ptr, field_indices.ptr, field_indices.len * sizeof(usize)) != 0))) {
            ok = false;
            groove_ivm_rt_a_drop_bytes(&key);
        }
    }
    if (!ok) {
        jz_vec_drop(&field_indices, sizeof(usize), NULL);
        jz_vec_drop(&field_types, sizeof(groove_ValueType), value_type_drop_v);
        return false;
    }
    /* A routed TopBy collects windows for its bound outputs before they are known to be touched. */
    bool has_root_ordering = bound->has_root_ordering_node;
    if (has_root_ordering) {
        bool consumes = true;
        jz_err e = groove__ivm__runtime__record_projection__output_consumes_root_positions(self->graph, bound->node, bound->root_ordering_node, &consumes);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); consumes = true; }
        has_root_ordering = consumes;
    }
    groove__ivm__graph__IvmGraph__add_route_barrier(self->graph, shared_node, barrier, field_indices, field_types, key, has_root_ordering, bound->root_ordering_node);
    *out = barrier;
    return true;
}

/* ---------------- subscription index / teardown ---------------- */
void SUBS(index_subscription_outputs)(groove_IvmRuntime *self, groove_SubscriptionId subscription_id, const jz_btree *outputs) {
    jz_str *sink; groove_CompiledNode *output;
    JZ_BTREE_FOR(outputs, sink, output) {
        bool inserted;
        jz_hmap *subscriptions = jz_hmap_entry(&self->subscriptions_by_output_node, &output->node, &inserted);
        if (inserted) jz_hmap_init(subscriptions, &groove_ivm_rt_a_hdesc_subscription_id_set);
        jz_hmap_insert(subscriptions, &subscription_id, NULL, NULL);
    }
}
void SUBS(unindex_subscription_outputs)(groove_IvmRuntime *self, groove_SubscriptionId subscription_id, const jz_btree *outputs) {
    jz_str *sink; groove_CompiledNode *output;
    JZ_BTREE_FOR(outputs, sink, output) {
        jz_hmap *subscriptions = jz_hmap_get(&self->subscriptions_by_output_node, &output->node);
        bool remove_node = false;
        if (subscriptions) {
            groove_SubscriptionId k;
            jz_hmap_remove(subscriptions, &subscription_id, &k, NULL);
            remove_node = subscriptions->len == 0;
        }
        if (remove_node) {
            groove_NodeId k; jz_hmap set;
            if (jz_hmap_remove(&self->subscriptions_by_output_node, &output->node, &k, &set)) jz_hmap_drop(&set);
        }
    }
}

static void release_route_barriers(groove_IvmRuntime *self, const groove_MultisinkSubscriptionTarget *target) {
    if (target->tag != GROOVE_MST_ROUTED_SHAPE) return;
    for (u32 i = 0; i < target->route_barriers.len; i++)
        groove__ivm__graph__IvmGraph__release_route_barrier(self->graph, target->route_barriers.ptr[i]);
}
static bool remove_binding_ref(groove_IvmRuntime *self, groove_PreparedShapeId shape_id, const groove_BindingKey *binding, groove_BindingDelta *out) {
    jz_str shape;
    jz_err e = binding_source_shape_name(self, shape_id, &shape);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    groove_BindingSourceKey key = prepared_key(&shape);
    jz_str_drop(&shape);
    bool some;
    remove_binding_ref_for_source(self, key, binding, &some, out);
    return some;
}

bool SUBS(unsubscribe)(groove_IvmRuntime *self, groove_SubscriptionId subscription_id) {
    groove_SubscriptionId k; groove_MultisinkSubscriptionState subscription;
    if (!jz_hmap_remove(&self->multisink_subscriptions, &subscription_id, &k, &subscription)) return false;
    SUBS(unindex_subscription_outputs)(self, subscription_id, &subscription.outputs);
    RTK(cancel_pending_subscription_hydration)(self, subscription_id);
    bool removed = groove__ivm__runtime__graph_lifecycle__IvmRuntime__remove_multisink_retainers(self, subscription_id, &subscription.outputs);
    release_route_barriers(self, &subscription.target);
    if (subscription.target.tag == GROOVE_MST_ROUTED_SHAPE) {
        groove_PreparedShapeId shape_id = subscription.target.shape_id;
        groove_BindingDelta param_delta;
        if (remove_binding_ref(self, shape_id, &subscription.target.binding_key, &param_delta)) {
            if (param_delta.deltas.len) {
                jz_vec_push(&self->pending_binding_retractions, &param_delta, sizeof param_delta);
                remove_unreferenced_auto_family(self, shape_id);
            } else {
                SB(BindingDelta__drop)(&param_delta);
            }
        }
        remove_unreferenced_binding_owned_shape(self, shape_id);
    }
    SB(MultisinkSubscriptionState__drop)(&subscription);
    return removed;
}

jz_err SUBS(unsubscribe_with_storage)(groove_IvmRuntime *self, groove_SubscriptionId subscription_id, groove_OrderedKvStorage *storage, bool *out) {
    groove_SubscriptionId k; groove_MultisinkSubscriptionState subscription;
    if (!jz_hmap_remove(&self->multisink_subscriptions, &subscription_id, &k, &subscription)) { *out = false; return JZ_OK; }
    SUBS(unindex_subscription_outputs)(self, subscription_id, &subscription.outputs);
    RTK(cancel_pending_subscription_hydration)(self, subscription_id);
    bool removed = groove__ivm__runtime__graph_lifecycle__IvmRuntime__remove_multisink_retainers(self, subscription_id, &subscription.outputs);
    release_route_barriers(self, &subscription.target);
    jz_err e = JZ_OK;
    if (subscription.target.tag == GROOVE_MST_ROUTED_SHAPE) {
        groove_PreparedShapeId shape_id = subscription.target.shape_id;
        groove_BindingDelta retraction;
        bool has_retraction = remove_binding_ref(self, shape_id, &subscription.target.binding_key, &retraction);
        if (has_retraction && retraction.deltas.len == 0) { SB(BindingDelta__drop)(&retraction); has_retraction = false; }
        if (has_retraction) jz_vec_push(&self->pending_binding_retractions, &retraction, sizeof retraction);
        /* No removed receiver remains to own this cleanup across a cold tick. */
        remove_unreferenced_binding_owned_shape(self, shape_id);
        if (has_retraction) {
            e = RTK(flush_pending_binding_retractions)(self, storage);
            /* Auto families also remove their source, after its required tick. */
            if (!JZ_IS_ERR(e)) remove_unreferenced_auto_family(self, shape_id);
        }
    }
    SB(MultisinkSubscriptionState__drop)(&subscription);
    if (JZ_IS_ERR(e)) return e;
    *out = removed;
    return JZ_OK;
}

/* Remove a caller-owned prepared shape after all of its bindings have been unsubscribed. */
jz_err SUBS(retire_prepared_shape)(groove_IvmRuntime *self, groove_PreparedShapeId shape_id) {
    if (any_subscription_targets(self, shape_id))
        return jz_err_fmt(GROOVE_IVM_ERR_PREPARED_SHAPE_HAS_ACTIVE_BINDINGS, "cannot retire prepared shape PreparedShapeId(%llu) while it has active bindings", (unsigned long long)shape_id.v);
    groove_PreparedShapeId k; groove_RoutedMultisinkShapeState shape;
    if (!jz_hmap_remove(&self->prepared_shapes, &shape_id, &k, &shape)) return err_prepared_shape_not_found(shape_id);
    if (shape.has_shared_key) {
        const groove_PreparedShapeId *current = jz_hmap_get(&self->shared_prepared_shapes, &shape.shared_key);
        if (current && current->v == shape_id.v) {
            groove_SharedShapeKey ok; groove_PreparedShapeId ov;
            if (jz_hmap_remove(&self->shared_prepared_shapes, &shape.shared_key, &ok, &ov)) SB(SharedShapeKey__drop)(&ok);
        }
    }
    {
        jz_str *sink; groove_RoutedMultisinkTerminalState *terminal;
        JZ_BTREE_FOR(&shape.terminals, sink, terminal) {
            groove_Retainer r = prepared_shape_retainer(shape_id);
            groove__ivm__runtime__graph_lifecycle__IvmRuntime__remove_retainer(self, terminal->output.node, &r);
            jz_str_drop(&r.key);
        }
    }
    groove__ivm__runtime__graph_lifecycle__IvmRuntime__collect_unretained_ephemeral_nodes(self);
    bool shared_by_other = false;
    {
        groove_PreparedShapeId *id; groove_RoutedMultisinkShapeState *other;
        JZ_HMAP_FOR(&self->prepared_shapes, id, other) if (jz_str_eq(&other->shape, &shape.shape)) { shared_by_other = true; break; }
    }
    if (!shared_by_other) {
        groove_BindingSourceKey key = prepared_key(&shape.shape);
        const groove_BindingSourceState *source = SB(BindingSources__get)(&self->binding_sources, &key);
        if (source && source->refcounts.len == 0) SB(BindingSources__remove)(&self->binding_sources, &key, NULL);
        groove__ivm__op_types__BindingSourceKey__drop(&key);
    }
    SB(RoutedMultisinkShapeState__drop)(&shape);
    return JZ_OK;
}

jz_err SUBS(prune_dropped_subscriptions_with_storage)(groove_IvmRuntime *self, groove_OrderedKvStorage *storage, usize *out) {
    JZ_VEC(groove_SubscriptionId) dropped = {0};
    groove_SubscriptionId *id; groove_MultisinkSubscriptionState *state;
    JZ_HMAP_FOR(&self->multisink_subscriptions, id, state)
        if (!groove_LivenessCell_upgrade(state->receiver_liveness)) jz_vec_push(&dropped, id, sizeof *id);
    for (u32 i = 0; i < dropped.len; i++) {
        bool removed;
        jz_err e = SUBS(unsubscribe_with_storage)(self, dropped.ptr[i], storage, &removed);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&dropped, sizeof(groove_SubscriptionId), NULL); return e; }
    }
    *out = dropped.len;
    jz_vec_drop(&dropped, sizeof(groove_SubscriptionId), NULL);
    return JZ_OK;
}

jz_err SUBS(add_dedup_schema_indices)(groove_IvmRuntime *self) {
    /* Rust iterates a clone of schema.tables (borrowck); indexing by position is equivalent. */
    for (u32 t = 0; t < self->schema.tables.len; t++) {
        const groove_TableSchema *table = &self->schema.tables.ptr[t];
        for (u32 i = 0; i < table->indices.len; i++)
            JZ_TRY(groove__ivm__runtime__compilation__IvmRuntime__add_dedup_schema_index(self, table, &table->indices.ptr[i]));
    }
    return JZ_OK;
}

static const groove_CompiledNode *single_output(const groove_IvmRuntime *self, groove_SubscriptionId id) {
    const groove_MultisinkSubscriptionState *subscription = jz_hmap_get(&self->multisink_subscriptions, &id);
    if (!subscription || subscription->outputs.len != 1) return NULL;
    jz_str *sink; groove_CompiledNode *output;
    if (!jz_btree_first(&subscription->outputs, (void **)&sink, (void **)&output)) return NULL;
    return output;
}
bool SUBS(subscription_output_node)(const groove_IvmRuntime *self, groove_SubscriptionId id, groove_NodeId *out) {
    const groove_CompiledNode *output = single_output(self, id);
    if (!output) return false;
    *out = output->node;
    return true;
}
const groove_RecordDescriptor *SUBS(subscription_output)(const groove_IvmRuntime *self, groove_SubscriptionId id) {
    const groove_CompiledNode *output = single_output(self, id);
    return output ? &output->output : NULL;
}

/* ---------------- descriptor inference ---------------- */
jz_err SUBS(infer_builder_output)(const groove_IvmRuntime *self, const groove_GraphBuilder *graph, groove_RecordDescriptor *out) {
    jz_hmap output_memo; jz_hmap_init(&output_memo, &hdesc_ptr_desc);
    jz_err e = SUBS(infer_builder_output_cached)(self, graph, &output_memo, out);
    jz_hmap_drop(&output_memo);
    return e;
}
jz_err SUBS(infer_builder_output_cached)(const groove_IvmRuntime *self, const groove_GraphBuilder *graph, jz_hmap *output_memo, groove_RecordDescriptor *out) {
    const groove_RecordDescriptor *cached = jz_hmap_get(output_memo, &graph);
    if (cached) { *out = *cached; return JZ_OK; }
    /* Infer every child before its parent explicitly so deep policy graphs do not consume the stack. */
    groove_GraphBuilderRefVec order = {0};
    groove__ivm__graph__GraphBuilder__postorder(graph, &order);
    for (u32 i = 0; i < order.len; i++) {
        const groove_GraphBuilder *builder = order.ptr[i];
        if (jz_hmap_get(output_memo, &builder)) continue;
        groove_RecordDescriptor output;
        jz_err e = SUBS(infer_builder_output_uncached)(self, builder, output_memo, &output);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&order, sizeof(void *), NULL); return e; }
        jz_hmap_insert(output_memo, &builder, &output, NULL);
    }
    jz_vec_drop(&order, sizeof(void *), NULL);
    cached = jz_hmap_get(output_memo, &graph);
    if (!cached) return groove_err_unsupported_operator();
    *out = *cached;
    return JZ_OK;
}

#define INFER(child, dst) JZ_TRY(SUBS(infer_builder_output_cached)(self, (child), output_memo, (dst)))
jz_err SUBS(infer_builder_output_uncached)(const groove_IvmRuntime *self, const groove_GraphBuilder *graph, jz_hmap *output_memo, groove_RecordDescriptor *out) {
    groove_RecordDescriptor input;
    switch (graph->tag) {
    case GROOVE_GB_TYPED_TEMPLATE: {
        const groove_TypedGraphTemplate *program = graph->program;
        if (graph->predicates.len != groove__ivm__template__TypedGraphTemplate__predicate_marker_count(program)) return groove_err_output_mismatch();
        if (graph->inputs.len != groove__ivm__template__TypedGraphTemplate__input_count(program)) return groove_err_output_mismatch();
        for (u32 i = 0; i < graph->inputs.len; i++) {
            groove_RecordDescriptor actual;
            INFER(graph->inputs.ptr[i], &actual);
            if (actual != groove__ivm__template__TypedGraphTemplate__input(program, i)) return groove_err_output_mismatch();
        }
        *out = groove__ivm__template__TypedGraphTemplate__output(program);
        return JZ_OK;
    }
    case GROOVE_GB_TEMPLATE_INPUT: {
        if (!graph->input) return groove_err_unsupported_operator();
        groove_RecordDescriptor actual;
        INFER(graph->input, &actual);
        if (actual != graph->output) return groove_err_output_mismatch();
        *out = graph->output;
        return JZ_OK;
    }
    case GROOVE_GB_TABLE:
        return table_source_output(self, &graph->table, graph->has_variant_projection, &graph->variant_projection, out);
    case GROOVE_GB_LOOKUP: {
        groove_RecordDescriptor keys;
        INFER(graph->input, &keys);
        return table_source_output(self, &graph->table, graph->has_variant_projection, &graph->variant_projection, out);
    }
    case GROOVE_GB_INLINE_RECORDS: case GROOVE_GB_INPUT_SOURCE:
    case GROOVE_GB_FRONTIER_SOURCE: case GROOVE_GB_BINDING_SOURCE:
        *out = graph->output;
        return JZ_OK;
    case GROOVE_GB_INDEX: {
        if (graph->has_variant_projection) {
            groove_VariantProjectionKey key = { graph->table, { GROOVE_VPT_NAMED, graph->variant_projection } };
            const groove_VariantProjection *projection = jz_hmap_get(&self->variant_projections, &key);
            if (!projection)
                return jz_err_fmt(GROOVE_IVM_ERR_VARIANT_PROJECTION_NOT_FOUND, "variant projection not found: %.*s.%.*s",
                    (int)graph->table.len, graph->table.ptr, (int)graph->variant_projection.len, graph->variant_projection.ptr);
            *out = projection->output;
            return JZ_OK;
        }
        *out = groove__ivm__runtime__record_projection__index_record_descriptor();
        return JZ_OK;
    }
    case GROOVE_GB_FILTER: case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY: case GROOVE_GB_TOP_BY:
        return SUBS(infer_builder_output_cached)(self, graph->input, output_memo, out);
    case GROOVE_GB_COLLECT_BY: {
        INFER(graph->input, &input);
        const groove_CollectByBuilder *collect = graph->collect;
        switch (collect->mode) {
        case GROOVE_CBM_ROOT:
            return groove__ivm__runtime__record_projection__collect_by_root_descriptor(input, collect->parent_fields.ptr, collect->parent_fields.len, out);
        case GROOVE_CBM_COLLECT:
            if (collect->slots.len == 0)
                return groove__ivm__runtime__record_projection__collect_by_descriptor(input, collect->parent_fields.ptr, collect->parent_fields.len,
                    collect->child_fields.ptr, collect->child_fields.len, &collect->collection_field, out);
            return groove__ivm__runtime__record_projection__collect_by_tree_descriptor(input, collect->parent_fields.ptr, collect->parent_fields.len,
                collect->slots.ptr, collect->slots.len, out);
        default:
            if (collect->slots.len == 0)
                return groove__ivm__runtime__record_projection__collect_by_expand_descriptor(input, collect->tuple_fields.ptr, collect->tuple_fields.len, out);
            return groove_err_invalid_collect_by("expand mode does not accept recursive collection slots");
        }
    }
    case GROOVE_GB_AGGREGATE:
        INFER(graph->input, &input);
        return groove__ivm__runtime__record_projection__aggregate_descriptor(input, graph->group_cols.ptr, graph->group_cols.len, graph->aggregates.ptr, graph->aggregates.len, out);
    case GROOVE_GB_UNWRAP_NULLABLE: {
        INFER(graph->input, &input);
        usize field_idx;
        JZ_TRY(groove__ivm__runtime__record_projection__resolve_field_ref(input, &graph->field, &field_idx));
        return groove__ivm__runtime__record_projection__unwrap_nullable_descriptor(input, field_idx, out);
    }
    case GROOVE_GB_UNNEST: {
        INFER(graph->input, &input);
        usize field_idx;
        JZ_TRY(groove__ivm__runtime__record_projection__resolve_field_ref(input, &graph->field, &field_idx));
        return groove__ivm__runtime__record_projection__unnest_descriptor(input, field_idx, &graph->element_field, out);
    }
    case GROOVE_GB_VARIANT_PROJECT:
        INFER(graph->input, &input);
        return groove__ivm__runtime__record_projection__variant_project_descriptor(input, &graph->field, &graph->case_name, out);
    case GROOVE_GB_PROJECT: {
        INFER(graph->input, &input);
        groove_ProjectionPlan *plan;
        JZ_TRY(groove__ivm__runtime__typed_projection__IvmRuntime__projection_plan((groove_IvmRuntime *)self, input, graph->fields.ptr, graph->fields.len, &plan));
        *out = groove__ivm__runtime__typed_projection__ProjectionPlan__output(plan);
        groove__ivm__runtime__typed_projection__ProjectionPlan__release(plan);
        return JZ_OK;
    }
    case GROOVE_GB_STREAMING_CHECKSUM: {
        if (graph->window_bytes == 0 || graph->max_bytes_per_turn == 0)
            return jz_err_new(GROOVE_IVM_ERR_INVALID_STREAMING_CHECKSUM_BUDGET, "streaming checksum window and work budgets must be non-zero");
        INFER(graph->input, &input);
        usize field_idx;
        JZ_TRY(groove__ivm__runtime__record_projection__resolve_field_ref(input, &graph->field, &field_idx));
        u32 tag = input->fields.ptr[field_idx].value_type.tag;
        if (tag != GROOVE_VT_STRING && tag != GROOVE_VT_BYTES)
            return jz_err_new(GROOVE_IVM_ERR_STREAMING_CHECKSUM_TYPE_MISMATCH, "streaming checksum requires a String or Bytes field");
        JZ_VEC(groove_NamedType) fields = {0};
        for (u32 index = 0; index < input->fields.len; index++) {
            const groove_DescriptorField *descriptor_field = &input->fields.ptr[index];
            groove_NamedType nt;
            if (index == field_idx) {
                nt.name = jz_str_clone(&graph->output_field);
                memset(&nt.value_type, 0, sizeof nt.value_type); nt.value_type.tag = GROOVE_VT_BYTES;
            } else {
                nt.name = descriptor_field->has_name ? jz_str_clone(&descriptor_field->name) : (jz_str){0};
                groove__records__values__ValueType__clone(&nt.value_type, &descriptor_field->value_type);
            }
            jz_vec_push(&fields, &nt, sizeof nt);
        }
        *out = groove__records__RecordDescriptor__new(fields.ptr, fields.len);
        jz_free(fields.ptr);
        return JZ_OK;
    }
    case GROOVE_GB_UNION: {
        bool has_output = false; groove_RecordDescriptor output = NULL;
        for (u32 i = 0; i < graph->inputs.len; i++) {
            groove_RecordDescriptor next;
            INFER(graph->inputs.ptr[i], &next);
            if (has_output) {
                if (!groove__records__RecordDescriptor__registry_compatible_with(output, next)) return groove_err_output_mismatch();
            } else {
                output = next; has_output = true;
            }
        }
        *out = has_output ? output : groove__records__RecordDescriptor__default();
        return JZ_OK;
    }
    case GROOVE_GB_JOIN: {
        groove_RecordDescriptor left, right;
        INFER(graph->left, &left);
        INFER(graph->right, &right);
        *out = groove__ivm__runtime__key_encoding__join_descriptor(left, right);
        return JZ_OK;
    }
    case GROOVE_GB_SEMI_JOIN: case GROOVE_GB_ANTI_JOIN:
        return SUBS(infer_builder_output_cached)(self, graph->left, output_memo, out);
    case GROOVE_GB_RECURSIVE: {
        groove_RecordDescriptor seed, step;
        INFER(graph->seed, &seed);
        INFER(graph->step, &step);
        if (!groove__records__RecordDescriptor__registry_compatible_with(seed, step)) return groove_err_output_mismatch();
        *out = seed;
        return JZ_OK;
    }
    case GROOVE_GB_RECURSIVE_STEP_WITNESS: {
        const groove_GraphBuilder *recursive = graph->input;
        if (recursive->tag != GROOVE_GB_RECURSIVE || !recursive->step_witness) return groove_err_unsupported_operator();
        return SUBS(infer_builder_output_cached)(self, recursive->step_witness, output_memo, out);
    }
    }
    return groove_err_unsupported_operator();
}
#undef INFER

/* The output of a table source, plain or through a named variant projection. */
static jz_err table_source_output(const groove_IvmRuntime *self, const jz_str *table, bool has_vp, const jz_str *variant_projection, groove_RecordDescriptor *out) {
    if (has_vp) {
        groove_VariantProjectionKey key = { *table, { GROOVE_VPT_NAMED, *variant_projection } };
        const groove_VariantProjection *projection = jz_hmap_get(&self->variant_projections, &key);
        if (!projection)
            return jz_err_fmt(GROOVE_IVM_ERR_VARIANT_PROJECTION_NOT_FOUND, "variant projection not found: %.*s.%.*s",
                (int)table->len, table->ptr, (int)variant_projection->len, variant_projection->ptr);
        *out = projection->output;
        return JZ_OK;
    }
    const groove_TableSchema *table_schema = groove__schema__DatabaseSchema__table(&self->schema, table->ptr, table->len);
    if (!table_schema) return groove_err_table_not_found(table);
    if (groove__schema__TableSchema__has_variants(table_schema))
        return jz_err_fmt(GROOVE_IVM_ERR_VARIANT_PROJECTION_REQUIRED, "schema-variant table requires a fixed-output projection: %.*s", (int)table->len, table->ptr);
    /* The runtime keeps this descriptor current with the schema. */
    const groove_RecordDescriptor *descriptor = jz_hmap_get(&self->table_descriptors, table);
    *out = descriptor ? *descriptor : groove__schema__TableSchema__record_schema(table_schema);
    return JZ_OK;
}

groove_SubscriptionId SUBS(next_subscription_id)(groove_IvmRuntime *self) {
    groove_SubscriptionId id = { self->input_source_runtime_namespace, self->next_subscription_id };
    self->next_subscription_id += 1;
    return id;
}
groove_PreparedShapeId SUBS(next_shape_id)(groove_IvmRuntime *self) {
    groove_PreparedShapeId id = { self->next_shape_id };
    self->next_shape_id += 1;
    return id;
}

/* ---------------- binding references ---------------- */
static jz_err add_binding_ref_for_shape(groove_IvmRuntime *self, const jz_str *shape, groove_BindingKey binding, groove_BindingDelta *out) {
    groove_BindingSourceKey key = prepared_key(shape);
    groove_BindingSourceState *source = SB(BindingSources__get_mut)(&self->binding_sources, &key);
    if (!source) { groove__ivm__op_types__BindingSourceKey__drop(&key); drop_binding_key(&binding); return err_binding_source_not_found(shape); }
    groove_BindingKey entry_key; groove_ivm_rt_a_clone_bytes(&entry_key.bytes, &binding.bytes);
    bool inserted;
    usize *count = jz_hmap_entry(&source->refcounts, &entry_key, &inserted);
    if (!inserted) drop_binding_key(&entry_key);
    *count += 1;
    out->key = key;
    out->descriptor = source->descriptor;
    memset(&out->deltas, 0, sizeof out->deltas);
    if (*count == 1) {
        groove_RecordDelta d = { binding.bytes, 1 };
        jz_vec_push(&out->deltas, &d, sizeof d);
    } else {
        drop_binding_key(&binding);
    }
    out->initializes_snapshot = false;
    return JZ_OK;
}
static jz_err add_binding_ref(groove_IvmRuntime *self, groove_PreparedShapeId shape_id, groove_BindingKey binding, groove_BindingDelta *out) {
    jz_str shape;
    jz_err e = binding_source_shape_name(self, shape_id, &shape);
    if (JZ_IS_ERR(e)) { drop_binding_key(&binding); return e; }
    e = add_binding_ref_for_shape(self, &shape, binding, out);
    jz_str_drop(&shape);
    return e;
}

typedef struct CancelCtx { const groove_BindingKey *binding; bool *cancelled; } CancelCtx;
static bool keep_uncancelled(void *item, void *ctx) {
    const groove_RecordDelta *delta = item; CancelCtx *c = ctx;
    bool matches = delta->weight < 0 && groove_ivm_rt_a_eq_bytes(&delta->record, &c->binding->bytes);
    *c->cancelled |= matches;
    return !matches;
}
static bool keep_nonempty_delta(void *item, void *ctx) { (void)ctx; return ((groove_BindingDelta *)item)->deltas.len != 0; }
/* Reacquiring the last reference before its queued retraction is evaluated
 * leaves the evaluator's binding resident: cancel that transition. */
bool SB(IvmRuntime__cancel_pending_binding_retraction)(groove_IvmRuntime *self, const jz_str *shape, const groove_BindingKey *binding) {
    bool cancelled = false;
    groove_BindingSourceKey key = prepared_key(shape);
    for (u32 i = 0; i < self->pending_binding_retractions.len; i++) {
        groove_BindingDelta *pending = &self->pending_binding_retractions.ptr[i];
        if (!groove__ivm__op_types__BindingSourceKey__eq(&pending->key, &key)) continue;
        CancelCtx ctx = { binding, &cancelled };
        jz_vec_retain(&pending->deltas, sizeof(groove_RecordDelta), keep_uncancelled, &ctx, SB(RecordDelta__drop));
    }
    groove__ivm__op_types__BindingSourceKey__drop(&key);
    jz_vec_retain(&self->pending_binding_retractions, sizeof(groove_BindingDelta), keep_nonempty_delta, NULL, SB(BindingDelta__drop));
    return cancelled;
}

jz_err SB(IvmRuntime__provisional_binding_delta)(const groove_IvmRuntime *self, groove_PreparedShapeId shape_id, const groove_BindingKey *binding, groove_BindingDelta *out) {
    jz_str shape;
    JZ_TRY(binding_source_shape_name(self, shape_id, &shape));
    groove_BindingSourceKey key = prepared_key(&shape);
    const groove_BindingSourceState *source = SB(BindingSources__get)(&self->binding_sources, &key);
    if (!source) {
        jz_err e = err_binding_source_not_found(&shape);
        groove__ivm__op_types__BindingSourceKey__drop(&key); jz_str_drop(&shape);
        return e;
    }
    jz_str_drop(&shape);
    out->key = key;
    out->descriptor = source->descriptor;
    memset(&out->deltas, 0, sizeof out->deltas);
    if (!jz_hmap_get(&source->refcounts, binding)) push_record_delta(&out->deltas, &binding->bytes, 1);
    out->initializes_snapshot = false;
    return JZ_OK;
}

static bool remove_binding_metadata(groove_IvmRuntime *self, const groove_BindingSourceKey *source_key, const groove_BindingKey *binding, groove_RecordDescriptor *descriptor, bool *last_reference) {
    groove_BindingSourceState *source = SB(BindingSources__get_mut)(&self->binding_sources, source_key);
    if (!source) return false;
    usize *count = jz_hmap_get(&source->refcounts, binding);
    if (!count) return false;
    *count -= 1;
    *last_reference = *count == 0;
    if (*last_reference) {
        groove_BindingKey k; usize v;
        if (jz_hmap_remove(&source->refcounts, binding, &k, &v)) drop_binding_key(&k);
    }
    *descriptor = source->descriptor;
    return true;
}
static jz_err remove_binding_ref_for_source(groove_IvmRuntime *self, groove_BindingSourceKey source_key, const groove_BindingKey *binding, bool *some, groove_BindingDelta *out) {
    groove_RecordDescriptor descriptor; bool last_reference;
    if (!remove_binding_metadata(self, &source_key, binding, &descriptor, &last_reference)) {
        groove__ivm__op_types__BindingSourceKey__drop(&source_key);
        *some = false;
        return JZ_OK;
    }
    out->key = source_key;
    out->descriptor = descriptor;
    memset(&out->deltas, 0, sizeof out->deltas);
    if (last_reference) push_record_delta(&out->deltas, &binding->bytes, -1);
    out->initializes_snapshot = false;
    *some = true;
    return JZ_OK;
}

static jz_err binding_source_shape_name(const groove_IvmRuntime *self, groove_PreparedShapeId shape_id, jz_str *out) {
    const groove_RoutedMultisinkShapeState *shape = jz_hmap_get(&self->prepared_shapes, &shape_id);
    if (shape) { *out = jz_str_clone(&shape->shape); return JZ_OK; }
    return err_prepared_shape_not_found(shape_id);
}

groove_BindingSnapshots *SUBS(binding_snapshot_deltas)(groove_IvmRuntime *self) {
    return SB(BindingSources__snapshot)(&self->binding_sources);
}

/* Retire a binding-owned shape once no retained binding targets it. */
static void remove_unreferenced_binding_owned_shape(groove_IvmRuntime *self, groove_PreparedShapeId shape_id) {
    const groove_RoutedMultisinkShapeState *shape = jz_hmap_get(&self->prepared_shapes, &shape_id);
    if (!shape || !shape->retire_when_unbound) return;
    if (any_subscription_targets(self, shape_id)) return;
    jz_err e = SUBS(retire_prepared_shape)(self, shape_id);
    jz_err_drop(&e);
}
static void remove_unreferenced_auto_family(groove_IvmRuntime *self, groove_PreparedShapeId shape_id) {
    const groove_RoutedMultisinkShapeState *shape = jz_hmap_get(&self->prepared_shapes, &shape_id);
    if (!shape || !shape->has_auto_family_key) return;
    if (any_subscription_targets(self, shape_id)) return;
    groove_AutoDirectFamilyKey key; SB(AutoDirectFamilyKey__clone)(&key, &shape->auto_family_key);
    jz_str shape_name = jz_str_clone(&shape->shape);
    groove_NodeIdVec output_nodes = {0};
    {
        jz_str *sink; groove_RoutedMultisinkTerminalState *terminal;
        JZ_BTREE_FOR(&shape->terminals, sink, terminal) jz_vec_push(&output_nodes, &terminal->output.node, sizeof(groove_NodeId));
    }
    groove_PreparedShapeId k; groove_RoutedMultisinkShapeState removed;
    if (jz_hmap_remove(&self->prepared_shapes, &shape_id, &k, &removed)) SB(RoutedMultisinkShapeState__drop)(&removed);
    groove_BindingSourceKey source_key = prepared_key(&shape_name);
    SB(BindingSources__remove)(&self->binding_sources, &source_key, NULL);
    groove__ivm__op_types__BindingSourceKey__drop(&source_key);
    jz_str_drop(&shape_name);
    groove_AutoDirectFamilyKey ok; groove_PreparedShapeId ov;
    if (jz_hmap_remove(&self->auto_direct_families, &key, &ok, &ov)) SB(AutoDirectFamilyKey__drop)(&ok);
    SB(AutoDirectFamilyKey__drop)(&key);
    for (u32 i = 0; i < output_nodes.len; i++) {
        groove_Retainer r = prepared_shape_retainer(shape_id);
        groove__ivm__runtime__graph_lifecycle__IvmRuntime__remove_retainer(self, output_nodes.ptr[i], &r);
        jz_str_drop(&r.key);
    }
    jz_vec_drop(&output_nodes, sizeof(groove_NodeId), NULL);
    groove__ivm__runtime__graph_lifecycle__IvmRuntime__collect_unretained_ephemeral_nodes(self);
}

/* ---------------- live attach ---------------- */
/* Owns a live binding reference until its asynchronous admission is handed off. */
typedef struct LiveAttachAdmission {
    groove_IvmRuntime *runtime;
    bool has_binding; groove_BindingSourceKey source; groove_BindingKey binding;
    bool published;
} LiveAttachAdmission;
static void live_attach_admission_drop(LiveAttachAdmission *self) {
    if (!self->has_binding) return;
    self->has_binding = false;
    if (self->published) {
        bool some; groove_BindingDelta delta;
        remove_binding_ref_for_source(self->runtime, self->source, &self->binding, &some, &delta);
        if (some) {
            if (delta.deltas.len) jz_vec_push(&self->runtime->pending_binding_retractions, &delta, sizeof delta);
            else SB(BindingDelta__drop)(&delta);
        }
    } else {
        groove_RecordDescriptor d; bool last;
        remove_binding_metadata(self->runtime, &self->source, &self->binding, &d, &last);
        groove__ivm__op_types__BindingSourceKey__drop(&self->source);
    }
    drop_binding_key(&self->binding);
}
static void live_attach_admission_finish(LiveAttachAdmission *self, jz_hmap borrowed, groove_LiveAttach *out) {
    if (!self->has_binding) jz_panic("live admission retains its binding until handoff");
    self->has_binding = false;
    out->binding_source = self->source;
    out->binding_key = self->binding;
    out->borrowed = borrowed;
}

static void collect_upstream(const groove_IvmRuntime *self, groove_NodeIdVec *pending, jz_hmap *closure) {
    groove_NodeId node;
    while (pending->len) {
        node = pending->ptr[--pending->len];
        if (jz_hmap_get(closure, &node)) continue;
        jz_hmap_insert(closure, &node, NULL, NULL);
        const groove_GraphNode *graph_node = groove__ivm__graph__IvmGraph__node(self->graph, node);
        if (graph_node) jz_vec_extend(pending, graph_node->descriptor.inputs.ptr, graph_node->descriptor.inputs.len, sizeof(groove_NodeId));
    }
}
static bool pending_retraction_for(const groove_IvmRuntime *self, const groove_BindingSourceKey *key) {
    for (u32 i = 0; i < self->pending_binding_retractions.len; i++)
        if (groove__ivm__op_types__BindingSourceKey__eq(&self->pending_binding_retractions.ptr[i].key, key)) return true;
    return false;
}

/* Drop the queued retractions of shape_id's binding source when the full
 * hydration this bind must take rebuilds everything they touch. */
typedef struct KeyNeCtx { const groove_BindingSourceKey *key; } KeyNeCtx;
static bool keep_other_source(void *item, void *ctx) { return !groove__ivm__op_types__BindingSourceKey__eq(&((groove_BindingDelta *)item)->key, ((KeyNeCtx *)ctx)->key); }
static jz_err absorb_unobserved_binding_retractions(groove_IvmRuntime *self, groove_PreparedShapeId shape_id) {
    const groove_RoutedMultisinkShapeState *shape = jz_hmap_get(&self->prepared_shapes, &shape_id);
    if (!shape) return err_prepared_shape_not_found(shape_id);
    groove_BindingSourceKey source_key = prepared_key(&shape->shape);
    bool keep_flush = !pending_retraction_for(self, &source_key) || RTK(has_pending_incremental)(self);
    if (!keep_flush) {
        const groove_BindingSourceState *source = SB(BindingSources__get)(&self->binding_sources, &source_key);
        keep_flush = source && source->refcounts.len != 0;
    }
    if (!keep_flush) {
        groove_SubscriptionId *id; groove_MultisinkSubscriptionState *subscription;
        JZ_HMAP_FOR(&self->multisink_subscriptions, id, subscription) {
            if (subscription->target.tag != GROOVE_MST_ROUTED_SHAPE) continue;
            const groove_RoutedMultisinkShapeState *target = jz_hmap_get(&self->prepared_shapes, &subscription->target.shape_id);
            if (!target || jz_str_eq(&target->shape, &shape->shape)) { keep_flush = true; break; }
        }
    }
    if (keep_flush) { groove__ivm__op_types__BindingSourceKey__drop(&source_key); return JZ_OK; }
    jz_hmap closure; jz_hmap_init(&closure, &groove_ivm_rt_a_hdesc_nodeid_set);
    groove_NodeIdVec pending = {0};
    {
        jz_str *sink; groove_RoutedMultisinkTerminalState *terminal;
        JZ_BTREE_FOR(&shape->terminals, sink, terminal) jz_vec_push(&pending, &terminal->output.node, sizeof(groove_NodeId));
    }
    collect_upstream(self, &pending, &closure);
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    groove_NodeIdVec affected = {0};
    groove__ivm__graph__IvmGraph__affected_nodes_through_routes(self->graph, NULL, 0, &source_key, 1, &affected);
    bool all_inside = true;
    for (u32 i = 0; all_inside && i < affected.len; i++) all_inside = jz_hmap_get(&closure, &affected.ptr[i]) != NULL;
    jz_vec_drop(&affected, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&closure);
    if (all_inside) {
        KeyNeCtx ctx = { &source_key };
        jz_vec_retain(&self->pending_binding_retractions, sizeof(groove_BindingDelta), keep_other_source, &ctx, SB(BindingDelta__drop));
    }
    groove__ivm__op_types__BindingSourceKey__drop(&source_key);
    return JZ_OK;
}

/* Whether any binding currently holds the prepared binding source named shape. */
bool SUBS(prepared_binding_source_is_bound)(const groove_IvmRuntime *self, const char *shape, u32 len) {
    groove_BindingSourceKey key; groove__ivm__op_types__BindingSourceKey__prepared(shape, len, &key);
    const groove_BindingSourceState *source = SB(BindingSources__get)(&self->binding_sources, &key);
    groove__ivm__op_types__BindingSourceKey__drop(&key);
    return source && source->refcounts.len != 0;
}

/* The shared nodes a live attach may borrow, or None when the shape is not
 * certainly maintained for a sibling binding. */
static jz_err live_attach_borrowed_nodes(const groove_IvmRuntime *self, groove_PreparedShapeId shape_id, const groove_Value *values, u32 n, bool *some, jz_hmap *out) {
    *some = false;
    const groove_RoutedMultisinkShapeState *shape = jz_hmap_get(&self->prepared_shapes, &shape_id);
    if (!shape) return err_prepared_shape_not_found(shape_id);
    groove_BindingKey binding_key = { {0} };
    JZ_TRY(groove__records__RecordDescriptor__create(shape->binding_descriptor, values, n, &binding_key.bytes));
    groove_BindingSourceKey source_key = prepared_key(&shape->shape);
    const groove_BindingSourceState *source = SB(BindingSources__get)(&self->binding_sources, &source_key);
    bool eligible = source != NULL;
    /* Only a new binding for a source that is already populated, with no
     * queued retraction and no dropped receiver the attach tick would discover. */
    if (eligible) eligible = !(source->refcounts.len == 0 || jz_hmap_get(&source->refcounts, &binding_key) != NULL
        || pending_retraction_for(self, &source_key) || RTK(has_pending_incremental)(self));
    if (eligible) {
        groove_SubscriptionId *id; groove_MultisinkSubscriptionState *subscription;
        JZ_HMAP_FOR(&self->multisink_subscriptions, id, subscription)
            if (!groove_LivenessCell_upgrade(subscription->receiver_liveness)) { eligible = false; break; }
    }
    groove__ivm__op_types__BindingSourceKey__drop(&source_key);
    drop_binding_key(&binding_key);
    if (!eligible) return JZ_OK;
    /* A retained sibling of the same terminals, whose hydration has completed, keeps every shared node current. */
    bool maintained = false;
    {
        groove_SubscriptionId *id; groove_MultisinkSubscriptionState *subscription;
        JZ_HMAP_FOR(&self->multisink_subscriptions, id, subscription) {
            if (subscription->target.tag != GROOVE_MST_ROUTED_SHAPE || subscription->failed) continue;
            const groove_RoutedMultisinkShapeState *sibling = jz_hmap_get(&self->prepared_shapes, &subscription->target.shape_id);
            if (!sibling || !jz_str_eq(&sibling->shape, &shape->shape) || sibling->terminals.len != shape->terminals.len) continue;
            bool all = true;
            jz_str *sink; groove_RoutedMultisinkTerminalState *terminal;
            JZ_BTREE_FOR(&shape->terminals, sink, terminal) {
                const groove_RoutedMultisinkTerminalState *other = jz_btree_get(&sibling->terminals, sink);
                if (!other || other->output.node.v != terminal->output.node.v) { all = false; break; }
            }
            if (all) { maintained = true; break; }
        }
    }
    if (!maintained) return JZ_OK;
    jz_hmap_init(out, &groove_ivm_rt_a_hdesc_nodeid_set);
    groove_NodeIdVec pending = {0};
    {
        jz_str *sink; groove_RoutedMultisinkTerminalState *terminal;
        JZ_BTREE_FOR(&shape->terminals, sink, terminal) jz_vec_push(&pending, &terminal->output.node, sizeof(groove_NodeId));
    }
    collect_upstream(self, &pending, out);
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    *some = true;
    return JZ_OK;
}

/* Attach a new binding to a prepared shape whose shared graph is already
 * maintained for sibling bindings: one ordinary binding tick admits it. */
jz_err SUBS(prepare_live_attach)(groove_IvmRuntime *self, groove_PreparedShapeId shape_id, const groove_Value *values, u32 n, groove_OrderedKvStorage *storage, bool *some, groove_LiveAttach *out) {
    *some = false;
    usize pruned;
    JZ_TRY(SUBS(prune_dropped_subscriptions_with_storage)(self, storage, &pruned));
    JZ_TRY(absorb_unobserved_binding_retractions(self, shape_id));
    JZ_TRY(RTK(flush_pending_binding_retractions)(self, storage));
    bool has_borrowed; jz_hmap borrowed;
    JZ_TRY(live_attach_borrowed_nodes(self, shape_id, values, n, &has_borrowed, &borrowed));
    if (!has_borrowed) return JZ_OK;
    jz_err e;
    const groove_RoutedMultisinkShapeState *shape = jz_hmap_get(&self->prepared_shapes, &shape_id);
    if (!shape) { jz_hmap_drop(&borrowed); return err_prepared_shape_not_found(shape_id); }
    groove_BindingKey binding_key = { {0} };
    e = groove__records__RecordDescriptor__create(shape->binding_descriptor, values, n, &binding_key.bytes);
    if (JZ_IS_ERR(e)) { jz_hmap_drop(&borrowed); return e; }
    groove_BindingKey ref_key; groove_ivm_rt_a_clone_bytes(&ref_key.bytes, &binding_key.bytes);
    groove_BindingDelta delta;
    e = add_binding_ref(self, shape_id, ref_key, &delta);
    if (JZ_IS_ERR(e)) { drop_binding_key(&binding_key); jz_hmap_drop(&borrowed); return e; }
    JZ_ASSERT(delta.deltas.len == 1, "a live attach admits a new binding");
    LiveAttachAdmission admission;
    admission.runtime = self;
    admission.has_binding = true;
    groove__ivm__op_types__BindingSourceKey__clone(&admission.source, &delta.key);
    admission.binding = binding_key;
    admission.published = false;
    groove_BindingDeltaVec deltas = {0};
    jz_vec_push(&deltas, &delta, sizeof delta);
    groove_TableDeltaVec none; empty_table_deltas(&none);
    groove_OptPublicationId no_publication = { false, {0} };
    groove_TickMetrics metrics;
    e = RTK(tick_with_params)(admission.runtime, none, deltas, owned_storage(storage), no_publication, &metrics);
    if (JZ_IS_ERR(e)) { live_attach_admission_drop(&admission); jz_hmap_drop(&borrowed); return e; }
    groove__ivm__runtime__state__TickMetrics__drop(&metrics);
    admission.published = true;
    /* A receiver dropped concurrently with the attach tick is discovered there
     * and its retraction queued: apply it before the new binding hydrates. */
    while (admission.runtime->pending_binding_retractions.len) {
        e = RTK(flush_pending_binding_retractions)(admission.runtime, storage);
        if (JZ_IS_ERR(e)) { live_attach_admission_drop(&admission); jz_hmap_drop(&borrowed); return e; }
    }
    admission.runtime->live_attaches += 1;
    live_attach_admission_finish(&admission, borrowed, out);
    *some = true;
    return JZ_OK;
}
