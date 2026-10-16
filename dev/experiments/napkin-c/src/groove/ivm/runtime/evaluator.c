/* groove::ivm::runtime::evaluator — per-tick evaluator state, memoization,
 * arrangements, and recursive execution. async fns are synchronous here; a
 * cooperative yield (Poll::Pending) surfaces as GROOVE_POLL_PENDING. */
#include "jz_rt.h"
#include "evaluator.h"
#include "record_projection.h"

#define EV(name) groove__ivm__runtime__evaluator__##name
#define TE(name) groove__ivm__runtime__evaluator__TickEvaluator__##name
#define CMAP(name) groove__ivm__runtime__counted_map__CountedMap__##name
#define UNARY_ROWS_PER_POLL 256u

typedef JZ_VEC(groove_TopBySortPart) groove_SortPartVec;

/* ---------------------------------------------------------------------
 * plan_expr_fields / plan_expr_names
 * ------------------------------------------------------------------- */
static bool plan_expr_field(const groove_PlanExpr *e) {
    switch (e->tag) {
    case GROOVE_PE_FIELD: case GROOVE_PE_NULLABLE: case GROOVE_PE_NULLABLE_FLAT:
    case GROOVE_PE_ENUM_TAG_REMAP: case GROOVE_PE_ENUM_REMAP: case GROOVE_PE_RECURSIVE_ENUM_REMAP:
        return true;
    default: return false; /* Literal | Null */
    }
}

void EV(plan_expr_fields)(const groove_PlanExpr *exprs, u32 n, jz_btree *out) {
    jz_btree_init(out, &groove_ivm_rt_a_desc_str_set);
    for (u32 i = 0; i < n; i++) {
        if (!plan_expr_field(&exprs[i])) continue;
        jz_str f = jz_str_clone(&exprs[i].field);
        if (jz_btree_insert(out, &f, NULL, NULL)) {}
    }
}

void EV(plan_expr_names)(const groove_PlanExpr *exprs, u32 n, groove_StrVec *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    for (u32 i = 0; i < n; i++) {
        if (!plan_expr_field(&exprs[i])) continue;
        JZ_VEC_PUSH(*out, jz_str_clone(&exprs[i].field));
    }
}

u64 EV(record_deltas_digest)(const groove_RecordDeltas *d) {
    u64 h = groove__records__RecordDescriptor__hash(d->descriptor);
    for (u32 i = 0; i < d->deltas.len; i++) {
        h = jz_hash_combine(h, (u64)d->deltas.ptr[i].weight);
        h = jz_hash_combine(h, jz_hash_bytes(d->deltas.ptr[i].record.ptr, d->deltas.ptr[i].record.len));
    }
    return h;
}

bool EV(builder_contains_recursive)(const groove_GraphBuilder *g) {
    groove_GraphBuilderRefVec nodes = {0};
    groove__ivm__graph__GraphBuilder__postorder(g, &nodes);
    bool any = false;
    for (u32 i = 0; i < nodes.len; i++)
        if (nodes.ptr[i]->tag == GROOVE_GB_RECURSIVE) { any = true; break; }
    jz_vec_drop(&nodes, sizeof(*nodes.ptr), NULL);
    return any;
}

jz_err EV(validate_arg_by_primary_key_indices)(const char *op_name, const groove_TableSchema *table, const usize *group, u32 ng, const usize *order, u32 no, const usize *pk, u32 npk) {
    bool eq = npk == ng + no;
    for (u32 i = 0; eq && i < ng; i++) eq = pk[i] == group[i];
    for (u32 i = 0; eq && i < no; i++) eq = pk[ng + i] == order[i];
    if (eq) return JZ_OK;
    return jz_err_fmt(GROOVE_IVM_ERR_UNSUPPORTED_ARG_MAX_BY,
                      "unsupported arg_by graph: %s v1 requires primary key for %.*s to equal group_cols + order_cols",
                      op_name, (int)table->name.len, table->name.ptr);
}

/* Operators whose hydration rebuilds retained state that a cached record
 * batch downstream cannot vouch for. */
static bool EV(holds_hydration_state)(const groove_OpType *op) {
    return op->tag == GROOVE_OP_AGGREGATE || op->tag == GROOVE_OP_ARG_MIN_BY || op->tag == GROOVE_OP_ARG_MAX_BY || op->tag == GROOVE_OP_ARRANGE;
}

/* ---------------------------------------------------------------------
 * CollectByOrderKey = (Vec<TopBySortPart>, Bytes)
 * ------------------------------------------------------------------- */
int EV(CollectByOrderKey__cmp)(const void *pa, const void *pb, void *ctx) {
    const groove_CollectByOrderKey *a = pa, *b = pb;
    (void)ctx;
    u32 n = a->parts.len < b->parts.len ? a->parts.len : b->parts.len;
    for (u32 i = 0; i < n; i++) {
        int c = groove__ivm__runtime__windows__TopBySortPart__Ord__cmp(&a->parts.ptr[i], &b->parts.ptr[i]);
        if (c) return c;
    }
    if (a->parts.len != b->parts.len) return a->parts.len < b->parts.len ? -1 : 1;
    return jz_bytes_cmp(&a->record, &b->record);
}
bool EV(CollectByOrderKey__eq)(const groove_CollectByOrderKey *a, const groove_CollectByOrderKey *b) {
    return EV(CollectByOrderKey__cmp)(a, b, NULL) == 0;
}
static void sort_part_clone(void *dst, const void *src) { groove__ivm__runtime__windows__TopBySortPart__clone(dst, src); }
static void sort_part_drop(void *p) { groove__ivm__runtime__windows__TopBySortPart__drop(p); }
void EV(CollectByOrderKey__clone)(void *dst, const void *src) {
    groove_CollectByOrderKey *d = dst; const groove_CollectByOrderKey *s = src;
    jz_vec_clone(&d->parts, &s->parts, sizeof(groove_TopBySortPart), sort_part_clone);
    d->record = jz_bytes_clone(&s->record);
}
void EV(CollectByOrderKey__drop)(void *k) {
    groove_CollectByOrderKey *key = k;
    jz_vec_drop(&key->parts, sizeof(groove_TopBySortPart), sort_part_drop);
    jz_bytes_drop(&key->record);
}

const jz_btree_desc groove_ivm_rt_a_desc_order_key_set = { sizeof(groove_CollectByOrderKey), 0, EV(CollectByOrderKey__cmp), EV(CollectByOrderKey__drop), NULL };
const jz_btree_desc groove_ivm_rt_a_desc_order_key_i64 = { sizeof(groove_CollectByOrderKey), sizeof(i64), EV(CollectByOrderKey__cmp), EV(CollectByOrderKey__drop), NULL };
static const jz_btree_desc desc_order_key_opt = { sizeof(groove_CollectByOrderKey), sizeof(groove_OptI64), EV(CollectByOrderKey__cmp), EV(CollectByOrderKey__drop), NULL };
static void pod_clone_i64(void *d, const void *s) { memcpy(d, s, sizeof(i64)); }
static void pod_clone_opt(void *d, const void *s) { memcpy(d, s, sizeof(groove_OptI64)); }

/* ---------------------------------------------------------------------
 * CollectByGroup
 * ------------------------------------------------------------------- */
static void counted_map_rc_drop(void *p) { CMAP(drop)(p); }
static void counted_map_rc_clone(void *dst, const void *src) { CMAP(clone)(dst, src, EV(CollectByOrderKey__clone)); }
static void btree_rc_drop(void *p) { jz_btree_drop(p); }
static void overlay_rc_clone(void *dst, const void *src) { jz_btree_clone(dst, src, EV(CollectByOrderKey__clone), pod_clone_opt); }

static groove_CountedMap *new_counted_map(void) {
    groove_CountedMap *m = jz_rc_new(sizeof(groove_CountedMap), counted_map_rc_drop);
    CMAP(default)(m, &groove_ivm_rt_a_desc_order_key_i64);
    return m;
}
static jz_btree *new_rc_btree(const jz_btree_desc *d) {
    jz_btree *t = jz_rc_new(sizeof(jz_btree), btree_rc_drop);
    jz_btree_init(t, d);
    return t;
}

void EV(CollectByGroup__default)(groove_CollectByGroup *g) {
    g->base = new_counted_map();
    g->overlay = new_rc_btree(&desc_order_key_opt);
}
void EV(CollectByGroup__clone)(void *dst, const void *src) {
    groove_CollectByGroup *d = dst; const groove_CollectByGroup *s = src;
    d->base = jz_rc_clone(s->base);
    d->overlay = jz_rc_clone(s->overlay);
}
void EV(CollectByGroup__drop)(void *p) {
    groove_CollectByGroup *g = p;
    if (g->base) jz_rc_release(g->base);
    if (g->overlay) jz_rc_release(g->overlay);
    g->base = NULL; g->overlay = NULL;
}

const i64 *EV(CollectByGroup__get)(const groove_CollectByGroup *g, const groove_CollectByOrderKey *key) {
    const groove_OptI64 *staged = jz_btree_get(g->overlay, key);
    if (staged) return staged->some ? &staged->v : NULL;
    return CMAP(get)(g->base, key);
}

void EV(CollectByGroup__set)(groove_CollectByGroup *g, groove_CollectByOrderKey key, i64 weight) {
    jz_btree *overlay = jz_rc_make_mut((void **)&g->overlay, sizeof(jz_btree), overlay_rc_clone);
    groove_OptI64 v = { weight != 0, weight };
    if (jz_btree_contains(overlay, &key)) {
        groove_OptI64 *slot = jz_btree_get(overlay, &key);
        *slot = v;
        EV(CollectByOrderKey__drop)(&key);
    } else {
        jz_btree_insert(overlay, &key, &v, NULL);
    }
}

void EV(CollectByGroup__count_before_each)(const groove_CollectByGroup *g, const groove_CollectByOrderKey *const *keys, u32 n, groove_UsizeVec *out) {
    jz_btree_iter it; jz_btree_iter_begin(&it, g->overlay);
    groove_CollectByOrderKey *ck = NULL; groove_OptI64 *cw = NULL;
    bool peeked = jz_btree_next(&it, (void **)&ck, (void **)&cw);
    isize adjustment = 0;
    out->ptr = NULL; out->len = out->cap = 0;
    for (u32 i = 0; i < n; i++) {
        const groove_CollectByOrderKey *key = keys[i];
        while (peeked && EV(CollectByOrderKey__cmp)(ck, key, NULL) < 0) {
            bool staged = cw->some && cw->v > 0;
            const i64 *bw = CMAP(get)(g->base, ck);
            bool base = bw && *bw > 0;
            adjustment += (isize)staged - (isize)base;
            peeked = jz_btree_next(&it, (void **)&ck, (void **)&cw);
        }
        isize rank = (isize)CMAP(positive_before)(g->base, key) + adjustment;
        if (rank < 0) jz_panic("rank counts present entries");
        usize r = (usize)rank;
        jz_vec_push(out, &r, sizeof(usize));
    }
}

usize EV(CollectByGroup__count_before)(const groove_CollectByGroup *g, const groove_CollectByOrderKey *key) {
    groove_UsizeVec ranks;
    EV(CollectByGroup__count_before_each)(g, &key, 1, &ranks);
    usize r = ranks.ptr[0];
    jz_vec_drop(&ranks, sizeof(usize), NULL);
    return r;
}

void EV(CollectByGroup__iter)(const groove_CollectByGroup *g, groove_CollectByGroupIter *it) {
    CMAP(iter)(g->base, &it->base);
    it->base_peeked = groove__ivm__runtime__counted_map__Iter__next(&it->base, &it->bk, &it->bw);
    jz_btree_iter_begin(&it->overlay, g->overlay);
    it->ov_peeked = jz_btree_next(&it->overlay, &it->ok, &it->ov);
}

static void cbg_advance_base(groove_CollectByGroupIter *it) { it->base_peeked = groove__ivm__runtime__counted_map__Iter__next(&it->base, &it->bk, &it->bw); }
static void cbg_advance_overlay(groove_CollectByGroupIter *it) { it->ov_peeked = jz_btree_next(&it->overlay, &it->ok, &it->ov); }

bool EV(CollectByGroupIter__Iterator__next)(groove_CollectByGroupIter *it, const groove_CollectByOrderKey **key, const i64 **weight) {
    for (;;) {
        if (it->base_peeked && it->ov_peeked) {
            int c = EV(CollectByOrderKey__cmp)(it->bk, it->ok, NULL);
            if (c < 0) {
                *key = it->bk; *weight = it->bw;
                cbg_advance_base(it);
                return true;
            } else if (c > 0) {
                const groove_CollectByOrderKey *k = it->ok; const groove_OptI64 *w = it->ov;
                cbg_advance_overlay(it);
                if (w->some) { *key = k; *weight = &w->v; return true; }
            } else {
                cbg_advance_base(it);
                const groove_CollectByOrderKey *k = it->ok; const groove_OptI64 *w = it->ov;
                if (!it->ov_peeked) jz_panic("overlay was peeked");
                cbg_advance_overlay(it);
                if (w->some) { *key = k; *weight = &w->v; return true; }
            }
        } else if (it->base_peeked) {
            *key = it->bk; *weight = it->bw;
            cbg_advance_base(it);
            return true;
        } else if (it->ov_peeked) {
            const groove_CollectByOrderKey *k = it->ok; const groove_OptI64 *w = it->ov;
            cbg_advance_overlay(it);
            if (w->some) { *key = k; *weight = &w->v; return true; }
        } else {
            return false;
        }
    }
}

bool EV(CollectByGroup__is_empty)(const groove_CollectByGroup *g) {
    groove_CollectByGroupIter it; const groove_CollectByOrderKey *k; const i64 *w;
    EV(CollectByGroup__iter)(g, &it);
    return !EV(CollectByGroupIter__Iterator__next)(&it, &k, &w);
}

/* Fold this tick's sparse mutations only after the old live operator was
 * removed, making the common commit path uniquely own the base map. */
void EV(CollectByGroup__commit_overlay)(groove_CollectByGroup *g) {
    if (g->overlay->len == 0) return;
    jz_btree *shared = g->overlay;
    g->overlay = new_rc_btree(&desc_order_key_opt);
    jz_btree overlay;
    if (jz_rc_strong(shared) == 1) {
        overlay = *shared;
        jz_btree_init(shared, &desc_order_key_opt);
    } else {
        jz_btree_clone(&overlay, shared, EV(CollectByOrderKey__clone), pod_clone_opt);
    }
    jz_rc_release(shared);
    groove_CountedMap *base = jz_rc_make_mut((void **)&g->base, sizeof(groove_CountedMap), counted_map_rc_clone);
    groove_CollectByOrderKey key; groove_OptI64 weight;
    while (jz_btree_pop_first(&overlay, &key, &weight)) {
        if (weight.some) {
            i64 old;
            CMAP(insert)(base, &key, weight.v, &old);
        } else {
            i64 old;
            CMAP(remove)(base, &key, &old);
            EV(CollectByOrderKey__drop)(&key);
        }
    }
    jz_btree_drop(&overlay);
}

/* ---------------------------------------------------------------------
 * SparseGroups
 * ------------------------------------------------------------------- */
static void opt_group_drop(void *p) { groove_OptCollectByGroup *g = p; if (g->some) EV(CollectByGroup__drop)(&g->g); }
static void opt_group_clone(void *dst, const void *src) {
    groove_OptCollectByGroup *d = dst; const groove_OptCollectByGroup *s = src;
    d->some = s->some;
    if (s->some) EV(CollectByGroup__clone)(&d->g, &s->g); else memset(&d->g, 0, sizeof(d->g));
}
static const jz_btree_desc desc_bytes_group = { sizeof(jz_bytes), sizeof(groove_CollectByGroup), jz_cmp_bytes, groove_ivm_rt_a_drop_bytes, EV(CollectByGroup__drop) };
static const jz_btree_desc desc_bytes_opt_group = { sizeof(jz_bytes), sizeof(groove_OptCollectByGroup), jz_cmp_bytes, groove_ivm_rt_a_drop_bytes, opt_group_drop };
static void groups_base_rc_clone(void *dst, const void *src) { jz_btree_clone(dst, src, groove_ivm_rt_a_clone_bytes, EV(CollectByGroup__clone)); }
static void groups_overlay_rc_clone(void *dst, const void *src) { jz_btree_clone(dst, src, groove_ivm_rt_a_clone_bytes, opt_group_clone); }

void EV(SparseGroups__default)(groove_SparseGroups *s) {
    s->base = new_rc_btree(&desc_bytes_group);
    s->overlay = new_rc_btree(&desc_bytes_opt_group);
}
void EV(SparseGroups__clone)(groove_SparseGroups *dst, const groove_SparseGroups *src) {
    dst->base = jz_rc_clone(src->base);
    dst->overlay = jz_rc_clone(src->overlay);
}
void EV(SparseGroups__drop)(groove_SparseGroups *s) {
    if (s->base) jz_rc_release(s->base);
    if (s->overlay) jz_rc_release(s->overlay);
    s->base = s->overlay = NULL;
}

const groove_CollectByGroup *EV(SparseGroups__get)(const groove_SparseGroups *s, const u8 *key, u32 len) {
    jz_bytes k = { (u8 *)key, len, 0 };
    const groove_OptCollectByGroup *staged = jz_btree_get(s->overlay, &k);
    if (staged) return staged->some ? &staged->g : NULL;
    return jz_btree_get(s->base, &k);
}

groove_CollectByGroup *EV(SparseGroups__get_or_default)(groove_SparseGroups *s, jz_bytes key) {
    const jz_btree *base = s->base;
    jz_btree *overlay = jz_rc_make_mut((void **)&s->overlay, sizeof(jz_btree), groups_overlay_rc_clone);
    groove_OptCollectByGroup *entry = jz_btree_get(overlay, &key);
    if (entry) {
        /* A previous retraction can leave a tombstone in the staged overlay.
         * A later delta in the same tick must revive it. */
        if (!entry->some) {
            const groove_CollectByGroup *b = jz_btree_get(base, &key);
            entry->some = true;
            if (b) EV(CollectByGroup__clone)(&entry->g, b); else EV(CollectByGroup__default)(&entry->g);
        }
        jz_bytes_drop(&key);
    } else {
        groove_OptCollectByGroup fresh = { true, {0} };
        const groove_CollectByGroup *b = jz_btree_get(base, &key);
        if (b) EV(CollectByGroup__clone)(&fresh.g, b); else EV(CollectByGroup__default)(&fresh.g);
        jz_bytes probe = key;
        jz_btree_insert(overlay, &key, &fresh, NULL);
        entry = jz_btree_get(overlay, &probe);
    }
    if (!entry->some) jz_panic("revived group is present");
    return &entry->g;
}

void EV(SparseGroups__remove_empty_touched_groups)(groove_SparseGroups *s, jz_bytes *keys, u32 n) {
    for (u32 i = 0; i < n; i++) {
        const groove_CollectByGroup *g = EV(SparseGroups__get)(s, keys[i].ptr, keys[i].len);
        if (g && EV(CollectByGroup__is_empty)(g)) {
            jz_btree *overlay = jz_rc_make_mut((void **)&s->overlay, sizeof(jz_btree), groups_overlay_rc_clone);
            groove_OptCollectByGroup none = { false, {0} };
            groove_OptCollectByGroup old;
            if (jz_btree_insert(overlay, &keys[i], &none, &old)) { opt_group_drop(&old); }
        } else {
            jz_bytes_drop(&keys[i]);
        }
    }
}

void EV(SparseGroups__clear)(groove_SparseGroups *s) {
    EV(SparseGroups__drop)(s);
    EV(SparseGroups__default)(s);
}

/* Present group keys in ascending order: untouched base keys merged with
 * staged insertions, skipping staged removals. */
typedef struct PresentKeys {
    jz_btree_iter base, overlay;
    bool b_peeked, o_peeked;
    jz_bytes *bk; jz_bytes *ok; groove_OptCollectByGroup *ov; void *bv;
} PresentKeys;
static void present_keys_begin(PresentKeys *p, const groove_SparseGroups *s) {
    jz_btree_iter_begin(&p->base, s->base);
    jz_btree_iter_begin(&p->overlay, s->overlay);
    p->b_peeked = jz_btree_next(&p->base, (void **)&p->bk, &p->bv);
    p->o_peeked = jz_btree_next(&p->overlay, (void **)&p->ok, (void **)&p->ov);
}
static bool present_keys_next(PresentKeys *p, const jz_bytes **out) {
    for (;;) {
        if (p->b_peeked && p->o_peeked && jz_bytes_cmp(p->bk, p->ok) < 0) {
            *out = p->bk;
            p->b_peeked = jz_btree_next(&p->base, (void **)&p->bk, &p->bv);
            return true;
        } else if (p->o_peeked) {
            if (p->b_peeked && jz_bytes_cmp(p->bk, p->ok) == 0)
                p->b_peeked = jz_btree_next(&p->base, (void **)&p->bk, &p->bv);
            bool present = p->ov->some;
            const jz_bytes *key = p->ok;
            p->o_peeked = jz_btree_next(&p->overlay, (void **)&p->ok, (void **)&p->ov);
            if (present) { *out = key; return true; }
        } else if (p->b_peeked) {
            *out = p->bk;
            p->b_peeked = jz_btree_next(&p->base, (void **)&p->bk, &p->bv);
            return true;
        } else {
            return false;
        }
    }
}

/* Rank each of `keys` (ascending) among the present groups in one merged walk. */
void EV(SparseGroups__count_before_each)(const groove_SparseGroups *s, const jz_slice *keys, u32 n, groove_UsizeVec *out) {
    PresentKeys present; present_keys_begin(&present, s);
    const jz_bytes *candidate = NULL;
    bool has = present_keys_next(&present, &candidate);
    usize before = 0;
    out->ptr = NULL; out->len = out->cap = 0;
    for (u32 i = 0; i < n; i++) {
        while (has && jz_slice_cmp(jz_bytes_slice(candidate), keys[i]) < 0) {
            before++;
            has = present_keys_next(&present, &candidate);
        }
        jz_vec_push(out, &before, sizeof(usize));
    }
}

/* Fold after the installed state was removed. Remove a replaced group from
 * the uniquely-owned outer map before folding its inner overlay. */
void EV(SparseGroups__commit_overlay)(groove_SparseGroups *s) {
    if (s->overlay->len == 0) return;
    jz_btree overlay, base;
    jz_btree *shared = s->overlay;
    s->overlay = new_rc_btree(&desc_bytes_opt_group);
    if (jz_rc_strong(shared) == 1) { overlay = *shared; jz_btree_init(shared, &desc_bytes_opt_group); }
    else jz_btree_clone(&overlay, shared, groove_ivm_rt_a_clone_bytes, opt_group_clone);
    jz_rc_release(shared);
    shared = s->base;
    s->base = NULL;
    if (jz_rc_strong(shared) == 1) { base = *shared; jz_btree_init(shared, &desc_bytes_group); }
    else jz_btree_clone(&base, shared, groove_ivm_rt_a_clone_bytes, EV(CollectByGroup__clone));
    jz_rc_release(shared);
    jz_bytes key; groove_OptCollectByGroup group;
    while (jz_btree_pop_first(&overlay, &key, &group)) {
        /* Dropping the pre-image first makes an existing staged group's
         * occurrence base uniquely owned on the successful install path. */
        jz_bytes old_key; groove_CollectByGroup old;
        if (jz_btree_remove(&base, &key, &old_key, &old)) { jz_bytes_drop(&old_key); EV(CollectByGroup__drop)(&old); }
        if (group.some) {
            EV(CollectByGroup__commit_overlay)(&group.g);
            jz_btree_insert(&base, &key, &group.g, NULL);
        } else {
            jz_bytes_drop(&key);
        }
    }
    jz_btree_drop(&overlay);
    s->base = jz_rc_new(sizeof(jz_btree), btree_rc_drop);
    *s->base = base;
}

/* ---------------------------------------------------------------------
 * TopByIncrementalState / CollectByIncrementalState
 * ------------------------------------------------------------------- */
/* Remove only groups touched by this update so a long-lived subscription
 * does not retain one empty map for every departed group. */
static void EV(TopByIncrementalState__remove_empty_touched_groups)(groove_TopByIncrementalState *s, jz_bytes *groups, u32 n) {
    for (u32 i = 0; i < n; i++) {
        const groove_CollectByGroup *g = EV(SparseGroups__get)(&s->groups, groups[i].ptr, groups[i].len);
        if (g && EV(CollectByGroup__is_empty)(g)) EV(SparseGroups__remove_empty_touched_groups)(&s->groups, &groups[i], 1);
        else jz_bytes_drop(&groups[i]);
    }
}

void EV(TopByIncrementalState__commit_overlays)(groove_TopByIncrementalState *s) {
    EV(SparseGroups__commit_overlay)(&s->groups);
}

static void collect_payload_drop(void *p) {
    groove_CollectByIncrementalPayload *c = p;
    EV(SparseGroups__drop)(&c->groups);
    jz_btree_drop(&c->roots);
    if (c->emitted_root_order) jz_rc_release(c->emitted_root_order);
    if (c->emitted_root_keys) jz_rc_release(c->emitted_root_keys);
}
static void collect_payload_clone(void *dst, const void *src) {
    groove_CollectByIncrementalPayload *d = dst; const groove_CollectByIncrementalPayload *s = src;
    EV(SparseGroups__clone)(&d->groups, &s->groups);
    jz_btree_clone(&d->roots, &s->roots, EV(CollectByOrderKey__clone), pod_clone_i64);
    d->emitted_root_order = jz_rc_clone(s->emitted_root_order);
    d->emitted_root_keys = jz_rc_clone(s->emitted_root_keys);
}
void EV(CollectByIncrementalState__default)(groove_CollectByIncrementalState *s) {
    groove_CollectByIncrementalPayload *p = jz_rc_new(sizeof(*p), collect_payload_drop);
    EV(SparseGroups__default)(&p->groups);
    jz_btree_init(&p->roots, &groove_ivm_rt_a_desc_order_key_i64);
    p->emitted_root_order = groove__ivm__runtime__rank_index__RankIndex__new_order();
    p->emitted_root_keys = groove__ivm__runtime__rank_index__RankIndex__new_keys();
    s->payload = p;
}
groove_CollectByIncrementalPayload *EV(CollectByIncrementalState__deref_mut)(groove_CollectByIncrementalState *s) {
    return jz_rc_make_mut((void **)&s->payload, sizeof(groove_CollectByIncrementalPayload), collect_payload_clone);
}

/* Reuse a fresh root collector's complete Insert seeds only when every group
 * has one unambiguous representative. */
typedef struct SeedRef { const jz_bytes *key; const jz_bytes *value; } SeedRef;
static int seed_cmp(const void *a, const void *b, void *ctx) { (void)ctx; return jz_bytes_cmp(((const SeedRef *)a)->key, ((const SeedRef *)b)->key); }
static bool EV(singleton_root_hydration_snapshot)(const groove_CollectByIncrementalState *state, const groove_RecordDelta *input, u32 n_input, const groove_TerminalOperation *ops, u32 n_ops, groove_RecordDeltaVec *out) {
    for (u32 i = 0; i < n_input; i++) if (input[i].weight <= 0) return false;
    JZ_VEC(SeedRef) seeds = {0};
    jz_vec_reserve(&seeds, n_ops, sizeof(SeedRef));
    for (u32 i = 0; i < n_ops; i++) {
        const groove_TerminalOperation *op = &ops[i];
        if (op->edit.tag != GROOVE_TE_INSERT) goto none;
        if (op->path.len != 0 || !jz_bytes_eq(&op->root_key, &op->edit.key)) goto none;
        const groove_CollectByGroup *g = EV(SparseGroups__get)(&state->payload->groups, op->edit.key.ptr, op->edit.key.len);
        if (!g) goto none;
        groove_CollectByGroupIter it; const groove_CollectByOrderKey *k; const i64 *w;
        EV(CollectByGroup__iter)(g, &it);
        if (!EV(CollectByGroupIter__Iterator__next)(&it, &k, &w)) goto none;
        if (*w <= 0 || EV(CollectByGroupIter__Iterator__next)(&it, &k, &w)) goto none;
        SeedRef sr = { &op->edit.key, &op->edit.value };
        jz_vec_push(&seeds, &sr, sizeof(sr));
    }
    /* Terminal operations use public rank order; the relational snapshot has
     * always visited the touched groups in encoded group-key order. */
    jz_sort_unstable(seeds.ptr, seeds.len, sizeof(SeedRef), seed_cmp, NULL);
    out->ptr = NULL; out->len = out->cap = 0;
    for (u32 i = 0; i < seeds.len; i++) {
        groove_RecordDelta d = { jz_bytes_clone(seeds.ptr[i].value), 1 };
        jz_vec_push(out, &d, sizeof(d));
    }
    jz_vec_drop(&seeds, sizeof(SeedRef), NULL);
    return true;
none:
    jz_vec_drop(&seeds, sizeof(SeedRef), NULL);
    return false;
}

/* ---------------------------------------------------------------------
 * OperatorState
 * ------------------------------------------------------------------- */
static void pending_checksum_clone(groove_PendingStreamingChecksum *d, const groove_PendingStreamingChecksum *s) {
    d->input = jz_rc_clone(s->input);
    d->next_delta = s->next_delta;
    d->current = s->current ? groove__large_values__StreamingChecksum__clone(s->current) : NULL;
    jz_vec_clone(&d->output, &s->output, sizeof(groove_RecordDelta), groove__ivm__runtime__subscriptions__RecordDelta__clone);
}
static void pending_checksum_drop(groove_PendingStreamingChecksum *p) {
    if (p->input) jz_rc_release(p->input);
    if (p->current) groove__large_values__StreamingChecksum__drop(p->current);
    jz_vec_drop(&p->output, sizeof(groove_RecordDelta), groove__ivm__runtime__subscriptions__RecordDelta__drop);
    memset(p, 0, sizeof(*p));
}

void EV(OperatorState__clone)(void *dst, const void *src) {
    groove_OperatorState *d = dst; const groove_OperatorState *s = src;
    d->tag = s->tag;
    switch (s->tag) {
    case GROOVE_OS_STATELESS: case GROOVE_OS_JOIN: break;
    case GROOVE_OS_SEMI_JOIN: d->v.semi_join = groove__ivm__runtime__join__SemiJoinState__clone(s->v.semi_join); break;
    case GROOVE_OS_ANTI_JOIN: d->v.anti_join = groove__ivm__runtime__join__AntiJoinState__clone(s->v.anti_join); break;
    case GROOVE_OS_ARG_BY: EV(SparseGroups__clone)(&d->v.arg_by.value, &s->v.arg_by.value); d->v.arg_by.as_of = s->v.arg_by.as_of; break;
    case GROOVE_OS_TOP_BY: EV(SparseGroups__clone)(&d->v.top_by.value.groups, &s->v.top_by.value.groups); d->v.top_by.as_of = s->v.top_by.as_of; break;
    case GROOVE_OS_RECURSIVE: d->v.recursive.value = groove__ivm__runtime__recursion__RecursiveState__clone(s->v.recursive.value); d->v.recursive.as_of = s->v.recursive.as_of; break;
    case GROOVE_OS_COLLECT_BY: d->v.collect_by.payload = jz_rc_clone(s->v.collect_by.payload); break;
    case GROOVE_OS_STREAMING_CHECKSUM: {
        groove_StreamingChecksumOperatorState *b = JZ_NEW(groove_StreamingChecksumOperatorState);
        b->has_pending = s->v.streaming_checksum->has_pending;
        if (b->has_pending) pending_checksum_clone(&b->pending, &s->v.streaming_checksum->pending);
        d->v.streaming_checksum = b;
        break;
    }
    case GROOVE_OS_LOOKUP: d->v.lookup.value.rc = jz_rc_clone(s->v.lookup.value.rc); d->v.lookup.as_of = s->v.lookup.as_of; break;
    default: JZ_UNREACHABLE();
    }
}

void EV(OperatorState__drop)(void *p) {
    groove_OperatorState *s = p;
    switch (s->tag) {
    case GROOVE_OS_SEMI_JOIN: groove__ivm__runtime__join__SemiJoinState__drop(s->v.semi_join); break;
    case GROOVE_OS_ANTI_JOIN: groove__ivm__runtime__join__AntiJoinState__drop(s->v.anti_join); break;
    case GROOVE_OS_ARG_BY: EV(SparseGroups__drop)(&s->v.arg_by.value); break;
    case GROOVE_OS_TOP_BY: EV(SparseGroups__drop)(&s->v.top_by.value.groups); break;
    case GROOVE_OS_RECURSIVE: groove__ivm__runtime__recursion__RecursiveState__drop(s->v.recursive.value); break;
    case GROOVE_OS_COLLECT_BY: if (s->v.collect_by.payload) jz_rc_release(s->v.collect_by.payload); break;
    case GROOVE_OS_STREAMING_CHECKSUM:
        if (s->v.streaming_checksum->has_pending) pending_checksum_drop(&s->v.streaming_checksum->pending);
        jz_free(s->v.streaming_checksum);
        break;
    case GROOVE_OS_LOOKUP: groove__ivm__runtime__evaluator__lookup__LookupState__drop(&s->v.lookup.value); break;
    default: break;
    }
    s->tag = GROOVE_OS_STATELESS;
}

void EV(operator_state_for)(const groove_OpType *op, groove_OperatorState *out) {
    memset(out, 0, sizeof(*out));
    switch (op->tag) {
    case GROOVE_OP_JOIN: out->tag = GROOVE_OS_JOIN; break;
    case GROOVE_OP_SEMI_JOIN: out->tag = GROOVE_OS_SEMI_JOIN; out->v.semi_join = groove__ivm__runtime__join__SemiJoinState__default(); break;
    case GROOVE_OP_ANTI_JOIN: out->tag = GROOVE_OS_ANTI_JOIN; out->v.anti_join = groove__ivm__runtime__join__AntiJoinState__default(); break;
    case GROOVE_OP_RECURSIVE: out->tag = GROOVE_OS_RECURSIVE; out->v.recursive.value = groove__ivm__runtime__recursion__RecursiveState__default(); break;
    case GROOVE_OP_ARG_MIN_BY: case GROOVE_OP_ARG_MAX_BY: out->tag = GROOVE_OS_ARG_BY; EV(SparseGroups__default)(&out->v.arg_by.value); break;
    case GROOVE_OP_TOP_BY: out->tag = GROOVE_OS_TOP_BY; EV(SparseGroups__default)(&out->v.top_by.value.groups); break;
    case GROOVE_OP_COLLECT_BY: out->tag = GROOVE_OS_COLLECT_BY; EV(CollectByIncrementalState__default)(&out->v.collect_by); break;
    case GROOVE_OP_STREAMING_CHECKSUM: out->tag = GROOVE_OS_STREAMING_CHECKSUM; out->v.streaming_checksum = JZ_NEW(groove_StreamingChecksumOperatorState); break;
    case GROOVE_OP_LOOKUP: out->tag = GROOVE_OS_LOOKUP; groove__ivm__runtime__evaluator__lookup__LookupState__default(&out->v.lookup.value); break;
    default: out->tag = GROOVE_OS_STATELESS; break;
    }
}

/* ---------------------------------------------------------------------
 * hash map descriptors
 * ------------------------------------------------------------------- */
static u64 hash_operator_key(const void *k) {
    const groove_OperatorStateKey *key = k;
    return jz_hash_combine(groove_hash_ScopeId(key->scope), key->node.v);
}
static bool eq_operator_key(const void *a, const void *b) {
    const groove_OperatorStateKey *x = a, *y = b;
    return x->scope == y->scope && x->node.v == y->node.v;
}
static void terminal_deltas_drop(void *p) { groove__ivm__runtime__terminal__TerminalDeltas__drop(p); }
static void pending_unary_drop(void *p) { EV(PendingUnaryBatch__drop)(p); }
const jz_hmap_desc groove_ivm_rt_a_hdesc_operator_states = { sizeof(groove_OperatorStateKey), sizeof(groove_OperatorState), hash_operator_key, eq_operator_key, NULL, EV(OperatorState__drop) };
const jz_hmap_desc groove_ivm_rt_a_hdesc_terminal_deltas = { sizeof(groove_NodeId), sizeof(groove_TerminalDeltas), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, terminal_deltas_drop };
const jz_hmap_desc groove_ivm_rt_a_hdesc_root_windows = { sizeof(groove_NodeId), sizeof(groove_RootOrderingWindows), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, EV(RootOrderingWindows__drop) };
const jz_hmap_desc groove_ivm_rt_a_hdesc_pending_unary = { sizeof(groove_NodeId), sizeof(groove_PendingUnaryBatch), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, pending_unary_drop };
static void transition_drop(void *p) {
    groove_GroupTransition *t = p;
    EV(CollectByGroup__drop)(&t->before);
    EV(CollectByGroup__drop)(&t->after);
    jz_btree_drop(&t->touched);
}
static const jz_hmap_desc hdesc_bytes_transition = { sizeof(jz_bytes), sizeof(groove_GroupTransition), groove_ivm_rt_a_hash_bytes, groove_ivm_rt_a_eq_bytes, groove_ivm_rt_a_drop_bytes, transition_drop };
typedef struct EvidencePair { bool before, now; } EvidencePair;
static const jz_hmap_desc hdesc_bytes_evidence = { sizeof(jz_bytes), sizeof(EvidencePair), groove_ivm_rt_a_hash_bytes, groove_ivm_rt_a_eq_bytes, groove_ivm_rt_a_drop_bytes, NULL };

groove_NodeRuntimeMeta *groove_ivm_rt_a_node_meta_entry(jz_hmap *node_meta, groove_NodeId node) {
    bool inserted = false;
    groove_NodeRuntimeMeta *m = jz_hmap_entry(node_meta, &node, &inserted);
    if (inserted) groove__ivm__runtime__operator_updates__NodeRuntimeMeta__default(m);
    return m;
}

static void strlist_drop(void *p) {
    groove_StrList *l = p;
    for (u32 i = 0; i < l->len; i++) jz_str_drop(&l->items[i]);
    jz_free(l->items);
}
groove_StrList *groove_ivm_rt_a_strlist_from_vec(groove_StrVec v) {
    groove_StrList *l = jz_rc_new(sizeof(groove_StrList), strlist_drop);
    l->len = v.len; l->items = v.ptr;
    return l;
}

/* ---------------------------------------------------------------------
 * RootOrderingWindows
 * ------------------------------------------------------------------- */
void EV(RootOrderingWindows__default)(groove_RootOrderingWindows *w) {
    memset(w, 0, sizeof(*w));
    jz_hmap_init(&w->groups, &hdesc_bytes_transition);
}
void EV(RootOrderingWindows__drop)(void *p) {
    groove_RootOrderingWindows *w = p;
    jz_hmap_drop(&w->groups);
    jz_vec_drop(&w->identity, sizeof(usize), NULL);
    if (w->has_top_by) groove__ivm__op_types__TopByOp__drop(&w->top_by);
}

void EV(RootOrderingWindows__record)(groove_RootOrderingWindows *w, groove_RecordDescriptor descriptor, const groove_TopByOp *top_by, const u8 *group, u32 group_len, groove_BeforeGroupFn before, void *before_ctx, const groove_CollectByGroup *after, groove_CollectByOrderKey *touched, u32 n_touched, bool touched_complete) {
    if (!w->has_descriptor) {
        w->has_descriptor = true;
        w->descriptor = descriptor;
        jz_vec_drop(&w->identity, sizeof(usize), NULL);
        groove__ivm__runtime__record_projection__top_by_identity_fields(top_by, descriptor->fields.len, &w->identity);
        if (w->has_top_by) groove__ivm__op_types__TopByOp__drop(&w->top_by);
        groove__ivm__op_types__TopByOp__clone(&w->top_by, top_by);
        w->has_top_by = true;
    }
    jz_bytes key = jz_bytes_from(group, group_len);
    bool inserted = false;
    groove_GroupTransition *t = jz_hmap_entry(&w->groups, &key, &inserted);
    if (inserted) {
        before(before_ctx, &t->before);
        EV(CollectByGroup__default)(&t->after);
        jz_btree_init(&t->touched, &groove_ivm_rt_a_desc_order_key_set);
        t->touched_complete = true;
    } else {
        jz_bytes_drop(&key);
    }
    EV(CollectByGroup__drop)(&t->after);
    EV(CollectByGroup__clone)(&t->after, after);
    for (u32 i = 0; i < n_touched; i++) {
        if (jz_btree_contains(&t->touched, &touched[i])) EV(CollectByOrderKey__drop)(&touched[i]);
        else jz_btree_insert(&t->touched, &touched[i], NULL, NULL);
    }
    t->touched_complete &= touched_complete;
}

/* A group's complete before and after windows across this tick. */
static bool EV(RootOrderingWindows__group_window)(const groove_RootOrderingWindows *w, const jz_bytes *group, groove_WindowedRecordVec *before, groove_WindowedRecordVec *after) {
    const groove_GroupTransition *t = jz_hmap_get(&w->groups, group);
    if (!t || !w->has_top_by) return false;
    groove__ivm__runtime__windows__top_by_window_from_ordered_group(&t->before, &w->top_by, before);
    groove__ivm__runtime__windows__top_by_window_from_ordered_group(&t->after, &w->top_by, after);
    return true;
}

static bool group_present(const groove_CollectByGroup *g, const groove_CollectByOrderKey *key) {
    const i64 *w = EV(CollectByGroup__get)(g, key);
    return w && *w > 0;
}
static void changed_rows_of(const groove_CollectByGroup *g, const groove_CollectByOrderKey **keys, u32 n, groove_IndexedRecordVec *out) {
    groove_UsizeVec ranks;
    EV(CollectByGroup__count_before_each)(g, keys, n, &ranks);
    out->ptr = NULL; out->len = out->cap = 0;
    for (u32 i = 0; i < n; i++) {
        groove_IndexedRecord r = { ranks.ptr[i], jz_bytes_clone(&keys[i]->record) };
        jz_vec_push(out, &r, sizeof(r));
    }
    jz_vec_drop(&ranks, sizeof(usize), NULL);
}

/* The rows whose presence changed in a zero-offset unbounded window, ranked
 * through the counted group index. None when bounded or touched incomplete. */
static bool EV(RootOrderingWindows__changed_rows)(const groove_RootOrderingWindows *w, const jz_bytes *group, groove_ChangedWindowRows *out) {
    const groove_GroupTransition *t = jz_hmap_get(&w->groups, group);
    if (!t || !w->has_top_by) return false;
    if (w->top_by.offset != 0 || w->top_by.limit.tag != GROOVE_LIMIT_UNBOUNDED || !t->touched_complete) return false;
    JZ_VEC(const groove_CollectByOrderKey *) removed = {0}, added = {0};
    groove_CollectByOrderKey *key; void *unused;
    JZ_BTREE_FOR(&t->touched, key, unused) {
        bool b = group_present(&t->before, key), a = group_present(&t->after, key);
        const groove_CollectByOrderKey *k = key;
        if (b && !a) jz_vec_push(&removed, &k, sizeof(k));
        else if (!b && a) jz_vec_push(&added, &k, sizeof(k));
    }
    changed_rows_of(&t->before, removed.ptr, removed.len, &out->removed);
    changed_rows_of(&t->after, added.ptr, added.len, &out->added);
    jz_vec_drop(&removed, sizeof(void *), NULL);
    jz_vec_drop(&added, sizeof(void *), NULL);
    return true;
}

static void indexed_record_drop(void *p) { jz_bytes_drop(&((groove_IndexedRecord *)p)->record); }
static void windowed_record_drop(void *p) { jz_bytes_drop(&((groove_WindowedRecord *)p)->record); }
static void indexed_key_drop(void *p) { jz_bytes_drop(&((groove_IndexedKey *)p)->key); }

/* ---------------------------------------------------------------------
 * PendingUnaryBatch / TickEvaluator lifecycle
 * ------------------------------------------------------------------- */
void EV(PendingUnaryBatch__drop)(void *p) {
    groove_PendingUnaryBatch *b = p;
    if (b->input) jz_rc_release(b->input);
    groove__ivm__runtime__subscriptions__RecordDeltas__drop(&b->output);
    if (b->projection) groove__records__PreparedProjection__release(b->projection);
}

void TE(init_maps)(groove_TickEvaluator *e) {
    jz_hmap_init(&e->terminal_deltas, &groove_ivm_rt_a_hdesc_terminal_deltas);
    jz_hmap_init(&e->root_ordering_windows, &groove_ivm_rt_a_hdesc_root_windows);
}
void TE(drop)(groove_TickEvaluator *e) {
    jz_hmap_drop(&e->terminal_deltas);
    jz_hmap_drop(&e->root_ordering_windows);
    groove__ivm__runtime__state__EvalContext__drop(&e->context);
}

/* ---------------------------------------------------------------------
 * GraphRuntimeView
 * ------------------------------------------------------------------- */
static void view_evaluator(groove_GraphRuntimeView *v, groove_TickEvaluator *e) {
    memset(e, 0, sizeof(*e));
    e->schema = v->schema; e->graph = v->graph; e->variant_projections = v->variant_projections;
    e->table_deltas = v->table_deltas; e->n_table_deltas = v->n_table_deltas;
    e->binding_deltas = v->binding_deltas; e->n_binding_deltas = v->n_binding_deltas;
    e->binding_snapshots = v->binding_snapshots; e->current_tick = v->current_tick;
    e->operator_states = v->operator_states; e->arrangement_states = v->arrangement_states;
    e->arrangement_keys_by_input = v->arrangement_keys_by_input;
    e->eval_memo = v->eval_memo; e->eval_memo_bytes = v->eval_memo_bytes;
    e->table_frontiers = v->table_frontiers; e->binding_frontiers = v->binding_frontiers;
    e->memo_use_clock = v->memo_use_clock; e->node_meta = v->node_meta;
    e->storage = v->storage; e->evaluation_inputs = NULL; e->metrics = v->metrics;
    TE(init_maps)(e);
}

static jz_err run_view_evaluator(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDeltas *out) {
    groove_RecordDeltas *records = NULL;
    jz_err err = TE(update_subgraph)(e, node, &records);
    if (!err.code) {
        groove__ivm__runtime__subscriptions__RecordDeltas__clone(out, records);
        jz_rc_release(records);
    }
    TE(drop)(e);
    return err;
}

jz_err EV(GraphRuntimeView__eval_with_binding)(groove_GraphRuntimeView *v, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_NodeId node, groove_RecordDeltas *out) {
    groove_TickEvaluator e;
    view_evaluator(v, &e);
    groove__ivm__runtime__state__EvalContext__with_binding(&e.context, v->scope, sub_tick, binding, deltas);
    return run_view_evaluator(&e, node, out);
}

jz_err EV(GraphRuntimeView__eval_with_binding_and_table_deltas)(groove_GraphRuntimeView *v, const groove_TableDelta *td, u32 ntd, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_NodeId node, groove_RecordDeltas *out) {
    groove_EvaluationMemo *isolated_memo = groove__ivm__runtime__evaluation_memo__EvaluationMemo__default();
    usize isolated_memo_bytes = 0;
    groove_TickEvaluator e;
    view_evaluator(v, &e);
    groove__ivm__runtime__state__EvalContext__with_binding_and_arrangement_mode(&e.context, v->scope, sub_tick, binding, deltas, GROOVE_AUM_REPLACE);
    if (v->evaluation_inputs) e.context.eval_mode = GROOVE_EVAL_HYDRATE;
    e.table_deltas = td; e.n_table_deltas = ntd;
    e.eval_memo = isolated_memo; e.eval_memo_bytes = &isolated_memo_bytes;
    e.evaluation_inputs = v->evaluation_inputs;
    jz_err err = run_view_evaluator(&e, node, out);
    groove__ivm__runtime__evaluation_memo__EvaluationMemo__drop(isolated_memo);
    return err;
}

static bool keep_other_scope(const void *k, void *v, void *ctx) {
    (void)v;
    return ((const groove_OperatorStateKey *)k)->scope != (groove_ScopeId)ctx;
}
void EV(GraphRuntimeView__clear_operator_state_for_scope)(groove_GraphRuntimeView *v) {
    jz_hmap_retain(v->operator_states, keep_other_scope, (void *)v->scope);
}

jz_err EV(GraphRuntimeView__eval_root)(groove_GraphRuntimeView *v, groove_NodeId node, groove_RecordDeltas *out) {
    groove_TickEvaluator e;
    view_evaluator(v, &e);
    groove__ivm__runtime__state__EvalContext__default(&e.context);
    e.context.scope = v->scope;
    e.context.sub_tick = 0;
    e.context.arrangement_update_mode = GROOVE_AUM_ACCUMULATE;
    e.context.eval_mode = GROOVE_EVAL_TICK;
    e.context.hydrate_arrangements = false;
    return run_view_evaluator(&e, node, out);
}

/* ---------------------------------------------------------------------
 * TickEvaluator: frame driving
 * ------------------------------------------------------------------- */
static jz_err TE(ready_unary_input)(groove_TickEvaluator *e, groove_NodeId node, const groove_NodeMemoLookup *lookup, bool *some, groove_RecordDeltas **out);
static jz_err TE(compute_unary_input)(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDeltas *input, groove_RecordDeltas *out);
static jz_err TE(compute_ready_batch)(groove_TickEvaluator *e, groove_NodeId node, const groove_NodeMemoLookup *lookup, const groove_ArcDeltasVec *ready_inputs, bool *handled, groove_RecordDeltas **out);
static bool TE(supports_resident_batch)(const groove_TickEvaluator *e, groove_NodeId node);
static jz_err TE(prepare_unary_batch)(groove_TickEvaluator *e, groove_NodeId node, const groove_NodeMemoLookup *lookup, groove_RecordDeltas *input, bool *some, groove_PendingUnaryBatch *out);
static jz_err TE(terminal_lineage)(groove_TickEvaluator *e, groove_NodeId root, groove_TerminalLineage **out);
static jz_err TE(cached_result_state_is_current)(groove_TickEvaluator *e, groove_NodeId node, bool *out);

static void arc_deltas_vec_drop(groove_ArcDeltasVec *v) {
    for (u32 i = 0; i < v->len; i++) jz_rc_release(v->ptr[i]);
    jz_vec_drop(v, sizeof(void *), NULL);
}

jz_err TE(poll_ready_node)(groove_TickEvaluator *e, groove_NodeId node, jz_hmap *pending, groove_FrameInputs frame_inputs, groove_RecordDeltas **out) {
    if (!jz_hmap_get(pending, &node)) {
        groove_NodeMemoLookup lookup;
        JZ_TRY(TE(prepare_memo_lookup)(e, node, &lookup));
        groove_RecordDeltas *cached = NULL;
        JZ_TRY(TE(cached_node_records)(e, &lookup, &cached));
        if (cached) { *out = cached; return JZ_OK; }
        bool has_resolved = false;
        groove_ArcDeltasVec resolved = {0};
        if (TE(supports_resident_batch)(e, node))
            JZ_TRY(TE(resolve_register_inputs)(e, frame_inputs, &has_resolved, &resolved));
        const groove_ArcDeltasVec *ready_inputs = has_resolved ? &resolved : NULL;
        groove_RecordDeltas *input = NULL;
        if (ready_inputs) {
            const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
            if (gn && (gn->descriptor.op.tag == GROOVE_OP_FILTER || gn->descriptor.op.tag == GROOVE_OP_MAP_PROJECT) && ready_inputs->len > 0)
                input = jz_rc_clone(ready_inputs->ptr[0]);
        }
        if (!input) {
            bool some = false;
            jz_err err = TE(ready_unary_input)(e, node, &lookup, &some, &input);
            if (err.code) { arc_deltas_vec_drop(&resolved); return err; }
            if (!some) {
                bool handled = false;
                err = TE(compute_ready_batch)(e, node, &lookup, ready_inputs, &handled, out);
                arc_deltas_vec_drop(&resolved);
                if (handled || err.code) return err;
                return TE(compute_node)(e, node, lookup, out);
            }
        }
        arc_deltas_vec_drop(&resolved);
        bool some = false;
        groove_PendingUnaryBatch batch;
        jz_err err = TE(prepare_unary_batch)(e, node, &lookup, input, &some, &batch);
        if (err.code) { jz_rc_release(input); return err; }
        if (some) {
            jz_hmap_insert(pending, &node, &batch, NULL);
            jz_rc_release(input);
        } else {
            groove_RecordDeltas result;
            err = TE(compute_unary_input)(e, node, input, &result);
            jz_rc_release(input);
            if (err.code) return err;
            *out = TE(memoize_result)(e, &lookup, result);
            return JZ_OK;
        }
    }
    groove_PendingUnaryBatch *batch = jz_hmap_get(pending, &node);
    if (!batch) jz_panic("prepared unary batch");
    const groove_GraphNode *graph_node = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!graph_node) jz_panic("frame retains node");
    usize end = batch->next + UNARY_ROWS_PER_POLL;
    if (end > batch->input->deltas.len) end = batch->input->deltas.len;
    const groove_RecordDelta *input = batch->input->deltas.ptr + batch->next;
    u32 n = (u32)(end - batch->next);
    groove_RecordDeltas result = groove_RecordDeltas_empty(batch->output.descriptor);
    jz_err err = JZ_OK;
    switch (graph_node->descriptor.op.tag) {
    case GROOVE_OP_MAP_PROJECT:
        err = groove__ivm__runtime__operator_updates__NodeState__update_map_project_slice(
            &graph_node->descriptor.op.v.map_project, batch->output.descriptor, batch->input->descriptor,
            input, n, batch->projection, false, &result);
        break;
    case GROOVE_OP_FILTER: {
        const groove_FilterOp *filter = &graph_node->descriptor.op.v.filter;
        for (u32 i = 0; i < n; i++) {
            bool matches = false;
            err = groove__ivm__runtime__subscriptions__PredicateExpr__matches_borrowed(&filter->predicate, groove_RecordDelta_borrowed(&input[i], batch->input->descriptor), filter->comparison, &matches);
            if (err.code) break;
            if (matches) {
                groove_RecordDelta d;
                groove__ivm__runtime__subscriptions__RecordDelta__clone(&d, &input[i]);
                jz_vec_push(&result.deltas, &d, sizeof(d));
            }
        }
        break;
    }
    default: jz_panic("internal error: entered unreachable code: prepared unary operator");
    }
    if (err.code) {
        groove__ivm__runtime__subscriptions__RecordDeltas__drop(&result);
        jz_hmap_remove(pending, &node, NULL, NULL);
        return err;
    }
    jz_vec_extend(&batch->output.deltas, result.deltas.ptr, result.deltas.len, sizeof(groove_RecordDelta));
    jz_vec_drop(&result.deltas, sizeof(groove_RecordDelta), NULL);
    batch->next = end;
    if (end < batch->input->deltas.len) {
        /* The caller owns wakeup and distinguishes this from a registered
         * storage request, just like recursive yields. */
        return groove_poll_pending();
    }
    groove_PendingUnaryBatch done;
    groove_NodeId k;
    if (!jz_hmap_remove(pending, &node, &k, &done)) jz_panic("completed unary batch");
    groove_RecordDeltas output = done.output;
    done.output = groove_RecordDeltas_empty(output.descriptor);
    *out = TE(memoize_result)(e, &done.lookup, output);
    EV(PendingUnaryBatch__drop)(&done);
    return JZ_OK;
}

static jz_err TE(prepare_unary_batch)(groove_TickEvaluator *e, groove_NodeId node, const groove_NodeMemoLookup *lookup, groove_RecordDeltas *input, bool *some, groove_PendingUnaryBatch *out) {
    *some = false;
    if (input->deltas.len <= UNARY_ROWS_PER_POLL) return JZ_OK;
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) return groove_err_node_not_found(node);
    groove_RecordDescriptor output = groove_NodeOutput_records(gn->descriptor.output);
    groove_PreparedProjection *projection = NULL;
    switch (gn->descriptor.op.tag) {
    case GROOVE_OP_MAP_PROJECT:
        JZ_TRY(TE(raw_projection_fields)(e, node, &gn->descriptor.op.v.map_project, input->descriptor, output, &projection));
        break;
    case GROOVE_OP_FILTER: {
        jz_btree referenced; jz_btree_init(&referenced, &groove_ivm_rt_a_desc_str_set);
        groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(&gn->descriptor.op.v.filter.predicate, &referenced);
        /* Indirect fields have request/error ordering semantics. Keep them on
         * the existing kernel until their resumable stages can retain both
         * materialization and predicate cursors. */
        jz_str *field; void *unused;
        bool bail = false;
        JZ_BTREE_FOR(&referenced, field, unused) {
            usize index;
            if (!groove__ivm__runtime__record_projection__resolve_field_name(input->descriptor, field->ptr, field->len, &index)) { bail = true; break; }
            if (groove__records__values__ValueType__may_contain_stored_scalar(&input->descriptor->fields.ptr[index].value_type)) { bail = true; break; }
        }
        jz_btree_drop(&referenced);
        if (bail) return JZ_OK;
        break;
    }
    default: jz_panic("internal error: entered unreachable code: checked unary operator");
    }
    if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) {
        e->metrics->hydration_memo_computes += 1;
        jz_hmap_insert(&e->metrics->hydration_memo_computed_nodes, &node, NULL, NULL);
    }
    out->lookup = *lookup;
    out->input = jz_rc_clone(input);
    out->next = 0;
    out->output = groove_RecordDeltas_empty(output);
    out->projection = projection;
    *some = true;
    return JZ_OK;
}

/* Evaluate one reachable graph slice in dependency order, iteratively so
 * stack use is independent of graph depth. */
typedef struct PendingVisit { groove_NodeId node; bool expanded; } PendingVisit;
jz_err TE(update_subgraph)(groove_TickEvaluator *e, groove_NodeId root, groove_RecordDeltas **out) {
    JZ_VEC(PendingVisit) pending = {0};
    jz_hmap discovered; jz_hmap_init(&discovered, &groove_ivm_rt_a_hdesc_nodeid_set);
    groove_NodeIdVec order = {0};
    jz_err err = JZ_OK;
    PendingVisit first = { root, false };
    jz_vec_push(&pending, &first, sizeof(first));
    while (pending.len) {
        PendingVisit visit = pending.ptr[--pending.len];
        if (visit.expanded) { jz_vec_push(&order, &visit.node, sizeof(groove_NodeId)); continue; }
        bool inserted = false;
        jz_hmap_entry(&discovered, &visit.node, &inserted);
        if (!inserted) continue;
        const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, visit.node);
        if (!gn) { err = groove_err_node_not_found(visit.node); goto done; }
        PendingVisit expanded = { visit.node, true };
        jz_vec_push(&pending, &expanded, sizeof(expanded));
        /* Recursive seed and step graphs run under a frontier-scoped evaluator
         * in update_recursive; evaluating them here would populate root-scoped
         * memo and operator state. */
        if (gn->descriptor.op.tag != GROOVE_OP_RECURSIVE) {
            for (u32 i = gn->descriptor.inputs.len; i-- > 0;) {
                PendingVisit child = { gn->descriptor.inputs.ptr[i], false };
                jz_vec_push(&pending, &child, sizeof(child));
            }
        }
    }
    groove_RecordDeltas *result = NULL;
    for (u32 i = 0; i < order.len; i++) {
        groove_RecordDeltas *records = NULL;
        err = TE(update_ready_node)(e, order.ptr[i], &records);
        if (err.code) { if (result) jz_rc_release(result); goto done; }
        if (order.ptr[i].v == root.v) { if (result) jz_rc_release(result); result = records; }
        else jz_rc_release(records);
    }
    if (!result) { err = groove_err_node_not_found(root); goto done; }
    *out = result;
done:
    jz_vec_drop(&pending, sizeof(PendingVisit), NULL);
    jz_hmap_drop(&discovered);
    jz_vec_drop(&order, sizeof(groove_NodeId), NULL);
    return err;
}

/* ---------------------------------------------------------------------
 * Root ordering
 * ------------------------------------------------------------------- */
typedef struct RootKeyCtx {
    const groove_IvmGraph *graph;
    const groove_RootIdentity *identity;
    groove_RecordDescriptor descriptor;
    const groove_RootOrderingWindows *windows;
} RootKeyCtx;

static jz_err root_key_of(void *pctx, const u8 *record, u32 len, jz_bytes *out) {
    RootKeyCtx *c = pctx;
    if (!c->identity->projected)
        return groove__ivm__runtime__windows__encoded_identity_key_part(c->descriptor, record, len, c->windows->identity.ptr, c->windows->identity.len, out);
    bool some = false; groove_RecordDescriptor output; jz_bytes projected = {0};
    JZ_TRY(groove__ivm__runtime__record_projection__project_window_record(c->graph, c->identity->chain.ptr, c->identity->chain.len, c->descriptor, record, len, &some, &output, &projected));
    if (!some) { out->ptr = NULL; out->len = out->cap = 0; return JZ_OK; } /* never an output root */
    jz_err err = groove__ivm__runtime__windows__encoded_identity_key_part(output, projected.ptr, projected.len, c->identity->fields.ptr, c->identity->fields.len, out);
    jz_bytes_drop(&projected);
    return err;
}

static jz_err keyed_rows(const groove_IndexedRecordVec *rows, RootKeyCtx *ctx, groove_IndexedKeyVec *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    for (u32 i = 0; i < rows->len; i++) {
        groove_IndexedKey k = { rows->ptr[i].index, {0} };
        jz_err err = root_key_of(ctx, rows->ptr[i].record.ptr, rows->ptr[i].record.len, &k.key);
        if (err.code) { jz_vec_drop(out, sizeof(groove_IndexedKey), indexed_key_drop); return err; }
        jz_vec_push(out, &k, sizeof(k));
    }
    return JZ_OK;
}

typedef struct ReachCtx { RootKeyCtx *key; const jz_hmap *evidence; } ReachCtx;
static jz_err reaching_rows(ReachCtx *rc, const groove_WindowedRecordVec *window, const groove_WindowedRecordVec *other, bool after, groove_WindowedRecordVec *kept) {
    kept->ptr = NULL; kept->len = kept->cap = 0;
    jz_vec_reserve(kept, window->len, sizeof(groove_WindowedRecord));
    for (u32 i = 0; i < window->len; i++) {
        const groove_WindowedRecord *entry = &window->ptr[i];
        u32 reach;
        JZ_TRY(groove__ivm__runtime__record_projection__window_record_reaches_output(rc->key->graph, rc->key->identity->chain.ptr, rc->key->identity->chain.len, rc->key->descriptor, &entry->record, &reach));
        bool reaches;
        if (reach == GROOVE_WR_YES) reaches = true;
        else if (reach == GROOVE_WR_NO) reaches = false;
        else {
            jz_bytes key = {0};
            JZ_TRY(root_key_of(rc->key, entry->record.ptr, entry->record.len, &key));
            const EvidencePair *seen = jz_hmap_get(rc->evidence, &key);
            jz_bytes_drop(&key);
            if (seen) reaches = after ? seen->now : seen->before;
            else {
                reaches = false;
                for (u32 j = 0; j < other->len; j++)
                    if (jz_bytes_eq(&other->ptr[j].record, &entry->record)) { reaches = true; break; }
            }
        }
        if (reaches) {
            groove_WindowedRecord c = { jz_bytes_clone(&entry->record), entry->weight };
            jz_vec_push(kept, &c, sizeof(c));
        }
    }
    return JZ_OK;
}

jz_err TE(apply_root_ordering)(groove_TickEvaluator *e, groove_NodeId ordering_node, groove_RecordDescriptor root_descriptor, const struct groove_RootIdentity *identity, const jz_btree *groups, groove_TerminalDeltas *terminal) {
    const groove_RootOrderingWindows *windows = jz_hmap_get(&e->root_ordering_windows, &ordering_node);
    if (!windows) return JZ_OK;
    /* Positions are keyed by the TopBy's row identity; an output whose chain
     * does not carry it gets no moves. */
    if (!identity) return JZ_OK;
    if (!windows->has_descriptor) return JZ_OK;
    RootKeyCtx kctx = { e->graph, identity, windows->descriptor, windows };
    groove_RootKeyOf key_of = { root_key_of, &kctx };
    usize visited = 0;
    jz_bytes *group; void *unused;
    if (!identity->filtered) {
        JZ_BTREE_FOR(groups, group, unused) {
            /* Every window row reaches the output, so only the rows whose
             * presence changed need a rank (#3505). */
            groove_ChangedWindowRows changed;
            if (EV(RootOrderingWindows__changed_rows)(windows, group, &changed)) {
                visited += changed.removed.len + changed.added.len;
                groove_GroupChanges changes = {{0}, {0}};
                jz_err err = keyed_rows(&changed.removed, &kctx, &changes.removed);
                if (!err.code) err = keyed_rows(&changed.added, &kctx, &changes.added);
                jz_vec_drop(&changed.removed, sizeof(groove_IndexedRecord), indexed_record_drop);
                jz_vec_drop(&changed.added, sizeof(groove_IndexedRecord), indexed_record_drop);
                if (err.code) { groove__ivm__runtime__record_projection__GroupChanges__drop(&changes); return err; }
                bool applied = groove__ivm__runtime__record_projection__apply_changed_group_ordering(&changes, root_descriptor, terminal);
                groove__ivm__runtime__record_projection__GroupChanges__drop(&changes);
                if (applied) continue;
            }
            groove_WindowedRecordVec before = {0}, after = {0};
            if (EV(RootOrderingWindows__group_window)(windows, group, &before, &after)) {
                visited += before.len + after.len;
                jz_err err = groove__ivm__runtime__record_projection__apply_group_window_ordering(before.ptr, before.len, after.ptr, after.len, key_of, root_descriptor, terminal);
                jz_vec_drop(&before, sizeof(groove_WindowedRecord), windowed_record_drop);
                jz_vec_drop(&after, sizeof(groove_WindowedRecord), windowed_record_drop);
                JZ_TRY(err);
            }
        }
        e->metrics->root_ordering_position_records += visited;
        return JZ_OK;
    }
    /* What this tick's output edits say about each root key. */
    jz_hmap evidence; jz_hmap_init(&evidence, &hdesc_bytes_evidence);
    for (u32 i = 0; i < terminal->operations.len; i++) {
        const groove_TerminalOperation *op = &terminal->operations.ptr[i];
        jz_bytes key = jz_bytes_clone(&op->root_key);
        bool inserted = false;
        EvidencePair *seen = jz_hmap_entry(&evidence, &key, &inserted);
        if (inserted) { seen->before = false; seen->now = false; } else jz_bytes_drop(&key);
        if (op->edit.tag == GROOVE_TE_INSERT && op->path.len == 0) seen->now = true;
        else if (op->edit.tag == GROOVE_TE_REMOVE && op->path.len == 0) seen->before = true;
        else { seen->before = true; seen->now = true; }
    }
    /* Indices are positions among the output's roots, so window rows the
     * chain drops take no position. */
    ReachCtx rctx = { &kctx, &evidence };
    jz_err err = JZ_OK;
    JZ_BTREE_FOR(groups, group, unused) {
        groove_WindowedRecordVec before = {0}, after = {0};
        if (!EV(RootOrderingWindows__group_window)(windows, group, &before, &after)) continue;
        visited += before.len + after.len;
        groove_WindowedRecordVec rb = {0}, ra = {0};
        err = reaching_rows(&rctx, &before, &after, false, &rb);
        if (!err.code) err = reaching_rows(&rctx, &after, &before, true, &ra);
        if (!err.code) err = groove__ivm__runtime__record_projection__apply_group_window_ordering(rb.ptr, rb.len, ra.ptr, ra.len, key_of, root_descriptor, terminal);
        jz_vec_drop(&before, sizeof(groove_WindowedRecord), windowed_record_drop);
        jz_vec_drop(&after, sizeof(groove_WindowedRecord), windowed_record_drop);
        jz_vec_drop(&rb, sizeof(groove_WindowedRecord), windowed_record_drop);
        jz_vec_drop(&ra, sizeof(groove_WindowedRecord), windowed_record_drop);
        if (err.code) break;
    }
    jz_hmap_drop(&evidence);
    JZ_TRY(err);
    e->metrics->root_ordering_position_records += visited;
    return JZ_OK;
}

jz_err TE(terminal_delta_node_for_output)(groove_TickEvaluator *e, groove_NodeId node, bool *has, groove_NodeId *out) {
    groove_TerminalLineage *lineage = NULL;
    JZ_TRY(TE(terminal_lineage)(e, node, &lineage));
    bool has_fallback = false; groove_NodeId fallback = {0};
    *has = false;
    for (u32 i = 0; i < lineage->candidates.len; i++) {
        groove_NodeId candidate = lineage->candidates.ptr[i].node;
        if (jz_hmap_get(&e->terminal_deltas, &candidate)) {
            if (lineage->candidates.ptr[i].is_public_root) { *has = true; *out = candidate; jz_rc_release(lineage); return JZ_OK; }
            if (!has_fallback) { has_fallback = true; fallback = candidate; }
        }
    }
    if (!lineage->has_public_root && has_fallback) { *has = true; *out = fallback; }
    jz_rc_release(lineage);
    return JZ_OK;
}

bool TE(terminal_deltas_for_consumer)(groove_TickEvaluator *e, groove_NodeId node, bool last_consumer, groove_TerminalDeltas *out) {
    if (last_consumer) {
        groove_NodeId k;
        return jz_hmap_remove(&e->terminal_deltas, &node, &k, out);
    }
    const groove_TerminalDeltas *t = jz_hmap_get(&e->terminal_deltas, &node);
    if (!t) return false;
    groove__ivm__runtime__terminal__TerminalDeltas__clone(out, t);
    return true;
}

jz_err TE(output_is_structured_collect_by)(groove_TickEvaluator *e, groove_NodeId node, bool *out) {
    return groove__ivm__runtime__record_projection__output_is_structured_collect_by(e->graph, node, out);
}

jz_err TE(output_has_public_root)(groove_TickEvaluator *e, groove_NodeId node, bool *out) {
    groove_TerminalLineage *lineage = NULL;
    JZ_TRY(TE(terminal_lineage)(e, node, &lineage));
    *out = lineage->has_public_root;
    jz_rc_release(lineage);
    return JZ_OK;
}

static void lineage_drop(void *p) { groove_TerminalLineage *l = p; jz_vec_drop(&l->candidates, sizeof(groove_LineageCandidate), NULL); }

static jz_err TE(terminal_lineage)(groove_TickEvaluator *e, groove_NodeId root, groove_TerminalLineage **out) {
    const groove_NodeRuntimeMeta *meta = jz_hmap_get(e->node_meta, &root);
    if (meta && meta->terminal_lineage) { *out = jz_rc_clone(meta->terminal_lineage); return JZ_OK; }
    groove_TerminalLineage *lineage = jz_rc_new(sizeof(groove_TerminalLineage), lineage_drop);
    groove_NodeIdVec pending = {0};
    jz_hmap seen; jz_hmap_init(&seen, &groove_ivm_rt_a_hdesc_nodeid_set);
    jz_vec_push(&pending, &root, sizeof(root));
    jz_err err = JZ_OK;
    while (pending.len) {
        groove_NodeId node = pending.ptr[--pending.len];
        bool inserted = false;
        jz_hmap_entry(&seen, &node, &inserted);
        if (!inserted) continue;
        const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
        if (!gn) { err = groove_err_node_not_found(node); break; }
        /* CollectBy is the only operator publishing into terminal_deltas.
         * Preserve the old depth-first/input-reverse priority exactly. */
        if (gn->descriptor.op.tag == GROOVE_OP_COLLECT_BY) {
            bool pub = gn->descriptor.op.v.collect_by->mode == GROOVE_CBM_ROOT;
            lineage->has_public_root |= pub;
            groove_LineageCandidate c = { node, pub };
            jz_vec_push(&lineage->candidates, &c, sizeof(c));
        }
        jz_vec_extend(&pending, gn->descriptor.inputs.ptr, gn->descriptor.inputs.len, sizeof(groove_NodeId));
    }
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&seen);
    if (err.code) { jz_rc_release(lineage); return err; }
    groove_NodeRuntimeMeta *m = groove_ivm_rt_a_node_meta_entry(e->node_meta, root);
    if (m->terminal_lineage) jz_rc_release(m->terminal_lineage);
    m->terminal_lineage = jz_rc_clone(lineage);
    *out = lineage;
    return JZ_OK;
}

static i8 cached_hydration_ancestor(const jz_hmap *meta, groove_NodeId node) {
    const groove_NodeRuntimeMeta *m = jz_hmap_get(meta, &node);
    return m ? m->has_hydration_state_ancestor : -1;
}

static jz_err TE(node_depends_on_aggregate)(groove_TickEvaluator *e, groove_NodeId node, bool *out) {
    i8 value = cached_hydration_ancestor(e->node_meta, node);
    if (value >= 0) { *out = value == 1; return JZ_OK; }
    /* Memoizing every visited node keeps a whole graph's classification linear. */
    JZ_VEC(PendingVisit) pending = {0};
    PendingVisit first = { node, false };
    jz_vec_push(&pending, &first, sizeof(first));
    jz_err err = JZ_OK;
    while (pending.len) {
        PendingVisit cur = pending.ptr[--pending.len];
        if (cached_hydration_ancestor(e->node_meta, cur.node) >= 0) continue;
        const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, cur.node);
        if (!gn) { err = groove_err_node_not_found(cur.node); break; }
        bool depends;
        if (EV(holds_hydration_state)(&gn->descriptor.op)) depends = true;
        else if (cur.expanded) {
            depends = false;
            for (u32 i = 0; i < gn->descriptor.inputs.len; i++)
                if (cached_hydration_ancestor(e->node_meta, gn->descriptor.inputs.ptr[i]) == 1) { depends = true; break; }
        } else {
            PendingVisit again = { cur.node, true };
            jz_vec_push(&pending, &again, sizeof(again));
            for (u32 i = 0; i < gn->descriptor.inputs.len; i++) {
                if (cached_hydration_ancestor(e->node_meta, gn->descriptor.inputs.ptr[i]) >= 0) continue;
                PendingVisit child = { gn->descriptor.inputs.ptr[i], false };
                jz_vec_push(&pending, &child, sizeof(child));
            }
            continue;
        }
        groove_ivm_rt_a_node_meta_entry(e->node_meta, cur.node)->has_hydration_state_ancestor = depends ? 1 : 0;
    }
    jz_vec_drop(&pending, sizeof(PendingVisit), NULL);
    JZ_TRY(err);
    value = cached_hydration_ancestor(e->node_meta, node);
    if (value < 0) jz_panic("classified above");
    *out = value == 1;
    return JZ_OK;
}

static jz_err TE(readiness_frontier)(groove_TickEvaluator *e, groove_NodeId root, groove_NodeIdList **out);
static jz_err TE(producer_state_is_current)(groove_TickEvaluator *e, groove_NodeId node, bool *out);

static jz_err TE(aggregate_arrangements_are_current)(groove_TickEvaluator *e, groove_NodeId node, bool *out) {
    groove_NodeIdList *frontier = NULL;
    JZ_TRY(TE(readiness_frontier)(e, node, &frontier));
    *out = true;
    for (u32 i = 0; i < frontier->len; i++) {
        bool current = false;
        jz_err err = TE(producer_state_is_current)(e, frontier->items[i], &current);
        if (err.code) { jz_rc_release(frontier); return err; }
        if (!current) { *out = false; break; }
    }
    jz_rc_release(frontier);
    return JZ_OK;
}

static void nodeid_list_drop(void *p) { jz_free(((groove_NodeIdList *)p)->items); }

static jz_err TE(readiness_frontier)(groove_TickEvaluator *e, groove_NodeId root, groove_NodeIdList **out) {
    const groove_NodeRuntimeMeta *meta = jz_hmap_get(e->node_meta, &root);
    if (meta && meta->readiness_frontier) { *out = jz_rc_clone(meta->readiness_frontier); return JZ_OK; }
    groove_NodeIdVec frontier = {0}, pending = {0};
    jz_hmap seen; jz_hmap_init(&seen, &groove_ivm_rt_a_hdesc_nodeid_set);
    jz_vec_push(&pending, &root, sizeof(root));
    jz_err err = JZ_OK;
    while (pending.len) {
        groove_NodeId node = pending.ptr[--pending.len];
        bool inserted = false;
        jz_hmap_entry(&seen, &node, &inserted);
        if (!inserted) continue;
        const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
        if (!gn) { err = groove_err_node_not_found(node); break; }
        switch (gn->descriptor.op.tag) {
        case GROOVE_OP_RECURSIVE:
            /* Child scopes are owned by this producer. */
            jz_vec_push(&frontier, &node, sizeof(node));
            continue;
        case GROOVE_OP_ARRANGE: case GROOVE_OP_ARG_MIN_BY: case GROOVE_OP_ARG_MAX_BY: case GROOVE_OP_AGGREGATE:
            jz_vec_push(&frontier, &node, sizeof(node));
            break;
        default: break;
        }
        for (u32 i = gn->descriptor.inputs.len; i-- > 0;) jz_vec_push(&pending, &gn->descriptor.inputs.ptr[i], sizeof(groove_NodeId));
    }
    jz_vec_drop(&pending, sizeof(groove_NodeId), NULL);
    jz_hmap_drop(&seen);
    if (err.code) { jz_vec_drop(&frontier, sizeof(groove_NodeId), NULL); return err; }
    groove_NodeIdList *list = jz_rc_new(sizeof(groove_NodeIdList), nodeid_list_drop);
    list->len = frontier.len; list->items = frontier.ptr;
    groove_NodeRuntimeMeta *m = groove_ivm_rt_a_node_meta_entry(e->node_meta, root);
    if (m->readiness_frontier) jz_rc_release(m->readiness_frontier);
    m->readiness_frontier = jz_rc_clone(list);
    *out = list;
    return JZ_OK;
}

static bool as_of_eq(const groove_AsOfStamp *s, groove_SubTick expected) {
    return s->has && s->at.tick == expected.tick && s->at.sub_tick == expected.sub_tick;
}

static jz_err TE(producer_state_is_current)(groove_TickEvaluator *e, groove_NodeId node, bool *out) {
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) return groove_err_node_not_found(node);
    const groove_OpType *op = &gn->descriptor.op;
    const groove_NodeIdVec *inputs = &gn->descriptor.inputs;
    if (op->tag == GROOVE_OP_RECURSIVE) {
        /* The recursive operator owns its seed/step scopes; its completed
         * hydration is the proof that those child scopes are ready. */
        groove_OperatorStateKey key = TE(operator_key)(e, node);
        u64 generation = TE(input_generation)(e, node);
        const groove_OperatorState *st = jz_hmap_get(e->operator_states, &key);
        u64 hydrated = 0;
        groove_SubTick tick = { e->current_tick, 0 };
        *out = st && st->tag == GROOVE_OS_RECURSIVE
            && as_of_eq(&st->v.recursive.as_of, tick)
            && !groove__ivm__runtime__recursion__RecursiveState__has_pending_hydration(st->v.recursive.value)
            && groove__ivm__runtime__recursion__RecursiveState__step_arrangements_hydrated(st->v.recursive.value)
            && groove__ivm__runtime__recursion__RecursiveState__hydrated_input_generation(st->v.recursive.value, &hydrated)
            && hydrated == generation;
        return JZ_OK;
    }
    if (op->tag == GROOVE_OP_ARRANGE && TE(arrangement_needs_index)(e, node)) {
        groove_ArrangementKey key = { e->context.scope, node };
        const groove_AsOfArrangement *st = jz_hmap_get(e->arrangement_states, &key);
        if (!st || !as_of_eq(&st->as_of, TE(arrangement_sub_tick)(e, &key))) { *out = false; return JZ_OK; }
    }
    if (op->tag == GROOVE_OP_ARG_MIN_BY || op->tag == GROOVE_OP_ARG_MAX_BY) {
        groove_OperatorStateKey key = TE(operator_key)(e, node);
        groove_SubTick expected = { e->current_tick, key.scope == groove__ivm__runtime__state__ScopeId__root() ? 0 : e->context.sub_tick };
        const groove_OperatorState *st = jz_hmap_get(e->operator_states, &key);
        if (!(st && st->tag == GROOVE_OS_ARG_BY && as_of_eq(&st->v.arg_by.as_of, expected))) { *out = false; return JZ_OK; }
    }
    if (op->tag == GROOVE_OP_AGGREGATE) {
        if (inputs->len != 1) return groove_err_arity(node);
        groove_NodeId input = inputs->ptr[0];
        const groove_GraphNode *in = groove__ivm__graph__IvmGraph__node(e->graph, input);
        if (!in) return groove_err_node_not_found(input);
        groove_StrList *group_fields = TE(aggregate_group_fields)(e, node, &op->v.aggregate);
        groove_ArrangementKey akey;
        jz_err err = TE(arrangement_key)(e, input, groove_NodeOutput_records(in->descriptor.output), group_fields->items, group_fields->len, GROOVE_VC_EXACT, &akey);
        jz_rc_release(group_fields);
        JZ_TRY(err);
        const groove_AsOfArrangement *st = jz_hmap_get(e->arrangement_states, &akey);
        if (!st || !as_of_eq(&st->as_of, TE(arrangement_sub_tick)(e, &akey))) { *out = false; return JZ_OK; }
    }
    *out = true;
    return JZ_OK;
}

/* The postorder driver has already evaluated ordinary inputs. Check their
 * memo before entering another evaluator frame. */
jz_err TE(update_node)(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDeltas **out) {
    groove_NodeMemoLookup lookup;
    JZ_TRY(TE(prepare_memo_lookup)(e, node, &lookup));
    groove_RecordDeltas *cached = NULL;
    JZ_TRY(TE(cached_node_records)(e, &lookup, &cached));
    if (cached) { *out = cached; return JZ_OK; }
    return TE(update_subgraph)(e, node, out);
}

static groove_NodeMemoLookup prepare_lookup(const groove_TickEvaluator *e, groove_NodeId node, const groove_NodeInputSignature *sig, u64 input_watermark) {
    groove_NodeMemoLookup l;
    l.key = TE(memo_key)(e, node, sig);
    l.input_watermark = input_watermark;
    l.depends_on_context = sig->frontier_bindings.len != 0;
    return l;
}

jz_err TE(prepare_memo_lookup)(groove_TickEvaluator *e, groove_NodeId node, groove_NodeMemoLookup *out) {
    /* The common path borrows the compiled signature and its watermark. */
    const groove_NodeRuntimeMeta *meta = jz_hmap_get(e->node_meta, &node);
    if (meta && meta->input_signature) {
        *out = prepare_lookup(e, node, meta->input_signature, meta->input_generation);
        return JZ_OK;
    }
    groove_NodeInputSignature *sig = NULL;
    JZ_TRY(TE(input_signature)(e, node, &sig));
    *out = prepare_lookup(e, node, sig, TE(input_generation)(e, node));
    jz_rc_release(sig);
    return JZ_OK;
}

jz_err TE(cached_node_records)(groove_TickEvaluator *e, const groove_NodeMemoLookup *lookup, groove_RecordDeltas **out) {
    groove_NodeId node = lookup->key.node;
    u64 current_watermark = lookup->input_watermark;
    *out = NULL;
    /* Readiness is necessary only when reusing a result. */
    groove_EvalMemoEntry *entry = groove__ivm__runtime__evaluation_memo__EvaluationMemo__get(e->eval_memo, &lookup->key);
    if (!entry || entry->input_watermark != current_watermark) return JZ_OK;
    bool current = false;
    JZ_TRY(TE(cached_result_state_is_current)(e, node, &current));
    if (!current) return JZ_OK;
    entry = groove__ivm__runtime__evaluation_memo__EvaluationMemo__get(e->eval_memo, &lookup->key);
    if (entry && entry->input_watermark == current_watermark) {
        *e->memo_use_clock += 1;
        entry->last_used = *e->memo_use_clock;
        if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) e->metrics->hydration_memo_hits += 1;
        *out = jz_rc_clone(entry->records);
    }
    return JZ_OK;
}

static jz_err TE(cached_result_state_is_current)(groove_TickEvaluator *e, groove_NodeId node, bool *out) {
    /* A cached record batch is not proof that its producer-owned physical
     * index exists in this scope. */
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    u32 tag = gn ? gn->descriptor.op.tag : (u32)-1;
    bool requires_producer;
    if (tag == GROOVE_OP_ARRANGE && TE(arrangement_needs_index)(e, node)) {
        groove_ArrangementKey key = { e->context.scope, node };
        requires_producer = jz_hmap_get(e->arrangement_states, &key) == NULL;
    } else if (tag == GROOVE_OP_LOOKUP && e->context.eval_mode == GROOVE_EVAL_HYDRATE) {
        /* A lookup's records are reusable only with the key set and matched
         * rows that maintain them. */
        groove_OperatorStateKey key = TE(operator_key)(e, node);
        const groove_OperatorState *st = jz_hmap_get(e->operator_states, &key);
        requires_producer = !(st && st->tag == GROOVE_OS_LOOKUP && st->v.lookup.as_of.has);
    } else {
        requires_producer = false;
    }
    bool rebuild = requires_producer;
    if (!rebuild && e->context.hydrate_arrangements) {
        bool depends = false;
        JZ_TRY(TE(node_depends_on_aggregate)(e, node, &depends));
        if (depends) {
            bool current = false;
            JZ_TRY(TE(aggregate_arrangements_are_current)(e, node, &current));
            rebuild = !current;
        }
    }
    if (!rebuild) rebuild = e->context.eval_mode == GROOVE_EVAL_TICK && e->context.arrangement_update_mode == GROOVE_AUM_REPLACE;
    *out = !rebuild;
    return JZ_OK;
}

jz_err TE(resolve_register_inputs)(groove_TickEvaluator *e, groove_FrameInputs inputs, bool *some, groove_ArcDeltasVec *out) {
    groove_ArcDeltasVec resolved = {0};
    *some = false;
    for (u32 i = 0; i < inputs.len; i++) {
        usize slot = inputs.slots[i];
        const groove_EvalMemoKey *key; groove_EvalMemoEntry *entry;
        if (!groove__ivm__runtime__evaluation_memo__EvaluationMemo__slot(e->eval_memo, slot, &key, &entry)) goto none;
        groove_NodeId node = key->node;
        groove_NodeMemoLookup lookup;
        jz_err err = TE(prepare_memo_lookup)(e, node, &lookup);
        if (err.code) { arc_deltas_vec_drop(&resolved); return err; }
        if (!groove__ivm__runtime__evaluation_memo__EvaluationMemo__slot(e->eval_memo, slot, &key, &entry)
            || memcmp(key, &lookup.key, sizeof(groove_EvalMemoKey)) != 0
            || entry->input_watermark != lookup.input_watermark)
            goto none;
        bool current = false;
        err = TE(cached_result_state_is_current)(e, node, &current);
        if (err.code) { arc_deltas_vec_drop(&resolved); return err; }
        if (!current) goto none;
        /* Readiness is checked live above, never stored in the register. */
        if (!groove__ivm__runtime__evaluation_memo__EvaluationMemo__slot(e->eval_memo, slot, &key, &entry)) jz_panic("validated frame slot");
        *e->memo_use_clock += 1;
        entry->last_used = *e->memo_use_clock;
        if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) e->metrics->hydration_memo_hits += 1;
        groove_RecordDeltas *records = jz_rc_clone(entry->records);
        jz_vec_push(&resolved, &records, sizeof(records));
    }
    *some = true;
    *out = resolved;
    return JZ_OK;
none:
    arc_deltas_vec_drop(&resolved);
    return JZ_OK;
}

/* Execute a node whose ordinary inputs have already been driven by the
 * caller's dependency queue (or scoped postorder driver). */
jz_err TE(update_ready_node)(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDeltas **out) {
    groove_NodeMemoLookup lookup;
    JZ_TRY(TE(prepare_memo_lookup)(e, node, &lookup));
    groove_RecordDeltas *cached = NULL;
    JZ_TRY(TE(cached_node_records)(e, &lookup, &cached));
    if (cached) { *out = cached; return JZ_OK; }
    bool handled = false;
    jz_err err = TE(compute_ready_batch)(e, node, &lookup, NULL, &handled, out);
    if (handled || err.code) return err;
    return TE(compute_node)(e, node, lookup, out);
}

static jz_err TE(ready_unary_input)(groove_TickEvaluator *e, groove_NodeId node, const groove_NodeMemoLookup *lookup, bool *some, groove_RecordDeltas **out) {
    *some = false;
    if (e->context.sub_tick > 1 && !lookup->depends_on_context) return JZ_OK;
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) return JZ_OK;
    if (gn->descriptor.op.tag != GROOVE_OP_FILTER && gn->descriptor.op.tag != GROOVE_OP_MAP_PROJECT) return JZ_OK;
    if (gn->descriptor.inputs.len != 1) { *some = true; return groove_err_arity(node); }
    groove_NodeMemoLookup key;
    JZ_TRY(TE(prepare_memo_lookup)(e, gn->descriptor.inputs.ptr[0], &key));
    groove_RecordDeltas *input = NULL;
    JZ_TRY(TE(cached_node_records)(e, &key, &input));
    if (input) { *some = true; *out = input; }
    return JZ_OK;
}

static jz_err TE(compute_unary_input)(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDeltas *input, groove_RecordDeltas *out) {
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) jz_panic("checked unary node");
    groove_RecordDescriptor output = groove_NodeOutput_records(gn->descriptor.output);
    if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) {
        e->metrics->hydration_memo_computes += 1;
        jz_hmap_insert(&e->metrics->hydration_memo_computed_nodes, &node, NULL, NULL);
    }
    switch (gn->descriptor.op.tag) {
    case GROOVE_OP_FILTER: return TE(compute_filter)(e, node, &gn->descriptor.op.v.filter, output, input, out);
    case GROOVE_OP_MAP_PROJECT: return TE(compute_projection)(e, node, &gn->descriptor.op.v.map_project, output, input, out);
    default: jz_panic("internal error: entered unreachable code: checked unary kernel");
    }
}

groove_RecordDeltas *TE(memoize_result)(groove_TickEvaluator *e, const groove_NodeMemoLookup *lookup, groove_RecordDeltas result) {
    e->metrics->records_processed += result.deltas.len;
    e->metrics->nodes_evaluated += 1;
    groove_RecordDeltas *arc = groove__ivm__runtime__subscriptions__RecordDeltas__arc_new(result);
    usize payload_bytes = groove__ivm__runtime__subscriptions__record_deltas_encoded_bytes(arc);
    *e->memo_use_clock += 1;
    groove_EvalMemoEntry entry = { jz_rc_clone(arc), lookup->input_watermark, payload_bytes, *e->memo_use_clock };
    groove_EvalMemoEntry previous;
    if (groove__ivm__runtime__evaluation_memo__EvaluationMemo__insert(e->eval_memo, &lookup->key, entry, &previous)) {
        *e->eval_memo_bytes = *e->eval_memo_bytes > previous.payload_bytes ? *e->eval_memo_bytes - previous.payload_bytes : 0;
        groove__ivm__runtime__state__EvalMemoEntry__drop(&previous);
    }
    usize sum = *e->eval_memo_bytes + payload_bytes;
    *e->eval_memo_bytes = sum < payload_bytes ? (usize)-1 : sum;
    return arc;
}

/* ---------------------------------------------------------------------
 * Kernels: filter / projection / compute_node
 * ------------------------------------------------------------------- */
jz_err TE(compute_filter)(groove_TickEvaluator *e, groove_NodeId node, const groove_FilterOp *filter, groove_RecordDescriptor output_desc, groove_RecordDeltas *input, groove_RecordDeltas *out) {
    if (groove__ivm__runtime__subscriptions__PredicateExpr__supports_indirect_literal_attempt(&filter->predicate) && e->evaluation_inputs) {
        groove_EvaluationInputs *inputs = e->evaluation_inputs;
        groove_RecordDeltaVec deltas = {0};
        for (u32 i = 0; i < input->deltas.len; i++) {
            const groove_RecordDelta *delta = &input->deltas.ptr[i];
            groove_BorrowedRecord record = groove_RecordDelta_borrowed(delta, input->descriptor);
            groove__ivm__runtime__evaluation_session__EvaluationInputs__set_chunk_scope(inputs, true, node);
            i8 attempt = -1;
            jz_err err = groove__ivm__runtime__subscriptions__PredicateExpr__matches_indirect_literal_attempt(&filter->predicate, record, inputs, &attempt);
            groove__ivm__runtime__evaluation_session__EvaluationInputs__set_chunk_scope(inputs, false, node);
            bool matches = false;
            if (!err.code) {
                if (attempt >= 0) matches = attempt == 1;
                else err = groove__ivm__runtime__subscriptions__PredicateExpr__matches_borrowed(&filter->predicate, record, filter->comparison, &matches);
            }
            if (err.code) { jz_vec_drop(&deltas, sizeof(groove_RecordDelta), groove__ivm__runtime__subscriptions__RecordDelta__drop); return err; }
            if (matches) {
                groove_RecordDelta c; groove__ivm__runtime__subscriptions__RecordDelta__clone(&c, delta);
                jz_vec_push(&deltas, &c, sizeof(c));
            }
        }
        out->descriptor = output_desc; out->deltas = deltas;
        return JZ_OK;
    }
    jz_btree referenced; jz_btree_init(&referenced, &groove_ivm_rt_a_desc_str_set);
    groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(&filter->predicate, &referenced);
    groove_RecordDeltas *materialized = NULL;
    jz_err err = TE(materialize_indirect_fields)(e, input, &referenced, &materialized);
    jz_btree_drop(&referenced);
    JZ_TRY(err);
    if (materialized == input) {
        jz_rc_release(materialized);
        return groove__ivm__runtime__operator_updates__NodeState__update_filter(filter, output_desc, input, out);
    }
    /* Materialization only serves the predicate. Emit the rows as they
     * arrived, so downstream keys see the same physical form as upstream. */
    groove_RecordDeltaVec deltas = {0};
    u32 n = input->deltas.len < materialized->deltas.len ? input->deltas.len : materialized->deltas.len;
    for (u32 i = 0; i < n; i++) {
        bool matches = false;
        err = groove__ivm__runtime__subscriptions__PredicateExpr__matches_borrowed(&filter->predicate, groove_RecordDelta_borrowed(&materialized->deltas.ptr[i], materialized->descriptor), filter->comparison, &matches);
        if (err.code) break;
        if (matches) {
            groove_RecordDelta c; groove__ivm__runtime__subscriptions__RecordDelta__clone(&c, &input->deltas.ptr[i]);
            jz_vec_push(&deltas, &c, sizeof(c));
        }
    }
    jz_rc_release(materialized);
    if (err.code) { jz_vec_drop(&deltas, sizeof(groove_RecordDelta), groove__ivm__runtime__subscriptions__RecordDelta__drop); return err; }
    out->descriptor = output_desc; out->deltas = deltas;
    return JZ_OK;
}

jz_err TE(compute_projection)(groove_TickEvaluator *e, groove_NodeId node, const groove_MapProjectOp *project, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out) {
    groove_PreparedProjection *raw = NULL;
    JZ_TRY(TE(raw_projection_fields)(e, node, project, input->descriptor, output_desc, &raw));
    jz_err err = groove__ivm__runtime__operator_updates__NodeState__update_map_project(project, output_desc, input, raw, false, out);
    if (raw) groove__records__PreparedProjection__release(raw);
    return err;
}

static void TE(note_hydration_compute)(groove_TickEvaluator *e, groove_NodeId node) {
    if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) {
        e->metrics->hydration_memo_computes += 1;
        jz_hmap_insert(&e->metrics->hydration_memo_computed_nodes, &node, NULL, NULL);
    }
}

static jz_err TE(update_unary_input)(groove_TickEvaluator *e, const groove_GraphNode *gn, groove_NodeId node, groove_RecordDeltas **out);
static jz_err TE(update_streaming_checksum)(groove_TickEvaluator *e, groove_NodeId node, const groove_StreamingChecksumOp *checksum, groove_RecordDescriptor output_desc, groove_RecordDeltas *input, groove_RecordDeltas *out);
static jz_err TE(update_recursive)(groove_TickEvaluator *e, groove_NodeId node, const groove_RecursiveOp *rec, groove_RecordDescriptor output_desc, groove_NodeId seed, groove_NodeId step, bool has_witness, groove_NodeId witness, groove_RecordDeltas *out);

/* Construct the large operator frame only on a memo miss. Only external
 * requests and recursive scopes need it; ordinary kernels share compute_batch. */
jz_err TE(compute_node)(groove_TickEvaluator *e, groove_NodeId node, groove_NodeMemoLookup lookup, groove_RecordDeltas **out) {
    TE(note_hydration_compute)(e, node);
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (e->context.sub_tick > 1 && !lookup.depends_on_context) {
        if (!gn) return groove_err_node_not_found(node);
        *out = TE(memoize_result)(e, &lookup, groove_RecordDeltas_empty(groove_NodeOutput_records(gn->descriptor.output)));
        return JZ_OK;
    }
    if (!gn) return groove_err_node_not_found(node);
    groove_RecordDescriptor output_desc = groove_NodeOutput_records(gn->descriptor.output);
    groove_RecordDeltas result = groove_RecordDeltas_empty(output_desc);
    const groove_OpType *op = &gn->descriptor.op;
    if (op->tag == GROOVE_OP_INDEX_SOURCE && (e->context.eval_mode != GROOVE_EVAL_HYDRATE || !e->evaluation_inputs)) {
        JZ_TRY(groove__ivm__runtime__operator_updates__NodeState__update_index_source(&op->v.index_source, e->schema, e->variant_projections, output_desc, e->table_deltas, e->n_table_deltas, e->storage, e->context.eval_mode, &result));
    } else if (op->tag == GROOVE_OP_STREAMING_CHECKSUM) {
        groove_RecordDeltas *input = NULL;
        JZ_TRY(TE(update_unary_input)(e, gn, node, &input));
        jz_err err = TE(update_streaming_checksum)(e, node, &op->v.streaming_checksum, output_desc, input, &result);
        jz_rc_release(input);
        JZ_TRY(err);
    } else if (op->tag == GROOVE_OP_RECURSIVE) {
        groove_NodeId seed, step, witness = {0};
        bool has_witness = false;
        if (gn->descriptor.inputs.len == 2) { seed = gn->descriptor.inputs.ptr[0]; step = gn->descriptor.inputs.ptr[1]; }
        else if (gn->descriptor.inputs.len == 3) { seed = gn->descriptor.inputs.ptr[0]; step = gn->descriptor.inputs.ptr[1]; witness = gn->descriptor.inputs.ptr[2]; has_witness = true; }
        else return groove_err_arity(node);
        JZ_TRY(TE(update_recursive)(e, node, &op->v.recursive, output_desc, seed, step, has_witness, witness, &result));
    } else {
        groove_ArcDeltasVec inputs = {0};
        for (u32 i = 0; i < gn->descriptor.inputs.len; i++) {
            groove_RecordDeltas *in = NULL;
            jz_err err = TE(update_node)(e, gn->descriptor.inputs.ptr[i], &in);
            if (err.code) { arc_deltas_vec_drop(&inputs); return err; }
            jz_vec_push(&inputs, &in, sizeof(in));
        }
        jz_err err = groove__ivm__runtime__evaluator__kernels__TickEvaluator__compute_batch(e, node, inputs.ptr, inputs.len, &result);
        arc_deltas_vec_drop(&inputs);
        JZ_TRY(err);
    }
    *out = TE(memoize_result)(e, &lookup, result);
    return JZ_OK;
}

/* Resolve every input before mutating operator state. */
static jz_err TE(compute_ready_batch)(groove_TickEvaluator *e, groove_NodeId node, const groove_NodeMemoLookup *lookup, const groove_ArcDeltasVec *ready_inputs, bool *handled, groove_RecordDeltas **out) {
    *handled = false;
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) return JZ_OK;
    if (e->context.sub_tick > 1 && !lookup->depends_on_context) {
        TE(note_hydration_compute)(e, node);
        *handled = true;
        *out = TE(memoize_result)(e, lookup, groove_RecordDeltas_empty(groove_NodeOutput_records(gn->descriptor.output)));
        return JZ_OK;
    }
    if (!TE(supports_resident_batch)(e, node)) return JZ_OK;
    groove_RecordDeltas result;
    if (ready_inputs) {
        TE(note_hydration_compute)(e, node);
        *handled = true;
        JZ_TRY(groove__ivm__runtime__evaluator__kernels__TickEvaluator__compute_batch(e, node, ready_inputs->ptr, ready_inputs->len, &result));
        *out = TE(memoize_result)(e, lookup, result);
        return JZ_OK;
    }
    groove_ArcDeltasVec inputs = {0};
    for (u32 i = 0; i < gn->descriptor.inputs.len; i++) {
        groove_NodeMemoLookup key;
        groove_RecordDeltas *records = NULL;
        jz_err err = TE(prepare_memo_lookup)(e, gn->descriptor.inputs.ptr[i], &key);
        if (!err.code) err = TE(cached_node_records)(e, &key, &records);
        if (err.code) { arc_deltas_vec_drop(&inputs); *handled = true; return err; }
        if (!records) { arc_deltas_vec_drop(&inputs); return JZ_OK; }
        jz_vec_push(&inputs, &records, sizeof(records));
    }
    TE(note_hydration_compute)(e, node);
    *handled = true;
    jz_err err = groove__ivm__runtime__evaluator__kernels__TickEvaluator__compute_batch(e, node, inputs.ptr, inputs.len, &result);
    arc_deltas_vec_drop(&inputs);
    JZ_TRY(err);
    *out = TE(memoize_result)(e, lookup, result);
    return JZ_OK;
}

static bool TE(supports_resident_batch)(const groove_TickEvaluator *e, groove_NodeId node) {
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) return false;
    switch (gn->descriptor.op.tag) {
    case GROOVE_OP_RECURSIVE: case GROOVE_OP_STREAMING_CHECKSUM: return false;
    case GROOVE_OP_INDEX_SOURCE: return e->context.eval_mode == GROOVE_EVAL_HYDRATE && e->evaluation_inputs != NULL;
    default: return true;
    }
}

groove_EvalMemoKey TE(memo_key)(const groove_TickEvaluator *e, groove_NodeId node, const groove_NodeInputSignature *sig) {
    groove_EvalMemoKey k;
    memset(&k, 0, sizeof(k));
    k.scope = e->context.scope;
    k.node = node;
    k.input_signature_hash = sig->hash;
    k.has_tick_epoch = e->context.eval_mode == GROOVE_EVAL_TICK;
    k.tick_epoch = k.has_tick_epoch ? e->current_tick : 0;
    k.sub_tick = e->context.sub_tick;
    k.context_digest = TE(context_digest)(e, sig);
    return k;
}

u64 TE(input_generation)(const groove_TickEvaluator *e, groove_NodeId node) {
    const groove_NodeRuntimeMeta *m = jz_hmap_get(e->node_meta, &node);
    return m ? m->input_generation : 0;
}

u64 TE(context_digest)(const groove_TickEvaluator *e, const groove_NodeInputSignature *sig) {
    if (sig->frontier_bindings.len == 0) return 0;
    u64 h = 0;
    for (u32 i = 0; i < sig->frontier_bindings.len; i++) {
        const groove_FrontierName *b = &sig->frontier_bindings.ptr[i];
        h = groove_hash_FrontierName(b, h);
        const u64 *digest = jz_hmap_get(&e->context.binding_digests, b);
        h = jz_hash_combine(h, digest ? *digest : 0);
    }
    return h;
}

/* Recursive step evaluation must be isolated per recursive node even for
 * context-independent table/index inputs; scoped child state is tick-local. */
groove_OperatorStateKey TE(operator_key)(const groove_TickEvaluator *e, groove_NodeId node) {
    groove_OperatorStateKey k = { e->context.scope, node };
    return k;
}

static int cmp_frontier_name(const void *a, const void *b, void *ctx) { (void)ctx; return jz_str_cmp(&((const groove_FrontierName *)a)->name, &((const groove_FrontierName *)b)->name); }
static void drop_frontier_name(void *p) { jz_str_drop(&((groove_FrontierName *)p)->name); }
static int cmp_binding_key(const void *pa, const void *pb, void *ctx) {
    const groove_BindingSourceKey *a = pa, *b = pb; (void)ctx;
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    if (a->tag == GROOVE_BSK_PREPARED) return jz_str_cmp(&a->prepared, &b->prepared);
    if (a->runtime_namespace != b->runtime_namespace) return a->runtime_namespace < b->runtime_namespace ? -1 : 1;
    return a->local < b->local ? -1 : a->local > b->local;
}
static void drop_binding_key(void *p) { jz_str_drop(&((groove_BindingSourceKey *)p)->prepared); }
static const jz_btree_desc desc_frontier_set = { sizeof(groove_FrontierName), 0, cmp_frontier_name, drop_frontier_name, NULL };
static const jz_btree_desc desc_binding_key_set = { sizeof(groove_BindingSourceKey), 0, cmp_binding_key, drop_binding_key, NULL };

static void set_insert_str(jz_btree *set, const jz_str *s) {
    if (jz_btree_contains(set, s)) return;
    jz_str c = jz_str_clone(s);
    jz_btree_insert(set, &c, NULL, NULL);
}
static void set_insert_binding(jz_btree *set, const groove_BindingSourceKey *k) {
    if (jz_btree_contains(set, k)) return;
    groove_BindingSourceKey c = *k; c.prepared = jz_str_clone(&k->prepared);
    jz_btree_insert(set, &c, NULL, NULL);
}
static void set_insert_frontier(jz_btree *set, const groove_FrontierName *f) {
    if (jz_btree_contains(set, f)) return;
    groove_FrontierName c = { jz_str_clone(&f->name) };
    jz_btree_insert(set, &c, NULL, NULL);
}

static jz_err TE(input_signature_inner)(groove_TickEvaluator *e, groove_NodeId node, jz_hmap *seen, groove_NodeInputSignature **out) {
    const groove_NodeRuntimeMeta *meta = jz_hmap_get(e->node_meta, &node);
    if (meta && meta->input_signature) { *out = jz_rc_clone(meta->input_signature); return JZ_OK; }
    bool inserted = false;
    jz_hmap_entry(seen, &node, &inserted);
    if (!inserted) { *out = groove__ivm__runtime__state__NodeInputSignature__default(); return JZ_OK; }
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) return groove_err_node_not_found(node);
    jz_btree tables, bindings, frontier_bindings;
    jz_btree_init(&tables, &groove_ivm_rt_a_desc_str_set);
    jz_btree_init(&bindings, &desc_binding_key_set);
    jz_btree_init(&frontier_bindings, &desc_frontier_set);
    const groove_OpType *op = &gn->descriptor.op;
    switch (op->tag) {
    case GROOVE_OP_TABLE_SOURCE: set_insert_str(&tables, &op->v.table_source.table); break;
    case GROOVE_OP_INDEX_SOURCE: set_insert_str(&tables, &op->v.index_source.table); break;
    case GROOVE_OP_LOOKUP: set_insert_str(&tables, &op->v.lookup.source.table); break;
    case GROOVE_OP_BINDING_SOURCE: set_insert_binding(&bindings, &op->v.binding_source.key); break;
    case GROOVE_OP_FRONTIER_SOURCE: set_insert_frontier(&frontier_bindings, &op->v.frontier_source.binding); break;
    default: break;
    }
    for (u32 i = 0; i < gn->descriptor.inputs.len; i++) {
        groove_NodeInputSignature *child = NULL;
        jz_err err = TE(input_signature_inner)(e, gn->descriptor.inputs.ptr[i], seen, &child);
        if (err.code) { jz_btree_drop(&tables); jz_btree_drop(&bindings); jz_btree_drop(&frontier_bindings); return err; }
        for (u32 j = 0; j < child->tables.len; j++) set_insert_str(&tables, &child->tables.ptr[j]);
        for (u32 j = 0; j < child->bindings.len; j++) set_insert_binding(&bindings, &child->bindings.ptr[j]);
        for (u32 j = 0; j < child->frontier_bindings.len; j++) set_insert_frontier(&frontier_bindings, &child->frontier_bindings.ptr[j]);
        jz_rc_release(child);
    }
    jz_hmap_remove(seen, &node, NULL, NULL);
    groove_NodeInputSignature *sig = groove__ivm__runtime__state__NodeInputSignature__from_sets(&tables, &bindings, &frontier_bindings);
    bool depends_on_context = sig->frontier_bindings.len != 0;
    groove_NodeRuntimeMeta *m = groove_ivm_rt_a_node_meta_entry(e->node_meta, node);
    m->depends_on_context = depends_on_context ? 1 : 0;
    if (m->input_signature) jz_rc_release(m->input_signature);
    m->input_signature = jz_rc_clone(sig);
    *out = sig;
    return JZ_OK;
}

jz_err TE(input_signature)(groove_TickEvaluator *e, groove_NodeId node, groove_NodeInputSignature **out) {
    jz_hmap seen; jz_hmap_init(&seen, &groove_ivm_rt_a_hdesc_nodeid_set);
    jz_err err = TE(input_signature_inner)(e, node, &seen, out);
    jz_hmap_drop(&seen);
    return err;
}

jz_err TE(raw_projection_fields)(groove_TickEvaluator *e, groove_NodeId node, const groove_MapProjectOp *project, groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, groove_PreparedProjection **out) {
    const groove_NodeRuntimeMeta *meta = jz_hmap_get(e->node_meta, &node);
    if (meta && meta->has_raw_projection_fields) {
        *out = meta->raw_projection_fields ? jz_rc_clone(meta->raw_projection_fields) : NULL;
        return JZ_OK;
    }
    groove_PreparedProjection *resolved = NULL;
    JZ_TRY(groove__ivm__runtime__record_projection__raw_projection_fields(project, input_desc, output_desc, &resolved));
    groove_NodeRuntimeMeta *m = groove_ivm_rt_a_node_meta_entry(e->node_meta, node);
    if (m->raw_projection_fields) groove__records__PreparedProjection__release(m->raw_projection_fields);
    m->has_raw_projection_fields = true;
    m->raw_projection_fields = resolved ? jz_rc_clone(resolved) : NULL;
    *out = resolved;
    return JZ_OK;
}

jz_err TE(frontier_source)(const groove_TickEvaluator *e, const groove_FrontierSourceOp *fs, groove_RecordDescriptor output, groove_RecordDeltas *out) {
    const groove_RecordDeltas *bound = jz_hmap_get(&e->context.bindings, &fs->binding);
    groove_RecordDeltas deltas;
    if (bound) groove__ivm__runtime__subscriptions__RecordDeltas__clone(&deltas, bound);
    else deltas = groove_RecordDeltas_empty(output);
    if (!groove__records__RecordDescriptor__registry_compatible_with(deltas.descriptor, output)) {
        groove__ivm__runtime__subscriptions__RecordDeltas__drop(&deltas);
        return groove_err_output_mismatch();
    }
    *out = deltas;
    return JZ_OK;
}

/* ---------------------------------------------------------------------
 * Joins
 * ------------------------------------------------------------------- */
static void TE(join_field_names)(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_StrList **left, groove_StrList **right);

jz_err TE(update_join)(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_RecordDescriptor output_desc, groove_NodeId left_input, groove_NodeId right_input, groove_RecordDeltas *ld, groove_RecordDeltas *rd, groove_RecordDeltas *out) {
    (void)ld; (void)rd;
    groove_StrList *left_on, *right_on;
    TE(join_field_names)(e, node, join, &left_on, &right_on);
    groove_PreparedRecordCopy *projection = NULL;
    groove_ArrangementKey left_key, right_key;
    jz_err err = TE(join_output_projection)(e, node, join->left_descriptor, join->right_descriptor, output_desc, &projection);
    if (!err.code) err = TE(arrangement_key)(e, left_input, join->left_descriptor, left_on->items, left_on->len, join->comparison, &left_key);
    if (!err.code) err = TE(arrangement_key)(e, right_input, join->right_descriptor, right_on->items, right_on->len, join->comparison, &right_key);
    jz_rc_release(left_on); jz_rc_release(right_on);
    if (err.code) { if (projection) jz_rc_release(projection); return err; }
    const groove_AsOfArrangement *left_state = jz_hmap_get(e->arrangement_states, &left_key);
    if (!left_state) { jz_rc_release(projection); return groove_err_node_not_found(left_input); }
    const groove_AsOfArrangement *right_state = jz_hmap_get(e->arrangement_states, &right_key);
    if (!right_state) { jz_rc_release(projection); return groove_err_node_not_found(right_input); }
    groove_ArrangementTransition l = { left_state, TE(arrangement_sub_tick)(e, &left_key) };
    groove_ArrangementTransition r = { right_state, TE(arrangement_sub_tick)(e, &right_key) };
    groove_RecordDeltaVec deltas = {0};
    err = groove__ivm__runtime__join__JoinState__evaluate_prepared(l, r, projection, e->context.arrangement_update_mode, &deltas);
    jz_rc_release(projection);
    JZ_TRY(err);
    out->descriptor = output_desc; out->deltas = deltas;
    return JZ_OK;
}

jz_err TE(update_threshold_join)(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_RecordDescriptor output_desc, groove_NodeId left_input, groove_NodeId right_input, groove_RecordDeltas *ld, groove_RecordDeltas *rd, bool semi, groove_RecordDeltas *out) {
    (void)ld; (void)rd;
    groove_OperatorStateKey operator_key = TE(operator_key)(e, node);
    groove_StrList *left_on, *right_on;
    TE(join_field_names)(e, node, join, &left_on, &right_on);
    groove_ArrangementKey left_key, right_key;
    jz_err err = TE(arrangement_key)(e, left_input, join->left_descriptor, left_on->items, left_on->len, join->comparison, &left_key);
    if (!err.code) err = TE(arrangement_key)(e, right_input, join->right_descriptor, right_on->items, right_on->len, join->comparison, &right_key);
    jz_rc_release(left_on); jz_rc_release(right_on);
    JZ_TRY(err);
    groove_SubTick lt = TE(arrangement_sub_tick)(e, &left_key);
    groove_SubTick rt = TE(arrangement_sub_tick)(e, &right_key);
    const groove_AsOfArrangement *left = jz_hmap_get(e->arrangement_states, &left_key);
    if (!left) return groove_err_node_not_found(left_input);
    const groove_AsOfArrangement *right = jz_hmap_get(e->arrangement_states, &right_key);
    if (!right) return groove_err_node_not_found(right_input);
    groove_ArrangementTransition l = { left, lt }, r = { right, rt };
    bool inserted = false;
    groove_OperatorState *op = jz_hmap_entry(e->operator_states, &operator_key, &inserted);
    if (inserted) {
        if (semi) { op->tag = GROOVE_OS_SEMI_JOIN; op->v.semi_join = groove__ivm__runtime__join__SemiJoinState__default(); }
        else { op->tag = GROOVE_OS_ANTI_JOIN; op->v.anti_join = groove__ivm__runtime__join__AntiJoinState__default(); }
    }
    groove_RecordDeltaVec deltas = {0};
    if (op->tag == GROOVE_OS_SEMI_JOIN && semi) groove__ivm__runtime__join__SemiJoinState__evaluate(op->v.semi_join, l, r, e->context.arrangement_update_mode, &deltas);
    else if (op->tag == GROOVE_OS_ANTI_JOIN && !semi) groove__ivm__runtime__join__AntiJoinState__evaluate(op->v.anti_join, l, r, e->context.arrangement_update_mode, &deltas);
    else return groove_err_state_mismatch(node);
    out->descriptor = output_desc; out->deltas = deltas;
    return JZ_OK;
}

jz_err TE(update_anti_join)(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_RecordDescriptor output_desc, groove_NodeId l, groove_NodeId r, groove_RecordDeltas *ld, groove_RecordDeltas *rd, groove_RecordDeltas *out) {
    return TE(update_threshold_join)(e, node, join, output_desc, l, r, ld, rd, false, out);
}
jz_err TE(update_semi_join)(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_RecordDescriptor output_desc, groove_NodeId l, groove_NodeId r, groove_RecordDeltas *ld, groove_RecordDeltas *rd, groove_RecordDeltas *out) {
    return TE(update_threshold_join)(e, node, join, output_desc, l, r, ld, rd, true, out);
}

/* ---------------------------------------------------------------------
 * ArgBy / TopBy / CollectBy
 * ------------------------------------------------------------------- */
static void delta_vec_drop(void *p) { jz_vec_drop(p, sizeof(groove_RecordDelta), groove__ivm__runtime__subscriptions__RecordDelta__drop); }
static const jz_btree_desc desc_bytes_deltas = { sizeof(jz_bytes), sizeof(groove_RecordDeltaVec), jz_cmp_bytes, groove_ivm_rt_a_drop_bytes, delta_vec_drop };
static void order_key_vec_drop(void *p) { jz_vec_drop(p, sizeof(groove_CollectByOrderKey), EV(CollectByOrderKey__drop)); }
typedef JZ_VEC(groove_CollectByOrderKey) groove_OrderKeyVec;
static const jz_btree_desc desc_bytes_order_keys = { sizeof(jz_bytes), sizeof(groove_OrderKeyVec), jz_cmp_bytes, groove_ivm_rt_a_drop_bytes, order_key_vec_drop };
static const jz_btree_desc desc_bytes_group_plain = { sizeof(jz_bytes), sizeof(groove_CollectByGroup), jz_cmp_bytes, groove_ivm_rt_a_drop_bytes, EV(CollectByGroup__drop) };
static void bytes_val_drop(void *p) { jz_bytes_drop(p); }
static const jz_btree_desc desc_bytes_bytes = { sizeof(jz_bytes), sizeof(jz_bytes), jz_cmp_bytes, groove_ivm_rt_a_drop_bytes, bytes_val_drop };
static void record_weight_drop(void *p) { jz_bytes_drop(&((groove_RecordWeight *)p)->record); }

typedef jz_err (*GroupKeyFn)(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out);
static jz_err group_deltas_by(GroupKeyFn key_fn, groove_RecordDescriptor d, const groove_RecordDeltas *input, const usize *idx, u32 n, jz_btree *out) {
    jz_btree_init(out, &desc_bytes_deltas);
    for (u32 i = 0; i < input->deltas.len; i++) {
        const groove_RecordDelta *delta = &input->deltas.ptr[i];
        jz_bytes key = {0};
        jz_err err = key_fn(d, delta->record.ptr, delta->record.len, idx, n, &key);
        if (err.code) { jz_btree_drop(out); return err; }
        bool inserted = false;
        groove_RecordDeltaVec *bucket = jz_btree_entry(out, &key, &inserted);
        if (!inserted) jz_bytes_drop(&key);
        groove_RecordDelta c; groove__ivm__runtime__subscriptions__RecordDelta__clone(&c, delta);
        jz_vec_push(bucket, &c, sizeof(c));
    }
    return JZ_OK;
}

static groove_SubTick scoped_sub_tick(const groove_TickEvaluator *e, groove_ScopeId scope) {
    groove_SubTick t = { e->current_tick, scope == groove__ivm__runtime__state__ScopeId__root() ? 0 : e->context.sub_tick };
    return t;
}

static jz_err out_of_order(const groove_AsOfStamp *st, groove_SubTick next) {
    return groove_ivm_rt_a_err_out_of_order(st->at, next, false);
}

jz_err TE(update_arg_by)(groove_TickEvaluator *e, groove_NodeId node, groove_ArgBySpec spec, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out) {
    if (e->context.eval_mode == GROOVE_EVAL_HYDRATE && !e->context.hydrate_arrangements) {
        /* A probe needs only the winners, not an index for future deltas. */
        return groove__ivm__runtime__recursion__hydrated_arg_by_winners(input, output_desc, spec.group_field_indices, spec.n_group, spec.comparison_field_indices, spec.n_comparison, spec.direction, out);
    }
    groove_OperatorStateKey operator_key = TE(operator_key)(e, node);
    groove_OperatorState operator;
    groove_OperatorStateKey removed_key;
    if (!jz_hmap_remove(e->operator_states, &operator_key, &removed_key, &operator)) {
        memset(&operator, 0, sizeof(operator));
        operator.tag = GROOVE_OS_ARG_BY;
        EV(SparseGroups__default)(&operator.v.arg_by.value);
    }
    if (operator.tag != GROOVE_OS_ARG_BY) { EV(OperatorState__drop)(&operator); return groove_err_state_mismatch(node); }
    groove_AsOfSparseGroups *state = &operator.v.arg_by;
    groove_SubTick sub_tick = scoped_sub_tick(e, operator_key.scope);
    if (state->as_of.has && groove_SubTick_cmp(state->as_of.at, sub_tick) > 0) {
        jz_err err = out_of_order(&state->as_of, sub_tick);
        EV(OperatorState__drop)(&operator);
        return err;
    }
    bool replace = e->context.arrangement_update_mode == GROOVE_AUM_REPLACE;
    if (!replace && as_of_eq(&state->as_of, sub_tick)) {
        jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
        *out = groove_RecordDeltas_empty(output_desc);
        return JZ_OK;
    }
    /* Hydration supplies the complete input, not another set of inserts. */
    if (replace) EV(SparseGroups__clear)(&state->value);
    jz_btree touched_groups;
    jz_err err = group_deltas_by(groove__ivm__runtime__windows__encoded_arrangement_key_part, output_desc, input, spec.group_field_indices, spec.n_group, &touched_groups);
    if (err.code) { EV(OperatorState__drop)(&operator); return err; }
    groove_RecordDeltaVec output = {0};
    jz_bytes group_prefix; groove_RecordDeltaVec group_deltas;
    while (!err.code && jz_btree_pop_first(&touched_groups, &group_prefix, &group_deltas)) {
        groove_CollectByGroup *group = EV(SparseGroups__get_or_default)(&state->value, jz_bytes_clone(&group_prefix));
        jz_bytes before = {0}, after = {0};
        bool has_before = groove__ivm__runtime__windows__arg_by_ordered_winner(group, &before);
        for (u32 i = 0; i < group_deltas.len && !err.code; i++) {
            groove_CollectByOrderKey key;
            err = groove__ivm__runtime__windows__arg_by_order_key(output_desc, &group_deltas.ptr[i], &spec, &key);
            if (err.code) break;
            const i64 *w = EV(CollectByGroup__get)(group, &key);
            EV(CollectByGroup__set)(group, key, (w ? *w : 0) + group_deltas.ptr[i].weight);
        }
        delta_vec_drop(&group_deltas);
        if (err.code) { jz_bytes_drop(&group_prefix); jz_bytes_drop(&before); break; }
        bool has_after = groove__ivm__runtime__windows__arg_by_ordered_winner(group, &after);
        EV(SparseGroups__remove_empty_touched_groups)(&state->value, &group_prefix, 1);
        if (has_before == has_after && (!has_before || jz_bytes_eq(&before, &after))) {
            jz_bytes_drop(&before); jz_bytes_drop(&after);
            continue;
        }
        if (has_before) { groove_RecordDelta d = { before, -1 }; jz_vec_push(&output, &d, sizeof(d)); }
        if (has_after) { groove_RecordDelta d = { after, 1 }; jz_vec_push(&output, &d, sizeof(d)); }
    }
    jz_btree_drop(&touched_groups);
    if (!err.code) err = groove__ivm__runtime__state__AsOf__mark_forward_as_of(&state->as_of, sub_tick, false);
    if (err.code) { EV(OperatorState__drop)(&operator); delta_vec_drop(&output); return err; }
    jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
    out->descriptor = output_desc; out->deltas = output;
    return JZ_OK;
}

typedef struct MoveGroupCtx { groove_CollectByGroup group; } MoveGroupCtx;
static void move_group_before(void *ctx, groove_CollectByGroup *out) { *out = ((MoveGroupCtx *)ctx)->group; memset(&((MoveGroupCtx *)ctx)->group, 0, sizeof(groove_CollectByGroup)); }
static void clone_group_before(void *ctx, groove_CollectByGroup *out) { EV(CollectByGroup__clone)(out, ctx); }

jz_err TE(update_top_by)(groove_TickEvaluator *e, groove_NodeId node, const groove_TopByOp *top_by, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out) {
    if (input->deltas.len == 0 || (top_by->limit.tag == GROOVE_LIMIT_FINITE && top_by->limit.n == 0)) {
        *out = groove_RecordDeltas_empty(output_desc);
        return JZ_OK;
    }
    groove_OperatorStateKey operator_key = TE(operator_key)(e, node);
    groove_OperatorState operator;
    groove_OperatorStateKey removed_key;
    if (!jz_hmap_remove(e->operator_states, &operator_key, &removed_key, &operator)) {
        groove_OpType op; memset(&op, 0, sizeof(op)); op.tag = GROOVE_OP_TOP_BY;
        EV(operator_state_for)(&op, &operator);
    }
    if (operator.tag != GROOVE_OS_TOP_BY) { EV(OperatorState__drop)(&operator); return groove_err_state_mismatch(node); }
    groove_AsOfTopBy *state = &operator.v.top_by;
    groove_SubTick sub_tick = scoped_sub_tick(e, operator_key.scope);
    if (state->as_of.has && groove_SubTick_cmp(state->as_of.at, sub_tick) > 0) {
        jz_err err = out_of_order(&state->as_of, sub_tick);
        EV(OperatorState__drop)(&operator);
        return err;
    }
    if (e->context.arrangement_update_mode == GROOVE_AUM_ACCUMULATE && as_of_eq(&state->as_of, sub_tick)) {
        jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
        *out = groove_RecordDeltas_empty(output_desc);
        return JZ_OK;
    }
    jz_btree touched_groups;
    jz_err err = group_deltas_by(groove__ivm__runtime__windows__encoded_record_key_part, output_desc, input, top_by->group_field_indices.ptr, top_by->group_field_indices.len, &touched_groups);
    if (err.code) { EV(OperatorState__drop)(&operator); return err; }
    groove_RecordDeltaVec output = {0};
    bool replace = e->context.arrangement_update_mode == GROOVE_AUM_REPLACE;
    bool positions = jz_hmap_get(&e->root_ordering_windows, &node) != NULL;
    JZ_VEC(jz_bytes) touched_keys_list = {0};
    jz_bytes *gk; groove_RecordDeltaVec *gd;
    if (!replace && top_by->offset == 0 && top_by->limit.tag == GROOVE_LIMIT_UNBOUNDED) {
        /* Without a selection boundary, only touched records can change
         * membership; a positions consumer ranks only changed rows (#3505). */
        JZ_BTREE_FOR(&touched_groups, gk, gd) {
            MoveGroupCtx before;
            if (positions) {
                const groove_CollectByGroup *g = EV(SparseGroups__get)(&state->value.groups, gk->ptr, gk->len);
                if (g) EV(CollectByGroup__clone)(&before.group, g); else EV(CollectByGroup__default)(&before.group);
            }
            groove_CollectByGroup *group = EV(SparseGroups__get_or_default)(&state->value.groups, jz_bytes_clone(gk));
            groove_OrderKeyVec touched = {0};
            groove_RecordDeltaVec part = {0};
            err = groove__ivm__runtime__windows__update_unbounded_top_by_group_touching(output_desc, top_by, group, gd->ptr, gd->len, positions ? (void *)&touched : NULL, &part);
            if (err.code) { if (positions) EV(CollectByGroup__drop)(&before.group); break; }
            jz_vec_extend(&output, part.ptr, part.len, sizeof(groove_RecordDelta));
            jz_vec_drop(&part, sizeof(groove_RecordDelta), NULL);
            e->metrics->top_by_delta_membership_records += gd->len;
            if (positions) {
                groove_RootOrderingWindows *windows = jz_hmap_get(&e->root_ordering_windows, &node);
                if (!windows) jz_panic("positions were requested");
                EV(RootOrderingWindows__record)(windows, output_desc, top_by, gk->ptr, gk->len, move_group_before, &before, group, touched.ptr, touched.len, true);
                EV(CollectByGroup__drop)(&before.group);
                jz_vec_drop(&touched, sizeof(groove_CollectByOrderKey), NULL);
            }
        }
        if (!err.code) {
            JZ_BTREE_FOR(&touched_groups, gk, gd) JZ_VEC_PUSH(touched_keys_list, jz_bytes_clone(gk));
            EV(TopByIncrementalState__remove_empty_touched_groups)(&state->value, touched_keys_list.ptr, touched_keys_list.len);
            jz_vec_drop(&touched_keys_list, sizeof(jz_bytes), NULL);
            err = groove__ivm__runtime__state__AsOf__mark_forward_as_of(&state->as_of, sub_tick, false);
        }
        jz_btree_drop(&touched_groups);
        if (err.code) { EV(OperatorState__drop)(&operator); delta_vec_drop(&output); return err; }
        jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
        out->descriptor = output_desc; out->deltas = output;
        return JZ_OK;
    }
    jz_btree before_groups; jz_btree_init(&before_groups, &desc_bytes_group_plain);
    JZ_BTREE_FOR(&touched_groups, gk, gd) {
        groove_CollectByGroup g;
        const groove_CollectByGroup *cur = replace ? NULL : EV(SparseGroups__get)(&state->value.groups, gk->ptr, gk->len);
        if (cur) EV(CollectByGroup__clone)(&g, cur); else EV(CollectByGroup__default)(&g);
        jz_bytes k = jz_bytes_clone(gk);
        jz_btree_insert(&before_groups, &k, &g, NULL);
    }
    if (replace) EV(SparseGroups__clear)(&state->value.groups);
    jz_btree touched_keys; jz_btree_init(&touched_keys, &desc_bytes_order_keys);
    JZ_BTREE_FOR(&touched_groups, gk, gd) {
        groove_CollectByGroup *group = EV(SparseGroups__get_or_default)(&state->value.groups, jz_bytes_clone(gk));
        for (u32 i = 0; i < gd->len; i++) {
            groove_CollectByOrderKey order_key = {{0}, {0}};
            err = groove__ivm__runtime__windows__top_by_sort_key(output_desc, gd->ptr[i].record.ptr, gd->ptr[i].record.len, top_by, &order_key.parts);
            if (err.code) break;
            order_key.record = jz_bytes_clone(&gd->ptr[i].record);
            const i64 *w = EV(CollectByGroup__get)(group, &order_key);
            i64 weight = (w ? *w : 0) + gd->ptr[i].weight;
            if (positions) {
                jz_bytes k = jz_bytes_clone(gk);
                bool inserted = false;
                groove_OrderKeyVec *list = jz_btree_entry(&touched_keys, &k, &inserted);
                if (!inserted) jz_bytes_drop(&k);
                groove_CollectByOrderKey c; EV(CollectByOrderKey__clone)(&c, &order_key);
                jz_vec_push(list, &c, sizeof(c));
            }
            EV(CollectByGroup__set)(group, order_key, weight);
        }
        if (err.code) break;
    }
    if (!err.code) {
        JZ_BTREE_FOR(&touched_groups, gk, gd) JZ_VEC_PUSH(touched_keys_list, jz_bytes_clone(gk));
        EV(TopByIncrementalState__remove_empty_touched_groups)(&state->value, touched_keys_list.ptr, touched_keys_list.len);
        jz_vec_drop(&touched_keys_list, sizeof(jz_bytes), NULL);
        err = groove__ivm__runtime__state__AsOf__mark_forward_as_of(&state->as_of, sub_tick, false);
    }
    if (!err.code) {
        JZ_BTREE_FOR(&touched_groups, gk, gd) {
            groove_CollectByGroup *before_group = jz_btree_get(&before_groups, gk);
            if (!before_group) jz_panic("before group recorded");
            groove_CollectByGroup after_group;
            const groove_CollectByGroup *cur = EV(SparseGroups__get)(&state->value.groups, gk->ptr, gk->len);
            if (cur) EV(CollectByGroup__clone)(&after_group, cur); else EV(CollectByGroup__default)(&after_group);
            groove_WindowedRecordVec before = {0}, after = {0};
            groove__ivm__runtime__windows__top_by_window_from_ordered_group(before_group, top_by, &before);
            groove__ivm__runtime__windows__top_by_window_from_ordered_group(&after_group, top_by, &after);
            groove_RootOrderingWindows *windows = jz_hmap_get(&e->root_ordering_windows, &node);
            if (windows) {
                groove_OrderKeyVec keys = {0};
                jz_bytes rk;
                jz_btree_remove(&touched_keys, gk, &rk, &keys) ? jz_bytes_drop(&rk) : (void)0;
                EV(RootOrderingWindows__record)(windows, output_desc, top_by, gk->ptr, gk->len, clone_group_before, before_group, &after_group, keys.ptr, keys.len, !replace);
                jz_vec_drop(&keys, sizeof(groove_CollectByOrderKey), NULL);
            } else {
                usize sum = (usize)before.len + after.len;
                e->metrics->root_ordering_position_records_skipped += sum;
            }
            groove__ivm__runtime__windows__diff_record_windows(before, after, &output);
            EV(CollectByGroup__drop)(&after_group);
        }
    }
    jz_btree_drop(&touched_keys);
    jz_btree_drop(&before_groups);
    jz_btree_drop(&touched_groups);
    if (err.code) { EV(OperatorState__drop)(&operator); delta_vec_drop(&output); return err; }
    jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
    out->descriptor = output_desc; out->deltas = output;
    return JZ_OK;
}

static jz_err TE(materialize_arranged_records)(groove_TickEvaluator *e, groove_RecordDescriptor d, groove_RecordWeightVec records, const groove_UsizeVec *fields, groove_RecordWeightVec *out);

typedef jz_err (*CollectRenderFn)(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const groove_RecordWeight *r, u32 n, bool *some, jz_bytes *out);

static void op_vec_drop_terminal(void *v) {
    groove_TerminalDeltas t; memcpy(&t.operations, v, sizeof(t.operations));
    groove__ivm__runtime__terminal__TerminalDeltas__drop(&t);
}

/* Render touched flat groups as complete parents. This intentionally uses a
 * root-scope arrangement keyed by the collector input. */
jz_err TE(update_collect_by)(groove_TickEvaluator *e, groove_NodeId node, const groove_CollectByOp *cb, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, const groove_RecordDeltas *canonical, groove_RecordDeltas *out) {
    if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) {
        /* Hydration supplies a complete snapshot; rebuild the shared state. */
        groove_OperatorStateKey operator_key = TE(operator_key)(e, node);
        jz_hmap_remove(e->operator_states, &operator_key, NULL, NULL);
    }
    if (input->deltas.len == 0 || (cb->limit.tag == GROOVE_LIMIT_FINITE && cb->limit.n == 0)) {
        *out = groove_RecordDeltas_empty(output_desc);
        return JZ_OK;
    }
    bool has_hydration_snapshot = false;
    groove_RecordDeltaVec hydration_snapshot = {0};
    const groove_CollectBySlot *direct_tree_slot = NULL;
    if (cb->slots.len == 1) {
        const groove_CollectBySlot *slot = &cb->slots.ptr[0];
        if (slot->slots.len == 0 && slot->limit.tag == GROOVE_LIMIT_UNBOUNDED && !slot->has_reference_array_field_index) direct_tree_slot = slot;
    }
    bool unbounded = cb->limit.tag == GROOVE_LIMIT_UNBOUNDED;
    if (cb->mode == GROOVE_CBM_ROOT
        || (cb->mode == GROOVE_CBM_COLLECT && (cb->slots.len == 0 || direct_tree_slot) && (unbounded || direct_tree_slot))) {
        groove_OperatorStateKey operator_key = TE(operator_key)(e, node);
        groove_OperatorState operator;
        groove_OperatorStateKey rk;
        if (!jz_hmap_remove(e->operator_states, &operator_key, &rk, &operator)) {
            memset(&operator, 0, sizeof(operator));
            operator.tag = GROOVE_OS_COLLECT_BY;
            EV(CollectByIncrementalState__default)(&operator.v.collect_by);
        }
        if (operator.tag != GROOVE_OS_COLLECT_BY) { EV(OperatorState__drop)(&operator); return groove_err_state_mismatch(node); }
        groove_TerminalOperationVec operations = {0};
        jz_err err = groove__ivm__runtime__windows__update_unbounded_collect_by_terminal_state(
            input->descriptor, output_desc, cb, direct_tree_slot, &operator.v.collect_by, input->deltas.ptr, input->deltas.len,
            e->context.eval_mode == GROOVE_EVAL_TICK || e->context.eval_mode == GROOVE_EVAL_HYDRATE, &operations);
        if (err.code) { EV(OperatorState__drop)(&operator); return err; }
        if (e->context.eval_mode == GROOVE_EVAL_HYDRATE && cb->mode == GROOVE_CBM_ROOT) {
            /* Hydrate removed the prior operator above, so these Inserts cover
             * the complete fresh state. */
            has_hydration_snapshot = EV(singleton_root_hydration_snapshot)(&operator.v.collect_by, input->deltas.ptr, input->deltas.len, operations.ptr, operations.len, &hydration_snapshot);
        }
        jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
        /* A subscription hydration is the first transition of the same
         * collector; retain its operations for the opening consumer. */
        if (operations.len) {
            groove_TerminalDeltas t; memcpy(&t.operations, &operations, sizeof(operations));
            jz_hmap_insert(&e->terminal_deltas, &node, &t, NULL);
        } else {
            op_vec_drop_terminal(&operations);
        }
        if (e->context.eval_mode == GROOVE_EVAL_TICK) {
            jz_vec_drop(&hydration_snapshot, sizeof(groove_RecordDelta), groove__ivm__runtime__subscriptions__RecordDelta__drop);
            *out = groove_RecordDeltas_empty(output_desc);
            return JZ_OK;
        }
    }
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) { delta_vec_drop(&hydration_snapshot); return groove_err_node_not_found(node); }
    if (gn->descriptor.inputs.len != 1) { delta_vec_drop(&hydration_snapshot); return groove_err_arity(node); }
    groove_NodeId input_node = gn->descriptor.inputs.ptr[0];
    if (has_hydration_snapshot) {
        out->descriptor = output_desc; out->deltas = hydration_snapshot;
        return JZ_OK;
    }
    groove_RecordDescriptor input_desc = input->descriptor;
    groove_ArrangementKey arrangement_key;
    JZ_TRY(TE(arrangement_key)(e, input_node, input_desc, cb->group_fields.ptr, cb->group_fields.len, GROOVE_VC_EXACT, &arrangement_key));
    /* Root collectors own an ordered terminal index, not a second retained
     * hash index. Only ambiguous hydration needs a temporary snapshot view. */
    groove_AsOfArrangement arrangement;
    if (cb->mode == GROOVE_CBM_ROOT) {
        arrangement.value = groove__ivm__runtime__join__ArrangementState__default();
        arrangement.as_of.has = false;
        jz_err err = groove__ivm__runtime__join__ArrangementState__apply_record_deltas(arrangement.value, input_desc, cb->group_fields.ptr, cb->group_fields.len, input->deltas.ptr, input->deltas.len, GROOVE_AUM_REPLACE);
        if (err.code) { groove__ivm__runtime__join__ArrangementState__drop(arrangement.value); return err; }
    } else {
        const groove_AsOfArrangement *st = jz_hmap_get(e->arrangement_states, &arrangement_key);
        if (!st) return groove_err_node_not_found(input_node);
        arrangement.value = groove__ivm__runtime__join__ArrangementState__clone(st->value);
        arrangement.as_of = st->as_of;
    }
    jz_btree touched_groups;
    jz_err err = group_deltas_by(groove__ivm__runtime__windows__encoded_record_key_part, input_desc, canonical, cb->group_field_indices.ptr, cb->group_field_indices.len, &touched_groups);
    if (err.code) { groove__ivm__runtime__join__ArrangementState__drop(arrangement.value); return err; }
    groove_UsizeVec read_fields = {0};
    groove__ivm__op_types__CollectByOp__read_field_indices(cb, &read_fields);
    groove_RecordDeltaVec output = {0};
    jz_bytes group_prefix; groove_RecordDeltaVec group_deltas;
    while (!err.code && jz_btree_pop_first(&touched_groups, &group_prefix, &group_deltas)) {
        groove_RecordWeightVec after_raw = {0}, before_raw = {0}, after_records = {0}, before_records = {0};
        groove__ivm__runtime__join__ArrangementState__records_for_key(arrangement.value, group_prefix.ptr, group_prefix.len, &after_raw);
        if (e->context.arrangement_update_mode != GROOVE_AUM_REPLACE) {
            groove_RecordWeightVec copy = {0};
            for (u32 i = 0; i < after_raw.len; i++) { groove_RecordWeight w = { jz_bytes_clone(&after_raw.ptr[i].record), after_raw.ptr[i].weight }; jz_vec_push(&copy, &w, sizeof(w)); }
            groove__ivm__runtime__windows__records_before_deltas(copy, group_deltas.ptr, group_deltas.len, &before_raw);
        }
        /* Only the fields the collector reads are rebuilt (#3830). */
        err = TE(materialize_arranged_records)(e, input_desc, after_raw, &read_fields, &after_records);
        if (!err.code) err = TE(materialize_arranged_records)(e, input_desc, before_raw, &read_fields, &before_records);
        else jz_vec_drop(&before_raw, sizeof(groove_RecordWeight), record_weight_drop);
        if (!err.code) {
            if (cb->mode == GROOVE_CBM_COLLECT || cb->mode == GROOVE_CBM_ROOT) {
                CollectRenderFn render = cb->mode == GROOVE_CBM_ROOT ? groove__ivm__runtime__windows__collect_by_root_from_records
                    : cb->slots.len == 0 ? groove__ivm__runtime__windows__collect_by_parent_from_records
                    : groove__ivm__runtime__windows__collect_by_tree_parent_from_records;
                bool hb = false, ha = false; jz_bytes before = {0}, after = {0};
                err = render(input_desc, output_desc, cb, before_records.ptr, before_records.len, &hb, &before);
                if (!err.code) err = render(input_desc, output_desc, cb, after_records.ptr, after_records.len, &ha, &after);
                if (!err.code) {
                    if (hb == ha && (!hb || jz_bytes_eq(&before, &after))) { jz_bytes_drop(&before); jz_bytes_drop(&after); }
                    else {
                        if (hb) { groove_RecordDelta d = { before, -1 }; jz_vec_push(&output, &d, sizeof(d)); }
                        if (ha) { groove_RecordDelta d = { after, 1 }; jz_vec_push(&output, &d, sizeof(d)); }
                    }
                } else { jz_bytes_drop(&before); }
            } else { /* Expand */
                jz_btree before, after;
                jz_btree_init(&before, &desc_bytes_bytes); jz_btree_init(&after, &desc_bytes_bytes);
                err = groove__ivm__runtime__windows__collect_by_expanded_window(input_desc, output_desc, cb, before_records.ptr, before_records.len, &before);
                if (!err.code) err = groove__ivm__runtime__windows__collect_by_expanded_window(input_desc, output_desc, cb, after_records.ptr, after_records.len, &after);
                if (!err.code) {
                    jz_btree occurrences; jz_btree_init(&occurrences, &groove_ivm_rt_a_desc_bytes_set);
                    jz_bytes *ok; jz_bytes *ov;
                    JZ_BTREE_FOR(&before, ok, ov) { if (!jz_btree_contains(&occurrences, ok)) { jz_bytes c = jz_bytes_clone(ok); jz_btree_insert(&occurrences, &c, NULL, NULL); } }
                    JZ_BTREE_FOR(&after, ok, ov) { if (!jz_btree_contains(&occurrences, ok)) { jz_bytes c = jz_bytes_clone(ok); jz_btree_insert(&occurrences, &c, NULL, NULL); } }
                    void *unused;
                    JZ_BTREE_FOR(&occurrences, ok, unused) {
                        const jz_bytes *b = jz_btree_get(&before, ok), *a = jz_btree_get(&after, ok);
                        if (b && a && jz_bytes_eq(b, a)) continue;
                        if (b) { groove_RecordDelta d = { jz_bytes_clone(b), -1 }; jz_vec_push(&output, &d, sizeof(d)); }
                        if (a) { groove_RecordDelta d = { jz_bytes_clone(a), 1 }; jz_vec_push(&output, &d, sizeof(d)); }
                        if (!a && !b) jz_panic("internal error: entered unreachable code: occurrence came from a selected window");
                    }
                    jz_btree_drop(&occurrences);
                }
                jz_btree_drop(&before); jz_btree_drop(&after);
            }
        }
        jz_vec_drop(&after_records, sizeof(groove_RecordWeight), record_weight_drop);
        jz_vec_drop(&before_records, sizeof(groove_RecordWeight), record_weight_drop);
        jz_bytes_drop(&group_prefix);
        delta_vec_drop(&group_deltas);
    }
    jz_btree_drop(&touched_groups);
    jz_vec_drop(&read_fields, sizeof(usize), NULL);
    groove__ivm__runtime__join__ArrangementState__drop(arrangement.value);
    if (err.code) { delta_vec_drop(&output); return err; }
    out->descriptor = output_desc; out->deltas = output;
    return JZ_OK;
}

/* ---------------------------------------------------------------------
 * Aggregate
 * ------------------------------------------------------------------- */
static void record_weight_vec_drop(void *p) { jz_vec_drop(p, sizeof(groove_RecordWeight), record_weight_drop); }
static const jz_btree_desc desc_bytes_record_weights = { sizeof(jz_bytes), sizeof(groove_RecordWeightVec), jz_cmp_bytes, groove_ivm_rt_a_drop_bytes, record_weight_vec_drop };
static int cmp_usize(const void *a, const void *b, void *ctx) { (void)ctx; usize x = *(const usize *)a, y = *(const usize *)b; return x < y ? -1 : x > y; }
static int cmp_usize2(const void *a, const void *b) { return cmp_usize(a, b, NULL); }

jz_err TE(update_aggregate)(groove_TickEvaluator *e, groove_NodeId node, const groove_AggregateOp *agg, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, const groove_RecordDeltas *canonical, groove_RecordDeltas *out) {
    const groove_GraphNode *gn = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!gn) return groove_err_node_not_found(node);
    if (gn->descriptor.inputs.len != 1) return groove_err_arity(node);
    groove_NodeId input_node = gn->descriptor.inputs.ptr[0];
    groove_RecordDescriptor input_desc = input->descriptor;
    groove_StrList *group_fields = TE(aggregate_group_fields)(e, node, agg);
    groove_ArrangementKey arrangement_key;
    jz_err err = TE(arrangement_key)(e, input_node, input_desc, group_fields->items, group_fields->len, GROOVE_VC_EXACT, &arrangement_key);
    jz_rc_release(group_fields);
    JZ_TRY(err);
    if (input->deltas.len == 0) {
        /* An ungrouped aggregate has one logical empty group, seeded on the
         * first complete empty frontier; grouped aggregates have none. */
        bool seed = agg->group_key.len == 0
            && (e->context.eval_mode == GROOVE_EVAL_HYDRATE || !jz_hmap_get(e->arrangement_states, &arrangement_key));
        if (seed) {
            bool some = false; jz_bytes record = {0};
            JZ_TRY(groove__ivm__runtime__aggregate__aggregate_row_from_records(input_desc, output_desc, agg, NULL, 0, &some, &record));
            if (!some) return groove_err_unsupported_operator();
            out->descriptor = output_desc; out->deltas.ptr = NULL; out->deltas.len = out->deltas.cap = 0;
            groove_RecordDelta d = { record, 1 };
            jz_vec_push(&out->deltas, &d, sizeof(d));
            return JZ_OK;
        }
        *out = groove_RecordDeltas_empty(output_desc);
        return JZ_OK;
    }
    if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) {
        jz_btree groups; jz_btree_init(&groups, &desc_bytes_record_weights);
        for (u32 i = 0; i < input->deltas.len; i++) {
            const groove_RecordDelta *delta = &input->deltas.ptr[i];
            jz_bytes key = {0};
            err = groove__ivm__runtime__windows__encoded_record_key_part(input_desc, delta->record.ptr, delta->record.len, agg->group_field_indices.ptr, agg->group_field_indices.len, &key);
            if (err.code) { jz_btree_drop(&groups); return err; }
            bool inserted = false;
            groove_RecordWeightVec *bucket = jz_btree_entry(&groups, &key, &inserted);
            if (!inserted) jz_bytes_drop(&key);
            groove_RecordWeight w = { jz_bytes_clone(&delta->record), delta->weight };
            jz_vec_push(bucket, &w, sizeof(w));
        }
        groove_RecordDeltaVec output = {0};
        jz_bytes *k; groove_RecordWeightVec *records;
        JZ_BTREE_FOR(&groups, k, records) {
            bool some = false; jz_bytes record = {0};
            err = groove__ivm__runtime__aggregate__aggregate_row_from_records(input_desc, output_desc, agg, records->ptr, records->len, &some, &record);
            if (err.code) break;
            if (some) { groove_RecordDelta d = { record, 1 }; jz_vec_push(&output, &d, sizeof(d)); }
        }
        jz_btree_drop(&groups);
        if (err.code) { delta_vec_drop(&output); return err; }
        out->descriptor = output_desc; out->deltas = output;
        return JZ_OK;
    }
    jz_btree touched_groups;
    JZ_TRY(group_deltas_by(groove__ivm__runtime__windows__encoded_record_key_part, input_desc, canonical, agg->group_field_indices.ptr, agg->group_field_indices.len, &touched_groups));
    const groove_AsOfArrangement *st = jz_hmap_get(e->arrangement_states, &arrangement_key);
    if (!st) { jz_btree_drop(&touched_groups); return groove_err_node_not_found(input_node); }
    groove_ArrangementState *arrangement = groove__ivm__runtime__join__ArrangementState__clone(st->value);

    groove_UsizeVec fields = {0};
    jz_vec_extend(&fields, agg->group_field_indices.ptr, agg->group_field_indices.len, sizeof(usize));
    groove_PlanExprVec expressions = {0};
    for (u32 i = 0; i < agg->aggregates.len; i++)
        if (agg->aggregates.ptr[i].has_expression) jz_vec_push(&expressions, &agg->aggregates.ptr[i].expression, sizeof(groove_PlanExpr)); /* borrowed */
    jz_btree expr_fields;
    EV(plan_expr_fields)(expressions.ptr, expressions.len, &expr_fields);
    jz_vec_drop(&expressions, sizeof(groove_PlanExpr), NULL);
    jz_str *field; void *unused;
    JZ_BTREE_FOR(&expr_fields, field, unused) {
        usize idx;
        if (!groove__ivm__runtime__record_projection__resolve_field_name(input_desc, field->ptr, field->len, &idx)) {
            err = groove_err_field_not_found(field->ptr, field->len);
            break;
        }
        jz_vec_push(&fields, &idx, sizeof(idx));
    }
    jz_btree_drop(&expr_fields);
    if (!err.code) {
        jz_sort_unstable(fields.ptr, fields.len, sizeof(usize), cmp_usize, NULL);
        jz_vec_dedup(&fields, sizeof(usize), cmp_usize2, NULL);
    }
    groove_RecordDeltaVec output = {0};
    jz_bytes *gk; groove_RecordDeltaVec *gd;
    if (!err.code) JZ_BTREE_FOR(&touched_groups, gk, gd) {
        groove_RecordWeightVec after_raw = {0}, copy = {0}, before_raw = {0}, after_records = {0}, before_records = {0};
        groove__ivm__runtime__join__ArrangementState__records_for_key(arrangement, gk->ptr, gk->len, &after_raw);
        for (u32 i = 0; i < after_raw.len; i++) { groove_RecordWeight w = { jz_bytes_clone(&after_raw.ptr[i].record), after_raw.ptr[i].weight }; jz_vec_push(&copy, &w, sizeof(w)); }
        groove_RecordDeltaVec deltas_copy = {0};
        jz_vec_clone(&deltas_copy, gd, sizeof(groove_RecordDelta), groove__ivm__runtime__subscriptions__RecordDelta__clone);
        groove__ivm__runtime__aggregate__records_before_from_deltas(copy, &deltas_copy, &before_raw);
        err = TE(materialize_arranged_records)(e, input_desc, after_raw, &fields, &after_records);
        if (!err.code) err = TE(materialize_arranged_records)(e, input_desc, before_raw, &fields, &before_records);
        else jz_vec_drop(&before_raw, sizeof(groove_RecordWeight), record_weight_drop);
        bool ha = false, hb = false; jz_bytes after = {0}, before = {0};
        if (!err.code) err = groove__ivm__runtime__aggregate__aggregate_row_from_records(input_desc, output_desc, agg, after_records.ptr, after_records.len, &ha, &after);
        if (!err.code) err = groove__ivm__runtime__aggregate__aggregate_row_from_records(input_desc, output_desc, agg, before_records.ptr, before_records.len, &hb, &before);
        record_weight_vec_drop(&after_records);
        record_weight_vec_drop(&before_records);
        if (err.code) { jz_bytes_drop(&after); break; }
        if (hb == ha && (!hb || jz_bytes_eq(&before, &after))) { jz_bytes_drop(&before); jz_bytes_drop(&after); continue; }
        if (hb) { groove_RecordDelta d = { before, -1 }; jz_vec_push(&output, &d, sizeof(d)); }
        if (ha) { groove_RecordDelta d = { after, 1 }; jz_vec_push(&output, &d, sizeof(d)); }
    }
    jz_vec_drop(&fields, sizeof(usize), NULL);
    jz_btree_drop(&touched_groups);
    groove__ivm__runtime__join__ArrangementState__drop(arrangement);
    if (err.code) { delta_vec_drop(&output); return err; }
    groove__ivm__runtime__key_encoding__consolidate_deltas(&output);
    out->descriptor = output_desc; out->deltas = output;
    return JZ_OK;
}

/* ---------------------------------------------------------------------
 * Arrangements
 * ------------------------------------------------------------------- */
bool TE(arrangement_needs_index)(const groove_TickEvaluator *e, groove_NodeId node) {
    const groove_GraphNode *arrange = groove__ivm__graph__IvmGraph__node(e->graph, node);
    if (!arrange) return false;
    groove_NodeId *child; void *unused;
    JZ_HMAP_FOR(&arrange->children, child, unused) {
        const groove_GraphNode *c = groove__ivm__graph__IvmGraph__node(e->graph, *child);
        if (!c) continue;
        switch (c->descriptor.op.tag) {
        case GROOVE_OP_JOIN: case GROOVE_OP_SEMI_JOIN: case GROOVE_OP_ANTI_JOIN: case GROOVE_OP_AGGREGATE: return true;
        case GROOVE_OP_COLLECT_BY: if (c->descriptor.op.v.collect_by->mode != GROOVE_CBM_ROOT) return true; break;
        default: break;
        }
    }
    return false;
}

jz_err TE(arrangement_key)(groove_TickEvaluator *e, groove_NodeId input, groove_RecordDescriptor d, const jz_str *fields, u32 nfields, u32 comparison, groove_ArrangementKey *out) {
    const groove_GraphNode *arrangement = groove__ivm__graph__IvmGraph__node(e->graph, input);
    if (!arrangement) return groove_err_node_not_found(input);
    if (arrangement->descriptor.op.tag != GROOVE_OP_ARRANGE) return groove_err_unsupported_operator();
    const groove_ArrangeOp *spec = &arrangement->descriptor.op.v.arrange;
    bool same = groove_NodeOutput_records(arrangement->descriptor.output) == d && spec->fields.len == nfields && spec->comparison == comparison;
    for (u32 i = 0; same && i < nfields; i++) same = jz_str_eq(&spec->fields.ptr[i], &fields[i]);
    if (!same) return groove_err_output_mismatch();
    out->scope = e->context.scope;
    out->input = input;
    return JZ_OK;
}

static groove_StrList *strlist_of_names(const groove_PlanExprVec *exprs) {
    groove_StrVec names;
    EV(plan_expr_names)(exprs->ptr, exprs->len, &names);
    return groove_ivm_rt_a_strlist_from_vec(names);
}

static void TE(join_field_names)(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_StrList **left, groove_StrList **right) {
    groove_NodeRuntimeMeta *meta = groove_ivm_rt_a_node_meta_entry(e->node_meta, node);
    if (!meta->join_left_fields) meta->join_left_fields = strlist_of_names(&join->left_key);
    if (!meta->join_right_fields) meta->join_right_fields = strlist_of_names(&join->right_key);
    *left = jz_rc_clone(meta->join_left_fields);
    *right = jz_rc_clone(meta->join_right_fields);
}

jz_err TE(join_output_projection)(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDescriptor l, groove_RecordDescriptor r, groove_RecordDescriptor o, groove_PreparedRecordCopy **out) {
    groove_NodeRuntimeMeta *meta = groove_ivm_rt_a_node_meta_entry(e->node_meta, node);
    if (meta->join_output) { *out = jz_rc_clone(meta->join_output); return JZ_OK; }
    groove_UsizePairVec mapping = {0};
    JZ_TRY(groove__ivm__runtime__join__join_output_mapping(l, r, o, &mapping));
    groove_RecordDescriptor inputs[2] = { l, r };
    groove_PreparedRecordCopy *projection = NULL;
    jz_err err = groove__records__PreparedRecordCopy__new(inputs, 2, o, mapping.ptr, mapping.len, &projection);
    jz_vec_drop(&mapping, sizeof(groove_UsizePair), NULL);
    JZ_TRY(err);
    meta = groove_ivm_rt_a_node_meta_entry(e->node_meta, node);
    meta->join_output = jz_rc_clone(projection);
    *out = projection;
    return JZ_OK;
}

groove_StrList *TE(aggregate_group_fields)(groove_TickEvaluator *e, groove_NodeId node, const groove_AggregateOp *agg) {
    groove_NodeRuntimeMeta *meta = groove_ivm_rt_a_node_meta_entry(e->node_meta, node);
    if (!meta->aggregate_group_fields) meta->aggregate_group_fields = strlist_of_names(&agg->group_key);
    return jz_rc_clone(meta->aggregate_group_fields);
}

/* Root-scope arrangements represent table time, not recursive evaluator time. */
groove_SubTick TE(arrangement_sub_tick)(const groove_TickEvaluator *e, const groove_ArrangementKey *key) {
    return scoped_sub_tick(e, key->scope);
}

static u64 hash_arrangement_key(const void *k) {
    const groove_ArrangementKey *key = k;
    return jz_hash_combine(groove_hash_ScopeId(key->scope), key->input.v);
}
static bool eq_arrangement_key(const void *a, const void *b) {
    const groove_ArrangementKey *x = a, *y = b;
    return x->scope == y->scope && x->input.v == y->input.v;
}
static const jz_hmap_desc hdesc_arrangement_key_set = { sizeof(groove_ArrangementKey), 0, hash_arrangement_key, eq_arrangement_key, NULL, NULL };
static void hmap_val_drop(void *p) { jz_hmap_drop(p); }
static const jz_hmap_desc hdesc_nodeid_arrangement_keys = { sizeof(groove_NodeId), sizeof(jz_hmap), groove_ivm_rt_a_hash_nodeid, groove_ivm_rt_a_eq_nodeid, NULL, hmap_val_drop };

void TE(insert_arrangement)(groove_TickEvaluator *e, groove_ArrangementKey key, groove_AsOfArrangement state) {
    bool inserted = false;
    jz_hmap *keys = jz_hmap_entry(e->arrangement_keys_by_input, &key.input, &inserted);
    if (inserted) jz_hmap_init(keys, &hdesc_arrangement_key_set);
    jz_hmap_insert(keys, &key, NULL, NULL);
    jz_hmap_insert(e->arrangement_states, &key, &state, NULL);
}

/* ---------------------------------------------------------------------
 * Recursion
 * ------------------------------------------------------------------- */
static void view_from_evaluator(groove_TickEvaluator *e, groove_ScopeId scope, groove_GraphRuntimeView *v) {
    v->schema = e->schema; v->graph = e->graph; v->variant_projections = e->variant_projections;
    v->table_deltas = e->table_deltas; v->n_table_deltas = e->n_table_deltas;
    v->binding_deltas = e->binding_deltas; v->n_binding_deltas = e->n_binding_deltas;
    v->binding_snapshots = e->binding_snapshots; v->current_tick = e->current_tick;
    v->operator_states = e->operator_states; v->arrangement_states = e->arrangement_states;
    v->arrangement_keys_by_input = e->arrangement_keys_by_input;
    v->eval_memo = e->eval_memo; v->eval_memo_bytes = e->eval_memo_bytes;
    v->table_frontiers = e->table_frontiers; v->binding_frontiers = e->binding_frontiers;
    v->memo_use_clock = e->memo_use_clock; v->node_meta = e->node_meta;
    v->storage = e->storage; v->evaluation_inputs = e->evaluation_inputs;
    v->scope = scope; v->metrics = e->metrics;
}

/* Retained recursive work yields through operator state. */
static jz_err EV(cooperative_operator_yield)(void) { return groove_poll_pending(); }

static jz_err TE(update_recursive)(groove_TickEvaluator *e, groove_NodeId node, const groove_RecursiveOp *rec, groove_RecordDescriptor output_desc, groove_NodeId seed, groove_NodeId step, bool has_witness, groove_NodeId witness, groove_RecordDeltas *out) {
    if (!e->storage) return jz_err_new(GROOVE_IVM_ERR_STORAGE_UNAVAILABLE, "storage unavailable for durable node");
    groove_OperatorStateKey operator_key = TE(operator_key)(e, node);
    u64 input_generation = TE(input_generation)(e, node);
    groove_RecursiveNodes nodes = { seed, step, has_witness, witness };
    if (e->context.eval_mode == GROOVE_EVAL_TICK) {
        const groove_OperatorState *st = jz_hmap_get(e->operator_states, &operator_key);
        const groove_RecursiveState *state = st && st->tag == GROOVE_OS_RECURSIVE ? st->v.recursive.value : NULL;
        bool some = false; groove_NodeId root;
        JZ_TRY(groove__ivm__runtime__recursion__snapshot_requirement(e->graph, node, nodes, e->table_deltas, e->n_table_deltas, e->binding_deltas, e->n_binding_deltas, state, &some, &root));
        if (some && e->evaluation_inputs)
            JZ_TRY(groove__ivm__runtime__recursion__require_snapshot_inputs(e->graph, e->evaluation_inputs, root));
    }
    /* Recursive child evaluation may touch the same state maps. Remove only
     * this recursive node's state; child operator state stays available. */
    groove_OperatorState operator;
    groove_OperatorStateKey rk;
    if (!jz_hmap_remove(e->operator_states, &operator_key, &rk, &operator)) {
        memset(&operator, 0, sizeof(operator));
        operator.tag = GROOVE_OS_RECURSIVE;
        operator.v.recursive.value = groove__ivm__runtime__recursion__RecursiveState__default();
    }
    if (operator.tag != GROOVE_OS_RECURSIVE) { EV(OperatorState__drop)(&operator); return groove_err_state_mismatch(node); }
    groove_AsOfRecursive *ra = &operator.v.recursive;
    groove_SubTick tick = { e->current_tick, 0 };
    jz_err err = JZ_OK;
    if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) {
        u64 hydrated = 0;
        if (!groove__ivm__runtime__recursion__RecursiveState__has_pending_hydration(ra->value)
            && groove__ivm__runtime__recursion__RecursiveState__step_arrangements_hydrated(ra->value)
            && as_of_eq(&ra->as_of, tick)
            && groove__ivm__runtime__recursion__RecursiveState__hydrated_input_generation(ra->value, &hydrated)
            && hydrated == input_generation) {
            err = groove__ivm__runtime__state__AsOf__value_at(&ra->as_of, tick, true);
            if (err.code) { EV(OperatorState__drop)(&operator); return err; }
            groove_RecordDeltaVec deltas = {0};
            groove__ivm__runtime__recursion__RecursiveState__accumulated_deltas(ra->value, &deltas);
            jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
            out->descriptor = output_desc; out->deltas = deltas;
            return JZ_OK;
        }
        groove_ScopeId scope = groove__ivm__runtime__state__ScopeId__child(e->context.scope, node);
        groove_HydrationRecomputeContext ctx = {
            e->schema, e->graph, e->variant_projections, e->evaluation_inputs,
            e->table_deltas, e->n_table_deltas, true, e->storage, e->binding_snapshots, scope, input_generation,
        };
        u32 progress = 0;
        err = groove__ivm__runtime__recursion__resume_inputs_hydration_recompute(ra->value, ctx, node, rec, output_desc, nodes, &progress);
        if (err.code) { jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL); return err; }
        if (progress == GROOVE_HRP_YIELD) {
            jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
            return EV(cooperative_operator_yield)();
        }
        groove_RecordDeltas accumulated;
        groove__ivm__runtime__recursion__RecursiveState__pending_hydration_accumulated_deltas(ra->value, output_desc, &accumulated);
        groove_GraphRuntimeView runtime;
        view_from_evaluator(e, scope, &runtime);
        groove_RecordDeltas copy;
        groove__ivm__runtime__subscriptions__RecordDeltas__clone(&copy, &accumulated);
        err = groove__ivm__runtime__recursion__hydrate_recursive_arrangements(&runtime, rec, step, copy);
        if (!err.code && has_witness) {
            groove_RecordDeltas wd = groove_RecordDeltas_empty(output_desc);
            groove__ivm__runtime__recursion__RecursiveState__accumulated_deltas(ra->value, &wd.deltas);
            err = groove__ivm__runtime__recursion__hydrate_recursive_arrangements(&runtime, rec, witness, wd);
        }
        if (err.code) {
            /* Rust drops the removed operator on this `?` path. */
            EV(OperatorState__drop)(&operator);
            groove__ivm__runtime__subscriptions__RecordDeltas__drop(&accumulated);
            return err;
        }
        if (groove__ivm__runtime__recursion__RecursiveState__has_pending_hydration(ra->value)) {
            jz_hmap next, step_witness;
            groove__ivm__runtime__recursion__RecursiveState__finish_hydration_recompute(ra->value, &next, &step_witness);
            groove__ivm__runtime__recursion__RecursiveState__replace_with(ra->value, next, NULL);
            groove__ivm__runtime__recursion__RecursiveState__replace_step_witness_with(ra->value, step_witness);
        }
        groove__ivm__runtime__recursion__RecursiveState__mark_step_arrangements_hydrated(ra->value);
        groove__ivm__runtime__recursion__RecursiveState__mark_hydrated_input_generation(ra->value, input_generation);
        err = groove__ivm__runtime__state__AsOf__mark_forward_as_of(&ra->as_of, tick, true);
        if (err.code) { EV(OperatorState__drop)(&operator); groove__ivm__runtime__subscriptions__RecordDeltas__drop(&accumulated); return err; }
        jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
        *out = accumulated;
        return JZ_OK;
    }
    groove_GraphRuntimeView runtime;
    view_from_evaluator(e, groove__ivm__runtime__state__ScopeId__child(e->context.scope, node), &runtime);
    u32 progress = 0;
    groove_RecordDeltaVec deltas = {0};
    err = groove__ivm__runtime__recursion__recursive_delta(ra->value, &runtime, node, rec, output_desc, nodes, &progress, &deltas);
    if (err.code) { jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL); return err; }
    if (progress == GROOVE_RDP_YIELD) {
        jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
        return EV(cooperative_operator_yield)();
    }
    err = groove__ivm__runtime__state__AsOf__mark_forward_as_of(&ra->as_of, tick, true);
    if (err.code) { EV(OperatorState__drop)(&operator); delta_vec_drop(&deltas); return err; }
    groove__ivm__runtime__recursion__RecursiveState__mark_hydrated_input_generation(ra->value, input_generation);
    jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
    out->descriptor = output_desc; out->deltas = deltas;
    return JZ_OK;
}

jz_err TE(update_recursive_step_witness)(groove_TickEvaluator *e, groove_NodeId recursive, groove_RecordDescriptor output_desc, groove_RecordDeltas *out) {
    groove_OperatorStateKey key = TE(operator_key)(e, recursive);
    const groove_OperatorState *st = jz_hmap_get(e->operator_states, &key);
    if (!st || st->tag != GROOVE_OS_RECURSIVE) return groove_err_state_mismatch(recursive);
    groove_RecordDeltaVec deltas = {0};
    if (e->context.eval_mode == GROOVE_EVAL_HYDRATE) groove__ivm__runtime__recursion__RecursiveState__step_witness_accumulated_deltas(st->v.recursive.value, &deltas);
    else groove__ivm__runtime__recursion__RecursiveState__step_witness_deltas(st->v.recursive.value, &deltas);
    out->descriptor = output_desc; out->deltas = deltas;
    return JZ_OK;
}

static jz_err TE(update_unary_input)(groove_TickEvaluator *e, const groove_GraphNode *gn, groove_NodeId node, groove_RecordDeltas **out) {
    if (gn->descriptor.inputs.len == 0) return groove_err_input_missing(node);
    return TE(update_node)(e, gn->descriptor.inputs.ptr[0], out);
}

/* ---------------------------------------------------------------------
 * StreamingChecksum
 * ------------------------------------------------------------------- */
static void values_drop(groove_ValueVec *v) {
    for (u32 i = 0; i < v->len; i++) groove__records__values__Value__drop(&v->ptr[i]);
    jz_vec_drop(v, sizeof(groove_Value), NULL);
}

static jz_err TE(update_streaming_checksum)(groove_TickEvaluator *e, groove_NodeId node, const groove_StreamingChecksumOp *checksum, groove_RecordDescriptor output_desc, groove_RecordDeltas *input, groove_RecordDeltas *out) {
    groove_OperatorStateKey operator_key = TE(operator_key)(e, node);
    groove_OperatorState operator;
    groove_OperatorStateKey rk;
    if (!jz_hmap_remove(e->operator_states, &operator_key, &rk, &operator)) {
        groove_OpType op; memset(&op, 0, sizeof(op)); op.tag = GROOVE_OP_STREAMING_CHECKSUM;
        EV(operator_state_for)(&op, &operator);
    }
    if (operator.tag != GROOVE_OS_STREAMING_CHECKSUM) { EV(OperatorState__drop)(&operator); return groove_err_state_mismatch(node); }
    groove_StreamingChecksumOperatorState *state = operator.v.streaming_checksum;
    bool replace_pending = !state->has_pending || !groove__ivm__runtime__subscriptions__RecordDeltas__eq(state->pending.input, input);
    if (replace_pending) {
        if (state->has_pending) pending_checksum_drop(&state->pending);
        state->has_pending = true;
        state->pending.input = jz_rc_clone(input);
        state->pending.next_delta = 0;
        state->pending.current = NULL;
        state->pending.output.ptr = NULL; state->pending.output.len = state->pending.output.cap = 0;
        jz_vec_reserve(&state->pending.output, input->deltas.len, sizeof(groove_RecordDelta));
    }
    groove_PendingStreamingChecksum *pending = &state->pending;
    jz_err err = JZ_OK;
    while (pending->next_delta < pending->input->deltas.len) {
        const groove_RecordDelta *delta = &pending->input->deltas.ptr[pending->next_delta];
        groove_BorrowedRecord record = groove_RecordDelta_borrowed(delta, pending->input->descriptor);
        groove_ValueVec values = {0};
        err = groove__records__BorrowedRecord__to_values(&record, &values);
        if (err.code) goto fail;
        if (checksum->field_idx >= values.len) { values_drop(&values); err = groove_err_field_oob(checksum->field_idx); goto fail; }
        groove_Value *value = &values.ptr[checksum->field_idx];
        u8 digest[32];
        bool has_digest = false;
        switch (value->tag) {
        case GROOVE_V_STRING: jz_blake3((const u8 *)value->v.s.ptr, value->v.s.len, digest); has_digest = true; break;
        case GROOVE_V_BYTES: jz_blake3(value->v.bytes.ptr, value->v.bytes.len, digest); has_digest = true; break;
        case GROOVE_V_LARGE: {
            if (!pending->current) {
                err = groove__large_values__StreamingChecksum__new(value->v.large, checksum->window_bytes, checksum->max_bytes_per_turn, &pending->current);
                if (err.code) { values_drop(&values); goto fail; }
            }
            groove_StreamingChecksum *streaming = pending->current;
            if (groove__large_values__StreamingChecksum__remaining_bytes(streaming) != 0) {
                groove_ByteRange range;
                if (!groove__large_values__StreamingChecksum__next_range(streaming, &range)) jz_panic("non-complete cursor has a range");
                groove_EvaluationInputs *inputs = e->evaluation_inputs;
                if (!inputs) { values_drop(&values); err = groove_err_evaluation_blocked(); goto fail; }
                groove__ivm__runtime__evaluation_session__EvaluationInputs__set_chunk_scope(inputs, true, node);
                jz_bytes bytes = {0};
                err = groove__large_values__byte_range_attempt(groove__large_values__StreamingChecksum__value(streaming), range, inputs, &bytes);
                groove__ivm__runtime__evaluation_session__EvaluationInputs__set_chunk_scope(inputs, false, node);
                if (err.code == GROOVE_IVM_ERR_EVALUATION_BLOCKED) {
                    values_drop(&values);
                    jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
                    return err;
                }
                if (err.code) { values_drop(&values); goto fail; }
                bool should_yield = false;
                err = groove__large_values__StreamingChecksum__consume_window(streaming, &bytes, &should_yield);
                jz_bytes_drop(&bytes);
                if (err.code) { values_drop(&values); goto fail; }
                groove__ivm__runtime__evaluation_session__EvaluationInputs__release_chunks_owned_by(inputs, node);
                if (should_yield) {
                    err = groove__large_values__StreamingChecksum__record_yield(streaming);
                    values_drop(&values);
                    if (err.code) goto fail;
                    jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
                    return EV(cooperative_operator_yield)();
                }
            }
            if (groove__large_values__StreamingChecksum__remaining_bytes(streaming) == 0) {
                groove_StreamingChecksum *completed = pending->current;
                pending->current = NULL;
                if (!completed) jz_panic("complete state exists");
                err = groove__large_values__StreamingChecksum__finish(completed, digest);
                if (err.code) { values_drop(&values); goto fail; }
                has_digest = true;
            }
            break;
        }
        default:
            values_drop(&values);
            err = jz_err_new(GROOVE_IVM_ERR_STREAMING_CHECKSUM_TYPE_MISMATCH, "streaming checksum requires a String or Bytes field");
            goto fail;
        }
        if (!has_digest) { values_drop(&values); continue; }
        groove__records__values__Value__drop(value);
        value->tag = GROOVE_V_BYTES;
        value->v.bytes = jz_bytes_from(digest, 32);
        groove_RecordDelta d = { {0}, delta->weight };
        err = groove__records__RecordDescriptor__create(output_desc, values.ptr, values.len, &d.record);
        values_drop(&values);
        if (err.code) goto fail;
        jz_vec_push(&pending->output, &d, sizeof(d));
        pending->next_delta += 1;
    }
    {
        groove_RecordDeltaVec output = pending->output;
        pending->output.ptr = NULL; pending->output.len = pending->output.cap = 0;
        pending_checksum_drop(pending);
        state->has_pending = false;
        jz_hmap_insert(e->operator_states, &operator_key, &operator, NULL);
        out->descriptor = output_desc; out->deltas = output;
        return JZ_OK;
    }
fail:
    EV(OperatorState__drop)(&operator);
    return err;
}

/* ---------------------------------------------------------------------
 * Indirect value materialization
 * ------------------------------------------------------------------- */
/* Index identity stays in producer representation. Consumers decode only
 * after reconstructing before/after multisets using those exact bytes. */
static jz_err TE(materialize_arranged_records)(groove_TickEvaluator *e, groove_RecordDescriptor d, groove_RecordWeightVec records, const groove_UsizeVec *fields, groove_RecordWeightVec *out) {
    if (!e->evaluation_inputs || (fields && fields->len == 0)) { *out = records; return JZ_OK; }
    groove_RecordDeltas value = groove_RecordDeltas_empty(d);
    for (u32 i = 0; i < records.len; i++) {
        groove_RecordDelta delta = { records.ptr[i].record, records.ptr[i].weight };
        jz_vec_push(&value.deltas, &delta, sizeof(delta));
    }
    jz_vec_drop(&records, sizeof(groove_RecordWeight), NULL);
    groove_RecordDeltas *input = groove__ivm__runtime__subscriptions__RecordDeltas__arc_new(value);
    groove_RecordDeltas *output = NULL;
    jz_err err = fields ? TE(materialize_indirect_field_indices)(e, input, fields->ptr, fields->len, &output)
                        : TE(materialize_indirect_input)(e, input, &output);
    jz_rc_release(input);
    JZ_TRY(err);
    out->ptr = NULL; out->len = out->cap = 0;
    for (u32 i = 0; i < output->deltas.len; i++) {
        groove_RecordWeight w = { jz_bytes_clone(&output->deltas.ptr[i].record), output->deltas.ptr[i].weight };
        jz_vec_push(out, &w, sizeof(w));
    }
    jz_rc_release(output);
    return JZ_OK;
}

static jz_err finish_materialized(groove_RecordDeltas *input, groove_RecordDeltaVec deltas, bool changed, groove_RecordDeltas **out) {
    if (changed) {
        groove_RecordDeltas value = { input->descriptor, deltas };
        *out = groove__ivm__runtime__subscriptions__RecordDeltas__arc_new(value);
    } else {
        delta_vec_drop(&deltas);
        *out = jz_rc_clone(input);
    }
    return JZ_OK;
}

jz_err TE(materialize_indirect_input)(groove_TickEvaluator *e, groove_RecordDeltas *input, groove_RecordDeltas **out) {
    groove_EvaluationInputs *inputs = e->evaluation_inputs;
    if (!inputs) { *out = jz_rc_clone(input); return JZ_OK; }
    groove_RecordDeltaVec deltas = {0};
    jz_vec_reserve(&deltas, input->deltas.len, sizeof(groove_RecordDelta));
    bool changed = false;
    for (u32 i = 0; i < input->deltas.len; i++) {
        const groove_RecordDelta *delta = &input->deltas.ptr[i];
        groove_RecordDelta d = { {0}, delta->weight };
        jz_err err = groove__large_values__materialize_record_attempt(input->descriptor, delta->record.ptr, delta->record.len, inputs, &d.record);
        if (err.code) { delta_vec_drop(&deltas); return err; }
        changed |= !jz_bytes_eq(&d.record, &delta->record);
        jz_vec_push(&deltas, &d, sizeof(d));
    }
    return finish_materialized(input, deltas, changed, out);
}

jz_err TE(materialize_indirect_fields)(groove_TickEvaluator *e, groove_RecordDeltas *input, const jz_btree *fields, groove_RecordDeltas **out) {
    groove_UsizeVec indices = {0};
    jz_str *field; void *unused;
    JZ_BTREE_FOR(fields, field, unused) {
        usize idx;
        if (!groove__ivm__runtime__record_projection__resolve_field_name(input->descriptor, field->ptr, field->len, &idx)) {
            jz_vec_drop(&indices, sizeof(usize), NULL);
            return groove_err_field_not_found(field->ptr, field->len);
        }
        jz_vec_push(&indices, &idx, sizeof(idx));
    }
    jz_err err = TE(materialize_indirect_field_indices)(e, input, indices.ptr, indices.len, out);
    jz_vec_drop(&indices, sizeof(usize), NULL);
    return err;
}

jz_err TE(materialize_indirect_field_indices)(groove_TickEvaluator *e, groove_RecordDeltas *input, const usize *idx, u32 n, groove_RecordDeltas **out) {
    groove_EvaluationInputs *inputs = e->evaluation_inputs;
    if (!inputs) { *out = jz_rc_clone(input); return JZ_OK; }
    bool required = false;
    u32 nfields = input->descriptor->fields.len;
    for (u32 i = 0; i < n; i++) {
        if (idx[i] >= nfields)
            return jz_err_fmt(GROOVE_IVM_ERR_RECORD_ENCODING, "field index %zu out of bounds for descriptor length %u", idx[i], nfields);
        required = required || groove__records__values__ValueType__may_contain_stored_scalar(&input->descriptor->fields.ptr[idx[i]].value_type);
    }
    if (!required) { *out = jz_rc_clone(input); return JZ_OK; }
    groove_RecordDeltaVec deltas = {0};
    jz_vec_reserve(&deltas, input->deltas.len, sizeof(groove_RecordDelta));
    bool changed = false;
    for (u32 i = 0; i < input->deltas.len; i++) {
        const groove_RecordDelta *delta = &input->deltas.ptr[i];
        groove_RecordDelta d = { {0}, delta->weight };
        jz_err err = groove__large_values__materialize_record_fields_attempt(input->descriptor, delta->record.ptr, delta->record.len, idx, n, inputs, &d.record);
        if (err.code) { delta_vec_drop(&deltas); return err; }
        changed |= !jz_bytes_eq(&d.record, &delta->record);
        jz_vec_push(&deltas, &d, sizeof(d));
    }
    return finish_materialized(input, deltas, changed, out);
}
