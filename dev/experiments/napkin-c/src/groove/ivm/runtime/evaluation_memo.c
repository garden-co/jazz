/* groove::ivm::runtime::evaluation_memo: dense frame slots for ordinary
 * execution, keyed overflow for recursive scopes and alternate contexts. */
#include "mod.h"

static const jz_hmap_desc MEMO_MAP = { sizeof(groove_EvalMemoKey), sizeof(groove_EvalMemoEntry), groove_EvalMemoKey_hash, groove_EvalMemoKey_eq, NULL, groove_EvalMemoEntry_drop };

void groove__ivm__runtime__evaluation_memo__EvaluationMemo__default(groove_EvaluationMemo *m) {
    memset(m, 0, sizeof(*m));
    jz_hmap_init(&m->overflow, &MEMO_MAP);
}

static void slot_drop(void *s) { groove_EvalMemoSlot *slot = s; if (slot->some) groove_EvalMemoEntry_drop(&slot->entry); }
static void slot_clone(void *dst, const void *src) {
    groove_EvalMemoSlot *d = dst; const groove_EvalMemoSlot *s = src;
    *d = *s;
    if (s->some) groove_EvalMemoEntry_clone(&d->entry, &s->entry);
}

void groove_EvaluationMemo_drop(groove_EvaluationMemo *m) {
    if (m->layout) jz_rc_release(m->layout);
    jz_vec_drop(&m->slots, sizeof(groove_EvalMemoSlot), slot_drop);
    jz_hmap_drop(&m->overflow);
}

void groove_EvaluationMemo_clone(groove_EvaluationMemo *dst, const groove_EvaluationMemo *src) {
    groove__ivm__runtime__evaluation_memo__EvaluationMemo__default(dst);
    dst->layout = src->layout ? jz_rc_clone(src->layout) : NULL;
    jz_vec_clone(&dst->slots, &src->slots, sizeof(groove_EvalMemoSlot), slot_clone);
    dst->occupied = src->occupied;
    dst->tick_entries = src->tick_entries;
    groove_EvalMemoKey *k; groove_EvalMemoEntry *v;
    JZ_HMAP_FOR(&src->overflow, k, v) {
        groove_EvalMemoEntry e; groove_EvalMemoEntry_clone(&e, v);
        jz_hmap_insert(&dst->overflow, k, &e, NULL);
    }
}

void groove__ivm__runtime__evaluation_memo__EvaluationMemo__for_layout(groove_EvaluationMemo *m, groove_ExecutionLayout *layout) {
    groove__ivm__runtime__evaluation_memo__EvaluationMemo__default(m);
    u32 n = layout->nodes.len;
    jz_vec_reserve(&m->slots, n, sizeof(groove_EvalMemoSlot));
    memset(m->slots.ptr, 0, (usize)n * sizeof(groove_EvalMemoSlot));
    m->slots.len = n;
    m->layout = layout;
}

/* Durable evaluation may precede discovery of the active subscription
 * layout. Move its results into that layout without copying their batches. */
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__set_layout(groove_EvaluationMemo *m, groove_ExecutionLayout *layout) {
    groove_EvaluationMemo previous = *m;
    groove__ivm__runtime__evaluation_memo__EvaluationMemo__for_layout(m, layout);
    /* extend(previous.into_entries()) */
    for (u32 i = 0; i < previous.slots.len; i++) {
        groove_EvalMemoSlot *s = &previous.slots.ptr[i];
        if (s->some) {
            s->some = false;
            groove__ivm__runtime__evaluation_memo__EvaluationMemo__insert(m, s->key, s->entry, NULL);
        }
    }
    jz_vec_drop(&previous.slots, sizeof(groove_EvalMemoSlot), NULL);
    groove_EvalMemoKey key; groove_EvalMemoEntry entry;
    for (;;) {
        jz_hmap_iter it; jz_hmap_iter_begin(&it, &previous.overflow);
        groove_EvalMemoKey *k; groove_EvalMemoEntry *v;
        if (!jz_hmap_next(&it, (void **)&k, (void **)&v)) break;
        key = *k;
        jz_hmap_remove(&previous.overflow, &key, NULL, &entry);
        groove__ivm__runtime__evaluation_memo__EvaluationMemo__insert(m, key, entry, NULL);
    }
    jz_hmap_drop(&previous.overflow);
    if (previous.layout) jz_rc_release(previous.layout);
}

static bool slot_for(const groove_EvaluationMemo *m, const groove_EvalMemoKey *key, u32 *slot) {
    if (key->scope != groove__ivm__runtime__state__ScopeId__root()) return false;
    if (!m->layout) return false;
    const u32 *s = jz_hmap_get(&m->layout->slots, &key->node);
    if (!s) return false;
    *slot = *s;
    return true;
}

const groove_EvalMemoSlot *groove__ivm__runtime__evaluation_memo__EvaluationMemo__slot(const groove_EvaluationMemo *m, u32 slot) {
    if (slot >= m->slots.len || !m->slots.ptr[slot].some) return NULL;
    return &m->slots.ptr[slot];
}

groove_EvalMemoSlot *groove__ivm__runtime__evaluation_memo__EvaluationMemo__slot_mut(groove_EvaluationMemo *m, u32 slot) {
    if (slot >= m->slots.len || !m->slots.ptr[slot].some) return NULL;
    return &m->slots.ptr[slot];
}

const groove_EvalMemoEntry *groove__ivm__runtime__evaluation_memo__EvaluationMemo__get(const groove_EvaluationMemo *m, const groove_EvalMemoKey *key) {
    u32 slot;
    if (slot_for(m, key, &slot)) {
        const groove_EvalMemoSlot *s = &m->slots.ptr[slot];
        if (s->some && groove_EvalMemoKey_eq(&s->key, key)) return &s->entry;
    }
    return jz_hmap_get(&m->overflow, key);
}

groove_EvalMemoEntry *groove__ivm__runtime__evaluation_memo__EvaluationMemo__get_mut(groove_EvaluationMemo *m, const groove_EvalMemoKey *key) {
    u32 slot;
    if (slot_for(m, key, &slot)) {
        groove_EvalMemoSlot *s = &m->slots.ptr[slot];
        if (s->some && groove_EvalMemoKey_eq(&s->key, key)) return &s->entry;
    }
    return jz_hmap_get(&m->overflow, key);
}

/* Returns the previous entry for exactly this key, if any. */
static bool insert_entry(groove_EvaluationMemo *m, groove_EvalMemoKey key, groove_EvalMemoEntry entry, groove_EvalMemoEntry *previous) {
    u32 slot;
    if (!slot_for(m, &key, &slot)) return jz_hmap_insert(&m->overflow, &key, &entry, previous);
    groove_EvalMemoSlot *s = &m->slots.ptr[slot];
    if (s->some && groove_EvalMemoKey_eq(&s->key, &key)) {
        if (previous) *previous = s->entry; else groove_EvalMemoEntry_drop(&s->entry);
        s->entry = entry;
        return true;
    }
    /* A different context must not overwrite a still-reusable result.
     * Spill the displaced context; each full key still has exactly one owner. */
    groove_EvalMemoEntry replaced;
    bool had = jz_hmap_remove(&m->overflow, &key, NULL, &replaced);
    if (s->some) {
        groove_EvalMemoKey old_key = s->key;
        groove_EvalMemoEntry old_entry = s->entry;
        s->key = key; s->entry = entry;
        jz_hmap_insert(&m->overflow, &old_key, &old_entry, NULL);
    } else {
        s->some = true; s->key = key; s->entry = entry;
        m->occupied += 1;
    }
    if (had) { if (previous) *previous = replaced; else groove_EvalMemoEntry_drop(&replaced); }
    return had;
}

bool groove__ivm__runtime__evaluation_memo__EvaluationMemo__insert(groove_EvaluationMemo *m, groove_EvalMemoKey key, groove_EvalMemoEntry entry, groove_EvalMemoEntry *previous) {
    bool tick_keyed = key.has_tick_epoch;
    bool had = insert_entry(m, key, entry, previous);
    if (tick_keyed && !had) m->tick_entries += 1;
    return had;
}

static bool remove_entry(groove_EvaluationMemo *m, const groove_EvalMemoKey *key, groove_EvalMemoEntry *removed) {
    u32 slot;
    if (slot_for(m, key, &slot)) {
        groove_EvalMemoSlot *s = &m->slots.ptr[slot];
        if (s->some && groove_EvalMemoKey_eq(&s->key, key)) {
            m->occupied -= 1;
            s->some = false;
            if (removed) *removed = s->entry; else groove_EvalMemoEntry_drop(&s->entry);
            return true;
        }
    }
    return jz_hmap_remove(&m->overflow, key, NULL, removed);
}

bool groove__ivm__runtime__evaluation_memo__EvaluationMemo__remove(groove_EvaluationMemo *m, const groove_EvalMemoKey *key, groove_EvalMemoEntry *removed) {
    bool had = remove_entry(m, key, removed);
    if (had && key->has_tick_epoch) m->tick_entries -= 1;
    return had;
}

typedef struct RetainCtx { bool (*keep)(const groove_EvalMemoKey *, groove_EvalMemoEntry *, void *); void *ctx; u32 removed_tick_entries; } RetainCtx;
static bool retain_overflow(const void *k, void *v, void *c) {
    RetainCtx *rc = c;
    const groove_EvalMemoKey *key = k;
    bool kept = rc->keep(key, v, rc->ctx);
    rc->removed_tick_entries += (!kept && key->has_tick_epoch);
    return kept;
}

void groove__ivm__runtime__evaluation_memo__EvaluationMemo__retain(groove_EvaluationMemo *m, bool (*keep)(const groove_EvalMemoKey *, groove_EvalMemoEntry *, void *), void *ctx) {
    RetainCtx rc = { keep, ctx, 0 };
    for (u32 i = 0; i < m->slots.len; i++) {
        groove_EvalMemoSlot *s = &m->slots.ptr[i];
        if (s->some && !keep(&s->key, &s->entry, ctx)) {
            rc.removed_tick_entries += s->key.has_tick_epoch;
            groove_EvalMemoEntry_drop(&s->entry);
            s->some = false;
            m->occupied -= 1;
        }
    }
    jz_hmap_retain(&m->overflow, retain_overflow, &rc);
    m->tick_entries -= rc.removed_tick_entries;
}

u32 groove__ivm__runtime__evaluation_memo__EvaluationMemo__tick_entries(const groove_EvaluationMemo *m) { return m->tick_entries; }
u32 groove__ivm__runtime__evaluation_memo__EvaluationMemo__len(const groove_EvaluationMemo *m) { return m->occupied + m->overflow.len; }

void groove__ivm__runtime__evaluation_memo__EvaluationMemo__iter(const groove_EvaluationMemo *m, groove_EvalMemoIter *it) {
    it->m = m; it->slot = 0;
    jz_hmap_iter_begin(&it->oit, &m->overflow);
}

bool groove__ivm__runtime__evaluation_memo__EvalMemoIter__next(groove_EvalMemoIter *it, const groove_EvalMemoKey **key, const groove_EvalMemoEntry **entry) {
    while (it->slot < it->m->slots.len) {
        const groove_EvalMemoSlot *s = &it->m->slots.ptr[it->slot++];
        if (s->some) { *key = &s->key; *entry = &s->entry; return true; }
    }
    return jz_hmap_next(&it->oit, (void **)key, (void **)entry);
}
