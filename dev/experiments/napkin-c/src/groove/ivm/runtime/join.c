/* groove::ivm::runtime::join: join, anti-join, and arrangement maintenance.
 * JoinBucketMap<V> is instantiated for i64 and Option<i64> in Rust; one C copy
 * stores Option<i64> for both (base entries are always Some). */
#include "mod.h"

typedef struct OptI64 { bool some; i64 v; } OptI64;

/* ---------- JoinBucketMap ---------- */
typedef enum { BM_Empty, BM_One, BM_Many } BucketMapTag;
typedef struct BucketMap { u8 tag; jz_bytes one_key; OptI64 one_val; jz_hmap many; /* Bytes -> OptI64 */ } BucketMap;

static const jz_hmap_desc BYTES_OPT_MAP = { sizeof(jz_bytes), sizeof(OptI64), groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, NULL };

static void bm_drop(void *p) {
    BucketMap *m = p;
    if (m->tag == BM_One) jz_bytes_drop(&m->one_key);
    else if (m->tag == BM_Many) jz_hmap_drop(&m->many);
    m->tag = BM_Empty;
}
static void bm_clone(void *dst, const void *src) {
    BucketMap *d = dst; const BucketMap *s = src;
    d->tag = s->tag;
    if (s->tag == BM_One) { d->one_key = jz_bytes_clone(&s->one_key); d->one_val = s->one_val; }
    else if (s->tag == BM_Many) {
        jz_hmap_init(&d->many, &BYTES_OPT_MAP);
        jz_bytes *k; OptI64 *v;
        JZ_HMAP_FOR(&s->many, k, v) { jz_bytes c = jz_bytes_clone(k); jz_hmap_insert(&d->many, &c, v, NULL); }
    }
}
static BucketMap *bm_new(void) { BucketMap *m = jz_rc_new(sizeof(BucketMap), bm_drop); memset(m, 0, sizeof *m); return m; }

static const OptI64 *bm_get(const BucketMap *m, const jz_bytes *record) {
    switch (m->tag) {
    case BM_Empty: return NULL;
    case BM_One: return jz_bytes_eq(&m->one_key, record) ? &m->one_val : NULL;
    default: return jz_hmap_get(&m->many, record);
    }
}
static bool bm_contains_key(const BucketMap *m, const jz_bytes *record) { return bm_get(m, record) != NULL; }
static bool bm_is_empty(const BucketMap *m) {
    switch (m->tag) {
    case BM_Empty: return true;
    case BM_One: return false;
    default: return m->many.len == 0;
    }
}
/* takes ownership of record */
static void bm_insert(BucketMap *m, jz_bytes record, OptI64 value) {
    switch (m->tag) {
    case BM_Empty:
        m->tag = BM_One; m->one_key = record; m->one_val = value;
        return;
    case BM_One:
        if (jz_bytes_eq(&m->one_key, &record)) { m->one_val = value; jz_bytes_drop(&record); return; }
        {
            jz_bytes old_record = m->one_key; OptI64 old_value = m->one_val;
            m->tag = BM_Many;
            jz_hmap_init(&m->many, &BYTES_OPT_MAP);
            jz_hmap_insert(&m->many, &old_record, &old_value, NULL);
            jz_hmap_insert(&m->many, &record, &value, NULL);
        }
        return;
    default: {
        OptI64 old;
        jz_bytes probe = record;
        OptI64 *slot = jz_hmap_get(&m->many, &probe);
        if (slot) { *slot = value; jz_bytes_drop(&record); return; }
        jz_hmap_insert(&m->many, &record, &value, &old);
    }
    }
}
static void bm_remove(BucketMap *m, const jz_bytes *record) {
    if (m->tag == BM_One && jz_bytes_eq(&m->one_key, record)) { jz_bytes_drop(&m->one_key); m->tag = BM_Empty; }
    else if (m->tag == BM_Many) { jz_bytes k; if (jz_hmap_remove(&m->many, record, &k, NULL)) jz_bytes_drop(&k); }
}

typedef struct BmIter { const BucketMap *m; bool one_done; jz_hmap_iter it; } BmIter;
static void bm_iter(const BucketMap *m, BmIter *it) { it->m = m; it->one_done = false; if (m->tag == BM_Many) jz_hmap_iter_begin(&it->it, &m->many); }
static bool bm_next(BmIter *it, const jz_bytes **k, const OptI64 **v) {
    if (it->m->tag == BM_One) {
        if (it->one_done) return false;
        it->one_done = true; *k = &it->m->one_key; *v = &it->m->one_val;
        return true;
    }
    if (it->m->tag == BM_Many) return jz_hmap_next(&it->it, (void **)k, (void **)v);
    return false;
}

/* ---------- JoinBucket ---------- */
typedef struct JoinBucket { BucketMap *base; BucketMap *overlay; i64 total_weight; u32 record_count; } JoinBucket;
static void bucket_default(JoinBucket *b) { b->base = bm_new(); b->overlay = bm_new(); b->total_weight = 0; b->record_count = 0; }
static void bucket_drop(void *p) { JoinBucket *b = p; jz_rc_release(b->base); jz_rc_release(b->overlay); }
static void bucket_clone(void *dst, const void *src) {
    JoinBucket *d = dst; const JoinBucket *s = src;
    *d = *s;
    d->base = jz_rc_clone(s->base);
    d->overlay = jz_rc_clone(s->overlay);
}

static i64 bucket_add_weight(JoinBucket *b, const jz_bytes *record, i64 delta) {
    /* An overlay tombstone means zero, not the weight in the base. */
    BucketMap *overlay = jz_rc_make_mut((void **)&b->overlay, sizeof(BucketMap), bm_clone);
    i64 previous, next;
    if (overlay->tag == BM_Many) {
        bool inserted;
        jz_bytes probe = *record;
        OptI64 *weight = jz_hmap_get(&overlay->many, &probe);
        if (!weight) {
            jz_bytes k = jz_bytes_clone(record);
            weight = jz_hmap_entry(&overlay->many, &k, &inserted);
            const OptI64 *base = bm_get(b->base, record);
            *weight = base ? *base : (OptI64){ false, 0 };
        }
        previous = weight->some ? weight->v : 0;
        next = previous + delta;
        weight->some = next != 0; weight->v = next;
    } else {
        const OptI64 *o = bm_get(overlay, record);
        OptI64 weight;
        if (o) weight = *o;
        else { const OptI64 *base = bm_get(b->base, record); weight = base ? *base : (OptI64){ false, 0 }; }
        previous = weight.some ? weight.v : 0;
        next = previous + delta;
        bm_insert(overlay, jz_bytes_clone(record), (OptI64){ next != 0, next });
    }
    b->total_weight += delta;
    b->record_count += (previous == 0 && next != 0);
    b->record_count -= (previous != 0 && next == 0);
    return next;
}

/* iter: base entries not shadowed by the overlay, then live overlay entries */
typedef struct BucketIter { const JoinBucket *b; u8 phase; BmIter it; } BucketIter;
static void bucket_iter(const JoinBucket *b, BucketIter *it) { it->b = b; it->phase = 0; bm_iter(b->base, &it->it); }
static bool bucket_next(BucketIter *it, const jz_bytes **record, i64 *weight) {
    const jz_bytes *k; const OptI64 *v;
    if (it->phase == 0) {
        while (bm_next(&it->it, &k, &v)) {
            if (bm_contains_key(it->b->overlay, k)) continue;
            *record = k; *weight = v->v;
            return true;
        }
        it->phase = 1;
        bm_iter(it->b->overlay, &it->it);
    }
    while (bm_next(&it->it, &k, &v)) {
        if (!v->some) continue;
        *record = k; *weight = v->v;
        return true;
    }
    return false;
}

static bool bucket_is_empty(const JoinBucket *b) { return b->record_count == 0; }

static void bucket_commit_overlay(JoinBucket *b) {
    if (bm_is_empty(b->overlay)) return;
    BucketMap *overlay = b->overlay;
    b->overlay = bm_new();
    BucketMap owned;
    if (jz_rc_strong(overlay) == 1) { owned = *overlay; overlay->tag = BM_Empty; }
    else bm_clone(&owned, overlay);
    jz_rc_release(overlay);
    BucketMap *base = jz_rc_make_mut((void **)&b->base, sizeof(BucketMap), bm_clone);
    BmIter it; const jz_bytes *k; const OptI64 *v;
    bm_iter(&owned, &it);
    while (bm_next(&it, &k, &v)) {
        if (v->some) bm_insert(base, jz_bytes_clone(k), *v);
        else bm_remove(base, k);
    }
    bm_drop(&owned);
}

/* ---------- JoinIndex = HashMap<JoinKey, JoinBucket> ---------- */
static const jz_hmap_desc JOIN_INDEX = { sizeof(jz_bytes), sizeof(JoinBucket), groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, bucket_drop };
typedef struct OptBucket { bool some; JoinBucket b; } OptBucket;
static void opt_bucket_drop(void *p) { OptBucket *o = p; if (o->some) bucket_drop(&o->b); }
static const jz_hmap_desc OVERLAY_MAP = { sizeof(jz_bytes), sizeof(OptBucket), groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, opt_bucket_drop };

static void hmap_rc_drop(void *p) { jz_hmap_drop(p); }
static void index_clone(void *dst, const void *src) {
    jz_hmap *d = dst; const jz_hmap *s = src;
    jz_hmap_init(d, &JOIN_INDEX);
    jz_bytes *k; JoinBucket *v;
    JZ_HMAP_FOR(s, k, v) { jz_bytes kc = jz_bytes_clone(k); JoinBucket bc; bucket_clone(&bc, v); jz_hmap_insert(d, &kc, &bc, NULL); }
}
static void overlay_clone(void *dst, const void *src) {
    jz_hmap *d = dst; const jz_hmap *s = src;
    jz_hmap_init(d, &OVERLAY_MAP);
    jz_bytes *k; OptBucket *v;
    JZ_HMAP_FOR(s, k, v) {
        jz_bytes kc = jz_bytes_clone(k); OptBucket oc = { v->some, {0} };
        if (v->some) bucket_clone(&oc.b, &v->b);
        jz_hmap_insert(d, &kc, &oc, NULL);
    }
}
static jz_hmap *rc_map_new(const jz_hmap_desc *d) { jz_hmap *m = jz_rc_new(sizeof(jz_hmap), hmap_rc_drop); jz_hmap_init(m, d); return m; }

/* ---------- touched_join_keys ---------- */
typedef struct KeyedRecordDelta { const groove_RecordDelta *delta; jz_bytes key; } KeyedRecordDelta;
typedef JZ_VEC(KeyedRecordDelta) KeyedVec;
static void keyed_drop(KeyedVec *v) { JZ_VEC_FOR(KeyedRecordDelta, k, *v) jz_bytes_drop(&k->key); jz_vec_drop(v, sizeof(KeyedRecordDelta), NULL); }
static jz_err keyed_join_deltas(groove_RecordDescriptor d, const jz_str *fields, u32 nfields, const groove_RecordDelta *deltas, u32 n, groove_ValueComparison comparison, KeyedVec *out);

jz_err groove__ivm__runtime__join__touched_join_keys(groove_RecordDescriptor d, const jz_str *fields, u32 nfields, const groove_RecordDelta *deltas, u32 n,
    groove_ValueComparison comparison, groove_BytesVec *out) {
    KeyedVec keyed;
    JZ_TRY(keyed_join_deltas(d, fields, nfields, deltas, n, comparison, &keyed));
    out->ptr = NULL; out->len = 0; out->cap = 0;
    JZ_VEC_FOR(KeyedRecordDelta, k, keyed) JZ_VEC_PUSH(*out, k->key);
    jz_vec_drop(&keyed, sizeof(KeyedRecordDelta), NULL);
    return JZ_OK;
}

/* ---------- VisibilityState / ThresholdJoinState ---------- */
static const jz_hmap_desc KEY_SET = { sizeof(jz_bytes), 0, groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, NULL };
static const jz_hmap_desc KEY_BOOL_MAP = { sizeof(jz_bytes), sizeof(bool), groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, NULL };
typedef struct VisibilityState {
    jz_hmap *visible;   /* Rc<HashSet<JoinKey>> */
    jz_hmap changes;    /* JoinKey -> bool */
    bool has_consumed; groove_SubTick consumed[2];
} VisibilityState;
struct groove_ThresholdJoinState { VisibilityState published; };

static void key_set_clone(void *dst, const void *src) {
    jz_hmap *d = dst; const jz_hmap *s = src;
    jz_hmap_init(d, &KEY_SET);
    jz_bytes *k; void *u;
    JZ_HMAP_FOR(s, k, u) { jz_bytes c = jz_bytes_clone(k); jz_hmap_insert(d, &c, NULL, NULL); }
}
static void visibility_default(VisibilityState *v) {
    v->visible = rc_map_new(&KEY_SET);
    jz_hmap_init(&v->changes, &KEY_BOOL_MAP);
    v->has_consumed = false;
}
static void visibility_drop(VisibilityState *v) { jz_rc_release(v->visible); jz_hmap_drop(&v->changes); }
static void visibility_clone(VisibilityState *d, const VisibilityState *s) {
    d->visible = jz_rc_clone(s->visible);
    jz_hmap_init(&d->changes, &KEY_BOOL_MAP);
    jz_bytes *k; bool *v;
    JZ_HMAP_FOR(&s->changes, k, v) { jz_bytes c = jz_bytes_clone(k); jz_hmap_insert(&d->changes, &c, v, NULL); }
    d->has_consumed = s->has_consumed;
    d->consumed[0] = s->consumed[0]; d->consumed[1] = s->consumed[1];
}
static bool visibility_contains(const VisibilityState *v, const jz_bytes *key) {
    const bool *c = jz_hmap_get(&v->changes, key);
    if (c) return *c;
    return jz_hmap_get(v->visible, key) != NULL;
}
static void visibility_set(VisibilityState *v, const jz_bytes *key, bool visible) {
    if (visibility_contains(v, key) == visible) return;
    bool *slot = jz_hmap_get(&v->changes, key);
    if (slot) { *slot = visible; return; }
    jz_bytes k = jz_bytes_clone(key);
    jz_hmap_insert(&v->changes, &k, &visible, NULL);
}
/* This is a transaction journal, not a reusable table-sized buffer: consume it. */
static void visibility_commit(VisibilityState *v) {
    if (v->changes.len == 0) return;
    jz_hmap *visible = jz_rc_make_mut((void **)&v->visible, sizeof(jz_hmap), key_set_clone);
    jz_hmap changes = v->changes;
    jz_hmap_init(&v->changes, &KEY_BOOL_MAP);
    jz_bytes *k; bool *present;
    JZ_HMAP_FOR(&changes, k, present) {
        if (*present) { jz_bytes c = jz_bytes_clone(k); if (jz_hmap_insert(visible, &c, NULL, NULL)) jz_bytes_drop(&c); }
        else { jz_bytes old; if (jz_hmap_remove(visible, k, &old, NULL)) jz_bytes_drop(&old); }
    }
    jz_hmap_drop(&changes);
}

static groove_ThresholdJoinState *threshold_default(void) { groove_ThresholdJoinState *s = JZ_NEW(groove_ThresholdJoinState); visibility_default(&s->published); return s; }
static groove_ThresholdJoinState *threshold_clone(const groove_ThresholdJoinState *s) { groove_ThresholdJoinState *d = JZ_NEW(groove_ThresholdJoinState); visibility_clone(&d->published, &s->published); return d; }
static void threshold_drop(groove_ThresholdJoinState *s) { visibility_drop(&s->published); jz_free(s); }
groove_SemiJoinState *groove__ivm__runtime__join__SemiJoinState__default(void) { return threshold_default(); }
groove_SemiJoinState *groove__ivm__runtime__join__SemiJoinState__clone(const groove_SemiJoinState *s) { return threshold_clone(s); }
void groove__ivm__runtime__join__SemiJoinState__drop(groove_SemiJoinState *s) { threshold_drop(s); }
groove_AntiJoinState *groove__ivm__runtime__join__AntiJoinState__default(void) { return threshold_default(); }
groove_AntiJoinState *groove__ivm__runtime__join__AntiJoinState__clone(const groove_AntiJoinState *s) { return threshold_clone(s); }
void groove__ivm__runtime__join__AntiJoinState__drop(groove_AntiJoinState *s) { threshold_drop(s); }

/* ---------- ArrangementState ---------- */
struct groove_ArrangementState {
    jz_hmap *index;    /* Rc<JoinIndex> */
    jz_hmap *overlay;  /* Rc<HashMap<JoinKey, Option<JoinBucket>>> */
    jz_hmap *changes;  /* Option<Rc<JoinIndex>> */
    const groove_RecordDeltas *snapshot; /* Weak<RecordDeltas>: identity only */
};

groove_ArrangementState *groove__ivm__runtime__join__ArrangementState__default(void) {
    groove_ArrangementState *s = JZ_NEW(groove_ArrangementState);
    s->index = rc_map_new(&JOIN_INDEX);
    s->overlay = rc_map_new(&OVERLAY_MAP);
    s->changes = NULL;
    s->snapshot = NULL;
    return s;
}
groove_ArrangementState *groove__ivm__runtime__join__ArrangementState__clone(const groove_ArrangementState *s) {
    groove_ArrangementState *d = JZ_NEW(groove_ArrangementState);
    d->index = jz_rc_clone(s->index);
    d->overlay = jz_rc_clone(s->overlay);
    d->changes = s->changes ? jz_rc_clone(s->changes) : NULL;
    d->snapshot = s->snapshot;
    return d;
}
void groove__ivm__runtime__join__ArrangementState__drop(groove_ArrangementState *s) {
    jz_rc_release(s->index);
    jz_rc_release(s->overlay);
    if (s->changes) jz_rc_release(s->changes);
    jz_free(s);
}

static const JoinBucket *arrangement_bucket(const groove_ArrangementState *s, const jz_bytes *key) {
    /* JoinKey hashes and compares as its byte slice. */
    const OptBucket *o = jz_hmap_get(s->overlay, key);
    if (o) return o->some ? &o->b : NULL;
    return jz_hmap_get(s->index, key);
}

static i64 arrangement_key_count(const groove_ArrangementState *s, const jz_bytes *key) {
    const JoinBucket *b = arrangement_bucket(s, key);
    return b ? b->total_weight : 0;
}

/* buckets(): index entries not shadowed by the overlay, then live overlay buckets */
typedef struct BucketsIter { const groove_ArrangementState *s; u8 phase; jz_hmap_iter it; } BucketsIter;
static void buckets_iter(const groove_ArrangementState *s, BucketsIter *it) { it->s = s; it->phase = 0; jz_hmap_iter_begin(&it->it, s->index); }
static bool buckets_next(BucketsIter *it, const jz_bytes **key, const JoinBucket **bucket) {
    jz_bytes *k; void *v;
    if (it->phase == 0) {
        while (jz_hmap_next(&it->it, (void **)&k, &v)) {
            if (jz_hmap_get(it->s->overlay, k)) continue;
            *key = k; *bucket = v;
            return true;
        }
        it->phase = 1;
        jz_hmap_iter_begin(&it->it, it->s->overlay);
    }
    while (jz_hmap_next(&it->it, (void **)&k, &v)) {
        OptBucket *o = v;
        if (!o->some) continue;
        *key = k; *bucket = &o->b;
        return true;
    }
    return false;
}

/* Drop the producer's transition for its tick, keeping the overlay. */
void groove__ivm__runtime__join__ArrangementState__release_changes(groove_ArrangementState *s) {
    if (s->changes) jz_rc_release(s->changes);
    s->changes = NULL;
}

/* Fold only the touched buckets into the shared base at tick commit. */
void groove__ivm__runtime__join__ArrangementState__commit_overlay(groove_ArrangementState *s) {
    groove__ivm__runtime__join__ArrangementState__release_changes(s);
    if (s->overlay->len == 0) return;
    jz_hmap *taken = s->overlay;
    s->overlay = rc_map_new(&OVERLAY_MAP);
    jz_hmap overlay;
    if (jz_rc_strong(taken) == 1) { overlay = *taken; jz_hmap_init(taken, &OVERLAY_MAP); }
    else overlay_clone(&overlay, taken);
    jz_rc_release(taken);
    jz_hmap *index = jz_rc_make_mut((void **)&s->index, sizeof(jz_hmap), index_clone);
    jz_bytes *key; OptBucket *bucket;
    JZ_HMAP_FOR(&overlay, key, bucket) {
        /* Release the previous bucket's ownership before checking whether the
         * new bucket can fold its record deltas without copying history. */
        jz_bytes old_key; JoinBucket old;
        if (jz_hmap_remove(index, key, &old_key, &old)) { jz_bytes_drop(&old_key); bucket_drop(&old); }
        if (bucket->some) {
            if (jz_rc_strong(bucket->b.base) == 1 && jz_rc_strong(bucket->b.overlay) == 1) bucket_commit_overlay(&bucket->b);
            jz_bytes kc = jz_bytes_clone(key);
            jz_hmap_insert(index, &kc, &bucket->b, NULL);
            bucket->some = false; /* moved */
        }
    }
    jz_hmap_drop(&overlay);
}

usize groove__ivm__runtime__join__ArrangementState__row_count(const groove_ArrangementState *s) {
    usize total = 0;
    BucketsIter it; const jz_bytes *key; const JoinBucket *bucket;
    buckets_iter(s, &it);
    while (buckets_next(&it, &key, &bucket)) {
        BucketIter bi; const jz_bytes *r; i64 w;
        bucket_iter(bucket, &bi);
        while (bucket_next(&bi, &r, &w)) total += (w != 0);
    }
    return total;
}

usize groove__ivm__runtime__join__ArrangementState__encoded_bytes(const groove_ArrangementState *s) {
    usize total = 0;
    BucketsIter it; const jz_bytes *key; const JoinBucket *bucket;
    buckets_iter(s, &it);
    while (buckets_next(&it, &key, &bucket)) {
        total += key->len;
        BucketIter bi; const jz_bytes *r; i64 w;
        bucket_iter(bucket, &bi);
        while (bucket_next(&bi, &r, &w)) total += r->len;
    }
    return total;
}

static void apply_join_delta_to_index(jz_hmap *index, const KeyedVec *deltas) {
    JZ_VEC_FOR(const KeyedRecordDelta, delta, *deltas) {
        JoinBucket *bucket = jz_hmap_get(index, &delta->key);
        if (!bucket) {
            bool inserted;
            jz_bytes k = jz_bytes_clone(&delta->key);
            bucket = jz_hmap_entry(index, &k, &inserted);
            bucket_default(bucket);
        }
        i64 next_weight = bucket_add_weight(bucket, &delta->delta->record, delta->delta->weight);
        if (next_weight == 0 && bucket_is_empty(bucket)) {
            jz_bytes k; JoinBucket b;
            if (jz_hmap_remove(index, &delta->key, &k, &b)) { jz_bytes_drop(&k); bucket_drop(&b); }
        }
    }
}

static jz_hmap *build_join_delta_index(const KeyedVec *deltas) {
    jz_hmap *index = rc_map_new(&JOIN_INDEX);
    apply_join_delta_to_index(index, deltas);
    jz_bytes *k; JoinBucket *b;
    JZ_HMAP_FOR(index, k, b) bucket_commit_overlay(b);
    return index;
}

static void arrangement_apply_update(groove_ArrangementState *s, const KeyedVec *deltas, u32 update_mode) {
    s->snapshot = NULL;
    /* An empty batch still advances the producer's timestamp, but must not
     * retain the previous transition or allocate an empty shared index. */
    if (s->changes) jz_rc_release(s->changes);
    s->changes = deltas->len != 0 ? build_join_delta_index(deltas) : NULL;
    if (update_mode == groove_ArrangementUpdateMode_Accumulate) {
        if (deltas->len == 0) return;
        const jz_hmap *index = s->index;
        jz_hmap *overlay = jz_rc_make_mut((void **)&s->overlay, sizeof(jz_hmap), overlay_clone);
        JZ_VEC_FOR(const KeyedRecordDelta, delta, *deltas) {
            /* A present None is an overlay tombstone, not permission to
             * resurrect the base. Clone a base bucket only on first touch. */
            OptBucket *slot = jz_hmap_get(overlay, &delta->key);
            if (!slot) {
                bool inserted;
                jz_bytes k = jz_bytes_clone(&delta->key);
                slot = jz_hmap_entry(overlay, &k, &inserted);
                const JoinBucket *base = jz_hmap_get(index, &delta->key);
                slot->some = base != NULL;
                if (base) bucket_clone(&slot->b, base);
            }
            if (!slot->some) { slot->some = true; bucket_default(&slot->b); }
            bucket_add_weight(&slot->b, &delta->delta->record, delta->delta->weight);
            if (bucket_is_empty(&slot->b)) { bucket_drop(&slot->b); slot->some = false; }
        }
    } else {
        jz_rc_release(s->index);
        s->index = s->changes ? jz_rc_clone(s->changes) : rc_map_new(&JOIN_INDEX);
        jz_rc_release(s->overlay);
        s->overlay = rc_map_new(&OVERLAY_MAP);
    }
}

jz_err groove__ivm__runtime__join__ArrangementState__apply_record_deltas(groove_ArrangementState *s, groove_RecordDescriptor d, const jz_str *fields, u32 nfields,
    const groove_RecordDelta *deltas, u32 n, u32 mode) {
    KeyedVec keyed;
    JZ_TRY(keyed_join_deltas(d, fields, nfields, deltas, n, groove_ValueComparison_Exact, &keyed));
    arrangement_apply_update(s, &keyed, mode);
    keyed_drop(&keyed);
    return JZ_OK;
}

void groove__ivm__runtime__join__ArrangementState__records_for_key(const groove_ArrangementState *s, const u8 *key, u32 len, groove_RecordWeightVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    jz_bytes probe = { (u8 *)key, len, len };
    const JoinBucket *bucket = arrangement_bucket(s, &probe);
    if (!bucket) return;
    BucketIter bi; const jz_bytes *r; i64 w;
    bucket_iter(bucket, &bi);
    while (bucket_next(&bi, &r, &w)) {
        if (w > 0) { groove_RecordWeight rw = { jz_bytes_clone(r), w }; JZ_VEC_PUSH(*out, rw); }
    }
}

/* ---------- ArrangementTransition ---------- */
groove_ArrangementTransition groove__ivm__runtime__join__ArrangementTransition__at(const groove_AsOfArrangement *state, groove_SubTick expected) {
    groove_ArrangementTransition t = { state, expected };
    return t;
}
static const groove_ArrangementState *transition_current(groove_ArrangementTransition t) { return t.state->value; }
static const jz_hmap *transition_changes(groove_ArrangementTransition t) {
    if (t.state->as_of.has && groove_SubTick_cmp(t.state->as_of.at, t.expected) == 0) return t.state->value->changes;
    return NULL;
}
static const JoinBucket *transition_delta_bucket(groove_ArrangementTransition t, const jz_bytes *key) {
    const jz_hmap *changes = transition_changes(t);
    return changes ? jz_hmap_get(changes, key) : NULL;
}

/* ---------- join kernel ---------- */
typedef struct OutSpan { u32 start, end; i64 weight; } OutSpan;
typedef struct JoinOutputBuffer { jz_bytes bytes; JZ_VEC(OutSpan) deltas; } JoinOutputBuffer;
typedef enum { LeftDelta, RightDelta } JoinProbeSide;
/* JoinLookup: an arrangement or a bare index */
typedef struct JoinLookup { const groove_ArrangementState *arrangement; const jz_hmap *index; } JoinLookup;
static const JoinBucket *lookup_bucket(const JoinLookup *l, const jz_bytes *key) {
    return l->arrangement ? arrangement_bucket(l->arrangement, key) : jz_hmap_get(l->index, key);
}

static jz_err append_bucket_pairs(JoinOutputBuffer *output, const groove_PreparedRecordCopy *projection, const jz_bytes *key, const JoinBucket *changed,
                                  const JoinLookup *stored, JoinProbeSide side, i64 sign) {
    const JoinBucket *bucket = lookup_bucket(stored, key);
    if (!bucket) return JZ_OK;
    BucketIter ci; const jz_bytes *changed_record; i64 changed_weight;
    bucket_iter(changed, &ci);
    while (bucket_next(&ci, &changed_record, &changed_weight)) {
        BucketIter si; const jz_bytes *stored_record; i64 stored_weight;
        bucket_iter(bucket, &si);
        while (bucket_next(&si, &stored_record, &stored_weight)) {
            i64 weight = sign * changed_weight * stored_weight;
            if (weight == 0) continue;
            jz_slice inputs[2];
            if (side == LeftDelta) { inputs[0] = jz_bytes_slice(changed_record); inputs[1] = jz_bytes_slice(stored_record); }
            else { inputs[0] = jz_bytes_slice(stored_record); inputs[1] = jz_bytes_slice(changed_record); }
            OutSpan span; span.weight = weight;
            JZ_TRY(groove__records__PreparedRecordCopy__project_into(projection, inputs, 2, &output->bytes, &span.start, &span.end));
            JZ_VEC_PUSH(output->deltas, span);
        }
    }
    return JZ_OK;
}

static jz_err append_join_index_changes(JoinOutputBuffer *output, const groove_PreparedRecordCopy *projection, const jz_hmap *changes,
                                        const JoinLookup *stored, JoinProbeSide side, i64 sign) {
    if (!changes) return JZ_OK;
    jz_bytes *key; JoinBucket *changed;
    JZ_HMAP_FOR(changes, key, changed) JZ_TRY(append_bucket_pairs(output, projection, key, changed, stored, side, sign));
    return JZ_OK;
}

static jz_err append_join_arrangement_buckets(JoinOutputBuffer *output, const groove_PreparedRecordCopy *projection, const groove_ArrangementState *source,
                                              const JoinLookup *stored, JoinProbeSide side, i64 sign) {
    BucketsIter it; const jz_bytes *key; const JoinBucket *changed;
    buckets_iter(source, &it);
    while (buckets_next(&it, &key, &changed)) JZ_TRY(append_bucket_pairs(output, projection, key, changed, stored, side, sign));
    return JZ_OK;
}

jz_err groove__ivm__runtime__join__JoinState__evaluate_prepared(groove_ArrangementTransition left, groove_ArrangementTransition right,
    const groove_PreparedRecordCopy *projection, u32 mode, groove_RecordDeltaVec *out) {
    JoinOutputBuffer output; memset(&output, 0, sizeof output);
    JoinLookup right_current = { transition_current(right), NULL };
    JoinLookup left_current = { transition_current(left), NULL };
    jz_err e = JZ_OK;
    if (mode == groove_ArrangementUpdateMode_Replace) {
        e = append_join_arrangement_buckets(&output, projection, transition_current(left), &right_current, LeftDelta, 1);
    } else {
        e = append_join_index_changes(&output, projection, transition_changes(left), &right_current, LeftDelta, 1);
        if (!JZ_IS_ERR(e)) e = append_join_index_changes(&output, projection, transition_changes(right), &left_current, RightDelta, 1);
        /* Both current indexes include this transition. Remove the repeated
         * cross term, borrowing the producer batch rather than rebuilding it. */
        const jz_hmap *left_changes = transition_changes(left);
        if (!JZ_IS_ERR(e) && left_changes) {
            JoinLookup lc = { NULL, left_changes };
            e = append_join_index_changes(&output, projection, transition_changes(right), &lc, RightDelta, -1);
        }
    }
    if (JZ_IS_ERR(e)) { jz_bytes_drop(&output.bytes); jz_vec_drop(&output.deltas, sizeof(OutSpan), NULL); return e; }
    jz_bytes bytes = groove__ivm__runtime__freeze_batch_buffer(output.bytes);
    out->ptr = NULL; out->len = 0; out->cap = 0;
    jz_vec_reserve(out, output.deltas.len, sizeof(groove_RecordDelta));
    JZ_VEC_FOR(OutSpan, s, output.deltas) {
        groove_RecordDelta d = { jz_bytes_from(bytes.ptr + s->start, s->end - s->start), s->weight };
        JZ_VEC_PUSH(*out, d);
    }
    jz_bytes_drop(&bytes);
    jz_vec_drop(&output.deltas, sizeof(OutSpan), NULL);
    groove__ivm__runtime__key_encoding__consolidate_deltas(out);
    return JZ_OK;
}

static void append_bucket(groove_RecordDeltaVec *output, const JoinBucket *bucket, i64 sign) {
    if (!bucket) return;
    BucketIter bi; const jz_bytes *r; i64 w;
    bucket_iter(bucket, &bi);
    while (bucket_next(&bi, &r, &w)) {
        groove_RecordDelta d = { jz_bytes_clone(r), sign * w };
        JZ_VEC_PUSH(*output, d);
    }
}

static void add_keys(jz_hmap *affected, const jz_hmap *m) {
    if (!m) return;
    jz_bytes *k; void *v;
    JZ_HMAP_FOR(m, k, v) { jz_bytes c = jz_bytes_clone(k); if (jz_hmap_insert(affected, &c, NULL, NULL)) jz_bytes_drop(&c); }
}

static void threshold_transition(VisibilityState *published, groove_ArrangementTransition left, groove_ArrangementTransition right,
                                 bool replace, bool semi, groove_RecordDeltaVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    groove_SubTick frontier[2] = { left.expected, right.expected };
    if (!replace && published->has_consumed && groove_SubTick_cmp(published->consumed[0], frontier[0]) == 0 &&
        groove_SubTick_cmp(published->consumed[1], frontier[1]) == 0)
        return;
    jz_hmap affected; jz_hmap_init(&affected, &KEY_SET);
    const groove_ArrangementState *lc = transition_current(left), *rc = transition_current(right);
    if (replace) {
        visibility_drop(published);
        visibility_default(published);
        add_keys(&affected, lc->index);
        add_keys(&affected, lc->overlay);
    } else {
        add_keys(&affected, transition_changes(left));
        add_keys(&affected, transition_changes(right));
    }
    jz_bytes *key; void *unused;
    JZ_HMAP_FOR(&affected, key, unused) {
        bool before = !replace && visibility_contains(published, key);
        i64 count = arrangement_key_count(rc, key);
        bool after = semi ? count > 0 : count == 0;
        if (before && after) append_bucket(out, transition_delta_bucket(left, key), 1);
        else if (!before && after) append_bucket(out, arrangement_bucket(lc, key), 1);
        else if (before && !after) {
            /* -L_before = -L_after + delta_L */
            append_bucket(out, arrangement_bucket(lc, key), -1);
            append_bucket(out, transition_delta_bucket(left, key), 1);
        }
        visibility_set(published, key, after && arrangement_bucket(lc, key) != NULL);
    }
    jz_hmap_drop(&affected);
    published->has_consumed = true;
    published->consumed[0] = frontier[0]; published->consumed[1] = frontier[1];
    groove__ivm__runtime__key_encoding__consolidate_deltas(out);
}

/* The recursive snapshot interpreter has no retained indexes. Produce two
 * temporary snapshot arrangements, then use the same read-only join kernel. */
jz_err groove__ivm__runtime__join__threshold_snapshot(const groove_RecordDeltas *left, const groove_RecordDeltas *right, const jz_str *left_keys, u32 nl,
    const jz_str *right_keys, u32 nr, groove_ValueComparison comparison, bool semi, groove_RecordDeltaVec *out) {
    groove_SubTick stamp = { 0, 0 };
    groove_AsOfArrangement l = { groove__ivm__runtime__join__ArrangementState__default(), { false, {0, 0}, groove_AsOfKind_SubTick } };
    groove_AsOfArrangement r = { groove__ivm__runtime__join__ArrangementState__default(), { false, {0, 0}, groove_AsOfKind_SubTick } };
    groove_JoinInput li = { left->deltas.ptr, left->deltas.len, NULL };
    groove_JoinInput ri = { right->deltas.ptr, right->deltas.len, NULL };
    jz_err e = groove__ivm__runtime__join__prepare_arrangement(&l, left->descriptor, left_keys, nl, comparison, li, stamp, groove_ArrangementUpdateMode_Replace);
    if (!JZ_IS_ERR(e)) e = groove__ivm__runtime__join__prepare_arrangement(&r, right->descriptor, right_keys, nr, comparison, ri, stamp, groove_ArrangementUpdateMode_Replace);
    if (!JZ_IS_ERR(e)) {
        VisibilityState published; visibility_default(&published);
        threshold_transition(&published, groove__ivm__runtime__join__ArrangementTransition__at(&l, stamp),
                             groove__ivm__runtime__join__ArrangementTransition__at(&r, stamp), true, semi, out);
        visibility_drop(&published);
    }
    groove__ivm__runtime__join__ArrangementState__drop(l.value);
    groove__ivm__runtime__join__ArrangementState__drop(r.value);
    return e;
}

void groove__ivm__runtime__join__ThresholdJoinState__commit_published_overlay(groove_ThresholdJoinState *s) { visibility_commit(&s->published); }

void groove__ivm__runtime__join__SemiJoinState__evaluate(groove_SemiJoinState *s, groove_ArrangementTransition l, groove_ArrangementTransition r, u32 mode, groove_RecordDeltaVec *out) {
    threshold_transition(&s->published, l, r, mode == groove_ArrangementUpdateMode_Replace, true, out);
}

void groove__ivm__runtime__join__AntiJoinState__evaluate(groove_AntiJoinState *s, groove_ArrangementTransition l, groove_ArrangementTransition r, u32 mode, groove_RecordDeltaVec *out) {
    threshold_transition(&s->published, l, r, mode == groove_ArrangementUpdateMode_Replace, false, out);
}

/* ---------- arrangement preparation ---------- */
static bool reuses_snapshot(const groove_AsOfArrangement *arrangement, const groove_RecordDeltas *snapshot, groove_SubTick sub_tick, u32 update_mode) {
    return update_mode == groove_ArrangementUpdateMode_Replace && arrangement->as_of.has &&
           groove_SubTick_cmp(arrangement->as_of.at, sub_tick) == 0 && snapshot && arrangement->value->snapshot == snapshot;
}

static jz_str subtick_debug(groove_SubTick s) {
    return jz_format("SubTick { tick: %llu, sub_tick: %llu }", (unsigned long long)s.tick, (unsigned long long)s.sub_tick);
}

static jz_err advance_arrangement(groove_AsOfArrangement *arrangement, const KeyedVec *deltas, groove_SubTick sub_tick, u32 update_mode,
                                  const groove_RecordDeltas *snapshot) {
    if (reuses_snapshot(arrangement, snapshot, sub_tick, update_mode)) return JZ_OK;
    if (update_mode == groove_ArrangementUpdateMode_Accumulate && arrangement->as_of.has && groove_SubTick_cmp(arrangement->as_of.at, sub_tick) == 0)
        return JZ_OK;
    /* Without exact snapshot identity, Replace callers intentionally rebuild
     * even when the stamp already matches this logical time. */
    bool replace_within_same_tick = update_mode == groove_ArrangementUpdateMode_Replace && arrangement->as_of.has && arrangement->as_of.at.tick == sub_tick.tick;
    if (!replace_within_same_tick && arrangement->as_of.has && groove_SubTick_cmp(arrangement->as_of.at, sub_tick) > 0)
        return groove__ivm__runtime__IvmRuntimeError__out_of_order(subtick_debug(arrangement->as_of.at), subtick_debug(sub_tick));
    arrangement_apply_update(arrangement->value, deltas, update_mode);
    if (update_mode == groove_ArrangementUpdateMode_Replace) arrangement->value->snapshot = snapshot;
    if (replace_within_same_tick) groove__ivm__runtime__state__AsOf__replace_as_of_at_least(&arrangement->as_of, sub_tick);
    else JZ_TRY(groove__ivm__runtime__state__AsOf__mark_forward_as_of(&arrangement->as_of, sub_tick));
    return JZ_OK;
}

jz_err groove__ivm__runtime__join__prepare_arrangement(groove_AsOfArrangement *state, groove_RecordDescriptor d, const jz_str *fields, u32 nfields,
    u32 comparison, groove_JoinInput input, groove_SubTick stamp, u32 mode) {
    if (reuses_snapshot(state, input.snapshot, stamp, mode)) return JZ_OK;
    KeyedVec keyed;
    JZ_TRY(keyed_join_deltas(d, fields, nfields, input.records, input.n, (groove_ValueComparison)comparison, &keyed));
    jz_err e = advance_arrangement(state, &keyed, stamp, mode, input.snapshot);
    keyed_drop(&keyed);
    return e;
}

/* ---------- keys ---------- */
static jz_err encode_join_key_part(jz_bytes *key, const groove_Value *value, groove_ValueComparison comparison) {
    if (comparison == groove_ValueComparison_Policy) {
        __int128 v;
        if (groove__ivm__runtime__key_encoding__integer_value(value, &v)) {
            jz_bytes_push_u8(key, 0xfe);
            u8 be[16];
            unsigned __int128 u = (unsigned __int128)v;
            for (int i = 15; i >= 0; i--) { be[i] = (u8)u; u >>= 8; }
            jz_bytes_push(key, be, 16);
            return JZ_OK;
        }
        if (value->tag == groove_Value_Nullable && value->v.nullable) return encode_join_key_part(key, value->v.nullable, comparison);
    }
    return groove__ivm__runtime__key_encoding__encode_key_part(key, value);
}

static jz_err scalar_join_field_indices(groove_RecordDescriptor d, const jz_str *fields, u32 nfields, bool *some, groove_U32Vec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    *some = false;
    jz_vec_reserve(out, nfields, sizeof(u32));
    for (u32 i = 0; i < nfields; i++) {
        u32 field_idx;
        if (!groove__ivm__runtime__record_projection__resolve_field_name(d, fields[i].ptr, fields[i].len, &field_idx)) {
            jz_vec_drop(out, sizeof(u32), NULL);
            return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &fields[i]);
        }
        const groove_DescriptorField *f = groove__records__RecordDescriptor__field(d, field_idx);
        if (!f) { jz_vec_drop(out, sizeof(u32), NULL); return groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, field_idx); }
        if (groove__ivm__runtime__key_encoding__expands_to_key_parts(&f->value_type)) { jz_vec_drop(out, sizeof(u32), NULL); return JZ_OK; }
        JZ_VEC_PUSH(*out, field_idx);
    }
    *some = true;
    return JZ_OK;
}

static jz_err keyed_join_deltas(groove_RecordDescriptor d, const jz_str *fields, u32 nfields, const groove_RecordDelta *deltas, u32 n,
                                groove_ValueComparison comparison, KeyedVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    bool some; groove_U32Vec field_indices;
    JZ_TRY(scalar_join_field_indices(d, fields, nfields, &some, &field_indices));
    if (some) {
        jz_vec_reserve(out, n, sizeof(KeyedRecordDelta));
        /* Reuse the temporary encoder buffer instead of allocating per row. */
        jz_bytes key = {0};
        for (u32 i = 0; i < n; i++) {
            key.len = 0;
            groove_BorrowedRecord rec = groove_RecordDelta_borrowed(&deltas[i], d);
            for (u32 j = 0; j < field_indices.len; j++) {
                groove_Value value;
                jz_err e = groove__records__BorrowedRecord__get_idx(&rec, field_indices.ptr[j], &value);
                if (!JZ_IS_ERR(e)) { e = encode_join_key_part(&key, &value, comparison); groove_Value_drop(&value); }
                if (JZ_IS_ERR(e)) { jz_bytes_drop(&key); jz_vec_drop(&field_indices, sizeof(u32), NULL); keyed_drop(out); return e; }
            }
            KeyedRecordDelta k = { &deltas[i], jz_bytes_from(key.ptr, key.len) };
            JZ_VEC_PUSH(*out, k);
        }
        jz_bytes_drop(&key);
        jz_vec_drop(&field_indices, sizeof(u32), NULL);
        return JZ_OK;
    }
    for (u32 i = 0; i < n; i++) {
        groove_BytesVec keys;
        jz_err e = groove__ivm__runtime__join__join_keys(d, jz_bytes_slice(&deltas[i].record), fields, nfields, comparison, &keys);
        if (JZ_IS_ERR(e)) { keyed_drop(out); return e; }
        JZ_VEC_FOR(jz_bytes, k, keys) { KeyedRecordDelta kd = { &deltas[i], *k }; JZ_VEC_PUSH(*out, kd); }
        jz_vec_drop(&keys, sizeof(jz_bytes), NULL);
    }
    return JZ_OK;
}

typedef struct JoinKeysCtx { groove_RecordDescriptor d; jz_slice record; const jz_str *fields; groove_ValueComparison comparison; } JoinKeysCtx;
static jz_err join_field_value(u32 i, groove_Value *out, void *p) {
    JoinKeysCtx *c = p;
    u32 field_idx;
    if (!groove__ivm__runtime__record_projection__resolve_field_name(c->d, c->fields[i].ptr, c->fields[i].len, &field_idx))
        return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &c->fields[i]);
    groove_BorrowedRecord rec = { c->record, c->d };
    return groove__records__BorrowedRecord__get_idx(&rec, field_idx, out);
}
static jz_err join_encode(jz_bytes *key, const groove_Value *v, void *p) {
    JoinKeysCtx *c = p;
    return encode_join_key_part(key, v, c->comparison);
}

jz_err groove__ivm__runtime__join__join_keys(groove_RecordDescriptor d, jz_slice record, const jz_str *fields, u32 nfields, groove_ValueComparison comparison, groove_BytesVec *out) {
    JoinKeysCtx ctx = { d, record, fields, comparison };
    jz_bytes prefix = {0};
    return groove__ivm__runtime__key_encoding__composite_keys(prefix, nfields, join_field_value, &ctx, join_encode, &ctx, out);
}

jz_err groove__ivm__runtime__join__create_join_record(groove_RecordDescriptor l, jz_slice lr, groove_RecordDescriptor r, jz_slice rr, groove_RecordDescriptor o, jz_bytes *out) {
    groove_UsizePairVec mapping;
    JZ_TRY(groove__ivm__runtime__join__join_output_mapping(l, r, o, &mapping));
    groove_RecordDescriptor descs[2] = { l, r };
    jz_slice raws[2] = { lr, rr };
    jz_err e = groove__records__RecordDescriptor__project_record_raw(o, descs, raws, 2, mapping.ptr, mapping.len, out);
    jz_vec_drop(&mapping, sizeof(groove_UsizePair), NULL);
    return e;
}

jz_err groove__ivm__runtime__join__join_output_mapping(groove_RecordDescriptor l, groove_RecordDescriptor r, groove_RecordDescriptor o, groove_UsizePairVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(o, &n);
    for (u32 i = 0; i < n; i++) {
        jz_err e;
        if (!fields[i].has_name) { e = GROOVE_IVM_ERR_C(GROOVE_IVM_GraphFieldNotFound, "<unnamed>"); goto fail; }
        const jz_str *name = &fields[i].name;
        groove_FieldRef ref; memset(&ref, 0, sizeof ref);
        ref.tag = groove_FieldRef_StoredName;
        groove_UsizePair pair;
        if (name->len >= 5 && memcmp(name->ptr, "left.", 5) == 0) {
            ref.name.ptr = name->ptr + 5; ref.name.len = ref.name.cap = name->len - 5;
            e = groove__ivm__runtime__record_projection__resolve_field_ref(l, &ref, &pair.b);
            pair.a = 0;
        } else if (name->len >= 6 && memcmp(name->ptr, "right.", 6) == 0) {
            ref.name.ptr = name->ptr + 6; ref.name.len = ref.name.cap = name->len - 6;
            e = groove__ivm__runtime__record_projection__resolve_field_ref(r, &ref, &pair.b);
            pair.a = 1;
        } else {
            e = GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, name);
        }
        if (JZ_IS_ERR(e)) goto fail;
        JZ_VEC_PUSH(*out, pair);
        continue;
    fail:
        jz_vec_drop(out, sizeof(groove_UsizePair), NULL);
        return e;
    }
    return JZ_OK;
}
