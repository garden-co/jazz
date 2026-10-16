/* groove::ivm::runtime::counted_map: ordered weighted map with
 * order-statistic ranks. One type-erased copy for every K. */
#include "mod.h"

#define MAX_CHUNK 256u
#define MIN_CHUNK (MAX_CHUNK / 4u)

static inline u32 entry_size(const groove_CountedMap *m) { return m->kd->size + (u32)sizeof(i64); }
static inline u8 *entry_at(const groove_CountedMap *m, const groove_CountedMapChunk *c, u32 i) { return (u8 *)c->entries.ptr + (usize)i * entry_size(m); }
static inline i64 *entry_weight(const groove_CountedMap *m, const groove_CountedMapChunk *c, u32 i) { return (i64 *)(entry_at(m, c, i) + m->kd->size); }

void groove__ivm__runtime__counted_map__CountedMap__default(groove_CountedMap *m, const groove_CountedMapKeyDesc *kd) {
    memset(m, 0, sizeof(*m));
    m->kd = kd;
    u32 zero = 0;
    JZ_VEC_PUSH(m->positive_tree, zero);
}

void groove_CountedMap_drop(groove_CountedMap *m) {
    for (u32 c = 0; c < m->chunks.len; c++) {
        groove_CountedMapChunk *chunk = &m->chunks.ptr[c];
        if (m->kd->drop)
            for (u32 i = 0; i < chunk->entries.len; i++) m->kd->drop(entry_at(m, chunk, i));
        jz_vec_drop(&chunk->entries, entry_size(m), NULL);
    }
    jz_vec_drop(&m->chunks, sizeof(groove_CountedMapChunk), NULL);
    jz_vec_drop(&m->positive_tree, sizeof(u32), NULL);
}

void groove_CountedMap_clone(groove_CountedMap *dst, const groove_CountedMap *src) {
    memset(dst, 0, sizeof(*dst));
    dst->kd = src->kd;
    dst->len = src->len;
    jz_vec_clone(&dst->positive_tree, &src->positive_tree, sizeof(u32), NULL);
    jz_vec_reserve(&dst->chunks, src->chunks.len, sizeof(groove_CountedMapChunk));
    for (u32 c = 0; c < src->chunks.len; c++) {
        const groove_CountedMapChunk *sc = &src->chunks.ptr[c];
        groove_CountedMapChunk nc = {{0}, sc->positive};
        jz_vec_reserve(&nc.entries, sc->entries.len, entry_size(src));
        for (u32 i = 0; i < sc->entries.len; i++) {
            u8 *slot = jz_vec_push_slot(&nc.entries, entry_size(src));
            src->kd->clone(slot, entry_at(src, sc, i));
            *(i64 *)(slot + src->kd->size) = *entry_weight(src, sc, i);
        }
        JZ_VEC_PUSH(dst->chunks, nc);
    }
}

static void rebuild_positive_tree(groove_CountedMap *m) {
    u32 count = m->chunks.len;
    m->positive_tree.len = 0;
    jz_vec_reserve(&m->positive_tree, count + 1, sizeof(u32));
    for (u32 i = 0; i <= count; i++) m->positive_tree.ptr[i] = 0;
    m->positive_tree.len = count + 1;
    for (u32 index = 1; index <= count; index++) {
        m->positive_tree.ptr[index] += m->chunks.ptr[index - 1].positive;
        u32 parent = index + (index & (0u - index));
        if (parent <= count) m->positive_tree.ptr[parent] += m->positive_tree.ptr[index];
    }
}

static void adjust_positive(groove_CountedMap *m, u32 chunk_index, bool gained, bool lost) {
    if (gained == lost) return;
    u32 index = chunk_index + 1;
    while (index < m->positive_tree.len) {
        if (gained) m->positive_tree.ptr[index] += 1;
        else m->positive_tree.ptr[index] -= 1;
        index += index & (0u - index);
    }
}

static u32 positive_in_chunks_before(const groove_CountedMap *m, u32 chunk_index) {
    u32 total = 0, index = chunk_index;
    while (index > 0) {
        total += m->positive_tree.ptr[index];
        index &= index - 1;
    }
    return total;
}

/* The chunk that holds `key` or would receive it. */
static u32 chunk_for(const groove_CountedMap *m, const void *key) {
    u32 lo = 0, hi = m->chunks.len;
    while (lo < hi) {
        u32 mid = lo + (hi - lo) / 2;
        const groove_CountedMapChunk *chunk = &m->chunks.ptr[mid];
        if (chunk->entries.len == 0) jz_panic("chunks are never empty");
        if (m->kd->cmp(entry_at(m, chunk, chunk->entries.len - 1), key, NULL) < 0) lo = mid + 1;
        else hi = mid;
    }
    u32 last = m->chunks.len ? m->chunks.len - 1 : 0;
    return lo < last ? lo : last;
}

/* binary_search_by over a chunk; returns true when found, idx = position or insertion point */
static bool chunk_search(const groove_CountedMap *m, const groove_CountedMapChunk *chunk, const void *key, u32 *idx) {
    u32 lo = 0, hi = chunk->entries.len;
    while (lo < hi) {
        u32 mid = lo + (hi - lo) / 2;
        int c = m->kd->cmp(entry_at(m, chunk, mid), key, NULL);
        if (c == 0) { *idx = mid; return true; }
        if (c < 0) lo = mid + 1; else hi = mid;
    }
    *idx = lo;
    return false;
}

const i64 *groove__ivm__runtime__counted_map__CountedMap__get(const groove_CountedMap *m, const void *key) {
    u32 ci = chunk_for(m, key);
    if (ci >= m->chunks.len) return NULL;
    const groove_CountedMapChunk *chunk = &m->chunks.ptr[ci];
    u32 idx;
    if (!chunk_search(m, chunk, key, &idx)) return NULL;
    return entry_weight(m, chunk, idx);
}

static u32 count_positive(const groove_CountedMap *m, const groove_CountedMapChunk *c, u32 from, u32 to) {
    u32 n = 0;
    for (u32 i = from; i < to; i++) n += *entry_weight(m, c, i) > 0;
    return n;
}

/* chunk.entries.split_off(at) into a new chunk inserted at `insert_at` */
static void split_chunk(groove_CountedMap *m, u32 chunk_index, u32 insert_at) {
    groove_CountedMapChunk *chunk = &m->chunks.ptr[chunk_index];
    u32 at = chunk->entries.len / 2;
    u32 es = entry_size(m);
    groove_CountedMapChunk tail = {{0}, 0};
    jz_vec_extend(&tail.entries, entry_at(m, chunk, at), chunk->entries.len - at, es);
    chunk->entries.len = at;
    tail.positive = count_positive(m, &tail, 0, tail.entries.len);
    chunk->positive -= tail.positive;
    jz_vec_insert(&m->chunks, insert_at, &tail, sizeof(groove_CountedMapChunk));
}

bool groove__ivm__runtime__counted_map__CountedMap__insert(groove_CountedMap *m, const void *key, i64 weight, i64 *old) {
    u32 es = entry_size(m);
    if (m->chunks.len == 0) {
        groove_CountedMapChunk chunk = {{0}, weight > 0 ? 1u : 0u};
        u8 *slot = jz_vec_push_slot(&chunk.entries, es);
        memcpy(slot, key, m->kd->size);
        memcpy(slot + m->kd->size, &weight, sizeof(i64));
        JZ_VEC_PUSH(m->chunks, chunk);
        m->len = 1;
        rebuild_positive_tree(m);
        return false;
    }
    u32 chunk_index = chunk_for(m, key);
    groove_CountedMapChunk *chunk = &m->chunks.ptr[chunk_index];
    u32 index;
    if (chunk_search(m, chunk, key, &index)) {
        i64 *w = entry_weight(m, chunk, index);
        i64 prev = *w;
        *w = weight;
        if (m->kd->drop) m->kd->drop((void *)key); /* the passed key is not stored */
        chunk->positive = chunk->positive - (prev > 0) + (weight > 0);
        adjust_positive(m, chunk_index, weight > 0, prev > 0);
        if (old) *old = prev;
        return true;
    }
    u8 tmp[512];
    u8 *buf = es <= sizeof(tmp) ? tmp : jz_alloc(es);
    memcpy(buf, key, m->kd->size);
    memcpy(buf + m->kd->size, &weight, sizeof(i64));
    jz_vec_insert(&chunk->entries, index, buf, es);
    if (buf != tmp) jz_free(buf);
    chunk->positive += weight > 0;
    m->len += 1;
    if (chunk->entries.len > MAX_CHUNK) {
        split_chunk(m, chunk_index, chunk_index + 1);
        rebuild_positive_tree(m);
    } else {
        adjust_positive(m, chunk_index, weight > 0, false);
    }
    return false;
}

/* Folds an undersized chunk into its next neighbour (or its previous one
 * at the end), splitting the result again if it overflows. */
static void merge_into_neighbour(groove_CountedMap *m, u32 chunk_index) {
    u32 left = chunk_index + 1 < m->chunks.len ? chunk_index : chunk_index - 1;
    groove_CountedMapChunk right;
    jz_vec_remove(&m->chunks, left + 1, &right, sizeof(groove_CountedMapChunk));
    groove_CountedMapChunk *merged = &m->chunks.ptr[left];
    jz_vec_extend(&merged->entries, right.entries.ptr, right.entries.len, entry_size(m));
    merged->positive += right.positive;
    jz_vec_drop(&right.entries, entry_size(m), NULL);
    if (merged->entries.len > MAX_CHUNK) split_chunk(m, left, left + 1);
}

bool groove__ivm__runtime__counted_map__CountedMap__remove(groove_CountedMap *m, const void *key, i64 *removed) {
    if (m->chunks.len == 0) return false;
    u32 chunk_index = chunk_for(m, key);
    groove_CountedMapChunk *chunk = &m->chunks.ptr[chunk_index];
    u32 index;
    if (!chunk_search(m, chunk, key, &index)) return false;
    u32 es = entry_size(m);
    u8 tmp[512];
    u8 *buf = es <= sizeof(tmp) ? tmp : jz_alloc(es);
    jz_vec_remove(&chunk->entries, index, buf, es);
    i64 weight;
    memcpy(&weight, buf + m->kd->size, sizeof(i64));
    if (m->kd->drop) m->kd->drop(buf);
    if (buf != tmp) jz_free(buf);
    chunk->positive -= weight > 0;
    m->len -= 1;
    if (chunk->entries.len == 0) {
        groove_CountedMapChunk gone;
        jz_vec_remove(&m->chunks, chunk_index, &gone, sizeof(gone));
        jz_vec_drop(&gone.entries, es, NULL);
        rebuild_positive_tree(m);
    } else if (chunk->entries.len < MIN_CHUNK && m->chunks.len > 1) {
        merge_into_neighbour(m, chunk_index);
        rebuild_positive_tree(m);
    } else {
        adjust_positive(m, chunk_index, false, weight > 0);
    }
    if (removed) *removed = weight;
    return true;
}

/* Entries with a positive weight whose key is strictly below `key`. */
u32 groove__ivm__runtime__counted_map__CountedMap__positive_before(const groove_CountedMap *m, const void *key) {
    if (m->chunks.len == 0) return 0;
    u32 chunk_index = chunk_for(m, key);
    u32 before = positive_in_chunks_before(m, chunk_index);
    const groove_CountedMapChunk *chunk = &m->chunks.ptr[chunk_index];
    u32 lo = 0, hi = chunk->entries.len;
    while (lo < hi) {
        u32 mid = lo + (hi - lo) / 2;
        if (m->kd->cmp(entry_at(m, chunk, mid), key, NULL) < 0) lo = mid + 1; else hi = mid;
    }
    u32 within = lo;
    return before + (chunk->positive == chunk->entries.len ? within : count_positive(m, chunk, 0, within));
}

void groove__ivm__runtime__counted_map__CountedMap__iter(const groove_CountedMap *m, groove_CountedMapIter *it) {
    it->m = m; it->chunk = 0; it->idx = 0;
}

bool groove__ivm__runtime__counted_map__Iter__next(groove_CountedMapIter *it, const void **key, const i64 **weight) {
    for (;;) {
        if (it->chunk >= it->m->chunks.len) return false;
        const groove_CountedMapChunk *c = &it->m->chunks.ptr[it->chunk];
        if (it->idx < c->entries.len) {
            *key = entry_at(it->m, c, it->idx);
            *weight = entry_weight(it->m, c, it->idx);
            it->idx++;
            return true;
        }
        it->chunk++;
        it->idx = 0;
    }
}

void groove__ivm__runtime__counted_map__CountedMap__from_iter(groove_CountedMap *m, const groove_CountedMapKeyDesc *kd, void *pairs, u32 n) {
    groove__ivm__runtime__counted_map__CountedMap__default(m, kd);
    u32 es = kd->size + (u32)sizeof(i64);
    for (u32 i = 0; i < n; i++) {
        u8 *p = (u8 *)pairs + (usize)i * es;
        i64 w;
        memcpy(&w, p + kd->size, sizeof(i64));
        groove__ivm__runtime__counted_map__CountedMap__insert(m, p, w, NULL);
    }
}
