/* groove::ivm::runtime::evaluation_session: owned work and request state for
 * interruptible evaluation. Request futures complete synchronously in C: a
 * pending request records what to run and is run when polled. */
#include "mod.h"

/* ---------- StorageRequestKey / EvaluationRequestKey (derive Clone, Ord) ---------- */
static void str_vec_pair_clone(void *dst, const void *src) {
    groove_IndexPrefixPair *d = dst; const groove_IndexPrefixPair *s = src;
    d->index = jz_str_clone(&s->index);
    d->prefix = jz_bytes_clone(&s->prefix);
}
static void str_vec_pair_drop(void *p) {
    groove_IndexPrefixPair *x = p;
    jz_str_drop(&x->index);
    jz_bytes_drop(&x->prefix);
}

void groove_StorageRequestKey_clone(groove_StorageRequestKey *d, const groove_StorageRequestKey *s) {
    d->tag = s->tag;
    d->family = jz_str_clone(&s->family);
    d->index = jz_str_clone(&s->index);
    d->a = jz_bytes_clone(&s->a);
    d->b = jz_bytes_clone(&s->b);
    d->max_items = s->max_items;
    d->reversed = s->reversed;
    jz_vec_clone(&d->intersections, &s->intersections, sizeof(groove_IndexPrefixPair), str_vec_pair_clone);
    d->candidate_table = jz_str_clone(&s->candidate_table);
    d->candidate_index = jz_str_clone(&s->candidate_index);
    d->candidate_prefix = jz_bytes_clone(&s->candidate_prefix);
    d->source_column = jz_str_clone(&s->source_column);
    d->candidate_column = jz_str_clone(&s->candidate_column);
}

void groove_StorageRequestKey_drop(groove_StorageRequestKey *k) {
    jz_str_drop(&k->family);
    jz_str_drop(&k->index);
    jz_bytes_drop(&k->a);
    jz_bytes_drop(&k->b);
    jz_vec_drop(&k->intersections, sizeof(groove_IndexPrefixPair), str_vec_pair_drop);
    jz_str_drop(&k->candidate_table);
    jz_str_drop(&k->candidate_index);
    jz_bytes_drop(&k->candidate_prefix);
    jz_str_drop(&k->source_column);
    jz_str_drop(&k->candidate_column);
}

#define CMP_RET(x) do { int _c = (x); if (_c) return _c; } while (0)

int groove_StorageRequestKey_cmp(const groove_StorageRequestKey *a, const groove_StorageRequestKey *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    switch (a->tag) {
    case groove_SRK_Get:
        CMP_RET(jz_str_cmp(&a->family, &b->family));
        return jz_bytes_cmp(&a->a, &b->a);
    case groove_SRK_ScanRange:
        CMP_RET(jz_str_cmp(&a->family, &b->family));
        CMP_RET(jz_bytes_cmp(&a->a, &b->a));
        return jz_bytes_cmp(&a->b, &b->b);
    case groove_SRK_ScanPrefix:
        CMP_RET(jz_str_cmp(&a->family, &b->family));
        return jz_bytes_cmp(&a->a, &b->a);
    case groove_SRK_ScanPrefixLimit:
        CMP_RET(jz_str_cmp(&a->family, &b->family));
        CMP_RET(jz_bytes_cmp(&a->a, &b->a));
        if (a->max_items != b->max_items) return a->max_items < b->max_items ? -1 : 1;
        return (int)a->reversed - (int)b->reversed;
    case groove_SRK_IndexedRowsPrefix:
        CMP_RET(jz_str_cmp(&a->family, &b->family));
        CMP_RET(jz_str_cmp(&a->index, &b->index));
        return jz_bytes_cmp(&a->a, &b->a);
    case groove_SRK_IndexedRowsPrefixLimit:
        CMP_RET(jz_str_cmp(&a->family, &b->family));
        CMP_RET(jz_str_cmp(&a->index, &b->index));
        CMP_RET(jz_bytes_cmp(&a->a, &b->a));
        if (a->max_items != b->max_items) return a->max_items < b->max_items ? -1 : 1;
        return (int)a->reversed - (int)b->reversed;
    case groove_SRK_IndexedRowsIntersection: {
        CMP_RET(jz_str_cmp(&a->family, &b->family));
        CMP_RET(jz_str_cmp(&a->index, &b->index));
        CMP_RET(jz_bytes_cmp(&a->a, &b->a));
        u32 n = a->intersections.len < b->intersections.len ? a->intersections.len : b->intersections.len;
        for (u32 i = 0; i < n; i++) {
            CMP_RET(jz_str_cmp(&a->intersections.ptr[i].index, &b->intersections.ptr[i].index));
            CMP_RET(jz_bytes_cmp(&a->intersections.ptr[i].prefix, &b->intersections.ptr[i].prefix));
        }
        return a->intersections.len < b->intersections.len ? -1 : a->intersections.len > b->intersections.len;
    }
    case groove_SRK_IndexedRowsCandidateFilter:
        CMP_RET(jz_str_cmp(&a->family, &b->family));
        CMP_RET(jz_str_cmp(&a->index, &b->index));
        CMP_RET(jz_bytes_cmp(&a->a, &b->a));
        CMP_RET(jz_str_cmp(&a->candidate_table, &b->candidate_table));
        CMP_RET(jz_str_cmp(&a->candidate_index, &b->candidate_index));
        CMP_RET(jz_bytes_cmp(&a->candidate_prefix, &b->candidate_prefix));
        CMP_RET(jz_str_cmp(&a->source_column, &b->source_column));
        return jz_str_cmp(&a->candidate_column, &b->candidate_column);
    case groove_SRK_IndexedRowsRange:
        CMP_RET(jz_str_cmp(&a->family, &b->family));
        CMP_RET(jz_str_cmp(&a->index, &b->index));
        CMP_RET(jz_bytes_cmp(&a->a, &b->a));
        return jz_bytes_cmp(&a->b, &b->b);
    }
    return 0;
}

static int request_key_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const groove_EvaluationRequestKey *a = pa, *b = pb;
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    if (a->tag == groove_ERK_Storage) return groove_StorageRequestKey_cmp(&a->storage, &b->storage);
    return groove_ChunkRequest_cmp(&a->chunk, &b->chunk);
}
static void request_key_drop(void *p) {
    groove_EvaluationRequestKey *k = p;
    if (k->tag == groove_ERK_Storage) groove_StorageRequestKey_drop(&k->storage);
    else groove_ChunkRequest_drop(&k->chunk);
}
static void request_key_clone(groove_EvaluationRequestKey *d, const groove_EvaluationRequestKey *s) {
    memset(d, 0, sizeof *d);
    d->tag = s->tag;
    if (s->tag == groove_ERK_Storage) groove_StorageRequestKey_clone(&d->storage, &s->storage);
    else groove_ChunkRequest_clone(&d->chunk, &s->chunk);
}

static void loaded_chunk_drop(groove_LoadedChunk *c) {
    if (c->tag == groove_LCB_Direct) jz_bytes_drop(&c->direct);
    else groove_ChunkLease_drop(&c->lease);
}
static void request_output_drop(void *p) {
    groove_EvaluationRequestOutput *o = p;
    if (o->tag == groove_ERO_Storage) {
        if (o->storage_tag == groove_SRO_Value) { if (o->has_value) jz_bytes_drop(&o->value); }
        else groove_KvPairVec_drop(&o->rows);
    } else {
        loaded_chunk_drop(&o->chunk);
    }
}

static void node_set_drop(void *p) { jz_btree_drop((jz_btree *)p); }
static const jz_btree_desc NODE_BTREE_SET = { sizeof(groove_NodeId), 0, jz_cmp_u64, NULL, NULL };
const jz_btree_desc groove_ivm_rt_REQUEST_OUTPUT_MAP = { sizeof(groove_EvaluationRequestKey), sizeof(groove_EvaluationRequestOutput), request_key_cmp, request_key_drop, request_output_drop };
const jz_btree_desc groove_ivm_rt_REQUEST_KEY_SET = { sizeof(groove_EvaluationRequestKey), 0, request_key_cmp, request_key_drop, NULL };
static const jz_btree_desc CHUNK_OWNERS_MAP = { sizeof(groove_EvaluationRequestKey), sizeof(jz_btree), request_key_cmp, request_key_drop, node_set_drop };

/* ---------- LoadedChunk ---------- */
const jz_bytes *groove__ivm__runtime__evaluation_session__LoadedChunk__bytes(const groove_LoadedChunk *c) {
    if (c->tag == groove_LCB_Direct) return &c->direct;
    return &c->lease.bytes;
}

bool groove__ivm__runtime__evaluation_session__LoadedChunk__verifies_object_hash(const groove_LoadedChunk *c, const u8 expected[32]) {
    switch (c->tag) {
    case groove_LCB_Direct: return false;
    default: return groove__chunks__ChunkLease__verifies_object_hash(&c->lease, expected);
    }
}

/* ---------- EvaluationInputs ---------- */
void groove__ivm__runtime__evaluation_session__EvaluationInputs__default(groove_EvaluationInputs *in) {
    jz_btree_init(&in->loaded, &groove_ivm_rt_REQUEST_OUTPUT_MAP);
    jz_btree_init(&in->missing, &groove_ivm_rt_REQUEST_KEY_SET);
    jz_btree_init(&in->chunk_owners, &CHUNK_OWNERS_MAP);
    in->has_chunk_scope = false;
    in->chunk_scope = 0;
}

void groove_EvaluationInputs_drop(groove_EvaluationInputs *in) {
    jz_btree_drop(&in->loaded);
    jz_btree_drop(&in->missing);
    jz_btree_drop(&in->chunk_owners);
}

void groove__ivm__runtime__evaluation_session__EvaluationInputs__set_chunk_scope(groove_EvaluationInputs *in, bool has, groove_NodeId scope) {
    in->has_chunk_scope = has;
    in->chunk_scope = scope;
}

typedef struct ReleaseCtx { groove_EvaluationInputs *in; groove_NodeId owner; } ReleaseCtx;
static bool release_keep_loaded(const void *k, void *v, void *ctx) {
    (void)v;
    ReleaseCtx *c = ctx;
    const groove_EvaluationRequestKey *key = k;
    if (key->tag != groove_ERK_Chunk) return true;
    jz_btree *owners = jz_btree_get(&c->in->chunk_owners, key);
    if (!owners) return false;
    jz_btree_remove(owners, &c->owner, NULL, NULL);
    return owners->len != 0;
}
static bool owners_nonempty(const void *k, void *v, void *ctx) {
    (void)k; (void)ctx;
    return ((jz_btree *)v)->len != 0;
}

/* Release only chunks no other evaluation path has claimed. */
void groove__ivm__runtime__evaluation_session__EvaluationInputs__release_chunks_owned_by(groove_EvaluationInputs *in, groove_NodeId owner) {
    ReleaseCtx c = { in, owner };
    jz_btree_retain(&in->loaded, release_keep_loaded, &c);
    jz_btree_retain(&in->chunk_owners, owners_nonempty, NULL);
}

void groove__ivm__runtime__evaluation_session__EvaluationInputs__take_missing_chunks(groove_EvaluationInputs *in, groove_ChunkRequestVec *out) {
    jz_btree missing;
    groove__ivm__runtime__evaluation_session__EvaluationInputs__take_missing(in, &missing);
    out->ptr = NULL; out->len = 0; out->cap = 0;
    groove_EvaluationRequestKey key;
    while (jz_btree_pop_first(&missing, &key, NULL)) {
        if (key.tag == groove_ERK_Chunk) JZ_VEC_PUSH(*out, key.chunk);
        else groove_StorageRequestKey_drop(&key.storage);
    }
    jz_btree_drop(&missing);
}

static void install_loaded_chunk(groove_EvaluationInputs *in, groove_ChunkRequest request, groove_LoadedChunk chunk) {
    groove_EvaluationRequestKey key; memset(&key, 0, sizeof key);
    key.tag = groove_ERK_Chunk;
    groove_ChunkRequest_clone(&key.chunk, &request);
    groove_ChunkRequest_drop(&request);
    groove_EvaluationRequestOutput out; memset(&out, 0, sizeof out);
    out.tag = groove_ERO_Chunk;
    out.chunk = chunk;
    jz_btree_insert(&in->loaded, &key, &out, NULL);
}

void groove__ivm__runtime__evaluation_session__EvaluationInputs__install_chunk(groove_EvaluationInputs *in, groove_ChunkRequest request, jz_bytes bytes) {
    groove_LoadedChunk c; memset(&c, 0, sizeof c);
    c.tag = groove_LCB_Direct; c.direct = bytes;
    install_loaded_chunk(in, request, c);
}

void groove__ivm__runtime__evaluation_session__EvaluationInputs__install_chunk_from_provider(groove_EvaluationInputs *in, groove_ChunkRequest request, groove_ChunkLease bytes) {
    groove_LoadedChunk c; memset(&c, 0, sizeof c);
    c.tag = groove_LCB_Leased; c.lease = bytes;
    install_loaded_chunk(in, request, c);
}

jz_err groove__ivm__runtime__evaluation_session__EvaluationInputs__chunk(groove_EvaluationInputs *in, groove_ChunkRequest request, const jz_bytes **out) {
    const groove_LoadedChunk *c;
    JZ_TRY(groove__ivm__runtime__evaluation_session__EvaluationInputs__loaded_chunk(in, request, &c));
    *out = groove__ivm__runtime__evaluation_session__LoadedChunk__bytes(c);
    return JZ_OK;
}

jz_err groove__ivm__runtime__evaluation_session__EvaluationInputs__loaded_chunk(groove_EvaluationInputs *in, groove_ChunkRequest request, const groove_LoadedChunk **out) {
    groove_EvaluationRequestKey key; memset(&key, 0, sizeof key);
    key.tag = groove_ERK_Chunk;
    key.chunk = request;
    if (in->has_chunk_scope) {
        bool inserted;
        jz_btree *owners = jz_btree_get(&in->chunk_owners, &key);
        if (!owners) {
            groove_EvaluationRequestKey k2; request_key_clone(&k2, &key);
            owners = jz_btree_entry(&in->chunk_owners, &k2, &inserted);
            jz_btree_init(owners, &NODE_BTREE_SET);
        }
        jz_btree_insert(owners, &in->chunk_scope, NULL, NULL);
    }
    groove_EvaluationRequestOutput *loaded = jz_btree_get(&in->loaded, &key);
    if (!loaded) {
        if (jz_btree_insert(&in->missing, &key, NULL, NULL)) request_key_drop(&key);
        return GROOVE_IVM_ERR(GROOVE_IVM_EvaluationBlocked);
    }
    request_key_drop(&key);
    if (loaded->tag == groove_ERO_Chunk) { *out = &loaded->chunk; return JZ_OK; }
    return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
}

jz_err groove__ivm__runtime__evaluation_session__EvaluationInputs__rows(groove_EvaluationInputs *in, groove_StorageRequestKey skey, const groove_KvPairVec **out) {
    groove_EvaluationRequestKey key; memset(&key, 0, sizeof key);
    key.tag = groove_ERK_Storage;
    key.storage = skey;
    groove_EvaluationRequestOutput *loaded = jz_btree_get(&in->loaded, &key);
    if (!loaded) {
        if (jz_btree_insert(&in->missing, &key, NULL, NULL)) request_key_drop(&key);
        return GROOVE_IVM_ERR(GROOVE_IVM_EvaluationBlocked);
    }
    request_key_drop(&key);
    if (loaded->tag == groove_ERO_Storage && loaded->storage_tag == groove_SRO_Rows) { *out = &loaded->rows; return JZ_OK; }
    if (loaded->tag == groove_ERO_Storage) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
}

/* self.loaded.extend(ready) */
void groove__ivm__runtime__evaluation_session__EvaluationInputs__install(groove_EvaluationInputs *in, jz_btree *ready) {
    groove_EvaluationRequestKey key; groove_EvaluationRequestOutput out;
    while (jz_btree_pop_first(ready, &key, &out)) {
        groove_EvaluationRequestOutput old;
        if (jz_btree_insert(&in->loaded, &key, &out, &old)) request_output_drop(&old);
    }
    jz_btree_drop(ready);
}

void groove__ivm__runtime__evaluation_session__EvaluationInputs__take_missing(groove_EvaluationInputs *in, jz_btree *out) {
    *out = in->missing;
    jz_btree_init(&in->missing, &groove_ivm_rt_REQUEST_KEY_SET);
}

void groove__ivm__runtime__evaluation_session__EvaluationInputs__hand_off_chunks(groove_EvaluationInputs *in, groove_EvaluationInputs *successor) {
    jz_btree taken = in->loaded;
    jz_btree_init(&in->loaded, &groove_ivm_rt_REQUEST_OUTPUT_MAP);
    groove_EvaluationRequestKey key; groove_EvaluationRequestOutput out;
    while (jz_btree_pop_first(&taken, &key, &out)) {
        if (key.tag == groove_ERK_Chunk) {
            if (jz_btree_contains(&successor->loaded, &key)) { request_key_drop(&key); request_output_drop(&out); }
            else jz_btree_insert(&successor->loaded, &key, &out, NULL);
        } else {
            jz_btree_insert(&in->loaded, &key, &out, NULL);
        }
    }
    jz_btree_drop(&taken);
}

/* ---------- EvaluationRequests ---------- */
/* PendingRequest: the deferred work behind a future plus its retry policy. */
typedef struct PendingRequest {
    bool eager_retry_safe;
    groove_Storage storage;
    bool has_chunks; groove_OwnedChunkProvider chunks;
    const groove_DatabaseSchema *schema;
} PendingRequest;
typedef struct ReadyResult { bool ok; groove_EvaluationRequestOutput output; groove_EvaluationRequestFailure failure; } ReadyResult;
static void pending_drop(void *p) {
    PendingRequest *r = p;
    if (r->has_chunks) groove_OwnedChunkProvider_drop(&r->chunks);
}
static void ready_drop(void *p) {
    ReadyResult *r = p;
    if (r->ok) request_output_drop(&r->output);
    else jz_err_drop(&r->failure.error);
}
static const jz_btree_desc PENDING_MAP = { sizeof(groove_EvaluationRequestKey), sizeof(PendingRequest), request_key_cmp, request_key_drop, pending_drop };
static const jz_btree_desc READY_MAP = { sizeof(groove_EvaluationRequestKey), sizeof(ReadyResult), request_key_cmp, request_key_drop, ready_drop };

void groove__ivm__runtime__evaluation_session__EvaluationRequests__new(groove_EvaluationRequests *r) {
    jz_btree_init(&r->pending, &PENDING_MAP);
    jz_btree_init(&r->ready, &READY_MAP);
}

void groove_EvaluationRequests_drop(groove_EvaluationRequests *r) {
    jz_btree_drop(&r->pending);
    jz_btree_drop(&r->ready);
}

/* Register a request if neither its future nor result already exists.
 * Returns true only for the caller that created the in-flight work. */
bool groove__ivm__runtime__evaluation_session__EvaluationRequests__request(groove_EvaluationRequests *r, groove_EvaluationRequestKey key,
    groove_Storage storage, const groove_OwnedChunkProvider *chunks, const groove_DatabaseSchema *schema) {
    if (jz_btree_contains(&r->pending, &key) || jz_btree_contains(&r->ready, &key)) {
        request_key_drop(&key);
        return false;
    }
    PendingRequest p; memset(&p, 0, sizeof p);
    switch (key.tag) {
    case groove_ERK_Storage: p.eager_retry_safe = groove__storage__Storage__permits_eager_read_retry(storage); break;
    default: p.eager_retry_safe = chunks && groove__chunks__OwnedChunkProvider__permits_eager_read_retry(chunks); break;
    }
    p.storage = storage;
    if (chunks) { p.has_chunks = true; p.chunks = groove__chunks__OwnedChunkProvider__clone(chunks); }
    p.schema = schema;
    jz_btree_insert(&r->pending, &key, &p, NULL);
    return true;
}

/* ---- storage request work ---- */
static void indexed_source(const groove_DatabaseSchema *schema, const jz_str *table, const jz_str *index,
                           const groove_TableSchema **table_schema, const groove_IndexSchema **index_schema) {
    const groove_TableSchema *t = groove__schema__DatabaseSchema__table(schema, table->ptr, table->len);
    if (!t) jz_panic("compiled indexed-row source table exists");
    const groove_IndexSchema *found = NULL;
    JZ_VEC_FOR(const groove_IndexSchema, candidate, t->indices) {
        if (jz_str_eq(&candidate->name, index)) { found = candidate; break; }
    }
    if (!found) jz_panic("compiled indexed-row source index exists");
    *table_schema = t;
    *index_schema = found;
}

static groove_ScanRequest index_prefix(const jz_bytes *prefix) {
    return groove__storage__ScanRequest__prefix(jz_str_from("indices"), jz_bytes_clone(prefix));
}

static groove_ScanRequest limited(groove_ScanRequest request, u32 max_items, bool reversed) {
    groove__storage__ScanRequest__with_max_items(&request, max_items);
    if (reversed) groove__storage__ScanRequest__reversed(&request);
    return request;
}

static jz_err indexed_primary_keys(const groove_TableSchema *table, const jz_str *index_name, const groove_IndexSchema *index_schema,
                                   const groove_KvPairVec *entries, groove_BytesVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    jz_vec_reserve(out, entries->len, sizeof(jz_bytes));
    JZ_VEC_FOR(const groove_KvPair, e, *entries) {
        jz_bytes primary_key;
        jz_err err = groove__db__encoding__persisted_index_primary_key(table, index_name->ptr, index_name->len, index_schema,
            jz_bytes_slice(&e->key), jz_bytes_slice(&e->value), &primary_key);
        if (JZ_IS_ERR(err)) {
            jz_err_drop(&err);
            jz_vec_drop(out, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
            return GROOVE_IVM_ERR_S(GROOVE_IVM_InvalidPersistedIndex, index_name);
        }
        JZ_VEC_PUSH(*out, primary_key);
    }
    return JZ_OK;
}

typedef struct OptUuid { bool some; jz_uuid uuid; } OptUuid;
typedef JZ_VEC(OptUuid) OptUuidVec;

static jz_err indexed_uuid_values(const groove_TableSchema *table, const jz_str *index_name, const groove_IndexSchema *index_schema,
                                  const groove_KvPairVec *entries, const jz_str *column, OptUuidVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    JZ_VEC_FOR(const groove_KvPair, e, *entries) {
        groove_Value value;
        jz_err err = groove__db__encoding__persisted_index_column_value(table, index_name->ptr, index_name->len, index_schema,
            jz_bytes_slice(&e->key), jz_bytes_slice(&e->value), column->ptr, column->len, &value);
        if (JZ_IS_ERR(err)) {
            jz_err_drop(&err);
            jz_vec_drop(out, sizeof(OptUuid), NULL);
            return GROOVE_IVM_ERR_S(GROOVE_IVM_InvalidPersistedIndex, index_name);
        }
        const groove_Value *v = &value;
        while (v->tag == groove_Value_Nullable && v->v.nullable) v = v->v.nullable;
        OptUuid o = { false, {{0}} };
        if (v->tag == groove_Value_Uuid) { o.some = true; o.uuid = v->v.uuid; }
        else if (v->tag == groove_Value_Nullable) { o.some = false; }
        else {
            groove_Value_drop(&value);
            jz_vec_drop(out, sizeof(OptUuid), NULL);
            return GROOVE_IVM_ERR_S(GROOVE_IVM_InvalidPersistedIndex, index_name);
        }
        groove_Value_drop(&value);
        JZ_VEC_PUSH(*out, o);
    }
    return JZ_OK;
}

/* load_indexed_rows: fan out one get per primary key, then collect in order. */
static jz_err load_indexed_rows(groove_Storage storage, const groove_TableSchema *table, const groove_IndexSchema *index_schema,
                                const jz_str *index_name, groove_KvPairVec entries, groove_EvaluationRequestOutput *out) {
    groove_BytesVec primary_keys;
    jz_err err = indexed_primary_keys(table, index_name, index_schema, &entries, &primary_keys);
    groove_KvPairVec_drop(&entries);
    JZ_TRY(err);
    groove_KvPairVec rows = {0};
    jz_vec_reserve(&rows, primary_keys.len, sizeof(groove_KvPair));
    for (u32 i = 0; i < primary_keys.len; i++) {
        bool found = false; jz_bytes record = {0};
        err = storage.vt->get(storage.self, &table->name, jz_bytes_slice(&primary_keys.ptr[i]), &found, &record);
        if (!JZ_IS_ERR(err) && !found) err = GROOVE_IVM_ERR_S(GROOVE_IVM_InvalidPersistedIndex, index_name);
        if (JZ_IS_ERR(err)) {
            groove_KvPairVec_drop(&rows);
            jz_vec_drop(&primary_keys, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
            return err;
        }
        groove_KvPair kv = { jz_bytes_clone(&primary_keys.ptr[i]), record };
        JZ_VEC_PUSH(rows, kv);
    }
    jz_vec_drop(&primary_keys, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
    out->tag = groove_ERO_Storage;
    out->storage_tag = groove_SRO_Rows;
    out->rows = rows;
    return JZ_OK;
}

static jz_err indexed_rows(groove_Storage storage, const groove_DatabaseSchema *schema, const jz_str *table, const jz_str *index,
                           groove_ScanRequest request, groove_EvaluationRequestOutput *out) {
    const groove_TableSchema *table_schema; const groove_IndexSchema *index_schema;
    indexed_source(schema, table, index, &table_schema, &index_schema);
    groove_KvPairVec entries = {0};
    jz_err err = storage.vt->scan(storage.self, &request, &entries);
    groove_ScanRequest_drop(&request);
    JZ_TRY(err);
    return load_indexed_rows(storage, table_schema, index_schema, index, entries, out);
}

static jz_err rows_output(groove_Storage storage, groove_ScanRequest request, groove_EvaluationRequestOutput *out) {
    groove_KvPairVec rows = {0};
    jz_err err = storage.vt->scan(storage.self, &request, &rows);
    groove_ScanRequest_drop(&request);
    JZ_TRY(err);
    out->tag = groove_ERO_Storage;
    out->storage_tag = groove_SRO_Rows;
    out->rows = rows;
    return JZ_OK;
}

/* Below this many source index entries, a candidate-filtered source hydrates
 * its rows directly instead of scanning the candidate index first. */
#define CANDIDATE_FILTER_MIN_SOURCE_ENTRIES 512u

static const jz_hmap_desc BYTES_SET_H = { sizeof(jz_bytes), 0, groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, NULL };
static const jz_hmap_desc UUID_SET_H = { sizeof(jz_uuid), 0, jz_uuid_hash, jz_uuid_eq_v, NULL, NULL };

static jz_err storage_intersection(const groove_StorageRequestKey *key, groove_Storage storage, const groove_DatabaseSchema *schema,
                                   groove_EvaluationRequestOutput *out) {
    const groove_TableSchema *table_schema; const groove_IndexSchema *primary_index_schema;
    indexed_source(schema, &key->family, &key->index, &table_schema, &primary_index_schema);
    groove_KvPairVec entries = {0};
    groove_ScanRequest req = index_prefix(&key->a);
    jz_err err = storage.vt->scan(storage.self, &req, &entries);
    groove_ScanRequest_drop(&req);
    JZ_TRY(err);
    groove_BytesVec primary_keys;
    err = indexed_primary_keys(table_schema, &key->index, primary_index_schema, &entries, &primary_keys);
    if (JZ_IS_ERR(err)) { groove_KvPairVec_drop(&entries); return err; }
    jz_hmap retained; jz_hmap_init(&retained, &BYTES_SET_H);
    JZ_VEC_FOR(jz_bytes, k, primary_keys) { jz_bytes c = jz_bytes_clone(k); if (jz_hmap_insert(&retained, &c, NULL, NULL)) jz_bytes_drop(&c); }
    JZ_VEC_FOR(const groove_IndexPrefixPair, other, key->intersections) {
        const groove_IndexSchema *other_schema = NULL;
        JZ_VEC_FOR(const groove_IndexSchema, candidate, table_schema->indices) {
            if (jz_str_eq(&candidate->name, &other->index)) { other_schema = candidate; break; }
        }
        if (!other_schema) jz_panic("compiled intersected index exists");
        groove_KvPairVec other_entries = {0};
        req = index_prefix(&other->prefix);
        err = storage.vt->scan(storage.self, &req, &other_entries);
        groove_ScanRequest_drop(&req);
        groove_BytesVec other_keys_v = {0};
        if (!JZ_IS_ERR(err)) err = indexed_primary_keys(table_schema, &other->index, other_schema, &other_entries, &other_keys_v);
        groove_KvPairVec_drop(&other_entries);
        if (JZ_IS_ERR(err)) goto fail;
        jz_hmap other_keys; jz_hmap_init(&other_keys, &BYTES_SET_H);
        JZ_VEC_FOR(jz_bytes, k, other_keys_v) { if (jz_hmap_insert(&other_keys, k, NULL, NULL)) jz_bytes_drop(k); }
        other_keys_v.len = 0;
        jz_vec_drop(&other_keys_v, sizeof(jz_bytes), NULL);
        /* retained.retain(|key| other_keys.contains(key)) */
        groove_BytesVec drop_keys = {0};
        jz_bytes *k; void *unused;
        JZ_HMAP_FOR(&retained, k, unused) { if (!jz_hmap_get(&other_keys, k)) JZ_VEC_PUSH(drop_keys, *k); }
        JZ_VEC_FOR(jz_bytes, dk, drop_keys) { jz_bytes owned; jz_bytes probe = *dk; if (jz_hmap_remove(&retained, &probe, &owned, NULL)) jz_bytes_drop(&owned); }
        jz_vec_drop(&drop_keys, sizeof(jz_bytes), NULL);
        jz_hmap_drop(&other_keys);
    }
    {
        groove_KvPairVec kept = {0};
        for (u32 i = 0; i < entries.len; i++) {
            if (jz_hmap_get(&retained, &primary_keys.ptr[i])) JZ_VEC_PUSH(kept, entries.ptr[i]);
            else groove_KvPair_drop(&entries.ptr[i]);
        }
        entries.len = 0;
        groove_KvPairVec_drop(&entries);
        jz_vec_drop(&primary_keys, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
        jz_hmap_drop(&retained);
        return load_indexed_rows(storage, table_schema, primary_index_schema, &key->index, kept, out);
    }
fail:
    groove_KvPairVec_drop(&entries);
    jz_vec_drop(&primary_keys, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
    jz_hmap_drop(&retained);
    return err;
}

static jz_err storage_candidate_filter(const groove_StorageRequestKey *key, groove_Storage storage, const groove_DatabaseSchema *schema,
                                       groove_EvaluationRequestOutput *out) {
    const groove_TableSchema *table_schema; const groove_IndexSchema *index_schema;
    indexed_source(schema, &key->family, &key->index, &table_schema, &index_schema);
    const groove_TableSchema *candidate_schema; const groove_IndexSchema *candidate_index_schema;
    indexed_source(schema, &key->candidate_table, &key->candidate_index, &candidate_schema, &candidate_index_schema);
    groove_KvPairVec entries = {0};
    groove_ScanRequest req = index_prefix(&key->a);
    jz_err err = storage.vt->scan(storage.self, &req, &entries);
    groove_ScanRequest_drop(&req);
    JZ_TRY(err);
    /* An already-small source is cheaper to hydrate directly. Cap the
     * candidate probe so a broad candidate prefix cannot scan thousands of
     * keys to filter a small source. */
    if (entries.len < CANDIDATE_FILTER_MIN_SOURCE_ENTRIES)
        return load_indexed_rows(storage, table_schema, index_schema, &key->index, entries, out);
    u32 candidate_scan_limit = (entries.len + 1) / 2;
    groove_KvPairVec candidates = {0};
    req = index_prefix(&key->candidate_prefix);
    groove__storage__ScanRequest__with_max_items(&req, candidate_scan_limit);
    err = storage.vt->scan(storage.self, &req, &candidates);
    groove_ScanRequest_drop(&req);
    if (JZ_IS_ERR(err)) { groove_KvPairVec_drop(&entries); return err; }
    if (candidates.len == candidate_scan_limit) {
        groove_KvPairVec_drop(&candidates);
        return load_indexed_rows(storage, table_schema, index_schema, &key->index, entries, out);
    }
    OptUuidVec cand_vals;
    err = indexed_uuid_values(candidate_schema, &key->candidate_index, candidate_index_schema, &candidates, &key->candidate_column, &cand_vals);
    groove_KvPairVec_drop(&candidates);
    if (JZ_IS_ERR(err)) { groove_KvPairVec_drop(&entries); return err; }
    jz_hmap candidate_uuids; jz_hmap_init(&candidate_uuids, &UUID_SET_H);
    JZ_VEC_FOR(OptUuid, o, cand_vals) if (o->some) jz_hmap_insert(&candidate_uuids, &o->uuid, NULL, NULL);
    jz_vec_drop(&cand_vals, sizeof(OptUuid), NULL);
    if (candidate_uuids.len == 0) {
        jz_hmap_drop(&candidate_uuids);
        groove_KvPairVec_drop(&entries);
        out->tag = groove_ERO_Storage; out->storage_tag = groove_SRO_Rows;
        out->rows.ptr = NULL; out->rows.len = 0; out->rows.cap = 0;
        return JZ_OK;
    }
    OptUuidVec source_uuids;
    err = indexed_uuid_values(table_schema, &key->index, index_schema, &entries, &key->source_column, &source_uuids);
    if (JZ_IS_ERR(err)) { jz_hmap_drop(&candidate_uuids); groove_KvPairVec_drop(&entries); return err; }
    groove_KvPairVec kept = {0};
    for (u32 i = 0; i < entries.len; i++) {
        if (source_uuids.ptr[i].some && jz_hmap_get(&candidate_uuids, &source_uuids.ptr[i].uuid)) JZ_VEC_PUSH(kept, entries.ptr[i]);
        else groove_KvPair_drop(&entries.ptr[i]);
    }
    entries.len = 0;
    groove_KvPairVec_drop(&entries);
    jz_vec_drop(&source_uuids, sizeof(OptUuid), NULL);
    jz_hmap_drop(&candidate_uuids);
    return load_indexed_rows(storage, table_schema, index_schema, &key->index, kept, out);
}

static jz_err storage_request(const groove_StorageRequestKey *key, groove_Storage storage, const groove_DatabaseSchema *schema,
                              groove_EvaluationRequestOutput *out) {
    memset(out, 0, sizeof *out);
    switch (key->tag) {
    case groove_SRK_Get: {
        bool found = false; jz_bytes value = {0};
        JZ_TRY(storage.vt->get(storage.self, &key->family, jz_bytes_slice(&key->a), &found, &value));
        out->tag = groove_ERO_Storage; out->storage_tag = groove_SRO_Value;
        out->has_value = found; out->value = value;
        return JZ_OK;
    }
    case groove_SRK_ScanRange:
        return rows_output(storage, groove__storage__ScanRequest__range(jz_str_clone(&key->family), jz_bytes_clone(&key->a), jz_bytes_clone(&key->b)), out);
    case groove_SRK_ScanPrefix:
        return rows_output(storage, groove__storage__ScanRequest__prefix(jz_str_clone(&key->family), jz_bytes_clone(&key->a)), out);
    case groove_SRK_ScanPrefixLimit:
        return rows_output(storage, limited(groove__storage__ScanRequest__prefix(jz_str_clone(&key->family), jz_bytes_clone(&key->a)),
                                            key->max_items, key->reversed), out);
    case groove_SRK_IndexedRowsPrefix:
        return indexed_rows(storage, schema, &key->family, &key->index, index_prefix(&key->a), out);
    case groove_SRK_IndexedRowsPrefixLimit:
        return indexed_rows(storage, schema, &key->family, &key->index, limited(index_prefix(&key->a), key->max_items, key->reversed), out);
    case groove_SRK_IndexedRowsRange:
        return indexed_rows(storage, schema, &key->family, &key->index,
                            groove__storage__ScanRequest__range(jz_str_from("indices"), jz_bytes_clone(&key->a), jz_bytes_clone(&key->b)), out);
    case groove_SRK_IndexedRowsIntersection:
        return storage_intersection(key, storage, schema, out);
    case groove_SRK_IndexedRowsCandidateFilter:
        return storage_candidate_filter(key, storage, schema, out);
    }
    JZ_UNREACHABLE();
}

/* Drive one request future to completion. */
static void run_request(const groove_EvaluationRequestKey *key, PendingRequest *p, ReadyResult *result) {
    memset(result, 0, sizeof *result);
    if (key->tag == groove_ERK_Storage) {
        jz_err err = storage_request(&key->storage, p->storage, p->schema, &result->output);
        if (JZ_IS_ERR(err)) { result->ok = false; result->failure.error = err; result->failure.publication_metadata_durability = false; }
        else result->ok = true;
        return;
    }
    if (!p->has_chunks) {
        result->ok = false;
        result->failure.error = groove__chunks__ChunkError__unavailable();
        result->failure.publication_metadata_durability = false;
        return;
    }
    groove_ChunkLease lease; bool durability = false;
    jz_err err = groove__chunks__OwnedChunkProvider__get_tracked(&p->chunks, &key->chunk, &lease, &durability);
    if (JZ_IS_ERR(err)) {
        result->ok = false;
        result->failure.error = err; /* IvmRuntimeError::Chunk(error) is transparent */
        result->failure.publication_metadata_durability = durability;
        return;
    }
    result->ok = true;
    result->output.tag = groove_ERO_Chunk;
    result->output.chunk.tag = groove_LCB_Leased;
    result->output.chunk.lease = lease;
}

typedef JZ_VEC(groove_EvaluationRequestKey) KeyVec;

static u32 poll_where(groove_EvaluationRequests *r, bool only_eager_retry_safe) {
    KeyVec completed = {0};
    JZ_VEC(ReadyResult) results = {0};
    groove_EvaluationRequestKey *key; PendingRequest *request;
    JZ_BTREE_FOR(&r->pending, key, request) {
        if (only_eager_retry_safe && !request->eager_retry_safe) continue;
        ReadyResult res;
        run_request(key, request, &res);
        groove_EvaluationRequestKey kc; request_key_clone(&kc, key);
        JZ_VEC_PUSH(completed, kc);
        JZ_VEC_PUSH(results, res);
    }
    u32 count = completed.len;
    for (u32 i = 0; i < count; i++) {
        groove_EvaluationRequestKey owned;
        PendingRequest p;
        if (jz_btree_remove(&r->pending, &completed.ptr[i], &owned, &p)) { request_key_drop(&owned); pending_drop(&p); }
        ReadyResult old;
        if (jz_btree_insert(&r->ready, &completed.ptr[i], &results.ptr[i], &old)) ready_drop(&old);
    }
    jz_vec_drop(&completed, sizeof(groove_EvaluationRequestKey), NULL);
    jz_vec_drop(&results, sizeof(ReadyResult), NULL);
    return count;
}

/* Advance every in-flight request once so one blocked source cannot hide a
 * different request that is already resident. */
u32 groove__ivm__runtime__evaluation_session__EvaluationRequests__poll(groove_EvaluationRequests *r) { return poll_where(r, false); }

/* Re-poll only requests whose storage backend guarantees an eager retry
 * cannot wait for external I/O. */
u32 groove__ivm__runtime__evaluation_session__EvaluationRequests__poll_eager_retry(groove_EvaluationRequests *r) { return poll_where(r, true); }

bool groove__ivm__runtime__evaluation_session__EvaluationRequests__has_pending(const groove_EvaluationRequests *r) { return r->pending.len != 0; }

/* Move every large-value chunk fetch, in flight or finished, into successor (#3901). */
void groove__ivm__runtime__evaluation_session__EvaluationRequests__hand_off_chunks(groove_EvaluationRequests *r, groove_EvaluationRequests *successor) {
    jz_btree pending = r->pending;
    jz_btree_init(&r->pending, &PENDING_MAP);
    groove_EvaluationRequestKey key; PendingRequest p;
    while (jz_btree_pop_first(&pending, &key, &p)) {
        if (key.tag == groove_ERK_Chunk) {
            if (jz_btree_contains(&successor->pending, &key)) { request_key_drop(&key); pending_drop(&p); }
            else jz_btree_insert(&successor->pending, &key, &p, NULL);
        } else {
            jz_btree_insert(&r->pending, &key, &p, NULL);
        }
    }
    jz_btree_drop(&pending);
    jz_btree ready = r->ready;
    jz_btree_init(&r->ready, &READY_MAP);
    ReadyResult res;
    while (jz_btree_pop_first(&ready, &key, &res)) {
        if (key.tag == groove_ERK_Chunk) {
            if (jz_btree_contains(&successor->ready, &key)) { request_key_drop(&key); ready_drop(&res); }
            else jz_btree_insert(&successor->ready, &key, &res, NULL);
        } else {
            jz_btree_insert(&r->ready, &key, &res, NULL);
        }
    }
    jz_btree_drop(&ready);
}

bool groove__ivm__runtime__evaluation_session__EvaluationRequests__has_pending_chunk(const groove_EvaluationRequests *r) {
    groove_EvaluationRequestKey *key; PendingRequest *p;
    JZ_BTREE_FOR(&r->pending, key, p) if (key->tag == groove_ERK_Chunk) return true;
    return false;
}

bool groove__ivm__runtime__evaluation_session__EvaluationRequests__drain_ready(groove_EvaluationRequests *r, jz_btree *out,
    groove_EvaluationRequestKey *failed_key, groove_EvaluationRequestFailure *failure) {
    jz_btree ready = r->ready;
    jz_btree_init(&r->ready, &READY_MAP);
    jz_btree_init(out, &groove_ivm_rt_REQUEST_OUTPUT_MAP);
    groove_EvaluationRequestKey key; ReadyResult res;
    while (jz_btree_pop_first(&ready, &key, &res)) {
        if (res.ok) {
            jz_btree_insert(out, &key, &res.output, NULL);
        } else {
            *failed_key = key;
            *failure = res.failure;
            jz_btree_drop(&ready);
            jz_btree_drop(out);
            return false;
        }
    }
    jz_btree_drop(&ready);
    return true;
}
